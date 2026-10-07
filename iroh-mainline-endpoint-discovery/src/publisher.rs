//! Announce Mainline infohashes, keeping the endpoint's index record current.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh_base::{EndpointId, SecretKey};
use n0_error::{Result, StackResultExt, StdResultExt};
use n0_future::task::{self, AbortOnDropHandle};
use n0_mainline::{Dht, Id};
use tokio::{
    sync::{Notify, Semaphore, mpsc, watch},
    time::Instant,
};

use crate::{
    AddrIndex,
    index_keeper::{IndexKeeper, announce_indexed},
};
use tracing::{info, warn};

/// How often to renew Mainline announcements.
pub const REFRESH: Duration = Duration::from_secs(10 * 60);
/// Age after which the address-index record is republished.
///
/// Index servers keep a record for an hour by default, so this leaves room for
/// failed attempts.
pub const INDEX_REFRESH: Duration = Duration::from_secs(30 * 60);
/// Minimum spacing between announcement starts.
pub const ANNOUNCE_SPACING: Duration = Duration::from_millis(250);
/// Delay before retrying a failed publication or announcement.
pub const RETRY: Duration = Duration::from_secs(30);

/// Keeps Mainline announcements and one signed endpoint value current.
///
/// Each infohash is announced on its own timer, every [`REFRESH`] with some
/// jitter. The index record is kept on a separate schedule while infohashes
/// are registered: it is republished when older than [`INDEX_REFRESH`], when
/// discovery finds other index servers, and when an announcement finds that
/// Mainline sees us at another address. It goes to all index servers and
/// counts as published once one stored it, since readers ask them all. A
/// failed publication is retried after [`RETRY`].
///
/// An announcement goes ahead only if an index server holds our record for
/// the address Mainline sees us at, since readers could not resolve it
/// otherwise. If none does, it waits for the next successful publication and
/// then resumes after a random delay of up to a minute.
///
/// Publishing runs in a background task owned by this handle. Dropping the
/// handle stops it, and the announcements expire from the DHT soon after.
#[derive(Debug, Clone)]
pub struct Publisher {
    state: Arc<State>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug)]
struct State {
    secret: SecretKey,
    dht: Dht,
    index: AddrIndex,
    entries: Mutex<HashSet<Id>>,
    notify: Notify,
    /// Socket of the index record once every registered hash is announced.
    published: watch::Sender<Option<SocketAddrV4>>,
    keeper: IndexKeeper,
    /// Whether any infohash is registered, which the keeper publishes for.
    active: watch::Sender<bool>,
}

impl Publisher {
    /// Creates a publisher from a secret key, a Mainline node, and an address index.
    ///
    /// Publishing starts immediately, and does nothing until the first
    /// infohash is added.
    pub fn new(secret: SecretKey, dht: Dht, index: AddrIndex) -> Self {
        let state = Arc::new(State {
            secret,
            dht,
            index,
            entries: Mutex::new(HashSet::new()),
            notify: Notify::new(),
            published: watch::channel(None).0,
            keeper: IndexKeeper::default(),
            active: watch::channel(false).0,
        });
        let task = task::spawn(state.clone().run());
        Self {
            state,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    /// Returns the endpoint identity published by this instance.
    pub fn id(&self) -> EndpointId {
        self.state.secret.public()
    }

    /// Returns the address index used by this publisher.
    pub fn index(&self) -> &AddrIndex {
        &self.state.index
    }

    /// Returns the most recently announced Mainline lookup key.
    pub fn public_v4(&self) -> Option<SocketAddrV4> {
        *self.state.published.borrow()
    }

    /// Returns the infohashes currently registered for announcement.
    pub fn infohashes(&self) -> Vec<Id> {
        self.state.infohashes()
    }

    /// Registers an infohash for announcement.
    ///
    /// Returns whether it was newly inserted.
    pub fn add_infohash(&self, infohash: Id) -> bool {
        let inserted = self
            .state
            .entries
            .lock()
            .expect("poisoned")
            .insert(infohash);
        if inserted {
            self.state.notify.notify_one();
        }
        inserted
    }

    /// Stops renewing an infohash.
    ///
    /// Returns whether it was registered.
    pub fn remove_infohash(&self, infohash: &Id) -> bool {
        let removed = self
            .state
            .entries
            .lock()
            .expect("poisoned")
            .remove(infohash);
        if removed {
            self.state.notify.notify_one();
        }
        removed
    }

    /// Waits until the index record and all currently registered hashes are published.
    ///
    /// After the first successful publication, subsequent calls return immediately,
    /// including after new infohashes are added.
    pub async fn wait_published(&self) {
        let mut receiver = self.state.published.subscribe();
        while receiver.borrow().is_none() && receiver.changed().await.is_ok() {}
    }
}

impl State {
    /// Runs the index keeper and one announcement worker per registered infohash.
    async fn run(self: Arc<Self>) {
        let keeper = self.keeper.run(
            self.active.subscribe(),
            self.index.servers(),
            || async {
                // Bootstrapping tells Mainline our address, so the first
                // record is published for the address announcements will see.
                let _ = self.dht.bootstrapped().await;
                public_address(&self.dht).await
            },
            || publish_index(&self.index, &self.secret),
        );
        tokio::pin!(keeper);
        let mut records = self.keeper.subscribe();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut workers = HashMap::new();
        let mut announced = HashSet::new();
        let pacing = Arc::new(tokio::sync::Mutex::new(Instant::now()));
        let permits = Arc::new(Semaphore::new(3));
        loop {
            let entries: HashSet<_> = self.infohashes().into_iter().collect();
            self.active.send_if_modified(|active| {
                let changed = *active == entries.is_empty();
                *active = !entries.is_empty();
                changed
            });
            workers.retain(|hash, _| entries.contains(hash));
            announced.retain(|hash| entries.contains(hash));
            for hash in entries.iter().copied() {
                workers.entry(hash).or_insert_with(|| {
                    let state = self.clone();
                    let tx = tx.clone();
                    let pacing = pacing.clone();
                    let permits = permits.clone();
                    AbortOnDropHandle::new(task::spawn(async move {
                        repeat_announcement(|| async {
                            let result = announce_indexed(
                                &state.keeper,
                                || async {
                                    let permit = permits
                                        .clone()
                                        .acquire_owned()
                                        .await
                                        .expect("semaphore closed");
                                    let mut next = pacing.lock().await;
                                    tokio::time::sleep_until(*next).await;
                                    *next = Instant::now() + ANNOUNCE_SPACING;
                                    permit
                                },
                                || async {
                                    // Finding the closest nodes primes their tokens
                                    // for the announce, and their replies tell us
                                    // the address the announce will store.
                                    state.dht.get_closest_nodes(hash).await.with_context(|_| {
                                        format!("get_closest_nodes for {hash}")
                                    })?;
                                    Ok(public_address(&state.dht).await)
                                },
                                || async {
                                    state.dht.announce_peer(hash, None).await.with_context(
                                        |_| format!("announce_peer infohash {hash}"),
                                    )?;
                                    info!(infohash = %hash, "renewed Mainline announcement");
                                    Ok(())
                                },
                            )
                            .await;
                            match &result {
                                Ok(()) => {
                                    let _ = tx.send(hash);
                                }
                                Err(err) => warn!(%hash, %err, "Mainline announcement failed"),
                            }
                            result.is_ok()
                        })
                        .await;
                    }))
                });
            }
            if !entries.is_empty() && entries.is_subset(&announced) {
                self.published.send_replace(self.keeper.mapping());
            }
            tokio::select! {
                () = &mut keeper => return,
                _ = self.notify.notified() => {},
                _ = records.changed() => {},
                Some(hash) = rx.recv() => { announced.insert(hash); },
            }
        }
    }

    /// Returns the registered infohashes, sorted.
    fn infohashes(&self) -> Vec<Id> {
        let mut entries: Vec<_> = self
            .entries
            .lock()
            .expect("poisoned")
            .iter()
            .copied()
            .collect();
        entries.sort();
        entries
    }
}

/// Publishes the endpoint record, returning the socket a server stored it under.
pub(crate) async fn publish_index(index: &AddrIndex, secret: &SecretKey) -> Result<SocketAddrV4> {
    index
        .publish(secret)
        .await?
        .first()
        .copied()
        .std_context("address-index publish returned no public mapping")
}

/// Returns the address Mainline sees us at, if it knows it yet.
pub(crate) async fn public_address(dht: &Dht) -> Option<SocketAddrV4> {
    dht.info().await.ok()?.public_address()
}

/// Each worker owns its timer; neither another hash nor a mapping change resets it.
async fn repeat_announcement<F, Fut>(mut announce: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    loop {
        let delay = if announce().await {
            // Refresh slightly early with jitter to avoid synchronized renewals.
            REFRESH - Duration::from_secs(rand::random_range(0..=60))
        } else {
            RETRY
        };
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn new_hash_does_not_wait_for_existing_refresh_and_removal_stops_it() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let first_tx = tx.clone();
        let first = tokio::spawn(repeat_announcement(move || {
            first_tx.send(1).expect("receiver alive");
            async { true }
        }));
        assert_eq!(rx.recv().await, Some(1));
        tokio::time::advance(Duration::from_secs(100)).await;
        let second = tokio::spawn(repeat_announcement(move || {
            tx.send(2).expect("receiver alive");
            async { true }
        }));
        assert_eq!(rx.recv().await, Some(2));
        let started = Instant::now();
        assert_eq!(rx.recv().await, Some(1));
        assert!(started.elapsed() >= Duration::from_secs(440));
        assert!(started.elapsed() <= Duration::from_secs(500));
        first.abort();
        first.await.expect_err("worker cancelled");
        assert_eq!(rx.recv().await, Some(2));
        second.abort();
        second.await.expect_err("worker cancelled");
        tokio::time::advance(REFRESH * 2).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn failure_retries_only_the_failed_hash() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let failed_tx = tx.clone();
        let failed = tokio::spawn(repeat_announcement(move || {
            failed_tx.send(1).expect("receiver alive");
            async { false }
        }));
        assert_eq!(rx.recv().await, Some(1));
        let healthy = tokio::spawn(repeat_announcement(move || {
            tx.send(2).expect("receiver alive");
            async { true }
        }));
        assert_eq!(rx.recv().await, Some(2));
        let started = Instant::now();
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(started.elapsed(), RETRY);
        failed.abort();
        healthy.abort();
    }
}
