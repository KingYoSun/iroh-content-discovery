//! Keeps one endpoint record on the address-index servers, and gates
//! announcements on it.
//!
//! The keeper and the announcements run on their own schedules. They meet in
//! the middle of each announcement: once Mainline has told us our address, the
//! announcement reports it to the keeper, and goes ahead only if an index
//! server holds our record for that address. Otherwise readers could find the
//! announcement but not resolve it, so it waits for the keeper's next
//! successful publication instead.

use std::{collections::BTreeSet, future::Future, net::SocketAddrV4, time::Duration};

use n0_error::Result;
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};
use tracing::{debug, info, warn};

use crate::{INDEX_REFRESH, RETRY};

/// How long index servers keep a record, the server default.
///
/// The protocol does not report it, so the keeper assumes it.
pub(crate) const INDEX_TTL: Duration = Duration::from_secs(60 * 60);

/// Longest random delay before an announcement resumes after waiting out an
/// index outage, so the waiting ones do not all restart at once.
pub(crate) const RESTART_JITTER: Duration = Duration::from_secs(60);

/// A record that an index server stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Record {
    /// Our address as Mainline reported it when publishing, if it knew it.
    pub(crate) public: Option<SocketAddrV4>,
    /// Socket the server stored the record under.
    pub(crate) mapping: SocketAddrV4,
    pub(crate) at: Instant,
}

impl Record {
    /// Returns whether readers can resolve an announcement from `public`.
    fn is_live(&self, public: Option<SocketAddrV4>, now: Instant) -> bool {
        self.public == public && now.saturating_duration_since(self.at) < INDEX_TTL
    }
}

/// Publishes the endpoint record while there is something to announce.
///
/// It publishes to all index servers and counts a publication as done once
/// one of them stored it, since readers ask them all. It republishes when the
/// record is older than [`INDEX_REFRESH`], when the servers change, and when
/// an announcement reports another address. A failed attempt is retried after
/// [`RETRY`], and no trigger makes it try sooner.
#[derive(Debug, Default)]
pub(crate) struct IndexKeeper {
    record: watch::Sender<Option<Record>>,
    /// Set when an announcement saw an address the record is not for.
    reported: Notify,
}

impl IndexKeeper {
    /// Returns the socket of the current record.
    pub(crate) fn mapping(&self) -> Option<SocketAddrV4> {
        self.record.borrow().map(|record| record.mapping)
    }

    /// Subscribes to successful publications.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Option<Record>> {
        self.record.subscribe()
    }

    /// Returns whether an index server holds our record for `public`.
    pub(crate) fn is_live(&self, public: Option<SocketAddrV4>, now: Instant) -> bool {
        self.record
            .borrow()
            .is_some_and(|record| record.is_live(public, now))
    }

    /// Reports the address Mainline sees us at, without waiting.
    ///
    /// If the record is for another address, the keeper republishes soon.
    pub(crate) fn report(&self, public: Option<SocketAddrV4>) {
        if self
            .record
            .borrow()
            .is_none_or(|record| record.public != public)
        {
            self.reported.notify_one();
        }
    }

    /// Keeps the record current until cancelled.
    ///
    /// Publishes only while `active` is true. `address` returns the address
    /// Mainline sees us at, and `publish` stores the record, returning the
    /// socket a server stored it under.
    pub(crate) async fn run<A, P>(
        &self,
        mut active: watch::Receiver<bool>,
        mut servers: watch::Receiver<BTreeSet<SocketAddrV4>>,
        address: impl Fn() -> A,
        publish: impl Fn() -> P,
    ) where
        A: Future<Output = Option<SocketAddrV4>>,
        P: Future<Output = Result<SocketAddrV4>>,
    {
        let mut published_to = None;
        let mut failed: Option<Instant> = None;
        loop {
            if !*active.borrow_and_update() {
                if active.changed().await.is_err() {
                    return;
                }
                continue;
            }
            let public = address().await;
            let current_servers = servers.borrow_and_update().clone();
            let record = *self.record.borrow();
            let now = Instant::now();
            let due = published_to.as_ref() != Some(&current_servers)
                || record.is_none_or(|record| {
                    record.public != public || now >= record.at + INDEX_REFRESH
                });
            let backing_off = failed.is_some_and(|failed| now < failed + RETRY);
            if due && !backing_off {
                match publish().await {
                    Ok(mapping) => {
                        if record.is_some_and(|record| record.public != public) {
                            info!(?public, %mapping, "Mainline address changed; republished index record");
                        } else {
                            debug!(?public, %mapping, "published index record");
                        }
                        self.record.send_replace(Some(Record {
                            public,
                            mapping,
                            at: now,
                        }));
                        published_to = Some(current_servers);
                        failed = None;
                    }
                    Err(err) => {
                        warn!(%err, "address-index publication failed; retrying later");
                        failed = Some(Instant::now());
                    }
                }
            }
            // After a failure the record is due already, so only the backoff
            // decides when to try again; waking for the refresh would spin.
            let next = match failed {
                Some(failed) => Some(failed + RETRY),
                None => self.record.borrow().map(|record| record.at + INDEX_REFRESH),
            };
            tokio::select! {
                _ = self.reported.notified() => {}
                Ok(()) = servers.changed() => {}
                Ok(()) = active.changed() => {}
                _ = sleep_until(next) => {}
            }
        }
    }
}

/// Announces once an index server holds our record for our address.
///
/// `closest` finds the closest nodes and returns the address Mainline now
/// sees us at; `announce` stores the announcement on them. Both run under the
/// guard `pace` returns, which is released while waiting. Without a live
/// record, this waits for the keeper's next publication and starts over,
/// since the closest nodes, their tokens and our address may all have
/// changed meanwhile. A wait of at least [`RETRY`] means a publication
/// failed, so the index was down; then it first sleeps up to
/// [`RESTART_JITTER`]. A shorter wait, for a publication already under way at
/// startup or after an address change, resumes right away.
pub(crate) async fn announce_indexed<G, P, C, A>(
    keeper: &IndexKeeper,
    pace: impl Fn() -> P,
    closest: impl Fn() -> C,
    announce: impl Fn() -> A,
) -> Result<()>
where
    P: Future<Output = G>,
    C: Future<Output = Result<Option<SocketAddrV4>>>,
    A: Future<Output = Result<()>>,
{
    loop {
        let guard = pace().await;
        let mut published = keeper.subscribe();
        published.mark_unchanged();
        let public = closest().await?;
        keeper.report(public);
        if keeper.is_live(public, Instant::now()) {
            let result = announce().await;
            drop(guard);
            return result;
        }
        drop(guard);
        debug!(
            ?public,
            "no index server holds our record; waiting to announce"
        );
        let waiting = Instant::now();
        if published.changed().await.is_err() {
            n0_error::bail_any!("index keeper stopped");
        }
        if waiting.elapsed() >= RETRY {
            tokio::time::sleep(restart_jitter()).await;
        }
    }
}

fn restart_jitter() -> Duration {
    Duration::from_millis(rand::random_range(0..=RESTART_JITTER.as_millis() as u64))
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use n0_future::task::{self, AbortOnDropHandle};

    use super::*;

    fn addr(port: u16) -> SocketAddrV4 {
        SocketAddrV4::new([203, 0, 113, 7].into(), port)
    }

    /// Lets spawned tasks run without advancing paused time noticeably.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    /// A keeper driven by fake Mainline and index operations.
    struct Harness {
        keeper: Arc<IndexKeeper>,
        address: Arc<Mutex<Option<SocketAddrV4>>>,
        failing: Arc<AtomicBool>,
        publishes: Arc<AtomicUsize>,
        active: watch::Sender<bool>,
        servers: watch::Sender<BTreeSet<SocketAddrV4>>,
        _task: AbortOnDropHandle<()>,
    }

    impl Harness {
        fn start(active: bool) -> Self {
            let keeper = Arc::new(IndexKeeper::default());
            let address = Arc::new(Mutex::new(Some(addr(1))));
            let failing = Arc::new(AtomicBool::new(false));
            let publishes = Arc::new(AtomicUsize::new(0));
            let (active_tx, active_rx) = watch::channel(active);
            let (servers_tx, servers_rx) = watch::channel(BTreeSet::from([addr(60125)]));
            let task = task::spawn({
                let keeper = keeper.clone();
                let address = address.clone();
                let failing = failing.clone();
                let publishes = publishes.clone();
                async move {
                    keeper
                        .run(
                            active_rx,
                            servers_rx,
                            || async { *address.lock().unwrap() },
                            || async {
                                publishes.fetch_add(1, Ordering::SeqCst);
                                if failing.load(Ordering::SeqCst) {
                                    n0_error::bail_any!("index servers unreachable");
                                }
                                Ok(addr(9999))
                            },
                        )
                        .await
                }
            });
            Self {
                keeper,
                address,
                failing,
                publishes,
                active: active_tx,
                servers: servers_tx,
                _task: AbortOnDropHandle::new(task),
            }
        }

        fn publishes(&self) -> usize {
            self.publishes.load(Ordering::SeqCst)
        }

        fn move_to(&self, port: u16) {
            *self.address.lock().unwrap() = Some(addr(port));
        }

        fn record_public(&self) -> Option<SocketAddrV4> {
            self.keeper.record.borrow().and_then(|record| record.public)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn publishes_only_while_active() {
        let harness = Harness::start(false);
        tokio::time::advance(INDEX_TTL).await;
        settle().await;
        assert_eq!(harness.publishes(), 0);
        harness.active.send_replace(true);
        settle().await;
        assert_eq!(harness.publishes(), 1);
        assert_eq!(harness.record_public(), Some(addr(1)));
        // Nothing changes, so nothing is republished before the record ages.
        tokio::time::advance(INDEX_REFRESH - Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(harness.publishes(), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(harness.publishes(), 2);
        // Deactivating stops the refreshes.
        harness.active.send_replace(false);
        tokio::time::advance(INDEX_TTL).await;
        settle().await;
        assert_eq!(harness.publishes(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reported_address_change_republishes_once() {
        let harness = Harness::start(true);
        settle().await;
        assert_eq!(harness.publishes(), 1);
        // Reporting the address the record is for does nothing.
        harness.keeper.report(Some(addr(1)));
        settle().await;
        assert_eq!(harness.publishes(), 1);
        harness.move_to(2);
        for _ in 0..3 {
            harness.keeper.report(Some(addr(2)));
        }
        settle().await;
        assert_eq!(harness.publishes(), 2);
        assert_eq!(harness.record_public(), Some(addr(2)));
        assert!(harness.keeper.is_live(Some(addr(2)), Instant::now()));
        assert!(!harness.keeper.is_live(Some(addr(1)), Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn new_servers_get_the_record() {
        let harness = Harness::start(true);
        settle().await;
        harness
            .servers
            .send_replace(BTreeSet::from([addr(60125), addr(60126)]));
        settle().await;
        assert_eq!(harness.publishes(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_back_off_whatever_triggers_them() {
        let harness = Harness::start(false);
        harness.failing.store(true, Ordering::SeqCst);
        harness.active.send_replace(true);
        settle().await;
        assert_eq!(harness.publishes(), 1);
        assert!(!harness.keeper.is_live(Some(addr(1)), Instant::now()));
        // Announcements keep reporting, but only the retry timer tries again.
        harness.move_to(2);
        for _ in 0..10 {
            harness.keeper.report(Some(addr(2)));
            settle().await;
        }
        assert_eq!(harness.publishes(), 1);
        tokio::time::advance(RETRY).await;
        settle().await;
        assert_eq!(harness.publishes(), 2);
        harness.failing.store(false, Ordering::SeqCst);
        tokio::time::advance(RETRY).await;
        settle().await;
        assert_eq!(harness.publishes(), 3);
        assert!(harness.keeper.is_live(Some(addr(2)), Instant::now()));
    }

    /// Also covers failures after the refresh is due, where a keeper that
    /// woke for the overdue refresh instead of the retry would spin forever.
    #[tokio::test(start_paused = true)]
    async fn a_record_expires_without_refreshes() {
        let harness = Harness::start(true);
        settle().await;
        harness.failing.store(true, Ordering::SeqCst);
        let published = Instant::now();
        tokio::time::advance(INDEX_TTL - Duration::from_secs(1)).await;
        settle().await;
        // Refreshes failed, but the servers still hold the record.
        assert!(harness.publishes() > 1);
        assert!(harness.keeper.is_live(Some(addr(1)), Instant::now()));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!harness.keeper.is_live(Some(addr(1)), published + INDEX_TTL));
    }

    /// Counts the fake DHT operations of one announcement.
    #[derive(Default)]
    struct Dht {
        address: Mutex<Option<SocketAddrV4>>,
        lookups: AtomicUsize,
        announces: AtomicUsize,
    }

    fn spawn_announce(keeper: Arc<IndexKeeper>, dht: Arc<Dht>) -> task::JoinHandle<Result<()>> {
        task::spawn(async move {
            announce_indexed(
                &keeper,
                || async {},
                || async {
                    dht.lookups.fetch_add(1, Ordering::SeqCst);
                    Ok(*dht.address.lock().unwrap())
                },
                || async {
                    dht.announces.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
        })
    }

    fn publish(keeper: &IndexKeeper, public: SocketAddrV4) {
        keeper.record.send_replace(Some(Record {
            public: Some(public),
            mapping: public,
            at: Instant::now(),
        }));
    }

    #[tokio::test(start_paused = true)]
    async fn announces_right_away_with_a_live_record() {
        let keeper = Arc::new(IndexKeeper::default());
        let dht = Arc::new(Dht::default());
        *dht.address.lock().unwrap() = Some(addr(1));
        publish(&keeper, addr(1));
        spawn_announce(keeper, dht.clone()).await.unwrap().unwrap();
        assert_eq!(dht.lookups.load(Ordering::SeqCst), 1);
        assert_eq!(dht.announces.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn waits_without_polling_then_restarts_within_the_jitter() {
        let keeper = Arc::new(IndexKeeper::default());
        let dht = Arc::new(Dht::default());
        *dht.address.lock().unwrap() = Some(addr(1));
        let task = spawn_announce(keeper.clone(), dht.clone());
        // No index server holds a record, however long we wait.
        tokio::time::advance(INDEX_TTL).await;
        settle().await;
        assert_eq!(dht.lookups.load(Ordering::SeqCst), 1);
        assert_eq!(dht.announces.load(Ordering::SeqCst), 0);
        publish(&keeper, addr(1));
        let published = Instant::now();
        let result = task.await.unwrap();
        result.unwrap();
        assert!(published.elapsed() <= RESTART_JITTER);
        // It started over, finding the closest nodes again.
        assert_eq!(dht.lookups.load(Ordering::SeqCst), 2);
        assert_eq!(dht.announces.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_address_waits_for_its_record() {
        let keeper = Arc::new(IndexKeeper::default());
        let dht = Arc::new(Dht::default());
        publish(&keeper, addr(1));
        // Mainline now sees us elsewhere, which the record is not for.
        *dht.address.lock().unwrap() = Some(addr(2));
        let task = spawn_announce(keeper.clone(), dht.clone());
        settle().await;
        assert_eq!(dht.announces.load(Ordering::SeqCst), 0);
        // The report asked the keeper to republish.
        tokio::time::timeout(Duration::from_millis(1), keeper.reported.notified())
            .await
            .expect("address change was not reported");
        publish(&keeper, addr(2));
        let published = Instant::now();
        task.await.unwrap().unwrap();
        // The republish came quickly, so there was no outage to spread out.
        assert!(published.elapsed() < Duration::from_millis(10));
        assert_eq!(dht.announces.load(Ordering::SeqCst), 1);
        assert_eq!(dht.lookups.load(Ordering::SeqCst), 2);
    }
}
