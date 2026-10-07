//! Announce Mainline infohashes one at a time, when the caller asks.

use std::sync::Arc;

use iroh_base::{EndpointId, SecretKey};
use n0_error::{Result, StackResultExt};
use n0_future::task::{self, AbortOnDropHandle};
use n0_mainline::{Dht, Id};
use tokio::{sync::watch, time::Instant};

use crate::{
    AddrIndex,
    index_keeper::IndexKeeper,
    publisher::{public_address, publish_index},
};

/// Announces infohashes one at a time, when the caller asks.
///
/// Unlike [`Publisher`](crate::Publisher), it holds no set of infohashes and
/// runs no timer per infohash: the caller decides what to announce and when,
/// and renews each announcement every [`REFRESH`](crate::REFRESH). It keeps
/// the endpoint's index record current, as the publisher does, from the first
/// announcement until it is dropped.
#[derive(Debug, Clone)]
pub struct Announcer {
    state: Arc<State>,
    _task: Arc<AbortOnDropHandle<()>>,
}

#[derive(Debug)]
struct State {
    secret: SecretKey,
    dht: Dht,
    index: AddrIndex,
    keeper: IndexKeeper,
    /// Whether an announcement was asked for, which the keeper publishes for.
    active: watch::Sender<bool>,
}

impl Announcer {
    /// Creates an announcer from a secret key, a Mainline node, and an address index.
    pub fn new(secret: SecretKey, dht: Dht, index: AddrIndex) -> Self {
        let state = Arc::new(State {
            secret,
            dht,
            index,
            keeper: IndexKeeper::default(),
            active: watch::channel(false).0,
        });
        let task = task::spawn({
            let state = state.clone();
            async move {
                state
                    .keeper
                    .run(
                        state.active.subscribe(),
                        state.index.servers(),
                        || async {
                            // Bootstrapping tells Mainline our address, so the first
                            // record is published for the address announcements will see.
                            let _ = state.dht.bootstrapped().await;
                            public_address(&state.dht).await
                        },
                        || publish_index(&state.index, &state.secret),
                    )
                    .await
            }
        });
        Self {
            state,
            _task: Arc::new(AbortOnDropHandle::new(task)),
        }
    }

    /// Returns the endpoint identity announced by this instance.
    pub fn id(&self) -> EndpointId {
        self.state.secret.public()
    }

    /// Announces `infohash` once.
    ///
    /// Fails without announcing while no index server holds our record for
    /// the address Mainline sees us at, since readers could not resolve the
    /// announcement. The first call starts keeping the record, so a later
    /// attempt can succeed; [`RETRY`](crate::RETRY) is a suitable delay.
    pub async fn announce(&self, infohash: Id) -> Result<()> {
        let state = &self.state;
        state
            .active
            .send_if_modified(|active| !std::mem::replace(active, true));
        // Finding the closest nodes primes their tokens for the announce, and
        // their replies tell us the address the announce will store.
        state
            .dht
            .get_closest_nodes(infohash)
            .await
            .with_context(|_| format!("get_closest_nodes for {infohash}"))?;
        let public = public_address(&state.dht).await;
        state.keeper.report(public);
        if !state.keeper.is_live(public, Instant::now()) {
            n0_error::bail_any!("no index server holds our record for {public:?} yet");
        }
        state
            .dht
            .announce_peer(infohash, None)
            .await
            .with_context(|_| format!("announce_peer infohash {infohash}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        time::Duration,
    };

    use n0_future::StreamExt;
    use n0_mainline::Testnet;
    use udp_addr_index::{Limits, Server};

    use super::*;
    use crate::Resolver;

    fn node(network: &Testnet) -> Dht {
        Dht::builder()
            .bootstrap(&network.bootstrap)
            .port(0)
            .build()
            .unwrap()
    }

    /// Announcements the caller asks for are found through the index, and
    /// leave no task behind per infohash.
    #[tokio::test]
    async fn announcements_are_found_through_the_index() {
        let network = Testnet::new(3).await.unwrap();
        let server = Server::new(Limits::for_tests());
        let handle = server.attach(node(&network)).await.unwrap();
        let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, handle.local_addr().port());
        let dht = node(&network);
        let index = AddrIndex::udp(dht.clone(), server_addr).await.unwrap();
        let announcer = Announcer::new(SecretKey::generate(), dht, index);
        let infohash = Id::random();
        // Announcing fails until the first record is published.
        tokio::time::timeout(Duration::from_secs(10), async {
            while announcer.announce(infohash).await.is_err() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("announced in time");

        let reader = node(&network);
        let resolver = Resolver::new(
            reader.clone(),
            AddrIndex::udp(reader, server_addr).await.unwrap(),
        );
        let found = tokio::time::timeout(
            Duration::from_secs(10),
            resolver.resolve_stream(infohash).next(),
        )
        .await
        .expect("resolved in time");
        assert_eq!(found, Some(announcer.id()));

        let tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        for _ in 0..20 {
            announcer.announce(Id::random()).await.unwrap();
        }
        assert_eq!(
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks(),
            tasks
        );
    }

    /// Without a record on an index server, an announcement fails at once
    /// instead of waiting for one.
    #[tokio::test]
    async fn an_announcement_without_an_index_record_fails_at_once() {
        let network = Testnet::new(3).await.unwrap();
        // An index server that never answers.
        let silent = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let silent_addr =
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, silent.local_addr().unwrap().port());
        let dht = node(&network);
        let index = AddrIndex::udp(dht.clone(), silent_addr).await.unwrap();
        let announcer = Announcer::new(SecretKey::generate(), dht, index);

        let result =
            tokio::time::timeout(Duration::from_secs(10), announcer.announce(Id::random())).await;
        assert!(
            matches!(result, Ok(Err(_))),
            "the announcement waited for a record"
        );
    }
}
