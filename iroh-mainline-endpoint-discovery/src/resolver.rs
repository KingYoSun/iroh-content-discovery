//! Resolution of a Mainline infohash to [`EndpointId`]s.
//!
//! Peers come from `get_peers`, and each peer's address is looked up in the
//! address index to recover the endpoint identity that published it. The
//! infohash is opaque to this crate: BLAKE3 content is hashed to `SHA-1` at
//! the call site.

use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    net::SocketAddrV4,
};

use iroh_base::EndpointId;
use n0_error::{Result, StackResultExt};
use n0_future::{FuturesUnordered, Stream, StreamExt, stream};
use n0_mainline::{Dht, Id};
use tokio::task::JoinSet;

use crate::{AddrIndex, AddrIndexError, SignedRecord};
use tracing::debug;

const MAX_INDEX_LOOKUPS: usize = 16;
const MAX_QUEUED_PEERS: usize = 64;

/// Looks up who announced a Mainline infohash.
#[derive(Debug, Clone)]
pub struct Resolver {
    dht: Dht,
    index: AddrIndex,
}

impl Resolver {
    /// Returns the shared Mainline node used for lookups.
    pub fn dht(&self) -> &Dht {
        &self.dht
    }

    /// Returns the address index peers are looked up in.
    pub fn index(&self) -> &AddrIndex {
        &self.index
    }

    /// Creates a resolver from a shared Mainline node and an address index.
    pub fn new(dht: Dht, index: AddrIndex) -> Self {
        Self { dht, index }
    }

    /// Yields endpoint IDs as Mainline peers and index records arrive.
    ///
    /// The stream is lazy: the Mainline lookup starts when the first item is
    /// requested, so a caller that gets what it needs elsewhere, e.g. from
    /// providers it already knows, can chain this behind them at no cost.
    /// Up to 16 index lookups run concurrently, so a failed or slow peer does
    /// not delay other results. Lookup failures are logged and skipped, and a
    /// lookup that cannot start, because the Mainline node is gone, ends the
    /// stream. Dropping the stream cancels its pending lookups. The caller
    /// should impose a deadline.
    pub fn resolve_stream(&self, infohash: Id) -> stream::Boxed<EndpointId> {
        let dht = self.dht.clone();
        let index = self.index.clone();
        providers(
            async move {
                debug!(%infohash, "starting Mainline provider stream");
                dht.get_peers(infohash)
                    .await
                    .inspect_err(|err| debug!(%infohash, %err, "Mainline lookup could not start"))
                    .ok()
            },
            move |peer| {
                let index = index.clone();
                async move { index.lookup(peer).await }
            },
        )
    }

    /// Keeps looking for providers until the consumer drops the stream.
    ///
    /// A new Mainline lookup starts when the consumer asks for another item
    /// after the previous lookup ends. Results may include duplicate IDs.
    /// The stream ends when the Mainline node is gone, which is the only
    /// reason a lookup cannot start. Consumers should set their own deadline
    /// for a bounded operation.
    pub fn resolve_continuously(&self, infohash: Id) -> stream::Boxed<EndpointId> {
        let resolver = self.clone();
        let stream = async_stream::stream! {
            loop {
                let mut found = resolver.resolve_stream(infohash);
                while let Some(id) = found.next().await {
                    yield id;
                }
                // A gone Mainline node does not come back, and its lookups end
                // at once, so retrying would spin this loop forever.
                if let Err(err) = resolver.dht.info().await {
                    debug!(%infohash, %err, "Mainline node is gone, ending provider stream");
                    break;
                }
                tokio::task::yield_now().await;
            }
        };
        stream.boxed()
    }

    /// Runs `get_peers` for `infohash`, then index-resolves each compact peer.
    ///
    /// Returns unique endpoint IDs, sorted, and nothing at all if the DHT has
    /// no peers or none of them are in the index. Returned endpoint IDs are
    /// dialed through normal iroh discovery, not through the DHT address.
    pub async fn resolve(&self, infohash: Id) -> Result<Vec<EndpointId>> {
        let mut stream = self.dht.get_peers(infohash).await.context("get_peers")?;
        let mut peers = HashSet::new();
        while let Some(batch) = stream.next().await {
            peers.extend(batch);
        }
        debug!(n_peers = peers.len(), "get_peers");

        let mut set = JoinSet::new();
        for peer in peers {
            let index = self.index.clone();
            set.spawn(async move { (peer, index.lookup(peer).await) });
        }

        let mut endpoint_ids = HashSet::new();
        let mut any_ok = false;
        let mut last_err = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, Ok(records))) => {
                    any_ok = true;
                    endpoint_ids.extend(records.into_iter().map(|r| r.endpoint_id));
                }
                Ok((peer, Err(err))) => {
                    debug!(%peer, %err, "index resolve");
                    last_err = Some(err);
                }
                Err(err) => debug!(%err, "index resolve join"),
            }
        }
        if !any_ok && let Some(err) = last_err {
            return Err(err.into());
        }
        let mut out: Vec<_> = endpoint_ids.into_iter().collect();
        out.sort();
        Ok(out)
    }
}

/// Yields the endpoints that `lookup` finds for the peers `peers` yields, as
/// [`Resolver::resolve_stream`] describes. `peers` runs on the first poll and
/// gives `None` when the lookup cannot start.
fn providers<P, S, L, F>(peers: P, lookup: L) -> stream::Boxed<EndpointId>
where
    P: Future<Output = Option<S>> + Send + 'static,
    S: Stream<Item = Vec<SocketAddrV4>> + Send + Unpin + 'static,
    L: Fn(SocketAddrV4) -> F + Send + 'static,
    F: Future<Output = Result<Vec<SignedRecord>, AddrIndexError>> + Send + 'static,
{
    let stream = async_stream::stream! {
        let Some(mut peers) = peers.await else { return };
        let mut pending_peers = VecDeque::new();
        let mut lookups = FuturesUnordered::new();
        let mut peers_done = false;
        loop {
            while lookups.len() < MAX_INDEX_LOOKUPS {
                let Some(peer) = pending_peers.pop_front() else { break };
                let lookup = lookup(peer);
                lookups.push(async move { (peer, lookup.await) });
            }
            if peers_done && pending_peers.is_empty() && lookups.is_empty() {
                break;
            }
            let poll_peers = !peers_done && pending_peers.len() < MAX_QUEUED_PEERS;
            let poll_lookups = !lookups.is_empty();
            tokio::select! {
                batch = peers.next(), if poll_peers => match batch {
                    Some(batch) => {
                        debug!(count = batch.len(), "Mainline peers received");
                        pending_peers.extend(batch);
                    },
                    None => peers_done = true,
                },
                result = lookups.next(), if poll_lookups => {
                    if let Some((peer, result)) = result {
                        match result {
                            Ok(records) => for record in records {
                                debug!(%peer, endpoint = %record.endpoint_id, "discovered content provider");
                                yield record.endpoint_id;
                            },
                            Err(err) => debug!(%peer, %err, "index resolve"),
                        }
                    }
                }
            }
        }
    };
    stream.boxed()
}

#[cfg(test)]
mod tests {
    use std::{
        net::Ipv4Addr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use iroh_base::SecretKey;

    use super::*;

    /// Counts the index lookups of a provider stream, and how many ran at once.
    #[derive(Default)]
    struct Lookups {
        started: AtomicUsize,
        running: AtomicUsize,
        most: AtomicUsize,
    }

    /// Feeds `peers` in batches of `batch` to a provider stream whose lookups
    /// all find a record after a moment, and reads up to `prefix` endpoints.
    /// Returns the lookups, how many batches the stream took, and the endpoints.
    async fn read_prefix(
        peers: usize,
        batch: usize,
        prefix: usize,
    ) -> (Arc<Lookups>, usize, usize) {
        let peers: Vec<_> = (0..peers)
            .map(|port| SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 7), port as u16))
            .collect();
        let taken = Arc::new(AtomicUsize::new(0));
        let batches = {
            let taken = taken.clone();
            stream::iter(peers.chunks(batch).map(<[_]>::to_vec).collect::<Vec<_>>()).inspect(
                move |_| {
                    taken.fetch_add(1, Ordering::SeqCst);
                },
            )
        };
        let lookups = Arc::new(Lookups::default());
        let secret = SecretKey::generate();
        let counted = lookups.clone();
        let found = providers(async move { Some(batches) }, move |peer| {
            counted.started.fetch_add(1, Ordering::SeqCst);
            let running = counted.running.fetch_add(1, Ordering::SeqCst) + 1;
            counted.most.fetch_max(running, Ordering::SeqCst);
            let counted = counted.clone();
            let record = SignedRecord::sign(&secret, peer);
            async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                counted.running.fetch_sub(1, Ordering::SeqCst);
                Ok(vec![record])
            }
        })
        .take(prefix)
        .collect::<Vec<_>>()
        .await;
        (lookups, taken.load(Ordering::SeqCst), found.len())
    }

    /// Reading a prefix costs the same, however many providers there are.
    #[tokio::test]
    async fn a_prefix_takes_the_same_work_for_any_number_of_providers() {
        for providers in [20, 200, 2000] {
            // Mainline nodes answer up to 20 peers at a time.
            let (lookups, batches, found) = read_prefix(providers, 20, 4).await;
            assert_eq!(found, 4);
            // The first 16 lookups, and one more after each of the first three results.
            let started = lookups.started.load(Ordering::SeqCst);
            assert!(started <= 19, "{providers} providers: {started} lookups");
            assert!(lookups.most.load(Ordering::SeqCst) <= MAX_INDEX_LOOKUPS);
            // A batch is taken only while fewer than 64 peers wait, so at most
            // 19 looked up and 83 waiting, which is less than six batches.
            assert!(batches <= 5, "{providers} providers: {batches} batches");
        }
    }

    #[tokio::test]
    async fn known_providers_chained_first_need_no_lookup() {
        // Nobody answers at the only bootstrap node, so a started lookup would
        // take seconds to give up.
        let dht = Dht::builder()
            .bootstrap(&["127.0.0.1:9"])
            .port(0)
            .build()
            .unwrap();
        let index = AddrIndex::udp(dht.clone(), "127.0.0.1:9".parse().unwrap())
            .await
            .unwrap();
        let resolver = Resolver::new(dht, index);
        let known = SecretKey::generate().public();
        let infohash = Id::from([7; 20]);
        let mut providers = stream::iter([known]).chain(resolver.resolve_stream(infohash));
        let first = tokio::time::timeout(Duration::from_millis(100), providers.next())
            .await
            .expect("the known provider waited for a lookup");
        assert_eq!(first, Some(known));
    }
}
