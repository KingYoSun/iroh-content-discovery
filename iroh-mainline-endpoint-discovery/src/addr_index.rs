//! Convenience wrapper around the UDP index client.

use n0_error::e;
use n0_future::StreamExt;
use n0_future::task::{self, AbortOnDropHandle};
use n0_mainline::Dht;
use std::{
    collections::{BTreeSet, HashSet},
    net::SocketAddrV4,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, watch};

use iroh_base::SecretKey;

use crate::{SignedRecord, UdpClient, UdpError, lookup_cache::LookupCache, udp::DEFAULT_TIMEOUT};
use tracing::{debug, info, warn};

/// Pkarr key of the index server list maintained by n0.
///
/// [`AddrIndexBuilder::n0_defaults`] discovers servers from this list.
///
/// Its z-base-32 name is `z6rb8uoy1pwuckhw8qx8i4qseczujxw4qakpe7xng3yi68wpyrqo`.
pub const DEFAULT_INDEX_LIST_KEY: [u8; 32] = [
    191, 136, 19, 206, 0, 147, 105, 54, 43, 148, 59, 158, 122, 233, 214, 67, 47, 52, 190, 154, 118,
    20, 212, 117, 226, 54, 65, 95, 30, 141, 1, 29,
];

/// How often discovery looks for the current index servers.
const DISCOVERY_REFRESH: Duration = Duration::from_secs(10 * 60);
/// Delay before retrying a failed discovery; the previous servers stay in use.
const DISCOVERY_RETRY: Duration = Duration::from_secs(30);

/// How long [`AddrIndexBuilder::n0_defaults`] remembers lookups.
pub const DEFAULT_LOOKUP_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

/// Configures where an [`AddrIndex`] finds its servers.
///
/// Starts empty; [`Self::n0_defaults`] uses the servers maintained by n0.
/// Explicit servers are used as given and skip discovery. Otherwise a trusted
/// Pkarr list is tried first, and the rendezvous hash only when the list
/// yields nothing. With no source at all, [`Self::build`] fails with
/// [`UdpError::NoServers`].
#[derive(Debug, Clone)]
pub struct AddrIndexBuilder {
    dht: Dht,
    servers: HashSet<SocketAddrV4>,
    sources: Sources,
    timeout: Duration,
    lookup_cache: Option<Duration>,
}

/// Discovery sources, tried in order.
#[derive(Debug, Clone, Default)]
struct Sources {
    list_key: Option<[u8; 32]>,
    fallback_list: Option<n0_mainline::MutableItem>,
    rendezvous_hash: Option<[u8; 20]>,
}

impl AddrIndexBuilder {
    /// Uses `server` directly, skipping discovery. Can be called repeatedly.
    pub fn server(mut self, server: SocketAddrV4) -> Self {
        self.servers.insert(server);
        self
    }

    /// Uses the servers listed by n0 and caches lookups.
    ///
    /// Sets [`DEFAULT_INDEX_LIST_KEY`] and [`DEFAULT_LOOKUP_CACHE_TTL`].
    pub fn n0_defaults(self) -> Self {
        self.list_key(DEFAULT_INDEX_LIST_KEY)
            .lookup_cache(DEFAULT_LOOKUP_CACHE_TTL)
    }

    /// Discovers servers from the Pkarr list signed by `key`.
    pub fn list_key(mut self, key: [u8; 32]) -> Self {
        self.sources.list_key = Some(key);
        self
    }

    /// Uses a previously stored signed list when the Pkarr lookup finds none.
    ///
    /// Only a list signed by the [`Self::list_key`] is used. Store the result
    /// of [`AddrIndex::signed_list`] to get one, so a record that stops
    /// resolving does not take discovery down with it.
    pub fn fallback_list(mut self, item: n0_mainline::MutableItem) -> Self {
        self.sources.fallback_list = Some(item);
        self
    }

    /// Falls back to servers announced under `hash` when no signed list is found.
    ///
    /// Announcements are untrusted: anyone can announce, so the candidates
    /// may be unreachable or dishonest.
    pub fn rendezvous_hash(mut self, hash: [u8; 20]) -> Self {
        self.sources.rendezvous_hash = Some(hash);
        self
    }

    /// Remembers the endpoints found at a socket for `ttl`.
    ///
    /// Sockets without a record are remembered for at most thirty seconds, so
    /// new publishers show up quickly, and failed lookups are not remembered.
    /// Concurrent lookups of one socket share a request. Off by default.
    pub fn lookup_cache(mut self, ttl: Duration) -> Self {
        self.lookup_cache = Some(ttl);
        self
    }

    /// Sets the deadline for each publish or lookup, [`DEFAULT_TIMEOUT`] by default.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attaches to the DHT socket and, unless servers were given, discovers them.
    ///
    /// Fails if the first discovery finds no servers. Discovery then repeats
    /// in the background every ten minutes, while the index or a clone of it
    /// is alive; a failed refresh keeps the previous servers and retries after
    /// thirty seconds. It keeps at most two servers, gives each lookup thirty
    /// seconds, and retains the highest signed sequence for the index's
    /// lifetime. Addresses are candidates; they do not prove availability.
    pub async fn build(self) -> Result<AddrIndex, UdpError> {
        debug!(servers = ?self.servers, sources = ?self.sources, "configuring index server discovery");
        let cache = self.lookup_cache.map(|ttl| Arc::new(LookupCache::new(ttl)));
        if !self.servers.is_empty() {
            let client = UdpClient::attach_with_timeout(self.dht, self.timeout).await?;
            client.replace_servers(self.servers.clone()).await?;
            // The servers never change, so the sender can go.
            let (_, servers) = watch::channel(self.servers.into_iter().collect());
            return Ok(AddrIndex {
                client,
                discovery: None,
                cache,
                servers,
                _refresh: None,
            });
        }
        if self.sources.list_key.is_none() && self.sources.rendezvous_hash.is_none() {
            return Err(e!(UdpError::NoServers));
        }
        let client = UdpClient::attach_with_timeout(self.dht.clone(), self.timeout).await?;
        let discovery = Arc::new(Discovery {
            dht: self.dht,
            sources: self.sources,
            state: Mutex::new(DiscoveryState::default()),
        });
        let (servers_tx, servers) = watch::channel(BTreeSet::new());
        discovery.refresh(&client, &servers_tx).await?;
        if servers.borrow().is_empty() {
            return Err(e!(UdpError::NoServers));
        }
        let refresh = task::spawn({
            let discovery = discovery.clone();
            let client = client.clone();
            async move {
                loop {
                    tokio::time::sleep(DISCOVERY_REFRESH).await;
                    while let Err(err) = discovery.refresh(&client, &servers_tx).await {
                        warn!(%err, "index server discovery failed; keeping the previous servers");
                        tokio::time::sleep(DISCOVERY_RETRY).await;
                    }
                }
            }
        });
        Ok(AddrIndex {
            client,
            discovery: Some(discovery),
            cache,
            servers,
            _refresh: Some(Arc::new(AbortOnDropHandle::new(refresh))),
        })
    }
}

/// UDP client for one or more index servers.
#[derive(Debug, Clone)]
pub struct AddrIndex {
    client: UdpClient,
    discovery: Option<Arc<Discovery>>,
    cache: Option<Arc<LookupCache>>,
    /// Servers in use, updated by each discovery refresh.
    servers: watch::Receiver<BTreeSet<SocketAddrV4>>,
    /// Background discovery, stopped when the last clone is dropped.
    _refresh: Option<Arc<AbortOnDropHandle<()>>>,
}

#[derive(Debug)]
struct Discovery {
    dht: Dht,
    sources: Sources,
    state: Mutex<DiscoveryState>,
}

#[derive(Debug, Default)]
struct DiscoveryState {
    signed: Option<n0_mainline::MutableItem>,
}

impl DiscoveryState {
    fn accept(&mut self, item: n0_mainline::MutableItem) {
        if item.salt().is_some()
            || item.seq() < 0
            || crate::ServerList::decode(item.value(), item.key()).is_none()
        {
            return;
        }
        if self
            .signed
            .as_ref()
            .is_none_or(|old| (item.seq(), item.value()) > (old.seq(), old.value()))
        {
            self.signed = Some(item);
        }
    }
}

impl Discovery {
    /// Finds the current servers and hands them to `client`.
    ///
    /// Tries the signed list, then the stored copy, then rendezvous. On
    /// failure, the servers in use stay unchanged.
    async fn refresh(
        &self,
        client: &UdpClient,
        servers: &watch::Sender<BTreeSet<SocketAddrV4>>,
    ) -> Result<(), UdpError> {
        // A lookup while the node is still bootstrapping has barely any peers
        // to ask, ends at once without an answer, and would report no servers.
        // Once bootstrapped, this returns immediately.
        let bootstrapped = self.dht.bootstrapped().await?;
        if !bootstrapped {
            debug!("Mainline bootstrap failed; discovering index servers anyway");
        }
        let mut items = Vec::new();
        let signed_result = match self.sources.list_key {
            Some(key) => match self.dht.get_mutable(&key, None, None).await {
                Ok(stream) => collect_within(stream, Duration::from_secs(30), &mut items)
                    .await
                    .map_err(|_| e!(UdpError::Timeout)),
                Err(err) => Err(err.into()),
            },
            None => Ok(()),
        };
        let received = items.len();
        let mut state = self.state.lock().await;
        // Answers that arrived before a timeout count as well.
        for item in items {
            state.accept(item);
        }
        if state.signed.is_none()
            && let Some(item) = &self.sources.fallback_list
            && self.sources.list_key.as_ref() == Some(item.key())
        {
            debug!(
                sequence = item.seq(),
                "signed index list not found; using the stored copy"
            );
            state.accept(item.clone());
        }
        debug!(
            received,
            ?signed_result,
            "signed index-list lookup completed"
        );
        let signed_list = state
            .signed
            .as_ref()
            .and_then(|item| crate::ServerList::decode(item.value(), item.key()));
        drop(state);
        // A signed empty list is an answer: the authority withdrew its servers.
        let withdrawn = signed_list
            .as_ref()
            .is_some_and(|list| list.addresses().is_empty());
        let mut peers = signed_list
            .map(|list| list.addresses().iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        if peers.is_empty() {
            if let Some(hash) = self.sources.rendezvous_hash {
                debug!(infohash = %crate::infohash_hex(&hash), "discovering index servers through Mainline rendezvous");
                let lookup = async {
                    let mut stream = self.dht.get_peers(hash.into()).await?;
                    while let Some(batch) = stream.next().await {
                        for peer in batch {
                            if peer.port() != 0
                                && !peer.ip().is_unspecified()
                                && !peer.ip().is_multicast()
                                && !peer.ip().is_broadcast()
                            {
                                peers.insert(peer);
                                if peers.len() == 2 {
                                    return Ok::<_, UdpError>(());
                                }
                            }
                        }
                    }
                    Ok(())
                };
                let rendezvous = match tokio::time::timeout(Duration::from_secs(30), lookup).await {
                    Ok(result) => result,
                    Err(_) if peers.is_empty() => Err(e!(UdpError::Timeout)),
                    Err(_) => Ok(()),
                };
                // After a withdrawal, the withdrawn servers are not kept either.
                if !withdrawn {
                    rendezvous?;
                }
            } else if !withdrawn {
                signed_result?;
            }
        }
        if peers.is_empty() && !withdrawn {
            debug!("index server discovery found no servers");
            return Err(e!(UdpError::NoServers));
        }
        debug!(?peers, "using discovered index servers");
        client.replace_servers(peers.clone()).await?;
        let peers: BTreeSet<_> = peers.into_iter().collect();
        servers.send_if_modified(|current| {
            let changed = *current != peers;
            if changed {
                if peers.is_empty() {
                    info!("the signed list withdrew all index servers");
                } else {
                    info!(servers = ?peers, "index servers changed");
                }
                *current = peers;
            }
            changed
        });
        Ok(())
    }
}

/// Collects items until `stream` ends or `limit` passes.
///
/// Items that arrived before a timeout stay in `items`.
async fn collect_within<T>(
    stream: impl n0_future::Stream<Item = T>,
    limit: Duration,
    items: &mut Vec<T>,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(limit, async {
        let mut stream = std::pin::pin!(stream);
        while let Some(item) = stream.next().await {
            items.push(item);
        }
    })
    .await
}

impl AddrIndex {
    /// Starts configuring an index on a Mainline node's UDP socket, with no servers.
    pub fn builder(dht: Dht) -> AddrIndexBuilder {
        AddrIndexBuilder {
            dht,
            servers: HashSet::new(),
            sources: Sources::default(),
            timeout: DEFAULT_TIMEOUT,
            lookup_cache: None,
        }
    }

    /// Uses one server directly, without discovery.
    pub async fn udp(dht: Dht, server: SocketAddrV4) -> Result<Self, UdpError> {
        Self::builder(dht).server(server).build().await
    }

    /// Discovers servers with [`AddrIndexBuilder::n0_defaults`].
    pub async fn discover(dht: Dht) -> Result<Self, UdpError> {
        Self::builder(dht).n0_defaults().build().await
    }

    /// Returns the servers in use, which change when discovery finds others.
    pub(crate) fn servers(&self) -> watch::Receiver<BTreeSet<SocketAddrV4>> {
        self.servers.clone()
    }

    /// Returns the signed server list in use, if discovery found one.
    ///
    /// Pass it to [`AddrIndexBuilder::fallback_list`] on the next start.
    pub async fn signed_list(&self) -> Option<n0_mainline::MutableItem> {
        let discovery = self.discovery.as_ref()?;
        discovery.state.lock().await.signed.clone()
    }

    /// Wraps an existing UDP client.
    pub fn from_udp(client: UdpClient) -> Self {
        Self {
            client,
            discovery: None,
            cache: None,
            servers: watch::channel(BTreeSet::new()).1,
            _refresh: None,
        }
    }

    /// Publishes a record for `secret`'s endpoint to all responsive servers.
    ///
    /// Each server gets a record signed for the socket it observed, so a
    /// reader can tell a record published here from one copied out of another
    /// publisher's slot.
    ///
    /// Returns the public UDP sockets under which servers stored it.
    pub async fn publish(&self, secret: &SecretKey) -> Result<Vec<SocketAddrV4>, AddrIndexError> {
        let secret = secret.clone();
        self.client
            .publish(move |addr| SignedRecord::sign(&secret, addr).encode())
            .await
            .map_err(Into::into)
    }

    /// Looks up the endpoints that listed `addr`.
    ///
    /// Records that were not signed for `addr` are discarded, so a record
    /// republished under another socket is not returned. With
    /// [`AddrIndexBuilder::lookup_cache`], a recent result may be returned
    /// without asking the servers.
    pub async fn lookup(&self, addr: SocketAddrV4) -> Result<Vec<SignedRecord>, AddrIndexError> {
        let Some(cache) = &self.cache else {
            return self.lookup_uncached(addr).await;
        };
        if let Some(records) = cache.get(addr) {
            debug!(%addr, records = records.len(), "index lookup cache hit");
            return Ok(records);
        }
        let pending = cache.pending(addr);
        let _pending = pending.lock().await;
        // A concurrent caller may have finished the same lookup while we waited.
        if let Some(records) = cache.get(addr) {
            return Ok(records);
        }
        let records = self.lookup_uncached(addr).await?;
        cache.insert(addr, records.clone());
        Ok(records)
    }

    /// Like [`Self::lookup`], but always asks the servers, even with a lookup cache.
    pub async fn lookup_uncached(
        &self,
        addr: SocketAddrV4,
    ) -> Result<Vec<SignedRecord>, AddrIndexError> {
        // A valid record is authenticated and bound to `addr`, so the first
        // one is good enough. A miss still waits for every server.
        let result = self
            .client
            .resolve_first(
                addr,
                Box::new(move |value| SignedRecord::decode(value, addr).is_some()),
            )
            .await?;
        let received = result.values.len();
        let records: Vec<_> = result
            .values
            .into_iter()
            .filter_map(|value| SignedRecord::decode(&value, addr))
            .collect();
        debug!(%addr, received, valid = records.len(), "validated index server endpoint records");
        Ok(records)
    }
}

impl From<UdpClient> for AddrIndex {
    fn from(value: UdpClient) -> Self {
        Self::from_udp(value)
    }
}

/// Error from [`AddrIndex::publish`] or [`AddrIndex::lookup`].
#[n0_error::stack_error(derive, add_meta)]
pub enum AddrIndexError {
    /// UDP transport failed.
    #[error(transparent)]
    Udp {
        /// Underlying UDP client error.
        #[error(from, source)]
        source: UdpError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServerList;

    #[tokio::test]
    async fn curated_list_takes_precedence_over_rendezvous() {
        use simple_dns::{
            CLASS, Packet, ResourceRecord,
            rdata::{RData, TXT},
        };
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let key = n0_mainline::SigningKey::from_bytes(&[43; 32]);
        let public = key.verifying_key().to_bytes();
        let owner = crate::pkarr_name(&public);
        // The same apex TXT records entered in iroh-share's DNS editor.
        let mut packet = Packet::new_reply(0);
        for socket in ["203.0.113.1:60125", "198.51.100.2:33445"] {
            packet.answers.push(ResourceRecord::new(
                owner.as_str().try_into().unwrap(),
                CLASS::IN,
                300,
                RData::TXT(TXT::try_from(socket).unwrap()),
            ));
        }
        let publisher = crate::PkarrPublisher::new(node());
        publisher.set_raw(&key, &packet).unwrap();
        publisher.publish_all().await.unwrap();
        let index = AddrIndex::builder(node())
            .list_key(public)
            .rendezvous_hash([42; 20])
            .build()
            .await
            .unwrap();
        let state = index.discovery.as_ref().unwrap().state.lock().await;
        let item = state
            .signed
            .as_ref()
            .expect("curated Pkarr packet was not discovered");
        assert_eq!(item.salt(), None);
        assert_eq!(
            ServerList::decode(item.value(), item.key())
                .unwrap()
                .addresses(),
            &[
                "203.0.113.1:60125".parse::<SocketAddrV4>().unwrap(),
                "198.51.100.2:33445".parse().unwrap(),
            ]
        );
    }

    #[tokio::test]
    async fn no_sources_fails_without_attaching_the_client() {
        let dht = Dht::builder().no_bootstrap().port(0).build().unwrap();
        let result = AddrIndex::builder(dht.clone()).build().await;
        assert!(matches!(result, Err(UdpError::NoServers { .. })));
        // The failed configuration did not consume the socket's datagram hook.
        assert!(
            AddrIndex::udp(dht, "127.0.0.1:60125".parse().unwrap())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn empty_curated_list_falls_back_to_rendezvous() {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let authority = node();
        let key = n0_mainline::SigningKey::from_bytes(&[44; 32]);
        authority
            .put_mutable(
                ServerList::new(vec![]).unwrap().sign(&key, 1).unwrap(),
                None,
            )
            .await
            .unwrap();
        let hash = [45; 20];
        authority
            .announce_peer(hash.into(), Some(60125))
            .await
            .unwrap();
        let index = AddrIndex::builder(node())
            .list_key(key.verifying_key().to_bytes())
            .rendezvous_hash(hash)
            .build()
            .await
            .unwrap();
        let state = index.discovery.as_ref().unwrap().state.lock().await;
        let item = state.signed.as_ref().unwrap();
        assert!(
            ServerList::decode(item.value(), item.key())
                .unwrap()
                .addresses()
                .is_empty()
        );
        drop(state);
        assert_eq!(index.servers().borrow().len(), 1);
    }

    #[tokio::test]
    async fn stored_list_is_used_when_the_record_does_not_resolve() {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let key = n0_mainline::SigningKey::from_bytes(&[46; 32]);
        let public = key.verifying_key().to_bytes();
        let list = ServerList::new(vec!["203.0.113.1:60125".parse().unwrap()]).unwrap();
        // Nothing was published, so only the stored copy can supply servers.
        let stored = list.sign(&key, 1).unwrap();
        let index = AddrIndex::builder(node())
            .list_key(public)
            .fallback_list(stored.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(index.signed_list().await, Some(stored));
        // A copy signed by another key is ignored.
        let other = n0_mainline::SigningKey::from_bytes(&[47; 32]);
        let result = AddrIndex::builder(node())
            .list_key(public)
            .fallback_list(list.sign(&other, 1).unwrap())
            .build()
            .await;
        assert!(matches!(result, Err(UdpError::NoServers { .. })));
    }

    #[tokio::test(start_paused = true)]
    async fn answers_before_a_timeout_are_kept() {
        let answers = n0_future::stream::iter([1, 2]).chain(n0_future::stream::pending());
        let mut items = Vec::new();
        let result = collect_within(answers, Duration::from_secs(30), &mut items).await;
        assert!(result.is_err());
        assert_eq!(items, [1, 2]);
    }

    #[tokio::test]
    async fn a_signed_withdrawal_removes_the_servers() {
        let network = n0_mainline::Testnet::new(3).await.unwrap();
        let node = || {
            Dht::builder()
                .bootstrap(&network.bootstrap)
                .port(0)
                .build()
                .unwrap()
        };
        let authority = node();
        let key = n0_mainline::SigningKey::from_bytes(&[49; 32]);
        let listed = ServerList::new(vec!["203.0.113.1:60125".parse().unwrap()]).unwrap();
        authority
            .put_mutable(listed.sign(&key, 1).unwrap(), None)
            .await
            .unwrap();
        let dht = node();
        let client = UdpClient::attach(dht.clone()).await.unwrap();
        let discovery = Discovery {
            dht,
            sources: Sources {
                list_key: Some(key.verifying_key().to_bytes()),
                ..Sources::default()
            },
            state: Mutex::new(DiscoveryState::default()),
        };
        let (servers_tx, servers) = watch::channel(BTreeSet::new());
        discovery.refresh(&client, &servers_tx).await.unwrap();
        assert_eq!(servers.borrow().len(), 1);
        authority
            .put_mutable(
                ServerList::new(vec![]).unwrap().sign(&key, 2).unwrap(),
                None,
            )
            .await
            .unwrap();
        discovery.refresh(&client, &servers_tx).await.unwrap();
        assert!(servers.borrow().is_empty());
    }

    #[test]
    fn default_list_key_matches_its_name() {
        assert_eq!(
            crate::pkarr_name(&DEFAULT_INDEX_LIST_KEY),
            "z6rb8uoy1pwuckhw8qx8i4qseczujxw4qakpe7xng3yi68wpyrqo"
        );
    }

    #[test]
    fn signed_updates_reject_rollback_and_malformed_values() {
        let key = n0_mainline::SigningKey::from_bytes(&[42; 32]);
        let list = ServerList::new(vec!["203.0.113.1:1234".parse().unwrap()]).unwrap();
        let mut state = DiscoveryState::default();
        state.accept(list.sign(&key, 2).unwrap());
        state.accept(list.sign(&key, 1).unwrap());
        assert_eq!(state.signed.as_ref().unwrap().seq(), 2);
        state.accept(n0_mainline::MutableItem::new(&key, &[255], 3, None));
        assert_eq!(state.signed.as_ref().unwrap().seq(), 2);
        // A newer empty list explicitly withdraws the signed candidates.
        state.accept(ServerList::new(vec![]).unwrap().sign(&key, 4).unwrap());
        assert_eq!(state.signed.as_ref().unwrap().seq(), 4);
        assert!(
            ServerList::decode(
                state.signed.as_ref().unwrap().value(),
                state.signed.as_ref().unwrap().key()
            )
            .unwrap()
            .addresses()
            .is_empty()
        );
    }
}
