//! Convenience wrapper around the UDP index client.

use n0_error::e;
use n0_future::StreamExt;
use n0_mainline::Dht;
use std::{collections::HashSet, net::SocketAddrV4, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use iroh_base::SecretKey;

use crate::{SignedRecord, UdpClient, UdpError, udp::DEFAULT_TIMEOUT};
use tracing::debug;

/// Pkarr key of the index server list maintained by n0.
///
/// [`AddrIndexBuilder::n0_defaults`] discovers servers from this list.
///
/// Its z-base-32 name is `z6rb8uoy1pwuckhw8qx8i4qseczujxw4qakpe7xng3yi68wpyrqo`.
pub const DEFAULT_INDEX_LIST_KEY: [u8; 32] = [
    191, 136, 19, 206, 0, 147, 105, 54, 43, 148, 59, 158, 122, 233, 214, 67, 47, 52, 190, 154, 118,
    20, 212, 117, 226, 54, 65, 95, 30, 141, 1, 29,
];

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

    /// Discovers servers from the list maintained by n0, [`DEFAULT_INDEX_LIST_KEY`].
    pub fn n0_defaults(self) -> Self {
        self.list_key(DEFAULT_INDEX_LIST_KEY)
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

    /// Sets the deadline for each publish or lookup, [`DEFAULT_TIMEOUT`] by default.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attaches to the DHT socket and, unless servers were given, discovers them.
    ///
    /// Discovery keeps at most two servers, gives each lookup thirty seconds,
    /// and refreshes on use after ten minutes, retaining the highest signed
    /// sequence for the index's lifetime. Addresses are candidates; they do
    /// not prove availability.
    pub async fn build(self) -> Result<AddrIndex, UdpError> {
        debug!(servers = ?self.servers, sources = ?self.sources, "configuring index server discovery");
        if !self.servers.is_empty() {
            let client = UdpClient::attach_with_timeout(self.dht, self.timeout).await?;
            client.replace_servers(self.servers).await?;
            return Ok(AddrIndex::from_udp(client));
        }
        if self.sources.list_key.is_none() && self.sources.rendezvous_hash.is_none() {
            return Err(e!(UdpError::NoServers));
        }
        let client = UdpClient::attach_with_timeout(self.dht.clone(), self.timeout).await?;
        let index = AddrIndex {
            client,
            discovery: Some(Arc::new(Discovery {
                dht: self.dht,
                sources: self.sources,
                state: Mutex::new(DiscoveryState::default()),
            })),
        };
        index.refresh_servers().await?;
        Ok(index)
    }
}

/// UDP client for one or more index servers.
#[derive(Debug, Clone)]
pub struct AddrIndex {
    client: UdpClient,
    discovery: Option<Arc<Discovery>>,
}

#[derive(Debug)]
struct Discovery {
    dht: Dht,
    sources: Sources,
    state: Mutex<DiscoveryState>,
}

#[derive(Debug, Default)]
struct DiscoveryState {
    refreshed: Option<tokio::time::Instant>,
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

impl AddrIndex {
    /// Starts configuring an index on a Mainline node's UDP socket, with no servers.
    pub fn builder(dht: Dht) -> AddrIndexBuilder {
        AddrIndexBuilder {
            dht,
            servers: HashSet::new(),
            sources: Sources::default(),
            timeout: DEFAULT_TIMEOUT,
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

    async fn refresh_servers(&self) -> Result<(), UdpError> {
        let Some(discovery) = &self.discovery else {
            return Ok(());
        };
        let mut state = discovery.state.lock().await;
        if state
            .refreshed
            .is_some_and(|time| time.elapsed() < Duration::from_secs(600))
        {
            return Ok(());
        }
        let signed_lookup = async {
            let Some(key) = discovery.sources.list_key else {
                return Ok::<_, UdpError>(());
            };
            let mut stream = discovery.dht.get_mutable(&key, None, None).await?;
            while let Some(item) = stream.next().await {
                state.accept(item);
            }
            Ok(())
        };
        let signed_result = tokio::time::timeout(Duration::from_secs(30), signed_lookup).await;
        if state.signed.is_none()
            && let Some(item) = &discovery.sources.fallback_list
            && discovery.sources.list_key.as_ref() == Some(item.key())
        {
            debug!(
                sequence = item.seq(),
                "signed index list not found; using the stored copy"
            );
            state.accept(item.clone());
        }
        debug!(?signed_result, "signed index-list lookup completed");
        let mut peers = state
            .signed
            .as_ref()
            .and_then(|item| crate::ServerList::decode(item.value(), item.key()))
            .map(|list| list.addresses().iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        if peers.is_empty() {
            if let Some(hash) = discovery.sources.rendezvous_hash {
                debug!(infohash = %crate::infohash_hex(&hash), "discovering index servers through Mainline rendezvous");
                let lookup = async {
                    let mut stream = discovery.dht.get_peers(hash.into()).await?;
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
                match tokio::time::timeout(Duration::from_secs(30), lookup).await {
                    Ok(result) => result?,
                    Err(_) if peers.is_empty() => return Err(e!(UdpError::Timeout)),
                    Err(_) => {}
                }
            } else {
                signed_result.map_err(|_| e!(UdpError::Timeout))??;
            }
        }
        if peers.is_empty() {
            debug!("index server discovery found no servers");
            return Err(e!(UdpError::NoServers));
        }
        debug!(?peers, "using discovered index servers");
        self.client.replace_servers(peers).await?;
        state.refreshed = Some(tokio::time::Instant::now());
        Ok(())
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
        self.refresh_servers().await?;
        let secret = secret.clone();
        self.client
            .publish(move |addr| SignedRecord::sign(&secret, addr).encode())
            .await
            .map_err(Into::into)
    }

    /// Looks up the endpoints that listed `addr`.
    ///
    /// Records that were not signed for `addr` are discarded, so a record
    /// republished under another socket is not returned.
    pub async fn lookup(&self, addr: SocketAddrV4) -> Result<Vec<SignedRecord>, AddrIndexError> {
        self.refresh_servers().await?;
        let result = self.client.resolve(addr).await?;
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
        assert!(state.refreshed.is_some());
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
