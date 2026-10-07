//! Concurrent UDP client for opaque address-index servers.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV4,
    time::Duration,
};

use n0_error::e;
use n0_mainline::{ActorShutdown, DatagramHook, Dht};
use tokio::sync::{mpsc, mpsc::error::TrySendError, oneshot};
use tracing::debug;
use udp_addr_index_proto::{
    MAGIC, MAX_DGRAM, MAX_VALUE_LEN, Proto, Request, RequestV1, Response, ResponseV1, TransactionId,
};

/// Default timeout for an address-index operation.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// Most publishes, and most lookups, waiting for answers at once.
const MAX_PENDING: usize = 256;

/// UDP address-index client error.
#[n0_error::stack_error(derive, add_meta)]
pub enum UdpError {
    /// The Mainline node's datagram hook could not be attached.
    Attach {
        /// Underlying Mainline actor error.
        #[error(from, source)]
        source: ActorShutdown,
    },
    /// No matching response arrived before the deadline.
    #[error("UDP operation timed out")]
    Timeout {},
    /// The opaque value exceeds [`MAX_VALUE_LEN`] or the UDP datagram limit.
    #[error("opaque value exceeds {MAX_VALUE_LEN} bytes")]
    TooLarge {},
    /// No server addresses are configured.
    #[error("no UDP servers configured")]
    NoServers {},
    /// The client actor stopped.
    #[error("UDP client closed")]
    Closed {},
    /// As many operations of this kind as the client allows wait for answers already.
    #[error("too many UDP operations waiting for answers")]
    Busy {},
}

/// Decides whether a looked-up value is good enough to stop waiting for others.
pub(crate) type Accept = Box<dyn Fn(&[u8]) -> bool + Send>;

/// Builds the value to store from the public socket a server observed.
pub type ValueFor = Box<dyn Fn(SocketAddrV4) -> Vec<u8> + Send>;

enum ActorMsg {
    AddServer(SocketAddrV4),
    ReplaceServers(HashSet<SocketAddrV4>),
    RemoveServer(SocketAddrV4),
    Publish(
        TransactionId,
        ValueFor,
        oneshot::Sender<Result<Vec<SocketAddrV4>, UdpError>>,
    ),
    Resolve(
        TransactionId,
        SocketAddrV4,
        Option<Accept>,
        oneshot::Sender<Result<ResolveResult, UdpError>>,
    ),
}

/// Address-index client attached to a Mainline node's UDP socket.
///
/// Dropping a call gives its operation up: one not sent yet is not sent, and
/// one under way stops waiting for answers. Up to 256 publishes and 256
/// lookups wait for answers at once; beyond that a call fails with
/// [`UdpError::Busy`].
#[derive(Debug, Clone)]
pub struct UdpClient {
    tx: mpsc::Sender<ActorMsg>,
    /// Operations given up by their callers. Each sends at most one, so this
    /// holds at most as many as there are operations.
    cancels: mpsc::UnboundedSender<TransactionId>,
}

/// Gives an operation up when dropped before it finished.
struct Withdraw<'a> {
    cancels: &'a mpsc::UnboundedSender<TransactionId>,
    tx: Option<TransactionId>,
}

impl Drop for Withdraw<'_> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx {
            let _ = self.cancels.send(tx);
        }
    }
}

impl UdpClient {
    /// Attaches to `dht` using the default operation timeout.
    pub async fn attach(dht: Dht) -> Result<Self, UdpError> {
        Self::attach_with_timeout(dht, DEFAULT_TIMEOUT).await
    }

    /// Attaches to `dht` with the given operation timeout.
    pub async fn attach_with_timeout(dht: Dht, timeout: Duration) -> Result<Self, UdpError> {
        let (incoming_tx, incoming_rx) = mpsc::channel(256);
        dht.set_datagram_hook(Some(DatagramHook::new(move |bytes, from| {
            if !bytes.starts_with(MAGIC) {
                return false;
            }
            match incoming_tx.try_send((Box::from(bytes), from)) {
                Ok(()) | Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Closed(_)) => false,
            }
        })))
        .await?;
        let (tx, rx) = mpsc::channel(32);
        let (cancels, cancelled) = mpsc::unbounded_channel();
        tokio::spawn(Actor::new(dht, incoming_rx, rx, cancelled, timeout).run());
        Ok(Self { tx, cancels })
    }

    /// Adds a server used by subsequent operations.
    pub async fn add_server(&self, server: SocketAddrV4) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::AddServer(server))
            .await
            .map_err(|_| e!(UdpError::Closed))
    }

    pub(crate) async fn replace_servers(
        &self,
        servers: HashSet<SocketAddrV4>,
    ) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::ReplaceServers(servers))
            .await
            .map_err(|_| e!(UdpError::Closed))
    }

    /// Removes a configured server.
    pub async fn remove_server(&self, server: SocketAddrV4) -> Result<(), UdpError> {
        self.tx
            .send(ActorMsg::RemoveServer(server))
            .await
            .map_err(|_| e!(UdpError::Closed))
    }

    /// Obtains tokens and publishes a value to every responsive server.
    ///
    /// The value is built per server, from the public socket that server
    /// observed, which is only known once it has answered. Servers behind
    /// different paths can legitimately see different sockets.
    ///
    /// Returns the public IPv4 sockets under which servers stored the value.
    pub async fn publish(
        &self,
        value: impl Fn(SocketAddrV4) -> Vec<u8> + Send + 'static,
    ) -> Result<Vec<SocketAddrV4>, UdpError> {
        let (response, rx) = oneshot::channel();
        self.run(|tx| ActorMsg::Publish(tx, Box::new(value), response), rx)
            .await
    }

    /// Reads and deduplicates opaque values from all configured servers.
    pub async fn resolve(&self, addr: SocketAddrV4) -> Result<ResolveResult, UdpError> {
        self.resolve_with(addr, None).await
    }

    /// Reads values, returning as soon as one passes `accept`.
    ///
    /// Without such a value, it waits for all servers as [`Self::resolve`]
    /// does, since a server that has not answered yet may hold one.
    pub(crate) async fn resolve_first(
        &self,
        addr: SocketAddrV4,
        accept: Accept,
    ) -> Result<ResolveResult, UdpError> {
        self.resolve_with(addr, Some(accept)).await
    }

    async fn resolve_with(
        &self,
        addr: SocketAddrV4,
        accept: Option<Accept>,
    ) -> Result<ResolveResult, UdpError> {
        let (response, rx) = oneshot::channel();
        self.run(|tx| ActorMsg::Resolve(tx, addr, accept, response), rx)
            .await
    }

    /// Hands an operation to the actor and waits for its result, giving it up
    /// if dropped before.
    async fn run<T>(
        &self,
        message: impl FnOnce(TransactionId) -> ActorMsg,
        result: oneshot::Receiver<Result<T, UdpError>>,
    ) -> Result<T, UdpError> {
        // Our socket and the servers we talk to are both public, so a
        // predictable id would be enough to answer on a server's behalf.
        let tx = rand::random();
        let mut withdraw = Withdraw {
            cancels: &self.cancels,
            tx: Some(tx),
        };
        self.tx
            .send(message(tx))
            .await
            .map_err(|_| e!(UdpError::Closed))?;
        let result = result.await;
        withdraw.tx = None;
        result.map_err(|_| e!(UdpError::Closed))?
    }
}

/// Result of an opaque address lookup.
#[derive(Debug, Clone, Default)]
pub struct ResolveResult {
    /// Deduplicated opaque values returned by servers.
    pub values: Vec<Vec<u8>>,
}

struct PendingPublish {
    value: ValueFor,
    /// Whether a built value was too large, so the failure is not a timeout.
    too_large: bool,
    awaiting: HashSet<SocketAddrV4>,
    /// Servers already sent a put.
    ///
    /// A repeated or forged `Prepared` cannot make us send the value again.
    prepared: HashSet<SocketAddrV4>,
    stored: HashSet<SocketAddrV4>,
    response: oneshot::Sender<Result<Vec<SocketAddrV4>, UdpError>>,
    deadline: tokio::time::Instant,
}

struct PendingResolve {
    addr: SocketAddrV4,
    /// Finishes the lookup early on the first value it accepts.
    accept: Option<Accept>,
    awaiting: HashSet<SocketAddrV4>,
    values: HashSet<Vec<u8>>,
    responded: bool,
    response: oneshot::Sender<Result<ResolveResult, UdpError>>,
    deadline: tokio::time::Instant,
}

struct Actor {
    dht: Dht,
    incoming: mpsc::Receiver<(Box<[u8]>, SocketAddrV4)>,
    rx: mpsc::Receiver<ActorMsg>,
    cancelled: mpsc::UnboundedReceiver<TransactionId>,
    servers: HashSet<SocketAddrV4>,
    publishes: HashMap<TransactionId, PendingPublish>,
    resolves: HashMap<TransactionId, PendingResolve>,
    timeout: Duration,
}

impl Actor {
    fn new(
        dht: Dht,
        incoming: mpsc::Receiver<(Box<[u8]>, SocketAddrV4)>,
        rx: mpsc::Receiver<ActorMsg>,
        cancelled: mpsc::UnboundedReceiver<TransactionId>,
        timeout: Duration,
    ) -> Self {
        Self {
            dht,
            incoming,
            rx,
            cancelled,
            servers: HashSet::new(),
            publishes: HashMap::new(),
            resolves: HashMap::new(),
            timeout,
        }
    }

    async fn run(mut self) {
        let mut send_buf = [0; MAX_DGRAM];
        loop {
            let deadline = self.next_deadline();
            tokio::select! {
                message = self.rx.recv() => {
                    let Some(message) = message else { break };
                    self.handle_message(message, &mut send_buf).await;
                }
                packet = self.incoming.recv() => match packet {
                    Some((data, from)) => self.handle_packet(&data, from, &mut send_buf).await,
                    None => break,
                },
                Some(tx) = self.cancelled.recv() => self.withdraw(tx),
                _ = sleep_until(deadline) => self.flush_expired(),
            }
        }
    }

    /// Forgets an operation its caller gave up.
    fn withdraw(&mut self, tx: TransactionId) {
        self.publishes.remove(&tx);
        self.resolves.remove(&tx);
    }

    async fn handle_message(&mut self, message: ActorMsg, buf: &mut [u8; MAX_DGRAM]) {
        // Operations given up first, so that they leave room for this one.
        while let Ok(tx) = self.cancelled.try_recv() {
            self.withdraw(tx);
        }
        match message {
            ActorMsg::ReplaceServers(servers) => {
                self.servers = servers;
            }
            ActorMsg::AddServer(addr) => {
                self.servers.insert(addr);
            }
            ActorMsg::RemoveServer(addr) => {
                self.servers.remove(&addr);
            }
            ActorMsg::Publish(tx, value, response) => {
                // The caller gave up before the request went out.
                if response.is_closed() {
                    return;
                }
                if self.servers.is_empty() {
                    let _ = response.send(Err(e!(UdpError::NoServers)));
                    return;
                }
                if self.publishes.len() >= MAX_PENDING {
                    let _ = response.send(Err(e!(UdpError::Busy)));
                    return;
                }
                let request = Request::V1(RequestV1::Prepare {
                    tx,
                    padding: [0; 24],
                });
                if let Some(bytes) = encode(request, buf) {
                    for server in &self.servers {
                        if let Err(err) = self.dht.send_datagram(bytes.to_vec(), *server).await {
                            debug!(%server, %err, "send prepare");
                        }
                    }
                    self.publishes.insert(
                        tx,
                        PendingPublish {
                            value,
                            too_large: false,
                            awaiting: self.servers.clone(),
                            prepared: HashSet::new(),
                            stored: HashSet::new(),
                            response,
                            deadline: tokio::time::Instant::now() + self.timeout,
                        },
                    );
                } else {
                    let _ = response.send(Err(e!(UdpError::TooLarge)));
                }
            }
            ActorMsg::Resolve(tx, addr, accept, response) => {
                // The caller gave up before the request went out.
                if response.is_closed() {
                    return;
                }
                if self.servers.is_empty() {
                    let _ = response.send(Err(e!(UdpError::NoServers)));
                    return;
                }
                if self.resolves.len() >= MAX_PENDING {
                    let _ = response.send(Err(e!(UdpError::Busy)));
                    return;
                }
                let request = Request::V1(RequestV1::Get { tx, addr });
                if let Some(bytes) = encode(request, buf) {
                    for server in &self.servers {
                        debug!(indexer = %server, peer = %addr, "querying indexer");
                        if let Err(err) = self.dht.send_datagram(bytes.to_vec(), *server).await {
                            debug!(indexer = %server, peer = %addr, %err, "send get");
                        }
                    }
                    self.resolves.insert(
                        tx,
                        PendingResolve {
                            addr,
                            accept,
                            awaiting: self.servers.clone(),
                            values: HashSet::new(),
                            responded: false,
                            response,
                            deadline: tokio::time::Instant::now() + self.timeout,
                        },
                    );
                } else {
                    let _ = response.send(Err(e!(UdpError::TooLarge)));
                }
            }
        }
    }

    async fn handle_packet(&mut self, data: &[u8], from: SocketAddrV4, buf: &mut [u8; MAX_DGRAM]) {
        let Some(Proto::Response(Response::V1(response))) = Proto::decode(data) else {
            return;
        };
        match response {
            ResponseV1::Prepared { tx, addr, token } => {
                let Some(pending) = self.publishes.get_mut(&tx) else {
                    return;
                };
                // One put per server per transaction: otherwise every repeated
                // or forged `Prepared` reflects the whole value at that server.
                if !pending.awaiting.contains(&from) || !pending.prepared.insert(from) {
                    return;
                }
                let value = (pending.value)(addr);
                if value.len() > MAX_VALUE_LEN {
                    pending.too_large = true;
                    pending.awaiting.remove(&from);
                    if pending.awaiting.is_empty() {
                        self.finish_publish(tx);
                    }
                    return;
                }
                let request = Request::V1(RequestV1::Put { tx, token, value });
                if let Some(bytes) = encode(request, buf)
                    && let Err(err) = self.dht.send_datagram(bytes.to_vec(), from).await
                {
                    debug!(%from, %addr, %err, "send authorized request");
                }
            }
            ResponseV1::Stored { tx, addr } => {
                let Some(pending) = self.publishes.get_mut(&tx) else {
                    return;
                };
                if !pending.awaiting.remove(&from) {
                    return;
                }
                pending.stored.insert(addr);
                if pending.awaiting.is_empty() {
                    self.finish_publish(tx);
                }
            }
            ResponseV1::Value { tx, addr, value } => {
                let Some(pending) = self.resolves.get_mut(&tx) else {
                    return;
                };
                if pending.addr != addr || !pending.awaiting.remove(&from) {
                    return;
                }
                debug!(
                    indexer = %from,
                    peer = %addr,
                    found = %match value.as_deref() {
                        Some(bytes) => crate::SignedRecord::decode(bytes, addr)
                            .map(|record| record.endpoint_id.to_string())
                            .unwrap_or_else(|| "invalid".to_owned()),
                        None => "none".to_owned(),
                    },
                    "indexer responded"
                );
                pending.responded = true;
                let mut accepted = false;
                if let Some(value) = value
                    && value.len() <= MAX_VALUE_LEN
                {
                    accepted = pending.accept.as_ref().is_some_and(|accept| accept(&value));
                    pending.values.insert(value);
                }
                if accepted || pending.awaiting.is_empty() {
                    self.finish_resolve(tx);
                }
            }
        }
    }

    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        self.publishes
            .values()
            .map(|pending| pending.deadline)
            .chain(self.resolves.values().map(|pending| pending.deadline))
            .min()
    }

    fn flush_expired(&mut self) {
        let now = tokio::time::Instant::now();
        let publishes: Vec<_> = self
            .publishes
            .iter()
            .filter_map(|(tx, pending)| (pending.deadline <= now).then_some(*tx))
            .collect();
        for tx in publishes {
            self.finish_publish(tx);
        }
        let resolves: Vec<_> = self
            .resolves
            .iter()
            .filter_map(|(tx, pending)| (pending.deadline <= now).then_some(*tx))
            .collect();
        for tx in resolves {
            self.finish_resolve(tx);
        }
    }

    fn finish_publish(&mut self, tx: TransactionId) {
        let Some(pending) = self.publishes.remove(&tx) else {
            return;
        };
        let result = if pending.stored.is_empty() {
            if pending.too_large {
                Err(e!(UdpError::TooLarge))
            } else {
                Err(e!(UdpError::Timeout))
            }
        } else {
            let mut addrs: Vec<_> = pending.stored.into_iter().collect();
            addrs.sort();
            Ok(addrs)
        };
        let _ = pending.response.send(result);
    }

    fn finish_resolve(&mut self, tx: TransactionId) {
        let Some(pending) = self.resolves.remove(&tx) else {
            return;
        };
        // After an early finish, the others simply were not waited for.
        if pending.deadline <= tokio::time::Instant::now() {
            for server in &pending.awaiting {
                debug!(indexer = %server, peer = %pending.addr, "index lookup timed out");
            }
        }
        let result = if !pending.responded {
            Err(e!(UdpError::Timeout))
        } else {
            let mut values: Vec<_> = pending.values.into_iter().collect();
            values.sort();
            Ok(ResolveResult { values })
        };
        let _ = pending.response.send(result);
    }
}

/// Frames a request for sending, or `None` if it does not fit a datagram.
fn encode(value: Request, buf: &mut [u8; MAX_DGRAM]) -> Option<&[u8]> {
    Proto::Request(value).encode(buf).ok()
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use n0_future::future::poll_once;
    use tokio::net::UdpSocket;

    use super::*;

    /// The requests a fake index server received.
    #[derive(Default)]
    struct Received {
        prepares: AtomicUsize,
        puts: AtomicUsize,
        gets: AtomicUsize,
    }

    /// An index server that never answers lookups, stores every put, and
    /// answers a prepare with a token after `token_after`, if set.
    async fn index_server(token_after: Option<Duration>) -> (SocketAddrV4, Arc<Received>) {
        let socket = Arc::new(UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap());
        let SocketAddr::V4(addr) = socket.local_addr().unwrap() else {
            unreachable!("bound to an IPv4 address")
        };
        let received = Arc::new(Received::default());
        let counted = received.clone();
        tokio::spawn(async move {
            let mut buf = [0; MAX_DGRAM];
            loop {
                // Windows reports an ICMP port unreachable as a receive error.
                let Ok((len, SocketAddr::V4(from))) = socket.recv_from(&mut buf).await else {
                    continue;
                };
                let Some(Proto::Request(Request::V1(request))) = Proto::decode(&buf[..len]) else {
                    continue;
                };
                let response = match request {
                    RequestV1::Prepare { tx, .. } => {
                        counted.prepares.fetch_add(1, Ordering::SeqCst);
                        let Some(delay) = token_after else {
                            continue;
                        };
                        tokio::time::sleep(delay).await;
                        ResponseV1::Prepared {
                            tx,
                            addr: from,
                            token: [0; 16],
                        }
                    }
                    RequestV1::Put { tx, .. } => {
                        counted.puts.fetch_add(1, Ordering::SeqCst);
                        ResponseV1::Stored { tx, addr: from }
                    }
                    RequestV1::Get { .. } => {
                        counted.gets.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                };
                let mut out = [0; MAX_DGRAM];
                let response = Proto::Response(Response::V1(response));
                let _ = socket
                    .send_to(response.encode(&mut out).unwrap(), from)
                    .await;
            }
        });
        (addr, received)
    }

    async fn client(server: SocketAddrV4, timeout: Duration) -> UdpClient {
        let dht = Dht::builder().no_bootstrap().port(0).build().unwrap();
        let client = UdpClient::attach_with_timeout(dht, timeout).await.unwrap();
        client.add_server(server).await.unwrap();
        client
    }

    fn peer(port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 7), port)
    }

    async fn wait_for(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the condition in time");
    }

    /// A lookup dropped before the client sent it is not sent.
    #[tokio::test]
    async fn a_lookup_dropped_before_it_is_sent_sends_nothing() {
        let (server, received) = index_server(None).await;
        let client = client(server, DEFAULT_TIMEOUT).await;
        let mut lookup = Box::pin(client.resolve(peer(1)));
        assert!(poll_once(&mut lookup).await.is_none());
        drop(lookup);

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            received.gets.load(Ordering::SeqCst),
            0,
            "the dropped lookup was sent"
        );
    }

    /// Lookups beyond the limit fail at once, until dropped ones free their slots.
    #[tokio::test]
    async fn lookups_beyond_the_limit_are_refused_until_dropped_ones_free_their_slots() {
        let (server, received) = index_server(None).await;
        // Lookups wait for answers that never come, longer than the test runs.
        let client = client(server, Duration::from_secs(60)).await;
        let mut lookups = Vec::new();
        // One at a time, so that the server receives every request.
        for port in 0..256 {
            lookups.push(tokio::spawn({
                let client = client.clone();
                async move { client.resolve(peer(port)).await }
            }));
            wait_for(|| received.gets.load(Ordering::SeqCst) == usize::from(port) + 1).await;
        }

        let refused =
            tokio::time::timeout(Duration::from_millis(500), client.resolve(peer(256))).await;
        assert!(
            matches!(refused, Ok(Err(_))),
            "the lookup over the limit was accepted"
        );

        for lookup in lookups {
            lookup.abort();
            let _ = lookup.await;
        }
        let next = tokio::spawn({
            let client = client.clone();
            async move { client.resolve(peer(257)).await }
        });
        wait_for(|| received.gets.load(Ordering::SeqCst) == 257).await;
        next.abort();
    }

    /// A publish dropped before the server's token arrives stores no value.
    #[tokio::test]
    async fn a_publish_dropped_before_its_token_arrives_sends_no_value() {
        let (server, received) = index_server(Some(Duration::from_millis(300))).await;
        let client = client(server, Duration::from_secs(60)).await;
        let publish = tokio::spawn({
            let client = client.clone();
            async move { client.publish(|_| vec![1, 2, 3]).await }
        });
        wait_for(|| received.prepares.load(Ordering::SeqCst) == 1).await;
        publish.abort();
        let _ = publish.await;

        // The token arrives 300 ms after the prepare.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            received.puts.load(Ordering::SeqCst),
            0,
            "the dropped publish sent its value"
        );

        // A publish that waits stores its value.
        let stored =
            tokio::time::timeout(Duration::from_secs(5), client.publish(|_| vec![4])).await;
        assert!(matches!(stored, Ok(Ok(_))), "{stored:?}");
        assert_eq!(received.puts.load(Ordering::SeqCst), 1);
    }
}
