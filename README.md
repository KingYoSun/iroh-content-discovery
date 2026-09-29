# iroh Mainline endpoint discovery

Discover iroh endpoints through Mainline. Mainline maps an application-defined
infohash to a compact IPv4 socket; the address index maps that socket to opaque
bytes containing a signed [`EndpointId`][eid]. This works for blobs, gossip
peers, and other iroh protocols.

The address index itself is generic:

```text
SocketAddrV4 → opaque bytes
```

A publisher first asks a server for a short-lived token. The server returns the
packet's observed public IPv4 socket and a stateless MAC bound to that socket. A
put carrying the token must arrive from the same socket; the server then derives
the map key from the packet source and stores the bytes under its own receipt
time and TTL. Reads are direct and public. Requests are padded to 1200 bytes and
shorter ones are dropped, so a response can never be larger than the request
that caused it, and every response fits one unfragmented datagram. The server
neither parses nor validates the value it stores.

Index traffic shares the UDP socket of the caller's Mainline node. Mainline
announcements use their implied source port, so the compact peer address and the
index key describe the same UDP mapping. `iroh-mainline-endpoint-discovery` binds
no socket of its own and owns no DHT node. A publisher behind a shared CGNAT
address therefore cannot claim another publisher's port, because neither the
announcement nor the index write accepts a caller-supplied one.

Index datagrams start with `00 61 64 64 72 69 64 78` (`\0addridx`). The leading
zero byte cannot begin a Mainline KRPC message, whose outer value is a bencoded
dictionary starting with `d`, so both protocols can share a socket.

The iroh discovery layer stores a signed endpoint record in those opaque bytes.
A resolver checks the signature, takes the endpoint ID, and dials it through
iroh's normal discovery. The `host:port` in the DHT and the index is a
rendezvous key, never an iroh address.

The record is signed for the socket the server observed, and a reader discards
a record it finds under any other socket. Reads are public, so anyone can copy
a record; without that binding they could store it in their own slot, which the
server accepts because they really do receive there, and every resolver would
then return that endpoint as a provider for content it does not serve.

`Resolver::resolve_stream` yields endpoint IDs as peer batches and index lookups
complete, translating up to 16 peers at a time so one slow lookup cannot hold up
the rest. An iroh-blobs downloader can start on the first provider while
discovery continues. The stream is lazy: the Mainline lookup starts only when
the first item is requested, so providers a caller already knows can be chained
in front of it without starting a lookup when they suffice. `Resolver::resolve_continuously` begins a new lookup
whenever the consumer asks for more, and may yield an endpoint it has yielded
before. `Resolver::resolve` collects a sorted list when a caller wants one.

All of this needs direct UDP access to a server. Mainline needs the same, so a
node that can use Mainline at all already meets the requirement.

The repository contains four Rust workspace crates and a browser extension:

- `udp-addr-index-proto`: versioned token, put, and get UDP messages; no iroh
  dependency
- `udp-addr-index`: an embeddable address index server, and the `udp-addr-index` binary
- `iroh-mainline-endpoint-discovery`: the `AddrIndex`, `Publisher` and
  `Resolver` APIs; the publisher takes an endpoint secret key and a Mainline
  node that the caller owns
- `iroh-local-gateway`: localhost HTTP streaming, MIME detection, and byte ranges
- `iroh-link-extension`: redirects hash and key subdomains to the gateway in
  Chrome, Brave and Firefox

```sh
cargo run -p udp-addr-index --features cli -- --dht-port 60125 \
  --rendezvous-hash b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b
cargo run -p iroh-mainline-endpoint-discovery --example blobs
cargo run -p iroh-mainline-endpoint-discovery --example spoof
cargo run -p iroh-mainline-endpoint-discovery --example gossip -- my-topic
```

The `gossip` example is a swarm without a ticket: the topic string is hashed
into a gossip topic id and a Mainline infohash, every member announces itself
under that infohash, and members that resolve each other join the same
iroh-gossip topic. Anyone who knows the string can join.

`udp-addr-index` and `iroh-mainline-endpoint-discovery` are libraries first,
so their commands sit behind a `cli` feature that is off by default: a
consumer of either library never compiles `clap` or `tracing-subscriber`. Pass
`--features cli` to run or install `udp-addr-index` or `iroh-index-list`. The
gateway is a command in its own right, so its `cli` feature is on by default,
and an embedder turns it off with `default-features = false`.

To expose index metrics for Prometheus, pass `--metrics-listen 127.0.0.1:9090`
to the `udp-addr-index` command and scrape `http://127.0.0.1:9090/metrics`.
The listener is disabled unless requested. Bind it to a trusted interface;
the endpoint has no authentication.

MIT or Apache-2.0, at your option.

[eid]: https://docs.rs/iroh/latest/iroh/struct.PublicKey.html

## Finding servers

Servers share a Mainline node's UDP socket and can announce themselves under the
rendezvous infohash `b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b`, which is the
SHA-1 of `iroh-addr-index servers v1`. An announcement uses the implied source
port, renews every ten minutes, and retries after thirty seconds on failure.

`Server::attach(dht)` serves and announces until the returned handle is dropped.
`Server::attach_with_rendezvous(dht, Some(hash))` picks a different hash, and
`None` serves without announcing at all. The CLI announces only when you pass
`--rendezvous-hash HEX`: use the hash above for default discovery, and omit the
flag when clients reach you through an explicit address or a signed list. The
CLI binds all IPv4 interfaces at `--dht-port`, 60125 by default, so allow
inbound UDP there. Mainline can choose the port but not the interface.

`AddrIndex::discover(dht)` reads the signed server list maintained by n0, whose
Pkarr key is `DEFAULT_INDEX_LIST_KEY`
(`z6rb8uoy1pwuckhw8qx8i4qseczujxw4qakpe7xng3yi68wpyrqo`), and then uses the
same DHT socket for index requests. Finding no servers at first is an error.
Discovery then repeats in the background every ten minutes; a failed refresh
keeps the previous servers and retries after thirty seconds, so lookups never
wait for it. It keeps at most two servers and gives each lookup thirty seconds.

A `Publisher` keeps its index record and its announcements on separate
schedules. Each infohash is announced every ten minutes, with jitter. The
record is republished only while there are infohashes to announce: when it is
older than thirty minutes (servers keep it for an hour), when discovery finds
other servers, and when an announcement finds that Mainline now reports a
different public address for us. It goes to all servers and counts once one
stored it, since readers ask them all; a failure is retried after thirty
seconds. Many infohashes share one record, so announcing them costs one index
publication.

An announcement goes ahead only if a server holds our record for the address
Mainline sees, since readers could not resolve it otherwise. Without one, it
waits for the next successful publication. After an outage of thirty seconds
or more, it resumes after a random delay of up to a minute, so the waiting
announcements do not restart at once.

Rendezvous discovery through `get_peers` is opt-in with the builder below. Its announcements are untrusted, so those candidates may be
unreachable or dishonest; the signed endpoint records they serve are validated
either way.

The examples and the gateway use `AddrIndex::builder(dht).n0_defaults()`. Set
`IROH_ADDR_INDEX=ip:port` to pin one server instead, or add `.server(addr)` in
code. Mainline bootstrap
nodes are still required, since no server address is hardcoded. The
`udp-addr-index` binary exits if its service or announcement task stops, and
shuts down on Ctrl-C or SIGTERM.

### Curated bootstrap list (Pkarr)

`AddrIndex::builder(dht)` configures where servers come from. Explicit
servers are used as given and skip discovery. Otherwise the trusted Pkarr
list is tried first, and the rendezvous hash only when the signed list yields
nothing. Each lookup has a thirty-second deadline.

```rust,ignore
let index = AddrIndex::builder(dht)
    .list_key(public_key)             // trusted Pkarr list, tried first
    .rendezvous_hash(rendezvous_hash) // untrusted fallback
    // .server("203.0.113.1:60125".parse()?) bypasses discovery
    .build()
    .await?;
```

The builder starts empty; `n0_defaults()` sets the list maintained by n0,
`DEFAULT_INDEX_LIST_KEY`, and caches lookups for `DEFAULT_LOOKUP_CACHE_TTL`
(five minutes). `lookup_cache(ttl)` sets the cache on its own. Sockets without
a record are remembered for at most thirty seconds, failed lookups not at all,
and concurrent lookups of one socket share a request. With no server, key, or hash, `build` returns
`NoServers` immediately. `AddrIndex::udp(dht, server)` and
`AddrIndex::discover(dht)` are shorthands for one explicit server and for
`builder(dht).n0_defaults()`. A server announces under a custom hash with
`Server::attach_with_rendezvous(dht, Some(hash))`.

Discovery keeps at most two servers, and a signed list holds up to two
addresses. A signed result is never topped up with rendezvous candidates.
Rendezvous candidates stay untrusted, and even a signed address only means the
authority vouches for that server, not that it answered. Falling back means no
signed address was found, not that a listed server failed a request.

The authority publishes a list with the `ServerList` helper:

```rust,ignore
let list = ServerList::new(vec!["203.0.113.1:6881".parse()?])?;
let item = list.sign(&signing_key, sequence)?;
dht.put_mutable(item, None).await?;
```

The signing key stays with the authority; clients need only the public key.
The value is a standard Pkarr DNS packet in an **unsalted** BEP44 item. Each
apex IN TXT record contains one IPv4 socket. Only the apex matching the signer's
z-base-32 public key is read; other owners and record types are ignored.
Malformed apex TXT sockets or more than two entries invalidate the list.
An empty apex TXT record (`@ 300 IN TXT ""`), or a packet without apex TXT
records, withdraws the signed candidates and allows the fallback.

To publish with **iroh-share**, create a standalone name, open its advanced DNS
record editor, and replace its records with:

```dns
@ 300 IN TXT "203.0.113.1:60125"
@ 300 IN TXT "198.51.100.2:33445"
```

Save the records and keep the iroh-share daemon running to renew publication.
Configure clients with that name's public key. The gateway accepts the bare
z-base-32 key displayed by iroh-share (without `https://` or `.pkarr.net`):

```sh
iroh-local-gateway --index-list-key <pkarr-public-key>
```

Without `--index-list-key`, the gateway uses `DEFAULT_INDEX_LIST_KEY`. Add
`--rendezvous-hash b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b` to allow the
public rendezvous fallback; curated addresses take precedence. With
`--state-dir`, the gateway stores the last list it resolved in
`index-list.pkarr`, a Pkarr signed packet that is verified again when read,
and uses it when the record does not resolve.
Pkarr sequences are Unix timestamps in microseconds; iroh-share manages them automatically.
The previous salted binary list format is no longer read.

`n0-mainline` verifies BEP44 signatures. An `AddrIndex` keeps the highest valid
list it has seen across refreshes, including when a lookup times out, and
rejects lower sequences for its lifetime. That memory does not survive a
restart, so a fresh client can still be handed an older signed list. Lists carry
no wall-clock expiry.

### Running the standalone Pkarr republisher

Run the list publisher separately from the servers it names:

```sh
# Set IROH_INDEX_LIST_SECRET to a 64-hex-digit Ed25519 secret seed.
cargo run -p iroh-mainline-endpoint-discovery --features cli \
  --bin iroh-index-list -- \
  --server 203.0.113.1:6881 203.0.113.2:6881
```

The process consumes and removes `IROH_INDEX_LIST_SECRET` before any runtime
thread starts, signs the list once, and zeroizes the secret buffers and the
signing key. It logs the public key so you can configure clients, and the
renewal task keeps only the signed item. Removing the variable here does not
remove it from the shell or service configuration that launched the process.

Publication starts immediately and repeats every ten minutes. A transient error
retries after thirty seconds, and each attempt times out after thirty. A
sequence conflict or a DHT shutdown stops the task. To change the list, restart
with new addresses. The timestamp defaults to the current time; an explicit
`--sequence` must exceed the previous timestamp. Omit `--server` to publish an empty
list. Ctrl-C stops renewal.

Embedded applications can run `republish_server_list(dht, signed_item)` as a
task of their own; dropping that future stops renewal without stopping the DHT.

## Local HTTP content gateway

The fourth workspace project, [`iroh-local-gateway`](iroh-local-gateway/README.md),
serves `http://127.0.0.1:45475/blake3/<z32>`. It finds a provider through Mainline
and the address index, then streams Bao-verified bytes with MIME detection
and HTTP range support for video seeking. Collection roots automatically show
a directory listing at `/blake3/<z32>`, and `/blake3/<z32>/path/to/file`
streams a file from the same provider. The same content is served on
`http://<z32>.blake3.localhost:45475/`, giving each hash its own browser
origin, and Pkarr keys resolve at `/pkarr/<key>` and
`http://<key>.pkarr.localhost:45475/`.

Query flags:

- `?tree` serves a known collection without detecting it first.
- `?download` saves a file under its collection name, or a root as its raw
  bytes.
- `?sizes` adds file sizes to a listing.

```sh
cargo run -p iroh-local-gateway -- --index-server 127.0.0.1:60125
```

To serve content, the `provide` example adds a file or directory as blobs plus
a collection, announces every hash, and publishes a Pkarr name for the
collection, printing both link URLs:

```sh
cargo run -p iroh-mainline-endpoint-discovery --example provide -- ./site
```

It reads `PKARR_SECRET` (64 hex digits) to keep the same name across runs, and
prints a generated one if unset. Use `--no-pkarr` to publish hashes only.

For a complete named-content round trip without the HTTP gateway, run:

```sh
cargo run -p iroh-mainline-endpoint-discovery --example pkarr-publish-resolve -- --once
```

The example generates a temporary signing keypair, serves a demo blob over iroh,
publishes its signed endpoint mapping, announces its content hash on Mainline,
and publishes a Pkarr name pointing to that hash. A separate DHT client starts
with only the public key and index configuration, resolves the signed name,
discovers a provider through Mainline and the address index, and downloads and
BLAKE3-verifies the blob into a separate store.

Both sides discover address index servers by default. Use `--index-server
IP:PORT` (or `IROH_ADDR_INDEX`) to use a particular reachable index on both sides.
The example needs UDP access to Mainline and a working address index; it does
not start an index server or HTTP gateway.

Use `--key-file ./site.key` to load or create a persistent keypair and
`--data 'updated content'` to change the blob while keeping the same name:

```sh
cargo run -p iroh-mainline-endpoint-discovery --example pkarr-publish-resolve -- \
  --key-file ./site.key --data 'hello from my named content' --once
```

With `--once`, the example exits after the verified download. Omit it to keep
serving the blob and republishing its provider announcement and Pkarr name until
Ctrl-C. Restart with the same key file and different data to update the name.

The gateway also accepts the Pkarr public key and rendezvous hash discovery
options. See its README for configuration and HTTP behavior.

## Browser extension

The fifth project, [`iroh-link-extension`](iroh-link-extension/README.md),
rewrites `https://<z32>.blake3.net/<path>` to
`http://<z32>.blake3.localhost:<port>/<path>`, and
`https://<key>.pkarr.net/<path>` to `http://<key>.pkarr.localhost:<port>/<path>`,
in Chrome, Brave and Firefox. Load that directory
unpacked from the browser's extensions page with Developer mode enabled. The
popup configures the local gateway port (default 45475) and enables/disables
rewrites. The apex `blake3.net` site is unaffected.

## License

This project is licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  https://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
