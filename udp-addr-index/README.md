# udp-addr-index

A tiny UDP service that stores opaque bytes for a verified public `host:port`:

```text
SocketAddrV4 → opaque bytes
```

It exists because a Mainline provider record is just a `host:port`, and an iroh
endpoint is dialed by its `EndpointId`. Providers store a signed record under
their socket; readers translate the `host:port` pairs they get from `get_peers`.
The service does not depend on iroh, and could be useful for other applications
that use Mainline. The wire format is in [`udp-addr-index-proto`][proto].

A publisher first asks for a short-lived token. The server returns the packet's
observed public IPv4 socket and a stateless MAC bound to that socket. A put
carrying the token must arrive from the same socket; the server then derives the
key from the packet source and stores the bytes under its own receipt time and
TTL. Reads are direct and public. Requests are padded to 1200 bytes and shorter
ones are dropped, so a response can never be larger than the request that caused
it, and every response fits one unfragmented datagram. The server neither parses
nor validates the value it stores, and keeps everything in memory.

Index traffic shares the UDP socket of a Mainline node. Index datagrams start
with `\0addridx`; the leading zero byte cannot begin a bencoded KRPC message, so
both protocols can share a socket. Mainline announcements use their implied
source port, so the announced peer address and the index key describe the same
UDP mapping. A publisher behind a shared CGNAT address therefore cannot claim
another publisher's port.

## Running a server

```sh
cargo run -p udp-addr-index --features cli -- --dht-port 60125 \
  --rendezvous-hash b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b
```

The command binds all IPv4 interfaces at `--dht-port`, 60125 by default, so
allow inbound UDP there. It exits if its service or announcement task stops, and
shuts down on Ctrl-C or SIGTERM.

Servers can announce themselves under the rendezvous infohash
`b86c3d910e1a67ec9ba8a69a95bd7f8b08be923b`, the SHA-1 of
`iroh-addr-index servers v1`. The command announces only when you pass
`--rendezvous-hash`: use the hash above for default discovery, and omit the flag
when clients reach you through an explicit address or a signed list. An
announcement uses the implied source port, renews every ten minutes, and retries
after thirty seconds on failure.

To expose metrics for Prometheus, pass `--metrics-listen 127.0.0.1:9090` and
scrape `http://127.0.0.1:9090/metrics`. The listener is off unless requested and
has no authentication, so bind it to a trusted interface.

## Embedding

The crate is a library first: the command sits behind the `cli` feature, which
is off by default, so embedders never compile `clap` or `tracing-subscriber`.

`Server::attach(dht)` serves and announces on an existing Mainline node until the
returned handle is dropped. `Server::attach_with_rendezvous(dht, Some(hash))`
picks a different hash, and `None` serves without announcing.

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

[proto]: https://docs.rs/udp-addr-index-proto
