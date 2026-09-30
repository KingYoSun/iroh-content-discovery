//! `?debug`: what discovery finds right now, bypassing all caches.
//!
//! For a hash: serving stops at the first provider that passes a probe and
//! remembers what it found. This asks Mainline for peers, looks up every peer
//! in the address index and probes every endpoint found, so the page also
//! shows peers that did not resolve and providers that failed.
//!
//! For a Pkarr key: every answer Mainline returns, the newest record in the
//! zone format the iroh-share GUI edits, and the providers of the content it
//! points to.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddrV4,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    http::header,
    response::{IntoResponse, Response},
};
use hickory_proto::{
    ProtoError,
    op::Message,
    rr::{Name, RData, RecordType},
};
use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use iroh_mainline_endpoint_discovery::{AddrIndexError, SignedRecord, infohash_from_blake3};
use n0_future::{BufferedStreamExt, StreamExt, stream};
use simple_dns::Packet;
use tokio::time::Instant;

use crate::{Gateway, LISTING_CSS, html_escape, pkarr_redirect};

/// How long to keep collecting answers from Mainline.
const MAINLINE_TIMEOUT: Duration = Duration::from_secs(15);
/// Deadline for one probe, as when serving.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const CONCURRENT_LOOKUPS: usize = 16;
const CONCURRENT_PROBES: usize = 8;

/// One index record for a peer, or why there is none.
enum Resolution {
    Record(SignedRecord),
    NotInIndex,
    Failed(AddrIndexError),
}

/// Renders the providers page for `hash`.
pub(crate) async fn providers(gateway: &Gateway, hash: Hash) -> Response {
    let encoded = z32::encode(hash.as_bytes());
    let body = providers_section(gateway, hash).await;
    page(&format!("Providers of {encoded}"), &body)
}

/// Renders the Pkarr page for `key`, and the providers of its content target.
pub(crate) async fn pkarr(gateway: &Gateway, key: &[u8; 32], encoded: &str) -> Response {
    let started = Instant::now();
    let mut items = Vec::new();
    let lookup = async {
        let mut answers = gateway
            .0
            .resolver
            .dht()
            .get_mutable(key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        while let Some(item) = answers.next().await {
            items.push(item);
        }
        Ok::<_, String>(())
    };
    let note = mainline_note(tokio::time::timeout(MAINLINE_TIMEOUT, lookup).await);
    let mut body = format!(
        "<p class=\"meta\">{} answers from Mainline in {:.1} s. Bypasses all caches.</p>\n",
        items.len(),
        started.elapsed().as_secs_f64(),
    );
    if let Some(note) = note {
        body.push_str(&format!("<p class=\"meta\">{}</p>\n", html_escape(&note)));
    }
    // Several sequence numbers mean some nodes still hold an older record.
    let mut answers: BTreeMap<i64, usize> = BTreeMap::new();
    for item in &items {
        *answers.entry(item.seq()).or_default() += 1;
    }
    if !answers.is_empty() {
        body.push_str(
            "<table>\n<tr><td>Sequence</td><td class=\"size\">Published</td>\
             <td class=\"size\">Answers</td></tr>\n",
        );
        for (seq, count) in answers.iter().rev() {
            body.push_str(&format!(
                "<tr><td>{seq}</td><td class=\"size\">{}</td><td class=\"size\">{count}</td></tr>\n",
                published(*seq)
            ));
        }
        body.push_str("</table>\n");
    }
    let newest = items
        .iter()
        .filter(|item| item.seq() >= 0 && item.key() == key)
        .max_by_key(|item| item.seq());
    let mut content = None;
    match newest {
        None => body.push_str("<p>No record found.</p>\n"),
        Some(item) => {
            match text(encoded, item.value()) {
                Ok(text) => body.push_str(&format!(
                    "<h1>Record</h1>\n<pre>{}</pre>\n",
                    html_escape(&text)
                )),
                Err(error) => body.push_str(&format!(
                    "<p>Cannot show the record: {}</p>\n",
                    html_escape(&error)
                )),
            }
            let target = Packet::parse(item.value())
                .ok()
                .and_then(|packet| pkarr_redirect::target(&packet, encoded));
            match target {
                None => body.push_str("<p class=\"meta\">No supported apex HTTPS target.</p>\n"),
                Some(authority) => {
                    body.push_str(&format!(
                        "<p class=\"meta\">Points to https://{}/</p>\n",
                        html_escape(&authority)
                    ));
                    content = pkarr_redirect::content_hash(&authority);
                }
            }
        }
    }
    if let Some(hash) = content {
        body.push_str(&format!(
            "<h1>Providers of {}</h1>\n",
            z32::encode(hash.as_bytes())
        ));
        body.push_str(&providers_section(gateway, hash).await);
    }
    page(&format!("Pkarr {encoded}"), &body)
}

/// Describes when a sequence number was published, if it is a timestamp.
///
/// Pkarr publishers use microseconds since the Unix epoch.
fn published(seq: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_micros() as i64);
    // Anything before 2017 is not a microsecond timestamp.
    if seq < 1_500_000_000_000_000 {
        return String::new();
    }
    format!(
        "{} ago",
        format_age((now.saturating_sub(seq) / 1_000_000) as u64)
    )
}

/// Renders every peer Mainline returns for `hash`, with its index records and probes.
async fn providers_section(gateway: &Gateway, hash: Hash) -> String {
    let started = Instant::now();
    let (peers, peers_note) = peers(gateway, hash).await;
    let lookups: Vec<(SocketAddrV4, Resolution)> = stream::iter(peers.iter().copied())
        .map(|peer| {
            let index = gateway.0.resolver.index().clone();
            async move {
                let resolutions = match index.lookup_uncached(peer).await {
                    Ok(records) if records.is_empty() => vec![Resolution::NotInIndex],
                    Ok(records) => records.into_iter().map(Resolution::Record).collect(),
                    Err(error) => vec![Resolution::Failed(error)],
                };
                resolutions
                    .into_iter()
                    .map(move |r| (peer, r))
                    .collect::<Vec<_>>()
            }
        })
        .buffered_unordered(CONCURRENT_LOOKUPS)
        .flat_map(stream::iter)
        .collect()
        .await;
    let endpoints: BTreeSet<EndpointId> = lookups
        .iter()
        .filter_map(|(_, resolution)| match resolution {
            Resolution::Record(record) => Some(record.endpoint_id),
            _ => None,
        })
        .collect();
    let probes: HashMap<EndpointId, Result<Duration, String>> = stream::iter(endpoints)
        .map(|provider| {
            let endpoint = gateway.0.endpoint.clone();
            async move { (provider, probe(&endpoint, hash, provider).await) }
        })
        .buffered_unordered(CONCURRENT_PROBES)
        .collect()
        .await;
    render_providers(&peers, peers_note, lookups, &probes, started.elapsed())
}

/// Collects the peers Mainline returns for `hash`, and a note if that stopped early.
async fn peers(gateway: &Gateway, hash: Hash) -> (BTreeSet<SocketAddrV4>, Option<String>) {
    let infohash = infohash_from_blake3(&blake3::Hash::from_bytes(*hash.as_bytes()));
    let mut peers = BTreeSet::new();
    let lookup = async {
        let mut batches = gateway
            .0
            .resolver
            .dht()
            .get_peers(infohash.into())
            .await
            .map_err(|error| error.to_string())?;
        while let Some(batch) = batches.next().await {
            peers.extend(batch);
        }
        Ok::<_, String>(())
    };
    let note = mainline_note(tokio::time::timeout(MAINLINE_TIMEOUT, lookup).await);
    (peers, note)
}

/// Describes a Mainline lookup that did not run to completion.
fn mainline_note(
    result: Result<Result<(), String>, tokio::time::error::Elapsed>,
) -> Option<String> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("Mainline lookup failed: {error}")),
        Err(_) => Some(format!(
            "Stopped collecting answers after {} s.",
            MAINLINE_TIMEOUT.as_secs()
        )),
    }
}

/// Returns how long `provider` took to serve a verified size for `hash`, or why it did not.
async fn probe(endpoint: &Endpoint, hash: Hash, provider: EndpointId) -> Result<Duration, String> {
    let started = Instant::now();
    let attempt = async {
        let connection = endpoint.connect(provider, iroh_blobs::ALPN).await?;
        crate::verified_size(&connection, hash).await
    };
    match tokio::time::timeout(PROBE_TIMEOUT, attempt).await {
        Ok(Ok(_size)) => Ok(started.elapsed()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("timed out after {} s", PROBE_TIMEOUT.as_secs())),
    }
}

fn render_providers(
    peers: &BTreeSet<SocketAddrV4>,
    peers_note: Option<String>,
    mut rows: Vec<(SocketAddrV4, Resolution)>,
    probes: &HashMap<EndpointId, Result<Duration, String>>,
    elapsed: Duration,
) -> String {
    // Working providers first, fastest first, then failed probes, then peers
    // without a record.
    let rank = |resolution: &Resolution| match resolution {
        Resolution::Record(record) => match probes.get(&record.endpoint_id) {
            Some(Ok(latency)) => (0, *latency),
            _ => (1, Duration::ZERO),
        },
        Resolution::NotInIndex => (2, Duration::ZERO),
        Resolution::Failed(_) => (3, Duration::ZERO),
    };
    rows.sort_by(|(a_peer, a), (b_peer, b)| rank(a).cmp(&rank(b)).then(a_peer.cmp(b_peer)));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let mut html = format!(
        "<p class=\"meta\">{} peers from Mainline, {} endpoints, in {:.1} s. \
         Bypasses all caches.</p>\n",
        peers.len(),
        probes.len(),
        elapsed.as_secs_f64(),
    );
    if let Some(note) = peers_note {
        html.push_str(&format!("<p class=\"meta\">{}</p>\n", html_escape(&note)));
    }
    html.push_str(
        "<table>\n<tr><td>Peer</td><td>Endpoint</td><td class=\"size\">Record age</td>\
         <td class=\"size\">Probe</td></tr>\n",
    );
    for (peer, resolution) in &rows {
        let (endpoint, age, probe) = match resolution {
            Resolution::Record(record) => {
                let id = record.endpoint_id.to_string();
                let age = now.saturating_sub(record.payload.v1().ts);
                let probe = match probes.get(&record.endpoint_id) {
                    Some(Ok(latency)) => format!("{} ms", latency.as_millis()),
                    Some(Err(error)) => html_escape(error),
                    None => String::new(),
                };
                (
                    format!("<span title=\"{id}\">{id}</span>"),
                    format_age(age),
                    probe,
                )
            }
            Resolution::NotInIndex => ("not in index".into(), String::new(), String::new()),
            Resolution::Failed(error) => (
                html_escape(&format!("index lookup failed: {error}")),
                String::new(),
                String::new(),
            ),
        };
        html.push_str(&format!(
            "<tr><td>{peer}</td><td class=\"hash\">{endpoint}</td>\
             <td class=\"size\">{age}</td><td class=\"size\">{probe}</td></tr>\n"
        ));
    }
    html.push_str("</table>\n");
    html
}

fn page(title: &str, body: &str) -> Response {
    let title = html_escape(title);
    let html = format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{title}</title>\n<style>{LISTING_CSS}</style>\n<h1>{title}</h1>\n{body}"
    );
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        html,
    )
        .into_response()
}

fn format_age(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds} s"),
        60..3600 => format!("{} min", seconds / 60),
        _ => format!("{} h", seconds / 3600),
    }
}

/// Renders a DNS packet as the zone-style lines the iroh-share GUI edits.
///
/// Copied from `dns_records::text` in iroh-share, so both show a record the
/// same way.
fn text(key: &str, bytes: &[u8]) -> Result<String, String> {
    use std::fmt::Write;
    let origin: Name = format!("{key}.")
        .parse()
        .map_err(|e: ProtoError| e.to_string())?;
    let message = Message::from_vec(bytes).map_err(|e| e.to_string())?;
    let mut text = String::new();
    for record in message.answers() {
        let name = record.name();
        if !origin.zone_of(name) {
            return Err("record owner must be within this pkarr name".into());
        }
        let owner = if *name == origin {
            "@".to_owned()
        } else {
            let labels = name.num_labels() - origin.num_labels();
            let mut relative =
                Name::from_labels(name.iter().take(labels.into())).map_err(|e| e.to_string())?;
            relative.set_fqdn(false);
            relative.to_string()
        };
        let ttl = record.ttl();
        match record.data() {
            RData::Unknown {
                code: RecordType::Unknown(256),
                rdata,
            } => {
                let (header, target) = rdata
                    .anything()
                    .split_first_chunk::<4>()
                    .ok_or("URI record is too short")?;
                let priority = u16::from_be_bytes([header[0], header[1]]);
                let weight = u16::from_be_bytes([header[2], header[3]]);
                let target = quote(target);
                writeln!(text, "{owner} {ttl} IN URI {priority} {weight} {target}")
                    .expect("writing to a String");
            }
            RData::TXT(txt) => {
                let parts: Vec<_> = txt.txt_data().iter().map(|part| quote(part)).collect();
                writeln!(text, "{owner} {ttl} IN TXT {}", parts.join(" "))
                    .expect("writing to a String");
            }
            data => writeln!(text, "{owner} {ttl} IN {} {data}", record.record_type())
                .expect("writing to a String"),
        }
    }
    Ok(text)
}

/// Quotes a character string, escaping what the zone parser would interpret.
fn quote(bytes: &[u8]) -> String {
    let mut quoted = String::from('"');
    for &byte in bytes {
        match byte {
            b'"' | b'\\' => {
                quoted.push('\\');
                quoted.push(byte.into());
            }
            0x20..=0x7e => quoted.push(byte.into()),
            _ => quoted.push_str(&format!("\\{byte:03}")),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use simple_dns::{
        CLASS, Name as DnsName, ResourceRecord,
        rdata::{HTTPS, NULL, RData as DnsRData, SVCB},
    };

    use super::*;

    const KEY: &str = "5ti57aszf7kaicsncb4wgigkf9bju39kofiz8dthwdujkmz85u8y";

    #[test]
    fn records_render_like_the_iroh_share_gui() {
        let apex = DnsName::new_unchecked(KEY);
        let uri_owner = format!("_https._tcp.{KEY}");
        let mut with_port = SVCB::new(1, DnsName::new_unchecked("example.com"));
        with_port.set_port(8443);
        let mut uri = 0u16.to_be_bytes().to_vec();
        uri.extend(0u16.to_be_bytes());
        uri.extend(b"https://example.com/a?b");
        let mut packet = Packet::new_reply(0);
        packet.answers = vec![
            ResourceRecord::new(
                apex.clone(),
                CLASS::IN,
                300,
                DnsRData::HTTPS(HTTPS(SVCB::new(0, DnsName::new_unchecked("example.com")))),
            ),
            ResourceRecord::new(apex, CLASS::IN, 300, DnsRData::HTTPS(HTTPS(with_port))),
            ResourceRecord::new(
                DnsName::new_unchecked(&uri_owner),
                CLASS::IN,
                300,
                DnsRData::NULL(256, NULL::new(&uri).unwrap()),
            ),
        ];
        let bytes = packet.build_bytes_vec().unwrap();
        assert_eq!(
            text(KEY, &bytes).unwrap(),
            "@ 300 IN HTTPS 0 example.com.\n\
             @ 300 IN HTTPS 1 example.com. port=8443\n\
             _https._tcp 300 IN URI 0 0 \"https://example.com/a?b\"\n"
        );
    }
}
