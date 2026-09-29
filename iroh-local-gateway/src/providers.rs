//! Optional content validation between provider discovery and downloading.

use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

use iroh::{Endpoint, EndpointId};
use iroh_blobs::Hash;
use n0_future::{FuturesUnordered, Stream, StreamExt, stream};
use tokio::time::Instant;
use tracing::debug;

use crate::provider_cache::ProviderCache;

const CONCURRENT_PROBES: usize = 3;
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a single probe may take before others start alongside it.
///
/// A candidate that stalls would otherwise hold up the rest until its probe
/// times out.
const HEDGE_DELAY: Duration = Duration::from_secs(2);

/// Where a candidate provider came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Provenance {
    /// Announced for the content and found through discovery: a claim.
    Discovered,
    /// Passed a probe for the content at `verified`.
    ///
    /// Probed again all the same for now; a recent one could skip it.
    Verified {
        /// When the probe passed.
        verified: Instant,
    },
}

/// Filters a provider stream to endpoints that serve a verified size for `hash`.
///
/// This optional stage is independent of discovery. Each distinct endpoint is
/// connected to and queried for the blob's last Bao chunk, validating the size
/// against the requested hash rather than trusting the response's size header.
/// Probes run one at a time until one fails or takes longer than two
/// seconds, then up to three at once, each with a ten-second total deadline.
/// A good first candidate thus pulls a single item from `providers`, so a
/// lazy lookup chained behind candidates the caller already knows does not
/// start. Failed probes are logged and
/// skipped; endpoints are yielded in completion order. Dropping the stream
/// cancels pending probes.
///
/// Only endpoint IDs are returned. The consumer connects again for downloading;
/// successful validation does not guarantee that this later transfer succeeds.
/// The caller should impose an overall deadline on discovery and filtering.
pub fn filter_verified_providers(
    endpoint: Endpoint,
    hash: Hash,
    providers: impl Stream<Item = EndpointId> + Send + 'static,
) -> stream::Boxed<EndpointId> {
    verified_providers(
        endpoint,
        hash,
        providers.map(|provider| (provider, Provenance::Discovered)),
        None,
    )
}

/// Like [`filter_verified_providers`], recording each probe's outcome in `cache`.
pub(crate) fn verified_providers(
    endpoint: Endpoint,
    hash: Hash,
    providers: impl Stream<Item = (EndpointId, Provenance)> + Send + 'static,
    cache: Option<Arc<ProviderCache>>,
) -> stream::Boxed<EndpointId> {
    probe_adaptively(providers, move |provider, provenance| {
        let endpoint = endpoint.clone();
        let cache = cache.clone();
        async move {
            let passed = probe(&endpoint, hash, provider, provenance).await;
            if let Some(cache) = cache {
                if passed {
                    cache.confirm(hash, provider);
                } else {
                    cache.forget(hash, provider);
                }
            }
            passed
        }
    })
}

/// Returns whether `provider` serves a verified size for `hash`.
async fn probe(
    endpoint: &Endpoint,
    hash: Hash,
    provider: EndpointId,
    provenance: Provenance,
) -> bool {
    let started = Instant::now();
    debug!(%hash, %provider, ?provenance, "probing provider with verified size request");
    let probe = async {
        let connection = endpoint.connect(provider, iroh_blobs::ALPN).await?;
        crate::verified_size(&connection, hash).await
    };
    match tokio::time::timeout(PROBE_TIMEOUT, probe).await {
        Ok(Ok(size)) => {
            debug!(%hash, %provider, size, elapsed_ms = started.elapsed().as_millis(), "provider size validated");
            true
        }
        Ok(Err(error)) => {
            debug!(%hash, %provider, ?error, elapsed_ms = started.elapsed().as_millis(), "provider probe failed; skipping");
            false
        }
        Err(_) => {
            debug!(%hash, %provider, elapsed_ms = started.elapsed().as_millis(), "provider probe timed out; skipping");
            false
        }
    }
}

/// Yields the candidates whose probe passes, skipping repeated endpoints.
///
/// Probes one candidate at a time until a probe fails or takes longer than
/// [`HEDGE_DELAY`], then up to [`CONCURRENT_PROBES`] at once, and pulls a
/// candidate only when a probe slot is free.
fn probe_adaptively<F>(
    providers: impl Stream<Item = (EndpointId, Provenance)> + Send + 'static,
    probe: impl Fn(EndpointId, Provenance) -> F + Send + 'static,
) -> stream::Boxed<EndpointId>
where
    F: Future<Output = bool> + Send + 'static,
{
    let stream = async_stream::stream! {
        let mut providers = Box::pin(providers);
        let mut seen = HashSet::new();
        let mut probes = FuturesUnordered::new();
        let mut limit = 1;
        let mut exhausted = false;
        let mut hedge = Box::pin(tokio::time::sleep(HEDGE_DELAY));
        while !(exhausted && probes.is_empty()) {
            tokio::select! {
                next = providers.next(), if !exhausted && probes.len() < limit => match next {
                    Some((provider, provenance)) => {
                        if seen.insert(provider) {
                            let passed = probe(provider, provenance);
                            probes.push(async move { (provider, passed.await) });
                            hedge.as_mut().reset(Instant::now() + HEDGE_DELAY);
                        }
                    }
                    None => exhausted = true,
                },
                Some((provider, passed)) = probes.next(), if !probes.is_empty() => {
                    if passed {
                        yield provider;
                    } else {
                        limit = CONCURRENT_PROBES;
                    }
                }
                () = &mut hedge, if limit == 1 && !probes.is_empty() => {
                    limit = CONCURRENT_PROBES;
                }
            }
        }
    };
    stream.boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use iroh::SecretKey;

    use super::*;

    fn provider() -> EndpointId {
        SecretKey::generate().public()
    }

    /// Candidates that count how many were pulled.
    fn counted(
        candidates: Vec<EndpointId>,
        pulled: Arc<AtomicUsize>,
    ) -> impl Stream<Item = (EndpointId, Provenance)> + Send + 'static {
        stream::iter(candidates).map(move |provider| {
            pulled.fetch_add(1, Ordering::SeqCst);
            (provider, Provenance::Discovered)
        })
    }

    #[tokio::test]
    async fn a_good_first_candidate_pulls_nothing_else() {
        let pulled = Arc::new(AtomicUsize::new(0));
        let good = provider();
        let candidates = vec![good, provider(), provider()];
        let mut found =
            probe_adaptively(counted(candidates, pulled.clone()), |_, _| async { true });
        assert_eq!(found.next().await, Some(good));
        assert_eq!(pulled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_widens_probing_to_several_at_once() {
        let pulled = Arc::new(AtomicUsize::new(0));
        let bad = provider();
        let candidates: Vec<_> = std::iter::once(bad)
            .chain((0..5).map(|_| provider()))
            .collect();
        let good = candidates[5];
        let probing = Arc::new(Mutex::new(Vec::new()));
        let mut found = probe_adaptively(counted(candidates, pulled.clone()), {
            let probing = probing.clone();
            move |candidate, _| {
                let probing = probing.clone();
                async move {
                    probing.lock().unwrap().push(candidate);
                    // Only the last one serves the content, after a while.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    candidate == good
                }
            }
        });
        assert_eq!(found.next().await, Some(good));
        // The failure came after a second, then three probes ran at once:
        // candidates two to four fail together, then five and six are tried.
        assert_eq!(probing.lock().unwrap().len(), 6);
        assert_eq!(pulled.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_candidate_holds_up_the_rest_only_briefly() {
        let stalled = provider();
        let good = provider();
        let started = Instant::now();
        let mut found = probe_adaptively(
            stream::iter([
                (stalled, Provenance::Discovered),
                (good, Provenance::Discovered),
            ]),
            move |candidate, _| async move {
                if candidate == stalled {
                    std::future::pending::<()>().await;
                }
                true
            },
        );
        assert_eq!(found.next().await, Some(good));
        assert_eq!(started.elapsed(), HEDGE_DELAY);
    }

    #[tokio::test]
    async fn repeated_candidates_are_probed_once() {
        let twice = provider();
        let probed = Arc::new(AtomicUsize::new(0));
        let candidates = stream::iter([
            (
                twice,
                Provenance::Verified {
                    verified: Instant::now(),
                },
            ),
            (twice, Provenance::Discovered),
        ]);
        let found: Vec<_> = probe_adaptively(candidates, {
            let probed = probed.clone();
            move |_, _| {
                probed.fetch_add(1, Ordering::SeqCst);
                async { false }
            }
        })
        .collect()
        .await;
        assert!(found.is_empty());
        assert_eq!(probed.load(Ordering::SeqCst), 1);
    }
}
