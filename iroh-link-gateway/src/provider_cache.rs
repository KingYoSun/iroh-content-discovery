//! Remembers which endpoints recently served a hash, and which hashes nobody did.

use std::{num::NonZeroUsize, sync::Mutex, time::Duration};

use iroh::EndpointId;
use iroh_blobs::Hash;
use lru::LruCache;
use tokio::time::Instant;

/// How long a provider stays known after it passed a probe.
const PROVIDER_TTL: Duration = Duration::from_secs(5 * 60);
/// How long a lookup that found nobody is remembered.
///
/// Short, since a provider may start announcing at any time; long enough that
/// a page asking for the same missing hash many times costs one lookup.
const MISS_TTL: Duration = Duration::from_secs(5);
/// Providers remembered per hash; the least recently verified ones go first.
const MAX_PROVIDERS: usize = 4;
/// Hashes remembered at once.
const SLOTS: NonZeroUsize = NonZeroUsize::new(4096).expect("nonzero");

/// A provider's last passed probe for a hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Probed {
    /// When the probe passed.
    pub(crate) at: Instant,
    /// How long it took, connecting included.
    pub(crate) latency: Duration,
}

/// Why a lookup for a hash came back without a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Miss {
    /// The lookup ended without a provider that passed its probe.
    NotFound,
    /// The lookup ran out of time.
    TimedOut,
}

#[derive(Debug, Default)]
struct Entry {
    /// Providers and their last passed probe, most recent first.
    providers: Vec<(EndpointId, Probed)>,
    missing: Option<(Miss, Instant)>,
}

/// Recently verified providers per hash, and recent failed lookups.
///
/// Known providers are tried before a new lookup, which then only starts if
/// none of them passes its probe. They only augment discovery: a provider is
/// added after it passed a probe and removed after it failed one.
#[derive(Debug)]
pub(crate) struct ProviderCache {
    entries: Mutex<LruCache<Hash, Entry>>,
}

impl Default for ProviderCache {
    fn default() -> Self {
        Self {
            entries: Mutex::new(LruCache::new(SLOTS)),
        }
    }
}

impl ProviderCache {
    /// Returns the providers that recently passed a probe for `hash`, with
    /// that probe, fastest first.
    ///
    /// Only the order depends on how fast a probe was. Which providers are
    /// kept depends on when they last passed one, since each probe replaces
    /// the last measurement and a fast provider may become slow.
    pub(crate) fn providers(&self, hash: Hash) -> Vec<(EndpointId, Probed)> {
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("poisoned");
        let Some(entry) = entries.get_mut(&hash) else {
            return Vec::new();
        };
        entry
            .providers
            .retain(|(_, probed)| now.saturating_duration_since(probed.at) < PROVIDER_TTL);
        let mut providers = entry.providers.clone();
        providers.sort_by_key(|(_, probed)| probed.latency);
        providers
    }

    /// Records that `provider` passed a probe for `hash` that took `latency`.
    pub(crate) fn confirm(&self, hash: Hash, provider: EndpointId, latency: Duration) {
        let mut entries = self.entries.lock().expect("poisoned");
        let entry = entries.get_or_insert_mut(hash, Entry::default);
        entry.providers.retain(|(known, _)| *known != provider);
        let probed = Probed {
            at: Instant::now(),
            latency,
        };
        entry.providers.insert(0, (provider, probed));
        entry.providers.truncate(MAX_PROVIDERS);
        entry.missing = None;
    }

    /// Records that `provider` failed a probe for `hash`.
    pub(crate) fn forget(&self, hash: Hash, provider: EndpointId) {
        let mut entries = self.entries.lock().expect("poisoned");
        if let Some(entry) = entries.get_mut(&hash) {
            entry.providers.retain(|(known, _)| *known != provider);
        }
    }

    /// Returns how a recent lookup for `hash` failed, if one did.
    pub(crate) fn missing(&self, hash: Hash) -> Option<Miss> {
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("poisoned");
        let entry = entries.get_mut(&hash)?;
        match entry.missing {
            Some((miss, at)) if now.saturating_duration_since(at) < MISS_TTL => Some(miss),
            _ => {
                entry.missing = None;
                None
            }
        }
    }

    /// Records that a lookup for `hash` found no provider.
    pub(crate) fn record_missing(&self, hash: Hash, miss: Miss) {
        let mut entries = self.entries.lock().expect("poisoned");
        entries.get_or_insert_mut(hash, Entry::default).missing = Some((miss, Instant::now()));
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    const LATENCY: Duration = Duration::from_millis(80);

    fn provider() -> EndpointId {
        SecretKey::generate().public()
    }

    fn ids(cache: &ProviderCache, hash: Hash) -> Vec<EndpointId> {
        cache
            .providers(hash)
            .into_iter()
            .map(|(provider, _)| provider)
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn confirmed_providers_expire_unless_renewed() {
        let cache = ProviderCache::default();
        let hash = Hash::new(b"content");
        let (old, new) = (provider(), provider());
        cache.confirm(hash, old, LATENCY);
        tokio::time::advance(Duration::from_secs(60)).await;
        cache.confirm(hash, new, Duration::from_millis(20));
        assert_eq!(ids(&cache, hash), vec![new, old]);
        let probes: Vec<_> = cache
            .providers(hash)
            .into_iter()
            .map(|(_, probed)| probed.latency)
            .collect();
        assert_eq!(probes, vec![Duration::from_millis(20), LATENCY]);
        // Another hash knows nothing about them.
        assert!(cache.providers(Hash::new(b"other")).is_empty());
        tokio::time::advance(PROVIDER_TTL - Duration::from_secs(60)).await;
        assert_eq!(ids(&cache, hash), vec![new]);
        // Confirming again renews the entry.
        cache.confirm(hash, new, LATENCY);
        tokio::time::advance(PROVIDER_TTL - Duration::from_secs(1)).await;
        assert_eq!(ids(&cache, hash), vec![new]);
    }

    #[test]
    fn fastest_first_but_the_oldest_are_dropped() {
        let cache = ProviderCache::default();
        let hash = Hash::new(b"content");
        let fast_but_old = provider();
        cache.confirm(hash, fast_but_old, Duration::from_millis(5));
        let slow = provider();
        cache.confirm(hash, slow, Duration::from_millis(500));
        let medium = provider();
        cache.confirm(hash, medium, Duration::from_millis(50));
        assert_eq!(ids(&cache, hash), vec![fast_but_old, medium, slow]);
        // A new measurement replaces the old one.
        cache.confirm(hash, slow, Duration::from_millis(1));
        assert_eq!(ids(&cache, hash), vec![slow, fast_but_old, medium]);
        // When the list is full, the least recently verified goes, however fast.
        for _ in 0..MAX_PROVIDERS - 2 {
            cache.confirm(hash, provider(), Duration::from_millis(100));
        }
        let known = ids(&cache, hash);
        assert_eq!(known.len(), MAX_PROVIDERS);
        assert!(!known.contains(&fast_but_old));
        assert!(known.contains(&slow) && known.contains(&medium));
    }

    #[test]
    fn failed_probes_remove_and_the_list_stays_short() {
        let cache = ProviderCache::default();
        let hash = Hash::new(b"content");
        let providers: Vec<_> = (0..MAX_PROVIDERS + 2).map(|_| provider()).collect();
        for provider in &providers {
            cache.confirm(hash, *provider, LATENCY);
        }
        let known = ids(&cache, hash);
        assert_eq!(known.len(), MAX_PROVIDERS);
        assert_eq!(known[0], *providers.last().unwrap());
        cache.forget(hash, known[0]);
        assert_eq!(ids(&cache, hash), known[1..]);
    }

    #[tokio::test(start_paused = true)]
    async fn misses_are_remembered_briefly_and_cleared_by_a_provider() {
        let cache = ProviderCache::default();
        let hash = Hash::new(b"content");
        assert_eq!(cache.missing(hash), None);
        cache.record_missing(hash, Miss::TimedOut);
        tokio::time::advance(MISS_TTL - Duration::from_millis(1)).await;
        assert_eq!(cache.missing(hash), Some(Miss::TimedOut));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(cache.missing(hash), None);
        cache.record_missing(hash, Miss::NotFound);
        cache.confirm(hash, provider(), LATENCY);
        assert_eq!(cache.missing(hash), None);
    }
}
