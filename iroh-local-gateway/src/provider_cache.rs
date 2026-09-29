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
/// Providers remembered per hash, most recently verified first.
const MAX_PROVIDERS: usize = 4;
/// Hashes remembered at once.
const SLOTS: NonZeroUsize = NonZeroUsize::new(4096).expect("nonzero");

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
    /// Providers and when they last passed a probe, most recent first.
    providers: Vec<(EndpointId, Instant)>,
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
    /// when they did, most recent first.
    pub(crate) fn providers(&self, hash: Hash) -> Vec<(EndpointId, Instant)> {
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("poisoned");
        let Some(entry) = entries.get_mut(&hash) else {
            return Vec::new();
        };
        entry
            .providers
            .retain(|(_, verified)| now.saturating_duration_since(*verified) < PROVIDER_TTL);
        entry.providers.clone()
    }

    /// Records that `provider` passed a probe for `hash`.
    pub(crate) fn confirm(&self, hash: Hash, provider: EndpointId) {
        let mut entries = self.entries.lock().expect("poisoned");
        let entry = entries.get_or_insert_mut(hash, Entry::default);
        entry.providers.retain(|(known, _)| *known != provider);
        entry.providers.insert(0, (provider, Instant::now()));
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
    async fn confirmed_providers_come_most_recent_first_and_expire() {
        let cache = ProviderCache::default();
        let hash = Hash::new(b"content");
        let (old, new) = (provider(), provider());
        cache.confirm(hash, old);
        tokio::time::advance(Duration::from_secs(60)).await;
        cache.confirm(hash, new);
        assert_eq!(ids(&cache, hash), vec![new, old]);
        // Another hash knows nothing about them.
        assert!(cache.providers(Hash::new(b"other")).is_empty());
        tokio::time::advance(PROVIDER_TTL - Duration::from_secs(60)).await;
        assert_eq!(ids(&cache, hash), vec![new]);
        // Confirming again renews the entry.
        cache.confirm(hash, new);
        tokio::time::advance(PROVIDER_TTL - Duration::from_secs(1)).await;
        assert_eq!(ids(&cache, hash), vec![new]);
    }

    #[test]
    fn failed_probes_remove_and_the_list_stays_short() {
        let cache = ProviderCache::default();
        let hash = Hash::new(b"content");
        let providers: Vec<_> = (0..MAX_PROVIDERS + 2).map(|_| provider()).collect();
        for provider in &providers {
            cache.confirm(hash, *provider);
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
        cache.confirm(hash, provider());
        assert_eq!(cache.missing(hash), None);
    }
}
