//! Short-lived cache of index lookups, shared by the clones of an `AddrIndex`.

use std::{
    collections::HashMap,
    net::SocketAddrV4,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use lru::LruCache;
use tokio::time::Instant;

use crate::SignedRecord;

/// Sockets remembered at once.
const CAPACITY: NonZeroUsize = NonZeroUsize::new(4096).expect("nonzero");

/// Longest time an empty result is remembered.
///
/// A socket without a record is exactly what changes when a provider starts
/// publishing, so a miss is kept only briefly.
const MISS_TTL: Duration = Duration::from_secs(30);

/// Remembers which endpoints listed a socket.
///
/// Records are signed and bound to their socket, and the endpoint is
/// authenticated when dialed, so a stale entry costs one failed dial at most.
#[derive(Debug)]
pub(crate) struct LookupCache {
    ttl: Duration,
    entries: Mutex<LruCache<SocketAddrV4, (Instant, Vec<SignedRecord>)>>,
    /// One lock per socket being looked up, so concurrent callers share a request.
    pending: Mutex<HashMap<SocketAddrV4, Weak<tokio::sync::Mutex<()>>>>,
}

impl LookupCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(LruCache::new(CAPACITY)),
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the records remembered for `addr`, unless they expired.
    pub(crate) fn get(&self, addr: SocketAddrV4) -> Option<Vec<SignedRecord>> {
        let mut entries = self.entries.lock().expect("poisoned");
        if entries
            .peek(&addr)
            .is_some_and(|(expires, _)| *expires <= Instant::now())
        {
            entries.pop(&addr);
            return None;
        }
        entries.get(&addr).map(|(_, records)| records.clone())
    }

    /// Remembers a successful lookup; an empty one for at most [`MISS_TTL`].
    pub(crate) fn insert(&self, addr: SocketAddrV4, records: Vec<SignedRecord>) {
        let ttl = if records.is_empty() {
            self.ttl.min(MISS_TTL)
        } else {
            self.ttl
        };
        if ttl.is_zero() {
            return;
        }
        self.entries
            .lock()
            .expect("poisoned")
            .put(addr, (Instant::now() + ttl, records));
    }

    /// Returns the lock for looking up `addr`, shared with concurrent callers.
    pub(crate) fn pending(&self, addr: SocketAddrV4) -> Arc<tokio::sync::Mutex<()>> {
        let mut pending = self.pending.lock().expect("poisoned");
        if let Some(lock) = pending.get(&addr).and_then(Weak::upgrade) {
            return lock;
        }
        // Forget finished lookups, so the map only holds the ones in flight.
        pending.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        pending.insert(addr, Arc::downgrade(&lock));
        lock
    }
}

#[cfg(test)]
mod tests {
    use iroh_base::SecretKey;

    use super::*;

    const ADDR: &str = "203.0.113.7:60125";

    fn addr() -> SocketAddrV4 {
        ADDR.parse().unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn hits_expire_after_the_ttl() {
        let cache = LookupCache::new(Duration::from_secs(300));
        let records = vec![SignedRecord::sign(&SecretKey::generate(), addr())];
        cache.insert(addr(), records.clone());
        tokio::time::advance(Duration::from_secs(299)).await;
        assert_eq!(cache.get(addr()), Some(records));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(cache.get(addr()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn misses_are_kept_briefly_and_zero_disables_caching() {
        let cache = LookupCache::new(Duration::from_secs(300));
        cache.insert(addr(), vec![]);
        tokio::time::advance(MISS_TTL - Duration::from_secs(1)).await;
        assert_eq!(cache.get(addr()), Some(vec![]));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(cache.get(addr()), None);
        let disabled = LookupCache::new(Duration::ZERO);
        disabled.insert(
            addr(),
            vec![SignedRecord::sign(&SecretKey::generate(), addr())],
        );
        assert_eq!(disabled.get(addr()), None);
    }

    #[test]
    fn concurrent_lookups_share_one_lock_until_they_finish() {
        let cache = LookupCache::new(Duration::from_secs(300));
        let first = cache.pending(addr());
        assert!(Arc::ptr_eq(&first, &cache.pending(addr())));
        let other = cache.pending("198.51.100.7:60125".parse().unwrap());
        assert!(!Arc::ptr_eq(&first, &other));
        drop(first);
        drop(other);
        cache.pending(addr());
        // Finished lookups were forgotten when the new one started.
        assert_eq!(cache.pending.lock().unwrap().len(), 1);
    }
}
