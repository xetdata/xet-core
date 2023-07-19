use crate::log::ErrorPrinter;
use lru::LruCache;
use nfsserve::nfs::nfsstat3::NFS3ERR_IO;
use nfsserve::nfs::{fattr3, fileid3, nfsstat3};
use std::sync::{Mutex, MutexGuard};

/// Thread-safe Cache for file attributes (i.e. stat).
///
/// Note that since the underlying LruCache always requires mutable access
/// (even on reads), we just use a [Mutex] instead of a [std::sync::RwLock].
pub struct StatCache {
    cache: Mutex<LruCache<fileid3, fattr3>>,
}

impl StatCache {
    /// Creates a new StatCache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(capacity)),
        }
    }

    /// Get the atrributes for the given id from the cache if it exists, or else, Ok(None) is returned.
    pub fn get(&self, id: fileid3) -> Result<Option<fattr3>, nfsstat3> {
        // annoyingly this LRU cache implementation is not thread-safe and thus, requires mut
        // on a read. Ostensibly to update the read count.
        self.lock().map(|mut cache| cache.get(&id).cloned())
    }

    /// Associate the id with the given attribute.
    pub fn put(&self, id: fileid3, attr: fattr3) -> Result<(), nfsstat3> {
        self.lock().map(|mut cache| {
            cache.put(id, attr);
        })
    }

    /// Remove all entries in the cache.
    pub fn clear(&self) -> Result<(), nfsstat3> {
        self.lock().map(|mut cache| {
            cache.clear();
        })
    }

    /// Lock the statcache, returning an error if the lock is poisoned (and logging the error).
    fn lock(&self) -> Result<MutexGuard<'_, LruCache<fileid3, fattr3>>, nfsstat3> {
        self.cache
            .lock()
            .log_error("Couldn't open StatCache lock")
            .map_err(|_| NFS3ERR_IO)
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    use nfsserve::nfs::{ftype3, size3};

    fn get_test_attr(fileid: fileid3, size: size3) -> fattr3 {
        fattr3 {
            ftype: ftype3::NF3REG,
            mode: 0o444,
            nlink: 1,
            size,
            used: size,
            fileid,
            ..Default::default()
        }
    }

    // TODO: make fattr3 implement/derive PartialEq
    //       (needs external repo/crate change)
    fn check_attr(expected: fattr3, actual: fattr3) {
        assert_eq!(expected.fileid, actual.fileid);
        assert_eq!(expected.fsid, actual.fsid);
        assert_eq!(expected.uid, actual.uid);
        assert_eq!(expected.gid, actual.gid);
        assert_eq!(expected.mode, actual.mode);
        assert_eq!(expected.nlink, actual.nlink);
        assert_eq!(expected.size, actual.size);
        assert_eq!(expected.used, actual.used);
    }

    #[test]
    fn test_single() {
        let cache = StatCache::new(2);
        let attr1 = get_test_attr(1, 182);
        cache.put(1, attr1).unwrap();
        let attr = cache.get(1).unwrap().unwrap();
        check_attr(attr1, attr);
    }

    #[test]
    fn test_multi() {
        let cache = StatCache::new(2);
        let attr1 = get_test_attr(1, 182);
        let attr2 = get_test_attr(2, 57201);
        cache.put(1, attr1).unwrap();
        cache.put(2, attr2).unwrap();
        let attr = cache.get(1).unwrap().unwrap();
        check_attr(attr1, attr);
        let attr = cache.get(2).unwrap().unwrap();
        check_attr(attr2, attr);
    }

    #[test]
    fn test_get_not_found() {
        let cache = StatCache::new(2);
        let attr1 = get_test_attr(1, 182);
        cache.put(1, attr1).unwrap();
        assert!(cache.get(5).unwrap().is_none());
    }

    #[test]
    fn test_eviction() {
        let cache = StatCache::new(2);
        let attr1 = get_test_attr(1, 182);
        let attr2 = get_test_attr(2, 57201);
        let attr3 = get_test_attr(3, 7629);
        cache.put(1, attr1).unwrap();
        cache.put(2, attr2).unwrap();
        cache.put(3, attr3).unwrap();
        assert!(cache.get(1).unwrap().is_none()); // 1 was evicted
        let attr = cache.get(2).unwrap().unwrap();
        check_attr(attr2, attr);
        let attr = cache.get(3).unwrap().unwrap();
        check_attr(attr3, attr);
    }

    #[test]
    fn test_eviction_lru() {
        let cache = StatCache::new(2);
        let attr1 = get_test_attr(1, 182);
        let attr2 = get_test_attr(2, 57201);
        let attr3 = get_test_attr(3, 7629);
        cache.put(1, attr1).unwrap();
        cache.put(2, attr2).unwrap();
        // access 1 to make it more recently used
        let attr = cache.get(1).unwrap().unwrap();
        check_attr(attr1, attr);

        cache.put(3, attr3).unwrap();
        assert!(cache.get(2).unwrap().is_none()); // 2 was evicted
        let attr = cache.get(1).unwrap().unwrap();
        check_attr(attr1, attr);
        let attr = cache.get(3).unwrap().unwrap();
        check_attr(attr3, attr);
    }

    #[test]
    fn test_clear() {
        let cache = StatCache::new(5);
        let attr1 = get_test_attr(1, 182);
        let attr2 = get_test_attr(2, 57201);
        let attr3 = get_test_attr(3, 7629);
        cache.put(1, attr1).unwrap();
        cache.put(2, attr2).unwrap();
        cache.put(3, attr3).unwrap();

        cache.clear().unwrap();
        assert!(cache.get(1).unwrap().is_none());
        assert!(cache.get(2).unwrap().is_none());
        assert!(cache.get(3).unwrap().is_none());
    }
}
