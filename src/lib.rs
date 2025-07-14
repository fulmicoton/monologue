mod concurrency_types;
mod hash_map;

pub use hash_map::BytesHashMap;
pub use hash_map::BytesHashMapReadOnly;

#[cfg(test)]
mod tests {
    use crate::concurrency_types::sync::atomic::{AtomicUsize, Ordering};
    use crate::BytesHashMap;

    #[test]
    fn simple_test() {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::default();
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        bytes_hash_map.mutate_or_create(b"key",
            || { AtomicUsize::new(17) },
            |counter| { counter.fetch_add(1, Ordering::Relaxed); }
        );
        bytes_hash_map.release();
        let version = bytes_hash_map_read_only.version();
        let v = bytes_hash_map_read_only.get(b"key", version);
        if version == 1 {
            assert!(v.is_some());
        } else {
            assert_eq!(version, 0);
            assert!(v.is_none());
        }
    }

    #[test]
    fn simple_test2() {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::default();
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        bytes_hash_map.mutate_or_create(b"key",
            || { AtomicUsize::new(17) },
            |counter| { counter.fetch_add(1, Ordering::Relaxed); }
        );
        bytes_hash_map.release();
        let version1 = bytes_hash_map_read_only.version();
        bytes_hash_map.mutate_or_create(b"key2",
            || { AtomicUsize::new(17) },
            |counter| { counter.fetch_add(1, Ordering::Relaxed); }
         );
        bytes_hash_map.release();
        let version2 = bytes_hash_map_read_only.version();
        assert_eq!(version1, 1);
        assert_eq!(version2, 2);
        let v = bytes_hash_map_read_only.get(b"key", version1);
        assert!(v.is_some());
        let v2 = bytes_hash_map_read_only.get(b"key2", version1);
        assert!(v2.is_none());
    }
}


#[cfg(all(loom, test))]
mod loom_test;
