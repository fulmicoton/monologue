mod concurrency_types;
mod hash_map;
mod murmurhash2;

use murmurhash2::murmurhash2;

pub use hash_map::BytesHashMap;
pub use hash_map::BytesHashMapReadOnly;

type HashType = u32;

#[cfg(test)]
mod tests {
    use crate::BytesHashMap;
    use crate::concurrency_types::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_insert_key() {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::with_capacity(1 << 5);
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        std::thread::spawn(move || {
            bytes_hash_map
                .entry(b"key")
                .or_insert(AtomicUsize::new(16))
                .fetch_add(1, Ordering::Relaxed);
            bytes_hash_map.release();
        });
        std::thread::spawn(move || {
            let version = bytes_hash_map_read_only.version();
            let v = bytes_hash_map_read_only.get_with_version(b"key", version);
            if version == 2 {
                assert!(v.is_some());
            } else {
                assert_eq!(version, 1);
                assert!(v.is_none());
            }
        });
    }

    #[test]
    fn simple_test2() {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::with_capacity(1 << 5);
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        bytes_hash_map
            .entry(b"key")
            .or_insert(AtomicUsize::new(16))
            .fetch_add(1, Ordering::Relaxed);
        bytes_hash_map.release();
        let version1 = bytes_hash_map_read_only.version();
        bytes_hash_map
            .entry(b"key2")
            .or_insert(AtomicUsize::new(16))
            .fetch_add(1, Ordering::Relaxed);
        bytes_hash_map.release();
        let version2 = bytes_hash_map_read_only.version();
        assert_eq!(version1, 2);
        assert_eq!(version2, 3);
        let v = bytes_hash_map_read_only.get_with_version(b"key", version1);
        assert!(v.is_some());
        let v2 = bytes_hash_map_read_only.get_with_version(b"key2", version1);
        assert!(v2.is_none());
    }

    #[test]
    fn test_bucket_collision() {
        let num_buckets = 1 << 5;
        let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(num_buckets);
        let reader = bytes_hash_map.read_only();
        let key1 = b"timbre";
        let key2 = b"timbromanist";
        assert_eq!(
            crate::murmurhash2(key1) % num_buckets as u32,
            crate::murmurhash2(key2) % num_buckets as u32
        );
        assert_ne!(crate::murmurhash2(key1), crate::murmurhash2(key2));
        bytes_hash_map.entry(key1).or_insert(0);
        bytes_hash_map.entry(key2).or_insert(1);
        {
            let version = reader.version();
            assert_eq!(version, 1);
            assert!(reader.get_with_version(key1, 1).is_none());
            assert!(reader.get_with_version(key2, 1).is_none());
        }
        bytes_hash_map.release();
        {
            let version = reader.version();
            assert_eq!(version, 2);
            assert_eq!(reader.get_with_version(key1, version).copied(), Some(0));
            assert_eq!(reader.get_with_version(key2, version).copied(), Some(1));
        }
    }

    #[test]
    fn test_hash_collision() {
        let num_buckets = 1 << 5;
        let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(num_buckets);
        let reader = bytes_hash_map.read_only();
        let key1 = b"antiphlogistin";
        let key2 = b"monosomic";
        assert_eq!(crate::murmurhash2(key1), crate::murmurhash2(key2));
        bytes_hash_map.entry(key1).or_insert(0);
        bytes_hash_map.entry(key2).or_insert(1);
        {
            let version = reader.version();
            assert_eq!(version, 1);
            assert!(reader.get_with_version(key1, 1).is_none());
            assert!(reader.get_with_version(key2, 1).is_none());
        }
        bytes_hash_map.release();
        {
            let version = reader.version();
            assert_eq!(version, 2);
            assert_eq!(reader.get_with_version(key1, version).copied(), Some(0));
            assert_eq!(reader.get_with_version(key2, version).copied(), Some(1));
        }
    }
}

#[cfg(all(loom, test))]
mod loom_test;
