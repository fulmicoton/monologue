mod concurrency_types;
mod hash_map;
mod linear_probing;
mod murmurhash2;

pub use linear_probing::LinearProbing;
pub use murmurhash2::murmurhash2;

pub use hash_map::BytesHashMap;
pub use hash_map::BytesHashMapReadOnly;
pub use hash_map::BytesHashMapSnapshot;

type HashType = u32;

#[cfg(test)]
mod tests {
    use crate::BytesHashMap;

    #[test]
    fn test_insert_key() {
        let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(1 << 5);
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        std::thread::spawn(move || {
            bytes_hash_map.entry(b"key1").or_insert(1);
            bytes_hash_map.entry(b"key2").or_insert(2);
            bytes_hash_map.release();
        });
        std::thread::spawn(move || {
            let snapshot = bytes_hash_map_read_only.snapshot();
            let v1 = snapshot.get(b"key1").copied();
            let v2 = snapshot.get(b"key2").copied();
            assert!(v1.is_some());
            assert!(v2.is_some());
            if let Some(v) = v1 {
                assert_eq!(v, 1);
                assert_eq!(v2, Some(2));
                assert_eq!(snapshot.len(), 2);
                assert!(!snapshot.is_empty());
            } else {
                assert_eq!(snapshot.len(), 0);
                assert!(snapshot.is_empty());
            }
        });
    }

    #[test]
    fn test_snapshot() {
        let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(1 << 5);
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        bytes_hash_map.entry(b"key").or_insert(1);
        bytes_hash_map.release();
        let snapshot1 = bytes_hash_map_read_only.snapshot();
        bytes_hash_map.entry(b"key2").or_insert(2);
        bytes_hash_map.release();
        let snapshot2 = bytes_hash_map_read_only.snapshot();
        let k1_v1 = snapshot1.get(b"key").copied();
        assert!(k1_v1.is_some());
        let k2_v1 = snapshot1.get(b"key2").copied();
        assert!(k2_v1.is_none());
        let k2_v2 = snapshot2.get(b"key2").copied();
        assert_eq!(k2_v2, Some(2));
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
            let snapshot1 = reader.snapshot();
            assert!(snapshot1.get(key1,).is_none());
            assert!(snapshot1.get(key2,).is_none());
        }
        {
            let snapshot1 = reader.snapshot();
            assert!(snapshot1.get(key1,).is_none());
            assert!(snapshot1.get(key2,).is_none());
        }
        bytes_hash_map.release();
        {
            let snapshot2 = reader.snapshot();
            assert_eq!(snapshot2.get(key1).copied(), Some(0));
            assert_eq!(snapshot2.get(key2).copied(), Some(1));
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
            let snapshot1 = reader.snapshot();
            assert!(snapshot1.get(key1).is_none());
            assert!(snapshot1.get(key2).is_none());
        }
        bytes_hash_map.release();
        {
            let snapshot2 = reader.snapshot();
            assert_eq!(snapshot2.get(key1).copied(), Some(0));
            assert_eq!(snapshot2.get(key2).copied(), Some(1));
        }
    }
}

#[cfg(all(loom, test))]
mod loom_test;
