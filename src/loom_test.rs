use loom;
use crate::BytesHashMap;
use crate::concurrency_types::sync::atomic::Ordering;
use crate::concurrency_types::sync::atomic::AtomicUsize;

#[test]
fn test_concurrency_logic() {
    loom::model(|| {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::default();
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        bytes_hash_map.mutate_or_create(b"key",
            || { AtomicUsize::new(17) },
            |counter| { counter.fetch_add(1, Ordering::Relaxed); }
        );
        bytes_hash_map.release();
        bytes_hash_map.mutate_or_create(b"key",
            || { AtomicUsize::new(17) },
            |counter| { counter.fetch_add(1, Ordering::Relaxed); }
        );
        // There is a bug here: the bump memory may have been released too early!
        let handle = loom::thread::spawn(move || {
            let version = bytes_hash_map_read_only.version();
            let v1 = bytes_hash_map_read_only.get(b"key", version);
            assert!(version <= 2);
            if version == 0 {
                assert_eq!(version, 0);
                assert!(v1.is_none());
            }
            if version == 1 {
                let val1 = v1.unwrap().load(Ordering::Relaxed);
                assert!(val1 == 17 || val1 == 18);
            }
            if version == 2 {
                let val1 =v1.unwrap().load(Ordering::Relaxed);
                assert_eq!(val1, 18);
            }
        });
        bytes_hash_map.release();
    });
}


#[test]
fn test_concurrency_logic_all_or_nothing() {
    loom::model(|| {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::default();
        let bytes_hash_map_read_only = bytes_hash_map.read_only();
        bytes_hash_map.mutate_or_create(b"key",
            || { AtomicUsize::new(1) },
            |counter| {  }
        );
        bytes_hash_map.mutate_or_create(b"key2",
            || { AtomicUsize::new(2) },
            |counter| {  }
        );
        let handle = loom::thread::spawn(move || {
            let version = bytes_hash_map_read_only.version();
            let v1 = bytes_hash_map_read_only.get(b"key", version);
            let v2 = bytes_hash_map_read_only.get(b"key2", version);
            assert!(version <= 1);
            if version == 0 {
                assert!(v1.is_none());
                assert!(v2.is_none());
            }
            if version == 1 {
                let val1 = v1.unwrap().load(Ordering::Relaxed);
                let val2 = v2.unwrap().load(Ordering::Relaxed);
                assert_eq!(val1, 1);
                assert_eq!(val2, 2);
            }
        });
        bytes_hash_map.release();
    });
}
