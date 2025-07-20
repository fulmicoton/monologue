use crate::BytesHashMap;
use crate::concurrency_types::sync::atomic::AtomicUsize;
use crate::concurrency_types::sync::atomic::Ordering;
use loom;

#[test]
fn test_concurrency_creation_then_mutation() {
    // Here we test a simple key insertion, followed by a key mutation
    loom::model(|| {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::with_capacity(1 << 4);
        let bytes_hash_map_read_only = bytes_hash_map.read_only();

        let writing_thread_handle = loom::thread::spawn(move || {
            bytes_hash_map
                .entry(b"key")
                .or_insert(AtomicUsize::new(16))
                .fetch_add(1, Ordering::Relaxed);
            bytes_hash_map.release();
            bytes_hash_map
                .entry(b"key")
                .or_insert(AtomicUsize::new(17))
                .fetch_add(1, Ordering::Relaxed);
            bytes_hash_map.release();
        });
        // There is a bug here: the bump memory may have been released too early!
        let reading_thread_handle = loom::thread::spawn(move || {
            let snapshot = bytes_hash_map_read_only.snapshot();
            let version = snapshot.version();
            let v1 = snapshot.get(b"key");
            assert!(version <= 3);
            assert!(version >= 1);
            if version == 1 {
                assert!(v1.is_none());
            }
            if version == 2 {
                let val1 = v1.unwrap().load(Ordering::Relaxed);
                assert!(val1 == 17 || val1 == 18);
            }
            if version == 3 {
                let val1 = v1.unwrap().load(Ordering::Relaxed);
                assert_eq!(val1, 18);
            }
        });
        reading_thread_handle.join().unwrap();
        writing_thread_handle.join().unwrap();
    });
}

#[test]
fn test_concurrency_logic_all_or_nothing() {
    // Here we make sure that either a client sees all of the changes or they see none.
    loom::model(|| {
        let mut bytes_hash_map: BytesHashMap<AtomicUsize> = BytesHashMap::with_capacity(1 << 4);
        let bytes_hash_map_read_only = bytes_hash_map.read_only();

        let writing_thread_handle = loom::thread::spawn(move || {
            bytes_hash_map.entry(b"key").or_insert(AtomicUsize::new(1));
            bytes_hash_map.entry(b"key2").or_insert(AtomicUsize::new(2));
            bytes_hash_map.release();
        });
        let reading_thread_handle = loom::thread::spawn(move || {
            let snapshot = bytes_hash_map_read_only.snapshot();
            let version = snapshot.version();
            let v1 = snapshot.get(b"key");
            let v2 = snapshot.get(b"key2");
            assert!(version >= 1);
            assert!(version <= 2);
            if version == 1 {
                assert!(v1.is_none());
                assert!(v2.is_none());
            }
            if version == 2 {
                let val1 = v1.unwrap().load(Ordering::Relaxed);
                let val2 = v2.unwrap().load(Ordering::Relaxed);
                assert_eq!(val1, 1);
                assert_eq!(val2, 2);
            }
        });
        writing_thread_handle.join().unwrap();
        reading_thread_handle.join().unwrap();
    });
}
