use crate::*;

#[test]
fn test_writer() {
    let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(1 << 5);
    assert_eq!(bytes_hash_map.len(), 0);
    assert!(bytes_hash_map.get(b"key1").is_none());
    let val_ref: &usize = bytes_hash_map.entry(b"key1").or_insert(1);
    assert_eq!(*val_ref, 1);
    assert_eq!(bytes_hash_map.get(b"key1").copied(), Some(1));
    let val_ref2 = bytes_hash_map.entry(b"key1").or_insert(2);
    assert_eq!(*val_ref2, 1);
}

fn test_capacity_aux(n: usize) {
    let mut hash_and_version = super::BytesHashMap::with_capacity(n);
    let cap = hash_and_version.capacity();
    assert!(cap >= n);
    for i in 0..cap {
        hash_and_version
            .entry(format!("{i}").as_bytes())
            .or_insert(1);
    }
    assert_eq!(cap, hash_and_version.capacity());

    hash_and_version
        .entry(format!("{cap}").as_bytes())
        .or_insert(1);
    assert!(hash_and_version.capacity() > cap);
}

#[test]
fn test_capacity() {
    for i in 0..10 {
        test_capacity_aux(i);
    }
}

#[test]
fn test_snapshots() {
    let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(1 << 5);
    let bytes_hash_map_read_only = bytes_hash_map.snapshot_provider();
    bytes_hash_map.entry(b"key1").or_insert(1);
    bytes_hash_map.entry(b"key2").or_insert(2);

    let snapshot_before_release = bytes_hash_map_read_only.snapshot();
    bytes_hash_map.release();
    let snapshot_after_release = bytes_hash_map_read_only.snapshot();
    assert_eq!(snapshot_before_release.len(), 0);
    assert!(snapshot_before_release.is_empty());

    let v1 = snapshot_before_release.get(b"key1").copied();
    let v2 = snapshot_before_release.get(b"key2").copied();
    assert!(v1.is_none());
    assert!(v2.is_none());

    let v1 = snapshot_after_release.get(b"key1").copied();
    let v2 = snapshot_after_release.get(b"key2").copied();
    assert_eq!(snapshot_after_release.len(), 2);
    assert_eq!(v1, Some(1));
    assert_eq!(v2, Some(2));
}

#[test]
fn test_resize() {
    let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(4);
    assert_eq!(bytes_hash_map.capacity(), 4);
    bytes_hash_map.entry(b"key1").or_insert(1);
    assert_eq!(bytes_hash_map.capacity(), 4);
    bytes_hash_map.entry(b"key2").or_insert(1);
    assert_eq!(bytes_hash_map.capacity(), 4);
    bytes_hash_map.entry(b"key3").or_insert(1);
    assert_eq!(bytes_hash_map.capacity(), 4);
    bytes_hash_map.entry(b"key4").or_insert(1);
    assert_eq!(bytes_hash_map.capacity(), 4);
    bytes_hash_map.entry(b"key5").or_insert(1);
    assert_eq!(bytes_hash_map.len(), 5);
    assert_eq!(bytes_hash_map.capacity(), 8);
    bytes_hash_map.entry(b"key6").or_insert(1);
    assert_eq!(bytes_hash_map.capacity(), 8);
}

#[test]
fn test_bucket_collision() {
    let num_buckets = 1 << 5;
    let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(num_buckets);
    let reader = bytes_hash_map.snapshot_provider();
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
    let reader = bytes_hash_map.snapshot_provider();
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

#[test]
fn test_entry_api() {
    let mut map = BytesHashMap::<u32>::with_capacity(16);
    let key = b"hello";

    // Test inserting a new key
    let value = match map.entry(key) {
        Entry::Occupied(_) => panic!("should be vacant"),
        Entry::Vacant(vacant) => vacant.insert(42),
    };
    assert_eq!(*value, 42);

    // Test getting an existing key
    let value = match map.entry(key) {
        Entry::Occupied(occupied) => occupied.get(),
        Entry::Vacant(_) => panic!("should be occupied"),
    };
    assert_eq!(*value, 42);
}

#[test]
fn test_or_insert_with() {
    let mut map = BytesHashMap::<u32>::with_capacity(16);
    let key = b"hello";

    // Test inserting a new key
    let value = map.entry(key).or_insert_with(|| 42);
    assert_eq!(*value, 42);

    // Test getting an existing key
    let value = map.entry(key).or_insert_with(|| 43);
    assert_eq!(*value, 42);
}

#[test]
fn test_pack_for_send() {
    let mut hash_map = BytesHashMap::default();
    hash_map.entry(b"hello").or_insert(10);
    let hash_map_packed_for_send = hash_map.release_and_pack_for_send();
    let join_handle = std::thread::spawn(move || {
        let mut hash_map = hash_map_packed_for_send.unpack();
        hash_map.entry(b"world").or_insert(20);
        assert_eq!(hash_map.get(b"hello").copied(), Some(10));
    });
    join_handle.join().unwrap();
}
