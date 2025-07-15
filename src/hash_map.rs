use arc_swap::ArcSwap;

const MIN_ALIGN: usize = 8;
const DEFAULT_HASH_TABLE_SIZE: usize = 1 << 10;

use crate::HashType;
use crate::concurrency_types::arc_from_box;
use std::alloc::Layout;
use std::marker::PhantomData;

use crate::concurrency_types::sync::Arc;
use crate::concurrency_types::sync::atomic::{
    self, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};

#[derive(Clone, Default, Copy, Eq, PartialEq, Debug)]
struct HashAndVersion {
    hash: u32,
    version: u32,
}

impl HashAndVersion {
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.version == 0
    }
}

impl From<u64> for HashAndVersion {
    #[inline(always)]
    fn from(value: u64) -> Self {
        HashAndVersion {
            hash: (value >> 32) as u32,
            version: value as u32,
        }
    }
}

impl From<HashAndVersion> for u64 {
    #[inline(always)]
    fn from(has_and_version: HashAndVersion) -> u64 {
        ((has_and_version.hash as u64) << 32) | (has_and_version.version as u64)
    }
}

struct AtomicBucketEntry {
    hash_and_version: AtomicU64,
    // ptr is the address where the value and the key str are stored.
    // They are stored as follows
    // [ V: (sized) ][ key_len: native endian u16 ][ str bytes ]
    //
    // Bucket entry has no ownership over the data it points to.
    // It will be freed when the bumpalo Bump is dropped.
    // It is crucial to ensure this happens after the bucket entries are dropped.
    ptr: AtomicPtr<u8>,
}

impl Default for AtomicBucketEntry {
    fn default() -> AtomicBucketEntry {
        AtomicBucketEntry {
            hash_and_version: AtomicU64::new(u64::from(HashAndVersion::default())),
            ptr: Default::default(),
        }
    }
}

impl AtomicBucketEntry {
    #[inline(always)]
    pub fn load_hash_and_version(&self, ordering: Ordering) -> HashAndVersion {
        HashAndVersion::from(self.hash_and_version.load(ordering))
    }

    #[inline(always)]
    pub fn store_hash_and_version(&self, hash_and_version: HashAndVersion, ordering: Ordering) {
        self.hash_and_version
            .store(u64::from(hash_and_version), ordering);
    }
}

pub struct BytesHashMap<V> {
    buckets: Arc<[AtomicBucketEntry]>,
    mask: usize,
    data: Arc<BytesHashMapData<V>>,
    arena: bumpalo::Bump<MIN_ALIGN>,
}

impl<V> Drop for BytesHashMap<V> {
    fn drop(&mut self) {
        let bumpalo = std::mem::replace(&mut self.arena, bumpalo::Bump::with_min_align());
        let mut drop_on_last_lock = self.data.drop_on_last.lock().unwrap();
        *drop_on_last_lock = Some(bumpalo);
    }
}

impl<V> Default for BytesHashMap<V> {
    fn default() -> BytesHashMap<V> {
        BytesHashMap::with_capacity(DEFAULT_HASH_TABLE_SIZE)
    }
}

impl<V> BytesHashMap<V> {
    pub fn with_capacity(hash_table_size: usize) -> BytesHashMap<V> {
        let hash_table_size = hash_table_size.next_power_of_two();
        let mask = hash_table_size - 1;
        let buckets: Arc<[AtomicBucketEntry]> = arc_from_box(
            std::iter::repeat_with(AtomicBucketEntry::default)
                .take(hash_table_size)
                .collect::<Vec<AtomicBucketEntry>>()
                .into_boxed_slice(),
        );
        let data = BytesHashMapData::with_buckets(buckets.clone());
        BytesHashMap {
            buckets,
            mask,
            arena: bumpalo::Bump::with_min_align(),
            data: Arc::new(data),
        }
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a, V> {
        assert!(key.len() <= u16::MAX as usize);
        if self.data.len() * 2 >= self.buckets.len() {
            self.grow_hash_table();
        }
        let hash = crate::murmurhash2(key);
        let mut probe = LinearProbing::compute(hash, self.mask);
        loop {
            let bucket_id = probe.next_probe();
            let bucket_entry = &self.buckets[bucket_id];
            let hash_and_version = bucket_entry.load_hash_and_version(Ordering::Relaxed);
            if hash_and_version.is_empty() {
                return Entry::Vacant(VacantEntry {
                    key,
                    map: self,
                    bucket_id,
                    hash,
                });
            }
            if hash == hash_and_version.hash {
                let ptr: *mut u8 = bucket_entry.ptr.load(atomic::Ordering::Relaxed);
                if unsafe { is_key_equal::<V>(ptr, key) } {
                    let value = unsafe { &*(ptr as *const V) };
                    return Entry::Occupied(OccupiedEntry { key, value });
                }
            }
        }
    }

    fn grow_hash_table(&mut self) {
        let new_hash_table_size = self.capacity() * 2;
        let new_mask = new_hash_table_size - 1;
        let new_buckets: Arc<[AtomicBucketEntry]> = arc_from_box(
            std::iter::repeat_with(AtomicBucketEntry::default)
                .take(new_hash_table_size)
                .collect::<Vec<AtomicBucketEntry>>()
                .into_boxed_slice(),
        );
        // let's reinsert all elements in `new_buckets`.
        for bucket in self.buckets.iter() {
            let hash_and_version = bucket.load_hash_and_version(Ordering::Relaxed);
            if hash_and_version.is_empty() {
                continue;
            }
            let ptr = bucket.ptr.load(Ordering::Relaxed);
            let mut probe = LinearProbing::compute(hash_and_version.hash, new_mask);
            loop {
                let bucket_id = probe.next_probe();
                let target_bucket_entry = &new_buckets[bucket_id];
                if target_bucket_entry
                    .load_hash_and_version(Ordering::Relaxed)
                    .is_empty()
                {
                    target_bucket_entry.store_hash_and_version(hash_and_version, Ordering::Relaxed);
                    target_bucket_entry.ptr.store(ptr, Ordering::Relaxed);
                    break;
                }
            }
        }
        self.buckets = new_buckets.clone();
        self.data.buckets.store(std::sync::Arc::new(new_buckets));
        self.mask = new_mask;
    }

    /// Publishes all changes made to the `BytesHashMap` since the last `release` call,
    /// making them atomically visible to all `BytesHashMapReadOnly` instances.
    ///
    /// This method is the key to the single-writer, multiple-reader concurrency model.
    /// All write operations (`entry`, `or_insert`, etc.) are buffered and are not
    /// visible to readers until this method is called.
    ///
    /// This allows the writer to batch a series of changes and make them available
    /// to readers as a single, atomic update. Each call to `release` increments
    /// the internal version of the hash map.
    pub fn release(&self) {
        self.data.version.fetch_add(1u32, Ordering::Release);
    }

    /// Creates a read-only handle to the `BytesHashMap`.
    ///
    /// The returned `BytesHashMapReadOnly` can be shared among multiple reader threads
    /// and can be used to access the data in the map concurrently with the writer
    /// thread.
    ///
    /// It is essential to create the read-only handle *before* the writer thread
    /// takes ownership of the `BytesHashMap`.
    pub fn read_only(&self) -> BytesHashMapReadOnly<V> {
        BytesHashMapReadOnly {
            data: self.data.clone(),
        }
    }
}

pub enum Entry<'a, V> {
    Occupied(OccupiedEntry<'a, V>),
    Vacant(VacantEntry<'a, V>),
}

impl<'a, V> Entry<'a, V> {
    pub fn or_insert(self, default: V) -> &'a V {
        match self {
            Entry::Occupied(entry) => entry.get(),
            Entry::Vacant(entry) => entry.insert(default),
        }
    }

    pub fn or_insert_with(self, f: impl FnOnce() -> V) -> &'a V {
        match self {
            Entry::Occupied(entry) => entry.get(),
            Entry::Vacant(entry) => entry.insert(f()),
        }
    }
}

pub struct OccupiedEntry<'a, V> {
    key: &'a [u8],
    value: &'a V,
}

impl<'a, V> OccupiedEntry<'a, V> {
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    pub fn get(&self) -> &'a V {
        self.value
    }
}

pub struct VacantEntry<'a, V> {
    key: &'a [u8],
    map: &'a mut BytesHashMap<V>,
    bucket_id: usize,
    hash: u32,
}

impl<'a, V> VacantEntry<'a, V> {
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    pub fn insert(self, value: V) -> &'a V {
        let bucket_entry = &self.map.buckets[self.bucket_id];
        let version = self.map.data.version.load(Ordering::Relaxed);
        self.map.data.len.fetch_add(1, atomic::Ordering::Relaxed);
        store_into_bucket(
            bucket_entry,
            HashAndVersion {
                hash: self.hash,
                version,
            },
            value,
            self.key,
            &self.map.arena,
        )
    }
}

#[derive(Clone)]
pub struct BytesHashMapReadOnly<V> {
    data: Arc<BytesHashMapData<V>>,
}

impl<V> BytesHashMapReadOnly<V> {
    pub fn version(&self) -> u32 {
        self.data.version.load(Ordering::Acquire)
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.get_with_version(key, self.version())
    }

    pub fn get_with_version(&self, key: &[u8], version: u32) -> Option<&V> {
        assert!(key.len() <= u16::MAX as usize);
        let hash = crate::murmurhash2(key);
        let buckets_guard = self.data.buckets.load();
        let buckets = &**buckets_guard.as_ref();
        let mask = buckets.len() - 1;
        let mut probe = LinearProbing::compute(hash, mask);
        loop {
            let bucket_id = probe.next_probe();
            let bucket_entry = &buckets[bucket_id];
            let hash_and_version = bucket_entry.load_hash_and_version(Ordering::Relaxed);
            if hash_and_version.is_empty() {
                return None;
            }
            if hash == hash_and_version.hash && hash_and_version.version < version {
                let ptr: *mut u8 = bucket_entry.ptr.load(atomic::Ordering::Relaxed);
                if unsafe { is_key_equal::<V>(ptr, key) } {
                    let value = unsafe { &*(ptr as *const V) };
                    return Some(value);
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

struct BytesHashMapData<V> {
    version: AtomicU32,
    // Does it feel dumb to have a ArcSwap<Arc>?
    // Certainly yes, but ArcSwap<..> requires the object it holds to be sized.
    buckets: ArcSwap<Arc<[AtomicBucketEntry]>>,
    data: PhantomData<V>,
    drop_on_last: crate::concurrency_types::sync::Mutex<Option<bumpalo::Bump<MIN_ALIGN>>>,
    len: AtomicUsize,
}

impl<V> BytesHashMapData<V> {
    fn with_buckets(buckets: Arc<[AtomicBucketEntry]>) -> Self {
        BytesHashMapData {
            version: AtomicU32::new(1),
            buckets: ArcSwap::new(std::sync::Arc::new(buckets)),
            data: PhantomData,
            drop_on_last: crate::concurrency_types::sync::Mutex::new(None),
            len: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(atomic::Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.buckets.load().len()
    }
}

struct LinearProbing {
    pos: usize,
    mask: usize,
}

impl LinearProbing {
    #[inline]
    fn compute(hash: HashType, mask: usize) -> LinearProbing {
        LinearProbing {
            pos: hash as usize,
            mask,
        }
    }

    #[inline]
    fn next_probe(&mut self) -> usize {
        // Not saving the masked version removes a dependency.
        self.pos = self.pos.wrapping_add(1);
        self.pos & self.mask
    }
}

#[inline(always)]
fn key_len_offset<V>() -> usize {
    let align = std::mem::align_of::<V>();
    let padding = if align % 2 == 0 { 0 } else { 1 };
    size_of::<V>() + padding
}

#[inline(always)]
fn get_size<V>(key_len: usize) -> usize {
    key_len_offset::<V>() + 2 + key_len
}

#[inline(always)]
fn get_layout<V>(key_len: usize) -> Layout {
    let size = get_size::<V>(key_len);
    let align = std::mem::align_of::<V>().max(2);
    unsafe { Layout::from_size_align_unchecked(size, align) }
}

#[inline(always)]
fn store_into_bucket<'a, V>(
    bucket_entry: &AtomicBucketEntry,
    hash_and_version: HashAndVersion,
    new_value: V,
    key: &[u8],
    arena: &'a bumpalo::Bump<MIN_ALIGN>,
) -> &'a V {
    let key_len = key.len();
    let layout = get_layout::<V>(key_len);
    let dst: *mut u8 = arena.alloc_layout(layout).as_ptr();
    bucket_entry.ptr.store(dst, atomic::Ordering::Relaxed);
    bucket_entry.store_hash_and_version(hash_and_version, atomic::Ordering::Relaxed);
    unsafe {
        std::ptr::write(dst as *mut V, new_value);
        std::ptr::write(dst.add(key_len_offset::<V>()) as *mut u16, key_len as u16);
        std::ptr::copy_nonoverlapping(key.as_ptr(), dst.add(key_len_offset::<V>() + 2), key_len);
        &*(dst as *const V)
    }
}

#[inline(always)]
unsafe fn is_key_equal<V>(ptr: *const u8, key: &[u8]) -> bool {
    unsafe {
        let key_len_ptr: *const u16 = ptr.add(std::mem::size_of::<V>()) as *const u16;
        let key_len: usize = std::ptr::read(key_len_ptr) as usize;
        let key_start_ptr: *const u8 =
            ptr.add(std::mem::size_of::<V>() + std::mem::size_of::<u16>());
        std::slice::from_raw_parts(key_start_ptr, key_len) == key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_and_version() {
        let hash_and_version = HashAndVersion::default();
        assert_eq!(hash_and_version.hash, 0);
        assert_eq!(hash_and_version.version, 0);
        assert!(hash_and_version.is_empty());
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
}
