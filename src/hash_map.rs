use arc_swap::ArcSwap;

const MIN_ALIGN: usize = 8;
const DEFAULT_HASH_TABLE_SIZE: usize = 1 << 10;

use crate::concurrency_types::arc_from_box;
use crate::murmurhash2;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use crate::concurrency_types::sync::Arc;
use crate::concurrency_types::sync::atomic::{self, AtomicPtr, AtomicU64, Ordering};

fn linear_probing(hash: crate::HashType, mask: usize) -> impl Iterator<Item = usize> {
    (hash as usize..).map(move |idx| idx & mask)
}

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

#[derive(Clone, Default, Copy, Eq, PartialEq, Debug)]
struct LenAndVersion {
    len: u32,
    version: u32,
}

impl From<LenAndVersion> for u64 {
    #[inline(always)]
    fn from(len_and_version: LenAndVersion) -> u64 {
        ((len_and_version.len as u64) << 32) | (len_and_version.version as u64)
    }
}

impl From<u64> for LenAndVersion {
    #[inline(always)]
    fn from(value: u64) -> Self {
        LenAndVersion {
            len: (value >> 32) as u32,
            version: value as u32,
        }
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
    core: BytesHashMapCore<V>,
    // We use this trick to mark Snapshots as !Send.
    // Snapshot heavily rely on the guarantee of reordering in
    _non_send_marker: PhantomData<*mut ()>,
}

impl<V> Default for BytesHashMap<V> {
    fn default() -> BytesHashMap<V> {
        BytesHashMap::with_capacity(DEFAULT_HASH_TABLE_SIZE)
    }
}

impl<V> Deref for BytesHashMap<V> {
    type Target = BytesHashMapCore<V>;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl<V> DerefMut for BytesHashMap<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl<V> BytesHashMap<V> {
    pub fn with_capacity(hash_table_size: usize) -> BytesHashMap<V> {
        let hash_table_size = (hash_table_size * 2).next_power_of_two();
        let mask = hash_table_size - 1;
        let buckets: Arc<[AtomicBucketEntry]> = arc_from_box(
            std::iter::repeat_with(AtomicBucketEntry::default)
                .take(hash_table_size)
                .collect::<Vec<AtomicBucketEntry>>()
                .into_boxed_slice(),
        );
        let data = BytesHashMapData::with_buckets(buckets.clone());
        let core = BytesHashMapCore {
            len: 0,
            version: 1,
            buckets,
            mask,
            arena: bumpalo::Bump::with_min_align(),
            data: Arc::new(data),
        };
        BytesHashMap {
            core,
            _non_send_marker: PhantomData,
        }
    }

    pub fn release_and_pack_for_send(mut self) -> BytesHashMapPackedForSend<V> {
        self.release();
        BytesHashMapPackedForSend(self.core)
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len() / 2
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get<'a>(&'a mut self, key: &'a [u8]) -> Option<&'a V> {
        match self.entry(key) {
            Entry::Occupied(occupied_entry) => Some(occupied_entry.get()),
            Entry::Vacant(_) => None,
        }
    }

    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a, V> {
        assert!(key.len() <= u16::MAX as usize);

        let hash = murmurhash2(key);
        for bucket_id in linear_probing(hash, self.mask) {
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
                    return Entry::Occupied(OccupiedEntry { value });
                }
            }
        }
        unreachable!()
    }

    fn grow_hash_table(&mut self) {
        let new_hash_table_size = self.buckets.len() * 2;
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
            for bucket_id in linear_probing(hash_and_version.hash, new_mask) {
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

    /// Publishes all changes made to the `BytesHashMap` since the last `release` call.
    /// These changes will only be made visible to new snapshots.
    //
    /// This operation is cheap! It is just an atomic store with Release Ordering.
    pub fn release(&mut self) {
        self.version += 1u32;
        let len_and_version = LenAndVersion {
            len: self.len as u32,
            version: self.version,
        };
        self.data
            .len_and_version
            .store(u64::from(len_and_version), Ordering::Release);
    }

    /// Creates a read-only handle to the `BytesHashMap`.
    ///
    /// The returned `BytesHashMapReadOnly` can be shared among multiple reader threads
    /// and can be used to access the data in the map concurrently with the writer
    /// thread.
    ///
    /// It is essential to create the read-only handle *before* the writer thread
    /// takes ownership of the `BytesHashMap`.
    pub fn snapshot_provider(&self) -> BytesHashMapSnapshotProvider<V> {
        BytesHashMapSnapshotProvider {
            data: self.data.clone(),
        }
    }
}

pub struct BytesHashMapPackedForSend<V>(BytesHashMapCore<V>);

impl<V> BytesHashMapPackedForSend<V> {
    pub fn unpack(self) -> BytesHashMap<V> {
        // Rust's memory model does not guarantee us after sending this struct to another
        // thread, the version we load is not in the past, hence the loop.
        //
        // In practise, we expect that to never happen.
        loop {
            let LenAndVersion {len, version} = self.0.data.load_len_and_version();
            // This inequality is implied by the fact that we release before packing.
            assert!(len as usize <= self.0.len);
            assert!(version <= self.0.version);
            if version == self.0.version {
                break;
            }
        }
        BytesHashMap {
            core: self.0,
            _non_send_marker: PhantomData,
        }
    }
}

pub struct BytesHashMapCore<V> {
    buckets: Arc<[AtomicBucketEntry]>,
    mask: usize,
    data: Arc<BytesHashMapData<V>>,
    len: usize,
    version: u32,
    arena: bumpalo::Bump<MIN_ALIGN>,
}

impl<V> Drop for BytesHashMapCore<V> {
    fn drop(&mut self) {
        let bumpalo = std::mem::replace(&mut self.arena, bumpalo::Bump::with_min_align());
        let mut drop_on_last_lock = self.data.drop_on_last.lock().unwrap();
        *drop_on_last_lock = Some(bumpalo);
    }
}

impl<V> From<BytesHashMap<V>> for BytesHashMapPackedForSend<V> {
    fn from(hash_map: BytesHashMap<V>) -> Self {
        hash_map.release_and_pack_for_send()
    }
}

impl<V> From<BytesHashMapPackedForSend<V>> for BytesHashMap<V> {
    fn from(hash_map_packed: BytesHashMapPackedForSend<V>) -> Self {
        hash_map_packed.unpack()
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
    value: &'a V,
}

impl<'a, V> OccupiedEntry<'a, V> {
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
        self.map.len += 1;
        if self.map.len * 2 > self.map.buckets.len() {
            self.map.grow_hash_table();
        }
        let bucket_entry = &self.map.buckets[self.bucket_id];
        let version = self.map.version;
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

/// Read-only does not do much in itself.
///
/// It does not give you access to the underlying data directly.
/// It requires a snapshot to access the data.
///
/// Contrary, to `BytesHashMapSnapshot` or the `BytesHashMap`,
/// it is `Send`!
#[derive(Clone)]
pub struct BytesHashMapSnapshotProvider<V> {
    data: Arc<BytesHashMapData<V>>,
}

impl<V> BytesHashMapSnapshotProvider<V> {
    pub fn snapshot(&self) -> BytesHashMapSnapshot<'_, V> {
        let LenAndVersion { len, version } = self.data.load_len_and_version();
        BytesHashMapSnapshot {
            data: &*self.data,
            len,
            version,
            _non_send_marker: PhantomData,
        }
    }

    pub fn owned_snapshot(&self) -> BytesHashMapOwnedSnapshot<V> {
        let LenAndVersion { len, version } =
            LenAndVersion::from(self.data.len_and_version.load(Ordering::Acquire));
        BytesHashMapOwnedSnapshot {
            data: self.data.clone(),
            len,
            version,
            _non_send_marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct BytesHashMapSnapshot<'a, V> {
    data: &'a BytesHashMapData<V>,
    version: u32,
    len: u32,
    // We use this trick to mark Snapshots as !Send.
    // Snapshot heavily rely on the guarantee of reordering in
    _non_send_marker: PhantomData<*mut ()>,
}

impl<'a, V> BytesHashMapSnapshot<'a, V> {
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.data.get_with_version(key, self.version)
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(all(loom, test))]
    pub(crate) fn version(&self) -> u32 {
        self.version
    }
}

#[derive(Clone)]
pub struct BytesHashMapOwnedSnapshot<V> {
    data: Arc<BytesHashMapData<V>>,
    version: u32,
    len: u32,
    // We use this trick to mark Snapshots as !Send.
    // Snapshot heavily rely on the guarantee of reordering in
    _non_send_marker: PhantomData<*mut ()>,
}

impl<V> BytesHashMapOwnedSnapshot<V> {
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.data.get_with_version(key, self.version)
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct BytesHashMapData<V> {
    len_and_version: AtomicU64,
    // Does it feel dumb to have a ArcSwap<Arc>?
    // Certainly yes, but ArcSwap<..> requires the object it holds to be sized.
    buckets: ArcSwap<Arc<[AtomicBucketEntry]>>,
    data: PhantomData<V>,
    drop_on_last: crate::concurrency_types::sync::Mutex<Option<bumpalo::Bump<MIN_ALIGN>>>,
}

impl<V> BytesHashMapData<V> {
    fn load_len_and_version(&self) -> LenAndVersion {
        LenAndVersion::from(self.len_and_version.load(Ordering::Acquire))
    }

    fn get_with_version(&self, key: &[u8], version: u32) -> Option<&V> {
        assert!(key.len() <= u16::MAX as usize);
        let hash = murmurhash2(key);
        let buckets_guard = self.buckets.load();
        let buckets = &**buckets_guard.as_ref();
        let mask = buckets.len() - 1;
        for bucket_id in linear_probing(hash, mask) {
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
        unreachable!()
    }

    fn with_buckets(buckets: Arc<[AtomicBucketEntry]>) -> Self {
        BytesHashMapData {
            len_and_version: AtomicU64::new(u64::from(LenAndVersion {
                len: 0u32,
                version: 1u32,
            })),
            buckets: ArcSwap::new(std::sync::Arc::new(buckets)),
            data: PhantomData,
            drop_on_last: crate::concurrency_types::sync::Mutex::new(None),
        }
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
    fn test_linear_probing() {
        let probes: Vec<usize> = linear_probing(3, 8 - 1).take(8).collect();
        assert_eq!(&probes, &[3, 4, 5, 6, 7, 0, 1, 2]);
    }
}
