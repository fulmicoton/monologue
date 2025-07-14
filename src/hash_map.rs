use std::alloc::Layout;
use std::hash::{Hash as _, Hasher as _};
use std::marker::PhantomData;
use crate::concurrency_types::sync::Arc;
use crate::concurrency_types::sync::atomic::{self, AtomicU32, AtomicU64, AtomicPtr, Ordering};

type HashType = u32;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
struct HashAndVersion {
    hash: u32,
    version: u32,
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
    ptr: AtomicPtr<u8>,
}

impl Default for AtomicBucketEntry {
    fn default() -> AtomicBucketEntry {
        AtomicBucketEntry {
            hash_and_version: AtomicU64::new(u64::from(HashAndVersion {
                hash: 0u32,
                version: u32::MAX,
            })
            ),
            ptr: Default::default()
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
        self.hash_and_version.store(u64::from(hash_and_version), ordering);
    }
}

pub struct BytesHashMap<V> {
    arena: bumpalo::Bump<2>,
    data: Arc<BytesHashMapData<V>>,
    gc_ready_rx: Option<oneshot::Receiver<()>>,
}

impl<V> Drop for BytesHashMap<V> {
    fn drop(&mut self) {
        let gc_ready_rx = self.gc_ready_rx.take().unwrap();
        if !Arc::strong_count(&self.data) == 1 {
            // Some ReadOnly instance is still refering to our bumpalo Bump.
            // We defer dropping it until the last one is dropped.
            //
            // We will do that in a separate thread.
            let bumpalo = std::mem::replace(&mut self.arena, bumpalo::Bump::<2>::with_min_align());
            std::thread::spawn(move || {
                // We wait on the one shot receiver to properly park our thread.
                let _ = gc_ready_rx.recv();
                drop(bumpalo);
            });
        } else {
            // We have the last reference to data.
            // We can safely drop our bumpalo Bump.
        }

    }
}

impl<V> Default for BytesHashMap<V> {
    fn default() -> BytesHashMap<V> {
        BytesHashMap::new()
    }
}

impl<V> BytesHashMap<V> {
    pub fn new() -> BytesHashMap<V> {
        let (gc_ready_tx, gc_ready_rx) = oneshot::channel();
        let data = BytesHashMapData::new(gc_ready_tx);
        BytesHashMap {
            arena: bumpalo::Bump::<2>::with_min_align_and_capacity(1_000_000),
            data: Arc::new(data),
            gc_ready_rx: Some(gc_ready_rx),
        }
    }

    pub fn read_only(&self) -> BytesHashMapReadOnly<V> {
        BytesHashMapReadOnly {
            data: self.data.clone(),
        }
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

    pub fn get(
        &self,
        key: &[u8],
        version: u32,
    ) -> Option<&V> {
        assert!(key.len() <= u16::MAX as usize);
        let hash = key_hash(key);
        let mut probe = LinearProbing::compute(hash, self.data.mask);
        loop {
            let bucket_id = probe.next_probe();
            let bucket_entry = &self.data.buckets[bucket_id];
            let hash_and_version = bucket_entry.load_hash_and_version(Ordering::Relaxed);
            // The bucket is empty.
            if hash_and_version.version == u32::MAX {
                return None;
            }
            if hash != hash_and_version.hash {
                // This is just a "truncated hash" collision. Let's keep looking.
                continue;
            }
            // This could still be still be 32-bit hash collision. Let's
            // check the actual string.
            //
            // Not we do not check the version here. We are in the single writer here.
            let ptr: *mut u8 = bucket_entry.ptr.load(atomic::Ordering::Relaxed);
            let bucket_key = unsafe {
                let key_len: usize = std::ptr::read(ptr.offset(std::mem::size_of::<V>() as isize) as *const u16) as usize;
                std::slice::from_raw_parts(ptr.offset((std::mem::size_of::<V>() + std::mem::size_of::<u16>()) as isize), key_len)
            };
            if key == bucket_key {
                if hash_and_version.version >= version {
                    // This is the right bucket entry, BUT it was inserted after version.
                    return None;
                }
                let value = unsafe { &*(ptr as *const V) };
                return Some(value);
            }
        }
    }

}

struct BytesHashMapData<V> {
    version: AtomicU32,
    buckets: Box<[AtomicBucketEntry]>,
    mask: usize,
    data: PhantomData<V>,
    // The point of this sender is just to get waked up when BytesHashMapData is dropped.
    _gc_ready_tx: oneshot::Sender<()>,
}

impl<V> BytesHashMapData<V> {
    fn new(gc_ready_tx: oneshot::Sender<()>) -> Self {
        BytesHashMapData {
            version: AtomicU32::new(0),
            buckets: std::iter::repeat_with(AtomicBucketEntry::default).take(1 << 20).collect::<Vec<AtomicBucketEntry>>().into_boxed_slice(),
            mask: (1<<20) - 1,
            data: PhantomData,
            _gc_ready_tx: gc_ready_tx,
        }
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
fn key_hash(key: &[u8]) -> HashType {
    let mut hasher = ahash::AHasher::default();
    key.hash(&mut hasher);
    hasher.finish() as HashType
}

impl<V> BytesHashMap<V> {
    pub fn release(&self) {
        self.data.version.fetch_add(1u32, Ordering::Relaxed);
    }

    pub fn mutate_or_create(
        &mut self,
        key: &[u8],
        mut creator: impl FnMut() -> V,
        mut updator: impl FnMut(&V),
    ) {
        assert!(key.len() <= u16::MAX as usize);
        let version = self.data.version.load(Ordering::Relaxed);
        let hash = key_hash(key);
        let mut probe = LinearProbing::compute(hash, self.data.mask);
        loop {
            let bucket_id = probe.next_probe();
            let bucket_entry = &self.data.buckets[bucket_id];
            let hash_and_version = bucket_entry.load_hash_and_version(Ordering::Relaxed);
            if hash_and_version.version == u32::MAX {
                // the bucket is vacant
                let new_value = creator();
                // bucket_entry.store_hash_and_version(HashAndVersion { hash, version }, Ordering::Relaxed);
                let key_len = key.len();
                let (size, align) = (std::mem::size_of_val(&new_value) + 2 + key.len(), std::mem::align_of_val(&new_value));
                let layout = unsafe { Layout::from_size_align_unchecked(size, align) };
                let dst: *mut u8 = self.arena.alloc_layout(layout).as_ptr();
                unsafe {
                    std::ptr::write(dst as *mut V, new_value);
                    std::ptr::write(dst.offset(std::mem::size_of::<V>() as isize) as *mut u16, key_len as u16);
                    std::ptr::copy_nonoverlapping(key.as_ptr(), dst.offset((std::mem::size_of::<V>() + std::mem::size_of::<u16>()) as isize), key_len);
                }
                bucket_entry.ptr.store(dst, atomic::Ordering::Relaxed);
                bucket_entry.store_hash_and_version(HashAndVersion { hash, version }, atomic::Ordering::Relaxed);
                break;
            }
            // We have found a possible match.
            // This could still be a collision however.
            if hash == hash_and_version.hash {
                // No need to check for the version. We are in the single writer here.
                let ptr: *mut u8 = bucket_entry.ptr.load(atomic::Ordering::Relaxed);
                let key_len: usize = unsafe { std::ptr::read(ptr.offset(std::mem::size_of::<V>() as isize) as *const u16) } as usize;
                let bucket_key = unsafe { std::slice::from_raw_parts(ptr.offset((std::mem::size_of::<V>() + std::mem::size_of::<u16>()) as isize), key_len) };
                if key == bucket_key {
                    let value = unsafe { &*(ptr as *const V) };
                    updator(value);
                    break;
                }
            }
        }
    }
}
