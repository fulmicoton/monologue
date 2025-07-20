mod concurrency_types;
mod hash_map;
mod murmurhash2;

pub use murmurhash2::murmurhash2;

pub use hash_map::BytesHashMap;
pub use hash_map::BytesHashMapOwnedSnapshot;
pub use hash_map::BytesHashMapSnapshotProvider;
pub use hash_map::{Entry, OccupiedEntry, VacantEntry};

type HashType = u32;

#[cfg(test)]
mod tests;

#[cfg(all(loom, test))]
mod loom_test;
