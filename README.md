# Monologue

Monologue is a hashmap implementation targetting a very contrived use case:
- single-writer/multiple-reader
- keys can only be added: no update nor deletes are possible.
- keys are limited to `u16::MAX` (65 535 bytes)
- the number of releases is limited to `u32::MAX` (~4 billion)
- it offers control for the writer on when to release pending changes.

Its implementation is lock-free.
It uses open-addressing with linear probing.

The client keeps control of when changes are made visible to readers by calling `.release()`.

## Usage Example

Here's a simple example of how to use `BytesHashMap`:

```rust
use monologue::BytesHashMap;
use std::thread;

fn main() {
    let mut bytes_hash_map: BytesHashMap<usize> = BytesHashMap::with_capacity(1024);

    // Create a read-only handle before moving the map into the writer thread
    let snapshot_provider = bytes_hash_map.snapshot_provider();

    let writer_thread_handle = thread::spawn(move || {
        // Perform some writes
        bytes_hash_map.entry(b"hello").or_insert(1);
        bytes_hash_map.entry(b"world").or_insert(2);

        // Release the changes to make them visible to readers
        bytes_hash_map.release();
    });

    let reader_thread_handle = thread::spawn(move || {
        let snapshot = snapshot_provider.snapshot();
        let Some(value): &usize = snapshot.get(b"hello", version);
        // ...
    });

    writer_handle.join().unwrap();
    reader_handle.join().unwrap();
}
```
