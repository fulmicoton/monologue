use criterion::{Criterion, criterion_group, criterion_main};
use monologue::BytesHashMap;

const WORDS: &str = include_str!("words.txt");
const WORDS_1WRITE9READ: &str = include_str!("words_1write9read.txt");

pub fn criterion_benchmark(c: &mut Criterion) {
    let words: Vec<&str> = WORDS.lines().map(|line| line.trim()).collect();
    let words_1write9read: Vec<&str> = WORDS_1WRITE9READ.lines().map(|line| line.trim()).collect();
    assert_eq!(words.len(), 100_000);
    c.bench_function("monologue insert with resize", |b| {
        b.iter(|| {
            let mut hash_map: BytesHashMap<usize> = monologue::BytesHashMap::with_capacity(10_000);
            for word in &words {
                hash_map.entry(word.as_bytes()).or_insert(0);
            }
        })
    });
    c.bench_function("monologue insert no resize", |b| {
        b.iter(|| {
            let mut hash_map: BytesHashMap<usize> =
                monologue::BytesHashMap::with_capacity(WORDS.len() * 2);
            let cap = hash_map.capacity();
            for word in &words {
                hash_map.entry(word.as_bytes()).or_insert(0);
            }
            assert_eq!(hash_map.capacity(), cap);
        })
    });
    c.bench_function("hashmap insert baseline with resize", |b| {
        b.iter(|| {
            let mut hash_map: hashbrown::HashMap<String, usize> =
                hashbrown::HashMap::with_capacity(10_000);
            for word in &words {
                hash_map.entry(word.to_string()).or_insert(0);
            }
        })
    });
    c.bench_function("hashmap insert baseline no resize", |b| {
        b.iter(|| {
            let mut hash_map: hashbrown::HashMap<String, usize> =
                hashbrown::HashMap::with_capacity(WORDS.len() * 2);
            let cap = hash_map.capacity();
            for word in &words {
                hash_map.entry(word.to_string()).or_insert(0);
            }
            assert_eq!(hash_map.capacity(), cap);
        })
    });
    c.bench_function("monologue insert x1 and get x9 no resize", |b| {
        b.iter(|| {
            let mut hash_map: monologue::BytesHashMap<usize> =
                monologue::BytesHashMap::with_capacity(30_000);
            let cap = hash_map.capacity();
            for word in &words_1write9read {
                hash_map.entry(word.as_bytes()).or_insert(0);
            }
            assert_eq!(hash_map.capacity(), cap);
        })
    });
    c.bench_function("hashmap insert x1 and get x9 no resize baseline", |b| {
        b.iter(|| {
            let mut hash_map: hashbrown::HashMap<String, usize> =
                hashbrown::HashMap::with_capacity(30_000);
            let cap = hash_map.capacity();
            for word in &words_1write9read {
                hash_map.entry(word.to_string()).or_insert(0);
            }
            assert_eq!(hash_map.capacity(), cap);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
