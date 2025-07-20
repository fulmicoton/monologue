use std::sync::RwLock;

use criterion::{Criterion, criterion_group, criterion_main};
use hashbrown::HashSet;
use monologue::BytesHashMap;

const WORDS: &str = include_str!("words.txt");
const WORDS_1WRITE9READ: &str = include_str!("words_1write9read.txt");

pub fn criterion_benchmark(c: &mut Criterion) {
    let words: Vec<&str> = WORDS.lines().map(|line| line.trim()).collect();
    assert_eq!(words.len(), 100_000);

    let words_1write9read: Vec<&str> = WORDS_1WRITE9READ.lines().map(|line| line.trim()).collect();
    let num_words_1write9read: usize = WORDS_1WRITE9READ
        .lines()
        .map(|line| line.trim())
        .collect::<HashSet<&str>>()
        .len();

    {
        let mut group = c.benchmark_group("insert no capacity");
        group.bench_function("monologue", |b| {
            b.iter(|| {
                let mut hash_map: BytesHashMap<usize> = monologue::BytesHashMap::default();
                let mut total = 0;
                for word in &words {
                    total += *hash_map.entry(word.as_bytes()).or_insert(0);
                }
                total
            })
        });
        group.bench_function("hashbrown", |b| {
            b.iter(|| {
                let mut hash_map: hashbrown::HashMap<String, usize> = hashbrown::HashMap::default();
                let mut total = 0;
                for word in &words {
                    total += *hash_map.entry_ref(*word).or_insert(0);
                }
                total
            })
        });
    }

    {
        let mut group = c.benchmark_group("insert with capacity");
        group.bench_function("monologue", |b| {
            b.iter(|| {
                let mut hash_map: BytesHashMap<usize> =
                    monologue::BytesHashMap::with_capacity(WORDS.len());
                let cap = hash_map.capacity();
                let mut total = 0;
                for word in &words {
                    total += *hash_map.entry(word.as_bytes()).or_insert(0);
                }
                assert_eq!(hash_map.capacity(), cap);
                total
            })
        });
        group.bench_function("hashbrown", |b| {
            b.iter(|| {
                let mut hash_map: hashbrown::HashMap<String, usize> =
                    hashbrown::HashMap::with_capacity(WORDS.len());
                let cap = hash_map.capacity();
                let mut total = 0;
                for word in &words {
                    total += *hash_map.entry_ref(*word).or_insert(0);
                }
                assert_eq!(hash_map.capacity(), cap);
                total
            })
        });
        group.bench_function("rwlock hashbrown", |b| {
            b.iter(|| {
                let hash_map: RwLock<hashbrown::HashMap<String, usize>> =
                    RwLock::new(hashbrown::HashMap::with_capacity(WORDS.len()));
                let cap = hash_map.read().unwrap().capacity();
                let mut total = 0;
                for word in &words {
                    let mut wlock = hash_map.write().unwrap();
                    total += *wlock.entry_ref(*word).or_insert(0);
                }
                assert_eq!(hash_map.read().unwrap().capacity(), cap);
                total
            })
        });
    }

    {
        let mut group = c.benchmark_group("insert x1 get x9 no capacity");
        group.bench_function("monologue", |b| {
            b.iter(|| {
                let mut hash_map: monologue::BytesHashMap<usize> =
                    monologue::BytesHashMap::default();
                let mut total = 0;
                for word in &words_1write9read {
                    total += *hash_map.entry(word.as_bytes()).or_insert(0);
                }
                total
            })
        });
        group.bench_function("hashbrown", |b| {
            b.iter(|| {
                let mut hash_map: hashbrown::HashMap<String, usize> = hashbrown::HashMap::default();
                let mut total = 0;
                for word in &words_1write9read {
                    total += *hash_map.entry(word.to_string()).or_insert(0);
                }
                total
            })
        });
    }
    {
        let mut group = c.benchmark_group("insert x1 get x9 with capacity");
        group.bench_function("monologue", |b| {
            b.iter(|| {
                let mut hash_map: monologue::BytesHashMap<usize> =
                    monologue::BytesHashMap::with_capacity(num_words_1write9read);
                let mut total = 0;
                for word in &words_1write9read {
                    total += *hash_map.entry(word.as_bytes()).or_insert(0);
                }
                total
            })
        });
        group.bench_function("hashbrown", |b| {
            b.iter(|| {
                let mut hash_map: hashbrown::HashMap<String, usize> =
                    hashbrown::HashMap::with_capacity(num_words_1write9read);
                let mut total = 0;
                for word in &words_1write9read {
                    total += *hash_map.entry(word.to_string()).or_insert(0);
                }
                total
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
