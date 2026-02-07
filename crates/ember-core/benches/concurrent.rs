//! Benchmark comparing sharded engine vs concurrent keyspace.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ember_core::concurrent::ConcurrentKeyspace;
use ember_core::keyspace::EvictionPolicy;
use std::sync::Arc;

fn bench_concurrent_set(c: &mut Criterion) {
    let ks = Arc::new(ConcurrentKeyspace::new(None, EvictionPolicy::NoEviction));

    let mut group = c.benchmark_group("concurrent");
    group.throughput(Throughput::Elements(1));

    group.bench_function("set", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key:{}", i);
            i = i.wrapping_add(1);
            ks.set(key, Bytes::from_static(b"value"), None);
            black_box(())
        })
    });

    // Pre-populate for get benchmark
    for i in 0..10000 {
        ks.set(format!("key:{}", i), Bytes::from_static(b"value"), None);
    }

    group.bench_function("get_existing", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key:{}", i % 10000);
            i = i.wrapping_add(1);
            black_box(ks.get(&key))
        })
    });

    group.bench_function("get_missing", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("missing:{}", i);
            i = i.wrapping_add(1);
            black_box(ks.get(&key))
        })
    });

    group.finish();
}

fn bench_concurrent_multithread(c: &mut Criterion) {
    use std::thread;

    let ks = Arc::new(ConcurrentKeyspace::new(None, EvictionPolicy::NoEviction));

    // Pre-populate
    for i in 0..100000 {
        ks.set(format!("key:{}", i), Bytes::from_static(b"value"), None);
    }

    let mut group = c.benchmark_group("concurrent_mt");
    group.throughput(Throughput::Elements(8)); // 8 threads

    group.bench_function("get_8_threads", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..8)
                .map(|t| {
                    let ks = Arc::clone(&ks);
                    thread::spawn(move || {
                        for i in 0..1000 {
                            let key = format!("key:{}", (t * 1000 + i) % 100000);
                            black_box(ks.get(&key));
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
        })
    });

    group.bench_function("set_8_threads", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..8)
                .map(|t| {
                    let ks = Arc::clone(&ks);
                    thread::spawn(move || {
                        for i in 0..1000 {
                            let key = format!("thread:{}:key:{}", t, i);
                            ks.set(key, Bytes::from_static(b"value"), None);
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_set, bench_concurrent_multithread);
criterion_main!(benches);
