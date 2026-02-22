//! Micro-benchmarks for keyspace operations.
//!
//! Measures raw data structure performance without async channels or
//! network overhead. Run with `cargo bench -p emberkv-core -- keyspace`.

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use ember_core::keyspace::Keyspace;
use ember_core::memory;
use ember_core::types::Value;

const KEY_COUNT: usize = 10_000;

/// Builds a value of the given size filled with 'x'.
fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Pre-populates a keyspace with `KEY_COUNT` keys of the given value size.
fn populated_keyspace(value_size: usize) -> Keyspace {
    let mut ks = Keyspace::new();
    let value = make_value(value_size);
    for i in 0..KEY_COUNT {
        ks.set(format!("key:{i}"), value.clone(), None, false, false);
    }
    ks
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("keyspace_get");

    for size in [64, 256, 1024, 16384] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{size}B")),
            &size,
            |b, &size| {
                let mut ks = populated_keyspace(size);
                b.iter(|| {
                    let _ = black_box(ks.get("key:5000"));
                });
            },
        );
    }

    group.finish();
}

fn bench_set_overwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("keyspace_set_overwrite");

    for size in [64, 256, 1024, 16384] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{size}B")),
            &size,
            |b, &size| {
                let mut ks = Keyspace::new();
                let value = make_value(size);
                ks.set("key".into(), value.clone(), None, false, false);
                b.iter(|| {
                    black_box(ks.set("key".into(), value.clone(), None, false, false));
                });
            },
        );
    }

    group.finish();
}

fn bench_set_with_expiry(c: &mut Criterion) {
    let mut group = c.benchmark_group("keyspace_set_with_expiry");
    let value = make_value(64);
    let ttl = Some(Duration::from_secs(300));

    group.bench_function("64B_with_ttl", |b| {
        let mut ks = Keyspace::new();
        ks.set("key".into(), value.clone(), ttl, false, false);
        b.iter(|| {
            black_box(ks.set("key".into(), value.clone(), ttl, false, false));
        });
    });

    group.finish();
}

fn bench_mixed(c: &mut Criterion) {
    let value = make_value(64);

    c.bench_function("keyspace_mixed_50_50", |b| {
        let mut ks = populated_keyspace(64);
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key:{}", i % KEY_COUNT as u64);
            if i.is_multiple_of(2) {
                let _ = black_box(ks.get(&key));
            } else {
                black_box(ks.set(key, value.clone(), None, false, false));
            }
            i += 1;
        });
    });
}

fn bench_entry_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_entry_size");

    let small_val = Value::String(make_value(64));
    let large_val = Value::String(make_value(16384));

    group.bench_function("64B_string", |b| {
        b.iter(|| black_box(memory::entry_size("key:12345", &small_val)));
    });

    group.bench_function("16KB_string", |b| {
        b.iter(|| black_box(memory::entry_size("key:12345", &large_val)));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_set_overwrite,
    bench_set_with_expiry,
    bench_mixed,
    bench_entry_size,
);
criterion_main!(benches);
