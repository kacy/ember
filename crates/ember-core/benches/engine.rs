//! Micro-benchmarks for the engine routing layer.
//!
//! Measures the full async path: mpsc send to shard, keyspace operation,
//! oneshot reply. Isolates channel overhead from TCP/protocol costs.
//! Run with `cargo bench -p emberkv-core -- engine`.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use ember_core::{Engine, ShardRequest};

const SHARD_COUNT: usize = 4;
const KEY_COUNT: usize = 10_000;

fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn bench_engine(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    // pre-populate the engine with keys across all shards
    let engine = rt.block_on(async {
        let engine = Engine::new(SHARD_COUNT);
        let value = make_value(64);
        for i in 0..KEY_COUNT {
            let key = format!("key:{i}");
            engine
                .route(
                    &key,
                    ShardRequest::Set {
                        key: key.clone(),
                        value: value.clone(),
                        expire: None,
                        nx: false,
                        xx: false,
                    },
                )
                .await
                .expect("pre-populate failed");
        }
        engine
    });

    let mut group = c.benchmark_group("engine");

    group.bench_function("route_get", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                engine
                    .route(
                        "key:5000",
                        ShardRequest::Get {
                            key: "key:5000".into(),
                        },
                    )
                    .await
                    .expect("route_get failed"),
            )
        });
    });

    group.bench_function("route_set_64B", |b| {
        let value = make_value(64);
        b.to_async(&rt).iter(|| {
            let v = value.clone();
            async {
                black_box(
                    engine
                        .route(
                            "key:0",
                            ShardRequest::Set {
                                key: "key:0".into(),
                                value: v,
                                expire: None,
                                nx: false,
                                xx: false,
                            },
                        )
                        .await
                        .expect("route_set failed"),
                )
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_engine);
criterion_main!(benches);
