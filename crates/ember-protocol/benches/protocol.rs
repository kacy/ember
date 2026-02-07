//! Micro-benchmarks for RESP3 parsing, serialization, and command dispatch.
//!
//! Run with `cargo bench -p ember-protocol -- resp3` or
//! `cargo bench -p ember-protocol -- command`.

use std::hint::black_box;

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use ember_protocol::{parse_frame, Command, Frame};

/// Builds the raw RESP3 bytes for `SET <key> <value>` where value is `size` bytes.
fn build_set_bytes(key: &str, value_size: usize) -> Vec<u8> {
    let value = "x".repeat(value_size);
    // *3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<vallen>\r\n<val>\r\n
    format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    )
    .into_bytes()
}

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("resp3_parse");

    let ping = b"*1\r\n$4\r\nPING\r\n";
    group.bench_function("ping", |b| {
        b.iter(|| black_box(parse_frame(ping).unwrap().unwrap()));
    });

    let get_cmd = b"*2\r\n$3\r\nGET\r\n$9\r\nkey:12345\r\n";
    group.bench_function("get", |b| {
        b.iter(|| black_box(parse_frame(get_cmd).unwrap().unwrap()));
    });

    let set_64 = build_set_bytes("key:12345", 64);
    group.bench_function("set_64B", |b| {
        b.iter(|| black_box(parse_frame(&set_64).unwrap().unwrap()));
    });

    let set_1k = build_set_bytes("key:12345", 1024);
    group.bench_function("set_1KB", |b| {
        b.iter(|| black_box(parse_frame(&set_1k).unwrap().unwrap()));
    });

    group.finish();
}

fn bench_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("resp3_serialize");

    let ok = Frame::Simple("OK".into());
    group.bench_function("simple_ok", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            ok.serialize(&mut buf);
            black_box(&buf);
        });
    });

    let bulk_64 = Frame::Bulk(Bytes::from(vec![b'x'; 64]));
    group.bench_function("bulk_64B", |b| {
        let mut buf = BytesMut::with_capacity(128);
        b.iter(|| {
            buf.clear();
            bulk_64.serialize(&mut buf);
            black_box(&buf);
        });
    });

    let bulk_1k = Frame::Bulk(Bytes::from(vec![b'x'; 1024]));
    group.bench_function("bulk_1KB", |b| {
        let mut buf = BytesMut::with_capacity(2048);
        b.iter(|| {
            buf.clear();
            bulk_1k.serialize(&mut buf);
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_command_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_from_frame");

    let get_frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"GET")),
        Frame::Bulk(Bytes::from_static(b"key:12345")),
    ]);
    group.bench_function("get", |b| {
        b.iter(|| black_box(Command::from_frame(get_frame.clone()).unwrap()));
    });

    let set_frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"key:12345")),
        Frame::Bulk(Bytes::from(vec![b'x'; 64])),
    ]);
    group.bench_function("set_64B", |b| {
        b.iter(|| black_box(Command::from_frame(set_frame.clone()).unwrap()));
    });

    group.finish();
}

criterion_group!(benches, bench_parse, bench_serialize, bench_command_parse);
criterion_main!(benches);
