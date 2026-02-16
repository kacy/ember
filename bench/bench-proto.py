#!/usr/bin/env python3
"""
protobuf storage overhead benchmark harness.

compares PROTO.SET/GET/GETFIELD/SETFIELD against raw SET/GET to measure
the overhead of server-side schema validation and field-level access.

called by bench-proto.sh with appropriate arguments.

usage:
    python3 bench/bench-proto.py --port 6379 --requests 100000
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time


def compile_proto(proto_path):
    """compile a .proto file to a FileDescriptorSet binary."""
    out = tempfile.NamedTemporaryFile(suffix=".pb", delete=False)
    out.close()
    subprocess.check_call([
        "protoc",
        f"--descriptor_set_out={out.name}",
        "--include_imports",
        f"--proto_path={os.path.dirname(proto_path)}",
        os.path.basename(proto_path),
    ])
    with open(out.name, "rb") as f:
        data = f.read()
    os.unlink(out.name)
    return data


def encode_user(name, age, email):
    """encode a User message using protobuf wire format.

    hand-rolled to avoid requiring protobuf python library at runtime.
    fields: name=1 (string), age=2 (int32), email=3 (string).
    """
    buf = bytearray()

    # field 1: string (wire type 2 = length-delimited)
    name_bytes = name.encode("utf-8")
    buf.append(0x0a)  # field 1, wire type 2
    buf.extend(_encode_varint(len(name_bytes)))
    buf.extend(name_bytes)

    # field 2: int32 (wire type 0 = varint)
    buf.append(0x10)  # field 2, wire type 0
    buf.extend(_encode_varint(age))

    # field 3: string (wire type 2 = length-delimited)
    email_bytes = email.encode("utf-8")
    buf.append(0x1a)  # field 3, wire type 2
    buf.extend(_encode_varint(len(email_bytes)))
    buf.extend(email_bytes)

    return bytes(buf)


def _encode_varint(value):
    """encode an integer as a protobuf varint."""
    buf = bytearray()
    while value > 0x7f:
        buf.append((value & 0x7f) | 0x80)
        value >>= 7
    buf.append(value & 0x7f)
    return bytes(buf)


def percentile(sorted_data, p):
    """compute p-th percentile from pre-sorted data."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_data) - 1)
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def compute_stats(elapsed, latencies, count):
    """compute throughput and latency stats."""
    ops_sec = count / elapsed if elapsed > 0 else 0
    sorted_lat = sorted(latencies)
    p50 = percentile(sorted_lat, 50) * 1000
    p95 = percentile(sorted_lat, 95) * 1000
    p99 = percentile(sorted_lat, 99) * 1000
    return {
        "ops_sec": round(ops_sec),
        "p50_ms": round(p50, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
    }


def bench_raw_set(r, keys, value, warmup=1000):
    """raw SET throughput (no validation)."""
    for i in range(warmup):
        r.set(f"raw_warmup:{i}", value)

    latencies = []
    start = time.perf_counter()
    for k in keys:
        t0 = time.perf_counter()
        r.set(k, value)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start
    return elapsed, latencies


def bench_raw_get(r, keys, warmup=1000):
    """raw GET throughput."""
    for i in range(warmup):
        r.get(f"raw_warmup:{i}")

    latencies = []
    start = time.perf_counter()
    for k in keys:
        t0 = time.perf_counter()
        r.get(k)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start
    return elapsed, latencies


def bench_proto_set(r, keys, value, type_name, warmup=1000):
    """PROTO.SET throughput (schema-validated)."""
    for i in range(warmup):
        r.execute_command("PROTO.SET", f"proto_warmup:{i}", type_name, value)

    latencies = []
    start = time.perf_counter()
    for k in keys:
        t0 = time.perf_counter()
        r.execute_command("PROTO.SET", k, type_name, value)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start
    return elapsed, latencies


def bench_proto_get(r, keys, warmup=1000):
    """PROTO.GET throughput."""
    for i in range(warmup):
        r.execute_command("PROTO.GET", f"proto_warmup:{i}")

    latencies = []
    start = time.perf_counter()
    for k in keys:
        t0 = time.perf_counter()
        r.execute_command("PROTO.GET", k)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start
    return elapsed, latencies


def bench_proto_getfield(r, keys, field, warmup=1000):
    """PROTO.GETFIELD throughput (single field read)."""
    for i in range(warmup):
        r.execute_command("PROTO.GETFIELD", f"proto_warmup:{i}", field)

    latencies = []
    start = time.perf_counter()
    for k in keys:
        t0 = time.perf_counter()
        r.execute_command("PROTO.GETFIELD", k, field)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start
    return elapsed, latencies


def bench_proto_setfield(r, keys, field, value, warmup=1000):
    """PROTO.SETFIELD throughput (single field update)."""
    for i in range(warmup):
        r.execute_command("PROTO.SETFIELD", f"proto_warmup:{i}", field, value)

    latencies = []
    start = time.perf_counter()
    for k in keys:
        t0 = time.perf_counter()
        r.execute_command("PROTO.SETFIELD", k, field, value)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start
    return elapsed, latencies


def main():
    parser = argparse.ArgumentParser(description="protobuf storage benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--requests", type=int, default=100000)
    parser.add_argument("--proto-file", default="bench/proto/user.proto")
    parser.add_argument("--output", default=None, help="JSON output file")
    args = parser.parse_args()

    import redis
    r = redis.Redis(host=args.host, port=args.port)

    count = args.requests
    type_name = "bench.User"

    # compile and register schema
    print("  compiling proto schema...", file=sys.stderr)
    descriptor = compile_proto(args.proto_file)

    print("  registering schema with PROTO.REGISTER...", file=sys.stderr)
    r.execute_command("PROTO.REGISTER", "bench", descriptor)

    # generate test data
    user_bytes = encode_user("alice", 30, "alice@example.com")
    raw_keys = [f"raw:{i}" for i in range(count)]
    proto_keys = [f"proto:{i}" for i in range(count)]

    results = {}

    # --- raw SET ---
    print("  raw SET...", file=sys.stderr)
    elapsed, latencies = bench_raw_set(r, raw_keys, user_bytes)
    results["raw_set"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['raw_set']['ops_sec']} ops/sec", file=sys.stderr)

    # --- PROTO.SET ---
    print("  PROTO.SET...", file=sys.stderr)
    elapsed, latencies = bench_proto_set(r, proto_keys, user_bytes, type_name)
    results["proto_set"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['proto_set']['ops_sec']} ops/sec", file=sys.stderr)

    # --- raw GET ---
    print("  raw GET...", file=sys.stderr)
    elapsed, latencies = bench_raw_get(r, raw_keys)
    results["raw_get"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['raw_get']['ops_sec']} ops/sec", file=sys.stderr)

    # --- PROTO.GET ---
    print("  PROTO.GET...", file=sys.stderr)
    elapsed, latencies = bench_proto_get(r, proto_keys)
    results["proto_get"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['proto_get']['ops_sec']} ops/sec", file=sys.stderr)

    # --- PROTO.GETFIELD ---
    print("  PROTO.GETFIELD (name)...", file=sys.stderr)
    elapsed, latencies = bench_proto_getfield(r, proto_keys, "name")
    results["proto_getfield"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['proto_getfield']['ops_sec']} ops/sec", file=sys.stderr)

    # --- PROTO.SETFIELD ---
    print("  PROTO.SETFIELD (age)...", file=sys.stderr)
    elapsed, latencies = bench_proto_setfield(r, proto_keys, "age", "31")
    results["proto_setfield"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['proto_setfield']['ops_sec']} ops/sec", file=sys.stderr)

    # --- compute overhead ---
    if results["raw_set"]["ops_sec"] > 0:
        results["set_overhead_pct"] = round(
            (1 - results["proto_set"]["ops_sec"] / results["raw_set"]["ops_sec"]) * 100, 1
        )
    if results["raw_get"]["ops_sec"] > 0:
        results["get_overhead_pct"] = round(
            (1 - results["proto_get"]["ops_sec"] / results["raw_get"]["ops_sec"]) * 100, 1
        )

    results["config"] = {
        "requests": count,
        "type_name": type_name,
        "message_bytes": len(user_bytes),
    }

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"  results saved to {args.output}", file=sys.stderr)

    json.dump(results, sys.stdout, indent=2)
    print()

    r.close()


if __name__ == "__main__":
    main()
