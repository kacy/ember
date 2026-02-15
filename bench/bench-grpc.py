#!/usr/bin/env python3
"""
gRPC vs RESP3 benchmark harness.

measures SET/GET throughput and latency for ember's gRPC interface
compared to the standard RESP3 protocol. called by bench-grpc.sh.

usage:
    python3 bench/bench-grpc.py --grpc-addr localhost:6380 --resp-port 6379
"""

import argparse
import json
import time
import sys
import os
import random
import string
import statistics


def random_value(size: int) -> bytes:
    """generate a random byte string of the given size."""
    return bytes(random.getrandbits(8) for _ in range(size))


def random_key(prefix: str, i: int) -> str:
    return f"{prefix}:{i}"


# ---------------------------------------------------------------------------
# RESP3 benchmarks (using redis-py)
# ---------------------------------------------------------------------------

def bench_resp_set(host, port, keys, value, warmup=1000):
    """sequential SET throughput via RESP3."""
    import redis
    r = redis.Redis(host=host, port=port)

    # warmup
    for i in range(warmup):
        r.set(f"warmup:{i}", value)

    latencies = []
    start = time.perf_counter()
    for i in range(len(keys)):
        t0 = time.perf_counter()
        r.set(keys[i], value)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start

    r.close()
    return elapsed, latencies


def bench_resp_get(host, port, keys, warmup=1000):
    """sequential GET throughput via RESP3."""
    import redis
    r = redis.Redis(host=host, port=port)

    # warmup
    for i in range(warmup):
        r.get(f"warmup:{i}")

    latencies = []
    start = time.perf_counter()
    for i in range(len(keys)):
        t0 = time.perf_counter()
        r.get(keys[i])
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start

    r.close()
    return elapsed, latencies


def bench_resp_pipeline(host, port, keys, value, batch_size=100):
    """pipelined SET+GET throughput via RESP3."""
    import redis
    r = redis.Redis(host=host, port=port)

    # SET pipeline
    set_latencies = []
    set_start = time.perf_counter()
    for batch_start in range(0, len(keys), batch_size):
        batch_end = min(batch_start + batch_size, len(keys))
        t0 = time.perf_counter()
        pipe = r.pipeline(transaction=False)
        for i in range(batch_start, batch_end):
            pipe.set(keys[i], value)
        pipe.execute()
        batch_time = time.perf_counter() - t0
        per_op = batch_time / (batch_end - batch_start)
        set_latencies.extend([per_op] * (batch_end - batch_start))
    set_elapsed = time.perf_counter() - set_start

    # GET pipeline
    get_latencies = []
    get_start = time.perf_counter()
    for batch_start in range(0, len(keys), batch_size):
        batch_end = min(batch_start + batch_size, len(keys))
        t0 = time.perf_counter()
        pipe = r.pipeline(transaction=False)
        for i in range(batch_start, batch_end):
            pipe.get(keys[i])
        pipe.execute()
        batch_time = time.perf_counter() - t0
        per_op = batch_time / (batch_end - batch_start)
        get_latencies.extend([per_op] * (batch_end - batch_start))
    get_elapsed = time.perf_counter() - get_start

    r.close()
    return set_elapsed, set_latencies, get_elapsed, get_latencies


# ---------------------------------------------------------------------------
# gRPC benchmarks (using ember-py)
# ---------------------------------------------------------------------------

def bench_grpc_set(addr, keys, value, warmup=1000):
    """sequential SET throughput via gRPC unary calls."""
    from ember import EmberClient
    client = EmberClient(addr)

    # warmup
    for i in range(warmup):
        client.set(f"warmup:{i}", value)

    latencies = []
    start = time.perf_counter()
    for i in range(len(keys)):
        t0 = time.perf_counter()
        client.set(keys[i], value)
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start

    client.close()
    return elapsed, latencies


def bench_grpc_get(addr, keys, warmup=1000):
    """sequential GET throughput via gRPC unary calls."""
    from ember import EmberClient
    client = EmberClient(addr)

    # warmup
    for i in range(warmup):
        client.get(f"warmup:{i}")

    latencies = []
    start = time.perf_counter()
    for i in range(len(keys)):
        t0 = time.perf_counter()
        client.get(keys[i])
        latencies.append(time.perf_counter() - t0)
    elapsed = time.perf_counter() - start

    client.close()
    return elapsed, latencies


# ---------------------------------------------------------------------------
# stats helpers
# ---------------------------------------------------------------------------

def percentile(data, p):
    """compute the p-th percentile of a list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def compute_stats(elapsed, latencies, count):
    """compute throughput and latency stats."""
    ops_sec = count / elapsed if elapsed > 0 else 0
    p50 = percentile(latencies, 50) * 1000  # ms
    p95 = percentile(latencies, 95) * 1000
    p99 = percentile(latencies, 99) * 1000
    return {
        "ops_sec": round(ops_sec),
        "p50_ms": round(p50, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="gRPC vs RESP3 benchmark")
    parser.add_argument("--resp-host", default="127.0.0.1")
    parser.add_argument("--resp-port", type=int, default=6379)
    parser.add_argument("--grpc-addr", default="127.0.0.1:6380")
    parser.add_argument("--requests", type=int, default=100000)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--pipeline-batch", type=int, default=100)
    parser.add_argument("--output", default=None, help="JSON output file")
    args = parser.parse_args()

    count = args.requests
    value = random_value(args.value_size)
    keys = [random_key("bench", i) for i in range(count)]

    # pre-populate keys for GET tests
    print(f"  pre-populating {count} keys via RESP3...", file=sys.stderr)
    import redis
    r = redis.Redis(host=args.resp_host, port=args.resp_port)
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.set(k, value)
    pipe.execute()
    r.close()

    results = {}

    # --- RESP3 sequential ---
    print("  RESP3 sequential SET...", file=sys.stderr)
    elapsed, latencies = bench_resp_set(args.resp_host, args.resp_port, keys, value)
    results["resp_set"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['resp_set']['ops_sec']} ops/sec", file=sys.stderr)

    print("  RESP3 sequential GET...", file=sys.stderr)
    elapsed, latencies = bench_resp_get(args.resp_host, args.resp_port, keys)
    results["resp_get"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['resp_get']['ops_sec']} ops/sec", file=sys.stderr)

    # --- RESP3 pipelined ---
    print(f"  RESP3 pipelined (batch={args.pipeline_batch})...", file=sys.stderr)
    se, sl, ge, gl = bench_resp_pipeline(
        args.resp_host, args.resp_port, keys, value, args.pipeline_batch
    )
    results["resp_pipeline_set"] = compute_stats(se, sl, count)
    results["resp_pipeline_get"] = compute_stats(ge, gl, count)
    print(f"    SET: {results['resp_pipeline_set']['ops_sec']} ops/sec", file=sys.stderr)
    print(f"    GET: {results['resp_pipeline_get']['ops_sec']} ops/sec", file=sys.stderr)

    # --- gRPC unary ---
    print("  gRPC unary SET...", file=sys.stderr)
    elapsed, latencies = bench_grpc_set(args.grpc_addr, keys, value)
    results["grpc_set"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['grpc_set']['ops_sec']} ops/sec", file=sys.stderr)

    print("  gRPC unary GET...", file=sys.stderr)
    elapsed, latencies = bench_grpc_get(args.grpc_addr, keys)
    results["grpc_get"] = compute_stats(elapsed, latencies, count)
    print(f"    {results['grpc_get']['ops_sec']} ops/sec", file=sys.stderr)

    # --- output ---
    results["config"] = {
        "requests": count,
        "value_size": args.value_size,
        "pipeline_batch": args.pipeline_batch,
    }

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"  results saved to {args.output}", file=sys.stderr)

    # return results as JSON to stdout for the shell wrapper
    json.dump(results, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
