#!/usr/bin/env python3
"""
pub/sub throughput and fan-out benchmark harness.

measures publish throughput, fan-out delivery rate, and delivery latency
across varying subscriber counts and message sizes.

called by bench-pubsub.sh with appropriate arguments.

usage:
    python3 bench/bench-pubsub.py --port 6379 --subscribers 10 --msg-size 64
"""

import argparse
import json
import time
import sys
import threading
import queue


def run_subscriber(host, port, channel, pattern, msg_count, result_queue, ready_event):
    """subscribe to a channel and count received messages."""
    import redis
    r = redis.Redis(host=host, port=port, decode_responses=True)
    ps = r.pubsub()

    if pattern:
        ps.psubscribe(channel)
    else:
        ps.subscribe(channel)

    # wait for subscription confirmation
    msg = ps.get_message(timeout=5)
    while msg and msg["type"] in ("subscribe", "psubscribe"):
        msg = ps.get_message(timeout=1)

    ready_event.set()

    received = 0
    timestamps = []
    deadline = time.perf_counter() + 30  # safety timeout

    while received < msg_count and time.perf_counter() < deadline:
        msg = ps.get_message(timeout=1)
        if msg and msg["type"] in ("message", "pmessage"):
            t_recv = time.perf_counter()
            # extract publish timestamp from message data
            try:
                t_pub = float(msg["data"].split("|")[0])
                timestamps.append(t_recv - t_pub)
            except (ValueError, IndexError):
                timestamps.append(0)
            received += 1

    ps.unsubscribe()
    ps.close()
    r.close()

    result_queue.put({
        "received": received,
        "latencies": timestamps,
    })


def bench_pubsub(host, port, num_subscribers, msg_count, msg_size,
                 use_pattern=False, warmup=1000):
    """run a pub/sub benchmark with the given parameters."""
    import redis

    channel = "bench:pubsub"
    pattern_channel = "bench:*" if use_pattern else None

    # start subscribers
    result_queue = queue.Queue()
    ready_events = []
    threads = []

    for _ in range(num_subscribers):
        ready = threading.Event()
        ready_events.append(ready)
        t = threading.Thread(
            target=run_subscriber,
            args=(host, port,
                  pattern_channel if use_pattern else channel,
                  use_pattern, msg_count, result_queue, ready),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # wait for all subscribers to be ready
    for ev in ready_events:
        if not ev.wait(timeout=10):
            print("  warning: subscriber did not become ready", file=sys.stderr)

    # small delay to ensure subscriptions are fully registered
    time.sleep(0.2)

    # publisher
    r = redis.Redis(host=host, port=port, decode_responses=True)

    # build message payload (timestamp prefix + padding)
    padding = "x" * max(0, msg_size - 30)  # ~30 bytes for timestamp prefix

    # warmup
    for i in range(warmup):
        r.publish(channel, f"{time.perf_counter()}|warmup{padding}")

    # timed publish
    pub_start = time.perf_counter()
    for i in range(msg_count):
        r.publish(channel, f"{time.perf_counter()}|{i}{padding}")
    pub_elapsed = time.perf_counter() - pub_start

    r.close()

    # collect subscriber results
    for t in threads:
        t.join(timeout=30)

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # aggregate
    total_received = sum(r["received"] for r in results)
    all_latencies = []
    for r in results:
        all_latencies.extend(r["latencies"])

    pub_rate = msg_count / pub_elapsed if pub_elapsed > 0 else 0
    fanout_rate = total_received / pub_elapsed if pub_elapsed > 0 else 0

    stats = {
        "subscribers": num_subscribers,
        "messages_published": msg_count,
        "total_received": total_received,
        "publish_rate": round(pub_rate),
        "fanout_rate": round(fanout_rate),
        "publish_elapsed_sec": round(pub_elapsed, 3),
    }

    if all_latencies:
        sorted_lat = sorted(all_latencies)
        stats["p50_ms"] = round(percentile(sorted_lat, 50) * 1000, 3)
        stats["p95_ms"] = round(percentile(sorted_lat, 95) * 1000, 3)
        stats["p99_ms"] = round(percentile(sorted_lat, 99) * 1000, 3)
    else:
        stats["p50_ms"] = 0
        stats["p95_ms"] = 0
        stats["p99_ms"] = 0

    return stats


def percentile(sorted_data, p):
    """compute p-th percentile from pre-sorted data."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_data) - 1)
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def main():
    parser = argparse.ArgumentParser(description="pub/sub benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--messages", type=int, default=10000)
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--output", default=None, help="JSON output file")
    args = parser.parse_args()

    all_results = []

    test_matrix = [
        # (subscribers, msg_size, use_pattern, label)
        (1, 64, False, "1 sub, 64B, SUBSCRIBE"),
        (10, 64, False, "10 sub, 64B, SUBSCRIBE"),
        (100, 64, False, "100 sub, 64B, SUBSCRIBE"),
        (1, 1024, False, "1 sub, 1KB, SUBSCRIBE"),
        (10, 1024, False, "10 sub, 1KB, SUBSCRIBE"),
        (100, 1024, False, "100 sub, 1KB, SUBSCRIBE"),
        (10, 64, True, "10 sub, 64B, PSUBSCRIBE"),
        (100, 64, True, "100 sub, 64B, PSUBSCRIBE"),
    ]

    for subs, msg_size, use_pattern, label in test_matrix:
        print(f"  {label}...", file=sys.stderr)
        stats = bench_pubsub(
            args.host, args.port,
            num_subscribers=subs,
            msg_count=args.messages,
            msg_size=msg_size,
            use_pattern=use_pattern,
            warmup=args.warmup,
        )
        stats["label"] = label
        all_results.append(stats)
        print(f"    pub: {stats['publish_rate']} msg/s, "
              f"fanout: {stats['fanout_rate']} msg/s, "
              f"p99: {stats['p99_ms']}ms",
              file=sys.stderr)

    output = {"tests": all_results}

    if args.output:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  results saved to {args.output}", file=sys.stderr)

    json.dump(output, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
