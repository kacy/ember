#!/bin/bash
#
# stress benchmarks — eviction, large values, and connection storm
#
# usage: ./bench/bench-stress.sh
#
# requirements:
#   - ember built with: cargo build --release
#   - redis-benchmark installed

set -e

EMBER_PORT=6382
CLIENTS=${CLIENTS:-50}
THREADS=${THREADS:-8}

cleanup() {
    pkill -f "ember-server.*${EMBER_PORT}" 2>/dev/null || true
}

trap cleanup EXIT

EMBER_BIN="./target/release/ember-server"
EMBER_CLI="./target/release/ember-cli"
if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN"
    echo "run: cargo build --release"
    exit 1
fi

echo "============================================="
echo "STRESS BENCHMARKS"
echo "============================================="
echo ""

# ─── 1. large value benchmarks ───────────────────────────────────────

echo "=== LARGE VALUE BENCHMARKS ==="
echo ""
echo "measures throughput at different value sizes to test"
echo "memory tracking accuracy and allocation overhead."
echo ""

cleanup
$EMBER_BIN --port $EMBER_PORT --no-grpc > /dev/null 2>&1 &
sleep 1
redis-cli -p $EMBER_PORT PING > /dev/null || { echo "ember failed to start"; exit 1; }

REQUESTS=100000
for SIZE in 64 256 1024 4096 16384 65536; do
    if [[ $SIZE -ge 16384 ]]; then
        REQUESTS=10000
    fi
    echo -n "  SET ${SIZE}B (P=16): "
    redis-benchmark -p $EMBER_PORT -t set -n $REQUESTS -c $CLIENTS \
        -P 16 -d $SIZE --threads $THREADS -q 2>/dev/null

    echo -n "  GET ${SIZE}B (P=16): "
    redis-benchmark -p $EMBER_PORT -t get -n $REQUESTS -c $CLIENTS \
        -P 16 -d $SIZE --threads $THREADS -q 2>/dev/null
done

cleanup

# ─── 2. eviction benchmark ───────────────────────────────────────────

echo ""
echo "=== EVICTION BENCHMARK ==="
echo ""
echo "starts ember with 32MB maxmemory, writes until eviction kicks in,"
echo "then measures steady-state throughput under memory pressure."
echo ""

# start with tight memory limit to force eviction quickly
$EMBER_BIN --port $EMBER_PORT --no-grpc --maxmemory 32mb > /dev/null 2>&1 &
sleep 1
redis-cli -p $EMBER_PORT PING > /dev/null || { echo "ember failed to start"; exit 1; }

# phase 1: fill memory (256-byte values, ~100k keys = ~25MB of data)
echo "  phase 1: filling memory..."
redis-benchmark -p $EMBER_PORT -t set -n 200000 -c $CLIENTS \
    -P 16 -d 256 --threads $THREADS -q 2>/dev/null | sed 's/^/    /'

# check memory usage
USED=$(redis-cli -p $EMBER_PORT INFO memory 2>/dev/null | grep "used_memory:" | tr -d '\r' | cut -d: -f2)
echo "    used_memory: ${USED:-unknown} bytes"

# phase 2: steady-state under eviction
echo ""
echo "  phase 2: throughput under eviction pressure..."
echo -n "    SET (P=16): "
redis-benchmark -p $EMBER_PORT -t set -n 200000 -c $CLIENTS \
    -P 16 -d 256 --threads $THREADS -q 2>/dev/null
echo -n "    GET (P=16): "
redis-benchmark -p $EMBER_PORT -t get -n 200000 -c $CLIENTS \
    -P 16 -d 256 --threads $THREADS -q 2>/dev/null

# phase 3: single-pipeline latency under eviction
echo ""
echo "  phase 3: latency under eviction (P=1)..."
echo -n "    SET (P=1):  "
redis-benchmark -p $EMBER_PORT -t set -n 50000 -c $CLIENTS \
    -P 1 -d 256 -q 2>/dev/null

cleanup

# ─── 3. connection storm benchmark ───────────────────────────────────

echo ""
echo "=== CONNECTION STORM BENCHMARK ==="
echo ""
echo "tests rapid connection/disconnection under burst load."
echo "verifies semaphore-based connection limiting works correctly."
echo ""

$EMBER_BIN --port $EMBER_PORT --no-grpc > /dev/null 2>&1 &
sleep 1
redis-cli -p $EMBER_PORT PING > /dev/null || { echo "ember failed to start"; exit 1; }

# rapid connect/disconnect with many clients
for BURST in 100 500 1000; do
    echo -n "  $BURST clients, PING (P=1): "
    redis-benchmark -p $EMBER_PORT -t ping -n $((BURST * 10)) -c $BURST \
        -P 1 -q 2>/dev/null
done

# high client count with pipelining
echo ""
for BURST in 100 500 1000; do
    echo -n "  $BURST clients, SET (P=16): "
    redis-benchmark -p $EMBER_PORT -t set -n $((BURST * 100)) -c $BURST \
        -P 16 -d 64 -q 2>/dev/null
done

cleanup

echo ""
echo "=== DONE ==="
