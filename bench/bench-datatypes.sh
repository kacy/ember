#!/bin/bash
#
# data type benchmark — compares ember vs redis across all data structures
#
# usage: ./bench/bench-datatypes.sh [--ember-only]
#
# requirements:
#   - ember built with: cargo build --release
#   - redis-server and redis-benchmark installed (unless --ember-only)

set -e

REQUESTS=${REQUESTS:-500000}
CLIENTS=${CLIENTS:-50}
PIPELINE=${PIPELINE:-16}
VALUE_SIZE=${VALUE_SIZE:-64}
THREADS=${THREADS:-8}
EMBER_ONLY=false

if [[ "$1" == "--ember-only" ]]; then
    EMBER_ONLY=true
fi

EMBER_PORT=6380
REDIS_PORT=6379

cleanup() {
    pkill -f "ember-server.*${EMBER_PORT}" 2>/dev/null || true
    if [[ "$EMBER_ONLY" == "false" ]]; then
        redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
    fi
}

trap cleanup EXIT

EMBER_BIN="./target/release/ember-server"
if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN"
    echo "run: cargo build --release"
    exit 1
fi

EMBER_CLI="./target/release/ember-cli"
if [[ ! -x "$EMBER_CLI" ]]; then
    echo "error: ember-cli not found at $EMBER_CLI"
    echo "run: cargo build --release"
    exit 1
fi

cleanup

echo "============================================="
echo "DATA TYPE BENCHMARK"
echo "============================================="
echo "requests:   $REQUESTS"
echo "clients:    $CLIENTS"
echo "pipeline:   $PIPELINE"
echo "value size: ${VALUE_SIZE}B"
echo "============================================="
echo ""

# start ember
echo "starting ember on port $EMBER_PORT..."
$EMBER_BIN --port $EMBER_PORT > /dev/null 2>&1 &
sleep 1
redis-cli -p $EMBER_PORT PING > /dev/null || { echo "ember failed to start"; exit 1; }

echo ""
echo "=== EMBER (all data types, P=$PIPELINE) ==="
echo ""

# use the built-in benchmark for data type workloads
$EMBER_CLI -p $EMBER_PORT benchmark \
    -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE -q \
    -t set,get,lpush,sadd,zadd,hset,hget

echo ""

# also run redis-benchmark for the standard types it supports
echo "=== EMBER (redis-benchmark, P=$PIPELINE) ==="
echo ""

for CMD in set get lpush rpush sadd; do
    echo -n "  $CMD: "
    redis-benchmark -p $EMBER_PORT -t $CMD -n $REQUESTS -c $CLIENTS \
        -P $PIPELINE -d $VALUE_SIZE --threads $THREADS -q 2>/dev/null
done

if [[ "$EMBER_ONLY" == "true" ]]; then
    echo ""
    echo "=== DONE (ember-only mode) ==="
    exit 0
fi

# start redis
if ! command -v redis-server &> /dev/null; then
    echo "redis-server not found, skipping redis comparison"
    exit 0
fi

echo ""
echo "starting redis on port $REDIS_PORT..."
redis-server --port $REDIS_PORT --daemonize yes --save "" --appendonly no > /dev/null 2>&1
sleep 1
redis-cli -p $REDIS_PORT PING > /dev/null || { echo "redis failed to start"; exit 1; }

echo ""
echo "=== REDIS (redis-benchmark, P=$PIPELINE) ==="
echo ""

for CMD in set get lpush rpush sadd; do
    echo -n "  $CMD: "
    redis-benchmark -p $REDIS_PORT -t $CMD -n $REQUESTS -c $CLIENTS \
        -P $PIPELINE -d $VALUE_SIZE --threads $THREADS -q 2>/dev/null
done

echo ""
echo "=== DONE ==="
