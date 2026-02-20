#!/bin/bash
#
# full benchmark suite comparing ember (sharded + concurrent) vs redis
#
# usage: ./scripts/bench.sh
#
# requirements:
#   - ember built with: cargo build --release --features jemalloc
#   - redis-server and redis-benchmark installed
#   - redis-benchmark with --threads support (redis 6+)

set -e

REQUESTS=${REQUESTS:-500000}
CLIENTS=${CLIENTS:-50}
PIPELINE=${PIPELINE:-16}
VALUE_SIZE=${VALUE_SIZE:-64}
THREADS=${THREADS:-8}

EMBER_SHARDED_PORT=6380
EMBER_CONCURRENT_PORT=6381
REDIS_PORT=6379

cleanup() {
    pkill -f "ember-server.*${EMBER_SHARDED_PORT}" 2>/dev/null || true
    pkill -f "ember-server.*${EMBER_CONCURRENT_PORT}" 2>/dev/null || true
    redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
}

trap cleanup EXIT

echo "============================================="
echo "EMBER BENCHMARK SUITE"
echo "============================================="
echo "requests:   $REQUESTS"
echo "clients:    $CLIENTS"
echo "pipeline:   $PIPELINE"
echo "value size: ${VALUE_SIZE}B"
echo "threads:    $THREADS"
echo "============================================="
echo ""

# find ember binary
EMBER_BIN="./target/release/ember-server"
if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN"
    echo "run: cargo build --release --features jemalloc"
    exit 1
fi

# check redis
if ! command -v redis-server &> /dev/null; then
    echo "error: redis-server not found"
    exit 1
fi

cleanup

echo "starting servers..."
$EMBER_BIN --port $EMBER_SHARDED_PORT --no-grpc > /dev/null 2>&1 &
$EMBER_BIN --port $EMBER_CONCURRENT_PORT --concurrent --no-grpc > /dev/null 2>&1 &
redis-server --port $REDIS_PORT --daemonize yes --save "" --appendonly no > /dev/null 2>&1
sleep 2

# verify servers are up
redis-cli -p $EMBER_SHARDED_PORT PING > /dev/null || { echo "ember sharded failed to start"; exit 1; }
redis-cli -p $EMBER_CONCURRENT_PORT PING > /dev/null || { echo "ember concurrent failed to start"; exit 1; }
redis-cli -p $REDIS_PORT PING > /dev/null || { echo "redis failed to start"; exit 1; }

echo ""
echo "=== THROUGHPUT (P=$PIPELINE, $THREADS threads) ==="
echo ""

echo "ember sharded:"
echo -n "  SET: "
redis-benchmark -p $EMBER_SHARDED_PORT -t set -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'
echo -n "  GET: "
redis-benchmark -p $EMBER_SHARDED_PORT -t get -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'

echo ""
echo "ember concurrent:"
echo -n "  SET: "
redis-benchmark -p $EMBER_CONCURRENT_PORT -t set -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'
echo -n "  GET: "
redis-benchmark -p $EMBER_CONCURRENT_PORT -t get -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'

echo ""
echo "redis:"
echo -n "  SET: "
redis-benchmark -p $REDIS_PORT -t set -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'
echo -n "  GET: "
redis-benchmark -p $REDIS_PORT -t get -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'

echo ""
echo "=== PIPELINE SCALING (sharded, $THREADS threads) ==="
echo ""

# sweep pipeline depths to show how throughput scales with batching.
# this validates the dispatch-collect pattern efficiency.
for P in 1 4 16 64 256; do
  echo -n "  P=$P SET: "
  redis-benchmark -p $EMBER_SHARDED_PORT -t set -n $REQUESTS -c $CLIENTS -P $P -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'
  echo -n "  P=$P GET: "
  redis-benchmark -p $EMBER_SHARDED_PORT -t get -n $REQUESTS -c $CLIENTS -P $P -d $VALUE_SIZE --threads $THREADS 2>/dev/null | grep "requests per second" | awk '{print $1}'
done

echo ""
echo "=== TRANSACTION OVERHEAD ==="
echo ""

# compare bare SET vs MULTI+SET+EXEC to quantify transaction cost.
# transactions force serial execution (no parallel shard dispatch).
MULTI_REQUESTS=${MULTI_REQUESTS:-100000}

echo "ember sharded (P=1):"
echo -n "  bare SET:       "
redis-benchmark -p $EMBER_SHARDED_PORT -t set -n $MULTI_REQUESTS -c $CLIENTS -P 1 -d $VALUE_SIZE 2>/dev/null | grep "requests per second" | awk '{print $1}'
echo -n "  MULTI/SET/EXEC: "
redis-benchmark -p $EMBER_SHARDED_PORT -n $MULTI_REQUESTS -c $CLIENTS -P 1 \
  eval "redis.call('MULTI'); redis.call('SET','__rand_int__','__data__'); return redis.call('EXEC')" 0 \
  2>/dev/null | grep "requests per second" | awk '{print $1}' || \
  echo "(skipped — redis-benchmark eval not supported)"

echo ""
echo "redis (P=1):"
echo -n "  bare SET:       "
redis-benchmark -p $REDIS_PORT -t set -n $MULTI_REQUESTS -c $CLIENTS -P 1 -d $VALUE_SIZE 2>/dev/null | grep "requests per second" | awk '{print $1}'
echo -n "  MULTI/SET/EXEC: "
redis-benchmark -p $REDIS_PORT -n $MULTI_REQUESTS -c $CLIENTS -P 1 \
  eval "redis.call('MULTI'); redis.call('SET','__rand_int__','__data__'); return redis.call('EXEC')" 0 \
  2>/dev/null | grep "requests per second" | awk '{print $1}' || \
  echo "(skipped — redis-benchmark eval not supported)"

echo ""
echo "=== LATENCY (P=1, single thread) ==="
echo ""

LATENCY_REQUESTS=100000

echo "ember sharded:"
redis-benchmark -p $EMBER_SHARDED_PORT -t set -n $LATENCY_REQUESTS -c $CLIENTS -d $VALUE_SIZE 2>/dev/null | grep -E "requests per second|<="

echo ""
echo "ember concurrent:"
redis-benchmark -p $EMBER_CONCURRENT_PORT -t set -n $LATENCY_REQUESTS -c $CLIENTS -d $VALUE_SIZE 2>/dev/null | grep -E "requests per second|<="

echo ""
echo "redis:"
redis-benchmark -p $REDIS_PORT -t set -n $LATENCY_REQUESTS -c $CLIENTS -d $VALUE_SIZE 2>/dev/null | grep -E "requests per second|<="

echo ""
echo "=== DONE ==="
