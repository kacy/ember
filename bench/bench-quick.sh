#!/bin/bash
#
# quick benchmark for sanity checks (ember only)
#
# usage: ./scripts/bench-quick.sh

set -e

REQUESTS=100000
CLIENTS=50
PIPELINE=16
VALUE_SIZE=64

PORT=6379

cleanup() {
    pkill -f "ember-server.*${PORT}" 2>/dev/null || true
}

trap cleanup EXIT

EMBER_BIN="./target/release/ember-server"
if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN"
    echo "run: cargo build --release"
    exit 1
fi

cleanup

echo "starting ember on port $PORT..."
$EMBER_BIN --port $PORT > /dev/null 2>&1 &
sleep 1

redis-cli -p $PORT PING > /dev/null || { echo "ember failed to start"; exit 1; }

echo ""
echo "=== QUICK BENCHMARK ==="
echo ""

echo -n "SET (P=$PIPELINE): "
redis-benchmark -p $PORT -t set -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE -q 2>/dev/null

echo -n "GET (P=$PIPELINE): "
redis-benchmark -p $PORT -t get -n $REQUESTS -c $CLIENTS -P $PIPELINE -d $VALUE_SIZE -q 2>/dev/null

echo -n "SET (P=1):  "
redis-benchmark -p $PORT -t set -n $REQUESTS -c $CLIENTS -P 1 -d $VALUE_SIZE -q 2>/dev/null

echo -n "GET (P=1):  "
redis-benchmark -p $PORT -t get -n $REQUESTS -c $CLIENTS -P 1 -d $VALUE_SIZE -q 2>/dev/null

echo ""
echo "done."
