#!/bin/bash
#
# memory usage comparison between ember and redis
#
# usage: ./scripts/bench-memory.sh

set -e

KEY_COUNT=${KEY_COUNT:-1000000}
VALUE_SIZE=${VALUE_SIZE:-64}

EMBER_PORT=6380
REDIS_PORT=6379

cleanup() {
    pkill -f "ember-server.*${EMBER_PORT}" 2>/dev/null || true
    redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
}

trap cleanup EXIT

EMBER_BIN="./target/release/ember-server"
if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN"
    echo "run: cargo build --release"
    exit 1
fi

cleanup

echo "============================================="
echo "MEMORY BENCHMARK"
echo "============================================="
echo "keys:       $KEY_COUNT"
echo "value size: ${VALUE_SIZE}B"
echo "============================================="
echo ""

echo "starting servers..."
$EMBER_BIN --port $EMBER_PORT --concurrent > /dev/null 2>&1 &
EMBER_PID=$!
redis-server --port $REDIS_PORT --daemonize yes --save "" --appendonly no > /dev/null 2>&1
sleep 2

echo ""
echo "initial memory:"
EMBER_INIT=$(ps -o rss= -p $EMBER_PID 2>/dev/null | awk '{print $1}')
REDIS_INIT=$(redis-cli -p $REDIS_PORT info memory 2>/dev/null | grep used_memory: | cut -d: -f2 | tr -d '\r')
echo "  ember: $((EMBER_INIT / 1024)) MB"
echo "  redis: $((REDIS_INIT / 1024 / 1024)) MB"

echo ""
echo "loading $KEY_COUNT keys..."
redis-benchmark -p $EMBER_PORT -t set -n $KEY_COUNT -r $KEY_COUNT -d $VALUE_SIZE -q 2>/dev/null
redis-benchmark -p $REDIS_PORT -t set -n $KEY_COUNT -r $KEY_COUNT -d $VALUE_SIZE -q 2>/dev/null

EMBER_KEYS=$(redis-cli -p $EMBER_PORT DBSIZE 2>/dev/null | awk '{print $2}')
REDIS_KEYS=$(redis-cli -p $REDIS_PORT DBSIZE 2>/dev/null | awk '{print $2}')

echo ""
echo "after loading:"
EMBER_FINAL=$(ps -o rss= -p $EMBER_PID 2>/dev/null | awk '{print $1}')
REDIS_FINAL=$(redis-cli -p $REDIS_PORT info memory 2>/dev/null | grep used_memory: | cut -d: -f2 | tr -d '\r')

EMBER_USED=$((EMBER_FINAL - EMBER_INIT))
REDIS_USED=$((REDIS_FINAL - REDIS_INIT))

echo "  ember: $((EMBER_FINAL / 1024)) MB ($EMBER_KEYS keys)"
echo "  redis: $((REDIS_FINAL / 1024 / 1024)) MB ($REDIS_KEYS keys)"

echo ""
echo "per-key overhead:"
if [[ $EMBER_KEYS -gt 0 ]]; then
    echo "  ember: $((EMBER_USED * 1024 / EMBER_KEYS)) bytes/key"
fi
if [[ $REDIS_KEYS -gt 0 ]]; then
    echo "  redis: $((REDIS_USED / REDIS_KEYS)) bytes/key"
fi

echo ""
echo "done."
