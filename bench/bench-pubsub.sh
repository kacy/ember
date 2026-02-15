#!/usr/bin/env bash
#
# pub/sub throughput and fan-out benchmark.
#
# measures publish throughput, fan-out delivery rate, and delivery latency
# across varying subscriber counts (1, 10, 100) and message sizes (64B, 1KB).
# compares SUBSCRIBE vs PSUBSCRIBE performance.
#
# usage:
#   bash bench/bench-pubsub.sh
#   bash bench/bench-pubsub.sh --quick    # fewer messages
#
# requirements:
#   - ember-server binary
#   - python3 with redis-py
#
# environment variables:
#   EMBER_PORT       ember port           (default: 6379)
#   BENCH_MESSAGES   messages per test    (default: 10000)

set -euo pipefail

EMBER_PORT="${EMBER_PORT:-6379}"
MESSAGES="${BENCH_MESSAGES:-10000}"
EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"
RESULTS_DIR="bench/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BENCH_SCRIPT="bench/bench-pubsub.py"

QUICK_MODE=false
for arg in "$@"; do
    case "$arg" in
        --quick) QUICK_MODE=true ;;
        *) echo "unknown flag: $arg"; exit 1 ;;
    esac
done

if [[ "$QUICK_MODE" == "true" ]]; then
    MESSAGES=1000
fi

# --- cleanup ---

EMBER_PID=""

cleanup() {
    [[ -n "$EMBER_PID" ]] && kill "$EMBER_PID" 2>/dev/null && wait "$EMBER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# --- checks ---

if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN" >&2
    echo "run: cargo build --release -p ember-server --features jemalloc" >&2
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo "error: python3 required" >&2
    exit 1
fi

# set up venv if needed
VENV_DIR=".bench-venv"

if ! python3 -c "import redis" 2>/dev/null; then
    if [[ ! -d "$VENV_DIR" ]]; then
        echo "creating python venv for benchmark dependencies..."
        python3 -m venv "$VENV_DIR"
    fi
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    pip install --quiet redis
fi

# --- start server ---

echo ""
echo "=== pub/sub benchmark ==="
echo "messages per test: $MESSAGES"
echo "port:              $EMBER_PORT"
echo ""

echo "starting ember on port $EMBER_PORT..."
"$EMBER_BIN" --port "$EMBER_PORT" > /dev/null 2>&1 &
EMBER_PID=$!

retries=50
while ! redis-cli -p "$EMBER_PORT" ping > /dev/null 2>&1; do
    retries=$((retries - 1))
    if [[ $retries -le 0 ]]; then
        echo "error: ember did not start on port $EMBER_PORT" >&2
        exit 1
    fi
    sleep 0.1
done

echo ""

# --- run benchmark ---

mkdir -p "$RESULTS_DIR"
RESULT_JSON="$RESULTS_DIR/${TIMESTAMP}-pubsub.json"

python3 "$BENCH_SCRIPT" \
    --port "$EMBER_PORT" \
    --messages "$MESSAGES" \
    --output "$RESULT_JSON" \
    > /dev/null

# --- display results ---

echo ""
echo "========================================================================"
echo "                pub/sub benchmark results"
echo "========================================================================"
echo ""

python3 -c "
import json
data = json.load(open('$RESULT_JSON'))
fmt = '%-30s %12s %14s %10s'
print(fmt % ('test', 'pub msg/s', 'fanout msg/s', 'p99 (ms)'))
print(fmt % ('----', '---------', '------------', '--------'))
for t in data['tests']:
    print(fmt % (t['label'], t['publish_rate'], t['fanout_rate'], t['p99_ms']))
"

echo ""
echo "results saved to $RESULT_JSON"
