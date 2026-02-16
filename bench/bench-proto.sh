#!/usr/bin/env bash
#
# protobuf storage overhead benchmark.
#
# compares PROTO.SET/GET/GETFIELD/SETFIELD against raw SET/GET to measure
# the cost of server-side schema validation and field-level access.
#
# usage:
#   bash bench/bench-proto.sh
#   bash bench/bench-proto.sh --quick    # 10k requests
#
# requirements:
#   - ember built with: cargo build --release -p ember-server --features jemalloc,protobuf
#   - protoc on PATH (for compiling the test schema)
#   - python3 with redis-py
#
# environment variables:
#   EMBER_PORT       ember port           (default: 6379)
#   BENCH_REQUESTS   requests per test    (default: 100000)

set -euo pipefail

EMBER_PORT="${EMBER_PORT:-6379}"
REQUESTS="${BENCH_REQUESTS:-100000}"
EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"
RESULTS_DIR="bench/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BENCH_SCRIPT="bench/bench-proto.py"

QUICK_MODE=false
for arg in "$@"; do
    case "$arg" in
        --quick) QUICK_MODE=true ;;
        *) echo "unknown flag: $arg"; exit 1 ;;
    esac
done

if [[ "$QUICK_MODE" == "true" ]]; then
    REQUESTS=10000
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
    echo "build with: cargo build --release -p ember-server --features jemalloc,protobuf" >&2
    exit 1
fi

if ! command -v protoc &> /dev/null; then
    echo "error: protoc not found. install protobuf compiler." >&2
    echo "  brew install protobuf   # macOS" >&2
    echo "  apt install protobuf-compiler  # ubuntu" >&2
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
echo "=== protobuf storage overhead benchmark ==="
echo "requests: $REQUESTS"
echo "port:     $EMBER_PORT"
echo ""

echo "starting ember (with protobuf support) on port $EMBER_PORT..."
"$EMBER_BIN" --port "$EMBER_PORT" --protobuf > /dev/null 2>&1 &
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
RESULT_JSON="$RESULTS_DIR/${TIMESTAMP}-proto.json"

python3 "$BENCH_SCRIPT" \
    --port "$EMBER_PORT" \
    --requests "$REQUESTS" \
    --output "$RESULT_JSON" \
    > /dev/null

# --- display results ---

echo ""
echo "========================================================================"
echo "            protobuf storage overhead results"
echo "========================================================================"
echo ""

python3 -c "
import json
d = json.load(open('$RESULT_JSON'))

fmt = '%-28s %14s %14s %14s'
print(fmt % ('test', 'ops/sec', 'p50 (ms)', 'p99 (ms)'))
print(fmt % ('----', '-------', '--------', '--------'))
print(fmt % ('raw SET', d['raw_set']['ops_sec'], d['raw_set']['p50_ms'], d['raw_set']['p99_ms']))
print(fmt % ('PROTO.SET', d['proto_set']['ops_sec'], d['proto_set']['p50_ms'], d['proto_set']['p99_ms']))
print(fmt % ('raw GET', d['raw_get']['ops_sec'], d['raw_get']['p50_ms'], d['raw_get']['p99_ms']))
print(fmt % ('PROTO.GET', d['proto_get']['ops_sec'], d['proto_get']['p50_ms'], d['proto_get']['p99_ms']))
print(fmt % ('PROTO.GETFIELD', d['proto_getfield']['ops_sec'], d['proto_getfield']['p50_ms'], d['proto_getfield']['p99_ms']))
print(fmt % ('PROTO.SETFIELD', d['proto_setfield']['ops_sec'], d['proto_setfield']['p50_ms'], d['proto_setfield']['p99_ms']))

print()
if 'set_overhead_pct' in d:
    print(f'  SET overhead: {d[\"set_overhead_pct\"]}%')
if 'get_overhead_pct' in d:
    print(f'  GET overhead: {d[\"get_overhead_pct\"]}%')
"

echo ""
echo "results saved to $RESULT_JSON"
