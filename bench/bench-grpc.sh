#!/usr/bin/env bash
#
# gRPC vs RESP3 benchmark for standard operations (SET/GET).
#
# compares ember's gRPC interface against RESP3 for sequential and pipelined
# workloads using the ember-py gRPC client and redis-py.
#
# usage:
#   bash bench/bench-grpc.sh
#   bash bench/bench-grpc.sh --quick    # 10k requests instead of 100k
#
# requirements:
#   - ember built with: cargo build --release -p ember-server --features jemalloc,grpc
#   - python3 with redis and ember-py installed
#
# environment variables:
#   EMBER_PORT       RESP3 port          (default: 6379)
#   EMBER_GRPC_PORT  gRPC port           (default: 6380)
#   BENCH_REQUESTS   requests per test   (default: 100000)
#   VALUE_SIZE       value size in bytes (default: 64)

set -euo pipefail

EMBER_PORT="${EMBER_PORT:-6379}"
EMBER_GRPC_PORT="${EMBER_GRPC_PORT:-6380}"
REQUESTS="${BENCH_REQUESTS:-100000}"
VALUE_SIZE="${VALUE_SIZE:-64}"
EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"
RESULTS_DIR="bench/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BENCH_SCRIPT="bench/bench-grpc.py"

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
    echo "build with: cargo build --release -p ember-server --features jemalloc,grpc" >&2
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo "error: python3 required" >&2
    exit 1
fi

# set up venv if needed
VENV_DIR=".bench-venv"

ensure_venv() {
    if [[ ! -d "$VENV_DIR" ]]; then
        echo "creating python venv for benchmark dependencies..."
        python3 -m venv "$VENV_DIR"
    fi
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
}

if ! python3 -c "import redis" 2>/dev/null; then
    ensure_venv
    pip install --quiet redis
fi

# install ember-py for gRPC client
ensure_venv
pip install --quiet ./clients/ember-py

# --- start server ---

echo ""
echo "=== gRPC vs RESP3 benchmark ==="
echo "requests:   $REQUESTS"
echo "value size: ${VALUE_SIZE}B"
echo "RESP port:  $EMBER_PORT"
echo "gRPC port:  $EMBER_GRPC_PORT"
echo ""

echo "starting ember (RESP: $EMBER_PORT, gRPC: $EMBER_GRPC_PORT)..."
"$EMBER_BIN" --port "$EMBER_PORT" --grpc-port "$EMBER_GRPC_PORT" > /dev/null 2>&1 &
EMBER_PID=$!

# wait for RESP
retries=50
while ! redis-cli -p "$EMBER_PORT" ping > /dev/null 2>&1; do
    retries=$((retries - 1))
    if [[ $retries -le 0 ]]; then
        echo "error: ember did not start on port $EMBER_PORT" >&2
        exit 1
    fi
    sleep 0.1
done

# brief extra wait for gRPC to bind
sleep 1

echo ""

# --- run python benchmark ---

mkdir -p "$RESULTS_DIR"
RESULT_JSON="$RESULTS_DIR/${TIMESTAMP}-grpc.json"

python3 "$BENCH_SCRIPT" \
    --resp-port "$EMBER_PORT" \
    --grpc-addr "127.0.0.1:$EMBER_GRPC_PORT" \
    --requests "$REQUESTS" \
    --value-size "$VALUE_SIZE" \
    --output "$RESULT_JSON" \
    > /dev/null

# --- parse and display results ---

extract() {
    python3 -c "
import json, sys
d = json.load(open('$RESULT_JSON'))
keys = '$1'.split('.')
v = d
for k in keys:
    v = v[k]
print(v)
" 2>/dev/null || echo "—"
}

echo ""
echo "========================================================================"
echo "             gRPC vs RESP3 benchmark results"
echo "========================================================================"
echo ""

fmt="%-28s %14s %14s %14s"
printf "$fmt\n" "test" "ops/sec" "p50 (ms)" "p99 (ms)"
printf "$fmt\n" "----" "-------" "--------" "--------"
printf "$fmt\n" "RESP3 SET (sequential)" "$(extract resp_set.ops_sec)" "$(extract resp_set.p50_ms)" "$(extract resp_set.p99_ms)"
printf "$fmt\n" "RESP3 GET (sequential)" "$(extract resp_get.ops_sec)" "$(extract resp_get.p50_ms)" "$(extract resp_get.p99_ms)"
printf "$fmt\n" "RESP3 SET (pipelined)" "$(extract resp_pipeline_set.ops_sec)" "$(extract resp_pipeline_set.p50_ms)" "$(extract resp_pipeline_set.p99_ms)"
printf "$fmt\n" "RESP3 GET (pipelined)" "$(extract resp_pipeline_get.ops_sec)" "$(extract resp_pipeline_get.p50_ms)" "$(extract resp_pipeline_get.p99_ms)"
printf "$fmt\n" "gRPC SET (unary)" "$(extract grpc_set.ops_sec)" "$(extract grpc_set.p50_ms)" "$(extract grpc_set.p99_ms)"
printf "$fmt\n" "gRPC GET (unary)" "$(extract grpc_get.ops_sec)" "$(extract grpc_get.p50_ms)" "$(extract grpc_get.p99_ms)"

echo ""
echo "results saved to $RESULT_JSON"
