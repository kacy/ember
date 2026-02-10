#!/usr/bin/env bash
#
# encryption overhead benchmark — compares persistence throughput with and
# without AES-256-GCM encryption at rest.
#
# usage:
#   bash bench/bench-encryption.sh
#
# requirements:
#   - ember built with: cargo build --release -p ember-server --features jemalloc,encryption
#   - redis-benchmark installed (redis 6+ for --threads)

set -euo pipefail

REQUESTS="${BENCH_REQUESTS:-200000}"
CLIENTS="${BENCH_CLIENTS:-50}"
PIPELINE="${BENCH_PIPELINE:-16}"
VALUE_SIZE="${BENCH_VALUE_SIZE:-64}"
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
THREADS="${BENCH_THREADS:-$CPU_CORES}"

PLAINTEXT_PORT="${PLAINTEXT_PORT:-6390}"
ENCRYPTED_PORT="${ENCRYPTED_PORT:-6391}"

EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"

PLAINTEXT_PID=""
ENCRYPTED_PID=""
TMPDIR_PLAIN=""
TMPDIR_ENC=""
KEY_FILE=""

cleanup() {
    [[ -n "$PLAINTEXT_PID" ]] && kill "$PLAINTEXT_PID" 2>/dev/null && wait "$PLAINTEXT_PID" 2>/dev/null || true
    [[ -n "$ENCRYPTED_PID" ]] && kill "$ENCRYPTED_PID" 2>/dev/null && wait "$ENCRYPTED_PID" 2>/dev/null || true
    [[ -n "$TMPDIR_PLAIN" ]] && rm -rf "$TMPDIR_PLAIN"
    [[ -n "$TMPDIR_ENC" ]] && rm -rf "$TMPDIR_ENC"
    [[ -n "$KEY_FILE" ]] && rm -f "$KEY_FILE"
}
trap cleanup EXIT

wait_for_server() {
    local port=$1
    local name=$2
    local retries=50
    while ! redis-cli -p "$port" ping > /dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            echo "error: $name did not start on port $port" >&2
            exit 1
        fi
        sleep 0.1
    done
}

extract_rps() {
    local csv_output=$1
    local test_name=$2
    echo "$csv_output" | grep "\"$test_name\"" | cut -d',' -f2 | tr -d '"' | cut -d'.' -f1
}

format_number() {
    printf "%'d" "$1"
}

calc_overhead() {
    local baseline=$1
    local encrypted=$2
    if [[ "$baseline" -gt 0 ]]; then
        local pct=$(( (baseline - encrypted) * 100 / baseline ))
        echo "${pct}%"
    else
        echo "n/a"
    fi
}

# --- checks ---

if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN" >&2
    echo "build with: cargo build --release -p ember-server --features jemalloc,encryption" >&2
    exit 1
fi

if ! command -v redis-benchmark &> /dev/null; then
    echo "error: redis-benchmark not found. install redis tools first." >&2
    exit 1
fi

# --- setup ---

echo "============================================="
echo "EMBER ENCRYPTION OVERHEAD BENCHMARK"
echo "============================================="
echo "requests:   $REQUESTS"
echo "clients:    $CLIENTS"
echo "pipeline:   $PIPELINE"
echo "value size: ${VALUE_SIZE}B"
echo "threads:    $THREADS"
echo "============================================="
echo ""

# temp directories for persistence
TMPDIR_PLAIN=$(mktemp -d)
TMPDIR_ENC=$(mktemp -d)

# generate a 32-byte random key file
KEY_FILE=$(mktemp)
dd if=/dev/urandom bs=32 count=1 2>/dev/null > "$KEY_FILE"

echo "starting plaintext server on port $PLAINTEXT_PORT..."
$EMBER_BIN --port "$PLAINTEXT_PORT" --data-dir "$TMPDIR_PLAIN" --appendonly > /dev/null 2>&1 &
PLAINTEXT_PID=$!
wait_for_server "$PLAINTEXT_PORT" "ember (plaintext)"

echo "starting encrypted server on port $ENCRYPTED_PORT..."
$EMBER_BIN --port "$ENCRYPTED_PORT" --data-dir "$TMPDIR_ENC" --appendonly --encryption-key-file "$KEY_FILE" > /dev/null 2>&1 &
ENCRYPTED_PID=$!
wait_for_server "$ENCRYPTED_PORT" "ember (encrypted)"

echo ""

# --- pipelined throughput ---

echo "=== PIPELINED THROUGHPUT (P=$PIPELINE, $THREADS threads) ==="
echo ""

PLAIN_CSV=$(redis-benchmark -p "$PLAINTEXT_PORT" -t set,get -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d "$VALUE_SIZE" --threads "$THREADS" --csv -q 2>/dev/null)
ENC_CSV=$(redis-benchmark -p "$ENCRYPTED_PORT" -t set,get -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d "$VALUE_SIZE" --threads "$THREADS" --csv -q 2>/dev/null)

PLAIN_SET=$(extract_rps "$PLAIN_CSV" "SET")
PLAIN_GET=$(extract_rps "$PLAIN_CSV" "GET")
ENC_SET=$(extract_rps "$ENC_CSV" "SET")
ENC_GET=$(extract_rps "$ENC_CSV" "GET")

printf "%-25s %15s %15s %10s\n" "" "plaintext" "encrypted" "overhead"
printf "%-25s %15s %15s %10s\n" "---" "---" "---" "---"
printf "%-25s %15s %15s %10s\n" "SET (P=$PIPELINE)" "$(format_number "$PLAIN_SET")/s" "$(format_number "$ENC_SET")/s" "$(calc_overhead "$PLAIN_SET" "$ENC_SET")"
printf "%-25s %15s %15s %10s\n" "GET (P=$PIPELINE)" "$(format_number "$PLAIN_GET")/s" "$(format_number "$ENC_GET")/s" "$(calc_overhead "$PLAIN_GET" "$ENC_GET")"

echo ""

# --- single-request latency ---

echo "=== SINGLE REQUEST THROUGHPUT (P=1) ==="
echo ""

PLAIN_CSV_1=$(redis-benchmark -p "$PLAINTEXT_PORT" -t set,get -n "$REQUESTS" -c "$CLIENTS" -P 1 -d "$VALUE_SIZE" --threads "$THREADS" --csv -q 2>/dev/null)
ENC_CSV_1=$(redis-benchmark -p "$ENCRYPTED_PORT" -t set,get -n "$REQUESTS" -c "$CLIENTS" -P 1 -d "$VALUE_SIZE" --threads "$THREADS" --csv -q 2>/dev/null)

PLAIN_SET_1=$(extract_rps "$PLAIN_CSV_1" "SET")
PLAIN_GET_1=$(extract_rps "$PLAIN_CSV_1" "GET")
ENC_SET_1=$(extract_rps "$ENC_CSV_1" "SET")
ENC_GET_1=$(extract_rps "$ENC_CSV_1" "GET")

printf "%-25s %15s %15s %10s\n" "" "plaintext" "encrypted" "overhead"
printf "%-25s %15s %15s %10s\n" "---" "---" "---" "---"
printf "%-25s %15s %15s %10s\n" "SET (P=1)" "$(format_number "$PLAIN_SET_1")/s" "$(format_number "$ENC_SET_1")/s" "$(calc_overhead "$PLAIN_SET_1" "$ENC_SET_1")"
printf "%-25s %15s %15s %10s\n" "GET (P=1)" "$(format_number "$PLAIN_GET_1")/s" "$(format_number "$ENC_GET_1")/s" "$(calc_overhead "$PLAIN_GET_1" "$ENC_GET_1")"

echo ""
echo "=== DONE ==="
echo ""
echo "note: GET overhead should be near zero since encryption only affects"
echo "persistence writes (AOF). GET reads from in-memory keyspace."
