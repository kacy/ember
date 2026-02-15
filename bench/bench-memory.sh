#!/bin/bash
#
# memory usage comparison between ember and redis across data types.
#
# tests both ember modes (concurrent + sharded) and measures per-key
# memory overhead for strings, hashes, sorted sets, and optionally vectors.
#
# usage:
#   ./bench/bench-memory.sh              # string, hash, sorted set (ember + redis)
#   ./bench/bench-memory.sh --vector     # include vector memory test (ember only)
#
# environment variables:
#   STRING_KEYS    number of string keys       (default: 1000000)
#   COMPLEX_KEYS   number of hash/zset keys    (default: 100000)
#   VECTOR_KEYS    number of vectors            (default: 100000)
#   VALUE_SIZE     string value size in bytes   (default: 64)
#   VECTOR_DIM     vector dimensions            (default: 128)

set -e

STRING_KEYS=${STRING_KEYS:-1000000}
COMPLEX_KEYS=${COMPLEX_KEYS:-100000}
VECTOR_KEYS=${VECTOR_KEYS:-100000}
VALUE_SIZE=${VALUE_SIZE:-64}
VECTOR_DIM=${VECTOR_DIM:-128}

EMBER_CONCURRENT_PORT=6379
EMBER_SHARDED_PORT=6380
REDIS_PORT=6399

VECTOR_MODE=false
for arg in "$@"; do
    case "$arg" in
        --vector) VECTOR_MODE=true ;;
        *) echo "unknown flag: $arg"; exit 1 ;;
    esac
done

# --- helpers ---

cleanup() {
    pkill -f "ember-server.*${EMBER_CONCURRENT_PORT}" 2>/dev/null || true
    pkill -f "ember-server.*${EMBER_SHARDED_PORT}" 2>/dev/null || true
    redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
}

trap cleanup EXIT

EMBER_BIN="./target/release/ember-server"
if [[ ! -x "$EMBER_BIN" ]]; then
    echo "error: ember-server not found at $EMBER_BIN"
    echo "run: cargo build --release"
    exit 1
fi

if ! command -v redis-benchmark &> /dev/null; then
    echo "error: redis-benchmark not found"
    exit 1
fi

HAS_REDIS=false
if command -v redis-server &> /dev/null; then
    HAS_REDIS=true
fi

get_rss_kb() {
    local pid=$1
    if [[ "$(uname)" == "Darwin" ]]; then
        ps -o rss= -p "$pid" 2>/dev/null | tr -d ' '
    else
        awk '/VmRSS/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0
    fi
}

get_redis_memory() {
    local port=$1
    redis-cli -p "$port" info memory 2>/dev/null | grep used_memory: | cut -d: -f2 | tr -d '\r'
}

wait_for_server() {
    local port=$1
    local retries=50
    while ! redis-cli -p "$port" ping > /dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            echo "error: server did not start on port $port" >&2
            exit 1
        fi
        sleep 0.1
    done
}

cleanup

echo "============================================="
echo "MEMORY BENCHMARK"
echo "============================================="
echo "string keys:  $STRING_KEYS (${VALUE_SIZE}B values)"
echo "hash keys:    $COMPLEX_KEYS (5 fields each)"
echo "sorted sets:  $COMPLEX_KEYS members"
if [[ "$VECTOR_MODE" == "true" ]]; then
    echo "vectors:      $VECTOR_KEYS (${VECTOR_DIM}-dim, ember only)"
fi
echo "============================================="
echo ""

# --- results arrays ---

declare -a TYPE_LABELS=()
declare -a EMBER_CONCURRENT_BYTES=()
declare -a EMBER_SHARDED_BYTES=()
declare -a REDIS_BYTES=()

# --- measure ember memory for one data type ---

measure_ember_type() {
    local pid=$1
    local port=$2
    local type=$3
    local count=$4

    local rss_before
    rss_before=$(get_rss_kb "$pid")

    case "$type" in
        string)
            redis-benchmark -p "$port" -t set -n "$count" -r "$count" -d "$VALUE_SIZE" -q > /dev/null 2>&1
            ;;
        hash)
            {
                for i in $(seq 1 "$count"); do
                    echo "HSET hash:$i f1 value001 f2 value002 f3 value003 f4 value004 f5 value005"
                done
            } | redis-cli -p "$port" --pipe > /dev/null 2>&1
            ;;
        zset)
            {
                for i in $(seq 1 "$count"); do
                    echo "ZADD myzset $((RANDOM % 100000)) member:$i"
                done
            } | redis-cli -p "$port" --pipe > /dev/null 2>&1
            ;;
    esac

    sleep 1

    local rss_after
    rss_after=$(get_rss_kb "$pid")
    local keys
    keys=$(redis-cli -p "$port" DBSIZE 2>/dev/null | awk '{print $2}' | tr -d '\r')

    local used_kb=$((rss_after - rss_before))
    local bytes_per_key=0
    if [[ "${keys:-0}" -gt 0 ]] 2>/dev/null; then
        bytes_per_key=$((used_kb * 1024 / keys))
    fi

    echo "$bytes_per_key"
}

# --- measure redis memory for one data type ---

measure_redis_type() {
    local port=$1
    local type=$2
    local count=$3

    local mem_before
    mem_before=$(get_redis_memory "$port")

    case "$type" in
        string)
            redis-benchmark -p "$port" -t set -n "$count" -r "$count" -d "$VALUE_SIZE" -q > /dev/null 2>&1
            ;;
        hash)
            {
                for i in $(seq 1 "$count"); do
                    echo "HSET hash:$i f1 value001 f2 value002 f3 value003 f4 value004 f5 value005"
                done
            } | redis-cli -p "$port" --pipe > /dev/null 2>&1
            ;;
        zset)
            {
                for i in $(seq 1 "$count"); do
                    echo "ZADD myzset $((RANDOM % 100000)) member:$i"
                done
            } | redis-cli -p "$port" --pipe > /dev/null 2>&1
            ;;
    esac

    sleep 1

    local mem_after
    mem_after=$(get_redis_memory "$port")
    local keys
    keys=$(redis-cli -p "$port" DBSIZE 2>/dev/null | awk '{print $2}' | tr -d '\r')

    local used=$((mem_after - mem_before))
    local bytes_per_key=0
    if [[ "${keys:-0}" -gt 0 ]] 2>/dev/null; then
        bytes_per_key=$((used / keys))
    fi

    echo "$bytes_per_key"
}

# --- run benchmarks for one type across all servers ---

run_type_benchmark() {
    local label=$1
    local type=$2
    local count=$3
    local skip_redis=${4:-false}

    echo "--- $label ($count keys) ---"
    echo ""

    TYPE_LABELS+=("$label")

    # ember concurrent
    echo "  starting ember concurrent..."
    $EMBER_BIN --port $EMBER_CONCURRENT_PORT --concurrent > /dev/null 2>&1 &
    local ec_pid=$!
    wait_for_server $EMBER_CONCURRENT_PORT

    local ec_bytes
    ec_bytes=$(measure_ember_type "$ec_pid" "$EMBER_CONCURRENT_PORT" "$type" "$count")
    EMBER_CONCURRENT_BYTES+=("$ec_bytes")
    echo "  ember concurrent: ${ec_bytes} bytes/key"

    kill "$ec_pid" 2>/dev/null && wait "$ec_pid" 2>/dev/null || true
    sleep 0.5

    # ember sharded
    echo "  starting ember sharded..."
    $EMBER_BIN --port $EMBER_SHARDED_PORT > /dev/null 2>&1 &
    local es_pid=$!
    wait_for_server $EMBER_SHARDED_PORT

    local es_bytes
    es_bytes=$(measure_ember_type "$es_pid" "$EMBER_SHARDED_PORT" "$type" "$count")
    EMBER_SHARDED_BYTES+=("$es_bytes")
    echo "  ember sharded:    ${es_bytes} bytes/key"

    kill "$es_pid" 2>/dev/null && wait "$es_pid" 2>/dev/null || true
    sleep 0.5

    # redis
    if [[ "$HAS_REDIS" == "true" ]] && [[ "$skip_redis" == "false" ]]; then
        echo "  starting redis..."
        redis-server --port $REDIS_PORT --save "" --appendonly no --loglevel warning > /dev/null 2>&1 &
        sleep 1
        wait_for_server $REDIS_PORT

        local r_bytes
        r_bytes=$(measure_redis_type "$REDIS_PORT" "$type" "$count")
        REDIS_BYTES+=("$r_bytes")
        echo "  redis:            ${r_bytes} bytes/key"

        redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
        sleep 0.5
    else
        REDIS_BYTES+=("—")
    fi

    echo ""
}

# --- run each data type ---

run_type_benchmark "string (${VALUE_SIZE}B)" "string" "$STRING_KEYS"
run_type_benchmark "hash (5 fields)" "hash" "$COMPLEX_KEYS"
run_type_benchmark "sorted set" "zset" "$COMPLEX_KEYS"

# --- vector (ember only, requires python3) ---

if [[ "$VECTOR_MODE" == "true" ]]; then
    echo "--- vector (${VECTOR_DIM}-dim, $VECTOR_KEYS vectors, ember only) ---"
    echo ""

    TYPE_LABELS+=("vector (${VECTOR_DIM}-dim)")

    if ! command -v python3 &> /dev/null; then
        echo "  error: python3 required for vector benchmark"
        EMBER_CONCURRENT_BYTES+=("—")
        EMBER_SHARDED_BYTES+=("—")
        REDIS_BYTES+=("—")
    else
        VECTOR_HELPER=$(mktemp /tmp/bench_vector_mem_XXXXXX.py)
        cat > "$VECTOR_HELPER" << 'PYEOF'
import redis
import random
import sys

port = int(sys.argv[1])
count = int(sys.argv[2])
dim = int(sys.argv[3])

r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)

for i in range(count):
    vec = [random.gauss(0, 1) for _ in range(dim)]
    norm = sum(v * v for v in vec) ** 0.5
    vec = [v / norm for v in vec]

    args = ["VADD", "vectors", f"v{i}"] + [str(v) for v in vec]
    if i == 0:
        args += ["METRIC", "COSINE"]
    r.execute_command(*args)

    if (i + 1) % 10000 == 0:
        print(f"    inserted {i + 1}/{count} vectors", file=sys.stderr)
PYEOF

        # vector requires sharded mode (full type support)
        echo "  starting ember sharded..."
        $EMBER_BIN --port $EMBER_SHARDED_PORT > /dev/null 2>&1 &
        VEC_PID=$!
        wait_for_server $EMBER_SHARDED_PORT

        VEC_RSS_BEFORE=$(get_rss_kb "$VEC_PID")
        python3 "$VECTOR_HELPER" "$EMBER_SHARDED_PORT" "$VECTOR_KEYS" "$VECTOR_DIM"
        sleep 1
        VEC_RSS_AFTER=$(get_rss_kb "$VEC_PID")

        VEC_USED_KB=$((VEC_RSS_AFTER - VEC_RSS_BEFORE))
        VEC_BYTES=$((VEC_USED_KB * 1024 / VECTOR_KEYS))

        EMBER_CONCURRENT_BYTES+=("—")
        EMBER_SHARDED_BYTES+=("$VEC_BYTES")
        REDIS_BYTES+=("—")
        echo "  ember sharded:    ${VEC_BYTES} bytes/vector"

        kill "$VEC_PID" 2>/dev/null && wait "$VEC_PID" 2>/dev/null || true
        rm -f "$VECTOR_HELPER"
    fi

    echo ""
fi

# --- summary table ---

echo "========================================================================"
echo "                     memory benchmark summary"
echo "========================================================================"
echo ""

fmt="%-22s %18s %18s %12s"
printf "$fmt\n" "data type" "ember concurrent" "ember sharded" "redis"
printf "$fmt\n" "---------" "----------------" "-------------" "-----"

for i in "${!TYPE_LABELS[@]}"; do
    ec="${EMBER_CONCURRENT_BYTES[$i]}"
    es="${EMBER_SHARDED_BYTES[$i]}"
    r="${REDIS_BYTES[$i]}"

    [[ "$ec" != "—" ]] && ec="${ec}B/key"
    [[ "$es" != "—" ]] && es="${es}B/key"
    [[ "$r" != "—" ]] && r="${r}B/key"

    printf "$fmt\n" "${TYPE_LABELS[$i]}" "$ec" "$es" "$r"
done

echo ""

# --- save CSV ---

RESULTS_DIR="bench/results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-memory.csv"

{
    echo "data_type,ember_concurrent_bytes_per_key,ember_sharded_bytes_per_key,redis_bytes_per_key"
    for i in "${!TYPE_LABELS[@]}"; do
        echo "${TYPE_LABELS[$i]},${EMBER_CONCURRENT_BYTES[$i]},${EMBER_SHARDED_BYTES[$i]},${REDIS_BYTES[$i]}"
    done
} > "$RESULT_FILE"

echo "raw results saved to $RESULT_FILE"
echo ""
echo "done."
