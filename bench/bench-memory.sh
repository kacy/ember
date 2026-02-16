#!/bin/bash
#
# memory usage comparison between ember and redis across data types.
#
# tests both ember modes (concurrent for strings, sharded for all types)
# and measures per-key/per-member memory overhead.
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

get_redis_memory() {
    local port=$1
    redis-cli -p "$port" info memory 2>/dev/null | grep "^used_memory:" | cut -d: -f2 | tr -d '\r'
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

# python helper for bulk loading hash and sorted set data.
# redis-cli --pipe doesn't work reliably with ember, so we use
# redis-py pipelining instead.
BULK_LOADER=$(mktemp /tmp/bench_bulk_load_XXXXXX.py)
cat > "$BULK_LOADER" << 'PYEOF'
import redis
import sys

port = int(sys.argv[1])
data_type = sys.argv[2]
count = int(sys.argv[3])

r = redis.Redis(host="127.0.0.1", port=port)
pipe = r.pipeline(transaction=False)
batch = 1000

for i in range(count):
    if data_type == "hash":
        pipe.hset(f"hash:{i}", mapping={
            "f1": "value001", "f2": "value002", "f3": "value003",
            "f4": "value004", "f5": "value005",
        })
    elif data_type == "zset":
        pipe.zadd("myzset", {f"member:{i}": float(i % 100000)})

    if (i + 1) % batch == 0:
        pipe.execute()
        pipe = r.pipeline(transaction=False)

pipe.execute()
r.close()
PYEOF

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

# --- measure memory for one data type on one server ---

measure_type() {
    local port=$1
    local type=$2
    local count=$3

    local mem_before
    mem_before=$(get_redis_memory "$port")

    case "$type" in
        string)
            redis-benchmark -p "$port" -t set -n "$count" -r "$count" -d "$VALUE_SIZE" -q > /dev/null 2>&1
            ;;
        hash|zset)
            python3 "$BULK_LOADER" "$port" "$type" "$count"
            ;;
    esac

    sleep 1

    local mem_after
    mem_after=$(get_redis_memory "$port")

    # for sorted sets, all members go into one key so DBSIZE=1.
    # divide by member count instead.
    local divisor
    if [[ "$type" == "zset" ]]; then
        divisor=$count
    else
        divisor=$(redis-cli -p "$port" DBSIZE 2>/dev/null | awk '{print $NF}' | tr -d '\r')
    fi

    local used=$((mem_after - mem_before))
    local bytes_per_key=0
    if [[ "${divisor:-0}" -gt 0 ]] 2>/dev/null; then
        bytes_per_key=$((used / divisor))
    fi

    echo "$bytes_per_key"
}

# --- string benchmark (concurrent + sharded + redis) ---

echo "--- string (${VALUE_SIZE}B) ($STRING_KEYS keys) ---"
echo ""
TYPE_LABELS+=("string (${VALUE_SIZE}B)")

# ember concurrent
echo "  starting ember concurrent..."
$EMBER_BIN --port $EMBER_CONCURRENT_PORT --concurrent --no-grpc > /dev/null 2>&1 &
EC_PID=$!
wait_for_server $EMBER_CONCURRENT_PORT

ec_bytes=$(measure_type "$EMBER_CONCURRENT_PORT" "string" "$STRING_KEYS")
EMBER_CONCURRENT_BYTES+=("$ec_bytes")
echo "  ember concurrent: ${ec_bytes} B/key"

kill "$EC_PID" 2>/dev/null && wait "$EC_PID" 2>/dev/null || true
sleep 0.5

# ember sharded
echo "  starting ember sharded..."
$EMBER_BIN --port $EMBER_SHARDED_PORT --no-grpc > /dev/null 2>&1 &
ES_PID=$!
wait_for_server $EMBER_SHARDED_PORT

es_bytes=$(measure_type "$EMBER_SHARDED_PORT" "string" "$STRING_KEYS")
EMBER_SHARDED_BYTES+=("$es_bytes")
echo "  ember sharded:    ${es_bytes} B/key"

kill "$ES_PID" 2>/dev/null && wait "$ES_PID" 2>/dev/null || true
sleep 0.5

# redis
if [[ "$HAS_REDIS" == "true" ]]; then
    echo "  starting redis..."
    redis-server --port $REDIS_PORT --save "" --appendonly no --loglevel warning > /dev/null 2>&1 &
    sleep 1
    wait_for_server $REDIS_PORT

    r_bytes=$(measure_type "$REDIS_PORT" "string" "$STRING_KEYS")
    REDIS_BYTES+=("$r_bytes")
    echo "  redis:            ${r_bytes} B/key"

    redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
    sleep 0.5
else
    REDIS_BYTES+=("—")
fi
echo ""

# --- hash benchmark (sharded only + redis) ---
# hash/zset commands are only supported in sharded mode

echo "--- hash (5 fields) ($COMPLEX_KEYS keys) ---"
echo ""
TYPE_LABELS+=("hash (5 fields)")
EMBER_CONCURRENT_BYTES+=("—")

echo "  starting ember sharded..."
$EMBER_BIN --port $EMBER_SHARDED_PORT --no-grpc > /dev/null 2>&1 &
ES_PID=$!
wait_for_server $EMBER_SHARDED_PORT

es_bytes=$(measure_type "$EMBER_SHARDED_PORT" "hash" "$COMPLEX_KEYS")
EMBER_SHARDED_BYTES+=("$es_bytes")
echo "  ember sharded:    ${es_bytes} B/key"

kill "$ES_PID" 2>/dev/null && wait "$ES_PID" 2>/dev/null || true
sleep 0.5

if [[ "$HAS_REDIS" == "true" ]]; then
    echo "  starting redis..."
    redis-server --port $REDIS_PORT --save "" --appendonly no --loglevel warning > /dev/null 2>&1 &
    sleep 1
    wait_for_server $REDIS_PORT

    r_bytes=$(measure_type "$REDIS_PORT" "hash" "$COMPLEX_KEYS")
    REDIS_BYTES+=("$r_bytes")
    echo "  redis:            ${r_bytes} B/key"

    redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
    sleep 0.5
else
    REDIS_BYTES+=("—")
fi
echo ""

# --- sorted set benchmark (sharded only + redis) ---

echo "--- sorted set ($COMPLEX_KEYS members) ---"
echo ""
TYPE_LABELS+=("sorted set")
EMBER_CONCURRENT_BYTES+=("—")

echo "  starting ember sharded..."
$EMBER_BIN --port $EMBER_SHARDED_PORT --no-grpc > /dev/null 2>&1 &
ES_PID=$!
wait_for_server $EMBER_SHARDED_PORT

es_bytes=$(measure_type "$EMBER_SHARDED_PORT" "zset" "$COMPLEX_KEYS")
EMBER_SHARDED_BYTES+=("$es_bytes")
echo "  ember sharded:    ${es_bytes} B/member"

kill "$ES_PID" 2>/dev/null && wait "$ES_PID" 2>/dev/null || true
sleep 0.5

if [[ "$HAS_REDIS" == "true" ]]; then
    echo "  starting redis..."
    redis-server --port $REDIS_PORT --save "" --appendonly no --loglevel warning > /dev/null 2>&1 &
    sleep 1
    wait_for_server $REDIS_PORT

    r_bytes=$(measure_type "$REDIS_PORT" "zset" "$COMPLEX_KEYS")
    REDIS_BYTES+=("$r_bytes")
    echo "  redis:            ${r_bytes} B/member"

    redis-cli -p $REDIS_PORT shutdown nosave 2>/dev/null || true
    sleep 0.5
else
    REDIS_BYTES+=("—")
fi
echo ""

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

batch_size = 500
for start in range(0, count, batch_size):
    end = min(start + batch_size, count)
    args = ["vectors", "DIM", str(dim)]
    for i in range(start, end):
        vec = [random.gauss(0, 1) for _ in range(dim)]
        norm = sum(v * v for v in vec) ** 0.5
        vec = [v / norm for v in vec]
        args.append(f"v{i}")
        args.extend(str(v) for v in vec)
    if start == 0:
        args += ["METRIC", "COSINE"]
    r.execute_command("VADD_BATCH", *args)

    if end % 10000 == 0 or end == count:
        print(f"    inserted {end}/{count} vectors", file=sys.stderr)
PYEOF

        # vector requires sharded mode
        echo "  starting ember sharded..."
        $EMBER_BIN --port $EMBER_SHARDED_PORT --no-grpc > /dev/null 2>&1 &
        VEC_PID=$!
        wait_for_server $EMBER_SHARDED_PORT

        VEC_MEM_BEFORE=$(get_redis_memory "$EMBER_SHARDED_PORT")
        python3 "$VECTOR_HELPER" "$EMBER_SHARDED_PORT" "$VECTOR_KEYS" "$VECTOR_DIM"
        sleep 1
        VEC_MEM_AFTER=$(get_redis_memory "$EMBER_SHARDED_PORT")

        VEC_USED=$((VEC_MEM_AFTER - VEC_MEM_BEFORE))
        VEC_BYTES=$((VEC_USED / VECTOR_KEYS))

        EMBER_CONCURRENT_BYTES+=("—")
        EMBER_SHARDED_BYTES+=("$VEC_BYTES")
        REDIS_BYTES+=("—")
        echo "  ember sharded:    ${VEC_BYTES} B/vector"

        kill "$VEC_PID" 2>/dev/null && wait "$VEC_PID" 2>/dev/null || true
        rm -f "$VECTOR_HELPER"
    fi

    echo ""
fi

# --- cleanup temp files ---
rm -f "$BULK_LOADER"

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
