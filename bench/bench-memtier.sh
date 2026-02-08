#!/usr/bin/env bash
#
# benchmark comparing ember, redis, and dragonfly using memtier_benchmark.
#
# memtier_benchmark provides better latency distributions and mixed workload
# testing compared to redis-benchmark. use alongside compare-redis.sh for a
# complete picture.
#
# usage:
#   bash bench/bench-memtier.sh              # full comparison
#   bash bench/bench-memtier.sh --ember-only # only benchmark ember
#   bash bench/bench-memtier.sh --quick      # reduced test matrix
#
# environment variables:
#   EMBER_CONCURRENT_PORT   ember concurrent port         (default: 6379)
#   EMBER_SHARDED_PORT      ember sharded port            (default: 6380)
#   REDIS_PORT              redis server port             (default: 6399)
#   DRAGONFLY_PORT          dragonfly server port         (default: 6389)
#   MEMTIER_THREADS         memtier threads               (default: 4)
#   MEMTIER_CLIENTS         clients per thread            (default: 12)
#   MEMTIER_REQUESTS        requests per client           (default: 10000)
#   MEMTIER_PIPELINE        pipeline depth for P>1 tests  (default: 16)
#   EMBER_BIN               path to ember-server binary   (default: ./target/release/ember-server)
#   DRAGONFLY_BIN           path to dragonfly binary      (default: dragonfly)

set -euo pipefail

# --- configuration ---

EMBER_CONCURRENT_PORT="${EMBER_CONCURRENT_PORT:-6379}"
EMBER_SHARDED_PORT="${EMBER_SHARDED_PORT:-6380}"
REDIS_PORT="${REDIS_PORT:-6399}"
DRAGONFLY_PORT="${DRAGONFLY_PORT:-6389}"

MEMTIER_THREADS="${MEMTIER_THREADS:-4}"
MEMTIER_CLIENTS="${MEMTIER_CLIENTS:-12}"
MEMTIER_REQUESTS="${MEMTIER_REQUESTS:-10000}"
PIPELINE="${MEMTIER_PIPELINE:-16}"
EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"
DRAGONFLY_BIN="${DRAGONFLY_BIN:-dragonfly}"

TOTAL_CONNECTIONS=$((MEMTIER_THREADS * MEMTIER_CLIENTS))
TOTAL_REQUESTS=$((MEMTIER_THREADS * MEMTIER_CLIENTS * MEMTIER_REQUESTS))
KEY_MAX=1000000
RESULTS_DIR="bench/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

EMBER_ONLY=false
QUICK_MODE=false

for arg in "$@"; do
    case "$arg" in
        --ember-only) EMBER_ONLY=true ;;
        --quick) QUICK_MODE=true ;;
        *) echo "unknown flag: $arg"; exit 1 ;;
    esac
done

# --- helpers ---

EMBER_CONCURRENT_PID=""
EMBER_SHARDED_PID=""
REDIS_PID=""
DRAGONFLY_PID=""

cleanup() {
    [[ -n "$EMBER_CONCURRENT_PID" ]] && kill "$EMBER_CONCURRENT_PID" 2>/dev/null && wait "$EMBER_CONCURRENT_PID" 2>/dev/null || true
    [[ -n "$EMBER_SHARDED_PID" ]] && kill "$EMBER_SHARDED_PID" 2>/dev/null && wait "$EMBER_SHARDED_PID" 2>/dev/null || true
    [[ -n "$REDIS_PID" ]] && kill "$REDIS_PID" 2>/dev/null && wait "$REDIS_PID" 2>/dev/null || true
    [[ -n "$DRAGONFLY_PID" ]] && kill "$DRAGONFLY_PID" 2>/dev/null && wait "$DRAGONFLY_PID" 2>/dev/null || true
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

# run memtier_benchmark and output "ops_sec p99_ms" on a single line.
#
# writes to a temp file to avoid both bash variable null-byte truncation
# and pipe buffering issues observed with certain memtier builds.
run_memtier() {
    local port=$1
    local pipeline=$2
    local ratio=$3
    local data_size=$4
    local tmpfile
    tmpfile=$(mktemp)

    memtier_benchmark \
        -s 127.0.0.1 -p "$port" \
        --threads="$MEMTIER_THREADS" \
        --clients="$MEMTIER_CLIENTS" \
        --requests="$MEMTIER_REQUESTS" \
        --pipeline="$pipeline" \
        --data-size="$data_size" \
        --ratio="$ratio" \
        --key-minimum=1 \
        --key-maximum="$KEY_MAX" \
        --hide-histogram \
        > "$tmpfile" 2>/dev/null || true

    if grep -q "^Totals" "$tmpfile"; then
        awk '/^Totals/{printf "%.0f %.3f\n", $2, $7}' "$tmpfile"
    else
        echo "warning: no Totals in memtier output for port $port" >&2
        echo "0 0.000"
    fi
    rm -f "$tmpfile"
}

# pre-populate keys so GET tests hit data
populate_keys() {
    local port=$1
    memtier_benchmark \
        -s 127.0.0.1 -p "$port" \
        --threads="$MEMTIER_THREADS" \
        --clients="$MEMTIER_CLIENTS" \
        --requests=5000 \
        --ratio=1:0 \
        --data-size=64 \
        --key-minimum=1 \
        --key-maximum="$KEY_MAX" \
        --hide-histogram \
        > /dev/null 2>&1
}

format_number() {
    printf "%'d" "$1"
}

calc_ratio() {
    local a=$1
    local b=$2
    if [[ "$b" -gt 0 ]]; then
        local ratio_x10=$(( (a * 10 + b / 2) / b ))
        local ratio_int=$((ratio_x10 / 10))
        local ratio_frac=$((ratio_x10 % 10))
        echo "${ratio_int}.${ratio_frac}x"
    else
        echo "n/a"
    fi
}

# --- checks ---

if ! command -v memtier_benchmark &> /dev/null; then
    echo "error: memtier_benchmark not found." >&2
    echo "  brew install memtier_benchmark  # macOS" >&2
    echo "  see bench/setup-vm.sh           # linux" >&2
    exit 1
fi

if ! command -v redis-cli &> /dev/null; then
    echo "error: redis-cli not found (needed for server health checks)." >&2
    exit 1
fi

if [[ ! -x "$EMBER_BIN" ]]; then
    echo "building ember-server in release mode..."
    cargo build --release -p ember-server
fi

HAS_REDIS=false
HAS_DRAGONFLY=false

if [[ "$EMBER_ONLY" == "false" ]]; then
    if command -v redis-server &> /dev/null; then
        HAS_REDIS=true
    else
        echo "note: redis-server not found, skipping redis benchmarks" >&2
    fi

    if command -v "$DRAGONFLY_BIN" &> /dev/null; then
        HAS_DRAGONFLY=true
    else
        echo "note: dragonfly not found, skipping dragonfly benchmarks" >&2
    fi
fi

mkdir -p "$RESULTS_DIR"

# --- start servers ---

CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)

echo ""
echo "=== memtier benchmark configuration ==="
echo "threads:      $MEMTIER_THREADS"
echo "clients:      $MEMTIER_CLIENTS per thread ($TOTAL_CONNECTIONS total)"
echo "requests:     $MEMTIER_REQUESTS per client ($TOTAL_REQUESTS total)"
echo "pipeline:     $PIPELINE"
echo "key range:    1 - $KEY_MAX"
echo ""

echo "starting ember concurrent on port $EMBER_CONCURRENT_PORT..."
"$EMBER_BIN" --port "$EMBER_CONCURRENT_PORT" --concurrent > /dev/null 2>&1 &
EMBER_CONCURRENT_PID=$!
wait_for_server "$EMBER_CONCURRENT_PORT" "ember-concurrent"

echo "starting ember sharded ($CPU_CORES shards) on port $EMBER_SHARDED_PORT..."
"$EMBER_BIN" --port "$EMBER_SHARDED_PORT" --shards "$CPU_CORES" > /dev/null 2>&1 &
EMBER_SHARDED_PID=$!
wait_for_server "$EMBER_SHARDED_PORT" "ember-sharded"

if [[ "$HAS_REDIS" == "true" ]]; then
    echo "starting redis on port $REDIS_PORT..."
    redis-server --port "$REDIS_PORT" --save "" --appendonly no --loglevel warning > /dev/null 2>&1 &
    REDIS_PID=$!
    wait_for_server "$REDIS_PORT" "redis"
fi

if [[ "$HAS_DRAGONFLY" == "true" ]]; then
    echo "starting dragonfly on port $DRAGONFLY_PORT..."
    "$DRAGONFLY_BIN" --port "$DRAGONFLY_PORT" --dbfilename "" > /dev/null 2>&1 &
    DRAGONFLY_PID=$!
    wait_for_server "$DRAGONFLY_PORT" "dragonfly"
fi

# --- test matrix ---
# format: "label|data_size|pipeline|ratio"

if [[ "$QUICK_MODE" == "true" ]]; then
    TESTS=(
        "SET (64B, P=$PIPELINE)|64|$PIPELINE|1:0"
        "GET (64B, P=$PIPELINE)|64|$PIPELINE|0:1"
        "mixed 1:10 (64B, P=$PIPELINE)|64|$PIPELINE|1:10"
        "mixed 1:1 (64B, P=$PIPELINE)|64|$PIPELINE|1:1"
    )
else
    TESTS=(
        "SET (64B, P=$PIPELINE)|64|$PIPELINE|1:0"
        "GET (64B, P=$PIPELINE)|64|$PIPELINE|0:1"
        "mixed 1:10 (64B, P=$PIPELINE)|64|$PIPELINE|1:10"
        "mixed 1:1 (64B, P=$PIPELINE)|64|$PIPELINE|1:1"
        "SET (1KB, P=$PIPELINE)|1024|$PIPELINE|1:0"
        "GET (1KB, P=$PIPELINE)|1024|$PIPELINE|0:1"
        "SET (64B, P=1)|64|1|1:0"
        "GET (64B, P=1)|64|1|0:1"
    )
fi

# --- run benchmarks ---

echo ""
echo "pre-populating keys..."
populate_keys "$EMBER_CONCURRENT_PORT"
populate_keys "$EMBER_SHARDED_PORT"
[[ "$HAS_REDIS" == "true" ]] && populate_keys "$REDIS_PORT"
[[ "$HAS_DRAGONFLY" == "true" ]] && populate_keys "$DRAGONFLY_PORT"

echo ""
echo "running benchmarks..."
echo ""

# collect labels from the test matrix
declare -a LABELS=()
for test_spec in "${TESTS[@]}"; do
    IFS='|' read -r label _ _ _ <<< "$test_spec"
    LABELS+=("$label")
done

# run all tests for one server before moving to the next.
# this avoids CPU contention from rapidly switching between
# servers running on the same machine.
run_server_tests() {
    local port=$1
    local name=$2
    local -n ops_arr=$3
    local -n p99_arr=$4

    echo "  $name (port $port)..."
    for test_spec in "${TESTS[@]}"; do
        IFS='|' read -r label data_size pipeline ratio <<< "$test_spec"
        local result ops p99
        result=$(run_memtier "$port" "$pipeline" "$ratio" "$data_size")
        ops=${result%% *}
        p99=${result##* }
        ops_arr+=("${ops:-0}")
        p99_arr+=("${p99:-0.000}")
    done
}

declare -a EC_OPS=()
declare -a ES_OPS=()
declare -a R_OPS=()
declare -a D_OPS=()
declare -a EC_P99=()
declare -a ES_P99=()
declare -a R_P99=()
declare -a D_P99=()

run_server_tests "$EMBER_CONCURRENT_PORT" "ember concurrent" EC_OPS EC_P99
run_server_tests "$EMBER_SHARDED_PORT" "ember sharded" ES_OPS ES_P99
[[ "$HAS_REDIS" == "true" ]] && run_server_tests "$REDIS_PORT" "redis" R_OPS R_P99
[[ "$HAS_DRAGONFLY" == "true" ]] && run_server_tests "$DRAGONFLY_PORT" "dragonfly" D_OPS D_P99

# --- output results ---

DATE=$(date +%Y-%m-%d)

echo ""
echo "========================================================================"
echo "             memtier benchmark results — $DATE"
echo "========================================================================"
echo ""
echo "system: $CPU_CORES cores, $TOTAL_CONNECTIONS connections, $TOTAL_REQUESTS requests/test"
echo ""

# build format string dynamically based on available servers
fmt="%-30s %16s %16s"
header_args=("test" "ember concurrent" "ember sharded")
divider_args=("----" "----------------" "-------------")

if [[ "$HAS_REDIS" == "true" ]]; then
    fmt="$fmt %12s"
    header_args+=("redis")
    divider_args+=("-----")
fi
if [[ "$HAS_DRAGONFLY" == "true" ]]; then
    fmt="$fmt %12s"
    header_args+=("dragonfly")
    divider_args+=("---------")
fi

# throughput table
echo "=== throughput (ops/sec) ==="
echo ""

printf "$fmt\n" "${header_args[@]}"
printf "$fmt\n" "${divider_args[@]}"

for i in "${!LABELS[@]}"; do
    row_args=("${LABELS[$i]}" "$(format_number "${EC_OPS[$i]}")" "$(format_number "${ES_OPS[$i]}")")
    if [[ "$HAS_REDIS" == "true" ]]; then
        row_args+=("$(format_number "${R_OPS[$i]}")")
    fi
    if [[ "$HAS_DRAGONFLY" == "true" ]]; then
        row_args+=("$(format_number "${D_OPS[$i]}")")
    fi
    printf "$fmt\n" "${row_args[@]}"
done

echo ""
echo ""

# p99 latency table
echo "=== p99 latency (ms) ==="
echo ""

printf "$fmt\n" "${header_args[@]}"
printf "$fmt\n" "${divider_args[@]}"

for i in "${!LABELS[@]}"; do
    row_args=("${LABELS[$i]}" "${EC_P99[$i]}" "${ES_P99[$i]}")
    if [[ "$HAS_REDIS" == "true" ]]; then
        row_args+=("${R_P99[$i]}")
    fi
    if [[ "$HAS_DRAGONFLY" == "true" ]]; then
        row_args+=("${D_P99[$i]}")
    fi
    printf "$fmt\n" "${row_args[@]}"
done

echo ""
echo ""

# ratio comparisons
if [[ "$HAS_REDIS" == "true" ]]; then
    echo "=== ember vs redis ==="
    echo ""
    printf "%-30s %16s %16s\n" "test" "concurrent" "sharded"
    printf "%-30s %16s %16s\n" "----" "----------" "-------"
    for i in "${!LABELS[@]}"; do
        ratio_c=$(calc_ratio "${EC_OPS[$i]}" "${R_OPS[$i]}")
        ratio_s=$(calc_ratio "${ES_OPS[$i]}" "${R_OPS[$i]}")
        printf "%-30s %16s %16s\n" "${LABELS[$i]}" "$ratio_c" "$ratio_s"
    done
    echo ""
    echo ""
fi

if [[ "$HAS_DRAGONFLY" == "true" ]]; then
    echo "=== ember vs dragonfly ==="
    echo ""
    printf "%-30s %16s %16s\n" "test" "concurrent" "sharded"
    printf "%-30s %16s %16s\n" "----" "----------" "-------"
    for i in "${!LABELS[@]}"; do
        ratio_c=$(calc_ratio "${EC_OPS[$i]}" "${D_OPS[$i]}")
        ratio_s=$(calc_ratio "${ES_OPS[$i]}" "${D_OPS[$i]}")
        printf "%-30s %16s %16s\n" "${LABELS[$i]}" "$ratio_c" "$ratio_s"
    done
    echo ""
    echo ""
fi

# ember modes comparison
echo "=== ember concurrent vs sharded ==="
echo ""
printf "%-30s %10s\n" "test" "ratio"
printf "%-30s %10s\n" "----" "-----"
for i in "${!LABELS[@]}"; do
    ratio=$(calc_ratio "${EC_OPS[$i]}" "${ES_OPS[$i]}")
    printf "%-30s %10s\n" "${LABELS[$i]}" "$ratio"
done
echo ""

# --- save raw results ---

RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-memtier.csv"
{
    echo "test,ember_concurrent_ops,ember_sharded_ops,redis_ops,dragonfly_ops,ec_p99,es_p99,redis_p99,dragonfly_p99"
    for i in "${!LABELS[@]}"; do
        echo "${LABELS[$i]},${EC_OPS[$i]},${ES_OPS[$i]},${R_OPS[$i]:-},${D_OPS[$i]:-},${EC_P99[$i]},${ES_P99[$i]},${R_P99[$i]:-},${D_P99[$i]:-}"
    done
} > "$RESULT_FILE"

echo "raw results saved to $RESULT_FILE"
