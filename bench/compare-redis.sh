#!/usr/bin/env bash
#
# Comprehensive benchmark comparing Ember (concurrent + sharded), Redis, and Dragonfly.
#
# Usage:
#   bash bench/compare-redis.sh              # full comparison
#   bash bench/compare-redis.sh --ember-only # only benchmark ember
#   bash bench/compare-redis.sh --quick      # reduced test matrix
#   bash bench/compare-redis.sh --json       # JSON output for CI
#
# Environment variables:
#   EMBER_CONCURRENT_PORT   ember concurrent port         (default: 6379)
#   EMBER_SHARDED_PORT      ember sharded port            (default: 6380)
#   REDIS_PORT              redis server port             (default: 6399)
#   DRAGONFLY_PORT          dragonfly server port         (default: 6389)
#   BENCH_REQUESTS          requests per test             (default: 100000)
#   BENCH_CLIENTS           concurrent clients            (default: 50)
#   BENCH_PIPELINE          pipeline depth for P>1 tests  (default: 16)
#   BENCH_THREADS           redis-benchmark threads       (default: CPU cores)
#   EMBER_BIN               path to ember-server binary   (default: ./target/release/ember-server)
#   DRAGONFLY_BIN           path to dragonfly binary      (default: dragonfly)

set -euo pipefail

# --- configuration ---

EMBER_CONCURRENT_PORT="${EMBER_CONCURRENT_PORT:-6379}"
EMBER_SHARDED_PORT="${EMBER_SHARDED_PORT:-6380}"
REDIS_PORT="${REDIS_PORT:-6399}"
DRAGONFLY_PORT="${DRAGONFLY_PORT:-6389}"
REQUESTS="${BENCH_REQUESTS:-100000}"
CLIENTS="${BENCH_CLIENTS:-50}"
PIPELINE="${BENCH_PIPELINE:-16}"
EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"

# detect CPU cores early for THREADS default
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
THREADS="${BENCH_THREADS:-$CPU_CORES}"
DRAGONFLY_BIN="${DRAGONFLY_BIN:-dragonfly}"
RESULTS_DIR="bench/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

EMBER_ONLY=false
QUICK_MODE=false
JSON_OUTPUT=false

for arg in "$@"; do
    case "$arg" in
        --ember-only) EMBER_ONLY=true ;;
        --quick) QUICK_MODE=true ;;
        --json) JSON_OUTPUT=true ;;
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

# extract requests/sec from redis-benchmark --csv output
extract_rps() {
    local csv_output=$1
    local test_name=$2
    echo "$csv_output" | grep "\"$test_name\"" | cut -d',' -f2 | tr -d '"' | cut -d'.' -f1
}

run_benchmark() {
    local port=$1
    local data_size=$2
    local pipeline=$3
    redis-benchmark -p "$port" -t set,get -n "$REQUESTS" -c "$CLIENTS" -P "$pipeline" -d "$data_size" --threads "$THREADS" --csv -q 2>/dev/null
}

# pre-populate keys so GET benchmarks have data to read
populate_keys() {
    local port=$1
    redis-benchmark -p "$port" -t set -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d 3 --threads "$THREADS" -q > /dev/null 2>&1
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

if ! command -v redis-benchmark &> /dev/null; then
    echo "error: redis-benchmark not found. install redis tools first." >&2
    echo "  brew install redis       # macOS" >&2
    echo "  apt install redis-tools  # debian/ubuntu" >&2
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
        echo "  install from: https://github.com/dragonflydb/dragonfly" >&2
    fi
fi

mkdir -p "$RESULTS_DIR"

# --- start servers ---

echo ""
echo "=== benchmark configuration ==="
echo "cpu cores:    $CPU_CORES"
echo "requests:     $REQUESTS"
echo "clients:      $CLIENTS"
echo "pipeline:     $PIPELINE"
echo "threads:      $THREADS"
echo ""

# ember concurrent mode (DashMap-backed, fastest for GET/SET)
# --no-grpc avoids gRPC port conflict when running multiple instances
echo "starting ember concurrent on port $EMBER_CONCURRENT_PORT..."
"$EMBER_BIN" --port "$EMBER_CONCURRENT_PORT" --concurrent --no-grpc > /dev/null 2>&1 &
EMBER_CONCURRENT_PID=$!
wait_for_server "$EMBER_CONCURRENT_PORT" "ember-concurrent"

# ember sharded mode (channel-based, supports all data types)
echo "starting ember sharded ($CPU_CORES shards) on port $EMBER_SHARDED_PORT..."
"$EMBER_BIN" --port "$EMBER_SHARDED_PORT" --no-grpc > /dev/null 2>&1 &
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

# --- define test matrix ---

if [[ "$QUICK_MODE" == "true" ]]; then
    TESTS=(
        "SET (64B, P=$PIPELINE):64:$PIPELINE"
        "GET (64B, P=$PIPELINE):64:$PIPELINE"
        "SET (64B, P=1):64:1"
        "GET (64B, P=1):64:1"
    )
else
    TESTS=(
        "SET (3B, P=$PIPELINE):3:$PIPELINE"
        "GET (3B, P=$PIPELINE):3:$PIPELINE"
        "SET (64B, P=$PIPELINE):64:$PIPELINE"
        "GET (64B, P=$PIPELINE):64:$PIPELINE"
        "SET (1KB, P=$PIPELINE):1024:$PIPELINE"
        "GET (1KB, P=$PIPELINE):1024:$PIPELINE"
        "SET (64B, P=1):64:1"
        "GET (64B, P=1):64:1"
    )
fi

# --- run benchmarks ---

echo ""
echo "running benchmarks..."
echo ""

# pre-populate all servers
populate_keys "$EMBER_CONCURRENT_PORT"
populate_keys "$EMBER_SHARDED_PORT"
[[ "$HAS_REDIS" == "true" ]] && populate_keys "$REDIS_PORT"
[[ "$HAS_DRAGONFLY" == "true" ]] && populate_keys "$DRAGONFLY_PORT"

declare -a LABELS=()
declare -a EMBER_CONCURRENT_RESULTS=()
declare -a EMBER_SHARDED_RESULTS=()
declare -a REDIS_RESULTS=()
declare -a DRAGONFLY_RESULTS=()

for test_spec in "${TESTS[@]}"; do
    IFS=':' read -r label data_size pipeline <<< "$test_spec"
    LABELS+=("$label")

    if [[ "$label" == SET* ]]; then
        test_type="SET"
    else
        test_type="GET"
    fi

    # ember concurrent
    csv=$(run_benchmark "$EMBER_CONCURRENT_PORT" "$data_size" "$pipeline")
    rps=$(extract_rps "$csv" "$test_type")
    EMBER_CONCURRENT_RESULTS+=("${rps:-0}")

    # ember sharded
    csv=$(run_benchmark "$EMBER_SHARDED_PORT" "$data_size" "$pipeline")
    rps=$(extract_rps "$csv" "$test_type")
    EMBER_SHARDED_RESULTS+=("${rps:-0}")

    # redis
    if [[ "$HAS_REDIS" == "true" ]]; then
        csv=$(run_benchmark "$REDIS_PORT" "$data_size" "$pipeline")
        rps=$(extract_rps "$csv" "$test_type")
        REDIS_RESULTS+=("${rps:-0}")
    fi

    # dragonfly
    if [[ "$HAS_DRAGONFLY" == "true" ]]; then
        csv=$(run_benchmark "$DRAGONFLY_PORT" "$data_size" "$pipeline")
        rps=$(extract_rps "$csv" "$test_type")
        DRAGONFLY_RESULTS+=("${rps:-0}")
    fi
done

# --- output results ---

if [[ "$JSON_OUTPUT" == "true" ]]; then
    echo "{"
    echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
    echo "  \"config\": {"
    echo "    \"cpu_cores\": $CPU_CORES,"
    echo "    \"requests\": $REQUESTS,"
    echo "    \"clients\": $CLIENTS,"
    echo "    \"pipeline\": $PIPELINE"
    echo "  },"
    echo "  \"ember_concurrent\": {"
    for i in "${!LABELS[@]}"; do
        comma=$([[ $i -lt $((${#LABELS[@]} - 1)) ]] && echo "," || echo "")
        echo "    \"${LABELS[$i]}\": ${EMBER_CONCURRENT_RESULTS[$i]}$comma"
    done
    echo "  },"
    echo "  \"ember_sharded\": {"
    for i in "${!LABELS[@]}"; do
        comma=$([[ $i -lt $((${#LABELS[@]} - 1)) ]] && echo "," || echo "")
        echo "    \"${LABELS[$i]}\": ${EMBER_SHARDED_RESULTS[$i]}$comma"
    done
    echo "  }"
    if [[ "$HAS_REDIS" == "true" ]]; then
        echo "  ,\"redis\": {"
        for i in "${!LABELS[@]}"; do
            comma=$([[ $i -lt $((${#LABELS[@]} - 1)) ]] && echo "," || echo "")
            echo "    \"${LABELS[$i]}\": ${REDIS_RESULTS[$i]}$comma"
        done
        echo "  }"
    fi
    if [[ "$HAS_DRAGONFLY" == "true" ]]; then
        echo "  ,\"dragonfly\": {"
        for i in "${!LABELS[@]}"; do
            comma=$([[ $i -lt $((${#LABELS[@]} - 1)) ]] && echo "," || echo "")
            echo "    \"${LABELS[$i]}\": ${DRAGONFLY_RESULTS[$i]}$comma"
        done
        echo "  }"
    fi
    echo "}"
else
    DATE=$(date +%Y-%m-%d)

    echo "========================================================================"
    echo "                    ember benchmark results — $DATE"
    echo "========================================================================"
    echo ""
    echo "system: $CPU_CORES cores, $REQUESTS requests, $CLIENTS clients"
    echo ""

    # --- all servers comparison ---
    echo "=== throughput comparison (requests/sec) ==="
    echo ""

    # build header
    header="%-22s %16s %16s"
    header_args=("test" "ember concurrent" "ember sharded")
    divider="%-22s %16s %16s"
    divider_args=("----" "----------------" "-------------")

    if [[ "$HAS_REDIS" == "true" ]]; then
        header="$header %12s"
        header_args+=("redis")
        divider="$divider %12s"
        divider_args+=("-----")
    fi
    if [[ "$HAS_DRAGONFLY" == "true" ]]; then
        header="$header %12s"
        header_args+=("dragonfly")
        divider="$divider %12s"
        divider_args+=("---------")
    fi

    printf "$header\n" "${header_args[@]}"
    printf "$divider\n" "${divider_args[@]}"

    for i in "${!LABELS[@]}"; do
        ec=${EMBER_CONCURRENT_RESULTS[$i]}
        es=${EMBER_SHARDED_RESULTS[$i]}

        row="%-22s %16s %16s"
        row_args=("${LABELS[$i]}" "$(format_number "$ec")" "$(format_number "$es")")

        if [[ "$HAS_REDIS" == "true" ]]; then
            row="$row %12s"
            row_args+=("$(format_number "${REDIS_RESULTS[$i]}")")
        fi
        if [[ "$HAS_DRAGONFLY" == "true" ]]; then
            row="$row %12s"
            row_args+=("$(format_number "${DRAGONFLY_RESULTS[$i]}")")
        fi

        printf "$row\n" "${row_args[@]}"
    done

    echo ""
    echo ""

    # --- vs redis ---
    if [[ "$HAS_REDIS" == "true" ]]; then
        echo "=== ember vs redis ==="
        echo ""
        printf "%-22s %16s %16s\n" "test" "concurrent" "sharded"
        printf "%-22s %16s %16s\n" "----" "----------" "-------"
        for i in "${!LABELS[@]}"; do
            ec=${EMBER_CONCURRENT_RESULTS[$i]}
            es=${EMBER_SHARDED_RESULTS[$i]}
            r=${REDIS_RESULTS[$i]}
            ratio_c=$(calc_ratio "$ec" "$r")
            ratio_s=$(calc_ratio "$es" "$r")
            printf "%-22s %16s %16s\n" "${LABELS[$i]}" "$ratio_c" "$ratio_s"
        done
        echo ""
        echo ""
    fi

    # --- vs dragonfly ---
    if [[ "$HAS_DRAGONFLY" == "true" ]]; then
        echo "=== ember vs dragonfly ==="
        echo ""
        printf "%-22s %16s %16s\n" "test" "concurrent" "sharded"
        printf "%-22s %16s %16s\n" "----" "----------" "-------"
        for i in "${!LABELS[@]}"; do
            ec=${EMBER_CONCURRENT_RESULTS[$i]}
            es=${EMBER_SHARDED_RESULTS[$i]}
            d=${DRAGONFLY_RESULTS[$i]}
            ratio_c=$(calc_ratio "$ec" "$d")
            ratio_s=$(calc_ratio "$es" "$d")
            printf "%-22s %16s %16s\n" "${LABELS[$i]}" "$ratio_c" "$ratio_s"
        done
        echo ""
        echo ""
    fi

    # --- ember modes comparison ---
    echo "=== ember concurrent vs sharded ==="
    echo ""
    printf "%-22s %10s\n" "test" "ratio"
    printf "%-22s %10s\n" "----" "-----"
    for i in "${!LABELS[@]}"; do
        ec=${EMBER_CONCURRENT_RESULTS[$i]}
        es=${EMBER_SHARDED_RESULTS[$i]}
        ratio=$(calc_ratio "$ec" "$es")
        printf "%-22s %10s\n" "${LABELS[$i]}" "$ratio"
    done
    echo ""
    echo "(concurrent mode uses DashMap, sharded uses channel routing)"
fi

# --- save raw results ---

RESULT_FILE="$RESULTS_DIR/$TIMESTAMP.csv"
{
    echo "test,ember_concurrent_rps,ember_sharded_rps,redis_rps,dragonfly_rps"
    for i in "${!LABELS[@]}"; do
        redis_val="${REDIS_RESULTS[$i]:-}"
        dragonfly_val="${DRAGONFLY_RESULTS[$i]:-}"
        echo "${LABELS[$i]},${EMBER_CONCURRENT_RESULTS[$i]},${EMBER_SHARDED_RESULTS[$i]},${redis_val},${dragonfly_val}"
    done
} > "$RESULT_FILE"

echo ""
echo "raw results saved to $RESULT_FILE"
