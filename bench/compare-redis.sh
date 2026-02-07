#!/usr/bin/env bash
#
# Runs redis-benchmark against both Ember and Redis (optionally) and
# prints a side-by-side throughput comparison. Saves raw results to
# bench/results/ for historical tracking.
#
# Usage:
#   bash bench/compare-redis.sh              # full comparison
#   bash bench/compare-redis.sh --ember-only # skip redis
#   bash bench/compare-redis.sh --json       # JSON output for CI
#
# Environment variables:
#   EMBER_PORT       ember server port           (default: 6379)
#   REDIS_PORT       redis server port           (default: 6399)
#   BENCH_REQUESTS   requests per test           (default: 100000)
#   BENCH_CLIENTS    concurrent clients          (default: 50)
#   BENCH_PIPELINE   pipeline depth for P>1 tests (default: 16)
#   EMBER_BIN        path to ember-server binary (default: ./target/release/ember-server)

set -euo pipefail

# --- configuration ---

EMBER_PORT="${EMBER_PORT:-6379}"
REDIS_PORT="${REDIS_PORT:-6399}"
REQUESTS="${BENCH_REQUESTS:-100000}"
CLIENTS="${BENCH_CLIENTS:-50}"
PIPELINE="${BENCH_PIPELINE:-16}"
EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"
RESULTS_DIR="bench/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

EMBER_ONLY=false
JSON_OUTPUT=false

for arg in "$@"; do
    case "$arg" in
        --ember-only) EMBER_ONLY=true ;;
        --json) JSON_OUTPUT=true ;;
        *) echo "unknown flag: $arg"; exit 1 ;;
    esac
done

# --- helpers ---

EMBER_PID=""
REDIS_PID=""

cleanup() {
    [[ -n "$EMBER_PID" ]] && kill "$EMBER_PID" 2>/dev/null && wait "$EMBER_PID" 2>/dev/null || true
    [[ -n "$REDIS_PID" ]] && kill "$REDIS_PID" 2>/dev/null && wait "$REDIS_PID" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_server() {
    local port=$1
    local name=$2
    local retries=30
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
# format: "TEST","rps"\n e.g. "SET","123456.78"
extract_rps() {
    local csv_output=$1
    local test_name=$2
    echo "$csv_output" | grep "\"$test_name\"" | cut -d',' -f2 | tr -d '"' | cut -d'.' -f1
}

run_benchmark() {
    local port=$1
    local data_size=$2
    local pipeline=$3
    redis-benchmark -p "$port" -t set,get -n "$REQUESTS" -c "$CLIENTS" -P "$pipeline" -d "$data_size" --csv -q 2>/dev/null
}

# pre-populate keys so GET benchmarks have data to read
populate_keys() {
    local port=$1
    redis-benchmark -p "$port" -t set -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" -d 3 -q > /dev/null 2>&1
}

# --- checks ---

if ! command -v redis-benchmark &> /dev/null; then
    echo "error: redis-benchmark not found. install redis tools first." >&2
    echo "  brew install redis    # macOS" >&2
    echo "  apt install redis-tools  # debian/ubuntu" >&2
    exit 1
fi

if [[ ! -x "$EMBER_BIN" ]]; then
    echo "building ember-server in release mode..."
    cargo build --release -p ember-server
fi

if [[ "$EMBER_ONLY" == "false" ]] && ! command -v redis-server &> /dev/null; then
    echo "warning: redis-server not found, running ember-only mode" >&2
    EMBER_ONLY=true
fi

mkdir -p "$RESULTS_DIR"

# --- start servers ---

echo "starting ember on port $EMBER_PORT..."
"$EMBER_BIN" --port "$EMBER_PORT" &
EMBER_PID=$!
wait_for_server "$EMBER_PORT" "ember"

if [[ "$EMBER_ONLY" == "false" ]]; then
    echo "starting redis on port $REDIS_PORT..."
    redis-server --port "$REDIS_PORT" --save "" --appendonly no --loglevel warning &
    REDIS_PID=$!
    wait_for_server "$REDIS_PORT" "redis"
fi

# --- define test matrix ---
# format: "label:data_size:pipeline"
TESTS=(
    "SET (3B, P=$PIPELINE):3:$PIPELINE"
    "GET (3B, P=$PIPELINE):3:$PIPELINE"
    "SET (64B, P=$PIPELINE):64:$PIPELINE"
    "GET (64B, P=$PIPELINE):64:$PIPELINE"
    "SET (1KB, P=$PIPELINE):1024:$PIPELINE"
    "GET (1KB, P=$PIPELINE):1024:$PIPELINE"
    "SET (3B, P=1):3:1"
    "GET (3B, P=1):3:1"
)

# --- run benchmarks ---

echo ""
echo "running benchmarks ($REQUESTS requests, $CLIENTS clients)..."
echo ""

# pre-populate
populate_keys "$EMBER_PORT"
if [[ "$EMBER_ONLY" == "false" ]]; then
    populate_keys "$REDIS_PORT"
fi

declare -a LABELS=()
declare -a EMBER_RESULTS=()
declare -a REDIS_RESULTS=()

for test_spec in "${TESTS[@]}"; do
    IFS=':' read -r label data_size pipeline <<< "$test_spec"
    LABELS+=("$label")

    # determine if this is SET or GET from the label
    if [[ "$label" == SET* ]]; then
        test_type="SET"
    else
        test_type="GET"
    fi

    # run ember
    csv=$(run_benchmark "$EMBER_PORT" "$data_size" "$pipeline")
    ember_rps=$(extract_rps "$csv" "$test_type")
    EMBER_RESULTS+=("${ember_rps:-0}")

    # run redis
    if [[ "$EMBER_ONLY" == "false" ]]; then
        csv=$(run_benchmark "$REDIS_PORT" "$data_size" "$pipeline")
        redis_rps=$(extract_rps "$csv" "$test_type")
        REDIS_RESULTS+=("${redis_rps:-0}")
    fi
done

# --- output results ---

if [[ "$JSON_OUTPUT" == "true" ]]; then
    echo "{"
    echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
    echo "  \"config\": { \"requests\": $REQUESTS, \"clients\": $CLIENTS, \"pipeline\": $PIPELINE },"
    echo "  \"ember\": {"
    for i in "${!LABELS[@]}"; do
        comma=""
        [[ $i -lt $((${#LABELS[@]} - 1)) ]] && comma=","
        echo "    \"${LABELS[$i]}\": ${EMBER_RESULTS[$i]}$comma"
    done
    echo "  }"
    if [[ "$EMBER_ONLY" == "false" ]]; then
        echo "  ,\"redis\": {"
        for i in "${!LABELS[@]}"; do
            comma=""
            [[ $i -lt $((${#LABELS[@]} - 1)) ]] && comma=","
            echo "    \"${LABELS[$i]}\": ${REDIS_RESULTS[$i]}$comma"
        done
        echo "  }"
    fi
    echo "}"
else
    DATE=$(date +%Y-%m-%d)

    if [[ "$EMBER_ONLY" == "false" ]]; then
        echo "ember vs redis benchmark — $DATE"
        echo "config: $REQUESTS requests, $CLIENTS clients"
        echo ""
        printf "%-24s %12s %12s %8s\n" "test" "ember (rps)" "redis (rps)" "ratio"
        printf "%-24s %12s %12s %8s\n" "----" "-----------" "-----------" "-----"

        for i in "${!LABELS[@]}"; do
            e=${EMBER_RESULTS[$i]}
            r=${REDIS_RESULTS[$i]}
            if [[ "$r" -gt 0 ]]; then
                # integer ratio with one decimal: (e * 10 / r) then insert decimal
                ratio_x10=$(( (e * 10 + r / 2) / r ))
                ratio_int=$((ratio_x10 / 10))
                ratio_frac=$((ratio_x10 % 10))
                ratio="${ratio_int}.${ratio_frac}x"
            else
                ratio="n/a"
            fi
            printf "%-24s %12s %12s %8s\n" "${LABELS[$i]}" \
                "$(printf "%'d" "$e")" \
                "$(printf "%'d" "$r")" \
                "$ratio"
        done
    else
        echo "ember benchmark — $DATE"
        echo "config: $REQUESTS requests, $CLIENTS clients"
        echo ""
        printf "%-24s %12s\n" "test" "ember (rps)"
        printf "%-24s %12s\n" "----" "-----------"

        for i in "${!LABELS[@]}"; do
            printf "%-24s %12s\n" "${LABELS[$i]}" "$(printf "%'d" "${EMBER_RESULTS[$i]}")"
        done
    fi
fi

# --- save raw results ---

RESULT_FILE="$RESULTS_DIR/$TIMESTAMP.csv"
{
    echo "test,ember_rps,redis_rps"
    for i in "${!LABELS[@]}"; do
        redis_val="${REDIS_RESULTS[$i]:-}"
        echo "${LABELS[$i]},${EMBER_RESULTS[$i]},${redis_val}"
    done
} > "$RESULT_FILE"

echo ""
echo "raw results saved to $RESULT_FILE"
