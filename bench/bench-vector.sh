#!/usr/bin/env bash
#
# vector similarity benchmark: ember vs chromadb vs pgvector.
#
# compares insert throughput, query throughput/latency, and memory usage
# across three vector databases using identical HNSW parameters (M=16,
# ef_construction=64) and cosine similarity.
#
# usage:
#   bash bench/bench-vector.sh              # full comparison (100k vectors)
#   bash bench/bench-vector.sh --ember-only # ember only (no docker needed)
#   bash bench/bench-vector.sh --quick      # quick run (1k vectors)
#   bash bench/bench-vector.sh --sift       # SIFT1M recall accuracy
#
# environment variables:
#   EMBER_PORT       ember port             (default: 6379)
#   CHROMA_PORT      chromadb port          (default: 8000)
#   PGVECTOR_PORT    pgvector port          (default: 5432)
#   VECTOR_COUNT     base vector count      (default: 100000)
#   VECTOR_DIM       vector dimensions      (default: 128)
#   QUERY_COUNT      query vector count     (default: 1000)
#   EMBER_BIN        ember-server binary    (default: ./target/release/ember-server)

set -euo pipefail

# --- configuration ---

EMBER_PORT="${EMBER_PORT:-6379}"
CHROMA_PORT="${CHROMA_PORT:-8000}"
PGVECTOR_PORT="${PGVECTOR_PORT:-5432}"
VECTOR_COUNT="${VECTOR_COUNT:-100000}"
VECTOR_DIM="${VECTOR_DIM:-128}"
QUERY_COUNT="${QUERY_COUNT:-1000}"
K=10
BATCH_SIZE=500
EMBER_BIN="${EMBER_BIN:-./target/release/ember-server}"
RESULTS_DIR="bench/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BENCH_SCRIPT="bench/bench-vector.py"
SIFT_DIR="bench/vector_data"

EMBER_ONLY=false
QUICK_MODE=false
SIFT_MODE=false

for arg in "$@"; do
    case "$arg" in
        --ember-only) EMBER_ONLY=true ;;
        --quick) QUICK_MODE=true ;;
        --sift) SIFT_MODE=true ;;
        *) echo "unknown flag: $arg"; exit 1 ;;
    esac
done

if [[ "$QUICK_MODE" == "true" ]]; then
    VECTOR_COUNT=1000
    QUERY_COUNT=100
fi

# --- cleanup ---

EMBER_PID=""
CHROMA_CONTAINER=""
PGVECTOR_CONTAINER=""

cleanup() {
    echo ""
    echo "cleaning up..."
    [[ -n "$EMBER_PID" ]] && kill "$EMBER_PID" 2>/dev/null && wait "$EMBER_PID" 2>/dev/null || true
    [[ -n "$CHROMA_CONTAINER" ]] && docker rm -f "$CHROMA_CONTAINER" > /dev/null 2>&1 || true
    [[ -n "$PGVECTOR_CONTAINER" ]] && docker rm -f "$PGVECTOR_CONTAINER" > /dev/null 2>&1 || true
    rm -f /tmp/bench_vector_*.json
}
trap cleanup EXIT

# --- helpers ---

format_number() {
    printf "%'d" "$1"
}

format_float() {
    printf "%.1f" "$1"
}

get_rss_mb() {
    local pid=$1
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS: ps reports RSS in bytes
        local rss_bytes
        rss_bytes=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
        if [[ -n "$rss_bytes" ]]; then
            echo $(( rss_bytes / 1024 ))
        else
            echo 0
        fi
    else
        # linux: /proc gives RSS in kB
        local rss_kb
        rss_kb=$(awk '/VmRSS/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
        echo $(( rss_kb / 1024 ))
    fi
}

get_container_rss_mb() {
    local container=$1
    # docker stats gives memory usage directly
    local mem
    mem=$(docker stats --no-stream --format "{{.MemUsage}}" "$container" 2>/dev/null | awk '{print $1}')
    if [[ "$mem" == *GiB* ]]; then
        local val=${mem%GiB*}
        echo "$val" | awk '{printf "%d", $1 * 1024}'
    elif [[ "$mem" == *MiB* ]]; then
        local val=${mem%MiB*}
        echo "$val" | awk '{printf "%d", $1}'
    else
        echo 0
    fi
}

wait_for_ember() {
    local port=$1
    local retries=50
    while ! redis-cli -p "$port" ping > /dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            echo "error: ember did not start on port $port" >&2
            exit 1
        fi
        sleep 0.1
    done
}

wait_for_chroma() {
    local port=$1
    local retries=60
    while ! curl -sf "http://127.0.0.1:$port/api/v2/heartbeat" > /dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            echo "error: chromadb did not start on port $port" >&2
            exit 1
        fi
        sleep 0.5
    done
}

wait_for_pgvector() {
    local container=$1
    local retries=60
    while ! docker exec "$container" pg_isready -U postgres > /dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            echo "error: pgvector did not start" >&2
            exit 1
        fi
        sleep 0.5
    done
    # extra pause for pgvector to fully initialize
    sleep 1
}

# --- dependency checks ---

if ! command -v python3 &> /dev/null; then
    echo "error: python3 required" >&2
    exit 1
fi

if ! command -v redis-cli &> /dev/null; then
    echo "error: redis-cli required (for ember health checks)" >&2
    exit 1
fi

# set up python venv for benchmark dependencies.
# this avoids issues with externally-managed environments (PEP 668)
# on macOS homebrew and modern linux distros.
VENV_DIR=".bench-venv"

ensure_venv() {
    if [[ ! -d "$VENV_DIR" ]]; then
        echo "creating python venv for benchmark dependencies..."
        python3 -m venv "$VENV_DIR"
    fi
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
}

REQUIRED_DEPS="redis numpy"
if [[ "$EMBER_ONLY" == "false" ]]; then
    REQUIRED_DEPS="redis numpy chromadb psycopg2-binary"

    if ! command -v docker &> /dev/null; then
        echo "error: docker required for chromadb/pgvector (or use --ember-only)" >&2
        exit 1
    fi
fi

# check if all deps are available in current python; if not, use venv
if ! python3 -c "import redis, numpy" 2>/dev/null; then
    ensure_venv
    pip install --quiet $REQUIRED_DEPS
elif [[ "$EMBER_ONLY" == "false" ]] && ! python3 -c "import chromadb, psycopg2" 2>/dev/null; then
    ensure_venv
    pip install --quiet $REQUIRED_DEPS
fi

# build ember if needed
if [[ ! -x "$EMBER_BIN" ]]; then
    echo "building ember-server with vector support..."
    cargo build --release -p ember-server --features jemalloc,vector
fi

# download SIFT dataset if needed
if [[ "$SIFT_MODE" == "true" ]]; then
    if [[ ! -f "$SIFT_DIR/sift/sift_base.fvecs" ]]; then
        echo "downloading SIFT1M dataset..."
        bash bench/vector_data/download_sift.sh "$SIFT_DIR"
    fi
fi

mkdir -p "$RESULTS_DIR"

# --- print configuration ---

MODE="random"
if [[ "$SIFT_MODE" == "true" ]]; then
    MODE="sift"
fi

echo ""
echo "=== vector benchmark configuration ==="
echo "mode:       $MODE"
if [[ "$SIFT_MODE" == "true" ]]; then
    echo "dataset:    SIFT1M (1M base, 10k queries, 128-dim)"
else
    echo "vectors:    $VECTOR_COUNT base, $QUERY_COUNT queries"
    echo "dimensions: $VECTOR_DIM"
fi
echo "k:          $K"
echo "hnsw:       M=16, ef_construction=64"
echo "metric:     cosine"
echo ""

# --- common benchmark args ---

BENCH_ARGS=(
    --dim "$VECTOR_DIM"
    --count "$VECTOR_COUNT"
    --queries "$QUERY_COUNT"
    --k "$K"
    --batch-size "$BATCH_SIZE"
    --ember-port "$EMBER_PORT"
    --chroma-port "$CHROMA_PORT"
    --pgvector-port "$PGVECTOR_PORT"
    --sift-dir "$SIFT_DIR"
)

if [[ "$SIFT_MODE" == "true" ]]; then
    BENCH_ARGS+=(--mode sift)
else
    BENCH_ARGS+=(--mode random)
fi

# --- benchmark ember ---

echo "--- ember ---"
echo ""
echo "  starting ember on port $EMBER_PORT..."
"$EMBER_BIN" --port "$EMBER_PORT" > /dev/null 2>&1 &
EMBER_PID=$!
wait_for_ember "$EMBER_PORT"

EMBER_JSON="/tmp/bench_vector_ember.json"
python3 "$BENCH_SCRIPT" --system ember "${BENCH_ARGS[@]}" --output "$EMBER_JSON"

EMBER_RSS=$(get_rss_mb "$EMBER_PID")
echo "  memory (RSS): ${EMBER_RSS} MB"

kill "$EMBER_PID" 2>/dev/null && wait "$EMBER_PID" 2>/dev/null || true
EMBER_PID=""
sleep 0.5
echo ""

# --- benchmark chromadb ---

CHROMA_JSON="/tmp/bench_vector_chromadb.json"
CHROMA_RSS=0

if [[ "$EMBER_ONLY" == "false" ]]; then
    echo "--- chromadb ---"
    echo ""
    echo "  starting chromadb container on port $CHROMA_PORT..."
    CHROMA_CONTAINER=$(docker run -d --rm -p "$CHROMA_PORT:8000" chromadb/chroma 2>/dev/null)
    wait_for_chroma "$CHROMA_PORT"

    python3 "$BENCH_SCRIPT" --system chromadb "${BENCH_ARGS[@]}" --output "$CHROMA_JSON"

    CHROMA_RSS=$(get_container_rss_mb "$CHROMA_CONTAINER")
    echo "  memory (RSS): ${CHROMA_RSS} MB"

    docker rm -f "$CHROMA_CONTAINER" > /dev/null 2>&1 || true
    CHROMA_CONTAINER=""
    sleep 0.5
    echo ""
fi

# --- benchmark pgvector ---

PGVECTOR_JSON="/tmp/bench_vector_pgvector.json"
PGVECTOR_RSS=0

if [[ "$EMBER_ONLY" == "false" ]]; then
    echo "--- pgvector ---"
    echo ""
    echo "  starting pgvector container on port $PGVECTOR_PORT..."
    PGVECTOR_CONTAINER=$(docker run -d --rm \
        --shm-size=512m \
        -p "$PGVECTOR_PORT:5432" \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=vectordb \
        -e POSTGRES_HOST_AUTH_METHOD=trust \
        pgvector/pgvector:pg17 \
        postgres -c shared_buffers=256MB -c maintenance_work_mem=256MB \
        2>/dev/null)
    wait_for_pgvector "$PGVECTOR_CONTAINER"

    python3 "$BENCH_SCRIPT" --system pgvector "${BENCH_ARGS[@]}" --output "$PGVECTOR_JSON"

    PGVECTOR_RSS=$(get_container_rss_mb "$PGVECTOR_CONTAINER")
    echo "  memory (RSS): ${PGVECTOR_RSS} MB"

    docker rm -f "$PGVECTOR_CONTAINER" > /dev/null 2>&1 || true
    PGVECTOR_CONTAINER=""
    sleep 0.5
    echo ""
fi

# --- parse results ---

# extract values from JSON files using python (avoids jq dependency)
extract() {
    local file=$1
    local key=$2
    python3 -c "
import json, sys
try:
    d = json.load(open('$file'))
    keys = '$key'.split('.')
    v = d
    for k in keys:
        v = v[k]
    print(v)
except Exception:
    print('—')
" 2>/dev/null
}

# ember results (always present)
E_INSERT=$(extract "$EMBER_JSON" "insert.throughput")
E_QUERY=$(extract "$EMBER_JSON" "query.throughput")
E_P50=$(extract "$EMBER_JSON" "query.p50_ms")
E_P95=$(extract "$EMBER_JSON" "query.p95_ms")
E_P99=$(extract "$EMBER_JSON" "query.p99_ms")

# chromadb results
C_INSERT="—"
C_QUERY="—"
C_P50="—"
C_P95="—"
C_P99="—"

if [[ "$EMBER_ONLY" == "false" ]] && [[ -f "$CHROMA_JSON" ]]; then
    C_INSERT=$(extract "$CHROMA_JSON" "insert.throughput")
    C_QUERY=$(extract "$CHROMA_JSON" "query.throughput")
    C_P50=$(extract "$CHROMA_JSON" "query.p50_ms")
    C_P95=$(extract "$CHROMA_JSON" "query.p95_ms")
    C_P99=$(extract "$CHROMA_JSON" "query.p99_ms")
fi

# pgvector results
P_INSERT="—"
P_QUERY="—"
P_P50="—"
P_P95="—"
P_P99="—"

if [[ "$EMBER_ONLY" == "false" ]] && [[ -f "$PGVECTOR_JSON" ]]; then
    P_INSERT=$(extract "$PGVECTOR_JSON" "insert.throughput")
    P_QUERY=$(extract "$PGVECTOR_JSON" "query.throughput")
    P_P50=$(extract "$PGVECTOR_JSON" "query.p50_ms")
    P_P95=$(extract "$PGVECTOR_JSON" "query.p95_ms")
    P_P99=$(extract "$PGVECTOR_JSON" "query.p99_ms")
fi

# --- display results ---

DATE=$(date +%Y-%m-%d)
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)

echo ""
echo "========================================================================"
echo "             vector benchmark results — $DATE"
echo "========================================================================"
echo ""
echo "system: $CPU_CORES cores | $MODE mode"
if [[ "$SIFT_MODE" == "true" ]]; then
    echo "dataset: SIFT1M (1M vectors, 128-dim, 10k queries)"
else
    echo "vectors: $VECTOR_COUNT base, $QUERY_COUNT queries, ${VECTOR_DIM}-dim"
fi
echo "hnsw: M=16, ef_construction=64, cosine similarity, k=$K"
echo ""

if [[ "$EMBER_ONLY" == "true" ]]; then
    fmt="%-24s %14s"
    printf "$fmt\n" "metric" "ember"
    printf "$fmt\n" "------" "-----"
    printf "$fmt\n" "insert (vectors/sec)" "$E_INSERT"
    printf "$fmt\n" "query (queries/sec)" "$E_QUERY"
    printf "$fmt\n" "query p50 (ms)" "$E_P50"
    printf "$fmt\n" "query p95 (ms)" "$E_P95"
    printf "$fmt\n" "query p99 (ms)" "$E_P99"
    printf "$fmt\n" "memory (MB)" "$EMBER_RSS"
else
    fmt="%-24s %14s %14s %14s"
    printf "$fmt\n" "metric" "ember" "chromadb" "pgvector"
    printf "$fmt\n" "------" "-----" "--------" "--------"
    printf "$fmt\n" "insert (vectors/sec)" "$E_INSERT" "$C_INSERT" "$P_INSERT"
    printf "$fmt\n" "query (queries/sec)" "$E_QUERY" "$C_QUERY" "$P_QUERY"
    printf "$fmt\n" "query p50 (ms)" "$E_P50" "$C_P50" "$P_P50"
    printf "$fmt\n" "query p95 (ms)" "$E_P95" "$C_P95" "$P_P95"
    printf "$fmt\n" "query p99 (ms)" "$E_P99" "$C_P99" "$P_P99"
    printf "$fmt\n" "memory (MB)" "$EMBER_RSS" "$CHROMA_RSS" "$PGVECTOR_RSS"
fi

# recall (sift mode only)
if [[ "$SIFT_MODE" == "true" ]]; then
    echo ""
    echo "=== recall@$K ==="
    echo ""

    E_RECALL=$(extract "$EMBER_JSON" "recall.recall_at_k")

    if [[ "$EMBER_ONLY" == "true" ]]; then
        printf "%-24s %14s\n" "metric" "ember"
        printf "%-24s %14s\n" "------" "-----"
        printf "%-24s %14s\n" "recall@$K" "$E_RECALL"
    else
        C_RECALL=$(extract "$CHROMA_JSON" "recall.recall_at_k")
        P_RECALL=$(extract "$PGVECTOR_JSON" "recall.recall_at_k")
        printf "%-24s %14s %14s %14s\n" "metric" "ember" "chromadb" "pgvector"
        printf "%-24s %14s %14s %14s\n" "------" "-----" "--------" "--------"
        printf "%-24s %14s %14s %14s\n" "recall@$K" "$E_RECALL" "$C_RECALL" "$P_RECALL"
    fi
fi

echo ""

# --- save CSV ---

RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-vector.csv"
{
    echo "metric,ember,chromadb,pgvector"
    echo "insert_throughput,$E_INSERT,$C_INSERT,$P_INSERT"
    echo "query_throughput,$E_QUERY,$C_QUERY,$P_QUERY"
    echo "query_p50_ms,$E_P50,$C_P50,$P_P50"
    echo "query_p95_ms,$E_P95,$C_P95,$P_P95"
    echo "query_p99_ms,$E_P99,$C_P99,$P_P99"
    echo "memory_mb,$EMBER_RSS,$CHROMA_RSS,$PGVECTOR_RSS"
    if [[ "$SIFT_MODE" == "true" ]]; then
        E_RECALL=$(extract "$EMBER_JSON" "recall.recall_at_k")
        C_RECALL="—"
        P_RECALL="—"
        if [[ "$EMBER_ONLY" == "false" ]]; then
            C_RECALL=$(extract "$CHROMA_JSON" "recall.recall_at_k")
            P_RECALL=$(extract "$PGVECTOR_JSON" "recall.recall_at_k")
        fi
        echo "recall_at_$K,$E_RECALL,$C_RECALL,$P_RECALL"
    fi
} > "$RESULT_FILE"

echo "raw results saved to $RESULT_FILE"
