#!/usr/bin/env bash
#
# run all benchmarks sequentially.
#
# builds ember with all features, then runs each benchmark script in order.
# designed for a dedicated VM (GCP c2-standard-8) where all dependencies
# are pre-installed via setup-vm.sh and setup-vm-vector.sh.
#
# usage:
#   bash bench/bench-all.sh
#
# skips benchmarks whose dependencies aren't available (docker, protoc, etc.)
# rather than failing hard.

set -euo pipefail

RESULTS_DIR="bench/results"
mkdir -p "$RESULTS_DIR"

echo "========================================================================"
echo "                    ember full benchmark suite"
echo "========================================================================"
echo ""

# --- build ---

FEATURES="jemalloc"

# add optional features if their deps are available
[[ -d "crates/ember-server/src" ]] && FEATURES="$FEATURES,vector"

# check for grpc feature support
if grep -q 'grpc' crates/ember-server/Cargo.toml 2>/dev/null; then
    FEATURES="$FEATURES,grpc"
fi

# check for protobuf feature support
if grep -q 'protobuf' crates/ember-server/Cargo.toml 2>/dev/null; then
    FEATURES="$FEATURES,protobuf"
fi

# check for encryption feature support
if grep -q 'encryption' crates/ember-server/Cargo.toml 2>/dev/null; then
    FEATURES="$FEATURES,encryption"
fi

echo "building ember-server with features: $FEATURES"
cargo build --release -p ember-server --features "$FEATURES"
echo ""

# --- run benchmarks ---

run_bench() {
    local name=$1
    local script=$2
    shift 2

    echo "========================================================================"
    echo "  $name"
    echo "========================================================================"
    echo ""

    if [[ ! -f "$script" ]]; then
        echo "  skipped: $script not found"
        echo ""
        return
    fi

    bash "$script" "$@" || echo "  warning: $name exited with non-zero status"
    echo ""
}

# 1. throughput (redis-benchmark)
run_bench "throughput (redis-benchmark)" bench/compare-redis.sh

# 2. throughput + latency (memtier_benchmark)
if command -v memtier_benchmark &> /dev/null; then
    run_bench "throughput + latency (memtier)" bench/bench-memtier.sh
else
    echo "  skipping memtier benchmarks (memtier_benchmark not found)"
    echo ""
fi

# 3. memory per data type
run_bench "memory (all data types)" bench/bench-memory.sh --vector

# 4. encryption overhead
run_bench "encryption overhead" bench/bench-encryption.sh

# 5. vector similarity (full comparison)
if command -v docker &> /dev/null; then
    run_bench "vector similarity (full)" bench/bench-vector.sh --qdrant --ember-grpc
else
    run_bench "vector similarity (ember only)" bench/bench-vector.sh --ember-only --ember-grpc
fi

# 6. SIFT1M recall accuracy
if command -v docker &> /dev/null; then
    run_bench "SIFT1M recall accuracy" bench/bench-vector.sh --sift --qdrant
else
    run_bench "SIFT1M recall accuracy" bench/bench-vector.sh --sift --ember-only
fi

# 7. gRPC vs RESP3
if [[ -f "bench/bench-grpc.sh" ]]; then
    run_bench "gRPC vs RESP3" bench/bench-grpc.sh
fi

# 8. pub/sub
if [[ -f "bench/bench-pubsub.sh" ]]; then
    run_bench "pub/sub throughput" bench/bench-pubsub.sh
fi

# 9. protobuf storage
if [[ -f "bench/bench-proto.sh" ]] && command -v protoc &> /dev/null; then
    run_bench "protobuf storage overhead" bench/bench-proto.sh
else
    echo "  skipping protobuf benchmarks (bench-proto.sh not found or protoc missing)"
    echo ""
fi

# --- summary ---

echo "========================================================================"
echo "  all benchmarks complete"
echo "========================================================================"
echo ""
echo "results directory:"
ls -la "$RESULTS_DIR"/ 2>/dev/null | tail -20
echo ""
echo "done."
