#!/usr/bin/env bash
#
# download SIFT1M dataset for recall benchmarks.
#
# source: ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
# 1M base vectors (128-dim), 10k queries with ground truth.
#
# usage: bash bench/vector_data/download_sift.sh [output_dir]

set -euo pipefail

DATA_DIR="${1:-$(dirname "$0")}"

if [[ -f "$DATA_DIR/sift/sift_base.fvecs" ]]; then
    echo "SIFT1M already downloaded in $DATA_DIR/sift/"
    exit 0
fi

echo "downloading SIFT1M dataset (~160MB)..."
curl -fSL "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz" -o "$DATA_DIR/sift.tar.gz"

echo "extracting..."
tar -xzf "$DATA_DIR/sift.tar.gz" -C "$DATA_DIR"
rm -f "$DATA_DIR/sift.tar.gz"

# verify key files exist and have expected sizes
for f in sift_base.fvecs sift_query.fvecs sift_groundtruth.ivecs; do
    if [[ ! -f "$DATA_DIR/sift/$f" ]]; then
        echo "error: expected file $f not found after extraction" >&2
        exit 1
    fi
done

echo "SIFT1M dataset ready in $DATA_DIR/sift/"
echo "  base vectors:   $(wc -c < "$DATA_DIR/sift/sift_base.fvecs") bytes"
echo "  query vectors:  $(wc -c < "$DATA_DIR/sift/sift_query.fvecs") bytes"
echo "  ground truth:   $(wc -c < "$DATA_DIR/sift/sift_groundtruth.ivecs") bytes"
