#!/bin/bash
#
# additional VM setup for vector similarity benchmarks.
# run after setup-vm.sh on a fresh ubuntu VM.
#
# installs: python3 + deps, docker, chromadb/pgvector/qdrant images
#
# usage: ssh user@vm 'bash -s' < ./bench/setup-vm-vector.sh

set -e

echo "=== installing python dependencies ==="

sudo apt-get update
sudo apt-get install -y python3-pip python3-dev

pip3 install --quiet redis chromadb psycopg2-binary numpy grpcio protobuf qdrant-client

echo ""
echo "=== installing docker ==="

if command -v docker &> /dev/null; then
    echo "docker already installed"
    docker --version
else
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER"
    echo "docker installed (you may need to log out and back in for group permissions)"
fi

echo ""
echo "=== pulling docker images ==="

sudo docker pull chromadb/chroma
sudo docker pull pgvector/pgvector:pg17
sudo docker pull qdrant/qdrant

echo ""
echo "=== installing ember python client ==="

cd ~/ember
pip3 install --quiet ./clients/ember-py

echo ""
echo "=== rebuilding ember with vector + grpc support ==="

source ~/.cargo/env
cargo build --release -p ember-server --features jemalloc,vector,grpc

echo ""
echo "=== verifying ==="

python3 -c "import redis, chromadb, psycopg2, numpy, qdrant_client; print('python deps: ok')"
sudo docker images | grep -E "chroma|pgvector|qdrant" || true
./target/release/ember-server --help | head -3

echo ""
echo "=== vector benchmark setup complete ==="
echo ""
echo "run benchmarks with:"
echo "  ./bench/bench-vector.sh               # 100k random vectors"
echo "  ./bench/bench-vector.sh --ember-only  # ember only (no docker)"
echo "  ./bench/bench-vector.sh --ember-grpc  # include gRPC benchmark"
echo "  ./bench/bench-vector.sh --quick       # quick sanity check"
echo "  ./bench/bench-vector.sh --qdrant       # include qdrant comparison"
echo "  ./bench/bench-vector.sh --sift        # SIFT1M recall accuracy"
