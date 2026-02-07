#!/bin/bash
#
# set up a fresh linux VM for benchmarking
#
# usage: ssh user@vm 'bash -s' < ./scripts/setup-vm.sh
#
# tested on ubuntu 22.04

set -e

echo "=== installing dependencies ==="

sudo apt-get update
sudo apt-get install -y build-essential git redis-server

echo ""
echo "=== installing rust ==="

if command -v rustup &> /dev/null; then
    echo "rust already installed"
    rustup update stable
else
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source ~/.cargo/env
fi

echo ""
echo "=== cloning ember ==="

if [[ -d ~/ember ]]; then
    cd ~/ember
    git pull origin main
else
    git clone https://github.com/kacy/ember.git ~/ember
    cd ~/ember
fi

echo ""
echo "=== building ember ==="

cargo build --release --features jemalloc

echo ""
echo "=== verifying ==="

./target/release/ember-server --help | head -5
redis-server --version
redis-benchmark --version

echo ""
echo "=== setup complete ==="
echo ""
echo "run benchmarks with:"
echo "  ./scripts/bench.sh        # full suite"
echo "  ./scripts/bench-quick.sh  # quick sanity check"
echo "  ./scripts/bench-memory.sh # memory comparison"
