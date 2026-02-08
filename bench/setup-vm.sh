#!/bin/bash
#
# set up a fresh linux VM for benchmarking
#
# usage: ssh user@vm 'bash -s' < ./bench/setup-vm.sh
#
# tested on ubuntu 22.04 (GCP c2-standard-8)

set -e

echo "=== installing system dependencies ==="

sudo apt-get update
sudo apt-get install -y build-essential git redis-server \
    autoconf automake libpcre3-dev libevent-dev pkg-config zlib1g-dev libssl-dev

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
echo "=== installing memtier_benchmark ==="

if command -v memtier_benchmark &> /dev/null; then
    echo "memtier_benchmark already installed"
    memtier_benchmark --version
else
    echo "building memtier_benchmark from source..."
    git clone https://github.com/RedisLabs/memtier_benchmark.git /tmp/memtier_build
    cd /tmp/memtier_build
    autoreconf -ivf
    ./configure
    make -j"$(nproc)"
    sudo make install
    cd ~
    rm -rf /tmp/memtier_build
    echo "memtier_benchmark installed"
    memtier_benchmark --version
fi

echo ""
echo "=== installing dragonfly ==="

if command -v dragonfly &> /dev/null; then
    echo "dragonfly already installed"
else
    ARCH=$(uname -m)
    if [[ "$ARCH" == "x86_64" ]]; then
        echo "downloading dragonfly binary..."
        DRAGONFLY_URL="https://github.com/dragonflydb/dragonfly/releases/latest/download/dragonfly-x86_64.tar.gz"
        if curl -fsSL "$DRAGONFLY_URL" -o /tmp/dragonfly.tar.gz; then
            mkdir -p /tmp/dragonfly_extract
            tar -xzf /tmp/dragonfly.tar.gz -C /tmp/dragonfly_extract
            # find the binary in the extracted archive
            DRAGONFLY_BIN=$(find /tmp/dragonfly_extract -name "dragonfly" -type f | head -1)
            if [[ -n "$DRAGONFLY_BIN" ]]; then
                sudo mv "$DRAGONFLY_BIN" /usr/local/bin/dragonfly
                sudo chmod +x /usr/local/bin/dragonfly
                echo "dragonfly installed"
            else
                echo "warning: could not find dragonfly binary in archive, skipping"
                echo "  benchmarks will work without dragonfly"
            fi
            rm -rf /tmp/dragonfly.tar.gz /tmp/dragonfly_extract
        else
            echo "warning: dragonfly download failed, skipping"
            echo "  install manually: https://github.com/dragonflydb/dragonfly/releases"
            echo "  benchmarks will work without dragonfly"
        fi
    else
        echo "note: dragonfly binary not available for $ARCH, skipping"
        echo "  benchmarks will work without dragonfly"
    fi
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
memtier_benchmark --version 2>/dev/null || echo "memtier_benchmark: not found"
dragonfly --version 2>/dev/null || echo "dragonfly: not found (optional)"

echo ""
echo "=== setup complete ==="
echo ""
echo "run benchmarks with:"
echo "  ./bench/bench-quick.sh        # quick sanity check"
echo "  ./bench/compare-redis.sh      # redis-benchmark comparison"
echo "  ./bench/bench-memtier.sh      # memtier_benchmark comparison"
echo "  ./bench/bench-memory.sh       # memory usage test"
echo "  ./bench/bench.sh              # full suite"
