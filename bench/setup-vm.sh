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
sudo apt-get install -y build-essential git redis-server unzip \
    autoconf automake libpcre3-dev libevent-dev pkg-config zlib1g-dev libssl-dev \
    gcc-12 g++-12 python3-venv

# prefer gcc-12 (needed for usearch SIMD in vector feature)
if command -v gcc-12 &> /dev/null; then
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 120 \
        --slave /usr/bin/g++ g++ /usr/bin/g++-12 2>/dev/null || true
fi

# disable the redis systemd service — benchmarks manage their own redis instances
sudo systemctl stop redis-server 2>/dev/null || true
sudo systemctl disable redis-server 2>/dev/null || true

# install modern protoc (needed for gRPC and protobuf features)
if ! protoc --version 2>/dev/null | grep -q "29"; then
    echo "installing protoc 29.3..."
    curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-linux-x86_64.zip" -o /tmp/protoc.zip
    sudo unzip -o /tmp/protoc.zip -d /usr/local
    rm -f /tmp/protoc.zip
    protoc --version
fi

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
            # find the binary in the extracted archive (named dragonfly-<arch>)
            DRAGONFLY_BIN=$(find /tmp/dragonfly_extract -name "dragonfly*" -type f ! -name "*.md" | head -1)
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

# detect available features
FEATURES="jemalloc"
if grep -q 'vector' crates/ember-server/Cargo.toml 2>/dev/null; then
    FEATURES="$FEATURES,vector"
fi
if grep -q 'grpc' crates/ember-server/Cargo.toml 2>/dev/null; then
    FEATURES="$FEATURES,grpc"
fi
if grep -q 'protobuf' crates/ember-server/Cargo.toml 2>/dev/null; then
    FEATURES="$FEATURES,protobuf"
fi
if grep -q 'encryption' crates/ember-server/Cargo.toml 2>/dev/null; then
    FEATURES="$FEATURES,encryption"
fi
echo "building with features: $FEATURES"
cargo build --release -p ember-server --features "$FEATURES"

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
