# benchmark scripts

scripts for benchmarking ember against redis.

## quick start (local)

```bash
# build ember
cargo build --release --features jemalloc

# run quick sanity check
./scripts/bench-quick.sh

# run full comparison (requires redis)
./scripts/bench.sh
```

## cloud vm benchmarking

for reproducible results, run on a dedicated VM (e.g., GCP c2-standard-8).

### 1. create a vm

```bash
# example: GCP compute-optimized instance
gcloud compute instances create ember-bench \
  --zone=us-central1-a \
  --machine-type=c2-standard-8 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud
```

### 2. set up the vm

```bash
# ssh and run setup script
gcloud compute ssh ember-bench --zone=us-central1-a \
  --command='bash -s' < ./scripts/setup-vm.sh
```

or manually:

```bash
gcloud compute ssh ember-bench --zone=us-central1-a

# on the vm:
sudo apt-get update && sudo apt-get install -y build-essential git redis-server
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
git clone https://github.com/kacy/ember.git
cd ember
cargo build --release --features jemalloc
```

### 3. run benchmarks

```bash
gcloud compute ssh ember-bench --zone=us-central1-a

cd ember
./scripts/bench.sh
```

### 4. cleanup

```bash
gcloud compute instances delete ember-bench --zone=us-central1-a
```

## scripts

| script | description |
|--------|-------------|
| `bench.sh` | full benchmark suite: ember (sharded + concurrent) vs redis |
| `bench-quick.sh` | quick sanity check (ember only, ~10 seconds) |
| `bench-memory.sh` | memory usage comparison with 1M keys |
| `setup-vm.sh` | install dependencies on a fresh ubuntu vm |

## configuration

all scripts support environment variables:

```bash
# customize benchmark parameters
REQUESTS=1000000 THREADS=16 ./scripts/bench.sh

# customize memory test
KEY_COUNT=5000000 VALUE_SIZE=128 ./scripts/bench-memory.sh
```

## expected results

on a c2-standard-8 (8 vCPU Intel Xeon @ 3.1GHz):

| mode | SET (P=16) | GET (P=16) |
|------|------------|------------|
| ember concurrent | ~1.8M/s | ~2.5M/s |
| ember sharded | ~900K/s | ~1.0M/s |
| redis | ~1.0M/s | ~1.2M/s |

results vary based on CPU, memory, and kernel version.
