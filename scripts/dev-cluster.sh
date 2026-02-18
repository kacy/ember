#!/usr/bin/env bash
# dev-cluster.sh — manage a 3-node local ember cluster for development
#
# usage: ./scripts/dev-cluster.sh [start|stop|status|clean]
#
# all state lives under $EMBER_DATA_DIR (default: ./data/cluster).
# base port is $EMBER_BASE_PORT (default: 6379); nodes use +0, +1, +2.
# gossip port is client port + cluster_port_offset (default 10000).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="${REPO_ROOT}/target/release/ember-server"
CLI="${REPO_ROOT}/target/release/ember-cli"

DATA_DIR="${EMBER_DATA_DIR:-${REPO_ROOT}/data/cluster}"
BASE_PORT="${EMBER_BASE_PORT:-6379}"
NODE_COUNT=3

# ── helpers ──────────────────────────────────────────────────────────────────

die() { echo "error: $*" >&2; exit 1; }

ensure_binaries() {
    if [[ ! -x "$SERVER" || ! -x "$CLI" ]]; then
        echo "building release binaries..."
        cargo build --release -p ember-server -p emberkv-cli \
            --manifest-path "${REPO_ROOT}/Cargo.toml" \
            || die "build failed"
    fi
}

node_port() { echo $(( BASE_PORT + $1 - 1 )); }

pid_file() { echo "${DATA_DIR}/node-$1.pid"; }

wait_for_node() {
    local port="$1"
    local label="$2"
    local deadline=$(( $(date +%s) + 10 ))
    while true; do
        if "$CLI" -p "$port" PING 2>/dev/null | grep -qi pong; then
            echo "  node :${port} ready"
            return 0
        fi
        if (( $(date +%s) > deadline )); then
            die "node ${label} (port ${port}) did not respond within 10s — check ${DATA_DIR}/logs/${label}.log"
        fi
        sleep 0.5
    done
}

# ── start ─────────────────────────────────────────────────────────────────────

cmd_start() {
    ensure_binaries

    # check if already running
    if [[ -f "$(pid_file 1)" ]]; then
        local pid; pid=$(cat "$(pid_file 1)")
        if kill -0 "$pid" 2>/dev/null; then
            die "cluster already running (node 1 pid ${pid}). run 'stop' first."
        fi
    fi

    mkdir -p "${DATA_DIR}/node-1" "${DATA_DIR}/node-2" "${DATA_DIR}/node-3" "${DATA_DIR}/logs"

    local p1; p1=$(node_port 1)
    local p2; p2=$(node_port 2)
    local p3; p3=$(node_port 3)

    echo "starting node 1 (port ${p1}, bootstrap)..."
    "$SERVER" \
        --port "$p1" \
        --cluster-enabled \
        --cluster-bootstrap \
        --data-dir "${DATA_DIR}/node-1" \
        >"${DATA_DIR}/logs/node-1.log" 2>&1 &
    echo $! >"$(pid_file 1)"

    echo "starting node 2 (port ${p2})..."
    "$SERVER" \
        --port "$p2" \
        --cluster-enabled \
        --data-dir "${DATA_DIR}/node-2" \
        >"${DATA_DIR}/logs/node-2.log" 2>&1 &
    echo $! >"$(pid_file 2)"

    echo "starting node 3 (port ${p3})..."
    "$SERVER" \
        --port "$p3" \
        --cluster-enabled \
        --data-dir "${DATA_DIR}/node-3" \
        >"${DATA_DIR}/logs/node-3.log" 2>&1 &
    echo $! >"$(pid_file 3)"

    wait_for_node "$p1" "node-1"
    wait_for_node "$p2" "node-2"
    wait_for_node "$p3" "node-3"

    echo "joining cluster..."
    "$CLI" -p "$p1" cluster meet 127.0.0.1 "$p2" >/dev/null
    "$CLI" -p "$p1" cluster meet 127.0.0.1 "$p3" >/dev/null

    echo "assigning slots..."
    "$CLI" -p "$p1" cluster addslotsrange 0 5460 >/dev/null
    "$CLI" -p "$p2" cluster addslotsrange 5461 10922 >/dev/null
    "$CLI" -p "$p3" cluster addslotsrange 10923 16383 >/dev/null

    # give gossip a moment to converge
    sleep 0.5

    echo ""
    "$CLI" -p "$p1" cluster info
}

# ── stop ──────────────────────────────────────────────────────────────────────

cmd_stop() {
    local stopped=0
    for i in $(seq 1 $NODE_COUNT); do
        local f; f=$(pid_file "$i")
        if [[ -f "$f" ]]; then
            local pid; pid=$(cat "$f")
            if kill -0 "$pid" 2>/dev/null; then
                echo "stopping node $i (pid ${pid})..."
                kill "$pid" || true
                stopped=$(( stopped + 1 ))
            fi
            rm -f "$f"
        fi
    done
    if (( stopped == 0 )); then
        echo "no running nodes found"
    fi
}

# ── status ────────────────────────────────────────────────────────────────────

cmd_status() {
    local p1; p1=$(node_port 1)
    echo "--- cluster info ---"
    "$CLI" -p "$p1" cluster info || echo "(could not reach node 1 on port ${p1})"
    echo ""
    echo "--- cluster nodes ---"
    "$CLI" -p "$p1" cluster nodes || true
}

# ── clean ─────────────────────────────────────────────────────────────────────

cmd_clean() {
    cmd_stop
    if [[ -d "$DATA_DIR" ]]; then
        echo "removing ${DATA_DIR}..."
        rm -rf "$DATA_DIR"
    fi
}

# ── dispatch ──────────────────────────────────────────────────────────────────

case "${1:-}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    status)  cmd_status ;;
    clean)   cmd_clean ;;
    *)
        echo "usage: $0 [start|stop|status|clean]"
        echo ""
        echo "  start   build binaries (if needed) and start a 3-node local cluster"
        echo "  stop    send SIGTERM to all running cluster nodes"
        echo "  status  print cluster info and node list"
        echo "  clean   stop cluster and delete all data"
        exit 1
        ;;
esac
