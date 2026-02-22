#!/usr/bin/env bash
# install.sh — download and install ember binaries from the latest GitHub release
#
# usage: curl -fsSL https://raw.githubusercontent.com/kacy/ember/main/scripts/install.sh | bash
# or:    curl -fsSL ... | bash -s -- --prefix /opt/ember
#
# options:
#   --prefix DIR    install directory (default: /usr/local/bin)
#   --version TAG   install a specific version (default: latest)
#   --server-only   skip installing ember-cli
#   --cli-only      skip installing ember-server

set -euo pipefail

REPO="kacy/ember"
INSTALL_PREFIX="/usr/local/bin"
VERSION=""
SERVER_ONLY=false
CLI_ONLY=false

# ── argument parsing ──────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix)    INSTALL_PREFIX="$2"; shift 2 ;;
        --version)   VERSION="$2"; shift 2 ;;
        --server-only) SERVER_ONLY=true; shift ;;
        --cli-only)    CLI_ONLY=true; shift ;;
        *)           echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

# ── helpers ───────────────────────────────────────────────────────────────────

die()  { echo "error: $*" >&2; exit 1; }
info() { echo "  $*" >&2; }

need_cmd() {
    if ! command -v "$1" &>/dev/null; then
        die "required command not found: $1 — please install it and try again"
    fi
}

# ── platform detection ────────────────────────────────────────────────────────

detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *)      die "unsupported operating system: $(uname -s) — only Linux and macOS are supported" ;;
    esac

    case "$(uname -m)" in
        x86_64)          arch="amd64" ;;
        amd64)           arch="amd64" ;;
        aarch64|arm64)   arch="arm64" ;;
        *)               die "unsupported architecture: $(uname -m) — only x86_64 and arm64 are supported" ;;
    esac

    echo "${os}-${arch}"
}

# ── version resolution ────────────────────────────────────────────────────────

resolve_version() {
    if [[ -n "$VERSION" ]]; then
        echo "$VERSION"
        return
    fi

    info "fetching latest release tag..."
    local tag
    tag=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases" \
        | grep '"tag_name"' \
        | grep -Eo '"v[0-9]+\.[0-9]+\.[0-9]+"' \
        | tr -d '"' \
        | head -1)

    if [[ -z "$tag" ]]; then
        die "could not determine the latest release — check your network or specify --version"
    fi

    echo "$tag"
}

# ── download and verify ───────────────────────────────────────────────────────

download_and_verify() {
    local name="$1"
    local tag="$2"
    local tmpdir="$3"

    local url="https://github.com/${REPO}/releases/download/${tag}/${name}"
    local checksums_url="https://github.com/${REPO}/releases/download/${tag}/checksums.txt"
    local dest="${tmpdir}/${name}"
    local checksums_file="${tmpdir}/checksums.txt"

    info "downloading ${name}..."
    if ! curl -fsSL --progress-bar -o "$dest" "$url"; then
        die "download failed: ${url}
       the release may not include a build for your platform, or the version may not exist"
    fi

    # download checksums only once (first call writes it, subsequent calls reuse)
    if [[ ! -f "$checksums_file" ]]; then
        info "downloading checksums..."
        if ! curl -fsSL -o "$checksums_file" "$checksums_url"; then
            die "could not download checksums from ${checksums_url}"
        fi
    fi

    info "verifying checksum for ${name}..."
    local expected actual
    expected=$(grep " ${name}$" "$checksums_file" | awk '{print $1}')
    if [[ -z "$expected" ]]; then
        die "checksum not found for ${name} in checksums.txt"
    fi

    if command -v sha256sum &>/dev/null; then
        actual=$(sha256sum "$dest" | awk '{print $1}')
    elif command -v shasum &>/dev/null; then
        actual=$(shasum -a 256 "$dest" | awk '{print $1}')
    else
        die "neither sha256sum nor shasum is available — cannot verify download"
    fi

    if [[ "$expected" != "$actual" ]]; then
        die "checksum mismatch for ${name}:
       expected: ${expected}
       got:      ${actual}
       the downloaded binary may be corrupted — please try again"
    fi
}

# ── install binary ────────────────────────────────────────────────────────────

install_binary() {
    local src="$1"
    local dest="$2"

    chmod +x "$src"

    if [[ -w "$(dirname "$dest")" ]]; then
        mv "$src" "$dest"
    else
        info "requesting sudo to write to $(dirname "$dest")..."
        sudo mv "$src" "$dest"
    fi

    info "installed to ${dest}"
}

# ── main ──────────────────────────────────────────────────────────────────────

main() {
    need_cmd curl
    need_cmd uname

    local platform
    platform=$(detect_platform)

    local tag
    tag=$(resolve_version)

    local tmpdir
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT

    echo ""
    echo "installing ember ${tag} for ${platform}"
    echo ""

    if ! "$CLI_ONLY"; then
        local server_name="ember-server-${platform}"
        download_and_verify "$server_name" "$tag" "$tmpdir"
        install_binary "${tmpdir}/${server_name}" "${INSTALL_PREFIX}/ember-server"
    fi

    if ! "$SERVER_ONLY"; then
        local cli_name="ember-cli-${platform}"
        download_and_verify "$cli_name" "$tag" "$tmpdir"
        install_binary "${tmpdir}/${cli_name}" "${INSTALL_PREFIX}/ember-cli"
    fi

    echo ""
    echo "done. verify with:"

    if ! "$CLI_ONLY"; then
        echo "  ember-server --version"
    fi
    if ! "$SERVER_ONLY"; then
        echo "  ember-cli --version"
    fi

    echo ""
    echo "to get started: https://github.com/${REPO}#getting-started"
}

main
