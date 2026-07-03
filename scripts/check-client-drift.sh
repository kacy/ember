#!/usr/bin/env bash
# Verifies that every RPC in the canonical proto exists in each client's
# generated stubs (and the TS client's vendored proto copy). Catches
# clients whose codegen wasn't re-run after the proto changed — in #337
# only the Go client was regenerated and the Python/TS clients silently
# fell 17 RPCs behind.
#
# Name-presence checking is deliberate: byte-exact regeneration in CI is
# brittle across protoc/plugin versions, and missing RPCs are the drift
# class that actually occurs.
set -euo pipefail

cd "$(dirname "$0")/.."

PROTO=proto/ember/v1/ember.proto
GO_STUB=clients/ember-go/proto/ember/v1/ember_grpc.pb.go
PY_STUB=clients/ember-py/ember/proto/ember/v1/ember_pb2_grpc.py
TS_STUB=clients/ember-ts/src/generated/ember/v1/EmberCache.ts
TS_PROTO=clients/ember-ts/proto/ember/v1/ember.proto

rpcs=$(grep -oE '^[[:space:]]*rpc [A-Za-z0-9]+' "$PROTO" | awk '{print $2}')
missing=0
for rpc in $rpcs; do
  grep -qw "$rpc" "$GO_STUB" || { echo "go client missing rpc: $rpc"; missing=1; }
  grep -q "ember.v1.EmberCache/$rpc'" "$PY_STUB" || { echo "python client missing rpc: $rpc"; missing=1; }
  grep -qw "$rpc" "$TS_STUB" || { echo "ts client missing rpc: $rpc"; missing=1; }
  grep -qE "rpc $rpc\(" "$TS_PROTO" || { echo "ts vendored proto missing rpc: $rpc"; missing=1; }
done

if [ "$missing" -ne 0 ]; then
  echo
  echo "client stubs are out of date with $PROTO. regenerate with:"
  echo "  make -C clients/ember-go proto-gen"
  echo "  make -C clients/ember-py proto-gen"
  echo "  make -C clients/ember-ts proto-gen"
  exit 1
fi
echo "all clients in sync: $(echo "$rpcs" | wc -l | tr -d ' ') rpcs"
