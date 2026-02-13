#!/usr/bin/env bash
#
# Integration test for genesis-sync-accelerator.
#
# Ensures source ImmutableDB data exists (via chain-init.sh), serves it over
# a local HTTP server, runs the accelerator, connects a consumer cardano-node,
# waits for the consumer to sync, and validates its ImmutableDB matches the
# source byte-for-byte.
#
# Usage:
#   nix develop .#integration-test
#   bash test/integration/run-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# ── Configuration ────────────────────────────────────────────────────────────

CDN_PORT=18080
ACCEL_PORT=13001
CONSUMER_PORT=13100
SYNC_TIMEOUT="${SYNC_TIMEOUT:-120}"
MAX_CACHED_CHUNKS=20
SOURCE_DB="${DB_DIR:-$SCRIPT_DIR/test-data/source-db}"
CONFIG="$SCRIPT_DIR/config/config.json"

# ── Cleanup ──────────────────────────────────────────────────────────────────

PIDS=()
TMPDIR=""

cleanup() {
  echo ""
  echo "=== Cleanup ==="
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "  Stopping pid $pid"
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n "$TMPDIR" && -d "$TMPDIR" ]]; then
    echo "  Removing $TMPDIR"
    rm -rf "$TMPDIR"
  fi
}
trap cleanup EXIT

# ── Ensure source data ──────────────────────────────────────────────────────

echo "${BOLD}=== Ensuring source data ===${NC}"

DB_DIR="$SOURCE_DB" bash "$SCRIPT_DIR/chain-init.sh"

IMMUTABLE_SRC="$SOURCE_DB/immutable"
CHUNK_COUNT=$(find "$IMMUTABLE_SRC" -name '*.chunk' | wc -l)
echo "  Source has $CHUNK_COUNT chunk(s)"

# ── Ephemeral workdir ────────────────────────────────────────────────────────

TMPDIR="$(mktemp -d -t gsa-test.XXXXXX)"
ACCEL_CACHE="$TMPDIR/accel-cache"
CONSUMER_DB="$TMPDIR/consumer-db"
mkdir -p "$ACCEL_CACHE" "$CONSUMER_DB"
echo "  Workdir: $TMPDIR"

# ── Start CDN ────────────────────────────────────────────────────────────────

echo ""
echo "${BOLD}=== Starting CDN ===${NC}"

python3 -m http.server "$CDN_PORT" --directory "$IMMUTABLE_SRC" \
  >"$TMPDIR/cdn.log" 2>&1 &
PIDS+=($!)

wait_for_port "$CDN_PORT" 10 "CDN"

# ── Start accelerator ────────────────────────────────────────────────────────

echo ""
echo "${BOLD}=== Starting accelerator ===${NC}"

genesis-sync-accelerator \
  --db "$ACCEL_CACHE" \
  --config "$CONFIG" \
  --rs-src-url "http://127.0.0.1:${CDN_PORT}" \
  --rs-cache-url "$ACCEL_CACHE" \
  --port "$ACCEL_PORT" \
  --max-cached-chunks "$MAX_CACHED_CHUNKS" \
  >"$TMPDIR/accelerator.log" 2>&1 &
PIDS+=($!)

wait_for_port "$ACCEL_PORT" 15 "Accelerator"

# ── Start consumer ────────────────────────────────────────────────────────────

echo ""
echo "${BOLD}=== Starting consumer ===${NC}"

cardano-node run \
  --config "$CONFIG" \
  --database-path "$CONSUMER_DB" \
  --topology "$SCRIPT_DIR/config/consumer-topology.json" \
  --port "$CONSUMER_PORT" \
  --socket-path "$TMPDIR/node.sock" \
  >"$TMPDIR/node.log" 2>&1 &
PIDS+=($!)

# ── Validate consumer ImmutableDB ────────────────────────────────────────────
#
# The consumer syncs from the accelerator, which fetches chunks on-demand from
# the CDN. We wait for the consumer's ImmutableDB to accumulate all expected
# chunks.
#
# The consumer gets all chunks except the last (we only read chunks in the immutable
# directory, and the last k chunks are still considered volatile).

echo ""
echo "${BOLD}=== Validate ===${NC}"

EXPECTED=$((CHUNK_COUNT - 1))
CONSUMER_IMMUTABLE="$CONSUMER_DB/immutable"

echo "  Waiting for $EXPECTED chunk(s) in consumer ImmutableDB …"

ELAPSED=0
while (( ELAPSED < SYNC_TIMEOUT )); do
  CHUNKS=0
  [[ -d "$CONSUMER_IMMUTABLE" ]] && CHUNKS=$(find "$CONSUMER_IMMUTABLE" -name '*.chunk' | wc -l)
  echo "  ${ELAPSED}/${SYNC_TIMEOUT}s — consumer has ${CHUNKS}/${EXPECTED} chunks"

  if (( CHUNKS >= EXPECTED )); then
    echo "  ${GREEN}Consumer has all ${EXPECTED} expected chunk(s)${NC}"
    break
  fi

  sleep 5
  ELAPSED=$((ELAPSED + 5))
done

if (( ELAPSED >= SYNC_TIMEOUT )); then
  echo "${RED}Timeout after ${SYNC_TIMEOUT}s (${CHUNKS}/${EXPECTED} chunks)${NC}"
  echo "--- accelerator log (last 30 lines) ---"
  tail -30 "$TMPDIR/accelerator.log" 2>/dev/null || true
  echo "--- consumer log (last 30 lines) ---"
  tail -30 "$TMPDIR/node.log" 2>/dev/null || true
  exit 1
fi

for ext in chunk primary secondary; do
  for f in "$CONSUMER_IMMUTABLE"/*."$ext"; do
    name="$(basename "$f")"
    src="$IMMUTABLE_SRC/$name"
    [[ -f "$src" ]] || { echo "  ${RED}UNEXPECTED${NC}: $name"; exit 1; }
    src_sum=$(sha256sum "$src" | awk '{print $1}')
    con_sum=$(sha256sum "$f" | awk '{print $1}')
    [[ "$src_sum" == "$con_sum" ]] || { echo "  ${RED}MISMATCH${NC}: $name"; exit 1; }
    echo "  ${GREEN}OK${NC}: $name"
  done
done

echo ""
echo "${GREEN}${BOLD}=== ALL CHECKS PASSED ===${NC}"
