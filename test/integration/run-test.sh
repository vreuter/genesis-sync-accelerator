#!/usr/bin/env bash
#
# Integration test for genesis-sync-accelerator.
#
# Ensures source ImmutableDB data exists (via chain-init.sh), serves it over
# a local HTTP server, runs the accelerator, connects a consumer cardano-node,
# waits for the accelerator to download all chunks from the CDN, validates
# the accelerator's cache matches the source byte-for-byte, and verifies the
# consumer received the correct number of blocks using db-analyser.
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
MIN_CHUNKS="${MIN_CHUNKS:-10}"
MAX_CACHED_CHUNKS=20
SOURCE_DB="${DB_DIR:-$SCRIPT_DIR/test-data/source-db}"
CONFIG="$SCRIPT_DIR/config/config.json"
GSA="${GSA:-genesis-sync-accelerator}"

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
echo "  Source has $(find "$IMMUTABLE_SRC" -name '*.chunk' | wc -l) chunk(s)"

# ── Ephemeral workdir ────────────────────────────────────────────────────────

TMPDIR="$(mktemp -d -t gsa-test.XXXXXX)"
ACCEL_CACHE="$TMPDIR/accel-cache"
CONSUMER_DB="$TMPDIR/consumer-db"
CDN_DATA="$TMPDIR/cdn-data"
mkdir -p "$ACCEL_CACHE" "$CONSUMER_DB" "$CDN_DATA"
echo "  Workdir: $TMPDIR"

# Copy only the first MIN_CHUNKS chunk triplets to serve from the CDN.
# This keeps the test fast and avoids stale-data issues with large source DBs.
for chunk_file in $(ls "$IMMUTABLE_SRC"/*.chunk 2>/dev/null | sort | head -n "$MIN_CHUNKS"); do
  base="$(basename "$chunk_file" .chunk)"
  for ext in chunk primary secondary; do
    cp "$IMMUTABLE_SRC/${base}.${ext}" "$CDN_DATA/"
  done
done
CHUNK_COUNT=$(find "$CDN_DATA" -name '*.chunk' | wc -l)
echo "  CDN will serve $CHUNK_COUNT chunk(s)"

# Secondary index entry size (see ouroboros-consensus Secondary.entrySize).
SECONDARY_ENTRY_SIZE=56
TOTAL_BLOCKS=0
for f in "$CDN_DATA"/*.secondary; do
  TOTAL_BLOCKS=$(( TOTAL_BLOCKS + $(stat -c%s "$f") / SECONDARY_ENTRY_SIZE ))
done
echo "  Total blocks in source data: $TOTAL_BLOCKS"

# The consumer keeps the last k blocks in VolatileDB; only older blocks move to
# ImmutableDB.  db-analyser --count-blocks reads ImmutableDB only, so the
# expected count is (total - k).  Read k from the Shelley genesis file.
SECURITY_PARAM=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['securityParam'])" \
  "$SCRIPT_DIR/config/shelley-genesis.json")
EXPECTED_IMMUTABLE=$(( TOTAL_BLOCKS > SECURITY_PARAM ? TOTAL_BLOCKS - SECURITY_PARAM : 0 ))
echo "  Security parameter (k): $SECURITY_PARAM"
echo "  Expected ImmutableDB blocks (total - k): $EXPECTED_IMMUTABLE"

# ── Start CDN ────────────────────────────────────────────────────────────────

echo ""
echo "${BOLD}=== Starting CDN ===${NC}"

python3 -m http.server "$CDN_PORT" --directory "$CDN_DATA" \
  >"$TMPDIR/cdn.log" 2>&1 &
PIDS+=($!)

wait_for_port "$CDN_PORT" 10 "CDN"

# ── Start accelerator ────────────────────────────────────────────────────────

echo ""
echo "${BOLD}=== Starting accelerator ===${NC}"

$GSA \
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
CONSUMER_PID=$!
PIDS+=($CONSUMER_PID)

# ── Validate accelerator cache ────────────────────────────────────────────────
#
# The consumer's ChainSync requests trigger the accelerator's on-demand
# downloads from the CDN.  We monitor the accelerator's cache directory
# rather than the consumer's ImmutableDB, because the consumer keeps all
# blocks in VolatileDB when the chain is shorter than the security parameter
# k=2160 (which it is for our small test data set).

echo ""
echo "${BOLD}=== Validate ===${NC}"

echo "  Waiting for $CHUNK_COUNT chunk(s) in accelerator cache …"

ELAPSED=0
CACHED_CHUNKS=0
while (( ELAPSED < SYNC_TIMEOUT )); do
  CACHED_CHUNKS=$(find "$ACCEL_CACHE" -maxdepth 1 -name '*.chunk' 2>/dev/null | wc -l)
  echo "  ${ELAPSED}/${SYNC_TIMEOUT}s — accelerator cache has ${CACHED_CHUNKS}/${CHUNK_COUNT} chunks"

  if (( CACHED_CHUNKS >= CHUNK_COUNT )); then
    echo "  ${GREEN}Accelerator downloaded all ${CACHED_CHUNKS} chunk(s)${NC}"
    break
  fi

  sleep 5
  ELAPSED=$((ELAPSED + 5))
done

if (( ELAPSED >= SYNC_TIMEOUT )); then
  echo "${RED}Timeout after ${SYNC_TIMEOUT}s (${CACHED_CHUNKS}/${CHUNK_COUNT} chunks)${NC}"
  echo "--- accelerator log (last 30 lines) ---"
  tail -30 "$TMPDIR/accelerator.log" 2>/dev/null || true
  echo "--- consumer log (last 30 lines) ---"
  tail -30 "$TMPDIR/node.log" 2>/dev/null || true
  exit 1
fi

# Validate each cached file matches the source byte-for-byte.
for ext in chunk primary secondary; do
  for src_file in "$CDN_DATA"/*."$ext"; do
    name="$(basename "$src_file")"
    cached_file="$ACCEL_CACHE/$name"
    if [[ -f "$cached_file" ]]; then
      src_sum=$(sha256sum "$src_file" | awk '{print $1}')
      cache_sum=$(sha256sum "$cached_file" | awk '{print $1}')
      if [[ "$src_sum" == "$cache_sum" ]]; then
        echo "  ${GREEN}OK${NC}: $name"
      else
        echo "  ${RED}MISMATCH${NC}: $name"
        exit 1
      fi
    else
      echo "  ${RED}MISSING${NC}: $name not in accelerator cache"
      exit 1
    fi
  done
done

# Wait for the consumer to finish syncing.  The accelerator cache being full
# only means the accelerator fetched all chunks — the consumer's BlockFetch
# may still be in-flight.  We watch "Chain extended" events in the consumer
# log and consider it done once no new events appear for QUIESCE_SECS.
QUIESCE_SECS=10
CONSUMER_TIMEOUT="${CONSUMER_TIMEOUT:-60}"

echo ""
echo "${BOLD}=== Waiting for consumer to finish syncing ===${NC}"

PREV_COUNT=0
QUIET_FOR=0
ELAPSED=0
while (( ELAPSED < CONSUMER_TIMEOUT )); do
  CUR_COUNT=$(grep -c 'Chain extended' "$TMPDIR/node.log" 2>/dev/null || echo "0")

  if (( CUR_COUNT > PREV_COUNT )); then
    QUIET_FOR=0
    PREV_COUNT=$CUR_COUNT
  else
    QUIET_FOR=$((QUIET_FOR + 2))
  fi

  echo "  ${ELAPSED}/${CONSUMER_TIMEOUT}s — ${CUR_COUNT} 'Chain extended' events (quiet ${QUIET_FOR}s)"

  if (( QUIET_FOR >= QUIESCE_SECS )); then
    echo "  ${GREEN}Consumer quiesced after ${CUR_COUNT} events${NC}"
    break
  fi

  sleep 2
  ELAPSED=$((ELAPSED + 2))
done

if (( ELAPSED >= CONSUMER_TIMEOUT )); then
  echo "  Warning: consumer did not quiesce within ${CONSUMER_TIMEOUT}s (${CUR_COUNT} events)"
fi

# Stop the consumer so db-analyser can open the ChainDB.
if kill -0 "$CONSUMER_PID" 2>/dev/null; then
  kill "$CONSUMER_PID" 2>/dev/null || true
  wait "$CONSUMER_PID" 2>/dev/null || true
fi

echo ""
echo "${BOLD}=== Consumer validation (db-analyser) ===${NC}"

DB_ANALYSER_OUT=$(db-analyser --db "$CONSUMER_DB" --config "$CONFIG" --count-blocks --v1-in-mem 2>&1) || true
BLOCK_COUNT=$(echo "$DB_ANALYSER_OUT" | grep -oP 'Counted \K[0-9]+' || echo "")

if [[ -z "$BLOCK_COUNT" ]]; then
  echo "  ${RED}FAIL${NC}: db-analyser did not report a block count"
  echo "--- db-analyser output ---"
  echo "$DB_ANALYSER_OUT"
  exit 1
fi

echo "  Consumer ImmutableDB: ${BLOCK_COUNT} blocks (expected ${EXPECTED_IMMUTABLE})"

if (( BLOCK_COUNT == EXPECTED_IMMUTABLE )); then
  echo "  ${GREEN}OK${NC}: Block count matches (total ${TOTAL_BLOCKS} - k ${SECURITY_PARAM} = ${EXPECTED_IMMUTABLE})"
else
  echo "  ${RED}FAIL${NC}: Expected ${EXPECTED_IMMUTABLE} blocks in ImmutableDB, got ${BLOCK_COUNT}"
  echo "--- db-analyser output ---"
  echo "$DB_ANALYSER_OUT"
  echo "--- consumer log (last 50 lines) ---"
  tail -50 "$TMPDIR/node.log" 2>/dev/null || true
  exit 1
fi

echo ""
echo "${GREEN}${BOLD}=== ALL CHECKS PASSED ===${NC}"
