#!/usr/bin/env bash
# Shared helpers for integration tests.
RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
BOLD=$'\033[1m'
NC=$'\033[0m'

# wait_for_port <port> <timeout_seconds> [label]
#   Block until something is listening on <port>, or fail.
wait_for_port() {
  local port="$1" timeout="$2" label="${3:-service}"
  local elapsed=0
  while ! ss -tlnp 2>/dev/null | grep -q ":${port} "; do
    if (( elapsed >= timeout )); then
      echo "${RED}$label did not start on port $port within ${timeout}s${NC}"
      return 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  echo "  $label listening on port $port"
}
