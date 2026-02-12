#!/usr/bin/env bash

set -euo pipefail


BOLD=$'\033[1m'
NC=$'\033[0m'

REQUIRED_TOOLS=(cardano-node genesis-sync-accelerator db-analyser python3)

for cmd in "${REQUIRED_TOOLS[@]}"; do
  path=$(command -v "$cmd" || true)
  if [ -z "$path" ]; then
    path="not found"
  fi
  printf "  %-30s\t%s\n" "${BOLD}${cmd}${NC}" "$path"
done

