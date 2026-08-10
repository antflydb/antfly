#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
binary="${1:-$repo_root/zig/zig-out/bin/antfly}"

if [[ ! -x "$binary" ]]; then
  echo "production runtime smoke binary is not executable: $binary" >&2
  exit 2
fi

smoke_root="$(mktemp -d "${TMPDIR:-/tmp}/antfly-production-runtime-smoke.XXXXXX")"
server_pid=""

cleanup_server() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  server_pid=""
}

cleanup() {
  cleanup_server
  rm -rf -- "$smoke_root"
}
trap cleanup EXIT INT TERM

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_ready() {
  local profile="$1"
  local port="$2"
  local log="$3"
  local attempt
  for attempt in $(seq 1 120); do
    if curl --fail --silent "http://127.0.0.1:${port}/readyz" | grep -q '"status":"ready"'; then
      return 0
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "production ${profile} runtime exited before becoming ready" >&2
      cat "$log" >&2
      return 1
    fi
    sleep 1
  done
  echo "production ${profile} runtime did not become ready within 120 seconds" >&2
  cat "$log" >&2
  return 1
}

run_profile() {
  local profile="$1"
  shift
  local port
  port="$(free_port)"
  local log="$smoke_root/${profile}.log"

  "$binary" standalone \
    --host 127.0.0.1 \
    --port "$port" \
    --health false \
    "$@" >"$log" 2>&1 &
  server_pid="$!"
  wait_ready "$profile" "$port" "$log"
  cleanup_server
  echo "production ${profile} runtime ready smoke passed"
}

run_profile local --data-dir "$smoke_root/local"
run_profile lite \
  --data-dir "$smoke_root/lite-data" \
  --storage-engine lite \
  --storage-path "$smoke_root/antfly.aflite" \
  --fsync false
