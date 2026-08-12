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
    if curl --max-time 10 --fail --silent "http://127.0.0.1:${port}/readyz" | grep -q '"status":"ready"'; then
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

assert_http_status() {
  local profile="$1"
  local expected="$2"
  local method="$3"
  local url="$4"
  local body="${5:-}"
  local response_file="$smoke_root/${profile}-response.txt"
  local status

  if [[ -n "$body" ]]; then
    status="$(curl --max-time 10 --silent --show-error --output "$response_file" --write-out '%{http_code}' \
      --request "$method" --header 'content-type: application/json' --data-binary "$body" "$url")"
  else
    status="$(curl --max-time 10 --silent --show-error --output "$response_file" --write-out '%{http_code}' \
      --request "$method" "$url")"
  fi
  if [[ "$status" != "$expected" ]]; then
    echo "production ${profile} expected HTTP ${expected} from ${method} ${url}, got ${status}" >&2
    cat "$response_file" >&2
    return 1
  fi
}

exercise_post_handler_init_failure() {
  local port="$1"
  local log="$smoke_root/post-handler-init-failure.log"

  # The listener lease is acquired after the API handler runtime is initialized.
  # Reusing the live server's port forces that startup path to unwind and catches
  # double-destroy regressions under the real ReleaseFast runtime.
  if "$binary" standalone \
    --host 127.0.0.1 \
    --port "$port" \
    --health false \
    --data-dir "$smoke_root/conflicting-listener" >"$log" 2>&1; then
    echo "production startup unexpectedly accepted an already leased port" >&2
    return 1
  fi
  if grep -Eiq 'panic|double free|segmentation fault' "$log"; then
    echo "production startup cleanup failed after handler initialization" >&2
    cat "$log" >&2
    return 1
  fi
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
  assert_http_status "$profile-missing-table" 404 GET \
    "http://127.0.0.1:${port}/db/v1/tables/does-not-exist/documents/missing"
  assert_http_status "$profile-invalid-query" 400 POST \
    "http://127.0.0.1:${port}/db/v1/tables/does-not-exist/query" '{'
  assert_http_status "$profile-invalid-restore" 400 POST \
    "http://127.0.0.1:${port}/db/v1/tables/does-not-exist/restore" '{}'
  assert_http_status "$profile-inference-models" 200 GET \
    "http://127.0.0.1:${port}/ai/v1/models"
  if [[ "$profile" == "local" ]]; then
    exercise_post_handler_init_failure "$port"
  fi
  cleanup_server
  echo "production ${profile} runtime boundary smoke passed"
}

run_profile local --data-dir "$smoke_root/local"
run_profile lite \
  --data-dir "$smoke_root/lite-data" \
  --storage-engine lite \
  --storage-path "$smoke_root/antfly.aflite" \
  --fsync false
