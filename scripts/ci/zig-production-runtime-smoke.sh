#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
  local smoke_exit=$?
  cleanup_server
  if [[ "$smoke_exit" != 0 ]]; then
    local log
    for log in "$smoke_root"/*.log; do
      [[ -f "$log" ]] || continue
      echo "production runtime log: ${log##*/}" >&2
      cat "$log" >&2
    done
  fi
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
  # double-destroy regressions under the real fast runtime.
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
  # Exercise catalog writes and ordered listing through the production storage
  # boundary, including the lite system-store adapter. A fresh-start readiness
  # check alone does not verify that persisted catalog indexes survive reopen.
  local catalog_url="http://127.0.0.1:${port}/db/v1/databases/runtime_smoke"
  local tables_url="${catalog_url}/namespaces/public/tables"
  assert_http_status "$profile-catalog-database" 201 POST "$catalog_url" '{}'
  assert_http_status "$profile-catalog-table" 201 POST "$tables_url/events" '{}'
  # A caller-owned Lite backend and the compiled physical engine have distinct
  # Zig error domains. Cover absent reads, batch writes and durable reopen.
  assert_http_status "$profile-document-missing" 404 GET "$tables_url/events/documents/smoke"
  assert_http_status "$profile-document-write" 201 POST "$tables_url/events/batch" \
    '{"inserts":{"smoke":{"value":7}},"sync_level":"write"}'
  assert_http_status "$profile-document-before" 200 GET "$tables_url/events/documents/smoke"
  assert_http_status "$profile-catalog-before" 200 GET "$tables_url"
  cleanup_server
  "$binary" standalone \
    --host 127.0.0.1 \
    --port "$port" \
    --health false \
    "$@" >"$log" 2>&1 &
  server_pid="$!"
  wait_ready "$profile-reopen" "$port" "$log"
  assert_http_status "$profile-catalog-after" 200 GET "$tables_url"
  assert_http_status "$profile-document-after" 200 GET "$tables_url/events/documents/smoke"
  python3 -c '
import json, sys
before, after = (json.load(open(path)) for path in sys.argv[1:])
assert before == after == {"value": 7}, (before, after)
' "$smoke_root/$profile-document-before-response.txt" "$smoke_root/$profile-document-after-response.txt"

  python3 -c '
import json, sys
before, after = (json.load(open(path)) for path in sys.argv[1:])
assert len(before) == len(after) == 1, (before, after)
assert before[0]["table_id"] == after[0]["table_id"], (before, after)
' "$smoke_root/$profile-catalog-before-response.txt" "$smoke_root/$profile-catalog-after-response.txt"
  cleanup_server
  echo "production ${profile} runtime boundary smoke passed"
}

run_profile local --data-dir "$smoke_root/local"
run_profile lite \
  --data-dir "$smoke_root/lite-data" \
  --storage-engine lite \
  --storage-path "$smoke_root/antfly.aflite" \
  --fsync false

# Exercise the compiled owner boundary: monolithic unit tests do not call
# adoptEmbeddedLiteRootFromKernelIfNeeded. The adopted table and its range must
# be journaled, survive restart, and travel with a checkpoint to a new data dir.
run_lite_adoption() {
  local source="$smoke_root/embedded.aflite"
  local copy="$smoke_root/embedded-copy.aflite"
  local port log phase path
  printf '%s\n' '{"inserts":{"portable":{"body":"snapshot portability","edited_locally":true}},"sync_level":"full_index"}' >"$smoke_root/embedded-write.json"
  printf '%s\n' '{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"body":{"type":"string"}},"required":["body"]}}}}' >"$smoke_root/embedded-schema.json"
  "$binary" lite init "$source" >"$smoke_root/embedded-init.log" 2>&1
  "$binary" lite schema set "$source" --file "$smoke_root/embedded-schema.json" >"$smoke_root/embedded-schema.log" 2>&1
  "$binary" lite batch "$source" --file "$smoke_root/embedded-write.json" >"$smoke_root/embedded-write.log" 2>&1
  for phase in adopt reopen copy; do
    port="$(free_port)"
    log="$smoke_root/embedded-${phase}.log"
    path="$source"
    [[ "$phase" != copy ]] || path="$copy"
    "$binary" standalone --host 127.0.0.1 --port "$port" --health false \
      --storage-engine lite --storage-path "$path" \
      --data-dir "$smoke_root/embedded-data-${phase}" >"$log" 2>&1 &
    server_pid="$!"
    wait_ready "embedded-${phase}" "$port" "$log"
    assert_http_status "embedded-${phase}-tables" 200 GET "http://127.0.0.1:${port}/db/v1/tables"
    assert_http_status "embedded-${phase}-document" 200 GET "http://127.0.0.1:${port}/db/v1/tables/default/documents/portable"
    assert_http_status "embedded-${phase}-table" 200 GET "http://127.0.0.1:${port}/db/v1/tables/default"
    assert_http_status "embedded-${phase}-query" 200 POST "http://127.0.0.1:${port}/db/v1/tables/default/query" \
      '{"full_text_search":{"match":"portability","field":"body"},"limit":10}'
    python3 - "$smoke_root/embedded-schema.json" "$smoke_root/embedded-${phase}-table-response.txt" "$smoke_root/embedded-${phase}-query-response.txt" <<'PY'
import json, sys
schema, table, result = (json.load(open(path)) for path in sys.argv[1:])
assert table["schema"] == schema, table
assert [row["_id"] for row in result["responses"][0]["hits"]["hits"]] == ["portable"], result
PY
    python3 - "$smoke_root/embedded-${phase}-tables-response.txt" "$smoke_root/embedded-${phase}-document-response.txt" <<'PY'
import json, sys
tables, document = (json.load(open(path)) for path in sys.argv[1:])
assert len(tables) == 1 and tables[0]["name"] == "default", tables
assert document == {"body": "snapshot portability", "edited_locally": True}, document
PY
    if [[ "$phase" == reopen ]]; then
      "$binary" lite snapshot "$source" --out "$copy" >"$smoke_root/embedded-snapshot.log" 2>&1
    fi
    cleanup_server
    if "$binary" lite schema set "$path" --file "$smoke_root/embedded-schema.json" >"$smoke_root/embedded-${phase}-offline-schema.log" 2>&1; then
      echo "adopted Lite file accepted an offline catalog mutation" >&2
      return 1
    fi
    if ! grep -q 'StandaloneLiteCatalogRequiresApi' "$smoke_root/embedded-${phase}-offline-schema.log"; then
      cat "$smoke_root/embedded-${phase}-offline-schema.log" >&2
      return 1
    fi
    "$binary" lite batch "$path" --file "$smoke_root/embedded-write.json" >"$smoke_root/embedded-${phase}-offline-write.log" 2>&1
    ANTFLY_URL='://invalid-server-url' "$binary" backup inspect --input "$path" >"$smoke_root/embedded-${phase}-inspection.json"
    python3 - "$smoke_root/embedded-${phase}-inspection.json" <<'PYREPORT'
import json, sys
report = json.load(open(sys.argv[1]))
assert report["completeness"]["scan_complete"], report
assert report["compatibility"]["verdict"] == "unknown", report
PYREPORT
  done
  echo "production Lite embedded adoption, catalog ownership, offline inspection, reopen and snapshot smoke passed"
}

run_lite_adoption
