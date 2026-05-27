#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ZIG_BUILD_FILE="$ROOT/zig/build.zig"
ZIG_CACHE_DIR="$ROOT/zig/.zig-cache"
ZIG_GLOBAL_CACHE_DIR="${DOCID_PRODUCTION_MATRIX_ZIG_GLOBAL_CACHE_DIR:-$ROOT/zig/.zig-global-cache}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${DOCID_PRODUCTION_MATRIX_OUT:-bench/results/docid-production-readiness/$STAMP}"
RUN_SCALE="${DOCID_PRODUCTION_MATRIX_RUN_SCALE:-0}"
RUN_E2E="${DOCID_PRODUCTION_MATRIX_RUN_E2E:-0}"
RUN_OLD_NEW="${DOCID_PRODUCTION_MATRIX_RUN_OLD_NEW:-0}"
OLD_ANTFLY_BIN="${DOCID_PRODUCTION_MATRIX_OLD_ANTFLY_BIN:-}"
NEW_ANTFLY_BIN="${DOCID_PRODUCTION_MATRIX_NEW_ANTFLY_BIN:-}"
E2E_PROJECT="${DOCID_PRODUCTION_MATRIX_E2E_PROJECT:-zig/e2e/antfly}"
E2E_AUTH_FILTER="${DOCID_PRODUCTION_MATRIX_E2E_AUTH_FILTER:-stateful_auth_enforces_table_permissions or stateful_auth_enforces_row_filters_on_lookup_and_query}"
PERF_DOCS="${DOCID_PRODUCTION_MATRIX_PERF_DOCS:-300000}"
PERF_QUERIES="${DOCID_PRODUCTION_MATRIX_PERF_QUERIES:-2}"
PERF_REPEATS="${DOCID_PRODUCTION_MATRIX_PERF_REPEATS:-1}"

mkdir -p "$OUT"

STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
ENV_FILE="$OUT/environment.txt"

: >"$STATUS_FILE"
: >"$COMMAND_FILE"

{
  printf 'timestamp_utc=%s\n' "$STAMP"
  printf 'root=%s\n' "$ROOT"
  printf 'run_scale=%s\n' "$RUN_SCALE"
  printf 'run_e2e=%s\n' "$RUN_E2E"
  printf 'run_old_new=%s\n' "$RUN_OLD_NEW"
  printf 'old_antfly_bin=%s\n' "${OLD_ANTFLY_BIN:-<unset>}"
  printf 'new_antfly_bin=%s\n' "${NEW_ANTFLY_BIN:-<unset>}"
  printf 'perf_docs=%s\n' "$PERF_DOCS"
  printf 'perf_queries=%s\n' "$PERF_QUERIES"
  printf 'perf_repeats=%s\n' "$PERF_REPEATS"
  git rev-parse --show-toplevel
  git rev-parse HEAD
  git status --short
} >"$ENV_FILE" 2>&1 || true

record_command() {
  local name="$1"
  shift
  {
    printf '%s\t' "$name"
    printf '%q ' "$@"
    printf '\n'
  } >>"$COMMAND_FILE"
}

run_case() {
  local name="$1"
  shift
  local log_file="$OUT/${name}.log"
  record_command "$name" "$@"
  printf 'running\t%s\n' "$name"
  if "$@" >"$log_file" 2>&1; then
    printf '%s\tok\t%s\n' "$name" "$log_file" >>"$STATUS_FILE"
  else
    local status=$?
    printf '%s\tfail:%s\t%s\n' "$name" "$status" "$log_file" >>"$STATUS_FILE"
    return "$status"
  fi
}

run_case docid-lifecycle-test zig build \
  --build-file "$ZIG_BUILD_FILE" \
  --cache-dir "$ZIG_CACHE_DIR" \
  --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
  docid-lifecycle-test

run_case docid-operational-hardening-test zig build \
  --build-file "$ZIG_BUILD_FILE" \
  --cache-dir "$ZIG_CACHE_DIR" \
  --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
  docid-operational-hardening-test

if [[ "$RUN_SCALE" == "1" ]]; then
  run_case docid-perf-scale-matrix env \
    DOCID_PERF_MATRIX_DOCS="$PERF_DOCS" \
    DOCID_PERF_MATRIX_QUERIES="$PERF_QUERIES" \
    DOCID_PERF_MATRIX_REPEATS="$PERF_REPEATS" \
    DOCID_PERF_MATRIX_OUT="$OUT/docid-perf-scale-matrix" \
    scripts/run_docid_perf_matrix.sh
fi

if [[ "$RUN_E2E" == "1" ]]; then
  run_case current-auth-e2e env \
    ANTFLY_BIN="${NEW_ANTFLY_BIN:-./zig/zig-out/bin/antfly}" \
    ANTFLY_E2E_PRESERVE_ROOT="${ANTFLY_E2E_PRESERVE_ROOT:-0}" \
    uv run --project "$E2E_PROJECT" pytest -q -x -s "$E2E_PROJECT/test_auth.py" -k "$E2E_AUTH_FILTER"
fi

if [[ "$RUN_OLD_NEW" == "1" ]]; then
  if [[ -z "$OLD_ANTFLY_BIN" || -z "$NEW_ANTFLY_BIN" ]]; then
    printf 'DOCID_PRODUCTION_MATRIX_RUN_OLD_NEW=1 requires DOCID_PRODUCTION_MATRIX_OLD_ANTFLY_BIN and DOCID_PRODUCTION_MATRIX_NEW_ANTFLY_BIN\n' >&2
    exit 2
  fi
  run_case old-binary-auth-e2e env \
    ANTFLY_BIN="$OLD_ANTFLY_BIN" \
    ANTFLY_E2E_PRESERVE_ROOT="${ANTFLY_E2E_PRESERVE_ROOT:-0}" \
    uv run --project "$E2E_PROJECT" pytest -q -x -s "$E2E_PROJECT/test_auth.py" -k "$E2E_AUTH_FILTER"
  run_case new-binary-auth-e2e env \
    ANTFLY_BIN="$NEW_ANTFLY_BIN" \
    ANTFLY_E2E_PRESERVE_ROOT="${ANTFLY_E2E_PRESERVE_ROOT:-0}" \
    uv run --project "$E2E_PROJECT" pytest -q -x -s "$E2E_PROJECT/test_auth.py" -k "$E2E_AUTH_FILTER"
fi

printf 'DOCID production readiness matrix complete: %s\n' "$OUT"
