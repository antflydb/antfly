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
ZIG_GLOBAL_CACHE_DIR="${DOCID_LIFECYCLE_MATRIX_ZIG_GLOBAL_CACHE_DIR:-$ROOT/zig/.zig-global-cache}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${DOCID_LIFECYCLE_MATRIX_OUT:-bench/results/docid-lifecycle-matrix/$STAMP}"
SMOKE="${DOCID_LIFECYCLE_MATRIX_SMOKE:-1}"
RUN_QUERY_MATRIX="${DOCID_LIFECYCLE_MATRIX_RUN_QUERY:-1}"

mkdir -p "$OUT"

STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
ENV_FILE="$OUT/environment.txt"

: >"$STATUS_FILE"
: >"$COMMAND_FILE"

{
  printf 'timestamp_utc=%s\n' "$STAMP"
  printf 'root=%s\n' "$ROOT"
  printf 'smoke=%s\n' "$SMOKE"
  printf 'run_query_matrix=%s\n' "$RUN_QUERY_MATRIX"
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

run_case docid-lifecycle-test zig build --build-file "$ZIG_BUILD_FILE" --cache-dir "$ZIG_CACHE_DIR" --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" docid-lifecycle-test
run_case lib-db-query-test zig build --build-file "$ZIG_BUILD_FILE" --cache-dir "$ZIG_CACHE_DIR" --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" lib-db-query-test
run_case lib-storage-docid-focused zig build --build-file "$ZIG_BUILD_FILE" --cache-dir "$ZIG_CACHE_DIR" --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" lib-storage-test \
  -- \
  --test-filter "db lsm primary compaction preserves doc identity ordinals" \
  --test-filter "db allocates final document ordinal with all index families present" \
  --test-filter "db text compaction preserves ordinal filters across reopen" \
  --test-filter "structured filter doc set cache separates shared namespace generation keys"

if [[ "$RUN_QUERY_MATRIX" == "1" ]]; then
  run_case docid-query-matrix env \
    DOCID_QUERY_MATRIX_SMOKE="$SMOKE" \
    DOCID_QUERY_MATRIX_OUT="$OUT/docid-query-matrix" \
    scripts/run_docid_query_matrix.sh
fi

printf 'DOCID lifecycle matrix complete: %s\n' "$OUT"
