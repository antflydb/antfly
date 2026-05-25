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
ZIG_GLOBAL_CACHE_DIR="${DOCID_OPERATIONAL_MATRIX_ZIG_GLOBAL_CACHE_DIR:-$ROOT/zig/.zig-global-cache}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${DOCID_OPERATIONAL_MATRIX_OUT:-bench/results/docid-operational-hardening/$STAMP}"
RUN_CHAOS="${DOCID_OPERATIONAL_MATRIX_RUN_CHAOS:-1}"
RUN_SCALE="${DOCID_OPERATIONAL_MATRIX_RUN_SCALE:-0}"
RUN_FULL_TARGET="${DOCID_OPERATIONAL_MATRIX_RUN_FULL_TARGET:-0}"
PERF_DOCS="${DOCID_OPERATIONAL_MATRIX_PERF_DOCS:-100000}"
PERF_QUERIES="${DOCID_OPERATIONAL_MATRIX_PERF_QUERIES:-2}"
PERF_REPEATS="${DOCID_OPERATIONAL_MATRIX_PERF_REPEATS:-1}"

mkdir -p "$OUT"

STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
ENV_FILE="$OUT/environment.txt"

: >"$STATUS_FILE"
: >"$COMMAND_FILE"

{
  printf 'timestamp_utc=%s\n' "$STAMP"
  printf 'root=%s\n' "$ROOT"
  printf 'run_chaos=%s\n' "$RUN_CHAOS"
  printf 'run_scale=%s\n' "$RUN_SCALE"
  printf 'run_full_target=%s\n' "$RUN_FULL_TARGET"
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

if [[ "$RUN_FULL_TARGET" == "1" ]]; then
  run_case docid-operational-hardening-test zig build \
    --build-file "$ZIG_BUILD_FILE" \
    --cache-dir "$ZIG_CACHE_DIR" \
    --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
    docid-operational-hardening-test
else
  run_case docid-lifecycle-test zig build \
    --build-file "$ZIG_BUILD_FILE" \
    --cache-dir "$ZIG_CACHE_DIR" \
    --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
    docid-lifecycle-test

  if [[ "$RUN_CHAOS" == "1" ]]; then
    run_case lib-metadata-transition-chaos-test zig build \
      --build-file "$ZIG_BUILD_FILE" \
      --cache-dir "$ZIG_CACHE_DIR" \
      --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
      lib-metadata-transition-chaos-test
    run_case lib-metadata-public-chaos-test zig build \
      --build-file "$ZIG_BUILD_FILE" \
      --cache-dir "$ZIG_CACHE_DIR" \
      --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
      lib-metadata-public-chaos-test
    run_case lib-lsm-backend-chaos-test zig build \
      --build-file "$ZIG_BUILD_FILE" \
      --cache-dir "$ZIG_CACHE_DIR" \
      --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
      lib-lsm-backend-chaos-test
  fi
fi

if [[ "$RUN_SCALE" == "1" ]]; then
  run_case docid-perf-matrix env \
    DOCID_PERF_MATRIX_DOCS="$PERF_DOCS" \
    DOCID_PERF_MATRIX_QUERIES="$PERF_QUERIES" \
    DOCID_PERF_MATRIX_REPEATS="$PERF_REPEATS" \
    DOCID_PERF_MATRIX_OUT="$OUT/docid-perf-matrix" \
    scripts/run_docid_perf_matrix.sh
fi

printf 'DOCID operational hardening matrix complete: %s\n' "$OUT"
