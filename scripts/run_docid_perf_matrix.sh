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
ZIG_GLOBAL_CACHE_DIR="${DOCID_PERF_MATRIX_ZIG_GLOBAL_CACHE_DIR:-$ROOT/zig/.zig-global-cache}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${DOCID_PERF_MATRIX_OUT:-bench/results/docid-perf-matrix/$STAMP}"
DOCS="${DOCID_PERF_MATRIX_DOCS:-100000}"
DIMS="${DOCID_PERF_MATRIX_DIMS:-64}"
QUERIES="${DOCID_PERF_MATRIX_QUERIES:-2}"
REPEATS="${DOCID_PERF_MATRIX_REPEATS:-1}"
K="${DOCID_PERF_MATRIX_K:-20}"
BATCH_SIZE="${DOCID_PERF_MATRIX_BATCH_SIZE:-1000}"
MODE="${DOCID_PERF_MATRIX_MODE:-handler}"
SYNC_LEVEL="${DOCID_PERF_MATRIX_SYNC_LEVEL:-full_index}"
LOAD_PROGRESS_INTERVAL="${DOCID_PERF_MATRIX_LOAD_PROGRESS_INTERVAL:-25000}"
RUN_BUILD="${DOCID_PERF_MATRIX_WARM_BUILD:-1}"
REQUIRE_SYMBOLIC_PROFILE="${DOCID_PERF_MATRIX_REQUIRE_SYMBOLIC_PROFILE:-0}"

mkdir -p "$OUT"

STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
SUMMARY_JSONL="$OUT/public-query-summary.jsonl"
ENV_FILE="$OUT/environment.txt"

: >"$STATUS_FILE"
: >"$COMMAND_FILE"
: >"$SUMMARY_JSONL"

{
  printf 'timestamp_utc=%s\n' "$STAMP"
  printf 'root=%s\n' "$ROOT"
  printf 'docs=%s\n' "$DOCS"
  printf 'dims=%s\n' "$DIMS"
  printf 'queries=%s\n' "$QUERIES"
  printf 'repeats=%s\n' "$REPEATS"
  printf 'k=%s\n' "$K"
  printf 'batch_size=%s\n' "$BATCH_SIZE"
  printf 'mode=%s\n' "$MODE"
  printf 'sync_level=%s\n' "$SYNC_LEVEL"
  printf 'require_symbolic_profile=%s\n' "$REQUIRE_SYMBOLIC_PROFILE"
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
  local stdout_file="$OUT/$name.stdout"
  local stderr_file="$OUT/$name.stderr"
  record_command "$name" "$@"
  printf 'running\t%s\n' "$name"
  if "$@" >"$stdout_file" 2>"$stderr_file"; then
    printf '%s\tok\t%s\t%s\n' "$name" "$stdout_file" "$stderr_file" >>"$STATUS_FILE"
    grep '"event":"public_query_guardrail_summary"' "$stderr_file" |
      sed "s/^{/{\"case\":\"$name\",/" >>"$SUMMARY_JSONL" || true
  else
    local status=$?
    printf '%s\tfail:%s\t%s\t%s\n' "$name" "$status" "$stdout_file" "$stderr_file" >>"$STATUS_FILE"
    return "$status"
  fi
}

base_cmd=(
  zig build
  --build-file "$ZIG_BUILD_FILE"
  --cache-dir "$ZIG_CACHE_DIR"
  --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR"
  public-query-guardrail
  --
  --mode "$MODE"
  --docs "$DOCS"
  --dims "$DIMS"
  --queries "$QUERIES"
  --repeats "$REPEATS"
  --k "$K"
  --batch-size "$BATCH_SIZE"
  --sync-level "$SYNC_LEVEL"
  --load-progress-interval "$LOAD_PROGRESS_INTERVAL"
)

if [[ "$RUN_BUILD" == "1" ]]; then
  run_case warm_build zig build \
    --build-file "$ZIG_BUILD_FILE" \
    --cache-dir "$ZIG_CACHE_DIR" \
    --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
    public-query-guardrail-build
fi

run_case full_text "${base_cmd[@]}" --query-shape full-text
run_case dense_filter "${base_cmd[@]}" --query-shape dense-filter
run_case sparse_filter "${base_cmd[@]}" --query-shape sparse-filter --with-sparse
run_case graph_expand "${base_cmd[@]}" --query-shape graph-expand --with-graph

algebraic_args=("${base_cmd[@]}" --query-shape algebraic-filter --with-algebraic)
hybrid_args=("${base_cmd[@]}" --query-shape hybrid-composed --with-sparse --with-algebraic)
if [[ "$REQUIRE_SYMBOLIC_PROFILE" == "1" ]]; then
  algebraic_args+=(--require-symbolic-profile)
  hybrid_args+=(--require-symbolic-profile)
fi

run_case algebraic_filter "${algebraic_args[@]}"
run_case hybrid_composed "${hybrid_args[@]}"

printf 'DOCID performance matrix complete: %s\n' "$OUT"
