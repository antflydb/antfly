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

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${DOCID_QUERY_MATRIX_OUT:-bench/results/docid-query-matrix/$STAMP}"
SMOKE="${DOCID_QUERY_MATRIX_SMOKE:-0}"
WARM_BUILD="${DOCID_QUERY_MATRIX_WARM_BUILD:-1}"
MAX_ORDINAL_RATIO="${DOCID_QUERY_MATRIX_MAX_ORDINAL_RATIO:-1.25}"
REQUIRE_PUBLIC_RESOLUTION_DELTA="${DOCID_QUERY_MATRIX_REQUIRE_PUBLIC_RESOLUTION_DELTA:-1}"

if [[ "$SMOKE" == "1" ]]; then
  TINY_DOCS="${DOCID_QUERY_MATRIX_TINY_DOCS:-128}"
  TINY_QUERIES="${DOCID_QUERY_MATRIX_TINY_QUERIES:-3}"
  TINY_REPEATS="${DOCID_QUERY_MATRIX_TINY_REPEATS:-2}"
  TINY_FILTER_SIZE="${DOCID_QUERY_MATRIX_TINY_FILTER_SIZE:-16}"
  TINY_SPARSE_DIMS="${DOCID_QUERY_MATRIX_TINY_SPARSE_DIMS:-16}"
  TINY_LIMIT="${DOCID_QUERY_MATRIX_TINY_LIMIT:-8}"

  SELECTIVE_DOCS="${DOCID_QUERY_MATRIX_SELECTIVE_DOCS:-256}"
  SELECTIVE_QUERIES="${DOCID_QUERY_MATRIX_SELECTIVE_QUERIES:-4}"
  SELECTIVE_REPEATS="${DOCID_QUERY_MATRIX_SELECTIVE_REPEATS:-2}"
  SELECTIVE_FILTER_SIZE="${DOCID_QUERY_MATRIX_SELECTIVE_FILTER_SIZE:-16}"
  SELECTIVE_SPARSE_DIMS="${DOCID_QUERY_MATRIX_SELECTIVE_SPARSE_DIMS:-64}"
  SELECTIVE_LIMIT="${DOCID_QUERY_MATRIX_SELECTIVE_LIMIT:-8}"

  BROAD_DOCS="${DOCID_QUERY_MATRIX_BROAD_DOCS:-384}"
  BROAD_QUERIES="${DOCID_QUERY_MATRIX_BROAD_QUERIES:-4}"
  BROAD_REPEATS="${DOCID_QUERY_MATRIX_BROAD_REPEATS:-2}"
  BROAD_FILTER_SIZE="${DOCID_QUERY_MATRIX_BROAD_FILTER_SIZE:-192}"
  BROAD_SPARSE_DIMS="${DOCID_QUERY_MATRIX_BROAD_SPARSE_DIMS:-32}"
  BROAD_LIMIT="${DOCID_QUERY_MATRIX_BROAD_LIMIT:-8}"
else
  TINY_DOCS="${DOCID_QUERY_MATRIX_TINY_DOCS:-1024}"
  TINY_QUERIES="${DOCID_QUERY_MATRIX_TINY_QUERIES:-8}"
  TINY_REPEATS="${DOCID_QUERY_MATRIX_TINY_REPEATS:-4}"
  TINY_FILTER_SIZE="${DOCID_QUERY_MATRIX_TINY_FILTER_SIZE:-128}"
  TINY_SPARSE_DIMS="${DOCID_QUERY_MATRIX_TINY_SPARSE_DIMS:-64}"
  TINY_LIMIT="${DOCID_QUERY_MATRIX_TINY_LIMIT:-16}"

  SELECTIVE_DOCS="${DOCID_QUERY_MATRIX_SELECTIVE_DOCS:-2048}"
  SELECTIVE_QUERIES="${DOCID_QUERY_MATRIX_SELECTIVE_QUERIES:-8}"
  SELECTIVE_REPEATS="${DOCID_QUERY_MATRIX_SELECTIVE_REPEATS:-3}"
  SELECTIVE_FILTER_SIZE="${DOCID_QUERY_MATRIX_SELECTIVE_FILTER_SIZE:-64}"
  SELECTIVE_SPARSE_DIMS="${DOCID_QUERY_MATRIX_SELECTIVE_SPARSE_DIMS:-256}"
  SELECTIVE_LIMIT="${DOCID_QUERY_MATRIX_SELECTIVE_LIMIT:-16}"

  BROAD_DOCS="${DOCID_QUERY_MATRIX_BROAD_DOCS:-2048}"
  BROAD_QUERIES="${DOCID_QUERY_MATRIX_BROAD_QUERIES:-8}"
  BROAD_REPEATS="${DOCID_QUERY_MATRIX_BROAD_REPEATS:-3}"
  BROAD_FILTER_SIZE="${DOCID_QUERY_MATRIX_BROAD_FILTER_SIZE:-1024}"
  BROAD_SPARSE_DIMS="${DOCID_QUERY_MATRIX_BROAD_SPARSE_DIMS:-64}"
  BROAD_LIMIT="${DOCID_QUERY_MATRIX_BROAD_LIMIT:-16}"
fi

BATCH_SIZE="${DOCID_QUERY_MATRIX_BATCH_SIZE:-256}"
BODY_REPEAT="${DOCID_QUERY_MATRIX_BODY_REPEAT:-1}"

mkdir -p "$OUT"

STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
COMBINED="$OUT/docid-query-matrix-combined.jsonl"
SUMMARY_JSONL="$OUT/docid-query-matrix-summary.jsonl"
: > "$STATUS_FILE"
: > "$COMMAND_FILE"
: > "$COMBINED"
: > "$SUMMARY_JSONL"

{
  echo "timestamp_utc=$STAMP"
  echo "root=$ROOT"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || true)"
  echo "git_status_porcelain_begin"
  git status --short || true
  echo "git_status_porcelain_end"
  echo "uname=$(uname -a)"
  echo "smoke=$SMOKE"
  echo "warm_build=$WARM_BUILD"
  echo "max_ordinal_ratio=$MAX_ORDINAL_RATIO"
  echo "require_public_resolution_delta=$REQUIRE_PUBLIC_RESOLUTION_DELTA"
  echo "batch_size=$BATCH_SIZE"
  echo "body_repeat=$BODY_REPEAT"
  echo "tiny_docs=$TINY_DOCS"
  echo "tiny_queries=$TINY_QUERIES"
  echo "tiny_repeats=$TINY_REPEATS"
  echo "tiny_filter_size=$TINY_FILTER_SIZE"
  echo "tiny_sparse_dims=$TINY_SPARSE_DIMS"
  echo "tiny_limit=$TINY_LIMIT"
  echo "selective_docs=$SELECTIVE_DOCS"
  echo "selective_queries=$SELECTIVE_QUERIES"
  echo "selective_repeats=$SELECTIVE_REPEATS"
  echo "selective_filter_size=$SELECTIVE_FILTER_SIZE"
  echo "selective_sparse_dims=$SELECTIVE_SPARSE_DIMS"
  echo "selective_limit=$SELECTIVE_LIMIT"
  echo "broad_docs=$BROAD_DOCS"
  echo "broad_queries=$BROAD_QUERIES"
  echo "broad_repeats=$BROAD_REPEATS"
  echo "broad_filter_size=$BROAD_FILTER_SIZE"
  echo "broad_sparse_dims=$BROAD_SPARSE_DIMS"
  echo "broad_limit=$BROAD_LIMIT"
} > "$OUT/environment.txt"

record_command() {
  local name="$1"
  shift
  printf "%s\t" "$name" >> "$COMMAND_FILE"
  printf "%q " "$@" >> "$COMMAND_FILE"
  printf "\n" >> "$COMMAND_FILE"
}

append_json_lines() {
  local name="$1"
  local stdout_file="$OUT/$name.stdout"
  local jsonl_file="$OUT/$name.jsonl"
  grep '^{.*}$' "$stdout_file" > "$jsonl_file"
  sed "s/^{/{\"case\":\"$name\",/" "$jsonl_file" >> "$COMBINED"
  grep '"event":"docid_query_bench_summary"' "$jsonl_file" |
    sed "s/^{/{\"case\":\"$name\",/" >> "$SUMMARY_JSONL"
}

run_case() {
  local name="$1"
  shift
  local args=("$@")
  local cmd=(zig build docid-query-bench -- "${args[@]}")
  echo "running $name"
  record_command "$name" "${cmd[@]}"
  local started ended rc
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  "${cmd[@]}" > "$OUT/$name.stdout" 2> "$OUT/$name.stderr"
  rc=$?
  set -e
  ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf "%s\t%s\t%s\t%s\n" "$name" "$rc" "$started" "$ended" >> "$STATUS_FILE"
  if [[ "$rc" != "0" ]]; then
    echo "failed $name rc=$rc"
    return "$rc"
  fi
  append_json_lines "$name"
}

run_bench_case() {
  local name="$1"
  local docs="$2"
  local queries="$3"
  local repeats="$4"
  local filter_size="$5"
  local sparse_dims="$6"
  local limit="$7"
  local args=(
    --docs "$docs"
    --queries "$queries"
    --repeats "$repeats"
    --filter-size "$filter_size"
    --batch-size "$BATCH_SIZE"
    --sparse-dims "$sparse_dims"
    --with-sparse
    --limit "$limit"
    --body-repeat "$BODY_REPEAT"
    --max-ordinal-ratio "$MAX_ORDINAL_RATIO"
  )
  if [[ "$REQUIRE_PUBLIC_RESOLUTION_DELTA" == "1" ]]; then
    args+=(--require-public-resolution-delta)
  fi
  run_case "$name" "${args[@]}"
}

if [[ "$WARM_BUILD" == "1" ]]; then
  echo "warming docid-query-bench"
  zig build docid-query-bench-build
fi

run_bench_case tiny_baseline "$TINY_DOCS" "$TINY_QUERIES" "$TINY_REPEATS" "$TINY_FILTER_SIZE" "$TINY_SPARSE_DIMS" "$TINY_LIMIT"
run_bench_case selective_small_filter "$SELECTIVE_DOCS" "$SELECTIVE_QUERIES" "$SELECTIVE_REPEATS" "$SELECTIVE_FILTER_SIZE" "$SELECTIVE_SPARSE_DIMS" "$SELECTIVE_LIMIT"
run_bench_case broad_large_filter "$BROAD_DOCS" "$BROAD_QUERIES" "$BROAD_REPEATS" "$BROAD_FILTER_SIZE" "$BROAD_SPARSE_DIMS" "$BROAD_LIMIT"

echo "wrote $OUT"
