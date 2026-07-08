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
ZIG_ROOT="$ROOT/zig"
ZIG_CACHE_DIR="$ROOT/zig/.zig-cache"
ZIG_GLOBAL_CACHE_DIR="${SORT_QUERY_MATRIX_ZIG_GLOBAL_CACHE_DIR:-$ROOT/zig/.zig-global-cache}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${SORT_QUERY_MATRIX_OUT:-$ROOT/bench/results/sort-query-matrix/$STAMP}"
case "$OUT" in
  /*) ;;
  *) OUT="$ROOT/$OUT" ;;
esac
SMOKE="${SORT_QUERY_MATRIX_SMOKE:-0}"
WARM_BUILD="${SORT_QUERY_MATRIX_WARM_BUILD:-1}"
MODE="${SORT_QUERY_MATRIX_MODE:-handler}"
SYNC_LEVEL="${SORT_QUERY_MATRIX_SYNC_LEVEL:-full_index}"

if [[ "$SMOKE" == "1" ]]; then
  DOCS_SMALL="${SORT_QUERY_MATRIX_DOCS_SMALL:-256}"
  DOCS_LARGE="${SORT_QUERY_MATRIX_DOCS_LARGE:-768}"
  QUERIES="${SORT_QUERY_MATRIX_QUERIES:-4}"
  REPEATS="${SORT_QUERY_MATRIX_REPEATS:-2}"
  LIMIT_SMALL="${SORT_QUERY_MATRIX_LIMIT_SMALL:-8}"
  LIMIT_LARGE="${SORT_QUERY_MATRIX_LIMIT_LARGE:-16}"
  BATCH_SIZE="${SORT_QUERY_MATRIX_BATCH_SIZE:-128}"
else
  DOCS_SMALL="${SORT_QUERY_MATRIX_DOCS_SMALL:-2048}"
  DOCS_LARGE="${SORT_QUERY_MATRIX_DOCS_LARGE:-8192}"
  QUERIES="${SORT_QUERY_MATRIX_QUERIES:-16}"
  REPEATS="${SORT_QUERY_MATRIX_REPEATS:-4}"
  LIMIT_SMALL="${SORT_QUERY_MATRIX_LIMIT_SMALL:-16}"
  LIMIT_LARGE="${SORT_QUERY_MATRIX_LIMIT_LARGE:-64}"
  BATCH_SIZE="${SORT_QUERY_MATRIX_BATCH_SIZE:-512}"
fi

mkdir -p "$OUT"
cd "$ZIG_ROOT"

STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
COMBINED="$OUT/sort-query-matrix-combined.jsonl"
SORT_PROFILE_JSONL="$OUT/sort-query-matrix-sort-profile.jsonl"
: > "$STATUS_FILE"
: > "$COMMAND_FILE"
: > "$COMBINED"
: > "$SORT_PROFILE_JSONL"

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
  echo "mode=$MODE"
  echo "sync_level=$SYNC_LEVEL"
  echo "docs_small=$DOCS_SMALL"
  echo "docs_large=$DOCS_LARGE"
  echo "queries=$QUERIES"
  echo "repeats=$REPEATS"
  echo "limit_small=$LIMIT_SMALL"
  echo "limit_large=$LIMIT_LARGE"
  echo "batch_size=$BATCH_SIZE"
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
  local stderr_file="$OUT/$name.stderr"
  local jsonl_file="$OUT/$name.jsonl"
  grep -h '^{.*}$' "$stdout_file" "$stderr_file" > "$jsonl_file" || true
  sed "s/^{/{\"case\":\"$name\",/" "$jsonl_file" >> "$COMBINED"
  grep '"event":"public_query_sort_profile"' "$jsonl_file" |
    sed "s/^{/{\"case\":\"$name\",/" >> "$SORT_PROFILE_JSONL" || true
}

run_case() {
  local name="$1"
  local query_shape="$2"
  local docs="$3"
  local limit="$4"
  local cmd=(
    zig build
    --cache-dir "$ZIG_CACHE_DIR"
    --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR"
    public-query-guardrail
    --
    --mode "$MODE"
    --query-shape "$query_shape"
    --docs "$docs"
    --queries "$QUERIES"
    --repeats "$REPEATS"
    --k "$limit"
    --batch-size "$BATCH_SIZE"
    --sync-level "$SYNC_LEVEL"
  )
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

if [[ "$WARM_BUILD" == "1" ]]; then
  echo "warming public-query-guardrail"
  zig build --cache-dir "$ZIG_CACHE_DIR" --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" public-query-guardrail-build
fi

run_case index_sort_first_page_small exact-sort-index-sort "$DOCS_SMALL" "$LIMIT_SMALL"
run_case index_sort_first_page_large exact-sort-index-sort "$DOCS_LARGE" "$LIMIT_SMALL"
run_case doc_values_top_n exact-sort-match-all "$DOCS_SMALL" "$LIMIT_SMALL"
run_case doc_values_top_n_large_page exact-sort-match-all "$DOCS_SMALL" "$LIMIT_LARGE"
run_case index_sort_filtered exact-sort-index-sort-filter "$DOCS_LARGE" "$LIMIT_SMALL"
run_case full_text_exact_sort exact-sort-full-text "$DOCS_SMALL" "$LIMIT_SMALL"

echo "wrote $OUT"
