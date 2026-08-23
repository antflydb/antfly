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
ZIG_CACHE_DIR="${RESOURCE_CACHE_MATRIX_ZIG_CACHE_DIR:-$ZIG_ROOT/.zig-cache}"
ZIG_GLOBAL_CACHE_DIR="${RESOURCE_CACHE_MATRIX_ZIG_GLOBAL_CACHE_DIR:-$ZIG_ROOT/.zig-global-cache}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${RESOURCE_CACHE_MATRIX_OUT:-$ZIG_ROOT/bench/results/resource-manager-cache-matrix/$STAMP}"
SMOKE="${RESOURCE_CACHE_MATRIX_SMOKE:-0}"
OPTIMIZE="${RESOURCE_CACHE_MATRIX_OPTIMIZE:-ReleaseFast}"
RESUME="${RESOURCE_CACHE_MATRIX_RESUME:-0}"

if [[ "$SMOKE" == "1" ]]; then
  SCALE_SIZES="${RESOURCE_CACHE_MATRIX_SCALE_SIZES:-256 1024}"
  ENDPOINT_SIZES="${RESOURCE_CACHE_MATRIX_ENDPOINT_SIZES:-256}"
  MEMORY_BUDGETS_MB="${RESOURCE_CACHE_MATRIX_MEMORY_BUDGETS_MB:-512}"
  DIMS="${RESOURCE_CACHE_MATRIX_DIMS:-32}"
  QUERIES="${RESOURCE_CACHE_MATRIX_QUERIES:-4}"
  REPEATS="${RESOURCE_CACHE_MATRIX_REPEATS:-2}"
  K="${RESOURCE_CACHE_MATRIX_K:-10}"
  BATCH_SIZE="${RESOURCE_CACHE_MATRIX_BATCH_SIZE:-128}"
  SEARCH_THREADS="${RESOURCE_CACHE_MATRIX_SEARCH_THREADS:-2}"
  FILTER_SELECTIVITIES="${RESOURCE_CACHE_MATRIX_FILTER_SELECTIVITIES:-10 1}"
else
  # The intermediate sizes identify whether the old 1M cliff is a smooth
  # working-set boundary or an unrelated algorithmic discontinuity.
  SCALE_SIZES="${RESOURCE_CACHE_MATRIX_SCALE_SIZES:-50000 600000 700000 800000 1000000}"
  ENDPOINT_SIZES="${RESOURCE_CACHE_MATRIX_ENDPOINT_SIZES:-50000 1000000}"
  MEMORY_BUDGETS_MB="${RESOURCE_CACHE_MATRIX_MEMORY_BUDGETS_MB:-2048 8192}"
  DIMS="${RESOURCE_CACHE_MATRIX_DIMS:-768}"
  QUERIES="${RESOURCE_CACHE_MATRIX_QUERIES:-25}"
  REPEATS="${RESOURCE_CACHE_MATRIX_REPEATS:-4}"
  K="${RESOURCE_CACHE_MATRIX_K:-100}"
  BATCH_SIZE="${RESOURCE_CACHE_MATRIX_BATCH_SIZE:-5000}"
  SEARCH_THREADS="${RESOURCE_CACHE_MATRIX_SEARCH_THREADS:-16}"
  FILTER_SELECTIVITIES="${RESOURCE_CACHE_MATRIX_FILTER_SELECTIVITIES:-10 1}"
fi

SYNC_LEVEL="${RESOURCE_CACHE_MATRIX_SYNC_LEVEL:-full_index}"
LOAD_PROGRESS_INTERVAL="${RESOURCE_CACHE_MATRIX_LOAD_PROGRESS_INTERVAL:-25000}"
RUN_BUILD="${RESOURCE_CACHE_MATRIX_WARM_BUILD:-1}"
ENFORCE_GATES="${RESOURCE_CACHE_MATRIX_ENFORCE_GATES:-$((1 - SMOKE))}"
MIN_50K_FILTER_QPS="${RESOURCE_CACHE_MATRIX_MIN_50K_FILTER_QPS:-150}"
MIN_1M_FILTER_TO_DENSE_RATIO="${RESOURCE_CACHE_MATRIX_MIN_1M_FILTER_TO_DENSE_RATIO:-0.80}"
SOAK_REPEATS="${RESOURCE_CACHE_MATRIX_SOAK_REPEATS:-25}"
RUN_ENDPOINTS="${RESOURCE_CACHE_MATRIX_RUN_ENDPOINTS:-1}"
RUN_MAINTENANCE="${RESOURCE_CACHE_MATRIX_RUN_MAINTENANCE:-$((1 - SMOKE))}"
MIN_P1_TO_P10_RATIO="${RESOURCE_CACHE_MATRIX_MIN_P1_TO_P10_RATIO:-0.70}"
MIN_THREAD_SCALING_RATIO="${RESOURCE_CACHE_MATRIX_MIN_THREAD_SCALING_RATIO:-0.70}"
MIN_MAINTENANCE_TO_BASE_RATIO="${RESOURCE_CACHE_MATRIX_MIN_MAINTENANCE_TO_BASE_RATIO:-0.70}"
MAX_RSS_TO_BUDGET_RATIO="${RESOURCE_CACHE_MATRIX_MAX_RSS_TO_BUDGET_RATIO:-1.25}"

mkdir -p "$OUT"
STATUS_FILE="$OUT/status.tsv"
COMMAND_FILE="$OUT/commands.txt"
SUMMARY_FILE="$OUT/public-query-summary.jsonl"
if [[ "$RESUME" == "1" ]]; then
  touch "$STATUS_FILE" "$COMMAND_FILE" "$SUMMARY_FILE"
else
  : >"$STATUS_FILE"
  : >"$COMMAND_FILE"
  : >"$SUMMARY_FILE"
fi

{
  printf 'timestamp_utc=%s\n' "$STAMP"
  printf 'root=%s\n' "$ROOT"
  printf 'smoke=%s\n' "$SMOKE"
  printf 'scale_sizes=%s\n' "$SCALE_SIZES"
  printf 'endpoint_sizes=%s\n' "$ENDPOINT_SIZES"
  printf 'memory_budgets_mb=%s\n' "$MEMORY_BUDGETS_MB"
  printf 'filter_selectivities_percent=%s\n' "$FILTER_SELECTIVITIES"
  printf 'run_endpoints=%s\nrun_maintenance=%s\n' "$RUN_ENDPOINTS" "$RUN_MAINTENANCE"
  printf 'optimize=%s\n' "$OPTIMIZE"
  printf 'resume=%s\n' "$RESUME"
  printf 'dims=%s\nqueries=%s\nrepeats=%s\nk=%s\nbatch_size=%s\nsearch_threads=%s\n' \
    "$DIMS" "$QUERIES" "$REPEATS" "$K" "$BATCH_SIZE" "$SEARCH_THREADS"
  git -C "$ROOT" rev-parse HEAD
  git -C "$ROOT" status --short
  uname -a
} >"$OUT/environment.txt" 2>&1 || true

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
  local shape="$2"
  local docs="$3"
  local budget_mb="$4"
  local filter_selectivity="${5:-10}"
  local case_repeats="${6:-$REPEATS}"
  if [[ "$RESUME" == "1" ]] && awk -F '\t' -v case_name="$name" '$1 == case_name { found = 1 } END { exit(found ? 0 : 1) }' "$STATUS_FILE"; then
    printf 'skipping-recorded\t%s\n' "$name"
    return 0
  fi
  local stdout_file="$OUT/$name.stdout"
  local stderr_file="$OUT/$name.stderr"
  local cmd=(
    zig build
    --cache-dir "$ZIG_CACHE_DIR"
    --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR"
    -Doptimize="$OPTIMIZE"
    public-query-standalone-guardrail
    --
    --mode standalone
    --server-kind zig
    --standalone-binary "$ZIG_ROOT/zig-out/bin/antfly"
    --query-shape "$shape"
    --docs "$docs"
    --dims "$DIMS"
    --queries "$QUERIES"
    --repeats "$case_repeats"
    --k "$K"
    --batch-size "$BATCH_SIZE"
    --search-threads "$SEARCH_THREADS"
    --search-thread-sweep
    --sync-level "$SYNC_LEVEL"
    --load-progress-interval "$LOAD_PROGRESS_INTERVAL"
    --process-memory-budget-mb "$budget_mb"
  )
  if [[ "$shape" == "dense-filter" ]]; then
    cmd+=(--filter-selectivity-percent "$filter_selectivity")
  fi
  if [[ "$shape" == "graph-expand" ]]; then
    cmd+=(--with-graph)
  fi
  record_command "$name" "${cmd[@]}"
  printf 'running\t%s\n' "$name"
  local started ended rc
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  (cd "$ZIG_ROOT" && "${cmd[@]}") >"$stdout_file" 2>"$stderr_file"
  rc=$?
  set -e
  ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\t%s\t%s\t%s\n' "$name" "$rc" "$started" "$ended" >>"$STATUS_FILE"
  grep '"event":"public_query_guardrail_summary"' "$stderr_file" |
    sed "s/^{/{\"case\":\"$name\",/" >>"$SUMMARY_FILE" || true
  if [[ "$rc" != "0" ]]; then
    printf 'failed\t%s\trc=%s\n' "$name" "$rc" >&2
    return "$rc"
  fi
}

if [[ "$RUN_BUILD" == "1" ]]; then
  (cd "$ZIG_ROOT" && zig build \
    --cache-dir "$ZIG_CACHE_DIR" \
    --global-cache-dir "$ZIG_GLOBAL_CACHE_DIR" \
    -Doptimize="$OPTIMIZE" \
    antfly public-query-standalone-guardrail-build release-blocker-regression-test)
fi

FAILURES="$(awk -F '\t' '$2 != 0 { count += 1 } END { print count + 0 }' "$STATUS_FILE")"
for budget_mb in $MEMORY_BUDGETS_MB; do
  for docs in $SCALE_SIZES; do
    run_case "dense-${docs}-m${budget_mb}" dense "$docs" "$budget_mb" || FAILURES=$((FAILURES + 1))
    for filter_selectivity in $FILTER_SELECTIVITIES; do
      run_case "dense-filter-p${filter_selectivity}-${docs}-m${budget_mb}" dense-filter "$docs" "$budget_mb" "$filter_selectivity" || FAILURES=$((FAILURES + 1))
    done
  done
  if [[ "$RUN_ENDPOINTS" == "1" ]]; then
    for docs in $ENDPOINT_SIZES; do
      run_case "full-text-${docs}-m${budget_mb}" full-text "$docs" "$budget_mb" || FAILURES=$((FAILURES + 1))
      run_case "graph-expand-${docs}-m${budget_mb}" graph-expand "$docs" "$budget_mb" || FAILURES=$((FAILURES + 1))
    done
  fi
done

# Keep issuing filtered reads after full-index publication so posting repair,
# cache reclamation, and query traffic overlap. The regular case already
# captures cold and warm phases; this longer endpoint case is the maintenance
# stability gate.
if [[ "$RUN_MAINTENANCE" == "1" ]]; then
  for budget_mb in $MEMORY_BUDGETS_MB; do
    run_case "maintenance-soak-p1-1000000-m${budget_mb}" dense-filter 1000000 "$budget_mb" 1 "$SOAK_REPEATS" || FAILURES=$((FAILURES + 1))
  done
fi

gate_float_ge() {
  local actual="$1"
  local expected="$2"
  awk -v actual="$actual" -v expected="$expected" 'BEGIN { exit(actual + 0 >= expected + 0 ? 0 : 1) }'
}

gate_float_le() {
  local actual="$1"
  local expected="$2"
  awk -v actual="$actual" -v expected="$expected" 'BEGIN { exit(actual + 0 <= expected + 0 ? 0 : 1) }'
}

summary_value() {
  local case_name="$1"
  local field="$2"
  jq -sr --arg case_name "$case_name" --arg field "$field" \
    'map(select(.case == $case_name)) | last | .[$field] // 0' "$SUMMARY_FILE"
}

thread_sweep_qps() {
  local case_name="$1"
  local target_threads="$2"
  awk -v target_threads="$target_threads" '
    $1 == "public_query_thread_sweep" {
      threads = ""
      qps = ""
      for (i = 2; i <= NF; i += 1) {
        if ($i ~ /^threads=/) { split($i, part, "="); threads = part[2] }
        if ($i ~ /^qps=/) { split($i, part, "="); qps = part[2] }
      }
      if (threads == target_threads) value = qps
    }
    END { print value + 0 }
  ' "$OUT/$case_name.stderr"
}

if [[ "$ENFORCE_GATES" == "1" ]]; then
  if ! command -v jq >/dev/null 2>&1; then
    printf 'resource-manager cache matrix gate requires jq\n' >&2
    FAILURES=$((FAILURES + 1))
  else
    for budget_mb in $MEMORY_BUDGETS_MB; do
      dense_1m_qps="$(summary_value "dense-1000000-m${budget_mb}" concurrent_qps)"
      filtered_1m_qps="$(summary_value "dense-filter-p1-1000000-m${budget_mb}" concurrent_qps)"
      filtered_50k_qps="$(summary_value "dense-filter-p1-50000-m${budget_mb}" concurrent_qps)"
      cold_us="$(summary_value "dense-filter-p1-1000000-m${budget_mb}" http_first_pass_us)"
      warm_us="$(summary_value "dense-filter-p1-1000000-m${budget_mb}" http_later_pass_us)"
      filtered_10_qps="$(summary_value "dense-filter-p10-1000000-m${budget_mb}" concurrent_qps)"
      search_rss_bytes="$(summary_value "dense-filter-p1-1000000-m${budget_mb}" search_rss_peak_bytes)"
      hbc_accounted_bytes="$(summary_value "dense-filter-p1-1000000-m${budget_mb}" hbc_accounted_bytes)"
      one_thread_qps="$(thread_sweep_qps "dense-filter-p1-1000000-m${budget_mb}" 1)"
      max_thread_qps="$(thread_sweep_qps "dense-filter-p1-1000000-m${budget_mb}" "$SEARCH_THREADS")"
      required_1m_qps="$(awk -v qps="$dense_1m_qps" -v ratio="$MIN_1M_FILTER_TO_DENSE_RATIO" 'BEGIN { printf "%.6f", qps * ratio }')"
      required_p1_qps="$(awk -v qps="$filtered_10_qps" -v ratio="$MIN_P1_TO_P10_RATIO" 'BEGIN { printf "%.6f", qps * ratio }')"
      required_thread_qps="$(awk -v qps="$one_thread_qps" -v ratio="$MIN_THREAD_SCALING_RATIO" 'BEGIN { printf "%.6f", qps * ratio }')"
      allowed_rss_bytes="$(awk -v mb="$budget_mb" -v ratio="$MAX_RSS_TO_BUDGET_RATIO" 'BEGIN { printf "%.0f", mb * 1048576 * ratio }')"
      allowed_accounted_bytes="$(awk -v mb="$budget_mb" 'BEGIN { printf "%.0f", mb * 1048576 }')"

      if ! gate_float_ge "$filtered_50k_qps" "$MIN_50K_FILTER_QPS"; then
        printf 'gate failed: 50k/1%% qps=%s minimum=%s budget_mb=%s\n' "$filtered_50k_qps" "$MIN_50K_FILTER_QPS" "$budget_mb" >&2
        FAILURES=$((FAILURES + 1))
      fi
      if ! gate_float_ge "$filtered_1m_qps" "$required_1m_qps"; then
        printf 'gate failed: 1m/1%% qps=%s required=%s (%.0f%% of dense=%s) budget_mb=%s\n' \
          "$filtered_1m_qps" "$required_1m_qps" "$(awk -v ratio="$MIN_1M_FILTER_TO_DENSE_RATIO" 'BEGIN { print ratio * 100 }')" "$dense_1m_qps" "$budget_mb" >&2
        FAILURES=$((FAILURES + 1))
      fi
      if ! gate_float_ge "$filtered_1m_qps" "$required_p1_qps"; then
        printf 'gate failed: selectivity regression 1%% qps=%s required=%s (%.0f%% of 10%%=%s) budget_mb=%s\n' \
          "$filtered_1m_qps" "$required_p1_qps" "$(awk -v ratio="$MIN_P1_TO_P10_RATIO" 'BEGIN { print ratio * 100 }')" "$filtered_10_qps" "$budget_mb" >&2
        FAILURES=$((FAILURES + 1))
      fi
      if ! gate_float_ge "$one_thread_qps" 0.001 || ! gate_float_ge "$max_thread_qps" "$required_thread_qps"; then
        printf 'gate failed: concurrency scaling threads=1 qps=%s threads=%s qps=%s required=%s budget_mb=%s\n' \
          "$one_thread_qps" "$SEARCH_THREADS" "$max_thread_qps" "$required_thread_qps" "$budget_mb" >&2
        FAILURES=$((FAILURES + 1))
      fi
      if ! gate_float_le "$search_rss_bytes" "$allowed_rss_bytes"; then
        printf 'gate failed: search rss bytes=%s allowed=%s budget_mb=%s ratio=%s\n' \
          "$search_rss_bytes" "$allowed_rss_bytes" "$budget_mb" "$MAX_RSS_TO_BUDGET_RATIO" >&2
        FAILURES=$((FAILURES + 1))
      fi
      if ! gate_float_le "$hbc_accounted_bytes" "$allowed_accounted_bytes"; then
        printf 'gate failed: hbc accounted bytes=%s process budget bytes=%s budget_mb=%s\n' \
          "$hbc_accounted_bytes" "$allowed_accounted_bytes" "$budget_mb" >&2
        FAILURES=$((FAILURES + 1))
      fi
      if ! gate_float_ge "$cold_us" 0.001 || ! gate_float_ge "$warm_us" 0.001; then
        printf 'gate failed: cold/warm phases missing cold_us=%s warm_us=%s budget_mb=%s\n' "$cold_us" "$warm_us" "$budget_mb" >&2
        FAILURES=$((FAILURES + 1))
      fi
      if [[ "$RUN_MAINTENANCE" == "1" ]]; then
        maintenance_qps="$(summary_value "maintenance-soak-p1-1000000-m${budget_mb}" concurrent_qps)"
        required_maintenance_qps="$(awk -v qps="$filtered_1m_qps" -v ratio="$MIN_MAINTENANCE_TO_BASE_RATIO" 'BEGIN { printf "%.6f", qps * ratio }')"
        if ! gate_float_ge "$maintenance_qps" "$required_maintenance_qps"; then
          printf 'gate failed: maintenance soak qps=%s required=%s base=%s budget_mb=%s\n' \
            "$maintenance_qps" "$required_maintenance_qps" "$filtered_1m_qps" "$budget_mb" >&2
          FAILURES=$((FAILURES + 1))
        fi
      fi
    done
  fi
fi

printf 'resource-manager cache matrix complete: %s\n' "$OUT"
if [[ "$FAILURES" != "0" ]]; then
  printf 'resource-manager cache matrix failures: %s\n' "$FAILURES" >&2
  exit 1
fi
