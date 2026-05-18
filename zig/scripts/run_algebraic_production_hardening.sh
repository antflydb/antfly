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
OUT="${ALGEBRAIC_HARDENING_OUT:-bench/results/algebraic-production-hardening/$STAMP}"

DOCS="${ALGEBRAIC_HARDENING_DOCS:-50000}"
REPEATS="${ALGEBRAIC_HARDENING_REPEATS:-5}"
BATCH_SIZE="${ALGEBRAIC_HARDENING_BATCH_SIZE:-1000}"
CHURN_OPS="${ALGEBRAIC_HARDENING_CHURN_OPS:-5000}"
FANOUT="${ALGEBRAIC_HARDENING_FANOUT:-4}"
CUSTOMERS="${ALGEBRAIC_HARDENING_CUSTOMERS:-4096}"
PRODUCTS="${ALGEBRAIC_HARDENING_PRODUCTS:-128}"
ADAPTIVE_DOCS="${ALGEBRAIC_HARDENING_ADAPTIVE_DOCS:-$DOCS}"
ADAPTIVE_BATCH_SIZE="${ALGEBRAIC_HARDENING_ADAPTIVE_BATCH_SIZE:-$BATCH_SIZE}"
ADAPTIVE_CHURN_OPS="${ALGEBRAIC_HARDENING_ADAPTIVE_CHURN_OPS:-$CHURN_OPS}"
COLD_DOCS="${ALGEBRAIC_HARDENING_COLD_DOCS:-$DOCS}"
COLD_BATCH_SIZE="${ALGEBRAIC_HARDENING_COLD_BATCH_SIZE:-$BATCH_SIZE}"
COLD_CHURN_OPS="${ALGEBRAIC_HARDENING_COLD_CHURN_OPS:-$CHURN_OPS}"
GRAPH_DOCS="${ALGEBRAIC_HARDENING_GRAPH_DOCS:-10000}"
GRAPH_REPEATS="${ALGEBRAIC_HARDENING_GRAPH_REPEATS:-5}"
PUBLIC_DOCS="${ALGEBRAIC_HARDENING_PUBLIC_DOCS:-10000}"
PUBLIC_DIMS="${ALGEBRAIC_HARDENING_PUBLIC_DIMS:-128}"
PUBLIC_QUERIES="${ALGEBRAIC_HARDENING_PUBLIC_QUERIES:-10}"
PUBLIC_REPEATS="${ALGEBRAIC_HARDENING_PUBLIC_REPEATS:-3}"
PUBLIC_K="${ALGEBRAIC_HARDENING_PUBLIC_K:-25}"
PUBLIC_THREADS="${ALGEBRAIC_HARDENING_PUBLIC_THREADS:-8}"
PUBLIC_MODE="${ALGEBRAIC_HARDENING_PUBLIC_MODE:-handler}"
PUBLIC_REQUIRE_SYMBOLIC_PROFILE="${ALGEBRAIC_HARDENING_PUBLIC_REQUIRE_SYMBOLIC_PROFILE:-0}"
LSM_BULK_INGEST="${ALGEBRAIC_HARDENING_LSM_BULK_INGEST:-0}"
LSM_BULK_FLUSH="${ALGEBRAIC_HARDENING_LSM_BULK_FLUSH:-1}"
LSM_BULK_COMPACT="${ALGEBRAIC_HARDENING_LSM_BULK_COMPACT:-0}"
LSM_BULK_MAX_DEFERRED_L0_RUNS="${ALGEBRAIC_HARDENING_LSM_BULK_MAX_DEFERRED_L0_RUNS:-}"
LSM_BULK_MAX_FOREGROUND_COMPACTION_STEPS="${ALGEBRAIC_HARDENING_LSM_BULK_MAX_FOREGROUND_COMPACTION_STEPS:-0}"
LSM_BULK_MAX_FOREGROUND_COMPACTION_INPUT_BYTES="${ALGEBRAIC_HARDENING_LSM_BULK_MAX_FOREGROUND_COMPACTION_INPUT_BYTES:-}"
LSM_BULK_MAX_FOREGROUND_COMPACTION_NS="${ALGEBRAIC_HARDENING_LSM_BULK_MAX_FOREGROUND_COMPACTION_NS:-}"
LSM_FLUSH_THRESHOLD="${ALGEBRAIC_HARDENING_LSM_FLUSH_THRESHOLD:-}"
LSM_FLUSH_THRESHOLD_BYTES="${ALGEBRAIC_HARDENING_LSM_FLUSH_THRESHOLD_BYTES:-}"
LSM_BULK_INGEST_FLUSH_THRESHOLD_MULTIPLIER="${ALGEBRAIC_HARDENING_LSM_BULK_INGEST_FLUSH_THRESHOLD_MULTIPLIER:-}"
LSM_BULK_INGEST_FLUSH_THRESHOLD_BYTES_MULTIPLIER="${ALGEBRAIC_HARDENING_LSM_BULK_INGEST_FLUSH_THRESHOLD_BYTES_MULTIPLIER:-}"
LSM_DIRECT_BULK_INGEST="${ALGEBRAIC_HARDENING_LSM_DIRECT_BULK_INGEST:-}"
LSM_COMPACT_THRESHOLD_RUNS="${ALGEBRAIC_HARDENING_LSM_COMPACT_THRESHOLD_RUNS:-}"
LSM_LEVEL_TARGET_RUNS_BASE="${ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_RUNS_BASE:-}"
LSM_LEVEL_TARGET_RUNS_MULTIPLIER="${ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_RUNS_MULTIPLIER:-}"
LSM_LEVEL_TARGET_BYTES_BASE="${ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_BYTES_BASE:-}"
LSM_LEVEL_TARGET_BYTES_MULTIPLIER="${ALGEBRAIC_HARDENING_LSM_LEVEL_TARGET_BYTES_MULTIPLIER:-}"
RUN_UNIT_TEST="${ALGEBRAIC_HARDENING_RUN_UNIT_TEST:-0}"
SMOKE="${ALGEBRAIC_HARDENING_SMOKE:-0}"
BASELINE="${ALGEBRAIC_HARDENING_BASELINE:-}"
MIN_DATASET_CASES="${ALGEBRAIC_HARDENING_MIN_DATASET_CASES:-1}"
MIN_LSM_DATASET_CASES="${ALGEBRAIC_HARDENING_MIN_LSM_DATASET_CASES:-1}"
MIN_ALGEBRAIC_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_ALGEBRAIC_QUERY_RECORDS:-1}"
MIN_DOC_SCAN_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_DOC_SCAN_QUERY_RECORDS:-1}"
MIN_FULL_TEXT_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_FULL_TEXT_QUERY_RECORDS:-1}"
MIN_LSM_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_LSM_QUERY_RECORDS:-1}"
MIN_COLD_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_COLD_QUERY_RECORDS:-1}"
MIN_WARM_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_WARM_QUERY_RECORDS:-1}"
MIN_CONSTRAINED_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_CONSTRAINED_QUERY_RECORDS:-1}"
MIN_WIDE_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_WIDE_QUERY_RECORDS:-1}"
MIN_STATS_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_STATS_QUERY_RECORDS:-1}"
MIN_CARDINALITY_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_CARDINALITY_QUERY_RECORDS:-1}"
MIN_RANGE_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_RANGE_QUERY_RECORDS:-1}"
MIN_HISTOGRAM_QUERY_RECORDS="${ALGEBRAIC_HARDENING_MIN_HISTOGRAM_QUERY_RECORDS:-1}"
MIN_FANOUT_DATASET_CASES="${ALGEBRAIC_HARDENING_MIN_FANOUT_DATASET_CASES:-1}"
MIN_CHURN_RECORDS="${ALGEBRAIC_HARDENING_MIN_CHURN_RECORDS:-1}"
MIN_PUBLIC_QUERY_COMPARISON_PAIRS="${ALGEBRAIC_HARDENING_MIN_PUBLIC_QUERY_COMPARISON_PAIRS:-2}"
MIN_LSM_SORTED_INGEST_RUNS="${ALGEBRAIC_HARDENING_MIN_LSM_SORTED_INGEST_RUNS:-0}"
MAX_LSM_FLUSHES="${ALGEBRAIC_HARDENING_MAX_LSM_FLUSHES:-}"
MAX_LSM_WRITE_PRESSURE_COMPACTIONS="${ALGEBRAIC_HARDENING_MAX_LSM_WRITE_PRESSURE_COMPACTIONS:-}"
MAX_CORRECTNESS_FAILURES="${ALGEBRAIC_HARDENING_MAX_CORRECTNESS_FAILURES:-0}"
MAX_UNCLASSIFIED_ALGEBRAIC_COMPARISONS="${ALGEBRAIC_HARDENING_MAX_UNCLASSIFIED_ALGEBRAIC_COMPARISONS:-0}"
MAX_ALGEBRAIC_QUERY_MS="${ALGEBRAIC_HARDENING_MAX_ALGEBRAIC_QUERY_MS:-}"
MAX_PUBLIC_QUERY_HTTP_US="${ALGEBRAIC_HARDENING_MAX_PUBLIC_QUERY_HTTP_US:-}"
MAX_ALGEBRAIC_BYTES_PER_DOC="${ALGEBRAIC_HARDENING_MAX_ALGEBRAIC_BYTES_PER_DOC:-}"
MAX_SYMBOL_BYTES_PER_DOC="${ALGEBRAIC_HARDENING_MAX_SYMBOL_BYTES_PER_DOC:-}"
MAX_SUPPORT_BYTES_PER_DOC="${ALGEBRAIC_HARDENING_MAX_SUPPORT_BYTES_PER_DOC:-}"
MAX_ACCUMULATOR_FLUSH_COUNT="${ALGEBRAIC_HARDENING_MAX_ACCUMULATOR_FLUSH_COUNT:-}"
MAX_PATH_DICTIONARY_FST_REBUILD_COUNT="${ALGEBRAIC_HARDENING_MAX_PATH_DICTIONARY_FST_REBUILD_COUNT:-}"
MAX_CHURN_ALGEBRAIC_UPDATE_MS="${ALGEBRAIC_HARDENING_MAX_CHURN_ALGEBRAIC_UPDATE_MS:-}"
MAX_CHURN_SIDECAR_BYTES="${ALGEBRAIC_HARDENING_MAX_CHURN_SIDECAR_BYTES:-}"
MAX_PUBLIC_QUERY_LOAD_RSS_PEAK_BYTES="${ALGEBRAIC_HARDENING_MAX_PUBLIC_QUERY_LOAD_RSS_PEAK_BYTES:-}"
MAX_PUBLIC_QUERY_SEARCH_RSS_PEAK_BYTES="${ALGEBRAIC_HARDENING_MAX_PUBLIC_QUERY_SEARCH_RSS_PEAK_BYTES:-}"
MAX_ALGEBRAIC_QUERY_MS_RATIO="${ALGEBRAIC_HARDENING_MAX_ALGEBRAIC_QUERY_MS_RATIO_VS_BASELINE:-}"
MAX_PUBLIC_QUERY_HTTP_US_RATIO="${ALGEBRAIC_HARDENING_MAX_PUBLIC_QUERY_HTTP_US_RATIO_VS_BASELINE:-}"
MAX_ALGEBRAIC_BYTES_PER_DOC_RATIO="${ALGEBRAIC_HARDENING_MAX_ALGEBRAIC_BYTES_PER_DOC_RATIO_VS_BASELINE:-}"
MAX_CHURN_ALGEBRAIC_UPDATE_MS_RATIO="${ALGEBRAIC_HARDENING_MAX_CHURN_ALGEBRAIC_UPDATE_MS_RATIO_VS_BASELINE:-}"

LSM_MODE="lsm-analytics"
GRAPH_MODE="graph-traversal"
ADAPTIVE_BACKEND="lsm"
GRAPH_BACKEND="lsm"
if [[ "$SMOKE" == "1" ]]; then
  LSM_MODE="lsm-analytics-smoke"
  GRAPH_MODE="graph-traversal-smoke"
  ADAPTIVE_BACKEND="mem"
  GRAPH_BACKEND="mem"
fi

mkdir -p "$OUT"
STAGE_JSONL_FILES=()

{
  echo "timestamp_utc=$STAMP"
  echo "root=$ROOT"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || true)"
  echo "git_status_porcelain_begin"
  git status --short || true
  echo "git_status_porcelain_end"
  echo "uname=$(uname -a)"
  echo "docs=$DOCS"
  echo "repeats=$REPEATS"
  echo "batch_size=$BATCH_SIZE"
  echo "churn_ops=$CHURN_OPS"
  echo "fanout=$FANOUT"
  echo "customers=$CUSTOMERS"
  echo "products=$PRODUCTS"
  echo "adaptive_docs=$ADAPTIVE_DOCS"
  echo "adaptive_batch_size=$ADAPTIVE_BATCH_SIZE"
  echo "adaptive_churn_ops=$ADAPTIVE_CHURN_OPS"
  echo "cold_docs=$COLD_DOCS"
  echo "cold_batch_size=$COLD_BATCH_SIZE"
  echo "cold_churn_ops=$COLD_CHURN_OPS"
  echo "graph_docs=$GRAPH_DOCS"
  echo "graph_repeats=$GRAPH_REPEATS"
  echo "public_docs=$PUBLIC_DOCS"
  echo "public_dims=$PUBLIC_DIMS"
  echo "public_queries=$PUBLIC_QUERIES"
  echo "public_repeats=$PUBLIC_REPEATS"
  echo "public_k=$PUBLIC_K"
  echo "public_threads=$PUBLIC_THREADS"
  echo "public_mode=$PUBLIC_MODE"
  echo "public_require_symbolic_profile=$PUBLIC_REQUIRE_SYMBOLIC_PROFILE"
  echo "lsm_bulk_ingest=$LSM_BULK_INGEST"
  echo "lsm_bulk_flush=$LSM_BULK_FLUSH"
  echo "lsm_bulk_compact=$LSM_BULK_COMPACT"
  echo "lsm_bulk_max_deferred_l0_runs=$LSM_BULK_MAX_DEFERRED_L0_RUNS"
  echo "lsm_bulk_max_foreground_compaction_steps=$LSM_BULK_MAX_FOREGROUND_COMPACTION_STEPS"
  echo "lsm_bulk_max_foreground_compaction_input_bytes=$LSM_BULK_MAX_FOREGROUND_COMPACTION_INPUT_BYTES"
  echo "lsm_bulk_max_foreground_compaction_ns=$LSM_BULK_MAX_FOREGROUND_COMPACTION_NS"
  echo "lsm_flush_threshold=$LSM_FLUSH_THRESHOLD"
  echo "lsm_flush_threshold_bytes=$LSM_FLUSH_THRESHOLD_BYTES"
  echo "lsm_bulk_ingest_flush_threshold_multiplier=$LSM_BULK_INGEST_FLUSH_THRESHOLD_MULTIPLIER"
  echo "lsm_bulk_ingest_flush_threshold_bytes_multiplier=$LSM_BULK_INGEST_FLUSH_THRESHOLD_BYTES_MULTIPLIER"
  echo "lsm_direct_bulk_ingest=$LSM_DIRECT_BULK_INGEST"
  echo "lsm_compact_threshold_runs=$LSM_COMPACT_THRESHOLD_RUNS"
  echo "lsm_level_target_runs_base=$LSM_LEVEL_TARGET_RUNS_BASE"
  echo "lsm_level_target_runs_multiplier=$LSM_LEVEL_TARGET_RUNS_MULTIPLIER"
  echo "lsm_level_target_bytes_base=$LSM_LEVEL_TARGET_BYTES_BASE"
  echo "lsm_level_target_bytes_multiplier=$LSM_LEVEL_TARGET_BYTES_MULTIPLIER"
  echo "smoke=$SMOKE"
  echo "lsm_mode=$LSM_MODE"
  echo "graph_mode=$GRAPH_MODE"
  echo "adaptive_backend=$ADAPTIVE_BACKEND"
  echo "graph_backend=$GRAPH_BACKEND"
  echo "baseline=$BASELINE"
  echo "min_dataset_cases=$MIN_DATASET_CASES"
  echo "min_lsm_dataset_cases=$MIN_LSM_DATASET_CASES"
  echo "min_algebraic_query_records=$MIN_ALGEBRAIC_QUERY_RECORDS"
  echo "min_doc_scan_query_records=$MIN_DOC_SCAN_QUERY_RECORDS"
  echo "min_full_text_query_records=$MIN_FULL_TEXT_QUERY_RECORDS"
  echo "min_lsm_query_records=$MIN_LSM_QUERY_RECORDS"
  echo "min_cold_query_records=$MIN_COLD_QUERY_RECORDS"
  echo "min_warm_query_records=$MIN_WARM_QUERY_RECORDS"
  echo "min_constrained_query_records=$MIN_CONSTRAINED_QUERY_RECORDS"
  echo "min_wide_query_records=$MIN_WIDE_QUERY_RECORDS"
  echo "min_stats_query_records=$MIN_STATS_QUERY_RECORDS"
  echo "min_cardinality_query_records=$MIN_CARDINALITY_QUERY_RECORDS"
  echo "min_range_query_records=$MIN_RANGE_QUERY_RECORDS"
  echo "min_histogram_query_records=$MIN_HISTOGRAM_QUERY_RECORDS"
  echo "min_fanout_dataset_cases=$MIN_FANOUT_DATASET_CASES"
  echo "min_churn_records=$MIN_CHURN_RECORDS"
  echo "min_public_query_comparison_pairs=$MIN_PUBLIC_QUERY_COMPARISON_PAIRS"
  echo "min_lsm_sorted_ingest_runs=$MIN_LSM_SORTED_INGEST_RUNS"
  echo "max_lsm_flushes=$MAX_LSM_FLUSHES"
  echo "max_lsm_write_pressure_compactions=$MAX_LSM_WRITE_PRESSURE_COMPACTIONS"
  echo "max_correctness_failures=$MAX_CORRECTNESS_FAILURES"
  echo "max_unclassified_algebraic_comparisons=$MAX_UNCLASSIFIED_ALGEBRAIC_COMPARISONS"
  echo "max_algebraic_query_ms=$MAX_ALGEBRAIC_QUERY_MS"
  echo "max_public_query_http_us=$MAX_PUBLIC_QUERY_HTTP_US"
  echo "max_algebraic_bytes_per_doc=$MAX_ALGEBRAIC_BYTES_PER_DOC"
  echo "max_symbol_bytes_per_doc=$MAX_SYMBOL_BYTES_PER_DOC"
  echo "max_support_bytes_per_doc=$MAX_SUPPORT_BYTES_PER_DOC"
  echo "max_accumulator_flush_count=$MAX_ACCUMULATOR_FLUSH_COUNT"
  echo "max_path_dictionary_fst_rebuild_count=$MAX_PATH_DICTIONARY_FST_REBUILD_COUNT"
  echo "max_churn_algebraic_update_ms=$MAX_CHURN_ALGEBRAIC_UPDATE_MS"
  echo "max_churn_sidecar_bytes=$MAX_CHURN_SIDECAR_BYTES"
  echo "max_public_query_load_rss_peak_bytes=$MAX_PUBLIC_QUERY_LOAD_RSS_PEAK_BYTES"
  echo "max_public_query_search_rss_peak_bytes=$MAX_PUBLIC_QUERY_SEARCH_RSS_PEAK_BYTES"
  echo "max_algebraic_query_ms_ratio_vs_baseline=$MAX_ALGEBRAIC_QUERY_MS_RATIO"
  echo "max_public_query_http_us_ratio_vs_baseline=$MAX_PUBLIC_QUERY_HTTP_US_RATIO"
  echo "max_algebraic_bytes_per_doc_ratio_vs_baseline=$MAX_ALGEBRAIC_BYTES_PER_DOC_RATIO"
  echo "max_churn_algebraic_update_ms_ratio_vs_baseline=$MAX_CHURN_ALGEBRAIC_UPDATE_MS_RATIO"
} > "$OUT/environment.txt"

run_jsonl_stderr() {
  local name="$1"
  shift
  echo "running $name"
  "$@" > "$OUT/$name.stdout" 2> "$OUT/$name.jsonl"
  STAGE_JSONL_FILES+=("$OUT/$name.jsonl")
}

LSM_BULK_ARGS=()
if [[ "$LSM_BULK_INGEST" == "1" ]]; then
  LSM_BULK_ARGS+=(--algebraic-bulk-ingest)
  if [[ "$LSM_BULK_FLUSH" == "1" ]]; then
    LSM_BULK_ARGS+=(--algebraic-bulk-flush)
  else
    LSM_BULK_ARGS+=(--algebraic-bulk-no-flush)
  fi
  if [[ "$LSM_BULK_COMPACT" == "1" ]]; then
    LSM_BULK_ARGS+=(--algebraic-bulk-compact)
  else
    LSM_BULK_ARGS+=(--algebraic-bulk-no-compact)
  fi
  if [[ -n "$LSM_BULK_MAX_DEFERRED_L0_RUNS" ]]; then
    LSM_BULK_ARGS+=(--algebraic-bulk-max-deferred-l0-runs "$LSM_BULK_MAX_DEFERRED_L0_RUNS")
  fi
  if [[ "$LSM_BULK_MAX_FOREGROUND_COMPACTION_STEPS" != "0" ]]; then
    LSM_BULK_ARGS+=(--algebraic-bulk-max-foreground-compaction-steps "$LSM_BULK_MAX_FOREGROUND_COMPACTION_STEPS")
  fi
  if [[ -n "$LSM_BULK_MAX_FOREGROUND_COMPACTION_INPUT_BYTES" ]]; then
    LSM_BULK_ARGS+=(--algebraic-bulk-max-foreground-compaction-input-bytes "$LSM_BULK_MAX_FOREGROUND_COMPACTION_INPUT_BYTES")
  fi
  if [[ -n "$LSM_BULK_MAX_FOREGROUND_COMPACTION_NS" ]]; then
    LSM_BULK_ARGS+=(--algebraic-bulk-max-foreground-compaction-ns "$LSM_BULK_MAX_FOREGROUND_COMPACTION_NS")
  fi
fi

LSM_TUNING_ARGS=()
if [[ -n "$LSM_FLUSH_THRESHOLD" ]]; then
  LSM_TUNING_ARGS+=(--lsm-flush-threshold "$LSM_FLUSH_THRESHOLD")
fi
if [[ -n "$LSM_FLUSH_THRESHOLD_BYTES" ]]; then
  LSM_TUNING_ARGS+=(--lsm-flush-threshold-bytes "$LSM_FLUSH_THRESHOLD_BYTES")
fi
if [[ -n "$LSM_BULK_INGEST_FLUSH_THRESHOLD_MULTIPLIER" ]]; then
  LSM_TUNING_ARGS+=(--lsm-bulk-ingest-flush-threshold-multiplier "$LSM_BULK_INGEST_FLUSH_THRESHOLD_MULTIPLIER")
fi
if [[ -n "$LSM_BULK_INGEST_FLUSH_THRESHOLD_BYTES_MULTIPLIER" ]]; then
  LSM_TUNING_ARGS+=(--lsm-bulk-ingest-flush-threshold-bytes-multiplier "$LSM_BULK_INGEST_FLUSH_THRESHOLD_BYTES_MULTIPLIER")
fi
if [[ "$LSM_DIRECT_BULK_INGEST" == "1" ]]; then
  LSM_TUNING_ARGS+=(--lsm-direct-bulk-ingest)
elif [[ "$LSM_DIRECT_BULK_INGEST" == "0" ]]; then
  LSM_TUNING_ARGS+=(--lsm-no-direct-bulk-ingest)
elif [[ -n "$LSM_DIRECT_BULK_INGEST" ]]; then
  echo "ALGEBRAIC_HARDENING_LSM_DIRECT_BULK_INGEST must be 0, 1, or empty" >&2
  exit 2
fi
if [[ -n "$LSM_COMPACT_THRESHOLD_RUNS" ]]; then
  LSM_TUNING_ARGS+=(--lsm-compact-threshold-runs "$LSM_COMPACT_THRESHOLD_RUNS")
fi
if [[ -n "$LSM_LEVEL_TARGET_RUNS_BASE" ]]; then
  LSM_TUNING_ARGS+=(--lsm-level-target-runs-base "$LSM_LEVEL_TARGET_RUNS_BASE")
fi
if [[ -n "$LSM_LEVEL_TARGET_RUNS_MULTIPLIER" ]]; then
  LSM_TUNING_ARGS+=(--lsm-level-target-runs-multiplier "$LSM_LEVEL_TARGET_RUNS_MULTIPLIER")
fi
if [[ -n "$LSM_LEVEL_TARGET_BYTES_BASE" ]]; then
  LSM_TUNING_ARGS+=(--lsm-level-target-bytes-base "$LSM_LEVEL_TARGET_BYTES_BASE")
fi
if [[ -n "$LSM_LEVEL_TARGET_BYTES_MULTIPLIER" ]]; then
  LSM_TUNING_ARGS+=(--lsm-level-target-bytes-multiplier "$LSM_LEVEL_TARGET_BYTES_MULTIPLIER")
fi

lsm_args=(
  zig build algebraic-bench --
  --mode "$LSM_MODE"
  --algebraic-backend lsm
  --algebraic-profile production_hardening
  --docs "$DOCS"
  --repeats "$REPEATS"
  --batch-size "$BATCH_SIZE"
  --churn-ops "$CHURN_OPS"
  --fanout "$FANOUT"
  --customers "$CUSTOMERS"
  --products "$PRODUCTS"
)
if [[ "${#LSM_TUNING_ARGS[@]}" -gt 0 ]]; then
  lsm_args+=("${LSM_TUNING_ARGS[@]}")
fi
if [[ "${#LSM_BULK_ARGS[@]}" -gt 0 ]]; then
  lsm_args+=("${LSM_BULK_ARGS[@]}")
fi
run_jsonl_stderr lsm_analytics "${lsm_args[@]}"

adaptive_args=(
  zig build algebraic-bench --
  --mode adaptive-coverage
  --algebraic-backend "$ADAPTIVE_BACKEND"
  --algebraic-profile production_hardening
  --docs "$ADAPTIVE_DOCS"
  --repeats "$REPEATS"
  --batch-size "$ADAPTIVE_BATCH_SIZE"
  --churn-ops "$ADAPTIVE_CHURN_OPS"
  --customers "$CUSTOMERS"
  --products "$PRODUCTS"
)
if [[ "${#LSM_TUNING_ARGS[@]}" -gt 0 ]]; then
  adaptive_args+=("${LSM_TUNING_ARGS[@]}")
fi
if [[ "${#LSM_BULK_ARGS[@]}" -gt 0 ]]; then
  adaptive_args+=("${LSM_BULK_ARGS[@]}")
fi
run_jsonl_stderr adaptive_coverage "${adaptive_args[@]}"

cold_args=(
  zig build algebraic-bench --
  --mode cold
  --algebraic-backend lsm
  --algebraic-profile production_hardening
  --docs "$COLD_DOCS"
  --repeats "$REPEATS"
  --batch-size "$COLD_BATCH_SIZE"
  --churn-ops "$COLD_CHURN_OPS"
  --customers "$CUSTOMERS"
  --products "$PRODUCTS"
)
if [[ "${#LSM_TUNING_ARGS[@]}" -gt 0 ]]; then
  cold_args+=("${LSM_TUNING_ARGS[@]}")
fi
if [[ "${#LSM_BULK_ARGS[@]}" -gt 0 ]]; then
  cold_args+=("${LSM_BULK_ARGS[@]}")
fi
run_jsonl_stderr cold_warm_reads "${cold_args[@]}"

graph_args=(
  zig build algebraic-bench --
  --mode "$GRAPH_MODE"
  --algebraic-backend "$GRAPH_BACKEND"
  --algebraic-profile production_hardening
  --docs "$GRAPH_DOCS"
  --repeats "$GRAPH_REPEATS"
  --fanout "$FANOUT"
  --customers "$CUSTOMERS"
  --products "$PRODUCTS"
)
if [[ "${#LSM_TUNING_ARGS[@]}" -gt 0 ]]; then
  graph_args+=("${LSM_TUNING_ARGS[@]}")
fi
run_jsonl_stderr graph_traversal "${graph_args[@]}"

for schema_mode in no_schema schema_only schema_algebraic; do
  args=(
    zig build public-query-guardrail --
    --mode "$PUBLIC_MODE"
    --query-shape hybrid-filter-exclude-project
    --docs "$PUBLIC_DOCS"
    --dims "$PUBLIC_DIMS"
    --queries "$PUBLIC_QUERIES"
    --repeats "$PUBLIC_REPEATS"
    --k "$PUBLIC_K"
    --search-threads "$PUBLIC_THREADS"
  )
  case "$schema_mode" in
    no_schema)
      ;;
    schema_only)
      args+=(--with-schema)
      ;;
    schema_algebraic)
      args+=(--with-schema --with-algebraic)
      if [[ "$PUBLIC_REQUIRE_SYMBOLIC_PROFILE" == "1" ]]; then
        args+=(--require-symbolic-profile)
      fi
      ;;
  esac
  run_jsonl_stderr "public_query_$schema_mode" "${args[@]}"
done

COMBINED="$OUT/algebraic-production-hardening-combined.jsonl"
cat "${STAGE_JSONL_FILES[@]}" > "$COMBINED"

summary_args=(
  zig build algebraic-summary --
  --input "$COMBINED"
  --require-performance-evidence
  --min-dataset-cases "$MIN_DATASET_CASES"
  --min-lsm-dataset-cases "$MIN_LSM_DATASET_CASES"
  --min-algebraic-query-records "$MIN_ALGEBRAIC_QUERY_RECORDS"
  --min-doc-scan-query-records "$MIN_DOC_SCAN_QUERY_RECORDS"
  --min-full-text-query-records "$MIN_FULL_TEXT_QUERY_RECORDS"
  --min-lsm-query-records "$MIN_LSM_QUERY_RECORDS"
  --min-cold-query-records "$MIN_COLD_QUERY_RECORDS"
  --min-warm-query-records "$MIN_WARM_QUERY_RECORDS"
  --min-constrained-query-records "$MIN_CONSTRAINED_QUERY_RECORDS"
  --min-wide-query-records "$MIN_WIDE_QUERY_RECORDS"
  --min-stats-query-records "$MIN_STATS_QUERY_RECORDS"
  --min-cardinality-query-records "$MIN_CARDINALITY_QUERY_RECORDS"
  --min-range-query-records "$MIN_RANGE_QUERY_RECORDS"
  --min-histogram-query-records "$MIN_HISTOGRAM_QUERY_RECORDS"
  --min-fanout-dataset-cases "$MIN_FANOUT_DATASET_CASES"
  --min-churn-records "$MIN_CHURN_RECORDS"
  --min-public-query-comparison-pairs "$MIN_PUBLIC_QUERY_COMPARISON_PAIRS"
  --min-lsm-sorted-ingest-runs "$MIN_LSM_SORTED_INGEST_RUNS"
  --max-correctness-failures "$MAX_CORRECTNESS_FAILURES"
  --max-unclassified-algebraic-comparisons "$MAX_UNCLASSIFIED_ALGEBRAIC_COMPARISONS"
)
if [[ -n "$BASELINE" ]]; then
  summary_args+=(--baseline "$BASELINE")
fi
if [[ -n "$MAX_ALGEBRAIC_QUERY_MS" ]]; then
  summary_args+=(--max-algebraic-query-ms "$MAX_ALGEBRAIC_QUERY_MS")
fi
if [[ -n "$MAX_PUBLIC_QUERY_HTTP_US" ]]; then
  summary_args+=(--max-public-query-http-us "$MAX_PUBLIC_QUERY_HTTP_US")
fi
if [[ -n "$MAX_ALGEBRAIC_BYTES_PER_DOC" ]]; then
  summary_args+=(--max-algebraic-bytes-per-doc "$MAX_ALGEBRAIC_BYTES_PER_DOC")
fi
if [[ -n "$MAX_SYMBOL_BYTES_PER_DOC" ]]; then
  summary_args+=(--max-symbol-bytes-per-doc "$MAX_SYMBOL_BYTES_PER_DOC")
fi
if [[ -n "$MAX_SUPPORT_BYTES_PER_DOC" ]]; then
  summary_args+=(--max-support-bytes-per-doc "$MAX_SUPPORT_BYTES_PER_DOC")
fi
if [[ -n "$MAX_ACCUMULATOR_FLUSH_COUNT" ]]; then
  summary_args+=(--max-accumulator-flush-count "$MAX_ACCUMULATOR_FLUSH_COUNT")
fi
if [[ -n "$MAX_PATH_DICTIONARY_FST_REBUILD_COUNT" ]]; then
  summary_args+=(--max-path-dictionary-fst-rebuild-count "$MAX_PATH_DICTIONARY_FST_REBUILD_COUNT")
fi
if [[ -n "$MAX_LSM_FLUSHES" ]]; then
  summary_args+=(--max-lsm-flushes "$MAX_LSM_FLUSHES")
fi
if [[ -n "$MAX_LSM_WRITE_PRESSURE_COMPACTIONS" ]]; then
  summary_args+=(--max-lsm-write-pressure-compactions "$MAX_LSM_WRITE_PRESSURE_COMPACTIONS")
fi
if [[ -n "$MAX_CHURN_ALGEBRAIC_UPDATE_MS" ]]; then
  summary_args+=(--max-churn-algebraic-update-ms "$MAX_CHURN_ALGEBRAIC_UPDATE_MS")
fi
if [[ -n "$MAX_CHURN_SIDECAR_BYTES" ]]; then
  summary_args+=(--max-churn-sidecar-bytes "$MAX_CHURN_SIDECAR_BYTES")
fi
if [[ -n "$MAX_PUBLIC_QUERY_LOAD_RSS_PEAK_BYTES" ]]; then
  summary_args+=(--max-public-query-load-rss-peak-bytes "$MAX_PUBLIC_QUERY_LOAD_RSS_PEAK_BYTES")
fi
if [[ -n "$MAX_PUBLIC_QUERY_SEARCH_RSS_PEAK_BYTES" ]]; then
  summary_args+=(--max-public-query-search-rss-peak-bytes "$MAX_PUBLIC_QUERY_SEARCH_RSS_PEAK_BYTES")
fi
if [[ -n "$MAX_ALGEBRAIC_QUERY_MS_RATIO" ]]; then
  summary_args+=(--max-algebraic-query-ms-ratio-vs-baseline "$MAX_ALGEBRAIC_QUERY_MS_RATIO")
fi
if [[ -n "$MAX_PUBLIC_QUERY_HTTP_US_RATIO" ]]; then
  summary_args+=(--max-public-query-http-us-ratio-vs-baseline "$MAX_PUBLIC_QUERY_HTTP_US_RATIO")
fi
if [[ -n "$MAX_ALGEBRAIC_BYTES_PER_DOC_RATIO" ]]; then
  summary_args+=(--max-algebraic-bytes-per-doc-ratio-vs-baseline "$MAX_ALGEBRAIC_BYTES_PER_DOC_RATIO")
fi
if [[ -n "$MAX_CHURN_ALGEBRAIC_UPDATE_MS_RATIO" ]]; then
  summary_args+=(--max-churn-algebraic-update-ms-ratio-vs-baseline "$MAX_CHURN_ALGEBRAIC_UPDATE_MS_RATIO")
fi

printf "%q " "${summary_args[@]}" > "$OUT/summary-command.txt"
printf "\n" >> "$OUT/summary-command.txt"
"${summary_args[@]}" > "$OUT/summary.stdout" 2> "$OUT/algebraic-production-hardening-summary.jsonl"

if [[ "$RUN_UNIT_TEST" == "1" ]]; then
  echo "running unit_test"
  zig build unit-test > "$OUT/unit-test.stdout" 2> "$OUT/unit-test.stderr"
fi

echo "wrote $OUT"
