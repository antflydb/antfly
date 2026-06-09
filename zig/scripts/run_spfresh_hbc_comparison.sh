#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZIG_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUN_OPTIMIZED_GATE_LABELS="${RUN_OPTIMIZED_GATE_LABELS:-0}"
PRINT_OPTIMIZED_GATE_LABELS="${PRINT_OPTIMIZED_GATE_LABELS:-0}"
SAMPLES="${SAMPLES:-1}"
VECTORS="${VECTORS:-128}"
DIMS="${DIMS:-16}"
BATCH_SIZE="${BATCH_SIZE:-32}"
LEAF_SIZE="${LEAF_SIZE:-16}"
BRANCHING_FACTOR="${BRANCHING_FACTOR:-8}"
SEARCH_WIDTH="${SEARCH_WIDTH:-0}"
STORAGE="${STORAGE:-host}"
OVERWRITE_HOT_KEYS="${OVERWRITE_HOT_KEYS:-32}"
OVERWRITE_ROUNDS="${OVERWRITE_ROUNDS:-2}"
POST_WRITE_QUERIES="${POST_WRITE_QUERIES:-8}"
POST_WRITE_QUERY_ROUNDS="${POST_WRITE_QUERY_ROUNDS:-2}"
POST_WRITE_K="${POST_WRITE_K:-5}"
SKIP_VECTOR_STORE="${SKIP_VECTOR_STORE:-0}"
if [[ -z "${READ_QUERIES:-}" ]]; then
  if ((VECTORS >= 1000000)); then
    READ_QUERIES=100
  else
    READ_QUERIES=8
  fi
fi
if [[ -z "${READ_K:-}" ]]; then
  if ((VECTORS >= 1000000)); then
    READ_K=10
  else
    READ_K=5
  fi
fi
if [[ -z "${READ_DATASET_MODE:-}" ]]; then
  if ((VECTORS >= 1000000)); then
    READ_DATASET_MODE=procedural
  else
    READ_DATASET_MODE=materialized
  fi
fi
VDBB_VECTORS="${VDBB_VECTORS:-${VECTORS}}"
VDBB_DIMS="${VDBB_DIMS:-1536}"
VDBB_BATCH_SIZE="${VDBB_BATCH_SIZE:-500}"
VDBB_LEAF_SIZE="${VDBB_LEAF_SIZE:-168}"
VDBB_BRANCHING_FACTOR="${VDBB_BRANCHING_FACTOR:-168}"
if [[ -z "${VDBB_SEARCH_WIDTH:-}" ]]; then
  if [[ "${RUN_OPTIMIZED_GATE_LABELS}" == "1" ]]; then
    VDBB_SEARCH_WIDTH=256
  else
    VDBB_SEARCH_WIDTH="${SEARCH_WIDTH}"
  fi
fi
VDBB_POST_WRITE_QUERIES="${VDBB_POST_WRITE_QUERIES:-${POST_WRITE_QUERIES}}"
VDBB_POST_WRITE_QUERY_ROUNDS="${VDBB_POST_WRITE_QUERY_ROUNDS:-${POST_WRITE_QUERY_ROUNDS}}"
VDBB_POST_WRITE_K="${VDBB_POST_WRITE_K:-${POST_WRITE_K}}"
VDBB_POST_WRITE_RECALL_MODE="${VDBB_POST_WRITE_RECALL_MODE:-exact}"
if [[ -z "${FLAT_PROBE_COUNT:-}" ]]; then
  if ((SEARCH_WIDTH > 0)); then
    FLAT_PROBE_COUNT="${SEARCH_WIDTH}"
  else
    FLAT_PROBE_COUNT=8
  fi
fi
if [[ -z "${VDBB_FLAT_PROBE_COUNT:-}" ]]; then
  if ((VDBB_SEARCH_WIDTH > 0)); then
    VDBB_FLAT_PROBE_COUNT="${VDBB_SEARCH_WIDTH}"
  else
    VDBB_FLAT_PROBE_COUNT="${FLAT_PROBE_COUNT}"
  fi
fi
FLAT_BLOCK_TARGET_BLOCKS="${FLAT_BLOCK_TARGET_BLOCKS:-16}"
FLAT_BLOCK_MIN_SIZE="${FLAT_BLOCK_MIN_SIZE:-2}"
FLAT_BLOCK_MAX_SIZE="${FLAT_BLOCK_MAX_SIZE:-128}"
if ((FLAT_BLOCK_TARGET_BLOCKS < 1)); then
  FLAT_BLOCK_TARGET_BLOCKS=1
fi
if ((FLAT_BLOCK_MIN_SIZE < 1)); then
  FLAT_BLOCK_MIN_SIZE=1
fi
if ((FLAT_BLOCK_MAX_SIZE < FLAT_BLOCK_MIN_SIZE)); then
  FLAT_BLOCK_MAX_SIZE="${FLAT_BLOCK_MIN_SIZE}"
fi
ESTIMATED_POSTINGS=$(((VECTORS + LEAF_SIZE - 1) / LEAF_SIZE))
if ((ESTIMATED_POSTINGS < 1)); then
  ESTIMATED_POSTINGS=1
fi
if [[ -z "${FLAT_BLOCK_SIZE:-}" ]]; then
  FLAT_BLOCK_SIZE=$(((ESTIMATED_POSTINGS + FLAT_BLOCK_TARGET_BLOCKS - 1) / FLAT_BLOCK_TARGET_BLOCKS))
  if ((FLAT_BLOCK_SIZE < FLAT_BLOCK_MIN_SIZE)); then
    FLAT_BLOCK_SIZE="${FLAT_BLOCK_MIN_SIZE}"
  fi
  if ((FLAT_BLOCK_SIZE > FLAT_BLOCK_MAX_SIZE)); then
    FLAT_BLOCK_SIZE="${FLAT_BLOCK_MAX_SIZE}"
  fi
fi
if [[ -z "${FLAT_BLOCK_PROBE_COUNT:-}" ]]; then
  ESTIMATED_CENTROID_BLOCKS=$(((ESTIMATED_POSTINGS + FLAT_BLOCK_SIZE - 1) / FLAT_BLOCK_SIZE))
  FLAT_BLOCK_PROBE_COUNT=1
  while ((FLAT_BLOCK_PROBE_COUNT * FLAT_BLOCK_PROBE_COUNT < ESTIMATED_CENTROID_BLOCKS)); do
    FLAT_BLOCK_PROBE_COUNT=$((FLAT_BLOCK_PROBE_COUNT + 1))
  done
  FLAT_BLOCK_PROBE_COUNT=$((FLAT_BLOCK_PROBE_COUNT * 2))
  MIN_BLOCK_PROBES_FOR_POSTINGS=$(((FLAT_PROBE_COUNT + FLAT_BLOCK_SIZE - 1) / FLAT_BLOCK_SIZE))
  if ((FLAT_BLOCK_PROBE_COUNT < MIN_BLOCK_PROBES_FOR_POSTINGS)); then
    FLAT_BLOCK_PROBE_COUNT="${MIN_BLOCK_PROBES_FOR_POSTINGS}"
  fi
  MAX_PRUNED_BLOCK_PROBES=$(((ESTIMATED_CENTROID_BLOCKS + 1) / 2))
  if ((MAX_PRUNED_BLOCK_PROBES < 1)); then
    MAX_PRUNED_BLOCK_PROBES=1
  fi
  if ((MAX_PRUNED_BLOCK_PROBES < MIN_BLOCK_PROBES_FOR_POSTINGS)); then
    MAX_PRUNED_BLOCK_PROBES="${MIN_BLOCK_PROBES_FOR_POSTINGS}"
  fi
  if ((FLAT_BLOCK_PROBE_COUNT > MAX_PRUNED_BLOCK_PROBES)); then
    FLAT_BLOCK_PROBE_COUNT="${MAX_PRUNED_BLOCK_PROBES}"
  fi
  if ((FLAT_BLOCK_PROBE_COUNT > ESTIMATED_CENTROID_BLOCKS)); then
    FLAT_BLOCK_PROBE_COUNT="${ESTIMATED_CENTROID_BLOCKS}"
  fi
fi
ESTIMATED_VDBB_POSTINGS=$(((VDBB_VECTORS + VDBB_LEAF_SIZE - 1) / VDBB_LEAF_SIZE))
if ((ESTIMATED_VDBB_POSTINGS < 1)); then
  ESTIMATED_VDBB_POSTINGS=1
fi
if [[ -z "${VDBB_FLAT_BLOCK_SIZE:-}" ]]; then
  VDBB_FLAT_BLOCK_SIZE=$(((ESTIMATED_VDBB_POSTINGS + FLAT_BLOCK_TARGET_BLOCKS - 1) / FLAT_BLOCK_TARGET_BLOCKS))
  if ((VDBB_FLAT_BLOCK_SIZE < FLAT_BLOCK_MIN_SIZE)); then
    VDBB_FLAT_BLOCK_SIZE="${FLAT_BLOCK_MIN_SIZE}"
  fi
  if ((VDBB_FLAT_BLOCK_SIZE > FLAT_BLOCK_MAX_SIZE)); then
    VDBB_FLAT_BLOCK_SIZE="${FLAT_BLOCK_MAX_SIZE}"
  fi
fi
if [[ -z "${VDBB_FLAT_BLOCK_PROBE_COUNT:-}" ]]; then
  ESTIMATED_VDBB_CENTROID_BLOCKS=$(((ESTIMATED_VDBB_POSTINGS + VDBB_FLAT_BLOCK_SIZE - 1) / VDBB_FLAT_BLOCK_SIZE))
  VDBB_FLAT_BLOCK_PROBE_COUNT=1
  while ((VDBB_FLAT_BLOCK_PROBE_COUNT * VDBB_FLAT_BLOCK_PROBE_COUNT < ESTIMATED_VDBB_CENTROID_BLOCKS)); do
    VDBB_FLAT_BLOCK_PROBE_COUNT=$((VDBB_FLAT_BLOCK_PROBE_COUNT + 1))
  done
  VDBB_FLAT_BLOCK_PROBE_COUNT=$((VDBB_FLAT_BLOCK_PROBE_COUNT * 2))
  VDBB_MIN_BLOCK_PROBES_FOR_POSTINGS=$(((VDBB_FLAT_PROBE_COUNT + VDBB_FLAT_BLOCK_SIZE - 1) / VDBB_FLAT_BLOCK_SIZE))
  if ((VDBB_FLAT_BLOCK_PROBE_COUNT < VDBB_MIN_BLOCK_PROBES_FOR_POSTINGS)); then
    VDBB_FLAT_BLOCK_PROBE_COUNT="${VDBB_MIN_BLOCK_PROBES_FOR_POSTINGS}"
  fi
  if ((VDBB_FLAT_BLOCK_PROBE_COUNT < VDBB_FLAT_PROBE_COUNT)); then
    VDBB_FLAT_BLOCK_PROBE_COUNT="${VDBB_FLAT_PROBE_COUNT}"
  fi
  VDBB_MAX_PRUNED_BLOCK_PROBES=$(((ESTIMATED_VDBB_CENTROID_BLOCKS + 1) / 2))
  if ((VDBB_MAX_PRUNED_BLOCK_PROBES < 1)); then
    VDBB_MAX_PRUNED_BLOCK_PROBES=1
  fi
  if ((VDBB_MAX_PRUNED_BLOCK_PROBES < VDBB_MIN_BLOCK_PROBES_FOR_POSTINGS)); then
    VDBB_MAX_PRUNED_BLOCK_PROBES="${VDBB_MIN_BLOCK_PROBES_FOR_POSTINGS}"
  fi
  if ((VDBB_MAX_PRUNED_BLOCK_PROBES < VDBB_FLAT_PROBE_COUNT)); then
    VDBB_MAX_PRUNED_BLOCK_PROBES="${VDBB_FLAT_PROBE_COUNT}"
  fi
  if ((VDBB_FLAT_BLOCK_PROBE_COUNT > VDBB_MAX_PRUNED_BLOCK_PROBES)); then
    VDBB_FLAT_BLOCK_PROBE_COUNT="${VDBB_MAX_PRUNED_BLOCK_PROBES}"
  fi
  if ((VDBB_FLAT_BLOCK_PROBE_COUNT > ESTIMATED_VDBB_CENTROID_BLOCKS)); then
    VDBB_FLAT_BLOCK_PROBE_COUNT="${ESTIMATED_VDBB_CENTROID_BLOCKS}"
  fi
fi
MAX_POSTING_OVERLAY_CACHE_BYTES="${MAX_POSTING_OVERLAY_CACHE_BYTES:-8388608}"
MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES="${MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES:-0}"
REPAIR_DIRTY_REASSIGNMENTS="${REPAIR_DIRTY_REASSIGNMENTS:-${OVERWRITE_HOT_KEYS}}"
MIXED_REPAIR_DIRTY_REASSIGNMENTS="${MIXED_REPAIR_DIRTY_REASSIGNMENTS:-${REPAIR_DIRTY_REASSIGNMENTS}}"
REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT="${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT:-0.0}"
REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS="${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS:-0}"
REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS="${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS:-0}"
OVERFULL_REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS="${OVERFULL_REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS:-4}"
OVERFULL_REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS="${OVERFULL_REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS:-1}"
REPAIR_MIN_DELTA_RECORDS_TO_FOLD="${REPAIR_MIN_DELTA_RECORDS_TO_FOLD:-999}"
REPAIR_MAX_DELTA_TAIL_POSTINGS="${REPAIR_MAX_DELTA_TAIL_POSTINGS:-1024}"
AUTO_MIN_DELTA_RECORDS_TO_FOLD="${AUTO_MIN_DELTA_RECORDS_TO_FOLD:-${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}}"
AUTO_MIN_TOMBSTONE_RECORDS_TO_FOLD="${AUTO_MIN_TOMBSTONE_RECORDS_TO_FOLD:-999}"
AUTO_MIN_DELTA_TO_BASE_RATIO_BPS="${AUTO_MIN_DELTA_TO_BASE_RATIO_BPS:-0}"
AUTO_SPLIT_FULL_MAX_POSTINGS="${AUTO_SPLIT_FULL_MAX_POSTINGS:-8}"
if [[ -z "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES:-}" ]]; then
  if [[ "${RUN_OPTIMIZED_GATE_LABELS}" == "1" ]]; then
    AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES=512
  else
    AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES="${AUTO_SPLIT_FULL_MAX_POSTINGS}"
  fi
fi
ENABLE_VDBB_PROCEDURAL="${ENABLE_VDBB_PROCEDURAL:-0}"
ENABLE_VDBB_1M_PROCEDURAL="${ENABLE_VDBB_1M_PROCEDURAL:-${RUN_OPTIMIZED_GATE_LABELS}}"
ENABLE_VDBB_PROCEDURAL_MUTATIONS="${ENABLE_VDBB_PROCEDURAL_MUTATIONS:-0}"
ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS="${ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS:-${RUN_OPTIMIZED_GATE_LABELS}}"
VDBB_1M_VECTORS="${VDBB_1M_VECTORS:-1000000}"
VDBB_1M_POST_WRITE_QUERIES="${VDBB_1M_POST_WRITE_QUERIES:-100}"
VDBB_1M_POST_WRITE_QUERY_ROUNDS="${VDBB_1M_POST_WRITE_QUERY_ROUNDS:-${VDBB_POST_WRITE_QUERY_ROUNDS}}"
VDBB_1M_POST_WRITE_K="${VDBB_1M_POST_WRITE_K:-10}"
VDBB_1M_POST_WRITE_RECALL_MODE="${VDBB_1M_POST_WRITE_RECALL_MODE:-exact}"
VDBB_1M_EXACT_RECALL_MAX_OPS="${VDBB_1M_EXACT_RECALL_MAX_OPS:-20000000000}"
ALLOW_SLOW_VDBB_1M_EXACT_RECALL="${ALLOW_SLOW_VDBB_1M_EXACT_RECALL:-0}"
PREBUILD_VDBB_1M_EXACT_TRUTH_CACHES="${PREBUILD_VDBB_1M_EXACT_TRUTH_CACHES:-0}"
VDBB_1M_FLAT_PROBE_COUNT="${VDBB_1M_FLAT_PROBE_COUNT:-${VDBB_FLAT_PROBE_COUNT}}"
ESTIMATED_VDBB_1M_POSTINGS=$(((VDBB_1M_VECTORS + VDBB_LEAF_SIZE - 1) / VDBB_LEAF_SIZE))
if ((ESTIMATED_VDBB_1M_POSTINGS < 1)); then
  ESTIMATED_VDBB_1M_POSTINGS=1
fi
if [[ -z "${VDBB_1M_FLAT_BLOCK_SIZE:-}" ]]; then
  VDBB_1M_FLAT_BLOCK_SIZE=$(((ESTIMATED_VDBB_1M_POSTINGS + FLAT_BLOCK_TARGET_BLOCKS - 1) / FLAT_BLOCK_TARGET_BLOCKS))
  if ((VDBB_1M_FLAT_BLOCK_SIZE < FLAT_BLOCK_MIN_SIZE)); then
    VDBB_1M_FLAT_BLOCK_SIZE="${FLAT_BLOCK_MIN_SIZE}"
  fi
  if ((VDBB_1M_FLAT_BLOCK_SIZE > FLAT_BLOCK_MAX_SIZE)); then
    VDBB_1M_FLAT_BLOCK_SIZE="${FLAT_BLOCK_MAX_SIZE}"
  fi
fi
if [[ -z "${VDBB_1M_FLAT_BLOCK_PROBE_COUNT:-}" ]]; then
  ESTIMATED_VDBB_1M_CENTROID_BLOCKS=$(((ESTIMATED_VDBB_1M_POSTINGS + VDBB_1M_FLAT_BLOCK_SIZE - 1) / VDBB_1M_FLAT_BLOCK_SIZE))
  VDBB_1M_FLAT_BLOCK_PROBE_COUNT=1
  while ((VDBB_1M_FLAT_BLOCK_PROBE_COUNT * VDBB_1M_FLAT_BLOCK_PROBE_COUNT < ESTIMATED_VDBB_1M_CENTROID_BLOCKS)); do
    VDBB_1M_FLAT_BLOCK_PROBE_COUNT=$((VDBB_1M_FLAT_BLOCK_PROBE_COUNT + 1))
  done
  VDBB_1M_FLAT_BLOCK_PROBE_COUNT=$((VDBB_1M_FLAT_BLOCK_PROBE_COUNT * 2))
  VDBB_1M_MIN_BLOCK_PROBES_FOR_POSTINGS=$(((VDBB_1M_FLAT_PROBE_COUNT + VDBB_1M_FLAT_BLOCK_SIZE - 1) / VDBB_1M_FLAT_BLOCK_SIZE))
  if ((VDBB_1M_FLAT_BLOCK_PROBE_COUNT < VDBB_1M_MIN_BLOCK_PROBES_FOR_POSTINGS)); then
    VDBB_1M_FLAT_BLOCK_PROBE_COUNT="${VDBB_1M_MIN_BLOCK_PROBES_FOR_POSTINGS}"
  fi
  if ((VDBB_1M_FLAT_BLOCK_PROBE_COUNT < VDBB_1M_FLAT_PROBE_COUNT)); then
    VDBB_1M_FLAT_BLOCK_PROBE_COUNT="${VDBB_1M_FLAT_PROBE_COUNT}"
  fi
  VDBB_1M_MAX_PRUNED_BLOCK_PROBES=$(((ESTIMATED_VDBB_1M_CENTROID_BLOCKS + 1) / 2))
  if ((VDBB_1M_MAX_PRUNED_BLOCK_PROBES < 1)); then
    VDBB_1M_MAX_PRUNED_BLOCK_PROBES=1
  fi
  if ((VDBB_1M_MAX_PRUNED_BLOCK_PROBES < VDBB_1M_MIN_BLOCK_PROBES_FOR_POSTINGS)); then
    VDBB_1M_MAX_PRUNED_BLOCK_PROBES="${VDBB_1M_MIN_BLOCK_PROBES_FOR_POSTINGS}"
  fi
  if ((VDBB_1M_MAX_PRUNED_BLOCK_PROBES < VDBB_1M_FLAT_PROBE_COUNT)); then
    VDBB_1M_MAX_PRUNED_BLOCK_PROBES="${VDBB_1M_FLAT_PROBE_COUNT}"
  fi
  if ((VDBB_1M_FLAT_BLOCK_PROBE_COUNT > VDBB_1M_MAX_PRUNED_BLOCK_PROBES)); then
    VDBB_1M_FLAT_BLOCK_PROBE_COUNT="${VDBB_1M_MAX_PRUNED_BLOCK_PROBES}"
  fi
  if ((VDBB_1M_FLAT_BLOCK_PROBE_COUNT > ESTIMATED_VDBB_1M_CENTROID_BLOCKS)); then
    VDBB_1M_FLAT_BLOCK_PROBE_COUNT="${ESTIMATED_VDBB_1M_CENTROID_BLOCKS}"
  fi
fi

RESULT_DIR="${RESULT_DIR:-${ZIG_ROOT}/bench/results/spfresh-hbc-comparison}"
if [[ "${RESULT_DIR}" != /* ]]; then
  RESULT_DIR="${PWD}/${RESULT_DIR}"
fi
RESULT_FILE="${RESULT_FILE:-${RESULT_DIR}/spfresh-hbc-comparison.jsonl}"
READ_EXACT_TRUTH_CACHE_PATH="${READ_EXACT_TRUTH_CACHE_PATH:-${RESULT_DIR}/read-exact-truth-${VECTORS}v-${DIMS}d-${READ_QUERIES}q-${READ_K}k-${READ_DATASET_MODE}.bin}"
if [[ "${READ_EXACT_TRUTH_CACHE_PATH}" != /* ]]; then
  READ_EXACT_TRUTH_CACHE_PATH="${PWD}/${READ_EXACT_TRUTH_CACHE_PATH}"
fi
POST_WRITE_TRUTH_CACHE_DIR="${POST_WRITE_TRUTH_CACHE_DIR:-${RESULT_DIR}/post-write-truth}"
if [[ "${POST_WRITE_TRUTH_CACHE_DIR}" != /* ]]; then
  POST_WRITE_TRUTH_CACHE_DIR="${PWD}/${POST_WRITE_TRUTH_CACHE_DIR}"
fi
RUN_LABELS="${RUN_LABELS:-}"
if [[ "${RUN_OPTIMIZED_GATE_LABELS}" == "1" && -z "${RUN_LABELS}" ]]; then
  RUN_LABELS="\
write_packed_hbc_hbc \
write_same_leaf_packed_hbc_hbc \
write_random_packed_hbc_hbc \
write_semantic_packed_hbc_hbc \
write_append_packed_hbc_hbc \
write_mixed_packed_hbc_hbc \
write_same_leaf_base_delta_hbc \
write_base_delta_hbc_auto_split_full \
write_vdbb_1m_procedural_packed_hbc_hbc \
write_vdbb_1m_procedural_base_delta_hbc_bounded_maintenance \
write_vdbb_1m_procedural_base_delta_flat_rabitq \
write_vdbb_1m_procedural_base_delta_two_level_rabitq_bounded_maintenance \
write_vdbb_1m_procedural_hot_packed_hbc_hbc \
write_vdbb_1m_procedural_hot_base_delta_hbc_bounded_maintenance \
write_vdbb_1m_procedural_random_packed_hbc_hbc \
write_vdbb_1m_procedural_random_base_delta_hbc_bounded_maintenance \
write_vdbb_1m_procedural_semantic_packed_hbc_hbc \
write_vdbb_1m_procedural_semantic_base_delta_hbc_bounded_maintenance \
write_vdbb_1m_procedural_append_packed_hbc_hbc \
write_vdbb_1m_procedural_append_base_delta_hbc_bounded_maintenance \
write_vdbb_1m_procedural_mixed_packed_hbc_hbc \
write_vdbb_1m_procedural_mixed_base_delta_hbc_bounded_maintenance \
read_packed_hbc_hbc \
read_base_delta_flat_rabitq_repaired \
read_base_delta_two_level_rabitq_repaired"
fi
if [[ "${PRINT_OPTIMIZED_GATE_LABELS}" == "1" ]]; then
  printf '%s\n' ${RUN_LABELS//,/ }
  exit 0
fi
APPEND_RESULTS="${APPEND_RESULTS:-0}"

mkdir -p "${RESULT_DIR}"
mkdir -p "${POST_WRITE_TRUTH_CACHE_DIR}"
if [[ "${APPEND_RESULTS}" != "1" ]]; then
  : > "${RESULT_FILE}"
fi

label_selected() {
  local label="$1"
  if [[ -z "${RUN_LABELS}" ]]; then
    return 0
  fi
  local normalized=" ${RUN_LABELS//,/ } "
  [[ "${normalized}" == *" ${label} "* ]]
}

vdbb_1m_exact_recall_cache_path() {
  printf '%s/write-vdbb-1m-procedural-online-%sv-%sd-%sq-%sk.bin' \
    "${POST_WRITE_TRUTH_CACHE_DIR}" \
    "${VDBB_1M_VECTORS}" \
    "${VDBB_DIMS}" \
    "${VDBB_1M_POST_WRITE_QUERIES}" \
    "${VDBB_1M_POST_WRITE_K}"
}

vdbb_1m_mutation_truth_cache_path() {
  local short="$1"
  printf '%s/write_vdbb_1m_procedural-%s-%sv-%sd-%sq-%sk.bin' \
    "${POST_WRITE_TRUTH_CACHE_DIR}" \
    "${short}" \
    "${VDBB_1M_VECTORS}" \
    "${VDBB_DIMS}" \
    "${VDBB_1M_POST_WRITE_QUERIES}" \
    "${VDBB_1M_POST_WRITE_K}"
}

vdbb_1m_online_label_selected() {
  label_selected "write_vdbb_1m_procedural_packed_hbc_hbc" ||
    label_selected "write_vdbb_1m_procedural_base_delta_hbc" ||
    label_selected "write_vdbb_1m_procedural_base_delta_hbc_bounded_maintenance" ||
    label_selected "write_vdbb_1m_procedural_base_delta_flat_rabitq" ||
    label_selected "write_vdbb_1m_procedural_base_delta_two_level_rabitq" ||
    label_selected "write_vdbb_1m_procedural_base_delta_two_level_rabitq_bounded_maintenance"
}

vdbb_1m_mutation_distribution_selected() {
  local short="$1"
  label_selected "write_vdbb_1m_procedural_${short}_packed_hbc_hbc" ||
    label_selected "write_vdbb_1m_procedural_${short}_base_delta_two_level_rabitq_bounded_maintenance"
}

vdbb_1m_enabled_label_selected() {
  if [[ "${ENABLE_VDBB_1M_PROCEDURAL}" == "1" ]] && vdbb_1m_online_label_selected; then
    return 0
  fi
  if [[ "${ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS}" == "1" ]]; then
    local short
    for short in hot random semantic append mixed; do
      if vdbb_1m_mutation_distribution_selected "${short}"; then
        return 0
      fi
    done
  fi
  return 1
}

vdbb_1m_exact_recall_selected_caches_present() {
  local missing=0
  if [[ "${ENABLE_VDBB_1M_PROCEDURAL}" == "1" ]] && vdbb_1m_online_label_selected; then
    local online_cache
    online_cache="$(vdbb_1m_exact_recall_cache_path)"
    if [[ ! -f "${online_cache}" ]]; then
      printf 'spfresh_comparison missing_vdbb_1m_exact_truth_cache=%s\n' "${online_cache}" >&2
      missing=1
    fi
  fi
  if [[ "${ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS}" == "1" ]]; then
    local short cache
    for short in hot random semantic append mixed; do
      if vdbb_1m_mutation_distribution_selected "${short}"; then
        cache="$(vdbb_1m_mutation_truth_cache_path "${short}")"
        if [[ ! -f "${cache}" ]]; then
          printf 'spfresh_comparison missing_vdbb_1m_exact_truth_cache=%s\n' "${cache}" >&2
          missing=1
        fi
      fi
    done
  fi
  [[ "${missing}" == "0" ]]
}

guard_vdbb_1m_exact_recall() {
  if [[ "${ENABLE_VDBB_1M_PROCEDURAL}" != "1" && "${ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS}" != "1" ]]; then
    return 0
  fi
  if [[ "${VDBB_1M_POST_WRITE_RECALL_MODE}" != "exact" ]]; then
    return 0
  fi
  if [[ "${PREBUILD_VDBB_1M_EXACT_TRUTH_CACHES}" == "1" ]]; then
    return 0
  fi
  if ! vdbb_1m_enabled_label_selected; then
    return 0
  fi
  local exact_ops=$((VDBB_1M_VECTORS * VDBB_1M_POST_WRITE_QUERIES * VDBB_DIMS))
  if ((exact_ops <= VDBB_1M_EXACT_RECALL_MAX_OPS)); then
    return 0
  fi
  if vdbb_1m_exact_recall_selected_caches_present; then
    printf 'spfresh_comparison info=cached_vdbb_1m_exact_recall ops=%d threshold=%d\n' "${exact_ops}" "${VDBB_1M_EXACT_RECALL_MAX_OPS}" >&2
    return 0
  fi
  if [[ "${ALLOW_SLOW_VDBB_1M_EXACT_RECALL}" == "1" ]]; then
    printf 'spfresh_comparison warning=slow_vdbb_1m_exact_recall ops=%d threshold=%d\n' "${exact_ops}" "${VDBB_1M_EXACT_RECALL_MAX_OPS}" >&2
    return 0
  fi
  printf 'spfresh_comparison error=slow_vdbb_1m_exact_recall ops=%d threshold=%d\n' "${exact_ops}" "${VDBB_1M_EXACT_RECALL_MAX_OPS}" >&2
  printf 'Set VDBB_1M_POST_WRITE_RECALL_MODE=self_hit for a 1M performance diagnostic, or set ALLOW_SLOW_VDBB_1M_EXACT_RECALL=1 to run the brute-force synthetic exact oracle.\n' >&2
  return 1
}

run_bench() {
  local label="$1"
  shift
  if ! label_selected "${label}"; then
    return 0
  fi
  printf 'spfresh_comparison label=%s\n' "${label}" >&2
  local tmp
  tmp="$(mktemp "${RESULT_DIR}/${label}.XXXXXX")"
  if "$@" > "${tmp}"; then
    :
  else
    local status=$?
    printf 'spfresh_comparison label=%s failed status=%s tmp=%s\n' "${label}" "${status}" "${tmp}" >&2
    rm -f "${tmp}"
    return 0
  fi
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" == \{* ]]; then
      printf '{"comparison_label":"%s",%s\n' "${label}" "${line#\{}" >> "${RESULT_FILE}"
    fi
  done < "${tmp}"
  rm -f "${tmp}"
}

run_truth_cache() {
  local label="$1"
  shift
  printf 'spfresh_comparison truth_cache_label=%s\n' "${label}" >&2
  "$@"
}

run_vdbb_procedural_mutation_rows() {
  local prefix="$1"
  local flat_block_size="$2"
  local flat_block_probe_count="$3"
  local flat_probe_count="$4"
  shift
  shift
  shift
  shift
  local -a common_args=("$@")
  local -a workloads=(
    "hot:overwrite_hot_vectors_warm"
    "random:overwrite_random_vectors_warm"
    "semantic:overwrite_semantic_drift_vectors_warm"
    "append:append_streaming_warm"
    "mixed:mixed_insert_delete_update_warm"
  )
  local cache_vectors="${VDBB_VECTORS}"
  local cache_queries="${VDBB_POST_WRITE_QUERIES}"
  local cache_k="${VDBB_POST_WRITE_K}"
  if [[ "${prefix}" == write_vdbb_1m_procedural* ]]; then
    cache_vectors="${VDBB_1M_VECTORS}"
    cache_queries="${VDBB_1M_POST_WRITE_QUERIES}"
    cache_k="${VDBB_1M_POST_WRITE_K}"
  fi
  local entry short workload
  for entry in "${workloads[@]}"; do
    short="${entry%%:*}"
    workload="${entry#*:}"
    local truth_cache_path="${POST_WRITE_TRUTH_CACHE_DIR}/${prefix}-${short}-${cache_vectors}v-${VDBB_DIMS}d-${cache_queries}q-${cache_k}k.bin"
    run_bench "${prefix}_${short}_packed_hbc_hbc" \
      zig build hbc-write-bench -- \
      "${common_args[@]}" \
      --workload "${workload}" \
      --post-write-truth-cache-path "${truth_cache_path}" \
      --overwrite-hot-keys "${OVERWRITE_HOT_KEYS}" \
      --overwrite-rounds "${OVERWRITE_ROUNDS}" \
      --posting-storage packed_hbc \
      --no-coalesce-overwrite-leaf-writes

    run_bench "${prefix}_${short}_base_delta_two_level_rabitq_bounded_maintenance" \
      zig build hbc-write-bench -- \
      "${common_args[@]}" \
      --workload "${workload}" \
      --post-write-truth-cache-path "${truth_cache_path}" \
      --overwrite-hot-keys "${OVERWRITE_HOT_KEYS}" \
      --overwrite-rounds "${OVERWRITE_ROUNDS}" \
      --posting-storage base_delta \
      --lazy-posting-maintenance \
      --repair-postings-after-write \
      --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
      --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
      --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
      --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
      --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
      --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
      --repair-rebalance-layout \
      --repair-split-full-postings \
      --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
      --defer-leaf-splits-to-posting-maintenance \
      --centroid-directory two_level_rabitq \
      --flat-centroid-block-size "${flat_block_size}" \
      --flat-centroid-block-probe-count "${flat_block_probe_count}" \
      --flat-centroid-probe-count "${flat_probe_count}"

    local -a hbc_repair_args=(
      --repair-postings-after-write
      --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}"
      --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}"
      --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}"
      --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}"
      --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}"
      --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}"
      --repair-rebalance-layout
      --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}"
    )
    if [[ "${short}" == "append" ]]; then
      hbc_repair_args=(
        --repair-rebalance-layout
        --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}"
      )
    fi

    run_bench "${prefix}_${short}_base_delta_hbc_bounded_maintenance" \
      zig build hbc-write-bench -- \
      "${common_args[@]}" \
      --workload "${workload}" \
      --post-write-truth-cache-path "${truth_cache_path}" \
      --overwrite-hot-keys "${OVERWRITE_HOT_KEYS}" \
      --overwrite-rounds "${OVERWRITE_ROUNDS}" \
      --posting-storage base_delta \
      --lazy-posting-maintenance \
      "${hbc_repair_args[@]}" \
      --defer-leaf-splits-to-posting-maintenance
  done
}

guard_vdbb_1m_exact_recall

cd "${ZIG_ROOT}"

COMMON_WRITE_ARGS=(
  --samples "${SAMPLES}"
  --vectors "${VECTORS}"
  --dims "${DIMS}"
  --batch-size "${BATCH_SIZE}"
  --leaf-size "${LEAF_SIZE}"
  --branching-factor "${BRANCHING_FACTOR}"
  --search-width "${SEARCH_WIDTH}"
  --storage "${STORAGE}"
  --workload overwrite_hot_vectors_warm
  --overwrite-hot-keys "${OVERWRITE_HOT_KEYS}"
  --overwrite-rounds "${OVERWRITE_ROUNDS}"
  --post-write-queries "${POST_WRITE_QUERIES}"
  --post-write-query-rounds "${POST_WRITE_QUERY_ROUNDS}"
  --post-write-k "${POST_WRITE_K}"
  --max-posting-overlay-cache-bytes "${MAX_POSTING_OVERLAY_CACHE_BYTES}"
  --max-posting-overlay-cache-entry-bytes "${MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES}"
)

COMMON_READ_ARGS=(
  --samples "${SAMPLES}"
  --vectors "${VECTORS}"
  --dims "${DIMS}"
  --queries "${READ_QUERIES}"
  --k "${READ_K}"
  --batch-size "${BATCH_SIZE}"
  --leaf-size "${LEAF_SIZE}"
  --branching-factor "${BRANCHING_FACTOR}"
  --search-width "${SEARCH_WIDTH}"
  --storage "${STORAGE}"
  --build bulk_build
  --dataset-mode "${READ_DATASET_MODE}"
  --exact-truth-cache-path "${READ_EXACT_TRUTH_CACHE_PATH}"
  --max-posting-overlay-cache-bytes "${MAX_POSTING_OVERLAY_CACHE_BYTES}"
  --max-posting-overlay-cache-entry-bytes "${MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES}"
)

COMMON_VDBB_WRITE_ARGS=(
  --samples "${SAMPLES}"
  --vectors "${VDBB_VECTORS}"
  --dims "${VDBB_DIMS}"
  --batch-size "${VDBB_BATCH_SIZE}"
  --leaf-size "${VDBB_LEAF_SIZE}"
  --branching-factor "${VDBB_BRANCHING_FACTOR}"
  --search-width "${VDBB_SEARCH_WIDTH}"
  --storage "${STORAGE}"
  --workload batch_apply_dense_external_vectors_warm
  --post-write-queries "${VDBB_POST_WRITE_QUERIES}"
  --post-write-query-rounds "${VDBB_POST_WRITE_QUERY_ROUNDS}"
  --post-write-k "${VDBB_POST_WRITE_K}"
  --post-write-recall-mode "${VDBB_POST_WRITE_RECALL_MODE}"
  --post-write-truth-cache-path "${POST_WRITE_TRUTH_CACHE_DIR}/write-vdbb-procedural-online-${VDBB_VECTORS}v-${VDBB_DIMS}d-${VDBB_POST_WRITE_QUERIES}q-${VDBB_POST_WRITE_K}k.bin"
  --max-posting-overlay-cache-bytes "${MAX_POSTING_OVERLAY_CACHE_BYTES}"
  --max-posting-overlay-cache-entry-bytes "${MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES}"
)

COMMON_VDBB_PROCEDURAL_WRITE_ARGS=(
  --samples "${SAMPLES}"
  --vectors "${VDBB_VECTORS}"
  --dims "${VDBB_DIMS}"
  --batch-size "${VDBB_BATCH_SIZE}"
  --leaf-size "${VDBB_LEAF_SIZE}"
  --branching-factor "${VDBB_BRANCHING_FACTOR}"
  --search-width "${VDBB_SEARCH_WIDTH}"
  --storage "${STORAGE}"
  --workload online_batches_dense_external_vectors_empty
  --dataset-mode procedural
  --skip-vector-store
  --post-write-queries "${VDBB_POST_WRITE_QUERIES}"
  --post-write-query-rounds "${VDBB_POST_WRITE_QUERY_ROUNDS}"
  --post-write-k "${VDBB_POST_WRITE_K}"
  --post-write-recall-mode "${VDBB_POST_WRITE_RECALL_MODE}"
  --max-posting-overlay-cache-bytes "${MAX_POSTING_OVERLAY_CACHE_BYTES}"
  --max-posting-overlay-cache-entry-bytes "${MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES}"
)

COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS=(
  --samples "${SAMPLES}"
  --vectors "${VDBB_1M_VECTORS}"
  --dims "${VDBB_DIMS}"
  --batch-size "${VDBB_BATCH_SIZE}"
  --leaf-size "${VDBB_LEAF_SIZE}"
  --branching-factor "${VDBB_BRANCHING_FACTOR}"
  --search-width "${VDBB_SEARCH_WIDTH}"
  --storage "${STORAGE}"
  --workload bulk_build_external_vectors_sequential_empty
  --dataset-mode procedural
  --skip-vector-store
  --post-write-queries "${VDBB_1M_POST_WRITE_QUERIES}"
  --post-write-query-rounds "${VDBB_1M_POST_WRITE_QUERY_ROUNDS}"
  --post-write-k "${VDBB_1M_POST_WRITE_K}"
  --post-write-recall-mode "${VDBB_1M_POST_WRITE_RECALL_MODE}"
  --post-write-truth-cache-path "${POST_WRITE_TRUTH_CACHE_DIR}/write-vdbb-1m-procedural-online-${VDBB_1M_VECTORS}v-${VDBB_DIMS}d-${VDBB_1M_POST_WRITE_QUERIES}q-${VDBB_1M_POST_WRITE_K}k.bin"
  --max-posting-overlay-cache-bytes "${MAX_POSTING_OVERLAY_CACHE_BYTES}"
  --max-posting-overlay-cache-entry-bytes "${MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES}"
)

if [[ "${SKIP_VECTOR_STORE}" == "1" ]]; then
  COMMON_WRITE_ARGS+=(--skip-vector-store)
  COMMON_VDBB_WRITE_ARGS+=(--skip-vector-store)
fi

prebuild_vdbb_1m_exact_truth_caches() {
  if [[ "${VDBB_1M_POST_WRITE_RECALL_MODE}" != "exact" ]]; then
    printf 'spfresh_comparison error=prebuild_vdbb_1m_exact_truth_requires_exact_mode mode=%s\n' "${VDBB_1M_POST_WRITE_RECALL_MODE}" >&2
    return 1
  fi
  if [[ "${ENABLE_VDBB_1M_PROCEDURAL}" == "1" ]] && vdbb_1m_online_label_selected; then
    run_truth_cache "write_vdbb_1m_procedural_online_exact_truth_cache" \
      zig build hbc-write-bench -- \
      "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
      --post-write-truth-cache-only
  fi
  if [[ "${ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS}" == "1" ]]; then
    local short workload truth_cache_path
    for short in hot random semantic append mixed; do
      if ! vdbb_1m_mutation_distribution_selected "${short}"; then
        continue
      fi
      case "${short}" in
        hot) workload="overwrite_hot_vectors_warm" ;;
        random) workload="overwrite_random_vectors_warm" ;;
        semantic) workload="overwrite_semantic_drift_vectors_warm" ;;
        append) workload="append_streaming_warm" ;;
        mixed) workload="mixed_insert_delete_update_warm" ;;
        *) return 1 ;;
      esac
      truth_cache_path="$(vdbb_1m_mutation_truth_cache_path "${short}")"
      run_truth_cache "write_vdbb_1m_procedural_${short}_exact_truth_cache" \
        zig build hbc-write-bench -- \
        "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
        --workload "${workload}" \
        --post-write-truth-cache-path "${truth_cache_path}" \
        --overwrite-hot-keys "${OVERWRITE_HOT_KEYS}" \
        --overwrite-rounds "${OVERWRITE_ROUNDS}" \
        --post-write-truth-cache-only
    done
  fi
}

if [[ "${PREBUILD_VDBB_1M_EXACT_TRUTH_CACHES}" == "1" ]]; then
  prebuild_vdbb_1m_exact_truth_caches
  printf 'spfresh_comparison_truth_cache_result=%s\n' "${POST_WRITE_TRUTH_CACHE_DIR}" >&2
  exit 0
fi

run_bench "write_packed_hbc_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage packed_hbc \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_base_delta_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-min-delta-records-to-fold 1 \
  --repair-min-tombstone-records-to-fold 0 \
  --repair-min-delta-to-base-ratio-bps 0

run_bench "write_base_delta_hbc_lazy_fold" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance

run_bench "write_base_delta_hbc_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
  --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance

run_bench "write_base_delta_hbc_auto_split_full" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --auto-posting-maintenance-max-postings "${AUTO_SPLIT_FULL_MAX_POSTINGS}" \
  --auto-posting-maintenance-min-delta-records-to-fold "${AUTO_MIN_DELTA_RECORDS_TO_FOLD}" \
  --auto-posting-maintenance-min-tombstone-records-to-fold "${AUTO_MIN_TOMBSTONE_RECORDS_TO_FOLD}" \
  --auto-posting-maintenance-min-delta-to-base-ratio-bps "${AUTO_MIN_DELTA_TO_BASE_RATIO_BPS}" \
  --auto-posting-maintenance-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --auto-posting-maintenance-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --auto-posting-maintenance-split-full-postings \
  --auto-posting-maintenance-min-postings-at-capacity-to-run 1 \
  --auto-posting-maintenance-max-boundary-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --auto-posting-maintenance-boundary-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}"

run_bench "write_base_delta_hbc_reassign_overfull" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-allow-overfull \
  --repair-dirty-reassignment-max-overfull-postings "${OVERFULL_REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
  --repair-dirty-reassignment-max-over-capacity-members "${OVERFULL_REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}"

run_bench "write_base_delta_flat_rabitq" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --centroid-directory flat_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

run_bench "write_base_delta_two_level_rabitq" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-split-full-postings \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --centroid-directory two_level_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-block-probe-count "${FLAT_BLOCK_PROBE_COUNT}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

run_bench "write_same_leaf_packed_hbc_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_same_leaf_vectors_warm \
  --posting-storage packed_hbc \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_same_leaf_base_delta_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_same_leaf_vectors_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance

run_bench "write_same_leaf_base_delta_hbc_no_coalesce" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_same_leaf_vectors_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_random_packed_hbc_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_random_vectors_warm \
  --posting-storage packed_hbc \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_random_base_delta_hbc_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_random_vectors_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance

run_bench "write_random_base_delta_two_level_rabitq_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_random_vectors_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance \
  --centroid-directory two_level_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-block-probe-count "${FLAT_BLOCK_PROBE_COUNT}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

run_bench "write_semantic_packed_hbc_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_semantic_drift_vectors_warm \
  --posting-storage packed_hbc \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_semantic_base_delta_hbc_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_semantic_drift_vectors_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance

run_bench "write_semantic_base_delta_two_level_rabitq_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload overwrite_semantic_drift_vectors_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance \
  --centroid-directory two_level_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-block-probe-count "${FLAT_BLOCK_PROBE_COUNT}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

run_bench "write_append_packed_hbc_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload append_streaming_warm \
  --posting-storage packed_hbc \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_append_base_delta_hbc_lazy_fold" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload append_streaming_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance

run_bench "write_append_base_delta_two_level_rabitq_lazy_fold" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload append_streaming_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance \
  --centroid-directory two_level_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-block-probe-count "${FLAT_BLOCK_PROBE_COUNT}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

run_bench "write_mixed_packed_hbc_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload mixed_insert_delete_update_warm \
  --posting-storage packed_hbc \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_mixed_base_delta_hbc_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload mixed_insert_delete_update_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${MIXED_REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance

run_bench "write_mixed_base_delta_two_level_rabitq_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_WRITE_ARGS[@]}" \
  --workload mixed_insert_delete_update_warm \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-dirty-reassignments "${MIXED_REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance \
  --centroid-directory two_level_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-block-probe-count "${FLAT_BLOCK_PROBE_COUNT}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

run_bench "write_vdbb_packed_hbc_hbc" \
  zig build hbc-write-bench -- \
  "${COMMON_VDBB_WRITE_ARGS[@]}" \
  --posting-storage packed_hbc \
  --no-coalesce-overwrite-leaf-writes

run_bench "write_vdbb_base_delta_hbc_reassign_capacity" \
  zig build hbc-write-bench -- \
  "${COMMON_VDBB_WRITE_ARGS[@]}" \
  --posting-storage base_delta \
  --lazy-posting-maintenance \
  --repair-postings-after-write \
  --repair-postings-before-bulk-finish \
  --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
  --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
  --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
  --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
  --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
  --repair-rebalance-layout \
  --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
  --defer-leaf-splits-to-posting-maintenance \
  --bulk-ingest-finish-compact \
  --bulk-ingest-finish-max-deferred-l0-runs 1 \
  --bulk-ingest-finish-max-foreground-compaction-steps 1

if [[ "${ENABLE_VDBB_PROCEDURAL}" == "1" ]]; then
  run_bench "write_vdbb_procedural_packed_hbc_hbc" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage packed_hbc \
    --no-coalesce-overwrite-leaf-writes

  run_bench "write_vdbb_procedural_base_delta_hbc" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance

  run_bench "write_vdbb_procedural_base_delta_hbc_bounded_maintenance" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --repair-postings-before-bulk-finish \
    --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
    --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
    --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
    --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
    --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
    --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
    --repair-rebalance-layout \
    --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
    --defer-leaf-splits-to-posting-maintenance \
    --bulk-ingest-finish-compact \
    --bulk-ingest-finish-max-deferred-l0-runs 1 \
    --bulk-ingest-finish-max-foreground-compaction-steps 1

  run_bench "write_vdbb_procedural_base_delta_flat_rabitq" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --centroid-directory flat_rabitq \
    --flat-centroid-block-size "${VDBB_FLAT_BLOCK_SIZE}" \
    --flat-centroid-probe-count "${VDBB_FLAT_PROBE_COUNT}"

  run_bench "write_vdbb_procedural_base_delta_two_level_rabitq" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --centroid-directory two_level_rabitq \
    --flat-centroid-block-size "${VDBB_FLAT_BLOCK_SIZE}" \
    --flat-centroid-block-probe-count "${VDBB_FLAT_BLOCK_PROBE_COUNT}" \
    --flat-centroid-probe-count "${VDBB_FLAT_PROBE_COUNT}"

  run_bench "write_vdbb_procedural_base_delta_two_level_rabitq_bounded_maintenance" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --repair-postings-before-bulk-finish \
    --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
    --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
    --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
    --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
    --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
    --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
    --repair-rebalance-layout \
    --repair-split-full-postings \
    --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
    --defer-leaf-splits-to-posting-maintenance \
    --bulk-ingest-finish-compact \
    --bulk-ingest-finish-max-deferred-l0-runs 1 \
    --bulk-ingest-finish-max-foreground-compaction-steps 1 \
    --centroid-directory two_level_rabitq \
    --flat-centroid-block-size "${VDBB_FLAT_BLOCK_SIZE}" \
    --flat-centroid-block-probe-count "${VDBB_FLAT_BLOCK_PROBE_COUNT}" \
    --flat-centroid-probe-count "${VDBB_FLAT_PROBE_COUNT}"
fi

if [[ "${ENABLE_VDBB_PROCEDURAL_MUTATIONS}" == "1" ]]; then
  run_vdbb_procedural_mutation_rows \
    "write_vdbb_procedural" \
    "${VDBB_FLAT_BLOCK_SIZE}" \
    "${VDBB_FLAT_BLOCK_PROBE_COUNT}" \
    "${VDBB_FLAT_PROBE_COUNT}" \
    "${COMMON_VDBB_PROCEDURAL_WRITE_ARGS[@]}"
fi

if [[ "${ENABLE_VDBB_1M_PROCEDURAL}" == "1" ]]; then
  run_bench "write_vdbb_1m_procedural_packed_hbc_hbc" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage packed_hbc \
    --no-coalesce-overwrite-leaf-writes

  run_bench "write_vdbb_1m_procedural_base_delta_hbc" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance

  run_bench "write_vdbb_1m_procedural_base_delta_hbc_bounded_maintenance" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --repair-postings-before-bulk-finish \
    --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
    --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
    --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
    --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
    --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
    --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
    --repair-rebalance-layout \
    --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
    --defer-leaf-splits-to-posting-maintenance \
    --bulk-ingest-finish-compact \
    --bulk-ingest-finish-max-deferred-l0-runs 1 \
    --bulk-ingest-finish-max-foreground-compaction-steps 1

  run_bench "write_vdbb_1m_procedural_base_delta_flat_rabitq" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --centroid-directory flat_rabitq \
    --flat-centroid-block-size "${VDBB_1M_FLAT_BLOCK_SIZE}" \
    --flat-centroid-probe-count "${VDBB_1M_FLAT_PROBE_COUNT}"

  run_bench "write_vdbb_1m_procedural_base_delta_two_level_rabitq" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --centroid-directory two_level_rabitq \
    --flat-centroid-block-size "${VDBB_1M_FLAT_BLOCK_SIZE}" \
    --flat-centroid-block-probe-count "${VDBB_1M_FLAT_BLOCK_PROBE_COUNT}" \
    --flat-centroid-probe-count "${VDBB_1M_FLAT_PROBE_COUNT}"

  run_bench "write_vdbb_1m_procedural_base_delta_two_level_rabitq_bounded_maintenance" \
    zig build hbc-write-bench -- \
    "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}" \
    --posting-storage base_delta \
    --lazy-posting-maintenance \
    --repair-postings-before-bulk-finish \
    --repair-dirty-reassignments "${REPAIR_DIRTY_REASSIGNMENTS}" \
    --repair-dirty-reassignment-min-improvement "${REPAIR_DIRTY_REASSIGNMENT_MIN_IMPROVEMENT}" \
    --repair-dirty-reassignment-max-overfull-postings "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS}" \
    --repair-dirty-reassignment-max-over-capacity-members "${REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS}" \
    --repair-min-delta-records-to-fold "${REPAIR_MIN_DELTA_RECORDS_TO_FOLD}" \
    --repair-max-delta-tail-postings "${REPAIR_MAX_DELTA_TAIL_POSTINGS}" \
    --repair-rebalance-layout \
    --repair-split-full-postings \
    --repair-max-layout-changes "${AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES}" \
    --defer-leaf-splits-to-posting-maintenance \
    --bulk-ingest-finish-compact \
    --bulk-ingest-finish-max-deferred-l0-runs 1 \
    --bulk-ingest-finish-max-foreground-compaction-steps 1 \
    --centroid-directory two_level_rabitq \
    --flat-centroid-block-size "${VDBB_1M_FLAT_BLOCK_SIZE}" \
    --flat-centroid-block-probe-count "${VDBB_1M_FLAT_BLOCK_PROBE_COUNT}" \
    --flat-centroid-probe-count "${VDBB_1M_FLAT_PROBE_COUNT}"
fi

if [[ "${ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS}" == "1" ]]; then
  run_vdbb_procedural_mutation_rows \
    "write_vdbb_1m_procedural" \
    "${VDBB_1M_FLAT_BLOCK_SIZE}" \
    "${VDBB_1M_FLAT_BLOCK_PROBE_COUNT}" \
    "${VDBB_1M_FLAT_PROBE_COUNT}" \
    "${COMMON_VDBB_1M_PROCEDURAL_WRITE_ARGS[@]}"
fi

run_bench "read_packed_hbc_hbc" \
  zig build hbc-read-bench -- \
  "${COMMON_READ_ARGS[@]}" \
  --posting-storage packed_hbc

run_bench "read_base_delta_hbc_repaired" \
  zig build hbc-read-bench -- \
  "${COMMON_READ_ARGS[@]}" \
  --posting-storage base_delta \
  --repair-postings-after-build

run_bench "read_base_delta_flat_rabitq_repaired" \
  zig build hbc-read-bench -- \
  "${COMMON_READ_ARGS[@]}" \
  --posting-storage base_delta \
  --repair-postings-after-build \
  --centroid-directory flat_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

run_bench "read_base_delta_two_level_rabitq_repaired" \
  zig build hbc-read-bench -- \
  "${COMMON_READ_ARGS[@]}" \
  --posting-storage base_delta \
  --repair-postings-after-build \
  --centroid-directory two_level_rabitq \
  --flat-centroid-block-size "${FLAT_BLOCK_SIZE}" \
  --flat-centroid-block-probe-count "${FLAT_BLOCK_PROBE_COUNT}" \
  --flat-centroid-probe-count "${FLAT_PROBE_COUNT}"

printf 'spfresh_comparison_result=%s\n' "${RESULT_FILE}" >&2
