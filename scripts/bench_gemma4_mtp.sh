#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/bench_gemma4_mtp.sh <target-model> <assistant-q8> [assistant-q4]

Environment:
  BIN=zig/pkg/inference/zig-out/bin/antfly-inference
  OUT_DIR=/tmp/gemma4-mtp-bench-<timestamp>
  MODE=production|no_fallback|measurement|profile|profile_sync
  SPEC_KS="1 2 4"
  ASSISTANTS_EXTRA="label|path label2|path2"
  PROMPT_FILTER="ants_chat code_chat"
  MAX_TOKENS=32
  RUN_TIMEOUT=360s
  RUN_TARGET_ONLY=1
  TEMP_CACHE_MB=512
  COMBINED_BUDGET_MB=22000
  BACKEND_BUDGET_MB=19000
  KV_BUDGET_MB=512
  SCRATCH_BUDGET_MB=1024
  CACHE_DTYPE=
  TURBOQUANT_MIN_TOKENS=
  PREFILL_CHUNK_SIZE=
  SPECULATION_POLICY=auto|force|off
  SPECULATION_CALIBRATION=none|probe|positive
  MTP_TARGET_REPLAY=auto|force|off
  MTP_REPLAY_CONTEXT_KEY=auto|0|1
  MTP_UNSAFE_TARGET_REPLAY=auto|0|1
  MTP_REPLAY_VERIFY_ROWS=1,2,3,4,5
  MTP_ASSISTANT_REPLAY=auto|0|1
  MTP_MATERIALIZE_REPLAY=auto|0|1
  MTP_DEDICATED_RUNTIME=auto|0|1
  MTP_VERIFY_DEVICE_RESULT=auto|0|1
  MTP_MASKED_SELECT_HIDDEN_FUSION=auto|0|1
  MTP_POSITION_MODE=target_constant|target_absolute|legacy_one
  MTP_TARGET_HIDDEN_SOURCE=final|pre_norm
  MTP_CONCAT_ORDER=embedding_activation|activation_embedding
  MTP_KV_DONOR_MODE=shared_type|tail_base|assistant_index|non_shared_tail_base|first_shared_base
  DRAFT_EMBED_CACHE=256
  JSON_TOKEN_IDS=0|1
  CUDA_TEMP_SLOT_PERIOD=
  CUDA_TEMP_SLOT_SKIP=
  CUDA_CAPTURE_PERSISTENT_REPLAY=auto|0|1
  ADAPTIVE_K=auto|0|1
  HIDDEN_ONLY_MATERIALIZE=auto|0|1
  ACCEPT_BONUS=auto|0|1
USAGE
}

if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage
  exit 2
fi

TARGET_MODEL="$1"
ASSISTANT_Q8="$2"
ASSISTANT_Q4="${3:-}"
BIN="${BIN:-zig/pkg/inference/zig-out/bin/antfly-inference}"
OUT_DIR="${OUT_DIR:-/tmp/gemma4-mtp-bench-$(date +%Y%m%d-%H%M%S)}"
MODE="${MODE:-measurement}"
SPEC_KS="${SPEC_KS:-1 2 4}"
ASSISTANTS_EXTRA="${ASSISTANTS_EXTRA:-}"
PROMPT_FILTER="${PROMPT_FILTER:-}"
MAX_TOKENS="${MAX_TOKENS:-32}"
RUN_TIMEOUT="${RUN_TIMEOUT:-360s}"
RUN_TARGET_ONLY="${RUN_TARGET_ONLY:-1}"
TEMP_CACHE_MB="${TEMP_CACHE_MB:-512}"
COMBINED_BUDGET_MB="${COMBINED_BUDGET_MB:-22000}"
BACKEND_BUDGET_MB="${BACKEND_BUDGET_MB:-19000}"
KV_BUDGET_MB="${KV_BUDGET_MB:-512}"
SCRATCH_BUDGET_MB="${SCRATCH_BUDGET_MB:-1024}"
CACHE_DTYPE="${CACHE_DTYPE:-}"
TURBOQUANT_MIN_TOKENS="${TURBOQUANT_MIN_TOKENS:-}"
PREFILL_CHUNK_SIZE="${PREFILL_CHUNK_SIZE:-}"
SPECULATION_POLICY="${SPECULATION_POLICY:-}"
SPECULATION_CALIBRATION="${SPECULATION_CALIBRATION:-}"
MTP_TARGET_REPLAY="${MTP_TARGET_REPLAY:-}"
MTP_REPLAY_CONTEXT_KEY="${MTP_REPLAY_CONTEXT_KEY:-auto}"
MTP_UNSAFE_TARGET_REPLAY="${MTP_UNSAFE_TARGET_REPLAY:-auto}"
MTP_REPLAY_VERIFY_ROWS="${MTP_REPLAY_VERIFY_ROWS:-}"
MTP_ASSISTANT_REPLAY="${MTP_ASSISTANT_REPLAY:-auto}"
MTP_MATERIALIZE_REPLAY="${MTP_MATERIALIZE_REPLAY:-auto}"
MTP_DEDICATED_RUNTIME="${MTP_DEDICATED_RUNTIME:-auto}"
MTP_VERIFY_DEVICE_RESULT="${MTP_VERIFY_DEVICE_RESULT:-auto}"
MTP_MASKED_SELECT_HIDDEN_FUSION="${MTP_MASKED_SELECT_HIDDEN_FUSION:-auto}"
MTP_POSITION_MODE="${MTP_POSITION_MODE:-}"
MTP_TARGET_HIDDEN_SOURCE="${MTP_TARGET_HIDDEN_SOURCE:-}"
MTP_CONCAT_ORDER="${MTP_CONCAT_ORDER:-}"
MTP_KV_DONOR_MODE="${MTP_KV_DONOR_MODE:-}"
DRAFT_EMBED_CACHE="${DRAFT_EMBED_CACHE:-}"
JSON_TOKEN_IDS="${JSON_TOKEN_IDS:-0}"
CUDA_TEMP_SLOT_PERIOD="${CUDA_TEMP_SLOT_PERIOD:-}"
CUDA_TEMP_SLOT_SKIP="${CUDA_TEMP_SLOT_SKIP:-}"
CUDA_CAPTURE_PERSISTENT_REPLAY="${CUDA_CAPTURE_PERSISTENT_REPLAY:-auto}"
ADAPTIVE_K="${ADAPTIVE_K:-auto}"
HIDDEN_ONLY_MATERIALIZE="${HIDDEN_ONLY_MATERIALIZE:-auto}"
ACCEPT_BONUS="${ACCEPT_BONUS:-auto}"

case "$MODE" in
  production|no_fallback|measurement|profile|profile_sync) ;;
  *)
    echo "invalid MODE=$MODE; expected production, no_fallback, measurement, profile, or profile_sync" >&2
    exit 2
    ;;
esac

mkdir -p "$OUT_DIR"

if [[ -z "$SPECULATION_POLICY" ]]; then
  if [[ "$MODE" == "production" ]]; then
    SPECULATION_POLICY="auto"
  else
    SPECULATION_POLICY="force"
  fi
fi
if [[ -z "$SPECULATION_CALIBRATION" ]]; then
  if [[ "$MODE" == "production" ]]; then
    SPECULATION_CALIBRATION="probe"
  else
    SPECULATION_CALIBRATION="positive"
  fi
fi

PROMPTS=(
  "ants_chat|chat|Write one sentence about ants."
  "factual_chat|chat|Name the capital of France and one nearby river."
  "explain_chat|chat|Explain CUDA graph replay in one short sentence."
  "list_chat|chat|Give three concise tips for reducing GPU inference latency."
  "completion_raw|raw|The best way to keep a CUDA decode loop fast is"
  "punct_raw|raw|Complete this with punctuation: CUDA kernels, KV cache, and MTP"
  "newline_chat|chat|Write exactly two short lines about GPUs."
  "code_chat|chat|Write a single Zig line that declares a mutable usize counter."
)

should_run_prompt() {
  local case_name="$1"
  if [[ -z "$PROMPT_FILTER" ]]; then
    return 0
  fi
  for wanted in $PROMPT_FILTER; do
    if [[ "$case_name" == "$wanted" ]]; then
      return 0
    fi
  done
  return 1
}

declare -a ASSISTANTS
ASSISTANTS+=("q8|$ASSISTANT_Q8")
if [[ -n "$ASSISTANT_Q4" ]]; then
  ASSISTANTS+=("q4|$ASSISTANT_Q4")
fi
if [[ -n "$ASSISTANTS_EXTRA" ]]; then
  # shellcheck disable=SC2206
  EXTRA_SPECS=($ASSISTANTS_EXTRA)
  for assistant_spec in "${EXTRA_SPECS[@]}"; do
    ASSISTANTS+=("$assistant_spec")
  done
fi

run_case() {
  local label="$1"
  local assistant="$2"
  local spec_k="$3"
  local case_name="$4"
  local prompt_mode="$5"
  local prompt="$6"
  local stem="${label}_k${spec_k}_${case_name}"
  local json_path="$OUT_DIR/${stem}.json"
  local log_path="$OUT_DIR/${stem}.log"
  local status_path="$OUT_DIR/${stem}.status"
  local -a env_args=()
  local -a mode_args=()
  local -a draft_args=()
  local -a prefill_args=()
  local -a cache_args=()

  env_args+=("ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB=$TEMP_CACHE_MB")
  if [[ "$JSON_TOKEN_IDS" == "1" ]]; then
    env_args+=("ANTFLY_INFERENCE_JSON_TOKEN_IDS=1")
  fi

  if [[ -n "$assistant" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_ALLOW_UNSHARED_TARGET=1")
    if [[ -n "$MTP_TARGET_REPLAY" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_TARGET_REPLAY=$MTP_TARGET_REPLAY")
    fi
    if [[ "$MTP_REPLAY_CONTEXT_KEY" != "auto" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_REPLAY_CONTEXT_KEY=$MTP_REPLAY_CONTEXT_KEY")
    fi
    if [[ "$MTP_UNSAFE_TARGET_REPLAY" != "auto" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_UNSAFE_TARGET_REPLAY=$MTP_UNSAFE_TARGET_REPLAY")
    fi
    if [[ -n "$MTP_REPLAY_VERIFY_ROWS" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_REPLAY_VERIFY_ROWS=$MTP_REPLAY_VERIFY_ROWS")
    fi
    if [[ "$MTP_ASSISTANT_REPLAY" != "auto" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_ASSISTANT_REPLAY=$MTP_ASSISTANT_REPLAY")
    fi
    if [[ "$MTP_MATERIALIZE_REPLAY" != "auto" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_MATERIALIZE_REPLAY=$MTP_MATERIALIZE_REPLAY")
    fi
    if [[ "$MTP_DEDICATED_RUNTIME" != "auto" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_DEDICATED_RUNTIME=$MTP_DEDICATED_RUNTIME")
    fi
    if [[ "$MTP_VERIFY_DEVICE_RESULT" != "auto" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_VERIFY_DEVICE_RESULT=$MTP_VERIFY_DEVICE_RESULT")
    fi
    if [[ "$MTP_MASKED_SELECT_HIDDEN_FUSION" != "auto" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_MASKED_SELECT_HIDDEN_FUSION=$MTP_MASKED_SELECT_HIDDEN_FUSION")
    fi
    if [[ -n "$MTP_POSITION_MODE" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_POSITION_MODE=$MTP_POSITION_MODE")
    fi
    if [[ -n "$MTP_TARGET_HIDDEN_SOURCE" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_TARGET_HIDDEN_SOURCE=$MTP_TARGET_HIDDEN_SOURCE")
    fi
    if [[ -n "$MTP_CONCAT_ORDER" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_CONCAT_ORDER=$MTP_CONCAT_ORDER")
    fi
    if [[ -n "$MTP_KV_DONOR_MODE" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_KV_DONOR_MODE=$MTP_KV_DONOR_MODE")
    fi
    if [[ -n "$CUDA_TEMP_SLOT_PERIOD" ]]; then
      env_args+=("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD=$CUDA_TEMP_SLOT_PERIOD")
    fi
    if [[ -n "$CUDA_TEMP_SLOT_SKIP" ]]; then
      env_args+=("ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP=$CUDA_TEMP_SLOT_SKIP")
    fi
    if [[ "$CUDA_CAPTURE_PERSISTENT_REPLAY" != "auto" ]]; then
      env_args+=("ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY=$CUDA_CAPTURE_PERSISTENT_REPLAY")
    fi
    if [[ -n "$DRAFT_EMBED_CACHE" ]]; then
      env_args+=("ANTFLY_GEMMA4_MTP_DRAFT_EMBED_CACHE=$DRAFT_EMBED_CACHE")
    fi
  fi
  if [[ "$ADAPTIVE_K" != "auto" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_ADAPTIVE_K=$ADAPTIVE_K")
  fi
  if [[ "$HIDDEN_ONLY_MATERIALIZE" != "auto" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_HIDDEN_ONLY_MATERIALIZE=$HIDDEN_ONLY_MATERIALIZE")
  fi
  if [[ "$ACCEPT_BONUS" != "auto" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=$ACCEPT_BONUS")
  fi

  if [[ "$MODE" == "measurement" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_TOPK=8")
    env_args+=("ANTFLY_GEMMA4_MTP_MIN_ACCEPTANCE_PERMILLE=0")
    env_args+=("ANTFLY_GEMMA4_MTP_ZERO_MATCH_FALLBACK_ROUNDS=0")
    env_args+=("ANTFLY_GEMMA4_MTP_ADAPTIVE_K=0")
  fi
  if [[ "$MODE" == "no_fallback" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_MIN_ACCEPTANCE_PERMILLE=0")
    env_args+=("ANTFLY_GEMMA4_MTP_ZERO_MATCH_FALLBACK_ROUNDS=0")
    env_args+=("ANTFLY_GEMMA4_MTP_ADAPTIVE_K=0")
  fi
  if [[ "$MODE" == "profile" || "$MODE" == "profile_sync" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_PROFILE=1")
    env_args+=("ANTFLY_INFERENCE_CUDA_LAZY_PROFILE=1")
  fi
  if [[ "$MODE" == "profile_sync" ]]; then
    env_args+=("ANTFLY_GEMMA4_MTP_PROFILE_SYNC=1")
    env_args+=("ANTFLY_INFERENCE_CUDA_PROFILE_DECODE=1")
  fi

  if [[ "$prompt_mode" == "raw" ]]; then
    mode_args+=("--raw-prompt" "--no-chat-template")
  fi

  if [[ -n "$assistant" ]]; then
    draft_args+=("--draft-model" "$assistant" "--speculative-k" "$spec_k" "--speculation-policy" "$SPECULATION_POLICY" "--speculation-calibration" "$SPECULATION_CALIBRATION")
  fi
  if [[ -n "$PREFILL_CHUNK_SIZE" ]]; then
    prefill_args+=("--prefill-chunk-size" "$PREFILL_CHUNK_SIZE")
  fi
  if [[ -n "$CACHE_DTYPE" ]]; then
    cache_args+=("--cache-dtype" "$CACHE_DTYPE")
    case "$CACHE_DTYPE" in
      polar4|turbo3)
        env_args+=("ANTFLY_INFERENCE_CUDA_TURBOQUANT_MIN_TOKENS=${TURBOQUANT_MIN_TOKENS:-0}")
        ;;
    esac
  fi

  echo "running label=$label k=$spec_k case=$case_name mode=$prompt_mode"
  if env "${env_args[@]}" timeout "$RUN_TIMEOUT" "$BIN" generate "$TARGET_MODEL" "$prompt" \
    --backend cuda \
    "${draft_args[@]}" \
    "${cache_args[@]}" \
    --max-tokens "$MAX_TOKENS" \
    --temperature 0 \
    --combined-budget-mb "$COMBINED_BUDGET_MB" \
    --backend-budget-mb "$BACKEND_BUDGET_MB" \
    --kv-budget-mb "$KV_BUDGET_MB" \
    --scratch-budget-mb "$SCRATCH_BUDGET_MB" \
    "${prefill_args[@]}" \
    --print-timing \
    --print-token-count \
    --json-timing "$json_path" \
    "${mode_args[@]}" >"$log_path" 2>&1; then
    echo "ok" >"$status_path"
  else
    local status=$?
    echo "exit_$status" >"$status_path"
    echo "case failed: label=$label k=$spec_k case=$case_name status=$status log=$log_path" >&2
  fi
}

if [[ "$RUN_TARGET_ONLY" == "1" ]]; then
  for prompt_spec in "${PROMPTS[@]}"; do
    IFS='|' read -r case_name prompt_mode prompt <<<"$prompt_spec"
    if ! should_run_prompt "$case_name"; then
      continue
    fi
    run_case "target" "" "0" "$case_name" "$prompt_mode" "$prompt"
  done
fi

for assistant_spec in "${ASSISTANTS[@]}"; do
  IFS='|' read -r label assistant <<<"$assistant_spec"
  for spec_k in $SPEC_KS; do
    for prompt_spec in "${PROMPTS[@]}"; do
      IFS='|' read -r case_name prompt_mode prompt <<<"$prompt_spec"
      if ! should_run_prompt "$case_name"; then
        continue
      fi
      run_case "$label" "$assistant" "$spec_k" "$case_name" "$prompt_mode" "$prompt"
    done
  done
done

if command -v python3 >/dev/null 2>&1; then
  python3 - "$OUT_DIR" "$SPECULATION_POLICY" "$SPECULATION_CALIBRATION" <<'PY'
import json
import pathlib
import re
import sys

out_dir = pathlib.Path(sys.argv[1])
configured_policy = sys.argv[2] if len(sys.argv) > 2 else ""
configured_calibration = sys.argv[3] if len(sys.argv) > 3 else ""
headers = [
    "assistant",
    "spec_k",
    "case",
    "status",
    "tokens",
    "finish_reason",
    "decode_tok_s",
    "policy",
    "calibration",
    "policy_decision",
    "graph_replay",
    "drafted",
    "matched",
    "accepted",
    "accept_permille",
    "fallbacks",
    "mismatches",
    "target_top2",
    "target_top4",
    "target_top8",
    "format_misses",
    "near_tie",
    "avg_margin",
    "profile_enabled",
    "profile_sync",
    "draft_steps",
    "resident_draft_steps",
    "target_verify_rows",
    "target_verify_argmax_calls",
    "target_verify_argmax_rows",
    "target_verify_argmax_batched_calls",
    "target_verify_argmax_syncs",
    "dedicated_runtime_hits",
    "dedicated_runtime_fallbacks",
    "device_verify_commit_hits",
    "device_verify_commit_fallbacks",
    "device_verify_commit_result_downloads",
    "target_choice_downloads",
    "commit_forwards_required",
    "commit_forwards_avoided",
    "accepted_hidden_reuse_rows",
    "activation_copies",
    "materializations",
    "hidden_only_materializations",
    "hidden_only_fallbacks",
    "correction_materializations",
    "bonus_materializations",
    "bonus_skips",
    "profile_draft_ms",
    "profile_draft_embedding_ms",
    "profile_draft_concat_ms",
    "profile_draft_preprojection_ms",
    "profile_draft_assistant_ms",
    "profile_draft_postprojection_ms",
    "profile_draft_argmax_ms",
    "profile_draft_lm_head_ms",
    "profile_draft_selection_ms",
    "profile_verify_ms",
    "profile_activation_copy_ms",
    "profile_materialization_ms",
    "profile_fallback_ms",
    "target_launches",
    "target_syncs",
    "target_launches_per_tok",
    "target_syncs_per_tok",
    "target_d2h",
    "target_d2d",
    "target_graph_begins",
    "target_graph_replays",
    "target_graph_persistent_replays",
    "target_graph_discards",
    "target_graph_capacity_skips",
    "target_graph_replay_us",
    "draft_launches",
    "draft_syncs",
    "draft_launches_per_tok",
    "draft_syncs_per_tok",
    "draft_h2d",
    "draft_d2h",
    "draft_d2d",
    "draft_graph_begins",
    "draft_graph_replays",
    "draft_graph_persistent_replays",
    "draft_graph_discards",
    "draft_graph_capacity_skips",
    "draft_graph_replay_us",
    "draft_cross_copies",
    "draft_cross_bytes",
    "draft_event_waits",
    "draft_sync_fallbacks",
    "draft_mtp_preproject_fused_hits",
    "draft_mtp_preproject_fused_f32_weight_hits",
    "draft_mtp_preproject_fused_bf16_weight_hits",
    "draft_mtp_preproject_fused_f16_weight_hits",
    "draft_mtp_preproject_fused_fallbacks",
    "draft_mtp_masked_select_fused_hits",
    "draft_mtp_masked_select_fused_f32_weight_hits",
    "draft_mtp_masked_select_fused_bf16_weight_hits",
    "draft_mtp_masked_select_fused_f16_weight_hits",
    "draft_mtp_masked_select_fused_fallbacks",
    "draft_mtp_masked_select_hidden_fused_hits",
    "draft_mtp_masked_select_hidden_fused_bf16_hits",
    "draft_mtp_masked_select_hidden_multiblock_hits",
    "draft_mtp_masked_select_hidden_fused_fallbacks",
    "draft_mtp_masked_argmax_hits",
    "draft_mtp_masked_argmax_fallbacks",
    "draft_kv_attempts",
    "draft_kv_successes",
    "draft_kv_reads",
    "draft_kv_writes",
    "draft_kv_fail_read",
    "draft_kv_fail_shape",
    "draft_decode_profile_events",
    "draft_embed_cache_hits",
    "draft_embed_cache_misses",
    "draft_embed_cache_inserts",
    "draft_embed_cache_evictions",
    "draft_embed_cache_disabled",
    "draft_embedding_cross_copies",
]
rows = []
draft_re = re.compile(
    r"draft_cuda_counts:.*h2d=(\d+) d2h=(\d+).*cross_backend_copies=(\d+) "
    r"cross_backend_bytes=(\d+) cross_backend_event_records=(\d+) "
    r"cross_backend_event_waits=(\d+) cross_backend_sync_fallbacks=(\d+).*"
    r"device_kv_attempts=(\d+) device_kv_successes=(\d+) device_kv_reads=(\d+) "
    r"device_kv_writes=(\d+) device_kv_fail_read=(\d+) device_kv_fail_shape=(\d+)"
)


def field(obj, name, default=""):
    if not isinstance(obj, dict):
        return default
    value = obj.get(name, default)
    return default if value is None else value


def ms_from_ns(value):
    try:
        return int(value) // 1_000_000
    except Exception:
        return ""


def blank_row(label, spec_k, case_name, status):
    return [label, spec_k, case_name, status] + [""] * (len(headers) - 4)

stems = {path.stem for path in out_dir.glob("*.json")} | {path.stem for path in out_dir.glob("*.status")}
stem_re = re.compile(r"^(.+)_k([0-9]+)_(.+)$")

for stem in sorted(stems):
    stem_match = stem_re.match(stem)
    if stem_match:
        label, spec_k, case_name = stem_match.groups()
    else:
        label = ""
        spec_k = ""
        case_name = stem
    status_path = out_dir / f"{stem}.status"
    status = status_path.read_text().strip() if status_path.exists() else "unknown"
    log_text = (out_dir / f"{stem}.log").read_text(errors="replace") if (out_dir / f"{stem}.log").exists() else ""
    json_path = out_dir / f"{stem}.json"
    if not json_path.exists():
        rows.append(blank_row(label, spec_k, case_name, status))
        continue
    try:
        data = json.loads(json_path.read_text())
    except Exception as exc:
        row = blank_row(label, spec_k, case_name, status)
        row[4] = f"json_error:{exc}"
        rows.append(row)
        continue
    spec = data.get("speculative") or {}
    quality = spec.get("mtp_quality") or {}
    profile = spec.get("mtp_profile") or {}
    cuda = data.get("cuda_generate") or data.get("cuda") or {}
    draft_cuda = data.get("draft_cuda_generate") or data.get("draft_cuda") or {}

    log_draft = {}
    match = draft_re.search(log_text)
    if match:
        (
            log_draft["h2d_bytes"],
            log_draft["d2h_bytes"],
            log_draft["cross_backend_copies"],
            log_draft["cross_backend_copy_bytes"],
            _event_records,
            log_draft["cross_backend_event_waits"],
            log_draft["cross_backend_sync_fallbacks"],
            log_draft["device_kv_attempts"],
            log_draft["device_kv_successes"],
            log_draft["device_kv_reads"],
            log_draft["device_kv_writes"],
            log_draft["device_kv_fail_read"],
            log_draft["device_kv_fail_shape"],
        ) = match.groups()

    def draft_field(name, default=""):
        return field(draft_cuda, name, log_draft.get(name, default))

    policy = spec.get("speculation_policy", "")
    calibration = spec.get("speculation_calibration", "")
    policy_decision = spec.get("speculation_policy_decision", "")
    graph_replay = spec.get("mtp_graph_replay", "")
    if not policy and configured_policy == "off" and label and label != "target":
        policy = "off"
        calibration = configured_calibration
        policy_decision = "disabled_off"

    rows.append([
        label,
        spec_k,
        case_name,
        status,
        data.get("tokens", ""),
        data.get("finish_reason", ""),
        f"{data.get('decode_tok_per_s', 0):.3f}",
        policy,
        calibration,
        policy_decision,
        graph_replay,
        spec.get("drafted", ""),
        spec.get("matched", ""),
        spec.get("accepted", ""),
        spec.get("mtp_acceptance_permille", ""),
        spec.get("adaptive_fallbacks", ""),
        quality.get("mismatches", ""),
        quality.get("target_in_assistant_top2", ""),
        quality.get("target_in_assistant_top4", ""),
        quality.get("target_in_assistant_top8", ""),
        quality.get("format_or_control_misses", ""),
        quality.get("near_tie_misses", ""),
        quality.get("avg_assistant_target_margin", ""),
        field(profile, "enabled"),
        field(profile, "sync_enabled"),
        field(profile, "draft_steps"),
        field(profile, "resident_draft_steps"),
        field(profile, "target_verify_rows"),
        field(profile, "target_verify_argmax_calls"),
        field(profile, "target_verify_argmax_rows"),
        field(profile, "target_verify_argmax_batched_calls"),
        field(profile, "target_verify_argmax_syncs"),
        field(profile, "dedicated_runtime_hits"),
        field(profile, "dedicated_runtime_fallbacks"),
        field(profile, "device_verify_commit_hits"),
        field(profile, "device_verify_commit_fallbacks"),
        field(profile, "device_verify_commit_result_downloads"),
        field(profile, "target_choice_downloads"),
        field(profile, "commit_forwards_required"),
        field(profile, "commit_forwards_avoided"),
        field(profile, "accepted_hidden_reuse_rows"),
        field(profile, "activation_copies"),
        field(profile, "materializations"),
        field(profile, "materialization_hidden_only_hits"),
        field(profile, "materialization_hidden_only_fallbacks"),
        field(profile, "correction_materializations"),
        field(profile, "bonus_materializations"),
        field(profile, "bonus_skips"),
        ms_from_ns(field(profile, "draft_token_ns", "")),
        ms_from_ns(field(profile, "draft_target_embedding_ns", "")),
        ms_from_ns(field(profile, "draft_concat_ns", "")),
        ms_from_ns(field(profile, "draft_preprojection_ns", "")),
        ms_from_ns(field(profile, "draft_assistant_ns", "")),
        ms_from_ns(field(profile, "draft_postprojection_ns", "")),
        ms_from_ns(field(profile, "draft_argmax_ns", "")),
        ms_from_ns(field(profile, "draft_lm_head_ns", "")),
        ms_from_ns(field(profile, "draft_selection_ns", "")),
        ms_from_ns(field(profile, "target_verify_ns", "")),
        ms_from_ns(field(profile, "activation_copy_ns", "")),
        ms_from_ns(field(profile, "materialization_ns", "")),
        ms_from_ns(field(profile, "fallback_ns", "")),
        field(cuda, "kernel_launches"),
        field(cuda, "stream_syncs"),
        field(cuda, "launches_per_token"),
        field(cuda, "syncs_per_token"),
        field(cuda, "d2h_bytes"),
        field(cuda, "d2d_bytes"),
        field(cuda, "graph_capture_begins"),
        field(cuda, "graph_capture_replays"),
        field(cuda, "graph_capture_persistent_replays"),
        field(cuda, "graph_capture_discards"),
        field(cuda, "graph_capture_capacity_skips"),
        field(cuda, "decode_profile_graph_replay_us"),
        draft_field("kernel_launches"),
        draft_field("stream_syncs"),
        draft_field("launches_per_token"),
        draft_field("syncs_per_token"),
        draft_field("h2d_bytes"),
        draft_field("d2h_bytes"),
        draft_field("d2d_bytes"),
        draft_field("graph_capture_begins"),
        draft_field("graph_capture_replays"),
        draft_field("graph_capture_persistent_replays"),
        draft_field("graph_capture_discards"),
        draft_field("graph_capture_capacity_skips"),
        draft_field("decode_profile_graph_replay_us"),
        draft_field("cross_backend_copies"),
        draft_field("cross_backend_copy_bytes"),
        draft_field("cross_backend_event_waits"),
        draft_field("cross_backend_sync_fallbacks"),
        draft_field("mtp_preproject_fused_hits"),
        draft_field("mtp_preproject_fused_f32_weight_hits"),
        draft_field("mtp_preproject_fused_bf16_weight_hits"),
        draft_field("mtp_preproject_fused_f16_weight_hits"),
        draft_field("mtp_preproject_fused_fallbacks"),
        draft_field("mtp_masked_select_fused_hits"),
        draft_field("mtp_masked_select_fused_f32_weight_hits"),
        draft_field("mtp_masked_select_fused_bf16_weight_hits"),
        draft_field("mtp_masked_select_fused_f16_weight_hits"),
        draft_field("mtp_masked_select_fused_fallbacks"),
        draft_field("mtp_masked_select_hidden_fused_hits"),
        draft_field("mtp_masked_select_hidden_fused_bf16_hits"),
        draft_field("mtp_masked_select_hidden_multiblock_hits"),
        draft_field("mtp_masked_select_hidden_fused_fallbacks"),
        draft_field("mtp_masked_argmax_hits"),
        draft_field("mtp_masked_argmax_fallbacks"),
        draft_field("device_kv_attempts"),
        draft_field("device_kv_successes"),
        draft_field("device_kv_reads"),
        draft_field("device_kv_writes"),
        draft_field("device_kv_fail_read"),
        draft_field("device_kv_fail_shape"),
        draft_field("decode_profile_events"),
        field(profile, "draft_embedding_cache_hits"),
        field(profile, "draft_embedding_cache_misses"),
        field(profile, "draft_embedding_cache_inserts"),
        field(profile, "draft_embedding_cache_evictions"),
        field(profile, "draft_embedding_cache_disabled"),
        field(profile, "draft_target_embedding_cross_copies"),
    ])

summary_path = out_dir / "summary.tsv"
with summary_path.open("w") as f:
    f.write("\t".join(headers) + "\n")
    for row in rows:
        f.write("\t".join(str(value) for value in row) + "\n")

print(f"summary={summary_path}")
PY
fi

echo "out_dir=$OUT_DIR"
