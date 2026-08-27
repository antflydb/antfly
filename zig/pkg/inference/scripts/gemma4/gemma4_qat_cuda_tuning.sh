#!/usr/bin/env bash

# Shared Gemma 4 QAT CUDA tuning profile. Source this file, then call
# gemma4_qat_cuda_tuning_env with the graph replay KV capacity. The resulting
# GEMMA4_QAT_CUDA_ENV array is suitable for `env "${...[@]}" command ...`.
gemma4_qat_cuda_tuning_env() {
  local capacity="${1:-${ANTFLY_CAPTURE_FORCE_KV_CAPACITY:-544}}"
  local decode_graph_replay="${antfly_decode_graph_replay:-${ANTFLY_DECODE_GRAPH_REPLAY:-required}}"
  local q4_rows="${antfly_q4_0_q8_1_prefill_rows:-${ANTFLY_Q4_0_Q8_1_PREFILL_ROWS:-1}}"
  local gate_up_q8="${ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE:-${antfly_q4_0_gate_up_activation_q8_1_precompute:-${ANTFLY_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE:-0}}}"
  # Prefer the typed profile consumed by the CUDA module. The independent
  # legacy booleans remain an input-only compatibility bridge; emitting a
  # single canonical value prevents ambiguous route combinations from reaching
  # production. Release qualification can explicitly preserve the runtime's
  # unset automatic default instead of having this tuning wrapper replace it.
  local gqa_profile="${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE:-${antfly_gqa_prefill_profile:-${ANTFLY_GQA_PREFILL_PROFILE:-}}}"
  local legacy_gqa_configured="${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_FAST+x}${antfly_gqa_prefill_fast+x}${ANTFLY_GQA_PREFILL_FAST+x}${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_TILED+x}${antfly_gqa_prefill_tiled+x}${ANTFLY_GQA_PREFILL_TILED+x}${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA+x}${antfly_gqa_prefill_mma+x}${ANTFLY_GQA_PREFILL_MMA+x}"
  local gqa_use_runtime_default="${ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT:-0}"
  case "$gqa_use_runtime_default" in
    1|true|yes|on) gqa_use_runtime_default=1 ;;
    0|false|no|off) gqa_use_runtime_default=0 ;;
    *)
      echo "ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT must be a boolean" >&2
      return 2
      ;;
  esac
  if [[ "$gqa_use_runtime_default" -eq 1 && ( -n "$gqa_profile" || -n "$legacy_gqa_configured" ) ]]; then
    echo "ANTFLY_GQA_PREFILL_USE_RUNTIME_DEFAULT conflicts with explicit GQA prefill configuration" >&2
    return 2
  fi
  if [[ "$gqa_use_runtime_default" -eq 0 && -z "$gqa_profile" ]]; then
    if [[ -z "$legacy_gqa_configured" ]]; then
      gqa_profile=required-fast
    else
      local gqa_fast="${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_FAST:-${antfly_gqa_prefill_fast:-${ANTFLY_GQA_PREFILL_FAST:-0}}}"
      local gqa_tiled="${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_TILED:-${antfly_gqa_prefill_tiled:-${ANTFLY_GQA_PREFILL_TILED:-0}}}"
      local gqa_mma="${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA:-${antfly_gqa_prefill_mma:-${ANTFLY_GQA_PREFILL_MMA:-0}}}"
      case "$gqa_fast:$gqa_tiled:$gqa_mma" in
        0:0:0) gqa_profile=off ;;
        1:0:0) gqa_profile=fast ;;
        0:1:0) gqa_profile=tiled ;;
        0:0:1) gqa_profile=mma ;;
        *)
          echo "ambiguous legacy GQA prefill booleans; use a canonical ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE" >&2
          return 2
          ;;
      esac
    fi
  fi
  if [[ "$gqa_use_runtime_default" -eq 0 ]]; then
    case "$gqa_profile" in
      off|fast|required-fast|tiled|mma|tiled-f16-exact|required-tiled-f16-exact|tiled-f16-warp|required-tiled-f16-warp|flash-f16-sm89|required-flash-f16-sm89) ;;
      *)
        echo "invalid ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=$gqa_profile" >&2
        return 2
        ;;
    esac
  fi
  # Decode split-K is shape-specific (Gemma 4 E2B GQA 8:1 on SM89), so the
  # shared tuning profile must never enable it implicitly for E4B/12B or other
  # attention topologies. Versioned E2B qualification profiles opt in.
  local gqa_decode_profile="${ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE:-off}"
  case "$gqa_decode_profile" in
    off|splitk-online-sm89|required-splitk-online-sm89) ;;
    *)
      echo "invalid ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE=$gqa_decode_profile" >&2
      return 2
      ;;
  esac
  # PLE mirror-first is a bounded SM89/E2B prefill candidate. Keep the shared
  # profile default off; a qualification run must opt in with the canonical
  # typed value, while decode remains on the existing fused Q4 route.
  local ple_gate_prefill_profile="${ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE:-off}"
  case "$ple_gate_prefill_profile" in
    off|mirror-first-sm89-e2b) ;;
    *)
      echo "invalid ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE=$ple_gate_prefill_profile" >&2
      return 2
      ;;
  esac
  local tile4_w8_min="${antfly_q4_0_linear_q8_1_tile4_w8_min_in_dim:-${ANTFLY_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM:-2048}}"
  local rows8_c4="${antfly_q4_0_linear_q8_1_rows8_c4:-${ANTFLY_Q4_0_LINEAR_Q8_1_ROWS8_C4:-1}}"
  local pair_rows8_c2="${antfly_q4_0_pair_activation_q8_1_rows8_c2:-${ANTFLY_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2:-1}}"
  local pair_rows16_c1="${antfly_q4_0_pair_activation_q8_1_rows16_c1:-${ANTFLY_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1:-1}}"
  local gemma_prewarm="${antfly_cuda_gemma_prefill_prewarm:-${ANTFLY_CUDA_GEMMA_PREFILL_PREWARM:-1}}"
  local prefill_first="${antfly_cuda_prefill_first_token:-${ANTFLY_CUDA_PREFILL_FIRST_TOKEN:-1}}"
  local prefill_coalesce="${antfly_cuda_prefill_first_token_coalesce_tokens:-${ANTFLY_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS:-2048}}"
  local prefill_profile="${ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS:-${antfly_cuda_profile_prefill_ops:-${ANTFLY_CUDA_PROFILE_PREFILL_OPS:-0}}}"
  local decode_profile="${ANTFLY_INFERENCE_CUDA_PROFILE_DECODE:-${antfly_cuda_profile_decode:-${ANTFLY_CUDA_PROFILE_DECODE:-0}}}"
  local rms_bf16="${antfly_rms_norm_bf16_mirror:-${ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR:-0}}"
  local resident_bf16="${ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16:-${antfly_bf16_resident_weights:-0}}"
  local hybrid_bf16="${antfly_hybrid_bf16_prefill:-${ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL:-0}}"
  local ple_bf16="${antfly_ple_model_proj_bf16:-${ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16:-$resident_bf16}}"
  local resident_bf16_enabled=0
  local hybrid_bf16_enabled=0
  case "$resident_bf16" in
    1|true|on|yes) resident_bf16_enabled=1 ;;
    0|false|off|no) ;;
    *)
      echo "invalid direct BF16 residency value: $resident_bf16" >&2
      return 2
      ;;
  esac
  case "$hybrid_bf16" in
    1|true|on|yes) hybrid_bf16_enabled=1 ;;
    0|false|off|no) ;;
    *)
      echo "invalid hybrid BF16 prefill value: $hybrid_bf16" >&2
      return 2
      ;;
  esac
  if ((resident_bf16_enabled && hybrid_bf16_enabled)); then
    echo "direct BF16 residency and hybrid Q4+BF16 prefill mirrors are mutually exclusive" >&2
    return 2
  fi
  # Generated decode attention is a dev candidate: its split-KV reduction has
  # long-output parity drift, so production stays on the handwritten route
  # unless an explicit candidate run opts in.
  local generated_attention="${antfly_generated_attention_decode:-${ANTFLY_GENERATED_ATTENTION_DECODE:-${ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE:-0}}}"
  # Omission means the runtime's evidence-bound automatic mode (qualified
  # Gemma 4 F16 geometry, sm_89, and long-context crossover). Explicit 0/1 is
  # reserved for control and candidate qualification runs.
  local generated_attention_score_prework="${antfly_generated_attention_score_prework:-${ANTFLY_GENERATED_ATTENTION_SCORE_PREWORK:-${ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK:-auto}}}"
  local turboquant_split_attention="${antfly_turboquant_split_attention:-${ANTFLY_TURBOQUANT_SPLIT_ATTENTION:-${ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION:-0}}}"
  local turboquant_split_attention_chunk="${antfly_turboquant_split_attention_chunk:-${ANTFLY_TURBOQUANT_SPLIT_ATTENTION_CHUNK:-${ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION_CHUNK:-12}}}"
  local q4_0_q8_1_lm_head_argmax="${antfly_q4_0_q8_1_lm_head_argmax:-${ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX:-1}}"
  local generated_q6_k_q8_1_lm_head_argmax="${antfly_generated_q6_k_q8_1_lm_head_argmax:-${ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX:-0}}"
  local generated_e2b_ffn="${ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN:-${antfly_generated_q4_0_e2b_ffn:-0}}"
  local generated_e2b_ffn_catalog="${ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_CATALOG_FFN_CANDIDATES:-$generated_e2b_ffn}"
  local generated_e2b_ffn_exact="${ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT:-${antfly_generated_q4_0_e2b_ffn_exact:-0}}"
  local generated_e2b_ffn_pair_only="${ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY:-${antfly_generated_q4_0_e2b_ffn_pair_only:-0}}"
  local generated_e2b_ffn_catalog_enabled=0
  local generated_e2b_ffn_exact_enabled=0
  local generated_e2b_ffn_pair_only_enabled=0
  case "$generated_e2b_ffn_catalog" in
    1|true|on|yes) generated_e2b_ffn_catalog_enabled=1 ;;
    0|false|off|no) ;;
    *)
      echo "invalid generated E2B FFN catalog candidate value: $generated_e2b_ffn_catalog" >&2
      return 2
      ;;
  esac
  case "$generated_e2b_ffn_exact" in
    1|true|on|yes) generated_e2b_ffn_exact_enabled=1 ;;
    0|false|off|no) ;;
    *)
      echo "invalid generated exact E2B FFN candidate value: $generated_e2b_ffn_exact" >&2
      return 2
      ;;
  esac
  case "$generated_e2b_ffn_pair_only" in
    1|true|on|yes) generated_e2b_ffn_pair_only_enabled=1 ;;
    0|false|off|no) ;;
    *)
      echo "invalid generated E2B FFN pair-only candidate value: $generated_e2b_ffn_pair_only" >&2
      return 2
      ;;
  esac
  if ((generated_e2b_ffn_catalog_enabled + generated_e2b_ffn_exact_enabled + generated_e2b_ffn_pair_only_enabled > 1)); then
    echo "generated E2B FFN catalog, exact-F32, and pair-only candidates are mutually exclusive" >&2
    return 2
  fi
  local linear_q8_1_dp4a="${ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A:-1}"
  local pair_q8_1_dp4a="${ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A:-1}"
  local pair_activation_q8_1_dp4a="${ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A:-1}"
  local gated_down_q8_1_dp4a="${ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A:-1}"
  local async_i32_download_staging="${ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING:-1}"
  local greedy_pending_token_readback="${ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK:-1}"
  # Decode graph capture learns and validates the temporary-buffer ABI at
  # runtime. Fixed period/skip values remain supported as an explicit
  # diagnostic override, but are no longer production defaults.
  local temp_arena_autoplan="${ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN:-${antfly_cuda_temp_arena_autoplan:-${ANTFLY_CUDA_TEMP_ARENA_AUTOPLAN:-1}}}"
  local temp_slot_period="${ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD:-${antfly_cuda_temp_slot_period:-${ANTFLY_CUDA_TEMP_SLOT_PERIOD:-0}}}"
  local temp_slot_skip="${ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP:-${antfly_cuda_temp_slot_skip:-${ANTFLY_CUDA_TEMP_SLOT_SKIP:-0}}}"

  GEMMA4_QAT_CUDA_ENV=(
    ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING="$async_i32_download_staging"
    ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION="$turboquant_split_attention"
    ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION_CHUNK="$turboquant_split_attention_chunk"
    ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_PRECOMPUTE=1
    ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE="$gate_up_q8"
    ANTFLY_INFERENCE_CUDA_Q4_0_Q8_1_PREFILL_ROWS="$q4_rows"
    ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE="$gqa_decode_profile"
    ANTFLY_INFERENCE_CUDA_PLE_GATE_PREFILL_PROFILE="$ple_gate_prefill_profile"
    ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8=1
    ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM="$tile4_w8_min"
    ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_ROWS8_C4="$rows8_c4"
    ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2="$pair_rows8_c2"
    ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1="$pair_rows16_c1"
    ANTFLY_INFERENCE_CUDA_GEMMA_PREFILL_PREWARM="$gemma_prewarm"
    ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN="$prefill_first"
    ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS="$prefill_coalesce"
    ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS="$prefill_profile"
    ANTFLY_INFERENCE_CUDA_PROFILE_DECODE="$decode_profile"
    ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR="$rms_bf16"
    ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16="$resident_bf16"
    ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL="$hybrid_bf16"
    ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16="$ple_bf16"
    ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W10_E4B_DOWN=1
    ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_TILE4_W8=1
    ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK="$greedy_pending_token_readback"
    ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A="$linear_q8_1_dp4a"
    ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_DP4A=1
    ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8=1
    ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A="$pair_q8_1_dp4a"
    ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A="$pair_activation_q8_1_dp4a"
    ANTFLY_INFERENCE_CUDA_Q4_0_ACTIVATION_SLICE_Q8_1_DP4A=1
    ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A="$gated_down_q8_1_dp4a"
    ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1=1
    ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1_TILE8_EXACT_THREADS=1
    ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY="$decode_graph_replay"
    ANTFLY_INFERENCE_CUDA_CAPTURE_FINAL_HIDDEN=1
    ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC=1
    ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS=1
    ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY=1
    ANTFLY_INFERENCE_CUDA_CAPTURE_GREEDY_TOKEN=1
    ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET=1
    ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN="$temp_arena_autoplan"
    ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD="$temp_slot_period"
    ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP="$temp_slot_skip"
    ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY="$capacity"
    ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE="$generated_attention"
    ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX="$q4_0_q8_1_lm_head_argmax"
    ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX="$generated_q6_k_q8_1_lm_head_argmax"
    ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN="$generated_e2b_ffn"
    ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT="$generated_e2b_ffn_exact"
    ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_PAIR_ONLY="$generated_e2b_ffn_pair_only"
  )

  if [[ "$gqa_use_runtime_default" -eq 0 ]]; then
    GEMMA4_QAT_CUDA_ENV+=(ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE="$gqa_profile")
  fi

  case "$generated_attention_score_prework" in
    auto) ;;
    1|true|on|yes)
      GEMMA4_QAT_CUDA_ENV+=(ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK=1)
      ;;
    0|false|off|no)
      GEMMA4_QAT_CUDA_ENV+=(ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK=0)
      ;;
    *)
      echo "invalid generated attention score-prework mode: $generated_attention_score_prework (expected auto, 0, or 1)" >&2
      return 2
      ;;
  esac

  case "$decode_graph_replay" in
    off|0|false)
      GEMMA4_QAT_CUDA_ENV+=(
        ANTFLY_INFERENCE_CUDA_CAPTURE_FINAL_HIDDEN=0
        ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC=0
        ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS=0
        ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY=0
        ANTFLY_INFERENCE_CUDA_CAPTURE_GREEDY_TOKEN=0
        ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY=0
        ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET=0
        ANTFLY_INFERENCE_CUDA_TEMP_ARENA_AUTOPLAN=0
        ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD=0
        ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP=0
      )
      ;;
  esac
}
