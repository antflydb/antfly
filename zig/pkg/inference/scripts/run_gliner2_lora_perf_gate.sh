#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run_gliner2_lora_perf_gate.sh [options] [-- extra compare args]

Runs the repeated GLiNER2 LoRA Python/Zig benchmark harness with stable
accelerator training defaults and optional pass/fail thresholds.

Options:
  --runs N                         Repeated comparison runs (default: 3)
  --seeds CSV                      Isolated per-run seeds (count must match --runs)
  --out-dir DIR                    Output directory (default: /private/tmp/termite-gliner2-lora-perf-gate)
  --model-dir DIR                  Zig model dir (default: /private/tmp/termite-models/gliner2)
  --python-model PATH_OR_ID        Python model path/id (default: model dir)
  --train-data FILE                Training JSONL (default: synthetic smoke diagnostic)
  --eval-data FILE                 Disjoint eval JSONL validated but not scored by --production-ready
  --python-bin FILE                Python executable (default: /private/tmp/gliner2-parity-venv/bin/python)
  --upstream-source DIR            Clean checkout of the pinned Python oracle commit
  --zig-backend metal|cuda         Zig accelerator under test (default: metal)
  --zig-cuda-artifacts NAME        portable, sm89, or fatbin (default: fatbin)
  --include-python                 Run upstream Python side as timing target
  --compare-steps N                Steps per comparison run (default: 1; production: 4)
  --production-batch32             Use the production batch-32/seq-128 LoRA profile
  --production-ready               Require the strict release-parity prerequisite; held-out quality is separate
  --allow-smoke-fallback           Allow checked-in smoke fixtures for non-production diagnostics
  --warm-research                  Batch-32 warm-step diagnostic: 1 warmup + 3 measured steps, non-strict
  --warm-production-ready          Strict warm-step production target: median <=1.0x Python, any-run <=1.10x
  --op-stats                       Enable Metal partition op-stats collection
  --op-runs                        Enable grouped-dot candidate shape summaries
  --loop-profile                   Enable executor loop-profile timing summaries
  --hazard-profile                 Enable planned-access hazard timing summaries
  --promote-gather-inputs          Enable Metal gather/reduce input promotion for low true-host-output probes (default for --production-batch32)
  --max-zig-median-ms N            Fail if Zig median trainer ms exceeds N
  --max-zig-python-step-ratio-median N
                                   Fail if Zig/Python median step ratio exceeds N
  --max-zig-python-step-ratio-any-run N
                                   Fail if any successful run's Zig/Python step ratio exceeds N
  --max-zig-python-warm-step-ratio-median N
                                   Fail if Zig/Python median warm-step ratio exceeds N
  --max-zig-python-warm-step-ratio-any-run N
                                   Fail if any successful run's Zig/Python warm-step ratio exceeds N
  --max-host-output-median N       Fail if median aggregate host outputs exceeds N
  --max-true-host-output-median N  Fail if median true host outputs exceeds N
  --max-parameter-materialization-median N
                                   Fail if median parameter materializations exceeds N
  --warn-parameter-materialization-median N
                                   Warn if median parameter materializations exceeds N
  --max-fallback-median N          Fail if median interpreter fallbacks exceeds N
  --max-command-dispatch-median N  Fail if median command dispatches exceeds N
  --max-dot-general-count-median N Fail if median dot_general command count exceeds N
  --max-dot-general-ms-median N    Fail if median dot_general total ms exceeds N
  --max-gather-host-output-median N
                                   Fail if median gather host outputs exceeds N
  --max-zig-metal-peak-live-bytes-median N
                                   Fail if median Zig Metal peak live bytes exceeds N
  --max-zig-metal-planned-barriers-median N
                                   Fail if median planned Metal barriers exceeds N
  --max-zig-metal-planned-scopes-median N
                                   Fail if median planned Metal scopes exceeds N
  --max-zig-low-rank-lora-backward-region-median N
                                   Fail if median low-rank LoRA-backward regions exceeds N
  --min-zig-residual-layernorm-region-median N
                                   Fail if median residual+LayerNorm regions is below N
  --max-zig-scaffold-region-median N
                                   Fail if median scaffold-only encoder regions exceeds N
  --max-zig-encoder-lora-fallback-median N
                                   Fail if median encoder LoRA region fallbacks exceeds N
  --min-zig-lora-backward-region-median N
                                   Fail if median LoRA-backward regions is below N
  --min-zig-low-rank-lora-backward-region-median N
                                   Fail if median low-rank LoRA-backward regions is below N
  --min-zig-rank-adapter-backward-region-median N
                                   Fail if median rank-adapter-backward regions is below N
  --min-zig-ffn-gelu-backward-region-median N
                                   Fail if median FFN GELU-backward regions is below N
  --min-zig-head-mlp-forward-region-median N
                                   Fail if median head-MLP forward regions is below N
  --min-zig-head-mlp-backward-region-median N
                                   Fail if median head-MLP backward regions is below N
  --warn-zig-median-ms N           Report a warning if Zig median trainer ms exceeds N
  --require-zig-beats-python       Fail unless Zig median step beats Python median step
  --require-loss-parity            Fail unless all runs report valid_loss_parity=true
  --help                           Show this help

Examples:
  scripts/run_gliner2_lora_perf_gate.sh --op-stats --max-host-output-median 388
  scripts/run_gliner2_lora_perf_gate.sh --include-python --require-zig-beats-python
EOF
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../../.." && pwd)"

resolve_path() {
  "${python_bin}" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).expanduser().resolve())' "$1"
}

# Venv launchers are commonly symlinks to the base interpreter. Make a
# relative launcher stable across the later cd without dereferencing that
# symlink: invoking the resolved base binary would discard the venv's prefix
# and site-packages, silently replacing the frozen Fastino oracle runtime.
resolve_executable_path() {
  "${python_bin}" -c 'import os, sys; print(os.path.abspath(os.path.expanduser(sys.argv[1])))' "$1"
}

runs=3
runs_explicit=0
perf_seeds=""
out_dir="/private/tmp/termite-gliner2-lora-perf-gate"
model_dir="/private/tmp/termite-models/gliner2"
model_dir_explicit=0
python_model=""
train_data="/private/tmp/termite-gliner2-smoke-diagnostic/train.jsonl"
train_data_explicit=0
eval_data=""
python_bin="/private/tmp/gliner2-parity-venv/bin/python"
upstream_source=""
zig_backend="metal"
zig_cuda_artifacts="fatbin"
include_python=0
compare_steps=1
compare_steps_explicit=0
production_batch32=0
production_ready=0
warm_research=0
warm_production_ready=0
allow_smoke_fallback=0
op_stats=0
op_runs=0
loop_profile=0
hazard_profile=0
promote_gather_inputs=0
max_zig_median_ms=""
max_zig_python_step_ratio_median=""
max_zig_python_step_ratio_any_run=""
max_zig_python_warm_step_ratio_median=""
max_zig_python_warm_step_ratio_any_run=""
max_host_output_median=""
max_true_host_output_median=""
max_parameter_materialization_median=""
warn_parameter_materialization_median=""
max_fallback_median=""
max_command_dispatch_median=""
max_dot_general_count_median=""
max_dot_general_ms_median=""
max_gather_host_output_median=""
max_zig_metal_peak_live_bytes_median=""
max_zig_metal_planned_barriers_median=""
max_zig_metal_planned_scopes_median=""
max_zig_low_rank_lora_backward_region_median=""
min_zig_residual_layernorm_region_median=""
max_zig_scaffold_region_median=""
max_zig_encoder_lora_fallback_median=""
min_zig_lora_backward_region_median=""
min_zig_low_rank_lora_backward_region_median=""
min_zig_rank_adapter_backward_region_median=""
min_zig_ffn_gelu_backward_region_median=""
min_zig_head_mlp_forward_region_median=""
min_zig_head_mlp_backward_region_median=""
warn_zig_median_ms=""
require_zig_beats_python=0
require_loss_parity=0
extra_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      runs="${2:?missing value for --runs}"
      runs_explicit=1
      shift 2
      ;;
    --seeds)
      perf_seeds="${2:?missing value for --seeds}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:?missing value for --out-dir}"
      shift 2
      ;;
    --model-dir)
      model_dir="${2:?missing value for --model-dir}"
      model_dir_explicit=1
      shift 2
      ;;
    --python-model)
      python_model="${2:?missing value for --python-model}"
      shift 2
      ;;
    --train-data)
      train_data="${2:?missing value for --train-data}"
      train_data_explicit=1
      shift 2
      ;;
    --eval-data)
      eval_data="${2:?missing value for --eval-data}"
      shift 2
      ;;
    --python-bin)
      python_bin="${2:?missing value for --python-bin}"
      shift 2
      ;;
    --upstream-source)
      upstream_source="${2:?missing value for --upstream-source}"
      shift 2
      ;;
    --zig-backend)
      zig_backend="${2:?missing value for --zig-backend}"
      if [[ "${zig_backend}" != "metal" && "${zig_backend}" != "cuda" ]]; then
        echo "--zig-backend must be metal or cuda" >&2
        exit 2
      fi
      shift 2
      ;;
    --zig-cuda-artifacts)
      zig_cuda_artifacts="${2:?missing value for --zig-cuda-artifacts}"
      if [[ "${zig_cuda_artifacts}" != "portable" && "${zig_cuda_artifacts}" != "sm89" && "${zig_cuda_artifacts}" != "fatbin" ]]; then
        echo "--zig-cuda-artifacts must be portable, sm89, or fatbin" >&2
        exit 2
      fi
      shift 2
      ;;
    --include-python)
      include_python=1
      shift
      ;;
    --compare-steps)
      compare_steps="${2:?missing value for --compare-steps}"
      compare_steps_explicit=1
      shift 2
      ;;
    --production-batch32)
      production_batch32=1
      shift
      ;;
    --production-ready)
      production_ready=1
      shift
      ;;
    --allow-smoke-fallback)
      allow_smoke_fallback=1
      shift
      ;;
    --warm-research)
      warm_research=1
      shift
      ;;
    --warm-production-ready)
      warm_production_ready=1
      shift
      ;;
    --op-stats)
      op_stats=1
      shift
      ;;
    --op-runs)
      op_runs=1
      shift
      ;;
    --loop-profile)
      loop_profile=1
      shift
      ;;
    --hazard-profile)
      hazard_profile=1
      shift
      ;;
    --promote-gather-inputs)
      promote_gather_inputs=1
      shift
      ;;
    --max-zig-median-ms)
      max_zig_median_ms="${2:?missing value for --max-zig-median-ms}"
      shift 2
      ;;
    --max-zig-python-step-ratio-median)
      max_zig_python_step_ratio_median="${2:?missing value for --max-zig-python-step-ratio-median}"
      shift 2
      ;;
    --max-zig-python-step-ratio-any-run)
      max_zig_python_step_ratio_any_run="${2:?missing value for --max-zig-python-step-ratio-any-run}"
      shift 2
      ;;
    --max-zig-python-warm-step-ratio-median)
      max_zig_python_warm_step_ratio_median="${2:?missing value for --max-zig-python-warm-step-ratio-median}"
      shift 2
      ;;
    --max-zig-python-warm-step-ratio-any-run)
      max_zig_python_warm_step_ratio_any_run="${2:?missing value for --max-zig-python-warm-step-ratio-any-run}"
      shift 2
      ;;
    --max-host-output-median)
      max_host_output_median="${2:?missing value for --max-host-output-median}"
      shift 2
      ;;
    --max-true-host-output-median)
      max_true_host_output_median="${2:?missing value for --max-true-host-output-median}"
      shift 2
      ;;
    --max-parameter-materialization-median)
      max_parameter_materialization_median="${2:?missing value for --max-parameter-materialization-median}"
      shift 2
      ;;
    --warn-parameter-materialization-median)
      warn_parameter_materialization_median="${2:?missing value for --warn-parameter-materialization-median}"
      shift 2
      ;;
    --max-fallback-median)
      max_fallback_median="${2:?missing value for --max-fallback-median}"
      shift 2
      ;;
    --max-command-dispatch-median)
      max_command_dispatch_median="${2:?missing value for --max-command-dispatch-median}"
      shift 2
      ;;
    --max-dot-general-count-median)
      max_dot_general_count_median="${2:?missing value for --max-dot-general-count-median}"
      shift 2
      ;;
    --max-dot-general-ms-median)
      max_dot_general_ms_median="${2:?missing value for --max-dot-general-ms-median}"
      shift 2
      ;;
    --max-gather-host-output-median)
      max_gather_host_output_median="${2:?missing value for --max-gather-host-output-median}"
      shift 2
      ;;
    --max-zig-metal-peak-live-bytes-median)
      max_zig_metal_peak_live_bytes_median="${2:?missing value for --max-zig-metal-peak-live-bytes-median}"
      shift 2
      ;;
    --max-zig-metal-planned-barriers-median)
      max_zig_metal_planned_barriers_median="${2:?missing value for --max-zig-metal-planned-barriers-median}"
      shift 2
      ;;
    --max-zig-metal-planned-scopes-median)
      max_zig_metal_planned_scopes_median="${2:?missing value for --max-zig-metal-planned-scopes-median}"
      shift 2
      ;;
    --max-zig-low-rank-lora-backward-region-median)
      max_zig_low_rank_lora_backward_region_median="${2:?missing value for --max-zig-low-rank-lora-backward-region-median}"
      shift 2
      ;;
    --min-zig-residual-layernorm-region-median)
      min_zig_residual_layernorm_region_median="${2:?missing value for --min-zig-residual-layernorm-region-median}"
      shift 2
      ;;
    --max-zig-scaffold-region-median)
      max_zig_scaffold_region_median="${2:?missing value for --max-zig-scaffold-region-median}"
      shift 2
      ;;
    --max-zig-encoder-lora-fallback-median)
      max_zig_encoder_lora_fallback_median="${2:?missing value for --max-zig-encoder-lora-fallback-median}"
      shift 2
      ;;
    --min-zig-lora-backward-region-median)
      min_zig_lora_backward_region_median="${2:?missing value for --min-zig-lora-backward-region-median}"
      shift 2
      ;;
    --min-zig-low-rank-lora-backward-region-median)
      min_zig_low_rank_lora_backward_region_median="${2:?missing value for --min-zig-low-rank-lora-backward-region-median}"
      shift 2
      ;;
    --min-zig-rank-adapter-backward-region-median)
      min_zig_rank_adapter_backward_region_median="${2:?missing value for --min-zig-rank-adapter-backward-region-median}"
      shift 2
      ;;
    --min-zig-ffn-gelu-backward-region-median)
      min_zig_ffn_gelu_backward_region_median="${2:?missing value for --min-zig-ffn-gelu-backward-region-median}"
      shift 2
      ;;
    --min-zig-head-mlp-forward-region-median)
      min_zig_head_mlp_forward_region_median="${2:?missing value for --min-zig-head-mlp-forward-region-median}"
      shift 2
      ;;
    --min-zig-head-mlp-backward-region-median)
      min_zig_head_mlp_backward_region_median="${2:?missing value for --min-zig-head-mlp-backward-region-median}"
      shift 2
      ;;
    --warn-zig-median-ms)
      warn_zig_median_ms="${2:?missing value for --warn-zig-median-ms}"
      shift 2
      ;;
    --require-zig-beats-python)
      require_zig_beats_python=1
      shift
      ;;
    --require-loss-parity)
      require_loss_parity=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    --)
      shift
      extra_args+=("$@")
      break
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if (( warm_research || warm_production_ready )); then
  production_batch32=1
  include_python=1
  # Profiling is intentionally opt-in for timing presets: its collection cost
  # is charged to Zig trainer time and has no matching Python-side overhead.
  if (( ! compare_steps_explicit )); then
    compare_steps=4
  fi
fi
if (( warm_production_ready )); then
  production_ready=1
fi
if (( require_loss_parity )); then
  include_python=1
fi
if (( production_ready )); then
  if ((${#extra_args[@]})); then
    echo "--production-ready rejects extra comparison arguments after --; release profile and tolerance overrides are not allowed" >&2
    exit 2
  fi
  production_batch32=1
  include_python=1
  require_loss_parity=1
  if (( ! warm_production_ready )); then
    op_stats=1
  fi
  if (( ! compare_steps_explicit )); then
    compare_steps=4
  elif [[ ! "${compare_steps}" =~ ^[0-9]+$ ]] || (( compare_steps < 2 )); then
    echo "--production-ready requires --compare-steps >= 2" >&2
    exit 2
  fi
  if [[ -z "${eval_data}" ]]; then
    echo "--production-ready requires disjoint --eval-data" >&2
    exit 2
  fi
  if (( ! train_data_explicit )); then
    echo "--production-ready requires explicit representative --train-data" >&2
    exit 2
  fi
  if (( ! runs_explicit )); then
    if (( warm_production_ready )); then
      runs=3
    else
      runs=5
    fi
  fi
elif (( warm_research && ! runs_explicit )); then
  runs=3
fi
if (( production_ready )) && [[ -z "${perf_seeds}" ]]; then
  if (( runs == 5 )); then
    perf_seeds="11,23,37,53,71"
  elif (( runs == 3 )); then
    perf_seeds="11,37,71"
  fi
fi
if (( include_python )) && [[ -z "${upstream_source}" ]]; then
  echo "--upstream-source is required whenever the Python oracle runs" >&2
  exit 2
fi

if (( production_batch32 )); then
  if [[ "${zig_backend}" == "metal" ]]; then
    promote_gather_inputs=1
  fi
  hf_gliner2_snapshot=""
  if [[ -n "${HF_HOME:-}" ]]; then
    hf_gliner2_snapshot="${HF_HOME}/hub/models--fastino--gliner2-base-v1/snapshots/f5b2ecedebe4381b088c1cf276f5bf72a52cac54"
  elif [[ -n "${HOME:-}" ]]; then
    hf_gliner2_snapshot="${HOME}/.cache/huggingface/hub/models--fastino--gliner2-base-v1/snapshots/f5b2ecedebe4381b088c1cf276f5bf72a52cac54"
  fi
  if (( ! model_dir_explicit )) && [[ -n "${hf_gliner2_snapshot}" && -d "${hf_gliner2_snapshot}" && ( ! -f "${model_dir}/model.safetensors" || ! -f "${model_dir}/config.json" || ! -f "${model_dir}/tokenizer.json" || ! -f "${model_dir}/encoder_config/config.json" ) ]]; then
    model_dir="${hf_gliner2_snapshot}"
  fi
  if (( ! train_data_explicit && allow_smoke_fallback && ! production_ready && ! require_loss_parity )) && [[ "${train_data}" == "/private/tmp/termite-gliner2-smoke-diagnostic/train.jsonl" ]]; then
    if [[ -f /private/tmp/gliner2_realistic.jsonl ]]; then
      train_data="/private/tmp/gliner2_realistic.jsonl"
    elif [[ -f /private/tmp/gliner2_ner_batch32_train.jsonl ]]; then
      train_data="/private/tmp/gliner2_ner_batch32_train.jsonl"
    fi
  fi
  if [[ "${zig_backend}" == "metal" ]] && (( ! production_ready )) && [[ -z "${max_zig_metal_peak_live_bytes_median}" ]]; then
    # Keep the diagnostic lane bounded, but do not impose one machine-specific
    # release ceiling. Production callers that have a deployment budget can
    # require it explicitly with --max-zig-metal-peak-live-bytes-median.
    max_zig_metal_peak_live_bytes_median=5583457485
  fi
  if [[ -z "${warn_zig_median_ms}" ]]; then
    warn_zig_median_ms=3000
  fi
  if [[ -z "${max_zig_median_ms}" ]]; then
    max_zig_median_ms=18000
  fi
  if [[ "${zig_backend}" == "metal" && -z "${max_true_host_output_median}" ]]; then
    if (( promote_gather_inputs )); then
      # The six host values are bounded i64 index conversions consumed by
      # device gathers, not trainable or activation round trips.
      max_true_host_output_median=6
    else
      max_true_host_output_median=23
    fi
  fi
  if [[ "${zig_backend}" == "metal" && -z "${warn_parameter_materialization_median}" ]]; then
    warn_parameter_materialization_median=331
  fi
  if [[ "${zig_backend}" == "metal" && -z "${max_fallback_median}" ]]; then
    max_fallback_median=0
  fi
  if [[ "${zig_backend}" == "metal" && -z "${max_command_dispatch_median}" ]]; then
    max_command_dispatch_median=6250
  fi
  if [[ "${zig_backend}" == "metal" ]] && (( op_stats )) && [[ -z "${max_dot_general_count_median}" ]]; then
    # The reviewed batch-32/seq-128 all-task graph emits exactly 1,102 timed
    # dot_general commands; retain a zero-slack regression ceiling.
    max_dot_general_count_median=1102
  fi
  if [[ "${zig_backend}" == "metal" ]] && (( op_stats && promote_gather_inputs )) && [[ -z "${max_gather_host_output_median}" ]]; then
    max_gather_host_output_median=0
  fi
  if [[ "${zig_backend}" == "metal" && -z "${max_zig_low_rank_lora_backward_region_median}" ]]; then
    max_zig_low_rank_lora_backward_region_median=0
  fi
  if [[ "${zig_backend}" == "metal" && -z "${min_zig_residual_layernorm_region_median}" ]]; then
    min_zig_residual_layernorm_region_median=12
  fi
  if [[ "${zig_backend}" == "metal" && -z "${max_zig_scaffold_region_median}" ]]; then
    max_zig_scaffold_region_median=0
  fi
  if [[ "${zig_backend}" == "metal" && -z "${max_zig_encoder_lora_fallback_median}" ]]; then
    max_zig_encoder_lora_fallback_median=0
  fi
  if [[ "${zig_backend}" == "metal" && -z "${min_zig_lora_backward_region_median}" ]]; then
    min_zig_lora_backward_region_median=100
  fi
  if [[ "${zig_backend}" == "metal" && -z "${min_zig_rank_adapter_backward_region_median}" ]]; then
    min_zig_rank_adapter_backward_region_median=100
  fi
  if (( include_python && ! warm_research && ! warm_production_ready )) && [[ -z "${max_zig_python_step_ratio_median}" ]]; then
    max_zig_python_step_ratio_median=1.5
  fi
  if (( production_ready && ! warm_production_ready )) && [[ -z "${max_zig_python_step_ratio_any_run}" ]]; then
    max_zig_python_step_ratio_any_run=1.65
  fi
  if (( warm_production_ready )) && [[ -z "${max_zig_python_warm_step_ratio_median}" ]]; then
    max_zig_python_warm_step_ratio_median=1.0
  fi
  if (( warm_production_ready )) && [[ -z "${max_zig_python_warm_step_ratio_any_run}" ]]; then
    max_zig_python_warm_step_ratio_any_run=1.10
  fi
  if [[ "${zig_backend}" == "metal" ]] && (( production_ready )) && [[ -z "${max_zig_metal_planned_barriers_median}" ]]; then
    # Default production graph (head fusion disabled) is stable at 81 planned
    # barriers; the experimental head lane retains its stricter 40 limit.
    max_zig_metal_planned_barriers_median=81
  fi
  if [[ "${zig_backend}" == "metal" ]] && (( production_ready )) && [[ -z "${max_zig_metal_planned_scopes_median}" ]]; then
    # The reviewed default graph reports 125.5 mean planned scopes. Keep a
    # half-scope rounding allowance while rejecting the next full-scope step.
    max_zig_metal_planned_scopes_median=126
  fi
  if [[ "${zig_backend}" == "metal" ]]; then
    export TERMITE_METAL_BUFFER_REUSE_STATS="${TERMITE_METAL_BUFFER_REUSE_STATS:-1}"
    export TERMITE_METAL_FRAME_CHUNK_OPS="${TERMITE_METAL_FRAME_CHUNK_OPS:-128}"
    export TERMITE_GRAPH_EXECUTOR_STATS="${TERMITE_GRAPH_EXECUTOR_STATS:-1}"
    export TERMITE_METAL_ENABLE_DEBERTA_ENCODER_LAYER_LORA_REGION=1
  fi
fi

# The benchmark runs from repo_root. Freeze every filesystem argument against
# the caller's cwd first so validation and execution cannot resolve different
# same-named paths.
model_dir="$(resolve_path "${model_dir}")"
train_data="$(resolve_path "${train_data}")"
out_dir="$(resolve_path "${out_dir}")"
if [[ -n "${eval_data}" ]]; then
  eval_data="$(resolve_path "${eval_data}")"
fi
if [[ -n "${python_model}" && -e "${python_model}" ]]; then
  python_model="$(resolve_path "${python_model}")"
fi
if [[ -n "${upstream_source}" ]]; then
  upstream_source="$(resolve_path "${upstream_source}")"
fi
if [[ "${python_bin}" == */* ]]; then
  python_bin="$(resolve_executable_path "${python_bin}")"
fi

required_model_files=(
  "model.safetensors"
  "config.json"
  "tokenizer.json"
  "tokenizer_config.json"
  "special_tokens_map.json"
  "added_tokens.json"
  "encoder_config/config.json"
)
missing_model_files=()
for required_model_file in "${required_model_files[@]}"; do
  if [[ ! -e "${model_dir}/${required_model_file}" ]]; then
    missing_model_files+=("${required_model_file}")
  fi
done
if ((${#missing_model_files[@]})); then
  echo "GLiNER2 perf gate model fixture is incomplete: ${model_dir}" >&2
  printf '  missing: %s\n' "${missing_model_files[@]}" >&2
  if (( model_dir_explicit )); then
    echo "Pass a complete --model-dir, for example a Hugging Face snapshot directory." >&2
  else
    echo "Default model fixture is incomplete and no local Hugging Face snapshot fallback was available." >&2
  fi
  exit 2
fi
if [[ ! -f "${train_data}" ]]; then
  echo "GLiNER2 perf gate training data is missing: ${train_data}" >&2
  if (( train_data_explicit )); then
    echo "Pass an existing --train-data JSONL file." >&2
  else
    echo "Default synthetic smoke diagnostic was missing; run prepare_gliner2_smoke_diagnostic.py or pass --train-data." >&2
  fi
  exit 2
fi
if (( production_ready )); then
  comparison_records=$((compare_steps * 32))
  "${python_bin}" "${script_dir}/validate_gliner2_release_data.py" \
    --train "${train_data}" --eval "${eval_data}" --require-full-task \
    --min-train-records "${comparison_records}" --min-eval-records 32 \
    --comparison-records "${comparison_records}"
elif (( require_loss_parity )); then
  "${python_bin}" "${script_dir}/validate_gliner2_release_data.py" --train "${train_data}"
fi

if [[ -z "${python_model}" ]]; then
  python_model="${model_dir}"
fi

echo "perf_gate_fixture: model_dir=${model_dir} train_data=${train_data}" >&2

bench_args=(
  "--runs" "${runs}"
  "--out-dir" "${out_dir}"
)
if [[ -n "${perf_seeds}" ]]; then
  bench_args+=("--seeds" "${perf_seeds}")
fi
if (( op_stats )); then
  bench_args+=("--op-stats")
fi
if (( op_runs )); then
  bench_args+=("--op-runs")
fi
if (( loop_profile )); then
  bench_args+=("--loop-profile")
fi
if (( hazard_profile )); then
  bench_args+=("--hazard-profile")
fi
if [[ -n "${max_zig_median_ms}" ]]; then
  bench_args+=("--max-zig-median-ms" "${max_zig_median_ms}")
fi
if [[ -n "${max_zig_python_step_ratio_median}" ]]; then
  bench_args+=("--max-zig-python-step-ratio-median" "${max_zig_python_step_ratio_median}")
fi
if [[ -n "${max_zig_python_step_ratio_any_run}" ]]; then
  bench_args+=("--max-zig-python-step-ratio-any-run" "${max_zig_python_step_ratio_any_run}")
fi
if [[ -n "${max_zig_python_warm_step_ratio_median}" ]]; then
  bench_args+=("--max-zig-python-warm-step-ratio-median" "${max_zig_python_warm_step_ratio_median}")
fi
if [[ -n "${max_zig_python_warm_step_ratio_any_run}" ]]; then
  bench_args+=("--max-zig-python-warm-step-ratio-any-run" "${max_zig_python_warm_step_ratio_any_run}")
fi
if [[ -n "${max_host_output_median}" ]]; then
  bench_args+=("--max-host-output-median" "${max_host_output_median}")
fi
if [[ -n "${max_true_host_output_median}" ]]; then
  bench_args+=("--max-true-host-output-median" "${max_true_host_output_median}")
fi
if [[ -n "${max_parameter_materialization_median}" ]]; then
  bench_args+=("--max-parameter-materialization-median" "${max_parameter_materialization_median}")
fi
if [[ -n "${warn_parameter_materialization_median}" ]]; then
  bench_args+=("--warn-parameter-materialization-median" "${warn_parameter_materialization_median}")
fi
if [[ -n "${max_fallback_median}" ]]; then
  bench_args+=("--max-fallback-median" "${max_fallback_median}")
fi
if [[ -n "${max_command_dispatch_median}" ]]; then
  bench_args+=("--max-command-dispatch-median" "${max_command_dispatch_median}")
fi
if [[ -n "${max_dot_general_count_median}" ]]; then
  bench_args+=("--max-dot-general-count-median" "${max_dot_general_count_median}")
fi
if [[ -n "${max_dot_general_ms_median}" ]]; then
  bench_args+=("--max-dot-general-ms-median" "${max_dot_general_ms_median}")
fi
if [[ -n "${max_gather_host_output_median}" ]]; then
  bench_args+=("--max-gather-host-output-median" "${max_gather_host_output_median}")
fi
if [[ -n "${max_zig_metal_peak_live_bytes_median}" ]]; then
  bench_args+=("--max-zig-metal-peak-live-bytes-median" "${max_zig_metal_peak_live_bytes_median}")
fi
if [[ -n "${max_zig_metal_planned_barriers_median}" ]]; then
  bench_args+=("--max-zig-metal-planned-barriers-median" "${max_zig_metal_planned_barriers_median}")
fi
if [[ -n "${max_zig_metal_planned_scopes_median}" ]]; then
  bench_args+=("--max-zig-metal-planned-scopes-median" "${max_zig_metal_planned_scopes_median}")
fi
if [[ -n "${max_zig_low_rank_lora_backward_region_median}" ]]; then
  bench_args+=("--max-zig-low-rank-lora-backward-region-median" "${max_zig_low_rank_lora_backward_region_median}")
fi
if [[ -n "${min_zig_residual_layernorm_region_median}" ]]; then
  bench_args+=("--min-zig-residual-layernorm-region-median" "${min_zig_residual_layernorm_region_median}")
fi
if [[ -n "${max_zig_scaffold_region_median}" ]]; then
  bench_args+=("--max-zig-scaffold-region-median" "${max_zig_scaffold_region_median}")
fi
if [[ -n "${max_zig_encoder_lora_fallback_median}" ]]; then
  bench_args+=("--max-zig-encoder-lora-fallback-median" "${max_zig_encoder_lora_fallback_median}")
fi
if [[ -n "${min_zig_lora_backward_region_median}" ]]; then
  bench_args+=("--min-zig-lora-backward-region-median" "${min_zig_lora_backward_region_median}")
fi
if [[ -n "${min_zig_low_rank_lora_backward_region_median}" ]]; then
  bench_args+=("--min-zig-low-rank-lora-backward-region-median" "${min_zig_low_rank_lora_backward_region_median}")
fi
if [[ -n "${min_zig_rank_adapter_backward_region_median}" ]]; then
  bench_args+=("--min-zig-rank-adapter-backward-region-median" "${min_zig_rank_adapter_backward_region_median}")
fi
if [[ -n "${min_zig_ffn_gelu_backward_region_median}" ]]; then
  bench_args+=("--min-zig-ffn-gelu-backward-region-median" "${min_zig_ffn_gelu_backward_region_median}")
fi
if [[ -n "${min_zig_head_mlp_forward_region_median}" ]]; then
  bench_args+=("--min-zig-head-mlp-forward-region-median" "${min_zig_head_mlp_forward_region_median}")
fi
if [[ -n "${min_zig_head_mlp_backward_region_median}" ]]; then
  bench_args+=("--min-zig-head-mlp-backward-region-median" "${min_zig_head_mlp_backward_region_median}")
fi
if [[ -n "${warn_zig_median_ms}" ]]; then
  bench_args+=("--warn-zig-median-ms" "${warn_zig_median_ms}")
fi
if (( require_zig_beats_python )); then
  bench_args+=("--require-zig-beats-python")
fi
if (( require_loss_parity )); then
  bench_args+=("--require-loss-parity")
fi
if (( production_ready )); then
  bench_args+=("--require-adapter-roundtrip")
fi

compare_args=(
  "--zig-backend" "${zig_backend}"
  "--model-dir" "${model_dir}"
  "--python-model" "${python_model}"
  "--python-bin" "${python_bin}"
  "--train-data" "${train_data}"
  "--entity-types" "person,organization,location"
  "--steps" "${compare_steps}"
  "--seq-len" "16"
  "--max-span-width" "2"
  "--lora-rank" "1"
  "--lora-alpha" "2"
  "--lora-dropout" "0"
  "--span-positive-weight" "1"
  "--span-negative-weight" "1"
  "--span-hard-negative-weight" "1"
  "--span-loss-reduction" "sum"
  "--timeout-seconds" "900"
)
if (( production_batch32 )); then
  compare_args=(
    "--zig-backend" "${zig_backend}"
    "--model-dir" "${model_dir}"
    "--python-model" "${python_model}"
    "--python-bin" "${python_bin}"
    "--train-data" "${train_data}"
    "--entity-types" "person,organization,location"
    "--steps" "${compare_steps}"
    "--batch-size" "32"
    "--seq-len" "128"
    "--max-span-width" "4"
    "--lora-rank" "16"
    "--lora-alpha" "32"
    "--lora-dropout" "0"
    "--span-positive-weight" "1"
    "--span-negative-weight" "1"
    "--span-hard-negative-weight" "1"
    "--span-negative-mask-rate" "0"
    "--span-loss-reduction" "sum"
    "--zig-objective" "gliner2-total-loss"
    "--structure-span-chunk-samples" "16"
    "--timeout-seconds" "900"
  )
fi
if [[ "${zig_backend}" == "cuda" ]]; then
  # A CUDA-vs-auto comparison can silently put the oracle on CPU when its
  # environment is incomplete. Also keep the standard performance lane
  # checkpoint-free on both sides; the L4 profile fits without recomputation.
  compare_args+=("--zig-build-cuda" "--zig-cuda-artifacts" "${zig_cuda_artifacts}" "--python-device" "cuda")
else
  compare_args+=("--zig-build-metal")
  if (( production_batch32 )); then
    compare_args+=(
      "--activation-checkpointing"
      "--activation-checkpoint-interval" "1"
      "--activation-checkpoint-strategy" "parameters-only"
    )
  fi
fi
if (( include_python )); then
  compare_args+=("--upstream-source" "${upstream_source}")
fi
if (( require_loss_parity )); then
  compare_args+=("--deterministic")
elif (( include_python )); then
  compare_args+=("--perf-target-only-python")
else
  compare_args+=("--skip-python")
fi
if ((${#extra_args[@]})); then
  compare_args+=("${extra_args[@]}")
fi
if (( production_ready )); then
  compare_args+=(
    "--zig-objective" "gliner2-total-loss"
    "--zig-lora-only-trainables"
    "--deterministic"
    "--strict"
    "--require-full-task-parity"
    "--dump-optimizer-parity"
    "--adapter-roundtrip"
  )
fi

cd "${repo_root}"
env_args=()
if (( promote_gather_inputs )); then
  env_args+=("TERMITE_METAL_GATHER_PROMOTE_INPUT=1")
  env_args+=("TERMITE_METAL_REDUCE_PROMOTE_INPUT=1")
fi
if ((${#env_args[@]})); then
  exec env "${env_args[@]}" "${python_bin}" "${script_dir}/benchmark_gliner2_lora_perf.py" "${bench_args[@]}" -- "${compare_args[@]}"
else
  exec "${python_bin}" "${script_dir}/benchmark_gliner2_lora_perf.py" "${bench_args[@]}" -- "${compare_args[@]}"
fi
