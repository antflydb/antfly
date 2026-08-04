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

usage() {
  cat <<'USAGE'
usage: run_cuda_gemma4_l4_e2e.sh [--nightly|--release]

Runs the opt-in Gemma 4 CUDA qualification suite on an NVIDIA L4 (SM89).
This is a local/hardware-lab E2E lane, not a GitHub Actions workflow.

Required environment:
  E2B_MODEL                   Gemma 4 E2B QAT GGUF
  GEMMA12B_Q4_MODEL           Gemma 4 12B Q4_K_M GGUF
  LLAMA_CPP_BIN               pinned llama-completion binary

Optional environment:
  ANTFLY_BIN                  prebuilt antfly-inference binary
  CUDA_EVIDENCE_DIR           output directory
  CUDA_RELEASE_MODE           nightly or release (default: nightly)
  CUDA_VISIBLE_DEVICES        selected L4 (default: 0)
  ZIG_BIN                     Zig 0.16 executable (default: zig)
  MTP_DRAFT_MODEL             optional nightly MTP draft GGUF

Long-context headline lane (all three values must be set together):
  LLAMA_SERVER_BIN
  LONG_E2E_LOCK
  LONG_E2E_LOCK_SHA256

E4B regression lane:
  E4B_QAT_MODEL
  E4B_MODELS_DIR
  E4B_BASELINE_EVIDENCE
  E4B_BASELINE_SHA256
  E4B_REGRESSION_LOCK
  E4B_REGRESSION_LOCK_SHA256

Release mode requires both locked long-context lanes. Nightly mode permits
their omission and treats MTP as collection-only, non-gating diagnostics.
USAGE
}

mode="${CUDA_RELEASE_MODE:-nightly}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --nightly) mode="nightly" ;;
    --release) mode="release" ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
  shift
done
case "$mode" in
  nightly|release) ;;
  *)
    echo "unsupported CUDA_RELEASE_MODE=$mode; expected nightly or release" >&2
    exit 2
    ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
inference_dir="$repo_root/zig/pkg/inference"
zig_bin="${ZIG_BIN:-zig}"
antfly_bin="${ANTFLY_BIN:-$inference_dir/zig-out/bin/antfly-inference}"
evidence_dir="${CUDA_EVIDENCE_DIR:-/tmp/antfly-cuda-gemma4-l4-e2e-$(date -u +%Y%m%dT%H%M%SZ)}"
e2b_model="${E2B_MODEL:-$repo_root/.models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf}"
gemma12b_q4_model="${GEMMA12B_Q4_MODEL:-$repo_root/.models/google/gemma-4-12B-it-q4_k/gemma-4-12B-it-Q4_K_M.gguf}"
llama_cpp_bin="${LLAMA_CPP_BIN:-/tmp/llama.cpp/build/bin/llama-completion}"
mtp_draft_model="${MTP_DRAFT_MODEL:-$repo_root/.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q8_0-MTP.gguf}"
llama_server_bin="${LLAMA_SERVER_BIN:-}"
long_e2e_lock="${LONG_E2E_LOCK:-}"
long_e2e_lock_sha256="${LONG_E2E_LOCK_SHA256:-}"
e4b_qat_model="${E4B_QAT_MODEL:-}"
e4b_models_dir="${E4B_MODELS_DIR:-}"
e4b_baseline_evidence="${E4B_BASELINE_EVIDENCE:-}"
e4b_baseline_sha256="${E4B_BASELINE_SHA256:-}"
e4b_regression_lock="${E4B_REGRESSION_LOCK:-}"
e4b_regression_lock_sha256="${E4B_REGRESSION_LOCK_SHA256:-}"

mkdir -p "$evidence_dir"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0}"
export ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-$evidence_dir/zig-local-cache}"
export ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-$evidence_dir/zig-global-cache}"
mkdir -p "$ZIG_LOCAL_CACHE_DIR" "$ZIG_GLOBAL_CACHE_DIR"

require_file() {
  local name="$1"
  local path="$2"
  if [[ ! -f "$path" ]]; then
    echo "missing $name: $path" >&2
    exit 1
  fi
}

require_sha256() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9a-f]{64}$ ]]; then
    echo "$name must be a lowercase 64-character SHA-256 digest" >&2
    exit 1
  fi
}

long_e2e_values=("$llama_server_bin" "$long_e2e_lock" "$long_e2e_lock_sha256")
long_e2e_configured=0
for value in "${long_e2e_values[@]}"; do
  [[ -n "$value" ]] && long_e2e_configured=$((long_e2e_configured + 1))
done
if [[ "$long_e2e_configured" -ne 0 && "$long_e2e_configured" -ne 3 ]]; then
  echo "LLAMA_SERVER_BIN, LONG_E2E_LOCK, and LONG_E2E_LOCK_SHA256 must be configured together" >&2
  exit 1
fi
if [[ "$long_e2e_configured" -eq 3 ]]; then
  require_sha256 "LONG_E2E_LOCK_SHA256" "$long_e2e_lock_sha256"
elif [[ "$mode" == "release" ]]; then
  echo "release mode requires LLAMA_SERVER_BIN, LONG_E2E_LOCK, and LONG_E2E_LOCK_SHA256" >&2
  exit 1
fi

e4b_regression_values=(
  "$e4b_baseline_evidence"
  "$e4b_baseline_sha256"
  "$e4b_regression_lock"
  "$e4b_regression_lock_sha256"
)
e4b_regression_configured=0
for value in "${e4b_regression_values[@]}"; do
  [[ -n "$value" ]] && e4b_regression_configured=$((e4b_regression_configured + 1))
done
if [[ "$e4b_regression_configured" -ne 0 && "$e4b_regression_configured" -ne 4 ]]; then
  echo "E4B_BASELINE_EVIDENCE, E4B_BASELINE_SHA256, E4B_REGRESSION_LOCK, and E4B_REGRESSION_LOCK_SHA256 must be configured together" >&2
  exit 1
fi
if [[ "$e4b_regression_configured" -eq 4 ]]; then
  [[ -n "$e4b_qat_model" ]] || {
    echo "E4B_QAT_MODEL is required when E4B regression evidence is configured" >&2
    exit 1
  }
  require_sha256 "E4B_BASELINE_SHA256" "$e4b_baseline_sha256"
  require_sha256 "E4B_REGRESSION_LOCK_SHA256" "$e4b_regression_lock_sha256"
fi
if [[ -n "$e4b_qat_model" && "$long_e2e_configured" -ne 3 ]]; then
  echo "the E4B lane requires LLAMA_SERVER_BIN and locked long-E2E inputs" >&2
  exit 1
fi
if [[ "$mode" == "release" && ( -z "$e4b_qat_model" || "$e4b_regression_configured" -ne 4 ) ]]; then
  echo "release mode requires E4B_QAT_MODEL and reviewed frozen E4B regression evidence" >&2
  exit 1
fi

e2b_models_dir="$(dirname "$(dirname "$e2b_model")")"
if [[ -n "$e4b_qat_model" && -z "$e4b_models_dir" ]]; then
  e4b_models_dir="$(dirname "$(dirname "$e4b_qat_model")")"
fi

require_file "E2B_MODEL" "$e2b_model"
require_file "GEMMA12B_Q4_MODEL" "$gemma12b_q4_model"
require_file "LLAMA_CPP_BIN" "$llama_cpp_bin"
if [[ "$long_e2e_configured" -eq 3 ]]; then
  require_file "LLAMA_SERVER_BIN" "$llama_server_bin"
  require_file "LONG_E2E_LOCK" "$long_e2e_lock"
fi
if [[ -n "$e4b_qat_model" ]]; then
  require_file "E4B_QAT_MODEL" "$e4b_qat_model"
fi
if [[ "$e4b_regression_configured" -eq 4 ]]; then
  require_file "E4B_BASELINE_EVIDENCE" "$e4b_baseline_evidence"
  require_file "E4B_REGRESSION_LOCK" "$e4b_regression_lock"
fi

command -v "$zig_bin" >/dev/null
command -v nvidia-smi >/dev/null
command -v nvcc >/dev/null
command -v python3 >/dev/null
if [[ "$CUDA_VISIBLE_DEVICES" == *,* ]]; then
  echo "cuda_l4 E2E requires exactly one CUDA_VISIBLE_DEVICES selector" >&2
  exit 1
fi
gpu_info="$(nvidia-smi --id="$CUDA_VISIBLE_DEVICES" --query-gpu=name,driver_version,compute_cap --format=csv,noheader)"
printf 'CUDA device: %s\n' "$gpu_info"
if [[ "$gpu_info" != *"L4"* || "$gpu_info" != *"8.9"* ]]; then
  echo "cuda_l4 E2E requires an NVIDIA L4 with compute capability 8.9" >&2
  exit 1
fi
nvcc --version
"$zig_bin" version

(
  cd "$inference_dir"
  "$zig_bin" build quant-kernel-codegen \
    -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 \
    -Doptimize=ReleaseFast -- --check
  bash scripts/regen-cuda-artifacts.sh --check --all
  "$zig_bin" build -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 \
    -Doptimize=ReleaseFast -j4
)
require_file "ANTFLY_BIN" "$antfly_bin"

(
  cd "$inference_dir"
  "$antfly_bin" cuda-info --smoke
  "$zig_bin" build quant-kernel-cuda-ffn-diff \
    -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 \
    -Doptimize=ReleaseFast -- --json
  python3 scripts/validate_gemma4_cuda_candidate.py \
    --kernel-id cuda.attention.gqa.decode.score_prework \
    --qualification-profile screening \
    --binary "$antfly_bin" \
    --artifact-check-script scripts/regen-cuda-artifacts.sh \
    --model "$e2b_model" \
    --prompt-fixture scripts/fixtures/gemma4_long_context_v1.json \
    --lengths 300 \
    --prefill-chunk-size 512 \
    --cache-dtype f16 \
    --capture-kv-capacity 2432 \
    --config-label l4-sm89-long-context-f16-decode-score-prework-screening \
    --output-dir "$evidence_dir/score-prework-screening"
  "$zig_bin" build quant-kernel-cuda-paged-attention-diff \
    -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 \
    -Doptimize=ReleaseFast -- \
    --candidate-hd256 src/ops/cuda/artifacts/inference_cuda_kernels.fatbin \
    --candidate-hd512 src/ops/cuda/artifacts/inference_cuda_kernels.fatbin \
    --kv-len 1024 --pattern all --key-format all --json \
    | tee "$evidence_dir/paged_attention_diff.json"
)

python3 "$inference_dir/scripts/benchmark_gemma4_cuda_batching.py" \
  --antfly-bin "$antfly_bin" \
  --model "$e2b_model" \
  --models-dir "$repo_root/.models" \
  --output-dir "$evidence_dir/batching" \
  --cache-dtype polar4 \
  --tokens 64 \
  --concurrency 1 2 \
  --warmups 1 \
  --repeats 2 \
  --min-c2-speedup 0.40 \
  --max-c1-p95-ratio 1.20

release_args=(
  python3 "$inference_dir/scripts/gemma4_cuda_l4_release_gate.py"
  --binary "$antfly_bin"
  --llama-cpp-bin "$llama_cpp_bin"
  --e2b-model "$e2b_model"
  --gemma12b-q4-model "$gemma12b_q4_model"
  --output-dir "$evidence_dir"
  --warmups 1
  --repeats 3
  --timeout-sec 600
)
if [[ "$mode" == "release" ]]; then
  release_args+=(--enforce-performance --min-comparable-ratio 0.70 --verify-artifacts)
fi
"${release_args[@]}"

if [[ "$long_e2e_configured" -eq 3 ]]; then
  long_e2e_dir="$evidence_dir/long-e2e"
  benchmark_rc=0
  python3 "$inference_dir/scripts/benchmark_gemma4_long_e2e_server.py" \
    --antfly-bin "$antfly_bin" \
    --llama-server-bin "$llama_server_bin" \
    --backend cuda \
    --profile headline \
    --model "$e2b_model" \
    --models-dir "$e2b_models_dir" \
    --output-dir "$long_e2e_dir" \
    --warmups 2 \
    --repeats 10 \
    --lockfile "$long_e2e_lock" \
    --lockfile-sha256 "$long_e2e_lock_sha256" \
    --require-lock \
    --enforce-performance || benchmark_rc=$?
  python3 "$inference_dir/scripts/merge_gemma4_long_e2e_release_summary.py" \
    "$evidence_dir/release_summary.json" \
    "$long_e2e_dir/evidence.json" \
    "$benchmark_rc"
  [[ "$benchmark_rc" -eq 0 ]] || exit "$benchmark_rc"
fi

if [[ -n "$e4b_qat_model" ]]; then
  e4b_dir="$evidence_dir/e4b-regression"
  e4b_args=(
    python3 "$inference_dir/scripts/benchmark_gemma4_long_e2e_server.py"
    --antfly-bin "$antfly_bin"
    --llama-server-bin "$llama_server_bin"
    --backend cuda
    --profile e4b-regression
    --model "$e4b_qat_model"
    --models-dir "$e4b_models_dir"
    --output-dir "$e4b_dir"
    --warmups 2
  )
  if [[ "$e4b_regression_configured" -eq 4 ]]; then
    e4b_args+=(
      --repeats 10
      --baseline-evidence "$e4b_baseline_evidence"
      --baseline-sha256 "$e4b_baseline_sha256"
      --lockfile "$e4b_regression_lock"
      --lockfile-sha256 "$e4b_regression_lock_sha256"
      --require-lock
      --enforce-performance
    )
  else
    e4b_args+=(--repeats 3 --collect-only)
  fi
  benchmark_rc=0
  "${e4b_args[@]}" || benchmark_rc=$?
  python3 "$inference_dir/scripts/merge_gemma4_long_e2e_release_summary.py" \
    --lane-field e4b_regression \
    --lane-label "Gemma 4 E4B warm-server correctness/regression lane" \
    "$evidence_dir/release_summary.json" \
    "$e4b_dir/evidence.json" \
    "$benchmark_rc"
  [[ "$benchmark_rc" -eq 0 ]] || exit "$benchmark_rc"
fi

# Nightly-mode MTP is collection-only and cannot fail the target-only release gate.
if [[ "$mode" == "nightly" ]]; then
  mtp_dir="$evidence_dir/mtp"
  mkdir -p "$mtp_dir"
  if [[ ! -f "$mtp_draft_model" ]]; then
    printf '%s\n' \
      'diagnostic_status=skipped_missing_draft' \
      'release_contract=none; experimental diagnostic only' \
      > "$mtp_dir/mtp_collection_profile.txt"
  else
    source "$inference_dir/scripts/gemma4_qat_cuda_tuning.sh"
    gemma4_qat_cuda_tuning_env 544
    export "${GEMMA4_QAT_CUDA_ENV[@]}"
    export ANTFLY_GEMMA4_MTP_TARGET_REPLAY=auto
    export ANTFLY_GEMMA4_MTP_UNSAFE_TARGET_REPLAY=0
    export ANTFLY_GEMMA4_MTP_VERIFY_DEVICE_RESULT=1
    export ANTFLY_GEMMA4_MTP_PREPROJECT_FUSION=1
    export ANTFLY_GEMMA4_MTP_MASKED_SELECT_FUSION=0
    export ANTFLY_GEMMA4_MTP_MASKED_SELECT_HIDDEN_FUSION=0
    export ANTFLY_GEMMA4_MTP_ADAPTIVE_K=1
    export ANTFLY_GEMMA4_MTP_AUTO_MAX_K=1
    export ANTFLY_GEMMA4_MTP_AUTO_COST_PROBE_ROUNDS=16
    export ANTFLY_GEMMA4_MTP_AUTO_MIN_ACCEPTED_PER_ROUND_MILLI=1500
    export ANTFLY_GEMMA4_MTP_HIDDEN_ONLY_MATERIALIZE=1
    export ANTFLY_GEMMA4_MTP_ACTIVE_MATERIALIZE=1
    export ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1
    export ANTFLY_GEMMA4_MTP_DEDICATED_RUNTIME=1
    export ANTFLY_GEMMA4_MTP_MATERIALIZE_REPLAY=0
    export ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE=0
    export ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE_TARGET_ACTIVATION=0
    export ANTFLY_GEMMA4_MTP_ASSISTANT_REPLAY=0
    mtp_status=0
    RUN_MTP=required \
    ANTFLY_BIN="$antfly_bin" \
    MTP_TARGET_MODEL="$e2b_model" \
    MTP_DRAFT_MODEL="$mtp_draft_model" \
    MTP_PROMPT="Here is a sentence about ants:" \
    MTP_TOKENS=255 \
    MTP_SPECULATIVE_K=1 \
    MTP_MIN_ACTIVE_SPEED_RATIO=0 \
    OUT_DIR="$mtp_dir" \
      "$inference_dir/scripts/gemma4_cuda_production_gate.sh" --mtp-only || mtp_status=$?
    printf '%s\n' \
      'comparison=collection-only; not paired or interleaved with llama.cpp' \
      'prompt=Here is a sentence about ants:' \
      'tokens=255' \
      'requested_speculative_k=1' \
      'auto_max_k=1' \
      'effective_speculative_k_when_active=1' \
      'release_contract=none; experimental diagnostic only' \
      "diagnostic_exit_code=$mtp_status" \
      > "$mtp_dir/mtp_collection_profile.txt"
  fi
fi

python3 - "$evidence_dir/release_summary.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
summary = json.loads(path.read_text(encoding="utf-8"))
if summary.get("release_scope") != "target_only":
    raise SystemExit(f"unexpected release_scope in {path}")
if not summary.get("passed"):
    raise SystemExit(f"CUDA L4 E2E evidence failed: {path}")
print(json.dumps(summary, indent=2, sort_keys=True))
PY
printf 'CUDA L4 E2E evidence: %s\n' "$evidence_dir"
