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

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

default_models_root="${ANTFLY_INFERENCE_MODELS_DIR:-${HOME:+$HOME/.antfly/inference/models}}"
model_dir="${ANTFLY_MXBAI_RERANK_MODEL_DIR:-${default_models_root:+$default_models_root/mixedbread-ai/mxbai-rerank-base-v1}}"
model_dirs_raw="${ANTFLY_MXBAI_RERANK_MODEL_DIRS:-}"
graph_modes_raw="${ANTFLY_MXBAI_RERANK_GRAPH_MODES:-off,required}"
qmatmul_variants_raw="${ANTFLY_MXBAI_RERANK_QMATMUL_VARIANTS:-auto}"
measure_iters="${ANTFLY_MXBAI_RERANK_SOAK_ITERS:-100}"
warmup_iters="${ANTFLY_MXBAI_RERANK_SOAK_WARMUP_ITERS:-2}"
timeout_seconds="${ANTFLY_MXBAI_RERANK_SOAK_TIMEOUT:-900}"
cuda_artifacts="${ANTFLY_CUDA_ARTIFACTS:-fatbin}"
cuda_libraries="${ANTFLY_CUDA_LIBS:-auto}"
zig_global_cache_dir="${ZIG_GLOBAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-zig-global-cache}"

resolve_zig() {
  if [[ -n "${ZIG:-}" ]]; then
    printf '%s\n' "$ZIG"
  elif command -v zig >/dev/null 2>&1; then
    command -v zig
  elif [[ -x "../../../.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig" ]]; then
    printf '%s\n' "../../../.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig"
  else
    echo "zig not found; set ZIG=/path/to/zig" >&2
    return 1
  fi
}

if [[ -z "$model_dir" && -z "$model_dirs_raw" ]]; then
  echo "set ANTFLY_MXBAI_RERANK_MODEL_DIR, or set ANTFLY_INFERENCE_MODELS_DIR/HOME for the default model location" >&2
  exit 1
fi

split_csv() {
  local raw="$1"
  local -n out_ref="$2"
  local old_ifs="$IFS"
  IFS=','
  read -r -a out_ref <<<"$raw"
  IFS="$old_ifs"
}

validate_model_dir() {
  local dir="$1"
  if [[ ! -f "$dir/tokenizer.json" ]]; then
    echo "missing tokenizer at $dir/tokenizer.json" >&2
    return 1
  fi
  if [[ ! -f "$dir/model.safetensors" && ! -f "$dir/model.gguf" && -z "$(find "$dir" -maxdepth 1 -type f -name '*.gguf' -print -quit 2>/dev/null)" ]]; then
    echo "missing model weights in $dir; expected model.safetensors, model.gguf, or a GGUF file" >&2
    return 1
  fi
}

model_dirs=()
if [[ -n "$model_dirs_raw" ]]; then
  split_csv "$model_dirs_raw" model_dirs
else
  model_dirs=("$model_dir")
fi

graph_modes=()
split_csv "$graph_modes_raw" graph_modes

qmatmul_variants=()
split_csv "$qmatmul_variants_raw" qmatmul_variants

for candidate_model_dir in "${model_dirs[@]}"; do
  validate_model_dir "$candidate_model_dir"
done

zig_bin="$(resolve_zig)"

before_gpu_mem="unknown"
after_gpu_mem="unknown"
if command -v nvidia-smi >/dev/null 2>&1; then
  before_gpu_mem="$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits | head -n1 || true)"
fi

echo "gpu_memory_before_mib=${before_gpu_mem}"

for candidate_model_dir in "${model_dirs[@]}"; do
  for graph_mode in "${graph_modes[@]}"; do
    for qmatmul_variant in "${qmatmul_variants[@]}"; do
      graph_env=()
      case "$graph_mode" in
        off)
          graph_env+=("ANTFLY_CUDA_ENABLE_DEBERTA_GRAPHS=0" "ANTFLY_CUDA_REQUIRE_DEBERTA_GRAPHS=0")
          ;;
        enabled)
          graph_env+=("ANTFLY_CUDA_ENABLE_DEBERTA_GRAPHS=1" "ANTFLY_CUDA_REQUIRE_DEBERTA_GRAPHS=0")
          ;;
        required)
          graph_env+=("ANTFLY_CUDA_ENABLE_DEBERTA_GRAPHS=1" "ANTFLY_CUDA_REQUIRE_DEBERTA_GRAPHS=1")
          ;;
        *)
          echo "unknown graph mode '$graph_mode'; expected off, enabled, or required" >&2
          exit 1
          ;;
      esac
      case "$qmatmul_variant" in
        auto|legacy|fast_r2c4|fast_r2c8|fast_r4c4|r2c4|r2c8|r4c4|tc_hmma|hmma|tensor_core)
          ;;
        *)
          echo "unknown qmatmul variant '$qmatmul_variant'; expected auto, legacy, fast_r2c4, fast_r2c8, fast_r4c4, or tc_hmma" >&2
          exit 1
          ;;
      esac
      echo "model_dir=${candidate_model_dir} graph_mode=${graph_mode} qmatmul_variant=${qmatmul_variant}"
      env "${graph_env[@]}" ANTFLY_CUDA_QMATMUL_VARIANT="$qmatmul_variant" timeout "$timeout_seconds" "$zig_bin" build --global-cache-dir "$zig_global_cache_dir" -Dcuda=true -Dcuda-artifacts="$cuda_artifacts" -Dcuda-libs="$cuda_libraries" bench-reranker-e2e -- \
        --model-dir "$candidate_model_dir" \
        --backend cuda \
        --warmup-iters "$warmup_iters" \
        --measure-iters "$measure_iters" \
        --batch-sweep \
        --format csv
    done
  done
done

if command -v nvidia-smi >/dev/null 2>&1; then
  after_gpu_mem="$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits | head -n1 || true)"
fi

echo "gpu_memory_after_mib=${after_gpu_mem}"
echo "mxbai CUDA reranker soak completed"
