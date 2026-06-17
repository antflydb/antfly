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

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

default_models_root="${ANTFLY_INFERENCE_MODELS_DIR:-${HOME:+$HOME/.antfly/inference/models}}"
model_dir="${ANTFLY_MXBAI_RERANK_MODEL_DIR:-${default_models_root:+$default_models_root/mixedbread-ai/mxbai-rerank-base-v1}}"
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
  elif [[ -x "../../../.tools/zig-x86_64-linux-0.16.0/zig" ]]; then
    printf '%s\n' "../../../.tools/zig-x86_64-linux-0.16.0/zig"
  else
    echo "zig not found; set ZIG=/path/to/zig" >&2
    return 1
  fi
}

if [[ -z "$model_dir" ]]; then
  echo "set ANTFLY_MXBAI_RERANK_MODEL_DIR, or set ANTFLY_INFERENCE_MODELS_DIR/HOME for the default model location" >&2
  exit 1
fi
if [[ ! -f "$model_dir/model.safetensors" ]]; then
  echo "missing model weights at $model_dir/model.safetensors" >&2
  exit 1
fi
if [[ ! -f "$model_dir/tokenizer.json" ]]; then
  echo "missing tokenizer at $model_dir/tokenizer.json" >&2
  exit 1
fi

zig_bin="$(resolve_zig)"

before_gpu_mem="unknown"
after_gpu_mem="unknown"
if command -v nvidia-smi >/dev/null 2>&1; then
  before_gpu_mem="$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits | head -n1 || true)"
fi

echo "gpu_memory_before_mib=${before_gpu_mem}"

timeout "$timeout_seconds" "$zig_bin" build --global-cache-dir "$zig_global_cache_dir" -Dcuda=true -Dcuda-artifacts="$cuda_artifacts" -Dcuda-libs="$cuda_libraries" bench-reranker-e2e -- \
  --model-dir "$model_dir" \
  --backend cuda \
  --warmup-iters "$warmup_iters" \
  --measure-iters "$measure_iters" \
  --batch-sweep \
  --format csv

if command -v nvidia-smi >/dev/null 2>&1; then
  after_gpu_mem="$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits | head -n1 || true)"
fi

echo "gpu_memory_after_mib=${after_gpu_mem}"
echo "mxbai CUDA reranker soak completed"
