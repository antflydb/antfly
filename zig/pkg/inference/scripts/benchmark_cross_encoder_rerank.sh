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

model_dir="${ANTFLY_INFERENCE_RERANK_MODEL_DIR:-/Users/tim/.cache/bge-reranker-base}"
tokenizer_dir="${ANTFLY_INFERENCE_RERANK_TOKENIZER_DIR:-$model_dir}"
query="${ANTFLY_INFERENCE_RERANK_QUERY:-what is Antfly inference}"
document="${ANTFLY_INFERENCE_RERANK_DOCUMENT:-Antfly inference is a Zig inference runtime with native model runtimes}"
repeat="${ANTFLY_INFERENCE_RERANK_BENCH_REPEAT:-10}"
world_size="${ANTFLY_INFERENCE_RERANK_TP_WORLD_SIZE:-2}"

if [[ ! -f "$model_dir/model.safetensors" ]]; then
  echo "missing model weights at $model_dir/model.safetensors" >&2
  exit 1
fi

if [[ ! -f "$tokenizer_dir/tokenizer.json" ]]; then
  echo "missing tokenizer at $tokenizer_dir/tokenizer.json" >&2
  exit 1
fi

ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache-antfly-inference-rerank-probe \
ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache-antfly-inference-rerank-probe \
zigup run master build probe-cross-encoder-rerank

echo "== BLAS =="
ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache-antfly-inference-rerank-probe \
ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache-antfly-inference-rerank-probe \
./zig-out/bin/probe-cross-encoder-rerank \
  "$model_dir" \
  "$query" \
  "$document" \
  --tokenizer-dir "$tokenizer_dir" \
  --backend blas \
  --repeat "$repeat"

tmpdir="$(mktemp -d /tmp/antfly-inference-rerank-bench.XXXXXX)"
trap 'rm -rf "$tmpdir"' EXIT
hostfile="$tmpdir/hosts.json"
{
  printf '[\n'
  for ((rank=0; rank<world_size; rank++)); do
    port=$((5970 + rank))
    if (( rank > 0 )); then
      printf ',\n'
    fi
    printf '  ["127.0.0.1:%s"]' "$port"
  done
  printf '\n]\n'
} > "$hostfile"

echo
echo "== MLX TP (${world_size} ranks) =="
pids=()
for ((rank=0; rank<world_size; rank++)); do
  (
    export TERMITE_MLX_DISTRIBUTED_ENABLE=1
    export TERMITE_MLX_DISTRIBUTED_MODE=tensor_parallel
    export TERMITE_MLX_DISTRIBUTED_BACKEND=ring
    export TERMITE_MLX_WORLD_SIZE="$world_size"
    export TERMITE_MLX_RANK="$rank"
    export TERMITE_MLX_LOCAL_RANK="$rank"
    export MLX_WORLD_SIZE="$world_size"
    export MLX_RANK="$rank"
    export MLX_HOSTFILE="$hostfile"
    export TERMITE_MLX_ALLOW_CPU_STREAM_WITHOUT_METAL=1
    ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache-antfly-inference-rerank-probe \
    ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache-antfly-inference-rerank-probe \
    ./zig-out/bin/probe-cross-encoder-rerank \
      "$model_dir" \
      "$query" \
      "$document" \
      --tokenizer-dir "$tokenizer_dir" \
      --backend mlx \
      --repeat "$repeat" > "$tmpdir/rank_${rank}.log" 2>&1
  ) &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

for ((rank=0; rank<world_size; rank++)); do
  echo "-- rank ${rank} --"
  cat "$tmpdir/rank_${rank}.log"
done
