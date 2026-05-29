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

model_dir="${TERMITE_GLINER2_MODEL_DIR:-/Users/tim/.cache/gliner2}"
text="${TERMITE_GLINER2_TEXT:-John works at Google in California.}"
labels_csv="${TERMITE_GLINER2_LABELS:-person,organization,location}"
world_size="${TERMITE_GLINER2_WORLD_SIZE:-2}"
score_tolerance="${TERMITE_GLINER2_SCORE_TOLERANCE:-0.01}"

if [[ ! -f "$model_dir/tokenizer.json" ]]; then
  echo "missing tokenizer at $model_dir/tokenizer.json" >&2
  exit 1
fi

if [[ ! -f "$model_dir/model.safetensors" && ! -f "$model_dir/pytorch_model.safetensors" ]]; then
  echo "missing safetensors weights at $model_dir" >&2
  exit 1
fi

label_args=()
IFS=',' read -r -a labels <<<"$labels_csv"
for label in "${labels[@]}"; do
  trimmed="$(printf '%s' "$label" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [[ -n "$trimmed" ]]; then
    label_args+=(--label "$trimmed")
  fi
done

if (( ${#label_args[@]} == 0 )); then
  echo "TERMITE_GLINER2_LABELS produced no labels" >&2
  exit 1
fi

ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache-termite-gliner2-probe \
ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache-termite-gliner2-probe \
zigup run master build probe-gliner2-recognize

blas_output="$({
  ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache-termite-gliner2-probe \
  ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache-termite-gliner2-probe \
  ./zig-out/bin/probe-gliner2-recognize \
    "$model_dir" \
    "$text" \
    "${label_args[@]}" \
    --backend blas
} 2>&1)"

printf '%s\n' "$blas_output"

grep -q "selected_backend=blas" <<<"$blas_output"
grep -q "pipeline uses_distributed_mlx=false uses_tensor_parallel_mlx=false" <<<"$blas_output"
grep -q '^{' <<<"$blas_output"

tmpdir="$(mktemp -d /tmp/termite-gliner2-dist.XXXXXX)"
trap 'rm -rf "$tmpdir"' EXIT
hostfile="$tmpdir/hosts.json"
{
  printf '[\n'
  for ((rank=0; rank<world_size; rank++)); do
    port=$((6050 + rank))
    if (( rank > 0 )); then
      printf ',\n'
    fi
    printf '  ["127.0.0.1:%s"]' "$port"
  done
  printf '\n]\n'
} > "$hostfile"

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
    ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache-termite-gliner2-probe \
    ZIG_LOCAL_CACHE_DIR=/tmp/zig-local-cache-termite-gliner2-probe \
    ./zig-out/bin/probe-gliner2-recognize \
      "$model_dir" \
      "$text" \
      "${label_args[@]}" \
      --backend mlx > "$tmpdir/rank_${rank}.log" 2>&1
  ) &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

for ((rank=0; rank<world_size; rank++)); do
  cat "$tmpdir/rank_${rank}.log"
done

rank0_output="$(cat "$tmpdir/rank_0.log")"
grep -q "selected_backend=mlx" <<<"$rank0_output"
grep -q "distributed enabled=true mode=tensor_parallel" <<<"$rank0_output"
grep -q "pipeline uses_distributed_mlx=true uses_tensor_parallel_mlx=true" <<<"$rank0_output"
grep -q '^{' <<<"$rank0_output"

BLAS_OUTPUT="$blas_output" MLX_OUTPUT="$rank0_output" python3 - "$score_tolerance" <<'PY'
import json
import os
import sys

tolerance = float(sys.argv[1])
blas_output = os.environ["BLAS_OUTPUT"]
mlx_output = os.environ["MLX_OUTPUT"]

def extract_payload(raw: str) -> dict:
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise SystemExit("missing JSON payload in probe output")

def normalize(payload: dict):
    entities = payload.get("entities", [])
    return [
        {
            "text": entity["text"],
            "label": entity["label"],
            "start": entity["start"],
            "end": entity["end"],
            "score": float(entity["score"]),
        }
        for entity in entities
    ]

blas_entities = normalize(extract_payload(blas_output))
mlx_entities = normalize(extract_payload(mlx_output))

if len(blas_entities) != len(mlx_entities):
    raise SystemExit(f"entity count mismatch: blas={len(blas_entities)} mlx={len(mlx_entities)}")

for idx, (lhs, rhs) in enumerate(zip(blas_entities, mlx_entities)):
    for field in ("text", "label", "start", "end"):
        if lhs[field] != rhs[field]:
            raise SystemExit(f"entity {idx} field {field} mismatch: {lhs[field]!r} != {rhs[field]!r}")
    diff = abs(lhs["score"] - rhs["score"])
    print(f"entity[{idx}] {lhs['label']} score_diff={diff:.6f}")
    if diff > tolerance:
        raise SystemExit(f"entity {idx} score diff {diff:.6f} exceeds tolerance {tolerance:.6f}")

print(f"verified {len(blas_entities)} entities")
PY

echo "gliner2 distributed MLX smoke verification completed"
