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

export ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-/private/tmp/zig-global-cache-termite-gliner2-readiness}"
export ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-/private/tmp/zig-local-cache-termite-gliner2-readiness}"

model_dir="${TERMITE_GLINER2_REAL_MODEL_DIR:-/private/tmp/termite-models/gliner2}"
smoke_jsonl="${TERMITE_GLINER2_REAL_NER_JSONL:-testdata/gliner2_ner_smoke.jsonl}"
out_dir="${TERMITE_GLINER2_READINESS_OUT_DIR:-/private/tmp/termite-gliner2-readiness-smoke}"
run_broad="${TERMITE_GLINER2_READINESS_RUN_BROAD:-1}"
require_model="${TERMITE_GLINER2_READINESS_REQUIRE_MODEL:-1}"
min_supervised_tps="${TERMITE_GLINER2_READINESS_MIN_SUPERVISED_TPS:-0.05}"
max_avg_step_wall_ms="${TERMITE_GLINER2_READINESS_MAX_AVG_STEP_WALL_MS:-300000}"
max_total_execute_ms="${TERMITE_GLINER2_READINESS_MAX_TOTAL_EXECUTE_MS:-300000}"
max_peak_resident_bytes="${TERMITE_GLINER2_READINESS_MAX_PEAK_RESIDENT_BYTES:-1200000000}"
min_examples="${TERMITE_GLINER2_READINESS_MIN_EXAMPLES:-1}"
min_steps="${TERMITE_GLINER2_READINESS_MIN_STEPS:-1}"
min_entity_labels="${TERMITE_GLINER2_READINESS_MIN_ENTITY_LABELS:-1}"
min_supervised_tokens="${TERMITE_GLINER2_READINESS_MIN_SUPERVISED_TOKENS:-1}"
min_entity_tokens="${TERMITE_GLINER2_READINESS_MIN_ENTITY_TOKENS:-1}"

run_step() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_step zig build test-gliner2-data --summary failures
run_step zig build test-gliner2-e2e --summary failures
run_step zig build test-gliner2-run-validation --summary failures
run_step zig build test-gliner2-cleanup-bundle --summary failures
run_step zig build test-finetune --summary failures

run_step zig build inspect-gliner2-dataset -- \
  "$model_dir" \
  "$smoke_jsonl" \
  person,organization,location \
  - 256 8 4 false \
  --preset smoke --fail-on-readiness

printf '\n==> smoke fixture must fail non-toy readiness\n'
if zig build inspect-gliner2-dataset -- \
  "$model_dir" \
  "$smoke_jsonl" \
  person,organization,location \
  - 256 8 4 false \
  --preset non-toy --fail-on-readiness; then
  echo "smoke fixture unexpectedly passed non-toy readiness" >&2
  exit 1
fi

if [[ ! -f "$model_dir/model.safetensors" ]]; then
  if [[ "$require_model" == "1" ]]; then
    echo "missing model weights at $model_dir/model.safetensors" >&2
    exit 1
  fi
  echo "skipping model-backed GLiNER2 readiness gates; missing $model_dir/model.safetensors" >&2
else
  if [[ ! -f "$smoke_jsonl" ]]; then
    echo "missing smoke dataset at $smoke_jsonl" >&2
    exit 1
  fi

  printf '\n==> class-capacity failure must stop before training\n'
  if zig build train-gliner2-autodiff -- \
    --model-dir "$model_dir" \
    --train-data "$smoke_jsonl" \
    --out-dir "${out_dir}-capacity-fail" \
    --epochs 1 \
    --batch-size 1 \
    --max-examples 1 \
    --seq-len 64 \
    --num-classes 3; then
    echo "class-capacity failure command unexpectedly succeeded" >&2
    exit 1
  fi

  run_step zig build run-gliner2-autodiff-smoke-workflow -- \
    "$model_dir" \
    "$smoke_jsonl" \
    "$out_dir" \
    --epochs 1 \
    --batch-size 1 \
    --max-examples 1 \
    --seq-len 64 \
    --num-classes 4

  validate_args=("$out_dir")
  if [[ -n "$min_supervised_tps" ]]; then
    validate_args+=(--min-supervised-tokens-per-second "$min_supervised_tps")
  fi
  if [[ -n "$max_avg_step_wall_ms" ]]; then
    validate_args+=(--max-avg-step-wall-ms "$max_avg_step_wall_ms")
  fi
  if [[ -n "$max_total_execute_ms" ]]; then
    validate_args+=(--max-total-execute-ms "$max_total_execute_ms")
  fi
  if [[ -n "$max_peak_resident_bytes" ]]; then
    validate_args+=(--max-peak-resident-bytes "$max_peak_resident_bytes")
  fi
  if [[ -n "$min_examples" ]]; then
    validate_args+=(--min-examples "$min_examples")
  fi
  if [[ -n "$min_steps" ]]; then
    validate_args+=(--min-steps "$min_steps")
  fi
  if [[ -n "$min_entity_labels" ]]; then
    validate_args+=(--min-entity-labels "$min_entity_labels")
  fi
  if [[ -n "$min_supervised_tokens" ]]; then
    validate_args+=(--min-supervised-tokens "$min_supervised_tokens")
  fi
  if [[ -n "$min_entity_tokens" ]]; then
    validate_args+=(--min-entity-tokens "$min_entity_tokens")
  fi
  run_step zig build validate-gliner2-autodiff-run -- "${validate_args[@]}"

  run_step env \
    TERMITE_GLINER2_REAL_MODEL_DIR="$model_dir" \
    TERMITE_GLINER2_REAL_NER_JSONL="$smoke_jsonl" \
    zig build test-gliner2-real-training --summary failures
fi

if [[ "$run_broad" == "1" ]]; then
  run_step zig build test --summary failures
else
  echo "skipping broad zig build test because TERMITE_GLINER2_READINESS_RUN_BROAD=$run_broad"
fi

echo "GLiNER2 autodiff readiness verification completed"
