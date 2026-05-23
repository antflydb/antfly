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

zig_build_flags_raw="${TERMITE_GLINER2_NON_TOY_ZIG_BUILD_FLAGS:-}"
zig_build_flags=()
if [[ -n "$zig_build_flags_raw" ]]; then
  # shellcheck disable=SC2206
  zig_build_flags=($zig_build_flags_raw)
fi
mlx_build=0
for flag in "${zig_build_flags[@]}"; do
  if [[ "$flag" == "-Dmlx=true" ]]; then
    mlx_build=1
  fi
done

if [[ "$mlx_build" == "0" ]]; then
  export ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-/private/tmp/zig-global-cache-termite-gliner2-readiness}"
  export ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-/private/tmp/zig-local-cache-termite-gliner2-readiness}"
fi

model_dir="${TERMITE_GLINER2_REAL_MODEL_DIR:-/private/tmp/termite-models/gliner2}"
source_url="${TERMITE_GLINER2_NON_TOY_SOURCE_URL:-https://raw.githubusercontent.com/autoih/conll2003/master/CoNLL-2003/eng.train}"
source_path="${TERMITE_GLINER2_NON_TOY_SOURCE_PATH:-/private/tmp/conll2003-eng.train}"
dataset_jsonl="${TERMITE_GLINER2_NON_TOY_JSONL:-/private/tmp/gliner2-conll2003-train-200.jsonl}"
converted_examples="${TERMITE_GLINER2_NON_TOY_CONVERTED_EXAMPLES:-200}"
entity_types="${TERMITE_GLINER2_NON_TOY_ENTITY_TYPES:-person,organization,location}"
out_dir="${TERMITE_GLINER2_NON_TOY_OUT_DIR:-/private/tmp/termite-gliner2-non-toy-run}"

epochs="${TERMITE_GLINER2_NON_TOY_EPOCHS:-5}"
batch_size="${TERMITE_GLINER2_NON_TOY_BATCH_SIZE:-1}"
max_examples="${TERMITE_GLINER2_NON_TOY_MAX_EXAMPLES:-100}"
seq_len="${TERMITE_GLINER2_NON_TOY_SEQ_LEN:-128}"
num_classes="${TERMITE_GLINER2_NON_TOY_NUM_CLASSES:-4}"
learning_rate="${TERMITE_GLINER2_NON_TOY_LR:-1e-3}"
objective="${TERMITE_GLINER2_NON_TOY_OBJECTIVE:-token}"
max_span_width="${TERMITE_GLINER2_NON_TOY_MAX_SPAN_WIDTH:-4}"

run_train="${TERMITE_GLINER2_NON_TOY_RUN_TRAIN:-0}"
require_loss_decrease="${TERMITE_GLINER2_NON_TOY_REQUIRE_LOSS_DECREASE:-1}"
force_convert="${TERMITE_GLINER2_NON_TOY_FORCE_CONVERT:-0}"

min_supervised_tps="${TERMITE_GLINER2_NON_TOY_MIN_SUPERVISED_TPS:-0.05}"
min_examples="${TERMITE_GLINER2_NON_TOY_MIN_EXAMPLES:-100}"
min_steps="${TERMITE_GLINER2_NON_TOY_MIN_STEPS:-500}"
min_entity_labels="${TERMITE_GLINER2_NON_TOY_MIN_ENTITY_LABELS:-2}"
min_supervised_tokens="${TERMITE_GLINER2_NON_TOY_MIN_SUPERVISED_TOKENS:-10000}"
min_entity_tokens="${TERMITE_GLINER2_NON_TOY_MIN_ENTITY_TOKENS:-2000}"
max_avg_step_wall_ms="${TERMITE_GLINER2_NON_TOY_MAX_AVG_STEP_WALL_MS:-300000}"
max_total_execute_ms="${TERMITE_GLINER2_NON_TOY_MAX_TOTAL_EXECUTE_MS:-3000000}"
max_peak_resident_bytes="${TERMITE_GLINER2_NON_TOY_MAX_PEAK_RESIDENT_BYTES:-2500000000}"

semantic_text="${TERMITE_GLINER2_NON_TOY_SEMANTIC_TEXT:-Alice joined Acme in Paris}"
expect_text="${TERMITE_GLINER2_NON_TOY_EXPECT_TEXT:-}"
expect_label="${TERMITE_GLINER2_NON_TOY_EXPECT_LABEL:-}"
min_score="${TERMITE_GLINER2_NON_TOY_MIN_SCORE:-}"

run_step() {
  printf '\n==> %s\n' "$*"
  "$@"
}

print_command() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
}

if [[ ! -f "$model_dir/model.safetensors" ]]; then
  echo "missing model weights at $model_dir/model.safetensors" >&2
  exit 1
fi

if [[ "$force_convert" == "1" || ! -f "$dataset_jsonl" ]]; then
  if [[ ! -f "$source_path" ]]; then
    run_step curl -fsSL "$source_url" -o "$source_path"
  fi
  run_step python3 scripts/convert_conll_ner_to_gliner2_jsonl.py \
    "$source_path" \
    "$dataset_jsonl" \
    --max-examples "$converted_examples"
fi

zig_build=(zig build "${zig_build_flags[@]}")

run_step "${zig_build[@]}" inspect-gliner2-dataset -- \
  "$model_dir" \
  "$dataset_jsonl" \
  "$entity_types" \
  - 256 8 "$max_span_width" false \
  --preset non-toy --fail-on-readiness

train_cmd=(
  "${zig_build[@]}" run-gliner2-autodiff-smoke-workflow --
  "$model_dir"
  "$dataset_jsonl"
  "$out_dir"
  --epochs "$epochs"
  --batch-size "$batch_size"
  --max-examples "$max_examples"
  --seq-len "$seq_len"
  --num-classes "$num_classes"
  --learning-rate "$learning_rate"
  --objective "$objective"
  --max-span-width "$max_span_width"
)
if [[ "$require_loss_decrease" == "1" ]]; then
  train_cmd+=(--require-loss-decrease)
fi

validate_cmd=(
  "${zig_build[@]}" validate-gliner2-autodiff-run --
  "$out_dir"
  --min-supervised-tokens-per-second "$min_supervised_tps"
  --min-examples "$min_examples"
  --min-steps "$min_steps"
  --min-entity-labels "$min_entity_labels"
  --min-supervised-tokens "$min_supervised_tokens"
  --min-entity-tokens "$min_entity_tokens"
  --max-avg-step-wall-ms "$max_avg_step_wall_ms"
  --max-peak-resident-bytes "$max_peak_resident_bytes"
)
if [[ "$require_loss_decrease" == "1" ]]; then
  validate_cmd+=(--require-loss-decrease)
fi
if [[ -n "$max_total_execute_ms" ]]; then
  validate_cmd+=(--max-total-execute-ms "$max_total_execute_ms")
fi

semantic_cmd=(
  "${zig_build[@]}" eval-gliner2-autodiff-adapter --
  "$model_dir"
  "$out_dir"
  "$semantic_text"
  --seq-len 64
  --max-span-width "$max_span_width"
  --objective "$objective"
)
if [[ -n "$expect_text" ]]; then
  semantic_cmd+=(--expect-text "$expect_text")
fi
if [[ -n "$expect_label" ]]; then
  semantic_cmd+=(--expect-label "$expect_label")
fi
if [[ -n "$min_score" ]]; then
  semantic_cmd+=(--min-score "$min_score")
fi

if [[ "$run_train" != "1" ]]; then
  cat <<EOF

Non-toy dataset readiness passed.

The full model-backed acceptance run is intentionally opt-in because the
current native backend takes roughly several minutes per step, and accelerated
builds require local GPU access. Set TERMITE_GLINER2_NON_TOY_RUN_TRAIN=1 to
execute:
EOF
  print_command "${train_cmd[@]}"
  print_command "${validate_cmd[@]}"
  print_command "${semantic_cmd[@]}"
  if [[ -z "$expect_text" || -z "$expect_label" || -z "$min_score" ]]; then
    cat <<EOF

Semantic golden enforcement is not fully configured. Set
TERMITE_GLINER2_NON_TOY_EXPECT_TEXT, TERMITE_GLINER2_NON_TOY_EXPECT_LABEL, and
TERMITE_GLINER2_NON_TOY_MIN_SCORE after choosing the golden from a completed
acceptance run.
EOF
  fi
  exit 0
fi

run_step "${train_cmd[@]}"
run_step "${validate_cmd[@]}"
run_step "${semantic_cmd[@]}"

echo "GLiNER2 non-toy acceptance workflow completed"
