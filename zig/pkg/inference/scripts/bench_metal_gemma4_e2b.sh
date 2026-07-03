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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/inference_cli.sh
source "$SCRIPT_DIR/inference_cli.sh"

ANTFLY_BIN="$(resolve_antfly_inference_bin)"
DEFAULT_MODELS_DIR="$HOME/.antfly/inference/models"
MODEL_NAME="${ANTFLY_INFERENCE_GEMMA4_MODEL_NAME:-ggml-org/gemma-4-e2b-it-gguf}"
MODELS_DIR="${ANTFLY_INFERENCE_GEMMA4_MODELS_DIR:-$DEFAULT_MODELS_DIR}"
MODEL_DIR="${ANTFLY_INFERENCE_GEMMA4_MODEL:-$MODELS_DIR/$MODEL_NAME}"
if [[ -z "${ANTFLY_INFERENCE_GEMMA4_MODEL_NAME:-}" && "$MODEL_DIR" == "$MODELS_DIR/"* ]]; then
  MODEL_NAME="${MODEL_DIR#"$MODELS_DIR"/}"
fi
PROMPT="${ANTFLY_INFERENCE_GEMMA4_BENCH_PROMPT:-Write one short paragraph about local inference.}"
WARMUP_TOKENS="${ANTFLY_INFERENCE_GEMMA4_BENCH_WARMUP_TOKENS:-64}"
MAX_TOKENS="${ANTFLY_INFERENCE_GEMMA4_BENCH_MAX_TOKENS:-128}"
RUNS="${ANTFLY_INFERENCE_GEMMA4_BENCH_RUNS:-5}"
SERVER_WARM="${ANTFLY_INFERENCE_GEMMA4_BENCH_SERVER_WARM:-0}"
SERVER_TOKENS="${ANTFLY_INFERENCE_GEMMA4_BENCH_SERVER_TOKENS:-4 64}"
SERVER_PORT="${ANTFLY_INFERENCE_GEMMA4_BENCH_SERVER_PORT:-$((18090 + RANDOM % 1000))}"
MIN_DECODE_TOK_S="${ANTFLY_INFERENCE_GEMMA4_MIN_DECODE_TOK_S:-0}"
MIN_HOT_DECODE_TOK_S="${ANTFLY_INFERENCE_GEMMA4_MIN_HOT_DECODE_TOK_S:-0}"
MIN_GENERATED_TOKENS="${ANTFLY_INFERENCE_GEMMA4_MIN_GENERATED_TOKENS:-0}"
REQUIRE_MAX_TOKENS="${ANTFLY_INFERENCE_GEMMA4_REQUIRE_MAX_TOKENS:-0}"
MIN_PREFILL_FRAME_EXECUTE="${ANTFLY_INFERENCE_GEMMA4_MIN_PREFILL_FRAME_EXECUTE:-0}"
MIN_Q4_0_DISPATCH="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_DISPATCH:-0}"
MIN_Q4_0_PAIR_REDUCE="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PAIR_REDUCE:-0}"
MIN_Q4_0_PAIR_ACT_REDUCE="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PAIR_ACT_REDUCE:-0}"
MIN_Q4_0_ACTIVATION_RHS_REDUCE="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_ACTIVATION_RHS_REDUCE:-0}"
MIN_Q4_0_ACTIVATION_RHS_REDUCE_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_ACTIVATION_RHS_REDUCE_OUT_F16:-0}"
MIN_Q4_0_PLE_ACTIVATION_RHS_REDUCE_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PLE_ACTIVATION_RHS_REDUCE_OUT_F16:-0}"
MIN_Q4_0_PLE_LINEAR_REDUCE_IN_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PLE_LINEAR_REDUCE_IN_F16:-0}"
MIN_Q4_0_PAIR_ACT_REDUCE_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PAIR_ACT_REDUCE_OUT_F16:-0}"
MIN_Q4_0_PAIR_ACT_RMS_SCALE_REDUCE_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PAIR_ACT_RMS_SCALE_REDUCE_OUT_F16:-0}"
MIN_Q4_0_LINEAR_REDUCE_IN_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_LINEAR_REDUCE_IN_F16:-0}"
MIN_Q4_0_LINEAR_REDUCE_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_LINEAR_REDUCE_OUT_F16:-0}"
MIN_Q4_0_LINEAR_REDUCE_IN_F16_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_LINEAR_REDUCE_IN_F16_OUT_F16:-0}"
MIN_Q4_0_LINEAR_REDUCE_SUMSQ="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_LINEAR_REDUCE_SUMSQ:-0}"
MIN_RMS_NORM_ADD_SUMSQ="${ANTFLY_INFERENCE_GEMMA4_MIN_RMS_NORM_ADD_SUMSQ:-0}"
MAX_Q4_0_LINEAR_REDUCE_SUMSQ="${ANTFLY_INFERENCE_GEMMA4_MAX_Q4_0_LINEAR_REDUCE_SUMSQ:--1}"
MAX_RMS_NORM_ADD_SUMSQ="${ANTFLY_INFERENCE_GEMMA4_MAX_RMS_NORM_ADD_SUMSQ:--1}"
MIN_PAGED_ATTENTION_1X="${ANTFLY_INFERENCE_GEMMA4_MIN_PAGED_ATTENTION_1X:-0}"
MIN_Q4_PAIR_ACT_REDUCE_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_PAIR_ACT_REDUCE_OUT_F16:-0}"
MIN_Q6_REDUCE_IN_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q6_REDUCE_IN_F16:-0}"
MIN_ACTIVE_DECODE_SUCCESS="${ANTFLY_INFERENCE_GEMMA4_MIN_ACTIVE_DECODE_SUCCESS:-0}"
MIN_ACTIVE_DECODE_FINAL_FUSED_ARGMAX="${ANTFLY_INFERENCE_GEMMA4_MIN_ACTIVE_DECODE_FINAL_FUSED_ARGMAX:-0}"
MAX_LAST_COMPUTE_ENCODERS="${ANTFLY_INFERENCE_GEMMA4_MAX_LAST_COMPUTE_ENCODERS:-0}"
MIN_SERVER_TOK_S="${ANTFLY_INFERENCE_GEMMA4_MIN_SERVER_TOK_S:-0}"
MAX_SERVER_WARM_MS="${ANTFLY_INFERENCE_GEMMA4_MAX_SERVER_WARM_MS:-0}"
CACHE_DTYPE="${ANTFLY_INFERENCE_GEMMA4_CACHE_DTYPE:-}"
REUSE_PROBE="${ANTFLY_INFERENCE_GEMMA4_REUSE_PROBE:-1}"
RAW_PROMPT="${ANTFLY_INFERENCE_GEMMA4_RAW_PROMPT:-0}"
DRAFT_MODEL="${ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL:-}"
SPECULATIVE_K="${ANTFLY_INFERENCE_GEMMA4_SPECULATIVE_K:-4}"
SPECULATION_POLICY="${ANTFLY_INFERENCE_GEMMA4_SPECULATION_POLICY:-auto}"
SPECULATION_CALIBRATION="${ANTFLY_INFERENCE_GEMMA4_SPECULATION_CALIBRATION:-positive}"
REQUIRE_MTP_ENABLED="${ANTFLY_INFERENCE_GEMMA4_REQUIRE_MTP_ENABLED:-0}"
MIN_SPECULATIVE_ROUNDS="${ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_ROUNDS:-0}"
MIN_SPECULATIVE_DRAFTED="${ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_DRAFTED:-0}"
MIN_SPECULATIVE_MATCHED="${ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_MATCHED:-0}"
MIN_SPECULATIVE_ACCEPTED="${ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_ACCEPTED:-0}"
MIN_MTP_ACCEPTANCE_PERMILLE="${ANTFLY_INFERENCE_GEMMA4_MIN_MTP_ACCEPTANCE_PERMILLE:-0}"
MAX_MTP_VERIFY_MS="${ANTFLY_INFERENCE_GEMMA4_MAX_MTP_VERIFY_MS:-0}"
MAX_MTP_MATERIALIZATION_MS="${ANTFLY_INFERENCE_GEMMA4_MAX_MTP_MATERIALIZATION_MS:-0}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-gemma4-e2b-metal-$(date -u +%Y%m%d-%H%M%S)}"

if [[ ! -x "$ANTFLY_BIN" ]]; then
  echo "antfly inference binary not executable: $ANTFLY_BIN" >&2
  echo "build it first: cd zig/pkg/inference && zig build -Doptimize=ReleaseFast -Dmetal=true -Donnx=false -Dpjrt=false" >&2
  exit 2
fi

if [[ ! -e "$MODEL_DIR" ]]; then
  echo "Gemma4 model not found: $MODEL_DIR" >&2
  echo "set ANTFLY_INFERENCE_GEMMA4_MODEL to the local GGUF model directory or file" >&2
  exit 2
fi

if [[ -n "$DRAFT_MODEL" && ! -e "$DRAFT_MODEL" ]]; then
  echo "Gemma4 MTP draft model not found: $DRAFT_MODEL" >&2
  echo "set ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL to a local Gemma4 assistant model" >&2
  exit 2
fi

if [[ -n "$DRAFT_MODEL" && "$SERVER_WARM" != "0" ]]; then
  echo "server warm benchmark does not support MTP draft-model yet" >&2
  exit 2
fi

if [[ -n "$DRAFT_MODEL" ]]; then
  export ANTFLY_GEMMA4_MTP_PROFILE="${ANTFLY_GEMMA4_MTP_PROFILE:-1}"
fi

if [[ "$SERVER_WARM" != "0" && "$MODEL_DIR" != "$MODELS_DIR/$MODEL_NAME" ]]; then
  echo "server warm mode needs MODEL_DIR to match MODELS_DIR/MODEL_NAME" >&2
  echo "set ANTFLY_INFERENCE_GEMMA4_MODELS_DIR and ANTFLY_INFERENCE_GEMMA4_MODEL_NAME for custom paths" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"

run_server_warm_bench() {
  local server_out="$OUT_DIR/server-warm.txt"
  local server_pid=""
  export TERMITE_SERVER_GENERATE_TIMING="${TERMITE_SERVER_GENERATE_TIMING:-1}"
  cleanup_server() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" >/dev/null 2>&1; then
      kill "$server_pid" >/dev/null 2>&1 || true
      wait "$server_pid" >/dev/null 2>&1 || true
    fi
  }
  trap cleanup_server EXIT

  echo "warming server model=$MODEL_NAME port=$SERVER_PORT..." >&2
  (
    cd "$ANTFLY_INFERENCE_ZIG_ROOT"
    run_antfly_inference run \
      --models-dir "$MODELS_DIR" \
      --host 127.0.0.1 \
      --port "$SERVER_PORT" \
      --preload-model "generator:metal:$MODEL_NAME"
  ) >"$server_out" 2>&1 &
  server_pid="$!"

  for _ in $(seq 1 900); do
    if ! kill -0 "$server_pid" >/dev/null 2>&1; then
      cat "$server_out" >&2 || true
      echo "server exited before listening" >&2
      exit 1
    fi
    if grep -q "listening on 127.0.0.1:$SERVER_PORT" "$server_out"; then
      break
    fi
    sleep 0.1
  done
  if ! grep -q "listening on 127.0.0.1:$SERVER_PORT" "$server_out"; then
    cat "$server_out" >&2 || true
    echo "server did not become ready" >&2
    exit 1
  fi

  local idx=0
  for tokens in $SERVER_TOKENS; do
    idx=$((idx + 1))
    local out="$OUT_DIR/server-request-$idx-${tokens}tok.txt"
    echo "requesting warmed server tokens=$tokens..." >&2
    (
      cd "$ANTFLY_INFERENCE_ZIG_ROOT"
      run_antfly_inference generate "$MODEL_DIR" "$PROMPT" \
        --server "http://127.0.0.1:$SERVER_PORT" \
        --backend metal \
        --max-tokens "$tokens" \
        --print-token-count \
        --print-timing \
        --print-finish-reason \
        --require-server
    ) >"$out" 2>&1
  done

  python3 - "$OUT_DIR" "$MIN_SERVER_TOK_S" "$MAX_SERVER_WARM_MS" <<'PY'
import json
import os
import re
import statistics
import sys
from pathlib import Path

TOGGLE_NAMES = [
    "TERMITE_METAL_DISABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ",
    "TERMITE_METAL_ENABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ",
    "TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_SUMSQ_EXPERIMENT",
    "TERMITE_METAL_DISABLE_Q4_0_SPLIT_GATE_UP_REDUCE",
    "TERMITE_METAL_DISABLE_GEMMA_FUSED_QKV",
    "TERMITE_METAL_ENABLE_GEMMA_FUSED_QKV",
    "TERMITE_METAL_ENABLE_Q4_0_F16_FFN",
    "TERMITE_METAL_Q4_0_F16_FFN_EXPERIMENT",
    "ANTFLY_INFERENCE_GEMMA4_ALLOW_UNSAFE_Q4_0_F16_FFN",
    "TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT",
    "TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT_EXPERIMENT",
    "TERMITE_METAL_DISABLE_PLANNED_COMPUTE_BARRIERS",
    "ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO",
    "ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL",
    "ANTFLY_INFERENCE_GEMMA4_RAW_PROMPT",
]

out_dir = Path(sys.argv[1])
min_tok_s = float(sys.argv[2])
max_warm_ms = int(sys.argv[3])
server_log = (out_dir / "server-warm.txt").read_text(encoding="utf-8", errors="replace")

def grab(pattern, text, default=None, cast=int):
    m = re.search(pattern, text)
    if not m:
        return default
    return cast(m.group(1))

timing_rows = [
    {
        "runtime_prepare_ms": int(m.group("runtime_prepare")),
        "prefill_ms": int(m.group("prefill")),
        "decode_ms": int(m.group("decode")),
        "total_ms": int(m.group("total")),
    }
    for m in re.finditer(
        r"generate_timing_ms: prompt_format=\d+ tokenize=\d+ runtime_prepare=(?P<runtime_prepare>\d+) prefill=(?P<prefill>\d+) decode=(?P<decode>\d+) total=(?P<total>\d+)",
        server_log,
    )
]
request_timing_rows = timing_rows[1:] if timing_rows else []

warm = {
    "elapsed_ms": grab(r"warmed inference generator[^\n]*elapsed_ms=(\d+)", server_log),
    "resolve_ms": grab(r"warmed inference generator[^\n]*resolve_ms=(\d+)", server_log, default=0),
    "load_ms": grab(r"warmed inference generator[^\n]*load_ms=(\d+)", server_log, default=0),
    "setup_ms": grab(r"warmed inference generator[^\n]*setup_ms=(\d+)", server_log, default=0),
    "generate_ms": grab(r"warmed inference generator[^\n]*generate_ms=(\d+)", server_log, default=0),
    "runtime_prepare_ms": grab(r"generate_timing_ms:.*\bruntime_prepare=(\d+)", server_log, default=0),
    "prefill_ms": grab(r"generate_timing_ms:.*\bprefill=(\d+)", server_log, default=0),
    "decode_ms": grab(r"generate_timing_ms:.*\bdecode=(\d+)", server_log, default=0),
}
if warm["elapsed_ms"] is None:
    raise SystemExit("missing warm timing in server-warm.txt")

rows = []
for idx, path in enumerate(sorted(out_dir.glob("server-request-*.txt"))):
    text = path.read_text(encoding="utf-8", errors="replace")
    tokens = grab(r"(?:finish_reason=\S+\s+)?tokens=(\d+)", text)
    total_ms = grab(r"timing_ms:.*\bserver_request=(\d+)", text)
    if tokens is None or total_ms is None:
        raise SystemExit(f"missing server request timing fields in {path}")
    inner = request_timing_rows[idx] if idx < len(request_timing_rows) else {}
    rows.append({
        "label": path.stem,
        "tokens": tokens,
        "server_request_ms": total_ms,
        "request_runtime_prepare_ms": inner.get("runtime_prepare_ms", 0),
        "request_prefill_ms": inner.get("prefill_ms", 0),
        "request_decode_ms": inner.get("decode_ms", 0),
        "request_total_ms": inner.get("total_ms", 0),
        "tok_s": tokens / (total_ms / 1000.0) if total_ms else 0.0,
        "decode_tok_s": tokens / (inner.get("decode_ms", 0) / 1000.0) if inner.get("decode_ms", 0) else 0.0,
        "file": str(path),
    })
if not rows:
    raise SystemExit("no server-request files found")

decode_tok_s_values = [r["decode_tok_s"] for r in rows if r["decode_tok_s"] > 0.0]
summary = {
    "warm": warm,
    "median_server_tok_s": statistics.median(r["tok_s"] for r in rows),
    "median_server_decode_tok_s": statistics.median(decode_tok_s_values) if decode_tok_s_values else 0.0,
    "runtime_toggles": {name: os.environ.get(name, "") for name in TOGGLE_NAMES},
    "rows": rows,
}
(out_dir / "server-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
with (out_dir / "server-summary.tsv").open("w", encoding="utf-8") as f:
    f.write("label\ttokens\tserver_request_ms\trequest_runtime_prepare_ms\trequest_prefill_ms\trequest_decode_ms\trequest_total_ms\ttok_s\tdecode_tok_s\twarm_elapsed_ms\twarm_load_ms\twarm_generate_ms\twarm_runtime_prepare_ms\twarm_prefill_ms\twarm_decode_ms\tfile\n")
    for r in rows:
        f.write(
            f"{r['label']}\t{r['tokens']}\t{r['server_request_ms']}\t"
            f"{r['request_runtime_prepare_ms']}\t{r['request_prefill_ms']}\t{r['request_decode_ms']}\t{r['request_total_ms']}\t"
            f"{r['tok_s']:.3f}\t{r['decode_tok_s']:.3f}\t"
            f"{warm['elapsed_ms']}\t{warm['load_ms']}\t{warm['generate_ms']}\t"
            f"{warm['runtime_prepare_ms']}\t{warm['prefill_ms']}\t{warm['decode_ms']}\t{r['file']}\n"
        )

print(f"server summary: {out_dir / 'server-summary.tsv'}")
print(
    "warm_elapsed_ms={elapsed_ms} warm_load_ms={load_ms} warm_generate_ms={generate_ms} "
    "warm_runtime_prepare_ms={runtime_prepare_ms} warm_prefill_ms={prefill_ms} warm_decode_ms={decode_ms}".format(**warm)
)
print(f"median_server_tok_s={summary['median_server_tok_s']:.3f}")
print(f"median_server_decode_tok_s={summary['median_server_decode_tok_s']:.3f}")
if max_warm_ms and warm["elapsed_ms"] > max_warm_ms:
    raise SystemExit(f"warm elapsed {warm['elapsed_ms']}ms above gate {max_warm_ms}ms")
if min_tok_s and summary["median_server_decode_tok_s"] < min_tok_s:
    raise SystemExit(f"median server decode tok/s {summary['median_server_decode_tok_s']:.3f} below gate {min_tok_s:.3f}")
PY

  cleanup_server
  trap - EXIT
  echo "raw output: $OUT_DIR"
}

if [[ "$SERVER_WARM" != "0" ]]; then
  run_server_warm_bench
  exit 0
fi

run_case() {
  local label="$1"
  local tokens="$2"
  local out="$OUT_DIR/${label}.txt"
  local args=(
    generate "$MODEL_DIR" "$PROMPT"
    --backend metal
    --max-tokens "$tokens"
    --print-token-count
    --print-timing
    --print-finish-reason
  )
  if [[ -n "$CACHE_DTYPE" ]]; then
    args+=(--cache-dtype "$CACHE_DTYPE")
  fi
  if [[ "$RAW_PROMPT" != "0" ]]; then
    args+=(--raw-prompt)
  fi
  if [[ -n "$DRAFT_MODEL" ]]; then
    args+=(
      --draft-model "$DRAFT_MODEL"
      --speculative-k "$SPECULATIVE_K"
      --speculation-policy "$SPECULATION_POLICY"
      --speculation-calibration "$SPECULATION_CALIBRATION"
    )
  fi
  echo "running $label tokens=$tokens cache_dtype=${CACHE_DTYPE:-default} reuse_probe=$REUSE_PROBE mtp=$([[ -n "$DRAFT_MODEL" ]] && echo 1 || echo 0)..." >&2
  (
    cd "$ANTFLY_INFERENCE_ZIG_ROOT"
    if [[ "$REUSE_PROBE" != "0" ]]; then
      TERMITE_METAL_EXECUTOR_REUSE_PROBE=1 run_antfly_inference "${args[@]}"
    else
      run_antfly_inference "${args[@]}"
    fi
  ) >"$out" 2>&1
}

run_case warmup "$WARMUP_TOKENS"
for i in $(seq 1 "$RUNS"); do
  run_case "run-$i" "$MAX_TOKENS"
done

python3 - "$OUT_DIR" "$MIN_DECODE_TOK_S" "$MIN_HOT_DECODE_TOK_S" "$MIN_PREFILL_FRAME_EXECUTE" "$MIN_Q4_0_DISPATCH" "$MIN_Q4_0_PAIR_REDUCE" "$MIN_Q4_0_PAIR_ACT_REDUCE" "$MIN_Q4_0_ACTIVATION_RHS_REDUCE" "$MIN_Q4_0_ACTIVATION_RHS_REDUCE_OUT_F16" "$MIN_Q4_0_PAIR_ACT_REDUCE_OUT_F16" "$MIN_Q4_0_PAIR_ACT_RMS_SCALE_REDUCE_OUT_F16" "$MIN_Q4_0_LINEAR_REDUCE_IN_F16" "$MIN_Q4_0_LINEAR_REDUCE_OUT_F16" "$MIN_Q4_0_LINEAR_REDUCE_IN_F16_OUT_F16" "$MIN_Q4_0_LINEAR_REDUCE_SUMSQ" "$MIN_RMS_NORM_ADD_SUMSQ" "$MIN_PAGED_ATTENTION_1X" "$MIN_Q4_PAIR_ACT_REDUCE_OUT_F16" "$MIN_Q6_REDUCE_IN_F16" "$MIN_ACTIVE_DECODE_SUCCESS" "$MIN_ACTIVE_DECODE_FINAL_FUSED_ARGMAX" "$MAX_TOKENS" "$MIN_GENERATED_TOKENS" "$REQUIRE_MAX_TOKENS" "$REQUIRE_MTP_ENABLED" "$MIN_SPECULATIVE_ROUNDS" "$MIN_SPECULATIVE_DRAFTED" "$MIN_SPECULATIVE_ACCEPTED" "$MIN_SPECULATIVE_MATCHED" "$MIN_MTP_ACCEPTANCE_PERMILLE" "$MAX_MTP_VERIFY_MS" "$MAX_MTP_MATERIALIZATION_MS" "$MAX_LAST_COMPUTE_ENCODERS" "$MIN_Q4_0_PLE_ACTIVATION_RHS_REDUCE_OUT_F16" "$MIN_Q4_0_PLE_LINEAR_REDUCE_IN_F16" "$MAX_Q4_0_LINEAR_REDUCE_SUMSQ" "$MAX_RMS_NORM_ADD_SUMSQ" <<'PY'
import json
import os
import re
import statistics
import sys
from pathlib import Path

TOGGLE_NAMES = [
    "TERMITE_METAL_DISABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ",
    "TERMITE_METAL_ENABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ",
    "TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_SUMSQ_EXPERIMENT",
    "TERMITE_METAL_DISABLE_Q4_0_SPLIT_GATE_UP_REDUCE",
    "TERMITE_METAL_DISABLE_GEMMA_FUSED_QKV",
    "TERMITE_METAL_ENABLE_GEMMA_FUSED_QKV",
    "TERMITE_METAL_ENABLE_Q4_0_F16_FFN",
    "TERMITE_METAL_Q4_0_F16_FFN_EXPERIMENT",
    "ANTFLY_INFERENCE_GEMMA4_ALLOW_UNSAFE_Q4_0_F16_FFN",
    "TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT",
    "TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT_EXPERIMENT",
    "TERMITE_METAL_DISABLE_PLANNED_COMPUTE_BARRIERS",
    "ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO",
    "ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL",
    "ANTFLY_INFERENCE_GEMMA4_RAW_PROMPT",
]

out_dir = Path(sys.argv[1])
min_decode = float(sys.argv[2])
min_hot_decode = float(sys.argv[3])
min_prefill_frame_execute = int(sys.argv[4])
min_q4_0_dispatch = int(sys.argv[5])
min_q4_0_pair_reduce = int(sys.argv[6])
min_q4_0_pair_act = int(sys.argv[7])
min_q4_0_activation_rhs = int(sys.argv[8])
min_q4_0_activation_rhs_f16 = int(sys.argv[9])
min_q4_0_pair_act_f16 = int(sys.argv[10])
min_q4_0_pair_act_rms_f16 = int(sys.argv[11])
min_q4_0_linear_f16 = int(sys.argv[12])
min_q4_0_linear_out_f16 = int(sys.argv[13])
min_q4_0_linear_in_f16_out_f16 = int(sys.argv[14])
min_q4_0_linear_sumsq = int(sys.argv[15])
min_rms_norm_add_sumsq = int(sys.argv[16])
min_paged_attention_1x = int(sys.argv[17])
min_q4_pair_act_f16 = int(sys.argv[18])
min_q6_f16 = int(sys.argv[19])
min_active_decode_success = int(sys.argv[20])
min_active_decode_final_fused_argmax = int(sys.argv[21])
target_max_tokens = int(sys.argv[22])
min_generated_tokens = int(sys.argv[23])
require_max_tokens = sys.argv[24] != "0"
require_mtp_enabled = sys.argv[25] != "0"
min_speculative_rounds = int(sys.argv[26])
min_speculative_drafted = int(sys.argv[27])
min_speculative_accepted = int(sys.argv[28])
min_speculative_matched = int(sys.argv[29])
min_mtp_acceptance_permille = int(sys.argv[30])
max_mtp_verify_ms = int(sys.argv[31])
max_mtp_materialization_ms = int(sys.argv[32])
max_last_compute_encoders = int(sys.argv[33])
min_q4_0_ple_activation_rhs_f16 = int(sys.argv[34])
min_q4_0_ple_linear_f16 = int(sys.argv[35])
max_q4_0_linear_sumsq = int(sys.argv[36])
max_rms_norm_add_sumsq = int(sys.argv[37])
rows = []

def grab(pattern, text, default=None, cast=int):
    m = re.search(pattern, text)
    if not m:
        return default
    return cast(m.group(1))

for path in sorted(out_dir.glob("*.txt")):
    text = path.read_text(encoding="utf-8", errors="replace")
    tokens = grab(r"(?:finish_reason=\S+\s+)?tokens=(\d+)", text)
    finish_reason = grab(r"\bfinish_reason=(\S+)", text, default="", cast=str)
    generate_ms = grab(r"timing_ms:.*\bgenerate=(\d+)", text)
    total_ms = grab(r"timing_ms:.*\btotal=(\d+)", text)
    runtime_prewarm_ms = grab(r"timing_ms:.*\bruntime_prewarm=(\d+)", text, default=0)
    first_token_request_ms = grab(r"first_token_ms:.*\brequest=(\d+)", text, default=0)
    first_token_service_ms = grab(r"first_token_ms:.*\bservice=(\d+)", text, default=0)
    first_token_prefill_ms = grab(r"first_token_ms:.*\bprefill=(\d+)", text, default=0)
    first_token_sample_ms = grab(r"first_token_ms:.*\bsample=(\d+)", text, default=0)
    reuse_first_token_service_ms = grab(r"metal_executor_reuse_first_token_ms:.*\bservice=(\d+)", text, default=0)
    reuse_first_token_prefill_ms = grab(r"metal_executor_reuse_first_token_ms:.*\bprefill=(\d+)", text, default=0)
    reuse_first_token_sample_ms = grab(r"metal_executor_reuse_first_token_ms:.*\bsample=(\d+)", text, default=0)
    backend = grab(r"selected backend (\w+)", text, default="", cast=str)
    decode_fallback = grab(r"metal_frame_fallbacks:.*\bdecode_fallback=(\d+)", text, default=0)
    prefill_execute = grab(r"metal_frame_fallbacks:.*\bprefill_execute=(\d+)/", text, default=0)
    prefill_execute_fail = grab(r"metal_frame_fallbacks:.*\bprefill_execute_fail=(\d+)", text, default=0)
    frame_begins = grab(r"metal_decoder_frame:\s+begins=(\d+)", text, default=0)
    frame_wait_ms = grab(r"metal_decoder_frame:.*\bwait_ms=(\d+)", text, default=0)
    frame_gpu_ms = grab(r"metal_decoder_frame:.*\bgpu_ms=(\d+)", text, default=0)
    last_compute_encoders = grab(r"metal_decoder_frame:.*\blast_compute_encoders=(\d+)", text, default=-1)
    last_blit_encoders = grab(r"metal_decoder_frame:.*\blast_blit_encoders=(\d+)", text, default=-1)
    planned_scopes = grab(r"metal_runtime_encoders:.*\bplanned_scopes=(\d+)", text, default=0)
    planned_barriers = grab(r"metal_runtime_encoders:.*\bplanned_barriers=(\d+)", text, default=0)
    q8_mmv = grab(r"metal_q8_0_dispatch:.*\bmmv=(\d+)", text, default=0)
    q8_mm = grab(r"metal_q8_0_dispatch:.*\bmm=(\d+)", text, default=0)
    q4_0_linear_reduce = grab(r"metal_q4_0_dispatch:.*\blinear_reduce=(\d+)", text, default=0)
    q4_0_linear_reduce_in_f16 = grab(r"metal_q4_0_dispatch:.*\blinear_reduce_in_f16=(\d+)", text, default=0)
    q4_0_linear_reduce_out_f16 = grab(r"metal_q4_0_dispatch:.*\blinear_reduce_out_f16=(\d+)", text, default=0)
    q4_0_linear_reduce_in_f16_out_f16 = grab(r"metal_q4_0_dispatch:.*\blinear_reduce_in_f16_out_f16=(\d+)", text, default=0)
    q4_0_linear_reduce_sumsq = grab(r"metal_q4_0_dispatch:.*\blinear_reduce_sumsq=(\d+)", text, default=0)
    q4_0_pair_act_reduce = grab(r"metal_q4_0_dispatch:.*\bpair_act_reduce=(\d+)", text, default=0)
    q4_0_pair_act_reduce_out_f16 = grab(r"metal_q4_0_dispatch:.*\bpair_act_reduce_out_f16=(\d+)", text, default=0)
    q4_0_pair_act_rms_scale_reduce_out_f16 = grab(r"metal_q4_0_dispatch:.*\bpair_act_rms_scale_reduce_out_f16=(\d+)", text, default=0)
    q4_0_activation_rhs_reduce = grab(r"metal_q4_0_dispatch:.*\bactivation_rhs_reduce=(\d+)", text, default=0)
    q4_0_activation_rhs_reduce_out_f16 = grab(r"metal_q4_0_dispatch:.*\bactivation_rhs_reduce_out_f16=(\d+)", text, default=0)
    q4_0_ple_activation_rhs_reduce_out_f16 = grab(r"metal_q4_0_ple_dispatch:.*\bactivation_rhs_reduce_out_f16=(\d+)", text, default=0)
    q4_0_ple_linear_reduce_in_f16 = grab(r"metal_q4_0_ple_dispatch:.*\blinear_reduce_in_f16=(\d+)", text, default=0)
    rms_norm_add_sumsq = grab(r"metal_q4_0_dispatch:.*\brms_norm_add_sumsq=(\d+)", text, default=0)
    paged_attention_1x = grab(r"metal_attention_dispatch:.*\bpaged_1x=(\d+)", text, default=0)
    q4_0_pair_reduce = grab(r"metal_q4_0_dispatch:.*\bpair_reduce=(\d+)", text, default=0)
    q4_0_pair = grab(r"metal_q4_0_dispatch:.*\bpair=(\d+)", text, default=0)
    q4_0_linear_reduce_encode_us = grab(r"metal_q4_0_encode_us:.*\blinear_reduce=(\d+)", text, default=0)
    q4_0_pair_reduce_encode_us = grab(r"metal_q4_0_encode_us:.*\bpair_reduce=(\d+)", text, default=0)
    q4_0_pair_act_reduce_encode_us = grab(r"metal_q4_0_encode_us:.*\bpair_act_reduce=(\d+)", text, default=0)
    q4_0_activation_rhs_reduce_encode_us = grab(r"metal_q4_0_encode_us:.*\bactivation_rhs_reduce=(\d+)", text, default=0)
    q4_linear_reduce = grab(r"metal_q4_q6_k_dispatch:.*\bq4_linear_reduce=(\d+)", text, default=0)
    q4_pair_reduce = grab(r"metal_q4_q6_k_dispatch:.*\bq4_pair_reduce=(\d+)", text, default=0)
    q4_pair_act_reduce = grab(r"metal_q4_q6_k_dispatch:.*\bq4_pair_act_reduce=(\d+)", text, default=0)
    q4_pair_act_reduce_out_f16 = grab(r"metal_q4_q6_k_dispatch:.*\bq4_pair_act_reduce_out_f16=(\d+)", text, default=0)
    q4_activation_rhs_reduce = grab(r"metal_q4_q6_k_dispatch:.*\bq4_activation_rhs_reduce=(\d+)", text, default=0)
    q6_linear_reduce = grab(r"metal_q4_q6_k_dispatch:.*\bq6_linear_reduce=(\d+)", text, default=0)
    q6_linear_reduce_in_f16 = grab(r"metal_q4_q6_k_dispatch:.*\bq6_linear_reduce_in_f16=(\d+)", text, default=0)
    active_decode_layers = grab(r"metal_active_decode_ops:\s+layers=(\d+)", text, default=0)
    active_decode_final_fused_argmax = grab(r"metal_active_decode_ops:.*\bfinal_fused_argmax=(\d+)", text, default=0)
    active_decode_final_split_argmax = grab(r"metal_active_decode_ops:.*\bfinal_split_argmax=(\d+)", text, default=0)
    active_decode_frame_attempts = grab(r"metal_frame_fallbacks:\s+decode_attempts=(\d+)", text, default=0)
    active_decode_frame_success = grab(r"metal_frame_fallbacks:.*\bdecode_success=(\d+)", text, default=0)
    command_ops = grab(r"metal_runtime_command_ops:\s+total=(\d+)", text, default=0)
    command_op_attention = grab(r"metal_runtime_command_ops:.*\battention=(\d+)", text, default=0)
    command_op_ffn_pre_norm_scale = grab(r"metal_runtime_command_ops:.*\bffn_pre_norm_scale=(\d+)", text, default=0)
    command_op_ffn_gate_up_activation = grab(r"metal_runtime_command_ops:.*\bffn_gate_up_activation=(\d+)", text, default=0)
    command_op_ple_projection = grab(r"metal_runtime_command_ops:.*\bple_projection=(\d+)", text, default=0)
    command_op_ple_post_norm_residual = grab(r"metal_runtime_command_ops:.*\bple_post_norm_residual=(\d+)", text, default=0)
    command_op_tail_lm_head = grab(r"metal_runtime_command_ops:.*\btail_lm_head=(\d+)", text, default=0)
    command_operator_mul_mv = grab(r"metal_runtime_command_operators:.*\bmul_mv=(\d+)", text, default=0)
    command_operator_mul_mm = grab(r"metal_runtime_command_operators:.*\bmul_mm=(\d+)", text, default=0)
    command_operator_attention_paged = grab(r"metal_runtime_command_operators:.*\battention_paged=(\d+)", text, default=0)
    runtime_region_attention_project = grab(r"metal_runtime_compute_regions:.*\battention_project=(\d+)", text, default=0)
    runtime_region_ffn = grab(r"metal_runtime_compute_regions:.*\bffn=(\d+)", text, default=0)
    runtime_region_ple = grab(r"metal_runtime_compute_regions:.*\bple=(\d+)", text, default=0)
    runtime_region_embedding = grab(r"metal_runtime_compute_regions:.*\bembedding=(\d+)", text, default=0)
    runtime_region_layer = grab(r"metal_runtime_compute_regions:.*\blayer=(\d+)", text, default=0)
    quant_block_apply_ms = grab(r"metal_quant_block_apply_ms:.*\btotal=(\d+)", text, default=0)
    quant_block_attention_span_ms = grab(r"metal_quant_block_apply_ms:.*\battention_span=(\d+)", text, default=0)
    quant_block_attention_prefix_ms = grab(r"metal_quant_block_apply_ms:.*\battention_prefix=(\d+)", text, default=0)
    quant_block_gated_ffn_ms = grab(r"metal_quant_block_apply_ms:.*\bgated_ffn=(\d+)", text, default=0)
    quant_block_command_wait_ms = grab(r"metal_quant_block_apply_ms:.*\bcommand_wait=(\d+)", text, default=0)
    quant_block_gpu_ms = grab(r"metal_quant_block_apply_ms:.*\bgpu=(\d+)", text, default=0)
    greedy_calls = grab(r"metal_executor_ms:.*\bgreedy_calls=(\d+)", text, default=0)
    greedy_direct_ms = grab(r"metal_executor_ms:.*\bgreedy_direct=(\d+)", text, default=0)
    greedy_layer_specs_ms = grab(r"decoder_gated_decode_ms:.*\bgreedy_layer_specs=(\d+)", text, default=0)
    prefill_direct_family_ms = grab(r"metal_executor_ms:.*\bprefill_direct_family=(\d+)", text, default=0)
    prefill_tokens = grab(r"decoder_gated_prefill_ops:.*\btokens=(\d+)", text, default=0)
    ple_prepare_ms = grab(r"decoder_gated_prefill_ms:.*\bple_prepare=(\d+)", text, default=0)
    quant_private_ms = grab(r"metal_quant_runtime_prepare:.*\bprivate_ms=(\d+)", text, default=0)
    quant_private_slots = grab(r"metal_quant_runtime_prepare:\s+private_slots=(\d+)", text, default=0)
    quant_mapped_slots = grab(r"metal_quant_runtime_prepare:.*\bmapped_slots=(\d+)", text, default=0)
    quant_mapped_failures = grab(r"metal_quant_runtime_prepare:.*\bmapped_failures=(\d+)", text, default=0)
    speculative_policy = grab(r"(?m)^speculative:\s+policy=(\S+)", text, default="", cast=str)
    speculative_decision = grab(r"(?m)^speculative:.*\bdecision=(\S+)", text, default="", cast=str)
    speculative_rounds = grab(r"(?m)^speculative:.*\brounds=(\d+)", text, default=0)
    speculative_drafted = grab(r"(?m)^speculative:.*\bdrafted=(\d+)", text, default=0)
    speculative_matched = grab(r"(?m)^speculative:.*\bmatched=(\d+)", text, default=0)
    speculative_accepted = grab(r"(?m)^speculative:.*\baccepted=(\d+)", text, default=0)
    speculative_mtp_enabled = grab(r"(?m)^speculative:.*\bmtp_enabled=(true|false)", text, default="false", cast=str) == "true"
    speculative_acceptance_permille = grab(r"(?m)^speculative:.*\bmtp_acceptance_permille=(\d+)", text, default=0)
    mtp_draft_steps = grab(r"(?m)^mtp_profile:.*\bdraft_steps=(\d+)", text, default=0)
    mtp_resident_draft_steps = grab(r"(?m)^mtp_profile:.*\bresident_draft_steps=(\d+)", text, default=0)
    mtp_host_draft_steps = grab(r"(?m)^mtp_profile:.*\bhost_draft_steps=(\d+)", text, default=0)
    mtp_target_verify_calls = grab(r"(?m)^mtp_profile:.*\btarget_verify_calls=(\d+)", text, default=0)
    mtp_dedicated_runtime_hits = grab(r"(?m)^mtp_profile:.*\bdedicated_runtime_hits=(\d+)", text, default=0)
    mtp_dedicated_runtime_fallbacks = grab(r"(?m)^mtp_profile:.*\bdedicated_runtime_fallbacks=(\d+)", text, default=0)
    mtp_device_verify_commit_hits = grab(r"(?m)^mtp_profile:.*\bdevice_verify_commit_hits=(\d+)", text, default=0)
    mtp_commit_forwards_required = grab(r"(?m)^mtp_profile:.*\bcommit_forwards_required=(\d+)", text, default=0)
    mtp_commit_forwards_avoided = grab(r"(?m)^mtp_profile:.*\bcommit_forwards_avoided=(\d+)", text, default=0)
    mtp_materializations = grab(r"(?m)^mtp_profile:.*\bmaterializations=(\d+)", text, default=0)
    mtp_draft_ms = grab(r"(?m)^mtp_profile:.*\bdraft_ms=(\d+)", text, default=0)
    mtp_verify_ms = grab(r"(?m)^mtp_profile:.*\bverify_ms=(\d+)", text, default=0)
    mtp_materialization_ms = grab(r"(?m)^mtp_profile:.*\bmaterialization_ms=(\d+)", text, default=0)
    if tokens is None or generate_ms is None or total_ms is None:
        raise SystemExit(f"missing timing fields in {path}")
    decode_tok_s = grab(r"(?m)^decode_tok_per_s=([0-9.]+)", text, default=None, cast=float)
    if decode_tok_s is None:
        decode_tok_s = tokens / (generate_ms / 1000.0) if generate_ms else 0.0
    e2e_tok_s = tokens / (total_ms / 1000.0) if total_ms else 0.0
    hot_decode_tok_s = greedy_calls / (greedy_direct_ms / 1000.0) if greedy_calls and greedy_direct_ms else 0.0
    prefill_tok_s = prefill_tokens / (prefill_direct_family_ms / 1000.0) if prefill_tokens and prefill_direct_family_ms else 0.0
    timing_invalid_reasons = []
    max_internal_ms = total_ms * 3
    if total_ms and greedy_direct_ms > max_internal_ms:
        timing_invalid_reasons.append(f"greedy_direct_ms={greedy_direct_ms}>3x_total_ms={max_internal_ms}")
    if total_ms and frame_wait_ms > max_internal_ms:
        timing_invalid_reasons.append(f"frame_wait_ms={frame_wait_ms}>3x_total_ms={max_internal_ms}")
    if total_ms and prefill_direct_family_ms > max_internal_ms:
        timing_invalid_reasons.append(f"prefill_direct_family_ms={prefill_direct_family_ms}>3x_total_ms={max_internal_ms}")
    rows.append({
        "label": path.stem,
        "tokens": tokens,
        "finish_reason": finish_reason,
        "generate_ms": generate_ms,
        "total_ms": total_ms,
        "runtime_prewarm_ms": runtime_prewarm_ms,
        "first_token_request_ms": first_token_request_ms,
        "first_token_service_ms": first_token_service_ms,
        "first_token_prefill_ms": first_token_prefill_ms,
        "first_token_sample_ms": first_token_sample_ms,
        "reuse_first_token_service_ms": reuse_first_token_service_ms,
        "reuse_first_token_prefill_ms": reuse_first_token_prefill_ms,
        "reuse_first_token_sample_ms": reuse_first_token_sample_ms,
        "decode_tok_s": decode_tok_s,
        "e2e_tok_s": e2e_tok_s,
        "backend": backend,
        "decode_fallback": decode_fallback,
        "prefill_execute": prefill_execute,
        "prefill_execute_fail": prefill_execute_fail,
        "frame_begins": frame_begins,
        "frame_wait_ms": frame_wait_ms,
        "frame_gpu_ms": frame_gpu_ms,
        "last_compute_encoders": last_compute_encoders,
        "last_blit_encoders": last_blit_encoders,
        "planned_scopes": planned_scopes,
        "planned_barriers": planned_barriers,
        "q8_mmv": q8_mmv,
        "q8_mm": q8_mm,
        "q4_0_linear_reduce": q4_0_linear_reduce,
        "q4_0_linear_reduce_in_f16": q4_0_linear_reduce_in_f16,
        "q4_0_linear_reduce_out_f16": q4_0_linear_reduce_out_f16,
        "q4_0_linear_reduce_in_f16_out_f16": q4_0_linear_reduce_in_f16_out_f16,
        "q4_0_linear_reduce_sumsq": q4_0_linear_reduce_sumsq,
        "q4_0_pair_act_reduce": q4_0_pair_act_reduce,
        "q4_0_pair_act_reduce_out_f16": q4_0_pair_act_reduce_out_f16,
        "q4_0_pair_act_rms_scale_reduce_out_f16": q4_0_pair_act_rms_scale_reduce_out_f16,
        "q4_0_activation_rhs_reduce": q4_0_activation_rhs_reduce,
        "q4_0_activation_rhs_reduce_out_f16": q4_0_activation_rhs_reduce_out_f16,
        "q4_0_ple_activation_rhs_reduce_out_f16": q4_0_ple_activation_rhs_reduce_out_f16,
        "q4_0_ple_linear_reduce_in_f16": q4_0_ple_linear_reduce_in_f16,
        "rms_norm_add_sumsq": rms_norm_add_sumsq,
        "paged_attention_1x": paged_attention_1x,
        "q4_0_pair_reduce": q4_0_pair_reduce,
        "q4_0_pair": q4_0_pair,
        "q4_0_linear_reduce_encode_us": q4_0_linear_reduce_encode_us,
        "q4_0_pair_reduce_encode_us": q4_0_pair_reduce_encode_us,
        "q4_0_pair_act_reduce_encode_us": q4_0_pair_act_reduce_encode_us,
        "q4_0_activation_rhs_reduce_encode_us": q4_0_activation_rhs_reduce_encode_us,
        "q4_linear_reduce": q4_linear_reduce,
        "q4_pair_reduce": q4_pair_reduce,
        "q4_pair_act_reduce": q4_pair_act_reduce,
        "q4_pair_act_reduce_out_f16": q4_pair_act_reduce_out_f16,
        "q4_activation_rhs_reduce": q4_activation_rhs_reduce,
        "q6_linear_reduce": q6_linear_reduce,
        "q6_linear_reduce_in_f16": q6_linear_reduce_in_f16,
        "active_decode_layers": active_decode_layers,
        "active_decode_final_fused_argmax": active_decode_final_fused_argmax,
        "active_decode_final_split_argmax": active_decode_final_split_argmax,
        "active_decode_frame_attempts": active_decode_frame_attempts,
        "active_decode_frame_success": active_decode_frame_success,
        "command_ops": command_ops,
        "command_op_attention": command_op_attention,
        "command_op_ffn_pre_norm_scale": command_op_ffn_pre_norm_scale,
        "command_op_ffn_gate_up_activation": command_op_ffn_gate_up_activation,
        "command_op_ple_projection": command_op_ple_projection,
        "command_op_ple_post_norm_residual": command_op_ple_post_norm_residual,
        "command_op_tail_lm_head": command_op_tail_lm_head,
        "command_operator_mul_mv": command_operator_mul_mv,
        "command_operator_mul_mm": command_operator_mul_mm,
        "command_operator_attention_paged": command_operator_attention_paged,
        "runtime_region_attention_project": runtime_region_attention_project,
        "runtime_region_ffn": runtime_region_ffn,
        "runtime_region_ple": runtime_region_ple,
        "runtime_region_embedding": runtime_region_embedding,
        "runtime_region_layer": runtime_region_layer,
        "quant_block_apply_ms": quant_block_apply_ms,
        "quant_block_attention_span_ms": quant_block_attention_span_ms,
        "quant_block_attention_prefix_ms": quant_block_attention_prefix_ms,
        "quant_block_gated_ffn_ms": quant_block_gated_ffn_ms,
        "quant_block_command_wait_ms": quant_block_command_wait_ms,
        "quant_block_gpu_ms": quant_block_gpu_ms,
        "greedy_calls": greedy_calls,
        "greedy_direct_ms": greedy_direct_ms,
        "hot_decode_tok_s": hot_decode_tok_s,
        "greedy_layer_specs_ms": greedy_layer_specs_ms,
        "prefill_direct_family_ms": prefill_direct_family_ms,
        "prefill_tokens": prefill_tokens,
        "prefill_tok_s": prefill_tok_s,
        "ple_prepare_ms": ple_prepare_ms,
        "quant_private_ms": quant_private_ms,
        "quant_private_slots": quant_private_slots,
        "quant_mapped_slots": quant_mapped_slots,
        "quant_mapped_failures": quant_mapped_failures,
        "speculative_policy": speculative_policy,
        "speculative_decision": speculative_decision,
        "speculative_rounds": speculative_rounds,
        "speculative_drafted": speculative_drafted,
        "speculative_matched": speculative_matched,
        "speculative_accepted": speculative_accepted,
        "speculative_mtp_enabled": speculative_mtp_enabled,
        "speculative_acceptance_permille": speculative_acceptance_permille,
        "mtp_draft_steps": mtp_draft_steps,
        "mtp_resident_draft_steps": mtp_resident_draft_steps,
        "mtp_host_draft_steps": mtp_host_draft_steps,
        "mtp_target_verify_calls": mtp_target_verify_calls,
        "mtp_dedicated_runtime_hits": mtp_dedicated_runtime_hits,
        "mtp_dedicated_runtime_fallbacks": mtp_dedicated_runtime_fallbacks,
        "mtp_device_verify_commit_hits": mtp_device_verify_commit_hits,
        "mtp_commit_forwards_required": mtp_commit_forwards_required,
        "mtp_commit_forwards_avoided": mtp_commit_forwards_avoided,
        "mtp_materializations": mtp_materializations,
        "mtp_draft_ms": mtp_draft_ms,
        "mtp_verify_ms": mtp_verify_ms,
        "mtp_materialization_ms": mtp_materialization_ms,
        "timing_valid": not timing_invalid_reasons,
        "timing_invalid_reason": ";".join(timing_invalid_reasons),
        "file": str(path),
    })

measured = [r for r in rows if r["label"].startswith("run-")]
if not measured:
    raise SystemExit("no measured run-* files found")
invalid_timings = [r for r in measured if not r["timing_valid"]]
valid_measured = [r for r in measured if r["timing_valid"]]
if not valid_measured:
    raise SystemExit("no valid measured run-* timing rows found")
median_decode = statistics.median(r["decode_tok_s"] for r in valid_measured)
mean_decode = statistics.mean(r["decode_tok_s"] for r in valid_measured)
median_e2e = statistics.median(r["e2e_tok_s"] for r in valid_measured)
median_hot_decode = statistics.median(r["hot_decode_tok_s"] for r in valid_measured)
mean_hot_decode = statistics.mean(r["hot_decode_tok_s"] for r in valid_measured)
summary = {
    "median_decode_tok_s": median_decode,
    "mean_decode_tok_s": mean_decode,
    "median_e2e_tok_s": median_e2e,
    "median_hot_decode_tok_s": median_hot_decode,
    "mean_hot_decode_tok_s": mean_hot_decode,
    "min_decode_tok_s": min_decode,
    "min_hot_decode_tok_s": min_hot_decode,
    "target_max_tokens": target_max_tokens,
    "min_generated_tokens": min_generated_tokens,
    "require_max_tokens": require_max_tokens,
    "min_prefill_frame_execute": min_prefill_frame_execute,
    "min_q4_0_dispatch": min_q4_0_dispatch,
    "min_q4_0_pair_reduce": min_q4_0_pair_reduce,
    "min_q4_0_pair_act_reduce": min_q4_0_pair_act,
    "min_q4_0_activation_rhs_reduce": min_q4_0_activation_rhs,
    "min_q4_0_activation_rhs_reduce_out_f16": min_q4_0_activation_rhs_f16,
    "min_q4_0_ple_activation_rhs_reduce_out_f16": min_q4_0_ple_activation_rhs_f16,
    "min_q4_0_ple_linear_reduce_in_f16": min_q4_0_ple_linear_f16,
    "min_q4_0_pair_act_reduce_out_f16": min_q4_0_pair_act_f16,
    "min_q4_0_pair_act_rms_scale_reduce_out_f16": min_q4_0_pair_act_rms_f16,
    "min_q4_0_linear_reduce_in_f16": min_q4_0_linear_f16,
    "min_q4_0_linear_reduce_out_f16": min_q4_0_linear_out_f16,
    "min_q4_0_linear_reduce_in_f16_out_f16": min_q4_0_linear_in_f16_out_f16,
    "min_q4_0_linear_reduce_sumsq": min_q4_0_linear_sumsq,
    "min_rms_norm_add_sumsq": min_rms_norm_add_sumsq,
    "min_paged_attention_1x": min_paged_attention_1x,
    "min_q4_pair_act_reduce_out_f16": min_q4_pair_act_f16,
    "min_q6_reduce_in_f16": min_q6_f16,
    "min_active_decode_success": min_active_decode_success,
    "min_active_decode_final_fused_argmax": min_active_decode_final_fused_argmax,
    "require_mtp_enabled": require_mtp_enabled,
    "min_speculative_rounds": min_speculative_rounds,
    "min_speculative_drafted": min_speculative_drafted,
    "min_speculative_accepted": min_speculative_accepted,
    "min_speculative_matched": min_speculative_matched,
    "min_mtp_acceptance_permille": min_mtp_acceptance_permille,
    "max_mtp_verify_ms": max_mtp_verify_ms,
    "max_mtp_materialization_ms": max_mtp_materialization_ms,
    "max_last_compute_encoders": max_last_compute_encoders,
    "max_q4_0_linear_reduce_sumsq": max_q4_0_linear_sumsq,
    "max_rms_norm_add_sumsq": max_rms_norm_add_sumsq,
    "invalid_timing_rows": [
        {"label": r["label"], "reason": r["timing_invalid_reason"], "file": r["file"]}
        for r in invalid_timings
    ],
    "runtime_toggles": {name: os.environ.get(name, "") for name in TOGGLE_NAMES},
    "rows": rows,
}
(out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
with (out_dir / "summary.tsv").open("w", encoding="utf-8") as f:
    f.write("label\ttokens\tfinish_reason\tgenerate_ms\ttotal_ms\truntime_prewarm_ms\tfirst_token_request_ms\tfirst_token_service_ms\tfirst_token_prefill_ms\tfirst_token_sample_ms\treuse_first_token_service_ms\treuse_first_token_prefill_ms\treuse_first_token_sample_ms\tdecode_tok_s\te2e_tok_s\thot_decode_tok_s\tprefill_tokens\tprefill_tok_s\tbackend\tdecode_fallback\tprefill_execute\tprefill_execute_fail\tframe_begins\tframe_wait_ms\tframe_gpu_ms\tlast_compute_encoders\tlast_blit_encoders\tplanned_scopes\tplanned_barriers\tq8_mmv\tq8_mm\tq4_0_linear_reduce\tq4_0_linear_reduce_in_f16\tq4_0_linear_reduce_out_f16\tq4_0_linear_reduce_in_f16_out_f16\tq4_0_linear_reduce_sumsq\tq4_0_pair_act_reduce\tq4_0_pair_act_reduce_out_f16\tq4_0_pair_act_rms_scale_reduce_out_f16\tq4_0_activation_rhs_reduce\tq4_0_activation_rhs_reduce_out_f16\tq4_0_ple_activation_rhs_reduce_out_f16\tq4_0_ple_linear_reduce_in_f16\trms_norm_add_sumsq\tpaged_attention_1x\tq4_0_pair_reduce\tq4_0_pair\tq4_0_linear_reduce_encode_us\tq4_0_pair_reduce_encode_us\tq4_0_pair_act_reduce_encode_us\tq4_0_activation_rhs_reduce_encode_us\tq4_linear_reduce\tq4_pair_reduce\tq4_pair_act_reduce\tq4_pair_act_reduce_out_f16\tq4_activation_rhs_reduce\tq6_linear_reduce\tq6_linear_reduce_in_f16\tactive_decode_layers\tactive_decode_final_fused_argmax\tactive_decode_final_split_argmax\tactive_decode_frame_attempts\tactive_decode_frame_success\tcommand_ops\tcommand_op_attention\tcommand_op_ffn_pre_norm_scale\tcommand_op_ffn_gate_up_activation\tcommand_op_ple_projection\tcommand_op_ple_post_norm_residual\tcommand_op_tail_lm_head\tcommand_operator_mul_mv\tcommand_operator_mul_mm\tcommand_operator_attention_paged\truntime_region_attention_project\truntime_region_ffn\truntime_region_ple\truntime_region_embedding\truntime_region_layer\tquant_block_apply_ms\tquant_block_attention_span_ms\tquant_block_attention_prefix_ms\tquant_block_gated_ffn_ms\tquant_block_command_wait_ms\tquant_block_gpu_ms\tgreedy_calls\tgreedy_direct_ms\tgreedy_layer_specs_ms\tprefill_direct_family_ms\tple_prepare_ms\tquant_private_ms\tquant_private_slots\tquant_mapped_slots\tquant_mapped_failures\tspeculative_policy\tspeculative_decision\tspeculative_rounds\tspeculative_drafted\tspeculative_matched\tspeculative_accepted\tspeculative_mtp_enabled\tspeculative_acceptance_permille\tmtp_draft_steps\tmtp_resident_draft_steps\tmtp_host_draft_steps\tmtp_target_verify_calls\tmtp_dedicated_runtime_hits\tmtp_dedicated_runtime_fallbacks\tmtp_device_verify_commit_hits\tmtp_commit_forwards_required\tmtp_commit_forwards_avoided\tmtp_materializations\tmtp_draft_ms\tmtp_verify_ms\tmtp_materialization_ms\ttiming_valid\ttiming_invalid_reason\tfile\n")
    for r in rows:
        f.write(
            f"{r['label']}\t{r['tokens']}\t{r['finish_reason']}\t{r['generate_ms']}\t{r['total_ms']}\t{r['runtime_prewarm_ms']}\t"
            f"{r['first_token_request_ms']}\t{r['first_token_service_ms']}\t{r['first_token_prefill_ms']}\t"
            f"{r['first_token_sample_ms']}\t{r['reuse_first_token_service_ms']}\t"
            f"{r['reuse_first_token_prefill_ms']}\t{r['reuse_first_token_sample_ms']}\t"
            f"{r['decode_tok_s']:.3f}\t{r['e2e_tok_s']:.3f}\t{r['hot_decode_tok_s']:.3f}\t"
            f"{r['prefill_tokens']}\t{r['prefill_tok_s']:.3f}\t{r['backend']}\t"
            f"{r['decode_fallback']}\t{r['prefill_execute']}\t{r['prefill_execute_fail']}\t"
            f"{r['frame_begins']}\t{r['frame_wait_ms']}\t"
            f"{r['frame_gpu_ms']}\t{r['last_compute_encoders']}\t{r['last_blit_encoders']}\t{r['planned_scopes']}\t{r['planned_barriers']}\t{r['q8_mmv']}\t{r['q8_mm']}\t"
            f"{r['q4_0_linear_reduce']}\t{r['q4_0_linear_reduce_in_f16']}\t{r['q4_0_linear_reduce_out_f16']}\t{r['q4_0_linear_reduce_in_f16_out_f16']}\t{r['q4_0_linear_reduce_sumsq']}\t{r['q4_0_pair_act_reduce']}\t{r['q4_0_pair_act_reduce_out_f16']}\t{r['q4_0_pair_act_rms_scale_reduce_out_f16']}\t{r['q4_0_activation_rhs_reduce']}\t{r['q4_0_activation_rhs_reduce_out_f16']}\t{r['q4_0_ple_activation_rhs_reduce_out_f16']}\t{r['q4_0_ple_linear_reduce_in_f16']}\t{r['rms_norm_add_sumsq']}\t{r['paged_attention_1x']}\t{r['q4_0_pair_reduce']}\t{r['q4_0_pair']}\t"
            f"{r['q4_0_linear_reduce_encode_us']}\t{r['q4_0_pair_reduce_encode_us']}\t{r['q4_0_pair_act_reduce_encode_us']}\t{r['q4_0_activation_rhs_reduce_encode_us']}\t"
            f"{r['q4_linear_reduce']}\t{r['q4_pair_reduce']}\t"
            f"{r['q4_pair_act_reduce']}\t{r['q4_pair_act_reduce_out_f16']}\t"
            f"{r['q4_activation_rhs_reduce']}\t{r['q6_linear_reduce']}\t"
            f"{r['q6_linear_reduce_in_f16']}\t{r['active_decode_layers']}\t"
            f"{r['active_decode_final_fused_argmax']}\t{r['active_decode_final_split_argmax']}\t"
            f"{r['active_decode_frame_attempts']}\t{r['active_decode_frame_success']}\t{r['command_ops']}\t"
            f"{r['command_op_attention']}\t{r['command_op_ffn_pre_norm_scale']}\t"
            f"{r['command_op_ffn_gate_up_activation']}\t{r['command_op_ple_projection']}\t"
            f"{r['command_op_ple_post_norm_residual']}\t{r['command_op_tail_lm_head']}\t"
            f"{r['command_operator_mul_mv']}\t{r['command_operator_mul_mm']}\t"
            f"{r['command_operator_attention_paged']}\t"
            f"{r['runtime_region_attention_project']}\t{r['runtime_region_ffn']}\t"
            f"{r['runtime_region_ple']}\t{r['runtime_region_embedding']}\t{r['runtime_region_layer']}\t"
            f"{r['quant_block_apply_ms']}\t{r['quant_block_attention_span_ms']}\t{r['quant_block_attention_prefix_ms']}\t"
            f"{r['quant_block_gated_ffn_ms']}\t{r['quant_block_command_wait_ms']}\t{r['quant_block_gpu_ms']}\t"
            f"{r['greedy_calls']}\t{r['greedy_direct_ms']}\t{r['greedy_layer_specs_ms']}\t"
            f"{r['prefill_direct_family_ms']}\t{r['ple_prepare_ms']}\t{r['quant_private_ms']}\t"
            f"{r['quant_private_slots']}\t{r['quant_mapped_slots']}\t{r['quant_mapped_failures']}\t"
            f"{r['speculative_policy']}\t{r['speculative_decision']}\t{r['speculative_rounds']}\t"
            f"{r['speculative_drafted']}\t{r['speculative_matched']}\t{r['speculative_accepted']}\t"
            f"{str(r['speculative_mtp_enabled']).lower()}\t{r['speculative_acceptance_permille']}\t"
            f"{r['mtp_draft_steps']}\t{r['mtp_resident_draft_steps']}\t{r['mtp_host_draft_steps']}\t"
            f"{r['mtp_target_verify_calls']}\t{r['mtp_dedicated_runtime_hits']}\t{r['mtp_dedicated_runtime_fallbacks']}\t"
            f"{r['mtp_device_verify_commit_hits']}\t{r['mtp_commit_forwards_required']}\t"
            f"{r['mtp_commit_forwards_avoided']}\t{r['mtp_materializations']}\t"
            f"{r['mtp_draft_ms']}\t{r['mtp_verify_ms']}\t{r['mtp_materialization_ms']}\t"
            f"{str(r['timing_valid']).lower()}\t{r['timing_invalid_reason']}\t"
            f"{r['file']}\n"
        )

bad_backend = [r for r in measured if r["backend"] != "metal"]
fallbacks = [r for r in measured if r["decode_fallback"] != 0]
prefill_failures = [r for r in measured if r["prefill_execute_fail"] != 0]
missing_prefill_execute = [r for r in measured if r["prefill_execute"] < min_prefill_frame_execute]
mapped_failures = [r for r in measured if r["quant_mapped_failures"] != 0]
short_generations = [r for r in measured if r["tokens"] < min_generated_tokens]
not_max_tokens = [r for r in measured if r["tokens"] < target_max_tokens]
missing_q4_0 = [r for r in measured if r["q4_0_linear_reduce"] + r["q4_0_linear_reduce_in_f16"] + r["q4_0_linear_reduce_out_f16"] + r["q4_0_linear_reduce_in_f16_out_f16"] + r["q4_0_linear_reduce_sumsq"] + r["q4_0_pair_act_reduce"] + r["q4_0_pair_act_reduce_out_f16"] + r["q4_0_pair_act_rms_scale_reduce_out_f16"] + r["q4_0_activation_rhs_reduce"] + r["q4_0_pair_reduce"] + r["q4_0_pair"] < min_q4_0_dispatch]
missing_q4_0_pair_reduce = [r for r in measured if r["q4_0_pair_reduce"] < min_q4_0_pair_reduce]
missing_q4_0_pair_act = [r for r in measured if r["q4_0_pair_act_reduce"] < min_q4_0_pair_act]
missing_q4_0_activation_rhs = [r for r in measured if r["q4_0_activation_rhs_reduce"] < min_q4_0_activation_rhs]
missing_q4_0_activation_rhs_f16 = [r for r in measured if r["q4_0_activation_rhs_reduce_out_f16"] < min_q4_0_activation_rhs_f16]
missing_q4_0_ple_activation_rhs_f16 = [r for r in measured if r["q4_0_ple_activation_rhs_reduce_out_f16"] < min_q4_0_ple_activation_rhs_f16]
missing_q4_0_ple_linear_f16 = [r for r in measured if r["q4_0_ple_linear_reduce_in_f16"] < min_q4_0_ple_linear_f16]
missing_q4_0_f16 = [r for r in measured if r["q4_0_pair_act_reduce_out_f16"] < min_q4_0_pair_act_f16]
missing_q4_0_rms_f16 = [r for r in measured if r["q4_0_pair_act_rms_scale_reduce_out_f16"] < min_q4_0_pair_act_rms_f16]
missing_q4_0_linear_f16 = [r for r in measured if r["q4_0_linear_reduce_in_f16"] < min_q4_0_linear_f16]
missing_q4_0_linear_out_f16 = [r for r in measured if r["q4_0_linear_reduce_out_f16"] < min_q4_0_linear_out_f16]
missing_q4_0_linear_in_f16_out_f16 = [r for r in measured if r["q4_0_linear_reduce_in_f16_out_f16"] < min_q4_0_linear_in_f16_out_f16]
missing_q4_0_linear_sumsq = [r for r in measured if r["q4_0_linear_reduce_sumsq"] < min_q4_0_linear_sumsq]
missing_rms_norm_add_sumsq = [r for r in measured if r["rms_norm_add_sumsq"] < min_rms_norm_add_sumsq]
excess_q4_0_linear_sumsq = [r for r in measured if max_q4_0_linear_sumsq >= 0 and r["q4_0_linear_reduce_sumsq"] > max_q4_0_linear_sumsq]
excess_rms_norm_add_sumsq = [r for r in measured if max_rms_norm_add_sumsq >= 0 and r["rms_norm_add_sumsq"] > max_rms_norm_add_sumsq]
missing_paged_attention_1x = [r for r in measured if r["paged_attention_1x"] < min_paged_attention_1x]
missing_q4_f16 = [r for r in measured if r["q4_pair_act_reduce_out_f16"] < min_q4_pair_act_f16]
missing_q6_f16 = [r for r in measured if r["q6_linear_reduce_in_f16"] < min_q6_f16]
missing_active_decode_success = [r for r in measured if r["active_decode_frame_success"] < min_active_decode_success]
missing_active_decode_final_fused_argmax = [r for r in measured if r["active_decode_final_fused_argmax"] < min_active_decode_final_fused_argmax]
missing_mtp = [r for r in measured if not r["speculative_mtp_enabled"]]
missing_speculative_rounds = [r for r in measured if r["speculative_rounds"] < min_speculative_rounds]
missing_speculative_drafted = [r for r in measured if r["speculative_drafted"] < min_speculative_drafted]
missing_speculative_accepted = [r for r in measured if r["speculative_accepted"] < min_speculative_accepted]
missing_speculative_matched = [r for r in measured if r["speculative_matched"] < min_speculative_matched]
missing_mtp_acceptance = [r for r in measured if r["speculative_acceptance_permille"] < min_mtp_acceptance_permille]
slow_mtp_verify = [r for r in measured if max_mtp_verify_ms and r["mtp_verify_ms"] > max_mtp_verify_ms]
slow_mtp_materialization = [r for r in measured if max_mtp_materialization_ms and r["mtp_materialization_ms"] > max_mtp_materialization_ms]
last_compute_encoder_regressions = [
    r for r in measured
    if max_last_compute_encoders and (r["last_compute_encoders"] < 0 or r["last_compute_encoders"] > max_last_compute_encoders)
]
print(f"summary: {out_dir / 'summary.tsv'}")
print(f"median_decode_tok_s={median_decode:.3f} mean_decode_tok_s={mean_decode:.3f} median_e2e_tok_s={median_e2e:.3f}")
print(f"median_hot_decode_tok_s={median_hot_decode:.3f} mean_hot_decode_tok_s={mean_hot_decode:.3f}")
graph_row = valid_measured[-1]
print(
    "last_frame_graph="
    f"command_ops={graph_row['command_ops']} "
    f"regions(attention_project={graph_row['runtime_region_attention_project']},ffn={graph_row['runtime_region_ffn']},ple={graph_row['runtime_region_ple']},layer={graph_row['runtime_region_layer']}) "
    f"operators(mul_mv={graph_row['command_operator_mul_mv']},mul_mm={graph_row['command_operator_mul_mm']},attention_paged={graph_row['command_operator_attention_paged']})"
)
if invalid_timings:
    raise SystemExit(f"invalid timing counters in measured runs: {[(r['label'], r['timing_invalid_reason']) for r in invalid_timings]}")
if any(r["speculative_policy"] for r in measured):
    print("speculative_mtp=" + ",".join(
        f"{r['label']}:enabled={str(r['speculative_mtp_enabled']).lower()}:decision={r['speculative_decision']}:accepted={r['speculative_accepted']}:drafted={r['speculative_drafted']}"
        for r in measured
    ))
if bad_backend:
    raise SystemExit(f"non-metal backend in measured runs: {[r['label'] for r in bad_backend]}")
if fallbacks:
    raise SystemExit(f"decode fallback in measured runs: {[r['label'] for r in fallbacks]}")
if prefill_failures:
    raise SystemExit(f"prefill frame execute failure in measured runs: {[r['label'] for r in prefill_failures]}")
if min_prefill_frame_execute and missing_prefill_execute:
    raise SystemExit(f"prefill frame execute below gate in measured runs: {[r['label'] for r in missing_prefill_execute]}")
if mapped_failures:
    raise SystemExit(f"mapped weight residency failures in measured runs: {[r['label'] for r in mapped_failures]}")
if min_generated_tokens and short_generations:
    raise SystemExit(f"generated token count below gate in measured runs: {[(r['label'], r['tokens'], r['finish_reason']) for r in short_generations]}")
if require_max_tokens and not_max_tokens:
    raise SystemExit(f"measured runs stopped before max tokens: {[(r['label'], r['tokens'], r['finish_reason']) for r in not_max_tokens]}")
if min_q4_0_dispatch and missing_q4_0:
    raise SystemExit(f"Q4_0 dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0]}")
if min_q4_0_pair_reduce and missing_q4_0_pair_reduce:
    raise SystemExit(f"Q4_0 pair-reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_pair_reduce]}")
if min_q4_0_pair_act and missing_q4_0_pair_act:
    raise SystemExit(f"Q4_0 pair activation reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_pair_act]}")
if min_q4_0_activation_rhs and missing_q4_0_activation_rhs:
    raise SystemExit(f"Q4_0 activation-rhs reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_activation_rhs]}")
if min_q4_0_activation_rhs_f16 and missing_q4_0_activation_rhs_f16:
    raise SystemExit(f"Q4_0 activation-rhs f16-output dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_activation_rhs_f16]}")
if min_q4_0_ple_activation_rhs_f16 and missing_q4_0_ple_activation_rhs_f16:
    raise SystemExit(f"Q4_0 PLE activation-rhs f16-output dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_ple_activation_rhs_f16]}")
if min_q4_0_ple_linear_f16 and missing_q4_0_ple_linear_f16:
    raise SystemExit(f"Q4_0 PLE f16-input linear reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_ple_linear_f16]}")
if min_q4_0_pair_act_f16 and missing_q4_0_f16:
    raise SystemExit(f"Q4_0 pair activation f16-output dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_f16]}")
if min_q4_0_pair_act_rms_f16 and missing_q4_0_rms_f16:
    raise SystemExit(f"Q4_0 pair activation RMS-scale f16-output dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_rms_f16]}")
if min_q4_0_linear_f16 and missing_q4_0_linear_f16:
    raise SystemExit(f"Q4_0 f16-input linear reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_linear_f16]}")
if min_q4_0_linear_out_f16 and missing_q4_0_linear_out_f16:
    raise SystemExit(f"Q4_0 f16-output linear reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_linear_out_f16]}")
if min_q4_0_linear_in_f16_out_f16 and missing_q4_0_linear_in_f16_out_f16:
    raise SystemExit(f"Q4_0 f16-input/f16-output linear reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_linear_in_f16_out_f16]}")
if min_q4_0_linear_sumsq and missing_q4_0_linear_sumsq:
    raise SystemExit(f"Q4_0 sumsq linear reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q4_0_linear_sumsq]}")
if min_rms_norm_add_sumsq and missing_rms_norm_add_sumsq:
    raise SystemExit(f"RMS/add sumsq dispatch below gate in measured runs: {[r['label'] for r in missing_rms_norm_add_sumsq]}")
if excess_q4_0_linear_sumsq:
    raise SystemExit(f"Q4_0 sumsq linear reduce dispatch above gate in measured runs: {[(r['label'], r['q4_0_linear_reduce_sumsq']) for r in excess_q4_0_linear_sumsq]}")
if excess_rms_norm_add_sumsq:
    raise SystemExit(f"RMS/add sumsq dispatch above gate in measured runs: {[(r['label'], r['rms_norm_add_sumsq']) for r in excess_rms_norm_add_sumsq]}")
if min_paged_attention_1x and missing_paged_attention_1x:
    raise SystemExit(f"paged attention 1x dispatch below gate in measured runs: {[r['label'] for r in missing_paged_attention_1x]}")
if min_q4_pair_act_f16 and missing_q4_f16:
    raise SystemExit(f"Q4_K pair activation f16-output dispatch below gate in measured runs: {[r['label'] for r in missing_q4_f16]}")
if min_q6_f16 and missing_q6_f16:
    raise SystemExit(f"Q6_K f16-input reduce dispatch below gate in measured runs: {[r['label'] for r in missing_q6_f16]}")
if min_active_decode_success and missing_active_decode_success:
    raise SystemExit(f"active decode frame success below gate in measured runs: {[r['label'] for r in missing_active_decode_success]}")
if min_active_decode_final_fused_argmax and missing_active_decode_final_fused_argmax:
    raise SystemExit(f"active decode fused argmax below gate in measured runs: {[r['label'] for r in missing_active_decode_final_fused_argmax]}")
if require_mtp_enabled and missing_mtp:
    raise SystemExit(f"MTP not enabled in measured runs: {[r['label'] for r in missing_mtp]}")
if min_speculative_rounds and missing_speculative_rounds:
    raise SystemExit(f"speculative rounds below gate in measured runs: {[r['label'] for r in missing_speculative_rounds]}")
if min_speculative_drafted and missing_speculative_drafted:
    raise SystemExit(f"speculative drafted tokens below gate in measured runs: {[r['label'] for r in missing_speculative_drafted]}")
if min_speculative_accepted and missing_speculative_accepted:
    raise SystemExit(f"speculative accepted tokens below gate in measured runs: {[r['label'] for r in missing_speculative_accepted]}")
if min_speculative_matched and missing_speculative_matched:
    raise SystemExit(f"speculative matched tokens below gate in measured runs: {[r['label'] for r in missing_speculative_matched]}")
if min_mtp_acceptance_permille and missing_mtp_acceptance:
    raise SystemExit(f"MTP acceptance below gate in measured runs: {[r['label'] for r in missing_mtp_acceptance]}")
if slow_mtp_verify:
    raise SystemExit(f"MTP verify latency above gate in measured runs: {[(r['label'], r['mtp_verify_ms']) for r in slow_mtp_verify]}")
if slow_mtp_materialization:
    raise SystemExit(f"MTP materialization latency above gate in measured runs: {[(r['label'], r['mtp_materialization_ms']) for r in slow_mtp_materialization]}")
if last_compute_encoder_regressions:
    raise SystemExit(f"last compute encoder count above gate in measured runs: {[(r['label'], r['last_compute_encoders']) for r in last_compute_encoder_regressions]}")
if median_decode < min_decode:
    raise SystemExit(f"median decode tok/s {median_decode:.3f} below gate {min_decode:.3f}")
if median_hot_decode < min_hot_decode:
    raise SystemExit(f"median hot decode tok/s {median_hot_decode:.3f} below gate {min_hot_decode:.3f}")
PY

echo "raw output: $OUT_DIR"
