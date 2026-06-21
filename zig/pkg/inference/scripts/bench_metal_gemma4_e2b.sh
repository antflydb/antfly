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
MODEL_DIR="${ANTFLY_INFERENCE_GEMMA4_MODEL:-$DEFAULT_MODELS_DIR/$MODEL_NAME}"
MODELS_DIR="${ANTFLY_INFERENCE_GEMMA4_MODELS_DIR:-$DEFAULT_MODELS_DIR}"
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
MIN_SERVER_TOK_S="${ANTFLY_INFERENCE_GEMMA4_MIN_SERVER_TOK_S:-0}"
MAX_SERVER_WARM_MS="${ANTFLY_INFERENCE_GEMMA4_MAX_SERVER_WARM_MS:-0}"
CACHE_DTYPE="${ANTFLY_INFERENCE_GEMMA4_CACHE_DTYPE:-}"
REUSE_PROBE="${ANTFLY_INFERENCE_GEMMA4_REUSE_PROBE:-1}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-gemma4-e2b-metal-$(date -u +%Y%m%d-%H%M%S)}"

if [[ ! -x "$ANTFLY_BIN" ]]; then
  echo "antfly inference binary not executable: $ANTFLY_BIN" >&2
  echo "build it first: cd zig/pkg/inference && zig build -Doptimize=ReleaseFast -Dmetal=true -Donnx=false -Dpjrt=false" >&2
  exit 2
fi

if [[ ! -d "$MODEL_DIR" ]]; then
  echo "Gemma4 model directory not found: $MODEL_DIR" >&2
  echo "set ANTFLY_INFERENCE_GEMMA4_MODEL to the local GGUF model directory" >&2
  exit 2
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
      --warm-generator "$MODEL_NAME" \
      --warm-generator-backend metal
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
import re
import statistics
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
min_tok_s = float(sys.argv[2])
max_warm_ms = int(sys.argv[3])
server_log = (out_dir / "server-warm.txt").read_text(encoding="utf-8", errors="replace")

def grab(pattern, text, default=None, cast=int):
    m = re.search(pattern, text)
    if not m:
        return default
    return cast(m.group(1))

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
for path in sorted(out_dir.glob("server-request-*.txt")):
    text = path.read_text(encoding="utf-8", errors="replace")
    tokens = grab(r"(?:finish_reason=\S+\s+)?tokens=(\d+)", text)
    total_ms = grab(r"timing_ms:.*\bserver_request=(\d+)", text)
    if tokens is None or total_ms is None:
        raise SystemExit(f"missing server request timing fields in {path}")
    rows.append({
        "label": path.stem,
        "tokens": tokens,
        "server_request_ms": total_ms,
        "tok_s": tokens / (total_ms / 1000.0) if total_ms else 0.0,
        "file": str(path),
    })
if not rows:
    raise SystemExit("no server-request files found")

summary = {
    "warm": warm,
    "median_server_tok_s": statistics.median(r["tok_s"] for r in rows),
    "rows": rows,
}
(out_dir / "server-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
with (out_dir / "server-summary.tsv").open("w", encoding="utf-8") as f:
    f.write("label\ttokens\tserver_request_ms\ttok_s\twarm_elapsed_ms\twarm_load_ms\twarm_generate_ms\twarm_runtime_prepare_ms\twarm_prefill_ms\twarm_decode_ms\tfile\n")
    for r in rows:
        f.write(
            f"{r['label']}\t{r['tokens']}\t{r['server_request_ms']}\t{r['tok_s']:.3f}\t"
            f"{warm['elapsed_ms']}\t{warm['load_ms']}\t{warm['generate_ms']}\t"
            f"{warm['runtime_prepare_ms']}\t{warm['prefill_ms']}\t{warm['decode_ms']}\t{r['file']}\n"
        )

print(f"server summary: {out_dir / 'server-summary.tsv'}")
print(
    "warm_elapsed_ms={elapsed_ms} warm_load_ms={load_ms} warm_generate_ms={generate_ms} "
    "warm_runtime_prepare_ms={runtime_prepare_ms} warm_prefill_ms={prefill_ms} warm_decode_ms={decode_ms}".format(**warm)
)
print(f"median_server_tok_s={summary['median_server_tok_s']:.3f}")
if max_warm_ms and warm["elapsed_ms"] > max_warm_ms:
    raise SystemExit(f"warm elapsed {warm['elapsed_ms']}ms above gate {max_warm_ms}ms")
if min_tok_s and summary["median_server_tok_s"] < min_tok_s:
    raise SystemExit(f"median server tok/s {summary['median_server_tok_s']:.3f} below gate {min_tok_s:.3f}")
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
  )
  if [[ -n "$CACHE_DTYPE" ]]; then
    args+=(--cache-dtype "$CACHE_DTYPE")
  fi
  echo "running $label tokens=$tokens cache_dtype=${CACHE_DTYPE:-default} reuse_probe=$REUSE_PROBE..." >&2
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

python3 - "$OUT_DIR" "$MIN_DECODE_TOK_S" "$MIN_HOT_DECODE_TOK_S" <<'PY'
import json
import re
import statistics
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
min_decode = float(sys.argv[2])
min_hot_decode = float(sys.argv[3])
rows = []

def grab(pattern, text, default=None, cast=int):
    m = re.search(pattern, text)
    if not m:
        return default
    return cast(m.group(1))

for path in sorted(out_dir.glob("*.txt")):
    text = path.read_text(encoding="utf-8", errors="replace")
    tokens = grab(r"(?:finish_reason=\S+\s+)?tokens=(\d+)", text)
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
    frame_begins = grab(r"metal_decoder_frame:\s+begins=(\d+)", text, default=0)
    frame_wait_ms = grab(r"metal_decoder_frame:.*\bwait_ms=(\d+)", text, default=0)
    frame_gpu_ms = grab(r"metal_decoder_frame:.*\bgpu_ms=(\d+)", text, default=0)
    q8_mmv = grab(r"metal_q8_0_dispatch:.*\bmmv=(\d+)", text, default=0)
    q8_mm = grab(r"metal_q8_0_dispatch:.*\bmm=(\d+)", text, default=0)
    command_ops = grab(r"metal_runtime_command_ops:\s+total=(\d+)", text, default=0)
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
    if tokens is None or generate_ms is None or total_ms is None:
        raise SystemExit(f"missing timing fields in {path}")
    decode_tok_s = tokens / (generate_ms / 1000.0) if generate_ms else 0.0
    e2e_tok_s = tokens / (total_ms / 1000.0) if total_ms else 0.0
    hot_decode_tok_s = greedy_calls / (greedy_direct_ms / 1000.0) if greedy_calls and greedy_direct_ms else 0.0
    prefill_tok_s = prefill_tokens / (prefill_direct_family_ms / 1000.0) if prefill_tokens and prefill_direct_family_ms else 0.0
    rows.append({
        "label": path.stem,
        "tokens": tokens,
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
        "frame_begins": frame_begins,
        "frame_wait_ms": frame_wait_ms,
        "frame_gpu_ms": frame_gpu_ms,
        "q8_mmv": q8_mmv,
        "q8_mm": q8_mm,
        "command_ops": command_ops,
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
        "file": str(path),
    })

measured = [r for r in rows if r["label"].startswith("run-")]
if not measured:
    raise SystemExit("no measured run-* files found")
median_decode = statistics.median(r["decode_tok_s"] for r in measured)
mean_decode = statistics.mean(r["decode_tok_s"] for r in measured)
median_e2e = statistics.median(r["e2e_tok_s"] for r in measured)
median_hot_decode = statistics.median(r["hot_decode_tok_s"] for r in measured)
mean_hot_decode = statistics.mean(r["hot_decode_tok_s"] for r in measured)
summary = {
    "median_decode_tok_s": median_decode,
    "mean_decode_tok_s": mean_decode,
    "median_e2e_tok_s": median_e2e,
    "median_hot_decode_tok_s": median_hot_decode,
    "mean_hot_decode_tok_s": mean_hot_decode,
    "min_decode_tok_s": min_decode,
    "min_hot_decode_tok_s": min_hot_decode,
    "rows": rows,
}
(out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
with (out_dir / "summary.tsv").open("w", encoding="utf-8") as f:
    f.write("label\ttokens\tgenerate_ms\ttotal_ms\truntime_prewarm_ms\tfirst_token_request_ms\tfirst_token_service_ms\tfirst_token_prefill_ms\tfirst_token_sample_ms\treuse_first_token_service_ms\treuse_first_token_prefill_ms\treuse_first_token_sample_ms\tdecode_tok_s\te2e_tok_s\thot_decode_tok_s\tprefill_tokens\tprefill_tok_s\tbackend\tdecode_fallback\tframe_begins\tframe_wait_ms\tframe_gpu_ms\tq8_mmv\tq8_mm\tcommand_ops\tgreedy_calls\tgreedy_direct_ms\tgreedy_layer_specs_ms\tprefill_direct_family_ms\tple_prepare_ms\tquant_private_ms\tquant_private_slots\tquant_mapped_slots\tquant_mapped_failures\tfile\n")
    for r in rows:
        f.write(
            f"{r['label']}\t{r['tokens']}\t{r['generate_ms']}\t{r['total_ms']}\t{r['runtime_prewarm_ms']}\t"
            f"{r['first_token_request_ms']}\t{r['first_token_service_ms']}\t{r['first_token_prefill_ms']}\t"
            f"{r['first_token_sample_ms']}\t{r['reuse_first_token_service_ms']}\t"
            f"{r['reuse_first_token_prefill_ms']}\t{r['reuse_first_token_sample_ms']}\t"
            f"{r['decode_tok_s']:.3f}\t{r['e2e_tok_s']:.3f}\t{r['hot_decode_tok_s']:.3f}\t"
            f"{r['prefill_tokens']}\t{r['prefill_tok_s']:.3f}\t{r['backend']}\t"
            f"{r['decode_fallback']}\t{r['frame_begins']}\t{r['frame_wait_ms']}\t"
            f"{r['frame_gpu_ms']}\t{r['q8_mmv']}\t{r['q8_mm']}\t{r['command_ops']}\t"
            f"{r['greedy_calls']}\t{r['greedy_direct_ms']}\t{r['greedy_layer_specs_ms']}\t"
            f"{r['prefill_direct_family_ms']}\t{r['ple_prepare_ms']}\t{r['quant_private_ms']}\t"
            f"{r['quant_private_slots']}\t{r['quant_mapped_slots']}\t{r['quant_mapped_failures']}\t"
            f"{r['file']}\n"
        )

bad_backend = [r for r in measured if r["backend"] != "metal"]
fallbacks = [r for r in measured if r["decode_fallback"] != 0]
mapped_failures = [r for r in measured if r["quant_mapped_failures"] != 0]
print(f"summary: {out_dir / 'summary.tsv'}")
print(f"median_decode_tok_s={median_decode:.3f} mean_decode_tok_s={mean_decode:.3f} median_e2e_tok_s={median_e2e:.3f}")
print(f"median_hot_decode_tok_s={median_hot_decode:.3f} mean_hot_decode_tok_s={mean_hot_decode:.3f}")
if bad_backend:
    raise SystemExit(f"non-metal backend in measured runs: {[r['label'] for r in bad_backend]}")
if fallbacks:
    raise SystemExit(f"decode fallback in measured runs: {[r['label'] for r in fallbacks]}")
if mapped_failures:
    raise SystemExit(f"mapped weight residency failures in measured runs: {[r['label'] for r in mapped_failures]}")
if median_decode < min_decode:
    raise SystemExit(f"median decode tok/s {median_decode:.3f} below gate {min_decode:.3f}")
if median_hot_decode < min_hot_decode:
    raise SystemExit(f"median hot decode tok/s {median_hot_decode:.3f} below gate {min_hot_decode:.3f}")
PY

echo "raw output: $OUT_DIR"
