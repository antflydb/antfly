#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: validate_cuda_turboquant_gemma4.sh [--quick|--full|--bench-only]

Runs the CUDA Gemma4 TurboQuant production-readiness gate.

Environment overrides:
  ZIG_BIN                 path to Zig 0.16 binary
  ANTFLY_BIN              path to antfly-inference binary
  OUT_DIR                 output directory for logs/json timing
  E2B_MODEL               Gemma4 E2B model directory
  GEMMA12B_Q4_MODEL       Gemma4 12B Q4 model path/directory
  REAL_BENCH_MODEL        model directory for the 128-token real CLI benchmark
  REAL_BENCH_PROMPT       prompt for the real CLI benchmark
  TURBOQUANT_BENEFIT_CHECK 1 to compare f32 vs requested turbo3 (default: 1)
  TURBOQUANT_BENCH_TOKENS generated tokens for benefit check (default: 256)
  TURBOQUANT_MIN_SPEED_RATIO requested turbo3 tok/s floor vs f32 (default: 1.0)
USAGE
}

mode="quick"
while [ $# -gt 0 ]; do
  case "$1" in
    --quick)
      mode="quick"
      ;;
    --full)
      mode="full"
      ;;
    --bench-only)
      mode="bench-only"
      ;;
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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
inference_dir="$repo_root/zig/pkg/inference"
zig_bin="${ZIG_BIN:-$repo_root/.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig}"
antfly_bin="${ANTFLY_BIN:-${ANTFY_BIN:-$inference_dir/zig-out/bin/antfly-inference}}"
out_dir="${OUT_DIR:-/tmp/antfly-cuda-turboquant-gate-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out_dir"

e2b_model="${E2B_MODEL:-$repo_root/.models/unsloth/gemma-4-E2B-it-qat-GGUF}"
gemma12b_q4_model="${GEMMA12B_Q4_MODEL:-$repo_root/.models/google/gemma-4-12B-it-q4_k}"
real_bench_model="${REAL_BENCH_MODEL:-$e2b_model}"
real_bench_prompt="${REAL_BENCH_PROMPT:-Give a one sentence summary of Korean history.}"
turboquant_benefit_check="${TURBOQUANT_BENEFIT_CHECK:-1}"
turboquant_bench_tokens="${TURBOQUANT_BENCH_TOKENS:-256}"
turboquant_min_speed_ratio="${TURBOQUANT_MIN_SPEED_RATIO:-1.0}"

summary="$out_dir/summary.txt"
: > "$summary"

log() {
  printf '%s\n' "$*" | tee -a "$summary"
}

require_path() {
  local label="$1"
  local path="$2"
  if [ ! -e "$path" ]; then
    log "missing $label: $path"
    exit 1
  fi
}

run_logged() {
  local name="$1"
  shift
  local log_file="$out_dir/$name.log"
  log "RUN $name: $*"
  "$@" 2>&1 | tee "$log_file"
}

run_generate() {
  local name="$1"
  local model="$2"
  local prompt="$3"
  local cache_dtype="$4"
  local max_tokens="$5"
  shift 5
  local log_file="$out_dir/$name.log"
  local json_file="$out_dir/$name.json"
  local -a args=(
    "$antfly_bin" generate "$model" "$prompt"
    --backend cuda
    --max-tokens "$max_tokens"
    --temperature 0
    --raw-prompt
    --no-chat-template
    --print-token-count
    --print-token-ids
    --print-timing
    --json-timing "$json_file"
  )
  if [ "$cache_dtype" != "default" ]; then
    args+=(--cache-dtype "$cache_dtype")
  fi
  args+=("$@")
  log "RUN $name: ${args[*]}"
  "${args[@]}" 2>&1 | tee "$log_file"
  {
    printf 'case=%s\n' "$name"
    grep -E '^(generate_timing_ms:|timing_ms:|decode_tok_per_s=|token_ids:|cuda_fallback_counts:|cuda_device_kv_counts:)' "$log_file" || true
    printf '\n'
  } >> "$summary"
}

check_turboquant_benefit() {
  local f32_json="$1"
  local turbo_json="$2"
  python3 - "$f32_json" "$turbo_json" "$turboquant_min_speed_ratio" <<'PY'
import json
import sys

f32_path, turbo_path, min_ratio_raw = sys.argv[1:4]
min_ratio = float(min_ratio_raw)

with open(f32_path, "r", encoding="utf-8") as f:
    f32 = json.load(f)
with open(turbo_path, "r", encoding="utf-8") as f:
    turbo = json.load(f)

f32_tps = float(f32.get("decode_tok_per_s", 0.0))
turbo_tps = float(turbo.get("decode_tok_per_s", 0.0))
errors = []
if f32_tps <= 0:
    errors.append(f"f32 decode_tok_per_s={f32_tps}, expected > 0")
if turbo_tps <= 0:
    errors.append(f"turboquant decode_tok_per_s={turbo_tps}, expected > 0")
if f32_tps > 0 and turbo_tps > 0 and turbo_tps < f32_tps * min_ratio:
    errors.append(
        f"turboquant decode_tok_per_s={turbo_tps:.3f} below f32 floor "
        f"{f32_tps * min_ratio:.3f} (f32={f32_tps:.3f}, ratio={min_ratio:.3f})"
    )

if errors:
    for error in errors:
        print(error, file=sys.stderr)
    sys.exit(1)

print(f"PASS turboquant_benefit: f32_tok_s={f32_tps:.3f} turboquant_tok_s={turbo_tps:.3f} ratio={turbo_tps / f32_tps:.3f}")
PY
}

require_path "zig binary" "$zig_bin"
require_path "antfly-inference binary" "$antfly_bin"

if [ "$mode" != "bench-only" ]; then
  (cd "$inference_dir" && run_logged build_cuda "$zig_bin" build -Dcuda=true)
  (cd "$repo_root" && run_logged cuda_artifact_check "$inference_dir/scripts/regen-cuda-artifacts.sh" --check --all)
  (cd "$repo_root" && run_logged cuda_smoke "$antfly_bin" cuda-info --smoke)
fi

require_path "E2B model" "$e2b_model"
require_path "12B Q4 model" "$gemma12b_q4_model"
require_path "real benchmark model" "$real_bench_model"

if [ "$mode" != "bench-only" ]; then
  run_generate e2b_f32_16 "$e2b_model" "Write one sentence about ants." f32 16 \
    --combined-budget-mb 12000 --backend-budget-mb 9000 --kv-budget-mb 256 --scratch-budget-mb 512
  run_generate e2b_polar4_16 "$e2b_model" "Write one sentence about ants." polar4 16 \
    --combined-budget-mb 12000 --backend-budget-mb 9000 --kv-budget-mb 256 --scratch-budget-mb 512
  run_generate e2b_turbo3_8 "$e2b_model" "Write one sentence about ants." turbo3 8 \
    --combined-budget-mb 12000 --backend-budget-mb 9000 --kv-budget-mb 256 --scratch-budget-mb 512
fi

if [ "$turboquant_benefit_check" = "1" ]; then
  run_generate e2b_turboquant_f32_benefit "$e2b_model" "Write one sentence about ants." f32 "$turboquant_bench_tokens" \
    --combined-budget-mb 12000 --backend-budget-mb 9000 --kv-budget-mb 512 --scratch-budget-mb 1024
  run_generate e2b_turboquant_on_benefit "$e2b_model" "Write one sentence about ants." turbo3 "$turboquant_bench_tokens" \
    --combined-budget-mb 12000 --backend-budget-mb 9000 --kv-budget-mb 512 --scratch-budget-mb 1024
  check_turboquant_benefit "$out_dir/e2b_turboquant_f32_benefit.json" "$out_dir/e2b_turboquant_on_benefit.json" | tee -a "$summary"
fi

if [ "$mode" = "full" ]; then
  run_generate e2b_polar4_128 "$e2b_model" "Write one sentence about ants." polar4 128 \
    --combined-budget-mb 12000 --backend-budget-mb 9000 --kv-budget-mb 512 --scratch-budget-mb 512
  for i in 1 2 3; do
    run_generate "gemma12b_q4_polar4_32_run$i" "$gemma12b_q4_model" "Write one sentence about ants." polar4 32 \
      --combined-budget-mb 22000 --backend-budget-mb 19000 --kv-budget-mb 1024 --scratch-budget-mb 1024
  done
else
  run_generate gemma12b_q4_polar4_8 "$gemma12b_q4_model" "Write one sentence about ants." polar4 8 \
    --combined-budget-mb 22000 --backend-budget-mb 19000 --kv-budget-mb 512 --scratch-budget-mb 1024
fi

run_generate real_bench_128 "$real_bench_model" "$real_bench_prompt" default 128

log "summary: $summary"
log "outputs: $out_dir"
