#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/gemma4/gemma4_cuda_production_gate.sh

Runs the Gemma4 CUDA production-readiness gate against local models.

Environment:
  ROOT_DIR=/path/to/antfly
  ZIG=.tools/zig-x86_64-linux-0.16.0/zig
  BIN=zig/pkg/inference/zig-out/bin/antfly-inference
  OUT_DIR=/tmp/gemma4-cuda-production-gate-<timestamp>

  RUN_BUILD=1
  RUN_SMOKE=1
  RUN_MICROBENCH=0
  RUN_DEFAULT_POLICY=1
  RUN_TARGET_ONLY=1
  RUN_12B_MTP=1
  RUN_E2B_MTP=1

  MAX_TOKENS=16
  PROMPT_FILTER="ants_chat code_chat"
  SPEC_KS="1 2 4"
  RUN_TIMEOUT=420s
  MTP_VERIFY_DEVICE_RESULT=auto|0|1
  MIN_12B_Q8_TOK_S=13.0
  MIN_12B_Q4K_TOK_S=8.5
  MIN_E2B_TOK_S=18.0

  GEMMA12_Q8=.models/google/gemma-4-12B-it-q8_0
  GEMMA12_Q4=.models/google/gemma-4-12B-it-q4_k
  GEMMA12_ASSISTANT_Q8=.models/google/gemma-4-12B-it-assistant
  GEMMA12_ASSISTANT_Q4=
  E2B_TARGET=.models/unsloth/gemma-4-E2B-it-qat-GGUF
  E2B_ASSISTANT_Q8=.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q8_0-MTP.gguf
  E2B_ASSISTANT_Q4=.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q4_0-MTP.gguf
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ $# -ne 0 ]]; then
  usage
  exit 2
fi

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ZIG="${ZIG:-$ROOT_DIR/.tools/zig-x86_64-linux-0.16.0/zig}"
BIN="${BIN:-$ROOT_DIR/zig/pkg/inference/zig-out/bin/antfly-inference}"
OUT_DIR="${OUT_DIR:-/tmp/gemma4-cuda-production-gate-$(date +%Y%m%d-%H%M%S)}"

RUN_BUILD="${RUN_BUILD:-1}"
RUN_SMOKE="${RUN_SMOKE:-1}"
RUN_MICROBENCH="${RUN_MICROBENCH:-0}"
RUN_DEFAULT_POLICY="${RUN_DEFAULT_POLICY:-1}"
RUN_TARGET_ONLY="${RUN_TARGET_ONLY:-1}"
RUN_12B_MTP="${RUN_12B_MTP:-1}"
RUN_E2B_MTP="${RUN_E2B_MTP:-1}"

MAX_TOKENS="${MAX_TOKENS:-16}"
PROMPT_FILTER="${PROMPT_FILTER:-ants_chat code_chat}"
SPEC_KS="${SPEC_KS:-1 2 4}"
RUN_TIMEOUT="${RUN_TIMEOUT:-420s}"
MICROBENCH_WARMUP_ITERS="${MICROBENCH_WARMUP_ITERS:-2}"
MICROBENCH_MEASURE_ITERS="${MICROBENCH_MEASURE_ITERS:-5}"
MTP_VERIFY_DEVICE_RESULT="${MTP_VERIFY_DEVICE_RESULT:-auto}"

COMBINED_BUDGET_MB="${COMBINED_BUDGET_MB:-22000}"
BACKEND_BUDGET_MB="${BACKEND_BUDGET_MB:-19000}"
KV_BUDGET_MB="${KV_BUDGET_MB:-512}"
SCRATCH_BUDGET_MB="${SCRATCH_BUDGET_MB:-1024}"
E2B_COMBINED_BUDGET_MB="${E2B_COMBINED_BUDGET_MB:-12000}"
E2B_BACKEND_BUDGET_MB="${E2B_BACKEND_BUDGET_MB:-9000}"
E2B_KV_BUDGET_MB="${E2B_KV_BUDGET_MB:-256}"
E2B_SCRATCH_BUDGET_MB="${E2B_SCRATCH_BUDGET_MB:-512}"

TARGET_BASELINE_MIN_RATIO="${TARGET_BASELINE_MIN_RATIO:-0.90}"
AUTO_MIN_RATIO="${AUTO_MIN_RATIO:-0.95}"
PROMOTION_RATIO="${PROMOTION_RATIO:-1.05}"
MIN_12B_Q8_TOK_S="${MIN_12B_Q8_TOK_S:-13.0}"
MIN_12B_Q4K_TOK_S="${MIN_12B_Q4K_TOK_S:-8.5}"
MIN_E2B_TOK_S="${MIN_E2B_TOK_S:-18.0}"

GEMMA12_Q8="${GEMMA12_Q8:-$ROOT_DIR/.models/google/gemma-4-12B-it-q8_0}"
GEMMA12_Q4="${GEMMA12_Q4:-$ROOT_DIR/.models/google/gemma-4-12B-it-q4_k}"
GEMMA12_ASSISTANT_Q8="${GEMMA12_ASSISTANT_Q8:-$ROOT_DIR/.models/google/gemma-4-12B-it-assistant}"
GEMMA12_ASSISTANT_Q4="${GEMMA12_ASSISTANT_Q4:-}"
E2B_TARGET="${E2B_TARGET:-$ROOT_DIR/.models/unsloth/gemma-4-E2B-it-qat-GGUF}"
E2B_ASSISTANT_Q8="${E2B_ASSISTANT_Q8:-$ROOT_DIR/.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q8_0-MTP.gguf}"
E2B_ASSISTANT_Q4="${E2B_ASSISTANT_Q4:-$ROOT_DIR/.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q4_0-MTP.gguf}"

mkdir -p "$OUT_DIR"
STEPS_TSV="$OUT_DIR/steps.tsv"
printf "step\tstatus\tdetail\n" >"$STEPS_TSV"

FAILED=0

record() {
  local step="$1"
  local status="$2"
  local detail="${3:-}"
  printf "%s\t%s\t%s\n" "$step" "$status" "$detail" >>"$STEPS_TSV"
  if [[ "$status" == "fail" ]]; then
    FAILED=1
  fi
}

exists_path() {
  [[ -e "$1" ]]
}

run_logged() {
  local step="$1"
  local log="$2"
  shift 2
  echo "gate: $step"
  if "$@" >"$log" 2>&1; then
    record "$step" "ok" "$log"
  else
    local status=$?
    record "$step" "fail" "$log exit_$status"
  fi
}

run_logged_in_dir() {
  local step="$1"
  local dir="$2"
  local log="$3"
  shift 3
  echo "gate: $step"
  if (cd "$dir" && "$@") >"$log" 2>&1; then
    record "$step" "ok" "$log"
  else
    local status=$?
    record "$step" "fail" "$log exit_$status"
  fi
}

run_target_generate() {
  local label="$1"
  local model="$2"
  local combined="$3"
  local backend="$4"
  local kv="$5"
  local scratch="$6"
  local json_path="$OUT_DIR/${label}.json"
  local log_path="$OUT_DIR/${label}.log"
  if ! exists_path "$model"; then
    record "$label" "skip" "missing $model"
    return
  fi
  run_logged "$label" "$log_path" \
    timeout "$RUN_TIMEOUT" "$BIN" generate "$model" "Write one sentence about ants." \
      --backend cuda \
      --max-tokens "$MAX_TOKENS" \
      --temperature 0 \
      --combined-budget-mb "$combined" \
      --backend-budget-mb "$backend" \
      --kv-budget-mb "$kv" \
      --scratch-budget-mb "$scratch" \
      --print-timing \
      --print-token-count \
      --json-timing "$json_path" \
      --raw-prompt \
      --no-chat-template
}

run_default_policy_check() {
  local target="$GEMMA12_Q8"
  local assistant="$GEMMA12_ASSISTANT_Q8"
  local json_path="$OUT_DIR/default_auto_uncalibrated.json"
  local log_path="$OUT_DIR/default_auto_uncalibrated.log"
  if ! exists_path "$target" || ! exists_path "$assistant"; then
    record "default_auto_uncalibrated" "skip" "missing 12B target or assistant"
    return
  fi
  run_logged "default_auto_uncalibrated" "$log_path" \
    timeout "$RUN_TIMEOUT" "$BIN" generate "$target" "Ants" \
      --backend cuda \
      --draft-model "$assistant" \
      --speculative-k 1 \
      --speculation-policy auto \
      --max-tokens 1 \
      --temperature 0 \
      --combined-budget-mb "$COMBINED_BUDGET_MB" \
      --backend-budget-mb "$BACKEND_BUDGET_MB" \
      --kv-budget-mb "$KV_BUDGET_MB" \
      --scratch-budget-mb "$SCRATCH_BUDGET_MB" \
      --print-timing \
      --print-token-count \
      --json-timing "$json_path" \
      --raw-prompt \
      --no-chat-template
}

run_mtp_matrix() {
  local label="$1"
  local target="$2"
  local assistant_q8="$3"
  local assistant_q4="$4"
  local combined="$5"
  local backend="$6"
  local kv="$7"
  local scratch="$8"
  local matrix_dir="$OUT_DIR/$label"
  local log_path="$OUT_DIR/${label}.log"
  if ! exists_path "$target" || ! exists_path "$assistant_q8"; then
    record "$label" "skip" "missing target or q8 assistant"
    return
  fi
  mkdir -p "$matrix_dir"
  echo "gate: $label"
  local -a args=("$target" "$assistant_q8")
  if [[ -n "$assistant_q4" && -e "$assistant_q4" ]]; then
    args+=("$assistant_q4")
  fi
  if OUT_DIR="$matrix_dir" \
    BIN="$BIN" \
    MODE=production \
    SPECULATION_POLICY=auto \
    SPECULATION_CALIBRATION=probe \
    SPEC_KS="$SPEC_KS" \
    PROMPT_FILTER="$PROMPT_FILTER" \
    MAX_TOKENS="$MAX_TOKENS" \
    RUN_TIMEOUT="$RUN_TIMEOUT" \
    MTP_VERIFY_DEVICE_RESULT="$MTP_VERIFY_DEVICE_RESULT" \
    RUN_TARGET_ONLY=1 \
    COMBINED_BUDGET_MB="$combined" \
    BACKEND_BUDGET_MB="$backend" \
    KV_BUDGET_MB="$kv" \
    SCRATCH_BUDGET_MB="$scratch" \
    "$ROOT_DIR/scripts/bench_gemma4_mtp.sh" "${args[@]}" >"$log_path" 2>&1; then
    record "$label" "ok" "$matrix_dir/summary.tsv"
  else
    local status=$?
    record "$label" "fail" "$log_path exit_$status"
  fi
}

if [[ "$RUN_BUILD" == "1" ]]; then
  run_logged_in_dir "build_cuda_sm89" "$ROOT_DIR/zig/pkg/inference" "$OUT_DIR/build_cuda_sm89.log" "$ZIG" build -Dcuda=true -Dcuda-artifacts=sm89
else
  record "build_cuda_sm89" "skip" "RUN_BUILD=0"
fi

if [[ "$RUN_SMOKE" == "1" ]]; then
  run_logged "cuda_smoke" "$OUT_DIR/cuda_smoke.log" "$BIN" cuda-info --smoke
else
  record "cuda_smoke" "skip" "RUN_SMOKE=0"
fi

if [[ "$RUN_MICROBENCH" == "1" ]]; then
  run_logged "bench_cuda_gemma4_shapes" "$OUT_DIR/bench_cuda_gemma4_shapes.log" \
    "$BIN" bench-cuda \
    --warmup-iters "$MICROBENCH_WARMUP_ITERS" \
    --measure-iters "$MICROBENCH_MEASURE_ITERS" \
    --gemma4-shapes \
    --json-out "$OUT_DIR/bench-cuda-gemma4-shapes.json"
else
  record "bench_cuda_gemma4_shapes" "skip" "RUN_MICROBENCH=0"
fi

if [[ "$RUN_DEFAULT_POLICY" == "1" ]]; then
  run_default_policy_check
else
  record "default_auto_uncalibrated" "skip" "RUN_DEFAULT_POLICY=0"
fi

if [[ "$RUN_TARGET_ONLY" == "1" ]]; then
  run_target_generate "target_12b_q8" "$GEMMA12_Q8" "$COMBINED_BUDGET_MB" "$BACKEND_BUDGET_MB" "$KV_BUDGET_MB" "$SCRATCH_BUDGET_MB"
  run_target_generate "target_12b_q4k" "$GEMMA12_Q4" "$COMBINED_BUDGET_MB" "$BACKEND_BUDGET_MB" "$KV_BUDGET_MB" "$SCRATCH_BUDGET_MB"
  run_target_generate "target_e2b" "$E2B_TARGET" "$E2B_COMBINED_BUDGET_MB" "$E2B_BACKEND_BUDGET_MB" "$E2B_KV_BUDGET_MB" "$E2B_SCRATCH_BUDGET_MB"
else
  record "target_only" "skip" "RUN_TARGET_ONLY=0"
fi

if [[ "$RUN_12B_MTP" == "1" ]]; then
  run_mtp_matrix "mtp_12b" "$GEMMA12_Q8" "$GEMMA12_ASSISTANT_Q8" "$GEMMA12_ASSISTANT_Q4" "$COMBINED_BUDGET_MB" "$BACKEND_BUDGET_MB" "$KV_BUDGET_MB" "$SCRATCH_BUDGET_MB"
else
  record "mtp_12b" "skip" "RUN_12B_MTP=0"
fi

if [[ "$RUN_E2B_MTP" == "1" ]]; then
  run_mtp_matrix "mtp_e2b" "$E2B_TARGET" "$E2B_ASSISTANT_Q8" "$E2B_ASSISTANT_Q4" "$E2B_COMBINED_BUDGET_MB" "$E2B_BACKEND_BUDGET_MB" "$E2B_KV_BUDGET_MB" "$E2B_SCRATCH_BUDGET_MB"
else
  record "mtp_e2b" "skip" "RUN_E2B_MTP=0"
fi

if command -v python3 >/dev/null 2>&1; then
  python3 - "$OUT_DIR" "$TARGET_BASELINE_MIN_RATIO" "$AUTO_MIN_RATIO" "$PROMOTION_RATIO" "$MIN_12B_Q8_TOK_S" "$MIN_12B_Q4K_TOK_S" "$MIN_E2B_TOK_S" <<'PY'
import csv
import json
import pathlib
import sys

out_dir = pathlib.Path(sys.argv[1])
target_baseline_min_ratio = float(sys.argv[2])
auto_min_ratio = float(sys.argv[3])
promotion_ratio = float(sys.argv[4])
target_rate_floors = {
    "target_12b_q8": float(sys.argv[5]),
    "target_12b_q4k": float(sys.argv[6]),
    "target_e2b": float(sys.argv[7]),
}

checks = []
failures = []
warnings = []
promotions = []

def add_check(name, ok, detail):
    checks.append({"name": name, "ok": bool(ok), "detail": detail})
    if not ok:
        failures.append(f"{name}: {detail}")

steps_path = out_dir / "steps.tsv"
step_rows = []
if steps_path.exists():
    with steps_path.open(newline="") as f:
        step_rows = list(csv.DictReader(f, delimiter="\t"))
    for row in step_rows:
        if row.get("status") == "fail":
            add_check(f"step_{row.get('step')}", False, row.get("detail", "failed"))
else:
    add_check("steps_tsv", False, "missing steps.tsv")

default_json = out_dir / "default_auto_uncalibrated.json"
if default_json.exists():
    data = json.loads(default_json.read_text())
    spec = data.get("speculative") or {}
    add_check(
        "default_auto_uncalibrated_policy",
        spec.get("speculation_policy") == "auto"
        and spec.get("speculation_calibration") == "none"
        and spec.get("speculation_policy_decision") == "disabled_uncalibrated"
        and spec.get("mtp_enabled") is False,
        json.dumps(spec, sort_keys=True),
    )
else:
    skipped = any(row.get("step") == "default_auto_uncalibrated" and row.get("status") == "skip" for row in step_rows)
    if not skipped:
        add_check("default_auto_uncalibrated_policy", False, "missing JSON")

def to_float(value):
    try:
        return float(value)
    except Exception:
        return None

def to_int(value):
    try:
        return int(value)
    except Exception:
        return None

microbench_json = out_dir / "bench-cuda-gemma4-shapes.json"
if microbench_json.exists():
    try:
        microbench = json.loads(microbench_json.read_text())
        shapes = microbench.get("gemma4_shapes") or []
        add_check("bench_cuda_gemma4_shapes_nonempty", bool(shapes), f"{len(shapes)} shapes")
        for row in shapes:
            label = row.get("label", "unknown")
            q8_speedup = to_float(row.get("q8_candidate_speedup"))
            q4_speedup = to_float(row.get("q4_candidate_speedup"))
            add_check(
                f"bench_cuda_gemma4_{label}_candidate_fields",
                bool(row.get("q8_candidate") and row.get("q4_candidate") and q8_speedup and q4_speedup),
                json.dumps({k: row.get(k) for k in ("q8_candidate", "q8_candidate_speedup", "q4_candidate", "q4_candidate_speedup")}, sort_keys=True),
            )
            if q8_speedup is not None:
                add_check(f"bench_cuda_gemma4_{label}_q8_speedup_positive", q8_speedup > 0, f"{q8_speedup:.3f}")
            if q4_speedup is not None:
                add_check(f"bench_cuda_gemma4_{label}_q4_speedup_positive", q4_speedup > 0, f"{q4_speedup:.3f}")
    except Exception as exc:
        add_check("bench_cuda_gemma4_shapes_json", False, repr(exc))

def analyze_summary(summary_path):
    rows = []
    with summary_path.open(newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    if not rows:
        add_check(f"{summary_path.parent.name}_summary_nonempty", False, "no rows")
        return

    targets = {}
    for row in rows:
        if row.get("assistant") == "target" and row.get("status") == "ok":
            rate = to_float(row.get("decode_tok_s"))
            if rate is not None:
                targets[row.get("case")] = rate

    add_check(f"{summary_path.parent.name}_target_rows", bool(targets), f"{len(targets)} target rows")

    for row in rows:
        label = row.get("assistant", "")
        status = row.get("status", "")
        case = row.get("case", "")
        if status != "ok":
            add_check(f"{summary_path.parent.name}_{label}_{case}_status", False, status)
            continue
        if label == "target":
            continue

        policy = row.get("policy", "")
        calibration = row.get("calibration", "")
        decision = row.get("policy_decision", "")
        graph_replay = row.get("graph_replay", "")
        add_check(
            f"{summary_path.parent.name}_{label}_{case}_policy_metadata",
            bool(policy and calibration and decision and graph_replay),
            f"policy={policy} calibration={calibration} decision={decision} graph={graph_replay}",
        )

        rate = to_float(row.get("decode_tok_s"))
        target_rate = targets.get(case)
        if rate is None or target_rate is None:
            warnings.append(f"{summary_path.parent.name}:{label}:{case}: missing rate comparison")
            continue

        ratio = rate / target_rate if target_rate > 0 else 0.0
        if decision in {"active", "forced"}:
            add_check(
                f"{summary_path.parent.name}_{label}_{case}_active_ratio",
                ratio >= auto_min_ratio,
                f"ratio={ratio:.3f} rate={rate:.3f} target={target_rate:.3f}",
            )
            fallbacks = to_int(row.get("dedicated_runtime_fallbacks"))
            if fallbacks is not None:
                add_check(
                    f"{summary_path.parent.name}_{label}_{case}_dedicated_runtime",
                    fallbacks == 0,
                    f"dedicated_runtime_fallbacks={fallbacks}",
                )
            if ratio >= promotion_ratio:
                promotions.append(
                    {
                        "matrix": summary_path.parent.name,
                        "assistant": label,
                        "case": case,
                        "spec_k": row.get("spec_k"),
                        "ratio": round(ratio, 3),
                    }
                )
        elif decision.startswith("disabled_"):
            if decision == "disabled_slow":
                add_check(
                    f"{summary_path.parent.name}_{label}_{case}_slow_disabled",
                    ratio < auto_min_ratio,
                    f"ratio={ratio:.3f} correctly disabled",
                )
        else:
            add_check(f"{summary_path.parent.name}_{label}_{case}_decision", False, decision)

for summary_path in sorted(out_dir.glob("*/summary.tsv")):
    analyze_summary(summary_path)

target_jsons = sorted(out_dir.glob("target_*.json"))
target_only_skipped = any(row.get("step") == "target_only" and row.get("status") == "skip" for row in step_rows)
if target_jsons or not target_only_skipped:
    add_check("target_only_jsons", bool(target_jsons), f"{len(target_jsons)} target-only JSON files")
for path in target_jsons:
    data = json.loads(path.read_text())
    rate = to_float(data.get("decode_tok_per_s"))
    add_check(path.stem + "_rate", rate is not None and rate > 0, f"decode_tok_per_s={rate}")
    floor = target_rate_floors.get(path.stem, 0.0)
    if floor > 0:
        add_check(
            path.stem + "_rate_floor",
            rate is not None and rate >= floor,
            f"decode_tok_per_s={rate} floor={floor}",
        )

report = {
    "ok": not failures,
    "out_dir": str(out_dir),
    "thresholds": {
        "target_baseline_min_ratio": target_baseline_min_ratio,
        "auto_min_ratio": auto_min_ratio,
        "promotion_ratio": promotion_ratio,
        "target_rate_floors_tok_s": target_rate_floors,
    },
    "checks": checks,
    "warnings": warnings,
    "promotions": promotions,
    "failures": failures,
}
(out_dir / "readiness.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
with (out_dir / "readiness.tsv").open("w") as f:
    f.write("ok\tname\tdetail\n")
    for check in checks:
        f.write(f"{check['ok']}\t{check['name']}\t{check['detail']}\n")

print(f"readiness_json={out_dir / 'readiness.json'}")
print(f"readiness_tsv={out_dir / 'readiness.tsv'}")
if failures:
    print("readiness=fail")
    for failure in failures:
        print(f"failure: {failure}")
    sys.exit(1)
print("readiness=ok")
if promotions:
    print("promotions=" + json.dumps(promotions, sort_keys=True))
PY
else
  record "readiness_summary" "skip" "python3 unavailable"
fi

echo "out_dir=$OUT_DIR"
if [[ "$FAILED" != "0" ]]; then
  exit 1
fi
