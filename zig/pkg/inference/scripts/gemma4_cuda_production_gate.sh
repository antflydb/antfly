#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: gemma4_cuda_production_gate.sh [--quick|--full|--bench-only|--mtp-only]

Runs the Gemma4 CUDA production-readiness gate for resident target inference
and TurboQuant compressed KV. MTP mode is an experimental diagnostic only; it
does not certify production readiness or claim superiority over llama.cpp.

Environment overrides:
  ZIG_BIN                       path to Zig 0.16 binary
  ANTFLY_BIN                    path to antfly-inference binary
  OUT_DIR                       output directory for logs/json timing
  E2B_MODEL                     Gemma4 E2B target model directory
  GEMMA12B_Q4_MODEL             Gemma4 12B Q4 model path/directory
  LONG_CONTEXT_TOKENS           full-mode E2B polar4 stress tokens (default: 512)
  REQUIRE_SPEED_THRESHOLDS      1 to enforce tok/s floors (default: full only)
  MIN_E2B_TEXT_TOK_S            full-mode E2B floor for real_bench_128 (default: 15.0)
  MIN_12B_Q4_TOK_S              full-mode 12B Q4 floor for 32-token runs (default: 8.0)
  RUN_RESIDENT                  auto|required|off (default: auto)
  RESIDENT_MODEL                resident server model (default: E2B_MODEL)
  RESIDENT_PROMPT               resident server prompt
  RESIDENT_TOKENS               resident server generated tokens (default: 32)
  RESIDENT_CACHE_DTYPE          resident server cache dtype (default: f32)
  RESIDENT_MODELS_DIR           models dir for resident server (default: .models)
  RESIDENT_PORT                 resident server port (default: auto)
  MAX_RESIDENT_WARM_COLD_RATIO  warm/cold E2E ratio ceiling (default: 0.75)
  MIN_RESIDENT_WARM_TOK_S       warm resident E2E tok/s floor (default: 10.0)
  RUN_MTP                       auto|required|off (default: off; --mtp-only: required)
  MTP_TARGET_MODEL              MTP target model (default: E2B_MODEL)
  MTP_DRAFT_MODEL               MTP assistant GGUF
  MTP_TOKENS                    MTP comparison tokens (default: 128)
  MTP_SPECULATIVE_K             MTP speculative window (default: 2)
  MTP_MIN_ACTIVE_SPEED_RATIO    active-MTP tok/s floor vs target-only (default: 1.0)
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
    --mtp-only)
      mode="mtp-only"
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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
inference_dir="$repo_root/zig/pkg/inference"
antfly_bin="${ANTFLY_BIN:-${ANTFY_BIN:-$inference_dir/zig-out/bin/antfly-inference}}"
out_dir="${OUT_DIR:-/tmp/antfly-gemma4-cuda-production-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out_dir"

e2b_model="${E2B_MODEL:-$repo_root/.models/unsloth/gemma-4-E2B-it-qat-GGUF}"
gemma12b_q4_model="${GEMMA12B_Q4_MODEL:-$repo_root/.models/google/gemma-4-12B-it-q4_k}"
mtp_target_model="${MTP_TARGET_MODEL:-$e2b_model}"
mtp_draft_model="${MTP_DRAFT_MODEL:-$repo_root/.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q8_0-MTP.gguf}"
mtp_prompt="${MTP_PROMPT:-Explain why database indexes improve reads but slow down writes.}"
mtp_tokens="${MTP_TOKENS:-128}"
mtp_speculative_k="${MTP_SPECULATIVE_K:-2}"
mtp_min_active_speed_ratio="${MTP_MIN_ACTIVE_SPEED_RATIO:-1.0}"
default_run_mtp="off"
if [ "$mode" = "mtp-only" ]; then
  default_run_mtp="required"
fi
run_mtp="${RUN_MTP:-$default_run_mtp}"
run_resident="${RUN_RESIDENT:-auto}"
resident_model="${RESIDENT_MODEL:-$e2b_model}"
resident_prompt="${RESIDENT_PROMPT:-Write one sentence about ants.}"
resident_tokens="${RESIDENT_TOKENS:-32}"
resident_cache_dtype="${RESIDENT_CACHE_DTYPE:-f32}"
resident_models_dir="${RESIDENT_MODELS_DIR:-$repo_root/.models}"
resident_host="${RESIDENT_HOST:-127.0.0.1}"
resident_port="${RESIDENT_PORT:-}"
resident_max_warm_cold_ratio="${MAX_RESIDENT_WARM_COLD_RATIO:-0.75}"
resident_min_warm_tok_s="${MIN_RESIDENT_WARM_TOK_S:-10.0}"
require_speed_thresholds="${REQUIRE_SPEED_THRESHOLDS:-}"
if [ -z "$require_speed_thresholds" ]; then
  if [ "$mode" = "full" ]; then
    require_speed_thresholds=1
  else
    require_speed_thresholds=0
  fi
fi

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

abs_path() {
  python3 - "$1" <<'PY'
import os
import sys

print(os.path.abspath(sys.argv[1]))
PY
}

choose_port() {
  python3 - <<'PY'
import socket

s = socket.socket()
s.bind(("127.0.0.1", 0))
try:
    print(s.getsockname()[1])
finally:
    s.close()
PY
}

resident_server_pid=""
cleanup_resident_server() {
  if [ -n "${resident_server_pid:-}" ]; then
    kill "$resident_server_pid" >/dev/null 2>&1 || true
    wait "$resident_server_pid" >/dev/null 2>&1 || true
    resident_server_pid=""
  fi
}
trap cleanup_resident_server EXIT

run_generate_json() {
  local name="$1"
  shift
  local log_file="$out_dir/$name.log"
  local json_file="$out_dir/$name.json"
  local -a args=("$@")
  log "RUN $name: ${args[*]}"
  "${args[@]}" --json-timing "$json_file" 2>&1 | tee "$log_file"
  last_json_file="$json_file"
}

write_json_table_and_check_speed() {
  local json_dir="$1"
  local require_speed="$2"
  local min_e2b="${MIN_E2B_TEXT_TOK_S:-15.0}"
  local min_12b="${MIN_12B_Q4_TOK_S:-8.0}"
  python3 - "$json_dir" "$out_dir/gemma4_cuda_timings.tsv" "$require_speed" "$min_e2b" "$min_12b" <<'PY'
import glob
import json
import os
import sys

json_dir, out_path, require_speed, min_e2b, min_12b = sys.argv[1:6]
require_speed = require_speed == "1"
min_e2b = float(min_e2b)
min_12b = float(min_12b)

rows = []
by_name = {}
for path in sorted(glob.glob(os.path.join(json_dir, "*.json"))):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    name = os.path.splitext(os.path.basename(path))[0]
    by_name[name] = data
    timing = data.get("timing_ms") or {}
    load_ms = timing.get("load_model", 0)
    warm_ttft_ms = (timing.get("prefill_inner", 0) or 0) + (timing.get("runtime_prepare_inner", 0) or 0)
    rows.append((
        name,
        str(data.get("tokens", "")),
        f"{load_ms / 1000.0:.2f}s",
        f"{warm_ttft_ms / 1000.0:.2f}s",
        f"{(load_ms + warm_ttft_ms) / 1000.0:.2f}s",
        f"{float(data.get('decode_tok_per_s', 0.0)):.3f}",
    ))

with open(out_path, "w", encoding="utf-8") as f:
    f.write("case\ttokens\tload\twarm_ttft\tcold_ttft\tdecode_tok_s\n")
    for row in rows:
        f.write("\t".join(row) + "\n")

print(f"timing_table={out_path}")
errors = []
if require_speed:
    e2b = by_name.get("real_bench_128")
    if not e2b:
        errors.append("missing real_bench_128.json for E2B speed gate")
    elif float(e2b.get("decode_tok_per_s", 0.0)) < min_e2b:
        errors.append(f"real_bench_128 decode_tok_per_s={e2b.get('decode_tok_per_s')} < {min_e2b}")

    q4 = by_name.get("gemma12b_q4_f32_32")
    if not q4:
        errors.append("missing gemma12b_q4_f32_32.json for 12B Q4 speed gate")
    elif float(q4.get("decode_tok_per_s", 0.0)) < min_12b:
        errors.append(f"gemma12b_q4_f32_32 decode_tok_per_s={q4.get('decode_tok_per_s')} < {min_12b}")

    polar_runs = [data for name, data in by_name.items() if name.startswith("gemma12b_q4_polar4_32_run")]
    if not polar_runs:
        errors.append("missing gemma12b_q4_polar4_32_run*.json for polar4 speed gate")
    else:
        avg = sum(float(data.get("decode_tok_per_s", 0.0)) for data in polar_runs) / len(polar_runs)
        if avg < min_12b:
            errors.append(f"gemma12b_q4_polar4_32 average decode_tok_per_s={avg:.3f} < {min_12b}")

if errors:
    for error in errors:
        print(error, file=sys.stderr)
    sys.exit(1)
PY
}

wait_for_resident_server() {
  local url="$1"
  local log_file="$2"
  local attempts=0
  while [ "$attempts" -lt 120 ]; do
    if python3 - "$url" <<'PY'
import sys
import urllib.request

try:
    with urllib.request.urlopen(sys.argv[1], timeout=1) as response:
        response.read()
    raise SystemExit(0)
except Exception:
    raise SystemExit(1)
PY
    then
      return 0
    fi
    if ! kill -0 "$resident_server_pid" >/dev/null 2>&1; then
      log "resident server exited before becoming ready"
      sed -n '1,220p' "$log_file" >&2 || true
      return 1
    fi
    attempts=$((attempts + 1))
    sleep 0.5
  done
  log "resident server did not become ready at $url"
  sed -n '1,220p' "$log_file" >&2 || true
  return 1
}

resident_generate_request() {
  local label="$1"
  local url="$2"
  local response_json="$3"
  python3 - "$url" "$response_json" "$resident_model" "$resident_prompt" "$resident_tokens" "$resident_cache_dtype" "$label" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

url, response_path, model, prompt, tokens, cache_dtype, label = sys.argv[1:8]
body = {
    "model": model,
    "backend": "cuda",
    "messages": [{"role": "user", "content": prompt}],
    "max_tokens": int(tokens),
    "temperature": 0,
    "stream": False,
}
if cache_dtype:
    body["cache_dtype"] = cache_dtype

payload = json.dumps(body).encode("utf-8")
request = urllib.request.Request(
    url,
    data=payload,
    headers={"content-type": "application/json"},
    method="POST",
)
start = time.monotonic()
try:
    with urllib.request.urlopen(request, timeout=300) as response:
        raw = response.read()
        status = response.status
except urllib.error.HTTPError as exc:
    raw = exc.read()
    status = exc.code
except Exception as exc:
    raise SystemExit(f"{label}: request failed: {exc}") from exc
elapsed_ms = (time.monotonic() - start) * 1000.0

with open(response_path, "wb") as f:
    f.write(raw)
if status != 200:
    text = raw.decode("utf-8", errors="replace")
    raise SystemExit(f"{label}: HTTP {status}: {text[:1000]}")

try:
    data = json.loads(raw)
except json.JSONDecodeError as exc:
    raise SystemExit(f"{label}: response was not JSON: {exc}") from exc
if data.get("error"):
    raise SystemExit(f"{label}: API error: {data.get('error')}")
usage = data.get("usage") or {}
completion_tokens = int(usage.get("completion_tokens") or 0)
if completion_tokens <= 0:
    raise SystemExit(f"{label}: completion_tokens={completion_tokens}, expected > 0")
tok_s = completion_tokens / (elapsed_ms / 1000.0) if elapsed_ms > 0 else 0.0
print(f"{label}\t{elapsed_ms:.1f}\t{completion_tokens}\t{tok_s:.3f}")
PY
}

run_resident_gate_if_enabled() {
  case "$run_resident" in
    auto|required|off)
      ;;
    *)
      log "invalid RUN_RESIDENT=$run_resident; expected auto|required|off"
      exit 2
      ;;
  esac
  if [ "$run_resident" = "off" ] || [ "$mode" = "mtp-only" ]; then
    return 0
  fi
  if [ ! -e "$resident_model" ]; then
    if [ "$run_resident" = "required" ]; then
      require_path "resident model" "$resident_model"
    fi
    log "SKIP resident_cuda_server: model missing: $resident_model"
    return 0
  fi

  resident_model="$(abs_path "$resident_model")"
  resident_models_dir="$(abs_path "$resident_models_dir")"
  if [ -z "$resident_port" ]; then
    resident_port="$(choose_port)"
  fi

  local server_log="$out_dir/resident_server.log"
  local endpoint="http://$resident_host:$resident_port/ai/v1/generate"
  local ready_url="http://$resident_host:$resident_port/healthz"
  local resident_tsv="$out_dir/resident_cuda_server.tsv"
  log "RUN resident_cuda_server: model=$resident_model cache_dtype=$resident_cache_dtype tokens=$resident_tokens port=$resident_port"
  "$antfly_bin" run \
    --host "$resident_host" \
    --port "$resident_port" \
    --models-dir "$resident_models_dir" >"$server_log" 2>&1 &
  resident_server_pid=$!
  wait_for_resident_server "$ready_url" "$server_log"

  {
    printf 'case\te2e_ms\tcompletion_tokens\te2e_tok_s\n'
    resident_generate_request resident_cold "$endpoint" "$out_dir/resident_cold.json"
    resident_generate_request resident_warm "$endpoint" "$out_dir/resident_warm.json"
  } | tee "$resident_tsv"

  python3 - "$resident_tsv" "$resident_max_warm_cold_ratio" "$resident_min_warm_tok_s" <<'PY' | tee -a "$summary"
import csv
import sys

path, max_ratio, min_warm_tok_s = sys.argv[1:4]
max_ratio = float(max_ratio)
min_warm_tok_s = float(min_warm_tok_s)
rows = {}
with open(path, "r", encoding="utf-8") as f:
    for row in csv.DictReader(f, delimiter="\t"):
        rows[row["case"]] = row

errors = []
try:
    cold_ms = float(rows["resident_cold"]["e2e_ms"])
    warm_ms = float(rows["resident_warm"]["e2e_ms"])
    warm_tok_s = float(rows["resident_warm"]["e2e_tok_s"])
except KeyError as exc:
    raise SystemExit(f"missing resident timing row: {exc}") from exc

if warm_ms >= cold_ms * max_ratio:
    errors.append(f"resident warm request {warm_ms:.1f}ms is not below cold*{max_ratio:.2f} ({cold_ms * max_ratio:.1f}ms)")
if warm_tok_s < min_warm_tok_s:
    errors.append(f"resident warm e2e_tok_s={warm_tok_s:.3f} < {min_warm_tok_s:.3f}")

if errors:
    for error in errors:
        print(error, file=sys.stderr)
    sys.exit(1)

print(
    f"PASS resident_cuda_server: cold_e2e={cold_ms / 1000.0:.2f}s "
    f"warm_e2e={warm_ms / 1000.0:.2f}s warm_e2e_tok_s={warm_tok_s:.3f}"
)
PY
  cleanup_resident_server
}

check_mtp_policy() {
  local target_json="$1"
  local mtp_json="$2"
  python3 - "$target_json" "$mtp_json" "$mtp_min_active_speed_ratio" "$mtp_tokens" <<'PY'
import json
import os
import sys

target_path, mtp_path, min_ratio, expected_tokens = sys.argv[1:5]
min_ratio = float(min_ratio)
expected_tokens = int(expected_tokens)
with open(target_path, "r", encoding="utf-8") as f:
    target = json.load(f)
with open(mtp_path, "r", encoding="utf-8") as f:
    mtp = json.load(f)

errors = []
target_tps = float(target.get("decode_tok_per_s", 0.0))
mtp_tps = float(mtp.get("decode_tok_per_s", 0.0))
target_token_ids = target.get("token_ids")
mtp_token_ids = mtp.get("token_ids")
spec = mtp.get("speculative")
if target_tps <= 0:
    errors.append(f"target decode_tok_per_s={target_tps}, expected > 0")
if mtp_tps <= 0:
    errors.append(f"mtp decode_tok_per_s={mtp_tps}, expected > 0")
if int(target.get("tokens", 0)) != expected_tokens:
    errors.append(f"target tokens={target.get('tokens')}, expected {expected_tokens}")
if int(mtp.get("tokens", 0)) != expected_tokens:
    errors.append(f"mtp tokens={mtp.get('tokens')}, expected {expected_tokens}")
if not isinstance(target_token_ids, list) or len(target_token_ids) != expected_tokens:
    errors.append(f"target token_ids length={len(target_token_ids) if isinstance(target_token_ids, list) else 'missing'}, expected {expected_tokens}")
if not isinstance(mtp_token_ids, list) or len(mtp_token_ids) != expected_tokens:
    errors.append(f"mtp token_ids length={len(mtp_token_ids) if isinstance(mtp_token_ids, list) else 'missing'}, expected {expected_tokens}")
if isinstance(target_token_ids, list) and isinstance(mtp_token_ids, list) and target_token_ids != mtp_token_ids:
    errors.append("MTP token_ids differ from target-only greedy token_ids")
if not isinstance(spec, dict):
    errors.append("MTP run did not emit speculative telemetry")
    spec = {}

if spec.get("speculation_policy") != "auto":
    errors.append(f"speculation_policy={spec.get('speculation_policy')!r}, expected auto")
if spec.get("speculation_calibration") != "probe":
    errors.append(f"speculation_calibration={spec.get('speculation_calibration')!r}, expected probe")

decision = spec.get("speculation_policy_decision")
allowed = {
    "active",
    "disabled_low_acceptance",
    "disabled_zero_match",
    "disabled_slow",
    "disabled_uncalibrated",
    "disabled_unavailable",
}
if decision not in allowed:
    errors.append(f"speculation_policy_decision={decision!r}, expected one of {sorted(allowed)}")

if spec.get("mtp_enabled"):
    if int(spec.get("rounds", 0)) <= 0:
        errors.append("mtp_enabled=true but rounds <= 0")
    if int(spec.get("drafted", 0)) <= 0:
        errors.append("mtp_enabled=true but drafted <= 0")
    cuda = mtp.get("cuda") or {}
    cuda_generate = mtp.get("cuda_generate") or {}
    device_hits = int(cuda.get("mtp_verify_commit_device_hits", 0)) + int(cuda_generate.get("mtp_verify_commit_device_hits", 0))
    device_fallbacks = int(cuda.get("mtp_verify_commit_device_fallbacks", 0)) + int(cuda_generate.get("mtp_verify_commit_device_fallbacks", 0))
    device_verify_required = os.environ.get("ANTFLY_GEMMA4_MTP_VERIFY_DEVICE_RESULT") == "1"
    if device_verify_required and device_hits <= 0:
        errors.append(f"mtp_verify_commit_device_hits={device_hits}, expected > 0")
    if device_verify_required and device_fallbacks != 0:
        errors.append(f"mtp_verify_commit_device_fallbacks={device_fallbacks}, expected 0")
    if decision == "active" and mtp_tps < target_tps * min_ratio:
        errors.append(
            f"MTP auto remained active at {mtp_tps:.3f} tok/s, below target-only floor "
            f"{target_tps * min_ratio:.3f} tok/s"
        )
elif decision == "active":
    errors.append("decision=active but mtp_enabled=false")

if errors:
    for error in errors:
        print(error, file=sys.stderr)
    sys.exit(1)

print(
    f"PASS mtp_policy: target_tok_s={target_tps:.3f} mtp_tok_s={mtp_tps:.3f} "
    f"decision={decision} acceptance_permille={spec.get('mtp_acceptance_permille')} "
    f"device_verify_hits={device_hits if spec.get('mtp_enabled') else 0}"
)
PY
}

require_path "antfly-inference binary" "$antfly_bin"

if [ "$mode" != "mtp-only" ]; then
  validate_mode="$mode"
  if [ "$mode" = "bench-only" ]; then
    validate_mode="bench-only"
  fi
  turboquant_out="$out_dir/turboquant"
  mkdir -p "$turboquant_out"
  log "RUN turboquant_gate: $validate_mode"
  OUT_DIR="$turboquant_out" \
    E2B_MODEL="$e2b_model" \
    GEMMA12B_Q4_MODEL="$gemma12b_q4_model" \
    REAL_BENCH_MODEL="$e2b_model" \
    LONG_CONTEXT_TOKENS="${LONG_CONTEXT_TOKENS:-512}" \
    "$inference_dir/scripts/validate_cuda_turboquant_gemma4.sh" "--$validate_mode" 2>&1 | tee "$out_dir/turboquant_gate.log"
  write_json_table_and_check_speed "$turboquant_out" "$require_speed_thresholds" | tee -a "$summary"
fi

run_resident_gate_if_enabled

case "$run_mtp" in
  auto|required|off)
    ;;
  *)
    log "invalid RUN_MTP=$run_mtp; expected auto|required|off"
    exit 2
    ;;
esac

last_json_file=""
if [ "$run_mtp" != "off" ]; then
  if [ ! -e "$mtp_target_model" ] || [ ! -e "$mtp_draft_model" ]; then
    if [ "$run_mtp" = "required" ]; then
      require_path "MTP target model" "$mtp_target_model"
      require_path "MTP draft model" "$mtp_draft_model"
    fi
    log "SKIP mtp_policy: target or draft model missing"
  else
    run_generate_json mtp_target_only "$antfly_bin" generate "$mtp_target_model" "$mtp_prompt" \
      --backend cuda \
      --cache-dtype f32 \
      --max-tokens "$mtp_tokens" \
      --temperature 0 \
      --raw-prompt \
      --no-chat-template \
      --ignore-eos \
      --print-token-count \
      --print-token-ids \
      --print-timing \
      --combined-budget-mb 16000 \
      --backend-budget-mb 12000 \
      --kv-budget-mb 512 \
      --scratch-budget-mb 512
    target_json="$last_json_file"

    run_generate_json mtp_auto_probe env ANTFLY_GEMMA4_MTP_PROFILE=1 "$antfly_bin" generate "$mtp_target_model" "$mtp_prompt" \
      --backend cuda \
      --draft-model "$mtp_draft_model" \
      --speculative-k "$mtp_speculative_k" \
      --speculation-policy auto \
      --speculation-calibration probe \
      --max-tokens "$mtp_tokens" \
      --temperature 0 \
      --raw-prompt \
      --no-chat-template \
      --ignore-eos \
      --print-token-count \
      --print-token-ids \
      --print-timing \
      --combined-budget-mb 16000 \
      --backend-budget-mb 12000 \
      --kv-budget-mb 512 \
      --scratch-budget-mb 512
    check_mtp_policy "$target_json" "$last_json_file" | tee -a "$summary"
  fi
fi

log "summary: $summary"
log "outputs: $out_dir"
