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

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"

default_models_root="${ANTFLY_INFERENCE_MODELS_DIR:-${HOME:+$HOME/.antfly/inference/models}}"
model_dir="${ANTFLY_GLINER2_MODEL_DIR:-${default_models_root:+$default_models_root/antflydb/gliner2-base-v1}}"
score_tolerance="${ANTFLY_GLINER2_SCORE_TOLERANCE:-0.01}"
command_timeout="${ANTFLY_GLINER2_COMMAND_TIMEOUT:-1200}"
warmup_iters="${ANTFLY_GLINER2_WARMUP_ITERS:-1}"
measure_iters="${ANTFLY_GLINER2_MEASURE_ITERS:-1}"
verify_generated_tc="${ANTFLY_GLINER2_VERIFY_GENERATED_TC:-0}"
verify_materialized_auto="${ANTFLY_GLINER2_VERIFY_MATERIALIZED_AUTO:-auto}"
cuda_artifacts="${ANTFLY_CUDA_ARTIFACTS:-fatbin}"
cuda_libraries="${ANTFLY_CUDA_LIBS:-auto}"
optimize="${ANTFLY_CUDA_VERIFY_OPTIMIZE:-ReleaseFast}"
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
  echo "set ANTFLY_GLINER2_MODEL_DIR, or set ANTFLY_INFERENCE_MODELS_DIR/HOME for the default model location" >&2
  exit 1
fi
if [[ ! -d "$model_dir" ]]; then
  echo "missing GLiNER model directory at $model_dir" >&2
  echo "note: a relative ANTFLY_GLINER2_MODEL_DIR resolves against the current working directory ($PWD)" >&2
  exit 1
fi
model_dir="$(cd "$model_dir" && pwd -P)"
cd "$repo_root"
if [[ ! -f "$model_dir/gliner_config.json" ]]; then
  echo "missing GLiNER config at $model_dir/gliner_config.json" >&2
  exit 1
fi
if [[ ! -f "$model_dir/gliner2-encoder.Q4_K.gguf" && ! -f "$model_dir/gliner2-encoder.gguf" ]]; then
  echo "missing GLiNER encoder weights under $model_dir" >&2
  exit 1
fi
if [[ ! -f "$model_dir/gliner2-head.Q4_K.gguf" && ! -f "$model_dir/gliner2-head.gguf" && ! -f "$model_dir/gliner_head.gguf" ]]; then
  echo "missing GLiNER head weights under $model_dir" >&2
  exit 1
fi

zig_bin="$(resolve_zig)"

python3 - "$zig_bin" "$model_dir" "$score_tolerance" "$command_timeout" "$warmup_iters" "$measure_iters" "$verify_generated_tc" "$verify_materialized_auto" "$cuda_artifacts" "$cuda_libraries" "$optimize" "$zig_global_cache_dir" <<'PY'
import csv
import io
import math
import os
import re
import subprocess
import sys

zig_bin, model_dir, tolerance_raw, timeout_raw, warmup_iters, measure_iters, verify_generated_tc_raw, verify_materialized_auto_raw, cuda_artifacts, cuda_libraries, optimize, zig_global_cache_dir = sys.argv[1:13]
tolerance = float(tolerance_raw)
timeout = float(timeout_raw)

def parse_switch(raw, name, allow_auto=False):
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    if allow_auto and value == "auto":
        return None
    raise SystemExit(f"{name} must be one of 0/1, false/true" + (", or auto" if allow_auto else ""))

def default_cuda_device_is_sm89():
    try:
        output = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=index,uuid,compute_cap", "--format=csv,noheader,nounits"],
            stderr=subprocess.STDOUT,
            text=True,
            timeout=min(timeout, 15.0),
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    by_index = {}
    by_uuid = {}
    for line in output.splitlines():
        fields = [field.strip() for field in line.split(",")]
        if len(fields) == 3:
            by_index[fields[0]] = fields[2]
            by_uuid[fields[1]] = fields[2]
    visible = os.environ.get("CUDA_VISIBLE_DEVICES", "").strip()
    if not visible:
        return by_index.get("0") == "8.9"
    # CUDA device zero maps to the first entry of an explicit visibility
    # mask. Container schedulers commonly pass GPU-<uuid> entries instead of
    # numeric indices, so resolve both forms.
    entry = visible.split(",", 1)[0].strip()
    if entry in by_index:
        return by_index[entry] == "8.9"
    if entry in by_uuid:
        return by_uuid[entry] == "8.9"
    # MIG ids can never resolve to SM89 today (L4 has no MIG), and negative
    # or malformed masks expose no device zero. Say so instead of skipping
    # silently: a quietly-disabled gate on qualification hardware is how a
    # materialized-route regression ships green.
    print(
        f"case=distinct_b8_s256 materialized_auto_detect=unresolved cuda_visible_devices_entry={entry}",
        flush=True,
    )
    return False

verify_generated_tc = parse_switch(
    verify_generated_tc_raw,
    "ANTFLY_GLINER2_VERIFY_GENERATED_TC",
)
materialized_setting = parse_switch(
    verify_materialized_auto_raw,
    "ANTFLY_GLINER2_VERIFY_MATERIALIZED_AUTO",
    allow_auto=True,
)
verify_materialized_auto = default_cuda_device_is_sm89() if materialized_setting is None else materialized_setting
if materialized_setting is None and not verify_materialized_auto:
    print("case=distinct_b8_s256 materialized_auto=skipped reason=default_cuda_device_not_detected_as_sm89", flush=True)

cases = [
    {
        "name": "organizations_locations",
        "text": "John Smith works for Apple Inc. in San Francisco. Apple Inc. is based in Cupertino.",
        "labels": ["person", "organization", "location"],
    },
    {
        "name": "technical_entities",
        "text": "NVIDIA released CUDA 13.2 for Blackwell and Hopper production inference workloads.",
        "labels": ["company", "product", "hardware", "software"],
    },
]

def print_subprocess_output(exc):
    output = getattr(exc, "output", None)
    if not output:
        return
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    sys.stderr.write(output)
    if not output.endswith("\n"):
        sys.stderr.write("\n")

def checked_output(cmd, env=None):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=timeout, env=env)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print_subprocess_output(exc)
        raise

def run_case(backend, case, attention_mode=None, batch_size=1, expected_encoder_seq_len=None, dump_entities=True):
    cmd = [
        zig_bin,
        "build",
        "--global-cache-dir",
        zig_global_cache_dir,
        "-Dcuda=true",
        f"-Dcuda-artifacts={cuda_artifacts}",
        f"-Dcuda-libs={cuda_libraries}",
        f"-Doptimize={optimize}",
        "bench-gliner2-e2e",
        "--",
        "--model-dir",
        model_dir,
        "--backend",
        backend,
        "--task",
        "entities",
        "--batch-size",
        str(batch_size),
        "--warmup-iters",
        warmup_iters,
        "--measure-iters",
        measure_iters,
        "--format",
        "csv",
    ]
    if "text_batch_file" in case:
        cmd.extend(["--text-batch-file", case["text_batch_file"]])
    elif "text_file" in case:
        cmd.extend(["--text-file", case["text_file"]])
    else:
        cmd.extend(["--text", case["text"]])
    if expected_encoder_seq_len is not None:
        cmd.extend(["--expect-encoder-seq-len", str(expected_encoder_seq_len)])
    if dump_entities:
        cmd.append("--dump-entities")
    for label in case["labels"]:
        cmd.extend(["--label", label])

    env = None
    if attention_mode is not None:
        env = dict(os.environ)
        env["ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE"] = attention_mode
        if attention_mode == "generated-tc":
            env["ANTFLY_INFERENCE_CUDA_DEBERTA_GENERATED_TC_VARIANT"] = "m32"
    raw = checked_output(cmd, env)
    csv_lines = []
    entities = []
    entity_pattern = re.compile(
        r'^entities\[(\d+)\]\[(\d+)\]: label=(.*?) span=(\d+)\.\.(\d+) score=([-+0-9.eE]+) text="(.*)"$'
    )
    keep = False
    for line in raw.splitlines():
        match = entity_pattern.match(line)
        if match and int(match.group(1)) == 0:
            entities.append({
                "label": match.group(3),
                "start": int(match.group(4)),
                "end": int(match.group(5)),
                "score": float(match.group(6)),
                "text": match.group(7),
            })
        if line.startswith("task,mode,"):
            csv_lines = [line]
            keep = True
            continue
        if keep and line.startswith("entities,"):
            csv_lines.append(line)
    if len(csv_lines) < 2:
        raise RuntimeError(f"{case['name']} {backend}: no CSV rows found:\n{raw}")
    rows = list(csv.DictReader(io.StringIO("\n".join(csv_lines))))
    warm = next((row for row in rows if row["mode"] == "warm_loaded_session"), rows[-1])
    entity_count = int(warm["entity_count"])
    score_sum = float(warm["score_sum"])
    if entity_count < 0 or not math.isfinite(score_sum):
        raise AssertionError(f"{case['name']} {backend}: invalid row {warm}")
    generated_calls = int(warm.get("cuda_deberta_generated_tc_attention_calls", "0") or 0)
    generated_m32_calls = int(warm.get("cuda_deberta_generated_tc_m32_attention_calls", "0") or 0)
    generated_fallbacks = int(warm.get("cuda_deberta_generated_tc_attention_fallbacks", "0") or 0)
    materialized_calls = int(warm.get("cuda_deberta_materialized_f16_attention_calls", "0") or 0)
    materialized_fallbacks = int(warm.get("cuda_deberta_materialized_f16_attention_fallbacks", "0") or 0)
    materialized_workspace_rejections = int(warm.get("cuda_deberta_materialized_workspace_rejections", "0") or 0)
    # Sum across every recorded row (first_run + warm samples): the cuBLASLt
    # heuristic is exercised freshly on the first run, so a first-touch
    # failure must fail the gate even when warm samples recover.
    f16_cublaslt_fallbacks = sum(int(row.get("cuda_f16_cublaslt_fallbacks", "0") or 0) for row in rows)
    f16_scalar_linear_calls = sum(int(row.get("cuda_f16_scalar_linear_calls", "0") or 0) for row in rows)
    if attention_mode == "generated-tc" and generated_calls <= 0:
        raise AssertionError(f"{case['name']} generated-tc: fused tensor-core route was not used: {warm}")
    if attention_mode == "generated-tc" and generated_m32_calls != generated_calls:
        raise AssertionError(f"{case['name']} generated-tc: requested M32 schedule silently fell back: {warm}")
    if attention_mode == "generated-tc" and generated_fallbacks != 0:
        raise AssertionError(f"{case['name']} generated-tc: route recorded {generated_fallbacks} fallback(s): {warm}")
    if backend == "cuda" and (f16_cublaslt_fallbacks != 0 or f16_scalar_linear_calls != 0):
        raise AssertionError(
            f"{case['name']} cuda: qualified shape used FP16 scalar fallback across first/warm rows: "
            f"cublaslt_fallbacks={f16_cublaslt_fallbacks} scalar_linear_calls={f16_scalar_linear_calls}"
        )
    entities.sort(key=lambda entity: (entity["start"], entity["end"], entity["label"], entity["text"]))
    return {
        "entity_count": entity_count,
        "score_sum": score_sum,
        "generated_calls": generated_calls,
        "generated_m32_calls": generated_m32_calls,
        "generated_fallbacks": generated_fallbacks,
        "materialized_calls": materialized_calls,
        "materialized_fallbacks": materialized_fallbacks,
        "materialized_workspace_rejections": materialized_workspace_rejections,
        "entities": entities,
    }

def verify_entities(case_name, reference, candidate, route):
    reference_keys = [(e["label"], e["start"], e["end"], e["text"]) for e in reference["entities"]]
    candidate_keys = [(e["label"], e["start"], e["end"], e["text"]) for e in candidate["entities"]]
    if reference_keys != candidate_keys:
        raise SystemExit(f"{case_name}: native/{route} entity identities differ: {reference_keys} vs {candidate_keys}")
    for expected, actual in zip(reference["entities"], candidate["entities"]):
        score_diff = abs(expected["score"] - actual["score"])
        if score_diff > tolerance:
            raise SystemExit(
                f"{case_name}: native/{route} score diff {score_diff:.8f} exceeds tolerance {tolerance:.8f} for {expected}"
            )

for case in cases:
    native = run_case("native", case)
    cuda = run_case("cuda", case)
    score_diff = abs(native["score_sum"] - cuda["score_sum"])
    print(
        f"case={case['name']} native_entities={native['entity_count']} cuda_entities={cuda['entity_count']} score_diff={score_diff:.8f}",
        flush=True,
    )
    if native["entity_count"] != cuda["entity_count"]:
        raise SystemExit(f"{case['name']}: native/cuda entity counts differ: {native} vs {cuda}")
    verify_entities(case["name"], native, cuda, "cuda")
    if score_diff > tolerance:
        raise SystemExit(f"{case['name']}: native/cuda score diff {score_diff:.8f} exceeds tolerance {tolerance:.8f}")
    if verify_generated_tc:
        generated = run_case("cuda", case, "generated-tc")
        generated_diff = abs(native["score_sum"] - generated["score_sum"])
        print(
            f"case={case['name']} generated_tc_entities={generated['entity_count']} generated_tc_score_diff={generated_diff:.8f} generated_tc_calls={generated['generated_calls']}",
            flush=True,
        )
        if native["entity_count"] != generated["entity_count"]:
            raise SystemExit(f"{case['name']}: native/generated-tc entity counts differ: {native} vs {generated}")
        verify_entities(case["name"], native, generated, "generated-tc")
        if generated_diff > tolerance:
            raise SystemExit(f"{case['name']}: native/generated-tc score diff {generated_diff:.8f} exceeds tolerance {tolerance:.8f}")

qualification_case = {
    "name": "distinct_b8_s256",
    "text_batch_file": "scripts/gliner2/fixtures/gliner2_256_distinct_b8.txt",
    "labels": ["person", "organization", "location", "date", "money"],
}
if verify_materialized_auto:
    production = run_case(
        "cuda",
        qualification_case,
        batch_size=8,
        expected_encoder_seq_len=256,
        dump_entities=False,
    )
    if production["materialized_calls"] <= 0:
        raise SystemExit(f"distinct B8 S256: production auto did not use materialized attention: {production}")
    if production["materialized_fallbacks"] != 0 or production["materialized_workspace_rejections"] != 0:
        raise SystemExit(f"distinct B8 S256: production materialized attention fell back or rejected workspace: {production}")
    print(
        f"case=distinct_b8_s256 materialized_calls={production['materialized_calls']} materialized_fallbacks={production['materialized_fallbacks']} workspace_rejections={production['materialized_workspace_rejections']}",
        flush=True,
    )

if verify_generated_tc:
    generated_b8 = run_case(
        "cuda",
        qualification_case,
        "generated-tc",
        batch_size=8,
        expected_encoder_seq_len=256,
        dump_entities=False,
    )
    print(
        f"case=distinct_b8_s256 generated_tc_calls={generated_b8['generated_calls']} generated_tc_m32_calls={generated_b8['generated_m32_calls']} generated_tc_fallbacks={generated_b8['generated_fallbacks']}",
        flush=True,
    )

print("gliner2 E2E native/cuda parity completed", flush=True)
PY
