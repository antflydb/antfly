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

# Paired production gate for SearchAF's long-answer path. It compares the
# compiled whole-model generation pipeline with llama.cpp using the same raw 2,003-token
# prompt, exact output length, greedy sampling, cache precision, and GPU backend.

set -euo pipefail

# Reject canonical dynamic-loader overrides before this script launches even a
# single helper process. This must stay ahead of SCRIPT_DIR resolution: a
# DYLD_INSERT_LIBRARIES override can otherwise affect dirname, pwd, or Python
# before the later provenance checks get a chance to fail closed.
early_allow_noncanonical_policy=false
case "${ALLOW_NONCANONICAL_POLICY:-0}" in
  1|[Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn])
    early_allow_noncanonical_policy=true
    ;;
esac
if [[ "$early_allow_noncanonical_policy" != true ]]; then
  for loader_name in ${!DYLD_@}; do
    echo "canonical benchmark loader override is set: $loader_name" >&2
    exit 2
  done
  if [[ -n "${LD_LIBRARY_PATH+x}" ]]; then
    echo "canonical benchmark loader override is set: LD_LIBRARY_PATH" >&2
    exit 2
  fi
  if [[ -n "${LD_PRELOAD+x}" ]]; then
    echo "canonical benchmark loader override is set: LD_PRELOAD" >&2
    exit 2
  fi
  for git_name in ${!GIT_@}; do
    echo "canonical benchmark Git override is set: $git_name" >&2
    exit 2
  done
fi

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/inference_cli.sh
source "$SCRIPT_DIR/inference_cli.sh"

MODEL="${MODEL:-$HOME/.antfly/inference/models/google/gemma-4-E4B-it-qat-q4_0-gguf}"
ANTFLY_BIN_OVERRIDE="${ANTFLY_BIN:-}"
LLAMA_CPP_BIN="${LLAMA_CPP_BIN:-llama-completion}"
LLAMA_CPP_BUNDLE_ROOT="${LLAMA_CPP_BUNDLE_ROOT:-}"
ZIG_BIN="${ZIG_BIN:-zig}"
EXPECTED_LLAMA_CPP_BUILD="${EXPECTED_LLAMA_CPP_BUILD:-10182}"
EXPECTED_LLAMA_CPP_SHA256="${EXPECTED_LLAMA_CPP_SHA256:-${LLAMA_CPP_EXPECTED_SHA256:-}}"
EXPECTED_LLAMA_CPP_BUNDLE_SHA256="${EXPECTED_LLAMA_CPP_BUNDLE_SHA256:-}"
CANONICAL_LLAMA_CPP_BUILD="10182"
CANONICAL_LLAMA_CPP_SHA256="faa8b1c2a6c69f50b0fcec71af86eda757d34f78bbbddbb3f485f170bc586d2f"
CANONICAL_LLAMA_CPP_BUNDLE_SHA256="23e601e646bbd901c4d4f1c1158fd4c99053d08969e6aa07f2005e87dc05a1fc"
CANONICAL_PROMPT_SHA256="1c0477d5acd34e3c76c1db35506df4f5eb66e59084efaf3aa36d8a2fe515a01f"
EXPECTED_TOKEN_IDS_SHA256="${EXPECTED_TOKEN_IDS_SHA256:-}"
EXPECTED_PROMPT_TOKEN_IDS_SHA256="${EXPECTED_PROMPT_TOKEN_IDS_SHA256:-}"
PROMPT_OVERRIDE_SET="${PROMPT+x}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-gemma4-metal-long-output-$(date -u +%Y%m%dT%H%M%SZ)}"
OUTPUT_TOKENS="${OUTPUT_TOKENS:-300}"
PROMPT_REPEAT="${PROMPT_REPEAT:-36}"
WARMUPS="${WARMUPS:-1}"
WARMUP_OUTPUT_TOKENS="${WARMUP_OUTPUT_TOKENS:-4}"
RUNS="${RUNS:-6}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-45}"
MAX_TOTAL_RATIO="${MAX_TOTAL_RATIO:-0.98}"
MIN_DECODE_RATIO="${MIN_DECODE_RATIO:-1.02}"
MAX_CV="${MAX_CV:-0.03}"
ANTFLY_CACHE_DTYPE="${ANTFLY_CACHE_DTYPE:-f16}"
LLAMA_CACHE_TYPE_K="${LLAMA_CACHE_TYPE_K:-f16}"
LLAMA_CACHE_TYPE_V="${LLAMA_CACHE_TYPE_V:-f16}"
LLAMA_CONTEXT_SIZE="${LLAMA_CONTEXT_SIZE:-4096}"
EXPECT_GENERATED_FLASH_PREFILL_CALLS="${EXPECT_GENERATED_FLASH_PREFILL_CALLS:-35}"
EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS="${EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS:-7}"
EXPECT_PREFILL_DIRECT_KV="${EXPECT_PREFILL_DIRECT_KV:-0}"
EXPECT_FAST_PREPARED_FRAME="${EXPECT_FAST_PREPARED_FRAME:-1}"
EXPECT_Q4_0_MMV_VARIANT="${EXPECT_Q4_0_MMV_VARIANT:-nr4-nsg2}"
EXPECT_SWA_SCAN_CLAMP="${EXPECT_SWA_SCAN_CLAMP:-1}"
EXPECT_ANTFLY_METAL_DEVICE="${EXPECT_ANTFLY_METAL_DEVICE:-Apple M4}"
EXPECT_LLAMA_METAL_DEVICE="${EXPECT_LLAMA_METAL_DEVICE:-Apple M4}"
EXPECT_LLAMA_OFFLOADED_LAYERS="${EXPECT_LLAMA_OFFLOADED_LAYERS:-43}"
REQUIRE_CONFIDENCE="${REQUIRE_CONFIDENCE:-1}"
ALLOW_NONCANONICAL_POLICY="${ALLOW_NONCANONICAL_POLICY:-0}"

export EXPECT_GENERATED_FLASH_PREFILL_CALLS
export EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS
export EXPECT_PREFILL_DIRECT_KV
export EXPECT_FAST_PREPARED_FRAME
export EXPECT_Q4_0_MMV_VARIANT
export EXPECT_SWA_SCAN_CLAMP
export EXPECT_ANTFLY_METAL_DEVICE
export EXPECT_LLAMA_METAL_DEVICE
export EXPECT_LLAMA_OFFLOADED_LAYERS
export TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE=1

for value in "$OUTPUT_TOKENS" "$WARMUP_OUTPUT_TOKENS" "$PROMPT_REPEAT" "$RUNS"; do
  case "$value" in
    ''|*[!0-9]*|0)
      echo "OUTPUT_TOKENS, WARMUP_OUTPUT_TOKENS, PROMPT_REPEAT, and RUNS must be positive integers" >&2
      exit 2
      ;;
  esac
done
if (( RUNS % 2 != 0 )); then
  echo "RUNS must be even so the measured execution order is balanced" >&2
  exit 2
fi
for value in "$WARMUPS" "$COOLDOWN_SECONDS"; do
  case "$value" in
    ''|*[!0-9]*)
      echo "WARMUPS and COOLDOWN_SECONDS must be non-negative integers" >&2
      exit 2
      ;;
  esac
done

for pin_name in EXPECTED_LLAMA_CPP_SHA256 EXPECTED_LLAMA_CPP_BUNDLE_SHA256 EXPECTED_TOKEN_IDS_SHA256 EXPECTED_PROMPT_TOKEN_IDS_SHA256; do
  pin_value="${!pin_name}"
  if [[ ! "$pin_value" =~ ^[0-9A-Fa-f]{64}$ ]]; then
    echo "$pin_name must be an explicit 64-character SHA-256 pin" >&2
    exit 2
  fi
done
for variant_spec in \
  "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT=${TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT:-auto}" \
  "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT=${TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT:-auto}"; do
  variant_name="${variant_spec%%=*}"
  variant_value="${variant_spec#*=}"
  variant_value_normalized="$(printf '%s' "$variant_value" | tr '[:upper:]' '[:lower:]')"
  if [[ -n "$variant_value_normalized" && "$variant_value_normalized" != "auto" ]]; then
    echo "$variant_name must be auto or unset for the pinned baseline comparator" >&2
    exit 2
  fi
done

require_confidence=false
require_confidence_normalized="$(printf '%s' "$REQUIRE_CONFIDENCE" | tr '[:upper:]' '[:lower:]')"
case "$require_confidence_normalized" in
  1|true|yes|on)
    require_confidence=true
    ;;
  0|false|no|off)
    ;;
  *)
    echo "REQUIRE_CONFIDENCE must be a boolean (0/1, false/true, no/yes, off/on)" >&2
    exit 2
    ;;
esac

allow_noncanonical_policy=false
allow_noncanonical_policy_normalized="$(printf '%s' "$ALLOW_NONCANONICAL_POLICY" | tr '[:upper:]' '[:lower:]')"
case "$allow_noncanonical_policy_normalized" in
  1|true|yes|on)
    allow_noncanonical_policy=true
    ;;
  0|false|no|off)
    ;;
  *)
    echo "ALLOW_NONCANONICAL_POLICY must be a boolean (0/1, false/true, no/yes, off/on)" >&2
    exit 2
    ;;
esac

if [[ "$allow_noncanonical_policy" != true ]]; then
  while IFS='=' read -r policy_name _; do
    case "$policy_name" in
      TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE)
        ;;
      DYLD_*|LD_LIBRARY_PATH|LD_PRELOAD)
        echo "canonical benchmark loader override is set: $policy_name" >&2
        exit 2
        ;;
      TERMITE_*|ANTFLY_GEMMA4_*|ANTFLY_INFERENCE_*|LLAMA_ARG_*|LLAMA_LOG_*|GGML_*)
        echo "noncanonical benchmark policy environment is set: $policy_name; use the A/B runner or set ALLOW_NONCANONICAL_POLICY=1 for an explicitly exploratory artifact" >&2
        exit 2
        ;;
    esac
  done < <(env)

  if [[ -n "$PROMPT_OVERRIDE_SET" ]]; then
    echo "canonical benchmark contract violation: PROMPT must be unset so the checked-in canonical prompt builder is used" >&2
    exit 2
  fi
  if [[ "$EXPECTED_LLAMA_CPP_BUILD" != "$CANONICAL_LLAMA_CPP_BUILD" ]]; then
    echo "canonical benchmark contract violation: EXPECTED_LLAMA_CPP_BUILD must be $CANONICAL_LLAMA_CPP_BUILD" >&2
    exit 2
  fi
  if [[ "$(printf '%s' "$EXPECTED_LLAMA_CPP_SHA256" | tr '[:upper:]' '[:lower:]')" != "$CANONICAL_LLAMA_CPP_SHA256" ]]; then
    echo "canonical benchmark contract violation: EXPECTED_LLAMA_CPP_SHA256 must match the approved b$CANONICAL_LLAMA_CPP_BUILD comparator" >&2
    exit 2
  fi
  if [[ "$(printf '%s' "$EXPECTED_LLAMA_CPP_BUNDLE_SHA256" | tr '[:upper:]' '[:lower:]')" != "$CANONICAL_LLAMA_CPP_BUNDLE_SHA256" ]]; then
    echo "canonical benchmark contract violation: EXPECTED_LLAMA_CPP_BUNDLE_SHA256 must match the approved b$CANONICAL_LLAMA_CPP_BUILD bundle" >&2
    exit 2
  fi
  if [[ "$PROMPT_REPEAT" != "36" ]]; then
    echo "canonical benchmark contract violation: PROMPT_REPEAT must be 36" >&2
    exit 2
  fi
  if [[ "$(printf '%s' "$EXPECTED_PROMPT_TOKEN_IDS_SHA256" | tr '[:upper:]' '[:lower:]')" != "d882b403c0229eb7ffc70ff2539123283996548d5eb67a4ef34db619be6e8a42" ]]; then
    echo "canonical benchmark contract violation: EXPECTED_PROMPT_TOKEN_IDS_SHA256 must match the approved 2,003-token prompt digest" >&2
    exit 2
  fi
  if [[ "$(printf '%s' "$EXPECTED_TOKEN_IDS_SHA256" | tr '[:upper:]' '[:lower:]')" != "711ddb9890d0fd867d7cd9c1ce10fe4c407a2ec597464fe42912a0802afe7052" ]]; then
    echo "canonical benchmark contract violation: EXPECTED_TOKEN_IDS_SHA256 must match the approved 300-token Antfly digest" >&2
    exit 2
  fi
  for expectation in \
    "EXPECT_GENERATED_FLASH_PREFILL_CALLS=35" \
    "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS=7" \
    "EXPECT_PREFILL_DIRECT_KV=0" \
    "EXPECT_FAST_PREPARED_FRAME=1" \
    "EXPECT_Q4_0_MMV_VARIANT=nr4-nsg2" \
    "EXPECT_SWA_SCAN_CLAMP=1" \
    "EXPECT_ANTFLY_METAL_DEVICE=Apple M4" \
    "EXPECT_LLAMA_METAL_DEVICE=Apple M4" \
    "EXPECT_LLAMA_OFFLOADED_LAYERS=43"; do
    expectation_name="${expectation%%=*}"
    expectation_value="${expectation#*=}"
    if [[ "${!expectation_name}" != "$expectation_value" ]]; then
      echo "canonical benchmark contract violation: $expectation_name must be $expectation_value" >&2
      exit 2
    fi
  done

  canonical_contract_error="$(python3 - \
    "$OUTPUT_TOKENS" "$RUNS" "$WARMUPS" "$COOLDOWN_SECONDS" \
    "$MAX_TOTAL_RATIO" "$MIN_DECODE_RATIO" "$MAX_CV" "$require_confidence" \
    "$ANTFLY_CACHE_DTYPE" "$LLAMA_CACHE_TYPE_K" "$LLAMA_CACHE_TYPE_V" \
    "$LLAMA_CONTEXT_SIZE" <<'PY'
import math
import sys

output_tokens, runs, warmups, cooldown = map(int, sys.argv[1:5])
try:
    max_total, min_decode, max_cv = map(float, sys.argv[5:8])
except ValueError:
    print("canonical thresholds must be finite numbers")
    raise SystemExit(0)
require_confidence = sys.argv[8] == "true"
cache_types = tuple(value.lower() for value in sys.argv[9:12])
try:
    context_size = int(sys.argv[12])
except ValueError:
    print("canonical LLAMA_CONTEXT_SIZE must be 4096")
    raise SystemExit(0)

errors = []
if output_tokens != 300:
    errors.append("OUTPUT_TOKENS must be 300")
if runs < 6 or runs % 2:
    errors.append("RUNS must be an even integer of at least 6")
if warmups < 1:
    errors.append("WARMUPS must be at least 1")
if cooldown < 45:
    errors.append("COOLDOWN_SECONDS must be at least 45")
if not all(math.isfinite(value) and value > 0 for value in (max_total, min_decode, max_cv)):
    errors.append("MAX_TOTAL_RATIO, MIN_DECODE_RATIO, and MAX_CV must be positive finite numbers")
else:
    if max_total > 0.98:
        errors.append("MAX_TOTAL_RATIO must be at most 0.98")
    if min_decode < 1.02:
        errors.append("MIN_DECODE_RATIO must be at least 1.02")
    if max_cv > 0.03:
        errors.append("MAX_CV must be at most 0.03")
if not require_confidence:
    errors.append("REQUIRE_CONFIDENCE must be enabled")
if cache_types != ("f16", "f16", "f16"):
    errors.append("Antfly and llama.cpp KV cache types must all be f16")
if context_size != 4096:
    errors.append("LLAMA_CONTEXT_SIZE must be 4096")
print("; ".join(errors))
PY
)"
  if [[ -n "$canonical_contract_error" ]]; then
    echo "canonical benchmark contract violation: $canonical_contract_error; set ALLOW_NONCANONICAL_POLICY=1 only for an explicitly exploratory artifact" >&2
    exit 2
  fi
fi

repo_root="$(cd -P "$SCRIPT_DIR/../../../.." && pwd -P)"
git_toplevel="$(git -C "$repo_root" rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$git_toplevel" ]]; then
  echo "cannot resolve repository root from benchmark script: $repo_root" >&2
  exit 2
fi
git_toplevel="$(cd -P "$git_toplevel" && pwd -P)"
if [[ "$git_toplevel" != "$repo_root" ]]; then
  echo "benchmark repository root mismatch: script=$repo_root git=$git_toplevel" >&2
  exit 2
fi
OUT_DIR="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$OUT_DIR")"
case "$OUT_DIR" in
  "$repo_root"|"$repo_root"/*)
    echo "OUT_DIR must be outside the repository so benchmark artifacts cannot contaminate Git provenance: $OUT_DIR" >&2
    exit 2
    ;;
esac
if [[ -e "$OUT_DIR" && ! -d "$OUT_DIR" ]]; then
  echo "OUT_DIR exists and is not a directory: $OUT_DIR" >&2
  exit 2
fi
if [[ -d "$OUT_DIR" ]]; then
  shopt -s nullglob dotglob
  out_dir_entries=("$OUT_DIR"/*)
  shopt -u nullglob dotglob
  if (( ${#out_dir_entries[@]} != 0 )); then
    echo "OUT_DIR must be new or empty; refusing to reuse nonempty directory: $OUT_DIR" >&2
    exit 2
  fi
else
  mkdir -p "$OUT_DIR"
fi

resolve_text_gguf() {
  local model="$1"
  if [[ -f "$model" ]]; then
    printf '%s\n' "$model"
    return
  fi
  find "$model" -maxdepth 1 -type f -name '*.gguf' ! -name '*mmproj*' | sort | head -n 1
}

GGUF="${GGUF:-$(resolve_text_gguf "$MODEL")}"
if [[ ! -e "$MODEL" || -z "$GGUF" || ! -f "$GGUF" ]]; then
  echo "missing Gemma4 model or text GGUF: $MODEL" >&2
  exit 2
fi
MODEL="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$MODEL")"
GGUF="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$GGUF")"
ANTFLY_BIN_RESOLVED="$(resolve_antfly_inference_bin)"
if [[ ! -x "$ANTFLY_BIN_RESOLVED" ]]; then
  echo "Antfly inference binary is not executable: $ANTFLY_BIN_RESOLVED" >&2
  exit 2
fi
ANTFLY_BIN_RESOLVED="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$ANTFLY_BIN_RESOLVED")"
LLAMA_CPP_BIN_RESOLVED="$(command -v "$LLAMA_CPP_BIN" 2>/dev/null || true)"
if [[ -z "$LLAMA_CPP_BIN_RESOLVED" || ! -x "$LLAMA_CPP_BIN_RESOLVED" ]]; then
  echo "llama-completion not found: $LLAMA_CPP_BIN" >&2
  exit 2
fi
LLAMA_CPP_BIN_RESOLVED="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$LLAMA_CPP_BIN_RESOLVED")"
if [[ -z "$LLAMA_CPP_BUNDLE_ROOT" ]]; then
  echo "LLAMA_CPP_BUNDLE_ROOT must name the extracted, immutable llama.cpp comparator bundle" >&2
  exit 2
fi
LLAMA_CPP_BUNDLE_ROOT="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$LLAMA_CPP_BUNDLE_ROOT")"
if [[ ! -d "$LLAMA_CPP_BUNDLE_ROOT" ]]; then
  echo "llama.cpp bundle root is not a directory: $LLAMA_CPP_BUNDLE_ROOT" >&2
  exit 2
fi
case "$LLAMA_CPP_BIN_RESOLVED" in
  "$LLAMA_CPP_BUNDLE_ROOT"/*)
    ;;
  *)
    echo "llama.cpp comparator must be inside LLAMA_CPP_BUNDLE_ROOT: $LLAMA_CPP_BIN_RESOLVED" >&2
    exit 2
    ;;
esac
case "$OUT_DIR" in
  "$LLAMA_CPP_BUNDLE_ROOT"|"$LLAMA_CPP_BUNDLE_ROOT"/*)
    echo "OUT_DIR must be outside LLAMA_CPP_BUNDLE_ROOT: $OUT_DIR" >&2
    exit 2
    ;;
esac
llama_binary_file_type="$(/usr/bin/file -b "$LLAMA_CPP_BIN_RESOLVED" 2>/dev/null || true)"
if [[ "$llama_binary_file_type" == Mach-O* ]]; then
  llama_loader_audit_mode="dyld_print_libraries_preflight"
elif [[ "$allow_noncanonical_policy" == true ]]; then
  llama_loader_audit_mode="skipped_non_macho_noncanonical"
else
  echo "canonical llama.cpp comparator must be a Mach-O executable, got: $llama_binary_file_type" >&2
  exit 2
fi
if [[ "$allow_noncanonical_policy" != true ]]; then
  python3 -I "$SCRIPT_DIR/gemma4_metal_long_output.py" validate-git-worktree \
    --repo-root "$repo_root"
fi
ZIG_BIN_RESOLVED="$(command -v "$ZIG_BIN" 2>/dev/null || true)"
if [[ -z "$ZIG_BIN_RESOLVED" || ! -x "$ZIG_BIN_RESOLVED" ]]; then
  echo "Zig executable not found: $ZIG_BIN" >&2
  exit 2
fi
ZIG_BIN_RESOLVED="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$ZIG_BIN_RESOLVED")"
if ! zig_version_output="$("$ZIG_BIN_RESOLVED" version 2>&1)"; then
  echo "Zig version probe failed: $ZIG_BIN_RESOLVED" >&2
  exit 2
fi
if [[ -z "$zig_version_output" ]]; then
  echo "Zig version probe returned empty output: $ZIG_BIN_RESOLVED" >&2
  exit 2
fi

PROMPT="${PROMPT:-}"
if [[ -z "$PROMPT" ]]; then
  PROMPT="$(python3 "$SCRIPT_DIR/gemma4_metal_long_output.py" render-prompt --repeat "$PROMPT_REPEAT")"
fi
model_sha256="$(shasum -a 256 "$GGUF" | awk '{print $1}')"
git_revision="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf unknown)"
git_status="$(LC_ALL=C git -C "$repo_root" status --porcelain=v1 --untracked-files=all)"
git_dirty=false
if [[ -n "$git_status" ]]; then
  git_dirty=true
fi
git_tracked_diff_sha256="$(LC_ALL=C git -C "$repo_root" diff --binary --no-ext-diff HEAD -- | shasum -a 256 | awk '{print $1}')"
git_status_sha256="$(LC_ALL=C git -C "$repo_root" status --porcelain=v1 --untracked-files=all | shasum -a 256 | awk '{print $1}')"
prompt_path="$OUT_DIR/prompt.txt"
printf '%s' "$PROMPT" >"$prompt_path"
prompt_sha256="$(shasum -a 256 "$prompt_path" | awk '{print $1}')"
if [[ "$allow_noncanonical_policy" != true && "$prompt_sha256" != "$CANONICAL_PROMPT_SHA256" ]]; then
  echo "canonical benchmark contract violation: canonical prompt byte SHA-256 changed: expected $CANONICAL_PROMPT_SHA256, got $prompt_sha256" >&2
  exit 2
fi
benchmark_harness_sha256="$(shasum -a 256 "$SCRIPT_DIR/benchmark_metal_gemma4_long_output.sh" | awk '{print $1}')"
benchmark_parser_sha256="$(shasum -a 256 "$SCRIPT_DIR/gemma4_metal_long_output.py" | awk '{print $1}')"
antfly_binary_sha256="$(shasum -a 256 "$ANTFLY_BIN_RESOLVED" | awk '{print $1}')"
llama_binary_sha256="$(shasum -a 256 "$LLAMA_CPP_BIN_RESOLVED" | awk '{print $1}')"
llama_bundle_manifest_path="$OUT_DIR/llama-cpp-bundle-manifest.json"
llama_bundle_sha256="$(python3 "$SCRIPT_DIR/gemma4_metal_long_output.py" bundle-manifest \
  --root "$LLAMA_CPP_BUNDLE_ROOT" --output "$llama_bundle_manifest_path")"
llama_version_output="$("$LLAMA_CPP_BIN_RESOLVED" --version 2>&1 || true)"
if [[ -z "$llama_version_output" ]]; then
  echo "llama.cpp comparator returned empty --version output: $LLAMA_CPP_BIN_RESOLVED" >&2
  exit 2
fi
expected_llama_cpp_sha256_normalized="$(printf '%s' "$EXPECTED_LLAMA_CPP_SHA256" | tr '[:upper:]' '[:lower:]')"
expected_llama_cpp_bundle_sha256_normalized="$(printf '%s' "$EXPECTED_LLAMA_CPP_BUNDLE_SHA256" | tr '[:upper:]' '[:lower:]')"
if [[ -n "$expected_llama_cpp_sha256_normalized" && "$llama_binary_sha256" != "$expected_llama_cpp_sha256_normalized" ]]; then
  echo "llama.cpp binary SHA-256 mismatch: expected $expected_llama_cpp_sha256_normalized, got $llama_binary_sha256" >&2
  exit 2
fi
if [[ "$llama_bundle_sha256" != "$expected_llama_cpp_bundle_sha256_normalized" ]]; then
  echo "llama.cpp bundle SHA-256 mismatch: expected $expected_llama_cpp_bundle_sha256_normalized, got $llama_bundle_sha256" >&2
  exit 2
fi
python3 - "$OUT_DIR/metadata.json" "$git_revision" "$MODEL" "$GGUF" "$model_sha256" \
  "$OUTPUT_TOKENS" "$PROMPT_REPEAT" "$RUNS" "$WARMUPS" "$COOLDOWN_SECONDS" \
  "$LLAMA_CPP_BIN" "$LLAMA_CPP_BIN_RESOLVED" "$llama_version_output" "$llama_binary_sha256" \
  "$EXPECTED_LLAMA_CPP_BUILD" "$expected_llama_cpp_sha256_normalized" "$ANTFLY_BIN_RESOLVED" \
  "$antfly_binary_sha256" "$prompt_sha256" "$ANTFLY_CACHE_DTYPE" "$LLAMA_CACHE_TYPE_K" \
  "$LLAMA_CACHE_TYPE_V" "$LLAMA_CONTEXT_SIZE" "$WARMUP_OUTPUT_TOKENS" "$git_dirty" \
  "$git_tracked_diff_sha256" "$git_status_sha256" "$benchmark_harness_sha256" \
  "$benchmark_parser_sha256" "$ZIG_BIN" "$ZIG_BIN_RESOLVED" "$zig_version_output" \
  "$require_confidence" "$EXPECTED_TOKEN_IDS_SHA256" "$EXPECTED_PROMPT_TOKEN_IDS_SHA256" \
  "$allow_noncanonical_policy" "$repo_root" "$LLAMA_CPP_BUNDLE_ROOT" \
  "$llama_bundle_sha256" "$expected_llama_cpp_bundle_sha256_normalized" \
  "$llama_binary_file_type" "$llama_loader_audit_mode" \
  "$MAX_TOTAL_RATIO" "$MIN_DECODE_RATIO" "$MAX_CV" "$PROMPT_OVERRIDE_SET" <<'PY'
import json
import os
import platform
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
version_output = sys.argv[13]
version_match = re.search(r"\bversion:\s*(\d+)\s*\(([0-9a-fA-F]+)\)", version_output)
if not version_match:
    raise SystemExit(f"unrecognized llama.cpp --version output: {version_output!r}")
build = int(version_match.group(1))
commit = version_match.group(2).lower()
expected_build = int(sys.argv[15])
if build != expected_build:
    raise SystemExit(f"llama.cpp build mismatch: expected b{expected_build}, got b{build}")
known_full_commits = {
    (10182, "afeebe103"): "afeebe103bd99cda8f5dfaefcabadf890db7fda7",
}
path.write_text(json.dumps({
    "schema": "antfly.gemma4_metal_long_output.metadata.v4",
    "repo_root": sys.argv[37],
    "git_revision": sys.argv[2],
    "git_revision_end": None,
    "git_dirty": sys.argv[25] == "true",
    "git_tracked_diff_sha256": sys.argv[26],
    "git_tracked_diff_sha256_end": None,
    "git_status_sha256": sys.argv[27],
    "git_status_sha256_end": None,
    "benchmark_harness_sha256": sys.argv[28],
    "benchmark_parser_sha256": sys.argv[29],
    "zig_bin": sys.argv[30],
    "zig_resolved_bin": sys.argv[31],
    "zig_version": sys.argv[32],
    "model": sys.argv[3],
    "gguf": sys.argv[4],
    "antfly_model_argument": sys.argv[4],
    "llama_model_argument": sys.argv[4],
    "gguf_sha256": sys.argv[5],
    "host": platform.platform(),
    "output_tokens": int(sys.argv[6]),
    "prompt_repeat": int(sys.argv[7]),
    "prompt_override_set": bool(sys.argv[46]),
    "prompt_file": "prompt.txt",
    "runs": int(sys.argv[8]),
    "warmups": int(sys.argv[9]),
    "cooldown_seconds": int(sys.argv[10]),
    "max_total_ratio": float(sys.argv[43]),
    "min_decode_ratio": float(sys.argv[44]),
    "max_cv": float(sys.argv[45]),
    "prompt_sha256": sys.argv[19],
    "llama_cpp_bin": sys.argv[11],
    "llama_cpp_resolved_bin": sys.argv[12],
    "llama_cpp_version": f"version: {build} ({commit})",
    "llama_cpp_version_output": version_output,
    "llama_cpp_build": build,
    "llama_cpp_commit": commit,
    "llama_cpp_full_commit": known_full_commits.get((build, commit)),
    "llama_cpp_binary_sha256": sys.argv[14],
    "llama_cpp_expected_build": expected_build,
    "llama_cpp_expected_sha256": sys.argv[16] or None,
    "llama_cpp_bundle_root": sys.argv[38],
    "llama_cpp_bundle_manifest_file": "llama-cpp-bundle-manifest.json",
    "llama_cpp_bundle_sha256": sys.argv[39],
    "llama_cpp_expected_bundle_sha256": sys.argv[40],
    "llama_cpp_binary_file_type": sys.argv[41],
    "llama_cpp_loader_audit_mode": sys.argv[42],
    "llama_cpp_comparator_id": f"llama.cpp-b{build}-{commit}-{sys.argv[39][:12]}",
    "antfly_bin": sys.argv[17],
    "antfly_binary_sha256": sys.argv[18],
    "antfly_route": "compiled_whole_model",
    "antfly_cache_dtype": sys.argv[20],
    "llama_cache_type_k": sys.argv[21],
    "llama_cache_type_v": sys.argv[22],
    "llama_context_size": int(sys.argv[23]),
    "warmup_output_tokens": int(sys.argv[24]),
    "execution_order_file": "execution-order.jsonl",
    "llama_prompt_preflight_file": "llama-prompt-preflight.log",
    "llama_prompt_preflight_validation_file": "llama-prompt-preflight-validation.json",
    "require_confidence": sys.argv[33] == "true",
    "expected_token_ids_sha256": sys.argv[34].lower(),
    "expected_prompt_token_ids_sha256": sys.argv[35].lower(),
    "allow_noncanonical_policy": sys.argv[36] == "true",
    "canonical_policy": sys.argv[36] != "true",
    "policy_environment_prefixes": [
        "TERMITE_", "ANTFLY_GEMMA4_", "ANTFLY_INFERENCE_",
        "LLAMA_ARG_", "LLAMA_LOG_", "GGML_",
    ],
    "process_policy_env": {
        name: value
        for name, value in sorted(os.environ.items())
        if name.startswith((
            "TERMITE_", "ANTFLY_GEMMA4_", "ANTFLY_INFERENCE_",
            "LLAMA_ARG_", "LLAMA_LOG_", "GGML_",
        ))
    },
    "git_environment_prefixes": ["GIT_"],
    "process_git_env": {
        name: value
        for name, value in sorted(os.environ.items())
        if name.startswith("GIT_")
    },
    "canonical_untracked_policy": "reject",
    "canonical_submodule_policy": "clean_and_pinned",
    "loader_environment_prefixes": ["DYLD_"],
    "loader_environment_names": ["LD_LIBRARY_PATH", "LD_PRELOAD"],
    "process_loader_env": {
        name: value
        for name, value in sorted(os.environ.items())
        if name.startswith("DYLD_") or name in ("LD_LIBRARY_PATH", "LD_PRELOAD")
    },
    "runner_injected_env": {
        "TERMITE_GEN_STAGE_DEBUG": "1",
        "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE": "1",
        **(
            {"DYLD_PRINT_LIBRARIES": "1 for unmeasured llama prompt preflight only"}
            if sys.argv[42] == "dyld_print_libraries_preflight"
            else {}
        ),
    },
    "split_gqa_enable": os.environ.get("TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT"),
    "split_gqa_disable": os.environ.get("TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT"),
    "pipelined_decode_frame_enable": os.environ.get("TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME"),
    "pipelined_decode_frame_disable": os.environ.get("TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME"),
    "metal_policy_env": {
        name: os.environ.get(name)
        for name in (
            "TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD",
            "TERMITE_METAL_DISABLE_PREFILL_SG_DIRECT_LOAD",
            "TERMITE_METAL_Q4_0_MMV_VARIANT",
            "TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO",
            "TERMITE_METAL_TRACE_Q4_0_MMV_VARIANT",
            "TERMITE_METAL_DISABLE_SWA_SCAN_CLAMP",
            "TERMITE_METAL_DISABLE_FAST_PREPARED_FRAME",
            "TERMITE_METAL_FORCE_DIAGNOSTIC_COMMAND_BUFFERS",
            "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT",
            "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT",
            "TERMITE_METAL_TRACE_DECODE_GQA_SPLIT_SCHEDULE",
            "EXPECT_GENERATED_FLASH_PREFILL_CALLS",
            "EXPECT_GENERATED_FLASH_PREFILL_HD512_CALLS",
            "EXPECT_PREFILL_DIRECT_KV",
            "EXPECT_FAST_PREPARED_FRAME",
            "EXPECT_Q4_0_MMV_VARIANT",
            "EXPECT_SWA_SCAN_CLAMP",
            "EXPECT_ANTFLY_METAL_DEVICE",
            "EXPECT_LLAMA_METAL_DEVICE",
            "EXPECT_LLAMA_OFFLOADED_LAYERS",
        )
    },
}, indent=2) + "\n")
PY

execution_order_path="$OUT_DIR/execution-order.jsonl"
execution_sequence=0

llama_common_args=(
  -m "$GGUF"
  --no-conversation
  --no-jinja
  --special
  -f "$prompt_path"
  --no-escape
  -c "$LLAMA_CONTEXT_SIZE"
  -b 512
  -ub 512
  -t 4
  -tb 4
  -ngl 999
  -dev MTL0
  -fit off
  -fa auto
  --repack
  --mmap
  --no-context-shift
  -lv 4
  --log-colors off
  -ctk "$LLAMA_CACHE_TYPE_K"
  -ctv "$LLAMA_CACHE_TYPE_V"
  --temp 0
  --repeat-penalty 1.0
  --presence-penalty 0
  --frequency-penalty 0
  --dry-multiplier 0
  --ignore-eos
  --no-display-prompt
)

record_execution() {
  local phase="$1"
  local sample="$2"
  local implementation="$3"
  execution_sequence=$((execution_sequence + 1))
  printf '{"sequence":%d,"phase":"%s","sample":%d,"implementation":"%s"}\n' \
    "$execution_sequence" "$phase" "$sample" "$implementation" >>"$execution_order_path"
}

run_antfly_binary() {
  if [[ "$(basename "$ANTFLY_BIN_RESOLVED")" == "antfly" ]]; then
    "$ANTFLY_BIN_RESOLVED" inference "$@"
  else
    "$ANTFLY_BIN_RESOLVED" "$@"
  fi
}

cooldown() {
  if (( COOLDOWN_SECONDS > 0 )); then
    sleep "$COOLDOWN_SECONDS"
  fi
}

run_antfly() {
  local sample="$1"
  local output_tokens="${2:-$OUTPUT_TOKENS}"
  local json="$OUT_DIR/antfly-$sample.json"
  local log="$OUT_DIR/antfly-$sample.log"
  TERMITE_GEN_STAGE_DEBUG=1 \
  run_antfly_binary generate "$GGUF" "$PROMPT" \
    --backend metal \
    --mode compiled \
    --compiled-target whole-model \
    --max-tokens "$output_tokens" \
    --temperature 0 \
    --raw-prompt \
    --ignore-eos \
    --cache-dtype "$ANTFLY_CACHE_DTYPE" \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    --print-prompt-token-ids \
    --print-timing \
    --json-timing "$json" >"$log" 2>&1
}

run_llama() {
  local sample="$1"
  local output_tokens="${2:-$OUTPUT_TOKENS}"
  local log="$OUT_DIR/llama-$sample.log"
  "$LLAMA_CPP_BIN_RESOLVED" "${llama_common_args[@]}" -n "$output_tokens" >"$log" 2>&1
}

if [[ "$llama_loader_audit_mode" == "dyld_print_libraries_preflight" ]]; then
  env DYLD_PRINT_LIBRARIES=1 \
    "$LLAMA_CPP_BIN_RESOLVED" "${llama_common_args[@]}" -n 1 --verbose-prompt \
    >"$OUT_DIR/llama-prompt-preflight.log" 2>&1
else
  "$LLAMA_CPP_BIN_RESOLVED" "${llama_common_args[@]}" -n 1 --verbose-prompt \
    >"$OUT_DIR/llama-prompt-preflight.log" 2>&1
fi

preflight_expected_tokens=0
if [[ "$allow_noncanonical_policy" != true ]]; then
  preflight_expected_tokens=2003
fi
python3 "$SCRIPT_DIR/gemma4_metal_long_output.py" validate-preflight \
  --log "$OUT_DIR/llama-prompt-preflight.log" \
  --expected-prompt-token-ids-sha256 "$(printf '%s' "$EXPECTED_PROMPT_TOKEN_IDS_SHA256" | tr '[:upper:]' '[:lower:]')" \
  --expected-prompt-tokens "$preflight_expected_tokens" \
  --loader-audit-mode "$llama_loader_audit_mode" \
  --bundle-root "$LLAMA_CPP_BUNDLE_ROOT" \
  --comparator-binary "$LLAMA_CPP_BIN_RESOLVED" \
  --output "$OUT_DIR/llama-prompt-preflight-validation.json"

for ((i = 1; i <= WARMUPS; i++)); do
  run_antfly "warmup-$i" "$WARMUP_OUTPUT_TOKENS"
  record_execution warmup "$i" antfly
  cooldown
  run_llama "warmup-$i" "$WARMUP_OUTPUT_TOKENS"
  record_execution warmup "$i" llama
  cooldown
done

for ((i = 1; i <= RUNS; i++)); do
  if (( i % 2 == 1 )); then
    run_antfly "$i"
    record_execution measured "$i" antfly
    cooldown
    run_llama "$i"
    record_execution measured "$i" llama
  else
    run_llama "$i"
    record_execution measured "$i" llama
    cooldown
    run_antfly "$i"
    record_execution measured "$i" antfly
  fi
  cooldown
done

git_revision_end="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf unknown)"
git_tracked_diff_sha256_end="$(LC_ALL=C git -C "$repo_root" diff --binary --no-ext-diff HEAD -- | shasum -a 256 | awk '{print $1}')"
git_status_sha256_end="$(LC_ALL=C git -C "$repo_root" status --porcelain=v1 --untracked-files=all | shasum -a 256 | awk '{print $1}')"
python3 - "$OUT_DIR/metadata.json" "$git_revision_end" \
  "$git_tracked_diff_sha256_end" "$git_status_sha256_end" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
metadata = json.loads(path.read_text())
metadata["git_revision_end"] = sys.argv[2]
metadata["git_tracked_diff_sha256_end"] = sys.argv[3]
metadata["git_status_sha256_end"] = sys.argv[4]
path.write_text(json.dumps(metadata, indent=2) + "\n")
PY

parser_args=(
  python3 "$SCRIPT_DIR/gemma4_metal_long_output.py"
  summarize
  --out-dir "$OUT_DIR"
  --runs "$RUNS"
  --output-tokens "$OUTPUT_TOKENS"
  --max-total-ratio "$MAX_TOTAL_RATIO"
  --min-decode-ratio "$MIN_DECODE_RATIO"
  --max-cv "$MAX_CV"
  --expected-token-ids-sha256 "$EXPECTED_TOKEN_IDS_SHA256"
)
if [[ "$require_confidence" == true ]]; then
  parser_args+=(--require-confidence)
fi

"${parser_args[@]}"

echo "output: $OUT_DIR"
