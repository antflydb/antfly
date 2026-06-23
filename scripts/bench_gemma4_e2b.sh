#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/bench_gemma4_e2b.sh

Environment:
  E2B_TARGET=.models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf
  E2B_ASSISTANT_Q8=.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q8_0-MTP.gguf
  E2B_ASSISTANT_Q4=.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q4_0-MTP.gguf
  OUT_DIR=/tmp/gemma4-e2b-mtp-bench-<timestamp>
  MODE=production|no_fallback|measurement|profile|profile_sync
  PROMPT_FILTER="ants_chat code_chat completion_raw punct_raw"
  SPEC_KS="1 2 4"
  MAX_TOKENS=32

All other environment variables supported by scripts/bench_gemma4_mtp.sh are
forwarded after applying E2B-sized CUDA budget defaults. Default paths resolve
under the repository root.
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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export E2B_TARGET="${E2B_TARGET:-$ROOT_DIR/.models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf}"
export E2B_ASSISTANT_Q8="${E2B_ASSISTANT_Q8:-$ROOT_DIR/.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q8_0-MTP.gguf}"
export E2B_ASSISTANT_Q4="${E2B_ASSISTANT_Q4:-$ROOT_DIR/.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/gemma-4-E2B-it-Q4_0-MTP.gguf}"
export BIN="${BIN:-$ROOT_DIR/zig/pkg/inference/zig-out/bin/antfly-inference}"

export OUT_DIR="${OUT_DIR:-/tmp/gemma4-e2b-mtp-bench-$(date +%Y%m%d-%H%M%S)}"
export MODE="${MODE:-production}"
export PROMPT_FILTER="${PROMPT_FILTER:-ants_chat code_chat completion_raw punct_raw}"
export SPEC_KS="${SPEC_KS:-1 2 4}"
export MAX_TOKENS="${MAX_TOKENS:-32}"
export RUN_TIMEOUT="${RUN_TIMEOUT:-360s}"
export TEMP_CACHE_MB="${TEMP_CACHE_MB:-512}"
export COMBINED_BUDGET_MB="${COMBINED_BUDGET_MB:-12000}"
export BACKEND_BUDGET_MB="${BACKEND_BUDGET_MB:-9000}"
export KV_BUDGET_MB="${KV_BUDGET_MB:-512}"
export SCRATCH_BUDGET_MB="${SCRATCH_BUDGET_MB:-1024}"

exec "$ROOT_DIR/scripts/bench_gemma4_mtp.sh" "$E2B_TARGET" "$E2B_ASSISTANT_Q8" "$E2B_ASSISTANT_Q4"
