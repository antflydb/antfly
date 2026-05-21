#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
termite_dir="$(cd "${script_dir}/.." && pwd)"
cd "${termite_dir}"

out_root="${TERMITE_CUDA_FINETUNE_SMOKE_OUT:-/tmp/termite-cuda-finetune-smoke}"
global_cache="${ZIG_GLOBAL_CACHE_DIR:-${out_root}/zig-global-cache}"
local_cache="${ZIG_LOCAL_CACHE_DIR:-${out_root}/zig-local-cache}"
mkdir -p "${out_root}" "${global_cache}" "${local_cache}"

echo "checking CUDA source/PTX artifacts"
export TERMITE_CUDA_REQUIRE_PTXAS="${TERMITE_CUDA_REQUIRE_PTXAS:-1}"
scripts/check_cuda_artifacts.sh

system_blas="${TERMITE_CUDA_SYSTEM_BLAS:-auto}"
case "${system_blas}" in
  auto)
    if [[ "$(uname -s)" == "Darwin" ]]; then
      system_blas=true
    elif [[ -n "${TERMITE_CUDA_BLAS_ROOT:-}" ]]; then
      system_blas=true
    else
      system_blas=false
    fi
    ;;
  1|true|TRUE|yes|YES)
    system_blas=true
    ;;
  0|false|FALSE|no|NO)
    system_blas=false
    ;;
  *)
    echo "invalid TERMITE_CUDA_SYSTEM_BLAS=${TERMITE_CUDA_SYSTEM_BLAS}" >&2
    exit 1
    ;;
esac

cuda_build_flags=(
  -Dshared-lib-root=../..
  -Dmlx=false
  -Dmetal=false
  -Donnx=false
  -Dcuda=true
  -Dcuda-artifacts=portable
  -Dsystem-blas="${system_blas}"
  -Dskip-openapi=true
)
if [[ -n "${TERMITE_CUDA_BLAS_ROOT:-}" ]]; then
  cuda_build_flags+=(-Dblas-root="${TERMITE_CUDA_BLAS_ROOT}")
fi

echo "building CUDA-enabled termite test binary"
ZIG_GLOBAL_CACHE_DIR="${global_cache}" ZIG_LOCAL_CACHE_DIR="${local_cache}" \
  zig build test-bin "${cuda_build_flags[@]}"

echo "running CUDA-enabled finetune tests"
ZIG_GLOBAL_CACHE_DIR="${global_cache}" ZIG_LOCAL_CACHE_DIR="${local_cache}" \
  zig build test-finetune "${cuda_build_flags[@]}"

echo "building CUDA-enabled termite CLI"
ZIG_GLOBAL_CACHE_DIR="${global_cache}" ZIG_LOCAL_CACHE_DIR="${local_cache}" \
  zig build "${cuda_build_flags[@]}"

termite_bin="zig-out/bin/termite"
if [[ ! -x "${termite_bin}" ]]; then
  echo "missing built termite CLI: ${termite_bin}" >&2
  exit 1
fi

cuda_info_json="${out_root}/cuda-info-smoke.json"
echo "running CUDA runtime/kernel preflight"
if ! "${termite_bin}" cuda-info --json --smoke > "${cuda_info_json}" 2>&1; then
  cat "${cuda_info_json}" >&2
  exit 1
fi
if command -v jq >/dev/null 2>&1; then
  jq -e '.cuda.runtime_available == true and .cuda.smoke.ok == true' "${cuda_info_json}" >/dev/null
else
  grep -q '"runtime_available":true' "${cuda_info_json}"
  grep -q '"smoke":{"requested":true,"ok":true' "${cuda_info_json}"
fi

strict_cuda=1
smoke_args=(finetune smoke-fast --out-root "${out_root}/fast-smoke" --require-cuda)
if [[ "${TERMITE_CUDA_STRICT:-1}" != "0" ]]; then
  smoke_args+=(--strict-cuda)
  export TERMITE_CUDA_ALLOW_HOST_TRAINING_FALLBACKS=0
else
  strict_cuda=0
fi

echo "running CUDA finetune smoke"
"${termite_bin}" "${smoke_args[@]}"

summary_json="${out_root}/fast-smoke/fast_smoke_summary.json"
cuda_execute_cases=(
  qwen35_sft_execute
  gemma4_lora_sft_execute
  qwen2_dpo_execute
  qwen2_grpo_execute
  gemma4_dpo_execute
  gemma4_grpo_execute
)

if [[ "${strict_cuda}" == "1" ]]; then
  echo "checking strict CUDA finetune reports"
  if command -v jq >/dev/null 2>&1; then
    jq -e '.status == "succeeded" and .require_cuda == true and .strict_cuda == true and .cuda.host_training_fallbacks_allowed == false' "${summary_json}" >/dev/null
    for case_name in "${cuda_execute_cases[@]}"; do
      jq -e '.metadata.backend.resolved == "cuda"' "${out_root}/fast-smoke/${case_name}/training_report.json" >/dev/null
    done
  else
    grep -q '"status": "succeeded"' "${summary_json}"
    grep -q '"require_cuda": true' "${summary_json}"
    grep -q '"strict_cuda": true' "${summary_json}"
    grep -q '"host_training_fallbacks_allowed": false' "${summary_json}"
    for case_name in "${cuda_execute_cases[@]}"; do
      grep -q '"resolved": "cuda"' "${out_root}/fast-smoke/${case_name}/training_report.json"
    done
  fi
fi

echo "CUDA runtime preflight passed: ${cuda_info_json}"
echo "CUDA finetune smoke passed: ${summary_json}"
