#!/usr/bin/env bash
set -euo pipefail

# Build-only entry point for the standalone SM89 exact Q6_K x Q8_1 LM-head
# argmax experiments.  The emitted cubin stays outside the source tree and is
# never wired into runtime dispatch.  GPU execution is a separate explicit
# qualification step.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
inference_dir="$(cd -- "${script_dir}/.." && pwd)"
output_dir="${1:-/tmp/antfly-cuda-q6-k-q8-1-lm-head-argmax-prototype}"
cuda_root="${CUDA_HOME:-/usr/local/cuda-13.2}"
nvcc="${NVCC:-${cuda_root}/bin/nvcc}"
cuobjdump="${cuda_root}/bin/cuobjdump"
python="${PYTHON:-python3}"
source_file="${inference_dir}/src/ops/cuda/prototypes/q6_k_q8_1_lm_head_argmax_sm89.cu"
harness="${inference_dir}/scripts/benchmark_cuda_q6_k_q8_1_lm_head_argmax_prototype.py"
candidate_cubin="${output_dir}/q6_k_q8_1_lm_head_argmax_sm89.cubin"
canonical_cubin="${inference_dir}/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"
sass="${output_dir}/q6_k_q8_1_lm_head_argmax_sm89.sass"
resources="${output_dir}/q6_k_q8_1_lm_head_argmax_sm89.resources.txt"

for executable in "${nvcc}" "${cuobjdump}" "${python}"; do
    if [[ ! -x "${executable}" ]] && ! command -v "${executable}" >/dev/null 2>&1; then
        echo "required executable is unavailable: ${executable}" >&2
        exit 1
    fi
done
for input in "${source_file}" "${harness}" "${canonical_cubin}"; do
    if [[ ! -r "${input}" ]]; then
        echo "required input is unavailable: ${input}" >&2
        exit 1
    fi
done

mkdir -p "${output_dir}"
"${nvcc}" \
    --std=c++17 \
    -arch=sm_89 \
    --cubin \
    --Werror all-warnings \
    -Xptxas=-v \
    "${source_file}" \
    -o "${candidate_cubin}"

"${cuobjdump}" --dump-sass "${candidate_cubin}" > "${sass}"
"${cuobjdump}" --dump-resource-usage "${candidate_cubin}" > "${resources}"
for symbol in \
    antfly_q6_k_q8_1_lm_head_argmax_persistent_tile8_sm89_prototype \
    antfly_q6_k_q8_1_lm_head_argmax_persistent_tile16_sm89_prototype \
    antfly_q6_k_q8_1_lm_head_argmax_persistent_tile32_sm89_prototype \
    antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_tile8_sm89_prototype \
    antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_tile16_sm89_prototype \
    antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_dedicated_tile8_sm89_prototype; do
    if ! grep -q "${symbol}" "${sass}"; then
        echo "compiled cubin is missing prototype symbol: ${symbol}" >&2
        exit 1
    fi
done

"${python}" -m py_compile "${harness}"
"${python}" "${harness}" --help >/dev/null

echo "candidate cubin: ${candidate_cubin}"
echo "canonical cubin: ${canonical_cubin}"
echo "prototype source: ${source_file}"
echo "differential harness: ${harness}"
echo "SASS audit: ${sass}"
echo "resource audit: ${resources}"
sha256sum "${candidate_cubin}" "${canonical_cubin}" "${source_file}" "${harness}" "${resources}"
echo "qualification command: ${harness} --candidate-cubin ${candidate_cubin} --baseline-cubin ${canonical_cubin} --suite all --iterations 100 --timing-pairs 7 --json-out ${output_dir}/evidence.json"
echo "GPU execution was not performed."
