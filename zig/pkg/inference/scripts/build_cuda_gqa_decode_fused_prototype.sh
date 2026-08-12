#!/usr/bin/env bash
set -euo pipefail

# Build-only entry point for the standalone SM89 exact q=1 GQA experiments,
# including the fused rejection and the two-stage coefficient/PV candidate.
# The emitted cubin is kept outside the source tree and is never wired into
# runtime dispatch.  GPU execution remains an explicit, separate step.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
inference_dir="$(cd -- "${script_dir}/.." && pwd)"
output_dir="${1:-/tmp/antfly-cuda-gqa-decode-fused-prototype}"
cuda_root="${CUDA_HOME:-/usr/local/cuda-13.2}"
nvcc="${NVCC:-${cuda_root}/bin/nvcc}"
cuobjdump="${cuda_root}/bin/cuobjdump"
python="${PYTHON:-python3}"
source_file="${inference_dir}/src/ops/cuda/prototypes/gqa_decode_fused_score_pv_sm89.cu"
harness="${inference_dir}/scripts/benchmark_cuda_gqa_decode_fused_prototype.py"
candidate_cubin="${output_dir}/gqa_decode_fused_score_pv_sm89.cubin"
canonical_cubin="${inference_dir}/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"
sass="${output_dir}/gqa_decode_fused_score_pv_sm89.sass"

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
for symbol in \
    antfly_gqa_attention_decode_fused_score_pv_hd256_swa512_f32_prototype \
    antfly_gqa_attention_decode_fused_score_pv_hd512_global_f32_prototype \
    antfly_gqa_attention_decode_score_coefficients_hd256_swa512_f32_prototype \
    antfly_gqa_attention_decode_score_coefficients_hd512_global_f32_prototype \
    antfly_gqa_attention_decode_coefficients_pv_shared_hd256_swa512_f32_prototype \
    antfly_gqa_attention_decode_coefficients_pv_shared_hd512_global_f32_prototype; do
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
sha256sum "${candidate_cubin}" "${canonical_cubin}" "${source_file}" "${harness}"
echo "qualification command: ${harness} --candidate-cubin ${candidate_cubin} --baseline-cubin ${canonical_cubin} --suite all --iterations 100 --timing-pairs 7 --json-out ${output_dir}/evidence.json"
echo "GPU execution was not performed."
