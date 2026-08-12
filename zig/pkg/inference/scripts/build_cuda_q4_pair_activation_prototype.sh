#!/usr/bin/env bash
set -euo pipefail

# Build-only entry point for the standalone SM89 Q4_0 pair-activation
# experiment. Outputs live under /tmp by default and are never wired into the
# canonical CUDA bundle or runtime dispatch.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
inference_dir="$(cd -- "${script_dir}/.." && pwd)"
output_dir="${1:-/tmp/antfly-cuda-q4-pair-prototype}"
cuda_root="${CUDA_HOME:-/usr/local/cuda-13.2}"
nvcc="${NVCC:-${cuda_root}/bin/nvcc}"
cuobjdump="${cuda_root}/bin/cuobjdump"
python="${PYTHON:-python3}"
source_file="${inference_dir}/src/ops/cuda/prototypes/q4_0_pair_activation_q8_1_sm89.cu"
harness="${inference_dir}/scripts/benchmark_cuda_q4_pair_activation_prototype.py"
candidate_cubin="${output_dir}/q4_pair_sm89.cubin"
canonical_cubin="${inference_dir}/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"
sass="${output_dir}/q4_pair_sm89.sass"
resources="${output_dir}/q4_pair_sm89.resources.txt"

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
for out_dim in 6144 12288; do
    for variant in \
        c4_t384_fixed \
        c4_t384_scale_broadcast \
        c4_t192_fixed \
        c8_t384_fixed; do
        symbol="antfly_q4_0_pair_activation_q8_1_e2b_${out_dim}_${variant}_prototype"
        if ! grep -q "${symbol}" "${sass}"; then
            echo "compiled cubin is missing prototype symbol: ${symbol}" >&2
            exit 1
        fi
    done
done

"${python}" -m py_compile "${harness}"
"${python}" "${harness}" --self-test
"${python}" "${harness}" --help >/dev/null

echo "candidate cubin: ${candidate_cubin}"
echo "canonical cubin: ${canonical_cubin}"
echo "prototype source: ${source_file}"
echo "differential harness: ${harness}"
echo "SASS audit: ${sass}"
echo "resource audit: ${resources}"
sha256sum "${candidate_cubin}" "${canonical_cubin}" "${source_file}" "${harness}"
echo "qualification command: ${harness} --candidate-cubin ${candidate_cubin} --baseline-cubin ${canonical_cubin} --suite all --iterations 200 --timing-pairs 7 --json-out ${output_dir}/evidence.json"
echo "GPU execution was not performed."
