#!/usr/bin/env bash
set -euo pipefail

# Build-only qualification check for the standalone SM89 paged-GQA
# flash-prefill prototype. Outputs stay outside the source tree and no CUDA
# context or GPU workload is created. Run the emitted harness explicitly after
# review; it loads the candidate and canonical cubins as separate modules.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
inference_dir="$(cd -- "${script_dir}/.." && pwd)"
repo_dir="$(cd -- "${inference_dir}/../../.." && pwd)"
output_dir="${1:-/tmp/antfly-cuda-flash-prefill-stage1}"
cuda_root="${CUDA_HOME:-/usr/local/cuda}"
nvcc="${cuda_root}/bin/nvcc"
cuobjdump="${cuda_root}/bin/cuobjdump"
zig="${ANTFLY_ZIG:-${repo_dir}/.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig}"
cuda_source="${inference_dir}/src/ops/cuda/prototypes/gqa_flash_prefill_f16_sm89.cu"
harness_source="${inference_dir}/src/quant_kernel_cuda_flash_prefill_prototype.zig"
cubin="${output_dir}/gqa_flash_prefill_f16_sm89.cubin"
harness="${output_dir}/antfly-cuda-flash-prefill-prototype"
sass="${output_dir}/gqa_flash_prefill_f16_sm89.sass"
canonical_cubin="${inference_dir}/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"

if [[ ! -x "${nvcc}" ]]; then
    echo "nvcc not found or not executable: ${nvcc}" >&2
    exit 1
fi
if [[ ! -x "${cuobjdump}" ]]; then
    echo "cuobjdump not found or not executable: ${cuobjdump}" >&2
    exit 1
fi
if [[ ! -x "${zig}" ]]; then
    echo "zig not found or not executable: ${zig}" >&2
    exit 1
fi
if [[ ! -r "${canonical_cubin}" ]]; then
    echo "canonical SM89 cubin not found or not readable: ${canonical_cubin}" >&2
    exit 1
fi

mkdir -p "${output_dir}"

"${nvcc}" \
    --std=c++17 \
    -arch=sm_89 \
    --cubin \
    --Werror all-warnings \
    -Xptxas=-v \
    "${cuda_source}" \
    -o "${cubin}"

"${cuobjdump}" --dump-sass "${cubin}" > "${sass}"
if ! grep -q "HMMA\.16816\.F32" "${sass}"; then
    echo "compiled prototype does not contain the required HMMA instructions" >&2
    exit 1
fi

"${zig}" test \
    -lc \
    "${harness_source}" \
    -O ReleaseSafe \
    --cache-dir "${output_dir}/zig-cache" \
    --global-cache-dir "${output_dir}/zig-global-cache"

"${zig}" build-exe \
    -lc \
    "${harness_source}" \
    -O ReleaseSafe \
    -femit-bin="${harness}" \
    --cache-dir "${output_dir}/zig-cache" \
    --global-cache-dir "${output_dir}/zig-global-cache"

echo "prototype cubin: ${cubin}"
echo "prototype harness: ${harness}"
echo "canonical cubin: ${canonical_cubin}"
echo "SASS audit: ${sass}"
sha256sum "${cubin}" "${canonical_cubin}"
echo "qualification command: ${harness} --candidate-cubin ${cubin} --baseline-cubin ${canonical_cubin} --iterations 20 --timing-pairs 7 --json-out ${output_dir}/evidence.json"
echo "GPU execution was not performed."
