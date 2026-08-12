#!/usr/bin/env bash
set -euo pipefail

# Build-only qualification for the standalone SM89 two-query-head Flash-v3
# experiment. This never mutates generated or production CUDA artifacts and
# does not create a CUDA context. GPU execution is an explicit follow-up using
# the printed harness commands.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
inference_dir="$(cd -- "${script_dir}/.." && pwd)"
repo_dir="$(cd -- "${inference_dir}/../../.." && pwd)"
output_dir="${1:-/tmp/antfly-cuda-flash-prefill-v3-gqa2}"
min_blocks_per_sm="${ANTFLY_FLASH_V3_MIN_BLOCKS_PER_SM:-0}"
cuda_root="${CUDA_HOME:-/usr/local/cuda}"
nvcc="${cuda_root}/bin/nvcc"
cuobjdump="${cuda_root}/bin/cuobjdump"
zig="${ANTFLY_ZIG:-${repo_dir}/.tools/zig-x86_64-linux-0.16.0/zig}"
cuda_source="${inference_dir}/src/ops/cuda/prototypes/gqa_flash_prefill_v3_gqa2_sm89.cu"
harness_source="${inference_dir}/src/quant_kernel_cuda_flash_prefill_prototype.zig"
summarizer="${inference_dir}/scripts/summarize_cuda_flash_prefill_v2.py"
cubin="${output_dir}/gqa_flash_prefill_v3_gqa2_sm89.cubin"
harness="${output_dir}/antfly-cuda-flash-prefill-v3-gqa2-prototype"
sass="${output_dir}/gqa_flash_prefill_v3_gqa2_sm89.sass"
resource_usage="${output_dir}/gqa_flash_prefill_v3_gqa2_sm89.resources.txt"
ptxas_log="${output_dir}/gqa_flash_prefill_v3_gqa2_sm89.ptxas.log"
canonical_cubin="${inference_dir}/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"

case "${min_blocks_per_sm}" in
    0|2) ;;
    *) echo "ANTFLY_FLASH_V3_MIN_BLOCKS_PER_SM must be 0 (unconstrained) or 2" >&2; exit 2 ;;
esac

for executable in "${nvcc}" "${cuobjdump}" "${zig}"; do
    if [[ ! -x "${executable}" ]]; then
        echo "required tool is not executable: ${executable}" >&2
        exit 1
    fi
done
for source in "${cuda_source}" "${harness_source}" "${summarizer}" "${canonical_cubin}"; do
    if [[ ! -r "${source}" ]]; then
        echo "required input is not readable: ${source}" >&2
        exit 1
    fi
done

mkdir -p "${output_dir}"

"${nvcc}" \
    --std=c++17 \
    -arch=sm_89 \
    --cubin \
    --Werror all-warnings \
    --ftz=false \
    --prec-div=true \
    --prec-sqrt=true \
    --fmad=true \
    -Xptxas=-v \
    -DANTFLY_FLASH_V3_MIN_BLOCKS_PER_SM="${min_blocks_per_sm}" \
    "${cuda_source}" \
    -o "${cubin}" \
    2>&1 | tee "${ptxas_log}"

"${cuobjdump}" --dump-sass "${cubin}" > "${sass}"
"${cuobjdump}" --dump-resource-usage "${cubin}" > "${resource_usage}"
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

echo "prototype: q16 k16 grouped_heads=2 concurrent warp partition"
if [[ "${min_blocks_per_sm}" == 0 ]]; then
    echo "launch bounds: unconstrained"
else
    echo "launch bounds: 256 threads, minimum ${min_blocks_per_sm} blocks/SM"
fi
echo "candidate dynamic shared: hd256=28356 hd512=44740 bytes"
echo "prototype cubin: ${cubin}"
echo "prototype harness: ${harness}"
echo "canonical cubin: ${canonical_cubin}"
echo "ptxas audit: ${ptxas_log}"
echo "resource audit: ${resource_usage}"
echo "SASS audit: ${sass}"
sha256sum "${cuda_source}" "${cubin}" "${canonical_cubin}"
common=(
    --candidate-cubin "${cubin}"
    --baseline-cubin "${canonical_cubin}"
    --baseline-route flash
    --candidate-query-tile 16
    --candidate-key-tile 16
    --candidate-head-group 2
    --candidate-gqa2-concurrent-layout
    --require-bitwise-candidate
)
echo "correctness command: ${harness} ${common[*]} --iterations 0 --json-out ${output_dir}/correctness.json"
echo "benchmark command: ${harness} ${common[*]} --layout identity --pattern random --iterations 20 --timing-pairs 7 --json-out ${output_dir}/timing.json"
echo "projection command: ${summarizer} ${output_dir}/timing.json --material-speedup 1.20 --require-material --json-out ${output_dir}/projection.json"
echo "GPU execution was not performed."
