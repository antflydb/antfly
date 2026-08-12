#!/usr/bin/env bash
set -euo pipefail

# Build-only qualification for the standalone SM89 GQA-native Flash-prefill
# v2 tile experiment. This script does not mutate generated/production CUDA
# artifacts and does not create a CUDA context. The emitted harness compares a
# separately loaded candidate cubin with the checked-in qualified Flash-v1
# cubin and an independent scalar reference.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
inference_dir="$(cd -- "${script_dir}/.." && pwd)"
repo_dir="$(cd -- "${inference_dir}/../../.." && pwd)"
query_tile="${ANTFLY_FLASH_V2_QUERY_TILE:-32}"
key_tile="${ANTFLY_FLASH_V2_KEY_TILE:-32}"
head_group="${ANTFLY_FLASH_V2_HEAD_GROUP:-1}"
tile_tag="q${query_tile}k${key_tile}g${head_group}"
output_dir="${1:-/tmp/antfly-cuda-flash-prefill-v2-${tile_tag}}"
cuda_root="${CUDA_HOME:-/usr/local/cuda}"
nvcc="${cuda_root}/bin/nvcc"
cuobjdump="${cuda_root}/bin/cuobjdump"
zig="${ANTFLY_ZIG:-${repo_dir}/.tools/zig-x86_64-linux-0.16.0/zig}"
cuda_source="${inference_dir}/src/ops/cuda/prototypes/gqa_flash_prefill_v2_sm89.cu"
harness_source="${inference_dir}/src/quant_kernel_cuda_flash_prefill_prototype.zig"
summarizer="${inference_dir}/scripts/summarize_cuda_flash_prefill_v2.py"
cubin="${output_dir}/gqa_flash_prefill_v2_sm89_${tile_tag}.cubin"
harness="${output_dir}/antfly-cuda-flash-prefill-v2-prototype"
sass="${output_dir}/gqa_flash_prefill_v2_sm89_${tile_tag}.sass"
resource_usage="${output_dir}/gqa_flash_prefill_v2_sm89_${tile_tag}.resources.txt"
ptxas_log="${output_dir}/gqa_flash_prefill_v2_sm89_${tile_tag}.ptxas.log"
canonical_cubin="${inference_dir}/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"

case "${query_tile}" in
    16|32|64) ;;
    *) echo "ANTFLY_FLASH_V2_QUERY_TILE must be 16, 32, or 64" >&2; exit 2 ;;
esac
case "${key_tile}" in
    16|32|64) ;;
    *) echo "ANTFLY_FLASH_V2_KEY_TILE must be 16, 32, or 64" >&2; exit 2 ;;
esac
case "${head_group}" in
    1|2|4) ;;
    *) echo "ANTFLY_FLASH_V2_HEAD_GROUP must be 1, 2, or 4" >&2; exit 2 ;;
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
    -Xptxas=-v \
    -DANTFLY_FLASH_V2_QUERY_TILE="${query_tile}" \
    -DANTFLY_FLASH_V2_KEY_TILE="${key_tile}" \
    -DANTFLY_FLASH_V2_HEAD_GROUP="${head_group}" \
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

echo "prototype tile: q=${query_tile} k=${key_tile} grouped_heads=${head_group}"
echo "prototype cubin: ${cubin}"
echo "prototype harness: ${harness}"
echo "canonical cubin: ${canonical_cubin}"
echo "ptxas audit: ${ptxas_log}"
echo "resource audit: ${resource_usage}"
echo "SASS audit: ${sass}"
sha256sum "${cubin}" "${canonical_cubin}"
echo "correctness command: ${harness} --candidate-cubin ${cubin} --baseline-cubin ${canonical_cubin} --baseline-route flash --candidate-query-tile ${query_tile} --candidate-key-tile ${key_tile} --candidate-head-group ${head_group} --require-bitwise-candidate --iterations 0 --json-out ${output_dir}/correctness.json"
echo "benchmark command: ${harness} --candidate-cubin ${cubin} --baseline-cubin ${canonical_cubin} --baseline-route flash --candidate-query-tile ${query_tile} --candidate-key-tile ${key_tile} --candidate-head-group ${head_group} --require-bitwise-candidate --layout identity --pattern random --iterations 20 --timing-pairs 7 --json-out ${output_dir}/timing.json"
echo "projection command: ${summarizer} ${output_dir}/timing.json --material-speedup 1.20 --require-material --json-out ${output_dir}/projection.json"
echo "GPU execution was not performed."
