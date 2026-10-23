// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Dev-only generated kernel candidate from graph/quant_kernel_compiler.zig.
// plan_id=cuda/q4_k/rows_1/none/mmv
// kernel_id=antfly_q4_k_mmv_f32_v1
// production_baseline=termite_linear_q4_k_f32_tile4
// production_enabled=false
// Decode candidate for Q4_K-backed Gemma 4 projections. It remains dev-only
// until target-specific microbench and full-model promotion gates pass.

#include <cuda_fp16.h>
#include <stdint.h>

static __device__ __forceinline__ float antfly_half_le_to_float(const uint8_t *p) {
    const uint16_t bits = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
    return __half2float(__ushort_as_half(bits));
}

__device__ __forceinline__ float antfly_q4_k_dequant_lane(const unsigned char *block, int lane) {
    int v0 = lane;
    int v1 = 5;
    int v2 = int(unsigned(v0) >> v1);
    int v3 = 31;
    int v4 = v0 & v3;
    int v5 = 1;
    int v6 = int(unsigned(v2) >> v5);
    int v7 = 32;
    int v8 = v6 * v7;
    int v9 = v8 + v4;
    int v10 = 16;
    int v11 = v10 + v9;
    int v12 = int(block[v11]);
    int v13 = v2 & v5;
    int v14 = 4;
    int v15 = int(unsigned(v12) >> v14);
    int v16 = 15;
    int v17 = v12 & v16;
    int v18 = v13 ? v15 : v17;
    int v19 = 0;
    float v20 = antfly_half_le_to_float(block + v19);
    int v21 = v2 < v14;
    int v22 = v2 + v14;
    int v23 = 8;
    int v24 = v2 + v23;
    int v25 = v21 ? v22 : v24;
    int v26 = int(block[v25]);
    int v27 = v26 & v16;
    int v28 = v2 + v19;
    int v29 = v21 ? v14 : v28;
    int v30 = int(block[v29]);
    int v31 = 6;
    int v32 = int(unsigned(v30) >> v31);
    int v33 = int(unsigned(v32) << v14);
    int v34 = v27 | v33;
    int v35 = 63;
    int v36 = v26 & v35;
    int v37 = v21 ? v36 : v34;
    float v38 = float(v37);
    float v39 = v20 * v38;
    float v40 = float(v18);
    float v41 = v39 * v40;
    int v42 = 2;
    float v43 = antfly_half_le_to_float(block + v42);
    int v44 = v21 ? v24 : v24;
    int v45 = int(block[v44]);
    int v46 = int(unsigned(v45) >> v14);
    int v47 = v21 ? v14 : v22;
    int v48 = int(block[v47]);
    int v49 = int(unsigned(v48) >> v31);
    int v50 = int(unsigned(v49) << v14);
    int v51 = v46 | v50;
    int v52 = v45 & v35;
    int v53 = v21 ? v52 : v51;
    float v54 = float(v53);
    float v55 = v43 * v54;
    float v56 = v41 - v55;
    return v56;
}

extern "C" __global__ void antfly_q4_k_mmv_f32_v1(
    float *output,
    const float *input,
    const uint8_t *weight_q4_k,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int col = blockIdx.x;
    const unsigned int tid = threadIdx.x;
    if (rows != 1u || col >= out_dim || blockDim.x != 256u || (in_dim & 255u) != 0u) return;

    float acc = 0.0f;
    const unsigned int block_count = in_dim >> 8u;
    for (unsigned int block_idx = 0u; block_idx < block_count; ++block_idx) {
        const uint8_t *block = weight_q4_k + ((size_t)col * block_count + block_idx) * 144u;
        const unsigned int input_index = (block_idx << 8u) + tid;
        acc += input[input_index] * antfly_q4_k_dequant_lane(block, tid);
    }

    __shared__ float partial[256];
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) output[col] = partial[0];
}
