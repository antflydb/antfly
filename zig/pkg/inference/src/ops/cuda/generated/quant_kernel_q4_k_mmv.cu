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
    int v13 = 1;
    int v14 = v2 & v13;
    int v15 = 4;
    int v16 = int(unsigned(v12) >> v15);
    int v17 = 15;
    int v18 = v12 & v17;
    int v19 = v14 ? v16 : v18;
    int v20 = 0;
    float v21 = antfly_half_le_to_float(block + v20);
    int v22 = 4;
    int v23 = v2 < v22;
    int v24 = 4;
    int v25 = v2 + v24;
    int v26 = 8;
    int v27 = v2 + v26;
    int v28 = v23 ? v25 : v27;
    int v29 = int(block[v28]);
    int v30 = 15;
    int v31 = v29 & v30;
    int v32 = 4;
    int v33 = 0;
    int v34 = v2 + v33;
    int v35 = v23 ? v32 : v34;
    int v36 = int(block[v35]);
    int v37 = 6;
    int v38 = int(unsigned(v36) >> v37);
    int v39 = 4;
    int v40 = int(unsigned(v38) << v39);
    int v41 = v31 | v40;
    int v42 = 63;
    int v43 = v29 & v42;
    int v44 = v23 ? v43 : v41;
    float v45 = float(v44);
    float v46 = v21 * v45;
    float v47 = float(v19);
    float v48 = v46 * v47;
    int v49 = 2;
    float v50 = antfly_half_le_to_float(block + v49);
    int v51 = 4;
    int v52 = v2 < v51;
    int v53 = 8;
    int v54 = v2 + v53;
    int v55 = 8;
    int v56 = v2 + v55;
    int v57 = v52 ? v54 : v56;
    int v58 = int(block[v57]);
    int v59 = 4;
    int v60 = int(unsigned(v58) >> v59);
    int v61 = 4;
    int v62 = 4;
    int v63 = v2 + v62;
    int v64 = v52 ? v61 : v63;
    int v65 = int(block[v64]);
    int v66 = 6;
    int v67 = int(unsigned(v65) >> v66);
    int v68 = 4;
    int v69 = int(unsigned(v67) << v68);
    int v70 = v60 | v69;
    int v71 = 63;
    int v72 = v58 & v71;
    int v73 = v52 ? v72 : v70;
    float v74 = float(v73);
    float v75 = v50 * v74;
    float v76 = v48 - v75;
    return v76;
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
