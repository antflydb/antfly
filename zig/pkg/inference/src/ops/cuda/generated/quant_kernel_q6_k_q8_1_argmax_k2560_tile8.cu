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
// plan_id=cuda/q6_k/rows_1/argmax/mmv
// kernel_id=antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1
// production_baseline=termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b+termite_argmax_reduce_rows_pairs_f32_w16
// production_enabled=false
// Standalone shape candidates. The handwritten Q6_K x Q8_1 LM-head route
// remains authoritative until exact-token and target-specific speed gates pass.

#include <cuda_fp16.h>

static __device__ __forceinline__ float antfly_half_bits_to_float(unsigned short bits) {
    return __half2float(__ushort_as_half(bits));
}

static __device__ __forceinline__ float antfly_warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

static __device__ __forceinline__ float antfly_q6_k_sub_scale_f32(
    const unsigned char *block,
    unsigned int sub
) {
    const signed char *scales = (const signed char *)(block + 192u);
    const unsigned short d_bits = (unsigned short)block[208] | ((unsigned short)block[209] << 8);
    return antfly_half_bits_to_float(d_bits) * (float)scales[sub];
}

static __device__ __forceinline__ int antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(
    const unsigned char* ql,
    const unsigned char* qh,
    unsigned int nibble_shift,
    unsigned int qh_shift,
    unsigned int offset
) {
    const unsigned int q0 = ((unsigned int)(ql[offset + 0u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 0u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q1 = ((unsigned int)(ql[offset + 1u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 1u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q2 = ((unsigned int)(ql[offset + 2u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 2u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q3 = ((unsigned int)(ql[offset + 3u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 3u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int p0 = (q0 - 32u) & 0xffu;
    const unsigned int p1 = (q1 - 32u) & 0xffu;
    const unsigned int p2 = (q2 - 32u) & 0xffu;
    const unsigned int p3 = (q3 - 32u) & 0xffu;
    return (int)(p0 | (p1 << 8u) | (p2 << 16u) | (p3 << 24u));
}

static __device__ __forceinline__ int antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_q8_1_dot16_sub(
    const unsigned char* block,
    unsigned int sub,
    int q8_pack0,
    int q8_pack1,
    int q8_pack2,
    int q8_pack3
) {
    const unsigned int half = sub >> 3u;
    const unsigned int group = (sub & 7u) >> 1u;
    const unsigned int l_base = (sub & 1u) * 16u;
    const unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    const unsigned int qh_off = half * 32u;
    const unsigned int qh_shift = group << 1u;
    const unsigned int nibble_shift = (group >> 1u) << 2u;
    const unsigned char* ql = block + ql_off + l_base;
    const unsigned char* qh = block + 128u + qh_off + l_base;
    int sumi = 0;
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 0u), q8_pack0, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 4u), q8_pack1, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 8u), q8_pack2, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 12u), q8_pack3, sumi);
    return sumi;
}
extern "C" __global__ void antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    const unsigned int cols = 8u;
    const unsigned int row_blocks = 10u;
    const unsigned int task_threads = 160u;
    if (rows != 1u || in_dim != 2560u || out_dim != 262144u || suppress_count != 0u || blockDim.x != 160u) return;

    const unsigned int global_tile = blockIdx.x;
    const unsigned int col_tile = global_tile * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float warp_partial[8][5];
    float acc[8];
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    if (tid < task_threads) {
        const unsigned int block = tid >> 4u;
        const unsigned int sub = tid & 15u;
        const unsigned int q8_sub_block = sub >> 1u;
        const unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = q8_input + (block * 8u + q8_sub_block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        const int q8_pack0 = *(const int*)(q8_values + q8_lane_base + 0u);
        const int q8_pack1 = *(const int*)(q8_values + q8_lane_base + 4u);
        const int q8_pack2 = *(const int*)(q8_values + q8_lane_base + 8u);
        const int q8_pack3 = *(const int*)(q8_values + q8_lane_base + 12u);
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;
            const int sumi = antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_q8_1_dot16_sub(bp, sub, q8_pack0, q8_pack1, q8_pack2, q8_pack3);
            acc[c] = (q8_d * antfly_q6_k_sub_scale_f32(bp, sub)) * (float)sumi;
        }
    }

#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid != 0u) return;

    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        float value = 0.0f;
#pragma unroll
        for (unsigned int w = 0u; w < 5u; ++w) value += warp_partial[c][w];
        const unsigned int col = col_tile + c;
        if (value > best_value || (value == best_value && col < best_index)) {
            best_value = value;
            best_index = col;
        }
    }
    partial_values[global_tile] = best_value;
    partial_indices[global_tile] = best_index;
}
