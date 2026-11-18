// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// M64/N64 online attention: retain FP16 operands/probabilities and FP32
// accumulation. Specified PTX MMA register maps avoid score/output round trips
// through shared memory. The denominator uses a four-lane sum reduction.
extern "C" __global__ __launch_bounds__(256) void antfly_gemma4_vision_attention_registers(
    float* dst, const float* q, const float* k, const float* v,
    const long long* mask, unsigned int batch, unsigned int seq,
    unsigned int heads, unsigned int dim, unsigned int radius
) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800

    constexpr unsigned int M = 64u, N = 64u, D = 64u, P = 72u, T = 256u;
    if (!seq || seq > 8192u || dim != D || radius < seq || blockDim.x != T) return;
    const unsigned int tid = threadIdx.x, lane = tid & 31u, warp = tid >> 5u;
    const unsigned int tiles = (seq + M - 1u) / M;
    const unsigned int start = (blockIdx.x % tiles) * M;
    const unsigned int matrix = blockIdx.x / tiles, head = matrix % heads, b = matrix / heads;
    if (b >= batch) return;
    const unsigned int hidden = heads * D, head_off = head * D;
    __shared__ __align__(16) half qt[M * P], kt[N * P], vt[N * P];
    __shared__ __align__(16) half probabilities[M * P];
    __shared__ float maximum[M], denominator[M], alpha[M];
    float output[4][4] = {};
    for (unsigned int i = tid; i < M * D; i += T) {
        const unsigned int row = i / D, d = i % D;
        qt[row * P + d] = __float2half_rn(start + row < seq ? q[(b * seq + start + row) * hidden + head_off + d] : 0.0f);
    }
    if (tid < M) { maximum[tid] = -INFINITY; denominator[tid] = 0.0f; }
    __syncthreads();
    for (unsigned int key_start = 0u; key_start < seq; key_start += N) {
        for (unsigned int i = tid; i < N * D; i += T) {
            const unsigned int row = i / D, d = i % D, key = key_start + row;
            const bool valid = key < seq && (!mask || mask[b * seq + key] != 0ll);
            const unsigned int offset = (b * seq + key) * hidden + head_off + d;
            kt[row * P + d] = __float2half_rn(valid ? k[offset] : 0.0f);
            vt[row * P + d] = __float2half_rn(valid ? v[offset] : 0.0f);
        }
        __syncthreads();
        if ((warp & 1u) == 0u) {
            const unsigned int group = warp / 2u;
            float scores[8][4] = {};
            #pragma unroll
            for (unsigned int d = 0u; d < D; d += 16u) {
                unsigned int qa[4];
                const unsigned int qa_addr = __cvta_generic_to_shared(qt +
                    (group * 16u + lane % 16u) * P + d + (lane / 16u) * 8u);
                asm volatile("ldmatrix.sync.aligned.m8n8.x4.shared.b16 {%0,%1,%2,%3}, [%4];"
                    : "=r"(qa[0]),"=r"(qa[1]),"=r"(qa[2]),"=r"(qa[3]) : "r"(qa_addr));
                #pragma unroll
                for (unsigned int tile = 0u; tile < 8u; ++tile) {
                    unsigned int kb[2];
                    const unsigned int kb_addr = __cvta_generic_to_shared(kt +
                        (tile * 8u + lane % 8u) * P + d + ((lane / 8u) % 2u) * 8u);
                    asm volatile("ldmatrix.sync.aligned.m8n8.x2.shared.b16 {%0,%1}, [%2];"
                        : "=r"(kb[0]),"=r"(kb[1]) : "r"(kb_addr));
                    asm volatile("mma.sync.aligned.m16n8k16.row.col.f32.f16.f16.f32 {%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%0,%1,%2,%3};"
                        : "+f"(scores[tile][0]),"+f"(scores[tile][1]),"+f"(scores[tile][2]),"+f"(scores[tile][3])
                        : "r"(qa[0]),"r"(qa[1]),"r"(qa[2]),"r"(qa[3]),"r"(kb[0]),"r"(kb[1]));
                }
            }
            #pragma unroll
            for (unsigned int r = 0u; r < 2u; ++r) {
                const unsigned int row = group * 16u + lane / 4u + r * 8u;
                float values[16], next_max = -INFINITY;
                #pragma unroll
                for (unsigned int c = 0u; c < 16u; ++c) {
                    const unsigned int column = (c / 2u) * 8u + (lane % 4u) * 2u + c % 2u;
                    const unsigned int key = key_start + column;
                    const bool valid = start + row < seq && key < seq && (!mask || mask[b * seq + key] != 0ll);
                    values[c] = valid ? scores[c / 2u][r * 2u + c % 2u] * 0.125f : -INFINITY;
                    next_max = fmaxf(next_max, values[c]);
                }
                next_max = fmaxf(next_max, __shfl_xor_sync(0xffffffffu, next_max, 2, 4));
                next_max = fmaxf(next_max, __shfl_xor_sync(0xffffffffu, next_max, 1, 4));
                next_max = fmaxf(maximum[row], next_max);
                const float correction = isfinite(maximum[row]) ? __expf(maximum[row] - next_max) : 0.0f;
                float sum = 0.0f;
                #pragma unroll
                for (unsigned int c = 0u; c < 16u; ++c) {
                    const float p = isfinite(next_max) ? __expf(values[c] - next_max) : 0.0f;
                    const unsigned int column = (c / 2u) * 8u + (lane % 4u) * 2u + c % 2u;
                    probabilities[row * P + column] = __float2half_rn(p);
                    sum += p;
                }
                sum += __shfl_xor_sync(0xffffffffu, sum, 2, 4);
                sum += __shfl_xor_sync(0xffffffffu, sum, 1, 4);
                if ((lane % 4u) == 0u) {
                    alpha[row] = correction;
                    denominator[row] = denominator[row] * correction + sum;
                    maximum[row] = next_max;
                }
            }
        }
        __syncthreads();
        // PTX m16n8k16 has a specified accumulator map: lane/4 selects
        // the first row, elements 2/3 select row+8. Rescale those registers
        // directly; no opaque WMMA fragment layout is assumed.
        const unsigned int query_group = warp / 2u;
        const unsigned int output_half = warp % 2u;
        const unsigned int first_row = query_group * 16u + lane / 4u;
        #pragma unroll
        for (unsigned int f = 0u; f < 4u; ++f) {
            output[f][0] *= alpha[first_row];
            output[f][1] *= alpha[first_row];
            output[f][2] *= alpha[first_row + 8u];
            output[f][3] *= alpha[first_row + 8u];
        }
        #pragma unroll
        for (unsigned int n = 0u; n < N; n += 16u) {
            unsigned int pa[4];
            const unsigned int pa_addr = __cvta_generic_to_shared(probabilities +
                (query_group * 16u + lane % 16u) * P + n + (lane / 16u) * 8u);
            asm volatile("ldmatrix.sync.aligned.m8n8.x4.shared.b16 {%0,%1,%2,%3}, [%4];"
                : "=r"(pa[0]),"=r"(pa[1]),"=r"(pa[2]),"=r"(pa[3]) : "r"(pa_addr));
            #pragma unroll
            for (unsigned int f = 0u; f < 4u; ++f) {
                unsigned int vb[2];
                const unsigned int col = output_half * 32u + f * 8u;
                const unsigned int vb_addr = __cvta_generic_to_shared(vt + (n + lane % 16u) * P + col);
                asm volatile("ldmatrix.sync.aligned.m8n8.x2.trans.shared.b16 {%0,%1}, [%2];"
                    : "=r"(vb[0]),"=r"(vb[1]) : "r"(vb_addr));
                asm volatile("mma.sync.aligned.m16n8k16.row.col.f32.f16.f16.f32 {%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%0,%1,%2,%3};"
                    : "+f"(output[f][0]),"+f"(output[f][1]),"+f"(output[f][2]),"+f"(output[f][3])
                    : "r"(pa[0]),"r"(pa[1]),"r"(pa[2]),"r"(pa[3]),"r"(vb[0]),"r"(vb[1]));
            }
        }
        __syncthreads();
    }
    #pragma unroll
    for (unsigned int f = 0u; f < 4u; ++f) {
        #pragma unroll
        for (unsigned int e = 0u; e < 4u; ++e) {
            const unsigned int row = (warp / 2u) * 16u + lane / 4u + (e / 2u) * 8u;
            const unsigned int d = (warp % 2u) * 32u + f * 8u + (lane % 4u) * 2u + e % 2u;
            if (start + row < seq)
                dst[(b * seq + start + row) * hidden + head_off + d] = denominator[row] > 0.0f ? output[f][e] / denominator[row] : 0.0f;
        }
    }

#endif
}
