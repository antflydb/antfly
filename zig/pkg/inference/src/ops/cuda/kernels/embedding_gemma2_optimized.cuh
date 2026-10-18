// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

#pragma once
#include <cuda_bf16.h>

// Split a packed QKV or gate/up GEMM without changing BF16 values.
extern "C" __global__ void termite_embedding_gemma2_split_columns_bf16(
    __nv_bfloat16* a, __nv_bfloat16* b, __nv_bfloat16* c,
    const __nv_bfloat16* input, unsigned rows, unsigned wa, unsigned wb, unsigned wc) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned width = wa + wb + wc;
    if (i >= rows * width) return;
    unsigned row = i / width, col = i % width;
    if (col < wa) a[(size_t)row * wa + col] = input[i];
    else if (col < wa + wb) b[(size_t)row * wb + col - wa] = input[i];
    else c[(size_t)row * wc + col - wa - wb] = input[i];
}

// Match the F32 rotary and Q scaling boundaries, rounding once into the BF16
// SDPA inputs. Positions retain the existing two-axis half-head layout.
extern "C" __global__ void termite_embedding_gemma2_vision_rope_pack_bf16(
    __nv_bfloat16* qo, __nv_bfloat16* ko, const __nv_bfloat16* q,
    const __nv_bfloat16* k, const unsigned* positions, unsigned count, float theta, float scale) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    unsigned d = i % 32, pair = d % 16, base = i - d;
    float angle = (float)positions[i / 32] * (1.0f / powf(theta, (2.0f * pair) / 32.0f));
    float s = sinf(angle), c = cosf(angle);
    float ql = __bfloat162float(q[base + pair]), qr = __bfloat162float(q[base + 16 + pair]);
    float kl = __bfloat162float(k[base + pair]), kr = __bfloat162float(k[base + 16 + pair]);
    float qv = d < 16 ? ql * c - qr * s : ql * s + qr * c;
    float kv = d < 16 ? kl * c - kr * s : kl * s + kr * c;
    qo[i] = __float2bfloat16_rn(qv * scale);
    ko[i] = __float2bfloat16_rn(kv);
}

// Native head-64 online attention with the same tiled reduction as the
// bundled F32-boundary kernel. Rotary-packed BF16 inputs and BF16 output
// eliminate whole-tensor widening/conversion passes.
extern "C" __global__ __launch_bounds__(256) void termite_embedding_gemma2_native_vision_bf16(
    __nv_bfloat16* dst,
    const __nv_bfloat16* q,
    const __nv_bfloat16* k,
    const __nv_bfloat16* v,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
    constexpr unsigned int kThreads = 256u;
    constexpr unsigned int kQueryTile = 32u;
    constexpr unsigned int kKeyTile = 16u;
    constexpr unsigned int kHeadDim = 64u;
    constexpr float kNegInf = -3.402823466e+38f;
    if (seq_len == 0u || head_dim != kHeadDim || blockDim.x != kThreads) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int query_tiles = (seq_len + kQueryTile - 1u) / kQueryTile;
    const unsigned int block = blockIdx.x;
    const unsigned int query_tile = block % query_tiles;
    const unsigned int matrix = block / query_tiles;
    const unsigned int head = matrix % num_heads;
    const unsigned int b = matrix / num_heads;
    if (b >= batch) return;

    const unsigned int hidden = num_heads * kHeadDim;
    const unsigned int head_off = head * kHeadDim;
    const unsigned int query_start = query_tile * kQueryTile;
    constexpr float scale = 0.125f;

    __shared__ __align__(16) __nv_bfloat16 q_tile[kQueryTile * kHeadDim];
    // K is transposed to the [D, N] row-major layout expected by WMMA B.
    __shared__ __align__(16) __nv_bfloat16 k_tile[kHeadDim * kKeyTile];
    __shared__ __align__(16) __nv_bfloat16 v_tile[kKeyTile * kHeadDim];
    __shared__ __align__(16) float scores[kQueryTile * kKeyTile];
    __shared__ __align__(16) __nv_bfloat16 probabilities[kQueryTile * kKeyTile];
    __shared__ __align__(16) float output[kQueryTile * kHeadDim];
    __shared__ float running_max[kQueryTile];
    __shared__ float running_sum[kQueryTile];
    __shared__ float row_alpha[kQueryTile];

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int row = index / kHeadDim;
        const unsigned int d = index - row * kHeadDim;
        const unsigned int qi = query_start + row;
        q_tile[index] = qi < seq_len ? q[(b * seq_len + qi) * hidden + head_off + d] : __float2bfloat16_rn(0.0f);
        output[index] = 0.0f;
    }
    if (tid < kQueryTile) {
        running_max[tid] = kNegInf;
        running_sum[tid] = 0.0f;
    }
    __syncthreads();

    for (unsigned int key_start = 0u; key_start < seq_len; key_start += kKeyTile) {
        for (unsigned int index = tid; index < kHeadDim * kKeyTile; index += kThreads) {
            const unsigned int d = index / kKeyTile;
            const unsigned int n = index - d * kKeyTile;
            const unsigned int ki = key_start + n;
            k_tile[index] = ki < seq_len ? k[(b * seq_len + ki) * hidden + head_off + d] : __float2bfloat16_rn(0.0f);
            v_tile[n * kHeadDim + d] = ki < seq_len ? v[(b * seq_len + ki) * hidden + head_off + d] : __float2bfloat16_rn(0.0f);
        }
        __syncthreads();

        // Two warps form the two 16-query halves of the 32x16 score tile.
        if (warp < 2u) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::fill_fragment(acc, 0.0f);
            #pragma unroll
            for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, __nv_bfloat16, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, __nv_bfloat16, wmma::row_major> b_frag;
                wmma::load_matrix_sync(a_frag, q_tile + warp * 16u * kHeadDim + d0, kHeadDim);
                wmma::load_matrix_sync(b_frag, k_tile + d0 * kKeyTile, kKeyTile);
                wmma::mma_sync(acc, a_frag, b_frag, acc);
            }
            wmma::store_matrix_sync(scores + warp * 16u * kKeyTile, acc, kKeyTile, wmma::mem_row_major);
        }
        __syncthreads();

        // Each half-warp normalizes one row.  Eight warps cover all 32 rows
        // in two passes while preserving an FP32 online-softmax accumulator.
        #pragma unroll
        for (unsigned int group = 0u; group < 2u; ++group) {
            const unsigned int lane_group = lane >> 4u;
            const unsigned int key_lane = lane & 15u;
            const unsigned int m = warp * 4u + group * 2u + lane_group;
            const unsigned int qi = query_start + m;
            const unsigned int ki = key_start + key_lane;
            const unsigned int group_mask = lane_group == 0u ? 0x0000ffffu : 0xffff0000u;
            float score = kNegInf;
            if (qi < seq_len && ki < seq_len) score = scores[m * kKeyTile + key_lane] * scale;
            float tile_max = score;
            #pragma unroll
            for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) {
                tile_max = fmaxf(tile_max, __shfl_down_sync(group_mask, tile_max, offset));
            }
            const unsigned int leader = lane_group * 16u;
            tile_max = __shfl_sync(group_mask, tile_max, leader);
            const float old_max = running_max[m];
            const float next_max = fmaxf(old_max, tile_max);
            const float alpha = old_max > kNegInf * 0.5f ? expf(old_max - next_max) : 0.0f;
            const float beta = score > kNegInf * 0.5f ? expf(score - next_max) : 0.0f;
            float tile_sum = beta;
            #pragma unroll
            for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) {
                tile_sum += __shfl_down_sync(group_mask, tile_sum, offset);
            }
            tile_sum = __shfl_sync(group_mask, tile_sum, leader);
            probabilities[m * kKeyTile + key_lane] = __float2bfloat16_rn(beta);
            if (key_lane == 0u) {
                running_max[m] = next_max;
                running_sum[m] = running_sum[m] * alpha + tile_sum;
                row_alpha[m] = alpha;
            }
        }
        __syncthreads();

        for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
            output[index] *= row_alpha[index / kHeadDim];
        }
        __syncthreads();

        // Eight warps cover both query fragments and all four output-D
        // fragments, so P*V for the 32x64 output tile completes in one pass.
        const unsigned int output_query_chunk = warp >> 2u;
        const unsigned int output_d_chunk = warp & 3u;
        wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
        wmma::load_matrix_sync(acc, output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, kHeadDim, wmma::mem_row_major);
        wmma::fragment<wmma::matrix_a, 16, 16, 16, __nv_bfloat16, wmma::row_major> p_frag;
        wmma::fragment<wmma::matrix_b, 16, 16, 16, __nv_bfloat16, wmma::row_major> v_frag;
        wmma::load_matrix_sync(p_frag, probabilities + output_query_chunk * 16u * kKeyTile, kKeyTile);
        wmma::load_matrix_sync(v_frag, v_tile + output_d_chunk * 16u, kHeadDim);
        wmma::mma_sync(acc, p_frag, v_frag, acc);
        wmma::store_matrix_sync(output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, acc, kHeadDim, wmma::mem_row_major);
        __syncthreads();
    }

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int m = index / kHeadDim;
        const unsigned int d = index - m * kHeadDim;
        const unsigned int qi = query_start + m;
        if (qi < seq_len) {
            const float denom = running_sum[m];
            dst[(b * seq_len + qi) * hidden + head_off + d] = __float2bfloat16_rn(denom > 0.0f ? output[index] / denom : 0.0f);
        }
    }
#else
    (void)dst;
    (void)q;
    (void)k;
    (void)v;
    (void)batch;
    (void)seq_len;
    (void)num_heads;
    (void)head_dim;
#endif
}

// Each operation keeps the original BF16 rounding point. All reductions use
// the same 256-thread row schedule as termite_embedding_gemma2_rms_norm_bf16.
extern "C" __global__ void termite_embedding_gemma2_norm_residual_bf16(
    __nv_bfloat16* hidden, __nv_bfloat16* next, const __nv_bfloat16* branch,
    const __nv_bfloat16* post_weight, const __nv_bfloat16* next_weight,
    const __nv_bfloat16* scalar, unsigned rows, unsigned width, float eps) {
    const unsigned row = blockIdx.x;
    if (row >= rows) return;
    const size_t base = (size_t)row * width;
    __shared__ float warp_sum[8], inv;
    float sum = 0.0f;
    for (unsigned d = threadIdx.x; d < width; d += blockDim.x) {
        float x = __bfloat162float(branch[base + d]); sum += x * x;
    }
    sum = eg2_warp_sum(sum);
    if ((threadIdx.x & 31u) == 0) warp_sum[threadIdx.x >> 5] = sum;
    __syncthreads();
    if (threadIdx.x < 32) {
        float x = threadIdx.x < (blockDim.x >> 5) ? warp_sum[threadIdx.x] : 0.0f;
        x = eg2_warp_sum(x);
        if (threadIdx.x == 0) inv = rsqrtf(x / (float)width + eps);
    }
    __syncthreads();
    for (unsigned d = threadIdx.x; d < width; d += blockDim.x) {
        const float x = __bfloat162float(branch[base + d]);
        const __nv_bfloat16 norm = __float2bfloat16_rn(x * inv * __bfloat162float(post_weight[d]));
        __nv_bfloat16 residual = __float2bfloat16_rn(__bfloat162float(hidden[base + d]) + __bfloat162float(norm));
        if (scalar) residual = __float2bfloat16_rn(__bfloat162float(residual) * __bfloat162float(scalar[0]));
        hidden[base + d] = residual;
    }
    if (!next_weight) return;
    __syncthreads();
    sum = 0.0f;
    for (unsigned d = threadIdx.x; d < width; d += blockDim.x) {
        float x = __bfloat162float(hidden[base + d]); sum += x * x;
    }
    sum = eg2_warp_sum(sum);
    if ((threadIdx.x & 31u) == 0) warp_sum[threadIdx.x >> 5] = sum;
    __syncthreads();
    if (threadIdx.x < 32) {
        float x = threadIdx.x < (blockDim.x >> 5) ? warp_sum[threadIdx.x] : 0.0f;
        x = eg2_warp_sum(x);
        if (threadIdx.x == 0) inv = rsqrtf(x / (float)width + eps);
    }
    __syncthreads();
    for (unsigned d = threadIdx.x; d < width; d += blockDim.x)
        next[base + d] = __float2bfloat16_rn(__bfloat162float(hidden[base + d]) * inv * __bfloat162float(next_weight[d]));
}

extern "C" __global__ void termite_embedding_gemma2_scale_rms_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input, const __nv_bfloat16* weight,
    unsigned rows, unsigned width, float scale, float eps, unsigned input_stride) {
    const unsigned row = blockIdx.x;
    if (row >= rows) return;
    const size_t base = (size_t)row * width;
    const size_t input_base = (size_t)row * input_stride;
    __shared__ float warp_sum[8], inv;
    float sum = 0.0f;
    for (unsigned d = threadIdx.x; d < width; d += blockDim.x) {
        float x = __bfloat162float(__float2bfloat16_rn(__bfloat162float(input[input_base + d]) * scale));
        sum += x * x;
    }
    sum = eg2_warp_sum(sum);
    if ((threadIdx.x & 31u) == 0) warp_sum[threadIdx.x >> 5] = sum;
    __syncthreads();
    if (threadIdx.x < 32) {
        float x = threadIdx.x < (blockDim.x >> 5) ? warp_sum[threadIdx.x] : 0.0f;
        x = eg2_warp_sum(x);
        if (threadIdx.x == 0) inv = rsqrtf(x / (float)width + eps);
    }
    __syncthreads();
    for (unsigned d = threadIdx.x; d < width; d += blockDim.x) {
        float x = __bfloat162float(__float2bfloat16_rn(__bfloat162float(input[input_base + d]) * scale));
        out[base + d] = __float2bfloat16_rn(x * inv * __bfloat162float(weight[d]));
    }
}

extern "C" __global__ void termite_embedding_gemma2_audio_segment_conv_f32(
    float* out, const float* input, const float* weight, const float* starts,
    unsigned rows, unsigned dim, unsigned kernel_size) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= (size_t)rows * dim) return;
    unsigned row = i / dim, col = i % dim, start = (unsigned)starts[row];
    float sum = 0.0f;
    for (unsigned k = 0; k < kernel_size; ++k) {
        if (row + k < kernel_size - 1 + start) continue;
        unsigned source = row + k - (kernel_size - 1);
        sum += input[(size_t)source * dim + col] * weight[(size_t)k * dim + col];
    }
    out[i] = sum;
}

// One block owns a complete head. RMS reduction and every BF16 conversion
// match the separate norm/rotary kernels; packed GEMM rows remain immutable.
extern "C" __global__ void termite_embedding_gemma2_packed_heads_bf16(
    __nv_bfloat16* q, __nv_bfloat16* k, __nv_bfloat16* v,
    const __nv_bfloat16* input, const __nv_bfloat16* qw, const __nv_bfloat16* kw,
    const long long* mask, const unsigned* positions, unsigned rows, unsigned seq,
    unsigned q_heads, unsigned kv_heads, unsigned dim, float eps, float theta,
    unsigned vision, float q_scale) {
    unsigned row = blockIdx.x, slot = blockIdx.y, tid = threadIdx.x;
    if (row >= rows || dim > 512u) return;
    unsigned kind = slot < q_heads ? 0u : slot < q_heads + kv_heads ? 1u : 2u;
    unsigned head = slot - (kind == 0u ? 0u : kind == 1u ? q_heads : q_heads + kv_heads);
    unsigned heads = kind == 0u ? q_heads : kv_heads;
    unsigned start = kind == 0u ? 0u : kind == 1u ? q_heads * dim : (q_heads + kv_heads) * dim;
    size_t base = (size_t)row * (q_heads + 2u * kv_heads) * dim + start + head * dim;
    const __nv_bfloat16* weight = kind == 0u ? qw : kind == 1u ? kw : nullptr;
    __nv_bfloat16* dst = (kind == 0u ? q : kind == 1u ? k : v) + ((size_t)row * heads + head) * dim;
    __shared__ float sums[8], inv;
    __shared__ __nv_bfloat16 normalized[512];
    float sum = 0.0f;
    for (unsigned d = tid; d < dim; d += blockDim.x) { float x = __bfloat162float(input[base+d]); sum += x*x; }
    sum = eg2_warp_sum(sum);
    if ((tid & 31u) == 0) sums[tid >> 5u] = sum;
    __syncthreads();
    if (tid < 32u) {
        float x = tid < 8u ? sums[tid] : 0.0f; x = eg2_warp_sum(x);
        if (tid == 0u) inv = rsqrtf(x / (float)dim + eps);
    }
    __syncthreads();
    for (unsigned d = tid; d < dim; d += blockDim.x) normalized[d] = __float2bfloat16_rn(__bfloat162float(input[base+d]) * inv * (weight ? __bfloat162float(weight[d]) : 1.0f));
    __syncthreads();
    bool rotate = kind != 2u && (vision || !mask || mask[row] != 0);
    for (unsigned d = tid; d < dim; d += blockDim.x) {
        if (!rotate) { dst[d] = normalized[d]; continue; }
        if (vision) {
            unsigned local = d % 32u, pair = local % 16u, first = d - local;
            float angle = (float)positions[((size_t)row * q_heads + head) * 2u + d / 32u] * (1.0f / powf(theta, (2.0f * pair) / 32.0f));
            float sn = sinf(angle), cs = cosf(angle);
            float lo = __bfloat162float(normalized[first+pair]), hi = __bfloat162float(normalized[first+16u+pair]);
            float x = local < 16u ? lo * cs - hi * sn : lo * sn + hi * cs;
            dst[d] = __float2bfloat16_rn(kind == 0u ? x * q_scale : x);
        } else {
            unsigned half = dim / 2u, pair = d % half;
            float angle = __fmul_rn((float)(row % seq), eg2_training_rope_inverse_frequency(theta, dim, pair));
            float sn, cs; sincosf(angle, &sn, &cs);
            float s = __bfloat162float(__float2bfloat16_rn(sn)), c = __bfloat162float(__float2bfloat16_rn(cs));
            float lo = __bfloat162float(normalized[pair]), hi = __bfloat162float(normalized[pair+half]);
            __nv_bfloat16 a = __float2bfloat16_rn((d < half ? lo : hi) * c);
            __nv_bfloat16 b = __float2bfloat16_rn((d < half ? -hi : lo) * s);
            dst[d] = __float2bfloat16_rn(__bfloat162float(a) + __bfloat162float(b));
        }
    }
}
extern "C" __global__ void termite_embedding_gemma2_packed_gelu_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input, unsigned rows, unsigned width) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= (size_t)rows * width) return;
    size_t base = (i / width) * width * 2u + i % width;
    float x = __bfloat162float(input[base]);
    float g = 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    g = __bfloat162float(__float2bfloat16_rn(g));
    out[i] = __float2bfloat16_rn(g * __bfloat162float(input[base+width]));
}

// Direct geometry indexing avoids repeated gather tables and temporary sums.
extern "C" __global__ void termite_embedding_gemma2_spatial_pool_f32(
    float* output, const float* input, const float* bias, const float* scale,
    unsigned rows, unsigned gx, unsigned gy, unsigned merge, unsigned dim, float factor) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= (size_t)rows * dim) return;
    unsigned channel = i % dim, row = i / dim, px = gx / merge, py = gy / merge;
    unsigned batch = row / (px * py), y = (row / px) % py, x = row % px;
    size_t base = ((size_t)batch * gx * gy + y * merge * gx + x * merge) * dim + channel;
    float sum = input[base];
    for (unsigned dy = 0; dy < merge; ++dy) for (unsigned dx = 0; dx < merge; ++dx)
        if (dy || dx) sum = __fadd_rn(sum, input[base + ((size_t)dy * gx + dx) * dim]);
    float value = __fmul_rn(sum, factor);
    if (bias) value = __fmul_rn(__fsub_rn(value, bias[channel]), scale[channel]);
    output[i] = value;
}

// Qualified BF16 alternative: round activations explicitly, accumulate in F32.
extern "C" __global__ void antfly_audio_conv_bf16_wmma(float *out,const float *input,const float *weight,const float *bias,unsigned ic,unsigned oc,unsigned h,unsigned w,unsigned oh,unsigned ow) {
#if __CUDA_ARCH__ >= 800
    using namespace nvcuda;
    __shared__ __align__(32) __nv_bfloat16 a[256], b[256];
    __shared__ __align__(32) float c[256];
    unsigned lane=threadIdx.x, channel0=blockIdx.y*16,pos0=blockIdx.x*16,batch=blockIdx.z;
    wmma::fragment<wmma::matrix_a,16,16,16,__nv_bfloat16,wmma::row_major> af;
    wmma::fragment<wmma::matrix_b,16,16,16,__nv_bfloat16,wmma::row_major> bf;
    wmma::fragment<wmma::accumulator,16,16,16,float> cf;
    wmma::fill_fragment(cf,0.f);
    for(unsigned base=0;base<ic*9;base+=16) {
        for(unsigned i=lane;i<256;i+=32) {
            unsigned row=i/16,col=i%16, ak=base+col,bk=base+row,pos=pos0+col;
            a[i]=__float2bfloat16_rn(channel0+row<oc && ak<ic*9 ? weight[(size_t)(channel0+row)*ic*9+ak] : 0.f);
            int iy=(int)((pos/ow)*2+bk/3%3)-1,ix=(int)((pos%ow)*2+bk%3)-1;
            b[i]=__float2bfloat16_rn(pos<oh*ow && bk<ic*9 && iy>=0 && iy<(int)h && ix>=0 && ix<(int)w ? input[((size_t)batch*ic+bk/9)*h*w+(unsigned)iy*w+(unsigned)ix] : 0.f);
        }
        __syncwarp(); wmma::load_matrix_sync(af,a,16); wmma::load_matrix_sync(bf,b,16); wmma::mma_sync(cf,af,bf,cf); __syncwarp();
    }
    wmma::store_matrix_sync(c,cf,16,wmma::mem_row_major); __syncwarp();
    for(unsigned i=lane;i<256;i+=32) { unsigned ch=channel0+i/16,pos=pos0+i%16; if(ch<oc && pos<oh*ow) out[((size_t)batch*oc+ch)*oh*ow+pos]=c[i]+bias[ch]; }
#endif
}
