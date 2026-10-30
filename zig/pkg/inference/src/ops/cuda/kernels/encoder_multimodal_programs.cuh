#include "../../gpu/depthwise_program.h"
#include "../../gpu/masked_pool_program.h"
// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "rotary_frequency_training.cuh"
#include "row_program.cuh"
#include "prefix_normalization_program.cuh"

// Bidirectional BF16 encoder programs retain unscaled FP32 attention.
// Decoder causal attention uses a different score scale and schedule.
// Historical exports are compatibility names; extend these shared owners.

// Pillow's RGB8 bicubic resize materializes quantized horizontal pixels.
// The CPU supplies its exact integer coefficient plan; both passes retain
// the signed 64-bit accumulation and rounding before clipping to RGB8.
extern "C" __global__ void termite_embedding_gemma2_pillow_horizontal_rgb8(
    unsigned char* out, const unsigned char* input,
    const unsigned long long* starts, const unsigned long long* offsets,
    const int* weights, unsigned int source_height, unsigned int target_width,
    unsigned long long source_stride, unsigned int precision_bits) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    const size_t count = (size_t)source_height * target_width * 3;
    if (i >= count) return;
    const unsigned int channel = i % 3;
    const size_t pixel = i / 3;
    const unsigned int x = pixel % target_width;
    const unsigned int y = pixel / target_width;
    long long value = 1ll << (precision_bits - 1);
    const unsigned long long begin = offsets[x], end = offsets[x + 1];
    for (unsigned long long tap = begin; tap < end; ++tap) {
        const size_t source = (size_t)y * source_stride + (starts[x] + tap - begin) * 3 + channel;
        value += (long long)input[source] * weights[tap];
    }
    value >>= precision_bits;
    out[i] = (unsigned char)(value < 0 ? 0 : value > 255 ? 255 : value);
}

// Write scaled, patch-major F32 pixels directly, avoiding the CPU CHW
// normalization, scaling, and patch-packing passes. The explicit rounding
// matches 2 * (RGB8 * float32(1/255) - 0.5) before the BF16 projection.
extern "C" __global__ void termite_embedding_gemma2_pillow_vertical_patches_f32(
    float* out, const unsigned char* horizontal,
    const unsigned long long* starts, const unsigned long long* offsets,
    const int* weights, unsigned int target_width, unsigned int target_height,
    unsigned int patch_size, unsigned int precision_bits) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    const size_t count = (size_t)target_width * target_height * 3;
    if (i >= count) return;
    const unsigned int patch_dim = patch_size * patch_size * 3;
    const size_t patch = i / patch_dim;
    const unsigned int within = i % patch_dim;
    const unsigned int channel = within % 3;
    const unsigned int pixel = within / 3;
    const unsigned int grid_x = target_width / patch_size;
    const unsigned int x = (patch % grid_x) * patch_size + pixel % patch_size;
    const unsigned int y = (patch / grid_x) * patch_size + pixel / patch_size;
    long long value = 1ll << (precision_bits - 1);
    const unsigned long long begin = offsets[y], end = offsets[y + 1];
    const size_t row_bytes = (size_t)target_width * 3;
    for (unsigned long long tap = begin; tap < end; ++tap) {
        const size_t source = (starts[y] + tap - begin) * row_bytes + (size_t)x * 3 + channel;
        value += (long long)horizontal[source] * weights[tap];
    }
    value >>= precision_bits;
    const float sample = (float)(value < 0 ? 0 : value > 255 ? 255 : value);
    const float rescaled = __fmul_rn(sample, 1.0f / 255.0f);
    out[i] = __fmul_rn(2.0f, __fsub_rn(rescaled, 0.5f));
}

// Gemma4/EmbeddingGemma2 audio local attention. Q/K/V/relative inputs and
// output are FP32; one warp owns a query/head and the causal context is tiny
// (24 slots, at most 12 valid keys), so scores and output stay in registers.
extern "C" __global__ void termite_gemma4_audio_local_attention_f32(
    float* out, const float* q, const float* k, const float* v,
    const float* rel, const float* q_scales, const float* valid,
    unsigned int rows, unsigned int hidden, unsigned int heads,
    unsigned int head_dim, unsigned int chunk, unsigned int context_left,
    unsigned int context, float k_scale, float logit_cap, float invalid_value, unsigned int flags) {
    unsigned int lane = threadIdx.x;
    unsigned int query = blockIdx.x, head = blockIdx.y;
    if (query >= rows || head >= heads || lane >= 32u) return;
    size_t out_base = (size_t)query * hidden + (size_t)head * head_dim;
    if ((flags & 2u) ? valid[query] <= 0.0f : valid[query] == 0.0f) {
        for (unsigned int d = lane; d < head_dim; d += 32u) out[out_base + d] = 0.0f;
        return;
    }
    float scores[24];
    #pragma unroll
    for (unsigned int i = 0; i < 24; ++i) scores[i] = invalid_value;
    unsigned int clip_start = (flags & 2u) ? (unsigned int)valid[query] - 1u : 0u;
    unsigned int block_start = clip_start + ((query - clip_start) / chunk) * chunk;
    unsigned int q_off = query - block_start;
    unsigned int past = context_left - 1u;
    float max_score = -INFINITY;
    for (unsigned int c = 0; c < context; ++c) {
        int rel_idx = (int)c - (int)q_off;
        int key_idx = (int)block_start + (int)c - (int)past;
        // Official sliding mask uses distance < (context_left - 1), so a
        // distance exactly equal to `past` is excluded.
        if (rel_idx < 0 || rel_idx >= (int)context_left || key_idx < 0 ||
            key_idx >= (int)rows || ((flags & 2u) ? valid[key_idx] != valid[query] : valid[key_idx] == 0.0f) || key_idx > (int)query ||
            query - (unsigned int)key_idx > past - (flags & 1u)) continue;
        float score = 0.0f;
        size_t qb = (size_t)query * hidden + (size_t)head * head_dim;
        size_t kb = (size_t)key_idx * hidden + (size_t)head * head_dim;
        size_t rb = (size_t)rel_idx * hidden + (size_t)head * head_dim;
        for (unsigned int d = lane; d < head_dim; d += 32u) {
            float qs = q[qb + d] * q_scales[d];
            score += qs * (k[kb + d] * k_scale + rel[rb + d]);
        }
        score = antfly_row_warp_sum(score);
        score = __shfl_sync(0xffffffffu, score, 0);
        score = tanhf(score / logit_cap) * logit_cap;
        scores[c] = score;
        max_score = fmaxf(max_score, score);
    }
    float denominator = 0.0f;
    #pragma unroll
    for (unsigned int c = 0; c < 24; ++c) {
        if (c < context) {
            scores[c] = expf(scores[c] - max_score);
            denominator += scores[c];
        }
    }
    for (unsigned int d = lane; d < head_dim; d += 32u) {
        float value = 0.0f;
        for (unsigned int c = 0; c < context; ++c) {
            int key_idx = (int)block_start + (int)c - (int)past;
            if (key_idx < 0 || key_idx >= (int)rows) continue;
            if ((flags & 2u) ? valid[key_idx] != valid[query] : valid[key_idx] == 0.0f) continue;
            if (key_idx > (int)query || query - (unsigned int)key_idx > past - (flags & 1u)) continue;
            value += (scores[c] / denominator) * v[(size_t)key_idx * hidden + (size_t)head * head_dim + d];
        }
        out[out_base + d] = denominator > 0.0f ? value : 0.0f;
    }
}

extern "C" __global__ void termite_embedding_gemma2_lookup_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* table, const long long* ids,
    unsigned int rows, unsigned int vocab, unsigned int hidden, float scale) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    size_t n = (size_t)rows * hidden;
    if (i >= n) return;
    unsigned int row = (unsigned int)(i / hidden), col = (unsigned int)(i % hidden);
    long long id = ids[row];
    out[i] = (id >= 0 && (unsigned long long)id < vocab)
        ? __float2bfloat16_rn(__bfloat162float(table[(size_t)id * hidden + col]) * scale)
        : __float2bfloat16_rn(0.0f);
}

extern "C" __global__ void termite_embedding_gemma2_rms_norm_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input, const __nv_bfloat16* weight,
    unsigned int rows, unsigned int width, float eps) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    const size_t base = (size_t)row * width;
    __shared__ float warp_sum[8], inv;
    antfly_row_rms(width, eps, warp_sum, inv,
        [&](unsigned i) { return __bfloat162float(input[base+i]); },
        [&](unsigned i, float x, float inverse) {
            out[base+i] = __float2bfloat16_rn(x * inverse * (weight ? __bfloat162float(weight[i]) : 1.0f));
        });
}

// Training-only RMSNorm matching ATen's vectorized contiguous row reduction:
// four independent vector lanes are combined before the block-x reduction.
extern "C" __global__ void termite_embedding_gemma2_training_rms_norm_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input, const __nv_bfloat16* weight,
    unsigned int rows, unsigned int width, float eps) {
    unsigned int row = blockIdx.x * blockDim.y + threadIdx.y;
    if (row >= rows) return;
    size_t base = (size_t)row * width;
    float s0=0.f,s1=0.f,s2=0.f,s3=0.f;
    for (unsigned int v=threadIdx.x; v<width/4; v+=blockDim.x) {
        unsigned int i=v*4;
        float x0=__bfloat162float(input[base+i]), x1=__bfloat162float(input[base+i+1]);
        float x2=__bfloat162float(input[base+i+2]), x3=__bfloat162float(input[base+i+3]);
        s0=__fadd_rn(s0,__fmul_rn(x0,x0)); s1=__fadd_rn(s1,__fmul_rn(x1,x1));
        s2=__fadd_rn(s2,__fmul_rn(x2,x2)); s3=__fadd_rn(s3,__fmul_rn(x3,x3));
    }
    float sum=((s0+s1)+s2)+s3;
    __shared__ float shared[512], inv[16];
    unsigned int lane=threadIdx.x, offset=threadIdx.y*blockDim.x;
    ANTFLY_CUDA_TRAINING_FOLD(
        shared[offset+lane]=sum;,
        shared[offset+lane]+=shared[offset+lane+d];,
        sum=shared[offset+lane];,
        sum+=__shfl_down_sync(0xffffffffu,sum,d);)
    if(lane==0) inv[threadIdx.y]=rsqrtf(sum/(float)width+eps);
    __syncthreads();
    for(unsigned int i=lane;i<width;i+=blockDim.x){
        float x=__bfloat162float(input[base+i]);
        float normalized=x*inv[threadIdx.y];
        out[base+i]=__float2bfloat16_rn(normalized*(weight?__bfloat162float(weight[i]):1.0f));
    }
}

// Input-only RMSNorm VJP for a frozen base model. Adapter training never
// produces a gradient for the immutable norm weight. The norm math remains
// F32, while cotangents cross the BF16 output/input cast nodes exactly as in
// the pinned autocast graph.
extern "C" __global__ void termite_embedding_gemma2_rms_norm_input_vjp_f32(
    float* dx, const float* dy, const __nv_bfloat16* input,
    const __nv_bfloat16* weight, unsigned int rows, unsigned int width,
    float eps) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    float square_sum = 0.0f, dot_sum = 0.0f;
    size_t base = (size_t)row * width;
    for (unsigned int i = threadIdx.x; i < width; i += blockDim.x) {
        float x = __bfloat162float(input[base + i]);
        float w = weight ? __bfloat162float(weight[i]) : 1.0f;
        square_sum += x * x;
        float y = __bfloat162float(__float2bfloat16_rn(dy[base + i]));
        dot_sum += y * w * x;
    }
    __shared__ float square_warps[8], dot_warps[8], inv, correction;
    float statistics[] = {square_sum, dot_sum};
    float* const planes[] = {square_warps, dot_warps};
    antfly_warp_block_statistics<false>(statistics, planes, [&](const float* totals) {
        inv = rsqrtf(totals[0] / (float)width + eps);
        correction = totals[1] * inv * inv / (float)width;
    });
    __syncthreads();
    for (unsigned int i = threadIdx.x; i < width; i += blockDim.x) {
        float x = __bfloat162float(input[base + i]);
        float w = weight ? __bfloat162float(weight[i]) : 1.0f;
        float y = __bfloat162float(__float2bfloat16_rn(dy[base + i]));
        dx[base + i] = __bfloat162float(__float2bfloat16_rn(inv * (y * w - x * correction)));
    }
}

extern "C" __global__ void termite_embedding_gemma2_training_rms_norm_input_vjp_f32(
    float* dx, const float* dy, const __nv_bfloat16* input,
    const __nv_bfloat16* weight, unsigned int rows, unsigned int width, float eps) {
    unsigned int row=blockIdx.x*blockDim.y+threadIdx.y;
    if(row>=rows)return;
    size_t base=(size_t)row*width;
    float q0=0.f,q1=0.f,q2=0.f,q3=0.f,d0=0.f,d1=0.f,d2=0.f,d3=0.f;
    for(unsigned int v=threadIdx.x;v<width/4;v+=blockDim.x){
        unsigned int i=v*4;
        #pragma unroll
        for(unsigned int j=0;j<4;j++){
            float x=__bfloat162float(input[base+i+j]);
            float y=__bfloat162float(__float2bfloat16_rn(dy[base+i+j]));
            float dn=y*(weight?__bfloat162float(weight[i+j]):1.f);
            float xx=__fmul_rn(x,x), dnx=__fmul_rn(dn,x);
            if(j==0){q0=__fadd_rn(q0,xx);d0=__fadd_rn(d0,dnx);} else if(j==1){q1=__fadd_rn(q1,xx);d1=__fadd_rn(d1,dnx);}
            else if(j==2){q2=__fadd_rn(q2,xx);d2=__fadd_rn(d2,dnx);} else {q3=__fadd_rn(q3,xx);d3=__fadd_rn(d3,dnx);}
        }
    }
    float square=((q0+q1)+q2)+q3, dot=((d0+d1)+d2)+d3;
    __shared__ float ss[512],ds[512],inv[16],dmean[16];
    unsigned int lane=threadIdx.x,off=threadIdx.y*blockDim.x;
    ANTFLY_CUDA_TRAINING_FOLD(
        ss[off+lane]=square;ds[off+lane]=dot;,
        ss[off+lane]+=ss[off+lane+d];ds[off+lane]+=ds[off+lane+d];,
        square=ss[off+lane];dot=ds[off+lane];,
        square+=__shfl_down_sync(0xffffffffu,square,d);dot+=__shfl_down_sync(0xffffffffu,dot,d);)
    if(lane==0){float mean_eps=square/(float)width+eps;inv[threadIdx.y]=rsqrtf(mean_eps);dmean[threadIdx.y]=((dot*-0.5f)*powf(mean_eps,-1.5f))/(float)width;}
    __syncthreads();
    for(unsigned int i=lane;i<width;i+=blockDim.x){
        float x=__bfloat162float(input[base+i]);
        float y=__bfloat162float(__float2bfloat16_rn(dy[base+i]));
        float dn=y*(weight?__bfloat162float(weight[i]):1.f);
        float direct=__fmul_rn(dn,inv[threadIdx.y]);
        float square_branch=__fmul_rn(__fmul_rn(dmean[threadIdx.y],2.f),x);
        float value=__fadd_rn(direct,square_branch);
        dx[base+i]=__bfloat162float(__float2bfloat16_rn(value));
    }
}

// Serving rounds coefficients/output; training also rounds every product/add.
// Keep lexical traversal, BF16 coefficients and training pointer/store order:
// equivalent helpers/address rewrites increase registers on SM75/80/110.
#define ANTFLY_ROPE_COEFFICIENTS_true \
    float sf, cf; sincosf(angle, &sf, &cf); \
    __nv_bfloat16 s = __float2bfloat16_rn(sf), c = __float2bfloat16_rn(cf);
#define ANTFLY_ROPE_COEFFICIENTS_false \
    float s, c; sincosf(angle, &s, &c); \
    s = __bfloat162float(__float2bfloat16_rn(s)); c = __bfloat162float(__float2bfloat16_rn(c));

#define ANTFLY_ROPE_PAIR(Training, Backward, output, input, base) do { \
    if constexpr (Training) { \
        __nv_bfloat16 x0, x1; \
        if constexpr (Backward) { \
            x0 = __float2bfloat16_rn(valid ? float((input + base)[0]) : 0.0f); \
            x1 = __float2bfloat16_rn(valid ? float((input + base)[pairs]) : 0.0f); \
        } else { x0 = (input + base)[0]; x1 = (input + base)[pairs]; } \
        __nv_bfloat16 a = __float2bfloat16_rn(__bfloat162float(x0) * __bfloat162float(c)); \
        __nv_bfloat16 b; \
        if constexpr (Backward) { \
            b = __float2bfloat16_rn(__bfloat162float(x1) * __bfloat162float(s)); \
        } else { \
            b = __float2bfloat16_rn(-__bfloat162float(x1) * __bfloat162float(s)); \
        } \
        __nv_bfloat16 e = __float2bfloat16_rn(__bfloat162float(x1) * __bfloat162float(c)); \
        __nv_bfloat16 d; \
        if constexpr (Backward) d = __float2bfloat16_rn(-__bfloat162float(x0) * __bfloat162float(s)); \
        else d = __float2bfloat16_rn(__bfloat162float(x0) * __bfloat162float(s)); \
        if constexpr (Backward) { \
            (output + base)[0] = __bfloat162float(__float2bfloat16_rn(__bfloat162float(a) + __bfloat162float(b))); \
            (output + base)[pairs] = __bfloat162float(__float2bfloat16_rn(__bfloat162float(e) + __bfloat162float(d))); \
        } else { \
            (output + base)[0] = __float2bfloat16_rn(__bfloat162float(a) + __bfloat162float(b)); \
            (output + base)[pairs] = __float2bfloat16_rn(__bfloat162float(e) + __bfloat162float(d)); \
        } \
    } else if constexpr (Backward) { \
        float x0 = valid ? float(input[base]) : 0.0f, x1 = valid ? float(input[base + pairs]) : 0.0f; \
        output[base] = x0 * float(c) + x1 * float(s); output[base + pairs] = x1 * float(c) - x0 * float(s); \
    } else { \
        float x0 = __bfloat162float(input[base]), x1 = __bfloat162float(input[base + pairs]); \
        output[base] = __float2bfloat16_rn(x0 * float(c) - x1 * float(s)); \
        output[base + pairs] = __float2bfloat16_rn(x1 * float(c) + x0 * float(s)); \
    } \
} while (0)

#define ANTFLY_ROPE(Training, Backward, q, k, input_q, input_k) do { \
    unsigned pairs = dim >> 1, pair = blockIdx.y * blockDim.x + threadIdx.x; \
    unsigned bs = blockIdx.x, head = blockIdx.z; \
    if (pair >= pairs || bs >= batch * seq || head >= q_heads) return; \
    unsigned b = bs / seq, pos = bs % seq; \
    bool valid = !mask || mask[(size_t)b * seq + pos] != 0; \
    if constexpr (!Backward) { if (!valid) return; } \
    float angle; \
    if constexpr (Training) \
        angle = __fmul_rn((float)pos / rope_scale, eg2_training_rope_inverse_frequency(theta, dim, pair)); \
    else angle = ((float)pos / rope_scale) * powf(theta, -(2.0f * pair) / (float)dim); \
    ANTFLY_ROPE_COEFFICIENTS_##Training \
    size_t qi = (((size_t)b * seq + pos) * q_heads + head) * dim + pair; \
    ANTFLY_ROPE_PAIR(Training, Backward, q, input_q, qi); \
    if (head < kv_heads) { \
        size_t ki = (((size_t)b * seq + pos) * kv_heads + head) * dim + pair; \
        ANTFLY_ROPE_PAIR(Training, Backward, k, input_k, ki); \
    } \
} while (0)

// Typed entrypoints share the buffer ABI and supply the traversal parameters.
#define ANTFLY_ROPE_FORWARD(Kernel, Training) \
extern "C" __global__ void Kernel( \
    __nv_bfloat16* q, __nv_bfloat16* k, const long long* mask, \
    unsigned int batch, unsigned int seq, unsigned int q_heads, \
    unsigned int kv_heads, unsigned int dim, float theta, float rope_scale) { \
    ANTFLY_ROPE(Training, false, q, k, q, k); \
}

#define ANTFLY_ROPE_VJP(Kernel, Training) \
extern "C" __global__ void Kernel( \
    float* dq, float* dk, const float* dq_rotated, const float* dk_rotated, \
    const long long* mask, unsigned int batch, unsigned int seq, \
    unsigned int q_heads, unsigned int kv_heads, unsigned int dim, \
    float theta, float rope_scale) { \
    ANTFLY_ROPE(Training, true, dq, dk, dq_rotated, dk_rotated); \
}

ANTFLY_ROPE_FORWARD(termite_embedding_gemma2_rope_bf16, false)
ANTFLY_ROPE_VJP(termite_embedding_gemma2_rope_vjp_f32, false)
ANTFLY_ROPE_FORWARD(termite_embedding_gemma2_training_rope_bf16, true)
ANTFLY_ROPE_VJP(termite_embedding_gemma2_training_rope_vjp_f32, true)

#undef ANTFLY_ROPE_FORWARD
#undef ANTFLY_ROPE_VJP
#undef ANTFLY_ROPE
#undef ANTFLY_ROPE_PAIR
#undef ANTFLY_ROPE_COEFFICIENTS_true
#undef ANTFLY_ROPE_COEFFICIENTS_false

// Exact bounded-memory route. One warp owns a query/head and performs online
// softmax entirely in registers, so no SxS tensor or block-wide barrier is
// created. Each lane owns up to 16 values (head_dim <= 512).
// local_radius==0 denotes full bidirectional attention.
extern "C" __global__ void termite_embedding_gemma2_attention_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* q, const __nv_bfloat16* k,
    const __nv_bfloat16* v, const long long* mask, unsigned int batch,
    unsigned int seq, unsigned int q_heads, unsigned int kv_heads,
    unsigned int dim, unsigned int local_radius, unsigned int query_offset) {
    unsigned int lane = threadIdx.x;
    unsigned int query = query_offset + blockIdx.x, qh = blockIdx.y, b = blockIdx.z;
    if (b >= batch || query >= seq || qh >= q_heads) return;
    size_t out_base = (((size_t)b * seq + query) * q_heads + qh) * dim;
    if (mask && mask[(size_t)b * seq + query] == 0) {
        for (unsigned int d = lane; d < dim; d += 32u) out[out_base + d] = __float2bfloat16_rn(0.0f);
        return;
    }
    unsigned int kvh = qh * kv_heads / q_heads;
    unsigned int begin = local_radius && query > local_radius ? query - local_radius : 0;
    unsigned int end = local_radius ? min(seq, query + local_radius + 1u) : seq;
    float accum[16];
    #pragma unroll
    for (unsigned int n = 0; n < 16; ++n) accum[n] = 0.0f;
    float max_score = -INFINITY, denominator = 0.0f;
    size_t qb = (((size_t)b * seq + query) * q_heads + qh) * dim;
    for (unsigned int j = begin; j < end; ++j) {
        if (mask && mask[(size_t)b * seq + j] == 0) continue;
        size_t kb = (((size_t)b * seq + j) * kv_heads + kvh) * dim;
        float dot = 0.0f;
        for (unsigned int d = lane; d < dim; d += 32u)
            dot += __bfloat162float(q[qb + d]) * __bfloat162float(k[kb + d]);
        dot = antfly_row_warp_sum(dot);
        dot = __shfl_sync(0xffffffffu, dot, 0);
        float next_max = fmaxf(max_score, dot);
        float prior_scale = isfinite(max_score) ? expf(max_score - next_max) : 0.0f;
        float probability = expf(dot - next_max);
        denominator = denominator * prior_scale + probability;
        max_score = next_max;
        #pragma unroll
        for (unsigned int n = 0; n < 16; ++n) {
            unsigned int d = lane + n * 32u;
            if (d < dim) accum[n] = accum[n] * prior_scale + probability * __bfloat162float(v[kb + d]);
        }
    }
    #pragma unroll
    for (unsigned int n = 0; n < 16; ++n) {
        unsigned int d = lane + n * 32u;
        if (d < dim) out[out_base + d] = __float2bfloat16_rn(denominator > 0.0f ? accum[n] / denominator : 0.0f);
    }
}


#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
extern "C" __global__ void termite_embedding_gemma2_attention_local_flash_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* q, const __nv_bfloat16* k,
    const __nv_bfloat16* v, const long long* mask, unsigned batch,
    unsigned seq, unsigned q_heads, unsigned kv_heads, unsigned dim,
    unsigned local_radius) {
    using namespace nvcuda;
    constexpr unsigned QT = 64, KT = 16, MMA = 16, THREADS = 512, WARPS = 16, QK_WARPS = 4;
    const unsigned tid = threadIdx.x, warp = tid >> 5, lane = tid & 31;
    const unsigned qh = blockIdx.x;
    const unsigned qt = blockIdx.y * QT;
    const unsigned b = blockIdx.z;
    if (blockDim.x != THREADS || b >= batch || qh >= q_heads || qt >= seq ||
        (dim != 256 && dim != 512) || (kv_heads != 1 && kv_heads != 2)) return;
    const unsigned valid_rows = min(QT, seq - qt);
    const unsigned kvh = qh * kv_heads / q_heads;

    extern __shared__ unsigned char storage[];
    __nv_bfloat16* kvs = reinterpret_cast<__nv_bfloat16*>(storage);
    // Four dimension partials for every query share storage with the sixteen
    // warp-private PV merge tiles; both require 4096 FP32 values.
    float* warp_scratch = reinterpret_cast<float*>(kvs + KT * dim);
    __nv_bfloat16* probs = reinterpret_cast<__nv_bfloat16*>(warp_scratch + QK_WARPS * QT * KT);
    float* running_max = reinterpret_cast<float*>(probs + QT * KT);
    float* running_sum = running_max + QT;
    float* alpha = running_sum + QT;

    if (tid < QT) {
        running_max[tid] = -INFINITY;
        running_sum[tid] = 0.0f;
    }
    __syncthreads();

    // Four query subtiles; HD256 uses one output fragment per warp and HD512
    // uses two.
    wmma::fragment<wmma::accumulator, 16, 16, 16, float> out_frag[4][2];
    #pragma unroll
    for (unsigned qsub = 0; qsub < 4; ++qsub)
        for (unsigned f = 0; f < 2; ++f) wmma::fill_fragment(out_frag[qsub][f], 0.0f);
    const unsigned fragments = (dim / 16) / WARPS;

    unsigned first = 0, last = seq;
    if (local_radius) {
        first = qt > local_radius ? qt - local_radius : 0;
        last = min(seq, qt + valid_rows + local_radius);
    }
    first = (first / KT) * KT;
    last = ((last + KT - 1) / KT) * KT;
    for (unsigned kt = first; kt < last; kt += KT) {
        for (unsigned i = tid; i < KT * dim; i += THREADS) {
            const unsigned row = i / dim, d = i - row * dim, key = kt + row;
            const bool valid = key < seq && (!mask || mask[(size_t)b * seq + key]);
            kvs[i] = valid ? k[(((size_t)b * seq + key) * kv_heads + kvh) * dim + d]
                           : __float2bfloat16_rn(0.0f);
        }
        __syncthreads();

        const unsigned qsub = (warp / QK_WARPS) * MMA;
        const unsigned partial = warp % QK_WARPS;
        const unsigned slice = dim / QK_WARPS;
        const unsigned begin_d = partial * slice;
        {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> sf;
            wmma::fill_fragment(sf, 0.0f);
            for (unsigned d = begin_d; d < begin_d + slice; d += MMA) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16,
                    __nv_bfloat16, wmma::row_major> af;
                wmma::fragment<wmma::matrix_b, 16, 16, 16,
                    __nv_bfloat16, wmma::col_major> bf;
                const __nv_bfloat16* q_tile = q +
                    (((size_t)b * seq + qt + qsub) * q_heads + qh) * dim;
                wmma::load_matrix_sync(af, q_tile + d, q_heads * dim);
                wmma::load_matrix_sync(bf, kvs + d, dim);
                wmma::mma_sync(sf, af, bf, sf);
            }
            wmma::store_matrix_sync(
                warp_scratch + partial * QT * KT + qsub * KT, sf, KT,
                wmma::mem_row_major);
        }
        __syncthreads();

        for (unsigned score_base = 0; score_base < QT * KT; score_base += THREADS) {
            const unsigned score_index = score_base + tid;
            const unsigned row = score_index / KT, col = score_index - row * KT;
            const unsigned query = qt + row, key = kt + col;
            float score = 0.0f;
            #pragma unroll
            for (unsigned w = 0; w < QK_WARPS; ++w)
                score += warp_scratch[(w * QT + row) * KT + col];
            const bool visible = row < valid_rows && query < seq && key < seq &&
                (!mask || (mask[(size_t)b * seq + query] && mask[(size_t)b * seq + key])) &&
                (!local_radius || (key <= query + local_radius && query <= key + local_radius));
            score = visible ? score : -INFINITY;
            float tile_max = score;
            #pragma unroll
            for (unsigned offset = KT / 2; offset != 0; offset >>= 1)
                tile_max = fmaxf(tile_max, __shfl_xor_sync(0xffffffffu, tile_max, offset, KT));
            const float old_max = running_max[row];
            const float next_max = fmaxf(old_max, tile_max);
            const float a = isfinite(old_max) ? expf(old_max - next_max) : 0.0f;
            const float p = isfinite(score) ? expf(score - next_max) : 0.0f;
            probs[row * KT + col] = __float2bfloat16_rn(p);
            float tile_sum = p;
            #pragma unroll
            for (unsigned offset = KT / 2; offset != 0; offset >>= 1)
                tile_sum += __shfl_xor_sync(0xffffffffu, tile_sum, offset, KT);
            if (col == 0) {
                alpha[row] = a;
                running_sum[row] = running_sum[row] * a + tile_sum;
                running_max[row] = next_max;
            }
        }
        __syncthreads();
        // QK partials are dead; reuse their shared allocation for V.
        for (unsigned i = tid; i < KT * dim; i += THREADS) {
            const unsigned row = i / dim, d = i - row * dim, key = kt + row;
            const bool valid = key < seq && (!mask || mask[(size_t)b * seq + key]);
            kvs[i] = valid ? v[(((size_t)b * seq + key) * kv_heads + kvh) * dim + d]
                           : __float2bfloat16_rn(0.0f);
        }
        __syncthreads();
        for (unsigned qsub = 0; qsub < 4; ++qsub) {
            wmma::fragment<wmma::matrix_a, 16, 16, 16,
                __nv_bfloat16, wmma::row_major> pf;
            wmma::load_matrix_sync(pf, probs + qsub * MMA * KT, KT);
            for (unsigned f = 0; f < fragments; ++f) {
                const unsigned ot = warp + f * WARPS;
                wmma::fragment<wmma::matrix_b, 16, 16, 16,
                    __nv_bfloat16, wmma::row_major> vf;
                wmma::fragment<wmma::accumulator, 16, 16, 16, float> tf;
                wmma::load_matrix_sync(vf, kvs + ot * MMA, dim);
                wmma::fill_fragment(tf, 0.0f);
                wmma::mma_sync(tf, pf, vf, tf);
                #pragma unroll
                for (unsigned i = 0; i < out_frag[qsub][f].num_elements; ++i) {
                    // SM80+ WMMA m16n16 accumulator ownership: each lane owns
                    // two columns in rows lane/4 and lane/4+8. Applying alpha
                    // here avoids a store/scale/reload of every output tile.
                    const unsigned fragment_row = (lane >> 2) + ((i & 2u) ? 8u : 0u);
                    out_frag[qsub][f].x[i] *= alpha[qsub * MMA + fragment_row];
                    out_frag[qsub][f].x[i] += tf.x[i];
                }
            }
        }
        __syncthreads();
    }

    for (unsigned qsub = 0; qsub < 4; ++qsub) {
        for (unsigned f = 0; f < fragments; ++f) {
            const unsigned ot = warp + f * WARPS;
            float* scratch = warp_scratch + warp * MMA * KT;
            wmma::store_matrix_sync(scratch, out_frag[qsub][f], KT, wmma::mem_row_major);
            __syncwarp();
            for (unsigned i = lane; i < MMA * KT; i += 32) {
                const unsigned row = qsub * MMA + i / KT, col = i % KT;
                if (row < valid_rows && ot * MMA + col < dim) {
                    const float denom = running_sum[row];
                    out[(((size_t)b * seq + qt + row) * q_heads + qh) * dim + ot * MMA + col] =
                        __float2bfloat16_rn(denom > 0.0f ? scratch[i] / denom : 0.0f);
                }
            }
        }
    }
}
#else
extern "C" __global__ void termite_embedding_gemma2_attention_local_flash_bf16(
    __nv_bfloat16*, const __nv_bfloat16*, const __nv_bfloat16*, const __nv_bfloat16*,
    const long long*, unsigned, unsigned, unsigned, unsigned, unsigned, unsigned) {}
#endif


extern "C" __global__ void termite_embedding_gemma2_attention_softmax_bf16(
    __nv_bfloat16* probs, const float* scores, const long long* mask,
    unsigned int sequence, unsigned int query_base, unsigned int rows,
    unsigned int batch_index, unsigned int tile_capacity) {
    unsigned int row = blockIdx.x % rows;
    size_t matrix_offset = (size_t)(blockIdx.x / rows) * tile_capacity * sequence;
    unsigned int lane = threadIdx.x, query = query_base + row;
    bool query_valid = !mask || mask[(size_t)batch_index * sequence + query] != 0;
    extern __shared__ float reduce[];
    float local_max = -INFINITY;
    for (unsigned int key = lane; key < sequence; key += blockDim.x)
        if (query_valid && (!mask || mask[(size_t)batch_index * sequence + key]))
            local_max = fmaxf(local_max, scores[matrix_offset + (size_t)row * sequence + key]);
    reduce[lane] = local_max; __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride; stride >>= 1) {
        if (lane < stride) reduce[lane] = fmaxf(reduce[lane], reduce[lane + stride]);
        __syncthreads();
    }
    float row_max = reduce[0], local_sum = 0.0f;
    for (unsigned int key = lane; key < sequence; key += blockDim.x) {
        bool visible = query_valid && (!mask || mask[(size_t)batch_index * sequence + key]);
        float value = visible ? expf(scores[matrix_offset + (size_t)row * sequence + key] - row_max) : 0.0f;
        probs[matrix_offset + (size_t)row * sequence + key] = __float2bfloat16_rn(value);
        local_sum += value;
    }
    reduce[lane] = local_sum; __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride; stride >>= 1) {
        if (lane < stride) reduce[lane] += reduce[lane + stride];
        __syncthreads();
    }
    float inverse = reduce[0] > 0.0f ? 1.0f / reduce[0] : 0.0f;
    for (unsigned int key = lane; key < sequence; key += blockDim.x)
        probs[matrix_offset + (size_t)row * sequence + key] = __float2bfloat16_rn(
            __bfloat162float(probs[matrix_offset + (size_t)row * sequence + key]) * inverse);
}

extern "C" __global__ void termite_embedding_gemma2_matmul_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input, const __nv_bfloat16* weight,
    unsigned int rows, unsigned int in_dim, unsigned int out_dim) {
    unsigned int row = blockIdx.x;
    unsigned int col = blockIdx.y * blockDim.x + threadIdx.x;
    if (row >= rows || col >= out_dim) return;
    float sum = 0.0f;
    for (unsigned int k = 0; k < in_dim; ++k)
        sum += __bfloat162float(input[(size_t)row * in_dim + k]) * __bfloat162float(weight[(size_t)col * in_dim + k]);
    out[(size_t)row * out_dim + col] = __float2bfloat16_rn(sum);
}

// PLE and FFN products share the same rounded GELU forward boundary.
// The analytic VJP below retains its distinct polynomial evaluation order.
__device__ __forceinline__ __nv_bfloat16 antfly_bf16_gelu_product(const __nv_bfloat16* gate, const __nv_bfloat16* right) {
    float x = __bfloat162float(*gate);
    float g = 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    g = __bfloat162float(__float2bfloat16_rn(g));
    return __float2bfloat16_rn(g * __bfloat162float(*right));
}
#define ANTFLY_BF16_GELU_PRODUCT(Kernel) \
extern "C" __global__ void Kernel(__nv_bfloat16* out, const __nv_bfloat16* gate, const __nv_bfloat16* right, unsigned int count) { \
    unsigned int i=blockIdx.x*blockDim.x+threadIdx.x; if(i>=count)return; \
    out[i]=antfly_bf16_gelu_product(gate+i,right+i); \
}
ANTFLY_BF16_GELU_PRODUCT(termite_embedding_gemma2_gelu_ple_bf16)
ANTFLY_BF16_GELU_PRODUCT(termite_embedding_gemma2_gelu_mul_bf16)
#undef ANTFLY_BF16_GELU_PRODUCT

extern "C" __global__ void termite_embedding_gemma2_gelu_mul_vjp_f32(
    float* dgate, float* dup, const float* dy, const __nv_bfloat16* gate,
    const __nv_bfloat16* up, unsigned int count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float x = __bfloat162float(gate[i]);
    float u = __bfloat162float(up[i]);
    float x2 = x * x;
    float t = tanhf(0.7978845608028654f * (x + 0.044715f * x * x2));
    float gelu = 0.5f * x * (1.0f + t);
    // Forward rounds GELU before the multiply.  The straight-through VJP uses
    // the analytic derivative at the BF16 input and the rounded forward value
    // for d(up), matching the pinned Torch autocast arithmetic profile.
    float rounded_gelu = __bfloat162float(__float2bfloat16_rn(gelu));
    float derivative = 0.5f * (1.0f + t) +
        0.5f * x * (1.0f - t * t) * 0.7978845608028654f *
        (1.0f + 3.0f * 0.044715f * x2);
    float y = __bfloat162float(__float2bfloat16_rn(dy[i]));
    float dgelu = __bfloat162float(__float2bfloat16_rn(y * u));
    dgate[i] = __bfloat162float(__float2bfloat16_rn(dgelu * derivative));
    dup[i] = __bfloat162float(__float2bfloat16_rn(y * rounded_gelu));
}

extern "C" __global__ void termite_embedding_gemma2_residual_vjp_f32(
    float* dresidual, float* dbranch, const float* dout,
    const __nv_bfloat16* ple, unsigned int count, float branch_scale,
    float ple_scale) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float y = __bfloat162float(__float2bfloat16_rn(dout[i]));
    float p = ple ? __bfloat162float(ple[i]) * ple_scale : 1.0f;
    dresidual[i] = __bfloat162float(__float2bfloat16_rn(y * p));
    dbranch[i] = __bfloat162float(__float2bfloat16_rn(y * p * branch_scale));
}

// Asymmetric in-batch contrastive objective. `valid[q,c]` is prepared from
// candidate ids, known-positive ids and duplicate ids by the bounded host
// dataset driver; the designated target must remain valid.  Probabilities and
// gradients are FP32 and deterministic (no atomic accumulation).
extern "C" __global__ void termite_embedding_gemma2_contrastive_prob_f32(
    float* probabilities, float* losses, const float* queries,
    const float* candidates, const unsigned char* valid,
    const unsigned int* targets, unsigned int query_count,
    unsigned int candidate_count, unsigned int width, float inverse_temperature,
    float objective_weight) {
    unsigned int q = blockIdx.x;
    if (q >= query_count) return;
    extern __shared__ float scratch[];
    const float positive_infinity = __int_as_float(0x7f800000);
    float local_max = -positive_infinity;
    for (unsigned int c = threadIdx.x; c < candidate_count; c += blockDim.x) {
        float score = -positive_infinity;
        if (!valid || valid[(size_t)q * candidate_count + c]) {
            score = 0.0f;
            for (unsigned int d = 0; d < width; ++d)
                score += queries[(size_t)q * width + d] * candidates[(size_t)c * width + d];
            score *= inverse_temperature;
        }
        probabilities[(size_t)q * candidate_count + c] = score;
        local_max = fmaxf(local_max, score);
    }
    float maxima[] = {local_max}; float* const maxima_planes[] = {scratch};
    antfly_warp_block_statistics<true>(maxima, maxima_planes, [&](const float* totals) { scratch[0] = totals[0]; }, -positive_infinity);
    __syncthreads();
    float row_max = scratch[0], local_sum = 0.0f;
    for (unsigned int c = threadIdx.x; c < candidate_count; c += blockDim.x) {
        float score = probabilities[(size_t)q * candidate_count + c];
        float value = isfinite(score) ? expf(score - row_max) : 0.0f;
        probabilities[(size_t)q * candidate_count + c] = value;
        local_sum += value;
    }
    float sums[] = {local_sum}; float* const sums_planes[] = {scratch};
    antfly_warp_block_statistics<false>(sums, sums_planes, [&](const float* totals) { scratch[0] = totals[0]; });
    __syncthreads();
    float total = scratch[0];
    for (unsigned int c = threadIdx.x; c < candidate_count; c += blockDim.x)
        probabilities[(size_t)q * candidate_count + c] = total > 0.0f
            ? probabilities[(size_t)q * candidate_count + c] / total : 0.0f;
    if (threadIdx.x == 0) {
        unsigned int target = targets[q];
        bool target_valid = target < candidate_count &&
            (!valid || valid[(size_t)q * candidate_count + target]);
        float probability = target_valid ? probabilities[(size_t)q * candidate_count + target] : 0.0f;
        losses[q] = probability > 0.0f ? -logf(probability) * objective_weight : positive_infinity;
    }
}

// Eager cross_entropy uses exp(log_softmax), then scales its coefficient
// matrix before the two GEMM backward edges. Explicit FP32 operations retain
// those tensor boundaries; fusing raw_logits / temperature with row_max can
// otherwise move BF16 encoder cotangents across a rounding midpoint.
extern "C" __global__ void termite_embedding_gemma2_training_contrastive_ce_f32(
    float* coefficients, float* losses, const float* raw_logits,
    const unsigned char* valid, const unsigned int* targets,
    unsigned int query_count, unsigned int candidate_count,
    float inverse_temperature, float objective_weight, float loss_scale) {
    unsigned int q = blockIdx.x;
    if (q >= query_count) return;
    const float infinity = __int_as_float(0x7f800000);
    unsigned int padded_count = 1;
    while (padded_count < candidate_count) padded_count <<= 1;
    const unsigned int lanes = padded_count < 32 ? padded_count : 32;
    const unsigned int lane = threadIdx.x;
    if (lane >= lanes) return;
    const unsigned int iterations = padded_count / lanes;
    const unsigned int lane_mask = lanes == 32 ? 0xffffffffu : (1u << lanes) - 1u;
    unsigned int target = targets[q];
    if (target >= candidate_count || (valid && !valid[(size_t)q * candidate_count + target])) {
        if (lane == 0) losses[q] = infinity;
        for (unsigned int c = lane; c < candidate_count; c += lanes)
            coefficients[(size_t)q * candidate_count + c] = 0.0f;
        return;
    }
    // Match persistent log_softmax: lane-strided values, a logical warp of
    // min(next_power_of_two(C), 32), and butterfly reductions. C <= 384.
    float scaled_values[16];
    float maximum = -infinity;
    #pragma unroll
    for (unsigned int it = 0; it < 16; ++it) {
        unsigned int c = lane + it * lanes;
        float score = -infinity;
        if (it < iterations && c < candidate_count) {
            size_t i = (size_t)q * candidate_count + c;
            if (!valid || valid[i]) score = __fmul_rn(raw_logits[i], inverse_temperature);
        }
        scaled_values[it] = score;
        if (it < iterations) maximum = fmaxf(maximum, score);
    }
    for (unsigned int offset = lanes / 2; offset; offset >>= 1)
        maximum = fmaxf(maximum, __shfl_xor_sync(lane_mask, maximum, offset, lanes));
    float total = 0.0f;
    for (unsigned int it = 0; it < iterations; ++it)
        total = __fadd_rn(total, expf(__fsub_rn(scaled_values[it], maximum)));
    for (unsigned int offset = lanes / 2; offset; offset >>= 1)
        total = __fadd_rn(total, __shfl_xor_sync(lane_mask, total, offset, lanes));
    float log_total = logf(total);
    float mean_gradient = __fdiv_rn(__fmul_rn(loss_scale, objective_weight), (float)query_count);
    for (unsigned int it = 0; it < iterations; ++it) {
        unsigned int c = lane + it * lanes;
        if (c >= candidate_count) continue;
        size_t i = (size_t)q * candidate_count + c;
        float probability = 0.0f;
        float log_probability = -infinity;
        if (!valid || valid[i]) {
            float shifted = __fsub_rn(scaled_values[it], maximum);
            log_probability = __fsub_rn(shifted, log_total);
            probability = expf(log_probability);
        }
        // Match NLLLoss -> LogSoftmaxBackward: scale the target gradient
        // before the fused probability correction, including partial batches.
        float coefficient = __fmaf_rn(probability, mean_gradient,
            c == targets[q] ? -mean_gradient : 0.0f);
        coefficients[i] = __fmul_rn(coefficient, inverse_temperature);
        if (c == targets[q]) losses[q] = __fmul_rn(-log_probability, objective_weight);
    }
}

extern "C" __global__ void termite_embedding_gemma2_contrastive_query_vjp_f32(
    float* dqueries, const float* probabilities, const float* candidates,
    const unsigned int* targets, unsigned int query_count,
    unsigned int candidate_count, unsigned int width, float gradient_scale) {
    unsigned int d = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int q = blockIdx.y;
    if (q >= query_count || d >= width) return;
    float sum = 0.0f;
    unsigned int target = targets[q];
    for (unsigned int c = 0; c < candidate_count; ++c) {
        float coefficient = probabilities[(size_t)q * candidate_count + c] - (c == target ? 1.0f : 0.0f);
        sum += coefficient * candidates[(size_t)c * width + d];
    }
    dqueries[(size_t)q * width + d] = sum * gradient_scale;
}

extern "C" __global__ void termite_embedding_gemma2_contrastive_candidate_vjp_f32(
    float* dcandidates, const float* probabilities, const float* queries,
    const unsigned int* targets, unsigned int query_count,
    unsigned int candidate_count, unsigned int width, float gradient_scale) {
    unsigned int d = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int c = blockIdx.y;
    if (c >= candidate_count || d >= width) return;
    float sum = 0.0f;
    for (unsigned int q = 0; q < query_count; ++q) {
        float coefficient = probabilities[(size_t)q * candidate_count + c] - (c == targets[q] ? 1.0f : 0.0f);
        sum += coefficient * queries[(size_t)q * width + d];
    }
    dcandidates[(size_t)c * width + d] = sum * gradient_scale;
}

extern "C" __global__ void termite_embedding_gemma2_training_softmax_f32(
    float* probabilities, const float* scores, const long long* mask,
    unsigned int batch, unsigned int sequence, unsigned int heads,
    unsigned int local_radius) {
    unsigned int query = blockIdx.x, head = blockIdx.y, b = blockIdx.z;
    if (query >= sequence || head >= heads || b >= batch) return;
    size_t row = ((size_t)b * heads + head) * sequence + query;
    size_t base = row * sequence;
    bool query_valid = !mask || mask[(size_t)b * sequence + query] != 0;
    float local_max = -__int_as_float(0x7f800000);
    for (unsigned int key = threadIdx.x; key < sequence; key += blockDim.x) {
        unsigned int distance = query > key ? query - key : key - query;
        bool valid = query_valid && (!mask || mask[(size_t)b * sequence + key] != 0) &&
            (local_radius == 0 || distance <= local_radius);
        local_max = fmaxf(local_max, valid ? scores[base + key] : -__int_as_float(0x7f800000));
    }
    // A single warp owns the row, including multiple keys per lane. The
    // saved FP32 probabilities must retain the reference reduction order.
    local_max = antfly_row_warp_max(local_max);
    float maximum = __shfl_sync(0xffffffffu, local_max, 0), local_sum = 0.0f;
    for (unsigned int key = threadIdx.x; key < sequence; key += blockDim.x) {
        unsigned int distance = query > key ? query - key : key - query;
        bool valid = query_valid && (!mask || mask[(size_t)b * sequence + key] != 0) &&
            (local_radius == 0 || distance <= local_radius);
        float value = valid ? expf(__fsub_rn(scores[base + key], maximum)) : 0.0f;
        probabilities[base + key] = value;
        local_sum = __fadd_rn(local_sum, value);
    }
    local_sum = antfly_row_warp_sum(local_sum);
    float sum = __shfl_sync(0xffffffffu, local_sum, 0);
    for (unsigned int key = threadIdx.x; key < sequence; key += blockDim.x)
        probabilities[base + key] = sum > 0.0f ? __fdiv_rn(probabilities[base + key], sum) : 0.0f;
}

extern "C" __global__ void termite_embedding_gemma2_training_softmax_vjp_f32(
    float* dscores, const float* dprobabilities, const float* probabilities,
    unsigned int batch, unsigned int sequence, unsigned int heads) {
    unsigned int query = blockIdx.x, head = blockIdx.y, b = blockIdx.z;
    if (query >= sequence || head >= heads || b >= batch) return;
    size_t base = (((size_t)b * heads + head) * sequence + query) * sequence;
    float dot = 0.0f;
    // ATen materializes grad_output * output before its row reduction.
    // Preserve that FP32 boundary even when a lane owns multiple keys.
    for (unsigned int key = threadIdx.x; key < sequence; key += blockDim.x)
        dot = __fadd_rn(dot, __fmul_rn(dprobabilities[base + key], probabilities[base + key]));
    dot = antfly_row_warp_sum(dot);
    float sum = __shfl_sync(0xffffffffu, dot, 0);
    for (unsigned int key = threadIdx.x; key < sequence; key += blockDim.x) {
        float probability = probabilities[base + key];
        float weighted_gradient = __fmul_rn(dprobabilities[base + key], probability);
        // The fused ATen epilogue has different cancellation behavior from
        // probability * (grad_output - sum), despite algebraic equivalence.
        dscores[base + key] = __fmaf_rn(-probability, sum, weighted_gradient);
    }
}

extern "C" __global__ void termite_embedding_gemma2_scale_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input, unsigned int count, float scale) {
    unsigned int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<count)out[i]=__float2bfloat16_rn(__bfloat162float(input[i])*scale);
}

extern "C" __global__ void termite_embedding_gemma2_bf16_to_f32(
    float* out, const __nv_bfloat16* input, unsigned int count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) out[i] = __bfloat162float(input[i]);
}

extern "C" __global__ void termite_embedding_gemma2_f32_to_bf16(
    __nv_bfloat16* out, const float* input, unsigned int count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) out[i] = __float2bfloat16_rn(input[i]);
}

// Head permutation owns indexing independently of storage and exported ABI.
// Repeat uses the original multiply/divide mapping; equal-head copies compile
// without that arithmetic, preserving the non-repeated schedule.
template<bool Repeat>
__device__ __forceinline__ size_t antfly_bhsd_to_bshd_index(
    size_t i, unsigned sequence, unsigned source_heads, unsigned target_heads, unsigned dim) {
    const unsigned d = i % dim;
    size_t t = i / dim;
    const unsigned position = t % sequence;
    t /= sequence;
    const unsigned head = t % target_heads;
    const unsigned b = t / target_heads;
    const unsigned source_head = Repeat ? head * source_heads / target_heads : head;
    return (((size_t)b * sequence + position) * source_heads + source_head) * dim + d;
}
template<typename Out, typename In> struct AntflyLayoutCast;
template<typename T> struct AntflyLayoutCast<T,T> {
    __device__ __forceinline__ static T apply(T x) { return x; }
};
template<> struct AntflyLayoutCast<float,__nv_bfloat16> {
    __device__ __forceinline__ static float apply(__nv_bfloat16 x) { return __bfloat162float(x); }
};
template<> struct AntflyLayoutCast<__nv_bfloat16,float> {
    __device__ __forceinline__ static __nv_bfloat16 apply(float x) { return __float2bfloat16_rn(x); }
};
template<typename Out, typename In, bool Scatter>
__device__ __forceinline__ void antfly_reorder_heads(
    Out* out, const In* input, unsigned batch, unsigned sequence, unsigned heads, unsigned dim) {
    const size_t count = (size_t)batch * sequence * heads * dim;
    const size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const size_t mapped = antfly_bhsd_to_bshd_index<false>(i, sequence, heads, heads, dim);
    if (Scatter) out[mapped] = AntflyLayoutCast<Out,In>::apply(input[i]);
    else out[i] = AntflyLayoutCast<Out,In>::apply(input[mapped]);
}

// Repeat KV heads while packing; cotangents cross the BF16 cast before GEMM.
extern "C" __global__ void termite_embedding_gemma2_training_pack_bhsd_bf16(
    __nv_bfloat16* out, const void* input, unsigned int batch,
    unsigned int sequence, unsigned int source_heads, unsigned int target_heads,
    unsigned int dim, unsigned int input_f32) {
    const size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= (size_t)batch * target_heads * sequence * dim) return;
    const size_t source = antfly_bhsd_to_bshd_index<true>(i, sequence, source_heads, target_heads, dim);
    out[i] = input_f32 ? __float2bfloat16_rn(((const float*)input)[source])
                      : ((const __nv_bfloat16*)input)[source];
}

// Collapse repeat_kv in FP32 and cross its BF16 backward boundary once.
extern "C" __global__ void termite_embedding_gemma2_training_unpack_bhsd_bf16(
    void* out, const __nv_bfloat16* input, unsigned int batch,
    unsigned int sequence, unsigned int source_heads, unsigned int target_heads,
    unsigned int dim, unsigned int output_f32) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    size_t count = (size_t)batch * sequence * target_heads * dim;
    if (i >= count) return;
    unsigned int d = i % dim;
    size_t t = i / dim;
    unsigned int h = t % target_heads;
    t /= target_heads;
    unsigned int s = t % sequence;
    unsigned int b = t / sequence;
    unsigned int groups = source_heads / target_heads;
    float value = 0.f;
    for (unsigned int g = 0; g < groups; ++g)
        value += __bfloat162float(input[(((size_t)b * source_heads + h * groups + g) * sequence + s) * dim + d]);
    __nv_bfloat16 rounded = __float2bfloat16_rn(value);
    if (output_f32) ((float*)out)[i] = __bfloat162float(rounded);
    else ((__nv_bfloat16*)out)[i] = rounded;
}

// tile-codegen:begin quant_cuda.bshd family (do not edit; run: zig build tile-kernel-codegen -- --write)
extern "C" __global__ void termite_embedding_gemma2_pack_bshd_bf16_f32(
    float* out, const __nv_bfloat16* input, unsigned int batch,
    unsigned int sequence, unsigned int heads, unsigned int dim) {
    antfly_reorder_heads<float,__nv_bfloat16,false>(out, input, batch, sequence, heads, dim);
}

extern "C" __global__ void termite_embedding_gemma2_pack_bshd_f32(
    float* out, const float* input, unsigned int batch,
    unsigned int sequence, unsigned int heads, unsigned int dim) {
    antfly_reorder_heads<float,float,false>(out, input, batch, sequence, heads, dim);
}

extern "C" __global__ void termite_embedding_gemma2_unpack_bhsd_f32(
    float* out, const float* input, unsigned int batch,
    unsigned int sequence, unsigned int heads, unsigned int dim) {
    antfly_reorder_heads<float,float,true>(out, input, batch, sequence, heads, dim);
}

extern "C" __global__ void termite_embedding_gemma2_unpack_bhsd_f32_bf16(
    __nv_bfloat16* out, const float* input, unsigned int batch,
    unsigned int sequence, unsigned int heads, unsigned int dim) {
    antfly_reorder_heads<__nv_bfloat16,float,true>(out, input, batch, sequence, heads, dim);
}
// tile-codegen:end quant_cuda.bshd family

// Retain the mean and normalization division boundaries used by the training
// graph. Frozen inference continues to use its separately qualified kernel.
__device__ __forceinline__ float eg2_training_masked_mean(
    const __nv_bfloat16* projected, const long long* mask, unsigned int b,
    unsigned int sequence, unsigned int width, unsigned int d,
    unsigned int count) {
    // The reference's non-contiguous sequence reduction uses four independent
    // accumulators. Keep masked positions in their original accumulator and
    // materialize the mask multiplication before reducing.
    // At the fixed 768-wide projection, ATen vectorizes four outputs and
    // splits sequences of at least 64 tokens across four virtual Y lanes.
    const unsigned int reduction_rows = sequence >= 64 ? 4 : 1;
    float partials[4] = {0, 0, 0, 0};
    for (unsigned int y = 0; y < reduction_rows; ++y) {
        float values[4] = {0, 0, 0, 0};
        for (unsigned int s = y; s < sequence; s += reduction_rows) {
            float valid = (!mask || mask[(size_t)b * sequence + s]) ? 1.0f : 0.0f;
            float value = __fmul_rn(__bfloat162float(projected[((size_t)b * sequence + s) * width + d]), valid);
            const unsigned int accumulator = (s / reduction_rows) & 3u;
            values[accumulator] = __fadd_rn(values[accumulator], value);
        }
        partials[y] = __fadd_rn(__fadd_rn(__fadd_rn(values[0], values[1]), values[2]), values[3]);
    }
    for (unsigned int offset = reduction_rows / 2; offset; offset >>= 1)
        for (unsigned int y = 0; y < offset; ++y)
            partials[y] = __fadd_rn(partials[y], partials[y + offset]);
    float sum = partials[0];
    return __fdiv_rn(sum, (float)(count ? count : 1));
}

__device__ __forceinline__ float eg2_training_mean_norm(
    const __nv_bfloat16* projected, const long long* mask, unsigned int b,
    unsigned int sequence, unsigned int width, unsigned int count, unsigned int batch) {
    const unsigned int lanes = antfly_four_lane_reduction_width(width, batch);
    float values[4] = {0, 0, 0, 0};
    if (threadIdx.x < lanes) {
        for (unsigned int vector = threadIdx.x; vector * 4 < width; vector += lanes) {
            #pragma unroll
            for (unsigned int i = 0; i < 4; ++i) {
                unsigned int d = vector * 4 + i;
                if (d < width) {
                    float mean = eg2_training_masked_mean(projected, mask, b, sequence, width, d, count);
                    values[i] = __fmaf_rn(mean, mean, values[i]);
                }
            }
        }
    }
    return sqrtf(antfly_four_lane_reduce(values, lanes));
}

extern "C" __global__ void termite_embedding_gemma2_training_mean_l2_f32(
    float* out, const __nv_bfloat16* projected, const long long* mask,
    unsigned int batch, unsigned int seq, unsigned int width) {
    unsigned int b = blockIdx.x;
    if (b >= batch) return;
    unsigned int count = 0;
    for (unsigned int s = 0; s < seq; ++s)
        if (!mask || mask[(size_t)b * seq + s]) ++count;
    float norm = fmaxf(eg2_training_mean_norm(projected, mask, b, seq, width, count, batch), 1.0e-12f);
    for (unsigned int d = threadIdx.x; d < width; d += blockDim.x) {
        float mean = eg2_training_masked_mean(projected, mask, b, seq, width, d, count);
        out[(size_t)b * width + d] = __fdiv_rn(mean, norm);
    }
}

extern "C" __global__ void termite_embedding_gemma2_mean_l2_vjp_f32(
    float* dprojected, const float* dembedding, const __nv_bfloat16* projected,
    const long long* mask, unsigned int batch, unsigned int seq, unsigned int width) {
    unsigned int b = blockIdx.x;
    if (b >= batch) return;
    unsigned int count = 0;
    for (unsigned int s = 0; s < seq; ++s)
        if (!mask || mask[(size_t)b * seq + s]) ++count;
    float raw_norm = eg2_training_mean_norm(projected, mask, b, seq, width, count, batch);
    float norm = fmaxf(raw_norm, 1.0e-12f);
    const unsigned int lanes = antfly_four_lane_reduction_width(width, batch);
    float values[4] = {0, 0, 0, 0};
    if (threadIdx.x < lanes) {
        for (unsigned int vector = threadIdx.x; vector * 4 < width; vector += lanes) {
            #pragma unroll
            for (unsigned int i = 0; i < 4; ++i) {
                unsigned int d = vector * 4 + i;
                if (d < width) {
                    float mean = eg2_training_masked_mean(projected, mask, b, seq, width, d, count);
                    float ratio = __fdiv_rn(__fdiv_rn(mean, norm), norm);
                    float product = __fmul_rn(dembedding[(size_t)b * width + d], ratio);
                    values[i] = __fadd_rn(values[i], __fsub_rn(0.0f, product));
                }
            }
        }
    }
    float norm_gradient = antfly_four_lane_reduce(values, lanes);
    if (raw_norm < 1.0e-12f) norm_gradient = 0.0f;
    for (unsigned int d = threadIdx.x; d < width; d += blockDim.x) {
        float mean = eg2_training_masked_mean(projected, mask, b, seq, width, d, count);
        float direct = __fdiv_rn(dembedding[(size_t)b * width + d], norm);
        float ratio = raw_norm > 0.0f ? __fdiv_rn(mean, raw_norm) : 0.0f;
        float gradient = __fadd_rn(direct, __fmul_rn(norm_gradient, ratio));
        gradient = __fdiv_rn(gradient, (float)(count ? count : 1));
        for (unsigned int s = 0; s < seq; ++s)
            dprojected[((size_t)b * seq + s) * width + d] =
                __fmul_rn(gradient, (!mask || mask[(size_t)b * seq + s]) ? 1.0f : 0.0f);
    }
}

extern "C" __global__ void termite_embedding_gemma2_scale_device_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input, const __nv_bfloat16* scale, unsigned int count) {
    unsigned int i=blockIdx.x*blockDim.x+threadIdx.x;
    if(i<count)out[i]=__float2bfloat16_rn(__bfloat162float(input[i])*__bfloat162float(scale[0]));
}

extern "C" __global__ void termite_embedding_gemma2_scale_device_f32_bf16(
    float* out, const float* input, const __nv_bfloat16* scale, unsigned int count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) {
        float y = __bfloat162float(__float2bfloat16_rn(input[i]));
        out[i] = __bfloat162float(__float2bfloat16_rn(y * __bfloat162float(scale[0])));
    }
}

extern "C" __global__ void termite_embedding_gemma2_accumulate_f32(
    float* destination, const float* source, unsigned int count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) destination[i] += source[i];
}

extern "C" __global__ void termite_embedding_gemma2_accumulate_bf16_boundary_f32(
    float* destination, const float* source, unsigned int count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) {
        float a = __bfloat162float(__float2bfloat16_rn(destination[i]));
        float b = __bfloat162float(__float2bfloat16_rn(source[i]));
        destination[i] = __bfloat162float(__float2bfloat16_rn(a + b));
    }
}

extern "C" __global__ void termite_embedding_gemma2_residual_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* residual, const __nv_bfloat16* branch,
    const __nv_bfloat16* ple, unsigned int count, float branch_scale, float ple_scale) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float x = __bfloat162float(residual[i]) + branch_scale * __bfloat162float(branch[i]);
    if (ple) x += ple_scale * __bfloat162float(ple[i]);
    out[i] = __float2bfloat16_rn(x);
}

extern "C" __global__ void termite_embedding_gemma2_mean_l2_f32(
    float* out, const __nv_bfloat16* projected, const long long* mask,
    unsigned int batch, unsigned int seq, unsigned int width) {
    unsigned int b = blockIdx.x;
    if (b >= batch) return;
    __shared__ float norm_parts[8], inv_norm;
    float norm = 0.0f;
    for (unsigned int d = threadIdx.x; d < width; d += blockDim.x) {
        float sum = 0.0f, count = 0.0f;
        sum = antfly_masked_column_sum<true,true,false,unsigned,size_t>(projected, mask,
            0u, seq, width, d, 1.0f, count, (size_t)b * seq);
        float x = count > 0.0f ? sum / count : 0.0f;
        out[(size_t)b * width + d] = x; norm += x * x;
    }
    float statistics[] = {norm}; float* const planes[] = {norm_parts};
    antfly_warp_block_statistics<false>(statistics, planes, [&](const float* totals) { inv_norm = totals[0] > 0.0f ? rsqrtf(totals[0]) : 0.0f; });
    __syncthreads();
    for (unsigned int d = threadIdx.x; d < width; d += blockDim.x)
        out[(size_t)b * width + d] *= inv_norm;
}

extern "C" __global__ void termite_gemma4_audio_clamp_f32(
    float* out, const float* input, unsigned int count,
    float lower, float upper, unsigned int has_lower, unsigned int has_upper) {
    const unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float value = input[i];
    // Ordered comparisons preserve NaN, matching torch.clamp. Infinities
    // clamp normally when the corresponding finite bound is present.
    if (has_lower && value < lower) value = lower;
    if (has_upper && value > upper) value = upper;
    out[i] = value;
}

extern "C" __global__ void termite_gemma4_audio_glu_rows_f32(
    float* out, const float* input, unsigned int rows, unsigned int dim) {
    const unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    const size_t count = (size_t)rows * dim;
    if ((size_t)i >= count) return;
    const unsigned int row = i / dim;
    const unsigned int column = i - row * dim;
    const size_t base = (size_t)row * dim * 2;
    const float gate = input[base + dim + column];
    out[i] = input[base + column] / (1.0f + expf(-gate));
}

extern "C" __global__ void termite_causal_depthwise_f32(
    float* out, const float* input, const float* weight,
    unsigned int rows, unsigned int dim, unsigned int kernel_size) {
    const unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    const size_t count = (size_t)rows * dim;
    if ((size_t)i >= count) return;
    const unsigned int time = i / dim;
    const unsigned int channel = i - time * dim;
    out[i]=antfly_causal_depthwise<false,false,false,true>(input,weight,input,size_t(time),channel,dim,kernel_size,0u);
}

extern "C" __global__ void antfly_audio_frontend_f32(const float *input, const float *aux, float *out, unsigned mode, unsigned rows, unsigned width, unsigned hop, float floor, unsigned batch) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    unsigned long long total=(unsigned long long)rows*width*(mode==4 ? hop : 1)*batch;
    if ((unsigned long long)i >= total) return;
    if (mode == 0) out[i]=__fmul_rn(input[(i/width)*hop+i%width],aux[i%width]);
    else if (mode == 1) { float re=input[2*i], im=input[2*i+1]; out[i]=sqrtf(__fadd_rn(__fmul_rn(re,re),__fmul_rn(im,im))); }
    else if (mode == 2) out[i]=__fmul_rn(logf(__fadd_rn(input[i],floor)),aux[i/width]);
    else if (mode == 3) {
        if (i>=(unsigned long long)rows*batch) return;
        unsigned b=i/rows, pos=i%rows;
        input += (size_t)b*rows*width; out += (size_t)b*rows*width;
        i=pos;
        float mean=0.f;
        for (unsigned c=0;c<width;c++) mean=__fadd_rn(mean,input[c*rows+i]);
        mean=__fdiv_rn(mean,(float)width);
        float var=0.f;
        for (unsigned c=0;c<width;c++) { float d=__fsub_rn(input[c*rows+i],mean); var=__fadd_rn(var,__fmul_rn(d,d)); }
        var=__fdiv_rn(var,(float)width);
        float scale=__fdiv_rn(1.f,sqrtf(__fadd_rn(var,floor)));
        for (unsigned c=0;c<width;c++) out[c*rows+i]=fmaxf(0.f,__fmul_rn(__fmul_rn(__fsub_rn(input[c*rows+i],mean),scale),aux[c]));
    } else {
        unsigned per=rows*width*hop, b=i/per, j=i%per;
        out[i]=input[(size_t)b*per+((j%hop)*rows+j/(width*hop))*width+(j/hop)%width];
    }
}

// One 512-point transform per CTA, fixed shared scratch and immutable twiddles.
extern "C" __global__ void antfly_audio_fft512_f32(const float *input, const float *tw, float *out) {
    __shared__ float2 values[512];
    unsigned t=threadIdx.x, frame=blockIdx.x;
    for (unsigned i=t;i<512;i+=256) values[__brev(i)>>23]=make_float2(i<320 ? input[(size_t)frame*320+i] : 0.f,0.f);
    __syncthreads();
    for (unsigned len=2;len<=512;len*=2) {
        unsigned half=len/2, j=t%half, a=(t/half)*len+j, b=a+half, w=j*(512/len);
        float2 x=values[a], y=values[b];
        float re=__fsub_rn(__fmul_rn(y.x,tw[2*w]),__fmul_rn(y.y,tw[2*w+1]));
        float im=__fadd_rn(__fmul_rn(y.x,tw[2*w+1]),__fmul_rn(y.y,tw[2*w]));
        values[a]=make_float2(__fadd_rn(x.x,re),__fadd_rn(x.y,im));
        values[b]=make_float2(__fsub_rn(x.x,re),__fsub_rn(x.y,im));
        __syncthreads();
    }
    for (unsigned i=t;i<257;i+=256) { float2 x=values[i]; out[(size_t)frame*257+i]=sqrtf(__fadd_rn(__fmul_rn(x.x,x.x),__fmul_rn(x.y,x.y))); }
}

// Implicit im2col: each tile shares 16 output channels and 16 positions.
// No expanded image matrix is allocated; reduction order remains IC/KY/KX.
extern "C" __global__ void antfly_audio_conv_tiled_f32(float *out,const float *input,const float *weight,const float *bias,unsigned ic,unsigned oc,unsigned h,unsigned w,unsigned oh,unsigned ow) {
    __shared__ float a[16][16], b[16][16];
    unsigned x=threadIdx.x,y=threadIdx.y, channel=blockIdx.y*16+y,pos=blockIdx.x*16+x, batch=blockIdx.z;
    float acc=channel<oc ? bias[channel] : 0.f;
    for(unsigned base=0;base<ic*9;base+=16) {
        unsigned ak=base+x, bk=base+y;
        a[y][x]=(channel<oc && ak<ic*9) ? weight[(size_t)channel*ic*9+ak] : 0.f;
        int iy=(int)((pos/ow)*2+bk/3%3)-1, ix=(int)((pos%ow)*2+bk%3)-1;
        b[y][x]=(pos<oh*ow && bk<ic*9 && iy>=0 && iy<(int)h && ix>=0 && ix<(int)w) ? input[((size_t)batch*ic+bk/9)*h*w+(unsigned)iy*w+(unsigned)ix] : 0.f;
        __syncthreads();
        #pragma unroll
        for(unsigned k=0;k<16;k++) acc=fmaf(a[y][k],b[k][x],acc);
        __syncthreads();
    }
    if(channel<oc && pos<oh*ow) out[((size_t)batch*oc+channel)*oh*ow+pos]=acc;
}

extern "C" __global__ void antfly_audio_logmel_f32(const float *input,const float *plan,const float *mask,float *out,unsigned hop,float floor) {
unsigned tid=threadIdx.x,frame=blockIdx.x;__shared__ float2 values[512];__shared__ float mag[257];
for(unsigned i=tid;i<512;i+=256) { unsigned r=__brev(i)>>23; values[r]=make_float2(i<320 ? input[(size_t)frame*hop+i]*plan[512+i] : 0.f,0.f); }
__syncthreads();
for(unsigned len=2;len<=512;len*=2) { unsigned span=len/2,j=tid%span,a=(tid/span)*len+j,b=a+span,w=j*(512/len);float2 x=values[a],y=values[b];float2 v=make_float2(__fsub_rn(__fmul_rn(y.x,plan[2*w]),__fmul_rn(y.y,plan[2*w+1])),__fadd_rn(__fmul_rn(y.x,plan[2*w+1]),__fmul_rn(y.y,plan[2*w])));values[a]=make_float2(__fadd_rn(x.x,v.x),__fadd_rn(x.y,v.y));values[b]=make_float2(__fsub_rn(x.x,v.x),__fsub_rn(x.y,v.y));__syncthreads(); }
bool bad=false;
for(unsigned i=tid;i<257;i+=256) { float2 x=values[i];mag[i]=sqrtf(__fadd_rn(__fmul_rn(x.x,x.x),__fmul_rn(x.y,x.y)));bad|=!isfinite(mag[i]); }
int nonfinite=__syncthreads_or(bad);
if(tid<128) { unsigned first=(unsigned)plan[832+tid*3],end=(unsigned)plan[833+tid*3],off=(unsigned)plan[834+tid*3];float sum=0.f;for(unsigned k=nonfinite ? 0 : first;k<(nonfinite ? 257 : end);k++) { float weight=k>=first && k<end ? plan[off+k-first] : 0.f; sum=__fadd_rn(sum,__fmul_rn(mag[k],weight)); }out[(size_t)frame*128+tid]=logf(sum+floor)*mask[frame]; }
}
