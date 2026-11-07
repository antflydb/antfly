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
    // Match the tensor-core traversal's BF16 probability-store boundary.
    // The row maximum is fixed across each 16-key value tile before rounding
    // its unnormalized probabilities; the denominator retains original F32 p.
    // Sixteen logits live in one F32 register per warp lane; the value pass
    // shuffles their exact computed bits without rereading Q/K or allocating.
    constexpr unsigned int KT = 16;
    const unsigned int first_tile = (begin / KT) * KT;
    for (unsigned int kt = first_tile; kt < end; kt += KT) {
        float tile_max = -INFINITY;
        float stored_score = -INFINITY;
        for (unsigned int j = max(begin, kt); j < min(end, kt + KT); ++j) {
            if (mask && mask[(size_t)b * seq + j] == 0) continue;
            size_t kb = (((size_t)b * seq + j) * kv_heads + kvh) * dim;
            float dot = 0.0f;
            for (unsigned int d = lane; d < dim; d += 32u)
                dot += __bfloat162float(q[qb + d]) * __bfloat162float(k[kb + d]);
            dot = antfly_row_warp_sum(dot);
            dot = __shfl_sync(0xffffffffu, dot, 0);
            if (lane == j - kt) stored_score = dot;
            tile_max = fmaxf(tile_max, dot);
        }
        const float next_max = fmaxf(max_score, tile_max);
        const float prior_scale = isfinite(max_score) ? expf(max_score - next_max) : 0.0f;
        denominator *= prior_scale;
        #pragma unroll
        for (unsigned int n = 0; n < 16; ++n) accum[n] *= prior_scale;
        max_score = next_max;
        for (unsigned int j = max(begin, kt); j < min(end, kt + KT); ++j) {
            if (mask && mask[(size_t)b * seq + j] == 0) continue;
            size_t kb = (((size_t)b * seq + j) * kv_heads + kvh) * dim;
            const float dot = __shfl_sync(0xffffffffu, stored_score, j - kt);
            const float probability = expf(dot - next_max);
            denominator += probability;
            const float value_probability = __bfloat162float(antfly_attention_probability_bf16(probability));
            #pragma unroll
            for (unsigned int n = 0; n < 16; ++n) {
                unsigned int d = lane + n * 32u;
                if (d < dim) accum[n] += value_probability * __bfloat162float(v[kb + d]);
            }
        }
    }
    #pragma unroll
    for (unsigned int n = 0; n < 16; ++n) {
        unsigned int d = lane + n * 32u;
        if (d < dim) out[out_base + d] = __float2bfloat16_rn(denominator > 0.0f ? accum[n] / denominator : 0.0f);
    }
}


#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
// Each deployment entry below selects its own register/occupancy policy.
// Generic BF16 online attention schedules. The policy is an explicit numerical
// descriptor: both use full-K Tensor Core accumulation with their source
// key-block sizes, traversal, row-reduction and normalization ownership.
template<AntflyAttentionSchedule Schedule, unsigned Dim, bool Split = false>
__device__ __forceinline__ void antfly_attention_block_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* q, const __nv_bfloat16* k,
    const __nv_bfloat16* v, const long long* mask, unsigned batch,
    unsigned seq, unsigned source_seq, unsigned q_heads, unsigned kv_heads,
    unsigned local_radius, float* partial_output, float* partial_lse, unsigned split_count) {
    using namespace nvcuda;
    constexpr unsigned QT = 16, KT = Schedule == AntflyAttentionSchedule::FlashBF16 ? 64 : 128, MMA = 16, THREADS = 128, WARPS = 4;
    constexpr unsigned dim = Dim;
    const unsigned tid = threadIdx.x, warp = tid >> 5, lane = tid & 31;
    const unsigned qh = blockIdx.x, qt = blockIdx.y * QT;
    const unsigned b = Split ? blockIdx.z % batch : blockIdx.z;
    const unsigned split = Split ? blockIdx.z / batch : 0;
    if (blockDim.x != THREADS || b >= batch || qh >= q_heads || qt >= seq || source_seq == 0 || source_seq > seq ||
        (dim != 256 && dim != 512) || (kv_heads != 1 && kv_heads != 2)) return;
    const unsigned valid_rows = min(QT, seq - qt), kvh = qh * kv_heads / q_heads;
    extern __shared__ unsigned char storage[];
    __nv_bfloat16* qs = reinterpret_cast<__nv_bfloat16*>(storage);
    __nv_bfloat16* kvs = qs + QT * dim;
    // Efficient QK/PV read K/V directly. After probability conversion, QS
    // plus dead score bytes hold the exact previous F32 output; reserve only
    // the padding needed to keep that output disjoint from live probabilities.
    constexpr unsigned KV_BYTES = Schedule == AntflyAttentionSchedule::FlashBF16
        ? MMA * dim * 2
        : (QT * dim * 4 > QT * dim * 2 + QT * KT * 4
            ? QT * dim * 4 - QT * dim * 2 - QT * KT * 4 : 0);
    float* scores = reinterpret_cast<float*>(reinterpret_cast<unsigned char*>(kvs) + KV_BYTES);
    __nv_bfloat16* probs = reinterpret_cast<__nv_bfloat16*>(scores + QT * KT);
    float* running_max = reinterpret_cast<float*>(probs + QT * KT);
    float* running_sum = running_max + QT; // Four Flash partials, one efficient sum.
    float* alpha = running_sum + QT * 4;
    for (unsigned i = tid; i < QT * dim; i += THREADS) {
        const unsigned row = i / dim, d = i % dim;
        const bool live = row < valid_rows && qt + row < source_seq && (!mask || mask[(size_t)b * seq + qt + row]);
        qs[i] = live ? q[(((size_t)b * seq + qt + row) * q_heads + qh) * dim + d]
                     : __float2bfloat16_rn(0.0f);
    }
    if (tid < QT) running_max[tid] = -INFINITY;
    if (tid < QT * 4) running_sum[tid] = 0.0f;
    __syncthreads();
    float out_frag[8][2][4] = {};
    const unsigned fragments = dim / (MMA * WARPS);
    unsigned first = 0, last = source_seq;
    if (local_radius) {
        first = qt > local_radius ? qt - local_radius : 0;
        last = min(source_seq, qt + valid_rows + local_radius);
    }
    first = (first / KT) * KT;
    if (Split) {
        const unsigned blocks = (source_seq + KT - 1) / KT;
        const unsigned per_split = (blocks + split_count - 1) / split_count;
        first = max(first, split * per_split * KT);
        last = min(last, min(blocks, (split + 1) * per_split) * KT);
    }
    first = min(first, ((last + KT - 1) / KT) * KT);
    const unsigned tiles = (last + KT - 1) / KT - first / KT;
    for (unsigned tile = 0; tile < tiles; ++tile) {
        const unsigned kt = first + (Schedule == AntflyAttentionSchedule::FlashBF16 ? tiles - 1 - tile : tile) * KT;
        // QS/KVS hold exact previous F32 output during efficient PV. Reload
        // this tile's queries only after every warp finished its prior merge.
        if (Schedule == AntflyAttentionSchedule::EfficientBF16) {
            for (unsigned i = tid; i < QT * dim; i += THREADS) {
                const unsigned row = i / dim, d = i % dim;
                const bool live = row < valid_rows && qt + row < source_seq && (!mask || mask[(size_t)b * seq + qt + row]);
                qs[i] = live ? q[(((size_t)b * seq + qt + row) * q_heads + qh) * dim + d]
                             : __float2bfloat16_rn(0.0f);
            }
            __syncthreads();
        }
        // Each warp owns independent 16-key score columns. Every score keeps
        // its original full ordered K8/K16 accumulator; no split-K summation.
        if (Schedule == AntflyAttentionSchedule::EfficientBF16) {
            for (unsigned sub = warp * MMA; sub < KT; sub += WARPS * MMA) {
                float sf[2][4] = {};
                #pragma unroll
                for (unsigned d = 0; d < dim; d += 8) {
                    const unsigned row = lane / 4, column = (lane % 4) * 2;
                    const unsigned a0 = *reinterpret_cast<const unsigned*>(qs + row * dim + d + column);
                    const unsigned a1 = *reinterpret_cast<const unsigned*>(qs + (row + 8) * dim + d + column);
                    #pragma unroll
                    for (unsigned half = 0; half < 2; ++half) {
                        const unsigned key = kt + sub + half * 8 + row;
                        const bool live = key < source_seq && (!mask || mask[(size_t)b * seq + key]);
                        const unsigned b0 = live
                            ? *reinterpret_cast<const unsigned*>(k + (((size_t)b * seq + key) * kv_heads + kvh) * dim + d + column)
                            : 0u;
                        antfly_attention_mma_bf16_k8(sf[half], a0, a1, b0);
                    }
                }
                #pragma unroll
                for (unsigned half = 0; half < 2; ++half) {
                    #pragma unroll
                    for (unsigned i = 0; i < 4; ++i) {
                        const unsigned row = lane / 4 + (i >= 2 ? 8 : 0);
                        const unsigned col = half * 8 + (lane % 4) * 2 + (i & 1);
                        scores[row * KT + sub + col] = sf[half][i];
                    }
                }
            }
            __syncthreads();
        } else {
            for (unsigned sub = 0; sub < KT; sub += MMA) {
                for (unsigned i = tid; i < MMA * dim; i += THREADS) {
                    const unsigned key = kt + sub + i / dim, d = i % dim;
                    const bool live = key < source_seq && (!mask || mask[(size_t)b * seq + key]);
                    kvs[i] = live ? k[(((size_t)b * seq + key) * kv_heads + kvh) * dim + d]
                                  : __float2bfloat16_rn(0.0f);
                }
                __syncthreads();
                if (warp == 0) {
                wmma::fragment<wmma::accumulator, 16, 16, 16, float> sf;
                wmma::fill_fragment(sf, 0.0f);
                #pragma unroll
                for (unsigned d = 0; d < dim; d += MMA) {
                    wmma::fragment<wmma::matrix_a, 16, 16, 16, __nv_bfloat16, wmma::row_major> af;
                    wmma::fragment<wmma::matrix_b, 16, 16, 16, __nv_bfloat16, wmma::col_major> bf;
                    wmma::load_matrix_sync(af, qs + d, dim);
                    wmma::load_matrix_sync(bf, kvs + d, dim);
                    wmma::mma_sync(sf, af, bf, sf);
                }
                wmma::store_matrix_sync(scores + sub, sf, KT, wmma::mem_row_major);
                }
                __syncthreads();
            }
        }
        // Tensor Core accumulator columns belong to four adjacent lanes:
        // lane c visits 2*c,2*c+1,2*c+8,2*c+9,... in that exact order.
        if (tid < QT * 4) {
            const unsigned row = tid / 4, c = tid % 4, query = qt + row;
            float maximum = -INFINITY;
            for (unsigned n = 0; n < KT / 8; ++n) {
                for (unsigned half = 0; half < 2; ++half) {
                    const unsigned col = n * 8 + c * 2 + half, key = kt + col;
                    const bool live = row < valid_rows && query < source_seq && key < source_seq &&
                        (!mask || (mask[(size_t)b * seq + query] && mask[(size_t)b * seq + key])) &&
                        (!local_radius || (key <= query + local_radius && query <= key + local_radius));
                    float score = live ? scores[row * KT + col] : -INFINITY;
                    if (Schedule == AntflyAttentionSchedule::EfficientBF16) score = antfly_attention_log2_score(score);
                    scores[row * KT + col] = score;
                    maximum = fmaxf(maximum, score);
                }
            }
            maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffffu, maximum, 2));
            maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffffu, maximum, 1));
            const float previous = running_max[row], next = fmaxf(previous, maximum);
            const float scale = isfinite(previous) ? antfly_attention_rescale_bf16<Schedule>(previous, next) : 0.0f;
            const float previous_sum = Schedule == AntflyAttentionSchedule::FlashBF16 ? running_sum[tid] : 0.0f;
            float sum = 0.0f;
            float efficient_sums[4] = {};
            for (unsigned n = 0; n < KT / 8; ++n) {
                for (unsigned half = 0; half < 2; ++half) {
                    const unsigned col = n * 8 + c * 2 + half;
                    const float score = scores[row * KT + col];
                    const float p = isfinite(score) ? antfly_attention_exponent_bf16<Schedule>(score, next) : 0.0f;
                    probs[row * KT + col] = antfly_attention_probability_bf16(p);
                    if (Schedule == AntflyAttentionSchedule::FlashBF16) {
                        // The original nonfirst-tile row reduction contracts
                        // rescale-old + first-probability as one F32 FMA.
                        // Later probabilities keep their sequential additions.
                        if (n == 0 && half == 0) sum = tile == 0 ? p : __fmaf_rn(previous_sum, scale, p);
                        else sum += p;
                    } else efficient_sums[col / 32] += p;
                }
            }
            if (Schedule == AntflyAttentionSchedule::FlashBF16) running_sum[tid] = sum;
            else {
                float total = __fmul_rn(running_sum[row * 4], scale);
                #pragma unroll
                for (unsigned n = 0; n < KT / 32; ++n) {
                    float part = efficient_sums[n];
                    part += __shfl_xor_sync(0xffffffffu, part, 1);
                    part += __shfl_xor_sync(0xffffffffu, part, 2);
                    total += part;
                }
                if (c == 0) running_sum[row * 4] = total;
            }
            if (c == 0) { running_max[row] = next; alpha[row] = scale; }
        }
        __syncthreads();
        #pragma unroll
        for (unsigned f = 0; f < fragments; ++f) {
            for (unsigned half = 0; half < 2; ++half) {
                for (unsigned i = 0; i < 4; ++i) {
                    const unsigned row = (lane >> 2) + (i >= 2 ? 8 : 0);
                    if (Schedule == AntflyAttentionSchedule::FlashBF16) out_frag[f][half][i] *= alpha[row];
                    else {
                        const unsigned col = (warp + f * WARPS) * MMA + half * 8 + (lane % 4) * 2 + (i & 1);
                        reinterpret_cast<float*>(qs)[row * dim + col] = out_frag[f][half][i];
                        out_frag[f][half][i] = 0.0f;
                    }
                }
            }
        }
        if (Schedule == AntflyAttentionSchedule::EfficientBF16) __syncthreads();
        // Efficient PV reads the same ordered K8 operands directly. QS/KVS
        // are no longer needed for QK and hold the previous F32 output exactly;
        // this avoids both per-subtile staging barriers and a duplicate
        // register accumulator without allocating another shared bank.
        if (Schedule == AntflyAttentionSchedule::EfficientBF16) {
            const auto* values = reinterpret_cast<const unsigned short*>(v);
            for (unsigned k8 = 0; k8 < KT; k8 += 8) {
                const unsigned row = lane / 4, column = (lane % 4) * 2;
                const unsigned a0 = *reinterpret_cast<const unsigned*>(probs + row * KT + k8 + column);
                const unsigned a1 = *reinterpret_cast<const unsigned*>(probs + (row + 8) * KT + k8 + column);
                const unsigned key0 = kt + k8 + column, key1 = key0 + 1;
                const bool live0 = key0 < source_seq && (!mask || mask[(size_t)b * seq + key0]);
                const bool live1 = key1 < source_seq && (!mask || mask[(size_t)b * seq + key1]);
                #pragma unroll
                for (unsigned f = 0; f < fragments; ++f) {
                    const unsigned ot = warp + f * WARPS;
                    #pragma unroll
                    for (unsigned half = 0; half < 2; ++half) {
                        const unsigned col = ot * MMA + half * 8 + row;
                        const unsigned lo = live0 ? values[(((size_t)b * seq + key0) * kv_heads + kvh) * dim + col] : 0u;
                        const unsigned hi = live1 ? values[(((size_t)b * seq + key1) * kv_heads + kvh) * dim + col] : 0u;
                        antfly_attention_mma_bf16_k8(out_frag[f][half], a0, a1, lo | (hi << 16));
                    }
                }
            }
        } else {
            for (unsigned sub = 0; sub < KT; sub += MMA) {
                for (unsigned i = tid; i < MMA * dim; i += THREADS) {
                    const unsigned key = kt + sub + i / dim, d = i % dim;
                    const bool live = key < source_seq && (!mask || mask[(size_t)b * seq + key]);
                    kvs[i] = live ? v[(((size_t)b * seq + key) * kv_heads + kvh) * dim + d]
                                  : __float2bfloat16_rn(0.0f);
                }
                __syncthreads();
            unsigned pf[4];
            const unsigned pp = __cvta_generic_to_shared(probs + (lane % 16) * KT + sub + (lane / 16) * 8);
            asm volatile("ldmatrix.sync.aligned.m8n8.x4.shared.b16 {%0,%1,%2,%3}, [%4];"
                : "=r"(pf[0]), "=r"(pf[1]), "=r"(pf[2]), "=r"(pf[3]) : "r"(pp));
            #pragma unroll
            for (unsigned f = 0; f < fragments; ++f) {
                const unsigned ot = warp + f * WARPS;
                unsigned vf[4];
                const unsigned vp = __cvta_generic_to_shared(kvs + (lane % 16) * dim + ot * MMA + (lane / 16) * 8);
                asm volatile("ldmatrix.sync.aligned.m8n8.x4.trans.shared.b16 {%0,%1,%2,%3}, [%4];"
                    : "=r"(vf[0]), "=r"(vf[1]), "=r"(vf[2]), "=r"(vf[3]) : "r"(vp));
                for (unsigned half = 0; half < 2; ++half) {
                    auto& tf = out_frag[f][half];
                    asm volatile("mma.sync.aligned.m16n8k16.row.col.f32.bf16.bf16.f32 "
                        "{%0,%1,%2,%3}, {%4,%5,%6,%7}, {%8,%9}, {%0,%1,%2,%3};"
                        : "+f"(tf[0]), "+f"(tf[1]), "+f"(tf[2]), "+f"(tf[3])
                        : "r"(pf[0]), "r"(pf[1]), "r"(pf[2]), "r"(pf[3]), "r"(vf[half*2]), "r"(vf[half*2+1]));
                }
            }
                __syncthreads();
            }
        }
        // Efficient attention keeps each key block's PV accumulator separate.
        // Its epilogue merges the previous F32 output, and applies final
        // normalization to both terms before the last BF16 output store.
        if (Schedule == AntflyAttentionSchedule::EfficientBF16) {
            #pragma unroll
            for (unsigned f = 0; f < fragments; ++f) {
                for (unsigned half = 0; half < 2; ++half) {
                    for (unsigned i = 0; i < 4; ++i) {
                        const unsigned row = (lane >> 2) + (i >= 2 ? 8 : 0);
                        const float denom = running_sum[row * 4];
                        const float scale = kt + KT >= source_seq && denom > 0.0f ? 1.0f / denom : 1.0f;
                        const float beta = scale * alpha[row];
                        const unsigned col = (warp + f * WARPS) * MMA + half * 8 + (lane % 4) * 2 + (i & 1);
                        const float old_output = reinterpret_cast<const float*>(qs)[row * dim + col];
                        // CUTLASS specializes nonfinal epilogues with alpha=1:
                        // rescale-old + new-PV contracts as that exact FMA.
                        // Final normalization instead rounds beta*old before
                        // the alpha*new-PV FMA. Cancellation makes the two
                        // contraction orders observably different in BF16.
                        out_frag[f][half][i] = tile == 0 ? scale * out_frag[f][half][i]
                            : kt + KT >= source_seq
                                ? __fmaf_rn(scale, out_frag[f][half][i], beta * old_output)
                                : __fmaf_rn(alpha[row], old_output, out_frag[f][half][i]);
                    }
                }
            }
        }
        // QS will become queries again on the next tile. Every prior-output
        // read must finish before any warp starts that role transition.
        if (Schedule == AntflyAttentionSchedule::EfficientBF16) __syncthreads();
    }
    // Efficient epilogues still consume the shared per-block rescale after
    // the last PV barrier. Finish every warp's merge before reusing alpha as
    // the final normalization plane; otherwise this is a real read/write race.
    if (Schedule == AntflyAttentionSchedule::EfficientBF16) __syncthreads();
    if (tid < QT * 4) {
        const unsigned row = tid / 4;
        float sum = running_sum[tid];
        if (Schedule == AntflyAttentionSchedule::FlashBF16) {
            sum += __shfl_xor_sync(0xffffffffu, sum, 2);
            sum += __shfl_xor_sync(0xffffffffu, sum, 1);
        } else sum = running_sum[row * 4];
        if (tid % 4 == 0) {
            // An explicit local bias still normalizes at the full sequence's
            // final key block in CUTLASS. If we skipped its zero-P tail, retain
            // that separate normalization after the last nonempty PV merge.
            const bool normalized_in_loop = tiles && ((last + KT - 1) / KT) * KT >= source_seq;
            alpha[row] = Schedule == AntflyAttentionSchedule::EfficientBF16
                ? (normalized_in_loop || sum <= 0.0f ? 1.0f : 1.0f / sum)
                : (sum > 0.0f ? 1.0f / sum : 0.0f);
            if (Split && row < valid_rows)
                partial_lse[(((size_t)split * batch + b) * q_heads + qh) * seq + qt + row] = sum > 0.0f ? __fadd_rn(running_max[row], __logf(sum)) : -INFINITY;
        }
    }
    __syncthreads();
    #pragma unroll
    for (unsigned f = 0; f < fragments; ++f) {
        const unsigned ot = warp + f * WARPS;
        for (unsigned half = 0; half < 2; ++half) {
            for (unsigned i = 0; i < 4; ++i) {
                const unsigned row = (lane >> 2) + (i >= 2 ? 8 : 0), col = half * 8 + (lane % 4) * 2 + (i & 1);
                if (row < valid_rows && ot * MMA + col < dim) {
                    const float result = out_frag[f][half][i] * alpha[row];
                    if (Split)
                        partial_output[((((size_t)split * batch + b) * q_heads + qh) * seq + qt + row) * dim + ot * MMA + col] = result;
                    else out[(((size_t)b * seq + qt + row) * q_heads + qh) * dim + ot * MMA + col] = __float2bfloat16_rn(result);
                }
            }
        }
    }
}

extern "C" __global__ __launch_bounds__(128, 2) void termite_embedding_gemma2_attention_local_flash_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* q, const __nv_bfloat16* k,
    const __nv_bfloat16* v, const long long* mask, unsigned batch,
    unsigned seq, unsigned source_seq, unsigned q_heads, unsigned kv_heads, unsigned dim,
    unsigned local_radius, unsigned schedule, float* partial_output,
    float* partial_lse, unsigned split_count, const unsigned* logical_metadata) {
    if (logical_metadata) {
        source_seq = logical_metadata[0];
        const unsigned original_mask_kind = logical_metadata[1];
        if (original_mask_kind > 2 || (original_mask_kind != 0 && schedule == (unsigned)AntflyAttentionSchedule::FlashBF16)) return;
    }
    if (schedule == (unsigned)AntflyAttentionSchedule::FlashBF16 && dim == 256) {
        if (split_count > 1)
            antfly_attention_block_bf16<AntflyAttentionSchedule::FlashBF16, 256, true>(out,q,k,v,mask,batch,seq,source_seq,q_heads,kv_heads,local_radius,partial_output,partial_lse,split_count);
        else antfly_attention_block_bf16<AntflyAttentionSchedule::FlashBF16, 256>(out,q,k,v,mask,batch,seq,source_seq,q_heads,kv_heads,local_radius,nullptr,nullptr,1);
    } else if (schedule == (unsigned)AntflyAttentionSchedule::EfficientBF16 && dim == 256)
        antfly_attention_block_bf16<AntflyAttentionSchedule::EfficientBF16, 256>(out,q,k,v,mask,batch,seq,source_seq,q_heads,kv_heads,local_radius,nullptr,nullptr,1);
    else if (schedule == (unsigned)AntflyAttentionSchedule::EfficientBF16 && dim == 512)
        antfly_attention_block_bf16<AntflyAttentionSchedule::EfficientBF16, 512>(out,q,k,v,mask,batch,seq,source_seq,q_heads,kv_heads,local_radius,nullptr,nullptr,1);
}

// The efficient HD256 layout admits four resident blocks without imposing
// HD512 or Flash register requirements on this entry. Arithmetic stays shared.
extern "C" __global__ __launch_bounds__(128, 4) void antfly_attention_efficient_bf16_hd256(
    __nv_bfloat16* out, const __nv_bfloat16* q, const __nv_bfloat16* k,
    const __nv_bfloat16* v, const long long* mask, unsigned batch,
    unsigned seq, unsigned source_seq, unsigned q_heads, unsigned kv_heads, unsigned dim,
    unsigned local_radius, unsigned schedule, float* partial_output,
    float* partial_lse, unsigned split_count, const unsigned* logical_metadata) {
    if (logical_metadata) {
        source_seq = logical_metadata[0];
        const unsigned original_mask_kind = logical_metadata[1];
        if (original_mask_kind > 2 || (original_mask_kind != 0 && schedule == (unsigned)AntflyAttentionSchedule::FlashBF16)) return;
    }
    if (schedule == (unsigned)AntflyAttentionSchedule::EfficientBF16 && dim == 256)
        antfly_attention_block_bf16<AntflyAttentionSchedule::EfficientBF16, 256>(out,q,k,v,mask,batch,seq,source_seq,q_heads,kv_heads,local_radius,nullptr,nullptr,1);
}

#else
extern "C" __global__ void termite_embedding_gemma2_attention_local_flash_bf16(
    __nv_bfloat16*, const __nv_bfloat16*, const __nv_bfloat16*, const __nv_bfloat16*,
    const long long*, unsigned, unsigned, unsigned, unsigned, unsigned, unsigned, unsigned, unsigned, float*, float*, unsigned, const unsigned*) {}
extern "C" __global__ void antfly_attention_efficient_bf16_hd256(
    __nv_bfloat16*, const __nv_bfloat16*, const __nv_bfloat16*, const __nv_bfloat16*,
    const long long*, unsigned, unsigned, unsigned, unsigned, unsigned, unsigned, unsigned, unsigned, float*, float*, unsigned, const unsigned*) {}
#endif


extern "C" __global__ void antfly_attention_split_merge_bf16(
    __nv_bfloat16* out, const float* partial_output, const float* partial_lse,
    unsigned batch, unsigned seq, unsigned heads, unsigned dim, unsigned splits) {
    const unsigned lane = threadIdx.x & 31, row = blockIdx.x;
    if (blockDim.x != 32 || row >= batch * heads * seq || splits < 2 || splits > 128) return;
    unsigned maximum_splits = 1;
    while (maximum_splits < splits) maximum_splits *= 2;
    const unsigned group = min(maximum_splits, 32u), local = lane % group;
    const unsigned pieces = maximum_splits / group;
    float lse[4];
    float maximum = -INFINITY;
    for (unsigned i = 0; i < pieces; ++i) {
        const unsigned split = local + i * group;
        lse[i] = split < splits ? partial_lse[(size_t)split * batch * heads * seq + row] : -INFINITY;
        maximum = fmaxf(maximum, lse[i]);
    }
    for (unsigned offset = group / 2; offset; offset /= 2) maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffffu, maximum, offset));
    if (maximum == -INFINITY) maximum = 0.0f;
    float sum = expf(lse[0] - maximum);
    for (unsigned i = 1; i < pieces; ++i) sum += expf(lse[i] - maximum);
    for (unsigned offset = group / 2; offset; offset /= 2) sum += __shfl_xor_sync(0xffffffffu, sum, offset);
    const float normalization = sum == 0.0f || isnan(sum) ? INFINITY : logf(sum) + maximum;
    const unsigned query = row % seq, head = (row / seq) % heads, b = row / (seq * heads);
    for (unsigned d = lane; d < dim; d += 32) {
        float value = 0.0f;
        for (unsigned split = 0; split < splits; ++split) {
            const float weight = expf(partial_lse[(size_t)split * batch * heads * seq + row] - normalization);
            value = __fmaf_rn(weight, partial_output[((size_t)split * batch * heads * seq + row) * dim + d], value);
        }
        out[(((size_t)b * seq + query) * heads + head) * dim + d] = __float2bfloat16_rn(value);
    }
}


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
    // Every warp must snapshot the maximum before any warp reuses reduce[0]
    // for its local sum; the preceding reduction barrier does not order this.
    __syncthreads();
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
// Historical export; both model adapter calls bind this one native product.
extern "C" __global__ void termite_embedding_gemma2_gelu_mul_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* gate, const __nv_bfloat16* right,
    unsigned int count) {
    unsigned int i=blockIdx.x*blockDim.x+threadIdx.x; if(i>=count)return;
    out[i]=antfly_bf16_gelu_product(gate+i,right+i);
}

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
    // Complete all maximum reads before the next block-statistics invocation
    // lets warp leaders overwrite this same shared scratch with sum partials.
    __syncthreads();
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
    // A target may belong to any warp; finish normalization before lane0
    // reads that other thread's probability for the scalar loss.
    __syncthreads();
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
