// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// The three strict-FP32 batched GEMMs have layouts [head,query,key] and
// [head,query,relative]. Each CTA owns one complete softmax row. Boundary
// descriptors supply compact bucket IDs and i32 masks; legacy Decide supplies
// expanded relative rows and i64 masks. No storage or arithmetic is reduced.
extern "C" __global__ void termite_gliner25_attention_softmax_f32_v2(
    float* scores, const float* c2p, const float* p2c,
    const void* mask_data, const int* buckets, unsigned sequence,
    unsigned relative, unsigned heads, unsigned first_head, unsigned boundary,
    unsigned model_heads) {
    const unsigned qi = blockIdx.x % sequence;
    const unsigned head = blockIdx.x / sequence;
    if (head >= heads) return;
    const unsigned document = (first_head + head) / model_heads;
    const unsigned lane = threadIdx.x & 31u, warp = threadIdx.x >> 5u;
    const unsigned row = (head * sequence + qi) * sequence;
    __shared__ float reductions[8];
    float value[2] = {-INFINITY, -INFINITY};
    const bool valid_query = !boundary || ((const int*)mask_data)[document * sequence + qi] != 0;
    float maximum = -INFINITY;
    #pragma unroll
    for (unsigned i = 0; i < 2; ++i) {
        const unsigned ki = threadIdx.x + i * 256u;
        if (ki < sequence && valid_query) {
            const bool valid_key = boundary ? ((const int*)mask_data)[document * sequence + ki] != 0
                : ((const long long*)mask_data)[document * sequence + ki] != 0;
            if (valid_key) {
                const unsigned offset = qi + sequence - 1u - ki;
                const int bucket = buckets ? buckets[offset] : (int)offset;
                if (bucket < 0 || (unsigned)bucket >= relative) value[i] = NAN;
                else value[i] = (scores[row + ki]
                    + c2p[(head * sequence + qi) * relative + bucket]
                    + p2c[(head * sequence + ki) * relative + bucket]) * rsqrtf(192.0f);
            }
        }
        maximum = fmaxf(maximum, value[i]);
    }
    for (unsigned d = 16; d; d >>= 1) maximum = fmaxf(maximum, __shfl_down_sync(0xffffffffu, maximum, d));
    if (lane == 0) reductions[warp] = maximum;
    __syncthreads();
    if (warp == 0) {
        maximum = lane < 8 ? reductions[lane] : -INFINITY;
        for (unsigned d = 16; d; d >>= 1) maximum = fmaxf(maximum, __shfl_down_sync(0xffffffffu, maximum, d));
        if (lane == 0) reductions[0] = maximum;
    }
    __syncthreads();
    maximum = reductions[0];
    __syncthreads();
    float sum = 0.0f;
    #pragma unroll
    for (unsigned i = 0; i < 2; ++i) {
        value[i] = value[i] == -INFINITY ? 0.0f : expf(value[i] - maximum);
        sum += value[i];
    }
    for (unsigned d = 16; d; d >>= 1) sum += __shfl_down_sync(0xffffffffu, sum, d);
    if (lane == 0) reductions[warp] = sum;
    __syncthreads();
    if (warp == 0) {
        sum = lane < 8 ? reductions[lane] : 0.0f;
        for (unsigned d = 16; d; d >>= 1) sum += __shfl_down_sync(0xffffffffu, sum, d);
        if (lane == 0) reductions[0] = sum;
    }
    __syncthreads();
    const float denominator = reductions[0];
    #pragma unroll
    for (unsigned i = 0; i < 2; ++i) {
        const unsigned ki = threadIdx.x + i * 256u;
        if (ki < sequence) scores[row + ki] = denominator == 0.0f ? 0.0f : value[i] / denominator;
    }
}

// Device pointer arrays batch interleaved heads across document boundaries,
// without packing activations or repeating shared relative-position vectors.
extern "C" __global__ void termite_gliner25_attention_pointer_views_f32_v1(
    unsigned long long* arrays, const float* q, const float* k, const float* v,
    const float* qr, const float* kr, float* output, float* cc, float* cp,
    float* pc, unsigned sequence, unsigned heads, unsigned relative,
    unsigned first, unsigned count) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned head = (first + i) % heads, document = (first + i) / heads;
    const size_t input_offset = ((size_t)document * sequence * heads + head) * 64u;
    const size_t relative_offset = (size_t)head * 64u;
    arrays[i] = (unsigned long long)(q + input_offset);
    arrays[count + i] = (unsigned long long)(k + input_offset);
    arrays[2u * count + i] = (unsigned long long)(v + input_offset);
    arrays[3u * count + i] = (unsigned long long)(qr + relative_offset);
    arrays[4u * count + i] = (unsigned long long)(kr + relative_offset);
    arrays[5u * count + i] = (unsigned long long)(cc + (size_t)i * sequence * sequence);
    arrays[6u * count + i] = (unsigned long long)(cp + (size_t)i * sequence * relative);
    arrays[7u * count + i] = (unsigned long long)(pc + (size_t)i * sequence * relative);
    arrays[8u * count + i] = (unsigned long long)(output + input_offset);
}

// One conversion pass stages all five operands. Each tensor retains its
// interleaved head layout; no QKV packing or relative-table replication.
extern "C" __global__ void termite_gliner25_attention_stage_bf16_v1(
    __nv_bfloat16* q16, __nv_bfloat16* k16, __nv_bfloat16* v16,
    __nv_bfloat16* qr16, __nv_bfloat16* kr16,
    const float* q, const float* k, const float* v,
    const float* qr, const float* kr, unsigned tokens, unsigned positions) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < tokens) q16[i] = __float2bfloat16_rn(q[i]);
    else if ((i -= tokens) < tokens) k16[i] = __float2bfloat16_rn(k[i]);
    else if ((i -= tokens) < tokens) v16[i] = __float2bfloat16_rn(v[i]);
    else if ((i -= tokens) < positions) qr16[i] = __float2bfloat16_rn(qr[i]);
    else if ((i -= positions) < positions) kr16[i] = __float2bfloat16_rn(kr[i]);
}

extern "C" __global__ void termite_gliner25_attention_pointer_views_bf16_v2(
    unsigned long long* arrays, const __nv_bfloat16* q, const __nv_bfloat16* k,
    const __nv_bfloat16* v, const __nv_bfloat16* qr, const __nv_bfloat16* kr,
    float* output, __nv_bfloat16* cc, __nv_bfloat16* cp, __nv_bfloat16* pc, __nv_bfloat16* probabilities,
    unsigned sequence, unsigned heads, unsigned relative, unsigned first, unsigned count) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned head = (first + i) % heads, document = (first + i) / heads;
    const size_t input_offset = ((size_t)document * sequence * heads + head) * 64u;
    const size_t relative_offset = (size_t)head * 64u;
    arrays[i] = (unsigned long long)(q + input_offset);
    arrays[count + i] = (unsigned long long)(k + input_offset);
    arrays[2u * count + i] = (unsigned long long)(v + input_offset);
    arrays[3u * count + i] = (unsigned long long)(qr + relative_offset);
    arrays[4u * count + i] = (unsigned long long)(kr + relative_offset);
    arrays[5u * count + i] = (unsigned long long)(cc + (size_t)i * sequence * sequence);
    arrays[6u * count + i] = (unsigned long long)(cp + (size_t)i * sequence * relative);
    arrays[7u * count + i] = (unsigned long long)(pc + (size_t)i * sequence * relative);
    arrays[8u * count + i] = (unsigned long long)(output + input_offset);
    arrays[9u * count + i] = (unsigned long long)(probabilities + (size_t)i * sequence * sequence);
}

// Score matrices and probabilities use BF16 storage; reductions remain F32.
// One warp per query and eight adjacent queries per CTA share the relative
// score working set. P2C is [head,relative,key], so distant log-bucket runs
// read contiguous keys instead of striding by the relative table width.
extern "C" __global__ void termite_gliner25_attention_softmax_bf16_v2(
    __nv_bfloat16* probabilities, const __nv_bfloat16* scores, const __nv_bfloat16* c2p, const __nv_bfloat16* p2c,
    const void* mask_data, const int* buckets, unsigned sequence,
    unsigned relative, unsigned heads, unsigned first_head, unsigned boundary,
    unsigned model_heads, unsigned relative_pitch) {
    const unsigned lane = threadIdx.x & 31u;
    const unsigned row = blockIdx.x * 8u + (threadIdx.x >> 5u);
    if (row >= heads * sequence) return;
    const unsigned qi = row % sequence, head = row / sequence;
    const unsigned document = (first_head + head) / model_heads;
    const bool valid_query = !boundary || ((const int*)mask_data)[document * sequence + qi] != 0;
    float value[16];
    float maximum = -INFINITY;
    #pragma unroll
    for (unsigned i = 0; i < 16; ++i) {
        const unsigned ki = lane + i * 32u;
        float x = -INFINITY;
        if (ki < sequence && valid_query) {
            const bool valid_key = boundary ? ((const int*)mask_data)[document * sequence + ki] != 0
                : ((const long long*)mask_data)[document * sequence + ki] != 0;
            if (valid_key) {
                const unsigned offset = qi + sequence - 1u - ki;
                const int bucket = buckets ? buckets[offset] : (int)offset;
                if (bucket < 0 || (unsigned)bucket >= relative) x = NAN;
                else x = (__bfloat162float(scores[row * sequence + ki])
                    + __bfloat162float(c2p[row * relative_pitch + bucket])
                    + __bfloat162float(p2c[(head * relative_pitch + bucket) * sequence + ki])) * rsqrtf(192.0f);
            }
        }
        value[i] = x;
        maximum = fmaxf(maximum, x);
    }
    for (unsigned d = 16; d; d >>= 1) maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffffu, maximum, d));
    float sum = 0.0f;
    #pragma unroll
    for (unsigned i = 0; i < 16; ++i) {
        value[i] = value[i] == -INFINITY ? 0.0f : expf(value[i] - maximum);
        sum += value[i];
    }
    for (unsigned d = 16; d; d >>= 1) sum += __shfl_xor_sync(0xffffffffu, sum, d);
    #pragma unroll
    for (unsigned i = 0; i < 16; ++i) {
        const unsigned ki = lane + i * 32u;
        if (ki < sequence) probabilities[row * sequence + ki] = __float2bfloat16_rn(sum == 0.0f ? 0.0f : value[i] / sum);
    }
}


extern "C" __global__ void termite_gliner25_ffn_bias_gelu_bf16_v1(
    __nv_bfloat16* output, const __nv_bfloat16* input, const float* bias,
    unsigned count, unsigned width, unsigned exact) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const float x = __bfloat162float(input[i]) + bias[i % width];
    const float y = exact ? 0.5f*x*(1.0f+erff(x*0.7071067811865476f))
        : 0.5f*x*(1.0f+tanhf(0.7978845608028654f*(x+0.044715f*x*x*x)));
    output[i] = __float2bfloat16_rn(y);
}
