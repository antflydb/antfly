// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Versioned replay attention, sharing its host schedule and admission with Metal.
// Every reduction has one output owner; no floating point atomic accumulation.
#pragma once
using uint = unsigned int;
using dt_u64 = unsigned long long;



// Mirrors DebertaTrainingAttentionV1Params in metal_runtime.zig and the host
// typedef in metal_kernels.m. V1 has fixed 64-key tiles and 64..256 lanes.
struct DTParams {
    uint batch, sequence, heads, dimension;
    uint relative_rows, phase, begin, count;
    uint batch_index, group_count, operand, threads;
    uint head_index, group_begin, order_begin, order_count;
    uint dropout_threshold;
    float dropout_scale, attention_scale;
    uint backward;
    dt_u64 dropout_stream;
};

__device__ __forceinline__ void dt_bad(unsigned int* status, uint code) {
    atomicOr(status, code);
}
__device__ __forceinline__ void dt_finite(float value, unsigned int* status) {
    if (!isfinite(value)) dt_bad(status, 1u);
}
__device__ __forceinline__ dt_u64 dt_mix(dt_u64 x) {
    x += 0x9e3779b97f4a7c15ul;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ul;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebul;
    return x ^ (x >> 31);
}
__device__ __forceinline__ float dt_keep(const DTParams& p, uint b, uint h, uint q, uint k) {
    const dt_u64 index = ((dt_u64(b) * p.heads + h) * p.sequence + q) * p.sequence + k;
    const uint bits = uint(dt_mix(p.dropout_stream ^ dt_mix(index)) >> 32);
    return bits < p.dropout_threshold ? 0.0f : p.dropout_scale;
}
__device__ __forceinline__ uint dt_data(const DTParams& p, uint b, uint h, uint token) {
    return ((b * p.sequence + token) * p.heads + h) * p.dimension;
}
__device__ __forceinline__ uint dt_total(const DTParams& p) {
    return p.batch * p.sequence * p.heads * p.dimension;
}
__device__ __forceinline__ uint dt_row(const DTParams& p, uint b, uint h, uint q) {
    return (b * p.heads + h) * p.sequence + q;
}
__device__ __forceinline__ uint dt_bucket(const int* control, const DTParams& p, uint q, uint k) {
    return uint(control[6u + p.batch * p.sequence + q + p.sequence - 1u - k]);
}
__device__ __forceinline__ bool dt_valid(const int* control, const DTParams& p, uint b, uint q, uint k) {
    return control[6u + b * p.sequence + q] != 0 && control[6u + b * p.sequence + k] != 0;
}
__device__ __forceinline__ float dt_score(const float* qkv, const float* relative,
                     const int* control, const DTParams& p,
                     uint b, uint h, uint q, uint k, uint bucket,
                     unsigned int* status) {
    // masked_fill uses finite -maxF32 for invalid queries as well as keys.
    if (!dt_valid(control, p, b, q, k)) return -0x1.fffffep127f;
    const uint total = dt_total(p);
    const uint qo = dt_data(p, b, h, q);
    const uint ko = total + dt_data(p, b, h, k);
    const uint ro = (bucket * p.heads + h) * p.dimension;
    const uint kr = p.relative_rows * p.heads * p.dimension + ro;
    float cc = 0.0f, cp = 0.0f, pc = 0.0f;
    for (uint d = 0; d < p.dimension; ++d) {
        const float qv = qkv[qo + d];
        const float kv = qkv[ko + d];
        cc += qv * (kv / p.attention_scale);
        cp += qv * relative[kr + d];
        pc += kv * relative[ro + d];
    }
    const float score = cc + (cp / p.attention_scale + pc / p.attention_scale);
    dt_finite(score, status);
    return score;
}
__device__ __forceinline__ float dt_dp(const float* qkv, const float* dout,
                   const DTParams& p, uint b, uint h, uint q, uint k,
                   unsigned int* status) {
    const uint vo = 2u * dt_total(p) + dt_data(p, b, h, k);
    const uint go = dt_data(p, b, h, q);
    float result = 0.0f;
    for (uint d = 0; d < p.dimension; ++d) result += dout[go + d] * qkv[vo + d];
    dt_finite(result, status);
    return result;
}
struct DTAdjoint { float ds, probability; };
__device__ __forceinline__ DTAdjoint dt_adjoint(const float* qkv, const float* relative,
                           const int* control, const float* dout,
                           const float* rows, const DTParams& p,
                           uint b, uint h, uint q, uint k, uint bucket,
                           unsigned int* status) {
    const uint row = 3u * dt_row(p, b, h, q);
    const float score = dt_score(qkv, relative, control, p, b, h, q, k, bucket, status);
    const float probability = expf(score - rows[row]) / rows[row + 1u];
    const float keep = dt_keep(p, b, h, q, k);
    const float dp = dt_dp(qkv, dout, p, b, h, q, k, status);
    const float ds = dt_valid(control, p, b, q, k) ? probability * (keep * dp - rows[row + 2u]) : 0.0f;
    dt_finite(ds, status);
    dt_finite(probability, status);
    return { ds, probability * keep };
}


static_assert(sizeof(DTParams) == 88, "attention descriptor ABI");
extern "C" __global__ void termite_gliner25_dt_validate_f32(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];
const float* input = p.operand == 0u ? qkv : (p.operand == 1u ? relative : dout);

    if (gid < p.count) dt_finite(input[p.begin + gid], status);
}
extern "C" __global__ void termite_gliner25_dt_validate_control(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];
const int* input = control;

    if (gid >= p.count) return;
    const uint i = p.begin + gid;
    const int value = input[i];
    if (i < 6u) return;
    if (i < 6u + p.batch * p.sequence) {
        if (value != 0 && value != 1) dt_bad(status, 2u);
    } else if (value < 0 || uint(value) >= p.relative_rows) dt_bad(status, 2u);
}
extern "C" __global__ void termite_gliner25_dt_zero(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];

    if (gid < p.count) output[p.begin + gid] = 0.0f;
}

extern "C" __global__ void termite_gliner25_dt_forward(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];

    const uint row = p.begin + group;
    const uint q = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    float* score = tile;
    float* weight = tile + 64;
    float* state = tile + 192;
    if (tid == 0) { state[0] = -INFINITY; state[1] = 0.0f; }
    float value = 0.0f;
    __syncthreads();
    for (uint start = 0; start < p.sequence; start += 64u) {
        const uint count = min(64u, p.sequence - start);
        if (tid < count) {
            const uint k = start + tid;
            score[tid] = dt_score(qkv, relative, control, p, b, h, q, k, dt_bucket(control, p, q, k), status);
        }
        __syncthreads();
        if (tid == 0) {
            float maximum = state[0];
            for (uint i = 0; i < count; ++i) maximum = max(maximum, score[i]);
            const float factor = state[1] == 0.0f ? 0.0f : expf(state[0] - maximum);
            state[2] = factor;
            state[0] = maximum;
            float sum = 0.0f;
            for (uint i = 0; i < count; ++i) {
                const float numerator = expf(score[i] - maximum);
                sum += numerator;
                weight[i] = numerator * dt_keep(p, b, h, q, start + i);
            }
            state[1] = state[1] * factor + sum;
        }
        __syncthreads();
        if (tid < p.dimension) {
            value *= state[2];
            for (uint i = 0; i < count; ++i)
                value += weight[i] * qkv[2u * dt_total(p) + dt_data(p, b, h, start + i) + tid];
        }
        __syncthreads();
    }
    if (tid < p.dimension) {
        value /= state[1];
        dt_finite(value, status);
        output[dt_data(p, b, h, q) + tid] = value;
    }
}

extern "C" __global__ void termite_gliner25_dt_rows(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];

    const uint row = p.begin + group;
    const uint q = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    float* score = tile;
    float* dp = tile + 64;
    float* state = tile + 192;
    if (tid == 0) { state[0] = -INFINITY; state[1] = 0.0f; state[2] = 0.0f; }
    __syncthreads();
    for (uint start = 0; start < p.sequence; start += 64u) {
        const uint count = min(64u, p.sequence - start);
        if (tid < count) {
            const uint k = start + tid;
            score[tid] = dt_score(qkv, relative, control, p, b, h, q, k, dt_bucket(control, p, q, k), status);
            dp[tid] = dt_dp(qkv, dout, p, b, h, q, k, status);
        }
        __syncthreads();
        if (tid == 0) {
            float maximum = state[0];
            for (uint i = 0; i < count; ++i) maximum = max(maximum, score[i]);
            const float factor = state[1] == 0.0f ? 0.0f : expf(state[0] - maximum);
            state[2] *= factor;
            float sum = 0.0f;
            for (uint i = 0; i < count; ++i) {
                const float numerator = expf(score[i] - maximum);
                sum += numerator;
                state[2] += (numerator * dt_keep(p, b, h, q, start + i)) * dp[i];
            }
            state[0] = maximum;
            state[1] = state[1] * factor + sum;
        }
        __syncthreads();
    }
    if (tid == 0) {
        const float delta = state[2] / state[1];
        dt_finite(state[0], status); dt_finite(state[1], status); dt_finite(delta, status);
        rows[3u * row] = state[0]; rows[3u * row + 1u] = state[1]; rows[3u * row + 2u] = delta;
    }
}

extern "C" __global__ void termite_gliner25_dt_dq(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];

    const uint row = p.begin + group;
    const uint q = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    float* ds = tile;
    uint* buckets = reinterpret_cast<uint*>(tile + 128);
    float cc = 0.0f, rel = 0.0f;
    for (uint start = 0; start < p.sequence; start += 64u) {
        const uint count = min(64u, p.sequence - start);
        if (tid < count) {
            const uint k = start + tid, bucket = dt_bucket(control, p, q, k);
            ds[tid] = dt_adjoint(qkv, relative, control, dout, rows, p, b, h, q, k, bucket, status).ds;
            buckets[tid] = bucket;
        }
        __syncthreads();
        if (tid < p.dimension) for (uint i = 0; i < count; ++i) {
            const uint ko = dt_total(p) + dt_data(p, b, h, start + i) + tid;
            const uint kr = ((p.relative_rows + buckets[i]) * p.heads + h) * p.dimension + tid;
            cc += ds[i] * (qkv[ko] / p.attention_scale);
            rel += (ds[i] / p.attention_scale) * relative[kr];
        }
        __syncthreads();
    }
    if (tid < p.dimension) {
        const float value = cc + rel;
        dt_finite(value, status); output[dt_data(p, b, h, q) + tid] = value;
    }
}

extern "C" __global__ void termite_gliner25_dt_dkdv(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];

    const uint row = p.begin + group;
    const uint k = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    float* ds = tile;
    float* probability = tile + 64;
    uint* buckets = reinterpret_cast<uint*>(tile + 128);
    float cc = 0.0f, rel = 0.0f, value = 0.0f;
    for (uint start = 0; start < p.sequence; start += 64u) {
        const uint count = min(64u, p.sequence - start);
        if (tid < count) {
            const uint q = start + tid, bucket = dt_bucket(control, p, q, k);
            const DTAdjoint adjoint = dt_adjoint(qkv, relative, control, dout, rows, p, b, h, q, k, bucket, status);
            ds[tid] = adjoint.ds; probability[tid] = adjoint.probability; buckets[tid] = bucket;
        }
        __syncthreads();
        if (tid < p.dimension) for (uint i = 0; i < count; ++i) {
            const uint qo = dt_data(p, b, h, start + i) + tid;
            const uint qr = (buckets[i] * p.heads + h) * p.dimension + tid;
            cc += ds[i] * qkv[qo];
            rel += (ds[i] / p.attention_scale) * relative[qr];
            value += probability[i] * dout[qo];
        }
        __syncthreads();
    }
    if (tid < p.dimension) {
        const uint index = dt_data(p, b, h, k) + tid;
        const float dk = cc / p.attention_scale + rel;
        dt_finite(dk, status); dt_finite(value, status);
        output[dt_total(p) + index] = dk;
        output[2u * dt_total(p) + index] = value;
    }
}

extern "C" __global__ void termite_gliner25_dt_relative(
const float* qkv, const float* relative, const int* control, const float* dout,
float* output, float* rows, const int* group_rows, const int* offsets,
const int* order, unsigned int* status, DTParams p) {
    const uint tid = threadIdx.x, group = blockIdx.x, gid = blockIdx.x * blockDim.x + threadIdx.x;
    __shared__ float tile[196];

    const uint g = p.group_begin + group;
    const uint bucket = uint(group_rows[g]), h = p.head_index, b = p.batch_index;
    const uint offset = 3u * dt_total(p) + (bucket * p.heads + h) * p.dimension;
    const uint relsize = p.relative_rows * p.heads * p.dimension;
    const uint first = uint(offsets[g]), last = uint(offsets[g + 1u]);
    const uint size = last - first;
    // The rare split path only has one query. Descending offset ordinals give
    // ascending key positions; every output has one writer in every wave.
    const uint begin = min(p.order_begin, size);
    const uint end = min(size, begin + p.order_count);
    float* ds = tile;
    int* keys = reinterpret_cast<int*>(tile + 128);
    float qr = tid < p.dimension ? output[offset + tid] : 0.0f;
    float kr = tid < p.dimension ? output[offset + relsize + tid] : 0.0f;
    for (uint q = p.begin; q < p.begin + p.count; ++q) {
        for (uint start = begin; start < end; start += 64u) {
            const uint count = min(64u, end - start);
            if (tid < count) {
                const int diagonal = order[last - 1u - (start + tid)] - int(p.sequence - 1u);
                const int key = int(q) - diagonal;
                keys[tid] = key;
                ds[tid] = key >= 0 && uint(key) < p.sequence ?
                    dt_adjoint(qkv, relative, control, dout, rows, p, b, h, q, uint(key), bucket, status).ds / p.attention_scale : 0.0f;
            }
            __syncthreads();
            if (tid < p.dimension) for (uint i = 0; i < count; ++i) {
                const int key = keys[i];
                if (key >= 0 && uint(key) < p.sequence) {
                    qr += ds[i] * qkv[dt_total(p) + dt_data(p, b, h, uint(key)) + tid];
                    kr += ds[i] * qkv[dt_data(p, b, h, q) + tid];
                }
            }
            __syncthreads();
        }
    }
    if (tid < p.dimension) {
        dt_finite(qr, status); dt_finite(kr, status);
        output[offset + tid] = qr; output[offset + relsize + tid] = kr;
    }
}
