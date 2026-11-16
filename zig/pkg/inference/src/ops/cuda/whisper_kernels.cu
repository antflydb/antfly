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

// Conv1d unfolds directly to GEMM rows. Both channel-major mel input and
// time-major hidden input preserve [input_channel, kernel_offset] weight order.
extern "C" __global__ void antfly_whisper_im2col(
    float *out, const float *in, unsigned channels, unsigned time,
    unsigned kernel, unsigned stride, unsigned padding, unsigned out_time,
    unsigned time_major, unsigned count) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    unsigned col = i % (channels * kernel);
    unsigned row = i / (channels * kernel);
    unsigned b = row / out_time, t = row % out_time;
    unsigned c = col / kernel, k = col % kernel;
    int src = (int)(t * stride + k) - (int)padding;
    out[i] = src < 0 || src >= (int)time ? 0.0f :
        in[b * channels * time + (time_major ? src * channels + c : c * time + src)];
}

#include <math.h>
#include <cuda_fp16.h>
#include <stdint.h>
struct WhisperParams {
    unsigned out_dim, suppress_count, ts_begin, text_allowed, ts_min, ts_max;
    unsigned eot, probe_id, mode, token_slot, stats_slot, reserved;
};
__device__ bool whisper_allowed(unsigned i, WhisperParams p, const int *suppress, const unsigned *mask) {
    bool allowed = i == p.eot || (i >= p.ts_begin ? i >= p.ts_min && i < p.ts_max : p.text_allowed != 0);
    if (p.out_dim <= 65536) return allowed && !(mask[i / 32] & (1u << (i % 32)));
    for (unsigned j = 0; allowed && j < p.suppress_count; ++j) if (suppress[j] >= 0 && (unsigned)suppress[j] == i) allowed = false;
    return allowed;
}
// One compact readback replaces a vocabulary-sized device-to-host transfer.
// Category order is best, text, timestamp, raw, matching hostLogitsStats.
extern "C" __global__ void antfly_whisper_stats(float *out, const float *logits, const int *suppress, WhisperParams p) {
    __shared__ float best[4][256];
    __shared__ unsigned ids[4][256];
    __shared__ float sums[3][256];
    __shared__ unsigned mask[2048];
    unsigned t = threadIdx.x;
    for (unsigned i = t; i < 2048; i += 256) mask[i] = 0;
    __syncthreads();
    for (unsigned j = t; j < p.suppress_count; j += 256) if (suppress[j] >= 0 && (unsigned)suppress[j] < 65536) atomicOr(&mask[(unsigned)suppress[j] / 32], 1u << ((unsigned)suppress[j] % 32));
    __syncthreads();
    float values[4] = {-INFINITY, -INFINITY, -INFINITY, -INFINITY};
    unsigned indices[4] = {UINT32_MAX, UINT32_MAX, UINT32_MAX, UINT32_MAX};
    unsigned end = p.out_dim < (blockIdx.x + 1) * 2048 ? p.out_dim : (blockIdx.x + 1) * 2048;
    out += blockIdx.x * 16;
    for (unsigned i = blockIdx.x * 2048 + t; i < end; i += 256) {
        float v = logits[i];
        bool allowed = whisper_allowed(i, p, suppress, mask);
        bool category[4] = {allowed, allowed && i < p.ts_begin, allowed && i >= p.ts_begin, true};
        for (unsigned c = 0; c < 4; ++c) if (category[c] && (indices[c] == UINT32_MAX || v > values[c] || (v == values[c] && i < indices[c]))) {
            values[c] = v; indices[c] = i;
        }
    }
    for (unsigned c = 0; c < 4; ++c) { best[c][t] = values[c]; ids[c][t] = indices[c]; }
    __syncthreads();
    for (unsigned width = 128; width; width >>= 1) {
        if (t < width) for (unsigned c = 0; c < 4; ++c) {
            float v = best[c][t + width]; unsigned id = ids[c][t + width];
            if (id != UINT32_MAX && (ids[c][t] == UINT32_MAX || v > best[c][t] || (v == best[c][t] && id < ids[c][t]))) { best[c][t] = v; ids[c][t] = id; }
        }
        __syncthreads();
    }
    float total = 0, ts = 0, raw = 0;
    for (unsigned i = blockIdx.x * 2048 + t; i < end; i += 256) {
        float v = logits[i];
        if (isfinite(best[3][0])) raw += expf(v - best[3][0]);
        if (whisper_allowed(i, p, suppress, mask)) {
            if (isfinite(best[0][0])) total += expf(v - best[0][0]);
            if (i >= p.ts_begin && isfinite(best[2][0])) ts += expf(v - best[2][0]);
        }
    }
    sums[0][t] = total; sums[1][t] = ts; sums[2][t] = raw;
    __syncthreads();
    for (unsigned width = 128; width; width >>= 1) {
        if (t < width) for (unsigned c = 0; c < 3; ++c) sums[c][t] += sums[c][t + width];
        __syncthreads();
    }
    if (t == 0) {
        for (unsigned c = 0; c < 4; ++c) { out[c * 2] = best[c][0]; out[c * 2 + 1] = __uint_as_float(ids[c][0]); }
        out[8] = best[0][0]; out[9] = sums[0][0];
        out[10] = best[2][0]; out[11] = sums[1][0];
        out[12] = best[3][0]; out[13] = sums[2][0];
        out[14] = p.probe_id < p.out_dim ? logits[p.probe_id] : 0;
        out[15] = p.eot < p.out_dim ? logits[p.eot] : -INFINITY;
    }
}

// Split the key sequence so a short decoder prefix still occupies the GPU.
// Each warp computes a coalesced 64-wide QK dot; each partial retains its
// own maximum and denominator for a stable FP32 merge without an NxN matrix.
__device__ float whisper_kv_read(const float *data, unsigned index, unsigned half_kv) {
    return half_kv ? __half2float(((const __half *)data)[index]) : data[index];
}
extern "C" __global__ void antfly_whisper_cross_partial(float *partial,
    const float *q, const float *k, const float *v, const int64_t *mask,
    unsigned queries, unsigned keys, unsigned heads, unsigned splits, unsigned half_kv) {
    __shared__ float scores[128];
    __shared__ float weights[128];
    unsigned t = threadIdx.x, lane = t % 32, warp = t / 32;
    unsigned h = blockIdx.x % heads, row = blockIdx.x / heads;
    unsigned b = row / queries, start = blockIdx.y * 128;
    const float *qr = q + (row * heads + h) * 64;
    for (unsigned j = warp; j < 128; j += 4) {
        unsigned key = start + j;
        float dot = 0;
        if (key < keys && (!mask || mask[b * keys + key])) {
            unsigned base = ((b * keys + key) * heads + h) * 64;
            dot = qr[lane] * whisper_kv_read(k, base + lane, half_kv) + qr[lane + 32] * whisper_kv_read(k, base + lane + 32, half_kv);
            for (unsigned offset = 16; offset; offset >>= 1) dot += __shfl_down_sync(0xffffffff, dot, offset);
        }
        if (lane == 0) scores[j] = key < keys && (!mask || mask[b * keys + key]) ? dot * 0.125f : -INFINITY;
    }
    __syncthreads();
    float maximum = scores[t];
    for (unsigned off = 16; off; off >>= 1) maximum = fmaxf(maximum, __shfl_xor_sync(0xffffffff, maximum, off));
    if (lane == 0) weights[warp] = maximum;
    __syncthreads();
    maximum = fmaxf(fmaxf(weights[0], weights[1]), fmaxf(weights[2], weights[3]));
    float weight = isfinite(maximum) ? expf(scores[t] - maximum) : 0;
    __syncthreads();
    weights[t] = weight;
    __syncthreads();
    float *out = partial + ((blockIdx.x * splits + blockIdx.y) * 66);
    if (t < 64) {
        float sum = 0;
        for (unsigned j = 0; j < 128 && start + j < keys; ++j)
            sum += weights[j] * whisper_kv_read(v, ((b * keys + start + j) * heads + h) * 64 + t, half_kv);
        out[t + 2] = sum;
    }
    for (unsigned off = 16; off; off >>= 1) weight += __shfl_down_sync(0xffffffff, weight, off);
    if (lane == 0) scores[warp] = weight;
    __syncthreads();
    if (t == 0) { out[0] = maximum; out[1] = scores[0] + scores[1] + scores[2] + scores[3]; }
}
extern "C" __global__ void antfly_whisper_cross_merge(float *out, const float *partial, unsigned splits) {
    unsigned c = threadIdx.x;
    const float *base = partial + blockIdx.x * splits * 66;
    float maximum = -INFINITY;
    for (unsigned s = 0; s < splits; ++s) maximum = fmaxf(maximum, base[s * 66]);
    float numerator = 0, denominator = 0;
    for (unsigned s = 0; s < splits; ++s) {
        const float *p = base + s * 66;
        float scale = isfinite(p[0]) ? expf(p[0] - maximum) : 0;
        numerator += p[c + 2] * scale;
        denominator += p[1] * scale;
    }
    out[blockIdx.x * 64 + c] = denominator > 0 ? numerator / denominator : 0;
}

#include <cuda_fp16.h>
// One warp reduces two output rows. Keep the existing FP16 activation
// rounding and FP32 accumulation, without a cast buffer or cuBLAS dispatch.
__device__ float whisper_activation(float x, unsigned activation) {
    return activation ? 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x))) : x;
}
extern "C" __global__ void antfly_whisper_gemv(float *out, const float *in,
    const __half *weight, const float *bias, unsigned inputs, unsigned outputs, unsigned activation) {
    unsigned lane = threadIdx.x % 32;
    unsigned row = blockIdx.x * 8 + (threadIdx.x / 32) * 2;
    float a = 0, b = 0;
    if ((inputs & 1) == 0) {
        float a1 = 0, b1 = 0;
        for (unsigned c = lane * 2; c < inputs; c += 64) {
            float x = __half2float(__float2half_rn(in[c]));
            float y = __half2float(__float2half_rn(in[c + 1]));
            if (row < outputs) {
                float2 w = __half22float2(*reinterpret_cast<const __half2 *>(weight + row * inputs + c));
                a = fmaf(x, w.x, a); a1 = fmaf(y, w.y, a1);
            }
            if (row + 1 < outputs) {
                float2 w = __half22float2(*reinterpret_cast<const __half2 *>(weight + (row + 1) * inputs + c));
                b = fmaf(x, w.x, b); b1 = fmaf(y, w.y, b1);
            }
        }
        a += a1; b += b1;
    } else {
        for (unsigned c = lane; c < inputs; c += 32) {
            float x = __half2float(__float2half_rn(in[c]));
            if (row < outputs) a = fmaf(x, __half2float(weight[row * inputs + c]), a);
            if (row + 1 < outputs) b = fmaf(x, __half2float(weight[(row + 1) * inputs + c]), b);
        }
    }
    for (unsigned offset = 16; offset; offset >>= 1) {
        a += __shfl_down_sync(0xffffffff, a, offset);
        b += __shfl_down_sync(0xffffffff, b, offset);
    }
    if (lane == 0) {
        if (row < outputs) out[row] = whisper_activation(a + (bias ? bias[row] : 0), activation);
        if (row + 1 < outputs) out[row + 1] = whisper_activation(b + (bias ? bias[row + 1] : 0), activation);
    }
}

extern "C" __global__ void antfly_whisper_stats_merge(float *out, const float *partial, unsigned blocks) {
    unsigned t = threadIdx.x;
    if (t < 4) {
        float maximum = -INFINITY; unsigned index = UINT32_MAX;
        for (unsigned b = 0; b < blocks; ++b) {
            float v = partial[b * 16 + t * 2];
            unsigned id = __float_as_uint(partial[b * 16 + t * 2 + 1]);
            if (id != UINT32_MAX && (index == UINT32_MAX || v > maximum || (v == maximum && id < index))) { maximum = v; index = id; }
        }
        out[t * 2] = maximum; out[t * 2 + 1] = __uint_as_float(index);
    }
    __syncthreads();
    if (t < 3) {
        unsigned category = t == 0 ? 0 : (t == 1 ? 2 : 3);
        float maximum = out[category * 2], sum = 0;
        for (unsigned b = 0; b < blocks; ++b) {
            float m = partial[b * 16 + 8 + t * 2];
            if (isfinite(m)) sum += partial[b * 16 + 9 + t * 2] * expf(m - maximum);
        }
        out[8 + t * 2] = maximum; out[9 + t * 2] = sum;
    }
    if (t == 0) { out[14] = partial[14]; out[15] = partial[15]; }
}

extern "C" __global__ void antfly_whisper_qkv(float *q, float *k, float *v, const float *in,
    const __half *qw, const __half *kw, const __half *vw,
    const float *qb, const float *kb, const float *vb, unsigned inputs, unsigned outputs) {
    float *out = blockIdx.y == 0 ? q : (blockIdx.y == 1 ? k : v);
    const __half *weight = blockIdx.y == 0 ? qw : (blockIdx.y == 1 ? kw : vw);
    const float *bias = blockIdx.y == 0 ? qb : (blockIdx.y == 1 ? kb : vb);
    unsigned lane = threadIdx.x % 32;
    unsigned row = blockIdx.x * 8 + (threadIdx.x / 32) * 2;
    float a = 0, b = 0;
    if ((inputs & 1) == 0) {
        float a1 = 0, b1 = 0;
        for (unsigned c = lane * 2; c < inputs; c += 64) {
            float x = __half2float(__float2half_rn(in[c]));
            float y = __half2float(__float2half_rn(in[c + 1]));
            if (row < outputs) {
                float2 w = __half22float2(*reinterpret_cast<const __half2 *>(weight + row * inputs + c));
                a = fmaf(x, w.x, a); a1 = fmaf(y, w.y, a1);
            }
            if (row + 1 < outputs) {
                float2 w = __half22float2(*reinterpret_cast<const __half2 *>(weight + (row + 1) * inputs + c));
                b = fmaf(x, w.x, b); b1 = fmaf(y, w.y, b1);
            }
        }
        a += a1; b += b1;
    } else {
        for (unsigned c = lane; c < inputs; c += 32) {
            float x = __half2float(__float2half_rn(in[c]));
            if (row < outputs) a = fmaf(x, __half2float(weight[row * inputs + c]), a);
            if (row + 1 < outputs) b = fmaf(x, __half2float(weight[(row + 1) * inputs + c]), b);
        }
    }
    for (unsigned offset = 16; offset; offset >>= 1) {
        a += __shfl_down_sync(0xffffffff, a, offset);
        b += __shfl_down_sync(0xffffffff, b, offset);
    }
    if (lane == 0) {
        if (row < outputs) out[row] = a + (bias ? bias[row] : 0);
        if (row + 1 < outputs) out[row + 1] = b + (bias ? bias[row + 1] : 0);
    }
}

extern "C" __global__ void antfly_whisper_add_norm_sum(
    float* dst,
    float* residual,
    const float* a,
    const float* b,
    const float* gamma,
    const float* beta,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    const unsigned int base = row * dim;
    unsigned int tid = threadIdx.x;
    __shared__ float sums[256];
    __shared__ float sumsq[256];

    float local_sum = 0.0f;
    float local_sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = a[base + i] + b[base + i];
        local_sum += x;
        local_sumsq += x * x;
    }
    sums[tid] = local_sum;
    sumsq[tid] = local_sumsq;
    __syncthreads();

    for (unsigned int stride = blockDim.x >> 1u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            sums[tid] += sums[tid + stride];
            sumsq[tid] += sumsq[tid + stride];
        }
        __syncthreads();
    }

    float mean = sums[0] / (float)dim;
    float var = fmaxf(sumsq[0] / (float)dim - mean * mean, 0.0f);
    float inv = rsqrtf(var + eps);
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = a[base + i] + b[base + i];
        residual[base + i] = x;
        dst[base + i] = (x - mean) * inv * gamma[i] + beta[i];
    }
}

