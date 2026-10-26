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

#include <math.h>

__device__ __forceinline__ void conv_channel(
    const float *x, const float *weight, const float *history,
    float *next_history, float *out, int seq, int channels, int initialized, int c) {
    if (c >= channels) return;
    for (int t = 0; t < seq; ++t) {
        float sum = 0;
        for (int k = 0; k < 4; ++k) {
            int source = t + k - 3;
            float v = source >= 0 ? x[source * channels + c]
                : (initialized ? history[c * 4 + source + 4] : 0.0f);
            sum += v * weight[c * 4 + k];
        }
        out[t * channels + c] = sum / (1.0f + expf(-sum));
    }
    // The host oracle right-pads an initial prefix shorter than four rows.
    // Continuations consume exactly that stored representation.
    for (int k = 0; k < 4; ++k) {
        int source = (initialized || seq >= 4) ? seq + k - 4 : k;
        next_history[c * 4 + k] = source >= 0 && source < seq
            ? x[source * channels + c]
            : (initialized && source < 0 ? history[c * 4 + source + 4] : 0.0f);
    }
}

extern "C" __global__ void lighton_conv(
    const float *x, const float *weight, const float *history,
    float *next_history, float *out, int seq, int channels, int initialized) {
    conv_channel(x, weight, history, next_history, out, seq, channels, initialized,
                 blockIdx.x * blockDim.x + threadIdx.x);
}

__device__ float warp_sum(float x) {
    for (int d = 16; d; d >>= 1) x += __shfl_down_sync(0xffffffffu, x, d);
    return x;
}

extern "C" __global__ void lighton_normalize(float *conv, int channels, int key_heads) {
    __shared__ float partial[4];
    int d = threadIdx.x;
    int head = blockIdx.x % (2 * key_heads);
    int t = blockIdx.x / (2 * key_heads);
    float *row = conv + t * channels + head * 128;
    float v = row[d];
    float sum = warp_sum(v * v);
    if ((d & 31) == 0) partial[d >> 5] = sum;
    __syncthreads();
    if (d == 0) partial[0] = 1.0f / sqrtf(partial[0] + partial[1] + partial[2] + partial[3] + 1e-6f);
    __syncthreads();
    row[d] = v * partial[0] * (head < key_heads ? 0.08838834764831845f : 1.0f);
}

__device__ __forceinline__ void recurrent_head(
    const float *conv, const float *beta, const float *a,
    const float *a_log, const float *dt_bias, float *state, float *out,
    int seq, int key_heads, int value_heads, int initialized, int vh, const float* cached_gates = nullptr) {
    // Each warp owns eight value columns, distributed over four key partitions.
    // State loads are contiguous in the value dimension and stay in registers
    // for the complete prefix. Four warps cover 32 columns per block.
    int lane = threadIdx.x & 31;
    int key_partition = lane >> 3;
    int column = blockIdx.x * 32 + (threadIdx.x >> 5) * 8 + (lane & 7);
    int kh = vh / (value_heads / key_heads);
    int channels = (2 * key_heads + value_heads) * 128;
    int width = value_heads * 128;
    float memory[32];
    #pragma unroll
    for (int i = 0; i < 32; ++i) {
        int k = key_partition * 32 + i;
        memory[i] = initialized ? state[(vh * 128 + k) * 128 + column] : 0.0f;
    }
    for (int t = 0; t < seq; ++t) {
        float decay, b;
        if (cached_gates != nullptr) {
            decay = cached_gates[t * width];
            b = cached_gates[t * width + 1];
        } else {
            float p = a[t * value_heads + vh] + dt_bias[vh];
            float softplus = p > 20.0f ? p : log1pf(expf(p));
            decay = expf(-expf(a_log[vh]) * softplus);
            b = 1.0f / (1.0f + expf(-beta[t * value_heads + vh]));
        }
        const float *q = conv + t * channels + kh * 128;
        const float *k = q + key_heads * 128;
        float prediction = 0;
        #pragma unroll
        for (int i = 0; i < 32; ++i) {
            memory[i] *= decay;
            prediction += memory[i] * k[key_partition * 32 + i];
        }
        prediction += __shfl_xor_sync(0xffffffffu, prediction, 8);
        prediction += __shfl_xor_sync(0xffffffffu, prediction, 16);
        float v = conv[t * channels + 2 * key_heads * 128 + vh * 128 + column];
        float delta = (v - prediction) * b;
        float result = 0;
        #pragma unroll
        for (int i = 0; i < 32; ++i) {
            int index = key_partition * 32 + i;
            memory[i] += k[index] * delta;
            result += memory[i] * q[index];
        }
        result += __shfl_xor_sync(0xffffffffu, result, 8);
        result += __shfl_xor_sync(0xffffffffu, result, 16);
        if (key_partition == 0) out[t * width + vh * 128 + column] = result;
    }
    #pragma unroll
    for (int i = 0; i < 32; ++i)
        state[(vh * 128 + key_partition * 32 + i) * 128 + column] = memory[i];
}

extern "C" __global__ void lighton_recurrent(
    const float *conv, const float *beta, const float *a,
    const float *a_log, const float *dt_bias, float *state, float *out,
    int seq, int key_heads, int value_heads, int initialized) {
    recurrent_head(conv, beta, a, a_log, dt_bias, state, out, seq,
                   key_heads, value_heads, initialized, blockIdx.y);
}

// The final output buffer is private and not consumed until gated_norm.
// Its first two values per token/head hold exact FP32 gates temporarily.
// This removes 512 identical nonlinear evaluations per head and token.
extern "C" __global__ void lighton_conv_cached_gates(
    const float *x, const float *weight, const float *history,
    float *next_history, float *out, int seq, int channels, int initialized,
    const float *beta, const float *a, const float *a_log, const float *dt_bias,
    float *cached, int value_heads) {
    conv_channel(x, weight, history, next_history, out, seq, channels, initialized,
                 blockIdx.x * blockDim.x + threadIdx.x);
    const int head = blockIdx.x;
    if (threadIdx.x == 0 && head < value_heads) {
        for (int t = 0; t < seq; ++t) {
            const float p = a[t * value_heads + head] + dt_bias[head];
            const float softplus = p > 20.f ? p : log1pf(expf(p));
            const int base = (t * value_heads + head) * 128;
            cached[base] = expf(-expf(a_log[head]) * softplus);
            cached[base+1] = 1.f / (1.f + expf(-beta[t * value_heads + head]));
        }
    }
}

extern "C" __global__ void lighton_recurrent_cached_gates(
    const float *conv, const float *beta, const float *a,
    const float *a_log, const float *dt_bias, float *state, float *out,
    int seq, int key_heads, int value_heads, int initialized, const float* cached) {
    recurrent_head(conv, beta, a, a_log, dt_bias, state, out, seq,
                   key_heads, value_heads, initialized, blockIdx.y,
                   cached + blockIdx.y * 128);
}

extern "C" __global__ void lighton_gated_norm(
    const float *core, const float *z, const float *weight, float *out,
    int value_heads, float eps) {
    __shared__ float partial[4];
    int d = threadIdx.x;
    int index = blockIdx.x * 128 + d;
    float v = core[index];
    float sum = warp_sum(v * v);
    if ((d & 31) == 0) partial[d >> 5] = sum;
    __syncthreads();
    if (d == 0) partial[0] = rsqrtf((partial[0] + partial[1] + partial[2] + partial[3]) / 128.0f + eps);
    __syncthreads();
    float gate = z[index];
    out[index] = v * partial[0] * weight[d] * (gate / (1.0f + expf(-gate)));
}

// Snapshot page pointers by value. No persistent upload buffer or cross-stream
// lifetime is introduced; all page state remains independently admitted.
struct BatchPage { float *history, *next_history, *state; };
struct BatchParams {
    const float *mixed, *conv_weight;
    float *conv;
    const float *z, *beta, *a, *a_log, *dt_bias;
    float *core;
    const float *norm_weight;
    float *out;
    int key_heads, value_heads, items;
    float eps;
    BatchPage pages[64];
};
static_assert(sizeof(BatchPage) == 24 && sizeof(BatchParams) == 1640, "batch ABI");
extern "C" __global__ void lighton_batch_conv(const __grid_constant__ BatchParams p) {
    int page = blockIdx.y;
    int channels = (2 * p.key_heads + p.value_heads) * 128;
    conv_channel(p.mixed + page * channels, p.conv_weight,
        p.pages[page].history, p.pages[page].next_history,
        p.conv + page * channels, 1, channels, 1,
        blockIdx.x * blockDim.x + threadIdx.x);
}
extern "C" __global__ void lighton_batch_recurrent(const __grid_constant__ BatchParams p) {
    int page = blockIdx.y / p.value_heads;
    int head = blockIdx.y % p.value_heads;
    int channels = (2 * p.key_heads + p.value_heads) * 128;
    int width = p.value_heads * 128;
    recurrent_head(p.conv + page * channels,
        p.beta + page * p.value_heads, p.a + page * p.value_heads,
        p.a_log, p.dt_bias, p.pages[page].state, p.core + page * width,
        1, p.key_heads, p.value_heads, 1, head);
}

// Keep the exact scalar/reduction order while removing two stream boundaries.
// A 128-channel convolution block already owns one complete Q or K head.
__device__ __forceinline__ void batch_conv_normalize(const BatchParams& p, bool cached) {
    int page = blockIdx.y;
    int channels = (2 * p.key_heads + p.value_heads) * 128;
    int head = blockIdx.x;
    int d = threadIdx.x;
    conv_channel(p.mixed + page * channels, p.conv_weight,
        p.pages[page].history, p.pages[page].next_history,
        p.conv + page * channels, 1, channels, 1, head * 128 + d);
    if (cached && d == 0 && head < p.value_heads) {
        const float value = p.a[page * p.value_heads + head] + p.dt_bias[head];
        const float softplus = value > 20.f ? value : log1pf(expf(value));
        const int base = (page * p.value_heads + head) * 128;
        p.out[base] = expf(-expf(p.a_log[head]) * softplus);
        p.out[base+1] = 1.f / (1.f + expf(-p.beta[page * p.value_heads + head]));
    }
    if (head >= 2 * p.key_heads) return; // Uniform across this block.
    __shared__ float partial[4];
    __syncthreads();
    float* row = p.conv + page * channels + head * 128;
    float value = row[d];
    float sum = warp_sum(value * value);
    if ((d & 31) == 0) partial[d >> 5] = sum;
    __syncthreads();
    if (d == 0) partial[0] = 1.0f / sqrtf(partial[0] + partial[1] + partial[2] + partial[3] + 1e-6f);
    __syncthreads();
    row[d] = value * partial[0] * (head < p.key_heads ? 0.08838834764831845f : 1.0f);
}

__device__ __forceinline__ void batch_recurrent_norm(const BatchParams& p, bool cached) {
    // Four old 128-thread column blocks become one 512-thread head block.
    // recurrent_head's warp/key partition arithmetic remains identical.
    int page = blockIdx.y / p.value_heads;
    int head = blockIdx.y % p.value_heads;
    int channels = (2 * p.key_heads + p.value_heads) * 128;
    int width = p.value_heads * 128;
    recurrent_head(p.conv + page * channels,
        p.beta + page * p.value_heads, p.a + page * p.value_heads,
        p.a_log, p.dt_bias, p.pages[page].state, p.core + page * width,
        1, p.key_heads, p.value_heads, 1, head, cached ? p.out + (page * p.value_heads + head) * 128 : nullptr);
    __shared__ float partial[4];
    __syncthreads();
    int d = threadIdx.x;
    float value = 0;
    if (d < 128) {
        value = p.core[page * width + head * 128 + d];
        float sum = warp_sum(value * value);
        if ((d & 31) == 0) partial[d >> 5] = sum;
    }
    __syncthreads();
    if (d == 0) partial[0] = rsqrtf((partial[0] + partial[1] + partial[2] + partial[3]) / 128.0f + p.eps);
    __syncthreads();
    if (d < 128) {
        int index = page * width + head * 128 + d;
        float gate = p.z[index];
        p.out[index] = value * partial[0] * p.norm_weight[d] * (gate / (1.0f + expf(-gate)));
    }
}

extern "C" __global__ void lighton_batch_conv_normalize(const __grid_constant__ BatchParams p) {
    batch_conv_normalize(p, false);
}
extern "C" __global__ void lighton_batch_recurrent_norm(const __grid_constant__ BatchParams p) {
    batch_recurrent_norm(p, false);
}
extern "C" __global__ void lighton_batch_conv_normalize_cached_gates(const __grid_constant__ BatchParams p) {
    batch_conv_normalize(p, true);
}
extern "C" __global__ void lighton_batch_recurrent_norm_cached_gates(const __grid_constant__ BatchParams p) {
    batch_recurrent_norm(p, true);
}

// Exactly split a packed FP32 projection at its original public boundaries.
// Packing BF16 checkpoint rows changes neither operand precision nor layout.
extern "C" __global__ void lighton_split_projections(
    float* mixed, float* z, float* beta, float* a, const float* joined,
    unsigned count, unsigned mixed_dim, unsigned z_dim, unsigned heads) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned stride = mixed_dim + z_dim + 2 * heads;
    const unsigned row = i / stride, column = i % stride;
    const float value = joined[i];
    if (column < mixed_dim) mixed[row * mixed_dim + column] = value;
    else if (column < mixed_dim + z_dim) z[row * z_dim + column - mixed_dim] = value;
    else if (column < mixed_dim + z_dim + heads) beta[row * heads + column - mixed_dim - z_dim] = value;
    else a[row * heads + column - mixed_dim - z_dim - heads] = value;
}

// Prefix tokens do not depend on each other's convolution outputs. Give each
// token/channel head its own block, preserving the scalar F32 arithmetic and
// the exact normalization reduction used by lighton_normalize.
__device__ __forceinline__ void prefix_conv_normalize_cached_gates(
    const float *x, const float *weight, const float *history,
    float *next_history, float *out, int seq, int channels, int initialized,
    const float *beta, const float *a, const float *a_log, const float *dt_bias,
    float *cached, int value_heads, int head, int t) {
    __shared__ float partial[4];
    const int d = threadIdx.x;
    const int c = head * 128 + d;
    const int key_heads = (channels / 128 - value_heads) / 2;
    float sum = 0;
    for (int k = 0; k < 4; ++k) {
        const int source = t + k - 3;
        const float v = source >= 0 ? x[source * channels + c]
            : (initialized ? history[c * 4 + source + 4] : 0.f);
        sum += v * weight[c * 4 + k];
    }
    float v = sum / (1.f + expf(-sum));
    if (head < 2 * key_heads) {
        const float square = warp_sum(v * v);
        if ((d & 31) == 0) partial[d >> 5] = square;
        __syncthreads();
        if (d == 0) partial[0] = 1.f / sqrtf(partial[0] + partial[1] + partial[2] + partial[3] + 1e-6f);
        __syncthreads();
        v = v * partial[0] * (head < key_heads ? 0.08838834764831845f : 1.f);
    }
    out[t * channels + c] = v;
    if (t == 0) {
        for (int k = 0; k < 4; ++k) {
            const int source = (initialized || seq >= 4) ? seq + k - 4 : k;
            next_history[c * 4 + k] = source >= 0 && source < seq
                ? x[source * channels + c]
                : (initialized && source < 0 ? history[c * 4 + source + 4] : 0.f);
        }
    }
    if (d == 0 && head < value_heads) {
        const float p = a[t * value_heads + head] + dt_bias[head];
        const float softplus = p > 20.f ? p : log1pf(expf(p));
        const int base = (t * value_heads + head) * 128;
        cached[base] = expf(-expf(a_log[head]) * softplus);
        cached[base + 1] = 1.f / (1.f + expf(-beta[t * value_heads + head]));
    }
}

extern "C" __global__ void lighton_prefix_conv_normalize_cached_gates(
    const float *x, const float *weight, const float *history,
    float *next_history, float *out, int seq, int channels, int initialized,
    const float *beta, const float *a, const float *a_log, const float *dt_bias,
    float *cached, int value_heads) {
    prefix_conv_normalize_cached_gates(x, weight, history, next_history, out,
        seq, channels, initialized, beta, a, a_log, dt_bias, cached, value_heads,
        blockIdx.x, blockIdx.y);
}

struct PrefixBatchParams { BatchParams base; int seq, reserved; };
static_assert(sizeof(PrefixBatchParams) == 1648, "prefix batch ABI");
extern "C" __global__ void lighton_prefix_batch_conv_normalize(const __grid_constant__ PrefixBatchParams p) {
    const int page = blockIdx.y / p.seq;
    const int t = blockIdx.y % p.seq;
    const int channels = (2 * p.base.key_heads + p.base.value_heads) * 128;
    const int width = p.base.value_heads * 128;
    prefix_conv_normalize_cached_gates(
        p.base.mixed + page * p.seq * channels, p.base.conv_weight,
        p.base.pages[page].history, p.base.pages[page].next_history,
        p.base.conv + page * p.seq * channels, p.seq, channels, 0,
        p.base.beta + page * p.seq * p.base.value_heads,
        p.base.a + page * p.seq * p.base.value_heads,
        p.base.a_log, p.base.dt_bias, p.base.out + page * p.seq * width,
        p.base.value_heads, blockIdx.x, t);
}
extern "C" __global__ void lighton_prefix_batch_recurrent(const __grid_constant__ PrefixBatchParams p) {
    const int page = blockIdx.y / p.base.value_heads;
    const int head = blockIdx.y % p.base.value_heads;
    const int channels = (2 * p.base.key_heads + p.base.value_heads) * 128;
    const int width = p.base.value_heads * 128;
    recurrent_head(p.base.conv + page * p.seq * channels,
        p.base.beta + page * p.seq * p.base.value_heads,
        p.base.a + page * p.seq * p.base.value_heads,
        p.base.a_log, p.base.dt_bias, p.base.pages[page].state,
        p.base.core + page * p.seq * width,
        p.seq, p.base.key_heads, p.base.value_heads, 0, head,
        p.base.out + page * p.seq * width + head * 128);
}

// Fresh-prefix Q/K staging preserves the original per-element arithmetic.
// Padding each 32-key partition avoids shared-memory bank conflicts between
// the four partitions in a warp. Eight tokens amortize both block barriers.
extern "C" __global__ void lighton_prefix_batch_recurrent_shared_qk(const __grid_constant__ PrefixBatchParams p) {
    const int page = blockIdx.y / p.base.value_heads;
    const int vh = blockIdx.y % p.base.value_heads;
    const int kh = vh / (p.base.value_heads / p.base.key_heads);
    const int channels = (2 * p.base.key_heads + p.base.value_heads) * 128;
    const int width = p.base.value_heads * 128;
    const int lane = threadIdx.x & 31;
    const int partition = lane >> 3;
    const int column = blockIdx.x * 32 + (threadIdx.x >> 5) * 8 + (lane & 7);
    __shared__ float qtile[8][132];
    __shared__ float ktile[8][132];
    float memory[32];
    #pragma unroll
    for (int i = 0; i < 32; ++i) memory[i] = 0.f;
    const float* conv = p.base.conv + page * p.seq * channels;
    const float* gates = p.base.out + page * p.seq * width + vh * 128;
    float* output = p.base.core + page * p.seq * width + vh * 128;
    for (int base = 0; base < p.seq; base += 8) {
        const int d = threadIdx.x;
        const int slot = (d / 32) * 33 + d % 32;
        #pragma unroll
        for (int local = 0; local < 8; ++local) {
            const int t = base + local;
            qtile[local][slot] = t < p.seq ? conv[t * channels + kh * 128 + d] : 0.f;
            ktile[local][slot] = t < p.seq ? conv[t * channels + (kh + p.base.key_heads) * 128 + d] : 0.f;
        }
        __syncthreads();
        #pragma unroll 1
        for (int local = 0; local < 8 && base + local < p.seq; ++local) {
            const int t = base + local;
            const float decay = gates[t * width];
            const float beta = gates[t * width + 1];
            float prediction = 0;
            #pragma unroll
            for (int i = 0; i < 32; ++i) {
                memory[i] *= decay;
                prediction += memory[i] * ktile[local][partition * 33 + i];
            }
            prediction += __shfl_xor_sync(0xffffffffu, prediction, 8);
            prediction += __shfl_xor_sync(0xffffffffu, prediction, 16);
            const float value = conv[t * channels + 2 * p.base.key_heads * 128 + vh * 128 + column];
            const float delta = (value - prediction) * beta;
            float result = 0;
            #pragma unroll
            for (int i = 0; i < 32; ++i) {
                memory[i] += ktile[local][partition * 33 + i] * delta;
                result += memory[i] * qtile[local][partition * 33 + i];
            }
            result += __shfl_xor_sync(0xffffffffu, result, 8);
            result += __shfl_xor_sync(0xffffffffu, result, 16);
            if (partition == 0) output[t * width + column] = result;
        }
        __syncthreads();
    }
    #pragma unroll
    for (int i = 0; i < 32; ++i) p.base.pages[page].state[(vh * 128 + partition * 32 + i) * 128 + column] = memory[i];
}

// Compact page rows; every page keeps its own sequence, initialization and state.
struct RaggedParams {
    BatchParams base;
    int offsets[64], lengths[64], initialized[64];
};
static_assert(sizeof(RaggedParams) == 2408, "ragged ABI");
extern "C" __global__ void lighton_ragged_conv_normalize_cached_gates(const __grid_constant__ RaggedParams p) {
    const int row = blockIdx.y;
    int lo = 0, hi = p.base.items;
    while (lo + 1 < hi) {
        const int mid = lo + (hi - lo) / 2;
        if (p.offsets[mid] <= row) lo = mid; else hi = mid;
    }
    const int page = lo, offset = p.offsets[page], t = row - offset;
    const int channels = (2 * p.base.key_heads + p.base.value_heads) * 128;
    const int width = p.base.value_heads * 128;
    prefix_conv_normalize_cached_gates(
        p.base.mixed + offset * channels, p.base.conv_weight,
        p.base.pages[page].history, p.base.pages[page].next_history,
        p.base.conv + offset * channels, p.lengths[page], channels, p.initialized[page],
        p.base.beta + offset * p.base.value_heads, p.base.a + offset * p.base.value_heads,
        p.base.a_log, p.base.dt_bias, p.base.out + offset * width,
        p.base.value_heads, blockIdx.x, t);
}

extern "C" __global__ void lighton_ragged_recurrent_shared_qk(const __grid_constant__ RaggedParams p) {
    const int page = blockIdx.y / p.base.value_heads;
    const int vh = blockIdx.y % p.base.value_heads;
    const int seq = p.lengths[page], offset = p.offsets[page];
    const int kh = vh / (p.base.value_heads / p.base.key_heads);
    const int channels = (2 * p.base.key_heads + p.base.value_heads) * 128;
    const int width = p.base.value_heads * 128;
    const int lane = threadIdx.x & 31;
    const int partition = lane >> 3;
    const int column = blockIdx.x * 32 + (threadIdx.x >> 5) * 8 + (lane & 7);
    __shared__ float qtile[8][132];
    __shared__ float ktile[8][132];
    float memory[32];
    #pragma unroll
    for (int i = 0; i < 32; ++i) memory[i] = p.initialized[page] ? p.base.pages[page].state[(vh * 128 + partition * 32 + i) * 128 + column] : 0.f;
    const float* conv = p.base.conv + offset * channels;
    const float* gates = p.base.out + offset * width + vh * 128;
    float* output = p.base.core + offset * width + vh * 128;
    for (int base = 0; base < seq; base += 8) {
        const int d = threadIdx.x;
        const int slot = (d / 32) * 33 + d % 32;
        #pragma unroll
        for (int local = 0; local < 8; ++local) {
            const int t = base + local;
            qtile[local][slot] = t < seq ? conv[t * channels + kh * 128 + d] : 0.f;
            ktile[local][slot] = t < seq ? conv[t * channels + (kh + p.base.key_heads) * 128 + d] : 0.f;
        }
        __syncthreads();
        #pragma unroll 1
        for (int local = 0; local < 8 && base + local < seq; ++local) {
            const int t = base + local;
            const float decay = gates[t * width];
            const float beta = gates[t * width + 1];
            float prediction = 0;
            #pragma unroll
            for (int i = 0; i < 32; ++i) {
                memory[i] *= decay;
                prediction += memory[i] * ktile[local][partition * 33 + i];
            }
            prediction += __shfl_xor_sync(0xffffffffu, prediction, 8);
            prediction += __shfl_xor_sync(0xffffffffu, prediction, 16);
            const float value = conv[t * channels + 2 * p.base.key_heads * 128 + vh * 128 + column];
            const float delta = (value - prediction) * beta;
            float result = 0;
            #pragma unroll
            for (int i = 0; i < 32; ++i) {
                memory[i] += ktile[local][partition * 33 + i] * delta;
                result += memory[i] * qtile[local][partition * 33 + i];
            }
            result += __shfl_xor_sync(0xffffffffu, result, 8);
            result += __shfl_xor_sync(0xffffffffu, result, 16);
            if (partition == 0) output[t * width + column] = result;
        }
        __syncthreads();
    }
    #pragma unroll
    for (int i = 0; i < 32; ++i) p.base.pages[page].state[(vh * 128 + partition * 32 + i) * 128 + column] = memory[i];
}

extern "C" __global__ void lighton_ragged_prefix_conv(const __grid_constant__ RaggedParams p) {
    const int row = blockIdx.y;
    int lo = 0, hi = p.base.items;
    while (lo + 1 < hi) {
        const int mid = lo + (hi - lo) / 2;
        if (p.offsets[mid] <= row) lo = mid; else hi = mid;
    }
    const int page = lo, offset = p.offsets[page], t = row - offset;
    if(p.lengths[page]==1)return;
    const int channels = (2 * p.base.key_heads + p.base.value_heads) * 128;
    const int width = p.base.value_heads * 128;
    prefix_conv_normalize_cached_gates(
        p.base.mixed + offset * channels, p.base.conv_weight,
        p.base.pages[page].history, p.base.pages[page].next_history,
        p.base.conv + offset * channels, p.lengths[page], channels, p.initialized[page],
        p.base.beta + offset * p.base.value_heads, p.base.a + offset * p.base.value_heads,
        p.base.a_log, p.base.dt_bias, p.base.out + offset * width,
        p.base.value_heads, blockIdx.x, t);
}



extern "C" __global__ void lighton_ragged_recurrent_prefix_only(const __grid_constant__ RaggedParams p) {
    const int page = blockIdx.y / p.base.value_heads;
    const int vh = blockIdx.y % p.base.value_heads;
    const int seq = p.lengths[page], offset = p.offsets[page];
    if (seq == 1) return;
    const int kh = vh / (p.base.value_heads / p.base.key_heads);
    const int channels = (2 * p.base.key_heads + p.base.value_heads) * 128;
    const int width = p.base.value_heads * 128;
    const int lane = threadIdx.x & 31;
    const int partition = lane >> 3;
    const int column = blockIdx.x * 32 + (threadIdx.x >> 5) * 8 + (lane & 7);
    __shared__ float qtile[8][132];
    __shared__ float ktile[8][132];
    float memory[32];
    #pragma unroll
    for (int i = 0; i < 32; ++i) memory[i] = p.initialized[page] ? p.base.pages[page].state[(vh * 128 + partition * 32 + i) * 128 + column] : 0.f;
    const float* conv = p.base.conv + offset * channels;
    const float* gates = p.base.out + offset * width + vh * 128;
    float* output = p.base.core + offset * width + vh * 128;
    for (int base = 0; base < seq; base += 8) {
        const int d = threadIdx.x;
        const int slot = (d / 32) * 33 + d % 32;
        #pragma unroll
        for (int local = 0; local < 8; ++local) {
            const int t = base + local;
            qtile[local][slot] = t < seq ? conv[t * channels + kh * 128 + d] : 0.f;
            ktile[local][slot] = t < seq ? conv[t * channels + (kh + p.base.key_heads) * 128 + d] : 0.f;
        }
        __syncthreads();
        #pragma unroll 1
        for (int local = 0; local < 8 && base + local < seq; ++local) {
            const int t = base + local;
            const float decay = gates[t * width];
            const float beta = gates[t * width + 1];
            float prediction = 0;
            #pragma unroll
            for (int i = 0; i < 32; ++i) {
                memory[i] *= decay;
                prediction += memory[i] * ktile[local][partition * 33 + i];
            }
            prediction += __shfl_xor_sync(0xffffffffu, prediction, 8);
            prediction += __shfl_xor_sync(0xffffffffu, prediction, 16);
            const float value = conv[t * channels + 2 * p.base.key_heads * 128 + vh * 128 + column];
            const float delta = (value - prediction) * beta;
            float result = 0;
            #pragma unroll
            for (int i = 0; i < 32; ++i) {
                memory[i] += ktile[local][partition * 33 + i] * delta;
                result += memory[i] * qtile[local][partition * 33 + i];
            }
            result += __shfl_xor_sync(0xffffffffu, result, 8);
            result += __shfl_xor_sync(0xffffffffu, result, 16);
            if (partition == 0) output[t * width + column] = result;
        }
        __syncthreads();
    }
    #pragma unroll
    for (int i = 0; i < 32; ++i) p.base.pages[page].state[(vh * 128 + partition * 32 + i) * 128 + column] = memory[i];
}


// F32 only. One CTA owns a complete head: no inter-block reductions or state aliases.
__device__ __forceinline__ float decode_conv_one(const RaggedParams& p,int page,int row,int channel,int owns_history){
    int channels=(2*p.base.key_heads+p.base.value_heads)*128;
    const float* hist=p.base.pages[page].history+channel*4;
    const float* weight=p.base.conv_weight+channel*4;
    float sample=p.base.mixed[row*channels+channel],sum=0.f;
    #pragma unroll
    for(int k=0;k<4;++k){float v=k==3?sample:(p.initialized[page]?hist[k+1]:0.f);sum+=v*weight[k];}
    if(owns_history){
        float* next=p.base.pages[page].next_history+channel*4;
        #pragma unroll
        for(int k=0;k<4;++k)next[k]=p.initialized[page]?(k==3?sample:hist[k+1]):(k==0?sample:0.f);
    }
    return sum/(1.f+expf(-sum));
}
template<int P> __device__ __forceinline__ void fused_decode_head(const RaggedParams& p){
    int page=blockIdx.y/p.base.value_heads,vh=blockIdx.y%p.base.value_heads;
    if(p.lengths[page]!=1)return;
    const int tid=threadIdx.x,row=p.offsets[page],kh=vh/(p.base.value_heads/p.base.key_heads);
    const int width=p.base.value_heads*128;
    __shared__ float q[132],k[132],values[128],results[128],sq[4],sk[4],gates[2];
    if(tid<128){
        int owns_qk=vh%(p.base.value_heads/p.base.key_heads)==0;
        float qv=decode_conv_one(p,page,row,kh*128+tid,owns_qk);
        float kv=decode_conv_one(p,page,row,(kh+p.base.key_heads)*128+tid,owns_qk);
        values[tid]=decode_conv_one(p,page,row,2*p.base.key_heads*128+vh*128+tid,1);
        float qsum=warp_sum(qv*qv),ksum=warp_sum(kv*kv);
        if((tid&31)==0){sq[tid/32]=qsum;sk[tid/32]=ksum;}
        int slot=(tid/32)*33+tid%32;q[slot]=qv;k[slot]=kv;
    }
    __syncthreads();
    if(tid==0){
        sq[0]=1.f/sqrtf(sq[0]+sq[1]+sq[2]+sq[3]+1e-6f);
        sk[0]=1.f/sqrtf(sk[0]+sk[1]+sk[2]+sk[3]+1e-6f);
        float a=p.base.a[row*p.base.value_heads+vh]+p.base.dt_bias[vh];
        float sp=a>20.f?a:log1pf(expf(a));
        gates[0]=expf(-expf(p.base.a_log[vh])*sp);
        gates[1]=1.f/(1.f+expf(-p.base.beta[row*p.base.value_heads+vh]));
    }
    __syncthreads();
    if(tid<128){int slot=(tid/32)*33+tid%32;q[slot]=q[slot]*sq[0]*0.08838834764831845f;k[slot]=k[slot]*sk[0]*1.f;}
    __syncthreads();
    const int lane=tid&31,part=lane/(32/P),col=(tid/32)*(32/P)+lane%(32/P);
    float memory[128/P];
    float* state=p.base.pages[page].state+vh*128*128;
    #pragma unroll
    for(int i=0;i<128/P;++i)memory[i]=p.initialized[page]?state[(part*(128/P)+i)*128+col]:0.f;
    float pred[4/P];
    #pragma unroll
    for(int group=0;group<4/P;++group){
        pred[group]=0.f;
        #pragma unroll
        for(int i=0;i<32;++i){int x=group*32+i,idx=part*(128/P)+x;memory[x]*=gates[0];pred[group]+=memory[x]*k[(idx/32)*33+idx%32];}
    }
    float prediction;
    if constexpr(P==4){prediction=pred[0];prediction+=__shfl_xor_sync(0xffffffffu,prediction,8);prediction+=__shfl_xor_sync(0xffffffffu,prediction,16);}
    if constexpr(P==2){prediction=pred[0]+pred[1];prediction+=__shfl_xor_sync(0xffffffffu,prediction,16);}
    if constexpr(P==1)prediction=(pred[0]+pred[1])+(pred[2]+pred[3]);
    float change=(values[col]-prediction)*gates[1];
    float dot[4/P];
    #pragma unroll
    for(int group=0;group<4/P;++group){
        dot[group]=0.f;
        #pragma unroll
        for(int i=0;i<32;++i){int x=group*32+i,idx=part*(128/P)+x,slot=(idx/32)*33+idx%32;memory[x]+=k[slot]*change;dot[group]+=memory[x]*q[slot];}
    }
    float result;
    if constexpr(P==4){result=dot[0];result+=__shfl_xor_sync(0xffffffffu,result,8);result+=__shfl_xor_sync(0xffffffffu,result,16);}
    if constexpr(P==2){result=dot[0]+dot[1];result+=__shfl_xor_sync(0xffffffffu,result,16);}
    if constexpr(P==1)result=(dot[0]+dot[1])+(dot[2]+dot[3]);
    #pragma unroll
    for(int i=0;i<128/P;++i)state[(part*(128/P)+i)*128+col]=memory[i];
    if(part==0)results[col]=result;
    __syncthreads();
    if(tid<128){float value=results[tid],sum=warp_sum(value*value);if((tid&31)==0)sq[tid/32]=sum;}
    __syncthreads();
    if(tid==0)sq[0]=rsqrtf((sq[0]+sq[1]+sq[2]+sq[3])/128.f+p.base.eps);
    __syncthreads();
    if(tid<128){int i=row*width+vh*128+tid;float z=p.base.z[i];p.base.out[i]=results[tid]*sq[0]*p.base.norm_weight[tid]*(z/(1.f+expf(-z)));}
}
extern "C" __global__ void lighton_decode_fused_p2(const __grid_constant__ RaggedParams p){fused_decode_head<2>(p);}
extern "C" __global__ void lighton_ragged_prefix_gated_norm(const __grid_constant__ RaggedParams p){
    int row=blockIdx.x/p.base.value_heads,vh=blockIdx.x%p.base.value_heads;
    int lo=0,hi=p.base.items;while(lo+1<hi){int mid=lo+(hi-lo)/2;if(p.offsets[mid]<=row)lo=mid;else hi=mid;}
    if(p.lengths[lo]==1)return;
    __shared__ float partial[4];int d=threadIdx.x,index=(row*p.base.value_heads+vh)*128+d;
    float value=p.base.core[index],sum=warp_sum(value*value);
    if((d&31)==0)partial[d>>5]=sum;__syncthreads();
    if(d==0)partial[0]=rsqrtf((partial[0]+partial[1]+partial[2]+partial[3])/128.f+p.base.eps);
    __syncthreads();float z=p.base.z[index];p.base.out[index]=value*partial[0]*p.base.norm_weight[d]*(z/(1.f+expf(-z)));
}
