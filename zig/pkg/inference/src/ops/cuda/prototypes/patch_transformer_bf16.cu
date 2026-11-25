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

#include <stdint.h>
#include "../kernels/row_program.cuh"

namespace {
constexpr unsigned width = 1024, dim = 64, patch = 20;
__device__ float f(__nv_bfloat16 x) { return __bfloat162float(x); }
__device__ __nv_bfloat16 b(float x) { return __float2bfloat16_rn(x); }
__device__ float interpolate(float a, float c, float t) {
    return t < 0.5f ? __fmaf_rn(a, 1.0f - t, c * t) : __fmaf_rn(c, t, a * (1.0f - t));
}
}

extern "C" __global__ void antfly_rgb_patches20_bf16(
    __nv_bfloat16* out, const uint8_t* rgb, unsigned image_x, unsigned image_y, unsigned grid_x
) {
    constexpr unsigned features = patch * patch * 3;
    constexpr float mean[3] = {0.48145466f, 0.4578275f, 0.40821073f};
    constexpr float processor_std[3] = {0.26862954f, 0.2613026f, 0.2757771f};
    constexpr float std[3] = {0.26862954f, 0.26130258f, 0.27577711f};
    const unsigned x = (blockIdx.x % grid_x) * patch;
    const unsigned y = (blockIdx.x / grid_x) * patch;
    for (unsigned i = threadIdx.x; i < features; i += blockDim.x) {
        const unsigned px = x + i / 3 % patch, py = y + i / (3 * patch), channel = i % 3;
        const float value = px < image_x && py < image_y ? rgb[((size_t)py * image_x + px) * 3 + channel] : -1.0f;
        const float normalized = f(b((value / 255.0f - mean[channel]) / processor_std[channel]));
        out[(size_t)blockIdx.x * features + i] = b(2.0f * (normalized * std[channel] + mean[channel]) - 1.0f);
    }
}

extern "C" __global__ void antfly_patch_position_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* tokens, const __nv_bfloat16* positions,
    const __nv_bfloat16* cls, const __nv_bfloat16* cls_pos, unsigned gx, unsigned gy
) {
    const unsigned row = blockIdx.x;
    const float x = row == 0 ? 0 : fmaxf(0, fminf(31, 32.0f * ((row - 1) % gx + 0.5f) / gx - 0.5f));
    const float y = row == 0 ? 0 : fmaxf(0, fminf(31, 32.0f * ((row - 1) / gx + 0.5f) / gy - 0.5f));
    const unsigned x0 = (unsigned)x, x1 = min(x0 + 1, 31u), y0 = (unsigned)y, y1 = min(y0 + 1, 31u);
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) {
        float value;
        if (row == 0) value = f(cls[i]) + f(cls_pos[i]);
        else {
            const float top = interpolate(f(positions[(y0 * 32 + x0) * width + i]), f(positions[(y0 * 32 + x1) * width + i]), x - x0);
            const float bottom = interpolate(f(positions[(y1 * 32 + x0) * width + i]), f(positions[(y1 * 32 + x1) * width + i]), x - x0);
            value = f(tokens[(size_t)(row - 1) * width + i]) + f(b(interpolate(top, bottom, y - y0)));
        }
        out[(size_t)row * width + i] = b(value);
    }
}

extern "C" __global__ void antfly_apply_patch_rotary_bf16(__nv_bfloat16* q, __nv_bfloat16* k, unsigned grid_x) {
    const unsigned row = blockIdx.x;
    if (row == 0) return;
    const unsigned x = (row - 1) % grid_x + 1, y = (row - 1) / grid_x + 1;
    for (unsigned i = threadIdx.x; i < width / 2; i += blockDim.x) {
        const unsigned pair = i % (dim / 2);
        const float angle = (pair < dim / 4 ? x : y) * powf(10000.0f, -(float)(pair % (dim / 4)) / (dim / 4));
        float sin, cos;
        sincosf(angle, &sin, &cos);
        const size_t j = (size_t)row * width + 2 * i;
        const float qe = f(q[j]), qo = f(q[j+1]), ke = f(k[j]), ko = f(k[j+1]);
        q[j] = b(__fmaf_rn(qe, cos, -qo * sin)); q[j+1] = b(__fmaf_rn(qo, cos, qe * sin));
        k[j] = b(__fmaf_rn(ke, cos, -ko * sin)); k[j+1] = b(__fmaf_rn(ko, cos, ke * sin));
    }
}

extern "C" __global__ void antfly_bidirectional_attention_d64_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* q, const __nv_bfloat16* k, const __nv_bfloat16* v, unsigned rows
) {
    const unsigned lane = threadIdx.x % 32, head = blockIdx.y * 4 + threadIdx.x / 32;
    const size_t offset = head * dim + lane;
    const size_t q_index = (size_t)blockIdx.x * width + offset;
    const float q0 = f(q[q_index]), q1 = f(q[q_index + 32]);
    float max_score = -INFINITY, sum = 0, value0 = 0, value1 = 0;
    for (unsigned j = 0; j < rows; ++j) {
        const size_t k_index = (size_t)j * width + offset;
        const float dot = __fmaf_rn(q0, f(k[k_index]), q1 * f(k[k_index + 32]));
        const float score = __shfl_sync(0xffffffffu, antfly_row_warp_sum(dot), 0) * 0.125f;
        const float next_max = fmaxf(score, max_score);
        const float before = __expf(max_score - next_max), after = __expf(score - next_max);
        value0 = __fmaf_rn(after, f(v[k_index]), value0 * before);
        value1 = __fmaf_rn(after, f(v[k_index + 32]), value1 * before);
        sum = __fmaf_rn(sum, before, after);
        max_score = next_max;
    }
    out[q_index] = b(value0 / sum);
    out[q_index + 32] = b(value1 / sum);
}

extern "C" __global__ void antfly_layer_scale_residual_bf16(
    __nv_bfloat16* hidden, const __nv_bfloat16* update, const __nv_bfloat16* gamma, unsigned count
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) hidden[i] = b(f(hidden[i]) + f(b(f(update[i]) * f(gamma[i % width]))));
}

extern "C" __global__ void antfly_exact_gelu_bf16(__nv_bfloat16* input, unsigned count) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) {
        const float x = f(input[i]);
        input[i] = b(x * 0.5f * (1.0f + erff(x * 0.70710678118f)));
    }
}

extern "C" __global__ void antfly_merge_2x2_patch_rows_bf16(__nv_bfloat16* out, const __nv_bfloat16* hidden, unsigned grid_x) {
    const unsigned merged = blockIdx.x, half_width = grid_x / 2;
    const unsigned top_left = 1 + 2 * (merged / half_width) * grid_x + 2 * (merged % half_width);
    for (unsigned i = threadIdx.x; i < 4 * width; i += blockDim.x) {
        const unsigned part = i / width;
        out[(size_t)merged * 4 * width + i] = hidden[(size_t)(top_left + (part / 2) * grid_x + part % 2) * width + i % width];
    }
}

extern "C" __global__ void antfly_patch_layer_norm_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* input,
    const __nv_bfloat16* gamma, const __nv_bfloat16* beta, float eps
) {
    const size_t offset = (size_t)blockIdx.x * width;
    __shared__ float partials[4], mean, inv;
    float sum = 0;
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) sum += f(input[offset + i]);
    float values[] = {sum};
    float* const planes[] = {partials};
    antfly_warp_block_statistics<false, 1, 4>(values, planes, [&](const float* total) { mean = total[0] / width; });
    __syncthreads();
    float square = 0;
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) {
        const float centered = f(input[offset + i]) - mean;
        square = __fmaf_rn(centered, centered, square);
    }
    values[0] = square;
    antfly_warp_block_statistics<false, 1, 4>(values, planes, [&](const float* total) { inv = rsqrtf(total[0] / width + eps); });
    __syncthreads();
    for (unsigned i = threadIdx.x; i < width; i += blockDim.x) {
        const float centered = f(input[offset + i]) - mean;
        out[offset + i] = b(__fmaf_rn(f(gamma[i]), centered * inv, f(beta[i])));
    }
}
