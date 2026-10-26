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

#include <metal_stdlib>
using namespace metal;

struct Params {
    uint op, n, dim, groups, position, stride, mode, index;
    float epsilon, scale, theta, reserved;
};

inline float fit_reduce(threadgroup float *scratch, uint lane, float value, bool maximum) {
    scratch[lane] = value;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint width = 64; width != 0; width >>= 1) {
        if (lane < width) scratch[lane] = maximum ? max(scratch[lane], scratch[lane + width]) : scratch[lane] + scratch[lane + width];
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    const float result = scratch[0];
    threadgroup_barrier(mem_flags::mem_threadgroup);
    return result;
}

inline void fit_add(thread float &sum, thread float &correction, float value) {
    const float adjusted = value - correction;
    const float next = sum + adjusted;
    correction = (next - sum) - adjusted;
    sum = next;
}

// One group owns one query. The host solver consumes these derived P/Y/log-Z
// matrices; residual selection, NNLS and QR retain their shared FP64 code.
kernel void metal_attention_match_fit(
    device const float *keys [[buffer(0)]], device const float *values [[buffer(1)]],
    device const float *queries [[buffer(2)]], device const float *bias [[buffer(3)]],
    device uchar *unused4 [[buffer(4)]], device uchar *unused5 [[buffer(5)]],
    device uchar *unused6 [[buffer(6)]], device float *out [[buffer(7)]],
    constant Params &p [[buffer(8)]], uint lane [[thread_index_in_threadgroup]],
    uint local_group [[threadgroup_position_in_grid]]) {
    const uint group = local_group + p.index;
    if (group >= p.groups) return;
    threadgroup float reduction[128];
    device const float *query = queries + group * p.dim;
    device float *probabilities = out + group * p.n;
    float maximum = -INFINITY;
    for (uint row = lane; row < p.n; row += 128) {
        float dot = 0, correction = 0;
        for (uint col = 0; col < p.dim; ++col) fit_add(dot, correction, keys[row * p.dim + col] * query[col]);
        dot *= p.scale;
        if (!isfinite(dot)) dot = NAN;
        else if (p.reserved > 0) dot = p.reserved * tanh(dot / p.reserved);
        // Previously fitted rows retain their original mass across cycles.
        const float logit = dot + bias[row];
        probabilities[row] = logit;
        maximum = max(maximum, logit);
    }
    maximum = fit_reduce(reduction, lane, maximum, true);
    float sum = 0, correction = 0;
    for (uint row = lane; row < p.n; row += 128) {
        probabilities[row] = exp(probabilities[row] - maximum);
        fit_add(sum, correction, probabilities[row]);
    }
    sum = fit_reduce(reduction, lane, sum, false);
    for (uint row = lane; row < p.n; row += 128) probabilities[row] /= sum;
    threadgroup_barrier(mem_flags::mem_device);
    device float *targets = out + p.groups * p.n + group * p.dim;
    for (uint col = lane; col < p.dim; col += 128) {
        float value = 0, error = 0;
        for (uint row = 0; row < p.n; ++row) fit_add(value, error, probabilities[row] * values[row * p.dim + col]);
        targets[col] = value;
    }
    if (lane == 0) out[p.groups * (p.n + p.dim) + group] = maximum + log(sum);
}
