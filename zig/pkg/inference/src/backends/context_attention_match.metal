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

// FP32 device frontend for the shared native FP64 OMP/NNLS/QR solver.
// Source KV is immutable. Every dispatch is synchronous at the Zig boundary.
#include <metal_stdlib>
using namespace metal;

struct Params {
    uint op, n, dim, groups, position, stride, mode, index;
    float epsilon, scale, theta, reserved;
};

inline float am_reduce(threadgroup float *s, uint lane, float value, bool maximum) {
    s[lane] = value;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint width = 64; width; width >>= 1) {
        if (lane < width) s[lane] = maximum ? max(s[lane], s[lane + width]) : s[lane] + s[lane + width];
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    const float result = s[0];
    // A following reduction may reuse this storage immediately.
    threadgroup_barrier(mem_flags::mem_threadgroup);
    return result;
}

inline void am_add(thread float &sum, thread float &correction, float value) {
    const float adjusted = value - correction;
    const float next = sum + adjusted;
    correction = (next - sum) - adjusted;
    sum = next;
}

inline float am_logit(device const half *keys, device const float *query,
                      uint row, uint dim, uint heads, uint head, float scale, float softcap) {
    float dot = 0;
    for (uint col = 0; col < dim; ++col) dot += float(keys[(row * heads + head) * dim + col]) * query[col];
    dot *= scale;
    if (!isfinite(dot)) return NAN;
    return softcap > 0 ? softcap * tanh(dot / softcap) : dot;
}

struct QueryMetrics { float mass_relative, error_squared, target_squared; uint invalid; };

kernel void context_attention_match(
    device uchar *a [[buffer(0)]], device uchar *b [[buffer(1)]],
    device uchar *c [[buffer(2)]], device uchar *d [[buffer(3)]],
    device uchar *e [[buffer(4)]], device uchar *f [[buffer(5)]],
    device uchar *g [[buffer(6)]], device uchar *out [[buffer(7)]],
    constant Params &p [[buffer(8)]], uint tid [[thread_position_in_grid]],
    uint lane [[thread_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
    threadgroup float reduction[128];
    if (p.op == 1) {
        // Gather original f16 keys; fitted values and mass were uploaded into
        // private buffers. No source key ever travels through host memory.
        if (tid >= p.stride * p.dim) return;
        device const half *keys = reinterpret_cast<device const half *>(a);
        device const uint *indices = reinterpret_cast<device const uint *>(b);
        reinterpret_cast<device half *>(out)[tid] = keys[(indices[tid / p.dim] * p.groups + p.position) * p.dim + tid % p.dim];
        return;
    }
    if (p.op == 2) {
        if (tid >= p.n * p.dim) return;
        reinterpret_cast<device half *>(out)[tid] = reinterpret_cast<device const half *>(a)[tid];
        reinterpret_cast<device half *>(d)[tid] = reinterpret_cast<device const half *>(b)[tid];
        if (tid % p.dim == 0) reinterpret_cast<device float *>(e)[tid / p.dim] = reinterpret_cast<device const float *>(c)[tid / p.dim];
        return;
    }
    if (p.op == 4) {
        if (tid < p.n) reinterpret_cast<device float *>(out)[tid] = reinterpret_cast<device const float *>(a)[tid];
        return;
    }
    if (p.op == 0) {
        // One 128-thread group owns a training query. n=source rows,
        // groups=KV stride, position=KV head, stride=query rows.
        if (group >= p.stride) return;
        device const half *keys = reinterpret_cast<device const half *>(a);
        device const half *values = reinterpret_cast<device const half *>(b);
        device const float *query = reinterpret_cast<device const float *>(c) + group * p.dim;
        device float *probs = reinterpret_cast<device float *>(out) + group * p.n;
        float maximum = -INFINITY;
        for (uint row = lane; row < p.n; row += 128) {
            const float x = am_logit(keys, query, row, p.dim, p.groups, p.position, p.scale, p.reserved);
            probs[row] = x;
            maximum = max(maximum, x);
        }
        maximum = am_reduce(reduction, lane, maximum, true);
        float sum = 0;
        for (uint row = lane; row < p.n; row += 128) { probs[row] = exp(probs[row] - maximum); sum += probs[row]; }
        sum = am_reduce(reduction, lane, sum, false);
        for (uint row = lane; row < p.n; row += 128) probs[row] /= sum;
        threadgroup_barrier(mem_flags::mem_device);
        device float *targets = reinterpret_cast<device float *>(out) + p.stride * p.n + group * p.dim;
        for (uint col = lane; col < p.dim; col += 128) {
            float y = 0, correction = 0;
            for (uint row = 0; row < p.n; ++row) am_add(y, correction, probs[row] * float(values[(row * p.groups + p.position) * p.dim + col]));
            targets[col] = y;
        }
        return;
    }
    if (p.op != 3 || group >= p.position) return;
    // Whole-head acceptance on the exact serving representation. Bias is
    // dense here: exact protected rows have zero bias. n/source and
    // groups/candidate contain only this head, including prefix and tail.
    device const half *keys = reinterpret_cast<device const half *>(a);
    device const half *values = reinterpret_cast<device const half *>(b);
    device const float *query = reinterpret_cast<device const float *>(c) + group * p.dim;
    device const half *fitted_keys = reinterpret_cast<device const half *>(d);
    device const half *fitted_values = reinterpret_cast<device const half *>(e);
    device const float *bias = reinterpret_cast<device const float *>(f);
    device float *probs = reinterpret_cast<device float *>(g) + group * (p.n + p.groups);
    device QueryMetrics *metrics = reinterpret_cast<device QueryMetrics *>(out);
    float maximum = -INFINITY, fitted_maximum = -INFINITY, invalid = 0;
    for (uint row = lane; row < p.n + p.groups; row += 128) {
        const bool fitted = row >= p.n;
        const uint r = fitted ? row - p.n : row;
        float x = am_logit(fitted ? fitted_keys : keys, query, r, p.dim, 1, 0, p.scale, p.reserved);
        // The mass correction must follow logit softcap.
        if (fitted) x += bias[r];
        if (!isfinite(x)) invalid = 1;
        probs[row] = x;
        if (fitted) fitted_maximum = max(fitted_maximum, x); else maximum = max(maximum, x);
    }
    invalid = am_reduce(reduction, lane, invalid, true);
    if (invalid != 0) { if (lane == 0) metrics[group] = { 0, 0, 0, 1 }; return; }
    maximum = am_reduce(reduction, lane, maximum, true);
    fitted_maximum = am_reduce(reduction, lane, fitted_maximum, true);
    float sum = 0, fitted_sum = 0;
    for (uint row = lane; row < p.n + p.groups; row += 128) {
        const bool fitted = row >= p.n;
        probs[row] = exp(probs[row] - (fitted ? fitted_maximum : maximum));
        if (fitted) fitted_sum += probs[row]; else sum += probs[row];
    }
    sum = am_reduce(reduction, lane, sum, false);
    fitted_sum = am_reduce(reduction, lane, fitted_sum, false);
    for (uint row = lane; row < p.n + p.groups; row += 128) probs[row] /= row < p.n ? sum : fitted_sum;
    threadgroup_barrier(mem_flags::mem_device);
    float error_squared = 0, target_squared = 0;
    for (uint col = lane; col < p.dim; col += 128) {
        float y = 0, yh = 0, correction = 0, fitted_correction = 0;
        for (uint row = 0; row < p.n; ++row) am_add(y, correction, probs[row] * float(values[row * p.dim + col]));
        for (uint row = 0; row < p.groups; ++row) am_add(yh, fitted_correction, probs[p.n + row] * float(fitted_values[row * p.dim + col]));
        error_squared += (y - yh) * (y - yh);
        target_squared += y * y;
    }
    error_squared = am_reduce(reduction, lane, error_squared, false);
    target_squared = am_reduce(reduction, lane, target_squared, false);
    if (lane == 0) {
        const float mass = abs(exp((fitted_maximum + log(fitted_sum)) - (maximum + log(sum))) - 1);
        metrics[group] = { mass, error_squared, target_squared,
            uint(!isfinite(mass) || !isfinite(error_squared) || !isfinite(target_squared)) };
    }
}
