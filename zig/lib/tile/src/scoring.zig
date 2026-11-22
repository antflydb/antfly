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

//! Distance/scoring schedules; reduction order is part of each contract.

const std = @import("std");

pub fn dot(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    const n = a.len;
    const simd_width = 8;
    const SimdF32 = @Vector(simd_width, f32);

    var sum0: SimdF32 = @splat(0.0);
    var sum1: SimdF32 = @splat(0.0);
    var sum2: SimdF32 = @splat(0.0);
    var sum3: SimdF32 = @splat(0.0);
    var i: usize = 0;

    while (i + simd_width * 4 <= n) : (i += simd_width * 4) {
        const av: SimdF32 = a[i..][0..simd_width].*;
        const bv: SimdF32 = b[i..][0..simd_width].*;
        const av1: SimdF32 = a[i + simd_width ..][0..simd_width].*;
        const bv1: SimdF32 = b[i + simd_width ..][0..simd_width].*;
        const av2: SimdF32 = a[i + simd_width * 2 ..][0..simd_width].*;
        const bv2: SimdF32 = b[i + simd_width * 2 ..][0..simd_width].*;
        const av3: SimdF32 = a[i + simd_width * 3 ..][0..simd_width].*;
        const bv3: SimdF32 = b[i + simd_width * 3 ..][0..simd_width].*;
        sum0 += av * bv;
        sum1 += av1 * bv1;
        sum2 += av2 * bv2;
        sum3 += av3 * bv3;
    }

    var sum = sum0 + sum1 + sum2 + sum3;
    while (i + simd_width <= n) : (i += simd_width) {
        const av: SimdF32 = a[i..][0..simd_width].*;
        const bv: SimdF32 = b[i..][0..simd_width].*;
        sum += av * bv;
    }

    var result: f32 = @reduce(.Add, sum);

    // Scalar tail.
    while (i < n) : (i += 1) {
        result += a[i] * b[i];
    }

    return result;
}

pub fn batchDot(query: []const f32, candidates: []const []const f32, dots: []f32) void {
    std.debug.assert(candidates.len <= dots.len);
    for (candidates, 0..) |candidate, i| {
        std.debug.assert(query.len == candidate.len);
        dots[i] = dot(query, candidate);
    }
}

/// Scalar dimension-order GPU distance. L2 uses direct squared differences,
/// avoiding cancellation from norms minus twice the dot product.
pub const metal_distance =
    \\inline float antfly_tile_distance(device const float* point_row, device const float* centroid_row, uint dims, int metric) {
    \\        float dot = 0.0f;
    \\        float point_norm_sq = 0.0f;
    \\        float centroid_norm_sq = 0.0f;
    \\        float l2 = 0.0f;
    \\        for (uint dim = 0; dim < dims; dim++) {
    \\            const float point = point_row[dim];
    \\            const float centroid = centroid_row[dim];
    \\            const float delta = point - centroid;
    \\            dot += point * centroid;
    \\            point_norm_sq += point * point;
    \\            centroid_norm_sq += centroid * centroid;
    \\            l2 += delta * delta;
    \\        }
    \\        float distance = l2;
    \\        if (metric == 1) {
    \\            distance = -dot;
    \\        } else if (metric == 2) {
    \\            if (point_norm_sq == 0.0f || centroid_norm_sq == 0.0f) {
    \\                distance = 1.0f;
    \\            } else {
    \\                distance = 1.0f - dot * rsqrt(point_norm_sq * centroid_norm_sq);
    \\            }
    \\        }
    \\    return distance;
    \\}
;
