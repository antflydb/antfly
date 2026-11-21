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

//! Shared F32 gated-delta state update. Native adapters retain their existing
//! control preparation, SIMD width and scalar key-reduction order.
const std = @import("std");
const Params = @import("ml").graph.node.GatedDeltaAttrs;

// Work on eight adjacent value columns together so both state passes use
// contiguous loads and SIMD. Each column retains the scalar key reduction
// order; the tail handles arbitrary value-head dimensions.
pub fn head(state: []f32, key: []const f32, query: []const f32, value: []const f32, output: []f32, decay: f32, beta: f32) void {
    const V = @Vector(8, f32);
    var col: usize = 0;
    while (col + 8 <= value.len) : (col += 8) {
        var memory: V = @splat(0);
        for (key, 0..) |k, row| {
            const offset = row * value.len + col;
            const old: V = state[offset..][0..8].*;
            const decayed = old * @as(V, @splat(decay));
            state[offset..][0..8].* = decayed;
            memory += decayed * @as(V, @splat(k));
        }
        const v: V = value[col..][0..8].*;
        const delta = (v - memory) * @as(V, @splat(beta));
        var out: V = @splat(0);
        for (key, query, 0..) |k, q, row| {
            const offset = row * value.len + col;
            const decayed: V = state[offset..][0..8].*;
            const updated = decayed + @as(V, @splat(k)) * delta;
            state[offset..][0..8].* = updated;
            out += updated * @as(V, @splat(q));
        }
        output[col..][0..8].* = out;
    }
    while (col < value.len) : (col += 1) {
        var memory: f32 = 0;
        for (key, 0..) |k, row| {
            const offset = row * value.len + col;
            state[offset] *= decay;
            memory += state[offset] * k;
        }
        const delta = (value[col] - memory) * beta;
        var out: f32 = 0;
        for (key, query, 0..) |k, q, row| {
            const offset = row * value.len + col;
            state[offset] += k * delta;
            out += state[offset] * q;
        }
        output[col] = out;
    }
}

/// Packed token-major Q/K/V with precomputed sigmoid beta and decay.
/// The returned prefix is independent of previous calls or graph executions.
pub fn fullPrefix(a: std.mem.Allocator, mixed: []const f32, beta: []const f32, decay: []const f32, p: Params) ![]f32 {
    if (p.rows == 0 or p.key_heads == 0 or p.value_heads == 0 or p.value_heads % p.key_heads != 0 or p.key_dim == 0 or p.key_dim > 512 or p.value_dim == 0) return error.InvalidInputShape;
    const key_width = try std.math.mul(usize, p.key_heads, p.key_dim);
    const value_width = try std.math.mul(usize, p.value_heads, p.value_dim);
    const channels = try std.math.add(usize, try std.math.mul(usize, key_width, 2), value_width);
    if (mixed.len != try std.math.mul(usize, p.rows, channels) or beta.len != try std.math.mul(usize, p.rows, p.value_heads) or decay.len != beta.len) return error.InvalidInputShape;
    const state_width = try std.math.mul(usize, p.key_dim, p.value_dim);
    const matrix = try a.alloc(f32, try std.math.mul(usize, p.value_heads, state_width));
    defer a.free(matrix);
    @memset(matrix, 0);
    const output = try a.alloc(f32, try std.math.mul(usize, p.rows, value_width));
    var q_storage: [512]f32 = undefined;
    var k_storage: [512]f32 = undefined;
    const q = q_storage[0..p.key_dim];
    const k = k_storage[0..p.key_dim];
    for (0..p.value_heads) |h| {
        const kh = h / (p.value_heads / p.key_heads);
        for (0..p.rows) |t| {
            const row = mixed[t * channels ..][0..channels];
            const qs = row[kh * p.key_dim ..][0..p.key_dim];
            const ks = row[key_width + kh * p.key_dim ..][0..p.key_dim];
            var q_sum: f32 = 0;
            var k_sum: f32 = 0;
            for (qs, ks) |x, y| {
                q_sum += x * x;
                k_sum += y * y;
            }
            const qi = 1 / @sqrt(q_sum + 1e-6);
            const ki = 1 / @sqrt(k_sum + 1e-6);
            for (qs, ks, q, k) |x, y, *qx, *kx| {
                qx.* = (x * qi) * (1 / @sqrt(@as(f32, @floatFromInt(p.key_dim))));
                kx.* = y * ki;
            }
            head(matrix[h * state_width ..][0..state_width], k, q, row[2 * key_width + h * p.value_dim ..][0..p.value_dim], output[t * value_width + h * p.value_dim ..][0..p.value_dim], decay[t * p.value_heads + h], beta[t * p.value_heads + h]);
        }
    }
    return output;
}
