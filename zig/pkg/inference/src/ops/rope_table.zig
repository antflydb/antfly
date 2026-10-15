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

//! Reference for `fused_rope_table` (`ml.graph.RopeTableAttrs`): split-half
//! rotary embedding by explicit per-(row, head) cos/sin tables. The Metal
//! kernel (`gliner_boundary_device` kind `rope_table`) uses the same
//! arithmetic.
const std = @import("std");

pub fn validate(x_len: usize, table_len: usize, width: usize, head_dim: usize) !usize {
    if (head_dim == 0 or head_dim % 2 != 0 or width == 0 or width % head_dim != 0 or x_len % width != 0)
        return error.InvalidRopeTable;
    const rows = x_len / width;
    if (table_len != rows * (width / head_dim) * (head_dim / 2)) return error.InvalidRopeTable;
    return rows;
}

pub fn apply(out: []f32, x: []const f32, cos: []const f32, sin: []const f32, width: usize, head_dim: usize, inverse: bool) !void {
    const rows = try validate(x.len, cos.len, width, head_dim);
    if (out.len != x.len or sin.len != cos.len) return error.InvalidRopeTable;
    const half = head_dim / 2;
    const heads = width / head_dim;
    for (0..rows) |row| for (0..heads) |head| {
        const base = row * width + head * head_dim;
        const table = (row * heads + head) * half;
        for (0..half) |i| {
            const lo = x[base + i];
            const hi = x[base + half + i];
            const c = cos[table + i];
            const s = sin[table + i];
            if (inverse) {
                out[base + i] = lo * c + hi * s;
                out[base + half + i] = hi * c - lo * s;
            } else {
                out[base + i] = lo * c - hi * s;
                out[base + half + i] = lo * s + hi * c;
            }
        }
    };
}

/// `fused_qkv_rope_table`: forward takes `x` = [rows, 3*width] (Q | K | V per
/// row) to planar [3, rows, width], rotating Q and K by the tables and copying
/// V. Inverse takes planar [3, rows, width] back to [rows, 3*width] with the
/// inverse rotation, which is the forward's VJP.
pub fn applyQkv(out: []f32, x: []const f32, cos: []const f32, sin: []const f32, width: usize, head_dim: usize, inverse: bool) !void {
    if (x.len % (3 * width) != 0 or out.len != x.len) return error.InvalidRopeTable;
    const rows = try validate(x.len / 3, cos.len, width, head_dim);
    if (sin.len != cos.len) return error.InvalidRopeTable;
    const half = head_dim / 2;
    const heads = width / head_dim;
    for (0..3) |part| for (0..rows) |row| for (0..heads) |head| {
        // Packed [rows, 3*width] and planar [3, rows, width] offsets of the
        // same (part, row, head) block.
        const packed_base = row * 3 * width + part * width + head * head_dim;
        const planar_base = (part * rows + row) * width + head * head_dim;
        const src = if (inverse) planar_base else packed_base;
        const dst = if (inverse) packed_base else planar_base;
        if (part == 2) {
            @memcpy(out[dst..][0..head_dim], x[src..][0..head_dim]);
            continue;
        }
        const table = (row * heads + head) * half;
        for (0..half) |i| {
            const lo = x[src + i];
            const hi = x[src + half + i];
            const c = cos[table + i];
            const s = sin[table + i];
            if (inverse) {
                out[dst + i] = lo * c + hi * s;
                out[dst + half + i] = hi * c - lo * s;
            } else {
                out[dst + i] = lo * c - hi * s;
                out[dst + half + i] = lo * s + hi * c;
            }
        }
    };
}

test "qkv rope table inverse restores the packed rows" {
    // Two rows, one head of dimension 4: x is [2, 12], tables [2, 2].
    var x: [24]f32 = undefined;
    for (&x, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) * 0.25 - 2;
    const cos = [_]f32{ 0.6, 1, -0.8, 0.28 };
    const sin = [_]f32{ 0.8, 0, 0.6, 0.96 };
    var planar: [24]f32 = undefined;
    var restored: [24]f32 = undefined;
    try applyQkv(&planar, &x, &cos, &sin, 4, 4, false);
    try applyQkv(&restored, &planar, &cos, &sin, 4, 4, true);
    for (x, restored) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
    // V of row 1 is copied unrotated into the third plane.
    try std.testing.expectEqualSlices(f32, x[12 + 8 .. 12 + 12], planar[2 * 8 + 4 .. 2 * 8 + 8]);
}

test "rope table inverse undoes the forward rotation" {
    const x = [_]f32{ 1, 2, 3, 4, -1, 0.5, 2, -3 };
    // One row of two heads of dimension 4: tables are [2, 2].
    const cos = [_]f32{ 0.6, 1, -0.8, 0.28 };
    const sin = [_]f32{ 0.8, 0, 0.6, 0.96 };
    var rotated: [8]f32 = undefined;
    var restored: [8]f32 = undefined;
    try apply(&rotated, &x, &cos, &sin, 8, 4, false);
    try apply(&restored, &rotated, &cos, &sin, 8, 4, true);
    for (x, restored) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1 * 0.6 - 3 * 0.8), rotated[0], 1e-6);
    try std.testing.expectError(error.InvalidRopeTable, apply(&rotated, &x, cos[0..2], sin[0..2], 8, 4, false));
}
