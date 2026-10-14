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
