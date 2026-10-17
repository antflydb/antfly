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

//! Immutable F32 panels for reused transposed weights. No runtime dependencies.
const std = @import("std");
const builtin = @import("builtin");
const primitives = @import("primitives.zig");
pub const columns = 64;
const width = if (builtin.cpu.arch == .aarch64) 4 else primitives.vec_len;
const vectors = if (width <= 4) 4 else 2;
const nr = width * vectors;
const mr = 4;

pub const Panels = struct {
    data: []f32,
    n: usize,
    k: usize,
    pub fn bytes(n: usize, k: usize) !usize {
        const blocks = try std.math.divCeil(usize, n, columns);
        return std.math.mul(usize, try std.math.mul(usize, blocks, columns), try std.math.mul(usize, k, @sizeOf(f32)));
    }
    pub fn init(a: std.mem.Allocator, source: []const f32, n: usize, k: usize) !Panels {
        if (n == 0 or k == 0 or source.len != try std.math.mul(usize, n, k)) return error.InvalidTensorShape;
        const data = try a.alloc(f32, (try bytes(n, k)) / @sizeOf(f32));
        @memset(data, 0);
        var j: usize = 0;
        while (j < n) : (j += columns) {
            for (0..k) |l| for (0..@min(columns, n - j)) |c| {
                data[j / columns * k * columns + l * columns + c] = source[(j + c) * k + l];
            };
        }
        return .{ .data = data, .n = n, .k = k };
    }
    pub fn deinit(self: Panels, a: std.mem.Allocator) void {
        a.free(self.data);
    }
};

/// Cache blocks bound A to MC*KC and B to KC*64. Accumulate K panels in
/// order; vectorize independent output columns instead of reducing K lanes.
pub fn rows(p: Panels, start: usize, end: usize, a: []const f32, c: []f32) void {
    rowsTiled(256, 32, p, start, end, a, c);
}
pub fn rowsTiled(comptime kc: usize, comptime mc: usize, p: Panels, start: usize, end: usize, a: []const f32, c: []f32) void {
    const V = @Vector(width, f32);
    var jb: usize = 0;
    while (jb < p.n) : (jb += columns) {
        const je = @min(p.n, jb + columns);
        var ib = start;
        while (ib < end) : (ib += mc) {
            const ie = @min(end, ib + mc);
            var kb: usize = 0;
            while (kb < p.k) : (kb += kc) {
                const ke = @min(p.k, kb + kc);
                var i = ib;
                while (i < ie) : (i += mr) {
                    const count = @min(mr, ie - i);
                    var j = jb;
                    while (j + nr <= je) : (j += nr) {
                        var acc: [mr][vectors]V = undefined;
                        inline for (0..mr) |r| inline for (0..vectors) |v| {
                            acc[r][v] = if (r < count and kb != 0) c[(i + r) * p.n + j + v * width ..][0..width].* else @splat(0);
                        };
                        var l = kb;
                        while (l < ke) : (l += 1) {
                            const base = jb / columns * p.k * columns + l * columns + j - jb;
                            inline for (0..vectors) |v| {
                                const b: V = p.data[base + v * width ..][0..width].*;
                                inline for (0..mr) |r| if (r < count) {
                                    acc[r][v] = primitives.mulAdd(V, @splat(a[(i + r) * p.k + l]), b, acc[r][v]);
                                };
                            }
                        }
                        inline for (0..mr) |r| if (r < count) {
                            inline for (0..vectors) |v| c[(i + r) * p.n + j + v * width ..][0..width].* = acc[r][v];
                        };
                    }
                    while (j < je) : (j += 1) for (0..count) |r| {
                        var value: f32 = if (kb == 0) 0 else c[(i + r) * p.n + j];
                        for (kb..ke) |l| value = primitives.mulAdd(f32, a[(i + r) * p.k + l], p.data[jb / columns * p.k * columns + l * columns + j - jb], value);
                        c[(i + r) * p.n + j] = value;
                    };
                }
            }
        }
    }
}

test "prepared GEMM tails blocks row partitions and independent double reference" {
    const a = std.testing.allocator;
    for ([_][3]usize{ .{ 1, 1, 1 }, .{ 7, 19, 137 }, .{ 35, 67, 259 }, .{ 64, 256, 512 } }) |s| {
        const m, const n, const k = .{ s[0], s[1], s[2] };
        const x = try a.alloc(f32, m * k);
        defer a.free(x);
        const w = try a.alloc(f32, n * k);
        defer a.free(w);
        const y = try a.alloc(f32, m * n);
        defer a.free(y);
        for (x, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 31)) - 15)) / 31;
        for (w, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 37)) - 18)) / 37;
        const p = try Panels.init(a, w, n, k);
        defer p.deinit(a);
        @memset(w, 999); // Panels must own their immutable snapshot.
        @memset(y, std.math.nan(f32));
        rows(p, 0, m / 2, x, y);
        rows(p, m / 2, m, x, y);
        for (0..m) |i| for (0..n) |j| {
            var expected: f64 = 0;
            for (0..k) |l| expected += @as(f64, x[i * k + l]) * (@as(f64, @floatFromInt(@as(i32, @intCast((j * k + l) % 37)) - 18)) / 37);
            try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), y[i * n + j], 2e-5);
        };
    }
}
