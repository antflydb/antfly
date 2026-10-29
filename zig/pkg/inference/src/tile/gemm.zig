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

//! Tile GEMM: out[M, N] = epilogue(x[M, K] · w[N, K]ᵀ). `w` is in the
//! PyTorch `nn.Linear` layout (one row per output feature), so the inner
//! loop is a dot product over two contiguous rows.
//!
//! The epilogue is an `FExpr` over the dot product (`acc`), a per-column
//! `bias` and a per-element `residual`; bias, GELU, residual adds and output
//! scaling fuse into the store. `residual` may alias `out`: each element is
//! read and written by the same lane exactly once.

const std = @import("std");
const fexpr = @import("fexpr.zig");
const parallel = @import("parallel.zig");

pub const Gemm = struct {
    name: []const u8,
    epilogue: fexpr.FExpr = .acc,
    /// GPU block tile and per-thread register tile.
    tile_m: u32 = 64,
    tile_n: u32 = 64,
    tile_k: u32 = 16,
    reg_m: u32 = 4,
    reg_n: u32 = 4,

    pub fn threads(comptime g: Gemm) u32 {
        return (g.tile_m / g.reg_m) * (g.tile_n / g.reg_n);
    }
    pub fn usesBias(comptime g: Gemm) bool {
        return g.epilogue.uses(.bias);
    }
    pub fn usesResidual(comptime g: Gemm) bool {
        return g.epilogue.uses(.residual);
    }
};

pub const Args = struct {
    x: []const f32,
    w: []const f32,
    bias: []const f32 = &.{},
    /// Pointer, not slice, so it may alias `out`.
    residual: ?[*]const f32 = null,
    out: [*]f32,
    m: usize,
    n: usize,
    k: usize,
};

/// GPU launch: grid (ceil(n / tile_n), ceil(m / tile_m)), `threads()` per block.
pub fn grid(comptime g: Gemm, m: usize, n: usize) [2]usize {
    return .{ (n + g.tile_n - 1) / g.tile_n, (m + g.tile_m - 1) / g.tile_m };
}

/// f64 oracle.
pub fn reference(comptime g: Gemm, a: Args) void {
    for (0..a.m) |i| for (0..a.n) |j| {
        var s: f64 = 0;
        for (0..a.k) |kk| s += @as(f64, a.x[i * a.k + kk]) * a.w[j * a.k + kk];
        const b: f64 = if (g.usesBias()) a.bias[j] else 0;
        const r: f64 = if (g.usesResidual()) a.residual.?[i * a.n + j] else 0;
        a.out[i * a.n + j] = @floatCast(g.epilogue.evalF64(s, b, r));
    };
}

const lanes = 16;
const V = @Vector(lanes, f32);

pub fn run(comptime g: Gemm, a: Args) void {
    // Split rows when there are enough, else columns (decode steps, logits).
    const by_rows = a.m >= 32;
    const items = if (by_rows) (a.m + 3) / 4 else (a.n + 3) / 4;
    const work_per_item = 4 * a.k * (if (by_rows) a.n else a.m);
    const min_items = @max(@as(usize, 1), (1 << 18) / @max(work_per_item, 1));
    const Ctx = struct {
        a: Args,
        by_rows: bool,
        fn body(ctx: @This(), begin: usize, end: usize) void {
            const a2 = ctx.a;
            if (ctx.by_rows) {
                rangeRows(g, a2, @min(begin * 4, a2.m), @min(end * 4, a2.m), 0, a2.n);
            } else {
                rangeRows(g, a2, 0, a2.m, @min(begin * 4, a2.n), @min(end * 4, a2.n));
            }
        }
    };
    parallel.forRanges(items, min_items, Ctx{ .a = a, .by_rows = by_rows }, Ctx.body);
}

fn rangeRows(comptime g: Gemm, a: Args, m0: usize, m1: usize, n0: usize, n1: usize) void {
    var i = m0;
    while (i < m1) {
        const mr = @min(4, m1 - i);
        var j = n0;
        while (j < n1) {
            const nr = @min(4, n1 - j);
            switch (mr) {
                4 => switch (nr) {
                    4 => block(g, 4, 4, a, i, j),
                    else => for (0..nr) |c| block(g, 4, 1, a, i, j + c),
                },
                else => for (0..mr) |r| switch (nr) {
                    4 => block(g, 1, 4, a, i + r, j),
                    else => for (0..nr) |c| block(g, 1, 1, a, i + r, j + c),
                },
            }
            j += nr;
        }
        i += mr;
    }
}

/// MR x NR outputs; K vectorized 16 wide with one accumulator per output.
inline fn block(comptime g: Gemm, comptime mr: usize, comptime nr: usize, a: Args, row0: usize, col0: usize) void {
    var acc: [mr][nr]V = @splat(@splat(@splat(0)));
    var kk: usize = 0;
    while (kk + lanes <= a.k) : (kk += lanes) {
        var xv: [mr]V = undefined;
        var wv: [nr]V = undefined;
        inline for (0..mr) |r| xv[r] = a.x[(row0 + r) * a.k + kk ..][0..lanes].*;
        inline for (0..nr) |c| wv[c] = a.w[(col0 + c) * a.k + kk ..][0..lanes].*;
        inline for (0..mr) |r| inline for (0..nr) |c| {
            acc[r][c] = @mulAdd(V, xv[r], wv[c], acc[r][c]);
        };
    }
    inline for (0..mr) |r| {
        var sums: @Vector(nr, f32) = undefined;
        var bias: @Vector(nr, f32) = @splat(0);
        var res: @Vector(nr, f32) = @splat(0);
        inline for (0..nr) |c| {
            var s = @reduce(.Add, acc[r][c]);
            var t = kk;
            while (t < a.k) : (t += 1) s += a.x[(row0 + r) * a.k + t] * a.w[(col0 + c) * a.k + t];
            sums[c] = s;
            if (comptime g.usesBias()) bias[c] = a.bias[col0 + c];
            if (comptime g.usesResidual()) res[c] = a.residual.?[(row0 + r) * a.n + col0 + c];
        }
        const y = g.epilogue.evalVec(nr, sums, bias, res);
        inline for (0..nr) |c| a.out[(row0 + r) * a.n + col0 + c] = y[c];
    }
}
