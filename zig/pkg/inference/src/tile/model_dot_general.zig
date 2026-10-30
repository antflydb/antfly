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

//! Small-M f32 dot_general register tiles (Metal).
//!
//! One threadgroup owns a rows x cols output tile. Each thread strides the
//! contraction axis by the threadgroup width, accumulating every tile
//! element in registers; per-element partials then go through one
//! threadgroup tree, and thread 0 writes the in-bounds outputs. The rhs is
//! read as [k, n] (`rhs_contract_axis == 0`) or [n, k].
//!
//! The tiles differ only in spelling: 1x8 and 4x4 are written out per
//! element, 4x8 and 4x16 use arrays and loops.

const std = @import("std");

const fmt = std.fmt.comptimePrint;

pub const Spelling = enum {
    /// One row, scalar column guards.
    unrolled_row,
    /// Row and column guards, scalar accumulators.
    unrolled_tile,
    /// Arrays and loops; columns guards from an initializer list.
    looped_list,
    /// Arrays and loops; column guards from a loop.
    looped,
};

pub const Tile = struct { name: []const u8, rows: u32, cols: u32, spelling: Spelling };

pub const tiles = [_]Tile{
    .{ .name = "termite_dot_general_2d_f32_small_m_tile", .rows = 1, .cols = 8, .spelling = .unrolled_row },
    .{ .name = "termite_dot_general_2d_f32_small_m_rows4_cols4", .rows = 4, .cols = 4, .spelling = .unrolled_tile },
    .{ .name = "termite_dot_general_2d_f32_small_m_rows4_cols8", .rows = 4, .cols = 8, .spelling = .looped_list },
    .{ .name = "termite_dot_general_2d_f32_small_m_rows4_cols16", .rows = 4, .cols = 16, .spelling = .looped },
};

const barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);";

/// Partial slot i: `0`, `width`, `<i>u * width`.
fn slot(comptime i: usize) []const u8 {
    return switch (i) {
        0 => "0",
        1 => "width",
        else => fmt("{d}u * width", .{i}),
    };
}

/// Partial slot i at thread t.
fn at(comptime i: usize, comptime t: []const u8) []const u8 {
    return if (i == 0) t else slot(i) ++ " + " ++ t;
}

/// `base`, `base + <i>u`.
fn plus(comptime base: []const u8, comptime i: usize) []const u8 {
    return if (i == 0) base else fmt("{s} + {d}u", .{ base, i });
}

fn join(comptime n: usize, comptime item: fn (comptime usize) []const u8) []const u8 {
    var s: []const u8 = "";
    for (0..n) |i| s = s ++ (if (i == 0) "" else " ") ++ item(i);
    return s;
}

fn kernel(comptime t: Tile) []const u8 {
    const R = t.rows;
    const C = t.cols;
    var s: []const u8 = "kernel void " ++ t.name ++ "(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_dot_general_2d_f32_params &p [[buffer(3)]], threadgroup float *partials [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], uint3 tpg [[threads_per_threadgroup]], uint3 tg [[threadgroup_position_in_grid]]) {\n";
    if (t.spelling == .unrolled_row) {
        s = s ++ fmt("    uint row = tg.y; uint col0 = tg.x * {d}u; uint width = tpg.x; if (row >= p.m || p.k == 0u || width == 0u) return;\n", .{C});
    } else {
        s = s ++ fmt("    uint row0 = tg.y * {d}u; uint col0 = tg.x * {d}u; uint width = tpg.x; if (row0 >= p.m || col0 >= p.n || p.k == 0u || width == 0u || p.rhs_contract_axis > 1u) return;\n", .{ R, C });
    }
    const E = struct {
        fn r(comptime i: usize) []const u8 {
            return fmt("uint r{d} = {s};", .{ i, plus("row0", i) });
        }
        fn c(comptime i: usize) []const u8 {
            return fmt("uint c{d} = {s};", .{ i, plus("col0", i) });
        }
        fn acc(comptime i: usize) []const u8 {
            return fmt("float acc{d} = 0.0f;", .{i});
        }
        fn rowFma(comptime i: usize) []const u8 {
            return fmt("if (c{0d} < p.n) acc{0d} += l * (p.rhs_contract_axis == 0u ? rhs[kk * p.n + c{0d}] : rhs[c{0d} * p.k + kk]);", .{i});
        }
        fn rowStore(comptime i: usize) []const u8 {
            return fmt("partials[{s}] = acc{d};", .{ at(i, "uint(tid)"), i });
        }
        fn rowTree(comptime i: usize) []const u8 {
            return fmt("partials[{0s}] += partials[{0s} + stride];", .{at(i, "uint(tid)")});
        }
        fn rowOut(comptime i: usize) []const u8 {
            return fmt("if (c{d} < p.n) output[{s}] = partials[{s}];", .{ i, plus("base", i), slot(i) });
        }
        fn a(comptime i: usize) []const u8 {
            return fmt("float a{d}{d} = 0.0f;", .{ i / 4, i % 4 });
        }
        fn vr(comptime i: usize) []const u8 {
            return fmt("bool vr{0d} = r{0d} < p.m;", .{i + 1});
        }
        fn vc(comptime i: usize) []const u8 {
            return fmt("bool vc{0d} = c{0d} < p.n;", .{i + 1});
        }
        fn x(comptime i: usize) []const u8 {
            return if (i == 0) "float x0 = lhs[r0 * p.k + kk];" else fmt("float x{0d} = vr{0d} ? lhs[r{0d} * p.k + kk] : 0.0f;", .{i});
        }
        fn y(comptime i: usize) []const u8 {
            const stride = if (i == 1) "p.k" else fmt("{d}u * p.k", .{i});
            return if (i == 0) "float y0 = rhs[rhs_base];" else fmt("float y{0d} = vc{0d} ? rhs[rhs_base + (p.rhs_contract_axis == 0u ? {0d}u : {1s})] : 0.0f;", .{ i, stride });
        }
        fn fma(comptime i: usize) []const u8 {
            return fmt("a{0d}{1d} += x{0d} * y{1d};", .{ i / 4, i % 4 });
        }
        fn store(comptime i: usize) []const u8 {
            return fmt("partials[{s}] = a{d}{d};", .{ at(i, "t"), i / 4, i % 4 });
        }
        fn out(comptime i: usize) []const u8 {
            const r_ = i / 4;
            const c_ = i % 4;
            const b = fmt("b{d}", .{r_});
            const store_ = fmt("output[{s}] = partials[{s}];", .{ plus(b, c_), slot(i) });
            const head = if (c_ != 0) "" else fmt("{s}uint {s} = r{d} * p.n + col0; ", .{ if (r_ == 0) "" else fmt("if (vr{d}) {{ ", .{r_}), b, r_ });
            const guard = if (c_ == 0) "" else fmt("if (vc{d}) ", .{c_});
            const tail = if (c_ == 3 and r_ != 0) " }" else "";
            return head ++ guard ++ store_ ++ tail;
        }
    };
    switch (t.spelling) {
        .unrolled_row => {
            s = s ++ "    " ++ join(C, E.c) ++ "\n";
            s = s ++ "    " ++ join(C, E.acc) ++ "\n";
            s = s ++ "    for (uint kk = uint(tid); kk < p.k; kk += width) { float l = lhs[row * p.k + kk]; " ++ join(C, E.rowFma) ++ " }\n";
            s = s ++ "    " ++ join(C, E.rowStore) ++ " " ++ barrier ++ "\n";
            s = s ++ "    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) { if (tid < stride) { " ++ join(C, E.rowTree) ++ " } " ++ barrier ++ " }\n";
            s = s ++ "    if (tid == 0u) { uint base = row * p.n + col0; " ++ join(C, E.rowOut) ++ " }\n";
        },
        .unrolled_tile => {
            if (R != 4 or C != 4) @compileError(t.name ++ ": unrolled_tile is 4x4");
            s = s ++ "    " ++ join(R, E.r) ++ " " ++ join(C, E.c) ++ "\n";
            s = s ++ "    " ++ join(R * C, E.a) ++ "\n";
            s = s ++ "    " ++ join(R - 1, E.vr) ++ " " ++ join(C - 1, E.vc) ++ "\n";
            s = s ++ "    for (uint kk = uint(tid); kk < p.k; kk += width) { " ++ join(R, E.x) ++ " uint rhs_base = p.rhs_contract_axis == 0u ? kk * p.n + col0 : col0 * p.k + kk; " ++ join(C, E.y) ++ " " ++ join(R * C, E.fma) ++ " }\n";
            s = s ++ "    uint t = uint(tid); " ++ join(R * C, E.store) ++ " " ++ barrier ++ "\n";
            s = s ++ fmt("    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) {{ if (tid < stride) {{ for (uint i = 0u; i < {d}u; ++i) partials[i * width + t] += partials[i * width + t + stride]; }} " ++ barrier ++ " }}\n", .{R * C});
            s = s ++ "    if (tid == 0u) { " ++ join(R * C, E.out) ++ " }\n";
        },
        .looped_list, .looped => {
            if (R != 4) @compileError(t.name ++ ": looped tiles have 4 rows");
            const n = R * C;
            s = s ++ fmt("    float acc[{0d}]; for (uint i = 0u; i < {0d}u; ++i) acc[i] = 0.0f;\n", .{n});
            s = s ++ "    bool valid_r[4] = { true, row0 + 1u < p.m, row0 + 2u < p.m, row0 + 3u < p.m };";
            if (t.spelling == .looped_list) {
                s = s ++ fmt(" bool valid_c[{d}] = {{ true", .{C});
                for (1..C) |c| s = s ++ fmt(", col0 + {d}u < p.n", .{c});
                s = s ++ " };\n";
            } else {
                s = s ++ fmt("\n    bool valid_c[{0d}]; for (uint c = 0u; c < {0d}u; ++c) valid_c[c] = col0 + c < p.n;\n", .{C});
            }
            s = s ++ fmt("    for (uint kk = uint(tid); kk < p.k; kk += width) {{ float x[4]; x[0] = lhs[row0 * p.k + kk]; x[1] = valid_r[1] ? lhs[(row0 + 1u) * p.k + kk] : 0.0f; x[2] = valid_r[2] ? lhs[(row0 + 2u) * p.k + kk] : 0.0f; x[3] = valid_r[3] ? lhs[(row0 + 3u) * p.k + kk] : 0.0f; float y[{0d}]; uint rhs_base = p.rhs_contract_axis == 0u ? kk * p.n + col0 : col0 * p.k + kk; uint rhs_stride = p.rhs_contract_axis == 0u ? 1u : p.k; for (uint c = 0u; c < {0d}u; ++c) y[c] = valid_c[c] ? rhs[rhs_base + c * rhs_stride] : 0.0f; for (uint r = 0u; r < 4u; ++r) {{ for (uint c = 0u; c < {0d}u; ++c) acc[r * {0d}u + c] += x[r] * y[c]; }} }}\n", .{C});
            s = s ++ fmt("    uint t = uint(tid); for (uint i = 0u; i < {d}u; ++i) partials[i * width + t] = acc[i]; " ++ barrier ++ "\n", .{n});
            s = s ++ fmt("    for (uint stride = width >> 1u; stride > 0u; stride >>= 1u) {{ if (tid < stride) {{ for (uint i = 0u; i < {d}u; ++i) partials[i * width + t] += partials[i * width + t + stride]; }} " ++ barrier ++ " }}\n", .{n});
            s = s ++ fmt("    if (tid == 0u) {{ for (uint r = 0u; r < 4u; ++r) {{ if (!valid_r[r]) continue; uint base = (row0 + r) * p.n + col0; for (uint c = 0u; c < {0d}u; ++c) {{ if (valid_c[c]) output[base + c] = partials[(r * {0d}u + c) * width]; }} }} }}\n", .{C});
        },
    }
    return s ++ "}\n";
}

pub fn source() []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var s: []const u8 = "";
        for (tiles) |t| s = s ++ kernel(t);
        return s;
    }
}
