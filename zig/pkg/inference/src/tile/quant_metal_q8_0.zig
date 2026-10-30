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

//! Q8_0 activation-multiply matvec (`*_activation_multiply_*`): the result
//! is `act(gate) * up` of two weights, or `act(x W) * rhs` of one weight
//! and a given right-hand side.
//!
//! `_1r_ext`: one input row, 8 x 4 lanes per simdgroup (eight lanes along K
//! in float4 chunks, four output rows), shuffle-down reduction.
//! `_1x_reduce4`: four output rows per threadgroup, NQ = 8 blocks per
//! simdgroup pass, unrolled accumulators and a threadgroup reduction.

const msl = @import("quant_metal_msl.zig");
const wrap = @import("quant_metal_wrap.zig");
const fill = msl.fill;
const join = msl.join;

/// A weight's buffer, accumulator, and local names (scale bits, scale,
/// weight vector or block sum).
const W = struct { buf: []const u8, acc: []const u8, bits: []const u8, d: []const u8, w: []const u8, block: []const u8, offset: []const u8 };

const one = [_]W{.{ .buf = "weight", .acc = "acc", .bits = "bits", .d = "d", .w = "w", .block = "block", .offset = "weight_offset" }};
const two = [_]W{
    .{ .buf = "weight_gate", .acc = "gate", .bits = "bits_g", .d = "dg", .w = "wg", .block = "block_g", .offset = "weight_offset" },
    .{ .buf = "weight_up", .acc = "up", .bits = "bits_u", .d = "du", .w = "wu", .block = "block_u", .offset = "second_weight_offset" },
};

fn per(comptime ws: []const W, comptime t: []const u8) []const u8 {
    var parts: []const []const u8 = &.{};
    for (ws, 0..) |x, i| {
        if (i != 0) parts = parts ++ .{" "};
        parts = parts ++ .{fill(t, .{ .buf = x.buf, .acc = x.acc, .bits = x.bits, .d = x.d, .w = x.w, .block = x.block, .offset = x.offset })};
    }
    return join(parts);
}

fn args(comptime ws: []const W, comptime out: []const u8, comptime rms: bool, comptime shmem: bool) []const []const u8 {
    var a: []const []const u8 = &.{wrap.in_f32};
    for (ws) |x| a = a ++ .{"device const uchar *" ++ x.buf};
    if (ws.len == 1) a = a ++ .{"device const float *rhs"};
    a = a ++ .{ "device " ++ out ++ " *output", wrap.linear_p, wrap.act_p };
    if (rms) a = a ++ .{ "device const float *norm_weight", "device const float *inv_scale" };
    if (shmem) a = a ++ .{"threadgroup float *shmem"};
    return a ++ wrap.sg;
}

/// float4 of the four int8 weights of chunk ci, times the block scale.
fn chunk4(comptime x: W) []const u8 {
    return fill("float4 ${w} = float4(float(as_type<char>(${b}[off + 2u + ci * 4u])), float(as_type<char>(${b}[off + 3u + ci * 4u])), float(as_type<char>(${b}[off + 4u + ci * 4u])), float(as_type<char>(${b}[off + 5u + ci * 4u]))) * ${d};", .{ .w = x.w, .b = x.buf, .d = x.d });
}

pub fn ext1r(comptime name: []const u8, comptime ws: []const W, comptime rms: bool, comptime half_out: bool) msl.Item {
    comptime {
        const pair = ws.len == 2;
        var w4: []const []const u8 = &.{};
        for (ws, 0..) |x, i| w4 = w4 ++ .{ if (i == 0) "" else " ", chunk4(x) };
        var shuffle: []const []const u8 = &.{};
        for ([_][]const u8{ "4u", "2u", "1u" }, 0..) |s, i| for (ws, 0..) |x, j| {
            if (i + j != 0) shuffle = shuffle ++ .{" "};
            shuffle = shuffle ++ .{fill("${a} += simd_shuffle_down(${a}, ${s});", .{ .a = x.acc, .s = s })};
        };
        const value = if (pair) "termite_gated_activation_product(gate, up, ap.activation_kind)" else "termite_gated_activation_product(acc, rhs[idx], ap.activation_kind)";
        return msl.msl(msl.signature(name, args(ws, if (half_out) "half" else "float", rms, false)) ++ fill(
            \\
            \\    ${offsets}
            \\    const uint NX = 8u; const uint NY = 4u; const uint NSG = 4u; const uint CHPT = 4u; uint tx = uint(lane) & 7u; uint ty = uint(lane) >> 3; uint o = tg.x * (NY * NSG) + uint(sgitg) * NY + ty; uint r = tg.y; if (r >= p.rows || o >= p.out_dim) return; uint row_offset = o * p.row_blocks * 34u; uint total_chunks = p.row_blocks * 8u; uint input_row = r * p.in_dim;${scale} ${accs}
            \\    for (uint chunk = tx; chunk < total_chunks; chunk += CHPT * NX) { for (uint ch = 0u; ch < CHPT; ++ch) { uint c = chunk + ch * NX; if (c >= total_chunks) continue; uint b = c >> 3; uint ci = c & 7u; uint off = row_offset + b * 34u; ${bits} ${ds} ${w4} uint in_col = input_row + c * 4u; ${x} ${dots} } }
            \\    ${shuffle} if (tx == 0u) ${store}
            \\}
            \\
        , .{
            .offsets = per(ws, "${buf} += p.${offset};"),
            .scale = if (rms) " float scale = inv_scale[r];" else "",
            .accs = per(ws, "float ${acc} = 0.0f;"),
            .bits = per(ws, "ushort ${bits} = (ushort(${buf}[off + 1u]) << 8) | ushort(${buf}[off]);"),
            .ds = per(ws, "float ${d} = float(as_type<half>(${bits}));"),
            .w4 = join(w4),
            .x = if (rms)
                "uint norm_col = c * 4u; float4 x = float4(input[in_col] * norm_weight[norm_col], input[in_col + 1u] * norm_weight[norm_col + 1u], input[in_col + 2u] * norm_weight[norm_col + 2u], input[in_col + 3u] * norm_weight[norm_col + 3u]) * scale;"
            else
                "float4 x = float4(input[in_col], input[in_col + 1u], input[in_col + 2u], input[in_col + 3u]);",
            .dots = per(ws, "${acc} += dot(x, ${w});"),
            .shuffle = join(shuffle),
            .store = if (pair)
                fill("output[r * p.out_dim + o] = ${v};", .{ .v = if (half_out) "half(" ++ value ++ ")" else value })
            else
                "{ uint idx = r * p.out_dim + o; output[idx] = " ++ value ++ "; }",
        }));
    }
}

pub const pair_ext1r = [_]msl.Item{
    ext1r("termite_q8_0_pair_activation_multiply_1r_ext", &two, false, false),
    ext1r("termite_q8_0_pair_activation_multiply_rms_scale_1r_ext", &two, true, false),
    ext1r("termite_q8_0_pair_activation_multiply_rms_scale_1r_ext_out_f16", &two, true, true),
};
pub const rhs_ext1r = ext1r("termite_q8_0_activation_multiply_1r_ext", &one, false, false);

/// `if (row == 0u) { ... } else if ... else { ... }` over NR0 rows; braces
/// only around several statements.
fn byRow(comptime nr: u32, comptime ws: []const W, comptime t: []const u8) []const u8 {
    var parts: []const []const u8 = &.{};
    for (0..nr) |k| {
        const body = per(ws, fill(t, .{ .k = k, .acc = "${acc}", .d = "${d}", .block = "${block}" }));
        const stmt = if (ws.len == 1) " " ++ body else " { " ++ body ++ " }";
        parts = parts ++ .{if (k == 0)
            "if (row == 0u)" ++ stmt
        else if (k + 1 < nr)
            fill(" else if (row == ${k}u)", .{ .k = k }) ++ stmt
        else
            " else" ++ stmt};
    }
    return join(parts);
}

/// `t` for each (row k, weight) slot in that order, slot index `i`.
fn slots(comptime nr: u32, comptime ws: []const W, comptime t: []const u8) []const u8 {
    var parts: []const []const u8 = &.{};
    for (0..nr) |k| for (ws, 0..) |x, j| {
        const i = k * ws.len + j;
        if (i != 0) parts = parts ++ .{" "};
        parts = parts ++ .{fill(t, .{
            .a = fill("${a}${k}", .{ .a = x.acc, .k = k }),
            .t = fill("total${s}${k}", .{ .s = if (ws.len == 1) "" else "_" ++ x.acc, .k = k }),
            .sh = if (i == 0) "" else fill("${o}u + ", .{ .o = 32 * i }),
        })};
    };
    return join(parts);
}

pub fn reduce4(comptime name: []const u8, comptime ws: []const W) msl.Item {
    comptime {
        const pair = ws.len == 2;
        var store: []const []const u8 = &.{};
        for (0..4) |k| {
            if (k != 0) store = store ++ .{" "};
            const o = if (k == 0) "first_o" else fill("first_o + ${k}u", .{ .k = k });
            const idx = if (k == 0) "idx" else fill("idx + ${k}u", .{ .k = k });
            store = store ++ .{fill("if (${o} < p.out_dim) output[${i}] = termite_gated_activation_product(${a}, ${b}, ap.activation_kind);", .{
                .o = o,
                .i = idx,
                .a = fill(if (pair) "total_gate${k}" else "total${k}", .{ .k = k }),
                .b = if (pair) fill("total_up${k}", .{ .k = k }) else "rhs[" ++ idx ++ "]",
            })};
        }
        return msl.msl(msl.signature(name, args(ws, "float", false, true)) ++ fill(
            \\
            \\    ${offsets}
            \\    const uint NR0 = 4u; const uint NSG = 4u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint ib0 = sgitg * NQ + ix; uint in_row = r * p.in_dim; ${accs}
            \\    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float x[8]; uint in_off = in_row + b * 32u + il * NQ; for (uint i = 0; i < NQ; ++i) { x[i] = input[in_off + i]; } for (uint row = 0; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 34u + b * 34u; ${bits} ${ds} ${blocks} ${dot} ${acc} } }
            \\    if (sgitg == 0u) { ${zero} } ${simd} threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    if (lane == 0u) { ${put} } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    ${total} if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; ${store} }
            \\}
            \\
        , .{
            .offsets = per(ws, "${buf} += p.${offset};"),
            .accs = slots(4, ws, "float ${a} = 0.0f;"),
            .bits = per(ws, "ushort ${bits} = (ushort(${buf}[off + 1]) << 8) | ushort(${buf}[off]);"),
            .ds = per(ws, "float ${d} = float(as_type<half>(${bits}));"),
            .blocks = per(ws, "float ${block} = 0.0f;"),
            .dot = if (pair)
                "for (uint i = 0; i < NQ; ++i) { float xv = x[i]; " ++ per(ws, "${block} += xv * float(as_type<char>(${buf}[off + 2u + il * NQ + i]));") ++ " }"
            else
                "for (uint i = 0; i < NQ; ++i) block += x[i] * float(as_type<char>(weight[off + 2u + il * NQ + i]));",
            .acc = byRow(4, ws, "${acc}${k} += ${d} * ${block};"),
            .zero = slots(4, ws, "shmem[${sh}lane] = 0.0f;"),
            .simd = slots(4, ws, "${a} = simd_sum(${a});"),
            .put = slots(4, ws, "shmem[${sh}sgitg] = ${a};"),
            .total = slots(4, ws, "float ${t} = simd_sum(shmem[${sh}lane]);"),
            .store = join(store),
        }));
    }
}

pub const pair_reduce4 = reduce4("termite_q8_0_pair_activation_multiply_1x_reduce4", &two);
pub const rhs_reduce4 = reduce4("termite_q8_0_activation_multiply_1x_reduce4", &one);
