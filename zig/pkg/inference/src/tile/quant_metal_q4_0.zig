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

//! Q4_0 matvec in the ggml mul_mv_q4_0 style ("mmv16"): sixteen lanes per
//! simdgroup each take eight values of a 32-value block (NQ = 16 blocks in
//! flight), activations are pre-divided by 1, 256, 16, 4096 so nibbles are
//! read through uint16 masks without shifts, and the -8 offset folds into
//! `- 8 * sumy`. Each simdgroup owns NR0 output rows; no threadgroup
//! reduction. Variants differ in input / output type, weight layout, a
//! second (gate / up) weight, and the epilogue.

const msl = @import("quant_metal_msl.zig");
const wrap = @import("quant_metal_wrap.zig");
const fill = msl.fill;
const join = msl.join;

/// Stages eight activations of block `b` into yl[16] (pre-scaled for the
/// uint16 nibble masks) and their sum. `cvt` converts an input element.
pub fn stage(comptime loop: []const u8, comptime in_off: []const u8, comptime cvt: []const u8) []const u8 {
    const ld = struct {
        fn f(comptime c: []const u8, comptime e: []const u8) []const u8 {
            const x = "input[in_off + " ++ e ++ "]";
            return if (c.len == 0) x else c ++ "(" ++ x ++ ")";
        }
    }.f;
    return fill("float yl[16]; float sumy = 0.0f; uint in_off = ${off}; ${loop} (uint i = 0u; i < 8u; i += 2u) { float x0 = ${a}; float x1 = ${b}; float x16 = ${c}; float x17 = ${d}; sumy += x0 + x1 + x16 + x17; yl[i] = x0; yl[i + 1u] = x1 / 256.0f; yl[i + 8u] = x16 / 16.0f; yl[i + 9u] = x17 / 4096.0f; }", .{
        .off = in_off,
        .loop = loop,
        .a = ld(cvt, "i"),
        .b = ld(cvt, "i + 1u"),
        .c = ld(cvt, "16u + i"),
        .d = ld(cvt, "16u + i + 1u"),
    });
}

/// `<q> += yl[...] * float(<packed> & mask)` for the four nibbles.
pub fn nibbles(comptime q: []const u8, comptime packed_: []const u8) []const u8 {
    return fill("${q} += yl[i] * float(${p} & 0x000Fu); ${q} += yl[i + 1u] * float(${p} & 0x0F00u); ${q} += yl[i + 8u] * float(${p} & 0x00F0u); ${q} += yl[i + 9u] * float(${p} & 0xF000u);", .{ .q = q, .p = packed_ });
}

/// One weight matrix of a kernel: its buffer, the per-row accumulator, and
/// the suffix of its locals (`d`, `qs`, `qacc`, `packed`) and prefix of its
/// `bits`.
pub const Weight = struct { buf: []const u8, acc: []const u8, sfx: []const u8 = "", pre: []const u8 = "" };

pub const single = [_]Weight{.{ .buf = "weight", .acc = "acc" }};
pub const gate_up = [_]Weight{ .{ .buf = "weight_gate", .acc = "gate", .sfx = "_gate", .pre = "gate_" }, .{ .buf = "weight_up", .acc = "up", .sfx = "_up", .pre = "up_" } };

/// For every weight, `t` with ${w} ${acc} ${s} ${pre} filled, joined by spaces.
fn per(comptime ws: []const Weight, comptime t: []const u8) []const u8 {
    var parts: []const []const u8 = &.{};
    for (ws, 0..) |x, i| {
        if (i != 0) parts = parts ++ .{" "};
        parts = parts ++ .{fill(t, .{ .w = x.buf, .acc = x.acc, .s = x.sfx, .pre = x.pre })};
    }
    return join(parts);
}

/// The row loop body after `uint off` (`u`: suffix of the `off + 1` literal): scales, packed nibble pointers, the
/// masked dot, and the per-row accumulation.
pub fn rowDot(comptime ws: []const Weight, comptime loop: []const u8, comptime u: []const u8) []const u8 {
    return join(&.{
        per(ws, fill("ushort $${pre}bits = (ushort($${w}[off + 1${u}]) << 8) | ushort($${w}[off]);", .{ .u = u })),                               " ",
        per(ws, "float d${s} = float(as_type<half>(${pre}bits));"),                                                     " ",
        per(ws, "const device ushort *qs${s} = reinterpret_cast<const device ushort *>(${w} + off + 2u) + (il >> 1);"), " ",
        per(ws, "float qacc${s} = 0.0f;"),                                                                              " ",
        loop,                                                                                                           " (uint i = 0u; i < 8u; i += 2u) { ",
        per(ws, "uint packed${s} = uint(qs${s}[i >> 1]);"),                                                             " ",
        per(ws, nibbles("qacc${s}", "packed${s}")),                                                                     " } ",
        per(ws, "${acc}[row] += d${s} * (qacc${s} - 8.0f * sumy);"),
    });
}

/// The NQ = 8 row body: eight unscaled activations x[i] at block offset
/// `j0`, nibbles decoded with the -8 offset, `hi` the high-nibble read.
pub fn rowDot8(comptime ws: []const Weight, comptime j0: []const u8, comptime hi: []const u8, comptime u: []const u8) []const u8 {
    return join(&.{
        per(ws, fill("ushort $${pre}bits = (ushort($${w}[off + 1${u}]) << 8) | ushort($${w}[off]);", .{ .u = u })),                                                " ",
        per(ws, "float d${s} = float(as_type<half>(${pre}bits));"),                                                                       " ",
        per(ws, "const device uchar *qs${s} = ${w} + off + 2u;"),                                                                         " ",
        per(ws, "float block${s} = 0.0f;"),                                                                                               " for (uint i = 0u; i < NQ; ++i) { uint j = ",
        j0,                                                                                                                               " + i; ",
        per(ws, "uchar packed${s} = qs${s}[j & 15u];"),                                                                                   " ",
        per(ws, fill("float q${s} = j < 16u ? float(int(packed${s} & 0x0Fu) - 8) : float(int(${hi}) - 8);", .{ .s = "${s}", .hi = hi })), " ",
        per(ws, "block${s} += x[i] * q${s};"),                                                                                            " } ",
        per(ws, "${acc}[row] += d${s} * block${s};"),
    });
}

fn zeros(comptime n: u32) []const u8 {
    var parts: []const []const u8 = &.{};
    for (0..n) |i| parts = parts ++ .{if (i == 0) "0.0f" else ", 0.0f"};
    return join(parts);
}

pub const Mmv16 = struct {
    name: []const u8,
    weights: []const Weight = &single,
    nr: u32,
    nsg: u32,
    /// Accumulator array length (some kernels keep a larger array than NR0).
    acc_len: u32 = 0,
    in_half: bool = false,
    out: []const u8 = "float",
    /// Extra device buffers after the output (before params).
    extra: []const []const u8 = &.{},
    activation: bool = false,
    /// `[tile][block][row][18]` 4-row packed weights.
    packed_t4: bool = false,
    /// Eight lanes per block (NQ = 8) with plain activations.
    nq8: bool = false,
    /// Value stored, from `v` (the accumulated row).
    store: []const u8 = "${v}",
    shmem: bool = true,
};

pub fn mmv16(comptime k: Mmv16) msl.Item {
    comptime {
        const pair = k.weights.len == 2;
        // The newer packed / NQ8 kernels skip the ib0 alias and cast sgitg.
        const lean = k.packed_t4 or k.nq8;
        var args: []const []const u8 = &.{if (k.in_half) wrap.in_f16 else wrap.in_f32};
        for (k.weights) |x| args = args ++ .{"device const uchar *" ++ x.buf};
        args = args ++ k.extra ++ .{ "device " ++ k.out ++ " *output", wrap.linear_p };
        if (k.activation) args = args ++ .{wrap.act_p};
        if (k.shmem) args = args ++ .{"threadgroup float *shmem"};
        args = args ++ wrap.sg;
        const value = if (pair) "termite_gated_activation_product(gate[row], up[row], ap.activation_kind)" else "acc[row]";
        return msl.msl(msl.signature(k.name, args) ++ fill(
            \\
            \\    const uint NR0 = ${nr}u; const uint NSG = ${nsg}u; const uint NQ = ${nq}u; const uint NW = 32u; uint first_o = (tg.x * NSG + ${sg}) * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = (lane - ix * (NW / NQ)) * ${il};${ib0} uint in_row = r * p.in_dim; ${accs}
            \\    for (uint b = ${b0}; b < p.row_blocks; b += NQ) { ${stage} for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = ${off}; ${dot} } }
            \\    for (uint row = 0u; row < NR0; ++row) ${simd} if (lane == 0u) { uint idx = r * p.out_dim + first_o; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = ${store}; } }
            \\}
            \\
        , .{
            .nr = k.nr,
            .nsg = k.nsg,
            .nq = if (k.nq8) 8 else 16,
            .il = if (k.nq8) "NQ" else "8u",
            .sg = if (lean) "uint(sgitg)" else "sgitg",
            .ib0 = if (lean) "" else " uint ib0 = ix;",
            .accs = per(k.weights, fill("float ${acc}[${n}] = {${z}};", .{ .acc = "${acc}", .n = if (k.acc_len == 0) k.nr else k.acc_len, .z = zeros(if (k.acc_len == 0) k.nr else k.acc_len) })),
            .b0 = if (lean) "ix" else "ib0",
            .stage = if (k.nq8) "float x[8]; uint in_off = in_row + b * 32u + il; for (uint i = 0u; i < NQ; ++i) x[i] = input[in_off + i];" else stage("for", "in_row + b * 32u + il", if (k.in_half) "float" else ""),
            .off = if (k.packed_t4) "(((o >> 2u) * p.row_blocks + b) * 4u + (o & 3u)) * 18u" else "o * p.row_blocks * 18u + b * 18u",
            .dot = if (k.nq8) rowDot8(k.weights, "il", "packed${s} >> 4", "u") else rowDot(k.weights, "for", if (k.packed_t4) "u" else ""),
            .simd = if (lean and !pair) "acc[row] = simd_sum(acc[row]);" else "{ " ++ per(k.weights, "${acc}[row] = simd_sum(${acc}[row]);") ++ " }",
            .store = fill(k.store, .{ .v = value }),
        }));
    }
}

pub const pair_activation_mmv16 = [_]msl.Item{
    mmv16(.{ .name = "termite_q4_0_pair_activation_1x_packed_t4_nsg2", .weights = &gate_up, .nr = 4, .nsg = 2, .activation = true, .packed_t4 = true, .shmem = false }),
    mmv16(.{ .name = "termite_q4_0_pair_activation_1x_reduce", .weights = &gate_up, .nr = 4, .nsg = 4, .activation = true }),
    mmv16(.{ .name = "termite_q4_0_pair_activation_1x_reduce_out_f16", .weights = &gate_up, .nr = 4, .nsg = 4, .activation = true, .out = "half", .store = "half(${v})" }),
};

const rhs_value = "termite_gated_activation_product(acc[row], rhs[idx + row], ap.activation_kind)";

pub const activation_rhs_mmv16 = [_]msl.Item{
    mmv16(.{ .name = "termite_q4_0_activation_rhs_1x_reduce", .nr = 16, .nsg = 4, .extra = &.{"device const float *rhs"}, .activation = true, .store = rhs_value }),
    mmv16(.{ .name = "termite_q4_0_activation_rhs_1x_reduce_out_f16", .nr = 4, .nsg = 4, .extra = &.{"device const float *rhs"}, .activation = true, .out = "half", .store = "half(" ++ rhs_value ++ ")" }),
};

pub const linear_packed_t4_nsg4 = mmv16(.{ .name = "termite_q4_0_linear_1x_packed_t4_nsg4", .nr = 4, .nsg = 4, .packed_t4 = true, .shmem = false });

pub const linear_mmv16_io = [_]msl.Item{
    mmv16(.{ .name = "termite_q4_0_linear_1x_reduce_out_f16", .nr = 16, .nsg = 4, .out = "half", .store = "half(${v})" }),
    mmv16(.{ .name = "termite_q4_0_linear_1x_reduce_in_f16", .nr = 8, .nsg = 4, .acc_len = 16, .in_half = true }),
    mmv16(.{ .name = "termite_q4_0_linear_1x_reduce_in_f16_small", .nr = 4, .nsg = 2, .in_half = true }),
    mmv16(.{ .name = "termite_q4_0_linear_1x_reduce_in_f16_out_f16", .nr = 8, .nsg = 4, .in_half = true, .out = "half", .store = "half(${v})" }),
};

pub const nq8 = [_]msl.Item{
    mmv16(.{ .name = "termite_q4_0_pair_activation_1x_reduce_nr4_nsg2_nq8", .weights = &gate_up, .nr = 4, .nsg = 2, .activation = true, .nq8 = true, .shmem = false }),
    mmv16(.{ .name = "termite_q4_0_linear_1x_reduce_nr4_nsg4_nq8", .nr = 4, .nsg = 4, .nq8 = true }),
};

/// NQ = 8 lanes striding blocks across NSG simdgroups (blocks b = sgitg *
/// NQ + ix, step NSG * NQ), then a threadgroup reduction over simdgroups in
/// shmem rows of 32 per weight.
pub const Striped = struct {
    name: []const u8,
    weights: []const Weight = &single,
    nr: u32,
    nsg: u32,
    in_half: bool = false,
    out: []const u8 = "float",
    /// RMSNorm folded into the activation load (norm_weight, inv_scale).
    rms_scale: bool = false,
    /// Also write per-threadgroup sums of squares of the outputs.
    sumsq: bool = false,
    activation: bool = false,
};

pub fn striped(comptime k: Striped) msl.Item {
    comptime {
        const pair = k.weights.len == 2;
        var args: []const []const u8 = &.{if (k.in_half) wrap.in_f16 else wrap.in_f32};
        for (k.weights) |x| args = args ++ .{"device const uchar *" ++ x.buf};
        args = args ++ .{ "device " ++ k.out ++ " *output", wrap.linear_p };
        if (k.activation) args = args ++ .{wrap.act_p};
        if (k.rms_scale) args = args ++ .{ "device const float *norm_weight", "device const float *inv_scale" };
        if (k.sumsq) args = args ++ .{"device float *sumsq"};
        args = args ++ .{"threadgroup float *shmem"} ++ wrap.sg;
        const stride = 32 * k.weights.len;
        const slot = fill("row * ${s}u + ", .{ .s = stride });
        const slots = [_][]const u8{ slot, slot ++ "32u + " };
        var zero: []const []const u8 = &.{};
        var put: []const []const u8 = &.{};
        var tot: []const []const u8 = &.{};
        for (k.weights, 0..) |x, i| {
            const sep = if (i == 0) "" else " ";
            zero = zero ++ .{ sep, "shmem[", slots[i], "lane] = 0.0f;" };
            put = put ++ .{ sep, "shmem[", slots[i], "sgitg] = ", x.acc, "[row];" };
            tot = tot ++ .{ sep, "total", x.sfx, "[row] = simd_sum(shmem[", slots[i], "lane]);" };
        }
        const store = if (k.sumsq)
            "float ss = 0.0f; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) { float v = total[row]; output[idx + row] = v; ss += v * v; } } sumsq[r * ((p.out_dim + 7u) / 8u) + tg.x] = ss;"
        else
            "for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o < p.out_dim) output[idx + row] = half(termite_gated_activation_product(total_gate[row], total_up[row], ap.activation_kind)); }";
        if (!pair and !k.sumsq) @compileError(k.name ++ ": striped single-weight kernels store sums of squares");
        return msl.msl(msl.signature(k.name, args) ++ fill(
            \\
            \\    const uint NR0 = ${nr}u; const uint NSG = ${nsg}u; const uint NQ = 8u; const uint NW = 32u; uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
            \\    uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ); uint ib0 = sgitg * NQ + ix; uint in_row = r * p.in_dim;${scale} ${accs}
            \\    for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float x[8]; uint qj0 = il * NQ; uint in_off = in_row + b * 32u + qj0;${norm} for (uint i = 0u; i < NQ; ++i) { x[i] = ${ld}; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 18u + b * 18u; ${dot} } }
            \\    if (sgitg == 0u) { for (uint row = 0u; row < NR0; ++row) { ${zero} } } for (uint row = 0u; row < NR0; ++row) { ${simd} } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    if (lane == 0u) { for (uint row = 0u; row < NR0; ++row) { ${put} } } threadgroup_barrier(mem_flags::mem_threadgroup);
            \\    ${totals} for (uint row = 0u; row < NR0; ++row) { ${tot} } if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; ${store} }
            \\}
            \\
        , .{
            .nr = k.nr,
            .nsg = k.nsg,
            .scale = if (k.rms_scale) " float scale = inv_scale[r];" else "",
            .accs = per(k.weights, fill("float ${acc}[${n}] = {${z}};", .{ .acc = "${acc}", .n = k.nr, .z = zeros(k.nr) })),
            .norm = if (k.rms_scale) " uint norm_off = b * 32u + qj0;" else "",
            .ld = if (k.rms_scale) "input[in_off + i] * norm_weight[norm_off + i] * scale" else if (k.in_half) "float(input[in_off + i])" else "input[in_off + i]",
            .dot = rowDot8(k.weights, "qj0", "(packed${s} >> 4) & 0x0Fu", ""),
            .zero = join(zero),
            .simd = per(k.weights, "${acc}[row] = simd_sum(${acc}[row]);"),
            .put = join(put),
            .totals = per(k.weights, fill("float total${s}[${n}];", .{ .s = "${s}", .n = k.nr })),
            .tot = join(tot),
            .store = store,
        }));
    }
}

pub const sumsq = [_]msl.Item{
    striped(.{ .name = "termite_q4_0_linear_1x_reduce_sumsq", .nr = 8, .nsg = 4, .sumsq = true }),
    striped(.{ .name = "termite_q4_0_linear_1x_reduce_in_f16_sumsq", .nr = 8, .nsg = 4, .in_half = true, .sumsq = true }),
};
pub const rms_scale_pair = striped(.{ .name = "termite_q4_0_pair_activation_rms_scale_1x_reduce_out_f16", .weights = &gate_up, .nr = 4, .nsg = 2, .out = "half", .rms_scale = true, .activation = true });
