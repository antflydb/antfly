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

//! Quantized row gather / scatter / copy kernels (`termite_<fmt>_get_rows`,
//! `_set_rows`, `_cpy_q_to_f32`, `_cpy_f32_to_q`), one thread per value
//! (dequantize) or per block (quantize). Each format supplies its block
//! shape, how a value is read, and how a block is quantized; the four
//! kernels differ only in where the row index comes from.

const msl = @import("quant_metal_msl.zig");
const fill = msl.fill;
const join = msl.join;

const Read = enum {
    /// `d * <fmt>_block_value`, d from `<fmt>_block_scale`.
    scaled,
    /// `<fmt>_block_value` includes the scale.
    plain,
    /// `<fmt>_block_scale(...) * <fmt>_block_value(...)` in one expression.
    scaled_inline,
};

const Format = struct { name: []const u8, qk: u32, bytes: u32, read: Read, quantize: []const u8 };

/// `weight[off + at] .. weight[off + at + n - 1]` = little-endian bytes of `v`
/// (no trailing space).
fn storeBytes(comptime v: []const u8, comptime at: u32, comptime n: u32) []const u8 {
    var parts: []const []const u8 = &.{};
    for (0..n) |k| {
        const dst = if (at + k == 0) "weight[off]" else fill("weight[off + ${a}u]", .{ .a = at + k });
        const src = if (k == 0) fill("${v} & 0xffu", .{ .v = v }) else fill("(${v} >> ${s}) & 0xffu", .{ .v = v, .s = 8 * k });
        parts = parts ++ .{fill("${p}${d} = uchar(${s});", .{ .p = if (k == 0) "" else " ", .d = dst, .s = src })};
    }
    return join(parts);
}

fn halfBits(comptime v: []const u8, comptime x: []const u8) []const u8 {
    return fill("ushort ${v} = as_type<ushort>(half(${x})); ", .{ .v = v, .x = x });
}

fn off(comptime bytes: u32) []const u8 {
    return fill("uint off = (target_row * row_blocks + block) * ${b}u; ", .{ .b = bytes });
}

/// Symmetric 32-value scale: d = max|x| / div.
fn absMax(comptime div: []const u8, comptime zero_d: []const u8, comptime inv_test: []const u8) []const u8 {
    return fill("float max_abs = 0.0f; for (uint i = 0u; i < 32u; ++i) max_abs = fmax(max_abs, fabs(input[input_base + i] * p.scale)); float d = max_abs == 0.0f ? ${z} : max_abs / ${div}; float inv = ${t} == 0.0f ? 0.0f : 1.0f / d; ", .{ .div = div, .z = zero_d, .t = inv_test });
}

/// Affine 32-value scale: d = (max - min) / div, m = min.
fn minMax(comptime div: []const u8) []const u8 {
    return fill("float min_v = input[input_base] * p.scale; float max_v = min_v; for (uint i = 1u; i < 32u; ++i) { float v = input[input_base + i] * p.scale; min_v = fmin(min_v, v); max_v = fmax(max_v, v); } float span = max_v - min_v; float d = span == 0.0f ? 1.0f : span / ${div}; float inv = span == 0.0f ? 0.0f : 1.0f / d; ", .{ .div = div });
}

/// The 16 byte pairs of a 32-value block: value i in the low nibble, i + 16
/// in the high one. `q` maps an input expression to the rounded code.
fn nibbles(comptime q: []const u8, comptime max: []const u8, comptime at: u32, comptime pack: []const u8) []const u8 {
    return fill("for (uint i = 0u; i < 16u; ++i) { uint lo = uint(clamp(round(${lo}), 0.0f, ${max})); uint hi = uint(clamp(round(${hi}), 0.0f, ${max})); weight[off + ${at}u + i] = ${pack}; }", .{
        .lo = fill(q, .{ .x = "input[input_base + i]" }),
        .hi = fill(q, .{ .x = "input[input_base + 16u + i]" }),
        .max = max,
        .at = at,
        .pack = pack,
    });
}

const q4_pack = "uchar(lo | (hi << 4))";
const q5_pack = "uchar((lo & 0x0fu) | ((hi & 0x0fu) << 4)); qh |= ((lo >> 4) & 1u) << i; qh |= ((hi >> 4) & 1u) << (i + 16u)";
const sym_q = "${x} * p.scale * inv + ${bias}";
const affine_q = "(${x} * p.scale - min_v) * inv";

fn q8(comptime at: u32, comptime extra: []const u8) []const u8 {
    return fill("for (uint i = 0u; i < 32u; ++i) { float scaled = input[input_base + i] * p.scale; int q = int(round(clamp(scaled * inv, -127.0f, 127.0f))); ${e}weight[off + ${at}u + i] = as_type<uchar>(char(q)); }", .{ .at = at, .e = extra });
}

/// Q4_K / Q5_K: per-32 scales and mins, 6-bit packed against d / dmin.
fn kScaleMins(comptime bytes: u32, comptime div: []const u8) []const u8 {
    return off(bytes) ++ fill("for (uint i = 0u; i < ${b}u; ++i) weight[off + i] = 0u; float sub_scales[8]; float sub_mins[8]; float max_scale = 0.0f; float max_min = 0.0f; for (uint sub = 0u; sub < 8u; ++sub) { float min_v = input[input_base + sub * 32u] * p.scale; float max_v = min_v; for (uint i = 1u; i < 32u; ++i) { float v = input[input_base + sub * 32u + i] * p.scale; min_v = fmin(min_v, v); max_v = fmax(max_v, v); } float min_mag = fmax(-min_v, 0.0f); float sc = (max_v + min_mag) == 0.0f ? 1.0f : (max_v + min_mag) / ${div}; sub_scales[sub] = sc; sub_mins[sub] = min_mag; max_scale = fmax(max_scale, sc); max_min = fmax(max_min, min_mag); } float d = max_scale == 0.0f ? 1.0f : max_scale / 63.0f; float dmin = max_min == 0.0f ? 1.0f : max_min / 63.0f; ", .{ .b = bytes, .div = div }) ++
        halfBits("d_bits", "d") ++ halfBits("dmin_bits", "dmin") ++ storeBytes("d_bits", 0, 2) ++ " " ++ storeBytes("dmin_bits", 2, 2) ++
        " uint scs[8]; uint mins[8]; for (uint sub = 0u; sub < 8u; ++sub) { scs[sub] = max_scale == 0.0f ? 0u : uint(clamp(round(sub_scales[sub] / d), 0.0f, 63.0f)); mins[sub] = max_min == 0.0f ? 0u : uint(clamp(round(sub_mins[sub] / dmin), 0.0f, 63.0f)); } pack_q4k_scale_mins(weight + off + 4u, scs, mins); ";
}

const formats = [_]Format{
    .{ .name = "q4_0", .qk = 32, .bytes = 18, .read = .scaled, .quantize = absMax("8.0f", "1.0f", "max_abs") ++ off(18) ++ halfBits("bits", "d") ++ storeBytes("bits", 0, 2) ++ " " ++
        nibbles(fill(sym_q, .{ .x = "${x}", .bias = "8.0f" }), "15.0f", 2, q4_pack) },
    .{ .name = "q4_1", .qk = 32, .bytes = 20, .read = .plain, .quantize = minMax("15.0f") ++ off(20) ++ halfBits("d_bits", "d") ++ halfBits("m_bits", "min_v") ++ storeBytes("d_bits", 0, 2) ++ " " ++ storeBytes("m_bits", 2, 2) ++ " " ++
        nibbles(affine_q, "15.0f", 4, q4_pack) },
    .{ .name = "q5_0", .qk = 32, .bytes = 22, .read = .scaled, .quantize = absMax("16.0f", "1.0f", "max_abs") ++ off(22) ++ halfBits("bits", "d") ++ storeBytes("bits", 0, 2) ++
        " uint qh = 0u; " ++ nibbles(fill(sym_q, .{ .x = "${x}", .bias = "16.0f" }), "31.0f", 6, q5_pack) ++ " " ++ storeBytes("qh", 2, 4) },
    .{ .name = "q5_1", .qk = 32, .bytes = 24, .read = .plain, .quantize = minMax("31.0f") ++ off(24) ++ halfBits("d_bits", "d") ++ halfBits("m_bits", "min_v") ++ storeBytes("d_bits", 0, 2) ++ " " ++ storeBytes("m_bits", 2, 2) ++
        " uint qh = 0u; " ++ nibbles(affine_q, "31.0f", 8, q5_pack) ++ " " ++ storeBytes("qh", 4, 4) },
    .{ .name = "q4_k", .qk = 256, .bytes = 144, .read = .plain, .quantize = kScaleMins(144, "15.0f") ++
        "for (uint chunk = 0u; chunk < 4u; ++chunk) { uint sub0 = chunk * 2u; uint sub1 = sub0 + 1u; float inv0 = sub_scales[sub0] == 0.0f ? 0.0f : 1.0f / sub_scales[sub0]; float inv1 = sub_scales[sub1] == 0.0f ? 0.0f : 1.0f / sub_scales[sub1]; uint q_off = off + 16u + chunk * 32u; for (uint i = 0u; i < 32u; ++i) { uint q0 = uint(clamp(round((input[input_base + sub0 * 32u + i] * p.scale + sub_mins[sub0]) * inv0), 0.0f, 15.0f)); uint q1 = uint(clamp(round((input[input_base + sub1 * 32u + i] * p.scale + sub_mins[sub1]) * inv1), 0.0f, 15.0f)); weight[q_off + i] = uchar(q0 | (q1 << 4)); } }" },
    .{ .name = "q5_k", .qk = 256, .bytes = 176, .read = .plain, .quantize = kScaleMins(176, "31.0f") ++
        "for (uint sub = 0u; sub < 8u; ++sub) { uint chunk = sub / 2u; bool high = (sub & 1u) != 0u; uint ql_off = off + 48u + chunk * 32u; float inv = sub_scales[sub] == 0.0f ? 0.0f : 1.0f / sub_scales[sub]; for (uint i = 0u; i < 32u; ++i) { uint q = uint(clamp(round((input[input_base + sub * 32u + i] * p.scale + sub_mins[sub]) * inv), 0.0f, 31.0f)); if (high) weight[ql_off + i] |= uchar((q & 0x0fu) << 4); else weight[ql_off + i] |= uchar(q & 0x0fu); weight[off + 16u + i] |= uchar(((q >> 4) & 1u) << sub); } }" },
    .{ .name = "q6_k", .qk = 256, .bytes = 210, .read = .plain, .quantize = off(210) ++
        "for (uint i = 0u; i < 210u; ++i) weight[off + i] = 0u; float sub_scales[16]; float max_scale = 0.0f; for (uint sub = 0u; sub < 16u; ++sub) { float sub_max = 0.0f; for (uint i = 0u; i < 16u; ++i) sub_max = fmax(sub_max, fabs(input[input_base + sub * 16u + i] * p.scale)); float sc = sub_max == 0.0f ? 1.0f : sub_max / 31.0f; sub_scales[sub] = sc; max_scale = fmax(max_scale, sc); } float d = max_scale == 0.0f ? 1.0f : max_scale / 127.0f; " ++
        halfBits("d_bits", "d") ++ storeBytes("d_bits", 208, 2) ++
        " for (uint sub = 0u; sub < 16u; ++sub) { uint scaled = max_scale == 0.0f ? 0u : uint(clamp(round(sub_scales[sub] / d), 0.0f, 127.0f)); weight[off + 192u + sub] = as_type<uchar>(char(scaled)); uint half_idx = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint ql_off = half_idx * 64u + (group & 1u) * 32u; uint qh_off = half_idx * 32u; uint qh_shift = group * 2u; uint nibble_shift = (group / 2u) * 4u; float inv = sub_scales[sub] == 0.0f ? 0.0f : 1.0f / sub_scales[sub]; for (uint i = 0u; i < 16u; ++i) { float v = input[input_base + sub * 16u + i] * p.scale; uint q = uint(clamp(round(v * inv + 32.0f), 0.0f, 63.0f)); uint l = l_base + i; weight[off + ql_off + l] |= uchar((q & 0x0fu) << nibble_shift); weight[off + 128u + qh_off + l] |= uchar(((q >> 4) & 0x03u) << qh_shift); } }" },
    .{ .name = "q8_0", .qk = 32, .bytes = 34, .read = .scaled, .quantize = absMax("127.0f", "0.0f", "d") ++ off(34) ++ halfBits("bits", "d") ++ storeBytes("bits", 0, 2) ++ " " ++ q8(2, "") },
    .{ .name = "q8_1", .qk = 32, .bytes = 36, .read = .scaled_inline, .quantize = absMax("127.0f", "1.0f", "max_abs") ++ off(36) ++ halfBits("bits", "d") ++ storeBytes("bits", 0, 2) ++
        " int qsum = 0; " ++ q8(4, "qsum += q; ") ++ " " ++ halfBits("sum_bits", "d * float(qsum)") ++ storeBytes("sum_bits", 2, 2) },
};

const params = "constant termite_metal_quant_rows_params &p";

fn dequant(comptime f: Format, comptime name: []const u8, comptime index: []const u8, comptime args: []const []const u8) []const u8 {
    const value = switch (f.read) {
        .scaled => fill("float d = termite_${f}_block_scale(weight, off); output[gid] = d * termite_${f}_block_value(weight, off, inner) * p.scale;", .{ .f = f.name }),
        .plain => fill("output[gid] = termite_${f}_block_value(weight, off, inner) * p.scale;", .{ .f = f.name }),
        .scaled_inline => fill("output[gid] = termite_${f}_block_scale(weight, off) * termite_${f}_block_value(weight, off, inner) * p.scale;", .{ .f = f.name }),
    };
    return msl.signature(fill("termite_${f}_${n}", .{ .f = f.name, .n = name }), args) ++ fill(
        \\
        \\    uint total_values = p.total * p.dim; if (gid >= total_values) return; uint row = gid / p.dim; uint col = gid % p.dim; uint source_row = ${index}; if (source_row >= p.source_rows || (p.dim & ${mask}u) != 0u) { output[gid] = 0.0f; return; } uint row_blocks = p.dim / ${qk}u; uint block = col / ${qk}u; uint inner = col - block * ${qk}u; uint off = (source_row * row_blocks + block) * ${bytes}u; ${value}
        \\}
        \\
    , .{ .index = index, .mask = f.qk - 1, .qk = f.qk, .bytes = f.bytes, .value = value });
}

fn quant(comptime f: Format, comptime name: []const u8, comptime index: []const u8, comptime args: []const []const u8) []const u8 {
    return msl.signature(fill("termite_${f}_${n}", .{ .f = f.name, .n = name }), args) ++ fill(
        \\
        \\    if ((p.dim & ${mask}u) != 0u) return; uint row_blocks = p.dim / ${qk}u; uint total_blocks = p.total * row_blocks; if (gid >= total_blocks) return; uint row = gid / row_blocks; uint block = gid - row * row_blocks; uint target_row = ${index}; if (target_row >= p.source_rows) return; uint input_base = row * p.dim + block * ${qk}u; ${body}
        \\}
        \\
    , .{ .index = index, .mask = f.qk - 1, .qk = f.qk, .body = f.quantize });
}

pub fn items() []const msl.Item {
    comptime {
        var out: []const msl.Item = &.{};
        for (formats) |f| out = out ++ .{
            msl.msl(dequant(f, "get_rows", "ids[row]", &.{ "device const uchar *weight", "device const uint *ids", "device float *output", params, "gid" })),
            msl.msl(quant(f, "set_rows", "ids[row]", &.{ "device const float *input", "device const uint *ids", "device uchar *weight", params, "gid" })),
            msl.msl(dequant(f, "cpy_q_to_f32", "p.row_offset + row", &.{ "device const uchar *weight", "device float *output", params, "gid" })),
            msl.msl(quant(f, "cpy_f32_to_q", "p.row_offset + row", &.{ "device const float *input", "device uchar *weight", params, "gid" })),
        };
        return out;
    }
}
