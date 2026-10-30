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

//! Reference quantized matvec kernels, `termite_<fmt>_linear` (one thread
//! per output, a serial walk over the row's blocks) and the simdgroup
//! `termite_<fmt>_linear_1x_reduce` (NR0 output rows per threadgroup, NSG
//! simdgroups striding blocks, eight values per lane, then a simd_sum and a
//! threadgroup reduction over simdgroups).
//!
//! A format is its block header (scale reads, written with `${u}` for the
//! index literal suffix, which differs between the two schedules) and its
//! per-block dot product for each schedule.

const msl = @import("quant_metal_msl.zig");
const wrap = @import("quant_metal_wrap.zig");
const fill = msl.fill;
const join = msl.join;

/// `ushort bits = (ushort(weight[off + at + 1]) << 8) | ushort(weight[off + at]);`
fn h16(comptime bits: []const u8, comptime at: u32) []const u8 {
    return fill("ushort ${b} = (ushort(weight[off + ${hi}$${u}]) << 8) | ushort(weight[${lo}]);", .{
        .b = bits,
        .hi = at + 1,
        .lo = if (at == 0) "off" else fill("off + ${a}$${u}", .{ .a = at }),
    });
}

fn half16(comptime d: []const u8, comptime bits: []const u8) []const u8 {
    return fill("float ${d} = float(as_type<half>(${b}));", .{ .d = d, .b = bits });
}

/// Little-endian u32 of the high bits of Q5 blocks at `at`.
fn qh32(comptime at: u32) []const u8 {
    return fill("uint qh = uint(weight[off + ${a}$${u}]) | (uint(weight[off + ${b}$${u}]) << 8) | (uint(weight[off + ${c}$${u}]) << 16) | (uint(weight[off + ${e}$${u}]) << 24);", .{ .a = at, .b = at + 1, .c = at + 2, .e = at + 3 });
}

fn qs(comptime at: u32) []const u8 {
    return fill("const device uchar *qs = weight + off + ${a}u;", .{ .a = at });
}

/// Scale d.
const head_d = h16("bits", 0) ++ " " ++ half16("d", "bits");
/// Scale d and min m.
const head_dm = h16("d_bits", 0) ++ " " ++ h16("m_bits", 2) ++ " " ++ half16("d", "d_bits") ++ " " ++ half16("m", "m_bits");
/// Scale d and min dmin (K-quants).
const head_dmin = h16("d_bits", 0) ++ " " ++ h16("dmin_bits", 2) ++ " " ++ half16("d", "d_bits") ++ " " ++ half16("dmin", "dmin_bits");
/// MXFP4 E8M0 scale byte.
const head_e8m0 = "uint scale_byte = uint(weight[off]); uint scale_bits = (scale_byte < 2u) ? (0x00200000u << scale_byte) : (uint(scale_byte - 1u) << 23); float d = as_type<float>(scale_bits);";

const iq4_lut = "[16] = {-127, -104, -83, -65, -49, -35, -22, -10, 1, 13, 25, 38, 53, 69, 89, 113};";
const fp4_lut = "[16] = {0, 1, 2, 3, 4, 6, 8, 12, 0, -1, -2, -3, -4, -6, -8, -12};";

/// The scalar walk over the two 16-value halves of a 32-value block.
fn q4Halves(comptime bias: []const u8, comptime v: []const u8) []const u8 {
    return join(&.{
        fill("for (uint i = 0; i < 16; ++i) { uchar packed = qs[i]; float lo = float(int(packed & 0x0Fu)${b}); acc += input[in_off + i] * (${v}); } ", .{ .b = bias, .v = fill(v, .{ .h = "lo" }) }),
        fill("for (uint i = 0; i < 16; ++i) { uchar packed = qs[i]; float hi = float(int((packed >> 4) & 0x0Fu)${b}); acc += input[in_off + 16u + i] * (${v}); }", .{ .b = bias, .v = fill(v, .{ .h = "hi" }) }),
    });
}

fn q5Halves(comptime v: []const u8) []const u8 {
    return join(&.{
        fill("for (uint i = 0; i < 16; ++i) { int lo4 = int(qs[i] & 0x0Fu); int hi1 = int((qh >> i) & 1u); acc += input[in_off + i] * (${v}); } ", .{ .v = v }),
        fill("for (uint i = 0; i < 16; ++i) { int lo4 = int(qs[i] >> 4); int hi1 = int((qh >> (i + 16u)) & 1u); acc += input[in_off + 16u + i] * (${v}); }", .{ .v = v }),
    });
}

/// The 1x_reduce lane dot over eight values j = qj0 + i of a 32-value
/// block whose code for j comes from `code` (packed = qs[j & 15]).
fn nibbleDot(comptime code: []const u8, comptime v: []const u8) []const u8 {
    return fill("uint j = qj0 + i; uchar packed = qs[j & 15u]; ${c} block += x[i] * ${v};", .{ .c = code, .v = v });
}

const in_off32 = "uint in_off = r * p.in_dim + b * 32u; ";

/// A 32-value block format with both schedules.
const Block32 = struct {
    name: []const u8,
    bytes: u32,
    /// Lookup table: `constexpr int <lut>`; on its own line in the scalar
    /// kernel when `lut_line`.
    lut: []const u8 = "",
    lut_line: bool = false,
    head: []const u8,
    /// Scalar kernel, after the head.
    scalar: []const u8,
    /// 1x_reduce: after the head, before `float block`.
    reduce_head: []const u8 = "",
    reduce_dot: []const u8,
    /// 1x_reduce accumulates `d * block` (else the dot already scaled it).
    scale_block: bool,
    nr: u32 = 2,
    nsg: u32 = 4,
};

const block32 = [_]Block32{
    .{ .name = "q4_1", .bytes = 20, .head = head_dm ++ " " ++ qs(4), .scalar = in_off32 ++ q4Halves("", "d * ${h} + m"), .reduce_dot = nibbleDot("float q = j < 16u ? float(int(packed & 0x0Fu)) : float(int((packed >> 4) & 0x0Fu));", "(d * q + m)"), .scale_block = false, .nr = 4, .nsg = 2 },
    .{ .name = "q5_0", .bytes = 22, .head = head_d ++ " " ++ qh32(2) ++ " " ++ qs(6), .scalar = in_off32 ++ q5Halves("d * float((lo4 | (hi1 << 4)) - 16)"), .reduce_dot = nibbleDot("int lo4 = j < 16u ? int(packed & 0x0Fu) : int(packed >> 4); int hi1 = int((qh >> j) & 1u);", "float((lo4 | (hi1 << 4)) - 16)"), .scale_block = true },
    .{ .name = "q5_1", .bytes = 24, .head = head_dm ++ " " ++ qh32(4) ++ " " ++ qs(8), .scalar = in_off32 ++ q5Halves("d * float(lo4 | (hi1 << 4)) + m"), .reduce_dot = nibbleDot("int lo4 = j < 16u ? int(packed & 0x0Fu) : int(packed >> 4); int hi1 = int((qh >> j) & 1u);", "(d * float(lo4 | (hi1 << 4)) + m)"), .scale_block = false },
    .{ .name = "q8_1", .bytes = 36, .head = head_d, .scalar = "float block_acc = 0.0f; " ++ in_off32 ++ "for (uint i = 0; i < 32; ++i) { char q = as_type<char>(weight[off + 4u + i]); block_acc += input[in_off + i] * float(q); } acc += d * block_acc;", .reduce_dot = "char q = as_type<char>(weight[off + 4u + qj0 + i]); block += x[i] * float(q);", .scale_block = true },
    .{ .name = "iq4_nl", .bytes = 18, .lut = "iq4_values" ++ iq4_lut, .head = head_d ++ " " ++ qs(2), .scalar = in_off32 ++ "for (uint i = 0; i < 16u; ++i) { uchar packed = qs[i]; acc += input[in_off + i] * (d * float(iq4_values[packed & 0x0Fu])); } for (uint i = 0; i < 16u; ++i) { uchar packed = qs[i]; acc += input[in_off + 16u + i] * (d * float(iq4_values[(packed >> 4) & 0x0Fu])); }", .reduce_dot = nibbleDot(code_lut, "float(iq4_values[code])"), .scale_block = true },
    .{ .name = "mxfp4", .bytes = 17, .lut = "mxfp4_lut" ++ fp4_lut, .lut_line = true, .head = head_e8m0, .scalar = "uint input_off = r * p.in_dim + b * 32u; " ++ qs(1) ++ " " ++ fp4Pairs("mxfp4_lut", "input_off", "16u", "d"), .reduce_head = " " ++ qs(1), .reduce_dot = nibbleDot(code_lut, "float(mxfp4_lut[code])"), .scale_block = true },
};

const code_lut = "uint code = j < 16u ? uint(packed & 0x0Fu) : uint((packed >> 4) & 0x0Fu);";

/// Low / high nibble codes through a LUT, the high one `hi` values later.
fn fp4Pairs(comptime lut: []const u8, comptime base: []const u8, comptime hi: []const u8, comptime d: []const u8) []const u8 {
    return fill("for (uint j = 0; j < 16u; ++j) { uchar byte = qs[j]; int lo = ${l}[byte & 0x0Fu]; int hi = ${l}[byte >> 4]; acc += input[${b} + j] * (${d} * float(lo)); acc += input[${b} + ${h} + j] * (${d} * float(hi)); }", .{ .l = lut, .b = base, .h = hi, .d = d });
}

/// Scalar kernels of other block shapes: (format, block bytes, body after
/// `uint off = ...;`, decl after `float acc`, decl line).
const Scalar = struct { name: []const u8, bytes: u32, body: []const u8, decl: []const u8 = "", decl_line: []const u8 = "" };

const scalar_other = [_]Scalar{
    .{ .name = "q4_0", .bytes = 18, .body = fill(head_d, .{ .u = "" }) ++ " " ++ qs(2) ++ " " ++ in_off32 ++ q4Halves(" - 8", "d * ${h}") },
    .{ .name = "q1_0", .bytes = 18, .body = fill(head_d, .{ .u = "" }) ++ " float signed_sum = 0.0f; uint in_off = r * p.in_dim + b * 128u; for (uint by = 0; by < 16u; ++by) { uchar sbits = weight[off + 2u + by]; uint base = in_off + by * 8u;" ++ signBits() ++ " } acc += d * signed_sum;" },
    .{ .name = "q8_k", .bytes = 292, .body = "uint bits = uint(weight[off]) | (uint(weight[off + 1]) << 8) | (uint(weight[off + 2]) << 16) | (uint(weight[off + 3]) << 24); float d = as_type<float>(bits); float block_acc = 0.0f; uint in_off = r * p.in_dim + b * 256u; for (uint i = 0; i < 256; ++i) { char q = as_type<char>(weight[off + 4u + i]); block_acc += input[in_off + i] * float(q); } acc += d * block_acc;" },
    .{ .name = "q2_k", .bytes = 84, .body = "const device uchar *scales_raw = weight + off; " ++ fill(h16("d_bits", 16) ++ " " ++ h16("dmin_bits", 18) ++ " " ++ half16("d", "d_bits") ++ " " ++ half16("dmin", "dmin_bits"), .{ .u = "" }) ++ " const device uchar *qs = weight + off + 20u; for (uint sub = 0; sub < 16; ++sub) { float sc = float(scales_raw[sub] & 0x0Fu); float m = float(scales_raw[sub] >> 4); float dsc = d * sc; float dmn = dmin * m; uint chunk = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint q_base = chunk * 32u; uint shift = group * 2u; uint input_off = r * p.in_dim + b * 256u + sub * 16u; for (uint i = 0; i < 16; ++i) { int q = int((qs[q_base + l_base + i] >> shift) & 0x03u); acc += input[input_off + i] * (dsc * float(q) - dmn); } }" },
    .{ .name = "q3_k", .bytes = 110, .body = "const device uchar *hmask = weight + off; const device uchar *qs = weight + off + 32u; const device uchar *scale_data = weight + off + 96u; " ++ fill(h16("d_bits", 108) ++ " " ++ half16("d", "d_bits"), .{ .u = "" }) ++ " int raw_scales[16]; for (uint i = 0; i < 4; ++i) { raw_scales[i] = int(scale_data[i] & 0x0Fu) | (int(scale_data[8 + i] & 0x03u) << 4); raw_scales[i + 4] = int(scale_data[4 + i] & 0x0Fu) | (int((scale_data[8 + i] >> 2) & 0x03u) << 4); raw_scales[i + 8] = int((scale_data[i] >> 4) & 0x0Fu) | (int((scale_data[8 + i] >> 4) & 0x03u) << 4); raw_scales[i + 12] = int((scale_data[4 + i] >> 4) & 0x0Fu) | (int((scale_data[8 + i] >> 6) & 0x03u) << 4); } for (uint sub = 0; sub < 16; ++sub) { float scale = d * float(raw_scales[sub] - 32); uint chunk = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint q_base = chunk * 32u; uint shift = group * 2u; uint hm_bit = chunk * 4u + group; uint input_off = r * p.in_dim + b * 256u + sub * 16u; for (uint i = 0; i < 16; ++i) { uint l = l_base + i; int low2 = int((qs[q_base + l] >> shift) & 0x03u); int high1 = int((hmask[l] >> hm_bit) & 0x01u); int q = low2 + high1 * 4 - 4; acc += input[input_off + i] * (scale * float(q)); } }" },
    .{ .name = "q4_k", .bytes = 144, .body = fill(head_dmin, .{ .u = "" }) ++ " const device uchar *scales = weight + off + 4u; const device uchar *qs = weight + off + 16u; thread float scs[8]; thread float mins[8]; unpack_q4k_scale_mins(scales, scs, mins); uint q_off = 0u; for (uint chunk = 0; chunk < 4; ++chunk) { uint sub = chunk * 2u; float dsc0 = d * scs[sub]; float dmn0 = dmin * mins[sub]; float dsc1 = d * scs[sub + 1u]; float dmn1 = dmin * mins[sub + 1u]; uint input_off = r * p.in_dim + b * 256u + chunk * 64u; for (uint i = 0; i < 32; ++i) { acc += input[input_off + i] * (dsc0 * float(qs[q_off + i] & 0x0Fu) - dmn0); } for (uint i = 0; i < 32; ++i) { acc += input[input_off + 32u + i] * (dsc1 * float(qs[q_off + i] >> 4) - dmn1); } q_off += 32u; }" },
    .{ .name = "q5_k", .bytes = 176, .body = fill(head_dmin, .{ .u = "" }) ++ " const device uchar *scales = weight + off + 4u; const device uchar *qh = weight + off + 16u; const device uchar *ql = weight + off + 48u; thread float scs[8]; thread float mins[8]; unpack_q4k_scale_mins(scales, scs, mins); for (uint sub = 0; sub < 8; ++sub) { float dsc = d * scs[sub]; float dmn = dmin * mins[sub]; uint chunk = sub / 2u; bool is_high = (sub & 1u) != 0u; uint ql_off = chunk * 32u; uint hb_shift = sub; uint input_off = r * p.in_dim + b * 256u + sub * 32u; for (uint i = 0; i < 32; ++i) { uchar low = is_high ? (ql[ql_off + i] >> 4) : (ql[ql_off + i] & 0x0Fu); int q = int(low) + int((qh[i] >> hb_shift) & 1u) * 16; acc += input[input_off + i] * (dsc * float(q) - dmn); } }" },
    .{ .name = "q6_k", .bytes = 210, .body = "const device uchar *ql = weight + off; const device uchar *qh = weight + off + 128u; const device char *scales = reinterpret_cast<const device char *>(weight + off + 192u); " ++ fill(h16("d_bits", 208) ++ " " ++ half16("d", "d_bits"), .{ .u = "" }) ++ " for (uint sub = 0; sub < 16; ++sub) { float scale = d * float(scales[sub]); uint half_idx = sub / 8u; uint group = (sub % 8u) / 2u; uint l_base = (sub % 2u) * 16u; uint ql_off = half_idx * 64u + (group & 1u) * 32u; uint qh_off = half_idx * 32u; uint qh_shift = group * 2u; uint nibble_shift = (group / 2u) * 4u; uint input_off = r * p.in_dim + b * 256u + sub * 16u; for (uint i = 0; i < 16; ++i) { uint l = l_base + i; int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0Fu); int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u); int q = (low4 | (high2 << 4)) - 32; acc += input[input_off + i] * (scale * float(q)); } }" },
    .{ .name = "iq4_xs", .bytes = 136, .decl_line = "constexpr int iq4_nl" ++ iq4_lut, .body = fill(h16("d_bits", 0) ++ " " ++ half16("d", "d_bits") ++ " " ++ h16("scales_h", 2), .{ .u = "" }) ++ " const device uchar *scales_l = weight + off + 4u; const device uchar *qs = weight + off + 8u; for (uint sub = 0; sub < 8; ++sub) { uint low = (uint(scales_l[sub >> 1u]) >> ((sub & 1u) * 4u)) & 0x0Fu; uint high = (uint(scales_h) >> (sub * 2u)) & 0x03u; int ls = int(low | (high << 4)); float dl = d * float(ls - 32); uint q_base = sub * 16u; uint input_off = r * p.in_dim + b * 256u + sub * 32u; for (uint j = 0; j < 16u; ++j) { uchar byte = qs[q_base + j]; int lo = iq4_nl[byte & 0x0Fu]; int hi = iq4_nl[byte >> 4]; acc += input[input_off + j] * (dl * float(lo)); acc += input[input_off + 16u + j] * (dl * float(hi)); } }" },
    .{ .name = "nvfp4", .bytes = 36, .decl = " constexpr int lut" ++ fp4_lut, .body = "const device uchar *scales = weight + off; const device uchar *qs = weight + off + 4u; uint input_off = r * p.in_dim + b * 64u; for (uint sub = 0; sub < 4u; ++sub) { float d = termite_nvfp4_scale(scales[sub]); uint q_base = sub * 8u; uint x_base = input_off + sub * 16u; for (uint j = 0; j < 8u; ++j) { uchar packed = qs[q_base + j]; acc += input[x_base + j] * (d * float(lut[packed & 0x0Fu])); acc += input[x_base + 8u + j] * (d * float(lut[packed >> 4])); } }" },
    .{ .name = "iq2_xs", .bytes = 74, .body = fill(h16("d_bits", 0) ++ " " ++ half16("d", "d_bits"), .{ .u = "u" }) ++ " const device uchar *qs = weight + off + 2u; const device uchar *scales = weight + off + 66u; uint input_off = r * p.in_dim + b * 256u; for (uint ib32 = 0u; ib32 < 8u; ++ib32) { float db0 = d * (0.5f + float(scales[ib32] & 0x0Fu)) * 0.25f; float db1 = d * (0.5f + float(scales[ib32] >> 4)) * 0.25f; for (uint lane = 0u; lane < 4u; ++lane) { uint qoff = (4u * ib32 + lane) * 2u; ushort packed = ushort(qs[qoff]) | (ushort(qs[qoff + 1u]) << 8); ushort grid = termite_iq2_xs_grid[packed & 0x01FFu]; uchar signs = termite_iq2_sign_mask(uint(packed >> 9)); float dl = lane < 2u ? db0 : db1; uint base = input_off + ib32 * 32u + lane * 8u; for (uint j = 0u; j < 8u; ++j) { float raw = float(2u * ((uint(grid) >> (2u * j)) & 0x03u) + 1u); float sign = ((uint(signs) >> j) & 1u) != 0u ? -1.0f : 1.0f; acc += input[base + j] * (dl * raw * sign); } } }" },
    .{ .name = "q8_0", .bytes = 34, .body = "float d = termite_q8_0_block_scale(weight, off); float block_acc = 0.0f; uint in_off = r * p.in_dim + b * 32u; for (uint i = 0; i < 32; ++i) { block_acc += input[in_off + i] * termite_q8_0_block_value(weight, off, i); } acc += d * block_acc;" },
};

/// Q1_0: one sign bit per value, `+x` or `-x`.
fn signBits() []const u8 {
    var parts: []const []const u8 = &.{};
    for (0..8) |k| {
        const x = if (k == 0) "input[base]" else fill("input[base + ${k}u]", .{ .k = k });
        parts = parts ++ .{fill(" signed_sum += (sbits & 0x${m}u) != 0u ? ${x} : -${x};", .{ .m = hex2(1 << k), .x = x })};
    }
    return join(parts);
}

fn hex2(comptime v: u32) []const u8 {
    const digits = "0123456789ABCDEF";
    return &.{ digits[v >> 4], digits[v & 15] };
}

const gid_abi = wrap.linear ++ .{"gid"};

/// `termite_<fmt>_linear`: one thread per (row, output).
fn scalarKernel(comptime name: []const u8, comptime row_offset: []const u8, comptime decl: []const u8, comptime decl_line: []const u8, comptime prologue: []const u8, comptime loop: []const u8) []const u8 {
    return msl.signature(fill("termite_${n}_linear", .{ .n = name }), &gid_abi) ++ fill(
        \\
        \\${pro}    uint total = p.rows * p.out_dim; if (gid >= total) return; uint r = gid / p.out_dim; uint o = gid % p.out_dim; uint row_offset = ${ro}; float acc = 0.0f;${decl}
        \\${line}    for (uint b = 0; b < p.row_blocks; ++b) { ${loop} }
        \\    output[gid] = acc;
        \\}
        \\
    , .{ .pro = prologue, .ro = row_offset, .decl = decl, .line = if (decl_line.len == 0) "" else "    " ++ decl_line ++ "\n", .loop = loop });
}

fn blockLoop(comptime bytes: u32, comptime body: []const u8) []const u8 {
    return fill("uint off = row_offset + b * ${b}u; ${body}", .{ .b = bytes, .body = body });
}

fn rowOffset(comptime bytes: u32) []const u8 {
    return fill("o * p.row_blocks * ${b}u", .{ .b = bytes });
}

pub fn scalar(comptime name: []const u8) msl.Item {
    for (block32) |f| if (eql(f.name, name)) return msl.msl(scalarKernel(name, rowOffset(f.bytes), if (f.lut_line or f.lut.len == 0) "" else " constexpr int " ++ f.lut, if (f.lut_line) "constexpr int " ++ f.lut else "", "", blockLoop(f.bytes, fill(f.head, .{ .u = "" }) ++ " " ++ f.scalar)));
    for (scalar_other) |f| if (eql(f.name, name)) return msl.msl(scalarKernel(name, rowOffset(f.bytes), f.decl, f.decl_line, if (eql(name, "q8_0")) "    weight += p.weight_offset;\n" else "", blockLoop(f.bytes, f.body)));
    if (eql(name, "i8_s")) return msl.msl(scalarKernel(name, "o * p.row_blocks", "", "", "", "char q = as_type<char>(weight[row_offset + b]); acc += input[r * p.in_dim + b] * float(q);"));
    @compileError("no scalar linear for " ++ name);
}

fn eql(comptime a: []const u8, comptime b: []const u8) bool {
    return @import("std").mem.eql(u8, a, b);
}

/// `if (row == 0u) acc0 += x; else if ... else acc<n-1> += x;`
fn accumulate(comptime nr: u32, comptime x: []const u8) []const u8 {
    var parts: []const []const u8 = &.{};
    for (0..nr) |k| parts = parts ++ .{if (k == 0)
        fill("if (row == 0u) acc0 += ${x};", .{ .x = x })
    else if (k + 1 < nr)
        fill(" else if (row == ${k}u) acc${k} += ${x};", .{ .k = k, .x = x })
    else
        fill(" else acc${k} += ${x};", .{ .k = k, .x = x })};
    return join(parts);
}

fn each(comptime nr: u32, comptime t: []const u8, comptime sep: []const u8) []const u8 {
    var parts: []const []const u8 = &.{};
    for (0..nr) |k| {
        if (k != 0) parts = parts ++ .{sep};
        parts = parts ++ .{fill(t, .{
            .k = k,
            .sh = if (k == 0) "" else fill("${o}u + ", .{ .o = 32 * k }),
            .o = if (k == 0) "first_o" else fill("first_o + ${k}u", .{ .k = k }),
            .idx = if (k == 0) "idx" else fill("idx + ${k}u", .{ .k = k }),
        })};
    }
    return join(parts);
}

/// Simd sums of acc0..acc<nr-1>, then a threadgroup sum over simdgroups
/// through shmem rows of 32, then lane 0 of simdgroup 0 stores.
pub fn reduceTail(comptime nr: u32) []const u8 {
    return fill(
        \\    if (sgitg == 0u) { ${zero} } ${simd} threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (lane == 0u) { ${put} } threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    ${total} if (lane == 0u && sgitg == 0u) { uint idx = r * p.out_dim + first_o; ${store} }
        \\}
        \\
    , .{
        .zero = each(nr, "shmem[${sh}lane] = 0.0f;", " "),
        .simd = each(nr, "acc${k} = simd_sum(acc${k});", " "),
        .put = each(nr, "shmem[${sh}sgitg] = acc${k};", " "),
        .total = each(nr, "float total${k} = simd_sum(shmem[${sh}lane]);", " "),
        .store = each(nr, "if (${o} < p.out_dim) output[${idx}] = total${k};", " "),
    });
}

const reduce_abi = wrap.linear ++ .{"threadgroup float *shmem"} ++ wrap.sg;

/// A 1x_reduce kernel with unrolled accumulators acc0..acc<nr-1>: `setup`
/// assigns lanes to blocks, `extra` follows the input row, `loop`
/// accumulates.
fn reduceKernel(comptime name: []const u8, comptime nr: u32, comptime nsg: u32, comptime nq: bool, comptime lut: []const u8, comptime setup: []const u8, comptime extra: []const u8, comptime loop: []const u8) msl.Item {
    return msl.msl(msl.signature(fill("termite_${n}_linear_1x_reduce", .{ .n = name }), &reduce_abi) ++ fill(
        \\
        \\    const uint NR0 = ${nr}u; const uint NSG = ${nsg}u;${nq} const uint NW = 32u;${lut} uint first_o = tg.x * NR0; uint r = tg.y; if (r >= p.rows || lane >= NW) return;
        \\    ${setup} uint in_row = r * p.in_dim;${extra} ${accs}
        \\    ${loop}
        \\
    , .{
        .nr = nr,
        .nsg = nsg,
        .nq = if (nq) " const uint NQ = 8u;" else "",
        .lut = if (lut.len == 0) "" else " constexpr int " ++ lut,
        .setup = setup,
        .extra = extra,
        .accs = each(nr, "float acc${k} = 0.0f;", " "),
        .loop = loop,
    }) ++ reduceTail(nr));
}

/// Eight values per lane, four lanes per 32-value block.
const lanes8 = "uint ix = lane / (NW / NQ); uint il = lane - ix * (NW / NQ);";

/// `termite_<fmt>_linear_1x_reduce`.
pub fn reduce(comptime name: []const u8) msl.Item {
    for (block32) |f| if (eql(f.name, name)) return reduceKernel(name, f.nr, f.nsg, true, f.lut, lanes8 ++ " uint ib0 = uint(sgitg) * NQ + ix;", "", fill(
        "for (uint b = ib0; b < p.row_blocks; b += NSG * NQ) { float x[8]; uint qj0 = il * NQ; uint in_off = in_row + b * 32u + qj0; for (uint i = 0u; i < NQ; ++i) { x[i] = input[in_off + i]; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * ${b}u + b * ${b}u; ${head}${rh} float block = 0.0f; for (uint i = 0u; i < NQ; ++i) { ${dot} } ${acc} } }",
        .{
            .b = f.bytes,
            .head = fill(f.head, .{ .u = "u" }),
            .rh = f.reduce_head,
            .dot = f.reduce_dot,
            .acc = accumulate(f.nr, if (f.scale_block) "d * block" else "block"),
        },
    ));
    if (eql(name, "iq4_xs")) return reduceKernel(name, 2, 4, true, "iq4_nl" ++ iq4_lut, lanes8 ++ " uint sb0 = uint(sgitg) * 8u + ix;", " uint total_subblocks = p.row_blocks * 8u;", "for (uint sb = sb0; sb < total_subblocks; sb += NSG * 8u) { uint b = sb >> 3; uint sub = sb & 7u; uint qj0 = il * NQ; uint in_off = in_row + b * 256u + sub * 32u + qj0; float x[8]; for (uint i = 0u; i < NQ; ++i) { x[i] = input[in_off + i]; } for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 136u + b * 136u; " ++
        fill(h16("d_bits", 0) ++ " " ++ half16("d", "d_bits") ++ " " ++ h16("scales_h", 2), .{ .u = "u" }) ++
        " const device uchar *scales_l = weight + off + 4u; const device uchar *qs = weight + off + 8u; uint low = (uint(scales_l[sub >> 1u]) >> ((sub & 1u) * 4u)) & 0x0Fu; uint high = (uint(scales_h) >> (sub * 2u)) & 0x03u; float dl = d * float(int(low | (high << 4)) - 32); uint q_base = sub * 16u; float block = 0.0f; for (uint i = 0u; i < NQ; ++i) { uint j = qj0 + i; uchar packed = qs[q_base + (j & 15u)]; uint code = j < 16u ? uint(packed & 0x0Fu) : uint(packed >> 4); block += x[i] * float(iq4_nl[code]); } " ++ accumulate(2, "dl * block") ++ " } }");
    if (eql(name, "q8_k")) return reduceKernel(name, 2, 2, false, "", "uint ix = lane / 4u; uint il = lane - ix * 4u; uint chunk0 = uint(sgitg) * 8u + ix;", " uint total_chunks = p.row_blocks * 64u;", "for (uint chunk = chunk0; chunk < total_chunks; chunk += NSG * 8u) { uint b = chunk >> 6; uint qj = ((chunk & 63u) << 2) + il; float x = input[in_row + b * 256u + qj]; for (uint row = 0u; row < NR0; ++row) { uint o = first_o + row; if (o >= p.out_dim) continue; uint off = o * p.row_blocks * 292u + b * 292u; uint bits = uint(weight[off]) | (uint(weight[off + 1u]) << 8) | (uint(weight[off + 2u]) << 16) | (uint(weight[off + 3u]) << 24); float d = as_type<float>(bits); char q = as_type<char>(weight[off + 4u + qj]); float block = x * (d * float(q)); " ++ accumulate(2, "block") ++ " } }");
    @compileError("no 1x_reduce for " ++ name);
}
