// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const tile = @import("tile.zig");

pub const native_port_available = true;

const simd_lanes_i32_f32 = 8;
const I32x8 = @Vector(simd_lanes_i32_f32, i32);
const F32x8 = @Vector(simd_lanes_i32_f32, f32);

/// Decode the exponent from a style-0 step value (5-bit expn stored in bits [7:3]).
pub fn style0Exponent(step_value: u16) u8 {
    return @intCast(step_value >> 3);
}

/// Decode the exponent from a style-1/2 step value.
/// Format: expn (5 bits) << 11 | mantissa (11 bits)
pub fn stepExponent(step_value: u16) u8 {
    return @intCast(step_value >> 11);
}

/// Decode the mantissa from a style-1/2 step value.
pub fn stepMantissa(step_value: u16) u16 {
    return step_value & 0x7ff;
}

/// Compute the quantization step size for a subband.
/// step_size = (1 + mantissa / 2048) * 2^(guard_bits + exponent - 1)
/// For style 0 (no quantization / implicit), mantissa is 0 and exponent comes from the step value.
pub fn stepSize(step_value: u16, guard_bits: u8, style: u8) f64 {
    if (style == 0) {
        const expn = style0Exponent(step_value);
        return @as(f64, @floatFromInt(@as(u64, 1) << @intCast(guard_bits + expn - 1)));
    }
    const expn = stepExponent(step_value);
    const mant = stepMantissa(step_value);
    const base: f64 = 1.0 + @as(f64, @floatFromInt(mant)) / 2048.0;
    if (guard_bits + expn == 0) return 0.0;
    const shift: i32 = @as(i32, guard_bits) + @as(i32, expn) - 1;
    if (shift >= 0) {
        return base * @as(f64, @floatFromInt(@as(u64, 1) << @intCast(shift)));
    } else {
        return base / @as(f64, @floatFromInt(@as(u64, 1) << @intCast(-shift)));
    }
}

/// Dequantize a subband's integer coefficients to f32 using the given step size.
/// result[i] = @floatCast(coefficients[i]) * step_size_f32
pub fn dequantizeSubband(
    allocator: std.mem.Allocator,
    coefficients: []const i32,
    step_value: u16,
    guard_bits: u8,
    style: u8,
) ![]f32 {
    const step = @as(f32, @floatCast(stepSize(step_value, guard_bits, style)));
    const out = try allocator.alloc(f32, coefficients.len);
    errdefer allocator.free(out);

    var i: usize = 0;
    const step_v: F32x8 = @splat(step);
    while (i + simd_lanes_i32_f32 <= coefficients.len) : (i += simd_lanes_i32_f32) {
        const coeff_v: I32x8 = coefficients[i..][0..simd_lanes_i32_f32].*;
        out[i..][0..simd_lanes_i32_f32].* = @as(F32x8, @floatFromInt(coeff_v)) * step_v;
    }

    while (i < coefficients.len) : (i += 1) {
        const coeff = coefficients[i];
        out[i] = @as(f32, @floatFromInt(coeff)) * step;
    }
    return out;
}

/// Compute effective bitplanes for a codeblock (used in Tier-1 pass scheduling).
/// ISO 15444-1 B.10.5: Mb = G + εb − 1
pub fn effectiveBitplanes(step_value: u16, guard_bits: u8, style: u8) u8 {
    const expn: u8 = if (style == 0) style0Exponent(step_value) else stepExponent(step_value);
    if (guard_bits + expn == 0) return 0;
    return guard_bits + expn - 1;
}

fn subbandStepIndex(resolution_index: u8, subband: tile.SubbandType) usize {
    if (resolution_index == 0 or subband == .ll) return 0;
    const subband_offset: usize = switch (subband) {
        .ll => 0,
        .hl => 0,
        .lh => 1,
        .hh => 2,
    };
    return 1 + 3 * (@as(usize, resolution_index) - 1) + subband_offset;
}

/// Return the effective packed step value for a subband. Scalar-derived
/// quantization (style 1) signals only one base step; subband exponents are
/// derived from that base while retaining its mantissa.
pub fn stepValueForSubband(
    style: u8,
    step_values: []const u16,
    resolution_index: u8,
    subband: tile.SubbandType,
) ?u16 {
    if (step_values.len == 0) return null;
    if (style == 1) {
        const base = step_values[0];
        const base_exp = stepExponent(base);
        const derived_exp_i: i16 = if (resolution_index == 0)
            @intCast(base_exp)
        else
            @as(i16, @intCast(base_exp)) - (@as(i16, resolution_index) - 1);
        if (derived_exp_i <= 0 or derived_exp_i > 31) return null;
        return (@as(u16, @intCast(derived_exp_i)) << 11) | stepMantissa(base);
    }

    const index = subbandStepIndex(resolution_index, subband);
    if (index >= step_values.len) return null;
    return step_values[index];
}

pub fn exponentForSubband(
    style: u8,
    step_values: []const u16,
    resolution_index: u8,
    subband: tile.SubbandType,
) ?u8 {
    const step_value = stepValueForSubband(style, step_values, resolution_index, subband) orelse return null;
    return if (style == 0) style0Exponent(step_value) else stepExponent(step_value);
}

/// Subband gain exponent per ISO 15444-1 Annex F (Table F-1) for irreversible 9/7.
/// 0 for LL, 1 for HL and LH, 2 for HH.
pub fn irreversibleSubbandGain(subband: tile.SubbandType) u8 {
    return switch (subband) {
        .ll => 0,
        .hl, .lh => 1,
        .hh => 2,
    };
}

/// ISO 15444-1 Annex E step size for irreversible (9/7) quantization:
///   Δ_b = 2^(R_b − ε_b) · (1 + μ_b / 2^11)
/// where R_b = precision + gain(b). Mirrors OpenJPEG's `dwt_stepsize`.
pub fn stepSizeIrreversible(step_value: u16, precision: u8, gain: u8) f64 {
    // Native decode path enforces precision <= 16 at the gate (see
    // codestream.State.fullNativeDecodeSupport) and ISO Annex F Table F-1
    // caps gain at 2 (HH subband), so R_b <= 18 in practice. Guard the
    // invariant so a future wider path cannot silently produce UB through
    // the `1 << shift` below; 31 matches the 5-bit εb field's range.
    std.debug.assert(@as(u32, precision) + @as(u32, gain) <= 31);
    const expn: i32 = @intCast(stepExponent(step_value));
    const mant = stepMantissa(step_value);
    const base: f64 = 1.0 + @as(f64, @floatFromInt(mant)) / 2048.0;
    const r_b: i32 = @as(i32, precision) + @as(i32, gain);
    const shift: i32 = r_b - expn;
    if (shift >= 0) {
        return base * @as(f64, @floatFromInt(@as(u64, 1) << @intCast(shift)));
    }
    return base / @as(f64, @floatFromInt(@as(u64, 1) << @intCast(-shift)));
}

/// OpenJPEG's `dwt_norms_real` table: per-subband L2 norms for the irreversible
/// 9/7 wavelet, indexed as `[orient][level]` where `orient` is 0=LL, 1=HL, 2=LH,
/// 3=HH and `level` is the decomposition level counted from the finest (level 0
/// is the highest-frequency detail). Values match OpenJPEG's reference table.
pub const dwt_norms_real = [4][10]f64{
    .{ 1.000, 1.965, 4.177, 8.403, 16.90, 33.84, 67.69, 135.3, 270.6, 540.9 },
    .{ 2.022, 3.989, 8.355, 17.04, 34.27, 68.63, 137.3, 274.6, 549.0, 1097.0 },
    .{ 2.022, 3.989, 8.355, 17.04, 34.27, 68.63, 137.3, 274.6, 549.0, 1097.0 },
    .{ 2.080, 3.865, 8.307, 17.18, 34.71, 69.59, 139.3, 278.6, 557.2, 1113.0 },
};

fn orientIndex(subband: tile.SubbandType) usize {
    return switch (subband) {
        .ll => 0,
        .hl => 1,
        .lh => 2,
        .hh => 3,
    };
}

/// Compute the ISO/OpenJPEG-compatible irreversible 9/7 stepsize Δ for a
/// given subband. `level` is the DWT decomposition level counted from the
/// finest detail (level 0 is the outermost detail pass; for LL use
/// `numres − 1` i.e. the deepest level). Matches OpenJPEG's
/// `opj_dwt_calc_explicit_stepsizes`: Δ = 2^gain(orient) / norms[orient][level].
pub fn irreversibleSubbandStepsize(subband: tile.SubbandType, level: u8) f64 {
    const orient = orientIndex(subband);
    const lvl: usize = if (level >= dwt_norms_real[0].len) dwt_norms_real[0].len - 1 else level;
    const gain: u3 = @intCast(irreversibleSubbandGain(subband));
    const num: f64 = @floatFromInt(@as(u32, 1) << gain);
    return num / dwt_norms_real[orient][lvl];
}

/// Encode an irreversible stepsize Δ into a packed (ε, μ) u16 step value.
/// Chooses ε such that the value fits in ISO's 5/11-bit split; μ fine-tunes the mantissa.
pub fn encodeStepValueIrreversible(delta: f64, precision: u8, gain: u8) u16 {
    const r_b: i32 = @as(i32, precision) + @as(i32, gain);
    // Δ = 2^(R_b − ε) · (1 + μ/2048). Choose ε so that 1 ≤ (1 + μ/2048) < 2.
    // ε = R_b − floor(log2(Δ))
    const log2_delta: f64 = @log2(delta);
    var expn: i32 = r_b - @as(i32, @intFromFloat(@floor(log2_delta)));
    if (expn < 0) expn = 0;
    if (expn > 31) expn = 31;
    const shift: i32 = r_b - expn;
    const base: f64 = if (shift >= 0)
        delta / @as(f64, @floatFromInt(@as(u64, 1) << @intCast(shift)))
    else
        delta * @as(f64, @floatFromInt(@as(u64, 1) << @intCast(-shift)));
    var mant_f: f64 = (base - 1.0) * 2048.0;
    if (mant_f < 0) mant_f = 0;
    if (mant_f > 2047) mant_f = 2047;
    const mant: u16 = @intFromFloat(@round(mant_f));
    return (@as(u16, @intCast(expn)) << 11) | (mant & 0x7ff);
}

test "style0Exponent extracts 5-bit exponent from bits 7:3" {
    // expn = 0b10101 = 21, stored as 21 << 3 = 168
    try std.testing.expectEqual(@as(u8, 21), style0Exponent(21 << 3));
    // expn = 0, stored as 0
    try std.testing.expectEqual(@as(u8, 0), style0Exponent(0));
    // expn = 1, stored as 1 << 3 = 8
    try std.testing.expectEqual(@as(u8, 1), style0Exponent(8));
    // expn = 31 (max 5-bit), stored as 31 << 3 = 248
    try std.testing.expectEqual(@as(u8, 31), style0Exponent(31 << 3));
    // lower 3 bits should be ignored
    try std.testing.expectEqual(@as(u8, 5), style0Exponent((5 << 3) | 0x07));
}

test "stepExponent and stepMantissa parse style-1/2 step values" {
    // expn = 13, mantissa = 1025 => step_value = (13 << 11) | 1025 = 27649
    const step_value: u16 = (13 << 11) | 1025;
    try std.testing.expectEqual(@as(u8, 13), stepExponent(step_value));
    try std.testing.expectEqual(@as(u16, 1025), stepMantissa(step_value));

    // expn = 0, mantissa = 0
    try std.testing.expectEqual(@as(u8, 0), stepExponent(0));
    try std.testing.expectEqual(@as(u16, 0), stepMantissa(0));

    // expn = 31 (max 5-bit), mantissa = 2047 (max 11-bit) => 0xFFFF
    try std.testing.expectEqual(@as(u8, 31), stepExponent(0xFFFF));
    try std.testing.expectEqual(@as(u16, 2047), stepMantissa(0xFFFF));

    // mantissa only (expn = 0, mantissa = 0x7ff)
    try std.testing.expectEqual(@as(u8, 0), stepExponent(0x7ff));
    try std.testing.expectEqual(@as(u16, 0x7ff), stepMantissa(0x7ff));
}

test "stepSize computes correct values for all 3 styles" {
    // Style 0 (implicit): step_size = 2^(guard_bits + expn - 1), no mantissa.
    // guard_bits = 2, expn = 3 (step_value = 3 << 3 = 24) => 2^(2+3-1) = 2^4 = 16.0
    try std.testing.expectEqual(@as(f64, 16.0), stepSize(24, 2, 0));

    // Style 0: guard_bits = 1, expn = 1 (step_value = 1 << 3 = 8) => 2^(1+1-1) = 2^1 = 2.0
    try std.testing.expectEqual(@as(f64, 2.0), stepSize(8, 1, 0));

    // Style 1 (scalar derived): guard_bits = 2, expn = 3, mantissa = 0
    // step_value = (3 << 11) | 0 = 6144
    // step_size = (1 + 0/2048) * 2^(2+3-1) = 1.0 * 16.0 = 16.0
    try std.testing.expectEqual(@as(f64, 16.0), stepSize(6144, 2, 1));

    // Style 2 (scalar expounded): guard_bits = 2, expn = 3, mantissa = 1024
    // step_value = (3 << 11) | 1024 = 7168
    // step_size = (1 + 1024/2048) * 2^(2+3-1) = 1.5 * 16.0 = 24.0
    try std.testing.expectEqual(@as(f64, 24.0), stepSize(7168, 2, 2));

    // Style 1: guard_bits + expn == 0 => returns 0.0
    try std.testing.expectEqual(@as(f64, 0.0), stepSize(0, 0, 1));

    // Style 1: negative shift case: guard_bits = 0, expn = 0 but handled above;
    // test with guard_bits = 0, expn = 1, mantissa = 0 => shift = 0+1-1 = 0 => 1.0 * 1.0 = 1.0
    try std.testing.expectEqual(@as(f64, 1.0), stepSize(1 << 11, 0, 1));
}

test "dequantizeSubband produces expected output" {
    const allocator = std.testing.allocator;

    // Style 1, guard_bits = 2, expn = 3, mantissa = 0 => step_size = 16.0
    const coefficients = [_]i32{ 1, -2, 3, 0, -1 };
    const result = try dequantizeSubband(allocator, &coefficients, 6144, 2, 1);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 5), result.len);
    try std.testing.expectEqual(@as(f32, 16.0), result[0]);
    try std.testing.expectEqual(@as(f32, -32.0), result[1]);
    try std.testing.expectEqual(@as(f32, 48.0), result[2]);
    try std.testing.expectEqual(@as(f32, 0.0), result[3]);
    try std.testing.expectEqual(@as(f32, -16.0), result[4]);
}

test "dequantizeSubband handles empty coefficients" {
    const allocator = std.testing.allocator;
    const empty = [_]i32{};
    const result = try dequantizeSubband(allocator, &empty, 6144, 2, 1);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "effectiveBitplanes returns correct values" {
    // Style 0: guard_bits = 2, expn = 5 (step_value = 5 << 3 = 40) => 2 + 5 - 1 = 6
    try std.testing.expectEqual(@as(u8, 6), effectiveBitplanes(40, 2, 0));

    // Style 1: guard_bits = 1, expn = 4 (step_value = 4 << 11 = 8192) => 1 + 4 - 1 = 4
    try std.testing.expectEqual(@as(u8, 4), effectiveBitplanes(8192, 1, 1));

    // Style 2: guard_bits = 3, expn = 2 (step_value = (2 << 11) | 500) => 3 + 2 - 1 = 4
    try std.testing.expectEqual(@as(u8, 4), effectiveBitplanes((2 << 11) | 500, 3, 2));

    // guard_bits + expn == 0 => returns 0
    try std.testing.expectEqual(@as(u8, 0), effectiveBitplanes(0, 0, 1));
}

test "stepValueForSubband derives scalar-derived exponents from base step" {
    const base = (@as(u16, 12) << 11) | 33;
    const steps = [_]u16{base};

    try std.testing.expectEqual(base, stepValueForSubband(1, &steps, 0, .ll).?);
    try std.testing.expectEqual(base, stepValueForSubband(1, &steps, 1, .hl).?);
    try std.testing.expectEqual((@as(u16, 11) << 11) | 33, stepValueForSubband(1, &steps, 2, .lh).?);
    try std.testing.expectEqual((@as(u16, 10) << 11) | 33, stepValueForSubband(1, &steps, 3, .hh).?);
}

test "stepSizeIrreversible matches ISO formula Δ = 2^(R_b − ε)·(1 + μ/2048)" {
    // precision=8, gain=0 (LL), ε=8, μ=0 => Δ = 2^0 · 1 = 1.0
    try std.testing.expectEqual(@as(f64, 1.0), stepSizeIrreversible(8 << 11, 8, 0));
    // precision=8, gain=2 (HH), ε=10, μ=0 => Δ = 2^0 · 1 = 1.0
    try std.testing.expectEqual(@as(f64, 1.0), stepSizeIrreversible(10 << 11, 8, 2));
    // precision=8, gain=0, ε=7, μ=0 => Δ = 2^1 · 1 = 2.0
    try std.testing.expectEqual(@as(f64, 2.0), stepSizeIrreversible(7 << 11, 8, 0));
    // precision=8, gain=0, ε=9, μ=0 => Δ = 2^-1 · 1 = 0.5
    try std.testing.expectEqual(@as(f64, 0.5), stepSizeIrreversible(9 << 11, 8, 0));
    // ε=8, μ=1024 => Δ = 1 · (1 + 0.5) = 1.5
    try std.testing.expectEqual(@as(f64, 1.5), stepSizeIrreversible((8 << 11) | 1024, 8, 0));
}

test "encodeStepValueIrreversible round-trips with stepSizeIrreversible" {
    const cases = [_]struct { delta: f64, precision: u8, gain: u8 }{
        .{ .delta = 1.0, .precision = 8, .gain = 0 },
        .{ .delta = 2.0, .precision = 8, .gain = 1 },
        .{ .delta = 1.5, .precision = 10, .gain = 2 },
        .{ .delta = 0.5, .precision = 12, .gain = 0 },
        .{ .delta = 4.0, .precision = 8, .gain = 2 },
    };
    for (cases) |c| {
        const encoded = encodeStepValueIrreversible(c.delta, c.precision, c.gain);
        const decoded = stepSizeIrreversible(encoded, c.precision, c.gain);
        try std.testing.expectApproxEqAbs(c.delta, decoded, 1e-3);
    }
}
