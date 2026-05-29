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

// QLoRA NF4 (4-bit NormalFloat) quantization with optional double quantization.
//
// TRAP: when `double_quant=true`, the second-level dequant overwrites
// `tensor.scales` in place with the reconstructed block scales — callers must
// not rely on the original quantized scale bytes afterward.

const std = @import("std");
const lora_init = @import("lora_init.zig");

pub const NF4_CODE: [16]f32 = .{
    -1.0,
    -0.6961928009986877,
    -0.5250730514526367,
    -0.39491748809814453,
    -0.28444138169288635,
    -0.18477343022823334,
    -0.09105003625154495,
    0.0,
    0.07958029955625534,
    0.16093020141124725,
    0.24611230194568634,
    0.33791524171829224,
    0.44070982933044434,
    0.5626170039176941,
    0.7229568362236023,
    1.0,
};

pub const NF4Config = struct {
    block_size: usize = 64,
    double_quant: bool = true,
    double_quant_block_size: usize = 256,
};

pub const NF4Tensor = struct {
    allocator: std.mem.Allocator,
    codes: []u8,
    scales: []f32,
    quant_scales: []u8,
    quant_scale_of_scales: []f32,
    num_elements: usize,
    config: NF4Config,

    pub fn deinit(self: *NF4Tensor) void {
        self.allocator.free(self.codes);
        self.allocator.free(self.scales);
        if (self.quant_scales.len > 0) self.allocator.free(self.quant_scales);
        if (self.quant_scale_of_scales.len > 0) self.allocator.free(self.quant_scale_of_scales);
        self.* = undefined;
    }
};

pub const LoftQNF4Options = struct {
    rank: usize,
    num_iter: u32 = 1,
    power_iters: u32 = 2,
    seed: u64 = 0x51f0_4a,
    nf4: NF4Config = .{},
};

pub const LoftQNF4Result = lora_init.LoftQResult;

pub fn nearestCodeIndex(x: f32) u4 {
    var best: u4 = 0;
    var best_d: f32 = @abs(x - NF4_CODE[0]);
    var i: usize = 1;
    while (i < 16) : (i += 1) {
        const d = @abs(x - NF4_CODE[i]);
        if (d < best_d) {
            best_d = d;
            best = @intCast(i);
        }
    }
    return best;
}

fn scaleByPowerOfTwo(x: f32, exponent: i32) f32 {
    var value = x;
    var e = exponent;
    while (e > 0) : (e -= 1) value *= 2.0;
    while (e < 0) : (e += 1) value *= 0.5;
    return value;
}

pub fn encodeFP8E4M3(x: f32) u8 {
    if (std.math.isNan(x)) return 0x7F;
    if (x == 0.0) {
        // preserve sign of zero
        const bits: u32 = @bitCast(x);
        return if ((bits >> 31) != 0) @as(u8, 0x80) else 0;
    }

    const sign_bit: u8 = if (x < 0.0) 0x80 else 0x00;
    var ax = @abs(x);

    // E4M3 max magnitude = S.1111.110 = 1.75 * 2^8 = 448 (no inf, 111.111 = NaN)
    const max_mag: f32 = 448.0;
    if (ax > max_mag) ax = max_mag;

    // Decompose: ax = sig * 2^e, sig in [1, 2)
    const split = std.math.frexp(ax); // significand in [0.5, 1)
    const sig: f32 = split.significand * 2.0; // now in [1, 2)
    const e: i32 = split.exponent - 1; // unbiased exponent for 1.xxx * 2^e

    const bias: i32 = 7;
    var biased: i32 = e + bias;

    // Subnormal handling: if biased <= 0, encode as subnormal with exponent field = 0.
    // Smallest normal = 2^(1-bias) = 2^-6. Subnormal step = 2^-6 / 8 = 2^-9.
    if (biased <= 0) {
        // Represent as (mantissa_frac) * 2^-6, mantissa range [0, 7] (3 bits)
        // value = mantissa * 2^-9
        const step: f32 = scaleByPowerOfTwo(1.0, -9);
        var q = @round(ax / step);
        if (q > 7) q = 7;
        const mant: u8 = @intFromFloat(q);
        return sign_bit | (mant & 0x07);
    }

    // Normal: exponent field in [1..15]. Exponent 15 with mantissa 7 = NaN, so max normal
    // is exponent 15, mantissa 6 (which is 1.75 * 2^8 = 448).
    // Mantissa: extract 3 bits from sig - 1 in [0,1), round-to-nearest-even.
    const frac = sig - 1.0; // [0, 1)
    const scaled = frac * 8.0; // [0, 8)
    var mant_i: i32 = @intFromFloat(@floor(scaled));
    const rem = scaled - @as(f32, @floatFromInt(mant_i));
    if (rem > 0.5) {
        mant_i += 1;
    } else if (rem == 0.5) {
        // round to even
        if ((mant_i & 1) == 1) mant_i += 1;
    }
    if (mant_i == 8) {
        mant_i = 0;
        biased += 1;
    }

    if (biased >= 16) {
        // overflow past max normal → saturate to 448 = exp 15, mantissa 6
        return sign_bit | (15 << 3) | 6;
    }

    // Avoid encoding NaN pattern (exp=15, mant=7) for a regular finite value
    if (biased == 15 and mant_i == 7) {
        mant_i = 6;
    }

    const exp_field: u8 = @intCast(biased);
    const mant_field: u8 = @intCast(mant_i);
    return sign_bit | (exp_field << 3) | (mant_field & 0x07);
}

pub fn decodeFP8E4M3(b: u8) f32 {
    const sign: f32 = if ((b & 0x80) != 0) -1.0 else 1.0;
    const exp_field: u8 = (b >> 3) & 0x0F;
    const mant_field: u8 = b & 0x07;

    // NaN pattern: S.1111.111
    if (exp_field == 0x0F and mant_field == 0x07) {
        return std.math.nan(f32);
    }

    const bias: i32 = 7;
    if (exp_field == 0) {
        // Subnormal: value = mant * 2^(1-bias-3) = mant * 2^-9
        const v = @as(f32, @floatFromInt(mant_field)) * scaleByPowerOfTwo(1.0, -9);
        return sign * v;
    }

    const e: i32 = @as(i32, exp_field) - bias;
    const frac = @as(f32, @floatFromInt(mant_field)) / 8.0;
    const mag = scaleByPowerOfTwo(1.0 + frac, e);
    return sign * mag;
}

pub fn quantize(
    allocator: std.mem.Allocator,
    input: []const f32,
    config: NF4Config,
) !NF4Tensor {
    const n = input.len;
    const bs = config.block_size;
    std.debug.assert(bs > 0);

    const num_blocks = (n + bs - 1) / bs;
    const num_bytes = (n + 1) / 2;

    var codes = try allocator.alloc(u8, num_bytes);
    errdefer allocator.free(codes);
    @memset(codes, 0);

    var scales = try allocator.alloc(f32, num_blocks);
    errdefer allocator.free(scales);

    // Per block: compute absmax scale and quantized codes.
    var b: usize = 0;
    while (b < num_blocks) : (b += 1) {
        const start = b * bs;
        const end = @min(start + bs, n);

        var absmax: f32 = 0.0;
        var i: usize = start;
        while (i < end) : (i += 1) {
            const a = @abs(input[i]);
            if (a > absmax) absmax = a;
        }
        const scale: f32 = if (absmax == 0.0) 1.0 else absmax;
        scales[b] = scale;

        i = start;
        while (i < end) : (i += 1) {
            const norm = input[i] / scale;
            const idx: u4 = nearestCodeIndex(norm);
            const byte_index = i / 2;
            if ((i & 1) == 0) {
                codes[byte_index] = (codes[byte_index] & 0xF0) | @as(u8, idx);
            } else {
                codes[byte_index] = (codes[byte_index] & 0x0F) | (@as(u8, idx) << 4);
            }
        }
    }

    var quant_scales: []u8 = &[_]u8{};
    var quant_sos: []f32 = &[_]f32{};

    if (config.double_quant and num_blocks > 0) {
        const dqbs = config.double_quant_block_size;
        std.debug.assert(dqbs > 0);
        const num_sos = (num_blocks + dqbs - 1) / dqbs;

        quant_scales = try allocator.alloc(u8, num_blocks);
        errdefer allocator.free(quant_scales);
        quant_sos = try allocator.alloc(f32, num_sos);
        errdefer allocator.free(quant_sos);

        var sb: usize = 0;
        while (sb < num_sos) : (sb += 1) {
            const s_start = sb * dqbs;
            const s_end = @min(s_start + dqbs, num_blocks);

            var sos_max: f32 = 0.0;
            var j: usize = s_start;
            while (j < s_end) : (j += 1) {
                const a = @abs(scales[j]);
                if (a > sos_max) sos_max = a;
            }
            // E4M3 max magnitude is 448; normalize scales into [-1,1] so fp8 encodes well.
            const sos: f32 = if (sos_max == 0.0) 1.0 else sos_max;
            quant_sos[sb] = sos;

            j = s_start;
            while (j < s_end) : (j += 1) {
                const normalized = scales[j] / sos; // in [-1, 1]
                const enc = encodeFP8E4M3(normalized);
                quant_scales[j] = enc;
                // Replace scales[j] with the dequantized value for runtime reads.
                scales[j] = decodeFP8E4M3(enc) * sos;
            }
        }
    }

    return NF4Tensor{
        .allocator = allocator,
        .codes = codes,
        .scales = scales,
        .quant_scales = quant_scales,
        .quant_scale_of_scales = quant_sos,
        .num_elements = n,
        .config = config,
    };
}

pub fn quantizeDequantize(
    allocator: std.mem.Allocator,
    input: []const f32,
    config: NF4Config,
) ![]f32 {
    var q = try quantize(allocator, input, config);
    defer q.deinit();
    const out = try allocator.alloc(f32, input.len);
    errdefer allocator.free(out);
    dequantize(&q, out);
    return out;
}

const LoftQNF4QuantizeContext = struct {
    config: NF4Config,
};

threadlocal var loftq_nf4_context: LoftQNF4QuantizeContext = .{ .config = .{} };

fn loftqNf4Quantize(allocator: std.mem.Allocator, w: []const f32) anyerror![]f32 {
    return quantizeDequantize(allocator, w, loftq_nf4_context.config);
}

/// LoftQ initialization specialized for QLoRA/NF4. The returned `a` and `b`
/// buffers are LoRA factors for the quantization residual W - Q_NF4(W), and
/// `residual` is the NF4-dequantized base approximation to persist/use for the
/// frozen quantized path.
pub fn loftqNf4Init(
    allocator: std.mem.Allocator,
    w: []const f32,
    out_features: usize,
    in_features: usize,
    options: LoftQNF4Options,
) !LoftQNF4Result {
    loftq_nf4_context = .{ .config = options.nf4 };
    return lora_init.loftqInit(
        allocator,
        w,
        out_features,
        in_features,
        options.rank,
        options.num_iter,
        options.power_iters,
        loftqNf4Quantize,
        options.seed,
    );
}

pub fn dequantize(tensor: *const NF4Tensor, out: []f32) void {
    std.debug.assert(out.len >= tensor.num_elements);
    const n = tensor.num_elements;
    const bs = tensor.config.block_size;

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const block = i / bs;
        const scale = tensor.scales[block];
        const byte_index = i / 2;
        const byte = tensor.codes[byte_index];
        const idx: u4 = if ((i & 1) == 0)
            @intCast(byte & 0x0F)
        else
            @intCast((byte >> 4) & 0x0F);
        out[i] = NF4_CODE[idx] * scale;
    }
}

pub fn dequantMatVec(
    allocator: std.mem.Allocator,
    w: *const NF4Tensor,
    rows: usize,
    cols: usize,
    x: []const f32,
    y: []f32,
) !void {
    std.debug.assert(rows * cols == w.num_elements);
    std.debug.assert(x.len >= cols);
    std.debug.assert(y.len >= rows);
    const bs = w.config.block_size;

    var scratch = try allocator.alloc(f32, cols);
    defer allocator.free(scratch);

    var r: usize = 0;
    while (r < rows) : (r += 1) {
        const row_start = r * cols;
        var c: usize = 0;
        while (c < cols) : (c += 1) {
            const gi = row_start + c;
            const block = gi / bs;
            const scale = w.scales[block];
            const byte_index = gi / 2;
            const byte = w.codes[byte_index];
            const idx: u4 = if ((gi & 1) == 0)
                @intCast(byte & 0x0F)
            else
                @intCast((byte >> 4) & 0x0F);
            scratch[c] = NF4_CODE[idx] * scale;
        }

        var acc: f32 = 0.0;
        var k: usize = 0;
        while (k < cols) : (k += 1) {
            acc += scratch[k] * x[k];
        }
        y[r] += acc;
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

test "nearestCodeIndex boundaries" {
    const t = std.testing;
    try t.expectEqual(@as(u4, 7), nearestCodeIndex(0.0));
    try t.expectEqual(@as(u4, 0), nearestCodeIndex(-1.0));
    try t.expectEqual(@as(u4, 15), nearestCodeIndex(1.0));
    try t.expectEqual(@as(u4, 0), nearestCodeIndex(-2.0));
    try t.expectEqual(@as(u4, 15), nearestCodeIndex(2.0));
}

test "zero input" {
    const t = std.testing;
    const a = std.testing.allocator;
    const n: usize = 128;
    const input = try a.alloc(f32, n);
    defer a.free(input);
    @memset(input, 0.0);

    var tensor = try quantize(a, input, .{ .block_size = 64, .double_quant = false });
    defer tensor.deinit();

    // All codes should be index 7
    for (0..n) |i| {
        const byte = tensor.codes[i / 2];
        const idx: u4 = if ((i & 1) == 0)
            @intCast(byte & 0x0F)
        else
            @intCast((byte >> 4) & 0x0F);
        try t.expectEqual(@as(u4, 7), idx);
    }

    const out = try a.alloc(f32, n);
    defer a.free(out);
    dequantize(&tensor, out);
    for (out) |v| try t.expectEqual(@as(f32, 0.0), v);
}

test "round trip relative error" {
    const t = std.testing;
    const a = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const rng = prng.random();

    const n: usize = 1024;
    const input = try a.alloc(f32, n);
    defer a.free(input);
    for (input) |*v| v.* = rng.floatNorm(f32);

    var tensor = try quantize(a, input, .{ .block_size = 64, .double_quant = false });
    defer tensor.deinit();

    const out = try a.alloc(f32, n);
    defer a.free(out);
    dequantize(&tensor, out);

    var num: f32 = 0.0;
    var den: f32 = 0.0;
    for (input, out) |xi, yi| {
        const e = xi - yi;
        num += e * e;
        den += xi * xi;
    }
    const rel = @sqrt(num / den);
    try t.expect(rel < 0.1);
}

test "tail block" {
    const t = std.testing;
    const a = std.testing.allocator;
    const n: usize = 130;
    const input = try a.alloc(f32, n);
    defer a.free(input);
    for (input, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) * 0.01 - 0.5;

    var tensor = try quantize(a, input, .{ .block_size = 64, .double_quant = false });
    defer tensor.deinit();

    try t.expectEqual(@as(usize, 3), tensor.scales.len);

    const out = try a.alloc(f32, n);
    defer a.free(out);
    dequantize(&tensor, out);

    var max_abs_err: f32 = 0.0;
    for (input, out) |xi, yi| {
        const e = @abs(xi - yi);
        if (e > max_abs_err) max_abs_err = e;
    }
    try t.expect(max_abs_err < 0.2);
}

test "double quant round trip" {
    const t = std.testing;
    const a = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x1234);
    const rng = prng.random();

    const n: usize = 4096;
    const input = try a.alloc(f32, n);
    defer a.free(input);
    for (input) |*v| v.* = rng.floatNorm(f32) * 2.0;

    var t_single = try quantize(a, input, .{ .block_size = 64, .double_quant = false });
    defer t_single.deinit();
    var t_double = try quantize(a, input, .{ .block_size = 64, .double_quant = true, .double_quant_block_size = 256 });
    defer t_double.deinit();

    try t.expect(t_double.quant_scales.len == t_double.scales.len);
    try t.expect(t_double.quant_scale_of_scales.len > 0);

    const out_s = try a.alloc(f32, n);
    defer a.free(out_s);
    const out_d = try a.alloc(f32, n);
    defer a.free(out_d);
    dequantize(&t_single, out_s);
    dequantize(&t_double, out_d);

    var num_s: f32 = 0.0;
    var num_d: f32 = 0.0;
    var den: f32 = 0.0;
    for (input, out_s, out_d) |xi, ys, yd| {
        num_s += (xi - ys) * (xi - ys);
        num_d += (xi - yd) * (xi - yd);
        den += xi * xi;
    }
    const rel_s = @sqrt(num_s / den);
    const rel_d = @sqrt(num_d / den);
    try t.expect(rel_d < rel_s + 0.02);
    try t.expect(rel_d < 0.12);
}

test "dequantMatVec matches reference" {
    const t = std.testing;
    const a = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rng = prng.random();

    const rows: usize = 8;
    const cols: usize = 128;
    const n = rows * cols;

    const w = try a.alloc(f32, n);
    defer a.free(w);
    for (w) |*v| v.* = rng.floatNorm(f32);

    const x = try a.alloc(f32, cols);
    defer a.free(x);
    for (x) |*v| v.* = rng.floatNorm(f32);

    var tensor = try quantize(a, w, .{ .block_size = 64, .double_quant = false });
    defer tensor.deinit();

    const w_dq = try a.alloc(f32, n);
    defer a.free(w_dq);
    dequantize(&tensor, w_dq);

    const y_ref = try a.alloc(f32, rows);
    defer a.free(y_ref);
    @memset(y_ref, 0.0);
    for (0..rows) |r| {
        var acc: f32 = 0.0;
        for (0..cols) |c| acc += w_dq[r * cols + c] * x[c];
        y_ref[r] = acc;
    }

    const y_mv = try a.alloc(f32, rows);
    defer a.free(y_mv);
    @memset(y_mv, 0.0);
    try dequantMatVec(a, &tensor, rows, cols, x, y_mv);

    for (y_ref, y_mv) |yr, ym| {
        try t.expect(@abs(yr - ym) < 1e-5);
    }
}

test "fp8 e4m3 basics" {
    const t = std.testing;
    try t.expectEqual(@as(f32, 0.0), decodeFP8E4M3(encodeFP8E4M3(0.0)));

    const one = decodeFP8E4M3(encodeFP8E4M3(1.0));
    try t.expect(@abs(one - 1.0) < 0.125);

    const onefive = decodeFP8E4M3(encodeFP8E4M3(1.5));
    try t.expect(@abs(onefive - 1.5) < 0.125);

    const neg = decodeFP8E4M3(encodeFP8E4M3(-0.75));
    try t.expect(@abs(neg - (-0.75)) < 0.0625);

    // Saturation
    const big = decodeFP8E4M3(encodeFP8E4M3(10000.0));
    try t.expect(big <= 448.0 and big >= 400.0);
}

test "loftqNf4Init returns quant-aware adapter factors" {
    const allocator = std.testing.allocator;
    const w = [_]f32{
        0.10,  -0.20, 0.30,
        0.40,  0.05,  -0.15,
        -0.35, 0.25,  0.12,
        0.18,  -0.08, 0.22,
    };
    var result = try loftqNf4Init(allocator, &w, 4, 3, .{
        .rank = 2,
        .num_iter = 1,
        .power_iters = 1,
        .seed = 123,
        .nf4 = .{ .block_size = 4, .double_quant = false },
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2 * 3), result.a.len);
    try std.testing.expectEqual(@as(usize, 4 * 2), result.b.len);
    try std.testing.expectEqual(@as(usize, w.len), result.residual.len);
    for (result.a) |v| try std.testing.expect(std.math.isFinite(v));
    for (result.b) |v| try std.testing.expect(std.math.isFinite(v));
    for (result.residual) |v| try std.testing.expect(std.math.isFinite(v));
}
