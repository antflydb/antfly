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

pub const Complex = struct {
    re: f32,
    im: f32,
};

/// Inverse MDCT of size `n` (n outputs from n/2 coefficients):
///
///     x[i] = (2/n) * sum_{k < n/2} X[k] * cos(2*pi/n * (i + 1/2 + n/4) * (k + 1/2))
///
/// which is the AAC (ISO 14496-3) and Vorbis kernel. Power-of-two sizes run
/// the classic n/4-point complex FFT with pre- and post-rotation; other sizes
/// (the AAC 960-sample frame) go through a Bluestein chirp-z convolution.
pub const Plan = struct {
    const Mode = enum {
        pow2_fft,
        bluestein,
    };

    n: usize,
    mode: Mode,
    fft_len: usize,
    pre_twiddle: []Complex,
    post_twiddle: []Complex,
    kernel_fft: []Complex,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, n: usize) !Plan {
        if (n == 0 or n % 4 != 0) return error.UnsupportedAudioFormat;

        if (n >= 8 and std.math.isPowerOfTwo(n)) {
            return initPow2Fft(allocator, n);
        }
        return initBluestein(allocator, n);
    }

    fn initPow2Fft(allocator: std.mem.Allocator, n: usize) !Plan {
        const quarter_n = n / 4;
        // One rotation table serves both the pre- and the post-rotation:
        // twiddle[i] = -exp(i * 2*pi*(i + 1/8)/n).
        const twiddle = try allocator.alloc(Complex, quarter_n);
        errdefer allocator.free(twiddle);
        const n_f = @as(f64, @floatFromInt(n));
        for (twiddle, 0..) |*value, i| {
            const angle = 2.0 * std.math.pi * (@as(f64, @floatFromInt(i)) + 0.125) / n_f;
            value.* = .{
                .re = @floatCast(-@cos(angle)),
                .im = @floatCast(-@sin(angle)),
            };
        }

        return .{
            .n = n,
            .mode = .pow2_fft,
            .fft_len = quarter_n,
            .pre_twiddle = twiddle,
            .post_twiddle = &.{},
            .kernel_fft = &.{},
            .allocator = allocator,
        };
    }

    fn initBluestein(allocator: std.mem.Allocator, n: usize) !Plan {
        const half_n = n / 2;
        const fft_len = ceilPowerOfTwo(2 * n - 1) orelse return error.UnsupportedAudioFormat;
        const pre_twiddle = try allocator.alloc(Complex, half_n);
        errdefer allocator.free(pre_twiddle);
        const post_twiddle = try allocator.alloc(Complex, n);
        errdefer allocator.free(post_twiddle);
        const kernel_fft = try allocator.alloc(Complex, fft_len);
        errdefer allocator.free(kernel_fft);
        @memset(kernel_fft, .{ .re = 0, .im = 0 });

        // cos(theta*a*b) = Re(exp(i*theta*(a+b)^2/2) * exp(-i*theta*a^2/2) * exp(-i*theta*b^2/2)),
        // so the sum over k is a linear convolution of the chirped
        // coefficients with the chirp kernel, evaluated at t = i + k.
        const n_f = @as(f64, @floatFromInt(n));
        const angle_scale = 2.0 * std.math.pi / n_f;
        const n_shift = n_f / 4.0 + 0.5;
        const k_shift = 0.5;

        for (pre_twiddle, 0..) |*twiddle, k| {
            const k_term = @as(f64, @floatFromInt(k)) + k_shift;
            const angle = -angle_scale * k_term * k_term * 0.5;
            twiddle.* = complexFromAngle(angle);
        }

        for (post_twiddle, 0..) |*twiddle, out_index| {
            const n_term = @as(f64, @floatFromInt(out_index)) + n_shift;
            const angle = -angle_scale * n_term * n_term * 0.5;
            twiddle.* = complexFromAngle(angle);
        }

        const kernel_len = n + half_n - 1;
        for (kernel_fft[0..kernel_len], 0..) |*value, t| {
            const t_term = @as(f64, @floatFromInt(t)) + n_shift + k_shift;
            const angle = angle_scale * t_term * t_term * 0.5;
            value.* = complexFromAngle(angle);
        }
        try fftComplex(kernel_fft, false);

        return .{
            .n = n,
            .mode = .bluestein,
            .fft_len = fft_len,
            .pre_twiddle = pre_twiddle,
            .post_twiddle = post_twiddle,
            .kernel_fft = kernel_fft,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Plan) void {
        self.allocator.free(self.pre_twiddle);
        if (self.post_twiddle.len > 0) self.allocator.free(self.post_twiddle);
        if (self.kernel_fft.len > 0) self.allocator.free(self.kernel_fft);
        self.* = undefined;
    }
};

pub fn imdctInto(out: []f32, coefficients: []const f32, plan: *const Plan, work: []Complex) !void {
    if (out.len != plan.n or coefficients.len * 2 != plan.n) return error.UnsupportedAudioFormat;
    if (work.len < plan.fft_len) return error.UnsupportedAudioFormat;

    switch (plan.mode) {
        .pow2_fft => return imdctIntoPow2Fft(out, coefficients, plan, work[0..plan.fft_len]),
        .bluestein => return imdctIntoBluestein(out, coefficients, plan, work[0..plan.fft_len]),
    }
}

/// The n/4-point FFT formulation: odd/even coefficient pairs are rotated
/// into n/4 complex values, transformed, rotated back, and the n/2 real
/// values that fall out are the middle half of the output, whose two outer
/// quarters follow from the IMDCT's odd and even symmetries.
fn imdctIntoPow2Fft(out: []f32, coefficients: []const f32, plan: *const Plan, z: []Complex) !void {
    const n = plan.n;
    const n2 = n / 2;
    const n4 = n / 4;
    const n8 = n / 8;
    if (z.len != n4) return error.UnsupportedAudioFormat;
    const twiddle = plan.pre_twiddle;

    for (0..n4) |k| {
        const a = coefficients[n2 - 1 - 2 * k];
        const b = coefficients[2 * k];
        z[k] = .{
            .re = a * twiddle[k].re - b * twiddle[k].im,
            .im = a * twiddle[k].im + b * twiddle[k].re,
        };
    }

    // The rotation tables assume an inverse (positive-exponent) transform;
    // `fftComplex` normalizes its inverse by 1/n4, which the scale undoes.
    try fftComplex(z, true);

    for (0..n8) |k| {
        const lo = z[n8 - k - 1];
        const hi = z[n8 + k];
        const t_lo = twiddle[n8 - k - 1];
        const t_hi = twiddle[n8 + k];
        const re_lo = lo.im * t_lo.im - lo.re * t_lo.re;
        const im_hi = lo.im * t_lo.re + lo.re * t_lo.im;
        const re_hi = hi.im * t_hi.im - hi.re * t_hi.re;
        const im_lo = hi.im * t_hi.re + hi.re * t_hi.im;
        z[n8 - k - 1] = .{ .re = re_lo, .im = im_lo };
        z[n8 + k] = .{ .re = re_hi, .im = im_hi };
    }

    // z now holds the middle half of the output as n/4 (re, im) pairs. The
    // rotation convention above yields the negated kernel, so the sign is
    // folded into the 2/n normalization along with the FFT's 1/n4.
    const scale = -(2.0 / @as(f32, @floatFromInt(n))) * @as(f32, @floatFromInt(n4));
    for (0..n4) |q| {
        out[n4 + 2 * q] = z[q].re * scale;
        out[n4 + 2 * q + 1] = z[q].im * scale;
    }
    for (0..n4) |k| {
        out[k] = -out[n2 - k - 1];
        out[n - k - 1] = out[n2 + k];
    }
}

fn imdctIntoBluestein(out: []f32, coefficients: []const f32, plan: *const Plan, fft_work: []Complex) !void {
    const half_n = coefficients.len;
    @memset(fft_work, .{ .re = 0, .im = 0 });

    for (coefficients, 0..) |coefficient, k| {
        const twiddle = plan.pre_twiddle[k];
        fft_work[half_n - 1 - k] = .{
            .re = coefficient * twiddle.re,
            .im = coefficient * twiddle.im,
        };
    }

    try fftComplex(fft_work, false);
    for (fft_work, plan.kernel_fft) |*value, kernel| {
        value.* = complexMul(value.*, kernel);
    }
    try fftComplex(fft_work, true);

    const scale = 2.0 / @as(f32, @floatFromInt(plan.n));
    for (out, 0..) |*sample, out_index| {
        const convolved = fft_work[out_index + half_n - 1];
        const rotated = complexMul(convolved, plan.post_twiddle[out_index]);
        sample.* = rotated.re * scale;
    }
}

fn ceilPowerOfTwo(value: usize) ?usize {
    if (value == 0) return 1;
    var out: usize = 1;
    while (out < value) {
        if (out > std.math.maxInt(usize) / 2) return null;
        out <<= 1;
    }
    return out;
}

fn complexFromAngle(angle: f64) Complex {
    return .{
        .re = @floatCast(@cos(angle)),
        .im = @floatCast(@sin(angle)),
    };
}

fn fftComplex(values: []Complex, inverse: bool) !void {
    if (values.len == 0) return;
    if (!std.math.isPowerOfTwo(values.len)) return error.UnsupportedAudioFormat;

    var j: usize = 0;
    for (1..values.len) |i| {
        var bit = values.len >> 1;
        while (j & bit != 0) : (bit >>= 1) {
            j ^= bit;
        }
        j ^= bit;
        if (i < j) std.mem.swap(Complex, &values[i], &values[j]);
    }

    var len: usize = 2;
    while (len <= values.len) : (len <<= 1) {
        const half = len >> 1;
        const sign: f32 = if (inverse) 1.0 else -1.0;
        const angle_step = sign * (2.0 * std.math.pi / @as(f32, @floatFromInt(len)));
        const wlen = Complex{ .re = @cos(angle_step), .im = @sin(angle_step) };

        var start: usize = 0;
        while (start < values.len) : (start += len) {
            var w = Complex{ .re = 1.0, .im = 0.0 };
            for (0..half) |i| {
                const u = values[start + i];
                const v = complexMul(values[start + i + half], w);
                values[start + i] = complexAdd(u, v);
                values[start + i + half] = complexSub(u, v);
                w = complexMul(w, wlen);
            }
        }
    }

    if (inverse) {
        const scale = @as(f32, @floatFromInt(values.len));
        for (values) |*value| {
            value.re /= scale;
            value.im /= scale;
        }
    }
}

fn complexAdd(a: Complex, b: Complex) Complex {
    return .{ .re = a.re + b.re, .im = a.im + b.im };
}

fn complexSub(a: Complex, b: Complex) Complex {
    return .{ .re = a.re - b.re, .im = a.im - b.im };
}

fn complexMul(a: Complex, b: Complex) Complex {
    return .{
        .re = a.re * b.re - a.im * b.im,
        .im = a.re * b.im + a.im * b.re,
    };
}

test "fft imdct matches naive kernel" {
    const sizes = [_]usize{ 8, 12, 16, 32, 64, 128, 256, 512, 1920, 2048 };
    for (sizes) |n| {
        const coefficients = try std.testing.allocator.alloc(f32, n / 2);
        defer std.testing.allocator.free(coefficients);
        for (coefficients, 0..) |*coefficient, i| {
            const signed_index = @as(f32, @floatFromInt(@as(isize, @intCast(i)) - @as(isize, @intCast(n / 4))));
            coefficient.* = @sin(@as(f32, @floatFromInt(i + 1)) * 0.37) + signed_index * 0.03125;
        }

        const expected = try std.testing.allocator.alloc(f32, n);
        defer std.testing.allocator.free(expected);
        const actual = try std.testing.allocator.alloc(f32, n);
        defer std.testing.allocator.free(actual);

        try imdctIntoNaiveForTest(expected, coefficients);

        var plan = try Plan.init(std.testing.allocator, n);
        defer plan.deinit();
        const work = try std.testing.allocator.alloc(Complex, plan.fft_len);
        defer std.testing.allocator.free(work);
        try imdctInto(actual, coefficients, &plan, work);

        for (expected, actual) |want, got| {
            try std.testing.expectApproxEqAbs(want, got, 2e-4);
        }
    }
}

fn imdctIntoNaiveForTest(out: []f32, coefficients: []const f32) !void {
    if (out.len != coefficients.len * 2) return error.UnsupportedAudioFormat;

    const n = out.len;
    const n_f = @as(f64, @floatFromInt(n));
    const scale = 2.0 / n_f;
    for (out, 0..) |*sample, n_idx| {
        const n_term = @as(f64, @floatFromInt(n_idx)) + 0.5 + n_f / 4.0;
        var accum: f64 = 0;
        for (coefficients, 0..) |coef, k_idx| {
            const k_term = @as(f64, @floatFromInt(k_idx)) + 0.5;
            accum += @as(f64, coef) * @cos((2.0 * std.math.pi / n_f) * n_term * k_term);
        }
        sample.* = @floatCast(accum * scale);
    }
}
