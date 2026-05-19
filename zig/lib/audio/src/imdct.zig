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

pub const Plan = struct {
    const Mode = enum {
        residue_fft,
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

        if (ceilPowerOfTwo(2 * n) == 2 * n) {
            return initResidueFft(allocator, n);
        }
        return initBluestein(allocator, n);
    }

    fn initResidueFft(allocator: std.mem.Allocator, n: usize) !Plan {
        const quarter_n = n / 4;
        const pre_twiddle = try allocator.alloc(Complex, 8 * quarter_n);
        errdefer allocator.free(pre_twiddle);
        const post_twiddle = try allocator.alloc(Complex, 2 * quarter_n);
        errdefer allocator.free(post_twiddle);

        const n_f = @as(f64, @floatFromInt(n));
        const output_shift = n_f / 4.0 + 0.5;

        for (0..4) |residue| {
            for (0..2) |parity| {
                const table_base = residueParityBase(residue, parity, quarter_n);
                for (0..quarter_n) |r| {
                    const k = 2 * r + parity;
                    const angle = std.math.pi *
                        (@as(f64, @floatFromInt(residue)) + output_shift) *
                        (@as(f64, @floatFromInt(k)) + 0.5) / n_f;
                    pre_twiddle[table_base + r] = complexFromAngle(angle);
                }
            }
        }

        for (0..2) |parity| {
            const frequency = @as(f64, @floatFromInt(parity)) + 0.5;
            const table_base = parity * quarter_n;
            for (0..quarter_n) |q| {
                const angle = 4.0 * std.math.pi *
                    @as(f64, @floatFromInt(q)) *
                    frequency / n_f;
                post_twiddle[table_base + q] = complexFromAngle(angle);
            }
        }

        return .{
            .n = n,
            .mode = .residue_fft,
            .fft_len = quarter_n,
            .pre_twiddle = pre_twiddle,
            .post_twiddle = post_twiddle,
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

        const n_f = @as(f64, @floatFromInt(n));
        const angle_scale = std.math.pi / n_f;
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
        self.allocator.free(self.post_twiddle);
        if (self.kernel_fft.len > 0) self.allocator.free(self.kernel_fft);
        self.* = undefined;
    }
};

pub fn imdctInto(out: []f32, coefficients: []const f32, plan: *const Plan, work: []Complex) !void {
    if (out.len != plan.n or coefficients.len * 2 != plan.n) return error.UnsupportedAudioFormat;
    if (work.len < plan.fft_len) return error.UnsupportedAudioFormat;

    switch (plan.mode) {
        .residue_fft => return imdctIntoResidueFft(out, coefficients, plan, work[0..plan.fft_len]),
        .bluestein => return imdctIntoBluestein(out, coefficients, plan, work[0..plan.fft_len]),
    }
}

fn imdctIntoResidueFft(out: []f32, coefficients: []const f32, plan: *const Plan, fft_work: []Complex) !void {
    const quarter_n = plan.n / 4;
    if (fft_work.len != quarter_n) return error.UnsupportedAudioFormat;
    @memset(out, 0);

    const scale = (2.0 / @as(f32, @floatFromInt(plan.n))) * @as(f32, @floatFromInt(quarter_n));
    for (0..4) |residue| {
        for (0..2) |parity| {
            const pre_base = residueParityBase(residue, parity, quarter_n);
            for (0..quarter_n) |r| {
                const coefficient = coefficients[2 * r + parity];
                const twiddle = plan.pre_twiddle[pre_base + r];
                fft_work[r] = .{
                    .re = coefficient * twiddle.re,
                    .im = coefficient * twiddle.im,
                };
            }

            try fftComplex(fft_work, true);

            const post_base = parity * quarter_n;
            for (0..quarter_n) |q| {
                const rotated = complexMul(fft_work[q], plan.post_twiddle[post_base + q]);
                out[4 * q + residue] += rotated.re * scale;
            }
        }
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

fn residueParityBase(residue: usize, parity: usize, quarter_n: usize) usize {
    return (residue * 2 + parity) * quarter_n;
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
    const sizes = [_]usize{ 8, 12, 16, 32, 64 };
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
            try std.testing.expectApproxEqAbs(want, got, 1e-4);
        }
    }
}

fn imdctIntoNaiveForTest(out: []f32, coefficients: []const f32) !void {
    if (out.len != coefficients.len * 2) return error.UnsupportedAudioFormat;

    const n = out.len;
    const scale = 2.0 / @as(f32, @floatFromInt(n));
    for (out, 0..) |*sample, n_idx| {
        const n_term = @as(f32, @floatFromInt(n_idx)) + 0.5 + @as(f32, @floatFromInt(n)) / 4.0;
        var accum: f32 = 0;
        for (coefficients, 0..) |coef, k_idx| {
            const k_term = @as(f32, @floatFromInt(k_idx)) + 0.5;
            accum += coef * @cos((std.math.pi / @as(f32, @floatFromInt(n))) * n_term * k_term);
        }
        sample.* = accum * scale;
    }
}
