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

const std = @import("std");
pub const lanes = 8;
pub fn fillTwiddles(out: *[512]f32) void {
    for (0..256) |i| {
        const angle = -2.0 * std.math.pi * @as(f64, @floatFromInt(i)) / 512.0;
        out[2 * i] = @floatCast(@cos(angle));
        out[2 * i + 1] = @floatCast(@sin(angle));
    }
}
/// SIMD lanes are independent frames. No reduction order crosses a lane.
/// Scratch is fixed (32 KiB), tails are zero-filled and never published.
pub fn magnitudes(samples: []const f32, frames: usize, hop: usize, window: []const f32, twiddles: []const f32, output: []f32) !void {
    if (frames == 0 or window.len != 320 or twiddles.len != 512 or output.len != try std.math.mul(usize, frames, 257)) return error.InvalidTensorShape;
    const required = try std.math.add(usize, try std.math.mul(usize, frames - 1, hop), 320);
    if (samples.len < required) return error.InvalidTensorShape;
    const V = @Vector(lanes, f32);
    var re: [512]V = undefined;
    var im: [512]V = undefined;
    var first: usize = 0;
    while (first < frames) : (first += lanes) {
        for (0..512) |i| {
            var value: [lanes]f32 = @splat(0);
            if (i < 320) for (&value, 0..) |*x, lane| {
                if (first + lane < frames) x.* = samples[(first + lane) * hop + i] * window[i];
            };
            const reversed: usize = @as(u32, @bitReverse(@as(u32, @intCast(i)))) >> 23;
            re[reversed] = value;
            im[reversed] = @splat(0);
        }
        var len: usize = 2;
        while (len <= 512) : (len *= 2) {
            const half = len / 2;
            var base: usize = 0;
            while (base < 512) : (base += len) for (0..half) |j| {
                const tw = j * (512 / len);
                const wr: V = @splat(twiddles[2 * tw]);
                const wi: V = @splat(twiddles[2 * tw + 1]);
                const r = re[base + j + half] * wr - im[base + j + half] * wi;
                const v = re[base + j + half] * wi + im[base + j + half] * wr;
                const ar = re[base + j];
                const ai = im[base + j];
                re[base + j] = ar + r;
                im[base + j] = ai + v;
                re[base + j + half] = ar - r;
                im[base + j + half] = ai - v;
            };
        }
        for (0..257) |bin| {
            const power: V = re[bin] * re[bin] + im[bin] * im[bin];
            const magnitude: V = @sqrt(power);
            const values: [lanes]f32 = magnitude;
            for (values, 0..) |x, lane| if (first + lane < frames) {
                output[(first + lane) * 257 + bin] = x;
            };
        }
    }
}

test "embeddinggemma2 SIMD FFT512 matches scalar DFT across frame tails and silent inputs" {
    const a = std.testing.allocator;
    var tw: [512]f32 = undefined;
    fillTwiddles(&tw);
    const frames = 11;
    const hop = 160;
    var samples: [(frames - 1) * hop + 320]f32 = undefined;
    var window: [320]f32 = undefined;
    for (&samples, 0..) |*x, i| x.* = @sin(@as(f32, @floatFromInt(i)) * 0.137) * 0.4;
    for (&window, 0..) |*x, i| x.* = 0.5 - 0.5 * @cos(2 * std.math.pi * @as(f32, @floatFromInt(i)) / 320);
    const out = try a.alloc(f32, frames * 257);
    defer a.free(out);
    try magnitudes(&samples, frames, hop, &window, &tw, out);
    for (0..frames) |frame| for (0..257) |bin| {
        var re: f64 = 0;
        var im: f64 = 0;
        for (0..320) |i| {
            const angle = -2 * std.math.pi * @as(f64, @floatFromInt(bin * i)) / 512;
            const x: @TypeOf(re) = samples[frame * hop + i] * window[i];
            re += x * @cos(angle);
            im += x * @sin(angle);
        }
        try std.testing.expectApproxEqAbs(@as(f32, @floatCast(@sqrt(re * re + im * im))), out[frame * 257 + bin], 0.00004);
    };
    @memset(&samples, 0);
    try magnitudes(&samples, frames, hop, &window, &tw, out);
    for (out) |x| try std.testing.expectEqual(@as(f32, 0), x);
    try std.testing.expectError(error.InvalidTensorShape, magnitudes(&samples, frames + 1, hop, &window, &tw, out));
}

test "embeddinggemma2 FFT512 completed CPU benchmark" {
    if (comptime @import("builtin").os.tag != .macos or !@import("builtin").link_libc) return error.SkipZigTest;
    const Blas = struct {
        extern fn cblas_sgemm(order: c_int, trans_a: c_int, trans_b: c_int, m: c_int, n: c_int, k: c_int, alpha: f32, a: [*]const f32, lda: c_int, b: [*]const f32, ldb: c_int, beta: f32, c: [*]f32, ldc: c_int) void;
    };
    if (std.c.getenv("ANTFLY_EMBEDDINGGEMMA2_BENCH_FFT") == null) return error.SkipZigTest;
    const Clock = struct {
        fn ns() u64 {
            var t: std.posix.timespec = undefined;
            _ = std.posix.system.clock_gettime(.MONOTONIC, &t);
            return @intCast(@as(i128, t.sec) * std.time.ns_per_s + t.nsec);
        }
    };
    const a = std.testing.allocator;
    var tw: [512]f32 = undefined;
    fillTwiddles(&tw);
    var window: [320]f32 = undefined;
    for (&window, 0..) |*x, i| x.* = 0.5 - 0.5 * @cos(2 * std.math.pi * @as(f32, @floatFromInt(i)) / 320);
    const basis = try a.alloc(f32, 320 * 514);
    defer a.free(basis);
    for (0..320) |i| for (0..257) |k| {
        const angle = 2 * std.math.pi * @as(f64, @floatFromInt(i * k)) / 512;
        basis[i * 514 + 2 * k] = @floatCast(@cos(angle));
        basis[i * 514 + 2 * k + 1] = @floatCast(-@sin(angle));
    };
    for ([_]usize{ 40, 200, 1000 }) |frames| {
        const samples = try a.alloc(f32, (frames - 1) * 160 + 320);
        defer a.free(samples);
        for (samples, 0..) |*x, i| x.* = @sin(@as(f32, @floatFromInt(i)) * 0.137) * 0.4;
        const packed_rows = try a.alloc(f32, frames * 320);
        defer a.free(packed_rows);
        const complex = try a.alloc(f32, frames * 514);
        defer a.free(complex);
        const output = try a.alloc(f32, frames * 257);
        defer a.free(output);
        const reference = try a.alloc(f32, frames * 257);
        defer a.free(reference);
        for (0..2) |route| {
            var total: u64 = 0;
            for (0..13) |iteration| {
                const start = Clock.ns();
                if (route == 0) {
                    for (0..frames) |r| for (0..320) |col| {
                        packed_rows[r * 320 + col] = samples[r * 160 + col] * window[col];
                    };
                    Blas.cblas_sgemm(101, 111, 111, @intCast(frames), 514, 320, 1, packed_rows.ptr, 320, basis.ptr, 514, 0, complex.ptr, 514);
                    for (output, 0..) |*x, i| x.* = @sqrt(complex[2 * i] * complex[2 * i] + complex[2 * i + 1] * complex[2 * i + 1]);
                } else try magnitudes(samples, frames, 160, &window, &tw, output);
                if (iteration >= 3) total += Clock.ns() - start;
            }
            if (route == 0) @memcpy(reference, output) else for (output, reference) |x, y| try std.testing.expectApproxEqAbs(y, x, 0.0001);
            std.debug.print("spectrum_cpu_release frames={} fft={} completed_ns={} sink={d}\n", .{ frames, route == 1, total / 10, output[0] });
        }
    }
}
