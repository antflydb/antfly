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

//! The audio ingress reference uses scipy.signal.resample_poly with its
//! default zero-padded, Kaiser(beta=5), 20*max(up,down)+1 tap low-pass filter.
//! Evaluate only the active polyphase taps; never materialize an upsampled
//! waveform. Keep this policy local to EG2 rather than changing other models.
const std = @import("std");

pub fn resample(allocator: std.mem.Allocator, samples: []const f32, source_rate: u32, target_rate: u32) ![]f32 {
    if (samples.len == 0 or source_rate == 0 or target_rate == 0) return error.InvalidAudioInput;
    for (samples) |sample| if (!std.math.isFinite(sample)) return error.InvalidAudioSamples;
    if (source_rate == target_rate) return allocator.dupe(f32, samples);

    const divisor = std.math.gcd(source_rate, target_rate);
    const up: usize = target_rate / divisor;
    const down: usize = source_rate / divisor;
    const factor = @max(up, down);
    // Bound coefficient storage and filter construction for adversarial WAV
    // sample rates. Standard audio rates through 384 kHz fit this bound.
    if (factor > 48_000) return error.UnsupportedAudioFormat;
    const half_length = 10 * factor;
    const filter_length = 2 * half_length + 1;
    const numerator = std.math.mul(usize, samples.len, up) catch return error.AudioTooLarge;
    const output_length = numerator / down + @intFromBool(numerator % down != 0);
    const output = try allocator.alloc(f32, output_length);
    errdefer allocator.free(output);
    const filter = try allocator.alloc(f32, filter_length);
    defer allocator.free(filter);

    // firwin normalizes in FP64, then resample_poly casts the filter to the
    // waveform dtype before multiplying by the interpolation factor.
    var normalization: f64 = 0;
    for (0..filter_length) |index| normalization += coefficient(index, half_length, factor);
    for (filter, 0..) |*value, index| {
        value.* = @as(f32, @floatCast(coefficient(index, half_length, factor) / normalization)) * @as(f32, @floatFromInt(up));
    }

    // Removing the zero-phase filter delay makes the active filter index
    // i*down + half_length - j*up, including both zero-padded boundaries.
    for (output, 0..) |*value, index| {
        const center = std.math.add(usize, std.math.mul(usize, index, down) catch return error.AudioTooLarge, half_length) catch return error.AudioTooLarge;
        const lower = center -| (filter_length - 1);
        const first = lower / up + @intFromBool(lower % up != 0);
        const last = @min(center / up, samples.len - 1);
        var sum: f32 = 0;
        if (first <= last) for (first..last + 1) |source| {
            sum += samples[source] * filter[center - source * up];
        };
        value.* = sum;
    }
    return output;
}

fn coefficient(index: usize, half_length: usize, factor: usize) f64 {
    const offset = @as(f64, @floatFromInt(index)) - @as(f64, @floatFromInt(half_length));
    const position = offset / @as(f64, @floatFromInt(factor));
    const sinc = if (position == 0) 1.0 else @sin(std.math.pi * position) / (std.math.pi * position);
    const radius = offset / @as(f64, @floatFromInt(half_length));
    const window = besselI0(5.0 * @sqrt(@max(0.0, 1.0 - radius * radius))) / besselI0(5.0);
    return sinc * window / @as(f64, @floatFromInt(factor));
}

fn besselI0(value: f64) f64 {
    const square = value * value / 4;
    var sum: f64 = 1;
    var term: f64 = 1;
    for (1..64) |iteration| {
        const k: f64 = @floatFromInt(iteration);
        term *= square / (k * k);
        sum += term;
        if (term < sum * 1e-16) break;
    }
    return sum;
}

test "embeddinggemma2 resampling matches independent scipy polyphase fixtures" {
    const input = [_]f32{ 0, 0.25, -0.5, 0.75, -1, 0.125, 0.5, -0.25, 0.75, 0, -0.125, 0.25, 0.875, -0.75, 0.5, 0.125, -0.25 };
    // Generated with scipy.signal.resample_poly(float32(input), 16000, rate).
    const cases = .{
        .{ @as(u32, 8000), &[_]f32{ 0, 0.3910512626, 0.2501293719, -0.3882071078, -0.5002587438, 0.2578689754, 0.7503881454, 0.05186596885, -1.000517488, -0.9637690187, 0.1250646859, 0.871479094, 0.5002587438, -0.2308451831, -0.2501293719, 0.3841871023, 0.7503881454, 0.4465270042, 0, -0.1280377954, -0.1250646859, -0.111086525, 0.2501293719, 0.8651272058, 0.8754528165, -0.01428273693, -0.7503881454, -0.3790534735, 0.5002587438, 0.6893428564, 0.1250646859, -0.3182721734, -0.2501293719, -0.04308865964 } },
        .{ @as(u32, 44100), &[_]f32{ 0.07831052691, -0.1218632981, 0.04420349374, 0.2444155216, 0.1604430079, 0.1261138916, -0.0659076944 } },
        .{ @as(u32, 48000), &[_]f32{ 0.06948269159, -0.1272706836, 0.1076226309, 0.2205001563, 0.1664095223, 0.02860498428 } },
    };
    inline for (cases) |case| {
        const actual = try resample(std.testing.allocator, &input, case[0], 16000);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqual(case[1].len, actual.len);
        for (actual, case[1]) |value, expected| try std.testing.expectApproxEqAbs(expected, value, 3e-7);
    }
}

test "embeddinggemma2 resampling rejects invalid inputs and preserves native samples" {
    try std.testing.expectError(error.InvalidAudioInput, resample(std.testing.allocator, &.{}, 16000, 16000));
    try std.testing.expectError(error.InvalidAudioInput, resample(std.testing.allocator, &.{1}, 0, 16000));
    try std.testing.expectError(error.InvalidAudioSamples, resample(std.testing.allocator, &.{std.math.nan(f32)}, 16000, 16000));
    try std.testing.expectError(error.UnsupportedAudioFormat, resample(std.testing.allocator, &.{1}, 0xffff_ffff, 16000));
    const native = try resample(std.testing.allocator, &.{ 0.25, -0.5 }, 16000, 16000);
    defer std.testing.allocator.free(native);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, -0.5 }, native);
}

test "embeddinggemma2 downsampling rejects energy above destination Nyquist" {
    var input: [4800]f32 = undefined;
    for (&input, 0..) |*value, index| value.* = @floatCast(@sin(2.0 * std.math.pi * 12000.0 * @as(f64, @floatFromInt(index)) / 48000.0));
    const output = try resample(std.testing.allocator, &input, 48000, 16000);
    defer std.testing.allocator.free(output);
    var energy: f64 = 0;
    for (output[20 .. output.len - 20]) |value| energy += value * value;
    try std.testing.expect(@sqrt(energy / @as(f64, @floatFromInt(output.len - 40))) < 0.001);
}
