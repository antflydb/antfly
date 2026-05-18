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

pub fn inverseMdct36(input: []const f32, output: []f32) !void {
    if (input.len != 18 or output.len != 36) return error.InvalidDimensions;
    inverseMdctGeneric(18, input, output);
}

pub fn inverseMdct12(input: []const f32, output: []f32) !void {
    if (input.len != 6 or output.len != 12) return error.InvalidDimensions;
    inverseMdctGeneric(6, input, output);
}

pub fn hybridLongBlock(block_type: u2, input: []const f32, output: []f32) !void {
    if (block_type == 2) return error.UnsupportedBlockType;
    try inverseMdct36(input, output);

    const window = longWindow(block_type);
    for (output, window) |*sample, weight| {
        sample.* *= weight;
    }
}

pub fn hybridShortBlock(input: []const f32, output: []f32) !void {
    if (input.len != 18 or output.len != 36) return error.InvalidDimensions;

    @memset(output, 0);

    var short_input: [6]f32 = [_]f32{0} ** 6;
    var short_output: [12]f32 = [_]f32{0} ** 12;
    const window = shortWindow();

    for (0..3) |window_index| {
        for (0..6) |i| {
            short_input[i] = input[i * 3 + window_index];
        }

        try inverseMdct12(short_input[0..], short_output[0..]);

        const base = 6 + (window_index * 6);
        for (0..12) |i| {
            output[base + i] += short_output[i] * window[i];
        }
    }
}

fn inverseMdctGeneric(comptime n: usize, input: []const f32, output: []f32) void {
    const pi = std.math.pi;
    const n_f = @as(f32, @floatFromInt(n));
    const scale = 2.0 / n_f;

    for (0..(n * 2)) |sample_index| {
        const sample_term = @as(f32, @floatFromInt((2 * sample_index) + 1 + n));
        var sum: f32 = 0;

        for (0..n) |coeff_index| {
            const coeff_term = @as(f32, @floatFromInt((2 * coeff_index) + 1));
            const angle = (pi / (4.0 * n_f)) * sample_term * coeff_term;
            sum += input[coeff_index] * @cos(angle);
        }

        output[sample_index] = sum * scale;
    }
}

fn longWindow(block_type: u2) [36]f32 {
    var window: [36]f32 = [_]f32{0} ** 36;
    const pi = std.math.pi;

    switch (block_type) {
        0 => {
            for (0..36) |i| {
                const angle = (pi / 36.0) * @as(f32, @floatFromInt(i)) + (pi / 72.0);
                window[i] = @sin(angle);
            }
        },
        1 => {
            for (0..18) |i| {
                const angle = (pi / 36.0) * @as(f32, @floatFromInt(i)) + (pi / 72.0);
                window[i] = @sin(angle);
            }
            for (18..24) |i| window[i] = 1.0;
            for (24..30) |i| {
                const angle = (pi / 12.0) * @as(f32, @floatFromInt(i - 18)) + (pi / 24.0);
                window[i] = @sin(angle);
            }
        },
        3 => {
            for (6..12) |i| {
                const angle = (pi / 12.0) * @as(f32, @floatFromInt(i - 6)) + (pi / 24.0);
                window[i] = @sin(angle);
            }
            for (12..18) |i| window[i] = 1.0;
            for (18..36) |i| {
                const angle = (pi / 36.0) * @as(f32, @floatFromInt(i)) + (pi / 72.0);
                window[i] = @sin(angle);
            }
        },
        else => unreachable,
    }

    return window;
}

fn shortWindow() [12]f32 {
    var window: [12]f32 = [_]f32{0} ** 12;
    const pi = std.math.pi;

    for (0..12) |i| {
        const angle = (pi / 12.0) * @as(f32, @floatFromInt(i)) + (pi / 24.0);
        window[i] = @sin(angle);
    }

    return window;
}

test "inverse mdct rejects invalid dimensions" {
    var output36: [36]f32 = [_]f32{0} ** 36;
    var output12: [12]f32 = [_]f32{0} ** 12;

    try std.testing.expectError(error.InvalidDimensions, inverseMdct36(&.{ 0, 1 }, output36[0..]));
    try std.testing.expectError(error.InvalidDimensions, inverseMdct12(&.{ 0, 1 }, output12[0..]));
    try std.testing.expectError(error.InvalidDimensions, hybridLongBlock(0, &.{ 0, 1 }, output36[0..]));
    try std.testing.expectError(error.InvalidDimensions, hybridShortBlock(&.{ 0, 1 }, output36[0..]));
}

test "inverse mdct of zero coefficients stays zero" {
    const input36 = [_]f32{0} ** 18;
    const input12 = [_]f32{0} ** 6;
    var output36: [36]f32 = [_]f32{1} ** 36;
    var output12: [12]f32 = [_]f32{1} ** 12;

    try inverseMdct36(input36[0..], output36[0..]);
    try inverseMdct12(input12[0..], output12[0..]);

    for (output36) |sample| try std.testing.expectEqual(@as(f32, 0), sample);
    for (output12) |sample| try std.testing.expectEqual(@as(f32, 0), sample);
}

test "hybrid long start window zeros the trailing tail" {
    var input: [18]f32 = [_]f32{0} ** 18;
    input[0] = 1;

    var output: [36]f32 = undefined;
    try hybridLongBlock(1, input[0..], output[0..]);

    for (output[30..36]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "hybrid long stop window zeros the leading head" {
    var input: [18]f32 = [_]f32{0} ** 18;
    input[0] = 1;

    var output: [36]f32 = undefined;
    try hybridLongBlock(3, input[0..], output[0..]);

    for (output[0..6]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "hybrid short block stays within central 24 samples" {
    const input: [18]f32 = [_]f32{1} ** 18;
    var output: [36]f32 = undefined;

    try hybridShortBlock(input[0..], output[0..]);

    for (output[0..6]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
    for (output[30..36]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }

    var energy: f32 = 0;
    for (output[6..30]) |sample| energy += @abs(sample);
    try std.testing.expect(energy > 0);
}
