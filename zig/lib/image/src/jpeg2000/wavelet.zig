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

pub const native_port_available = true;

fn divFloorI32(numerator: i32, denominator: i32) i32 {
    return @intCast(@divFloor(numerator, denominator));
}

pub fn inverse53Line(allocator: std.mem.Allocator, low: []const i32, high: []const i32) ![]i32 {
    const len = low.len + high.len;
    const out = try allocator.alloc(i32, len);
    errdefer allocator.free(out);
    try inverse53LineInto(out, low, high);
    return out;
}

pub fn inverse53LineInto(out: []i32, low: []const i32, high: []const i32) !void {
    return inverse53LineIntoPhase(out, low, high, 0);
}

pub fn inverse53LineIntoPhase(out: []i32, low: []const i32, high: []const i32, phase: u1) !void {
    const even = try std.heap.page_allocator.alloc(i32, low.len);
    defer std.heap.page_allocator.free(even);
    return inverse53LineIntoPhaseScratch(out, low, high, phase, even);
}

fn inverse53LineIntoPhaseScratch(out: []i32, low: []const i32, high: []const i32, phase: u1, even_scratch: []i32) !void {
    if (out.len != low.len + high.len) return error.InvalidWaveletBufferShape;
    if (phase == 0 and low.len == 0) return error.InvalidWaveletBufferShape;
    if (phase == 1 and high.len == 0) return error.InvalidWaveletBufferShape;
    if (even_scratch.len < low.len) return error.InvalidWaveletBufferShape;

    if (high.len == 0) {
        out[0] = low[0];
        return;
    }
    if (low.len == 0) {
        out[0] = @divTrunc(high[0], 2);
        return;
    }

    const even = even_scratch[0..low.len];

    for (low, 0..) |sample, i| {
        const left = if (phase == 0)
            high[if (i == 0) 0 else i - 1]
        else
            high[@min(i, high.len - 1)];
        const right = if (phase == 0)
            high[if (i < high.len) i else high.len - 1]
        else
            high[if (i + 1 < high.len) i + 1 else high.len - 1];
        even[i] = sample - divFloorI32(left + right + 2, 4);
    }
    if (phase == 0) {
        for (even, 0..) |sample, i| out[i * 2] = sample;
    } else {
        for (even, 0..) |sample, i| out[i * 2 + 1] = sample;
    }
    for (high, 0..) |sample, i| {
        const left = if (phase == 0)
            even[@min(i, even.len - 1)]
        else
            even[if (i == 0) 0 else i - 1];
        const right = if (phase == 0)
            even[if (i + 1 < even.len) i + 1 else even.len - 1]
        else
            even[@min(i, even.len - 1)];
        const reconstructed = sample + divFloorI32(left + right, 2);
        if (phase == 0) {
            out[i * 2 + 1] = reconstructed;
        } else {
            out[i * 2] = reconstructed;
        }
    }
}

pub fn inverse53LevelInPlace(allocator: std.mem.Allocator, data: []i32, width: usize, height: usize) !void {
    return inverse53LevelInPlacePhase(allocator, data, width, height, 0, 0);
}

pub fn inverse53LevelInPlacePhase(allocator: std.mem.Allocator, data: []i32, width: usize, height: usize, phase_x: u1, phase_y: u1) !void {
    if (data.len != width * height or width == 0 or height == 0) return error.InvalidWaveletBufferShape;

    const low_w = if (phase_x == 0) (width + 1) / 2 else width / 2;
    const high_w = width - low_w;
    const low_h = if (phase_y == 0) (height + 1) / 2 else height / 2;
    const high_h = height - low_h;

    var temp = try allocator.alloc(i32, data.len);
    defer allocator.free(temp);
    var low_col = try allocator.alloc(i32, low_h);
    defer allocator.free(low_col);
    var high_col = try allocator.alloc(i32, high_h);
    defer allocator.free(high_col);
    const out_col = try allocator.alloc(i32, height);
    defer allocator.free(out_col);
    const low_row = try allocator.alloc(i32, low_w);
    defer allocator.free(low_row);
    const high_row = try allocator.alloc(i32, high_w);
    defer allocator.free(high_row);
    const out_row = try allocator.alloc(i32, width);
    defer allocator.free(out_row);
    const line_even_scratch = try allocator.alloc(i32, @max(low_w, low_h));
    defer allocator.free(line_even_scratch);

    var y: usize = 0;
    while (y < height) : (y += 1) {
        @memcpy(low_row, data[y * width .. y * width + low_w]);
        @memcpy(high_row, data[y * width + low_w .. y * width + width]);
        try inverse53LineIntoPhaseScratch(out_row, low_row, high_row, phase_x, line_even_scratch);
        @memcpy(temp[y * width .. y * width + width], out_row);
    }

    var x: usize = 0;
    while (x < width) : (x += 1) {
        y = 0;
        while (y < low_h) : (y += 1) low_col[y] = temp[y * width + x];
        y = 0;
        while (y < high_h) : (y += 1) high_col[y] = temp[(low_h + y) * width + x];
        try inverse53LineIntoPhaseScratch(out_col, low_col, high_col, phase_y, line_even_scratch);
        y = 0;
        while (y < height) : (y += 1) data[y * width + x] = out_col[y];
    }
}

fn forward53LineForTest(allocator: std.mem.Allocator, input: []const i32) !struct { low: []i32, high: []i32 } {
    const low_len = (input.len + 1) / 2;
    const high_len = input.len / 2;
    const even = try allocator.alloc(i32, low_len);
    errdefer allocator.free(even);
    const odd = try allocator.alloc(i32, high_len);
    errdefer allocator.free(odd);

    for (even, 0..) |*sample, i| sample.* = input[i * 2];
    for (odd, 0..) |*sample, i| sample.* = input[i * 2 + 1];

    for (odd, 0..) |*sample, i| {
        const left = even[i];
        const right = even[if (i + 1 < even.len) i + 1 else i];
        sample.* -= divFloorI32(left + right, 2);
    }
    for (even, 0..) |*sample, i| {
        if (odd.len == 0) break;
        const left = odd[if (i == 0) 0 else i - 1];
        const right = odd[if (i < odd.len) i else odd.len - 1];
        sample.* += divFloorI32(left + right + 2, 4);
    }

    return .{ .low = even, .high = odd };
}

pub fn forward53Level(allocator: std.mem.Allocator, input: []const i32, width: usize, height: usize) ![]i32 {
    if (input.len != width * height or width == 0 or height == 0) return error.InvalidWaveletBufferShape;

    const low_w = (width + 1) / 2;
    const high_w = width / 2;
    const low_h = (height + 1) / 2;
    const high_h = height / 2;

    var temp = try allocator.alloc(i32, input.len);
    defer allocator.free(temp);
    const out = try allocator.alloc(i32, input.len);
    errdefer allocator.free(out);
    var col = try allocator.alloc(i32, height);
    defer allocator.free(col);

    var x: usize = 0;
    while (x < width) : (x += 1) {
        var y: usize = 0;
        while (y < height) : (y += 1) col[y] = input[y * width + x];
        const coeffs = try forward53LineForTest(allocator, col[0..height]);
        defer allocator.free(coeffs.low);
        defer allocator.free(coeffs.high);
        y = 0;
        while (y < low_h) : (y += 1) temp[y * width + x] = coeffs.low[y];
        y = 0;
        while (y < high_h) : (y += 1) temp[(low_h + y) * width + x] = coeffs.high[y];
    }

    var y: usize = 0;
    while (y < height) : (y += 1) {
        const row = temp[y * width .. y * width + width];
        const coeffs = try forward53LineForTest(allocator, row);
        defer allocator.free(coeffs.low);
        defer allocator.free(coeffs.high);
        @memcpy(out[y * width .. y * width + low_w], coeffs.low);
        @memcpy(out[y * width + low_w .. y * width + width], coeffs.high);
    }

    _ = high_w;
    return out;
}

// CDF 9/7 irreversible wavelet (f32 floating-point arithmetic)

const cdf97_alpha: f32 = -1.586134342;
const cdf97_beta: f32 = -0.052980118;
const cdf97_gamma: f32 = 0.882911076;
const cdf97_delta: f32 = 0.443506852;
const cdf97_K: f32 = 1.230174105;

pub fn inverse97LineInto(out: []f32, low: []const f32, high: []const f32) !void {
    return inverse97LineIntoPhase(out, low, high, 0);
}

fn lift97(target: []f32, source: []const f32, coeff: f32, phase: u1) void {
    for (target, 0..) |*sample, i| {
        const left_index = if (phase == 0) i else if (i == 0) 0 else i - 1;
        const right_index = if (phase == 0) i + 1 else i;
        const left = source[@min(left_index, source.len - 1)];
        const right = source[@min(right_index, source.len - 1)];
        sample.* -= coeff * (left + right);
    }
}

pub fn inverse97LineIntoPhase(out: []f32, low: []const f32, high: []const f32, phase: u1) !void {
    return inverse97LineIntoPhaseWithScaling(out, low, high, phase, false);
}

pub fn inverse97LineIntoPhaseOpenJpeg(out: []f32, low: []const f32, high: []const f32, phase: u1) !void {
    return inverse97LineIntoPhaseWithScaling(out, low, high, phase, true);
}

fn inverse97LineIntoPhaseWithScaling(out: []f32, low: []const f32, high: []const f32, phase: u1, openjpeg_highpass_scaling: bool) !void {
    const even = try std.heap.page_allocator.alloc(f32, low.len);
    defer std.heap.page_allocator.free(even);
    const odd = try std.heap.page_allocator.alloc(f32, high.len);
    defer std.heap.page_allocator.free(odd);
    return inverse97LineIntoPhaseWithScalingScratch(out, low, high, phase, openjpeg_highpass_scaling, even, odd);
}

fn inverse97LineIntoPhaseWithScalingScratch(
    out: []f32,
    low: []const f32,
    high: []const f32,
    phase: u1,
    openjpeg_highpass_scaling: bool,
    even_scratch: []f32,
    odd_scratch: []f32,
) !void {
    if (out.len != low.len + high.len) return error.InvalidWaveletBufferShape;
    if (phase == 0 and low.len == 0) return error.InvalidWaveletBufferShape;
    if (phase == 1 and high.len == 0) return error.InvalidWaveletBufferShape;
    if (even_scratch.len < low.len or odd_scratch.len < high.len) return error.InvalidWaveletBufferShape;

    if (out.len <= 1) {
        out[0] = if (low.len != 0) low[0] else high[0];
        return;
    }

    const high_scale: f32 = if (openjpeg_highpass_scaling) 2.0 / cdf97_K else 1.0 / cdf97_K;
    if (high.len == 0) {
        out[0] = low[0] * cdf97_K;
        return;
    }
    if (low.len == 0) {
        out[0] = high[0] * high_scale;
        return;
    }

    const even = even_scratch[0..low.len];
    const odd = odd_scratch[0..high.len];

    // Step 1: Undo scaling
    for (low, 0..) |sample, i| even[i] = sample * cdf97_K;
    for (high, 0..) |sample, i| odd[i] = sample * high_scale;

    // Step 2: Undo delta lifting on even samples
    lift97(even, odd, cdf97_delta, 1 - phase);

    // Step 3: Undo gamma lifting on odd samples
    lift97(odd, even, cdf97_gamma, phase);

    // Step 4: Undo beta lifting on even samples
    lift97(even, odd, cdf97_beta, 1 - phase);

    // Step 5: Undo alpha lifting on odd samples
    lift97(odd, even, cdf97_alpha, phase);

    // Step 6: Interleave
    if (phase == 0) {
        for (even, 0..) |sample, i| out[i * 2] = sample;
        for (odd, 0..) |sample, i| out[i * 2 + 1] = sample;
    } else {
        for (odd, 0..) |sample, i| out[i * 2] = sample;
        for (even, 0..) |sample, i| out[i * 2 + 1] = sample;
    }
}

pub fn inverse97LevelInPlace(allocator: std.mem.Allocator, data: []f32, width: usize, height: usize) !void {
    return inverse97LevelInPlacePhase(allocator, data, width, height, 0, 0);
}

pub fn inverse97LevelInPlacePhase(allocator: std.mem.Allocator, data: []f32, width: usize, height: usize, phase_x: u1, phase_y: u1) !void {
    return inverse97LevelInPlacePhaseWithScaling(allocator, data, width, height, phase_x, phase_y, false);
}

pub fn inverse97LevelInPlacePhaseOpenJpeg(allocator: std.mem.Allocator, data: []f32, width: usize, height: usize, phase_x: u1, phase_y: u1) !void {
    return inverse97LevelInPlacePhaseWithScaling(allocator, data, width, height, phase_x, phase_y, true);
}

fn inverse97LevelInPlacePhaseWithScaling(allocator: std.mem.Allocator, data: []f32, width: usize, height: usize, phase_x: u1, phase_y: u1, openjpeg_highpass_scaling: bool) !void {
    if (data.len != width * height or width == 0 or height == 0) return error.InvalidWaveletBufferShape;

    const low_w = if (phase_x == 0) (width + 1) / 2 else width / 2;
    const high_w = width - low_w;
    const low_h = if (phase_y == 0) (height + 1) / 2 else height / 2;
    const high_h = height - low_h;

    var temp = try allocator.alloc(f32, data.len);
    defer allocator.free(temp);
    var low_col = try allocator.alloc(f32, low_h);
    defer allocator.free(low_col);
    var high_col = try allocator.alloc(f32, high_h);
    defer allocator.free(high_col);
    const out_col = try allocator.alloc(f32, height);
    defer allocator.free(out_col);
    const low_row = try allocator.alloc(f32, low_w);
    defer allocator.free(low_row);
    const high_row = try allocator.alloc(f32, high_w);
    defer allocator.free(high_row);
    const out_row = try allocator.alloc(f32, width);
    defer allocator.free(out_row);
    const line_even_scratch = try allocator.alloc(f32, @max(low_w, low_h));
    defer allocator.free(line_even_scratch);
    const line_odd_scratch = try allocator.alloc(f32, @max(high_w, high_h));
    defer allocator.free(line_odd_scratch);

    var y: usize = 0;
    while (y < height) : (y += 1) {
        @memcpy(low_row, data[y * width .. y * width + low_w]);
        @memcpy(high_row, data[y * width + low_w .. y * width + width]);
        try inverse97LineIntoPhaseWithScalingScratch(
            out_row,
            low_row,
            high_row,
            phase_x,
            openjpeg_highpass_scaling,
            line_even_scratch,
            line_odd_scratch,
        );
        @memcpy(temp[y * width .. y * width + width], out_row);
    }

    var x: usize = 0;
    while (x < width) : (x += 1) {
        y = 0;
        while (y < low_h) : (y += 1) low_col[y] = temp[y * width + x];
        y = 0;
        while (y < high_h) : (y += 1) high_col[y] = temp[(low_h + y) * width + x];
        try inverse97LineIntoPhaseWithScalingScratch(
            out_col,
            low_col,
            high_col,
            phase_y,
            openjpeg_highpass_scaling,
            line_even_scratch,
            line_odd_scratch,
        );
        y = 0;
        while (y < height) : (y += 1) data[y * width + x] = out_col[y];
    }
}

/// Forward 9/7 CDF irreversible wavelet on a single line of f32 samples.
/// Returns the low-pass (even) and high-pass (odd) coefficients for the caller to manage.
pub fn forward97Line(allocator: std.mem.Allocator, input: []const f32) !struct { low: []f32, high: []f32 } {
    return forward97LineForTest(allocator, input);
}

/// One level of the forward 9/7 wavelet decomposition on a 2D region of f32 data.
/// Output layout (same size as input): LL top-left, HL top-right, LH bottom-left, HH bottom-right.
pub fn forward97Level(allocator: std.mem.Allocator, input: []const f32, width: usize, height: usize) ![]f32 {
    if (input.len != width * height or width == 0 or height == 0) return error.InvalidWaveletBufferShape;

    const low_w = (width + 1) / 2;
    const high_w = width / 2;
    const low_h = (height + 1) / 2;
    const high_h = height / 2;

    var temp = try allocator.alloc(f32, input.len);
    defer allocator.free(temp);
    const out = try allocator.alloc(f32, input.len);
    errdefer allocator.free(out);
    var col = try allocator.alloc(f32, height);
    defer allocator.free(col);

    var x: usize = 0;
    while (x < width) : (x += 1) {
        var y: usize = 0;
        while (y < height) : (y += 1) col[y] = input[y * width + x];
        const coeffs = try forward97LineForTest(allocator, col[0..height]);
        defer allocator.free(coeffs.low);
        defer allocator.free(coeffs.high);
        y = 0;
        while (y < low_h) : (y += 1) temp[y * width + x] = coeffs.low[y];
        y = 0;
        while (y < high_h) : (y += 1) temp[(low_h + y) * width + x] = coeffs.high[y];
    }

    var y: usize = 0;
    while (y < height) : (y += 1) {
        const row = temp[y * width .. y * width + width];
        const coeffs = try forward97LineForTest(allocator, row);
        defer allocator.free(coeffs.low);
        defer allocator.free(coeffs.high);
        @memcpy(out[y * width .. y * width + low_w], coeffs.low);
        @memcpy(out[y * width + low_w .. y * width + width], coeffs.high);
    }

    _ = high_w;
    return out;
}

fn forward97LineForTest(allocator: std.mem.Allocator, input: []const f32) !struct { low: []f32, high: []f32 } {
    const low_len = (input.len + 1) / 2;
    const high_len = input.len / 2;
    const even = try allocator.alloc(f32, low_len);
    errdefer allocator.free(even);
    const odd = try allocator.alloc(f32, high_len);
    errdefer allocator.free(odd);

    for (even, 0..) |*sample, i| sample.* = input[i * 2];
    for (odd, 0..) |*sample, i| sample.* = input[i * 2 + 1];

    // Step 1: alpha lifting on odd samples
    for (odd, 0..) |*sample, i| {
        const left = even[i];
        const right = even[if (i + 1 < even.len) i + 1 else i];
        sample.* += cdf97_alpha * (left + right);
    }

    // Step 2: beta lifting on even samples
    for (even, 0..) |*sample, i| {
        if (odd.len == 0) break;
        const left = odd[if (i == 0) 0 else i - 1];
        const right = odd[if (i < odd.len) i else odd.len - 1];
        sample.* += cdf97_beta * (left + right);
    }

    // Step 3: gamma lifting on odd samples
    for (odd, 0..) |*sample, i| {
        const left = even[i];
        const right = even[if (i + 1 < even.len) i + 1 else i];
        sample.* += cdf97_gamma * (left + right);
    }

    // Step 4: delta lifting on even samples
    for (even, 0..) |*sample, i| {
        if (odd.len == 0) break;
        const left = odd[if (i == 0) 0 else i - 1];
        const right = odd[if (i < odd.len) i else odd.len - 1];
        sample.* += cdf97_delta * (left + right);
    }

    // Step 5: Scaling (ISO 15444-1 F.4.8.2 steps 5/6): high-pass (odd) *= K,
    // low-pass (even) *= 1/K. Undone by the reciprocal scaling in the inverse.
    for (even) |*sample| sample.* /= cdf97_K;
    for (odd) |*sample| sample.* *= cdf97_K;

    return .{ .low = even, .high = odd };
}

test "inverse 5/3 line round trips odd length input" {
    const allocator = std.testing.allocator;
    const source = [_]i32{ 12, 16, 18, 20, 21 };
    const coeffs = try forward53LineForTest(allocator, &source);
    defer allocator.free(coeffs.low);
    defer allocator.free(coeffs.high);

    const reconstructed = try inverse53Line(allocator, coeffs.low, coeffs.high);
    defer allocator.free(reconstructed);
    try std.testing.expectEqualSlices(i32, &source, reconstructed);
}

test "inverse 5/3 line round trips even length input" {
    const allocator = std.testing.allocator;
    const source = [_]i32{ 4, 8, 15, 16, 23, 42 };
    const coeffs = try forward53LineForTest(allocator, &source);
    defer allocator.free(coeffs.low);
    defer allocator.free(coeffs.high);

    const reconstructed = try inverse53Line(allocator, coeffs.low, coeffs.high);
    defer allocator.free(reconstructed);
    try std.testing.expectEqualSlices(i32, &source, reconstructed);
}

test "inverse 5/3 level in place round trips one level layout" {
    const allocator = std.testing.allocator;
    const source = [_]i32{
        12, 16,
        18, 20,
    };
    const coeffs = try forward53Level(allocator, &source, 2, 2);
    defer allocator.free(coeffs);

    try inverse53LevelInPlace(allocator, coeffs, 2, 2);
    try std.testing.expectEqualSlices(i32, &source, coeffs);
}

test "forward 5/3 level round trips with inverse on 3x3 input" {
    const allocator = std.testing.allocator;
    const source = [_]i32{
        127,  0,   -64,
        -96,  64,  -128,
        -112, 112, -80,
    };
    const coeffs = try forward53Level(allocator, &source, 3, 3);
    defer allocator.free(coeffs);

    try inverse53LevelInPlace(allocator, coeffs, 3, 3);
    try std.testing.expectEqualSlices(i32, &source, coeffs);
}

test "multi-level 5/3 round trips on 8x8 input with 3 decomposition levels" {
    const allocator = std.testing.allocator;
    const source = [_]i32{
        100,  110, 120, 130, 140, 150, 160,  170,
        90,   80,  70,  60,  50,  40,  30,   20,
        -10,  -20, -30, -40, -50, -60, -70,  -80,
        5,    15,  25,  35,  45,  55,  65,   75,
        127,  0,   -64, 32,  -96, 64,  -128, 96,
        -112, 112, -80, 48,  -16, 80,  -48,  16,
        88,   -88, 44,  -44, 22,  -22, 11,   -11,
        1,    2,   3,   4,   5,   6,   7,    8,
    };
    const width: usize = 8;
    const height: usize = 8;

    // Apply 3 levels of forward DWT
    var current = try allocator.dupe(i32, &source);
    defer allocator.free(current);

    // Level 1: full 8x8
    const level1 = try forward53Level(allocator, current, width, height);
    @memcpy(current, level1);
    allocator.free(level1);

    // Level 2: top-left 4x4 (the LL subband of level 1)
    const low_w1: usize = (width + 1) / 2;
    const low_h1: usize = (height + 1) / 2;
    var sub2 = try allocator.alloc(i32, low_w1 * low_h1);
    defer allocator.free(sub2);
    for (0..low_h1) |y| @memcpy(sub2[y * low_w1 .. y * low_w1 + low_w1], current[y * width .. y * width + low_w1]);
    const level2 = try forward53Level(allocator, sub2, low_w1, low_h1);
    defer allocator.free(level2);
    for (0..low_h1) |y| @memcpy(current[y * width .. y * width + low_w1], level2[y * low_w1 .. y * low_w1 + low_w1]);

    // Level 3: top-left 2x2 (the LL subband of level 2)
    const low_w2: usize = (low_w1 + 1) / 2;
    const low_h2: usize = (low_h1 + 1) / 2;
    var sub3 = try allocator.alloc(i32, low_w2 * low_h2);
    defer allocator.free(sub3);
    for (0..low_h2) |y| @memcpy(sub3[y * low_w2 .. y * low_w2 + low_w2], current[y * width .. y * width + low_w2]);
    const level3 = try forward53Level(allocator, sub3, low_w2, low_h2);
    defer allocator.free(level3);
    for (0..low_h2) |y| @memcpy(current[y * width .. y * width + low_w2], level3[y * low_w2 .. y * low_w2 + low_w2]);

    // Now apply 3 levels of inverse DWT (smallest to largest)
    // Level 3 inverse: 2x2
    for (0..low_h2) |y| @memcpy(sub3[y * low_w2 .. y * low_w2 + low_w2], current[y * width .. y * width + low_w2]);
    try inverse53LevelInPlace(allocator, sub3, low_w2, low_h2);
    for (0..low_h2) |y| @memcpy(current[y * width .. y * width + low_w2], sub3[y * low_w2 .. y * low_w2 + low_w2]);

    // Level 2 inverse: 4x4
    for (0..low_h1) |y| @memcpy(sub2[y * low_w1 .. y * low_w1 + low_w1], current[y * width .. y * width + low_w1]);
    try inverse53LevelInPlace(allocator, sub2, low_w1, low_h1);
    for (0..low_h1) |y| @memcpy(current[y * width .. y * width + low_w1], sub2[y * low_w1 .. y * low_w1 + low_w1]);

    // Level 1 inverse: full 8x8
    try inverse53LevelInPlace(allocator, current, width, height);

    try std.testing.expectEqualSlices(i32, &source, current);
}

test "inverse 9/7 line round trips odd length input" {
    const allocator = std.testing.allocator;
    const source = [_]f32{ 12.0, 16.0, 18.0, 20.0, 21.0 };
    const coeffs = try forward97LineForTest(allocator, &source);
    defer allocator.free(coeffs.low);
    defer allocator.free(coeffs.high);

    const reconstructed = try allocator.alloc(f32, source.len);
    defer allocator.free(reconstructed);
    try inverse97LineInto(reconstructed, coeffs.low, coeffs.high);
    for (source, reconstructed) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
    }
}

test "inverse 9/7 line round trips even length input" {
    const allocator = std.testing.allocator;
    const source = [_]f32{ 4.0, 8.0, 15.0, 16.0, 23.0, 42.0 };
    const coeffs = try forward97LineForTest(allocator, &source);
    defer allocator.free(coeffs.low);
    defer allocator.free(coeffs.high);

    const reconstructed = try allocator.alloc(f32, source.len);
    defer allocator.free(reconstructed);
    try inverse97LineInto(reconstructed, coeffs.low, coeffs.high);
    for (source, reconstructed) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
    }
}

test "inverse 9/7 line handles single sample" {
    const allocator = std.testing.allocator;
    const coeffs = [_]f32{42.0};
    const reconstructed = try allocator.alloc(f32, coeffs.len);
    defer allocator.free(reconstructed);
    try inverse97LineInto(reconstructed, &coeffs, &.{});
    try std.testing.expectApproxEqAbs(coeffs[0], reconstructed[0], 1e-4);
}

test "DIAG 9/7 multi-level forward coefficient magnitudes" {
    const allocator = std.testing.allocator;
    const w: usize = 32;
    const h: usize = 32;
    const input_value: f32 = 128.0;
    const src = try allocator.alloc(f32, w * h);
    defer allocator.free(src);
    @memset(src, input_value);

    // Level 1
    const coeffs1 = try forward97Level(allocator, src, w, h);
    defer allocator.free(coeffs1);
    // LL of level 1
    const ll1_w: usize = (w + 1) / 2;
    const ll1_h: usize = (h + 1) / 2;
    var sum1: f64 = 0;
    for (0..ll1_h) |y| {
        for (0..ll1_w) |x| sum1 += @as(f64, coeffs1[y * w + x]);
    }
    std.debug.print(
        "\n[DIAG-ML] LL @ L=1: mean={d:.3} (norms[0][1]=1.965, expected ={d:.3})\n",
        .{ sum1 / @as(f64, @floatFromInt(ll1_w * ll1_h)), input_value * 1.965 },
    );

    // Extract LL1 and apply level 2
    const ll1 = try allocator.alloc(f32, ll1_w * ll1_h);
    defer allocator.free(ll1);
    for (0..ll1_h) |y| {
        @memcpy(ll1[y * ll1_w .. y * ll1_w + ll1_w], coeffs1[y * w .. y * w + ll1_w]);
    }
    const coeffs2 = try forward97Level(allocator, ll1, ll1_w, ll1_h);
    defer allocator.free(coeffs2);
    const ll2_w: usize = (ll1_w + 1) / 2;
    const ll2_h: usize = (ll1_h + 1) / 2;
    var sum2: f64 = 0;
    for (0..ll2_h) |y| {
        for (0..ll2_w) |x| sum2 += @as(f64, coeffs2[y * ll1_w + x]);
    }
    std.debug.print(
        "[DIAG-ML] LL @ L=2: mean={d:.3} (norms[0][2]=4.177, expected ={d:.3})\n",
        .{ sum2 / @as(f64, @floatFromInt(ll2_w * ll2_h)), input_value * 4.177 },
    );

    // Level 3
    const ll2 = try allocator.alloc(f32, ll2_w * ll2_h);
    defer allocator.free(ll2);
    for (0..ll2_h) |y| {
        @memcpy(ll2[y * ll2_w .. y * ll2_w + ll2_w], coeffs2[y * ll1_w .. y * ll1_w + ll2_w]);
    }
    const coeffs3 = try forward97Level(allocator, ll2, ll2_w, ll2_h);
    defer allocator.free(coeffs3);
    const ll3_w: usize = (ll2_w + 1) / 2;
    const ll3_h: usize = (ll2_h + 1) / 2;
    var sum3: f64 = 0;
    for (0..ll3_h) |y| {
        for (0..ll3_w) |x| sum3 += @as(f64, coeffs3[y * ll2_w + x]);
    }
    std.debug.print(
        "[DIAG-ML] LL @ L=3: mean={d:.3} (norms[0][3]=8.403, expected ={d:.3})\n",
        .{ sum3 / @as(f64, @floatFromInt(ll3_w * ll3_h)), input_value * 8.403 },
    );
}

test "DIAG 9/7 forward coefficient magnitudes" {
    const allocator = std.testing.allocator;
    // 8x8 constant-value input (post level-shift). LL at deepest level should be
    // input_value * synthesis_norm (per ISO F.4.8.2 scaling conventions).
    const w: usize = 8;
    const h: usize = 8;
    const input_value: f32 = 128.0;
    const src = try allocator.alloc(f32, w * h);
    defer allocator.free(src);
    @memset(src, input_value);

    const coeffs = try forward97Level(allocator, src, w, h);
    defer allocator.free(coeffs);

    // LL-level-1 lives in top-left low_w × low_h quadrant after 1 level.
    const low_w: usize = (w + 1) / 2;
    const low_h: usize = (h + 1) / 2;
    std.debug.print("\n[DIAG] 9/7 forward on 8x8 const-{d} input:\n", .{input_value});
    var sum_ll: f64 = 0;
    for (0..low_h) |y| {
        for (0..low_w) |x| {
            sum_ll += @as(f64, coeffs[y * w + x]);
        }
    }
    std.debug.print(
        "  LL mean = {d:.3} (expected per opj: input × synthesis_norm for 1 level ≈ {d:.3})\n",
        .{ sum_ll / @as(f64, @floatFromInt(low_w * low_h)), input_value * 1.965 },
    );

    // Also HH (bottom-right).
    var sum_hh: f64 = 0;
    const cnt_hh: usize = (w - low_w) * (h - low_h);
    for (low_h..h) |y| {
        for (low_w..w) |x| {
            sum_hh += @abs(@as(f64, coeffs[y * w + x]));
        }
    }
    if (cnt_hh > 0) std.debug.print("  HH |mean| = {d:.3}\n", .{sum_hh / @as(f64, @floatFromInt(cnt_hh))});
}

test "inverse 9/7 level in place round trips 4x4 input" {
    const allocator = std.testing.allocator;
    const source = [_]f32{
        12.0, 16.0,  18.0,  20.0,
        30.0, 25.0,  15.0,  10.0,
        -5.0, -10.0, -15.0, -20.0,
        8.0,  12.0,  16.0,  20.0,
    };
    const width: usize = 4;
    const height: usize = 4;

    // Forward transform: rows then columns
    var temp = try allocator.alloc(f32, source.len);
    defer allocator.free(temp);

    const low_w = (width + 1) / 2;
    const low_h = (height + 1) / 2;
    const high_h = height / 2;

    // Forward rows
    var y: usize = 0;
    while (y < height) : (y += 1) {
        const coeffs = try forward97LineForTest(allocator, source[y * width .. y * width + width]);
        defer allocator.free(coeffs.low);
        defer allocator.free(coeffs.high);
        @memcpy(temp[y * width .. y * width + low_w], coeffs.low);
        @memcpy(temp[y * width + low_w .. y * width + width], coeffs.high);
    }

    // Forward columns
    var data = try allocator.alloc(f32, source.len);
    defer allocator.free(data);
    var col = try allocator.alloc(f32, height);
    defer allocator.free(col);
    var x: usize = 0;
    while (x < width) : (x += 1) {
        y = 0;
        while (y < height) : (y += 1) col[y] = temp[y * width + x];
        const coeffs = try forward97LineForTest(allocator, col[0..height]);
        defer allocator.free(coeffs.low);
        defer allocator.free(coeffs.high);
        y = 0;
        while (y < low_h) : (y += 1) data[y * width + x] = coeffs.low[y];
        y = 0;
        while (y < high_h) : (y += 1) data[(low_h + y) * width + x] = coeffs.high[y];
    }

    // Inverse transform
    try inverse97LevelInPlace(allocator, data, width, height);

    for (source, data) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-3);
    }
}
