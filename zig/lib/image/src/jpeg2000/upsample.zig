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

/// Upsample a plane using nearest-neighbor interpolation.
/// Input plane has dimensions (in_width x in_height).
/// Output plane has dimensions (out_width x out_height).
/// Scale factors: x_factor = out_width / in_width, y_factor = out_height / in_height (integer ratios)
pub fn nearestNeighborI32(
    allocator: std.mem.Allocator,
    input: []const i32,
    in_width: usize,
    in_height: usize,
    out_width: usize,
    out_height: usize,
) ![]i32 {
    const out = try allocator.alloc(i32, out_width * out_height);
    errdefer allocator.free(out);
    var y: usize = 0;
    while (y < out_height) : (y += 1) {
        const src_y = y * in_height / out_height;
        var x: usize = 0;
        while (x < out_width) : (x += 1) {
            const src_x = x * in_width / out_width;
            out[y * out_width + x] = input[src_y * in_width + src_x];
        }
    }
    return out;
}

/// Same as nearestNeighborI32 but for f32 planes (irreversible pipeline).
pub fn nearestNeighborF32(
    allocator: std.mem.Allocator,
    input: []const f32,
    in_width: usize,
    in_height: usize,
    out_width: usize,
    out_height: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, out_width * out_height);
    errdefer allocator.free(out);
    var y: usize = 0;
    while (y < out_height) : (y += 1) {
        const src_y = y * in_height / out_height;
        var x: usize = 0;
        while (x < out_width) : (x += 1) {
            const src_x = x * in_width / out_width;
            out[y * out_width + x] = input[src_y * in_width + src_x];
        }
    }
    return out;
}

test "nearest neighbor i32 2x2 to 4x4" {
    const allocator = std.testing.allocator;
    const input = [_]i32{ 1, 2, 3, 4 };
    const result = try nearestNeighborI32(allocator, &input, 2, 2, 4, 4);
    defer allocator.free(result);

    const expected = [_]i32{
        1, 1, 2, 2,
        1, 1, 2, 2,
        3, 3, 4, 4,
        3, 3, 4, 4,
    };
    try std.testing.expectEqualSlices(i32, &expected, result);
}

test "nearest neighbor i32 2x1 to 4x2" {
    const allocator = std.testing.allocator;
    const input = [_]i32{ 10, 20 };
    const result = try nearestNeighborI32(allocator, &input, 2, 1, 4, 2);
    defer allocator.free(result);

    const expected = [_]i32{
        10, 10, 20, 20,
        10, 10, 20, 20,
    };
    try std.testing.expectEqualSlices(i32, &expected, result);
}

test "nearest neighbor i32 1x1 to 3x3" {
    const allocator = std.testing.allocator;
    const input = [_]i32{42};
    const result = try nearestNeighborI32(allocator, &input, 1, 1, 3, 3);
    defer allocator.free(result);

    for (result) |val| {
        try std.testing.expectEqual(@as(i32, 42), val);
    }
}

// ---------------------------------------------------------------------------
// Bilinear upsampling
// ---------------------------------------------------------------------------

inline fn clampISize(v: isize, lo: isize, hi: isize) isize {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

/// Bilinear upsampling for i32 planes. Rounds-to-nearest on output.
/// Uses the common "pixel-center" mapping:
///   src_x = (x + 0.5) * in_w / out_w - 0.5
/// Coordinates are clamped at the edges.
pub fn bilinearI32(
    allocator: std.mem.Allocator,
    input: []const i32,
    in_width: usize,
    in_height: usize,
    out_width: usize,
    out_height: usize,
) ![]i32 {
    std.debug.assert(in_width > 0 and in_height > 0);
    std.debug.assert(out_width > 0 and out_height > 0);
    const out = try allocator.alloc(i32, out_width * out_height);
    errdefer allocator.free(out);

    const in_w_f: f64 = @floatFromInt(in_width);
    const in_h_f: f64 = @floatFromInt(in_height);
    const out_w_f: f64 = @floatFromInt(out_width);
    const out_h_f: f64 = @floatFromInt(out_height);
    const x_scale = in_w_f / out_w_f;
    const y_scale = in_h_f / out_h_f;
    const max_x: isize = @as(isize, @intCast(in_width)) - 1;
    const max_y: isize = @as(isize, @intCast(in_height)) - 1;

    var y: usize = 0;
    while (y < out_height) : (y += 1) {
        const sy = (@as(f64, @floatFromInt(y)) + 0.5) * y_scale - 0.5;
        const y0_f = @floor(sy);
        const fy = sy - y0_f;
        const y0_i: isize = @intFromFloat(y0_f);
        const y1_i: isize = y0_i + 1;
        const y0 = @as(usize, @intCast(clampISize(y0_i, 0, max_y)));
        const y1 = @as(usize, @intCast(clampISize(y1_i, 0, max_y)));

        var x: usize = 0;
        while (x < out_width) : (x += 1) {
            const sx = (@as(f64, @floatFromInt(x)) + 0.5) * x_scale - 0.5;
            const x0_f = @floor(sx);
            const fx = sx - x0_f;
            const x0_i: isize = @intFromFloat(x0_f);
            const x1_i: isize = x0_i + 1;
            const x0 = @as(usize, @intCast(clampISize(x0_i, 0, max_x)));
            const x1 = @as(usize, @intCast(clampISize(x1_i, 0, max_x)));

            const p00: f64 = @floatFromInt(input[y0 * in_width + x0]);
            const p01: f64 = @floatFromInt(input[y0 * in_width + x1]);
            const p10: f64 = @floatFromInt(input[y1 * in_width + x0]);
            const p11: f64 = @floatFromInt(input[y1 * in_width + x1]);

            const top = p00 * (1.0 - fx) + p01 * fx;
            const bot = p10 * (1.0 - fx) + p11 * fx;
            const v = top * (1.0 - fy) + bot * fy;
            out[y * out_width + x] = @intFromFloat(@round(v));
        }
    }
    return out;
}

/// Bilinear upsampling for f32 planes.
pub fn bilinearF32(
    allocator: std.mem.Allocator,
    input: []const f32,
    in_width: usize,
    in_height: usize,
    out_width: usize,
    out_height: usize,
) ![]f32 {
    std.debug.assert(in_width > 0 and in_height > 0);
    std.debug.assert(out_width > 0 and out_height > 0);
    const out = try allocator.alloc(f32, out_width * out_height);
    errdefer allocator.free(out);

    const in_w_f: f32 = @floatFromInt(in_width);
    const in_h_f: f32 = @floatFromInt(in_height);
    const out_w_f: f32 = @floatFromInt(out_width);
    const out_h_f: f32 = @floatFromInt(out_height);
    const x_scale = in_w_f / out_w_f;
    const y_scale = in_h_f / out_h_f;
    const max_x: isize = @as(isize, @intCast(in_width)) - 1;
    const max_y: isize = @as(isize, @intCast(in_height)) - 1;

    var y: usize = 0;
    while (y < out_height) : (y += 1) {
        const sy = (@as(f32, @floatFromInt(y)) + 0.5) * y_scale - 0.5;
        const y0_f = @floor(sy);
        const fy = sy - y0_f;
        const y0_i: isize = @intFromFloat(y0_f);
        const y1_i: isize = y0_i + 1;
        const y0 = @as(usize, @intCast(clampISize(y0_i, 0, max_y)));
        const y1 = @as(usize, @intCast(clampISize(y1_i, 0, max_y)));

        var x: usize = 0;
        while (x < out_width) : (x += 1) {
            const sx = (@as(f32, @floatFromInt(x)) + 0.5) * x_scale - 0.5;
            const x0_f = @floor(sx);
            const fx = sx - x0_f;
            const x0_i: isize = @intFromFloat(x0_f);
            const x1_i: isize = x0_i + 1;
            const x0 = @as(usize, @intCast(clampISize(x0_i, 0, max_x)));
            const x1 = @as(usize, @intCast(clampISize(x1_i, 0, max_x)));

            const p00 = input[y0 * in_width + x0];
            const p01 = input[y0 * in_width + x1];
            const p10 = input[y1 * in_width + x0];
            const p11 = input[y1 * in_width + x1];

            const top = p00 * (1.0 - fx) + p01 * fx;
            const bot = p10 * (1.0 - fx) + p11 * fx;
            out[y * out_width + x] = top * (1.0 - fy) + bot * fy;
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// Separable 6-tap Lanczos-3 2x upsampling
// ---------------------------------------------------------------------------

// Normalized 6-tap Lanczos-3 weights at the +/-0.5 phase offsets.
// Base taps at offsets {-2.5,-1.5,-0.5,0.5,1.5,2.5} for the "even" output
// and the mirror for the "odd" output. The two sets are symmetric, so one
// array suffices — produce both by reversing for the partner phase.
const lanczos2x_taps = [6]f64{ 0.024, -0.135, 0.611, 0.611, -0.135, 0.024 };

inline fn tapSampleI32(input: []const i32, width: usize, max_x: isize, y: usize, xi: isize) f64 {
    const c = clampISize(xi, 0, max_x);
    return @floatFromInt(input[y * width + @as(usize, @intCast(c))]);
}

inline fn tapSampleRowF64(row: []const f64, max_x: isize, xi: isize) f64 {
    const c = clampISize(xi, 0, max_x);
    return row[@as(usize, @intCast(c))];
}

/// Separable 6-tap Lanczos-3 upsampling by 2x in each axis.
/// Output dimensions: (2*in_width, 2*in_height).
/// Integer variant rounds-to-nearest and clamps to i32 range.
pub fn lanczos2xI32(
    allocator: std.mem.Allocator,
    input: []const i32,
    in_width: usize,
    in_height: usize,
) ![]i32 {
    std.debug.assert(in_width > 0 and in_height > 0);
    const out_w = in_width * 2;
    const out_h = in_height * 2;

    // Horizontal pass into f64 intermediate plane sized (out_w x in_height).
    const horiz = try allocator.alloc(f64, out_w * in_height);
    defer allocator.free(horiz);

    const max_x: isize = @as(isize, @intCast(in_width)) - 1;
    const max_y: isize = @as(isize, @intCast(in_height)) - 1;

    // For 2x upsample using a 6-tap kernel centered between samples:
    // Output pixel 2*i     (left of source i) draws from src indices {i-2,i-1,i,i+1,i+2,i+3}
    //   with taps applied as:       t[5],t[4],t[3],t[2],t[1],t[0]
    // Output pixel 2*i + 1 (right of source i) uses the same neighborhood but
    //   taps applied as:            t[0],t[1],t[2],t[3],t[4],t[5]
    // where t = lanczos2x_taps.
    const t = lanczos2x_taps;

    var y: usize = 0;
    while (y < in_height) : (y += 1) {
        var i: usize = 0;
        while (i < in_width) : (i += 1) {
            const ii: isize = @intCast(i);
            const s0 = tapSampleI32(input, in_width, max_x, y, ii - 2);
            const s1 = tapSampleI32(input, in_width, max_x, y, ii - 1);
            const s2 = tapSampleI32(input, in_width, max_x, y, ii);
            const s3 = tapSampleI32(input, in_width, max_x, y, ii + 1);
            const s4 = tapSampleI32(input, in_width, max_x, y, ii + 2);
            const s5 = tapSampleI32(input, in_width, max_x, y, ii + 3);

            const even = s0 * t[5] + s1 * t[4] + s2 * t[3] + s3 * t[2] + s4 * t[1] + s5 * t[0];
            const odd = s0 * t[0] + s1 * t[1] + s2 * t[2] + s3 * t[3] + s4 * t[4] + s5 * t[5];

            horiz[y * out_w + 2 * i] = even;
            horiz[y * out_w + 2 * i + 1] = odd;
        }
    }

    // Vertical pass into i32 output sized (out_w x out_h).
    const out = try allocator.alloc(i32, out_w * out_h);
    errdefer allocator.free(out);

    var x: usize = 0;
    while (x < out_w) : (x += 1) {
        var j: usize = 0;
        while (j < in_height) : (j += 1) {
            const jj: isize = @intCast(j);
            // Pull a column from horiz: values at rows jj-2..jj+3, column x.
            const s0 = horiz[@as(usize, @intCast(clampISize(jj - 2, 0, max_y))) * out_w + x];
            const s1 = horiz[@as(usize, @intCast(clampISize(jj - 1, 0, max_y))) * out_w + x];
            const s2 = horiz[@as(usize, @intCast(clampISize(jj, 0, max_y))) * out_w + x];
            const s3 = horiz[@as(usize, @intCast(clampISize(jj + 1, 0, max_y))) * out_w + x];
            const s4 = horiz[@as(usize, @intCast(clampISize(jj + 2, 0, max_y))) * out_w + x];
            const s5 = horiz[@as(usize, @intCast(clampISize(jj + 3, 0, max_y))) * out_w + x];

            const even = s0 * t[5] + s1 * t[4] + s2 * t[3] + s3 * t[2] + s4 * t[1] + s5 * t[0];
            const odd = s0 * t[0] + s1 * t[1] + s2 * t[2] + s3 * t[3] + s4 * t[4] + s5 * t[5];

            const i32_min: f64 = @floatFromInt(std.math.minInt(i32));
            const i32_max: f64 = @floatFromInt(std.math.maxInt(i32));
            const e_clamped = std.math.clamp(@round(even), i32_min, i32_max);
            const o_clamped = std.math.clamp(@round(odd), i32_min, i32_max);

            out[(2 * j) * out_w + x] = @intFromFloat(e_clamped);
            out[(2 * j + 1) * out_w + x] = @intFromFloat(o_clamped);
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// Dispatch wrapper
// ---------------------------------------------------------------------------

pub const UpsampleMode = enum { nearest, bilinear, lanczos2x };

/// Dispatch upsampling by mode.
/// - nearest: nearest-neighbor.
/// - bilinear: bilinear.
/// - lanczos2x: if out dims are exactly 2x the input dims, uses the separable
///   Lanczos-3 2x kernel; otherwise falls back to bilinear.
pub fn upsampleI32(
    allocator: std.mem.Allocator,
    mode: UpsampleMode,
    input: []const i32,
    in_width: usize,
    in_height: usize,
    out_width: usize,
    out_height: usize,
) ![]i32 {
    return switch (mode) {
        .nearest => nearestNeighborI32(allocator, input, in_width, in_height, out_width, out_height),
        .bilinear => bilinearI32(allocator, input, in_width, in_height, out_width, out_height),
        .lanczos2x => if (out_width == 2 * in_width and out_height == 2 * in_height)
            lanczos2xI32(allocator, input, in_width, in_height)
        else
            bilinearI32(allocator, input, in_width, in_height, out_width, out_height),
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "bilinear i32 2x2 to 4x4 corners and blend" {
    const allocator = std.testing.allocator;
    const input = [_]i32{
        0,   100,
        200, 300,
    };
    const result = try bilinearI32(allocator, &input, 2, 2, 4, 4);
    defer allocator.free(result);

    // Corners should equal the source corners (because the clamped
    // pixel-center mapping at the extremes lands on the source corner).
    try std.testing.expectEqual(@as(i32, 0), result[0]);
    try std.testing.expectEqual(@as(i32, 100), result[3]);
    try std.testing.expectEqual(@as(i32, 200), result[12]);
    try std.testing.expectEqual(@as(i32, 300), result[15]);

    // Interior pixels should be strictly between min and max source values
    // and show proper blending (monotonic along rows and columns).
    var y: usize = 0;
    while (y < 4) : (y += 1) {
        var x: usize = 0;
        while (x < 3) : (x += 1) {
            const a = result[y * 4 + x];
            const b = result[y * 4 + x + 1];
            try std.testing.expect(a <= b);
        }
    }
    var x: usize = 0;
    while (x < 4) : (x += 1) {
        var y2: usize = 0;
        while (y2 < 3) : (y2 += 1) {
            const a = result[y2 * 4 + x];
            const b = result[(y2 + 1) * 4 + x];
            try std.testing.expect(a <= b);
        }
    }

    // The central 2x2 block should lie strictly inside [0, 300] and not all
    // be equal to any corner (i.e. real blending happened).
    const c0 = result[1 * 4 + 1];
    const c1 = result[1 * 4 + 2];
    const c2 = result[2 * 4 + 1];
    const c3 = result[2 * 4 + 2];
    for ([_]i32{ c0, c1, c2, c3 }) |v| {
        try std.testing.expect(v > 0 and v < 300);
    }
}

test "bilinear i32 4x4 to 8x8 preserves constant plane" {
    const allocator = std.testing.allocator;
    var input: [16]i32 = undefined;
    for (&input) |*p| p.* = 77;
    const result = try bilinearI32(allocator, &input, 4, 4, 8, 8);
    defer allocator.free(result);
    for (result) |v| try std.testing.expectEqual(@as(i32, 77), v);
}

test "bilinear i32 4x4 to 8x8 preserves linear ramp monotonicity" {
    const allocator = std.testing.allocator;
    // Horizontal ramp 0,10,20,30 repeated over 4 rows.
    var input: [16]i32 = undefined;
    var y: usize = 0;
    while (y < 4) : (y += 1) {
        var x: usize = 0;
        while (x < 4) : (x += 1) input[y * 4 + x] = @as(i32, @intCast(x * 10));
    }
    const result = try bilinearI32(allocator, &input, 4, 4, 8, 8);
    defer allocator.free(result);

    // Each output row must be non-decreasing across x.
    var ry: usize = 0;
    while (ry < 8) : (ry += 1) {
        var rx: usize = 0;
        while (rx < 7) : (rx += 1) {
            try std.testing.expect(result[ry * 8 + rx] <= result[ry * 8 + rx + 1]);
        }
    }
    // Endpoints match the edge samples (clamped mapping hits the source edges).
    try std.testing.expectEqual(@as(i32, 0), result[0]);
    try std.testing.expectEqual(@as(i32, 30), result[7]);
}

test "bilinear i32 3x2 to 6x4 non power of two" {
    const allocator = std.testing.allocator;
    const input = [_]i32{
        0,  50, 100,
        60, 80, 120,
    };
    const result = try bilinearI32(allocator, &input, 3, 2, 6, 4);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 24), result.len);

    // Corners match source corners.
    try std.testing.expectEqual(@as(i32, 0), result[0]);
    try std.testing.expectEqual(@as(i32, 100), result[5]);
    try std.testing.expectEqual(@as(i32, 60), result[3 * 6 + 0]);
    try std.testing.expectEqual(@as(i32, 120), result[3 * 6 + 5]);

    // All samples within [0, 120].
    for (result) |v| try std.testing.expect(v >= 0 and v <= 120);
}

test "bilinear f32 2x2 to 4x4 center average" {
    const allocator = std.testing.allocator;
    const input = [_]f32{
        0.0,   100.0,
        200.0, 300.0,
    };
    const result = try bilinearF32(allocator, &input, 2, 2, 4, 4);
    defer allocator.free(result);

    // Corner preservation.
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), result[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 100.0), result[3], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 200.0), result[12], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 300.0), result[15], 1e-4);
}

test "lanczos2x i32 output dimensions and constant plane" {
    const allocator = std.testing.allocator;
    var input: [16]i32 = undefined;
    for (&input) |*p| p.* = 128;
    const result = try lanczos2xI32(allocator, &input, 4, 4);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 64), result.len);
    // Tap weights sum to 1 so a flat input must remain flat.
    for (result) |v| try std.testing.expectEqual(@as(i32, 128), v);
}

test "lanczos2x i32 smooth ramp stays smooth" {
    const allocator = std.testing.allocator;
    // Build a wide, smooth horizontal ramp so kernel support is well-defined.
    const iw: usize = 8;
    const ih: usize = 4;
    var input: [32]i32 = undefined;
    var y: usize = 0;
    while (y < ih) : (y += 1) {
        var x: usize = 0;
        while (x < iw) : (x += 1) {
            // Values 0..70 by 10s.
            input[y * iw + x] = @as(i32, @intCast(x * 10));
        }
    }
    const result = try lanczos2xI32(allocator, &input, iw, ih);
    defer allocator.free(result);

    const ow = iw * 2;
    const oh = ih * 4 / 2; // = ih*2
    try std.testing.expectEqual(ow * oh, result.len);

    // In the interior of each output row (avoiding edge ringing from the
    // replicated boundary), output must be non-decreasing across x.
    var ry: usize = 0;
    while (ry < oh) : (ry += 1) {
        var rx: usize = 4;
        while (rx < ow - 5) : (rx += 1) {
            try std.testing.expect(result[ry * ow + rx] <= result[ry * ow + rx + 1]);
        }
    }

    // Edges should not be "blown out": all samples within a small guard band
    // around the source range [0, 70].
    for (result) |v| try std.testing.expect(v >= -20 and v <= 90);
}

test "lanczos2x i32 step edge: no wild overshoot" {
    const allocator = std.testing.allocator;
    const iw: usize = 8;
    const ih: usize = 2;
    var input: [16]i32 = undefined;
    var y: usize = 0;
    while (y < ih) : (y += 1) {
        var x: usize = 0;
        while (x < iw) : (x += 1) {
            input[y * iw + x] = if (x < 4) 0 else 100;
        }
    }
    const result = try lanczos2xI32(allocator, &input, iw, ih);
    defer allocator.free(result);

    // Lanczos-3 with these normalized taps has small overshoot; bound it.
    // Max negative lobe and max overshoot above 100 should both be modest.
    for (result) |v| try std.testing.expect(v >= -20 and v <= 120);
}

test "upsampleI32 dispatch nearest" {
    const allocator = std.testing.allocator;
    const input = [_]i32{ 1, 2, 3, 4 };
    const result = try upsampleI32(allocator, .nearest, &input, 2, 2, 4, 4);
    defer allocator.free(result);
    const expected = [_]i32{
        1, 1, 2, 2,
        1, 1, 2, 2,
        3, 3, 4, 4,
        3, 3, 4, 4,
    };
    try std.testing.expectEqualSlices(i32, &expected, result);
}

test "upsampleI32 dispatch bilinear" {
    const allocator = std.testing.allocator;
    const input = [_]i32{ 0, 100, 200, 300 };
    const a = try upsampleI32(allocator, .bilinear, &input, 2, 2, 4, 4);
    defer allocator.free(a);
    const b = try bilinearI32(allocator, &input, 2, 2, 4, 4);
    defer allocator.free(b);
    try std.testing.expectEqualSlices(i32, b, a);
}

test "upsampleI32 dispatch lanczos2x uses lanczos when factor==2" {
    const allocator = std.testing.allocator;
    var input: [16]i32 = undefined;
    for (&input, 0..) |*p, i| p.* = @as(i32, @intCast(i));
    const a = try upsampleI32(allocator, .lanczos2x, &input, 4, 4, 8, 8);
    defer allocator.free(a);
    const b = try lanczos2xI32(allocator, &input, 4, 4);
    defer allocator.free(b);
    try std.testing.expectEqualSlices(i32, b, a);
}

test "upsampleI32 dispatch lanczos2x falls back to bilinear for non-2x" {
    const allocator = std.testing.allocator;
    // 4x3 -> 7x5 is not a 2x integer upscale so it must fall back to bilinear.
    const input = [_]i32{
        0,  40, 80, 120,
        10, 50, 90, 130,
        20, 60, 100, 140,
    };
    const a = try upsampleI32(allocator, .lanczos2x, &input, 4, 3, 7, 5);
    defer allocator.free(a);
    const b = try bilinearI32(allocator, &input, 4, 3, 7, 5);
    defer allocator.free(b);
    try std.testing.expectEqualSlices(i32, b, a);
}
