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

const simd_lanes_f32 = 8;
const F32x8 = @Vector(simd_lanes_f32, f32);
const simd_lanes_i32 = 8;
const I32x8 = @Vector(simd_lanes_i32, i32);
const U5x8 = @Vector(simd_lanes_i32, u5);

/// Inverse RCT (Reversible Color Transform) - integer arithmetic.
/// Converts from Y, Cb, Cr back to R, G, B.
/// Formulas (from ISO 15444-1 Annex G):
///   G = Y - floor((Cb + Cr) / 4)
///   R = Cr + G
///   B = Cb + G
/// Operates in-place on the three component planes.
pub fn inverseRct(y_plane: []i32, cb_plane: []i32, cr_plane: []i32) void {
    std.debug.assert(y_plane.len == cb_plane.len and cb_plane.len == cr_plane.len);

    var i: usize = 0;
    const v_shift_2: U5x8 = @splat(2);
    while (i + simd_lanes_i32 <= y_plane.len) : (i += simd_lanes_i32) {
        const yv: I32x8 = y_plane[i..][0..simd_lanes_i32].*;
        const cbv: I32x8 = cb_plane[i..][0..simd_lanes_i32].*;
        const crv: I32x8 = cr_plane[i..][0..simd_lanes_i32].*;
        const g = yv - ((cbv + crv) >> v_shift_2);
        y_plane[i..][0..simd_lanes_i32].* = crv + g;
        cb_plane[i..][0..simd_lanes_i32].* = g;
        cr_plane[i..][0..simd_lanes_i32].* = cbv + g;
    }

    while (i < y_plane.len) : (i += 1) {
        const y = &y_plane[i];
        const cb = &cb_plane[i];
        const cr = &cr_plane[i];
        const g = y.* - @divFloor(cb.* + cr.*, 4);
        const r = cr.* + g;
        const b = cb.* + g;
        y.* = r; // plane 0 becomes R
        cb.* = g; // plane 1 becomes G
        cr.* = b; // plane 2 becomes B
    }
}

/// Forward RCT (for encoder) - integer arithmetic.
/// Converts from R, G, B to Y, Cb, Cr.
/// Formulas:
///   Y = floor((R + 2*G + B) / 4)
///   Cb = B - G
///   Cr = R - G
pub fn forwardRct(r_plane: []i32, g_plane: []i32, b_plane: []i32) void {
    std.debug.assert(r_plane.len == g_plane.len and g_plane.len == b_plane.len);
    for (r_plane, g_plane, b_plane) |*r, *g, *b| {
        const y = @divFloor(r.* + 2 * g.* + b.*, 4);
        const cb = b.* - g.*;
        const cr = r.* - g.*;
        r.* = y; // plane 0 becomes Y
        g.* = cb; // plane 1 becomes Cb
        b.* = cr; // plane 2 becomes Cr
    }
}

/// Inverse ICT (Irreversible Color Transform) - floating point.
/// Converts from Y, Cb, Cr back to R, G, B.
/// Standard matrix (ITU-R BT.601):
///   R = Y + 1.402 * Cr
///   G = Y - 0.34413 * Cb - 0.71414 * Cr
///   B = Y + 1.772 * Cb
pub fn inverseIct(y_plane: []f32, cb_plane: []f32, cr_plane: []f32) void {
    std.debug.assert(y_plane.len == cb_plane.len and cb_plane.len == cr_plane.len);

    var i: usize = 0;
    const v_1_402: F32x8 = @splat(1.402);
    const v_0_34413: F32x8 = @splat(0.34413);
    const v_0_71414: F32x8 = @splat(0.71414);
    const v_1_772: F32x8 = @splat(1.772);
    while (i + simd_lanes_f32 <= y_plane.len) : (i += simd_lanes_f32) {
        const yv: F32x8 = y_plane[i..][0..simd_lanes_f32].*;
        const cbv: F32x8 = cb_plane[i..][0..simd_lanes_f32].*;
        const crv: F32x8 = cr_plane[i..][0..simd_lanes_f32].*;
        y_plane[i..][0..simd_lanes_f32].* = yv + v_1_402 * crv;
        cb_plane[i..][0..simd_lanes_f32].* = yv - v_0_34413 * cbv - v_0_71414 * crv;
        cr_plane[i..][0..simd_lanes_f32].* = yv + v_1_772 * cbv;
    }

    while (i < y_plane.len) : (i += 1) {
        const y = y_plane[i];
        const cb = cb_plane[i];
        const cr = cr_plane[i];
        y_plane[i] = y + 1.402 * cr;
        cb_plane[i] = y - 0.34413 * cb - 0.71414 * cr;
        cr_plane[i] = y + 1.772 * cb;
    }
}

/// Forward ICT (for encoder) - floating point.
/// Converts from R, G, B to Y, Cb, Cr.
///   Y  =  0.299 * R + 0.587 * G + 0.114 * B
///   Cb = -0.16875 * R - 0.33126 * G + 0.5 * B
///   Cr =  0.5 * R - 0.41869 * G - 0.08131 * B
pub fn forwardIct(r_plane: []f32, g_plane: []f32, b_plane: []f32) void {
    std.debug.assert(r_plane.len == g_plane.len and g_plane.len == b_plane.len);

    var i: usize = 0;
    const v_0_299: F32x8 = @splat(0.299);
    const v_0_587: F32x8 = @splat(0.587);
    const v_0_114: F32x8 = @splat(0.114);
    const v_neg_0_16875: F32x8 = @splat(-0.16875);
    const v_neg_0_33126: F32x8 = @splat(-0.33126);
    const v_0_5: F32x8 = @splat(0.5);
    const v_neg_0_41869: F32x8 = @splat(-0.41869);
    const v_neg_0_08131: F32x8 = @splat(-0.08131);
    while (i + simd_lanes_f32 <= r_plane.len) : (i += simd_lanes_f32) {
        const rv: F32x8 = r_plane[i..][0..simd_lanes_f32].*;
        const gv: F32x8 = g_plane[i..][0..simd_lanes_f32].*;
        const bv: F32x8 = b_plane[i..][0..simd_lanes_f32].*;
        r_plane[i..][0..simd_lanes_f32].* = v_0_299 * rv + v_0_587 * gv + v_0_114 * bv;
        g_plane[i..][0..simd_lanes_f32].* = v_neg_0_16875 * rv + v_neg_0_33126 * gv + v_0_5 * bv;
        b_plane[i..][0..simd_lanes_f32].* = v_0_5 * rv + v_neg_0_41869 * gv + v_neg_0_08131 * bv;
    }

    while (i < r_plane.len) : (i += 1) {
        const r = r_plane[i];
        const g = g_plane[i];
        const b = b_plane[i];
        r_plane[i] = 0.299 * r + 0.587 * g + 0.114 * b;
        g_plane[i] = -0.16875 * r - 0.33126 * g + 0.5 * b;
        b_plane[i] = 0.5 * r - 0.41869 * g - 0.08131 * b;
    }
}

// ---------------------------------------------------------------------------
// Custom Multiple Component Transform (MCT) — ISO 15444-1 Annex J.
// Generalizes RCT/ICT to arbitrary user-supplied linear decorrelation matrices.
// ---------------------------------------------------------------------------

/// Maximum number of components supported by the custom MCT helpers.
/// Chosen to keep the per-pixel temporary buffer on the stack.
pub const custom_mct_max_components: u8 = 16;

pub const CustomMctError = error{
    TooManyComponents,
    DimensionMismatch,
    SingularMatrix,
    ShiftTooLarge,
};

/// Floating-point custom decorrelation matrix.
///
/// Forward: `out[i] = sum_j forward[i,j] * (in[j] - offsets[j])`
/// Inverse: `out[i] = sum_j inverse[i,j] * in[j] + offsets[i]`
///
/// Matrices are stored row-major (index `i*N + j`).
pub const CustomMctMatrix = struct {
    /// Number of components N. Matrix is N×N.
    num_components: u8,
    /// Row-major N×N decorrelation (forward) matrix.
    forward: []const f32,
    /// Row-major N×N reconstruction (inverse) matrix.
    inverse: []const f32,
    /// Per-component pre/post offsets (applied before forward / after inverse).
    offsets: []const f32,

    fn validate(self: CustomMctMatrix) CustomMctError!void {
        if (self.num_components == 0 or self.num_components > custom_mct_max_components)
            return error.TooManyComponents;
        const n: usize = self.num_components;
        if (self.forward.len != n * n) return error.DimensionMismatch;
        if (self.inverse.len != n * n) return error.DimensionMismatch;
        if (self.offsets.len != n) return error.DimensionMismatch;
    }
};

/// Integer custom decorrelation matrix for lossless / fixed-point use.
///
/// The effective rational coefficient is `forward[i,j] * 2^(-shift)`.
/// Apply: `out[i] = round( (sum_j forward[i,j] * (in[j] - offsets[j])) / 2^shift )`.
pub const CustomMctMatrixI32 = struct {
    num_components: u8,
    forward: []const i32,
    inverse: []const i32,
    shift: u5,
    offsets: []const i32,

    fn validate(self: CustomMctMatrixI32) CustomMctError!void {
        if (self.num_components == 0 or self.num_components > custom_mct_max_components)
            return error.TooManyComponents;
        if (self.shift > 16) return error.ShiftTooLarge;
        const n: usize = self.num_components;
        if (self.forward.len != n * n) return error.DimensionMismatch;
        if (self.inverse.len != n * n) return error.DimensionMismatch;
        if (self.offsets.len != n) return error.DimensionMismatch;
    }
};

fn validatePlanes(planes: []const []f32, n: u8) CustomMctError!usize {
    if (planes.len != n) return error.DimensionMismatch;
    if (planes.len == 0) return 0;
    const len = planes[0].len;
    for (planes[1..]) |p| if (p.len != len) return error.DimensionMismatch;
    return len;
}

fn validatePlanesI32(planes: []const []i32, n: u8) CustomMctError!usize {
    if (planes.len != n) return error.DimensionMismatch;
    if (planes.len == 0) return 0;
    const len = planes[0].len;
    for (planes[1..]) |p| if (p.len != len) return error.DimensionMismatch;
    return len;
}

/// Apply the forward custom MCT in place across `planes`
/// (`planes.len == matrix.num_components`, all same length).
pub fn applyCustomMctForward(matrix: CustomMctMatrix, planes: []const []f32) CustomMctError!void {
    try matrix.validate();
    const n = matrix.num_components;
    const px_count = try validatePlanes(planes, n);

    var in_buf: [custom_mct_max_components]f32 = undefined;
    var out_buf: [custom_mct_max_components]f32 = undefined;

    var p: usize = 0;
    while (p < px_count) : (p += 1) {
        var j: usize = 0;
        while (j < n) : (j += 1) {
            in_buf[j] = planes[j][p] - matrix.offsets[j];
        }
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var acc: f32 = 0.0;
            var k: usize = 0;
            while (k < n) : (k += 1) {
                acc += matrix.forward[i * n + k] * in_buf[k];
            }
            out_buf[i] = acc;
        }
        i = 0;
        while (i < n) : (i += 1) planes[i][p] = out_buf[i];
    }
}

/// Apply the inverse custom MCT in place across `planes`.
pub fn applyCustomMctInverse(matrix: CustomMctMatrix, planes: []const []f32) CustomMctError!void {
    try matrix.validate();
    const n = matrix.num_components;
    const px_count = try validatePlanes(planes, n);

    var in_buf: [custom_mct_max_components]f32 = undefined;
    var out_buf: [custom_mct_max_components]f32 = undefined;

    var p: usize = 0;
    while (p < px_count) : (p += 1) {
        var j: usize = 0;
        while (j < n) : (j += 1) in_buf[j] = planes[j][p];
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var acc: f32 = 0.0;
            var k: usize = 0;
            while (k < n) : (k += 1) {
                acc += matrix.inverse[i * n + k] * in_buf[k];
            }
            out_buf[i] = acc + matrix.offsets[i];
        }
        i = 0;
        while (i < n) : (i += 1) planes[i][p] = out_buf[i];
    }
}

fn roundShiftI64(v: i64, shift: u5) i32 {
    if (shift == 0) return @intCast(v);
    const bias: i64 = @as(i64, 1) << (@as(u6, shift) - 1);
    const adj: i64 = if (v >= 0) v + bias else v - bias;
    const shifted = @divTrunc(adj, @as(i64, 1) << shift);
    return @intCast(shifted);
}

/// Apply the forward integer custom MCT in place.
pub fn applyCustomMctForwardI32(matrix: CustomMctMatrixI32, planes: []const []i32) CustomMctError!void {
    try matrix.validate();
    const n = matrix.num_components;
    const px_count = try validatePlanesI32(planes, n);

    var in_buf: [custom_mct_max_components]i32 = undefined;
    var out_buf: [custom_mct_max_components]i32 = undefined;

    var p: usize = 0;
    while (p < px_count) : (p += 1) {
        var j: usize = 0;
        while (j < n) : (j += 1) in_buf[j] = planes[j][p] - matrix.offsets[j];
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var acc: i64 = 0;
            var k: usize = 0;
            while (k < n) : (k += 1) {
                acc += @as(i64, matrix.forward[i * n + k]) * @as(i64, in_buf[k]);
            }
            out_buf[i] = roundShiftI64(acc, matrix.shift);
        }
        i = 0;
        while (i < n) : (i += 1) planes[i][p] = out_buf[i];
    }
}

/// Apply the inverse integer custom MCT in place.
pub fn applyCustomMctInverseI32(matrix: CustomMctMatrixI32, planes: []const []i32) CustomMctError!void {
    try matrix.validate();
    const n = matrix.num_components;
    const px_count = try validatePlanesI32(planes, n);

    var in_buf: [custom_mct_max_components]i32 = undefined;
    var out_buf: [custom_mct_max_components]i32 = undefined;

    var p: usize = 0;
    while (p < px_count) : (p += 1) {
        var j: usize = 0;
        while (j < n) : (j += 1) in_buf[j] = planes[j][p];
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var acc: i64 = 0;
            var k: usize = 0;
            while (k < n) : (k += 1) {
                acc += @as(i64, matrix.inverse[i * n + k]) * @as(i64, in_buf[k]);
            }
            out_buf[i] = roundShiftI64(acc, matrix.shift) + matrix.offsets[i];
        }
        i = 0;
        while (i < n) : (i += 1) planes[i][p] = out_buf[i];
    }
}

/// Invert an N×N row-major matrix via Gauss-Jordan elimination with partial
/// pivoting. Caller owns returned slice. Returns `error.SingularMatrix` when
/// the matrix is (numerically) singular.
pub fn invertMctMatrixGaussJordan(
    forward: []const f32,
    n: u8,
    allocator: std.mem.Allocator,
) ![]f32 {
    if (n == 0 or n > custom_mct_max_components) return error.TooManyComponents;
    const nn: usize = n;
    if (forward.len != nn * nn) return error.DimensionMismatch;

    // Build augmented [A | I] matrix with 2N columns.
    const cols: usize = 2 * nn;
    const aug = try allocator.alloc(f32, nn * cols);
    defer allocator.free(aug);

    for (0..nn) |i| {
        for (0..nn) |j| {
            aug[i * cols + j] = forward[i * nn + j];
            aug[i * cols + nn + j] = if (i == j) @as(f32, 1.0) else @as(f32, 0.0);
        }
    }

    // f32 singular-pivot threshold. 1e-12 is below f32 epsilon and never triggers; 1e-6 is a
    // reasonable default for matrices with entries near unit magnitude.
    const eps: f32 = 1e-6;
    for (0..nn) |col| {
        // Partial pivot: find largest magnitude in column `col` at/below row `col`.
        var pivot_row: usize = col;
        var pivot_mag: f32 = @abs(aug[col * cols + col]);
        var r: usize = col + 1;
        while (r < nn) : (r += 1) {
            const m = @abs(aug[r * cols + col]);
            if (m > pivot_mag) {
                pivot_mag = m;
                pivot_row = r;
            }
        }
        if (pivot_mag <= eps) return error.SingularMatrix;

        if (pivot_row != col) {
            // Swap rows.
            var c: usize = 0;
            while (c < cols) : (c += 1) {
                const tmp = aug[col * cols + c];
                aug[col * cols + c] = aug[pivot_row * cols + c];
                aug[pivot_row * cols + c] = tmp;
            }
        }

        // Scale pivot row to make pivot == 1.
        const pivot = aug[col * cols + col];
        var c: usize = 0;
        while (c < cols) : (c += 1) aug[col * cols + c] /= pivot;

        // Eliminate other rows.
        var rr: usize = 0;
        while (rr < nn) : (rr += 1) {
            if (rr == col) continue;
            const factor = aug[rr * cols + col];
            if (factor == 0) continue;
            var cc: usize = 0;
            while (cc < cols) : (cc += 1) {
                aug[rr * cols + cc] -= factor * aug[col * cols + cc];
            }
        }
    }

    const result = try allocator.alloc(f32, nn * nn);
    for (0..nn) |i| {
        for (0..nn) |j| result[i * nn + j] = aug[i * cols + nn + j];
    }
    return result;
}

// ---------------------------------------------------------------------------

test "custom MCT identity forward-then-inverse is identity (f32)" {
    const allocator = std.testing.allocator;
    const n: u8 = 3;
    const ident = [_]f32{
        1, 0, 0,
        0, 1, 0,
        0, 0, 1,
    };
    const offsets = [_]f32{ 0, 0, 0 };
    const matrix = CustomMctMatrix{
        .num_components = n,
        .forward = &ident,
        .inverse = &ident,
        .offsets = &offsets,
    };

    const p0 = try allocator.dupe(f32, &[_]f32{ 10, 20, 30, 40 });
    defer allocator.free(p0);
    const p1 = try allocator.dupe(f32, &[_]f32{ -5, 0, 127, 255 });
    defer allocator.free(p1);
    const p2 = try allocator.dupe(f32, &[_]f32{ 1.5, 2.5, 3.5, 4.5 });
    defer allocator.free(p2);

    var planes = [_][]f32{ p0, p1, p2 };
    try applyCustomMctForward(matrix, &planes);
    try applyCustomMctInverse(matrix, &planes);

    const orig0 = [_]f32{ 10, 20, 30, 40 };
    const orig1 = [_]f32{ -5, 0, 127, 255 };
    const orig2 = [_]f32{ 1.5, 2.5, 3.5, 4.5 };
    for (0..4) |i| {
        try std.testing.expectApproxEqAbs(orig0[i], p0[i], 1e-5);
        try std.testing.expectApproxEqAbs(orig1[i], p1[i], 1e-5);
        try std.testing.expectApproxEqAbs(orig2[i], p2[i], 1e-5);
    }
}

test "custom MCT identity forward-then-inverse is identity (i32)" {
    const allocator = std.testing.allocator;
    const n: u8 = 3;
    // shift = 8, so identity entries become 256.
    const fwd = [_]i32{
        256, 0,   0,
        0,   256, 0,
        0,   0,   256,
    };
    const offsets = [_]i32{ 0, 0, 0 };
    const matrix = CustomMctMatrixI32{
        .num_components = n,
        .forward = &fwd,
        .inverse = &fwd,
        .shift = 8,
        .offsets = &offsets,
    };

    const p0 = try allocator.dupe(i32, &[_]i32{ 10, 20, 30, -40 });
    defer allocator.free(p0);
    const p1 = try allocator.dupe(i32, &[_]i32{ -5, 0, 127, 255 });
    defer allocator.free(p1);
    const p2 = try allocator.dupe(i32, &[_]i32{ 1, 2, 3, 4 });
    defer allocator.free(p2);

    var planes = [_][]i32{ p0, p1, p2 };
    try applyCustomMctForwardI32(matrix, &planes);
    try applyCustomMctInverseI32(matrix, &planes);

    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 20, 30, -40 }, p0);
    try std.testing.expectEqualSlices(i32, &[_]i32{ -5, 0, 127, 255 }, p1);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 4 }, p2);
}

test "custom MCT approximates built-in ICT on RGB input" {
    const allocator = std.testing.allocator;
    // Forward ICT coefficients laid out as Y/Cb/Cr rows.
    const forward = [_]f32{
        0.299,    0.587,    0.114,
        -0.16875, -0.33126, 0.5,
        0.5,      -0.41869, -0.08131,
    };
    const inverse = [_]f32{
        1.0, 0.0,      1.402,
        1.0, -0.34413, -0.71414,
        1.0, 1.772,    0.0,
    };
    const offsets = [_]f32{ 0, 0, 0 };
    const matrix = CustomMctMatrix{
        .num_components = 3,
        .forward = &forward,
        .inverse = &inverse,
        .offsets = &offsets,
    };

    const r_orig = [_]f32{ 100.0, 200.0, 50.0, 0.0, 255.0, 128.0 };
    const g_orig = [_]f32{ 150.0, 100.0, 75.0, 0.0, 255.0, 64.0 };
    const b_orig = [_]f32{ 200.0, 50.0, 100.0, 0.0, 255.0, 32.0 };

    const r_custom = try allocator.dupe(f32, &r_orig);
    defer allocator.free(r_custom);
    const g_custom = try allocator.dupe(f32, &g_orig);
    defer allocator.free(g_custom);
    const b_custom = try allocator.dupe(f32, &b_orig);
    defer allocator.free(b_custom);

    const r_ref = try allocator.dupe(f32, &r_orig);
    defer allocator.free(r_ref);
    const g_ref = try allocator.dupe(f32, &g_orig);
    defer allocator.free(g_ref);
    const b_ref = try allocator.dupe(f32, &b_orig);
    defer allocator.free(b_ref);

    var planes = [_][]f32{ r_custom, g_custom, b_custom };
    try applyCustomMctForward(matrix, &planes);
    forwardIct(r_ref, g_ref, b_ref);

    const tolerance: f32 = 1e-3;
    for (0..r_orig.len) |i| {
        try std.testing.expectApproxEqAbs(r_ref[i], r_custom[i], tolerance);
        try std.testing.expectApproxEqAbs(g_ref[i], g_custom[i], tolerance);
        try std.testing.expectApproxEqAbs(b_ref[i], b_custom[i], tolerance);
    }

    // Now inverse the custom transform and confirm RGB comes back.
    try applyCustomMctInverse(matrix, &planes);
    for (0..r_orig.len) |i| {
        try std.testing.expectApproxEqAbs(r_orig[i], r_custom[i], 1e-2);
        try std.testing.expectApproxEqAbs(g_orig[i], g_custom[i], 1e-2);
        try std.testing.expectApproxEqAbs(b_orig[i], b_custom[i], 1e-2);
    }
}

test "Gauss-Jordan invert 3x3 yields identity product" {
    const allocator = std.testing.allocator;
    const a = [_]f32{
        2, 1, 1,
        1, 3, 2,
        1, 0, 0,
    };
    const inv = try invertMctMatrixGaussJordan(&a, 3, allocator);
    defer allocator.free(inv);

    // Compute a * inv and check identity.
    const n: usize = 3;
    var out: [9]f32 = undefined;
    for (0..n) |i| {
        for (0..n) |j| {
            var acc: f32 = 0;
            for (0..n) |k| acc += a[i * n + k] * inv[k * n + j];
            out[i * n + j] = acc;
        }
    }
    const tol: f32 = 1e-5;
    for (0..n) |i| {
        for (0..n) |j| {
            const expected: f32 = if (i == j) 1.0 else 0.0;
            try std.testing.expectApproxEqAbs(expected, out[i * n + j], tol);
        }
    }
}

test "Gauss-Jordan reports singular matrix" {
    const allocator = std.testing.allocator;
    // All rows identical → rank 1 → singular.
    const a = [_]f32{
        1, 2, 3,
        1, 2, 3,
        1, 2, 3,
    };
    try std.testing.expectError(
        error.SingularMatrix,
        invertMctMatrixGaussJordan(&a, 3, allocator),
    );
}

test "custom MCT N=4 CMYK-like round trip" {
    const allocator = std.testing.allocator;
    // Simple invertible 4x4 transform: a diagonal scale plus a small mixing row.
    const forward = [_]f32{
        0.5, 0.5,  0.5,  0.5,
        0.5, -0.5, 0.5,  -0.5,
        0.5, 0.5,  -0.5, -0.5,
        0.5, -0.5, -0.5, 0.5,
    };
    // This matrix is orthogonal — its inverse is its transpose. Since it is
    // symmetric (check: yes) the inverse equals itself.
    const inverse = forward;
    const offsets = [_]f32{ 0, 0, 0, 0 };
    const matrix = CustomMctMatrix{
        .num_components = 4,
        .forward = &forward,
        .inverse = &inverse,
        .offsets = &offsets,
    };

    const c_orig = [_]f32{ 12.0, 34.0, 56.0, 78.0 };
    const m_orig = [_]f32{ 90.0, 11.0, 22.0, 33.0 };
    const y_orig = [_]f32{ 44.0, 55.0, 66.0, 77.0 };
    const k_orig = [_]f32{ 88.0, 99.0, 10.0, 21.0 };

    const c = try allocator.dupe(f32, &c_orig);
    defer allocator.free(c);
    const m = try allocator.dupe(f32, &m_orig);
    defer allocator.free(m);
    const y = try allocator.dupe(f32, &y_orig);
    defer allocator.free(y);
    const k = try allocator.dupe(f32, &k_orig);
    defer allocator.free(k);

    var planes = [_][]f32{ c, m, y, k };
    try applyCustomMctForward(matrix, &planes);
    try applyCustomMctInverse(matrix, &planes);

    const tol: f32 = 1e-3;
    for (0..4) |i| {
        try std.testing.expectApproxEqAbs(c_orig[i], c[i], tol);
        try std.testing.expectApproxEqAbs(m_orig[i], m[i], tol);
        try std.testing.expectApproxEqAbs(y_orig[i], y[i], tol);
        try std.testing.expectApproxEqAbs(k_orig[i], k[i], tol);
    }
}

test "custom MCT N=1 passthrough with offset" {
    const allocator = std.testing.allocator;
    const forward = [_]f32{2.0};
    const inverse = [_]f32{0.5};
    const offsets = [_]f32{10.0};
    const matrix = CustomMctMatrix{
        .num_components = 1,
        .forward = &forward,
        .inverse = &inverse,
        .offsets = &offsets,
    };

    const src = [_]f32{ 0, 10, 20, 30, -5 };
    const data = try allocator.dupe(f32, &src);
    defer allocator.free(data);
    var planes = [_][]f32{data};

    try applyCustomMctForward(matrix, &planes);
    // Forward: (x - 10) * 2
    const expected_fwd = [_]f32{ -20, 0, 20, 40, -30 };
    for (0..src.len) |i| try std.testing.expectApproxEqAbs(expected_fwd[i], data[i], 1e-6);

    try applyCustomMctInverse(matrix, &planes);
    // Inverse: y * 0.5 + 10
    for (0..src.len) |i| try std.testing.expectApproxEqAbs(src[i], data[i], 1e-6);
}

test "custom MCT rejects too-many components" {
    const over: usize = @as(usize, custom_mct_max_components) + 1;
    const forward = @as([(17 * 17)]f32, @splat(0));
    const offsets = @as([17]f32, @splat(0));
    const matrix = CustomMctMatrix{
        .num_components = @intCast(over),
        .forward = forward[0..],
        .inverse = forward[0..],
        .offsets = offsets[0..],
    };
    var dummy: [1]f32 = .{0};
    var planes: [17][]f32 = undefined;
    for (&planes) |*p| p.* = dummy[0..];
    try std.testing.expectError(error.TooManyComponents, applyCustomMctForward(matrix, &planes));
}

test "RCT round-trip is lossless" {
    const allocator = std.testing.allocator;
    const n = 6;

    const r_orig = [_]i32{ 100, 200, 50, 0, 255, 128 };
    const g_orig = [_]i32{ 150, 100, 75, 0, 255, 64 };
    const b_orig = [_]i32{ 200, 50, 100, 0, 255, 32 };

    const r = try allocator.dupe(i32, &r_orig);
    defer allocator.free(r);
    const g = try allocator.dupe(i32, &g_orig);
    defer allocator.free(g);
    const b = try allocator.dupe(i32, &b_orig);
    defer allocator.free(b);

    forwardRct(r, g, b);
    inverseRct(r, g, b);

    for (0..n) |i| {
        try std.testing.expectEqual(r_orig[i], r[i]);
        try std.testing.expectEqual(g_orig[i], g[i]);
        try std.testing.expectEqual(b_orig[i], b[i]);
    }
}

test "ICT round-trip is approximately lossless" {
    const allocator = std.testing.allocator;
    const n = 6;
    const tolerance: f32 = 1e-2;

    const r_orig = [_]f32{ 100.0, 200.0, 50.0, 0.0, 255.0, 128.0 };
    const g_orig = [_]f32{ 150.0, 100.0, 75.0, 0.0, 255.0, 64.0 };
    const b_orig = [_]f32{ 200.0, 50.0, 100.0, 0.0, 255.0, 32.0 };

    const r = try allocator.dupe(f32, &r_orig);
    defer allocator.free(r);
    const g = try allocator.dupe(f32, &g_orig);
    defer allocator.free(g);
    const b = try allocator.dupe(f32, &b_orig);
    defer allocator.free(b);

    forwardIct(r, g, b);
    inverseIct(r, g, b);

    for (0..n) |i| {
        try std.testing.expectApproxEqAbs(r_orig[i], r[i], tolerance);
        try std.testing.expectApproxEqAbs(g_orig[i], g[i], tolerance);
        try std.testing.expectApproxEqAbs(b_orig[i], b[i], tolerance);
    }
}

test "RCT with known RGB values" {
    const allocator = std.testing.allocator;

    // Pure red (255, 0, 0), pure green (0, 255, 0), pure white (255, 255, 255)
    const r = try allocator.dupe(i32, &[_]i32{ 255, 0, 255 });
    defer allocator.free(r);
    const g = try allocator.dupe(i32, &[_]i32{ 0, 255, 255 });
    defer allocator.free(g);
    const b = try allocator.dupe(i32, &[_]i32{ 0, 0, 255 });
    defer allocator.free(b);

    forwardRct(r, g, b);

    // Pure red: Y = floor((255 + 0 + 0) / 4) = 63, Cb = 0 - 0 = 0, Cr = 255 - 0 = 255
    try std.testing.expectEqual(@as(i32, 63), r[0]);
    try std.testing.expectEqual(@as(i32, 0), g[0]);
    try std.testing.expectEqual(@as(i32, 255), b[0]);

    // Pure green: Y = floor((0 + 510 + 0) / 4) = 127, Cb = 0 - 255 = -255, Cr = 0 - 255 = -255
    try std.testing.expectEqual(@as(i32, 127), r[1]);
    try std.testing.expectEqual(@as(i32, -255), g[1]);
    try std.testing.expectEqual(@as(i32, -255), b[1]);

    // Pure white: Y = floor((255 + 510 + 255) / 4) = 255, Cb = 0, Cr = 0
    try std.testing.expectEqual(@as(i32, 255), r[2]);
    try std.testing.expectEqual(@as(i32, 0), g[2]);
    try std.testing.expectEqual(@as(i32, 0), b[2]);

    // Inverse should recover original values
    inverseRct(r, g, b);

    try std.testing.expectEqual(@as(i32, 255), r[0]);
    try std.testing.expectEqual(@as(i32, 0), g[0]);
    try std.testing.expectEqual(@as(i32, 0), b[0]);

    try std.testing.expectEqual(@as(i32, 0), r[1]);
    try std.testing.expectEqual(@as(i32, 255), g[1]);
    try std.testing.expectEqual(@as(i32, 0), b[1]);

    try std.testing.expectEqual(@as(i32, 255), r[2]);
    try std.testing.expectEqual(@as(i32, 255), g[2]);
    try std.testing.expectEqual(@as(i32, 255), b[2]);
}

test "ICT with known RGB values" {
    const allocator = std.testing.allocator;
    const tolerance: f32 = 1e-2;

    // Pure red (255, 0, 0), pure green (0, 255, 0), pure white (255, 255, 255)
    const r = try allocator.dupe(f32, &[_]f32{ 255.0, 0.0, 255.0 });
    defer allocator.free(r);
    const g = try allocator.dupe(f32, &[_]f32{ 0.0, 255.0, 255.0 });
    defer allocator.free(g);
    const b = try allocator.dupe(f32, &[_]f32{ 0.0, 0.0, 255.0 });
    defer allocator.free(b);

    forwardIct(r, g, b);

    // Pure red: Y = 0.299 * 255 = 76.245, Cb = -0.16875 * 255 = -43.03125, Cr = 0.5 * 255 = 127.5
    try std.testing.expectApproxEqAbs(@as(f32, 76.245), r[0], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, -43.03125), g[0], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 127.5), b[0], tolerance);

    // Pure green: Y = 0.587 * 255 = 149.685, Cb = -0.33126 * 255 = -84.4713, Cr = -0.41869 * 255 = -106.76595
    try std.testing.expectApproxEqAbs(@as(f32, 149.685), r[1], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, -84.4713), g[1], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, -106.76595), b[1], tolerance);

    // Pure white: Y = (0.299+0.587+0.114)*255 = 255.0, Cb ~ 0, Cr ~ 0
    try std.testing.expectApproxEqAbs(@as(f32, 255.0), r[2], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), g[2], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), b[2], tolerance);

    // Inverse should recover original values
    inverseIct(r, g, b);

    try std.testing.expectApproxEqAbs(@as(f32, 255.0), r[0], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), g[0], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), b[0], tolerance);

    try std.testing.expectApproxEqAbs(@as(f32, 0.0), r[1], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 255.0), g[1], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), b[1], tolerance);

    try std.testing.expectApproxEqAbs(@as(f32, 255.0), r[2], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 255.0), g[2], tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 255.0), b[2], tolerance);
}
