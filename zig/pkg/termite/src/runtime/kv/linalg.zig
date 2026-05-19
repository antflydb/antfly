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

// Dense linear algebra utilities for KV cache compaction.
//
// Provides a small Cholesky solver and matrix helpers for the OLS
// (ordinary least squares) solve used by Attention Matching compaction.
// System sizes are M x M where M = retained keys (typically 50-200).

const std = @import("std");
const native = @import("../../backends/native.zig");

const VEC_LEN = 8;
const F32xN = @Vector(VEC_LEN, f32);

/// Solve A * X = B in-place via Cholesky decomposition where A is [n x n]
/// symmetric positive definite (row-major) and B is [n x nrhs] (row-major).
/// On return, the lower triangle of A contains L (L * L^T = A) and B contains X.
pub fn choleskySolve(A: []f32, B: []f32, n: usize, nrhs: usize) !void {
    if (n == 0) return;

    // Cholesky factorization: A = L * L^T (in-place, lower triangle).
    for (0..n) |j| {
        // Diagonal element: L[j,j] = sqrt(A[j,j] - sum_k L[j,k]^2)
        var diag = A[j * n + j];
        diag -= dotSelf(A[j * n ..].ptr, j);
        if (diag <= 0) return error.NotPositiveDefinite;
        const l_jj = @sqrt(diag);
        A[j * n + j] = l_jj;
        const inv_jj = 1.0 / l_jj;

        // Off-diagonal: L[i,j] = (A[i,j] - sum_k L[i,k]*L[j,k]) / L[j,j]
        for (j + 1..n) |i| {
            var val = A[i * n + j];
            val -= dotProd(A[i * n ..].ptr, A[j * n ..].ptr, j);
            A[i * n + j] = val * inv_jj;
        }
    }

    // Forward substitution: L * Y = B  (solve for Y, store in B).
    // Vectorized across nrhs (= head_dim, typically 64-128).
    for (0..n) |i| {
        const inv_splat: F32xN = @splat(1.0 / A[i * n + i]);
        for (0..i) |k| {
            const l_ik: F32xN = @splat(A[i * n + k]);
            const b_row_i = B[i * nrhs ..][0..nrhs];
            const b_row_k = B[k * nrhs ..][0..nrhs];
            var r: usize = 0;
            while (r + VEC_LEN <= nrhs) : (r += VEC_LEN) {
                const bi: F32xN = b_row_i[r..][0..VEC_LEN].*;
                const bk: F32xN = b_row_k[r..][0..VEC_LEN].*;
                b_row_i[r..][0..VEC_LEN].* = bi - l_ik * bk;
            }
            while (r < nrhs) : (r += 1) {
                b_row_i[r] -= A[i * n + k] * b_row_k[r];
            }
        }
        // Scale row by 1/L[i,i].
        const b_row = B[i * nrhs ..][0..nrhs];
        var r: usize = 0;
        while (r + VEC_LEN <= nrhs) : (r += VEC_LEN) {
            const v: F32xN = b_row[r..][0..VEC_LEN].*;
            b_row[r..][0..VEC_LEN].* = v * inv_splat;
        }
        while (r < nrhs) : (r += 1) {
            b_row[r] *= 1.0 / A[i * n + i];
        }
    }

    // Back substitution: L^T * X = Y  (solve for X, store in B).
    // Process rows from n-1 down to 0, vectorized across nrhs.
    var i_plus: usize = n;
    while (i_plus > 0) {
        i_plus -= 1;
        const i = i_plus;
        const inv_splat: F32xN = @splat(1.0 / A[i * n + i]);
        for (i + 1..n) |k| {
            const l_ki: F32xN = @splat(A[k * n + i]);
            const b_row_i = B[i * nrhs ..][0..nrhs];
            const b_row_k = B[k * nrhs ..][0..nrhs];
            var r: usize = 0;
            while (r + VEC_LEN <= nrhs) : (r += VEC_LEN) {
                const bi: F32xN = b_row_i[r..][0..VEC_LEN].*;
                const bk: F32xN = b_row_k[r..][0..VEC_LEN].*;
                b_row_i[r..][0..VEC_LEN].* = bi - l_ki * bk;
            }
            while (r < nrhs) : (r += 1) {
                b_row_i[r] -= A[k * n + i] * b_row_k[r];
            }
        }
        // Scale row by 1/L[i,i].
        const b_row = B[i * nrhs ..][0..nrhs];
        var r: usize = 0;
        while (r + VEC_LEN <= nrhs) : (r += VEC_LEN) {
            const v: F32xN = b_row[r..][0..VEC_LEN].*;
            b_row[r..][0..VEC_LEN].* = v * inv_splat;
        }
        while (r < nrhs) : (r += 1) {
            b_row[r] *= 1.0 / A[i * n + i];
        }
    }
}

/// Compute A^T * A -> out [n x n], where A is [m x n] row-major.
pub fn matmulAtA(A: []const f32, m: usize, n: usize, out: []f32) void {
    @memset(out[0 .. n * n], 0);
    native.sgemmTransA(n, n, m, 1.0, A, A, 0.0, out);
}

/// Compute A^T * B -> out [k x n], where A is [m x k] and B is [m x n], both row-major.
pub fn matmulAtB(A: []const f32, B: []const f32, m: usize, k: usize, n: usize, out: []f32) void {
    @memset(out[0 .. k * n], 0);
    native.sgemmTransA(k, n, m, 1.0, A, B, 0.0, out);
}

/// SIMD dot product of a[0..len] with itself (sum of squares).
fn dotSelf(a: [*]const f32, len: usize) f32 {
    var acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= len) : (i += VEC_LEN) {
        const v: F32xN = a[i..][0..VEC_LEN].*;
        acc += v * v;
    }
    var sum = @reduce(.Add, acc);
    while (i < len) : (i += 1) {
        sum += a[i] * a[i];
    }
    return sum;
}

/// SIMD dot product of a[0..len] and b[0..len].
fn dotProd(a: [*]const f32, b: [*]const f32, len: usize) f32 {
    var acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= len) : (i += VEC_LEN) {
        const va: F32xN = a[i..][0..VEC_LEN].*;
        const vb: F32xN = b[i..][0..VEC_LEN].*;
        acc += va * vb;
    }
    var sum = @reduce(.Add, acc);
    while (i < len) : (i += 1) {
        sum += a[i] * b[i];
    }
    return sum;
}

// --- Tests ---

test "choleskySolve 2x2 identity" {
    // A = I, B = [3, 7] -> X = [3, 7]
    var a = [_]f32{ 1, 0, 0, 1 };
    var b = [_]f32{ 3, 7 };
    try choleskySolve(&a, &b, 2, 1);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), b[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 7.0), b[1], 1e-5);
}

test "choleskySolve 3x3 known system" {
    // A = [[4,2,1],[2,5,3],[1,3,6]], B = [1,2,3]
    // Solution (via numpy): x ≈ [0.08955, 0.10448, 0.43284]
    var a = [_]f32{ 4, 2, 1, 2, 5, 3, 1, 3, 6 };
    var b = [_]f32{ 1, 2, 3 };
    try choleskySolve(&a, &b, 3, 1);
    try std.testing.expectApproxEqAbs(@as(f32, 0.08955), b[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.10448), b[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.43284), b[2], 1e-4);
}

test "choleskySolve 3x3 multiple rhs" {
    // Same A, two right-hand sides: B = [[1,10],[2,20],[3,30]]
    // Second RHS is 10x the first, so solution should be 10x too.
    var a = [_]f32{ 4, 2, 1, 2, 5, 3, 1, 3, 6 };
    var b = [_]f32{ 1, 10, 2, 20, 3, 30 };
    try choleskySolve(&a, &b, 3, 2);
    try std.testing.expectApproxEqAbs(@as(f32, 0.08955), b[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8955), b[1], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 0.43284), b[4], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 4.3284), b[5], 1e-3);
}

test "choleskySolve not positive definite" {
    // A = [[1,0],[0,-1]] is not SPD.
    var a = [_]f32{ 1, 0, 0, -1 };
    var b = [_]f32{ 1, 1 };
    try std.testing.expectError(error.NotPositiveDefinite, choleskySolve(&a, &b, 2, 1));
}

test "choleskySolve SIMD substitution with wide rhs" {
    // 2x2 system with nrhs=10 (> VEC_LEN=8, exercises both SIMD and scalar remainder).
    // A = [[4,1],[1,3]], B = [[1,2,3,4,5,6,7,8,9,10],[2,4,6,8,10,12,14,16,18,20]]
    // Solution: x0 = (3*b0 - b1)/11, x1 = (4*b1 - b0)/11
    var a = [_]f32{ 4, 1, 1, 3 };
    var b = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20 };
    try choleskySolve(&a, &b, 2, 10);
    // Row 0: x[0,r] = (3*b0[r] - b1[r]) / 11
    // For r=0: (3*1 - 2)/11 = 1/11 ≈ 0.0909
    try std.testing.expectApproxEqAbs(@as(f32, 1.0 / 11.0), b[0], 1e-5);
    // For r=9: (3*10 - 20)/11 = 10/11 ≈ 0.9091
    try std.testing.expectApproxEqAbs(@as(f32, 10.0 / 11.0), b[9], 1e-5);
    // Row 1: x[1,r] = (4*b1[r] - b0[r]) / 11
    // For r=0: (4*2 - 1)/11 = 7/11 ≈ 0.6364
    try std.testing.expectApproxEqAbs(@as(f32, 7.0 / 11.0), b[10], 1e-5);
}

test "matmulAtA computes A^T * A" {
    // A = [2,3]: [[1,2,3],[4,5,6]]
    // A^T * A = [3,3]: [[17,22,27],[22,29,36],[27,36,45]]
    const a = [_]f32{ 1, 2, 3, 4, 5, 6 };
    var out: [9]f32 = undefined;
    matmulAtA(&a, 2, 3, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 17.0), out[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), out[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 29.0), out[4], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 45.0), out[8], 1e-5);
}

test "matmulAtB computes A^T * B" {
    // A = [3,2]: [[1,2],[3,4],[5,6]]
    // B = [3,1]: [[1],[2],[3]]
    // A^T * B = [2,1]: [[22],[28]]
    const a = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const b = [_]f32{ 1, 2, 3 };
    var out: [2]f32 = undefined;
    matmulAtB(&a, &b, 3, 2, 1, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), out[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 28.0), out[1], 1e-5);
}
