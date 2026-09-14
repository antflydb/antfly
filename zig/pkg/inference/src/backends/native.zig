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
const build_options = @import("build_options");
const linalg = @import("inference_linalg");

// Optional system BLAS bindings. CBLAS enum values are stable across vecLib
// and OpenBLAS; declaring the small surface we use avoids translate-c in
// optimized builds.
const c = if (build_options.enable_system_blas) struct {
    pub const CblasRowMajor: c_int = 101;
    pub const CblasNoTrans: c_int = 111;
    pub const CblasTrans: c_int = 112;

    pub extern "c" fn cblas_sgemm(
        layout: c_int,
        transa: c_int,
        transb: c_int,
        m: c_int,
        n: c_int,
        k: c_int,
        alpha: f32,
        a: [*]const f32,
        lda: c_int,
        b: [*]const f32,
        ldb: c_int,
        beta: f32,
        c_out: [*]f32,
        ldc: c_int,
    ) void;
} else struct {};

pub const Io = std.Io;
pub const Cancelable = std.Io.Cancelable;

// --- Canonical Io-aware API ---
//
// These take an `io: std.Io` and dispatch via `linalg.sgemm*`, which uses
// `io.Group.async` for parallel work.  When `enable_system_blas` is on, Io
// is ignored: cblas owns its own thread pool, so per-call runtime dispatch
// would just thrash.

/// SGEMM: C = alpha * A @ B + beta * C
pub fn sgemm(
    io: Io,
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) Cancelable!void {
    if (!build_options.enable_system_blas) {
        return linalg.sgemm(io, m, n, k, alpha, a, b, beta, c_out);
    }
    sgemmSync(m, n, k, alpha, a, b, beta, c_out);
}

/// SGEMM with B transposed: C = alpha * A @ B^T + beta * C
/// A: [m, k], B: [n, k] (stored as [n, k]), C: [m, n]
pub fn sgemmTransB(
    io: Io,
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) Cancelable!void {
    if (!build_options.enable_system_blas) {
        return linalg.sgemmTransB(io, m, n, k, alpha, a, b, beta, c_out);
    }
    sgemmTransBSync(m, n, k, alpha, a, b, beta, c_out);
}

/// Transposed-B SGEMM writing a column panel directly into row-major C.
/// `c_out` starts at the panel's first column; `output_stride` is the full
/// matrix row width. Padding columns are neither read nor written.
pub fn sgemmTransBStrided(
    io: ?Io,
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
    output_stride: usize,
) Cancelable!void {
    if (m == 0 or n == 0) return;
    std.debug.assert(output_stride >= n);
    std.debug.assert(a.len >= m * k and b.len >= n * k);
    std.debug.assert(c_out.len >= (m - 1) * output_stride + n);
    if (io) |runtime_io| try runtime_io.checkCancel();
    if (k == 0) {
        for (0..m) |row| {
            for (c_out[row * output_stride ..][0..n]) |*value| {
                value.* = if (beta == 0) 0 else value.* * beta;
            }
        }
        return;
    }
    if (build_options.enable_system_blas) {
        c.cblas_sgemm(
            c.CblasRowMajor,
            c.CblasNoTrans,
            c.CblasTrans,
            @intCast(m),
            @intCast(n),
            @intCast(k),
            alpha,
            a.ptr,
            @intCast(@max(k, 1)),
            b.ptr,
            @intCast(@max(k, 1)),
            beta,
            c_out.ptr,
            @intCast(output_stride),
        );
        if (io) |runtime_io| try runtime_io.checkCancel();
        return;
    }
    if (output_stride == n) {
        if (io) |runtime_io| return linalg.sgemmTransB(runtime_io, m, n, k, alpha, a, b, beta, c_out);
        linalg.sgemmTransBSync(m, n, k, alpha, a, b, beta, c_out);
        return;
    }
    // The portable API has no output leading dimension. Callers processing
    // large panels should retain contiguous scratch for parallel GEMM there.
    for (0..m) |row| {
        const output_row = c_out[row * output_stride ..][0..n];
        const input_row = a[row * k ..][0..k];
        if (io) |runtime_io| {
            try linalg.sgemmTransB(runtime_io, 1, n, k, alpha, input_row, b, beta, output_row);
        } else {
            linalg.sgemmTransBSync(1, n, k, alpha, input_row, b, beta, output_row);
        }
    }
}

test "sgemmTransBStrided preserves padding offsets and beta" {
    const a = [_]f32{ 1, -2, 3, 4, 5, -6 };
    const b = [_]f32{ 1, 2, 3, -4, 5, 6, 7, 8, -9 };
    var output = [_]f32{17} ** 13;
    try sgemmTransBStrided(null, 2, 3, 3, 0.5, &a, &b, 0.25, output[1..], 6);
    for (0..2) |row| {
        for (0..3) |column| {
            var expected: f32 = 17 * 0.25;
            for (0..3) |inner| expected += 0.5 * a[row * 3 + inner] * b[column * 3 + inner];
            try std.testing.expectApproxEqAbs(expected, output[1 + row * 6 + column], 1e-5);
        }
    }
    for ([_]usize{ 0, 4, 5, 6, 10, 11, 12 }) |index| try std.testing.expectEqual(@as(f32, 17), output[index]);
    try sgemmTransBStrided(null, 0, 3, 3, 1, &.{}, &.{}, 0, &.{}, 6);
    try sgemmTransBStrided(null, 2, 3, 0, 1, &.{}, &.{}, 0, output[1..], 6);
    for (0..2) |row| {
        for (0..3) |column| try std.testing.expectEqual(@as(f32, 0), output[1 + row * 6 + column]);
    }
    for ([_]usize{ 0, 4, 5, 6, 10, 11, 12 }) |index| try std.testing.expectEqual(@as(f32, 17), output[index]);
}

/// SGEMM with f16 weights consumed directly via @floatCast (F16C / AVX-512
/// FP16 on x86).  No system-BLAS f16-weight path; always uses linalg.
pub fn sgemmTransBF16Weights(
    io: Io,
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f16,
    beta: f32,
    c_out: []f32,
) Cancelable!void {
    return linalg.sgemmTransBF16Weights(io, m, n, k, alpha, a, b, beta, c_out);
}

// --- Sync escape hatches ---
//
// No Io required.  Use when the caller doesn't have a runtime: tests,
// one-shot benchmarks, leaf utilities.  These dispatch parallel work via
// linalg's process-wide futex pool (Linux) or run synchronously (others).
// Production code that has access to an Io should prefer the canonical
// variants above so matmul work composes with the caller's thread pool.

pub fn sgemmSync(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) void {
    if (!build_options.enable_system_blas) {
        linalg.sgemmSync(m, n, k, alpha, a, b, beta, c_out);
        return;
    }
    c.cblas_sgemm(
        c.CblasRowMajor,
        c.CblasNoTrans,
        c.CblasNoTrans,
        @intCast(m),
        @intCast(n),
        @intCast(k),
        alpha,
        a.ptr,
        @intCast(k),
        b.ptr,
        @intCast(n),
        beta,
        c_out.ptr,
        @intCast(n),
    );
}

pub fn sgemmTransBSync(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) void {
    if (!build_options.enable_system_blas) {
        linalg.sgemmTransBSync(m, n, k, alpha, a, b, beta, c_out);
        return;
    }
    c.cblas_sgemm(
        c.CblasRowMajor,
        c.CblasNoTrans,
        c.CblasTrans,
        @intCast(m),
        @intCast(n),
        @intCast(k),
        alpha,
        a.ptr,
        @intCast(k),
        b.ptr,
        @intCast(k),
        beta,
        c_out.ptr,
        @intCast(n),
    );
}

pub fn sgemmTransBF16WeightsSync(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f16,
    beta: f32,
    c_out: []f32,
) void {
    linalg.sgemmTransBF16WeightsSync(m, n, k, alpha, a, b, beta, c_out);
}

/// SGEMM with A transposed: C = alpha * A^T @ B + beta * C
/// A: [k, m] (stored row-major as [k, m]), B: [k, n], C: [m, n]
/// (No Io variant exists in lib/linalg yet; KV compaction is the only caller.)
pub fn sgemmTransA(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) void {
    if (!build_options.enable_system_blas) {
        linalg.sgemmTransA(m, n, k, alpha, a, b, beta, c_out);
        return;
    }
    c.cblas_sgemm(
        c.CblasRowMajor,
        c.CblasTrans,
        c.CblasNoTrans,
        @intCast(m),
        @intCast(n),
        @intCast(k),
        alpha,
        a.ptr,
        @intCast(m),
        b.ptr,
        @intCast(n),
        beta,
        c_out.ptr,
        @intCast(n),
    );
}

pub const l2Normalize = linalg.l2Normalize;
pub const meanPool = linalg.meanPool;
