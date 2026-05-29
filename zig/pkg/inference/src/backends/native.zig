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

// Optional system BLAS bindings — uses vecLib/cblas on macOS, OpenBLAS elsewhere.
const c = if (build_options.enable_system_blas) @cImport({
    if (@import("builtin").os.tag == .macos) {
        @cInclude("vecLib/cblas.h");
    } else {
        @cInclude("cblas.h");
    }
}) else struct {};

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
