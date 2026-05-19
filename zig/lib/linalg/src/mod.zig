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
const builtin = @import("builtin");
pub const primitives = @import("primitives.zig");
pub const gemm = @import("gemm.zig");
pub const pool = @import("pool.zig");
pub const attention = @import("attention.zig");
pub const layout = @import("attention_layout.zig");

const vec_len = primitives.vec_len;
const PoolJob = pool.Job;
const dispatchJobs = pool.dispatchJobs;
const cachedCpuCount = pool.cachedCpuCount;

// SGEMM kernel core, register tile, and single-threaded entry points live
// in `gemm.zig`; this file owns the threaded dispatch (Sync via the
// process-wide futex pool, Io via std.Io.Group.async) and the public Sync
// / Io wrappers.  Re-exported here so existing callers keep working.
pub const SgemmTile = gemm.SgemmTile;
pub const sgemm_tile = gemm.sgemm_tile;
pub const sgemmTransA = gemm.sgemmTransA;
pub const sgemmSequential = gemm.sgemmSequential;
pub const sgemmTransBSequential = gemm.sgemmTransBSequential;

const applyBeta = gemm.applyBeta;
const sgemmAddSlice = gemm.sgemmAddSlice;
const sgemmTransBAddSlice = gemm.sgemmTransBAddSlice;
const sgemmTransBF16AddSlice = gemm.sgemmTransBF16AddSlice;

// --- Io plumbing ---
//
// Zig 0.16 moved Mutex / Condition / thread-pool primitives into std.Io,
// because they're the abstraction point that lets a library compose with
// async runtimes, fibers, etc.  GEMM is pure CPU work and doesn't need
// cooperative semantics for its own sake, but we plumb Io through the
// public API so callers can supply *their* runtime's thread pool and so
// we have a place to add cancellation later.
//
// Two API surfaces are exposed:
//   - The canonical functions (sgemm, sgemmTransB, ...) take an Io
//     parameter, dispatch via std.Io.Group, and return Cancelable!void.
//     Callers that already have an Io (servers, CLIs, anything plumbing
//     std.Io.Threaded) should use these so matmul work composes with the
//     caller's thread pool and inherits cancellation semantics.
//   - The `*Sync` escape hatches (sgemmSync, sgemmTransBSync, ...) keep
//     the no-Io signature.  They use a process-wide raw-futex worker pool
//     internally for zero-allocation, low-latency dispatch on Linux; on
//     other OSes they execute synchronously.  Use these only when the
//     caller genuinely doesn't have an Io: tests, one-shot benchmarks,
//     leaf utility functions with no runtime context.
pub const Io = std.Io;
pub const Cancelable = Io.Cancelable;

// --- Thread pool selection ---

// Don't thread tiny matmuls; even a persistent pool's enqueue+signal costs
// ~1us per worker, so each worker needs a few hundred us of work to justify
// it.  Threshold = 4M FMAs: e.g. 77x77x512 attention QK^T (3M FMAs) stays
// single-threaded, while 77x512x512 QKV projection (20M FMAs) goes parallel.
const sgemm_thread_flops_threshold: u64 = 4_000_000;

// Hard cap on workers.  Beyond ~8 threads, the bandwidth-limited GEMM shapes
// in transformer inference saturate the memory bus.
const sgemm_max_workers: usize = pool.max_workers;

fn pickWorkers(m: usize, work: u64, min_rows_per_worker: usize) usize {
    if (work < sgemm_thread_flops_threshold) return 1;
    if (builtin.single_threaded) return 1;
    const cpu = cachedCpuCount();
    const by_rows = if (min_rows_per_worker == 0) sgemm_max_workers else m / min_rows_per_worker;
    return @max(1, @min(@min(sgemm_max_workers, cpu), by_rows));
}

inline fn rowChunkBoundary(m: usize, total: usize, idx: usize, mr_align: usize) usize {
    var b = (m * idx + total - 1) / total;
    // Round to MR boundary so each worker gets full register tiles.  The last
    // worker still owns the m-tail.
    if (idx < total) b = (b / mr_align) * mr_align;
    return b;
}

// Worker pool dispatch lives in `pool.zig`; sgemm-specific tiling thresholds
// stay here.

// --- Generic SGEMM dispatch helpers ---------------------------------------
//
// Both helpers split a `[0, m)` row range into `pickWorkers`-many chunks
// aligned to `sgemm_tile.MR`, then dispatch each chunk via the chosen
// concurrency primitive.  The per-shape wrappers (sgemmSync,
// sgemmTransBSync, ...) bring their own Ctx/AddSlice pair and let comptime
// stamp out the boilerplate; the previous incarnation of this file copied
// the same 25-line dispatch loop into each public wrapper.
//
// Sync path uses the process-wide futex pool and a typed Ctx struct (the
// pool's PoolJob requires a `*const fn (*anyopaque) void` callback).  Io
// path uses `std.Io.Group.async`, which can take any function with a
// matching `Cancelable!void` signature directly — no Ctx wrapper needed.

/// Row dispatch: per-worker contexts copied from `template`, with
/// `m_start` / `m_end` overwritten per chunk.  `CtxT` must declare those
/// fields and a static `pub fn run(raw: *anyopaque) void` that calls the
/// shape's AddSlice worker.  When `io` is null, dispatch goes through the
/// process-wide futex pool (`pool.dispatchJobs`); when non-null, through
/// `pool.dispatchJobsIo` so work composes with the caller's runtime
/// thread pool and inherits its cancellation semantics.
///
/// Single-source replacement for the previous Sync / Io dispatch
/// helpers, which built the same per-worker contexts but routed through
/// two different code paths.  Now both routes share 95% of the code.
fn dispatchRows(
    io: ?Io,
    comptime CtxT: type,
    template: CtxT,
    m: usize,
    work: u64,
) Cancelable!void {
    const workers = pickWorkers(m, work, sgemm_tile.MR);
    if (workers <= 1) {
        var ctx = template;
        ctx.m_start = 0;
        ctx.m_end = m;
        CtxT.run(@ptrCast(&ctx));
        return;
    }
    var contexts: [sgemm_max_workers]CtxT = undefined;
    var jobs: [sgemm_max_workers]PoolJob = undefined;
    var num_jobs: usize = 0;
    for (0..workers) |w| {
        const start = rowChunkBoundary(m, workers, w, sgemm_tile.MR);
        const end = if (w + 1 == workers) m else rowChunkBoundary(m, workers, w + 1, sgemm_tile.MR);
        if (start >= end) continue;
        contexts[num_jobs] = template;
        contexts[num_jobs].m_start = start;
        contexts[num_jobs].m_end = end;
        jobs[num_jobs] = .{ .fn_ptr = CtxT.run, .ctx = @ptrCast(&contexts[num_jobs]) };
        num_jobs += 1;
    }
    if (io) |io_runtime| {
        try pool.dispatchJobsIo(io_runtime, jobs[0..num_jobs]);
    } else {
        dispatchJobs(jobs[0..num_jobs]);
    }
}

/// Generic dispatch context for SGEMM entry points.  Each shape
/// instantiates this with its B-element type (f32 or f16) and the AddSlice
/// kernel that consumes that type.  The futex pool's PoolJob.fn_ptr expects
/// a `*const fn(*anyopaque) void`, so each instantiation generates its own
/// concrete `run` static method that re-casts the type-erased pointer.
///
/// The `add_slice` parameter is comptime, so each `dispatchRows` call
/// dispatches through the AddSlice it was instantiated with -- no runtime
/// indirection and no risk of mixing shapes.
fn SgemmDispatchCtx(comptime BT: type, comptime add_slice: anytype) type {
    return struct {
        m_start: usize,
        m_end: usize,
        n: usize,
        k: usize,
        alpha: f32,
        a: []const f32,
        b: []const BT,
        c_out: []f32,

        const Self = @This();

        fn run(raw: *anyopaque) void {
            const ctx: *const Self = @ptrCast(@alignCast(raw));
            add_slice(ctx.m_start, ctx.m_end, ctx.n, ctx.k, ctx.alpha, ctx.a, ctx.b, ctx.c_out);
        }
    };
}

const SgemmCtx = SgemmDispatchCtx(f32, sgemmAddSlice);
const SgemmTransBCtx = SgemmDispatchCtx(f32, sgemmTransBAddSlice);
const SgemmTransBF16Ctx = SgemmDispatchCtx(f16, sgemmTransBF16AddSlice);

// --- sgemm: C = alpha * A @ B + beta * C ---


/// Pure Zig SGEMM: C = alpha * A @ B + beta * C
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
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    const work = @as(u64, m) * @as(u64, n) * @as(u64, k);
    dispatchRows(null, SgemmCtx, .{
        .m_start = 0,
        .m_end = 0,
        .n = n,
        .k = k,
        .alpha = alpha,
        .a = a,
        .b = b,
        .c_out = c_out,
    }, m, work) catch unreachable;
}

// --- sgemmTransBF16Weights: C = alpha * A @ B^T + beta * C, B in f16 ---
//
// Halves weight memory bandwidth by consuming f16 weights directly: the inner
// K-loop loads @Vector(V, f16), upcasts to @Vector(V, f32) on-chip via F16C
// (VCVTPH2PS) or the AVX-512 FP16 instructions, then FMAs.  Accumulation
// stays in f32, so the only precision loss is whatever was already in the
// weight quantization.  Avoids the up-front f16 -> f32 weight materialization
// that the previous path did via convertTensorToOwnedF32.




/// Pure Zig SGEMM with B in f16 + transposed: C = alpha * A @ B^T + beta * C.
/// A and C are f32; B is f16 and consumed directly without prior up-cast.
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
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    const work = @as(u64, m) * @as(u64, n) * @as(u64, k);
    dispatchRows(null, SgemmTransBF16Ctx, .{
        .m_start = 0,
        .m_end = 0,
        .n = n,
        .k = k,
        .alpha = alpha,
        .a = a,
        .b = b,
        .c_out = c_out,
    }, m, work) catch unreachable;
}

// --- sgemmTransB: C = alpha * A @ B^T + beta * C ---


/// Pure Zig SGEMM with B transposed: C = alpha * A @ B^T + beta * C
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
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    const work = @as(u64, m) * @as(u64, n) * @as(u64, k);
    dispatchRows(null, SgemmTransBCtx, .{
        .m_start = 0,
        .m_end = 0,
        .n = n,
        .k = k,
        .alpha = alpha,
        .a = a,
        .b = b,
        .c_out = c_out,
    }, m, work) catch unreachable;
}

// Per-worker compute slice: process rows [m_start, m_end) and ADD into C.
// Caller has already pre-scaled C by beta.  Mirrors the previous sgemmTransB
// kernel body, but on a row range rather than [0, m).

/// Pure Zig SGEMM with A transposed: C = alpha * A^T @ B + beta * C

/// L2 normalize a batch of vectors in-place.
pub fn l2Normalize(embeddings: []f32, dim: usize) void {
    const batch = embeddings.len / dim;
    for (0..batch) |i| {
        const vec = embeddings[i * dim .. (i + 1) * dim];
        var norm: f32 = 0.0;
        for (vec) |v| norm += v * v;
        norm = @sqrt(norm);
        if (norm > 0.0) {
            for (vec) |*v| v.* /= norm;
        }
    }
}

/// Mean pool over sequence dimension with attention mask: [batch, seq, hidden] -> [batch, hidden]
pub fn meanPool(
    allocator: std.mem.Allocator,
    hidden_states: []const f32,
    attention_mask: []const i32,
    batch: usize,
    seq_len: usize,
    hidden: usize,
) ![]f32 {
    const result = try allocator.alloc(f32, batch * hidden);
    @memset(result, 0.0);

    for (0..batch) |b| {
        var count: f32 = 0.0;
        for (0..seq_len) |s| {
            if (attention_mask[b * seq_len + s] > 0) {
                const offset = (b * seq_len + s) * hidden;
                for (0..hidden) |h| {
                    result[b * hidden + h] += hidden_states[offset + h];
                }
                count += 1.0;
            }
        }
        if (count > 0.0) {
            for (0..hidden) |h| {
                result[b * hidden + h] /= count;
            }
        }
    }

    return result;
}
pub const t5RelativePositionBucket = attention.t5RelativePositionBucket;
pub const ropeCore = attention.ropeCore;
pub const flashCausalAttentionHost = attention.flashCausalAttentionHost;
pub const flashAttentionHost = attention.flashAttentionHost;
pub const crossAttentionHost = attention.crossAttentionHost;
pub const debertaDisentangledAttentionHost = attention.debertaDisentangledAttentionHost;
pub const WindowPack = layout.WindowPack;
pub const padTokensToWindows = layout.padTokensToWindows;
pub const unpadWindowTokens = layout.unpadWindowTokens;
pub const channelAttention = attention.channelAttention;
pub const dot = primitives.dot;
pub const axpy = primitives.axpy;

// Register-tiled microkernel: accumulate alpha * A_block @ B_block into a
// MR x (NR_VECS * V) tile of C in registers, then add to C in one pass.
// A_block is MR rows of A starting at row `i_base`; lda = k.
// B_block is the full panel of B starting at column `j_base`; ldb = n.
// C_block has stride ldc = n.  Caller is responsible for pre-scaling C by beta.
// --- Io-aware variants ---
//
// Same kernels, but parallel dispatch goes through `std.Io.Group.async` so
// the work is scheduled on the caller's runtime thread pool (typically a
// long-lived `std.Io.Threaded` instance owned by the application).  This
// avoids hand-rolling a process-wide thread pool inside lib/linalg and
// gives us a future hook for cancellation and async runtimes.
//
// The implementation reuses the same `*AddSlice` worker functions as the
// legacy void variants -- only the dispatch differs.  Single-threaded /
// below-threshold paths run inline on the calling thread (no Group).

// Note: the Io path also uses `pickWorkers`; over-emitting is safe because
// `io.Group.async` falls back to synchronous execution when no worker
// thread is available.

/// SGEMM: C = alpha * A @ B + beta * C.  Parallel work is dispatched via
/// `io.Group.async`; pass any `std.Io` implementation (typically from
/// `std.Io.Threaded`).  Use `sgemmSync` if you don't have an Io.
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
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    const work = @as(u64, m) * @as(u64, n) * @as(u64, k);
    return dispatchRows(io, SgemmCtx, .{
        .m_start = 0,
        .m_end = 0,
        .n = n,
        .k = k,
        .alpha = alpha,
        .a = a,
        .b = b,
        .c_out = c_out,
    }, m, work);
}

/// SGEMM with B transposed: C = alpha * A @ B^T + beta * C.  Parallel work
/// dispatches via `io.Group.async`.  Use `sgemmTransBSync` for a no-Io call.
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
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    const work = @as(u64, m) * @as(u64, n) * @as(u64, k);
    return dispatchRows(io, SgemmTransBCtx, .{
        .m_start = 0,
        .m_end = 0,
        .n = n,
        .k = k,
        .alpha = alpha,
        .a = a,
        .b = b,
        .c_out = c_out,
    }, m, work);
}

/// SGEMM with f16 weights consumed directly via @floatCast (F16C / AVX-512
/// FP16 on x86); A and C are f32, accumulation in f32.  Use
/// `sgemmTransBF16WeightsSync` for a no-Io call.
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
    if (m == 0 or n == 0) return;
    applyBeta(c_out[0 .. m * n], beta);
    if (k == 0) return;
    const work = @as(u64, m) * @as(u64, n) * @as(u64, k);
    return dispatchRows(io, SgemmTransBF16Ctx, .{
        .m_start = 0,
        .m_end = 0,
        .n = n,
        .k = k,
        .alpha = alpha,
        .a = a,
        .b = b,
        .c_out = c_out,
    }, m, work);
}

// Pull in tests defined in our submodules so `zig build test-linalg` runs
// them too.  Without this, only tests living directly in mod.zig are picked
// up by the test runner.
test {
    std.testing.refAllDecls(attention);
    std.testing.refAllDecls(primitives);
    std.testing.refAllDecls(layout);
}

test "l2 normalize" {
    var data = [_]f32{ 3.0, 4.0, 0.0, 1.0 };
    l2Normalize(&data, 2);
    try std.testing.expectApproxEqAbs(@as(f32, 0.6), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), data[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), data[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), data[3], 1e-6);
}

test "sgemm identity multiply" {
    const a = [_]f32{ 1, 0, 0, 1 };
    const b = [_]f32{ 1, 2, 3, 4 };
    var result = [_]f32{ 0, 0, 0, 0 };
    sgemmSync(2, 2, 2, 1.0, &a, &b, 0.0, &result);
    try std.testing.expectEqualSlices(f32, &b, &result);
}

test "sgemmTransA computes A^T @ B" {
    const a = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const b = [_]f32{ 1, 2, 3 };
    var result = [_]f32{ 0, 0 };
    sgemmTransA(2, 1, 3, 1.0, &a, &b, 0.0, &result);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), result[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 28.0), result[1], 1e-5);
}

test "meanPool with mask" {
    const allocator = std.testing.allocator;
    const hidden_states = [_]f32{
        1,   2,
        3,   4,
        100, 100,
    };
    const attention_mask = [_]i32{ 1, 1, 0 };
    const pooled = try meanPool(allocator, &hidden_states, &attention_mask, 1, 3, 2);
    defer allocator.free(pooled);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), pooled[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), pooled[1], 1e-6);
}

// Reference scalar matmul for sgemmTransB (B is row-major [n, k]).
fn refMatmulTransBForTest(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c: []f32,
) void {
    for (0..m) |i| {
        for (0..n) |j| {
            var sum: f32 = 0.0;
            for (0..k) |l| sum += a[i * k + l] * b[j * k + l];
            c[i * n + j] = alpha * sum + beta * c[i * n + j];
        }
    }
}

test "sgemmTransB threaded path matches reference" {
    const allocator = std.testing.allocator;
    // Shape large enough to trip the >= 4M FMA threading threshold.
    const m: usize = 96;
    const n: usize = 96;
    const k: usize = 480;

    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, n * k);
    defer allocator.free(b);
    const c_ref = try allocator.alloc(f32, m * n);
    defer allocator.free(c_ref);
    const c_test = try allocator.alloc(f32, m * n);
    defer allocator.free(c_test);

    var prng = std.Random.DefaultPrng.init(0xa11ce);
    const rand = prng.random();
    for (a) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    for (b) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    @memset(c_ref, 0);
    @memset(c_test, 0);

    refMatmulTransBForTest(m, n, k, 1.0, a, b, 0.0, c_ref);
    sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c_test);

    for (c_ref, c_test) |x, y| try std.testing.expect(@abs(x - y) < 5e-4);
}

// The canonical sgemmTransB takes an Io and dispatches via io.Group.async;
// this test validates that path against sgemmTransBSync (which uses the
// process-wide futex pool).  Both should produce identical results.
test "sgemmTransB Io path matches sync via std.Io.Threaded" {
    const allocator = std.testing.allocator;
    if (builtin.single_threaded) return error.SkipZigTest;

    var threaded = Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const m: usize = 96;
    const n: usize = 96;
    const k: usize = 480;

    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, n * k);
    defer allocator.free(b);
    const c_sync = try allocator.alloc(f32, m * n);
    defer allocator.free(c_sync);
    const c_io = try allocator.alloc(f32, m * n);
    defer allocator.free(c_io);

    var prng = std.Random.DefaultPrng.init(0xa11ce);
    const rand = prng.random();
    for (a) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    for (b) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    @memset(c_sync, 0);
    @memset(c_io, 0);

    sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c_sync);
    try sgemmTransB(io, m, n, k, 1.0, a, b, 0.0, c_io);

    for (c_sync, c_io) |x, y| try std.testing.expect(@abs(x - y) < 1e-4);
}

// Concurrent *Sync callers share the process-wide futex pool.  Without the
// submit_mu lock, simultaneous dispatchJobs calls overwrite each other's
// worker slots and corrupt the completion counter.  Run several threads in
// parallel, each computing the same matmul, and assert every output matches
// the single-threaded reference exactly.
test "sgemmTransBSync is safe under concurrent callers" {
    const allocator = std.testing.allocator;
    if (builtin.single_threaded) return error.SkipZigTest;

    // Shape is large enough to actually exercise the pool dispatch path
    // (>= sgemm_thread_flops_threshold = 4M FMAs).
    const m: usize = 96;
    const n: usize = 96;
    const k: usize = 480;
    const num_threads: usize = 4;
    const calls_per_thread: usize = 8;

    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, n * k);
    defer allocator.free(b);
    var prng = std.Random.DefaultPrng.init(0xc0ffee);
    const rand = prng.random();
    for (a) |*v| v.* = rand.float(f32) * 2 - 1;
    for (b) |*v| v.* = rand.float(f32) * 2 - 1;

    // Reference: single-threaded run.
    const c_ref = try allocator.alloc(f32, m * n);
    defer allocator.free(c_ref);
    @memset(c_ref, 0);
    sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c_ref);

    const Worker = struct {
        fn run(buf_a: []const f32, buf_b: []const f32, out: []f32, M: usize, N: usize, K: usize, calls: usize) void {
            for (0..calls) |_| {
                @memset(out, 0);
                sgemmTransBSync(M, N, K, 1.0, buf_a, buf_b, 0.0, out);
            }
        }
    };

    var threads: [num_threads]std.Thread = undefined;
    var per_thread_outputs: [num_threads][]f32 = undefined;
    for (0..num_threads) |i| {
        per_thread_outputs[i] = try allocator.alloc(f32, m * n);
    }
    defer for (per_thread_outputs) |buf| allocator.free(buf);

    for (0..num_threads) |i| {
        threads[i] = try std.Thread.spawn(.{}, Worker.run, .{ a, b, per_thread_outputs[i], m, n, k, calls_per_thread });
    }
    for (threads) |t| t.join();

    for (per_thread_outputs) |buf| {
        for (c_ref, buf) |x, y| try std.testing.expect(@abs(x - y) < 1e-3);
    }
}

// j-outer loop has separate code paths for the j-main loop, the j-tail
// (n not divisible by NR=2), the m-main loop, and the m-tail.  Use a shape
// that exercises all four and asserts equality with the reference.
test "sgemmTransB j-outer covers j and m tails" {
    const allocator = std.testing.allocator;
    // m=10 -> two MR=4 tiles + 2-row m-tail; n=21 -> ten NR=2 tiles + 1 j-tail.
    const m: usize = 10;
    const n: usize = 21;
    const k: usize = 35;

    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, n * k);
    defer allocator.free(b);
    const c_ref = try allocator.alloc(f32, m * n);
    defer allocator.free(c_ref);
    const c_test = try allocator.alloc(f32, m * n);
    defer allocator.free(c_test);

    var prng = std.Random.DefaultPrng.init(0xbeef);
    const rand = prng.random();
    for (a) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    for (b) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    @memset(c_ref, 0);
    @memset(c_test, 0);
    refMatmulTransBForTest(m, n, k, 1.0, a, b, 0.0, c_ref);
    sgemmTransBSync(m, n, k, 1.0, a, b, 0.0, c_test);
    for (c_ref, c_test) |x, y| try std.testing.expect(@abs(x - y) < 1e-4);

    // Same shape, beta != 0 to exercise the pre-scale path against the j-outer kernel.
    for (c_ref) |*v| v.* = rand.float(f32) * 0.5;
    @memcpy(c_test, c_ref);
    refMatmulTransBForTest(m, n, k, 0.5, a, b, 0.75, c_ref);
    sgemmTransBSync(m, n, k, 0.5, a, b, 0.75, c_test);
    for (c_ref, c_test) |x, y| try std.testing.expect(@abs(x - y) < 1e-4);
}

test "sgemm Kc-blocked path matches reference for large K" {
    const allocator = std.testing.allocator;
    // K > Kc=256 forces multiple Kc-blocks.
    const m: usize = 16;
    const n: usize = 32;
    const k: usize = 600;

    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b = try allocator.alloc(f32, k * n);
    defer allocator.free(b);
    const c_ref = try allocator.alloc(f32, m * n);
    defer allocator.free(c_ref);
    const c_test = try allocator.alloc(f32, m * n);
    defer allocator.free(c_test);

    var prng = std.Random.DefaultPrng.init(0xb0b);
    const rand = prng.random();
    for (a) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    for (b) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    @memset(c_ref, 0);
    @memset(c_test, 0);

    for (0..m) |i| {
        for (0..n) |j| {
            var sum: f32 = 0.0;
            for (0..k) |l| sum += a[i * k + l] * b[l * n + j];
            c_ref[i * n + j] = sum;
        }
    }
    sgemmSync(m, n, k, 1.0, a, b, 0.0, c_test);
    for (c_ref, c_test) |x, y| try std.testing.expect(@abs(x - y) < 5e-4);
}

test "sgemmTransBF16Weights matches sgemmTransB on round-tripped weights" {
    const allocator = std.testing.allocator;
    const m: usize = 16;
    const n: usize = 64;
    const k: usize = 96;

    const a = try allocator.alloc(f32, m * k);
    defer allocator.free(a);
    const b_f32 = try allocator.alloc(f32, n * k);
    defer allocator.free(b_f32);
    const b_f16 = try allocator.alloc(f16, n * k);
    defer allocator.free(b_f16);
    const c_f32 = try allocator.alloc(f32, m * n);
    defer allocator.free(c_f32);
    const c_f16 = try allocator.alloc(f32, m * n);
    defer allocator.free(c_f16);

    var prng = std.Random.DefaultPrng.init(0xd00d);
    const rand = prng.random();
    for (a) |*v| v.* = rand.float(f32) * 2.0 - 1.0;
    // Generate weights in f16 and round-trip to f32 so the two paths see
    // bit-identical inputs (no precision delta from the cast).
    for (b_f16, b_f32) |*v16, *v32| {
        const x = rand.float(f32) * 2.0 - 1.0;
        v16.* = @floatCast(x);
        v32.* = @floatCast(v16.*);
    }

    @memset(c_f32, 0);
    @memset(c_f16, 0);
    sgemmTransBSync(m, n, k, 1.0, a, b_f32, 0.0, c_f32);
    sgemmTransBF16WeightsSync(m, n, k, 1.0, a, b_f16, 0.0, c_f16);

    for (c_f32, c_f16) |x, y| try std.testing.expect(@abs(x - y) < 1e-4);
}

// Reference for plain sgemm (non-transposed B): C += alpha * A @ B + beta * C
// Used by the parity tests below to validate any tile-shape variation.
fn refMatmulForTest(
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c_out: []f32,
) void {
    for (0..m) |i| {
        for (0..n) |j| {
            var sum: f32 = 0.0;
            for (0..k) |l| sum += a[i * k + l] * b[l * n + j];
            c_out[i * n + j] = beta * c_out[i * n + j] + alpha * sum;
        }
    }
}

// Sweep of M-tail / N-tail / K-tail boundaries that a portable register tile
// has to handle correctly.  With sgemm_tile.MR potentially != 4 (today: 6 on
// AVX2/AVX-512/aarch64), the existing fixed-shape tests cover only one
// configuration; this sweep makes sure we keep parity for every M and N tail
// from 0 up to MR-1 / NR*V-1 plus a representative selection of K tails.
test "sgemmTransBSync matches reference across MR/NR/K tail boundaries" {
    const allocator = std.testing.allocator;
    const V = vec_len;
    const MR = sgemm_tile.MR;
    const NR_PANEL = sgemm_tile.NR; // B-rows per j-block
    var prng = std.Random.DefaultPrng.init(0x70F1_DEAD);

    const m_main: usize = 2 * MR;
    const n_main: usize = 4 * NR_PANEL;
    const m_tails = [_]usize{ 0, 1, MR - 1 };
    const n_tails = [_]usize{ 0, 1, NR_PANEL - 1, NR_PANEL };
    const k_values = [_]usize{ 1, V - 1, V, V + 1, 2 * V + 3, 3 * V };

    for (m_tails) |mt| for (n_tails) |nt| for (k_values) |k| {
        const m = m_main + mt;
        const n = n_main + nt;
        if (m == 0 or n == 0 or k == 0) continue;

        const a = try allocator.alloc(f32, m * k);
        defer allocator.free(a);
        const b = try allocator.alloc(f32, n * k);
        defer allocator.free(b);
        const c_ref = try allocator.alloc(f32, m * n);
        defer allocator.free(c_ref);
        const c_test = try allocator.alloc(f32, m * n);
        defer allocator.free(c_test);

        for (a) |*v| v.* = prng.random().float(f32) * 2.0 - 1.0;
        for (b) |*v| v.* = prng.random().float(f32) * 2.0 - 1.0;
        for (c_ref) |*v| v.* = prng.random().float(f32) * 0.25;
        @memcpy(c_test, c_ref);

        refMatmulTransBForTest(m, n, k, 0.7, a, b, 0.3, c_ref);
        sgemmTransBSync(m, n, k, 0.7, a, b, 0.3, c_test);
        for (c_ref, c_test) |x, y| try std.testing.expect(@abs(x - y) < 5e-4);
    };
}

test "sgemmSync matches reference across MR/NR/K tail boundaries" {
    const allocator = std.testing.allocator;
    const V = vec_len;
    const MR = sgemm_tile.MR;
    const NR_VECS = sgemm_tile.NR;
    const NR_MAIN = NR_VECS * V; // n column tile width
    var prng = std.Random.DefaultPrng.init(0x55A1_C0DE);

    const m_main: usize = 2 * MR;
    const n_main: usize = 2 * NR_MAIN;
    const m_tails = [_]usize{ 0, 1, MR - 1 };
    const n_tails = [_]usize{ 0, 1, V - 1, V, V + 1 };
    // Includes Kc-block boundaries (KC=128 on AVX-512, 256 elsewhere) so
    // the multi-Kc accumulation path is exercised too.
    const k_values = [_]usize{ 1, V, 64, 257 };

    for (m_tails) |mt| for (n_tails) |nt| for (k_values) |k| {
        const m = m_main + mt;
        const n = n_main + nt;

        const a = try allocator.alloc(f32, m * k);
        defer allocator.free(a);
        const b = try allocator.alloc(f32, k * n);
        defer allocator.free(b);
        const c_ref = try allocator.alloc(f32, m * n);
        defer allocator.free(c_ref);
        const c_test = try allocator.alloc(f32, m * n);
        defer allocator.free(c_test);

        for (a) |*v| v.* = prng.random().float(f32) * 2.0 - 1.0;
        for (b) |*v| v.* = prng.random().float(f32) * 2.0 - 1.0;
        for (c_ref) |*v| v.* = prng.random().float(f32) * 0.25;
        @memcpy(c_test, c_ref);

        refMatmulForTest(m, n, k, 0.5, a, b, 0.25, c_ref);
        sgemmSync(m, n, k, 0.5, a, b, 0.25, c_test);
        for (c_ref, c_test) |x, y| try std.testing.expect(@abs(x - y) < 5e-4);
    };
}

// Lock down the production CLIP/CLAP matmul shapes against a reference
// implementation.  These are the exact (m, n, k) triples the inference path
// drives; if a future tile change breaks them the failure surfaces here
// instead of as a numerical drift in the model.
test "sgemmTransBSync matches reference for CLIP/CLAP production shapes" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xC11C_AAAA);

    const cases = [_]struct { m: usize, n: usize, k: usize }{
        // CLIP-B-Patch16 text encoder QKV projection: 77 x 512 x 512.
        .{ .m = 77, .n = 512, .k = 512 },
        // CLIP-B-Patch16 vision encoder QKV projection: 257 x 768 x 768.
        .{ .m = 257, .n = 768, .k = 768 },
        // CLIP-B MLP up-projection (4x widening): 77 x 2048 x 512.
        .{ .m = 77, .n = 2048, .k = 512 },
        // CLIP-L vision MLP down-projection: 257 x 1024 x 4096.
        .{ .m = 257, .n = 1024, .k = 4096 },
    };

    for (cases) |c| {
        const a = try allocator.alloc(f32, c.m * c.k);
        defer allocator.free(a);
        const b = try allocator.alloc(f32, c.n * c.k);
        defer allocator.free(b);
        const c_ref = try allocator.alloc(f32, c.m * c.n);
        defer allocator.free(c_ref);
        const c_test = try allocator.alloc(f32, c.m * c.n);
        defer allocator.free(c_test);

        for (a) |*v| v.* = prng.random().float(f32) * 0.1;
        for (b) |*v| v.* = prng.random().float(f32) * 0.1;
        @memset(c_ref, 0);
        @memset(c_test, 0);

        refMatmulTransBForTest(c.m, c.n, c.k, 1.0, a, b, 0.0, c_ref);
        sgemmTransBSync(c.m, c.n, c.k, 1.0, a, b, 0.0, c_test);
        // Larger tolerance than the small tests — accumulation order
        // differences across a 4096-K dot product produce ~1e-3 drift.
        for (c_ref, c_test) |x, y| try std.testing.expect(@abs(x - y) < 5e-3);
    }
}
