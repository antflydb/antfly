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

// Process-wide worker pool used by sgemm and other linalg kernels that want
// short, latency-sensitive parallel work without paying spawn+join costs.
//
// Implemented with raw Linux futexes (futex_wait / futex_wake) for parking
// idle workers without burning CPU.  Outside Linux we fall back to running
// jobs sequentially -- Zig 0.16 moved Mutex/Condition into std.Io which
// would require plumbing an Io context through every kernel call, defeating
// the point of the *Sync escape hatches.
//
// This file holds only the dispatch/worker/futex primitives.  Callers (e.g.
// `mod.zig`, `attention.zig`) decide their own work-size threshold and the
// shape of each job.

const std = @import("std");
const builtin = @import("builtin");

pub const have_futex = builtin.os.tag == .linux;

/// Hard cap on worker threads.  Beyond ~8, the bandwidth-limited kernels in
/// transformer inference saturate the memory bus.
pub const max_workers: usize = 8;

pub const Job = struct {
    fn_ptr: *const fn (*anyopaque) void,
    ctx: *anyopaque,
};

const Worker = struct {
    thread: std.Thread = undefined,
    // 0 = idle (waiting for work), 1 = job pending, 2 = shutdown.
    state: std.atomic.Value(u32) = .{ .raw = 0 },
    job: Job = undefined,
    pool: *Pool = undefined,
};

// Small futex-backed mutex.  std.Thread.Mutex was removed in Zig 0.16
// (mutex primitives moved to std.Io.Mutex which requires an Io to lock).
// The Sync path explicitly has no Io so we roll one inline using the same
// futex helpers the worker pool already uses.  On non-Linux the futex
// helpers are no-ops; in that case the pool itself won't initialize
// (have_futex=false short-circuits to sequential execution) and this
// mutex is never actually contended.
const SubmitMutex = struct {
    state: std.atomic.Value(u32) = .{ .raw = 0 }, // 0 = free, 1 = locked

    fn lock(self: *SubmitMutex) void {
        while (true) {
            if (@cmpxchgWeak(u32, &self.state.raw, 0, 1, .acquire, .monotonic) == null) return;
            futexWait(&self.state, 1);
        }
    }

    fn unlock(self: *SubmitMutex) void {
        self.state.store(0, .release);
        futexWake(&self.state, 1);
    }
};

const Pool = struct {
    workers: [max_workers - 1]Worker = undefined,
    worker_count: usize = 0,
    initialized: std.atomic.Value(bool) = .{ .raw = false },
    init_started: std.atomic.Value(bool) = .{ .raw = false },
    completion: std.atomic.Value(u32) = .{ .raw = 0 },
    // Serializes concurrent dispatch calls.  The shared worker job slots
    // and completion counter are not safe for simultaneous writers;
    // production servers that thread-per-request and call *Sync without
    // an Io would otherwise corrupt each other's state.  The lock spans
    // only the dispatch+wait window, so within a single dispatch the
    // workers still run in parallel.
    submit_mu: SubmitMutex = .{},
};

var pool_storage: Pool = .{};

inline fn futexWait(ptr: *const std.atomic.Value(u32), expected: u32) void {
    if (!have_futex) return;
    _ = std.os.linux.futex_3arg(@ptrCast(ptr), .{ .cmd = .WAIT, .private = true }, expected);
}

inline fn futexWake(ptr: *const std.atomic.Value(u32), max: u32) void {
    if (!have_futex) return;
    _ = std.os.linux.futex_3arg(@ptrCast(ptr), .{ .cmd = .WAKE, .private = true }, max);
}

fn workerLoop(w: *Worker) void {
    while (true) {
        // Park until state != 0.
        while (true) {
            const s = w.state.load(.acquire);
            if (s != 0) break;
            futexWait(&w.state, 0);
        }
        const s = w.state.load(.acquire);
        if (s == 2) return;
        // s == 1: run the job and reset to idle.
        w.job.fn_ptr(w.job.ctx);
        w.state.store(0, .release);
        // Decrement completion; main thread is futex-waiting on it.
        const remaining = w.pool.completion.fetchSub(1, .acq_rel) - 1;
        if (remaining == 0) futexWake(&w.pool.completion, 1);
    }
}

pub inline fn cachedCpuCount() usize {
    const Once = struct {
        var value: std.atomic.Value(usize) = .{ .raw = 0 };
    };
    const cached = Once.value.load(.acquire);
    if (cached != 0) return cached;
    const detected = std.Thread.getCpuCount() catch 1;
    Once.value.store(@max(detected, 1), .release);
    return @max(detected, 1);
}

/// Lazy-init the global pool.  Returns the actual number of background
/// workers available (which may be 0 on platforms without futex support).
pub fn ensurePool(worker_count: usize) usize {
    if (builtin.single_threaded or !have_futex) return 0;
    if (worker_count == 0) return 0;
    if (pool_storage.initialized.load(.acquire)) {
        return @min(worker_count, pool_storage.worker_count);
    }
    // Race the init: only the first thread that sets init_started actually
    // does the spawning; everyone else spins until initialized is observed.
    if (pool_storage.init_started.swap(true, .acq_rel)) {
        while (!pool_storage.initialized.load(.acquire)) std.Thread.yield() catch {};
        return @min(worker_count, pool_storage.worker_count);
    }

    const cpu = cachedCpuCount();
    const max_bg = @min(max_workers - 1, cpu -| 1);
    var spawned: usize = 0;
    while (spawned < max_bg) : (spawned += 1) {
        const w = &pool_storage.workers[spawned];
        w.* = .{ .pool = &pool_storage };
        w.thread = std.Thread.spawn(.{}, workerLoop, .{w}) catch break;
    }
    pool_storage.worker_count = spawned;
    pool_storage.initialized.store(true, .release);
    return @min(worker_count, spawned);
}

/// Async wrapper that std.Io.Group.async can dispatch.  The `Job.fn_ptr`
/// signature is `fn(*anyopaque) void`, but io.async needs a function it
/// can pass arguments to and that matches a Cancelable error union.
fn jobRunner(fn_ptr: *const fn (*anyopaque) void, ctx: *anyopaque) std.Io.Cancelable!void {
    fn_ptr(ctx);
}

/// Io-aware dispatch.  Submits jobs via `io.Group.async` so the work
/// schedules on the caller's runtime thread pool (typically a long-lived
/// `std.Io.Threaded`).  This composes with the runtime's cancellation
/// semantics: when one task fails or the group is cancelled, in-flight
/// work surfaces `error.Canceled` from `g.await`.  Use this when the
/// caller has an `Io` available (server requests, plumbed CLI calls);
/// use `dispatchJobs` (Sync) for tests, leaf utilities, and benchmarks.
pub fn dispatchJobsIo(io: std.Io, jobs: []const Job) std.Io.Cancelable!void {
    if (jobs.len == 0) return;
    if (jobs.len == 1) {
        jobs[0].fn_ptr(jobs[0].ctx);
        return;
    }
    var g: std.Io.Group = .init;
    errdefer g.cancel(io);
    for (jobs[0 .. jobs.len - 1]) |j| {
        g.async(io, jobRunner, .{ j.fn_ptr, j.ctx });
    }
    // Run the last job inline on the calling thread so a single-CPU
    // runtime still makes forward progress.
    jobs[jobs.len - 1].fn_ptr(jobs[jobs.len - 1].ctx);
    try g.await(io);
}

/// Submit `n_jobs` jobs.  Last job runs on the calling thread; the rest are
/// dispatched to pool workers.  Falls back to fully-synchronous execution if
/// the pool didn't initialize.
pub fn dispatchJobs(jobs: []const Job) void {
    if (jobs.len == 0) return;
    if (jobs.len == 1) {
        jobs[0].fn_ptr(jobs[0].ctx);
        return;
    }
    const bg_jobs = jobs.len - 1;
    const available = ensurePool(bg_jobs);
    if (available < bg_jobs) {
        // Pool unavailable (e.g., non-Linux) or undersized: run sequentially.
        for (jobs) |j| j.fn_ptr(j.ctx);
        return;
    }

    // Concurrent *Sync callers must not overlap their pool usage; see
    // Pool.submit_mu.  Held across the dispatch+wait window.
    pool_storage.submit_mu.lock();
    defer pool_storage.submit_mu.unlock();

    // Set the completion counter before signalling so a worker that finishes
    // immediately doesn't observe a stale zero.
    pool_storage.completion.store(@intCast(bg_jobs), .release);

    for (0..bg_jobs) |i| {
        const w = &pool_storage.workers[i];
        w.job = jobs[i];
        w.state.store(1, .release);
        futexWake(&w.state, 1);
    }

    // Run the last job inline on the main thread.
    jobs[bg_jobs].fn_ptr(jobs[bg_jobs].ctx);

    // Wait for all background workers via futex-wait on the counter.
    while (true) {
        const remaining = pool_storage.completion.load(.acquire);
        if (remaining == 0) break;
        futexWait(&pool_storage.completion, remaining);
    }
}

const TestCtx = struct {
    counter: *std.atomic.Value(u32),
    contribution: u32,

    fn run(raw: *anyopaque) void {
        const self: *TestCtx = @ptrCast(@alignCast(raw));
        _ = self.counter.fetchAdd(self.contribution, .acq_rel);
    }
};

test "dispatchJobs runs every submitted job exactly once" {
    var counter: std.atomic.Value(u32) = .{ .raw = 0 };
    var contexts: [4]TestCtx = .{
        .{ .counter = &counter, .contribution = 1 },
        .{ .counter = &counter, .contribution = 2 },
        .{ .counter = &counter, .contribution = 4 },
        .{ .counter = &counter, .contribution = 8 },
    };
    var jobs: [4]Job = undefined;
    for (&contexts, 0..) |*ctx, i| {
        jobs[i] = .{ .fn_ptr = TestCtx.run, .ctx = @ptrCast(ctx) };
    }
    dispatchJobs(&jobs);
    try std.testing.expectEqual(@as(u32, 15), counter.load(.acquire));
}

test "dispatchJobs handles a single job inline on the calling thread" {
    var counter: std.atomic.Value(u32) = .{ .raw = 0 };
    var ctx = TestCtx{ .counter = &counter, .contribution = 7 };
    var jobs = [_]Job{
        .{ .fn_ptr = TestCtx.run, .ctx = @ptrCast(&ctx) },
    };
    dispatchJobs(&jobs);
    try std.testing.expectEqual(@as(u32, 7), counter.load(.acquire));
}

test "dispatchJobs is reentry-safe across repeated submissions" {
    // Reusing the same job slots across many dispatch calls catches
    // completion-counter / submit-mu regressions where stale state from
    // a previous call would corrupt the next one.
    var counter: std.atomic.Value(u32) = .{ .raw = 0 };
    var contexts: [3]TestCtx = .{
        .{ .counter = &counter, .contribution = 1 },
        .{ .counter = &counter, .contribution = 1 },
        .{ .counter = &counter, .contribution = 1 },
    };
    var jobs: [3]Job = undefined;
    for (&contexts, 0..) |*ctx, i| {
        jobs[i] = .{ .fn_ptr = TestCtx.run, .ctx = @ptrCast(ctx) };
    }
    for (0..16) |_| dispatchJobs(&jobs);
    try std.testing.expectEqual(@as(u32, 48), counter.load(.acquire));
}
