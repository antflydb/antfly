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

//! Bounded Io scheduling for synchronous kernel entry points. The legacy
//! Sync API keeps a process-lifetime owner on Linux; other platforms retain
//! their sequential fallback. Io-aware callers use their own scheduling owner.

const std = @import("std");
const builtin = @import("builtin");

pub const max_workers: usize = 8;
pub const supports_sync_parallelism = !builtin.single_threaded and builtin.os.tag == .linux;
threadlocal var scoped_worker_limit: ?usize = null;

/// Synchronous CPU work borrows this many admitted workers. Scope lifetime
/// stays on one OS thread; dispatched children receive a one-worker scope so
/// nested kernels cannot multiply the parent's admitted team.
pub const WorkerScope = struct {
    previous: ?usize,
    limit: usize,
    thread: if (builtin.single_threaded) void else std.Thread.Id,

    pub fn init(workers: usize) !WorkerScope {
        if (workers == 0) return error.InvalidRunnableDemand;
        const previous = scoped_worker_limit;
        const limit = @min(workers, previous orelse workers);
        scoped_worker_limit = limit;
        return .{ .previous = previous, .limit = limit, .thread = if (comptime builtin.single_threaded) {} else std.Thread.getCurrentId() };
    }

    pub fn deinit(self: *WorkerScope) void {
        if (comptime !builtin.single_threaded) std.debug.assert(self.thread == std.Thread.getCurrentId());
        std.debug.assert(scoped_worker_limit == self.limit);
        scoped_worker_limit = self.previous;
        self.* = undefined;
    }
};

pub fn scopedWorkerLimit() ?usize {
    return scoped_worker_limit;
}
pub const Job = struct {
    fn_ptr: *const fn (*anyopaque) void,
    ctx: *anyopaque,
};

const SyncPool = struct {
    io_impl: if (supports_sync_parallelism) std.Io.Threaded else void,
    submit_mutex: std.Io.Mutex = .init,
    capacity: usize,

    fn init(capacity: usize) SyncPool {
        if (comptime supports_sync_parallelism) {
            const bounded = @min(capacity, max_workers - 1);
            return .{
                .capacity = bounded,
                .io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{
                    .async_limit = .limited(bounded),
                    .concurrent_limit = .limited(bounded),
                }),
            };
        } else {
            return .{ .capacity = 0, .io_impl = {} };
        }
    }

    pub fn deinit(self: *SyncPool) void {
        if (comptime supports_sync_parallelism) self.io_impl.deinit();
    }

    fn dispatch(self: *SyncPool, jobs: []const Job) void {
        if (comptime !supports_sync_parallelism) {
            for (jobs) |job| job.fn_ptr(job.ctx);
            return;
        } else if (jobs.len <= 1 or jobs.len - 1 > self.capacity) {
            for (jobs) |job| job.fn_ptr(job.ctx);
            return;
        }
        if (comptime supports_sync_parallelism) {
            const io = self.io_impl.io();
            // Sync kernels promise completed output and have no cancellation
            // result. Keep all jobs and caller-owned buffers alive through drain.
            const protection = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(protection);
            self.submit_mutex.lockUncancelable(io);
            defer self.submit_mutex.unlock(io);
            dispatchJobsIo(io, jobs) catch unreachable;
        }
    }
};

// The Sync compatibility API has no lifecycle parameter. As with the old
// kernel pool, this owner lives until process exit and retains at most seven
// background workers. Local SyncPool owners in tests exercise explicit drain.
var pool_storage: SyncPool = undefined;
var pool_initialized: std.atomic.Value(bool) = .init(false);
var pool_init_mutex: std.Io.Mutex = .init;

pub inline fn cachedCpuCount() usize {
    return @min(processCpuCount(), scoped_worker_limit orelse std.math.maxInt(usize));
}

/// Process capacity is independent of whichever request initializes a shared
/// pool or native library first.
pub inline fn processCpuCount() usize {
    const Once = struct {
        var value: std.atomic.Value(usize) = .{ .raw = 0 };
    };
    const cached = Once.value.load(.acquire);
    if (cached != 0) return cached;
    const detected = @import("cpu_budget.zig").effective(std.Thread.getCpuCount() catch 1);
    Once.value.store(@max(detected, 1), .release);
    return @max(detected, 1);
}

/// Return the bounded background capacity for the Sync compatibility path.
/// Individual async launches may still fall back inline on resource pressure.
pub fn ensurePool(worker_count: usize) usize {
    if (comptime !supports_sync_parallelism) return 0;
    if (worker_count == 0 or scoped_worker_limit == 1) return 0;
    if (!pool_initialized.load(.acquire)) {
        const io = std.Io.Threaded.global_single_threaded.io();
        pool_init_mutex.lockUncancelable(io);
        defer pool_init_mutex.unlock(io);
        if (!pool_initialized.load(.monotonic)) {
            pool_storage = SyncPool.init(processCpuCount() -| 1);
            pool_initialized.store(true, .release);
        }
    }
    return @min(worker_count, pool_storage.capacity, (scoped_worker_limit orelse std.math.maxInt(usize)) -| 1);
}

/// Async wrapper that std.Io.Group.async can dispatch.  The `Job.fn_ptr`
/// signature is `fn(*anyopaque) void`, but io.async needs a function it
/// can pass arguments to and that matches a Cancelable error union.
fn jobRunner(fn_ptr: *const fn (*anyopaque) void, ctx: *anyopaque, scoped: bool) std.Io.Cancelable!void {
    var scope = if (scoped) WorkerScope.init(1) catch unreachable else null;
    defer if (scope) |*active| active.deinit();
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
    if (scoped_worker_limit) |limit| {
        var begin: usize = 0;
        while (begin < jobs.len) {
            const end = begin + @min(limit, jobs.len - begin);
            try dispatchWave(io, jobs[begin..end], true);
            begin = end;
        }
        return;
    }
    try dispatchWave(io, jobs, false);
}

fn dispatchWave(io: std.Io, jobs: []const Job, scoped: bool) std.Io.Cancelable!void {
    if (jobs.len == 0) return;
    if (jobs.len == 1) {
        try jobRunner(jobs[0].fn_ptr, jobs[0].ctx, scoped);
        return;
    }
    var g: std.Io.Group = .init;
    errdefer g.cancel(io);
    for (jobs[0 .. jobs.len - 1]) |j| {
        g.async(io, jobRunner, .{ j.fn_ptr, j.ctx, scoped });
    }
    // Run the last job inline on the calling thread so a single-CPU
    // runtime still makes forward progress.
    try jobRunner(jobs[jobs.len - 1].fn_ptr, jobs[jobs.len - 1].ctx, scoped);
    try g.await(io);
}

/// Run the final job on the caller and drain bounded async jobs before return.
/// Preserve the existing sequential fallback outside Linux or over capacity.
pub fn dispatchJobs(jobs: []const Job) void {
    if (comptime @import("builtin").single_threaded) {
        for (jobs) |job| job.fn_ptr(job.ctx);
        return;
    }
    if (jobs.len <= 1 or ensurePool(jobs.len - 1) < jobs.len - 1) {
        var scope = if (scoped_worker_limit != null) WorkerScope.init(1) catch unreachable else null;
        defer if (scope) |*active| active.deinit();
        for (jobs) |job| job.fn_ptr(job.ctx);
        return;
    }
    pool_storage.dispatch(jobs);
}

const TestCtx = struct {
    counter: *std.atomic.Value(u32),
    contribution: u32,

    fn run(raw: *anyopaque) void {
        const self: *TestCtx = @ptrCast(@alignCast(raw));
        _ = self.counter.fetchAdd(self.contribution, .acq_rel);
    }
};

test "admitted worker scope rejects zero demand and restores nested limits" {
    try std.testing.expect(scopedWorkerLimit() == null);
    try std.testing.expectError(error.InvalidRunnableDemand, WorkerScope.init(0));
    try std.testing.expect(scopedWorkerLimit() == null);
    var outer = try WorkerScope.init(2);
    defer outer.deinit();
    try std.testing.expectEqual(@as(?usize, 2), scopedWorkerLimit());
    try std.testing.expect(cachedCpuCount() <= 2);
    const process_capacity = processCpuCount();
    var wider = try WorkerScope.init(8);
    try std.testing.expectEqual(@as(?usize, 2), scopedWorkerLimit());
    wider.deinit();
    var narrower = try WorkerScope.init(1);
    try std.testing.expectEqual(@as(?usize, 1), scopedWorkerLimit());
    try std.testing.expectEqual(@as(usize, 1), cachedCpuCount());
    try std.testing.expectEqual(process_capacity, processCpuCount());
    try std.testing.expectEqual(@as(usize, 0), ensurePool(7));
    narrower.deinit();
    try std.testing.expectEqual(@as(?usize, 2), scopedWorkerLimit());
}

test "admitted worker scope bounds actual dispatch and nested kernel fanout" {
    const State = struct {
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        completed: std.atomic.Value(usize) = .init(0),
        invalid_scope: std.atomic.Value(bool) = .init(false),

        fn leaf(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (scopedWorkerLimit() != 1) self.invalid_scope.store(true, .release);
            const count = self.active.fetchAdd(1, .acq_rel) + 1;
            _ = self.peak.fetchMax(count, .acq_rel);
            for (0..1000) |_| std.atomic.spinLoopHint();
            _ = self.completed.fetchAdd(1, .acq_rel);
            _ = self.active.fetchSub(1, .acq_rel);
        }
        fn nested(raw: *anyopaque) void {
            // A child cannot expand its one-worker allowance even if a nested
            // kernel requests a wider team or explicitly submits many jobs.
            var scope = WorkerScope.init(8) catch unreachable;
            defer scope.deinit();
            const jobs: [3]Job = @splat(.{ .fn_ptr = leaf, .ctx = raw });
            dispatchJobs(&jobs);
            dispatchJobsIo(std.testing.io, &jobs) catch unreachable;
        }
    };
    for ([_]usize{ 1, 2 }) |limit| {
        var state: State = .{};
        const jobs: [7]Job = @splat(.{ .fn_ptr = State.nested, .ctx = &state });
        var scope = try WorkerScope.init(limit);
        try dispatchJobsIo(std.testing.io, &jobs);
        dispatchJobs(&jobs);
        scope.deinit();
        try std.testing.expect(scopedWorkerLimit() == null);
        try std.testing.expect(!state.invalid_scope.load(.acquire));
        try std.testing.expectEqual(@as(usize, 84), state.completed.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), state.active.load(.acquire));
        try std.testing.expect(state.peak.load(.acquire) >= 1);
        try std.testing.expect(state.peak.load(.acquire) <= limit);
    }
}

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

test "Sync pool drains concurrent callers with bounded and unavailable capacity" {
    const Caller = struct {
        owner: *SyncPool,
        total: std.atomic.Value(u32) = .init(0),
        fn run(self: *@This()) void {
            var contexts: [3]TestCtx = undefined;
            var jobs: [3]Job = undefined;
            for (&contexts, &jobs) |*ctx, *job| {
                ctx.* = .{ .counter = &self.total, .contribution = 1 };
                job.* = .{ .fn_ptr = TestCtx.run, .ctx = ctx };
            }
            for (0..32) |_| self.owner.dispatch(&jobs);
        }
    };
    for ([_]usize{ 0, 2 }) |capacity| {
        var owner = SyncPool.init(capacity);
        defer owner.deinit();
        var callers: [4]Caller = undefined;
        var futures: [4]std.Io.Future(void) = undefined;
        var started: usize = 0;
        defer for (futures[0..started]) |*future| future.await(std.testing.io);
        for (&callers, &futures) |*caller, *future| {
            caller.* = .{ .owner = &owner };
            future.* = try std.testing.io.concurrent(Caller.run, .{caller});
            started += 1;
        }
        for (futures[0..started]) |*future| future.await(std.testing.io);
        started = 0;
        for (&callers) |*caller| try std.testing.expectEqual(@as(u32, 96), caller.total.load(.acquire));
    }
}

test "Sync pool completes inline while its only worker is occupied" {
    if (comptime !supports_sync_parallelism) return error.SkipZigTest;
    var owner = SyncPool.init(1);
    defer owner.deinit();
    const io = owner.io_impl.io();
    const Waiter = struct {
        fn run(active_io: std.Io, release: *std.Io.Event) void {
            release.waitUncancelable(active_io);
        }
    };
    var release: std.Io.Event = .unset;
    var occupied = try io.concurrent(Waiter.run, .{ io, &release });
    defer {
        release.set(io);
        occupied.await(io);
    }
    var total: std.atomic.Value(u32) = .init(0);
    var contexts = [_]TestCtx{
        .{ .counter = &total, .contribution = 2 },
        .{ .counter = &total, .contribution = 3 },
    };
    const jobs = [_]Job{
        .{ .fn_ptr = TestCtx.run, .ctx = &contexts[0] },
        .{ .fn_ptr = TestCtx.run, .ctx = &contexts[1] },
    };
    owner.dispatch(&jobs);
    try std.testing.expectEqual(@as(u32, 5), total.load(.acquire));
}
