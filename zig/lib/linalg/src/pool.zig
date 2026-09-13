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

//! Bounded Io scheduling for synchronous kernel entry points. The legacy
//! Sync API keeps a process-lifetime owner on Linux; other platforms retain
//! their sequential fallback. Io-aware callers use their own scheduling owner.

const std = @import("std");
const builtin = @import("builtin");

pub const max_workers: usize = 8;
pub const supports_sync_parallelism = !builtin.single_threaded and builtin.os.tag == .linux;
pub const Job = struct {
    fn_ptr: *const fn (*anyopaque) void,
    ctx: *anyopaque,
};

const SyncPool = struct {
    io_impl: std.Io.Threaded,
    submit_mutex: std.Io.Mutex = .init,
    capacity: usize,

    fn init(capacity: usize) SyncPool {
        const bounded = @min(capacity, max_workers - 1);
        return .{
            .capacity = bounded,
            .io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{
                .async_limit = .limited(bounded),
                .concurrent_limit = .limited(bounded),
            }),
        };
    }

    fn deinit(self: *SyncPool) void {
        self.io_impl.deinit();
    }

    fn dispatch(self: *SyncPool, jobs: []const Job) void {
        if (jobs.len <= 1 or jobs.len - 1 > self.capacity) {
            for (jobs) |job| job.fn_ptr(job.ctx);
            return;
        }
        const io = self.io_impl.io();
        // Sync kernels promise completed output and have no cancellation
        // result. Keep all jobs and caller-owned buffers alive through drain.
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        self.submit_mutex.lockUncancelable(io);
        defer self.submit_mutex.unlock(io);
        dispatchJobsIo(io, jobs) catch unreachable;
    }
};

// The Sync compatibility API has no lifecycle parameter. As with the old
// kernel pool, this owner lives until process exit and retains at most seven
// background workers. Local SyncPool owners in tests exercise explicit drain.
var pool_storage: SyncPool = undefined;
var pool_initialized: std.atomic.Value(bool) = .init(false);
var pool_init_mutex: std.Io.Mutex = .init;

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

/// Return the bounded background capacity for the Sync compatibility path.
/// Individual async launches may still fall back inline on resource pressure.
pub fn ensurePool(worker_count: usize) usize {
    if (!supports_sync_parallelism or worker_count == 0) return 0;
    if (!pool_initialized.load(.acquire)) {
        const io = std.Io.Threaded.global_single_threaded.io();
        pool_init_mutex.lockUncancelable(io);
        defer pool_init_mutex.unlock(io);
        if (!pool_initialized.load(.monotonic)) {
            pool_storage = SyncPool.init(cachedCpuCount() -| 1);
            pool_initialized.store(true, .release);
        }
    }
    return @min(worker_count, pool_storage.capacity);
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

/// Run the final job on the caller and drain bounded async jobs before return.
/// Preserve the existing sequential fallback outside Linux or over capacity.
pub fn dispatchJobs(jobs: []const Job) void {
    if (jobs.len <= 1 or ensurePool(jobs.len - 1) < jobs.len - 1) {
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
