// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//! Fixed-thread executor for CPU work that requires physical worker affinity.
//!
//! Unlike `std.Io`, a logical task submitted here remains on one owned worker
//! for its complete invocation and receives that worker's private scratch
//! allocator. Scratch is reset after every job and retention is capped, while
//! a lane-wide backing allocator enforces a hard aggregate byte ceiling.

const std = @import("std");

const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const Options = struct {
    worker_count: usize,
    queue_capacity: usize,
    max_scratch_bytes: usize,
    retained_scratch_bytes_per_worker: usize,
};

pub const BatchStats = struct {
    peak_parallelism: usize,
};

pub const Stats = struct {
    worker_count: usize,
    queue_capacity: usize,
    submitted_jobs: u64,
    completed_jobs: u64,
    peak_queued_jobs: usize,
    peak_active_jobs: usize,
    scratch_resets: u64,
    scratch_limit_rejections: u64,
    job_scratch_limit_rejections: u64,
    retained_scratch_bytes: usize,
    peak_scratch_bytes: usize,
};

pub const JobFn = *const fn (context: *anyopaque, scratch: Allocator) void;

const Batch = struct {
    remaining: std.atomic.Value(usize),
    active: std.atomic.Value(usize) = .init(0),
    peak_active: std.atomic.Value(usize) = .init(0),
    mutex: Io.Mutex = .init,
    complete: Io.Condition = .init,
    scratch_budget: ScratchBudget,

    fn init(job_count: usize, max_scratch_bytes: usize) Batch {
        return .{
            .remaining = .init(job_count),
            .scratch_budget = .{ .max_bytes = max_scratch_bytes },
        };
    }

    fn begin(self: *Batch) void {
        const active = self.active.fetchAdd(1, .acq_rel) + 1;
        updateAtomicMax(&self.peak_active, active);
    }

    fn finish(self: *Batch, sync_io: Io) void {
        _ = self.active.fetchSub(1, .acq_rel);
        const previous = self.remaining.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous == 1) {
            self.mutex.lockUncancelable(sync_io);
            self.complete.broadcast(sync_io);
            self.mutex.unlock(sync_io);
        }
    }

    fn wait(self: *Batch, sync_io: Io) void {
        self.mutex.lockUncancelable(sync_io);
        defer self.mutex.unlock(sync_io);
        while (self.remaining.load(.acquire) != 0)
            self.complete.waitUncancelable(sync_io, &self.mutex);
    }

    fn cancelUnsubmitted(self: *Batch, count: usize, sync_io: Io) void {
        if (count == 0) return;
        const previous = self.remaining.fetchSub(count, .acq_rel);
        std.debug.assert(previous >= count);
        if (previous == count) {
            self.mutex.lockUncancelable(sync_io);
            self.complete.broadcast(sync_io);
            self.mutex.unlock(sync_io);
        }
    }
};

const QueuedJob = struct {
    context: *anyopaque,
    run: JobFn,
    batch: *Batch,
};

const ScratchBudget = struct {
    max_bytes: usize,
    live_bytes: std.atomic.Value(usize) = .init(0),
    peak_bytes: std.atomic.Value(usize) = .init(0),
    rejections: std.atomic.Value(u64) = .init(0),

    fn reserve(self: *ScratchBudget, bytes: usize) bool {
        var observed = self.live_bytes.load(.acquire);
        while (true) {
            const next = std.math.add(usize, observed, bytes) catch {
                _ = self.rejections.fetchAdd(1, .monotonic);
                return false;
            };
            if (next > self.max_bytes) {
                _ = self.rejections.fetchAdd(1, .monotonic);
                return false;
            }
            if (self.live_bytes.cmpxchgWeak(observed, next, .acq_rel, .acquire)) |actual| {
                observed = actual;
                continue;
            }
            updateAtomicMax(&self.peak_bytes, next);
            return true;
        }
    }

    fn release(self: *ScratchBudget, bytes: usize) void {
        const previous = self.live_bytes.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
    }
};

const BoundedBackingAllocator = struct {
    budget: *ScratchBudget,
    job_budget: ?*ScratchBudget = null,
    job_charged_bytes: usize = 0,

    fn allocator(self: *BoundedBackingAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn alloc(context: *anyopaque, len: usize, alignment: std.mem.Alignment, return_address: usize) ?[*]u8 {
        const self: *BoundedBackingAllocator = @ptrCast(@alignCast(context));
        if (!self.budget.reserve(len)) return null;
        if (self.job_budget) |job_budget| if (!job_budget.reserve(len)) {
            self.budget.release(len);
            return null;
        };
        const result = std.heap.page_allocator.rawAlloc(len, alignment, return_address) orelse {
            self.budget.release(len);
            if (self.job_budget) |job_budget| job_budget.release(len);
            return null;
        };
        self.job_charged_bytes = std.math.add(usize, self.job_charged_bytes, len) catch unreachable;
        return result;
    }

    fn resize(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, return_address: usize) bool {
        const self: *BoundedBackingAllocator = @ptrCast(@alignCast(context));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.budget.reserve(growth)) return false;
        if (growth > 0) if (self.job_budget) |job_budget| if (!job_budget.reserve(growth)) {
            self.budget.release(growth);
            return false;
        };
        if (!std.heap.page_allocator.rawResize(memory, alignment, new_len, return_address)) {
            if (growth > 0) self.budget.release(growth);
            if (growth > 0) if (self.job_budget) |job_budget| job_budget.release(growth);
            return false;
        }
        self.job_charged_bytes = std.math.add(usize, self.job_charged_bytes, growth) catch unreachable;
        if (memory.len > new_len) self.budget.release(memory.len - new_len);
        return true;
    }

    fn remap(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, return_address: usize) ?[*]u8 {
        const self: *BoundedBackingAllocator = @ptrCast(@alignCast(context));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.budget.reserve(growth)) return null;
        if (growth > 0) if (self.job_budget) |job_budget| if (!job_budget.reserve(growth)) {
            self.budget.release(growth);
            return null;
        };
        const result = std.heap.page_allocator.rawRemap(memory, alignment, new_len, return_address) orelse {
            if (growth > 0) self.budget.release(growth);
            if (growth > 0) if (self.job_budget) |job_budget| job_budget.release(growth);
            return null;
        };
        self.job_charged_bytes = std.math.add(usize, self.job_charged_bytes, growth) catch unreachable;
        if (memory.len > new_len) self.budget.release(memory.len - new_len);
        return result;
    }

    fn free(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, return_address: usize) void {
        const self: *BoundedBackingAllocator = @ptrCast(@alignCast(context));
        std.heap.page_allocator.rawFree(memory, alignment, return_address);
        self.budget.release(memory.len);
    }

    fn beginJob(self: *BoundedBackingAllocator, budget: *ScratchBudget) void {
        std.debug.assert(self.job_budget == null);
        std.debug.assert(self.job_charged_bytes == 0);
        self.job_budget = budget;
    }

    fn endJob(self: *BoundedBackingAllocator) void {
        const budget = self.job_budget.?;
        budget.release(self.job_charged_bytes);
        self.job_budget = null;
        self.job_charged_bytes = 0;
    }
};

const Worker = struct {
    executor: *Executor,
    backing: BoundedBackingAllocator,
    arena: std.heap.ArenaAllocator,
    thread: ?std.Thread = null,

    fn run(self: *Worker) void {
        while (self.executor.take()) |job| {
            const active = self.executor.active_jobs.fetchAdd(1, .acq_rel) + 1;
            updateAtomicMax(&self.executor.peak_active_jobs, active);
            job.batch.begin();
            self.backing.beginJob(&job.batch.scratch_budget);
            job.run(job.context, self.arena.allocator());
            _ = self.arena.reset(.{
                .retain_with_limit = self.executor.retained_scratch_bytes_per_worker,
            });
            self.backing.endJob();
            _ = self.executor.scratch_resets.fetchAdd(1, .monotonic);
            _ = self.executor.active_jobs.fetchSub(1, .acq_rel);
            _ = self.executor.completed_jobs.fetchAdd(1, .monotonic);
            // Publish lane completion before waking the synchronous caller so
            // a stats snapshot taken immediately after runBatch is coherent.
            job.batch.finish(self.executor.sync_io);
        }
    }
};

pub const Executor = struct {
    alloc: Allocator,
    sync_io: Io,
    workers: []Worker,
    queue: []QueuedJob,
    queue_head: usize = 0,
    queue_len: usize = 0,
    closing: bool = false,
    mutex: Io.Mutex = .init,
    work_available: Io.Condition = .init,
    space_available: Io.Condition = .init,
    scratch_budget: ScratchBudget,
    retained_scratch_bytes_per_worker: usize,
    submitted_jobs: std.atomic.Value(u64) = .init(0),
    completed_jobs: std.atomic.Value(u64) = .init(0),
    peak_queued_jobs: std.atomic.Value(usize) = .init(0),
    active_jobs: std.atomic.Value(usize) = .init(0),
    peak_active_jobs: std.atomic.Value(usize) = .init(0),
    scratch_resets: std.atomic.Value(u64) = .init(0),
    job_scratch_limit_rejections: std.atomic.Value(u64) = .init(0),

    pub fn create(alloc: Allocator, options: Options) !*Executor {
        if (options.worker_count == 0 or options.queue_capacity == 0 or
            options.max_scratch_bytes == 0 or
            options.retained_scratch_bytes_per_worker > options.max_scratch_bytes)
            return error.InvalidBoundedWorkerLaneOptions;

        const self = try alloc.create(Executor);
        errdefer alloc.destroy(self);
        const workers = try alloc.alloc(Worker, options.worker_count);
        errdefer alloc.free(workers);
        const queue = try alloc.alloc(QueuedJob, options.queue_capacity);
        errdefer alloc.free(queue);
        self.* = .{
            .alloc = alloc,
            .sync_io = Io.Threaded.global_single_threaded.io(),
            .workers = workers,
            .queue = queue,
            .scratch_budget = .{ .max_bytes = options.max_scratch_bytes },
            .retained_scratch_bytes_per_worker = options.retained_scratch_bytes_per_worker,
        };

        var initialized_workers: usize = 0;
        errdefer {
            self.closeQueue();
            for (workers[0..initialized_workers]) |*worker| if (worker.thread) |thread| thread.join();
            for (workers[0..initialized_workers]) |*worker| worker.arena.deinit();
        }
        for (workers) |*worker| {
            worker.executor = self;
            worker.backing = .{ .budget = &self.scratch_budget };
            worker.arena = std.heap.ArenaAllocator.init(worker.backing.allocator());
            worker.thread = null;
            initialized_workers += 1;
        }
        for (workers) |*worker| worker.thread = try std.Thread.spawn(.{}, Worker.run, .{worker});
        return self;
    }

    /// The owner must close external admission and await all callers before
    /// destroying the executor. Every queued job is drained before workers
    /// exit, so completed callbacks never reference a destroyed lane.
    pub fn destroy(self: *Executor) void {
        self.closeQueue();
        for (self.workers) |worker| if (worker.thread) |thread| thread.join();
        for (self.workers) |*worker| worker.arena.deinit();
        std.debug.assert(self.scratch_budget.live_bytes.load(.acquire) == 0);
        const alloc = self.alloc;
        alloc.free(self.queue);
        alloc.free(self.workers);
        alloc.destroy(self);
    }

    pub fn concurrentCapacity(self: *const Executor) usize {
        return self.workers.len;
    }

    pub fn runBatch(
        self: *Executor,
        contexts: []const *anyopaque,
        run: JobFn,
        max_scratch_bytes: usize,
    ) !BatchStats {
        if (contexts.len == 0) return .{ .peak_parallelism = 0 };
        if (max_scratch_bytes == 0) return error.InvalidBoundedWorkerLaneScratchLimit;
        // This limit covers new arena backing committed by this batch. Memory
        // already retained by a worker is bounded separately as explicit lane
        // overhead; higher layers still account every logical render
        // allocation against their admitted per-window budget.
        var batch = Batch.init(contexts.len, max_scratch_bytes);
        var enqueued: usize = 0;
        var enqueue_failure: ?anyerror = null;
        for (contexts) |context| {
            self.enqueue(.{
                .context = context,
                .run = run,
                .batch = &batch,
            }) catch |err| {
                enqueue_failure = err;
                break;
            };
            enqueued += 1;
        }
        // Queued jobs borrow this stack Batch. If close wakes a producer after
        // partial enqueue, account for jobs that will never run and await every
        // published job before returning the enqueue error.
        batch.cancelUnsubmitted(contexts.len - enqueued, self.sync_io);
        batch.wait(self.sync_io);
        _ = self.job_scratch_limit_rejections.fetchAdd(
            batch.scratch_budget.rejections.load(.acquire),
            .monotonic,
        );
        if (enqueue_failure) |err| return err;
        return .{ .peak_parallelism = batch.peak_active.load(.acquire) };
    }

    /// Stop accepting work and wake blocked submitters. Already queued jobs
    /// drain normally. The owner must still await callers before `destroy`.
    pub fn close(self: *Executor) void {
        self.closeQueue();
    }

    pub fn snapshotStats(self: *const Executor) Stats {
        return .{
            .worker_count = self.workers.len,
            .queue_capacity = self.queue.len,
            .submitted_jobs = self.submitted_jobs.load(.acquire),
            .completed_jobs = self.completed_jobs.load(.acquire),
            .peak_queued_jobs = self.peak_queued_jobs.load(.acquire),
            .peak_active_jobs = self.peak_active_jobs.load(.acquire),
            .scratch_resets = self.scratch_resets.load(.acquire),
            .scratch_limit_rejections = self.scratch_budget.rejections.load(.acquire),
            .job_scratch_limit_rejections = self.job_scratch_limit_rejections.load(.acquire),
            .retained_scratch_bytes = self.scratch_budget.live_bytes.load(.acquire),
            .peak_scratch_bytes = self.scratch_budget.peak_bytes.load(.acquire),
        };
    }

    fn enqueue(self: *Executor, job: QueuedJob) !void {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (!self.closing and self.queue_len == self.queue.len)
            self.space_available.waitUncancelable(self.sync_io, &self.mutex);
        if (self.closing) return error.BoundedWorkerLaneClosed;
        const tail = (self.queue_head + self.queue_len) % self.queue.len;
        self.queue[tail] = job;
        self.queue_len += 1;
        updateAtomicMax(&self.peak_queued_jobs, self.queue_len);
        _ = self.submitted_jobs.fetchAdd(1, .monotonic);
        self.work_available.signal(self.sync_io);
    }

    fn take(self: *Executor) ?QueuedJob {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (!self.closing and self.queue_len == 0)
            self.work_available.waitUncancelable(self.sync_io, &self.mutex);
        if (self.queue_len == 0) return null;
        const job = self.queue[self.queue_head];
        self.queue_head = (self.queue_head + 1) % self.queue.len;
        self.queue_len -= 1;
        self.space_available.signal(self.sync_io);
        return job;
    }

    fn closeQueue(self: *Executor) void {
        self.mutex.lockUncancelable(self.sync_io);
        self.closing = true;
        self.work_available.broadcast(self.sync_io);
        self.space_available.broadcast(self.sync_io);
        self.mutex.unlock(self.sync_io);
    }
};

fn updateAtomicMax(counter: *std.atomic.Value(usize), value: usize) void {
    var observed = counter.load(.acquire);
    while (observed < value) {
        if (counter.cmpxchgWeak(observed, value, .acq_rel, .acquire) == null) return;
        observed = counter.load(.acquire);
    }
}

test "bounded worker lane reuses one thread-confined scratch arena" {
    if (comptime @import("builtin").single_threaded) return;
    var executor = try Executor.create(std.testing.allocator, .{
        .worker_count = 1,
        .queue_capacity = 1,
        .max_scratch_bytes = 256 * 1024,
        .retained_scratch_bytes_per_worker = 64 * 1024,
    });
    defer executor.destroy();

    const Capture = struct {
        thread_id: std.Thread.Id = undefined,
        allocation: [*]u8 = undefined,

        fn run(context: *anyopaque, scratch: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.thread_id = std.Thread.getCurrentId();
            const memory = scratch.alloc(u8, 4096) catch unreachable;
            self.allocation = memory.ptr;
        }
    };
    var first = Capture{};
    var second = Capture{};
    _ = try executor.runBatch(&.{@ptrCast(&first)}, Capture.run, 64 * 1024);
    _ = try executor.runBatch(&.{@ptrCast(&second)}, Capture.run, 64 * 1024);
    try std.testing.expectEqual(first.thread_id, second.thread_id);
    try std.testing.expectEqual(first.allocation, second.allocation);
    const stats = executor.snapshotStats();
    try std.testing.expectEqual(@as(u64, 2), stats.scratch_resets);
    try std.testing.expect(stats.retained_scratch_bytes <= 64 * 1024);
}

test "bounded worker lane caps aggregate scratch and handles concurrent submitters" {
    if (comptime @import("builtin").single_threaded) return;
    var executor = try Executor.create(std.testing.allocator, .{
        .worker_count = 2,
        .queue_capacity = 2,
        .max_scratch_bytes = 128 * 1024,
        .retained_scratch_bytes_per_worker = 16 * 1024,
    });
    defer executor.destroy();

    const Capture = struct {
        completed: std.atomic.Value(usize) = .init(0),
        rejected: std.atomic.Value(usize) = .init(0),

        fn run(context: *anyopaque, scratch: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (scratch.alloc(u8, 256 * 1024)) |_| {
                unreachable;
            } else |_| {
                _ = self.rejected.fetchAdd(1, .monotonic);
            }
            _ = self.completed.fetchAdd(1, .monotonic);
        }
    };
    var capture = Capture{};
    const caller_count = 4;
    const jobs_per_caller = 4;
    var contexts = [_]*anyopaque{@ptrCast(&capture)} ** jobs_per_caller;
    var failures = [_]?anyerror{null} ** caller_count;
    var threads: [caller_count]std.Thread = undefined;
    for (&threads, &failures) |*thread, *failure| thread.* = try std.Thread.spawn(.{}, struct {
        fn run(target: *Executor, items: []const *anyopaque, result: *?anyerror) void {
            _ = target.runBatch(items, Capture.run, 64 * 1024) catch |err| {
                result.* = err;
                return;
            };
        }
    }.run, .{ executor, &contexts, failure });
    for (threads) |thread| thread.join();
    for (failures) |failure| try std.testing.expect(failure == null);
    try std.testing.expectEqual(@as(usize, caller_count * jobs_per_caller), capture.completed.load(.acquire));
    try std.testing.expectEqual(capture.completed.load(.acquire), capture.rejected.load(.acquire));
    const stats = executor.snapshotStats();
    try std.testing.expectEqual(@as(u64, caller_count * jobs_per_caller), stats.submitted_jobs);
    try std.testing.expectEqual(stats.submitted_jobs, stats.completed_jobs);
    try std.testing.expect(stats.peak_queued_jobs <= stats.queue_capacity);
    try std.testing.expect(stats.peak_active_jobs <= stats.worker_count);
    try std.testing.expect(stats.peak_scratch_bytes <= 128 * 1024);
    try std.testing.expect(stats.retained_scratch_bytes <= 32 * 1024);
    try std.testing.expect(stats.scratch_limit_rejections > 0);
}

test "bounded worker lane enforces each batch backing grant" {
    if (comptime @import("builtin").single_threaded) return;
    var executor = try Executor.create(std.testing.allocator, .{
        .worker_count = 1,
        .queue_capacity = 1,
        .max_scratch_bytes = 256 * 1024,
        .retained_scratch_bytes_per_worker = 0,
    });
    defer executor.destroy();

    const Capture = struct {
        rejected: bool = false,

        fn run(context: *anyopaque, scratch: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            _ = scratch.alloc(u8, 96 * 1024) catch {
                self.rejected = true;
                return;
            };
        }
    };
    var capture = Capture{};
    _ = try executor.runBatch(&.{@ptrCast(&capture)}, Capture.run, 64 * 1024);
    try std.testing.expect(capture.rejected);
    const stats = executor.snapshotStats();
    try std.testing.expectEqual(@as(u64, 0), stats.scratch_limit_rejections);
    try std.testing.expect(stats.job_scratch_limit_rejections > 0);
    try std.testing.expectEqual(@as(usize, 0), stats.retained_scratch_bytes);
}

test "bounded worker lane drains partial enqueue before reporting close" {
    if (comptime @import("builtin").single_threaded) return;
    var executor = try Executor.create(std.testing.allocator, .{
        .worker_count = 1,
        .queue_capacity = 1,
        .max_scratch_bytes = 64 * 1024,
        .retained_scratch_bytes_per_worker = 4096,
    });
    defer executor.destroy();

    const Capture = struct {
        started: std.atomic.Value(usize) = .init(0),
        completed: std.atomic.Value(usize) = .init(0),
        release: std.atomic.Value(bool) = .init(false),

        fn run(context: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            _ = self.started.fetchAdd(1, .monotonic);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
            _ = self.completed.fetchAdd(1, .monotonic);
        }
    };
    var capture = Capture{};
    var contexts = [_]*anyopaque{@ptrCast(&capture)} ** 32;
    var failure: ?anyerror = null;
    const submitter = try std.Thread.spawn(.{}, struct {
        fn run(target: *Executor, items: []const *anyopaque, result: *?anyerror) void {
            _ = target.runBatch(items, Capture.run, 64 * 1024) catch |err| {
                result.* = err;
                return;
            };
        }
    }.run, .{ executor, &contexts, &failure });
    while (capture.started.load(.acquire) == 0) std.Thread.yield() catch {};
    executor.close();
    capture.release.store(true, .release);
    submitter.join();

    try std.testing.expect(failure.? == error.BoundedWorkerLaneClosed);
    try std.testing.expect(capture.completed.load(.acquire) > 0);
    try std.testing.expect(capture.completed.load(.acquire) < contexts.len);
    const stats = executor.snapshotStats();
    try std.testing.expectEqual(stats.submitted_jobs, stats.completed_jobs);
}
