// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const platform_time = @import("../platform/time.zig");
const platform_sync = @import("antfly_platform").sync;

var coordinator_boot_sequence: std.atomic.Value(u64) = .init(1);

pub const Operation = enum { check, compact, vacuum };
pub const State = enum { queued, running, succeeded, failed, canceled };

pub const CancelToken = struct {
    requested: std.atomic.Value(bool) = .init(false),

    pub fn request(self: *CancelToken) void {
        self.requested.store(true, .release);
    }

    pub fn check(self: *const CancelToken) !void {
        if (self.requested.load(.acquire)) return error.MaintenanceCanceled;
    }
};

pub const Capabilities = struct {
    check: bool = false,
    compact: bool = false,
    vacuum: bool = false,
    online: bool = false,
    asynchronous: bool = true,
};

pub const Status = struct {
    engine: []const u8,
    format: ?[]const u8 = null,
    fsync: ?bool = null,
    maintenance: Capabilities,
};

pub const Result = struct {
    valid: ?bool = null,
    issue: ?[]const u8 = null,
    file_size: ?u64 = null,
    valid_prefix_size: ?u64 = null,
    reclaimable_bytes: ?u64 = null,
    before_size: ?u64 = null,
    after_size: ?u64 = null,
    reclaimed_bytes: ?u64 = null,
    live_file_count: ?u64 = null,
    live_bytes: ?u64 = null,
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        status: *const fn (*anyopaque) Status,
        run: *const fn (*anyopaque, Operation, *const CancelToken) anyerror!Result,
    };

    pub fn status(self: Source) Status {
        return self.vtable.status(self.ptr);
    }

    pub fn run(self: Source, operation: Operation, cancel: *const CancelToken) !Result {
        return try self.vtable.run(self.ptr, operation, cancel);
    }
};

var local_source_token: u8 = 0;

pub const localSource = Source{
    .ptr = &local_source_token,
    .vtable = &.{
        .status = struct {
            fn call(_: *anyopaque) Status {
                return .{ .engine = "local", .maintenance = .{} };
            }
        }.call,
        .run = struct {
            fn call(_: *anyopaque, _: Operation, _: *const CancelToken) anyerror!Result {
                return error.UnsupportedStorageMaintenance;
            }
        }.call,
    },
};

pub const Coordinator = struct {
    allocator: std.mem.Allocator,
    source: Source,
    mutex: std.atomic.Mutex = .unlocked,
    id_seed: u64,
    id_sequence: u64 = 1,
    exclusive_active: std.atomic.Value(bool) = .init(false),
    active_job_id: ?u64 = null,
    jobs: std.ArrayListUnmanaged(*Job) = .empty,

    const max_retained_jobs: usize = 4096;
    const idempotency_retention_ms: i64 = 24 * 60 * 60 * 1000;

    pub const Job = struct {
        id: u64,
        operation: Operation,
        state: State = .queued,
        idempotency_key: ?[]u8 = null,
        created_at_ms: i64,
        started_at_ms: ?i64 = null,
        completed_at_ms: ?i64 = null,
        result: ?Result = null,
        // @errorName strings have static lifetime, so response snapshots never
        // borrow heap memory from a prunable Job.
        error_name: ?[]const u8 = null,
        thread: ?std.Thread = null,
        cancel: CancelToken = .{},

        fn deinit(self: *Job, allocator: std.mem.Allocator) void {
            if (self.thread) |thread| thread.join();
            if (self.idempotency_key) |key| allocator.free(key);
            allocator.destroy(self);
        }
    };

    pub const Snapshot = struct {
        job_id: u64,
        operation: Operation,
        state: State,
        created_at_ms: i64,
        started_at_ms: ?i64,
        completed_at_ms: ?i64,
        result: ?Result,
        error_name: ?[]const u8,
    };

    pub fn init(allocator: std.mem.Allocator, source: Source) Coordinator {
        const seed = [2]u64{
            platform_time.realtimeNs(),
            coordinator_boot_sequence.fetchAdd(1, .monotonic),
        };
        var seed_random: u64 = undefined;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        io_impl.io().randomSecure(std.mem.asBytes(&seed_random)) catch {
            seed_random = std.hash.Wyhash.hash(0x616e74666c792d6d, std.mem.asBytes(&seed));
        };
        return initWithSeed(allocator, source, seed_random);
    }

    fn initWithSeed(allocator: std.mem.Allocator, source: Source, seed: u64) Coordinator {
        return .{
            .allocator = allocator,
            .source = source,
            .id_seed = if (seed == 0) 1 else seed,
        };
    }

    pub fn isExclusiveActive(self: *const Coordinator) bool {
        return self.exclusive_active.load(.acquire);
    }

    pub fn deinit(self: *Coordinator) void {
        // No new jobs can be submitted once the owning HTTP server is down.
        for (self.jobs.items) |job| job.cancel.request();
        for (self.jobs.items) |job| job.deinit(self.allocator);
        self.jobs.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn status(self: *const Coordinator) Status {
        return self.source.status();
    }

    pub fn start(self: *Coordinator, operation: Operation, idempotency_key: ?[]const u8) !Snapshot {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.reapCompletedThreadsLocked();

        if (idempotency_key) |key| {
            if (key.len == 0 or key.len > 256) return error.InvalidIdempotencyKey;
            for (self.jobs.items) |job| {
                const existing = job.idempotency_key orelse continue;
                if (!std.mem.eql(u8, existing, key)) continue;
                if (job.operation != operation) return error.IdempotencyConflict;
                return snapshotLocked(job);
            }
        }
        if (self.active_job_id != null) return error.MaintenanceBusy;
        self.pruneExpiredLocked(nowMs());
        if (self.jobs.items.len >= max_retained_jobs) return error.MaintenanceHistoryFull;

        const job = try self.allocator.create(Job);
        errdefer self.allocator.destroy(job);
        const job_id = self.nextJobIdLocked() orelse return error.MaintenanceJobIdExhausted;
        job.* = .{
            .id = job_id,
            .operation = operation,
            .idempotency_key = if (idempotency_key) |key| try self.allocator.dupe(u8, key) else null,
            .created_at_ms = nowMs(),
        };
        errdefer if (job.idempotency_key) |key| self.allocator.free(key);
        try self.jobs.append(self.allocator, job);
        errdefer _ = self.jobs.pop();
        // Publish coordinator state only after every allocation needed to own
        // the queued job has succeeded. In particular, an OOM growing `jobs`
        // must not leave the coordinator permanently busy.
        self.active_job_id = job.id;
        const exclusive = !self.source.status().maintenance.online;
        if (exclusive) self.exclusive_active.store(true, .release);
        job.thread = std.Thread.spawn(.{}, runJob, .{ self, job }) catch |err| {
            self.active_job_id = null;
            if (exclusive) self.exclusive_active.store(false, .release);
            return err;
        };
        return snapshotLocked(job);
    }

    pub fn get(self: *Coordinator, job_id: u64) ?Snapshot {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.reapCompletedThreadsLocked();
        for (self.jobs.items) |job| {
            if (job.id == job_id) return snapshotLocked(job);
        }
        return null;
    }

    pub fn cancel(self: *Coordinator, job_id: u64) ?Snapshot {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.reapCompletedThreadsLocked();
        for (self.jobs.items) |job| {
            if (job.id != job_id) continue;
            if (job.state == .queued or job.state == .running) job.cancel.request();
            return snapshotLocked(job);
        }
        return null;
    }

    fn runJob(self: *Coordinator, job: *Job) void {
        platform_sync.lockYielding(&self.mutex);
        job.state = .running;
        job.started_at_ms = nowMs();
        self.mutex.unlock();

        const outcome = self.source.run(job.operation, &job.cancel);

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (outcome) |result| {
            job.result = result;
            job.state = .succeeded;
        } else |err| {
            job.error_name = @errorName(err);
            job.state = if (err == error.MaintenanceCanceled) .canceled else .failed;
        }
        job.completed_at_ms = nowMs();
        if (self.active_job_id == job.id) self.active_job_id = null;
        self.exclusive_active.store(false, .release);
    }

    fn nextJobIdLocked(self: *Coordinator) ?u64 {
        var attempts: usize = 0;
        while (attempts < 16) : (attempts += 1) {
            const material = [2]u64{ self.id_seed, self.id_sequence };
            self.id_sequence +%= 1;
            var candidate = std.hash.Wyhash.hash(0xa66f_6c79_6d61_696e, std.mem.asBytes(&material)) & std.math.maxInt(i64);
            if (candidate == 0) candidate = 1;
            var collision = false;
            for (self.jobs.items) |job| {
                if (job.id == candidate) {
                    collision = true;
                    break;
                }
            }
            if (!collision) return candidate;
        }
        return null;
    }

    fn pruneExpiredLocked(self: *Coordinator, now_ms: i64) void {
        var i: usize = 0;
        while (i < self.jobs.items.len) {
            const job = self.jobs.items[i];
            const terminal = job.state == .succeeded or job.state == .failed or job.state == .canceled;
            const completed = job.completed_at_ms orelse {
                i += 1;
                continue;
            };
            if (!terminal or now_ms -| completed < idempotency_retention_ms) {
                i += 1;
                continue;
            }
            _ = self.jobs.orderedRemove(i);
            job.deinit(self.allocator);
        }
    }

    fn reapCompletedThreadsLocked(self: *Coordinator) void {
        for (self.jobs.items) |job| {
            if (job.state != .succeeded and job.state != .failed and job.state != .canceled) continue;
            if (job.thread) |thread| {
                thread.join();
                job.thread = null;
            }
        }
    }

    fn snapshotLocked(job: *const Job) Snapshot {
        return .{
            .job_id = job.id,
            .operation = job.operation,
            .state = job.state,
            .created_at_ms = job.created_at_ms,
            .started_at_ms = job.started_at_ms,
            .completed_at_ms = job.completed_at_ms,
            .result = job.result,
            .error_name = job.error_name,
        };
    }
};

fn nowMs() i64 {
    return @intCast(@divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms));
}

test "storage maintenance coordinator is idempotent and single flight" {
    const Fake = struct {
        runs: std.atomic.Value(u64) = .init(0),

        fn source(self: *@This()) Source {
            return .{ .ptr = self, .vtable = &.{ .status = status, .run = run } };
        }
        fn status(_: *anyopaque) Status {
            return .{ .engine = "fake", .maintenance = .{ .check = true, .online = true } };
        }
        fn run(ptr: *anyopaque, _: Operation, cancel: *const CancelToken) anyerror!Result {
            try cancel.check();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.runs.fetchAdd(1, .monotonic);
            return .{ .valid = true };
        }
    };
    var fake = Fake{};
    var coordinator = Coordinator.init(std.testing.allocator, fake.source());
    defer coordinator.deinit();
    const first = try coordinator.start(.check, "same-key");
    const replay = try coordinator.start(.check, "same-key");
    try std.testing.expectEqual(first.job_id, replay.job_id);
    while (true) {
        const snapshot = coordinator.get(first.job_id).?;
        if (snapshot.state == .succeeded) break;
        std.Thread.yield() catch {};
    }
    try std.testing.expectEqual(@as(u64, 1), fake.runs.load(.monotonic));
    try std.testing.expectError(error.IdempotencyConflict, coordinator.start(.vacuum, "same-key"));
}

test "storage maintenance job ids are namespaced by server boot" {
    var token: u8 = 0;
    const source = Source{
        .ptr = &token,
        .vtable = &.{
            .status = struct {
                fn call(_: *anyopaque) Status {
                    return .{ .engine = "fake", .maintenance = .{} };
                }
            }.call,
            .run = struct {
                fn call(_: *anyopaque, _: Operation, _: *const CancelToken) anyerror!Result {
                    return .{};
                }
            }.call,
        },
    };
    var first = Coordinator.initWithSeed(std.testing.allocator, source, 11);
    defer first.deinit();
    var second = Coordinator.initWithSeed(std.testing.allocator, source, 12);
    defer second.deinit();
    const first_id = first.nextJobIdLocked().?;
    const second_id = second.nextJobIdLocked().?;
    try std.testing.expect(first_id != second_id);
    try std.testing.expect(first_id <= std.math.maxInt(i64));
    try std.testing.expect(second_id <= std.math.maxInt(i64));
}

test "storage maintenance cancellation reaches a cooperative engine" {
    const Fake = struct {
        fn source(self: *@This()) Source {
            return .{ .ptr = self, .vtable = &.{ .status = status, .run = run } };
        }
        fn status(_: *anyopaque) Status {
            return .{ .engine = "fake", .maintenance = .{ .vacuum = true } };
        }
        fn run(_: *anyopaque, _: Operation, cancel: *const CancelToken) anyerror!Result {
            while (true) {
                try cancel.check();
                std.Thread.yield() catch {};
            }
        }
    };
    var fake = Fake{};
    var coordinator = Coordinator.init(std.testing.allocator, fake.source());
    defer coordinator.deinit();
    const started = try coordinator.start(.vacuum, "cancel-me");
    try std.testing.expect(coordinator.isExclusiveActive());
    _ = coordinator.cancel(started.job_id).?;
    while (true) {
        const snapshot = coordinator.get(started.job_id).?;
        if (snapshot.state == .canceled) {
            try std.testing.expectEqualStrings("MaintenanceCanceled", snapshot.error_name.?);
            try std.testing.expect(!coordinator.isExclusiveActive());
            break;
        }
        std.Thread.yield() catch {};
    }
}

test "storage maintenance snapshots remain valid after retention pruning" {
    const Fake = struct {
        fn source(self: *@This()) Source {
            return .{ .ptr = self, .vtable = &.{ .status = status, .run = run } };
        }
        fn status(_: *anyopaque) Status {
            return .{ .engine = "fake", .maintenance = .{ .check = true } };
        }
        fn run(_: *anyopaque, _: Operation, cancel: *const CancelToken) anyerror!Result {
            try cancel.check();
            return error.InjectedMaintenanceFailure;
        }
    };
    var fake = Fake{};
    var coordinator = Coordinator.init(std.testing.allocator, fake.source());
    defer coordinator.deinit();

    const first = try coordinator.start(.check, null);
    const retained = blk: {
        while (true) {
            const snapshot = coordinator.get(first.job_id).?;
            if (snapshot.state == .failed) break :blk snapshot;
            std.Thread.yield() catch {};
        }
    };
    try std.testing.expectEqualStrings("InjectedMaintenanceFailure", retained.error_name.?);

    platform_sync.lockYielding(&coordinator.mutex);
    coordinator.jobs.items[0].completed_at_ms = nowMs() - Coordinator.idempotency_retention_ms;
    coordinator.mutex.unlock();
    const next = try coordinator.start(.check, null);
    while (coordinator.get(next.job_id).?.state != .failed) std.Thread.yield() catch {};
    try std.testing.expect(coordinator.get(first.job_id) == null);
    try std.testing.expectEqualStrings("InjectedMaintenanceFailure", retained.error_name.?);
}

test "storage maintenance append allocation failure does not wedge coordinator" {
    const Fake = struct {
        fn source(self: *@This()) Source {
            return .{ .ptr = self, .vtable = &.{ .status = status, .run = run } };
        }
        fn status(_: *anyopaque) Status {
            return .{ .engine = "fake", .maintenance = .{ .check = true } };
        }
        fn run(_: *anyopaque, _: Operation, cancel: *const CancelToken) anyerror!Result {
            try cancel.check();
            return .{ .valid = true };
        }
    };

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var fake = Fake{};
    var coordinator = Coordinator.init(failing.allocator(), fake.source());
    defer coordinator.deinit();

    // Job allocation succeeds; growing the job list fails.
    failing.fail_index = failing.alloc_index + 1;
    try std.testing.expectError(error.OutOfMemory, coordinator.start(.check, null));
    try std.testing.expectEqual(@as(?u64, null), coordinator.active_job_id);

    failing.fail_index = std.math.maxInt(usize);
    const started = try coordinator.start(.check, null);
    while (coordinator.get(started.job_id).?.state != .succeeded) std.Thread.yield() catch {};
}
