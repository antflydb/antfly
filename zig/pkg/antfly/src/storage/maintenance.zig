// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const platform_time = @import("../platform/time.zig");
const platform_sync = @import("antfly_platform").sync;

pub const Operation = enum { check, compact, vacuum };
pub const State = enum { queued, running, succeeded, failed };

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
        run: *const fn (*anyopaque, Operation) anyerror!Result,
    };

    pub fn status(self: Source) Status {
        return self.vtable.status(self.ptr);
    }

    pub fn run(self: Source, operation: Operation) !Result {
        return try self.vtable.run(self.ptr, operation);
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
            fn call(_: *anyopaque, _: Operation) anyerror!Result {
                return error.UnsupportedStorageMaintenance;
            }
        }.call,
    },
};

pub const Coordinator = struct {
    allocator: std.mem.Allocator,
    source: Source,
    mutex: std.atomic.Mutex = .unlocked,
    next_job_id: u64 = 1,
    active_job_id: ?u64 = null,
    jobs: std.ArrayListUnmanaged(*Job) = .empty,

    const max_retained_jobs: usize = 128;

    pub const Job = struct {
        id: u64,
        operation: Operation,
        state: State = .queued,
        idempotency_key: ?[]u8 = null,
        created_at_ms: i64,
        started_at_ms: ?i64 = null,
        completed_at_ms: ?i64 = null,
        result: ?Result = null,
        error_name: ?[]u8 = null,
        thread: ?std.Thread = null,

        fn deinit(self: *Job, allocator: std.mem.Allocator) void {
            if (self.thread) |thread| thread.join();
            if (self.idempotency_key) |key| allocator.free(key);
            if (self.error_name) |name| allocator.free(name);
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
        return .{ .allocator = allocator, .source = source };
    }

    pub fn deinit(self: *Coordinator) void {
        // No new jobs can be submitted once the owning HTTP server is down.
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
        try self.pruneLocked();

        const job = try self.allocator.create(Job);
        errdefer self.allocator.destroy(job);
        job.* = .{
            .id = self.next_job_id,
            .operation = operation,
            .idempotency_key = if (idempotency_key) |key| try self.allocator.dupe(u8, key) else null,
            .created_at_ms = nowMs(),
        };
        errdefer if (job.idempotency_key) |key| self.allocator.free(key);
        self.next_job_id +|= 1;
        self.active_job_id = job.id;
        try self.jobs.append(self.allocator, job);
        errdefer _ = self.jobs.pop();
        job.thread = std.Thread.spawn(.{}, runJob, .{ self, job }) catch |err| {
            self.active_job_id = null;
            return err;
        };
        return snapshotLocked(job);
    }

    pub fn get(self: *Coordinator, job_id: u64) ?Snapshot {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.jobs.items) |job| {
            if (job.id == job_id) return snapshotLocked(job);
        }
        return null;
    }

    fn runJob(self: *Coordinator, job: *Job) void {
        platform_sync.lockYielding(&self.mutex);
        job.state = .running;
        job.started_at_ms = nowMs();
        self.mutex.unlock();

        const outcome = self.source.run(job.operation);

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (outcome) |result| {
            job.result = result;
            job.state = .succeeded;
        } else |err| {
            job.error_name = self.allocator.dupe(u8, @errorName(err)) catch null;
            job.state = .failed;
        }
        job.completed_at_ms = nowMs();
        if (self.active_job_id == job.id) self.active_job_id = null;
    }

    fn pruneLocked(self: *Coordinator) !void {
        while (self.jobs.items.len >= max_retained_jobs) {
            var remove_index: ?usize = null;
            for (self.jobs.items, 0..) |job, i| {
                if (job.state == .succeeded or job.state == .failed) {
                    remove_index = i;
                    break;
                }
            }
            const i = remove_index orelse return error.MaintenanceBusy;
            const job = self.jobs.orderedRemove(i);
            job.deinit(self.allocator);
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
        fn run(ptr: *anyopaque, _: Operation) anyerror!Result {
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
