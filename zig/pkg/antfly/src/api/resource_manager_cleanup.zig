// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One joined, isolated consumer for manager-owned physical/accounting debt.
//! Producers only publish atomics; no stopped foreground scope is retained.
const std = @import("std");
const sources = @import("antfly_local_sources");
const native = sources.common_request_admission.runnable_scheduler;
const resources = sources.storage_resource_manager;
const admission = sources.common_read_admission;
const clock = @import("antfly_platform").time;

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    manager: *resources.ResourceManager,
    scheduler: *native.Scheduler,
    future: ?std.Io.Future(void) = null,
    stopping: std.atomic.Value(bool) = .init(false),
    joined: bool = false,
    rounds: std.atomic.Value(u64) = .init(0),
    failed_rounds: std.atomic.Value(u64) = .init(0),
    retired_payloads: std.atomic.Value(u64) = .init(0),

    pub const poll_interval_ms = 10;
    pub const round_duration_ns = 25 * std.time.ns_per_ms;
    pub const Snapshot = struct { rounds: u64, failed_rounds: u64, retired_payloads: u64, ready_epoch: u64, pending: bool, stopping: bool };

    pub fn create(allocator: std.mem.Allocator, io: std.Io, manager: *resources.ResourceManager, scheduler: *native.Scheduler) !*Runtime {
        if (@min(@as(usize, 4), scheduler.config.max_operations / 2) < 2 or
            retainedBytes() > scheduler.config.control_bytes / 2) return error.ManagerCleanupCapacityUnavailable;
        try manager.bindCleanupConsumer();
        errdefer manager.unbindCleanupConsumer();
        try scheduler.ensureCleanupCpuBeforePublication();
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .io = io, .manager = manager, .scheduler = scheduler };
        try self.adoptRegistry();
        // Actual independent worker admission precedes publication.
        self.future = try io.concurrent(run, .{self});
        return self;
    }
    fn retainedBytes() u64 {
        return @sizeOf(Runtime) + @sizeOf(native.Execution) + @sizeOf(Budget) + @sizeOf(admission.Payload);
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Runtime = @ptrCast(@alignCast(raw));
        return self.stopping.load(.acquire);
    }
    const Budget = struct {
        execution: *native.Execution,
        io: std.Io,
        pub fn checkpoint(self: *@This()) !void {
            try self.execution.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.checkpoint();
            try self.execution.park(.resource_blocked);
            // The complete cleanup root remains owned while its CPU is loaned.
            try self.io.sleep(.fromMilliseconds(1), .awake);
            try self.execution.enter(.control);
            try self.checkpoint();
        }
    };
    fn adoptRegistry(self: *Runtime) !void {
        var execution: native.Execution = .{};
        defer execution.finish();
        try execution.startCleanupRoot(self.scheduler, .{
            .io = self.io,
            .native_now_ns = clock.monotonicNs,
            .deadline_ns = clock.monotonicNs() +| round_duration_ns,
            .retained_bytes = retainedBytes(),
            .allow_wait = false,
            .cancellation = .{ .ptr = self, .requested = canceled },
        });
        var budget = Budget{ .execution = &execution, .io = self.io };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        try self.manager.admitReclaimerMetadataWithAdmission(admission.Port.init(&payload));
    }
    fn round(self: *Runtime) !void {
        var execution: native.Execution = .{};
        defer execution.finish();
        try execution.startCleanupRoot(self.scheduler, .{
            .io = self.io,
            .native_now_ns = clock.monotonicNs,
            .deadline_ns = clock.monotonicNs() +| round_duration_ns,
            .retained_bytes = retainedBytes(),
            .allow_wait = false,
            .cancellation = .{ .ptr = self, .requested = canceled },
        });
        _ = self.rounds.fetchAdd(1, .monotonic);
        var budget = Budget{ .execution = &execution, .io = self.io };
        var adapter = admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const control = admission.Port.init(&payload);
        const retired = try self.manager.drainPayloadRetirementWithAdmission(control);
        _ = self.retired_payloads.fetchAdd(retired, .monotonic);
        try self.manager.drainConstructionCleanupWithAdmission(control);
    }
    fn run(self: *Runtime) void {
        while (!self.stopping.load(.acquire)) {
            if (self.manager.cleanupPending()) self.round() catch {
                _ = self.failed_rounds.fetchAdd(1, .monotonic);
            };
            // A finite poll avoids producer locks, borrowed callbacks and lost
            // wakeups. Idle consumers hold no runnable operation or CPU grant.
            self.io.sleep(.fromMilliseconds(poll_interval_ms), .awake) catch return;
        }
    }
    pub fn snapshot(self: *const Runtime) Snapshot {
        return .{ .rounds = self.rounds.load(.acquire), .failed_rounds = self.failed_rounds.load(.acquire), .retired_payloads = self.retired_payloads.load(.acquire), .ready_epoch = self.manager.cleanup_epoch.load(.acquire), .pending = self.manager.cleanupPending(), .stopping = self.stopping.load(.acquire) };
    }
    pub fn close(self: *Runtime) void {
        self.stopping.store(true, .release);
    }
    pub fn join(self: *Runtime) void {
        if (self.joined) return;
        self.close();
        self.future.?.cancel(self.io);
        self.future = null;
        // Release the manager binding only after every destructor has joined.
        self.manager.unbindCleanupConsumer();
        self.joined = true;
    }
    pub fn destroyJoined(self: *Runtime) void {
        std.debug.assert(self.joined);
        self.allocator.destroy(self);
    }
    pub fn deinit(self: *Runtime) void {
        self.join();
        self.destroyJoined();
    }
};
