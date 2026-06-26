// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const apply_rw_lock_mod = @import("../apply_rw_lock.zig");
const db_internal = @import("../internal.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const lease_mod = @import("../lease.zig");
const ownership_mod = @import("../ownership.zig");
const graph_mod = @import("../../../graph/graph.zig");
const graph_query_mod = @import("../../../graph/query.zig");
const platform_clock = @import("../../../platform/clock.zig");
const types = @import("../types.zig");
const background_runtime_mod = @import("../../background_runtime.zig");
const yieldToBackground = db_internal.yieldToBackground;

const TestHelpers = if (builtin.is_test) @import("../test_support.zig") else struct {};

pub const Role = enum {
    combined,
    coordinator,
    worker,
    worker_pool,
};

pub const Config = struct {
    enabled: bool = false,
    start_background_loop: bool = true,
    role: Role = .combined,
    runtime_id: []const u8 = "",
    lease_owned: bool = false,
    owner_id: []const u8 = "local",
    lease_ttl_ms: u64 = 30_000,
    coordinator_start_background_builds: bool = true,
    idle_interval_ms: u64 = 50,
    error_interval_ms: u64 = 250,
    planned_options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions = .{},
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub const Stats = struct {
    enabled: bool = false,
    role: Role = .combined,
    runtime_id_hash: u64 = 0,
    owner_id_hash: u64 = 0,
    lease_key_hash: u64 = 0,
    worker_id_hash: u64 = 0,
    worker_count: usize = 0,
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    takeover_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
    started: bool = false,
    shutdown: bool = false,
    notified: bool = false,
    ticks_started: u64 = 0,
    ticks_completed: u64 = 0,
    durable_progress_ticks: u64 = 0,
    idle_ticks: u64 = 0,
    error_ticks: u64 = 0,
    last_error_name: ?[]const u8 = null,
    total_result: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult = .{},
    last_result: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult = .{},
};

pub fn expectPlannedAutoIdleDecision(
    index_manager: *index_manager_mod.IndexManager,
    options: index_manager_mod.IndexManager.GraphMetricPlannedAutoIdleOptions,
    should_run_planned: bool,
    active_builds: usize,
    eligible_queued: usize,
    deferred_queued: usize,
    ineligible_queued: usize,
) !void {
    const decision = try index_manager.graphMetricPlannedAutoIdleDecision(options);
    try std.testing.expectEqual(should_run_planned, decision.shouldRunPlanned());
    try std.testing.expectEqual(active_builds, decision.active_builds);
    try std.testing.expectEqual(eligible_queued, decision.eligible_queued);
    try std.testing.expectEqual(deferred_queued, decision.deferred_queued);
    try std.testing.expectEqual(ineligible_queued, decision.ineligible_queued);
}

pub fn expectDegreeCanaryDecision(
    index_manager: *index_manager_mod.IndexManager,
    options: index_manager_mod.IndexManager.GraphMetricDegreeCanaryOptions,
    should_run_planned: bool,
    active_degree_builds: usize,
    eligible_queued_degree: usize,
    blocked_active_non_degree: usize,
    blocked_queued_non_degree: usize,
) !void {
    const decision = try index_manager.graphMetricDegreeCanaryDecision(options);
    try std.testing.expectEqual(should_run_planned, decision.shouldRunPlanned());
    try std.testing.expectEqual(active_degree_builds, decision.active_degree_builds);
    try std.testing.expectEqual(eligible_queued_degree, decision.eligible_queued_degree);
    try std.testing.expectEqual(blocked_active_non_degree, decision.blocked_active_non_degree);
    try std.testing.expectEqual(blocked_queued_non_degree, decision.blocked_queued_non_degree);
    try std.testing.expectEqual(@as(usize, 0), decision.failed_pages);
    try std.testing.expect(!decision.truncated_pages);
}

pub const MaintenanceBoundary = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const WorkerPoolSweepOptions = struct {
        worker_id: []const u8 = "",
        worker_ids: []const []const u8 = &.{},
        max_pages: usize = 64,
        now_ms: ?u64 = null,
    };

    pub const VTable = struct {
        run_combined: *const fn (
            *anyopaque,
            index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
        ) anyerror!index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
        run_coordinator: *const fn (
            *anyopaque,
            index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
        ) anyerror!index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
        run_worker: *const fn (
            *anyopaque,
            index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions,
        ) anyerror!index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
        run_worker_pool: *const fn (
            *anyopaque,
            WorkerPoolSweepOptions,
        ) anyerror!index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
    };

    pub fn direct(index_manager: *index_manager_mod.IndexManager) MaintenanceBoundary {
        return .{
            .ptr = index_manager,
            .vtable = &direct_vtable,
        };
    }

    pub fn init(ptr: *anyopaque, vtable: *const VTable) MaintenanceBoundary {
        return .{
            .ptr = ptr,
            .vtable = vtable,
        };
    }

    pub fn runCombined(
        self: MaintenanceBoundary,
        options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        return try self.vtable.run_combined(self.ptr, options);
    }

    pub fn runCoordinator(
        self: MaintenanceBoundary,
        options: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        return try self.vtable.run_coordinator(self.ptr, options);
    }

    pub fn runWorker(
        self: MaintenanceBoundary,
        options: index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        return try self.vtable.run_worker(self.ptr, options);
    }

    pub fn runWorkerPool(
        self: MaintenanceBoundary,
        options: WorkerPoolSweepOptions,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        return try self.vtable.run_worker_pool(self.ptr, options);
    }
};

const direct_vtable = MaintenanceBoundary.VTable{
    .run_combined = directRunCombined,
    .run_coordinator = directRunCoordinator,
    .run_worker = directRunWorker,
    .run_worker_pool = directRunWorkerPool,
};

fn directIndexManager(ptr: *anyopaque) *index_manager_mod.IndexManager {
    return @ptrCast(@alignCast(ptr));
}

fn directRunCombined(
    ptr: *anyopaque,
    options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    return try directIndexManager(ptr).runGraphMetricPlannedMaintenance(options);
}

fn directRunCoordinator(
    ptr: *anyopaque,
    options: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    return try directIndexManager(ptr).runGraphMetricPlannedCoordinatorSweep(options);
}

fn directRunWorker(
    ptr: *anyopaque,
    options: index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    return try directIndexManager(ptr).runGraphMetricPlannedWorkerSweep(options);
}

fn directRunWorkerPool(
    ptr: *anyopaque,
    options: MaintenanceBoundary.WorkerPoolSweepOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    if (options.worker_ids.len == 0) {
        return try directRunWorker(ptr, .{
            .worker_id = options.worker_id,
            .max_pages = options.max_pages,
            .now_ms = options.now_ms,
        });
    }

    var total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var pages_remaining = options.max_pages;
    while (pages_remaining > 0) {
        var worker_progressed = false;
        for (options.worker_ids) |worker_id| {
            if (pages_remaining == 0) break;
            const worker = try directRunWorker(ptr, .{
                .worker_id = worker_id,
                .max_pages = 1,
                .now_ms = options.now_ms,
            });
            total.add(worker);
            pages_remaining -= @min(pages_remaining, worker.worker_steps);
            worker_progressed = worker_progressed or worker.durableProgressed();
        }
        if (!worker_progressed) break;
    }
    return total;
}

pub const default_lease_key = default_combined_lease_key;
pub const default_combined_lease_key = "\x00\x00__metadata__:graph_metric_runtime_lease:combined";
pub const default_coordinator_lease_key = "\x00\x00__metadata__:graph_metric_runtime_lease:coordinator";
pub const default_worker_lease_key = "\x00\x00__metadata__:graph_metric_runtime_lease:worker";
pub const default_worker_pool_lease_key = "\x00\x00__metadata__:graph_metric_runtime_lease:worker_pool";

pub fn defaultLeaseKey(role: Role) []const u8 {
    return switch (role) {
        .combined => default_combined_lease_key,
        .coordinator => default_coordinator_lease_key,
        .worker => default_worker_lease_key,
        .worker_pool => default_worker_pool_lease_key,
    };
}

pub const GraphMetricRuntime = if (builtin.os.tag == .freestanding) struct {
    config: Config,

    pub fn init(
        _: Allocator,
        _: anytype,
        _: *index_manager_mod.IndexManager,
        _: *apply_rw_lock_mod.ApplyRwLock,
        _: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !@This() {
        return .{ .config = config };
    }

    pub fn deinit(self: *@This()) void {
        self.* = undefined;
    }

    pub fn deinitPreserveLease(self: *@This()) void {
        self.* = undefined;
    }

    pub fn start(self: *@This()) !void {
        if (self.config.enabled and self.config.start_background_loop) return error.UnsupportedPlatform;
    }

    pub fn notify(self: *@This()) void {
        _ = self;
    }

    pub fn stats(self: *@This()) Stats {
        return initialStats(self.config);
    }

    pub fn runOnce(self: *@This()) !bool {
        _ = self;
        return false;
    }

    pub fn runOnceDetailed(self: *@This()) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        _ = self;
        return .{};
    }

    pub fn runCoordinatorOnce(self: *@This(), start_background_builds: bool) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        _ = self;
        _ = start_background_builds;
        return .{};
    }

    pub fn runWorkerOnce(self: *@This(), worker_id: []const u8) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        _ = self;
        _ = worker_id;
        return .{};
    }

    pub fn runWorkerPoolOnce(self: *@This()) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        _ = self;
        return .{};
    }
} else struct {
    alloc: Allocator,
    io_impl: ?*Io.Threaded,
    maintenance_boundary: MaintenanceBoundary,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    config: Config,
    lease_key: []u8,
    ownership: ownership_mod.State,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    shutdown: bool = false,
    notified: bool = false,
    future: ?Io.Future(void) = null,
    stats_snapshot: Stats = .{},

    pub fn init(
        alloc: Allocator,
        store: anytype,
        index_manager: *index_manager_mod.IndexManager,
        apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !GraphMetricRuntime {
        const io_impl = backend_runtime.io_impl;
        if (config.enabled and io_impl == null) return error.MissingBackendRuntimeIo;
        try validateConfig(config);
        const lease_key = try runtimeLeaseKeyAlloc(alloc, config);
        errdefer alloc.free(lease_key);
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .maintenance_boundary = MaintenanceBoundary.direct(index_manager),
            .apply_mutex = apply_mutex,
            .config = config,
            .lease_key = lease_key,
            .ownership = try ownership_mod.State.init(alloc, store, lease_key, .{
                .lease_owned = config.lease_owned,
                .owner_id = if (config.owner_id.len != 0) config.owner_id else config.runtime_id,
                .lease_ttl_ms = config.lease_ttl_ms,
            }),
            .stats_snapshot = initialStatsWithLeaseKey(config, lease_key),
        };
    }

    fn stopRuntime(self: *GraphMetricRuntime) void {
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.shutdown = true;
            self.notified = true;
            self.cond.broadcast(io);
            self.mutex.unlock(io);

            if (self.future) |*future| _ = future.await(io);
        }
        self.future = null;
    }

    pub fn deinit(self: *GraphMetricRuntime) void {
        self.stopRuntime();
        self.ownership.deinit(self.alloc);
        self.alloc.free(self.lease_key);
        self.* = undefined;
    }

    pub fn deinitPreserveLease(self: *GraphMetricRuntime) void {
        self.stopRuntime();
        self.ownership.deinitPreserveLease(self.alloc);
        self.alloc.free(self.lease_key);
        self.* = undefined;
    }

    pub fn start(self: *GraphMetricRuntime) !void {
        if (!self.config.enabled) return;
        if (!self.config.start_background_loop) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        self.future = try io_impl.io().concurrent(workerMain, .{self});
        self.recordStarted();
    }

    pub fn notify(self: *GraphMetricRuntime) void {
        if (!self.config.enabled) return;
        const io_impl = self.io_impl orelse return;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.notified = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);
    }

    pub fn stats(self: *GraphMetricRuntime) Stats {
        const io_impl = self.io_impl orelse return self.stats_snapshot;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var snapshot = self.stats_snapshot;
        snapshot.started = self.future != null;
        snapshot.shutdown = self.shutdown;
        snapshot.notified = self.notified;
        applyOwnershipStats(&snapshot, self.ownership.stats());
        return snapshot;
    }

    pub fn runOnce(self: *GraphMetricRuntime) !bool {
        const result = try self.runOnceDetailed();
        return result.durableProgressed();
    }

    pub fn runOnceDetailed(self: *GraphMetricRuntime) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        if (!self.config.enabled) return .{};
        self.recordTickStarted();
        const now_ms = self.config.clock.nowRealtimeMs();
        if (!self.ensureRuntimeLease(now_ms)) {
            self.recordTickSuccess(.{});
            return .{};
        }
        if (!lockApplyExclusiveBackoff(self)) return .{};
        defer self.apply_mutex.unlockExclusive();

        const result = runBoundaryTick(self.maintenance_boundary, self.config, now_ms) catch |err| {
            self.recordTickError(err);
            return err;
        };
        self.recordTickSuccess(result);
        return result;
    }

    pub fn runCoordinatorOnce(
        self: *GraphMetricRuntime,
        start_background_builds: bool,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        if (!self.config.enabled) return .{};
        self.recordTickStarted();
        if (!coordinatorCallAllowed(self.config)) {
            const err = error.InvalidGraphMetricRuntimeRole;
            self.recordTickError(err);
            return err;
        }
        const now_ms = self.config.clock.nowRealtimeMs();
        if (!self.ensureRuntimeLease(now_ms)) {
            self.recordTickSuccess(.{});
            return .{};
        }
        if (!lockApplyExclusiveBackoff(self)) return .{};
        defer self.apply_mutex.unlockExclusive();

        const result = self.maintenance_boundary.runCoordinator(.{
            .max_metrics = self.config.planned_options.max_metrics_per_round,
            .start_background_builds = start_background_builds,
            .now_ms = now_ms,
        }) catch |err| {
            self.recordTickError(err);
            return err;
        };
        self.recordTickSuccess(result);
        return result;
    }

    pub fn runWorkerOnce(
        self: *GraphMetricRuntime,
        worker_id: []const u8,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        if (!self.config.enabled) return .{};
        self.recordTickStarted();
        if (!workerCallAllowed(self.config, worker_id)) {
            const err = error.InvalidGraphMetricBuildWorker;
            self.recordTickError(err);
            return err;
        }
        const now_ms = self.config.clock.nowRealtimeMs();
        if (!self.ensureRuntimeLease(now_ms)) {
            self.recordTickSuccess(.{});
            return .{};
        }
        if (!lockApplyExclusiveBackoff(self)) return .{};
        defer self.apply_mutex.unlockExclusive();

        const result = self.maintenance_boundary.runWorker(.{
            .worker_id = worker_id,
            .max_pages = self.config.planned_options.max_pages_per_round,
            .now_ms = now_ms,
        }) catch |err| {
            self.recordTickError(err);
            return err;
        };
        self.recordTickSuccess(result);
        return result;
    }

    pub fn runWorkerPoolOnce(self: *GraphMetricRuntime) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        if (!workerPoolCallAllowed(self.config)) {
            if (!self.config.enabled) return .{};
            self.recordTickStarted();
            const err = error.InvalidGraphMetricRuntimeRole;
            self.recordTickError(err);
            return err;
        }
        if (self.config.planned_options.worker_ids.len == 0) {
            return try self.runWorkerOnce(self.config.planned_options.worker_id);
        }
        if (!self.config.enabled) return .{};
        self.recordTickStarted();
        const now_ms = self.config.clock.nowRealtimeMs();
        if (!self.ensureRuntimeLease(now_ms)) {
            self.recordTickSuccess(.{});
            return .{};
        }
        if (!lockApplyExclusiveBackoff(self)) return .{};
        defer self.apply_mutex.unlockExclusive();

        const total = self.runWorkerPoolSweepLockedAt(now_ms) catch |err| {
            self.recordTickError(err);
            return err;
        };
        self.recordTickSuccess(total);
        return total;
    }

    fn runWorkerPoolSweepLocked(self: *GraphMetricRuntime) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        return try self.runWorkerPoolSweepLockedAt(self.config.clock.nowRealtimeMs());
    }

    fn runWorkerPoolSweepLockedAt(
        self: *GraphMetricRuntime,
        now_ms: u64,
    ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
        return try self.maintenance_boundary.runWorkerPool(.{
            .worker_id = self.config.planned_options.worker_id,
            .worker_ids = self.config.planned_options.worker_ids,
            .max_pages = self.config.planned_options.max_pages_per_round,
            .now_ms = now_ms,
        });
    }

    fn ensureRuntimeLease(self: *GraphMetricRuntime, now_ms: u64) bool {
        const io_impl = self.io_impl orelse return false;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.ownership.ensureLease(now_ms) catch {
            self.ownership.noteAcquireFailure();
            return false;
        };
    }

    fn recordStarted(self: *GraphMetricRuntime) void {
        const io_impl = self.io_impl orelse {
            self.stats_snapshot.started = true;
            return;
        };
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.stats_snapshot.started = true;
        self.mutex.unlock(io);
    }

    fn recordTickStarted(self: *GraphMetricRuntime) void {
        const io_impl = self.io_impl orelse {
            self.stats_snapshot.ticks_started += 1;
            return;
        };
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.stats_snapshot.ticks_started += 1;
        self.mutex.unlock(io);
    }

    fn recordTickSuccess(
        self: *GraphMetricRuntime,
        result: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
    ) void {
        const io_impl = self.io_impl orelse {
            updateSuccessStats(&self.stats_snapshot, result);
            return;
        };
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        updateSuccessStats(&self.stats_snapshot, result);
        self.mutex.unlock(io);
    }

    fn recordTickError(self: *GraphMetricRuntime, err: anyerror) void {
        const io_impl = self.io_impl orelse {
            updateErrorStats(&self.stats_snapshot, err);
            return;
        };
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        updateErrorStats(&self.stats_snapshot, err);
        self.mutex.unlock(io);
    }
};

fn updateSuccessStats(
    stats_snapshot: *Stats,
    result: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
) void {
    stats_snapshot.ticks_completed += 1;
    if (result.durableProgressed()) {
        stats_snapshot.durable_progress_ticks += 1;
    } else {
        stats_snapshot.idle_ticks += 1;
    }
    stats_snapshot.last_error_name = null;
    stats_snapshot.total_result.add(result);
    stats_snapshot.last_result = result;
}

fn updateErrorStats(stats_snapshot: *Stats, err: anyerror) void {
    stats_snapshot.error_ticks += 1;
    stats_snapshot.last_error_name = @errorName(err);
}

pub fn initialStats(config: Config) Stats {
    return .{
        .enabled = config.enabled,
        .role = config.role,
        .runtime_id_hash = identityHash(config.runtime_id),
        .owner_id_hash = identityHash(runtimeOwnerId(config)),
        .worker_id_hash = workerIdentityHash(config),
        .worker_count = configuredWorkerCount(config),
        .lease_owned = config.lease_owned,
        .has_lease = !config.lease_owned,
    };
}

pub fn initialStatsWithLeaseKey(config: Config, lease_key: []const u8) Stats {
    var stats = initialStats(config);
    stats.lease_key_hash = identityHash(lease_key);
    return stats;
}

fn applyOwnershipStats(stats_snapshot: *Stats, ownership_stats: ownership_mod.Stats) void {
    stats_snapshot.lease_owned = ownership_stats.lease_owned;
    stats_snapshot.has_lease = ownership_stats.has_lease;
    stats_snapshot.acquisition_count = ownership_stats.acquisition_count;
    stats_snapshot.takeover_count = ownership_stats.takeover_count;
    stats_snapshot.lease_acquire_failures = ownership_stats.lease_acquire_failures;
    stats_snapshot.lost_leases = ownership_stats.lost_leases;
    stats_snapshot.last_acquired_ms = ownership_stats.last_acquired_ms;
}

pub fn identityHash(value: []const u8) u64 {
    if (value.len == 0) return 0;
    return std.hash.Wyhash.hash(0, value);
}

pub fn runtimeOwnerId(config: Config) []const u8 {
    return if (config.owner_id.len != 0) config.owner_id else config.runtime_id;
}

pub fn workerSetIdentityHash(worker_ids: []const []const u8) u64 {
    if (worker_ids.len == 0) return 0;
    var xor_hash: u64 = 0;
    var sum_hash: u64 = 0;
    for (worker_ids) |worker_id| {
        const item_hash = identityHash(worker_id);
        xor_hash ^= item_hash;
        sum_hash +%= item_hash;
    }
    const fingerprint_words = [_]u64{
        @intCast(worker_ids.len),
        xor_hash,
        sum_hash,
    };
    return std.hash.Wyhash.hash(0, std.mem.asBytes(&fingerprint_words));
}

pub fn workerIdentityHash(config: Config) u64 {
    if (config.role == .coordinator) return 0;
    if (config.planned_options.worker_ids.len == 0) return identityHash(config.planned_options.worker_id);
    return workerSetIdentityHash(config.planned_options.worker_ids);
}

pub fn runtimeLeaseKeyAlloc(alloc: Allocator, config: Config) ![]u8 {
    const base_key = defaultLeaseKey(config.role);
    switch (config.role) {
        .combined, .coordinator => return try alloc.dupe(u8, base_key),
        .worker, .worker_pool => {
            if (configuredWorkerCount(config) == 0) return try alloc.dupe(u8, base_key);
            return try std.fmt.allocPrint(alloc, "{s}:{x}", .{
                base_key,
                workerIdentityHash(config),
            });
        },
    }
}

pub fn runBoundaryTick(
    boundary: MaintenanceBoundary,
    config: Config,
    now_ms: u64,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    var planned_options = config.planned_options;
    planned_options.now_ms = now_ms;
    return switch (config.role) {
        .combined => boundary.runCombined(planned_options),
        .coordinator => boundary.runCoordinator(.{
            .max_metrics = config.planned_options.max_metrics_per_round,
            .start_background_builds = config.coordinator_start_background_builds,
            .now_ms = now_ms,
        }),
        .worker => boundary.runWorker(.{
            .worker_id = config.planned_options.worker_id,
            .max_pages = config.planned_options.max_pages_per_round,
            .now_ms = now_ms,
        }),
        .worker_pool => boundary.runWorkerPool(.{
            .worker_id = config.planned_options.worker_id,
            .worker_ids = config.planned_options.worker_ids,
            .max_pages = config.planned_options.max_pages_per_round,
            .now_ms = now_ms,
        }),
    };
}

fn validateConfig(config: Config) !void {
    if (!config.enabled) return;
    if (config.lease_ttl_ms == 0) return error.InvalidGraphMetricRuntimeConfig;
    if (config.planned_options.max_rounds == 0) return error.InvalidGraphMetricRuntimeConfig;
    if (config.planned_options.max_metrics_per_round == 0) return error.InvalidGraphMetricRuntimeConfig;
    if (config.planned_options.max_pages_per_round == 0) return error.InvalidGraphMetricRuntimeConfig;
    try validateWorkerIdentities(config);
}

fn validateWorkerIdentities(config: Config) !void {
    if (config.role == .coordinator) {
        if (config.planned_options.worker_ids.len != 0) return error.InvalidGraphMetricBuildWorker;
        return;
    }
    if (config.role == .worker and config.planned_options.worker_ids.len != 0) {
        return error.InvalidGraphMetricBuildWorker;
    }
    if (config.planned_options.worker_ids.len == 0) {
        if ((config.role == .worker or config.role == .worker_pool) and
            config.planned_options.worker_id.len == 0)
        {
            return error.InvalidGraphMetricBuildWorker;
        }
        return;
    }

    for (config.planned_options.worker_ids, 0..) |worker_id, i| {
        if (worker_id.len == 0) return error.InvalidGraphMetricBuildWorker;
        for (config.planned_options.worker_ids[0..i]) |prior_worker_id| {
            if (std.mem.eql(u8, worker_id, prior_worker_id)) return error.InvalidGraphMetricBuildWorker;
        }
    }
}

fn workerCallAllowed(config: Config, worker_id: []const u8) bool {
    if (worker_id.len == 0) return false;
    return switch (config.role) {
        .combined => true,
        .coordinator => false,
        .worker => std.mem.eql(u8, worker_id, config.planned_options.worker_id),
        .worker_pool => {
            if (config.planned_options.worker_ids.len == 0) {
                return std.mem.eql(u8, worker_id, config.planned_options.worker_id);
            }
            for (config.planned_options.worker_ids) |configured_worker_id| {
                if (std.mem.eql(u8, worker_id, configured_worker_id)) return true;
            }
            return false;
        },
    };
}

fn coordinatorCallAllowed(config: Config) bool {
    return config.role == .combined or config.role == .coordinator;
}

fn workerPoolCallAllowed(config: Config) bool {
    return config.role == .combined or config.role == .worker or config.role == .worker_pool;
}

fn configuredWorkerCount(config: Config) usize {
    if (config.role == .coordinator) return 0;
    if (config.planned_options.worker_ids.len != 0) return config.planned_options.worker_ids.len;
    return if (config.planned_options.worker_id.len == 0) 0 else 1;
}

test "graph metric runtime config rejects zero lease and maintenance budgets when enabled" {
    try std.testing.expectError(error.InvalidGraphMetricRuntimeConfig, validateConfig(.{
        .enabled = true,
        .lease_ttl_ms = 0,
    }));
    try std.testing.expectError(error.InvalidGraphMetricRuntimeConfig, validateConfig(.{
        .enabled = true,
        .planned_options = .{ .max_rounds = 0 },
    }));
    try std.testing.expectError(error.InvalidGraphMetricRuntimeConfig, validateConfig(.{
        .enabled = true,
        .planned_options = .{ .max_metrics_per_round = 0 },
    }));
    try std.testing.expectError(error.InvalidGraphMetricRuntimeConfig, validateConfig(.{
        .enabled = true,
        .planned_options = .{ .max_pages_per_round = 0 },
    }));
    try validateConfig(.{
        .enabled = false,
        .lease_ttl_ms = 0,
        .planned_options = .{
            .max_rounds = 0,
            .max_metrics_per_round = 0,
            .max_pages_per_round = 0,
        },
    });
}

test "graph metric runtime config rejects worker id lists for single-owner roles" {
    const workers = [_][]const u8{ "worker-a", "worker-b" };

    try validateConfig(.{
        .enabled = true,
        .role = .coordinator,
        .planned_options = .{ .worker_id = "coordinator-unused" },
    });
    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, validateConfig(.{
        .enabled = true,
        .role = .coordinator,
        .planned_options = .{ .worker_ids = workers[0..] },
    }));
    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, validateConfig(.{
        .enabled = true,
        .role = .worker,
        .planned_options = .{
            .worker_id = "worker-a",
            .worker_ids = workers[0..],
        },
    }));
}

test "graph metric runtime role gates apply without durable lease ownership" {
    const combined = Config{
        .enabled = true,
        .role = .combined,
        .lease_owned = false,
        .planned_options = .{ .worker_id = "combined-worker" },
    };
    try std.testing.expect(coordinatorCallAllowed(combined));
    try std.testing.expect(workerCallAllowed(combined, "any-worker"));
    try std.testing.expect(workerPoolCallAllowed(combined));

    const coordinator = Config{
        .enabled = true,
        .role = .coordinator,
        .lease_owned = false,
        .planned_options = .{ .worker_id = "coordinator-unused" },
    };
    try std.testing.expect(coordinatorCallAllowed(coordinator));
    try std.testing.expect(!workerCallAllowed(coordinator, "coordinator-unused"));
    try std.testing.expect(!workerPoolCallAllowed(coordinator));

    const worker = Config{
        .enabled = true,
        .role = .worker,
        .lease_owned = false,
        .planned_options = .{ .worker_id = "worker-a" },
    };
    try std.testing.expect(!coordinatorCallAllowed(worker));
    try std.testing.expect(workerCallAllowed(worker, "worker-a"));
    try std.testing.expect(!workerCallAllowed(worker, "worker-b"));
    try std.testing.expect(workerPoolCallAllowed(worker));

    const pool_workers = [_][]const u8{ "pool-a", "pool-b" };
    const worker_pool = Config{
        .enabled = true,
        .role = .worker_pool,
        .lease_owned = false,
        .planned_options = .{ .worker_ids = pool_workers[0..] },
    };
    try std.testing.expect(!coordinatorCallAllowed(worker_pool));
    try std.testing.expect(workerCallAllowed(worker_pool, "pool-a"));
    try std.testing.expect(workerCallAllowed(worker_pool, "pool-b"));
    try std.testing.expect(!workerCallAllowed(worker_pool, "pool-c"));
    try std.testing.expect(workerPoolCallAllowed(worker_pool));
}

test "graph metric runtime worker pool identity is order independent" {
    const alloc = std.testing.allocator;
    const workers_ab = [_][]const u8{ "worker-a", "worker-b" };
    const workers_ba = [_][]const u8{ "worker-b", "worker-a" };
    const workers_ac = [_][]const u8{ "worker-a", "worker-c" };

    const config_ab = Config{
        .enabled = true,
        .role = .worker_pool,
        .lease_owned = true,
        .planned_options = .{ .worker_ids = workers_ab[0..] },
    };
    const config_ba = Config{
        .enabled = true,
        .role = .worker_pool,
        .lease_owned = true,
        .planned_options = .{ .worker_ids = workers_ba[0..] },
    };
    const config_ac = Config{
        .enabled = true,
        .role = .worker_pool,
        .lease_owned = true,
        .planned_options = .{ .worker_ids = workers_ac[0..] },
    };

    try std.testing.expectEqual(@as(usize, 2), configuredWorkerCount(config_ab));
    try std.testing.expectEqual(workerIdentityHash(config_ab), workerIdentityHash(config_ba));
    try std.testing.expect(workerIdentityHash(config_ab) != workerIdentityHash(config_ac));

    const lease_ab = try runtimeLeaseKeyAlloc(alloc, config_ab);
    defer alloc.free(lease_ab);
    const lease_ba = try runtimeLeaseKeyAlloc(alloc, config_ba);
    defer alloc.free(lease_ba);
    const lease_ac = try runtimeLeaseKeyAlloc(alloc, config_ac);
    defer alloc.free(lease_ac);

    try std.testing.expectEqualStrings(lease_ab, lease_ba);
    try std.testing.expect(!std.mem.eql(u8, lease_ab, lease_ac));
}

const FakeMaintenanceBoundary = struct {
    combined_calls: usize = 0,
    coordinator_calls: usize = 0,
    worker_calls: usize = 0,
    worker_pool_calls: usize = 0,
    last_worker_id: []const u8 = "",
    last_worker_count: usize = 0,
    last_max_pages: usize = 0,
    last_now_ms: ?u64 = null,

    fn boundary(self: *FakeMaintenanceBoundary) MaintenanceBoundary {
        return MaintenanceBoundary.init(self, &fake_boundary_vtable);
    }
};

const fake_boundary_vtable = MaintenanceBoundary.VTable{
    .run_combined = fakeBoundaryRunCombined,
    .run_coordinator = fakeBoundaryRunCoordinator,
    .run_worker = fakeBoundaryRunWorker,
    .run_worker_pool = fakeBoundaryRunWorkerPool,
};

fn fakeBoundaryContext(ptr: *anyopaque) *FakeMaintenanceBoundary {
    return @ptrCast(@alignCast(ptr));
}

fn fakeBoundaryRunCombined(
    ptr: *anyopaque,
    options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    const fake = fakeBoundaryContext(ptr);
    fake.combined_calls += 1;
    fake.last_worker_id = options.worker_id;
    fake.last_worker_count = options.worker_ids.len;
    fake.last_max_pages = options.max_pages_per_round;
    fake.last_now_ms = options.now_ms;
    return .{ .metrics_scanned = 1 };
}

fn fakeBoundaryRunCoordinator(
    ptr: *anyopaque,
    options: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    const fake = fakeBoundaryContext(ptr);
    fake.coordinator_calls += 1;
    fake.last_max_pages = options.max_metrics;
    fake.last_now_ms = options.now_ms;
    return .{ .coordinator_steps = 1 };
}

fn fakeBoundaryRunWorker(
    ptr: *anyopaque,
    options: index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    const fake = fakeBoundaryContext(ptr);
    fake.worker_calls += 1;
    fake.last_worker_id = options.worker_id;
    fake.last_max_pages = options.max_pages;
    fake.last_now_ms = options.now_ms;
    return .{ .worker_steps = 1 };
}

fn fakeBoundaryRunWorkerPool(
    ptr: *anyopaque,
    options: MaintenanceBoundary.WorkerPoolSweepOptions,
) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    const fake = fakeBoundaryContext(ptr);
    fake.worker_pool_calls += 1;
    fake.last_worker_id = options.worker_id;
    fake.last_worker_count = options.worker_ids.len;
    fake.last_max_pages = options.max_pages;
    fake.last_now_ms = options.now_ms;
    return .{ .worker_steps = options.worker_ids.len, .pages_completed = options.worker_ids.len };
}

test "graph metric runtime boundary tick preserves worker pool operation" {
    const workers = [_][]const u8{ "worker-a", "worker-b" };
    var fake = FakeMaintenanceBoundary{};
    const result = try runBoundaryTick(fake.boundary(), .{
        .enabled = true,
        .role = .worker_pool,
        .planned_options = .{
            .worker_id = "unused-worker",
            .worker_ids = workers[0..],
            .max_pages_per_round = 5,
        },
    }, 1234);

    try std.testing.expectEqual(@as(usize, 0), fake.worker_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.worker_pool_calls);
    try std.testing.expectEqualStrings("unused-worker", fake.last_worker_id);
    try std.testing.expectEqual(@as(usize, 2), fake.last_worker_count);
    try std.testing.expectEqual(@as(usize, 5), fake.last_max_pages);
    try std.testing.expectEqual(@as(?u64, 1234), fake.last_now_ms);
    try std.testing.expectEqual(@as(usize, 2), result.worker_steps);
    try std.testing.expectEqual(@as(usize, 2), result.pages_completed);
}

fn workerMain(runtime: *GraphMetricRuntime) void {
    while (true) {
        if (isShutdown(runtime)) return;
        const ran = runtime.runOnce() catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.warn("graph metric maintenance worker failed: {s}", .{@errorName(err)});
            }
            sleepMs(runtime, runtime.config.error_interval_ms);
            continue;
        };
        if (ran) continue;
        waitForWork(runtime);
    }
}

fn waitForWork(runtime: *GraphMetricRuntime) void {
    var remaining_ms = runtime.config.idle_interval_ms;
    if (remaining_ms == 0) remaining_ms = 1;

    const io_impl = runtime.io_impl orelse return;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    if (runtime.notified or runtime.shutdown) {
        runtime.notified = false;
        runtime.mutex.unlock(io);
        return;
    }
    runtime.mutex.unlock(io);

    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 10);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
        runtime.mutex.lockUncancelable(io);
        const notified = runtime.notified;
        runtime.notified = false;
        runtime.mutex.unlock(io);
        if (notified) return;
    }
}

fn sleepMs(runtime: *GraphMetricRuntime, ms: u64) void {
    var remaining_ms = if (ms == 0) 1 else ms;
    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 10);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
    }
}

fn isShutdown(runtime: *GraphMetricRuntime) bool {
    const io_impl = runtime.io_impl orelse return runtime.shutdown;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    return runtime.shutdown;
}

fn lockApplyExclusiveBackoff(runtime: *GraphMetricRuntime) bool {
    while (!runtime.apply_mutex.tryLockExclusive()) {
        if (isShutdown(runtime)) return false;
        sleepMs(runtime, 1);
    }
    return true;
}

test "db graph metric runtime lease ownership blocks duplicate owners and allows takeover" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(1_000 * std.time.ns_per_ms);
    const resources = db.core.asyncResources();
    var owner_a = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-owned-a",
            .lease_owned = true,
            .owner_id = "runtime-owner-a",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-owned-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer owner_a.deinit();
    var owner_b = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-owned-b",
            .lease_owned = true,
            .owner_id = "runtime-owner-b",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-owned-worker-b",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer owner_b.deinit();

    const owner_a_tick = try owner_a.runOnceDetailed();
    try std.testing.expect(owner_a_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), owner_a_tick.builds_started);
    {
        const stats = owner_a.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-owner-a"), stats.owner_id_hash);
        try std.testing.expectEqual(owner_a.stats().lease_key_hash, owner_b.stats().lease_key_hash);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 0), stats.lost_leases);
        try std.testing.expectEqual(@as(u64, 1_000), stats.last_acquired_ms);
    }

    const blocked_tick = try owner_b.runOnceDetailed();
    try std.testing.expect(!blocked_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), blocked_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 0), blocked_tick.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), blocked_tick.coordinator_steps);
    {
        const stats = owner_b.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-owner-b"), stats.owner_id_hash);
        try std.testing.expectEqual(@as(u64, 0), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 1), stats.ticks_started);
        try std.testing.expectEqual(@as(u64, 1), stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 1), stats.idle_ticks);
    }

    manual_clock.advanceMs(101);
    const takeover_tick = try owner_b.runOnceDetailed();
    try std.testing.expect(takeover_tick.durableProgressed());
    {
        const stats = owner_b.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 1_101), stats.last_acquired_ms);
    }

    const lost_tick = try owner_a.runOnceDetailed();
    try std.testing.expect(!lost_tick.durableProgressed());
    {
        const stats = owner_a.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.lost_leases);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    db.graph_metric_runtime = &owner_b;
    defer db.graph_metric_runtime = null;
    {
        const mapped_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, mapped_stats);
        try std.testing.expect(mapped_stats.graph_metric_runtime.lease_owned);
        try std.testing.expect(mapped_stats.graph_metric_runtime.has_lease);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-owner-b"), mapped_stats.graph_metric_runtime.owner_id_hash);
        try std.testing.expectEqual(owner_b.stats().lease_key_hash, mapped_stats.graph_metric_runtime.lease_key_hash);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.takeover_count);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 1_101), mapped_stats.graph_metric_runtime.last_acquired_ms);
    }
}

test "db graph metric runtime lease releases durable owner lease on deinit" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(5_000 * std.time.ns_per_ms);
    const resources = db.core.asyncResources();
    var owner_a = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-release-a",
            .lease_owned = true,
            .owner_id = "runtime-release-owner-a",
            .lease_ttl_ms = 30_000,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-release-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    var owner_a_active = true;
    errdefer if (owner_a_active) owner_a.deinit();

    const owner_a_tick = try owner_a.runOnceDetailed();
    try std.testing.expect(!owner_a_tick.durableProgressed());
    {
        const stats = owner_a.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
    }
    const owner_a_lease_key_hash = owner_a.stats().lease_key_hash;
    {
        var lease = try lease_mod.Lease.init(alloc, resources.store, default_combined_lease_key);
        defer lease.deinit();
        var record = (try lease.load(alloc)) orelse return error.TestExpectedGraphMetricRuntimeLease;
        defer lease_mod.deinitRecord(alloc, &record);
        try std.testing.expectEqualStrings("runtime-release-owner-a", record.owner_id);
        try std.testing.expectEqual(@as(u64, 35_000), record.expires_at_ms);
    }

    owner_a.deinit();
    owner_a_active = false;
    {
        var lease = try lease_mod.Lease.init(alloc, resources.store, default_combined_lease_key);
        defer lease.deinit();
        try std.testing.expect((try lease.load(alloc)) == null);
    }

    var owner_b = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-release-b",
            .lease_owned = true,
            .owner_id = "runtime-release-owner-b",
            .lease_ttl_ms = 30_000,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-release-worker-b",
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    defer owner_b.deinit();

    const owner_b_tick = try owner_b.runOnceDetailed();
    try std.testing.expect(!owner_b_tick.durableProgressed());
    {
        const stats = owner_b.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(owner_a_lease_key_hash, stats.lease_key_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-release-owner-b"), stats.owner_id_hash);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 5_000), stats.last_acquired_ms);
    }
}

test "db graph metric runtime lease stale deinit preserves replacement owner lease" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(6_000 * std.time.ns_per_ms);
    const resources = db.core.asyncResources();
    var owner_a = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-stale-release-a",
            .lease_owned = true,
            .owner_id = "runtime-stale-release-owner-a",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-stale-release-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    var owner_a_active = true;
    errdefer if (owner_a_active) owner_a.deinit();

    const owner_a_tick = try owner_a.runOnceDetailed();
    try std.testing.expect(!owner_a_tick.durableProgressed());
    {
        const stats = owner_a.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 6_000), stats.last_acquired_ms);
    }
    const lease_key_hash = owner_a.stats().lease_key_hash;
    {
        var lease = try lease_mod.Lease.init(alloc, resources.store, default_combined_lease_key);
        defer lease.deinit();
        var record = (try lease.load(alloc)) orelse return error.TestExpectedGraphMetricRuntimeLease;
        defer lease_mod.deinitRecord(alloc, &record);
        try std.testing.expectEqualStrings("runtime-stale-release-owner-a", record.owner_id);
        try std.testing.expectEqual(@as(u64, 6_100), record.expires_at_ms);
    }

    manual_clock.advanceMs(101);
    var owner_b = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-stale-release-b",
            .lease_owned = true,
            .owner_id = "runtime-stale-release-owner-b",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-stale-release-worker-b",
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    defer owner_b.deinit();

    const owner_b_tick = try owner_b.runOnceDetailed();
    try std.testing.expect(!owner_b_tick.durableProgressed());
    {
        const stats = owner_b.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(lease_key_hash, stats.lease_key_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-stale-release-owner-b"), stats.owner_id_hash);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 6_101), stats.last_acquired_ms);
    }
    {
        var lease = try lease_mod.Lease.init(alloc, resources.store, default_combined_lease_key);
        defer lease.deinit();
        var record = (try lease.load(alloc)) orelse return error.TestExpectedGraphMetricRuntimeLease;
        defer lease_mod.deinitRecord(alloc, &record);
        try std.testing.expectEqualStrings("runtime-stale-release-owner-b", record.owner_id);
        try std.testing.expectEqual(@as(u64, 6_201), record.expires_at_ms);
    }

    owner_a.deinit();
    owner_a_active = false;
    {
        var lease = try lease_mod.Lease.init(alloc, resources.store, default_combined_lease_key);
        defer lease.deinit();
        var record = (try lease.load(alloc)) orelse return error.TestExpectedGraphMetricRuntimeLease;
        defer lease_mod.deinitRecord(alloc, &record);
        try std.testing.expectEqualStrings("runtime-stale-release-owner-b", record.owner_id);
        try std.testing.expectEqual(@as(u64, 6_201), record.expires_at_ms);
    }

    var owner_c = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-stale-release-c",
            .lease_owned = true,
            .owner_id = "runtime-stale-release-owner-c",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-stale-release-worker-c",
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    defer owner_c.deinit();

    const owner_c_tick = try owner_c.runOnceDetailed();
    try std.testing.expect(!owner_c_tick.durableProgressed());
    {
        const stats = owner_c.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(lease_key_hash, stats.lease_key_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-stale-release-owner-c"), stats.owner_id_hash);
        try std.testing.expectEqual(@as(u64, 0), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    const owner_b_renew_tick = try owner_b.runOnceDetailed();
    try std.testing.expect(!owner_b_renew_tick.durableProgressed());
    {
        const stats = owner_b.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lost_leases);
    }
}

test "db graph metric runtime role leases allow split owners and block duplicate coordinators" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(2_000 * std.time.ns_per_ms);
    const resources = db.core.asyncResources();
    var coordinator = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-role-lease-coordinator-a",
            .lease_owned = true,
            .owner_id = "role-lease-coordinator-a",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-role-lease-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator.deinit();
    var duplicate_coordinator = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-role-lease-coordinator-b",
            .lease_owned = true,
            .owner_id = "role-lease-coordinator-b",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-role-lease-coordinator-b-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer duplicate_coordinator.deinit();
    var worker = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-role-lease-worker",
            .lease_owned = true,
            .owner_id = "role-lease-worker",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-role-lease-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker.deinit();
    var duplicate_worker = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-role-lease-worker-duplicate",
            .lease_owned = true,
            .owner_id = "role-lease-worker-duplicate",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-role-lease-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer duplicate_worker.deinit();

    const early_worker = try worker.runOnceDetailed();
    try std.testing.expect(!early_worker.durableProgressed());
    {
        const stats = worker.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
    }

    const coordinator_start = try coordinator.runOnceDetailed();
    try std.testing.expect(coordinator_start.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), coordinator_start.builds_started);
    {
        const stats = coordinator.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
    }

    const duplicate_blocked = try duplicate_coordinator.runOnceDetailed();
    try std.testing.expect(!duplicate_blocked.durableProgressed());
    {
        const stats = duplicate_coordinator.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 0), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    const duplicate_worker_blocked = try duplicate_worker.runOnceDetailed();
    try std.testing.expect(!duplicate_worker_blocked.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), duplicate_worker_blocked.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), duplicate_worker_blocked.pages_completed);
    {
        const stats = duplicate_worker.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(worker.stats().lease_key_hash, stats.lease_key_hash);
        try std.testing.expectEqual(worker.stats().worker_id_hash, stats.worker_id_hash);
        try std.testing.expectEqual(@as(u64, 0), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    const worker_prepare = try worker.runOnceDetailed();
    try std.testing.expect(worker_prepare.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.pages_completed);

    manual_clock.advanceMs(101);
    const duplicate_takeover = try duplicate_coordinator.runOnceDetailed();
    try std.testing.expect(duplicate_takeover.durableProgressed());
    {
        const stats = duplicate_coordinator.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 2_101), stats.last_acquired_ms);
    }

    const coordinator_lost = try coordinator.runOnceDetailed();
    try std.testing.expect(!coordinator_lost.durableProgressed());
    {
        const stats = coordinator.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.lost_leases);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    const worker_after_coordinator_takeover = try worker.runOnceDetailed();
    try std.testing.expect(worker_after_coordinator_takeover.durableProgressed());
    try std.testing.expect(worker_after_coordinator_takeover.worker_steps > 0);
    {
        const stats = worker.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lost_leases);
    }
}

test "db graph metric runtime role worker leases are scoped by worker identity" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(3_000 * std.time.ns_per_ms);
    const resources = db.core.asyncResources();
    var worker_a = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-worker-lease-a",
            .lease_owned = true,
            .owner_id = "runtime-worker-owner-a",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_a.deinit();
    var worker_b = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-worker-lease-b",
            .lease_owned = true,
            .owner_id = "runtime-worker-owner-b",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-worker-b",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_b.deinit();
    var duplicate_worker_a = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-worker-lease-a-duplicate",
            .lease_owned = true,
            .owner_id = "runtime-worker-owner-a-duplicate",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer duplicate_worker_a.deinit();

    const idle_a = try worker_a.runOnceDetailed();
    try std.testing.expect(!idle_a.durableProgressed());
    {
        const stats = worker_a.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(usize, 1), stats.worker_count);
        try std.testing.expect(stats.worker_id_hash != 0);
        try std.testing.expect(stats.lease_key_hash != 0);
        try std.testing.expect(stats.lease_key_hash != worker_b.stats().lease_key_hash);
        try std.testing.expectEqual(stats.lease_key_hash, duplicate_worker_a.stats().lease_key_hash);
    }

    const idle_b = try worker_b.runOnceDetailed();
    try std.testing.expect(!idle_b.durableProgressed());
    {
        const stats = worker_b.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(usize, 1), stats.worker_count);
        try std.testing.expect(stats.worker_id_hash != 0);
    }

    const duplicate_blocked = try duplicate_worker_a.runOnceDetailed();
    try std.testing.expect(!duplicate_blocked.durableProgressed());
    {
        const stats = duplicate_worker_a.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 0), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    manual_clock.advanceMs(101);
    const duplicate_takeover = try duplicate_worker_a.runOnceDetailed();
    try std.testing.expect(!duplicate_takeover.durableProgressed());
    {
        const stats = duplicate_worker_a.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.takeover_count);
        try std.testing.expectEqual(@as(u64, 3_101), stats.last_acquired_ms);
    }

    const worker_a_lost = try worker_a.runOnceDetailed();
    try std.testing.expect(!worker_a_lost.durableProgressed());
    {
        const stats = worker_a.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.lost_leases);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    const worker_b_renewed = try worker_b.runOnceDetailed();
    try std.testing.expect(!worker_b_renewed.durableProgressed());
    {
        const stats = worker_b.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lost_leases);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
    }
}

test "db graph metric runtime role worker pool leases are scoped by worker identity set" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(4_000 * std.time.ns_per_ms);
    const resources = db.core.asyncResources();
    const pool_workers = [_][]const u8{ "runtime-pool-worker-a", "runtime-pool-worker-b" };
    const reversed_pool_workers = [_][]const u8{ "runtime-pool-worker-b", "runtime-pool-worker-a" };
    const other_pool_workers = [_][]const u8{ "runtime-pool-worker-c", "runtime-pool-worker-d" };
    var pool_a = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-worker-pool-lease-a",
            .lease_owned = true,
            .owner_id = "runtime-worker-pool-owner-a",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_ids = pool_workers[0..],
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer pool_a.deinit();
    var duplicate_reordered_pool = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-worker-pool-lease-a-reordered",
            .lease_owned = true,
            .owner_id = "runtime-worker-pool-owner-a-reordered",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_ids = reversed_pool_workers[0..],
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer duplicate_reordered_pool.deinit();
    var pool_b = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-worker-pool-lease-b",
            .lease_owned = true,
            .owner_id = "runtime-worker-pool-owner-b",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_ids = other_pool_workers[0..],
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer pool_b.deinit();

    const idle_pool_a = try pool_a.runOnceDetailed();
    try std.testing.expect(!idle_pool_a.durableProgressed());
    {
        const stats = pool_a.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(usize, 2), stats.worker_count);
        try std.testing.expect(stats.worker_id_hash != 0);
        try std.testing.expect(stats.lease_key_hash != 0);
        try std.testing.expectEqual(stats.lease_key_hash, duplicate_reordered_pool.stats().lease_key_hash);
        try std.testing.expect(stats.lease_key_hash != pool_b.stats().lease_key_hash);
    }

    const duplicate_blocked = try duplicate_reordered_pool.runOnceDetailed();
    try std.testing.expect(!duplicate_blocked.durableProgressed());
    {
        const stats = duplicate_reordered_pool.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 0), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
        try std.testing.expectEqual(pool_a.stats().worker_id_hash, stats.worker_id_hash);
    }

    const idle_pool_b = try pool_b.runOnceDetailed();
    try std.testing.expect(!idle_pool_b.durableProgressed());
    {
        const stats = pool_b.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 0), stats.lease_acquire_failures);
        try std.testing.expect(stats.worker_id_hash != pool_a.stats().worker_id_hash);
    }

    manual_clock.advanceMs(101);
    const duplicate_takeover = try duplicate_reordered_pool.runOnceDetailed();
    try std.testing.expect(!duplicate_takeover.durableProgressed());
    {
        const stats = duplicate_reordered_pool.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 4_101), stats.last_acquired_ms);
    }
}

test "db graph metric runtime role planned worker pools reject duplicate worker identities" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const duplicate_workers = [_][]const u8{ "runtime-pool-duplicate", "runtime-pool-duplicate" };
    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_ids = duplicate_workers[0..],
        .max_rounds = 1,
        .max_metrics_per_round = 1,
        .max_pages_per_round = 2,
    }));

    const resources = db.core.asyncResources();
    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-worker-pool-duplicate",
            .planned_options = .{
                .worker_ids = duplicate_workers[0..],
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 2,
            },
        },
    ));
}

test "db graph metric runtime role owned runtime worker calls are bound to configured identity" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(5_000 * std.time.ns_per_ms);
    const resources = db.core.asyncResources();
    var worker_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-owned-bound-worker",
            .lease_owned = true,
            .owner_id = "runtime-owned-bound-worker-owner",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-owned-bound-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_runtime.deinit();

    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, worker_runtime.runWorkerOnce("runtime-owned-bound-worker-b"));
    {
        const stats = worker_runtime.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.ticks_started);
        try std.testing.expectEqual(@as(u64, 0), stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 1), stats.error_ticks);
        try std.testing.expectEqualStrings("InvalidGraphMetricBuildWorker", stats.last_error_name.?);
    }

    const allowed_worker_tick = try worker_runtime.runWorkerOnce("runtime-owned-bound-worker-a");
    try std.testing.expect(!allowed_worker_tick.durableProgressed());
    {
        const stats = worker_runtime.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 1), stats.idle_ticks);
        try std.testing.expectEqual(@as(?[]const u8, null), stats.last_error_name);
    }
    try std.testing.expectError(error.InvalidGraphMetricRuntimeRole, worker_runtime.runCoordinatorOnce(false));
    {
        const stats = worker_runtime.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqualStrings("InvalidGraphMetricRuntimeRole", stats.last_error_name.?);
    }

    const pool_workers = [_][]const u8{ "runtime-owned-bound-pool-a", "runtime-owned-bound-pool-b" };
    var pool_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-owned-bound-pool",
            .lease_owned = true,
            .owner_id = "runtime-owned-bound-pool-owner",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_ids = pool_workers[0..],
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    defer pool_runtime.deinit();

    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, pool_runtime.runWorkerOnce("runtime-owned-bound-pool-c"));
    {
        const stats = pool_runtime.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.error_ticks);
        try std.testing.expectEqualStrings("InvalidGraphMetricBuildWorker", stats.last_error_name.?);
    }

    const allowed_pool_tick = try pool_runtime.runWorkerOnce("runtime-owned-bound-pool-b");
    try std.testing.expect(!allowed_pool_tick.durableProgressed());
    {
        const stats = pool_runtime.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 1), stats.ticks_completed);
        try std.testing.expectEqual(@as(?[]const u8, null), stats.last_error_name);
    }
    try std.testing.expectError(error.InvalidGraphMetricRuntimeRole, pool_runtime.runCoordinatorOnce(false));
    {
        const stats = pool_runtime.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqualStrings("InvalidGraphMetricRuntimeRole", stats.last_error_name.?);
    }

    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-owned-bound-coordinator",
            .lease_owned = true,
            .owner_id = "runtime-owned-bound-coordinator-owner",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-owned-bound-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 1,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, coordinator_runtime.runWorkerOnce("runtime-owned-bound-coordinator-worker"));
    {
        const stats = coordinator_runtime.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.error_ticks);
        try std.testing.expectEqualStrings("InvalidGraphMetricBuildWorker", stats.last_error_name.?);
    }
    try std.testing.expectError(error.InvalidGraphMetricRuntimeRole, coordinator_runtime.runWorkerPoolOnce());
    {
        const stats = coordinator_runtime.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqualStrings("InvalidGraphMetricRuntimeRole", stats.last_error_name.?);
    }
}

test "db graph metric runtime role automatic coordinator and worker loops stay separate" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const resources = db.core.asyncResources();
    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-role-coordinator-owner",
            .planned_options = .{
                .worker_id = "runtime-role-coordinator",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    var worker_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-role-worker-owner",
            .planned_options = .{
                .worker_id = "runtime-role-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_runtime.deinit();

    const early_worker = try worker_runtime.runOnceDetailed();
    try std.testing.expect(!early_worker.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), early_worker.builds_started);
    try std.testing.expectEqual(@as(usize, 0), early_worker.worker_steps);

    const coordinator_start = try coordinator_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), coordinator_start.builds_started);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.pages_completed);

    const worker_prepare = try worker_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), worker_prepare.phases_advanced);
    try std.testing.expectEqual(@as(usize, 0), worker_prepare.published);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, status.phase);
    }

    const coordinator_advance = try coordinator_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 0), coordinator_advance.builds_started);
    try std.testing.expect(coordinator_advance.phases_advanced > 0);

    var finished = false;
    var steps: usize = 0;
    while (steps < 200) : (steps += 1) {
        const worker_tick = try worker_runtime.runOnceDetailed();
        try std.testing.expectEqual(@as(usize, 0), worker_tick.phases_advanced);

        const coordinator_tick = try coordinator_runtime.runOnceDetailed();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            finished = true;
            break;
        }
        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expectEqual(Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-role-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_runtime.stats();
        try std.testing.expectEqual(Role.worker, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-role-worker-owner"), worker_stats.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-role-worker"), worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 1), worker_stats.worker_count);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:a", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime role distinct worker owners complete separate active pages" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });

    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(6_000 * std.time.ns_per_ms);
    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-distinct-workers-coordinator",
            .lease_owned = true,
            .owner_id = "runtime-distinct-workers-coordinator",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-workers-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    var worker_a_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-distinct-worker-a",
            .lease_owned = true,
            .owner_id = "runtime-distinct-worker-a",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_a_runtime.deinit();

    var worker_b_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-distinct-worker-b",
            .lease_owned = true,
            .owner_id = "runtime-distinct-worker-b",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-worker-b",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_b_runtime.deinit();

    var replacement_worker_a_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-distinct-worker-a-replacement",
            .lease_owned = true,
            .owner_id = "runtime-distinct-worker-a-replacement",
            .lease_ttl_ms = 100,
            .clock = manual_clock.clock(),
            .planned_options = .{
                .worker_id = "runtime-distinct-worker-a",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer replacement_worker_a_runtime.deinit();

    const coordinator_start = try coordinator_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), coordinator_start.builds_started);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.worker_steps);

    const worker_a_prepare = try worker_a_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), worker_a_prepare.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_a_prepare.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), worker_a_prepare.phases_advanced);

    const coordinator_scan = try coordinator_runtime.runOnceDetailed();
    try std.testing.expect(coordinator_scan.phases_advanced > 0);
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, status.phase);
    }

    const replacement_blocked = try replacement_worker_a_runtime.runOnceDetailed();
    try std.testing.expect(!replacement_blocked.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), replacement_blocked.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), replacement_blocked.pages_completed);
    {
        const stats = replacement_worker_a_runtime.stats();
        try std.testing.expect(stats.lease_owned);
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(worker_a_runtime.stats().lease_key_hash, stats.lease_key_hash);
        try std.testing.expectEqual(worker_a_runtime.stats().worker_id_hash, stats.worker_id_hash);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    manual_clock.advanceMs(101);
    const replacement_scan = try replacement_worker_a_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), replacement_scan.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), replacement_scan.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), replacement_scan.phases_advanced);
    try std.testing.expect(replacement_scan.budget_exhausted);
    {
        const stats = replacement_worker_a_runtime.stats();
        try std.testing.expect(stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.acquisition_count);
        try std.testing.expectEqual(@as(u64, 6_101), stats.last_acquired_ms);
        try std.testing.expect(stats.last_result.budget_exhausted);
    }

    const worker_a_lost = try worker_a_runtime.runOnceDetailed();
    try std.testing.expect(!worker_a_lost.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), worker_a_lost.worker_steps);
    {
        const stats = worker_a_runtime.stats();
        try std.testing.expect(!stats.has_lease);
        try std.testing.expectEqual(@as(u64, 1), stats.lost_leases);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_acquire_failures);
    }

    const worker_b_scan = try worker_b_runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), worker_b_scan.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_b_scan.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), worker_b_scan.phases_advanced);

    {
        const worker_a_stats = worker_a_runtime.stats();
        const worker_b_stats = worker_b_runtime.stats();
        try std.testing.expect(worker_a_stats.lease_owned);
        try std.testing.expect(worker_b_stats.lease_owned);
        try std.testing.expect(!worker_a_stats.has_lease);
        try std.testing.expect(worker_b_stats.has_lease);
        try std.testing.expect(worker_a_stats.lease_key_hash != 0);
        try std.testing.expect(worker_b_stats.lease_key_hash != 0);
        try std.testing.expect(worker_a_stats.lease_key_hash != worker_b_stats.lease_key_hash);
        try std.testing.expect(worker_a_stats.worker_id_hash != worker_b_stats.worker_id_hash);
        try std.testing.expectEqual(@as(u64, 1), worker_a_stats.lease_acquire_failures);
        try std.testing.expectEqual(@as(u64, 0), worker_b_stats.lease_acquire_failures);
        try std.testing.expect(worker_a_stats.total_result.pages_completed >= 1);
        try std.testing.expect(worker_b_stats.total_result.pages_completed >= 1);
        const replacement_stats = replacement_worker_a_runtime.stats();
        try std.testing.expect(replacement_stats.has_lease);
        try std.testing.expectEqual(worker_a_stats.lease_key_hash, replacement_stats.lease_key_hash);
        try std.testing.expectEqual(worker_a_stats.worker_id_hash, replacement_stats.worker_id_hash);
        try std.testing.expect(replacement_stats.total_result.pages_completed >= 1);
    }

    var finished = false;
    var steps: usize = 0;
    while (steps < 200) : (steps += 1) {
        const worker_tick = if (steps % 2 == 0)
            try replacement_worker_a_runtime.runOnceDetailed()
        else
            try worker_b_runtime.runOnceDetailed();
        try std.testing.expectEqual(@as(usize, 0), worker_tick.phases_advanced);

        const coordinator_tick = try coordinator_runtime.runOnceDetailed();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation and status.phase == .complete) {
            finished = true;
            break;
        }
        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime background skips paused metrics" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var paused = try db.pauseGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer paused.deinit(alloc);
    try std.testing.expect(paused.maintenance_paused);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, paused.state);

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expectEqual(@as(usize, 1), pending.paused_metrics);
        try std.testing.expect(!pending.hasWork());
    }

    const resources = db.core.asyncResources();
    var runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-paused-degree",
            .planned_options = .{
                .worker_id = "runtime-paused-degree-worker",
                .max_rounds = 4,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 4,
            },
        },
    );
    defer runtime.deinit();

    const paused_tick = try runtime.runOnceDetailed();
    try std.testing.expect(!paused_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), paused_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 0), paused_tick.pages_claimed);
    try std.testing.expectEqual(@as(usize, 0), paused_tick.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), paused_tick.published);
    {
        const runtime_stats = runtime.stats();
        try std.testing.expectEqual(@as(u64, 1), runtime_stats.ticks_started);
        try std.testing.expectEqual(@as(u64, 1), runtime_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 0), runtime_stats.durable_progress_ticks);
        try std.testing.expectEqual(@as(u64, 1), runtime_stats.idle_ticks);
        try std.testing.expectEqual(@as(u64, 0), runtime_stats.error_ticks);
        try std.testing.expect(!runtime_stats.last_result.durableProgressed());
    }

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expect(status.maintenance_paused);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        try std.testing.expectEqual(@as(u64, 0), status.build_job_id);
        try std.testing.expectEqual(@as(u64, 0), status.published_generation);
    }

    var resumed = try db.resumeGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer resumed.deinit(alloc);
    try std.testing.expect(!resumed.maintenance_paused);

    var steps: usize = 0;
    while (try runtime.runOnce()) {
        steps += 1;
        if (steps > 200) return error.TestUnexpectedResult;
    }
    try std.testing.expect(steps > 0);

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background idles on failed planned generation" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const resources = db.core.asyncResources();
    var runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-failed-terminal-degree",
            .planned_options = .{
                .worker_id = "runtime-failed-terminal-degree-worker",
                .max_rounds = 4,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 4,
            },
        },
    );
    defer runtime.deinit();
    db.graph_metric_runtime = &runtime;
    defer db.graph_metric_runtime = null;

    const started_tick = try runtime.runCoordinatorOnce(true);
    try std.testing.expect(started_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), started_tick.builds_started);

    var failed = try db.failGraphMetricPlannedBuild(alloc, "graph_idx", "degree", error.InvalidGraphMetricBuildManifest);
    defer failed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expect(failed.build_queued);
    try std.testing.expectEqualStrings("InvalidGraphMetricBuildManifest", failed.last_error);
    const failed_target_generation = failed.target_edge_generation;

    const terminal_tick = try runtime.runOnceDetailed();
    try std.testing.expect(!terminal_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), terminal_tick.active_builds);
    try std.testing.expectEqual(@as(usize, 0), terminal_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 0), terminal_tick.pages_claimed);
    try std.testing.expectEqual(@as(usize, 0), terminal_tick.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), terminal_tick.published);
    try std.testing.expectEqual(@as(usize, 0), terminal_tick.failed_builds);

    {
        const runtime_stats = runtime.stats();
        try std.testing.expectEqual(@as(u64, 2), runtime_stats.ticks_started);
        try std.testing.expectEqual(@as(u64, 2), runtime_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 1), runtime_stats.durable_progress_ticks);
        try std.testing.expectEqual(@as(u64, 1), runtime_stats.idle_ticks);
        try std.testing.expectEqual(@as(u64, 0), runtime_stats.error_ticks);
        try std.testing.expect(!runtime_stats.last_result.durableProgressed());
    }
    {
        const mapped_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, mapped_stats);
        try std.testing.expect(mapped_stats.graph_metric_runtime.enabled);
        try std.testing.expectEqual(types.GraphMetricRuntimeRole.combined, mapped_stats.graph_metric_runtime.role.?);
        try std.testing.expectEqual(@as(u64, 2), mapped_stats.graph_metric_runtime.ticks_started);
        try std.testing.expectEqual(mapped_stats.graph_metric_runtime.ticks_started, mapped_stats.graph_metric_runtime.ticks_completed);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.durable_progress_ticks);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.idle_ticks);
        try std.testing.expectEqual(@as(u64, 0), mapped_stats.graph_metric_runtime.error_ticks);
        try std.testing.expectEqual(@as(u64, 0), mapped_stats.graph_metric_runtime.last_builds_started);
        try std.testing.expectEqual(@as(u64, 0), mapped_stats.graph_metric_runtime.last_failed_builds);
        try std.testing.expect(!mapped_stats.graph_metric_runtime.last_budget_exhausted);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, status.state);
        try std.testing.expectEqual(failed_target_generation, status.target_edge_generation);
        try std.testing.expectEqual(@as(u64, 0), status.build_job_id);
        var failed_events: usize = 0;
        for (status.recent_events) |event| {
            if (event.kind == .failed) failed_events += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), failed_events);
    }

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:c",
            .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const new_generation_tick = try runtime.runCoordinatorOnce(true);
    try std.testing.expect(new_generation_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), new_generation_tick.builds_started);
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expect(status.building_generation > failed_target_generation);
    }
}

test "db graph metric runtime background skips paused active planned builds" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-paused-active-degree",
            .planned_options = .{
                .worker_id = "runtime-paused-active-degree-worker",
                .max_rounds = 4,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 4,
            },
        },
    );
    defer runtime.deinit();

    const started_tick = try runtime.runCoordinatorOnce(true);
    try std.testing.expect(started_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), started_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 0), started_tick.pages_claimed);
    try std.testing.expectEqual(@as(usize, 0), started_tick.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), started_tick.published);

    const active_job_id, const active_phase = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(target_generation, status.building_generation);
        try std.testing.expect(status.build_job_id != 0);
        break :blk .{ status.build_job_id, status.phase };
    };

    var paused = try db.pauseGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer paused.deinit(alloc);
    try std.testing.expect(paused.maintenance_paused);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, paused.state);
    try std.testing.expectEqual(target_generation, paused.building_generation);
    try std.testing.expectEqual(active_job_id, paused.build_job_id);
    try std.testing.expectEqual(active_phase, paused.phase);

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expectEqual(@as(usize, 1), pending.paused_metrics);
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
        try std.testing.expect(!pending.hasWork());
    }

    const paused_worker_tick = try runtime.runWorkerOnce("runtime-paused-active-worker");
    try std.testing.expect(!paused_worker_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), paused_worker_tick.active_builds);
    try std.testing.expectEqual(@as(usize, 0), paused_worker_tick.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), paused_worker_tick.pages_claimed);
    try std.testing.expectEqual(@as(usize, 0), paused_worker_tick.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), paused_worker_tick.published);

    const paused_coordinator_tick = try runtime.runCoordinatorOnce(true);
    try std.testing.expect(!paused_coordinator_tick.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), paused_coordinator_tick.active_builds);
    try std.testing.expectEqual(@as(usize, 0), paused_coordinator_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 0), paused_coordinator_tick.coordinator_steps);
    try std.testing.expectEqual(@as(usize, 0), paused_coordinator_tick.phases_advanced);
    try std.testing.expectEqual(@as(usize, 0), paused_coordinator_tick.published);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expect(status.maintenance_paused);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(target_generation, status.building_generation);
        try std.testing.expectEqual(active_job_id, status.build_job_id);
        try std.testing.expectEqual(active_phase, status.phase);
        try std.testing.expectEqual(@as(usize, 0), status.build_pages.len);
        try std.testing.expectEqual(@as(u64, 0), status.published_generation);
    }

    var resumed = try db.resumeGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer resumed.deinit(alloc);
    try std.testing.expect(!resumed.maintenance_paused);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, resumed.state);
    try std.testing.expectEqual(target_generation, resumed.building_generation);
    try std.testing.expectEqual(active_job_id, resumed.build_job_id);

    var finished = false;
    var step_index: usize = 0;
    while (step_index < 200) : (step_index += 1) {
        const worker_tick = try runtime.runWorkerOnce("runtime-resumed-active-worker");
        const coordinator_tick = try runtime.runCoordinatorOnce(false);

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            finished = true;
            break;
        }
        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background starts automatically and drains notified degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_maintenance = .{
            .enabled = true,
            .runtime_id = "runtime-auto-degree",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-auto-degree-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.executor.waitForAll(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var fresh = false;
    for (0..200) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const runtime_stats = db.graphMetricRuntimeStats();
        try std.testing.expect(runtime_stats.enabled);
        try std.testing.expectEqual(types.GraphMetricRuntimeRole.combined, runtime_stats.role.?);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-auto-degree"), runtime_stats.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-auto-degree-worker"), runtime_stats.worker_id_hash);
        try std.testing.expect(runtime_stats.ticks_started > 0);
        try std.testing.expect(runtime_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), runtime_stats.error_ticks);
        try std.testing.expect(runtime_stats.total_builds_started > 0);
        try std.testing.expect(runtime_stats.total_worker_steps > 0);
        try std.testing.expect(runtime_stats.total_coordinator_steps > 0);
        try std.testing.expect(runtime_stats.total_pages_completed > 0);
        try std.testing.expect(runtime_stats.total_published > 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:a", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background open-configured split owners publish degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
            .sync_level = .write,
        });

        for (0..64) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
            defer alloc.free(key);
            const value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
                .{i},
            );
            defer alloc.free(value);
            try db.batch(.{
                .writes = &.{.{ .key = key, .value = value }},
                .sync_level = .write,
            });
        }

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    const workers = [_][]const u8{ "open-runtime-worker-a", "open-runtime-worker-b" };
    var saw_coordinator_role = false;
    var saw_worker_pool_role = false;
    var fresh = false;
    for (0..400) |_| {
        {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "open-configured-coordinator",
                    .lease_owned = true,
                    .owner_id = "open-configured-coordinator",
                    .planned_options = .{
                        .worker_id = "open-configured-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            coordinator_total.add(tick);
            const stats = coordinator.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.coordinator, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "open-configured-coordinator"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "open-configured-coordinator"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_worker_steps);
            try std.testing.expectEqual(@as(u64, 0), stats.error_ticks);
            saw_coordinator_role = true;
        }

        {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .worker_pool,
                    .runtime_id = "open-configured-worker-pool",
                    .lease_owned = true,
                    .owner_id = "open-configured-worker-pool",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            });
            defer worker_pool.close();

            const tick = try worker_pool.graph_metric_runtime.?.runOnceDetailed();
            worker_total.add(tick);
            const stats = worker_pool.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "open-configured-worker-pool"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "open-configured-worker-pool"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(workerSetIdentityHash(workers[0..]), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 2), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_coordinator_steps);
            try std.testing.expectEqual(@as(u64, 0), stats.total_published);
            try std.testing.expectEqual(@as(u64, 0), stats.error_ticks);
            saw_worker_pool_role = true;
        }

        {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "open-configured-coordinator",
                    .lease_owned = true,
                    .owner_id = "open-configured-coordinator",
                    .planned_options = .{
                        .worker_id = "open-configured-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            coordinator_total.add(tick);
            const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("degree");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation) {
                fresh = true;
                break;
            }
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_coordinator_role);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(worker_total.worker_steps > 0);
    try std.testing.expect(worker_total.pages_completed > 0);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expect(coordinator_total.published > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 64.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
    }
}

test "db graph metric runtime background separates coordinator and worker ticks" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const resources = db.core.asyncResources();
    var runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .planned_options = .{
                .worker_id = "runtime-split-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer runtime.deinit();

    try std.testing.expectError(error.InvalidGraphMetricBuildWorker, runtime.runWorkerOnce(""));
    {
        const error_stats = runtime.stats();
        try std.testing.expectEqual(@as(u64, 1), error_stats.ticks_started);
        try std.testing.expectEqual(@as(u64, 0), error_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 1), error_stats.error_ticks);
        try std.testing.expectEqualStrings("InvalidGraphMetricBuildWorker", error_stats.last_error_name.?);
    }

    const coordinator_start = try runtime.runCoordinatorOnce(true);
    try std.testing.expectEqual(@as(usize, 1), coordinator_start.builds_started);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), coordinator_start.pages_completed);
    {
        const recovered_stats = runtime.stats();
        try std.testing.expectEqual(@as(?[]const u8, null), recovered_stats.last_error_name);
        try std.testing.expectEqual(@as(u64, 1), recovered_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 1), recovered_stats.durable_progress_ticks);
        try std.testing.expectEqual(@as(usize, 1), recovered_stats.last_result.builds_started);
    }

    const worker_prepare = try runtime.runWorkerOnce("runtime-split-worker");
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), worker_prepare.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), worker_prepare.phases_advanced);
    try std.testing.expectEqual(@as(usize, 0), worker_prepare.published);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, status.phase);
    }

    const coordinator_advance = try runtime.runCoordinatorOnce(false);
    try std.testing.expectEqual(@as(usize, 0), coordinator_advance.builds_started);
    try std.testing.expect(coordinator_advance.phases_advanced > 0);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, status.phase);
    }

    var steps: usize = 0;
    while (try runtime.runOnce()) {
        steps += 1;
        if (steps > 200) return error.TestUnexpectedResult;
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:a", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime background coordinator and worker loops publish degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    var worker_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker,
            .runtime_id = "runtime-bg-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer worker_runtime.deinit();

    try coordinator_runtime.start();
    try worker_runtime.start();
    coordinator_runtime.notify();
    worker_runtime.notify();

    var fresh = false;
    for (0..300) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(Role.worker, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-worker-owner"), worker_stats.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-worker"), worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 1), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:a", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background coordinator and worker pool loops publish degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });

    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-pool-worker-a", "runtime-bg-pool-worker-b" };
    var worker_pool_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..500) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps >= 2);
        try std.testing.expect(worker_stats.total_result.pages_completed >= 2);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 130.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background coordinator and worker pool loops publish pagerank" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-pagerank-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-pagerank-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-pagerank-pool-worker-a", "runtime-bg-pagerank-pool-worker-b" };
    var worker_pool_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-pagerank-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..700) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pagerank-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expect(coordinator_stats.total_result.published > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-pagerank-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime background coordinator and worker pool loops publish eigenvector" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-eigenvector-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-eigenvector-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-eigenvector-pool-worker-a", "runtime-bg-eigenvector-pool-worker-b" };
    var worker_pool_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-eigenvector-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..700) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-eigenvector-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expect(coordinator_stats.total_result.published > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-eigenvector-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "eigenvector",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
}

test "db graph metric runtime background coordinator and worker pool loops publish hits pair" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var coordinator_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .coordinator,
            .runtime_id = "runtime-bg-hits-pool-coordinator-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_id = "runtime-bg-hits-pool-coordinator-unused",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer coordinator_runtime.deinit();

    const workers = [_][]const u8{ "runtime-bg-hits-pool-worker-a", "runtime-bg-hits-pool-worker-b" };
    var worker_pool_runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .role = .worker_pool,
            .runtime_id = "runtime-bg-hits-pool-worker-owner",
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer worker_pool_runtime.deinit();

    try coordinator_runtime.start();
    try worker_pool_runtime.start();
    coordinator_runtime.notify();
    worker_pool_runtime.notify();

    var fresh = false;
    for (0..700) |_| {
        yieldToBackground();
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        if (authority_status.state == .fresh and
            hub_status.state == .fresh and
            authority_status.published_generation == target_generation and
            hub_status.published_generation == target_generation)
        {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);

    {
        const coordinator_stats = coordinator_runtime.stats();
        try std.testing.expect(coordinator_stats.started);
        try std.testing.expectEqual(Role.coordinator, coordinator_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-hits-pool-coordinator-owner"), coordinator_stats.runtime_id_hash);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.worker_count);
        try std.testing.expect(coordinator_stats.ticks_started > 0);
        try std.testing.expect(coordinator_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), coordinator_stats.error_ticks);
        try std.testing.expect(coordinator_stats.total_result.builds_started > 0);
        try std.testing.expect(coordinator_stats.total_result.coordinator_steps > 0);
        try std.testing.expect(coordinator_stats.total_result.phases_advanced > 0);
        try std.testing.expect(coordinator_stats.total_result.published > 0);
        try std.testing.expectEqual(@as(usize, 0), coordinator_stats.total_result.worker_steps);
        try std.testing.expect(coordinator_stats.last_result.worker_steps == 0);
    }
    {
        const worker_stats = worker_pool_runtime.stats();
        try std.testing.expect(worker_stats.started);
        try std.testing.expectEqual(Role.worker_pool, worker_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-bg-hits-pool-worker-owner"), worker_stats.runtime_id_hash);
        const expected_worker_hash = workerSetIdentityHash(workers[0..]);
        try std.testing.expectEqual(expected_worker_hash, worker_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 2), worker_stats.worker_count);
        try std.testing.expect(worker_stats.ticks_started > 0);
        try std.testing.expect(worker_stats.durable_progress_ticks > 0);
        try std.testing.expectEqual(@as(u64, 0), worker_stats.error_ticks);
        try std.testing.expect(worker_stats.total_result.worker_steps > 0);
        try std.testing.expect(worker_stats.total_result.pages_completed > 0);
        try std.testing.expectEqual(@as(usize, 0), worker_stats.total_result.coordinator_steps);
        try std.testing.expect(worker_stats.last_result.coordinator_steps == 0);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
    try std.testing.expectEqualStrings("doc:authority", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime background worker pool survives separate reopened handles" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
            .sync_level = .write,
        });

        for (0..130) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
            defer alloc.free(key);
            const value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
                .{i},
            );
            defer alloc.free(value);
            try db.batch(.{
                .writes = &.{.{ .key = key, .value = value }},
                .sync_level = .write,
            });
        }

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-pool-worker-a", "runtime-reopened-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-pool-worker-b", "runtime-reopened-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_two_page_worker_pool_tick = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..400) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_pool.close();

            const worker_resources = worker_pool.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                worker_resources.store,
                worker_resources.index_manager,
                worker_resources.apply_mutex,
                worker_pool.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            );
            defer runtime.deinit();

            const tick = try runtime.runOnceDetailed();
            const stats = runtime.stats();
            try std.testing.expectEqual(Role.worker_pool, stats.role);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            const expected_worker_hash = workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(usize, 2), stats.worker_count);
            try std.testing.expectEqual(@as(usize, 0), stats.total_result.coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps >= 2 and tick.pages_completed >= 2) saw_two_page_worker_pool_tick = true;
            if (tick.worker_steps > 0) {
                var duplicate_runtime = try GraphMetricRuntime.init(
                    alloc,
                    worker_resources.store,
                    worker_resources.index_manager,
                    worker_resources.apply_mutex,
                    worker_pool.backend_runtime,
                    .{
                        .enabled = true,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                );
                defer duplicate_runtime.deinit();

                const duplicate_tick = try duplicate_runtime.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_runtime.stats();
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const coordinator_resources = coordinator.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                coordinator_resources.store,
                coordinator_resources.index_manager,
                coordinator_resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const tick = try runtime.runOnceDetailed();
            const stats = runtime.stats();
            try std.testing.expectEqual(Role.coordinator, stats.role);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(usize, 0), stats.worker_count);
            try std.testing.expectEqual(@as(usize, 0), stats.total_result.worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("degree");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation) {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_two_page_worker_pool_tick);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps >= 2);
    try std.testing.expect(worker_total.pages_completed >= 2);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 130.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
    }
}

test "db graph metric runtime background open-configured pagerank worker pool survives separate reopened handles" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-pagerank-pool-worker-a", "runtime-reopened-pagerank-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-pagerank-pool-worker-b", "runtime-reopened-pagerank-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..500) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-pagerank-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pagerank-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            });
            defer worker_pool.close();

            const tick = try worker_pool.graph_metric_runtime.?.runOnceDetailed();
            const stats = worker_pool.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-worker-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            const expected_worker_hash = workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 2), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps > 0) {
                var duplicate_worker_pool = try DB.open(alloc, std.mem.span(path), .{
                    .open_mode = .writer_no_replay,
                    .ttl_cleanup = .{ .enabled = false },
                    .graph_metric_maintenance = .{
                        .enabled = true,
                        .start_background_loop = false,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-pagerank-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-pagerank-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                });
                defer duplicate_worker_pool.close();

                const duplicate_tick = try duplicate_worker_pool.graph_metric_runtime.?.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_worker_pool.graphMetricRuntimeStats();
                try std.testing.expect(duplicate_stats.enabled);
                try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, duplicate_stats.role.?);
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-pagerank-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-pagerank-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-pagerank-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            const stats = coordinator.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.coordinator, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-pagerank-pool-coordinator-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation) {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expect(coordinator_total.published > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps > 0);
    try std.testing.expect(worker_total.pages_completed > 0);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "pagerank",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
        try std.testing.expect(metric_result.graph_metric_results[0].scores[0].score >= metric_result.graph_metric_results[0].scores[1].score);
    }
}

test "db graph metric runtime background open-configured eigenvector worker pool survives separate reopened handles" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-eigenvector-pool-worker-a", "runtime-reopened-eigenvector-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-eigenvector-pool-worker-b", "runtime-reopened-eigenvector-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..500) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-eigenvector-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-eigenvector-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            });
            defer worker_pool.close();

            const tick = try worker_pool.graph_metric_runtime.?.runOnceDetailed();
            const stats = worker_pool.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-worker-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            const expected_worker_hash = workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 2), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps > 0) {
                var duplicate_worker_pool = try DB.open(alloc, std.mem.span(path), .{
                    .open_mode = .writer_no_replay,
                    .ttl_cleanup = .{ .enabled = false },
                    .graph_metric_maintenance = .{
                        .enabled = true,
                        .start_background_loop = false,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-eigenvector-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-eigenvector-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                });
                defer duplicate_worker_pool.close();

                const duplicate_tick = try duplicate_worker_pool.graph_metric_runtime.?.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_worker_pool.graphMetricRuntimeStats();
                try std.testing.expect(duplicate_stats.enabled);
                try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, duplicate_stats.role.?);
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-eigenvector-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-eigenvector-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-eigenvector-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            const stats = coordinator.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.coordinator, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-eigenvector-pool-coordinator-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation) {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expect(coordinator_total.published > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps > 0);
    try std.testing.expect(worker_total.pages_completed > 0);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "eigenvector",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "eigenvector",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric runtime background open-configured hits worker pool survives separate reopened handles" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "runtime-reopened-hits-pool-worker-a", "runtime-reopened-hits-pool-worker-b" };
    const reversed_workers = [_][]const u8{ "runtime-reopened-hits-pool-worker-b", "runtime-reopened-hits-pool-worker-a" };
    var coordinator_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var worker_total = index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var saw_worker_pool_role = false;
    var saw_live_duplicate_worker_pool_fenced = false;
    var fresh = false;
    for (0..500) |_| {
        const worker_tick = blk: {
            var worker_pool = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .worker_pool,
                    .runtime_id = "runtime-reopened-hits-pool-worker-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-hits-pool-worker-owner",
                    .planned_options = .{
                        .worker_ids = &workers,
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 2,
                    },
                },
            });
            defer worker_pool.close();

            const tick = try worker_pool.graph_metric_runtime.?.runOnceDetailed();
            const stats = worker_pool.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-worker-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-worker-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            const expected_worker_hash = workerSetIdentityHash(workers[0..]);
            try std.testing.expectEqual(expected_worker_hash, stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 2), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_coordinator_steps);
            saw_worker_pool_role = true;
            if (tick.worker_steps > 0) {
                var duplicate_worker_pool = try DB.open(alloc, std.mem.span(path), .{
                    .open_mode = .writer_no_replay,
                    .ttl_cleanup = .{ .enabled = false },
                    .graph_metric_maintenance = .{
                        .enabled = true,
                        .start_background_loop = false,
                        .role = .worker_pool,
                        .runtime_id = "runtime-reopened-hits-pool-worker-owner-duplicate",
                        .lease_owned = true,
                        .owner_id = "runtime-reopened-hits-pool-worker-owner-duplicate",
                        .planned_options = .{
                            .worker_ids = &reversed_workers,
                            .max_rounds = 1,
                            .max_metrics_per_round = 8,
                            .max_pages_per_round = 2,
                        },
                    },
                });
                defer duplicate_worker_pool.close();

                const duplicate_tick = try duplicate_worker_pool.graph_metric_runtime.?.runOnceDetailed();
                try std.testing.expect(!duplicate_tick.durableProgressed());
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.worker_steps);
                try std.testing.expectEqual(@as(usize, 0), duplicate_tick.pages_completed);
                const duplicate_stats = duplicate_worker_pool.graphMetricRuntimeStats();
                try std.testing.expect(duplicate_stats.enabled);
                try std.testing.expectEqual(types.GraphMetricRuntimeRole.worker_pool, duplicate_stats.role.?);
                try std.testing.expect(duplicate_stats.lease_owned);
                try std.testing.expect(!duplicate_stats.has_lease);
                try std.testing.expectEqual(stats.worker_id_hash, duplicate_stats.worker_id_hash);
                try std.testing.expectEqual(stats.lease_key_hash, duplicate_stats.lease_key_hash);
                try std.testing.expectEqual(@as(u64, 0), duplicate_stats.acquisition_count);
                try std.testing.expectEqual(@as(u64, 1), duplicate_stats.lease_acquire_failures);
                saw_live_duplicate_worker_pool_fenced = true;
            }
            break :blk tick;
        };
        worker_total.add(worker_tick);

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .writer_no_replay,
                .ttl_cleanup = .{ .enabled = false },
                .graph_metric_maintenance = .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-hits-pool-coordinator-owner",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-hits-pool-coordinator-owner",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-hits-pool-coordinator-unused",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            });
            defer coordinator.close();

            const tick = try coordinator.graph_metric_runtime.?.runOnceDetailed();
            const stats = coordinator.graphMetricRuntimeStats();
            try std.testing.expect(stats.enabled);
            try std.testing.expectEqual(types.GraphMetricRuntimeRole.coordinator, stats.role.?);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-coordinator-owner"), stats.runtime_id_hash);
            try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-reopened-hits-pool-coordinator-owner"), stats.owner_id_hash);
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expect(!stats.started);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_id_hash);
            try std.testing.expectEqual(@as(u64, 0), stats.worker_count);
            try std.testing.expectEqual(@as(u64, 0), stats.total_worker_steps);
            break :blk tick;
        };
        coordinator_total.add(coordinator_tick);

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .open_mode = .query_readonly,
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();

            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer authority_status.deinit(alloc);
            var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
            defer hub_status.deinit(alloc);
            if (authority_status.state == .fresh and
                hub_status.state == .fresh and
                authority_status.published_generation == target_generation and
                hub_status.published_generation == target_generation)
            {
                fresh = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(fresh);
    try std.testing.expect(saw_worker_pool_role);
    try std.testing.expect(saw_live_duplicate_worker_pool_fenced);
    try std.testing.expect(coordinator_total.builds_started > 0);
    try std.testing.expect(coordinator_total.coordinator_steps > 0);
    try std.testing.expect(coordinator_total.phases_advanced > 0);
    try std.testing.expect(coordinator_total.published > 0);
    try std.testing.expectEqual(@as(usize, 0), coordinator_total.worker_steps);
    try std.testing.expect(worker_total.worker_steps > 0);
    try std.testing.expect(worker_total.pages_completed > 0);
    try std.testing.expectEqual(@as(usize, 0), worker_total.coordinator_steps);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var metric_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{
                .{
                    .name = "authority",
                    .query = .{
                        .index_name = "graph_idx",
                        .metric_name = "hits_authority",
                        .top_k = 3,
                        .freshness = .fresh,
                    },
                },
                .{
                    .name = "hub",
                    .query = .{
                        .index_name = "graph_idx",
                        .metric_name = "hits_hub",
                        .top_k = 3,
                        .freshness = .fresh,
                    },
                },
            },
            .limit = 0,
        });
        defer metric_result.deinit();
        try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
        try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
        try std.testing.expectEqualStrings("doc:authority", metric_result.graph_metric_results[0].scores[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
    }
}

test "db graph metric runtime background split ticks survive reopened pagerank handles" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });

        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-reopened-coordinator",
                .lease_owned = true,
                .owner_id = "runtime-reopened-coordinator",
                .planned_options = .{
                    .worker_id = "runtime-reopened-coordinator",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const started = try runtime.runCoordinatorOnce(true);
        try std.testing.expectEqual(@as(usize, 1), started.builds_started);
        try std.testing.expectEqual(@as(usize, 0), started.worker_steps);
        try std.testing.expectEqual(@as(usize, 0), started.pages_completed);
        {
            const stats = runtime.stats();
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(Role.coordinator, stats.role);
        }
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        const resources = worker.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            worker.backend_runtime,
            .{
                .enabled = true,
                .role = .worker,
                .runtime_id = "runtime-reopened-worker-a",
                .lease_owned = true,
                .owner_id = "runtime-reopened-worker-a",
                .planned_options = .{
                    .worker_id = "runtime-reopened-worker-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const prepared = try runtime.runWorkerOnce("runtime-reopened-worker-a");
        try std.testing.expectEqual(@as(usize, 1), prepared.worker_steps);
        try std.testing.expectEqual(@as(usize, 1), prepared.pages_completed);
        try std.testing.expectEqual(@as(usize, 0), prepared.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), prepared.published);
        {
            const stats = runtime.stats();
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(Role.worker, stats.role);
        }
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, status.phase);
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-reopened-coordinator",
                .lease_owned = true,
                .owner_id = "runtime-reopened-coordinator",
                .planned_options = .{
                    .worker_id = "runtime-reopened-coordinator",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const advanced = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), advanced.builds_started);
        try std.testing.expect(advanced.phases_advanced > 0);
        {
            const stats = runtime.stats();
            try std.testing.expect(stats.lease_owned);
            try std.testing.expect(stats.has_lease);
            try std.testing.expectEqual(Role.coordinator, stats.role);
        }
    }

    const workers = [_][]const u8{ "runtime-reopened-worker-a", "runtime-reopened-worker-b" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .runtime_id = workers[step_index % workers.len],
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-reopened-coordinator",
                    .lease_owned = true,
                    .owner_id = "runtime-reopened-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-reopened-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            const tick = try runtime.runCoordinatorOnce(false);
            const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation == target_generation and status.phase == .complete) {
                try std.testing.expect(status.iterations_completed > 0);
                finished = true;
            }
            break :blk tick;
        };

        if (finished) break;
        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
        try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
    }
}

test "db graph metric runtime background reopened coordinators do not duplicate pagerank publish" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "runtime-publish-race-worker-a", "runtime-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .lease_owned = true,
                    .owner_id = "runtime-publish-race-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-publish-race-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runCoordinatorOnce(false);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-publish-race-coordinator-a",
                .lease_owned = true,
                .owner_id = "runtime-publish-race-coordinator-a",
                .planned_options = .{
                    .worker_id = "runtime-publish-race-coordinator-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const publish = try runtime.runCoordinatorOnce(false);
        try std.testing.expect(publish.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), publish.worker_steps);
        const publish_stats = runtime.stats();
        try std.testing.expect(publish_stats.lease_owned);
        try std.testing.expect(publish_stats.has_lease);
        try std.testing.expectEqual(Role.coordinator, publish_stats.role);

        {
            var live_duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer live_duplicate_coordinator.close();

            const duplicate_resources = live_duplicate_coordinator.core.asyncResources();
            var duplicate_runtime = try GraphMetricRuntime.init(
                alloc,
                duplicate_resources.store,
                duplicate_resources.index_manager,
                duplicate_resources.apply_mutex,
                live_duplicate_coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-publish-race-coordinator-b",
                    .lease_owned = true,
                    .owner_id = "runtime-publish-race-coordinator-b",
                    .planned_options = .{
                        .worker_id = "runtime-publish-race-coordinator-b",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer duplicate_runtime.deinit();

            const live_duplicate = try duplicate_runtime.runCoordinatorOnce(false);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.published);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.worker_steps);
            const live_duplicate_stats = duplicate_runtime.stats();
            try std.testing.expect(live_duplicate_stats.lease_owned);
            try std.testing.expect(!live_duplicate_stats.has_lease);
            try std.testing.expectEqual(@as(u64, 1), live_duplicate_stats.lease_acquire_failures);
            try std.testing.expectEqual(Role.coordinator, live_duplicate_stats.role);
        }

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const resources = duplicate_coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            duplicate_coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-publish-race-coordinator-b",
                .lease_owned = true,
                .owner_id = "runtime-publish-race-coordinator-b",
                .planned_options = .{
                    .worker_id = "runtime-publish-race-coordinator-b",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const duplicate = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), duplicate.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), duplicate.published);
        try std.testing.expectEqual(@as(usize, 0), duplicate.worker_steps);
        const duplicate_stats = runtime.stats();
        try std.testing.expect(duplicate_stats.lease_owned);
        try std.testing.expect(duplicate_stats.has_lease);
        try std.testing.expectEqual(Role.coordinator, duplicate_stats.role);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const resources = worker.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = "runtime-publish-race-cleaner",
                    .planned_options = .{
                        .worker_id = "runtime-publish-race-cleaner",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const cleanup = try runtime.runWorkerOnce("runtime-publish-race-cleaner");
            try std.testing.expectEqual(@as(usize, 0), cleanup.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), cleanup.published);
            const graph_entry = worker.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.phase == .complete) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }
}

test "db graph metric runtime background reopened coordinators do not duplicate eigenvector publish" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "eigenvector", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "runtime-eigenvector-publish-race-worker-a", "runtime-eigenvector-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .lease_owned = true,
                    .owner_id = "runtime-eigenvector-publish-race-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-eigenvector-publish-race-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runCoordinatorOnce(false);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-eigenvector-publish-race-coordinator-a",
                .lease_owned = true,
                .owner_id = "runtime-eigenvector-publish-race-coordinator-a",
                .planned_options = .{
                    .worker_id = "runtime-eigenvector-publish-race-coordinator-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const publish = try runtime.runCoordinatorOnce(false);
        try std.testing.expect(publish.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), publish.worker_steps);
        const publish_stats = runtime.stats();
        try std.testing.expect(publish_stats.lease_owned);
        try std.testing.expect(publish_stats.has_lease);
        try std.testing.expectEqual(Role.coordinator, publish_stats.role);

        {
            var live_duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer live_duplicate_coordinator.close();

            const duplicate_resources = live_duplicate_coordinator.core.asyncResources();
            var duplicate_runtime = try GraphMetricRuntime.init(
                alloc,
                duplicate_resources.store,
                duplicate_resources.index_manager,
                duplicate_resources.apply_mutex,
                live_duplicate_coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-eigenvector-publish-race-coordinator-b",
                    .lease_owned = true,
                    .owner_id = "runtime-eigenvector-publish-race-coordinator-b",
                    .planned_options = .{
                        .worker_id = "runtime-eigenvector-publish-race-coordinator-b",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer duplicate_runtime.deinit();

            const live_duplicate = try duplicate_runtime.runCoordinatorOnce(false);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.published);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.worker_steps);
            const live_duplicate_stats = duplicate_runtime.stats();
            try std.testing.expect(live_duplicate_stats.lease_owned);
            try std.testing.expect(!live_duplicate_stats.has_lease);
            try std.testing.expectEqual(@as(u64, 1), live_duplicate_stats.lease_acquire_failures);
            try std.testing.expectEqual(Role.coordinator, live_duplicate_stats.role);
        }

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const resources = duplicate_coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            duplicate_coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-eigenvector-publish-race-coordinator-b",
                .lease_owned = true,
                .owner_id = "runtime-eigenvector-publish-race-coordinator-b",
                .planned_options = .{
                    .worker_id = "runtime-eigenvector-publish-race-coordinator-b",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const duplicate = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), duplicate.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), duplicate.published);
        try std.testing.expectEqual(@as(usize, 0), duplicate.worker_steps);
        const duplicate_stats = runtime.stats();
        try std.testing.expect(duplicate_stats.lease_owned);
        try std.testing.expect(duplicate_stats.has_lease);
        try std.testing.expectEqual(Role.coordinator, duplicate_stats.role);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const resources = worker.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = "runtime-eigenvector-publish-race-cleaner",
                    .planned_options = .{
                        .worker_id = "runtime-eigenvector-publish-race-cleaner",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const cleanup = try runtime.runWorkerOnce("runtime-eigenvector-publish-race-cleaner");
            try std.testing.expectEqual(@as(usize, 0), cleanup.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), cleanup.published);
            const graph_entry = worker.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.phase == .complete) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }
}

test "db graph metric runtime background reopened coordinators do not duplicate hits publish" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:hub_a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:hub_b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:authority", .value = "{\"title\":\"authority\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "runtime-hits-publish-race-worker-a", "runtime-hits-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker_tick = blk: {
            var worker = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker.close();

            const resources = worker.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = workers[step_index % workers.len],
                    .planned_options = .{
                        .worker_id = workers[step_index % workers.len],
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runWorkerOnce(workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator_tick = blk: {
            var coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator.close();

            const resources = coordinator.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .lease_owned = true,
                    .owner_id = "runtime-hits-publish-race-coordinator",
                    .planned_options = .{
                        .worker_id = "runtime-hits-publish-race-coordinator",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();
            break :blk try runtime.runCoordinatorOnce(false);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker_tick.durableProgressed() and !coordinator_tick.durableProgressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const resources = coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-hits-publish-race-coordinator-a",
                .lease_owned = true,
                .owner_id = "runtime-hits-publish-race-coordinator-a",
                .planned_options = .{
                    .worker_id = "runtime-hits-publish-race-coordinator-a",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const publish = try runtime.runCoordinatorOnce(false);
        try std.testing.expect(publish.phases_advanced > 0);
        try std.testing.expectEqual(@as(usize, 0), publish.worker_steps);
        const publish_stats = runtime.stats();
        try std.testing.expect(publish_stats.lease_owned);
        try std.testing.expect(publish_stats.has_lease);
        try std.testing.expectEqual(Role.coordinator, publish_stats.role);

        {
            var live_duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer live_duplicate_coordinator.close();

            const duplicate_resources = live_duplicate_coordinator.core.asyncResources();
            var duplicate_runtime = try GraphMetricRuntime.init(
                alloc,
                duplicate_resources.store,
                duplicate_resources.index_manager,
                duplicate_resources.apply_mutex,
                live_duplicate_coordinator.backend_runtime,
                .{
                    .enabled = true,
                    .role = .coordinator,
                    .runtime_id = "runtime-hits-publish-race-coordinator-b",
                    .lease_owned = true,
                    .owner_id = "runtime-hits-publish-race-coordinator-b",
                    .planned_options = .{
                        .worker_id = "runtime-hits-publish-race-coordinator-b",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer duplicate_runtime.deinit();

            const live_duplicate = try duplicate_runtime.runCoordinatorOnce(false);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.published);
            try std.testing.expectEqual(@as(usize, 0), live_duplicate.worker_steps);
            const live_duplicate_stats = duplicate_runtime.stats();
            try std.testing.expect(live_duplicate_stats.lease_owned);
            try std.testing.expect(!live_duplicate_stats.has_lease);
            try std.testing.expectEqual(@as(u64, 1), live_duplicate_stats.lease_acquire_failures);
            try std.testing.expectEqual(Role.coordinator, live_duplicate_stats.role);
        }

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const resources = duplicate_coordinator.core.asyncResources();
        var runtime = try GraphMetricRuntime.init(
            alloc,
            resources.store,
            resources.index_manager,
            resources.apply_mutex,
            duplicate_coordinator.backend_runtime,
            .{
                .enabled = true,
                .role = .coordinator,
                .runtime_id = "runtime-hits-publish-race-coordinator-b",
                .lease_owned = true,
                .owner_id = "runtime-hits-publish-race-coordinator-b",
                .planned_options = .{
                    .worker_id = "runtime-hits-publish-race-coordinator-b",
                    .max_rounds = 1,
                    .max_metrics_per_round = 8,
                    .max_pages_per_round = 1,
                },
            },
        );
        defer runtime.deinit();

        const duplicate = try runtime.runCoordinatorOnce(false);
        try std.testing.expectEqual(@as(usize, 0), duplicate.phases_advanced);
        try std.testing.expectEqual(@as(usize, 0), duplicate.published);
        try std.testing.expectEqual(@as(usize, 0), duplicate.worker_steps);
        const duplicate_stats = runtime.stats();
        try std.testing.expect(duplicate_stats.lease_owned);
        try std.testing.expect(duplicate_stats.has_lease);
        try std.testing.expectEqual(Role.coordinator, duplicate_stats.role);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var worker = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const resources = worker.core.asyncResources();
            var runtime = try GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                worker.backend_runtime,
                .{
                    .enabled = true,
                    .role = .worker,
                    .lease_owned = true,
                    .owner_id = "runtime-hits-publish-race-cleaner",
                    .planned_options = .{
                        .worker_id = "runtime-hits-publish-race-cleaner",
                        .max_rounds = 1,
                        .max_metrics_per_round = 8,
                        .max_pages_per_round = 1,
                    },
                },
            );
            defer runtime.deinit();

            const cleanup = try runtime.runWorkerOnce("runtime-hits-publish-race-cleaner");
            try std.testing.expectEqual(@as(usize, 0), cleanup.phases_advanced);
            try std.testing.expectEqual(@as(usize, 0), cleanup.published);
            const graph_entry = worker.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var authority = try graph_entry.index.graphMetricStatus("hits_authority");
            defer authority.deinit(alloc);
            var hub = try graph_entry.index.graphMetricStatus("hits_hub");
            defer hub.deinit(alloc);
            if (authority.state == .fresh and authority.phase == .complete and hub.state == .fresh and hub.phase == .complete) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }
}

test "db graph metric runtime background cycles multiple worker ids across planned pages" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });

    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d:0>3}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const workers = [_][]const u8{ "runtime-worker-a", "runtime-worker-b" };
    const resources = db.core.asyncResources();
    var runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .planned_options = .{
                .worker_ids = &workers,
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 2,
            },
        },
    );
    defer runtime.deinit();

    const prepare_tick = try runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 1), prepare_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 2), prepare_tick.worker_steps);
    try std.testing.expectEqual(@as(usize, 1), prepare_tick.pages_completed);
    try std.testing.expect(prepare_tick.phases_advanced > 0);

    const scan_tick = try runtime.runOnceDetailed();
    try std.testing.expectEqual(@as(usize, 0), scan_tick.builds_started);
    try std.testing.expectEqual(@as(usize, 2), scan_tick.worker_steps);
    try std.testing.expectEqual(@as(usize, 2), scan_tick.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), scan_tick.phases_advanced);

    var steps: usize = 0;
    while (try runtime.runOnce()) {
        steps += 1;
        if (steps > 200) return error.TestUnexpectedResult;
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:hub", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 130.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime planned scheduler does not auto retry failed graph metric generation" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .planned,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const start = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 1), start.builds_started);
    try std.testing.expectEqual(@as(usize, 1), start.active_builds);

    var failed = try db.failGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", error.InvalidGraphMetricBuildManifest);
    defer failed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expect(failed.build_queued);
    try std.testing.expectEqualStrings("InvalidGraphMetricBuildManifest", failed.last_error);
    const failed_target_generation = failed.target_edge_generation;

    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, false, 0, 0, 0, 0);
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const duplicate = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 0), duplicate.builds_started);
    try std.testing.expectEqual(@as(usize, 0), duplicate.active_builds);
    try std.testing.expectEqual(@as(usize, 0), duplicate.coordinator_steps);
    try std.testing.expect(!duplicate.durableProgressed());

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, status.state);
        try std.testing.expectEqual(failed_target_generation, status.target_edge_generation);
        try std.testing.expectEqual(@as(u64, 0), status.build_job_id);
        var failed_events: usize = 0;
        for (status.recent_events) |event| {
            if (event.kind == .failed) failed_events += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), failed_events);
    }

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:e",
            .value = "{\"title\":\"epsilon\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);
    const retry_new_generation = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 1), retry_new_generation.builds_started);
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expect(status.building_generation > failed_target_generation);
    }
}

test "db graph metric runtime planned scheduler boundary completes degree by name" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "manual_degree", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "worker-a", "worker-b" };
    var step_index: usize = 0;
    var published = false;
    while (step_index < 1000) : (step_index += 1) {
        const now_ms: u64 = 1000 + @as(u64, @intCast(step_index)) * 2;
        const worker_step = try db.runGraphMetricPlannedWorkerPageStepAt("graph_idx", "manual_degree", workers[step_index % workers.len], now_ms);
        try std.testing.expect(!worker_step.advanced_phase);
        if (worker_step.completed_build) {
            try std.testing.expect(worker_step.phase == .complete or worker_step.phase == .cleanup_old_generations);
            published = true;
            break;
        }

        const coordinator_step = try db.runGraphMetricPlannedCoordinatorStepAt("graph_idx", "manual_degree", now_ms + 1);
        if (coordinator_step.completed_build) {
            published = true;
            break;
        }

        const progressed =
            worker_step.claimed_page or
            worker_step.completed_page or
            coordinator_step.advanced_phase;
        if (!progressed and worker_step.phase != .cleanup_old_generations) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(published);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 3,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime planned scheduler sweeps active degree work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "degree", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const start = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = false,
    });
    try std.testing.expectEqual(@as(usize, 1), start.metrics_scanned);
    try std.testing.expectEqual(@as(usize, 0), start.builds_started);
    try std.testing.expect(start.active_builds > 0);

    const workers = [_][]const u8{ "sweep-worker-a", "sweep-worker-b" };
    var finished = false;
    var saw_coordinator_publish = false;
    var step_index: usize = 0;
    while (step_index < 1000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        try std.testing.expectEqual(@as(usize, 0), worker.published);
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        if (coordinator.published > 0) saw_coordinator_publish = true;
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("degree");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_coordinator_publish);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 3,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime planned scheduler sweeps active pagerank work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":20,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "pagerank-sweep-a", "pagerank-sweep-b", "pagerank-sweep-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                try std.testing.expect(status.iterations_completed > 0);
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
}

test "db graph metric runtime planned single-vector failed planned rebuild preserves published public reads" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;
    try TestHelpers.verifyDbSingleVectorFailedPlannedRebuildPreservesPublishedPublicReads(DB, alloc, "pagerank", "pagerank");
    try TestHelpers.verifyDbSingleVectorFailedPlannedRebuildPreservesPublishedPublicReads(DB, alloc, "eigenvector", "eigenvector");
}

test "db graph metric runtime planned paired hits failed planned rebuild preserves published public reads" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "hits_authority");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);
    const published_generation = refreshed.published_generation;
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_status.state);
        try std.testing.expectEqual(published_generation, hub_status.published_generation);
    }

    var initial = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer initial.deinit();
    try std.testing.expectEqual(@as(usize, 2), initial.graph_metric_results.len);
    try std.testing.expectEqualStrings("doc:authority", initial.graph_metric_results[0].scores[0].node);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[1].status.state);
    try std.testing.expectEqual(published_generation, initial.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(published_generation, initial.graph_metric_results[1].status.published_generation);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:new-hub", .value = "{\"title\":\"new hub\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const rebuilding_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        try std.testing.expect(target_generation > published_generation);
        var building = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
        defer building.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
        try std.testing.expectEqual(target_generation, building.building_generation);
        break :blk target_generation;
    };

    var failed = try db.failGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", error.InvalidGraphMetricScore);
    defer failed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
    try std.testing.expectEqual(published_generation, failed.published_generation);
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, authority_status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, hub_status.state);
        try std.testing.expectEqual(published_generation, authority_status.published_generation);
        try std.testing.expectEqual(published_generation, hub_status.published_generation);
        try std.testing.expectEqual(rebuilding_generation, authority_status.target_edge_generation);
        try std.testing.expectEqual(rebuilding_generation, hub_status.target_edge_generation);
    }

    var published_after_failure = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 4,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 4,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    });
    defer published_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 2), published_after_failure.graph_metric_results.len);
    for (published_after_failure.graph_metric_results) |result| {
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, result.status.state);
        try std.testing.expectEqual(published_generation, result.status.published_generation);
        try std.testing.expect(result.scores.len > 0);
        for (result.scores) |score| {
            try std.testing.expect(!std.mem.eql(u8, score.node, "doc:new-hub"));
        }
    }
    try std.testing.expectEqualStrings("doc:authority", published_after_failure.graph_metric_results[0].scores[0].node);

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .published },
        .{ .name = "hits_hub", .freshness = .published },
    };
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:hub-a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), traversal_after_failure.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 2), traversal_after_failure.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("hits_authority", traversal_after_failure.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(traversal_after_failure.graph_results[0].nodes[0].metrics[0].score != null);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), traversal_after_failure.graph_results[0].nodes[0].metrics[0].score.?, 0.001);
    try std.testing.expectEqual(@as(usize, 2), traversal_after_failure.graph_results[0].metric_status.len);
    for (traversal_after_failure.graph_results[0].metric_status) |status| {
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, status.state);
        try std.testing.expectEqual(published_generation, status.published_generation);
    }

    const published_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .published,
    }};
    var published_order_query = published_graph_query;
    published_order_query.order_by = &published_metric_orders;
    var traversal_order_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_order_query }},
        .limit = 0,
    });
    defer traversal_order_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_order_after_failure.graph_results.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_order_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, traversal_order_after_failure.graph_results[0].metric_status[0].state);

    const published_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.5,
        .freshness = .published,
    }};
    var published_filter_query = published_graph_query;
    published_filter_query.where_metric = &published_metric_filters;
    var traversal_filter_after_failure = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_filter_query }},
        .limit = 0,
    });
    defer traversal_filter_after_failure.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal_filter_after_failure.graph_results.len);
    try std.testing.expectEqualStrings("doc:authority", traversal_filter_after_failure.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, traversal_filter_after_failure.graph_results[0].metric_status[0].state);

    var rerank_after_failure = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank_after_failure.deinit();
    try std.testing.expectEqual(@as(u32, 4), rerank_after_failure.total_hits);
    const rerank_status = rerank_after_failure.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, rerank_status.state);
    try std.testing.expectEqual(published_generation, rerank_status.published_generation);
    var saw_authority_score = false;
    var saw_new_hub_missing_score = false;
    for (rerank_after_failure.hits) |hit| {
        const details = hit.score_details orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(published_generation, details.published_generation);
        if (std.mem.eql(u8, hit.id, "doc:authority")) {
            saw_authority_score = details.metric_score != null;
        } else if (std.mem.eql(u8, hit.id, "doc:new-hub")) {
            saw_new_hub_missing_score = details.metric_score == null;
        }
    }
    try std.testing.expect(saw_authority_score);
    try std.testing.expect(saw_new_hub_missing_score);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "authority",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_authority",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "hub",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_hub",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .fresh },
        .{ .name = "hits_hub", .freshness = .fresh },
    };
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_order_query = published_graph_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.5,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_graph_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

test "db graph metric runtime planned scheduler sweeps pagerank across reopened handles" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":20,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);

        const initial_tick = try coordinator.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        try std.testing.expect(initial_tick.active_builds > 0);
    }

    const workers = [_][]const u8{ "reopened-pagerank-a", "reopened-pagerank-b", "reopened-pagerank-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerSweep(.{
                .worker_id = workers[step_index % workers.len],
                .max_pages = 1,
            });
        };

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            const sweep = try coordinator_db.runGraphMetricPlannedCoordinatorSweep(.{
                .max_metrics = 8,
                .start_background_builds = false,
            });
            {
                const graph_entry = coordinator_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
                var status = try graph_entry.index.graphMetricStatus("pagerank");
                defer status.deinit(alloc);
                if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                    try std.testing.expectEqual(target_generation, status.published_generation);
                    try std.testing.expect(status.iterations_completed > 0);
                    finished = true;
                }
            }
            break :blk sweep;
        };
        if (finished) break;
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
        try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
        try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
    }
}

test "db graph metric runtime planned scheduler reopened coordinators do not duplicate pagerank publish" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "db-publish-race-worker-a", "db-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "pagerank", workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            break :blk try coordinator_db.runGraphMetricPlannedCoordinatorStep("graph_idx", "pagerank");
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker.claimed_page and !worker.completed_page and !coordinator.advanced_phase) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const publish = try coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "pagerank");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.publish_generation, publish.phase);
        try std.testing.expect(publish.advanced_phase);

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const duplicate = try duplicate_coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "pagerank");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, duplicate.phase);
        try std.testing.expect(!duplicate.advanced_phase);
        try std.testing.expect(!duplicate.published);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker_db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker_db.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const cleanup = try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "pagerank", "db-publish-race-cleaner");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, cleanup.phase);
            try std.testing.expect(!cleanup.advanced_phase);
            if (cleanup.published) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric runtime planned scheduler reopened coordinators do not duplicate eigenvector publish" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "eigenvector", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "db-eigenvector-publish-race-worker-a", "db-eigenvector-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "eigenvector", workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            break :blk try coordinator_db.runGraphMetricPlannedCoordinatorStep("graph_idx", "eigenvector");
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker.claimed_page and !worker.completed_page and !coordinator.advanced_phase) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const publish = try coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "eigenvector");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.publish_generation, publish.phase);
        try std.testing.expect(publish.advanced_phase);

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const duplicate = try duplicate_coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "eigenvector");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, duplicate.phase);
        try std.testing.expect(!duplicate.advanced_phase);
        try std.testing.expect(!duplicate.published);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);
    }

    {
        var worker_db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker_db.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const cleanup = try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "eigenvector", "db-eigenvector-publish-race-cleaner");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, cleanup.phase);
            try std.testing.expect(!cleanup.advanced_phase);
            if (cleanup.published) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
        try std.testing.expectEqual(target_generation, status.published_generation);
        try std.testing.expectEqual(@as(usize, 1), status.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, status.recent_events[0].kind);

        var published_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "eigenvector",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer published_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric runtime planned scheduler reopened coordinators do not duplicate hits publish" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var target_generation: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "graph_idx",
            .kind = .graph,
            .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:hub_a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:hub_b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
                .{ .key = "doc:authority", .value = "{\"title\":\"authority\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            },
            .sync_level = .write,
        });
        try db.runUntilIdle();

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        var started = try coordinator.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
        defer started.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
        try std.testing.expectEqual(target_generation, started.building_generation);
    }

    const workers = [_][]const u8{ "db-hits-publish-race-worker-a", "db-hits-publish-race-worker-b" };
    var reached_publish = false;
    var step_index: usize = 0;
    while (step_index < 400) : (step_index += 1) {
        const worker = blk: {
            var worker_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer worker_db.close();
            break :blk try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "hits_authority", workers[step_index % workers.len]);
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        const coordinator = blk: {
            var coordinator_db = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer coordinator_db.close();
            break :blk try coordinator_db.runGraphMetricPlannedCoordinatorStep("graph_idx", "hits_authority");
        };

        {
            var reader = try DB.open(alloc, std.mem.span(path), .{
                .start_index_workers = false,
                .ttl_cleanup = .{ .enabled = false },
            });
            defer reader.close();
            const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer status.deinit(alloc);
            if (status.phase == .publish_generation) {
                reached_publish = true;
                break;
            }
        }

        if (!worker.claimed_page and !worker.completed_page and !coordinator.advanced_phase) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(reached_publish);

    {
        var coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer coordinator.close();

        const publish = try coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "hits_authority");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.publish_generation, publish.phase);
        try std.testing.expect(publish.advanced_phase);

        const graph_entry = coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var duplicate_coordinator = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer duplicate_coordinator.close();

        const duplicate = try duplicate_coordinator.runGraphMetricPlannedCoordinatorStep("graph_idx", "hits_authority");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, duplicate.phase);
        try std.testing.expect(!duplicate.advanced_phase);
        try std.testing.expect(!duplicate.published);

        const graph_entry = duplicate_coordinator.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);
    }

    {
        var worker_db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer worker_db.close();

        var cleanup_finished = false;
        for (0..12) |_| {
            const cleanup = try worker_db.runGraphMetricPlannedWorkerPageStep("graph_idx", "hits_authority", "db-hits-publish-race-cleaner");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations, cleanup.phase);
            try std.testing.expect(!cleanup.advanced_phase);
            if (cleanup.published) {
                cleanup_finished = true;
                break;
            }
        }
        try std.testing.expect(cleanup_finished);
    }

    {
        var reader = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();

        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub.state);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, authority.phase);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, hub.phase);
        try std.testing.expectEqual(target_generation, authority.published_generation);
        try std.testing.expectEqual(authority.published_generation, hub.published_generation);
        try std.testing.expectEqual(@as(usize, 1), authority.recent_events.len);
        try std.testing.expectEqual(@as(usize, 1), hub.recent_events.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, authority.recent_events[0].kind);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricEventKind.publish, hub.recent_events[0].kind);

        var authority_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer authority_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), authority_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, authority_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), authority_result.graph_metric_results[0].scores.len);

        var hub_result = try reader.search(alloc, .{
            .graph_metric_queries = &.{.{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        });
        defer hub_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), hub_result.graph_metric_results.len);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_result.graph_metric_results[0].status.state);
        try std.testing.expectEqual(target_generation, hub_result.graph_metric_results[0].status.published_generation);
        try std.testing.expectEqual(@as(usize, 2), hub_result.graph_metric_results[0].scores.len);
    }
}

test "db graph metric runtime planned maintenance drains background pagerank work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        break :blk graph_entry.index.edge_generation;
    };

    const maintenance = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-pagerank",
        .max_rounds = 200,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), maintenance.builds_started);
    try std.testing.expect(maintenance.pages_completed > 0);
    try std.testing.expect(maintenance.phases_advanced > 0);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[0].scores[0].score >= published_result.graph_metric_results[0].scores[1].score);
}

test "db graph metric runtime background drains pagerank through planned maintenance" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        break :blk graph_entry.index.edge_generation;
    };

    const resources = db.core.asyncResources();
    var runtime = try GraphMetricRuntime.init(
        alloc,
        resources.store,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .runtime_id = "runtime-pagerank-combined",
            .planned_options = .{
                .worker_id = "runtime-pagerank-worker",
                .max_rounds = 1,
                .max_metrics_per_round = 8,
                .max_pages_per_round = 1,
            },
        },
    );
    defer runtime.deinit();

    {
        const initial_stats = runtime.stats();
        try std.testing.expect(initial_stats.enabled);
        try std.testing.expectEqual(Role.combined, initial_stats.role);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-combined"), initial_stats.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "local"), initial_stats.owner_id_hash);
        try std.testing.expect(initial_stats.lease_key_hash != 0);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-worker"), initial_stats.worker_id_hash);
        try std.testing.expectEqual(@as(usize, 1), initial_stats.worker_count);
        try std.testing.expectEqual(@as(u64, 0), initial_stats.ticks_started);
        try std.testing.expectEqual(@as(u64, 0), initial_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, 0), initial_stats.durable_progress_ticks);
        try std.testing.expectEqual(@as(?[]const u8, null), initial_stats.last_error_name);
    }

    var steps: usize = 0;
    while (try runtime.runOnce()) {
        steps += 1;
        if (steps > 200) return error.TestUnexpectedResult;
    }
    try std.testing.expect(steps > 0);

    {
        const drained_stats = runtime.stats();
        try std.testing.expectEqual(@as(u64, @intCast(steps + 1)), drained_stats.ticks_started);
        try std.testing.expectEqual(drained_stats.ticks_started, drained_stats.ticks_completed);
        try std.testing.expectEqual(@as(u64, @intCast(steps)), drained_stats.durable_progress_ticks);
        try std.testing.expectEqual(@as(u64, 1), drained_stats.idle_ticks);
        try std.testing.expectEqual(@as(u64, 0), drained_stats.error_ticks);
        try std.testing.expectEqual(@as(?[]const u8, null), drained_stats.last_error_name);
        try std.testing.expect(!drained_stats.last_result.durableProgressed());
    }

    db.graph_metric_runtime = &runtime;
    defer db.graph_metric_runtime = null;
    {
        const mapped_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, mapped_stats);
        try std.testing.expect(mapped_stats.graph_metric_runtime.enabled);
        try std.testing.expectEqual(types.GraphMetricRuntimeRole.combined, mapped_stats.graph_metric_runtime.role.?);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-combined"), mapped_stats.graph_metric_runtime.runtime_id_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "local"), mapped_stats.graph_metric_runtime.owner_id_hash);
        try std.testing.expectEqual(runtime.stats().lease_key_hash, mapped_stats.graph_metric_runtime.lease_key_hash);
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, "runtime-pagerank-worker"), mapped_stats.graph_metric_runtime.worker_id_hash);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.worker_count);
        try std.testing.expectEqual(@as(u64, @intCast(steps + 1)), mapped_stats.graph_metric_runtime.ticks_started);
        try std.testing.expectEqual(mapped_stats.graph_metric_runtime.ticks_started, mapped_stats.graph_metric_runtime.ticks_completed);
        try std.testing.expectEqual(@as(u64, @intCast(steps)), mapped_stats.graph_metric_runtime.durable_progress_ticks);
        try std.testing.expectEqual(@as(u64, 1), mapped_stats.graph_metric_runtime.idle_ticks);
        try std.testing.expectEqual(@as(u64, 0), mapped_stats.graph_metric_runtime.error_ticks);
        try std.testing.expectEqual(@as(u64, 0), mapped_stats.graph_metric_runtime.last_builds_started);
        try std.testing.expect(!mapped_stats.graph_metric_runtime.last_budget_exhausted);
    }

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
    }

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime planned maintenance reports budget exhaustion and resumes" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-budgeted",
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expect(status.state != .fresh or status.phase != .complete);
    }
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expect(pending.active_builds > 0);
    }

    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 200) : (tick_index += 1) {
        const tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_id = "planned-maintenance-budgeted",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expect(status.iterations_completed > 0);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(finished);

    const no_work = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-budgeted",
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(!no_work.budget_exhausted);
    try std.testing.expect(!no_work.durableProgressed());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:d", published_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime planned pagerank production budget matches local oracle" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank_local\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"pagerank_planned\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:src-{d:0>3}", .{i});
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    const hub_key = try alloc.dupe(u8, "doc:hub");
    errdefer alloc.free(hub_key);
    const hub_value = try alloc.dupe(u8, "{\"title\":\"hub\"}");
    errdefer alloc.free(hub_value);
    try writes.append(alloc, .{ .key = hub_key, .value = hub_value });

    try db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var local_refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "pagerank_local");
    defer local_refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, local_refreshed.state);
    try std.testing.expectEqual(target_generation, local_refreshed.published_generation);

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    try std.testing.expect(first_tick.rounds_executed <= 1);

    var total = first_tick;
    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 400) : (tick_index += 1) {
        const tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        total.add(tick);
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank_planned");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(total.rounds_executed > 1);
    try std.testing.expect(total.pages_completed > 1);
    try std.testing.expect(total.phases_advanced > 0);
    try std.testing.expect(total.published > 0);

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var local_status = try graph_entry.index.graphMetricStatus("pagerank_local");
    defer local_status.deinit(alloc);
    var planned_status = try graph_entry.index.graphMetricStatus("pagerank_planned");
    defer planned_status.deinit(alloc);
    try std.testing.expectEqual(local_status.published_generation, planned_status.published_generation);
    try std.testing.expectEqual(local_status.iterations_completed, planned_status.iterations_completed);
    try std.testing.expectEqual(local_status.converged, planned_status.converged);
    try std.testing.expectApproxEqAbs(local_status.delta, planned_status.delta, 0.0000001);

    const top_limit: usize = 32;
    const local_top = try graph_entry.index.graphMetricTopK("pagerank_local", top_limit);
    defer {
        for (local_top) |*score| score.deinit(alloc);
        alloc.free(local_top);
    }
    const planned_top = try graph_entry.index.graphMetricTopK("pagerank_planned", top_limit);
    defer {
        for (planned_top) |*score| score.deinit(alloc);
        alloc.free(planned_top);
    }
    try std.testing.expectEqual(local_top.len, planned_top.len);
    try std.testing.expectEqualStrings("doc:hub", planned_top[0].node);
    for (local_top, planned_top) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }
}

test "db graph metric runtime planned eigenvector production budget matches local oracle" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector_local\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"eigenvector_planned\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:src-{d:0>3}", .{i});
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    const hub_key = try alloc.dupe(u8, "doc:hub");
    errdefer alloc.free(hub_key);
    const hub_value = try alloc.dupe(u8, "{\"title\":\"hub\"}");
    errdefer alloc.free(hub_value);
    try writes.append(alloc, .{ .key = hub_key, .value = hub_value });

    try db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var local_refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "eigenvector_local");
    defer local_refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, local_refreshed.state);
    try std.testing.expectEqual(target_generation, local_refreshed.published_generation);

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    try std.testing.expect(first_tick.rounds_executed <= 1);

    var total = first_tick;
    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 600) : (tick_index += 1) {
        const tick = try db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        total.add(tick);
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector_planned");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.phase == .complete) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(total.rounds_executed > 1);
    try std.testing.expect(total.pages_completed > 1);
    try std.testing.expect(total.phases_advanced > 0);
    try std.testing.expect(total.published > 0);

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var local_status = try graph_entry.index.graphMetricStatus("eigenvector_local");
    defer local_status.deinit(alloc);
    var planned_status = try graph_entry.index.graphMetricStatus("eigenvector_planned");
    defer planned_status.deinit(alloc);
    try std.testing.expectEqual(local_status.published_generation, planned_status.published_generation);
    try std.testing.expectEqual(local_status.iterations_completed, planned_status.iterations_completed);
    try std.testing.expectEqual(local_status.converged, planned_status.converged);
    try std.testing.expectApproxEqAbs(local_status.delta, planned_status.delta, 0.0000001);

    const top_limit: usize = 32;
    const local_top = try graph_entry.index.graphMetricTopK("eigenvector_local", top_limit);
    defer {
        for (local_top) |*score| score.deinit(alloc);
        alloc.free(local_top);
    }
    const planned_top = try graph_entry.index.graphMetricTopK("eigenvector_planned", top_limit);
    defer {
        for (planned_top) |*score| score.deinit(alloc);
        alloc.free(planned_top);
    }
    try std.testing.expectEqual(local_top.len, planned_top.len);
    for (local_top, planned_top) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expect(std.math.isFinite(local.score));
        try std.testing.expect(std.math.isFinite(planned.score));
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }
}

test "db graph metric runtime planned hits production budget matches local oracle" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var local_path_buf: [256]u8 = undefined;
    const local_path = TestHelpers.tempPath(&local_path_buf);
    defer TestHelpers.cleanupTempDir(local_path);
    var planned_path_buf: [256]u8 = undefined;
    const planned_path = TestHelpers.tempPath(&planned_path_buf);
    defer TestHelpers.cleanupTempDir(planned_path);

    var local_db = try DB.open(alloc, std.mem.span(local_path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer local_db.close();
    var planned_db = try DB.open(alloc, std.mem.span(planned_path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer planned_db.close();

    const config_json =
        "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}";
    try local_db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = config_json,
    });
    try planned_db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = config_json,
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    for (0..130) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:hub-{d:0>3}", .{i});
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"hub {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:authority\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    const authority_key = try alloc.dupe(u8, "doc:authority");
    errdefer alloc.free(authority_key);
    const authority_value = try alloc.dupe(u8, "{\"title\":\"authority\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}");
    errdefer alloc.free(authority_value);
    try writes.append(alloc, .{ .key = authority_key, .value = authority_value });

    try local_db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try planned_db.batch(.{
        .writes = writes.items,
        .sync_level = .write,
    });
    try local_db.runDerivedUntil(local_db.core.nextDerivedSequence());
    try planned_db.runDerivedUntil(planned_db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = planned_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };

    var local_authority_refreshed = try local_db.refreshGraphMetric(alloc, "graph_idx", "hits_authority");
    defer local_authority_refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, local_authority_refreshed.state);
    try std.testing.expectEqual(target_generation, local_authority_refreshed.published_generation);

    {
        const pending = planned_db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    const first_tick = try planned_db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(first_tick.budget_exhausted);
    try std.testing.expect(first_tick.durableProgressed());
    try std.testing.expect(first_tick.rounds_executed <= 1);

    var total = first_tick;
    var finished = false;
    var saw_budget_exhausted = first_tick.budget_exhausted;
    var tick_index: usize = 0;
    while (tick_index < 800) : (tick_index += 1) {
        const tick = try planned_db.runGraphMetricPlannedMaintenanceForIdle(.{
            .worker_ids = &.{ "budget-worker-a", "budget-worker-b" },
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        });
        total.add(tick);
        saw_budget_exhausted = saw_budget_exhausted or tick.budget_exhausted;
        const graph_entry = planned_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        if (authority_status.state == .fresh and authority_status.phase == .complete and hub_status.state == .fresh and hub_status.phase == .complete) {
            try std.testing.expectEqual(target_generation, authority_status.published_generation);
            try std.testing.expectEqual(authority_status.published_generation, hub_status.published_generation);
            finished = true;
            break;
        }
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
    }
    try std.testing.expect(finished);
    try std.testing.expect(saw_budget_exhausted);
    try std.testing.expect(total.rounds_executed > 1);
    try std.testing.expect(total.pages_completed > 1);
    try std.testing.expect(total.phases_advanced > 0);
    try std.testing.expect(total.published > 0);

    const local_graph = local_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    const planned_graph = planned_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var local_authority_status = try local_graph.index.graphMetricStatus("hits_authority");
    defer local_authority_status.deinit(alloc);
    var local_hub_status = try local_graph.index.graphMetricStatus("hits_hub");
    defer local_hub_status.deinit(alloc);
    var planned_authority_status = try planned_graph.index.graphMetricStatus("hits_authority");
    defer planned_authority_status.deinit(alloc);
    var planned_hub_status = try planned_graph.index.graphMetricStatus("hits_hub");
    defer planned_hub_status.deinit(alloc);
    try std.testing.expectEqual(local_authority_status.published_generation, planned_authority_status.published_generation);
    try std.testing.expectEqual(local_hub_status.published_generation, planned_hub_status.published_generation);
    try std.testing.expectEqual(planned_authority_status.published_generation, planned_hub_status.published_generation);
    try std.testing.expectEqual(local_authority_status.iterations_completed, planned_authority_status.iterations_completed);
    try std.testing.expectEqual(local_hub_status.iterations_completed, planned_hub_status.iterations_completed);
    try std.testing.expectEqual(local_authority_status.converged, planned_authority_status.converged);
    try std.testing.expectEqual(local_hub_status.converged, planned_hub_status.converged);
    try std.testing.expectApproxEqAbs(local_authority_status.delta, planned_authority_status.delta, 0.0000001);
    try std.testing.expectApproxEqAbs(local_hub_status.delta, planned_hub_status.delta, 0.0000001);

    const local_authorities = try local_graph.index.graphMetricTopK("hits_authority", 32);
    defer {
        for (local_authorities) |*score| score.deinit(alloc);
        alloc.free(local_authorities);
    }
    const planned_authorities = try planned_graph.index.graphMetricTopK("hits_authority", 32);
    defer {
        for (planned_authorities) |*score| score.deinit(alloc);
        alloc.free(planned_authorities);
    }
    try std.testing.expectEqual(local_authorities.len, planned_authorities.len);
    try std.testing.expectEqualStrings("doc:authority", planned_authorities[0].node);
    for (local_authorities, planned_authorities) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expect(std.math.isFinite(local.score));
        try std.testing.expect(std.math.isFinite(planned.score));
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }

    const local_hubs = try local_graph.index.graphMetricTopK("hits_hub", 32);
    defer {
        for (local_hubs) |*score| score.deinit(alloc);
        alloc.free(local_hubs);
    }
    const planned_hubs = try planned_graph.index.graphMetricTopK("hits_hub", 32);
    defer {
        for (planned_hubs) |*score| score.deinit(alloc);
        alloc.free(planned_hubs);
    }
    try std.testing.expectEqual(local_hubs.len, planned_hubs.len);
    for (local_hubs, planned_hubs) |local, planned| {
        try std.testing.expectEqualStrings(local.node, planned.node);
        try std.testing.expect(std.math.isFinite(local.score));
        try std.testing.expect(std.math.isFinite(planned.score));
        try std.testing.expectApproxEqAbs(local.score, planned.score, 0.0000001);
    }
}

test "db graph metric runtime planned maintenance drains background centrality family work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const metric_names = [_][]const u8{ "degree", "eigenvector", "hits_authority", "hits_hub" };
        for (metric_names) |metric_name| {
            var status = try graph_entry.index.graphMetricStatus(metric_name);
            defer status.deinit(alloc);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        }
        break :blk graph_entry.index.edge_generation;
    };

    const maintenance = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "planned-maintenance-centrality",
        .max_rounds = 400,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expectEqual(@as(usize, 3), maintenance.builds_started);
    try std.testing.expect(maintenance.pages_completed > 0);
    try std.testing.expect(maintenance.phases_advanced > 0);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const metric_names = [_][]const u8{ "degree", "eigenvector", "hits_authority", "hits_hub" };
        var authority_generation: u64 = 0;
        for (metric_names) |metric_name| {
            var status = try graph_entry.index.graphMetricStatus(metric_name);
            defer status.deinit(alloc);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.complete, status.phase);
            try std.testing.expect(status.iterations_completed > 0);
            if (std.mem.eql(u8, metric_name, "hits_authority")) {
                authority_generation = status.published_generation;
            } else if (std.mem.eql(u8, metric_name, "hits_hub")) {
                try std.testing.expectEqual(authority_generation, status.published_generation);
            }
        }
    }

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results.len);
    try std.testing.expectEqualStrings("doc:authority", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectEqualStrings("doc:authority", published_result.graph_metric_results[1].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[2].scores[0].score >= published_result.graph_metric_results[2].scores[1].score);
}

test "db graph metric runtime planned scheduler sweeps active eigenvector work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":20,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "eigenvector", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "eigenvector-sweep-a", "eigenvector-sweep-b", "eigenvector-sweep-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var status = try graph_entry.index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.state == .fresh and status.published_generation != 0 and status.phase == .complete) {
                try std.testing.expect(status.iterations_completed > 0);
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 3,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expect(published_result.graph_metric_results[0].scores[0].score > published_result.graph_metric_results[0].scores[1].score);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime planned scheduler sweeps active hits work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);

    const workers = [_][]const u8{ "hits-sweep-a", "hits-sweep-b", "hits-sweep-c" };
    var finished = false;
    var step_index: usize = 0;
    while (step_index < 2000) : (step_index += 1) {
        const worker = try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = workers[step_index % workers.len],
            .max_pages = 1,
        });
        const coordinator = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = 8,
            .start_background_builds = false,
        });
        {
            const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
            var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
            defer authority_status.deinit(alloc);
            var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
            defer hub_status.deinit(alloc);
            if (authority_status.state == .fresh and authority_status.published_generation != 0 and authority_status.phase == .complete) {
                try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_status.state);
                try std.testing.expectEqual(authority_status.published_generation, hub_status.published_generation);
                try std.testing.expectEqual(authority_status.iterations_completed, hub_status.iterations_completed);
                try std.testing.expect(authority_status.iterations_completed > 0);
                finished = true;
                break;
            }
        }
        if (!worker.progressed() and !coordinator.progressed()) {
            return error.GraphMetricBuildNoEligiblePage;
        }
    }
    try std.testing.expect(finished);

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, published_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(published_result.graph_metric_results[0].status.published_generation, published_result.graph_metric_results[1].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(@as(usize, 3), published_result.graph_metric_results[1].scores.len);
    try std.testing.expectEqualStrings("doc:authority", published_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), published_result.graph_metric_results[0].scores[0].score, 0.001);
    try std.testing.expect(published_result.graph_metric_results[1].scores[0].score >= published_result.graph_metric_results[1].scores[1].score);
    try std.testing.expect(published_result.graph_metric_results[1].scores[1].score > published_result.graph_metric_results[1].scores[2].score);
}

test "db graph metric runtime query public reads fail not ready before first publish" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .full_index,
    });
    try db.runUntilIdle();

    try std.testing.expectError(error.MetricNotReady, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    }));

    try std.testing.expectError(error.MetricNotReady, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 10,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    try std.testing.expectError(error.MetricNotReady, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 2,
        .include_stored = false,
    }));

    try std.testing.expectError(error.MetricNotReady, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 2,
        .include_stored = false,
    }));
}

test "db graph metric runtime query freshness distinguishes published stale scores from fresh requirement" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(refreshed.published_generation, published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), published_result.graph_metric_results[0].scores.len);
    for (published_result.graph_metric_results[0].scores) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc:c"));
        try std.testing.expectApproxEqAbs(@as(f64, 1.0), score.score, 0.001);
    }

    const active_target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        var building = try graph_entry.index.ensureGraphMetricPlannedBuild("manual_degree", target_generation);
        defer building.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
        try std.testing.expectEqual(target_generation, building.building_generation);

        const prepare = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("manual_degree", "worker-prepare");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, prepare.phase);
        try std.testing.expect(prepare.claimed_page);
        try std.testing.expect(prepare.completed_page);

        const advance_prepare = try graph_entry.index.runGraphMetricPlannedCoordinatorStepForMetric("manual_degree");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, advance_prepare.phase);
        try std.testing.expect(advance_prepare.advanced_phase);

        const scan = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("manual_degree", "worker-scan");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, scan.phase);
        try std.testing.expect(scan.claimed_page);
        try std.testing.expect(scan.completed_page);
        break :blk target_generation;
    };

    var building_published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer building_published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), building_published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building_published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(refreshed.published_generation, building_published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(active_target_generation, building_published_result.graph_metric_results[0].status.building_generation);
    try std.testing.expectEqual(@as(usize, 2), building_published_result.graph_metric_results[0].scores.len);
    for (building_published_result.graph_metric_results[0].scores) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc:c"));
        try std.testing.expectApproxEqAbs(@as(f64, 1.0), score.score, 0.001);
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var failed = try graph_entry.index.failGraphMetricPlannedBuild("manual_degree", error.InvalidGraphMetricScore);
        defer failed.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
        try std.testing.expectEqual(refreshed.published_generation, failed.published_generation);
    }

    var failed_published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer failed_published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), failed_published_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed_published_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(refreshed.published_generation, failed_published_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), failed_published_result.graph_metric_results[0].scores.len);
    for (failed_published_result.graph_metric_results[0].scores) |score| {
        try std.testing.expect(!std.mem.eql(u8, score.node, "doc:c"));
        try std.testing.expectApproxEqAbs(@as(f64, 1.0), score.score, 0.001);
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));
}

test "db graph metric runtime query rerank applies published metric scores to search hits" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .full_index,
    });

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 3,
        .include_stored = false,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 3), result.total_hits);
    try std.testing.expectEqual(@as(usize, 3), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), result.hits[0].score orelse return error.TestUnexpectedResult, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), result.hits[1].score orelse return error.TestUnexpectedResult, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), result.hits[2].score orelse return error.TestUnexpectedResult, 0.001);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .full_index,
    });
    try db.runUntilIdle();

    const active_target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        var building = try graph_entry.index.ensureGraphMetricPlannedBuild("manual_degree", target_generation);
        defer building.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
        try std.testing.expectEqual(target_generation, building.building_generation);

        const prepare = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("manual_degree", "worker-prepare");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, prepare.phase);
        try std.testing.expect(prepare.claimed_page);
        try std.testing.expect(prepare.completed_page);

        const advance_prepare = try graph_entry.index.runGraphMetricPlannedCoordinatorStepForMetric("manual_degree");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, advance_prepare.phase);
        try std.testing.expect(advance_prepare.advanced_phase);

        const scan = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("manual_degree", "worker-scan");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, scan.phase);
        try std.testing.expect(scan.claimed_page);
        try std.testing.expect(scan.completed_page);

        var active_status = try graph_entry.index.graphMetricStatus("manual_degree");
        defer active_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, active_status.state);
        try std.testing.expectEqual(refreshed.published_generation, active_status.published_generation);
        try std.testing.expectEqual(target_generation, active_status.building_generation);
        break :blk target_generation;
    };

    var stale_ok = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer stale_ok.deinit();
    try std.testing.expectEqualStrings("doc:b", stale_ok.hits[0].id);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), stale_ok.hits[0].score orelse return error.TestUnexpectedResult, 0.001);
    const stale_ok_status = stale_ok.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, stale_ok_status.state);
    try std.testing.expectEqual(refreshed.published_generation, stale_ok_status.published_generation);
    try std.testing.expectEqual(active_target_generation, stale_ok_status.building_generation);

    var explicit_expression = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 2.0,
            .missing_score = -10.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer explicit_expression.deinit();
    try std.testing.expectEqualStrings("doc:b", explicit_expression.hits[0].id);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), explicit_expression.hits[0].score orelse return error.TestUnexpectedResult, 0.001);
    const top_details = explicit_expression.hits[0].score_details orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("graph_idx", top_details.index_name);
    try std.testing.expectEqualStrings("manual_degree", top_details.metric_name);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), top_details.base_score, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), top_details.base_weight, 0.001);
    try std.testing.expect(top_details.metric_score != null);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), top_details.metric_score.?, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), top_details.metric_score_used, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), top_details.metric_weight, 0.001);
    try std.testing.expect(!top_details.missing_score_used);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), top_details.final_score, 0.001);
    try std.testing.expectEqual(refreshed.published_generation, top_details.published_generation);
    try std.testing.expectEqualStrings("doc:d", explicit_expression.hits[3].id);
    try std.testing.expectApproxEqAbs(@as(f32, -20.0), explicit_expression.hits[3].score orelse return error.TestUnexpectedResult, 0.001);
    const missing_details = explicit_expression.hits[3].score_details orelse return error.TestUnexpectedResult;
    try std.testing.expect(missing_details.metric_score == null);
    try std.testing.expect(missing_details.missing_score_used);
    try std.testing.expectApproxEqAbs(@as(f64, -10.0), missing_details.metric_score_used, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, -20.0), missing_details.final_score, 0.001);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var failed = try graph_entry.index.failGraphMetricPlannedBuild("manual_degree", error.InvalidGraphMetricScore);
        defer failed.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
        try std.testing.expectEqual(refreshed.published_generation, failed.published_generation);
    }

    var failed_rerank = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer failed_rerank.deinit();
    try std.testing.expectEqualStrings("doc:b", failed_rerank.hits[0].id);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), failed_rerank.hits[0].score orelse return error.TestUnexpectedResult, 0.001);
    const failed_rerank_status = failed_rerank.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed_rerank_status.state);
    try std.testing.expectEqual(refreshed.published_generation, failed_rerank_status.published_generation);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "manual_degree",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

test "db graph metric runtime query not ready semantics distinguish projection from ranking" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "manual_degree",
        .freshness = .published,
    }};
    const base_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var projection_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = base_query }},
        .limit = 0,
    });
    defer projection_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), projection_result.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), projection_result.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", projection_result.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), projection_result.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("manual_degree", projection_result.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(projection_result.graph_results[0].nodes[0].metrics[0].score == null);
    try std.testing.expectEqual(@as(usize, 1), projection_result.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, projection_result.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(@as(u64, 0), projection_result.graph_results[0].metric_status[0].published_generation);

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "manual_degree",
        .freshness = .fresh,
    }};
    var fresh_projection_query = base_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricNotReady, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const published_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "manual_degree",
        .freshness = .published,
    }};
    var order_query = base_query;
    order_query.order_by = &published_metric_orders;
    try std.testing.expectError(error.MetricNotReady, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = order_query }},
        .limit = 0,
    }));

    const published_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "manual_degree",
        .op = .gte,
        .value = 0.5,
        .freshness = .published,
    }};
    var filter_query = base_query;
    filter_query.where_metric = &published_metric_filters;
    try std.testing.expectError(error.MetricNotReady, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = filter_query }},
        .limit = 0,
    }));
}

test "db graph metric runtime query freshness distinguishes stale projection from fresh ordering and filtering" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "manual_degree",
        .freshness = .published,
    }};
    const published_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var published_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_query }},
        .limit = 0,
    });
    defer published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", published_result.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("manual_degree", published_result.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(published_result.graph_results[0].nodes[0].metrics[0].score != null);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), published_result.graph_results[0].nodes[0].metrics[0].score.?, 0.001);
    try std.testing.expectEqual(@as(usize, 1), published_result.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, published_result.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(refreshed.published_generation, published_result.graph_results[0].metric_status[0].published_generation);

    const active_target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        var building = try graph_entry.index.ensureGraphMetricPlannedBuild("manual_degree", target_generation);
        defer building.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
        try std.testing.expectEqual(target_generation, building.building_generation);

        const prepare = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("manual_degree", "worker-prepare");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, prepare.phase);
        try std.testing.expect(prepare.claimed_page);
        try std.testing.expect(prepare.completed_page);

        const advance_prepare = try graph_entry.index.runGraphMetricPlannedCoordinatorStepForMetric("manual_degree");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, advance_prepare.phase);
        try std.testing.expect(advance_prepare.advanced_phase);

        const scan = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("manual_degree", "worker-scan");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, scan.phase);
        try std.testing.expect(scan.claimed_page);
        try std.testing.expect(scan.completed_page);
        break :blk target_generation;
    };

    var building_published_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_query }},
        .limit = 0,
    });
    defer building_published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), building_published_result.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), building_published_result.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", building_published_result.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), building_published_result.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("manual_degree", building_published_result.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(building_published_result.graph_results[0].nodes[0].metrics[0].score != null);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), building_published_result.graph_results[0].nodes[0].metrics[0].score.?, 0.001);
    try std.testing.expectEqual(@as(usize, 1), building_published_result.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building_published_result.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(refreshed.published_generation, building_published_result.graph_results[0].metric_status[0].published_generation);
    try std.testing.expectEqual(active_target_generation, building_published_result.graph_results[0].metric_status[0].building_generation);

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "manual_degree",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "manual_degree",
        .freshness = .fresh,
    }};
    var fresh_order_query = published_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "manual_degree",
        .op = .gte,
        .value = 0.5,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var failed = try graph_entry.index.failGraphMetricPlannedBuild("manual_degree", error.InvalidGraphMetricScore);
        defer failed.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed.state);
        try std.testing.expectEqual(refreshed.published_generation, failed.published_generation);
    }

    var failed_published_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_query }},
        .limit = 0,
    });
    defer failed_published_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), failed_published_result.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), failed_published_result.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", failed_published_result.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), failed_published_result.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("manual_degree", failed_published_result.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(failed_published_result.graph_results[0].nodes[0].metrics[0].score != null);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), failed_published_result.graph_results[0].nodes[0].metrics[0].score.?, 0.001);
    try std.testing.expectEqual(@as(usize, 1), failed_published_result.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.failed, failed_published_result.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(refreshed.published_generation, failed_published_result.graph_results[0].metric_status[0].published_generation);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));
}

test "db graph metric runtime degree canary gate tracks queued active and capped degree work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "degree-canary-worker",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectDegreeCanaryDecision(db.core.index_manager, .{}, true, 0, 1, 0, 0);
    const queued_decision = try db.core.index_manager.graphMetricDegreeCanaryDecision(.{});
    try std.testing.expectEqual(@as(usize, 0), queued_decision.control_records);
    try std.testing.expect(queued_decision.queued_degree_control_records > 0);
    const capped_queued_decision = try db.core.index_manager.graphMetricDegreeCanaryDecision(.{
        .max_control_records = queued_decision.queued_degree_control_records - 1,
    });
    try std.testing.expect(!capped_queued_decision.shouldRunPlanned());
    try std.testing.expectEqual(queued_decision.queued_degree_control_records, capped_queued_decision.queued_degree_control_records);

    const start = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 1), start.builds_started);
    try expectDegreeCanaryDecision(db.core.index_manager, .{}, true, 1, 0, 0, 0);

    const active_decision = try db.core.index_manager.graphMetricDegreeCanaryDecision(.{});
    try std.testing.expect(active_decision.control_records > 0);
    try std.testing.expect(active_decision.shouldRunPlanned());

    const capped_decision = try db.core.index_manager.graphMetricDegreeCanaryDecision(.{
        .max_control_records = active_decision.control_records - 1,
    });
    try std.testing.expect(!capped_decision.shouldRunPlanned());
    try std.testing.expectEqual(active_decision.control_records, capped_decision.control_records);

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();
    try expectDegreeCanaryDecision(db.core.index_manager, .{}, false, 0, 0, 0, 0);
}

test "db graph metric runtime degree canary gate blocks non degree queued work" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectDegreeCanaryDecision(db.core.index_manager, .{}, false, 0, 1, 0, 1);

    const degree_canary = try db.core.index_manager.shouldRunGraphMetricDegreeCanary(.{});
    try std.testing.expect(!degree_canary);
}

test "db graph metric runtime degree canary runUntilIdle uses planned maintenance for one degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .degree_canary,
        .graph_metric_idle_planned_options = .{
            .worker_id = "degree-canary-idle",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectDegreeCanaryDecision(db.core.index_manager, .{}, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:b", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime degree canary planned maintenance reports bounded rounds and resumes" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .degree_canary,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const decision = try db.core.index_manager.graphMetricDegreeCanaryDecision(db.graph_metric_idle_degree_canary_options);
    try std.testing.expect(decision.shouldRunPlanned());
    try std.testing.expectEqual(@as(usize, 1), decision.eligible_queued_degree);

    const budgeted = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "degree-canary-round-budget",
        .max_rounds = 1,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), budgeted.rounds_executed);
    try std.testing.expect(budgeted.budget_exhausted);
    try std.testing.expect(budgeted.durableProgressed());

    const resumed = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "degree-canary-round-budget",
        .max_rounds = 200,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 1,
    });
    try std.testing.expect(!resumed.budget_exhausted);
    try std.testing.expect(resumed.rounds_executed > 0);
    try std.testing.expect(resumed.rounds_executed <= 200);

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:b", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime degree canary runUntilIdle preserves published scores while rebuild is active" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .degree_canary,
        .graph_metric_idle_planned_options = .{
            .worker_id = "degree-canary-freshness",
            .max_rounds = 200,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.runUntilIdle();

    var initial = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer initial.deinit();
    try std.testing.expectEqual(@as(usize, 1), initial.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[0].status.state);
    const published_generation = initial.graph_metric_results[0].status.published_generation;
    try std.testing.expect(published_generation > 0);
    try std.testing.expectEqualStrings("doc:b", initial.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), initial.graph_metric_results[0].scores[0].score, 0.001);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:d",
            .value = "{\"title\":\"delta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    db.graph_metric_idle_planned_options.max_rounds = 1;
    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());

    var published = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published.deinit();
    try std.testing.expectEqual(@as(usize, 1), published.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, published.graph_metric_results[0].status.state);
    try std.testing.expectEqual(published_generation, published.graph_metric_results[0].status.published_generation);
    try std.testing.expect(published.graph_metric_results[0].status.building_generation > published_generation);
    try std.testing.expectEqualStrings("doc:b", published.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), published.graph_metric_results[0].scores[0].score, 0.001);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "degree",
        .freshness = .published,
    }};
    const traversal_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = traversal_query }},
        .limit = 0,
    });
    defer traversal.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", traversal.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("degree", traversal.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), traversal.graph_results[0].nodes[0].metrics[0].score orelse return error.TestUnexpectedResult, 0.001);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].metric_status.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, traversal.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(published_generation, traversal.graph_results[0].metric_status[0].published_generation);

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "degree",
        .freshness = .fresh,
    }};
    var fresh_traversal_query = traversal_query;
    fresh_traversal_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_traversal_query }},
        .limit = 0,
    }));

    var rerank = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "degree",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank.deinit();
    try std.testing.expectEqualStrings("doc:b", rerank.hits[0].id);
    const rerank_status = rerank.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, rerank_status.state);
    try std.testing.expectEqual(published_generation, rerank_status.published_generation);
    const rerank_details = rerank.hits[0].score_details orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("degree", rerank_details.metric_name);
    try std.testing.expectEqual(published_generation, rerank_details.published_generation);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rerank_details.metric_score orelse return error.TestUnexpectedResult, 0.001);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "degree",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

test "db graph metric runtime degree canary runUntilIdle fails fast when active planned work is outside guardrails" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .degree_canary,
        .graph_metric_idle_planned_options = .{
            .worker_id = "degree-canary-capped-active",
            .max_rounds = 200,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_degree_canary_options = .{
            .max_control_records = 0,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const start = try db.runGraphMetricPlannedCoordinatorSweep(.{
        .max_metrics = 8,
        .start_background_builds = true,
    });
    try std.testing.expectEqual(@as(usize, 1), start.builds_started);

    const decision = try db.core.index_manager.graphMetricDegreeCanaryDecision(db.graph_metric_idle_degree_canary_options);
    try std.testing.expectEqual(@as(usize, 1), decision.active_degree_builds);
    try std.testing.expect(decision.control_records > decision.max_control_records);
    try std.testing.expect(!decision.shouldRunPlanned());

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }
}

test "db graph metric runtime degree canary runUntilIdle falls back to local oracle for mixed metrics" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .degree_canary,
        .graph_metric_idle_planned_options = .{
            .worker_id = "degree-canary-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectDegreeCanaryDecision(db.core.index_manager, .{}, false, 0, 1, 0, 1);

    try db.runUntilIdle();

    var degree_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer degree_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), degree_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, degree_result.graph_metric_results[0].status.state);

    var pagerank_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer pagerank_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), pagerank_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, pagerank_result.graph_metric_results[0].status.state);
}

test "db graph metric runtime default gate runUntilIdle publishes configured graph pagerank metrics" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"max_iterations\":40,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"max_iterations\":20,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"max_iterations\":20,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"max_iterations\":20,\"tolerance\":0.000001,\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}],\"related\":[{\"target\":\"doc:x\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
            .{ .key = "doc:x", .value = "{\"title\":\"excluded\"}" },
        },
        .sync_level = .write,
    });

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
        var degree_status = try graph_entry.index.graphMetricStatus("degree");
        defer degree_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, degree_status.state);
        var eigenvector_status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer eigenvector_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, eigenvector_status.state);
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, authority_status.state);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, hub_status.state);
    }

    try db.runUntilIdle();

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expect(status.published_generation > 0);
        try std.testing.expect(status.converged or status.iterations_completed == 40);
        var degree_status = try graph_entry.index.graphMetricStatus("degree");
        defer degree_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, degree_status.state);
        try std.testing.expect(degree_status.converged);
        try std.testing.expectEqual(@as(u32, 1), degree_status.iterations_completed);
        var eigenvector_status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer eigenvector_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, eigenvector_status.state);
        try std.testing.expect(eigenvector_status.converged or eigenvector_status.iterations_completed == 20);
        var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority_status.state);
        try std.testing.expect(authority_status.converged or authority_status.iterations_completed == 20);
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_status.state);
        try std.testing.expect(hub_status.converged or hub_status.iterations_completed == 20);

        const top = try graph_entry.index.graphMetricTopK("pagerank", 10);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 4), top.len);
        for (top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const degree_top = try graph_entry.index.graphMetricTopK("degree", 10);
        defer {
            for (degree_top) |*score| score.deinit(alloc);
            alloc.free(degree_top);
        }
        try std.testing.expectEqual(@as(usize, 4), degree_top.len);
        try std.testing.expectEqualStrings("doc:b", degree_top[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 3.0), degree_top[0].score, 0.001);
        for (degree_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const eigenvector_top = try graph_entry.index.graphMetricTopK("eigenvector", 10);
        defer {
            for (eigenvector_top) |*score| score.deinit(alloc);
            alloc.free(eigenvector_top);
        }
        try std.testing.expectEqual(@as(usize, 4), eigenvector_top.len);
        for (eigenvector_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const authority_top = try graph_entry.index.graphMetricTopK("hits_authority", 10);
        defer {
            for (authority_top) |*score| score.deinit(alloc);
            alloc.free(authority_top);
        }
        try std.testing.expectEqual(@as(usize, 4), authority_top.len);
        for (authority_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));

        const hub_top = try graph_entry.index.graphMetricTopK("hits_hub", 10);
        defer {
            for (hub_top) |*score| score.deinit(alloc);
            alloc.free(hub_top);
        }
        try std.testing.expectEqual(@as(usize, 4), hub_top.len);
        for (hub_top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:x"));
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 0), metric_result.hits.len);
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);

    var degree_metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer degree_metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), degree_metric_result.graph_metric_results.len);
    try std.testing.expectEqual(@as(usize, 1), degree_metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", degree_metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), degree_metric_result.graph_metric_results[0].scores[0].score, 0.001);
}

test "db graph metric runtime operations manual refresh rebuild and delete operate on configured metric" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"manual_degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runUntilIdle();
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("manual_degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, status.state);
    }

    var refreshed = try db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed.state);
    try std.testing.expect(refreshed.published_generation > 0);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("manual_degree");
        defer status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, status.state);
    }

    var rebuilt = try db.rebuildGraphMetric(alloc, "graph_idx", "manual_degree");
    defer rebuilt.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, rebuilt.state);
    try std.testing.expect(rebuilt.published_generation >= refreshed.published_generation);

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    const top = try graph_entry.index.graphMetricTopK("manual_degree", 1);
    defer {
        for (top) |*score| score.deinit(alloc);
        alloc.free(top);
    }
    try std.testing.expectEqual(@as(usize, 1), top.len);
    try std.testing.expectEqualStrings("doc:b", top[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), top[0].score, 0.001);

    var deleted = try db.deleteGraphMetricMaterialization(alloc, "graph_idx", "manual_degree");
    defer deleted.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.not_ready, deleted.state);
    try std.testing.expectEqual(@as(u64, 0), deleted.published_generation);
    try std.testing.expect(!deleted.maintenance_paused);

    try std.testing.expectError(error.MetricNotReady, graph_entry.index.graphMetricTopK("manual_degree", 1));

    var refreshed_after_delete = try db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer refreshed_after_delete.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed_after_delete.state);
}

test "db graph metric runtime operations pause and resume controls background maintenance" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try db.runUntilIdle();

    var initial = try db.refreshGraphMetric(alloc, "graph_idx", "degree");
    defer initial.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, initial.state);
    try std.testing.expect(!initial.maintenance_paused);
    const first_generation = initial.published_generation;
    try std.testing.expect(first_generation > 0);

    var paused = try db.pauseGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer paused.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, paused.state);
    try std.testing.expect(paused.maintenance_paused);
    try std.testing.expectEqual(first_generation, paused.published_generation);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expectEqual(@as(usize, 1), pending.metrics_scanned);
        try std.testing.expectEqual(@as(usize, 1), pending.paused_metrics);
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_pages);
        try std.testing.expect(!pending.hasWork());
    }

    const planned_while_paused = try db.runGraphMetricPlannedMaintenanceForIdle(.{
        .worker_id = "paused-degree-worker",
        .max_rounds = 4,
        .max_metrics_per_round = 8,
        .max_pages_per_round = 4,
    });
    try std.testing.expect(!planned_while_paused.durableProgressed());
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.builds_started);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.worker_steps);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.coordinator_steps);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.pages_completed);
    try std.testing.expectEqual(@as(usize, 0), planned_while_paused.published);

    try db.runUntilIdle();

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expect(status.maintenance_paused);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.stale, status.state);
        try std.testing.expectEqual(first_generation, status.published_generation);

        const top = try graph_entry.index.graphMetricTopK("degree", 10);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 2), top.len);
        for (top) |score| try std.testing.expect(!std.mem.eql(u8, score.node, "doc:c"));
    }

    var refreshed_while_paused = try db.refreshGraphMetric(alloc, "graph_idx", "degree");
    defer refreshed_while_paused.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, refreshed_while_paused.state);
    try std.testing.expect(refreshed_while_paused.maintenance_paused);
    try std.testing.expect(refreshed_while_paused.published_generation > first_generation);

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const top = try graph_entry.index.graphMetricTopK("degree", 1);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 1), top.len);
        try std.testing.expectEqualStrings("doc:b", top[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 2.0), top[0].score, 0.001);
    }

    var resumed = try db.resumeGraphMetricMaintenance(alloc, "graph_idx", "degree");
    defer resumed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, resumed.state);
    try std.testing.expect(!resumed.maintenance_paused);

    const resumed_generation = resumed.published_generation;
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runUntilIdle();

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        try std.testing.expect(!status.maintenance_paused);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
        try std.testing.expect(status.published_generation > resumed_generation);

        const top = try graph_entry.index.graphMetricTopK("degree", 1);
        defer {
            for (top) |*score| score.deinit(alloc);
            alloc.free(top);
        }
        try std.testing.expectEqual(@as(usize, 1), top.len);
        try std.testing.expectEqualStrings("doc:b", top[0].node);
        try std.testing.expectApproxEqAbs(@as(f64, 3.0), top[0].score, 0.001);
    }
}

test "db graph metric runtime default gate runUntilIdle can use planned graph metric maintenance when enabled" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .planned,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle planned graph metric maintenance reports budget exhaustion and resumes" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .planned,
        .graph_metric_idle_planned_options = .{
            .worker_id = "planned-idle-budgeted",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    {
        const decision = try db.core.index_manager.graphMetricPlannedAutoIdleDecision(db.graph_metric_idle_auto_options);
        try std.testing.expect(decision.shouldRunPlanned());
        try std.testing.expectEqual(@as(usize, 0), decision.active_builds);
        try std.testing.expectEqual(@as(usize, 1), decision.eligible_queued);
        try std.testing.expectEqual(@as(usize, 0), decision.ineligible_queued);
    }

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expect(pending.active_builds > 0);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto chooses planned for one degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-degree-idle",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:b", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto chooses planned for one small pagerank" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-pagerank-idle",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    {
        const decision = try db.core.index_manager.graphMetricPlannedAutoIdleDecision(db.graph_metric_idle_auto_options);
        try std.testing.expect(decision.shouldRunPlanned());
        try std.testing.expectEqual(@as(usize, 0), decision.active_builds);
        try std.testing.expectEqual(@as(usize, 1), decision.eligible_queued);
        try std.testing.expectEqual(@as(usize, 0), decision.ineligible_queued);
    }

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto can cap larger pagerank" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-large-pagerank-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_pagerank_iterations = 3,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":4,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, false, 0, 0, 0, 1);

    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto can widen pagerank planned gate" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-wide-pagerank-planned",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_pagerank_iterations = 4,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":4,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    {
        const decision = try db.core.index_manager.graphMetricPlannedAutoIdleDecision(db.graph_metric_idle_auto_options);
        try std.testing.expect(decision.shouldRunPlanned());
        try std.testing.expectEqual(@as(usize, 0), decision.active_builds);
        try std.testing.expectEqual(@as(usize, 1), decision.eligible_queued);
        try std.testing.expectEqual(@as(usize, 0), decision.ineligible_queued);
    }

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto chooses bounded planned for multi metric indexes" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-multi-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 2, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 2), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "pagerank",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqualStrings("doc:b", metric_result.graph_metric_results[1].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto chooses planned for one small eigenvector" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-eigenvector-idle",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "eigenvector",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto chooses planned for compatible hits by default" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-hits-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
}

test "db graph metric runtime default gate runUntilIdle default graph metric maintenance auto resumes active planned pagerank" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_planned_options = .{
            .worker_id = "default-auto-active-pagerank",
            .max_rounds = 0,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"manual\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "pagerank", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 1, 0, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance chooses planned for one small pagerank" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-planned-idle",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance chooses planned for one degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-degree-idle",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:b", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance chooses planned for one small eigenvector" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-eigenvector-idle",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "eigenvector",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance chooses bounded planned for multi metric indexes" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-legacy-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 2, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 2), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "pagerank",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqualStrings("doc:b", metric_result.graph_metric_results[1].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance defers queued work at per-index cap" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-bounded-fair-cap",
            .max_rounds = 200,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_active_builds_per_index = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 1, 0);

    try db.runUntilIdle();

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "pagerank",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "pagerank",
                    .top_k = 2,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "degree",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "degree",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance can cap larger pagerank" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-large-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_pagerank_iterations = 3,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":4,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, false, 0, 0, 0, 1);

    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqualStrings("doc:d", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance can cap larger eigenvector" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-large-eigenvector-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_eigenvector_iterations = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, false, 0, 0, 0, 1);

    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "eigenvector",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance can widen eigenvector planned gate" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-wide-eigenvector-planned",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_eigenvector_iterations = 2,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    {
        const decision = try db.core.index_manager.graphMetricPlannedAutoIdleDecision(db.graph_metric_idle_auto_options);
        try std.testing.expect(decision.shouldRunPlanned());
        try std.testing.expectEqual(@as(usize, 0), decision.active_builds);
        try std.testing.expectEqual(@as(usize, 1), decision.eligible_queued);
        try std.testing.expectEqual(@as(usize, 0), decision.ineligible_queued);
    }

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "eigenvector",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance chooses planned for compatible hits by default" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-hits-default-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 0, 1, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance falls back for incompatible hits pair" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-hits-incompatible-fallback",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_hits_iterations = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.00001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, false, 0, 0, 0, 1);

    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance chooses planned for one compatible small hits pair" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-hits-opt-in",
            .max_rounds = 1,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
        .graph_metric_idle_auto_options = .{
            .max_hits_iterations = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .write,
    });

    try db.runDerivedUntil(db.core.nextDerivedSequence());
    {
        const decision = try db.core.index_manager.graphMetricPlannedAutoIdleDecision(db.graph_metric_idle_auto_options);
        try std.testing.expect(decision.shouldRunPlanned());
        try std.testing.expectEqual(@as(usize, 0), decision.active_builds);
        try std.testing.expectEqual(@as(usize, 1), decision.eligible_queued);
        try std.testing.expectEqual(@as(usize, 0), decision.ineligible_queued);
    }

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(@as(usize, 3), metric_result.graph_metric_results[1].scores.len);
    try std.testing.expectEqualStrings("doc:authority", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance resumes active planned degree" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-active-degree",
            .max_rounds = 0,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"manual\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "degree", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 1, 0, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqualStrings("doc:b", metric_result.graph_metric_results[0].scores[0].node);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance resumes active planned eigenvector" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-active-eigenvector",
            .max_rounds = 0,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "eigenvector", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 1, 0, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "eigenvector",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results[0].scores.len);
}

test "db graph metric runtime default gate runUntilIdle auto graph metric maintenance resumes active planned hits pair" {
    const DB = @import("../mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_idle_maintenance = .auto,
        .graph_metric_idle_planned_options = .{
            .worker_id = "auto-active-hits",
            .max_rounds = 0,
            .max_metrics_per_round = 8,
            .max_pages_per_round = 1,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
    defer started.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, started.state);
    try std.testing.expectEqual(target_generation, started.building_generation);
    try expectPlannedAutoIdleDecision(db.core.index_manager, db.graph_metric_idle_auto_options, true, 1, 0, 0, 0);

    try std.testing.expectError(error.RunUntilIdleDidNotConverge, db.runUntilIdle());
    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 1), pending.active_builds);
    }

    db.graph_metric_idle_planned_options.max_rounds = 200;
    try db.runUntilIdle();

    {
        const pending = db.pendingWorkStats().graph_metric;
        try std.testing.expect(!pending.hasWork());
        try std.testing.expectEqual(@as(usize, 0), pending.queued_builds);
        try std.testing.expectEqual(@as(usize, 0), pending.active_builds);
    }

    var metric_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    defer metric_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), metric_result.graph_metric_results.len);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[0].status.state);
    try std.testing.expectEqual(target_generation, metric_result.graph_metric_results[0].status.published_generation);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, metric_result.graph_metric_results[1].status.state);
    try std.testing.expectEqual(metric_result.graph_metric_results[0].status.published_generation, metric_result.graph_metric_results[1].status.published_generation);
    try std.testing.expectEqual(@as(usize, 3), metric_result.graph_metric_results[0].scores.len);
    try std.testing.expectEqual(@as(usize, 3), metric_result.graph_metric_results[1].scores.len);
    try std.testing.expectEqualStrings("doc:authority", metric_result.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_result.graph_metric_results[0].scores[0].score, 0.001);
    try std.testing.expect(metric_result.graph_metric_results[1].scores[0].score >= metric_result.graph_metric_results[1].scores[1].score);
    try std.testing.expect(metric_result.graph_metric_results[1].scores[1].score > metric_result.graph_metric_results[1].scores[2].score);
}
