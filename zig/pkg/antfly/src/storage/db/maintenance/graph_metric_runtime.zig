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
const index_manager_mod = @import("../catalog/index_manager.zig");
const ownership_mod = @import("../ownership.zig");
const graph_mod = @import("../../../graph/graph.zig");
const graph_query_mod = @import("../../../graph/query.zig");
const platform_clock = @import("../../../platform/clock.zig");
const background_runtime_mod = @import("../../background_runtime.zig");

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

test "db graph metric runtime query public reads fail not ready before first publish" {
    const DB = @import("../mod.zig").DB;
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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

test "db graph metric runtime default gate runUntilIdle can use planned graph metric maintenance when enabled" {
    const DB = @import("../mod.zig").DB;
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
    const db_test_support = @import("../test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

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
