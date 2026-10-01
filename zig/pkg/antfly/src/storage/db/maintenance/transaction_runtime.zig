// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const transactions_mod = @import("../../transactions.zig");
const build_options = @import("build_options");
const types = @import("../types.zig");
const ownership_mod = @import("../ownership.zig");
const platform_clock = @import("antfly_platform").clock;
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = @import("../transaction_recovery_contract.zig").Config;

pub const default_lease_key = "\x00\x00__metadata__:transaction_recovery_lease";

const LocalRuntime = if (builtin.os.tag == .freestanding) struct {
    config: Config,
    stats_value: types.TransactionRecoveryStats = .{},

    pub fn init(
        alloc: Allocator,
        store: anytype,
        _: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !@This() {
        _ = alloc;
        _ = store;
        return .{
            .config = config,
            .stats_value = .{
                .enabled = config.enabled,
            },
        };
    }

    pub fn deinit(self: *@This()) void {
        self.* = undefined;
    }

    pub fn start(self: *@This()) !void {
        if (self.config.enabled) return error.UnsupportedPlatform;
    }

    pub fn stop(_: *@This()) bool {
        return false;
    }

    pub fn pause(_: *@This()) bool {
        return false;
    }

    pub fn resumeAfterPause(_: *@This()) !void {}

    pub fn ensureRunning(_: *@This()) !bool {
        return true;
    }

    pub fn isStarted(_: *const @This()) bool {
        return false;
    }

    pub fn stats(self: *@This()) types.TransactionRecoveryStats {
        return self.stats_value;
    }

    pub fn runOnce(self: *@This()) !void {
        if (self.config.enabled) return error.UnsupportedPlatform;
    }
} else struct {
    alloc: Allocator,
    /// Borrowed backend-neutral executor. The owning BackendRuntime keeps the
    /// implementation alive through this runtime's deinit, so the same worker
    /// lifecycle runs on Threaded and deterministic VoprIo backends.
    io: ?Io,
    store: backend_erased.Store,
    owns_store: bool,
    config: Config,
    ownership: ownership_mod.State,
    mutex: Io.Mutex = .init,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    desired_running: bool = false,
    paused: bool = false,
    shutdown: std.atomic.Value(bool) = .init(false),
    stats_value: types.TransactionRecoveryStats = .{},
    future: ?background_runtime_mod.MaintenanceScheduler.Handle = null,
    backend_runtime: ?*background_runtime_mod.BackendRuntime = null,
    scan_after: ?transactions_mod.TxnId = null,

    pub fn init(
        alloc: Allocator,
        store: anytype,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !LocalRuntime {
        const io = backend_runtime.io();
        if (config.enabled and io == null) return error.MissingBackendRuntimeIo;
        var runtime_store = try initRuntimeStore(alloc, store);
        errdefer runtime_store.deinit();
        return .{
            .alloc = alloc,
            .io = io,
            .backend_runtime = backend_runtime,
            .store = runtime_store.store,
            .owns_store = runtime_store.owned,
            .config = config,
            .ownership = try ownership_mod.State.init(alloc, store, default_lease_key, .{
                .lease_owned = config.lease_owned,
                .owner_id = config.owner_id,
                .lease_ttl_ms = config.lease_ttl_ms,
            }),
            .stats_value = .{
                .enabled = config.enabled,
            },
        };
    }

    pub fn deinit(self: *LocalRuntime) void {
        _ = self.stop();
        self.ownership.deinit(self.alloc);
        if (self.owns_store) self.store.deinit();
        self.* = undefined;
    }

    pub fn start(self: *LocalRuntime) !void {
        if (!self.config.enabled) return;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.desired_running = true;
        self.paused = false;
        try self.startLocked();
    }

    pub fn stop(self: *LocalRuntime) bool {
        if (!self.config.enabled) return false;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.desired_running = false;
        self.paused = true;
        return self.stopLocked();
    }

    pub fn pause(self: *LocalRuntime) bool {
        if (!self.config.enabled) return false;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.paused = true;
        const desired = self.desired_running;
        _ = self.stopLocked();
        return desired;
    }

    pub fn resumeAfterPause(self: *LocalRuntime) !void {
        if (!self.config.enabled) return;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.paused = false;
        if (self.desired_running) try self.startLocked();
    }

    pub fn ensureRunning(self: *LocalRuntime) !bool {
        if (!self.config.enabled) return true;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (!self.desired_running) return true;
        if (self.paused) return false;
        try self.startLocked();
        return true;
    }

    pub fn isStarted(self: *const LocalRuntime) bool {
        return self.future != null;
    }

    fn startLocked(self: *LocalRuntime) !void {
        if (self.future != null or self.paused or !self.desired_running) return;
        const io = self.io orelse return error.MissingBackendRuntimeIo;
        self.mutex.lockUncancelable(io);
        self.shutdown.store(false, .release);
        self.mutex.unlock(io);
        self.future = try (try self.backend_runtime.?.maintenanceScheduler()).register(self, workerStep);
    }

    fn stopLocked(self: *LocalRuntime) bool {
        const io = self.io orelse return false;
        if (self.future == null) return false;
        self.mutex.lockUncancelable(io);
        self.shutdown.store(true, .release);
        self.mutex.unlock(io);
        self.future.?.cancel(io);
        self.future = null;
        self.ownership.release();
        return true;
    }

    /// Publish shutdown without joining the worker. Borrowed deterministic
    /// schedulers use this before draining fibers; ordinary owners continue to
    /// use `stop`, which publishes the same flag and joins the future.
    pub fn beginTeardown(self: *LocalRuntime) void {
        self.shutdown.store(true, .release);
    }

    pub fn stats(self: *LocalRuntime) types.TransactionRecoveryStats {
        const maybe_io = self.io;
        if (maybe_io) |io| self.mutex.lockUncancelable(io);
        defer if (maybe_io) |io| self.mutex.unlock(io);
        var snapshot = self.stats_value;
        const ownership_stats = self.ownership.stats();
        snapshot.lease_owned = ownership_stats.lease_owned;
        snapshot.has_lease = ownership_stats.has_lease;
        snapshot.acquisition_count = ownership_stats.acquisition_count;
        snapshot.lease_acquire_failures = ownership_stats.lease_acquire_failures;
        snapshot.lost_leases = ownership_stats.lost_leases;
        snapshot.last_acquired_ms = ownership_stats.last_acquired_ms;
        return snapshot;
    }

    pub fn runOnce(self: *LocalRuntime) !void {
        if (!self.config.enabled) return;
        const now_ns = self.config.clock.nowRealtimeNs();
        if (!ensureLease(self, now_ns)) return;
        const summary = try runRecovery(self, now_ns);
        recordRun(self, now_ns, summary, false);
    }
};

pub fn recoverOnce(alloc: Allocator, store: anytype, config: Config) !types.TransactionRecoveryStats {
    if (!config.enabled) return .{};

    var runtime_store = try initRuntimeStore(alloc, store);
    defer runtime_store.deinit();
    const now_ns = config.clock.nowRealtimeNs();
    const summary = try runRecoveryWithConfig(alloc, runtime_store.store, config, now_ns);
    const stats: types.TransactionRecoveryStats = .{
        .enabled = true,
        .runs = 1,
        .scanned_records = summary.recovery.scanned_records,
        .auto_aborted = summary.recovery.auto_aborted,
        .resolved_finalized = summary.recovery.resolved_finalized,
        .cleaned_records = summary.recovery.cleaned_records,
        .kept_recent_pending = summary.recovery.kept_recent_pending,
        .deferred_unresolved = summary.recovery.deferred_unresolved,
        .notification_attempts = summary.notification_attempts,
        .notification_successes = summary.notification_successes,
        .notification_failures = summary.notification_failures,
        .last_run_ns = now_ns,
        .error_count = summary.record_failures,
    };
    return stats;
}

fn workerStep(runtime: *LocalRuntime) ?u64 {
    if (isShutdown(runtime)) return null;
    const now_ns = runtime.config.clock.nowRealtimeNs();
    if (ensureLease(runtime, now_ns)) {
        const summary = runRecovery(runtime, now_ns) catch {
            recordRun(runtime, now_ns, .{}, true);
            return @max(1, runtime.config.interval_ms);
        };
        recordRun(runtime, now_ns, summary, false);
    }
    return @max(1, runtime.config.interval_ms);
}

fn ensureLease(runtime: *LocalRuntime, now_ns: u64) bool {
    const now_ms: u64 = @intCast(now_ns / std.time.ns_per_ms);
    const io = runtime.io orelse return false;
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    const acquired = runtime.ownership.ensureLease(now_ms) catch {
        runtime.ownership.noteAcquireFailure();
        return false;
    };
    return acquired;
}

const RunSummary = struct {
    recovery: transactions_mod.RecoveryStats = .{},
    notification_attempts: u64 = 0,
    notification_successes: u64 = 0,
    notification_failures: u64 = 0,
    record_failures: u64 = 0,
    next_scan_after: ?transactions_mod.TxnId = null,
};

fn runRecovery(runtime: *LocalRuntime, now_ns: u64) !RunSummary {
    const summary = try runRecoveryPageWithConfig(
        runtime.alloc,
        runtime.store,
        runtime.config,
        now_ns,
        runtime.scan_after,
        @max(1, runtime.config.max_records_per_run),
    );
    runtime.scan_after = summary.next_scan_after;
    return summary;
}

fn runRecoveryWithConfig(
    alloc: Allocator,
    store: anytype,
    config: Config,
    now_ns: u64,
) !RunSummary {
    return try runRecoveryPageWithConfig(alloc, store, config, now_ns, null, std.math.maxInt(usize));
}

fn runRecoveryPageWithConfig(
    alloc: Allocator,
    store: anytype,
    config: Config,
    now_ns: u64,
    after: ?transactions_mod.TxnId,
    limit: usize,
) !RunSummary {
    var summary: RunSummary = .{};
    var manager = try transactions_mod.TxnManager.init(alloc, try backend_erased.storeFrom(alloc, store));
    defer manager.deinit();
    const page = try manager.listTransactionsPage(alloc, after, limit);
    defer alloc.free(page.items);
    summary.next_scan_after = page.next_after;

    var admitted: usize = 0;
    for (page.items) |txn| {
        if (txn.status == .pending) {
            page.items[admitted] = txn;
            admitted += 1;
            continue;
        }
        if (try manager.hasIntents(txn.txn_id) or try manager.hasReplicationOutbox(txn.txn_id)) {
            const resolve = config.resolve_local_fn orelse return error.MissingLocalTransactionResolver;
            resolve(config.local_resolution_ctx orelse return error.MissingLocalTransactionResolver, txn.txn_id, txn.status, txn.commit_version) catch {
                summary.record_failures += 1;
                continue;
            };
        }
        page.items[admitted] = txn;
        admitted += 1;
    }

    const cutoff = now_ns -| config.cutoff_ns;
    summary.recovery = try manager.recoverTransactionSummariesWithExtraBatchHooksAndOptions(
        page.items[0..admitted],
        cutoff,
        now_ns,
        config.resolution_extra_hooks,
        .{
            .presume_abort_distributed = false,
            .retained_cutoff_timestamp = now_ns -| config.retained_terminal_ns,
        },
    );
    summary.recovery.scanned_records += summary.record_failures;
    summary.recovery.deferred_unresolved += summary.record_failures;
    return summary;
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = true };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn isShutdown(runtime: *LocalRuntime) bool {
    return runtime.shutdown.load(.acquire);
}

fn recordRun(runtime: *LocalRuntime, now_ns: u64, summary: RunSummary, failed: bool) void {
    const maybe_io = runtime.io;
    if (maybe_io) |io| runtime.mutex.lockUncancelable(io);
    defer if (maybe_io) |io| runtime.mutex.unlock(io);
    runtime.stats_value.runs += 1;
    runtime.stats_value.scanned_records += summary.recovery.scanned_records;
    runtime.stats_value.auto_aborted += summary.recovery.auto_aborted;
    runtime.stats_value.resolved_finalized += summary.recovery.resolved_finalized;
    runtime.stats_value.cleaned_records += summary.recovery.cleaned_records;
    runtime.stats_value.kept_recent_pending += summary.recovery.kept_recent_pending;
    runtime.stats_value.deferred_unresolved += summary.recovery.deferred_unresolved;
    runtime.stats_value.notification_attempts += summary.notification_attempts;
    runtime.stats_value.notification_successes += summary.notification_successes;
    runtime.stats_value.notification_failures += summary.notification_failures;
    runtime.stats_value.last_run_ns = now_ns;
    runtime.stats_value.error_count += summary.record_failures;
    if (failed) runtime.stats_value.error_count += 1;
}

fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) @import("antfly_platform").time.yieldNow();
}

const contract = @import("../transaction_recovery_contract.zig");
pub const Runtime = struct {
    local: ?LocalRuntime = null,
    external: ?contract.OwnedRuntime = null,
    external_store: ?RuntimeStoreHandle = null,

    pub fn init(alloc: Allocator, store: anytype, background: *background_runtime_mod.BackendRuntime, config: Config) !Runtime {
        if (config.enabled) if (config.factory) |factory| {
            var runtime_store = try initRuntimeStore(alloc, store);
            errdefer runtime_store.deinit();
            const external = try factory.create(factory.ptr, alloc, runtime_store.store, background, .{
                .resolution_extra_hooks = config.resolution_extra_hooks,
                .local_resolution_ctx = config.local_resolution_ctx,
                .resolve_local_fn = config.resolve_local_fn,
            });
            return .{ .external = external, .external_store = runtime_store };
        };
        return .{ .local = try LocalRuntime.init(alloc, store, background, config) };
    }
    pub fn deinit(self: *Runtime) void {
        if (self.external) |runtime| {
            runtime.vtable.deinit(runtime.ptr);
            if (self.external_store) |*store| store.deinit();
            self.* = undefined;
            return;
        }
        return self.local.?.deinit();
    }
    pub fn start(self: *Runtime) anyerror!void {
        if (self.external) |runtime| return try runtime.vtable.start(runtime.ptr);
        return try self.local.?.start();
    }
    pub fn stop(self: *Runtime) bool {
        if (self.external) |runtime| return runtime.vtable.stop(runtime.ptr);
        return self.local.?.stop();
    }
    pub fn pause(self: *Runtime) bool {
        if (self.external) |runtime| return runtime.vtable.pause(runtime.ptr);
        return self.local.?.pause();
    }
    pub fn resumeAfterPause(self: *Runtime) anyerror!void {
        if (self.external) |runtime| return try runtime.vtable.resume_after_pause(runtime.ptr);
        return try self.local.?.resumeAfterPause();
    }
    pub fn ensureRunning(self: *Runtime) anyerror!bool {
        if (self.external) |runtime| return try runtime.vtable.ensure_running(runtime.ptr);
        return try self.local.?.ensureRunning();
    }
    pub fn isStarted(self: *Runtime) bool {
        if (self.external) |runtime| return runtime.vtable.is_started(runtime.ptr);
        return self.local.?.isStarted();
    }
    pub fn beginTeardown(self: *Runtime) void {
        if (self.external) |runtime| return runtime.vtable.teardown(runtime.ptr);
        if (comptime builtin.os.tag != .freestanding) if (self.local) |*runtime| runtime.beginTeardown();
    }
    pub fn stats(self: *Runtime) types.TransactionRecoveryStats {
        if (self.external) |runtime| return runtime.vtable.stats(runtime.ptr);
        return self.local.?.stats();
    }
    pub fn runOnce(self: *Runtime) anyerror!void {
        if (self.external) |runtime| return try runtime.vtable.run_once(runtime.ptr);
        return try self.local.?.runOnce();
    }
};

test "local transaction recovery preserves failed resolution and makes bounded progress" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "local-recovery" });
    defer store.deinit();
    var manager = try transactions_mod.TxnManager.init(alloc, &store);
    defer manager.deinit();
    const poison: transactions_mod.TxnId = .{1} ** 16;
    const healthy: transactions_mod.TxnId = .{2} ** 16;
    const pending: transactions_mod.TxnId = .{3} ** 16;
    for ([_]transactions_mod.TxnId{ poison, healthy }) |id| {
        try manager.initTransaction(id, 1_000);
        try manager.writeIntents(id, &.{.{ .key = &id, .value = "{}" }}, &.{});
        const outbox = transactions_mod.makeTransactionReplicationBatchOutboxKey(id);
        _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 2_000, .{ .writes = &.{.{ .key = &outbox, .value = "batch" }} });
    }
    try manager.initTransactionWithParticipants(pending, 1_000, &.{"remote"});
    const Resolver = struct {
        store: *backend_erased.Store,
        calls: usize = 0,
        fn resolve(raw: *anyopaque, id: transactions_mod.TxnId, _: transactions_mod.TxnStatus, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (std.mem.eql(u8, &id, &poison)) return error.PoisonTransaction;
            var txn_manager = try transactions_mod.TxnManager.init(std.testing.allocator, self.store);
            defer txn_manager.deinit();
            try txn_manager.clearReplicationOutbox(id, .batch);
        }
    };
    var resolver = Resolver{ .store = &store };
    const config: Config = .{ .enabled = true, .cutoff_ns = 500, .local_resolution_ctx = &resolver, .resolve_local_fn = Resolver.resolve };
    const first = try runRecoveryPageWithConfig(alloc, store, config, 3_000, null, 2);
    try std.testing.expectEqual(@as(usize, 2), resolver.calls);
    try std.testing.expectEqual(@as(u64, 1), first.record_failures);
    try std.testing.expect(try manager.hasReplicationOutbox(poison));
    try std.testing.expect(!try manager.hasReplicationOutbox(healthy));
    try std.testing.expect(first.next_scan_after != null);
    const second = try runRecoveryPageWithConfig(alloc, store, config, 3_000, first.next_scan_after, 2);
    try std.testing.expectEqual(@as(u64, 0), second.recovery.auto_aborted);
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try manager.getTransactionStatus(pending));
}

test "local transaction recovery factory releases owned adapters on initialization failure" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "factory-failure" });
    defer store.deinit();
    var background = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual, .filesystem_io = std.testing.io });
    defer background.deinit();
    const Failure = struct {
        fn create(_: *anyopaque, _: Allocator, _: backend_erased.Store, _: *background_runtime_mod.BackendRuntime, _: contract.CreateContext) !contract.OwnedRuntime {
            return error.FactoryInitializationFailed;
        }
    };
    // Force creation of an owned erased adapter around a borrowed backend.
    // The caller's store remains live after factory failure and runtime close.
    const Handle = struct {
        store: backend_erased.Store,
        pub fn backendStore(self: *@This()) backend_erased.Store {
            return self.store;
        }
    };
    var handle = Handle{ .store = store };
    var ctx: u8 = 0;
    try std.testing.expectError(error.FactoryInitializationFailed, Runtime.init(alloc, &handle, background.ptr(), .{
        .enabled = true,
        .factory = .{ .ptr = &ctx, .create = Failure.create },
    }));
    var disabled = try Runtime.init(alloc, &handle, background.ptr(), .{
        .enabled = false,
        .factory = .{ .ptr = &ctx, .create = Failure.create },
    });
    defer disabled.deinit();
    try std.testing.expect(disabled.external == null);
    try disabled.start();
    try std.testing.expect(!disabled.isStarted());
}
