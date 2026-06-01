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
const tracing = @import("../../../tracing/mod.zig");
const types = @import("../types.zig");
const ownership_mod = @import("../ownership.zig");
const resolution_mod = @import("../transaction_resolution.zig");
const platform_clock = @import("../../../platform/clock.zig");
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = struct {
    enabled: bool = false,
    lease_owned: bool = false,
    owner_id: []const u8 = "local",
    lease_ttl_ms: u64 = 30_000,
    interval_ms: u64 = 30_000,
    cutoff_ns: u64 = 5 * std.time.ns_per_min,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
    resolver_ctx: ?*anyopaque = null,
    resolve_participant_fn: ?resolution_mod.ResolveParticipantFn = null,
    resolution_extra_hooks: transactions_mod.TxnManager.RecoveryExtraBatchHooks = .{},
};

pub const default_lease_key = "\x00\x00__metadata__:transaction_recovery_lease";

pub const Runtime = if (builtin.os.tag == .freestanding) struct {
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
        if (config.enabled and (config.resolve_participant_fn == null or config.resolver_ctx == null)) {
            return error.MissingParticipantResolver;
        }
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

    pub fn stats(self: *@This()) types.TransactionRecoveryStats {
        return self.stats_value;
    }

    pub fn runOnce(self: *@This()) !void {
        if (self.config.enabled) return error.UnsupportedPlatform;
    }
} else struct {
    alloc: Allocator,
    io_impl: ?*Io.Threaded,
    store: backend_erased.Store,
    owns_store: bool,
    config: Config,
    ownership: ownership_mod.State,
    mutex: Io.Mutex = .init,
    shutdown: bool = false,
    stats_value: types.TransactionRecoveryStats = .{},
    future: ?Io.Future(void) = null,

    pub fn init(
        alloc: Allocator,
        store: anytype,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !Runtime {
        if (config.enabled and (config.resolve_participant_fn == null or config.resolver_ctx == null)) {
            return error.MissingParticipantResolver;
        }
        const io_impl = backend_runtime.io_impl;
        if (config.enabled and io_impl == null) return error.MissingBackendRuntimeIo;
        var runtime_store = try initRuntimeStore(alloc, store);
        errdefer runtime_store.deinit();
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
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

    pub fn deinit(self: *Runtime) void {
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.shutdown = true;
            self.mutex.unlock(io);

            if (self.future) |*future| _ = future.await(io);
        }
        self.future = null;
        self.ownership.deinit(self.alloc);
        if (self.owns_store) self.store.deinit();
        self.* = undefined;
    }

    pub fn start(self: *Runtime) !void {
        if (!self.config.enabled) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        self.future = try io.concurrent(workerMain, .{self});
    }

    pub fn stats(self: *Runtime) types.TransactionRecoveryStats {
        const maybe_io = if (self.io_impl) |io_impl| io_impl.io() else null;
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

    pub fn runOnce(self: *Runtime) !void {
        if (!self.config.enabled) return;
        const now_ns = self.config.clock.nowRealtimeNs();
        if (!ensureLease(self, now_ns)) return;
        const summary = try runRecovery(self, now_ns);
        recordRun(self, now_ns, summary, false);
    }
};

pub fn recoverOnce(alloc: Allocator, store: anytype, config: Config) !types.TransactionRecoveryStats {
    if (!config.enabled) return .{};
    if (config.resolve_participant_fn == null or config.resolver_ctx == null) return error.MissingParticipantResolver;

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
    };
    return stats;
}

fn workerMain(runtime: *Runtime) void {
    while (true) {
        if (isShutdown(runtime)) return;
        const now_ns = runtime.config.clock.nowRealtimeNs();
        if (!ensureLease(runtime, now_ns)) {
            sleepInterval(runtime);
            continue;
        }

        const summary = runRecovery(runtime, now_ns) catch {
            recordRun(runtime, now_ns, .{}, true);
            sleepInterval(runtime);
            continue;
        };
        recordRun(runtime, now_ns, summary, false);
        sleepInterval(runtime);
    }
}

fn ensureLease(runtime: *Runtime, now_ns: u64) bool {
    const now_ms: u64 = @intCast(now_ns / std.time.ns_per_ms);
    const io_impl = runtime.io_impl orelse return false;
    const io = io_impl.io();
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
};

fn runRecovery(runtime: *Runtime, now_ns: u64) !RunSummary {
    return try runRecoveryWithConfig(runtime.alloc, runtime.store, runtime.config, now_ns);
}

fn runRecoveryWithConfig(
    alloc: Allocator,
    store: anytype,
    config: Config,
    now_ns: u64,
) !RunSummary {
    var summary: RunSummary = .{};
    var manager = try transactions_mod.TxnManager.init(alloc, try backend_erased.storeFrom(alloc, store));
    defer manager.deinit();
    const txns = try manager.listTransactions(alloc);
    defer alloc.free(txns);

    for (txns) |txn| {
        if (txn.status == .pending) continue;

        const unresolved = try manager.getUnresolvedParticipants(alloc, txn.txn_id);
        defer transactions_mod.freeParticipantList(alloc, unresolved);
        if (unresolved.len == 0) continue;

        for (unresolved) |participant| {
            summary.notification_attempts += 1;
            config.resolve_participant_fn.?(config.resolver_ctx.?, txn.txn_id, participant, txn.status, txn.commit_version) catch {
                summary.notification_failures += 1;
                continue;
            };
            try manager.markParticipantResolved(txn.txn_id, participant);
            summary.notification_successes += 1;
        }
    }

    const cutoff = now_ns -| config.cutoff_ns;
    summary.recovery = try manager.recoverTransactionsWithExtraBatchHooks(cutoff, now_ns, config.resolution_extra_hooks);
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

fn sleepInterval(runtime: *Runtime) void {
    var remaining_ms = runtime.config.interval_ms;
    if (remaining_ms == 0) remaining_ms = 1;

    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 100);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
    }
}

fn isShutdown(runtime: *Runtime) bool {
    const io_impl = runtime.io_impl orelse return runtime.shutdown;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    return runtime.shutdown;
}

fn recordRun(runtime: *Runtime, now_ns: u64, summary: RunSummary, failed: bool) void {
    const maybe_io = if (runtime.io_impl) |io_impl| io_impl.io() else null;
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
    if (failed) runtime.stats_value.error_count += 1;
}

const TestResolver = struct {
    fn resolve(_: *anyopaque, _: transactions_mod.TxnId, _: []const u8, _: transactions_mod.TxnStatus, _: u64) !void {}
};

test "transaction recovery runtime recoverOnce works with memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime_store.deinit();

    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();

    const txn_id: transactions_mod.TxnId = .{ 9, 9, 9, 9, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3 };
    try manager.initTransaction(txn_id, 1_000);

    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(5_000);
    var ctx: u8 = 0;

    const stats = try recoverOnce(alloc, &runtime_store, .{
        .enabled = true,
        .cutoff_ns = 3_000,
        .clock = clock.clock(),
        .resolver_ctx = &ctx,
        .resolve_participant_fn = TestResolver.resolve,
    });
    try std.testing.expectEqual(@as(u64, 1), stats.auto_aborted);
    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try manager.getTransactionStatus(txn_id));
}

test "transaction recovery runtime recoverOnce works with lsm backend store" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime_store.deinit();

    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();

    const txn_id: transactions_mod.TxnId = .{ 8, 8, 8, 8, 1, 1, 1, 1, 4, 4, 4, 4, 5, 5, 5, 5 };
    try manager.initTransaction(txn_id, 1_000);

    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(5_000);
    var ctx: u8 = 0;

    const stats = try recoverOnce(alloc, &runtime_store, .{
        .enabled = true,
        .cutoff_ns = 3_000,
        .clock = clock.clock(),
        .resolver_ctx = &ctx,
        .resolve_participant_fn = TestResolver.resolve,
    });
    try std.testing.expectEqual(@as(u64, 1), stats.auto_aborted);
    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try manager.getTransactionStatus(txn_id));
}
