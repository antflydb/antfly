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
const platform_clock = @import("antfly_platform").clock;
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = struct {
    enabled: bool = false,
    lease_owned: bool = false,
    owner_id: []const u8 = "local",
    lease_ttl_ms: u64 = 30_000,
    interval_ms: u64 = 30_000,
    cutoff_ns: u64 = 5 * std.time.ns_per_min,
    /// Stable transaction sessions may be retried for seven days. Retain the
    /// terminal decision for an extra day so boundary retries cannot reapply.
    retained_terminal_ns: u64 = 8 * std.time.ns_per_day,
    /// Bound each background pass; a cursor rotates across the keyspace so
    /// retained idempotency decisions cannot create unbounded allocations or
    /// periodic CPU spikes.
    max_records_per_run: usize = 16_384,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
    resolver_ctx: ?*anyopaque = null,
    resolve_participant_fn: ?resolution_mod.ResolveParticipantFn = null,
    /// Replicated DBs route all transaction metadata changes through their
    /// coordinator Raft group. Standalone stores keep the direct local path.
    replicated_metadata: bool = false,
    owns_recovery_fn: ?*const fn (ctx: *anyopaque, owner_participant: []const u8) bool = null,
    acknowledge_participant_fn: ?*const fn (
        ctx: *anyopaque,
        txn_id: transactions_mod.TxnId,
        owner_participant: []const u8,
        participant: []const u8,
    ) anyerror!void = null,
    cleanup_transaction_fn: ?*const fn (
        ctx: *anyopaque,
        txn_id: transactions_mod.TxnId,
        owner_participant: []const u8,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) anyerror!void = null,
    /// Participant represented by the DB currently being recovered. Local
    /// effects are resolved through the DB pipeline before notifications, so
    /// this participant can be acknowledged without recursively routing back
    /// through the table-write source.
    local_participant: ?[]const u8 = null,
    local_resolution_ctx: ?*anyopaque = null,
    resolve_local_fn: ?*const fn (
        ctx: *anyopaque,
        txn_id: transactions_mod.TxnId,
        status: transactions_mod.TxnStatus,
        commit_version: u64,
    ) anyerror!void = null,
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
        if (config.enabled and config.replicated_metadata and
            (config.owns_recovery_fn == null or config.acknowledge_participant_fn == null or config.cleanup_transaction_fn == null))
        {
            return error.MissingReplicatedRecoveryHooks;
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
    scan_after: ?transactions_mod.TxnId = null,

    pub fn init(
        alloc: Allocator,
        store: anytype,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !Runtime {
        if (config.enabled and (config.resolve_participant_fn == null or config.resolver_ctx == null)) {
            return error.MissingParticipantResolver;
        }
        if (config.enabled and config.replicated_metadata and
            (config.owns_recovery_fn == null or config.acknowledge_participant_fn == null or config.cleanup_transaction_fn == null))
        {
            return error.MissingReplicatedRecoveryHooks;
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
    if (config.replicated_metadata and
        (config.owns_recovery_fn == null or config.acknowledge_participant_fn == null or config.cleanup_transaction_fn == null))
    {
        return error.MissingReplicatedRecoveryHooks;
    }

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
    next_scan_after: ?transactions_mod.TxnId = null,
};

fn runRecovery(runtime: *Runtime, now_ns: u64) !RunSummary {
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

    transaction: for (page.items) |txn| {
        summary.recovery.scanned_records += 1;
        if (txn.status == .pending) {
            const cutoff = now_ns -| config.cutoff_ns;
            if (txn.coordinator_known and txn.coordinator and txn.created_at > 0 and txn.created_at < cutoff) {
                const participants = try manager.getParticipants(alloc, txn.txn_id);
                defer transactions_mod.freeParticipantList(alloc, participants);
                if (participants.len > 0) {
                    if (config.owns_recovery_fn) |owns| {
                        if (!owns(config.resolver_ctx.?, participants[0])) continue;
                    }
                    summary.notification_attempts += 1;
                    config.resolve_participant_fn.?(
                        config.resolver_ctx.?,
                        txn.txn_id,
                        participants[0],
                        .aborted,
                        now_ns,
                    ) catch {
                        summary.notification_failures += 1;
                        continue;
                    };
                    summary.notification_successes += 1;
                }
            }
            continue;
        }

        const participants = try manager.getParticipants(alloc, txn.txn_id);
        defer transactions_mod.freeParticipantList(alloc, participants);
        const owner_participant = if (participants.len > 0) participants[0] else null;
        if (config.replicated_metadata and owner_participant == null) return error.InvalidParticipant;
        if (owner_participant) |owner| if (config.owns_recovery_fn) |owns| {
            if (!owns(config.resolver_ctx.?, owner)) continue;
        };

        const has_intents = try manager.hasIntents(txn.txn_id);
        const has_ha_outbox = if (config.replicated_metadata) false else try manager.hasHAOutbox(txn.txn_id);
        if (has_intents or has_ha_outbox) {
            if (config.replicated_metadata) {
                summary.notification_attempts += 1;
                config.resolve_participant_fn.?(
                    config.resolver_ctx.?,
                    txn.txn_id,
                    owner_participant.?,
                    txn.status,
                    txn.commit_version,
                ) catch {
                    summary.notification_failures += 1;
                    continue :transaction;
                };
                summary.notification_successes += 1;
            } else if (config.resolve_local_fn) |resolve_local| {
                summary.notification_attempts += 1;
                resolve_local(
                    config.local_resolution_ctx orelse return error.MissingLocalTransactionResolver,
                    txn.txn_id,
                    txn.status,
                    txn.commit_version,
                ) catch {
                    // A corrupt or otherwise poison transaction must not pin the
                    // bounded cursor and starve every record behind it. Leave its
                    // durable effects intact for the next keyspace rotation while
                    // allowing independent transactions in this page to recover.
                    summary.notification_failures += 1;
                    continue :transaction;
                };
                summary.notification_successes += 1;
            }
        }

        const unresolved = try manager.getUnresolvedParticipants(alloc, txn.txn_id);
        defer transactions_mod.freeParticipantList(alloc, unresolved);
        var all_resolved = true;
        const retained_cutoff = now_ns -| config.retained_terminal_ns;

        for (unresolved) |participant| {
            // Retained coordinators use their self-acknowledgement as the
            // durable handoff from the API session registry. Recovery must not
            // invent it during the advertised retry window. Once that window
            // has elapsed, the API session has expired and storage must release
            // the topology fence even if its node-local registry was lost.
            if (txn.status == .committed and txn.coordinator and txn.retain_terminal and owner_participant != null and
                std.mem.eql(u8, participant, owner_participant.?) and txn.finalized_at >= retained_cutoff)
            {
                all_resolved = false;
                continue;
            }
            summary.notification_attempts += 1;
            config.resolve_participant_fn.?(config.resolver_ctx.?, txn.txn_id, participant, txn.status, txn.commit_version) catch {
                summary.notification_failures += 1;
                all_resolved = false;
                continue;
            };
            if (config.replicated_metadata) {
                config.acknowledge_participant_fn.?(
                    config.resolver_ctx.?,
                    txn.txn_id,
                    owner_participant.?,
                    participant,
                ) catch {
                    summary.notification_failures += 1;
                    all_resolved = false;
                    continue;
                };
            } else try manager.markParticipantResolved(txn.txn_id, participant);
            summary.notification_successes += 1;
        }
        if (config.replicated_metadata and all_resolved) {
            const cutoff = now_ns -| config.cutoff_ns;
            if (txn.finalized_at < (if (txn.retain_terminal) retained_cutoff else cutoff)) {
                try config.cleanup_transaction_fn.?(
                    config.resolver_ctx.?,
                    txn.txn_id,
                    owner_participant.?,
                    cutoff,
                    retained_cutoff,
                );
            }
        }
    }

    if (config.replicated_metadata) return summary;

    const cutoff = now_ns -| config.cutoff_ns;
    summary.recovery = try manager.recoverTransactionSummariesWithExtraBatchHooksAndOptions(
        page.items,
        cutoff,
        now_ns,
        config.resolution_extra_hooks,
        .{
            .presume_abort_distributed = false,
            .retained_cutoff_timestamp = now_ns -| config.retained_terminal_ns,
        },
    );
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

test "transaction recovery drains terminal HA outbox without remaining intents" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "ha-outbox" });
    defer runtime_store.deinit();

    const txn_id: transactions_mod.TxnId = .{5} ** 16;
    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    try manager.initTransaction(txn_id, 1_000);
    try manager.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "{}" }}, &.{});
    const outbox_key = transactions_mod.makeTransactionHABatchOutboxKey(txn_id);
    _ = try manager.resolveIntentsWithExtraBatch(txn_id, .committed, 2_000, .{
        .writes = &.{.{ .key = &outbox_key, .value = "encoded-ha-batch" }},
    });
    try std.testing.expect(!try manager.hasIntents(txn_id));
    try std.testing.expect(try manager.hasHAOutbox(txn_id));

    const Recorder = struct {
        store: *backend_erased.Store,
        calls: usize = 0,

        fn resolveLocal(ptr: *anyopaque, actual_txn_id: transactions_mod.TxnId, status: transactions_mod.TxnStatus, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(txn_id, actual_txn_id);
            try std.testing.expectEqual(transactions_mod.TxnStatus.committed, status);
            var local_manager = try transactions_mod.TxnManager.init(std.testing.allocator, self.store);
            defer local_manager.deinit();
            try local_manager.clearHAOutbox(actual_txn_id, .batch);
        }
    };
    var recorder = Recorder{ .store = &runtime_store };
    const summary = try runRecoveryPageWithConfig(alloc, runtime_store, .{
        .enabled = true,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TestResolver.resolve,
        .local_resolution_ctx = &recorder,
        .resolve_local_fn = Recorder.resolveLocal,
    }, 3_000, null, 1);
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectEqual(@as(u64, 1), summary.recovery.scanned_records);
    try std.testing.expect(!try manager.hasHAOutbox(txn_id));
}

test "transaction recovery advances past a failed local resolution" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "local-resolution-fairness" });
    defer runtime_store.deinit();

    const poison_txn: transactions_mod.TxnId = .{1} ** 16;
    const healthy_txn: transactions_mod.TxnId = .{2} ** 16;
    const later_txn: transactions_mod.TxnId = .{3} ** 16;
    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    for ([_]transactions_mod.TxnId{ poison_txn, healthy_txn }) |txn_id| {
        try manager.initTransaction(txn_id, 1_000);
        try manager.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "{}" }}, &.{});
        const outbox_key = transactions_mod.makeTransactionHABatchOutboxKey(txn_id);
        _ = try manager.resolveIntentsWithExtraBatch(txn_id, .committed, 2_000, .{
            .writes = &.{.{ .key = &outbox_key, .value = "encoded-ha-batch" }},
        });
    }
    try manager.initTransaction(later_txn, 2_500);

    const Recorder = struct {
        store: *backend_erased.Store,
        calls: [2]transactions_mod.TxnId = undefined,
        call_count: usize = 0,

        fn resolveLocal(ptr: *anyopaque, txn_id: transactions_mod.TxnId, _: transactions_mod.TxnStatus, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls[self.call_count] = txn_id;
            self.call_count += 1;
            if (std.mem.eql(u8, &txn_id, &poison_txn)) return error.PoisonTransaction;

            var local_manager = try transactions_mod.TxnManager.init(std.testing.allocator, self.store);
            defer local_manager.deinit();
            try local_manager.clearHAOutbox(txn_id, .batch);
        }
    };
    var recorder = Recorder{ .store = &runtime_store };
    const summary = try runRecoveryPageWithConfig(alloc, runtime_store, .{
        .enabled = true,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TestResolver.resolve,
        .local_resolution_ctx = &recorder,
        .resolve_local_fn = Recorder.resolveLocal,
    }, 3_000, null, 2);

    try std.testing.expectEqual(@as(usize, 2), recorder.call_count);
    try std.testing.expectEqual(poison_txn, recorder.calls[0]);
    try std.testing.expectEqual(healthy_txn, recorder.calls[1]);
    try std.testing.expectEqual(@as(u64, 2), summary.notification_attempts);
    try std.testing.expectEqual(@as(u64, 1), summary.notification_failures);
    try std.testing.expectEqual(@as(u64, 1), summary.notification_successes);
    try std.testing.expect(try manager.hasHAOutbox(poison_txn));
    try std.testing.expect(!try manager.hasHAOutbox(healthy_txn));
    try std.testing.expect(summary.next_scan_after != null);
}

test "non-replicated transaction recovery honors the per-run page limit" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "bounded-local" });
    defer runtime_store.deinit();

    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    const txn_ids = [_]transactions_mod.TxnId{ .{1} ** 16, .{2} ** 16, .{3} ** 16 };
    for (txn_ids) |txn_id| {
        try manager.initTransactionWithParticipantsCreatedAtAndRole(txn_id, 1_000, 1_000, &.{}, true);
    }
    var ctx: u8 = 0;
    const summary = try runRecoveryPageWithConfig(alloc, runtime_store, .{
        .enabled = true,
        .cutoff_ns = 1_000,
        .resolver_ctx = &ctx,
        .resolve_participant_fn = TestResolver.resolve,
    }, 5_000, null, 1);
    try std.testing.expectEqual(@as(u64, 1), summary.recovery.scanned_records);
    try std.testing.expectEqual(@as(u64, 1), summary.recovery.auto_aborted);
    try std.testing.expect(summary.next_scan_after != null);
    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try manager.getTransactionStatus(txn_ids[0]));
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try manager.getTransactionStatus(txn_ids[1]));
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try manager.getTransactionStatus(txn_ids[2]));
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

test "transaction recovery delegates stale coordinator abort to replicated resolver" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "coordinator" });
    defer runtime_store.deinit();

    const txn_id: transactions_mod.TxnId = .{7} ** 16;
    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    try manager.initTransactionWithParticipantsCreatedAtAndRole(
        txn_id,
        10_000,
        1_000,
        &.{ "table2:4:docs:group:7", "table2:4:docs:group:8" },
        true,
    );
    try manager.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "{}" }}, &.{});

    const Recorder = struct {
        calls: usize = 0,
        expected_txn_id: transactions_mod.TxnId,
        fn resolve(ptr: *anyopaque, actual_txn_id: transactions_mod.TxnId, participant: []const u8, status: transactions_mod.TxnStatus, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(self.expected_txn_id, actual_txn_id);
            try std.testing.expectEqualStrings("table2:4:docs:group:7", participant);
            try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, status);
        }
    };
    var recorder = Recorder{ .expected_txn_id = txn_id };
    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(5_000);
    const stats = try recoverOnce(alloc, &runtime_store, .{
        .enabled = true,
        .cutoff_ns = 3_000,
        .clock = clock.clock(),
        .resolver_ctx = &recorder,
        .resolve_participant_fn = Recorder.resolve,
    });
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectEqual(@as(u64, 1), stats.notification_attempts);
    try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try manager.getTransactionStatus(txn_id));
}

test "replicated recovery is coordinator-owned and acknowledges through hooks" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "replicated-coordinator" });
    defer runtime_store.deinit();

    const txn_id: transactions_mod.TxnId = .{6} ** 16;
    const coordinator = "table2:4:docs:group:7";
    const remote = "table2:4:docs:group:8";
    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(
        txn_id,
        1_000,
        1_000,
        &.{ coordinator, remote },
        true,
        true,
    );
    try manager.resolveIntents(txn_id, .committed, 2_000);

    const Recorder = struct {
        resolve_calls: usize = 0,
        ack_calls: usize = 0,
        cleanup_calls: usize = 0,

        fn owns(_: *anyopaque, owner: []const u8) bool {
            std.testing.expectEqualStrings(coordinator, owner) catch return false;
            return true;
        }

        fn resolve(ptr: *anyopaque, actual_txn_id: transactions_mod.TxnId, participant: []const u8, status: transactions_mod.TxnStatus, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.resolve_calls += 1;
            try std.testing.expectEqual(txn_id, actual_txn_id);
            try std.testing.expect(std.mem.eql(u8, remote, participant) or std.mem.eql(u8, coordinator, participant));
            try std.testing.expectEqual(transactions_mod.TxnStatus.committed, status);
        }

        fn acknowledge(ptr: *anyopaque, actual_txn_id: transactions_mod.TxnId, owner: []const u8, participant: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.ack_calls += 1;
            try std.testing.expectEqual(txn_id, actual_txn_id);
            try std.testing.expectEqualStrings(coordinator, owner);
            try std.testing.expect(std.mem.eql(u8, remote, participant) or std.mem.eql(u8, coordinator, participant));
        }

        fn cleanup(ptr: *anyopaque, _: transactions_mod.TxnId, _: []const u8, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cleanup_calls += 1;
        }
    };

    var recorder = Recorder{};
    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(10_000);
    const stats = try recoverOnce(alloc, &runtime_store, .{
        .enabled = true,
        .clock = clock.clock(),
        .cutoff_ns = 1_000,
        .retained_terminal_ns = 20_000,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = Recorder.resolve,
        .replicated_metadata = true,
        .owns_recovery_fn = Recorder.owns,
        .acknowledge_participant_fn = Recorder.acknowledge,
        .cleanup_transaction_fn = Recorder.cleanup,
    });
    try std.testing.expectEqual(@as(usize, 1), recorder.resolve_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.ack_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.cleanup_calls);
    try std.testing.expectEqual(@as(u64, 1), stats.notification_successes);

    // Recovery requested remote propagation only. It neither invented the
    // stable API handoff acknowledgement for the coordinator itself nor
    // mutated coordinator metadata behind Raft's back.
    const unresolved = try manager.getUnresolvedParticipants(alloc, txn_id);
    defer transactions_mod.freeParticipantList(alloc, unresolved);
    try std.testing.expectEqual(@as(usize, 2), unresolved.len);
    try std.testing.expectEqualStrings(coordinator, unresolved[0]);
    try std.testing.expectEqualStrings(remote, unresolved[1]);

    // After the complete stable-session retry window, storage recovery is the
    // final safety net for a permanently lost node-local API registry.
    clock.setRealtimeNs(30_000);
    const expired_stats = try recoverOnce(alloc, &runtime_store, .{
        .enabled = true,
        .clock = clock.clock(),
        .cutoff_ns = 1_000,
        .retained_terminal_ns = 20_000,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = Recorder.resolve,
        .replicated_metadata = true,
        .owns_recovery_fn = Recorder.owns,
        .acknowledge_participant_fn = Recorder.acknowledge,
        .cleanup_transaction_fn = Recorder.cleanup,
    });
    try std.testing.expectEqual(@as(usize, 3), recorder.resolve_calls);
    try std.testing.expectEqual(@as(usize, 3), recorder.ack_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.cleanup_calls);
    try std.testing.expectEqual(@as(u64, 2), expired_stats.notification_successes);
}
