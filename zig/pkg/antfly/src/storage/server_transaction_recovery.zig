// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const driver = @import("db/maintenance/transaction_recovery_driver.zig");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const lsm_backend = @import("lsm_backend.zig");
const mem_backend = @import("mem_backend.zig");
const transactions_mod = @import("transactions.zig");
const types = @import("db/types.zig");
const platform_clock = @import("antfly_platform").clock;
const background_runtime_mod = @import("background_runtime.zig");

pub const Config = @import("server_transaction_recovery_contract.zig").Config;

const AcknowledgementWindow = struct {
    members: [64][]const u8 = undefined,
    count: usize = 0,

    fn flush(self: *@This(), manager: *transactions_mod.TxnManager, config: Config, txn_id: transactions_mod.TxnId, owner: ?[]const u8, summary: anytype) !bool {
        if (self.count == 0) return true;
        const participants = self.members[0..self.count];
        defer self.count = 0;
        if (!config.replicated_metadata) {
            try manager.markParticipantsResolvedExtraBatch(txn_id, participants, .{});
            summary.notification_successes += participants.len;
            return true;
        }
        if (config.acknowledge_participants_fn) |many| {
            const delivered = delivered: {
                many(config.resolver_ctx.?, txn_id, owner.?, participants) catch |err| {
                    if (err == error.UnsupportedOperation or err == error.UnsupportedRaftBatchProtocolVersion) break :delivered false;
                    // Even if a subset committed, no delivery was proven for
                    // this window. Durable resolution remains safe to retry.
                    summary.notification_failures += participants.len;
                    return false;
                };
                break :delivered true;
            };
            if (delivered) {
                summary.notification_successes += participants.len;
                return true;
            }
        }
        var complete = true;
        for (participants) |participant| {
            config.acknowledge_participant_fn.?(config.resolver_ctx.?, txn_id, owner.?, participant) catch {
                summary.notification_failures += 1;
                complete = false;
                continue;
            };
            summary.notification_successes += 1;
        }
        return complete;
    }
};

pub const default_lease_key = driver.default_lease_key;

pub const Runtime = driver.Driver(Policy);

const Policy = struct {
    pub const Config = @import("server_transaction_recovery_contract.zig").Config;
    pub const runPage = runRecoveryPageWithConfig;
    pub fn validate(config: @This().Config) !void {
        if (config.enabled and (config.resolve_participant_fn == null or config.resolver_ctx == null)) return error.MissingParticipantResolver;
        if (config.enabled and config.replicated_metadata and
            (config.owns_recovery_fn == null or config.acknowledge_participant_fn == null or config.cleanup_transaction_fn == null)) return error.MissingReplicatedRecoveryHooks;
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
        .error_count = summary.record_failures,
    };
    return stats;
}

const RunSummary = driver.RunSummary;

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
        const has_replication_outbox = if (config.replicated_metadata) false else try manager.hasReplicationOutbox(txn.txn_id);
        var local_effects_resolved = true;
        if (has_intents or has_replication_outbox) {
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
                    local_effects_resolved = false;
                };
                if (local_effects_resolved) summary.notification_successes += 1;
            } else if (config.resolve_local_fn) |resolve_local| {
                summary.notification_attempts += 1;
                resolve_local(
                    config.local_resolution_ctx orelse return error.MissingLocalTransactionResolver,
                    txn.txn_id,
                    txn.status,
                    txn.commit_version,
                ) catch {
                    // A corrupt or otherwise poison transaction must not pin the
                    // bounded cursor and starve work behind it. Leave its durable
                    // effects intact for the next keyspace rotation while still
                    // propagating the decision to independent remote participants
                    // and recovering later transactions in this page.
                    summary.notification_failures += 1;
                    local_effects_resolved = false;
                };
                if (local_effects_resolved) summary.notification_successes += 1;
            }
        }

        const unresolved = try manager.getUnresolvedParticipants(alloc, txn.txn_id);
        defer transactions_mod.freeParticipantList(alloc, unresolved);
        var all_resolved = local_effects_resolved;
        const retained_cutoff = now_ns -| config.retained_terminal_ns;

        var acknowledgements: AcknowledgementWindow = .{};
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
            acknowledgements.members[acknowledgements.count] = participant;
            acknowledgements.count += 1;
            if (acknowledgements.count == acknowledgements.members.len) {
                if (!try acknowledgements.flush(&manager, config, txn.txn_id, owner_participant, &summary)) all_resolved = false;
            }
        }
        if (!try acknowledgements.flush(&manager, config, txn.txn_id, owner_participant, &summary)) all_resolved = false;
        if (config.replicated_metadata and all_resolved) {
            const cutoff = now_ns -| config.cutoff_ns;
            if (txn.finalized_at < (if (txn.retain_terminal) retained_cutoff else cutoff)) {
                config.cleanup_transaction_fn.?(
                    config.resolver_ctx.?,
                    txn.txn_id,
                    owner_participant.?,
                    cutoff,
                    retained_cutoff,
                ) catch {
                    // Cleanup is an idempotent record-local Raft operation. A
                    // failed proposal must remain retryable without pinning the
                    // bounded recovery cursor ahead of unrelated transactions.
                    summary.record_failures += 1;
                    continue :transaction;
                };
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

const initRuntimeStore = driver.initRuntimeStore;

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
    const outbox_key = transactions_mod.makeTransactionReplicationBatchOutboxKey(txn_id);
    _ = try manager.resolveIntentsWithExtraBatch(txn_id, .committed, 2_000, .{
        .writes = &.{.{ .key = &outbox_key, .value = "encoded-ha-batch" }},
    });
    try std.testing.expect(!try manager.hasIntents(txn_id));
    try std.testing.expect(try manager.hasReplicationOutbox(txn_id));

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
            try local_manager.clearReplicationOutbox(actual_txn_id, .batch);
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
    try std.testing.expect(!try manager.hasReplicationOutbox(txn_id));
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
    try manager.initTransactionWithParticipants(poison_txn, 1_000, &.{"remote"});
    try manager.initTransaction(healthy_txn, 1_000);
    for ([_]transactions_mod.TxnId{ poison_txn, healthy_txn }) |txn_id| {
        try manager.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "{}" }}, &.{});
        const outbox_key = transactions_mod.makeTransactionReplicationBatchOutboxKey(txn_id);
        _ = try manager.resolveIntentsWithExtraBatch(txn_id, .committed, 2_000, .{
            .writes = &.{.{ .key = &outbox_key, .value = "encoded-ha-batch" }},
        });
    }
    try manager.initTransaction(later_txn, 2_500);

    const Recorder = struct {
        store: *backend_erased.Store,
        calls: [2]transactions_mod.TxnId = undefined,
        call_count: usize = 0,
        remote_calls: usize = 0,

        fn resolveLocal(ptr: *anyopaque, txn_id: transactions_mod.TxnId, _: transactions_mod.TxnStatus, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls[self.call_count] = txn_id;
            self.call_count += 1;
            if (std.mem.eql(u8, &txn_id, &poison_txn)) return error.PoisonTransaction;

            var local_manager = try transactions_mod.TxnManager.init(std.testing.allocator, self.store);
            defer local_manager.deinit();
            try local_manager.clearReplicationOutbox(txn_id, .batch);
        }

        fn resolveParticipant(ptr: *anyopaque, txn_id: transactions_mod.TxnId, participant: []const u8, status: transactions_mod.TxnStatus, commit_version: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.remote_calls += 1;
            try std.testing.expectEqual(poison_txn, txn_id);
            try std.testing.expectEqualStrings("remote", participant);
            try std.testing.expectEqual(transactions_mod.TxnStatus.committed, status);
            try std.testing.expectEqual(@as(u64, 2_000), commit_version);
        }
    };
    var recorder = Recorder{ .store = &runtime_store };
    const summary = try runRecoveryPageWithConfig(alloc, runtime_store, .{
        .enabled = true,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = Recorder.resolveParticipant,
        .local_resolution_ctx = &recorder,
        .resolve_local_fn = Recorder.resolveLocal,
    }, 3_000, null, 2);

    try std.testing.expectEqual(@as(usize, 2), recorder.call_count);
    try std.testing.expectEqual(poison_txn, recorder.calls[0]);
    try std.testing.expectEqual(healthy_txn, recorder.calls[1]);
    try std.testing.expectEqual(@as(usize, 1), recorder.remote_calls);
    try std.testing.expectEqual(@as(u64, 3), summary.notification_attempts);
    try std.testing.expectEqual(@as(u64, 1), summary.notification_failures);
    try std.testing.expectEqual(@as(u64, 2), summary.notification_successes);
    try std.testing.expect(try manager.hasReplicationOutbox(poison_txn));
    try std.testing.expect(!try manager.hasReplicationOutbox(healthy_txn));
    const unresolved = try manager.getUnresolvedParticipants(alloc, poison_txn);
    defer transactions_mod.freeParticipantList(alloc, unresolved);
    try std.testing.expectEqual(@as(usize, 0), unresolved.len);
    try std.testing.expect(summary.next_scan_after != null);
}

test "transaction recovery advances past a failed replicated cleanup" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "replicated-cleanup-fairness" });
    defer runtime_store.deinit();

    const poison_txn: transactions_mod.TxnId = .{1} ** 16;
    const healthy_txn: transactions_mod.TxnId = .{2} ** 16;
    const later_txn: transactions_mod.TxnId = .{3} ** 16;
    const owner = "owner";
    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    for ([_]transactions_mod.TxnId{ poison_txn, healthy_txn }) |txn_id| {
        try manager.initTransactionWithParticipantsCreatedAtAndRole(txn_id, 1_000, 1_000, &.{owner}, true);
        try manager.resolveIntents(txn_id, .committed, 2_000);
        try manager.markParticipantResolved(txn_id, owner);
    }
    try manager.initTransaction(later_txn, 2_500);

    const Recorder = struct {
        calls: [2]transactions_mod.TxnId = undefined,
        call_count: usize = 0,

        fn owns(_: *anyopaque, _: []const u8) bool {
            return true;
        }

        fn resolve(_: *anyopaque, _: transactions_mod.TxnId, _: []const u8, _: transactions_mod.TxnStatus, _: u64) !void {
            return error.UnexpectedParticipantResolution;
        }

        fn acknowledge(_: *anyopaque, _: transactions_mod.TxnId, _: []const u8, _: []const u8) !void {
            return error.UnexpectedParticipantAcknowledgement;
        }

        fn cleanup(ptr: *anyopaque, txn_id: transactions_mod.TxnId, _: []const u8, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls[self.call_count] = txn_id;
            self.call_count += 1;
            if (std.mem.eql(u8, &txn_id, &poison_txn)) return error.PoisonCleanup;
        }
    };
    var recorder = Recorder{};
    const summary = try runRecoveryPageWithConfig(alloc, runtime_store, .{
        .enabled = true,
        .cutoff_ns = 1_000,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = Recorder.resolve,
        .replicated_metadata = true,
        .owns_recovery_fn = Recorder.owns,
        .acknowledge_participant_fn = Recorder.acknowledge,
        .cleanup_transaction_fn = Recorder.cleanup,
    }, 10_000, null, 2);

    try std.testing.expectEqual(@as(usize, 2), recorder.call_count);
    try std.testing.expectEqual(poison_txn, recorder.calls[0]);
    try std.testing.expectEqual(healthy_txn, recorder.calls[1]);
    try std.testing.expectEqual(@as(u64, 1), summary.record_failures);
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

test "transaction recovery executes production pass on borrowed VoprIo" {
    const vopr = @import("vopr");
    const alloc = std.testing.allocator;
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        .required = .of(&.{ .clock_read, .task_scheduling, .synchronization, .sleep }),
    });
    defer vopr_io.deinit();

    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "vopr-transaction-recovery" });
    defer runtime_store.deinit();
    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    const txn_id: transactions_mod.TxnId = .{6} ** 16;
    try manager.initTransaction(txn_id, 1_000);

    var runtime_owners_closed = false;
    var backend_runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .borrowed_io = .{ .general = vopr_io.io() },
    });
    defer if (!runtime_owners_closed) backend_runtime.deinit();
    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(5_000);
    var resolver_ctx: u8 = 0;
    var runtime = try Runtime.init(alloc, &runtime_store, backend_runtime.ptr(), .{
        .enabled = true,
        .cutoff_ns = 3_000,
        .clock = clock.clock(),
        .resolver_ctx = &resolver_ctx,
        .resolve_participant_fn = TestResolver.resolve,
    });
    defer if (!runtime_owners_closed) runtime.deinit();

    try runtime.runOnce();
    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try manager.getTransactionStatus(txn_id));
    try std.testing.expectEqual(@as(u64, 1), runtime.stats().runs);
    var lifecycle_ok = false;
    const Lifecycle = struct {
        fn run(target: *Runtime, backend_owner: *background_runtime_mod.BackendRuntimeHandle, closed: *bool, passed: *bool) void {
            // Shared executor ownership outlives the registration. Drain both
            // inside VoprIo before requiring the scheduler to be quiescent.
            defer {
                target.deinit();
                backend_owner.deinit();
                closed.* = true;
            }
            target.start() catch return;
            if (!target.isStarted()) return;
            if (!target.pause()) return;
            if (target.isStarted()) return;
            target.resumeAfterPause() catch return;
            if (!target.isStarted()) return;
            if (!target.stop()) return;
            if (target.isStarted()) return;
            passed.* = true;
        }
    };
    _ = vopr_io.io().async(Lifecycle.run, .{ &runtime, &backend_runtime, &runtime_owners_closed, &lifecycle_ok });
    const scheduler = vopr_io.scheduler();
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        try scheduler.executeReady(enabled.items.items[0].id, &events, alloc);
    }
    try std.testing.expect(lifecycle_ok);
    try vopr_io.ensureNoCapabilityViolation();
}

test "replicated recovery batches proven followers preserves uncertain debt and retains self handoff" {
    const alloc = std.testing.allocator;
    const Recorder = struct {
        const Mode = enum { success, unsupported, unknown, committed_reply_loss };
        manager: *transactions_mod.TxnManager,
        mode: Mode,
        batches: usize = 0,
        singles: usize = 0,
        cleanups: usize = 0,
        fn owns(_: *anyopaque, owner: []const u8) bool {
            return std.mem.eql(u8, owner, "owner");
        }
        fn resolve(_: *anyopaque, _: transactions_mod.TxnId, participant: []const u8, _: transactions_mod.TxnStatus, _: u64) !void {
            if (std.mem.eql(u8, participant, "failed")) return error.GroupLeaderUnavailable;
        }
        fn single(ptr: *anyopaque, txn: transactions_mod.TxnId, owner: []const u8, participant: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("owner", owner);
            self.singles += 1;
            try self.manager.markParticipantResolved(txn, participant);
        }
        fn many(ptr: *anyopaque, txn: transactions_mod.TxnId, owner: []const u8, participants: []const []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batches += 1;
            try std.testing.expectEqualStrings("owner", owner);
            try std.testing.expect(participants.len > 0 and participants.len <= 64);
            for (participants) |participant| try std.testing.expect(!std.mem.eql(u8, participant, "failed") and !std.mem.eql(u8, participant, "owner"));
            switch (self.mode) {
                .unsupported => return error.UnsupportedOperation,
                .unknown => return error.RaftBatchWriteOutcomeUnknown,
                .committed_reply_loss => {
                    try self.manager.markParticipantsResolvedExtraBatch(txn, participants, .{});
                    return error.RaftBatchWriteOutcomeUnknown;
                },
                .success => try self.manager.markParticipantsResolvedExtraBatch(txn, participants, .{}),
            }
        }
        fn cleanup(ptr: *anyopaque, _: transactions_mod.TxnId, _: []const u8, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cleanups += 1;
        }
    };
    var participants: [132][]const u8 = undefined;
    participants[0] = "owner";
    participants[1] = "failed";
    var initialized: usize = 2;
    defer for (participants[2..initialized]) |participant| alloc.free(participant);
    for (participants[2..], 0..) |*participant, i| {
        participant.* = try std.fmt.allocPrint(alloc, "member-{d}", .{i});
        initialized += 1;
    }
    for ([_]Recorder.Mode{ .success, .unsupported, .unknown, .committed_reply_loss }) |mode| {
        var backend = mem_backend.Backend.init(alloc, .{});
        defer backend.close();
        var runtime_store = try backend.runtimeStore(alloc, .{});
        defer runtime_store.deinit();
        var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
        defer manager.deinit();
        const txn: transactions_mod.TxnId = @splat(37);
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(txn, 1000, 1000, &participants, true, true);
        try manager.resolveIntents(txn, .committed, 2000);
        var recorder: Recorder = .{ .manager = &manager, .mode = mode };
        var clock: platform_clock.ManualClock = .{};
        clock.setRealtimeNs(5000);
        const stats = try recoverOnce(alloc, &runtime_store, .{
            .enabled = true,
            .clock = clock.clock(),
            .cutoff_ns = 3000,
            .resolver_ctx = &recorder,
            .replicated_metadata = true,
            .owns_recovery_fn = Recorder.owns,
            .resolve_participant_fn = Recorder.resolve,
            .acknowledge_participant_fn = Recorder.single,
            .acknowledge_participants_fn = Recorder.many,
            .cleanup_transaction_fn = Recorder.cleanup,
        });
        try std.testing.expectEqual(@as(usize, 3), recorder.batches);
        try std.testing.expectEqual(@as(usize, if (mode == .unsupported) 130 else 0), recorder.singles);
        try std.testing.expectEqual(@as(usize, 0), recorder.cleanups);
        try std.testing.expectEqual(@as(u64, if (mode == .unknown or mode == .committed_reply_loss) 131 else 1), stats.notification_failures);
        const unresolved = try manager.getUnresolvedParticipants(alloc, txn);
        defer transactions_mod.freeParticipantList(alloc, unresolved);
        try std.testing.expectEqual(@as(usize, if (mode == .unknown) 132 else 2), unresolved.len);
        try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try manager.getTransactionStatus(txn));
    }
}

pub fn runDbRecoveryOnce(self: *@import("db/db.zig").DB, config: Config) !types.TransactionRecoveryStats {
    var replication_mutation = try self.admitTransactionRecovery();
    defer if (replication_mutation) |*lease| lease.release();
    if (!config.enabled) return .{};
    if (config.replicated_metadata) {
        return try recoverOnce(
            self.alloc,
            self.core.batchExecutionResources().store,
            config,
        );
    }
    const resolve_participant = config.resolve_participant_fn orelse return error.MissingParticipantResolver;
    const resolver_ctx = config.resolver_ctx orelse return error.MissingParticipantResolver;
    const now_ns = config.clock.nowRealtimeNs();
    const resolved_finalized = try self.recoverFinalizedTransactionIntents(now_ns);

    var recovery_stats: types.TransactionRecoveryStats = .{
        .enabled = true,
        .lease_owned = config.lease_owned,
        .runs = 1,
        .resolved_finalized = resolved_finalized,
        .last_run_ns = now_ns,
    };

    // Notification may perform network I/O or route back to this DB. Keep
    // it entirely outside the apply lock and acknowledge each successful
    // delivery with a separate short, idempotent locked update.
    const txns = try self.core.listTransactions(self.alloc);
    defer self.alloc.free(txns);
    for (txns) |txn| {
        if (txn.status == .pending) continue;
        const unresolved = self.core.getUnresolvedTransactionParticipants(self.alloc, txn.txn_id) catch |err| switch (err) {
            transactions_mod.TxnError.TxnNotFound => continue,
            else => return err,
        };
        defer transactions_mod.freeParticipantList(self.alloc, unresolved);
        var acknowledged: [64][]const u8 = undefined;
        var acknowledged_count: usize = 0;
        for (unresolved) |participant| {
            recovery_stats.notification_attempts += 1;
            const is_local = if (config.local_participant) |local| std.mem.eql(u8, local, participant) else false;
            if (!is_local) resolve_participant(resolver_ctx, txn.txn_id, participant, txn.status, txn.commit_version) catch {
                recovery_stats.notification_failures += 1;
                continue;
            };
            acknowledged[acknowledged_count] = participant;
            acknowledged_count += 1;
            if (acknowledged_count == acknowledged.len) {
                self.markTransactionParticipantsResolved(txn.txn_id, acknowledged[0..acknowledged_count]) catch |err| switch (err) {
                    transactions_mod.TxnError.TxnNotFound => {},
                    else => return err,
                };
                recovery_stats.notification_successes += acknowledged_count;
                acknowledged_count = 0;
            }
        }
        if (acknowledged_count != 0) {
            self.markTransactionParticipantsResolved(txn.txn_id, acknowledged[0..acknowledged_count]) catch |err| switch (err) {
                transactions_mod.TxnError.TxnNotFound => {},
                else => return err,
            };
            recovery_stats.notification_successes += acknowledged_count;
        }
    }

    // Serialize presumed-abort and metadata cleanup with prepare/resolve,
    // but do not hold the lock across participant callbacks above.
    const local_stats = try self.recoverLocalTransactionRecords(now_ns -| config.cutoff_ns, now_ns);
    recovery_stats.scanned_records = local_stats.scanned_records;
    recovery_stats.auto_aborted = local_stats.auto_aborted;
    recovery_stats.resolved_finalized += local_stats.resolved_finalized;
    recovery_stats.cleaned_records = local_stats.cleaned_records;
    recovery_stats.kept_recent_pending = local_stats.kept_recent_pending;
    recovery_stats.deferred_unresolved = local_stats.deferred_unresolved;
    return recovery_stats;
}

const local_contract = @import("db/transaction_recovery_contract.zig");

/// Config and its callback contexts must outlive DB initialization. The
/// constructed runtime snapshots Config and retains only its borrowed contexts.
pub fn borrowedConfig(config: *const Config) local_contract.Config {
    return .{ .enabled = config.enabled, .factory = .{ .ptr = @constCast(config), .create = createBorrowedRuntime } };
}
fn createBorrowedRuntime(ptr: *anyopaque, alloc: Allocator, store: backend_erased.Store, background: *background_runtime_mod.BackendRuntime, context: local_contract.CreateContext) !local_contract.OwnedRuntime {
    const config: *const Config = @ptrCast(@alignCast(ptr));
    return try createOwnedRuntime(config.*, alloc, store, background, context);
}

/// Keep configuration construction in the server owner whose borrowed context
/// already outlives the DB. No server DTO or coordinator policy enters DB.
pub fn configFor(comptime Context: type, ptr: *Context, comptime get: anytype) local_contract.Config {
    const Adapter = struct {
        fn create(raw: *anyopaque, alloc: Allocator, store: backend_erased.Store, background: *background_runtime_mod.BackendRuntime, context: local_contract.CreateContext) !local_contract.OwnedRuntime {
            const owner: *Context = @ptrCast(@alignCast(raw));
            return try createOwnedRuntime(get(owner), alloc, store, background, context);
        }
    };
    return .{ .enabled = get(ptr).enabled, .factory = .{ .ptr = ptr, .create = Adapter.create } };
}
fn createOwnedRuntime(config: Config, alloc: Allocator, store: backend_erased.Store, background: *background_runtime_mod.BackendRuntime, context: local_contract.CreateContext) !local_contract.OwnedRuntime {
    var effective = config;
    effective.resolution_extra_hooks = context.resolution_extra_hooks;
    effective.local_resolution_ctx = context.local_resolution_ctx;
    effective.resolve_local_fn = context.resolve_local_fn;
    const owner = try alloc.create(OwnedServerRuntime);
    errdefer alloc.destroy(owner);
    // The engine owns this adapter; Runtime snapshots the borrowed handle.
    var borrowed_store = store;
    owner.* = .{ .alloc = alloc, .runtime = try Runtime.init(alloc, &borrowed_store, background, effective) };
    return .{ .ptr = owner, .vtable = &OwnedServerRuntime.vtable };
}
const OwnedServerRuntime = struct {
    alloc: Allocator,
    runtime: Runtime,
    fn owner(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn deinit(ptr: *anyopaque) void {
        const self = owner(ptr);
        const alloc = self.alloc;
        self.runtime.deinit();
        alloc.destroy(self);
    }
    fn start(ptr: *anyopaque) anyerror!void {
        return try owner(ptr).runtime.start();
    }
    fn stop(ptr: *anyopaque) bool {
        return owner(ptr).runtime.stop();
    }
    fn pause(ptr: *anyopaque) bool {
        return owner(ptr).runtime.pause();
    }
    fn resumeAfterPause(ptr: *anyopaque) anyerror!void {
        return try owner(ptr).runtime.resumeAfterPause();
    }
    fn ensureRunning(ptr: *anyopaque) anyerror!bool {
        return try owner(ptr).runtime.ensureRunning();
    }
    fn isStarted(ptr: *anyopaque) bool {
        return owner(ptr).runtime.isStarted();
    }
    fn teardown(ptr: *anyopaque) void {
        return owner(ptr).runtime.beginTeardown();
    }
    fn stats(ptr: *anyopaque) types.TransactionRecoveryStats {
        return owner(ptr).runtime.stats();
    }
    fn runOnce(ptr: *anyopaque) anyerror!void {
        return try owner(ptr).runtime.runOnce();
    }
    const vtable: local_contract.OwnedRuntime.VTable = .{
        .deinit = deinit,
        .start = start,
        .stop = stop,
        .pause = pause,
        .resume_after_pause = resumeAfterPause,
        .ensure_running = ensureRunning,
        .is_started = isStarted,
        .teardown = teardown,
        .stats = stats,
        .run_once = runOnce,
    };
};

/// Inspect the owned server configuration only in server integration fixtures.
pub const test_support = if (builtin.is_test) struct {
    pub fn runtimeConfig(runtime: *@import("db/maintenance/transaction_runtime.zig").Runtime) Config {
        return OwnedServerRuntime.owner(runtime.external.?.ptr).runtime.config;
    }
} else struct {};
