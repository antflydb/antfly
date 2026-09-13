// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
const std = @import("std");
const platform_clock = @import("antfly_platform").clock;
const resolution_mod = @import("transaction_resolution.zig");
const transactions_mod = @import("../transactions.zig");

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
