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
const platform_clock = @import("antfly_platform").clock;
const transactions_mod = @import("transactions.zig");

pub const ResolveParticipantFn = *const fn (*anyopaque, transactions_mod.TxnId, []const u8, transactions_mod.TxnStatus, u64) anyerror!void;

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
    resolve_participant_fn: ?ResolveParticipantFn = null,
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
    /// Optional bounded coordinator ACK capability. Unsupported is a
    /// pre-admission feature result; uncertain delivery must retain debt.
    acknowledge_participants_fn: ?*const fn (
        ctx: *anyopaque,
        txn_id: transactions_mod.TxnId,
        owner_participant: []const u8,
        participants: []const []const u8,
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
