// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Data-only transaction envelope used by the runtime callback boundary.

const std = @import("std");
const db_types = @import("../storage/db/types.zig");

/// A pre-decision participant may be tried at another replica only when the
/// responding node proves that it did not admit the mutation. A dedicated
/// transaction header keeps that proof distinct from routed batch forwarding
/// and lets clients fail closed across proxies and rolling upgrades.
pub const pre_decision_outcome_header = "X-Antfly-Txn-Pre-Decision-Outcome";
pub const pre_decision_not_proposed_v1 = "not-proposed-v1";
/// Relative server-side budget. Monotonic clocks are process-local, so the
/// coordinator sends a duration and ingress establishes the absolute deadline
/// before authentication and request dispatch consume it.
pub const pre_decision_remaining_ms_header = "X-Antfly-Txn-Pre-Decision-Remaining-Ms";
pub const max_pre_decision_server_budget_ms: u32 = 5_000;
pub const pre_decision_server_response_reserve_ms: u32 = 50;
/// Relative budget for a linearizable participant-status observation. The
/// receiving leader applies this same budget to its quorum read barrier.
pub const status_remaining_ms_header = "X-Antfly-Txn-Status-Remaining-Ms";
pub const max_status_server_budget_ms: u32 = 5_000;
/// Time retained by the caller after the server's read-barrier budget for
/// response encoding, scheduling, and transport back to the coordinator.
pub const status_server_response_reserve_ms: u32 = 50;

/// Process-local execution context established by the receiving node. This is
/// never serialized directly across the wire.
pub const PreDecisionContext = struct {
    sql_locks: ?@import("../storage/pessimistic_lock_transaction.zig").Ref = null,
    external_decision_owner: ?@import("../storage/transactions.zig").ExternalDecisionOwner = null,
    route_fence: ?@import("../metadata/catalog_route_contract.zig").CatalogRouteFence = null,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
    deadline_ns: ?u64 = null,
    deadline_io: ?@import("antfly_runtime_abi").io_abi.Borrow = null,
    cancellation: db_types.CancellationToken = .none,
};

/// Authenticated private recovery observation. A locator is never authority by
/// itself: hidden owner handlers validate the exact plan and scope together.
pub const TxnStatusRequest = struct {
    route_fence: ?@import("../metadata/catalog_route_contract.zig").CatalogRouteFence = null,
    external_decision_owner: ?@import("../storage/transactions.zig").ExternalDecisionOwner = null,
    txn_id: db_types.TxnId,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

/// Resolution retains its exact authority through routing and native callbacks.
pub const TxnResolveRequest = struct {
    route_fence: ?@import("../metadata/catalog_route_contract.zig").CatalogRouteFence = null,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
    txn_id: db_types.TxnId,
    status: db_types.TxnStatus,
    commit_version: u64,
    topology_epoch: u64 = 0,
    sync_level: db_types.SyncLevel = .propose,
    external_decision_owner: ?@import("../storage/transactions.zig").ExternalDecisionOwner = null,
};

pub const TableCommitRequest = struct {
    graph_preparation: ?@import("../storage/db/prepared_graph_effects.zig").Admission = null,
    /// Immutable processed rows, retained by protected SQL/session staging.
    /// Public ingress rejects this private preparation evidence.
    trigger_receipts: []const @import("../sql/trigger_handoff.zig").Receipt = &.{},
    /// Process-long API signer; never serialized into durable or public data.
    trigger_minter: ?@import("../common/trigger_preparation_authority.zig").Minter = null,
    /// Server-resolved physical owner epoch. Public JSON cannot enlist a
    /// replacement native generation by supplying a table name alone.
    relation_owner_minter: ?@import("../common/relation_owner_authority.zig").Minter = null,
    sql_persistence: ?@import("../common/relation_persistence.zig").Descriptor = null,
    row_policy_principal_proof: []const u8 = "",
    row_policy_database: []const u8 = "",
    row_policy_admitted_at_seconds: i64 = 0,
    range_guards: []const @import("range_read_guards.zig").OwnerRangeProof = &.{},
    schema_version: ?u32 = null,
    table_name: []const u8,
    relational_schema_version: ?u32 = null,
    relational_integrity_generation_set: ?[32]u8 = null,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
    relational_repair: bool = false,
    writes: []const db_types.TransactionWrite = &.{},
    deletes: []const []const u8 = &.{},
    transforms: []const db_types.DocumentTransform = &.{},
    graph_writes: []const db_types.GraphEdgeWrite = &.{},
    graph_deletes: []const db_types.GraphEdgeDelete = &.{},
    predicates: []const db_types.TransactionVersionPredicate = &.{},
    integrity: []const db_types.TransactionIntegrityOperation = &.{},
    integrity_commands: []const @import("../storage/db/relational_integrity_contract.zig").Command = &.{},
    relational_activation: ?@import("../storage/db/relational_integrity_activation_contract.zig").Command = null,
    relational_retirement: ?@import("../storage/db/relational_integrity_retirement_contract.zig").Command = null,
    relational_index_maintenance: ?@import("../storage/db/relational_index_maintenance_contract.zig").Command = null,
    pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (comptime nativeJsonSkipFieldComptime(field)) continue;
            if (!self.nativeJsonSkipField(field)) {
                try stream.objectField(field);
                try stream.write(@field(self, field));
            }
        }
        try stream.endObject();
    }
    pub fn nativeJsonSkipFieldComptime(comptime field: []const u8) bool {
        return std.mem.eql(u8, field, "relation_owner_minter") or std.mem.eql(u8, field, "trigger_minter");
    }
    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return ((comptime std.mem.eql(u8, field, "graph_preparation")) and self.graph_preparation == null) or (comptime nativeJsonSkipFieldComptime(field)) or ((comptime std.mem.eql(u8, field, "sql_persistence")) and self.sql_persistence == null) or ((comptime std.mem.eql(u8, field, "graph_writes")) and self.graph_writes.len == 0) or ((comptime std.mem.eql(u8, field, "graph_deletes")) and self.graph_deletes.len == 0);
    }
};

pub const CommitConflict = struct {
    table_name: []const u8,
    key: []const u8,
    message: []const u8,
    group_id: ?u64 = null,
    phase: ?ParticipantPhase = null,
    reason: ?CommitConflictReason = null,
    retryable: bool = false,
};

pub const CommitConflictReason = enum {
    unique_constraint_violation,
    foreign_key_parent_missing,
    foreign_key_referenced,
};

pub const ParticipantPhase = enum {
    begin,
    prepare,
    resolve,
};

pub const ExecuteResult = struct {
    participant_count: usize,
    /// Stable sessions persist their terminal result before acknowledging the
    /// coordinator itself. These coordinates identify that durable decision
    /// record without recomputing it from mutable table routing.
    coordinator_group_id: ?u64 = null,
    coordinator_table_name: ?[]const u8 = null,
    /// The commit decision is durable, but at least one participant still
    /// needs phase-two delivery by foreground retry or recovery.
    propagation_pending: bool = false,
    /// Participant writes are durable, but the requested visibility barrier
    /// was not reached before the response was produced.
    visibility_pending: bool = false,
    /// The visibility barrier can still complete without operator repair.
    /// Kept separate from terminal repair debt so a mixed outcome cannot
    /// prematurely release stable transaction recovery.
    visibility_retry_pending: bool = false,
    /// The visibility barrier is pending because enrichment reached a
    /// terminal worker failure. This is a strict subset of
    /// `visibility_pending` and tells clients to repair rather than poll.
    visibility_repair_required: bool = false,
};

pub const CommitOutcome = union(enum) {
    committed: ExecuteResult,
    conflict: CommitConflict,
};
