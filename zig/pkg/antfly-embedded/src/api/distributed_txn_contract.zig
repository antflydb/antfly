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
    route_fence: ?@import("../metadata/catalog_route_contract.zig").CatalogRouteFence = null,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
    deadline_ns: ?u64 = null,
    deadline_io: ?@import("antfly_runtime_abi").io_abi.Borrow = null,
    cancellation: db_types.CancellationToken = .none,
    /// Per-call worker authority retained through this synchronous participant
    /// operation. Concurrent participants require independently owned children;
    /// never forward the coordinator token to multiple workers or serialize it.
    execution_runnable: ?@import("antfly_runtime_abi").runnable_abi.Borrowed = null,

    /// Clamp an existing budget into the destination clock domain. Sampling
    /// that clock first prevents translation from extending the caller's
    /// remaining time. Preserve all identity and cancellation fences.
    pub fn boundDeadline(self: PreDecisionContext, ceiling_ns: u64, clock_io: ?@import("antfly_runtime_abi").io_abi.Borrow) PreDecisionContext {
        const routing = @import("routing_budget.zig");
        const target: routing.RoutingBudget = .{ .io = clock_io };
        var result = self;
        result.deadline_ns = @min(ceiling_ns, target.deadlineFrom(.{ .deadline_ns = self.deadline_ns, .io = self.deadline_io }) orelse ceiling_ns);
        result.deadline_io = clock_io;
        return result;
    }

    /// A legacy callback cannot carry these admission fences or hidden-owner
    /// identity. Even an inactive/incomplete supplied token must not be silently
    /// discarded: its caller requires this context-aware operation contract.
    pub fn requiresAwareCallback(self: PreDecisionContext) bool {
        return self.route_fence != null or self.restore_staging_scope != null or self.restore_staging_plan_id != null or
            self.deadline_ns != null or self.deadline_io != null or self.execution_runnable != null or self.cancellation.ptr != null or
            self.cancellation.is_cancelled_fn != null or self.cancellation.check_fn != null;
    }
};

/// Lease-fenced session storage for the original routed cohort. Bytes are
/// private, versioned coordinator data, never a caller-supplied routing hint.
/// Returned buffers belong to the invocation allocator.
pub const ParticipantPlanStore = struct {
    pub const Loaded = union(enum) { legacy, unplanned, planned: []u8 };
    ptr: *anyopaque,
    load: *const fn (*anyopaque, @import("std").mem.Allocator, db_types.TxnId) anyerror!Loaded,
    seal: *const fn (*anyopaque, @import("std").mem.Allocator, db_types.TxnId, []const u8) anyerror![]u8,
};

/// Invocation-owned commit authority. The original pre-decision fences are
/// separate from post-commit response cancellation; neither may replace the
/// independent authority needed to finish or recover accepted mutations.
pub const CommitControl = struct {
    team: ?*@import("antfly_runtime_abi").runnable_abi.Team = null,
    pre_decision: PreDecisionContext = .{},
    participant_plan_store: ?ParticipantPlanStore = null,

    pub fn requiresAwareCallback(self: CommitControl) bool {
        return self.team != null or self.participant_plan_store != null or self.pre_decision.requiresAwareCallback();
    }

    pub fn fromRequest(request: @import("operation.zig").RequestContext) CommitControl {
        return .{
            .team = request.execution_team,
            .pre_decision = .{
                .deadline_ns = request.deadline_ns,
                .deadline_io = request.deadline_io,
                .cancellation = request.cancellation,
            },
        };
    }
};

/// Authenticated private recovery observation. A locator is never authority by
/// itself: hidden owner handlers validate the exact plan and scope together.
pub const TxnStatusRequest = struct {
    txn_id: db_types.TxnId,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

pub const TableCommitRequest = struct {
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
    predicates: []const db_types.TransactionVersionPredicate = &.{},
    integrity: []const db_types.TransactionIntegrityOperation = &.{},
    integrity_commands: []const @import("../storage/db/relational_integrity_contract.zig").Command = &.{},
    relational_activation: ?@import("../storage/db/relational_integrity_activation_contract.zig").Command = null,
    relational_retirement: ?@import("../storage/db/relational_integrity_retirement_contract.zig").Command = null,
    relational_index_maintenance: ?@import("../storage/db/relational_index_maintenance_contract.zig").Command = null,
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
