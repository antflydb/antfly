// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

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

/// Process-local execution context established by the receiving node. This is
/// never serialized directly across the wire.
pub const PreDecisionContext = struct {
    deadline_ns: ?u64 = null,
    cancellation: db_types.CancellationToken = .none,
};

pub const TableCommitRequest = struct {
    table_name: []const u8,
    writes: []const db_types.TransactionWrite = &.{},
    deletes: []const []const u8 = &.{},
    transforms: []const db_types.DocumentTransform = &.{},
    predicates: []const db_types.TransactionVersionPredicate = &.{},
};

pub const CommitConflict = struct {
    table_name: []const u8,
    key: []const u8,
    message: []const u8,
    group_id: ?u64 = null,
    phase: ?ParticipantPhase = null,
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
