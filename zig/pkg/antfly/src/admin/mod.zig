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

pub const openapi = @import("antfly_admin_openapi");
pub const routes = @import("routes.zig");

pub const types = openapi.types;
pub const server = openapi.server;
pub const ServerRouter = openapi.ServerRouter;

pub const ReplicationSlotCreateRequest = openapi.ReplicationSlotCreateRequest;
pub const BaseBackupStartRequest = openapi.BaseBackupStartRequest;
pub const BaseBackupManifestPathRequest = openapi.BaseBackupManifestPathRequest;
pub const StandbyBootstrapRequest = openapi.StandbyBootstrapRequest;
pub const HASyncPolicy = openapi.HASyncPolicy;
pub const CommitCheckRequest = openapi.CommitCheckRequest;
pub const CommitAppendRequest = openapi.CommitAppendRequest;
pub const ReadCheckRequest = openapi.ReadCheckRequest;
pub const WriteCheckRequest = openapi.WriteCheckRequest;
pub const OwnerJobCheckRequest = openapi.OwnerJobCheckRequest;
pub const HAIdentity = openapi.HAIdentity;
pub const FenceAcquireRequest = openapi.FenceAcquireRequest;
pub const HAFenceReceipt = openapi.HAFenceReceipt;
pub const PromotionAssessRequest = openapi.PromotionAssessRequest;
pub const RejoinAssessRequest = openapi.RejoinAssessRequest;

pub const HAPrimaryStatusResponse = openapi.HAPrimaryStatusResponse;
pub const HAStandbyStatusResponse = openapi.HAStandbyStatusResponse;
pub const HACommitCheckResponse = openapi.HACommitCheckResponse;
pub const HACommitAppendResponse = openapi.HACommitAppendResponse;
pub const HAReadCheckResponse = openapi.HAReadCheckResponse;
pub const HAWriteCheckResponse = openapi.HAWriteCheckResponse;
pub const HAOwnerJobCheckResponse = openapi.HAOwnerJobCheckResponse;
pub const HAReplicationSlotActionResponse = openapi.HAReplicationSlotActionResponse;
pub const HAReplicationSlotListResponse = openapi.HAReplicationSlotListResponse;
pub const HABaseBackupBeginResponse = openapi.HABaseBackupBeginResponse;
pub const HABaseBackupFinishResponse = openapi.HABaseBackupFinishResponse;
pub const HAStandbyBootstrapResponse = openapi.HAStandbyBootstrapResponse;
pub const HAFenceResponse = openapi.HAFenceResponse;
pub const HACurrentFenceResponse = openapi.HACurrentFenceResponse;
pub const HAPromotionAssessResponse = openapi.HAPromotionAssessResponse;
pub const HAPromotionResponse = openapi.HAPromotionResponse;
pub const HARejoinAssessResponse = openapi.HARejoinAssessResponse;
pub const HARejoinRewindResult = openapi.HARejoinRewindResult;
pub const HARejoinReseedResult = openapi.HARejoinReseedResult;

pub const HAPromotionAssessment = openapi.HAPromotionAssessment;
pub const HAPromotionResult = openapi.HAPromotionResult;
pub const HARejoinAssessment = openapi.HARejoinAssessment;
pub const HAPrimarySnapshot = openapi.HAPrimarySnapshot;
pub const HAStandbySnapshot = openapi.HAStandbySnapshot;
pub const HASlotSnapshot = openapi.HASlotSnapshot;
pub const HARetentionSnapshot = openapi.HARetentionSnapshot;
pub const HADurabilityDecision = openapi.HADurabilityDecision;
pub const HAReadDecision = openapi.HAReadDecision;
pub const HAPromotionHandoff = openapi.HAPromotionHandoff;
pub const HAWriteDecision = openapi.HAWriteDecision;
pub const HAOwnerJobDecision = openapi.HAOwnerJobDecision;
pub const HACommitGate = openapi.HACommitGate;
pub const HAReplicationSlot = openapi.HAReplicationSlot;
pub const HAActionReceipt = openapi.HAActionReceipt;
pub const CommitAppendRequestKind = openapi.CommitAppendRequestKind;
pub const CommitAppendRequestPayloadCodec = openapi.CommitAppendRequestPayloadCodec;
pub const HACommitGateAction = openapi.HACommitGateAction;
pub const HADurabilityDecisionMode = openapi.HADurabilityDecisionMode;
pub const HADurabilityDecisionSelection = openapi.HADurabilityDecisionSelection;
pub const HADurabilityDecisionStatus = openapi.HADurabilityDecisionStatus;
pub const HAActionReceiptActionKind = openapi.HAActionReceiptActionKind;
pub const HAActionReceiptState = openapi.HAActionReceiptState;
pub const HAOwnerJobDecisionKind = openapi.HAOwnerJobDecisionKind;
pub const HAOwnerJobDecisionRole = openapi.HAOwnerJobDecisionRole;
pub const HAPrimarySnapshotRole = openapi.HAPrimarySnapshotRole;
pub const HAPromotionAssessmentMode = openapi.HAPromotionAssessmentMode;
pub const HAReadDecisionAction = openapi.HAReadDecisionAction;
pub const HAReadDecisionConsistency = openapi.HAReadDecisionConsistency;
pub const HARejoinAssessmentAction = openapi.HARejoinAssessmentAction;
pub const HARejoinAssessmentReason = openapi.HARejoinAssessmentReason;
pub const HAReplicationSlotActionResponseSlotAction = openapi.HAReplicationSlotActionResponseSlotAction;
pub const HASlotSnapshotStatus = openapi.HASlotSnapshotStatus;
pub const HAStandbySnapshotRole = openapi.HAStandbySnapshotRole;
pub const HASyncPolicyMode = openapi.HASyncPolicyMode;
pub const HASyncPolicySelection = openapi.HASyncPolicySelection;
pub const HASyncPolicyFailurePolicy = openapi.HASyncPolicyFailurePolicy;
pub const HAWriteDecisionAction = openapi.HAWriteDecisionAction;
pub const HAWriteDecisionRole = openapi.HAWriteDecisionRole;
pub const HAOwnerJobDecisionAction = openapi.HAOwnerJobDecisionAction;
pub const OwnerJobCheckRequestRole = openapi.OwnerJobCheckRequestRole;
pub const OwnerJobCheckRequestKind = openapi.OwnerJobCheckRequestKind;
pub const WriteCheckRequestRole = openapi.WriteCheckRequestRole;

const std = @import("std");

test {
    _ = openapi;
    _ = routes;
    _ = ServerRouter;
    _ = HAPromotionResponse;
    _ = HARejoinAssessResponse;
    _ = HARejoinRewindResult;
    _ = HARejoinReseedResult;
    _ = HAActionReceipt;
}

test "admin facade mirrors generated HA OpenAPI contract types" {
    inline for (ha_contract_type_names) |name| {
        try expectFacadeTypeAlias(name);
    }
}

test "admin facade re-exports HA action receipt result types" {
    const receipt_info = @typeInfo(HAActionReceipt);
    const rewind_info = @typeInfo(HARejoinRewindResult);
    const reseed_info = @typeInfo(HARejoinReseedResult);

    try std.testing.expect(receipt_info == .@"struct");
    try std.testing.expect(@hasField(HAActionReceipt, "node_id"));
    try std.testing.expect(rewind_info == .@"struct");
    try std.testing.expect(reseed_info == .@"struct");
    try std.testing.expect(@hasField(HARejoinAssessResponse, "action"));
    try std.testing.expect(@hasField(HARejoinAssessResponse, "rewind"));
    try std.testing.expect(@hasField(HARejoinAssessResponse, "reseed"));
}

test "admin facade preserves HA failover receipt schema fields" {
    inline for (ha_action_receipt_fields) |name| {
        try expectFacadeStructField(HAActionReceipt, name);
    }
    inline for (ha_promotion_assessment_fields) |name| {
        try expectFacadeStructField(HAPromotionAssessment, name);
    }
    inline for (ha_promotion_assess_response_fields) |name| {
        try expectFacadeStructField(HAPromotionAssessResponse, name);
    }
    inline for (ha_promotion_response_fields) |name| {
        try expectFacadeStructField(HAPromotionResponse, name);
    }
    inline for (ha_promotion_result_fields) |name| {
        try expectFacadeStructField(HAPromotionResult, name);
    }
    inline for (ha_rejoin_assess_response_fields) |name| {
        try expectFacadeStructField(HARejoinAssessResponse, name);
    }
    inline for (ha_rejoin_assessment_fields) |name| {
        try expectFacadeStructField(HARejoinAssessment, name);
    }
    inline for (ha_rejoin_rewind_result_fields) |name| {
        try expectFacadeStructField(HARejoinRewindResult, name);
    }
    inline for (ha_rejoin_reseed_result_fields) |name| {
        try expectFacadeStructField(HARejoinReseedResult, name);
    }
}

test "admin facade preserves HA status and gate schema fields" {
    inline for (ha_primary_status_response_fields) |name| {
        try expectFacadeStructField(HAPrimaryStatusResponse, name);
    }
    inline for (ha_standby_status_response_fields) |name| {
        try expectFacadeStructField(HAStandbyStatusResponse, name);
    }
    inline for (ha_primary_snapshot_fields) |name| {
        try expectFacadeStructField(HAPrimarySnapshot, name);
    }
    inline for (ha_standby_snapshot_fields) |name| {
        try expectFacadeStructField(HAStandbySnapshot, name);
    }
    inline for (ha_slot_snapshot_fields) |name| {
        try expectFacadeStructField(HASlotSnapshot, name);
    }
    inline for (ha_retention_snapshot_fields) |name| {
        try expectFacadeStructField(HARetentionSnapshot, name);
    }
    inline for (ha_durability_decision_fields) |name| {
        try expectFacadeStructField(HADurabilityDecision, name);
    }
    inline for (ha_commit_check_response_fields) |name| {
        try expectFacadeStructField(HACommitCheckResponse, name);
    }
    inline for (ha_commit_append_response_fields) |name| {
        try expectFacadeStructField(HACommitAppendResponse, name);
    }
    inline for (ha_commit_gate_fields) |name| {
        try expectFacadeStructField(HACommitGate, name);
    }
    inline for (ha_read_check_response_fields) |name| {
        try expectFacadeStructField(HAReadCheckResponse, name);
    }
    inline for (ha_read_decision_fields) |name| {
        try expectFacadeStructField(HAReadDecision, name);
    }
    inline for (ha_write_check_response_fields) |name| {
        try expectFacadeStructField(HAWriteCheckResponse, name);
    }
    inline for (ha_write_decision_fields) |name| {
        try expectFacadeStructField(HAWriteDecision, name);
    }
    inline for (ha_owner_job_check_response_fields) |name| {
        try expectFacadeStructField(HAOwnerJobCheckResponse, name);
    }
    inline for (ha_owner_job_decision_fields) |name| {
        try expectFacadeStructField(HAOwnerJobDecision, name);
    }
    inline for (ha_promotion_handoff_fields) |name| {
        try expectFacadeStructField(HAPromotionHandoff, name);
    }
}

test "admin facade preserves HA slot seed and fence schema fields" {
    inline for (ha_identity_fields) |name| {
        try expectFacadeStructField(HAIdentity, name);
    }
    inline for (ha_replication_slot_fields) |name| {
        try expectFacadeStructField(HAReplicationSlot, name);
    }
    inline for (ha_replication_slot_action_response_fields) |name| {
        try expectFacadeStructField(HAReplicationSlotActionResponse, name);
    }
    inline for (ha_replication_slot_list_response_fields) |name| {
        try expectFacadeStructField(HAReplicationSlotListResponse, name);
    }
    inline for (ha_base_backup_begin_response_fields) |name| {
        try expectFacadeStructField(HABaseBackupBeginResponse, name);
    }
    inline for (ha_base_backup_finish_response_fields) |name| {
        try expectFacadeStructField(HABaseBackupFinishResponse, name);
    }
    inline for (ha_standby_bootstrap_response_fields) |name| {
        try expectFacadeStructField(HAStandbyBootstrapResponse, name);
    }
    inline for (ha_fence_receipt_fields) |name| {
        try expectFacadeStructField(HAFenceReceipt, name);
    }
    inline for (ha_fence_response_fields) |name| {
        try expectFacadeStructField(HAFenceResponse, name);
    }
    inline for (ha_current_fence_response_fields) |name| {
        try expectFacadeStructField(HACurrentFenceResponse, name);
    }
}

const ha_contract_type_names = [_][]const u8{
    "ReplicationSlotCreateRequest",
    "BaseBackupStartRequest",
    "BaseBackupManifestPathRequest",
    "StandbyBootstrapRequest",
    "HASyncPolicy",
    "CommitCheckRequest",
    "CommitAppendRequest",
    "ReadCheckRequest",
    "WriteCheckRequest",
    "OwnerJobCheckRequest",
    "HAIdentity",
    "FenceAcquireRequest",
    "HAFenceReceipt",
    "PromotionAssessRequest",
    "RejoinAssessRequest",
    "HAPrimaryStatusResponse",
    "HAStandbyStatusResponse",
    "HACommitCheckResponse",
    "HACommitAppendResponse",
    "HAReadCheckResponse",
    "HAWriteCheckResponse",
    "HAOwnerJobCheckResponse",
    "HAReplicationSlotActionResponse",
    "HAReplicationSlotListResponse",
    "HABaseBackupBeginResponse",
    "HABaseBackupFinishResponse",
    "HAStandbyBootstrapResponse",
    "HAFenceResponse",
    "HACurrentFenceResponse",
    "HAPromotionAssessResponse",
    "HAPromotionResponse",
    "HARejoinAssessResponse",
    "HARejoinRewindResult",
    "HARejoinReseedResult",
    "HAPromotionAssessment",
    "HAPromotionResult",
    "HARejoinAssessment",
    "HAPrimarySnapshot",
    "HAStandbySnapshot",
    "HASlotSnapshot",
    "HARetentionSnapshot",
    "HADurabilityDecision",
    "HAReadDecision",
    "HAPromotionHandoff",
    "HAWriteDecision",
    "HAOwnerJobDecision",
    "HACommitGate",
    "HAReplicationSlot",
    "HAActionReceipt",
};

const ha_action_receipt_fields = [_][]const u8{
    "action_id",
    "action_kind",
    "target",
    "state",
    "node_id",
};

const ha_promotion_assessment_fields = [_][]const u8{
    "required_lsn",
    "received_lsn",
    "applied_lsn",
    "has_required_lsn",
    "caught_up_to_received",
    "fencing_confirmed",
    "force",
    "mode",
    "data_loss_possible",
    "safe",
    "requires_fencing",
    "requires_force",
    "can_promote",
};

const ha_promotion_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "assessment",
    "promotion",
    "fence_generation",
    "fence_token",
    "forced",
};

const ha_promotion_assess_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "assessment",
};

const ha_promotion_result_fields = [_][]const u8{
    "node_id",
    "switch_lsn",
    "old_identity",
    "new_identity",
    "forced",
    "data_loss_possible",
};

const ha_rejoin_assess_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "assessment",
    "rewind",
    "reseed",
};

const ha_rejoin_rewind_result_fields = [_][]const u8{
    "node_id",
    "fork_lsn",
    "previous_last_lsn",
    "current_last_lsn",
    "next_lsn",
    "discarded_lsn_count",
    "target_timeline_id",
    "target_epoch",
    "data_loss_discarded",
};

const ha_rejoin_reseed_result_fields = [_][]const u8{
    "node_id",
    "slot_name",
    "target_timeline_id",
    "target_epoch",
    "fork_lsn",
    "former_last_lsn",
    "reseed_required",
    "base_backup_required",
};

const ha_primary_status_response_fields = [_][]const u8{
    "schema_version",
    "snapshot",
};

const ha_standby_status_response_fields = [_][]const u8{
    "schema_version",
    "snapshot",
};

const ha_primary_snapshot_fields = [_][]const u8{
    "role",
    "node_id",
    "identity",
    "current_lsn",
    "slots",
    "retention",
    "durability",
};

const ha_standby_snapshot_fields = [_][]const u8{
    "role",
    "node_id",
    "identity",
    "received_lsn",
    "applied_lsn",
    "safe_read_lsn",
    "upstream_lsn",
    "write_lag_lsn",
    "receive_lag_lsn",
    "apply_lag_lsn",
    "last_error",
    "last_attempt_ns",
    "last_success_ns",
    "replication_failures_total",
    "unapplied_lsn_count",
    "caught_up_to_received",
    "can_serve_safe_reads",
};

const ha_slot_snapshot_fields = [_][]const u8{
    "name",
    "timeline_id",
    "active",
    "reseed_required",
    "restart_lsn",
    "received_lsn",
    "applied_lsn",
    "safe_read_lsn",
    "write_lag_lsn",
    "apply_lag_lsn",
    "safe_read_lag_lsn",
    "retention_lag_lsn",
    "status",
    "last_error",
};

const ha_retention_snapshot_fields = [_][]const u8{
    "primary_lsn",
    "oldest_restart_lsn",
    "retained_lsn_count",
    "retained_byte_count",
    "retained_age_ns",
    "active_slots",
    "reseed_recommended",
};

const ha_durability_decision_fields = [_][]const u8{
    "status",
    "mode",
    "selection",
    "target_lsn",
    "progress_lsn",
    "missing_lsn_count",
    "satisfied_count",
    "required_count",
    "candidate_count",
};

const ha_commit_check_response_fields = [_][]const u8{
    "schema_version",
    "gate",
};

const ha_commit_append_response_fields = [_][]const u8{
    "schema_version",
    "lsn",
    "gate",
};

const ha_commit_gate_fields = [_][]const u8{
    "target_lsn",
    "action",
    "durability",
};

const ha_read_check_response_fields = [_][]const u8{
    "schema_version",
    "decision",
};

const ha_read_decision_fields = [_][]const u8{
    "action",
    "consistency",
    "required_lsn",
    "required_metadata_lsn",
    "received_lsn",
    "applied_lsn",
    "safe_read_lsn",
    "metadata_applied_lsn",
    "serve_lsn",
    "missing_lsn_count",
    "metadata_missing_lsn_count",
};

const ha_write_check_response_fields = [_][]const u8{
    "schema_version",
    "decision",
};

const ha_write_decision_fields = [_][]const u8{
    "role",
    "action",
    "identity",
    "durable_lsn",
    "next_lsn",
    "promotion_handoff",
};

const ha_owner_job_check_response_fields = [_][]const u8{
    "schema_version",
    "decision",
};

const ha_owner_job_decision_fields = [_][]const u8{
    "kind",
    "role",
    "action",
    "identity",
    "durable_lsn",
    "next_lsn",
    "promotion_handoff",
};

const ha_promotion_handoff_fields = [_][]const u8{
    "identity",
    "switch_lsn",
    "next_lsn",
};

const ha_identity_fields = [_][]const u8{
    "cluster_id",
    "shard_id",
    "table_id",
    "timeline_id",
    "epoch",
};

const ha_replication_slot_fields = [_][]const u8{
    "slot_name",
    "timeline_id",
    "restart_lsn",
    "received_lsn",
    "applied_lsn",
    "safe_read_lsn",
    "active",
    "reseed_required",
    "last_error",
    "current_lsn",
    "dropped",
};

const ha_replication_slot_action_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "slot_action",
    "slot",
};

const ha_replication_slot_list_response_fields = [_][]const u8{
    "schema_version",
    "slots",
};

const ha_base_backup_begin_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "slot_name",
    "manifest_id",
    "backup_lsn",
    "start_record_lsn",
};

const ha_base_backup_finish_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "manifest_id",
    "backup_lsn",
    "end_record_lsn",
};

const ha_standby_bootstrap_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "manifest_id",
    "backup_lsn",
    "checkpoint_lsn",
};

const ha_fence_receipt_fields = [_][]const u8{
    "identity",
    "old_primary_id",
    "promoted_node_id",
    "parent_timeline_id",
    "parent_epoch",
    "new_timeline_id",
    "new_epoch",
    "required_lsn",
    "observed_lsn",
    "generation",
    "forced",
    "token",
    "reason",
};

const ha_fence_response_fields = [_][]const u8{
    "schema_version",
    "action",
    "receipt",
};

const ha_rejoin_assessment_fields = [_][]const u8{
    "action",
    "reason",
    "former_node_id",
    "target_timeline_id",
    "target_epoch",
    "parent_cluster_id",
    "parent_shard_id",
    "parent_table_id",
    "parent_timeline_id",
    "parent_epoch",
    "fork_lsn",
    "former_last_lsn",
    "retained_from_lsn",
    "data_loss_discarded",
};

const ha_current_fence_response_fields = [_][]const u8{
    "schema_version",
    "held",
    "receipt",
};

fn expectFacadeTypeAlias(comptime name: []const u8) !void {
    try std.testing.expect(@hasDecl(@This(), name));
    try std.testing.expect(@hasDecl(openapi, name));
    try std.testing.expect(@field(@This(), name) == @field(openapi, name));
}

fn expectFacadeStructField(comptime T: type, comptime field_name: []const u8) !void {
    try std.testing.expect(@typeInfo(T) == .@"struct");
    try std.testing.expect(@hasField(T, field_name));
}
