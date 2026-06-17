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
    inline for (ha_promotion_response_fields) |name| {
        try expectFacadeStructField(HAPromotionResponse, name);
    }
    inline for (ha_promotion_result_fields) |name| {
        try expectFacadeStructField(HAPromotionResult, name);
    }
    inline for (ha_rejoin_assess_response_fields) |name| {
        try expectFacadeStructField(HARejoinAssessResponse, name);
    }
    inline for (ha_rejoin_rewind_result_fields) |name| {
        try expectFacadeStructField(HARejoinRewindResult, name);
    }
    inline for (ha_rejoin_reseed_result_fields) |name| {
        try expectFacadeStructField(HARejoinReseedResult, name);
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

fn expectFacadeTypeAlias(comptime name: []const u8) !void {
    try std.testing.expect(@hasDecl(@This(), name));
    try std.testing.expect(@hasDecl(openapi, name));
    try std.testing.expect(@field(@This(), name) == @field(openapi, name));
}

fn expectFacadeStructField(comptime T: type, comptime field_name: []const u8) !void {
    try std.testing.expect(@typeInfo(T) == .@"struct");
    try std.testing.expect(@hasField(T, field_name));
}
