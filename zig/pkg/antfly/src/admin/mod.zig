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
pub const HACommandResult = openapi.HACommandResult;
pub const HAActionReceipt = openapi.HAActionReceipt;

test {
    _ = openapi;
    _ = routes;
    _ = ServerRouter;
    _ = HAPromotionResponse;
    _ = HARejoinAssessResponse;
    _ = HAActionReceipt;
}
