package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sdk/admin/oapi"
)

const (
	AdminV1Path                       = "/admin/v1"
	HAPath                            = AdminV1Path + "/ha"
	HAPrimaryStatusPath               = HAPath + "/primary/status"
	HAStandbyStatusPath               = HAPath + "/standby/status"
	HACommitCheckPath                 = HAPath + "/commit/check"
	HACommitAppendPath                = HAPath + "/commit/append"
	HAReadCheckPath                   = HAPath + "/read/check"
	HAWriteCheckPath                  = HAPath + "/write/check"
	HAOwnerJobCheckPath               = HAPath + "/owner-jobs/check"
	HAReplicationSlotsPath            = HAPath + "/replication-slots"
	HAReplicationSlotPathPrefix       = HAReplicationSlotsPath + "/"
	HAReplicationSlotPausePathSuffix  = "/pause"
	HAReplicationSlotResumePathSuffix = "/resume"
	HABaseBackupsPath                 = HAPath + "/base-backups"
	HABaseBackupsFinishPath           = HABaseBackupsPath + "/finish"
	HAStandbyBootstrapPath            = HAPath + "/standby/bootstrap"
	HAFencePath                       = HAPath + "/fence"
	HAFenceCurrentPath                = HAFencePath + "/current"
	HAPromotionPath                   = HAPath + "/promotion"
	HAPromotionAssessPath             = HAPath + "/promotion/assess"
	HAPromotionCurrentFencePath       = HAPath + "/promotion/current-fence"
	HARejoinAssessPath                = HAPath + "/rejoin/assess"
	HARejoinRewindPath                = HAPath + "/rejoin/rewind"
	HARejoinReseedPath                = HAPath + "/rejoin/reseed"
)

const adminV1Path = AdminV1Path

type (
	HAActionReceipt                    = oapi.HAActionReceipt
	HAActionReceiptActionKind          = oapi.HAActionReceiptActionKind
	HAActionReceiptState               = oapi.HAActionReceiptState
	HABaseBackupBeginResponse          = oapi.HABaseBackupBeginResponse
	HABaseBackupFinishResponse         = oapi.HABaseBackupFinishResponse
	HACommitAppendResponse             = oapi.HACommitAppendResponse
	HACommitCheckResponse              = oapi.HACommitCheckResponse
	HACommitGate                       = oapi.HACommitGate
	HACommitGateAction                 = oapi.HACommitGateAction
	HACurrentFenceResponse             = oapi.HACurrentFenceResponse
	HADurabilityDecision               = oapi.HADurabilityDecision
	HADurabilityDecisionMode           = oapi.HADurabilityDecisionMode
	HADurabilityDecisionSelection      = oapi.HADurabilityDecisionSelection
	HADurabilityDecisionStatus         = oapi.HADurabilityDecisionStatus
	HAFenceReceipt                     = oapi.HAFenceReceipt
	HAFenceResponse                    = oapi.HAFenceResponse
	HAIdentity                         = oapi.HAIdentity
	HAOwnerJobCheckResponse            = oapi.HAOwnerJobCheckResponse
	HAOwnerJobDecision                 = oapi.HAOwnerJobDecision
	HAOwnerJobDecisionAction           = oapi.HAOwnerJobDecisionAction
	HAOwnerJobDecisionKind             = oapi.HAOwnerJobDecisionKind
	HAOwnerJobDecisionRole             = oapi.HAOwnerJobDecisionRole
	HAPrimarySnapshot                  = oapi.HAPrimarySnapshot
	HAPrimarySnapshotRole              = oapi.HAPrimarySnapshotRole
	HAPrimaryStatusParams              = oapi.GetHAPrimaryStatusParams
	HAPrimaryStatusParamsSyncMode      = oapi.GetHAPrimaryStatusParamsSyncMode
	HAPrimaryStatusParamsSyncSelection = oapi.GetHAPrimaryStatusParamsSyncSelection
	HAPrimaryStatusParamsSyncFail      = oapi.GetHAPrimaryStatusParamsSyncFailure
	HAPrimaryStatusResponse            = oapi.HAPrimaryStatusResponse
	HAPromotionAssessResponse          = oapi.HAPromotionAssessResponse
	HAPromotionAssessment              = oapi.HAPromotionAssessment
	HAPromotionHandoff                 = oapi.HAPromotionHandoff
	HAPromotionResponse                = oapi.HAPromotionResponse
	HAPromotionResult                  = oapi.HAPromotionResult
	HAReadCheckResponse                = oapi.HAReadCheckResponse
	HAReadDecision                     = oapi.HAReadDecision
	HAReadDecisionAction               = oapi.HAReadDecisionAction
	HAReadDecisionConsistency          = oapi.HAReadDecisionConsistency
	HARejoinAssessResponse             = oapi.HARejoinAssessResponse
	HARejoinAssessment                 = oapi.HARejoinAssessment
	HARejoinAssessmentAction           = oapi.HARejoinAssessmentAction
	HARejoinAssessmentReason           = oapi.HARejoinAssessmentReason
	HARejoinReseedResult               = oapi.HARejoinReseedResult
	HARejoinRewindResult               = oapi.HARejoinRewindResult
	HAReplicationSlot                  = oapi.HAReplicationSlot
	HAReplicationSlotActionResponse    = oapi.HAReplicationSlotActionResponse
	HAReplicationSlotAction            = oapi.HAReplicationSlotActionResponseSlotAction
	HAReplicationSlotListResponse      = oapi.HAReplicationSlotListResponse
	HARetentionSnapshot                = oapi.HARetentionSnapshot
	HASlotSnapshot                     = oapi.HASlotSnapshot
	HASlotSnapshotStatus               = oapi.HASlotSnapshotStatus
	HAStandbySnapshot                  = oapi.HAStandbySnapshot
	HAStandbySnapshotRole              = oapi.HAStandbySnapshotRole
	HAStandbyBootstrapResponse         = oapi.HAStandbyBootstrapResponse
	HAStandbyStatusParams              = oapi.GetHAStandbyStatusParams
	HAStandbyStatusResponse            = oapi.HAStandbyStatusResponse
	HASyncPolicy                       = oapi.HASyncPolicy
	HASyncPolicyFailurePolicy          = oapi.HASyncPolicyFailurePolicy
	HASyncPolicyMode                   = oapi.HASyncPolicyMode
	HASyncPolicySelection              = oapi.HASyncPolicySelection
	HAWriteCheckResponse               = oapi.HAWriteCheckResponse
	HAWriteDecision                    = oapi.HAWriteDecision
	HAWriteDecisionAction              = oapi.HAWriteDecisionAction
	HAWriteDecisionRole                = oapi.HAWriteDecisionRole

	BaseBackupManifestPathRequest = oapi.BaseBackupManifestPathRequest
	BaseBackupStartRequest        = oapi.BaseBackupStartRequest
	CommitAppendRequest           = oapi.CommitAppendRequest
	CommitAppendRequestKind       = oapi.CommitAppendRequestKind
	CommitAppendRequestCodec      = oapi.CommitAppendRequestPayloadCodec
	CommitCheckRequest            = oapi.CommitCheckRequest
	FenceAcquireRequest           = oapi.FenceAcquireRequest
	OwnerJobCheckRequest          = oapi.OwnerJobCheckRequest
	OwnerJobCheckRequestKind      = oapi.OwnerJobCheckRequestKind
	OwnerJobCheckRequestRole      = oapi.OwnerJobCheckRequestRole
	PromotionAssessRequest        = oapi.PromotionAssessRequest
	ReadCheckRequest              = oapi.ReadCheckRequest
	ReadCheckRequestConsistency   = oapi.ReadCheckRequestConsistency
	RejoinAssessRequest           = oapi.RejoinAssessRequest
	ReplicationSlotCreateRequest  = oapi.ReplicationSlotCreateRequest
	StandbyBootstrapRequest       = oapi.StandbyBootstrapRequest
	WriteCheckRequest             = oapi.WriteCheckRequest
	WriteCheckRequestRole         = oapi.WriteCheckRequestRole
)

const (
	HAActionKindBaseBackupBegin       = oapi.HAActionReceiptActionKindBaseBackupBegin
	HAActionKindBaseBackupFinish      = oapi.HAActionReceiptActionKindBaseBackupFinish
	HAActionKindFenceAcquire          = oapi.HAActionReceiptActionKindFenceAcquire
	HAActionKindPromotion             = oapi.HAActionReceiptActionKindPromotion
	HAActionKindPromotionAssess       = oapi.HAActionReceiptActionKindPromotionAssess
	HAActionKindRejoinAssess          = oapi.HAActionReceiptActionKindRejoinAssess
	HAActionKindRejoinReseed          = oapi.HAActionReceiptActionKindRejoinReseed
	HAActionKindRejoinRewind          = oapi.HAActionReceiptActionKindRejoinRewind
	HAActionKindReplicationSlotCreate = oapi.HAActionReceiptActionKindReplicationSlotCreate
	HAActionKindReplicationSlotDrop   = oapi.HAActionReceiptActionKindReplicationSlotDrop
	HAActionKindReplicationSlotPause  = oapi.HAActionReceiptActionKindReplicationSlotPause
	HAActionKindReplicationSlotResume = oapi.HAActionReceiptActionKindReplicationSlotResume
	HAActionKindStandbyBootstrap      = oapi.HAActionReceiptActionKindStandbyBootstrap

	HAActionStateAlreadyApplied = oapi.HAActionReceiptStateAlreadyApplied
	HAActionStateApplied        = oapi.HAActionReceiptStateApplied
	HAActionStateAssessed       = oapi.HAActionReceiptStateAssessed

	HAPrimarySnapshotRolePrimary = oapi.HAPrimarySnapshotRolePrimary

	HAStandbySnapshotRoleStandby = oapi.HAStandbySnapshotRoleStandby

	HASlotSnapshotStatusHealthy        = oapi.HASlotSnapshotStatusHealthy
	HASlotSnapshotStatusLagging        = oapi.HASlotSnapshotStatusLagging
	HASlotSnapshotStatusReseedRequired = oapi.HASlotSnapshotStatusReseedRequired

	HADurabilityStatusSatisfied       = oapi.HADurabilityDecisionStatusSatisfied
	HADurabilityStatusWouldBlock      = oapi.HADurabilityDecisionStatusWouldBlock
	HADurabilityStatusFailClosed      = oapi.HADurabilityDecisionStatusFailClosed
	HADurabilityStatusDegradedToAsync = oapi.HADurabilityDecisionStatusDegradedToAsync

	HADurabilityModeAsync       = oapi.HADurabilityDecisionModeAsync
	HADurabilityModeRemoteWrite = oapi.HADurabilityDecisionModeRemoteWrite
	HADurabilityModeRemoteApply = oapi.HADurabilityDecisionModeRemoteApply

	HADurabilitySelectionAny   = oapi.HADurabilityDecisionSelectionAny
	HADurabilitySelectionFirst = oapi.HADurabilityDecisionSelectionFirst
	HADurabilitySelectionAll   = oapi.HADurabilityDecisionSelectionAll

	HAPrimaryStatusSyncModeAsync       = oapi.GetHAPrimaryStatusParamsSyncModeAsync
	HAPrimaryStatusSyncModeRemoteWrite = oapi.GetHAPrimaryStatusParamsSyncModeRemoteWrite
	HAPrimaryStatusSyncModeRemoteApply = oapi.GetHAPrimaryStatusParamsSyncModeRemoteApply

	HAPrimaryStatusSyncSelectionAny   = oapi.GetHAPrimaryStatusParamsSyncSelectionAny
	HAPrimaryStatusSyncSelectionFirst = oapi.GetHAPrimaryStatusParamsSyncSelectionFirst
	HAPrimaryStatusSyncSelectionAll   = oapi.GetHAPrimaryStatusParamsSyncSelectionAll

	HAPrimaryStatusSyncFailureBlock          = oapi.GetHAPrimaryStatusParamsSyncFailureBlock
	HAPrimaryStatusSyncFailureFailClosed     = oapi.GetHAPrimaryStatusParamsSyncFailureFailClosed
	HAPrimaryStatusSyncFailureDegradeToAsync = oapi.GetHAPrimaryStatusParamsSyncFailureDegradeToAsync

	HASyncPolicyModeAsync       = oapi.HASyncPolicyModeAsync
	HASyncPolicyModeRemoteWrite = oapi.HASyncPolicyModeRemoteWrite
	HASyncPolicyModeRemoteApply = oapi.HASyncPolicyModeRemoteApply

	HASyncPolicySelectionAny   = oapi.HASyncPolicySelectionAny
	HASyncPolicySelectionFirst = oapi.HASyncPolicySelectionFirst
	HASyncPolicySelectionAll   = oapi.HASyncPolicySelectionAll

	HASyncPolicyFailureBlock          = oapi.HASyncPolicyFailurePolicyBlock
	HASyncPolicyFailureFailClosed     = oapi.HASyncPolicyFailurePolicyFailClosed
	HASyncPolicyFailureDegradeToAsync = oapi.HASyncPolicyFailurePolicyDegradeToAsync

	HACommitGateActionAcknowledge         = oapi.HACommitGateActionAcknowledge
	HACommitGateActionAcknowledgeDegraded = oapi.HACommitGateActionAcknowledgeDegraded
	HACommitGateActionReject              = oapi.HACommitGateActionReject
	HACommitGateActionWaitForStandby      = oapi.HACommitGateActionWaitForStandby

	HAReadDecisionActionRouteToPrimary  = oapi.HAReadDecisionActionRouteToPrimary
	HAReadDecisionActionServeStandby    = oapi.HAReadDecisionActionServeStandby
	HAReadDecisionActionWaitForApply    = oapi.HAReadDecisionActionWaitForApply
	HAReadDecisionActionWaitForMetadata = oapi.HAReadDecisionActionWaitForMetadata

	HAReadDecisionConsistencyAtLeastLSN = oapi.HAReadDecisionConsistencyAtLeastLsn
	HAReadDecisionConsistencyPrimary    = oapi.HAReadDecisionConsistencyPrimary
	HAReadDecisionConsistencyStaleOK    = oapi.HAReadDecisionConsistencyStaleOk

	HAWriteDecisionActionAllowWrite          = oapi.HAWriteDecisionActionAllowWrite
	HAWriteDecisionActionOpenPromotedPrimary = oapi.HAWriteDecisionActionOpenPromotedPrimary
	HAWriteDecisionActionRejectReadOnly      = oapi.HAWriteDecisionActionRejectReadOnlyStandby

	HAWriteDecisionRolePrimary         = oapi.HAWriteDecisionRolePrimary
	HAWriteDecisionRolePromotedStandby = oapi.HAWriteDecisionRolePromotedStandby
	HAWriteDecisionRoleStandby         = oapi.HAWriteDecisionRoleStandby

	HAOwnerJobDecisionActionDisableOnStandby    = oapi.HAOwnerJobDecisionActionDisableOnStandby
	HAOwnerJobDecisionActionOpenPromotedPrimary = oapi.HAOwnerJobDecisionActionOpenPromotedPrimary
	HAOwnerJobDecisionActionRun                 = oapi.HAOwnerJobDecisionActionRun

	HAOwnerJobDecisionKindCompactionPublish   = oapi.HAOwnerJobDecisionKindCompactionPublish
	HAOwnerJobDecisionKindDerivedEffectWriter = oapi.HAOwnerJobDecisionKindDerivedEffectWriter
	HAOwnerJobDecisionKindEnrichmentWriter    = oapi.HAOwnerJobDecisionKindEnrichmentWriter
	HAOwnerJobDecisionKindRetentionAdvance    = oapi.HAOwnerJobDecisionKindRetentionAdvance

	HAOwnerJobDecisionRolePrimary         = oapi.HAOwnerJobDecisionRolePrimary
	HAOwnerJobDecisionRolePromotedStandby = oapi.HAOwnerJobDecisionRolePromotedStandby
	HAOwnerJobDecisionRoleStandby         = oapi.HAOwnerJobDecisionRoleStandby

	HARejoinActionAlreadyCurrent = oapi.HARejoinAssessmentActionAlreadyCurrent
	HARejoinActionRejectUnfenced = oapi.HARejoinAssessmentActionRejectUnfenced
	HARejoinActionReseed         = oapi.HARejoinAssessmentActionReseed
	HARejoinActionRewind         = oapi.HARejoinAssessmentActionRewind

	HARejoinReasonCurrentTimeline          = oapi.HARejoinAssessmentReasonCurrentTimeline
	HARejoinReasonIncompatibleTimeline     = oapi.HARejoinAssessmentReasonIncompatibleTimeline
	HARejoinReasonLocalLSNBeforeFork       = oapi.HARejoinAssessmentReasonLocalLsnBeforeFork
	HARejoinReasonNoFence                  = oapi.HARejoinAssessmentReasonNoFence
	HARejoinReasonParentTimelineRetained   = oapi.HARejoinAssessmentReasonParentTimelineRetained
	HARejoinReasonParentTimelineWALExpired = oapi.HARejoinAssessmentReasonParentTimelineWalExpired
	HARejoinReasonWrongCluster             = oapi.HARejoinAssessmentReasonWrongCluster
	HARejoinReasonWrongOldPrimary          = oapi.HARejoinAssessmentReasonWrongOldPrimary
	HARejoinReasonWrongShard               = oapi.HARejoinAssessmentReasonWrongShard
	HARejoinReasonWrongTable               = oapi.HARejoinAssessmentReasonWrongTable

	HAReplicationSlotActionCreate = oapi.HAReplicationSlotActionResponseSlotActionCreate
	HAReplicationSlotActionDrop   = oapi.HAReplicationSlotActionResponseSlotActionDrop
	HAReplicationSlotActionPause  = oapi.HAReplicationSlotActionResponseSlotActionPause
	HAReplicationSlotActionResume = oapi.HAReplicationSlotActionResponseSlotActionResume

	CommitAppendKindBatchMutation    = oapi.CommitAppendRequestKindBatchMutation
	CommitAppendKindMetadataMutation = oapi.CommitAppendRequestKindMetadataMutation
	CommitAppendKindDerivedEffect    = oapi.CommitAppendRequestKindDerivedEffect
	CommitAppendKindTimelineSwitch   = oapi.CommitAppendRequestKindTimelineSwitch
	CommitAppendKindBackupStart      = oapi.CommitAppendRequestKindBackupStart
	CommitAppendKindBackupEnd        = oapi.CommitAppendRequestKindBackupEnd
	CommitAppendKindCheckpoint       = oapi.CommitAppendRequestKindCheckpoint
	CommitAppendKindManifest         = oapi.CommitAppendRequestKindManifest
	CommitAppendKindTruncate         = oapi.CommitAppendRequestKindTruncate

	CommitAppendCodecRaw    = oapi.CommitAppendRequestPayloadCodecRaw
	CommitAppendCodecJSON   = oapi.CommitAppendRequestPayloadCodecJson
	CommitAppendCodecBinary = oapi.CommitAppendRequestPayloadCodecBinary

	ReadCheckConsistencyStaleOK    = oapi.ReadCheckRequestConsistencyStaleOk
	ReadCheckConsistencyAtLeastLSN = oapi.ReadCheckRequestConsistencyAtLeastLsn
	ReadCheckConsistencyPrimary    = oapi.ReadCheckRequestConsistencyPrimary

	WriteCheckRolePrimary = oapi.WriteCheckRequestRolePrimary
	WriteCheckRoleStandby = oapi.WriteCheckRequestRoleStandby

	OwnerJobCheckKindCompactionPublish   = oapi.OwnerJobCheckRequestKindCompactionPublish
	OwnerJobCheckKindRetentionAdvance    = oapi.OwnerJobCheckRequestKindRetentionAdvance
	OwnerJobCheckKindDerivedEffectWriter = oapi.OwnerJobCheckRequestKindDerivedEffectWriter
	OwnerJobCheckKindEnrichmentWriter    = oapi.OwnerJobCheckRequestKindEnrichmentWriter

	OwnerJobCheckRolePrimary = oapi.OwnerJobCheckRequestRolePrimary
	OwnerJobCheckRoleStandby = oapi.OwnerJobCheckRequestRoleStandby
)

// HAClient is a typed client for the stable /admin/v1/ha API.
type HAClient struct {
	client  *oapi.ClientWithResponses
	editors []oapi.RequestEditorFn
}

// HAOperation identifies a stable /admin/v1/ha method and full admin path.
// Operator status and automation should use these values rather than carrying a
// separate route table outside the admin SDK wrapper.
type HAOperation struct {
	Method string
	Path   string
}

// HAReceiptExpectation identifies the generated action receipt kind/state a
// successful idempotent admin operation should return.
type HAReceiptExpectation struct {
	ActionKind HAActionReceiptActionKind
	State      HAActionReceiptState
}

func (e HAReceiptExpectation) Strings() (string, string) {
	return string(e.ActionKind), string(e.State)
}

// HAReceiptMatches verifies that a node-local HA admin receipt matches the
// expected operation and acted-on target.
func HAReceiptMatches(receipt HAActionReceipt, expectation HAReceiptExpectation, expectedTarget string) bool {
	actionID := strings.TrimSpace(receipt.ActionId)
	actionKind := strings.TrimSpace(string(receipt.ActionKind))
	actionTarget := strings.TrimSpace(receipt.Target)
	actionState := strings.TrimSpace(string(receipt.State))
	expectedKind := strings.TrimSpace(string(expectation.ActionKind))
	expectedTarget = strings.TrimSpace(expectedTarget)
	expectedState := strings.TrimSpace(string(expectation.State))
	if actionID == "" ||
		actionKind == "" ||
		actionTarget == "" ||
		actionState == "" ||
		expectedKind == "" ||
		expectedTarget == "" ||
		expectedState == "" {
		return false
	}
	if actionKind != expectedKind || actionTarget != expectedTarget || actionState != expectedState {
		if !(expectedState == string(HAActionStateApplied) && actionState == string(HAActionStateAlreadyApplied)) {
			return false
		}
	}
	return actionID == expectedKind+":"+expectedTarget
}

// HAReceiptNodeMatches verifies that the node-local endpoint that returned a
// receipt is the intended endpoint. Some compatibility paths can tolerate an
// unset expected node id, but typed direct admin execution should require it.
func HAReceiptNodeMatches(receipt HAActionReceipt, expectedNodeID string, requireExpectedNode bool) bool {
	nodeID := strings.TrimSpace(receipt.NodeId)
	if nodeID == "" {
		return false
	}
	expectedNodeID = strings.TrimSpace(expectedNodeID)
	if expectedNodeID == "" {
		return !requireExpectedNode
	}
	return nodeID == expectedNodeID
}

func HAReceiptMatchesNode(receipt HAActionReceipt, expectation HAReceiptExpectation, expectedTarget string, expectedNodeID string, requireExpectedNode bool) bool {
	return HAReceiptMatches(receipt, expectation, expectedTarget) &&
		HAReceiptNodeMatches(receipt, expectedNodeID, requireExpectedNode)
}

func ValidateHAReplicationSlotActionResponse(response HAReplicationSlotActionResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing replication slot action schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing replication slot action receipt")
	}
	switch response.SlotAction {
	case HAReplicationSlotActionCreate, HAReplicationSlotActionDrop, HAReplicationSlotActionPause, HAReplicationSlotActionResume:
	default:
		return fmt.Errorf("invalid replication slot action %q", response.SlotAction)
	}
	if !HAReplicationSlotComplete(response.Slot) {
		return fmt.Errorf("missing replication slot action slot fields")
	}
	return nil
}

func ValidateHAReplicationSlotListResponse(response HAReplicationSlotListResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing replication slot list schema_version")
	}
	for _, slot := range response.Slots {
		if !HAReplicationSlotComplete(slot) {
			return fmt.Errorf("missing replication slot list slot fields")
		}
	}
	return nil
}

func ValidateHABaseBackupBeginResponse(response HABaseBackupBeginResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing base backup begin schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing base backup begin action receipt")
	}
	if strings.TrimSpace(response.SlotName) == "" {
		return fmt.Errorf("missing base backup begin slot_name")
	}
	if strings.TrimSpace(response.ManifestId) == "" {
		return fmt.Errorf("missing base backup begin manifest_id")
	}
	if response.BackupLsn == 0 {
		return fmt.Errorf("missing base backup begin backup_lsn")
	}
	if response.StartRecordLsn == 0 {
		return fmt.Errorf("missing base backup begin start_record_lsn")
	}
	return nil
}

func ValidateHABaseBackupFinishResponse(response HABaseBackupFinishResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing base backup finish schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing base backup finish action receipt")
	}
	if strings.TrimSpace(response.ManifestId) == "" {
		return fmt.Errorf("missing base backup finish manifest_id")
	}
	if response.BackupLsn == 0 {
		return fmt.Errorf("missing base backup finish backup_lsn")
	}
	if response.EndRecordLsn == 0 {
		return fmt.Errorf("missing base backup finish end_record_lsn")
	}
	return nil
}

func ValidateHAStandbyBootstrapResponse(response HAStandbyBootstrapResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing standby bootstrap schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing standby bootstrap action receipt")
	}
	if strings.TrimSpace(response.ManifestId) == "" {
		return fmt.Errorf("missing standby bootstrap manifest_id")
	}
	if response.BackupLsn == 0 {
		return fmt.Errorf("missing standby bootstrap backup_lsn")
	}
	if response.CheckpointLsn == 0 {
		return fmt.Errorf("missing standby bootstrap checkpoint_lsn")
	}
	return nil
}

func ValidateHAPrimaryStatusResponse(response HAPrimaryStatusResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing primary status schema_version")
	}
	snapshot := response.Snapshot
	if snapshot.Role != HAPrimarySnapshotRolePrimary {
		return fmt.Errorf("invalid primary status role %q", snapshot.Role)
	}
	if !HAIdentityComplete(snapshot.Identity) {
		return fmt.Errorf("missing primary status identity fields")
	}
	if err := validateHAPrimaryRetentionSnapshot(snapshot.Retention, snapshot.CurrentLsn, len(snapshot.Slots)); err != nil {
		return err
	}
	for _, slot := range snapshot.Slots {
		if err := validateHASlotSnapshot(slot, snapshot.CurrentLsn); err != nil {
			return err
		}
	}
	if !HADurabilityDecisionEmpty(snapshot.Durability) {
		if err := validateHADurabilityDecision(snapshot.Durability); err != nil {
			return err
		}
	}
	return nil
}

func ValidateHAStandbyStatusResponse(response HAStandbyStatusResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing standby status schema_version")
	}
	snapshot := response.Snapshot
	if snapshot.Role != HAStandbySnapshotRoleStandby {
		return fmt.Errorf("invalid standby status role %q", snapshot.Role)
	}
	if !HAIdentityComplete(snapshot.Identity) {
		return fmt.Errorf("missing standby status identity fields")
	}
	if snapshot.AppliedLsn > snapshot.ReceivedLsn {
		return fmt.Errorf("standby status inconsistent: applied_lsn=%d exceeds received_lsn=%d", snapshot.AppliedLsn, snapshot.ReceivedLsn)
	}
	if snapshot.SafeReadLsn > snapshot.AppliedLsn {
		return fmt.Errorf("standby status inconsistent: safe_read_lsn=%d exceeds applied_lsn=%d", snapshot.SafeReadLsn, snapshot.AppliedLsn)
	}
	if snapshot.UnappliedLsnCount != snapshot.ReceivedLsn-snapshot.AppliedLsn {
		return fmt.Errorf("standby status inconsistent: unapplied_lsn_count=%d expected=%d", snapshot.UnappliedLsnCount, snapshot.ReceivedLsn-snapshot.AppliedLsn)
	}
	if snapshot.CaughtUpToReceived != (snapshot.AppliedLsn >= snapshot.ReceivedLsn) {
		return fmt.Errorf("standby status inconsistent: caught_up_to_received=%t with applied_lsn=%d received_lsn=%d", snapshot.CaughtUpToReceived, snapshot.AppliedLsn, snapshot.ReceivedLsn)
	}
	if snapshot.CanServeSafeReads != (snapshot.SafeReadLsn <= snapshot.AppliedLsn) {
		return fmt.Errorf("standby status inconsistent: can_serve_safe_reads=%t with safe_read_lsn=%d applied_lsn=%d", snapshot.CanServeSafeReads, snapshot.SafeReadLsn, snapshot.AppliedLsn)
	}
	if snapshot.UpstreamLsn > 0 || snapshot.WriteLagLsn > 0 || snapshot.ReceiveLagLsn > 0 || snapshot.ApplyLagLsn > 0 {
		if snapshot.WriteLagLsn != haSaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn) {
			return fmt.Errorf("standby status inconsistent: write_lag_lsn=%d expected=%d", snapshot.WriteLagLsn, haSaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn))
		}
		if snapshot.ReceiveLagLsn != haSaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn) {
			return fmt.Errorf("standby status inconsistent: receive_lag_lsn=%d expected=%d", snapshot.ReceiveLagLsn, haSaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn))
		}
		if snapshot.ApplyLagLsn != haSaturatingSub(snapshot.UpstreamLsn, snapshot.AppliedLsn) {
			return fmt.Errorf("standby status inconsistent: apply_lag_lsn=%d expected=%d", snapshot.ApplyLagLsn, haSaturatingSub(snapshot.UpstreamLsn, snapshot.AppliedLsn))
		}
	}
	return nil
}

func ValidateHACommitCheckResponse(response HACommitCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing commit check schema_version")
	}
	if !HACommitGateComplete(response.Gate) {
		return fmt.Errorf("missing commit check gate fields")
	}
	return nil
}

func ValidateHACommitAppendResponse(response HACommitAppendResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing commit append schema_version")
	}
	if !HACommitGateComplete(response.Gate) {
		return fmt.Errorf("missing commit append gate fields")
	}
	return nil
}

func ValidateHAReadCheckResponse(response HAReadCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing read check schema_version")
	}
	if !HAReadDecisionComplete(response.Decision) {
		return fmt.Errorf("missing read decision fields")
	}
	return nil
}

func ValidateHAWriteCheckResponse(response HAWriteCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing write check schema_version")
	}
	if !HAWriteDecisionComplete(response.Decision) {
		return fmt.Errorf("missing write decision fields")
	}
	return nil
}

func ValidateHAOwnerJobCheckResponse(response HAOwnerJobCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing owner job check schema_version")
	}
	if !HAOwnerJobDecisionComplete(response.Decision) {
		return fmt.Errorf("missing owner job decision fields")
	}
	return nil
}

func ValidateHAFenceResponse(response HAFenceResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing fence response schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing fence response action receipt")
	}
	if !HAFenceReceiptComplete(response.Receipt) {
		return fmt.Errorf("missing fence response receipt fields")
	}
	return nil
}

func ValidateHAFenceResponseEvidence(raw []byte) error {
	var response haFenceResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !haFenceReceiptEvidenceComplete(response.Receipt) {
		return fmt.Errorf("missing fence response receipt field evidence")
	}
	return nil
}

func ValidateHACurrentFenceResponse(response HACurrentFenceResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing current fence schema_version")
	}
	if response.Held {
		if !HAFenceReceiptComplete(response.Receipt) {
			return fmt.Errorf("missing current fence receipt fields")
		}
		return nil
	}
	if !HAFenceReceiptEmpty(response.Receipt) {
		return fmt.Errorf("current fence response has receipt while not held")
	}
	return nil
}

func ValidateHACurrentFenceResponseEvidence(raw []byte) error {
	var response haCurrentFenceResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.Held == nil {
		return fmt.Errorf("missing current fence held field evidence")
	}
	if *response.Held && !haFenceReceiptEvidenceComplete(response.Receipt) {
		return fmt.Errorf("missing current fence receipt field evidence")
	}
	return nil
}

func ValidateHAPromotionAssessResponse(response HAPromotionAssessResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing promotion assess schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing promotion assess action receipt")
	}
	if !HAPromotionAssessmentComplete(response.Assessment) {
		return fmt.Errorf("missing promotion assessment fields")
	}
	return nil
}

func ValidateHAPromotionAssessResponseEvidence(raw []byte) error {
	var response haPromotionAssessResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !haPromotionAssessmentEvidenceComplete(response.Assessment) {
		return fmt.Errorf("missing promotion assessment field evidence")
	}
	return nil
}

func ValidateHAPromotionResponse(response HAPromotionResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing promotion response schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing promotion response action receipt")
	}
	if !HAPromotionAssessmentComplete(response.Assessment) {
		return fmt.Errorf("missing promotion response assessment fields")
	}
	if !HAPromotionResultComplete(response.Promotion) {
		return fmt.Errorf("missing promotion result fields")
	}
	if response.FenceGeneration == 0 {
		return fmt.Errorf("missing promotion fence_generation")
	}
	if strings.TrimSpace(response.FenceToken) == "" {
		return fmt.Errorf("missing promotion fence_token")
	}
	return nil
}

func ValidateHAPromotionResponseEvidence(raw []byte) error {
	var response haPromotionResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.Forced == nil {
		return fmt.Errorf("missing promotion forced field evidence")
	}
	if !haPromotionAssessmentEvidenceComplete(response.Assessment) {
		return fmt.Errorf("missing promotion assessment field evidence")
	}
	if !haPromotionResultEvidenceComplete(response.Promotion) {
		return fmt.Errorf("missing promotion result field evidence")
	}
	return nil
}

func ValidateHARejoinAssessResponse(response HARejoinAssessResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing rejoin response schema_version")
	}
	if !HAActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing rejoin response action receipt")
	}
	if !HARejoinAssessmentComplete(response.Assessment) {
		return fmt.Errorf("missing rejoin assessment fields")
	}
	switch response.Assessment.Action {
	case HARejoinActionRewind:
		if !HARejoinRewindComplete(response.Rewind) {
			return fmt.Errorf("missing rejoin rewind fields")
		}
	case HARejoinActionReseed:
		if !HARejoinReseedComplete(response.Reseed) {
			return fmt.Errorf("missing rejoin reseed fields")
		}
	}
	if err := validateHARejoinReceiptCorrelation(response); err != nil {
		return err
	}
	return nil
}

func validateHARejoinReceiptCorrelation(response HARejoinAssessResponse) error {
	action := response.Action
	target := strings.TrimSpace(action.Target)
	if target != strings.TrimSpace(response.Assessment.FormerNodeId) {
		return fmt.Errorf("rejoin response action target does not match former node")
	}
	if strings.TrimSpace(action.ActionId) != strings.TrimSpace(string(action.ActionKind))+":"+target {
		return fmt.Errorf("rejoin response action id does not match action kind and target")
	}

	switch action.ActionKind {
	case HAActionKindRejoinAssess:
		if action.State != HAActionStateAssessed {
			return fmt.Errorf("rejoin assess response action state mismatch")
		}
		if strings.TrimSpace(action.NodeId) != target {
			return fmt.Errorf("rejoin assess response executor node mismatch")
		}
	case HAActionKindRejoinRewind:
		if action.State != HAActionStateApplied && action.State != HAActionStateAlreadyApplied {
			return fmt.Errorf("rejoin rewind response action state mismatch")
		}
		if strings.TrimSpace(action.NodeId) != target {
			return fmt.Errorf("rejoin rewind response executor node mismatch")
		}
		if response.Assessment.Action != HARejoinActionRewind {
			return fmt.Errorf("rejoin rewind response assessment action mismatch")
		}
		if strings.TrimSpace(response.Rewind.NodeId) != target {
			return fmt.Errorf("rejoin rewind response result node mismatch")
		}
	case HAActionKindRejoinReseed:
		if action.State != HAActionStateApplied && action.State != HAActionStateAlreadyApplied {
			return fmt.Errorf("rejoin reseed response action state mismatch")
		}
		if response.Assessment.Action != HARejoinActionReseed {
			return fmt.Errorf("rejoin reseed response assessment action mismatch")
		}
		if strings.TrimSpace(response.Reseed.NodeId) != target || strings.TrimSpace(response.Reseed.SlotName) != target {
			return fmt.Errorf("rejoin reseed response result target mismatch")
		}
	default:
		return fmt.Errorf("invalid rejoin response action kind %q", action.ActionKind)
	}
	return nil
}

func ValidateHARejoinAssessResponseEvidence(raw []byte) error {
	var response haRejoinAssessResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !haRejoinAssessmentEvidenceComplete(response.Assessment) {
		return fmt.Errorf("missing rejoin assessment field evidence")
	}
	switch strings.TrimSpace(response.Assessment.Action) {
	case string(HARejoinActionRewind):
		if !haRejoinRewindEvidenceComplete(response.Rewind) {
			return fmt.Errorf("missing rejoin rewind field evidence")
		}
	case string(HARejoinActionReseed):
		if !haRejoinReseedEvidenceComplete(response.Reseed) {
			return fmt.Errorf("missing rejoin reseed field evidence")
		}
	}
	return nil
}

type haPromotionAssessmentEvidence struct {
	RequiredLsn        *uint64 `json:"required_lsn"`
	ReceivedLsn        *uint64 `json:"received_lsn"`
	AppliedLsn         *uint64 `json:"applied_lsn"`
	HasRequiredLsn     *bool   `json:"has_required_lsn"`
	CaughtUpToReceived *bool   `json:"caught_up_to_received"`
	FencingConfirmed   *bool   `json:"fencing_confirmed"`
	Force              *bool   `json:"force"`
	DataLossPossible   *bool   `json:"data_loss_possible"`
	Safe               *bool   `json:"safe"`
	RequiresFencing    *bool   `json:"requires_fencing"`
	RequiresForce      *bool   `json:"requires_force"`
	CanPromote         *bool   `json:"can_promote"`
}

type haPromotionAssessResponseEvidence struct {
	Assessment haPromotionAssessmentEvidence `json:"assessment"`
}

type haPromotionResultEvidence struct {
	DataLossPossible *bool `json:"data_loss_possible"`
	Forced           *bool `json:"forced"`
}

type haPromotionResponseEvidence struct {
	Assessment haPromotionAssessmentEvidence `json:"assessment"`
	Forced     *bool                         `json:"forced"`
	Promotion  haPromotionResultEvidence     `json:"promotion"`
}

type haFenceReceiptIdentityEvidence struct {
	ClusterId  *uint64 `json:"cluster_id"`
	ShardId    *uint64 `json:"shard_id"`
	TableId    *uint64 `json:"table_id"`
	TimelineId *uint64 `json:"timeline_id"`
	Epoch      *uint64 `json:"epoch"`
}

type haFenceReceiptEvidence struct {
	Identity         haFenceReceiptIdentityEvidence `json:"identity"`
	ParentTimelineId *uint64                        `json:"parent_timeline_id"`
	ParentEpoch      *uint64                        `json:"parent_epoch"`
	NewTimelineId    *uint64                        `json:"new_timeline_id"`
	NewEpoch         *uint64                        `json:"new_epoch"`
	RequiredLsn      *uint64                        `json:"required_lsn"`
	ObservedLsn      *uint64                        `json:"observed_lsn"`
	Generation       *uint64                        `json:"generation"`
	Forced           *bool                          `json:"forced"`
	Reason           *string                        `json:"reason"`
}

type haFenceResponseEvidence struct {
	Receipt haFenceReceiptEvidence `json:"receipt"`
}

type haCurrentFenceResponseEvidence struct {
	Held    *bool                  `json:"held"`
	Receipt haFenceReceiptEvidence `json:"receipt"`
}

type haRejoinAssessmentEvidence struct {
	Action            string  `json:"action"`
	DataLossDiscarded *bool   `json:"data_loss_discarded"`
	ForkLsn           *uint64 `json:"fork_lsn"`
	FormerLastLsn     *uint64 `json:"former_last_lsn"`
	ParentClusterId   *uint64 `json:"parent_cluster_id"`
	ParentShardId     *uint64 `json:"parent_shard_id"`
	ParentTableId     *uint64 `json:"parent_table_id"`
	ParentTimelineId  *uint64 `json:"parent_timeline_id"`
	ParentEpoch       *uint64 `json:"parent_epoch"`
	RetainedFromLsn   *uint64 `json:"retained_from_lsn"`
	TargetTimelineId  *uint64 `json:"target_timeline_id"`
	TargetEpoch       *uint64 `json:"target_epoch"`
}

type haRejoinRewindEvidence struct {
	CurrentLastLsn    *uint64 `json:"current_last_lsn"`
	DataLossDiscarded *bool   `json:"data_loss_discarded"`
	DiscardedLsnCount *uint64 `json:"discarded_lsn_count"`
	ForkLsn           *uint64 `json:"fork_lsn"`
	NextLsn           *uint64 `json:"next_lsn"`
	PreviousLastLsn   *uint64 `json:"previous_last_lsn"`
	TargetTimelineId  *uint64 `json:"target_timeline_id"`
	TargetEpoch       *uint64 `json:"target_epoch"`
}

type haRejoinReseedEvidence struct {
	BaseBackupRequired *bool   `json:"base_backup_required"`
	ForkLsn            *uint64 `json:"fork_lsn"`
	FormerLastLsn      *uint64 `json:"former_last_lsn"`
	ReseedRequired     *bool   `json:"reseed_required"`
	TargetTimelineId   *uint64 `json:"target_timeline_id"`
	TargetEpoch        *uint64 `json:"target_epoch"`
}

type haRejoinAssessResponseEvidence struct {
	Assessment haRejoinAssessmentEvidence `json:"assessment"`
	Rewind     haRejoinRewindEvidence     `json:"rewind"`
	Reseed     haRejoinReseedEvidence     `json:"reseed"`
}

func haPromotionAssessmentEvidenceComplete(assessment haPromotionAssessmentEvidence) bool {
	return assessment.RequiredLsn != nil &&
		assessment.ReceivedLsn != nil &&
		assessment.AppliedLsn != nil &&
		assessment.HasRequiredLsn != nil &&
		assessment.CaughtUpToReceived != nil &&
		assessment.FencingConfirmed != nil &&
		assessment.Force != nil &&
		assessment.DataLossPossible != nil &&
		assessment.Safe != nil &&
		assessment.RequiresFencing != nil &&
		assessment.RequiresForce != nil &&
		assessment.CanPromote != nil
}

func haPromotionResultEvidenceComplete(result haPromotionResultEvidence) bool {
	return result.DataLossPossible != nil && result.Forced != nil
}

func haFenceReceiptEvidenceComplete(receipt haFenceReceiptEvidence) bool {
	return receipt.Identity.ClusterId != nil &&
		receipt.Identity.ShardId != nil &&
		receipt.Identity.TableId != nil &&
		receipt.Identity.TimelineId != nil &&
		receipt.Identity.Epoch != nil &&
		receipt.ParentTimelineId != nil &&
		receipt.ParentEpoch != nil &&
		receipt.NewTimelineId != nil &&
		receipt.NewEpoch != nil &&
		receipt.RequiredLsn != nil &&
		receipt.ObservedLsn != nil &&
		receipt.Generation != nil &&
		receipt.Forced != nil &&
		receipt.Reason != nil
}

func haRejoinAssessmentEvidenceComplete(assessment haRejoinAssessmentEvidence) bool {
	return assessment.DataLossDiscarded != nil &&
		assessment.ForkLsn != nil &&
		assessment.FormerLastLsn != nil &&
		assessment.ParentClusterId != nil &&
		assessment.ParentShardId != nil &&
		assessment.ParentTableId != nil &&
		assessment.ParentTimelineId != nil &&
		assessment.ParentEpoch != nil &&
		assessment.RetainedFromLsn != nil &&
		assessment.TargetTimelineId != nil &&
		assessment.TargetEpoch != nil
}

func haRejoinRewindEvidenceComplete(rewind haRejoinRewindEvidence) bool {
	return rewind.CurrentLastLsn != nil &&
		rewind.DataLossDiscarded != nil &&
		rewind.DiscardedLsnCount != nil &&
		rewind.ForkLsn != nil &&
		rewind.NextLsn != nil &&
		rewind.PreviousLastLsn != nil &&
		rewind.TargetTimelineId != nil &&
		rewind.TargetEpoch != nil
}

func haRejoinReseedEvidenceComplete(reseed haRejoinReseedEvidence) bool {
	return reseed.BaseBackupRequired != nil &&
		reseed.ForkLsn != nil &&
		reseed.FormerLastLsn != nil &&
		reseed.ReseedRequired != nil &&
		reseed.TargetTimelineId != nil &&
		reseed.TargetEpoch != nil
}

func HARejoinAssessmentComplete(assessment HARejoinAssessment) bool {
	return HARejoinAssessmentActionValid(assessment.Action) &&
		HARejoinAssessmentReasonValid(assessment.Reason) &&
		strings.TrimSpace(assessment.FormerNodeId) != "" &&
		assessment.TargetTimelineId > 0 &&
		assessment.TargetEpoch > 0 &&
		assessment.ParentClusterId > 0 &&
		assessment.ParentTimelineId > 0 &&
		assessment.ParentEpoch > 0
}

func HARejoinAssessmentActionValid(action HARejoinAssessmentAction) bool {
	switch action {
	case HARejoinActionRejectUnfenced, HARejoinActionAlreadyCurrent, HARejoinActionRewind, HARejoinActionReseed:
		return true
	default:
		return false
	}
}

func HARejoinAssessmentReasonValid(reason HARejoinAssessmentReason) bool {
	switch reason {
	case HARejoinReasonNoFence,
		HARejoinReasonCurrentTimeline,
		HARejoinReasonParentTimelineRetained,
		HARejoinReasonParentTimelineWALExpired,
		HARejoinReasonIncompatibleTimeline,
		HARejoinReasonWrongOldPrimary,
		HARejoinReasonWrongCluster,
		HARejoinReasonWrongShard,
		HARejoinReasonWrongTable,
		HARejoinReasonLocalLSNBeforeFork:
		return true
	default:
		return false
	}
}

func HARejoinRewindComplete(rewind HARejoinRewindResult) bool {
	return strings.TrimSpace(rewind.NodeId) != "" &&
		rewind.TargetTimelineId > 0 &&
		rewind.TargetEpoch > 0 &&
		rewind.NextLsn > 0
}

func HARejoinReseedComplete(reseed HARejoinReseedResult) bool {
	return strings.TrimSpace(reseed.NodeId) != "" &&
		strings.TrimSpace(reseed.SlotName) != "" &&
		reseed.TargetTimelineId > 0 &&
		reseed.TargetEpoch > 0 &&
		reseed.ReseedRequired &&
		reseed.BaseBackupRequired
}

func HACommitGateComplete(gate HACommitGate) bool {
	return HACommitGateActionValid(gate.Action) &&
		HADurabilityDecisionComplete(gate.Durability)
}

func validateHAPrimaryRetentionSnapshot(retention HARetentionSnapshot, currentLSN uint64, slotCount int) error {
	if retention.PrimaryLsn != currentLSN {
		return fmt.Errorf("primary retention snapshot inconsistent: primary_lsn=%d current_lsn=%d", retention.PrimaryLsn, currentLSN)
	}
	if retention.OldestRestartLsn > retention.PrimaryLsn {
		return fmt.Errorf("primary retention snapshot inconsistent: oldest_restart_lsn=%d exceeds primary_lsn=%d", retention.OldestRestartLsn, retention.PrimaryLsn)
	}
	if retention.RetainedLsnCount != retention.PrimaryLsn-retention.OldestRestartLsn {
		return fmt.Errorf("primary retention snapshot inconsistent: retained_lsn_count=%d expected=%d", retention.RetainedLsnCount, retention.PrimaryLsn-retention.OldestRestartLsn)
	}
	if retention.ActiveSlots > uint64(slotCount) {
		return fmt.Errorf("primary retention snapshot inconsistent: active_slots=%d exceeds slot count=%d", retention.ActiveSlots, slotCount)
	}
	if retention.ReseedRecommended > uint64(slotCount) {
		return fmt.Errorf("primary retention snapshot inconsistent: reseed_recommended=%d exceeds slot count=%d", retention.ReseedRecommended, slotCount)
	}
	return nil
}

func validateHASlotSnapshot(slot HASlotSnapshot, currentLSN uint64) error {
	name := strings.TrimSpace(slot.Name)
	if name == "" {
		return fmt.Errorf("missing slot snapshot name")
	}
	if slot.TimelineId == 0 {
		return fmt.Errorf("slot %s snapshot missing timeline_id", name)
	}
	if !HASlotSnapshotStatusValid(slot.Status) {
		return fmt.Errorf("slot %s snapshot invalid status %q", name, slot.Status)
	}
	if slot.RestartLsn > currentLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: restart_lsn=%d exceeds current_lsn=%d", name, slot.RestartLsn, currentLSN)
	}
	if slot.ReceivedLsn > currentLSN {
		return fmt.Errorf("slot %s snapshot inconsistent: received_lsn=%d exceeds current_lsn=%d", name, slot.ReceivedLsn, currentLSN)
	}
	if slot.AppliedLsn > slot.ReceivedLsn {
		return fmt.Errorf("slot %s snapshot inconsistent: applied_lsn=%d exceeds received_lsn=%d", name, slot.AppliedLsn, slot.ReceivedLsn)
	}
	if slot.SafeReadLsn > slot.AppliedLsn {
		return fmt.Errorf("slot %s snapshot inconsistent: safe_read_lsn=%d exceeds applied_lsn=%d", name, slot.SafeReadLsn, slot.AppliedLsn)
	}
	if slot.WriteLagLsn != haSaturatingSub(currentLSN, slot.ReceivedLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: write_lag_lsn=%d expected=%d", name, slot.WriteLagLsn, haSaturatingSub(currentLSN, slot.ReceivedLsn))
	}
	if slot.ApplyLagLsn != haSaturatingSub(currentLSN, slot.AppliedLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: apply_lag_lsn=%d expected=%d", name, slot.ApplyLagLsn, haSaturatingSub(currentLSN, slot.AppliedLsn))
	}
	if slot.SafeReadLagLsn != haSaturatingSub(currentLSN, slot.SafeReadLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: safe_read_lag_lsn=%d expected=%d", name, slot.SafeReadLagLsn, haSaturatingSub(currentLSN, slot.SafeReadLsn))
	}
	if slot.RetentionLagLsn != haSaturatingSub(currentLSN, slot.RestartLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: retention_lag_lsn=%d expected=%d", name, slot.RetentionLagLsn, haSaturatingSub(currentLSN, slot.RestartLsn))
	}
	return nil
}

func HASlotSnapshotStatusValid(status HASlotSnapshotStatus) bool {
	switch status {
	case HASlotSnapshotStatusHealthy, HASlotSnapshotStatusLagging, HASlotSnapshotStatusReseedRequired:
		return true
	default:
		return false
	}
}

func validateHADurabilityDecision(decision HADurabilityDecision) error {
	if !HADurabilityDecisionComplete(decision) {
		return fmt.Errorf("missing durability decision fields")
	}
	if decision.ProgressLsn > decision.TargetLsn {
		return fmt.Errorf("durability decision inconsistent: progress_lsn=%d exceeds target_lsn=%d", decision.ProgressLsn, decision.TargetLsn)
	}
	if decision.MissingLsnCount != decision.TargetLsn-decision.ProgressLsn {
		return fmt.Errorf("durability decision inconsistent: missing_lsn_count=%d expected=%d", decision.MissingLsnCount, decision.TargetLsn-decision.ProgressLsn)
	}
	if decision.RequiredCount > decision.CandidateCount {
		return fmt.Errorf("durability decision inconsistent: required_count=%d exceeds candidate_count=%d", decision.RequiredCount, decision.CandidateCount)
	}
	if decision.SatisfiedCount > decision.CandidateCount {
		return fmt.Errorf("durability decision inconsistent: satisfied_count=%d exceeds candidate_count=%d", decision.SatisfiedCount, decision.CandidateCount)
	}
	if decision.Status == HADurabilityStatusSatisfied && decision.SatisfiedCount < decision.RequiredCount {
		return fmt.Errorf("durability decision inconsistent: satisfied_count=%d below required_count=%d", decision.SatisfiedCount, decision.RequiredCount)
	}
	return nil
}

func HACommitGateActionValid(action HACommitGateAction) bool {
	switch action {
	case HACommitGateActionAcknowledge,
		HACommitGateActionWaitForStandby,
		HACommitGateActionReject,
		HACommitGateActionAcknowledgeDegraded:
		return true
	default:
		return false
	}
}

func HADurabilityDecisionComplete(decision HADurabilityDecision) bool {
	return HADurabilityDecisionStatusValid(decision.Status) &&
		HADurabilityDecisionModeValid(decision.Mode) &&
		HADurabilityDecisionSelectionValid(decision.Selection)
}

func HADurabilityDecisionEmpty(decision HADurabilityDecision) bool {
	return decision == (HADurabilityDecision{})
}

func HADurabilityDecisionStatusValid(status HADurabilityDecisionStatus) bool {
	switch status {
	case HADurabilityStatusSatisfied,
		HADurabilityStatusWouldBlock,
		HADurabilityStatusFailClosed,
		HADurabilityStatusDegradedToAsync:
		return true
	default:
		return false
	}
}

func HADurabilityDecisionModeValid(mode HADurabilityDecisionMode) bool {
	switch mode {
	case HADurabilityModeAsync, HADurabilityModeRemoteWrite, HADurabilityModeRemoteApply:
		return true
	default:
		return false
	}
}

func HADurabilityDecisionSelectionValid(selection HADurabilityDecisionSelection) bool {
	switch selection {
	case HADurabilitySelectionAny, HADurabilitySelectionFirst, HADurabilitySelectionAll:
		return true
	default:
		return false
	}
}

func HAReadDecisionComplete(decision HAReadDecision) bool {
	return HAReadDecisionActionValid(decision.Action) &&
		HAReadDecisionConsistencyValid(decision.Consistency)
}

func HAReadDecisionActionValid(action HAReadDecisionAction) bool {
	switch action {
	case HAReadDecisionActionServeStandby,
		HAReadDecisionActionWaitForApply,
		HAReadDecisionActionWaitForMetadata,
		HAReadDecisionActionRouteToPrimary:
		return true
	default:
		return false
	}
}

func HAReadDecisionConsistencyValid(consistency HAReadDecisionConsistency) bool {
	switch consistency {
	case HAReadDecisionConsistencyStaleOK,
		HAReadDecisionConsistencyAtLeastLSN,
		HAReadDecisionConsistencyPrimary:
		return true
	default:
		return false
	}
}

func HAWriteDecisionComplete(decision HAWriteDecision) bool {
	return HAWriteDecisionRoleValid(decision.Role) &&
		HAWriteDecisionActionValid(decision.Action) &&
		HAIdentityComplete(decision.Identity) &&
		HAPromotionHandoffCompleteOrEmpty(decision.PromotionHandoff)
}

func HAWriteDecisionRoleValid(role HAWriteDecisionRole) bool {
	switch role {
	case HAWriteDecisionRolePrimary, HAWriteDecisionRoleStandby, HAWriteDecisionRolePromotedStandby:
		return true
	default:
		return false
	}
}

func HAWriteDecisionActionValid(action HAWriteDecisionAction) bool {
	switch action {
	case HAWriteDecisionActionAllowWrite,
		HAWriteDecisionActionRejectReadOnly,
		HAWriteDecisionActionOpenPromotedPrimary:
		return true
	default:
		return false
	}
}

func HAOwnerJobDecisionComplete(decision HAOwnerJobDecision) bool {
	return HAOwnerJobDecisionKindValid(decision.Kind) &&
		HAOwnerJobDecisionRoleValid(decision.Role) &&
		HAOwnerJobDecisionActionValid(decision.Action) &&
		HAIdentityComplete(decision.Identity) &&
		HAPromotionHandoffCompleteOrEmpty(decision.PromotionHandoff)
}

func HAOwnerJobDecisionKindValid(kind HAOwnerJobDecisionKind) bool {
	switch kind {
	case HAOwnerJobDecisionKindCompactionPublish,
		HAOwnerJobDecisionKindDerivedEffectWriter,
		HAOwnerJobDecisionKindEnrichmentWriter,
		HAOwnerJobDecisionKindRetentionAdvance:
		return true
	default:
		return false
	}
}

func HAOwnerJobDecisionRoleValid(role HAOwnerJobDecisionRole) bool {
	switch role {
	case HAOwnerJobDecisionRolePrimary, HAOwnerJobDecisionRoleStandby, HAOwnerJobDecisionRolePromotedStandby:
		return true
	default:
		return false
	}
}

func HAOwnerJobDecisionActionValid(action HAOwnerJobDecisionAction) bool {
	switch action {
	case HAOwnerJobDecisionActionRun,
		HAOwnerJobDecisionActionDisableOnStandby,
		HAOwnerJobDecisionActionOpenPromotedPrimary:
		return true
	default:
		return false
	}
}

func HAPromotionHandoffCompleteOrEmpty(handoff HAPromotionHandoff) bool {
	if !HAIdentityComplete(handoff.Identity) && handoff.SwitchLsn == 0 && handoff.NextLsn == 0 {
		return true
	}
	return HAIdentityComplete(handoff.Identity)
}

func HAPromotionAssessmentComplete(assessment HAPromotionAssessment) bool {
	return assessment.RequiredLsn > 0
}

func HAPromotionResultComplete(result HAPromotionResult) bool {
	return strings.TrimSpace(result.NodeId) != "" &&
		result.SwitchLsn > 0 &&
		HAIdentityComplete(result.OldIdentity) &&
		HAIdentityComplete(result.NewIdentity)
}

func HAFenceReceiptComplete(receipt HAFenceReceipt) bool {
	return HAIdentityComplete(receipt.Identity) &&
		strings.TrimSpace(receipt.OldPrimaryId) != "" &&
		strings.TrimSpace(receipt.PromotedNodeId) != "" &&
		receipt.ParentTimelineId > 0 &&
		receipt.ParentEpoch > 0 &&
		receipt.NewTimelineId > 0 &&
		receipt.NewEpoch > 0 &&
		receipt.RequiredLsn > 0 &&
		receipt.Generation > 0 &&
		strings.TrimSpace(receipt.Token) != "" &&
		strings.TrimSpace(receipt.Reason) != ""
}

func HAFenceReceiptEmpty(receipt HAFenceReceipt) bool {
	return !receipt.Forced &&
		receipt.Generation == 0 &&
		receipt.Identity == (HAIdentity{}) &&
		receipt.NewEpoch == 0 &&
		receipt.NewTimelineId == 0 &&
		receipt.ObservedLsn == 0 &&
		strings.TrimSpace(receipt.OldPrimaryId) == "" &&
		receipt.ParentEpoch == 0 &&
		receipt.ParentTimelineId == 0 &&
		strings.TrimSpace(receipt.PromotedNodeId) == "" &&
		strings.TrimSpace(receipt.Reason) == "" &&
		receipt.RequiredLsn == 0 &&
		strings.TrimSpace(receipt.Token) == ""
}

func HAIdentityComplete(identity HAIdentity) bool {
	return identity.ClusterId > 0 &&
		identity.TimelineId > 0 &&
		identity.Epoch > 0
}

func HAActionReceiptPresent(receipt HAActionReceipt) bool {
	return strings.TrimSpace(receipt.ActionId) != "" &&
		strings.TrimSpace(string(receipt.ActionKind)) != "" &&
		strings.TrimSpace(receipt.Target) != "" &&
		strings.TrimSpace(string(receipt.State)) != "" &&
		strings.TrimSpace(receipt.NodeId) != ""
}

func HAReplicationSlotComplete(slot HAReplicationSlot) bool {
	return strings.TrimSpace(slot.SlotName) != "" &&
		slot.TimelineId > 0
}

func HAListReplicationSlotsOperation() HAOperation {
	return HAOperation{Method: http.MethodGet, Path: HAReplicationSlotsPath}
}

func HACreateReplicationSlotOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HAReplicationSlotsPath}
}

func HADropReplicationSlotOperation(slotName string) (HAOperation, bool) {
	path, ok := HAReplicationSlotPath(slotName)
	if !ok {
		return HAOperation{}, false
	}
	return HAOperation{Method: http.MethodDelete, Path: path}, true
}

func HAPauseReplicationSlotOperation(slotName string) (HAOperation, bool) {
	path, ok := HAReplicationSlotPath(slotName)
	if !ok {
		return HAOperation{}, false
	}
	return HAOperation{Method: http.MethodPut, Path: path + HAReplicationSlotPausePathSuffix}, true
}

func HAResumeReplicationSlotOperation(slotName string) (HAOperation, bool) {
	path, ok := HAReplicationSlotPath(slotName)
	if !ok {
		return HAOperation{}, false
	}
	return HAOperation{Method: http.MethodPut, Path: path + HAReplicationSlotResumePathSuffix}, true
}

func HABeginBaseBackupOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HABaseBackupsPath}
}

func HAFinishBaseBackupOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HABaseBackupsFinishPath}
}

func HABootstrapStandbyOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HAStandbyBootstrapPath}
}

func HAAcquireFenceOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HAFencePath}
}

func HAAssessPromotionOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HAPromotionAssessPath}
}

func HAPromoteWithCurrentFenceOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HAPromotionCurrentFencePath}
}

func HAAssessRejoinOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HARejoinAssessPath}
}

func HARewindRejoinOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HARejoinRewindPath}
}

func HAReseedRejoinOperation() HAOperation {
	return HAOperation{Method: http.MethodPost, Path: HARejoinReseedPath}
}

func HAReplicationSlotPath(slotName string) (string, bool) {
	slotName = strings.TrimSpace(slotName)
	if slotName == "" {
		return "", false
	}
	return HAReplicationSlotPathPrefix + url.PathEscape(slotName), true
}

func HAReplicationSlotCreateReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindReplicationSlotCreate, State: HAActionStateApplied}
}

func HAReplicationSlotResumeReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindReplicationSlotResume, State: HAActionStateApplied}
}

func HAReplicationSlotPauseReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindReplicationSlotPause, State: HAActionStateApplied}
}

func HAReplicationSlotDropReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindReplicationSlotDrop, State: HAActionStateApplied}
}

func HABaseBackupBeginReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindBaseBackupBegin, State: HAActionStateApplied}
}

func HABaseBackupFinishReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindBaseBackupFinish, State: HAActionStateApplied}
}

func HAStandbyBootstrapReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindStandbyBootstrap, State: HAActionStateApplied}
}

func HAFenceAcquireReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindFenceAcquire, State: HAActionStateApplied}
}

func HAPromotionAssessReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindPromotionAssess, State: HAActionStateAssessed}
}

func HAPromotionReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindPromotion, State: HAActionStateApplied}
}

func HARejoinAssessReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindRejoinAssess, State: HAActionStateAssessed}
}

func HARejoinRewindReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindRejoinRewind, State: HAActionStateApplied}
}

func HARejoinReseedReceiptExpectation() HAReceiptExpectation {
	return HAReceiptExpectation{ActionKind: HAActionKindRejoinReseed, State: HAActionStateApplied}
}

// HAResponse keeps the typed response and the original response body together.
// The raw body is useful for callers that must validate field presence, not
// just decoded values.
type HAResponse[T any] struct {
	Value      *T
	Body       []byte
	StatusCode int
}

// HAAPIError describes a non-2xx HA admin API response.
type HAAPIError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *HAAPIError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s returned status %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("%s returned status %d: %s", e.Operation, e.StatusCode, e.Body)
}

// HAResponseValidationError describes a 2xx HA admin response that decoded but
// did not satisfy the wrapper's typed response contract.
type HAResponseValidationError struct {
	Operation string
	Err       error
}

func (e *HAResponseValidationError) Error() string {
	return fmt.Sprintf("%s response invalid: %v", e.Operation, e.Err)
}

func (e *HAResponseValidationError) Unwrap() error {
	return e.Err
}

// NewHAClient creates a typed HA admin client. The base URL may be either the
// Antfly server root or an explicit /admin/v1 admin API root.
func NewHAClient(baseURL string, httpClient *http.Client) (*HAClient, error) {
	opts := []oapi.ClientOption{}
	if httpClient != nil {
		opts = append(opts, oapi.WithHTTPClient(httpClient))
	}
	return NewHAClientWithOptions(baseURL, opts...)
}

// NewHAClientWithOptions creates a typed HA admin client with generated-client options.
func NewHAClientWithOptions(baseURL string, opts ...oapi.ClientOption) (*HAClient, error) {
	client, err := oapi.NewClientWithResponses(normalizeAdminBaseURL(baseURL), opts...)
	if err != nil {
		return nil, err
	}
	return &HAClient{client: client, editors: []oapi.RequestEditorFn{acceptJSONEditor}}, nil
}

// WithToken configures bearer-token authentication for HA admin requests.
func (c *HAClient) WithToken(token string) *HAClient {
	token = strings.TrimSpace(token)
	c.editors = []oapi.RequestEditorFn{acceptJSONEditor}
	if token != "" {
		c.editors = append(c.editors, func(_ context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		})
	}
	return c
}

// Client returns the underlying generated client for low-level operations.
func (c *HAClient) Client() *oapi.ClientWithResponses {
	return c.client
}

func acceptJSONEditor(_ context.Context, req *http.Request) error {
	req.Header.Set("Accept", "application/json")
	return nil
}

func normalizeAdminBaseURL(baseURL string) string {
	trimmed := strings.TrimRight(baseURL, "/")
	return strings.TrimSuffix(trimmed, adminV1Path) + adminV1Path
}

func requireHAJSON200[T any](operation string, statusCode int, body []byte, value *T, err error) (*HAResponse[T], error) {
	if err != nil {
		return nil, err
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		return nil, &HAAPIError{
			Operation:  operation,
			StatusCode: statusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	if value == nil {
		return nil, &HAAPIError{
			Operation:  operation,
			StatusCode: statusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	return &HAResponse[T]{
		Value:      value,
		Body:       body,
		StatusCode: statusCode,
	}, nil
}

func requireHAJSON200Validated[T any](operation string, statusCode int, body []byte, value *T, err error, validate func(T) error) (*HAResponse[T], error) {
	return requireHAJSON200ValidatedEvidence(operation, statusCode, body, value, err, validate, nil)
}

func requireHAJSON200ValidatedEvidence[T any](operation string, statusCode int, body []byte, value *T, err error, validate func(T) error, validateEvidence func([]byte) error) (*HAResponse[T], error) {
	response, err := requireHAJSON200(operation, statusCode, body, value, err)
	if err != nil {
		return nil, err
	}
	if validate != nil {
		if err := validate(*response.Value); err != nil {
			return nil, &HAResponseValidationError{Operation: operation, Err: err}
		}
	}
	if validateEvidence != nil {
		if err := validateEvidence(response.Body); err != nil {
			return nil, &HAResponseValidationError{Operation: operation, Err: err}
		}
	}
	return response, nil
}

func requireHA2xx(operation string, statusCode int, body []byte, err error) error {
	if err != nil {
		return err
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		return &HAAPIError{
			Operation:  operation,
			StatusCode: statusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	return nil
}

func haResponseValue[T any](response *HAResponse[T], err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	return response.Value, nil
}

func (c *HAClient) PrimaryStatusResponse(ctx context.Context, params *HAPrimaryStatusParams) (*HAResponse[HAPrimaryStatusResponse], error) {
	resp, err := c.client.GetHAPrimaryStatusWithResponse(ctx, params, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("get HA primary status", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAPrimaryStatusResponse)
}

func (c *HAClient) PrimaryStatus(ctx context.Context, params *HAPrimaryStatusParams) (*HAPrimaryStatusResponse, error) {
	return haResponseValue(c.PrimaryStatusResponse(ctx, params))
}

func (c *HAClient) PrimaryStatusParsedResponse(ctx context.Context, params *HAPrimaryStatusParams) (*HAResponse[ParsedHAPrimaryStatus], error) {
	resp, err := c.client.GetHAPrimaryStatusWithResponse(ctx, params, c.editors...)
	if resp == nil {
		return nil, err
	}
	if err := requireHA2xx("get HA primary status", resp.StatusCode(), resp.Body, err); err != nil {
		return nil, err
	}
	parsed, err := ParseHAPrimaryStatus(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse HA primary status: %w", err)
	}
	return &HAResponse[ParsedHAPrimaryStatus]{
		Value:      parsed,
		Body:       resp.Body,
		StatusCode: resp.StatusCode(),
	}, nil
}

func (c *HAClient) PrimaryStatusParsed(ctx context.Context, params *HAPrimaryStatusParams) (*ParsedHAPrimaryStatus, error) {
	return haResponseValue(c.PrimaryStatusParsedResponse(ctx, params))
}

func (c *HAClient) StandbyStatusResponse(ctx context.Context, params *HAStandbyStatusParams) (*HAResponse[HAStandbyStatusResponse], error) {
	resp, err := c.client.GetHAStandbyStatusWithResponse(ctx, params, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("get HA standby status", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAStandbyStatusResponse)
}

func (c *HAClient) StandbyStatus(ctx context.Context, params *HAStandbyStatusParams) (*HAStandbyStatusResponse, error) {
	return haResponseValue(c.StandbyStatusResponse(ctx, params))
}

func (c *HAClient) StandbyStatusParsedResponse(ctx context.Context, params *HAStandbyStatusParams) (*HAResponse[HAStandbyStatusResponse], error) {
	resp, err := c.client.GetHAStandbyStatusWithResponse(ctx, params, c.editors...)
	if resp == nil {
		return nil, err
	}
	if err := requireHA2xx("get HA standby status", resp.StatusCode(), resp.Body, err); err != nil {
		return nil, err
	}
	parsed, err := ParseHAStandbyStatus(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse HA standby status: %w", err)
	}
	return &HAResponse[HAStandbyStatusResponse]{
		Value:      parsed,
		Body:       resp.Body,
		StatusCode: resp.StatusCode(),
	}, nil
}

func (c *HAClient) StandbyStatusParsed(ctx context.Context, params *HAStandbyStatusParams) (*HAStandbyStatusResponse, error) {
	return haResponseValue(c.StandbyStatusParsedResponse(ctx, params))
}

func (c *HAClient) AppendCommitResponse(ctx context.Context, body CommitAppendRequest) (*HAResponse[HACommitAppendResponse], error) {
	resp, err := c.client.AppendHACommitWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("append HA commit", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHACommitAppendResponse)
}

func (c *HAClient) AppendCommit(ctx context.Context, body CommitAppendRequest) (*HACommitAppendResponse, error) {
	return haResponseValue(c.AppendCommitResponse(ctx, body))
}

func (c *HAClient) CheckCommitResponse(ctx context.Context, body CommitCheckRequest) (*HAResponse[HACommitCheckResponse], error) {
	resp, err := c.client.CheckHACommitWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("check HA commit", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHACommitCheckResponse)
}

func (c *HAClient) CheckCommit(ctx context.Context, body CommitCheckRequest) (*HACommitCheckResponse, error) {
	return haResponseValue(c.CheckCommitResponse(ctx, body))
}

func (c *HAClient) CheckReadResponse(ctx context.Context, body ReadCheckRequest) (*HAResponse[HAReadCheckResponse], error) {
	resp, err := c.client.CheckHAReadWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("check HA read", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAReadCheckResponse)
}

func (c *HAClient) CheckRead(ctx context.Context, body ReadCheckRequest) (*HAReadCheckResponse, error) {
	return haResponseValue(c.CheckReadResponse(ctx, body))
}

func (c *HAClient) CheckWriteResponse(ctx context.Context, body WriteCheckRequest) (*HAResponse[HAWriteCheckResponse], error) {
	resp, err := c.client.CheckHAWriteWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("check HA write", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAWriteCheckResponse)
}

func (c *HAClient) CheckWrite(ctx context.Context, body WriteCheckRequest) (*HAWriteCheckResponse, error) {
	return haResponseValue(c.CheckWriteResponse(ctx, body))
}

func (c *HAClient) CheckOwnerJobResponse(ctx context.Context, body OwnerJobCheckRequest) (*HAResponse[HAOwnerJobCheckResponse], error) {
	resp, err := c.client.CheckHAOwnerJobWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("check HA owner job", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAOwnerJobCheckResponse)
}

func (c *HAClient) CheckOwnerJob(ctx context.Context, body OwnerJobCheckRequest) (*HAOwnerJobCheckResponse, error) {
	return haResponseValue(c.CheckOwnerJobResponse(ctx, body))
}

func (c *HAClient) ListReplicationSlotsResponse(ctx context.Context) (*HAResponse[HAReplicationSlotListResponse], error) {
	resp, err := c.client.ListHAReplicationSlotsWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("list HA replication slots", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAReplicationSlotListResponse)
}

func (c *HAClient) ListReplicationSlots(ctx context.Context) (*HAReplicationSlotListResponse, error) {
	return haResponseValue(c.ListReplicationSlotsResponse(ctx))
}

func (c *HAClient) CreateReplicationSlotResponse(ctx context.Context, body ReplicationSlotCreateRequest) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.CreateHAReplicationSlotWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("create HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAReplicationSlotActionResponse)
}

func (c *HAClient) CreateReplicationSlot(ctx context.Context, body ReplicationSlotCreateRequest) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.CreateReplicationSlotResponse(ctx, body))
}

func (c *HAClient) PauseReplicationSlotResponse(ctx context.Context, slotName string) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.PauseHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("pause HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAReplicationSlotActionResponse)
}

func (c *HAClient) PauseReplicationSlot(ctx context.Context, slotName string) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.PauseReplicationSlotResponse(ctx, slotName))
}

func (c *HAClient) ResumeReplicationSlotResponse(ctx context.Context, slotName string) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.ResumeHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("resume HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAReplicationSlotActionResponse)
}

func (c *HAClient) ResumeReplicationSlot(ctx context.Context, slotName string) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.ResumeReplicationSlotResponse(ctx, slotName))
}

func (c *HAClient) DropReplicationSlotResponse(ctx context.Context, slotName string) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.DropHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("drop HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAReplicationSlotActionResponse)
}

func (c *HAClient) DropReplicationSlot(ctx context.Context, slotName string) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.DropReplicationSlotResponse(ctx, slotName))
}

func (c *HAClient) BeginBaseBackupResponse(ctx context.Context, body BaseBackupStartRequest) (*HAResponse[HABaseBackupBeginResponse], error) {
	resp, err := c.client.BeginHABaseBackupWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("begin HA base backup", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHABaseBackupBeginResponse)
}

func (c *HAClient) BeginBaseBackup(ctx context.Context, body BaseBackupStartRequest) (*HABaseBackupBeginResponse, error) {
	return haResponseValue(c.BeginBaseBackupResponse(ctx, body))
}

func (c *HAClient) FinishBaseBackupResponse(ctx context.Context, body BaseBackupManifestPathRequest) (*HAResponse[HABaseBackupFinishResponse], error) {
	resp, err := c.client.FinishHABaseBackupWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("finish HA base backup", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHABaseBackupFinishResponse)
}

func (c *HAClient) FinishBaseBackup(ctx context.Context, body BaseBackupManifestPathRequest) (*HABaseBackupFinishResponse, error) {
	return haResponseValue(c.FinishBaseBackupResponse(ctx, body))
}

func (c *HAClient) BootstrapStandbyResponse(ctx context.Context, body StandbyBootstrapRequest) (*HAResponse[HAStandbyBootstrapResponse], error) {
	resp, err := c.client.BootstrapHAStandbyWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200Validated("bootstrap HA standby", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAStandbyBootstrapResponse)
}

func (c *HAClient) BootstrapStandby(ctx context.Context, body StandbyBootstrapRequest) (*HAStandbyBootstrapResponse, error) {
	return haResponseValue(c.BootstrapStandbyResponse(ctx, body))
}

func (c *HAClient) AcquireFenceResponse(ctx context.Context, body FenceAcquireRequest) (*HAResponse[HAFenceResponse], error) {
	resp, err := c.client.AcquireHAFenceWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("acquire HA fence", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAFenceResponse, ValidateHAFenceResponseEvidence)
}

func (c *HAClient) AcquireFence(ctx context.Context, body FenceAcquireRequest) (*HAFenceResponse, error) {
	return haResponseValue(c.AcquireFenceResponse(ctx, body))
}

func (c *HAClient) CurrentFenceResponse(ctx context.Context) (*HAResponse[HACurrentFenceResponse], error) {
	resp, err := c.client.GetHACurrentFenceWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("get current HA fence", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHACurrentFenceResponse, ValidateHACurrentFenceResponseEvidence)
}

func (c *HAClient) CurrentFence(ctx context.Context) (*HACurrentFenceResponse, error) {
	return haResponseValue(c.CurrentFenceResponse(ctx))
}

func (c *HAClient) AssessPromotionResponse(ctx context.Context, body PromotionAssessRequest) (*HAResponse[HAPromotionAssessResponse], error) {
	resp, err := c.client.AssessHAPromotionWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("assess HA promotion", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAPromotionAssessResponse, ValidateHAPromotionAssessResponseEvidence)
}

func (c *HAClient) AssessPromotion(ctx context.Context, body PromotionAssessRequest) (*HAPromotionAssessResponse, error) {
	return haResponseValue(c.AssessPromotionResponse(ctx, body))
}

func (c *HAClient) PromoteResponse(ctx context.Context, body FenceAcquireRequest) (*HAResponse[HAPromotionResponse], error) {
	resp, err := c.client.PromoteHAWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("promote HA standby", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAPromotionResponse, ValidateHAPromotionResponseEvidence)
}

func (c *HAClient) Promote(ctx context.Context, body FenceAcquireRequest) (*HAPromotionResponse, error) {
	return haResponseValue(c.PromoteResponse(ctx, body))
}

func (c *HAClient) PromoteWithCurrentFenceResponse(ctx context.Context) (*HAResponse[HAPromotionResponse], error) {
	resp, err := c.client.PromoteHAWithCurrentFenceWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("promote HA standby with current fence", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHAPromotionResponse, ValidateHAPromotionResponseEvidence)
}

func (c *HAClient) PromoteWithCurrentFence(ctx context.Context) (*HAPromotionResponse, error) {
	return haResponseValue(c.PromoteWithCurrentFenceResponse(ctx))
}

func (c *HAClient) AssessRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*HAResponse[HARejoinAssessResponse], error) {
	resp, err := c.client.AssessHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("assess HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHARejoinAssessResponse, ValidateHARejoinAssessResponseEvidence)
}

func (c *HAClient) AssessRejoin(ctx context.Context, body RejoinAssessRequest) (*HARejoinAssessResponse, error) {
	return haResponseValue(c.AssessRejoinResponse(ctx, body))
}

func (c *HAClient) RewindRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*HAResponse[HARejoinAssessResponse], error) {
	resp, err := c.client.RewindHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("rewind HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHARejoinAssessResponse, ValidateHARejoinAssessResponseEvidence)
}

func (c *HAClient) RewindRejoin(ctx context.Context, body RejoinAssessRequest) (*HARejoinAssessResponse, error) {
	return haResponseValue(c.RewindRejoinResponse(ctx, body))
}

func (c *HAClient) ReseedRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*HAResponse[HARejoinAssessResponse], error) {
	resp, err := c.client.ReseedHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200ValidatedEvidence("reseed HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateHARejoinAssessResponse, ValidateHARejoinAssessResponseEvidence)
}

func (c *HAClient) ReseedRejoin(ctx context.Context, body RejoinAssessRequest) (*HARejoinAssessResponse, error) {
	return haResponseValue(c.ReseedRejoinResponse(ctx, body))
}
