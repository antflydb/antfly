package admin

import (
	"context"
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
	return nil
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
	return requireHAJSON200("get HA primary status", resp.StatusCode(), resp.Body, resp.JSON200, err)
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
	return requireHAJSON200("get HA standby status", resp.StatusCode(), resp.Body, resp.JSON200, err)
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
	return requireHAJSON200("append HA commit", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) AppendCommit(ctx context.Context, body CommitAppendRequest) (*HACommitAppendResponse, error) {
	return haResponseValue(c.AppendCommitResponse(ctx, body))
}

func (c *HAClient) CheckCommitResponse(ctx context.Context, body CommitCheckRequest) (*HAResponse[HACommitCheckResponse], error) {
	resp, err := c.client.CheckHACommitWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("check HA commit", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) CheckCommit(ctx context.Context, body CommitCheckRequest) (*HACommitCheckResponse, error) {
	return haResponseValue(c.CheckCommitResponse(ctx, body))
}

func (c *HAClient) CheckReadResponse(ctx context.Context, body ReadCheckRequest) (*HAResponse[HAReadCheckResponse], error) {
	resp, err := c.client.CheckHAReadWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("check HA read", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) CheckRead(ctx context.Context, body ReadCheckRequest) (*HAReadCheckResponse, error) {
	return haResponseValue(c.CheckReadResponse(ctx, body))
}

func (c *HAClient) CheckWriteResponse(ctx context.Context, body WriteCheckRequest) (*HAResponse[HAWriteCheckResponse], error) {
	resp, err := c.client.CheckHAWriteWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("check HA write", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) CheckWrite(ctx context.Context, body WriteCheckRequest) (*HAWriteCheckResponse, error) {
	return haResponseValue(c.CheckWriteResponse(ctx, body))
}

func (c *HAClient) CheckOwnerJobResponse(ctx context.Context, body OwnerJobCheckRequest) (*HAResponse[HAOwnerJobCheckResponse], error) {
	resp, err := c.client.CheckHAOwnerJobWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("check HA owner job", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) CheckOwnerJob(ctx context.Context, body OwnerJobCheckRequest) (*HAOwnerJobCheckResponse, error) {
	return haResponseValue(c.CheckOwnerJobResponse(ctx, body))
}

func (c *HAClient) ListReplicationSlotsResponse(ctx context.Context) (*HAResponse[HAReplicationSlotListResponse], error) {
	resp, err := c.client.ListHAReplicationSlotsWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("list HA replication slots", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) ListReplicationSlots(ctx context.Context) (*HAReplicationSlotListResponse, error) {
	return haResponseValue(c.ListReplicationSlotsResponse(ctx))
}

func (c *HAClient) CreateReplicationSlotResponse(ctx context.Context, body ReplicationSlotCreateRequest) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.CreateHAReplicationSlotWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("create HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) CreateReplicationSlot(ctx context.Context, body ReplicationSlotCreateRequest) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.CreateReplicationSlotResponse(ctx, body))
}

func (c *HAClient) PauseReplicationSlotResponse(ctx context.Context, slotName string) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.PauseHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("pause HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) PauseReplicationSlot(ctx context.Context, slotName string) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.PauseReplicationSlotResponse(ctx, slotName))
}

func (c *HAClient) ResumeReplicationSlotResponse(ctx context.Context, slotName string) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.ResumeHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("resume HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) ResumeReplicationSlot(ctx context.Context, slotName string) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.ResumeReplicationSlotResponse(ctx, slotName))
}

func (c *HAClient) DropReplicationSlotResponse(ctx context.Context, slotName string) (*HAResponse[HAReplicationSlotActionResponse], error) {
	resp, err := c.client.DropHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("drop HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) DropReplicationSlot(ctx context.Context, slotName string) (*HAReplicationSlotActionResponse, error) {
	return haResponseValue(c.DropReplicationSlotResponse(ctx, slotName))
}

func (c *HAClient) BeginBaseBackupResponse(ctx context.Context, body BaseBackupStartRequest) (*HAResponse[HABaseBackupBeginResponse], error) {
	resp, err := c.client.BeginHABaseBackupWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("begin HA base backup", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) BeginBaseBackup(ctx context.Context, body BaseBackupStartRequest) (*HABaseBackupBeginResponse, error) {
	return haResponseValue(c.BeginBaseBackupResponse(ctx, body))
}

func (c *HAClient) FinishBaseBackupResponse(ctx context.Context, body BaseBackupManifestPathRequest) (*HAResponse[HABaseBackupFinishResponse], error) {
	resp, err := c.client.FinishHABaseBackupWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("finish HA base backup", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) FinishBaseBackup(ctx context.Context, body BaseBackupManifestPathRequest) (*HABaseBackupFinishResponse, error) {
	return haResponseValue(c.FinishBaseBackupResponse(ctx, body))
}

func (c *HAClient) BootstrapStandbyResponse(ctx context.Context, body StandbyBootstrapRequest) (*HAResponse[HAStandbyBootstrapResponse], error) {
	resp, err := c.client.BootstrapHAStandbyWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("bootstrap HA standby", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) BootstrapStandby(ctx context.Context, body StandbyBootstrapRequest) (*HAStandbyBootstrapResponse, error) {
	return haResponseValue(c.BootstrapStandbyResponse(ctx, body))
}

func (c *HAClient) AcquireFenceResponse(ctx context.Context, body FenceAcquireRequest) (*HAResponse[HAFenceResponse], error) {
	resp, err := c.client.AcquireHAFenceWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("acquire HA fence", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) AcquireFence(ctx context.Context, body FenceAcquireRequest) (*HAFenceResponse, error) {
	return haResponseValue(c.AcquireFenceResponse(ctx, body))
}

func (c *HAClient) CurrentFenceResponse(ctx context.Context) (*HAResponse[HACurrentFenceResponse], error) {
	resp, err := c.client.GetHACurrentFenceWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("get current HA fence", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) CurrentFence(ctx context.Context) (*HACurrentFenceResponse, error) {
	return haResponseValue(c.CurrentFenceResponse(ctx))
}

func (c *HAClient) AssessPromotionResponse(ctx context.Context, body PromotionAssessRequest) (*HAResponse[HAPromotionAssessResponse], error) {
	resp, err := c.client.AssessHAPromotionWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("assess HA promotion", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) AssessPromotion(ctx context.Context, body PromotionAssessRequest) (*HAPromotionAssessResponse, error) {
	return haResponseValue(c.AssessPromotionResponse(ctx, body))
}

func (c *HAClient) PromoteResponse(ctx context.Context, body FenceAcquireRequest) (*HAResponse[HAPromotionResponse], error) {
	resp, err := c.client.PromoteHAWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("promote HA standby", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) Promote(ctx context.Context, body FenceAcquireRequest) (*HAPromotionResponse, error) {
	return haResponseValue(c.PromoteResponse(ctx, body))
}

func (c *HAClient) PromoteWithCurrentFenceResponse(ctx context.Context) (*HAResponse[HAPromotionResponse], error) {
	resp, err := c.client.PromoteHAWithCurrentFenceWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("promote HA standby with current fence", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) PromoteWithCurrentFence(ctx context.Context) (*HAPromotionResponse, error) {
	return haResponseValue(c.PromoteWithCurrentFenceResponse(ctx))
}

func (c *HAClient) AssessRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*HAResponse[HARejoinAssessResponse], error) {
	resp, err := c.client.AssessHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("assess HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) AssessRejoin(ctx context.Context, body RejoinAssessRequest) (*HARejoinAssessResponse, error) {
	return haResponseValue(c.AssessRejoinResponse(ctx, body))
}

func (c *HAClient) RewindRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*HAResponse[HARejoinAssessResponse], error) {
	resp, err := c.client.RewindHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("rewind HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) RewindRejoin(ctx context.Context, body RejoinAssessRequest) (*HARejoinAssessResponse, error) {
	return haResponseValue(c.RewindRejoinResponse(ctx, body))
}

func (c *HAClient) ReseedRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*HAResponse[HARejoinAssessResponse], error) {
	resp, err := c.client.ReseedHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireHAJSON200("reseed HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err)
}

func (c *HAClient) ReseedRejoin(ctx context.Context, body RejoinAssessRequest) (*HARejoinAssessResponse, error) {
	return haResponseValue(c.ReseedRejoinResponse(ctx, body))
}
