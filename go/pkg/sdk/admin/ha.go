package admin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"github.com/antflydb/antfly/go/pkg/sdk/admin/oapi"
)

const (
	AdminV1Path                       = "/admin/v1"
	HAPath                            = AdminV1Path + "/ha"
	HAPrimaryStatusPath               = HAPath + "/primary/status"
	HAWatchdogProofPath               = HAPath + "/watchdog-proof"
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
	HABaseBackupsCapturePath          = HABaseBackupsPath + "/capture"
	HABaseBackupsActivatePath         = HABaseBackupsPath + "/activate"
	HASeedLifecycleReceiptsPath       = HAPath + "/seed-lifecycle/receipts"
	HAStandbyBootstrapPath            = HAPath + "/standby/bootstrap"
	HAStandbyUpstreamPath             = HAPath + "/standby/upstream"
	HAFencePath                       = HAPath + "/fence"
	HAFenceCurrentPath                = HAFencePath + "/current"
	HAPromotionPath                   = HAPath + "/promotion"
	HAPromotionAssessPath             = HAPath + "/promotion/assess"
	HAPromotionCurrentFencePath       = HAPath + "/promotion/current-fence"
	HARejoinAssessPath                = HAPath + "/rejoin/assess"
	HARejoinRewindPath                = HAPath + "/rejoin/rewind"
	HARejoinReseedPath                = HAPath + "/rejoin/reseed"

	maxStandbyIdentifierBytes = 128
)

// Canonical (post-deprecation-window) admin route prefix and paths for
// hot-standby endpoints. The server serves both these and the legacy
// /admin/v1/ha paths above as aliases of one another for one minor; see
// PathStyle. The Standby*Path constants intentionally mirror the legacy
// suffixes verbatim; the standby-role routes drop the segment that the new
// prefix already spells (`/standby/status`, not `/standby/standby/status`),
// which come from sub-resources named "standby" nested under the renamed
// top-level "standby" prefix), since that is what the OpenAPI spec defines.
const (
	StandbyPath                      = AdminV1Path + "/standby"
	StandbyPrimaryStatusPath         = StandbyPath + "/primary/status"
	StandbyWatchdogProofPath         = StandbyPath + "/watchdog-proof"
	StandbyStatusPath                = StandbyPath + "/status"
	StandbyCommitCheckPath           = StandbyPath + "/commit/check"
	StandbyCommitAppendPath          = StandbyPath + "/commit/append"
	StandbyReadCheckPath             = StandbyPath + "/read/check"
	StandbyWriteCheckPath            = StandbyPath + "/write/check"
	StandbyOwnerJobCheckPath         = StandbyPath + "/owner-jobs/check"
	StandbyReplicationSlotsPath      = StandbyPath + "/replication-slots"
	StandbyReplicationSlotPathPrefix = StandbyReplicationSlotsPath + "/"
	StandbyBaseBackupsPath           = StandbyPath + "/base-backups"
	StandbyBaseBackupsFinishPath     = StandbyBaseBackupsPath + "/finish"
	StandbyBaseBackupsCapturePath    = StandbyBaseBackupsPath + "/capture"
	StandbyBaseBackupsActivatePath   = StandbyBaseBackupsPath + "/activate"
	StandbySeedLifecycleReceiptsPath = StandbyPath + "/seed-lifecycle/receipts"
	StandbyBootstrapPath             = StandbyPath + "/bootstrap"
	StandbyUpstreamPath              = StandbyPath + "/upstream"
	StandbyFencePath                 = StandbyPath + "/fence"
	StandbyFenceCurrentPath          = StandbyFencePath + "/current"
	StandbyPromotionPath             = StandbyPath + "/promotion"
	StandbyPromotionAssessPath       = StandbyPath + "/promotion/assess"
	StandbyPromotionCurrentFencePath = StandbyPath + "/promotion/current-fence"
	StandbyRejoinAssessPath          = StandbyPath + "/rejoin/assess"
	StandbyRejoinRewindPath          = StandbyPath + "/rejoin/rewind"
	StandbyRejoinReseedPath          = StandbyPath + "/rejoin/reseed"
)

const adminV1Path = AdminV1Path

type (
	StandbyActionReceipt                    = oapi.StandbyActionReceipt
	StandbyActionReceiptActionKind          = oapi.StandbyActionReceiptActionKind
	StandbyActionReceiptState               = oapi.StandbyActionReceiptState
	StandbyBaseBackupBeginResponse          = oapi.StandbyBaseBackupBeginResponse
	StandbyBaseBackupFinishResponse         = oapi.StandbyBaseBackupFinishResponse
	StandbySeedArtifactCaptureResponse      = oapi.StandbySeedArtifactCaptureResponse
	StandbySeedLifecycleReceiptEvent        = oapi.StandbySeedLifecycleReceiptEvent
	StandbySeedLifecycleReceiptInventory    = oapi.StandbySeedLifecycleReceiptInventoryResponse
	StandbyRuntimeLifecycleObservation      = oapi.StandbyRuntimeLifecycleObservation
	StandbySeededSlotActivateResponse       = oapi.StandbySeededSlotActivateResponse
	StandbyCommitAppendResponse             = oapi.StandbyCommitAppendResponse
	StandbyCommitCheckResponse              = oapi.StandbyCommitCheckResponse
	StandbyCommitGate                       = oapi.StandbyCommitGate
	StandbyCommitGateAction                 = oapi.StandbyCommitGateAction
	StandbyCurrentFenceResponse             = oapi.StandbyCurrentFenceResponse
	StandbyDurabilityDecision               = oapi.StandbyDurabilityDecision
	StandbyDurabilityDecisionMode           = oapi.StandbyDurabilityDecisionMode
	StandbyDurabilityDecisionSelection      = oapi.StandbyDurabilityDecisionSelection
	StandbyDurabilityDecisionStatus         = oapi.StandbyDurabilityDecisionStatus
	StandbyFenceReceipt                     = oapi.StandbyFenceReceipt
	StandbyFenceResponse                    = oapi.StandbyFenceResponse
	StandbyIdentity                         = oapi.StandbyIdentity
	StandbyLeaseWatchdogProof               = oapi.StandbyLeaseWatchdogProof
	StandbyWatchdogProofResponse            = oapi.StandbyWatchdogProofResponse
	StandbyOwnerJobCheckResponse            = oapi.StandbyOwnerJobCheckResponse
	StandbyOwnerJobDecision                 = oapi.StandbyOwnerJobDecision
	StandbyOwnerJobDecisionAction           = oapi.StandbyOwnerJobDecisionAction
	StandbyOwnerJobDecisionKind             = oapi.StandbyOwnerJobDecisionKind
	StandbyOwnerJobDecisionRole             = oapi.StandbyOwnerJobDecisionRole
	StandbyPrimarySnapshot                  = oapi.StandbyPrimarySnapshot
	StandbyPrimarySnapshotRole              = oapi.StandbyPrimarySnapshotRole
	StandbyPrimaryStatusParams              = oapi.GetHAPrimaryStatusParams
	StandbyPrimaryStatusParamsSyncMode      = oapi.GetHAPrimaryStatusParamsSyncMode
	StandbyPrimaryStatusParamsSyncSelection = oapi.GetHAPrimaryStatusParamsSyncSelection
	StandbyPrimaryStatusParamsSyncFail      = oapi.GetHAPrimaryStatusParamsSyncFailure
	StandbyPrimaryStatusResponse            = oapi.StandbyPrimaryStatusResponse
	StandbyPromotionAssessResponse          = oapi.StandbyPromotionAssessResponse
	StandbyPromotionAssessment              = oapi.StandbyPromotionAssessment
	StandbyPromotionAssessmentMode          = oapi.StandbyPromotionAssessmentMode
	StandbyPromotionHandoff                 = oapi.StandbyPromotionHandoff
	StandbyPromotionResponse                = oapi.StandbyPromotionResponse
	StandbyPromotionResult                  = oapi.StandbyPromotionResult
	StandbyReadCheckResponse                = oapi.StandbyReadCheckResponse
	StandbyReadDecision                     = oapi.StandbyReadDecision
	StandbyReadDecisionAction               = oapi.StandbyReadDecisionAction
	StandbyReadDecisionConsistency          = oapi.StandbyReadDecisionConsistency
	StandbyRejoinAssessResponse             = oapi.StandbyRejoinAssessResponse
	StandbyRejoinAssessment                 = oapi.StandbyRejoinAssessment
	StandbyRejoinAssessmentAction           = oapi.StandbyRejoinAssessmentAction
	StandbyRejoinAssessmentReason           = oapi.StandbyRejoinAssessmentReason
	StandbyRejoinReseedResult               = oapi.StandbyRejoinReseedResult
	StandbyRejoinRewindResult               = oapi.StandbyRejoinRewindResult
	StandbyReplicationSlot                  = oapi.StandbyReplicationSlot
	StandbyReplicationSlotActionResponse    = oapi.StandbyReplicationSlotActionResponse
	StandbyReplicationSlotAction            = oapi.StandbyReplicationSlotActionResponseSlotAction
	StandbyReplicationSlotListResponse      = oapi.StandbyReplicationSlotListResponse
	StandbyRetentionSnapshot                = oapi.StandbyRetentionSnapshot
	StandbySlotSnapshot                     = oapi.StandbySlotSnapshot
	StandbySlotSnapshotStatus               = oapi.StandbySlotSnapshotStatus
	StandbySnapshot                         = oapi.StandbySnapshot
	StandbySnapshotRole                     = oapi.StandbySnapshotRole
	StandbyBootstrapResponse                = oapi.StandbyBootstrapResponse
	StandbyStatusParams                     = oapi.GetHAStandbyStatusParams
	StandbyStatusResponse                   = oapi.StandbyStatusResponse
	StandbyUpstream                         = oapi.StandbyUpstream
	StandbyUpstreamResponse                 = oapi.StandbyUpstreamResponse
	StandbySyncPolicy                       = oapi.StandbySyncPolicy
	StandbySyncPolicyFailurePolicy          = oapi.StandbySyncPolicyFailurePolicy
	StandbySyncPolicyMode                   = oapi.StandbySyncPolicyMode
	StandbySyncPolicySelection              = oapi.StandbySyncPolicySelection
	StandbyWriteCheckResponse               = oapi.StandbyWriteCheckResponse
	StandbyWriteDecision                    = oapi.StandbyWriteDecision
	StandbyWriteDecisionAction              = oapi.StandbyWriteDecisionAction
	StandbyWriteDecisionRole                = oapi.StandbyWriteDecisionRole

	BaseBackupManifestPathRequest     = oapi.BaseBackupManifestPathRequest
	BaseBackupStartRequest            = oapi.BaseBackupStartRequest
	CommitAppendRequest               = oapi.CommitAppendRequest
	CommitAppendRequestKind           = oapi.CommitAppendRequestKind
	CommitAppendRequestCodec          = oapi.CommitAppendRequestPayloadCodec
	CommitCheckRequest                = oapi.CommitCheckRequest
	FenceAcquireRequest               = oapi.FenceAcquireRequest
	OwnerJobCheckRequest              = oapi.OwnerJobCheckRequest
	OwnerJobCheckRequestKind          = oapi.OwnerJobCheckRequestKind
	OwnerJobCheckRequestRole          = oapi.OwnerJobCheckRequestRole
	PromotionAssessRequest            = oapi.PromotionAssessRequest
	ReadCheckRequest                  = oapi.ReadCheckRequest
	ReadCheckRequestConsistency       = oapi.ReadCheckRequestConsistency
	RejoinAssessRequest               = oapi.RejoinAssessRequest
	ReplicationSlotCreateRequest      = oapi.ReplicationSlotCreateRequest
	StandbyBootstrapRequest           = oapi.StandbyBootstrapRequest
	StandbyUpstreamRequest            = oapi.StandbyUpstreamRequest
	SeededSlotActivateRequest         = oapi.SeededSlotActivateRequest
	SeedArtifactCaptureRequest        = oapi.SeedArtifactCaptureRequest
	StandbySeedLifecycleReceiptParams = oapi.GetHASeedLifecycleReceiptsParams
	StandbySeedLifecycleReceiptKind   = oapi.GetHASeedLifecycleReceiptsParamsKind
	WriteCheckRequest                 = oapi.WriteCheckRequest
	WriteCheckRequestRole             = oapi.WriteCheckRequestRole
)

const (
	StandbySeedLifecycleReceiptKindCapture    = oapi.GetHASeedLifecycleReceiptsParamsKindCapture
	StandbySeedLifecycleReceiptKindActivation = oapi.GetHASeedLifecycleReceiptsParamsKindActivation
)

const (
	StandbyActionKindBaseBackupBegin       = oapi.StandbyActionReceiptActionKindBaseBackupBegin
	StandbyActionKindBaseBackupFinish      = oapi.StandbyActionReceiptActionKindBaseBackupFinish
	StandbyActionKindSeedCapture           = oapi.StandbyActionReceiptActionKindSeedCapture
	StandbyActionKindSeededSlotActivate    = oapi.StandbyActionReceiptActionKindSeededSlotActivate
	StandbyActionKindFenceAcquire          = oapi.StandbyActionReceiptActionKindFenceAcquire
	StandbyActionKindPromotion             = oapi.StandbyActionReceiptActionKindPromotion
	StandbyActionKindPromotionAssess       = oapi.StandbyActionReceiptActionKindPromotionAssess
	StandbyActionKindRejoinAssess          = oapi.StandbyActionReceiptActionKindRejoinAssess
	StandbyActionKindRejoinReseed          = oapi.StandbyActionReceiptActionKindRejoinReseed
	StandbyActionKindRejoinRewind          = oapi.StandbyActionReceiptActionKindRejoinRewind
	StandbyActionKindReplicationSlotCreate = oapi.StandbyActionReceiptActionKindReplicationSlotCreate
	StandbyActionKindReplicationSlotDrop   = oapi.StandbyActionReceiptActionKindReplicationSlotDrop
	StandbyActionKindReplicationSlotPause  = oapi.StandbyActionReceiptActionKindReplicationSlotPause
	StandbyActionKindReplicationSlotResume = oapi.StandbyActionReceiptActionKindReplicationSlotResume
	StandbyActionKindStandbyBootstrap      = oapi.StandbyActionReceiptActionKindStandbyBootstrap
	StandbyActionKindStandbyUpstream       = oapi.StandbyActionReceiptActionKindStandbyUpstream

	StandbyActionStateAlreadyApplied = oapi.StandbyActionReceiptStateAlreadyApplied
	StandbyActionStateApplied        = oapi.StandbyActionReceiptStateApplied
	StandbyActionStateAssessed       = oapi.StandbyActionReceiptStateAssessed

	StandbyPrimarySnapshotRolePrimary = oapi.StandbyPrimarySnapshotRolePrimary

	StandbySnapshotRoleStandby = oapi.StandbySnapshotRoleStandby

	StandbySlotSnapshotStatusHealthy        = oapi.StandbySlotSnapshotStatusHealthy
	StandbySlotSnapshotStatusLagging        = oapi.StandbySlotSnapshotStatusLagging
	StandbySlotSnapshotStatusReseedRequired = oapi.StandbySlotSnapshotStatusReseedRequired

	StandbyDurabilityStatusSatisfied       = oapi.StandbyDurabilityDecisionStatusSatisfied
	StandbyDurabilityStatusWouldBlock      = oapi.StandbyDurabilityDecisionStatusWouldBlock
	StandbyDurabilityStatusFailClosed      = oapi.StandbyDurabilityDecisionStatusFailClosed
	StandbyDurabilityStatusDegradedToAsync = oapi.StandbyDurabilityDecisionStatusDegradedToAsync

	StandbyDurabilityModeAsync       = oapi.StandbyDurabilityDecisionModeAsync
	StandbyDurabilityModeRemoteWrite = oapi.StandbyDurabilityDecisionModeRemoteWrite
	StandbyDurabilityModeRemoteApply = oapi.StandbyDurabilityDecisionModeRemoteApply

	StandbyDurabilitySelectionAny   = oapi.StandbyDurabilityDecisionSelectionAny
	StandbyDurabilitySelectionFirst = oapi.StandbyDurabilityDecisionSelectionFirst
	StandbyDurabilitySelectionAll   = oapi.StandbyDurabilityDecisionSelectionAll

	StandbyPrimaryStatusSyncModeAsync       = oapi.GetHAPrimaryStatusParamsSyncModeAsync
	StandbyPrimaryStatusSyncModeRemoteWrite = oapi.GetHAPrimaryStatusParamsSyncModeRemoteWrite
	StandbyPrimaryStatusSyncModeRemoteApply = oapi.GetHAPrimaryStatusParamsSyncModeRemoteApply

	StandbyPrimaryStatusSyncSelectionAny   = oapi.GetHAPrimaryStatusParamsSyncSelectionAny
	StandbyPrimaryStatusSyncSelectionFirst = oapi.GetHAPrimaryStatusParamsSyncSelectionFirst
	StandbyPrimaryStatusSyncSelectionAll   = oapi.GetHAPrimaryStatusParamsSyncSelectionAll

	StandbyPrimaryStatusSyncFailureBlock          = oapi.GetHAPrimaryStatusParamsSyncFailureBlock
	StandbyPrimaryStatusSyncFailureFailClosed     = oapi.GetHAPrimaryStatusParamsSyncFailureFailClosed
	StandbyPrimaryStatusSyncFailureDegradeToAsync = oapi.GetHAPrimaryStatusParamsSyncFailureDegradeToAsync

	StandbySyncPolicyModeAsync       = oapi.StandbySyncPolicyModeAsync
	StandbySyncPolicyModeRemoteWrite = oapi.StandbySyncPolicyModeRemoteWrite
	StandbySyncPolicyModeRemoteApply = oapi.StandbySyncPolicyModeRemoteApply

	StandbySyncPolicySelectionAny   = oapi.StandbySyncPolicySelectionAny
	StandbySyncPolicySelectionFirst = oapi.StandbySyncPolicySelectionFirst
	StandbySyncPolicySelectionAll   = oapi.StandbySyncPolicySelectionAll

	StandbySyncPolicyFailureBlock          = oapi.StandbySyncPolicyFailurePolicyBlock
	StandbySyncPolicyFailureFailClosed     = oapi.StandbySyncPolicyFailurePolicyFailClosed
	StandbySyncPolicyFailureDegradeToAsync = oapi.StandbySyncPolicyFailurePolicyDegradeToAsync

	StandbyCommitGateActionAcknowledge         = oapi.StandbyCommitGateActionAcknowledge
	StandbyCommitGateActionAcknowledgeDegraded = oapi.StandbyCommitGateActionAcknowledgeDegraded
	StandbyCommitGateActionReject              = oapi.StandbyCommitGateActionReject
	StandbyCommitGateActionWaitForStandby      = oapi.StandbyCommitGateActionWaitForStandby

	StandbyReadDecisionActionRouteToPrimary  = oapi.StandbyReadDecisionActionRouteToPrimary
	StandbyReadDecisionActionServeStandby    = oapi.StandbyReadDecisionActionServeStandby
	StandbyReadDecisionActionWaitForApply    = oapi.StandbyReadDecisionActionWaitForApply
	StandbyReadDecisionActionWaitForMetadata = oapi.StandbyReadDecisionActionWaitForMetadata

	StandbyReadDecisionConsistencyAtLeastLSN = oapi.StandbyReadDecisionConsistencyAtLeastLsn
	StandbyReadDecisionConsistencyPrimary    = oapi.StandbyReadDecisionConsistencyPrimary
	StandbyReadDecisionConsistencyStaleOK    = oapi.StandbyReadDecisionConsistencyStaleOk

	StandbyWriteDecisionActionAllowWrite          = oapi.StandbyWriteDecisionActionAllowWrite
	StandbyWriteDecisionActionOpenPromotedPrimary = oapi.StandbyWriteDecisionActionOpenPromotedPrimary
	StandbyWriteDecisionActionRejectFencedPrimary = oapi.StandbyWriteDecisionActionRejectFencedPrimary
	StandbyWriteDecisionActionRejectReadOnly      = oapi.StandbyWriteDecisionActionRejectReadOnlyStandby

	StandbyWriteDecisionRoleFencedPrimary   = oapi.StandbyWriteDecisionRoleFencedPrimary
	StandbyWriteDecisionRolePrimary         = oapi.StandbyWriteDecisionRolePrimary
	StandbyWriteDecisionRolePromotedStandby = oapi.StandbyWriteDecisionRolePromotedStandby
	StandbyWriteDecisionRoleStandby         = oapi.StandbyWriteDecisionRoleStandby

	StandbyOwnerJobDecisionActionDisableOnStandby    = oapi.StandbyOwnerJobDecisionActionDisableOnStandby
	StandbyOwnerJobDecisionActionOpenPromotedPrimary = oapi.StandbyOwnerJobDecisionActionOpenPromotedPrimary
	StandbyOwnerJobDecisionActionRun                 = oapi.StandbyOwnerJobDecisionActionRun

	StandbyOwnerJobDecisionKindCompactionPublish   = oapi.StandbyOwnerJobDecisionKindCompactionPublish
	StandbyOwnerJobDecisionKindDerivedEffectWriter = oapi.StandbyOwnerJobDecisionKindDerivedEffectWriter
	StandbyOwnerJobDecisionKindEnrichmentWriter    = oapi.StandbyOwnerJobDecisionKindEnrichmentWriter
	StandbyOwnerJobDecisionKindRetentionAdvance    = oapi.StandbyOwnerJobDecisionKindRetentionAdvance

	StandbyOwnerJobDecisionRolePrimary         = oapi.StandbyOwnerJobDecisionRolePrimary
	StandbyOwnerJobDecisionRolePromotedStandby = oapi.StandbyOwnerJobDecisionRolePromotedStandby
	StandbyOwnerJobDecisionRoleStandby         = oapi.StandbyOwnerJobDecisionRoleStandby

	StandbyPromotionModeBlocked = oapi.StandbyPromotionAssessmentModeBlocked
	StandbyPromotionModeSafe    = oapi.StandbyPromotionAssessmentModeSafe
	StandbyPromotionModeForced  = oapi.StandbyPromotionAssessmentModeForced
	StandbyPromotionModeLossy   = oapi.StandbyPromotionAssessmentModeLossy

	StandbyRejoinActionAlreadyCurrent = oapi.StandbyRejoinAssessmentActionAlreadyCurrent
	StandbyRejoinActionRejectUnfenced = oapi.StandbyRejoinAssessmentActionRejectUnfenced
	StandbyRejoinActionReseed         = oapi.StandbyRejoinAssessmentActionReseed
	StandbyRejoinActionRewind         = oapi.StandbyRejoinAssessmentActionRewind

	StandbyRejoinReasonCurrentTimeline          = oapi.StandbyRejoinAssessmentReasonCurrentTimeline
	StandbyRejoinReasonIncompatibleTimeline     = oapi.StandbyRejoinAssessmentReasonIncompatibleTimeline
	StandbyRejoinReasonLocalLSNBeforeFork       = oapi.StandbyRejoinAssessmentReasonLocalLsnBeforeFork
	StandbyRejoinReasonNoFence                  = oapi.StandbyRejoinAssessmentReasonNoFence
	StandbyRejoinReasonParentTimelineRetained   = oapi.StandbyRejoinAssessmentReasonParentTimelineRetained
	StandbyRejoinReasonParentTimelineWALExpired = oapi.StandbyRejoinAssessmentReasonParentTimelineWalExpired
	StandbyRejoinReasonWrongCluster             = oapi.StandbyRejoinAssessmentReasonWrongCluster
	StandbyRejoinReasonWrongOldPrimary          = oapi.StandbyRejoinAssessmentReasonWrongOldPrimary
	StandbyRejoinReasonWrongShard               = oapi.StandbyRejoinAssessmentReasonWrongShard
	StandbyRejoinReasonWrongTable               = oapi.StandbyRejoinAssessmentReasonWrongTable

	StandbyReplicationSlotActionCreate = oapi.StandbyReplicationSlotActionResponseSlotActionCreate
	StandbyReplicationSlotActionDrop   = oapi.StandbyReplicationSlotActionResponseSlotActionDrop
	StandbyReplicationSlotActionPause  = oapi.StandbyReplicationSlotActionResponseSlotActionPause
	StandbyReplicationSlotActionResume = oapi.StandbyReplicationSlotActionResponseSlotActionResume

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

// PathStyle selects which admin route prefix a StandbyClient sends requests
// to: the legacy /admin/v1/ha prefix or the canonical /admin/v1/standby
// prefix. The server accepts both for one minor after the rename.
type PathStyle int

const (
	// PathStyleLegacy sends requests to /admin/v1/ha/... . This is the
	// zero value and the default for this release; PathStyleAuto is the
	// recommended setting and the one the Kubernetes operator uses.
	PathStyleLegacy PathStyle = iota
	// PathStyleCanonical sends requests to /admin/v1/standby/... .
	PathStyleCanonical
	// PathStyleAuto negotiates: it sends the canonical /admin/v1/standby/...
	// path first and, on a 404 that looks like an unrouted path rather than
	// a missing resource, retries the legacy /admin/v1/ha/... spelling once.
	// Whichever spelling answers is remembered for the client and, keyed by
	// base URL, for later clients in the same process, so a short-lived
	// client (one per reconcile, say) does not pay the probe every time. A
	// remembered spelling is re-probed only when it starts returning
	// unrouted 404s, which covers a server rollback.
	PathStyleAuto
)

// negotiatedPathStyles remembers, per normalized admin base URL, which
// spelling a server answered under PathStyleAuto.
var negotiatedPathStyles sync.Map

// ResetNegotiatedPathStyles forgets every spelling remembered by
// PathStyleAuto. Intended for tests that reuse a base URL across servers.
func ResetNegotiatedPathStyles() {
	negotiatedPathStyles.Range(func(key, _ any) bool {
		negotiatedPathStyles.Delete(key)
		return true
	})
}

// pathNegotiator holds a client's PathStyleAuto state.
type pathNegotiator struct {
	mu       sync.Mutex
	cacheKey string
	style    PathStyle
	pinned   bool
}

func newPathNegotiator(cacheKey string) *pathNegotiator {
	n := &pathNegotiator{cacheKey: cacheKey, style: PathStyleCanonical}
	if cached, ok := negotiatedPathStyles.Load(cacheKey); ok {
		n.style = cached.(PathStyle)
		n.pinned = true
	}
	return n
}

func (n *pathNegotiator) current() (PathStyle, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.style, n.pinned
}

func (n *pathNegotiator) pin(style PathStyle) {
	n.mu.Lock()
	n.style = style
	n.pinned = true
	n.mu.Unlock()
	negotiatedPathStyles.Store(n.cacheKey, style)
}

// pathNegotiatingDoer wraps the HTTP doer the generated client uses and
// implements PathStyleAuto. The generated client always builds canonical
// paths; this doer rewrites and retries them as the negotiation dictates.
type pathNegotiatingDoer struct {
	inner  oapi.HttpRequestDoer
	client *StandbyClient
}

func (d *pathNegotiatingDoer) Do(req *http.Request) (*http.Response, error) {
	c := d.client
	if c == nil || c.pathStyle != PathStyleAuto || req == nil || req.URL == nil {
		return d.inner.Do(req)
	}
	legacyPath, ok := legacyAdminRequestPath(req.URL.Path)
	if !ok {
		return d.inner.Do(req)
	}
	canonicalPath := req.URL.Path
	pathFor := func(style PathStyle) string {
		if style == PathStyleLegacy {
			return legacyPath
		}
		return canonicalPath
	}

	first, pinned := c.negotiator.current()
	firstReq := req
	if first == PathStyleLegacy {
		firstReq = requestWithPath(req, legacyPath)
	}
	resp, err := d.inner.Do(firstReq)
	if err != nil {
		return resp, err
	}
	if resp.StatusCode != http.StatusNotFound {
		if !pinned {
			c.negotiator.pin(first)
		}
		return resp, nil
	}

	// A 404 means either the spelling is unknown to this server or the
	// resource is genuinely absent. Once a spelling is pinned, only an
	// unrouted-looking body triggers a re-probe, so a real SlotNotFound
	// does not cost an extra round trip.
	body, restored := peekBody(resp)
	if !restored || (pinned && !looksUnrouted(body)) {
		return resp, nil
	}
	other := PathStyleLegacy
	if first == PathStyleLegacy {
		other = PathStyleCanonical
	}
	retryReq, ok := replayableRequestWithPath(req, pathFor(other))
	if !ok {
		return resp, nil
	}
	retryResp, retryErr := d.inner.Do(retryReq)
	if retryErr != nil || retryResp.StatusCode == http.StatusNotFound {
		if retryResp != nil && retryResp.Body != nil {
			_ = retryResp.Body.Close()
		}
		return resp, nil
	}
	_ = resp.Body.Close()
	c.negotiator.pin(other)
	return retryResp, nil
}

// requestWithPath returns a shallow clone of req addressed at path. The body
// is shared, so the clone must be the only one sent.
func requestWithPath(req *http.Request, path string) *http.Request {
	clone := req.Clone(req.Context())
	clone.URL.Path = path
	if clone.URL.RawPath != "" {
		clone.URL.RawPath = path
	}
	return clone
}

// replayableRequestWithPath is requestWithPath for a request whose body may
// already have been consumed; it reports false when the body cannot be
// replayed.
func replayableRequestWithPath(req *http.Request, path string) (*http.Request, bool) {
	clone := requestWithPath(req, path)
	if req.Body == nil || req.Body == http.NoBody {
		return clone, true
	}
	if req.GetBody == nil {
		return nil, false
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, false
	}
	clone.Body = body
	return clone, true
}

// peekBody reads a response body so it can be inspected and then restores it
// for the caller.
func peekBody(resp *http.Response) ([]byte, bool) {
	if resp.Body == nil {
		return nil, true
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if err != nil {
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return body, false
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return body, true
}

// looksUnrouted reports whether a 404 body reads like a router miss rather
// than a typed resource error such as SlotNotFound.
func looksUnrouted(body []byte) bool {
	text := strings.ToLower(strings.TrimSpace(string(body)))
	return text == "" || strings.Contains(text, "not found") || strings.HasPrefix(text, "404")
}

// StandbyClient is a typed client for the stable /admin/v1/standby API
// (still /admin/v1/ha by default this release; see PathStyle).
type StandbyClient struct {
	client     *oapi.ClientWithResponses
	editors    []oapi.RequestEditorFn
	authEditor oapi.RequestEditorFn
	pathStyle  PathStyle
	negotiator *pathNegotiator
}

// rebuildEditors recomputes the request editor chain from the client's
// current auth and path-style configuration. It must be called whenever
// either changes.
func (c *StandbyClient) rebuildEditors() {
	editors := []oapi.RequestEditorFn{acceptJSONEditor}
	if c.authEditor != nil {
		editors = append(editors, c.authEditor)
	}
	if c.pathStyle == PathStyleLegacy {
		editors = append(editors, legacyPathStyleEditor)
	}
	c.editors = editors
}

// legacyPathStyleEditor rewrites the canonical /admin/v1/standby/... request
// path the generated client builds (from the current OpenAPI spec) back to
// the legacy /admin/v1/ha/... path the server also still accepts. Removed
// once PathStyleLegacy is removed.
func legacyPathStyleEditor(_ context.Context, req *http.Request) error {
	if req == nil || req.URL == nil {
		return nil
	}
	if newPath, ok := legacyAdminRequestPath(req.URL.Path); ok {
		req.URL.Path = newPath
	}
	if req.URL.RawPath != "" {
		if newRawPath, ok := legacyAdminRequestPath(req.URL.RawPath); ok {
			req.URL.RawPath = newRawPath
		}
	}
	return nil
}

// legacyAdminRequestPath maps a canonical /admin/v1/standby/... request path
// onto its legacy /admin/v1/ha/... spelling. It reports false for paths
// outside the standby admin surface.
func legacyAdminRequestPath(path string) (string, bool) {
	// Standby-role routes dropped their redundant segment with the new
	// prefix, so they are not a plain prefix swap.
	switch path {
	case StandbyStatusPath:
		return HAStandbyStatusPath, true
	case StandbyBootstrapPath:
		return HAStandbyBootstrapPath, true
	case StandbyUpstreamPath:
		return HAStandbyUpstreamPath, true
	}
	if path == StandbyPath {
		return HAPath, true
	}
	if strings.HasPrefix(path, StandbyPath+"/") {
		return HAPath + strings.TrimPrefix(path, StandbyPath), true
	}
	return path, false
}

// StandbyOperation identifies a stable /admin/v1/ha method and full admin path.
// Operator status and automation should use these values rather than carrying a
// separate route table outside the admin SDK wrapper.
type StandbyOperation struct {
	Method string
	Path   string
}

// StandbyReceiptExpectation identifies the generated action receipt kind/state a
// successful idempotent admin operation should return.
type StandbyReceiptExpectation struct {
	ActionKind StandbyActionReceiptActionKind
	State      StandbyActionReceiptState
}

func (e StandbyReceiptExpectation) Strings() (string, string) {
	return string(e.ActionKind), string(e.State)
}

// StandbyReceiptMatches verifies that a node-local HA admin receipt matches the
// expected operation and acted-on target.
func StandbyReceiptMatches(receipt StandbyActionReceipt, expectation StandbyReceiptExpectation, expectedTarget string) bool {
	actionID := receipt.ActionId
	actionKind := string(receipt.ActionKind)
	actionTarget := receipt.Target
	actionState := string(receipt.State)
	expectedKind := string(expectation.ActionKind)
	expectedState := string(expectation.State)
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
		if expectedState != string(StandbyActionStateApplied) || actionState != string(StandbyActionStateAlreadyApplied) {
			return false
		}
	}
	return actionID == expectedKind+":"+expectedTarget
}

// StandbyReceiptNodeMatches verifies that the node-local endpoint that returned a
// receipt is the intended endpoint. Some compatibility paths can tolerate an
// unset expected node id, but typed direct admin execution should require it.
func StandbyReceiptNodeMatches(receipt StandbyActionReceipt, expectedNodeID string, requireExpectedNode bool) bool {
	nodeID := receipt.NodeId
	if !validStandbyIdentifier(nodeID) {
		return false
	}
	if expectedNodeID == "" {
		return !requireExpectedNode
	}
	return validStandbyIdentifier(expectedNodeID) && nodeID == expectedNodeID
}

func StandbyReceiptMatchesNode(receipt StandbyActionReceipt, expectation StandbyReceiptExpectation, expectedTarget string, expectedNodeID string, requireExpectedNode bool) bool {
	return StandbyReceiptMatches(receipt, expectation, expectedTarget) &&
		StandbyReceiptNodeMatches(receipt, expectedNodeID, requireExpectedNode)
}

func ValidateStandbyReplicationSlotActionResponse(response StandbyReplicationSlotActionResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing replication slot action schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing replication slot action receipt")
	}
	switch response.SlotAction {
	case StandbyReplicationSlotActionCreate, StandbyReplicationSlotActionDrop, StandbyReplicationSlotActionPause, StandbyReplicationSlotActionResume:
	default:
		return fmt.Errorf("invalid replication slot action %q", response.SlotAction)
	}
	if !StandbyReplicationSlotComplete(response.Slot) {
		return fmt.Errorf("missing replication slot action slot fields")
	}
	if err := validateStandbyReplicationSlotActionCorrelation(response); err != nil {
		return err
	}
	return nil
}

func ValidateStandbyReplicationSlotListResponse(response StandbyReplicationSlotListResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing replication slot list schema_version")
	}
	for _, slot := range response.Slots {
		if !StandbyReplicationSlotComplete(slot) {
			return fmt.Errorf("missing replication slot list slot fields")
		}
	}
	return nil
}

func ValidateStandbyReplicationSlotActionResponseEvidence(raw []byte) error {
	var response standbyReplicationSlotActionResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !standbyReplicationSlotEvidenceComplete(response.Slot) {
		return fmt.Errorf("missing replication slot action slot field evidence")
	}
	return nil
}

func ValidateStandbyReplicationSlotListResponseEvidence(raw []byte) error {
	var response standbyReplicationSlotListResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.Slots == nil {
		return fmt.Errorf("missing replication slot list slots field evidence")
	}
	for i, slot := range *response.Slots {
		if !standbyReplicationSlotEvidenceComplete(slot) {
			return fmt.Errorf("missing replication slot list slot field evidence at index %d", i)
		}
	}
	return nil
}

func ValidateStandbyBaseBackupBeginResponse(response StandbyBaseBackupBeginResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing base backup begin schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
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
	if err := validateStandbyActionReceiptTarget(response.Action, StandbyBaseBackupBeginReceiptExpectation(), response.ManifestId, "base backup begin"); err != nil {
		return err
	}
	return nil
}

func ValidateStandbyBaseBackupBeginResponseEvidence(raw []byte) error {
	var response standbyBaseBackupBeginResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.BackupLsn == nil || response.StartRecordLsn == nil {
		return fmt.Errorf("missing base backup begin field evidence")
	}
	return nil
}

func ValidateStandbyBaseBackupFinishResponse(response StandbyBaseBackupFinishResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing base backup finish schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
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
	if err := validateStandbyActionReceiptTarget(response.Action, StandbyBaseBackupFinishReceiptExpectation(), response.ManifestId, "base backup finish"); err != nil {
		return err
	}
	return nil
}

func ValidateStandbyBaseBackupFinishResponseEvidence(raw []byte) error {
	var response standbyBaseBackupFinishResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.BackupLsn == nil || response.EndRecordLsn == nil {
		return fmt.Errorf("missing base backup finish field evidence")
	}
	return nil
}

func ValidateStandbySeedArtifactCaptureResponse(response StandbySeedArtifactCaptureResponse) error {
	if response.SchemaVersion == 0 || !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing seed capture schema or action receipt")
	}
	if err := validateStandbyReplicationSlotNameForRequest("seed capture", response.SlotName); err != nil {
		return err
	}
	if !validStandbyIdentifier(response.Generation) || strings.TrimSpace(response.ManifestId) == "" {
		return fmt.Errorf("invalid seed capture generation or manifest")
	}
	for field, value := range map[string]string{
		"topology_id":     response.TopologyId,
		"node_id":         response.NodeId,
		"target_pvc_name": response.TargetPvcName,
		"target_pvc_uid":  response.TargetPvcUid,
	} {
		if err := validateStandbyIdentifierForRequest("seed capture", field, value); err != nil {
			return err
		}
	}
	if response.TopologyGeneration == 0 {
		return fmt.Errorf("missing seed capture topology_generation")
	}
	if response.ClusterId == 0 || response.TimelineId == 0 || response.Epoch == 0 ||
		response.BackupLsn == 0 || response.CheckpointLsn == 0 || response.EndRecordLsn == 0 || response.FileCount == 0 {
		return fmt.Errorf("missing seed capture identity, checkpoint, or file evidence")
	}
	if !validSHA256Hex(response.SourcePlanSha256) || !validSHA256Hex(response.ManifestSha256) ||
		!validSHA256Hex(response.CaptureReceiptSha256) {
		return fmt.Errorf("invalid seed capture digest evidence")
	}
	for field, value := range map[string]string{
		"generation_root": response.GenerationRoot,
		"content_root":    response.ContentRoot,
		"manifest_path":   response.ManifestPath,
	} {
		if err := validateStandbyPathForRequest("seed capture", field, value); err != nil {
			return err
		}
	}
	if err := validateStandbyActionReceiptTarget(response.Action, StandbySeedCaptureReceiptExpectation(), response.Generation, "seed capture"); err != nil {
		return err
	}
	return nil
}

func ValidateStandbySeedArtifactCaptureResponseEvidence(raw []byte) error {
	var response standbySeedArtifactCaptureResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.ClusterId == nil || response.ShardId == nil || response.TableId == nil ||
		response.TimelineId == nil || response.Epoch == nil || response.BackupLsn == nil ||
		response.CheckpointLsn == nil || response.EndRecordLsn == nil || response.FileCount == nil ||
		response.TotalBytes == nil || response.AlreadyCaptured == nil || response.TopologyId == nil ||
		response.TopologyGeneration == nil || response.NodeId == nil || response.TargetPvcName == nil ||
		response.TargetPvcUid == nil || response.CaptureReceiptSha256 == nil {
		return fmt.Errorf("missing seed capture field evidence")
	}
	return nil
}

func ValidateStandbySeedLifecycleReceiptInventory(response StandbySeedLifecycleReceiptInventory) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing seed lifecycle inventory schema_version")
	}
	if response.Runtime.ObservedAtUnixNs == 0 {
		return fmt.Errorf("missing seed lifecycle runtime observation timestamp")
	}
	if response.Runtime.NodeId != "" && !validStandbyIdentifier(response.Runtime.NodeId) {
		return fmt.Errorf("invalid seed lifecycle runtime node_id")
	}
	if response.Runtime.PodUid != "" && !validStandbyIdentifier(response.Runtime.PodUid) {
		return fmt.Errorf("invalid seed lifecycle runtime pod_uid")
	}
	switch response.Runtime.Role {
	case oapi.StandbyRuntimeLifecycleObservationRolePrimary, oapi.StandbyRuntimeLifecycleObservationRoleStandby, oapi.StandbyRuntimeLifecycleObservationRoleUnknown:
	default:
		return fmt.Errorf("invalid seed lifecycle runtime role %q", response.Runtime.Role)
	}
	if response.EndCursor == 0 {
		if response.FirstCursor != 0 || len(response.Entries) != 0 {
			return fmt.Errorf("invalid empty seed lifecycle cursor range")
		}
	} else if response.FirstCursor == 0 || response.FirstCursor > response.EndCursor {
		return fmt.Errorf("invalid seed lifecycle cursor range")
	}
	if response.HistoryTruncated != (response.FirstCursor > 1) {
		return fmt.Errorf("inconsistent seed lifecycle truncation evidence")
	}
	var previous uint64
	for i, event := range response.Entries {
		if event.Cursor == 0 || event.Cursor <= previous || event.Cursor > response.EndCursor {
			return fmt.Errorf("invalid seed lifecycle event cursor at index %d", i)
		}
		previous = event.Cursor
		if err := validateStandbySeedLifecycleReceiptEvent(event); err != nil {
			return fmt.Errorf("seed lifecycle event %d: %w", i, err)
		}
	}
	if len(response.Entries) > 0 && response.NextCursor != response.Entries[len(response.Entries)-1].Cursor {
		return fmt.Errorf("seed lifecycle next_cursor does not match returned page")
	}
	return nil
}

func validateStandbySeedLifecycleReceiptEvent(event StandbySeedLifecycleReceiptEvent) error {
	for field, value := range map[string]string{
		"generation":      event.Generation,
		"slot_name":       event.SlotName,
		"topology_id":     event.TopologyId,
		"node_id":         event.NodeId,
		"target_pvc_name": event.TargetPvcName,
		"target_pvc_uid":  event.TargetPvcUid,
	} {
		if !validStandbyIdentifier(value) {
			return fmt.Errorf("invalid %s", field)
		}
	}
	if event.TopologyGeneration == 0 || event.RecordedAtUnixNs == 0 || !validSHA256Hex(event.ReceiptSha256) {
		return fmt.Errorf("missing topology, timestamp, or digest evidence")
	}
	if event.PodUid != "" && !validStandbyIdentifier(event.PodUid) {
		return fmt.Errorf("invalid pod_uid")
	}
	switch event.AuthoritativeState {
	case oapi.StandbySeedLifecycleReceiptEventAuthoritativeStateRetained, oapi.StandbySeedLifecycleReceiptEventAuthoritativeStateMissing:
	default:
		return fmt.Errorf("invalid authoritative_state %q", event.AuthoritativeState)
	}
	var receipt struct {
		FormatVersion      uint16 `json:"format_version"`
		Generation         string `json:"generation"`
		SlotName           string `json:"slot_name"`
		TopologyId         string `json:"topology_id"`
		TopologyGeneration uint64 `json:"topology_generation"`
		NodeId             string `json:"node_id"`
		TargetPvcName      string `json:"target_pvc_name"`
		TargetPvcUid       string `json:"target_pvc_uid"`
	}
	if err := json.Unmarshal([]byte(event.ReceiptJson), &receipt); err != nil {
		return fmt.Errorf("invalid receipt_json: %w", err)
	}
	digest := sha256.Sum256([]byte(event.ReceiptJson))
	if fmt.Sprintf("%x", digest[:]) != event.ReceiptSha256 {
		return fmt.Errorf("receipt_json digest mismatch")
	}
	if receipt.Generation != event.Generation || receipt.SlotName != event.SlotName ||
		receipt.TopologyId != event.TopologyId || receipt.TopologyGeneration != event.TopologyGeneration ||
		receipt.NodeId != event.NodeId || receipt.TargetPvcName != event.TargetPvcName || receipt.TargetPvcUid != event.TargetPvcUid {
		return fmt.Errorf("receipt_json lifecycle binding mismatch")
	}
	switch event.Kind {
	case oapi.StandbySeedLifecycleReceiptEventKindCapture:
		if receipt.FormatVersion != 2 {
			return fmt.Errorf("invalid capture receipt format_version")
		}
	case oapi.StandbySeedLifecycleReceiptEventKindActivation:
		if receipt.FormatVersion != 1 {
			return fmt.Errorf("invalid activation receipt format_version")
		}
	default:
		return fmt.Errorf("invalid lifecycle receipt kind %q", event.Kind)
	}
	return nil
}

func ValidateStandbySeedLifecycleReceiptInventoryEvidence(raw []byte) error {
	var response standbySeedLifecycleReceiptInventoryEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.SchemaVersion == nil || response.Entries == nil || response.FirstCursor == nil ||
		response.EndCursor == nil || response.NextCursor == nil || response.HistoryTruncated == nil ||
		response.Gap == nil || response.HasMore == nil || response.Runtime.Role == nil ||
		response.Runtime.Fenced == nil || response.Runtime.ObservedAtUnixNs == nil {
		return fmt.Errorf("missing seed lifecycle inventory field evidence")
	}
	for _, event := range *response.Entries {
		if event.Cursor == nil || event.Kind == nil || event.Generation == nil || event.SlotName == nil ||
			event.TopologyId == nil || event.TopologyGeneration == nil || event.NodeId == nil ||
			event.TargetPvcName == nil || event.TargetPvcUid == nil || event.ReceiptSha256 == nil ||
			event.ReceiptJson == nil || event.RecordedAtUnixNs == nil || event.AuthoritativeState == nil {
			return fmt.Errorf("missing seed lifecycle event field evidence")
		}
	}
	return nil
}

func ValidateStandbySeededSlotActivateResponse(response StandbySeededSlotActivateResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing seeded slot activate schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing seeded slot activate action receipt")
	}
	if err := validateStandbyReplicationSlotNameForRequest("seeded slot activate", response.SlotName); err != nil {
		return err
	}
	if !validStandbyIdentifier(response.Generation) {
		return fmt.Errorf("invalid seeded slot activate generation")
	}
	if strings.TrimSpace(response.ManifestId) == "" || response.TimelineId == 0 || response.CheckpointLsn == 0 {
		return fmt.Errorf("missing seeded slot activate identity or checkpoint")
	}
	if !validSHA256Hex(response.SeedReceiptSha256) || !validSHA256Hex(response.CaptureReceiptSha256) ||
		!validSHA256Hex(response.ManifestSha256) || !validSHA256Hex(response.AggregateSha256) {
		return fmt.Errorf("invalid seeded slot activate digest evidence")
	}
	if err := validateStandbyActionReceiptTarget(response.Action, StandbySeededSlotActivateReceiptExpectation(), response.Generation, "seeded slot activate"); err != nil {
		return err
	}
	return nil
}

func ValidateStandbySeededSlotActivateResponseEvidence(raw []byte) error {
	var response standbySeededSlotActivateResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.TimelineId == nil || response.CheckpointLsn == nil || response.SeedReceiptSha256 == nil ||
		response.CaptureReceiptSha256 == nil ||
		response.ManifestSha256 == nil || response.AggregateSha256 == nil {
		return fmt.Errorf("missing seeded slot activate field evidence")
	}
	return nil
}

func ValidateStandbyBootstrapResponse(response StandbyBootstrapResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing standby bootstrap schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
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
	if err := validateStandbyActionReceiptTarget(response.Action, StandbyBootstrapReceiptExpectation(), response.ManifestId, "standby bootstrap"); err != nil {
		return err
	}
	return nil
}

func ValidateStandbyBootstrapResponseEvidence(raw []byte) error {
	var response standbyBootstrapResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.BackupLsn == nil || response.CheckpointLsn == nil {
		return fmt.Errorf("missing standby bootstrap field evidence")
	}
	return nil
}

func ValidateStandbyUpstreamResponse(response StandbyUpstreamResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing standby upstream schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing standby upstream action receipt")
	}
	if !StandbyIdentityComplete(response.Identity) {
		return fmt.Errorf("missing standby upstream identity fields")
	}
	if !StandbyUpstreamComplete(response.Upstream) {
		return fmt.Errorf("missing standby upstream fields")
	}
	if err := validateStandbyActionReceiptTarget(response.Action, StandbyUpstreamReceiptExpectation(), response.Upstream.SlotName, "standby upstream"); err != nil {
		return err
	}
	previousPresent := response.Previous != (StandbyUpstream{})
	if response.Changed {
		if previousPresent && response.Previous == response.Upstream {
			return fmt.Errorf("standby upstream response reports changed with an unchanged previous upstream")
		}
		return nil
	}
	// changed=false means the requested upstream already matched, so the
	// standby must have had a continuous previous upstream identical to it.
	if !previousPresent {
		return fmt.Errorf("standby upstream response reports unchanged without a previous upstream")
	}
	if response.Previous != response.Upstream {
		return fmt.Errorf("standby upstream response reports unchanged with a mismatched previous upstream")
	}
	return nil
}

func ValidateStandbyUpstreamResponseEvidence(raw []byte) error {
	var response standbyUpstreamResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !standbyFenceReceiptIdentityEvidenceComplete(response.Identity) {
		return fmt.Errorf("missing standby upstream identity field evidence")
	}
	if !standbyUpstreamEvidenceComplete(response.Upstream) {
		return fmt.Errorf("missing standby upstream field evidence")
	}
	if response.Changed == nil {
		return fmt.Errorf("missing standby upstream changed field evidence")
	}
	if standbyUpstreamEvidencePresent(response.Previous) && !standbyUpstreamEvidenceComplete(response.Previous) {
		return fmt.Errorf("missing standby upstream previous field evidence")
	}
	if !*response.Changed && !standbyUpstreamEvidenceComplete(response.Previous) {
		return fmt.Errorf("missing standby upstream previous field evidence")
	}
	return nil
}

func validateStandbyReplicationSlotActionCorrelation(response StandbyReplicationSlotActionResponse) error {
	expectation := StandbyReceiptExpectation{}
	switch response.SlotAction {
	case StandbyReplicationSlotActionCreate:
		expectation = StandbyReplicationSlotCreateReceiptExpectation()
	case StandbyReplicationSlotActionDrop:
		expectation = StandbyReplicationSlotDropReceiptExpectation()
	case StandbyReplicationSlotActionPause:
		expectation = StandbyReplicationSlotPauseReceiptExpectation()
	case StandbyReplicationSlotActionResume:
		expectation = StandbyReplicationSlotResumeReceiptExpectation()
	default:
		return fmt.Errorf("invalid replication slot action %q", response.SlotAction)
	}
	return validateStandbyActionReceiptTarget(response.Action, expectation, response.Slot.SlotName, "replication slot action")
}

func validateStandbyActionReceiptTarget(receipt StandbyActionReceipt, expectation StandbyReceiptExpectation, expectedTarget string, label string) error {
	if !StandbyReceiptMatches(receipt, expectation, expectedTarget) {
		return fmt.Errorf("%s receipt does not match action target", label)
	}
	return nil
}

func ValidateStandbyPrimaryStatusResponse(response StandbyPrimaryStatusResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing primary status schema_version")
	}
	snapshot := response.Snapshot
	if snapshot.Role != StandbyPrimarySnapshotRolePrimary {
		return fmt.Errorf("invalid primary status role %q", snapshot.Role)
	}
	if !validStandbyIdentifier(snapshot.NodeId) {
		return fmt.Errorf("invalid primary status node_id %q", snapshot.NodeId)
	}
	if !StandbyIdentityComplete(snapshot.Identity) {
		return fmt.Errorf("missing primary status identity fields")
	}
	if err := validateStandbyPrimaryRetentionSnapshot(snapshot.Retention, snapshot.CurrentLsn, len(snapshot.Slots)); err != nil {
		return err
	}
	for _, slot := range snapshot.Slots {
		if err := validateStandbySlotSnapshot(slot, snapshot.CurrentLsn); err != nil {
			return err
		}
	}
	if !StandbyDurabilityDecisionEmpty(snapshot.Durability) {
		if err := validateStandbyDurabilityDecision(snapshot.Durability); err != nil {
			return err
		}
	}
	return nil
}

func ValidateStandbyWatchdogProofResponse(response StandbyWatchdogProofResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing watchdog proof schema_version")
	}
	proof := response.Proof
	if proof.CapabilityVersion == 0 || strings.TrimSpace(proof.LeaseName) == "" ||
		strings.TrimSpace(proof.LeaseNamespace) == "" || strings.TrimSpace(proof.StableTopologyId) == "" ||
		strings.TrimSpace(proof.LocalNodeId) == "" || strings.TrimSpace(proof.PodUid) == "" ||
		strings.TrimSpace(proof.ProcessBootId) == "" || proof.MaxFenceLatencyMs == 0 {
		return fmt.Errorf("missing watchdog capability proof fields")
	}
	return nil
}

func ValidateStandbyStatusResponse(response StandbyStatusResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing standby status schema_version")
	}
	snapshot := response.Snapshot
	if snapshot.Role != StandbySnapshotRoleStandby {
		return fmt.Errorf("invalid standby status role %q", snapshot.Role)
	}
	if !validStandbyIdentifier(snapshot.NodeId) {
		return fmt.Errorf("invalid standby status node_id %q", snapshot.NodeId)
	}
	if !StandbyIdentityComplete(snapshot.Identity) {
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
		if snapshot.WriteLagLsn != standbySaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn) {
			return fmt.Errorf("standby status inconsistent: write_lag_lsn=%d expected=%d", snapshot.WriteLagLsn, standbySaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn))
		}
		if snapshot.ReceiveLagLsn != standbySaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn) {
			return fmt.Errorf("standby status inconsistent: receive_lag_lsn=%d expected=%d", snapshot.ReceiveLagLsn, standbySaturatingSub(snapshot.UpstreamLsn, snapshot.ReceivedLsn))
		}
		if snapshot.ApplyLagLsn != standbySaturatingSub(snapshot.UpstreamLsn, snapshot.AppliedLsn) {
			return fmt.Errorf("standby status inconsistent: apply_lag_lsn=%d expected=%d", snapshot.ApplyLagLsn, standbySaturatingSub(snapshot.UpstreamLsn, snapshot.AppliedLsn))
		}
	}
	return nil
}

func ValidateStandbyCommitCheckResponse(response StandbyCommitCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing commit check schema_version")
	}
	if err := validateStandbyCommitGate(response.Gate); err != nil {
		return fmt.Errorf("invalid commit check gate: %w", err)
	}
	return nil
}

func ValidateStandbyCommitAppendResponse(response StandbyCommitAppendResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing commit append schema_version")
	}
	if err := validateStandbyCommitGate(response.Gate); err != nil {
		return fmt.Errorf("invalid commit append gate: %w", err)
	}
	if response.Lsn != response.Gate.TargetLsn {
		return fmt.Errorf("commit append lsn=%d does not match gate target_lsn=%d", response.Lsn, response.Gate.TargetLsn)
	}
	return nil
}

func ValidateStandbyReadCheckResponse(response StandbyReadCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing read check schema_version")
	}
	if err := validateStandbyReadDecision(response.Decision); err != nil {
		return fmt.Errorf("invalid read decision: %w", err)
	}
	return nil
}

func ValidateStandbyWriteCheckResponse(response StandbyWriteCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing write check schema_version")
	}
	if err := validateStandbyWriteDecision(response.Decision); err != nil {
		return fmt.Errorf("invalid write decision: %w", err)
	}
	return nil
}

func ValidateStandbyOwnerJobCheckResponse(response StandbyOwnerJobCheckResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing owner job check schema_version")
	}
	if err := validateStandbyOwnerJobDecision(response.Decision); err != nil {
		return fmt.Errorf("invalid owner job decision: %w", err)
	}
	return nil
}

func ValidateStandbyFenceResponse(response StandbyFenceResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing fence response schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing fence response action receipt")
	}
	if !StandbyFenceReceiptComplete(response.Receipt) {
		return fmt.Errorf("missing fence response receipt fields")
	}
	if err := validateStandbyFenceReceiptConsistency(response.Receipt); err != nil {
		return err
	}
	if err := validateStandbyFenceActionCorrelation(response.Action, response.Receipt); err != nil {
		return err
	}
	return nil
}

func ValidateStandbyFenceResponseEvidence(raw []byte) error {
	var response standbyFenceResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !standbyFenceReceiptEvidenceComplete(response.Receipt) {
		return fmt.Errorf("missing fence response receipt field evidence")
	}
	return nil
}

func ValidateStandbyCurrentFenceResponse(response StandbyCurrentFenceResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing current fence schema_version")
	}
	if response.Held {
		if !StandbyFenceReceiptComplete(response.Receipt) {
			return fmt.Errorf("missing current fence receipt fields")
		}
		if err := validateStandbyFenceReceiptConsistency(response.Receipt); err != nil {
			return err
		}
		return nil
	}
	if !StandbyFenceReceiptEmpty(response.Receipt) {
		return fmt.Errorf("current fence response has receipt while not held")
	}
	return nil
}

func validateStandbyFenceActionCorrelation(action StandbyActionReceipt, receipt StandbyFenceReceipt) error {
	promoted := receipt.PromotedNodeId
	if action.ActionKind != StandbyActionKindFenceAcquire {
		return fmt.Errorf("fence response action kind mismatch")
	}
	if action.State != StandbyActionStateApplied && action.State != StandbyActionStateAlreadyApplied {
		return fmt.Errorf("fence response action state mismatch")
	}
	// A fence receipt can be persisted first on the old writer and then on the
	// candidate. The target is always the promoted node, while node_id identifies
	// the endpoint that durably recorded this copy of the receipt.
	if !validStandbyIdentifier(action.Target) || action.Target != promoted ||
		(action.NodeId != promoted && action.NodeId != receipt.OldPrimaryId) {
		return fmt.Errorf("fence response action node mismatch")
	}
	if action.ActionId != string(action.ActionKind)+":"+promoted {
		return fmt.Errorf("fence response action id does not match action kind and target")
	}
	return nil
}

func validateStandbyFenceReceiptConsistency(receipt StandbyFenceReceipt) error {
	if receipt.Identity.TimelineId != receipt.NewTimelineId || receipt.Identity.Epoch != receipt.NewEpoch {
		return fmt.Errorf("fence receipt identity does not match promoted timeline")
	}
	if receipt.NewTimelineId <= receipt.ParentTimelineId || receipt.NewEpoch <= receipt.ParentEpoch {
		return fmt.Errorf("fence receipt new identity does not advance")
	}
	if receipt.ObservedLsn < receipt.RequiredLsn {
		return fmt.Errorf("fence receipt observed_lsn is below required_lsn")
	}
	return nil
}

func ValidateStandbyCurrentFenceResponseEvidence(raw []byte) error {
	var response standbyCurrentFenceResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.Held == nil {
		return fmt.Errorf("missing current fence held field evidence")
	}
	if *response.Held && !standbyFenceReceiptEvidenceComplete(response.Receipt) {
		return fmt.Errorf("missing current fence receipt field evidence")
	}
	return nil
}

func ValidateStandbyPromotionAssessResponse(response StandbyPromotionAssessResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing promotion assess schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing promotion assess action receipt")
	}
	if !StandbyPromotionAssessmentComplete(response.Assessment) {
		return fmt.Errorf("missing promotion assessment fields")
	}
	if err := validateStandbyPromotionAssessmentConsistency(response.Assessment); err != nil {
		return err
	}
	if err := validateStandbyPromotionAssessReceiptCorrelation(response); err != nil {
		return err
	}
	return nil
}

func ValidateStandbyPromotionAssessResponseEvidence(raw []byte) error {
	var response standbyPromotionAssessResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !standbyPromotionAssessmentEvidenceComplete(response.Assessment) {
		return fmt.Errorf("missing promotion assessment field evidence")
	}
	return nil
}

func ValidateStandbyPromotionResponse(response StandbyPromotionResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing promotion response schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing promotion response action receipt")
	}
	if !StandbyPromotionAssessmentComplete(response.Assessment) {
		return fmt.Errorf("missing promotion response assessment fields")
	}
	if !StandbyPromotionResultComplete(response.Promotion) {
		return fmt.Errorf("missing promotion result fields")
	}
	if response.FenceGeneration == 0 {
		return fmt.Errorf("missing promotion fence_generation")
	}
	if strings.TrimSpace(response.FenceToken) == "" {
		return fmt.Errorf("missing promotion fence_token")
	}
	if err := validateStandbyPromotionAssessmentConsistency(response.Assessment); err != nil {
		return err
	}
	if err := validateStandbyPromotionReceiptCorrelation(response); err != nil {
		return err
	}
	return nil
}

func validateStandbyPromotionAssessReceiptCorrelation(response StandbyPromotionAssessResponse) error {
	action := response.Action
	target := action.Target
	if action.ActionKind != StandbyActionKindPromotionAssess {
		return fmt.Errorf("promotion assess response action kind mismatch")
	}
	if action.State != StandbyActionStateAssessed {
		return fmt.Errorf("promotion assess response action state mismatch")
	}
	if !validStandbyIdentifier(target) || action.NodeId != target {
		return fmt.Errorf("promotion assess response executor node mismatch")
	}
	if action.ActionId != string(action.ActionKind)+":"+target {
		return fmt.Errorf("promotion assess response action id does not match action kind and target")
	}
	return nil
}

func validateStandbyPromotionReceiptCorrelation(response StandbyPromotionResponse) error {
	action := response.Action
	promotion := response.Promotion
	nodeID := promotion.NodeId
	if action.ActionKind != StandbyActionKindPromotion {
		return fmt.Errorf("promotion response action kind mismatch")
	}
	if action.State != StandbyActionStateApplied {
		return fmt.Errorf("promotion response action state mismatch")
	}
	if !validStandbyIdentifier(action.Target) || action.Target != nodeID || action.NodeId != nodeID {
		return fmt.Errorf("promotion response action node mismatch")
	}
	if action.ActionId != string(action.ActionKind)+":"+nodeID {
		return fmt.Errorf("promotion response action id does not match action kind and target")
	}
	if promotion.SwitchLsn != response.Assessment.ReceivedLsn+1 {
		return fmt.Errorf("promotion response switch_lsn does not follow received_lsn")
	}
	if !response.Assessment.FencingConfirmed || !response.Assessment.CanPromote {
		return fmt.Errorf("promotion response assessment is not promotable")
	}
	if response.Forced != promotion.Forced || response.Forced != response.Assessment.Force {
		return fmt.Errorf("promotion response forced fields mismatch")
	}
	if promotion.DataLossPossible != response.Assessment.DataLossPossible {
		return fmt.Errorf("promotion response data_loss_possible mismatch")
	}
	if promotion.OldIdentity.ClusterId != promotion.NewIdentity.ClusterId ||
		promotion.OldIdentity.ShardId != promotion.NewIdentity.ShardId ||
		promotion.OldIdentity.TableId != promotion.NewIdentity.TableId {
		return fmt.Errorf("promotion response identity scope mismatch")
	}
	if promotion.NewIdentity.TimelineId <= promotion.OldIdentity.TimelineId ||
		promotion.NewIdentity.Epoch <= promotion.OldIdentity.Epoch {
		return fmt.Errorf("promotion response new identity does not advance")
	}
	return nil
}

func validateStandbyPromotionAssessmentConsistency(assessment StandbyPromotionAssessment) error {
	if assessment.HasRequiredLsn != (assessment.ReceivedLsn >= assessment.RequiredLsn) {
		return fmt.Errorf("promotion assessment has_required_lsn mismatch")
	}
	if assessment.CaughtUpToReceived != (assessment.AppliedLsn >= assessment.ReceivedLsn) {
		return fmt.Errorf("promotion assessment caught_up_to_received mismatch")
	}
	dataLossPossible := !assessment.HasRequiredLsn ||
		!assessment.CaughtUpToReceived ||
		assessment.AppliedLsn < assessment.RequiredLsn
	if assessment.DataLossPossible != dataLossPossible {
		return fmt.Errorf("promotion assessment data_loss_possible mismatch")
	}
	if assessment.Safe != (assessment.FencingConfirmed && !assessment.DataLossPossible) {
		return fmt.Errorf("promotion assessment safe mismatch")
	}
	if assessment.RequiresFencing != (!assessment.FencingConfirmed && !assessment.Force) {
		return fmt.Errorf("promotion assessment requires_fencing mismatch")
	}
	if assessment.RequiresForce != (assessment.DataLossPossible && !assessment.Force) {
		return fmt.Errorf("promotion assessment requires_force mismatch")
	}
	canPromote := !assessment.RequiresFencing && (!assessment.RequiresForce || assessment.Force)
	if assessment.CanPromote != canPromote {
		return fmt.Errorf("promotion assessment can_promote mismatch")
	}
	if assessment.Mode != expectedStandbyPromotionMode(assessment.Force, assessment.DataLossPossible, assessment.CanPromote) {
		return fmt.Errorf("promotion assessment mode mismatch")
	}
	return nil
}

func expectedStandbyPromotionMode(force bool, dataLossPossible bool, canPromote bool) StandbyPromotionAssessmentMode {
	if !canPromote {
		return StandbyPromotionModeBlocked
	}
	if dataLossPossible {
		return StandbyPromotionModeLossy
	}
	if force {
		return StandbyPromotionModeForced
	}
	return StandbyPromotionModeSafe
}

func ValidateStandbyPromotionResponseEvidence(raw []byte) error {
	var response standbyPromotionResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if response.Forced == nil {
		return fmt.Errorf("missing promotion forced field evidence")
	}
	if !standbyPromotionAssessmentEvidenceComplete(response.Assessment) {
		return fmt.Errorf("missing promotion assessment field evidence")
	}
	if !standbyPromotionResultEvidenceComplete(response.Promotion) {
		return fmt.Errorf("missing promotion result field evidence")
	}
	return nil
}

func ValidateStandbyRejoinAssessResponse(response StandbyRejoinAssessResponse) error {
	if response.SchemaVersion == 0 {
		return fmt.Errorf("missing rejoin response schema_version")
	}
	if !StandbyActionReceiptPresent(response.Action) {
		return fmt.Errorf("missing rejoin response action receipt")
	}
	if !StandbyRejoinAssessmentComplete(response.Assessment) {
		return fmt.Errorf("missing rejoin assessment fields")
	}
	switch response.Action.ActionKind {
	case StandbyActionKindRejoinRewind:
		if !StandbyRejoinRewindComplete(response.Rewind) {
			return fmt.Errorf("missing rejoin rewind fields")
		}
	case StandbyActionKindRejoinReseed:
		if !StandbyRejoinReseedComplete(response.Reseed) {
			return fmt.Errorf("missing rejoin reseed fields")
		}
	}
	if err := validateStandbyRejoinReceiptCorrelation(response); err != nil {
		return err
	}
	return nil
}

func validateStandbyRejoinReceiptCorrelation(response StandbyRejoinAssessResponse) error {
	action := response.Action
	target := action.Target
	if !validStandbyIdentifier(target) || target != response.Assessment.FormerNodeId {
		return fmt.Errorf("rejoin response action target does not match former node")
	}
	if action.ActionId != string(action.ActionKind)+":"+target {
		return fmt.Errorf("rejoin response action id does not match action kind and target")
	}

	switch action.ActionKind {
	case StandbyActionKindRejoinAssess:
		if action.State != StandbyActionStateAssessed {
			return fmt.Errorf("rejoin assess response action state mismatch")
		}
		if action.NodeId != target {
			return fmt.Errorf("rejoin assess response executor node mismatch")
		}
	case StandbyActionKindRejoinRewind:
		if action.State != StandbyActionStateApplied && action.State != StandbyActionStateAlreadyApplied {
			return fmt.Errorf("rejoin rewind response action state mismatch")
		}
		if action.NodeId != target {
			return fmt.Errorf("rejoin rewind response executor node mismatch")
		}
		if response.Assessment.Action != StandbyRejoinActionRewind {
			return fmt.Errorf("rejoin rewind response assessment action mismatch")
		}
		if response.Rewind.NodeId != target {
			return fmt.Errorf("rejoin rewind response result node mismatch")
		}
	case StandbyActionKindRejoinReseed:
		if action.State != StandbyActionStateApplied && action.State != StandbyActionStateAlreadyApplied {
			return fmt.Errorf("rejoin reseed response action state mismatch")
		}
		if response.Assessment.Action != StandbyRejoinActionReseed {
			return fmt.Errorf("rejoin reseed response assessment action mismatch")
		}
		if response.Reseed.NodeId != target || response.Reseed.SlotName != target {
			return fmt.Errorf("rejoin reseed response result target mismatch")
		}
	default:
		return fmt.Errorf("invalid rejoin response action kind %q", action.ActionKind)
	}
	return nil
}

func ValidateStandbyRejoinAssessResponseEvidence(raw []byte) error {
	var response standbyRejoinAssessResponseEvidence
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	if !standbyRejoinAssessmentEvidenceComplete(response.Assessment) {
		return fmt.Errorf("missing rejoin assessment field evidence")
	}
	if standbyRejoinRewindEvidencePresent(response.Rewind) {
		if !standbyRejoinRewindEvidenceComplete(response.Rewind) {
			return fmt.Errorf("missing rejoin rewind field evidence")
		}
	}
	if standbyRejoinReseedEvidencePresent(response.Reseed) {
		if !standbyRejoinReseedEvidenceComplete(response.Reseed) {
			return fmt.Errorf("missing rejoin reseed field evidence")
		}
	}
	return nil
}

type standbyPromotionAssessmentEvidence struct {
	RequiredLsn        *uint64 `json:"required_lsn"`
	ReceivedLsn        *uint64 `json:"received_lsn"`
	AppliedLsn         *uint64 `json:"applied_lsn"`
	HasRequiredLsn     *bool   `json:"has_required_lsn"`
	CaughtUpToReceived *bool   `json:"caught_up_to_received"`
	FencingConfirmed   *bool   `json:"fencing_confirmed"`
	Force              *bool   `json:"force"`
	Mode               *string `json:"mode"`
	DataLossPossible   *bool   `json:"data_loss_possible"`
	Safe               *bool   `json:"safe"`
	RequiresFencing    *bool   `json:"requires_fencing"`
	RequiresForce      *bool   `json:"requires_force"`
	CanPromote         *bool   `json:"can_promote"`
}

type standbyPromotionAssessResponseEvidence struct {
	Assessment standbyPromotionAssessmentEvidence `json:"assessment"`
}

type standbyPromotionResultEvidence struct {
	DataLossPossible *bool `json:"data_loss_possible"`
	Forced           *bool `json:"forced"`
}

type standbyPromotionResponseEvidence struct {
	Assessment standbyPromotionAssessmentEvidence `json:"assessment"`
	Forced     *bool                              `json:"forced"`
	Promotion  standbyPromotionResultEvidence     `json:"promotion"`
}

type standbyReplicationSlotEvidence struct {
	SlotName       *string `json:"slot_name"`
	TimelineId     *uint64 `json:"timeline_id"`
	RestartLsn     *uint64 `json:"restart_lsn"`
	ReceivedLsn    *uint64 `json:"received_lsn"`
	AppliedLsn     *uint64 `json:"applied_lsn"`
	SafeReadLsn    *uint64 `json:"safe_read_lsn"`
	Active         *bool   `json:"active"`
	ReseedRequired *bool   `json:"reseed_required"`
	CurrentLsn     *uint64 `json:"current_lsn"`
}

type standbyReplicationSlotActionResponseEvidence struct {
	Slot standbyReplicationSlotEvidence `json:"slot"`
}

type standbyReplicationSlotListResponseEvidence struct {
	Slots *[]standbyReplicationSlotEvidence `json:"slots"`
}

type standbyBaseBackupBeginResponseEvidence struct {
	BackupLsn      *uint64 `json:"backup_lsn"`
	StartRecordLsn *uint64 `json:"start_record_lsn"`
}

type standbyBaseBackupFinishResponseEvidence struct {
	BackupLsn    *uint64 `json:"backup_lsn"`
	EndRecordLsn *uint64 `json:"end_record_lsn"`
}

type standbySeededSlotActivateResponseEvidence struct {
	TimelineId           *uint64 `json:"timeline_id"`
	CheckpointLsn        *uint64 `json:"checkpoint_lsn"`
	SeedReceiptSha256    *string `json:"seed_receipt_sha256"`
	CaptureReceiptSha256 *string `json:"capture_receipt_sha256"`
	ManifestSha256       *string `json:"manifest_sha256"`
	AggregateSha256      *string `json:"aggregate_sha256"`
}

type standbySeedArtifactCaptureResponseEvidence struct {
	ClusterId            *uint64 `json:"cluster_id"`
	ShardId              *uint64 `json:"shard_id"`
	TableId              *uint64 `json:"table_id"`
	TimelineId           *uint64 `json:"timeline_id"`
	Epoch                *uint64 `json:"epoch"`
	BackupLsn            *uint64 `json:"backup_lsn"`
	CheckpointLsn        *uint64 `json:"checkpoint_lsn"`
	EndRecordLsn         *uint64 `json:"end_record_lsn"`
	FileCount            *uint64 `json:"file_count"`
	TotalBytes           *uint64 `json:"total_bytes"`
	AlreadyCaptured      *bool   `json:"already_captured"`
	TopologyId           *string `json:"topology_id"`
	TopologyGeneration   *uint64 `json:"topology_generation"`
	NodeId               *string `json:"node_id"`
	TargetPvcName        *string `json:"target_pvc_name"`
	TargetPvcUid         *string `json:"target_pvc_uid"`
	CaptureReceiptSha256 *string `json:"capture_receipt_sha256"`
}

type standbySeedLifecycleReceiptEventEvidence struct {
	Cursor             *uint64 `json:"cursor"`
	Kind               *string `json:"kind"`
	Generation         *string `json:"generation"`
	SlotName           *string `json:"slot_name"`
	TopologyId         *string `json:"topology_id"`
	TopologyGeneration *uint64 `json:"topology_generation"`
	NodeId             *string `json:"node_id"`
	TargetPvcName      *string `json:"target_pvc_name"`
	TargetPvcUid       *string `json:"target_pvc_uid"`
	ReceiptSha256      *string `json:"receipt_sha256"`
	ReceiptJson        *string `json:"receipt_json"`
	RecordedAtUnixNs   *uint64 `json:"recorded_at_unix_ns"`
	AuthoritativeState *string `json:"authoritative_state"`
}

type standbySeedLifecycleReceiptInventoryEvidence struct {
	SchemaVersion    *uint32                                     `json:"schema_version"`
	Entries          *[]standbySeedLifecycleReceiptEventEvidence `json:"entries"`
	FirstCursor      *uint64                                     `json:"first_cursor"`
	EndCursor        *uint64                                     `json:"end_cursor"`
	NextCursor       *uint64                                     `json:"next_cursor"`
	HistoryTruncated *bool                                       `json:"history_truncated"`
	Gap              *bool                                       `json:"gap"`
	HasMore          *bool                                       `json:"has_more"`
	Runtime          struct {
		Role             *string `json:"role"`
		Fenced           *bool   `json:"fenced"`
		ObservedAtUnixNs *uint64 `json:"observed_at_unix_ns"`
	} `json:"runtime"`
}

type standbyBootstrapResponseEvidence struct {
	BackupLsn     *uint64 `json:"backup_lsn"`
	CheckpointLsn *uint64 `json:"checkpoint_lsn"`
}

type standbyUpstreamEvidence struct {
	UpstreamUrl *string `json:"upstream_url"`
	SlotName    *string `json:"slot_name"`
}

type standbyUpstreamResponseEvidence struct {
	Identity standbyFenceReceiptIdentityEvidence `json:"identity"`
	Upstream standbyUpstreamEvidence             `json:"upstream"`
	Previous standbyUpstreamEvidence             `json:"previous"`
	Changed  *bool                               `json:"changed"`
}

type standbyFenceReceiptIdentityEvidence struct {
	ClusterId  *uint64 `json:"cluster_id"`
	ShardId    *uint64 `json:"shard_id"`
	TableId    *uint64 `json:"table_id"`
	TimelineId *uint64 `json:"timeline_id"`
	Epoch      *uint64 `json:"epoch"`
}

type standbyFenceReceiptEvidence struct {
	Identity         standbyFenceReceiptIdentityEvidence `json:"identity"`
	ParentTimelineId *uint64                             `json:"parent_timeline_id"`
	ParentEpoch      *uint64                             `json:"parent_epoch"`
	NewTimelineId    *uint64                             `json:"new_timeline_id"`
	NewEpoch         *uint64                             `json:"new_epoch"`
	RequiredLsn      *uint64                             `json:"required_lsn"`
	ObservedLsn      *uint64                             `json:"observed_lsn"`
	Generation       *uint64                             `json:"generation"`
	Forced           *bool                               `json:"forced"`
	Reason           *string                             `json:"reason"`
}

type standbyFenceResponseEvidence struct {
	Receipt standbyFenceReceiptEvidence `json:"receipt"`
}

type standbyCurrentFenceResponseEvidence struct {
	Held    *bool                       `json:"held"`
	Receipt standbyFenceReceiptEvidence `json:"receipt"`
}

type standbyRejoinAssessmentEvidence struct {
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

type standbyRejoinRewindEvidence struct {
	CurrentLastLsn    *uint64 `json:"current_last_lsn"`
	DataLossDiscarded *bool   `json:"data_loss_discarded"`
	DiscardedLsnCount *uint64 `json:"discarded_lsn_count"`
	ForkLsn           *uint64 `json:"fork_lsn"`
	NextLsn           *uint64 `json:"next_lsn"`
	PreviousLastLsn   *uint64 `json:"previous_last_lsn"`
	TargetTimelineId  *uint64 `json:"target_timeline_id"`
	TargetEpoch       *uint64 `json:"target_epoch"`
}

type standbyRejoinReseedEvidence struct {
	BaseBackupRequired *bool   `json:"base_backup_required"`
	ForkLsn            *uint64 `json:"fork_lsn"`
	FormerLastLsn      *uint64 `json:"former_last_lsn"`
	ReseedRequired     *bool   `json:"reseed_required"`
	TargetTimelineId   *uint64 `json:"target_timeline_id"`
	TargetEpoch        *uint64 `json:"target_epoch"`
}

type standbyRejoinAssessResponseEvidence struct {
	Assessment standbyRejoinAssessmentEvidence `json:"assessment"`
	Rewind     standbyRejoinRewindEvidence     `json:"rewind"`
	Reseed     standbyRejoinReseedEvidence     `json:"reseed"`
}

func standbyPromotionAssessmentEvidenceComplete(assessment standbyPromotionAssessmentEvidence) bool {
	return assessment.RequiredLsn != nil &&
		assessment.ReceivedLsn != nil &&
		assessment.AppliedLsn != nil &&
		assessment.HasRequiredLsn != nil &&
		assessment.CaughtUpToReceived != nil &&
		assessment.FencingConfirmed != nil &&
		assessment.Force != nil &&
		assessment.Mode != nil &&
		strings.TrimSpace(*assessment.Mode) != "" &&
		assessment.DataLossPossible != nil &&
		assessment.Safe != nil &&
		assessment.RequiresFencing != nil &&
		assessment.RequiresForce != nil &&
		assessment.CanPromote != nil
}

func standbyPromotionResultEvidenceComplete(result standbyPromotionResultEvidence) bool {
	return result.DataLossPossible != nil && result.Forced != nil
}

func standbyReplicationSlotEvidenceComplete(slot standbyReplicationSlotEvidence) bool {
	return slot.SlotName != nil &&
		slot.TimelineId != nil &&
		slot.RestartLsn != nil &&
		slot.ReceivedLsn != nil &&
		slot.AppliedLsn != nil &&
		slot.SafeReadLsn != nil &&
		slot.Active != nil &&
		slot.ReseedRequired != nil &&
		slot.CurrentLsn != nil
}

func standbyFenceReceiptIdentityEvidenceComplete(identity standbyFenceReceiptIdentityEvidence) bool {
	return identity.ClusterId != nil &&
		identity.ShardId != nil &&
		identity.TableId != nil &&
		identity.TimelineId != nil &&
		identity.Epoch != nil
}

func standbyUpstreamEvidenceComplete(upstream standbyUpstreamEvidence) bool {
	return upstream.UpstreamUrl != nil && strings.TrimSpace(*upstream.UpstreamUrl) != "" &&
		upstream.SlotName != nil && strings.TrimSpace(*upstream.SlotName) != ""
}

func standbyUpstreamEvidencePresent(upstream standbyUpstreamEvidence) bool {
	return upstream.UpstreamUrl != nil || upstream.SlotName != nil
}

func standbyFenceReceiptEvidenceComplete(receipt standbyFenceReceiptEvidence) bool {
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

func standbyRejoinAssessmentEvidenceComplete(assessment standbyRejoinAssessmentEvidence) bool {
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

func standbyRejoinRewindEvidenceComplete(rewind standbyRejoinRewindEvidence) bool {
	return rewind.CurrentLastLsn != nil &&
		rewind.DataLossDiscarded != nil &&
		rewind.DiscardedLsnCount != nil &&
		rewind.ForkLsn != nil &&
		rewind.NextLsn != nil &&
		rewind.PreviousLastLsn != nil &&
		rewind.TargetTimelineId != nil &&
		rewind.TargetEpoch != nil
}

func standbyRejoinRewindEvidencePresent(rewind standbyRejoinRewindEvidence) bool {
	return rewind.CurrentLastLsn != nil ||
		rewind.DataLossDiscarded != nil ||
		rewind.DiscardedLsnCount != nil ||
		rewind.ForkLsn != nil ||
		rewind.NextLsn != nil ||
		rewind.PreviousLastLsn != nil ||
		rewind.TargetTimelineId != nil ||
		rewind.TargetEpoch != nil
}

func standbyRejoinReseedEvidenceComplete(reseed standbyRejoinReseedEvidence) bool {
	return reseed.BaseBackupRequired != nil &&
		reseed.ForkLsn != nil &&
		reseed.FormerLastLsn != nil &&
		reseed.ReseedRequired != nil &&
		reseed.TargetTimelineId != nil &&
		reseed.TargetEpoch != nil
}

func standbyRejoinReseedEvidencePresent(reseed standbyRejoinReseedEvidence) bool {
	return reseed.BaseBackupRequired != nil ||
		reseed.ForkLsn != nil ||
		reseed.FormerLastLsn != nil ||
		reseed.ReseedRequired != nil ||
		reseed.TargetTimelineId != nil ||
		reseed.TargetEpoch != nil
}

func StandbyRejoinAssessmentComplete(assessment StandbyRejoinAssessment) bool {
	return StandbyRejoinAssessmentActionValid(assessment.Action) &&
		StandbyRejoinAssessmentReasonValid(assessment.Reason) &&
		validStandbyIdentifier(assessment.FormerNodeId) &&
		assessment.TargetTimelineId > 0 &&
		assessment.TargetEpoch > 0 &&
		assessment.ParentClusterId > 0 &&
		assessment.ParentTimelineId > 0 &&
		assessment.ParentEpoch > 0
}

func StandbyRejoinAssessmentActionValid(action StandbyRejoinAssessmentAction) bool {
	switch action {
	case StandbyRejoinActionRejectUnfenced, StandbyRejoinActionAlreadyCurrent, StandbyRejoinActionRewind, StandbyRejoinActionReseed:
		return true
	default:
		return false
	}
}

func StandbyRejoinAssessmentReasonValid(reason StandbyRejoinAssessmentReason) bool {
	switch reason {
	case StandbyRejoinReasonNoFence,
		StandbyRejoinReasonCurrentTimeline,
		StandbyRejoinReasonParentTimelineRetained,
		StandbyRejoinReasonParentTimelineWALExpired,
		StandbyRejoinReasonIncompatibleTimeline,
		StandbyRejoinReasonWrongOldPrimary,
		StandbyRejoinReasonWrongCluster,
		StandbyRejoinReasonWrongShard,
		StandbyRejoinReasonWrongTable,
		StandbyRejoinReasonLocalLSNBeforeFork:
		return true
	default:
		return false
	}
}

func StandbyRejoinRewindComplete(rewind StandbyRejoinRewindResult) bool {
	return validStandbyIdentifier(rewind.NodeId) &&
		rewind.TargetTimelineId > 0 &&
		rewind.TargetEpoch > 0 &&
		rewind.NextLsn > 0
}

func StandbyRejoinReseedComplete(reseed StandbyRejoinReseedResult) bool {
	return validStandbyIdentifier(reseed.NodeId) &&
		validStandbyIdentifier(reseed.SlotName) &&
		reseed.TargetTimelineId > 0 &&
		reseed.TargetEpoch > 0 &&
		reseed.ReseedRequired &&
		reseed.BaseBackupRequired
}

func StandbyCommitGateComplete(gate StandbyCommitGate) bool {
	return StandbyCommitGateActionValid(gate.Action) &&
		StandbyDurabilityDecisionComplete(gate.Durability)
}

func validateStandbyCommitGate(gate StandbyCommitGate) error {
	if !StandbyCommitGateActionValid(gate.Action) {
		return fmt.Errorf("invalid commit gate action %q", gate.Action)
	}
	if gate.TargetLsn != gate.Durability.TargetLsn {
		return fmt.Errorf("commit gate target_lsn=%d does not match durability target_lsn=%d", gate.TargetLsn, gate.Durability.TargetLsn)
	}
	return validateStandbyDurabilityDecision(gate.Durability)
}

func validateStandbyPrimaryRetentionSnapshot(retention StandbyRetentionSnapshot, currentLSN uint64, slotCount int) error {
	if retention.PrimaryLsn != currentLSN {
		return fmt.Errorf("primary retention snapshot inconsistent: primary_lsn=%d current_lsn=%d", retention.PrimaryLsn, currentLSN)
	}
	if retention.OldestRestartLsn > retention.PrimaryLsn {
		return fmt.Errorf("primary retention snapshot inconsistent: oldest_restart_lsn=%d exceeds primary_lsn=%d", retention.OldestRestartLsn, retention.PrimaryLsn)
	}
	if !standbyRetainedLSNCountConsistent(retention.PrimaryLsn, retention.OldestRestartLsn, retention.RetainedLsnCount, slotCount) {
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

func standbyRetainedLSNCountConsistent(primaryLSN uint64, oldestRestartLSN uint64, retainedLSNCount uint64, slotCount int) bool {
	expected := primaryLSN - oldestRestartLSN
	if retainedLSNCount == expected {
		return true
	}
	return primaryLSN == oldestRestartLSN && retainedLSNCount == 1 && slotCount > 0
}

func validateStandbySlotSnapshot(slot StandbySlotSnapshot, currentLSN uint64) error {
	name := slot.Name
	if !validStandbyIdentifier(name) {
		return fmt.Errorf("invalid slot snapshot name %q", slot.Name)
	}
	if slot.TimelineId == 0 {
		return fmt.Errorf("slot %s snapshot missing timeline_id", name)
	}
	if !StandbySlotSnapshotStatusValid(slot.Status) {
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
	if slot.WriteLagLsn != standbySaturatingSub(currentLSN, slot.ReceivedLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: write_lag_lsn=%d expected=%d", name, slot.WriteLagLsn, standbySaturatingSub(currentLSN, slot.ReceivedLsn))
	}
	if slot.ApplyLagLsn != standbySaturatingSub(currentLSN, slot.AppliedLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: apply_lag_lsn=%d expected=%d", name, slot.ApplyLagLsn, standbySaturatingSub(currentLSN, slot.AppliedLsn))
	}
	if slot.SafeReadLagLsn != standbySaturatingSub(currentLSN, slot.SafeReadLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: safe_read_lag_lsn=%d expected=%d", name, slot.SafeReadLagLsn, standbySaturatingSub(currentLSN, slot.SafeReadLsn))
	}
	if slot.RetentionLagLsn != standbySaturatingSub(currentLSN, slot.RestartLsn) {
		return fmt.Errorf("slot %s snapshot inconsistent: retention_lag_lsn=%d expected=%d", name, slot.RetentionLagLsn, standbySaturatingSub(currentLSN, slot.RestartLsn))
	}
	return nil
}

func StandbySlotSnapshotStatusValid(status StandbySlotSnapshotStatus) bool {
	switch status {
	case StandbySlotSnapshotStatusHealthy, StandbySlotSnapshotStatusLagging, StandbySlotSnapshotStatusReseedRequired:
		return true
	default:
		return false
	}
}

func validateStandbyDurabilityDecision(decision StandbyDurabilityDecision) error {
	if !StandbyDurabilityDecisionComplete(decision) {
		return fmt.Errorf("missing durability decision fields")
	}
	if decision.ProgressLsn > decision.TargetLsn {
		return fmt.Errorf("durability decision inconsistent: progress_lsn=%d exceeds target_lsn=%d", decision.ProgressLsn, decision.TargetLsn)
	}
	if decision.MissingLsnCount != decision.TargetLsn-decision.ProgressLsn {
		return fmt.Errorf("durability decision inconsistent: missing_lsn_count=%d expected=%d", decision.MissingLsnCount, decision.TargetLsn-decision.ProgressLsn)
	}
	if decision.SatisfiedCount > decision.CandidateCount {
		return fmt.Errorf("durability decision inconsistent: satisfied_count=%d exceeds candidate_count=%d", decision.SatisfiedCount, decision.CandidateCount)
	}
	if decision.Status == StandbyDurabilityStatusSatisfied && decision.SatisfiedCount < decision.RequiredCount {
		return fmt.Errorf("durability decision inconsistent: satisfied_count=%d below required_count=%d", decision.SatisfiedCount, decision.RequiredCount)
	}
	return nil
}

func StandbyCommitGateActionValid(action StandbyCommitGateAction) bool {
	switch action {
	case StandbyCommitGateActionAcknowledge,
		StandbyCommitGateActionWaitForStandby,
		StandbyCommitGateActionReject,
		StandbyCommitGateActionAcknowledgeDegraded:
		return true
	default:
		return false
	}
}

func StandbyDurabilityDecisionComplete(decision StandbyDurabilityDecision) bool {
	return StandbyDurabilityDecisionStatusValid(decision.Status) &&
		StandbyDurabilityDecisionModeValid(decision.Mode) &&
		StandbyDurabilityDecisionSelectionValid(decision.Selection)
}

func StandbyDurabilityDecisionEmpty(decision StandbyDurabilityDecision) bool {
	return decision == (StandbyDurabilityDecision{})
}

func StandbyDurabilityDecisionStatusValid(status StandbyDurabilityDecisionStatus) bool {
	switch status {
	case StandbyDurabilityStatusSatisfied,
		StandbyDurabilityStatusWouldBlock,
		StandbyDurabilityStatusFailClosed,
		StandbyDurabilityStatusDegradedToAsync:
		return true
	default:
		return false
	}
}

func StandbyDurabilityDecisionModeValid(mode StandbyDurabilityDecisionMode) bool {
	switch mode {
	case StandbyDurabilityModeAsync, StandbyDurabilityModeRemoteWrite, StandbyDurabilityModeRemoteApply:
		return true
	default:
		return false
	}
}

func StandbyDurabilityDecisionSelectionValid(selection StandbyDurabilityDecisionSelection) bool {
	switch selection {
	case StandbyDurabilitySelectionAny, StandbyDurabilitySelectionFirst, StandbyDurabilitySelectionAll:
		return true
	default:
		return false
	}
}

func StandbyReadDecisionComplete(decision StandbyReadDecision) bool {
	return StandbyReadDecisionActionValid(decision.Action) &&
		StandbyReadDecisionConsistencyValid(decision.Consistency)
}

func validateStandbyReadDecision(decision StandbyReadDecision) error {
	if !StandbyReadDecisionComplete(decision) {
		return fmt.Errorf("missing read decision fields")
	}
	if decision.AppliedLsn > decision.ReceivedLsn {
		return fmt.Errorf("read decision inconsistent: applied_lsn=%d exceeds received_lsn=%d", decision.AppliedLsn, decision.ReceivedLsn)
	}
	if decision.SafeReadLsn > decision.AppliedLsn {
		return fmt.Errorf("read decision inconsistent: safe_read_lsn=%d exceeds applied_lsn=%d", decision.SafeReadLsn, decision.AppliedLsn)
	}
	if decision.MetadataAppliedLsn > 0 && decision.MetadataAppliedLsn > decision.AppliedLsn {
		return fmt.Errorf("read decision inconsistent: metadata_applied_lsn=%d exceeds applied_lsn=%d", decision.MetadataAppliedLsn, decision.AppliedLsn)
	}
	if decision.ServeLsn > 0 {
		if decision.ServeLsn > decision.SafeReadLsn {
			return fmt.Errorf("read decision inconsistent: serve_lsn=%d exceeds safe_read_lsn=%d", decision.ServeLsn, decision.SafeReadLsn)
		}
		if decision.MetadataAppliedLsn > 0 && decision.ServeLsn > decision.MetadataAppliedLsn {
			return fmt.Errorf("read decision inconsistent: serve_lsn=%d exceeds metadata_applied_lsn=%d", decision.ServeLsn, decision.MetadataAppliedLsn)
		}
	}

	expectedMissing := standbySaturatingSub(decision.RequiredLsn, decision.SafeReadLsn)
	if decision.MissingLsnCount != expectedMissing {
		return fmt.Errorf("read decision inconsistent: missing_lsn_count=%d expected=%d", decision.MissingLsnCount, expectedMissing)
	}
	requiredMetadataLSN := decision.RequiredMetadataLsn
	if requiredMetadataLSN == 0 {
		requiredMetadataLSN = decision.RequiredLsn
	}
	appliedMetadataLSN := decision.MetadataAppliedLsn
	if appliedMetadataLSN == 0 {
		appliedMetadataLSN = decision.SafeReadLsn
	}
	expectedMetadataMissing := standbySaturatingSub(requiredMetadataLSN, appliedMetadataLSN)
	if decision.MetadataMissingLsnCount != expectedMetadataMissing {
		return fmt.Errorf("read decision inconsistent: metadata_missing_lsn_count=%d expected=%d", decision.MetadataMissingLsnCount, expectedMetadataMissing)
	}

	switch decision.Consistency {
	case StandbyReadDecisionConsistencyPrimary:
		if decision.Action != StandbyReadDecisionActionRouteToPrimary {
			return fmt.Errorf("read decision inconsistent: primary consistency action=%q", decision.Action)
		}
		if decision.ServeLsn != 0 || decision.MissingLsnCount != 0 || decision.MetadataMissingLsnCount != 0 {
			return fmt.Errorf("read decision inconsistent: primary consistency should not serve or wait locally")
		}
	case StandbyReadDecisionConsistencyStaleOK:
		if decision.Action != StandbyReadDecisionActionServeStandby {
			return fmt.Errorf("read decision inconsistent: stale_ok action=%q", decision.Action)
		}
	case StandbyReadDecisionConsistencyAtLeastLSN:
		switch decision.Action {
		case StandbyReadDecisionActionServeStandby, StandbyReadDecisionActionWaitForApply, StandbyReadDecisionActionWaitForMetadata:
		default:
			return fmt.Errorf("read decision inconsistent: at_least_lsn action=%q", decision.Action)
		}
	}
	return nil
}

func StandbyReadDecisionActionValid(action StandbyReadDecisionAction) bool {
	switch action {
	case StandbyReadDecisionActionServeStandby,
		StandbyReadDecisionActionWaitForApply,
		StandbyReadDecisionActionWaitForMetadata,
		StandbyReadDecisionActionRouteToPrimary:
		return true
	default:
		return false
	}
}

func StandbyReadDecisionConsistencyValid(consistency StandbyReadDecisionConsistency) bool {
	switch consistency {
	case StandbyReadDecisionConsistencyStaleOK,
		StandbyReadDecisionConsistencyAtLeastLSN,
		StandbyReadDecisionConsistencyPrimary:
		return true
	default:
		return false
	}
}

func StandbyWriteDecisionComplete(decision StandbyWriteDecision) bool {
	return StandbyWriteDecisionRoleValid(decision.Role) &&
		StandbyWriteDecisionActionValid(decision.Action) &&
		StandbyIdentityComplete(decision.Identity) &&
		StandbyPromotionHandoffCompleteOrEmpty(decision.PromotionHandoff)
}

func validateStandbyWriteDecision(decision StandbyWriteDecision) error {
	if !StandbyWriteDecisionComplete(decision) {
		return fmt.Errorf("missing write decision fields")
	}
	if err := validateStandbyNextLSN("write decision", decision.DurableLsn, decision.NextLsn); err != nil {
		return err
	}

	promoted := false
	switch decision.Role {
	case StandbyWriteDecisionRolePrimary:
		if decision.Action != StandbyWriteDecisionActionAllowWrite {
			return fmt.Errorf("write decision inconsistent: primary role action=%q", decision.Action)
		}
	case StandbyWriteDecisionRoleStandby:
		if decision.Action != StandbyWriteDecisionActionRejectReadOnly {
			return fmt.Errorf("write decision inconsistent: standby role action=%q", decision.Action)
		}
	case StandbyWriteDecisionRolePromotedStandby:
		if decision.Action != StandbyWriteDecisionActionOpenPromotedPrimary {
			return fmt.Errorf("write decision inconsistent: promoted_standby role action=%q", decision.Action)
		}
		promoted = true
	case StandbyWriteDecisionRoleFencedPrimary:
		if decision.Action != StandbyWriteDecisionActionRejectFencedPrimary {
			return fmt.Errorf("write decision inconsistent: fenced_primary role action=%q", decision.Action)
		}
	}
	return validateStandbyPromotionHandoffForGate("write decision", promoted, decision.Identity, decision.DurableLsn, decision.NextLsn, decision.PromotionHandoff)
}

func StandbyWriteDecisionRoleValid(role StandbyWriteDecisionRole) bool {
	switch role {
	case StandbyWriteDecisionRolePrimary,
		StandbyWriteDecisionRoleStandby,
		StandbyWriteDecisionRolePromotedStandby,
		StandbyWriteDecisionRoleFencedPrimary:
		return true
	default:
		return false
	}
}

func StandbyWriteDecisionActionValid(action StandbyWriteDecisionAction) bool {
	switch action {
	case StandbyWriteDecisionActionAllowWrite,
		StandbyWriteDecisionActionRejectReadOnly,
		StandbyWriteDecisionActionOpenPromotedPrimary,
		StandbyWriteDecisionActionRejectFencedPrimary:
		return true
	default:
		return false
	}
}

func StandbyOwnerJobDecisionComplete(decision StandbyOwnerJobDecision) bool {
	return StandbyOwnerJobDecisionKindValid(decision.Kind) &&
		StandbyOwnerJobDecisionRoleValid(decision.Role) &&
		StandbyOwnerJobDecisionActionValid(decision.Action) &&
		StandbyIdentityComplete(decision.Identity) &&
		StandbyPromotionHandoffCompleteOrEmpty(decision.PromotionHandoff)
}

func validateStandbyOwnerJobDecision(decision StandbyOwnerJobDecision) error {
	if !StandbyOwnerJobDecisionComplete(decision) {
		return fmt.Errorf("missing owner job decision fields")
	}
	if err := validateStandbyNextLSN("owner job decision", decision.DurableLsn, decision.NextLsn); err != nil {
		return err
	}

	promoted := false
	switch decision.Role {
	case StandbyOwnerJobDecisionRolePrimary:
		if decision.Action != StandbyOwnerJobDecisionActionRun {
			return fmt.Errorf("owner job decision inconsistent: primary role action=%q", decision.Action)
		}
	case StandbyOwnerJobDecisionRoleStandby:
		if decision.Action != StandbyOwnerJobDecisionActionDisableOnStandby {
			return fmt.Errorf("owner job decision inconsistent: standby role action=%q", decision.Action)
		}
	case StandbyOwnerJobDecisionRolePromotedStandby:
		if decision.Action != StandbyOwnerJobDecisionActionOpenPromotedPrimary {
			return fmt.Errorf("owner job decision inconsistent: promoted_standby role action=%q", decision.Action)
		}
		promoted = true
	}
	return validateStandbyPromotionHandoffForGate("owner job decision", promoted, decision.Identity, decision.DurableLsn, decision.NextLsn, decision.PromotionHandoff)
}

func StandbyOwnerJobDecisionKindValid(kind StandbyOwnerJobDecisionKind) bool {
	switch kind {
	case StandbyOwnerJobDecisionKindCompactionPublish,
		StandbyOwnerJobDecisionKindDerivedEffectWriter,
		StandbyOwnerJobDecisionKindEnrichmentWriter,
		StandbyOwnerJobDecisionKindRetentionAdvance:
		return true
	default:
		return false
	}
}

func StandbyOwnerJobDecisionRoleValid(role StandbyOwnerJobDecisionRole) bool {
	switch role {
	case StandbyOwnerJobDecisionRolePrimary, StandbyOwnerJobDecisionRoleStandby, StandbyOwnerJobDecisionRolePromotedStandby:
		return true
	default:
		return false
	}
}

func StandbyOwnerJobDecisionActionValid(action StandbyOwnerJobDecisionAction) bool {
	switch action {
	case StandbyOwnerJobDecisionActionRun,
		StandbyOwnerJobDecisionActionDisableOnStandby,
		StandbyOwnerJobDecisionActionOpenPromotedPrimary:
		return true
	default:
		return false
	}
}

func StandbyPromotionHandoffCompleteOrEmpty(handoff StandbyPromotionHandoff) bool {
	if !StandbyIdentityComplete(handoff.Identity) && handoff.SwitchLsn == 0 && handoff.NextLsn == 0 {
		return true
	}
	return StandbyIdentityComplete(handoff.Identity)
}

func validateStandbyNextLSN(label string, durableLSN uint64, nextLSN uint64) error {
	if durableLSN == ^uint64(0) {
		return fmt.Errorf("%s inconsistent: durable_lsn overflows next_lsn", label)
	}
	expected := durableLSN + 1
	if nextLSN != expected {
		return fmt.Errorf("%s inconsistent: next_lsn=%d expected=%d", label, nextLSN, expected)
	}
	return nil
}

func validateStandbyPromotionHandoffForGate(label string, promoted bool, identity StandbyIdentity, durableLSN uint64, nextLSN uint64, handoff StandbyPromotionHandoff) error {
	if !promoted {
		if !StandbyPromotionHandoffEmpty(handoff) {
			return fmt.Errorf("%s inconsistent: promotion_handoff present without promoted role", label)
		}
		return nil
	}
	if !StandbyIdentityComplete(handoff.Identity) {
		return fmt.Errorf("%s inconsistent: missing promotion_handoff identity", label)
	}
	if handoff.Identity != identity {
		return fmt.Errorf("%s inconsistent: promotion_handoff identity mismatch", label)
	}
	if handoff.SwitchLsn != durableLSN {
		return fmt.Errorf("%s inconsistent: promotion_handoff switch_lsn=%d expected durable_lsn=%d", label, handoff.SwitchLsn, durableLSN)
	}
	if handoff.NextLsn != nextLSN {
		return fmt.Errorf("%s inconsistent: promotion_handoff next_lsn=%d expected=%d", label, handoff.NextLsn, nextLSN)
	}
	return nil
}

func StandbyPromotionHandoffEmpty(handoff StandbyPromotionHandoff) bool {
	return !StandbyIdentityComplete(handoff.Identity) && handoff.SwitchLsn == 0 && handoff.NextLsn == 0
}

func StandbyPromotionAssessmentComplete(assessment StandbyPromotionAssessment) bool {
	return assessment.RequiredLsn <= assessment.ReceivedLsn || !assessment.HasRequiredLsn
}

func StandbyPromotionResultComplete(result StandbyPromotionResult) bool {
	return validStandbyIdentifier(result.NodeId) &&
		result.SwitchLsn > 0 &&
		StandbyIdentityComplete(result.OldIdentity) &&
		StandbyIdentityComplete(result.NewIdentity)
}

func StandbyFenceReceiptComplete(receipt StandbyFenceReceipt) bool {
	return StandbyIdentityComplete(receipt.Identity) &&
		validStandbyIdentifier(receipt.OldPrimaryId) &&
		validStandbyIdentifier(receipt.PromotedNodeId) &&
		receipt.ParentTimelineId > 0 &&
		receipt.ParentEpoch > 0 &&
		receipt.NewTimelineId > 0 &&
		receipt.NewEpoch > 0 &&
		receipt.RequiredLsn > 0 &&
		receipt.Generation > 0 &&
		strings.TrimSpace(receipt.Token) != ""
}

func StandbyFenceReceiptEmpty(receipt StandbyFenceReceipt) bool {
	return !receipt.Forced &&
		receipt.Generation == 0 &&
		receipt.Identity == (StandbyIdentity{}) &&
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

func StandbyIdentityComplete(identity StandbyIdentity) bool {
	return identity.ClusterId > 0 &&
		identity.TimelineId > 0 &&
		identity.Epoch > 0
}

func StandbyActionReceiptPresent(receipt StandbyActionReceipt) bool {
	return strings.TrimSpace(receipt.ActionId) != "" &&
		strings.TrimSpace(string(receipt.ActionKind)) != "" &&
		strings.TrimSpace(receipt.Target) != "" &&
		strings.TrimSpace(string(receipt.State)) != "" &&
		validStandbyIdentifier(receipt.NodeId)
}

func StandbyReplicationSlotComplete(slot StandbyReplicationSlot) bool {
	return validStandbyIdentifier(slot.SlotName) &&
		slot.TimelineId > 0
}

func StandbyUpstreamComplete(upstream StandbyUpstream) bool {
	return validStandbyIdentifier(upstream.SlotName) && validHTTPURL(upstream.UpstreamUrl)
}

// Deprecated: use StandbyPrimaryStatusOperation. Removed after 0.4.
func HAPrimaryStatusOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: HAPrimaryStatusPath}
}

func StandbyPrimaryStatusOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: StandbyPrimaryStatusPath}
}

// Deprecated: use StandbyStatusOperation. Removed after 0.4.
func HAStandbyStatusOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: HAStandbyStatusPath}
}

func StandbyStatusOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: StandbyStatusPath}
}

// Deprecated: use StandbyCheckCommitOperation. Removed after 0.4.
func HACheckCommitOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HACommitCheckPath}
}

func StandbyCheckCommitOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyCommitCheckPath}
}

// Deprecated: use StandbyAppendCommitOperation. Removed after 0.4.
func HAAppendCommitOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HACommitAppendPath}
}

func StandbyAppendCommitOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyCommitAppendPath}
}

// Deprecated: use StandbyCheckReadOperation. Removed after 0.4.
func HACheckReadOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAReadCheckPath}
}

func StandbyCheckReadOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyReadCheckPath}
}

// Deprecated: use StandbyCheckWriteOperation. Removed after 0.4.
func HACheckWriteOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAWriteCheckPath}
}

func StandbyCheckWriteOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyWriteCheckPath}
}

// Deprecated: use StandbyCheckOwnerJobOperation. Removed after 0.4.
func HACheckOwnerJobOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAOwnerJobCheckPath}
}

func StandbyCheckOwnerJobOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyOwnerJobCheckPath}
}

// Deprecated: use StandbyListReplicationSlotsOperation. Removed after 0.4.
func HAListReplicationSlotsOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: HAReplicationSlotsPath}
}

func StandbyListReplicationSlotsOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: StandbyReplicationSlotsPath}
}

// Deprecated: use StandbyCreateReplicationSlotOperation. Removed after 0.4.
func HACreateReplicationSlotOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAReplicationSlotsPath}
}

func StandbyCreateReplicationSlotOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyReplicationSlotsPath}
}

// Deprecated: use StandbyDropReplicationSlotOperation. Removed after 0.4.
func HADropReplicationSlotOperation(slotName string) (StandbyOperation, bool) {
	path, ok := HAReplicationSlotPath(slotName)
	if !ok {
		return StandbyOperation{}, false
	}
	return StandbyOperation{Method: http.MethodDelete, Path: path}, true
}

func StandbyDropReplicationSlotOperation(slotName string) (StandbyOperation, bool) {
	path, ok := StandbyReplicationSlotPath(slotName)
	if !ok {
		return StandbyOperation{}, false
	}
	return StandbyOperation{Method: http.MethodDelete, Path: path}, true
}

// Deprecated: use StandbyPauseReplicationSlotOperation. Removed after 0.4.
func HAPauseReplicationSlotOperation(slotName string) (StandbyOperation, bool) {
	path, ok := HAReplicationSlotPath(slotName)
	if !ok {
		return StandbyOperation{}, false
	}
	return StandbyOperation{Method: http.MethodPut, Path: path + HAReplicationSlotPausePathSuffix}, true
}

func StandbyPauseReplicationSlotOperation(slotName string) (StandbyOperation, bool) {
	path, ok := StandbyReplicationSlotPath(slotName)
	if !ok {
		return StandbyOperation{}, false
	}
	return StandbyOperation{Method: http.MethodPut, Path: path + HAReplicationSlotPausePathSuffix}, true
}

// Deprecated: use StandbyResumeReplicationSlotOperation. Removed after 0.4.
func HAResumeReplicationSlotOperation(slotName string) (StandbyOperation, bool) {
	path, ok := HAReplicationSlotPath(slotName)
	if !ok {
		return StandbyOperation{}, false
	}
	return StandbyOperation{Method: http.MethodPut, Path: path + HAReplicationSlotResumePathSuffix}, true
}

func StandbyResumeReplicationSlotOperation(slotName string) (StandbyOperation, bool) {
	path, ok := StandbyReplicationSlotPath(slotName)
	if !ok {
		return StandbyOperation{}, false
	}
	return StandbyOperation{Method: http.MethodPut, Path: path + HAReplicationSlotResumePathSuffix}, true
}

// Deprecated: use StandbyBeginBaseBackupOperation. Removed after 0.4.
func HABeginBaseBackupOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsPath}
}

func StandbyBeginBaseBackupOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyBaseBackupsPath}
}

// Deprecated: use StandbyFinishBaseBackupOperation. Removed after 0.4.
func HAFinishBaseBackupOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsFinishPath}
}

func StandbyFinishBaseBackupOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyBaseBackupsFinishPath}
}

// Deprecated: use StandbySeedCaptureOperation. Removed after 0.4.
func HASeedCaptureOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsCapturePath}
}

func StandbySeedCaptureOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyBaseBackupsCapturePath}
}

// Deprecated: use StandbyActivateSeededSlotOperation. Removed after 0.4.
func HAActivateSeededSlotOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HABaseBackupsActivatePath}
}

func StandbyActivateSeededSlotOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyBaseBackupsActivatePath}
}

// Deprecated: use StandbyBootstrapStandbyOperation. Removed after 0.4.
func HABootstrapStandbyOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAStandbyBootstrapPath}
}

func StandbyBootstrapStandbyOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyBootstrapPath}
}

// Deprecated: use StandbySetStandbyUpstreamOperation. Removed after 0.4.
func HASetStandbyUpstreamOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAStandbyUpstreamPath}
}

func StandbySetStandbyUpstreamOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyUpstreamPath}
}

// Deprecated: use StandbyAcquireFenceOperation. Removed after 0.4.
func HAAcquireFenceOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAFencePath}
}

func StandbyAcquireFenceOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyFencePath}
}

// Deprecated: use StandbyCurrentFenceOperation. Removed after 0.4.
func HACurrentFenceOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: HAFenceCurrentPath}
}

func StandbyCurrentFenceOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodGet, Path: StandbyFenceCurrentPath}
}

// Deprecated: use StandbyAssessPromotionOperation. Removed after 0.4.
func HAAssessPromotionOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAPromotionAssessPath}
}

func StandbyAssessPromotionOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyPromotionAssessPath}
}

// Deprecated: use StandbyPromoteWithCurrentFenceOperation. Removed after 0.4.
func HAPromoteWithCurrentFenceOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAPromotionCurrentFencePath}
}

func StandbyPromoteWithCurrentFenceOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyPromotionCurrentFencePath}
}

// Deprecated: use StandbyPromoteOperation. Removed after 0.4.
func HAPromoteOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HAPromotionPath}
}

func StandbyPromoteOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyPromotionPath}
}

// Deprecated: use StandbyAssessRejoinOperation. Removed after 0.4.
func HAAssessRejoinOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HARejoinAssessPath}
}

func StandbyAssessRejoinOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyRejoinAssessPath}
}

// Deprecated: use StandbyRewindRejoinOperation. Removed after 0.4.
func HARewindRejoinOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HARejoinRewindPath}
}

func StandbyRewindRejoinOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyRejoinRewindPath}
}

// Deprecated: use StandbyReseedRejoinOperation. Removed after 0.4.
func HAReseedRejoinOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: HARejoinReseedPath}
}

func StandbyReseedRejoinOperation() StandbyOperation {
	return StandbyOperation{Method: http.MethodPost, Path: StandbyRejoinReseedPath}
}

// Deprecated: use StandbyReplicationSlotPath. Removed after 0.4.
func HAReplicationSlotPath(slotName string) (string, bool) {
	if !validStandbyIdentifier(slotName) {
		return "", false
	}
	return HAReplicationSlotPathPrefix + url.PathEscape(slotName), true
}

func StandbyReplicationSlotPath(slotName string) (string, bool) {
	if !validStandbyIdentifier(slotName) {
		return "", false
	}
	return StandbyReplicationSlotPathPrefix + url.PathEscape(slotName), true
}

func validStandbyIdentifier(value string) bool {
	if len(value) == 0 || len(value) > maxStandbyIdentifierBytes {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'A' && c <= 'Z' {
			continue
		}
		if c >= 'a' && c <= 'z' {
			continue
		}
		if c >= '0' && c <= '9' {
			continue
		}
		switch c {
		case '_', '-', '.', ':':
			continue
		default:
			return false
		}
	}
	return true
}

// validHTTPURL reports whether value is a non-empty, unpadded absolute URL
// with an http or https scheme and a host, suitable for an HA standby
// upstream base URL.
func validHTTPURL(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return false
	}
	return parsed.Scheme == "http" || parsed.Scheme == "https"
}

func validSHA256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if (value[i] < '0' || value[i] > '9') && (value[i] < 'a' || value[i] > 'f') {
			return false
		}
	}
	return true
}

func StandbyReplicationSlotCreateReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindReplicationSlotCreate, State: StandbyActionStateApplied}
}

func StandbyReplicationSlotResumeReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindReplicationSlotResume, State: StandbyActionStateApplied}
}

func StandbyReplicationSlotPauseReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindReplicationSlotPause, State: StandbyActionStateApplied}
}

func StandbyReplicationSlotDropReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindReplicationSlotDrop, State: StandbyActionStateApplied}
}

func StandbyBaseBackupBeginReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindBaseBackupBegin, State: StandbyActionStateApplied}
}

func StandbyBaseBackupFinishReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindBaseBackupFinish, State: StandbyActionStateApplied}
}

func StandbySeedCaptureReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindSeedCapture, State: StandbyActionStateApplied}
}

func StandbySeededSlotActivateReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindSeededSlotActivate, State: StandbyActionStateApplied}
}

func StandbyBootstrapReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindStandbyBootstrap, State: StandbyActionStateApplied}
}

func StandbyUpstreamReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindStandbyUpstream, State: StandbyActionStateApplied}
}

func StandbyFenceAcquireReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindFenceAcquire, State: StandbyActionStateApplied}
}

func StandbyPromotionAssessReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindPromotionAssess, State: StandbyActionStateAssessed}
}

func StandbyPromotionReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindPromotion, State: StandbyActionStateApplied}
}

func StandbyRejoinAssessReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindRejoinAssess, State: StandbyActionStateAssessed}
}

func StandbyRejoinRewindReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindRejoinRewind, State: StandbyActionStateApplied}
}

func StandbyRejoinReseedReceiptExpectation() StandbyReceiptExpectation {
	return StandbyReceiptExpectation{ActionKind: StandbyActionKindRejoinReseed, State: StandbyActionStateApplied}
}

// StandbyResponse keeps the typed response and the original response body together.
// The raw body is useful for callers that must validate field presence, not
// just decoded values.
type StandbyResponse[T any] struct {
	Value      *T
	Body       []byte
	StatusCode int
}

// StandbyAPIError describes a non-2xx HA admin API response.
type StandbyAPIError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *StandbyAPIError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s returned status %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("%s returned status %d: %s", e.Operation, e.StatusCode, e.Body)
}

// Retryable reports whether the status is a transient admin API failure.
func (e *StandbyAPIError) Retryable() bool {
	if e == nil {
		return false
	}
	switch e.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// StandbyStatusCode returns the HTTP status code for a wrapped HA admin API error.
func StandbyStatusCode(err error) (int, bool) {
	var apiErr *StandbyAPIError
	if errors.As(err, &apiErr) && apiErr != nil {
		return apiErr.StatusCode, true
	}
	return 0, false
}

// StandbyIsUnauthorized reports whether an HA admin API error was rejected by auth.
func StandbyIsUnauthorized(err error) bool {
	status, ok := StandbyStatusCode(err)
	return ok && status == http.StatusUnauthorized
}

// StandbyIsConflict reports whether an HA admin API error hit an operation conflict.
func StandbyIsConflict(err error) bool {
	status, ok := StandbyStatusCode(err)
	return ok && status == http.StatusConflict
}

// StandbyResponseValidationError describes a 2xx HA admin response that decoded but
// did not satisfy the wrapper's typed response contract.
type StandbyResponseValidationError struct {
	Operation string
	Err       error
}

func (e *StandbyResponseValidationError) Error() string {
	return fmt.Sprintf("%s response invalid: %v", e.Operation, e.Err)
}

func (e *StandbyResponseValidationError) Unwrap() error {
	return e.Err
}

// StandbyIsRetryable reports whether an error from the HA admin SDK is safe to
// retry without the caller reimplementing HTTP and transport classification.
func StandbyIsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var apiErr *StandbyAPIError
	if errors.As(err, &apiErr) {
		return apiErr.Retryable()
	}
	var validationErr *StandbyResponseValidationError
	if errors.As(err, &validationErr) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		return true
	}
	return false
}

// NewStandbyClient creates a typed HA admin client. The base URL may be the Antfly
// server root, an explicit /admin/v1 admin API root, or the /admin/v1/ha HA
// root advertised to operators.
func NewStandbyClient(baseURL string, httpClient *http.Client) (*StandbyClient, error) {
	var doer oapi.HttpRequestDoer = http.DefaultClient
	if httpClient != nil {
		doer = httpClient
	}
	return newStandbyClientWithDoer(baseURL, doer)
}

func newStandbyClientWithDoer(baseURL string, doer oapi.HttpRequestDoer) (*StandbyClient, error) {
	normalizedBaseURL, err := normalizeAdminBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	negotiating := &pathNegotiatingDoer{inner: doer}
	client, err := oapi.NewClientWithResponses(normalizedBaseURL, oapi.WithHTTPClient(negotiating))
	if err != nil {
		return nil, err
	}
	c := &StandbyClient{client: client, pathStyle: PathStyleLegacy, negotiator: newPathNegotiator(normalizedBaseURL)}
	negotiating.client = c
	c.rebuildEditors()
	return c, nil
}

// WithToken configures bearer-token authentication for standby admin requests.
func (c *StandbyClient) WithToken(token string) *StandbyClient {
	token = strings.TrimSpace(token)
	if token == "" {
		c.authEditor = nil
	} else {
		c.authEditor = func(_ context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		}
	}
	c.rebuildEditors()
	return c
}

// WithPathStyle selects whether the client sends requests to the legacy
// /admin/v1/ha prefix (PathStyleLegacy, the default this release), the
// canonical /admin/v1/standby prefix (PathStyleCanonical), or negotiates
// per server (PathStyleAuto, recommended).
func (c *StandbyClient) WithPathStyle(style PathStyle) *StandbyClient {
	c.pathStyle = style
	c.rebuildEditors()
	return c
}

// NegotiatedPathStyle reports the admin path style this client has confirmed
// for its target. Under PathStyleAuto, the second return value is false
// until a request has actually succeeded (or a previous client in this
// process already pinned an answer for the same base URL), and the first
// return value is meaningless until then. Under an explicit PathStyleLegacy
// or PathStyleCanonical, this simply reports that fixed style with pinned
// set to false, since no negotiation ever occurs: callers that need to treat
// a successful explicit-style request as proof should key off the client's
// configured style directly rather than this accessor.
func (c *StandbyClient) NegotiatedPathStyle() (PathStyle, bool) {
	if c == nil || c.negotiator == nil || c.pathStyle != PathStyleAuto {
		return PathStyleLegacy, false
	}
	return c.negotiator.current()
}

func acceptJSONEditor(_ context.Context, req *http.Request) error {
	req.Header.Set("Accept", "application/json")
	return nil
}

func normalizeAdminBaseURL(baseURL string) (string, error) {
	trimmedSpace := strings.TrimSpace(baseURL)
	if trimmedSpace == "" {
		return "", fmt.Errorf("invalid HA admin base URL %q", baseURL)
	}
	if baseURL != trimmedSpace || containsASCIIWhitespace(baseURL) {
		return "", fmt.Errorf("invalid HA admin base URL %q", baseURL)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid HA admin base URL %q", baseURL)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("invalid HA admin base URL %q", baseURL)
	}
	trimmed := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(trimmed, HAPath) {
		return strings.TrimSuffix(trimmed, HAPath) + adminV1Path, nil
	}
	if strings.HasSuffix(trimmed, StandbyPath) {
		return strings.TrimSuffix(trimmed, StandbyPath) + adminV1Path, nil
	}
	return strings.TrimSuffix(trimmed, adminV1Path) + adminV1Path, nil
}

func containsASCIIWhitespace(raw string) bool {
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			return true
		}
	}
	return false
}

func requireStandbyJSON200[T any](operation string, statusCode int, body []byte, value *T, err error) (*StandbyResponse[T], error) {
	if err != nil {
		return nil, err
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		return nil, &StandbyAPIError{
			Operation:  operation,
			StatusCode: statusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	if value == nil {
		return nil, &StandbyAPIError{
			Operation:  operation,
			StatusCode: statusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	return &StandbyResponse[T]{
		Value:      value,
		Body:       body,
		StatusCode: statusCode,
	}, nil
}

func requireStandbyJSON200Validated[T any](operation string, statusCode int, body []byte, value *T, err error, validate func(T) error) (*StandbyResponse[T], error) {
	return requireStandbyJSON200ValidatedEvidence(operation, statusCode, body, value, err, validate, nil)
}

func requireStandbyJSON200ValidatedEvidence[T any](operation string, statusCode int, body []byte, value *T, err error, validate func(T) error, validateEvidence func([]byte) error) (*StandbyResponse[T], error) {
	response, err := requireStandbyJSON200(operation, statusCode, body, value, err)
	if err != nil {
		return nil, err
	}
	if validate != nil {
		if err := validate(*response.Value); err != nil {
			return nil, &StandbyResponseValidationError{Operation: operation, Err: err}
		}
	}
	if validateEvidence != nil {
		if err := validateEvidence(response.Body); err != nil {
			return nil, &StandbyResponseValidationError{Operation: operation, Err: err}
		}
	}
	return response, nil
}

func requireStandby2xx(operation string, statusCode int, body []byte, err error) error {
	if err != nil {
		return err
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		return &StandbyAPIError{
			Operation:  operation,
			StatusCode: statusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	return nil
}

func standbyResponseValue[T any](response *StandbyResponse[T], err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	return response.Value, nil
}

func (c *StandbyClient) PrimaryStatusResponse(ctx context.Context, params *StandbyPrimaryStatusParams) (*StandbyResponse[StandbyPrimaryStatusResponse], error) {
	if err := validateStandbyPrimaryStatusParamsForRequest("get HA primary status", params); err != nil {
		return nil, err
	}
	resp, err := c.client.GetHAPrimaryStatusWithResponse(ctx, params, appendStandbyRequestEditors(c.editors, standbyPrimaryStatusQueryEditor(params))...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("get HA primary status", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyPrimaryStatusResponse, ValidateStandbyPrimaryStatusResponseEvidence)
}

func (c *StandbyClient) PrimaryStatus(ctx context.Context, params *StandbyPrimaryStatusParams) (*StandbyPrimaryStatusResponse, error) {
	return standbyResponseValue(c.PrimaryStatusResponse(ctx, params))
}

func (c *StandbyClient) WatchdogProofResponse(ctx context.Context) (*StandbyResponse[StandbyWatchdogProofResponse], error) {
	resp, err := c.client.GetHAWatchdogProofWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200Validated("get HA watchdog proof", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyWatchdogProofResponse)
}

func (c *StandbyClient) WatchdogProof(ctx context.Context) (*StandbyWatchdogProofResponse, error) {
	return standbyResponseValue(c.WatchdogProofResponse(ctx))
}

func (c *StandbyClient) PrimaryStatusParsedResponse(ctx context.Context, params *StandbyPrimaryStatusParams) (*StandbyResponse[ParsedStandbyPrimaryStatus], error) {
	if err := validateStandbyPrimaryStatusParamsForRequest("get HA primary status", params); err != nil {
		return nil, err
	}
	resp, err := c.client.GetHAPrimaryStatusWithResponse(ctx, params, appendStandbyRequestEditors(c.editors, standbyPrimaryStatusQueryEditor(params))...)
	if resp == nil {
		return nil, err
	}
	if err := requireStandby2xx("get HA primary status", resp.StatusCode(), resp.Body, err); err != nil {
		return nil, err
	}
	if err := validateDirectStandbyPrimaryStatusEvidence(resp.Body); err != nil {
		return nil, fmt.Errorf("parse HA primary status: %w", err)
	}
	parsed, err := ParseStandbyPrimaryStatus(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse HA primary status: %w", err)
	}
	return &StandbyResponse[ParsedStandbyPrimaryStatus]{
		Value:      parsed,
		Body:       resp.Body,
		StatusCode: resp.StatusCode(),
	}, nil
}

func (c *StandbyClient) PrimaryStatusParsed(ctx context.Context, params *StandbyPrimaryStatusParams) (*ParsedStandbyPrimaryStatus, error) {
	return standbyResponseValue(c.PrimaryStatusParsedResponse(ctx, params))
}

func appendStandbyRequestEditors(editors []oapi.RequestEditorFn, extra ...oapi.RequestEditorFn) []oapi.RequestEditorFn {
	combined := make([]oapi.RequestEditorFn, 0, len(editors)+len(extra))
	combined = append(combined, editors...)
	combined = append(combined, extra...)
	return combined
}

func standbyPrimaryStatusQueryEditor(params *StandbyPrimaryStatusParams) oapi.RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		if params == nil || req == nil || req.URL == nil {
			return nil
		}
		query := req.URL.Query()
		removeZeroQueryParam(query, "max_lag_lsn")
		removeZeroQueryParam(query, "max_retained_bytes")
		removeZeroQueryParam(query, "max_retained_age_ns")
		removeZeroQueryParam(query, "sync_required")
		removeEmptyQueryParam(query, "sync_mode")
		removeEmptyQueryParam(query, "sync_selection")
		removeEmptyQueryParam(query, "sync_standby")
		removeEmptyQueryParam(query, "sync_failure")
		if params.SyncSelection == StandbyPrimaryStatusSyncSelectionAll {
			query.Del("sync_required")
		}
		req.URL.RawQuery = query.Encode()
		return nil
	}
}

func removeZeroQueryParam(query url.Values, key string) {
	values, ok := query[key]
	if ok && len(values) == 1 && values[0] == "0" {
		query.Del(key)
	}
}

func removeEmptyQueryParam(query url.Values, key string) {
	values, ok := query[key]
	if ok && len(values) == 1 && values[0] == "" {
		query.Del(key)
	}
}

func (c *StandbyClient) StandbyStatusResponse(ctx context.Context, params *StandbyStatusParams) (*StandbyResponse[StandbyStatusResponse], error) {
	resp, err := c.client.GetHAStandbyStatusWithResponse(ctx, params, appendStandbyRequestEditors(c.editors, standbyStatusQueryEditor(params))...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("get HA standby status", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyStatusResponse, ValidateStandbyStatusResponseEvidence)
}

func (c *StandbyClient) StandbyStatus(ctx context.Context, params *StandbyStatusParams) (*StandbyStatusResponse, error) {
	return standbyResponseValue(c.StandbyStatusResponse(ctx, params))
}

func (c *StandbyClient) StandbyStatusParsedResponse(ctx context.Context, params *StandbyStatusParams) (*StandbyResponse[ParsedStandbyStatus], error) {
	resp, err := c.client.GetHAStandbyStatusWithResponse(ctx, params, appendStandbyRequestEditors(c.editors, standbyStatusQueryEditor(params))...)
	if resp == nil {
		return nil, err
	}
	if err := requireStandby2xx("get HA standby status", resp.StatusCode(), resp.Body, err); err != nil {
		return nil, err
	}
	if err := validateDirectStandbyStatusEvidence(resp.Body); err != nil {
		return nil, fmt.Errorf("parse HA standby status: %w", err)
	}
	parsed, err := ParseStandbyStatus(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse HA standby status: %w", err)
	}
	return &StandbyResponse[ParsedStandbyStatus]{
		Value:      parsed,
		Body:       resp.Body,
		StatusCode: resp.StatusCode(),
	}, nil
}

func (c *StandbyClient) StandbyStatusParsed(ctx context.Context, params *StandbyStatusParams) (*ParsedStandbyStatus, error) {
	return standbyResponseValue(c.StandbyStatusParsedResponse(ctx, params))
}

func standbyStatusQueryEditor(params *StandbyStatusParams) oapi.RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		if params == nil || req == nil || req.URL == nil {
			return nil
		}
		query := req.URL.Query()
		removeZeroQueryParam(query, "upstream_lsn")
		req.URL.RawQuery = query.Encode()
		return nil
	}
}

func (c *StandbyClient) AppendCommitResponse(ctx context.Context, body CommitAppendRequest) (*StandbyResponse[StandbyCommitAppendResponse], error) {
	if err := validateStandbySyncPolicyForRequest("append HA commit", body.SyncPolicy); err != nil {
		return nil, err
	}
	resp, err := c.client.AppendHACommitWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200Validated("append HA commit", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyCommitAppendResponse)
}

func (c *StandbyClient) AppendCommit(ctx context.Context, body CommitAppendRequest) (*StandbyCommitAppendResponse, error) {
	return standbyResponseValue(c.AppendCommitResponse(ctx, body))
}

func (c *StandbyClient) CheckCommitResponse(ctx context.Context, body CommitCheckRequest) (*StandbyResponse[StandbyCommitCheckResponse], error) {
	if err := validateStandbySyncPolicyForRequest("check HA commit", body.SyncPolicy); err != nil {
		return nil, err
	}
	resp, err := c.client.CheckHACommitWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200Validated("check HA commit", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyCommitCheckResponse)
}

func (c *StandbyClient) CheckCommit(ctx context.Context, body CommitCheckRequest) (*StandbyCommitCheckResponse, error) {
	return standbyResponseValue(c.CheckCommitResponse(ctx, body))
}

func (c *StandbyClient) CheckReadResponse(ctx context.Context, body ReadCheckRequest) (*StandbyResponse[StandbyReadCheckResponse], error) {
	resp, err := c.client.CheckHAReadWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200Validated("check HA read", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyReadCheckResponse)
}

func (c *StandbyClient) CheckRead(ctx context.Context, body ReadCheckRequest) (*StandbyReadCheckResponse, error) {
	return standbyResponseValue(c.CheckReadResponse(ctx, body))
}

func (c *StandbyClient) CheckWriteResponse(ctx context.Context, body WriteCheckRequest) (*StandbyResponse[StandbyWriteCheckResponse], error) {
	resp, err := c.client.CheckHAWriteWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200Validated("check HA write", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyWriteCheckResponse)
}

func (c *StandbyClient) CheckWrite(ctx context.Context, body WriteCheckRequest) (*StandbyWriteCheckResponse, error) {
	return standbyResponseValue(c.CheckWriteResponse(ctx, body))
}

func (c *StandbyClient) CheckOwnerJobResponse(ctx context.Context, body OwnerJobCheckRequest) (*StandbyResponse[StandbyOwnerJobCheckResponse], error) {
	resp, err := c.client.CheckHAOwnerJobWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200Validated("check HA owner job", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyOwnerJobCheckResponse)
}

func (c *StandbyClient) CheckOwnerJob(ctx context.Context, body OwnerJobCheckRequest) (*StandbyOwnerJobCheckResponse, error) {
	return standbyResponseValue(c.CheckOwnerJobResponse(ctx, body))
}

func (c *StandbyClient) ListReplicationSlotsResponse(ctx context.Context) (*StandbyResponse[StandbyReplicationSlotListResponse], error) {
	resp, err := c.client.ListHAReplicationSlotsWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("list HA replication slots", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyReplicationSlotListResponse, ValidateStandbyReplicationSlotListResponseEvidence)
}

func (c *StandbyClient) ListReplicationSlots(ctx context.Context) (*StandbyReplicationSlotListResponse, error) {
	return standbyResponseValue(c.ListReplicationSlotsResponse(ctx))
}

func (c *StandbyClient) CreateReplicationSlotResponse(ctx context.Context, body ReplicationSlotCreateRequest) (*StandbyResponse[StandbyReplicationSlotActionResponse], error) {
	if err := validateStandbyReplicationSlotNameForRequest("create HA replication slot", body.SlotName); err != nil {
		return nil, err
	}
	resp, err := c.client.CreateHAReplicationSlotWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("create HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyReplicationSlotActionResponse, ValidateStandbyReplicationSlotActionResponseEvidence)
}

func (c *StandbyClient) CreateReplicationSlot(ctx context.Context, body ReplicationSlotCreateRequest) (*StandbyReplicationSlotActionResponse, error) {
	return standbyResponseValue(c.CreateReplicationSlotResponse(ctx, body))
}

func (c *StandbyClient) PauseReplicationSlotResponse(ctx context.Context, slotName string) (*StandbyResponse[StandbyReplicationSlotActionResponse], error) {
	if err := validateStandbyReplicationSlotNameForRequest("pause HA replication slot", slotName); err != nil {
		return nil, err
	}
	resp, err := c.client.PauseHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("pause HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyReplicationSlotActionResponse, ValidateStandbyReplicationSlotActionResponseEvidence)
}

func (c *StandbyClient) PauseReplicationSlot(ctx context.Context, slotName string) (*StandbyReplicationSlotActionResponse, error) {
	return standbyResponseValue(c.PauseReplicationSlotResponse(ctx, slotName))
}

func (c *StandbyClient) ResumeReplicationSlotResponse(ctx context.Context, slotName string) (*StandbyResponse[StandbyReplicationSlotActionResponse], error) {
	if err := validateStandbyReplicationSlotNameForRequest("resume HA replication slot", slotName); err != nil {
		return nil, err
	}
	resp, err := c.client.ResumeHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("resume HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyReplicationSlotActionResponse, ValidateStandbyReplicationSlotActionResponseEvidence)
}

func (c *StandbyClient) ResumeReplicationSlot(ctx context.Context, slotName string) (*StandbyReplicationSlotActionResponse, error) {
	return standbyResponseValue(c.ResumeReplicationSlotResponse(ctx, slotName))
}

func (c *StandbyClient) DropReplicationSlotResponse(ctx context.Context, slotName string) (*StandbyResponse[StandbyReplicationSlotActionResponse], error) {
	if err := validateStandbyReplicationSlotNameForRequest("drop HA replication slot", slotName); err != nil {
		return nil, err
	}
	resp, err := c.client.DropHAReplicationSlotWithResponse(ctx, slotName, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("drop HA replication slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyReplicationSlotActionResponse, ValidateStandbyReplicationSlotActionResponseEvidence)
}

func (c *StandbyClient) DropReplicationSlot(ctx context.Context, slotName string) (*StandbyReplicationSlotActionResponse, error) {
	return standbyResponseValue(c.DropReplicationSlotResponse(ctx, slotName))
}

func validateStandbyReplicationSlotNameForRequest(operation string, slotName string) error {
	return validateStandbyIdentifierForRequest(operation, "replication slot name", slotName)
}

func validateStandbyNodeIDForRequest(operation string, field string, nodeID string) error {
	return validateStandbyIdentifierForRequest(operation, field, nodeID)
}

func validateStandbyIdentifierForRequest(operation string, field string, value string) error {
	if validStandbyIdentifier(value) {
		return nil
	}
	return fmt.Errorf("%s: invalid HA %s %q", operation, field, value)
}

func validateStandbyIdentifierListForRequest(operation string, field string, values []string) error {
	for i, value := range values {
		if err := validateStandbyIdentifierForRequest(operation, fmt.Sprintf("%s[%d]", field, i), value); err != nil {
			return err
		}
	}
	return nil
}

func validateStandbyPrimaryStatusParamsForRequest(operation string, params *StandbyPrimaryStatusParams) error {
	if params == nil {
		return nil
	}
	return validateStandbyIdentifierListForRequest(operation, "sync_standby", params.SyncStandby)
}

func validateStandbySyncPolicyForRequest(operation string, policy StandbySyncPolicy) error {
	return validateStandbyIdentifierListForRequest(operation, "standby_names", policy.StandbyNames)
}

// validateStandbyFenceAcquireRequestForRequest validates the locally-checkable
// fields of a fence acquisition/promotion request. It intentionally does not
// require body.Generation to be set: the field is optional (oapi-codegen
// renders it as a value uint64 with omitzero, so a zero value is
// indistinguishable from "absent" on the wire), and the server allocates the
// next generation itself when it is omitted. Callers whose fencing authority
// is a Kubernetes Lease must still supply the exact Lease transition
// generation; that requirement is enforced server-side, not here.
func validateStandbyFenceAcquireRequestForRequest(operation string, body FenceAcquireRequest) error {
	if err := validateStandbyNodeIDForRequest(operation, "old_primary_id", body.OldPrimaryId); err != nil {
		return err
	}
	return validateStandbyNodeIDForRequest(operation, "promoted_node_id", body.PromotedNodeId)
}

func validateStandbyUpstreamRequestForRequest(operation string, body StandbyUpstreamRequest) error {
	if err := validateStandbyIdentityForRequest(operation, "identity", body.Identity); err != nil {
		return err
	}
	if err := validateStandbyUpstreamURLForRequest(operation, "upstream_url", body.UpstreamUrl); err != nil {
		return err
	}
	return validateStandbyReplicationSlotNameForRequest(operation, body.SlotName)
}

func validateStandbyIdentityForRequest(operation string, field string, identity StandbyIdentity) error {
	if StandbyIdentityComplete(identity) {
		return nil
	}
	return fmt.Errorf("%s: invalid HA %s %+v", operation, field, identity)
}

func validateStandbyUpstreamURLForRequest(operation string, field string, value string) error {
	if validHTTPURL(value) {
		return nil
	}
	return fmt.Errorf("%s: invalid HA %s %q", operation, field, value)
}

func validateStandbyRejoinAssessRequestForRequest(operation string, body RejoinAssessRequest) error {
	if err := validateStandbyNodeIDForRequest(operation, "node_id", body.NodeId); err != nil {
		return err
	}
	if StandbyFenceReceiptEmpty(body.Receipt) {
		return nil
	}
	return validateStandbyFenceReceiptIdentifiersForRequest(operation, "receipt", body.Receipt)
}

func validateStandbyFenceReceiptIdentifiersForRequest(operation string, field string, receipt StandbyFenceReceipt) error {
	if err := validateStandbyNodeIDForRequest(operation, field+".old_primary_id", receipt.OldPrimaryId); err != nil {
		return err
	}
	return validateStandbyNodeIDForRequest(operation, field+".promoted_node_id", receipt.PromotedNodeId)
}

func validateStandbyPathForRequest(operation string, field string, value string) error {
	if value == "" || strings.TrimSpace(value) != value || !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return fmt.Errorf("%s: invalid HA %s %q", operation, field, value)
	}
	return nil
}

func validateStandbyOptionalPathForRequest(operation string, field string, value string) error {
	if value == "" {
		return nil
	}
	return validateStandbyPathForRequest(operation, field, value)
}

func (c *StandbyClient) BeginBaseBackupResponse(ctx context.Context, body BaseBackupStartRequest) (*StandbyResponse[StandbyBaseBackupBeginResponse], error) {
	if err := validateStandbyReplicationSlotNameForRequest("begin HA base backup", body.SlotName); err != nil {
		return nil, err
	}
	resp, err := c.client.BeginHABaseBackupWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("begin HA base backup", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyBaseBackupBeginResponse, ValidateStandbyBaseBackupBeginResponseEvidence)
}

func (c *StandbyClient) BeginBaseBackup(ctx context.Context, body BaseBackupStartRequest) (*StandbyBaseBackupBeginResponse, error) {
	return standbyResponseValue(c.BeginBaseBackupResponse(ctx, body))
}

func (c *StandbyClient) FinishBaseBackupResponse(ctx context.Context, body BaseBackupManifestPathRequest) (*StandbyResponse[StandbyBaseBackupFinishResponse], error) {
	if err := validateStandbyPathForRequest("finish HA base backup", "manifest_path", body.ManifestPath); err != nil {
		return nil, err
	}
	resp, err := c.client.FinishHABaseBackupWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("finish HA base backup", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyBaseBackupFinishResponse, ValidateStandbyBaseBackupFinishResponseEvidence)
}

func (c *StandbyClient) FinishBaseBackup(ctx context.Context, body BaseBackupManifestPathRequest) (*StandbyBaseBackupFinishResponse, error) {
	return standbyResponseValue(c.FinishBaseBackupResponse(ctx, body))
}

func (c *StandbyClient) CaptureSeedArtifactResponse(ctx context.Context, body SeedArtifactCaptureRequest) (*StandbyResponse[StandbySeedArtifactCaptureResponse], error) {
	if err := validateStandbyReplicationSlotNameForRequest("capture HA seed artifact", body.SlotName); err != nil {
		return nil, err
	}
	if !validStandbyIdentifier(body.Generation) {
		return nil, fmt.Errorf("capture HA seed artifact requires a valid generation")
	}
	for field, value := range map[string]string{
		"topology_id":     body.TopologyId,
		"node_id":         body.NodeId,
		"target_pvc_name": body.TargetPvcName,
		"target_pvc_uid":  body.TargetPvcUid,
	} {
		if err := validateStandbyIdentifierForRequest("capture HA seed artifact", field, value); err != nil {
			return nil, err
		}
	}
	if body.TopologyGeneration == 0 {
		return nil, fmt.Errorf("capture HA seed artifact requires topology_generation")
	}
	resp, err := c.client.CaptureHASeedArtifactWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	result, err := requireStandbyJSON200ValidatedEvidence("capture HA seed artifact", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbySeedArtifactCaptureResponse, ValidateStandbySeedArtifactCaptureResponseEvidence)
	if err != nil {
		return nil, err
	}
	value := result.Value
	if value.SlotName != body.SlotName || value.Generation != body.Generation ||
		value.TopologyId != body.TopologyId || value.TopologyGeneration != body.TopologyGeneration ||
		value.NodeId != body.NodeId || value.TargetPvcName != body.TargetPvcName || value.TargetPvcUid != body.TargetPvcUid {
		return nil, &StandbyResponseValidationError{Operation: "capture HA seed artifact", Err: fmt.Errorf("response lifecycle binding does not match request")}
	}
	return result, nil
}

func (c *StandbyClient) CaptureSeedArtifact(ctx context.Context, body SeedArtifactCaptureRequest) (*StandbySeedArtifactCaptureResponse, error) {
	return standbyResponseValue(c.CaptureSeedArtifactResponse(ctx, body))
}

func (c *StandbyClient) SeedLifecycleReceiptsResponse(ctx context.Context, params *StandbySeedLifecycleReceiptParams) (*StandbyResponse[StandbySeedLifecycleReceiptInventory], error) {
	if params == nil {
		return nil, fmt.Errorf("get HA seed lifecycle receipts requires kind")
	}
	if params.Kind != StandbySeedLifecycleReceiptKindCapture && params.Kind != StandbySeedLifecycleReceiptKindActivation {
		return nil, fmt.Errorf("get HA seed lifecycle receipts requires capture or activation kind")
	}
	if params.Limit > 1000 {
		return nil, fmt.Errorf("get HA seed lifecycle receipts limit exceeds 1000")
	}
	resp, err := c.client.GetHASeedLifecycleReceiptsWithResponse(ctx, params, c.editors...)
	if resp == nil {
		return nil, err
	}
	result, err := requireStandbyJSON200ValidatedEvidence("get HA seed lifecycle receipts", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbySeedLifecycleReceiptInventory, ValidateStandbySeedLifecycleReceiptInventoryEvidence)
	if err != nil {
		return nil, err
	}
	for _, event := range result.Value.Entries {
		if string(event.Kind) != string(params.Kind) {
			return nil, &StandbyResponseValidationError{Operation: "get HA seed lifecycle receipts", Err: fmt.Errorf("response kind does not match request")}
		}
		if event.Cursor <= params.After {
			return nil, &StandbyResponseValidationError{Operation: "get HA seed lifecycle receipts", Err: fmt.Errorf("response cursor is not after request cursor")}
		}
	}
	return result, nil
}

func (c *StandbyClient) SeedLifecycleReceipts(ctx context.Context, params *StandbySeedLifecycleReceiptParams) (*StandbySeedLifecycleReceiptInventory, error) {
	return standbyResponseValue(c.SeedLifecycleReceiptsResponse(ctx, params))
}

func (c *StandbyClient) ActivateSeededSlotResponse(ctx context.Context, body SeededSlotActivateRequest) (*StandbyResponse[StandbySeededSlotActivateResponse], error) {
	if err := validateStandbyReplicationSlotNameForRequest("activate HA seeded slot", body.SlotName); err != nil {
		return nil, err
	}
	if !validStandbyIdentifier(body.Generation) || strings.TrimSpace(body.ManifestId) == "" || body.TimelineId == 0 || body.CheckpointLsn == 0 {
		return nil, fmt.Errorf("activate HA seeded slot requires generation, manifest, timeline and checkpoint evidence")
	}
	if !validSHA256Hex(body.SeedReceiptSha256) || !validSHA256Hex(body.CaptureReceiptSha256) ||
		!validSHA256Hex(body.ManifestSha256) || !validSHA256Hex(body.AggregateSha256) {
		return nil, fmt.Errorf("activate HA seeded slot requires lowercase SHA-256 digest evidence")
	}
	resp, err := c.client.ActivateHASeededSlotWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("activate HA seeded slot", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbySeededSlotActivateResponse, ValidateStandbySeededSlotActivateResponseEvidence)
}

func (c *StandbyClient) ActivateSeededSlot(ctx context.Context, body SeededSlotActivateRequest) (*StandbySeededSlotActivateResponse, error) {
	return standbyResponseValue(c.ActivateSeededSlotResponse(ctx, body))
}

func (c *StandbyClient) BootstrapStandbyResponse(ctx context.Context, body StandbyBootstrapRequest) (*StandbyResponse[StandbyBootstrapResponse], error) {
	if err := validateStandbyPathForRequest("bootstrap HA standby", "manifest_path", body.ManifestPath); err != nil {
		return nil, err
	}
	if err := validateStandbyOptionalPathForRequest("bootstrap HA standby", "content_root", body.ContentRoot); err != nil {
		return nil, err
	}
	resp, err := c.client.BootstrapHAStandbyWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("bootstrap HA standby", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyBootstrapResponse, ValidateStandbyBootstrapResponseEvidence)
}

func (c *StandbyClient) BootstrapStandby(ctx context.Context, body StandbyBootstrapRequest) (*StandbyBootstrapResponse, error) {
	return standbyResponseValue(c.BootstrapStandbyResponse(ctx, body))
}

func (c *StandbyClient) SetStandbyUpstreamResponse(ctx context.Context, body StandbyUpstreamRequest) (*StandbyResponse[StandbyUpstreamResponse], error) {
	if err := validateStandbyUpstreamRequestForRequest("set HA standby upstream", body); err != nil {
		return nil, err
	}
	resp, err := c.client.SetHAStandbyUpstreamWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("set HA standby upstream", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyUpstreamResponse, ValidateStandbyUpstreamResponseEvidence)
}

func (c *StandbyClient) SetStandbyUpstream(ctx context.Context, body StandbyUpstreamRequest) (*StandbyUpstreamResponse, error) {
	return standbyResponseValue(c.SetStandbyUpstreamResponse(ctx, body))
}

func (c *StandbyClient) AcquireFenceResponse(ctx context.Context, body FenceAcquireRequest) (*StandbyResponse[StandbyFenceResponse], error) {
	if err := validateStandbyFenceAcquireRequestForRequest("acquire HA fence", body); err != nil {
		return nil, err
	}
	resp, err := c.client.AcquireHAFenceWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("acquire HA fence", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyFenceResponse, ValidateStandbyFenceResponseEvidence)
}

func (c *StandbyClient) AcquireFence(ctx context.Context, body FenceAcquireRequest) (*StandbyFenceResponse, error) {
	return standbyResponseValue(c.AcquireFenceResponse(ctx, body))
}

func (c *StandbyClient) CurrentFenceResponse(ctx context.Context) (*StandbyResponse[StandbyCurrentFenceResponse], error) {
	resp, err := c.client.GetHACurrentFenceWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("get current HA fence", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyCurrentFenceResponse, ValidateStandbyCurrentFenceResponseEvidence)
}

func (c *StandbyClient) CurrentFence(ctx context.Context) (*StandbyCurrentFenceResponse, error) {
	return standbyResponseValue(c.CurrentFenceResponse(ctx))
}

func (c *StandbyClient) AssessPromotionResponse(ctx context.Context, body PromotionAssessRequest) (*StandbyResponse[StandbyPromotionAssessResponse], error) {
	resp, err := c.client.AssessHAPromotionWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("assess HA promotion", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyPromotionAssessResponse, ValidateStandbyPromotionAssessResponseEvidence)
}

func (c *StandbyClient) AssessPromotion(ctx context.Context, body PromotionAssessRequest) (*StandbyPromotionAssessResponse, error) {
	return standbyResponseValue(c.AssessPromotionResponse(ctx, body))
}

func (c *StandbyClient) PromoteResponse(ctx context.Context, body FenceAcquireRequest) (*StandbyResponse[StandbyPromotionResponse], error) {
	if err := validateStandbyFenceAcquireRequestForRequest("promote HA standby", body); err != nil {
		return nil, err
	}
	resp, err := c.client.PromoteHAWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("promote HA standby", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyPromotionResponse, ValidateStandbyPromotionResponseEvidence)
}

func (c *StandbyClient) Promote(ctx context.Context, body FenceAcquireRequest) (*StandbyPromotionResponse, error) {
	return standbyResponseValue(c.PromoteResponse(ctx, body))
}

func (c *StandbyClient) PromoteWithCurrentFenceResponse(ctx context.Context) (*StandbyResponse[StandbyPromotionResponse], error) {
	resp, err := c.client.PromoteHAWithCurrentFenceWithResponse(ctx, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("promote HA standby with current fence", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyPromotionResponse, ValidateStandbyPromotionResponseEvidence)
}

func (c *StandbyClient) PromoteWithCurrentFence(ctx context.Context) (*StandbyPromotionResponse, error) {
	return standbyResponseValue(c.PromoteWithCurrentFenceResponse(ctx))
}

func (c *StandbyClient) AssessRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*StandbyResponse[StandbyRejoinAssessResponse], error) {
	if err := validateStandbyRejoinAssessRequestForRequest("assess HA rejoin", body); err != nil {
		return nil, err
	}
	resp, err := c.client.AssessHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("assess HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyRejoinAssessResponse, ValidateStandbyRejoinAssessResponseEvidence)
}

func (c *StandbyClient) AssessRejoin(ctx context.Context, body RejoinAssessRequest) (*StandbyRejoinAssessResponse, error) {
	return standbyResponseValue(c.AssessRejoinResponse(ctx, body))
}

func (c *StandbyClient) RewindRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*StandbyResponse[StandbyRejoinAssessResponse], error) {
	if err := validateStandbyRejoinAssessRequestForRequest("rewind HA rejoin", body); err != nil {
		return nil, err
	}
	resp, err := c.client.RewindHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("rewind HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyRejoinAssessResponse, ValidateStandbyRejoinAssessResponseEvidence)
}

func (c *StandbyClient) RewindRejoin(ctx context.Context, body RejoinAssessRequest) (*StandbyRejoinAssessResponse, error) {
	return standbyResponseValue(c.RewindRejoinResponse(ctx, body))
}

func (c *StandbyClient) ReseedRejoinResponse(ctx context.Context, body RejoinAssessRequest) (*StandbyResponse[StandbyRejoinAssessResponse], error) {
	if err := validateStandbyRejoinAssessRequestForRequest("reseed HA rejoin", body); err != nil {
		return nil, err
	}
	resp, err := c.client.ReseedHARejoinWithResponse(ctx, body, c.editors...)
	if resp == nil {
		return nil, err
	}
	return requireStandbyJSON200ValidatedEvidence("reseed HA rejoin", resp.StatusCode(), resp.Body, resp.JSON200, err, ValidateStandbyRejoinAssessResponse, ValidateStandbyRejoinAssessResponseEvidence)
}

func (c *StandbyClient) ReseedRejoin(ctx context.Context, body RejoinAssessRequest) (*StandbyRejoinAssessResponse, error) {
	return standbyResponseValue(c.ReseedRejoinResponse(ctx, body))
}

// -----------------------------------------------------------------
// Deprecated aliases.
//
// The identifiers below are pre-rename HA* names for the Standby*
// canonical names defined above. They exist only so that code built
// against the pre-rename API (notably go/pkg/operator, which keeps
// calling the legacy /admin/v1/ha paths via PathStyleLegacy for this
// release) keeps compiling unchanged. All are removed after 0.4.
// -----------------------------------------------------------------

type (
	HAAPIError                         = StandbyAPIError                         // Deprecated: use StandbyAPIError.
	HAActionReceipt                    = StandbyActionReceipt                    // Deprecated: use StandbyActionReceipt.
	HAActionReceiptActionKind          = StandbyActionReceiptActionKind          // Deprecated: use StandbyActionReceiptActionKind.
	HAActionReceiptState               = StandbyActionReceiptState               // Deprecated: use StandbyActionReceiptState.
	HABaseBackupBeginResponse          = StandbyBaseBackupBeginResponse          // Deprecated: use StandbyBaseBackupBeginResponse.
	HABaseBackupFinishResponse         = StandbyBaseBackupFinishResponse         // Deprecated: use StandbyBaseBackupFinishResponse.
	HAClient                           = StandbyClient                           // Deprecated: use StandbyClient.
	HACommitAppendResponse             = StandbyCommitAppendResponse             // Deprecated: use StandbyCommitAppendResponse.
	HACommitCheckResponse              = StandbyCommitCheckResponse              // Deprecated: use StandbyCommitCheckResponse.
	HACommitGate                       = StandbyCommitGate                       // Deprecated: use StandbyCommitGate.
	HACommitGateAction                 = StandbyCommitGateAction                 // Deprecated: use StandbyCommitGateAction.
	HACurrentFenceResponse             = StandbyCurrentFenceResponse             // Deprecated: use StandbyCurrentFenceResponse.
	HADurabilityDecision               = StandbyDurabilityDecision               // Deprecated: use StandbyDurabilityDecision.
	HADurabilityDecisionMode           = StandbyDurabilityDecisionMode           // Deprecated: use StandbyDurabilityDecisionMode.
	HADurabilityDecisionSelection      = StandbyDurabilityDecisionSelection      // Deprecated: use StandbyDurabilityDecisionSelection.
	HADurabilityDecisionStatus         = StandbyDurabilityDecisionStatus         // Deprecated: use StandbyDurabilityDecisionStatus.
	HAFenceReceipt                     = StandbyFenceReceipt                     // Deprecated: use StandbyFenceReceipt.
	HAFenceResponse                    = StandbyFenceResponse                    // Deprecated: use StandbyFenceResponse.
	HAIdentity                         = StandbyIdentity                         // Deprecated: use StandbyIdentity.
	HALeaseWatchdogProof               = StandbyLeaseWatchdogProof               // Deprecated: use StandbyLeaseWatchdogProof.
	HAOperation                        = StandbyOperation                        // Deprecated: use StandbyOperation.
	HAOwnerJobCheckResponse            = StandbyOwnerJobCheckResponse            // Deprecated: use StandbyOwnerJobCheckResponse.
	HAOwnerJobDecision                 = StandbyOwnerJobDecision                 // Deprecated: use StandbyOwnerJobDecision.
	HAOwnerJobDecisionAction           = StandbyOwnerJobDecisionAction           // Deprecated: use StandbyOwnerJobDecisionAction.
	HAOwnerJobDecisionKind             = StandbyOwnerJobDecisionKind             // Deprecated: use StandbyOwnerJobDecisionKind.
	HAOwnerJobDecisionRole             = StandbyOwnerJobDecisionRole             // Deprecated: use StandbyOwnerJobDecisionRole.
	HAPrimarySnapshot                  = StandbyPrimarySnapshot                  // Deprecated: use StandbyPrimarySnapshot.
	HAPrimarySnapshotRole              = StandbyPrimarySnapshotRole              // Deprecated: use StandbyPrimarySnapshotRole.
	HAPrimaryStatusParams              = StandbyPrimaryStatusParams              // Deprecated: use StandbyPrimaryStatusParams.
	HAPrimaryStatusParamsSyncFail      = StandbyPrimaryStatusParamsSyncFail      // Deprecated: use StandbyPrimaryStatusParamsSyncFail.
	HAPrimaryStatusParamsSyncMode      = StandbyPrimaryStatusParamsSyncMode      // Deprecated: use StandbyPrimaryStatusParamsSyncMode.
	HAPrimaryStatusParamsSyncSelection = StandbyPrimaryStatusParamsSyncSelection // Deprecated: use StandbyPrimaryStatusParamsSyncSelection.
	HAPrimaryStatusResponse            = StandbyPrimaryStatusResponse            // Deprecated: use StandbyPrimaryStatusResponse.
	HAPromotionAssessResponse          = StandbyPromotionAssessResponse          // Deprecated: use StandbyPromotionAssessResponse.
	HAPromotionAssessment              = StandbyPromotionAssessment              // Deprecated: use StandbyPromotionAssessment.
	HAPromotionAssessmentMode          = StandbyPromotionAssessmentMode          // Deprecated: use StandbyPromotionAssessmentMode.
	HAPromotionHandoff                 = StandbyPromotionHandoff                 // Deprecated: use StandbyPromotionHandoff.
	HAPromotionResponse                = StandbyPromotionResponse                // Deprecated: use StandbyPromotionResponse.
	HAPromotionResult                  = StandbyPromotionResult                  // Deprecated: use StandbyPromotionResult.
	HAReadCheckResponse                = StandbyReadCheckResponse                // Deprecated: use StandbyReadCheckResponse.
	HAReadDecision                     = StandbyReadDecision                     // Deprecated: use StandbyReadDecision.
	HAReadDecisionAction               = StandbyReadDecisionAction               // Deprecated: use StandbyReadDecisionAction.
	HAReadDecisionConsistency          = StandbyReadDecisionConsistency          // Deprecated: use StandbyReadDecisionConsistency.
	HAReceiptExpectation               = StandbyReceiptExpectation               // Deprecated: use StandbyReceiptExpectation.
	HARejoinAssessResponse             = StandbyRejoinAssessResponse             // Deprecated: use StandbyRejoinAssessResponse.
	HARejoinAssessment                 = StandbyRejoinAssessment                 // Deprecated: use StandbyRejoinAssessment.
	HARejoinAssessmentAction           = StandbyRejoinAssessmentAction           // Deprecated: use StandbyRejoinAssessmentAction.
	HARejoinAssessmentReason           = StandbyRejoinAssessmentReason           // Deprecated: use StandbyRejoinAssessmentReason.
	HARejoinReseedResult               = StandbyRejoinReseedResult               // Deprecated: use StandbyRejoinReseedResult.
	HARejoinRewindResult               = StandbyRejoinRewindResult               // Deprecated: use StandbyRejoinRewindResult.
	HAReplicationSlot                  = StandbyReplicationSlot                  // Deprecated: use StandbyReplicationSlot.
	HAReplicationSlotAction            = StandbyReplicationSlotAction            // Deprecated: use StandbyReplicationSlotAction.
	HAReplicationSlotActionResponse    = StandbyReplicationSlotActionResponse    // Deprecated: use StandbyReplicationSlotActionResponse.
	HAReplicationSlotListResponse      = StandbyReplicationSlotListResponse      // Deprecated: use StandbyReplicationSlotListResponse.
	HAResponse[T any]                  = StandbyResponse[T]                      // Deprecated: use StandbyResponse.
	HAResponseValidationError          = StandbyResponseValidationError          // Deprecated: use StandbyResponseValidationError.
	HARetentionSnapshot                = StandbyRetentionSnapshot                // Deprecated: use StandbyRetentionSnapshot.
	HARuntimeLifecycleObservation      = StandbyRuntimeLifecycleObservation      // Deprecated: use StandbyRuntimeLifecycleObservation.
	HASeedArtifactCaptureResponse      = StandbySeedArtifactCaptureResponse      // Deprecated: use StandbySeedArtifactCaptureResponse.
	HASeedLifecycleReceiptEvent        = StandbySeedLifecycleReceiptEvent        // Deprecated: use StandbySeedLifecycleReceiptEvent.
	HASeedLifecycleReceiptInventory    = StandbySeedLifecycleReceiptInventory    // Deprecated: use StandbySeedLifecycleReceiptInventory.
	HASeedLifecycleReceiptKind         = StandbySeedLifecycleReceiptKind         // Deprecated: use StandbySeedLifecycleReceiptKind.
	HASeedLifecycleReceiptParams       = StandbySeedLifecycleReceiptParams       // Deprecated: use StandbySeedLifecycleReceiptParams.
	HASeededSlotActivateResponse       = StandbySeededSlotActivateResponse       // Deprecated: use StandbySeededSlotActivateResponse.
	HASlotSnapshot                     = StandbySlotSnapshot                     // Deprecated: use StandbySlotSnapshot.
	HASlotSnapshotStatus               = StandbySlotSnapshotStatus               // Deprecated: use StandbySlotSnapshotStatus.
	HAStandbyBootstrapResponse         = StandbyBootstrapResponse                // Deprecated: use StandbyBootstrapResponse.
	HAStandbySnapshot                  = StandbySnapshot                         // Deprecated: use StandbySnapshot.
	HAStandbySnapshotRole              = StandbySnapshotRole                     // Deprecated: use StandbySnapshotRole.
	HAStandbyStatusParams              = StandbyStatusParams                     // Deprecated: use StandbyStatusParams.
	HAStandbyStatusResponse            = StandbyStatusResponse                   // Deprecated: use StandbyStatusResponse.
	HAStandbyUpstream                  = StandbyUpstream                         // Deprecated: use StandbyUpstream.
	HAStandbyUpstreamResponse          = StandbyUpstreamResponse                 // Deprecated: use StandbyUpstreamResponse.
	HASyncPolicy                       = StandbySyncPolicy                       // Deprecated: use StandbySyncPolicy.
	HASyncPolicyFailurePolicy          = StandbySyncPolicyFailurePolicy          // Deprecated: use StandbySyncPolicyFailurePolicy.
	HASyncPolicyMode                   = StandbySyncPolicyMode                   // Deprecated: use StandbySyncPolicyMode.
	HASyncPolicySelection              = StandbySyncPolicySelection              // Deprecated: use StandbySyncPolicySelection.
	HAWatchdogProofResponse            = StandbyWatchdogProofResponse            // Deprecated: use StandbyWatchdogProofResponse.
	HAWriteCheckResponse               = StandbyWriteCheckResponse               // Deprecated: use StandbyWriteCheckResponse.
	HAWriteDecision                    = StandbyWriteDecision                    // Deprecated: use StandbyWriteDecision.
	HAWriteDecisionAction              = StandbyWriteDecisionAction              // Deprecated: use StandbyWriteDecisionAction.
	HAWriteDecisionRole                = StandbyWriteDecisionRole                // Deprecated: use StandbyWriteDecisionRole.
	ParsedHAPrimaryStatus              = ParsedStandbyPrimaryStatus              // Deprecated: use ParsedStandbyPrimaryStatus.
	ParsedHAStandbyStatus              = ParsedStandbyStatus                     // Deprecated: use ParsedStandbyStatus.
)

const (
	HAActionKindBaseBackupBegin                 = StandbyActionKindBaseBackupBegin                 // Deprecated: use StandbyActionKindBaseBackupBegin.
	HAActionKindBaseBackupFinish                = StandbyActionKindBaseBackupFinish                // Deprecated: use StandbyActionKindBaseBackupFinish.
	HAActionKindFenceAcquire                    = StandbyActionKindFenceAcquire                    // Deprecated: use StandbyActionKindFenceAcquire.
	HAActionKindPromotion                       = StandbyActionKindPromotion                       // Deprecated: use StandbyActionKindPromotion.
	HAActionKindPromotionAssess                 = StandbyActionKindPromotionAssess                 // Deprecated: use StandbyActionKindPromotionAssess.
	HAActionKindRejoinAssess                    = StandbyActionKindRejoinAssess                    // Deprecated: use StandbyActionKindRejoinAssess.
	HAActionKindRejoinReseed                    = StandbyActionKindRejoinReseed                    // Deprecated: use StandbyActionKindRejoinReseed.
	HAActionKindRejoinRewind                    = StandbyActionKindRejoinRewind                    // Deprecated: use StandbyActionKindRejoinRewind.
	HAActionKindReplicationSlotCreate           = StandbyActionKindReplicationSlotCreate           // Deprecated: use StandbyActionKindReplicationSlotCreate.
	HAActionKindReplicationSlotDrop             = StandbyActionKindReplicationSlotDrop             // Deprecated: use StandbyActionKindReplicationSlotDrop.
	HAActionKindReplicationSlotPause            = StandbyActionKindReplicationSlotPause            // Deprecated: use StandbyActionKindReplicationSlotPause.
	HAActionKindReplicationSlotResume           = StandbyActionKindReplicationSlotResume           // Deprecated: use StandbyActionKindReplicationSlotResume.
	HAActionKindSeedCapture                     = StandbyActionKindSeedCapture                     // Deprecated: use StandbyActionKindSeedCapture.
	HAActionKindSeededSlotActivate              = StandbyActionKindSeededSlotActivate              // Deprecated: use StandbyActionKindSeededSlotActivate.
	HAActionKindStandbyBootstrap                = StandbyActionKindStandbyBootstrap                // Deprecated: use StandbyActionKindStandbyBootstrap.
	HAActionKindStandbyUpstream                 = StandbyActionKindStandbyUpstream                 // Deprecated: use StandbyActionKindStandbyUpstream.
	HAActionStateAlreadyApplied                 = StandbyActionStateAlreadyApplied                 // Deprecated: use StandbyActionStateAlreadyApplied.
	HAActionStateApplied                        = StandbyActionStateApplied                        // Deprecated: use StandbyActionStateApplied.
	HAActionStateAssessed                       = StandbyActionStateAssessed                       // Deprecated: use StandbyActionStateAssessed.
	HACommitGateActionAcknowledge               = StandbyCommitGateActionAcknowledge               // Deprecated: use StandbyCommitGateActionAcknowledge.
	HACommitGateActionAcknowledgeDegraded       = StandbyCommitGateActionAcknowledgeDegraded       // Deprecated: use StandbyCommitGateActionAcknowledgeDegraded.
	HACommitGateActionReject                    = StandbyCommitGateActionReject                    // Deprecated: use StandbyCommitGateActionReject.
	HACommitGateActionWaitForStandby            = StandbyCommitGateActionWaitForStandby            // Deprecated: use StandbyCommitGateActionWaitForStandby.
	HADurabilityModeAsync                       = StandbyDurabilityModeAsync                       // Deprecated: use StandbyDurabilityModeAsync.
	HADurabilityModeRemoteApply                 = StandbyDurabilityModeRemoteApply                 // Deprecated: use StandbyDurabilityModeRemoteApply.
	HADurabilityModeRemoteWrite                 = StandbyDurabilityModeRemoteWrite                 // Deprecated: use StandbyDurabilityModeRemoteWrite.
	HADurabilitySelectionAll                    = StandbyDurabilitySelectionAll                    // Deprecated: use StandbyDurabilitySelectionAll.
	HADurabilitySelectionAny                    = StandbyDurabilitySelectionAny                    // Deprecated: use StandbyDurabilitySelectionAny.
	HADurabilitySelectionFirst                  = StandbyDurabilitySelectionFirst                  // Deprecated: use StandbyDurabilitySelectionFirst.
	HADurabilityStatusDegradedToAsync           = StandbyDurabilityStatusDegradedToAsync           // Deprecated: use StandbyDurabilityStatusDegradedToAsync.
	HADurabilityStatusFailClosed                = StandbyDurabilityStatusFailClosed                // Deprecated: use StandbyDurabilityStatusFailClosed.
	HADurabilityStatusSatisfied                 = StandbyDurabilityStatusSatisfied                 // Deprecated: use StandbyDurabilityStatusSatisfied.
	HADurabilityStatusWouldBlock                = StandbyDurabilityStatusWouldBlock                // Deprecated: use StandbyDurabilityStatusWouldBlock.
	HAOwnerJobDecisionActionDisableOnStandby    = StandbyOwnerJobDecisionActionDisableOnStandby    // Deprecated: use StandbyOwnerJobDecisionActionDisableOnStandby.
	HAOwnerJobDecisionActionOpenPromotedPrimary = StandbyOwnerJobDecisionActionOpenPromotedPrimary // Deprecated: use StandbyOwnerJobDecisionActionOpenPromotedPrimary.
	HAOwnerJobDecisionActionRun                 = StandbyOwnerJobDecisionActionRun                 // Deprecated: use StandbyOwnerJobDecisionActionRun.
	HAOwnerJobDecisionKindCompactionPublish     = StandbyOwnerJobDecisionKindCompactionPublish     // Deprecated: use StandbyOwnerJobDecisionKindCompactionPublish.
	HAOwnerJobDecisionKindDerivedEffectWriter   = StandbyOwnerJobDecisionKindDerivedEffectWriter   // Deprecated: use StandbyOwnerJobDecisionKindDerivedEffectWriter.
	HAOwnerJobDecisionKindEnrichmentWriter      = StandbyOwnerJobDecisionKindEnrichmentWriter      // Deprecated: use StandbyOwnerJobDecisionKindEnrichmentWriter.
	HAOwnerJobDecisionKindRetentionAdvance      = StandbyOwnerJobDecisionKindRetentionAdvance      // Deprecated: use StandbyOwnerJobDecisionKindRetentionAdvance.
	HAOwnerJobDecisionRolePrimary               = StandbyOwnerJobDecisionRolePrimary               // Deprecated: use StandbyOwnerJobDecisionRolePrimary.
	HAOwnerJobDecisionRolePromotedStandby       = StandbyOwnerJobDecisionRolePromotedStandby       // Deprecated: use StandbyOwnerJobDecisionRolePromotedStandby.
	HAOwnerJobDecisionRoleStandby               = StandbyOwnerJobDecisionRoleStandby               // Deprecated: use StandbyOwnerJobDecisionRoleStandby.
	HAPrimarySnapshotRolePrimary                = StandbyPrimarySnapshotRolePrimary                // Deprecated: use StandbyPrimarySnapshotRolePrimary.
	HAPrimaryStatusSyncFailureBlock             = StandbyPrimaryStatusSyncFailureBlock             // Deprecated: use StandbyPrimaryStatusSyncFailureBlock.
	HAPrimaryStatusSyncFailureDegradeToAsync    = StandbyPrimaryStatusSyncFailureDegradeToAsync    // Deprecated: use StandbyPrimaryStatusSyncFailureDegradeToAsync.
	HAPrimaryStatusSyncFailureFailClosed        = StandbyPrimaryStatusSyncFailureFailClosed        // Deprecated: use StandbyPrimaryStatusSyncFailureFailClosed.
	HAPrimaryStatusSyncModeAsync                = StandbyPrimaryStatusSyncModeAsync                // Deprecated: use StandbyPrimaryStatusSyncModeAsync.
	HAPrimaryStatusSyncModeRemoteApply          = StandbyPrimaryStatusSyncModeRemoteApply          // Deprecated: use StandbyPrimaryStatusSyncModeRemoteApply.
	HAPrimaryStatusSyncModeRemoteWrite          = StandbyPrimaryStatusSyncModeRemoteWrite          // Deprecated: use StandbyPrimaryStatusSyncModeRemoteWrite.
	HAPrimaryStatusSyncSelectionAll             = StandbyPrimaryStatusSyncSelectionAll             // Deprecated: use StandbyPrimaryStatusSyncSelectionAll.
	HAPrimaryStatusSyncSelectionAny             = StandbyPrimaryStatusSyncSelectionAny             // Deprecated: use StandbyPrimaryStatusSyncSelectionAny.
	HAPrimaryStatusSyncSelectionFirst           = StandbyPrimaryStatusSyncSelectionFirst           // Deprecated: use StandbyPrimaryStatusSyncSelectionFirst.
	HAPromotionModeBlocked                      = StandbyPromotionModeBlocked                      // Deprecated: use StandbyPromotionModeBlocked.
	HAPromotionModeForced                       = StandbyPromotionModeForced                       // Deprecated: use StandbyPromotionModeForced.
	HAPromotionModeLossy                        = StandbyPromotionModeLossy                        // Deprecated: use StandbyPromotionModeLossy.
	HAPromotionModeSafe                         = StandbyPromotionModeSafe                         // Deprecated: use StandbyPromotionModeSafe.
	HAReadDecisionActionRouteToPrimary          = StandbyReadDecisionActionRouteToPrimary          // Deprecated: use StandbyReadDecisionActionRouteToPrimary.
	HAReadDecisionActionServeStandby            = StandbyReadDecisionActionServeStandby            // Deprecated: use StandbyReadDecisionActionServeStandby.
	HAReadDecisionActionWaitForApply            = StandbyReadDecisionActionWaitForApply            // Deprecated: use StandbyReadDecisionActionWaitForApply.
	HAReadDecisionActionWaitForMetadata         = StandbyReadDecisionActionWaitForMetadata         // Deprecated: use StandbyReadDecisionActionWaitForMetadata.
	HAReadDecisionConsistencyAtLeastLSN         = StandbyReadDecisionConsistencyAtLeastLSN         // Deprecated: use StandbyReadDecisionConsistencyAtLeastLSN.
	HAReadDecisionConsistencyPrimary            = StandbyReadDecisionConsistencyPrimary            // Deprecated: use StandbyReadDecisionConsistencyPrimary.
	HAReadDecisionConsistencyStaleOK            = StandbyReadDecisionConsistencyStaleOK            // Deprecated: use StandbyReadDecisionConsistencyStaleOK.
	HARejoinActionAlreadyCurrent                = StandbyRejoinActionAlreadyCurrent                // Deprecated: use StandbyRejoinActionAlreadyCurrent.
	HARejoinActionRejectUnfenced                = StandbyRejoinActionRejectUnfenced                // Deprecated: use StandbyRejoinActionRejectUnfenced.
	HARejoinActionReseed                        = StandbyRejoinActionReseed                        // Deprecated: use StandbyRejoinActionReseed.
	HARejoinActionRewind                        = StandbyRejoinActionRewind                        // Deprecated: use StandbyRejoinActionRewind.
	HARejoinReasonCurrentTimeline               = StandbyRejoinReasonCurrentTimeline               // Deprecated: use StandbyRejoinReasonCurrentTimeline.
	HARejoinReasonIncompatibleTimeline          = StandbyRejoinReasonIncompatibleTimeline          // Deprecated: use StandbyRejoinReasonIncompatibleTimeline.
	HARejoinReasonLocalLSNBeforeFork            = StandbyRejoinReasonLocalLSNBeforeFork            // Deprecated: use StandbyRejoinReasonLocalLSNBeforeFork.
	HARejoinReasonNoFence                       = StandbyRejoinReasonNoFence                       // Deprecated: use StandbyRejoinReasonNoFence.
	HARejoinReasonParentTimelineRetained        = StandbyRejoinReasonParentTimelineRetained        // Deprecated: use StandbyRejoinReasonParentTimelineRetained.
	HARejoinReasonParentTimelineWALExpired      = StandbyRejoinReasonParentTimelineWALExpired      // Deprecated: use StandbyRejoinReasonParentTimelineWALExpired.
	HARejoinReasonWrongCluster                  = StandbyRejoinReasonWrongCluster                  // Deprecated: use StandbyRejoinReasonWrongCluster.
	HARejoinReasonWrongOldPrimary               = StandbyRejoinReasonWrongOldPrimary               // Deprecated: use StandbyRejoinReasonWrongOldPrimary.
	HARejoinReasonWrongShard                    = StandbyRejoinReasonWrongShard                    // Deprecated: use StandbyRejoinReasonWrongShard.
	HARejoinReasonWrongTable                    = StandbyRejoinReasonWrongTable                    // Deprecated: use StandbyRejoinReasonWrongTable.
	HAReplicationSlotActionCreate               = StandbyReplicationSlotActionCreate               // Deprecated: use StandbyReplicationSlotActionCreate.
	HAReplicationSlotActionDrop                 = StandbyReplicationSlotActionDrop                 // Deprecated: use StandbyReplicationSlotActionDrop.
	HAReplicationSlotActionPause                = StandbyReplicationSlotActionPause                // Deprecated: use StandbyReplicationSlotActionPause.
	HAReplicationSlotActionResume               = StandbyReplicationSlotActionResume               // Deprecated: use StandbyReplicationSlotActionResume.
	HASeedLifecycleReceiptKindActivation        = StandbySeedLifecycleReceiptKindActivation        // Deprecated: use StandbySeedLifecycleReceiptKindActivation.
	HASeedLifecycleReceiptKindCapture           = StandbySeedLifecycleReceiptKindCapture           // Deprecated: use StandbySeedLifecycleReceiptKindCapture.
	HASlotSnapshotStatusHealthy                 = StandbySlotSnapshotStatusHealthy                 // Deprecated: use StandbySlotSnapshotStatusHealthy.
	HASlotSnapshotStatusLagging                 = StandbySlotSnapshotStatusLagging                 // Deprecated: use StandbySlotSnapshotStatusLagging.
	HASlotSnapshotStatusReseedRequired          = StandbySlotSnapshotStatusReseedRequired          // Deprecated: use StandbySlotSnapshotStatusReseedRequired.
	HAStandbySnapshotRoleStandby                = StandbySnapshotRoleStandby                       // Deprecated: use StandbySnapshotRoleStandby.
	HASyncPolicyFailureBlock                    = StandbySyncPolicyFailureBlock                    // Deprecated: use StandbySyncPolicyFailureBlock.
	HASyncPolicyFailureDegradeToAsync           = StandbySyncPolicyFailureDegradeToAsync           // Deprecated: use StandbySyncPolicyFailureDegradeToAsync.
	HASyncPolicyFailureFailClosed               = StandbySyncPolicyFailureFailClosed               // Deprecated: use StandbySyncPolicyFailureFailClosed.
	HASyncPolicyModeAsync                       = StandbySyncPolicyModeAsync                       // Deprecated: use StandbySyncPolicyModeAsync.
	HASyncPolicyModeRemoteApply                 = StandbySyncPolicyModeRemoteApply                 // Deprecated: use StandbySyncPolicyModeRemoteApply.
	HASyncPolicyModeRemoteWrite                 = StandbySyncPolicyModeRemoteWrite                 // Deprecated: use StandbySyncPolicyModeRemoteWrite.
	HASyncPolicySelectionAll                    = StandbySyncPolicySelectionAll                    // Deprecated: use StandbySyncPolicySelectionAll.
	HASyncPolicySelectionAny                    = StandbySyncPolicySelectionAny                    // Deprecated: use StandbySyncPolicySelectionAny.
	HASyncPolicySelectionFirst                  = StandbySyncPolicySelectionFirst                  // Deprecated: use StandbySyncPolicySelectionFirst.
	HAWriteDecisionActionAllowWrite             = StandbyWriteDecisionActionAllowWrite             // Deprecated: use StandbyWriteDecisionActionAllowWrite.
	HAWriteDecisionActionOpenPromotedPrimary    = StandbyWriteDecisionActionOpenPromotedPrimary    // Deprecated: use StandbyWriteDecisionActionOpenPromotedPrimary.
	HAWriteDecisionActionRejectFencedPrimary    = StandbyWriteDecisionActionRejectFencedPrimary    // Deprecated: use StandbyWriteDecisionActionRejectFencedPrimary.
	HAWriteDecisionActionRejectReadOnly         = StandbyWriteDecisionActionRejectReadOnly         // Deprecated: use StandbyWriteDecisionActionRejectReadOnly.
	HAWriteDecisionRoleFencedPrimary            = StandbyWriteDecisionRoleFencedPrimary            // Deprecated: use StandbyWriteDecisionRoleFencedPrimary.
	HAWriteDecisionRolePrimary                  = StandbyWriteDecisionRolePrimary                  // Deprecated: use StandbyWriteDecisionRolePrimary.
	HAWriteDecisionRolePromotedStandby          = StandbyWriteDecisionRolePromotedStandby          // Deprecated: use StandbyWriteDecisionRolePromotedStandby.
	HAWriteDecisionRoleStandby                  = StandbyWriteDecisionRoleStandby                  // Deprecated: use StandbyWriteDecisionRoleStandby.
)

// Deprecated function aliases. Each var holds the corresponding
// Standby-named function above; signatures are unchanged.
var (
	HAActionReceiptPresent                          = StandbyActionReceiptPresent                          // Deprecated: use StandbyActionReceiptPresent.
	HABaseBackupBeginReceiptExpectation             = StandbyBaseBackupBeginReceiptExpectation             // Deprecated: use StandbyBaseBackupBeginReceiptExpectation.
	HABaseBackupFinishReceiptExpectation            = StandbyBaseBackupFinishReceiptExpectation            // Deprecated: use StandbyBaseBackupFinishReceiptExpectation.
	HACommitGateActionValid                         = StandbyCommitGateActionValid                         // Deprecated: use StandbyCommitGateActionValid.
	HACommitGateComplete                            = StandbyCommitGateComplete                            // Deprecated: use StandbyCommitGateComplete.
	HADurabilityDecisionComplete                    = StandbyDurabilityDecisionComplete                    // Deprecated: use StandbyDurabilityDecisionComplete.
	HADurabilityDecisionEmpty                       = StandbyDurabilityDecisionEmpty                       // Deprecated: use StandbyDurabilityDecisionEmpty.
	HADurabilityDecisionModeValid                   = StandbyDurabilityDecisionModeValid                   // Deprecated: use StandbyDurabilityDecisionModeValid.
	HADurabilityDecisionSelectionValid              = StandbyDurabilityDecisionSelectionValid              // Deprecated: use StandbyDurabilityDecisionSelectionValid.
	HADurabilityDecisionStatusValid                 = StandbyDurabilityDecisionStatusValid                 // Deprecated: use StandbyDurabilityDecisionStatusValid.
	HAFenceAcquireReceiptExpectation                = StandbyFenceAcquireReceiptExpectation                // Deprecated: use StandbyFenceAcquireReceiptExpectation.
	HAFenceReceiptComplete                          = StandbyFenceReceiptComplete                          // Deprecated: use StandbyFenceReceiptComplete.
	HAFenceReceiptEmpty                             = StandbyFenceReceiptEmpty                             // Deprecated: use StandbyFenceReceiptEmpty.
	HAIdentityComplete                              = StandbyIdentityComplete                              // Deprecated: use StandbyIdentityComplete.
	HAIsConflict                                    = StandbyIsConflict                                    // Deprecated: use StandbyIsConflict.
	HAIsRetryable                                   = StandbyIsRetryable                                   // Deprecated: use StandbyIsRetryable.
	HAIsUnauthorized                                = StandbyIsUnauthorized                                // Deprecated: use StandbyIsUnauthorized.
	HAOwnerJobDecisionActionValid                   = StandbyOwnerJobDecisionActionValid                   // Deprecated: use StandbyOwnerJobDecisionActionValid.
	HAOwnerJobDecisionComplete                      = StandbyOwnerJobDecisionComplete                      // Deprecated: use StandbyOwnerJobDecisionComplete.
	HAOwnerJobDecisionKindValid                     = StandbyOwnerJobDecisionKindValid                     // Deprecated: use StandbyOwnerJobDecisionKindValid.
	HAOwnerJobDecisionRoleValid                     = StandbyOwnerJobDecisionRoleValid                     // Deprecated: use StandbyOwnerJobDecisionRoleValid.
	HAPromotionAssessReceiptExpectation             = StandbyPromotionAssessReceiptExpectation             // Deprecated: use StandbyPromotionAssessReceiptExpectation.
	HAPromotionAssessmentComplete                   = StandbyPromotionAssessmentComplete                   // Deprecated: use StandbyPromotionAssessmentComplete.
	HAPromotionHandoffCompleteOrEmpty               = StandbyPromotionHandoffCompleteOrEmpty               // Deprecated: use StandbyPromotionHandoffCompleteOrEmpty.
	HAPromotionHandoffEmpty                         = StandbyPromotionHandoffEmpty                         // Deprecated: use StandbyPromotionHandoffEmpty.
	HAPromotionReceiptExpectation                   = StandbyPromotionReceiptExpectation                   // Deprecated: use StandbyPromotionReceiptExpectation.
	HAPromotionResultComplete                       = StandbyPromotionResultComplete                       // Deprecated: use StandbyPromotionResultComplete.
	HAReadDecisionActionValid                       = StandbyReadDecisionActionValid                       // Deprecated: use StandbyReadDecisionActionValid.
	HAReadDecisionComplete                          = StandbyReadDecisionComplete                          // Deprecated: use StandbyReadDecisionComplete.
	HAReadDecisionConsistencyValid                  = StandbyReadDecisionConsistencyValid                  // Deprecated: use StandbyReadDecisionConsistencyValid.
	HAReceiptMatches                                = StandbyReceiptMatches                                // Deprecated: use StandbyReceiptMatches.
	HAReceiptMatchesNode                            = StandbyReceiptMatchesNode                            // Deprecated: use StandbyReceiptMatchesNode.
	HAReceiptNodeMatches                            = StandbyReceiptNodeMatches                            // Deprecated: use StandbyReceiptNodeMatches.
	HARejoinAssessReceiptExpectation                = StandbyRejoinAssessReceiptExpectation                // Deprecated: use StandbyRejoinAssessReceiptExpectation.
	HARejoinAssessmentActionValid                   = StandbyRejoinAssessmentActionValid                   // Deprecated: use StandbyRejoinAssessmentActionValid.
	HARejoinAssessmentComplete                      = StandbyRejoinAssessmentComplete                      // Deprecated: use StandbyRejoinAssessmentComplete.
	HARejoinAssessmentReasonValid                   = StandbyRejoinAssessmentReasonValid                   // Deprecated: use StandbyRejoinAssessmentReasonValid.
	HARejoinReseedComplete                          = StandbyRejoinReseedComplete                          // Deprecated: use StandbyRejoinReseedComplete.
	HARejoinReseedReceiptExpectation                = StandbyRejoinReseedReceiptExpectation                // Deprecated: use StandbyRejoinReseedReceiptExpectation.
	HARejoinRewindComplete                          = StandbyRejoinRewindComplete                          // Deprecated: use StandbyRejoinRewindComplete.
	HARejoinRewindReceiptExpectation                = StandbyRejoinRewindReceiptExpectation                // Deprecated: use StandbyRejoinRewindReceiptExpectation.
	HAReplicationSlotComplete                       = StandbyReplicationSlotComplete                       // Deprecated: use StandbyReplicationSlotComplete.
	HAReplicationSlotCreateReceiptExpectation       = StandbyReplicationSlotCreateReceiptExpectation       // Deprecated: use StandbyReplicationSlotCreateReceiptExpectation.
	HAReplicationSlotDropReceiptExpectation         = StandbyReplicationSlotDropReceiptExpectation         // Deprecated: use StandbyReplicationSlotDropReceiptExpectation.
	HAReplicationSlotPauseReceiptExpectation        = StandbyReplicationSlotPauseReceiptExpectation        // Deprecated: use StandbyReplicationSlotPauseReceiptExpectation.
	HAReplicationSlotResumeReceiptExpectation       = StandbyReplicationSlotResumeReceiptExpectation       // Deprecated: use StandbyReplicationSlotResumeReceiptExpectation.
	HASeedCaptureReceiptExpectation                 = StandbySeedCaptureReceiptExpectation                 // Deprecated: use StandbySeedCaptureReceiptExpectation.
	HASeededSlotActivateReceiptExpectation          = StandbySeededSlotActivateReceiptExpectation          // Deprecated: use StandbySeededSlotActivateReceiptExpectation.
	HASlotSnapshotStatusValid                       = StandbySlotSnapshotStatusValid                       // Deprecated: use StandbySlotSnapshotStatusValid.
	HAStandbyBootstrapReceiptExpectation            = StandbyBootstrapReceiptExpectation                   // Deprecated: use StandbyBootstrapReceiptExpectation.
	HAStandbyUpstreamComplete                       = StandbyUpstreamComplete                              // Deprecated: use StandbyUpstreamComplete.
	HAStandbyUpstreamReceiptExpectation             = StandbyUpstreamReceiptExpectation                    // Deprecated: use StandbyUpstreamReceiptExpectation.
	HAStatusCode                                    = StandbyStatusCode                                    // Deprecated: use StandbyStatusCode.
	HAWriteDecisionActionValid                      = StandbyWriteDecisionActionValid                      // Deprecated: use StandbyWriteDecisionActionValid.
	HAWriteDecisionComplete                         = StandbyWriteDecisionComplete                         // Deprecated: use StandbyWriteDecisionComplete.
	HAWriteDecisionRoleValid                        = StandbyWriteDecisionRoleValid                        // Deprecated: use StandbyWriteDecisionRoleValid.
	NewHAClient                                     = NewStandbyClient                                     // Deprecated: use NewStandbyClient.
	ParseHAPrimaryStatus                            = ParseStandbyPrimaryStatus                            // Deprecated: use ParseStandbyPrimaryStatus.
	ParseHAStandbyStatus                            = ParseStandbyStatus                                   // Deprecated: use ParseStandbyStatus.
	ValidateHABaseBackupBeginResponse               = ValidateStandbyBaseBackupBeginResponse               // Deprecated: use ValidateStandbyBaseBackupBeginResponse.
	ValidateHABaseBackupBeginResponseEvidence       = ValidateStandbyBaseBackupBeginResponseEvidence       // Deprecated: use ValidateStandbyBaseBackupBeginResponseEvidence.
	ValidateHABaseBackupFinishResponse              = ValidateStandbyBaseBackupFinishResponse              // Deprecated: use ValidateStandbyBaseBackupFinishResponse.
	ValidateHABaseBackupFinishResponseEvidence      = ValidateStandbyBaseBackupFinishResponseEvidence      // Deprecated: use ValidateStandbyBaseBackupFinishResponseEvidence.
	ValidateHACommitAppendResponse                  = ValidateStandbyCommitAppendResponse                  // Deprecated: use ValidateStandbyCommitAppendResponse.
	ValidateHACommitCheckResponse                   = ValidateStandbyCommitCheckResponse                   // Deprecated: use ValidateStandbyCommitCheckResponse.
	ValidateHACurrentFenceResponse                  = ValidateStandbyCurrentFenceResponse                  // Deprecated: use ValidateStandbyCurrentFenceResponse.
	ValidateHACurrentFenceResponseEvidence          = ValidateStandbyCurrentFenceResponseEvidence          // Deprecated: use ValidateStandbyCurrentFenceResponseEvidence.
	ValidateHAFenceResponse                         = ValidateStandbyFenceResponse                         // Deprecated: use ValidateStandbyFenceResponse.
	ValidateHAFenceResponseEvidence                 = ValidateStandbyFenceResponseEvidence                 // Deprecated: use ValidateStandbyFenceResponseEvidence.
	ValidateHAOwnerJobCheckResponse                 = ValidateStandbyOwnerJobCheckResponse                 // Deprecated: use ValidateStandbyOwnerJobCheckResponse.
	ValidateHAPrimaryStatusResponse                 = ValidateStandbyPrimaryStatusResponse                 // Deprecated: use ValidateStandbyPrimaryStatusResponse.
	ValidateHAPrimaryStatusResponseEvidence         = ValidateStandbyPrimaryStatusResponseEvidence         // Deprecated: use ValidateStandbyPrimaryStatusResponseEvidence.
	ValidateHAPromotionAssessResponse               = ValidateStandbyPromotionAssessResponse               // Deprecated: use ValidateStandbyPromotionAssessResponse.
	ValidateHAPromotionAssessResponseEvidence       = ValidateStandbyPromotionAssessResponseEvidence       // Deprecated: use ValidateStandbyPromotionAssessResponseEvidence.
	ValidateHAPromotionResponse                     = ValidateStandbyPromotionResponse                     // Deprecated: use ValidateStandbyPromotionResponse.
	ValidateHAPromotionResponseEvidence             = ValidateStandbyPromotionResponseEvidence             // Deprecated: use ValidateStandbyPromotionResponseEvidence.
	ValidateHAReadCheckResponse                     = ValidateStandbyReadCheckResponse                     // Deprecated: use ValidateStandbyReadCheckResponse.
	ValidateHARejoinAssessResponse                  = ValidateStandbyRejoinAssessResponse                  // Deprecated: use ValidateStandbyRejoinAssessResponse.
	ValidateHARejoinAssessResponseEvidence          = ValidateStandbyRejoinAssessResponseEvidence          // Deprecated: use ValidateStandbyRejoinAssessResponseEvidence.
	ValidateHAReplicationSlotActionResponse         = ValidateStandbyReplicationSlotActionResponse         // Deprecated: use ValidateStandbyReplicationSlotActionResponse.
	ValidateHAReplicationSlotActionResponseEvidence = ValidateStandbyReplicationSlotActionResponseEvidence // Deprecated: use ValidateStandbyReplicationSlotActionResponseEvidence.
	ValidateHAReplicationSlotListResponse           = ValidateStandbyReplicationSlotListResponse           // Deprecated: use ValidateStandbyReplicationSlotListResponse.
	ValidateHAReplicationSlotListResponseEvidence   = ValidateStandbyReplicationSlotListResponseEvidence   // Deprecated: use ValidateStandbyReplicationSlotListResponseEvidence.
	ValidateHASeedArtifactCaptureResponse           = ValidateStandbySeedArtifactCaptureResponse           // Deprecated: use ValidateStandbySeedArtifactCaptureResponse.
	ValidateHASeedArtifactCaptureResponseEvidence   = ValidateStandbySeedArtifactCaptureResponseEvidence   // Deprecated: use ValidateStandbySeedArtifactCaptureResponseEvidence.
	ValidateHASeedLifecycleReceiptInventory         = ValidateStandbySeedLifecycleReceiptInventory         // Deprecated: use ValidateStandbySeedLifecycleReceiptInventory.
	ValidateHASeedLifecycleReceiptInventoryEvidence = ValidateStandbySeedLifecycleReceiptInventoryEvidence // Deprecated: use ValidateStandbySeedLifecycleReceiptInventoryEvidence.
	ValidateHASeededSlotActivateResponse            = ValidateStandbySeededSlotActivateResponse            // Deprecated: use ValidateStandbySeededSlotActivateResponse.
	ValidateHASeededSlotActivateResponseEvidence    = ValidateStandbySeededSlotActivateResponseEvidence    // Deprecated: use ValidateStandbySeededSlotActivateResponseEvidence.
	ValidateHAStandbyBootstrapResponse              = ValidateStandbyBootstrapResponse                     // Deprecated: use ValidateStandbyBootstrapResponse.
	ValidateHAStandbyBootstrapResponseEvidence      = ValidateStandbyBootstrapResponseEvidence             // Deprecated: use ValidateStandbyBootstrapResponseEvidence.
	ValidateHAStandbyStatusResponse                 = ValidateStandbyStatusResponse                        // Deprecated: use ValidateStandbyStatusResponse.
	ValidateHAStandbyStatusResponseEvidence         = ValidateStandbyStatusResponseEvidence                // Deprecated: use ValidateStandbyStatusResponseEvidence.
	ValidateHAStandbyUpstreamResponse               = ValidateStandbyUpstreamResponse                      // Deprecated: use ValidateStandbyUpstreamResponse.
	ValidateHAStandbyUpstreamResponseEvidence       = ValidateStandbyUpstreamResponseEvidence              // Deprecated: use ValidateStandbyUpstreamResponseEvidence.
	ValidateHAWatchdogProofResponse                 = ValidateStandbyWatchdogProofResponse                 // Deprecated: use ValidateStandbyWatchdogProofResponse.
	ValidateHAWriteCheckResponse                    = ValidateStandbyWriteCheckResponse                    // Deprecated: use ValidateStandbyWriteCheckResponse.
)
