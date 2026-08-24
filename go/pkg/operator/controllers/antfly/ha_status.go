package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	adminsdk "github.com/antflydb/antfly/go/pkg/sdk/admin"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type haActionKind string

const (
	haActionCreateSlot              haActionKind = "CreateSlot"
	haActionResumeSlot              haActionKind = "ResumeSlot"
	haActionPauseSlot               haActionKind = "PauseSlot"
	haActionDropSlot                haActionKind = "DropSlot"
	haActionSeedStandby             haActionKind = "SeedStandby"
	haActionFinishStandbySeed       haActionKind = "FinishStandbySeed"
	haActionCaptureSeedArtifact     haActionKind = "CaptureSeedArtifact"
	haActionPublishSeedArtifact     haActionKind = "PublishSeedArtifact"
	haActionGCSourceSeedGenerations haActionKind = "GCSourceSeedGenerations"
	haActionRestoreSeedArtifact     haActionKind = "RestoreSeedArtifact"
	haActionActivateSeedArtifact    haActionKind = "ActivateSeedArtifact"
	haActionActivateSeededSlot      haActionKind = "ActivateSeededSlot"
	haActionGCTargetSeedGenerations haActionKind = "GCTargetSeedGenerations"
	haActionBootstrapStandbySeed    haActionKind = "BootstrapStandbySeed"
	haActionPruneSeedArtifacts      haActionKind = "PruneSeedArtifacts"
	haActionMarkReseed              haActionKind = "MarkReseed"
	haActionAcquireFence            haActionKind = "AcquireFence"
	haActionAssessPromotion         haActionKind = "AssessPromotion"
	haActionPromoteStandby          haActionKind = "PromoteStandby"
	haActionUpdatePrimaryRoute      haActionKind = "UpdatePrimaryRoute"
	haActionIsolateFormerPrimary    haActionKind = "IsolateFormerPrimary"
	haActionFenceFormerPrimary      haActionKind = "FenceFormerPrimary"
	haActionDemoteFormerPrimary     haActionKind = "DemoteFormerPrimary"
	haActionRewindFormerPrimary     haActionKind = "RewindFormerPrimary"
	haActionReseedFormerPrimary     haActionKind = "ReseedFormerPrimary"
)

type haActionPhase string

const (
	haActionPhaseReconcile haActionPhase = "Reconcile"
	haActionPhaseSeed      haActionPhase = "Seed"
	haActionPhaseFence     haActionPhase = "Fence"
	haActionPhasePromote   haActionPhase = "Promote"
	haActionPhaseRoute     haActionPhase = "Route"
	haActionPhaseRejoin    haActionPhase = "Rejoin"
)

type haActionExecutor string

const (
	haActionExecutorAdminAPI         haActionExecutor = "AdminAPI"
	haActionExecutorCLIJob           haActionExecutor = "CLIJob"
	haActionExecutorControllerAction haActionExecutor = "ControllerAction"
)

const (
	haAdminBasePath                        = adminsdk.AdminV1Path
	haAdminHAPath                          = adminsdk.HAPath
	haAdminPrimaryStatusPath               = adminsdk.HAPrimaryStatusPath
	haAdminWatchdogProofPath               = adminsdk.HAWatchdogProofPath
	haAdminStandbyStatusPath               = adminsdk.HAStandbyStatusPath
	haAdminCommitCheckPath                 = adminsdk.HACommitCheckPath
	haAdminCommitAppendPath                = adminsdk.HACommitAppendPath
	haAdminReadCheckPath                   = adminsdk.HAReadCheckPath
	haAdminWriteCheckPath                  = adminsdk.HAWriteCheckPath
	haAdminOwnerJobCheckPath               = adminsdk.HAOwnerJobCheckPath
	haAdminReplicationSlotsPath            = adminsdk.HAReplicationSlotsPath
	haAdminReplicationSlotPathPrefix       = adminsdk.HAReplicationSlotPathPrefix
	haAdminReplicationSlotPausePathSuffix  = adminsdk.HAReplicationSlotPausePathSuffix
	haAdminReplicationSlotResumePathSuffix = adminsdk.HAReplicationSlotResumePathSuffix
	haAdminBaseBackupsPath                 = adminsdk.HABaseBackupsPath
	haAdminBaseBackupsFinishPath           = adminsdk.HABaseBackupsFinishPath
	haAdminBaseBackupsCapturePath          = adminsdk.HABaseBackupsCapturePath
	haAdminBaseBackupsActivatePath         = adminsdk.HABaseBackupsActivatePath
	haAdminSeedLifecycleReceiptsPath       = adminsdk.HASeedLifecycleReceiptsPath
	haAdminStandbyBootstrapPath            = adminsdk.HAStandbyBootstrapPath
	haAdminFencePath                       = adminsdk.HAFencePath
	haAdminFenceCurrentPath                = adminsdk.HAFenceCurrentPath
	haAdminPromotionPath                   = adminsdk.HAPromotionPath
	haAdminPromotionAssessPath             = adminsdk.HAPromotionAssessPath
	haAdminPromotionCurrentFencePath       = adminsdk.HAPromotionCurrentFencePath
	haAdminRejoinAssessPath                = adminsdk.HARejoinAssessPath
	haAdminRejoinRewindPath                = adminsdk.HARejoinRewindPath
	haAdminRejoinReseedPath                = adminsdk.HARejoinReseedPath
)

const haFencingLeaseDefaultDurationSeconds int32 = 30

const haSeededSlotActivationReceiptPath = "/var/run/antfly-ha/seeded-slot-activation/seeded-slot-activation.json"

const (
	haFencingLeaseAnnotationClusterID           = "antfly.io/ha-fence-cluster-id"
	haFencingLeaseAnnotationShardID             = "antfly.io/ha-fence-shard-id"
	haFencingLeaseAnnotationTableID             = "antfly.io/ha-fence-table-id"
	haFencingLeaseAnnotationTimelineID          = "antfly.io/ha-fence-timeline-id"
	haFencingLeaseAnnotationEpoch               = "antfly.io/ha-fence-epoch"
	haFencingLeaseAnnotationCurrentPrimaryID    = "antfly.io/ha-fence-current-primary-id"
	haFencingLeaseAnnotationPrimaryLSN          = "antfly.io/ha-fence-primary-lsn"
	haFencingLeaseAnnotationTopologyID          = "antfly.io/ha-fence-topology-id"
	haFencingLeaseAnnotationTransferCommitted   = "antfly.io/ha-fence-transfer-committed"
	haFencingLeaseAnnotationFormerHolder        = "antfly.io/ha-fence-former-holder"
	haFencingLeaseAnnotationTransferOriginUID   = "antfly.io/ha-fence-transfer-origin-uid"
	haFencingLeaseAnnotationCommittedTransition = "antfly.io/ha-fence-committed-transition"
	haFencingLeaseAnnotationBootstrapReceipt    = "antfly.io/ha-fence-bootstrap-receipt"
	haFencingLeaseAnnotationProcessBootID       = "antfly.io/ha-fence-process-boot-id"
)

type haProcessIncarnationGraceKey struct {
	leaseUID       types.UID
	transition     int32
	renewUnixNS    int64
	currentProcess string
	candidate      string
}

func haKubernetesLeaseRenewalEnabled(cluster *antflyv1.AntflyCluster) bool {
	// Every promotion candidate must observe the exact Lease and publish a
	// process-bound watchdog capability proof, but only the declarative primary
	// owns renewal. Keeping standby observation read-only avoids duplicate
	// renewal traffic while allowing an in-place promotion to prove that the
	// candidate was already fail-closed before authority transferred.
	return haRuntimeLeaseWatchdogEnabled(cluster) &&
		cluster.Spec.HighAvailability.Runtime.Role == antflyv1.HARuntimeRolePrimary
}

type haPlannedAction struct {
	Kind                             haActionKind
	DependsOn                        haActionKind
	StandbyName                      string
	SlotName                         string
	TargetLSN                        uint64
	ObservedLSN                      uint64
	RetainedFromLSN                  uint64
	RouteFrom                        string
	RouteTo                          string
	FenceAuthority                   antflyv1.HAFencingAuthority
	FenceHolder                      string
	FenceGeneration                  uint64
	FenceReason                      string
	SeedManifestPath                 string
	SeedContentRoot                  string
	SeedArtifactTargetRoot           string
	SeedArtifactLocation             string
	SeedArtifactGeneration           string
	SeedArtifactRetention            int32
	SeedArtifactCaptureRoot          string
	SeedCaptureReceiptPath           string
	SeedCaptureReceiptSHA256         string
	SeedArtifactProtectedGenerations []string
	TopologyID                       string
	TopologyGeneration               int64
	TopologyNodeID                   string
	SourcePVCName                    string
	SourcePVCUID                     string
	TargetPVCName                    string
	TargetPVCUID                     string
	TargetLocalNodeID                uint64
	TargetReplicaID                  uint64
	Reason                           string
}

type haSyncEvaluation struct {
	Mode          antflyv1.HADurabilityMode
	Selection     antflyv1.HAStandbySelection
	Required      int32
	Satisfied     int32
	Candidates    int32
	FailurePolicy antflyv1.HAFailurePolicy
	Degraded      bool
	Action        string
}

type haFormerPrimaryEvaluation struct {
	Present            bool
	NodeID             string
	Fenced             bool
	RejoinRequired     bool
	RewindPossible     bool
	ReseedRequired     bool
	Diverged           bool
	ParentTimelineID   uint64
	NewTimelineID      uint64
	ObservedTimelineID uint64
	SwitchLSN          uint64
	ObservedLSN        uint64
	FenceAuthority     antflyv1.HAFencingAuthority
	FenceHolder        string
	FenceGeneration    uint64
	Action             string
	Reason             string
}

type haPrimaryRouteEvaluation struct {
	ServiceName     string
	CurrentTarget   string
	DesiredTarget   string
	FenceAuthority  antflyv1.HAFencingAuthority
	FenceGeneration uint64
	Stale           bool
	Action          string
	Reason          string
}

type haPlan struct {
	Actions                   []haPlannedAction
	AutomaticPromotionAllowed bool
	DesiredStandbyCount       int32
	HealthyStandbyCount       int32
	UnhealthyStandbyCount     int32
	LaggingStandbyCount       int32
	ReadSafeStandbyCount      int32
	ReseedRequiredCount       int32
	FencingReady              bool
	PromotionRouteReady       bool
	PromotionBoundaryReady    bool
	PromotionAlreadyRecorded  bool
	PromotionStandbyName      string
	PrimaryAdminUnavailable   bool
	SyncPolicyDegraded        bool
	SyncPolicy                haSyncEvaluation
	FormerPrimary             haFormerPrimaryEvaluation
	PrimaryRoute              haPrimaryRouteEvaluation
}

type haOperatorPlanTable struct {
	AutomaticPromotionAllowed bool
	DesiredStandbyCount       int32
	HealthyStandbyCount       int32
	UnhealthyStandbyCount     int32
	LaggingStandbyCount       int32
	ReseedRequiredCount       int32
	Actions                   []antflyv1.HAPlannedActionStatus
}

func (r *AntflyClusterReconciler) updateHAStatusAndConditions(cluster *antflyv1.AntflyCluster) {
	plan := planHA(cluster)
	applyHAPlanStatus(cluster, plan)
	setHAConditions(cluster, plan)
}

func (r *AntflyClusterReconciler) reconcileHAFencingLease(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled ||
		ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled ||
		!haAutomaticFailoverExecutionEnabled(ha) ||
		ha.AutomaticFailover.FencingAuthority != antflyv1.HAFencingAuthorityKubernetesLease {
		return nil
	}
	if cluster.Status.HAStatus == nil {
		return nil
	}
	if ha.Runtime == nil {
		return nil
	}
	localNodeID := strings.TrimSpace(ha.Runtime.NodeID)
	if localNodeID == "" {
		return nil
	}
	pendingWatchdogAuthority := cluster.Status.HAStatus.PrimaryWatchdogProof != nil &&
		cluster.Status.HAStatus.PrimaryWatchdogProof.Active &&
		!cluster.Status.HAStatus.PrimaryWatchdogProof.AuthorityGranted
	holder := haKubernetesLeaseFenceCandidate(ha, cluster.Status.HAStatus)
	if holder == "" {
		if !cluster.Status.HAStatus.PrimaryAdminReachable &&
			strings.Contains(cluster.Status.HAStatus.PrimaryAdminLastError, "HA Lease watchdog") &&
			!pendingWatchdogAuthority {
			// Never renew authority for an authenticated runtime that reports
			// itself inactive (or cannot prove the capability). Let the old
			// generation remain fenced while failover debounce selects a candidate.
			return nil
		}
		identity := haReplicationIdentity(ha)
		if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) == "" {
			return nil
		}
		// A fail-closed runtime must prove its normal write authority before
		// serving. Keep the healthy primary as holder until the atomic transfer
		// to a promotion candidate increments LeaseTransitions.
		holder = strings.TrimSpace(identity.CurrentPrimaryID)
	}
	scope, ok := haFencingLeaseReconcileScope(cluster, holder)
	bootstrapUnknownBoundary := false
	if !ok {
		identity := haReplicationIdentity(ha)
		if identity == nil || cluster.Status.HAStatus.PrimaryLSN != 0 || holder != localNodeID ||
			holder != strings.TrimSpace(identity.CurrentPrimaryID) {
			return nil
		}
		// LSN zero is an unknown boundary, not a safe promotion boundary. It is
		// accepted only to create the configured primary's first authority Lease.
		// If the Lease already exists, its positive persisted boundary must be
		// recovered below and can never be overwritten with zero.
		scope = haFencingLeaseScopeForIdentity(identity, 0)
		bootstrapUnknownBoundary = true
	}

	now := metav1.NewMicroTime(r.haNow())
	lease := &coordinationv1.Lease{}
	err := r.haBoundaryReader().Get(ctx, types.NamespacedName{
		Name:      haFencingLeaseName(cluster),
		Namespace: cluster.Namespace,
	}, lease)
	if apierrors.IsNotFound(err) {
		// A missing shared Lease can only be initialized by the runtime that is
		// itself the configured current writer. Creating it directly for a
		// candidate would skip the compare-and-swap handoff from the old holder.
		identity := haReplicationIdentity(ha)
		if identity == nil || holder != localNodeID || holder != strings.TrimSpace(identity.CurrentPrimaryID) {
			return nil
		}
		transitions := int32(1)
		durationSeconds := haFencingLeaseDefaultDurationSeconds
		annotations := scope.annotations()
		annotations[haFencingLeaseAnnotationTopologyID] = haFencingLeaseTopologyID(cluster)
		if pendingWatchdogAuthority {
			annotations[haFencingLeaseAnnotationProcessBootID] = strings.TrimSpace(cluster.Status.HAStatus.PrimaryWatchdogProof.ProcessBootID)
		}
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:        haFencingLeaseName(cluster),
				Namespace:   cluster.Namespace,
				Labels:      haFencingLeaseLabels(cluster),
				Annotations: annotations,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: &durationSeconds,
				AcquireTime:          &now,
				RenewTime:            &now,
				LeaseTransitions:     &transitions,
			},
		}
		// The shared Lease outlives any one primary AntflyCluster CR. It must not
		// carry a controller ownerReference or garbage collection could erase the
		// fencing authority during topology handoff.
		if err := r.Create(ctx, lease); err != nil {
			return err
		}
		if bootstrapUnknownBoundary || pendingWatchdogAuthority {
			haRecordPendingFencingLeaseStatus(cluster, holder, transitions)
		} else {
			haRecordRenewedFencingLeaseStatus(cluster, holder, transitions)
		}
		return nil
	}
	if err != nil {
		return err
	}

	currentHolder := ""
	if lease.Spec.HolderIdentity != nil {
		currentHolder = *lease.Spec.HolderIdentity
	}
	transitions := int32(0)
	if lease.Spec.LeaseTransitions != nil {
		transitions = *lease.Spec.LeaseTransitions
	}
	if currentHolder == "" || transitions <= 0 ||
		lease.Annotations[haFencingLeaseAnnotationTopologyID] != haFencingLeaseTopologyID(cluster) {
		return nil
	}
	if bootstrapUnknownBoundary {
		persistedBoundary, parseErr := strconv.ParseUint(
			strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationPrimaryLSN]), 10, 64,
		)
		if parseErr != nil {
			return nil
		}
		if persistedBoundary > 0 {
			scope.primaryLSN = persistedBoundary
		}
		if !haLeaseFenceScopeMatches(lease, scope) &&
			(!pendingWatchdogAuthority || !haCommittedTransferSuccessorScopeMatches(cluster, lease, scope, currentHolder, transitions)) {
			return nil
		}
	}
	// Capture an exact parent->child handoff before the ordinary owner-renewal
	// branch can atomically close its transfer receipt. A successor runtime may
	// already report full watchdog authority on its first controller observation,
	// skipping the pending-authority branch below; its standby-era status LSN is
	// still not allowed to replace the committed promotion boundary.
	committedSuccessorBoundary, committedSuccessor := haCommittedTransferSuccessorBoundary(
		cluster, lease, scope, currentHolder, transitions,
	)

	// Pending authority is an explicit, terminal branch before holder changes
	// or ordinary owner renewal. It can advance only the exact configured
	// current owner's unchanged Lease and records a durable process/generation
	// receipt so a fresh observation timestamp cannot reopen the exception.
	if pendingWatchdogAuthority {
		identity := haReplicationIdentity(ha)
		proof := cluster.Status.HAStatus.PrimaryWatchdogProof
		currentReceipt := haFencingLeaseBootstrapReceipt(currentHolder, transitions, proof.ProcessBootID)
		// A replacement successor process can observe a lower cached runtime LSN
		// than the already-authoritative child Lease. Normalize only an unchanged
		// identity before the exact comparison so process rebinding preserves that
		// durable boundary instead of deadlocking until the Lease expires.
		scope = haFencingLeaseScopeWithMonotonicBoundary(lease, scope)
		if identity == nil || holder != currentHolder || currentHolder != localNodeID ||
			currentHolder != strings.TrimSpace(identity.CurrentPrimaryID) ||
			lease.Spec.RenewTime == nil || (!haLeaseFenceScopeMatches(lease, scope) && !committedSuccessor) ||
			strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt]) == currentReceipt {
			return nil
		}
		ready, err := r.haCurrentPrimaryRuntimeWatchdogReady(
			ctx, cluster, currentHolder, uint64(transitions), lease.Spec.RenewTime.Time, false,
		)
		if err != nil || !ready {
			return err
		}
		currentProcess := strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationProcessBootID])
		if currentProcess == "" {
			currentProcess = strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt])
		}
		unboundInitialBootstrap := currentProcess == "" && bootstrapUnknownBoundary && scope.primaryLSN == 0
		if !unboundInitialBootstrap && currentProcess != proof.ProcessBootID && currentProcess != currentReceipt &&
			!r.haProcessIncarnationBarrierElapsed(cluster, lease, currentProcess, proof.ProcessBootID) {
			return nil
		}
		lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt] = currentReceipt
		lease.Annotations[haFencingLeaseAnnotationProcessBootID] = proof.ProcessBootID
		if committedSuccessor {
			// The holder transfer is committed on the parent timeline before Colony
			// publishes the promoted CR's successor identity. Advance the durable
			// Lease scope and bind the already fail-closed successor process in one
			// write; publishing either half alone would let the runtime and Lease
			// controller wait indefinitely for each other.
			scope.primaryLSN = committedSuccessorBoundary
			for key, value := range scope.annotations() {
				lease.Annotations[key] = value
			}
		}
		lease.Spec.RenewTime = &now
		if err := r.Update(ctx, lease); err != nil {
			return err
		}
		haRecordPendingFencingLeaseStatus(cluster, currentHolder, transitions)
		return nil
	}

	clearBootstrapReceipt := false
	bootstrapReceipt := strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt])
	if bootstrapReceipt != "" || (bootstrapUnknownBoundary && scope.primaryLSN == 0) {
		if lease.Spec.RenewTime == nil || !haLeaseFenceScopeMatches(lease, scope) {
			return nil
		}
		ready, err := r.haCurrentPrimaryRuntimeWatchdogReady(
			ctx, cluster, currentHolder, uint64(transitions), lease.Spec.RenewTime.Time, true,
		)
		if err != nil || !ready {
			return err
		}
		clearBootstrapReceipt = bootstrapReceipt != ""
	}
	preserveTransferredScope := false
	if lease.Annotations[haFencingLeaseAnnotationTransferCommitted] == "true" && currentHolder != "" &&
		haKubernetesLeaseFenceCandidate(ha, cluster.Status.HAStatus) == "" && holder != currentHolder {
		// Status loss or controller restart must never hand authority back to the
		// former writer after a committed transfer recorded on the Lease itself.
		holder = currentHolder
		preserveTransferredScope = true
	}
	holderChanged := currentHolder != holder
	if holderChanged {
		// Only the exact current holder controller may commit the next holder.
		// The candidate must independently prove that its live runtime process
		// pre-watched this exact Lease generation without write authority.
		if localNodeID != currentHolder || cluster.UID == "" {
			return nil
		}
		ready, err := r.haCandidateRuntimeWatchdogReady(ctx, cluster, holder, currentHolder, uint64(transitions))
		if err != nil || !ready {
			return err
		}
		transitions++
		lease.Spec.LeaseTransitions = &transitions
		lease.Spec.AcquireTime = &now
		if lease.Annotations == nil {
			lease.Annotations = map[string]string{}
		}
		lease.Annotations[haFencingLeaseAnnotationTransferCommitted] = "true"
		lease.Annotations[haFencingLeaseAnnotationFormerHolder] = currentHolder
		lease.Annotations[haFencingLeaseAnnotationTransferOriginUID] = string(cluster.UID)
		lease.Annotations[haFencingLeaseAnnotationCommittedTransition] = strconv.FormatInt(int64(transitions), 10)
	} else if transitions == 0 {
		transitions = 1
		lease.Spec.LeaseTransitions = &transitions
		if lease.Spec.AcquireTime == nil {
			lease.Spec.AcquireTime = &now
		}
	} else {
		ownerRenewal := localNodeID == currentHolder
		handoffRenewal := lease.Annotations[haFencingLeaseAnnotationTransferCommitted] == "true" &&
			lease.Annotations[haFencingLeaseAnnotationFormerHolder] == localNodeID &&
			lease.Annotations[haFencingLeaseAnnotationTransferOriginUID] == string(cluster.UID) &&
			lease.Annotations[haFencingLeaseAnnotationCommittedTransition] == strconv.FormatInt(int64(transitions), 10)
		if !ownerRenewal && !handoffRenewal {
			return nil
		}
		if ownerRenewal {
			// Taking over renewal closes the former controller's narrow handoff
			// bridge. From this point only the new holder can renew or transfer.
			delete(lease.Annotations, haFencingLeaseAnnotationTransferCommitted)
			delete(lease.Annotations, haFencingLeaseAnnotationFormerHolder)
			delete(lease.Annotations, haFencingLeaseAnnotationTransferOriginUID)
			delete(lease.Annotations, haFencingLeaseAnnotationCommittedTransition)
		}
	}
	durationSeconds := haFencingLeaseDefaultDurationSeconds
	if lease.Spec.LeaseDurationSeconds != nil && *lease.Spec.LeaseDurationSeconds > 0 {
		durationSeconds = *lease.Spec.LeaseDurationSeconds
	}
	lease.Spec.HolderIdentity = &holder
	lease.Spec.LeaseDurationSeconds = &durationSeconds
	lease.Spec.RenewTime = &now
	if lease.Labels == nil {
		lease.Labels = map[string]string{}
	}
	for key, value := range haFencingLeaseLabels(cluster) {
		lease.Labels[key] = value
	}
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	// Runtime status can briefly lag after a role/topology transition. Once the
	// Lease carries a positive boundary for an unchanged fencing identity, an
	// ordinary renewal may advance that lower bound but must never regress it.
	// Successor bootstrap still performs its exact boundary comparison above;
	// this normalization is deliberately limited to the final renewal write.
	if committedSuccessor && committedSuccessorBoundary > scope.primaryLSN {
		scope.primaryLSN = committedSuccessorBoundary
	}
	scope = haFencingLeaseScopeWithMonotonicBoundary(lease, scope)
	if !preserveTransferredScope {
		for key, value := range scope.annotations() {
			lease.Annotations[key] = value
		}
	}
	if clearBootstrapReceipt {
		delete(lease.Annotations, haFencingLeaseAnnotationBootstrapReceipt)
	}
	if proof := cluster.Status.HAStatus.PrimaryWatchdogProof; proof != nil && proof.AuthorityGranted &&
		strings.TrimSpace(proof.ProcessBootID) != "" && holder == localNodeID {
		lease.Annotations[haFencingLeaseAnnotationProcessBootID] = strings.TrimSpace(proof.ProcessBootID)
	}
	lease.Annotations[haFencingLeaseAnnotationTopologyID] = haFencingLeaseTopologyID(cluster)
	if err := r.Update(ctx, lease); err != nil {
		return err
	}
	haRecordRenewedFencingLeaseStatus(cluster, holder, transitions)
	return nil
}

// renewCurrentHAFencingLease advances only an unchanged current holder. It is
// intentionally narrower than reconcileHAFencingLease: topology handoff,
// bootstrap, and scope changes remain owned by the main cluster reconciler.
// A dedicated controller calls this path so renewal scheduling is not queued
// behind seed-capture reconciliation work. The runtime proof endpoint must
// still remain responsive independently of the capture critical section.
func (r *AntflyClusterReconciler) renewCurrentHAFencingLease(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if cluster == nil || cluster.Spec.HighAvailability == nil || cluster.Status.HAStatus == nil {
		return nil
	}
	ha := cluster.Spec.HighAvailability
	if ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled || ha.Runtime == nil ||
		ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled ||
		!haAutomaticFailoverExecutionEnabled(ha) ||
		ha.AutomaticFailover.FencingAuthority != antflyv1.HAFencingAuthorityKubernetesLease {
		return nil
	}
	localNodeID := strings.TrimSpace(ha.Runtime.NodeID)
	if localNodeID == "" {
		return nil
	}

	lease := &coordinationv1.Lease{}
	if err := r.haBoundaryReader().Get(ctx, types.NamespacedName{
		Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace,
	}, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) != localNodeID ||
		lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions <= 0 || lease.Spec.RenewTime == nil ||
		lease.Annotations[haFencingLeaseAnnotationTopologyID] != haFencingLeaseTopologyID(cluster) {
		return nil
	}
	identity := haReplicationIdentity(ha)
	if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) != localNodeID {
		return nil
	}
	proof := cluster.Status.HAStatus.PrimaryWatchdogProof
	if proof == nil || strings.TrimSpace(proof.ProcessBootID) == "" ||
		strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationProcessBootID]) != strings.TrimSpace(proof.ProcessBootID) {
		return nil
	}
	identityScope := haFencingLeaseScopeForIdentity(identity, 0)
	for key, value := range identityScope.annotations() {
		if key == haFencingLeaseAnnotationPrimaryLSN {
			continue
		}
		if lease.Annotations[key] != value {
			return nil
		}
	}

	ready, err := r.haCurrentPrimaryRuntimeWatchdogReady(
		ctx,
		cluster,
		localNodeID,
		uint64(*lease.Spec.LeaseTransitions),
		lease.Spec.RenewTime.Time,
		true,
	)
	if err != nil || !ready {
		return err
	}
	now := metav1.NewMicroTime(r.haNow())
	lease.Spec.RenewTime = &now
	return r.Update(ctx, lease)
}

func (r *AntflyClusterReconciler) haProcessIncarnationBarrierElapsed(
	cluster *antflyv1.AntflyCluster,
	lease *coordinationv1.Lease,
	currentProcess string,
	candidate string,
) bool {
	if r == nil || cluster == nil || lease == nil || lease.Spec.RenewTime == nil ||
		cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil ||
		cluster.Spec.HighAvailability.Runtime.FencingLease == nil {
		return false
	}
	grace := cluster.Spec.HighAvailability.Runtime.FencingLease.WatchdogGraceSeconds
	if grace <= 0 {
		return false
	}
	transition := int32(0)
	if lease.Spec.LeaseTransitions != nil {
		transition = *lease.Spec.LeaseTransitions
	}
	key := haProcessIncarnationGraceKey{
		leaseUID: lease.UID, transition: transition, renewUnixNS: lease.Spec.RenewTime.UnixNano(),
		currentProcess: currentProcess, candidate: candidate,
	}
	now := r.haMonotonicNow()
	value, loaded := r.haProcessGraceStarts.LoadOrStore(key, now)
	if !loaded {
		return false
	}
	started, ok := value.(time.Time)
	if !ok || now.Before(started) {
		r.haProcessGraceStarts.Store(key, now)
		return false
	}
	return now.Sub(started) >= time.Duration(grace)*time.Second
}

func haFencingLeaseBootstrapReceipt(holder string, transition int32, processBootID string) string {
	return fmt.Sprintf("%s:%d:%s", strings.TrimSpace(holder), transition, strings.TrimSpace(processBootID))
}

func (r *AntflyClusterReconciler) haCurrentPrimaryRuntimeWatchdogReady(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	holder string,
	transition uint64,
	leaseRenewTime time.Time,
	requireAuthority bool,
) (bool, error) {
	if cluster == nil || cluster.Status.HAStatus == nil || cluster.Status.HAStatus.PrimaryWatchdogProof == nil ||
		cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Runtime == nil ||
		cluster.Spec.HighAvailability.Runtime.FencingLease == nil {
		return false, nil
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	proof := cluster.Status.HAStatus.PrimaryWatchdogProof
	lease := cluster.Spec.HighAvailability.Runtime.FencingLease
	expectedMaxFenceLatencyMS := int32(10_000)
	if lease.WatchdogGraceSeconds > 0 {
		expectedMaxFenceLatencyMS = lease.WatchdogGraceSeconds * 1000
	}
	now := r.haNow()
	authorityReady := !proof.AuthorityGranted && proof.AuthorityRemainingMS == 0
	if requireAuthority {
		authorityReady = proof.AuthorityGranted && proof.AuthorityRemainingMS > 0 &&
			now.Sub(proof.ObservedAt.Time) < time.Duration(proof.AuthorityRemainingMS)*time.Millisecond
	}
	if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) != holder ||
		strings.TrimSpace(cluster.Spec.HighAvailability.Runtime.NodeID) != holder ||
		proof.ObservedAt.IsZero() || !proof.ObservedAt.After(leaseRenewTime) || now.Before(proof.ObservedAt.Time) ||
		now.Sub(proof.ObservedAt.Time) >= time.Duration(proof.MaxFenceLatencyMS)*time.Millisecond ||
		proof.CapabilityVersion != 1 || !proof.Active || !authorityReady ||
		proof.MaxFenceLatencyMS != expectedMaxFenceLatencyMS ||
		proof.LocalNodeID != holder || proof.ObservedHolderNodeID != holder ||
		proof.LeaseName != strings.TrimSpace(lease.Name) || proof.LeaseNamespace != cluster.Namespace ||
		proof.TopologyID != strings.TrimSpace(lease.TopologyID) ||
		proof.ObservedLeaseTransitions <= 0 || uint64(proof.ObservedLeaseTransitions) != transition ||
		proof.PodUID == "" || !haWatchdogProcessBootIDValid(proof.ProcessBootID) {
		return false, nil
	}

	pods := &corev1.PodList{}
	if err := r.haBoundaryReader().List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return false, err
	}
	matches := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) != proof.PodUID || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for j := range pod.Status.ContainerStatuses {
			container := &pod.Status.ContainerStatuses[j]
			if container.Name == "antfly" && container.State.Running != nil &&
				!proof.ObservedAt.Before(&container.State.Running.StartedAt) {
				matches++
				break
			}
		}
	}
	return matches == 1, nil
}

func (r *AntflyClusterReconciler) haCandidateRuntimeWatchdogReady(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	candidate string,
	observedHolder string,
	transition uint64,
) (bool, error) {
	if cluster == nil || cluster.Status.HAStatus == nil || cluster.Spec.HighAvailability == nil ||
		cluster.Spec.HighAvailability.Runtime == nil || cluster.Spec.HighAvailability.Runtime.FencingLease == nil {
		return false, nil
	}
	var proof *antflyv1.HAWatchdogProofStatus
	for i := range cluster.Status.HAStatus.Standbys {
		standby := &cluster.Status.HAStatus.Standbys[i]
		if standby.Name == candidate || standby.SlotName == candidate {
			proof = standby.WatchdogProof
			break
		}
	}
	if proof == nil {
		return false, nil
	}
	lease := cluster.Spec.HighAvailability.Runtime.FencingLease
	expectedMaxFenceLatencyMS := int32(10_000)
	if lease.WatchdogGraceSeconds > 0 {
		expectedMaxFenceLatencyMS = lease.WatchdogGraceSeconds * 1000
	}
	now := r.haNow()
	if proof.ObservedAt.IsZero() || now.Before(proof.ObservedAt.Time) ||
		now.Sub(proof.ObservedAt.Time) >= time.Duration(proof.MaxFenceLatencyMS)*time.Millisecond ||
		proof.CapabilityVersion != 1 || !proof.Active || proof.AuthorityGranted || proof.AuthorityRemainingMS != 0 ||
		proof.MaxFenceLatencyMS != expectedMaxFenceLatencyMS ||
		proof.LocalNodeID != candidate || proof.ObservedHolderNodeID != observedHolder ||
		proof.LeaseName != strings.TrimSpace(lease.Name) || proof.LeaseNamespace != cluster.Namespace ||
		proof.TopologyID != strings.TrimSpace(lease.TopologyID) ||
		proof.ObservedLeaseTransitions <= 0 || uint64(proof.ObservedLeaseTransitions) != transition ||
		proof.PodUID == "" || !haWatchdogProcessBootIDValid(proof.ProcessBootID) {
		return false, nil
	}

	pods := &corev1.PodList{}
	if err := r.haBoundaryReader().List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return false, err
	}
	matches := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) != proof.PodUID || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for j := range pod.Status.ContainerStatuses {
			container := &pod.Status.ContainerStatuses[j]
			if container.Name == "antfly" && container.State.Running != nil &&
				!proof.ObservedAt.Before(&container.State.Running.StartedAt) {
				matches++
				break
			}
		}
	}
	return matches == 1, nil
}

func haRecordRenewedFencingLeaseStatus(cluster *antflyv1.AntflyCluster, holder string, generation int32) {
	if cluster == nil || cluster.Spec.HighAvailability == nil || generation <= 0 {
		return
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: cluster.Spec.HighAvailability.Mode}
	}
	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
		Ready:      true,
		Holder:     holder,
		Generation: uint64(generation),
		Reason:     "LeaseHeld",
	}
}

func haRecordPendingFencingLeaseStatus(cluster *antflyv1.AntflyCluster, holder string, generation int32) {
	if cluster == nil || cluster.Spec.HighAvailability == nil || generation <= 0 {
		return
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: cluster.Spec.HighAvailability.Mode}
	}
	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
		Ready:      false,
		Holder:     holder,
		Generation: uint64(generation),
		Reason:     "LeaseBootstrapPendingAuthority",
	}
}

func (r *AntflyClusterReconciler) observeHAFencingStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled ||
		ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled {
		return nil
	}
	if ha.AutomaticFailover.FencingAuthority != antflyv1.HAFencingAuthorityKubernetesLease {
		return nil
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: ha.Mode}
	}

	lease := &coordinationv1.Lease{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      haFencingLeaseName(cluster),
		Namespace: cluster.Namespace,
	}, lease)
	if apierrors.IsNotFound(err) {
		cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
			Authority: antflyv1.HAFencingAuthorityKubernetesLease,
			Reason:    "LeaseMissing",
		}
		return nil
	}
	if err != nil {
		return err
	}

	generation := haLeaseFenceGeneration(lease)
	holder := ""
	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}
	ready, reason := haLeaseFenceReady(lease, generation, time.Now())
	if strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt]) != "" {
		ready = false
		reason = "LeaseBootstrapPendingAuthority"
	}
	if ready {
		scope, ok := haCurrentFencingLeaseScope(cluster)
		if !ok {
			ready = false
			reason = "LeaseScopeMissing"
		} else if !haLeaseFenceScopeMatches(lease, scope) {
			// After the former-primary fence call has started, the Lease is
			// intentionally bound to the election-time lower boundary. The old
			// writer may advance while its admin transport recovers; that does not
			// invalidate the committed ownership transfer. Only accept the older
			// scope when every identity field and the exact recorded action
			// boundary still match.
			committed, committedOK := haCommittedFencingLeaseScope(cluster, holder, generation)
			lowerBound, lowerBoundOK := haCommittedFencingLeaseLowerBoundScope(cluster, holder, generation)
			if (!committedOK || !haLeaseFenceScopeMatches(lease, committed)) &&
				(!lowerBoundOK || !haLeaseFenceScopeMatches(lease, lowerBound)) {
				ready = false
				reason = "LeaseScopeMismatch"
			}
		}
	}
	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
		Ready:      ready,
		Holder:     holder,
		Generation: generation,
		Reason:     reason,
	}
	return nil
}

func (r *AntflyClusterReconciler) observeHAFormerPrimaryFenceStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return
	}
	ha := cluster.Spec.HighAvailability
	promotion := haPromotionReceipt(cluster.Status.HAStatus)
	if ha == nil || promotion == nil || strings.TrimSpace(promotion.OldPrimaryID) == "" {
		return
	}
	if haSucceededFormerPrimaryIsolation(cluster, cluster.Status.HAStatus, promotion) != nil {
		haSetFormerPrimaryFenceObserved(cluster.Status.HAStatus, promotion, true)
		return
	}
	adminURL := haFormerPrimaryAdminURL(ha, haPlannedAction{
		StandbyName: promotion.OldPrimaryID,
		RouteFrom:   promotion.OldPrimaryID,
	})
	if strings.TrimSpace(adminURL) == "" {
		haSetFormerPrimaryFenceObserved(cluster.Status.HAStatus, promotion, false)
		return
	}
	adminClient, err := r.haAdminSDKClient(cluster, adminURL)
	if err != nil {
		haSetFormerPrimaryFenceObserved(cluster.Status.HAStatus, promotion, false)
		return
	}
	current, err := adminClient.CurrentFence(ctx)
	if err != nil {
		haSetFormerPrimaryFenceObserved(cluster.Status.HAStatus, promotion, false)
		return
	}
	haSetFormerPrimaryFenceObserved(
		cluster.Status.HAStatus,
		promotion,
		haCurrentFenceMatchesPromotion(current, promotion),
	)
}

func haSetFormerPrimaryFenceObserved(status *antflyv1.HAStatus, promotion *antflyv1.HAPromotionStatus, observed bool) {
	if status == nil || promotion == nil {
		return
	}
	if status.FormerPrimary == nil {
		status.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{NodeID: promotion.OldPrimaryID}
	}
	former := status.FormerPrimary
	former.NodeID = promotion.OldPrimaryID
	former.Fenced = observed
	former.FenceAuthority = promotion.FenceAuthority
	former.FenceHolder = promotion.PromotedStandbyID
	former.FenceGeneration = promotion.FenceGeneration
	if !observed {
		former.RejoinRequired = true
		former.Action = string(haActionDemoteFormerPrimary)
		former.Reason = "FormerPrimaryFenceNotObserved"
	}
}

func haCurrentFenceMatchesPromotion(current *adminsdk.HACurrentFenceResponse, promotion *antflyv1.HAPromotionStatus) bool {
	if current == nil || promotion == nil || !current.Held {
		return false
	}
	receipt := current.Receipt
	return receipt.Identity.ClusterId == promotion.ClusterID &&
		receipt.Identity.ShardId == promotion.ShardID &&
		receipt.Identity.TableId == promotion.TableID &&
		strings.TrimSpace(string(receipt.OldPrimaryId)) == strings.TrimSpace(promotion.OldPrimaryID) &&
		strings.TrimSpace(string(receipt.PromotedNodeId)) == strings.TrimSpace(promotion.PromotedStandbyID) &&
		receipt.ParentTimelineId == promotion.ParentTimelineID &&
		receipt.ParentEpoch == promotion.ParentEpoch &&
		receipt.NewTimelineId == promotion.NewTimelineID &&
		receipt.NewEpoch == promotion.NewEpoch &&
		receipt.RequiredLsn == haPromotionRequiredLSN(promotion) &&
		receipt.ObservedLsn >= haPromotionRequiredLSN(promotion) &&
		receipt.Generation == promotion.FenceGeneration &&
		strings.TrimSpace(receipt.Token) == strings.TrimSpace(promotion.FenceToken)
}

func haFencingLeaseName(cluster *antflyv1.AntflyCluster) string {
	if haRuntimeLeaseWatchdogEnabled(cluster) {
		return strings.TrimSpace(cluster.Spec.HighAvailability.Runtime.FencingLease.Name)
	}
	return cluster.Name + "-ha-fence"
}

func haFencingLeaseTopologyID(cluster *antflyv1.AntflyCluster) string {
	if !haRuntimeLeaseWatchdogEnabled(cluster) {
		return ""
	}
	return strings.TrimSpace(cluster.Spec.HighAvailability.Runtime.FencingLease.TopologyID)
}

func haFencingLeaseLabels(cluster *antflyv1.AntflyCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "antfly",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "antfly-operator",
		"antfly.io/ha-fence":           "kubernetes-lease",
	}
}

type haFencingLeaseScope struct {
	clusterID        uint64
	shardID          uint64
	tableID          uint64
	timelineID       uint64
	epoch            uint64
	currentPrimaryID string
	primaryLSN       uint64
}

func haCurrentFencingLeaseScope(cluster *antflyv1.AntflyCluster) (haFencingLeaseScope, bool) {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return haFencingLeaseScope{}, false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || cluster.Status.HAStatus.PrimaryLSN == 0 {
		return haFencingLeaseScope{}, false
	}
	return haFencingLeaseScope{
		clusterID:        identity.ClusterID,
		shardID:          identity.ShardID,
		tableID:          identity.TableID,
		timelineID:       identity.TimelineID,
		epoch:            identity.Epoch,
		currentPrimaryID: identity.CurrentPrimaryID,
		primaryLSN:       cluster.Status.HAStatus.PrimaryLSN,
	}, true
}

func haFencingLeaseReconcileScope(cluster *antflyv1.AntflyCluster, holder string) (haFencingLeaseScope, bool) {
	if cluster != nil && cluster.Status.HAStatus != nil {
		if scope, ok := haCommittedFencingLeaseScope(cluster, holder, cluster.Status.HAStatus.Fencing.Generation); ok {
			return scope, true
		}
	}
	return haCurrentFencingLeaseScope(cluster)
}

func haCommittedFencingLeaseScope(cluster *antflyv1.AntflyCluster, holder string, generation uint64) (haFencingLeaseScope, bool) {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return haFencingLeaseScope{}, false
	}
	ha := cluster.Spec.HighAvailability
	action := haCommittedFormerPrimaryBoundaryAction(ha, cluster.Status.HAStatus, holder, generation)
	identity := haReplicationIdentity(ha)
	if action == nil || identity == nil {
		return haFencingLeaseScope{}, false
	}
	boundary := haAutomaticFailoverPromotionBoundary(cluster.Status.HAStatus, holder, action.FenceGeneration)
	if boundary == 0 {
		return haFencingLeaseScope{}, false
	}
	return haFencingLeaseScopeForIdentity(identity, boundary), true
}

func haCommittedFencingLeaseLowerBoundScope(cluster *antflyv1.AntflyCluster, holder string, generation uint64) (haFencingLeaseScope, bool) {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return haFencingLeaseScope{}, false
	}
	ha := cluster.Spec.HighAvailability
	action := haCommittedFormerPrimaryBoundaryAction(ha, cluster.Status.HAStatus, holder, generation)
	identity := haReplicationIdentity(ha)
	if action == nil || identity == nil || action.TargetLSN == 0 {
		return haFencingLeaseScope{}, false
	}
	// Physical isolation is initiated against the election-time Lease scope,
	// then freezes the old writer at its actual durable tail. Preserve that
	// original scope as the temporary lower bound so observation does not revoke
	// an in-flight handoff before the former holder can publish the stronger
	// frozen boundary. Promotion is separately gated on the exact frozen scope.
	if haActionKind(action.Kind) == haActionIsolateFormerPrimary {
		if scope, ok := haPhysicalIsolationReceiptScope(action.PhysicalIsolationReceipt); ok {
			return scope, true
		}
	}
	return haFencingLeaseScopeForIdentity(identity, action.TargetLSN), true
}

func haCommittedFormerPrimaryBoundaryAction(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus, holder string, generation uint64) *antflyv1.HAPlannedActionStatus {
	// A physical-isolation receipt supersedes the election-time action because
	// its target is the only boundary proven after the old process stopped.
	if action := haCommittedFormerPrimaryIsolationAction(ha, status, holder, generation); action != nil {
		return action
	}
	return haCommittedFormerPrimaryFenceAction(ha, status, holder, generation)
}

func haFencingLeaseScopeForIdentity(identity *antflyv1.HAReplicationIdentitySpec, boundary uint64) haFencingLeaseScope {
	return haFencingLeaseScope{
		clusterID:        identity.ClusterID,
		shardID:          identity.ShardID,
		tableID:          identity.TableID,
		timelineID:       identity.TimelineID,
		epoch:            identity.Epoch,
		currentPrimaryID: identity.CurrentPrimaryID,
		primaryLSN:       boundary,
	}
}

func (scope haFencingLeaseScope) annotations() map[string]string {
	return map[string]string{
		haFencingLeaseAnnotationClusterID:        strconv.FormatUint(scope.clusterID, 10),
		haFencingLeaseAnnotationShardID:          strconv.FormatUint(scope.shardID, 10),
		haFencingLeaseAnnotationTableID:          strconv.FormatUint(scope.tableID, 10),
		haFencingLeaseAnnotationTimelineID:       strconv.FormatUint(scope.timelineID, 10),
		haFencingLeaseAnnotationEpoch:            strconv.FormatUint(scope.epoch, 10),
		haFencingLeaseAnnotationCurrentPrimaryID: scope.currentPrimaryID,
		haFencingLeaseAnnotationPrimaryLSN:       strconv.FormatUint(scope.primaryLSN, 10),
	}
}

func haFencingLeaseScopeWithMonotonicBoundary(lease *coordinationv1.Lease, scope haFencingLeaseScope) haFencingLeaseScope {
	if lease == nil || lease.Annotations == nil {
		return scope
	}
	identityAnnotations := scope.annotations()
	for _, key := range []string{
		haFencingLeaseAnnotationClusterID,
		haFencingLeaseAnnotationShardID,
		haFencingLeaseAnnotationTableID,
		haFencingLeaseAnnotationTimelineID,
		haFencingLeaseAnnotationEpoch,
		haFencingLeaseAnnotationCurrentPrimaryID,
	} {
		if lease.Annotations[key] != identityAnnotations[key] {
			return scope
		}
	}
	persisted, err := strconv.ParseUint(strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationPrimaryLSN]), 10, 64)
	if err == nil && persisted > scope.primaryLSN {
		scope.primaryLSN = persisted
	}
	return scope
}

func haLeaseFenceScopeMatches(lease *coordinationv1.Lease, scope haFencingLeaseScope) bool {
	if lease == nil {
		return false
	}
	annotations := lease.Annotations
	if annotations == nil {
		return false
	}
	for key, value := range scope.annotations() {
		if annotations[key] != value {
			return false
		}
	}
	return true
}

// haCommittedTransferSuccessorScopeMatches recognizes the one safe scope
// mismatch that exists between a committed Lease handoff and the promoted
// runtime's first authoritative observation. The transfer is committed on the
// parent identity; Colony then publishes the exact next timeline/epoch on a
// different AntflyCluster CR. Until the new process is bound, that runtime must
// remain fail-closed and can retain a lower standby-era LSN. The committed
// Lease boundary is authoritative; the successor may adopt it but may neither
// replace it with the lower cached value nor claim a value above it.
//
// Every durable transfer dimension must agree, the identity may advance by
// exactly one timeline and one epoch, and the positive promotion boundary may
// not change. This deliberately rejects arbitrary identity edits, same-CR
// renewal, skipped generations, and incomplete transfer receipts.
func haCommittedTransferSuccessorScopeMatches(
	cluster *antflyv1.AntflyCluster,
	lease *coordinationv1.Lease,
	successor haFencingLeaseScope,
	holder string,
	transition int32,
) bool {
	_, ok := haCommittedTransferSuccessorBoundary(cluster, lease, successor, holder, transition)
	return ok
}

func haCommittedTransferSuccessorBoundary(
	cluster *antflyv1.AntflyCluster,
	lease *coordinationv1.Lease,
	successor haFencingLeaseScope,
	holder string,
	transition int32,
) (uint64, bool) {
	if cluster == nil || cluster.UID == "" || lease == nil || lease.Annotations == nil ||
		transition <= 1 || successor.primaryLSN == 0 || strings.TrimSpace(holder) == "" ||
		strings.TrimSpace(holder) != strings.TrimSpace(successor.currentPrimaryID) ||
		lease.Annotations[haFencingLeaseAnnotationTransferCommitted] != "true" ||
		strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationFormerHolder]) == "" ||
		strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationFormerHolder]) == strings.TrimSpace(holder) ||
		strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationFormerHolder]) !=
			strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationCurrentPrimaryID]) ||
		!haWatchdogProcessBootIDValid(strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationProcessBootID])) ||
		strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationTransferOriginUID]) == "" ||
		strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationTransferOriginUID]) == string(cluster.UID) ||
		lease.Annotations[haFencingLeaseAnnotationCommittedTransition] != strconv.FormatInt(int64(transition), 10) {
		return 0, false
	}
	if lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) != strings.TrimSpace(holder) ||
		lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions != transition {
		return 0, false
	}

	parse := func(key string) (uint64, bool) {
		value, err := strconv.ParseUint(strings.TrimSpace(lease.Annotations[key]), 10, 64)
		return value, err == nil
	}
	clusterID, clusterOK := parse(haFencingLeaseAnnotationClusterID)
	shardID, shardOK := parse(haFencingLeaseAnnotationShardID)
	tableID, tableOK := parse(haFencingLeaseAnnotationTableID)
	parentTimelineID, timelineOK := parse(haFencingLeaseAnnotationTimelineID)
	parentEpoch, epochOK := parse(haFencingLeaseAnnotationEpoch)
	boundary, boundaryOK := parse(haFencingLeaseAnnotationPrimaryLSN)
	ok := clusterOK && shardOK && tableOK && timelineOK && epochOK && boundaryOK && boundary > 0 &&
		clusterID == successor.clusterID && shardID == successor.shardID && tableID == successor.tableID &&
		successor.timelineID > 0 && parentTimelineID == successor.timelineID-1 &&
		successor.epoch > 0 && parentEpoch == successor.epoch-1 &&
		successor.primaryLSN <= boundary
	if !ok {
		return 0, false
	}
	return boundary, true
}

func haLeaseFenceReady(lease *coordinationv1.Lease, generation uint64, now time.Time) (bool, string) {
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return false, "LeaseNotHeld"
	}
	if generation == 0 {
		return false, "LeaseGenerationMissing"
	}
	if lease.Spec.RenewTime == nil || lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds <= 0 {
		return false, "LeaseTimingMissing"
	}
	expiresAt := lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	if !now.Before(expiresAt) {
		return false, "LeaseExpired"
	}
	return true, "LeaseHeld"
}

func haLeaseFenceGeneration(lease *coordinationv1.Lease) uint64 {
	if lease.Spec.LeaseTransitions != nil && *lease.Spec.LeaseTransitions > 0 {
		return uint64(*lease.Spec.LeaseTransitions)
	}
	if lease.Generation > 0 {
		return uint64(lease.Generation)
	}
	return 0
}

func planHA(cluster *antflyv1.AntflyCluster) haPlan {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled {
		return haPlan{}
	}

	status := cluster.Status.HAStatus
	if status == nil {
		status = &antflyv1.HAStatus{Mode: ha.Mode}
	}

	slotByName := map[string]antflyv1.HAStandbyStatus{}
	for _, standby := range status.Standbys {
		name := standby.Name
		if name == "" {
			name = standby.SlotName
		}
		if name != "" {
			slotByName[name] = standby
		}
		if standby.SlotName != "" {
			slotByName[standby.SlotName] = standby
		}
	}

	var plan haPlan
	promotedPrimaryID := haPromotedPrimaryNodeID(status)
	for _, standby := range ha.Standbys {
		slotName := standbySlotName(standby)
		if promotedPrimaryID != "" &&
			(promotedPrimaryID == strings.TrimSpace(standby.Name) || promotedPrimaryID == strings.TrimSpace(slotName)) {
			continue
		}
		observed, ok := slotByName[standby.Name]
		if !ok {
			observed, ok = slotByName[slotName]
		}
		if !standbyDesired(standby) {
			if haStandbyMatchesFormerPrimary(status, standby.Name, slotName) {
				continue
			}
			if ok && observed.Active {
				plan.Actions = append(plan.Actions, haPlannedAction{
					Kind:        haActionPauseSlot,
					StandbyName: standby.Name,
					SlotName:    slotName,
					Reason:      "StandbyMarkedUndesired",
				})
			}
			if ok && standby.DropSlotOnRemoval {
				dependsOn := haActionKind("")
				if observed.Active {
					dependsOn = haActionPauseSlot
				}
				plan.Actions = append(plan.Actions, haPlannedAction{
					Kind:        haActionDropSlot,
					DependsOn:   dependsOn,
					StandbyName: standby.Name,
					SlotName:    slotName,
					Reason:      "StandbyMarkedForSlotDrop",
				})
			}
			continue
		}
		plan.DesiredStandbyCount++
		// Standby-local observation errors create a configured status entry even
		// before the primary owns a replication slot. Runtime-owned portable
		// seeding activates that slot only at the end of the seed chain. A typed
		// primary slot observation with a nonzero timeline can still describe the
		// intermediate "seeding" lifecycle, so only active runtime state or a
		// completed, validated ActivateSeededSlot receipt proves bootstrap crossed
		// the activation boundary.
		runtimeOwnedSeed := haStandbyUsesRuntimeOwnedSeedCapture(standby)
		runtimeOwnedSeedTargetLSN := uint64(0)
		runtimeOwnedSeedActivated := false
		runtimeOwnedSeedInFlight := false
		runtimeOwnedSeedLifecycleCompleted := false
		if runtimeOwnedSeed {
			runtimeOwnedSeedTargetLSN = haRuntimeOwnedSeedTargetLSN(status, standby, slotName)
			runtimeOwnedSeedInFlight = runtimeOwnedSeedTargetLSN > 0
			runtimeOwnedSeedActivated = observed.Active ||
				haRuntimeOwnedSeedActivationCompleted(status, standby, slotName, runtimeOwnedSeedTargetLSN)
			runtimeOwnedSeedLifecycleCompleted = haRuntimeOwnedSeedLifecycleCompleted(status, standby, slotName, runtimeOwnedSeedTargetLSN)
		}
		if !ok || (runtimeOwnedSeed && (!runtimeOwnedSeedActivated ||
			(runtimeOwnedSeedInFlight && !runtimeOwnedSeedLifecycleCompleted))) {
			plan.UnhealthyStandbyCount++
			seedTargetLSN := initialStandbyLSN(standby, status.PrimaryLSN)
			if runtimeOwnedSeed {
				seedTargetLSN = runtimeOwnedSeedTargetLSN
				if seedTargetLSN == 0 {
					seedTargetLSN = haRuntimeOwnedInitialSeedTargetLSN(status)
				}
			}
			if seedTargetLSN == 0 {
				continue
			}
			if runtimeOwnedSeed {
				plan.Actions = append(plan.Actions, haSeedCompletionActions(
					standby,
					slotName,
					seedTargetLSN,
					"StandbyNeedsBaseBackup",
					"",
				)...)
				continue
			}
			plan.Actions = append(plan.Actions, haPlannedAction{
				Kind:        haActionCreateSlot,
				StandbyName: standby.Name,
				SlotName:    slotName,
				TargetLSN:   seedTargetLSN,
				Reason:      "SlotMissing",
			})
			plan.Actions = append(plan.Actions, haPlannedAction{
				Kind:        haActionSeedStandby,
				DependsOn:   haActionCreateSlot,
				StandbyName: standby.Name,
				SlotName:    slotName,
				TargetLSN:   haSeedBeginTargetLSN(status.PrimaryLSN),
				Reason:      "StandbyNeedsBaseBackup",
			})
			plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, haSeedBeginTargetLSN(status.PrimaryLSN), "StandbyNeedsBaseBackup", haActionSeedStandby)...)
			continue
		}
		if !observed.Active && !observed.ReseedRequired {
			plan.UnhealthyStandbyCount++
			plan.Actions = append(plan.Actions, haPlannedAction{
				Kind:        haActionResumeSlot,
				StandbyName: standby.Name,
				SlotName:    slotName,
				Reason:      "SlotInactive",
			})
			continue
		}
		if observed.LastError != "" {
			plan.UnhealthyStandbyCount++
		}
		if standbyLagging(observed) {
			plan.LaggingStandbyCount++
		}
		if observed.ReseedRequired || observed.Status == "reseed_required" {
			plan.ReseedRequiredCount++
			if status.PrimaryLSN > 0 {
				seedTargetLSN := haSeedBeginTargetLSN(status.PrimaryLSN)
				if haStandbyUsesRuntimeOwnedSeedCapture(standby) {
					plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, seedTargetLSN, "SlotRequiresReseed", "")...)
					continue
				}
				plan.Actions = append(plan.Actions, haPlannedAction{
					Kind:        haActionMarkReseed,
					StandbyName: standby.Name,
					SlotName:    slotName,
					TargetLSN:   seedTargetLSN,
					Reason:      "SlotRequiresReseed",
				})
				plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, seedTargetLSN, "SlotRequiresReseed", haActionMarkReseed)...)
			}
			continue
		}
		if observed.Active && observed.ApplyLagLSN == 0 {
			plan.HealthyStandbyCount++
		}
		if standbyReadSafe(status, observed) {
			plan.ReadSafeStandbyCount++
		}
	}

	plan.SyncPolicy = haEvaluateSyncPolicy(ha, status)
	plan.SyncPolicyDegraded = plan.SyncPolicy.Degraded
	plan.FencingReady = haFencingReady(ha, status)
	plan.PromotionRouteReady = haPromotionRouteReady(ha, status)
	plan.PrimaryAdminUnavailable = haPrimaryAdminUnavailable(status)
	plan.PromotionBoundaryReady = haPromotionBoundaryReady(status)
	plan.PromotionAlreadyRecorded = haPromotionAlreadyRecorded(ha, status)
	plan.PromotionStandbyName = haAutomaticPromotionStandby(ha, status, plan)
	plan.AutomaticPromotionAllowed = plan.PromotionStandbyName != ""
	if plan.AutomaticPromotionAllowed {
		fence := status.Fencing
		promotionBoundary := haAutomaticFailoverPromotionBoundary(status, fence.Holder, fence.Generation)
		formerPrimaryID := haAutomaticFailoverFormerPrimaryID(ha)
		formerPrimaryFenceAction := haActionIsolateFormerPrimary
		plan.Actions = append(plan.Actions,
			haPlannedAction{
				Kind:            formerPrimaryFenceAction,
				StandbyName:     formerPrimaryID,
				TargetLSN:       promotionBoundary,
				RouteFrom:       formerPrimaryID,
				RouteTo:         plan.PromotionStandbyName,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				FenceReason:     fence.Reason,
				Reason:          "IsolateUnreachableFormerPrimaryForPromotion",
			},
			haPlannedAction{
				Kind:            haActionAcquireFence,
				DependsOn:       formerPrimaryFenceAction,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       promotionBoundary,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				FenceReason:     fence.Reason,
				Reason:          "AutomaticFailoverReady",
			},
			haPlannedAction{
				Kind:            haActionAssessPromotion,
				DependsOn:       haActionAcquireFence,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       promotionBoundary,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				FenceReason:     fence.Reason,
				Reason:          "AutomaticFailoverReady",
			},
			haPlannedAction{
				Kind:            haActionPromoteStandby,
				DependsOn:       haActionAssessPromotion,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       promotionBoundary,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				FenceReason:     fence.Reason,
				Reason:          "AutomaticFailoverReady",
			},
			haPlannedAction{
				Kind:            haActionUpdatePrimaryRoute,
				DependsOn:       haActionPromoteStandby,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       promotionBoundary,
				RouteFrom:       haPrimaryRouteCurrentTarget(status),
				RouteTo:         plan.PromotionStandbyName,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				FenceReason:     fence.Reason,
				Reason:          "PromotionPlanned",
			},
			haPlannedAction{
				Kind:            haActionDemoteFormerPrimary,
				DependsOn:       haActionPromoteStandby,
				StandbyName:     formerPrimaryID,
				TargetLSN:       promotionBoundary,
				ObservedLSN:     promotionBoundary,
				RetainedFromLSN: status.Retention.OldestRestartLSN,
				RouteFrom:       formerPrimaryID,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				FenceReason:     fence.Reason,
				Reason:          "PromotionPlanned",
			},
		)
	}
	plan.PrimaryRoute = haEvaluatePrimaryRoute(cluster, status, plan.PromotionStandbyName)
	if action := haPrimaryRoutePlannedAction(plan.PrimaryRoute, status); action.Kind != "" && !haHasPlannedAction(plan.Actions, action.Kind) {
		plan.Actions = append(plan.Actions, action)
	}
	plan.FormerPrimary = haEvaluateFormerPrimary(status)
	formerPrimaryFenceDependency := haActionFenceFormerPrimary
	if action := haFormerPrimaryFencePlannedAction(cluster, status); action.Kind != "" {
		formerPrimaryFenceDependency = action.Kind
		plan.Actions = append(plan.Actions, action)
	}
	if action := haFormerPrimaryPlannedAction(plan.FormerPrimary, status); action.Kind != "" {
		action.DependsOn = formerPrimaryFenceDependency
		plan.Actions = append(plan.Actions, action)
		if action.Kind == haActionReseedFormerPrimary {
			if standby, ok := haStandbySpecByName(ha, action.StandbyName); ok {
				slotName := standbySlotName(standby)
				if status.PrimaryLSN > 0 {
					seedDependsOn := haActionReseedFormerPrimary
					seedTargetLSN := haSeedBeginTargetLSN(status.PrimaryLSN)
					seedBeginSatisfied := false
					if observed, ok := slotByName[slotName]; ok {
						if observed.Active && observed.RestartLSN > 0 && !observed.ReseedRequired {
							seedTargetLSN = observed.RestartLSN
							plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, seedTargetLSN, "FormerPrimaryRequiresReseed", seedDependsOn)...)
							seedBeginSatisfied = true
						}
					}
					if !seedBeginSatisfied {
						seed := haPlannedAction{
							Kind:        haActionSeedStandby,
							DependsOn:   seedDependsOn,
							StandbyName: standby.Name,
							SlotName:    slotName,
							TargetLSN:   seedTargetLSN,
							Reason:      "FormerPrimaryRequiresReseed",
						}
						plan.Actions = append(plan.Actions, seed)
						plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, seedTargetLSN, "FormerPrimaryRequiresReseed", haActionSeedStandby)...)
					}
				}
			}
		}
	}

	return plan
}

func haFormerPrimaryFencePlannedAction(cluster *antflyv1.AntflyCluster, status *antflyv1.HAStatus) haPlannedAction {
	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return haPlannedAction{}
	}
	if isolated := haSucceededFormerPrimaryIsolation(cluster, status, promotion); isolated != nil {
		return haPlannedAction{
			Kind:            haActionIsolateFormerPrimary,
			StandbyName:     promotion.OldPrimaryID,
			TargetLSN:       isolated.TargetLSN,
			ObservedLSN:     isolated.ObservedLSN,
			RouteFrom:       promotion.OldPrimaryID,
			RouteTo:         promotion.PromotedStandbyID,
			FenceAuthority:  promotion.FenceAuthority,
			FenceHolder:     promotion.PromotedStandbyID,
			FenceGeneration: promotion.FenceGeneration,
			FenceReason:     promotion.FenceReason,
			Reason:          "FormerPrimaryPhysicallyIsolated",
		}
	}
	return haPlannedAction{
		Kind:            haActionFenceFormerPrimary,
		StandbyName:     promotion.OldPrimaryID,
		TargetLSN:       haPromotionObservedLSN(promotion),
		RouteFrom:       promotion.OldPrimaryID,
		RouteTo:         promotion.PromotedStandbyID,
		FenceAuthority:  promotion.FenceAuthority,
		FenceHolder:     promotion.PromotedStandbyID,
		FenceGeneration: promotion.FenceGeneration,
		FenceReason:     promotion.FenceReason,
		Reason:          "FenceFormerPrimaryForPromotion",
	}
}

func haSucceededFormerPrimaryIsolation(cluster *antflyv1.AntflyCluster, status *antflyv1.HAStatus, promotion *antflyv1.HAPromotionStatus) *antflyv1.HAPlannedActionStatus {
	if status == nil || promotion == nil || promotion.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		promotion.FenceGeneration == 0 || strings.TrimSpace(promotion.OldPrimaryID) == "" ||
		strings.TrimSpace(promotion.PromotedStandbyID) == "" || haPromotionRequiredLSN(promotion) == 0 {
		return nil
	}
	for i := range status.PlannedActions {
		action := &status.PlannedActions[i]
		if haActionKind(action.Kind) != haActionIsolateFormerPrimary ||
			!haPhysicalIsolationSucceededWithEvidence(cluster, *action) ||
			strings.TrimSpace(action.StandbyName) != strings.TrimSpace(promotion.OldPrimaryID) ||
			strings.TrimSpace(action.RouteTo) != strings.TrimSpace(promotion.PromotedStandbyID) ||
			action.FenceAuthority != promotion.FenceAuthority ||
			action.FenceGeneration != promotion.FenceGeneration ||
			action.TargetLSN == 0 || action.TargetLSN != haPromotionRequiredLSN(promotion) {
			continue
		}
		return action
	}
	return nil
}

func haSeedBeginTargetLSN(primaryLSN uint64) uint64 {
	if primaryLSN == 0 || primaryLSN == ^uint64(0) {
		return 0
	}
	return primaryLSN + 1
}

func haRuntimeOwnedInitialSeedTargetLSN(status *antflyv1.HAStatus) uint64 {
	if status == nil {
		return 0
	}
	if targetLSN := haSeedBeginTargetLSN(status.PrimaryLSN); targetLSN != 0 {
		return targetLSN
	}
	// A successful authenticated primary observation distinguishes a valid
	// empty HA log from an unknown boundary. Runtime-owned capture appends the
	// backup_start control record atomically, making LSN 1 the seed checkpoint.
	if status.PrimaryLSN == 0 && status.PrimaryAdminReachable {
		return 1
	}
	return 0
}

func haRuntimeOwnedSeedTargetLSN(status *antflyv1.HAStatus, standby antflyv1.HAStandbySpec, slotName string) uint64 {
	if status == nil || !haStandbyUsesRuntimeOwnedSeedCapture(standby) {
		return 0
	}
	explicitGeneration := ""
	if standby.SeedArtifact != nil {
		explicitGeneration = strings.TrimSpace(standby.SeedArtifact.Generation)
	}
	for _, action := range status.PlannedActions {
		if !haPlannedActionKindIsPortableArtifact(haActionKind(action.Kind)) ||
			strings.TrimSpace(action.StandbyName) != strings.TrimSpace(standby.Name) ||
			strings.TrimSpace(action.SlotName) != strings.TrimSpace(slotName) ||
			action.TargetLSN == 0 ||
			(explicitGeneration != "" && strings.TrimSpace(action.SeedArtifactGeneration) != explicitGeneration) {
			continue
		}
		return action.TargetLSN
	}
	return 0
}

func haRuntimeOwnedSeedActivationCompleted(
	status *antflyv1.HAStatus,
	standby antflyv1.HAStandbySpec,
	slotName string,
	targetLSN uint64,
) bool {
	if status == nil || targetLSN == 0 || !haStandbyUsesRuntimeOwnedSeedCapture(standby) {
		return false
	}
	generation := haSeedArtifactGeneration(standby, slotName, targetLSN)
	if generation == "" {
		return false
	}
	for _, action := range status.PlannedActions {
		if haActionKind(action.Kind) != haActionActivateSeededSlot ||
			strings.TrimSpace(action.StandbyName) != strings.TrimSpace(standby.Name) ||
			strings.TrimSpace(action.SlotName) != strings.TrimSpace(slotName) ||
			action.TargetLSN != targetLSN ||
			strings.TrimSpace(action.SeedArtifactGeneration) != generation {
			continue
		}
		return haAdminActionSucceededWithEvidence(action)
	}
	return false
}

func haRuntimeOwnedSeedLifecycleCompleted(
	status *antflyv1.HAStatus,
	standby antflyv1.HAStandbySpec,
	slotName string,
	targetLSN uint64,
) bool {
	if status == nil || targetLSN == 0 || !haStandbyUsesRuntimeOwnedSeedCapture(standby) {
		return false
	}
	generation := haSeedArtifactGeneration(standby, slotName, targetLSN)
	if generation == "" {
		return false
	}
	for _, action := range status.PlannedActions {
		if haActionKind(action.Kind) != haActionPruneSeedArtifacts ||
			strings.TrimSpace(action.StandbyName) != strings.TrimSpace(standby.Name) ||
			strings.TrimSpace(action.SlotName) != strings.TrimSpace(slotName) ||
			action.TargetLSN != targetLSN ||
			strings.TrimSpace(action.SeedArtifactGeneration) != generation {
			continue
		}
		return haAdminActionSucceededWithEvidence(action)
	}
	return false
}

func haStandbyMatchesFormerPrimary(status *antflyv1.HAStatus, standbyName string, slotName string) bool {
	if status == nil || status.LastPromotion == nil {
		return false
	}
	oldPrimaryID := strings.TrimSpace(status.LastPromotion.OldPrimaryID)
	if oldPrimaryID == "" {
		return false
	}
	return oldPrimaryID == strings.TrimSpace(standbyName) || oldPrimaryID == strings.TrimSpace(slotName)
}

func applyHAPlanStatus(cluster *antflyv1.AntflyCluster, plan haPlan) {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled {
		cluster.Status.HAStatus = nil
		return
	}

	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{}
	}
	cluster.Status.HAStatus.Mode = ha.Mode
	cluster.Status.HAStatus.DesiredStandbyCount = plan.DesiredStandbyCount
	cluster.Status.HAStatus.HealthyStandbyCount = plan.HealthyStandbyCount
	cluster.Status.HAStatus.UnhealthyStandbyCount = plan.UnhealthyStandbyCount
	cluster.Status.HAStatus.LaggingStandbyCount = plan.LaggingStandbyCount
	cluster.Status.HAStatus.ReadSafeStandbyCount = plan.ReadSafeStandbyCount
	cluster.Status.HAStatus.ReseedRequiredCount = plan.ReseedRequiredCount
	cluster.Status.HAStatus.AutomaticPromotionAllowed = plan.AutomaticPromotionAllowed
	for i := range plan.Actions {
		if plan.Actions[i].Kind != haActionActivateSeedArtifact || cluster.Spec.Standalone == nil ||
			cluster.Spec.Standalone.NodeID <= 0 || cluster.Spec.Standalone.Replicas != 1 {
			continue
		}
		plan.Actions[i].TargetLocalNodeID = uint64(cluster.Spec.Standalone.NodeID)
		plan.Actions[i].TargetReplicaID = 1
	}
	cluster.Status.HAStatus.PlannedActions = haPlannedActionStatuses(plan.Actions, ha, cluster.Status.HAStatus, cluster)
	cluster.Status.HAStatus.PrimaryRoute = haPrimaryRouteStatus(plan.PrimaryRoute)
	cluster.Status.HAStatus.Sync = haSyncStatus(plan.SyncPolicy)
	cluster.Status.HAStatus.FormerPrimary = haFormerPrimaryStatus(plan.FormerPrimary)
	mergeConfiguredStandbys(cluster.Status.HAStatus, ha)
}

func haSyncStatus(evaluation haSyncEvaluation) antflyv1.HASyncStatus {
	return antflyv1.HASyncStatus{
		Mode:          evaluation.Mode,
		Selection:     evaluation.Selection,
		Required:      evaluation.Required,
		Satisfied:     evaluation.Satisfied,
		Candidates:    evaluation.Candidates,
		FailurePolicy: evaluation.FailurePolicy,
		Degraded:      evaluation.Degraded,
		Action:        evaluation.Action,
	}
}

func haPlannedActionStatuses(actions []haPlannedAction, ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus, clusters ...*antflyv1.AntflyCluster) []antflyv1.HAPlannedActionStatus {
	completedRewind := haCompletedFormerPrimaryRewind(status)
	for _, action := range actions {
		if action.Kind == haActionRewindFormerPrimary {
			completedRewind = nil
			break
		}
	}
	retainedSeedActions := haRetainedCompletedSeedActions(actions, ha, status)
	if len(actions) == 0 && completedRewind == nil && len(retainedSeedActions) == 0 {
		return nil
	}
	retainedAssessment := haRetainedFormerPrimaryAssessment(actions, status)
	outCapacity := len(actions)
	if completedRewind != nil {
		outCapacity++
	}
	if retainedAssessment != nil {
		outCapacity++
	}
	outCapacity += len(retainedSeedActions)
	out := make([]antflyv1.HAPlannedActionStatus, 0, outCapacity)
	if completedRewind != nil {
		if retainedAssessment != nil {
			out = append(out, *retainedAssessment)
			retainedAssessment = nil
		}
		out = append(out, *completedRewind)
	}
	for _, action := range actions {
		if retainedAssessment != nil &&
			(action.Kind == haActionRewindFormerPrimary || action.Kind == haActionReseedFormerPrimary) {
			out = append(out, *retainedAssessment)
			retainedAssessment = nil
		}
		haBindSeedCaptureResult(&action, status)
		haBindSeedArtifactProtections(&action, status)
		haBindSeedSourcePVCName(&action, ha)
		haBindSeedTopology(&action, ha)
		haBindFormerPrimaryTopology(&action, ha)
		adminMethod, adminPath := haAdminOperation(action)
		statusAction := antflyv1.HAPlannedActionStatus{
			Kind:                             string(action.Kind),
			Phase:                            string(haPlannedActionPhase(action.Kind)),
			Executor:                         string(haPlannedActionExecutor(action.Kind)),
			DependsOn:                        string(action.DependsOn),
			StandbyName:                      action.StandbyName,
			SlotName:                         action.SlotName,
			TargetLSN:                        action.TargetLSN,
			ObservedLSN:                      action.ObservedLSN,
			RetainedFromLSN:                  action.RetainedFromLSN,
			RouteFrom:                        action.RouteFrom,
			RouteTo:                          action.RouteTo,
			FenceAuthority:                   action.FenceAuthority,
			FenceHolder:                      action.FenceHolder,
			FenceGeneration:                  action.FenceGeneration,
			FenceReason:                      action.FenceReason,
			SeedManifestPath:                 action.SeedManifestPath,
			SeedContentRoot:                  action.SeedContentRoot,
			SeedArtifactTargetRoot:           action.SeedArtifactTargetRoot,
			SeedArtifactLocation:             action.SeedArtifactLocation,
			SeedArtifactGeneration:           action.SeedArtifactGeneration,
			SeedArtifactRetainGenerations:    action.SeedArtifactRetention,
			SeedArtifactCaptureRoot:          action.SeedArtifactCaptureRoot,
			SeedCaptureReceiptPath:           action.SeedCaptureReceiptPath,
			SeedCaptureReceiptSHA256:         action.SeedCaptureReceiptSHA256,
			SeedArtifactProtectedGenerations: append([]string(nil), action.SeedArtifactProtectedGenerations...),
			TopologyID:                       action.TopologyID,
			TopologyGeneration:               action.TopologyGeneration,
			TopologyNodeID:                   action.TopologyNodeID,
			SourcePVCName:                    action.SourcePVCName,
			SourcePVCUID:                     action.SourcePVCUID,
			TargetPVCName:                    action.TargetPVCName,
			TargetPVCUID:                     action.TargetPVCUID,
			TargetLocalNodeID:                action.TargetLocalNodeID,
			TargetReplicaID:                  action.TargetReplicaID,
			AdminCommand:                     haAdminCommand(action, haReplicationIdentity(ha), status),
			AdminURL:                         haAdminURL(action, ha, status),
			AdminNodeID:                      haAdminNodeID(action, ha, status),
			AdminMethod:                      adminMethod,
			AdminPath:                        adminPath,
			Reason:                           action.Reason,
		}
		if ha != nil && ha.Admin != nil {
			statusAction.RetryGeneration = ha.Admin.RetryGeneration
		}
		statusAction.OperationID = haPlannedActionOperationID(statusAction)
		statusAction = haPreservePlannedActionExecution(statusAction, status, clusters...)
		out = append(out, statusAction)
	}
	out = append(out, retainedSeedActions...)
	return out
}

// haRetainedCompletedSeedActions keeps the operator-observed portable-seed
// receipt chain available after the primary slot becomes active. That receipt
// is the cross-CR authority used to open the target runtime's startup gate; if
// the planner discarded it as soon as the slot activated, the primary and
// target controllers could each wait forever for evidence owned by the other.
//
// Retention is deliberately bounded to one completed runtime-owned chain per
// currently desired standby and is tied to the exact declared generation,
// topology, node, and PVC incarnation. Any topology/PVC/generation change
// drops the old evidence rather than allowing it to authorize a replacement.
func haRetainedCompletedSeedActions(
	planned []haPlannedAction,
	ha *antflyv1.HighAvailabilitySpec,
	status *antflyv1.HAStatus,
) []antflyv1.HAPlannedActionStatus {
	if ha == nil || status == nil || len(status.PlannedActions) == 0 {
		return nil
	}
	expectedKinds := [...]haActionKind{
		haActionCaptureSeedArtifact,
		haActionPublishSeedArtifact,
		haActionGCSourceSeedGenerations,
		haActionRestoreSeedArtifact,
		haActionActivateSeedArtifact,
		haActionActivateSeededSlot,
		haActionGCTargetSeedGenerations,
		haActionPruneSeedArtifacts,
	}
	retained := make([]antflyv1.HAPlannedActionStatus, 0, len(expectedKinds))
	for _, standby := range ha.Standbys {
		if !standbyDesired(standby) || !haStandbyUsesRuntimeOwnedSeedCapture(standby) || standby.SeedArtifact == nil {
			continue
		}
		artifact := standby.SeedArtifact
		slotName := standbySlotName(standby)
		generation := strings.TrimSpace(artifact.Generation)
		if generation == "" || strings.TrimSpace(artifact.TopologyID) == "" || artifact.TopologyGeneration <= 0 ||
			strings.TrimSpace(artifact.NodeID) != strings.TrimSpace(standby.Name) || artifact.TargetPVC == nil ||
			strings.TrimSpace(artifact.TargetPVC.ClaimName) == "" || strings.TrimSpace(artifact.TargetPVCUID) == "" {
			continue
		}
		// A live plan for this exact generation already preserves execution
		// state through haPreservePlannedActionExecution; do not duplicate it.
		alreadyPlanned := false
		for _, action := range planned {
			if strings.TrimSpace(action.StandbyName) == strings.TrimSpace(standby.Name) &&
				strings.TrimSpace(action.SlotName) == strings.TrimSpace(slotName) &&
				strings.TrimSpace(action.SeedArtifactGeneration) == generation {
				alreadyPlanned = true
				break
			}
		}
		if alreadyPlanned {
			continue
		}

		chain := make([]antflyv1.HAPlannedActionStatus, 0, len(expectedKinds))
		for _, kind := range expectedKinds {
			found := false
			for i := range status.PlannedActions {
				action := status.PlannedActions[i]
				if haActionKind(action.Kind) != kind || action.AdminJobPhase != haAdminJobPhaseSucceeded ||
					strings.TrimSpace(action.StandbyName) != strings.TrimSpace(standby.Name) ||
					strings.TrimSpace(action.SlotName) != strings.TrimSpace(slotName) ||
					strings.TrimSpace(action.SeedArtifactGeneration) != generation ||
					strings.TrimSpace(action.TopologyID) != strings.TrimSpace(artifact.TopologyID) ||
					action.TopologyGeneration != artifact.TopologyGeneration ||
					strings.TrimSpace(action.TopologyNodeID) != strings.TrimSpace(artifact.NodeID) ||
					strings.TrimSpace(action.TargetPVCName) != strings.TrimSpace(artifact.TargetPVC.ClaimName) ||
					strings.TrimSpace(action.TargetPVCUID) != strings.TrimSpace(artifact.TargetPVCUID) {
					continue
				}
				chain = append(chain, *action.DeepCopy())
				found = true
				break
			}
			if !found {
				chain = nil
				break
			}
		}
		if len(chain) != len(expectedKinds) || !haAdminActionSucceededWithEvidence(chain[len(chain)-1]) {
			continue
		}
		retained = append(retained, chain...)
	}
	return retained
}

func haBindFormerPrimaryTopology(action *haPlannedAction, ha *antflyv1.HighAvailabilitySpec) {
	if action == nil || (action.Kind != haActionRewindFormerPrimary &&
		action.Kind != haActionReseedFormerPrimary && action.Kind != haActionDemoteFormerPrimary) {
		return
	}
	standby, ok := haStandbySpecByName(ha, strings.TrimSpace(action.StandbyName))
	if !ok || standby.SeedArtifact == nil {
		return
	}
	artifact := standby.SeedArtifact
	topologyID := strings.TrimSpace(artifact.TopologyID)
	nodeID := strings.TrimSpace(artifact.NodeID)
	targetPVCUID := strings.TrimSpace(artifact.TargetPVCUID)
	if topologyID == "" || artifact.TopologyGeneration <= 0 || nodeID == "" ||
		nodeID != strings.TrimSpace(action.StandbyName) || artifact.TargetPVC == nil ||
		strings.TrimSpace(artifact.TargetPVC.ClaimName) == "" || targetPVCUID == "" {
		return
	}
	// Rejoin execution is observed through a remote node-local admin endpoint.
	// Freeze the declarative topology/PVC incarnation into status so a Colony
	// controller can reject receipts and retries that belong to a replaced
	// former-primary volume even though the admin request itself is remote.
	action.TopologyID = topologyID
	action.TopologyGeneration = artifact.TopologyGeneration
	action.TopologyNodeID = nodeID
	action.TargetPVCName = strings.TrimSpace(artifact.TargetPVC.ClaimName)
	action.TargetPVCUID = targetPVCUID
}

func haBindSeedSourcePVCName(action *haPlannedAction, ha *antflyv1.HighAvailabilitySpec) {
	if action == nil || ha == nil || !haPlannedActionKindUsesSeedSourceAuthority(action.Kind) {
		return
	}
	standby, ok := haStandbySpecByName(ha, strings.TrimSpace(action.StandbyName))
	if !ok || standby.SeedArtifact == nil || standby.SeedArtifact.SourcePVC == nil {
		return
	}
	action.SourcePVCName = strings.TrimSpace(standby.SeedArtifact.SourcePVC.ClaimName)
}

func haBindSeedTopology(action *haPlannedAction, ha *antflyv1.HighAvailabilitySpec) {
	if action == nil || ha == nil || ha.Runtime == nil || ha.Runtime.StartupGate == nil ||
		ha.Runtime.StartupGate.RequiredReceipt == nil {
		return
	}
	required := ha.Runtime.StartupGate.RequiredReceipt
	if strings.TrimSpace(action.SlotName) != strings.TrimSpace(required.SlotName) ||
		strings.TrimSpace(action.SeedArtifactGeneration) != strings.TrimSpace(required.Generation) {
		return
	}
	action.TopologyID = strings.TrimSpace(required.TopologyID)
	action.TopologyGeneration = required.TopologyGeneration
	action.TopologyNodeID = strings.TrimSpace(required.NodeID)
	action.TargetPVCName = strings.TrimSpace(required.TargetPVCName)
	action.TargetPVCUID = strings.TrimSpace(required.TargetPVCUID)
}

func haBindSeedCaptureResult(action *haPlannedAction, status *antflyv1.HAStatus) {
	if action == nil || status == nil ||
		(action.Kind != haActionPublishSeedArtifact && action.Kind != haActionGCSourceSeedGenerations &&
			action.Kind != haActionRestoreSeedArtifact && action.Kind != haActionActivateSeedArtifact) {
		return
	}
	for i := range status.PlannedActions {
		capture := status.PlannedActions[i]
		if haActionKind(capture.Kind) != haActionCaptureSeedArtifact ||
			strings.TrimSpace(capture.StandbyName) != strings.TrimSpace(action.StandbyName) ||
			strings.TrimSpace(capture.SlotName) != strings.TrimSpace(action.SlotName) ||
			strings.TrimSpace(capture.SeedArtifactGeneration) != strings.TrimSpace(action.SeedArtifactGeneration) ||
			!haAdminActionSucceededWithEvidence(capture) || capture.AdminResult == nil {
			continue
		}
		result := capture.AdminResult
		if action.Kind == haActionGCSourceSeedGenerations {
			action.SeedArtifactCaptureRoot = haSeedCaptureRoot(result.SeedGenerationRoot, action.SeedArtifactGeneration)
			return
		}
		receiptPath := haSeedCaptureReceiptPath(result.SeedGenerationRoot, action.SeedArtifactGeneration)
		if receiptPath == "" || !isLowerHexDigest(result.CaptureReceiptSHA256) {
			return
		}
		action.SeedCaptureReceiptPath = receiptPath
		action.SeedCaptureReceiptSHA256 = strings.TrimSpace(result.CaptureReceiptSHA256)
		if action.Kind == haActionPublishSeedArtifact {
			action.SeedManifestPath = result.SeedManifestPath
			action.SeedContentRoot = result.SeedContentRoot
		}
		return
	}
}

func haSeedCaptureReceiptPath(generationRoot, generation string) string {
	if haSeedCaptureRoot(generationRoot, generation) == "" {
		return ""
	}
	return path.Join(path.Clean(strings.TrimSpace(generationRoot)), "COMPLETE.json")
}

func haSeedCaptureRoot(generationRoot, generation string) string {
	generationRoot = path.Clean(strings.TrimSpace(generationRoot))
	generation = strings.TrimSpace(generation)
	if generationRoot == "." || !path.IsAbs(generationRoot) || generation == "" || path.Base(generationRoot) != generation {
		return ""
	}
	generationsRoot := path.Dir(generationRoot)
	if path.Base(generationsRoot) != "generations" {
		return ""
	}
	root := path.Dir(generationsRoot)
	if root == "." || root == "/" {
		return ""
	}
	return root
}

func haBindSeedArtifactProtections(action *haPlannedAction, status *antflyv1.HAStatus) {
	if action == nil || (action.Kind != haActionGCSourceSeedGenerations && action.Kind != haActionGCTargetSeedGenerations) {
		return
	}
	protected := map[string]struct{}{}
	if generation := strings.TrimSpace(action.SeedArtifactGeneration); generation != "" {
		protected[generation] = struct{}{}
	}
	if status != nil {
		if gate := status.StartupGate; gate != nil && gate.ActivationReceipt != nil &&
			strings.TrimSpace(gate.ActivationReceipt.SlotName) == strings.TrimSpace(action.SlotName) {
			if generation := strings.TrimSpace(gate.ActivationReceipt.Generation); generation != "" {
				protected[generation] = struct{}{}
			}
		}
		for i := range status.PlannedActions {
			candidate := status.PlannedActions[i]
			if strings.TrimSpace(candidate.StandbyName) != strings.TrimSpace(action.StandbyName) ||
				strings.TrimSpace(candidate.SlotName) != strings.TrimSpace(action.SlotName) {
				continue
			}
			if generation := strings.TrimSpace(candidate.SeedArtifactGeneration); generation != "" {
				protected[generation] = struct{}{}
			}
		}
	}
	values := make([]string, 0, len(protected))
	for generation := range protected {
		values = append(values, generation)
	}
	sort.Strings(values)
	if len(values) > 256 {
		values = values[:256]
	}
	action.SeedArtifactProtectedGenerations = values
}

func haRetainedFormerPrimaryAssessment(actions []haPlannedAction, status *antflyv1.HAStatus) *antflyv1.HAPlannedActionStatus {
	if status == nil {
		return nil
	}
	hasDisposition := haCompletedFormerPrimaryRewind(status) != nil
	for _, action := range actions {
		switch action.Kind {
		case haActionDemoteFormerPrimary:
			return nil
		case haActionRewindFormerPrimary, haActionReseedFormerPrimary:
			hasDisposition = true
		}
	}
	if !hasDisposition {
		return nil
	}
	for i := range status.PlannedActions {
		previous := status.PlannedActions[i]
		if haActionKind(previous.Kind) != haActionDemoteFormerPrimary ||
			previous.AdminJobPhase != haAdminJobPhaseSucceeded ||
			!haFormerPrimaryDemotePreserveAllowed(status, previous) ||
			!haAdminActionSucceededWithStatusEvidence(status, previous) {
			continue
		}
		return previous.DeepCopy()
	}
	return nil
}

func haCompletedFormerPrimaryRewind(status *antflyv1.HAStatus) *antflyv1.HAPlannedActionStatus {
	promotion := haPromotionReceipt(status)
	if promotion == nil || !haFormerPrimaryFenced(status, promotion) {
		return nil
	}
	for i := range status.PlannedActions {
		action := status.PlannedActions[i]
		if haActionKind(action.Kind) != haActionRewindFormerPrimary ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			!haFormerPrimaryActionSucceededWithPromotionEvidence(status, action) {
			continue
		}
		return action.DeepCopy()
	}
	return nil
}

func haPreservePlannedActionExecution(action antflyv1.HAPlannedActionStatus, status *antflyv1.HAStatus, clusters ...*antflyv1.AntflyCluster) antflyv1.HAPlannedActionStatus {
	if status == nil {
		return action
	}
	for _, previous := range status.PlannedActions {
		if !haSamePlannedActionIdentity(action, previous) {
			continue
		}
		if haPlannedActionKindUsesSeedSourceAuthority(haActionKind(action.Kind)) &&
			strings.TrimSpace(action.SourcePVCName) != "" && action.SourcePVCName == previous.SourcePVCName &&
			strings.TrimSpace(action.SourcePVCUID) == "" {
			// The live Kubernetes UID is frozen at the execution boundary rather
			// than accepted from spec. Preserve that exact incarnation across pure
			// replans only while the declarative source claim name is unchanged.
			action.SourcePVCUID = previous.SourcePVCUID
		}
		if haActionKind(previous.Kind) == haActionDemoteFormerPrimary &&
			previous.AdminJobPhase == haAdminJobPhaseSucceeded &&
			!haFormerPrimaryDemotePreserveAllowed(status, previous) {
			return action
		}
		if haActionKind(previous.Kind) == haActionIsolateFormerPrimary && previous.AdminJobPhase == haAdminJobPhaseSucceeded {
			valid := haPhysicalIsolationSucceededStructurallyWithEvidence(previous)
			if len(clusters) > 0 && clusters[0] != nil {
				valid = haPhysicalIsolationSucceededWithEvidence(clusters[0], previous)
			}
			if !valid {
				return action
			}
		}
		if haActionRequiresAdminResult(haActionKind(previous.Kind)) &&
			previous.AdminJobPhase == haAdminJobPhaseSucceeded &&
			!haAdminActionSucceededWithStatusEvidence(status, previous) {
			return action
		}
		if haActionRequiresSeedArtifactReceipt(haActionKind(previous.Kind)) &&
			previous.AdminJobPhase == haAdminJobPhaseSucceeded &&
			!haSeedArtifactReceiptMatches(previous) {
			return action
		}
		if haPlannedActionExecutionStarted(previous) {
			// Once an external execution is possible, the entire request payload is
			// immutable. In particular, advancing primary/standby LSN observations and
			// regenerated Reason strings must not silently retarget a retry.
			frozen := previous.DeepCopy()
			if strings.TrimSpace(frozen.OperationID) == "" {
				frozen.OperationID = haPlannedActionOperationID(action)
			}
			return *frozen
		}
		action.AdminJobName = previous.AdminJobName
		action.AdminJobPhase = previous.AdminJobPhase
		action.AdminError = previous.AdminError
		action.AdminStatusCode = previous.AdminStatusCode
		action.AttemptCount = previous.AttemptCount
		action.RetryBudgetUsed = previous.RetryBudgetUsed
		action.Retryable = previous.Retryable
		action.ErrorClass = previous.ErrorClass
		if previous.FirstAttemptAt != nil {
			action.FirstAttemptAt = previous.FirstAttemptAt.DeepCopy()
		}
		if previous.LastAttemptAt != nil {
			action.LastAttemptAt = previous.LastAttemptAt.DeepCopy()
		}
		if previous.NextRetryAt != nil {
			action.NextRetryAt = previous.NextRetryAt.DeepCopy()
		}
		if previous.CompletedAt != nil {
			action.CompletedAt = previous.CompletedAt.DeepCopy()
		}
		if previous.AdminResult != nil {
			action.AdminResult = previous.AdminResult.DeepCopy()
		}
		if previous.SeedArtifactReceipt != nil {
			action.SeedArtifactReceipt = previous.SeedArtifactReceipt.DeepCopy()
		}
		if previous.PhysicalIsolationReceipt != nil {
			action.PhysicalIsolationReceipt = previous.PhysicalIsolationReceipt.DeepCopy()
		}
		return action
	}
	return action
}

func haPlannedActionExecutionStarted(action antflyv1.HAPlannedActionStatus) bool {
	if strings.TrimSpace(action.AdminJobName) != "" || action.AttemptCount > 0 || action.InFlightAttempt > 0 {
		return true
	}
	switch action.AdminJobPhase {
	case haAdminJobPhaseRunning, haAdminJobPhaseSucceeded, haAdminJobPhaseFailed, haAdminJobPhaseWaitingJobFallback:
		return true
	default:
		return false
	}
}

// haPlannedActionOperationID intentionally excludes mutable observations
// (TargetLSN, ObservedLSN, RetainedFromLSN, FenceReason, Reason, argv hints, and
// the live-observed source PVC identity). Those values remain frozen in the
// persisted action payload after execution begins, but they do not create an
// unbounded stream of new retry identities. The source PVC UID is preserved
// only while its declarative claim name remains unchanged and is revalidated at
// the execution boundary.
func haPlannedActionOperationID(action antflyv1.HAPlannedActionStatus) string {
	if strings.TrimSpace(action.SeedArtifactCaptureRoot) == "" && strings.TrimSpace(action.TopologyID) == "" &&
		action.TopologyGeneration == 0 && strings.TrimSpace(action.TopologyNodeID) == "" &&
		strings.TrimSpace(action.TargetPVCName) == "" && strings.TrimSpace(action.TargetPVCUID) == "" &&
		strings.TrimSpace(action.SeedCaptureReceiptPath) == "" && strings.TrimSpace(action.SeedCaptureReceiptSHA256) == "" &&
		action.TargetLocalNodeID == 0 && action.TargetReplicaID == 0 {
		return haLegacyPlannedActionOperationID(action)
	}
	identity := struct {
		Version                       int    `json:"version"`
		Kind                          string `json:"kind"`
		Executor                      string `json:"executor"`
		StandbyName                   string `json:"standby_name"`
		SlotName                      string `json:"slot_name"`
		RouteFrom                     string `json:"route_from"`
		RouteTo                       string `json:"route_to"`
		FenceAuthority                string `json:"fence_authority"`
		FenceHolder                   string `json:"fence_holder"`
		FenceGeneration               uint64 `json:"fence_generation"`
		AdminURL                      string `json:"admin_url"`
		AdminNodeID                   string `json:"admin_node_id"`
		AdminMethod                   string `json:"admin_method"`
		AdminPath                     string `json:"admin_path"`
		SeedManifestPath              string `json:"seed_manifest_path"`
		SeedContentRoot               string `json:"seed_content_root"`
		SeedArtifactTargetRoot        string `json:"seed_artifact_target_root"`
		SeedArtifactLocation          string `json:"seed_artifact_location"`
		SeedArtifactGeneration        string `json:"seed_artifact_generation"`
		SeedArtifactRetainGenerations int32  `json:"seed_artifact_retain_generations"`
		SeedArtifactCaptureRoot       string `json:"seed_artifact_capture_root"`
		SeedCaptureReceiptPath        string `json:"seed_capture_receipt_path"`
		SeedCaptureReceiptSHA256      string `json:"seed_capture_receipt_sha256"`
		TopologyID                    string `json:"topology_id"`
		TopologyGeneration            int64  `json:"topology_generation"`
		TopologyNodeID                string `json:"topology_node_id"`
		TargetPVCName                 string `json:"target_pvc_name"`
		TargetPVCUID                  string `json:"target_pvc_uid"`
		TargetLocalNodeID             uint64 `json:"target_local_node_id"`
		TargetReplicaID               uint64 `json:"target_replica_id"`
		RetryGeneration               int64  `json:"retry_generation"`
	}{
		Version:                       2,
		Kind:                          strings.TrimSpace(action.Kind),
		Executor:                      strings.TrimSpace(action.Executor),
		StandbyName:                   strings.TrimSpace(action.StandbyName),
		SlotName:                      strings.TrimSpace(action.SlotName),
		RouteFrom:                     strings.TrimSpace(action.RouteFrom),
		RouteTo:                       strings.TrimSpace(action.RouteTo),
		FenceAuthority:                strings.TrimSpace(string(action.FenceAuthority)),
		FenceHolder:                   strings.TrimSpace(action.FenceHolder),
		FenceGeneration:               action.FenceGeneration,
		AdminURL:                      strings.TrimSpace(action.AdminURL),
		AdminNodeID:                   strings.TrimSpace(action.AdminNodeID),
		AdminMethod:                   strings.TrimSpace(action.AdminMethod),
		AdminPath:                     strings.TrimSpace(action.AdminPath),
		SeedManifestPath:              strings.TrimSpace(action.SeedManifestPath),
		SeedContentRoot:               strings.TrimSpace(action.SeedContentRoot),
		SeedArtifactTargetRoot:        strings.TrimSpace(action.SeedArtifactTargetRoot),
		SeedArtifactLocation:          strings.TrimSpace(action.SeedArtifactLocation),
		SeedArtifactGeneration:        strings.TrimSpace(action.SeedArtifactGeneration),
		SeedArtifactRetainGenerations: action.SeedArtifactRetainGenerations,
		SeedArtifactCaptureRoot:       strings.TrimSpace(action.SeedArtifactCaptureRoot),
		SeedCaptureReceiptPath:        strings.TrimSpace(action.SeedCaptureReceiptPath),
		SeedCaptureReceiptSHA256:      strings.TrimSpace(action.SeedCaptureReceiptSHA256),
		TopologyID:                    strings.TrimSpace(action.TopologyID),
		TopologyGeneration:            action.TopologyGeneration,
		TopologyNodeID:                strings.TrimSpace(action.TopologyNodeID),
		TargetPVCName:                 strings.TrimSpace(action.TargetPVCName),
		TargetPVCUID:                  strings.TrimSpace(action.TargetPVCUID),
		TargetLocalNodeID:             action.TargetLocalNodeID,
		TargetReplicaID:               action.TargetReplicaID,
		RetryGeneration:               action.RetryGeneration,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		panic(fmt.Sprintf("marshal HA operation identity: %v", err))
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("haop-v2-%x", digest[:])
}

func haLegacyPlannedActionOperationID(action antflyv1.HAPlannedActionStatus) string {
	identity := struct {
		Version                       int    `json:"version"`
		Kind                          string `json:"kind"`
		Executor                      string `json:"executor"`
		StandbyName                   string `json:"standby_name"`
		SlotName                      string `json:"slot_name"`
		RouteFrom                     string `json:"route_from"`
		RouteTo                       string `json:"route_to"`
		FenceAuthority                string `json:"fence_authority"`
		FenceHolder                   string `json:"fence_holder"`
		FenceGeneration               uint64 `json:"fence_generation"`
		AdminURL                      string `json:"admin_url"`
		AdminNodeID                   string `json:"admin_node_id"`
		AdminMethod                   string `json:"admin_method"`
		AdminPath                     string `json:"admin_path"`
		SeedManifestPath              string `json:"seed_manifest_path"`
		SeedContentRoot               string `json:"seed_content_root"`
		SeedArtifactTargetRoot        string `json:"seed_artifact_target_root"`
		SeedArtifactLocation          string `json:"seed_artifact_location"`
		SeedArtifactGeneration        string `json:"seed_artifact_generation"`
		SeedArtifactRetainGenerations int32  `json:"seed_artifact_retain_generations"`
		RetryGeneration               int64  `json:"retry_generation"`
	}{
		Version:                       1,
		Kind:                          strings.TrimSpace(action.Kind),
		Executor:                      strings.TrimSpace(action.Executor),
		StandbyName:                   strings.TrimSpace(action.StandbyName),
		SlotName:                      strings.TrimSpace(action.SlotName),
		RouteFrom:                     strings.TrimSpace(action.RouteFrom),
		RouteTo:                       strings.TrimSpace(action.RouteTo),
		FenceAuthority:                strings.TrimSpace(string(action.FenceAuthority)),
		FenceHolder:                   strings.TrimSpace(action.FenceHolder),
		FenceGeneration:               action.FenceGeneration,
		AdminURL:                      strings.TrimSpace(action.AdminURL),
		AdminNodeID:                   strings.TrimSpace(action.AdminNodeID),
		AdminMethod:                   strings.TrimSpace(action.AdminMethod),
		AdminPath:                     strings.TrimSpace(action.AdminPath),
		SeedManifestPath:              strings.TrimSpace(action.SeedManifestPath),
		SeedContentRoot:               strings.TrimSpace(action.SeedContentRoot),
		SeedArtifactTargetRoot:        strings.TrimSpace(action.SeedArtifactTargetRoot),
		SeedArtifactLocation:          strings.TrimSpace(action.SeedArtifactLocation),
		SeedArtifactGeneration:        strings.TrimSpace(action.SeedArtifactGeneration),
		SeedArtifactRetainGenerations: action.SeedArtifactRetainGenerations,
		RetryGeneration:               action.RetryGeneration,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		panic(fmt.Sprintf("marshal legacy HA operation identity: %v", err))
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("haop-v1-%x", digest[:])
}

func haSamePlannedActionIdentity(a antflyv1.HAPlannedActionStatus, b antflyv1.HAPlannedActionStatus) bool {
	aID := strings.TrimSpace(a.OperationID)
	if aID == "" {
		aID = haPlannedActionOperationID(a)
	}
	bID := strings.TrimSpace(b.OperationID)
	if bID == "" {
		bID = haPlannedActionOperationID(b)
	}
	return aID == bID
}

func haFormerPrimaryDemotePreserveAllowed(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return false
	}
	if standbyName := strings.TrimSpace(action.StandbyName); standbyName != "" &&
		standbyName != strings.TrimSpace(promotion.OldPrimaryID) {
		return false
	}
	if action.FenceGeneration != 0 && action.FenceGeneration != promotion.FenceGeneration {
		return false
	}
	return haFormerPrimaryFenced(status, promotion)
}

func haSamePlannedActionOperation(a antflyv1.HAPlannedActionStatus, b antflyv1.HAPlannedActionStatus) bool {
	return a.Kind == b.Kind &&
		a.Phase == b.Phase &&
		a.Executor == b.Executor &&
		a.DependsOn == b.DependsOn &&
		a.StandbyName == b.StandbyName &&
		a.SlotName == b.SlotName &&
		a.TargetLSN == b.TargetLSN &&
		a.ObservedLSN == b.ObservedLSN &&
		a.RetainedFromLSN == b.RetainedFromLSN &&
		a.RouteFrom == b.RouteFrom &&
		a.RouteTo == b.RouteTo &&
		a.FenceAuthority == b.FenceAuthority &&
		a.FenceHolder == b.FenceHolder &&
		a.FenceGeneration == b.FenceGeneration &&
		a.FenceReason == b.FenceReason &&
		a.AdminURL == b.AdminURL &&
		a.AdminNodeID == b.AdminNodeID &&
		a.AdminMethod == b.AdminMethod &&
		a.AdminPath == b.AdminPath &&
		a.SeedManifestPath == b.SeedManifestPath &&
		a.SeedContentRoot == b.SeedContentRoot &&
		a.SeedArtifactTargetRoot == b.SeedArtifactTargetRoot &&
		a.SeedArtifactLocation == b.SeedArtifactLocation &&
		a.SeedArtifactGeneration == b.SeedArtifactGeneration &&
		a.SeedArtifactRetainGenerations == b.SeedArtifactRetainGenerations &&
		a.SeedArtifactCaptureRoot == b.SeedArtifactCaptureRoot &&
		a.SeedCaptureReceiptPath == b.SeedCaptureReceiptPath &&
		a.SeedCaptureReceiptSHA256 == b.SeedCaptureReceiptSHA256 &&
		slices.Equal(a.SeedArtifactProtectedGenerations, b.SeedArtifactProtectedGenerations) &&
		a.TopologyID == b.TopologyID &&
		a.TopologyGeneration == b.TopologyGeneration &&
		a.TopologyNodeID == b.TopologyNodeID &&
		a.SourcePVCName == b.SourcePVCName &&
		a.SourcePVCUID == b.SourcePVCUID &&
		a.TargetPVCName == b.TargetPVCName &&
		a.TargetPVCUID == b.TargetPVCUID &&
		a.TargetLocalNodeID == b.TargetLocalNodeID &&
		a.TargetReplicaID == b.TargetReplicaID &&
		a.Reason == b.Reason &&
		haSameAdminCommandHint(a, b)
}

func haSameAdminCommandHint(a antflyv1.HAPlannedActionStatus, b antflyv1.HAPlannedActionStatus) bool {
	if haPlannedActionUsesTypedAdminAPI(a) && haPlannedActionUsesTypedAdminAPI(b) {
		return true
	}
	return slices.Equal(a.AdminCommand, b.AdminCommand)
}

func haPlannedActionUsesTypedAdminAPI(action antflyv1.HAPlannedActionStatus) bool {
	if action.Executor == string(haActionExecutorCLIJob) {
		return false
	}
	return haPlannedActionSupportsDirectAdminAPI(haActionKind(action.Kind)) &&
		strings.TrimSpace(action.AdminMethod) != "" &&
		strings.TrimSpace(action.AdminPath) != ""
}

func parseHAOperatorPlanTable(body string) (haOperatorPlanTable, error) {
	lines := parseHATableLines(body)
	if result := strings.TrimSpace(lines["result"]); result != "" && result != "operator_plan" {
		return haOperatorPlanTable{}, fmt.Errorf("unexpected HA operator plan result %q", result)
	}

	var parsed haOperatorPlanTable
	parsed.AutomaticPromotionAllowed, _ = parseHAResultBool(lines, "automatic_promotion_allowed")
	parsed.DesiredStandbyCount = haParseInt32Line(lines, "desired_standby_count")
	parsed.HealthyStandbyCount = haParseInt32Line(lines, "healthy_standby_count")
	parsed.UnhealthyStandbyCount = haParseInt32Line(lines, "unhealthy_standby_count")
	parsed.LaggingStandbyCount = haParseInt32Line(lines, "lagging_standby_count")
	parsed.ReseedRequiredCount = haParseInt32Line(lines, "reseed_required_count")

	actionCount, ok := parseHAResultUint(lines, "action_count")
	if !ok {
		return haOperatorPlanTable{}, fmt.Errorf("missing action_count")
	}
	if actionCount == 0 {
		return parsed, nil
	}
	parsed.Actions = make([]antflyv1.HAPlannedActionStatus, 0, actionCount)
	for i := uint64(0); i < actionCount; i++ {
		action, err := parseHAOperatorPlanAction(lines, i)
		if err != nil {
			return haOperatorPlanTable{}, err
		}
		parsed.Actions = append(parsed.Actions, action)
	}
	return parsed, nil
}

func parseHAOperatorPlanAction(lines map[string]string, idx uint64) (antflyv1.HAPlannedActionStatus, error) {
	prefix := fmt.Sprintf("actions.%d.", idx)
	kind, ok := haCLIActionKind(strings.TrimSpace(lines[prefix+"kind"]))
	if !ok {
		return antflyv1.HAPlannedActionStatus{}, fmt.Errorf("missing or unknown HA operator action kind at index %d", idx)
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind:             string(kind),
		Phase:            string(haPlannedActionPhase(kind)),
		Executor:         string(haPlannedActionExecutor(kind)),
		DependsOn:        haCLIOptionalActionKind(lines[prefix+"depends_on"]),
		StandbyName:      strings.TrimSpace(lines[prefix+"standby_name"]),
		SlotName:         strings.TrimSpace(lines[prefix+"slot_name"]),
		RouteFrom:        strings.TrimSpace(lines[prefix+"route_from"]),
		RouteTo:          strings.TrimSpace(lines[prefix+"route_to"]),
		FenceAuthority:   haCLIFencingAuthority(lines[prefix+"fence_authority"]),
		FenceHolder:      strings.TrimSpace(lines[prefix+"fence_holder"]),
		FenceReason:      strings.TrimSpace(lines[prefix+"fence_reason"]),
		AdminURL:         strings.TrimSpace(lines[prefix+"admin_url"]),
		AdminNodeID:      strings.TrimSpace(lines[prefix+"admin_node_id"]),
		AdminMethod:      strings.TrimSpace(lines[prefix+"admin_method"]),
		AdminPath:        strings.TrimSpace(lines[prefix+"admin_path"]),
		SeedManifestPath: strings.TrimSpace(lines[prefix+"seed_manifest_path"]),
		SeedContentRoot:  strings.TrimSpace(lines[prefix+"seed_content_root"]),
		Reason:           strings.TrimSpace(lines[prefix+"reason"]),
	}
	if phase, ok := haCLIActionPhase(lines[prefix+"phase"]); ok {
		action.Phase = string(phase)
	}
	if executor, ok := haCLIActionExecutor(lines[prefix+"executor"]); ok {
		action.Executor = string(executor)
	}
	action.TargetLSN, _ = parseHAResultUint(lines, prefix+"target_lsn")
	action.ObservedLSN, _ = parseHAResultUint(lines, prefix+"observed_lsn")
	action.RetainedFromLSN, _ = parseHAResultUint(lines, prefix+"retained_from_lsn")
	action.FenceGeneration, _ = parseHAResultUint(lines, prefix+"fence_generation")
	if action.AdminMethod == "" || action.AdminPath == "" {
		derivedMethod, derivedPath := haAdminOperation(haPlannedAction{
			Kind:             kind,
			StandbyName:      action.StandbyName,
			SlotName:         action.SlotName,
			SeedManifestPath: action.SeedManifestPath,
		})
		if action.AdminMethod == "" {
			action.AdminMethod = derivedMethod
		}
		if action.AdminPath == "" {
			action.AdminPath = derivedPath
		}
	}
	return action, nil
}

func haParseInt32Line(lines map[string]string, key string) int32 {
	value, ok := parseHAResultUint(lines, key)
	if !ok || value > uint64(^uint32(0)>>1) {
		return 0
	}
	return int32(value)
}

func haCLIOptionalActionKind(raw string) string {
	kind, ok := haCLIActionKind(strings.TrimSpace(raw))
	if !ok {
		return ""
	}
	return string(kind)
}

func haCLIActionKind(raw string) (haActionKind, bool) {
	switch raw {
	case "create_slot":
		return haActionCreateSlot, true
	case "resume_slot":
		return haActionResumeSlot, true
	case "pause_slot":
		return haActionPauseSlot, true
	case "drop_slot":
		return haActionDropSlot, true
	case "seed_standby":
		return haActionSeedStandby, true
	case "finish_standby_seed":
		return haActionFinishStandbySeed, true
	case "bootstrap_standby_seed":
		return haActionBootstrapStandbySeed, true
	case "mark_reseed":
		return haActionMarkReseed, true
	case "acquire_fence":
		return haActionAcquireFence, true
	case "fence_former_primary":
		return haActionFenceFormerPrimary, true
	case "isolate_former_primary":
		return haActionIsolateFormerPrimary, true
	case "assess_promotion", "promotion_assess":
		return haActionAssessPromotion, true
	case "promote_standby":
		return haActionPromoteStandby, true
	case "update_primary_endpoint":
		return haActionUpdatePrimaryRoute, true
	case "demote_former_primary":
		return haActionDemoteFormerPrimary, true
	case "rewind_former_primary":
		return haActionRewindFormerPrimary, true
	case "reseed_former_primary":
		return haActionReseedFormerPrimary, true
	default:
		return "", false
	}
}

func haCLIActionPhase(raw string) (haActionPhase, bool) {
	switch strings.TrimSpace(raw) {
	case "reconcile":
		return haActionPhaseReconcile, true
	case "fence":
		return haActionPhaseFence, true
	case "promote":
		return haActionPhasePromote, true
	case "route":
		return haActionPhaseRoute, true
	case "rejoin":
		return haActionPhaseRejoin, true
	default:
		return "", false
	}
}

func haCLIActionExecutor(raw string) (haActionExecutor, bool) {
	switch strings.TrimSpace(raw) {
	case "admin_api":
		return haActionExecutorAdminAPI, true
	case "admin_command":
		return haActionExecutorAdminAPI, true
	case "cli_job":
		return haActionExecutorCLIJob, true
	case "controller_action":
		return haActionExecutorControllerAction, true
	default:
		return "", false
	}
}

func haCLIFencingAuthority(raw string) antflyv1.HAFencingAuthority {
	switch strings.TrimSpace(raw) {
	case "none":
		return antflyv1.HAFencingAuthorityNone
	case "kubernetes_lease":
		return antflyv1.HAFencingAuthorityKubernetesLease
	case "storage_fence":
		return antflyv1.HAFencingAuthorityStorageFence
	case "metadata_raft":
		return antflyv1.HAFencingAuthorityMetadataRaft
	case "external":
		return antflyv1.HAFencingAuthorityExternal
	default:
		return ""
	}
}

func haPlannedActionPhase(kind haActionKind) haActionPhase {
	switch kind {
	case haActionAcquireFence, haActionFenceFormerPrimary, haActionIsolateFormerPrimary:
		return haActionPhaseFence
	case haActionAssessPromotion, haActionPromoteStandby:
		return haActionPhasePromote
	case haActionUpdatePrimaryRoute:
		return haActionPhaseRoute
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		return haActionPhaseRejoin
	case haActionCaptureSeedArtifact, haActionPublishSeedArtifact, haActionGCSourceSeedGenerations, haActionRestoreSeedArtifact, haActionActivateSeedArtifact, haActionActivateSeededSlot, haActionGCTargetSeedGenerations, haActionPruneSeedArtifacts:
		return haActionPhaseSeed
	default:
		return haActionPhaseReconcile
	}
}

func haPlannedActionExecutor(kind haActionKind) haActionExecutor {
	if kind == haActionUpdatePrimaryRoute || kind == haActionIsolateFormerPrimary {
		return haActionExecutorControllerAction
	}
	if haPlannedActionKindIsPortableArtifact(kind) {
		return haActionExecutorCLIJob
	}
	return haActionExecutorAdminAPI
}

func haPlannedActionKindIsPortableArtifact(kind haActionKind) bool {
	return kind == haActionPublishSeedArtifact ||
		kind == haActionGCSourceSeedGenerations ||
		kind == haActionRestoreSeedArtifact ||
		kind == haActionActivateSeedArtifact ||
		kind == haActionGCTargetSeedGenerations ||
		kind == haActionPruneSeedArtifacts
}

func haPlannedActionKindUsesSeedSourceAuthority(kind haActionKind) bool {
	return kind == haActionCaptureSeedArtifact ||
		haPlannedActionKindIsPortableArtifact(kind) ||
		kind == haActionActivateSeededSlot
}

func haAdminCommand(action haPlannedAction, identity *antflyv1.HAReplicationIdentitySpec, status *antflyv1.HAStatus) []string {
	switch action.Kind {
	case haActionCreateSlot:
		slotName := action.SlotName
		if slotName == "" {
			slotName = action.StandbyName
		}
		if slotName == "" {
			return nil
		}
		command := []string{"slot", "create", "--slot", slotName}
		if action.TargetLSN > 0 {
			command = append(command, "--initial-lsn", strconv.FormatUint(action.TargetLSN, 10))
		}
		return command
	case haActionResumeSlot:
		return haSlotLifecycleCommand("resume", action)
	case haActionPauseSlot:
		return haSlotLifecycleCommand("pause", action)
	case haActionDropSlot:
		return haSlotLifecycleCommand("drop", action)
	case haActionSeedStandby, haActionMarkReseed:
		slotName := action.SlotName
		if slotName == "" {
			slotName = action.StandbyName
		}
		if slotName == "" || action.TargetLSN == 0 {
			return nil
		}
		return []string{"seed", "begin", "--slot", slotName, "--manifest-id", fmt.Sprintf("base-%s-%d", slotName, action.TargetLSN)}
	case haActionFinishStandbySeed:
		if action.SeedManifestPath == "" {
			return nil
		}
		return []string{"seed", "finish", "--manifest", action.SeedManifestPath}
	case haActionPublishSeedArtifact:
		if action.SeedArtifactLocation == "" || action.SeedArtifactGeneration == "" ||
			action.SeedManifestPath == "" || action.SeedContentRoot == "" || action.SlotName == "" ||
			action.SeedCaptureReceiptPath == "" || !isLowerHexDigest(action.SeedCaptureReceiptSHA256) {
			return nil
		}
		command := []string{
			"artifact", "publish",
			"--location", action.SeedArtifactLocation,
			"--generation", action.SeedArtifactGeneration,
			"--slot", action.SlotName,
			"--manifest", action.SeedManifestPath,
			"--content-root", action.SeedContentRoot,
			"--capture-receipt", action.SeedCaptureReceiptPath,
			"--capture-receipt-sha256", action.SeedCaptureReceiptSHA256,
		}
		return haAppendArtifactTopologyBinding(command, action)
	case haActionGCSourceSeedGenerations:
		if action.SeedArtifactLocation == "" || action.SeedArtifactGeneration == "" ||
			action.SlotName == "" || action.SeedArtifactCaptureRoot == "" || action.SeedArtifactRetention <= 0 {
			return nil
		}
		command := []string{
			"artifact", "gc-source",
			"--location", action.SeedArtifactLocation,
			"--generation", action.SeedArtifactGeneration,
			"--slot", action.SlotName,
			"--capture-root", action.SeedArtifactCaptureRoot,
			"--retain-generations", strconv.FormatInt(int64(action.SeedArtifactRetention), 10),
		}
		for _, generation := range action.SeedArtifactProtectedGenerations {
			command = append(command, "--protect-generation", generation)
		}
		return command
	case haActionRestoreSeedArtifact:
		if identity == nil || action.SeedArtifactLocation == "" || action.SeedArtifactGeneration == "" ||
			action.SeedContentRoot == "" || action.SlotName == "" ||
			!isLowerHexDigest(action.SeedCaptureReceiptSHA256) {
			return nil
		}
		command := []string{
			"artifact", "restore",
			"--location", action.SeedArtifactLocation,
			"--generation", action.SeedArtifactGeneration,
			"--slot", action.SlotName,
			"--staging-root", action.SeedContentRoot,
			"--ha-cluster-id", strconv.FormatUint(identity.ClusterID, 10),
			"--ha-shard-id", strconv.FormatUint(identity.ShardID, 10),
			"--ha-table-id", strconv.FormatUint(identity.TableID, 10),
			"--ha-timeline-id", strconv.FormatUint(identity.TimelineID, 10),
			"--ha-epoch", strconv.FormatUint(identity.Epoch, 10),
			"--minimum-checkpoint-lsn", strconv.FormatUint(action.TargetLSN, 10),
			"--capture-receipt-sha256", action.SeedCaptureReceiptSHA256,
		}
		return haAppendArtifactTopologyBinding(command, action)
	case haActionActivateSeedArtifact:
		if identity == nil || action.SeedArtifactGeneration == "" || action.SeedContentRoot == "" ||
			action.SeedArtifactTargetRoot == "" || action.SlotName == "" ||
			!isLowerHexDigest(action.SeedCaptureReceiptSHA256) ||
			action.TargetLocalNodeID == 0 || action.TargetReplicaID == 0 {
			return nil
		}
		command := []string{
			"artifact", "activate",
			"--generation", action.SeedArtifactGeneration,
			"--slot", action.SlotName,
			"--staging-root", action.SeedContentRoot,
			"--target-root", action.SeedArtifactTargetRoot,
			"--ha-cluster-id", strconv.FormatUint(identity.ClusterID, 10),
			"--ha-shard-id", strconv.FormatUint(identity.ShardID, 10),
			"--ha-table-id", strconv.FormatUint(identity.TableID, 10),
			"--ha-timeline-id", strconv.FormatUint(identity.TimelineID, 10),
			"--ha-epoch", strconv.FormatUint(identity.Epoch, 10),
			"--minimum-checkpoint-lsn", strconv.FormatUint(action.TargetLSN, 10),
			"--capture-receipt-sha256", action.SeedCaptureReceiptSHA256,
		}
		command = haAppendArtifactTopologyBinding(command, action)
		if command == nil {
			return nil
		}
		return append(command,
			"--target-local-node-id", strconv.FormatUint(action.TargetLocalNodeID, 10),
			"--target-replica-id", strconv.FormatUint(action.TargetReplicaID, 10),
		)
	case haActionGCTargetSeedGenerations:
		if action.SeedArtifactGeneration == "" || action.SeedArtifactTargetRoot == "" ||
			action.SlotName == "" || action.SeedArtifactRetention <= 0 {
			return nil
		}
		command := []string{
			"artifact", "gc-target",
			"--target-root", action.SeedArtifactTargetRoot,
			"--slot-activation-receipt", haSeededSlotActivationReceiptPath,
			"--retain-generations", strconv.FormatInt(int64(action.SeedArtifactRetention), 10),
		}
		for _, generation := range action.SeedArtifactProtectedGenerations {
			command = append(command, "--protect-generation", generation)
		}
		return command
	case haActionPruneSeedArtifacts:
		if action.SeedArtifactLocation == "" || action.SeedArtifactGeneration == "" ||
			action.SeedArtifactRetention <= 0 || action.SlotName == "" {
			return nil
		}
		return []string{
			"artifact", "prune",
			"--location", action.SeedArtifactLocation,
			"--generation", action.SeedArtifactGeneration,
			"--slot", action.SlotName,
			"--retain-generations", strconv.FormatInt(int64(action.SeedArtifactRetention), 10),
		}
	case haActionBootstrapStandbySeed:
		if action.SeedManifestPath == "" {
			return nil
		}
		args := []string{"seed", "bootstrap", "--manifest", action.SeedManifestPath}
		if action.SeedContentRoot != "" {
			args = append(args, "--content-root", action.SeedContentRoot)
		}
		return args
	case haActionAssessPromotion:
		return []string{"promote", "assess", "--current-fence"}
	case haActionPromoteStandby:
		return []string{"promote", "--current-fence"}
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		return haFormerPrimaryAdminCommand(action, identity, status)
	case haActionAcquireFence:
		if identity == nil || identity.CurrentPrimaryID == "" || action.StandbyName == "" {
			return nil
		}
		reason := action.Reason
		if strings.TrimSpace(reason) == "" {
			reason = action.FenceReason
		}
		return []string{
			"fence", "acquire",
			"--cluster-id", strconv.FormatUint(identity.ClusterID, 10),
			"--shard-id", strconv.FormatUint(identity.ShardID, 10),
			"--table-id", strconv.FormatUint(identity.TableID, 10),
			"--timeline-id", strconv.FormatUint(identity.TimelineID, 10),
			"--epoch", strconv.FormatUint(identity.Epoch, 10),
			"--old-primary-id", identity.CurrentPrimaryID,
			"--promoted-node-id", action.StandbyName,
			"--new-timeline-id", strconv.FormatUint(identity.TimelineID+1, 10),
			"--new-epoch", strconv.FormatUint(identity.Epoch+1, 10),
			"--generation", strconv.FormatUint(action.FenceGeneration, 10),
			"--required-lsn", strconv.FormatUint(action.TargetLSN, 10),
			"--observed-lsn", strconv.FormatUint(action.TargetLSN, 10),
			"--reason", reason,
		}
	case haActionFenceFormerPrimary:
		promotion := haPromotionReceipt(status)
		if identity == nil {
			return nil
		}
		if promotion == nil {
			oldPrimaryID := strings.TrimSpace(action.StandbyName)
			promotedNodeID := strings.TrimSpace(action.RouteTo)
			if oldPrimaryID == "" || promotedNodeID == "" || action.TargetLSN == 0 {
				return nil
			}
			reason := action.FenceReason
			if strings.TrimSpace(reason) == "" {
				reason = action.Reason
			}
			return []string{
				"fence", "acquire",
				"--cluster-id", strconv.FormatUint(identity.ClusterID, 10),
				"--shard-id", strconv.FormatUint(identity.ShardID, 10),
				"--table-id", strconv.FormatUint(identity.TableID, 10),
				"--timeline-id", strconv.FormatUint(identity.TimelineID, 10),
				"--epoch", strconv.FormatUint(identity.Epoch, 10),
				"--old-primary-id", oldPrimaryID,
				"--promoted-node-id", promotedNodeID,
				"--new-timeline-id", strconv.FormatUint(identity.TimelineID+1, 10),
				"--new-epoch", strconv.FormatUint(identity.Epoch+1, 10),
				"--generation", strconv.FormatUint(action.FenceGeneration, 10),
				"--required-lsn", strconv.FormatUint(action.TargetLSN, 10),
				"--observed-lsn", strconv.FormatUint(action.TargetLSN, 10),
				"--reason", reason,
			}
		}
		return []string{
			"fence", "acquire",
			"--cluster-id", strconv.FormatUint(identity.ClusterID, 10),
			"--shard-id", strconv.FormatUint(identity.ShardID, 10),
			"--table-id", strconv.FormatUint(identity.TableID, 10),
			"--timeline-id", strconv.FormatUint(promotion.ParentTimelineID, 10),
			"--epoch", strconv.FormatUint(promotion.ParentEpoch, 10),
			"--old-primary-id", promotion.OldPrimaryID,
			"--promoted-node-id", promotion.PromotedStandbyID,
			"--new-timeline-id", strconv.FormatUint(promotion.NewTimelineID, 10),
			"--new-epoch", strconv.FormatUint(promotion.NewEpoch, 10),
			"--generation", strconv.FormatUint(promotion.FenceGeneration, 10),
			"--required-lsn", strconv.FormatUint(haPromotionRequiredLSN(promotion), 10),
			"--observed-lsn", strconv.FormatUint(haPromotionObservedLSN(promotion), 10),
			"--reason", promotion.FenceReason,
		}
	default:
		return nil
	}
}

func haAppendArtifactTopologyBinding(command []string, action haPlannedAction) []string {
	if strings.TrimSpace(action.TopologyID) == "" || action.TopologyGeneration <= 0 ||
		strings.TrimSpace(action.TopologyNodeID) == "" || strings.TrimSpace(action.TargetPVCName) == "" ||
		strings.TrimSpace(action.TargetPVCUID) == "" {
		return nil
	}
	return append(command,
		"--topology-id", action.TopologyID,
		"--topology-generation", strconv.FormatInt(action.TopologyGeneration, 10),
		"--node-id", action.TopologyNodeID,
		"--target-pvc-name", action.TargetPVCName,
		"--target-pvc-uid", action.TargetPVCUID,
	)
}

func haSlotLifecycleCommand(operation string, action haPlannedAction) []string {
	slotName := action.SlotName
	if slotName == "" {
		slotName = action.StandbyName
	}
	if slotName == "" {
		return nil
	}
	return []string{"slot", operation, "--slot", slotName}
}

func haFormerPrimaryAdminCommand(action haPlannedAction, identity *antflyv1.HAReplicationIdentitySpec, status *antflyv1.HAStatus) []string {
	if identity == nil || action.StandbyName == "" {
		return nil
	}
	lastLSN := action.ObservedLSN
	if lastLSN == 0 {
		lastLSN = action.TargetLSN
	}
	args := []string{
		"rejoin", haFormerPrimaryRejoinSubcommand(action.Kind),
		"--node-id", action.StandbyName,
		"--cluster-id", strconv.FormatUint(identity.ClusterID, 10),
		"--shard-id", strconv.FormatUint(identity.ShardID, 10),
		"--table-id", strconv.FormatUint(identity.TableID, 10),
		"--timeline-id", strconv.FormatUint(identity.TimelineID, 10),
		"--epoch", strconv.FormatUint(identity.Epoch, 10),
		"--last-lsn", strconv.FormatUint(lastLSN, 10),
		"--retained-from-lsn", strconv.FormatUint(action.RetainedFromLSN, 10),
	}
	if action.Kind == haActionDemoteFormerPrimary {
		return args
	}

	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return nil
	}
	args = append(args,
		"--fence-old-primary-id", promotion.OldPrimaryID,
		"--fence-promoted-node-id", promotion.PromotedStandbyID,
		"--fence-parent-timeline-id", strconv.FormatUint(promotion.ParentTimelineID, 10),
		"--fence-parent-epoch", strconv.FormatUint(promotion.ParentEpoch, 10),
		"--fence-new-timeline-id", strconv.FormatUint(promotion.NewTimelineID, 10),
		"--fence-new-epoch", strconv.FormatUint(promotion.NewEpoch, 10),
		"--fence-required-lsn", strconv.FormatUint(haPromotionRequiredLSN(promotion), 10),
		"--fence-observed-lsn", strconv.FormatUint(haPromotionObservedLSN(promotion), 10),
		"--fence-generation", strconv.FormatUint(promotion.FenceGeneration, 10),
		"--fence-token", promotion.FenceToken,
	)
	if promotion.FenceReason != "" {
		args = append(args, "--fence-reason", promotion.FenceReason)
	}
	if promotion.Forced {
		args = append(args, "--fence-forced")
	}
	return args
}

func haFormerPrimaryRejoinSubcommand(kind haActionKind) string {
	switch kind {
	case haActionRewindFormerPrimary:
		return "rewind"
	case haActionReseedFormerPrimary:
		return "reseed"
	default:
		return "assess"
	}
}

func haPromotionReceipt(status *antflyv1.HAStatus) *antflyv1.HAPromotionStatus {
	if status == nil || status.LastPromotion == nil {
		return nil
	}
	promotion := status.LastPromotion
	if promotion.OldPrimaryID == "" || promotion.PromotedStandbyID == "" ||
		promotion.ParentTimelineID == 0 || promotion.ParentEpoch == 0 ||
		promotion.NewTimelineID == 0 || promotion.NewEpoch == 0 ||
		haPromotionRequiredLSN(promotion) == 0 || haPromotionObservedLSN(promotion) == 0 ||
		promotion.FenceAuthority == "" || promotion.FenceAuthority == antflyv1.HAFencingAuthorityNone ||
		promotion.FenceGeneration == 0 || promotion.FenceToken == "" {
		return nil
	}
	return promotion
}

func haPromotionRequiredLSN(promotion *antflyv1.HAPromotionStatus) uint64 {
	if promotion.RequiredLSN != 0 {
		return promotion.RequiredLSN
	}
	return promotion.SwitchLSN
}

func haPromotionObservedLSN(promotion *antflyv1.HAPromotionStatus) uint64 {
	if promotion.ObservedLSN != 0 {
		return promotion.ObservedLSN
	}
	return promotion.SwitchLSN
}

func haAdminURL(action haPlannedAction, ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) string {
	if ha == nil {
		return ""
	}
	switch action.Kind {
	case haActionCreateSlot, haActionResumeSlot, haActionPauseSlot, haActionDropSlot, haActionSeedStandby, haActionFinishStandbySeed, haActionCaptureSeedArtifact, haActionActivateSeededSlot, haActionMarkReseed:
		return haCurrentPrimaryActionURL(ha, status)
	case haActionAcquireFence:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionFenceFormerPrimary:
		return haFormerPrimaryAdminURL(ha, action)
	case haActionBootstrapStandbySeed:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionAssessPromotion, haActionPromoteStandby:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary:
		return haFormerPrimaryAdminURL(ha, action)
	case haActionReseedFormerPrimary:
		// Reseed is a current-primary operation: it marks the old primary's
		// replication slot as needing a fresh base backup. The following
		// SeedStandby/BootstrapStandby actions rebuild the former primary.
		return haCurrentPrimaryActionURL(ha, status)
	default:
		return ""
	}
}

func haFormerPrimaryAdminURL(ha *antflyv1.HighAvailabilitySpec, action haPlannedAction) string {
	if url := haStandbyAdminURL(ha, action.StandbyName); url != "" {
		return url
	}
	if strings.TrimSpace(action.RouteFrom) == "" || strings.TrimSpace(action.RouteFrom) != strings.TrimSpace(action.StandbyName) {
		return ""
	}
	if ha != nil && ha.Admin != nil {
		return haPrimaryActionURL(ha.Admin)
	}
	return ""
}

func haCurrentPrimaryAdminURL(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) string {
	if promoted := haPromotedPrimaryNodeID(status); promoted != "" {
		if url := haStandbyAdminURL(ha, promoted); url != "" {
			return url
		}
		return ""
	}
	if ha != nil && ha.Admin != nil {
		return ha.Admin.PrimaryURL
	}
	return ""
}

func haCurrentPrimaryActionURL(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) string {
	if promoted := haPromotedPrimaryNodeID(status); promoted != "" {
		if url := haStandbyAdminURL(ha, promoted); url != "" {
			return url
		}
		return ""
	}
	if ha != nil && ha.Admin != nil {
		return haPrimaryActionURL(ha.Admin)
	}
	return ""
}

func haPrimaryActionURL(admin *antflyv1.HAAdminSpec) string {
	if admin == nil {
		return ""
	}
	if actionURL := strings.TrimSpace(admin.PrimaryActionURL); actionURL != "" {
		return actionURL
	}
	return admin.PrimaryURL
}

func haAdminNodeID(action haPlannedAction, ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) string {
	switch action.Kind {
	case haActionCreateSlot, haActionResumeSlot, haActionPauseSlot, haActionDropSlot, haActionSeedStandby, haActionFinishStandbySeed, haActionCaptureSeedArtifact, haActionActivateSeededSlot, haActionMarkReseed:
		return haCurrentPrimaryNodeID(ha, status)
	case haActionAcquireFence, haActionBootstrapStandbySeed, haActionAssessPromotion, haActionPromoteStandby:
		return strings.TrimSpace(action.StandbyName)
	case haActionFenceFormerPrimary:
		return strings.TrimSpace(action.StandbyName)
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary:
		return strings.TrimSpace(action.StandbyName)
	case haActionReseedFormerPrimary:
		return haCurrentPrimaryNodeID(ha, status)
	default:
		return ""
	}
}

func haCurrentPrimaryNodeID(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) string {
	if promoted := haPromotedPrimaryNodeID(status); promoted != "" {
		return promoted
	}
	if identity := haReplicationIdentity(ha); identity != nil {
		return strings.TrimSpace(identity.CurrentPrimaryID)
	}
	return ""
}

func haPromotedPrimaryNodeID(status *antflyv1.HAStatus) string {
	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return ""
	}
	return strings.TrimSpace(promotion.PromotedStandbyID)
}

func haAdminOperation(action haPlannedAction) (string, string) {
	operation := adminsdk.HAOperation{}
	ok := true
	switch action.Kind {
	case haActionCreateSlot:
		operation = adminsdk.HACreateReplicationSlotOperation()
	case haActionResumeSlot:
		operation, ok = adminsdk.HAResumeReplicationSlotOperation(haPlannedActionSlotName(action))
	case haActionPauseSlot:
		operation, ok = adminsdk.HAPauseReplicationSlotOperation(haPlannedActionSlotName(action))
	case haActionDropSlot:
		operation, ok = adminsdk.HADropReplicationSlotOperation(haPlannedActionSlotName(action))
	case haActionSeedStandby, haActionMarkReseed:
		operation = adminsdk.HABeginBaseBackupOperation()
	case haActionFinishStandbySeed:
		operation = adminsdk.HAFinishBaseBackupOperation()
	case haActionCaptureSeedArtifact:
		operation = adminsdk.HASeedCaptureOperation()
	case haActionActivateSeededSlot:
		operation = adminsdk.HAActivateSeededSlotOperation()
	case haActionBootstrapStandbySeed:
		operation = adminsdk.HABootstrapStandbyOperation()
	case haActionAcquireFence:
		operation = adminsdk.HAAcquireFenceOperation()
	case haActionFenceFormerPrimary:
		operation = adminsdk.HAAcquireFenceOperation()
	case haActionAssessPromotion:
		operation = adminsdk.HAAssessPromotionOperation()
	case haActionPromoteStandby:
		operation = adminsdk.HAPromoteWithCurrentFenceOperation()
	case haActionDemoteFormerPrimary:
		operation = adminsdk.HAAssessRejoinOperation()
	case haActionRewindFormerPrimary:
		operation = adminsdk.HARewindRejoinOperation()
	case haActionReseedFormerPrimary:
		operation = adminsdk.HAReseedRejoinOperation()
	default:
		ok = false
	}
	if !ok {
		return "", ""
	}
	return operation.Method, operation.Path
}

func haPlannedActionSlotName(action haPlannedAction) string {
	if action.SlotName != "" {
		return action.SlotName
	}
	return action.StandbyName
}

func haStandbyAdminURL(ha *antflyv1.HighAvailabilitySpec, standbyName string) string {
	if ha == nil || standbyName == "" {
		return ""
	}
	for _, standby := range ha.Standbys {
		if standby.Name == standbyName {
			return standby.AdminURL
		}
	}
	return ""
}

func haStandbySpecByName(ha *antflyv1.HighAvailabilitySpec, standbyName string) (antflyv1.HAStandbySpec, bool) {
	if ha == nil || standbyName == "" {
		return antflyv1.HAStandbySpec{}, false
	}
	for _, standby := range ha.Standbys {
		if standby.Name == standbyName {
			return standby, true
		}
	}
	return antflyv1.HAStandbySpec{}, false
}

func haReplicationIdentity(ha *antflyv1.HighAvailabilitySpec) *antflyv1.HAReplicationIdentitySpec {
	if ha == nil || ha.Identity == nil {
		return nil
	}
	identity := ha.Identity
	if identity.ClusterID == 0 || identity.TimelineID == 0 ||
		identity.Epoch == 0 || identity.CurrentPrimaryID == "" {
		return nil
	}
	return identity
}

func haSeedCompletionActions(standby antflyv1.HAStandbySpec, slotName string, targetLSN uint64, reason string, dependsOn haActionKind) []haPlannedAction {
	if artifact := standby.SeedArtifact; artifact != nil {
		// Colony first declares the source/target transport, then binds it to
		// observed topology and PVC identity. The unbound descriptor is pending
		// state only: never materialize an executable action chain until every
		// immutable authority field is present.
		if strings.TrimSpace(artifact.TopologyID) == "" || artifact.TopologyGeneration <= 0 ||
			strings.TrimSpace(artifact.NodeID) == "" || strings.TrimSpace(artifact.TargetPVCUID) == "" {
			return nil
		}
		location := strings.TrimSpace(artifact.Location)
		stagingRoot := strings.TrimRight(strings.TrimSpace(artifact.StagingRoot), "/")
		contentRoot := strings.TrimSpace(standby.SeedContentRoot)
		targetRoot := ""
		if artifact.TargetPVC != nil {
			targetMount := strings.TrimRight(strings.TrimSpace(artifact.TargetPVC.MountPath), "/")
			if targetMount != "" {
				targetRoot = targetMount + "/.antfly-ha/active"
			}
		}
		runtimeOwned := strings.TrimSpace(standby.SeedManifestPath) == "" && contentRoot == ""
		if location == "" || stagingRoot == "" || targetRoot == "" || targetRoot == "." ||
			(runtimeOwned && artifact.SourcePVC == nil) {
			return nil
		}
		generation := haSeedArtifactGeneration(standby, slotName, targetLSN)
		retention := artifact.RetainGenerations
		if retention == 0 {
			retention = 2
		}
		actions := make([]haPlannedAction, 0, 8)
		publishDependsOn := haActionFinishStandbySeed
		if runtimeOwned {
			actions = append(actions, haPlannedAction{
				Kind:                   haActionCaptureSeedArtifact,
				DependsOn:              dependsOn,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedArtifactGeneration: generation,
				Reason:                 reason,
			})
			publishDependsOn = haActionCaptureSeedArtifact
		} else {
			if standby.SeedManifestPath == "" || contentRoot == "" {
				return nil
			}
			actions = append(actions, haPlannedAction{
				Kind:             haActionFinishStandbySeed,
				DependsOn:        dependsOn,
				StandbyName:      standby.Name,
				SlotName:         slotName,
				TargetLSN:        targetLSN,
				SeedManifestPath: standby.SeedManifestPath,
				SeedContentRoot:  contentRoot,
				Reason:           reason,
			})
		}
		actions = append(actions,
			haPlannedAction{
				Kind:                   haActionPublishSeedArtifact,
				DependsOn:              publishDependsOn,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedManifestPath:       standby.SeedManifestPath,
				SeedContentRoot:        contentRoot,
				SeedArtifactLocation:   location,
				SeedArtifactGeneration: generation,
				Reason:                 reason,
			},
		)
		restoreDependsOn := haActionPublishSeedArtifact
		if runtimeOwned {
			actions = append(actions, haPlannedAction{
				Kind:                   haActionGCSourceSeedGenerations,
				DependsOn:              haActionPublishSeedArtifact,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedArtifactLocation:   location,
				SeedArtifactGeneration: generation,
				SeedArtifactRetention:  retention,
				Reason:                 reason,
			})
			restoreDependsOn = haActionGCSourceSeedGenerations
		}
		actions = append(actions,
			haPlannedAction{
				Kind:                   haActionRestoreSeedArtifact,
				DependsOn:              restoreDependsOn,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedContentRoot:        stagingRoot,
				SeedArtifactLocation:   location,
				SeedArtifactGeneration: generation,
				Reason:                 reason,
			},
			haPlannedAction{
				Kind:                   haActionActivateSeedArtifact,
				DependsOn:              haActionRestoreSeedArtifact,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedContentRoot:        stagingRoot,
				SeedArtifactTargetRoot: targetRoot,
				SeedArtifactGeneration: generation,
				Reason:                 reason,
			},
			haPlannedAction{
				Kind:                   haActionActivateSeededSlot,
				DependsOn:              haActionActivateSeedArtifact,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedArtifactGeneration: generation,
				Reason:                 reason,
			},
			haPlannedAction{
				Kind:                   haActionGCTargetSeedGenerations,
				DependsOn:              haActionActivateSeededSlot,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedArtifactTargetRoot: targetRoot,
				SeedArtifactGeneration: generation,
				SeedArtifactRetention:  retention,
				Reason:                 reason,
			},
			haPlannedAction{
				Kind:                   haActionPruneSeedArtifacts,
				DependsOn:              haActionGCTargetSeedGenerations,
				StandbyName:            standby.Name,
				SlotName:               slotName,
				TargetLSN:              targetLSN,
				SeedArtifactLocation:   location,
				SeedArtifactGeneration: generation,
				SeedArtifactRetention:  retention,
				Reason:                 reason,
			},
		)
		for i := range actions {
			actions[i].TopologyID = strings.TrimSpace(artifact.TopologyID)
			actions[i].TopologyGeneration = artifact.TopologyGeneration
			actions[i].TopologyNodeID = strings.TrimSpace(artifact.NodeID)
			if artifact.TargetPVC != nil {
				actions[i].TargetPVCName = strings.TrimSpace(artifact.TargetPVC.ClaimName)
			}
			actions[i].TargetPVCUID = strings.TrimSpace(artifact.TargetPVCUID)
		}
		return actions
	}
	if standby.SeedManifestPath == "" {
		return nil
	}
	return []haPlannedAction{
		{
			Kind:             haActionFinishStandbySeed,
			DependsOn:        dependsOn,
			StandbyName:      standby.Name,
			SlotName:         slotName,
			TargetLSN:        targetLSN,
			SeedManifestPath: standby.SeedManifestPath,
			SeedContentRoot:  standby.SeedContentRoot,
			Reason:           reason,
		},
		{
			Kind:             haActionBootstrapStandbySeed,
			DependsOn:        haActionFinishStandbySeed,
			StandbyName:      standby.Name,
			SlotName:         slotName,
			TargetLSN:        targetLSN,
			SeedManifestPath: standby.SeedManifestPath,
			SeedContentRoot:  standby.SeedContentRoot,
			Reason:           reason,
		},
	}
}

func haSeedArtifactGeneration(standby antflyv1.HAStandbySpec, slotName string, targetLSN uint64) string {
	if standby.SeedArtifact == nil {
		return ""
	}
	if generation := strings.TrimSpace(standby.SeedArtifact.Generation); generation != "" {
		return generation
	}
	prefix := strings.TrimSpace(standby.SeedArtifact.GenerationPrefix)
	if prefix == "" {
		prefix = "seed"
	}
	return fmt.Sprintf("%s-%s-%d", prefix, slotName, targetLSN)
}

func haStandbyUsesRuntimeOwnedSeedCapture(standby antflyv1.HAStandbySpec) bool {
	return standby.SeedArtifact != nil &&
		strings.TrimSpace(standby.SeedManifestPath) == "" &&
		strings.TrimSpace(standby.SeedContentRoot) == "" &&
		standby.SeedArtifact.SourcePVC != nil
}

func haAutomaticFailoverFormerPrimaryID(ha *antflyv1.HighAvailabilitySpec) string {
	identity := haReplicationIdentity(ha)
	if identity == nil {
		return ""
	}
	return identity.CurrentPrimaryID
}

func haPrimaryRouteStatus(evaluation haPrimaryRouteEvaluation) antflyv1.HAPrimaryRouteStatus {
	return antflyv1.HAPrimaryRouteStatus{
		ServiceName:     evaluation.ServiceName,
		CurrentTarget:   evaluation.CurrentTarget,
		DesiredTarget:   evaluation.DesiredTarget,
		FenceAuthority:  evaluation.FenceAuthority,
		FenceGeneration: evaluation.FenceGeneration,
		Stale:           evaluation.Stale,
		Action:          evaluation.Action,
		Reason:          evaluation.Reason,
	}
}

func haPrimaryRoutePlannedAction(evaluation haPrimaryRouteEvaluation, status *antflyv1.HAStatus) haPlannedAction {
	if !evaluation.Stale || evaluation.Action != string(haActionUpdatePrimaryRoute) {
		return haPlannedAction{}
	}
	action := haPlannedAction{
		Kind:            haActionUpdatePrimaryRoute,
		RouteFrom:       evaluation.CurrentTarget,
		RouteTo:         evaluation.DesiredTarget,
		FenceAuthority:  evaluation.FenceAuthority,
		FenceGeneration: evaluation.FenceGeneration,
		Reason:          evaluation.Reason,
	}
	if status != nil {
		action.TargetLSN = status.PrimaryLSN
		if action.FenceAuthority == "" {
			action.FenceAuthority = status.Fencing.Authority
		}
		action.FenceHolder = status.Fencing.Holder
		action.FenceReason = haPlannedActionFenceReason(status)
	}
	if evaluation.DesiredTarget != "" && evaluation.DesiredTarget != "primary" {
		action.StandbyName = evaluation.DesiredTarget
	}
	return action
}

func haHasPlannedAction(actions []haPlannedAction, kind haActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

func haFormerPrimaryStatus(evaluation haFormerPrimaryEvaluation) *antflyv1.HAFormerPrimaryStatus {
	if !evaluation.Present {
		return nil
	}
	return &antflyv1.HAFormerPrimaryStatus{
		NodeID:             evaluation.NodeID,
		Fenced:             evaluation.Fenced,
		RejoinRequired:     evaluation.RejoinRequired,
		RewindPossible:     evaluation.RewindPossible,
		ReseedRequired:     evaluation.ReseedRequired,
		Diverged:           evaluation.Diverged,
		ParentTimelineID:   evaluation.ParentTimelineID,
		NewTimelineID:      evaluation.NewTimelineID,
		ObservedTimelineID: evaluation.ObservedTimelineID,
		SwitchLSN:          evaluation.SwitchLSN,
		ObservedLSN:        evaluation.ObservedLSN,
		FenceAuthority:     evaluation.FenceAuthority,
		FenceHolder:        evaluation.FenceHolder,
		FenceGeneration:    evaluation.FenceGeneration,
		Action:             evaluation.Action,
		Reason:             evaluation.Reason,
	}
}

func haFormerPrimaryPlannedAction(evaluation haFormerPrimaryEvaluation, status *antflyv1.HAStatus) haPlannedAction {
	if !evaluation.Present {
		return haPlannedAction{}
	}
	retainedFromLSN := uint64(0)
	targetLSN := evaluation.SwitchLSN
	fenceReason := ""
	if status != nil {
		retainedFromLSN = status.Retention.OldestRestartLSN
		if promotion := haPromotionReceipt(status); promotion != nil {
			fenceReason = promotion.FenceReason
			if forkLSN := haPromotionObservedLSN(promotion); forkLSN > 0 {
				targetLSN = forkLSN
			}
		}
	}
	switch evaluation.Action {
	case string(haActionDemoteFormerPrimary), string(haActionRewindFormerPrimary), string(haActionReseedFormerPrimary):
		return haPlannedAction{
			Kind:            haActionKind(evaluation.Action),
			StandbyName:     evaluation.NodeID,
			RouteFrom:       evaluation.NodeID,
			TargetLSN:       targetLSN,
			ObservedLSN:     evaluation.ObservedLSN,
			RetainedFromLSN: retainedFromLSN,
			FenceAuthority:  evaluation.FenceAuthority,
			FenceHolder:     evaluation.FenceHolder,
			FenceGeneration: evaluation.FenceGeneration,
			FenceReason:     fenceReason,
			Reason:          evaluation.Reason,
		}
	default:
		return haPlannedAction{}
	}
}

func mergeConfiguredStandbys(status *antflyv1.HAStatus, ha *antflyv1.HighAvailabilitySpec) {
	if ha == nil {
		return
	}
	observed := map[string]antflyv1.HAStandbyStatus{}
	for _, standby := range status.Standbys {
		key := standby.Name
		if key == "" {
			key = standby.SlotName
		}
		if key != "" {
			observed[key] = standby
		}
		if standby.SlotName != "" {
			observed[standby.SlotName] = standby
		}
	}
	merged := make([]antflyv1.HAStandbyStatus, 0, len(ha.Standbys))
	promotedPrimaryID := haPromotedPrimaryNodeID(status)
	for _, desired := range ha.Standbys {
		if !standbyDesired(desired) {
			continue
		}
		if promotedPrimaryID != "" &&
			(promotedPrimaryID == strings.TrimSpace(desired.Name) || promotedPrimaryID == strings.TrimSpace(standbySlotName(desired))) {
			continue
		}
		entry, ok := observed[desired.Name]
		if !ok {
			entry = observed[standbySlotName(desired)]
		}
		entry.Name = desired.Name
		if entry.SlotName == "" {
			entry.SlotName = standbySlotName(desired)
		}
		merged = append(merged, entry)
	}
	status.Standbys = merged
}

func setHAConditions(cluster *antflyv1.AntflyCluster, plan haPlan) {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled {
		setHACondition(cluster, antflyv1.TypeHAAvailable, metav1.ConditionTrue, antflyv1.ReasonHADisabled, "Hot-standby HA is disabled")
		setHACondition(cluster, antflyv1.TypeHADegraded, metav1.ConditionFalse, antflyv1.ReasonHADisabled, "Hot-standby HA is disabled")
		setHACondition(cluster, antflyv1.TypeHAUnhealthy, metav1.ConditionFalse, antflyv1.ReasonHADisabled, "Hot-standby HA is disabled")
		setHACondition(cluster, antflyv1.TypeHALagging, metav1.ConditionFalse, antflyv1.ReasonHADisabled, "Hot-standby HA is disabled")
		setHACondition(cluster, antflyv1.TypeHARetentionPressure, metav1.ConditionFalse, antflyv1.ReasonHADisabled, "Hot-standby HA is disabled")
		setHACondition(cluster, antflyv1.TypeHAReseedRequired, metav1.ConditionFalse, antflyv1.ReasonHADisabled, "Hot-standby HA is disabled")
		setHACondition(cluster, antflyv1.TypeHAAutomaticFailoverReady, metav1.ConditionFalse, antflyv1.ReasonHADisabled, "Hot-standby HA is disabled")
		return
	}

	if plan.ReadSafeStandbyCount > 0 {
		setHACondition(cluster, antflyv1.TypeHAAvailable, metav1.ConditionTrue, antflyv1.ReasonHAStandbyAvailable, fmt.Sprintf("%d desired standby is safe for reads", plan.ReadSafeStandbyCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHAAvailable, metav1.ConditionFalse, antflyv1.ReasonHANoHealthyStandby, "No desired standby is safe for reads")
	}

	degraded := haSyncPolicyDegraded(ha, plan)
	if degraded {
		setHACondition(cluster, antflyv1.TypeHADegraded, metav1.ConditionTrue, antflyv1.ReasonHASyncPolicyUnsatisfied, "Synchronous HA policy is not currently satisfied")
	} else if haPrimaryAdminObservationFailed(cluster.Status.HAStatus) {
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			haAdminStatusUnavailableReason(cluster, antflyv1.ReasonHAPrimaryAdminUnavailable),
			fmt.Sprintf("Primary HA admin endpoint cannot be observed: %s", strings.TrimSpace(cluster.Status.HAStatus.PrimaryAdminLastError)),
		)
	} else {
		setHACondition(cluster, antflyv1.TypeHADegraded, metav1.ConditionFalse, antflyv1.ReasonHASyncPolicySatisfied, "Synchronous HA policy is satisfied or not configured")
	}

	if plan.UnhealthyStandbyCount > 0 {
		setHACondition(cluster, antflyv1.TypeHAUnhealthy, metav1.ConditionTrue, antflyv1.ReasonHAStandbyUnhealthy, fmt.Sprintf("%d desired standby is missing, inactive, or reporting replication errors", plan.UnhealthyStandbyCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHAUnhealthy, metav1.ConditionFalse, antflyv1.ReasonHAStandbysHealthy, "No desired standby is missing, inactive, or reporting replication errors")
	}

	if plan.LaggingStandbyCount > 0 {
		setHACondition(cluster, antflyv1.TypeHALagging, metav1.ConditionTrue, antflyv1.ReasonHAStandbyLagging, fmt.Sprintf("%d desired standby has replication lag", plan.LaggingStandbyCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHALagging, metav1.ConditionFalse, antflyv1.ReasonHANoLaggingStandbys, "No desired standby has replication lag")
	}

	retentionPressure := cluster.Status.HAStatus != nil && cluster.Status.HAStatus.Retention.ReseedRecommended > 0
	if retentionPressure {
		setHACondition(cluster, antflyv1.TypeHARetentionPressure, metav1.ConditionTrue, antflyv1.ReasonHARetentionCapExceeded, "One or more slots are forcing WAL retention beyond policy")
	} else {
		setHACondition(cluster, antflyv1.TypeHARetentionPressure, metav1.ConditionFalse, antflyv1.ReasonHARetentionWithinPolicy, "WAL retention is within configured policy")
	}

	if plan.ReseedRequiredCount > 0 {
		setHACondition(cluster, antflyv1.TypeHAReseedRequired, metav1.ConditionTrue, antflyv1.ReasonHAStandbyRequiresReseed, fmt.Sprintf("%d desired standby requires reseed", plan.ReseedRequiredCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHAReseedRequired, metav1.ConditionFalse, antflyv1.ReasonHANoReseedRequired, "No desired standby requires reseed")
	}

	reason := haAutomaticFailoverReason(ha, plan)
	if plan.AutomaticPromotionAllowed {
		setHACondition(cluster, antflyv1.TypeHAAutomaticFailoverReady, metav1.ConditionTrue, reason, "Automatic failover may acquire a fence and promote a caught-up standby")
	} else {
		setHACondition(cluster, antflyv1.TypeHAAutomaticFailoverReady, metav1.ConditionFalse, reason, "Automatic failover is disabled or missing a safe fencing/readiness prerequisite")
	}
}

func setHACondition(cluster *antflyv1.AntflyCluster, conditionType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func haAutomaticPromotionStandby(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus, plan haPlan) string {
	if ha == nil || ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled {
		return ""
	}
	if !haAutomaticFailoverExecutionEnabled(ha) {
		return ""
	}
	if ha.AutomaticFailover.FencingAuthority == "" || ha.AutomaticFailover.FencingAuthority == antflyv1.HAFencingAuthorityNone {
		return ""
	}
	if !haAutomaticFailoverFencingAuthoritySupported(ha) {
		return ""
	}
	if !plan.FencingReady {
		return ""
	}
	committedStandby := haCommittedAutomaticPromotionStandby(ha, status)
	if plan.PromotionAlreadyRecorded {
		return ""
	}
	if !plan.PromotionRouteReady {
		return ""
	}
	// Once the Lease-backed transaction is recorded, do not re-run the initial
	// candidate election gates against a moving old-primary tail. The pending
	// node-local fence freezes the actual durable tail; downstream assessment
	// then waits for the chosen standby to apply that exact boundary. Requiring
	// the old writer to remain unreachable and the candidate to remain caught
	// up here would erase the transaction precisely when transport recovery lets
	// the controller execute the fence.
	if committedStandby != "" {
		if status == nil || committedStandby != status.Fencing.Holder {
			return ""
		}
		return committedStandby
	}
	if !haPrimaryAdminUnavailable(status) {
		return ""
	}
	if !haPromotionBoundaryReady(status) {
		return ""
	}
	if haSyncPolicyDegraded(ha, plan) {
		return ""
	}
	if status == nil {
		return ""
	}
	fenceHolder := status.Fencing.Holder
	if !desiredStandbyNamed(ha, fenceHolder) {
		return ""
	}
	maxLag := ha.AutomaticFailover.MaximumLagLSN
	requireApply := ha.AutomaticFailover.RequireRemoteApply == nil || *ha.AutomaticFailover.RequireRemoteApply
	for _, standby := range status.Standbys {
		if standby.Name != fenceHolder && standby.SlotName != fenceHolder {
			continue
		}
		if !standbyPromotionEligible(standby) {
			continue
		}
		if standby.ReceivedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		if requireApply && !standbyRemoteApplyReady(status, standby, maxLag) {
			continue
		}
		return fenceHolder
	}
	return ""
}

func haKubernetesLeaseFenceCandidate(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) string {
	if ha == nil || ha.AutomaticFailover == nil || status == nil {
		return ""
	}
	if committed := haCommittedAutomaticPromotionStandby(ha, status); committed != "" {
		return committed
	}
	if !haPrimaryAdminUnavailable(status) {
		return ""
	}
	if !haPromotionBoundaryReady(status) {
		return ""
	}
	sync := haEvaluateSyncPolicy(ha, status)
	if sync.Degraded {
		return ""
	}
	standbys := haStandbyStatusByName(status)
	maxLag := ha.AutomaticFailover.MaximumLagLSN
	requireApply := ha.AutomaticFailover.RequireRemoteApply == nil || *ha.AutomaticFailover.RequireRemoteApply
	for _, desired := range ha.Standbys {
		if !standbyDesired(desired) || desired.Name == "" {
			continue
		}
		if strings.TrimSpace(desired.AdminURL) == "" {
			continue
		}
		if len(desired.RouteSelector) == 0 {
			continue
		}
		standby, ok := standbys[desired.Name]
		if !ok {
			standby, ok = standbys[standbySlotName(desired)]
		}
		if !ok || !standbyPromotionEligible(standby) {
			continue
		}
		if standby.ReceivedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		if requireApply && !standbyRemoteApplyReady(status, standby, maxLag) {
			continue
		}
		return desired.Name
	}
	return ""
}

// Once a failover plan has reached the former-primary fence step, the
// Kubernetes Lease represents an in-flight ownership transfer. A transient
// recovery of the old primary admin endpoint must let the controller finish
// that transfer; otherwise the very connection needed to durably fence the old
// writer would make the promotion plan disappear.
func haCommittedAutomaticPromotionStandby(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) string {
	if ha == nil || status == nil || haPromotionReceipt(status) != nil {
		return ""
	}
	fencing := status.Fencing
	if fencing.Generation == 0 || !desiredStandbyNamed(ha, fencing.Holder) {
		return ""
	}
	if !fencing.Ready && fencing.Reason != "LeaseExpired" {
		return ""
	}
	if haCommittedFormerPrimaryFenceAction(ha, status, fencing.Holder, fencing.Generation) != nil ||
		haCommittedFormerPrimaryIsolationAction(ha, status, fencing.Holder, fencing.Generation) != nil {
		return fencing.Holder
	}
	return ""
}

func haCommittedFormerPrimaryIsolationAction(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus, holder string, generation uint64) *antflyv1.HAPlannedActionStatus {
	if ha == nil || ha.AutomaticFailover == nil || status == nil || haPromotionReceipt(status) != nil ||
		generation == 0 || !desiredStandbyNamed(ha, holder) {
		return nil
	}
	identity := haReplicationIdentity(ha)
	if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) == "" {
		return nil
	}
	for i := range status.PlannedActions {
		action := &status.PlannedActions[i]
		if haActionKind(action.Kind) != haActionIsolateFormerPrimary ||
			strings.TrimSpace(action.StandbyName) != strings.TrimSpace(identity.CurrentPrimaryID) ||
			strings.TrimSpace(action.RouteTo) != strings.TrimSpace(holder) ||
			strings.TrimSpace(action.FenceHolder) != strings.TrimSpace(holder) ||
			action.FenceGeneration != generation ||
			action.FenceAuthority != ha.AutomaticFailover.FencingAuthority ||
			action.TargetLSN == 0 {
			continue
		}
		switch action.AdminJobPhase {
		case haAdminJobPhaseRunning:
			if !haPhysicalIsolationIntentStructurallyMatches(*action) {
				continue
			}
			return action
		case haAdminJobPhaseSucceeded:
			if !haPhysicalIsolationSucceededStructurallyWithEvidence(*action) {
				continue
			}
			return action
		default:
			continue
		}
	}
	return nil
}

func haCommittedFormerPrimaryFenceAction(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus, holder string, generation uint64) *antflyv1.HAPlannedActionStatus {
	if ha == nil || ha.AutomaticFailover == nil || status == nil || haPromotionReceipt(status) != nil ||
		generation == 0 || !desiredStandbyNamed(ha, holder) {
		return nil
	}
	identity := haReplicationIdentity(ha)
	if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) == "" {
		return nil
	}
	for i := range status.PlannedActions {
		action := &status.PlannedActions[i]
		if haActionKind(action.Kind) != haActionFenceFormerPrimary ||
			strings.TrimSpace(action.StandbyName) != strings.TrimSpace(identity.CurrentPrimaryID) ||
			strings.TrimSpace(action.RouteTo) != strings.TrimSpace(holder) ||
			strings.TrimSpace(action.FenceHolder) != strings.TrimSpace(holder) ||
			action.FenceGeneration != generation ||
			action.FenceAuthority != ha.AutomaticFailover.FencingAuthority ||
			action.TargetLSN == 0 {
			continue
		}
		// Merely rendering a plan is not a commitment: the candidate can still
		// become unhealthy before execution begins. Preserve the transaction only
		// after the former-primary fence call has actually started (or completed).
		switch action.AdminJobPhase {
		case haAdminJobPhasePending, haAdminJobPhaseRunning, haAdminJobPhaseSucceeded:
		default:
			continue
		}
		return action
	}
	return nil
}

func haAutomaticFailoverPromotionBoundary(status *antflyv1.HAStatus, holder string, generation uint64) uint64 {
	if status == nil {
		return 0
	}
	var committedLowerBound uint64
	for _, action := range status.PlannedActions {
		kind := haActionKind(action.Kind)
		if (kind != haActionFenceFormerPrimary && kind != haActionIsolateFormerPrimary) ||
			strings.TrimSpace(action.RouteTo) != strings.TrimSpace(holder) ||
			action.FenceGeneration != generation {
			continue
		}
		if action.TargetLSN != 0 {
			committedLowerBound = action.TargetLSN
		}
		if kind == haActionIsolateFormerPrimary && action.AdminJobPhase == haAdminJobPhaseSucceeded {
			return action.TargetLSN
		}
		if action.AdminJobPhase != haAdminJobPhaseSucceeded || action.AdminResult == nil {
			continue
		}
		result := action.AdminResult
		if result.FenceRequiredLSN == 0 ||
			result.FenceObservedLSN != result.FenceRequiredLSN ||
			result.FenceGeneration != generation ||
			strings.TrimSpace(result.FencePromotedNodeID) != strings.TrimSpace(holder) {
			continue
		}
		return result.FenceRequiredLSN
	}
	if committedLowerBound != 0 {
		return committedLowerBound
	}
	return status.PrimaryLSN
}

func standbyReadSafe(status *antflyv1.HAStatus, standby antflyv1.HAStandbyStatus) bool {
	if !standbyPromotionEligible(standby) {
		return false
	}
	return standby.CanServeSafeReads && standbySafeReadLSN(standby) >= status.PrimaryLSN
}

func standbyRemoteApplyReady(status *antflyv1.HAStatus, standby antflyv1.HAStandbyStatus, maxLag uint64) bool {
	return standby.CanServeSafeReads &&
		standby.AppliedLSN+maxLag >= status.PrimaryLSN &&
		standbySafeReadLSN(standby)+maxLag >= status.PrimaryLSN
}

func standbyPromotionEligible(standby antflyv1.HAStandbyStatus) bool {
	if !standby.Active || standby.ReseedRequired {
		return false
	}
	errName := strings.TrimSpace(standby.LastError)
	if errName == "" {
		return true
	}
	// Losing the old primary is the event automatic failover is designed to
	// survive. A standby that has durably applied everything it received and can
	// still serve that boundary remains a valid candidate when its only error is
	// that the upstream transport disappeared. Semantic replication failures
	// (identity, timeline, decoding, storage, etc.) continue to fail closed.
	if !standby.CaughtUpToReceived || !standby.CanServeSafeReads {
		return false
	}
	if standbyUpstreamTransportUnavailable(errName) {
		return true
	}
	return false
}

func standbyUpstreamTransportUnavailable(errName string) bool {
	switch errName {
	case "HttpConnectionClosing",
		"ConnectionResetByPeer",
		"ConnectionRefused",
		"BrokenPipe",
		"EndOfStream",
		"NoAddressReturned",
		"Timeout",
		"ConnectionTimedOut",
		"NetworkUnreachable",
		"HostUnreachable",
		"NetworkDown",
		"AddressUnavailable",
		"TemporaryNameServerFailure",
		"NameServerFailure",
		"NotListening":
		return true
	default:
		return false
	}
}

func standbySafeReadLSN(standby antflyv1.HAStandbyStatus) uint64 {
	if standby.SafeReadLSN != 0 || standby.SafeReadLagLSN != 0 {
		return standby.SafeReadLSN
	}
	return standby.AppliedLSN
}

func haFencingReady(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) bool {
	if ha == nil || ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled {
		return false
	}
	authority := ha.AutomaticFailover.FencingAuthority
	if authority == "" || authority == antflyv1.HAFencingAuthorityNone {
		return false
	}
	if !haAutomaticFailoverFencingAuthoritySupported(ha) {
		return false
	}
	if status == nil {
		return false
	}
	fencing := status.Fencing
	if !fencing.Ready {
		return false
	}
	if fencing.Authority != authority {
		return false
	}
	if fencing.Holder == "" || fencing.Generation == 0 {
		return false
	}
	if !haStandbyAdminURLConfigured(ha, fencing.Holder) {
		return false
	}
	return true
}

func haPromotionRouteReady(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) bool {
	if ha == nil || ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled || status == nil {
		return false
	}
	return haStandbyRouteSelectorConfigured(ha, status.Fencing.Holder)
}

func haStandbyRouteSelectorConfigured(ha *antflyv1.HighAvailabilitySpec, standbyName string) bool {
	standby, ok := desiredStandbySpecByName(ha, standbyName)
	if !ok {
		return false
	}
	return len(standby.RouteSelector) > 0
}

func haStandbyAdminURLConfigured(ha *antflyv1.HighAvailabilitySpec, standbyName string) bool {
	standby, ok := desiredStandbySpecByName(ha, standbyName)
	if !ok {
		return false
	}
	return strings.TrimSpace(standby.AdminURL) != ""
}

func haAutomaticFailoverReason(ha *antflyv1.HighAvailabilitySpec, plan haPlan) string {
	if plan.AutomaticPromotionAllowed {
		return antflyv1.ReasonHAFencedPromotionReady
	}
	if ha == nil || ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled {
		return antflyv1.ReasonHAAutomaticFailoverDisabled
	}
	if !haAutomaticFailoverExecutionEnabled(ha) {
		return antflyv1.ReasonHAAutomaticFailoverExecutionDisabled
	}
	if ha.AutomaticFailover.FencingAuthority == "" || ha.AutomaticFailover.FencingAuthority == antflyv1.HAFencingAuthorityNone {
		return antflyv1.ReasonHAFencingAuthorityMissing
	}
	if !haAutomaticFailoverFencingAuthoritySupported(ha) {
		return antflyv1.ReasonHAFencingAuthorityUnsupported
	}
	if !plan.FencingReady {
		return antflyv1.ReasonHAFencingNotReady
	}
	if !plan.PrimaryAdminUnavailable {
		return antflyv1.ReasonHAPrimaryStillReachable
	}
	if !plan.PromotionBoundaryReady {
		return antflyv1.ReasonHAPromotionBoundaryMissing
	}
	if plan.PromotionAlreadyRecorded {
		return antflyv1.ReasonHAPromotionAlreadyRecorded
	}
	if !plan.PromotionRouteReady {
		return antflyv1.ReasonHAPrimaryRouteSelectorMissing
	}
	if haSyncPolicyDegraded(ha, plan) {
		return antflyv1.ReasonHASyncPolicyUnsatisfied
	}
	return antflyv1.ReasonHANoHealthyStandby
}

func haAutomaticFailoverExecutionEnabled(ha *antflyv1.HighAvailabilitySpec) bool {
	return ha != nil && ha.Admin != nil && ha.Admin.ExecutePlannedActions
}

const (
	defaultHAAutomaticFailoverMinimumConsecutiveFailures int32 = 3
	defaultHAAutomaticFailoverMinimumUnreachableSeconds  int32 = 30
)

func haAutomaticFailoverFailureThresholds(ha *antflyv1.HighAvailabilitySpec) (int32, time.Duration) {
	minimumFailures := defaultHAAutomaticFailoverMinimumConsecutiveFailures
	minimumSeconds := defaultHAAutomaticFailoverMinimumUnreachableSeconds
	if ha != nil && ha.AutomaticFailover != nil {
		if ha.AutomaticFailover.MinimumConsecutiveFailures >= 2 {
			minimumFailures = ha.AutomaticFailover.MinimumConsecutiveFailures
		}
		if ha.AutomaticFailover.MinimumUnreachableDurationSeconds >= 1 {
			minimumSeconds = ha.AutomaticFailover.MinimumUnreachableDurationSeconds
		}
	}
	return minimumFailures, time.Duration(minimumSeconds) * time.Second
}

func haAutomaticFailoverFencingAuthoritySupported(ha *antflyv1.HighAvailabilitySpec) bool {
	return ha != nil &&
		ha.AutomaticFailover != nil &&
		ha.AutomaticFailover.FencingAuthority == antflyv1.HAFencingAuthorityKubernetesLease
}

func haPrimaryAdminUnavailable(status *antflyv1.HAStatus) bool {
	if !haPrimaryAdminObservationFailed(status) {
		return false
	}
	return status.PrimaryAdminStatusCode != http.StatusUnauthorized &&
		status.PrimaryAdminFailureThresholdMet
}

func haPrimaryAdminObservationFailed(status *antflyv1.HAStatus) bool {
	return status != nil && !status.PrimaryAdminReachable && strings.TrimSpace(status.PrimaryAdminLastError) != ""
}

func haPromotionBoundaryReady(status *antflyv1.HAStatus) bool {
	return status != nil && status.PrimaryLSN > 0
}

func haPromotionAlreadyRecorded(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) bool {
	promotion := haPromotionReceipt(status)
	if promotion == nil {
		return false
	}
	identity := haReplicationIdentity(ha)
	if identity == nil {
		return false
	}
	return haIdentityMatchesPromotionParentOrChild(identity, promotion)
}

// haIdentityMatchesPromotionParentOrChild keeps a durable promotion receipt
// valid across the spec update that adopts its new primary and timeline. The
// receipt may be observed immediately before or immediately after that update;
// accepting any unrelated identity would permit stale topology evidence.
func haIdentityMatchesPromotionParentOrChild(identity *antflyv1.HAReplicationIdentitySpec, promotion *antflyv1.HAPromotionStatus) bool {
	if identity == nil || promotion == nil ||
		(promotion.ClusterID != 0 && promotion.ClusterID != identity.ClusterID) ||
		(promotion.ShardID != 0 && promotion.ShardID != identity.ShardID) ||
		(promotion.TableID != 0 && promotion.TableID != identity.TableID) {
		return false
	}
	parent := strings.TrimSpace(identity.CurrentPrimaryID) == strings.TrimSpace(promotion.OldPrimaryID) &&
		identity.TimelineID == promotion.ParentTimelineID && identity.Epoch == promotion.ParentEpoch
	child := strings.TrimSpace(identity.CurrentPrimaryID) == strings.TrimSpace(promotion.PromotedStandbyID) &&
		identity.TimelineID == promotion.NewTimelineID && identity.Epoch == promotion.NewEpoch
	return parent || child
}

func haSyncPolicyDegraded(ha *antflyv1.HighAvailabilitySpec, plan haPlan) bool {
	_ = ha
	return plan.SyncPolicyDegraded
}

func haEvaluateSyncPolicy(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) haSyncEvaluation {
	evaluation := haSyncEvaluation{Action: "Satisfied"}
	if ha == nil || ha.SyncPolicy == nil || ha.SyncPolicy.Mode == "" || ha.SyncPolicy.Mode == antflyv1.HADurabilityModeAsync {
		evaluation.Mode = antflyv1.HADurabilityModeAsync
		return evaluation
	}
	policy := ha.SyncPolicy
	evaluation.Mode = policy.Mode
	evaluation.Selection = policy.Selection
	if evaluation.Selection == "" {
		evaluation.Selection = antflyv1.HAStandbySelectionAny
	}
	evaluation.FailurePolicy = policy.FailurePolicy
	if evaluation.FailurePolicy == "" {
		evaluation.FailurePolicy = antflyv1.HAFailurePolicyBlock
	}
	evaluation.Required = haSyncPolicyRequired(policy)
	if observed, ok := haObservedAdminSyncStatus(policy, status); ok {
		return observed
	}
	if status == nil {
		evaluation.Degraded = true
		evaluation.Action = haSyncFailureAction(evaluation.FailurePolicy)
		return evaluation
	}
	standbys := haStandbyStatusByName(status)
	switch evaluation.Selection {
	case antflyv1.HAStandbySelectionAll:
		if len(policy.StandbyNames) == 0 {
			evaluation.Degraded = true
			break
		}
		evaluation.Required = haLenToInt32(len(policy.StandbyNames))
		for _, name := range policy.StandbyNames {
			standby, ok := standbys[name]
			if !ok || !standbySyncEligible(standby) {
				continue
			}
			evaluation.Candidates++
			if standbySatisfiesSync(status.PrimaryLSN, policy.Mode, standby) {
				evaluation.Satisfied++
			}
		}
		evaluation.Degraded = evaluation.Satisfied < evaluation.Required
	case antflyv1.HAStandbySelectionFirst:
		for _, name := range policy.StandbyNames {
			standby, ok := standbys[name]
			if !ok || !standbySyncEligible(standby) {
				continue
			}
			evaluation.Candidates++
			if standbySatisfiesSync(status.PrimaryLSN, policy.Mode, standby) {
				evaluation.Satisfied++
			}
			if evaluation.Candidates == evaluation.Required {
				break
			}
		}
		evaluation.Degraded = evaluation.Candidates < evaluation.Required || evaluation.Satisfied < evaluation.Required
	default:
		for _, name := range policy.StandbyNames {
			standby, ok := standbys[name]
			if !ok || !standbySyncEligible(standby) {
				continue
			}
			evaluation.Candidates++
			if standbySatisfiesSync(status.PrimaryLSN, policy.Mode, standby) {
				evaluation.Satisfied++
			}
		}
		evaluation.Degraded = evaluation.Satisfied < evaluation.Required
	}
	if evaluation.Degraded {
		evaluation.Action = haSyncFailureAction(evaluation.FailurePolicy)
	}
	return evaluation
}

func haObservedAdminSyncStatus(policy *antflyv1.HASyncPolicy, status *antflyv1.HAStatus) (haSyncEvaluation, bool) {
	if policy == nil || status == nil || !status.PrimaryAdminReachable || status.Sync.Mode == "" {
		return haSyncEvaluation{}, false
	}
	if status.Sync.Mode != policy.Mode {
		return haSyncEvaluation{}, false
	}
	selection := policy.Selection
	if selection == "" {
		selection = antflyv1.HAStandbySelectionAny
	}
	if status.Sync.Selection != "" && status.Sync.Selection != selection {
		return haSyncEvaluation{}, false
	}
	failurePolicy := policy.FailurePolicy
	if failurePolicy == "" {
		failurePolicy = antflyv1.HAFailurePolicyBlock
	}
	required := haSyncPolicyRequired(policy)
	if status.Sync.Required > 0 && status.Sync.Required != required {
		return haSyncEvaluation{}, false
	}
	action := status.Sync.Action
	if action == "" {
		if status.Sync.Degraded {
			action = haSyncFailureAction(failurePolicy)
		} else {
			action = "Satisfied"
		}
	}
	return haSyncEvaluation{
		Mode:          status.Sync.Mode,
		Selection:     selection,
		Required:      required,
		Satisfied:     status.Sync.Satisfied,
		Candidates:    status.Sync.Candidates,
		FailurePolicy: failurePolicy,
		Degraded:      status.Sync.Degraded,
		Action:        action,
	}, true
}

func haSyncPolicyRequired(policy *antflyv1.HASyncPolicy) int32 {
	if policy == nil {
		return 0
	}
	selection := policy.Selection
	if selection == "" {
		selection = antflyv1.HAStandbySelectionAny
	}
	if selection == antflyv1.HAStandbySelectionAll {
		return haLenToInt32(len(policy.StandbyNames))
	}
	if policy.Required > 0 {
		return policy.Required
	}
	return 1
}

func haLenToInt32(n int) int32 {
	if n > int(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}
	return int32(n) //nolint:gosec // n is bounded to MaxInt32 above.
}

func haSyncFailureAction(policy antflyv1.HAFailurePolicy) string {
	switch policy {
	case antflyv1.HAFailurePolicyFailClosed:
		return "RejectWrites"
	case antflyv1.HAFailurePolicyDegradeToAsync:
		return "DegradeToAsync"
	default:
		return "BlockWrites"
	}
}

func haEvaluatePrimaryRoute(cluster *antflyv1.AntflyCluster, status *antflyv1.HAStatus, promotionTarget string) haPrimaryRouteEvaluation {
	currentFenceAuthority := antflyv1.HAFencingAuthority("")
	currentFenceGeneration := uint64(0)
	evaluation := haPrimaryRouteEvaluation{
		ServiceName:   cluster.Name + "-public-api",
		CurrentTarget: haPrimaryRouteCurrentTarget(status),
		DesiredTarget: "primary",
		Action:        "None",
		Reason:        "PrimaryRouteCurrent",
	}
	if status != nil {
		currentFenceAuthority = status.PrimaryRoute.FenceAuthority
		currentFenceGeneration = status.PrimaryRoute.FenceGeneration
		if promotion := haPromotionReceipt(status); promotion != nil {
			evaluation.FenceAuthority = promotion.FenceAuthority
			evaluation.FenceGeneration = promotion.FenceGeneration
			evaluation.DesiredTarget = promotion.PromotedStandbyID
			if haPromotionAlreadyRecorded(cluster.Spec.HighAvailability, status) &&
				currentFenceAuthority == "" &&
				evaluation.CurrentTarget == evaluation.DesiredTarget &&
				currentFenceGeneration != 0 &&
				currentFenceGeneration == promotion.FenceGeneration {
				currentFenceAuthority = promotion.FenceAuthority
			}
		}
	}
	if promotionTarget != "" {
		evaluation.DesiredTarget = promotionTarget
		if status != nil {
			evaluation.FenceAuthority = status.Fencing.Authority
			evaluation.FenceGeneration = status.Fencing.Generation
		}
	}
	evaluation.Stale = evaluation.CurrentTarget != evaluation.DesiredTarget ||
		(evaluation.FenceAuthority != "" && currentFenceAuthority != evaluation.FenceAuthority) ||
		(evaluation.FenceGeneration > 0 && currentFenceGeneration < evaluation.FenceGeneration)
	if evaluation.Stale {
		evaluation.Action = string(haActionUpdatePrimaryRoute)
		if evaluation.CurrentTarget != evaluation.DesiredTarget {
			evaluation.Reason = "PrimaryRouteTargetChanged"
		} else if evaluation.FenceAuthority != "" && currentFenceAuthority != evaluation.FenceAuthority {
			evaluation.Reason = "PrimaryRouteFenceAuthorityStale"
		} else {
			evaluation.Reason = "PrimaryRouteFenceGenerationStale"
		}
	}
	return evaluation
}

func haPrimaryRouteCurrentTarget(status *antflyv1.HAStatus) string {
	if status != nil && status.PrimaryRoute.CurrentTarget != "" {
		return status.PrimaryRoute.CurrentTarget
	}
	return "primary"
}

func haPlannedActionFenceReason(status *antflyv1.HAStatus) string {
	if status == nil {
		return ""
	}
	if status.Fencing.Reason != "" {
		return status.Fencing.Reason
	}
	if status.LastPromotion != nil {
		return status.LastPromotion.FenceReason
	}
	return ""
}

func haEvaluateFormerPrimary(status *antflyv1.HAStatus) haFormerPrimaryEvaluation {
	if status == nil || status.LastPromotion == nil || status.LastPromotion.OldPrimaryID == "" {
		return haFormerPrimaryEvaluation{}
	}
	promotion := status.LastPromotion
	evaluation := haFormerPrimaryEvaluation{
		Present:          true,
		NodeID:           promotion.OldPrimaryID,
		RejoinRequired:   true,
		ParentTimelineID: promotion.ParentTimelineID,
		NewTimelineID:    promotion.NewTimelineID,
		SwitchLSN:        promotion.SwitchLSN,
		FenceAuthority:   promotion.FenceAuthority,
		FenceHolder:      promotion.PromotedStandbyID,
		FenceGeneration:  promotion.FenceGeneration,
		Action:           string(haActionDemoteFormerPrimary),
		Reason:           "FormerPrimaryFenceNotObserved",
	}
	evaluation.Fenced = haFormerPrimaryFenced(status, promotion)
	if !evaluation.Fenced {
		return evaluation
	}
	if completed := haCompletedFormerPrimaryRewind(status); completed != nil {
		result := completed.AdminResult
		evaluation.ObservedTimelineID = promotion.NewTimelineID
		evaluation.ObservedLSN = result.RewindCurrentLastLSN
		evaluation.SwitchLSN = result.ForkLSN
		evaluation.RejoinRequired = false
		evaluation.RewindPossible = false
		evaluation.ReseedRequired = false
		evaluation.Diverged = false
		evaluation.Action = "None"
		evaluation.Reason = "FormerPrimaryRewindApplied"
		return evaluation
	}
	if standby, ok := haStandbyStatusByName(status)[promotion.OldPrimaryID]; ok &&
		standby.Active &&
		!standby.ReseedRequired &&
		standby.Status != "reseed_required" &&
		standby.TimelineID == promotion.NewTimelineID {
		evaluation.ObservedTimelineID = standby.TimelineID
		evaluation.ObservedLSN = maxHAObservedLSN(standby)
		evaluation.RejoinRequired = false
		evaluation.Action = "None"
		evaluation.Reason = "FormerPrimaryOnPromotionTimeline"
		return evaluation
	}
	if haApplyFormerPrimaryAssessment(&evaluation, status.FormerPrimary, promotion) {
		return evaluation
	}

	standby, ok := haStandbyStatusByName(status)[promotion.OldPrimaryID]
	if !ok {
		evaluation.Reason = "FormerPrimaryNotObserved"
		return evaluation
	}
	evaluation.ObservedTimelineID = standby.TimelineID
	evaluation.ObservedLSN = maxHAObservedLSN(standby)
	if evaluation.ObservedTimelineID == 0 {
		evaluation.Reason = "FormerPrimaryTimelineUnknown"
		return evaluation
	}
	if !standby.Active || standby.ReseedRequired || standby.Status == "reseed_required" {
		evaluation.ReseedRequired = true
		evaluation.Diverged = true
		evaluation.Action = string(haActionReseedFormerPrimary)
		evaluation.Reason = "FormerPrimaryRequiresReseed"
		return evaluation
	}
	if evaluation.ObservedTimelineID == promotion.NewTimelineID {
		evaluation.RejoinRequired = false
		evaluation.Action = "None"
		evaluation.Reason = "FormerPrimaryOnPromotionTimeline"
		return evaluation
	}
	if evaluation.ObservedTimelineID != promotion.ParentTimelineID {
		evaluation.ReseedRequired = true
		evaluation.Diverged = true
		evaluation.Action = string(haActionReseedFormerPrimary)
		evaluation.Reason = "FormerPrimaryTimelineDiverged"
		return evaluation
	}
	if !promotion.DataLossPossible && evaluation.ObservedLSN <= promotion.SwitchLSN {
		evaluation.RewindPossible = true
		evaluation.Action = string(haActionRewindFormerPrimary)
		evaluation.Reason = "FormerPrimaryNeedsRewind"
		return evaluation
	}
	evaluation.ReseedRequired = true
	evaluation.Diverged = true
	evaluation.Action = string(haActionReseedFormerPrimary)
	evaluation.Reason = "FormerPrimaryRequiresReseed"
	return evaluation
}

func haApplyFormerPrimaryAssessment(evaluation *haFormerPrimaryEvaluation, former *antflyv1.HAFormerPrimaryStatus, promotion *antflyv1.HAPromotionStatus) bool {
	if evaluation == nil || former == nil || promotion == nil ||
		former.NodeID != promotion.OldPrimaryID ||
		former.TargetTimelineID != promotion.NewTimelineID ||
		former.TargetEpoch != promotion.NewEpoch ||
		former.FenceAuthority != promotion.FenceAuthority ||
		former.FenceHolder != promotion.PromotedStandbyID ||
		former.FenceGeneration != promotion.FenceGeneration ||
		former.ForkLSN == 0 ||
		former.AssessedAction == "" {
		return false
	}
	evaluation.SwitchLSN = former.ForkLSN
	if former.FormerLastLSN != 0 {
		evaluation.ObservedLSN = former.FormerLastLSN
	}
	reason := former.AssessedReason
	if reason == "" {
		reason = former.Reason
	}
	switch former.AssessedAction {
	case "already_current":
		evaluation.RejoinRequired = false
		evaluation.RewindPossible = false
		evaluation.ReseedRequired = false
		evaluation.Diverged = false
		evaluation.Action = "None"
		if reason == "" {
			reason = "FormerPrimaryOnPromotionTimeline"
		}
	case "rewind":
		// A rewind is only safe when the former primary has no divergent suffix.
		// HA log records are an effects stream, not an undo log: truncating an
		// already-applied divergent record would hide it from replication without
		// removing its effects from storage. Older runtimes could report such a
		// disposition as rewindable, so fail closed and require a reseed.
		if former.FormerLastLSN != former.ForkLSN || former.DataLossDiscarded {
			evaluation.RejoinRequired = true
			evaluation.RewindPossible = false
			evaluation.ReseedRequired = true
			evaluation.Diverged = true
			evaluation.Action = string(haActionReseedFormerPrimary)
			evaluation.Reason = "FormerPrimaryRequiresReseed"
			return true
		}
		evaluation.RejoinRequired = true
		evaluation.RewindPossible = true
		evaluation.ReseedRequired = false
		evaluation.Diverged = false
		evaluation.Action = string(haActionRewindFormerPrimary)
		if reason == "" {
			reason = "FormerPrimaryNeedsRewind"
		}
	case "reseed":
		evaluation.RejoinRequired = true
		evaluation.RewindPossible = false
		evaluation.ReseedRequired = true
		evaluation.Diverged = true
		evaluation.Action = string(haActionReseedFormerPrimary)
		if reason == "" {
			reason = "FormerPrimaryRequiresReseed"
		}
	default:
		return false
	}
	evaluation.Reason = reason
	return true
}

func haFormerPrimaryFenced(status *antflyv1.HAStatus, promotion *antflyv1.HAPromotionStatus) bool {
	if status == nil || promotion == nil {
		return false
	}
	former := status.FormerPrimary
	if promotion.FenceGeneration == 0 || former == nil || !former.Fenced {
		return false
	}
	if strings.TrimSpace(former.NodeID) != strings.TrimSpace(promotion.OldPrimaryID) {
		return false
	}
	if promotion.FenceAuthority != "" && former.FenceAuthority != promotion.FenceAuthority {
		return false
	}
	if former.FenceGeneration != promotion.FenceGeneration {
		return false
	}
	if promotion.PromotedStandbyID != "" &&
		former.FenceHolder != promotion.PromotedStandbyID {
		return false
	}
	return true
}

func maxHAObservedLSN(standby antflyv1.HAStandbyStatus) uint64 {
	lsn := standby.ReceivedLSN
	if standby.AppliedLSN > lsn {
		lsn = standby.AppliedLSN
	}
	if standby.SafeReadLSN > lsn {
		lsn = standby.SafeReadLSN
	}
	return lsn
}

func haStandbyStatusByName(status *antflyv1.HAStatus) map[string]antflyv1.HAStandbyStatus {
	standbys := map[string]antflyv1.HAStandbyStatus{}
	if status == nil {
		return standbys
	}
	for _, standby := range status.Standbys {
		if standby.Name != "" {
			standbys[standby.Name] = standby
		}
		if standby.SlotName != "" {
			standbys[standby.SlotName] = standby
		}
	}
	return standbys
}

func standbySatisfiesSync(primaryLSN uint64, mode antflyv1.HADurabilityMode, standby antflyv1.HAStandbyStatus) bool {
	if !standbySyncEligible(standby) {
		return false
	}
	switch mode {
	case antflyv1.HADurabilityModeRemoteWrite:
		return standby.ReceivedLSN >= primaryLSN
	case antflyv1.HADurabilityModeRemoteApply:
		return standby.AppliedLSN >= primaryLSN
	default:
		return true
	}
}

func standbySyncEligible(standby antflyv1.HAStandbyStatus) bool {
	return standbyPromotionEligible(standby)
}

func standbyLagging(standby antflyv1.HAStandbyStatus) bool {
	return standby.Status == "lagging" ||
		standby.WriteLagLSN > 0 ||
		standby.ReceiveLagLSN > 0 ||
		standby.ApplyLagLSN > 0 ||
		standby.SafeReadLagLSN > 0
}

func standbyDesired(standby antflyv1.HAStandbySpec) bool {
	return standby.Desired == nil || *standby.Desired
}

func desiredStandbyNamed(ha *antflyv1.HighAvailabilitySpec, name string) bool {
	_, ok := desiredStandbySpecByName(ha, name)
	return ok
}

func desiredStandbySpecByName(ha *antflyv1.HighAvailabilitySpec, name string) (antflyv1.HAStandbySpec, bool) {
	if ha == nil || name == "" {
		return antflyv1.HAStandbySpec{}, false
	}
	for _, standby := range ha.Standbys {
		if standbyDesired(standby) && standby.Name == name {
			return standby, true
		}
	}
	return antflyv1.HAStandbySpec{}, false
}

func standbySlotName(standby antflyv1.HAStandbySpec) string {
	if standby.SlotName != "" {
		return standby.SlotName
	}
	return standby.Name
}

func initialStandbyLSN(standby antflyv1.HAStandbySpec, currentLSN uint64) uint64 {
	if standby.InitialLSN != nil {
		return *standby.InitialLSN
	}
	return currentLSN
}
