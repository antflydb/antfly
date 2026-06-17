package controllers

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type haActionKind string

const (
	haActionCreateSlot           haActionKind = "CreateSlot"
	haActionResumeSlot           haActionKind = "ResumeSlot"
	haActionPauseSlot            haActionKind = "PauseSlot"
	haActionDropSlot             haActionKind = "DropSlot"
	haActionSeedStandby          haActionKind = "SeedStandby"
	haActionFinishStandbySeed    haActionKind = "FinishStandbySeed"
	haActionBootstrapStandbySeed haActionKind = "BootstrapStandbySeed"
	haActionMarkReseed           haActionKind = "MarkReseed"
	haActionAcquireFence         haActionKind = "AcquireFence"
	haActionPromoteStandby       haActionKind = "PromoteStandby"
	haActionUpdatePrimaryRoute   haActionKind = "UpdatePrimaryRoute"
	haActionDemoteFormerPrimary  haActionKind = "DemoteFormerPrimary"
	haActionRewindFormerPrimary  haActionKind = "RewindFormerPrimary"
	haActionReseedFormerPrimary  haActionKind = "ReseedFormerPrimary"
)

type haActionPhase string

const (
	haActionPhaseReconcile haActionPhase = "Reconcile"
	haActionPhaseFence     haActionPhase = "Fence"
	haActionPhasePromote   haActionPhase = "Promote"
	haActionPhaseRoute     haActionPhase = "Route"
	haActionPhaseRejoin    haActionPhase = "Rejoin"
)

type haActionExecutor string

const (
	haActionExecutorAdminCommand     haActionExecutor = "AdminCommand"
	haActionExecutorControllerAction haActionExecutor = "ControllerAction"
)

const (
	haAdminBasePath                        = "/admin/v1"
	haAdminHAPath                          = haAdminBasePath + "/ha"
	haAdminPrimaryStatusPath               = haAdminHAPath + "/primary/status"
	haAdminStandbyStatusPath               = haAdminHAPath + "/standby/status"
	haAdminReplicationSlotsPath            = haAdminHAPath + "/replication-slots"
	haAdminReplicationSlotPathPrefix       = haAdminReplicationSlotsPath + "/"
	haAdminReplicationSlotPausePathSuffix  = "/pause"
	haAdminReplicationSlotResumePathSuffix = "/resume"
	haAdminBaseBackupsPath                 = haAdminHAPath + "/base-backups"
	haAdminBaseBackupsFinishPath           = haAdminBaseBackupsPath + "/finish"
	haAdminStandbyBootstrapPath            = haAdminHAPath + "/standby/bootstrap"
	haAdminFencePath                       = haAdminHAPath + "/fence"
	haAdminPromotionCurrentFencePath       = haAdminHAPath + "/promotion/current-fence"
	haAdminRejoinAssessPath                = haAdminHAPath + "/rejoin/assess"
	haAdminRejoinRewindPath                = haAdminHAPath + "/rejoin/rewind"
	haAdminRejoinReseedPath                = haAdminHAPath + "/rejoin/reseed"
)

const haFencingLeaseDefaultDurationSeconds int32 = 30

func haFencingLeaseRenewalRequeueAfter() time.Duration {
	return time.Duration(haFencingLeaseDefaultDurationSeconds) * time.Second / 3
}

func haKubernetesLeaseRenewalEnabled(cluster *antflyv1.AntflyCluster) bool {
	if cluster == nil {
		return false
	}
	ha := cluster.Spec.HighAvailability
	return ha != nil &&
		ha.Mode != "" &&
		ha.Mode != antflyv1.HAModeDisabled &&
		ha.AutomaticFailover != nil &&
		ha.AutomaticFailover.Enabled &&
		ha.AutomaticFailover.FencingAuthority == antflyv1.HAFencingAuthorityKubernetesLease
}

type haPlannedAction struct {
	Kind             haActionKind
	DependsOn        haActionKind
	StandbyName      string
	SlotName         string
	TargetLSN        uint64
	ObservedLSN      uint64
	RetainedFromLSN  uint64
	RouteFrom        string
	RouteTo          string
	FenceAuthority   antflyv1.HAFencingAuthority
	FenceHolder      string
	FenceGeneration  uint64
	FenceReason      string
	SeedManifestPath string
	SeedContentRoot  string
	Reason           string
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
		ha.AutomaticFailover.FencingAuthority != antflyv1.HAFencingAuthorityKubernetesLease {
		return nil
	}
	if cluster.Status.HAStatus == nil {
		return nil
	}
	holder := haKubernetesLeaseFenceCandidate(ha, cluster.Status.HAStatus)
	if holder == "" {
		return nil
	}

	now := metav1.NowMicro()
	lease := &coordinationv1.Lease{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      haFencingLeaseName(cluster),
		Namespace: cluster.Namespace,
	}, lease)
	if apierrors.IsNotFound(err) {
		transitions := int32(1)
		durationSeconds := haFencingLeaseDefaultDurationSeconds
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      haFencingLeaseName(cluster),
				Namespace: cluster.Namespace,
				Labels:    haFencingLeaseLabels(cluster),
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: &durationSeconds,
				AcquireTime:          &now,
				RenewTime:            &now,
				LeaseTransitions:     &transitions,
			},
		}
		if r.Scheme != nil {
			if err := controllerutil.SetControllerReference(cluster, lease, r.Scheme); err != nil {
				return err
			}
		}
		return r.Create(ctx, lease)
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
	holderChanged := currentHolder != holder
	if holderChanged {
		transitions++
		lease.Spec.LeaseTransitions = &transitions
		lease.Spec.AcquireTime = &now
	} else if transitions == 0 {
		transitions = 1
		lease.Spec.LeaseTransitions = &transitions
		if lease.Spec.AcquireTime == nil {
			lease.Spec.AcquireTime = &now
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
	if r.Scheme != nil {
		if err := controllerutil.SetControllerReference(cluster, lease, r.Scheme); err != nil {
			return err
		}
	}
	return r.Update(ctx, lease)
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
	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
		Ready:      ready,
		Holder:     holder,
		Generation: generation,
		Reason:     reason,
	}
	return nil
}

func haFencingLeaseName(cluster *antflyv1.AntflyCluster) string {
	return cluster.Name + "-ha-fence"
}

func haFencingLeaseLabels(cluster *antflyv1.AntflyCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "antfly",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "antfly-operator",
		"antfly.io/ha-fence":           "kubernetes-lease",
	}
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
	expiresAt := lease.Spec.RenewTime.Time.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
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
	for _, standby := range ha.Standbys {
		slotName := standbySlotName(standby)
		observed, ok := slotByName[standby.Name]
		if !ok {
			observed, ok = slotByName[slotName]
		}
		if !standbyDesired(standby) {
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
		if !ok {
			plan.UnhealthyStandbyCount++
			plan.Actions = append(plan.Actions, haPlannedAction{
				Kind:        haActionCreateSlot,
				StandbyName: standby.Name,
				SlotName:    slotName,
				TargetLSN:   initialStandbyLSN(standby, status.PrimaryLSN),
				Reason:      "SlotMissing",
			}, haPlannedAction{
				Kind:        haActionSeedStandby,
				DependsOn:   haActionCreateSlot,
				StandbyName: standby.Name,
				SlotName:    slotName,
				TargetLSN:   initialStandbyLSN(standby, status.PrimaryLSN),
				Reason:      "StandbyNeedsBaseBackup",
			})
			plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, initialStandbyLSN(standby, status.PrimaryLSN), "StandbyNeedsBaseBackup", haActionSeedStandby)...)
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
			plan.Actions = append(plan.Actions, haPlannedAction{
				Kind:        haActionMarkReseed,
				StandbyName: standby.Name,
				SlotName:    slotName,
				TargetLSN:   status.PrimaryLSN,
				Reason:      "SlotRequiresReseed",
			})
			plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, status.PrimaryLSN, "SlotRequiresReseed", haActionMarkReseed)...)
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
		plan.Actions = append(plan.Actions,
			haPlannedAction{
				Kind:            haActionAcquireFence,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       status.PrimaryLSN,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				FenceReason:     fence.Reason,
				Reason:          "AutomaticFailoverReady",
			},
			haPlannedAction{
				Kind:            haActionPromoteStandby,
				DependsOn:       haActionAcquireFence,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       status.PrimaryLSN,
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
				TargetLSN:       status.PrimaryLSN,
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
				StandbyName:     haAutomaticFailoverFormerPrimaryID(ha),
				TargetLSN:       status.PrimaryLSN,
				ObservedLSN:     status.PrimaryLSN,
				RetainedFromLSN: status.Retention.OldestRestartLSN,
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
	if action := haFormerPrimaryPlannedAction(plan.FormerPrimary, status); action.Kind != "" {
		plan.Actions = append(plan.Actions, action)
		if action.Kind == haActionReseedFormerPrimary {
			if standby, ok := haStandbySpecByName(ha, action.StandbyName); ok {
				slotName := standbySlotName(standby)
				seed := haPlannedAction{
					Kind:        haActionSeedStandby,
					DependsOn:   haActionReseedFormerPrimary,
					StandbyName: standby.Name,
					SlotName:    slotName,
					TargetLSN:   status.PrimaryLSN,
					Reason:      "FormerPrimaryRequiresReseed",
				}
				plan.Actions = append(plan.Actions, seed)
				plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, status.PrimaryLSN, "FormerPrimaryRequiresReseed", haActionSeedStandby)...)
			}
		}
	}

	return plan
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
	cluster.Status.HAStatus.PlannedActions = haPlannedActionStatuses(plan.Actions, ha, cluster.Status.HAStatus)
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

func haPlannedActionStatuses(actions []haPlannedAction, ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) []antflyv1.HAPlannedActionStatus {
	if len(actions) == 0 {
		return nil
	}
	out := make([]antflyv1.HAPlannedActionStatus, 0, len(actions))
	for _, action := range actions {
		adminMethod, adminPath := haAdminOperation(action)
		statusAction := antflyv1.HAPlannedActionStatus{
			Kind:             string(action.Kind),
			Phase:            string(haPlannedActionPhase(action.Kind)),
			Executor:         string(haPlannedActionExecutor(action.Kind)),
			DependsOn:        string(action.DependsOn),
			StandbyName:      action.StandbyName,
			SlotName:         action.SlotName,
			TargetLSN:        action.TargetLSN,
			ObservedLSN:      action.ObservedLSN,
			RetainedFromLSN:  action.RetainedFromLSN,
			RouteFrom:        action.RouteFrom,
			RouteTo:          action.RouteTo,
			FenceAuthority:   action.FenceAuthority,
			FenceHolder:      action.FenceHolder,
			FenceGeneration:  action.FenceGeneration,
			FenceReason:      action.FenceReason,
			SeedManifestPath: action.SeedManifestPath,
			SeedContentRoot:  action.SeedContentRoot,
			AdminCommand:     haAdminCommand(action, haReplicationIdentity(ha), status),
			AdminURL:         haAdminURL(action, ha),
			AdminMethod:      adminMethod,
			AdminPath:        adminPath,
			Reason:           action.Reason,
		}
		statusAction = haPreservePlannedActionExecution(statusAction, status)
		out = append(out, statusAction)
	}
	return out
}

func haPreservePlannedActionExecution(action antflyv1.HAPlannedActionStatus, status *antflyv1.HAStatus) antflyv1.HAPlannedActionStatus {
	if status == nil {
		return action
	}
	for _, previous := range status.PlannedActions {
		if !haSamePlannedActionOperation(action, previous) {
			continue
		}
		if previous.AdminJobName == haAdminDirectAPIName &&
			previous.AdminJobPhase == haAdminJobPhaseSucceeded &&
			!haAdminActionSucceededWithEvidence(previous) {
			return action
		}
		action.AdminJobName = previous.AdminJobName
		action.AdminJobPhase = previous.AdminJobPhase
		action.AdminError = previous.AdminError
		if previous.AdminResult != nil {
			action.AdminResult = previous.AdminResult.DeepCopy()
		}
		return action
	}
	return action
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
		a.AdminMethod == b.AdminMethod &&
		a.AdminPath == b.AdminPath &&
		a.SeedManifestPath == b.SeedManifestPath &&
		a.SeedContentRoot == b.SeedContentRoot &&
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
	case "admin_command":
		return haActionExecutorAdminCommand, true
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
	case haActionAcquireFence:
		return haActionPhaseFence
	case haActionPromoteStandby:
		return haActionPhasePromote
	case haActionUpdatePrimaryRoute:
		return haActionPhaseRoute
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		return haActionPhaseRejoin
	default:
		return haActionPhaseReconcile
	}
}

func haPlannedActionExecutor(kind haActionKind) haActionExecutor {
	if kind == haActionUpdatePrimaryRoute {
		return haActionExecutorControllerAction
	}
	return haActionExecutorAdminCommand
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
		return []string{"slot", "create", "--slot", slotName, "--initial-lsn", strconv.FormatUint(action.TargetLSN, 10)}
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
		if slotName == "" {
			return nil
		}
		return []string{"seed", "begin", "--slot", slotName, "--manifest-id", fmt.Sprintf("base-%s-%d", slotName, action.TargetLSN)}
	case haActionFinishStandbySeed:
		if action.SeedManifestPath == "" {
			return nil
		}
		return []string{"seed", "finish", "--manifest", action.SeedManifestPath}
	case haActionBootstrapStandbySeed:
		if action.SeedManifestPath == "" {
			return nil
		}
		args := []string{"seed", "bootstrap", "--manifest", action.SeedManifestPath}
		if action.SeedContentRoot != "" {
			args = append(args, "--content-root", action.SeedContentRoot)
		}
		return args
	case haActionPromoteStandby:
		return []string{"promote", "--current-fence"}
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		return haFormerPrimaryAdminCommand(action, identity, status)
	case haActionAcquireFence:
		if identity == nil || identity.CurrentPrimaryID == "" || action.StandbyName == "" {
			return nil
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
			"--required-lsn", strconv.FormatUint(action.TargetLSN, 10),
			"--observed-lsn", strconv.FormatUint(action.TargetLSN, 10),
			"--reason", action.Reason,
		}
	default:
		return nil
	}
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

func haAdminURL(action haPlannedAction, ha *antflyv1.HighAvailabilitySpec) string {
	if ha == nil {
		return ""
	}
	switch action.Kind {
	case haActionCreateSlot, haActionResumeSlot, haActionPauseSlot, haActionDropSlot, haActionSeedStandby, haActionFinishStandbySeed, haActionMarkReseed:
		if ha.Admin == nil {
			return ""
		}
		return ha.Admin.PrimaryURL
	case haActionAcquireFence:
		if url := haStandbyAdminURL(ha, action.StandbyName); url != "" {
			return url
		}
		if ha.Admin == nil {
			return ""
		}
		return ha.Admin.PrimaryURL
	case haActionBootstrapStandbySeed:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionPromoteStandby:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionReseedFormerPrimary:
		if ha.Admin == nil {
			return ""
		}
		return ha.Admin.PrimaryURL
	default:
		return ""
	}
}

func haAdminOperation(action haPlannedAction) (string, string) {
	switch action.Kind {
	case haActionCreateSlot:
		return "POST", haAdminReplicationSlotsPath
	case haActionResumeSlot:
		if slotName := haPlannedActionSlotName(action); slotName != "" {
			return "PUT", haAdminReplicationSlotPathPrefix + url.PathEscape(slotName) + haAdminReplicationSlotResumePathSuffix
		}
	case haActionPauseSlot:
		if slotName := haPlannedActionSlotName(action); slotName != "" {
			return "PUT", haAdminReplicationSlotPathPrefix + url.PathEscape(slotName) + haAdminReplicationSlotPausePathSuffix
		}
	case haActionDropSlot:
		if slotName := haPlannedActionSlotName(action); slotName != "" {
			return "DELETE", haAdminReplicationSlotPathPrefix + url.PathEscape(slotName)
		}
	case haActionSeedStandby, haActionMarkReseed:
		return "POST", haAdminBaseBackupsPath
	case haActionFinishStandbySeed:
		return "POST", haAdminBaseBackupsFinishPath
	case haActionBootstrapStandbySeed:
		return "POST", haAdminStandbyBootstrapPath
	case haActionAcquireFence:
		return "POST", haAdminFencePath
	case haActionPromoteStandby:
		return "POST", haAdminPromotionCurrentFencePath
	case haActionDemoteFormerPrimary:
		return "POST", haAdminRejoinAssessPath
	case haActionRewindFormerPrimary:
		return "POST", haAdminRejoinRewindPath
	case haActionReseedFormerPrimary:
		return "POST", haAdminRejoinReseedPath
	}
	return "", ""
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
	fenceReason := ""
	if status != nil {
		retainedFromLSN = status.Retention.OldestRestartLSN
		if status.LastPromotion != nil {
			fenceReason = status.LastPromotion.FenceReason
		}
	}
	switch evaluation.Action {
	case string(haActionDemoteFormerPrimary), string(haActionRewindFormerPrimary), string(haActionReseedFormerPrimary):
		return haPlannedAction{
			Kind:            haActionKind(evaluation.Action),
			StandbyName:     evaluation.NodeID,
			TargetLSN:       evaluation.SwitchLSN,
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
	for _, desired := range ha.Standbys {
		if !standbyDesired(desired) {
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
	} else {
		setHACondition(cluster, antflyv1.TypeHADegraded, metav1.ConditionFalse, "HASyncPolicySatisfied", "Synchronous HA policy is satisfied or not configured")
	}

	if plan.UnhealthyStandbyCount > 0 {
		setHACondition(cluster, antflyv1.TypeHAUnhealthy, metav1.ConditionTrue, "HAStandbyUnhealthy", fmt.Sprintf("%d desired standby is missing, inactive, or reporting replication errors", plan.UnhealthyStandbyCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHAUnhealthy, metav1.ConditionFalse, "HAStandbysHealthy", "No desired standby is missing, inactive, or reporting replication errors")
	}

	if plan.LaggingStandbyCount > 0 {
		setHACondition(cluster, antflyv1.TypeHALagging, metav1.ConditionTrue, "HAStandbyLagging", fmt.Sprintf("%d desired standby has replication lag", plan.LaggingStandbyCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHALagging, metav1.ConditionFalse, "HANoLaggingStandbys", "No desired standby has replication lag")
	}

	retentionPressure := cluster.Status.HAStatus != nil && cluster.Status.HAStatus.Retention.ReseedRecommended > 0
	if retentionPressure {
		setHACondition(cluster, antflyv1.TypeHARetentionPressure, metav1.ConditionTrue, "HARetentionCapExceeded", "One or more slots are forcing WAL retention beyond policy")
	} else {
		setHACondition(cluster, antflyv1.TypeHARetentionPressure, metav1.ConditionFalse, "HARetentionWithinPolicy", "WAL retention is within configured policy")
	}

	if plan.ReseedRequiredCount > 0 {
		setHACondition(cluster, antflyv1.TypeHAReseedRequired, metav1.ConditionTrue, "HAStandbyRequiresReseed", fmt.Sprintf("%d desired standby requires reseed", plan.ReseedRequiredCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHAReseedRequired, metav1.ConditionFalse, "HANoReseedRequired", "No desired standby requires reseed")
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
	if ha.AutomaticFailover.FencingAuthority == "" || ha.AutomaticFailover.FencingAuthority == antflyv1.HAFencingAuthorityNone {
		return ""
	}
	if !plan.FencingReady {
		return ""
	}
	if !haPrimaryAdminUnavailable(status) {
		return ""
	}
	if !haPromotionBoundaryReady(status) {
		return ""
	}
	if plan.PromotionAlreadyRecorded {
		return ""
	}
	if !plan.PromotionRouteReady {
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
		if !standby.Active || standby.ReseedRequired {
			continue
		}
		if standby.ReceivedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		if requireApply && standby.AppliedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		if standbySafeReadLSN(standby)+maxLag < status.PrimaryLSN {
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
		if len(desired.RouteSelector) == 0 {
			continue
		}
		standby, ok := standbys[desired.Name]
		if !ok {
			standby, ok = standbys[standbySlotName(desired)]
		}
		if !ok || !standby.Active || standby.ReseedRequired {
			continue
		}
		if standby.ReceivedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		if requireApply && standby.AppliedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		if standbySafeReadLSN(standby)+maxLag < status.PrimaryLSN {
			continue
		}
		return desired.Name
	}
	return ""
}

func standbyReadSafe(status *antflyv1.HAStatus, standby antflyv1.HAStandbyStatus) bool {
	if !standby.Active || standby.ReseedRequired {
		return false
	}
	return standbySafeReadLSN(standby) >= status.PrimaryLSN
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
	if !desiredStandbyNamed(ha, fencing.Holder) {
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
	if ha == nil || standbyName == "" {
		return false
	}
	for _, standby := range ha.Standbys {
		if standbyDesired(standby) && standby.Name == standbyName {
			return len(standby.RouteSelector) > 0
		}
	}
	return false
}

func haAutomaticFailoverReason(ha *antflyv1.HighAvailabilitySpec, plan haPlan) string {
	if plan.AutomaticPromotionAllowed {
		return "HAFencedPromotionReady"
	}
	if ha == nil || ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled {
		return "HAAutomaticFailoverDisabled"
	}
	if ha.AutomaticFailover.FencingAuthority == "" || ha.AutomaticFailover.FencingAuthority == antflyv1.HAFencingAuthorityNone {
		return antflyv1.ReasonHAFencingAuthorityMissing
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

func haPrimaryAdminUnavailable(status *antflyv1.HAStatus) bool {
	return status != nil && !status.PrimaryAdminReachable && status.PrimaryAdminLastError != ""
}

func haPromotionBoundaryReady(status *antflyv1.HAStatus) bool {
	return status != nil && status.PrimaryLSN > 0
}

func haPromotionAlreadyRecorded(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus) bool {
	if status == nil || status.LastPromotion == nil {
		return false
	}
	identity := haReplicationIdentity(ha)
	if identity == nil {
		return false
	}
	promotion := status.LastPromotion
	return promotion.OldPrimaryID == identity.CurrentPrimaryID &&
		promotion.ParentTimelineID == identity.TimelineID &&
		promotion.ParentEpoch == identity.Epoch &&
		promotion.PromotedStandbyID != "" &&
		promotion.NewTimelineID != 0 &&
		promotion.NewEpoch != 0 &&
		promotion.SwitchLSN != 0
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
	evaluation.Required = policy.Required
	if evaluation.Required == 0 {
		evaluation.Required = 1
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
		evaluation.Required = int32(len(policy.StandbyNames))
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
		if status.LastPromotion != nil {
			evaluation.FenceAuthority = status.LastPromotion.FenceAuthority
			evaluation.FenceGeneration = status.LastPromotion.FenceGeneration
			if status.LastPromotion.PromotedStandbyID != "" {
				evaluation.DesiredTarget = status.LastPromotion.PromotedStandbyID
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
		(evaluation.FenceAuthority != "" && currentFenceAuthority != "" && currentFenceAuthority != evaluation.FenceAuthority) ||
		(evaluation.FenceGeneration > 0 && currentFenceGeneration < evaluation.FenceGeneration)
	if evaluation.Stale {
		evaluation.Action = string(haActionUpdatePrimaryRoute)
		if evaluation.CurrentTarget != evaluation.DesiredTarget {
			evaluation.Reason = "PrimaryRouteTargetChanged"
		} else if evaluation.FenceAuthority != "" && currentFenceAuthority != "" && currentFenceAuthority != evaluation.FenceAuthority {
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
	if haApplyFormerPrimaryAssessment(&evaluation, status.FormerPrimary, promotion) {
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
	if promotion.FenceGeneration == 0 {
		return false
	}
	if promotion.FenceAuthority != "" && status.Fencing.Authority != promotion.FenceAuthority {
		return false
	}
	if status.Fencing.Generation < promotion.FenceGeneration {
		return false
	}
	if promotion.PromotedStandbyID != "" && status.Fencing.Holder != promotion.PromotedStandbyID {
		return false
	}
	return status.Fencing.Ready
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
	return standby.Active && !standby.ReseedRequired
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
	if ha == nil || name == "" {
		return false
	}
	for _, standby := range ha.Standbys {
		if standbyDesired(standby) && standby.Name == name {
			return true
		}
	}
	return false
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
