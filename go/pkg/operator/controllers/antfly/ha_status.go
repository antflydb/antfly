package controllers

import (
	"context"
	"fmt"
	"strconv"
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

const haFencingLeaseDefaultDurationSeconds int32 = 30

type haPlannedAction struct {
	Kind             haActionKind
	StandbyName      string
	SlotName         string
	TargetLSN        uint64
	ObservedLSN      uint64
	RetainedFromLSN  uint64
	RouteTo          string
	FenceAuthority   antflyv1.HAFencingAuthority
	FenceHolder      string
	FenceGeneration  uint64
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
	FenceGeneration    uint64
	Action             string
	Reason             string
}

type haPrimaryRouteEvaluation struct {
	ServiceName     string
	CurrentTarget   string
	DesiredTarget   string
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
	PromotionStandbyName      string
	SyncPolicyDegraded        bool
	SyncPolicy                haSyncEvaluation
	FormerPrimary             haFormerPrimaryEvaluation
	PrimaryRoute              haPrimaryRouteEvaluation
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
				plan.Actions = append(plan.Actions, haPlannedAction{
					Kind:        haActionDropSlot,
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
				StandbyName: standby.Name,
				SlotName:    slotName,
				TargetLSN:   initialStandbyLSN(standby, status.PrimaryLSN),
				Reason:      "StandbyNeedsBaseBackup",
			})
			plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, initialStandbyLSN(standby, status.PrimaryLSN), "StandbyNeedsBaseBackup")...)
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
			plan.Actions = append(plan.Actions, haSeedCompletionActions(standby, slotName, status.PrimaryLSN, "SlotRequiresReseed")...)
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
				Reason:          "AutomaticFailoverReady",
			},
			haPlannedAction{
				Kind:            haActionPromoteStandby,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       status.PrimaryLSN,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				Reason:          "AutomaticFailoverReady",
			},
			haPlannedAction{
				Kind:            haActionUpdatePrimaryRoute,
				StandbyName:     plan.PromotionStandbyName,
				TargetLSN:       status.PrimaryLSN,
				RouteTo:         plan.PromotionStandbyName,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
				Reason:          "PromotionPlanned",
			},
			haPlannedAction{
				Kind:            haActionDemoteFormerPrimary,
				StandbyName:     haAutomaticFailoverFormerPrimaryID(ha),
				TargetLSN:       status.PrimaryLSN,
				ObservedLSN:     status.PrimaryLSN,
				RetainedFromLSN: status.Retention.OldestRestartLSN,
				FenceAuthority:  fence.Authority,
				FenceHolder:     fence.Holder,
				FenceGeneration: fence.Generation,
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
		out = append(out, antflyv1.HAPlannedActionStatus{
			Kind:             string(action.Kind),
			StandbyName:      action.StandbyName,
			SlotName:         action.SlotName,
			TargetLSN:        action.TargetLSN,
			ObservedLSN:      action.ObservedLSN,
			RetainedFromLSN:  action.RetainedFromLSN,
			RouteTo:          action.RouteTo,
			FenceAuthority:   action.FenceAuthority,
			FenceHolder:      action.FenceHolder,
			FenceGeneration:  action.FenceGeneration,
			SeedManifestPath: action.SeedManifestPath,
			SeedContentRoot:  action.SeedContentRoot,
			AdminCommand:     haAdminCommand(action, haReplicationIdentity(ha), status),
			AdminURL:         haAdminURL(action, ha),
			Reason:           action.Reason,
		})
	}
	return out
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
		"rejoin", "assess",
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
	case haActionCreateSlot, haActionResumeSlot, haActionPauseSlot, haActionDropSlot, haActionSeedStandby, haActionFinishStandbySeed, haActionMarkReseed, haActionAcquireFence:
		if ha.Admin == nil {
			return ""
		}
		return ha.Admin.PrimaryURL
	case haActionBootstrapStandbySeed:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionPromoteStandby:
		return haStandbyAdminURL(ha, action.StandbyName)
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		if url := haStandbyAdminURL(ha, action.StandbyName); url != "" {
			return url
		}
		if ha.Admin == nil {
			return ""
		}
		return ha.Admin.PrimaryURL
	default:
		return ""
	}
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

func haReplicationIdentity(ha *antflyv1.HighAvailabilitySpec) *antflyv1.HAReplicationIdentitySpec {
	if ha == nil || ha.Identity == nil {
		return nil
	}
	identity := ha.Identity
	if identity.ClusterID == 0 || identity.ShardID == 0 || identity.TableID == 0 ||
		identity.TimelineID == 0 || identity.Epoch == 0 || identity.CurrentPrimaryID == "" {
		return nil
	}
	return identity
}

func haSeedCompletionActions(standby antflyv1.HAStandbySpec, slotName string, targetLSN uint64, reason string) []haPlannedAction {
	if standby.SeedManifestPath == "" {
		return nil
	}
	return []haPlannedAction{
		{
			Kind:             haActionFinishStandbySeed,
			StandbyName:      standby.Name,
			SlotName:         slotName,
			TargetLSN:        targetLSN,
			SeedManifestPath: standby.SeedManifestPath,
			SeedContentRoot:  standby.SeedContentRoot,
			Reason:           reason,
		},
		{
			Kind:             haActionBootstrapStandbySeed,
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
		RouteTo:         evaluation.DesiredTarget,
		FenceGeneration: evaluation.FenceGeneration,
		Reason:          evaluation.Reason,
	}
	if status != nil {
		action.TargetLSN = status.PrimaryLSN
		action.FenceAuthority = status.Fencing.Authority
		action.FenceHolder = status.Fencing.Holder
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
	if status != nil {
		retainedFromLSN = status.Retention.OldestRestartLSN
	}
	switch evaluation.Action {
	case string(haActionDemoteFormerPrimary), string(haActionRewindFormerPrimary), string(haActionReseedFormerPrimary):
		return haPlannedAction{
			Kind:            haActionKind(evaluation.Action),
			StandbyName:     evaluation.NodeID,
			TargetLSN:       evaluation.SwitchLSN,
			ObservedLSN:     evaluation.ObservedLSN,
			RetainedFromLSN: retainedFromLSN,
			FenceGeneration: evaluation.FenceGeneration,
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
	if haSyncPolicyDegraded(ha, plan) {
		return antflyv1.ReasonHASyncPolicyUnsatisfied
	}
	return antflyv1.ReasonHANoHealthyStandby
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
	evaluation := haPrimaryRouteEvaluation{
		ServiceName:   cluster.Name + "-public-api",
		CurrentTarget: "primary",
		DesiredTarget: "primary",
		Action:        "None",
		Reason:        "PrimaryRouteCurrent",
	}
	if status != nil {
		if status.PrimaryRoute.CurrentTarget != "" {
			evaluation.CurrentTarget = status.PrimaryRoute.CurrentTarget
		}
		if status.LastPromotion != nil {
			evaluation.FenceGeneration = status.LastPromotion.FenceGeneration
			if status.LastPromotion.PromotedStandbyID != "" {
				evaluation.DesiredTarget = status.LastPromotion.PromotedStandbyID
			}
		}
	}
	if promotionTarget != "" {
		evaluation.DesiredTarget = promotionTarget
		if status != nil {
			evaluation.FenceGeneration = status.Fencing.Generation
		}
	}
	evaluation.Stale = evaluation.CurrentTarget != evaluation.DesiredTarget
	if evaluation.Stale {
		evaluation.Action = string(haActionUpdatePrimaryRoute)
		evaluation.Reason = "PrimaryRouteTargetChanged"
	}
	return evaluation
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

func haFormerPrimaryFenced(status *antflyv1.HAStatus, promotion *antflyv1.HAPromotionStatus) bool {
	if promotion.FenceGeneration == 0 {
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
