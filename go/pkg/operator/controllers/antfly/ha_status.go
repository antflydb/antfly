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
)

type haActionKind string

const (
	haActionCreateSlot          haActionKind = "CreateSlot"
	haActionSeedStandby         haActionKind = "SeedStandby"
	haActionMarkReseed          haActionKind = "MarkReseed"
	haActionAcquireFence        haActionKind = "AcquireFence"
	haActionPromoteStandby      haActionKind = "PromoteStandby"
	haActionUpdatePrimaryRoute  haActionKind = "UpdatePrimaryRoute"
	haActionDemoteFormerPrimary haActionKind = "DemoteFormerPrimary"
	haActionRewindFormerPrimary haActionKind = "RewindFormerPrimary"
	haActionReseedFormerPrimary haActionKind = "ReseedFormerPrimary"
)

type haPlannedAction struct {
	Kind            haActionKind
	StandbyName     string
	SlotName        string
	TargetLSN       uint64
	RouteTo         string
	FenceAuthority  antflyv1.HAFencingAuthority
	FenceHolder     string
	FenceGeneration uint64
	Reason          string
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
	}

	var plan haPlan
	for _, standby := range ha.Standbys {
		if !standbyDesired(standby) {
			continue
		}
		plan.DesiredStandbyCount++
		observed, ok := slotByName[standby.Name]
		slotName := standbySlotName(standby)
		if !ok {
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
			continue
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
				TargetLSN:       status.PrimaryLSN,
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
	if action := haFormerPrimaryPlannedAction(plan.FormerPrimary); action.Kind != "" {
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
	cluster.Status.HAStatus.ReadSafeStandbyCount = plan.ReadSafeStandbyCount
	cluster.Status.HAStatus.ReseedRequiredCount = plan.ReseedRequiredCount
	cluster.Status.HAStatus.AutomaticPromotionAllowed = plan.AutomaticPromotionAllowed
	cluster.Status.HAStatus.PlannedActions = haPlannedActionStatuses(plan.Actions, ha)
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

func haPlannedActionStatuses(actions []haPlannedAction, ha *antflyv1.HighAvailabilitySpec) []antflyv1.HAPlannedActionStatus {
	if len(actions) == 0 {
		return nil
	}
	out := make([]antflyv1.HAPlannedActionStatus, 0, len(actions))
	for _, action := range actions {
		out = append(out, antflyv1.HAPlannedActionStatus{
			Kind:            string(action.Kind),
			StandbyName:     action.StandbyName,
			SlotName:        action.SlotName,
			TargetLSN:       action.TargetLSN,
			RouteTo:         action.RouteTo,
			FenceAuthority:  action.FenceAuthority,
			FenceHolder:     action.FenceHolder,
			FenceGeneration: action.FenceGeneration,
			AdminCommand:    haAdminCommand(action, haReplicationIdentity(ha)),
			AdminURL:        haAdminURL(action, ha),
			Reason:          action.Reason,
		})
	}
	return out
}

func haAdminCommand(action haPlannedAction, identity *antflyv1.HAReplicationIdentitySpec) []string {
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
	case haActionSeedStandby, haActionMarkReseed:
		slotName := action.SlotName
		if slotName == "" {
			slotName = action.StandbyName
		}
		if slotName == "" {
			return nil
		}
		return []string{"seed", "begin", "--slot", slotName, "--manifest-id", fmt.Sprintf("base-%s-%d", slotName, action.TargetLSN)}
	case haActionPromoteStandby:
		return []string{"promote", "--current-fence"}
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

func haAdminURL(action haPlannedAction, ha *antflyv1.HighAvailabilitySpec) string {
	if ha == nil {
		return ""
	}
	switch action.Kind {
	case haActionCreateSlot, haActionSeedStandby, haActionMarkReseed, haActionAcquireFence:
		if ha.Admin == nil {
			return ""
		}
		return ha.Admin.PrimaryURL
	case haActionPromoteStandby:
		return haStandbyAdminURL(ha, action.StandbyName)
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

func haFormerPrimaryPlannedAction(evaluation haFormerPrimaryEvaluation) haPlannedAction {
	if !evaluation.Present {
		return haPlannedAction{}
	}
	switch evaluation.Action {
	case string(haActionDemoteFormerPrimary), string(haActionRewindFormerPrimary), string(haActionReseedFormerPrimary):
		return haPlannedAction{
			Kind:            haActionKind(evaluation.Action),
			StandbyName:     evaluation.NodeID,
			TargetLSN:       evaluation.SwitchLSN,
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
	}
	merged := make([]antflyv1.HAStandbyStatus, 0, len(ha.Standbys))
	for _, desired := range ha.Standbys {
		if !standbyDesired(desired) {
			continue
		}
		entry := observed[desired.Name]
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
