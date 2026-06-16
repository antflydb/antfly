package controllers

import (
	"fmt"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

type haPlannedAction struct {
	Kind        haActionKind
	StandbyName string
	SlotName    string
	TargetLSN   uint64
	Reason      string
}

type haPlan struct {
	Actions                   []haPlannedAction
	AutomaticPromotionAllowed bool
	DesiredStandbyCount       int32
	HealthyStandbyCount       int32
	ReseedRequiredCount       int32
}

func (r *AntflyClusterReconciler) updateHAStatusAndConditions(cluster *antflyv1.AntflyCluster) {
	plan := planHA(cluster)
	applyHAPlanStatus(cluster, plan)
	setHAConditions(cluster, plan)
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
	}

	plan.AutomaticPromotionAllowed = haAutomaticPromotionAllowed(ha, status, plan)
	if plan.AutomaticPromotionAllowed {
		plan.Actions = append(plan.Actions,
			haPlannedAction{Kind: haActionAcquireFence, TargetLSN: status.PrimaryLSN, Reason: "AutomaticFailoverReady"},
			haPlannedAction{Kind: haActionPromoteStandby, TargetLSN: status.PrimaryLSN, Reason: "AutomaticFailoverReady"},
			haPlannedAction{Kind: haActionUpdatePrimaryRoute, TargetLSN: status.PrimaryLSN, Reason: "PromotionPlanned"},
			haPlannedAction{Kind: haActionDemoteFormerPrimary, TargetLSN: status.PrimaryLSN, Reason: "PromotionPlanned"},
		)
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
	cluster.Status.HAStatus.ReseedRequiredCount = plan.ReseedRequiredCount
	cluster.Status.HAStatus.AutomaticPromotionAllowed = plan.AutomaticPromotionAllowed
	mergeConfiguredStandbys(cluster.Status.HAStatus, ha)
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

	if plan.HealthyStandbyCount > 0 {
		setHACondition(cluster, antflyv1.TypeHAAvailable, metav1.ConditionTrue, antflyv1.ReasonHAStandbyAvailable, fmt.Sprintf("%d desired standby is caught up to apply", plan.HealthyStandbyCount))
	} else {
		setHACondition(cluster, antflyv1.TypeHAAvailable, metav1.ConditionFalse, antflyv1.ReasonHANoHealthyStandby, "No desired standby is caught up to apply")
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

func haAutomaticPromotionAllowed(ha *antflyv1.HighAvailabilitySpec, status *antflyv1.HAStatus, plan haPlan) bool {
	if ha == nil || ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled {
		return false
	}
	if ha.AutomaticFailover.FencingAuthority == "" || ha.AutomaticFailover.FencingAuthority == antflyv1.HAFencingAuthorityNone {
		return false
	}
	if haSyncPolicyDegraded(ha, plan) {
		return false
	}
	if status == nil {
		return false
	}
	maxLag := ha.AutomaticFailover.MaximumLagLSN
	requireApply := ha.AutomaticFailover.RequireRemoteApply == nil || *ha.AutomaticFailover.RequireRemoteApply
	for _, standby := range status.Standbys {
		if !standby.Active || standby.ReseedRequired {
			continue
		}
		if standby.ReceivedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		if requireApply && standby.AppliedLSN+maxLag < status.PrimaryLSN {
			continue
		}
		return true
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
	if haSyncPolicyDegraded(ha, plan) {
		return antflyv1.ReasonHASyncPolicyUnsatisfied
	}
	return antflyv1.ReasonHANoHealthyStandby
}

func haSyncPolicyDegraded(ha *antflyv1.HighAvailabilitySpec, plan haPlan) bool {
	if ha == nil || ha.SyncPolicy == nil || ha.SyncPolicy.Mode == "" || ha.SyncPolicy.Mode == antflyv1.HADurabilityModeAsync {
		return false
	}
	required := ha.SyncPolicy.Required
	if required == 0 {
		required = 1
	}
	return plan.HealthyStandbyCount < required
}

func standbyDesired(standby antflyv1.HAStandbySpec) bool {
	return standby.Desired == nil || *standby.Desired
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
