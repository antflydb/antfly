package controllers

import (
	"context"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestUpdateHAStatusDisabledClearsStatusAndPublishesConditions(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "antfly",
			Namespace:  "default",
			Generation: 3,
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{PrimaryLSN: 10},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus != nil {
		t.Fatalf("expected disabled HA to clear HAStatus, got %#v", cluster.Status.HAStatus)
	}
	available := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAvailable)
	if available == nil {
		t.Fatalf("expected %s condition", antflyv1.TypeHAAvailable)
	}
	if available.Status != metav1.ConditionTrue || available.Reason != antflyv1.ReasonHADisabled {
		t.Fatalf("expected disabled available condition, got status=%s reason=%s", available.Status, available.Reason)
	}
}

func TestPlanHAPlansSlotAndBaseBackupForMissingStandby(t *testing.T) {
	initial := uint64(5)
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 9}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{
		{Name: "standby-a", InitialLSN: &initial},
	}

	plan := planHA(cluster)

	if plan.DesiredStandbyCount != 1 {
		t.Fatalf("expected one desired standby, got %d", plan.DesiredStandbyCount)
	}
	if len(plan.Actions) != 2 {
		t.Fatalf("expected create-slot and seed actions, got %#v", plan.Actions)
	}
	if plan.Actions[0].Kind != haActionCreateSlot || plan.Actions[0].TargetLSN != initial {
		t.Fatalf("unexpected first action: %#v", plan.Actions[0])
	}
	if plan.Actions[1].Kind != haActionSeedStandby || plan.Actions[1].TargetLSN != initial {
		t.Fatalf("unexpected second action: %#v", plan.Actions[1])
	}

	reconciler := &AntflyClusterReconciler{}
	reconciler.updateHAStatusAndConditions(cluster)
	if len(cluster.Status.HAStatus.PlannedActions) != 2 {
		t.Fatalf("expected planned actions in status, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionCreateSlot) ||
		cluster.Status.HAStatus.PlannedActions[0].SlotName != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[0].TargetLSN != initial {
		t.Fatalf("unexpected planned create-slot status: %#v", cluster.Status.HAStatus.PlannedActions[0])
	}
}

func TestUpdateHAStatusReportsReseedAndDegradedSync(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:         antflyv1.HADurabilityModeRemoteApply,
		Required:     1,
		StandbyNames: []string{"standby-a"},
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 10,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:           "standby-a",
			SlotName:       "standby-a",
			Active:         true,
			ReseedRequired: true,
			ReceivedLSN:    2,
			AppliedLSN:     1,
			ApplyLagLSN:    9,
			Status:         "reseed_required",
		}},
		Retention: antflyv1.HARetentionStatus{ReseedRecommended: 1},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.ReseedRequiredCount != 1 {
		t.Fatalf("expected reseed count=1, got %d", cluster.Status.HAStatus.ReseedRequiredCount)
	}
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != antflyv1.ReasonHASyncPolicyUnsatisfied {
		t.Fatalf("expected degraded sync condition, got %#v", degraded)
	}
	retention := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHARetentionPressure)
	if retention == nil || retention.Status != metav1.ConditionTrue {
		t.Fatalf("expected retention pressure condition, got %#v", retention)
	}
	reseed := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAReseedRequired)
	if reseed == nil || reseed.Status != metav1.ConditionTrue {
		t.Fatalf("expected reseed required condition, got %#v", reseed)
	}
}

func TestUpdateHAStatusAllowsAutomaticPromotionOnlyWithFenceAndCaughtUpStandby(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:         antflyv1.HADurabilityModeRemoteApply,
		Required:     1,
		StandbyNames: []string{"standby-a"},
	}
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityNone,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked without fencing")
	}
	failover := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAFencingAuthorityMissing {
		t.Fatalf("expected missing-fence condition, got %#v", failover)
	}

	cluster.Spec.HighAvailability.AutomaticFailover.FencingAuthority = antflyv1.HAFencingAuthorityKubernetesLease
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked until fencing is observed ready")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAFencingNotReady {
		t.Fatalf("expected fence-not-ready condition, got %#v", failover)
	}

	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityStorageFence,
		Ready:      true,
		Holder:     "standby-a",
		Generation: 1,
		Reason:     "WrongAuthority",
	}
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked with mismatched observed fencing authority")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAFencingNotReady {
		t.Fatalf("expected fence-not-ready condition for mismatched authority, got %#v", failover)
	}

	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
		Ready:      true,
		Holder:     "standby-b",
		Generation: 1,
		Reason:     "WrongHolder",
	}
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked with undesired fence holder")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAFencingNotReady {
		t.Fatalf("expected fence-not-ready condition for undesired holder, got %#v", failover)
	}

	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	reconciler.updateHAStatusAndConditions(cluster)

	if !cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be allowed with ready fencing and caught-up standby")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionTrue || failover.Reason != "HAFencedPromotionReady" {
		t.Fatalf("expected failover-ready condition, got %#v", failover)
	}
	plan := planHA(cluster)
	if len(plan.Actions) != 4 {
		t.Fatalf("expected fenced promotion action chain, got %#v", plan.Actions)
	}
	if plan.PromotionStandbyName != "standby-a" {
		t.Fatalf("expected promotion standby standby-a, got %q", plan.PromotionStandbyName)
	}
	if plan.Actions[0].Kind != haActionAcquireFence || plan.Actions[1].Kind != haActionPromoteStandby {
		t.Fatalf("unexpected promotion actions: %#v", plan.Actions)
	}
	if plan.Actions[1].StandbyName != "standby-a" || plan.Actions[2].RouteTo != "standby-a" {
		t.Fatalf("expected promotion and route actions to target standby-a, got %#v", plan.Actions)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 4 {
		t.Fatalf("expected fenced promotion action chain in status, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionAcquireFence) ||
		cluster.Status.HAStatus.PlannedActions[1].Kind != string(haActionPromoteStandby) {
		t.Fatalf("unexpected promotion action status: %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[1].StandbyName != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[2].RouteTo != "standby-a" {
		t.Fatalf("expected planned action status to publish route target, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		cluster.Status.HAStatus.PlannedActions[0].FenceHolder != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[0].FenceGeneration != 1 {
		t.Fatalf("expected planned action to publish fence precondition, got %#v", cluster.Status.HAStatus.PlannedActions[0])
	}
	route := cluster.Status.HAStatus.PrimaryRoute
	if route.ServiceName != "antfly-public-api" ||
		route.CurrentTarget != "primary" ||
		route.DesiredTarget != "standby-a" ||
		!route.Stale ||
		route.Action != string(haActionUpdatePrimaryRoute) {
		t.Fatalf("expected stale route to promoted standby, got %#v", route)
	}
}

func TestUpdateHAStatusRequiresSafeReadProgressForAvailabilityAndAutomaticPromotion(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:         antflyv1.HADurabilityModeRemoteApply,
		Required:     1,
		StandbyNames: []string{"standby-a"},
	}
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	cluster.Status.HAStatus.Standbys[0].SafeReadLSN = 10
	cluster.Status.HAStatus.Standbys[0].SafeReadLagLSN = 2
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.HealthyStandbyCount != 1 {
		t.Fatalf("expected applied standby to remain healthy for sync, got %d", cluster.Status.HAStatus.HealthyStandbyCount)
	}
	if cluster.Status.HAStatus.ReadSafeStandbyCount != 0 {
		t.Fatalf("expected no read-safe standby, got %d", cluster.Status.HAStatus.ReadSafeStandbyCount)
	}
	available := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAvailable)
	if available == nil || available.Status != metav1.ConditionFalse || available.Reason != antflyv1.ReasonHANoHealthyStandby {
		t.Fatalf("expected unavailable read-safe condition, got %#v", available)
	}
	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to wait for safe-read progress")
	}

	cluster.Status.HAStatus.Standbys[0].SafeReadLSN = 12
	cluster.Status.HAStatus.Standbys[0].SafeReadLagLSN = 0
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.ReadSafeStandbyCount != 1 {
		t.Fatalf("expected one read-safe standby, got %d", cluster.Status.HAStatus.ReadSafeStandbyCount)
	}
	if !cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion after safe-read catch-up")
	}
}

func TestUpdateHAStatusRespectsSyncPolicySelectionSemantics(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{
		{Name: "standby-a"},
		{Name: "standby-b"},
		{Name: "standby-c"},
	}
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:          antflyv1.HADurabilityModeRemoteWrite,
		Selection:     antflyv1.HAStandbySelectionAny,
		Required:      2,
		StandbyNames:  []string{"standby-a", "standby-b", "standby-c"},
		FailurePolicy: antflyv1.HAFailurePolicyFailClosed,
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 10,
		Standbys: []antflyv1.HAStandbyStatus{
			{Name: "standby-a", SlotName: "standby-a", Active: true, ReceivedLSN: 10, AppliedLSN: 1, ApplyLagLSN: 9, Status: "receiving"},
			{Name: "standby-b", SlotName: "standby-b", Active: true, ReceivedLSN: 9, AppliedLSN: 9, ApplyLagLSN: 1, Status: "lagging"},
			{Name: "standby-c", SlotName: "standby-c", Active: true, ReceivedLSN: 10, AppliedLSN: 1, ApplyLagLSN: 9, Status: "receiving"},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.HealthyStandbyCount != 0 {
		t.Fatalf("expected no remote-apply healthy standbys, got %d", cluster.Status.HAStatus.HealthyStandbyCount)
	}
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionFalse {
		t.Fatalf("expected ANY remote-write sync policy to be satisfied by standby-a and standby-c, got %#v", degraded)
	}
	if cluster.Status.HAStatus.Sync.Mode != antflyv1.HADurabilityModeRemoteWrite ||
		cluster.Status.HAStatus.Sync.Selection != antflyv1.HAStandbySelectionAny ||
		cluster.Status.HAStatus.Sync.Required != 2 ||
		cluster.Status.HAStatus.Sync.Satisfied != 2 ||
		cluster.Status.HAStatus.Sync.Candidates != 3 ||
		cluster.Status.HAStatus.Sync.FailurePolicy != antflyv1.HAFailurePolicyFailClosed ||
		cluster.Status.HAStatus.Sync.Degraded ||
		cluster.Status.HAStatus.Sync.Action != "Satisfied" {
		t.Fatalf("unexpected satisfied sync status: %#v", cluster.Status.HAStatus.Sync)
	}

	cluster.Spec.HighAvailability.SyncPolicy.Selection = antflyv1.HAStandbySelectionFirst
	reconciler.updateHAStatusAndConditions(cluster)

	degraded = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != antflyv1.ReasonHASyncPolicyUnsatisfied {
		t.Fatalf("expected FIRST policy to require standby-a and standby-b, got %#v", degraded)
	}
	if cluster.Status.HAStatus.Sync.Satisfied != 1 ||
		cluster.Status.HAStatus.Sync.Candidates != 2 ||
		!cluster.Status.HAStatus.Sync.Degraded ||
		cluster.Status.HAStatus.Sync.Action != "RejectWrites" {
		t.Fatalf("unexpected fail-closed sync status: %#v", cluster.Status.HAStatus.Sync)
	}

	cluster.Spec.HighAvailability.SyncPolicy.Selection = antflyv1.HAStandbySelectionAll
	cluster.Spec.HighAvailability.SyncPolicy.FailurePolicy = antflyv1.HAFailurePolicyDegradeToAsync
	reconciler.updateHAStatusAndConditions(cluster)

	degraded = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionTrue {
		t.Fatalf("expected ALL policy to require every named standby, got %#v", degraded)
	}
	if cluster.Status.HAStatus.Sync.Action != "DegradeToAsync" {
		t.Fatalf("expected degraded sync action to surface degrade-to-async, got %#v", cluster.Status.HAStatus.Sync)
	}

	cluster.Status.HAStatus.Standbys[1].ReceivedLSN = 10
	reconciler.updateHAStatusAndConditions(cluster)

	degraded = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionFalse {
		t.Fatalf("expected ALL policy to be satisfied after standby-b receives primary LSN, got %#v", degraded)
	}
	if cluster.Status.HAStatus.Sync.Required != 3 ||
		cluster.Status.HAStatus.Sync.Satisfied != 3 ||
		cluster.Status.HAStatus.Sync.Candidates != 3 ||
		cluster.Status.HAStatus.Sync.Degraded ||
		cluster.Status.HAStatus.Sync.Action != "Satisfied" {
		t.Fatalf("unexpected satisfied ALL sync status: %#v", cluster.Status.HAStatus.Sync)
	}
}

func TestUpdateHAStatusReportsFormerPrimaryRejoinDisposition(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{
		{Name: "old-primary"},
		{Name: "standby-a"},
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
			CurrentTarget: "standby-a",
		},
		Fencing: antflyv1.HAFencingStatus{
			Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
			Ready:      true,
			Holder:     "standby-a",
			Generation: 4,
			Reason:     "LeaseHeld",
		},
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "old-primary",
			PromotedStandbyID: "standby-a",
			ParentTimelineID:  1,
			NewTimelineID:     2,
			SwitchLSN:         10,
			FenceGeneration:   4,
		},
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "old-primary",
			SlotName:    "old-primary",
			Active:      true,
			TimelineID:  1,
			ReceivedLSN: 10,
			AppliedLSN:  10,
		}, {
			Name:        "standby-a",
			SlotName:    "standby-a",
			Active:      true,
			TimelineID:  2,
			ReceivedLSN: 12,
			AppliedLSN:  12,
			SafeReadLSN: 12,
		}},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	former := cluster.Status.HAStatus.FormerPrimary
	if former == nil {
		t.Fatal("expected former-primary status")
	}
	if former.NodeID != "old-primary" ||
		!former.Fenced ||
		!former.RejoinRequired ||
		!former.RewindPossible ||
		former.ReseedRequired ||
		former.Action != string(haActionRewindFormerPrimary) ||
		former.Reason != "FormerPrimaryNeedsRewind" {
		t.Fatalf("unexpected rewind disposition: %#v", former)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionRewindFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[0].StandbyName != "old-primary" ||
		cluster.Status.HAStatus.PlannedActions[0].TargetLSN != 10 {
		t.Fatalf("expected rewind planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.Standbys[0].ReceivedLSN = 11
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		!former.ReseedRequired ||
		!former.Diverged ||
		former.RewindPossible ||
		former.Action != string(haActionReseedFormerPrimary) ||
		former.Reason != "FormerPrimaryRequiresReseed" {
		t.Fatalf("unexpected reseed disposition: %#v", former)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionReseedFormerPrimary) {
		t.Fatalf("expected reseed planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.Standbys[0].TimelineID = 2
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		former.RejoinRequired ||
		former.RewindPossible ||
		former.ReseedRequired ||
		former.Action != "None" ||
		former.Reason != "FormerPrimaryOnPromotionTimeline" {
		t.Fatalf("unexpected joined disposition: %#v", former)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 0 {
		t.Fatalf("expected no former-primary planned action after rejoin, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestUpdateHAStatusPlansPrimaryRouteAfterCompletedPromotion(t *testing.T) {
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
			CurrentTarget: "primary",
		},
		LastPromotion: &antflyv1.HAPromotionStatus{
			PromotedStandbyID: "standby-a",
			FenceGeneration:   5,
		},
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "standby-a",
			SlotName:    "standby-a",
			Active:      true,
			TimelineID:  2,
			ReceivedLSN: 12,
			AppliedLSN:  12,
			SafeReadLSN: 12,
		}},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	route := cluster.Status.HAStatus.PrimaryRoute
	if route.CurrentTarget != "primary" ||
		route.DesiredTarget != "standby-a" ||
		route.FenceGeneration != 5 ||
		!route.Stale ||
		route.Action != string(haActionUpdatePrimaryRoute) {
		t.Fatalf("expected route update after completed promotion, got %#v", route)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionUpdatePrimaryRoute) ||
		cluster.Status.HAStatus.PlannedActions[0].RouteTo != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[0].FenceGeneration != 5 {
		t.Fatalf("expected route planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.PrimaryRoute.CurrentTarget = "standby-a"
	reconciler.updateHAStatusAndConditions(cluster)

	route = cluster.Status.HAStatus.PrimaryRoute
	if route.Stale || route.Action != "None" || route.DesiredTarget != "standby-a" {
		t.Fatalf("expected current route after update, got %#v", route)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 0 {
		t.Fatalf("expected no route planned action once current, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestObserveHAFencingStatusReportsMissingKubernetesLease(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	reconciler := testHAReconciler(t)

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe fencing status: %v", err)
	}

	if cluster.Status.HAStatus == nil {
		t.Fatal("expected HA status to be initialized")
	}
	fencing := cluster.Status.HAStatus.Fencing
	if fencing.Authority != antflyv1.HAFencingAuthorityKubernetesLease ||
		fencing.Ready ||
		fencing.Reason != "LeaseMissing" {
		t.Fatalf("expected missing lease fencing status, got %#v", fencing)
	}
}

func TestObserveHAFencingStatusReportsExpiredKubernetesLease(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	lease := haFenceLease(cluster, time.Now().Add(-time.Minute), 10, 2, "standby-a")
	reconciler := testHAReconciler(t, lease)

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe fencing status: %v", err)
	}

	fencing := cluster.Status.HAStatus.Fencing
	if fencing.Ready || fencing.Holder != "standby-a" || fencing.Generation != 2 || fencing.Reason != "LeaseExpired" {
		t.Fatalf("expected expired lease fencing status, got %#v", fencing)
	}
}

func TestObserveHAFencingStatusAllowsPromotionWithReadyKubernetesLease(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:         antflyv1.HADurabilityModeRemoteApply,
		Required:     1,
		StandbyNames: []string{"standby-a"},
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	lease := haFenceLease(cluster, time.Now(), 30, 3, "standby-a")
	reconciler := testHAReconciler(t, lease)

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe fencing status: %v", err)
	}
	reconciler.updateHAStatusAndConditions(cluster)

	fencing := cluster.Status.HAStatus.Fencing
	if !fencing.Ready || fencing.Holder != "standby-a" || fencing.Generation != 3 || fencing.Reason != "LeaseHeld" {
		t.Fatalf("expected ready lease fencing status, got %#v", fencing)
	}
	if !cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected ready Kubernetes lease to satisfy automatic promotion fencing gate")
	}
}

func haCluster() *antflyv1.AntflyCluster {
	return &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "antfly",
			Namespace:  "default",
			Generation: 7,
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name: "standby-a",
				}},
			},
		},
	}
}

func haClusterWithAutomaticKubernetesLeaseFailover() *antflyv1.AntflyCluster {
	cluster := haCluster()
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	return cluster
}

func caughtUpHAStatus() *antflyv1.HAStatus {
	return &antflyv1.HAStatus{
		PrimaryLSN: 12,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "standby-a",
			SlotName:    "standby-a",
			Active:      true,
			ReceivedLSN: 12,
			AppliedLSN:  12,
			SafeReadLSN: 12,
			ApplyLagLSN: 0,
			Status:      "healthy",
		}},
	}
}

func haFenceLease(cluster *antflyv1.AntflyCluster, renewTime time.Time, durationSeconds int32, transitions int32, holder string) *coordinationv1.Lease {
	renew := metav1.NewMicroTime(renewTime)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      haFencingLeaseName(cluster),
			Namespace: cluster.Namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &durationSeconds,
			RenewTime:            &renew,
			LeaseTransitions:     &transitions,
		},
	}
}

func testHAReconciler(t *testing.T, objects ...client.Object) *AntflyClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add antfly scheme: %v", err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add coordination scheme: %v", err)
	}
	return &AntflyClusterReconciler{
		Client: clientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			Build(),
		Scheme: scheme,
	}
}

func readyFencingStatus() antflyv1.HAFencingStatus {
	return antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
		Ready:      true,
		Holder:     "standby-a",
		Generation: 1,
		Reason:     "LeaseHeld",
	}
}
