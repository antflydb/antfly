package controllers

import (
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	if !cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be allowed with fencing and caught-up standby")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionTrue || failover.Reason != "HAFencedPromotionReady" {
		t.Fatalf("expected failover-ready condition, got %#v", failover)
	}
	plan := planHA(cluster)
	if len(plan.Actions) != 4 {
		t.Fatalf("expected fenced promotion action chain, got %#v", plan.Actions)
	}
	if plan.Actions[0].Kind != haActionAcquireFence || plan.Actions[1].Kind != haActionPromoteStandby {
		t.Fatalf("unexpected promotion actions: %#v", plan.Actions)
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
