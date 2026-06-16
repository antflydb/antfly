package controllers

import (
	"context"
	"reflect"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
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
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[0].AdminCommand, []string{"slot", "create", "--slot", "standby-a", "--initial-lsn", "5"}) {
		t.Fatalf("unexpected create-slot admin command: %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[1].AdminCommand, []string{"seed", "begin", "--slot", "standby-a", "--manifest-id", "base-standby-a-5"}) {
		t.Fatalf("unexpected seed admin command: %#v", cluster.Status.HAStatus.PlannedActions[1].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://primary-ha.default.svc:8081" ||
		cluster.Status.HAStatus.PlannedActions[1].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("expected slot and seed actions to target primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestPlanHAPlansSeedFinishAndBootstrapWhenManifestPathConfigured(t *testing.T) {
	initial := uint64(5)
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 9}
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:             "standby-a",
		InitialLSN:       &initial,
		AdminURL:         "http://standby-a-ha.default.svc:8081",
		SeedManifestPath: "/backup/base-standby-a-5.afha",
		SeedContentRoot:  "/backup/base-standby-a-5",
	}}

	reconciler := &AntflyClusterReconciler{}
	reconciler.updateHAStatusAndConditions(cluster)

	actions := cluster.Status.HAStatus.PlannedActions
	if len(actions) != 4 {
		t.Fatalf("expected create-slot, seed begin, finish, and bootstrap actions, got %#v", actions)
	}
	if actions[2].Kind != string(haActionFinishStandbySeed) ||
		!reflect.DeepEqual(actions[2].AdminCommand, []string{"seed", "finish", "--manifest", "/backup/base-standby-a-5.afha"}) ||
		actions[2].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("unexpected seed finish action: %#v", actions[2])
	}
	if actions[3].Kind != string(haActionBootstrapStandbySeed) ||
		!reflect.DeepEqual(actions[3].AdminCommand, []string{"seed", "bootstrap", "--manifest", "/backup/base-standby-a-5.afha", "--content-root", "/backup/base-standby-a-5"}) ||
		actions[3].AdminURL != "http://standby-a-ha.default.svc:8081" {
		t.Fatalf("unexpected seed bootstrap action: %#v", actions[3])
	}
}

func TestPlanHAPlansPauseAndResumeSlotLifecycle(t *testing.T) {
	undesired := false
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:              "standby-a",
		Desired:           &undesired,
		DropSlotOnRemoval: true,
	}, {
		Name:     "standby-b",
		SlotName: "slot-b",
	}}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 10,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:     "standby-a",
			SlotName: "standby-a",
			Active:   true,
		}, {
			Name:     "slot-b",
			SlotName: "slot-b",
			Active:   false,
		}},
	}

	reconciler := &AntflyClusterReconciler{}
	reconciler.updateHAStatusAndConditions(cluster)

	actions := cluster.Status.HAStatus.PlannedActions
	if len(actions) != 3 {
		t.Fatalf("expected pause, drop, and resume actions, got %#v", actions)
	}
	if actions[0].Kind != string(haActionPauseSlot) ||
		!reflect.DeepEqual(actions[0].AdminCommand, []string{"slot", "pause", "--slot", "standby-a"}) ||
		actions[0].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("unexpected pause action: %#v", actions[0])
	}
	if actions[1].Kind != string(haActionDropSlot) ||
		!reflect.DeepEqual(actions[1].AdminCommand, []string{"slot", "drop", "--slot", "standby-a"}) ||
		actions[1].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("unexpected drop action: %#v", actions[1])
	}
	if actions[2].Kind != string(haActionResumeSlot) ||
		!reflect.DeepEqual(actions[2].AdminCommand, []string{"slot", "resume", "--slot", "slot-b"}) ||
		actions[2].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("unexpected resume action: %#v", actions[2])
	}
	if cluster.Status.HAStatus.DesiredStandbyCount != 1 {
		t.Fatalf("expected only desired standby counted, got %d", cluster.Status.HAStatus.DesiredStandbyCount)
	}
	if len(cluster.Status.HAStatus.Standbys) != 1 ||
		cluster.Status.HAStatus.Standbys[0].Name != "standby-b" ||
		cluster.Status.HAStatus.Standbys[0].SlotName != "slot-b" {
		t.Fatalf("expected slotName override to survive status merge, got %#v", cluster.Status.HAStatus.Standbys)
	}
}

func TestPlanHALeavesUndesiredSlotPausedUnlessDropIsExplicit(t *testing.T) {
	undesired := false
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:    "standby-a",
		Desired: &undesired,
	}}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 10,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:     "standby-a",
			SlotName: "standby-a",
			Active:   true,
		}},
	}

	reconciler := &AntflyClusterReconciler{}
	reconciler.updateHAStatusAndConditions(cluster)

	actions := cluster.Status.HAStatus.PlannedActions
	if len(actions) != 1 {
		t.Fatalf("expected pause-only action, got %#v", actions)
	}
	if actions[0].Kind != string(haActionPauseSlot) ||
		!reflect.DeepEqual(actions[0].AdminCommand, []string{"slot", "pause", "--slot", "standby-a"}) {
		t.Fatalf("unexpected pause-only action: %#v", actions[0])
	}
}

func TestUpdateHAStatusReportsUnhealthyAndLaggingStandbys(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name: "standby-a",
	}, {
		Name: "standby-b",
	}}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 10,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "standby-a",
			SlotName:    "standby-a",
			Active:      true,
			ReceivedLSN: 9,
			AppliedLSN:  8,
			WriteLagLSN: 1,
			ApplyLagLSN: 2,
			Status:      "lagging",
			LastError:   "replication timeout",
		}},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.UnhealthyStandbyCount != 2 {
		t.Fatalf("expected two unhealthy standbys including missing standby, got %d", cluster.Status.HAStatus.UnhealthyStandbyCount)
	}
	if cluster.Status.HAStatus.LaggingStandbyCount != 1 {
		t.Fatalf("expected one lagging standby, got %d", cluster.Status.HAStatus.LaggingStandbyCount)
	}
	unhealthy := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAUnhealthy)
	if unhealthy == nil || unhealthy.Status != metav1.ConditionTrue || unhealthy.Reason != "HAStandbyUnhealthy" {
		t.Fatalf("expected unhealthy condition, got %#v", unhealthy)
	}
	lagging := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHALagging)
	if lagging == nil || lagging.Status != metav1.ConditionTrue || lagging.Reason != "HAStandbyLagging" {
		t.Fatalf("expected lagging condition, got %#v", lagging)
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
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
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
	expectedAcquireCommand := []string{
		"fence", "acquire",
		"--cluster-id", "100",
		"--shard-id", "10",
		"--table-id", "20",
		"--timeline-id", "4",
		"--epoch", "6",
		"--old-primary-id", "primary-a",
		"--promoted-node-id", "standby-a",
		"--new-timeline-id", "5",
		"--new-epoch", "7",
		"--required-lsn", "12",
		"--observed-lsn", "12",
		"--reason", "AutomaticFailoverReady",
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[0].AdminCommand, expectedAcquireCommand) {
		t.Fatalf("unexpected acquire-fence admin command: %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[1].AdminCommand, []string{"promote", "--current-fence"}) {
		t.Fatalf("unexpected promote admin command: %#v", cluster.Status.HAStatus.PlannedActions[1].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("expected acquire-fence action to target primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[0])
	}
	if cluster.Status.HAStatus.PlannedActions[1].AdminURL != "http://standby-a-ha.default.svc:8081" {
		t.Fatalf("expected promote action to target standby HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[1])
	}
	if cluster.Status.HAStatus.PlannedActions[2].AdminCommand != nil {
		t.Fatalf("route action should not publish an HA admin command without service execution context, got %#v", cluster.Status.HAStatus.PlannedActions[2].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[2].AdminURL != "" {
		t.Fatalf("route action should not publish an HA admin URL without service execution context, got %#v", cluster.Status.HAStatus.PlannedActions[2].AdminURL)
	}
	expectedDemoteCommand := []string{
		"rejoin", "assess",
		"--node-id", "primary-a",
		"--cluster-id", "100",
		"--shard-id", "10",
		"--table-id", "20",
		"--timeline-id", "4",
		"--epoch", "6",
		"--last-lsn", "12",
		"--retained-from-lsn", "0",
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[3].AdminCommand, expectedDemoteCommand) {
		t.Fatalf("unexpected former-primary demote admin command: %#v", cluster.Status.HAStatus.PlannedActions[3].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[3].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("expected former-primary demote to target primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[3])
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
		cluster.Status.HAStatus.PlannedActions[0].TargetLSN != 10 ||
		cluster.Status.HAStatus.PlannedActions[0].ObservedLSN != 10 {
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

func TestUpdateHAStatusRendersFormerPrimaryRejoinCommandsWithReceipt(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name: "old-primary",
	}}
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "old-primary",
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
			CurrentTarget: "standby-a",
		},
		Retention: antflyv1.HARetentionStatus{
			OldestRestartLSN: 8,
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
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			SwitchLSN:         10,
			RequiredLSN:       10,
			ObservedLSN:       10,
			FenceGeneration:   4,
			FenceReason:       "operator-approved",
		},
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "old-primary",
			SlotName:    "old-primary",
			Active:      true,
			TimelineID:  4,
			ReceivedLSN: 10,
			AppliedLSN:  10,
		}},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionRewindFormerPrimary) {
		t.Fatalf("expected rewind planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminCommand != nil {
		t.Fatalf("rewind should not be executable without a fence token, got %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
	}

	cluster.Status.HAStatus.LastPromotion.FenceToken = "token"
	reconciler.updateHAStatusAndConditions(cluster)

	expected := []string{
		"rejoin", "assess",
		"--node-id", "old-primary",
		"--cluster-id", "100",
		"--shard-id", "10",
		"--table-id", "20",
		"--timeline-id", "4",
		"--epoch", "6",
		"--last-lsn", "10",
		"--retained-from-lsn", "8",
		"--fence-old-primary-id", "old-primary",
		"--fence-promoted-node-id", "standby-a",
		"--fence-parent-timeline-id", "4",
		"--fence-parent-epoch", "6",
		"--fence-new-timeline-id", "5",
		"--fence-new-epoch", "7",
		"--fence-required-lsn", "10",
		"--fence-observed-lsn", "10",
		"--fence-generation", "4",
		"--fence-token", "token",
		"--fence-reason", "operator-approved",
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[0].AdminCommand, expected) {
		t.Fatalf("unexpected fenced rejoin command: %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("expected former primary rejoin to target primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[0])
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

func TestReconcileHAFencingLeaseCreatesReadyLeaseForCaughtUpStandby(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:         antflyv1.HADurabilityModeRemoteApply,
		Required:     1,
		StandbyNames: []string{"standby-a"},
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	reconciler := testHAReconciler(t, cluster)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	lease := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease); err != nil {
		t.Fatalf("get fencing lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "standby-a" {
		t.Fatalf("expected standby-a lease holder, got %#v", lease.Spec.HolderIdentity)
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != haFencingLeaseDefaultDurationSeconds {
		t.Fatalf("expected default lease duration, got %#v", lease.Spec.LeaseDurationSeconds)
	}
	if lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions != 1 {
		t.Fatalf("expected first lease transition, got %#v", lease.Spec.LeaseTransitions)
	}
	if lease.Spec.AcquireTime == nil || lease.Spec.RenewTime == nil {
		t.Fatalf("expected acquire and renew timestamps, got %#v", lease.Spec)
	}
	if lease.Labels["antfly.io/ha-fence"] != "kubernetes-lease" {
		t.Fatalf("expected HA fence label, got %#v", lease.Labels)
	}
	if len(lease.OwnerReferences) != 1 || lease.OwnerReferences[0].Name != cluster.Name {
		t.Fatalf("expected cluster owner reference, got %#v", lease.OwnerReferences)
	}

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe fencing status: %v", err)
	}
	reconciler.updateHAStatusAndConditions(cluster)
	if !cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected reconciled Kubernetes lease to satisfy automatic promotion fencing gate")
	}
}

func TestReconcileHAFencingLeaseRetargetsUnsafeHolder(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.Standbys = append(cluster.Spec.HighAvailability.Standbys, antflyv1.HAStandbySpec{Name: "standby-b"})
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "standby-a",
			SlotName:    "standby-a",
			Active:      true,
			ReceivedLSN: 10,
			AppliedLSN:  10,
			SafeReadLSN: 10,
			Status:      "lagging",
		}, {
			Name:        "standby-b",
			SlotName:    "standby-b",
			Active:      true,
			ReceivedLSN: 12,
			AppliedLSN:  12,
			SafeReadLSN: 12,
			Status:      "healthy",
		}},
	}
	durationSeconds := int32(15)
	lease := haFenceLease(cluster, time.Now().Add(-time.Second), durationSeconds, 2, "standby-a")
	reconciler := testHAReconciler(t, cluster, lease)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	observed := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, observed); err != nil {
		t.Fatalf("get fencing lease: %v", err)
	}
	if observed.Spec.HolderIdentity == nil || *observed.Spec.HolderIdentity != "standby-b" {
		t.Fatalf("expected standby-b lease holder, got %#v", observed.Spec.HolderIdentity)
	}
	if observed.Spec.LeaseTransitions == nil || *observed.Spec.LeaseTransitions != 3 {
		t.Fatalf("expected holder transition to increment, got %#v", observed.Spec.LeaseTransitions)
	}
	if observed.Spec.LeaseDurationSeconds == nil || *observed.Spec.LeaseDurationSeconds != durationSeconds {
		t.Fatalf("expected existing lease duration to be preserved, got %#v", observed.Spec.LeaseDurationSeconds)
	}
	if observed.Spec.AcquireTime == nil || observed.Spec.RenewTime == nil || !observed.Spec.RenewTime.After(lease.Spec.RenewTime.Time) {
		t.Fatalf("expected timestamps to advance on holder change, got old=%#v new=%#v", lease.Spec.RenewTime, observed.Spec)
	}
}

func TestReconcileHAFencingLeaseSkipsWithoutSafeCandidate(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "standby-a",
			SlotName:    "standby-a",
			Active:      true,
			ReceivedLSN: 11,
			AppliedLSN:  11,
			SafeReadLSN: 11,
			Status:      "lagging",
		}},
	}
	reconciler := testHAReconciler(t, cluster)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	lease := &coordinationv1.Lease{}
	err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no fencing lease without safe candidate, got lease=%#v err=%v", lease, err)
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

func TestPeriodicRequeueRenewsKubernetesLeaseBeforeExpiry(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()

	if got, want := periodicRequeueAfter(cluster), 10*time.Second; got != want {
		t.Fatalf("expected HA lease renewal requeue %s, got %s", want, got)
	}

	cluster.Spec.DataNodes.AutoScaling = &antflyv1.AutoScalingSpec{Enabled: true}
	if got, want := periodicRequeueAfter(cluster), 10*time.Second; got != want {
		t.Fatalf("expected HA lease requeue to win over autoscaling, got %s", got)
	}

	cluster.Spec.HighAvailability.AutomaticFailover.Enabled = false
	if got, want := periodicRequeueAfter(cluster), 30*time.Second; got != want {
		t.Fatalf("expected autoscaling requeue without HA renewal, got %s", got)
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
