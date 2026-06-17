package controllers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"
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
	"sigs.k8s.io/yaml"
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

func TestHAReplicationIdentityAllowsWholeInstanceScope(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          0,
			TableID:          0,
			TimelineID:       1,
			Epoch:            1,
			CurrentPrimaryID: "primary-a",
		},
	}

	identity := haReplicationIdentity(ha)
	if identity == nil {
		t.Fatal("expected whole-instance HA identity to be accepted")
	}
	if identity.ShardID != 0 || identity.TableID != 0 {
		t.Fatalf("expected zero shard/table identity to be preserved, got %#v", identity)
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
	if plan.Actions[1].Kind != haActionSeedStandby || plan.Actions[1].DependsOn != haActionCreateSlot || plan.Actions[1].TargetLSN != initial {
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
	if cluster.Status.HAStatus.PlannedActions[1].DependsOn != string(haActionCreateSlot) {
		t.Fatalf("expected seed action to depend on create-slot, got %#v", cluster.Status.HAStatus.PlannedActions[1])
	}
	if cluster.Status.HAStatus.PlannedActions[0].Phase != string(haActionPhaseReconcile) ||
		cluster.Status.HAStatus.PlannedActions[0].Executor != string(haActionExecutorAdminAPI) ||
		cluster.Status.HAStatus.PlannedActions[1].Phase != string(haActionPhaseReconcile) ||
		cluster.Status.HAStatus.PlannedActions[1].Executor != string(haActionExecutorAdminAPI) {
		t.Fatalf("expected slot and seed actions to publish reconcile/admin executor metadata, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://primary-ha.default.svc:8081" ||
		cluster.Status.HAStatus.PlannedActions[1].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("expected slot and seed actions to target primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminMethod != "POST" ||
		cluster.Status.HAStatus.PlannedActions[0].AdminPath != "/admin/v1/ha/replication-slots" ||
		cluster.Status.HAStatus.PlannedActions[1].AdminMethod != "POST" ||
		cluster.Status.HAStatus.PlannedActions[1].AdminPath != "/admin/v1/ha/base-backups" {
		t.Fatalf("expected slot and seed actions to publish typed admin operations, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestPlanHAWaitsForPrimaryLSNBeforeMissingStandbySeed(t *testing.T) {
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 0}
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}

	plan := planHA(cluster)

	if plan.UnhealthyStandbyCount != 1 {
		t.Fatalf("expected missing standby to remain unhealthy, got %d", plan.UnhealthyStandbyCount)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("expected no create/seed actions while primary LSN is unknown, got %#v", plan.Actions)
	}

	reconciler := &AntflyClusterReconciler{}
	reconciler.updateHAStatusAndConditions(cluster)
	if len(cluster.Status.HAStatus.PlannedActions) != 0 {
		t.Fatalf("expected no planned actions while primary LSN is unknown, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestPlanHAWaitsForPrimaryLSNBeforeReseedBaseBackup(t *testing.T) {
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 0,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:           "standby-a",
			SlotName:       "standby-a",
			ReseedRequired: true,
			Status:         "reseed_required",
		}},
	}
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}

	plan := planHA(cluster)

	if plan.ReseedRequiredCount != 1 {
		t.Fatalf("expected reseed-required status to remain visible, got %d", plan.ReseedRequiredCount)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("expected no reseed actions while primary LSN is unknown, got %#v", plan.Actions)
	}
}

func TestHAPlannedActionStatusesPreserveExecutionOnlyForSameOperation(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       4,
			Epoch:            6,
			CurrentPrimaryID: "primary-a",
		},
	}
	actions := []haPlannedAction{{
		Kind:        haActionCreateSlot,
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		TargetLSN:   5,
		Reason:      "SlotMissing",
	}}

	initial := haPlannedActionStatuses(actions, ha, &antflyv1.HAStatus{})
	if len(initial) != 1 {
		t.Fatalf("expected one planned action, got %#v", initial)
	}
	previous := initial[0]
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseSucceeded
	previous.AdminError = "previous direct-admin-api diagnostic"
	previous.AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1,
		ActionID:      "replication_slot_create:standby-a",
		ActionKind:    "replication_slot_create",
		ActionTarget:  "standby-a",
		ActionState:   "applied",
		ActionNodeID:  "primary-a",
		SlotAction:    "create",
		SlotName:      "standby-a",
	}

	status := &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{previous}}
	preserved := haPlannedActionStatuses(actions, ha, status)
	if preserved[0].AdminJobName != haAdminDirectAPIName ||
		preserved[0].AdminJobPhase != haAdminJobPhaseSucceeded ||
		preserved[0].AdminError != "previous direct-admin-api diagnostic" ||
		preserved[0].AdminResult == nil ||
		preserved[0].AdminResult.SlotAction != "create" {
		t.Fatalf("expected execution state to survive identical replan, got %#v", preserved[0])
	}

	changed := []haPlannedAction{{
		Kind:        haActionCreateSlot,
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		TargetLSN:   6,
		Reason:      "SlotMissing",
	}}
	notPreserved := haPlannedActionStatuses(changed, ha, status)
	if notPreserved[0].AdminJobName != "" ||
		notPreserved[0].AdminJobPhase != "" ||
		notPreserved[0].AdminError != "" ||
		notPreserved[0].AdminResult != nil {
		t.Fatalf("expected changed action to drop stale execution state, got %#v", notPreserved[0])
	}
}

func TestHAPlannedActionStatusesPreserveTypedExecutionAcrossAdminCommandHints(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       4,
			Epoch:            6,
			CurrentPrimaryID: "primary-a",
		},
	}
	actions := []haPlannedAction{{
		Kind:        haActionCreateSlot,
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		TargetLSN:   5,
		Reason:      "SlotMissing",
	}}

	initial := haPlannedActionStatuses(actions, ha, &antflyv1.HAStatus{})
	if len(initial) != 1 {
		t.Fatalf("expected one planned action, got %#v", initial)
	}
	previous := initial[0]
	previous.AdminCommand = []string{"legacy-slot-create"}
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseSucceeded
	previous.AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1,
		ActionID:      "replication_slot_create:standby-a",
		ActionKind:    "replication_slot_create",
		ActionTarget:  "standby-a",
		ActionState:   "applied",
		ActionNodeID:  "primary-a",
		SlotAction:    "create",
		SlotName:      "standby-a",
	}

	status := &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{previous}}
	preserved := haPlannedActionStatuses(actions, ha, status)
	if preserved[0].AdminJobName != haAdminDirectAPIName ||
		preserved[0].AdminJobPhase != haAdminJobPhaseSucceeded ||
		preserved[0].AdminResult == nil ||
		preserved[0].AdminResult.SlotAction != "create" {
		t.Fatalf("expected typed admin execution state to survive CLI hint drift, got %#v", preserved[0])
	}
}

func TestHAPlannedActionUsesTypedAdminAPISkipsCLIJob(t *testing.T) {
	action := antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionCreateSlot),
		Executor:    string(haActionExecutorCLIJob),
		AdminMethod: "POST",
		AdminPath:   haAdminReplicationSlotsPath,
	}
	if haPlannedActionUsesTypedAdminAPI(action) {
		t.Fatalf("CLI-backed action should not be treated as typed admin API execution: %#v", action)
	}
	action.Executor = string(haActionExecutorAdminAPI)
	if !haPlannedActionUsesTypedAdminAPI(action) {
		t.Fatalf("AdminAPI action with method/path should be treated as typed admin API execution: %#v", action)
	}
}

func TestHAPlannedActionStatusesDropInvalidDirectAdminSuccess(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
	}
	actions := []haPlannedAction{{
		Kind:        haActionCreateSlot,
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		TargetLSN:   5,
		Reason:      "SlotMissing",
	}}

	initial := haPlannedActionStatuses(actions, ha, &antflyv1.HAStatus{})
	if len(initial) != 1 {
		t.Fatalf("expected one planned action, got %#v", initial)
	}
	previous := initial[0]
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseSucceeded
	previous.AdminError = "weak direct-admin-api diagnostic"
	previous.AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1,
		SlotAction:    "create",
		SlotName:      "standby-a",
	}

	status := &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{previous}}
	preserved := haPlannedActionStatuses(actions, ha, status)
	if preserved[0].AdminJobName != "" ||
		preserved[0].AdminJobPhase != "" ||
		preserved[0].AdminError != "" ||
		preserved[0].AdminResult != nil {
		t.Fatalf("expected invalid direct admin success to be dropped, got %#v", preserved[0])
	}

	previous.AdminJobName = "legacy-cli-job"
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}
	preserved = haPlannedActionStatuses(actions, ha, status)
	if preserved[0].AdminJobName != "" ||
		preserved[0].AdminJobPhase != "" ||
		preserved[0].AdminError != "" ||
		preserved[0].AdminResult != nil {
		t.Fatalf("expected invalid CLI-backed admin success to be dropped, got %#v", preserved[0])
	}
}

func TestHAPlannedActionStatusesDropFormerPrimarySuccessWithoutPromotionReceipt(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Standbys: []antflyv1.HAStandbySpec{{
			Name:     "old-primary",
			AdminURL: "http://old-primary-ha.default.svc:8081",
		}},
	}
	action := haPlannedAction{
		Kind:            haActionRewindFormerPrimary,
		StandbyName:     "old-primary",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 3,
		Reason:          "FormerPrimaryNeedsRewind",
	}
	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "old-primary",
			PromotedStandbyID: "standby-a",
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			SwitchLSN:         12,
			RequiredLSN:       12,
			ObservedLSN:       12,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   3,
			FenceToken:        "ha-fence-token",
		},
	}

	initial := haPlannedActionStatuses([]haPlannedAction{action}, ha, status)
	if len(initial) != 1 {
		t.Fatalf("expected one planned action, got %#v", initial)
	}
	previous := initial[0]
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseSucceeded
	previous.AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion:           1,
		ActionID:                "rejoin_rewind:old-primary",
		ActionKind:              "rejoin_rewind",
		ActionTarget:            "old-primary",
		ActionState:             "applied",
		ActionNodeID:            "old-primary",
		RejoinAction:            "rewind",
		FormerNodeID:            "old-primary",
		TargetTimelineID:        5,
		TargetEpoch:             7,
		ForkLSN:                 12,
		FormerLastLSN:           13,
		RetainedFromLSN:         8,
		RewindExecuted:          true,
		RewindPreviousLastLSN:   13,
		RewindCurrentLastLSN:    12,
		RewindNextLSN:           13,
		RewindDiscardedLSNCount: 1,
	}
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}

	preserved := haPlannedActionStatuses([]haPlannedAction{action}, ha, status)
	if preserved[0].AdminJobName != haAdminDirectAPIName ||
		preserved[0].AdminJobPhase != haAdminJobPhaseSucceeded ||
		preserved[0].AdminResult == nil ||
		preserved[0].AdminResult.RejoinAction != "rewind" {
		t.Fatalf("expected matching former-primary execution state to survive replan, got %#v", preserved[0])
	}

	status.LastPromotion.FenceToken = ""
	notPreserved := haPlannedActionStatuses([]haPlannedAction{action}, ha, status)
	if notPreserved[0].AdminJobName != "" ||
		notPreserved[0].AdminJobPhase != "" ||
		notPreserved[0].AdminResult != nil {
		t.Fatalf("expected former-primary execution state without promotion receipt to be dropped, got %#v", notPreserved[0])
	}
}

func TestHAPlannedActionStatusesDropPromotionSuccessMismatchedWithRecordedPromotion(t *testing.T) {
	action := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionPromoteStandby),
		Phase:           string(haActionPhasePromote),
		Executor:        string(haActionExecutorAdminAPI),
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 7,
		AdminURL:        "http://standby-a-ha.default.svc:8081",
		AdminNodeID:     "standby-a",
		AdminMethod:     "POST",
		AdminPath:       haAdminPromotionCurrentFencePath,
		Reason:          "AutomaticFailoverReady",
	}
	previous := action
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseSucceeded
	previous.AdminResult = haPromotionAdminResult(7, "ha-fence-token", "standby-a")

	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "primary-a",
			PromotedStandbyID: "standby-a",
			ClusterID:         100,
			ShardID:           10,
			TableID:           20,
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			RequiredLSN:       12,
			ObservedLSN:       13,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   7,
			FenceToken:        "ha-fence-token",
		},
		PlannedActions: []antflyv1.HAPlannedActionStatus{previous},
	}

	preserved := haPreservePlannedActionExecution(action, status)
	if preserved.AdminJobName != haAdminDirectAPIName ||
		preserved.AdminJobPhase != haAdminJobPhaseSucceeded ||
		preserved.AdminResult == nil {
		t.Fatalf("expected matching promotion execution state to survive replan, got %#v", preserved)
	}

	status.LastPromotion.NewTimelineID = 6
	notPreserved := haPreservePlannedActionExecution(action, status)
	if notPreserved.AdminJobName != "" ||
		notPreserved.AdminJobPhase != "" ||
		notPreserved.AdminResult != nil {
		t.Fatalf("expected mismatched promotion execution state to be dropped, got %#v", notPreserved)
	}

	status.LastPromotion.NewTimelineID = 5
	status.LastPromotion.FenceToken = "different-token"
	notPreserved = haPreservePlannedActionExecution(action, status)
	if notPreserved.AdminJobName != "" ||
		notPreserved.AdminJobPhase != "" ||
		notPreserved.AdminResult != nil {
		t.Fatalf("expected stale promotion token evidence to be dropped, got %#v", notPreserved)
	}

	status.LastPromotion.FenceToken = "ha-fence-token"
	previous.AdminResult = haPromotionAdminResult(7, "ha-fence-token", "standby-a")
	previous.AdminResult.FenceTableID = 21
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}
	notPreserved = haPreservePlannedActionExecution(action, status)
	if notPreserved.AdminJobName != "" ||
		notPreserved.AdminJobPhase != "" ||
		notPreserved.AdminResult != nil {
		t.Fatalf("expected promotion evidence with mismatched identity scope to be dropped, got %#v", notPreserved)
	}

	previous.AdminResult = haPromotionAdminResult(7, "ha-fence-token", "standby-a")
	previous.AdminResult.FenceOldPrimaryID = "different-primary"
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}
	notPreserved = haPreservePlannedActionExecution(action, status)
	if notPreserved.AdminJobName != "" ||
		notPreserved.AdminJobPhase != "" ||
		notPreserved.AdminResult != nil {
		t.Fatalf("expected promotion evidence with mismatched old primary to be dropped, got %#v", notPreserved)
	}

	previous.AdminResult = haPromotionAdminResult(7, "ha-fence-token", "standby-a")
	previous.AdminResult.FenceClusterID = 0
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}
	notPreserved = haPreservePlannedActionExecution(action, status)
	if notPreserved.AdminJobName != "" ||
		notPreserved.AdminJobPhase != "" ||
		notPreserved.AdminResult != nil {
		t.Fatalf("expected promotion evidence without cluster identity to be dropped, got %#v", notPreserved)
	}
}

func TestHAPromotionReceiptRequiresConcreteFenceAuthority(t *testing.T) {
	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "primary-a",
			PromotedStandbyID: "standby-a",
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			RequiredLSN:       12,
			ObservedLSN:       13,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   7,
			FenceToken:        "ha-fence-token",
		},
	}

	if haPromotionReceipt(status) == nil {
		t.Fatalf("expected complete fenced promotion receipt")
	}

	status.LastPromotion.FenceAuthority = ""
	if haPromotionReceipt(status) != nil {
		t.Fatalf("expected promotion receipt without fence authority to be incomplete")
	}

	status.LastPromotion.FenceAuthority = antflyv1.HAFencingAuthorityNone
	if haPromotionReceipt(status) != nil {
		t.Fatalf("expected promotion receipt with None fence authority to be incomplete")
	}
}

func TestHAPlannedActionStatusesPreserveTypedSeedFinishDespiteCLIHintDrift(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Identity: &antflyv1.HAReplicationIdentitySpec{
			CurrentPrimaryID: "primary-a",
		},
	}
	actions := []haPlannedAction{{
		Kind:             haActionFinishStandbySeed,
		DependsOn:        haActionSeedStandby,
		StandbyName:      "standby-a",
		TargetLSN:        5,
		SeedManifestPath: "/backup/base-standby-a-5.afha",
		Reason:           "SeedManifestReady",
	}}

	initial := haPlannedActionStatuses(actions, ha, &antflyv1.HAStatus{})
	if len(initial) != 1 {
		t.Fatalf("expected one planned action, got %#v", initial)
	}
	previous := initial[0]
	previous.AdminCommand = []string{"legacy-seed-finish"}
	previous.AdminJobName = "seed-finish-job"
	previous.AdminJobPhase = haAdminJobPhaseSucceeded
	previous.AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1,
		ActionID:      "base_backup_finish:base-standby-a-5",
		ActionKind:    "base_backup_finish",
		ActionTarget:  "base-standby-a-5",
		ActionState:   "applied",
		ActionNodeID:  "primary-a",
		ManifestID:    "base-standby-a-5",
		BackupLSN:     5,
		EndRecordLSN:  5,
	}

	status := &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{previous}}
	preserved := haPlannedActionStatuses(actions, ha, status)
	if preserved[0].AdminJobName != "seed-finish-job" ||
		preserved[0].AdminJobPhase != haAdminJobPhaseSucceeded ||
		preserved[0].AdminResult == nil ||
		preserved[0].AdminResult.ManifestID != "base-standby-a-5" {
		t.Fatalf("expected typed seed finish execution state to survive CLI hint drift, got %#v", preserved[0])
	}
}

func TestParseHAOperatorPlanTableActions(t *testing.T) {
	plan, err := parseHAOperatorPlanTable(strings.Join([]string{
		"result=operator_plan",
		"automatic_promotion_allowed=true",
		"desired_standby_count=1",
		"healthy_standby_count=1",
		"unhealthy_standby_count=0",
		"lagging_standby_count=0",
		"reseed_required_count=0",
		"action_count=4",
		"actions.0.kind=acquire_fence",
		"actions.0.phase=fence",
		"actions.0.executor=admin_api",
		"actions.0.reason=AutomaticFailoverReady",
		"actions.0.standby_name=standby-a",
		"actions.0.target_lsn=12",
		"actions.0.fence_authority=kubernetes_lease",
		"actions.0.fence_holder=standby-a",
		"actions.0.fence_generation=3",
		"actions.0.fence_reason=LeaseHeld",
		"actions.0.admin_url=http://standby-a-ha.default.svc:8081",
		"actions.0.admin_method=POST",
		"actions.0.admin_path=/admin/v1/ha/fence",
		"actions.1.kind=promote_standby",
		"actions.1.phase=promote",
		"actions.1.executor=admin_api",
		"actions.1.depends_on=acquire_fence",
		"actions.1.standby_name=standby-a",
		"actions.1.target_lsn=12",
		"actions.2.kind=update_primary_endpoint",
		"actions.2.phase=route",
		"actions.2.executor=controller_action",
		"actions.2.depends_on=promote_standby",
		"actions.2.route_from=primary",
		"actions.2.route_to=standby-a",
		"actions.3.kind=rewind_former_primary",
		"actions.3.phase=rejoin",
		"actions.3.executor=admin_api",
		"actions.3.depends_on=promote_standby",
		"actions.3.standby_name=primary-a",
		"actions.3.target_lsn=12",
		"actions.3.observed_lsn=13",
		"actions.3.retained_from_lsn=8",
		"actions.3.seed_manifest_path=/backup/base.afha",
		"actions.3.seed_content_root=/backup/base",
		"",
	}, "\n"))
	if err != nil {
		t.Fatalf("parse operator plan table: %v", err)
	}
	if !plan.AutomaticPromotionAllowed ||
		plan.DesiredStandbyCount != 1 ||
		plan.HealthyStandbyCount != 1 ||
		plan.UnhealthyStandbyCount != 0 ||
		len(plan.Actions) != 4 {
		t.Fatalf("unexpected parsed operator plan summary: %#v", plan)
	}
	if plan.Actions[0].Kind != string(haActionAcquireFence) ||
		plan.Actions[0].Phase != string(haActionPhaseFence) ||
		plan.Actions[0].Executor != string(haActionExecutorAdminAPI) ||
		plan.Actions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		plan.Actions[0].FenceHolder != "standby-a" ||
		plan.Actions[0].FenceGeneration != 3 ||
		plan.Actions[0].AdminURL != "http://standby-a-ha.default.svc:8081" ||
		plan.Actions[0].AdminMethod != "POST" ||
		plan.Actions[0].AdminPath != "/admin/v1/ha/fence" {
		t.Fatalf("unexpected parsed acquire-fence action: %#v", plan.Actions[0])
	}
	if plan.Actions[1].Kind != string(haActionPromoteStandby) ||
		plan.Actions[1].DependsOn != string(haActionAcquireFence) ||
		plan.Actions[1].AdminMethod != "POST" ||
		plan.Actions[1].AdminPath != "/admin/v1/ha/promotion/current-fence" {
		t.Fatalf("unexpected parsed promote action: %#v", plan.Actions[1])
	}
	if plan.Actions[2].Kind != string(haActionUpdatePrimaryRoute) ||
		plan.Actions[2].Executor != string(haActionExecutorControllerAction) ||
		plan.Actions[2].DependsOn != string(haActionPromoteStandby) ||
		plan.Actions[2].RouteFrom != "primary" ||
		plan.Actions[2].RouteTo != "standby-a" {
		t.Fatalf("unexpected parsed route action: %#v", plan.Actions[2])
	}
	if plan.Actions[3].Kind != string(haActionRewindFormerPrimary) ||
		plan.Actions[3].Phase != string(haActionPhaseRejoin) ||
		plan.Actions[3].StandbyName != "primary-a" ||
		plan.Actions[3].TargetLSN != 12 ||
		plan.Actions[3].ObservedLSN != 13 ||
		plan.Actions[3].RetainedFromLSN != 8 ||
		plan.Actions[3].SeedManifestPath != "/backup/base.afha" ||
		plan.Actions[3].SeedContentRoot != "/backup/base" ||
		plan.Actions[3].AdminMethod != "POST" ||
		plan.Actions[3].AdminPath != "/admin/v1/ha/rejoin/rewind" {
		t.Fatalf("unexpected parsed former-primary action: %#v", plan.Actions[3])
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
		actions[2].DependsOn != string(haActionSeedStandby) ||
		!reflect.DeepEqual(actions[2].AdminCommand, []string{"seed", "finish", "--manifest", "/backup/base-standby-a-5.afha"}) ||
		actions[2].AdminURL != "http://primary-ha.default.svc:8081" ||
		actions[2].AdminMethod != "POST" ||
		actions[2].AdminPath != "/admin/v1/ha/base-backups/finish" {
		t.Fatalf("unexpected seed finish action: %#v", actions[2])
	}
	if actions[3].Kind != string(haActionBootstrapStandbySeed) ||
		actions[3].DependsOn != string(haActionFinishStandbySeed) ||
		!reflect.DeepEqual(actions[3].AdminCommand, []string{"seed", "bootstrap", "--manifest", "/backup/base-standby-a-5.afha", "--content-root", "/backup/base-standby-a-5"}) ||
		actions[3].AdminURL != "http://standby-a-ha.default.svc:8081" ||
		actions[3].AdminMethod != "POST" ||
		actions[3].AdminPath != "/admin/v1/ha/standby/bootstrap" {
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
		actions[0].AdminURL != "http://primary-ha.default.svc:8081" ||
		actions[0].AdminMethod != "PUT" ||
		actions[0].AdminPath != "/admin/v1/ha/replication-slots/standby-a/pause" {
		t.Fatalf("unexpected pause action: %#v", actions[0])
	}
	if actions[1].Kind != string(haActionDropSlot) ||
		actions[1].DependsOn != string(haActionPauseSlot) ||
		!reflect.DeepEqual(actions[1].AdminCommand, []string{"slot", "drop", "--slot", "standby-a"}) ||
		actions[1].AdminURL != "http://primary-ha.default.svc:8081" ||
		actions[1].AdminMethod != "DELETE" ||
		actions[1].AdminPath != "/admin/v1/ha/replication-slots/standby-a" {
		t.Fatalf("unexpected drop action: %#v", actions[1])
	}
	if actions[2].Kind != string(haActionResumeSlot) ||
		!reflect.DeepEqual(actions[2].AdminCommand, []string{"slot", "resume", "--slot", "slot-b"}) ||
		actions[2].AdminURL != "http://primary-ha.default.svc:8081" ||
		actions[2].AdminMethod != "PUT" ||
		actions[2].AdminPath != "/admin/v1/ha/replication-slots/slot-b/resume" {
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

func TestPlanHAEscapesSlotNamesInAdminPaths(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:     "standby-special",
		SlotName: "standby/a b%",
	}}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 10,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:     "standby-special",
			SlotName: "standby/a b%",
			Active:   false,
		}},
	}

	reconciler := &AntflyClusterReconciler{}
	reconciler.updateHAStatusAndConditions(cluster)

	actions := cluster.Status.HAStatus.PlannedActions
	if len(actions) != 1 {
		t.Fatalf("expected one resume action, got %#v", actions)
	}
	if actions[0].Kind != string(haActionResumeSlot) ||
		!reflect.DeepEqual(actions[0].AdminCommand, []string{"slot", "resume", "--slot", "standby/a b%"}) ||
		actions[0].AdminMethod != "PUT" ||
		actions[0].AdminPath != "/admin/v1/ha/replication-slots/standby%2Fa%20b%25/resume" {
		t.Fatalf("unexpected escaped slot action: %#v", actions[0])
	}
}

func TestHAAdminOperationsMatchAdminOpenAPISpec(t *testing.T) {
	operations := loadAdminOpenAPIOperations(t)
	fixedOperations := []struct {
		name        string
		method      string
		path        string
		openAPIPath string
		operationID string
	}{
		{
			name:        "primary status",
			method:      "GET",
			path:        haAdminPrimaryStatusPath,
			openAPIPath: "/ha/primary/status",
			operationID: "getHAPrimaryStatus",
		},
		{
			name:        "standby status",
			method:      "GET",
			path:        haAdminStandbyStatusPath,
			openAPIPath: "/ha/standby/status",
			operationID: "getHAStandbyStatus",
		},
		{
			name:        "promotion assessment",
			method:      "POST",
			path:        haAdminPromotionAssessPath,
			openAPIPath: "/ha/promotion/assess",
			operationID: "assessHAPromotion",
		},
		{
			name:        "current fence",
			method:      "GET",
			path:        haAdminFenceCurrentPath,
			openAPIPath: "/ha/fence/current",
			operationID: "getHACurrentFence",
		},
		{
			name:        "combined fence and promote",
			method:      "POST",
			path:        haAdminPromotionPath,
			openAPIPath: "/ha/promotion",
			operationID: "promoteHA",
		},
	}
	for _, tt := range fixedOperations {
		t.Run(tt.name, func(t *testing.T) {
			path := strings.TrimPrefix(tt.path, haAdminBasePath)
			if path != tt.openAPIPath {
				t.Fatalf("expected OpenAPI path %s, got %s", tt.openAPIPath, path)
			}
			key := tt.method + " " + path
			if operations[key] != tt.operationID {
				t.Fatalf("expected %s to resolve to operationId %s, got %q", key, tt.operationID, operations[key])
			}
		})
	}

	slotAction := func(kind haActionKind) haPlannedAction {
		return haPlannedAction{Kind: kind, StandbyName: "standby-a", SlotName: "standby-a"}
	}
	tests := []struct {
		name        string
		action      haPlannedAction
		openAPIPath string
		operationID string
	}{
		{
			name:        "create slot",
			action:      slotAction(haActionCreateSlot),
			openAPIPath: "/ha/replication-slots",
			operationID: "createHAReplicationSlot",
		},
		{
			name:        "resume slot",
			action:      slotAction(haActionResumeSlot),
			openAPIPath: "/ha/replication-slots/{slot_name}/resume",
			operationID: "resumeHAReplicationSlot",
		},
		{
			name:        "pause slot",
			action:      slotAction(haActionPauseSlot),
			openAPIPath: "/ha/replication-slots/{slot_name}/pause",
			operationID: "pauseHAReplicationSlot",
		},
		{
			name:        "drop slot",
			action:      slotAction(haActionDropSlot),
			openAPIPath: "/ha/replication-slots/{slot_name}",
			operationID: "dropHAReplicationSlot",
		},
		{
			name:        "seed standby",
			action:      haPlannedAction{Kind: haActionSeedStandby, StandbyName: "standby-a", SlotName: "standby-a"},
			openAPIPath: "/ha/base-backups",
			operationID: "beginHABaseBackup",
		},
		{
			name:        "mark reseed",
			action:      haPlannedAction{Kind: haActionMarkReseed, StandbyName: "standby-a", SlotName: "standby-a"},
			openAPIPath: "/ha/base-backups",
			operationID: "beginHABaseBackup",
		},
		{
			name:        "finish standby seed",
			action:      haPlannedAction{Kind: haActionFinishStandbySeed, StandbyName: "standby-a"},
			openAPIPath: "/ha/base-backups/finish",
			operationID: "finishHABaseBackup",
		},
		{
			name:        "bootstrap standby seed",
			action:      haPlannedAction{Kind: haActionBootstrapStandbySeed, StandbyName: "standby-a"},
			openAPIPath: "/ha/standby/bootstrap",
			operationID: "bootstrapHAStandby",
		},
		{
			name:        "acquire fence",
			action:      haPlannedAction{Kind: haActionAcquireFence, StandbyName: "standby-a"},
			openAPIPath: "/ha/fence",
			operationID: "acquireHAFence",
		},
		{
			name:        "assess promotion",
			action:      haPlannedAction{Kind: haActionAssessPromotion, StandbyName: "standby-a"},
			openAPIPath: "/ha/promotion/assess",
			operationID: "assessHAPromotion",
		},
		{
			name:        "promote standby",
			action:      haPlannedAction{Kind: haActionPromoteStandby, StandbyName: "standby-a"},
			openAPIPath: "/ha/promotion/current-fence",
			operationID: "promoteHAWithCurrentFence",
		},
		{
			name:        "demote former primary",
			action:      haPlannedAction{Kind: haActionDemoteFormerPrimary, StandbyName: "primary-a"},
			openAPIPath: "/ha/rejoin/assess",
			operationID: "assessHARejoin",
		},
		{
			name:        "rewind former primary",
			action:      haPlannedAction{Kind: haActionRewindFormerPrimary, StandbyName: "primary-a"},
			openAPIPath: "/ha/rejoin/rewind",
			operationID: "rewindHARejoin",
		},
		{
			name:        "reseed former primary",
			action:      haPlannedAction{Kind: haActionReseedFormerPrimary, StandbyName: "primary-a"},
			openAPIPath: "/ha/rejoin/reseed",
			operationID: "reseedHARejoin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method, path := haAdminOperation(tt.action)
			if method == "" || path == "" {
				t.Fatalf("expected typed admin operation for %s", tt.name)
			}
			path = strings.Replace(path, "/standby-a", "/{slot_name}", 1)
			path = strings.TrimPrefix(path, haAdminBasePath)
			if path != tt.openAPIPath {
				t.Fatalf("expected OpenAPI path %s, got %s", tt.openAPIPath, path)
			}
			key := method + " " + path
			if operations[key] != tt.operationID {
				t.Fatalf("expected %s to resolve to operationId %s, got %q", key, tt.operationID, operations[key])
			}
		})
	}
}

func TestHAAdminRouteConstantsAreDocumentedInAdminOpenAPISpec(t *testing.T) {
	operations := loadAdminOpenAPIOperations(t)
	routes := []struct {
		method string
		path   string
	}{
		{method: "GET", path: haAdminPrimaryStatusPath},
		{method: "GET", path: haAdminStandbyStatusPath},
		{method: "POST", path: haAdminCommitCheckPath},
		{method: "POST", path: haAdminCommitAppendPath},
		{method: "POST", path: haAdminReadCheckPath},
		{method: "POST", path: haAdminWriteCheckPath},
		{method: "POST", path: haAdminOwnerJobCheckPath},
		{method: "GET", path: haAdminReplicationSlotsPath},
		{method: "POST", path: haAdminReplicationSlotsPath},
		{method: "DELETE", path: haAdminReplicationSlotPathPrefix + "{slot_name}"},
		{method: "PUT", path: haAdminReplicationSlotPathPrefix + "{slot_name}" + haAdminReplicationSlotPausePathSuffix},
		{method: "PUT", path: haAdminReplicationSlotPathPrefix + "{slot_name}" + haAdminReplicationSlotResumePathSuffix},
		{method: "POST", path: haAdminBaseBackupsPath},
		{method: "POST", path: haAdminBaseBackupsFinishPath},
		{method: "POST", path: haAdminStandbyBootstrapPath},
		{method: "POST", path: haAdminFencePath},
		{method: "GET", path: haAdminFenceCurrentPath},
		{method: "POST", path: haAdminPromotionAssessPath},
		{method: "POST", path: haAdminPromotionCurrentFencePath},
		{method: "POST", path: haAdminPromotionPath},
		{method: "POST", path: haAdminRejoinAssessPath},
		{method: "POST", path: haAdminRejoinRewindPath},
		{method: "POST", path: haAdminRejoinReseedPath},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			path := strings.TrimPrefix(route.path, haAdminBasePath)
			key := route.method + " " + path
			if operations[key] == "" {
				t.Fatalf("operator HA admin route %s is missing from specs/openapi/antfly/admin.yaml", key)
			}
		})
	}

	covered := map[string]bool{}
	for _, route := range routes {
		covered[route.method+" "+strings.TrimPrefix(route.path, haAdminBasePath)] = true
	}
	for key := range operations {
		if !strings.Contains(key, " /ha/") {
			continue
		}
		if !covered[key] {
			t.Fatalf("admin OpenAPI HA route %s is not registered in operator HA admin route constants", key)
		}
	}
}

func TestHADirectAdminSupportMatchesAdminOperations(t *testing.T) {
	directActions := []haPlannedAction{
		{Kind: haActionCreateSlot, StandbyName: "standby-a", SlotName: "standby-a"},
		{Kind: haActionResumeSlot, StandbyName: "standby-a", SlotName: "standby-a"},
		{Kind: haActionPauseSlot, StandbyName: "standby-a", SlotName: "standby-a"},
		{Kind: haActionDropSlot, StandbyName: "standby-a", SlotName: "standby-a"},
		{Kind: haActionSeedStandby, StandbyName: "standby-a", SlotName: "standby-a"},
		{Kind: haActionMarkReseed, StandbyName: "standby-a", SlotName: "standby-a"},
		{Kind: haActionFinishStandbySeed, StandbyName: "standby-a", SeedManifestPath: "/backups/base-standby-a-5.afha"},
		{Kind: haActionBootstrapStandbySeed, StandbyName: "standby-a", SeedManifestPath: "/backups/base-standby-a-5.afha"},
		{Kind: haActionAcquireFence, StandbyName: "standby-a"},
		{Kind: haActionAssessPromotion, StandbyName: "standby-a"},
		{Kind: haActionPromoteStandby, StandbyName: "standby-a"},
		{Kind: haActionDemoteFormerPrimary, StandbyName: "primary-a"},
		{Kind: haActionRewindFormerPrimary, StandbyName: "primary-a"},
		{Kind: haActionReseedFormerPrimary, StandbyName: "primary-a"},
	}

	for _, action := range directActions {
		t.Run(string(action.Kind), func(t *testing.T) {
			method, path := haAdminOperation(action)
			if method == "" || path == "" {
				t.Fatalf("direct admin action %s has no typed admin operation", action.Kind)
			}
			if !haPlannedActionSupportsDirectAdminAPI(action.Kind) {
				t.Fatalf("direct admin action %s has a typed admin operation but is not directly executable", action.Kind)
			}
			if !haActionRequiresAdminResult(action.Kind) {
				t.Fatalf("direct admin action %s is directly executable without typed result evidence", action.Kind)
			}
		})
	}

	unsupported := haPlannedAction{Kind: haActionUpdatePrimaryRoute, StandbyName: "standby-a"}
	method, path := haAdminOperation(unsupported)
	if method != "" || path != "" {
		t.Fatalf("controller-only action %s unexpectedly has typed admin operation %s %s", unsupported.Kind, method, path)
	}
	if haPlannedActionSupportsDirectAdminAPI(unsupported.Kind) {
		t.Fatalf("controller-only action %s unexpectedly supports direct admin API", unsupported.Kind)
	}
	if haActionRequiresAdminResult(unsupported.Kind) {
		t.Fatalf("controller-only action %s unexpectedly requires admin result evidence", unsupported.Kind)
	}
}

func TestHAAdminURLTargetsNodeLocalAdminAPI(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Standbys: []antflyv1.HAStandbySpec{{
			Name:     "standby-a",
			AdminURL: "http://standby-a-ha.default.svc:8081",
		}, {
			Name:     "old-primary",
			AdminURL: "http://old-primary-ha.default.svc:8081",
		}},
	}

	tests := []struct {
		name   string
		action haPlannedAction
		status *antflyv1.HAStatus
		want   string
	}{{
		name:   "primary scoped slot",
		action: haPlannedAction{Kind: haActionCreateSlot, StandbyName: "standby-a"},
		want:   "http://primary-ha.default.svc:8081",
	}, {
		name:   "standby scoped fence acquire",
		action: haPlannedAction{Kind: haActionAcquireFence, StandbyName: "standby-a"},
		want:   "http://standby-a-ha.default.svc:8081",
	}, {
		name:   "standby scoped fence acquire without node url",
		action: haPlannedAction{Kind: haActionAcquireFence, StandbyName: "missing-standby"},
		want:   "",
	}, {
		name:   "standby scoped promotion assessment",
		action: haPlannedAction{Kind: haActionAssessPromotion, StandbyName: "standby-a"},
		want:   "http://standby-a-ha.default.svc:8081",
	}, {
		name:   "standby scoped promotion",
		action: haPlannedAction{Kind: haActionPromoteStandby, StandbyName: "standby-a"},
		want:   "http://standby-a-ha.default.svc:8081",
	}, {
		name:   "former primary assess",
		action: haPlannedAction{Kind: haActionDemoteFormerPrimary, StandbyName: "old-primary"},
		want:   "http://old-primary-ha.default.svc:8081",
	}, {
		name:   "former primary rewind",
		action: haPlannedAction{Kind: haActionRewindFormerPrimary, StandbyName: "old-primary"},
		want:   "http://old-primary-ha.default.svc:8081",
	}, {
		name:   "former primary reseed scheduling",
		action: haPlannedAction{Kind: haActionReseedFormerPrimary, StandbyName: "old-primary"},
		want:   "http://primary-ha.default.svc:8081",
	}, {
		name:   "post-promotion primary scoped reseed uses promoted primary node",
		action: haPlannedAction{Kind: haActionReseedFormerPrimary, StandbyName: "old-primary"},
		status: &antflyv1.HAStatus{LastPromotion: &antflyv1.HAPromotionStatus{PromotedStandbyID: "standby-a"}},
		want:   "http://standby-a-ha.default.svc:8081",
	}, {
		name:   "post-promotion primary scoped seed uses promoted primary node",
		action: haPlannedAction{Kind: haActionSeedStandby, StandbyName: "old-primary"},
		status: &antflyv1.HAStatus{LastPromotion: &antflyv1.HAPromotionStatus{PromotedStandbyID: "standby-a"}},
		want:   "http://standby-a-ha.default.svc:8081",
	}, {
		name:   "post-promotion primary scoped action missing promoted node url fails closed",
		action: haPlannedAction{Kind: haActionSeedStandby, StandbyName: "old-primary"},
		status: &antflyv1.HAStatus{LastPromotion: &antflyv1.HAPromotionStatus{PromotedStandbyID: "standby-missing"}},
		want:   "",
	}, {
		name:   "former primary rewind without node url",
		action: haPlannedAction{Kind: haActionRewindFormerPrimary, StandbyName: "missing-old-primary"},
		want:   "",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := haAdminURL(tt.action, ha, tt.status); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestHAPlannedActionStatusTargetsPromotedPrimaryAdminURL(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Standbys: []antflyv1.HAStandbySpec{{
			Name:     "old-primary",
			AdminURL: "http://old-primary-ha.default.svc:8081",
		}, {
			Name:     "standby-a",
			AdminURL: "http://standby-a-ha.default.svc:8081",
		}},
		Identity: &antflyv1.HAReplicationIdentitySpec{
			CurrentPrimaryID: "old-primary",
		},
	}
	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			PromotedStandbyID: "standby-a",
		},
	}
	planned := haPlannedActionStatuses([]haPlannedAction{{
		Kind:        haActionSeedStandby,
		StandbyName: "old-primary",
		SlotName:    "old-primary",
		TargetLSN:   12,
	}}, ha, status)

	if len(planned) != 1 ||
		planned[0].AdminURL != "http://standby-a-ha.default.svc:8081" ||
		planned[0].AdminNodeID != "standby-a" {
		t.Fatalf("expected primary-scoped action to target promoted primary admin URL and node id, got %#v", planned)
	}
}

func TestHAFormerPrimaryAdminCommandUsesExecutableRejoinSubcommands(t *testing.T) {
	identity := &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       1,
		Epoch:            1,
		CurrentPrimaryID: "primary-a",
	}
	status := &antflyv1.HAStatus{LastPromotion: &antflyv1.HAPromotionStatus{
		OldPrimaryID:      "primary-a",
		PromotedStandbyID: "standby-a",
		ParentTimelineID:  1,
		ParentEpoch:       1,
		NewTimelineID:     2,
		NewEpoch:          2,
		RequiredLSN:       10,
		ObservedLSN:       10,
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration:   4,
		FenceToken:        "token",
	}}
	tests := []struct {
		kind       haActionKind
		subcommand string
	}{
		{kind: haActionDemoteFormerPrimary, subcommand: "assess"},
		{kind: haActionRewindFormerPrimary, subcommand: "rewind"},
		{kind: haActionReseedFormerPrimary, subcommand: "reseed"},
	}
	for _, tt := range tests {
		t.Run(string(tt.kind), func(t *testing.T) {
			command := haFormerPrimaryAdminCommand(haPlannedAction{
				Kind:            tt.kind,
				StandbyName:     "primary-a",
				TargetLSN:       10,
				ObservedLSN:     11,
				RetainedFromLSN: 8,
			}, identity, status)
			if len(command) < 2 || command[0] != "rejoin" || command[1] != tt.subcommand {
				t.Fatalf("expected rejoin %s command, got %#v", tt.subcommand, command)
			}
		})
	}
}

func TestHADirectAdminRequestBodiesMarshalOpenAPIFields(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{LastPromotion: &antflyv1.HAPromotionStatus{
		OldPrimaryID:      "primary-a",
		PromotedStandbyID: "standby-a",
		ParentTimelineID:  4,
		ParentEpoch:       6,
		NewTimelineID:     5,
		NewEpoch:          7,
		RequiredLSN:       12,
		ObservedLSN:       13,
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration:   3,
		FenceToken:        "ha-fence-token",
		FenceReason:       "LeaseAcquired",
	}}

	fence, ok := haFenceAcquireBody(cluster, antflyv1.HAPlannedActionStatus{
		StandbyName: "standby-a",
		TargetLSN:   12,
		Reason:      "AutomaticFailoverReady",
	})
	if !ok {
		t.Fatal("expected fence request body")
	}
	fenceJSON := marshalJSONMap(t, fence)
	if fenceJSON["old_primary_id"] != "primary-a" ||
		fenceJSON["promoted_node_id"] != "standby-a" ||
		fenceJSON["new_timeline_id"] != float64(5) ||
		fenceJSON["new_epoch"] != float64(7) ||
		fenceJSON["required_lsn"] != float64(12) ||
		fenceJSON["observed_lsn"] != float64(12) ||
		fenceJSON["reason"] != "AutomaticFailoverReady" {
		t.Fatalf("unexpected fence request JSON: %#v", fenceJSON)
	}
	fenceIdentity := fenceJSON["identity"].(map[string]any)
	if fenceIdentity["cluster_id"] != float64(100) ||
		fenceIdentity["timeline_id"] != float64(4) ||
		fenceIdentity["epoch"] != float64(6) {
		t.Fatalf("unexpected fence identity JSON: %#v", fenceIdentity)
	}

	fencedReason, ok := haFenceAcquireBody(cluster, antflyv1.HAPlannedActionStatus{
		StandbyName: "standby-a",
		TargetLSN:   12,
		FenceReason: "LeaseAcquired",
		Reason:      "AutomaticFailoverReady",
	})
	if !ok {
		t.Fatal("expected fence request body with fence reason")
	}
	fencedReasonJSON := marshalJSONMap(t, fencedReason)
	if fencedReasonJSON["reason"] != "LeaseAcquired" {
		t.Fatalf("expected fence request to prefer observed fence reason, got %#v", fencedReasonJSON)
	}

	rejoin, ok := haRejoinAssessBody(cluster, antflyv1.HAPlannedActionStatus{
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
	})
	if !ok {
		t.Fatal("expected rejoin request body")
	}
	rejoinJSON := marshalJSONMap(t, rejoin)
	if rejoinJSON["node_id"] != "primary-a" ||
		rejoinJSON["last_lsn"] != float64(13) ||
		rejoinJSON["retained_from_lsn"] != float64(8) ||
		rejoinJSON["allow_rewind_after_forced_promotion"] != false {
		t.Fatalf("unexpected rejoin request JSON: %#v", rejoinJSON)
	}
	receipt := rejoinJSON["receipt"].(map[string]any)
	if receipt["old_primary_id"] != "primary-a" ||
		receipt["promoted_node_id"] != "standby-a" ||
		receipt["generation"] != float64(3) ||
		receipt["token"] != "ha-fence-token" ||
		receipt["reason"] != "LeaseAcquired" {
		t.Fatalf("unexpected rejoin receipt JSON: %#v", receipt)
	}
	receiptIdentity := receipt["identity"].(map[string]any)
	if receiptIdentity["timeline_id"] != float64(5) || receiptIdentity["epoch"] != float64(7) {
		t.Fatalf("expected rejoin receipt identity to use promoted timeline, got %#v", receiptIdentity)
	}

	cluster.Status.HAStatus.LastPromotion.FenceAuthority = ""
	_, ok = haRejoinAssessBody(cluster, antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionRewindFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
	})
	if ok {
		t.Fatal("expected rejoin request body to require a concrete fence authority")
	}
}

func TestHAFenceAdminCommandUsesFenceReasonFallback(t *testing.T) {
	identity := &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
	command := haAdminCommand(haPlannedAction{
		Kind:            haActionAcquireFence,
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceGeneration: 3,
		FenceReason:     "LeaseHeld",
	}, identity, nil)

	if !reflect.DeepEqual(command, []string{
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
		"--reason", "LeaseHeld",
	}) {
		t.Fatalf("expected fence reason fallback in admin command, got %#v", command)
	}
}

func marshalJSONMap(t *testing.T, value any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal JSON map: %v", err)
	}
	return out
}

func TestHASeedAdminCommandRequiresTargetLSN(t *testing.T) {
	create := haAdminCommand(haPlannedAction{
		Kind:     haActionCreateSlot,
		SlotName: "standby-a",
	}, nil, nil)
	if !reflect.DeepEqual(create, []string{"slot", "create", "--slot", "standby-a"}) {
		t.Fatalf("unexpected create command without target LSN: %#v", create)
	}

	create = haAdminCommand(haPlannedAction{
		Kind:      haActionCreateSlot,
		SlotName:  "standby-a",
		TargetLSN: 5,
	}, nil, nil)
	if !reflect.DeepEqual(create, []string{"slot", "create", "--slot", "standby-a", "--initial-lsn", "5"}) {
		t.Fatalf("unexpected create command with target LSN: %#v", create)
	}

	command := haAdminCommand(haPlannedAction{
		Kind:     haActionSeedStandby,
		SlotName: "standby-a",
	}, nil, nil)
	if command != nil {
		t.Fatalf("expected seed command without target LSN to be suppressed, got %#v", command)
	}

	command = haAdminCommand(haPlannedAction{
		Kind:      haActionSeedStandby,
		SlotName:  "standby-a",
		TargetLSN: 5,
	}, nil, nil)
	if !reflect.DeepEqual(command, []string{"seed", "begin", "--slot", "standby-a", "--manifest-id", "base-standby-a-5"}) {
		t.Fatalf("unexpected seed command: %#v", command)
	}
}

func loadAdminOpenAPIOperations(t *testing.T) map[string]string {
	t.Helper()
	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	specPath := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "specs", "openapi", "antfly", "admin.yaml"))
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read admin OpenAPI spec %s: %v", specPath, err)
	}
	var spec struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
		} `json:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse admin OpenAPI spec %s: %v", specPath, err)
	}
	operations := map[string]string{}
	for path, methods := range spec.Paths {
		for method, operation := range methods {
			operations[strings.ToUpper(method)+" "+path] = operation.OperationID
		}
	}
	return operations
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
	if unhealthy == nil || unhealthy.Status != metav1.ConditionTrue || unhealthy.Reason != antflyv1.ReasonHAStandbyUnhealthy {
		t.Fatalf("expected unhealthy condition, got %#v", unhealthy)
	}
	lagging := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHALagging)
	if lagging == nil || lagging.Status != metav1.ConditionTrue || lagging.Reason != antflyv1.ReasonHAStandbyLagging {
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

func TestUpdateHAStatusReportsPrimaryAdminUnavailable(t *testing.T) {
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN:            10,
		PrimaryAdminReachable: false,
		PrimaryAdminLastError: "primary admin refused connection",
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:              "standby-a",
			SlotName:          "standby-a",
			Active:            true,
			ReceivedLSN:       10,
			AppliedLSN:        10,
			SafeReadLSN:       10,
			CanServeSafeReads: true,
		}},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil ||
		degraded.Status != metav1.ConditionTrue ||
		degraded.Reason != antflyv1.ReasonHAPrimaryAdminUnavailable ||
		!strings.Contains(degraded.Message, "primary admin refused connection") {
		t.Fatalf("expected primary admin unavailable degraded condition, got %#v", degraded)
	}

	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.PrimaryAdminLastError = ""
	reconciler.updateHAStatusAndConditions(cluster)

	degraded = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionFalse || degraded.Reason != antflyv1.ReasonHASyncPolicySatisfied {
		t.Fatalf("expected primary admin degraded condition to clear, got %#v", degraded)
	}
}

func TestUpdateHAStatusAllowsAutomaticPromotionOnlyWithFenceAndCaughtUpStandby(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.Standbys[0].RouteSelector = haTestRouteSelector("standby-a")
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

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked while primary admin failure is not observed")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAPrimaryStillReachable {
		t.Fatalf("expected primary-still-reachable condition, got %#v", failover)
	}

	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.PrimaryAdminLastError = ""
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked while primary admin is reachable")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAPrimaryStillReachable {
		t.Fatalf("expected primary-still-reachable condition, got %#v", failover)
	}

	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin connection refused"
	observedPrimaryLSN := cluster.Status.HAStatus.PrimaryLSN
	cluster.Status.HAStatus.PrimaryLSN = 0
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked without an observed primary LSN boundary")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAPromotionBoundaryMissing {
		t.Fatalf("expected promotion-boundary-missing condition, got %#v", failover)
	}

	cluster.Status.HAStatus.PrimaryLSN = observedPrimaryLSN
	reconciler.updateHAStatusAndConditions(cluster)

	if !cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be allowed with ready fencing, primary failure, and caught-up standby")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionTrue || failover.Reason != antflyv1.ReasonHAFencedPromotionReady {
		t.Fatalf("expected failover-ready condition, got %#v", failover)
	}
	plan := planHA(cluster)
	if len(plan.Actions) != 5 {
		t.Fatalf("expected fenced promotion action chain, got %#v", plan.Actions)
	}
	if plan.PromotionStandbyName != "standby-a" {
		t.Fatalf("expected promotion standby standby-a, got %q", plan.PromotionStandbyName)
	}
	if plan.Actions[0].Kind != haActionAcquireFence ||
		plan.Actions[1].Kind != haActionAssessPromotion ||
		plan.Actions[2].Kind != haActionPromoteStandby {
		t.Fatalf("unexpected promotion actions: %#v", plan.Actions)
	}
	if plan.Actions[1].StandbyName != "standby-a" ||
		plan.Actions[2].StandbyName != "standby-a" ||
		plan.Actions[3].RouteTo != "standby-a" {
		t.Fatalf("expected promotion and route actions to target standby-a, got %#v", plan.Actions)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 5 {
		t.Fatalf("expected fenced promotion action chain in status, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionAcquireFence) ||
		cluster.Status.HAStatus.PlannedActions[1].Kind != string(haActionAssessPromotion) ||
		cluster.Status.HAStatus.PlannedActions[2].Kind != string(haActionPromoteStandby) {
		t.Fatalf("unexpected promotion action status: %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].Phase != string(haActionPhaseFence) ||
		cluster.Status.HAStatus.PlannedActions[1].Phase != string(haActionPhasePromote) ||
		cluster.Status.HAStatus.PlannedActions[2].Phase != string(haActionPhasePromote) ||
		cluster.Status.HAStatus.PlannedActions[3].Phase != string(haActionPhaseRoute) ||
		cluster.Status.HAStatus.PlannedActions[4].Phase != string(haActionPhaseRejoin) ||
		cluster.Status.HAStatus.PlannedActions[0].Executor != string(haActionExecutorAdminAPI) ||
		cluster.Status.HAStatus.PlannedActions[3].Executor != string(haActionExecutorControllerAction) {
		t.Fatalf("expected promotion action status to publish phase/executor metadata, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[1].StandbyName != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[1].DependsOn != string(haActionAcquireFence) ||
		cluster.Status.HAStatus.PlannedActions[2].DependsOn != string(haActionAssessPromotion) ||
		cluster.Status.HAStatus.PlannedActions[3].DependsOn != string(haActionPromoteStandby) ||
		cluster.Status.HAStatus.PlannedActions[4].DependsOn != string(haActionPromoteStandby) ||
		cluster.Status.HAStatus.PlannedActions[3].RouteFrom != "primary" ||
		cluster.Status.HAStatus.PlannedActions[3].RouteTo != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[4].RouteFrom != "primary-a" {
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
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[1].AdminCommand, []string{"promote", "assess", "--current-fence"}) {
		t.Fatalf("unexpected promotion-assessment admin command: %#v", cluster.Status.HAStatus.PlannedActions[1].AdminCommand)
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[2].AdminCommand, []string{"promote", "--current-fence"}) {
		t.Fatalf("unexpected promote admin command: %#v", cluster.Status.HAStatus.PlannedActions[2].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://standby-a-ha.default.svc:8081" {
		t.Fatalf("expected acquire-fence action to target standby HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[0])
	}
	if cluster.Status.HAStatus.PlannedActions[1].AdminURL != "http://standby-a-ha.default.svc:8081" ||
		cluster.Status.HAStatus.PlannedActions[2].AdminURL != "http://standby-a-ha.default.svc:8081" {
		t.Fatalf("expected promotion actions to target standby HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[1:3])
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminNodeID != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[1].AdminNodeID != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[2].AdminNodeID != "standby-a" {
		t.Fatalf("expected fence/promotion actions to require standby node receipts, got %#v", cluster.Status.HAStatus.PlannedActions[:3])
	}
	if cluster.Status.HAStatus.PlannedActions[3].AdminCommand != nil {
		t.Fatalf("route action should not publish an HA admin command without service execution context, got %#v", cluster.Status.HAStatus.PlannedActions[3].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[3].AdminURL != "" {
		t.Fatalf("route action should not publish an HA admin URL without service execution context, got %#v", cluster.Status.HAStatus.PlannedActions[3].AdminURL)
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
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[4].AdminCommand, expectedDemoteCommand) {
		t.Fatalf("unexpected former-primary demote admin command: %#v", cluster.Status.HAStatus.PlannedActions[4].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[4].AdminURL != "" {
		t.Fatalf("expected former-primary demote to require a former-primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[4])
	}
	if cluster.Status.HAStatus.PlannedActions[4].AdminNodeID != "primary-a" {
		t.Fatalf("expected former-primary demote to require old primary node receipt, got %#v", cluster.Status.HAStatus.PlannedActions[4])
	}
	if cluster.Status.HAStatus.PlannedActions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		cluster.Status.HAStatus.PlannedActions[0].FenceHolder != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[0].FenceGeneration != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].FenceReason != "LeaseHeld" {
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

func TestUpdateHAStatusBlocksAutomaticPromotionWhenAdminExecutionDisabled(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: false,
	}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.Standbys[0].RouteSelector = haTestRouteSelector("standby-a")
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked when admin execution is disabled")
	}
	failover := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse ||
		failover.Reason != antflyv1.ReasonHAAutomaticFailoverExecutionDisabled {
		t.Fatalf("expected admin-execution-disabled condition, got %#v", failover)
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind == string(haActionAcquireFence) || action.Kind == string(haActionAssessPromotion) || action.Kind == string(haActionPromoteStandby) {
			t.Fatalf("expected no automatic promotion actions when admin execution is disabled, got %#v", cluster.Status.HAStatus.PlannedActions)
		}
	}
}

func TestUpdateHAStatusBlocksAutomaticPromotionWithUnsupportedFencingAuthority(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.Standbys[0].RouteSelector = haTestRouteSelector("standby-a")
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityExternal,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityExternal,
		Ready:      true,
		Holder:     "standby-a",
		Generation: 1,
		Reason:     "ExternalFenceReady",
	}
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked for unsupported fencing authority")
	}
	failover := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse ||
		failover.Reason != antflyv1.ReasonHAFencingAuthorityUnsupported {
		t.Fatalf("expected unsupported-fencing-authority condition, got %#v", failover)
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind == string(haActionAcquireFence) || action.Kind == string(haActionAssessPromotion) || action.Kind == string(haActionPromoteStandby) {
			t.Fatalf("expected no automatic promotion actions with unsupported fencing authority, got %#v", cluster.Status.HAStatus.PlannedActions)
		}
	}
}

func TestUpdateHAStatusBlocksAutomaticPromotionWithoutRouteSelector(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin connection refused"
	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked without a standby route selector")
	}
	failover := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAPrimaryRouteSelectorMissing {
		t.Fatalf("expected route-selector-missing condition, got %#v", failover)
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind == string(haActionAcquireFence) || action.Kind == string(haActionAssessPromotion) || action.Kind == string(haActionPromoteStandby) {
			t.Fatalf("expected no automatic promotion actions without route selector, got %#v", cluster.Status.HAStatus.PlannedActions)
		}
	}
}

func TestUpdateHAStatusBlocksAutomaticPromotionWithoutStandbyAdminURL(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster.Spec.HighAvailability.Standbys[0].RouteSelector = haTestRouteSelector("standby-a")
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin connection refused"
	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked without standby admin URL")
	}
	failover := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAFencingNotReady {
		t.Fatalf("expected fence-not-ready condition for holder without admin URL, got %#v", failover)
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind == string(haActionAcquireFence) || action.Kind == string(haActionAssessPromotion) || action.Kind == string(haActionPromoteStandby) {
			t.Fatalf("expected no automatic promotion actions without standby admin URL, got %#v", cluster.Status.HAStatus.PlannedActions)
		}
	}
}

func TestUpdateHAStatusRequiresSafeReadProgressForAvailabilityAndAutomaticPromotion(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.Standbys[0].RouteSelector = haTestRouteSelector("standby-a")
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
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
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

func TestUpdateHAStatusPrefersReachableAdminDurabilityDecision(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{
		{Name: "standby-a"},
	}
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:          antflyv1.HADurabilityModeRemoteApply,
		Selection:     antflyv1.HAStandbySelectionFirst,
		Required:      1,
		StandbyNames:  []string{"standby-a"},
		FailurePolicy: antflyv1.HAFailurePolicyFailClosed,
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN:            10,
		PrimaryAdminReachable: true,
		PrimaryAdminLastError: "",
		Sync: antflyv1.HASyncStatus{
			Mode:       antflyv1.HADurabilityModeRemoteApply,
			Selection:  antflyv1.HAStandbySelectionFirst,
			Required:   1,
			Satisfied:  0,
			Candidates: 1,
			Degraded:   true,
			Action:     "RejectWrites",
		},
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:              "standby-a",
			SlotName:          "standby-a",
			Active:            true,
			ReceivedLSN:       10,
			AppliedLSN:        10,
			SafeReadLSN:       10,
			ApplyLagLSN:       0,
			SafeReadLagLSN:    0,
			CanServeSafeReads: true,
			Status:            "healthy",
		}},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != antflyv1.ReasonHASyncPolicyUnsatisfied {
		t.Fatalf("expected reachable primary admin durability to mark sync degraded, got %#v", degraded)
	}
	if cluster.Status.HAStatus.Sync.Mode != antflyv1.HADurabilityModeRemoteApply ||
		cluster.Status.HAStatus.Sync.Selection != antflyv1.HAStandbySelectionFirst ||
		cluster.Status.HAStatus.Sync.Required != 1 ||
		cluster.Status.HAStatus.Sync.Satisfied != 0 ||
		cluster.Status.HAStatus.Sync.Candidates != 1 ||
		cluster.Status.HAStatus.Sync.FailurePolicy != antflyv1.HAFailurePolicyFailClosed ||
		!cluster.Status.HAStatus.Sync.Degraded ||
		cluster.Status.HAStatus.Sync.Action != "RejectWrites" {
		t.Fatalf("unexpected admin-derived sync status: %#v", cluster.Status.HAStatus.Sync)
	}

	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "connection refused"
	reconciler.updateHAStatusAndConditions(cluster)

	degraded = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Reason != antflyv1.ReasonHAPrimaryAdminUnavailable {
		t.Fatalf("expected unreachable primary admin to stop trusting stale admin sync evidence, got %#v", degraded)
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
			CurrentTarget:   "standby-a",
			FenceGeneration: 4,
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
			ParentEpoch:       1,
			NewTimelineID:     2,
			NewEpoch:          2,
			SwitchLSN:         10,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
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
		former.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		former.FenceHolder != "standby-a" ||
		former.Action != string(haActionRewindFormerPrimary) ||
		former.Reason != "FormerPrimaryNeedsRewind" {
		t.Fatalf("unexpected rewind disposition: %#v", former)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionRewindFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[0].StandbyName != "old-primary" ||
		cluster.Status.HAStatus.PlannedActions[0].TargetLSN != 10 ||
		cluster.Status.HAStatus.PlannedActions[0].ObservedLSN != 10 ||
		cluster.Status.HAStatus.PlannedActions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		cluster.Status.HAStatus.PlannedActions[0].FenceHolder != "standby-a" {
		t.Fatalf("expected rewind planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	cluster.Status.HAStatus.Fencing.Authority = antflyv1.HAFencingAuthorityStorageFence
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		former.Fenced ||
		former.Action != string(haActionDemoteFormerPrimary) ||
		former.Reason != "FormerPrimaryFenceNotObserved" {
		t.Fatalf("expected authority-mismatched fence to block former-primary rejoin, got %#v", former)
	}

	cluster.Status.HAStatus.Fencing.Authority = antflyv1.HAFencingAuthorityKubernetesLease
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
	if len(cluster.Status.HAStatus.PlannedActions) != 2 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionReseedFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[1].Kind != string(haActionSeedStandby) ||
		cluster.Status.HAStatus.PlannedActions[1].DependsOn != string(haActionReseedFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[1].StandbyName != "old-primary" {
		t.Fatalf("expected reseed planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{
		NodeID:            "old-primary",
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:       "standby-a",
		FenceGeneration:   4,
		TargetTimelineID:  2,
		TargetEpoch:       2,
		ForkLSN:           10,
		FormerLastLSN:     11,
		RetainedFromLSN:   8,
		DataLossDiscarded: true,
		AssessedAction:    "rewind",
		AssessedReason:    "parent_timeline_retained",
	}
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		!former.RewindPossible ||
		former.ReseedRequired ||
		former.Action != string(haActionRewindFormerPrimary) ||
		former.Reason != "parent_timeline_retained" ||
		former.SwitchLSN != 10 ||
		former.ObservedLSN != 11 {
		t.Fatalf("expected recorded rejoin assessment to preserve rewind disposition, got %#v", former)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionRewindFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[0].TargetLSN != 10 ||
		cluster.Status.HAStatus.PlannedActions[0].ObservedLSN != 11 {
		t.Fatalf("expected assessed rewind planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.FormerPrimary.FenceGeneration = 3
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		!former.ReseedRequired ||
		!former.Diverged ||
		former.RewindPossible ||
		former.Action != string(haActionReseedFormerPrimary) ||
		former.Reason != "FormerPrimaryRequiresReseed" {
		t.Fatalf("expected stale assessed fence generation to be ignored, got %#v", former)
	}

	cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{
		NodeID:            "old-primary",
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:       "standby-a",
		FenceGeneration:   4,
		TargetTimelineID:  2,
		TargetEpoch:       2,
		ForkLSN:           10,
		FormerLastLSN:     11,
		RetainedFromLSN:   8,
		DataLossDiscarded: true,
		AssessedAction:    "rewind",
		AssessedReason:    "parent_timeline_retained",
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

func TestUpdateHAStatusUsesPromotionReceiptForFormerPrimaryRejoinAfterFenceExpires(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "old-primary",
	}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:     "old-primary",
		AdminURL: "http://old-primary-ha.default.svc:8081",
	}, {
		Name:     "standby-a",
		AdminURL: "http://standby-a-ha.default.svc:8081",
	}}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
			CurrentTarget:   "standby-a",
			FenceGeneration: 4,
		},
		Retention: antflyv1.HARetentionStatus{
			OldestRestartLSN: 8,
		},
		Fencing: antflyv1.HAFencingStatus{
			Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
			Ready:      false,
			Holder:     "standby-a",
			Generation: 4,
			Reason:     "LeaseExpired",
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
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   4,
			FenceReason:       "operator-approved",
			FenceToken:        "token",
		},
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:        "old-primary",
			SlotName:    "old-primary",
			Active:      true,
			TimelineID:  4,
			ReceivedLSN: 10,
			AppliedLSN:  10,
		}, {
			Name:        "standby-a",
			SlotName:    "standby-a",
			Active:      true,
			TimelineID:  5,
			ReceivedLSN: 12,
			AppliedLSN:  12,
			SafeReadLSN: 12,
		}},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	former := cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		!former.Fenced ||
		!former.RejoinRequired ||
		!former.RewindPossible ||
		former.ReseedRequired ||
		former.Action != string(haActionRewindFormerPrimary) ||
		former.Reason != "FormerPrimaryNeedsRewind" {
		t.Fatalf("expected promotion receipt to permit rewind after live fence expiry, got %#v", former)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionRewindFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://old-primary-ha.default.svc:8081" {
		t.Fatalf("expected executable former-primary rewind, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	command := strings.Join(cluster.Status.HAStatus.PlannedActions[0].AdminCommand, " ")
	if !strings.Contains(command, "--fence-token token") ||
		!strings.Contains(command, "--fence-generation 4") {
		t.Fatalf("expected rejoin command to carry durable fence receipt, got %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
	}
}

func TestUpdateHAStatusRendersFormerPrimaryRejoinCommandsWithReceipt(t *testing.T) {
	cluster := haCluster()
	disabled := false
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:     "old-primary",
		AdminURL: "http://old-primary-ha.default.svc:8081",
	}, {
		Name:     "standby-a",
		Desired:  &disabled,
		AdminURL: "http://standby-a-ha.default.svc:8081",
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
			CurrentTarget:   "standby-a",
			FenceGeneration: 4,
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
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
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
	if cluster.Status.HAStatus.PlannedActions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		cluster.Status.HAStatus.PlannedActions[0].FenceHolder != "standby-a" {
		t.Fatalf("expected former-primary planned action to carry fence identity, got %#v", cluster.Status.HAStatus.PlannedActions[0])
	}

	cluster.Status.HAStatus.LastPromotion.FenceToken = "token"
	reconciler.updateHAStatusAndConditions(cluster)

	expected := []string{
		"rejoin", "rewind",
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
	if cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://old-primary-ha.default.svc:8081" {
		t.Fatalf("expected former primary rejoin to target former-primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[0])
	}

	cluster.Status.HAStatus.LastPromotion.Forced = true
	cluster.Status.HAStatus.LastPromotion.DataLossPossible = true
	reconciler.updateHAStatusAndConditions(cluster)

	if len(cluster.Status.HAStatus.PlannedActions) != 2 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionReseedFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://standby-a-ha.default.svc:8081" {
		t.Fatalf("expected forced promotion to require former-primary reseed, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	forcedCommand := strings.Join(cluster.Status.HAStatus.PlannedActions[0].AdminCommand, " ")
	if !strings.Contains(forcedCommand, "--fence-forced") {
		t.Fatalf("expected forced rejoin command to carry forced fence evidence, got %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
	}
	if strings.Contains(forcedCommand, "allow-rewind-after-forced-promotion") {
		t.Fatalf("forced promotion must not opt into former-primary rewind automatically, got %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
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
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   5,
			FenceReason:       "operator-approved",
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
		route.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		route.FenceGeneration != 5 ||
		!route.Stale ||
		route.Action != string(haActionUpdatePrimaryRoute) {
		t.Fatalf("expected route update after completed promotion, got %#v", route)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionUpdatePrimaryRoute) ||
		cluster.Status.HAStatus.PlannedActions[0].RouteFrom != "primary" ||
		cluster.Status.HAStatus.PlannedActions[0].RouteTo != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		cluster.Status.HAStatus.PlannedActions[0].FenceGeneration != 5 ||
		cluster.Status.HAStatus.PlannedActions[0].FenceReason != "operator-approved" {
		t.Fatalf("expected route planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.PrimaryRoute.CurrentTarget = "standby-a"
	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = antflyv1.HAFencingAuthorityKubernetesLease
	cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = 4
	reconciler.updateHAStatusAndConditions(cluster)

	route = cluster.Status.HAStatus.PrimaryRoute
	if !route.Stale ||
		route.Action != string(haActionUpdatePrimaryRoute) ||
		route.Reason != "PrimaryRouteFenceGenerationStale" ||
		route.CurrentTarget != "standby-a" ||
		route.DesiredTarget != "standby-a" ||
		route.FenceGeneration != 5 {
		t.Fatalf("expected route update for stale fence generation, got %#v", route)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionUpdatePrimaryRoute) ||
		cluster.Status.HAStatus.PlannedActions[0].RouteFrom != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[0].RouteTo != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		cluster.Status.HAStatus.PlannedActions[0].FenceGeneration != 5 {
		t.Fatalf("expected route planned action for stale fence generation, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = 5
	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = antflyv1.HAFencingAuthorityStorageFence
	reconciler.updateHAStatusAndConditions(cluster)

	route = cluster.Status.HAStatus.PrimaryRoute
	if !route.Stale ||
		route.Action != string(haActionUpdatePrimaryRoute) ||
		route.Reason != "PrimaryRouteFenceAuthorityStale" ||
		route.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease {
		t.Fatalf("expected route update for stale fence authority, got %#v", route)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 1 ||
		cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionUpdatePrimaryRoute) ||
		cluster.Status.HAStatus.PlannedActions[0].FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease {
		t.Fatalf("expected route planned action for stale fence authority, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = antflyv1.HAFencingAuthorityKubernetesLease
	reconciler.updateHAStatusAndConditions(cluster)

	route = cluster.Status.HAStatus.PrimaryRoute
	if route.Stale || route.Action != "None" || route.DesiredTarget != "standby-a" {
		t.Fatalf("expected current route after update, got %#v", route)
	}
	if len(cluster.Status.HAStatus.PlannedActions) != 0 {
		t.Fatalf("expected no route planned action once current, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestUpdateHAStatusDoesNotReplanRecordedPromotion(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin connection refused"
	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	cluster.Status.HAStatus.PrimaryRoute = antflyv1.HAPrimaryRouteStatus{CurrentTarget: "standby-a", FenceGeneration: 1}
	cluster.Status.HAStatus.LastPromotion = &antflyv1.HAPromotionStatus{
		OldPrimaryID:      "primary-a",
		PromotedStandbyID: "standby-a",
		ParentTimelineID:  4,
		ParentEpoch:       6,
		NewTimelineID:     5,
		NewEpoch:          7,
		SwitchLSN:         12,
		RequiredLSN:       12,
		ObservedLSN:       12,
		FenceGeneration:   1,
		FenceToken:        "token",
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked after a promotion has already been recorded")
	}
	failover := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAPromotionAlreadyRecorded {
		t.Fatalf("expected promotion-already-recorded condition, got %#v", failover)
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind == string(haActionAcquireFence) || action.Kind == string(haActionPromoteStandby) {
			t.Fatalf("expected no repeated promotion action after recorded promotion, got %#v", cluster.Status.HAStatus.PlannedActions)
		}
	}
	route := cluster.Status.HAStatus.PrimaryRoute
	if route.Stale || route.Action != "None" || route.CurrentTarget != "standby-a" || route.DesiredTarget != "standby-a" {
		t.Fatalf("expected route to remain current after recorded promotion, got %#v", route)
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

func TestReconcileHAFencingLeaseSkipsWhilePrimaryAdminReachable(t *testing.T) {
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
	err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no fencing lease while primary admin remains observable, got lease=%#v err=%v", lease, err)
	}
}

func TestReconcileHAFencingLeaseSkipsWhenAdminExecutionDisabled(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.Admin.ExecutePlannedActions = false
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	reconciler := testHAReconciler(t, cluster)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	lease := &coordinationv1.Lease{}
	err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no fencing lease when admin execution is disabled, got lease=%#v err=%v", lease, err)
	}
	if haKubernetesLeaseRenewalEnabled(cluster) {
		t.Fatal("expected HA lease renewal to be disabled when admin execution is disabled")
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
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
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
		t.Fatal("expected reconciled Kubernetes lease and primary admin failure to satisfy automatic promotion fencing gate")
	}
}

func TestReconcileHAFencingLeaseRetargetsUnsafeHolder(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.Standbys = append(cluster.Spec.HighAvailability.Standbys, antflyv1.HAStandbySpec{
		Name:          "standby-b",
		AdminURL:      "http://standby-b-ha.default.svc:8081",
		RouteSelector: haTestRouteSelector("standby-b"),
	})
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN:            12,
		PrimaryAdminReachable: false,
		PrimaryAdminLastError: "primary admin timeout",
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
		PrimaryLSN:            12,
		PrimaryAdminReachable: false,
		PrimaryAdminLastError: "primary admin timeout",
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

func TestReconcileHAFencingLeaseSkipsCandidateWithoutAdminURL(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = ""
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	reconciler := testHAReconciler(t, cluster)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	lease := &coordinationv1.Lease{}
	err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no fencing lease without candidate admin URL, got lease=%#v err=%v", lease, err)
	}
}

func TestReconcileHAFencingLeaseSkipsWithoutPromotionBoundary(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryLSN = 0
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	reconciler := testHAReconciler(t, cluster)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	lease := &coordinationv1.Lease{}
	err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no fencing lease without promotion boundary, got lease=%#v err=%v", lease, err)
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
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
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
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster.Spec.HighAvailability.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	cluster.Spec.HighAvailability.Standbys[0].RouteSelector = haTestRouteSelector("standby-a")
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	return cluster
}

func haTestRouteSelector(component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "antfly-database",
		"app.kubernetes.io/component": component,
		"app.kubernetes.io/instance":  "antfly",
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
