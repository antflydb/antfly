package controllers

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strconv"
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
		return
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
		return
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
	if plan.Actions[1].Kind != haActionSeedStandby || plan.Actions[1].DependsOn != haActionCreateSlot || plan.Actions[1].TargetLSN != 10 {
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
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[1].AdminCommand, []string{"seed", "begin", "--slot", "standby-a", "--manifest-id", "base-standby-a-10"}) {
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

func TestPlanHAPromotedNodeNeverBecomesItsOwnStandby(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:     "standby-a",
		SlotName: "standby-a",
		AdminURL: "http://standby-a-ha.default.svc:8081",
	}}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 21,
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "primary-a",
			PromotedStandbyID: "standby-a",
			ParentTimelineID:  1,
			ParentEpoch:       1,
			NewTimelineID:     2,
			NewEpoch:          2,
			RequiredLSN:       20,
			ObservedLSN:       20,
			SwitchLSN:         21,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   1,
			FenceToken:        "token",
		},
	}

	plan := planHA(cluster)
	if plan.DesiredStandbyCount != 0 {
		t.Fatalf("promoted node must be removed from the desired standby count, got %d", plan.DesiredStandbyCount)
	}
	for _, action := range plan.Actions {
		switch action.Kind {
		case haActionCreateSlot, haActionSeedStandby, haActionResumeSlot, haActionMarkReseed:
			if action.StandbyName == "standby-a" || action.SlotName == "standby-a" {
				t.Fatalf("promoted node must not be planned as its own standby: %#v", action)
			}
		}
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
	previous.AdminStatusCode = 409
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
		preserved[0].AdminStatusCode != 409 ||
		preserved[0].AdminResult == nil ||
		preserved[0].AdminResult.SlotAction != "create" {
		t.Fatalf("expected execution state to survive identical replan, got %#v", preserved[0])
	}

	changed := []haPlannedAction{{
		Kind:        haActionCreateSlot,
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		TargetLSN:   6,
		Reason:      "PrimaryLSNAdvanced",
	}}
	frozen := haPlannedActionStatuses(changed, ha, status)
	if frozen[0].TargetLSN != previous.TargetLSN ||
		frozen[0].Reason != previous.Reason ||
		frozen[0].AdminJobName != haAdminDirectAPIName ||
		frozen[0].AdminJobPhase != haAdminJobPhaseSucceeded ||
		frozen[0].AdminError != "previous direct-admin-api diagnostic" ||
		frozen[0].AdminStatusCode != 409 ||
		frozen[0].AdminResult == nil ||
		frozen[0].AdminResult.SlotAction != "create" {
		t.Fatalf("expected an executing semantic operation to retain its exact frozen payload and evidence across mutable LSN/reason drift, got %#v", frozen[0])
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
		ObservedLSN:     12,
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
		FormerLastLSN:           12,
		RetainedFromLSN:         8,
		RewindExecuted:          true,
		RewindPreviousLastLSN:   12,
		RewindCurrentLastLSN:    13,
		RewindNextLSN:           14,
		RewindDiscardedLSNCount: 0,
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

func TestHAPlannedActionStatusesRetainSuccessfulFormerPrimaryAssessmentUntilDisposition(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Standbys: []antflyv1.HAStandbySpec{{
			Name:     "old-primary",
			AdminURL: "http://old-primary-ha.default.svc:8081",
		}},
	}
	previous := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionDemoteFormerPrimary),
		Phase:           string(haActionPhaseRejoin),
		Executor:        string(haActionExecutorAdminAPI),
		DependsOn:       string(haActionPromoteStandby),
		StandbyName:     "old-primary",
		TargetLSN:       10,
		ObservedLSN:     10,
		RetainedFromLSN: 8,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 4,
		AdminURL:        "http://old-primary-ha.default.svc:8081",
		AdminNodeID:     "old-primary",
		AdminMethod:     "POST",
		AdminPath:       haAdminRejoinAssessPath,
		Reason:          "PromotionPlanned",
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			SchemaVersion:    1,
			ActionID:         "rejoin_assess:old-primary",
			ActionKind:       "rejoin_assess",
			ActionTarget:     "old-primary",
			ActionState:      "assessed",
			ActionNodeID:     "old-primary",
			RejoinAction:     "reseed",
			FormerNodeID:     "old-primary",
			TargetTimelineID: 2,
			TargetEpoch:      2,
			ForkLSN:          10,
			FormerLastLSN:    11,
			RetainedFromLSN:  8,
		},
	}
	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "old-primary",
			PromotedStandbyID: "standby-a",
			ParentTimelineID:  1,
			ParentEpoch:       1,
			NewTimelineID:     2,
			NewEpoch:          2,
			RequiredLSN:       10,
			ObservedLSN:       10,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   4,
			FenceToken:        "ha-fence-token",
		},
		FormerPrimary: &antflyv1.HAFormerPrimaryStatus{
			NodeID:          "old-primary",
			Fenced:          true,
			FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
			FenceHolder:     "standby-a",
			FenceGeneration: 4,
			AssessedAction:  "reseed",
		},
		PlannedActions: []antflyv1.HAPlannedActionStatus{previous},
	}
	if !haFormerPrimaryDemotePreserveAllowed(status, previous) {
		t.Fatalf("expected former-primary assessment to remain bound to the recorded durable fence: %#v", status)
	}
	if !haAdminActionSucceededWithStatusEvidence(status, previous) {
		t.Fatalf("expected valid typed former-primary assessment evidence: %#v", previous)
	}

	actions := haPlannedActionStatuses([]haPlannedAction{{
		Kind:            haActionReseedFormerPrimary,
		DependsOn:       haActionFenceFormerPrimary,
		StandbyName:     "old-primary",
		TargetLSN:       10,
		ObservedLSN:     11,
		RetainedFromLSN: 8,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 4,
		Reason:          "parent_timeline_wal_expired",
	}}, ha, status)

	assessment, ok := haPlannedActionByKind(actions, haActionDemoteFormerPrimary)
	if !ok || assessment.AdminJobPhase != haAdminJobPhaseSucceeded || assessment.AdminResult == nil {
		t.Fatalf("expected successful typed assessment evidence to survive the disposition replan, got %#v", actions)
	}
	if _, ok := haPlannedActionByKind(actions, haActionReseedFormerPrimary); !ok {
		t.Fatalf("expected current reseed disposition alongside assessment evidence, got %#v", actions)
	}
}

func TestHACompletedFormerPrimaryRewindRemainsIdempotentAcrossRetentionMovement(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Standbys: []antflyv1.HAStandbySpec{{
			Name:     "old-primary",
			AdminURL: "http://old-primary-ha.default.svc:8081",
		}},
	}
	rewind := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionRewindFormerPrimary),
		Phase:           string(haActionPhaseRejoin),
		Executor:        string(haActionExecutorAdminAPI),
		DependsOn:       string(haActionFenceFormerPrimary),
		StandbyName:     "old-primary",
		TargetLSN:       10,
		ObservedLSN:     10,
		RetainedFromLSN: 8,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 4,
		AdminURL:        "http://old-primary-ha.default.svc:8081",
		AdminNodeID:     "old-primary",
		AdminMethod:     "POST",
		AdminPath:       haAdminRejoinRewindPath,
		Reason:          "parent_timeline_retained",
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			SchemaVersion:           1,
			ActionID:                "rejoin_rewind:old-primary",
			ActionKind:              "rejoin_rewind",
			ActionTarget:            "old-primary",
			ActionState:             "applied",
			ActionNodeID:            "old-primary",
			RejoinAction:            "rewind",
			RejoinReason:            "parent_timeline_retained",
			FormerNodeID:            "old-primary",
			TargetTimelineID:        2,
			TargetEpoch:             2,
			ForkLSN:                 10,
			FormerLastLSN:           10,
			RetainedFromLSN:         8,
			RewindExecuted:          true,
			RewindPreviousLastLSN:   10,
			RewindCurrentLastLSN:    11,
			RewindNextLSN:           12,
			RewindDiscardedLSNCount: 0,
		},
	}
	assessment := *rewind.DeepCopy()
	assessment.Kind = string(haActionDemoteFormerPrimary)
	assessment.AdminPath = haAdminRejoinAssessPath
	assessment.AdminResult.ActionID = "rejoin_assess:old-primary"
	assessment.AdminResult.ActionKind = "rejoin_assess"
	assessment.AdminResult.ActionState = "assessed"
	assessment.AdminResult.RewindExecuted = false
	assessment.AdminResult.RewindPreviousLastLSN = 0
	assessment.AdminResult.RewindCurrentLastLSN = 0
	assessment.AdminResult.RewindNextLSN = 0
	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "old-primary",
			PromotedStandbyID: "standby-a",
			ParentTimelineID:  1,
			ParentEpoch:       1,
			NewTimelineID:     2,
			NewEpoch:          2,
			RequiredLSN:       10,
			ObservedLSN:       10,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   4,
			FenceToken:        "ha-fence-token",
		},
		FormerPrimary: &antflyv1.HAFormerPrimaryStatus{
			NodeID:           "old-primary",
			Fenced:           true,
			FenceAuthority:   antflyv1.HAFencingAuthorityKubernetesLease,
			FenceHolder:      "standby-a",
			FenceGeneration:  4,
			TargetTimelineID: 2,
			TargetEpoch:      2,
			ForkLSN:          10,
			FormerLastLSN:    10,
			RetainedFromLSN:  8,
			AssessedAction:   "rewind",
			AssessedReason:   "parent_timeline_retained",
			RejoinRequired:   true,
			RewindPossible:   true,
		},
		PlannedActions: []antflyv1.HAPlannedActionStatus{assessment, rewind},
	}

	evaluation := haEvaluateFormerPrimary(status)
	if evaluation.RejoinRequired || evaluation.Action != "None" || evaluation.Reason != "FormerPrimaryRewindApplied" {
		t.Fatalf("expected a safely applied timeline switch to make rewind idempotently complete, got %#v", evaluation)
	}
	retained := haPlannedActionStatuses(nil, ha, status)
	completed, ok := haPlannedActionByKind(retained, haActionRewindFormerPrimary)
	if !ok || completed.AdminJobPhase != haAdminJobPhaseSucceeded || completed.AdminResult == nil {
		t.Fatalf("expected successful rewind receipt to remain auditable after the current plan empties, got %#v", retained)
	}
	if assessed, ok := haPlannedActionByKind(retained, haActionDemoteFormerPrimary); !ok || assessed.AdminJobPhase != haAdminJobPhaseSucceeded || assessed.AdminResult == nil {
		t.Fatalf("expected the typed assessment receipt to remain alongside the completed rewind, got %#v", retained)
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

	previous = action
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseFailed
	previous.AdminError = "HA admin API returned status 409: BaseBackupSlotInUse"
	previous.AttemptCount = defaultHADirectAdminRetryLimit
	previous.ErrorClass = "RetryBudgetExhausted"
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}
	preserved = haPreservePlannedActionExecution(action, status)
	if preserved.AdminJobName != haAdminDirectAPIName ||
		preserved.AdminJobPhase != haAdminJobPhaseFailed ||
		preserved.AdminError == "" ||
		preserved.AttemptCount != defaultHADirectAdminRetryLimit ||
		preserved.ErrorClass != "RetryBudgetExhausted" {
		t.Fatalf("expected terminal direct-admin failure to survive replan, got %#v", preserved)
	}

	previous = action
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseFailed
	previous.AdminError = "HA admin action DemoteFormerPrimary succeeded without typed rejoin assessment"
	previous.ErrorClass = "PermanentAdminError"
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}
	preserved = haPreservePlannedActionExecution(action, status)
	if preserved.AdminJobName != haAdminDirectAPIName ||
		preserved.AdminJobPhase != haAdminJobPhaseFailed ||
		preserved.AdminError == "" || preserved.ErrorClass != "PermanentAdminError" {
		t.Fatalf("expected typed-evidence failure to remain terminal until desired identity changes, got %#v", preserved)
	}

	previous = action
	previous.AdminJobName = haAdminDirectAPIName
	previous.AdminJobPhase = haAdminJobPhaseSucceeded
	previous.AdminResult = haPromotionAdminResult(7, "ha-fence-token", "standby-a")
	status.PlannedActions = []antflyv1.HAPlannedActionStatus{previous}
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
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       4,
			Epoch:            6,
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

func TestPlanHAPlansPortablePublishRestoreAndVerifiedBootstrap(t *testing.T) {
	initial := uint64(5)
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 9}
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID: 100, TimelineID: 4, Epoch: 6, CurrentPrimaryID: "primary-a",
	}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:             "standby-a",
		InitialLSN:       &initial,
		AdminURL:         "http://standby-a-ha.default.svc:8081",
		SeedManifestPath: "/source/seed/manifest.afha",
		SeedContentRoot:  "/source/seed/content",
		SeedArtifact: &antflyv1.HASeedArtifactSpec{
			Location:         "s3://ha-seeds/cluster-a",
			GenerationPrefix: "base",
			StagingRoot:      "/target/seed/staging",
			TargetPVC: &antflyv1.HASeedArtifactPVCSpec{
				ClaimName: "standby-a-data",
				MountPath: "/target",
			},
		},
	}}

	(&AntflyClusterReconciler{}).updateHAStatusAndConditions(cluster)
	actions := cluster.Status.HAStatus.PlannedActions
	if len(actions) != 8 {
		t.Fatalf("expected create, begin, finish, publish, restore, activate artifact, activate slot, prune actions, got %#v", actions)
	}
	publish := actions[3]
	if publish.Kind != string(haActionPublishSeedArtifact) ||
		publish.DependsOn != string(haActionFinishStandbySeed) ||
		publish.Phase != string(haActionPhaseSeed) ||
		publish.Executor != string(haActionExecutorCLIJob) ||
		publish.AdminURL != "" ||
		publish.SeedArtifactGeneration != "base-standby-a-10" ||
		publish.SeedArtifactLocation != "s3://ha-seeds/cluster-a" ||
		!reflect.DeepEqual(publish.AdminCommand, []string{
			"artifact", "publish",
			"--location", "s3://ha-seeds/cluster-a",
			"--generation", "base-standby-a-10",
			"--slot", "standby-a",
			"--manifest", "/source/seed/manifest.afha",
			"--content-root", "/source/seed/content",
		}) {
		t.Fatalf("unexpected portable publish action: %#v", publish)
	}
	restore := actions[4]
	if restore.Kind != string(haActionRestoreSeedArtifact) ||
		restore.DependsOn != string(haActionPublishSeedArtifact) ||
		restore.Executor != string(haActionExecutorCLIJob) ||
		restore.AdminURL != "" ||
		restore.SeedContentRoot != "/target/seed/staging" ||
		!strings.Contains(strings.Join(restore.AdminCommand, " "), "--minimum-checkpoint-lsn 10") {
		t.Fatalf("unexpected portable restore action: %#v", restore)
	}
	activate := actions[5]
	if activate.Kind != "ActivateSeedArtifact" ||
		activate.DependsOn != string(haActionRestoreSeedArtifact) ||
		activate.Executor != string(haActionExecutorCLIJob) ||
		!reflect.DeepEqual(activate.AdminCommand, []string{
			"artifact", "activate",
			"--generation", "base-standby-a-10",
			"--slot", "standby-a",
			"--staging-root", "/target/seed/staging",
			"--target-root", "/target/.antfly-ha/active",
			"--ha-cluster-id", "100",
			"--ha-shard-id", "0",
			"--ha-table-id", "0",
			"--ha-timeline-id", "4",
			"--ha-epoch", "6",
			"--minimum-checkpoint-lsn", "10",
		}) {
		t.Fatalf("activation must durably publish the verified generation on the target PVC: %#v", activate)
	}
	activateSlot := actions[6]
	if activateSlot.Kind != "ActivateSeededSlot" ||
		activateSlot.DependsOn != "ActivateSeedArtifact" ||
		activateSlot.Executor != string(haActionExecutorAdminAPI) ||
		activateSlot.AdminURL != "http://primary-ha.default.svc:8081" ||
		activateSlot.AdminMethod != "POST" ||
		activateSlot.AdminPath != "/admin/v1/ha/base-backups/activate" {
		t.Fatalf("slot must remain non-streamable until durable target activation evidence exists: %#v", activateSlot)
	}
	prune := actions[7]
	if prune.Kind != string(haActionPruneSeedArtifacts) ||
		prune.DependsOn != "ActivateSeededSlot" ||
		prune.SeedArtifactRetainGenerations != 2 ||
		!reflect.DeepEqual(prune.AdminCommand, []string{
			"artifact", "prune",
			"--location", "s3://ha-seeds/cluster-a",
			"--generation", "base-standby-a-10",
			"--slot", "standby-a",
			"--retain-generations", "2",
		}) {
		t.Fatalf("unexpected post-bootstrap prune action: %#v", prune)
	}
}

func TestPlanHAPlansRuntimeOwnedCaptureAndActivationWithoutPrebuiltSeedFiles(t *testing.T) {
	initial := uint64(5)
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 9}
	cluster.Spec.HighAvailability.Admin = &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"}
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID: 100, TimelineID: 4, Epoch: 6, CurrentPrimaryID: "primary-a",
	}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name:       "standby-a",
		InitialLSN: &initial,
		AdminURL:   "http://standby-a-ha.default.svc:8081",
		SeedArtifact: &antflyv1.HASeedArtifactSpec{
			Location:         "s3://ha-seeds/cluster-a",
			GenerationPrefix: "base",
			StagingRoot:      "/target/.antfly-ha/staging",
			SourcePVC: &antflyv1.HASeedArtifactPVCSpec{
				ClaimName: "primary-a-data",
				MountPath: "/antflydb",
			},
			TargetPVC: &antflyv1.HASeedArtifactPVCSpec{
				ClaimName: "standby-a-data",
				MountPath: "/target",
			},
		},
	}}

	(&AntflyClusterReconciler{}).updateHAStatusAndConditions(cluster)
	actions := cluster.Status.HAStatus.PlannedActions
	wantKinds := []string{
		string(haActionCaptureSeedArtifact),
		string(haActionPublishSeedArtifact),
		string(haActionRestoreSeedArtifact),
		string(haActionActivateSeedArtifact),
		string(haActionActivateSeededSlot),
		string(haActionPruneSeedArtifacts),
	}
	gotKinds := make([]string, len(actions))
	for i := range actions {
		gotKinds[i] = actions[i].Kind
	}
	if !reflect.DeepEqual(gotKinds, wantKinds) {
		t.Fatalf("SEED_CAPTURE_MISSING: runtime-owned seed workflow requires %v without caller-provided manifest/content paths, got %v (%#v)", wantKinds, gotKinds, actions)
	}
	if actions[0].Executor != string(haActionExecutorAdminAPI) ||
		actions[0].AdminURL != "http://primary-ha.default.svc:8081" ||
		actions[0].AdminPath != "/admin/v1/ha/base-backups/capture" {
		t.Fatalf("SEED_CAPTURE_NOT_RUNTIME_OWNED: capture must execute atomically in the mounted primary runtime, got %#v", actions[0])
	}
	if actions[3].Executor != string(haActionExecutorCLIJob) ||
		actions[3].SeedArtifactGeneration != "base-standby-a-10" {
		t.Fatalf("TARGET_ACTIVATION_MISSING: activation must be a generation-bound target-PVC job, got %#v", actions[3])
	}
	digest := strings.Repeat("a", 64)
	cluster.Status.HAStatus.PlannedActions[0].AdminJobName = haAdminDirectAPIName
	cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	cluster.Status.HAStatus.PlannedActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion:          1,
		ActionID:               "seed_capture:base-standby-a-10",
		ActionKind:             "seed_capture",
		ActionTarget:           "base-standby-a-10",
		ActionState:            "applied",
		ActionNodeID:           "primary-a",
		SlotName:               "standby-a",
		ManifestID:             "base-standby-a-10",
		BackupLSN:              10,
		CheckpointLSN:          10,
		EndRecordLSN:           11,
		SeedArtifactGeneration: "base-standby-a-10",
		ManifestSHA256:         digest,
		SeedClusterID:          100,
		SeedTimelineID:         4,
		SeedEpoch:              6,
		SeedSourcePlanSHA256:   digest,
		SeedFileCount:          2,
		SeedTotalBytes:         20,
		SeedGenerationRoot:     "/antflydb/ha/seed-captures/generations/base-standby-a-10",
		SeedContentRoot:        "/antflydb/ha/seed-captures/generations/base-standby-a-10/content",
		SeedManifestPath:       "/antflydb/ha/seed-captures/generations/base-standby-a-10/manifest.afha",
		SeedAlreadyCaptured:    false,
	}
	// Keep the synthetic planner fixture in the missing-standby state across the
	// second reconcile; mergeConfiguredStandbys otherwise creates a placeholder
	// observation that correctly plans ResumeSlot instead.
	cluster.Status.HAStatus.Standbys = nil
	(&AntflyClusterReconciler{}).updateHAStatusAndConditions(cluster)
	publish := cluster.Status.HAStatus.PlannedActions[1]
	if publish.SeedManifestPath != "/antflydb/ha/seed-captures/generations/base-standby-a-10/manifest.afha" ||
		publish.SeedContentRoot != "/antflydb/ha/seed-captures/generations/base-standby-a-10/content" ||
		!reflect.DeepEqual(publish.AdminCommand, []string{
			"artifact", "publish",
			"--location", "s3://ha-seeds/cluster-a",
			"--generation", "base-standby-a-10",
			"--slot", "standby-a",
			"--manifest", "/antflydb/ha/seed-captures/generations/base-standby-a-10/manifest.afha",
			"--content-root", "/antflydb/ha/seed-captures/generations/base-standby-a-10/content",
		}) {
		t.Fatalf("CAPTURE_OUTPUT_NOT_PUBLISHED: publish must consume exact runtime capture paths, got %#v", publish)
	}
}

func TestPlanHAUsesExplicitExactSeedArtifactGenerationAcrossChangingLSN(t *testing.T) {
	standby := antflyv1.HAStandbySpec{
		Name: "standby-a",
		SeedArtifact: &antflyv1.HASeedArtifactSpec{
			Location: "s3://ha-seeds/cluster-a", Generation: "release-2026-07-14-a",
			StagingRoot: "/target/.antfly-ha/staging",
			SourcePVC:   &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
			TargetPVC:   &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
		},
	}
	first := haSeedCompletionActions(standby, "standby-a", 10, "seed", haActionCreateSlot)
	second := haSeedCompletionActions(standby, "standby-a", 999, "seed", haActionCreateSlot)
	if len(first) == 0 || len(second) == 0 {
		t.Fatalf("expected explicit-generation seed actions, got first=%#v second=%#v", first, second)
	}
	for _, actions := range [][]haPlannedAction{first, second} {
		for _, action := range actions {
			if action.Kind == haActionFinishStandbySeed {
				continue
			}
			if action.SeedArtifactGeneration != "release-2026-07-14-a" {
				t.Fatalf("EXACT_GENERATION_RECOMPUTED: action %s used %q", action.Kind, action.SeedArtifactGeneration)
			}
		}
	}
}

func TestPlanHAStandbyLocalTargetOnlyArtifactNeverCapturesOrPublishes(t *testing.T) {
	targetOnly := false
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 10}
	cluster.Spec.HighAvailability.Runtime = &antflyv1.HARuntimeSpec{
		Role:   antflyv1.HARuntimeRoleStandby,
		NodeID: "standby-a",
		Standby: &antflyv1.HAStandbyRuntimeSpec{
			UpstreamURL: "http://primary.default.svc:8080",
			SlotName:    "standby-a",
		},
		StartupGate: &antflyv1.HAStartupGateSpec{
			Policy:             antflyv1.HAStartupGatePolicyRequireActivatedSeed,
			ReceiptMatchPolicy: antflyv1.HAReceiptMatchPolicyExact,
			RequiredReceipt: &antflyv1.HARequiredSeedActivationReceipt{
				TopologyID: "antfly", TopologyGeneration: 7, NodeID: "standby-a", SlotName: "standby-a",
				Generation: "prod-standby-a-10", TargetPVCName: "standby-a-data",
			},
		},
	}
	standby := antflyv1.HAStandbySpec{
		Name:    "standby-a",
		Desired: &targetOnly,
		SeedArtifact: &antflyv1.HASeedArtifactSpec{
			Location:    "s3://ha-seeds/antfly",
			Generation:  "prod-standby-a-10",
			StagingRoot: "/target/.antfly-ha/staging",
			TargetPVC:   &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-a-data", MountPath: "/target"},
			SourcePVC:   nil,
		},
	}
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{standby}

	plan := planHA(cluster)
	if plan.DesiredStandbyCount != 0 || len(plan.Actions) != 0 {
		t.Fatalf("TARGET_ONLY_SEED_WAS_PLANNED: standby-local target descriptor must not enter primary seed planning, got %#v", plan)
	}
	if actions := haSeedCompletionActions(standby, "standby-a", 10, "must-not-run", ""); len(actions) != 0 {
		t.Fatalf("TARGET_ONLY_SEED_WAS_PUBLISHED: source-less target descriptor must not produce capture or publication actions, got %#v", actions)
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

func TestPlanHADoesNotPublishTypedAdminPathForInvalidSlotNames(t *testing.T) {
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
		!reflect.DeepEqual(actions[0].AdminCommand, []string{"slot", "resume", "--slot", "standby/a b%"}) {
		t.Fatalf("unexpected invalid slot action: %#v", actions[0])
	}
	if actions[0].AdminMethod != "" || actions[0].AdminPath != "" {
		t.Fatalf("invalid slot name should not publish typed admin operation, got method=%q path=%q", actions[0].AdminMethod, actions[0].AdminPath)
	}
	if haPlannedActionUsesTypedAdminAPI(actions[0]) {
		t.Fatalf("invalid slot action should not use typed admin API: %#v", actions[0])
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
			name:        "activate seeded slot",
			action:      haPlannedAction{Kind: haActionActivateSeededSlot, StandbyName: "standby-a", SlotName: "standby-a", SeedArtifactGeneration: "seed-standby-a-10"},
			openAPIPath: "/ha/base-backups/activate",
			operationID: "activateHASeededSlot",
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
		method      string
		path        string
		operationID string
	}{
		{method: "GET", path: haAdminPrimaryStatusPath, operationID: "getHAPrimaryStatus"},
		{method: "GET", path: haAdminStandbyStatusPath, operationID: "getHAStandbyStatus"},
		{method: "POST", path: haAdminCommitCheckPath, operationID: "checkHACommit"},
		{method: "POST", path: haAdminCommitAppendPath, operationID: "appendHACommit"},
		{method: "POST", path: haAdminReadCheckPath, operationID: "checkHARead"},
		{method: "POST", path: haAdminWriteCheckPath, operationID: "checkHAWrite"},
		{method: "POST", path: haAdminOwnerJobCheckPath, operationID: "checkHAOwnerJob"},
		{method: "GET", path: haAdminReplicationSlotsPath, operationID: "listHAReplicationSlots"},
		{method: "POST", path: haAdminReplicationSlotsPath, operationID: "createHAReplicationSlot"},
		{method: "DELETE", path: haAdminReplicationSlotPathPrefix + "{slot_name}", operationID: "dropHAReplicationSlot"},
		{method: "PUT", path: haAdminReplicationSlotPathPrefix + "{slot_name}" + haAdminReplicationSlotPausePathSuffix, operationID: "pauseHAReplicationSlot"},
		{method: "PUT", path: haAdminReplicationSlotPathPrefix + "{slot_name}" + haAdminReplicationSlotResumePathSuffix, operationID: "resumeHAReplicationSlot"},
		{method: "POST", path: haAdminBaseBackupsPath, operationID: "beginHABaseBackup"},
		{method: "POST", path: haAdminBaseBackupsFinishPath, operationID: "finishHABaseBackup"},
		{method: "POST", path: haAdminBaseBackupsCapturePath, operationID: "captureHASeedArtifact"},
		{method: "POST", path: haAdminBaseBackupsActivatePath, operationID: "activateHASeededSlot"},
		{method: "POST", path: haAdminStandbyBootstrapPath, operationID: "bootstrapHAStandby"},
		{method: "POST", path: haAdminFencePath, operationID: "acquireHAFence"},
		{method: "GET", path: haAdminFenceCurrentPath, operationID: "getHACurrentFence"},
		{method: "POST", path: haAdminPromotionAssessPath, operationID: "assessHAPromotion"},
		{method: "POST", path: haAdminPromotionCurrentFencePath, operationID: "promoteHAWithCurrentFence"},
		{method: "POST", path: haAdminPromotionPath, operationID: "promoteHA"},
		{method: "POST", path: haAdminRejoinAssessPath, operationID: "assessHARejoin"},
		{method: "POST", path: haAdminRejoinRewindPath, operationID: "rewindHARejoin"},
		{method: "POST", path: haAdminRejoinReseedPath, operationID: "reseedHARejoin"},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			path := strings.TrimPrefix(route.path, haAdminBasePath)
			key := route.method + " " + path
			if operations[key] == "" {
				t.Fatalf("operator HA admin route %s is missing from specs/openapi/antfly/admin.yaml", key)
			}
			if operations[key] != route.operationID {
				t.Fatalf("operator HA admin route %s resolved to operationId %q, want %q", key, operations[key], route.operationID)
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

func TestHAAdminOpenAPIContractIsNotDuplicatedInOtherSpecs(t *testing.T) {
	adminOperations := loadAdminOpenAPIOperations(t)
	adminOperationIDs := map[string]bool{}
	for _, operationID := range adminOperations {
		adminOperationIDs[operationID] = true
	}

	_, adminSpecPath := readAdminOpenAPISpec(t)
	specDir := filepath.Dir(adminSpecPath)
	entries, err := os.ReadDir(specDir)
	if err != nil {
		t.Fatalf("read OpenAPI spec dir %s: %v", specDir, err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == "admin.yaml" || (!strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml")) {
			continue
		}

		specPath := filepath.Join(specDir, name)
		raw, err := os.ReadFile(specPath)
		if err != nil {
			t.Fatalf("read OpenAPI spec %s: %v", specPath, err)
		}
		var spec struct {
			Paths map[string]map[string]any `json:"paths"`
		}
		if err := yaml.Unmarshal(raw, &spec); err != nil {
			t.Fatalf("parse OpenAPI spec %s: %v", specPath, err)
		}

		for path, pathItem := range spec.Paths {
			for method, operationValue := range pathItem {
				operation, ok := operationValue.(map[string]any)
				if !ok {
					continue
				}
				operationID, _ := operation["operationId"].(string)
				if adminOperationIDs[operationID] {
					t.Errorf("%s duplicates admin OpenAPI operationId %q at %s %s", specPath, operationID, strings.ToUpper(method), path)
				}
				if strings.HasPrefix(path, "/ha/") && (name != "internal.yaml" || !strings.HasPrefix(path, "/ha/replication/")) {
					t.Errorf("%s defines HA operator/control-plane path %s %s outside specs/openapi/antfly/admin.yaml", specPath, strings.ToUpper(method), path)
				}
			}
		}
	}
}

func TestAdminOpenAPISpecDocumentsBearerAuth(t *testing.T) {
	raw, specPath := readAdminOpenAPISpec(t)
	var spec struct {
		Security []map[string][]string `json:"security"`
		Paths    map[string]map[string]struct {
			Responses map[string]struct {
				Ref string `json:"$ref"`
			} `json:"responses"`
		} `json:"paths"`
		Components struct {
			SecuritySchemes map[string]struct {
				Type   string `json:"type"`
				Scheme string `json:"scheme"`
			} `json:"securitySchemes"`
		} `json:"components"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse admin OpenAPI spec %s: %v", specPath, err)
	}

	bearer, ok := spec.Components.SecuritySchemes["BearerAuth"]
	if !ok {
		t.Fatalf("admin OpenAPI spec must define components.securitySchemes.BearerAuth")
	}
	if bearer.Type != "http" || bearer.Scheme != "bearer" {
		t.Fatalf("BearerAuth scheme = %#v, want http bearer", bearer)
	}
	for _, requirement := range spec.Security {
		if _, ok := requirement["BearerAuth"]; ok {
			for path, methods := range spec.Paths {
				if !strings.HasPrefix(path, "/ha/") {
					continue
				}
				for method, operation := range methods {
					unauthorized, ok := operation.Responses["401"]
					if !ok {
						t.Fatalf("admin OpenAPI HA route %s %s must document 401 Unauthorized", strings.ToUpper(method), path)
					}
					if unauthorized.Ref != "#/components/responses/Unauthorized" {
						t.Fatalf("admin OpenAPI HA route %s %s 401 response = %q, want Unauthorized ref", strings.ToUpper(method), path, unauthorized.Ref)
					}
				}
			}
			return
		}
	}
	t.Fatalf("admin OpenAPI spec must apply BearerAuth at the top level")
}

func TestOperatorUsesAdminSDKWrapperOnly(t *testing.T) {
	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("failed to locate test file")
	}
	operatorRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	forbiddenPrefix := "github.com/antflydb/antfly/go/pkg/sdk/admin/oapi"
	fset := token.NewFileSet()

	if err := filepath.WalkDir(operatorRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, importSpec := range parsed.Imports {
			importPath, err := strconv.Unquote(importSpec.Path.Value)
			if err != nil {
				return err
			}
			if importPath == forbiddenPrefix || strings.HasPrefix(importPath, forbiddenPrefix+"/") {
				t.Errorf("%s imports generated admin OpenAPI internals directly; use go/pkg/sdk/admin wrapper instead", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("scan operator imports: %v", err)
	}
}

func TestOperatorCRDExposesHAAdminRuntimeFields(t *testing.T) {
	crd := readOperatorCRD(t)
	schema := crdVersionSchema(t, crd, "v1")

	ha := crdSchemaProperty(t, schema, "spec", "highAvailability")
	haProperties := crdSchemaProperties(t, ha)
	for _, field := range []string{"admin", "automaticFailover", "identity", "retention", "runtime", "standbys", "syncPolicy"} {
		if _, ok := haProperties[field]; !ok {
			t.Fatalf("operator CRD spec.highAvailability is missing %q", field)
		}
	}

	admin := crdSchemaProperty(t, ha, "admin")
	adminProperties := crdSchemaProperties(t, admin)
	for _, field := range []string{"primaryURL", "executePlannedActions", "tokenEnvVar", "jobBackoffLimit", "volumes", "volumeMounts"} {
		if _, ok := adminProperties[field]; !ok {
			t.Fatalf("operator CRD spec.highAvailability.admin is missing %q", field)
		}
	}

	runtime := crdSchemaProperty(t, ha, "runtime")
	runtimeProperties := crdSchemaProperties(t, runtime)
	for _, field := range []string{"role", "nodeID", "fencePath", "formerPrimaryLogPath", "adminTokenEnvVar", "adminTokenSecretRef", "primary", "standby"} {
		if _, ok := runtimeProperties[field]; !ok {
			t.Fatalf("operator CRD spec.highAvailability.runtime is missing %q", field)
		}
	}

	primary := crdSchemaProperty(t, runtime, "primary")
	primaryProperties := crdSchemaProperties(t, primary)
	for _, field := range []string{"logPath", "slotsPath"} {
		if _, ok := primaryProperties[field]; !ok {
			t.Fatalf("operator CRD spec.highAvailability.runtime.primary is missing %q", field)
		}
	}

	standby := crdSchemaProperty(t, runtime, "standby")
	standbyProperties := crdSchemaProperties(t, standby)
	for _, field := range []string{"logPath", "progressPath", "slotName", "upstreamURL"} {
		if _, ok := standbyProperties[field]; !ok {
			t.Fatalf("operator CRD spec.highAvailability.runtime.standby is missing %q", field)
		}
	}

	failover := crdSchemaProperty(t, ha, "automaticFailover")
	failoverProperties := crdSchemaProperties(t, failover)
	for _, field := range []string{"enabled", "fencingAuthority", "maximumLagLSN", "requireRemoteApply"} {
		if _, ok := failoverProperties[field]; !ok {
			t.Fatalf("operator CRD spec.highAvailability.automaticFailover is missing %q", field)
		}
	}
}

func TestOperatorProductionDoesNotHardCodeHAAdminPaths(t *testing.T) {
	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("failed to locate test file")
	}
	operatorRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	fset := token.NewFileSet()

	if err := filepath.WalkDir(operatorRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			lit, ok := node.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Errorf("%s: unquote string literal %s: %v", path, lit.Value, err)
				return true
			}
			if strings.Contains(value, "/admin/v1/ha") {
				t.Errorf("%s hard-codes HA admin path %q; use go/pkg/sdk/admin wrapper constants instead", path, value)
			}
			return true
		})
		return nil
	}); err != nil {
		t.Fatalf("scan operator production sources: %v", err)
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

			status := antflyv1.HAPlannedActionStatus{
				Kind:     string(action.Kind),
				Executor: string(haActionExecutorAdminAPI),
			}
			if err := haValidatePlannedActionAdminOperation(status); err == nil {
				t.Fatalf("direct admin action %s executed without published typed method/path", action.Kind)
			}
			if haPlannedActionHasDirectAdminOperation(status) {
				t.Fatalf("direct admin action %s reported executable without published typed method/path", action.Kind)
			}

			status.StandbyName = action.StandbyName
			status.SlotName = action.SlotName
			status.SeedManifestPath = action.SeedManifestPath
			status.AdminMethod = method
			status.AdminPath = path
			if err := haValidatePlannedActionAdminOperation(status); err != nil {
				t.Fatalf("direct admin action %s rejected matching typed operation: %v", action.Kind, err)
			}
			if !haPlannedActionHasDirectAdminOperation(status) {
				t.Fatalf("direct admin action %s did not report executable with published typed method/path", action.Kind)
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
		name:   "former primary reseed marks slot on original primary before promotion is recorded",
		action: haPlannedAction{Kind: haActionReseedFormerPrimary, StandbyName: "old-primary"},
		want:   "http://primary-ha.default.svc:8081",
	}, {
		name:   "post-promotion former primary reseed marks slot on promoted primary",
		action: haPlannedAction{Kind: haActionReseedFormerPrimary, StandbyName: "old-primary"},
		status: &antflyv1.HAStatus{LastPromotion: haCompletePromotionReceipt("primary-a", "standby-a")},
		want:   "http://standby-a-ha.default.svc:8081",
	}, {
		name:   "post-promotion primary scoped seed uses promoted primary node",
		action: haPlannedAction{Kind: haActionSeedStandby, StandbyName: "old-primary"},
		status: &antflyv1.HAStatus{LastPromotion: haCompletePromotionReceipt("primary-a", "standby-a")},
		want:   "http://standby-a-ha.default.svc:8081",
	}, {
		name:   "incomplete promotion does not retarget primary scoped action",
		action: haPlannedAction{Kind: haActionSeedStandby, StandbyName: "old-primary"},
		status: &antflyv1.HAStatus{LastPromotion: &antflyv1.HAPromotionStatus{PromotedStandbyID: "standby-a"}},
		want:   "http://primary-ha.default.svc:8081",
	}, {
		name:   "post-promotion primary scoped action missing promoted node url fails closed",
		action: haPlannedAction{Kind: haActionSeedStandby, StandbyName: "old-primary"},
		status: &antflyv1.HAStatus{LastPromotion: haCompletePromotionReceipt("primary-a", "standby-missing")},
		want:   "",
	}, {
		name:   "former primary rewind without node url",
		action: haPlannedAction{Kind: haActionRewindFormerPrimary, StandbyName: "missing-old-primary"},
		want:   "",
	}, {
		name:   "automatic former primary demote uses explicit route source primary url",
		action: haPlannedAction{Kind: haActionDemoteFormerPrimary, StandbyName: "missing-old-primary", RouteFrom: "missing-old-primary"},
		want:   "http://primary-ha.default.svc:8081",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := haAdminURL(tt.action, ha, tt.status); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestHAFormerPrimaryPlannedActionKeepsOriginalPrimaryAdminRoute(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Standbys: []antflyv1.HAStandbySpec{{
			Name:     "standby-a",
			AdminURL: "http://standby-a-ha.default.svc:8081",
		}},
	}
	promotion := haCompletePromotionReceipt("primary-a", "standby-a")
	promotion.RequiredLSN = 11
	promotion.ObservedLSN = 12
	promotion.SwitchLSN = 13
	status := &antflyv1.HAStatus{LastPromotion: promotion}
	action := haFormerPrimaryPlannedAction(haFormerPrimaryEvaluation{
		Present:          true,
		NodeID:           "primary-a",
		RejoinRequired:   true,
		ParentTimelineID: 1,
		NewTimelineID:    2,
		SwitchLSN:        13,
		FenceAuthority:   antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:      "standby-a",
		FenceGeneration:  3,
		Action:           string(haActionDemoteFormerPrimary),
		Reason:           "FormerPrimaryNotObserved",
	}, status)
	planned := haPlannedActionStatuses([]haPlannedAction{action}, ha, status)

	if action.RouteFrom != "primary-a" ||
		action.TargetLSN != 12 ||
		len(planned) != 1 ||
		planned[0].AdminURL != "http://primary-ha.default.svc:8081" ||
		planned[0].AdminNodeID != "primary-a" {
		t.Fatalf("expected former-primary action to retain its original primary admin route, got action=%#v planned=%#v", action, planned)
	}
}

func TestHAFormerPrimaryFencePrecedesRejoinAndTargetsOldPrimary(t *testing.T) {
	ha := &antflyv1.HighAvailabilitySpec{
		Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       4,
			Epoch:            6,
			CurrentPrimaryID: "primary-a",
		},
		Standbys: []antflyv1.HAStandbySpec{{
			Name:     "standby-a",
			AdminURL: "http://standby-a-ha.default.svc:8081",
		}},
	}
	promotion := haCompletePromotionReceipt("primary-a", "standby-a")
	promotion.RequiredLSN = 11
	promotion.ObservedLSN = 12
	promotion.SwitchLSN = 13
	status := &antflyv1.HAStatus{LastPromotion: promotion}
	fence := haFormerPrimaryFencePlannedAction(status)
	rejoin := haFormerPrimaryPlannedAction(haFormerPrimaryEvaluation{
		Present:          true,
		NodeID:           "primary-a",
		RejoinRequired:   true,
		ParentTimelineID: 4,
		NewTimelineID:    5,
		SwitchLSN:        13,
		FenceAuthority:   antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:      "standby-a",
		FenceGeneration:  5,
		Action:           string(haActionDemoteFormerPrimary),
		Reason:           "FormerPrimaryNotObserved",
	}, status)
	rejoin.DependsOn = haActionFenceFormerPrimary
	planned := haPlannedActionStatuses([]haPlannedAction{fence, rejoin}, ha, status)

	if len(planned) != 2 {
		t.Fatalf("expected durable fence followed by rejoin, got %#v", planned)
	}
	if planned[0].Kind != string(haActionFenceFormerPrimary) ||
		planned[0].StandbyName != "primary-a" ||
		planned[0].RouteTo != "standby-a" ||
		planned[0].TargetLSN != 12 ||
		planned[0].AdminURL != "http://primary-ha.default.svc:8081" ||
		planned[0].AdminNodeID != "primary-a" ||
		planned[0].AdminMethod != http.MethodPost ||
		planned[0].AdminPath != "/admin/v1/ha/fence" {
		t.Fatalf("expected fence action to execute against the original primary with promotion evidence, got %#v", planned[0])
	}
	if planned[1].DependsOn != string(haActionFenceFormerPrimary) {
		t.Fatalf("expected rejoin to wait for former-primary durable fencing, got %#v", planned[1])
	}
}

func TestHAFormerPrimaryFenceRequiresOldNodeDurableObservation(t *testing.T) {
	promotion := &antflyv1.HAPromotionStatus{
		OldPrimaryID:      "primary-a",
		PromotedStandbyID: "standby-a",
		ParentTimelineID:  1,
		ParentEpoch:       1,
		NewTimelineID:     2,
		NewEpoch:          2,
		RequiredLSN:       10,
		ObservedLSN:       10,
		SwitchLSN:         11,
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration:   3,
		FenceToken:        "token",
	}
	status := &antflyv1.HAStatus{
		LastPromotion: promotion,
		Fencing: antflyv1.HAFencingStatus{
			Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
			Ready:      true,
			Holder:     "standby-a",
			Generation: 3,
		},
	}

	if haFormerPrimaryFenced(status, promotion) {
		t.Fatal("promotion and Lease receipts must not impersonate a durable fence on the old node")
	}
	status.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{
		NodeID:           "primary-a",
		Fenced:           true,
		FenceAuthority:   antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:      "standby-a",
		FenceGeneration:  3,
		TargetTimelineID: 2,
		TargetEpoch:      2,
	}
	if !haFormerPrimaryFenced(status, promotion) {
		t.Fatal("matching durable old-node fence observation should permit rejoin")
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
		LastPromotion: haCompletePromotionReceipt("old-primary", "standby-a"),
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

func TestExecuteHAPlannedActionTypedUsesAdminSDKForReplicationSlotCreate(t *testing.T) {
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")

	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != haAdminReplicationSlotsPath {
			t.Errorf("expected %s, got %s", haAdminReplicationSlotsPath, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer operator-token" {
			t.Errorf("expected bearer token header, got %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": uint32(1),
			"action": map[string]any{
				"action_id":   "replication_slot_create:standby-a",
				"action_kind": "replication_slot_create",
				"target":      "standby-a",
				"state":       "applied",
				"node_id":     "primary-a",
			},
			"slot_action": "create",
			"slot": map[string]any{
				"slot_name":       "standby-a",
				"timeline_id":     uint64(4),
				"restart_lsn":     uint64(12),
				"received_lsn":    uint64(12),
				"applied_lsn":     uint64(12),
				"safe_read_lsn":   uint64(12),
				"active":          true,
				"reseed_required": false,
				"current_lsn":     uint64(12),
			},
		})
	}))
	defer server.Close()

	cluster := haCluster()
	action := antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionCreateSlot),
		Executor:    string(haActionExecutorAdminAPI),
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		TargetLSN:   12,
		AdminURL:    server.URL,
		AdminNodeID: "primary-a",
		AdminMethod: http.MethodPost,
		AdminPath:   haAdminReplicationSlotsPath,
	}

	handled, err := (&AntflyClusterReconciler{}).executeHAPlannedActionTyped(context.Background(), cluster, &action)
	if err != nil {
		t.Fatalf("executeHAPlannedActionTyped returned error: %v", err)
	}
	if !handled {
		t.Fatal("expected direct admin action to be handled")
	}
	if requestBody["slot_name"] != "standby-a" || requestBody["initial_lsn"] != float64(12) {
		t.Fatalf("unexpected slot create request body: %#v", requestBody)
	}
	if action.AdminResult == nil {
		t.Fatal("expected typed admin result evidence")
	}
	if action.AdminResult.ActionNodeID != "primary-a" ||
		action.AdminResult.ActionKind != "replication_slot_create" ||
		action.AdminResult.ActionState != "applied" ||
		action.AdminResult.SlotName != "standby-a" ||
		action.AdminResult.SlotAction != "create" {
		t.Fatalf("unexpected admin result: %#v", action.AdminResult)
	}
}

func TestExecuteHAPlannedActionTypedRejectsInvalidInputsBeforeHTTP(t *testing.T) {
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")

	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	base := func(kind haActionKind) antflyv1.HAPlannedActionStatus {
		method, path := haAdminOperation(haPlannedAction{
			Kind:             kind,
			StandbyName:      "standby-a",
			SlotName:         "standby-a",
			SeedManifestPath: "/backup/base-standby-a-5.afha",
		})
		return antflyv1.HAPlannedActionStatus{
			Kind:             string(kind),
			Executor:         string(haActionExecutorAdminAPI),
			StandbyName:      "standby-a",
			SlotName:         "standby-a",
			SeedManifestPath: "/backup/base-standby-a-5.afha",
			SeedContentRoot:  "/backup/base-standby-a-5",
			TargetLSN:        5,
			ObservedLSN:      5,
			RetainedFromLSN:  1,
			AdminURL:         server.URL,
			AdminNodeID:      "primary-a",
			AdminMethod:      method,
			AdminPath:        path,
		}
	}

	tests := []struct {
		name   string
		action antflyv1.HAPlannedActionStatus
	}{{
		name: "create slot padded slot name",
		action: func() antflyv1.HAPlannedActionStatus {
			action := base(haActionCreateSlot)
			action.SlotName = " standby-a"
			return action
		}(),
	}, {
		name: "finish seed padded manifest path",
		action: func() antflyv1.HAPlannedActionStatus {
			action := base(haActionFinishStandbySeed)
			action.SeedManifestPath = " /backup/base-standby-a-5.afha"
			return action
		}(),
	}, {
		name: "bootstrap seed padded content root",
		action: func() antflyv1.HAPlannedActionStatus {
			action := base(haActionBootstrapStandbySeed)
			action.SeedContentRoot = "/backup/base-standby-a-5 "
			return action
		}(),
	}, {
		name: "acquire fence padded promoted standby",
		action: func() antflyv1.HAPlannedActionStatus {
			action := base(haActionAcquireFence)
			action.StandbyName = "standby-a "
			action.AdminNodeID = "primary-a"
			return action
		}(),
	}, {
		name: "rejoin padded former primary",
		action: func() antflyv1.HAPlannedActionStatus {
			action := base(haActionDemoteFormerPrimary)
			action.StandbyName = "primary-a "
			action.AdminNodeID = "primary-a"
			return action
		}(),
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := tt.action
			handled, err := (&AntflyClusterReconciler{}).executeHAPlannedActionTyped(context.Background(), haClusterWithAutomaticKubernetesLeaseFailover(), &action)
			if !handled {
				t.Fatal("expected typed admin action to be handled")
			}
			if err == nil || !strings.Contains(err.Error(), "invalid HA") {
				t.Fatalf("executeHAPlannedActionTyped error = %v, want local invalid HA input error", err)
			}
			if action.AdminResult != nil {
				t.Fatalf("unexpected admin result for invalid input: %#v", action.AdminResult)
			}
		})
	}
	if requests != 0 {
		t.Fatalf("server received %d requests for invalid typed HA inputs", requests)
	}
}

func TestExecuteHAPlannedActionTypedRequiresExpectedAdminNodeEvidence(t *testing.T) {
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")

	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": uint32(1),
			"action": map[string]any{
				"action_id":   "replication_slot_create:standby-a",
				"action_kind": "replication_slot_create",
				"target":      "standby-a",
				"state":       "applied",
				"node_id":     "standby-a",
			},
			"slot_action": "create",
			"slot": map[string]any{
				"slot_name":       "standby-a",
				"timeline_id":     uint64(4),
				"restart_lsn":     uint64(12),
				"received_lsn":    uint64(12),
				"applied_lsn":     uint64(12),
				"safe_read_lsn":   uint64(12),
				"active":          true,
				"reseed_required": false,
				"current_lsn":     uint64(12),
			},
		})
	}))
	defer server.Close()

	baseAction := antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionCreateSlot),
		Executor:    string(haActionExecutorAdminAPI),
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		TargetLSN:   12,
		AdminURL:    server.URL,
		AdminMethod: http.MethodPost,
		AdminPath:   haAdminReplicationSlotsPath,
	}

	missingNode := baseAction
	handled, err := (&AntflyClusterReconciler{}).executeHAPlannedActionTyped(context.Background(), haCluster(), &missingNode)
	if err == nil || !strings.Contains(err.Error(), "requires adminNodeID") {
		t.Fatalf("expected missing adminNodeID error, got %v", err)
	}
	if !handled {
		t.Fatal("expected direct admin action with missing adminNodeID to be handled as an error")
	}
	if called {
		t.Fatal("expected missing adminNodeID to fail before making an HTTP request")
	}
	if missingNode.AdminResult != nil {
		t.Fatalf("expected missing adminNodeID to leave no admin result, got %#v", missingNode.AdminResult)
	}

	wrongNode := baseAction
	wrongNode.AdminNodeID = "primary-a"
	handled, err = (&AntflyClusterReconciler{}).executeHAPlannedActionTyped(context.Background(), haCluster(), &wrongNode)
	if err == nil || !strings.Contains(err.Error(), "typed result evidence") {
		t.Fatalf("expected wrong-node receipt error, got %v", err)
	}
	if !handled {
		t.Fatal("expected wrong-node direct admin action to be handled as an error")
	}
	if wrongNode.AdminResult != nil {
		t.Fatalf("expected wrong-node receipt to be discarded, got %#v", wrongNode.AdminResult)
	}
}

func TestExecuteHAPlannedActionTypedAppliesAdminSDKPromotionReceipt(t *testing.T) {
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != haAdminPromotionCurrentFencePath {
			t.Errorf("expected %s, got %s", haAdminPromotionCurrentFencePath, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer operator-token" {
			t.Errorf("expected bearer token header, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": uint32(1),
			"action": map[string]any{
				"action_id":   "promotion:standby-a",
				"action_kind": "promotion",
				"target":      "standby-a",
				"state":       "applied",
				"node_id":     "standby-a",
			},
			"assessment": map[string]any{
				"required_lsn":          uint64(12),
				"received_lsn":          uint64(12),
				"applied_lsn":           uint64(12),
				"has_required_lsn":      true,
				"caught_up_to_received": true,
				"fencing_confirmed":     true,
				"force":                 false,
				"mode":                  "safe",
				"data_loss_possible":    false,
				"safe":                  true,
				"requires_fencing":      false,
				"requires_force":        false,
				"can_promote":           true,
			},
			"promotion": map[string]any{
				"node_id": "standby-a",
				"old_identity": map[string]any{
					"cluster_id":  uint64(100),
					"shard_id":    uint64(10),
					"table_id":    uint64(20),
					"timeline_id": uint64(4),
					"epoch":       uint64(6),
				},
				"new_identity": map[string]any{
					"cluster_id":  uint64(100),
					"shard_id":    uint64(10),
					"table_id":    uint64(20),
					"timeline_id": uint64(5),
					"epoch":       uint64(7),
				},
				"switch_lsn":         uint64(13),
				"forced":             false,
				"data_loss_possible": false,
			},
			"fence_generation": uint64(3),
			"fence_token":      "ha-fence-token",
			"forced":           false,
		})
	}))
	defer server.Close()

	cluster := haCluster()
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{}
	action := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionPromoteStandby),
		Executor:        string(haActionExecutorAdminAPI),
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 3,
		FenceReason:     "LeaseAcquired",
		AdminURL:        server.URL,
		AdminNodeID:     "standby-a",
		AdminMethod:     http.MethodPost,
		AdminPath:       haAdminPromotionCurrentFencePath,
	}

	handled, err := (&AntflyClusterReconciler{}).executeHAPlannedActionTyped(context.Background(), cluster, &action)
	if err != nil {
		t.Fatalf("executeHAPlannedActionTyped returned error: %v", err)
	}
	if !handled {
		t.Fatal("expected direct admin promotion action to be handled")
	}
	if action.AdminResult == nil {
		t.Fatal("expected typed promotion result evidence")
	}
	if action.AdminResult.ActionNodeID != "standby-a" ||
		action.AdminResult.ActionKind != "promotion" ||
		action.AdminResult.FenceGeneration != 3 ||
		action.AdminResult.FenceToken != "ha-fence-token" ||
		action.AdminResult.FenceNewTimelineID != 5 ||
		action.AdminResult.FenceObservedLSN != 12 {
		t.Fatalf("unexpected promotion admin result: %#v", action.AdminResult)
	}
	if cluster.Status.HAStatus.LastPromotion == nil {
		t.Fatal("expected promotion receipt to update HA status")
	}
	promotion := cluster.Status.HAStatus.LastPromotion
	if promotion.OldPrimaryID != "primary-a" ||
		promotion.PromotedStandbyID != "standby-a" ||
		promotion.ParentTimelineID != 4 ||
		promotion.NewTimelineID != 5 ||
		promotion.SwitchLSN != 13 ||
		promotion.FenceGeneration != 3 ||
		promotion.FenceToken != "ha-fence-token" {
		t.Fatalf("unexpected HA promotion status: %#v", promotion)
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
	raw, specPath := readAdminOpenAPISpec(t)
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

func readAdminOpenAPISpec(t *testing.T) ([]byte, string) {
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
	return raw, specPath
}

func readOperatorCRD(t *testing.T) map[string]any {
	t.Helper()
	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	crdPath := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "manifests", "crd", "antfly.io_antflyclusters.yaml"))
	raw, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read operator CRD %s: %v", crdPath, err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("parse operator CRD %s: %v", crdPath, err)
	}
	return crd
}

func crdVersionSchema(t *testing.T, crd map[string]any, versionName string) map[string]any {
	t.Helper()
	spec := crdMap(t, crd["spec"], "spec")
	versions := crdSlice(t, spec["versions"], "spec.versions")
	for _, versionValue := range versions {
		version := crdMap(t, versionValue, "spec.versions[]")
		if name, _ := version["name"].(string); name == versionName {
			return crdMap(t, crdMap(t, version["schema"], "schema")["openAPIV3Schema"], "openAPIV3Schema")
		}
	}
	t.Fatalf("operator CRD is missing version %q", versionName)
	return nil
}

func crdSchemaProperty(t *testing.T, schema map[string]any, path ...string) map[string]any {
	t.Helper()
	current := schema
	fullPath := "schema"
	for _, name := range path {
		properties := crdSchemaProperties(t, current)
		fullPath += ".properties." + name
		next, ok := properties[name]
		if !ok {
			t.Fatalf("operator CRD is missing %s", fullPath)
		}
		current = crdMap(t, next, fullPath)
	}
	return current
}

func crdSchemaProperties(t *testing.T, schema map[string]any) map[string]any {
	t.Helper()
	return crdMap(t, schema["properties"], "properties")
}

func crdMap(t *testing.T, value any, name string) map[string]any {
	t.Helper()
	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("operator CRD %s has type %T, want object", name, value)
	}
	return m
}

func crdSlice(t *testing.T, value any, name string) []any {
	t.Helper()
	s, ok := value.([]any)
	if !ok {
		t.Fatalf("operator CRD %s has type %T, want array", name, value)
	}
	return s
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

func TestUpdateHAStatusReportsPrimaryAdminUnauthorized(t *testing.T) {
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN:                10,
		PrimaryAdminReachable:     false,
		PrimaryAdminLastError:     "get HA primary status returned status 401: missing bearer token",
		PrimaryAdminStatusCode:    http.StatusUnauthorized,
		ReadSafeStandbyCount:      1,
		HealthyStandbyCount:       1,
		DesiredStandbyCount:       1,
		UnhealthyStandbyCount:     0,
		LaggingStandbyCount:       0,
		ReseedRequiredCount:       0,
		AutomaticPromotionAllowed: false,
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
		degraded.Reason != antflyv1.ReasonHAAdminUnauthorized ||
		!strings.Contains(degraded.Message, "status 401") {
		t.Fatalf("expected unauthorized HA admin degraded condition, got %#v", degraded)
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
	cluster.Status.HAStatus.PrimaryAdminLastError = "get HA primary status returned status 401: missing bearer token"
	cluster.Status.HAStatus.PrimaryAdminStatusCode = http.StatusUnauthorized
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked when primary admin observation is unauthorized")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHAPrimaryStillReachable {
		t.Fatalf("expected primary-still-reachable condition for unauthorized primary admin observation, got %#v", failover)
	}

	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin connection refused"
	cluster.Status.HAStatus.PrimaryAdminStatusCode = 0
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
	if len(plan.Actions) != 6 {
		t.Fatalf("expected fenced promotion action chain, got %#v", plan.Actions)
	}
	if plan.PromotionStandbyName != "standby-a" {
		t.Fatalf("expected promotion standby standby-a, got %q", plan.PromotionStandbyName)
	}
	if plan.Actions[0].Kind != haActionFenceFormerPrimary ||
		plan.Actions[1].Kind != haActionAcquireFence ||
		plan.Actions[2].Kind != haActionAssessPromotion ||
		plan.Actions[3].Kind != haActionPromoteStandby {
		t.Fatalf("unexpected promotion actions: %#v", plan.Actions)
	}
	if plan.Actions[0].StandbyName != "primary-a" ||
		plan.Actions[0].RouteTo != "standby-a" ||
		plan.Actions[1].StandbyName != "standby-a" ||
		plan.Actions[2].StandbyName != "standby-a" ||
		plan.Actions[3].StandbyName != "standby-a" ||
		plan.Actions[4].RouteTo != "standby-a" {
		t.Fatalf("expected promotion and route actions to target standby-a, got %#v", plan.Actions)
	}

	cluster.Status.HAStatus.Standbys[0].LastError = "standby admin timeout"
	cluster.Status.HAStatus.Standbys[0].CanServeSafeReads = false
	reconciler.updateHAStatusAndConditions(cluster)
	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to be blocked while standby admin status is failed")
	}
	failover = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHAAutomaticFailoverReady)
	if failover == nil || failover.Status != metav1.ConditionFalse || failover.Reason != antflyv1.ReasonHASyncPolicyUnsatisfied {
		t.Fatalf("expected sync-policy-unsatisfied condition after standby admin failure, got %#v", failover)
	}

	cluster.Status.HAStatus.Standbys[0].LastError = ""
	cluster.Status.HAStatus.Standbys[0].CanServeSafeReads = true
	reconciler.updateHAStatusAndConditions(cluster)
	if len(cluster.Status.HAStatus.PlannedActions) != 6 {
		t.Fatalf("expected fenced promotion action chain in status, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].Kind != string(haActionFenceFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[1].Kind != string(haActionAcquireFence) ||
		cluster.Status.HAStatus.PlannedActions[2].Kind != string(haActionAssessPromotion) ||
		cluster.Status.HAStatus.PlannedActions[3].Kind != string(haActionPromoteStandby) {
		t.Fatalf("unexpected promotion action status: %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].Phase != string(haActionPhaseFence) ||
		cluster.Status.HAStatus.PlannedActions[1].Phase != string(haActionPhaseFence) ||
		cluster.Status.HAStatus.PlannedActions[2].Phase != string(haActionPhasePromote) ||
		cluster.Status.HAStatus.PlannedActions[3].Phase != string(haActionPhasePromote) ||
		cluster.Status.HAStatus.PlannedActions[4].Phase != string(haActionPhaseRoute) ||
		cluster.Status.HAStatus.PlannedActions[5].Phase != string(haActionPhaseRejoin) ||
		cluster.Status.HAStatus.PlannedActions[0].Executor != string(haActionExecutorAdminAPI) ||
		cluster.Status.HAStatus.PlannedActions[4].Executor != string(haActionExecutorControllerAction) {
		t.Fatalf("expected promotion action status to publish phase/executor metadata, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if cluster.Status.HAStatus.PlannedActions[0].StandbyName != "primary-a" ||
		cluster.Status.HAStatus.PlannedActions[0].RouteTo != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[1].StandbyName != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[1].DependsOn != string(haActionFenceFormerPrimary) ||
		cluster.Status.HAStatus.PlannedActions[2].DependsOn != string(haActionAcquireFence) ||
		cluster.Status.HAStatus.PlannedActions[3].DependsOn != string(haActionAssessPromotion) ||
		cluster.Status.HAStatus.PlannedActions[4].DependsOn != string(haActionPromoteStandby) ||
		cluster.Status.HAStatus.PlannedActions[5].DependsOn != string(haActionPromoteStandby) ||
		cluster.Status.HAStatus.PlannedActions[4].RouteFrom != "primary" ||
		cluster.Status.HAStatus.PlannedActions[4].RouteTo != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[5].RouteFrom != "primary-a" {
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
	expectedFormerFenceCommand := append([]string(nil), expectedAcquireCommand...)
	expectedFormerFenceCommand[len(expectedFormerFenceCommand)-1] = "LeaseHeld"
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[0].AdminCommand, expectedFormerFenceCommand) {
		t.Fatalf("unexpected former-primary fence admin command: %#v", cluster.Status.HAStatus.PlannedActions[0].AdminCommand)
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[1].AdminCommand, expectedAcquireCommand) {
		t.Fatalf("unexpected acquire-fence admin command: %#v", cluster.Status.HAStatus.PlannedActions[1].AdminCommand)
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[2].AdminCommand, []string{"promote", "assess", "--current-fence"}) {
		t.Fatalf("unexpected promotion-assessment admin command: %#v", cluster.Status.HAStatus.PlannedActions[2].AdminCommand)
	}
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[3].AdminCommand, []string{"promote", "--current-fence"}) {
		t.Fatalf("unexpected promote admin command: %#v", cluster.Status.HAStatus.PlannedActions[3].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[0].AdminURL != "http://primary-ha.default.svc:8081" ||
		cluster.Status.HAStatus.PlannedActions[0].AdminNodeID != "primary-a" {
		t.Fatalf("expected former-primary fence to target the old writer, got %#v", cluster.Status.HAStatus.PlannedActions[0])
	}
	if cluster.Status.HAStatus.PlannedActions[1].AdminURL != "http://standby-a-ha.default.svc:8081" ||
		cluster.Status.HAStatus.PlannedActions[2].AdminURL != "http://standby-a-ha.default.svc:8081" ||
		cluster.Status.HAStatus.PlannedActions[3].AdminURL != "http://standby-a-ha.default.svc:8081" {
		t.Fatalf("expected fence/promotion actions to target standby HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[1:4])
	}
	if cluster.Status.HAStatus.PlannedActions[1].AdminNodeID != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[2].AdminNodeID != "standby-a" ||
		cluster.Status.HAStatus.PlannedActions[3].AdminNodeID != "standby-a" {
		t.Fatalf("expected candidate fence/promotion actions to require standby node receipts, got %#v", cluster.Status.HAStatus.PlannedActions[1:4])
	}
	if cluster.Status.HAStatus.PlannedActions[4].AdminCommand != nil {
		t.Fatalf("route action should not publish an HA admin command without service execution context, got %#v", cluster.Status.HAStatus.PlannedActions[4].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[4].AdminURL != "" {
		t.Fatalf("route action should not publish an HA admin URL without service execution context, got %#v", cluster.Status.HAStatus.PlannedActions[4].AdminURL)
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
	if !reflect.DeepEqual(cluster.Status.HAStatus.PlannedActions[5].AdminCommand, expectedDemoteCommand) {
		t.Fatalf("unexpected former-primary demote admin command: %#v", cluster.Status.HAStatus.PlannedActions[5].AdminCommand)
	}
	if cluster.Status.HAStatus.PlannedActions[5].AdminURL != "http://primary-ha.default.svc:8081" {
		t.Fatalf("expected former-primary demote to target old primary HA admin URL, got %#v", cluster.Status.HAStatus.PlannedActions[5])
	}
	if cluster.Status.HAStatus.PlannedActions[5].AdminNodeID != "primary-a" {
		t.Fatalf("expected former-primary demote to require old primary node receipt, got %#v", cluster.Status.HAStatus.PlannedActions[5])
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

func TestPlanHAContinuesCommittedFenceTransactionWhenPrimaryAdminRecovers(t *testing.T) {
	cluster := haCluster()
	ha := cluster.Spec.HighAvailability
	ha.Admin = &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	ha.Standbys[0].AdminURL = "http://standby-a-ha.default.svc:8081"
	ha.Standbys[0].RouteSelector = haTestRouteSelector("standby-a")
	ha.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
	ha.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:         antflyv1.HADurabilityModeRemoteApply,
		Required:     1,
		StandbyNames: []string{"standby-a"},
	}
	ha.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{
		Enabled:          true,
		FencingAuthority: antflyv1.HAFencingAuthorityKubernetesLease,
	}
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.PrimaryAdminLastError = ""
	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{{
		Kind:            string(haActionFenceFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		RouteFrom:       "primary-a",
		RouteTo:         "standby-a",
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 1,
		FenceReason:     "LeaseHeld",
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhasePending,
		AdminError:      "connection refused",
	}}

	plan := planHA(cluster)
	if !plan.AutomaticPromotionAllowed || plan.PromotionStandbyName != "standby-a" {
		t.Fatalf("expected committed fence transaction to continue after admin recovery, got %#v", plan)
	}
	if len(plan.Actions) != 6 || plan.Actions[0].Kind != haActionFenceFormerPrimary {
		t.Fatalf("expected full pre-fence promotion chain, got %#v", plan.Actions)
	}
	if holder := haKubernetesLeaseFenceCandidate(ha, cluster.Status.HAStatus); holder != "standby-a" {
		t.Fatalf("expected committed Lease holder to be renewed, got %q", holder)
	}

	// The old writer can advance after its admin transport recovers but before
	// the pending node-local fence call succeeds. That transient lag must not
	// erase the committed transaction: fencing freezes the actual durable tail,
	// and the downstream assessment waits for the candidate to apply it.
	cluster.Status.HAStatus.PrimaryLSN = 13
	cluster.Status.HAStatus.Standbys[0].ApplyLagLSN = 1
	plan = planHA(cluster)
	if !plan.AutomaticPromotionAllowed || plan.PromotionStandbyName != "standby-a" {
		t.Fatalf("expected committed transaction to survive a moving old-primary tail, got %#v", plan)
	}
	if len(plan.Actions) != 6 || plan.Actions[0].Kind != haActionFenceFormerPrimary {
		t.Fatalf("expected full pre-fence chain while the candidate catches up, got %#v", plan.Actions)
	}
	if plan.Actions[0].TargetLSN != 12 {
		t.Fatalf("expected the pending transaction to preserve its original lower-bound LSN, got %#v", plan.Actions[0])
	}

	// A delayed admin call may cross the 30-second Lease deadline between
	// reconciles. The recorded in-flight fence transaction must remain the
	// renewal candidate so the controller can make the Lease ready again rather
	// than silently abandoning an ownership transfer it already started.
	cluster.Status.HAStatus.Fencing.Ready = false
	cluster.Status.HAStatus.Fencing.Reason = "LeaseExpired"
	plan = planHA(cluster)
	if plan.AutomaticPromotionAllowed || plan.PromotionStandbyName != "" {
		t.Fatalf("expected promotion execution to pause until the expired Lease is renewed, got %#v", plan)
	}
	if holder := haKubernetesLeaseFenceCandidate(ha, cluster.Status.HAStatus); holder != "standby-a" {
		t.Fatalf("expected expired committed Lease holder to remain the renewal candidate, got %q", holder)
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
	cluster.Status.HAStatus.Standbys[0].CanServeSafeReads = false
	reconciler.updateHAStatusAndConditions(cluster)

	if cluster.Status.HAStatus.ReadSafeStandbyCount != 0 {
		t.Fatalf("expected no read-safe standby while standby safe-read serving is disabled, got %d", cluster.Status.HAStatus.ReadSafeStandbyCount)
	}
	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected automatic promotion to wait for standby safe-read serving")
	}

	cluster.Status.HAStatus.Standbys[0].CanServeSafeReads = true
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

func TestUpdateHAStatusNormalizesAdminAllSyncRequiredCount(t *testing.T) {
	cluster := haCluster()
	cluster.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{
		{Name: "standby-a"},
		{Name: "standby-b"},
	}
	cluster.Spec.HighAvailability.SyncPolicy = &antflyv1.HASyncPolicy{
		Mode:          antflyv1.HADurabilityModeRemoteApply,
		Selection:     antflyv1.HAStandbySelectionAll,
		StandbyNames:  []string{"standby-a", "standby-b"},
		FailurePolicy: antflyv1.HAFailurePolicyDegradeToAsync,
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN:            10,
		PrimaryAdminReachable: true,
		PrimaryAdminLastError: "",
		Sync: antflyv1.HASyncStatus{
			Mode:       antflyv1.HADurabilityModeRemoteApply,
			Selection:  antflyv1.HAStandbySelectionAll,
			Satisfied:  1,
			Candidates: 2,
			Degraded:   true,
			Action:     "DegradeToAsync",
		},
		Standbys: []antflyv1.HAStandbyStatus{
			{Name: "standby-a", SlotName: "standby-a", Active: true, ReceivedLSN: 10, AppliedLSN: 10, SafeReadLSN: 10, CanServeSafeReads: true},
			{Name: "standby-b", SlotName: "standby-b", Active: true, ReceivedLSN: 10, AppliedLSN: 9, ApplyLagLSN: 1, SafeReadLSN: 9},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAStatusAndConditions(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != antflyv1.ReasonHASyncPolicyUnsatisfied {
		t.Fatalf("expected reachable admin ALL sync evidence to keep sync degraded, got %#v", degraded)
	}
	if cluster.Status.HAStatus.Sync.Required != 2 ||
		cluster.Status.HAStatus.Sync.Satisfied != 1 ||
		cluster.Status.HAStatus.Sync.Candidates != 2 ||
		cluster.Status.HAStatus.Sync.Action != "DegradeToAsync" {
		t.Fatalf("expected ALL sync required count to match named standbys, got %#v", cluster.Status.HAStatus.Sync)
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
		FormerPrimary: &antflyv1.HAFormerPrimaryStatus{
			NodeID:          "old-primary",
			Fenced:          true,
			FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
			FenceHolder:     "standby-a",
			FenceGeneration: 4,
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
		return
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
	rewindAction, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionRewindFormerPrimary)
	if !ok ||
		rewindAction.StandbyName != "old-primary" ||
		rewindAction.TargetLSN != 10 ||
		rewindAction.ObservedLSN != 10 ||
		rewindAction.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		rewindAction.FenceHolder != "standby-a" {
		t.Fatalf("expected rewind planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	cluster.Status.HAStatus.FormerPrimary.FenceAuthority = antflyv1.HAFencingAuthorityStorageFence
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		former.Fenced ||
		former.Action != string(haActionDemoteFormerPrimary) ||
		former.Reason != "FormerPrimaryFenceNotObserved" {
		t.Fatalf("expected authority-mismatched fence to block former-primary rejoin, got %#v", former)
	}

	cluster.Status.HAStatus.FormerPrimary.Fenced = true
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
	reseedAction, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionReseedFormerPrimary)
	seedAction, seedOK := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionSeedStandby)
	if !ok ||
		!seedOK ||
		reseedAction.StandbyName != "old-primary" ||
		seedAction.DependsOn != string(haActionReseedFormerPrimary) ||
		seedAction.StandbyName != "old-primary" {
		t.Fatalf("expected reseed planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{
		NodeID:            "old-primary",
		Fenced:            true,
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
		former.RewindPossible ||
		!former.ReseedRequired ||
		!former.Diverged ||
		former.Action != string(haActionReseedFormerPrimary) ||
		former.Reason != "FormerPrimaryRequiresReseed" ||
		former.SwitchLSN != 10 ||
		former.ObservedLSN != 11 {
		t.Fatalf("expected unsafe recorded rewind assessment to fail closed to reseed, got %#v", former)
	}
	reseedAction, ok = haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionReseedFormerPrimary)
	if !ok ||
		reseedAction.TargetLSN != 10 ||
		reseedAction.ObservedLSN != 11 {
		t.Fatalf("expected assessed reseed planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.Standbys = []antflyv1.HAStandbyStatus{{
		Name:        "standby-a",
		SlotName:    "standby-a",
		Active:      true,
		TimelineID:  2,
		ReceivedLSN: 12,
		AppliedLSN:  12,
		SafeReadLSN: 12,
	}}
	cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{
		NodeID:            "old-primary",
		Fenced:            true,
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:       "standby-a",
		FenceGeneration:   4,
		TargetTimelineID:  2,
		TargetEpoch:       2,
		ForkLSN:           10,
		FormerLastLSN:     10,
		RetainedFromLSN:   8,
		DataLossDiscarded: false,
		AssessedAction:    "rewind",
		AssessedReason:    "parent_timeline_retained",
	}
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		former.Action != string(haActionRewindFormerPrimary) ||
		former.Reason != "parent_timeline_retained" ||
		former.SwitchLSN != 10 ||
		former.ObservedLSN != 10 {
		t.Fatalf("expected recorded rejoin assessment to drive rewind without standby observation, got %#v", former)
	}
	rewindAction, ok = haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionRewindFormerPrimary)
	if !ok ||
		rewindAction.TargetLSN != 10 ||
		rewindAction.ObservedLSN != 10 {
		t.Fatalf("expected assessment-only rewind planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.Standbys = []antflyv1.HAStandbyStatus{{
		Name:        "old-primary",
		SlotName:    "old-primary",
		Active:      true,
		TimelineID:  1,
		ReceivedLSN: 11,
		AppliedLSN:  11,
	}, {
		Name:        "standby-a",
		SlotName:    "standby-a",
		Active:      true,
		TimelineID:  2,
		ReceivedLSN: 12,
		AppliedLSN:  12,
		SafeReadLSN: 12,
	}}
	cluster.Status.HAStatus.FormerPrimary.FenceGeneration = 3
	reconciler.updateHAStatusAndConditions(cluster)

	former = cluster.Status.HAStatus.FormerPrimary
	if former == nil ||
		former.Fenced ||
		former.Action != string(haActionDemoteFormerPrimary) ||
		former.Reason != "FormerPrimaryFenceNotObserved" {
		t.Fatalf("expected stale assessed fence generation to block rejoin, got %#v", former)
	}

	cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{
		NodeID:            "old-primary",
		Fenced:            true,
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:       "standby-a",
		FenceGeneration:   4,
		TargetTimelineID:  2,
		TargetEpoch:       2,
		ForkLSN:           10,
		FormerLastLSN:     10,
		RetainedFromLSN:   8,
		DataLossDiscarded: false,
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
	if _, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionRewindFormerPrimary); ok {
		t.Fatalf("expected no former-primary rewind action after rejoin, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if _, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionReseedFormerPrimary); ok {
		t.Fatalf("expected no former-primary planned action after rejoin, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestUpdateHAStatusUsesDurableFormerPrimaryFenceAfterLeaseExpires(t *testing.T) {
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
		FormerPrimary: &antflyv1.HAFormerPrimaryStatus{
			NodeID:          "old-primary",
			Fenced:          true,
			FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
			FenceHolder:     "standby-a",
			FenceGeneration: 4,
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
		t.Fatalf("expected durable former-primary fence to permit rewind after live lease expiry, got %#v", former)
	}
	rewindAction, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionRewindFormerPrimary)
	if !ok ||
		rewindAction.DependsOn != string(haActionFenceFormerPrimary) ||
		rewindAction.AdminURL != "http://old-primary-ha.default.svc:8081" {
		t.Fatalf("expected executable former-primary rewind, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	command := strings.Join(rewindAction.AdminCommand, " ")
	if !strings.Contains(command, "--fence-token token") ||
		!strings.Contains(command, "--fence-generation 4") {
		t.Fatalf("expected rejoin command to carry durable fence receipt, got %#v", rewindAction.AdminCommand)
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
		FormerPrimary: &antflyv1.HAFormerPrimaryStatus{
			NodeID:          "old-primary",
			Fenced:          true,
			FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
			FenceHolder:     "standby-a",
			FenceGeneration: 4,
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
	rewindAction, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionRewindFormerPrimary)
	if !ok {
		t.Fatalf("expected rewind planned action, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	if rewindAction.AdminCommand != nil {
		t.Fatalf("rewind should not be executable without a fence token, got %#v", rewindAction.AdminCommand)
	}
	if rewindAction.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		rewindAction.FenceHolder != "standby-a" {
		t.Fatalf("expected former-primary planned action to carry fence identity, got %#v", rewindAction)
	}

	cluster.Status.HAStatus.LastPromotion.FenceToken = "token"
	reconciler.updateHAStatusAndConditions(cluster)
	rewindAction, ok = haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionRewindFormerPrimary)
	if !ok {
		t.Fatalf("expected rewind planned action with command, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

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
	if !reflect.DeepEqual(rewindAction.AdminCommand, expected) {
		t.Fatalf("unexpected fenced rejoin command: %#v", rewindAction.AdminCommand)
	}
	if rewindAction.AdminURL != "http://old-primary-ha.default.svc:8081" {
		t.Fatalf("expected former primary rejoin to target former-primary HA admin URL, got %#v", rewindAction)
	}

	cluster.Status.HAStatus.LastPromotion.Forced = true
	cluster.Status.HAStatus.LastPromotion.DataLossPossible = true
	reconciler.updateHAStatusAndConditions(cluster)

	reseedAction, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionReseedFormerPrimary)
	if !ok ||
		reseedAction.DependsOn != string(haActionFenceFormerPrimary) ||
		reseedAction.AdminURL != "http://standby-a-ha.default.svc:8081" ||
		reseedAction.AdminNodeID != "standby-a" {
		t.Fatalf("expected forced promotion to require former-primary reseed, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
	forcedCommand := strings.Join(reseedAction.AdminCommand, " ")
	if !strings.Contains(forcedCommand, "--fence-forced") {
		t.Fatalf("expected forced rejoin command to carry forced fence evidence, got %#v", reseedAction.AdminCommand)
	}
	if strings.Contains(forcedCommand, "allow-rewind-after-forced-promotion") {
		t.Fatalf("forced promotion must not opt into former-primary rewind automatically, got %#v", reseedAction.AdminCommand)
	}
}

func TestUpdateHAStatusPlansPrimaryRouteAfterCompletedPromotion(t *testing.T) {
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
			CurrentTarget: "primary",
		},
		LastPromotion: haCompletePromotionReceipt("primary-a", "standby-a"),
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
	routeAction, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionUpdatePrimaryRoute)
	if !ok ||
		routeAction.RouteFrom != "primary" ||
		routeAction.RouteTo != "standby-a" ||
		routeAction.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		routeAction.FenceGeneration != 5 ||
		routeAction.FenceReason != "operator-approved" {
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
	routeAction, ok = haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionUpdatePrimaryRoute)
	if !ok ||
		routeAction.RouteFrom != "standby-a" ||
		routeAction.RouteTo != "standby-a" ||
		routeAction.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		routeAction.FenceGeneration != 5 {
		t.Fatalf("expected route planned action for stale fence generation, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = ""
	cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = 5
	reconciler.updateHAStatusAndConditions(cluster)

	route = cluster.Status.HAStatus.PrimaryRoute
	if !route.Stale ||
		route.Action != string(haActionUpdatePrimaryRoute) ||
		route.Reason != "PrimaryRouteFenceAuthorityStale" ||
		route.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		route.FenceGeneration != 5 {
		t.Fatalf("expected route update for missing fence authority, got %#v", route)
	}
	routeAction, ok = haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionUpdatePrimaryRoute)
	if !ok ||
		routeAction.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		routeAction.FenceGeneration != 5 {
		t.Fatalf("expected route planned action for missing fence authority, got %#v", cluster.Status.HAStatus.PlannedActions)
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
	routeAction, ok = haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionUpdatePrimaryRoute)
	if !ok ||
		routeAction.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease {
		t.Fatalf("expected route planned action for stale fence authority, got %#v", cluster.Status.HAStatus.PlannedActions)
	}

	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = antflyv1.HAFencingAuthorityKubernetesLease
	reconciler.updateHAStatusAndConditions(cluster)

	route = cluster.Status.HAStatus.PrimaryRoute
	if route.Stale || route.Action != "None" || route.DesiredTarget != "standby-a" {
		t.Fatalf("expected current route after update, got %#v", route)
	}
	if _, ok := haPlannedActionByKind(cluster.Status.HAStatus.PlannedActions, haActionUpdatePrimaryRoute); ok {
		t.Fatalf("expected no route planned action once current, got %#v", cluster.Status.HAStatus.PlannedActions)
	}
}

func TestUpdateHAStatusDoesNotPlanPrimaryRouteFromIncompletePromotion(t *testing.T) {
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12,
		PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
			CurrentTarget: "primary",
		},
		LastPromotion: &antflyv1.HAPromotionStatus{
			PromotedStandbyID: "standby-a",
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
	if route.Stale || route.Action != "None" || route.CurrentTarget != "primary" || route.DesiredTarget != "primary" {
		t.Fatalf("expected incomplete promotion to leave primary route unchanged, got %#v", route)
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind == string(haActionUpdatePrimaryRoute) {
			t.Fatalf("expected no route update from incomplete promotion evidence, got %#v", cluster.Status.HAStatus.PlannedActions)
		}
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
	cluster.Status.HAStatus.LastPromotion = haCompletePromotionReceipt("primary-a", "standby-a")
	cluster.Status.HAStatus.LastPromotion.FenceGeneration = 1
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
	cluster.Spec.HighAvailability.Identity.ShardID = 10
	cluster.Spec.HighAvailability.Identity.TableID = 20
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
	if lease.Annotations[haFencingLeaseAnnotationClusterID] != "100" ||
		lease.Annotations[haFencingLeaseAnnotationShardID] != "10" ||
		lease.Annotations[haFencingLeaseAnnotationTableID] != "20" ||
		lease.Annotations[haFencingLeaseAnnotationCurrentPrimaryID] != "primary-a" ||
		lease.Annotations[haFencingLeaseAnnotationTimelineID] != "4" ||
		lease.Annotations[haFencingLeaseAnnotationEpoch] != "6" ||
		lease.Annotations[haFencingLeaseAnnotationPrimaryLSN] != "12" {
		t.Fatalf("expected HA fence scope annotations, got %#v", lease.Annotations)
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

func TestReconcileHAFencingLeaseRenewsExpiredCommittedTransfer(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.PrimaryAdminLastError = ""
	cluster.Status.HAStatus.Fencing = antflyv1.HAFencingStatus{
		Authority:  antflyv1.HAFencingAuthorityKubernetesLease,
		Ready:      false,
		Holder:     "standby-a",
		Generation: 2,
		Reason:     "LeaseExpired",
	}
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{{
		Kind:            string(haActionFenceFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		RouteFrom:       "primary-a",
		RouteTo:         "standby-a",
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 2,
		FenceReason:     "LeaseHeld",
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhasePending,
		AdminError:      "connection refused",
	}}
	durationSeconds := int32(30)
	lease := haFenceLease(cluster, time.Now().Add(-2*time.Minute), durationSeconds, 2, "standby-a")
	reconciler := testHAReconciler(t, cluster, lease)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("renew committed fencing lease: %v", err)
	}

	observed := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, observed); err != nil {
		t.Fatalf("get renewed fencing lease: %v", err)
	}
	if observed.Spec.RenewTime == nil || !observed.Spec.RenewTime.After(lease.Spec.RenewTime.Time) {
		t.Fatalf("expected expired committed Lease renew time to advance, old=%#v new=%#v", lease.Spec.RenewTime, observed.Spec.RenewTime)
	}
	if !cluster.Status.HAStatus.Fencing.Ready ||
		cluster.Status.HAStatus.Fencing.Holder != "standby-a" ||
		cluster.Status.HAStatus.Fencing.Generation != 2 ||
		cluster.Status.HAStatus.Fencing.Reason != "LeaseHeld" {
		t.Fatalf("expected successful renewal to publish ready in-memory fencing status, got %#v", cluster.Status.HAStatus.Fencing)
	}
	plan := planHA(cluster)
	if !plan.AutomaticPromotionAllowed || plan.PromotionStandbyName != "standby-a" {
		t.Fatalf("expected the renewed committed transfer to continue, got %#v", plan)
	}
}

func TestReconcileHAFencingLeaseAllowsRemoteWriteCandidate(t *testing.T) {
	remoteApply := false
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.AutomaticFailover.RequireRemoteApply = &remoteApply
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	cluster.Status.HAStatus.Standbys[0].AppliedLSN = 11
	cluster.Status.HAStatus.Standbys[0].SafeReadLSN = 11
	cluster.Status.HAStatus.Standbys[0].ApplyLagLSN = 1
	cluster.Status.HAStatus.Standbys[0].CanServeSafeReads = false
	reconciler := testHAReconciler(t, cluster)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	lease := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease); err != nil {
		t.Fatalf("get fencing lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "standby-a" {
		t.Fatalf("expected remote-write standby-a lease holder, got %#v", lease.Spec.HolderIdentity)
	}

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe fencing status: %v", err)
	}
	reconciler.updateHAStatusAndConditions(cluster)
	if !cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected received-but-not-applied standby to satisfy automatic promotion when requireRemoteApply=false")
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
			Name:              "standby-b",
			SlotName:          "standby-b",
			Active:            true,
			ReceivedLSN:       12,
			AppliedLSN:        12,
			SafeReadLSN:       12,
			CanServeSafeReads: true,
			Status:            "healthy",
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

func TestReconcileHAFencingLeaseSkipsCandidateWithoutSafeReadServing(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	cluster.Status.HAStatus.Standbys[0].CanServeSafeReads = false
	reconciler := testHAReconciler(t, cluster)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile fencing lease: %v", err)
	}

	lease := &coordinationv1.Lease{}
	err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no fencing lease without safe-read serving, got lease=%#v err=%v", lease, err)
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

func TestObserveHAFencingStatusRejectsStaleTimelineLeaseScope(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	lease := haFenceLease(cluster, time.Now(), 30, 3, "standby-a")
	lease.Annotations[haFencingLeaseAnnotationTimelineID] = "3"
	reconciler := testHAReconciler(t, lease)

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe fencing status: %v", err)
	}
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	reconciler.updateHAStatusAndConditions(cluster)

	fencing := cluster.Status.HAStatus.Fencing
	if fencing.Ready || fencing.Reason != "LeaseScopeMismatch" {
		t.Fatalf("expected stale timeline lease scope to be rejected, got %#v", fencing)
	}
	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected stale timeline lease scope to block automatic promotion")
	}
}

func TestObserveHAFencingStatusRejectsStaleIdentityLeaseScope(t *testing.T) {
	cases := []struct {
		name       string
		annotation string
		value      string
	}{{
		name:       "cluster",
		annotation: haFencingLeaseAnnotationClusterID,
		value:      "99",
	}, {
		name:       "shard",
		annotation: haFencingLeaseAnnotationShardID,
		value:      "11",
	}, {
		name:       "table",
		annotation: haFencingLeaseAnnotationTableID,
		value:      "21",
	}, {
		name:       "epoch",
		annotation: haFencingLeaseAnnotationEpoch,
		value:      "5",
	}, {
		name:       "current primary",
		annotation: haFencingLeaseAnnotationCurrentPrimaryID,
		value:      "primary-b",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := haClusterWithAutomaticKubernetesLeaseFailover()
			cluster.Spec.HighAvailability.Identity.ShardID = 10
			cluster.Spec.HighAvailability.Identity.TableID = 20
			cluster.Status.HAStatus = caughtUpHAStatus()
			lease := haFenceLease(cluster, time.Now(), 30, 3, "standby-a")
			lease.Annotations[tc.annotation] = tc.value
			reconciler := testHAReconciler(t, lease)

			if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
				t.Fatalf("observe fencing status: %v", err)
			}
			cluster.Status.HAStatus.PrimaryAdminReachable = false
			cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
			reconciler.updateHAStatusAndConditions(cluster)

			fencing := cluster.Status.HAStatus.Fencing
			if fencing.Ready || fencing.Reason != "LeaseScopeMismatch" {
				t.Fatalf("expected stale %s lease scope to be rejected, got %#v", tc.name, fencing)
			}
			if cluster.Status.HAStatus.AutomaticPromotionAllowed {
				t.Fatalf("expected stale %s lease scope to block automatic promotion", tc.name)
			}
		})
	}
}

func TestObserveHAFencingStatusRejectsStalePromotionBoundaryLeaseScope(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	lease := haFenceLease(cluster, time.Now(), 30, 3, "standby-a")
	lease.Annotations[haFencingLeaseAnnotationPrimaryLSN] = "11"
	reconciler := testHAReconciler(t, lease)

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe fencing status: %v", err)
	}
	cluster.Status.HAStatus.PrimaryAdminReachable = false
	cluster.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	reconciler.updateHAStatusAndConditions(cluster)

	fencing := cluster.Status.HAStatus.Fencing
	if fencing.Ready || fencing.Reason != "LeaseScopeMismatch" {
		t.Fatalf("expected stale promotion-boundary lease scope to be rejected, got %#v", fencing)
	}
	if cluster.Status.HAStatus.AutomaticPromotionAllowed {
		t.Fatal("expected stale promotion-boundary lease scope to block automatic promotion")
	}
}

func TestObserveHAFencingStatusPreservesCommittedBoundaryWhileOldPrimaryTailMoves(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{{
		Kind:            string(haActionFenceFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		RouteFrom:       "primary-a",
		RouteTo:         "standby-a",
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 3,
		FenceReason:     "LeaseHeld",
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhasePending,
		AdminError:      "connection refused",
	}}
	lease := haFenceLease(cluster, time.Now(), 30, 3, "standby-a")

	// The Lease and action committed LSN 12 while the old-primary admin
	// transport was isolated. Once transport heals, the still-unfenced writer
	// may report a newer tail before the pending fence call linearizes.
	cluster.Status.HAStatus.PrimaryLSN = 13
	cluster.Status.HAStatus.Standbys[0].ApplyLagLSN = 1
	reconciler := testHAReconciler(t, lease)

	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe committed fencing status: %v", err)
	}
	fencing := cluster.Status.HAStatus.Fencing
	if !fencing.Ready || fencing.Reason != "LeaseHeld" || fencing.Holder != "standby-a" || fencing.Generation != 3 {
		t.Fatalf("expected the exact committed lower-bound Lease to remain ready, got %#v", fencing)
	}
	plan := planHA(cluster)
	if !plan.AutomaticPromotionAllowed || plan.PromotionStandbyName != "standby-a" {
		t.Fatalf("expected the committed transaction to survive the moving tail, got %#v", plan)
	}

	// The successful node-local fence freezes the newer tail, but observation
	// runs before Lease renewal in the next reconcile. The still-valid Lease can
	// therefore carry the original lower bound for exactly this transition.
	cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	cluster.Status.HAStatus.PlannedActions[0].AdminError = ""
	cluster.Status.HAStatus.PlannedActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		FencePromotedNodeID: "standby-a",
		FenceRequiredLSN:    13,
		FenceObservedLSN:    13,
		FenceGeneration:     3,
	}
	if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
		t.Fatalf("observe successful fence transition: %v", err)
	}
	fencing = cluster.Status.HAStatus.Fencing
	if !fencing.Ready || fencing.Reason != "LeaseHeld" {
		t.Fatalf("expected original lower-bound Lease to remain ready until renewal publishes the frozen tail, got %#v", fencing)
	}
}

func TestReconcileHAFencingLeaseKeepsCommittedLowerBoundWhileOldPrimaryTailMoves(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.Fencing = readyFencingStatus()
	cluster.Status.HAStatus.Fencing.Generation = 3
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{{
		Kind:            string(haActionFenceFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		RouteFrom:       "primary-a",
		RouteTo:         "standby-a",
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 3,
		FenceReason:     "LeaseHeld",
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhasePending,
		AdminError:      "connection refused",
	}}
	lease := haFenceLease(cluster, time.Now().Add(-time.Second), 30, 3, "standby-a")
	cluster.Status.HAStatus.PrimaryLSN = 13
	reconciler := testHAReconciler(t, cluster, lease)

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("renew committed fencing lease: %v", err)
	}
	observed := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, observed); err != nil {
		t.Fatalf("get renewed fencing lease: %v", err)
	}
	if got := observed.Annotations[haFencingLeaseAnnotationPrimaryLSN]; got != "12" {
		t.Fatalf("expected renewal to preserve committed lower-bound LSN 12, got %q", got)
	}
	if observed.Spec.RenewTime == nil || !observed.Spec.RenewTime.After(lease.Spec.RenewTime.Time) {
		t.Fatalf("expected committed lease renewal time to advance, old=%#v new=%#v", lease.Spec.RenewTime, observed.Spec.RenewTime)
	}

	cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	cluster.Status.HAStatus.PlannedActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		FencePromotedNodeID: "standby-a",
		FenceRequiredLSN:    13,
		FenceObservedLSN:    13,
		FenceGeneration:     3,
	}
	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("advance committed fencing lease to frozen tail: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, observed); err != nil {
		t.Fatalf("get advanced fencing lease: %v", err)
	}
	if got := observed.Annotations[haFencingLeaseAnnotationPrimaryLSN]; got != "13" {
		t.Fatalf("expected renewal to advance from the lower bound to frozen tail 13, got %q", got)
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

	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{Enabled: false}
	if got, want := periodicRequeueAfter(cluster), 30*time.Second; got != want {
		t.Fatalf("expected autoscaling requeue without HA renewal, got %s", got)
	}
}

func TestPeriodicRequeueRetriesDirectHAAdminAction(t *testing.T) {
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	nextRetry := metav1.NewTime(now.Add(17 * time.Second))
	cluster := haCluster()
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PlannedActions: []antflyv1.HAPlannedActionStatus{{
			Kind:          string(haActionCreateSlot),
			AdminJobName:  haAdminDirectAPIName,
			AdminJobPhase: haAdminJobPhasePending,
			AdminError:    "HA admin API returned status 503: primary restarting",
			Retryable:     true,
			NextRetryAt:   &nextRetry,
		}},
	}

	if got, want := periodicRequeueAfterAt(cluster, now), 17*time.Second; got != want {
		t.Fatalf("expected direct HA admin retry requeue %s, got %s", want, got)
	}
	cluster.Spec.HighAvailability.AutomaticFailover = &antflyv1.HAAutomaticFailoverPolicy{Enabled: false}
	if got, want := periodicRequeueAfterAt(cluster, now), 17*time.Second; got != want {
		t.Fatalf("expected persisted direct HA admin retry requeue %s, got %s", want, got)
	}

	cluster.Status.HAStatus.PlannedActions[0].AdminError = ""
	if got := periodicRequeueAfterAt(cluster, now); got != 0 {
		t.Fatalf("expected no retry requeue without transient error, got %s", got)
	}

	cluster.Status.HAStatus.PlannedActions[0].AdminError = "HA admin API returned status 503"
	cluster.Status.HAStatus.PlannedActions[0].AdminJobName = "antfly-ha-action"
	if got := periodicRequeueAfterAt(cluster, now); got != 0 {
		t.Fatalf("expected no retry requeue for CLI admin job, got %s", got)
	}
}

func TestHADirectAdminRetryDelayUsesConfiguredExponentialCap(t *testing.T) {
	baseSeconds := int32(3)
	maxSeconds := int32(10)
	admin := &antflyv1.HAAdminSpec{
		DirectRetryBaseSeconds: &baseSeconds,
		DirectRetryMaxSeconds:  &maxSeconds,
	}
	tests := []struct {
		attempt int32
		want    time.Duration
	}{
		{attempt: 1, want: 3 * time.Second},
		{attempt: 2, want: 6 * time.Second},
		{attempt: 3, want: 10 * time.Second},
		{attempt: 30, want: 10 * time.Second},
	}
	for _, test := range tests {
		if got := haDirectAdminRetryDelay(admin, test.attempt); got != test.want {
			t.Fatalf("attempt %d retry delay = %s, want %s", test.attempt, got, test.want)
		}
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
	cluster.Spec.HighAvailability.Identity = &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
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
			Name:              "standby-a",
			SlotName:          "standby-a",
			Active:            true,
			ReceivedLSN:       12,
			AppliedLSN:        12,
			SafeReadLSN:       12,
			CanServeSafeReads: true,
			ApplyLagLSN:       0,
			Status:            "healthy",
		}},
	}
}

func haCompletePromotionReceipt(oldPrimaryID, promotedNodeID string) *antflyv1.HAPromotionStatus {
	return &antflyv1.HAPromotionStatus{
		OldPrimaryID:      oldPrimaryID,
		PromotedStandbyID: promotedNodeID,
		ClusterID:         100,
		ShardID:           10,
		TableID:           20,
		ParentTimelineID:  4,
		ParentEpoch:       6,
		NewTimelineID:     5,
		NewEpoch:          7,
		SwitchLSN:         12,
		RequiredLSN:       12,
		ObservedLSN:       12,
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration:   5,
		FenceReason:       "operator-approved",
		FenceToken:        "token",
	}
}

func haPlannedActionByKind(actions []antflyv1.HAPlannedActionStatus, kind haActionKind) (antflyv1.HAPlannedActionStatus, bool) {
	for _, action := range actions {
		if action.Kind == string(kind) {
			return action, true
		}
	}
	return antflyv1.HAPlannedActionStatus{}, false
}

func haFenceLease(cluster *antflyv1.AntflyCluster, renewTime time.Time, durationSeconds int32, transitions int32, holder string) *coordinationv1.Lease {
	renew := metav1.NewMicroTime(renewTime)
	annotations := map[string]string{}
	if scope, ok := haCurrentFencingLeaseScope(cluster); ok {
		annotations = scope.annotations()
	}
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:        haFencingLeaseName(cluster),
			Namespace:   cluster.Namespace,
			Annotations: annotations,
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
