package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// This is the durable-commit safety oracle for the controller action. A
// dependent promotion action must never observe Succeeded in the same
// reconciliation that first constructs the final isolation receipt.
func TestReconcilePhysicalIsolationCheckpointsFinalReceiptBeforeReleasingDependency(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cluster, action := validPhysicalIsolationReceiptFixture(now)
	cluster.Status.HAStatus.Standbys[0].CaughtUpToReceived = true
	action.AdminJobPhase = haAdminJobPhaseRunning
	action.CompletedAt = nil
	action.PhysicalIsolationReceipt.CompletedAt = nil
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{
		action,
		{Kind: string(haActionAcquireFence), DependsOn: string(haActionIsolateFormerPrimary)},
	}

	sts, lease := currentPhysicalIsolationObjects(cluster, now)
	reconciler := testHAReconciler(t, cluster, sts, lease)
	reconciler.BoundaryReader = haTestResourceVersionReader{Reader: reconciler.Client, listResourceVersion: "pods-absence-current"}
	reconciler.Now = func() time.Time { return now.Add(12 * time.Second) }
	monotonicNow := now
	reconciler.MonotonicNow = func() time.Time { return monotonicNow }

	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); err != nil {
		t.Fatalf("start local watchdog barrier: %v", err)
	}
	if haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, 1, cluster) {
		t.Fatal("dependent action released before local watchdog barrier elapsed")
	}
	monotonicNow = monotonicNow.Add(10 * time.Second)
	err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster)
	if !errors.Is(err, errHAStatusCheckpointed) {
		t.Fatalf("final isolation receipt was not a durable checkpoint barrier: %v", err)
	}
	stored := &antflyv1.AntflyCluster{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, stored); err != nil {
		t.Fatalf("read durably checkpointed cluster status: %v", err)
	}
	if stored.Status.HAStatus == nil || len(stored.Status.HAStatus.PlannedActions) < 1 ||
		!haPhysicalIsolationSucceededWithEvidence(stored, stored.Status.HAStatus.PlannedActions[0]) {
		t.Fatalf("final isolation receipt was not present in persisted status: %#v", stored.Status.HAStatus)
	}
	if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, 1, cluster) {
		t.Fatal("checkpointed complete isolation receipt did not satisfy its dependent action")
	}
}

// A persisted Succeeded phase is not enough after an operator process or
// leader restart. The new controller instance has no local monotonic grace
// observation and must fail closed before any dependent action can run.
func TestReconcilePhysicalIsolationFreshControllerWaitsLocalWatchdogBarrier(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cluster, action := validPhysicalIsolationReceiptFixture(now)
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{action}

	sts, lease := currentPhysicalIsolationObjects(cluster, now)
	reconciler := testHAReconciler(t, cluster, sts, lease)
	reconciler.BoundaryReader = haTestResourceVersionReader{Reader: reconciler.Client, listResourceVersion: "pods-absence-current"}
	reconciler.Now = func() time.Time { return now.Add(20 * time.Second) }
	monotonicNow := now
	reconciler.MonotonicNow = func() time.Time { return monotonicNow }

	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); !errors.Is(err, errHAPhysicalIsolationGracePending) {
		t.Fatalf("fresh controller did not fail closed on missing local watchdog barrier: %v", err)
	}
	monotonicNow = monotonicNow.Add(10 * time.Second)
	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); err != nil {
		t.Fatalf("exact receipt remained blocked after fresh local watchdog barrier elapsed: %v", err)
	}

	// A leader/process replacement gets a different in-memory barrier and must
	// conservatively wait the full bound again.
	restarted := testHAReconciler(t, cluster, sts.DeepCopy(), lease.DeepCopy())
	restarted.BoundaryReader = haTestResourceVersionReader{Reader: restarted.Client, listResourceVersion: "pods-absence-current"}
	restarted.Now = reconciler.Now
	restarted.MonotonicNow = func() time.Time { return monotonicNow }
	if err := restarted.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); !errors.Is(err, errHAPhysicalIsolationGracePending) {
		t.Fatalf("restarted controller borrowed prior process watchdog time: %v", err)
	}
}

// Pod API absence is not proof that a force-deleted container stopped on a
// partitioned node. Without an exact pre-transfer runtime watchdog proof the
// action must remain Running forever, even after every wall/monotonic delay.
func TestReconcilePhysicalIsolationForceDeletedOrphanWithoutWatchdogProofFailsClosed(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cluster, action := validPhysicalIsolationReceiptFixture(now)
	action.AdminJobPhase = haAdminJobPhaseRunning
	action.CompletedAt = nil
	receipt := action.PhysicalIsolationReceipt
	receipt.WatchdogProof = nil
	receipt.IsolatedStatefulSetGeneration = 0
	receipt.IsolatedStatefulSetObservedGeneration = 0
	receipt.IsolatedStatefulSetResourceVersion = ""
	receipt.ObservedLeaseResourceVersion = ""
	receipt.AbsenceProven = false
	receipt.AbsencePodListResourceVersion = ""
	receipt.FrozenBoundaryLSN = 0
	receipt.ObservedAt = nil
	receipt.CompletedAt = nil
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{action}

	sts, lease := currentPhysicalIsolationObjects(cluster, now)
	reconciler := testHAReconciler(t, cluster, sts, lease)
	reconciler.BoundaryReader = haTestResourceVersionReader{Reader: reconciler.Client, listResourceVersion: "force-deleted-pod-list"}
	reconciler.Now = func() time.Time { return now.Add(20 * time.Second) }
	reconciler.MonotonicNow = func() time.Time { return now.Add(24 * time.Hour) }

	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); err != nil {
		t.Fatalf("missing watchdog proof should fail closed without corrupting status: %v", err)
	}
	got := cluster.Status.HAStatus.PlannedActions[0]
	if got.AdminJobPhase != haAdminJobPhaseRunning || got.PhysicalIsolationReceipt.ObservedAt != nil || got.CompletedAt != nil {
		t.Fatalf("force-deleted orphan was treated as isolated without watchdog proof: %#v", got)
	}
}

func currentPhysicalIsolationObjects(cluster *antflyv1.AntflyCluster, now time.Time) (*appsv1.StatefulSet, *coordinationv1.Lease) {
	zero := int32(0)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "antfly-standalone", Namespace: "default", UID: types.UID("sts-uid"), ResourceVersion: "2", Generation: 2,
			OwnerReferences: []metav1.OwnerReference{{UID: cluster.UID, Controller: ptr.To(true)}},
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: &zero},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 2},
	}
	lease := haFenceLease(cluster, now, haFencingLeaseDefaultDurationSeconds, 2, "standby-a")
	lease.UID = types.UID("lease-uid")
	return sts, lease
}
