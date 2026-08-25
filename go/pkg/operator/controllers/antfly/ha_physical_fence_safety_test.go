package controllers

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func TestPhysicalIsolationOwnsOnlyExactPendingPrimaryFailure(t *testing.T) {
	status := &antflyv1.HAStatus{
		PrimaryAdminFailureThresholdMet: true,
		PrimaryAdminLastError:           "primary admin endpoint timed out",
		PlannedActions: []antflyv1.HAPlannedActionStatus{{
			Kind:                     string(haActionIsolateFormerPrimary),
			AdminJobName:             haKubernetesPhysicalFenceName,
			AdminJobPhase:            haAdminJobPhaseRunning,
			PhysicalIsolationReceipt: &antflyv1.HAPhysicalIsolationReceiptStatus{},
		}},
	}
	if !haPhysicalIsolationOwnsPrimaryFailure(status) {
		t.Fatal("exact running physical isolation did not retain the durable primary failure observation")
	}

	tests := map[string]func(*antflyv1.HAStatus){
		"reachable primary": func(candidate *antflyv1.HAStatus) { candidate.PrimaryAdminReachable = true },
		"threshold not met": func(candidate *antflyv1.HAStatus) { candidate.PrimaryAdminFailureThresholdMet = false },
		"missing error":     func(candidate *antflyv1.HAStatus) { candidate.PrimaryAdminLastError = "" },
		"missing receipt": func(candidate *antflyv1.HAStatus) {
			candidate.PlannedActions[0].PhysicalIsolationReceipt = nil
		},
		"wrong executor": func(candidate *antflyv1.HAStatus) {
			candidate.PlannedActions[0].AdminJobName = "direct-admin-api"
		},
		"promotion recorded": func(candidate *antflyv1.HAStatus) {
			candidate.LastPromotion = &antflyv1.HAPromotionStatus{ClusterID: 1}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := status.DeepCopy()
			mutate(candidate)
			if haPhysicalIsolationOwnsPrimaryFailure(candidate) {
				t.Fatal("non-authoritative isolation state suppressed a live primary probe")
			}
		})
	}
}

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

// Once the exact isolation receipt is complete, later route/topology work must
// not depend on the promotion Lease remaining live. The promoted process can
// restart or change authority mode before Colony adopts the child identity.
func TestReconcileCompletedPhysicalIsolationDoesNotRevalidateMutableLease(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cluster, action := validPhysicalIsolationReceiptFixture(now)
	identity := cluster.Spec.HighAvailability.Identity
	promotion := haCompletePromotionReceipt(action.StandbyName, action.RouteTo)
	promotion.ClusterID = identity.ClusterID
	promotion.ShardID = identity.ShardID
	promotion.TableID = identity.TableID
	promotion.ParentTimelineID = identity.TimelineID
	promotion.ParentEpoch = identity.Epoch
	promotion.FenceGeneration = action.FenceGeneration
	cluster.Status.HAStatus.LastPromotion = promotion
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{action}

	// Deliberately omit the StatefulSet and Lease. A terminal, structurally
	// valid receipt must release route reconciliation from external old-scope
	// objects even while the spec still records the parent topology.
	reconciler := testHAReconciler(t, cluster)
	reconciler.Now = func() time.Time { return now.Add(24 * time.Hour) }
	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); err != nil {
		t.Fatalf("completed receipt borrowed authority from mutable external objects: %v", err)
	}

	cluster.Status.HAStatus.PlannedActions[0].PhysicalIsolationReceipt.WatchdogProof.ProcessBootID = "corrupt"
	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); err == nil ||
		!strings.Contains(err.Error(), "completed action lacks a complete matching physical-isolation receipt") {
		t.Fatalf("corrupt completed receipt was accepted: %v", err)
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

// A Service-selector partition mutates the StatefulSet selector label on the
// old Pod. Kubernetes consequently releases the controller reference and a
// scale-to-zero no longer removes that process. After the exact process-bound
// watchdog grace, the operator must delete that orphan without risking a
// same-name replacement.
func TestReconcilePhysicalIsolationDeletesExactOrphanAfterWatchdogBarrier(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cluster, action := validPhysicalIsolationReceiptFixture(now)
	action.AdminJobPhase = haAdminJobPhaseRunning
	action.CompletedAt = nil
	receipt := action.PhysicalIsolationReceipt
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
	proof := receipt.WatchdogProof
	orphan := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: proof.PodName, Namespace: cluster.Namespace, UID: types.UID(proof.PodUID), ResourceVersion: "pod-rv",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: proof.ContainerName, ContainerID: proof.ContainerID, RestartCount: proof.ContainerRestartCount,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: proof.ContainerStartedAt}},
			}},
		},
	}
	reconciler := testHAReconciler(t, cluster, sts, lease, orphan)
	reconciler.BoundaryReader = haTestResourceVersionReader{Reader: reconciler.Client, listResourceVersion: "pods-with-orphan"}
	reconciler.Now = func() time.Time { return now.Add(20 * time.Second) }
	monotonicNow := now
	reconciler.MonotonicNow = func() time.Time { return monotonicNow }

	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); err != nil {
		t.Fatalf("start local watchdog barrier: %v", err)
	}
	current := &corev1.Pod{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: orphan.Name, Namespace: orphan.Namespace}, current); err != nil {
		t.Fatalf("orphan disappeared before watchdog barrier: %v", err)
	}

	monotonicNow = monotonicNow.Add(10 * time.Second)
	if err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster); !errors.Is(err, errHAStatusCheckpointed) {
		t.Fatalf("exact orphan deletion was not a reconciliation barrier: %v", err)
	}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: orphan.Name, Namespace: orphan.Namespace}, current); !apierrors.IsNotFound(err) {
		t.Fatalf("exact self-fenced orphan survived cleanup: %v", err)
	}
	got := cluster.Status.HAStatus.PlannedActions[0]
	if got.AdminJobPhase != haAdminJobPhaseRunning || got.PhysicalIsolationReceipt.ObservedAt != nil || got.CompletedAt != nil {
		t.Fatalf("orphan deletion skipped the required post-delete observation checkpoint: %#v", got)
	}
}

func TestPhysicalIsolationOrphanCleanupRefusesSameNameReplacement(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cluster, action := validPhysicalIsolationReceiptFixture(now)
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: action.PhysicalIsolationReceipt.WatchdogProof.PodName, Namespace: cluster.Namespace,
			UID: types.UID("replacement-pod-uid"), ResourceVersion: "replacement-rv",
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	reconciler := testHAReconciler(t, cluster, replacement)
	deleted, err := reconciler.deletePhysicallyIsolatedOrphan(
		context.Background(),
		&action,
		&corev1.PodList{Items: []corev1.Pod{*replacement}},
	)
	if err != nil || deleted {
		t.Fatalf("same-name replacement matched old-process cleanup: deleted=%v err=%v", deleted, err)
	}
	current := &corev1.Pod{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: replacement.Name, Namespace: replacement.Namespace}, current); err != nil {
		t.Fatalf("same-name replacement was deleted: %v", err)
	}
}

// The irreversible intent must contain all evidence needed after the old Pod
// is gone. Publishing a partial receipt and then scaling to zero creates a
// permanent failover deadlock because the process identity cannot be recovered.
func TestReconcilePhysicalIsolationRefusesIntentWithoutExactWatchdogProof(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cluster, action := validPhysicalIsolationReceiptFixture(now)
	action.AdminJobName = ""
	action.AdminJobPhase = haAdminJobPhaseWaitingDependency
	action.CompletedAt = nil
	action.PhysicalIsolationReceipt = nil
	cluster.Status.HAStatus.PrimaryWatchdogProof = nil
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{action}

	sts, lease := currentPhysicalIsolationObjects(cluster, now)
	one := int32(1)
	sts.Spec.Replicas = &one
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: sts.Generation, Replicas: 1, CurrentReplicas: 1, ReadyReplicas: 1}
	reconciler := testHAReconciler(t, cluster, sts, lease)
	reconciler.BoundaryReader = haTestResourceVersionReader{Reader: reconciler.Client, listResourceVersion: "pods-current"}
	reconciler.Now = func() time.Time { return now }

	err := reconciler.reconcileHAFormerPrimaryIsolation(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "exact pre-transfer runtime watchdog proof is unavailable") {
		t.Fatalf("partial irreversible intent was not rejected: %v", err)
	}
	storedSTS := &appsv1.StatefulSet{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}, storedSTS); err != nil {
		t.Fatalf("read StatefulSet after rejected intent: %v", err)
	}
	if storedSTS.Spec.Replicas == nil || *storedSTS.Spec.Replicas != 1 {
		t.Fatalf("old writer was scaled without a complete intent receipt: %#v", storedSTS.Spec.Replicas)
	}
	storedCluster := &antflyv1.AntflyCluster{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, storedCluster); err != nil {
		t.Fatalf("read cluster after rejected intent: %v", err)
	}
	if storedCluster.Status.HAStatus.PlannedActions[0].PhysicalIsolationReceipt != nil {
		t.Fatalf("partial physical-isolation receipt was persisted: %#v", storedCluster.Status.HAStatus.PlannedActions[0])
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
