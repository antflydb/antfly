// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	adminsdk "github.com/antflydb/antfly/go/pkg/sdk/admin"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestHAPrimaryRouteRequiresFreshUncachedRuntimeAndLeaseProof(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryWatchdogProof = validRouteWatchdogProof(now)

	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	currentPod := validRouteWatchdogPod(now, "promoted-pod-uid")
	currentLease := haFenceLease(cluster, now, haFencingLeaseDefaultDurationSeconds, 4, "standby-a")
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, currentPod.DeepCopy(), currentLease.DeepCopy()).Build()
	boundary := fake.NewClientBuilder().WithScheme(scheme).WithObjects(currentPod.DeepCopy(), currentLease.DeepCopy()).Build()
	reconciler := &AntflyClusterReconciler{Client: cached, BoundaryReader: boundary, Now: func() time.Time { return now }}

	ready, err := reconciler.haPromotedRuntimeWatchdogReady(context.Background(), cluster, "standby-a", 4)
	if err != nil || !ready {
		t.Fatalf("fresh exact promoted runtime proof rejected: ready=%v err=%v", ready, err)
	}
	ready, err = reconciler.haCurrentLeaseAuthorizesRoute(context.Background(), cluster, "standby-a", 4)
	if err != nil || !ready {
		t.Fatalf("fresh exact uncached Lease rejected: ready=%v err=%v", ready, err)
	}

	// A stale cache still says the promoted node holds the Lease. The safety
	// boundary sees a later holder and must win.
	staleBoundaryLease := currentLease.DeepCopy()
	other := "other-node"
	transition := int32(5)
	staleBoundaryLease.Spec.HolderIdentity = &other
	staleBoundaryLease.Spec.LeaseTransitions = &transition
	boundary = fake.NewClientBuilder().WithScheme(scheme).WithObjects(currentPod.DeepCopy(), staleBoundaryLease).Build()
	reconciler.BoundaryReader = boundary
	ready, err = reconciler.haCurrentLeaseAuthorizesRoute(context.Background(), cluster, "standby-a", 4)
	if err != nil || ready {
		t.Fatalf("cached authorized Lease overrode uncached transfer: ready=%v err=%v", ready, err)
	}

	// Recreating the candidate Pod invalidates the process-bound proof even if
	// the stable Pod name and all labels are reused.
	recreated := validRouteWatchdogPod(now, "replacement-pod-uid")
	boundary = fake.NewClientBuilder().WithScheme(scheme).WithObjects(recreated, currentLease.DeepCopy()).Build()
	reconciler.BoundaryReader = boundary
	ready, err = reconciler.haPromotedRuntimeWatchdogReady(context.Background(), cluster, "standby-a", 4)
	if err != nil || ready {
		t.Fatalf("recreated Pod satisfied stale process proof: ready=%v err=%v", ready, err)
	}

	// A structurally exact proof is still unusable after its runtime-declared
	// maximum fence latency.
	boundary = fake.NewClientBuilder().WithScheme(scheme).WithObjects(currentPod.DeepCopy(), currentLease.DeepCopy()).Build()
	reconciler.BoundaryReader = boundary
	reconciler.Now = func() time.Time { return now.Add(10*time.Second + time.Millisecond) }
	ready, err = reconciler.haPromotedRuntimeWatchdogReady(context.Background(), cluster, "standby-a", 4)
	if err != nil || ready {
		t.Fatalf("expired promoted runtime proof authorized route: ready=%v err=%v", ready, err)
	}
}

// This is the differential red/green oracle for the cached-authority race. It
// intentionally exercises the unchanged reconciliation entrypoint so it also
// runs against the pre-fix revision: that revision updates the Service from the
// cached Lease, while the fixed revision leaves it unrouted.
func TestReconcileHAPrimaryRouteRejectsCachedAuthorityWhenBoundaryMoved(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryWatchdogProof = validRouteWatchdogProof(now)
	cluster.Status.HAStatus.LastPromotion = &antflyv1.HAPromotionStatus{
		OldPrimaryID: "primary-a", PromotedStandbyID: "standby-a",
		ClusterID: 100, ParentTimelineID: 4, ParentEpoch: 6, NewTimelineID: 5, NewEpoch: 7,
		SwitchLSN: 12, RequiredLSN: 12, ObservedLSN: 12,
		FenceAuthority: antflyv1.HAFencingAuthorityKubernetesLease, FenceGeneration: 4, FenceToken: "fence-token",
	}
	cluster.Status.HAStatus.PlannedActions = []antflyv1.HAPlannedActionStatus{{
		Kind: string(haActionUpdatePrimaryRoute), RouteFrom: "primary-a", RouteTo: "standby-a", TargetLSN: 12,
		FenceAuthority: antflyv1.HAFencingAuthorityKubernetesLease, FenceGeneration: 4,
	}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-public-api", Namespace: cluster.Namespace}}
	pod := validRouteWatchdogPod(now, "promoted-pod-uid")
	cachedLease := haFenceLease(cluster, now, haFencingLeaseDefaultDurationSeconds, 4, "standby-a")
	boundaryLease := cachedLease.DeepCopy()
	other := "other-node"
	transition := int32(5)
	boundaryLease.Spec.HolderIdentity = &other
	boundaryLease.Spec.LeaseTransitions = &transition

	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, service, pod.DeepCopy(), cachedLease).Build()
	boundary := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy(), boundaryLease).Build()
	reconciler := &AntflyClusterReconciler{Client: cached, BoundaryReader: boundary, Now: func() time.Time { return now }}
	if err := reconciler.reconcileHAPrimaryRoute(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile route: %v", err)
	}
	observed := &corev1.Service{}
	if err := cached.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed); err != nil {
		t.Fatal(err)
	}
	if observed.Annotations[haPrimaryRouteTargetAnnotation] != "" {
		t.Fatalf("stale cached Lease routed traffic after uncached holder moved: annotations=%v", observed.Annotations)
	}
}

func TestHAWatchdogAuthorityProofRequiresObservedSelfHolder(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	raw := &adminsdk.HALeaseWatchdogProof{
		CapabilityVersion:        1,
		Active:                   true,
		AuthorityGranted:         true,
		AuthorityRemainingMs:     10_000,
		LeaseName:                "topology-ha-fence",
		LeaseNamespace:           "default",
		StableTopologyId:         "topology-anchor-uid",
		LocalNodeId:              "standby-a",
		ObservedHolderNodeId:     "primary-a",
		PodUid:                   "promoted-pod-uid",
		ProcessBootId:            strings.Repeat("a", 64),
		ObservedLeaseTransitions: 4,
		MaxFenceLatencyMs:        10_000,
	}
	now := time.Now()
	if _, err := haWatchdogProofFromAdmin(raw, cluster, "standby-a", true, now, now); err == nil {
		t.Fatal("authority proof with a different observed Lease holder was accepted")
	}
}

func TestHAWatchdogAuthorityProofSubtractsDelayedResponseRTT(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	raw := &adminsdk.HALeaseWatchdogProof{
		CapabilityVersion:        1,
		Active:                   true,
		AuthorityGranted:         true,
		AuthorityRemainingMs:     10_000,
		LeaseName:                "topology-ha-fence",
		LeaseNamespace:           "default",
		StableTopologyId:         "topology-anchor-uid",
		LocalNodeId:              "standby-a",
		ObservedHolderNodeId:     "standby-a",
		PodUid:                   "promoted-pod-uid",
		ProcessBootId:            strings.Repeat("a", 64),
		ObservedLeaseTransitions: 4,
		MaxFenceLatencyMs:        10_000,
	}
	started := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	if _, err := haWatchdogProofFromAdmin(raw, cluster, "standby-a", true, started, started.Add(9800*time.Millisecond)); err == nil {
		t.Fatal("delayed admin response refreshed already-expired runtime authority")
	}
	proof, err := haWatchdogProofFromAdmin(raw, cluster, "standby-a", true, started, started.Add(time.Second))
	if err != nil {
		t.Fatalf("bounded admin response rejected: %v", err)
	}
	if proof.AuthorityRemainingMS != 8_750 {
		t.Fatalf("operator did not subtract RTT and margin: remaining=%d", proof.AuthorityRemainingMS)
	}
	if !proof.ObservedAt.Time.Equal(started) {
		t.Fatalf("SAFETY: proof was anchored after the request and can be combined with a same-Pod replacement process: %s", proof.ObservedAt.Time)
	}
}

func validRouteWatchdogProof(now time.Time) *antflyv1.HAWatchdogProofStatus {
	return &antflyv1.HAWatchdogProofStatus{
		CapabilityVersion:        1,
		Active:                   true,
		AuthorityGranted:         true,
		AuthorityRemainingMS:     9_750,
		LeaseName:                "topology-ha-fence",
		LeaseNamespace:           "default",
		TopologyID:               "topology-anchor-uid",
		LocalNodeID:              "standby-a",
		ObservedHolderNodeID:     "standby-a",
		PodUID:                   "promoted-pod-uid",
		ProcessBootID:            strings.Repeat("a", 64),
		ObservedLeaseTransitions: 4,
		MaxFenceLatencyMS:        10_000,
		ObservedAt:               metav1.NewTime(now),
	}
}

func validRouteWatchdogPod(now time.Time, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "standby-a-0", Namespace: "default", UID: types.UID(uid)},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "antfly",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.NewTime(now.Add(-time.Minute)),
				}},
			}},
		},
	}
}
