package controllers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestLeaseRenewalClockIgnoresStatusOnlyEvents(t *testing.T) {
	predicate := haLeaseRenewalEventPredicate()
	oldCluster := haClusterWithAutomaticKubernetesLeaseFailover()
	oldCluster.Generation = 7
	updatedStatus := oldCluster.DeepCopy()
	updatedStatus.Status.HAStatus = caughtUpHAStatus()
	if predicate.Update(event.UpdateEvent{ObjectOld: oldCluster, ObjectNew: updatedStatus}) {
		t.Fatal("status-only update bypassed the dedicated renewal cadence")
	}

	updatedSpec := updatedStatus.DeepCopy()
	updatedSpec.Generation++
	if !predicate.Update(event.UpdateEvent{ObjectOld: updatedStatus, ObjectNew: updatedSpec}) {
		t.Fatal("HA spec generation change did not wake Lease renewal immediately")
	}
}

func TestFullReconcileIgnoresStatusFeedbackButObservesDesiredState(t *testing.T) {
	predicate := antflyClusterDesiredStateEventPredicate()
	oldCluster := haClusterWithAutomaticKubernetesLeaseFailover()
	oldCluster.Generation = 7
	updatedStatus := oldCluster.DeepCopy()
	updatedStatus.Status.HAStatus = caughtUpHAStatus()
	if predicate.Update(event.UpdateEvent{ObjectOld: oldCluster, ObjectNew: updatedStatus}) {
		t.Fatal("operator-owned status update re-enqueued the full reconciler")
	}

	cases := map[string]func(*antflyv1.AntflyCluster){
		"spec generation": func(cluster *antflyv1.AntflyCluster) { cluster.Generation++ },
		"label": func(cluster *antflyv1.AntflyCluster) {
			cluster.Labels = map[string]string{"cloud.antfly.io/instance-id": "instance-a"}
		},
		"annotation": func(cluster *antflyv1.AntflyCluster) {
			cluster.Annotations = map[string]string{"cloud.antfly.io/ha-topology-generation": "2"}
		},
		"finalizer": func(cluster *antflyv1.AntflyCluster) {
			cluster.Finalizers = []string{"storage.antfly.io/seed-retention"}
		},
		"deletion": func(cluster *antflyv1.AntflyCluster) {
			now := metav1.Now()
			cluster.DeletionTimestamp = &now
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			updated := updatedStatus.DeepCopy()
			mutate(updated)
			if !predicate.Update(event.UpdateEvent{ObjectOld: updatedStatus, ObjectNew: updated}) {
				t.Fatal("desired-state change did not wake full reconciliation")
			}
		})
	}
}

func TestReconcileHAFencingLeaseRejectsStaleControllersAcrossSuccessiveTransfers(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	clusterA := haClusterWithAutomaticKubernetesLeaseFailover()
	clusterA.UID = types.UID("cluster-a-uid")
	clusterA.Status.HAStatus = caughtUpHAStatus()
	clusterA.Status.HAStatus.PrimaryAdminReachable = false
	clusterA.Status.HAStatus.PrimaryAdminLastError = "primary admin timeout"
	clusterA.Status.HAStatus.Standbys[0].WatchdogProof = candidateLeaseProof(now, "standby-a", "primary-a", 1)
	lease := haFenceLease(clusterA, now.Add(-time.Second), 30, 1, "primary-a")
	podA := candidateLeasePod(now, "standby-a-pod-uid")
	podC := candidateLeasePod(now, "standby-c-pod-uid")
	reconciler := testHAReconciler(t, lease, podA, podC)
	reconciler.Now = func() time.Time { return now }

	if err := reconciler.reconcileHAFencingLease(context.Background(), clusterA); err != nil {
		t.Fatalf("A -> B transfer: %v", err)
	}
	assertLeaseHolderAndTransition(t, reconciler, "standby-a", 2)

	// A may keep B alive only through the exact committed handoff bridge.
	// The bridge must survive B's transient inactive proof while Colony is
	// publishing B's successor CR; otherwise the Lease expires during an
	// otherwise successful failover and B self-fences.
	clusterA.Status.HAStatus.PrimaryAdminLastError = "HA Lease watchdog is not active for node standby-a"
	beforeHandoff := getOwnershipTestLease(t, reconciler).Spec.RenewTime.DeepCopy()
	now = now.Add(time.Second)
	if err := reconciler.reconcileHAFencingLease(context.Background(), clusterA); err != nil {
		t.Fatalf("A handoff renewal for B: %v", err)
	}
	assertLeaseHolderAndTransition(t, reconciler, "standby-a", 2)
	afterHandoff := getOwnershipTestLease(t, reconciler)
	if afterHandoff.Spec.RenewTime == nil || !afterHandoff.Spec.RenewTime.Time.Equal(now) ||
		afterHandoff.Spec.RenewTime.Equal(beforeHandoff) {
		t.Fatalf("exact inactive-successor handoff did not advance renewTime: before=%s after=%s", beforeHandoff, afterHandoff.Spec.RenewTime)
	}

	clusterB := clusterA.DeepCopy()
	clusterB.UID = types.UID("cluster-b-uid")
	clusterB.Spec.HighAvailability.Runtime.NodeID = "standby-a"
	clusterB.Spec.HighAvailability.Identity.CurrentPrimaryID = "standby-a"
	clusterB.Status.HAStatus = &antflyv1.HAStatus{PrimaryAdminReachable: true, PrimaryLSN: 12}
	now = now.Add(time.Second)
	if err := reconciler.reconcileHAFencingLease(context.Background(), clusterB); err != nil {
		t.Fatalf("B takes over renewal: %v", err)
	}
	observed := getOwnershipTestLease(t, reconciler)
	if observed.Annotations[haFencingLeaseAnnotationTransferOriginUID] != "" {
		t.Fatalf("B takeover did not close A handoff bridge: %#v", observed.Annotations)
	}

	clusterB.Spec.HighAvailability.Standbys = []antflyv1.HAStandbySpec{{
		Name: "standby-c", AdminURL: "http://standby-c-ha.default.svc:8081", RouteSelector: haTestRouteSelector("standby-c"),
	}}
	clusterB.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryLSN: 12, PrimaryAdminReachable: false, PrimaryAdminLastError: "primary admin timeout", PrimaryAdminFailureThresholdMet: true,
		Standbys: []antflyv1.HAStandbyStatus{{
			Name: "standby-c", SlotName: "standby-c", Active: true, ReceivedLSN: 12, AppliedLSN: 12, SafeReadLSN: 12,
			CanServeSafeReads: true, Status: "healthy", WatchdogProof: candidateLeaseProof(now, "standby-c", "standby-a", 2),
		}},
	}
	now = now.Add(time.Second)
	clusterB.Status.HAStatus.Standbys[0].WatchdogProof.ObservedAt = metav1.NewTime(now)
	if err := reconciler.reconcileHAFencingLease(context.Background(), clusterB); err != nil {
		t.Fatalf("B -> C transfer: %v", err)
	}
	assertLeaseHolderAndTransition(t, reconciler, "standby-c", 3)

	// A still wants B and must not roll C back to its stale candidate.
	now = now.Add(time.Second)
	if err := reconciler.reconcileHAFencingLease(context.Background(), clusterA); err != nil {
		t.Fatalf("stale A reconcile: %v", err)
	}
	assertLeaseHolderAndTransition(t, reconciler, "standby-c", 3)

	clusterC := clusterB.DeepCopy()
	clusterC.UID = types.UID("cluster-c-uid")
	clusterC.Spec.HighAvailability.Runtime.NodeID = "standby-c"
	clusterC.Spec.HighAvailability.Identity.CurrentPrimaryID = "standby-c"
	clusterC.Status.HAStatus = &antflyv1.HAStatus{PrimaryAdminReachable: true, PrimaryLSN: 12}
	if err := reconciler.reconcileHAFencingLease(context.Background(), clusterC); err != nil {
		t.Fatalf("C takes over renewal: %v", err)
	}

	// Once C acknowledges ownership, stale B loses even its handoff-renewal right.
	before := getOwnershipTestLease(t, reconciler).Spec.RenewTime.DeepCopy()
	now = now.Add(time.Second)
	if err := reconciler.reconcileHAFencingLease(context.Background(), clusterB); err != nil {
		t.Fatalf("stale B reconcile: %v", err)
	}
	after := getOwnershipTestLease(t, reconciler)
	assertLeaseHolderAndTransition(t, reconciler, "standby-c", 3)
	if !after.Spec.RenewTime.Equal(before) {
		t.Fatalf("stale B renewed C after C takeover: before=%s after=%s", before, after.Spec.RenewTime)
	}
}

func candidateLeaseProof(now time.Time, nodeID, observedHolder string, transition int32) *antflyv1.HAWatchdogProofStatus {
	return &antflyv1.HAWatchdogProofStatus{
		CapabilityVersion: 1, Active: true, AuthorityGranted: false, AuthorityRemainingMS: 0,
		LeaseName: "topology-ha-fence", LeaseNamespace: "default", TopologyID: "topology-anchor-uid",
		LocalNodeID: nodeID, ObservedHolderNodeID: observedHolder, PodUID: nodeID + "-pod-uid",
		ProcessBootID: strings.Repeat("a", 64), ObservedLeaseTransitions: transition, MaxFenceLatencyMS: 10_000,
		ObservedAt: metav1.NewTime(now),
	}
}

func candidateLeasePod(now time.Time, uid string) *corev1.Pod {
	started := metav1.NewTime(now.Add(-time.Minute))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: string(uid), Namespace: "default", UID: types.UID(uid)},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "antfly", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: started}},
		}}},
	}
}

func TestDedicatedLeaseRenewalAdvancesUnchangedHolderFromFreshRuntimeProof(t *testing.T) {
	now := time.Now().UTC()
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	proof := candidateLeaseProof(now, "primary-a", "primary-a", 1)
	proof.AuthorityGranted = true
	proof.AuthorityRemainingMS = 8_000
	cluster.Status.HAStatus.PrimaryWatchdogProof = proof
	lease := haFenceLease(cluster, now.Add(-time.Second), 10, 1, "primary-a")
	lease.Annotations[haFencingLeaseAnnotationProcessBootID] = proof.ProcessBootID
	reconciler := testHAReconciler(t, cluster, lease, candidateLeasePod(now, "primary-a-pod-uid"))
	reconciler.Now = func() time.Time { return now }

	if err := reconciler.renewCurrentHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("dedicated renewal: %v", err)
	}
	renewed := getOwnershipTestLease(t, reconciler)
	// Lease renewTime is an RFC3339 microsecond timestamp. Some fake-client
	// serializers preserve nanoseconds in-process while CI round-trips through
	// the API representation, so compare at the wire precision.
	wantRenewTime := metav1.NewMicroTime(now.Truncate(time.Microsecond))
	if renewed.Spec.RenewTime == nil || !renewed.Spec.RenewTime.Equal(&wantRenewTime) {
		t.Fatalf("expected dedicated path to publish strictly newer renewal %s, got %#v", wantRenewTime.Time, renewed.Spec.RenewTime)
	}
	if renewed.Spec.HolderIdentity == nil || *renewed.Spec.HolderIdentity != "primary-a" ||
		renewed.Spec.LeaseTransitions == nil || *renewed.Spec.LeaseTransitions != 1 {
		t.Fatalf("dedicated renewal changed holder authority: %#v", renewed.Spec)
	}
}

func TestDedicatedLeaseRenewalPreservesPendingBootstrapCompareBoundary(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	proof := candidateLeaseProof(now, "primary-a", "primary-a", 1)
	proof.AuthorityGranted = true
	proof.AuthorityRemainingMS = 8_000
	cluster.Status.HAStatus.PrimaryWatchdogProof = proof
	leaseRenewedAt := now.Add(-time.Second)
	lease := haFenceLease(cluster, leaseRenewedAt, 10, 1, "primary-a")
	lease.Annotations[haFencingLeaseAnnotationProcessBootID] = proof.ProcessBootID
	receipt := haFencingLeaseBootstrapReceipt("primary-a", 1, proof.ProcessBootID)
	lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt] = receipt
	reconciler := testHAReconciler(t, cluster, lease, candidateLeasePod(now, "primary-a-pod-uid"))
	reconciler.Now = func() time.Time { return now }

	if err := reconciler.renewCurrentHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("dedicated renewal with pending bootstrap receipt: %v", err)
	}
	observed := getOwnershipTestLease(t, reconciler)
	if observed.Spec.RenewTime == nil || !observed.Spec.RenewTime.Time.Equal(leaseRenewedAt) {
		t.Fatalf("dedicated renewal moved the full reconciler's compare boundary: %#v", observed.Spec.RenewTime)
	}
	if observed.Annotations[haFencingLeaseAnnotationBootstrapReceipt] != receipt {
		t.Fatalf("dedicated renewal consumed another controller's bootstrap receipt: %#v", observed.Annotations)
	}
}

func TestDedicatedLeaseRenewalAdvancesOnlyExactCommittedHandoff(t *testing.T) {
	for _, tt := range []struct {
		name        string
		receipt     string
		wantRenewed bool
	}{
		{name: "before successor process binding", wantRenewed: true},
		{
			name: "exact bound successor process",
			receipt: haFencingLeaseBootstrapReceipt(
				"standby-a", 2, strings.Repeat("b", 64),
			),
			wantRenewed: true,
		},
		{
			name: "mismatched bound successor process",
			receipt: haFencingLeaseBootstrapReceipt(
				"standby-a", 2, strings.Repeat("c", 64),
			),
			wantRenewed: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			cluster := haClusterWithAutomaticKubernetesLeaseFailover()
			cluster.Status.HAStatus = caughtUpHAStatus()
			leaseRenewedAt := now.Add(-time.Second)
			lease := haFenceLease(cluster, leaseRenewedAt, 30, 2, "standby-a")
			lease.Annotations[haFencingLeaseAnnotationTransferCommitted] = "true"
			lease.Annotations[haFencingLeaseAnnotationFormerHolder] = "primary-a"
			lease.Annotations[haFencingLeaseAnnotationTransferOriginUID] = string(cluster.UID)
			lease.Annotations[haFencingLeaseAnnotationCommittedTransition] = "2"
			lease.Annotations[haFencingLeaseAnnotationProcessBootID] = strings.Repeat("b", 64)
			if tt.receipt != "" {
				lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt] = tt.receipt
			}
			reconciler := testHAReconciler(t, cluster, lease)
			reconciler.Now = func() time.Time { return now }

			if err := reconciler.renewCurrentHAFencingLease(context.Background(), cluster); err != nil {
				t.Fatalf("dedicated committed-handoff renewal: %v", err)
			}
			observed := getOwnershipTestLease(t, reconciler)
			wantRenewTime := leaseRenewedAt
			if tt.wantRenewed {
				wantRenewTime = now
			}
			if observed.Spec.RenewTime == nil || !observed.Spec.RenewTime.Time.Equal(wantRenewTime) {
				t.Fatalf("renewTime = %#v, want %s", observed.Spec.RenewTime, wantRenewTime)
			}
			if observed.Spec.HolderIdentity == nil || *observed.Spec.HolderIdentity != "standby-a" ||
				observed.Spec.LeaseTransitions == nil || *observed.Spec.LeaseTransitions != 2 ||
				observed.Annotations[haFencingLeaseAnnotationTransferCommitted] != "true" ||
				observed.Annotations[haFencingLeaseAnnotationProcessBootID] != strings.Repeat("b", 64) {
				t.Fatalf("dedicated handoff renewal mutated authority: %#v", observed)
			}
		})
	}
}

func TestDedicatedLeaseRenewalUsesAuthenticatedWatchdogProofEndpoint(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	t.Setenv("TEST_HA_WATCHDOG_TOKEN", "watchdog-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/admin/v1/ha/watchdog-proof" {
			t.Fatalf("request = %s %s, want dedicated watchdog proof endpoint", req.Method, req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer watchdog-token" {
			t.Fatalf("Authorization = %q, want Bearer watchdog-token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"schema_version":1,"proof":{"capability_version":1,"active":true,"authority_granted":true,"authority_remaining_ms":8000,"lease_name":"topology-ha-fence","lease_namespace":"default","stable_topology_id":"topology-anchor-uid","local_node_id":"primary-a","observed_holder_node_id":"primary-a","pod_uid":"primary-a-pod-uid","process_boot_id":"%s","observed_lease_transitions":1,"max_fence_latency_ms":10000}}`, strings.Repeat("a", 64))
	}))
	defer server.Close()

	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Spec.HighAvailability.Admin.PrimaryURL = server.URL
	cluster.Spec.HighAvailability.Admin.TokenEnvVar = "TEST_HA_WATCHDOG_TOKEN"
	lease := haFenceLease(cluster, now.Add(-time.Second), 10, 1, "primary-a")
	lease.Annotations[haFencingLeaseAnnotationProcessBootID] = strings.Repeat("a", 64)
	reconciler := testHAReconciler(t, cluster, lease, candidateLeasePod(now, "primary-a-pod-uid"))
	reconciler.HTTPClient = server.Client()
	reconciler.Now = func() time.Time { return now }

	if err := reconciler.observeHACurrentPrimaryWatchdogProof(context.Background(), cluster); err != nil {
		t.Fatalf("observe dedicated watchdog proof: %v", err)
	}
	if err := reconciler.renewCurrentHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("renew from dedicated watchdog proof: %v", err)
	}
	renewed := getOwnershipTestLease(t, reconciler)
	if renewed.Spec.RenewTime == nil || !renewed.Spec.RenewTime.Time.Equal(now) {
		t.Fatalf("renewTime = %#v, want %s", renewed.Spec.RenewTime, now)
	}
}

func getOwnershipTestLease(t *testing.T, reconciler *AntflyClusterReconciler) *coordinationv1.Lease {
	t.Helper()
	lease := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: "topology-ha-fence", Namespace: "default"}, lease); err != nil {
		t.Fatal(err)
	}
	return lease
}

func assertLeaseHolderAndTransition(t *testing.T, reconciler *AntflyClusterReconciler, holder string, transition int32) {
	t.Helper()
	lease := getOwnershipTestLease(t, reconciler)
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != holder ||
		lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions != transition {
		t.Fatalf("lease = holder %#v transition %#v, want %q/%d", lease.Spec.HolderIdentity, lease.Spec.LeaseTransitions, holder, transition)
	}
}
