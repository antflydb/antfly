// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

package controllers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestInitialPrimaryLeaseBootstrapRequiresPendingProofThenFullAuthority(t *testing.T) {
	leaseTime := time.Date(2026, 7, 16, 4, 21, 14, 680204000, time.UTC)
	now := leaseTime.Add(time.Second)
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.Identity.ShardID = 10
	cluster.Spec.HighAvailability.Identity.TableID = 20
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 1}
	lease := haFenceLease(cluster, leaseTime, haFencingLeaseDefaultDurationSeconds, 1, "primary-a")
	lease.Annotations[haFencingLeaseAnnotationPrimaryLSN] = "0"
	cluster.Status.HAStatus = &antflyv1.HAStatus{}
	pod := candidateLeasePod(now, "primary-a-pod-uid")

	authorityGranted := false
	reconciler := testHAReconciler(t, lease, pod)
	reconciler.Now = func() time.Time { return now }
	reconciler.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/admin/v1/ha/primary/status" {
			t.Fatalf("unexpected primary status path %q", req.URL.Path)
		}
		lsn := uint64(77)
		authorityRemainingMS := uint64(0)
		if authorityGranted {
			lsn = 0
			authorityRemainingMS = 10_000
		}
		body := fmt.Sprintf(`{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":4,"epoch":6},"current_lsn":%d,"slots":[],"retention":{"primary_lsn":%d,"oldest_restart_lsn":%d,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0},"lease_watchdog":{"capability_version":1,"active":true,"authority_granted":%t,"authority_remaining_ms":%d,"lease_name":"topology-ha-fence","lease_namespace":"default","stable_topology_id":"topology-anchor-uid","local_node_id":"primary-a","observed_holder_node_id":"primary-a","pod_uid":"primary-a-pod-uid","process_boot_id":"%s","observed_lease_transitions":1,"max_fence_latency_ms":10000}}}`, lsn, lsn, lsn, authorityGranted, authorityRemainingMS, strings.Repeat("a", 64))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})}

	if err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster); err == nil {
		t.Fatal("pending watchdog authority was accepted as authoritative primary health")
	}
	status := cluster.Status.HAStatus
	if status.PrimaryAdminReachable || status.PrimaryLSN != 0 || len(status.Standbys) != 0 {
		t.Fatalf("pending proof merged authoritative primary state: %#v", status)
	}
	if status.PrimaryWatchdogProof == nil || !status.PrimaryWatchdogProof.Active ||
		status.PrimaryWatchdogProof.AuthorityGranted || status.PrimaryWatchdogProof.LocalNodeID != "primary-a" {
		t.Fatalf("authenticated pending capability proof was not retained: %#v", status.PrimaryWatchdogProof)
	}

	now = leaseTime.Add(2 * time.Second)
	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("renew initial zero-boundary Lease from pending capability proof: %v", err)
	}
	renewed := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, renewed); err != nil {
		t.Fatal(err)
	}
	if renewed.Spec.RenewTime == nil || !renewed.Spec.RenewTime.Time.Equal(now) {
		t.Fatalf("initial Lease did not advance from T to T2: old=%s new=%#v", leaseTime, renewed.Spec.RenewTime)
	}
	if renewed.Spec.HolderIdentity == nil || *renewed.Spec.HolderIdentity != "primary-a" ||
		renewed.Spec.LeaseTransitions == nil || *renewed.Spec.LeaseTransitions != 1 ||
		renewed.Annotations[haFencingLeaseAnnotationPrimaryLSN] != "0" {
		t.Fatalf("bootstrap renewal changed holder, generation, or zero boundary: %#v", renewed)
	}
	wantReceipt := haFencingLeaseBootstrapReceipt("primary-a", 1, strings.Repeat("a", 64))
	if renewed.Annotations[haFencingLeaseAnnotationBootstrapReceipt] != wantReceipt {
		t.Fatalf("bootstrap renewal did not persist its exact process/generation receipt: %#v", renewed.Annotations)
	}
	if status.Fencing.Ready {
		t.Fatalf("pending capability renewal became authoritative fencing health: %#v", status.Fencing)
	}
	reconciler.updateHAStatusAndConditions(cluster)
	if status.AutomaticPromotionAllowed {
		t.Fatal("pending capability renewal enabled automatic promotion")
	}
	for _, action := range status.PlannedActions {
		if action.Kind == string(haActionAcquireFence) || action.Kind == string(haActionAssessPromotion) || action.Kind == string(haActionPromoteStandby) || action.Kind == string(haActionUpdatePrimaryRoute) {
			t.Fatalf("pending capability renewal planned lifecycle action %#v", action)
		}
	}

	// The real reconcile loop observes admin status again before every Lease
	// reconcile. A fresh operator ObservedAt for the same still-pending runtime
	// must not reopen the one-shot exception.
	now = leaseTime.Add(3 * time.Second)
	if err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster); err == nil {
		t.Fatal("second pending watchdog observation was accepted as authoritative primary health")
	}
	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("repeat pending-authority reconcile: %v", err)
	}
	afterRepeat := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, afterRepeat); err != nil {
		t.Fatal(err)
	}
	if !afterRepeat.Spec.RenewTime.Equal(renewed.Spec.RenewTime) {
		t.Fatalf("pending capability proof renewed more than once: first=%s repeat=%s", renewed.Spec.RenewTime, afterRepeat.Spec.RenewTime)
	}

	// After T2 the runtime can grant normal authority. Only that full proof may
	// merge primary state and resume ordinary owner renewal/lifecycle gates.
	authorityGranted = true
	now = leaseTime.Add(4 * time.Second)
	if err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster); err != nil {
		t.Fatalf("full authority observation after T2: %v", err)
	}
	if !status.PrimaryAdminReachable || status.PrimaryLSN != 0 || status.PrimaryWatchdogProof == nil ||
		!status.PrimaryWatchdogProof.AuthorityGranted {
		t.Fatalf("full authority did not become authoritative primary health: %#v", status)
	}
	now = leaseTime.Add(5 * time.Second)
	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("normal renewal after full authority: %v", err)
	}
	authorized := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, authorized); err != nil {
		t.Fatal(err)
	}
	if authorized.Spec.RenewTime == nil || !authorized.Spec.RenewTime.Time.Equal(now) ||
		authorized.Annotations[haFencingLeaseAnnotationPrimaryLSN] != "0" ||
		authorized.Annotations[haFencingLeaseAnnotationBootstrapReceipt] != "" || !status.Fencing.Ready {
		t.Fatalf("normal authority did not resume after full proof: lease=%#v fencing=%#v", authorized, status.Fencing)
	}
}

func TestPositiveBoundaryPrimaryRestartBootstrapIsOneShot(t *testing.T) {
	leaseTime := time.Date(2026, 7, 16, 5, 0, 0, 0, time.UTC)
	for _, statusLSN := range []uint64{0, 17} {
		t.Run(fmt.Sprintf("status-lsn-%d", statusLSN), func(t *testing.T) {
			cluster := haClusterWithAutomaticKubernetesLeaseFailover()
			cluster.Spec.HighAvailability.Identity.ShardID = 10
			cluster.Spec.HighAvailability.Identity.TableID = 20
			cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 17}
			lease := haFenceLease(cluster, leaseTime, haFencingLeaseDefaultDurationSeconds, 1, "primary-a")
			lease.Spec.AcquireTime = &metav1.MicroTime{Time: leaseTime.Add(-time.Minute)}
			proofTime := leaseTime.Add(time.Second)
			cluster.Status.HAStatus = &antflyv1.HAStatus{
				PrimaryLSN:            statusLSN,
				PrimaryAdminLastError: "HA Lease watchdog authority is pending for node primary-a",
				PrimaryWatchdogProof:  candidateLeaseProof(proofTime, "primary-a", "primary-a", 1),
			}
			pod := candidateLeasePod(proofTime, "primary-a-pod-uid")
			reconciler := testHAReconciler(t, lease, pod)
			now := leaseTime.Add(2 * time.Second)
			reconciler.Now = func() time.Time { return now }

			if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
				t.Fatalf("positive-boundary restart bootstrap: %v", err)
			}
			renewed := &coordinationv1.Lease{}
			if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, renewed); err != nil {
				t.Fatal(err)
			}
			if renewed.Spec.RenewTime == nil || !renewed.Spec.RenewTime.Time.Equal(now) ||
				renewed.Annotations[haFencingLeaseAnnotationPrimaryLSN] != "17" ||
				renewed.Annotations[haFencingLeaseAnnotationBootstrapReceipt] == "" {
				t.Fatalf("positive bootstrap did not renew once while preserving scope: %#v", renewed)
			}
			if err := reconciler.observeHAFencingStatus(context.Background(), cluster); err != nil {
				t.Fatalf("observe pending positive bootstrap fencing: %v", err)
			}
			if cluster.Status.HAStatus.Fencing.Ready {
				t.Fatalf("positive pending bootstrap became authoritative: %#v", cluster.Status.HAStatus.Fencing)
			}

			// Simulate the next real admin observation: same live process and Lease
			// generation, but a fresh operator timestamp and still no authority.
			now = leaseTime.Add(3 * time.Second)
			cluster.Status.HAStatus.PrimaryWatchdogProof.ObservedAt = metav1.NewTime(now)
			if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
				t.Fatalf("repeat positive pending bootstrap: %v", err)
			}
			repeated := &coordinationv1.Lease{}
			if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, repeated); err != nil {
				t.Fatal(err)
			}
			if !repeated.Spec.RenewTime.Equal(renewed.Spec.RenewTime) {
				t.Fatalf("fresh pending proof bypassed durable one-shot receipt: first=%s repeat=%s", renewed.Spec.RenewTime, repeated.Spec.RenewTime)
			}

			// Full authority alone reopens the normal owner-renewal path and clears
			// the durable bootstrap receipt without changing the positive boundary.
			now = leaseTime.Add(4 * time.Second)
			proof := cluster.Status.HAStatus.PrimaryWatchdogProof
			proof.AuthorityGranted = true
			proof.AuthorityRemainingMS = 9_000
			proof.ObservedAt = metav1.NewTime(now)
			cluster.Status.HAStatus.PrimaryAdminReachable = true
			cluster.Status.HAStatus.PrimaryAdminLastError = ""
			now = leaseTime.Add(5 * time.Second)
			if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
				t.Fatalf("positive normal renewal after full authority: %v", err)
			}
			authorized := &coordinationv1.Lease{}
			if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, authorized); err != nil {
				t.Fatal(err)
			}
			if authorized.Spec.RenewTime == nil || !authorized.Spec.RenewTime.Time.Equal(now) ||
				authorized.Annotations[haFencingLeaseAnnotationPrimaryLSN] != "17" ||
				authorized.Annotations[haFencingLeaseAnnotationBootstrapReceipt] != "" ||
				!cluster.Status.HAStatus.Fencing.Ready {
				t.Fatalf("positive scope did not resume only after full authority: lease=%#v fencing=%#v", authorized, cluster.Status.HAStatus.Fencing)
			}
		})
	}
}

func TestPendingBootstrapReceiptAllowsReplacementProcessOnce(t *testing.T) {
	leaseTime := time.Date(2026, 7, 16, 5, 30, 0, 0, time.UTC)
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.Identity.ShardID = 10
	cluster.Spec.HighAvailability.Identity.TableID = 20
	cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 1}
	lease := haFenceLease(cluster, leaseTime, haFencingLeaseDefaultDurationSeconds, 1, "primary-a")
	lease.Annotations[haFencingLeaseAnnotationPrimaryLSN] = "0"
	lease.Annotations[haFencingLeaseAnnotationBootstrapReceipt] =
		haFencingLeaseBootstrapReceipt("primary-a", 1, strings.Repeat("a", 64))
	replacementObserved := leaseTime.Add(time.Second)
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		PrimaryAdminLastError: "HA Lease watchdog authority is pending for node primary-a",
		PrimaryWatchdogProof:  candidateLeaseProof(replacementObserved, "primary-a", "primary-a", 1),
	}
	cluster.Status.HAStatus.PrimaryWatchdogProof.ProcessBootID = strings.Repeat("b", 64)
	pod := candidateLeasePod(replacementObserved, "primary-a-pod-uid")
	reconciler := testHAReconciler(t, lease, pod)
	now := leaseTime.Add(2 * time.Second)
	reconciler.Now = func() time.Time { return now }

	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("replacement process bootstrap renewal: %v", err)
	}
	renewed := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, renewed); err != nil {
		t.Fatal(err)
	}
	wantReplacementReceipt := haFencingLeaseBootstrapReceipt("primary-a", 1, strings.Repeat("b", 64))
	if renewed.Spec.RenewTime == nil || !renewed.Spec.RenewTime.Time.Equal(now) ||
		renewed.Annotations[haFencingLeaseAnnotationBootstrapReceipt] != wantReplacementReceipt {
		t.Fatalf("replacement process did not receive its one-shot renewal: %#v", renewed)
	}
	if cluster.Status.HAStatus.Fencing.Ready {
		t.Fatalf("replacement pending process became authoritative: %#v", cluster.Status.HAStatus.Fencing)
	}

	// A fresh observation from process B is still the same durable receipt and
	// cannot advance the Lease a second time.
	now = leaseTime.Add(3 * time.Second)
	cluster.Status.HAStatus.PrimaryWatchdogProof.ObservedAt = metav1.NewTime(now)
	if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
		t.Fatalf("repeat replacement pending reconcile: %v", err)
	}
	repeated := &coordinationv1.Lease{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, repeated); err != nil {
		t.Fatal(err)
	}
	if !repeated.Spec.RenewTime.Equal(renewed.Spec.RenewTime) {
		t.Fatalf("replacement process renewed twice: first=%s repeat=%s", renewed.Spec.RenewTime, repeated.Spec.RenewTime)
	}
}

func TestInitialPrimaryLeaseBootstrapRejectsDifferentHolderOrScope(t *testing.T) {
	leaseTime := time.Date(2026, 7, 16, 4, 21, 14, 680204000, time.UTC)
	tests := []struct {
		name   string
		mutate func(*antflyv1.AntflyCluster, *coordinationv1.Lease)
	}{
		{
			name: "different live holder",
			mutate: func(_ *antflyv1.AntflyCluster, lease *coordinationv1.Lease) {
				holder := "standby-a"
				lease.Spec.HolderIdentity = &holder
			},
		},
		{
			name: "different identity scope",
			mutate: func(_ *antflyv1.AntflyCluster, lease *coordinationv1.Lease) {
				lease.Annotations[haFencingLeaseAnnotationClusterID] = "999"
			},
		},
		{
			name: "different proof holder",
			mutate: func(cluster *antflyv1.AntflyCluster, _ *coordinationv1.Lease) {
				cluster.Status.HAStatus.PrimaryWatchdogProof.ObservedHolderNodeID = "standby-a"
			},
		},
		{
			name: "different proof topology",
			mutate: func(cluster *antflyv1.AntflyCluster, _ *coordinationv1.Lease) {
				cluster.Status.HAStatus.PrimaryWatchdogProof.TopologyID = "other-topology"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proofTime := leaseTime.Add(time.Second)
			cluster := haClusterWithAutomaticKubernetesLeaseFailover()
			cluster.Spec.HighAvailability.Identity.ShardID = 10
			cluster.Spec.HighAvailability.Identity.TableID = 20
			cluster.Status.HAStatus = &antflyv1.HAStatus{PrimaryLSN: 1}
			lease := haFenceLease(cluster, leaseTime, haFencingLeaseDefaultDurationSeconds, 1, "primary-a")
			lease.Annotations[haFencingLeaseAnnotationPrimaryLSN] = "0"
			cluster.Status.HAStatus = &antflyv1.HAStatus{
				PrimaryAdminLastError: "HA Lease watchdog authority is pending for node primary-a",
				PrimaryWatchdogProof:  candidateLeaseProof(proofTime, "primary-a", "primary-a", 1),
			}
			tt.mutate(cluster, lease)
			pod := candidateLeasePod(proofTime, "primary-a-pod-uid")
			reconciler := testHAReconciler(t, lease, pod)
			reconciler.Now = func() time.Time { return proofTime.Add(time.Second) }

			if err := reconciler.reconcileHAFencingLease(context.Background(), cluster); err != nil {
				t.Fatalf("reconcile rejected bootstrap: %v", err)
			}
			observed := &coordinationv1.Lease{}
			if err := reconciler.Get(context.Background(), client.ObjectKey{Name: lease.Name, Namespace: lease.Namespace}, observed); err != nil {
				t.Fatal(err)
			}
			if !observed.Spec.RenewTime.Equal(lease.Spec.RenewTime) {
				t.Fatalf("mismatched bootstrap renewed Lease: before=%s after=%s", lease.Spec.RenewTime, observed.Spec.RenewTime)
			}
		})
	}
}

func TestPendingPrimaryWatchdogProofCannotAuthorizePromotedRuntime(t *testing.T) {
	now := time.Date(2026, 7, 16, 4, 21, 16, 0, time.UTC)
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Status.HAStatus = caughtUpHAStatus()
	cluster.Status.HAStatus.PrimaryWatchdogProof = candidateLeaseProof(now, "standby-a", "standby-a", 4)
	pod := candidateLeasePod(now, "standby-a-pod-uid")
	reconciler := testHAReconciler(t, pod)
	reconciler.Now = func() time.Time { return now }

	ready, err := reconciler.haPromotedRuntimeWatchdogReady(context.Background(), cluster, "standby-a", 4)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("pending watchdog capability authorized a promoted runtime")
	}
}
