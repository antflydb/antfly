// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	haLeaseRenewalInterval = 2 * time.Second
	haLeaseProofTimeout    = 1500 * time.Millisecond
)

// haLeaseRenewalReconciler is deliberately limited to fresh watchdog
// observation plus renewal of the unchanged current holder. Expensive seed
// capture and every topology transition stay on the main reconciler.
type haLeaseRenewalReconciler struct {
	parent *AntflyClusterReconciler
}

func haLeaseRenewalEventPredicate() predicate.Predicate {
	// Status observations are intentionally excluded. The controller's own
	// fixed-cadence RequeueAfter is the renewal clock; allowing the main
	// reconciler's status writes to enqueue this key can turn health churn into
	// an unbounded proof-request loop and starve that clock. Spec generations
	// still wake renewal immediately when HA is enabled, disabled, or changed.
	return predicate.GenerationChangedPredicate{}
}

func (r *haLeaseRenewalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cluster := &antflyv1.AntflyCluster{}
	if err := r.parent.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if cluster.Spec.HighAvailability == nil {
		return ctrl.Result{}, nil
	}

	proofCtx, cancel := context.WithTimeout(ctx, haLeaseProofTimeout)
	defer cancel()
	if err := r.parent.observeHACurrentPrimaryWatchdogProof(proofCtx, cluster); err == nil {
		if err := r.parent.renewCurrentHAFencingLease(proofCtx, cluster); err != nil && !apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: haLeaseRenewalInterval}, err
		}
	}
	return ctrl.Result{RequeueAfter: haLeaseRenewalInterval}, nil
}

// observeHACurrentPrimaryWatchdogProof fetches only the authenticated runtime
// capability needed for Lease renewal. The runtime serves this proof outside
// storage mutation critical sections; all exact Lease, topology, Pod, process,
// and freshness checks remain in renewCurrentHAFencingLease.
func (r *AntflyClusterReconciler) observeHACurrentPrimaryWatchdogProof(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if cluster == nil || cluster.Spec.HighAvailability == nil || cluster.Status.HAStatus == nil {
		return fmt.Errorf("HA watchdog proof requires configured HA status")
	}
	ha := cluster.Spec.HighAvailability
	adminURL := strings.TrimSpace(haCurrentPrimaryAdminURL(ha, cluster.Status.HAStatus))
	nodeID := strings.TrimSpace(haCurrentPrimaryNodeID(ha, cluster.Status.HAStatus))
	if adminURL == "" || nodeID == "" {
		return fmt.Errorf("HA watchdog proof requires the current primary admin URL and node ID")
	}
	adminClient, err := r.haAdminSDKClient(cluster, adminURL)
	if err != nil {
		return err
	}
	requestStartedAt := r.haNow()
	response, err := adminClient.WatchdogProofResponse(ctx)
	if err != nil {
		return err
	}
	observedAt := r.haNow()
	proof, err := haWatchdogProofFromAdmin(&response.Value.Proof, cluster, nodeID, true, requestStartedAt, observedAt)
	if err != nil {
		return err
	}
	cluster.Status.HAStatus.PrimaryWatchdogProof = proof
	return nil
}
