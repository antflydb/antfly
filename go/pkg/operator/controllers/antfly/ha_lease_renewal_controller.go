// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

package controllers

import (
	"context"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
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
	if err := r.parent.observeHAPrimaryAdminStatus(proofCtx, cluster); err == nil {
		if err := r.parent.renewCurrentHAFencingLease(proofCtx, cluster); err != nil && !apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: haLeaseRenewalInterval}, err
		}
	}
	return ctrl.Result{RequeueAfter: haLeaseRenewalInterval}, nil
}
