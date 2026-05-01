// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package controllers implements the Kubernetes controllers for Termite CRDs.
package controllers

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/operator/termite/api/v1alpha1"
)

// TermiteRouteReconciler reconciles a TermiteRoute object
type TermiteRouteReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	Recorder           events.EventRecorder
	validationAttempts sync.Map
}

// +kubebuilder:rbac:groups=antfly.io,resources=termiteroutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=antfly.io,resources=termiteroutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=antfly.io,resources=termiteroutes/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile handles TermiteRoute reconciliation
func (r *TermiteRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	key := req.String()

	// Fetch the TermiteRoute
	route := &antflyaiv1alpha1.TermiteRoute{}
	if err := r.Get(ctx, req.NamespacedName, route); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("TermiteRoute not found, ignoring")
			r.validationAttempts.Delete(key)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling TermiteRoute", "name", route.Name)

	// Generation guard: skip validation if spec unchanged since last success.
	needsValidation := route.Status.ObservedGeneration != route.Generation || !route.Status.Active
	if needsValidation {
		// Validate configuration (fallback when webhook is disabled).
		// Note: immutability checks require the old object and are only
		// enforced by the admission webhook.
		if err := route.ValidateTermiteRoute(); err != nil {
			logger.Error(err, "TermiteRoute validation failed")
			attempt := r.incrementValidationAttempts(key)
			meta.SetStatusCondition(&route.Status.Conditions, metav1.Condition{
				Type:    antflyaiv1alpha1.TypeConfigurationValid,
				Status:  metav1.ConditionFalse,
				Reason:  antflyaiv1alpha1.ReasonValidationFailed,
				Message: err.Error(),
			})
			if route.Status.Active {
				route.Status.Active = false
			}
			if statusErr := r.Status().Update(ctx, route); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			r.Recorder.Eventf(route, nil, corev1.EventTypeWarning, antflyaiv1alpha1.ReasonValidationFailed, antflyaiv1alpha1.ReasonValidationFailed, "Validation failed: %s", err.Error())
			return ctrl.Result{RequeueAfter: calculateBackoff(attempt - 1)}, nil
		}

		// Validate referenced pools exist
		for _, dest := range route.Spec.Route {
			pool := &antflyaiv1alpha1.TermitePool{}
			if err := r.Get(ctx, client.ObjectKey{Name: dest.Pool, Namespace: route.Namespace}, pool); err != nil {
				if errors.IsNotFound(err) {
					logger.Error(err, "Referenced pool not found", "pool", dest.Pool)
					attempt := r.incrementValidationAttempts(key)
					meta.SetStatusCondition(&route.Status.Conditions, metav1.Condition{
						Type:    antflyaiv1alpha1.TypeConfigurationValid,
						Status:  metav1.ConditionFalse,
						Reason:  antflyaiv1alpha1.ReasonValidationFailed,
						Message: fmt.Sprintf("Referenced pool %q not found", dest.Pool),
					})
					if route.Status.Active {
						route.Status.Active = false
					}
					if err := r.Status().Update(ctx, route); err != nil {
						return ctrl.Result{}, err
					}
					r.Recorder.Eventf(route, nil, corev1.EventTypeWarning, antflyaiv1alpha1.ReasonValidationFailed, antflyaiv1alpha1.ReasonValidationFailed, "Referenced pool %q not found", dest.Pool)
					return ctrl.Result{RequeueAfter: calculateBackoff(attempt - 1)}, nil
				}
				return ctrl.Result{}, err
			}
		}

		// Route is valid, mark as active
		r.validationAttempts.Delete(key)
		meta.SetStatusCondition(&route.Status.Conditions, metav1.Condition{
			Type:    antflyaiv1alpha1.TypeConfigurationValid,
			Status:  metav1.ConditionTrue,
			Reason:  antflyaiv1alpha1.ReasonValidationPassed,
			Message: "Configuration is valid",
		})
		if !route.Status.Active || route.Status.ObservedGeneration != route.Generation {
			route.Status.Active = true
			route.Status.ObservedGeneration = route.Generation
			if err := r.Status().Update(ctx, route); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// The actual route configuration is applied by the proxy
	// which watches TermiteRoute resources directly.
	// The operator's role is primarily validation and status management.

	return ctrl.Result{}, nil
}

func (r *TermiteRouteReconciler) incrementValidationAttempts(key string) int {
	val, _ := r.validationAttempts.LoadOrStore(key, 0)
	count := val.(int) + 1
	r.validationAttempts.Store(key, count)
	return count
}

// SetupWithManager sets up the controller with the Manager
func (r *TermiteRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&antflyaiv1alpha1.TermiteRoute{}).
		Complete(r)
}
