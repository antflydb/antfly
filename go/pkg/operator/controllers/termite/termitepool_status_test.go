// Copyright 2026 Antfly, Inc.
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

package controllers

import (
	"context"
	"testing"

	antflyaiv1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/termite/v1alpha1"
	"github.com/antflydb/antfly/go/pkg/operator/controllers/internal/poddiagnostics"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSetRuntimeConditionsReportsModelPullFailure(t *testing.T) {
	pool := &antflyaiv1alpha1.TermitePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "termite",
			Namespace:  "default",
			Generation: 7,
		},
	}
	reconciler := &TermitePoolReconciler{}

	reconciler.setRuntimeConditions(pool, true, 1, 0, 1, []poddiagnostics.Finding{{
		Type:      poddiagnostics.FindingModelPullFailed,
		Pod:       "termite-0",
		Container: "model-puller-0",
		Reason:    "CrashLoopBackOff",
		Message:   "registry blob returned 404",
	}})

	modelsReady := meta.FindStatusCondition(pool.Status.Conditions, antflyaiv1alpha1.TypeModelsReady)
	if modelsReady == nil {
		t.Fatalf("expected %s condition", antflyaiv1alpha1.TypeModelsReady)
	}
	if modelsReady.Status != metav1.ConditionFalse {
		t.Fatalf("expected ModelsReady false, got %s", modelsReady.Status)
	}
	if modelsReady.Reason != antflyaiv1alpha1.ReasonModelPullFailed {
		t.Fatalf("expected model pull reason, got %s", modelsReady.Reason)
	}

	available := meta.FindStatusCondition(pool.Status.Conditions, antflyaiv1alpha1.TypeAvailable)
	if available == nil || available.Status != metav1.ConditionFalse {
		t.Fatalf("expected Available false, got %#v", available)
	}
}

func TestSetRuntimeConditionsClearsAfterPodsReady(t *testing.T) {
	pool := &antflyaiv1alpha1.TermitePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "termite",
			Namespace:  "default",
			Generation: 8,
		},
	}
	reconciler := &TermitePoolReconciler{}

	reconciler.setRuntimeConditions(pool, true, 1, 1, 1, nil)

	for _, conditionType := range []string{
		antflyaiv1alpha1.TypeImageAvailable,
		antflyaiv1alpha1.TypeModelsReady,
		antflyaiv1alpha1.TypePodsReady,
		antflyaiv1alpha1.TypeAvailable,
	} {
		condition := meta.FindStatusCondition(pool.Status.Conditions, conditionType)
		if condition == nil {
			t.Fatalf("expected %s condition", conditionType)
		}
		if condition.Status != metav1.ConditionTrue {
			t.Fatalf("expected %s true, got %s", conditionType, condition.Status)
		}
	}
}

func TestUpdateStatusMissingStatefulSetStaysPending(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyaiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add termite scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	pool := &antflyaiv1alpha1.TermitePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "missing-sts",
			Namespace:  "default",
			Generation: 1,
		},
	}
	reconciler := &TermitePoolReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pool).
			WithStatusSubresource(pool).
			Build(),
		Scheme: scheme,
	}

	if err := reconciler.updateStatus(context.Background(), pool, nil); err != nil {
		t.Fatalf("update status: %v", err)
	}

	updated := &antflyaiv1alpha1.TermitePool{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}, updated); err != nil {
		t.Fatalf("get updated pool: %v", err)
	}
	if updated.Status.Phase != antflyaiv1alpha1.TermitePoolPhasePending {
		t.Fatalf("expected pending without StatefulSet, got %s", updated.Status.Phase)
	}

	for _, conditionType := range []string{
		antflyaiv1alpha1.TypePodsReady,
		antflyaiv1alpha1.TypeAvailable,
	} {
		condition := meta.FindStatusCondition(updated.Status.Conditions, conditionType)
		if condition == nil {
			t.Fatalf("expected %s condition", conditionType)
		}
		if condition.Status != metav1.ConditionFalse {
			t.Fatalf("expected %s false without StatefulSet, got %s", conditionType, condition.Status)
		}
		if condition.Reason != antflyaiv1alpha1.ReasonWaitingForPods {
			t.Fatalf("expected %s waiting reason, got %s", conditionType, condition.Reason)
		}
	}

	for _, conditionType := range []string{
		antflyaiv1alpha1.TypeWorkloadReconciled,
		antflyaiv1alpha1.TypePodsScheduled,
		antflyaiv1alpha1.TypeImageAvailable,
		antflyaiv1alpha1.TypeModelsReady,
	} {
		condition := meta.FindStatusCondition(updated.Status.Conditions, conditionType)
		if condition == nil {
			t.Fatalf("expected %s condition", conditionType)
		}
		if condition.Status != metav1.ConditionUnknown {
			t.Fatalf("expected %s unknown without StatefulSet, got %s", conditionType, condition.Status)
		}
	}
}
