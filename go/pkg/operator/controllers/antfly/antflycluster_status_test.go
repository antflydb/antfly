// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	"github.com/antflydb/antfly/go/pkg/operator/controllers/internal/poddiagnostics"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetComponentConditionReportsImagePullFailure(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "antfly",
			Namespace:  "default",
			Generation: 4,
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.setComponentCondition(cluster, antflyv1.TypeDataReady, 0, 1, []poddiagnostics.Finding{{
		Type:      poddiagnostics.FindingImagePullFailed,
		Pod:       "antfly-data-0",
		Container: "antfly",
		Reason:    "ImagePullBackOff",
		Message:   "failed to pull image",
	}}, "data")

	condition := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeDataReady)
	if condition == nil {
		t.Fatalf("expected %s condition", antflyv1.TypeDataReady)
		return
	}
	if condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected condition false, got %s", condition.Status)
	}
	if condition.Reason != antflyv1.ReasonImagePullFailed {
		t.Fatalf("expected image pull reason, got %s", condition.Reason)
	}
}

func TestSetAvailableConditionClearsWhenReady(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "antfly",
			Namespace:  "default",
			Generation: 5,
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.setAvailableCondition(cluster, nil, true)

	condition := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeAvailable)
	if condition == nil {
		t.Fatalf("expected %s condition", antflyv1.TypeAvailable)
		return
	}
	if condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected Available true, got %s", condition.Status)
	}
}
