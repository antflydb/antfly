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
	"context"
	"fmt"
	"strings"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestStandaloneSuspensionRequiresLiveQuiescence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*appsv1.StatefulSet, *corev1.PodList)
		suspended bool
		missing   bool
	}{
		{name: "acknowledged-and-empty", suspended: true},
		{name: "unobserved-scale-down", mutate: func(sts *appsv1.StatefulSet, _ *corev1.PodList) { sts.Status.ObservedGeneration-- }},
		{name: "live-running-pod"},
		{name: "pending-pod", mutate: func(_ *appsv1.StatefulSet, pods *corev1.PodList) { pods.Items[0].Status.Phase = corev1.PodPending }},
		{name: "terminating-pod", mutate: func(_ *appsv1.StatefulSet, pods *corev1.PodList) {
			now := metav1.Now()
			pods.Items[0].DeletionTimestamp = &now
			pods.Items[0].Finalizers = []string{"test/hold"}
		}},
		{name: "terminal-pod", mutate: func(_ *appsv1.StatefulSet, pods *corev1.PodList) { pods.Items[0].Status.Phase = corev1.PodSucceeded }},
		{name: "owned-pod-without-ordinal-name", mutate: func(sts *appsv1.StatefulSet, pods *corev1.PodList) {
			controller := true
			pods.Items[0].Name = "old-writer"
			pods.Items[0].OwnerReferences = []metav1.OwnerReference{{UID: sts.UID, Controller: &controller}}
		}},
		{name: "unrelated-pod", suspended: true, mutate: func(_ *appsv1.StatefulSet, pods *corev1.PodList) { pods.Items[0].Name = "unrelated" }},
		{name: "live-resume-despite-cached-zero", mutate: func(sts *appsv1.StatefulSet, _ *corev1.PodList) { one := int32(1); sts.Spec.Replicas = &one }},
		{name: "stale-ready-counter", mutate: func(sts *appsv1.StatefulSet, _ *corev1.PodList) { sts.Status.ReadyReplicas = 1 }},
		{name: "foreign-workload", mutate: func(sts *appsv1.StatefulSet, _ *corev1.PodList) { sts.OwnerReferences[0].UID = "other-cluster" }},
		{name: "terminating-workload", mutate: func(sts *appsv1.StatefulSet, _ *corev1.PodList) {
			now := metav1.Now()
			sts.DeletionTimestamp = &now
			sts.Finalizers = []string{"test/hold"}
		}},
		{name: "missing-workload", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			cluster := baseStandaloneControllerCluster()
			cluster.UID = "test-cluster"
			cluster.Spec.Standalone.Suspend = true
			cluster.Status.StandaloneStatus = &antflyv1.StandaloneStatus{PodName: "stale-name", PodIP: "stale-ip"}
			zero := int32(0)
			controller := true
			live := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace, UID: "writer-set", Generation: 3,
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "antfly.io/v1", Kind: "AntflyCluster", Name: cluster.Name, UID: cluster.UID, Controller: &controller}}},
				Spec: appsv1.StatefulSetSpec{Replicas: &zero}, Status: appsv1.StatefulSetStatus{ObservedGeneration: 3},
			}
			// Only the independent boundary reader sees the current generation and
			// Pods. Pods deliberately have no service labels or controller reference.
			cached := live.DeepCopy()
			cached.Status.ObservedGeneration = 1
			cache := newHAControllerTestClient(t, scheme, cluster, cached)
			pods := &corev1.PodList{}
			if strings.Contains(tc.name, "pod") {
				pods.Items = []corev1.Pod{{
					ObjectMeta: metav1.ObjectMeta{Name: live.Name + "-0", Namespace: cluster.Namespace},
					Status: corev1.PodStatus{
						Phase:             corev1.PodRunning,
						Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
						ContainerStatuses: []corev1.ContainerStatus{{Name: "antfly", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
					},
				}}
			}
			if tc.mutate != nil {
				tc.mutate(live, pods)
			}
			var objects []client.Object
			if !tc.missing {
				objects = append(objects, live)
			}
			for i := range pods.Items {
				objects = append(objects, &pods.Items[i])
			}
			boundary := newHAControllerTestClient(t, scheme, objects...)
			r := &AntflyClusterReconciler{Client: cache, BoundaryReader: boundary, Scheme: scheme}
			if err := cache.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, cluster); err != nil {
				t.Fatal(err)
			}
			if err := r.updateStatus(ctx, cluster); err != nil {
				t.Fatal(err)
			}
			if cluster.Status.StandaloneStatus.Ready {
				t.Fatal("suspension request advertised a ready runtime")
			}
			for _, condition := range cluster.Status.Conditions {
				if condition.Type == antflyv1.TypeStandaloneReady || condition.Type == antflyv1.TypeMetadataReady || condition.Type == antflyv1.TypeDataReady || condition.Type == antflyv1.TypeInferenceReady {
					if condition.Status != metav1.ConditionFalse {
						t.Fatalf("suspension request advertised %s=%s", condition.Type, condition.Status)
					}
				}
			}
			if got := cluster.Status.Phase == "Suspended"; got != tc.suspended {
				t.Fatalf("phase=%s, want suspended=%t", cluster.Status.Phase, tc.suspended)
			}
			if tc.suspended && (cluster.Status.StandaloneStatus.PodName != "" || cluster.Status.StandaloneStatus.PodIP != "") {
				t.Fatal("suspended status retained stale pod identity")
			}
		})
	}
}

type suspensionFailureReader struct {
	client.Reader
	failGet bool
}

func (r suspensionFailureReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if r.failGet {
		return fmt.Errorf("live workload read failed")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}
func (r suspensionFailureReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return fmt.Errorf("live pod list failed")
}

func TestStandaloneSuspensionReadFailureCannotCertify(t *testing.T) {
	for _, failGet := range []bool{false, true} {
		scheme := runtime.NewScheme()
		for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
			if err := add(scheme); err != nil {
				t.Fatal(err)
			}
		}
		cluster := baseStandaloneControllerCluster()
		cluster.Spec.Standalone.Suspend = true
		cache := newHAControllerTestClient(t, scheme, cluster)
		r := &AntflyClusterReconciler{Client: cache, Scheme: scheme, BoundaryReader: suspensionFailureReader{Reader: cache, failGet: failGet}}
		if err := r.updateStatus(context.Background(), cluster); err == nil {
			t.Fatal("ignored failed live read")
		}
		if cluster.Status.Phase == "Suspended" {
			t.Fatal("certified a failed observation")
		}
	}
}
