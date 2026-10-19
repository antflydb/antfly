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
	"reflect"
	"strings"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestStandalonePodExclusiveVolumeAndImmutableReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOncePod
	client := newHAControllerTestClient(t, scheme, cluster)
	r := &AntflyClusterReconciler{Client: client, Scheme: scheme}
	r.applyDefaults(cluster)
	if err := r.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster); err != nil {
		t.Fatal(err)
	}
	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace}
	if err := client.Get(context.Background(), key, sts); err != nil {
		t.Fatal(err)
	}
	if *sts.Spec.Replicas != 1 || len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("unexpected writer topology: %+v", sts.Spec)
	}
	modes := sts.Spec.VolumeClaimTemplates[0].Spec.AccessModes
	if len(modes) != 1 || modes[0] != corev1.ReadWriteOncePod {
		t.Fatalf("expected pod-exclusive claim, got %v", modes)
	}
	cluster.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOnce
	if err := r.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster); err == nil || !strings.Contains(err.Error(), "access mode migration") {
		t.Fatalf("webhook bypass changed access mode: %v", err)
	}
	observed := &appsv1.StatefulSet{}
	if err := client.Get(context.Background(), key, observed); err != nil {
		t.Fatal(err)
	}
	if observed.Spec.VolumeClaimTemplates[0].Spec.AccessModes[0] != corev1.ReadWriteOncePod {
		t.Fatal("rejection changed the volume")
	}
}

func TestStandaloneExistingRWOVolumeCannotClaimPodExclusivity(t *testing.T) {
	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOncePod
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: standaloneStatefulSetName(cluster), ResourceVersion: "1", Annotations: map[string]string{annotationStorageEngine: "lite", annotationLiteFileName: "antfly.aflite"}},
		Spec:       appsv1.StatefulSetSpec{VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: standaloneStorageVolumeName(cluster)}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}}}}},
	}
	if err := validateAndSetStandaloneStorageIdentity(sts, cluster); err == nil || !strings.Contains(err.Error(), "access mode migration") {
		t.Fatalf("expected mismatch rejection: %v", err)
	}
}

func TestStandaloneSeedPVCRequiresRequestedAccessMode(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOncePod
	cluster.Spec.HighAvailability = &antflyv1.HighAvailabilitySpec{Runtime: &antflyv1.HARuntimeSpec{
		StartupGate: &antflyv1.HAStartupGateSpec{Policy: antflyv1.HAStartupGatePolicyRequireActivatedSeed,
			RequiredReceipt: &antflyv1.HARequiredSeedActivationReceipt{TargetPVCName: "seed-data"}},
	}}
	client := newHAControllerTestClient(t, scheme)
	r := &AntflyClusterReconciler{Client: client, Scheme: scheme}
	claim, err := r.reconcileHAStartupTargetPVC(context.Background(), cluster, "1Gi")
	if err != nil {
		t.Fatal(err)
	}
	if len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteOncePod {
		t.Fatalf("seed claim lost fence: %v", claim.Spec.AccessModes)
	}
	claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	if err := client.Update(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileHAStartupTargetPVC(context.Background(), cluster, "1Gi"); err == nil || !strings.Contains(err.Error(), "access mode migration") {
		t.Fatalf("accepted incompatible seed: %v", err)
	}
}

func TestStandaloneSuspendRetainsIdentityAndResumesOneOwner(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOncePod
	c := newHAControllerTestClient(t, scheme, cluster)
	r := &AntflyClusterReconciler{Client: c, Scheme: scheme}
	r.applyDefaults(cluster)
	ctx := context.Background()
	if err := r.reconcileStandaloneStatefulSet(ctx, &envFromCache{}, cluster); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace}
	before := &appsv1.StatefulSet{}
	if err := c.Get(ctx, key, before); err != nil {
		t.Fatal(err)
	}
	// Even a destructive old policy must be made safe before scaling down.
	before.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType, WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType}
	if err := c.Update(ctx, before); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.Standalone.Suspend = true
	for i := 0; i < 2; i++ {
		if err := r.reconcileStandaloneStatefulSet(ctx, &envFromCache{}, cluster); err != nil {
			t.Fatal(err)
		}
	}
	asleep := &appsv1.StatefulSet{}
	if err := c.Get(ctx, key, asleep); err != nil {
		t.Fatal(err)
	}
	if *asleep.Spec.Replicas != 0 || asleep.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType || asleep.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatal("suspend did not retain claims before stopping compute")
	}
	if !reflect.DeepEqual(before.Spec.Template, asleep.Spec.Template) || !reflect.DeepEqual(before.Spec.VolumeClaimTemplates, asleep.Spec.VolumeClaimTemplates) {
		t.Fatal("suspension changed workload or storage identity")
	}
	cluster.Spec.Standalone.Suspend = false
	if err := r.reconcileStandaloneStatefulSet(ctx, &envFromCache{}, cluster); err != nil {
		t.Fatal(err)
	}
	awake := &appsv1.StatefulSet{}
	if err := c.Get(ctx, key, awake); err != nil {
		t.Fatal(err)
	}
	if *awake.Spec.Replicas != 1 || !reflect.DeepEqual(before.Spec.VolumeClaimTemplates, awake.Spec.VolumeClaimTemplates) {
		t.Fatal("resume failed to reuse one owner and the same claim")
	}
}
