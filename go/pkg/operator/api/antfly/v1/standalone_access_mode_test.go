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

package v1

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestStandaloneAccessModeAdmission(t *testing.T) {
	for _, mode := range []corev1.PersistentVolumeAccessMode{"", corev1.ReadWriteOnce, corev1.ReadWriteOncePod} {
		cluster := baseStandaloneCluster()
		cluster.Spec.Storage.Engine = "lite"
		cluster.Spec.Storage.StandaloneAccessMode = mode
		if err := cluster.ValidateCreate(); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
	}
	cluster := baseStandaloneCluster()
	cluster.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteMany
	if err := cluster.ValidateCreate(); err == nil || !strings.Contains(err.Error(), "standaloneAccessMode") {
		t.Fatalf("expected invalid access mode rejection: %v", err)
	}
	if err := ValidateOperatorManagedStorageSpec(ClusterModeDistributed, StorageSpec{StandaloneAccessMode: corev1.ReadWriteOncePod}); err == nil {
		t.Fatal("distributed profile accepted standalone access mode")
	}
}

func TestStandaloneAccessModeImmutable(t *testing.T) {
	old := baseStandaloneCluster()
	updated := old.DeepCopy()
	updated.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOnce
	if err := updated.ValidateUpdate(old); err != nil {
		t.Fatalf("explicit legacy default must be equivalent: %v", err)
	}
	updated.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOncePod
	if err := updated.ValidateUpdate(old); err == nil || !strings.Contains(err.Error(), "standaloneAccessMode") {
		t.Fatalf("expected immutable access mode rejection: %v", err)
	}
	old.Spec.Storage.StandaloneAccessMode = corev1.ReadWriteOncePod
	updated.Spec.Storage.StandaloneAccessMode = ""
	if err := updated.ValidateUpdate(old); err == nil || !strings.Contains(err.Error(), "standaloneAccessMode") {
		t.Fatalf("removing pod exclusivity must also be rejected: %v", err)
	}
}
