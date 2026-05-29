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

package poddiagnostics

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDiagnosePodsClassifiesModelPullFailure(t *testing.T) {
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "inference-0"},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "model-puller-0",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "CrashLoopBackOff",
						Message: "registry blob returned 404",
					},
				},
			}},
		},
	}}

	findings := DiagnosePods(pods)
	if !Has(findings, FindingModelPullFailed) {
		t.Fatalf("expected model pull failure, got %#v", findings)
	}
	finding, ok := First(findings, FindingModelPullFailed)
	if !ok {
		t.Fatal("expected first model pull finding")
	}
	if finding.Pod != "inference-0" || finding.Container != "model-puller-0" {
		t.Fatalf("unexpected finding target: %#v", finding)
	}
}

func TestDiagnosePodsIgnoresNormalInitWaitingReasons(t *testing.T) {
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "inference-0"},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "model-puller-0",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "PodInitializing",
						Message: "waiting for pod startup",
					},
				},
			}, {
				Name: "model-puller-1",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "ContainerCreating",
						Message: "creating container",
					},
				},
			}},
		},
	}}

	findings := DiagnosePods(pods)
	if len(findings) != 0 {
		t.Fatalf("expected no findings for normal init waiting states, got %#v", findings)
	}
}

func TestDiagnosePodsClassifiesImagePullFailure(t *testing.T) {
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "inference-0"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "inference",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "ImagePullBackOff",
						Message: "failed to pull image",
					},
				},
			}},
		},
	}}

	findings := DiagnosePods(pods)
	if !Has(findings, FindingImagePullFailed) {
		t.Fatalf("expected image pull failure, got %#v", findings)
	}
}

func TestDiagnosePodsClassifiesUnschedulable(t *testing.T) {
	pods := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "antfly-data-0"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:    corev1.PodScheduled,
				Status:  corev1.ConditionFalse,
				Reason:  corev1.PodReasonUnschedulable,
				Message: "0/3 nodes are available",
			}},
		},
	}}

	findings := DiagnosePods(pods)
	if !Has(findings, FindingUnschedulable) {
		t.Fatalf("expected unschedulable finding, got %#v", findings)
	}
}
