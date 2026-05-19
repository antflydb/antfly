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

package v1alpha1

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validPool() *TermitePool {
	return &TermitePool{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pool", Namespace: "default"},
		Spec: TermitePoolSpec{
			Models: ModelConfig{
				Preload: []ModelSpec{{Name: "bge-small-en-v1.5"}},
			},
			Replicas: ReplicaConfig{Min: 1, Max: 3},
		},
	}
}

func TestValidateTermitePool_Valid(t *testing.T) {
	pool := validPool()
	if err := pool.ValidateTermitePool(); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

func TestValidateTermitePool_ValidAutoscaling(t *testing.T) {
	pool := validPool()
	pool.Spec.Autoscaling = &AutoscalingConfig{
		Enabled: true,
		Metrics: []ScalingMetric{
			{Type: MetricTypeCPU, Target: "75%"},
			{Type: MetricTypeMemory, Target: "512Mi"},
			{Type: MetricTypeQueueDepth, Target: "10"},
		},
	}

	if err := pool.ValidateTermitePool(); err != nil {
		t.Errorf("expected no error for valid autoscaling config, got: %v", err)
	}
}

func TestValidateTermitePool_InvalidAutoscalingTarget(t *testing.T) {
	pool := validPool()
	pool.Spec.Autoscaling = &AutoscalingConfig{
		Enabled: true,
		Metrics: []ScalingMetric{
			{Type: MetricTypeCPU, Target: "not-a-percent"},
		},
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Fatal("expected error for invalid autoscaling target")
	}
	if !strings.Contains(err.Error(), "spec.autoscaling.metrics[0].target") {
		t.Fatalf("expected autoscaling target error, got: %v", err)
	}
}

func TestValidateTermitePool_InvalidAutoscalingPolicy(t *testing.T) {
	pool := validPool()
	pool.Spec.Autoscaling = &AutoscalingConfig{
		Enabled: true,
		Metrics: []ScalingMetric{
			{
				Type:   MetricTypeQueueDepth,
				Target: "10",
				ScaleUp: &ScalingBehavior{
					Policies: []ScalingPolicy{{Type: "Replicas", Value: 0, PeriodSeconds: 0}},
				},
			},
		},
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Fatal("expected error for invalid autoscaling policy")
	}
	if !strings.Contains(err.Error(), "scaleUp.policies[0].type") {
		t.Fatalf("expected autoscaling policy error, got: %v", err)
	}
}

func TestValidateTermitePool_DisabledAutoscalingIgnoresMetricConfig(t *testing.T) {
	pool := validPool()
	pool.Spec.Autoscaling = &AutoscalingConfig{
		Enabled: false,
		Metrics: []ScalingMetric{
			{
				Type:   MetricTypeCPU,
				Target: "not-a-percent",
				ScaleUp: &ScalingBehavior{
					Policies: []ScalingPolicy{{Type: "Replicas", Value: 0, PeriodSeconds: 0}},
				},
			},
		},
	}

	if err := pool.ValidateTermitePool(); err != nil {
		t.Fatalf("expected disabled autoscaling to ignore nested metric config, got: %v", err)
	}
}

func TestValidateTermitePool_DisabledAutoscalingStillRejectsImpossibleValues(t *testing.T) {
	pool := validPool()
	warmup := int32(-1)
	pool.Spec.Autoscaling = &AutoscalingConfig{
		Enabled:        false,
		WarmupReplicas: &warmup,
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Fatal("expected error for negative warmup replicas")
	}
	if !strings.Contains(err.Error(), "warmupReplicas") {
		t.Fatalf("expected warmupReplicas error, got: %v", err)
	}
}

func TestValidateTermitePool_RejectsHPAStabilizationAboveLimit(t *testing.T) {
	pool := validPool()
	window := metav1.Duration{Duration: 3601 * time.Second}
	pool.Spec.Autoscaling = &AutoscalingConfig{
		Enabled:                true,
		ScaleDownStabilization: &window,
		Metrics: []ScalingMetric{
			{
				Type:   MetricTypeCPU,
				Target: "75%",
				ScaleUp: &ScalingBehavior{
					StabilizationWindow: &window,
				},
			},
		},
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Fatal("expected error for HPA stabilization windows above Kubernetes limit")
	}
	if !strings.Contains(err.Error(), "scaleDownStabilization") {
		t.Fatalf("expected top-level stabilization error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "scaleUp.stabilizationWindow") {
		t.Fatalf("expected per-metric stabilization error, got: %v", err)
	}
}

func TestValidateTermitePool_RejectsHPAPeriodAboveLimit(t *testing.T) {
	pool := validPool()
	pool.Spec.Autoscaling = &AutoscalingConfig{
		Enabled: true,
		Metrics: []ScalingMetric{
			{
				Type:   MetricTypeCPU,
				Target: "75%",
				ScaleUp: &ScalingBehavior{
					Policies: []ScalingPolicy{{Type: "Pods", Value: 1, PeriodSeconds: 1801}},
				},
			},
		},
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Fatal("expected error for HPA policy period above Kubernetes limit")
	}
	if !strings.Contains(err.Error(), "periodSeconds must be <= 1800") {
		t.Fatalf("expected HPA period upper-bound error, got: %v", err)
	}
}

func TestValidateTermitePool_MinGreaterThanMax(t *testing.T) {
	pool := validPool()
	pool.Spec.Replicas.Min = 5
	pool.Spec.Replicas.Max = 2

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for min > max replicas")
	} else if !strings.Contains(err.Error(), "replicas.min") {
		t.Errorf("expected error about replicas.min, got: %v", err)
	}
}

func TestValidateTermitePool_NegativeMin(t *testing.T) {
	pool := validPool()
	pool.Spec.Replicas.Min = -1

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for negative min replicas")
	}
}

func TestValidateTermitePool_ZeroMax(t *testing.T) {
	pool := validPool()
	pool.Spec.Replicas.Max = 0

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for zero max replicas")
	}
}

func TestValidateTermitePool_ComputeClassWithoutAutopilot(t *testing.T) {
	pool := validPool()
	pool.Spec.GKE = &GKEConfig{
		Autopilot:             false,
		AutopilotComputeClass: "Balanced",
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for compute class without autopilot")
	} else if !strings.Contains(err.Error(), "autopilotComputeClass") {
		t.Errorf("expected error about autopilotComputeClass, got: %v", err)
	}
}

func TestValidateTermitePool_AcceleratorWithoutGPU(t *testing.T) {
	pool := validPool()
	pool.Spec.GKE = &GKEConfig{
		Autopilot:             true,
		AutopilotComputeClass: "Accelerator",
	}
	// No GPU resources

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for Accelerator without GPU resources")
	} else if !strings.Contains(err.Error(), "Accelerator") {
		t.Errorf("expected error about Accelerator, got: %v", err)
	}
}

func TestValidateTermitePool_AcceleratorWithGPU(t *testing.T) {
	pool := validPool()
	pool.Spec.GKE = &GKEConfig{
		Autopilot:             true,
		AutopilotComputeClass: "Accelerator",
	}
	pool.Spec.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		},
	}

	if err := pool.ValidateTermitePool(); err != nil {
		t.Errorf("expected no error for Accelerator with GPU, got: %v", err)
	}
}

func TestValidateTermitePool_SpotConflictsWithAutopilot(t *testing.T) {
	pool := validPool()
	pool.Spec.GKE = &GKEConfig{Autopilot: true}
	pool.Spec.Hardware.Spot = true

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for spot + autopilot conflict")
	} else if !strings.Contains(err.Error(), "spot") {
		t.Errorf("expected error about spot, got: %v", err)
	}
}

func TestValidateTermitePool_NodeSelectorConflictsWithAutopilot(t *testing.T) {
	pool := validPool()
	pool.Spec.GKE = &GKEConfig{Autopilot: true}
	pool.Spec.NodeSelector = map[string]string{"node": "gpu"}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for nodeSelector + autopilot conflict")
	} else if !strings.Contains(err.Error(), "nodeSelector") {
		t.Errorf("expected error about nodeSelector, got: %v", err)
	}
}

func TestValidateTermitePool_GKEAndEKSConflict(t *testing.T) {
	pool := validPool()
	pool.Spec.GKE = &GKEConfig{Autopilot: true}
	pool.Spec.EKS = &EKSConfig{Enabled: true}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for GKE + EKS conflict")
	}
}

func TestValidateTermitePool_GKENonAutopilotAndEKSConflict(t *testing.T) {
	pool := validPool()
	pool.Spec.GKE = &GKEConfig{Autopilot: false}
	pool.Spec.EKS = &EKSConfig{Enabled: true}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for non-Autopilot GKE + EKS conflict")
	}
}

func TestValidateImmutability_AutopilotChange(t *testing.T) {
	old := validPool()
	old.Spec.GKE = &GKEConfig{Autopilot: true, AutopilotComputeClass: "Balanced"}

	new := old.DeepCopy()
	new.Spec.GKE.Autopilot = false

	err := new.ValidateImmutability(old)
	if err == nil {
		t.Error("expected error for changing autopilot")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestValidateImmutability_ComputeClassChange(t *testing.T) {
	old := validPool()
	old.Spec.GKE = &GKEConfig{Autopilot: true, AutopilotComputeClass: "Balanced"}

	new := old.DeepCopy()
	new.Spec.GKE.AutopilotComputeClass = "Performance"

	err := new.ValidateImmutability(old)
	if err == nil {
		t.Error("expected error for changing compute class")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestValidateImmutability_NoChange(t *testing.T) {
	old := validPool()
	old.Spec.GKE = &GKEConfig{Autopilot: true, AutopilotComputeClass: "Balanced"}

	new := old.DeepCopy()

	if err := new.ValidateImmutability(old); err != nil {
		t.Errorf("expected no error when nothing changed, got: %v", err)
	}
}

func TestValidateEKS_ValidIRSARoleARN(t *testing.T) {
	pool := validPool()
	pool.Spec.EKS = &EKSConfig{
		Enabled:     true,
		IRSARoleARN: "arn:aws:iam::123456789012:role/termite-role",
	}

	if err := pool.ValidateTermitePool(); err != nil {
		t.Errorf("expected no error for valid IRSA ARN, got: %v", err)
	}
}

func TestValidateEKS_InvalidIRSARoleARN(t *testing.T) {
	pool := validPool()
	pool.Spec.EKS = &EKSConfig{
		Enabled:     true,
		IRSARoleARN: "not-an-arn",
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for invalid IRSA ARN")
	} else if !strings.Contains(err.Error(), "IRSA") {
		t.Errorf("expected error about IRSA, got: %v", err)
	}
}

func TestValidateEKS_ValidInstanceTypes(t *testing.T) {
	pool := validPool()
	pool.Spec.EKS = &EKSConfig{
		Enabled:       true,
		InstanceTypes: []string{"m5.large", "c5.xlarge"},
	}

	if err := pool.ValidateTermitePool(); err != nil {
		t.Errorf("expected no error for valid instance types, got: %v", err)
	}
}

func TestValidateEKS_InvalidInstanceType(t *testing.T) {
	pool := validPool()
	pool.Spec.EKS = &EKSConfig{
		Enabled:       true,
		InstanceTypes: []string{"INVALID"},
	}

	err := pool.ValidateTermitePool()
	if err == nil {
		t.Error("expected error for invalid instance type format")
	}
}

func TestValidateEKS_HyphenatedInstanceTypes(t *testing.T) {
	validTypes := []string{"u-6tb1.56xlarge", "mac2-m2.metal", "m5.large", "c5.xlarge", "p3dn.24xlarge"}
	for _, it := range validTypes {
		t.Run(it, func(t *testing.T) {
			pool := validPool()
			pool.Spec.EKS = &EKSConfig{
				Enabled:       true,
				InstanceTypes: []string{it},
			}

			if err := pool.ValidateTermitePool(); err != nil {
				t.Errorf("expected no error for instance type %s, got: %v", it, err)
			}
		})
	}
}

func TestValidateEKS_ClearlyInvalidInstanceTypes(t *testing.T) {
	invalidTypes := []string{"INVALID", "m5", ".large", ""}
	for _, it := range invalidTypes {
		name := it
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			pool := validPool()
			pool.Spec.EKS = &EKSConfig{
				Enabled:       true,
				InstanceTypes: []string{it},
			}

			if err := pool.ValidateTermitePool(); err == nil {
				t.Errorf("expected error for instance type %q, got nil", it)
			}
		})
	}
}

func TestValidateTermitePool_AllowsCustomAntflyCompatibleImage(t *testing.T) {
	pool := validPool()
	pool.Spec.Image = "ghcr.io/acme/custom-antfly-runtime:latest"

	if err := pool.ValidateTermitePool(); err != nil {
		t.Fatalf("expected no error for custom Antfly-compatible image, got: %v", err)
	}
}
