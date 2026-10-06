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
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	antflyaiv1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
)

func TestGenerateCompleteConfigUsesZigRuntimeContract(t *testing.T) {
	g := NewWithT(t)
	keepAlive := metav1.Duration{Duration: 90 * time.Second}
	maxLoaded := 3
	pool := &antflyaiv1alpha1.InferencePool{
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Models: antflyaiv1alpha1.ModelConfig{
				Preload: []antflyaiv1alpha1.ModelSpec{
					{Name: "hf:owner/model:gguf:Q4_K", Tasks: []string{"generate"}},
				},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyBounded,
				KeepAlive:       &keepAlive,
				MaxLoadedModels: &maxLoaded,
			},
		},
	}

	raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	var config map[string]any
	g.Expect(json.Unmarshal([]byte(raw), &config)).To(Succeed())
	g.Expect(config).To(HaveKeyWithValue("models_dir", "/models"))
	g.Expect(config).To(HaveKeyWithValue("keep_alive_ms", float64(90_000)))
	g.Expect(config).To(HaveKeyWithValue("max_loaded_models", float64(3)))
	g.Expect(config).NotTo(HaveKey("keep_alive"))
	g.Expect(config).NotTo(HaveKey("backend_priority"))
	g.Expect(config).NotTo(HaveKey("preload"))
}

func TestGenerateCompleteConfigBuildsEagerPreloadArtifactSelection(t *testing.T) {
	g := NewWithT(t)
	pool := &antflyaiv1alpha1.InferencePool{
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Models: antflyaiv1alpha1.ModelConfig{
				Preload: []antflyaiv1alpha1.ModelSpec{
					{Name: "hf:owner/model:gguf:Q4_K", Tasks: []string{"generate"}},
				},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
			},
			Hardware: antflyaiv1alpha1.HardwareConfig{Accelerator: "nvidia-l4"},
		},
	}

	raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	var config struct {
		Preload []map[string]any `json:"preload"`
	}
	g.Expect(json.Unmarshal([]byte(raw), &config)).To(Succeed())
	g.Expect(config.Preload).To(HaveLen(1))
	g.Expect(config.Preload[0]).To(HaveKeyWithValue("kind", "generator"))
	g.Expect(config.Preload[0]).To(HaveKeyWithValue("name", "owner/model:gguf:Q4_K"))
	g.Expect(config.Preload[0]).NotTo(HaveKey("backend"))
	g.Expect(config.Preload[0]).To(HaveKeyWithValue("format", "gguf"))
	g.Expect(config.Preload[0]).To(HaveKeyWithValue("quantization", "Q4_K"))
}

// TestGenerateCompleteConfigOrdersPreloadByPriority verifies ModelSpec.Priority
// controls the emitted config.preload order: the Zig runtime
// (server.Node.warmConfiguredModels) warms config.preload sequentially in
// list order, so high-priority models must be emitted before medium, which
// must be emitted before low, regardless of spec.models.preload declaration
// order. Same-priority models keep their declaration order.
func TestGenerateCompleteConfigOrdersPreloadByPriority(t *testing.T) {
	g := NewWithT(t)
	pool := &antflyaiv1alpha1.InferencePool{
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Models: antflyaiv1alpha1.ModelConfig{
				Preload: []antflyaiv1alpha1.ModelSpec{
					{Name: "hf:owner/low-a:gguf", Tasks: []string{"embed"}, Priority: antflyaiv1alpha1.ModelPriorityLow},
					{Name: "hf:owner/medium:gguf", Tasks: []string{"embed"}},
					{Name: "hf:owner/high-a:gguf", Tasks: []string{"embed"}, Priority: antflyaiv1alpha1.ModelPriorityHigh},
					{Name: "hf:owner/low-b:gguf", Tasks: []string{"embed"}, Priority: antflyaiv1alpha1.ModelPriorityLow},
					{Name: "hf:owner/high-b:gguf", Tasks: []string{"embed"}, Priority: antflyaiv1alpha1.ModelPriorityHigh},
				},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
			},
		},
	}

	raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	var config struct {
		Preload []map[string]any `json:"preload"`
	}
	g.Expect(json.Unmarshal([]byte(raw), &config)).To(Succeed())
	g.Expect(config.Preload).To(HaveLen(5))
	names := make([]string, len(config.Preload))
	for i, entry := range config.Preload {
		names[i] = entry["name"].(string)
	}
	g.Expect(names).To(Equal([]string{
		"owner/high-a:gguf", "owner/high-b:gguf",
		"owner/medium:gguf",
		"owner/low-a:gguf", "owner/low-b:gguf",
	}))
}

func TestGenerateCompleteConfigPreservesExplicitRuntimeOverrides(t *testing.T) {
	g := NewWithT(t)
	pool := &antflyaiv1alpha1.InferencePool{
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Config: `{"models_dir":"/custom-models","preload":[{"kind":"embedder","name":"custom/model:i8","backend":"cuda"}]}`,
			Models: antflyaiv1alpha1.ModelConfig{
				Preload: []antflyaiv1alpha1.ModelSpec{
					{Name: "hf:owner/model:i8", Tasks: []string{"embed"}},
				},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
			},
		},
	}

	raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	var config map[string]any
	g.Expect(json.Unmarshal([]byte(raw), &config)).To(Succeed())
	g.Expect(config).To(HaveKeyWithValue("models_dir", "/custom-models"))
	preload, ok := config["preload"].([]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(preload).To(HaveLen(1))
	g.Expect(preload[0]).To(Equal(map[string]any{
		"kind":    "embedder",
		"name":    "custom/model:i8",
		"backend": "cuda",
	}))
}

func TestGenerateCompleteConfigPreservesLazyAcceleratorStrategy(t *testing.T) {
	g := NewWithT(t)
	pool := &antflyaiv1alpha1.InferencePool{
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Models: antflyaiv1alpha1.ModelConfig{
				Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "model"}},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyLazy,
			},
			Hardware: antflyaiv1alpha1.HardwareConfig{Accelerator: "tpu-v5-lite-podslice"},
		},
	}

	raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	var config map[string]any
	g.Expect(json.Unmarshal([]byte(raw), &config)).To(Succeed())
	g.Expect(config).NotTo(HaveKey("preload"))
	g.Expect(config).To(HaveKeyWithValue("keep_alive_ms", float64(defaultInferenceKeepAliveMillis)))
}

func TestInferenceBackendPolicy(t *testing.T) {
	g := NewWithT(t)
	tests := []struct {
		name              string
		pool              *antflyaiv1alpha1.InferencePool
		expectedPreferred string
		expectedRequired  string
	}{
		{
			name: "tpu",
			pool: &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
				Hardware: antflyaiv1alpha1.HardwareConfig{Accelerator: "TPU-v5-lite-podslice"},
			}},
			expectedPreferred: "pjrt",
		},
		{
			name: "gpu accelerator",
			pool: &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
				Hardware: antflyaiv1alpha1.HardwareConfig{Accelerator: "nvidia-l4"},
			}},
			expectedPreferred: "cuda",
			expectedRequired:  "cuda",
		},
		{
			name: "gpu resource",
			pool: &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
				Resources: &corev1.ResourceRequirements{Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				}},
			}},
			expectedPreferred: "cuda",
			expectedRequired:  "cuda",
		},
		{
			name: "cpu",
			pool: &antflyaiv1alpha1.InferencePool{},
		},
		{
			name: "explicit cpu",
			pool: &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
				Hardware: antflyaiv1alpha1.HardwareConfig{InferenceBackend: antflyaiv1alpha1.InferenceRuntimeBackendCPU},
			}},
			expectedPreferred: "native",
			expectedRequired:  "native",
		},
		{
			name: "explicit cuda",
			pool: &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
				Hardware: antflyaiv1alpha1.HardwareConfig{InferenceBackend: antflyaiv1alpha1.InferenceRuntimeBackendCUDA},
			}},
			expectedPreferred: "cuda",
			expectedRequired:  "cuda",
		},
		{
			name: "explicit pjrt",
			pool: &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
				Hardware: antflyaiv1alpha1.HardwareConfig{InferenceBackend: antflyaiv1alpha1.InferenceRuntimeBackendPJRT},
			}},
			expectedPreferred: "pjrt",
			expectedRequired:  "pjrt",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := antflyaiv1alpha1.ResolveInferenceBackendProfile(test.pool)
			g.Expect(profile.PreferredRuntime).To(Equal(test.expectedPreferred))
			g.Expect(profile.RequiredRuntime).To(Equal(test.expectedRequired))
		})
	}
}

func TestReconcileConfigMapRequiresCUDAForGPUPools(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	scheme := runtime.NewScheme()
	g.Expect(antflyaiv1alpha1.AddToScheme(scheme)).To(Succeed())
	g.Expect(corev1.AddToScheme(scheme)).To(Succeed())

	pool := &antflyaiv1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gpu-pool",
			Namespace: "default",
			UID:       types.UID("gpu-pool"),
		},
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Hardware: antflyaiv1alpha1.HardwareConfig{Accelerator: "nvidia-l4"},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
	reconciler := &InferencePoolReconciler{Client: client, Scheme: scheme}

	g.Expect(reconciler.reconcileConfigMap(ctx, pool)).To(Succeed())
	configMap := &corev1.ConfigMap{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "gpu-pool-config", Namespace: "default"}, configMap)).To(Succeed())
	g.Expect(configMap.Data).To(HaveKeyWithValue("ANTFLY_INFERENCE_PREFERRED_BACKEND", "cuda"))
	g.Expect(configMap.Data).To(HaveKeyWithValue("ANTFLY_INFERENCE_REQUIRED_BACKEND", "cuda"))
}

func TestEnsureTPUResourcesIgnoresNonTPUAccelerator(t *testing.T) {
	g := NewWithT(t)
	resources := corev1.ResourceRequirements{}
	pool := &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
		Hardware: antflyaiv1alpha1.HardwareConfig{Accelerator: "nvidia-l4"},
	}}

	(&InferencePoolReconciler{}).ensureTPUResources(&resources, pool)
	_, requested := resources.Requests["google.com/tpu"]
	_, limited := resources.Limits["google.com/tpu"]
	g.Expect(requested).To(BeFalse())
	g.Expect(limited).To(BeFalse())
}

func TestApplyGKEPodSpecSelectsL4GPU(t *testing.T) {
	g := NewWithT(t)
	template := &corev1.PodTemplateSpec{}
	pool := &antflyaiv1alpha1.InferencePool{Spec: antflyaiv1alpha1.InferencePoolSpec{
		Hardware: antflyaiv1alpha1.HardwareConfig{Accelerator: "nvidia-l4"},
		Resources: &corev1.ResourceRequirements{Limits: corev1.ResourceList{
			"nvidia.com/gpu": resource.MustParse("1"),
		}},
		GKE: &antflyaiv1alpha1.GKEConfig{Autopilot: true, AutopilotComputeClass: "Accelerator"},
	}}

	(&InferencePoolReconciler{}).applyGKEPodSpec(template, pool)
	g.Expect(template.Spec.NodeSelector).To(HaveKeyWithValue("cloud.google.com/compute-class", "Accelerator"))
	g.Expect(template.Spec.NodeSelector).To(HaveKeyWithValue("cloud.google.com/gke-accelerator", "nvidia-l4"))
}

func TestDesiredInferenceReplicasHonorsActivationWindow(t *testing.T) {
	g := NewWithT(t)
	now := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	idle := metav1.Duration{Duration: 10 * time.Minute}
	pool := &antflyaiv1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu", Namespace: "default", UID: types.UID("gpu-uid")},
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Replicas:    antflyaiv1alpha1.ReplicaConfig{Min: 0, Max: 1},
			ScaleToZero: &antflyaiv1alpha1.ScaleToZeroConfig{Enabled: true, IdleTimeout: &idle},
		},
	}
	holder := "gpu-uid"
	// The operator owns the idle policy and must not trust a proxy-supplied
	// Lease duration that is longer than the pool configuration.
	duration := int32((24 * time.Hour) / time.Second)
	renewedAt := metav1.NewMicroTime(now.Add(-5 * time.Minute))
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu", Namespace: "default", Labels: map[string]string{
			antflyaiv1alpha1.ActivationLeasePoolLabel: "gpu",
		}},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &renewedAt},
	}

	g.Expect(desiredInferenceReplicasAt(pool, lease, now)).To(Equal(int32(1)))
	g.Expect(desiredInferenceReplicasAt(pool, lease, now.Add(6*time.Minute))).To(Equal(int32(0)))
	staleHolder := "previous-pool-uid"
	lease.Spec.HolderIdentity = &staleHolder
	g.Expect(desiredInferenceReplicasAt(pool, lease, now)).To(Equal(int32(0)))
	lease.Spec.HolderIdentity = &holder
	futureRenewal := metav1.NewMicroTime(now.Add(2 * time.Minute))
	lease.Spec.RenewTime = &futureRenewal
	g.Expect(desiredInferenceReplicasAt(pool, lease, now)).To(Equal(int32(0)))
}

func TestZigWarmModelKindUsesRegistryTaskPrecedence(t *testing.T) {
	g := NewWithT(t)
	g.Expect(zigWarmModelKind([]string{"embed"})).To(Equal("embedder"))
	g.Expect(zigWarmModelKind([]string{"generate", "rerank"})).To(Equal("reranker"))
	g.Expect(zigWarmModelKind(nil)).To(BeEmpty())
	g.Expect(zigWarmModelKind([]string{"unknown-task"})).To(BeEmpty())
}

func TestInferenceWarmModelNamePreservesVariant(t *testing.T) {
	g := NewWithT(t)
	g.Expect(inferenceWarmModelName("BAAI/bge-small-en-v1.5:i8")).To(Equal("BAAI/bge-small-en-v1.5:i8"))
	g.Expect(inferenceWarmModelName("hf:owner/model:i8")).To(Equal("owner/model:i8"))
	g.Expect(inferenceWarmModelName("owner/model")).To(Equal("owner/model"))
	g.Expect(inferenceWarmModelName("s3://bucket/model")).To(Equal("s3://bucket/model"))
}

// TestProcessMemoryBudgetMB covers the ANTFLY_PROCESS_MEMORY_BUDGET_MB
// derivation: 90% of an explicit memory limit, in MiB, and no budget when no
// limit is set (an unbounded/"max" cgroup gives nothing safe to derive from).
func TestProcessMemoryBudgetMB(t *testing.T) {
	g := NewWithT(t)

	budget, ok := processMemoryBudgetMB(corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("8Gi"),
	})
	g.Expect(ok).To(BeTrue())
	g.Expect(budget).To(Equal(int64(7372)))

	_, ok = processMemoryBudgetMB(nil)
	g.Expect(ok).To(BeFalse())

	_, ok = processMemoryBudgetMB(corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"),
	})
	g.Expect(ok).To(BeFalse())
}

// TestReconcileStatefulSetSetsProcessMemoryBudget verifies the InferencePool
// pod gets ANTFLY_PROCESS_MEMORY_BUDGET_MB set below its container memory
// limit. buildResources defaults to a 4Gi/8Gi request/limit split (Burstable
// QoS), so this applies even without spec.resources set. See
// zig/MANAGERS.md "Budget derivation".
func TestReconcileStatefulSetSetsProcessMemoryBudget(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newInferenceUnitTestScheme(g)
	pool := &antflyaiv1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{Name: "budget-pool", Namespace: "default", UID: types.UID("budget-pool")},
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Models: antflyaiv1alpha1.ModelConfig{Preload: []antflyaiv1alpha1.ModelSpec{{Name: "test-model", Tasks: []string{"embed"}}}},
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(pool).Build()
	reconciler := &InferencePoolReconciler{Client: client, Scheme: s, AntflyImage: "ghcr.io/antflydb/antfly:zig-test"}

	g.Expect(reconciler.reconcileConfigMap(ctx, pool)).To(Succeed())
	g.Expect(reconciler.reconcileStatefulSet(ctx, pool)).To(Succeed())

	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}, sts)).To(Succeed())
	var container corev1.Container
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == "inference" {
			container = c
		}
	}
	g.Expect(container.Name).To(Equal("inference"))
	var budgetValue string
	var found bool
	for _, env := range container.Env {
		if env.Name == antflyProcessMemoryBudgetEnvVar {
			found = true
			budgetValue = env.Value
		}
	}
	g.Expect(found).To(BeTrue(), "expected %s to be set", antflyProcessMemoryBudgetEnvVar)
	g.Expect(budgetValue).To(Equal("7372")) // 90% of the default 8Gi limit, in MiB
}
