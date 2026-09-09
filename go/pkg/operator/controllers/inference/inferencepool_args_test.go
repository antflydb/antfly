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
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
)

func TestInferenceStatefulSetModelArguments(t *testing.T) {
	for _, strategy := range []api.LoadingStrategy{"", api.LoadingStrategyEager, api.LoadingStrategyLazy, api.LoadingStrategyBounded} {
		t.Run(string(strategy), func(t *testing.T) {
			g := NewWithT(t)
			scheme := newInferenceUnitTestScheme(g)
			maxLoaded := 3
			pool := &api.InferencePool{
				ObjectMeta: metav1.ObjectMeta{Name: "models", Namespace: "default", UID: "pool-uid"},
				Spec: api.InferencePoolSpec{Models: api.ModelConfig{
					LoadingStrategy: strategy, MaxLoadedModels: &maxLoaded,
					Preload: []api.ModelSpec{
						{Name: "hf:owner/embed:gguf:Q4_K", Tasks: []string{"embed"}},
						{Name: "owner/extract:gguf:Q4_K", Tasks: []string{"extract"}, Strategy: api.LoadingStrategyEager},
						{Name: "owner/lazy:i8", Tasks: []string{"embed"}, Strategy: api.LoadingStrategyLazy},
					},
				}},
			}
			client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
			r := &InferencePoolReconciler{Client: client, Scheme: scheme, AntflyImage: "antfly:v0.2.1"}
			ctx := context.Background()
			g.Expect(r.reconcileStatefulSet(ctx, pool)).To(Succeed())
			sts := &appsv1.StatefulSet{}
			key := types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}
			g.Expect(client.Get(ctx, key, sts)).To(Succeed())
			want := []string{"inference", "run", "--host", "0.0.0.0", "--port", "8080", "--config", "/config/config.json", "--allow-insecure-public-bind", "--models-dir", "/models", "--max-loaded-models", "3"}
			if strategy == "" || strategy == api.LoadingStrategyEager {
				want = append(want, "--preload-model", "embedder:owner/embed:gguf:Q4_K")
			}
			want = append(want, "--preload-model", "extractor:owner/extract:gguf:Q4_K")
			g.Expect(sts.Spec.Template.Spec.Containers[0].Args).To(Equal(want))
			// All models are downloaded, even when only the eager subset is warmed.
			g.Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(3))
			for _, c := range sts.Spec.Template.Spec.InitContainers {
				g.Expect(c.Args[3:5]).To(Equal([]string{"--models-dir", "/models"}))
			}

			// A config override must affect the pullers, server and shared mount,
			// and must change the template hash so existing pools roll forward.
			oldHash := sts.Spec.Template.Annotations["inference.antfly.io/template-hash"]
			pool.Spec.Config = `{"models_dir":"/custom-models","max_loaded_models":2,"preload":[{"kind":"embedder","backend":"cuda","name":"owner/embed","format":"gguf","quantization":"Q4_K"}]}`
			g.Expect(r.reconcileStatefulSet(ctx, pool)).To(Succeed())
			g.Expect(client.Get(ctx, key, sts)).To(Succeed())
			g.Expect(sts.Spec.Template.Annotations["inference.antfly.io/template-hash"]).NotTo(Equal(oldHash))
			g.Expect(sts.Spec.Template.Spec.Containers[0].Args).To(Equal([]string{
				"inference", "run", "--host", "0.0.0.0", "--port", "8080", "--config", "/config/config.json", "--allow-insecure-public-bind",
				"--models-dir", "/custom-models", "--max-loaded-models", "2", "--preload-model", "embedder:cuda:owner/embed:gguf:Q4_K",
			}))
			g.Expect(sts.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{Name: "models", MountPath: "/custom-models"}))
			for _, c := range sts.Spec.Template.Spec.InitContainers {
				g.Expect(c.Args[3:5]).To(Equal([]string{"--models-dir", "/custom-models"}))
				g.Expect(c.VolumeMounts).To(ContainElement(corev1.VolumeMount{Name: "models", MountPath: "/custom-models"}))
			}
		})
	}
}

func TestInferenceModelArgsValidation(t *testing.T) {
	for _, config := range []string{
		`{"models_dir":123}`, `{"models_dir":"relative"}`, `{"models_dir":"/"}`, `{"models_dir":"/config/models"}`,
		`{"models_dir":"/models","ml_dir":""}`, `{"models_dir":"/models","ml_dir":"relative"}`,
		`{"models_dir":"/models","max_loaded_models":-1}`, `{"models_dir":"/models","max_loaded_models":1.5}`,
		`{"models_dir":"/models","preload":[{"kind":"bogus","name":"owner/model"}]}`,
		`{"models_dir":"/models","preload":[{"backend":"bogus","name":"owner/model"}]}`,
		`{"models_dir":"/models","preload":[{}]}`,
		`{"models_dir":"/models","preload":[{"name":"owner/model:gguf:Q4_K","format":"onnx"}]}`,
	} {
		t.Run(config, func(t *testing.T) {
			_, _, err := inferenceModelArgs(config)
			if err == nil {
				t.Fatal("expected invalid model configuration to fail")
			}
		})
	}
}

func TestInferenceResolvedConfigLegacyCompatibility(t *testing.T) {
	for _, input := range []string{
		`{"inference":{"preload":[]}}`,
		`{"inference":{"preload":[{"name":"hf:owner/model","format":"gguf","quantization":"Q4_K","residency_mode":"streamed","memory_budget_mb":4096}]}}`,
		`{"preload":[{"name":"hf:owner/model","format":"gguf","quantization":"Q4_K","residency_mode":"streamed","memory_budget_mb":4096}]}`,
	} {
		t.Run(input, func(t *testing.T) {
			g := NewWithT(t)
			pool := &api.InferencePool{Spec: api.InferencePoolSpec{Config: input}}
			raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
			g.Expect(err).NotTo(HaveOccurred())
			var resolved map[string]any
			g.Expect(json.Unmarshal([]byte(raw), &resolved)).To(Succeed())
			g.Expect(resolved).NotTo(HaveKey("inference"))
			preload := resolved["preload"].([]any)
			if len(preload) > 0 {
				model := preload[0].(map[string]any)
				g.Expect(model["kind"]).To(Equal("generator"))
				g.Expect(model["name"]).To(Equal("owner/model:gguf:Q4_K"))
				g.Expect(model["residency_mode"]).To(Equal("streamed"))
				g.Expect(model["memory_budget_mb"]).To(Equal(float64(4096)))
			}
		})
	}
}

func TestInferenceResolvedConfigPreservesNonModelSettings(t *testing.T) {
	g := NewWithT(t)
	pool := &api.InferencePool{Spec: api.InferencePoolSpec{Config: `{"inference":{"keep_alive_ms":42,"ml_dir":"/traditional-models","preload":[]},"admission":{"inference":{"max_concurrent_requests":3}}}`}}
	raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	var resolved map[string]any
	g.Expect(json.Unmarshal([]byte(raw), &resolved)).To(Succeed())
	g.Expect(resolved["inference"]).To(Equal(map[string]any{"api_url": "", "keep_alive_ms": float64(42)}))
	g.Expect(resolved["admission"]).To(Equal(map[string]any{"inference": map[string]any{"max_concurrent_requests": float64(3)}}))
	_, args, err := inferenceModelArgs(raw)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(args).To(Equal([]string{"--models-dir", "/models", "--ml-dir", "/traditional-models"}))
	for _, invalid := range []string{`{"preload":null}`, `{"preload":[null]}`, `{"preload":[{"name":123}]}`} {
		pool.Spec.Config = invalid
		_, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
		g.Expect(err).To(HaveOccurred(), invalid)
	}
}

func TestInferenceNestedConfigOverridesGeneratedModelSettings(t *testing.T) {
	for _, preload := range []string{`[]`, `[{"kind":"generator","name":"owner/custom","backend":"cuda","format":"gguf","quantization":"Q4_K","residency_mode":"streamed","memory_budget_mb":4096}]`} {
		t.Run(preload, func(t *testing.T) {
			g := NewWithT(t)
			scheme := newInferenceUnitTestScheme(g)
			pool := &api.InferencePool{
				ObjectMeta: metav1.ObjectMeta{Name: "nested", Namespace: "default", UID: "nested-uid"},
				Spec: api.InferencePoolSpec{
					Models: api.ModelConfig{Preload: []api.ModelSpec{{Name: "owner/generated", Tasks: []string{"embed"}}}},
					Config: `{"models_dir":"/flat","max_loaded_models":9,"inference":{"models_dir":"/custom-models","max_loaded_models":0,"preload":` + preload + `}}`,
				},
			}
			client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
			r := &InferencePoolReconciler{Client: client, Scheme: scheme}
			ctx := context.Background()
			g.Expect(r.reconcileConfigMap(ctx, pool)).To(Succeed())
			g.Expect(r.reconcileStatefulSet(ctx, pool)).To(Succeed())
			sts := &appsv1.StatefulSet{}
			g.Expect(client.Get(ctx, types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}, sts)).To(Succeed())
			want := []string{"inference", "run", "--host", "0.0.0.0", "--port", "8080", "--config", "/config/config.json", "--allow-insecure-public-bind", "--models-dir", "/custom-models", "--max-loaded-models", "0"}
			if preload != "[]" {
				want = append(want, "--preload-model", "generator:cuda:owner/custom:gguf:Q4_K")
			}
			g.Expect(sts.Spec.Template.Spec.Containers[0].Args).To(Equal(want))
			g.Expect(sts.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{Name: "models", MountPath: "/custom-models"}))
			for _, c := range sts.Spec.Template.Spec.InitContainers {
				g.Expect(c.Args[3:5]).To(Equal([]string{"--models-dir", "/custom-models"}))
				g.Expect(c.VolumeMounts).To(ContainElement(corev1.VolumeMount{Name: "models", MountPath: "/custom-models"}))
			}
			cm := &corev1.ConfigMap{}
			g.Expect(client.Get(ctx, types.NamespacedName{Name: pool.Name + "-config", Namespace: pool.Namespace}, cm)).To(Succeed())
			if preload != "[]" {
				g.Expect(cm.Data["config.json"]).To(ContainSubstring(`"residency_mode": "streamed"`))
				g.Expect(cm.Data["config.json"]).To(ContainSubstring(`"memory_budget_mb": 4096`))
			}
		})
	}
}

func TestInferenceModelArgsEagerCapacityAndEmptyOverride(t *testing.T) {
	g := NewWithT(t)
	pool := &api.InferencePool{}
	for i := range 11 {
		pool.Spec.Models.Preload = append(pool.Spec.Models.Preload, api.ModelSpec{Name: fmt.Sprintf("owner/model-%d:i8", i), Tasks: []string{"embed"}})
	}
	raw, err := (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	_, args, err := inferenceModelArgs(raw)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(args[:4]).To(Equal([]string{"--models-dir", "/models", "--max-loaded-models", "11"}))
	g.Expect(args).To(HaveLen(26))
	pool.Spec.Config = `{"preload":[],"max_loaded_models":0}`
	raw, err = (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	_, args, err = inferenceModelArgs(raw)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(args).To(Equal([]string{"--models-dir", "/models", "--max-loaded-models", "0"}))
	pool.Spec.Config = `{"inference":{"preload":[]}}`
	raw, err = (&InferencePoolReconciler{}).generateCompleteConfig(pool)
	g.Expect(err).NotTo(HaveOccurred())
	_, args, err = inferenceModelArgs(raw)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(args).To(Equal([]string{"--models-dir", "/models"}))
	for _, config := range []string{`null`, `{"inference":null}`, `{"inference":[]}`} {
		pool.Spec.Config = config
		_, err = (&InferencePoolReconciler{}).generateCompleteConfig(pool)
		g.Expect(err).To(HaveOccurred())
	}
}
