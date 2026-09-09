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
}
