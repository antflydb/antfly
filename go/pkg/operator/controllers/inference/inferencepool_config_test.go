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
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestGenerateCompleteConfigPinsAcceleratorPreloadBackend(t *testing.T) {
	g := NewWithT(t)
	pool := &antflyaiv1alpha1.InferencePool{
		Spec: antflyaiv1alpha1.InferencePoolSpec{
			Models: antflyaiv1alpha1.ModelConfig{
				Preload: []antflyaiv1alpha1.ModelSpec{
					{Name: "hf:owner/model:gguf:Q4_K", Tasks: []string{"generate"}},
				},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyLazy,
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
	g.Expect(config.Preload[0]).To(HaveKeyWithValue("backend", "cuda"))
	g.Expect(config.Preload[0]).To(HaveKeyWithValue("format", "gguf"))
	g.Expect(config.Preload[0]).To(HaveKeyWithValue("quantization", "Q4_K"))
}

func TestZigWarmModelKindUsesRegistryTaskPrecedence(t *testing.T) {
	g := NewWithT(t)
	g.Expect(zigWarmModelKind([]string{"embed"})).To(Equal("embedder"))
	g.Expect(zigWarmModelKind([]string{"generate", "rerank"})).To(Equal("reranker"))
	g.Expect(zigWarmModelKind(nil)).To(Equal("generator"))
}
