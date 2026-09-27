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

package oapi

import "testing"

func TestInferencePreloadUsesAutomaticLoadingPolicy(t *testing.T) {
	doc, err := GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	schema := doc.Components.Schemas["InferenceModelRef"].Value
	model := map[string]any{"kind": "generator", "name": "gemma-a4b", "backend": "cuda"}
	if err := schema.VisitJSON(model); err != nil {
		t.Fatalf("ordinary CUDA preload rejected: %v", err)
	}
	for field, value := range map[string]any{
		"load_strategy":              "pipeline",
		"load_workers":               float64(6),
		"load_staging_mb":            float64(384),
		"prepared_pack":              "required",
		"drop_host_cache_after_load": true,
		"startup_strategy":           "prefetch",
	} {
		t.Run(field, func(t *testing.T) {
			model[field] = value
			defer delete(model, field)
			if err := schema.VisitJSON(model); err == nil {
				t.Fatal("public preload schema accepted model-specific tuning")
			}
		})
	}
}
