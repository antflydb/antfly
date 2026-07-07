/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestModelDirUsesAntflyInferenceLayout(t *testing.T) {
	dir, err := ModelDir("/tmp/models", "antflydb/clipclap:gguf:Q4_K")
	if err != nil {
		t.Fatalf("ModelDir: %v", err)
	}
	if got, want := dir, filepath.Join("/tmp/models", "antflydb", "clipclap"); got != want {
		t.Fatalf("ModelDir = %q, want %q", got, want)
	}
}

func TestSortProfilePreservesAdditionalDiagnostics(t *testing.T) {
	payload := []byte(`{
		"plan": "native_doc_values_top_n",
		"cursor": "after",
		"candidate_count": 7,
		"native_doc_value_load_us": 13,
		"collector_heap_peak": 5
	}`)

	var profile oapi.SortProfile
	if err := json.Unmarshal(payload, &profile); err != nil {
		t.Fatalf("unmarshal SortProfile: %v", err)
	}
	if got, want := profile.Plan, "native_doc_values_top_n"; got != want {
		t.Fatalf("Plan = %q, want %q", got, want)
	}
	if got, want := profile.CandidateCount, int64(7); got != want {
		t.Fatalf("CandidateCount = %d, want %d", got, want)
	}
	if _, found := profile.Get("plan"); found {
		t.Fatalf("stable field plan should not be duplicated in additional properties")
	}
	gotDiagnostic, found := profile.Get("native_doc_value_load_us")
	gotDiagnosticNumber, ok := gotDiagnostic.(float64)
	if !found || !ok || gotDiagnosticNumber != 13 {
		t.Fatalf("native_doc_value_load_us additional property = (%v, %v), want (13, true)", gotDiagnostic, found)
	}

	profile.Set("sorted_segment_scan_budget", 100)
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatalf("marshal SortProfile: %v", err)
	}
	var roundTrip map[string]any
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatalf("unmarshal encoded SortProfile: %v", err)
	}
	if _, found := roundTrip["collector_heap_peak"]; !found {
		t.Fatalf("collector_heap_peak diagnostic was not preserved: %s", encoded)
	}
	if _, found := roundTrip["sorted_segment_scan_budget"]; !found {
		t.Fatalf("sorted_segment_scan_budget diagnostic was not preserved: %s", encoded)
	}
}

func TestPullHuggingFaceModelSelectsClipclapQ4K(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/antflydb/clipclap/tree/main":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"path":"config.json","type":"file","size":2},
				{"path":"clipclap-Q4_K.gguf","type":"file","size":4},
				{"path":"clipclap-F16.gguf","type":"file","size":4}
			]`))
		case "/antflydb/clipclap/resolve/main/config.json":
			_, _ = w.Write([]byte(`{}`))
		case "/antflydb/clipclap/resolve/main/clipclap-Q4_K.gguf":
			_, _ = w.Write([]byte(`q4_k`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	modelsDir := t.TempDir()
	modelDir, err := PullHuggingFaceModel(context.Background(), "antflydb/clipclap:gguf:Q4_K", ModelPullOptions{
		ModelsDir:          modelsDir,
		HuggingFaceBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("PullHuggingFaceModel: %v", err)
	}
	if got, want := modelDir, filepath.Join(modelsDir, "antflydb", "clipclap"); got != want {
		t.Fatalf("model dir = %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(modelDir, "clipclap-Q4_K.gguf")); err != nil {
		t.Fatalf("Q4_K gguf missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(modelDir, "clipclap-F16.gguf")); !os.IsNotExist(err) {
		t.Fatalf("F16 gguf should not be downloaded, err=%v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(modelDir, "model_manifest.json"))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if !strings.Contains(string(manifest), `"variant": "Q4_K"`) {
		t.Fatalf("manifest did not record Q4_K variant: %s", manifest)
	}
	if !strings.Contains(string(manifest), `"source": "antflydb/clipclap:gguf:Q4_K"`) {
		t.Fatalf("manifest did not record tagged source: %s", manifest)
	}
}

func TestPullHuggingFaceModelDefaultsRerankerToQ4K(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/antflydb/mxbai-rerank-base-v1/tree/main":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"path":"config.json","type":"file","size":2},
				{"path":"mxbai-rerank-base-v1.Q4_K.gguf","type":"file","size":4},
				{"path":"mxbai-rerank-base-v1.Q8_0.gguf","type":"file","size":4}
			]`))
		case "/antflydb/mxbai-rerank-base-v1/resolve/main/config.json":
			_, _ = w.Write([]byte(`{}`))
		case "/antflydb/mxbai-rerank-base-v1/resolve/main/mxbai-rerank-base-v1.Q4_K.gguf":
			_, _ = w.Write([]byte(`q4_k`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	modelsDir := t.TempDir()
	modelDir, err := PullHuggingFaceModel(context.Background(), "antflydb/mxbai-rerank-base-v1", ModelPullOptions{
		ModelsDir:          modelsDir,
		HuggingFaceBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("PullHuggingFaceModel: %v", err)
	}
	if _, err := os.Stat(filepath.Join(modelDir, "mxbai-rerank-base-v1.Q4_K.gguf")); err != nil {
		t.Fatalf("Q4_K gguf missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(modelDir, "mxbai-rerank-base-v1.Q8_0.gguf")); !os.IsNotExist(err) {
		t.Fatalf("Q8_0 gguf should not be downloaded, err=%v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(modelDir, "model_manifest.json"))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if !strings.Contains(string(manifest), `"source": "antflydb/mxbai-rerank-base-v1:gguf:Q4_K"`) {
		t.Fatalf("manifest did not record resolved Q4_K source: %s", manifest)
	}
}
