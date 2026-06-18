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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
