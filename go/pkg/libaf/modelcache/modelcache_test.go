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

package modelcache

import (
	"context"
	"errors"
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

func TestParseModelRefRejectsUnsafePathComponents(t *testing.T) {
	for _, model := range []string{
		"../clipclap",
		"antflydb/..",
		"./clipclap",
		"antflydb/.",
		"antflydb/clipclap/extra",
	} {
		if _, err := ParseModelRef(model); err == nil {
			t.Fatalf("ParseModelRef(%q) succeeded, want error", model)
		}
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
	modelDir, err := PullHuggingFaceModel(context.Background(), "antflydb/clipclap:gguf:Q4_K", ModelSpec{
		Task:           "embedder",
		DefaultFormat:  ModelFormatGGUF,
		DefaultVariant: "Q4_K",
	}, ModelPullOptions{
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

func TestPullHuggingFaceModelRequiresMatchingArtifact(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/antflydb/clipclap/tree/main":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"path":"config.json","type":"file","size":2},
				{"path":"clipclap-Q4_K.gguf","type":"file","size":4}
			]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	modelsDir := t.TempDir()
	modelDir, err := PullHuggingFaceModel(context.Background(), "antflydb/clipclap:gguf:Q5_K", ModelSpec{
		Task:           "embedder",
		DefaultFormat:  ModelFormatGGUF,
		DefaultVariant: "Q4_K",
	}, ModelPullOptions{
		ModelsDir:          modelsDir,
		HuggingFaceBaseURL: server.URL,
	})
	if err == nil {
		t.Fatalf("PullHuggingFaceModel returned modelDir=%q, want missing artifact error", modelDir)
	}
	if _, statErr := os.Stat(filepath.Join(modelsDir, "antflydb", "clipclap", "model_manifest.json")); !os.IsNotExist(statErr) {
		t.Fatalf("manifest should not be written without matching artifact, stat err=%v", statErr)
	}
}

func TestPullHuggingFaceModelDoesNotWriteSupportBeforeArtifact(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/antflydb/clipclap/tree/main":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"path":"config.json","type":"file","size":2},
				{"path":"clipclap-Q4_K.gguf","type":"file","size":4}
			]`))
		case "/antflydb/clipclap/resolve/main/config.json":
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	modelsDir := t.TempDir()
	modelDir, err := PullHuggingFaceModel(context.Background(), "antflydb/clipclap:gguf:Q4_K", ModelSpec{
		Task:           "embedder",
		DefaultFormat:  ModelFormatGGUF,
		DefaultVariant: "Q4_K",
	}, ModelPullOptions{
		ModelsDir:          modelsDir,
		HuggingFaceBaseURL: server.URL,
	})
	if err == nil {
		t.Fatalf("PullHuggingFaceModel returned modelDir=%q, want artifact download error", modelDir)
	}
	if _, statErr := os.Stat(filepath.Join(modelsDir, "antflydb", "clipclap", "config.json")); !os.IsNotExist(statErr) {
		t.Fatalf("support config should not be written before artifact succeeds, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(modelsDir, "antflydb", "clipclap", "model_manifest.json")); !os.IsNotExist(statErr) {
		t.Fatalf("manifest should not be written after artifact failure, stat err=%v", statErr)
	}
}

func TestPullHuggingFaceModelPreservesRepoRelativePaths(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/antflydb/gliner2-base-v1/tree/main":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"path":"config.json","type":"file","size":4},
				{"path":"encoder_config/config.json","type":"file","size":4},
				{"path":"gliner2-encoder.Q4_K.gguf","type":"file","size":7},
				{"path":"gliner2-head.Q4_K.gguf","type":"file","size":4}
			]`))
		case "/antflydb/gliner2-base-v1/resolve/main/config.json":
			_, _ = w.Write([]byte(`root`))
		case "/antflydb/gliner2-base-v1/resolve/main/encoder_config/config.json":
			_, _ = w.Write([]byte(`encd`))
		case "/antflydb/gliner2-base-v1/resolve/main/gliner2-encoder.Q4_K.gguf":
			_, _ = w.Write([]byte(`encoder`))
		case "/antflydb/gliner2-base-v1/resolve/main/gliner2-head.Q4_K.gguf":
			_, _ = w.Write([]byte(`head`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	modelsDir := t.TempDir()
	modelDir, err := PullHuggingFaceModel(context.Background(), "antflydb/gliner2-base-v1:gguf:Q4_K", ModelSpec{
		Task:           "extractor",
		DefaultFormat:  ModelFormatGGUF,
		DefaultVariant: "Q4_K",
	}, ModelPullOptions{
		ModelsDir:          modelsDir,
		HuggingFaceBaseURL: server.URL,
	})
	if err != nil {
		t.Fatalf("PullHuggingFaceModel: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(modelDir, "config.json")); err != nil || string(data) != "root" {
		t.Fatalf("root config = %q, %v; want root", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(modelDir, "encoder_config", "config.json")); err != nil || string(data) != "encd" {
		t.Fatalf("encoder config = %q, %v; want encd", data, err)
	}
	manifest, err := os.ReadFile(filepath.Join(modelDir, "model_manifest.json"))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if !strings.Contains(string(manifest), `"name": "encoder_config/config.json"`) {
		t.Fatalf("manifest did not record repo-relative nested path: %s", manifest)
	}
}

func TestPullHuggingFaceModelReportsResumeAndResolvedSource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/antflydb/clipclap/tree/main":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"path":"config.json","type":"file","size":2},
				{"path":"clipclap-Q4_K.gguf","type":"file","size":4}
			]`))
		case "/antflydb/clipclap/resolve/main/clipclap-Q4_K.gguf":
			_, _ = w.Write([]byte(`q4_k`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	modelsDir := t.TempDir()
	modelDir := filepath.Join(modelsDir, "antflydb", "clipclap")
	if err := os.MkdirAll(modelDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modelDir, "config.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	var progress []ModelPullProgress
	_, err := PullHuggingFaceModel(context.Background(), "antflydb/clipclap", ModelSpec{
		Task:           "embedder",
		DefaultFormat:  ModelFormatGGUF,
		DefaultVariant: "Q4_K",
	}, ModelPullOptions{
		ModelsDir:          modelsDir,
		HuggingFaceBaseURL: server.URL,
		Progress: func(p ModelPullProgress) {
			progress = append(progress, p)
		},
	})
	if err != nil {
		t.Fatalf("PullHuggingFaceModel: %v", err)
	}
	if len(progress) == 0 {
		t.Fatal("expected progress events")
	}
	first := progress[0]
	if first.Downloaded != 2 || first.Total != 6 || first.ResumedBytes != 2 || first.BlobsDone != 1 || first.BlobsTotal != 2 {
		t.Fatalf("initial progress = %+v, want resumed config.json accounted", first)
	}
	last := progress[len(progress)-1]
	if last.Downloaded != 6 || last.BlobsDone != 2 || last.BlobsTotal != 2 || last.ResumedBytes != 2 {
		t.Fatalf("final progress = %+v, want completed pull with resume metadata", last)
	}

	manifest, err := os.ReadFile(filepath.Join(modelDir, "model_manifest.json"))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if !strings.Contains(string(manifest), `"source": "antflydb/clipclap:gguf:Q4_K"`) {
		t.Fatalf("manifest did not record resolved tagged source: %s", manifest)
	}
}

func TestPullHuggingFaceModelDiskSpaceError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/antflydb/clipclap/tree/main":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"path":"clipclap-Q4_K.gguf","type":"file","size":100}
			]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, err := PullHuggingFaceModel(context.Background(), "antflydb/clipclap", ModelSpec{
		Task:           "embedder",
		DefaultFormat:  ModelFormatGGUF,
		DefaultVariant: "Q4_K",
	}, ModelPullOptions{
		ModelsDir:          t.TempDir(),
		HuggingFaceBaseURL: server.URL,
		DiskFreeBytes: func(string) (int64, error) {
			return 50, nil
		},
	})
	var pullErr *ModelPullError
	if !errors.As(err, &pullErr) || pullErr.Code != "disk_space" {
		t.Fatalf("error = %v, want disk_space ModelPullError", err)
	}
}
