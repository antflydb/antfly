// Copyright 2025 Antfly, Inc.
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

package modelregistry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

// testContent creates deterministic test content and its sha256 digest for a given name.
func testContent(name string) ([]byte, string) {
	content := []byte("content-of-" + name)
	h := sha256.New()
	h.Write(content)
	return content, "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// setupBlobServer creates an httptest server that serves blobs by digest and
// tracks which digests were downloaded. Returns the client and downloaded set.
func setupBlobServer(t *testing.T, blobs map[string][]byte) (*Client, map[string]bool) {
	t.Helper()
	downloaded := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		digest := r.URL.Path[len("/v1/blobs/"):]
		if content, ok := blobs[digest]; ok {
			downloaded[digest] = true
			_, _ = w.Write(content)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	client := NewClient(WithBaseURL(server.URL+"/v1"), WithLogger(zap.NewNop()))
	return client, downloaded
}

func TestClientFetchIndex(t *testing.T) {
	t.Run("schema v1", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/index.json" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{
					"schemaVersion": 1,
					"models": [
						{"name": "bge-small", "type": "embedder", "size": 1000}
					]
				}`))
				return
			}
			http.NotFound(w, r)
		}))
		defer server.Close()

		client := NewClient(WithBaseURL(server.URL + "/v1"))

		index, err := client.FetchIndex(context.Background())
		if err != nil {
			t.Fatalf("FetchIndex() error = %v", err)
		}

		if len(index.Models) != 1 {
			t.Errorf("len(Models) = %v, want 1", len(index.Models))
		}
		if index.Models[0].Name != "bge-small" {
			t.Errorf("Models[0].Name = %v, want bge-small", index.Models[0].Name)
		}
	})

	t.Run("schema v2", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/index.json" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{
					"schemaVersion": 2,
					"models": [
						{"name": "bge-small-en-v1.5", "owner": "BAAI", "source": "BAAI/bge-small-en-v1.5", "type": "embedder", "size": 1000, "variants": ["i8", "f16"]},
						{"name": "mxbai-rerank-base-v1", "owner": "mixedbread-ai", "source": "mixedbread-ai/mxbai-rerank-base-v1", "type": "reranker", "size": 2000}
					]
				}`))
				return
			}
			http.NotFound(w, r)
		}))
		defer server.Close()

		client := NewClient(WithBaseURL(server.URL + "/v1"))

		index, err := client.FetchIndex(context.Background())
		if err != nil {
			t.Fatalf("FetchIndex() error = %v", err)
		}

		if index.SchemaVersion != 2 {
			t.Errorf("SchemaVersion = %v, want 2", index.SchemaVersion)
		}
		if len(index.Models) != 2 {
			t.Errorf("len(Models) = %v, want 2", len(index.Models))
		}
		if index.Models[0].Owner != "BAAI" {
			t.Errorf("Models[0].Owner = %v, want BAAI", index.Models[0].Owner)
		}
		if index.Models[0].Source != "BAAI/bge-small-en-v1.5" {
			t.Errorf("Models[0].Source = %v, want BAAI/bge-small-en-v1.5", index.Models[0].Source)
		}
		if len(index.Models[0].Variants) != 2 {
			t.Errorf("Models[0].Variants = %v, want [i8, f16]", index.Models[0].Variants)
		}
	})
}

func TestClientFetchManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/manifests/bge-small.json" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"schemaVersion": 1,
				"name": "bge-small",
				"type": "embedder",
				"files": [
					{"name": "model.onnx", "digest": "sha256:abc123", "size": 1000}
				]
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL + "/v1"))

	t.Run("existing model", func(t *testing.T) {
		manifest, err := client.FetchManifest(context.Background(), "bge-small")
		if err != nil {
			t.Fatalf("FetchManifest() error = %v", err)
		}
		if manifest.Name != "bge-small" {
			t.Errorf("Name = %v, want bge-small", manifest.Name)
		}
	})

	t.Run("non-existent model", func(t *testing.T) {
		_, err := client.FetchManifest(context.Background(), "not-found")
		if err == nil {
			t.Error("Expected error for non-existent model")
		}
	})
}

func TestClientPullModel(t *testing.T) {
	// Create test file content and its hash
	testContent := []byte("test model content")
	hasher := sha256.New()
	hasher.Write(testContent)
	testDigest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/blobs/"+testDigest {
			_, _ = w.Write(testContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient(
		WithBaseURL(server.URL+"/v1"),
		WithLogger(zap.NewNop()),
	)

	manifest := &ModelManifest{
		SchemaVersion: 1,
		Name:          "test-model",
		Type:          ModelTypeEmbedder,
		Files: []ModelFile{
			{Name: "model.onnx", Digest: testDigest, Size: int64(len(testContent))},
		},
	}

	// Create temp directory
	tmpDir := t.TempDir()

	err := client.PullModel(context.Background(), manifest, tmpDir, nil)
	if err != nil {
		t.Fatalf("PullModel() error = %v", err)
	}

	// Verify file was created
	modelPath := filepath.Join(tmpDir, "embedders", "test-model", "model.onnx")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}

	if string(content) != string(testContent) {
		t.Errorf("File content mismatch")
	}
}

func TestClientSkipsExistingFile(t *testing.T) {
	// Create test file content and its hash
	testContent := []byte("existing content")
	hasher := sha256.New()
	hasher.Write(testContent)
	testDigest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))

	downloadCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloadCount++
		_, _ = w.Write(testContent)
	}))
	defer server.Close()

	client := NewClient(
		WithBaseURL(server.URL+"/v1"),
		WithLogger(zap.NewNop()),
	)

	manifest := &ModelManifest{
		SchemaVersion: 1,
		Name:          "test-model",
		Type:          ModelTypeEmbedder,
		Files: []ModelFile{
			{Name: "model.onnx", Digest: testDigest, Size: int64(len(testContent))},
		},
	}

	// Create temp directory and pre-create the file
	tmpDir := t.TempDir()
	modelDir := filepath.Join(tmpDir, "embedders", "test-model")
	if err := os.MkdirAll(modelDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modelDir, "model.onnx"), testContent, 0644); err != nil {
		t.Fatal(err)
	}

	// Pull should skip the existing file
	err := client.PullModel(context.Background(), manifest, tmpDir, nil)
	if err != nil {
		t.Fatalf("PullModel() error = %v", err)
	}

	// Should not have downloaded
	if downloadCount > 0 {
		t.Errorf("Expected 0 downloads for existing file, got %d", downloadCount)
	}
}

func TestClientHashMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("wrong content"))
	}))
	defer server.Close()

	client := NewClient(
		WithBaseURL(server.URL+"/v1"),
		WithLogger(zap.NewNop()),
	)

	manifest := &ModelManifest{
		SchemaVersion: 1,
		Name:          "test-model",
		Type:          ModelTypeEmbedder,
		Files: []ModelFile{
			{Name: "model.onnx", Digest: "sha256:expected_hash", Size: 100},
		},
	}

	tmpDir := t.TempDir()

	err := client.PullModel(context.Background(), manifest, tmpDir, nil)
	if err == nil {
		t.Error("Expected error for hash mismatch")
	}
}

func TestClientPullModelVariantSkipsF32OnnxData(t *testing.T) {
	// Simulate a CLIP-style multimodal model with .onnx.data external data files
	// and auxiliary projection models (visual_projection.onnx, text_projection.onnx).
	// When pulling only "i8":
	//   - f32 encoder .onnx and .onnx.data files should NOT be downloaded
	//   - auxiliary ONNX files with no variant replacement SHOULD be downloaded
	//   - variant files (visual_model_i8.onnx, text_model_i8.onnx) SHOULD be downloaded
	tokContent, tokDigest := testContent("tokenizer.json")
	visualOnnx, visualOnnxDigest := testContent("visual_model.onnx")
	visualData, visualDataDigest := testContent("visual_model.onnx.data")
	textOnnx, textOnnxDigest := testContent("text_model.onnx")
	textData, textDataDigest := testContent("text_model.onnx.data")
	// Auxiliary projection models — shared across all variants
	visualProj, visualProjDigest := testContent("visual_projection.onnx")
	visualProjData, visualProjDataDigest := testContent("visual_projection.onnx.data")
	textProj, textProjDigest := testContent("text_projection.onnx")
	textProjData, textProjDataDigest := testContent("text_projection.onnx.data")
	visualI8, visualI8Digest := testContent("visual_model_i8.onnx")
	textI8, textI8Digest := testContent("text_model_i8.onnx")

	blobs := map[string][]byte{
		tokDigest:            tokContent,
		visualOnnxDigest:     visualOnnx,
		visualDataDigest:     visualData,
		textOnnxDigest:       textOnnx,
		textDataDigest:       textData,
		visualProjDigest:     visualProj,
		visualProjDataDigest: visualProjData,
		textProjDigest:       textProj,
		textProjDataDigest:   textProjData,
		visualI8Digest:       visualI8,
		textI8Digest:         textI8,
	}

	client, downloaded := setupBlobServer(t, blobs)

	manifest := &ModelManifest{
		SchemaVersion: 2,
		Name:          "clip-vit-base-patch32",
		Owner:         "openai",
		Type:          ModelTypeEmbedder,
		Capabilities:  []string{"image"},
		Files: []ModelFile{
			{Name: "visual_model.onnx", Digest: visualOnnxDigest, Size: int64(len(visualOnnx))},
			{Name: "visual_model.onnx.data", Digest: visualDataDigest, Size: int64(len(visualData))},
			{Name: "text_model.onnx", Digest: textOnnxDigest, Size: int64(len(textOnnx))},
			{Name: "text_model.onnx.data", Digest: textDataDigest, Size: int64(len(textData))},
			{Name: "visual_projection.onnx", Digest: visualProjDigest, Size: int64(len(visualProj))},
			{Name: "visual_projection.onnx.data", Digest: visualProjDataDigest, Size: int64(len(visualProjData))},
			{Name: "text_projection.onnx", Digest: textProjDigest, Size: int64(len(textProj))},
			{Name: "text_projection.onnx.data", Digest: textProjDataDigest, Size: int64(len(textProjData))},
			{Name: "tokenizer.json", Digest: tokDigest, Size: int64(len(tokContent))},
		},
		Variants: map[string]VariantEntry{
			"i8": {Files: []ModelFile{
				{Name: "visual_model_i8.onnx", Digest: visualI8Digest, Size: int64(len(visualI8))},
				{Name: "text_model_i8.onnx", Digest: textI8Digest, Size: int64(len(textI8))},
			}},
		},
	}

	tmpDir := t.TempDir()

	// Pull only i8 variant — f32 encoder files should be skipped,
	// but projection (auxiliary) ONNX files must still be downloaded.
	if err := client.PullModel(context.Background(), manifest, tmpDir, []string{"i8"}); err != nil {
		t.Fatalf("PullModel() error = %v", err)
	}

	// Supporting file should be downloaded
	if !downloaded[tokDigest] {
		t.Error("tokenizer.json should have been downloaded")
	}

	// Variant files should be downloaded
	if !downloaded[visualI8Digest] {
		t.Error("visual_model_i8.onnx should have been downloaded")
	}
	if !downloaded[textI8Digest] {
		t.Error("text_model_i8.onnx should have been downloaded")
	}

	// Auxiliary projection ONNX files should be downloaded (they have no variant replacement)
	if !downloaded[visualProjDigest] {
		t.Error("visual_projection.onnx should have been downloaded (no i8 variant exists for it)")
	}
	if !downloaded[visualProjDataDigest] {
		t.Error("visual_projection.onnx.data should have been downloaded (no i8 variant exists for it)")
	}
	if !downloaded[textProjDigest] {
		t.Error("text_projection.onnx should have been downloaded (no i8 variant exists for it)")
	}
	if !downloaded[textProjDataDigest] {
		t.Error("text_projection.onnx.data should have been downloaded (no i8 variant exists for it)")
	}

	// f32 encoder ONNX files and their .onnx.data should NOT be downloaded
	if downloaded[visualOnnxDigest] {
		t.Error("visual_model.onnx should NOT have been downloaded when only i8 requested")
	}
	if downloaded[visualDataDigest] {
		t.Error("visual_model.onnx.data should NOT have been downloaded when only i8 requested")
	}
	if downloaded[textOnnxDigest] {
		t.Error("text_model.onnx should NOT have been downloaded when only i8 requested")
	}
	if downloaded[textDataDigest] {
		t.Error("text_model.onnx.data should NOT have been downloaded when only i8 requested")
	}
}

func TestClientPullModelF32DownloadsAuxiliaryOnnx(t *testing.T) {
	// When pulling f32, ALL base manifest files should be downloaded — including
	// both large encoder files (visual_model.onnx) and auxiliary projection files
	// (visual_projection.onnx). This guards against regressions in the variantStems path.
	tokContent, tokDigest := testContent("tokenizer.json")
	visualOnnx, visualOnnxDigest := testContent("visual_model.onnx")
	visualData, visualDataDigest := testContent("visual_model.onnx.data")
	visualProj, visualProjDigest := testContent("visual_projection.onnx")
	visualProjData, visualProjDataDigest := testContent("visual_projection.onnx.data")
	visualI8, visualI8Digest := testContent("visual_model_i8.onnx")

	blobs := map[string][]byte{
		tokDigest:            tokContent,
		visualOnnxDigest:     visualOnnx,
		visualDataDigest:     visualData,
		visualProjDigest:     visualProj,
		visualProjDataDigest: visualProjData,
		visualI8Digest:       visualI8,
	}

	client, downloaded := setupBlobServer(t, blobs)

	manifest := &ModelManifest{
		SchemaVersion: 2,
		Name:          "clip-vit-base-patch32",
		Owner:         "openai",
		Type:          ModelTypeEmbedder,
		Capabilities:  []string{"image"},
		Files: []ModelFile{
			{Name: "visual_model.onnx", Digest: visualOnnxDigest, Size: int64(len(visualOnnx))},
			{Name: "visual_model.onnx.data", Digest: visualDataDigest, Size: int64(len(visualData))},
			{Name: "visual_projection.onnx", Digest: visualProjDigest, Size: int64(len(visualProj))},
			{Name: "visual_projection.onnx.data", Digest: visualProjDataDigest, Size: int64(len(visualProjData))},
			{Name: "tokenizer.json", Digest: tokDigest, Size: int64(len(tokContent))},
		},
		Variants: map[string]VariantEntry{
			"i8": {Files: []ModelFile{
				{Name: "visual_model_i8.onnx", Digest: visualI8Digest, Size: int64(len(visualI8))},
			}},
		},
	}

	tmpDir := t.TempDir()

	// Pull f32 — everything should be downloaded
	if err := client.PullModel(context.Background(), manifest, tmpDir, []string{"f32"}); err != nil {
		t.Fatalf("PullModel() error = %v", err)
	}

	if !downloaded[visualOnnxDigest] {
		t.Error("visual_model.onnx should have been downloaded for f32")
	}
	if !downloaded[visualDataDigest] {
		t.Error("visual_model.onnx.data should have been downloaded for f32")
	}
	if !downloaded[visualProjDigest] {
		t.Error("visual_projection.onnx should have been downloaded for f32")
	}
	if !downloaded[visualProjDataDigest] {
		t.Error("visual_projection.onnx.data should have been downloaded for f32")
	}
	if !downloaded[tokDigest] {
		t.Error("tokenizer.json should have been downloaded for f32")
	}
}

func TestClientPullModelMultiVariantDownloadsBoth(t *testing.T) {
	// When pulling both f32 and i8, all encoder files plus projection files should download.
	visualOnnx, visualOnnxDigest := testContent("visual_model.onnx")
	visualData, visualDataDigest := testContent("visual_model.onnx.data")
	visualProj, visualProjDigest := testContent("visual_projection.onnx")
	visualProjData, visualProjDataDigest := testContent("visual_projection.onnx.data")
	visualI8, visualI8Digest := testContent("visual_model_i8.onnx")
	tok, tokDigest := testContent("tokenizer.json")

	blobs := map[string][]byte{
		visualOnnxDigest:     visualOnnx,
		visualDataDigest:     visualData,
		visualProjDigest:     visualProj,
		visualProjDataDigest: visualProjData,
		visualI8Digest:       visualI8,
		tokDigest:            tok,
	}

	client, downloaded := setupBlobServer(t, blobs)

	manifest := &ModelManifest{
		SchemaVersion: 2,
		Name:          "clip-vit-base-patch32",
		Owner:         "openai",
		Type:          ModelTypeEmbedder,
		Capabilities:  []string{"image"},
		Files: []ModelFile{
			{Name: "visual_model.onnx", Digest: visualOnnxDigest, Size: int64(len(visualOnnx))},
			{Name: "visual_model.onnx.data", Digest: visualDataDigest, Size: int64(len(visualData))},
			{Name: "visual_projection.onnx", Digest: visualProjDigest, Size: int64(len(visualProj))},
			{Name: "visual_projection.onnx.data", Digest: visualProjDataDigest, Size: int64(len(visualProjData))},
			{Name: "tokenizer.json", Digest: tokDigest, Size: int64(len(tok))},
		},
		Variants: map[string]VariantEntry{
			"i8": {Files: []ModelFile{
				{Name: "visual_model_i8.onnx", Digest: visualI8Digest, Size: int64(len(visualI8))},
			}},
		},
	}

	tmpDir := t.TempDir()

	if err := client.PullModel(context.Background(), manifest, tmpDir, []string{"f32", "i8"}); err != nil {
		t.Fatalf("PullModel() error = %v", err)
	}

	for _, tc := range []struct {
		digest string
		name   string
	}{
		{visualOnnxDigest, "visual_model.onnx"},
		{visualDataDigest, "visual_model.onnx.data"},
		{visualProjDigest, "visual_projection.onnx"},
		{visualProjDataDigest, "visual_projection.onnx.data"},
		{visualI8Digest, "visual_model_i8.onnx"},
		{tokDigest, "tokenizer.json"},
	} {
		if !downloaded[tc.digest] {
			t.Errorf("%s should have been downloaded for f32+i8 pull", tc.name)
		}
	}
}

func TestNewClientOptions(t *testing.T) {
	logger := zap.NewNop()

	var progressCalled bool
	progressHandler := func(downloaded, total int64, filename string) {
		progressCalled = true
	}

	client := NewClient(
		WithBaseURL("https://custom.registry.com/v2"),
		WithLogger(logger),
		WithProgressHandler(progressHandler),
	)

	if client.baseURL != "https://custom.registry.com/v2" {
		t.Errorf("baseURL = %v, want https://custom.registry.com/v2", client.baseURL)
	}

	// Test progress handler was set
	client.progressHandler(100, 1000, "test.onnx")
	if !progressCalled {
		t.Error("Progress handler was not called")
	}
}
