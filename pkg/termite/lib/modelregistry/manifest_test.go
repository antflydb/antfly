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
	"testing"
)

func TestParseModelType(t *testing.T) {
	tests := []struct {
		input    string
		expected ModelType
		wantErr  bool
	}{
		{"embedder", ModelTypeEmbedder, false},
		{"embedders", ModelTypeEmbedder, false},
		{"EMBEDDER", ModelTypeEmbedder, false},
		{"chunker", ModelTypeChunker, false},
		{"chunkers", ModelTypeChunker, false},
		{"reranker", ModelTypeReranker, false},
		{"rerankers", ModelTypeReranker, false},
		{"recognizer", ModelTypeRecognizer, false},
		{"recognizers", ModelTypeRecognizer, false},
		{"RECOGNIZER", ModelTypeRecognizer, false},
		{"rewriter", ModelTypeRewriter, false},
		{"rewriters", ModelTypeRewriter, false},
		{"REWRITER", ModelTypeRewriter, false},
		{"unknown", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseModelType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseModelType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ParseModelType(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestPipelineTagToModelType(t *testing.T) {
	tests := []struct {
		tag      string
		expected ModelType
		wantOK   bool
	}{
		// Embedder
		{"feature-extraction", ModelTypeEmbedder, true},
		{"sentence-similarity", ModelTypeEmbedder, true},
		{"zero-shot-image-classification", ModelTypeEmbedder, true},
		// Reranker
		{"text-ranking", ModelTypeReranker, true},
		// Generator
		{"text-generation", ModelTypeGenerator, true},
		// Rewriter
		{"text2text-generation", ModelTypeRewriter, true},
		{"summarization", ModelTypeRewriter, true},
		{"translation", ModelTypeRewriter, true},
		// Reader
		{"image-text-to-text", ModelTypeReader, true},
		{"image-to-text", ModelTypeReader, true},
		{"document-question-answering", ModelTypeReader, true},
		// Transcriber
		{"automatic-speech-recognition", ModelTypeTranscriber, true},
		// Classifier
		{"text-classification", ModelTypeClassifier, true},
		{"token-classification", ModelTypeClassifier, true},
		{"zero-shot-classification", ModelTypeClassifier, true},
		// Recognizer
		{"object-detection", ModelTypeRecognizer, true},
		{"image-classification", ModelTypeRecognizer, true},
		{"image-segmentation", ModelTypeRecognizer, true},
		// Unknown
		{"unknown-task", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			got, ok := pipelineTagToModelType[tt.tag]
			if ok != tt.wantOK {
				t.Errorf("pipelineTagToModelType[%q] ok = %v, want %v", tt.tag, ok, tt.wantOK)
				return
			}
			if got != tt.expected {
				t.Errorf("pipelineTagToModelType[%q] = %v, want %v", tt.tag, got, tt.expected)
			}
		})
	}
}

func TestModelTypeDirName(t *testing.T) {
	tests := []struct {
		modelType ModelType
		expected  string
	}{
		{ModelTypeEmbedder, "embedders"},
		{ModelTypeChunker, "chunkers"},
		{ModelTypeReranker, "rerankers"},
		{ModelTypeRecognizer, "recognizers"},
		{ModelTypeRewriter, "rewriters"},
	}

	for _, tt := range tests {
		t.Run(string(tt.modelType), func(t *testing.T) {
			if got := tt.modelType.DirName(); got != tt.expected {
				t.Errorf("DirName() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseManifest(t *testing.T) {
	validManifest := `{
		"schemaVersion": 1,
		"name": "bge-small-en-v1.5",
		"type": "embedder",
		"description": "BGE small English embedding model",
		"files": [
			{"name": "model.onnx", "digest": "sha256:abc123", "size": 12345},
			{"name": "tokenizer.json", "digest": "sha256:def456", "size": 1000}
		],
		"variants": {
			"i8": {"name": "model_i8.onnx", "digest": "sha256:ghi789", "size": 6789},
			"f16": {"name": "model_f16.onnx", "digest": "sha256:jkl012", "size": 8000}
		}
	}`

	t.Run("valid manifest", func(t *testing.T) {
		manifest, err := ParseManifest([]byte(validManifest))
		if err != nil {
			t.Fatalf("ParseManifest() error = %v", err)
		}

		if manifest.Name != "bge-small-en-v1.5" {
			t.Errorf("Name = %v, want bge-small-en-v1.5", manifest.Name)
		}
		if manifest.Type != ModelTypeEmbedder {
			t.Errorf("Type = %v, want embedder", manifest.Type)
		}
		if len(manifest.Files) != 2 {
			t.Errorf("len(Files) = %v, want 2", len(manifest.Files))
		}
		if len(manifest.Variants) != 2 {
			t.Errorf("len(Variants) = %v, want 2", len(manifest.Variants))
		}
		if _, ok := manifest.Variants["i8"]; !ok {
			t.Error("Variants should contain 'i8' key")
		}
	})

	// Test registry format with variants having "files" array wrapper
	registryManifest := `{
		"schemaVersion": 2,
		"name": "bge-small-en-v1.5",
		"owner": "BAAI",
		"type": "embedder",
		"source": "BAAI/bge-small-en-v1.5",
		"files": [
			{"name": "model.onnx", "digest": "sha256:abc123", "size": 12345},
			{"name": "tokenizer.json", "digest": "sha256:def456", "size": 1000}
		],
		"variants": {
			"f16": {"files": [{"name": "model_f16.onnx", "digest": "sha256:jkl012", "size": 8000}]},
			"i8": {"files": [{"name": "model_i8.onnx", "digest": "sha256:ghi789", "size": 6789}]}
		}
	}`

	t.Run("valid manifest with registry variant format", func(t *testing.T) {
		manifest, err := ParseManifest([]byte(registryManifest))
		if err != nil {
			t.Fatalf("ParseManifest() error = %v", err)
		}

		if manifest.Name != "bge-small-en-v1.5" {
			t.Errorf("Name = %v, want bge-small-en-v1.5", manifest.Name)
		}
		if manifest.Owner != "BAAI" {
			t.Errorf("Owner = %v, want BAAI", manifest.Owner)
		}
		if len(manifest.Variants) != 2 {
			t.Errorf("len(Variants) = %v, want 2", len(manifest.Variants))
		}
		f16Variant, ok := manifest.Variants["f16"]
		if !ok {
			t.Fatal("Variants should contain 'f16' key")
		}
		if len(f16Variant.Files) != 1 {
			t.Errorf("f16 variant should have 1 file, got %d", len(f16Variant.Files))
		}
		if f16Variant.Files[0].Name != "model_f16.onnx" {
			t.Errorf("f16 variant file name = %v, want model_f16.onnx", f16Variant.Files[0].Name)
		}
	})

	t.Run("missing name", func(t *testing.T) {
		data := `{"schemaVersion": 1, "type": "embedder", "files": [{"name": "model.onnx", "digest": "sha256:abc", "size": 1}]}`
		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("Expected error for missing name")
		}
	})

	t.Run("missing type", func(t *testing.T) {
		data := `{"schemaVersion": 1, "name": "test", "files": [{"name": "model.onnx", "digest": "sha256:abc", "size": 1}]}`
		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("Expected error for missing type")
		}
	})

	t.Run("no onnx files", func(t *testing.T) {
		data := `{"schemaVersion": 1, "name": "test", "type": "embedder", "files": [{"name": "tokenizer.json", "digest": "sha256:abc", "size": 1}]}`
		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("Expected error for missing .onnx files")
		}
	})

	t.Run("invalid schema version", func(t *testing.T) {
		data := `{"schemaVersion": 99, "name": "test", "type": "embedder", "files": [{"name": "model.onnx", "digest": "sha256:abc", "size": 1}]}`
		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("Expected error for invalid schema version")
		}
	})

	t.Run("invalid digest format", func(t *testing.T) {
		data := `{"schemaVersion": 1, "name": "test", "type": "embedder", "files": [{"name": "model.onnx", "digest": "md5:abc", "size": 1}]}`
		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("Expected error for invalid digest format")
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := ParseManifest([]byte("not json"))
		if err == nil {
			t.Error("Expected error for invalid JSON")
		}
	})
}

func TestParseRegistryIndex(t *testing.T) {
	validIndexV1 := `{
		"schemaVersion": 1,
		"models": [
			{"name": "bge-small-en-v1.5", "type": "embedder", "description": "BGE embedding", "size": 12345, "variants": ["i8", "f16"]},
			{"name": "mxbai-rerank-base-v1", "type": "reranker", "description": "Reranker", "size": 67890}
		]
	}`

	validIndexV2 := `{
		"schemaVersion": 2,
		"models": [
			{"name": "bge-small-en-v1.5", "owner": "BAAI", "source": "BAAI/bge-small-en-v1.5", "type": "embedder", "description": "BGE embedding", "size": 12345, "variants": ["i8", "f16"]},
			{"name": "mxbai-rerank-base-v1", "owner": "mixedbread-ai", "source": "mixedbread-ai/mxbai-rerank-base-v1", "type": "reranker", "description": "Reranker", "size": 67890}
		]
	}`

	t.Run("valid index v1", func(t *testing.T) {
		index, err := ParseRegistryIndex([]byte(validIndexV1))
		if err != nil {
			t.Fatalf("ParseRegistryIndex() error = %v", err)
		}

		if len(index.Models) != 2 {
			t.Errorf("len(Models) = %v, want 2", len(index.Models))
		}
		if index.Models[0].Name != "bge-small-en-v1.5" {
			t.Errorf("Models[0].Name = %v, want bge-small-en-v1.5", index.Models[0].Name)
		}
		if len(index.Models[0].Variants) != 2 {
			t.Errorf("Models[0].Variants should have 2 entries, got %d", len(index.Models[0].Variants))
		}
	})

	t.Run("valid index v2", func(t *testing.T) {
		index, err := ParseRegistryIndex([]byte(validIndexV2))
		if err != nil {
			t.Fatalf("ParseRegistryIndex() error = %v", err)
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
	})

	t.Run("invalid schema version", func(t *testing.T) {
		data := `{"schemaVersion": 99, "models": []}`
		_, err := ParseRegistryIndex([]byte(data))
		if err == nil {
			t.Error("Expected error for invalid schema version")
		}
	})

	t.Run("schema version 0 invalid", func(t *testing.T) {
		data := `{"schemaVersion": 0, "models": []}`
		_, err := ParseRegistryIndex([]byte(data))
		if err == nil {
			t.Error("Expected error for schema version 0")
		}
	})

	t.Run("index v2 with backends", func(t *testing.T) {
		data := `{
			"schemaVersion": 2,
			"models": [
				{"name": "nomic-embed-text-v1.5", "owner": "nomic-ai", "type": "embedder", "backends": ["onnx"]},
				{"name": "bge-m3", "owner": "BAAI", "type": "embedder", "backends": ["onnx", "xla"]}
			]
		}`
		index, err := ParseRegistryIndex([]byte(data))
		if err != nil {
			t.Fatalf("ParseRegistryIndex() error = %v", err)
		}

		if len(index.Models) != 2 {
			t.Errorf("len(Models) = %v, want 2", len(index.Models))
		}

		// Check nomic model has backends = ["onnx"]
		if len(index.Models[0].Backends) != 1 {
			t.Errorf("Models[0].Backends = %v, want 1 backend", index.Models[0].Backends)
		}
		if len(index.Models[0].Backends) > 0 && index.Models[0].Backends[0] != "onnx" {
			t.Errorf("Models[0].Backends[0] = %v, want onnx", index.Models[0].Backends[0])
		}

		// Check bge-m3 model has backends = ["onnx", "xla"]
		if len(index.Models[1].Backends) != 2 {
			t.Errorf("Models[1].Backends = %v, want 2 backends", index.Models[1].Backends)
		}
	})
}

func TestManifestValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*ModelManifest)
		wantErr bool
	}{
		{
			name:    "valid manifest",
			modify:  func(m *ModelManifest) {},
			wantErr: false,
		},
		{
			name:    "invalid model type",
			modify:  func(m *ModelManifest) { m.Type = "invalid" },
			wantErr: true,
		},
		{
			name:    "empty files",
			modify:  func(m *ModelManifest) { m.Files = nil },
			wantErr: true,
		},
		{
			name: "file missing name",
			modify: func(m *ModelManifest) {
				m.Files[0].Name = ""
			},
			wantErr: true,
		},
		{
			name: "file missing digest",
			modify: func(m *ModelManifest) {
				m.Files[0].Digest = ""
			},
			wantErr: true,
		},
		{
			name: "non-standard onnx filenames valid",
			modify: func(m *ModelManifest) {
				m.Type = ModelTypeRecognizer
				m.Files = []ModelFile{
					{Name: "det.onnx", Digest: "sha256:def456", Size: 5000},
					{Name: "rec.onnx", Digest: "sha256:ghi789", Size: 8000},
				}
			},
			wantErr: false,
		},
		{
			name: "no onnx files",
			modify: func(m *ModelManifest) {
				m.Files = []ModelFile{
					{Name: "tokenizer.json", Digest: "sha256:abc123", Size: 500},
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest := &ModelManifest{
				SchemaVersion: 1,
				Name:          "test-model",
				Type:          ModelTypeEmbedder,
				Files: []ModelFile{
					{Name: "model.onnx", Digest: "sha256:abc123", Size: 1000},
				},
			}
			tt.modify(manifest)

			err := manifest.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOnnxStem(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"model.onnx", "model"},
		{"visual_model.onnx.data", "visual_model"},
		{"visual_model.onnx_data", "visual_model"},
		{"tokenizer.json", "tokenizer.json"},
		{"model_i8.onnx", "model_i8"},
	}
	for _, tt := range tests {
		if got := onnxStem(tt.input); got != tt.want {
			t.Errorf("onnxStem(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsONNXFile(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"model.onnx", true},
		{"model.onnx.data", true},
		{"model.onnx_data", true},
		{"tokenizer.json", false},
		{"config.json", false},
	}
	for _, tt := range tests {
		if got := isONNXFile(tt.input); got != tt.want {
			t.Errorf("isONNXFile(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// clipManifest returns a CLIP-style manifest for testing variant stem logic.
func clipManifest() *ModelManifest {
	return &ModelManifest{
		SchemaVersion: 2,
		Name:          "clip-vit-base-patch32",
		Owner:         "openai",
		Type:          ModelTypeEmbedder,
		Files: []ModelFile{
			{Name: "visual_model.onnx", Size: 100},
			{Name: "visual_model.onnx.data", Size: 200},
			{Name: "text_model.onnx", Size: 80},
			{Name: "visual_projection.onnx", Size: 10},
			{Name: "text_projection.onnx", Size: 10},
			{Name: "tokenizer.json", Size: 5},
		},
		Variants: map[string]VariantEntry{
			"i8": {Files: []ModelFile{
				{Name: "visual_model_i8.onnx", Size: 50},
				{Name: "text_model_i8.onnx", Size: 40},
			}},
		},
	}
}

func TestVariantBaseStems(t *testing.T) {
	m := clipManifest()
	stems := m.VariantBaseStems()

	if !stems["visual_model"] {
		t.Error("expected visual_model in stems")
	}
	if !stems["text_model"] {
		t.Error("expected text_model in stems")
	}
	if stems["visual_projection"] {
		t.Error("visual_projection should not be in stems (no variant replacement)")
	}
	if stems["tokenizer"] {
		t.Error("tokenizer should not be in stems")
	}
}

func TestDownloadSize(t *testing.T) {
	m := clipManifest()

	tests := []struct {
		name     string
		variants []string
		want     int64
	}{
		{
			name:     "nil defaults to f32",
			variants: nil,
			// all base files: 100+200+80+10+10+5 = 405
			want: 405,
		},
		{
			name:     "f32 explicit",
			variants: []string{VariantF32},
			want:     405,
		},
		{
			name:     "i8 only",
			variants: []string{VariantI8},
			// supporting+auxiliary: 10+10+5 = 25, plus i8 variant: 50+40 = 90
			// encoder files (visual_model, text_model) excluded
			want: 115,
		},
		{
			name:     "f32 and i8",
			variants: []string{VariantF32, VariantI8},
			// all base files: 405 + i8 variant: 90 = 495
			want: 495,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.DownloadSize(tt.variants)
			if got != tt.want {
				t.Errorf("DownloadSize(%v) = %d, want %d", tt.variants, got, tt.want)
			}
		})
	}
}
