// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package indexes

import (
	"testing"
)

func TestIndexTypeValidate(t *testing.T) {
	tests := []struct {
		name      string
		indexType IndexType
		wantErr   bool
	}{
		{
			name:      "valid embeddings",
			indexType: IndexTypeEmbeddings,
			wantErr:   false,
		},
		{
			name:      "valid full_text",
			indexType: IndexTypeFullText,
			wantErr:   false,
		},
		{
			name:      "valid graph",
			indexType: IndexTypeGraph,
			wantErr:   false,
		},
		{
			name:      "legacy aknn_v0",
			indexType: IndexTypeAknnV0,
			wantErr:   false,
		},
		{
			name:      "legacy full_text_v0",
			indexType: IndexTypeFullTextV0,
			wantErr:   false,
		},
		{
			name:      "legacy graph_v0",
			indexType: IndexTypeGraphV0,
			wantErr:   false,
		},
		{
			name:      "empty type",
			indexType: "",
			wantErr:   true,
		},
		{
			name:      "invalid type",
			indexType: "invalid",
			wantErr:   true,
		},
		{
			name:      "wrong version",
			indexType: "aknn_v1",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.indexType.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("IndexType.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIndexTypeIsValid(t *testing.T) {
	if !IndexTypeFullTextV0.IsValid() {
		t.Error("Expected full_text_v0 to be valid")
	}
	if IndexType("invalid").IsValid() {
		t.Error("Expected 'invalid' to be invalid")
	}
}

func TestValidIndexTypes(t *testing.T) {
	types := ValidIndexTypes()
	if len(types) == 0 {
		t.Error("Expected at least one valid index type from spec")
	}

	// Check that canonical types are present in the spec enum
	knownTypes := []IndexType{
		IndexTypeEmbeddings,
		IndexTypeFullText,
		IndexTypeGraph,
	}

	typeSet := make(map[IndexType]bool)
	for _, it := range types {
		typeSet[it] = true
	}

	for _, known := range knownTypes {
		if !typeSet[known] {
			t.Errorf("Expected index type %s to be in valid types list", known)
		}
	}

	// Legacy types should NOT be in the spec enum (they're handled separately)
	for _, legacy := range []IndexType{IndexTypeAknnV0, IndexTypeFullTextV0, IndexTypeGraphV0} {
		if typeSet[legacy] {
			t.Errorf("Legacy index type %s should not be in spec enum", legacy)
		}
	}
}

func TestIndexConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *IndexConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "valid config",
			config: &IndexConfig{
				Type: IndexTypeFullTextV0,
			},
			wantErr: false,
		},
		{
			name: "empty type",
			config: &IndexConfig{
				Type: "",
			},
			wantErr: true,
		},
		{
			name: "invalid type",
			config: &IndexConfig{
				Type: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("IndexConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEmbeddingsIndexConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *EmbeddingsIndexConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "dense valid config with field",
			config: &EmbeddingsIndexConfig{
				Dimension: 768,
				Field:     "content",
			},
			wantErr: false,
		},
		{
			name: "dense valid config with template",
			config: &EmbeddingsIndexConfig{
				Dimension: 768,
				Template:  "{{title}} {{body}}",
			},
			wantErr: false,
		},
		{
			name: "dense valid config with both field and template",
			config: &EmbeddingsIndexConfig{
				Dimension: 768,
				Field:     "content",
				Template:  "{{title}} {{body}}",
			},
			wantErr: false,
		},
		{
			name: "dense missing field and template",
			config: &EmbeddingsIndexConfig{
				Dimension: 768,
			},
			wantErr: true,
		},
		{
			name: "dense empty field and template",
			config: &EmbeddingsIndexConfig{
				Dimension: 768,
				Field:     "",
				Template:  "",
			},
			wantErr: true,
		},
		{
			name: "dense with dimension omitted is valid (auto-detected later by API)",
			config: &EmbeddingsIndexConfig{
				Dimension: 0,
				Field:     "content",
			},
			wantErr: false,
		},
		{
			name: "sparse with field",
			config: &EmbeddingsIndexConfig{
				Sparse: true,
				Field:  "content",
			},
			wantErr: false,
		},
		{
			name: "sparse with template",
			config: &EmbeddingsIndexConfig{
				Sparse:   true,
				Template: "{{title}} {{body}}",
			},
			wantErr: false,
		},
		{
			name: "sparse missing field and template",
			config: &EmbeddingsIndexConfig{
				Sparse: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("EmbeddingsIndexConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
