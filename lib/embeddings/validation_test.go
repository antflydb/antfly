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

package embeddings

import (
	"testing"
)

func TestEmbedderProviderValidate(t *testing.T) {
	tests := []struct {
		name     string
		provider EmbedderProvider
		wantErr  bool
	}{
		{
			name:     "valid bedrock",
			provider: EmbedderProviderBedrock,
			wantErr:  false,
		},
		{
			name:     "valid gemini",
			provider: EmbedderProviderGemini,
			wantErr:  false,
		},
		{
			name:     "valid mock",
			provider: EmbedderProviderMock,
			wantErr:  false,
		},
		{
			name:     "valid ollama",
			provider: EmbedderProviderOllama,
			wantErr:  false,
		},
		{
			name:     "valid openai",
			provider: EmbedderProviderOpenai,
			wantErr:  false,
		},
		{
			name:     "valid vertex",
			provider: EmbedderProviderVertex,
			wantErr:  false,
		},
		{
			name:     "valid termite",
			provider: EmbedderProviderTermite,
			wantErr:  false,
		},
		{
			name:     "empty provider",
			provider: "",
			wantErr:  true,
		},
		{
			name:     "invalid provider",
			provider: "invalid",
			wantErr:  true,
		},
		{
			name:     "typo in provider",
			provider: "opennai",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.provider.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("EmbedderProvider.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEmbedderProviderIsValid(t *testing.T) {
	if !EmbedderProviderOpenai.IsValid() {
		t.Error("Expected openai to be valid")
	}
	if EmbedderProvider("invalid").IsValid() {
		t.Error("Expected 'invalid' to be invalid")
	}
}

func TestValidEmbedderProviders(t *testing.T) {
	providers := ValidEmbedderProviders()
	if len(providers) == 0 {
		t.Error("Expected at least one valid provider from spec")
	}

	// Check that known providers are present (from generated constants)
	knownProviders := []EmbedderProvider{
		EmbedderProviderBedrock,
		EmbedderProviderGemini,
		EmbedderProviderOllama,
		EmbedderProviderOpenai,
		EmbedderProviderTermite,
		EmbedderProviderVertex,
	}

	providerSet := make(map[EmbedderProvider]bool)
	for _, p := range providers {
		providerSet[p] = true
	}

	for _, known := range knownProviders {
		if !providerSet[known] {
			t.Errorf("Expected provider %s to be in valid providers list", known)
		}
	}
}

func TestEmbedderConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *EmbedderConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "valid config",
			config: &EmbedderConfig{
				Provider: EmbedderProviderOpenai,
			},
			wantErr: false,
		},
		{
			name: "empty provider",
			config: &EmbedderConfig{
				Provider: "",
			},
			wantErr: true,
		},
		{
			name: "invalid provider",
			config: &EmbedderConfig{
				Provider: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("EmbedderConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
