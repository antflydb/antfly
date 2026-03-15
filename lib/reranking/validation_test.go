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

package reranking

import (
	"testing"
)

func TestRerankerProviderValidate(t *testing.T) {
	tests := []struct {
		name     string
		provider RerankerProvider
		wantErr  bool
	}{
		{
			name:     "valid ollama",
			provider: RerankerProviderOllama,
			wantErr:  false,
		},
		{
			name:     "valid termite",
			provider: RerankerProviderTermite,
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
			provider: "ollamaa",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.provider.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("RerankerProvider.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRerankerProviderIsValid(t *testing.T) {
	if !RerankerProviderOllama.IsValid() {
		t.Error("Expected ollama to be valid")
	}
	if RerankerProvider("invalid").IsValid() {
		t.Error("Expected 'invalid' to be invalid")
	}
}

func TestValidRerankerProviders(t *testing.T) {
	providers := ValidRerankerProviders()
	if len(providers) == 0 {
		t.Error("Expected at least one valid provider from spec")
	}

	// Check that known providers are present (from generated constants)
	knownProviders := []RerankerProvider{
		RerankerProviderOllama,
		RerankerProviderTermite,
	}

	providerSet := make(map[RerankerProvider]bool)
	for _, p := range providers {
		providerSet[p] = true
	}

	for _, known := range knownProviders {
		if !providerSet[known] {
			t.Errorf("Expected provider %s to be in valid providers list", known)
		}
	}
}

func TestRerankerConfigValidate(t *testing.T) {
	field := "content"
	template := "{{title}} {{body}}"
	emptyStr := ""

	tests := []struct {
		name    string
		config  *RerankerConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "valid config with field",
			config: &RerankerConfig{
				Provider: RerankerProviderOllama,
				Field:    &field,
			},
			wantErr: false,
		},
		{
			name: "valid config with template",
			config: &RerankerConfig{
				Provider: RerankerProviderOllama,
				Template: &template,
			},
			wantErr: false,
		},
		{
			name: "valid config with both field and template",
			config: &RerankerConfig{
				Provider: RerankerProviderOllama,
				Field:    &field,
				Template: &template,
			},
			wantErr: false,
		},
		{
			name: "empty provider",
			config: &RerankerConfig{
				Provider: "",
				Field:    &field,
			},
			wantErr: true,
		},
		{
			name: "invalid provider",
			config: &RerankerConfig{
				Provider: "invalid",
				Field:    &field,
			},
			wantErr: true,
		},
		{
			name: "missing field and template",
			config: &RerankerConfig{
				Provider: RerankerProviderOllama,
			},
			wantErr: true,
		},
		{
			name: "empty field and nil template",
			config: &RerankerConfig{
				Provider: RerankerProviderOllama,
				Field:    &emptyStr,
			},
			wantErr: true,
		},
		{
			name: "nil field and empty template",
			config: &RerankerConfig{
				Provider: RerankerProviderOllama,
				Template: &emptyStr,
			},
			wantErr: true,
		},
		{
			name: "both field and template empty",
			config: &RerankerConfig{
				Provider: RerankerProviderOllama,
				Field:    &emptyStr,
				Template: &emptyStr,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("RerankerConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
