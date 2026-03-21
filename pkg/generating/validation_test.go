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

package generating

import (
	"testing"
)

func TestGeneratorProviderValidate(t *testing.T) {
	tests := []struct {
		name     string
		provider GeneratorProvider
		wantErr  bool
	}{
		{
			name:     "valid anthropic",
			provider: GeneratorProviderAnthropic,
			wantErr:  false,
		},
		{
			name:     "valid bedrock",
			provider: GeneratorProviderBedrock,
			wantErr:  false,
		},
		{
			name:     "valid gemini",
			provider: GeneratorProviderGemini,
			wantErr:  false,
		},
		{
			name:     "valid mock",
			provider: GeneratorProviderMock,
			wantErr:  false,
		},
		{
			name:     "valid ollama",
			provider: GeneratorProviderOllama,
			wantErr:  false,
		},
		{
			name:     "valid openai",
			provider: GeneratorProviderOpenai,
			wantErr:  false,
		},
		{
			name:     "valid vertex",
			provider: GeneratorProviderVertex,
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
			provider: "gemnii",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.provider.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("GeneratorProvider.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGeneratorProviderIsValid(t *testing.T) {
	if !GeneratorProviderOpenai.IsValid() {
		t.Error("Expected openai to be valid")
	}
	if GeneratorProvider("invalid").IsValid() {
		t.Error("Expected 'invalid' to be invalid")
	}
}

func TestValidGeneratorProviders(t *testing.T) {
	providers := ValidGeneratorProviders()
	if len(providers) == 0 {
		t.Error("Expected at least one valid provider from spec")
	}

	// Check that known providers are present (from generated constants)
	knownProviders := []GeneratorProvider{
		GeneratorProviderAnthropic,
		GeneratorProviderBedrock,
		GeneratorProviderGemini,
		GeneratorProviderOllama,
		GeneratorProviderOpenai,
		GeneratorProviderVertex,
	}

	providerSet := make(map[GeneratorProvider]bool)
	for _, p := range providers {
		providerSet[p] = true
	}

	for _, known := range knownProviders {
		if !providerSet[known] {
			t.Errorf("Expected provider %s to be in valid providers list", known)
		}
	}
}

func TestGeneratorConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *GeneratorConfig
		wantErr bool
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "valid config",
			config: &GeneratorConfig{
				Provider: GeneratorProviderOpenai,
			},
			wantErr: false,
		},
		{
			name: "empty provider",
			config: &GeneratorConfig{
				Provider: "",
			},
			wantErr: true,
		},
		{
			name: "invalid provider",
			config: &GeneratorConfig{
				Provider: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("GeneratorConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
