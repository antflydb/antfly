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
	"fmt"
	"slices"
	"sync"
)

var (
	validEmbedderProviders     []EmbedderProvider
	validEmbedderProvidersOnce sync.Once
)

// ValidEmbedderProviders returns all valid embedder provider values.
// Values are parsed from the embedded OpenAPI spec at first call.
func ValidEmbedderProviders() []EmbedderProvider {
	validEmbedderProvidersOnce.Do(func() {
		validEmbedderProviders = loadEmbedderProvidersFromSpec()
	})
	return validEmbedderProviders
}

// loadEmbedderProvidersFromSpec extracts valid EmbedderProvider values from the OpenAPI spec.
func loadEmbedderProvidersFromSpec() []EmbedderProvider {
	swagger, err := GetSwagger()
	if err != nil {
		// Fallback to empty list if spec parsing fails
		return nil
	}

	schema, ok := swagger.Components.Schemas["EmbedderProvider"]
	if !ok || schema.Value == nil {
		return nil
	}

	var providers []EmbedderProvider
	for _, v := range schema.Value.Enum {
		if s, ok := v.(string); ok {
			providers = append(providers, EmbedderProvider(s))
		}
	}
	return providers
}

// Validate checks if the EmbedderProvider value is one of the valid enum values.
// Returns an error if the provider is invalid or empty.
func (p EmbedderProvider) Validate() error {
	if p == "" {
		return fmt.Errorf("embedder provider cannot be empty, valid values are: %v", ValidEmbedderProviders())
	}

	if slices.Contains(ValidEmbedderProviders(), p) {
		return nil
	}
	return fmt.Errorf("invalid embedder provider %q, valid values are: %v", p, ValidEmbedderProviders())
}

// IsValid returns true if the EmbedderProvider value is valid.
func (p EmbedderProvider) IsValid() bool {
	return p.Validate() == nil
}

// Validate checks if the EmbedderConfig has a valid provider.
func (c *EmbedderConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("embedder config cannot be nil")
	}
	return c.Provider.Validate()
}
