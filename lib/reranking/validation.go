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
	"fmt"
	"slices"
	"sync"
)

var (
	validRerankerProviders     []RerankerProvider
	validRerankerProvidersOnce sync.Once
)

// ValidRerankerProviders returns all valid reranker provider values.
// Values are parsed from the embedded OpenAPI spec at first call.
func ValidRerankerProviders() []RerankerProvider {
	validRerankerProvidersOnce.Do(func() {
		validRerankerProviders = loadRerankerProvidersFromSpec()
	})
	return validRerankerProviders
}

// loadRerankerProvidersFromSpec extracts valid RerankerProvider values from the OpenAPI spec.
func loadRerankerProvidersFromSpec() []RerankerProvider {
	swagger, err := GetSwagger()
	if err != nil {
		return nil
	}

	schema, ok := swagger.Components.Schemas["RerankerProvider"]
	if !ok || schema.Value == nil {
		return nil
	}

	var providers []RerankerProvider
	for _, v := range schema.Value.Enum {
		if s, ok := v.(string); ok {
			providers = append(providers, RerankerProvider(s))
		}
	}
	return providers
}

// Validate checks if the RerankerProvider value is one of the valid enum values.
// Returns an error if the provider is invalid or empty.
func (p RerankerProvider) Validate() error {
	if p == "" {
		return fmt.Errorf("reranker provider cannot be empty, valid values are: %v", ValidRerankerProviders())
	}

	if slices.Contains(ValidRerankerProviders(), p) {
		return nil
	}
	return fmt.Errorf("invalid reranker provider %q, valid values are: %v", p, ValidRerankerProviders())
}

// IsValid returns true if the RerankerProvider value is valid.
func (p RerankerProvider) IsValid() bool {
	return p.Validate() == nil
}

// Validate checks if the RerankerConfig has a valid provider and field/template configuration.
func (c *RerankerConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("reranker config cannot be nil")
	}
	if err := c.Provider.Validate(); err != nil {
		return err
	}
	// Validate that at least one of field or template is specified
	field := ""
	if c.Field != nil {
		field = *c.Field
	}
	template := ""
	if c.Template != nil {
		template = *c.Template
	}
	if field == "" && template == "" {
		return fmt.Errorf("reranker config must specify either field or template")
	}
	return nil
}
