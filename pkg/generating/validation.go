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
	"fmt"
	"slices"
	"sync"
)

var (
	validGeneratorProviders     []GeneratorProvider
	validGeneratorProvidersOnce sync.Once
)

// ValidGeneratorProviders returns all valid generator provider values.
// Values are parsed from the embedded OpenAPI spec at first call.
func ValidGeneratorProviders() []GeneratorProvider {
	validGeneratorProvidersOnce.Do(func() {
		validGeneratorProviders = loadGeneratorProvidersFromSpec()
	})
	return validGeneratorProviders
}

// loadGeneratorProvidersFromSpec extracts valid GeneratorProvider values from the OpenAPI spec.
func loadGeneratorProvidersFromSpec() []GeneratorProvider {
	swagger, err := GetSwagger()
	if err != nil {
		return nil
	}

	schema, ok := swagger.Components.Schemas["GeneratorProvider"]
	if !ok || schema.Value == nil {
		return nil
	}

	var providers []GeneratorProvider
	for _, v := range schema.Value.Enum {
		if s, ok := v.(string); ok {
			providers = append(providers, GeneratorProvider(s))
		}
	}
	return providers
}

// Validate checks if the GeneratorProvider value is one of the valid enum values.
// Returns an error if the provider is invalid or empty.
func (p GeneratorProvider) Validate() error {
	if p == "" {
		return fmt.Errorf("generator provider cannot be empty, valid values are: %v", ValidGeneratorProviders())
	}

	if slices.Contains(ValidGeneratorProviders(), p) {
		return nil
	}
	return fmt.Errorf("invalid generator provider %q, valid values are: %v", p, ValidGeneratorProviders())
}

// IsValid returns true if the GeneratorProvider value is valid.
func (p GeneratorProvider) IsValid() bool {
	return p.Validate() == nil
}

// Validate checks if the GeneratorConfig has a valid provider.
func (c *GeneratorConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("generator config cannot be nil")
	}
	return c.Provider.Validate()
}
