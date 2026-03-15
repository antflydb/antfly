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
	"fmt"
	"slices"
	"sync"
)

var (
	validIndexTypes     []IndexType
	validIndexTypesOnce sync.Once
)

// legacyIndexTypes are old type names that are still accepted for backward compatibility
// but are not in the OpenAPI spec enum.
var legacyIndexTypes = []IndexType{
	IndexTypeAknnV0,
	IndexTypeFullTextV0,
	IndexTypeGraphV0,
}

// ValidIndexTypes returns all valid index type values.
// Values are parsed from the embedded OpenAPI spec at first call.
func ValidIndexTypes() []IndexType {
	validIndexTypesOnce.Do(func() {
		validIndexTypes = loadIndexTypesFromSpec()
	})
	return validIndexTypes
}

// loadIndexTypesFromSpec extracts valid IndexType values from the OpenAPI spec.
func loadIndexTypesFromSpec() []IndexType {
	swagger, err := GetSwagger()
	if err != nil {
		return nil
	}

	schema, ok := swagger.Components.Schemas["IndexType"]
	if !ok || schema.Value == nil {
		return nil
	}

	var types []IndexType
	for _, v := range schema.Value.Enum {
		if s, ok := v.(string); ok {
			types = append(types, IndexType(s))
		}
	}
	return types
}

// Validate checks if the IndexType value is one of the valid enum values
// or a recognized legacy type name.
func (t IndexType) Validate() error {
	if t == "" {
		return fmt.Errorf("index type cannot be empty, valid values are: %v", ValidIndexTypes())
	}

	if slices.Contains(ValidIndexTypes(), t) {
		return nil
	}
	// Accept legacy type names for backward compatibility
	if slices.Contains(legacyIndexTypes, t) {
		return nil
	}
	return fmt.Errorf("invalid index type %q, valid values are: %v", t, ValidIndexTypes())
}

// IsValid returns true if the IndexType value is valid.
func (t IndexType) IsValid() bool {
	return t.Validate() == nil
}

// Validate checks if the IndexConfig has a valid type.
func (c *IndexConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("index config cannot be nil")
	}
	return c.Type.Validate()
}

// Validate checks if the EmbeddingsIndexConfig has valid field/template configuration.
func (c *EmbeddingsIndexConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("embeddings index config cannot be nil")
	}
	if c.Sparse {
		// Sparse embeddings need field or template
		if c.Field == "" && c.Template == "" {
			return fmt.Errorf("sparse embeddings index config must specify either field or template")
		}
	} else {
		// Dense embeddings need field or template
		// Dimension can be 0 here — it will be auto-detected later by the API via embedder probe
		if c.Field == "" && c.Template == "" {
			return fmt.Errorf("dense embeddings index config must specify either field or template")
		}
	}
	return nil
}
