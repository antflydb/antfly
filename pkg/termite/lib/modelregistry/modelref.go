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
	"fmt"
	"path/filepath"
	"strings"
)

// ModelRef represents a parsed model reference
type ModelRef struct {
	// Owner is the namespace/organization (e.g., "BAAI", "sentence-transformers")
	Owner string
	// Name is the model name (e.g., "bge-small-en-v1.5")
	Name string
	// Variant is the optional model variant (e.g., "i8", "f16")
	Variant string
	// IsHuggingFace indicates if this was a hf: prefixed reference
	IsHuggingFace bool
}

// FullName returns "owner/name" format (e.g., "BAAI/bge-small-en-v1.5")
func (r ModelRef) FullName() string {
	if r.Owner == "" {
		return r.Name
	}
	return r.Owner + "/" + r.Name
}

// RegistryName returns the name with variant suffix for registry lookups
// e.g., "bge-small-en-v1.5" or "bge-small-en-v1.5-i8"
func (r ModelRef) RegistryName() string {
	if r.Variant == "" {
		return r.Name
	}
	return r.Name + "-" + r.Variant
}

// FullRegistryName returns owner/name-variant format for display
// e.g., "BAAI/bge-small-en-v1.5" or "BAAI/bge-small-en-v1.5-i8"
func (r ModelRef) FullRegistryName() string {
	if r.Owner == "" {
		return r.RegistryName()
	}
	return r.Owner + "/" + r.RegistryName()
}

// DirPath returns the directory path relative to the model type directory
// e.g., "BAAI/bge-small-en-v1.5"
func (r ModelRef) DirPath() string {
	if r.Owner == "" {
		return r.Name
	}
	return filepath.Join(r.Owner, r.Name)
}

// String returns a human-readable representation
func (r ModelRef) String() string {
	s := r.FullName()
	if r.Variant != "" {
		s += ":" + r.Variant
	}
	if r.IsHuggingFace {
		s = "hf:" + s
	}
	return s
}

// ParseModelRef parses various model reference formats:
//
//	"BAAI/bge-small-en-v1.5"         -> Owner: BAAI, Name: bge-small-en-v1.5
//	"BAAI/bge-small-en-v1.5:i8"      -> Owner: BAAI, Name: bge-small-en-v1.5, Variant: i8
//	"hf:BAAI/bge-small-en-v1.5"      -> same, but IsHuggingFace: true
//	"hf:BAAI/bge-small-en-v1.5:i8"   -> full form with HF prefix and variant
//	"bge-small-en-v1.5"              -> Owner: "", Name: bge-small-en-v1.5 (legacy)
//	"bge-small-en-v1.5:i8"           -> Owner: "", Name: bge-small-en-v1.5, Variant: i8 (legacy)
func ParseModelRef(ref string) (ModelRef, error) {
	if ref == "" {
		return ModelRef{}, fmt.Errorf("empty model reference")
	}

	result := ModelRef{}

	// Check for hf: prefix
	if after, ok := strings.CutPrefix(ref, "hf:"); ok {
		result.IsHuggingFace = true
		ref = after
	}

	// Check for variant suffix (colon-separated like Docker/Ollama tags)
	if idx := strings.LastIndex(ref, ":"); idx != -1 {
		result.Variant = ref[idx+1:]
		ref = ref[:idx]

		// Validate variant if specified
		if result.Variant != "" && !isValidVariantID(result.Variant) {
			return ModelRef{}, fmt.Errorf("invalid variant %q: valid variants are %v",
				result.Variant, validVariantIDs())
		}
	}

	// Split owner/name
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) == 2 {
		result.Owner = parts[0]
		result.Name = parts[1]
	} else {
		// Legacy: no owner specified
		result.Owner = ""
		result.Name = parts[0]
	}

	// Validate name is not empty
	if result.Name == "" {
		return ModelRef{}, fmt.Errorf("model reference has empty name: %q", ref)
	}

	return result, nil
}

// MustParseModelRef parses a model reference or panics
func MustParseModelRef(ref string) ModelRef {
	r, err := ParseModelRef(ref)
	if err != nil {
		panic(err)
	}
	return r
}

// isValidVariantID checks if a variant ID is valid
func isValidVariantID(variant string) bool {
	switch variant {
	case "", VariantF32, VariantF16, VariantBF16, VariantI8, VariantI8Static, VariantI4:
		return true
	default:
		return false
	}
}

// validVariantIDs returns the list of valid variant identifiers
func validVariantIDs() []string {
	return []string{VariantF16, VariantBF16, VariantI8, VariantI8Static, VariantI4}
}

// ModelRefFromManifest creates a ModelRef from a manifest
func ModelRefFromManifest(m *ModelManifest) ModelRef {
	return ModelRef{
		Owner: m.Owner,
		Name:  m.Name,
	}
}

// WithVariant returns a new ModelRef with the specified variant
func (r ModelRef) WithVariant(variant string) ModelRef {
	return ModelRef{
		Owner:         r.Owner,
		Name:          r.Name,
		Variant:       variant,
		IsHuggingFace: r.IsHuggingFace,
	}
}

// HasOwner returns true if the model reference has an owner
func (r ModelRef) HasOwner() bool {
	return r.Owner != ""
}

// Validate checks that the model reference is valid
func (r ModelRef) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("model name is required")
	}
	if r.Variant != "" && !isValidVariantID(r.Variant) {
		return fmt.Errorf("invalid variant %q", r.Variant)
	}
	return nil
}
