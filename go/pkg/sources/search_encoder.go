// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// NativeVectorSpace resolves a managed recipe from an immutable native encoder
// pin. The pin includes assets, tokenizer and processor. Retrieval overrides
// participate separately in the preprocessing identity.
func NativeVectorSpace(id string, embedder map[string]any, dimension int, metric, documentModality string) (VectorSpace, error) {
	model, _ := embedder["model"].(string)
	pin, _ := embedder["model_identity"].(string)
	query, document, err := nativePreprocessing(embedder)
	if err != nil {
		return VectorSpace{}, err
	}
	space := VectorSpace{Id: id, Model: model, Revision: pin, Dimension: dimension,
		QueryPreprocessing: query, DocumentPreprocessing: document,
	}
	// JSON generated enums remain strings at the portable API boundary.
	space.Metric = management.VectorSpaceMetric(metric)
	space.QueryModality = "text"
	space.DocumentModality = management.VectorSpaceDocumentModality(documentModality)
	return space, ValidateManagedVectorSpace(space, embedder)
}

// ValidateManagedVectorSpace deliberately refuses mutable hosted model aliases.
// External writers may assert other recipes, but cannot claim native resolution.
func ValidateManagedVectorSpace(space VectorSpace, embedder map[string]any) error {
	if err := ValidateVectorSpace(space); err != nil {
		return err
	}
	pin, _ := embedder["model_identity"].(string)
	bytes, err := hex.DecodeString(pin)
	if embedder["provider"] != "antfly" || err != nil || len(bytes) != 32 || pin != hex.EncodeToString(bytes) || space.Revision != pin || embedder["model"] != space.Model {
		return fmt.Errorf("managed vectors require an immutable native encoder identity matching model and revision")
	}
	query, document, err := nativePreprocessing(embedder)
	if err != nil {
		return err
	}
	if space.QueryPreprocessing != query || space.DocumentPreprocessing != document || space.QueryModality != "text" {
		return fmt.Errorf("managed recipe differs from the native retrieval encoding protocol")
	}
	return nil
}

// Only transport location is excluded. Unknown execution settings participate in
// the identity too, so future native options cannot silently alias an old recipe.
// Empty/default configurations retain the original protocol identity.
func nativePreprocessing(embedder map[string]any) (string, string, error) {
	config := make(map[string]any)
	for key, value := range embedder {
		switch key {
		case "provider", "model", "model_identity", "api_url":
			continue
		}
		if key == "retrieval" {
			raw, err := json.Marshal(value)
			if err != nil {
				return "", "", err
			}
			var overrides map[string]any
			if err = json.Unmarshal(raw, &overrides); err != nil {
				return "", "", fmt.Errorf("invalid retrieval configuration: %w", err)
			}
			for k, v := range overrides {
				if v == "" || v == nil {
					delete(overrides, k)
				}
			}
			if len(overrides) == 0 {
				continue
			}
			value = overrides
		}
		config[key] = value
	}
	if len(config) == 0 {
		return "antfly:retrieval_query:v1", "antfly:retrieval_document:v1", nil
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(raw)
	suffix := hex.EncodeToString(digest[:])
	return "antfly:retrieval_query:v2:" + suffix, "antfly:retrieval_document:v2:" + suffix, nil
}
