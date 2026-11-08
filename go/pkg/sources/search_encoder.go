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
	"encoding/hex"
	"fmt"
	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// NativeVectorSpace resolves a managed recipe from an immutable native encoder
// pin. The pin includes assets, tokenizer and processor; role names describe the
// native retrieval protocol, not arbitrary client preprocessing instructions.
func NativeVectorSpace(id string, embedder map[string]any, dimension int, metric, documentModality string) (VectorSpace, error) {
	model, _ := embedder["model"].(string)
	pin, _ := embedder["model_identity"].(string)
	space := VectorSpace{Id: id, Model: model, Revision: pin, Dimension: dimension,
		QueryPreprocessing: "antfly:retrieval_query:v1", DocumentPreprocessing: "antfly:retrieval_document:v1",
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
	if space.QueryPreprocessing != "antfly:retrieval_query:v1" || space.DocumentPreprocessing != "antfly:retrieval_document:v1" || space.QueryModality != "text" {
		return fmt.Errorf("managed recipe differs from the native retrieval encoding protocol")
	}
	return nil
}
