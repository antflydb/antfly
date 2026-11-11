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

package declarations

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type ProcessingDeclaration = management.ProcessingDeclaration

func NativeIndexName(prefix string, config map[string]any) string {
	raw, _ := json.Marshal(config)
	sum := sha256.Sum256(raw)
	return prefix + "-" + hex.EncodeToString(sum[:16])
}
func hasWriteOnlyProducer(v any) bool {
	switch value := v.(type) {
	case map[string]any:
		for k, v := range value {
			if k == "producer" || k == "producer_json" || k == "transcriber" || ProcessingCredentialField(k) {
				return true
			}
			if hasWriteOnlyProducer(v) {
				return true
			}
		}
	case []any:
		for _, v := range value {
			if hasWriteOnlyProducer(v) {
				return true
			}
		}
	}
	return false
}

func ValidateProcessing(plan *ProcessingDeclaration) error {
	if plan == nil {
		return nil
	}
	if len(plan.Indexes) > 32 {
		return fmt.Errorf("too many processing indexes")
	}
	names := map[string]bool{}
	for _, index := range plan.Indexes {
		if err := ValidateProcessingSecrets(index.Config); err != nil {
			return err
		}
		if hasWriteOnlyProducer(index.Config) && !strings.HasSuffix(index.Name, NativeIndexName("", index.Config)) {
			return fmt.Errorf("write-only producer recipes require a content-addressed native index name")
		}

		if !ValidBindingName(index.Name) || names[index.Name] || index.Config["type"] == nil {
			return fmt.Errorf("invalid native index declaration")
		}
		names[index.Name] = true
	}
	return nil
}

func ValidateProcessingSecrets(value any) error { return validateProcessingSecretsDepth(value, 0) }

func validateProcessingSecretsDepth(value any, depth int) error {
	if depth > 64 {
		return fmt.Errorf("processing declaration exceeds depth budget")
	}
	switch v := value.(type) {
	case map[string]any:
		for key, value := range v {
			if text, ok := value.(string); ok && text != "" && strings.HasSuffix(key, "_json") {
				var decoded any
				if err := json.Unmarshal([]byte(text), &decoded); err != nil {
					return fmt.Errorf("invalid native processing JSON configuration")
				}
				if err := validateProcessingSecretsDepth(decoded, depth+1); err != nil {
					return err
				}
			}
			if ProcessingCredentialField(key) {
				if text, ok := value.(string); ok && text != "" && !processingSecretReference(text) {
					return fmt.Errorf("processing credentials require native secret references")
				}
			}
			if text, ok := value.(string); ok && processingEndpointField(key) && processingURLCredentials(text) {
				return fmt.Errorf("processing endpoint credentials require native secret references")
			}
			if err := validateProcessingSecretsDepth(value, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, value := range v {
			if err := validateProcessingSecretsDepth(value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
