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

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type corpusTableBinding struct {
	Endpoint            string `json:"endpoint"`
	Table               string `json:"table"`
	TableID             string `json:"table_id"`
	StoredDescription   string `json:"stored_description"`
	SemanticDescription string `json:"semantic_description"`
}

// Upgrade the client's understanding, never the table or its accepted receipts.
// Keeping the stored description also keeps completed append/load bindings valid.
func resolveCorpusDescription(state string, accepted []string, endpoint, table, tableID, stored string, indexes map[string]any, pages int) (string, error) {
	current := corpusDescription(indexes, pages)
	if stored == current {
		return stored, nil
	}
	expected := corpusTableBinding{strings.TrimRight(endpoint, "/"), table, tableID, stored, current}
	locations := append([]string{state}, accepted...)
	for _, location := range locations {
		var binding corpusTableBinding
		err := readCorpusJSON(filepath.Join(location, "table-binding.json"), &binding)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if err == nil && binding == expected {
			if err := atomicCorpusJSON(filepath.Join(state, "table-binding.json"), expected); err != nil {
				return "", err
			}
			return stored, nil
		}
	}
	legacy, err := legacyCorpusDescription(indexes, pages)
	if err != nil {
		return "", err
	}
	// The pre-release policy-aware v1 spelling is checked separately. Neither
	// spelling permits a different model, source recipe, page window or table ID.
	previousPolicyAware := strings.Replace(current, "grepstein-corpus-v2:", "grepstein-corpus-v1:", 1)
	if stored != legacy && stored != previousPolicyAware {
		return "", fmt.Errorf("table configuration differs from this corpus; for an existing v1 table, first verify with its original creation reader configuration (including max_tokens), then update execution through artifact PUT")
	}
	if err := atomicCorpusJSON(filepath.Join(state, "table-binding.json"), expected); err != nil {
		return "", err
	}
	return stored, nil
}

// Reconstruct precisely the released v1 table-creation fingerprint, including
// the old recipe-level budget. This value is for verification only: it must
// never be PUT back into the table or used to invalidate accepted extraction.
func legacyCorpusDescription(indexes map[string]any, pages int) (string, error) {
	data, err := json.Marshal(indexes[corpusTextIndex])
	if err != nil {
		return "", err
	}
	var text map[string]any
	if err = json.Unmarshal(data, &text); err != nil {
		return "", err
	}
	if enrichments, ok := text["enrichments"].([]any); ok {
		for _, item := range enrichments {
			enrichment, ok := item.(map[string]any)
			if !ok {
				return "", fmt.Errorf("invalid corpus enrichment")
			}
			execution, _ := enrichment["execution"].(map[string]any)
			if budget := execution["max_output_tokens_per_item"]; budget != nil {
				raw, ok := enrichment["producer_json"].(string)
				if !ok {
					return "", fmt.Errorf("missing corpus producer")
				}
				var producer map[string]any
				if err := json.Unmarshal([]byte(raw), &producer); err != nil {
					return "", err
				}
				config, _ := producer["config"].(map[string]any)
				ocr, _ := config["ocr"].(map[string]any)
				reader, _ := ocr["reader"].(map[string]any)
				if reader == nil {
					return "", fmt.Errorf("missing corpus OCR reader")
				}
				reader["max_tokens"] = budget
				encoded, err := json.Marshal(producer)
				if err != nil {
					return "", err
				}
				enrichment["producer_json"] = string(encoded)
			}
			delete(enrichment, "execution")
		}
	}
	encoded, err := json.Marshal([]any{text, pages})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "grepstein-corpus-v1:" + hex.EncodeToString(sum[:]), nil
}
