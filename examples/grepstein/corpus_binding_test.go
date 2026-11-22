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
	"os"
	"path/filepath"
	"testing"
)

func TestCorpusV1BindingUpgradePreservesAcceptedCheckpoints(t *testing.T) {
	state := t.TempDir()
	config := filepath.Join(t.TempDir(), "reader.json")
	if err := os.WriteFile(config, []byte(`{"provider":"antfly","model":"reader-v1","max_tokens":8192}`), 0600); err != nil {
		t.Fatal(err)
	}
	indexes, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "", config, false, false)
	if err != nil {
		t.Fatal(err)
	}
	text := indexes[corpusTextIndex].(map[string]any)
	asset := text["enrichments"].([]any)[0].(map[string]any)
	original := asset["producer_json"]
	var producer map[string]any
	if err := json.Unmarshal([]byte(original.(string)), &producer); err != nil {
		t.Fatal(err)
	}
	reader := producer["config"].(map[string]any)["ocr"].(map[string]any)["reader"].(map[string]any)
	reader["max_tokens"] = 8192
	encoded, _ := json.Marshal(producer)
	asset["producer_json"] = string(encoded)
	execution := asset["execution"]
	delete(asset, "execution")
	raw, _ := json.Marshal([]any{text, 25})
	sum := sha256.Sum256(raw)
	legacy := "grepstein-corpus-v1:" + hex.EncodeToString(sum[:])
	asset["producer_json"] = original
	asset["execution"] = execution
	checkpoint := []byte(`{"complete":true,"binding":"accepted-original-binding","records":42}`)
	if err := os.WriteFile(filepath.Join(state, "load.json"), checkpoint, 0600); err != nil {
		t.Fatal(err)
	}
	verify := func(id string) (string, error) {
		return resolveCorpusDescription(state, nil, "https://example/db/v1", "corpus", id, legacy, indexes, 25)
	}
	got, err := verify("table-1")
	if err != nil || got != legacy {
		t.Fatalf("migration: %q %v", got, err)
	}
	after, _ := os.ReadFile(filepath.Join(state, "load.json"))
	if string(after) != string(checkpoint) {
		t.Fatal("migration rewrote accepted checkpoint")
	}
	asset["execution"].(map[string]any)["max_output_tokens_per_item"] = uint32(16384)
	if got, err = verify("table-1"); err != nil || got != legacy {
		t.Fatalf("policy update: %q %v", got, err)
	}
	if _, err = verify("table-2"); err == nil {
		t.Fatal("reused another table's binding")
	}
	next := t.TempDir()
	if got, err = resolveCorpusDescription(next, []string{state}, "https://example/db/v1", "corpus", "table-1", legacy, indexes, 25); err != nil || got != legacy {
		t.Fatalf("append migration: %q %v", got, err)
	}
	reader["model"] = "reader-v2"
	delete(reader, "max_tokens")
	changed, _ := json.Marshal(producer)
	asset["producer_json"] = string(changed)
	if _, err = verify("table-1"); err == nil {
		t.Fatal("accepted a changed model")
	}
}
