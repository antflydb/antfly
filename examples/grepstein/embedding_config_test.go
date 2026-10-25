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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestVertexCorpusEmbeddingSharesDimensionsAndProject(t *testing.T) {
	config, err := corpusEmbedderConfig("unused", "vertex-embedder.example.json")
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := corpusIndexes("unused", DefaultAutographModel, "", "", false, true, config)
	if err != nil {
		t.Fatal(err)
	}
	index := indexes[DefaultEmbeddingIndex].(map[string]any)
	if index["dimension"] != 3072 {
		t.Fatal(index)
	}
	producer := index["embedder"].(map[string]any)
	if producer["model"] != "gemini-embedding-2" || producer["provider"] != "vertex" || producer["project_id"] != "antfly-showcases" {
		t.Fatal(producer)
	}
	enrichment := index["enrichments"].([]any)[0].(map[string]any)
	if enrichment["expected_dims"] != index["dimension"] {
		t.Fatal("producer/index dimension mismatch")
	}
	plain, err := corpusIndexes("unused", DefaultAutographModel, "", "", false, false, config)
	if err != nil {
		t.Fatal(err)
	}
	if plain[DefaultEmbeddingIndex] != nil {
		t.Fatal("configured embedder activated before semantic stage")
	}
}

func TestCorpusEmbedderRejectsWrongTagsSecretsAndDimensions(t *testing.T) {
	for _, raw := range []string{
		`{"provider":"vertex","model":"gemini-embedding-002","project_id":"p","location":"global","dimension":3072}`,
		`{"provider":"vertex","model":"gemini-embedding-2","project_id":"p","location":"us-central1","dimension":3072}`,
		`{"provider":"vertex","model":"gemini-embedding-2","project_id":"p","location":"global","dimension":3072,"api_key":"private"}`,
		`{"provider":"vertex","model":"gemini-embedding-2","project_id":"p","location":"global","dimension":2.5}`,
		`{"provider":"vertex","model":"gemini-embedding-2","project_id":"p","location":"global","dimension":3072,"credentials_path":"local-user.json"}`,
	} {
		var config map[string]any
		if err := json.Unmarshal([]byte(raw), &config); err != nil {
			t.Fatal(err)
		}
		if err := validateCorpusEmbedder(config); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	path := filepath.Join(t.TempDir(), "embedder.json")
	if err := os.WriteFile(path, []byte(`{"provider":"vertex","model":"gemini-embedding-2","project_id":"p","location":"global","dimension":768}`), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := corpusEmbedderConfig("unused", path)
	if err != nil {
		t.Fatal(err)
	}
	if corpusEmbeddingDimensions(config) != 768 {
		t.Fatal(config)
	}
}
