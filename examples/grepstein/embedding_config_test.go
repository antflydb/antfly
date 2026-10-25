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
	indexes, err := corpusIndexes("unused", DefaultAutographModel, "", "", false, true, corpusIndexOptions{Embedder: config})
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
	plain, err := corpusIndexes("unused", DefaultAutographModel, "", "", false, false, corpusIndexOptions{Embedder: config})
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

func TestHostedCorpusProfilesKeepModelsDimensionsAndRoutingAligned(t *testing.T) {
	config, err := corpusEmbedderConfig("unused", "antfly-embedder.example.json")
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := corpusIndexes("unused", DefaultAutographModel, "https://inference.example", "florence-reader.example.json", true, true, corpusIndexOptions{Embedder: config, InferenceSecret: "hosted.inference.key"})
	if err != nil {
		t.Fatal(err)
	}
	semantic := indexes[DefaultEmbeddingIndex].(map[string]any)
	if semantic["dimension"] != 768 || semantic["enrichments"].([]any)[0].(map[string]any)["expected_dims"] != 768 {
		t.Fatal(semantic)
	}
	if semantic["embedder"].(map[string]any)["api_url"] != "https://inference.example/ai/v1" {
		t.Fatal(semantic)
	}
	if config["provider"] != "antfly" || config["model"] != "google/embeddinggemma-2:safetensors@914f7f89142e33e77833254d9c9b90c3cef7303b" {
		t.Fatal(config)
	}
	text := indexes[corpusTextIndex].(map[string]any)
	asset := text["enrichments"].([]any)[0].(map[string]any)
	var producer map[string]any
	if err := json.Unmarshal([]byte(asset["producer_json"].(string)), &producer); err != nil {
		t.Fatal(err)
	}
	ocr := producer["config"].(map[string]any)["ocr"].(map[string]any)
	reader := ocr["config"].(map[string]any)
	if ocr["enabled"] != true || ocr["executor"] != "reader" || reader["model"] != DefaultOCRModel || reader["api_url"] != "https://inference.example/ai/v1" {
		t.Fatal(ocr)
	}
	graphBytes, err := json.Marshal(indexes[DefaultAutographIndex])
	if err != nil {
		t.Fatal(err)
	}
	var graph map[string]any
	if err := json.Unmarshal(graphBytes, &graph); err != nil {
		t.Fatal(err)
	}
	extractor := graph["artifact"].(map[string]any)["producer_json"].(map[string]any)["config"].(map[string]any)
	if reader["api_key"] != "${secret:hosted.inference.key}" || semantic["embedder"].(map[string]any)["api_key"] != reader["api_key"] || extractor["api_key"] != reader["api_key"] {
		t.Fatal("inference credentials must remain the same server-side secret reference")
	}
	if extractor["model"] != DefaultAutographModel || extractor["api_url"] != reader["api_url"] {
		t.Fatal(extractor)
	}
	if len(extractor["schema"].(map[string]any)["relations"].([]any)) == 0 {
		t.Fatal("missing relationship schema")
	}
}

func TestHostedInferenceSecretRejectsAmbiguousCredentialRouting(t *testing.T) {
	for _, option := range []corpusIndexOptions{
		{InferenceSecret: "${secret:nested}"},
		{InferenceSecret: "key with spaces"},
		{InferenceSecret: "hosted.key", Embedder: map[string]any{"provider": "antfly", "model": DefaultEmbeddingModel, "dimension": 512, "api_url": "https://other.example/ai/v1"}},
	} {
		if _, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "https://inference.example", "", true, true, option); err == nil {
			t.Fatalf("accepted ambiguous credential routing: %+v", option)
		}
	}
	if _, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "", "", false, true, corpusIndexOptions{InferenceSecret: "hosted.key"}); err == nil {
		t.Fatal("secret requires an explicit endpoint")
	}
	for _, value := range []string{"private-token", "${secret:}", "${secret:key}literal}"} {
		config := map[string]any{"provider": "antfly", "model": DefaultEmbeddingModel, "dimension": 512, "api_key": value}
		if err := validateCorpusEmbedder(config); err == nil {
			t.Fatal("accepted inline or malformed credential")
		}
	}
}
