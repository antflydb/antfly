// Copyright 2026 Antfly, Inc.
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

package oapi

import (
	"encoding/json"
	"testing"
)

func TestChunkerConfigExposesEffectiveProviderFields(t *testing.T) {
	const response = `{"name":"semantic","type":"embeddings","dimension":3,"chunker":{"provider":"antfly","api_url":"http://inference.internal:8080","model":"fixed","store_chunks":false}}`

	var created CreatedEmbeddingsIndex
	if err := json.Unmarshal([]byte(response), &created); err != nil {
		t.Fatalf("decode created index response: %v", err)
	}
	chunker := created.Chunker
	if chunker.Model != "fixed" {
		t.Fatalf("model = %q", chunker.Model)
	}
	if chunker.ApiUrl != "http://inference.internal:8080" {
		t.Fatalf("api_url = %q", chunker.ApiUrl)
	}

	encoded, err := json.Marshal(chunker)
	if err != nil {
		t.Fatalf("encode chunker request: %v", err)
	}
	var roundTrip map[string]any
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatalf("decode round-trip JSON: %v", err)
	}
	if roundTrip["model"] != "fixed" || roundTrip["api_url"] != "http://inference.internal:8080" {
		t.Fatalf("provider fields lost during round trip: %s", encoded)
	}
}

func TestChunkerConfigDirectConstructionPreservesProviderFields(t *testing.T) {
	chunker := ChunkerConfig{
		Provider: ChunkerProviderAntfly,
		ApiUrl:   "http://inference.internal:8080",
		Model:    "custom-model",
	}

	encoded, err := json.Marshal(chunker)
	if err != nil {
		t.Fatalf("encode chunker request: %v", err)
	}
	var request map[string]any
	if err := json.Unmarshal(encoded, &request); err != nil {
		t.Fatalf("decode request JSON: %v", err)
	}
	if request["model"] != "custom-model" || request["api_url"] != "http://inference.internal:8080" {
		t.Fatalf("provider fields lost during request encoding: %s", encoded)
	}
}
