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
