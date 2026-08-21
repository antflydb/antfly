package sdk

import (
	"encoding/json"
	"testing"
)

func TestNewEmbedderConfigSupportsAntfly(t *testing.T) {
	cfg, err := NewEmbedderConfig(AntflyEmbedderConfig{
		Model: "antflydb/clipclap",
	})
	if err != nil {
		t.Fatalf("NewEmbedderConfig failed: %v", err)
	}
	if cfg.Provider != EmbedderProviderAntfly {
		t.Fatalf("provider = %q, want %q", cfg.Provider, EmbedderProviderAntfly)
	}

	embedder, err := cfg.AsAntflyEmbedderConfig()
	if err != nil {
		t.Fatalf("AsAntflyEmbedderConfig failed: %v", err)
	}
	if embedder.Model != "antflydb/clipclap" {
		t.Fatalf("model = %q, want %q", embedder.Model, "antflydb/clipclap")
	}
}

func TestNewCreateIndexRequestOmitsPathIdentity(t *testing.T) {
	request, err := NewCreateIndexRequest(EmbeddingsIndexConfig{Dimension: 512})
	if err != nil {
		t.Fatalf("NewCreateIndexRequest failed: %v", err)
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal create index request: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("unmarshal create index request: %v", err)
	}
	if _, exists := body["name"]; exists {
		t.Fatalf("request must not duplicate path-owned name: %s", data)
	}
	if body["type"] != "embeddings" || body["dimension"] != float64(512) {
		t.Fatalf("unexpected request: %s", data)
	}
}

func TestNewCreateIndexRequestPreservesFullTypedRequest(t *testing.T) {
	request, err := NewCreateIndexRequest(CreateEmbeddingsIndexRequest{
		Description: "semantic product search",
		Dimension:   768,
		External:    true,
	})
	if err != nil {
		t.Fatalf("NewCreateIndexRequest failed: %v", err)
	}
	variant, err := request.AsCreateEmbeddingsIndexRequest()
	if err != nil {
		t.Fatalf("AsCreateEmbeddingsIndexRequest failed: %v", err)
	}
	if variant.Type != CreateEmbeddingsIndexRequestTypeEmbeddings {
		t.Fatalf("type = %q, want embeddings", variant.Type)
	}
	if variant.Description != "semantic product search" || variant.Dimension != 768 || !variant.External {
		t.Fatalf("common or variant fields were lost: %#v", variant)
	}
}

func TestNewCreateIndexRequestConvertsNamedIndexConfig(t *testing.T) {
	legacy, err := NewIndexConfig("semantic", EmbeddingsIndexConfig{
		Dimension: 384,
		Chunker: ChunkerConfig{
			Provider: ChunkerProviderAntfly,
			ApiUrl:   "https://inference.example/ai/v1",
			Model:    "antfly/chunker-v2",
		},
	})
	if err != nil {
		t.Fatalf("NewIndexConfig failed: %v", err)
	}

	request, err := NewCreateIndexRequest(legacy)
	if err != nil {
		t.Fatalf("NewCreateIndexRequest failed: %v", err)
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal create index request: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("unmarshal create index request: %v", err)
	}
	if _, exists := body["name"]; exists {
		t.Fatalf("request must not duplicate path-owned name: %s", data)
	}
	chunker, ok := body["chunker"].(map[string]any)
	if !ok {
		t.Fatalf("chunker missing from converted request: %s", data)
	}
	if chunker["model"] != "antfly/chunker-v2" || chunker["api_url"] != "https://inference.example/ai/v1" {
		t.Fatalf("converted request lost chunker fields: %s", data)
	}
}

func TestNewCreateIndexRequestPreservesLegacyVariantPayload(t *testing.T) {
	var legacy IndexConfig
	if err := json.Unmarshal([]byte(`{
		"name":"relationships",
		"type":"graph",
		"source":{"kind":"artifact","artifact":"relations"},
		"future_option":{"enabled":true}
	}`), &legacy); err != nil {
		t.Fatalf("unmarshal index config: %v", err)
	}

	request, err := NewCreateIndexRequest(legacy)
	if err != nil {
		t.Fatalf("NewCreateIndexRequest failed: %v", err)
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal create index request: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("unmarshal create index request: %v", err)
	}
	if _, exists := body["name"]; exists {
		t.Fatalf("request must not duplicate path-owned name: %s", data)
	}
	future, ok := body["future_option"].(map[string]any)
	if !ok || future["enabled"] != true {
		t.Fatalf("converted request lost variant payload: %s", data)
	}
}

func TestNewArtifactEmbeddingIndexConfig(t *testing.T) {
	embedder, err := NewEmbedderConfig(OllamaEmbedderConfig{Model: "embeddinggemma"})
	if err != nil {
		t.Fatalf("NewEmbedderConfig failed: %v", err)
	}

	idx, err := NewArtifactEmbeddingIndexConfig("document_vectors", ArtifactEmbeddingIndexConfig{
		SourceArtifactName: "document_chunks_v1",
		EmbeddingName:      "document_chunk_dense_v1",
		SourceField:        "text",
		ExpectedDims:       768,
		Embedder:           *embedder,
		DistanceMetric:     DistanceMetricCosine,
	})
	if err != nil {
		t.Fatalf("NewArtifactEmbeddingIndexConfig failed: %v", err)
	}

	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal index config: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("unmarshal index config: %v", err)
	}
	if body["type"] != "embeddings" {
		t.Fatalf("type = %v, want embeddings", body["type"])
	}
	if body["name"] != "document_vectors" {
		t.Fatalf("name = %v, want document_vectors", body["name"])
	}
	if body["field"] != "embedding" {
		t.Fatalf("field = %v, want embedding", body["field"])
	}
	if body["embedding_name"] != "document_chunk_dense_v1" {
		t.Fatalf("embedding_name = %v, want document_chunk_dense_v1", body["embedding_name"])
	}
	if body["source_artifact_name"] != "document_chunks_v1" {
		t.Fatalf("source_artifact_name = %v, want document_chunks_v1", body["source_artifact_name"])
	}

	enrichments, ok := body["enrichments"].([]any)
	if !ok || len(enrichments) != 1 {
		t.Fatalf("enrichments = %#v, want one structured enrichment", body["enrichments"])
	}
	enrichment, ok := enrichments[0].(map[string]any)
	if !ok {
		t.Fatalf("enrichment = %#v, want object", enrichments[0])
	}
	if enrichment["kind"] != "embedding" {
		t.Fatalf("enrichment kind = %v, want embedding", enrichment["kind"])
	}
	if enrichment["name"] != "document_chunk_dense_v1" {
		t.Fatalf("enrichment name = %v, want document_chunk_dense_v1", enrichment["name"])
	}
	if enrichment["field"] != "text" {
		t.Fatalf("enrichment field = %v, want text", enrichment["field"])
	}
	if enrichment["source_artifact_name"] != "document_chunks_v1" {
		t.Fatalf("enrichment source_artifact_name = %v, want document_chunks_v1", enrichment["source_artifact_name"])
	}
	if enrichment["expected_dims"] != float64(768) {
		t.Fatalf("enrichment expected_dims = %v, want 768", enrichment["expected_dims"])
	}
}
