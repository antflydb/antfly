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
