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
		VectorSpace: "test:dense-v1",
		Sources: []ArtifactEmbeddingSource{{
			ArtifactName:       "document_chunk_dense_v1",
			SourceArtifactName: "document_chunks_v1",
			SourceField:        "text",
		}},
		ExpectedDims:   768,
		Embedder:       *embedder,
		DistanceMetric: DistanceMetricCosine,
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
	if _, ok := body["field"]; ok {
		t.Fatalf("field = %v, want omitted", body["field"])
	}
	if _, ok := body["embedding_name"]; ok {
		t.Fatalf("embedding_name = %v, want omitted", body["embedding_name"])
	}
	if _, ok := body["source_artifact_name"]; ok {
		t.Fatalf("source_artifact_name = %v, want omitted", body["source_artifact_name"])
	}
	sources, ok := body["sources"].([]any)
	if !ok || len(sources) != 1 {
		t.Fatalf("sources = %#v, want one source", body["sources"])
	}
	source, ok := sources[0].(map[string]any)
	if !ok || source["artifact"] != "document_chunk_dense_v1" {
		t.Fatalf("source = %#v, want document_chunk_dense_v1", sources[0])
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

func TestNewArtifactEmbeddingIndexConfigSupportsMultipleSources(t *testing.T) {
	embedder, err := NewEmbedderConfig(AntflyEmbedderConfig{Model: "antflydb/clipclap"})
	if err != nil {
		t.Fatalf("NewEmbedderConfig failed: %v", err)
	}
	idx, err := NewArtifactEmbeddingIndexConfig("document_vectors", ArtifactEmbeddingIndexConfig{
		VectorSpace: "test:dense-v1",
		Sources: []ArtifactEmbeddingSource{
			{ArtifactName: "title_dense_v1", SourceArtifactName: "title_chunks_v1"},
			{ArtifactName: "body_dense_v1", SourceArtifactName: "body_chunks_v1"},
		},
		ExpectedDims: 384,
		Embedder:     *embedder,
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
	if got := len(body["sources"].([]any)); got != 2 {
		t.Fatalf("sources len = %d, want 2", got)
	}
	if got := len(body["enrichments"].([]any)); got != 2 {
		t.Fatalf("enrichments len = %d, want 2", got)
	}
}

func TestNewArtifactEmbeddingIndexConfigAllowsAutomaticVectorSpace(t *testing.T) {
	embedder, err := NewEmbedderConfig(AntflyEmbedderConfig{Model: "antflydb/clipclap"})
	if err != nil {
		t.Fatalf("NewEmbedderConfig failed: %v", err)
	}
	idx, err := NewArtifactEmbeddingIndexConfig("document_vectors", ArtifactEmbeddingIndexConfig{
		Sources: []ArtifactEmbeddingSource{
			{ArtifactName: "title_dense_v1", SourceField: "title"},
			{ArtifactName: "body_dense_v1", SourceField: "body"},
		},
		ExpectedDims: 384,
		Embedder:     *embedder,
	})
	if err != nil {
		t.Fatalf("NewArtifactEmbeddingIndexConfig failed: %v", err)
	}
	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal index config: %v", err)
	}
	if string(data) == "" || json.Valid(data) == false {
		t.Fatalf("invalid index JSON: %s", data)
	}
	if string(data) != "" && containsJSONField(data, "vector_space") {
		t.Fatalf("vector_space should be omitted for automatic validation: %s", data)
	}
}

func containsJSONField(data []byte, field string) bool {
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		return false
	}
	enrichments, _ := body["enrichments"].([]any)
	for _, raw := range enrichments {
		if enrichment, ok := raw.(map[string]any); ok {
			if _, exists := enrichment[field]; exists {
				return true
			}
		}
	}
	return false
}

func TestGraphIndexConfigUsesPerSourcePathAndFormat(t *testing.T) {
	graphSources, err := NewGraphIndexSources(
		GraphIndexSource{
			Artifact: "title_relations_v1",
			Path:     "$.relations[*]",
			Format:   GraphIndexSourceFormatExtractionRelation,
		},
		GraphIndexSource{
			Artifact: "entity_graph_v1",
			Path:     "$.graph",
			Format:   GraphIndexSourceFormatExtractionGraph,
		},
	)
	if err != nil {
		t.Fatalf("NewGraphIndexSources failed: %v", err)
	}
	idx, err := NewIndexConfig("knowledge_graph", GraphIndexConfig{
		Sources: graphSources,
	})
	if err != nil {
		t.Fatalf("NewIndexConfig failed: %v", err)
	}
	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatalf("marshal index config: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("unmarshal index config: %v", err)
	}
	if _, ok := body["path"]; ok {
		t.Fatalf("graph path must be source-specific: %s", data)
	}
	sources, ok := body["sources"].([]any)
	if !ok || len(sources) != 2 {
		t.Fatalf("sources = %#v, want two graph sources", body["sources"])
	}
	first := sources[0].(map[string]any)
	second := sources[1].(map[string]any)
	if first["path"] != "$.relations[*]" || first["format"] != "extraction_relation" {
		t.Fatalf("first source = %#v", first)
	}
	if second["path"] != "$.graph" || second["format"] != "extraction_graph" {
		t.Fatalf("second source = %#v", second)
	}
}

func TestNewArtifactIndexSources(t *testing.T) {
	sources, err := NewArtifactIndexSources("title_chunks_v1", "body_chunks_v1")
	if err != nil {
		t.Fatalf("NewArtifactIndexSources failed: %v", err)
	}
	if len(sources) != 2 || sources[0].Artifact != "title_chunks_v1" || sources[1].Artifact != "body_chunks_v1" {
		t.Fatalf("sources = %#v", sources)
	}
	if _, err := NewArtifactIndexSources("body_chunks_v1", "body_chunks_v1"); err == nil {
		t.Fatal("duplicate artifact sources should fail")
	}
}

func TestArtifactIndexHelpersRejectInvalidEnums(t *testing.T) {
	if _, err := NewGraphIndexSources(GraphIndexSource{Artifact: "relations_v1", Format: "unknown"}); err == nil {
		t.Fatal("invalid graph source format should fail")
	}
	if _, err := NewArtifactEmbeddingIndexConfig("vectors", ArtifactEmbeddingIndexConfig{
		Sources:        []ArtifactEmbeddingSource{{ArtifactName: "dense_v1"}},
		Embedder:       EmbedderConfig{Provider: "antfly"},
		DistanceMetric: "unknown",
	}); err == nil {
		t.Fatal("invalid distance metric should fail")
	}
}

func TestNewIndexConfigSupportsSchemaDerivedAlgebraicIndex(t *testing.T) {
	idx, err := NewIndexConfig("algebraic", AlgebraicIndexConfig{DeriveFromSchema: true})
	if err != nil {
		t.Fatalf("NewIndexConfig failed: %v", err)
	}
	if idx.Type != IndexTypeAlgebraic {
		t.Fatalf("index type = %q, want %q", idx.Type, IndexTypeAlgebraic)
	}
}
