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

func TestNewCreateIndexRequestPreservesCanonicalGraphMappingPayload(t *testing.T) {
	var config IndexConfig
	if err := json.Unmarshal([]byte(`{
		"name":"relationships",
		"type":"graph",
		"source":{"artifact":"relations","nodes":{"model":"document","target":"{{ _item.target.text }}"},"edge":{"weight":"{{ _item.score }}","metadata":{"source":"extractor"}},"context":{"doc_fields":["title"]}},
		"algebraic_planning":{"bounded_traversal":{"law":"provenance_semiring"}}
	}`), &config); err != nil {
		t.Fatalf("unmarshal index config: %v", err)
	}

	request, err := NewCreateIndexRequest(config)
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
	graphSource, ok := body["source"].(map[string]any)
	if !ok {
		t.Fatalf("converted request lost graph source: %s", data)
	}
	nodes, ok := graphSource["nodes"].(map[string]any)
	if !ok || nodes["target"] != "{{ _item.target.text }}" {
		t.Fatalf("converted request lost node mapping: %s", data)
	}
	edge, ok := graphSource["edge"].(map[string]any)
	if !ok || edge["weight"] != "{{ _item.score }}" {
		t.Fatalf("converted request lost edge mapping: %s", data)
	}
	planning, ok := body["algebraic_planning"].(map[string]any)
	if !ok {
		t.Fatalf("converted request lost algebraic planning: %s", data)
	}
	bounded, ok := planning["bounded_traversal"].(map[string]any)
	if !ok || bounded["law"] != "provenance_semiring" {
		t.Fatalf("converted request lost traversal law: %s", data)
	}
}

func TestNewCreateIndexRequestSupportsTypedGraphMapping(t *testing.T) {
	var source, target, edgeType, weight GraphTemplateValue
	if err := source.FromGraphTemplateValue0("{{ _doc.key }}"); err != nil {
		t.Fatalf("set source template: %v", err)
	}
	if err := target.FromGraphTemplateValue0("{{ _item.target.text }}"); err != nil {
		t.Fatalf("set target template: %v", err)
	}
	if err := edgeType.FromGraphTemplateValue0("{{ _item.type }}"); err != nil {
		t.Fatalf("set edge type template: %v", err)
	}
	if err := weight.FromGraphTemplateValue1(0.75); err != nil {
		t.Fatalf("set edge weight: %v", err)
	}

	request, err := NewCreateIndexRequest(GraphIndexConfig{
		Source: GraphArtifactSourceConfig{
			Artifact: "relations_v1",
			Format:   GraphArtifactSourceConfigFormatExtractionGraph,
			Nodes: GraphArtifactNodeMappingConfig{
				Model:  GraphArtifactNodeMappingConfigModelDocument,
				Source: source,
				Target: target,
			},
			Edge: GraphArtifactEdgeMappingConfig{
				Type:     edgeType,
				Weight:   weight,
				Metadata: map[string]any{"source": "extractor"},
			},
			Context: GraphArtifactContextConfig{DocFields: []string{"title", "body"}},
		},
		Artifact: GraphArtifactProducerConfig{
			Name: "relations_v1",
			Kind: GraphArtifactProducerConfigKindAsset,
			Source: GraphArtifactProducerSourceConfig{
				Type:  GraphArtifactProducerSourceConfigTypeTemplate,
				Value: "{{ body }}",
			},
			Execution: ExecutionPolicy{
				BatchItems: 8,
			},
		},
		AlgebraicPlanning: GraphAlgebraicPlanningConfig{
			BoundedTraversal: GraphBoundedTraversalConfig{
				Law: GraphBoundedTraversalConfigLawProvenanceSemiring,
			},
		},
	})
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
	if body["type"] != "graph" {
		t.Fatalf("type = %v, want graph: %s", body["type"], data)
	}
	graphSource := body["source"].(map[string]any)
	edge := graphSource["edge"].(map[string]any)
	if edge["weight"] != 0.75 {
		t.Fatalf("edge weight = %v, want 0.75: %s", edge["weight"], data)
	}
	planning := body["algebraic_planning"].(map[string]any)
	bounded := planning["bounded_traversal"].(map[string]any)
	if bounded["law"] != "provenance_semiring" {
		t.Fatalf("traversal law = %v: %s", bounded["law"], data)
	}
	if _, exists := bounded["enabled"]; exists {
		t.Fatalf("bounded traversal must use presence semantics without enabled: %s", data)
	}
	artifact := body["artifact"].(map[string]any)
	sourceConfig := artifact["source"].(map[string]any)
	if sourceConfig["type"] != "template" || sourceConfig["value"] != "{{ body }}" {
		t.Fatalf("artifact source = %v: %s", sourceConfig, data)
	}
	execution := artifact["execution"].(map[string]any)
	if execution["batch_items"] != float64(8) {
		t.Fatalf("artifact batch_items = %v: %s", execution["batch_items"], data)
	}
}

func TestNewArtifactEmbeddingIndexConfig(t *testing.T) {
	embedder, err := NewEmbedderConfig(OllamaEmbedderConfig{Model: "embeddinggemma"})
	if err != nil {
		t.Fatalf("NewEmbedderConfig failed: %v", err)
	}

	idx, err := NewArtifactEmbeddingIndexConfig("document_vectors", ArtifactEmbeddingIndexConfig{
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
	if _, exists := body["embedding_name"]; exists {
		t.Fatalf("embedding_name must be omitted for sources mode: %s", data)
	}
	sources, ok := body["sources"].([]any)
	if !ok || len(sources) != 1 || sources[0].(map[string]any)["artifact"] != "document_chunk_dense_v1" {
		t.Fatalf("sources = %#v, want document_chunk_dense_v1", body["sources"])
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
	if _, exists := enrichment["vector_space"]; exists {
		t.Fatalf("same-producer helper must not emit vector_space")
	}
}

func TestNewArtifactEmbeddingIndexConfigSupportsDocumentAndChunkSources(t *testing.T) {
	embedder, err := NewEmbedderConfig(AntflyEmbedderConfig{Model: "antflydb/clipclap"})
	if err != nil {
		t.Fatal(err)
	}
	idx, err := NewArtifactEmbeddingIndexConfig("document_vectors", ArtifactEmbeddingIndexConfig{
		Sources: []ArtifactEmbeddingSource{
			{ArtifactName: "document_dense_v1", SourceField: "semantic_content"},
			{ArtifactName: "document_chunk_dense_v1", SourceArtifactName: "document_chunks_v1"},
		},
		ExpectedDims: 384,
		Embedder:     *embedder,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	if len(body["sources"].([]any)) != 2 || len(body["enrichments"].([]any)) != 2 {
		t.Fatalf("multi-source config lost members: %s", data)
	}
}

func TestNewArtifactEmbeddingIndexConfigTemplateOmitsNoopField(t *testing.T) {
	embedder, err := NewEmbedderConfig(AntflyEmbedderConfig{Model: "antflydb/clipclap"})
	if err != nil {
		t.Fatal(err)
	}
	idx, err := NewArtifactEmbeddingIndexConfig("templated_vectors", ArtifactEmbeddingIndexConfig{
		Sources: []ArtifactEmbeddingSource{{
			ArtifactName:   "templated_v1",
			SourceField:    "text",
			SourceTemplate: "{{ title }}: {{ body }}",
		}},
		Embedder: *embedder,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	enrichment := body["enrichments"].([]any)[0].(map[string]any)
	if _, exists := enrichment["field"]; exists {
		t.Fatalf("template-only enrichment must omit no-op field: %s", data)
	}
}

func TestNewGraphIndexSourcesValidatesAndCopies(t *testing.T) {
	typeValue, err := NewGraphTemplateValue("{{ _item.type }}")
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{"origin": "extractor", "nested": map[string]any{"score": float64(1)}}
	sources, err := NewGraphIndexSources(
		GraphArtifactSourceConfig{
			Artifact: "relations_v1",
			Path:     "$.relations[*]",
			Format:   GraphArtifactSourceConfigFormatExtractionRelation,
			Nodes: GraphArtifactNodeMappingConfig{
				Model: GraphArtifactNodeMappingConfigModelExternal,
			},
			Edge:    GraphArtifactEdgeMappingConfig{Type: typeValue, Metadata: metadata},
			Context: GraphArtifactContextConfig{DocFields: []string{"title", "url"}},
		},
		GraphArtifactSourceConfig{Artifact: "graph_v1", Format: GraphArtifactSourceConfigFormatExtractionGraph},
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata["nested"].(map[string]any)["score"] = float64(2)
	if got := sources[0].Edge.Metadata["nested"].(map[string]any)["score"]; got != float64(1) {
		t.Fatalf("metadata was not defensively copied: %v", got)
	}
	if len(sources[0].Context.DocFields) != 2 || sources[1].Format != GraphArtifactSourceConfigFormatExtractionGraph {
		t.Fatalf("graph sources lost configuration: %#v", sources)
	}

	if _, err := NewGraphIndexSources(
		GraphArtifactSourceConfig{Artifact: "same"},
		GraphArtifactSourceConfig{Artifact: "same"},
	); err == nil {
		t.Fatal("duplicate graph sources must be rejected")
	}
	if _, err := NewGraphIndexSources(GraphArtifactSourceConfig{Artifact: "relations", Path: "$.relations[0]"}); err == nil {
		t.Fatal("unsupported graph artifact paths must be rejected")
	}
}

func TestNewArtifactFullTextIndexConfig(t *testing.T) {
	config, err := NewArtifactFullTextIndexConfig("document_text", "document_text_v1", "document_chunks_v1")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	if body["name"] != "document_text" || len(body["sources"].([]any)) != 2 {
		t.Fatalf("multi-source full-text config lost members: %s", data)
	}

	configured, err := NewArtifactFullTextIndexConfigWithOptions(
		"document_text",
		ArtifactFullTextIndexOptions{Field: " text ", MemOnly: true},
		"document_text_v1",
		"document_chunks_v1",
	)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(configured)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	if body["field"] != "text" || body["mem_only"] != true {
		t.Fatalf("artifact full-text options were not preserved: %s", data)
	}
}
