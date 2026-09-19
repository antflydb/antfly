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

package main

import (
	"encoding/json"
	"fmt"
)

// Artifact and index names used throughout dogfood. Kept as constants so the
// ingest, query, entity, and status commands all agree on the same wiring.
const (
	// relationsArtifact is the generated extraction-relation asset stream that
	// feeds the knowledge graph index (the "autograph" pattern).
	relationsArtifact = "relations_v1"

	// Index names. fullTextIndexName matches
	// zig/pkg/antfly/src/api/full_text_indexes.zig's
	// default_full_text_index_name constant: current libantfly builds
	// auto-provision full_text_index_v0 on Create, matching the full server;
	// dogfood only adds it explicitly as a fallback for older builds.
	fullTextIndexName   = "full_text_index_v0"
	chunkVectorsIndex   = "chunk_vectors"
	knowledgeGraphIndex = "knowledge"

	// qwen3EmbeddingDims is the output dimension of Qwen/Qwen3-Embedding-0.6B-GGUF.
	qwen3EmbeddingDims = 1024
)

// entityLabels and relationLabels define the GLiNER2.5 extraction schema used
// to build the knowledge graph over dogfood's own design docs and work log.
var (
	entityLabels = []string{
		"component", "subsystem", "file", "test", "invariant",
		"decision", "person", "model", "backend", "format", "protocol",
	}
	relationTypes = []string{
		"depends_on", "owns", "implements", "supersedes", "tested_by", "documented_in",
	}
)

// addIndexEnvelope is the wire shape accepted by antfly_db_add_index_json:
// {"name": ..., "kind": ..., "config_json": "<escaped JSON string>"}. kind is
// one of full_text, dense_vector, sparse_vector, graph, algebraic -- the raw
// Lite storage-engine kinds, not the public "embeddings" API discriminator.
func addIndexEnvelope(name, kind string, config any) ([]byte, error) {
	inner, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("marshal %s config: %w", name, err)
	}
	envelope := struct {
		Name       string `json:"name"`
		Kind       string `json:"kind"`
		ConfigJSON string `json:"config_json"`
	}{Name: name, Kind: kind, ConfigJSON: string(inner)}
	out, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal %s envelope: %w", name, err)
	}
	return out, nil
}

// schemaJSON returns the SetSchemaJSON body for dogfood's single document
// type: one row per markdown section (design doc or work-log entry).
func schemaJSON() []byte {
	schema := map[string]any{
		"version":      1,
		"default_type": "doc_section",
		"document_schemas": map[string]any{
			"doc_section": map[string]any{
				"schema": map[string]any{
					"type":                 "object",
					"required":             []string{"title", "body", "source", "kind"},
					"additionalProperties": true,
				},
			},
		},
	}
	out, err := json.Marshal(schema)
	if err != nil {
		// schema is a static literal; marshaling cannot fail.
		panic(err)
	}
	return out
}

// fullTextIndexJSON returns the smallest explicit default full-text index over
// the section body. dogfood always checks IndexesJSON before adding this: the
// Lite C ABI does not auto-provision a default full-text index the way a
// managed Antfly table does (confirmed against the "full_text_index_v0"
// hosted-profile capi test in zig/pkg/antfly/src/capi/db.zig, which adds the
// index explicitly before writing or querying any documents).
func fullTextIndexJSON() ([]byte, error) {
	config := map[string]any{
		"field": "body",
	}
	return addIndexEnvelope(fullTextIndexName, "full_text", config)
}

// Artifact names for the two-stage chunk pipeline: a chunk enrichment
// produces chunkArtifact rows from each section body, and an embedding
// enrichment consumes them into denseArtifact, which chunk_vectors indexes.
// This is the server's chunk-artifact pattern (go/pkg/docsaf,
// antfly.NewArtifactEmbeddingIndexConfig), so chunks are a queryable
// artifact and semantic hits carry hierarchy.parent_doc_key back to the
// section document.
const (
	chunkArtifact = "doc_chunks_v1"
	denseArtifact = "doc_chunk_dense_v1"
)

// chunkVectorsIndexJSON returns the artifact-sourced embeddings index over
// fixed-size chunks of each section body, embedded with Qwen3. Both
// enrichments travel inline on the index that owns them; Lite registers them
// in dependency order before admitting the index. With no inference URL the
// producers run on libantfly's embedded runtime.
func chunkVectorsIndexJSON(embedModel, inferenceURL string, targetTokens, overlapTokens int) ([]byte, error) {
	// chunk_size/chunk_overlap on a chunk enrichment are byte counts (the
	// runtime's fixed byte slicer, storage/db/enrichment/chunker.zig); the
	// token-aware "fixed" chunker is selected through chunker_json. Keep both
	// consistent at roughly four bytes per token so a build without the
	// tokenizer-backed chunker produces comparably sized chunks.
	const bytesPerToken = 4
	chunker, err := json.Marshal(withProviderURL(map[string]any{
		"provider": "antfly",
		"model":    "fixed",
		"text": map[string]any{
			"target_tokens":  targetTokens,
			"overlap_tokens": overlapTokens,
		},
	}, inferenceURL))
	if err != nil {
		return nil, fmt.Errorf("marshal chunker config: %w", err)
	}
	chunkEnrichment := map[string]any{
		"name":          chunkArtifact,
		"kind":          "chunk",
		"field":         "body",
		"chunk_size":    targetTokens * bytesPerToken,
		"chunk_overlap": overlapTokens * bytesPerToken,
		"chunker_json":  string(chunker),
	}
	config := map[string]any{
		"type":      "embeddings",
		"sources":   []map[string]any{{"artifact": denseArtifact}},
		"dimension": qwen3EmbeddingDims,
		"embedder": withProviderURL(map[string]any{
			"provider": "antfly",
			"model":    embedModel,
		}, inferenceURL),
		"distance_metric": "cosine",
		"enrichments": []map[string]any{
			chunkEnrichment,
			{
				"name":                 denseArtifact,
				"kind":                 "embedding",
				"field":                "body",
				"source_artifact_name": chunkArtifact,
				"expected_dims":        qwen3EmbeddingDims,
			},
		},
	}
	return addIndexEnvelope(chunkVectorsIndex, "dense_vector", config)
}

// knowledgeGraphIndexJSON returns the "autograph" graph index: a GLiNER2.5
// extractor asset producer (relations_v1) materialized into graph edges. This
// mirrors examples/epstein/main.go's createArtifactGraphIndex /
// artifactProducerConfig pattern, expressed directly as the raw index JSON
// consumed by antfly_db_add_index_json instead of SDK builder types.
func knowledgeGraphIndexJSON(extractModel, inferenceURL string, includeMetrics bool) ([]byte, error) {
	relationSchemas := make([]map[string]any, 0, len(relationTypes))
	for _, t := range relationTypes {
		relationSchemas = append(relationSchemas, map[string]any{"type": t})
	}

	producerJSON := map[string]any{
		"type": "extractor",
		"config": withProviderURL(map[string]any{
			"provider": "antfly",
			"model":    extractModel,
			"schema": map[string]any{
				"entities":  entityLabels,
				"relations": relationSchemas,
			},
			"options": map[string]any{
				"include_confidence": true,
				"include_spans":      true,
				// Real design-doc/work-log sections routinely exceed the
				// qualified single-window LengthContract (see
				// zig/pkg/inference/models/gliner2/GLINER25.md); request
				// windowed long-document execution so those sections are
				// served instead of failing closed with
				// UnsupportedGlinerBoundaryRuntime/BoundaryTextLimitExceeded.
				"long_document": map[string]any{
					"mode": "window",
				},
			},
		}, inferenceURL),
	}

	config := map[string]any{
		"source": map[string]any{
			"artifact": relationsArtifact,
			"path":     "$.relations[*]",
			"format":   "extraction_relation",
			"nodes": map[string]any{
				"model":  "document",
				"target": "{{ _item.target.text }}",
			},
			"edge": map[string]any{
				"weight": "{{ _item.score }}",
				"metadata": map[string]any{
					"type":          "{{ _item.type }}",
					"source_entity": "{{ _item.source.text }}",
					"target_entity": "{{ _item.target.text }}",
					"score":         "{{ _item.score }}",
				},
			},
		},
		"artifact": map[string]any{
			"name": relationsArtifact,
			"kind": "asset",
			"source": map[string]any{
				"type":  "field",
				"value": "body",
			},
			"content_type":  "application/json",
			"producer_json": producerJSON,
		},
		"algebraic_planning": map[string]any{
			"bounded_traversal": map[string]any{
				"law": "provenance_semiring",
			},
		},
	}
	if includeMetrics {
		config["metrics"] = map[string]any{
			"pagerank": map[string]any{
				"kind":    "pagerank",
				"enabled": true,
			},
		}
	}
	return addIndexEnvelope(knowledgeGraphIndex, "graph", config)
}

// withProviderURL records api_url on an antfly producer config only when a
// remote inference server was requested. With no URL the producer runs on
// libantfly's embedded inference runtime (Lite "local_embedded" mode).
func withProviderURL(config map[string]any, inferenceURL string) map[string]any {
	if inferenceURL != "" {
		config["api_url"] = inferenceURL
	}
	return config
}
