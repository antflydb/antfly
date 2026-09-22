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
	"strings"

	"github.com/antflydb/antfly/go/pkg/antflylite"
)

// Provisioning is name-keyed (an existing index is never re-added), so a
// re-run with different flags -- another extractor, embedding model, chunk
// size, or a changed schema in this file -- would otherwise silently keep the
// old configuration. verifyExistingIndexConfigs compares what this build
// would create against what the database actually has and fails with a
// rebuild instruction on drift, the same guard
// examples/epstein/autoschema.go's verifyAutoschemaGraphIndexConfig applies
// on the server.

// verifyKnowledgeGraphConfig subset-compares the desired graph config against
// the stored one. Graph configs pass through Lite's AddIndex untranslated, so
// every field this build would set must be present and equal in the stored
// config. The "metrics" section is advisory: ensureSchemaAndIndexes retries
// index creation without metrics on deployments that reject the metric kind,
// so its absence warns instead of failing.
func verifyKnowledgeGraphConfig(storedConfigJSON string, cfg indexBuildConfig) error {
	desired := knowledgeGraphConfig(cfg.ExtractModel, cfg.InferenceURL, cfg.Metrics)
	desiredValue, err := jsonRoundTrip(desired)
	if err != nil {
		return fmt.Errorf("normalize desired %s config: %w", knowledgeGraphIndex, err)
	}
	var stored any
	if err := json.Unmarshal([]byte(storedConfigJSON), &stored); err != nil {
		return fmt.Errorf("decode stored %s config: %w", knowledgeGraphIndex, err)
	}

	desiredMap, _ := desiredValue.(map[string]any)
	storedMap, _ := stored.(map[string]any)
	if desiredMap != nil && storedMap != nil {
		if _, wantMetrics := desiredMap["metrics"]; wantMetrics {
			if _, haveMetrics := storedMap["metrics"]; !haveMetrics {
				fmt.Printf("note: index %q has no %q section (creation likely fell back to -metrics=false); re-ingest with -reset to enable it\n",
					knowledgeGraphIndex, "metrics")
				delete(desiredMap, "metrics")
			}
		}
	}

	if path, mismatch := subsetMismatch(desiredValue, stored, "$"); mismatch {
		return fmt.Errorf(
			"index %q exists with a different configuration (first drift at %s); "+
				"the extractor, schema, or resolver settings changed since it was created. "+
				"Re-run ingest with -reset (or a fresh -db path) to rebuild it",
			knowledgeGraphIndex, path)
	}
	return nil
}

// catalogEnrichment is the subset of the enrichment-catalog row shape
// (db_mod.types.EnrichmentConfig serialized by antfly_db_list_enrichments_json)
// dogfood verifies.
type catalogEnrichment struct {
	Name               string `json:"name"`
	Kind               string `json:"kind"`
	Field              string `json:"field"`
	SourceArtifactName string `json:"source_artifact_name"`
	ExpectedDims       int    `json:"expected_dims"`
	ChunkSize          int    `json:"chunk_size"`
	ChunkOverlap       int    `json:"chunk_overlap"`
	ProducerJSON       string `json:"producer_json"`
}

// verifyChunkPipelineEnrichments checks the chunk_vectors pipeline against
// the enrichment catalog: the chunk slicing geometry, the embedding stage's
// artifact wiring and dimensions, and (via the stored index config, which is
// translated for dense indexes and therefore only checked for the model
// string) the embedding model itself.
func verifyChunkPipelineEnrichments(db *antflylite.DB, storedChunkVectorsConfig string, cfg indexBuildConfig) error {
	raw, err := db.EnrichmentsJSON()
	if err != nil {
		return fmt.Errorf("list enrichments: %w", err)
	}
	var enrichments []catalogEnrichment
	if err := json.Unmarshal(raw, &enrichments); err != nil {
		return fmt.Errorf("decode enrichments: %w\nraw: %s", err, raw)
	}
	byName := make(map[string]catalogEnrichment, len(enrichments))
	for _, e := range enrichments {
		byName[e.Name] = e
	}

	const bytesPerToken = 4
	drift := func(name, what string, want, got any) error {
		return fmt.Errorf(
			"enrichment %q exists with %s=%v (this run wants %v); ingestion settings changed since the database was built. "+
				"Re-run ingest with -reset (or a fresh -db path) to rebuild",
			name, what, got, want)
	}

	if chunk, ok := byName[chunkArtifact]; ok {
		if want := cfg.TargetTokens * bytesPerToken; chunk.ChunkSize != want {
			return drift(chunkArtifact, "chunk_size", want, chunk.ChunkSize)
		}
		if want := cfg.OverlapTokens * bytesPerToken; chunk.ChunkOverlap != want {
			return drift(chunkArtifact, "chunk_overlap", want, chunk.ChunkOverlap)
		}
	}
	if dense, ok := byName[denseArtifact]; ok {
		if dense.SourceArtifactName != chunkArtifact {
			return drift(denseArtifact, "source_artifact_name", chunkArtifact, dense.SourceArtifactName)
		}
		if dense.ExpectedDims != qwen3EmbeddingDims {
			return drift(denseArtifact, "expected_dims", qwen3EmbeddingDims, dense.ExpectedDims)
		}
	}
	if extractor, ok := byName[relationsArtifact]; ok {
		if !strings.Contains(extractor.ProducerJSON, cfg.ExtractModel) {
			return drift(relationsArtifact, "extraction model", cfg.ExtractModel, "a different producer configuration")
		}
	}
	// Dense index configs are translated on AddIndex, so the embedder is only
	// checked for the configured model name surviving in the stored config.
	if storedChunkVectorsConfig != "" && !strings.Contains(storedChunkVectorsConfig, cfg.EmbedModel) {
		return fmt.Errorf(
			"index %q was built with a different embedding model than -embed-model %s; "+
				"re-run ingest with -reset (or a fresh -db path) to rebuild",
			chunkVectorsIndex, cfg.EmbedModel)
	}
	return nil
}

// jsonRoundTrip normalizes a Go config map into generic JSON values (numbers
// as float64, maps as map[string]any) so it compares against decoded stored
// configs.
func jsonRoundTrip(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// subsetMismatch reports whether `want` is NOT a subset of `got`: every
// object key in want must exist in got with a recursively matching value,
// arrays must match element-wise at equal length, and scalars must be equal.
// Returns the JSONPath-ish location of the first mismatch.
func subsetMismatch(want, got any, path string) (string, bool) {
	switch wantTyped := want.(type) {
	case map[string]any:
		gotMap, ok := got.(map[string]any)
		if !ok {
			return path, true
		}
		for key, wantValue := range wantTyped {
			gotValue, present := gotMap[key]
			if !present {
				return path + "." + key, true
			}
			if at, mismatch := subsetMismatch(wantValue, gotValue, path+"."+key); mismatch {
				return at, true
			}
		}
		return "", false
	case []any:
		gotSlice, ok := got.([]any)
		if !ok || len(gotSlice) != len(wantTyped) {
			return path, true
		}
		for i, wantValue := range wantTyped {
			if at, mismatch := subsetMismatch(wantValue, gotSlice[i], fmt.Sprintf("%s[%d]", path, i)); mismatch {
				return at, true
			}
		}
		return "", false
	default:
		if want != got {
			return path, true
		}
		return "", false
	}
}
