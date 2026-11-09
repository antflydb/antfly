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

package sources

import (
	"encoding/json"
	"fmt"
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"math"
	"strings"
)

// TextProcessingPreset composes ordinary native enrichments. Models, prompts
// and secret references are explicit native producer settings, never inferred
// from a provider connection. An empty Producer uses acquired inline text.
type TextProcessingPreset struct {
	OutputField   string
	ContentType   string
	Name          string
	Field         string
	ResourceField string
	Purpose       string
	Generated     bool
	Producer      map[string]any
	VectorSpace   *VectorSpace
	Embedder      map[string]any
}

func BuildTextProcessing(preset TextProcessingPreset) (ProcessingDeclaration, SearchStream, ArtifactBinding, error) {
	if !validBindingName(preset.Name) || (preset.Field == "") == (preset.ResourceField == "") {
		return ProcessingDeclaration{}, SearchStream{}, ArtifactBinding{}, fmt.Errorf("processing preset requires a name and source")
	}

	input := preset.Field
	if input == "" {
		input = preset.ResourceField + ".uri"
	}
	producer := preset.Producer
	if preset.ResourceField != "" && producer == nil {
		return ProcessingDeclaration{}, SearchStream{}, ArtifactBinding{}, fmt.Errorf("resource processing requires an explicit native producer")
	}
	if producer == nil {
		producer = map[string]any{"type": "copy"}
	}
	contentType := preset.ContentType
	outputField := preset.OutputField
	if contentType == "" {
		contentType = "text/plain"
		if producer["type"] == "document_extraction" || producer["type"] == "transcriber" {
			contentType = "application/json"
		}
	}
	if outputField == "" {
		outputField = "text"
	}
	if contentType == "text/plain" && outputField != "text" {
		return ProcessingDeclaration{}, SearchStream{}, ArtifactBinding{}, fmt.Errorf("plain text assets use the native text indexing slot")
	}
	unitConfig := map[string]any{"kind": "asset", "field": input, "content_type": contentType, "producer": producer}
	// Native field selectors name literal root fields. Logical nested source
	// paths use raw-text templates: ordinary Handlebars interpolation would
	// HTML-escape both content and signed resource URLs.
	if strings.Contains(input, ".") {
		delete(unitConfig, "field")
		unitConfig["template"] = "{{{" + input + "}}}"
	}
	if preset.ResourceField != "" && producer["type"] == "generator" {
		delete(unitConfig, "field")
		unitConfig["template"] = "{{#if " + input + "}}{{remoteMedia url=" + input + "}}{{/if}}"
	}

	if preset.Purpose == "summary" || preset.Purpose == "caption" {
		preset.Generated = true
	}
	units := NativeIndexName(preset.Name+"_units", unitConfig)
	unitConfig["name"] = units
	chunkConfig := map[string]any{"kind": "chunk", "source_artifact_name": units, "field": outputField, "chunk_size": 512, "chunk_overlap": 64}
	chunks := NativeIndexName(preset.Name+"_chunks", chunkConfig)
	chunkConfig["name"] = chunks
	enrichments := []any{unitConfig}
	textConfig := map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": units, "field": outputField}}, "enrichments": enrichments}
	textName := NativeIndexName(preset.Name+"_text", textConfig)
	projectionField := outputField
	if contentType == "text/plain" {
		projectionField = "$"
	}
	stream := SearchStream{Name: preset.Name, Artifact: units, Field: projectionField, Purpose: preset.Purpose, Generated: preset.Generated, Boundary: "unit", LocationField: "provenance", Retrieval: &RetrievalBindings{Exact: &management.ExactRetrieval{Default: !preset.Generated}, FullText: &management.FullTextRetrieval{Index: textName, Default: !preset.Generated}}}
	plan := ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: textName, Config: textConfig}}}
	artifact := ArtifactBinding{Name: preset.Name, Artifact: units, Purpose: preset.Purpose, Generated: preset.Generated, ContentType: contentType, LocationField: "provenance"}
	if preset.VectorSpace != nil {
		if preset.VectorSpace.DocumentModality != "text" {
			return plan, stream, artifact, fmt.Errorf("text processing embeds text members; use a media representation for native image/audio embeddings")
		}
		if err := ValidateVectorSpace(*preset.VectorSpace); err != nil {
			return plan, stream, artifact, err
		}
		if err := ValidateManagedVectorSpace(*preset.VectorSpace, preset.Embedder); err != nil {
			return plan, stream, artifact, err
		}
		all := append(append([]any{}, enrichments...), chunkConfig)
		vectorSource, vectorField := chunks, "text"
		denseConfig := map[string]any{"kind": "embedding", "source_artifact_name": vectorSource, "field": vectorField, "expected_dims": preset.VectorSpace.Dimension, "vector_space": VectorSpaceIdentity(*preset.VectorSpace)}
		dense := NativeIndexName(preset.Name+"_dense", denseConfig)
		denseConfig["name"] = dense
		all = append(all, denseConfig)
		config := map[string]any{"type": "embeddings", "dimension": preset.VectorSpace.Dimension, "distance_metric": preset.VectorSpace.Metric, "embedder": preset.Embedder, "sources": []any{map[string]any{"artifact": dense}}, "enrichments": all}
		name := NativeIndexName(preset.Name+"_vectors", config)
		stream.Retrieval.Semantic = &management.SemanticRetrieval{Index: name, MemberField: "text", MemberArtifacts: []string{dense}, VectorSpace: *preset.VectorSpace, QueryInputs: []management.SemanticRetrievalQueryInputs{"text", "vector"}, Default: !preset.Generated}
		plan.Indexes = append(plan.Indexes, management.NativeIndexDeclaration{Name: name, Config: config})
	}
	return plan, stream, artifact, ValidateProcessing(&plan)
}

func ExternalVectorIndex(name string, space VectorSpace) (management.NativeIndexDeclaration, error) {
	if err := ValidateVectorSpace(space); err != nil {
		return management.NativeIndexDeclaration{}, err
	}
	return management.NativeIndexDeclaration{Name: name, Config: map[string]any{"type": "embeddings", "external": true, "external_vector_space": VectorSpaceIdentity(space), "dimension": space.Dimension, "distance_metric": space.Metric}}, nil
}

// WithExternalVector binds an authored vector to the parent's acquired revision
// and declares its recipe. Native admission independently checks identity/shape.
// The vector and receipt must accompany each authored row together; withdraw
// the receipt when updating a row without authoring its vector. Native omission
// may retain physical bytes, but cannot retain that assertion. It cannot prove
// that an untrusted writer used the asserted model.
func WithExternalVector(parent map[string]any, index string, space VectorSpace, vector []float32) (map[string]any, error) {
	if !validBindingName(index) || len(vector) != space.Dimension {
		return nil, fmt.Errorf("external vector shape mismatch")
	}
	if err := ValidateVectorSpace(space); err != nil {
		return nil, err
	}
	for _, v := range vector {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, fmt.Errorf("external vector is not finite")
		}
	}
	raw, err := json.Marshal(parent)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	source, ok := out["source"].(map[string]any)
	revision, _ := source["revision"].(string)
	if !ok || revision == "" {
		return nil, fmt.Errorf("external embeddings require a revision-bound parent")
	}
	metadata, ok := out["metadata"].(map[string]any)
	if !ok {
		metadata = map[string]any{}
		out["metadata"] = metadata
	}
	spaces, ok := metadata["embedding_spaces"].(map[string]any)
	if !ok {
		spaces = map[string]any{}
		metadata["embedding_spaces"] = spaces
	}
	spaces[index] = VectorSpaceIdentity(space)
	embeddings, ok := out["_embeddings"].(map[string]any)
	if !ok {
		embeddings = map[string]any{}
		out["_embeddings"] = embeddings
	}
	embeddings[index] = vector
	return out, nil
}
