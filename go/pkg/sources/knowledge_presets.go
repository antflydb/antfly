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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// KnowledgePreset builds ordinary native graph/extractor/resolver declarations.
// Source, Producer and Resolver use the existing native schemas. This builder
// adds identities/provenance, not another extraction or matching language.
type KnowledgePreset struct {
	Name, SourceID, Audience, Field, UpstreamArtifact, EntityTable string
	LocationField, SchemaURI                                       string
	Producer                                                       map[string]any
	Source                                                         map[string]any
	Resolver                                                       map[string]any
}

func BuildKnowledgeProcessing(p KnowledgePreset) (ProcessingDeclaration, KnowledgeDeclaration, ArtifactBinding, error) {
	var plan ProcessingDeclaration
	var k KnowledgeDeclaration
	var artifact ArtifactBinding
	if !canonicalID(p.SourceID) || !validBindingName(p.Name) || !validBindingName(p.EntityTable) || !validBindingName(p.Field) || !validBindingName(p.Audience) {
		return plan, k, artifact, fmt.Errorf("knowledge preset requires source, audience, name, input and entity table")
	}
	// Own all nested maps: callers may reuse producer/schema/mapping templates.
	raw, err := json.Marshal(p)
	if err != nil {
		return plan, k, artifact, err
	}
	var owned KnowledgePreset
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&owned); err != nil {
		return plan, k, artifact, err
	}
	p = owned
	namespace := "source/" + p.SourceID + "/" + p.Name + "/"
	config := map[string]any{"type": "graph"}
	asset := map[string]any{"kind": "asset", "content_type": "application/json"}
	if p.UpstreamArtifact != "" {
		asset["source_artifact_name"] = p.UpstreamArtifact
	}
	if strings.Contains(p.Field, ".") {
		asset["template"] = "{{{" + p.Field + "}}}"
	} else {
		asset["field"] = p.Field
	}
	if len(p.Producer) > 0 {
		asset["producer"] = p.Producer
	}
	artifactName := NativeIndexName(p.Name+"_extraction", asset)
	asset["name"] = artifactName

	source := p.Source
	if source == nil {
		source = map[string]any{"path": "$.relations[*]", "format": "extraction_relation"}
	}
	if _, ok := source["format"]; !ok {
		source["format"] = "extraction_relation"
	}
	context, _ := source["context"].(map[string]any)
	if context == nil {
		context = map[string]any{}
		source["context"] = context
	}
	fields, _ := context["doc_fields"].([]any)
	hasSource := false
	for _, f := range fields {
		if f == "source" {
			hasSource = true
		}
	}
	if !hasSource {
		fields = append(fields, "source")
	}
	context["doc_fields"] = fields
	edge, _ := source["edge"].(map[string]any)
	if edge == nil {
		edge = map[string]any{}
		source["edge"] = edge
	}
	if _, ok := edge["edge_id"]; !ok {
		edge["edge_id"] = "{{ _item.id }}"
	}
	metadata, _ := edge["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		edge["metadata"] = metadata
	}
	metadata["source_id"] = "{{ _doc.value.source.id }}"
	metadata["parent_revision"] = "{{ _doc.value.source.revision }}"
	config["sources"] = []any{source}
	resolver := p.Resolver
	if resolver == nil {
		resolver = map[string]any{}
	}
	// Execution identities are immutable; discovery and canonical namespaces
	// remain stable while multiple recipe generations coexist.
	delete(resolver, "name")
	delete(resolver, "source_artifact")
	delete(resolver, "resolution_artifact")
	resolver["candidate_key_prefix"] = namespace
	resolver["table"] = p.EntityTable
	resolver["source_artifact_kind"] = "asset"
	if _, ok := resolver["config_generation"]; !ok {
		resolver["config_generation"] = 1
	}
	resolver["key_template"] = namespace + "{{ slug _entity.label }}/{{ slug _entity.text }}"
	if _, ok := resolver["promote"]; !ok {
		resolver["promote"] = false
	}
	if _, ok := resolver["min_confidence"]; !ok {
		resolver["min_confidence"] = 0
	}
	execution := NativeIndexName(p.Name+"_resolver", map[string]any{"extraction": artifactName, "resolver": resolver})
	// Native label routing is table-scoped by source artifact. A versioned local
	// copy gives each resolver its own input stream without recomputing extraction.
	input := map[string]any{"kind": "asset", "content_type": "application/json", "source_artifact_name": artifactName, "producer": map[string]any{"type": "copy"}}
	inputName := NativeIndexName(execution+"_input", input)
	input["name"] = inputName
	config["enrichments"] = []any{asset, input}
	source["artifact"] = inputName
	resolver["name"] = execution
	resolver["source_artifact"] = inputName
	resolution := NativeIndexName(p.Name+"_resolution", resolver)
	resolver["resolution_artifact"] = resolution
	config["resolvers"] = []any{resolver}
	index := NativeIndexName(p.Name+"_graph", config)
	plan.Indexes = []management.NativeIndexDeclaration{{Name: index, Config: config}}
	k = KnowledgeDeclaration{Version: 1, Mode: "instance_one_audience", Audience: p.Audience, Entities: []EntityBinding{{Name: p.Name, Index: index, Resolver: execution, Table: p.EntityTable, Namespace: namespace, SourceArtifact: inputName, ResolutionArtifact: resolution, LocationField: p.LocationField, SchemaUri: p.SchemaURI}}, Graphs: []GraphBinding{{Name: p.Name, Index: index, Artifacts: []string{inputName}, Entities: []string{p.Name}, Temporal: management.GraphTemporalBinding{Mapping: "native_intervals_v1", History: "current_state"}, LocationField: p.LocationField, SchemaUri: p.SchemaURI}}}
	artifact = ArtifactBinding{Name: p.Name, Artifact: artifactName, ContentType: "application/json", Purpose: "structured_extraction", Generated: len(p.Producer) > 0, LocationField: p.LocationField, SchemaUri: p.SchemaURI}
	if err = ValidateProcessing(&plan); err == nil {
		err = ValidateKnowledgeProcessing(&k, &plan, p.Audience)
	}
	return plan, k, artifact, err
}

// EntityJoinKey encodes an already-authorized canonical identity without lossy
// concatenation. Equal display names or unrelated namespaces never imply identity.
func EntityJoinKey(authority string, e EntityBinding, key string) (string, error) {
	if !validBindingName(authority) || !validBindingName(e.Namespace) || !strings.HasSuffix(e.Namespace, "/") || !validBindingName(e.Table) || !strings.HasPrefix(key, e.Namespace) || len(key) == len(e.Namespace) || !validBindingName(key) {
		return "", fmt.Errorf("entity key is outside its declared namespace")
	}
	raw, err := json.Marshal([]string{authority, e.Table, e.Namespace, key})
	return string(raw), err
}
