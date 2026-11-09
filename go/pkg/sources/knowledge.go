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
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type KnowledgeDeclaration = management.KnowledgeDeclaration
type EntityBinding = management.EntityBinding
type GraphBinding = management.GraphBinding
type KnowledgeRequest = management.KnowledgeRequest

// ValidateKnowledge validates discovery only. The trusted runtime supplies its
// deployment audience separately; a declaration cannot enable a security mode.
func ValidateKnowledge(k *KnowledgeDeclaration, audience string) error {
	if k == nil {
		return nil
	}
	if k.Version != 1 || k.Mode != "instance_one_audience" || !validBindingName(k.Audience) {
		return fmt.Errorf("invalid knowledge contract")
	}
	if len(k.Entities) > 64 || len(k.Graphs) > 64 {
		return fmt.Errorf("invalid knowledge binding count")
	}
	// An explicit empty declaration clears discovery even after an operator
	// disables the deployment boundary; it never executes native processing.
	if len(k.Entities)+len(k.Graphs) == 0 {
		return nil
	}
	if audience == "" || k.Audience != audience || !validBindingName(audience) {
		return fmt.Errorf("knowledge requires a runtime-enforced single-audience instance")
	}

	entities := map[string]bool{}
	namespaces := map[string]bool{}
	for _, e := range k.Entities {
		for _, v := range []string{e.Name, e.Index, e.Resolver, e.Table, e.Namespace, e.SourceArtifact, e.ResolutionArtifact} {
			if !validBindingName(v) {
				return fmt.Errorf("invalid entity binding")
			}
		}
		if entities[e.Name] || namespaces[e.Table+"\x00"+e.Namespace] || !strings.HasSuffix(e.Namespace, "/") || strings.ContainsAny(e.Namespace, "{}") {
			return fmt.Errorf("entity names and namespaces must be distinct stable prefixes")
		}
		identityPrefix := e.Table + "\x00" + e.Namespace
		for existing := range namespaces {
			if strings.HasPrefix(existing, identityPrefix) || strings.HasPrefix(identityPrefix, existing) {
				return fmt.Errorf("entity namespaces must not overlap within a table")
			}
		}
		entities[e.Name] = true
		namespaces[identityPrefix] = true
	}
	graphs := map[string]bool{}
	for _, g := range k.Graphs {
		if !validBindingName(g.Name) || !validBindingName(g.Index) || graphs[g.Name] || len(g.Artifacts) == 0 || len(g.Artifacts) > 64 || len(g.Entities) > 64 || g.Temporal.Mapping != "native_intervals_v1" || g.Temporal.History != "current_state" {
			return fmt.Errorf("invalid graph binding or unsupported temporal history")
		}
		graphs[g.Name] = true
		seen := map[string]bool{}
		for _, a := range g.Artifacts {
			if !validBindingName(a) || seen[a] {
				return fmt.Errorf("invalid graph artifact")
			}
			seen[a] = true
		}
		seen = map[string]bool{}
		for _, n := range g.Entities {
			if !entities[n] || seen[n] {
				return fmt.Errorf("unknown or duplicate graph entity binding")
			}
			seen[n] = true
			for _, e := range k.Entities {
				if e.Name == n && (e.Index != g.Index || !containsString(g.Artifacts, e.SourceArtifact)) {
					return fmt.Errorf("entity binding does not belong to graph")
				}
			}
		}
	}
	return nil
}
func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// Stable templates are intentionally restricted to native normalized mention
// fields. Candidate IDs, enumeration indices and generated display names are
// never accepted as join identities. Native curation owns redirects and splits.
func checkEntityBinding(e EntityBinding, cfg map[string]any) error {
	values, _ := cfg["resolvers"].([]any)
	for _, v := range values {
		r, ok := v.(map[string]any)
		if !ok || r["name"] != e.Resolver {
			continue
		}
		key, _ := r["key_template"].(string)
		stable := key == e.Namespace+"{{ slug _entity.text }}" || key == e.Namespace+"{{ slug _entity.label }}/{{ slug _entity.text }}"
		if !stable || r["table"] != e.Table || r["source_artifact"] != e.SourceArtifact || r["resolution_artifact"] != e.ResolutionArtifact {
			return fmt.Errorf("resolver identity mismatch or unstable key template")
		}
		// Native scoring breaks equal-score ties by durable table/key identity.
		// Discovery preserves that authority rather than inventing source matching.
		if search, _ := r["candidate_search"].(string); search != "" && search != "exact_key" && search != "prefix" && search != "ann" {
			return fmt.Errorf("unsupported native candidate search")
		}

		aliases, _ := r["aliases"].([]any)
		for _, v := range aliases {
			a, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid alias")
			}
			from, _ := a["key"].(string)
			to, _ := a["target"].(string)
			if !strings.HasPrefix(from, e.Namespace) || !strings.HasPrefix(to, e.Namespace) {
				return fmt.Errorf("cross-namespace aliases require explicit client composition")
			}
		}
		return nil
	}
	return fmt.Errorf("native resolver missing")
}
func checkGraphBinding(g GraphBinding, cfg map[string]any) error {
	if cfg["type"] != "graph" {
		return fmt.Errorf("binding requires a native graph")
	}
	sources, _ := cfg["sources"].([]any)
	if single, ok := cfg["source"].(map[string]any); ok {
		if len(sources) > 0 {
			return fmt.Errorf("graph source forms are mutually exclusive")
		}
		sources = append(sources, single)
	}
	if len(sources) != len(g.Artifacts) {
		return fmt.Errorf("graph source set mismatch")
	}
	seen := map[string]bool{}
	for _, v := range sources {
		s, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid graph source")
		}
		a, _ := s["artifact"].(string)
		if seen[a] || !containsString(g.Artifacts, a) {
			return fmt.Errorf("graph source set mismatch")
		}
		seen[a] = true
		edge, _ := s["edge"].(map[string]any)
		id, ok := edge["edge_id"].(string)
		if !ok || strings.TrimSpace(id) == "" || strings.Contains(id, "_item_index") || strings.Contains(id, "@index") {
			return fmt.Errorf("relationship requires stable member identity independent of array order")
		}
		metadata, _ := edge["metadata"].(map[string]any)
		if metadata["source_id"] != "{{ _doc.value.source.id }}" || metadata["parent_revision"] != "{{ _doc.value.source.revision }}" {
			return fmt.Errorf("graph source requires revision-bound parent provenance")
		}
		context, _ := s["context"].(map[string]any)
		fields, _ := context["doc_fields"].([]any)
		hasSource := false
		for _, f := range fields {
			if f == "source" {
				hasSource = true
			}
		}
		if !hasSource {
			return fmt.Errorf("graph provenance requires source context")
		}
	}
	return nil
}

// ValidateKnowledgeProcessing checks the complete desired graph before any DDL.
// Actual native admission remains authoritative for extractor/resolver schemas.
func ValidateKnowledgeProcessing(k *KnowledgeDeclaration, plan *ProcessingDeclaration, audience string) error {
	if err := ValidateKnowledge(k, audience); err != nil {
		return err
	}
	// Graph processing is subject to the same runtime boundary even when a
	// caller omits discovery. Otherwise raw native declarations could bypass
	// hosted entity-table authorization and enable mixed-audience promotion.
	if plan != nil {
		raw, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		var canonical ProcessingDeclaration
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err = decoder.Decode(&canonical); err != nil {
			return err
		}
		plan = &canonical
	}
	if plan != nil {
		for _, idx := range plan.Indexes {
			resolvers, _ := idx.Config["resolvers"].([]any)
			if idx.Config["type"] != "graph" && len(resolvers) == 0 {
				continue
			}
			if k == nil || len(k.Entities)+len(k.Graphs) == 0 {
				return fmt.Errorf("source graph processing requires enabled knowledge bindings")
			}
			for _, value := range resolvers {
				r, ok := value.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid native resolver")
				}
				declared := false
				for _, e := range k.Entities {
					if e.Index == idx.Name && e.Resolver == r["name"] {
						if err := checkEntityBinding(e, idx.Config); err != nil {
							return err
						}
						declared = true
					}
				}
				if !declared {
					return fmt.Errorf("every processing resolver requires an authorized entity binding")
				}
			}
		}
	}
	if k == nil {
		return nil
	}
	// Omitted processing may reference independently administered native resources.
	// Installed readback is still verified before readiness or query admission.
	if plan == nil {
		return nil
	}
	configs := map[string]map[string]any{}
	if plan != nil {
		for _, idx := range plan.Indexes {
			configs[idx.Name] = idx.Config
		}
	}
	for _, g := range k.Graphs {
		if err := checkGraphBinding(g, configs[g.Index]); err != nil {
			return fmt.Errorf("graph %s: %w", g.Name, err)
		}
	}
	for _, e := range k.Entities {
		cfg := configs[e.Index]
		if cfg["type"] != "graph" {
			return fmt.Errorf("entity resolver requires its declared native graph")
		}
		if err := checkEntityBinding(e, cfg); err != nil {
			return fmt.Errorf("entity %s: %w", e.Name, err)
		}
	}
	return nil
}

// ObserveKnowledge uses the same bounded, cached index observations as search.
// Catalog presence is never treated as extraction or resolution completion.
func ObserveKnowledge(ctx context.Context, client SearchIndexClient, b Binding, audience string) management.KnowledgeStatus {
	_, status := ObserveSource(ctx, client, b, audience)
	return status
}

// ObserveSource shares one bounded metadata snapshot across both discovery
// families. Native index readback is fetched once per distinct index.
func ObserveSource(ctx context.Context, client SearchIndexClient, b Binding, audience string) (management.SearchStatus, management.KnowledgeStatus) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata := b
	if ValidateKnowledge(b.Knowledge, audience) != nil {
		metadata.Knowledge = nil
	}
	cache, failures := observeSearchIndexes(ctx, client, metadata)
	return searchStatusFromIndexes(b, cache, failures), knowledgeStatusFromIndexes(b, audience, cache, failures)
}
func knowledgeStatusFromIndexes(b Binding, audience string, cache map[string]nativeSearchIndex, failures map[string]error) management.KnowledgeStatus {
	out := management.KnowledgeStatus{Version: 1, ObservedAt: int(time.Now().Unix()), Mode: "unsupported", Entities: []management.RepresentationStatus{}, Graphs: []management.RepresentationStatus{}}
	if b.Knowledge == nil {
		return out
	}
	err := ValidateKnowledge(b.Knowledge, audience)
	if err == nil {
		out.Mode = string(b.Knowledge.Mode)
		out.Audience = audience
	}
	if len(b.Knowledge.Entities)+len(b.Knowledge.Graphs) == 0 {
		out.Mode = "disabled"
		return out
	}
	state := func(name, index string, check func(map[string]any) error) management.RepresentationStatus {
		s := management.RepresentationStatus{Name: name, State: "unsupported", Reason: "knowledge_deployment_boundary_unavailable"}
		if err != nil {
			return s
		}
		idx, ok := cache[index]
		if !ok {
			s.Reason = "native_observation_unavailable"
			failure, found := failures[index]
			if !found || errors.Is(failure, errSearchMetadataBudget) {
				s.Reason = "native_metadata_budget_exceeded"
			}
			if errors.Is(failure, context.DeadlineExceeded) || errors.Is(failure, context.Canceled) {
				s.Reason = "native_observation_timeout"
			}
			return s
		}
		if check(idx.Config) != nil {
			s.Reason = "native_binding_mismatch"
			return s
		}
		s.State = "building"
		s.Reason = "native_index_incomplete"
		r := idx.Status.Readiness
		if r.State == "failed" {
			s.State = "failed"
			s.Reason = "native_index_failed"
		} else if r.Complete && r.Queryable {
			s.State = "ready"
			s.Reason = ""
		} else if r.State == "queryable_partial" {
			s.State = "partial"
			s.Reason = "native_index_partial"
		}
		return s
	}
	for _, e := range b.Knowledge.Entities {
		out.Entities = append(out.Entities, state(e.Name, e.Index, func(c map[string]any) error { return checkEntityBinding(e, c) }))
	}
	for _, g := range b.Knowledge.Graphs {
		out.Graphs = append(out.Graphs, state(g.Name, g.Index, func(c map[string]any) error { return checkGraphBinding(g, c) }))
	}
	return out
}

// BuildKnowledgeQuery composes ordinary native traversal. It never hydrates
// arbitrary entity documents or executes curation. Hosts authorize every call.
func BuildKnowledgeQuery(b Binding, request KnowledgeRequest, audience string) ([]byte, error) {
	if b.Knowledge == nil {
		return nil, fmt.Errorf("knowledge is not declared")
	}
	if err := ValidateKnowledge(b.Knowledge, audience); err != nil {
		return nil, err
	}
	var graph *GraphBinding
	for i := range b.Knowledge.Graphs {
		if b.Knowledge.Graphs[i].Name == request.Graph {
			graph = &b.Knowledge.Graphs[i]
			break
		}
	}
	if graph == nil {
		return nil, fmt.Errorf("unknown graph")
	}
	if request.Depth == 0 {
		request.Depth = 1
	}
	if request.Limit == 0 {
		request.Limit = 20
	}
	if request.Depth < 1 || request.Depth > 8 || request.Limit < 1 || request.Limit > 100 || len(request.Start) < 1 || len(request.Start) > 16 {
		return nil, fmt.Errorf("graph request exceeds work bounds")
	}
	seen := map[string]bool{}
	for _, key := range request.Start {
		if !validBindingName(key) || seen[key] {
			return nil, fmt.Errorf("invalid start keys")
		}
		seen[key] = true
	}
	if !canonicalID(b.ID) {
		return nil, fmt.Errorf("knowledge traversal requires a source identity")
	}
	start := map[string]any{"keys": request.Start}
	if request.Entity != "" {
		if !containsString(graph.Entities, request.Entity) {
			return nil, fmt.Errorf("entity is not declared by graph")
		}
		for _, e := range b.Knowledge.Entities {
			if e.Name == request.Entity {
				identities := []any{}
				for _, key := range request.Start {
					if !strings.HasPrefix(key, e.Namespace) || key == e.Namespace {
						return nil, fmt.Errorf("start key is outside entity namespace")
					}
					identities = append(identities, map[string]any{"table": e.Table, "key": key})
				}
				start = map[string]any{"identities": identities}
			}
		}
	}
	filter := map[string]any{"properties": []any{map[string]any{"field": "/metadata/source_id", "op": "eq", "value": b.ID}}}
	for field, v := range map[string]string{"valid_at": request.ValidAt, "known_at": request.KnownAt} {
		if v != "" {
			if len(v) > 64 {
				return nil, fmt.Errorf("invalid graph timestamp")
			}
			if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
				return nil, fmt.Errorf("invalid graph timestamp")
			}
			filter[field] = v
		}
	}
	return json.Marshal(map[string]any{"limit": 0, "graph_queries": map[string]any{"source_knowledge": map[string]any{"index": graph.Index, "traverse": map[string]any{"start": start, "max_depth": request.Depth, "limit": request.Limit, "include_paths": true, "deduplicate_nodes": false, "include_documents": false, "edge_filter": filter}}}})
}

// QueryKnowledge requires an operator-provided audience. That is a deployment
// assertion, not a claim that row ACLs can secure a mixed-audience graph. The
// native/gateway policy still authorizes current access, including historical queries.
type KnowledgeQueryClient interface {
	SearchIndexClient
	Query(context.Context, string, []byte) ([]byte, error)
}

func (p *Publisher) QueryKnowledge(ctx context.Context, b Binding, request KnowledgeRequest, audience string) ([]byte, error) {
	return QueryKnowledge(ctx, p, b, request, audience)
}

func QueryKnowledge(ctx context.Context, p KnowledgeQueryClient, b Binding, request KnowledgeRequest, audience string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if b.Status != "searchable" || b.ValidUntil == nil || time.Now().Unix() >= *b.ValidUntil {
		return nil, fmt.Errorf("source access is not searchable")
	}
	query, err := BuildKnowledgeQuery(b, request, audience)
	if err != nil {
		return nil, err
	}
	ctx = scopedSearchContext(ctx, b)
	var selected GraphBinding
	for _, g := range b.Knowledge.Graphs {
		if g.Name == request.Graph {
			selected = g
		}
	}
	observe := func() ([32]byte, error) {
		raw, err := p.InspectSearchIndex(ctx, b.Table, selected.Index)
		if err != nil {
			return [32]byte{}, err
		}
		if len(raw) > 8<<20 {
			return [32]byte{}, fmt.Errorf("native graph metadata exceeds budget")
		}
		idx, err := decodeSearchIndex(raw)
		if err != nil {
			return [32]byte{}, err
		}
		if err = checkGraphBinding(selected, idx.Config); err != nil {
			return [32]byte{}, err
		}
		for _, e := range b.Knowledge.Entities {
			if containsString(selected.Entities, e.Name) {
				if err = checkEntityBinding(e, idx.Config); err != nil {
					return [32]byte{}, err
				}
			}
		}
		if !idx.Status.Readiness.Complete || !idx.Status.Readiness.Queryable {
			return [32]byte{}, fmt.Errorf("native graph is incomplete")
		}
		// Preserve native incarnation and exact u64 generation/revision values.
		// The compatibility decoder intentionally ignores some status fields;
		// hashing its reduced view would miss a same-config index replacement.
		var wire struct {
			Config json.RawMessage `json:"config"`
			Status struct {
				Readiness json.RawMessage `json:"readiness"`
			} `json:"status"`
		}
		if err = json.Unmarshal(raw, &wire); err != nil {
			return [32]byte{}, err
		}
		encoded, err := json.Marshal(wire)

		return sha256.Sum256(encoded), err
	}
	before, err := observe()
	if err != nil {
		return nil, err
	}
	raw, err := p.Query(ctx, b.Table, query)
	if err != nil {
		return nil, err
	}
	if len(raw) > management.MaxResponseBytes {
		return nil, fmt.Errorf("knowledge response exceeds budget")
	}
	after, err := observe()
	if err != nil {
		return nil, err
	}
	if before != after || time.Now().Unix() >= *b.ValidUntil {
		return nil, fmt.Errorf("knowledge generation or access changed during query")
	}
	consistency, err := SearchConsistency(raw)
	if err != nil {
		return nil, err
	}
	if request.RequireSnapshot && consistency != "retained_snapshot" {
		return nil, fmt.Errorf("knowledge query requires a retained snapshot")
	}
	var required struct {
		Responses []struct {
			GraphResults map[string]json.RawMessage `json:"graph_results"`
		} `json:"responses"`
	}
	if err = json.Unmarshal(raw, &required); err != nil {
		return nil, err
	}
	if len(required.Responses) != 1 || len(required.Responses[0].GraphResults["source_knowledge"]) == 0 || string(required.Responses[0].GraphResults["source_knowledge"]) == "null" {
		return nil, fmt.Errorf("native graph result unavailable")
	}
	var response map[string]json.RawMessage
	if err = json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	response["source_knowledge"], err = json.Marshal(map[string]any{"source": b.ID, "graph": request.Graph, "audience": audience, "consistency": consistency, "history": "current_state", "generated": true})
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(response)
	if len(encoded) > management.MaxResponseBytes {
		return nil, fmt.Errorf("knowledge response exceeds budget")
	}
	return encoded, err
}

func knowledgeVersions(audience string) []int {
	if audience != "" {
		return []int{1}
	}
	return nil
}

// EntityResources references the existing native authority. These relative
// paths carry no credentials or access grants; clients authorize native reads
// and curation independently. Redirect revisions/suppression remain native.
func EntityResources(b Binding, name string) (map[string]string, error) {
	if b.Knowledge == nil || !validBindingName(b.Table) {
		return nil, fmt.Errorf("unknown entity binding")
	}
	for _, e := range b.Knowledge.Entities {
		if e.Name == name {
			base := tablePath(b.Table) + "/resolvers/" + url.PathEscape(e.Resolver)
			return map[string]string{"resolver": base, "render": base + ":render", "resolve": base + ":resolve", "redirects": base + "/redirects", "suppressions": base + "/suppressions", "reviews": base + "/reviews", "entity_table": tablePath(e.Table)}, nil
		}
	}
	return nil, fmt.Errorf("unknown entity binding")
}
