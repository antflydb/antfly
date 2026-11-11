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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"

	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func knowledgeFixture(t *testing.T) (ProcessingDeclaration, KnowledgeDeclaration) {
	return knowledgeFixtureForTable(t, "entities", "$")
}
func knowledgeFixtureForTable(t *testing.T, table, path string) (ProcessingDeclaration, KnowledgeDeclaration) {
	t.Helper()
	plan, k, _, err := BuildKnowledgeProcessing(KnowledgePreset{Name: "facts", SourceID: testSourceID, Audience: "research", Field: "facts", EntityTable: table, Source: map[string]any{"path": path, "format": "extraction_relation", "nodes": map[string]any{"source": "{{ _item.source }}", "target": "{{ _item.target }}"}, "edge": map[string]any{"edge_id": "{{ _item.id }}", "type": "RELATES_TO", "metadata": map[string]any{"valid_at": "{{ _item.valid_at }}", "invalid_at": "{{ _item.invalid_at }}", "created_at": "{{ _item.created_at }}", "expired_at": "{{ _item.expired_at }}"}}}})
	require.NoError(t, err)
	return plan, k
}
func TestKnowledgeAdmissionAndIdentity(t *testing.T) {
	plan, k := knowledgeFixture(t)
	require.Error(t, ValidateKnowledge(&k, ""))
	require.Error(t, ValidateKnowledge(&k, "another-audience"))
	require.NoError(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
	require.Error(t, ValidateKnowledgeProcessing(nil, &plan, "research"))
	entity := k.Entities[0]
	collision := entity
	collision.Name = "overlapping"
	collision.Namespace += "nested/"
	k.Entities = append(k.Entities, collision)
	require.Error(t, ValidateKnowledge(&k, "research"))
	k.Entities = k.Entities[:1]
	key := entity.Namespace + "person/alice"
	join, err := EntityJoinKey("instance-one", entity, key)
	require.NoError(t, err)
	other := entity
	other.Namespace = "another/"
	_, err = EntityJoinKey("instance-one", other, key)
	require.Error(t, err)
	other = entity
	other.Table = "another-table"
	otherJoin, err := EntityJoinKey("instance-one", other, key)
	require.NoError(t, err)
	require.NotEqual(t, join, otherJoin)
	anotherAuthority, err := EntityJoinKey("instance-two", entity, key)
	require.NoError(t, err)
	require.NotEqual(t, join, anotherAuthority)
	resolver := plan.Indexes[0].Config["resolvers"].([]any)[0].(map[string]any)
	for _, bad := range []string{"{{ _candidate.id }}", "{{ @index }}", entity.Namespace + "{{ _entity.id }}"} {
		resolver["key_template"] = bad
		require.Error(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
	}
	resolver["key_template"] = entity.Namespace + "{{ slug _entity.label }}/{{ slug _entity.text }}"
	resolver["candidate_search"] = "invalid"
	require.Error(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
	resolver["candidate_search"] = "exact_key"
	resolver["aliases"] = []any{map[string]any{"key": key, "target": "other/private"}}
	require.Error(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
	delete(resolver, "aliases")
	source := plan.Indexes[0].Config["sources"].([]any)[0].(map[string]any)
	edge := source["edge"].(map[string]any)
	for _, id := range []any{nil, "", "{{ _item_index }}", "{{ @index }}"} {
		edge["edge_id"] = id
		require.Error(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
	}
	edge["edge_id"] = "{{ _item.id }}"
	require.NoError(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
	delete(edge["metadata"].(map[string]any), "source_id")
	require.Error(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
}
func TestKnowledgeQueryScopeTimeAndBudgets(t *testing.T) {
	_, k := knowledgeFixture(t)
	b := Binding{ID: testSourceID, Knowledge: &k}
	raw, err := BuildKnowledgeQuery(b, KnowledgeRequest{Graph: "facts", Start: []string{"alice"}, ValidAt: "2020-01-01T00:00:00Z", KnownAt: "2021-01-01T00:00:00Z"}, "research")
	require.NoError(t, err)
	var query map[string]any
	require.NoError(t, json.Unmarshal(raw, &query))
	traverse := query["graph_queries"].(map[string]any)["source_knowledge"].(map[string]any)["traverse"].(map[string]any)
	require.Equal(t, false, traverse["include_documents"])
	filter := traverse["edge_filter"].(map[string]any)
	require.Equal(t, "2020-01-01T00:00:00Z", filter["valid_at"])
	require.Equal(t, testSourceID, filter["properties"].([]any)[0].(map[string]any)["value"])
	raw, err = BuildKnowledgeQuery(b, KnowledgeRequest{Graph: "facts", Entity: "facts", Start: []string{k.Entities[0].Namespace + "person/alice"}}, "research")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &query))
	identities := query["graph_queries"].(map[string]any)["source_knowledge"].(map[string]any)["traverse"].(map[string]any)["start"].(map[string]any)["identities"].([]any)
	require.Equal(t, k.Entities[0].Table, identities[0].(map[string]any)["table"])

	for _, r := range []KnowledgeRequest{{Graph: "unknown", Start: []string{"a"}}, {Graph: "facts"}, {Graph: "facts", Start: []string{"a", "a"}}, {Graph: "facts", Start: []string{"a"}, Depth: 9}, {Graph: "facts", Start: []string{"a"}, Limit: 101}, {Graph: "facts", Start: []string{"a"}, KnownAt: "yesterday"}} {
		_, err = BuildKnowledgeQuery(b, r, "research")
		require.Error(t, err)
	}
	b.ID = ""
	_, err = BuildKnowledgeQuery(b, KnowledgeRequest{Graph: "facts", Start: []string{"a"}}, "research")
	require.Error(t, err)
}
func TestKnowledgeReadinessAndNoAcquisitionIdentityChange(t *testing.T) {
	plan, k := knowledgeFixture(t)
	client := &knowledgeClient{config: plan.Indexes[0].Config, complete: true}
	b := Binding{ID: testSourceID, Table: "docs", Knowledge: &k}
	status := ObserveKnowledge(t.Context(), client, b, "research")
	require.EqualValues(t, "ready", status.Graphs[0].State)
	require.EqualValues(t, "ready", status.Entities[0].State)
	require.Equal(t, 1, client.inspections)
	client.complete = false
	require.EqualValues(t, "building", ObserveKnowledge(t.Context(), client, b, "research").Graphs[0].State)
	client.complete = true
	client.config["resolvers"].([]any)[0].(map[string]any)["key_template"] = "unstable/{{@index}}"
	require.EqualValues(t, "unsupported", ObserveKnowledge(t.Context(), client, b, "research").Entities[0].State)
	connection := Connection{Provider: ProviderLocal}
	source := Source{ID: testSourceID, Table: "docs"}
	before, err := sourceConfigurationHash(source, connection)
	require.NoError(t, err)
	source.Knowledge = &k
	source.Processing = &plan
	after, err := sourceConfigurationHash(source, connection)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

type knowledgeClient struct {
	config        map[string]any
	complete      bool
	inspections   int
	response      string
	generation    uint64
	extraResolver map[string]any
	activePrefix  *string
	onQuery       func()
}

func (c *knowledgeClient) InspectSearchIndex(context.Context, string, string) ([]byte, error) {
	c.inspections++
	resolvers, _ := c.config["resolvers"].([]any)
	if c.extraResolver != nil {
		resolvers = append(append([]any{}, resolvers...), c.extraResolver)
	}
	if c.activePrefix != nil {
		observed := make([]any, 0, len(resolvers))
		for _, value := range resolvers {
			copy := map[string]any{}
			for key, field := range value.(map[string]any) {
				copy[key] = field
			}
			copy["candidate_key_prefix"] = *c.activePrefix
			observed = append(observed, copy)
		}
		resolvers = observed
	}
	return json.Marshal(map[string]any{"config": c.config, "status": map[string]any{"resolver_replay": map[string]any{"resolvers": resolvers}, "readiness": map[string]any{"complete": c.complete, "queryable": c.complete, "incarnation": "knowledge-incarnation", "target_revision": c.generation, "published_revision": c.generation}}})
}
func (c *knowledgeClient) CreateSearchIndex(context.Context, string, string, map[string]any) error {
	return fmt.Errorf("unexpected DDL")
}
func (c *knowledgeClient) Query(context.Context, string, []byte) ([]byte, error) {
	if c.onQuery != nil {
		c.onQuery()
	}
	return []byte(c.response), nil
}
func TestKnowledgeQueryRevalidatesAndPreservesNumbers(t *testing.T) {
	plan, k := knowledgeFixture(t)
	expires := time.Now().Add(time.Minute).Unix()
	b := Binding{ID: testSourceID, Table: "docs", Knowledge: &k, Status: "searchable", ValidUntil: &expires}
	c := &knowledgeClient{config: plan.Indexes[0].Config, complete: true, response: `{"responses":[{"graph_results":{"source_knowledge":{"nodes":[],"large":18446744073709551615}}}],"live_read":true}`}
	raw, err := QueryKnowledge(t.Context(), c, b, KnowledgeRequest{Graph: "facts", Start: []string{"a"}}, "research")
	require.NoError(t, err)
	require.Contains(t, string(raw), "18446744073709551615")
	require.Contains(t, string(raw), "source_knowledge")
	_, err = QueryKnowledge(t.Context(), c, b, KnowledgeRequest{Graph: "facts", Start: []string{"a"}, RequireSnapshot: true}, "research")
	require.Error(t, err)
	c.generation = 1 << 53
	c.onQuery = func() { c.generation++ }
	_, err = QueryKnowledge(t.Context(), c, b, KnowledgeRequest{Graph: "facts", Start: []string{"a"}}, "research")
	require.ErrorContains(t, err, "generation")
	c.onQuery = nil
	c.complete = false
	_, err = QueryKnowledge(t.Context(), c, b, KnowledgeRequest{Graph: "facts", Start: []string{"a"}}, "research")
	require.Error(t, err)
}

func TestKnowledgeLegacyUpdatePreservesAndExplicitClear(t *testing.T) {
	s, _, _ := serviceFixture(t)
	_, k := knowledgeFixture(t)
	s.mu.Lock()
	s.knowledgeAudience = "research"
	s.profile.Sources[0].Knowledge = &k
	source := cloneProfile(s.profile).Sources[0]
	s.mu.Unlock()
	source.Knowledge = nil // An older client does not understand this field.
	response := serviceCall(s, "PUT", "/v1/sources/"+source.ID, source)
	require.Equal(t, 200, response.Code, response.Body.String())
	s.mu.Lock()
	require.Equal(t, &k, s.profile.Sources[0].Knowledge)
	s.knowledgeAudience = "" // Operators may disable the boundary before clearing it.
	s.mu.Unlock()
	source.Knowledge = &KnowledgeDeclaration{Version: 1, Mode: "instance_one_audience", Audience: "research"}
	response = serviceCall(s, "PUT", "/v1/sources/"+source.ID, source)
	require.Equal(t, 200, response.Code, response.Body.String())
	s.mu.Lock()
	require.Empty(t, s.profile.Sources[0].Knowledge.Graphs)
	s.mu.Unlock()
}

// Measures bounded request construction separately from native traversal or
// model inference; no corpus work belongs in the discovery client.
func BenchmarkBuildKnowledgeQuery(b *testing.B) {
	_, k, _, err := BuildKnowledgeProcessing(KnowledgePreset{Name: "facts", SourceID: testSourceID, Audience: "research", Field: "facts", EntityTable: "entities"})
	if err != nil {
		b.Fatal(err)
	}
	binding := Binding{ID: testSourceID, Table: "documents", Knowledge: &k}
	request := KnowledgeRequest{Graph: "facts", Entity: "facts", Start: []string{k.Entities[0].Namespace + "person/alice"}, Depth: 8, Limit: 100}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := BuildKnowledgeQuery(binding, request, "research"); err != nil {
			b.Fatal(err)
		}
	}
}

func TestLocalKnowledgeQueryRejectsConcurrentPublicationRetirement(t *testing.T) {
	s, _, _ := serviceFixture(t)
	plan, k := knowledgeFixture(t)
	until := time.Now().Add(time.Minute).Unix()
	binding := Binding{ID: testSourceID, Table: "docs", Knowledge: &k, Status: "searchable", ValidUntil: &until}
	catalogPath := filepath.Join(s.runnerPath(), "catalog.json")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/query") {
			require.NoError(t, writeJSON(catalogPath, Catalog{Version: 2, Sources: []Binding{}}, 32<<20))
			_, _ = w.Write([]byte(`{"responses":[{"graph_results":{"source_knowledge":{"nodes":[]}}}],"live_read":true}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"config": plan.Indexes[0].Config, "status": map[string]any{"resolver_replay": map[string]any{"resolvers": plan.Indexes[0].Config["resolvers"]}, "readiness": map[string]any{"complete": true, "queryable": true}}})
	}))
	defer server.Close()
	s.mu.Lock()
	s.knowledgeAudience = "research"
	s.profile.Endpoint = server.URL
	s.profile.Sources[0].Table = "docs"
	s.profile.Sources[0].Knowledge = &k
	s.mu.Unlock()
	snapshot, err := (localManagement{s}).knowledgeSnapshot(testSourceID)
	require.NoError(t, err)
	binding.AcquisitionID = snapshot.acquisition
	require.NoError(t, writeJSON(catalogPath, Catalog{Version: 2, Endpoint: server.URL, Sources: []Binding{binding}}, 32<<20))
	_, err = (localManagement{s}).QueryKnowledge(t.Context(), testSourceID, KnowledgeRequest{Graph: "facts", Start: []string{"alice"}})
	require.ErrorContains(t, err, "knowledge_changed_during_query")
}

func TestEntityOnlyDiscoveryRequiresActiveNativeScope(t *testing.T) {
	plan, k := knowledgeFixture(t)
	k.Graphs = nil
	client := &knowledgeClient{config: plan.Indexes[0].Config, complete: true}
	binding := Binding{Table: "docs", Knowledge: &k}
	require.NoError(t, ValidateInstalledKnowledge(t.Context(), client, binding, "research"))
	require.EqualValues(t, "ready", ObserveKnowledge(t.Context(), client, binding, "research").Entities[0].State)
	unscoped := ""
	client.activePrefix = &unscoped
	require.Error(t, ValidateInstalledKnowledge(t.Context(), client, binding, "research"))
	require.EqualValues(t, "unsupported", ObserveKnowledge(t.Context(), client, binding, "research").Entities[0].State)
}
