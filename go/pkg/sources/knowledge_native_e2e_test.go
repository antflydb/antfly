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
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Runs native asset publication, resolution, graph projection, temporal filters
// and owner retraction. Synthetic facts qualify integration, not extraction quality.
func TestNativeSourceKnowledge(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCES_E2E_URL")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCES_E2E_URL to a disposable native owner")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	p := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("source_knowledge_%d", time.Now().UnixNano())
	require.NoError(t, p.EnsureTable(ctx, table))
	entityTable := table + "_entities"
	plan, k := knowledgeFixtureForTable(t, entityTable, "$.relations[*]")
	require.NoError(t, p.EnsureTable(ctx, entityTable))
	// Both claims use logical entity endpoints independent of parent ownership.
	// Native resolution also reads stable mentions from this same structured asset.
	require.NoError(t, ReconcileProcessing(ctx, p, table, &plan))
	resources, err := EntityResources(Binding{Table: table, Knowledge: &k}, "facts")
	require.NoError(t, err)
	for _, id := range []string{"local-a", "local-b"} {
		rendered, err := p.call(ctx, "POST", resources["render"], map[string]any{"text": "Alice", "label": "person", "local_id": id})
		require.NoError(t, err)
		var r map[string]any
		require.NoError(t, json.Unmarshal(rendered, &r))
		require.Equal(t, k.Entities[0].Namespace+"person/alice", r["minted_key"])
	}
	until := time.Now().Add(5 * time.Minute).Unix()
	b := Binding{ID: testSourceID, Table: table, Knowledge: &k, Status: "searchable", ValidUntil: &until}
	fact := func(id, sourceID, revision string) map[string]any {
		return map[string]any{"source_id": sourceID, "doc_type": "source_item", "source": map[string]any{"id": sourceID, "revision": revision, "uri": "local:///" + id}, "permissions": map[string]any{"public": true}, "facts": map[string]any{"entities": []any{map[string]any{"id": "a", "label": "person", "text": "Alice"}}, "relations": []any{map[string]any{"id": id, "source": "alice", "target": "acme", "valid_at": "2020-01-01T00:00:00Z", "invalid_at": "2030-01-01T00:00:00Z", "created_at": "2021-01-01T00:00:00Z", "expired_at": "2031-01-01T00:00:00Z"}}}}
	}
	otherID := "22222222-2222-4222-8222-222222222222"
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"claim-a": fact("a", testSourceID, "r1"), "claim-b": fact("b", testSourceID, "r1"), "other-source": fact("other", otherID, "r1")}, "sync_level": "write"})
	require.NoError(t, err)
	request := KnowledgeRequest{Graph: "facts", Start: []string{"alice"}, Depth: 1, Limit: 10, ValidAt: "2022-01-01T00:00:00Z", KnownAt: "2022-01-01T00:00:00Z"}
	var raw []byte
	defer func() {
		if t.Failed() {
			t.Logf("final status: %+v; response=%s", ObserveKnowledge(ctx, p, b, "research"), raw)
			unqualified := request
			unqualified.Entity = ""
			diagnostic, diagnosticErr := p.QueryKnowledge(ctx, b, unqualified, "research")
			t.Logf("unqualified diagnostic: error=%v response=%s", diagnosticErr, diagnostic)
		}
	}()
	require.Eventually(t, func() bool {
		var err error
		raw, err = p.QueryKnowledge(ctx, b, request, "research")
		return err == nil && strings.Contains(string(raw), "claim-a") && strings.Contains(string(raw), "claim-b")
	}, 60*time.Second, 100*time.Millisecond)
	require.NotContains(t, string(raw), "other-source")
	request.ValidAt = "2040-01-01T00:00:00Z"
	raw, err = p.QueryKnowledge(ctx, b, request, "research")
	require.NoError(t, err)
	require.NotContains(t, string(raw), "claim-a")
	request.ValidAt = "2022-01-01T00:00:00Z"
	for _, known := range []string{"2020-01-01T00:00:00Z", "2031-01-01T00:00:00Z"} {
		request.KnownAt = known
		raw, err = p.QueryKnowledge(ctx, b, request, "research")
		require.NoError(t, err)
		require.NotContains(t, string(raw), "claim-a")
	}
	request.KnownAt = "2022-01-01T00:00:00Z"
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"deletes": []string{"claim-a"}, "sync_level": "write"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var err error
		raw, err = p.QueryKnowledge(ctx, b, request, "research")
		return err == nil && !strings.Contains(string(raw), "claim-a") && strings.Contains(string(raw), "claim-b")
	}, 30*time.Second, 100*time.Millisecond)
	// Empty extraction retires the remaining owner's relationships without
	// deleting the unrelated source's claim. Rebuilds remain native, source sync
	// is never used as the processing executor.
	empty := fact("b", testSourceID, "r2")
	empty["facts"] = map[string]any{"entities": []any{}, "relations": []any{}}
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"claim-b": empty}, "sync_level": "write"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var err error
		raw, err = p.QueryKnowledge(ctx, b, request, "research")
		return err == nil && !strings.Contains(string(raw), "claim-b")
	}, 30*time.Second, 100*time.Millisecond)
	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Contains(t, wire, "source_knowledge")
	b.ID = otherID
	raw, err = p.QueryKnowledge(ctx, b, request, "research")
	require.NoError(t, err)
	require.Contains(t, string(raw), "other-source")
	// Alias/merge/unmerge remain the existing revisioned native authority.
	from := k.Entities[0].Namespace + "person/alice"
	to := k.Entities[0].Namespace + "person/alicia"
	_, err = p.call(ctx, "POST", resources["redirects"], map[string]any{"from": from, "to": to, "actor": "source-knowledge-fixture", "reason": "qualify explicit reversible composition"})
	require.NoError(t, err)
	rendered, err := p.call(ctx, "POST", resources["render"], map[string]any{"text": "Alice", "label": "person"})
	require.NoError(t, err)
	require.Contains(t, string(rendered), to)
	_, err = p.call(ctx, "DELETE", resources["redirects"]+"/"+url.PathEscape(from), map[string]any{"actor": "source-knowledge-fixture", "reason": "restore independent key"})
	require.NoError(t, err)
	rendered, err = p.call(ctx, "POST", resources["render"], map[string]any{"text": "Alice", "label": "person"})
	require.NoError(t, err)
	var restored map[string]any
	require.NoError(t, json.Unmarshal(rendered, &restored))
	require.Equal(t, from, restored["key"])

	// The ordinary resolver-backed shape has no mention-edge projection and no
	// endpoint string templates. Its first full-index write must re-render the
	// graph when resolution lands, without a second parent update or rebuild.
	resolvedPlan, resolvedKnowledge, _, err := BuildKnowledgeProcessing(KnowledgePreset{Name: "resolved", SourceID: testSourceID, Audience: "research", Field: "resolved_facts", EntityTable: entityTable, Source: map[string]any{"path": "$.relations[*]", "edge": map[string]any{"edge_id": "{{ _item.id }}", "metadata": map[string]any{"created_at": "{{ _item.created_at }}", "valid_at": "{{ _item.valid_at }}"}}}})
	require.NoError(t, err)
	require.NoError(t, ReconcileProcessing(ctx, p, table, &resolvedPlan))
	parent := map[string]any{"source_id": testSourceID, "doc_type": "source_item", "source": map[string]any{"id": testSourceID, "revision": "resolved-r1"}, "permissions": map[string]any{"public": true}, "resolved_facts": map[string]any{"entities": []any{map[string]any{"id": "e0", "text": "Alice", "label": "person"}, map[string]any{"id": "e1", "text": "Acme", "label": "organization"}}, "relations": []any{map[string]any{"id": "employment", "type": "WORKS_AT", "source": "e0", "target": "e1", "valid_at": "2020-01-01T00:00:00Z", "created_at": "2021-01-01T00:00:00Z"}}}}
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"resolved-claim": parent}, "sync_level": "full_index"})
	require.NoError(t, err)
	resolvedBinding := Binding{ID: testSourceID, Table: table, Knowledge: &resolvedKnowledge, Status: "searchable", ValidUntil: &until}
	resolvedRequest := KnowledgeRequest{Graph: "resolved", Entity: "resolved", Start: []string{resolvedKnowledge.Entities[0].Namespace + "person/alice"}, ValidAt: "2022-01-01T00:00:00Z", KnownAt: "2022-01-01T00:00:00Z"}
	b, request = resolvedBinding, resolvedRequest // Failure diagnostics follow the active branch.
	require.Eventually(t, func() bool {
		var err error
		raw, err = p.QueryKnowledge(ctx, resolvedBinding, resolvedRequest, "research")
		return err == nil && strings.Contains(string(raw), "employment") && strings.Contains(string(raw), "resolved-claim") && strings.Contains(string(raw), resolvedKnowledge.Entities[0].Namespace+"organization/acme")
	}, 60*time.Second, 100*time.Millisecond)
	resolvedResources, err := EntityResources(resolvedBinding, "resolved")
	require.NoError(t, err)
	canonical := resolvedRequest.Start[0]
	_, err = p.call(ctx, "POST", resolvedResources["suppressions"], map[string]any{"keys": []string{canonical}, "actor": "source-knowledge-fixture", "reason": "withdraw resolved evidence"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		raw, err = p.QueryKnowledge(ctx, resolvedBinding, resolvedRequest, "research")
		return err == nil && !strings.Contains(string(raw), "employment")
	}, 30*time.Second, 100*time.Millisecond)
	_, err = p.call(ctx, "DELETE", resolvedResources["suppressions"]+"/"+url.PathEscape(canonical), map[string]any{"actor": "source-knowledge-fixture", "reason": "restore admitted evidence"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		raw, err = p.QueryKnowledge(ctx, resolvedBinding, resolvedRequest, "research")
		return err == nil && strings.Contains(string(raw), "employment")
	}, 30*time.Second, 100*time.Millisecond)
}

// A recipe migration must coexist with the serving index on a real sharded
// owner. Labeled resolvers exercise native table-wide label-routing admission.
func TestNativeSourceKnowledgeRecipeMigration(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCES_E2E_URL")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCES_E2E_URL to a disposable native owner")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	p := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("knowledge_migration_%d", time.Now().UnixNano())
	entities := table + "_entities"
	for _, name := range []string{table, entities} {
		_, err = p.call(ctx, "POST", tablePath(name), map[string]any{"num_shards": 2})
		require.NoError(t, err)
	}
	preset := KnowledgePreset{Name: "facts", SourceID: testSourceID, Audience: "research", Field: "facts", EntityTable: entities, Resolver: map[string]any{"labels": []string{"person", "organization"}}}
	plans := []ProcessingDeclaration{}
	bindings := []Binding{}
	until := time.Now().Add(5 * time.Minute).Unix()
	parent := func(source string) map[string]any {
		facts := func(org string) map[string]any {
			return map[string]any{"entities": []any{map[string]any{"id": "p", "label": "person", "text": "Alice"}, map[string]any{"id": "o", "label": "organization", "text": org}}, "relations": []any{map[string]any{"id": "employment", "source": "p", "target": "o", "type": "WORKS_AT"}}}
		}
		return map[string]any{"source_id": source, "source": map[string]any{"id": source, "revision": "r1"}, "facts": facts("Acme"), "facts_v2": facts("Contoso")}
	}
	query := KnowledgeRequest{Graph: "facts", Entity: "facts", Start: []string{"source/" + testSourceID + "/facts/person/alice"}, Depth: 2, Limit: 10}
	for version := 0; version < 3; version++ {
		if version == 1 {
			preset.Field = "facts_v2"
		}
		if version == 2 {
			preset.Resolver["config_generation"] = 2
		}
		plan, k, _, err := BuildKnowledgeProcessing(preset)
		require.NoError(t, err)
		require.NoError(t, ReconcileSourceSearch(ctx, p, Source{ID: testSourceID, Table: table, Knowledge: &k, Processing: &plan}, "research"))
		plans = append(plans, plan)
		bindings = append(bindings, Binding{ID: testSourceID, Table: table, Knowledge: &k, Status: "searchable", ValidUntil: &until})
		if version == 0 {
			_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"a-owner": parent(testSourceID), "z-owner": parent(testSourceID), "other-owner": parent("22222222-2222-4222-8222-222222222222")}, "sync_level": "full_index"})
			require.NoError(t, err)
		}
		for i, binding := range bindings {
			want := "organization/acme"
			if i > 0 {
				want = "organization/contoso"
			}
			require.Eventually(t, func() bool {
				raw, err := QueryKnowledge(ctx, p, binding, query, "research")
				return err == nil && strings.Contains(string(raw), want) && strings.Contains(string(raw), "a-owner") && strings.Contains(string(raw), "z-owner") && !strings.Contains(string(raw), "other-owner")
			}, 45*time.Second, 100*time.Millisecond, "version %d serving generation %d", version, i)
		}
	}
	// Scorer-only migrations consume the same extraction artifact but an
	// independent local resolver stream; no model/provider work is repeated.
	inputs := func(plan ProcessingDeclaration) []any { return plan.Indexes[0].Config["enrichments"].([]any) }
	require.Equal(t, inputs(plans[1])[0], inputs(plans[2])[0])
	require.NotEqual(t, inputs(plans[1])[1], inputs(plans[2])[1])
	last, err := EntityResources(bindings[2], "facts")
	require.NoError(t, err)
	from := query.Start[0]
	to := strings.TrimSuffix(from, "alice") + "alicia"
	shared, err := EntityCurationResources(bindings[0], "facts", from)
	require.NoError(t, err)
	_, err = p.call(ctx, "POST", shared["merge"], map[string]any{"into": to, "actor": "migration-fixture", "reason": "stable namespace across recipes"})
	require.NoError(t, err)
	rendered, err := p.call(ctx, "POST", last["render"], map[string]any{"text": "Alice", "label": "person"})
	require.NoError(t, err)
	require.Contains(t, string(rendered), to)
	if path := os.Getenv("ANTFLY_SOURCES_KNOWLEDGE_RESTART_STATE"); path != "" {
		query.Start = []string{to}
		for _, b := range bindings {
			require.Eventually(t, func() bool {
				raw, err := QueryKnowledge(ctx, p, b, query, "research")
				return err == nil && strings.Contains(string(raw), "a-owner") && strings.Contains(string(raw), "z-owner")
			}, 30*time.Second, 100*time.Millisecond)
		}
		require.NoError(t, writeJSON(path, knowledgeMigrationRestartState{Bindings: bindings, Request: query}, 2<<20))
	}
}

type knowledgeMigrationRestartState struct {
	Bindings []Binding        `json:"bindings"`
	Request  KnowledgeRequest `json:"request"`
}

func TestNativeSourceKnowledgeAfterRestart(t *testing.T) {
	endpoint, path := os.Getenv("ANTFLY_SOURCES_E2E_URL"), os.Getenv("ANTFLY_SOURCES_KNOWLEDGE_RESTART_STATE")
	if endpoint == "" || path == "" {
		t.Skip("requires the disposable-owner restart harness")
	}
	var state knowledgeMigrationRestartState
	require.NoError(t, loadJSON(path, &state, 2<<20))
	require.Len(t, state.Bindings, 3)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	p := &Publisher{Endpoint: endpoint}
	verify := func(absent string) {
		for _, b := range state.Bindings {
			require.Eventually(t, func() bool {
				raw, err := QueryKnowledge(ctx, p, b, state.Request, "research")
				return err == nil && strings.Contains(string(raw), "z-owner") && (absent == "" && strings.Contains(string(raw), "a-owner") || absent != "" && !strings.Contains(string(raw), absent))
			}, 30*time.Second, 100*time.Millisecond)
		}
	}
	verify("")
	_, err := p.call(ctx, "POST", tablePath(state.Bindings[0].Table)+"/batch", map[string]any{"deletes": []string{"a-owner"}, "sync_level": "full_index"})
	require.NoError(t, err)
	verify("a-owner")
}
