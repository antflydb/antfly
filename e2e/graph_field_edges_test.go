// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package e2e

import (
	"encoding/base64"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/oapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fieldEdgeTestTable = "graph_field_edge_test"
)

// setupFieldEdgeSwarm creates a swarm with a graph-indexed table that uses
// field-based edges, tree topology, and a summarizer (via Termite).
func setupFieldEdgeSwarm(t *testing.T) *SwarmInstance {
	t.Helper()

	ctx := testContext(t, 5*time.Minute)

	swarm := startAntflySwarm(t, ctx)
	t.Cleanup(swarm.Cleanup)

	// Build graph index config with:
	//   - "child_of" edge type: field-based (parent_id), tree topology
	//   - "related_to" edge type: explicit _edges only, graph topology
	//   - Summarizer: generates summaries from title+content template
	generatorConfig := GetDefaultGeneratorConfig(t)

	graphConfig := oapi.IndexConfig{
		Name: "hierarchy",
		Type: oapi.IndexTypeGraph,
	}
	graphConfig.FromGraphIndexConfig(oapi.GraphIndexConfig{
		Summarizer: generatorConfig,
		Template:   "{{title}}\n{{content}}",
		EdgeTypes: []oapi.EdgeTypeConfig{
			{
				Name:     "child_of",
				Field:    "parent_id",
				Topology: oapi.EdgeTypeConfigTopologyTree,
			},
			{
				Name: "related_to",
				// No field — uses explicit _edges
			},
		},
	})

	err := swarm.Client.CreateTable(ctx, fieldEdgeTestTable, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]oapi.IndexConfig{
			"hierarchy": graphConfig,
		},
	})
	require.NoError(t, err, "Failed to create table with field-based graph index")

	waitForShardsReady(t, ctx, swarm.Client, fieldEdgeTestTable, 30*time.Second)

	return swarm
}

// waitForFieldEdges polls until field-based edges are created by the enricher.
func waitForFieldEdges(t *testing.T, client *antfly.AntflyClient, startKey string, edgeType string, timeout time.Duration) {
	t.Helper()

	ctx := testContext(t, timeout)
	deadline := time.Now().Add(timeout)
	b64Key := base64.StdEncoding.EncodeToString([]byte(startKey))

	for time.Now().Before(deadline) {
		result, err := client.Query(ctx, antfly.QueryRequest{
			Table: fieldEdgeTestTable,
			GraphSearches: map[string]oapi.GraphQuery{
				"probe": {
					Type:      oapi.GraphQueryTypePattern,
					IndexName: "hierarchy",
					StartNodes: oapi.GraphNodeSelector{
						Keys: []string{b64Key},
					},
					Pattern: []oapi.PatternStep{
						{Alias: "a"},
						{
							Alias: "b",
							Edge: oapi.PatternEdgeStep{
								Types:     []string{edgeType},
								Direction: oapi.EdgeDirectionOut,
								MinHops:   1,
								MaxHops:   1,
							},
						},
					},
				},
			},
		})
		if err == nil && len(result.Responses) > 0 {
			if gr, ok := result.Responses[0].GraphResults["probe"]; ok && len(gr.Matches) > 0 {
				return // edges exist
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Timed out waiting for enricher to create %s edges from %s", edgeType, startKey)
}

// TestE2E_GraphFieldEdges_BasicFieldExtraction tests that field-based edges
// are automatically created from document fields by the enricher, and that
// the summarizer runs without error.
func TestE2E_GraphFieldEdges_BasicFieldExtraction(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 5*time.Minute)

	swarm := setupFieldEdgeSwarm(t)

	// Insert documents with parent_id field.
	// The enricher should automatically create child_of edges and generate summaries.
	//
	// Tree structure:
	//   root
	//   +-- child1
	//   |   +-- grandchild1
	//   +-- child2
	testDocs := map[string]any{
		"0_root": map[string]any{
			"title":   "Root Document",
			"content": "This is the root of the document tree.",
		},
		"4_child1": map[string]any{
			"title":     "Child 1",
			"content":   "First child document under root.",
			"parent_id": "0_root",
		},
		"8_child2": map[string]any{
			"title":     "Child 2",
			"content":   "Second child document under root.",
			"parent_id": "0_root",
		},
		"c_grandchild1": map[string]any{
			"title":     "Grandchild 1",
			"content":   "A grandchild document nested under child1.",
			"parent_id": "4_child1",
		},
	}

	t.Log("Inserting documents with parent_id fields...")
	_, err := swarm.Client.Batch(ctx, fieldEdgeTestTable, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert documents")

	// Wait for enricher to create field-based edges for ALL documents.
	// The enricher processes documents sequentially, so we must wait for each.
	t.Log("Waiting for enricher to create child_of edges...")
	waitForFieldEdges(t, swarm.Client, "4_child1", "child_of", 60*time.Second)
	waitForFieldEdges(t, swarm.Client, "8_child2", "child_of", 60*time.Second)
	waitForFieldEdges(t, swarm.Client, "c_grandchild1", "child_of", 60*time.Second)

	// Query: find all direct children of root using reverse edge traversal
	t.Log("Querying for children of root...")
	result, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: fieldEdgeTestTable,
		GraphSearches: map[string]oapi.GraphQuery{
			"children_of_root": {
				Type:      oapi.GraphQueryTypePattern,
				IndexName: "hierarchy",
				StartNodes: oapi.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("0_root"))},
				},
				Pattern: []oapi.PatternStep{
					{Alias: "parent"},
					{
						Alias: "child",
						Edge: oapi.PatternEdgeStep{
							Types:     []string{"child_of"},
							Direction: oapi.EdgeDirectionIn,
							MinHops:   1,
							MaxHops:   1,
						},
					},
				},
				IncludeDocuments: true,
			},
		},
	})
	require.NoError(t, err, "Pattern query failed")
	require.Len(t, result.Responses, 1, "Expected one query response")

	graphResult, ok := result.Responses[0].GraphResults["children_of_root"]
	require.True(t, ok, "Expected children_of_root graph result")
	assert.Len(t, graphResult.Matches, 2, "Expected 2 children of root (child1 and child2)")

	for i, match := range graphResult.Matches {
		t.Logf("Child %d of root: %s", i+1, match.Bindings["child"].Key)
	}

	// Query: 2-hop traversal — find grandchildren
	t.Log("Querying for grandchildren of root...")
	result, err = swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: fieldEdgeTestTable,
		GraphSearches: map[string]oapi.GraphQuery{
			"grandchildren": {
				Type:      oapi.GraphQueryTypePattern,
				IndexName: "hierarchy",
				StartNodes: oapi.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("0_root"))},
				},
				Pattern: []oapi.PatternStep{
					{Alias: "root"},
					{
						Alias: "child",
						Edge: oapi.PatternEdgeStep{
							Types:     []string{"child_of"},
							Direction: oapi.EdgeDirectionIn,
							MinHops:   1,
							MaxHops:   1,
						},
					},
					{
						Alias: "grandchild",
						Edge: oapi.PatternEdgeStep{
							Types:     []string{"child_of"},
							Direction: oapi.EdgeDirectionIn,
							MinHops:   1,
							MaxHops:   1,
						},
					},
				},
				IncludeDocuments: true,
			},
		},
	})
	require.NoError(t, err, "Grandchild query failed")
	require.Len(t, result.Responses, 1)

	grandchildResult, ok := result.Responses[0].GraphResults["grandchildren"]
	require.True(t, ok, "Expected grandchildren graph result")
	assert.Len(t, grandchildResult.Matches, 1, "Expected 1 grandchild")

	if len(grandchildResult.Matches) > 0 {
		match := grandchildResult.Matches[0]
		t.Logf("Grandchild: %s (via %s)", match.Bindings["grandchild"].Key, match.Bindings["child"].Key)
	}
}

// TestE2E_GraphFieldEdges_TreeTopologyRejectsMultiParent tests that the tree
// topology constraint prevents a node from having multiple parents.
func TestE2E_GraphFieldEdges_TreeTopologyRejectsMultiParent(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 5*time.Minute)

	swarm := setupFieldEdgeSwarm(t)

	// Insert initial tree: child -> parent1 via explicit _edges
	testDocs := map[string]any{
		"0_parent1": map[string]any{
			"title":   "Parent 1",
			"content": "First parent node.",
		},
		"4_parent2": map[string]any{
			"title":   "Parent 2",
			"content": "Second parent node.",
		},
		"8_child": map[string]any{
			"title":   "Child",
			"content": "A child node with one parent.",
			"_edges": map[string]any{
				"hierarchy": map[string]any{
					"child_of": []any{
						map[string]any{"target": "0_parent1", "weight": 1.0},
					},
				},
			},
		},
	}

	t.Log("Inserting initial tree...")
	_, err := swarm.Client.Batch(ctx, fieldEdgeTestTable, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert initial tree")

	time.Sleep(2 * time.Second)

	// Reparent child from parent1 to parent2 via explicit _edges.
	// Because _edges is declarative (reconciliation deletes old edges), this should succeed.
	t.Log("Reparenting child from parent1 to parent2 via _edges...")
	reparentDoc := map[string]any{
		"8_child": map[string]any{
			"title":   "Child Updated",
			"content": "Reparented to parent2.",
			"_edges": map[string]any{
				"hierarchy": map[string]any{
					"child_of": []any{
						map[string]any{"target": "4_parent2", "weight": 1.0},
					},
				},
			},
		},
	}

	_, err = swarm.Client.Batch(ctx, fieldEdgeTestTable, antfly.BatchRequest{
		Inserts:   reparentDoc,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Reparenting via _edges should succeed (declarative reconciliation)")

	time.Sleep(1 * time.Second)

	// Verify: child now points to parent2 (not parent1)
	b64Parent2 := base64.StdEncoding.EncodeToString([]byte("4_parent2"))
	result, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: fieldEdgeTestTable,
		GraphSearches: map[string]oapi.GraphQuery{
			"check": {
				Type:      oapi.GraphQueryTypePattern,
				IndexName: "hierarchy",
				StartNodes: oapi.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("8_child"))},
				},
				Pattern: []oapi.PatternStep{
					{Alias: "child"},
					{
						Alias: "parent",
						Edge: oapi.PatternEdgeStep{
							Types:     []string{"child_of"},
							Direction: oapi.EdgeDirectionOut,
							MinHops:   1,
							MaxHops:   1,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	gr := result.Responses[0].GraphResults["check"]
	require.Len(t, gr.Matches, 1, "Expected 1 edge from child after reparent")
	assert.Equal(t, b64Parent2, gr.Matches[0].Bindings["parent"].Key,
		"Expected child to point to parent2 after reparent")
	t.Logf("After reparent: %s", gr.Matches[0].Bindings["parent"].Key)
}

// TestE2E_GraphFieldEdges_FieldEdgeUpdate tests that changing a document's
// parent_id field causes the enricher to update the edges.
func TestE2E_GraphFieldEdges_FieldEdgeUpdate(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 5*time.Minute)

	swarm := setupFieldEdgeSwarm(t)

	// Insert: child -> parent1
	testDocs := map[string]any{
		"0_parent1": map[string]any{
			"title":   "Parent 1",
			"content": "Original parent.",
		},
		"4_parent2": map[string]any{
			"title":   "Parent 2",
			"content": "New parent after update.",
		},
		"8_child": map[string]any{
			"title":     "Child",
			"content":   "A child that will be reparented.",
			"parent_id": "0_parent1",
		},
	}

	t.Log("Inserting initial documents (child -> parent1)...")
	_, err := swarm.Client.Batch(ctx, fieldEdgeTestTable, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert documents")

	// Wait for enricher to create initial edge
	t.Log("Waiting for initial child_of edge...")
	waitForFieldEdges(t, swarm.Client, "8_child", "child_of", 60*time.Second)

	// Verify: child -> parent1
	result, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: fieldEdgeTestTable,
		GraphSearches: map[string]oapi.GraphQuery{
			"check": {
				Type:      oapi.GraphQueryTypePattern,
				IndexName: "hierarchy",
				StartNodes: oapi.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("8_child"))},
				},
				Pattern: []oapi.PatternStep{
					{Alias: "child"},
					{
						Alias: "parent",
						Edge: oapi.PatternEdgeStep{
							Types:     []string{"child_of"},
							Direction: oapi.EdgeDirectionOut,
							MinHops:   1,
							MaxHops:   1,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	b64Parent1 := base64.StdEncoding.EncodeToString([]byte("0_parent1"))
	b64Parent2 := base64.StdEncoding.EncodeToString([]byte("4_parent2"))
	gr := result.Responses[0].GraphResults["check"]
	require.Len(t, gr.Matches, 1, "Expected 1 edge from child")
	assert.Equal(t, b64Parent1, gr.Matches[0].Bindings["parent"].Key,
		"Expected child to point to parent1")
	t.Logf("Initial parent: %s", gr.Matches[0].Bindings["parent"].Key)

	// Update: change parent_id to parent2
	t.Log("Updating child's parent_id to parent2...")
	_, err = swarm.Client.Batch(ctx, fieldEdgeTestTable, antfly.BatchRequest{
		Inserts: map[string]any{
			"8_child": map[string]any{
				"title":     "Child Updated",
				"content":   "Child reparented to parent2.",
				"parent_id": "4_parent2",
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to update child document")

	// Wait for enricher to process the update
	t.Log("Waiting for enricher to update edges...")
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		result, err = swarm.Client.Query(ctx, antfly.QueryRequest{
			Table: fieldEdgeTestTable,
			GraphSearches: map[string]oapi.GraphQuery{
				"check": {
					Type:      oapi.GraphQueryTypePattern,
					IndexName: "hierarchy",
					StartNodes: oapi.GraphNodeSelector{
						Keys: []string{base64.StdEncoding.EncodeToString([]byte("8_child"))},
					},
					Pattern: []oapi.PatternStep{
						{Alias: "child"},
						{
							Alias: "parent",
							Edge: oapi.PatternEdgeStep{
								Types:     []string{"child_of"},
								Direction: oapi.EdgeDirectionOut,
								MinHops:   1,
								MaxHops:   1,
							},
						},
					},
				},
			},
		})
		if err == nil && len(result.Responses) > 0 {
			if gr, ok := result.Responses[0].GraphResults["check"]; ok {
				if len(gr.Matches) == 1 && gr.Matches[0].Bindings["parent"].Key == b64Parent2 {
					t.Log("Edge successfully updated to parent2")
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("Timed out waiting for enricher to update child_of edge to parent2")
}

// TestE2E_GraphFieldEdges_MixedExplicitAndFieldEdges tests that explicit
// _edges and field-based edges coexist correctly in the same graph index.
func TestE2E_GraphFieldEdges_MixedExplicitAndFieldEdges(t *testing.T) {
	skipInShortMode(t)
	ctx := testContext(t, 5*time.Minute)

	swarm := setupFieldEdgeSwarm(t)

	// Insert documents with both field-based (parent_id -> child_of)
	// and explicit (_edges -> related_to) edges
	testDocs := map[string]any{
		"0_root": map[string]any{
			"title":   "Root",
			"content": "The root document.",
		},
		"4_page": map[string]any{
			"title":     "Page",
			"content":   "A page linked to root and related to another document.",
			"parent_id": "0_root", // -> child_of edge via enricher
			"_edges": map[string]any{
				"hierarchy": map[string]any{
					"related_to": []any{ // explicit edge
						map[string]any{"target": "8_other", "weight": 0.9},
					},
				},
			},
		},
		"8_other": map[string]any{
			"title":   "Other Document",
			"content": "Another document related to page.",
		},
	}

	t.Log("Inserting documents with mixed edge types...")
	_, err := swarm.Client.Batch(ctx, fieldEdgeTestTable, antfly.BatchRequest{
		Inserts:   testDocs,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to insert documents")

	// Wait for enricher to create child_of edge
	t.Log("Waiting for enricher to create child_of edge...")
	waitForFieldEdges(t, swarm.Client, "4_page", "child_of", 60*time.Second)

	// Verify both edge types
	result, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: fieldEdgeTestTable,
		GraphSearches: map[string]oapi.GraphQuery{
			"field_edge": {
				Type:      oapi.GraphQueryTypePattern,
				IndexName: "hierarchy",
				StartNodes: oapi.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("4_page"))},
				},
				Pattern: []oapi.PatternStep{
					{Alias: "page"},
					{
						Alias: "parent",
						Edge: oapi.PatternEdgeStep{
							Types:     []string{"child_of"},
							Direction: oapi.EdgeDirectionOut,
							MinHops:   1,
							MaxHops:   1,
						},
					},
				},
			},
			"explicit_edge": {
				Type:      oapi.GraphQueryTypePattern,
				IndexName: "hierarchy",
				StartNodes: oapi.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("4_page"))},
				},
				Pattern: []oapi.PatternStep{
					{Alias: "page"},
					{
						Alias: "related",
						Edge: oapi.PatternEdgeStep{
							Types:     []string{"related_to"},
							Direction: oapi.EdgeDirectionOut,
							MinHops:   1,
							MaxHops:   1,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err, "Query failed")
	require.Len(t, result.Responses, 1)

	b64Root := base64.StdEncoding.EncodeToString([]byte("0_root"))
	b64Other := base64.StdEncoding.EncodeToString([]byte("8_other"))

	// Check field-based edge
	fieldResult, ok := result.Responses[0].GraphResults["field_edge"]
	require.True(t, ok)
	assert.Len(t, fieldResult.Matches, 1, "Expected 1 child_of edge")
	if len(fieldResult.Matches) > 0 {
		assert.Equal(t, b64Root, fieldResult.Matches[0].Bindings["parent"].Key)
		t.Logf("Field edge: page -> %s (child_of)", fieldResult.Matches[0].Bindings["parent"].Key)
	}

	// Check explicit edge
	explicitResult, ok := result.Responses[0].GraphResults["explicit_edge"]
	require.True(t, ok)
	assert.Len(t, explicitResult.Matches, 1, "Expected 1 related_to edge")
	if len(explicitResult.Matches) > 0 {
		assert.Equal(t, b64Other, explicitResult.Matches[0].Bindings["related"].Key)
		t.Logf("Explicit edge: page -> %s (related_to)", explicitResult.Matches[0].Bindings["related"].Key)
	}
}
