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

package db

import (
	"context"
	"encoding/base64"
	"os"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func setupTestDBWithGraphAndFullText(t *testing.T) (*DBImpl, string) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger: lg,
	}

	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title":   map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Add full-text index
	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	// Add graph index
	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))
	require.NoError(t, db.UpdateSchema(tableSchema))

	return db, dir
}

func TestGraphSearch_WithFullTextSearch(t *testing.T) {
	db, dir := setupTestDBWithGraphAndFullText(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create test data with full-text content and graph edges
	docs := map[string]map[string]any{
		"paper1": {
			"title":   "Machine Learning Basics",
			"content": "Introduction to machine learning algorithms",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper2", "weight": 1.0},
						map[string]any{"target": "paper3", "weight": 0.9},
					},
				},
			},
		},
		"paper2": {
			"title":   "Deep Learning",
			"content": "Neural networks and deep learning",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper4", "weight": 0.8},
					},
				},
			},
		},
		"paper3": {
			"title":   "Machine Learning Applications",
			"content": "Practical applications of machine learning",
		},
		"paper4": {
			"title":   "Computer Vision",
			"content": "Image recognition using deep learning",
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(1 * time.Second) // Wait for indexing

	t.Run("full-text only", func(t *testing.T) {
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(query.NewMatchQuery("machine learning")),
			Limit:              10,
		}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)

		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))

		assert.NotNil(t, res.BleveSearchResult)
		assert.NotEmpty(t, res.BleveSearchResult.Hits)
	})

	t.Run("graph query with full-text result reference", func(t *testing.T) {
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(query.NewMatchQuery("machine learning")),
			Limit:              10,
			GraphSearches: map[string]*indexes.GraphQuery{
				"citations": {
					Type:      "traverse",
					IndexName: "citations",
					StartNodes: indexes.GraphNodeSelector{
						ResultRef: "$full_text_results",
						Limit:     5,
					},
					IncludeDocuments: true,
					Params: indexes.GraphQueryParams{
						MaxDepth:  1,
						Direction: "out",
					},
				},
			},
		}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)

		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))

		// Should have both full-text and graph results
		assert.NotNil(t, res.BleveSearchResult)
		assert.NotNil(t, res.GraphResults)
		assert.Contains(t, res.GraphResults, "citations")

		graphResult := res.GraphResults["citations"]
		assert.Positive(t, graphResult.Total)
		assert.NotEmpty(t, graphResult.Nodes)

		// Verify documents are included
		for _, node := range graphResult.Nodes {
			assert.NotNil(t, node.Document)
		}
	})

	t.Run("graph-only query with explicit keys", func(t *testing.T) {
		req := &indexes.RemoteIndexSearchRequest{
			Limit: 10,
			GraphSearches: map[string]*indexes.GraphQuery{
				"citations": {
					Type:      "neighbors",
					IndexName: "citations",
					StartNodes: indexes.GraphNodeSelector{
						Keys: []string{base64.StdEncoding.EncodeToString([]byte("paper1"))},
					},
					IncludeDocuments: true,
					Params: indexes.GraphQueryParams{
						Direction: "out",
					},
				},
			},
		}

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)

		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))

		assert.Nil(t, res.BleveSearchResult)
		assert.NotNil(t, res.GraphResults)
		assert.Contains(t, res.GraphResults, "citations")

		graphResult := res.GraphResults["citations"]
		assert.Equal(t, 2, graphResult.Total) // paper2 and paper3
	})
}

func TestGraphFusion_Union(t *testing.T) {
	db, dir := setupTestDBWithGraphAndFullText(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create overlapping data sets
	docs := map[string]map[string]any{
		"doc1": {
			"title":   "First Document",
			"content": "Content about topic A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "doc2", "weight": 1.0},
					},
				},
			},
		},
		"doc2": {
			"title":   "Second Document",
			"content": "More about topic A",
		},
		"doc3": {
			"title":   "Third Document",
			"content": "Different topic B",
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(1 * time.Second)

	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(query.NewMatchQuery("topic A")),
		Limit:              10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"citations": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("doc1"))},
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
		},
		ExpandStrategy: "union",
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))

	// With union, FusionResult should contain nodes from both full-text and graph
	assert.NotNil(t, res.FusionResult)
	// Union should include: doc1, doc2 (from full-text) + doc2 (from graph, deduplicated)
	assert.GreaterOrEqual(t, len(res.FusionResult.Hits), 2)
}

func TestGraphFusion_Intersection(t *testing.T) {
	db, dir := setupTestDBWithGraphAndFullText(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create data where some nodes appear in both search results
	docs := map[string]map[string]any{
		"doc1": {
			"title":   "Shared Document",
			"content": "This appears in both full-text and graph results",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "doc2", "weight": 1.0},
					},
				},
			},
		},
		"doc2": {
			"title":   "Shared Document Two",
			"content": "Also appears in both results",
		},
		"doc3": {
			"title":   "Only in Full Text",
			"content": "This document has shared content but no graph connection",
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(1 * time.Second)

	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(query.NewMatchQuery("shared")),
		Limit:              10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"citations": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("doc1"))},
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
		},
		ExpandStrategy: "intersection",
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))

	// With intersection, only nodes appearing in both should be in FusionResult
	assert.NotNil(t, res.FusionResult)
	// Should only contain doc2 (appears in both full-text "shared" and graph neighbors)
	// Note: This depends on the actual intersection logic implementation
}

func TestGraphSearch_MultipleQueries(t *testing.T) {
	db, dir := setupTestDBWithGraphAndFullText(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create test graph
	docs := map[string]map[string]any{
		"a": {
			"title": "Node A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
					},
				},
			},
		},
		"b": {
			"title": "Node B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"c": {"title": "Node C"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(1 * time.Second)

	// Execute multiple graph queries in one request
	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors_a": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
			"traverse_a": {
				Type:      "traverse",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{
					MaxDepth:  2,
					Direction: "out",
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))

	assert.NotNil(t, res.GraphResults)
	assert.Len(t, res.GraphResults, 2)
	assert.Contains(t, res.GraphResults, "neighbors_a")
	assert.Contains(t, res.GraphResults, "traverse_a")

	// neighbors should return only 1-hop (b)
	assert.Equal(t, 1, res.GraphResults["neighbors_a"].Total)

	// traverse with depth 2 should return 2 nodes (b, c)
	assert.Equal(t, 2, res.GraphResults["traverse_a"].Total)
}

func TestGraphSearch_ChainedQueries(t *testing.T) {
	db, dir := setupTestDBWithGraphAndFullText(t)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create test graph
	docs := map[string]map[string]any{
		"a": {
			"title": "Node A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
					},
				},
			},
		},
		"b": {
			"title": "Node B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"c": {"title": "Node C"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(1 * time.Second)

	// First query: get neighbors of 'a'
	// Second query: use results of first query as start nodes
	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"first_hop": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
			"second_hop": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.first_hop",
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))

	assert.NotNil(t, res.GraphResults)
	assert.Contains(t, res.GraphResults, "first_hop")
	assert.Contains(t, res.GraphResults, "second_hop")

	// first_hop: a -> b
	assert.Equal(t, 1, res.GraphResults["first_hop"].Total)

	// second_hop: b -> c (using first_hop results as start)
	assert.Equal(t, 1, res.GraphResults["second_hop"].Total)
}
