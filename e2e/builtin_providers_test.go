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
	"context"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// TestE2E_BuiltinProviders tests the full pipeline using only built-in Antfly
// providers (embedder, reranker, fixed chunker) with no external ML service.
func TestE2E_BuiltinProviders(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Start swarm without Termite — all providers are embedded in the binary.
	t.Log("Starting Antfly swarm (no Termite)...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	defer swarm.Cleanup()

	tableName := "builtin_providers_test"

	// --- Create table with antfly embedder + fixed chunker ---

	embedderConfig := &antfly.EmbedderConfig{}
	embedderConfig.Provider = antfly.EmbedderProviderAntfly
	err := embedderConfig.FromAntflyEmbedderConfig(antfly.AntflyEmbedderConfig{})
	require.NoError(t, err)

	chunker := antfly.ChunkerConfig{}
	err = chunker.FromAntflyChunkerConfig(antfly.AntflyChunkerConfig{
		Text: antfly.TextChunkOptions{
			TargetTokens:  256,
			OverlapTokens: 25,
		},
	})
	require.NoError(t, err)

	embeddingIndexConfig := antfly.IndexConfig{
		Name: "embeddings",
		Type: "aknn_v0",
	}
	err = embeddingIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *embedderConfig,
		Chunker:  chunker,
	})
	require.NoError(t, err)

	err = swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"embeddings": embeddingIndexConfig,
		},
	})
	require.NoError(t, err, "Failed to create table")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)
	t.Log("Table created with antfly embedder + fixed chunker")

	// --- Insert documents ---

	docs := map[string]any{
		"doc1": map[string]any{
			"title":   "Machine Learning Basics",
			"content": "Machine learning is a subset of artificial intelligence that builds systems which learn from data. Supervised learning uses labeled examples to train models that can make predictions on new data.",
		},
		"doc2": map[string]any{
			"title":   "Weather Report",
			"content": "Today's weather forecast calls for sunny skies with temperatures reaching 75 degrees. There is a slight chance of afternoon showers in the mountain areas.",
		},
		"doc3": map[string]any{
			"title":   "Deep Learning and Neural Networks",
			"content": "Deep learning uses neural networks with multiple layers to learn hierarchical representations. Convolutional neural networks are particularly effective for image recognition tasks.",
		},
		"doc4": map[string]any{
			"title":   "Cooking Pasta",
			"content": "To cook pasta properly, bring a large pot of salted water to a rolling boil. Add the pasta and stir occasionally. Cook until al dente according to package directions.",
		},
		"doc5": map[string]any{
			"title":   "Reinforcement Learning",
			"content": "Reinforcement learning trains agents through trial and error with reward signals. The agent learns a policy that maximizes cumulative reward over time.",
		},
	}

	_, err = swarm.Client.Batch(ctx, tableName, antfly.BatchRequest{
		Inserts:   docs,
		SyncLevel: antfly.SyncLevelAknn,
	})
	require.NoError(t, err, "Failed to insert documents")
	t.Logf("Inserted %d documents", len(docs))

	// --- Wait for embeddings ---

	waitForEmbeddings(t, ctx, swarm.Client, tableName, "embeddings", len(docs), 3*time.Minute)

	// --- Semantic search without reranker ---

	results, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "What is machine learning?",
		Indexes:        []string{"embeddings"},
		Limit:          5,
	})
	require.NoError(t, err, "Semantic search failed")
	require.NotEmpty(t, results.Responses)
	hitsNoReranker := results.Responses[0].Hits.Hits
	require.NotEmpty(t, hitsNoReranker, "Expected search results without reranker")
	t.Logf("Search without reranker returned %d hits", len(hitsNoReranker))
	for i, hit := range hitsNoReranker {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// --- Semantic search with antfly reranker ---

	rerankerConfig := &antfly.RerankerConfig{}
	rerankerConfig.Provider = antfly.RerankerProviderAntfly
	err = rerankerConfig.FromAntflyRerankerConfig(antfly.AntflyRerankerConfig{})
	require.NoError(t, err)
	rerankerConfig.Field = "content"

	results, err = swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "What is machine learning?",
		Indexes:        []string{"embeddings"},
		Limit:          5,
		Reranker:       rerankerConfig,
	})
	require.NoError(t, err, "Semantic search with reranker failed")
	require.NotEmpty(t, results.Responses)
	hitsReranked := results.Responses[0].Hits.Hits
	require.NotEmpty(t, hitsReranked, "Expected search results with reranker")
	t.Logf("Search with antfly reranker returned %d hits", len(hitsReranked))
	for i, hit := range hitsReranked {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// Verify ML-related documents appear in top results.
	topIDs := make([]string, 0, len(hitsReranked))
	for _, hit := range hitsReranked {
		topIDs = append(topIDs, hit.ID)
	}
	mlDocIDs := map[string]bool{"doc1": true, "doc3": true, "doc5": true}
	foundML := 0
	for _, id := range topIDs[:min(3, len(topIDs))] {
		if mlDocIDs[id] {
			foundML++
		}
	}
	require.GreaterOrEqual(t, foundML, 2,
		"Expected at least 2 of the 3 ML documents in top-3 results, got %d (top IDs: %v)", foundML, topIDs)

	t.Log("Builtin providers e2e test passed")
}
