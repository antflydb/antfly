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
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

const spladeModel = "naver/splade-cocondenser-ensembledistil"

// sparseModelAvailable checks if the SPLADE model has been exported to the
// models directory. The model is large (~530 MB) and only available after
// running the export script.
func sparseModelAvailable() bool {
	// Walk up from this source file to find the repo root.
	// The e2e directory has its own go.mod, so we look for the models/
	// directory directly rather than stopping at the first go.mod.
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return false
	}
	dir := filepath.Dir(filename)
	for {
		modelPath := filepath.Join(dir, "models", "embedders", spladeModel, "model.onnx")
		if _, err := os.Stat(modelPath); err == nil {
			return true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return false
		}
		dir = parent
	}
}

// TestE2E_Sparse tests the sparse (SPLADE) embedding pipeline end-to-end:
// document ingestion → async sparse enrichment → sparse vector search.
func TestE2E_Sparse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}
	if !sparseModelAvailable() {
		t.Skip("Skipping: SPLADE model not available (export naver/splade-cocondenser-ensembledistil first)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Start swarm with Termite so the SPLADE model is loaded.
	t.Log("Starting Antfly swarm with Termite...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{})
	defer swarm.Cleanup()

	termiteURL := GetTermiteURL()
	require.NotEmpty(t, termiteURL, "Termite URL should be set after swarm start")

	t.Run("SparseSearch", func(t *testing.T) {
		testSparseSearch(t, ctx, swarm, termiteURL)
	})

	t.Run("HybridSearch", func(t *testing.T) {
		testHybridSearch(t, ctx, swarm, termiteURL)
	})

	t.Run("SparseImport", func(t *testing.T) {
		testSparseImport(t, ctx, swarm, termiteURL)
	})
}

// testSparseSearch creates a table with a sparse_v0 index, inserts documents,
// waits for SPLADE enrichment, and verifies that sparse search returns
// semantically relevant results.
func testSparseSearch(t *testing.T, ctx context.Context, swarm *SwarmInstance, termiteURL string) {
	t.Helper()

	tableName := "sparse_search_test"

	// -- Create embedder config pointing at the SPLADE model in Termite --
	embedderConfig := antfly.EmbedderConfig{}
	embedderConfig.Provider = antfly.EmbedderProviderTermite
	err := embedderConfig.FromTermiteEmbedderConfig(antfly.TermiteEmbedderConfig{
		Model:  spladeModel,
		ApiUrl: termiteURL,
	})
	require.NoError(t, err)

	// -- Create sparse index config --
	sparseIndexConfig := antfly.IndexConfig{
		Name: "sparse",
		Type: antfly.IndexTypeEmbeddings,
	}
	err = sparseIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Sparse:   true,
		Field:    "content",
		Embedder: embedderConfig,
	})
	require.NoError(t, err)

	// -- Create table --
	err = swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"sparse": sparseIndexConfig,
		},
	})
	require.NoError(t, err, "Failed to create table with sparse index")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)
	t.Log("Table created with sparse_v0 index")

	// -- Insert documents --
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

	// -- Wait for sparse enrichment --
	waitForSparseEmbeddings(t, ctx, swarm.Client, tableName, "sparse", len(docs), 5*time.Minute)

	// -- Sparse search --
	results, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "What is machine learning?",
		Indexes:        []string{"sparse"},
		Limit:          5,
	})
	require.NoError(t, err, "Sparse search failed")
	require.NotEmpty(t, results.Responses)

	hits := results.Responses[0].Hits.Hits
	require.NotEmpty(t, hits, "Expected sparse search results")
	t.Logf("Sparse search returned %d hits", len(hits))
	for i, hit := range hits {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// Verify ML-related documents appear in top results.
	topIDs := make([]string, 0, len(hits))
	for _, hit := range hits {
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
		"Expected at least 2 of the 3 ML documents in top-3 sparse results, got %d (top IDs: %v)", foundML, topIDs)
}

// testHybridSearch creates a table with both dense (aknn_v0) and sparse
// (sparse_v0) indexes to verify three-way hybrid search (BM25 + dense + sparse)
// with RRF fusion.
func testHybridSearch(t *testing.T, ctx context.Context, swarm *SwarmInstance, termiteURL string) {
	t.Helper()

	tableName := "hybrid_search_test"

	// -- Dense embedder (built-in antfly embedder) --
	denseEmbedder := &antfly.EmbedderConfig{}
	denseEmbedder.Provider = antfly.EmbedderProviderAntfly
	err := denseEmbedder.FromAntflyEmbedderConfig(antfly.AntflyEmbedderConfig{})
	require.NoError(t, err)

	chunker := antfly.ChunkerConfig{}
	err = chunker.FromAntflyChunkerConfig(antfly.AntflyChunkerConfig{
		Text: antfly.TextChunkOptions{
			TargetTokens:  256,
			OverlapTokens: 25,
		},
	})
	require.NoError(t, err)

	denseIndexConfig := antfly.IndexConfig{
		Name: "dense",
		Type: antfly.IndexTypeEmbeddings,
	}
	err = denseIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Field:    "content",
		Embedder: *denseEmbedder,
		Chunker:  chunker,
	})
	require.NoError(t, err)

	// -- Sparse embedder (Termite SPLADE) --
	sparseEmbedder := antfly.EmbedderConfig{}
	sparseEmbedder.Provider = antfly.EmbedderProviderTermite
	err = sparseEmbedder.FromTermiteEmbedderConfig(antfly.TermiteEmbedderConfig{
		Model:  spladeModel,
		ApiUrl: termiteURL,
	})
	require.NoError(t, err)

	sparseIndexConfig := antfly.IndexConfig{
		Name: "sparse",
		Type: antfly.IndexTypeEmbeddings,
	}
	err = sparseIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Sparse:   true,
		Field:    "content",
		Embedder: sparseEmbedder,
	})
	require.NoError(t, err)

	// -- Create table with both indexes --
	err = swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"dense":  denseIndexConfig,
			"sparse": sparseIndexConfig,
		},
	})
	require.NoError(t, err, "Failed to create table with hybrid indexes")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)
	t.Log("Table created with dense + sparse indexes")

	// -- Insert documents --
	docs := map[string]any{
		"doc1": map[string]any{
			"title":   "Database Indexing",
			"content": "B-tree indexes provide efficient ordered lookups. Hash indexes are faster for equality queries but don't support range scans. Inverted indexes power full-text search engines.",
		},
		"doc2": map[string]any{
			"title":   "Garden Care",
			"content": "Water your garden early in the morning to reduce evaporation. Mulch around plants to retain moisture and suppress weeds. Prune dead branches in late winter.",
		},
		"doc3": map[string]any{
			"title":   "Vector Search Engines",
			"content": "Vector search uses approximate nearest neighbor algorithms like HNSW to find similar embeddings. Hybrid search combines vector similarity with traditional keyword matching for better relevance.",
		},
		"doc4": map[string]any{
			"title":   "Search Engine Architecture",
			"content": "Modern search engines combine multiple retrieval stages: sparse lexical matching with BM25, dense semantic vectors, and learned sparse representations like SPLADE for comprehensive recall.",
		},
	}

	_, err = swarm.Client.Batch(ctx, tableName, antfly.BatchRequest{
		Inserts:   docs,
		SyncLevel: antfly.SyncLevelAknn,
	})
	require.NoError(t, err, "Failed to insert documents")
	t.Logf("Inserted %d documents", len(docs))

	// -- Wait for both dense and sparse enrichment --
	waitForEmbeddings(t, ctx, swarm.Client, tableName, "dense", len(docs), 5*time.Minute)
	waitForSparseEmbeddings(t, ctx, swarm.Client, tableName, "sparse", len(docs), 5*time.Minute)

	// -- Hybrid search (dense + sparse via RRF) --
	results, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "How do search engines combine different retrieval methods?",
		Indexes:        []string{"dense", "sparse"},
		Limit:          4,
	})
	require.NoError(t, err, "Hybrid search failed")
	require.NotEmpty(t, results.Responses)

	hits := results.Responses[0].Hits.Hits
	require.NotEmpty(t, hits, "Expected hybrid search results")
	t.Logf("Hybrid search returned %d hits", len(hits))
	for i, hit := range hits {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// Verify search-related documents outrank the garden doc.
	searchDocIDs := map[string]bool{"doc1": true, "doc3": true, "doc4": true}
	foundSearch := 0
	topN := min(3, len(hits))
	for _, hit := range hits[:topN] {
		if searchDocIDs[hit.ID] {
			foundSearch++
		}
	}
	require.GreaterOrEqual(t, foundSearch, 2,
		"Expected at least 2 search-related docs in top-3 hybrid results, got %d", foundSearch)
}

// testSparseImport tests importing pre-computed sparse embeddings via the
// _embeddings field, bypassing the SPLADE enrichment pipeline.
func testSparseImport(t *testing.T, ctx context.Context, swarm *SwarmInstance, termiteURL string) {
	t.Helper()

	tableName := "sparse_import_test"

	// -- Create sparse index (no embedder needed for import-only) --
	sparseEmbedder := antfly.EmbedderConfig{}
	sparseEmbedder.Provider = antfly.EmbedderProviderTermite
	err := sparseEmbedder.FromTermiteEmbedderConfig(antfly.TermiteEmbedderConfig{
		Model:  spladeModel,
		ApiUrl: termiteURL,
	})
	require.NoError(t, err)

	sparseIndexConfig := antfly.IndexConfig{
		Name: "sparse",
		Type: antfly.IndexTypeEmbeddings,
	}
	err = sparseIndexConfig.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Sparse:   true,
		Field:    "content",
		Embedder: sparseEmbedder,
	})
	require.NoError(t, err)

	err = swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"sparse": sparseIndexConfig,
		},
	})
	require.NoError(t, err, "Failed to create table for sparse import test")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)
	t.Log("Table created for sparse import test")

	// -- Insert documents with pre-computed sparse embeddings --
	// Sparse embeddings are maps of string(term_id) → weight.
	// We craft synthetic vectors that share terms to test dot-product scoring.
	docs := map[string]any{
		"doc1": map[string]any{
			"title":   "Alpha",
			"content": "First document about alpha topic",
			"_embeddings": map[string]any{
				"sparse": map[string]any{
					"10": 2.5, "20": 1.0, "30": 0.5,
				},
			},
		},
		"doc2": map[string]any{
			"title":   "Beta",
			"content": "Second document about beta topic",
			"_embeddings": map[string]any{
				"sparse": map[string]any{
					"10": 0.1, "40": 3.0, "50": 1.5,
				},
			},
		},
		"doc3": map[string]any{
			"title":   "Gamma",
			"content": "Third document about gamma topic",
			"_embeddings": map[string]any{
				"sparse": map[string]any{
					"10": 1.8, "20": 0.8, "60": 2.0,
				},
			},
		},
	}

	_, err = swarm.Client.Batch(ctx, tableName, antfly.BatchRequest{
		Inserts:   docs,
		SyncLevel: antfly.SyncLevelAknn,
	})
	require.NoError(t, err, "Failed to insert documents with pre-computed sparse embeddings")
	t.Logf("Inserted %d documents with pre-computed sparse embeddings", len(docs))

	// Wait for the imported sparse embeddings to be indexed.
	waitForSparseEmbeddings(t, ctx, swarm.Client, tableName, "sparse", len(docs), 3*time.Minute)

	// -- Verify search works with imported embeddings --
	// Use a text query (goes through SPLADE) to search against imported vectors.
	results, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "alpha topic first",
		Indexes:        []string{"sparse"},
		Limit:          3,
	})
	require.NoError(t, err, "Sparse search on imported embeddings failed")
	require.NotEmpty(t, results.Responses)

	hits := results.Responses[0].Hits.Hits
	require.NotEmpty(t, hits, "Expected search results from imported sparse embeddings")
	t.Logf("Sparse search on imported embeddings returned %d hits", len(hits))
	for i, hit := range hits {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}
}

// waitForSparseEmbeddings polls the sparse index status until enrichment is
// complete. Similar to waitForEmbeddings but uses AsEmbeddingsIndexStats.
func waitForSparseEmbeddings(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName, indexName string, expectedDocs int, timeout time.Duration) {
	t.Helper()

	t.Logf("Waiting for sparse enrichment on index %q to complete (%d documents expected)...", indexName, expectedDocs)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	pollCount := 0
	lastIndexed := uint64(0)
	stableCount := 0

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Context cancelled while waiting for sparse embeddings")
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				t.Fatalf("Timeout waiting for sparse enrichment after %d polls (got %d/%d embeddings)",
					pollCount, lastIndexed, expectedDocs)
			}

			indexStatus, err := client.GetIndex(ctx, tableName, indexName)
			if err != nil {
				t.Logf("  [Poll %d] Error getting index status: %v", pollCount, err)
				continue
			}

			stats, err := indexStatus.Status.AsEmbeddingsIndexStats()
			if err != nil {
				t.Logf("  [Poll %d] Error decoding sparse stats: %v", pollCount, err)
				continue
			}

			totalIndexed := stats.TotalIndexed
			t.Logf("  [Poll %d] Sparse embeddings: %d/%d indexed (%.1f%%)",
				pollCount, totalIndexed, expectedDocs,
				float64(totalIndexed)/float64(expectedDocs)*100)

			if totalIndexed >= uint64(expectedDocs) {
				t.Logf("All sparse embeddings indexed after %d polls (~%ds)",
					pollCount, pollCount*5)
				return
			}

			if totalIndexed == lastIndexed {
				stableCount++
				if stableCount >= 3 && totalIndexed > 0 {
					t.Logf("Sparse embedding count stable at %d/%d after %d polls, proceeding",
						totalIndexed, expectedDocs, pollCount)
					return
				}
			} else {
				stableCount = 0
			}
			lastIndexed = totalIndexed
		}
	}
}
