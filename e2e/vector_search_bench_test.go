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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// TestE2E_VectorSearchLatency measures end-to-end vector search latency and
// recall through the full Antfly stack:
//
//	HTTP API → metadata → shard routing → HBC index → response
//
// Uses the pre-computed wiki-articles 768d-10k dataset (no Termite needed) to
// isolate the database search path from embedding generation. Both inserts and
// queries use the packed dense format (base64-encoded little-endian float32
// bytes) which is ~4x more compact on the wire than JSON float arrays.
//
// Recall is measured by comparing the e2e search results against brute-force
// exact kNN computed locally on the same dataset.
//
// Run:
//
//	cd e2e && GOEXPERIMENT=simd go test -run TestE2E_VectorSearchLatency -v -timeout 10m
//
// With CPU profile:
//
//	cd e2e && GOEXPERIMENT=simd go test -run TestE2E_VectorSearchLatency -v -timeout 10m -cpuprofile cpu.prof
//	go tool pprof -http=:8080 cpu.prof
func TestE2E_VectorSearchLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e vector search benchmark in short mode")
	}

	const (
		tableName  = "vector_bench"
		indexName  = "vec"
		batchSize  = 100
		topK       = 10
		numQueries = 100
	)

	// Load the pre-computed dataset.
	dataset := testutils.LoadDataset(t, testutils.WikiDataset)
	totalVectors := int(dataset.GetCount())
	dim := int(dataset.GetDims())
	dataCount := totalVectors * 98 / 100
	queryCount := min(numQueries, totalVectors-dataCount)
	t.Logf("Dataset: %d vectors × %d dims, using %d for data, %d for queries",
		totalVectors, dim, dataCount, queryCount)

	dataVectors := dataset.Slice(0, dataCount)
	queryVectors := dataset.Slice(dataCount, queryCount)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Start single-node swarm without Termite.
	t.Log("Starting Antfly swarm (no Termite)...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	defer swarm.Cleanup()

	// Create table with external embedding index (no embedder).
	embIdx := antfly.IndexConfig{Name: indexName, Type: "aknn_v0"}
	err := embIdx.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Dimension:      dim,
		DistanceMetric: antfly.DistanceMetricCosine,
		External:       true,
	})
	require.NoError(t, err)

	err = swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes:   map[string]antfly.IndexConfig{indexName: embIdx},
	})
	require.NoError(t, err)
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)

	// Insert documents with pre-computed embeddings via _embeddings field.
	// Using vector.T triggers base64 MarshalJSON (~4x smaller than float arrays).
	t.Logf("Inserting %d documents...", dataCount)
	insertStart := time.Now()
	for batchStart := 0; batchStart < dataCount; batchStart += batchSize {
		batchEnd := min(batchStart+batchSize, dataCount)
		inserts := make(map[string]any, batchEnd-batchStart)
		for i := batchStart; i < batchEnd; i++ {
			inserts[fmt.Sprintf("doc-%05d", i)] = map[string]any{
				"title":       fmt.Sprintf("Document %d", i),
				"_embeddings": map[string]any{indexName: vector.T(dataset.At(i))},
			}
		}
		_, batchErr := swarm.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   inserts,
			SyncLevel: antfly.SyncLevelAknn,
		})
		require.NoError(t, batchErr)
	}
	t.Logf("Inserted %d docs in %v", dataCount, time.Since(insertStart))

	// Wait for index to be searchable.
	queryEmb := antfly.NewPackedDenseEmbedding(queryVectors.At(0))
	t.Log("Waiting for index to become ready...")
	require.Eventually(t, func() bool {
		resp, qErr := swarm.Client.Query(ctx, antfly.QueryRequest{
			Table:      tableName,
			Limit:      1,
			Embeddings: map[string]antfly.Embedding{indexName: queryEmb},
		})
		return qErr == nil && resp != nil && len(resp.Responses) > 0 &&
			len(resp.Responses[0].Hits.Hits) > 0
	}, 2*time.Minute, 500*time.Millisecond, "Index did not become ready")

	// Build query embeddings from the held-out portion (packed dense format).
	queryEmbeddings := make([]antfly.Embedding, queryCount)
	for i := range queryCount {
		queryEmbeddings[i] = antfly.NewPackedDenseEmbedding(queryVectors.At(i))
	}

	// Warmup.
	for i := range min(10, queryCount) {
		_, err := swarm.Client.Query(ctx, antfly.QueryRequest{
			Table:      tableName,
			Limit:      topK,
			Embeddings: map[string]antfly.Embedding{indexName: queryEmbeddings[i]},
		})
		require.NoError(t, err)
	}

	// Compute brute-force ground truth for recall measurement.
	dataKeys := make([]int, dataCount)
	for i := range dataCount {
		dataKeys[i] = i
	}

	// Timed search loop with recall tracking.
	t.Logf("Running %d search queries (topK=%d, packed dense format)...", queryCount, topK)
	latencies := make([]time.Duration, queryCount)
	var recallSum float64
	for i := range queryCount {
		start := time.Now()
		resp, qErr := swarm.Client.Query(ctx, antfly.QueryRequest{
			Table:      tableName,
			Limit:      topK,
			Embeddings: map[string]antfly.Embedding{indexName: queryEmbeddings[i]},
		})
		latencies[i] = time.Since(start)

		require.NoError(t, qErr)
		require.NotNil(t, resp)
		require.NotEmpty(t, resp.Responses)
		hits := resp.Responses[0].Hits.Hits
		require.NotEmpty(t, hits, "expected search results for query %d", i)

		// Map returned doc IDs ("doc-00042") back to dataset indices for recall.
		prediction := make([]int, len(hits))
		for j, hit := range hits {
			prediction[j] = parseDocIndex(t, hit.ID)
		}

		truth := testutils.CalculateTruth(
			topK,
			vector.DistanceMetric_Cosine,
			queryVectors.At(i),
			dataVectors,
			dataKeys,
		)
		recallSum += testutils.CalculateRecall(prediction, truth)
	}

	avgRecall := recallSum / float64(queryCount)

	// Report percentile latencies.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	t.Logf("=== Vector Search Results (%d queries, %d docs, %dd) ===", queryCount, dataCount, dim)
	t.Logf("  recall@%d: %.1f%%", topK, avgRecall*100)
	t.Logf("  avg:  %v", total/time.Duration(queryCount))
	t.Logf("  p50:  %v", latencies[queryCount*50/100])
	t.Logf("  p95:  %v", latencies[queryCount*95/100])
	t.Logf("  p99:  %v", latencies[queryCount*99/100])
	t.Logf("  min:  %v", latencies[0])
	t.Logf("  max:  %v", latencies[queryCount-1])

	require.Greater(t, avgRecall, 0.80, "recall@%d should be above 80%%", topK)

	// Verify packed dense embeddings produce identical results to dense array.
	t.Log("Verifying packed dense embedding format...")
	for i := range min(5, queryCount) {
		v := queryVectors.At(i)

		denseResp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
			Table:      tableName,
			Limit:      topK,
			Embeddings: map[string]antfly.Embedding{indexName: antfly.NewDenseEmbedding(v)},
		})
		require.NoError(t, err)

		packedResp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
			Table:      tableName,
			Limit:      topK,
			Embeddings: map[string]antfly.Embedding{indexName: antfly.NewPackedDenseEmbedding(v)},
		})
		require.NoError(t, err)

		require.Equal(t, len(denseResp.Responses[0].Hits.Hits), len(packedResp.Responses[0].Hits.Hits),
			"packed dense query %d returned different number of hits", i)
		for j, denseHit := range denseResp.Responses[0].Hits.Hits {
			packedHit := packedResp.Responses[0].Hits.Hits[j]
			require.Equal(t, denseHit.ID, packedHit.ID,
				"packed dense query %d hit %d: ID mismatch", i, j)
		}
	}
	t.Log("Packed dense embedding format verified OK")

	// Compare wire sizes: dense array vs packed base64.
	sampleVec := queryVectors.At(0)
	denseReq := antfly.QueryRequest{
		Table:      tableName,
		Limit:      topK,
		Embeddings: map[string]antfly.Embedding{indexName: antfly.NewDenseEmbedding(sampleVec)},
	}
	packedReq := antfly.QueryRequest{
		Table:      tableName,
		Limit:      topK,
		Embeddings: map[string]antfly.Embedding{indexName: antfly.NewPackedDenseEmbedding(sampleVec)},
	}
	denseJSON, _ := json.Marshal(denseReq)
	packedJSON, _ := json.Marshal(packedReq)
	t.Logf("=== Wire Size Comparison (%dd vector) ===", dim)
	t.Logf("  dense array:  %d bytes", len(denseJSON))
	t.Logf("  packed base64: %d bytes", len(packedJSON))
	t.Logf("  savings:       %.1f%%", 100*(1-float64(len(packedJSON))/float64(len(denseJSON))))
}

// parseDocIndex extracts the dataset index from a doc key like "doc-00042".
func parseDocIndex(t *testing.T, docID string) int {
	t.Helper()
	idx, err := strconv.Atoi(strings.TrimPrefix(docID, "doc-"))
	require.NoError(t, err, "unexpected doc ID format: %q", docID)
	return idx
}
