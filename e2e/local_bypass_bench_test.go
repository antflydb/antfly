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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// TestE2E_LocalBypassLatency compares end-to-end vector search latency with
// and without the in-process local bypass. Both modes run against the same
// dataset so the numbers are directly comparable.
//
// Run:
//
//	cd e2e && GOEXPERIMENT=simd go test -run TestE2E_LocalBypassLatency -v -timeout 10m
func TestE2E_LocalBypassLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping local bypass benchmark in short mode")
	}

	const (
		indexName  = "vec"
		batchSize  = 100
		topK       = 10
		numQueries = 100
	)

	// Load dataset once — shared across both sub-tests.
	dataset := testutils.LoadDataset(t, testutils.WikiDataset)
	totalVectors := int(dataset.GetCount())
	dim := int(dataset.GetDims())
	dataCount := totalVectors * 98 / 100
	queryCount := min(numQueries, totalVectors-dataCount)
	t.Logf("Dataset: %d vectors × %d dims, using %d for data, %d for queries",
		totalVectors, dim, dataCount, queryCount)

	dataVectors := dataset.Slice(0, dataCount)
	queryVectors := dataset.Slice(dataCount, queryCount)

	// Pre-build query embeddings.
	queryEmbeddings := make([]antfly.Embedding, queryCount)
	for i := range queryCount {
		queryEmbeddings[i] = antfly.NewPackedDenseEmbedding(queryVectors.At(i))
	}

	// Ground truth for recall.
	dataKeys := make([]int, dataCount)
	for i := range dataCount {
		dataKeys[i] = i
	}

	type benchResult struct {
		avgRecall  float64
		avg, p50   time.Duration
		p95, p99   time.Duration
		minL, maxL time.Duration
	}

	runBench := func(t *testing.T, localBypass bool) benchResult {
		t.Helper()

		tableName := "bypass_bench_http"
		if localBypass {
			tableName = "bypass_bench_local"
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{
			DisableTermite: true,
			LocalBypass:    localBypass,
		})
		defer swarm.Cleanup()

		// Create table.
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

		// Insert.
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

		// Wait for index readiness.
		require.Eventually(t, func() bool {
			resp, qErr := swarm.Client.Query(ctx, antfly.QueryRequest{
				Table:      tableName,
				Limit:      1,
				Embeddings: map[string]antfly.Embedding{indexName: queryEmbeddings[0]},
			})
			return qErr == nil && resp != nil && len(resp.Responses) > 0 &&
				len(resp.Responses[0].Hits.Hits) > 0
		}, 2*time.Minute, 500*time.Millisecond, "Index did not become ready")

		// Warmup.
		for i := range min(10, queryCount) {
			_, err := swarm.Client.Query(ctx, antfly.QueryRequest{
				Table:      tableName,
				Limit:      topK,
				Embeddings: map[string]antfly.Embedding{indexName: queryEmbeddings[i]},
			})
			require.NoError(t, err)
		}

		// Timed queries.
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

		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		var total time.Duration
		for _, l := range latencies {
			total += l
		}

		return benchResult{
			avgRecall: recallSum / float64(queryCount),
			avg:       total / time.Duration(queryCount),
			p50:       latencies[queryCount*50/100],
			p95:       latencies[queryCount*95/100],
			p99:       latencies[queryCount*99/100],
			minL:      latencies[0],
			maxL:      latencies[queryCount-1],
		}
	}

	// Run HTTP mode first, then local bypass.
	var httpResult, localResult benchResult

	t.Run("HTTP", func(t *testing.T) {
		httpResult = runBench(t, false)
		t.Logf("=== HTTP Mode (%d queries, %d docs, %dd) ===", queryCount, dataCount, dim)
		t.Logf("  recall@%d: %.1f%%", topK, httpResult.avgRecall*100)
		t.Logf("  avg: %v  p50: %v  p95: %v  p99: %v", httpResult.avg, httpResult.p50, httpResult.p95, httpResult.p99)
		t.Logf("  min: %v  max: %v", httpResult.minL, httpResult.maxL)
	})

	t.Run("LocalBypass", func(t *testing.T) {
		localResult = runBench(t, true)
		t.Logf("=== Local Bypass Mode (%d queries, %d docs, %dd) ===", queryCount, dataCount, dim)
		t.Logf("  recall@%d: %.1f%%", topK, localResult.avgRecall*100)
		t.Logf("  avg: %v  p50: %v  p95: %v  p99: %v", localResult.avg, localResult.p50, localResult.p95, localResult.p99)
		t.Logf("  min: %v  max: %v", localResult.minL, localResult.maxL)
	})

	// Summary comparison.
	if httpResult.avg > 0 && localResult.avg > 0 {
		t.Logf("")
		t.Logf("=== Comparison ===")
		t.Logf("  avg latency:  HTTP %v → Local %v (%.1f%% faster)",
			httpResult.avg, localResult.avg, 100*(1-float64(localResult.avg)/float64(httpResult.avg)))
		t.Logf("  p50 latency:  HTTP %v → Local %v (%.1f%% faster)",
			httpResult.p50, localResult.p50, 100*(1-float64(localResult.p50)/float64(httpResult.p50)))
		t.Logf("  p95 latency:  HTTP %v → Local %v (%.1f%% faster)",
			httpResult.p95, localResult.p95, 100*(1-float64(localResult.p95)/float64(httpResult.p95)))
		t.Logf("  recall:       HTTP %.1f%% vs Local %.1f%%",
			httpResult.avgRecall*100, localResult.avgRecall*100)
	}
}
