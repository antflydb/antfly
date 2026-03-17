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

package indexes

import (
	"testing"

	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRRFResults tests the Reciprocal Rank Fusion implementation
func TestRRFResults(t *testing.T) {
	tests := []struct {
		name          string
		bleveHits     []*search.DocumentMatch
		vectorHits    map[string]*vectorindex.SearchResult
		limit         int
		rankConstant  float64
		weights       map[string]float64
		expectedOrder []string // Expected document IDs in order
		expectedLen   int
	}{
		{
			name: "basic RRF with bleve and vector results",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0, Fields: map[string]any{"title": "First"}},
				{ID: "doc2", Score: 8.0, Fields: map[string]any{"title": "Second"}},
				{ID: "doc3", Score: 6.0, Fields: map[string]any{"title": "Third"}},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc2", Distance: 0.1, Score: 0.9},
						{ID: "doc1", Distance: 0.2, Score: 0.8},
						{ID: "doc4", Distance: 0.3, Score: 0.7},
					},
					Total: 3,
				},
			},
			limit:         5,
			rankConstant:  60.0,
			weights:       nil,
			expectedOrder: []string{"doc1", "doc2", "doc4", "doc3"},
			expectedLen:   4,
		},
		{
			name: "RRF with only bleve results",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
				{ID: "doc2", Score: 8.0},
				{ID: "doc3", Score: 6.0},
			},
			vectorHits:    nil,
			limit:         3,
			rankConstant:  60.0,
			weights:       nil,
			expectedOrder: []string{"doc1", "doc2", "doc3"},
			expectedLen:   3,
		},
		{
			name:      "RRF with only vector results",
			bleveHits: nil,
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc1", Score: 0.9},
						{ID: "doc2", Score: 0.8},
						{ID: "doc3", Score: 0.7},
					},
					Total: 3,
				},
			},
			limit:         3,
			rankConstant:  60.0,
			weights:       nil,
			expectedOrder: []string{"doc1", "doc2", "doc3"},
			expectedLen:   3,
		},
		{
			name: "RRF with limit smaller than results",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
				{ID: "doc2", Score: 8.0},
				{ID: "doc3", Score: 6.0},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc2", Score: 0.9},
						{ID: "doc1", Score: 0.8},
					},
					Total: 2,
				},
			},
			limit:         2,
			rankConstant:  60.0,
			weights:       nil,
			expectedOrder: []string{"doc2", "doc1"},
			expectedLen:   2,
		},
		{
			name: "RRF with multiple vector indexes",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx1": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc2", Score: 0.9},
						{ID: "doc1", Score: 0.8},
					},
					Total: 2,
				},
				"embedding_idx2": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc3", Score: 0.95},
						{ID: "doc1", Score: 0.85},
					},
					Total: 2,
				},
			},
			limit:         3,
			rankConstant:  60.0,
			weights:       nil,
			expectedOrder: []string{"doc1", "doc3", "doc2"},
			expectedLen:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create RemoteIndexSearchResult
			res := &RemoteIndexSearchResult{}

			if tt.bleveHits != nil {
				res.BleveSearchResult = &bleve.SearchResult{
					Hits: tt.bleveHits,
				}
			}

			if tt.vectorHits != nil {
				res.VectorSearchResult = tt.vectorHits
			}

			// Execute RRF
			result := res.RRFResults(tt.limit, tt.rankConstant, tt.weights)

			// Verify results
			require.NotNil(t, result)
			assert.Len(t, result.Hits, tt.expectedLen, "unexpected number of hits")

			// Verify expected documents are present (order may vary for equal scores due to map iteration)
			if len(tt.expectedOrder) > 0 {
				actualIDs := make([]string, len(result.Hits))
				for i, hit := range result.Hits {
					actualIDs[i] = hit.ID
				}
				assert.ElementsMatch(t, tt.expectedOrder, actualIDs, "unexpected document IDs")
			}

			// Verify scores are decreasing
			for i := 1; i < len(result.Hits); i++ {
				assert.GreaterOrEqual(t, result.Hits[i-1].Score, result.Hits[i].Score,
					"scores should be in descending order")
			}

			// Verify max score
			if len(result.Hits) > 0 {
				assert.Equal(t, result.Hits[0].Score, result.MaxScore, "max score should match first hit")
			}
		})
	}
}

// TestRRFWeighted tests that named weights are applied correctly in RRF
func TestRRFWeighted(t *testing.T) {
	// Create result where bleve and vector each have one unique doc
	res := &RemoteIndexSearchResult{
		BleveSearchResult: &bleve.SearchResult{
			Hits: []*search.DocumentMatch{
				{ID: "bleve_only", Score: 10.0, Fields: map[string]any{"title": "Bleve"}},
			},
		},
		VectorSearchResult: map[string]*vectorindex.SearchResult{
			"embedding_idx": {
				Hits: []*vectorindex.SearchHit{
					{ID: "vector_only", Score: 0.9},
				},
				Total: 1,
			},
		},
	}

	t.Run("equal weights produce equal RRF scores for rank-1 docs", func(t *testing.T) {
		result := res.RRFResults(10, 60.0, map[string]float64{
			FusionKeyFullText: 1.0,
			"embedding_idx":   1.0,
		})
		require.Len(t, result.Hits, 2)
		// Both are rank 1 with weight 1.0, so RRF scores should be equal
		assert.Equal(t, result.Hits[0].Score, result.Hits[1].Score)
	})

	t.Run("higher bleve weight boosts bleve-only doc", func(t *testing.T) {
		result := res.RRFResults(10, 60.0, map[string]float64{
			FusionKeyFullText: 3.0,
			"embedding_idx":   1.0,
		})
		require.Len(t, result.Hits, 2)
		assert.Equal(t, "bleve_only", result.Hits[0].ID)
		assert.Greater(t, result.Hits[0].Score, result.Hits[1].Score)
	})

	t.Run("higher vector weight boosts vector-only doc", func(t *testing.T) {
		result := res.RRFResults(10, 60.0, map[string]float64{
			FusionKeyFullText: 1.0,
			"embedding_idx":   3.0,
		})
		require.Len(t, result.Hits, 2)
		assert.Equal(t, "vector_only", result.Hits[0].ID)
		assert.Greater(t, result.Hits[0].Score, result.Hits[1].Score)
	})

	t.Run("nil weights default to 1.0", func(t *testing.T) {
		result := res.RRFResults(10, 60.0, nil)
		require.Len(t, result.Hits, 2)
		assert.Equal(t, result.Hits[0].Score, result.Hits[1].Score)
	})

	t.Run("rank constant affects scores", func(t *testing.T) {
		resultK60 := res.RRFResults(10, 60.0, nil)
		resultK1 := res.RRFResults(10, 1.0, nil)
		require.Len(t, resultK60.Hits, 2)
		require.Len(t, resultK1.Hits, 2)
		// Lower k means higher scores: 1/(1+1) = 0.5 vs 1/(60+1) ≈ 0.016
		assert.Greater(t, resultK1.Hits[0].Score, resultK60.Hits[0].Score)
	})
}

// TestRSFResults tests the Relative Score Fusion implementation
func TestRSFResults(t *testing.T) {
	tests := []struct {
		name          string
		bleveHits     []*search.DocumentMatch
		vectorHits    map[string]*vectorindex.SearchResult
		limit         int
		windowSize    int
		weights       map[string]float64
		expectedOrder []string
		expectedLen   int
		checkScores   bool
	}{
		{
			name: "basic RSF with equal weights",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0, Fields: map[string]any{"title": "First"}},
				{ID: "doc2", Score: 8.0, Fields: map[string]any{"title": "Second"}},
				{ID: "doc3", Score: 6.0, Fields: map[string]any{"title": "Third"}},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc2", Distance: 0.1, Score: 0.9},
						{ID: "doc1", Distance: 0.2, Score: 0.8},
						{ID: "doc3", Distance: 0.3, Score: 0.7},
					},
					Total: 3,
				},
			},
			limit:      5,
			windowSize: 10,
			weights: map[string]float64{
				FusionKeyFullText: 1.0,
				"embedding_idx":   1.0,
			},
			expectedOrder: []string{"doc1", "doc2", "doc3"},
			expectedLen:   3,
			checkScores:   true,
		},
		{
			name: "RSF with weighted preference for full-text",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
				{ID: "doc2", Score: 8.0},
				{ID: "doc3", Score: 6.0},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc3", Score: 0.9},
						{ID: "doc2", Score: 0.8},
						{ID: "doc1", Score: 0.7},
					},
					Total: 3,
				},
			},
			limit:      3,
			windowSize: 10,
			weights: map[string]float64{
				FusionKeyFullText: 2.0,
				"embedding_idx":   1.0,
			},
			expectedOrder: []string{"doc1", "doc2", "doc3"},
			expectedLen:   3,
		},
		{
			name: "RSF with weighted preference for vector",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
				{ID: "doc2", Score: 8.0},
				{ID: "doc3", Score: 6.0},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc3", Score: 0.9},
						{ID: "doc2", Score: 0.8},
						{ID: "doc1", Score: 0.7},
					},
					Total: 3,
				},
			},
			limit:      3,
			windowSize: 10,
			weights: map[string]float64{
				FusionKeyFullText: 1.0,
				"embedding_idx":   2.0,
			},
			expectedOrder: []string{"doc3", "doc2", "doc1"},
			expectedLen:   3,
		},
		{
			name: "RSF with window size smaller than results",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
				{ID: "doc2", Score: 8.0},
				{ID: "doc3", Score: 6.0},
				{ID: "doc4", Score: 4.0},
				{ID: "doc5", Score: 2.0},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc1", Score: 0.9},
						{ID: "doc2", Score: 0.8},
						{ID: "doc3", Score: 0.7},
						{ID: "doc4", Score: 0.6},
						{ID: "doc5", Score: 0.5},
					},
					Total: 5,
				},
			},
			limit:      3,
			windowSize: 2, // Only consider top 2 from each source
			weights: map[string]float64{
				FusionKeyFullText: 1.0,
				"embedding_idx":   1.0,
			},
			expectedLen: 2,
		},
		{
			name: "RSF with multiple vector indexes and different weights",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx1": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc2", Score: 0.9},
						{ID: "doc1", Score: 0.8},
					},
					Total: 2,
				},
				"embedding_idx2": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc3", Score: 0.95},
						{ID: "doc1", Score: 0.85},
					},
					Total: 2,
				},
			},
			limit:      3,
			windowSize: 10,
			weights: map[string]float64{
				FusionKeyFullText: 1.0,
				"embedding_idx1":  1.5,
				"embedding_idx2":  0.5,
			},
			expectedLen: 3,
		},
		{
			name: "RSF with nil weights defaults to 1.0",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
				{ID: "doc2", Score: 8.0},
			},
			vectorHits: map[string]*vectorindex.SearchResult{
				"embedding_idx": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc2", Score: 0.9},
						{ID: "doc1", Score: 0.8},
					},
					Total: 2,
				},
			},
			limit:       2,
			windowSize:  10,
			weights:     nil, // Will use default equal weights of 1.0
			expectedLen: 2,
		},
		{
			name: "RSF with only bleve results",
			bleveHits: []*search.DocumentMatch{
				{ID: "doc1", Score: 10.0},
				{ID: "doc2", Score: 8.0},
				{ID: "doc3", Score: 6.0},
			},
			vectorHits: nil,
			limit:      3,
			windowSize: 10,
			weights: map[string]float64{
				FusionKeyFullText: 1.0,
			},
			expectedLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create RemoteIndexSearchResult
			res := &RemoteIndexSearchResult{}

			if tt.bleveHits != nil {
				res.BleveSearchResult = &bleve.SearchResult{
					Hits: tt.bleveHits,
				}
			}

			if tt.vectorHits != nil {
				res.VectorSearchResult = tt.vectorHits
			}

			// Execute RSF
			result := res.RSFResults(tt.limit, tt.windowSize, tt.weights)

			// Verify results
			require.NotNil(t, result)
			assert.Len(t, result.Hits, tt.expectedLen, "unexpected number of hits")

			// Verify expected documents are present (order may vary due to implementation details)
			if len(tt.expectedOrder) > 0 {
				actualIDs := make([]string, len(result.Hits))
				for i, hit := range result.Hits {
					actualIDs[i] = hit.ID
				}
				assert.ElementsMatch(t, tt.expectedOrder, actualIDs, "unexpected document IDs")
			}

			// Verify scores are decreasing
			for i := 1; i < len(result.Hits); i++ {
				assert.GreaterOrEqual(t, result.Hits[i-1].Score, result.Hits[i].Score,
					"scores should be in descending order")
			}

			// Verify max score
			if len(result.Hits) > 0 {
				assert.Equal(t, result.Hits[0].Score, result.MaxScore, "max score should match first hit")
			}

			// Verify all scores are >= 0
			for i, hit := range result.Hits {
				assert.GreaterOrEqual(t, hit.Score, 0.0, "score at position %d should be non-negative", i)
			}

			// Verify IndexScores are populated
			for _, hit := range result.Hits {
				assert.NotEmpty(t, hit.IndexScores, "IndexScores should be populated for hit %s", hit.ID)
			}
		})
	}
}

// TestRSFNormalization tests that RSF properly normalizes scores
func TestRSFNormalization(t *testing.T) {
	// Test with known scores to verify normalization
	bleveHits := []*search.DocumentMatch{
		{ID: "doc1", Score: 100.0}, // max
		{ID: "doc2", Score: 50.0},  // mid
		{ID: "doc3", Score: 0.0},   // min
	}

	res := &RemoteIndexSearchResult{
		BleveSearchResult: &bleve.SearchResult{
			Hits: bleveHits,
		},
		VectorSearchResult: map[string]*vectorindex.SearchResult{
			"embedding_idx": {
				Hits: []*vectorindex.SearchHit{
					{ID: "doc1", Score: 1.0}, // max
					{ID: "doc2", Score: 0.5}, // mid
					{ID: "doc3", Score: 0.0}, // min
				},
				Total: 3,
			},
		},
	}

	// With equal weights, normalized scores should be:
	// doc1: (1.0 + 1.0) / 2 = 1.0 (both max)
	// doc2: (0.5 + 0.5) / 2 = 0.5 (both mid)
	// doc3: (0.0 + 0.0) / 2 = 0.0 (both min)
	result := res.RSFResults(3, 10, map[string]float64{
		FusionKeyFullText: 1.0,
		"embedding_idx":   1.0,
	})

	require.NotNil(t, result)
	require.Len(t, result.Hits, 3)

	// Verify doc1 has the highest score
	assert.Equal(t, "doc1", result.Hits[0].ID)
	assert.Equal(t, 2.0, result.Hits[0].Score) // 1.0 + 1.0

	// Verify doc2 has the middle score
	assert.Equal(t, "doc2", result.Hits[1].ID)
	assert.Equal(t, 1.0, result.Hits[1].Score) // 0.5 + 0.5

	// Verify doc3 has the lowest score
	assert.Equal(t, "doc3", result.Hits[2].ID)
	assert.Equal(t, 0.0, result.Hits[2].Score) // 0.0 + 0.0
}

// TestRSFWindowSize tests that window size is properly applied
func TestRSFWindowSize(t *testing.T) {
	// Create 10 documents in bleve results
	bleveHits := make([]*search.DocumentMatch, 10)
	for i := range 10 {
		bleveHits[i] = &search.DocumentMatch{
			ID:    string(rune('a' + i)),
			Score: float64(10 - i), // Decreasing scores
		}
	}

	res := &RemoteIndexSearchResult{
		BleveSearchResult: &bleve.SearchResult{
			Hits: bleveHits,
		},
	}

	// Use window size of 3 - only top 3 should be normalized
	result := res.RSFResults(10, 3, map[string]float64{FusionKeyFullText: 1.0})

	require.NotNil(t, result)

	// Only 3 documents should appear (those within the window)
	assert.Len(t, result.Hits, 3)

	// Verify they are the top 3
	assert.Equal(t, "a", result.Hits[0].ID)
	assert.Equal(t, "b", result.Hits[1].ID)
	assert.Equal(t, "c", result.Hits[2].ID)
}

// TestRSFDeterminism tests that RSF with named weights is deterministic
// regardless of map iteration order (the bug that positional weights had)
func TestRSFDeterminism(t *testing.T) {
	makeResult := func() *RemoteIndexSearchResult {
		return &RemoteIndexSearchResult{
			BleveSearchResult: &bleve.SearchResult{
				Hits: []*search.DocumentMatch{
					{ID: "doc1", Score: 10.0},
				},
			},
			VectorSearchResult: map[string]*vectorindex.SearchResult{
				"title_embedding": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc1", Score: 0.9},
						{ID: "doc2", Score: 0.8},
					},
					Total: 2,
				},
				"content_embedding": {
					Hits: []*vectorindex.SearchHit{
						{ID: "doc1", Score: 0.7},
						{ID: "doc3", Score: 0.6},
					},
					Total: 2,
				},
			},
		}
	}

	weights := map[string]float64{
		FusionKeyFullText:   0.5,
		"title_embedding":   2.0,
		"content_embedding": 0.3,
	}

	// Run 100 times and verify all produce identical results
	var firstScores map[string]float64
	for i := range 100 {
		res := makeResult()
		result := res.RSFResults(10, 10, weights)
		scores := make(map[string]float64, len(result.Hits))
		for _, hit := range result.Hits {
			scores[hit.ID] = hit.Score
		}
		if i == 0 {
			firstScores = scores
		} else {
			assert.Equal(t, firstScores, scores, "iteration %d produced different scores", i)
		}
	}
}

// TestFusionResultMerge tests the Merge functionality
func TestFusionResultMerge(t *testing.T) {
	result1 := &FusionResult{
		Hits: []*FusionHit{
			{ID: "doc1", Score: 10.0},
			{ID: "doc2", Score: 8.0},
		},
		Total:    2,
		MaxScore: 10.0,
	}

	result2 := &FusionResult{
		Hits: []*FusionHit{
			{ID: "doc3", Score: 12.0},
			{ID: "doc4", Score: 6.0},
		},
		Total:    2,
		MaxScore: 12.0,
	}

	result1.Merge(result2)
	result1.FinalizeSort()

	assert.Len(t, result1.Hits, 4)
	assert.Equal(t, uint64(4), result1.Total)
	assert.Equal(t, 12.0, result1.MaxScore) // Should take max of both
}

// TestEmptyResults tests edge cases with empty results
func TestEmptyResults(t *testing.T) {
	t.Run("empty bleve and vector results for RRF", func(t *testing.T) {
		res := &RemoteIndexSearchResult{}
		result := res.RRFResults(10, 60.0, nil)
		require.NotNil(t, result)
		assert.Empty(t, result.Hits)
		assert.Equal(t, 0.0, result.MaxScore)
	})

	t.Run("empty bleve and vector results for RSF", func(t *testing.T) {
		res := &RemoteIndexSearchResult{}
		result := res.RSFResults(10, 10, map[string]float64{
			FusionKeyFullText: 1.0,
			"embedding_idx":   1.0,
		})
		require.NotNil(t, result)
		assert.Empty(t, result.Hits)
		assert.Equal(t, 0.0, result.MaxScore)
	})
}

// TestPruner tests the result pruning functionality
func TestPruner(t *testing.T) {
	// Helper to create test hits
	makeHits := func(scores ...float64) []*FusionHit {
		hits := make([]*FusionHit, len(scores))
		for i, score := range scores {
			hits[i] = &FusionHit{
				ID:    string(rune('A' + i)),
				Score: score,
				IndexScores: map[string]float64{
					"full_text_v0": score * 10,
				},
			}
		}
		return hits
	}

	// Helper to create multi-index hits
	makeMultiIndexHits := func(scores ...float64) []*FusionHit {
		hits := make([]*FusionHit, len(scores))
		for i, score := range scores {
			hits[i] = &FusionHit{
				ID:    string(rune('A' + i)),
				Score: score,
				IndexScores: map[string]float64{
					"full_text_v0":   score * 10,
					"embedding_idx1": score * 0.5,
				},
			}
		}
		return hits
	}

	t.Run("nil pruning config returns original hits", func(t *testing.T) {
		hits := makeHits(10.0, 8.0, 6.0)
		var rp *Pruner
		result := rp.PruneResults(hits)
		assert.Len(t, result, 3)
	})

	t.Run("empty pruning config returns original hits", func(t *testing.T) {
		hits := makeHits(10.0, 8.0, 6.0)
		rp := &Pruner{}
		result := rp.PruneResults(hits)
		assert.Len(t, result, 3)
		assert.True(t, rp.IsEmpty())
	})

	t.Run("MinAbsoluteScore filters low scores", func(t *testing.T) {
		hits := makeHits(10.0, 8.0, 6.0, 4.0, 2.0)
		rp := &Pruner{MinAbsoluteScore: 5.0}
		result := rp.PruneResults(hits)
		assert.Len(t, result, 3)
		assert.Equal(t, "A", result[0].ID) // 10.0
		assert.Equal(t, "B", result[1].ID) // 8.0
		assert.Equal(t, "C", result[2].ID) // 6.0
	})

	t.Run("MinScoreRatio filters relative to max", func(t *testing.T) {
		hits := makeHits(10.0, 8.0, 6.0, 4.0, 2.0)
		rp := &Pruner{MinScoreRatio: 0.5}
		result := rp.PruneResults(hits)
		// 10.0 * 0.5 = 5.0 threshold
		assert.Len(t, result, 3)
		assert.Equal(t, "A", result[0].ID) // 10.0 >= 5.0
		assert.Equal(t, "B", result[1].ID) // 8.0 >= 5.0
		assert.Equal(t, "C", result[2].ID) // 6.0 >= 5.0
	})

	t.Run("MaxScoreGapPercent detects score gaps", func(t *testing.T) {
		// Scores: 10.0, 9.0, 8.5, 5.0, 4.0 — range = 6.0
		// Gaps as % of range: 16.7%, 8.3%, 58.3%, 16.7%
		hits := makeHits(10.0, 9.0, 8.5, 5.0, 4.0)
		rp := &Pruner{MaxScoreGapPercent: 30.0}
		result := rp.PruneResults(hits)
		// Should stop at the 58.3% gap (between 8.5 and 5.0)
		assert.Len(t, result, 3)
		assert.Equal(t, "A", result[0].ID) // 10.0
		assert.Equal(t, "B", result[1].ID) // 9.0
		assert.Equal(t, "C", result[2].ID) // 8.5
	})

	t.Run("StdDevThreshold filters statistical outliers", func(t *testing.T) {
		// Scores: 10.0, 9.0, 8.0, 7.0, 1.0
		// Mean: 7.0, StdDev: ~3.16
		// Threshold with multiplier 1.0: 7.0 - 3.16 = 3.84
		hits := makeHits(10.0, 9.0, 8.0, 7.0, 1.0)
		rp := &Pruner{StdDevThreshold: 1.0}
		result := rp.PruneResults(hits)
		// 1.0 < 3.84, so should be filtered out
		assert.Len(t, result, 4)
		for _, hit := range result {
			assert.NotEqual(t, "E", hit.ID) // 1.0 should be filtered
		}
	})

	t.Run("RequireMultiIndex filters single-index hits", func(t *testing.T) {
		// Mix of single and multi-index hits
		hits := []*FusionHit{
			{ID: "A", Score: 10.0, IndexScores: map[string]float64{"full_text_v0": 100.0, "embedding_idx": 0.9}},
			{ID: "B", Score: 9.0, IndexScores: map[string]float64{"full_text_v0": 90.0}}, // Single index
			{ID: "C", Score: 8.0, IndexScores: map[string]float64{"full_text_v0": 80.0, "embedding_idx": 0.7}},
			{ID: "D", Score: 7.0, IndexScores: map[string]float64{"embedding_idx": 0.6}}, // Single index
		}
		rp := &Pruner{RequireMultiIndex: true}
		result := rp.PruneResults(hits)
		assert.Len(t, result, 2)
		assert.Equal(t, "A", result[0].ID)
		assert.Equal(t, "C", result[1].ID)
	})

	t.Run("combined pruning strategies", func(t *testing.T) {
		hits := makeMultiIndexHits(10.0, 9.0, 8.0, 3.0, 2.0)
		rp := &Pruner{
			MinAbsoluteScore:   2.5,
			MinScoreRatio:      0.3,
			MaxScoreGapPercent: 50.0,
		}
		result := rp.PruneResults(hits)
		// MinAbsoluteScore: filters 2.0
		// MinScoreRatio: 10.0 * 0.3 = 3.0, filters nothing new
		// MaxScoreGapPercent: 62.5% gap between 8.0 and 3.0, stops at 8.0
		assert.Len(t, result, 3)
	})

	t.Run("pruning with reranked scores", func(t *testing.T) {
		rerankedScore := 15.0
		hits := []*FusionHit{
			{ID: "A", Score: 10.0, RerankedScore: &rerankedScore, IndexScores: map[string]float64{"idx": 1.0}},
			{ID: "B", Score: 9.0, IndexScores: map[string]float64{"idx": 1.0}},
			{ID: "C", Score: 8.0, IndexScores: map[string]float64{"idx": 1.0}},
		}
		rp := &Pruner{MinScoreRatio: 0.6}
		result := rp.PruneResults(hits)
		// Max effective score is 15.0 (reranked), threshold = 9.0
		// A: 15.0 >= 9.0 ✓
		// B: 9.0 >= 9.0 ✓
		// C: 8.0 < 9.0 ✗
		assert.Len(t, result, 2)
	})

	t.Run("empty hits returns empty", func(t *testing.T) {
		rp := &Pruner{MinAbsoluteScore: 5.0}
		result := rp.PruneResults([]*FusionHit{})
		assert.Empty(t, result)
	})

	t.Run("single hit is preserved unless filtered by absolute score", func(t *testing.T) {
		hits := makeHits(10.0)
		rp := &Pruner{
			MaxScoreGapPercent: 10.0, // No gap to detect with single hit
			StdDevThreshold:    1.0,  // Not applied with < 3 hits
		}
		result := rp.PruneResults(hits)
		assert.Len(t, result, 1)
	})
}

// TestPruneByScoreGap tests the elbow detection algorithm in detail
func TestPruneByScoreGap(t *testing.T) {
	makeHits := func(scores ...float64) []*FusionHit {
		hits := make([]*FusionHit, len(scores))
		for i, score := range scores {
			hits[i] = &FusionHit{ID: string(rune('A' + i)), Score: score}
		}
		return hits
	}

	tests := []struct {
		name           string
		scores         []float64
		maxDropPercent float64
		expectedLen    int
	}{
		{
			name:           "no gap exceeds threshold",
			scores:         []float64{10.0, 9.5, 9.0, 8.5, 8.0},
			maxDropPercent: 30.0, // each gap is 0.5, range is 2.0, so 25% of range
			expectedLen:    5,
		},
		{
			name:           "clear elbow at position 3",
			scores:         []float64{10.0, 9.0, 8.0, 2.0, 1.0},
			maxDropPercent: 50.0,
			expectedLen:    3,
		},
		{
			name:           "immediate large gap",
			scores:         []float64{10.0, 1.0, 0.5},
			maxDropPercent: 50.0,
			expectedLen:    1,
		},
		{
			name:           "gradual decline",
			scores:         []float64{10.0, 8.1, 6.6, 5.4, 4.4},
			maxDropPercent: 35.0, // largest gap is 1.9 (10→8.1), range is 5.6, so 33.9%
			expectedLen:    5,
		},
		{
			name:           "single hit",
			scores:         []float64{10.0},
			maxDropPercent: 50.0,
			expectedLen:    1,
		},
		{
			name:           "zero threshold allows all",
			scores:         []float64{10.0, 1.0, 0.1},
			maxDropPercent: 0.0,
			expectedLen:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hits := makeHits(tt.scores...)
			result := pruneByScoreGap(hits, tt.maxDropPercent)
			assert.Len(t, result, tt.expectedLen)
		})
	}
}

// TestPrunerSmallScores tests pruning with sub-1.0 scores typical of
// rerankers and embedding similarity (e.g. 0.0003–0.05 range).
func TestPrunerSmallScores(t *testing.T) {
	makeHits := func(scores ...float64) []*FusionHit {
		hits := make([]*FusionHit, len(scores))
		for i, score := range scores {
			hits[i] = &FusionHit{
				ID:    string(rune('A' + i)),
				Score: score,
				IndexScores: map[string]float64{
					"embedding_idx": score,
				},
			}
		}
		return hits
	}

	t.Run("reranker scores with clear elbow", func(t *testing.T) {
		// Typical reranker output: cluster of good results then dropoff
		hits := makeHits(0.92, 0.89, 0.85, 0.83, 0.61, 0.58, 0.55)
		rp := &Pruner{MaxScoreGapPercent: 40.0}
		result := rp.PruneResults(hits)
		// Range: 0.37, gap 0.83→0.61 = 0.22, 0.22/0.37 = 59.5% → triggers
		assert.Len(t, result, 4)
		assert.Equal(t, "D", result[3].ID) // 0.83 is last kept
	})

	t.Run("very small scores with elbow", func(t *testing.T) {
		// Embedding similarity scores in a small range
		hits := makeHits(0.05, 0.045, 0.042, 0.01, 0.003, 0.0003)
		rp := &Pruner{MaxScoreGapPercent: 40.0}
		result := rp.PruneResults(hits)
		// Range: 0.0497, gap 0.042→0.01 = 0.032, 0.032/0.0497 = 64.4% → triggers
		assert.Len(t, result, 3)
	})

	t.Run("negative scores with min_score_ratio", func(t *testing.T) {
		// Cross-encoder rerankers often produce negative logit scores
		hits := makeHits(-1.0, -2.0, -5.0, -8.0, -10.0)
		rp := &Pruner{MinScoreRatio: 0.5}
		result := rp.PruneResults(hits)
		// maxScore = -1.0, threshold = -1.0 / 0.5 = -2.0
		// Keeps scores >= -2.0: -1.0, -2.0
		assert.Len(t, result, 2)
		assert.Equal(t, "A", result[0].ID) // -1.0
		assert.Equal(t, "B", result[1].ID) // -2.0
	})

	t.Run("all negative scores with low ratio", func(t *testing.T) {
		hits := makeHits(-1.0, -2.0, -5.0, -8.0, -10.0)
		rp := &Pruner{MinScoreRatio: 0.01}
		result := rp.PruneResults(hits)
		// maxScore = -1.0, threshold = -1.0 / 0.01 = -100.0
		// All scores >= -100.0, so all kept
		assert.Len(t, result, 5)
	})

	t.Run("small scores with min_score_ratio", func(t *testing.T) {
		hits := makeHits(0.05, 0.045, 0.042, 0.01, 0.003)
		rp := &Pruner{MinScoreRatio: 0.6}
		result := rp.PruneResults(hits)
		// threshold = 0.05 * 0.6 = 0.03, keeps 0.05, 0.045, 0.042
		assert.Len(t, result, 3)
	})

	t.Run("small scores combined pruning", func(t *testing.T) {
		hits := makeHits(0.05, 0.048, 0.045, 0.02, 0.018, 0.005)
		rp := &Pruner{
			MinScoreRatio:      0.3,
			MaxScoreGapPercent: 40.0,
		}
		result := rp.PruneResults(hits)
		// MinScoreRatio: 0.05 * 0.3 = 0.015, filters 0.005
		// Remaining: [0.05, 0.048, 0.045, 0.02, 0.018]
		// Range: 0.032, gap 0.045→0.02 = 0.025, 0.025/0.032 = 78.1% → triggers
		assert.Len(t, result, 3)
	})

	t.Run("tight cluster no elbow", func(t *testing.T) {
		// All scores very close — no elbow to detect
		hits := makeHits(0.051, 0.050, 0.049, 0.048, 0.047)
		rp := &Pruner{MaxScoreGapPercent: 40.0}
		result := rp.PruneResults(hits)
		// Range: 0.004, each gap 0.001, 0.001/0.004 = 25% → doesn't trigger
		assert.Len(t, result, 5)
	})
}

// TestPruneByStdDev tests the standard deviation filtering
func TestPruneByStdDev(t *testing.T) {
	makeHits := func(scores ...float64) []*FusionHit {
		hits := make([]*FusionHit, len(scores))
		for i, score := range scores {
			hits[i] = &FusionHit{ID: string(rune('A' + i)), Score: score}
		}
		return hits
	}

	t.Run("filters outliers", func(t *testing.T) {
		// Scores: 10, 10, 10, 10, 1 (outlier)
		// Mean: 8.2, Variance: 12.96, StdDev: 3.6
		// Threshold (1.0 * stddev): 8.2 - 3.6 = 4.6
		hits := makeHits(10.0, 10.0, 10.0, 10.0, 1.0)
		result := pruneByStdDev(hits, 1.0)
		assert.Len(t, result, 4)
	})

	t.Run("preserves all with high threshold", func(t *testing.T) {
		hits := makeHits(10.0, 9.0, 8.0, 7.0, 1.0)
		result := pruneByStdDev(hits, 3.0) // Very permissive threshold
		assert.Len(t, result, 5)
	})

	t.Run("returns original if less than 3 hits", func(t *testing.T) {
		hits := makeHits(10.0, 1.0)
		result := pruneByStdDev(hits, 0.5)
		assert.Len(t, result, 2)
	})
}

// TestPrunerIsEmpty tests the IsEmpty helper
func TestPrunerIsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		rp       *Pruner
		expected bool
	}{
		{name: "nil is empty", rp: nil, expected: true},
		{name: "zero values is empty", rp: &Pruner{}, expected: true},
		{name: "MinScoreRatio set", rp: &Pruner{MinScoreRatio: 0.5}, expected: false},
		{name: "MaxScoreGapPercent set", rp: &Pruner{MaxScoreGapPercent: 30.0}, expected: false},
		{name: "MinAbsoluteScore set", rp: &Pruner{MinAbsoluteScore: 0.01}, expected: false},
		{name: "RequireMultiIndex set", rp: &Pruner{RequireMultiIndex: true}, expected: false},
		{name: "StdDevThreshold set", rp: &Pruner{StdDevThreshold: 1.5}, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.rp.IsEmpty())
		})
	}
}
