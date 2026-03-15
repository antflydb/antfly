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

package storeutils

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDocument_JustDocument(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:1")
	doc := []byte(`{"id": 1, "name": "Test Document"}`)

	insertDocument(t, db, key, doc, nil, nil)

	result, err := GetDocument(ctx, db, key, QueryOptions{})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(1), "name": "Test Document"}, result.Document)
	assert.Nil(t, result.Embedding)
	assert.Equal(t, uint64(0), result.EmbeddingHashID)
	assert.Empty(t, result.Summary)
	assert.Equal(t, uint64(0), result.SummaryHashID)
	assert.Nil(t, result.Embeddings)
	assert.Nil(t, result.Summaries)
}

func TestGetDocument_AllEmbeddings(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:2")
	doc := []byte(`{"id": 2, "title": "Embedding Test"}`)
	embeddings := map[string][]float32{
		"index1": {0.1, 0.2, 0.3},
		"index2": {0.4, 0.5, 0.6},
		"index3": {0.7, 0.8, 0.9},
	}

	insertDocument(t, db, key, doc, embeddings, nil)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		AllEmbeddings: true,
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(2), "title": "Embedding Test"}, result.Document)
	assert.NotNil(t, result.Embeddings)
	assert.Len(t, result.Embeddings, len(embeddings))

	for indexName, expectedEmb := range embeddings {
		actualEmb, ok := result.Embeddings[indexName]
		assert.True(t, ok, "Expected embedding for index %s", indexName)
		assert.InDeltaSlice(t, expectedEmb, actualEmb, 0.0001)
	}
}

func TestGetDocument_AllSummaries(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:3")
	doc := []byte(`{"id": 3, "content": "Summary Test"}`)
	summaries := map[string]string{
		"summary_index1": "This is a summary for index 1",
		"summary_index2": "This is a summary for index 2",
		"summary_index3": "This is a summary for index 3",
	}

	insertDocument(t, db, key, doc, nil, summaries)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		AllSummaries: true,
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(3), "content": "Summary Test"}, result.Document)
	assert.NotNil(t, result.Summaries)
	assert.Len(t, result.Summaries, len(summaries))

	for indexName, expectedSum := range summaries {
		actualSum, ok := result.Summaries[indexName]
		assert.True(t, ok, "Expected summary for index %s", indexName)
		assert.Equal(t, expectedSum, actualSum)
	}
}

func TestGetDocument_AllEmbeddingsAndSummaries(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:4")
	doc := []byte(`{"id": 4, "data": "Combined Test"}`)
	embeddings := map[string][]float32{
		"emb_index1": {0.1, 0.2},
		"emb_index2": {0.3, 0.4},
	}
	summaries := map[string]string{
		"sum_index1": "Summary 1",
		"sum_index2": "Summary 2",
	}

	insertDocument(t, db, key, doc, embeddings, summaries)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		AllEmbeddings: true,
		AllSummaries:  true,
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(4), "data": "Combined Test"}, result.Document)
	assert.Len(t, result.Embeddings, len(embeddings))
	assert.Len(t, result.Summaries, len(summaries))

	for indexName, expectedEmb := range embeddings {
		actualEmb, ok := result.Embeddings[indexName]
		assert.True(t, ok)
		assert.InDeltaSlice(t, expectedEmb, actualEmb, 0.0001)
	}

	for indexName, expectedSum := range summaries {
		actualSum, ok := result.Summaries[indexName]
		assert.True(t, ok)
		assert.Equal(t, expectedSum, actualSum)
	}
}

func TestGetDocument_SpecificEmbedding(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:5")
	doc := []byte(`{"id": 5, "test": "specific embedding"}`)
	embeddings := map[string][]float32{
		"target_index": {1.0, 2.0, 3.0},
		"other_index":  {4.0, 5.0, 6.0},
	}

	insertDocument(t, db, key, doc, embeddings, nil)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		EmbeddingSuffix: []byte(":i:target_index:e"),
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(5), "test": "specific embedding"}, result.Document)
	assert.NotNil(t, result.Embedding)
	assert.InDeltaSlice(t, embeddings["target_index"], result.Embedding, 0.0001)
	assert.Equal(t, uint64(12345), result.EmbeddingHashID)
	assert.Nil(t, result.Embeddings)
}

func TestGetDocument_SpecificSummary(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:6")
	doc := []byte(`{"id": 6, "test": "specific summary"}`)
	summaries := map[string]string{
		"target_summary": "This is the target summary",
		"other_summary":  "This is another summary",
	}

	insertDocument(t, db, key, doc, nil, summaries)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		SummarySuffix: []byte(":i:target_summary:s"),
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(6), "test": "specific summary"}, result.Document)
	assert.Equal(t, summaries["target_summary"], result.Summary)
	assert.Equal(t, uint64(67890), result.SummaryHashID)
	assert.Nil(t, result.Summaries)
}

func TestGetDocument_SpecificEmbeddingAndSummary(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:7")
	doc := []byte(`{"id": 7, "test": "both specific"}`)
	embeddings := map[string][]float32{
		"my_index": {0.5, 1.5, 2.5},
	}
	summaries := map[string]string{
		"my_index": "My index summary",
	}

	insertDocument(t, db, key, doc, embeddings, summaries)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		EmbeddingSuffix: []byte(":i:my_index:e"),
		SummarySuffix:   []byte(":i:my_index:s"),
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(7), "test": "both specific"}, result.Document)
	assert.InDeltaSlice(t, embeddings["my_index"], result.Embedding, 0.0001)
	assert.Equal(t, uint64(12345), result.EmbeddingHashID)
	assert.Equal(t, summaries["my_index"], result.Summary)
	assert.Equal(t, uint64(67890), result.SummaryHashID)
}

func TestGetDocument_NotFound(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:nonexistent")

	result, err := GetDocument(ctx, db, key, QueryOptions{})

	assert.Error(t, err)
	assert.ErrorIs(t, err, pebble.ErrNotFound)
	assert.Nil(t, result)
}

func TestGetDocument_EmptyEmbedding(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:8")
	doc := []byte(`{"id": 8}`)

	insertDocument(t, db, key, doc, nil, nil)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		EmbeddingSuffix: []byte(":i:missing_index:e"),
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(8)}, result.Document)
	assert.Nil(t, result.Embedding)
	assert.Equal(t, uint64(0), result.EmbeddingHashID)
}

func TestGetDocument_EmptySummary(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:9")
	doc := []byte(`{"id": 9}`)

	insertDocument(t, db, key, doc, nil, nil)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		SummarySuffix: []byte(":i:missing_summary:s"),
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": float64(9)}, result.Document)
	assert.Empty(t, result.Summary)
	assert.Equal(t, uint64(0), result.SummaryHashID)
}

func TestGetDocument_MixedIndexNames(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:10")
	doc := []byte(`{"id": 10, "test": "mixed"}`)
	embeddings := map[string][]float32{
		"vector_index_v1": {1.0, 2.0},
		"vector_index_v2": {3.0, 4.0},
		"text_embeddings": {5.0, 6.0},
	}
	summaries := map[string]string{
		"full_text_index":   "Full text summary",
		"semantic_search":   "Semantic summary",
		"custom_index_name": "Custom summary",
	}

	insertDocument(t, db, key, doc, embeddings, summaries)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		AllEmbeddings: true,
		AllSummaries:  true,
	})

	require.NoError(t, err)
	assert.Len(t, result.Embeddings, len(embeddings))
	assert.Len(t, result.Summaries, len(summaries))

	// Verify all index names are correctly extracted
	for indexName := range embeddings {
		_, ok := result.Embeddings[indexName]
		assert.True(t, ok, "Missing embedding for index: %s", indexName)
	}

	for indexName := range summaries {
		_, ok := result.Summaries[indexName]
		assert.True(t, ok, "Missing summary for index: %s", indexName)
	}
}

func TestGetDocument_LargeEmbedding(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:11")
	doc := []byte(`{"id": 11, "large": "embedding test"}`)

	// Create a large embedding (e.g., 1536 dimensions like OpenAI embeddings)
	largeEmb := make([]float32, 1536)
	for i := range largeEmb {
		largeEmb[i] = float32(i) * 0.001
	}

	embeddings := map[string][]float32{
		"large_embedding": largeEmb,
	}

	insertDocument(t, db, key, doc, embeddings, nil)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		AllEmbeddings: true,
	})

	require.NoError(t, err)
	assert.Len(t, result.Embeddings, 1)
	actualEmb := result.Embeddings["large_embedding"]
	assert.Len(t, actualEmb, len(largeEmb))
	assert.InDeltaSlice(t, largeEmb, actualEmb, 0.0001)
}

func TestGetDocument_LongSummary(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:12")
	doc := []byte(`{"id": 12, "long": "summary test"}`)

	// Create a long summary
	longSummary := ""
	var longSummarySb339 strings.Builder
	for range 1000 {
		longSummarySb339.WriteString("This is a test summary. ")
	}
	longSummary += longSummarySb339.String()

	summaries := map[string]string{
		"long_summary": longSummary,
	}

	insertDocument(t, db, key, doc, nil, summaries)

	result, err := GetDocument(ctx, db, key, QueryOptions{
		AllSummaries: true,
	})

	require.NoError(t, err)
	assert.Len(t, result.Summaries, 1)
	assert.Equal(t, longSummary, result.Summaries["long_summary"])
}

func TestGetDocument_DudEmbedding_SpecificSuffix(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:dud1")
	doc := []byte(`{"id": "dud1", "title": "Black Swan Funds"}`)

	// Insert document
	batch := db.NewBatch()
	docKey := KeyRangeStart(key)
	compressed := compressDocument(t, doc)
	require.NoError(t, batch.Set(docKey, compressed, nil))

	// Write a dud enrichment marker at the embedding key
	embKey := append(bytes.Clone(key), []byte(":i:thumbnail:e")...)
	require.NoError(t, batch.Set(embKey, DudEnrichmentValue, nil))
	require.NoError(t, batch.Commit(pebble.Sync))
	require.NoError(t, batch.Close())

	// GetDocument with specific EmbeddingSuffix should skip the dud
	result, err := GetDocument(ctx, db, key, QueryOptions{
		EmbeddingSuffix: []byte(":i:thumbnail:e"),
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": "dud1", "title": "Black Swan Funds"}, result.Document)
	assert.Nil(t, result.Embedding)
	assert.Equal(t, uint64(0), result.EmbeddingHashID)
}

func TestGetDocument_DudEmbedding_AllEmbeddings(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	key := []byte("test:doc:dud2")
	doc := []byte(`{"id": "dud2", "title": "Test Doc"}`)

	// Insert document with one real embedding and one dud
	batch := db.NewBatch()
	docKey := KeyRangeStart(key)
	compressed := compressDocument(t, doc)
	require.NoError(t, batch.Set(docKey, compressed, nil))

	// Real embedding
	realEmbKey := append(bytes.Clone(key), []byte(":i:title_body:e")...)
	realEmb := encodeEmbedding(t, []float32{0.1, 0.2, 0.3}, 12345)
	require.NoError(t, batch.Set(realEmbKey, realEmb, nil))

	// Dud embedding
	dudEmbKey := append(bytes.Clone(key), []byte(":i:thumbnail:e")...)
	require.NoError(t, batch.Set(dudEmbKey, DudEnrichmentValue, nil))

	require.NoError(t, batch.Commit(pebble.Sync))
	require.NoError(t, batch.Close())

	// GetDocument with AllEmbeddings should skip the dud and return the real one
	result, err := GetDocument(ctx, db, key, QueryOptions{
		AllEmbeddings: true,
	})

	require.NoError(t, err)
	assert.Len(t, result.Embeddings, 1)
	assert.InDeltaSlice(t, []float32{0.1, 0.2, 0.3}, result.Embeddings["title_body"], 0.0001)
	_, hasDud := result.Embeddings["thumbnail"]
	assert.False(t, hasDud)
}
