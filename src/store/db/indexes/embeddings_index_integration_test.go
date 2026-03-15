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
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestEmbeddingIndex_WithEnricher(t *testing.T) {
	origDefaultFlushTime := DefaultFlushTime
	DefaultFlushTime = 50 * time.Millisecond // Shorten for test speed
	t.Cleanup(func() {
		DefaultFlushTime = origDefaultFlushTime
	})
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Track embeddings generated
	var embeddingsMu sync.Mutex
	generatedEmbeddings := make(map[string][]float32)

	// Create a mock embedder that tracks what it generates
	mockEmb := &mockEmbedder{
		embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
			result := make([][]float32, len(values))
			for i, val := range values {
				// Generate deterministic embeddings based on content
				result[i] = []float32{float32(i), float32(len(val)), float32(len(val) % 10)}
				embeddingsMu.Lock()
				generatedEmbeddings[val] = result[i]
				embeddingsMu.Unlock()
			}
			return result, nil
		},
	}

	// Override the plugin registry for this test
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return mockEmb, nil
		},
	)
	t.Cleanup(func() {
		embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	})

	config := NewEmbeddingsConfig("test_index", EmbeddingsIndexConfig{
		Dimension: 3,
		Template:  "{{title}}: {{content}}",
		Embedder: embeddings.NewEmbedderConfigFromJSON(
			"mock",
			[]byte(`{ "provider": "mock", "model": "test-model" }`),
		),
	})

	idx, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_index", config, nil)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	schema := &schema.TableSchema{}
	byteRange := types.Range{nil, nil}
	err = ei.Open(true, schema, byteRange)
	require.NoError(t, err)

	// Simulate leader factory being called
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	persistCalled := 0
	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		persistCalled++
		// Actually persist the embeddings
		pbatch := db.NewBatch()
		defer pbatch.Close()

		keys := make([][]byte, len(writes))
		for j := range writes {
			keys[j] = writes[j][0]
			if err := pbatch.Set(keys[j], writes[j][1], nil); err != nil {
				return err
			}
		}
		if err := pbatch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("failed to persist embeddings: %w", err)
		}
		if err := idx.(*EmbeddingIndex).enricherCallback(ctx, keys); err != nil {
			return fmt.Errorf("signaling index: %w", err)
		}
		return nil
	}

	go func() {
		_ = ei.LeaderFactory(ctx, persistFunc)
	}()

	// Wait for enricher to be ready
	time.Sleep(100 * time.Millisecond)

	// Store test documents
	testDocs := []struct {
		key []byte
		doc map[string]any
	}{
		{
			key: []byte("doc1"),
			doc: map[string]any{
				"title":   "Document 1",
				"content": "This is the first document",
			},
		},
		{
			key: []byte("doc2"),
			doc: map[string]any{
				"title":   "Document 2",
				"content": "This is the second document",
			},
		},
		{
			key: []byte("doc3"),
			doc: map[string]any{
				"title":   "Document 3",
				"content": "This is the third document",
			},
		},
	}

	// Store compressed documents
	for _, td := range testDocs {
		docBytes, err := json.Marshal(td.doc)
		require.NoError(t, err)

		writer, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressed := writer.EncodeAll(docBytes, nil)

		err = db.Set(append(td.key, storeutils.DBRangeStart...), compressed, nil)
		require.NoError(t, err)
	}

	// Process documents through batch
	writes := make([][2][]byte, len(testDocs))
	for i, td := range testDocs {
		writes[i] = [2][]byte{td.key, {}}
	}

	err = ei.Batch(t.Context(), writes, nil, false)
	require.NoError(t, err)

	// Wait for enrichment to complete
	// time.Sleep(500 * time.Millisecond)
	time.Sleep(2*DefaultFlushTime + 500*time.Millisecond)

	// Verify embeddings were generated and persisted
	// assert.Greater(t, persistCalled, 0)
	embeddingsMu.Lock()
	hasEmbeddings := len(generatedEmbeddings) > 0
	embeddingsMu.Unlock()
	assert.True(t, hasEmbeddings, "generatedEmbeddings should not be empty")

	// Verify we can retrieve the embeddings
	for _, td := range testDocs {
		embKey := append(td.key, ei.embedderSuffix...)
		val, closer, err := db.Get(embKey)
		require.NoError(t, err)
		assert.NotNil(t, val)
		closer.Close()

		// Decode and verify the embedding (with hashID prefix)
		_, embedding, _, err := vectorindex.DecodeEmbeddingWithHashID(val)
		assert.NoError(t, err)
		assert.Len(t, embedding, 3)
	}

	// Test search functionality
	searchReq := &vectorindex.SearchRequest{
		Embedding: []float32{0.0, 10.0, 5.0}, // Similar to our generated embeddings
		K:         2,
	}

	// Wait a bit more for index to be fully populated
	time.Sleep(200 * time.Millisecond)

	resp, err := ei.Search(t.Context(), searchReq)
	require.NoError(t, err)
	searchResp, ok := resp.(*vectorindex.SearchResult)
	require.True(t, ok)

	// We should get some results since we indexed documents
	assert.NotNil(t, searchResp.Hits)

	// Clean up
	cancel()
	assert.NoError(t, ei.Close())
}

func TestEmbeddingIndex_RecoveryWithWAL(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := NewEmbeddingsConfig("test_index", EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	})

	// Create and open index
	idx, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_index", config, nil)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	schema := &schema.TableSchema{}
	byteRange := types.Range{nil, nil}
	err = ei.Open(true, schema, byteRange)
	require.NoError(t, err)

	// Add some vectors directly
	batch := ei.NewBatch()
	testVectors := []struct {
		key    []byte
		vector []float32
	}{
		{[]byte("vec1"), []float32{1.0, 0.0, 0.0}},
		{[]byte("vec2"), []float32{0.0, 1.0, 0.0}},
		{[]byte("vec3"), []float32{0.0, 0.0, 1.0}},
	}
	vecBatch := ei.db.NewBatch()
	for i := range testVectors {
		k := append(bytes.Clone(testVectors[i].key), bytes.Clone(ei.embedderSuffix)...)
		e, err := vectorindex.EncodeEmbeddingWithHashID(nil, testVectors[i].vector, 0)
		require.NoError(t, err)
		require.NoError(t, vecBatch.Set(k, e, nil))
	}
	err = vecBatch.Commit(pebble.Sync)
	require.NoError(t, err)

	for _, tv := range testVectors {
		err = batch.InsertSingle(tv.key, tv.vector)
		require.NoError(t, err)
	}
	err = batch.Commit(t.Context())
	require.NoError(t, err)

	// Wait for backfill to complete before closing
	ei.WaitForBackfill(t.Context())

	// Close the index
	err = ei.Close()
	require.NoError(t, err)

	// Reopen without rebuild - should recover from WAL
	idx2, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_index", config, nil)
	require.NoError(t, err)
	ei2 := idx2.(*EmbeddingIndex)

	err = ei2.Open(false, schema, byteRange) // false = no rebuild
	require.NoError(t, err)

	// Wait for recovery
	time.Sleep(200 * time.Millisecond)

	// Verify we can search and find our vectors
	searchReq := &vectorindex.SearchRequest{
		Embedding: []float32{1.0, 0.0, 0.0},
		K:         3,
	}
	resp, err := ei2.Search(context.Background(), searchReq)
	assert.NoError(t, err)

	searchResp, ok := resp.(*vectorindex.SearchResult)
	require.True(t, ok)
	assert.NotNil(t, searchResp.Hits)

	err = ei2.Close()
	assert.NoError(t, err)
}

// TestEmbeddingIndex_EdgeCases tests various edge cases
func TestEmbeddingIndex_EdgeCases(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := NewEmbeddingsConfig("test_index", EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	})

	idx, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_index", config, nil)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	schema := &schema.TableSchema{}
	byteRange := types.Range{nil, nil}
	err = ei.Open(true, schema, byteRange)
	require.NoError(t, err)
	defer ei.Close()

	t.Run("empty batch operations", func(t *testing.T) {
		err := ei.Batch(t.Context(), nil, nil, false)
		assert.NoError(t, err)

		batch := ei.NewBatch()
		err = batch.Commit(t.Context())
		assert.NoError(t, err)
	})

	t.Run("search with empty embedding", func(t *testing.T) {
		searchReq := &vectorindex.SearchRequest{
			Embedding: []float32{},
			K:         5,
		}
		_, err = ei.Search(context.Background(), searchReq)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must specify an embedding")
	})

	t.Run("search with wrong dimension", func(t *testing.T) {
		searchReq := &vectorindex.SearchRequest{
			Embedding: []float32{1.0, 2.0}, // Wrong dimension (2 instead of 3)
			K:         5,
		}
		_, err = ei.Search(context.Background(), searchReq)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dimensionality mismatch")
	})
}

// EmbedderProviderMock summarizer for testing
type mockSummarizer struct {
	summarizeFunc func(ctx context.Context, contents [][]ai.ContentPart, _ ...ai.GenerateOption) ([]string, error)
}

func (m *mockSummarizer) SummarizeParts(
	ctx context.Context,
	contents [][]ai.ContentPart,
	_ ...ai.GenerateOption,
) ([]string, error) {
	if m.summarizeFunc != nil {
		return m.summarizeFunc(ctx, contents)
	}
	// Default implementation returns simple summaries
	result := make([]string, len(contents))
	for i, content := range contents {
		if len(content) > 0 {
			if textPart, ok := content[0].(ai.TextContent); ok {
				result[i] = "Summary of: " + textPart.Text
			} else {
				result[i] = fmt.Sprintf("Summary %d", i)
			}
		} else {
			result[i] = fmt.Sprintf("Summary %d", i)
		}
	}
	return result, nil
}

func (m *mockSummarizer) SummarizeRenderedDocs(
	ctx context.Context,
	renderedDocs []string,
	opts ...ai.GenerateOption,
) ([]string, error) {
	// Convert rendered docs to ContentPart arrays and delegate to SummarizeParts
	contents := make([][]ai.ContentPart, len(renderedDocs))
	for i, doc := range renderedDocs {
		contents[i] = []ai.ContentPart{ai.TextContent{Text: doc}}
	}
	return m.SummarizeParts(ctx, contents, opts...)
}

func TestEmbeddingIndex_WithSummarizerPipeline(t *testing.T) {
	origDefaultFlushTime := DefaultFlushTime
	DefaultFlushTime = 50 * time.Millisecond // Shorten for test speed
	t.Cleanup(func() {
		DefaultFlushTime = origDefaultFlushTime
	})
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Track both embeddings and summaries generated
	var embeddingsMu sync.Mutex
	var summariesMu sync.Mutex
	generatedEmbeddings := make(map[string][]float32)
	generatedSummaries := make(map[string]string)

	// Create a mock embedder that tracks what it generates
	mockEmb := &mockEmbedder{
		embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
			result := make([][]float32, len(values))
			for i, val := range values {
				// Generate deterministic embeddings based on content
				result[i] = []float32{float32(i), float32(len(val)), float32(len(val) % 10)}
				embeddingsMu.Lock()
				generatedEmbeddings[val] = result[i]
				embeddingsMu.Unlock()
			}
			return result, nil
		},
	}

	// Create a mock summarizer that tracks what it generates
	mockSum := &mockSummarizer{
		summarizeFunc: func(ctx context.Context, contents [][]ai.ContentPart, _ ...ai.GenerateOption) ([]string, error) {
			result := make([]string, len(contents))
			for i, content := range contents {
				if len(content) > 0 {
					if textPart, ok := content[0].(ai.TextContent); ok {
						summary := "Summary of: " + textPart.Text
						result[i] = summary
						summariesMu.Lock()
						generatedSummaries[textPart.Text] = summary
						summariesMu.Unlock()
					} else {
						result[i] = fmt.Sprintf("Summary %d", i)
					}
				} else {
					result[i] = fmt.Sprintf("Summary %d", i)
				}
			}
			return result, nil
		},
	}

	// Override the plugin registry for this test
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return mockEmb, nil
		},
	)
	ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)
	ai.RegisterDocumentSummarizer(
		ai.GeneratorProviderMock,
		func(ctx context.Context, config ai.GeneratorConfig) (ai.DocumentSummarizer, error) {
			return mockSum, nil
		},
	)
	t.Cleanup(func() {
		embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
		ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)
	})

	config := NewEmbeddingsConfig("test_summarizer_index", EmbeddingsIndexConfig{
		Dimension: 3,
		Template:  "{{title}}: {{content}}",
		Embedder: embeddings.NewEmbedderConfigFromJSON(
			"mock",
			[]byte(`{ "provider": "mock", "model": "test-embedder" }`),
		),
		Summarizer: ai.NewGeneratorConfigFromJSON(
			"mock",
			[]byte(`{ "provider": "mock", "model": "test-summarizer" }`),
		),
	})

	idx, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_summarizer_index", config, nil)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	schema := &schema.TableSchema{}
	byteRange := types.Range{nil, nil}
	err = ei.Open(true, schema, byteRange)
	require.NoError(t, err)

	// Simulate leader factory being called
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	persistCalled := 0
	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		persistCalled++
		// Actually persist the data (embeddings and summaries)
		pbatch := db.NewBatch()
		defer pbatch.Close()

		keys := make([][]byte, len(writes))
		for j := range writes {
			keys[j] = writes[j][0]
			if err := pbatch.Set(keys[j], writes[j][1], nil); err != nil {
				return err
			}
		}
		if err := pbatch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("failed to persist data: %w", err)
		}
		if err := idx.(*EmbeddingIndex).enricherCallback(ctx, keys); err != nil {
			return fmt.Errorf("signaling index: %w", err)
		}
		return nil
	}

	go func() {
		_ = ei.LeaderFactory(ctx, persistFunc)
	}()

	// Wait for enricher to be ready
	time.Sleep(100 * time.Millisecond)

	// Store test documents
	testDocs := []struct {
		key []byte
		doc map[string]any
	}{
		{
			key: []byte("doc1"),
			doc: map[string]any{
				"title":   "Document 1",
				"content": "This is the first document that needs summarization",
			},
		},
		{
			key: []byte("doc2"),
			doc: map[string]any{
				"title":   "Document 2",
				"content": "This is the second document for pipeline testing",
			},
		},
		{
			key: []byte("doc3"),
			doc: map[string]any{
				"title":   "Document 3",
				"content": "This is the third document with different content",
			},
		},
	}

	// Store compressed documents
	for _, td := range testDocs {
		docBytes, err := json.Marshal(td.doc)
		require.NoError(t, err)

		writer, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressed := writer.EncodeAll(docBytes, nil)

		err = db.Set(append(td.key, storeutils.DBRangeStart...), compressed, nil)
		require.NoError(t, err)
	}

	// Process documents through batch
	writes := make([][2][]byte, len(testDocs))
	for i, td := range testDocs {
		writes[i] = [2][]byte{td.key, {}}
	}

	err = ei.Batch(t.Context(), writes, nil, false)
	require.NoError(t, err)

	// Wait for enrichment to complete
	time.Sleep(2*DefaultFlushTime + 500*time.Millisecond)

	// Verify summaries were generated
	summariesMu.Lock()
	hasSummaries := len(generatedSummaries) > 0
	summariesMu.Unlock()
	assert.True(t, hasSummaries, "generatedSummaries should not be empty")

	// Verify embeddings were generated from summaries
	embeddingsMu.Lock()
	hasEmbeddings := len(generatedEmbeddings) > 0
	embeddingsMu.Unlock()
	assert.True(t, hasEmbeddings, "generatedEmbeddings should not be empty")

	// Verify we can retrieve the summaries
	for _, td := range testDocs {
		sumKey := append(td.key, ei.summarizerSuffix...)
		val, closer, err := db.Get(sumKey)
		require.NoError(t, err)
		assert.NotNil(t, val)
		closer.Close()
	}

	// Verify we can retrieve the embeddings (generated from summaries)
	for _, td := range testDocs {
		embKey := append(td.key, ei.embedderSuffix...)
		val, closer, err := db.Get(embKey)
		require.NoError(t, err)
		assert.NotNil(t, val)
		closer.Close()

		// Decode and verify the embedding (with hashID prefix)
		_, embedding, _, err := vectorindex.DecodeEmbeddingWithHashID(val)
		assert.NoError(t, err)
		assert.Len(t, embedding, 3)
	}

	// Test search functionality with pipeline-generated embeddings
	searchReq := &vectorindex.SearchRequest{
		Embedding: []float32{0.0, 10.0, 5.0}, // Similar to our generated embeddings
		K:         2,
	}

	// Wait a bit more for index to be fully populated
	time.Sleep(200 * time.Millisecond)

	resp, err := ei.Search(t.Context(), searchReq)
	require.NoError(t, err)
	searchResp, ok := resp.(*vectorindex.SearchResult)
	require.True(t, ok)

	// We should get some results since we indexed documents through the pipeline
	assert.NotNil(t, searchResp.Hits)

	// Clean up
	cancel()
	assert.NoError(t, ei.Close())
}
