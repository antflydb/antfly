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
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// setupTestDBWithIndex creates a test DB with an embedding index configured
func setupTestDBWithIndex(
	t *testing.T,
	dimension int,
	field string,
	template string,
) (*DBImpl, string) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger: lg,
	}

	// Open the database
	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))

	// Create schema
	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
						"title":   map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Create embedding index config
	indexConfig := indexes.EmbeddingsIndexConfig{
		Dimension: dimension,
		Field:     field,
		Template:  template,
	}

	idxCfg := indexes.NewEmbeddingsConfig("test_idx", indexConfig)

	// Initialize index manager
	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	// Create the index
	require.NoError(t, indexManager.Register("test_idx", false, *idxCfg))
	require.NoError(t, indexManager.Start(false))

	return db, dir
}

func TestBatch_UserProvidedEmbeddings(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Prepare document with user-provided embeddings
	doc := map[string]any{
		"content": "test content",
		"_embeddings": map[string]any{
			"test_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write the document
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Verify the embedding was stored with correct format
	embKey := append(key, []byte(":i:test_idx:e")...)
	embData, closer, err := db.pdb.Get(embKey)
	require.NoError(t, err)
	defer closer.Close()

	// Decode the stored embedding (format: [hashID][vector])
	storedHashID, storedEmbedding, _, err := vectorindex.DecodeEmbeddingWithHashID(embData)
	require.NoError(t, err)
	require.Len(t, storedEmbedding, 3)
	assert.InDelta(t, 1.0, storedEmbedding[0], 0.001)
	assert.InDelta(t, 2.0, storedEmbedding[1], 0.001)
	assert.InDelta(t, 3.0, storedEmbedding[2], 0.001)

	// Calculate expected hashID
	expectedHashID := xxhash.Sum64String("test content")
	assert.Equal(t, expectedHashID, storedHashID)

	// Verify document content (Get adds _embeddings back from Pebble)
	storedDoc, err := db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "test content", storedDoc["content"])
	// The _embeddings field should be populated by Get() from the stored embedding
	assert.Contains(t, storedDoc, "_embeddings")
}

func TestBatch_EmbeddingDimensionMismatch(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	// Prepare document with wrong dimension
	doc := map[string]any{
		"content": "test content",
		"_embeddings": map[string]any{
			"test_idx": []any{1.0, 2.0}, // Only 2 dimensions, expected 3
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write should fail
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

func TestBatch_EmbeddingNonExistentIndex(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	// Prepare document with embedding for non-existent index
	doc := map[string]any{
		"content": "test content",
		"_embeddings": map[string]any{
			"nonexistent_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write should fail
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index not found")
}

func TestBatch_EmbeddingsRemovedFromDocument(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Prepare document with embeddings
	doc := map[string]any{
		"content": "test content",
		"title":   "test title",
		"_embeddings": map[string]any{
			"test_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write the document
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Retrieve and verify document content
	storedDoc, err := db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "test content", storedDoc["content"])
	assert.Equal(t, "test title", storedDoc["title"])
	// Get() adds _embeddings back from Pebble storage
	assert.Contains(t, storedDoc, "_embeddings")
}

func TestBatch_MultipleEmbeddingsOneDocument(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger: lg,
	}

	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Create schema
	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
						"title":   map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Create two different embedding indexes
	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	idxCfg1 := indexes.NewEmbeddingsConfig("idx1", indexes.EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	})
	idxCfg2 := indexes.NewEmbeddingsConfig("idx2", indexes.EmbeddingsIndexConfig{
		Dimension: 4,
		Field:     "title",
	})

	require.NoError(t, indexManager.Register("idx1", false, *idxCfg1))
	require.NoError(t, indexManager.Register("idx2", false, *idxCfg2))
	require.NoError(t, indexManager.Start(false))

	// Prepare document with embeddings for both indexes
	doc := map[string]any{
		"content": "test content",
		"title":   "test title",
		"_embeddings": map[string]any{
			"idx1": []any{1.0, 2.0, 3.0},
			"idx2": []any{4.0, 5.0, 6.0, 7.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write the document
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Verify both embeddings were stored
	embKey1 := append(key, []byte(":i:idx1:e")...)
	embData1, closer1, err := db.pdb.Get(embKey1)
	require.NoError(t, err)
	defer closer1.Close()

	_, storedEmbedding1, _, err := vectorindex.DecodeEmbeddingWithHashID(embData1)
	require.NoError(t, err)
	require.Len(t, storedEmbedding1, 3)

	embKey2 := append(key, []byte(":i:idx2:e")...)
	embData2, closer2, err := db.pdb.Get(embKey2)
	require.NoError(t, err)
	defer closer2.Close()

	_, storedEmbedding2, _, err := vectorindex.DecodeEmbeddingWithHashID(embData2)
	require.NoError(t, err)
	require.Len(t, storedEmbedding2, 4)

	// Verify document was stored correctly
	storedDoc, err := db.Get(ctx, key)
	require.NoError(t, err)
	// Get() adds _embeddings back from Pebble storage
	assert.Contains(t, storedDoc, "_embeddings")
}

func TestBatch_NoPerformanceRegressionWithoutSpecialFields(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	// Prepare a normal document without _embeddings
	doc := map[string]any{
		"content": "test content",
		"title":   "test title",
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// This should take the fast path (byte scan returns false)
	// and not decode the JSON unnecessarily
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Verify document was stored correctly
	ctx := context.Background()
	storedDoc, err := db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, "test content", storedDoc["content"])
	assert.Equal(t, "test title", storedDoc["title"])
}

func TestBatch_HashIDGenerationFromTemplate(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "", "Title: {{title}}, Content: {{content}}")
	defer db.Close()
	defer os.RemoveAll(dir)

	// Prepare document
	doc := map[string]any{
		"content": "test content",
		"title":   "test title",
		"_embeddings": map[string]any{
			"test_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write the document
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Verify hashID was generated from template
	embKey := append(key, []byte(":i:test_idx:e")...)
	embData, closer, err := db.pdb.Get(embKey)
	require.NoError(t, err)
	defer closer.Close()

	// Decode the stored embedding (format: [hashID][vector])
	storedHashID, _, _, err := vectorindex.DecodeEmbeddingWithHashID(embData)
	require.NoError(t, err)

	// Calculate expected hashID from rendered template
	expectedPrompt := "Title: test title, Content: test content"
	expectedHashID := xxhash.Sum64String(expectedPrompt)
	assert.Equal(t, expectedHashID, storedHashID)
}

func TestBatch_HashIDGenerationFromField(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	// Prepare document
	doc := map[string]any{
		"content": "test content",
		"title":   "test title",
		"_embeddings": map[string]any{
			"test_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write the document
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Verify hashID was generated from field
	embKey := append(key, []byte(":i:test_idx:e")...)
	embData, closer, err := db.pdb.Get(embKey)
	require.NoError(t, err)
	defer closer.Close()

	// Decode the stored embedding (format: [hashID][vector])
	storedHashID, _, _, err := vectorindex.DecodeEmbeddingWithHashID(embData)
	require.NoError(t, err)

	// Calculate expected hashID from field
	expectedHashID := xxhash.Sum64String("test content")
	assert.Equal(t, expectedHashID, storedHashID)
}

func TestBatch_NonEmbeddingIndexRejectsEmbeddings(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger: lg,
	}

	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	defer db.Close()
	defer os.RemoveAll(dir)

	// Create schema
	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Create a full-text index (not an embedding index)
	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	idxCfg := indexes.NewFullTextIndexConfig("fulltext_idx", true)
	require.NoError(t, indexManager.Register("fulltext_idx", false, *idxCfg))
	require.NoError(t, indexManager.Start(false))

	// Try to send embeddings for the full-text index
	doc := map[string]any{
		"content": "test content",
		"_embeddings": map[string]any{
			"fulltext_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write should fail because the index doesn't implement EmbeddingsPreProcessor
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support embeddings")
}

func TestEmbeddingsPreProcessor_Interface(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db, err := pebble.Open(filepath.Join(dir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Create embedding index
	idxCfg := indexes.NewEmbeddingsConfig("test_idx", indexes.EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	})

	idx, err := indexes.NewEmbeddingIndex(lg, &common.Config{}, db, dir, "test_idx", idxCfg, nil)
	require.NoError(t, err)

	// Verify it implements EmbeddingsPreProcessor
	embPreProcessor, ok := idx.(indexes.EmbeddingsPreProcessor)
	require.True(t, ok, "EmbeddingIndex should implement EmbeddingsPreProcessor")

	// Test GetDimension
	assert.Equal(t, 3, embPreProcessor.GetDimension())

	// Test RenderPrompt
	doc := map[string]any{
		"content": "test content",
	}

	prompt, hashID, err := embPreProcessor.RenderPrompt(doc)
	require.NoError(t, err)
	assert.Equal(t, "test content", prompt)
	assert.Equal(t, xxhash.Sum64String("test content"), hashID)
}

func TestBatch_HashIDGenerationFromNestedField(t *testing.T) {
	// Test JSONPath support for nested fields
	db, dir := setupTestDBWithIndex(t, 3, "metadata.title", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	// Prepare document with nested structure
	doc := map[string]any{
		"content": "test content",
		"metadata": map[string]any{
			"title":  "nested title",
			"author": "test author",
		},
		"_embeddings": map[string]any{
			"test_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write the document
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Verify hashID was generated from nested field
	embKey := append(key, []byte(":i:test_idx:e")...)
	embData, closer, err := db.pdb.Get(embKey)
	require.NoError(t, err)
	defer closer.Close()

	// Decode the stored embedding (format: [hashID][vector])
	storedHashID, _, _, err := vectorindex.DecodeEmbeddingWithHashID(embData)
	require.NoError(t, err)

	// Calculate expected hashID from nested field
	expectedHashID := xxhash.Sum64String("nested title")
	assert.Equal(t, expectedHashID, storedHashID)
}

func TestBatch_EmbeddingIndexedInVectorIndex(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer os.RemoveAll(dir)
	defer db.Close()

	ctx := context.Background()

	// Prepare document with user-provided embedding
	doc := map[string]any{
		"content": "searchable content",
		"_embeddings": map[string]any{
			"test_idx": []any{1.0, 2.0, 3.0},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write the document with SyncLevelAknn to wait for vector indexing
	// This is safe because we're providing user embeddings (no enrichment needed)
	// so there are no promptKeys requiring pre-enrichment in dbWrapper
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelEmbeddings)
	// SyncLevelAknn returns ErrPartialSuccess (aknn synced, other indexes async)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Get the index
	idx := db.indexManager.GetIndex("test_idx")
	require.NotNil(t, idx)

	embIdx, ok := idx.(*indexes.EmbeddingIndex)
	require.True(t, ok, "Index should be an EmbeddingIndex")

	// Search for a nearby vector
	searchVector := []float32{1.1, 2.1, 3.1}
	searchReq := &vectorindex.SearchRequest{
		Embedding: searchVector,
		K:         10,
	}

	// Wait for indexing to complete (indexing is async even with sync=true)
	// Note: 30 second timeout to handle contention when running full test suite
	var searchResp *vectorindex.SearchResult
	require.Eventually(t, func() bool {
		result, err := embIdx.Search(ctx, searchReq)
		if err != nil {
			t.Logf("Search error: %v", err)
			return false
		}

		resp, ok := result.(*vectorindex.SearchResult)
		if !ok || len(resp.Hits) == 0 {
			t.Logf("No results yet, continuing to wait...")
			return false
		}

		searchResp = resp
		return true
	}, 30*time.Second, 200*time.Millisecond, "Should find indexed vector within 30 seconds")

	// Should find our indexed vector
	require.GreaterOrEqual(t, len(searchResp.Hits), 1, "Should find at least one result")

	// The first result should be our key (without suffix)
	assert.Equal(t, string(key), searchResp.Hits[0].ID)
}

func TestBatch_CosineEmbeddingsIndexAcceptsNormalizedVectorsAcrossInternalSplits(t *testing.T) {
	const (
		indexName  = "test_cosine_idx"
		numVectors = 20_000
		dims       = 32
		batchSize  = 250
	)

	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger: lg,
	}
	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	defer db.Close()
	defer os.RemoveAll(dir)

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	idxCfg := indexes.NewEmbeddingsConfig(indexName, indexes.EmbeddingsIndexConfig{
		Dimension:      dims,
		Field:          "content",
		DistanceMetric: indexes.DistanceMetricCosine,
	})
	require.NoError(t, indexManager.Register(indexName, false, *idxCfg))
	require.NoError(t, indexManager.Start(false))
	idx := db.indexManager.GetIndex(indexName)
	require.NotNil(t, idx)
	embIdx, ok := idx.(*indexes.EmbeddingIndex)
	require.True(t, ok, "Index should be an EmbeddingIndex")

	rng := rand.New(rand.NewPCG(42, 1024))
	for start := 0; start < numVectors; start += batchSize {
		end := min(start+batchSize, numVectors)
		writes := make([][2][]byte, 0, end-start)
		for i := start; i < end; i++ {
			embedding := make([]float32, dims)
			for j := range embedding {
				embedding[j] = rng.Float32()*2 - 1
			}
			vec.NormalizeFloat32(embedding)

			docBytes, err := json.Marshal(map[string]any{
				"content": fmt.Sprintf("document %d", i),
				"_embeddings": map[string]any{
					indexName: embedding,
				},
			})
			require.NoError(t, err)
			writes = append(writes, [2][]byte{
				[]byte(fmt.Sprintf("doc/%05d", i)),
				docBytes,
			})
		}

		err := db.Batch(t.Context(), writes, nil, Op_SyncLevelEmbeddings)
		if err != nil && !errors.Is(err, ErrPartialSuccess) {
			require.NoError(t, err)
		}
	}

	waitForEmbeddingIndexTotalIndexed(t, embIdx, numVectors, 60*time.Second, 100*time.Millisecond)

	for _, key := range []string{"doc/00000", "doc/10000", "doc/19999"} {
		doc, err := db.Get(t.Context(), []byte(key))
		require.NoError(t, err)
		require.NotNil(t, doc)
	}
}

func TestVectorSearch_EndToEndRecall(t *testing.T) {
	db, dir := setupTestDBWithFullTextAndEmbeddings(t, 20)
	defer db.Close()
	defer os.RemoveAll(dir)

	const (
		indexName     = "test_embedding_idx"
		dataCount     = 900
		queryCount    = 100
		topK          = 10
		minAvgRecall  = 0.95
		waitTimeout   = 30 * time.Second
		waitFrequency = 200 * time.Millisecond
	)

	ctx := context.Background()
	dataset := testutils.LoadDataset(t, testutils.RandomDataset20d)
	dataVectors := dataset.Slice(0, dataCount)
	queryVectors := dataset.Slice(dataCount, queryCount)

	writes := make([][2][]byte, 0, dataCount)
	dataKeys := make([]string, 0, dataCount)
	for i := range dataCount {
		key := []byte(fmt.Sprintf("doc:%04d", i))
		dataKeys = append(dataKeys, string(key))

		docJSON, err := json.Marshal(map[string]any{
			"content": fmt.Sprintf("document %d", i),
			"_embeddings": map[string]any{
				indexName: slices.Clone(dataVectors.At(i)),
			},
		})
		require.NoError(t, err)
		writes = append(writes, [2][]byte{key, docJSON})
	}

	err := db.Batch(ctx, writes, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	idx := db.indexManager.GetIndex(indexName)
	require.NotNil(t, idx)
	embIdx, ok := idx.(*indexes.EmbeddingIndex)
	require.True(t, ok, "Index should be an EmbeddingIndex")

	waitForEmbeddingIndexTotalIndexed(t, embIdx, dataCount, waitTimeout, waitFrequency)

	avgRecall, err := calculateDBVectorRecall(
		ctx,
		db,
		indexName,
		queryVectors,
		dataVectors,
		dataKeys,
		topK,
	)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, avgRecall, minAvgRecall,
		"expected average recall >= %.2f, got %.4f", minAvgRecall, avgRecall)
}

func calculateDBVectorRecall(
	ctx context.Context,
	db *DBImpl,
	indexName string,
	queryVectors *vector.Set,
	dataVectors *vector.Set,
	dataKeys []string,
	topK int,
) (float64, error) {
	var recallSum float64

	for i := range int(queryVectors.GetCount()) {
		queryVec := slices.Clone(queryVectors.At(i))
		reqBytes, err := json.Marshal(indexes.RemoteIndexSearchRequest{
			Limit: topK,
			VectorSearches: map[string]vector.T{
				indexName: queryVec,
			},
		})
		if err != nil {
			return 0, fmt.Errorf("marshal vector search request %d: %w", i, err)
		}

		respBytes, err := db.Search(ctx, reqBytes)
		if err != nil {
			return 0, fmt.Errorf("search query %d: %w", i, err)
		}

		var result indexes.RemoteIndexSearchResult
		if err := json.Unmarshal(respBytes, &result); err != nil {
			return 0, fmt.Errorf("unmarshal vector search result %d: %w", i, err)
		}

		searchResult := result.VectorSearchResult[indexName]
		if searchResult == nil {
			return 0, fmt.Errorf("vector search result missing for index %q", indexName)
		}
		if len(searchResult.Hits) < topK {
			return 0, fmt.Errorf("vector search returned %d hits, expected at least %d", len(searchResult.Hits), topK)
		}

		prediction := make([]string, 0, topK)
		for _, hit := range searchResult.Hits[:topK] {
			prediction = append(prediction, hit.ID)
		}

		truth := testutils.CalculateTruth(
			topK,
			vector.DistanceMetric_L2Squared,
			queryVec,
			dataVectors,
			dataKeys,
		)
		recallSum += testutils.CalculateRecall(prediction, truth)
	}

	return recallSum / float64(queryVectors.GetCount()), nil
}

func waitForEmbeddingIndexTotalIndexed(
	t *testing.T,
	embIdx *indexes.EmbeddingIndex,
	expected int,
	timeout time.Duration,
	frequency time.Duration,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		stats, err := embIdx.Stats().AsEmbeddingsIndexStats()
		if err != nil {
			t.Logf("embedding stats error: %v", err)
			return false
		}
		if stats.TotalIndexed < uint64(expected) {
			t.Logf("waiting for vector index population: %d/%d", stats.TotalIndexed, expected)
			return false
		}
		return true
	}, timeout, frequency, "expected all vectors to be indexed before measuring recall")
}

func TestBatch_UserProvidedEmbeddingsAsyncDoesNotOvercountIndexedVectors(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	idx := db.indexManager.GetIndex("test_idx")
	require.NotNil(t, idx)
	embIdx, ok := idx.(*indexes.EmbeddingIndex)
	require.True(t, ok)

	const numDocs = 2048
	writes := make([][2][]byte, 0, numDocs)
	for i := range numDocs {
		doc := map[string]any{
			"content": fmt.Sprintf("doc-%03d", i),
			"_embeddings": map[string]any{
				"test_idx": []any{float64(i), float64(i + 1), float64(i + 2)},
			},
		}
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(fmt.Sprintf("doc/%03d", i)), docJSON})
	}

	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	waitForEmbeddingIndexTotalIndexed(t, embIdx, numDocs, 30*time.Second, 100*time.Millisecond)

	// Give the async pipeline a little extra time to surface duplicate replays.
	time.Sleep(500 * time.Millisecond)

	stats, err := embIdx.Stats().AsEmbeddingsIndexStats()
	require.NoError(t, err)
	assert.EqualValues(t, numDocs, stats.TotalIndexed, "async indexing should not overcount indexed vectors")
}

func TestBatch_UserProvidedEmbeddingsManyAsyncBatchesDoNotOvercountIndexedVectors(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 3, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	idx := db.indexManager.GetIndex("test_idx")
	require.NotNil(t, idx)
	embIdx, ok := idx.(*indexes.EmbeddingIndex)
	require.True(t, ok)

	const (
		numDocs   = 4096
		batchSize = 32
	)

	for start := 0; start < numDocs; start += batchSize {
		end := min(start+batchSize, numDocs)
		writes := make([][2][]byte, 0, end-start)
		for i := start; i < end; i++ {
			doc := map[string]any{
				"content": fmt.Sprintf("doc-%04d", i),
				"_embeddings": map[string]any{
					"test_idx": []any{float64(i), float64(i + 1), float64(i + 2)},
				},
			}
			docJSON, err := json.Marshal(doc)
			require.NoError(t, err)
			writes = append(writes, [2][]byte{[]byte(fmt.Sprintf("doc/%04d", i)), docJSON})
		}
		err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
		require.NoError(t, err)
	}

	waitForEmbeddingIndexTotalIndexed(t, embIdx, numDocs, 30*time.Second, 100*time.Millisecond)
	time.Sleep(time.Second)

	stats, err := embIdx.Stats().AsEmbeddingsIndexStats()
	require.NoError(t, err)
	assert.EqualValues(t, numDocs, stats.TotalIndexed, "many async batches should not overcount indexed vectors")
}

func TestBatch_UserProvidedCosine1536DAsyncQuantizedDoesNotPanic(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{logger: lg}
	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	defer db.Close()

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	idxCfg := indexes.NewEmbeddingsConfig("test_idx", indexes.EmbeddingsIndexConfig{
		Dimension:      1536,
		Field:          "content",
		DistanceMetric: indexes.DistanceMetricCosine,
	})
	require.NoError(t, indexManager.Register("test_idx", false, *idxCfg))
	require.NoError(t, indexManager.Start(false))

	idx := db.indexManager.GetIndex("test_idx")
	require.NotNil(t, idx)
	embIdx, ok := idx.(*indexes.EmbeddingIndex)
	require.True(t, ok)

	const numDocs = 512
	for start := 0; start < numDocs; start += 64 {
		end := min(start+64, numDocs)
		writes := make([][2][]byte, 0, end-start)
		for i := start; i < end; i++ {
			vec := make([]any, 1536)
			var norm float64
			for j := range vec {
				v := float64((i+j)%17 + 1)
				vec[j] = v
				norm += v * v
			}
			norm = math.Sqrt(norm)
			for j := range vec {
				vec[j] = vec[j].(float64) / norm
			}
			doc := map[string]any{
				"content": fmt.Sprintf("doc-%04d", i),
				"_embeddings": map[string]any{"test_idx": vec},
			}
			docJSON, err := json.Marshal(doc)
			require.NoError(t, err)
			writes = append(writes, [2][]byte{[]byte(fmt.Sprintf("doc/%04d", i)), docJSON})
		}
		require.NoError(t, db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite))
	}

	waitForEmbeddingIndexTotalIndexed(t, embIdx, numDocs, 30*time.Second, 100*time.Millisecond)
	stats, err := embIdx.Stats().AsEmbeddingsIndexStats()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, stats.TotalIndexed, uint64(numDocs))
}

func TestBatch_EmbeddingTypeConversions(t *testing.T) {
	db, dir := setupTestDBWithIndex(t, 4, "content", "")
	defer db.Close()
	defer os.RemoveAll(dir)

	// Test various numeric types that should convert to float32
	doc := map[string]any{
		"content": "test content",
		"_embeddings": map[string]any{
			"test_idx": []any{
				int(1),       // int
				float64(2.5), // float64
				int32(3),     // int32
				float32(4.2), // float32
			},
		},
	}

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("test_key")

	// Write should succeed with type conversions
	err = db.Batch(t.Context(), [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Verify the embedding was stored
	embKey := append(key, []byte(":i:test_idx:e")...)
	embData, closer, err := db.pdb.Get(embKey)
	require.NoError(t, err)
	defer closer.Close()

	_, storedEmbedding, _, err := vectorindex.DecodeEmbeddingWithHashID(embData)
	require.NoError(t, err)
	require.Len(t, storedEmbedding, 4)
	assert.InDelta(t, 1.0, storedEmbedding[0], 0.001)
	assert.InDelta(t, 2.5, storedEmbedding[1], 0.001)
	assert.InDelta(t, 3.0, storedEmbedding[2], 0.001)
	assert.InDelta(t, 4.2, storedEmbedding[3], 0.001)
}

// setupTestDBWithFullTextAndEmbeddings creates a test DB with both full-text and embedding indexes
func setupTestDBWithFullTextAndEmbeddings(t *testing.T, dimension int) (*DBImpl, string) {
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
						"content": map[string]any{"type": "string"},
						"title":   map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	// Register full-text index
	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index_v0", false)
	require.NoError(t, indexManager.Register("full_text_index_v0", false, *fullTextConfig))

	// Register embedding index
	embeddingConfig := indexes.NewEmbeddingsConfig("test_embedding_idx", indexes.EmbeddingsIndexConfig{
		Dimension: dimension,
		Field:     "content",
	})
	require.NoError(t, indexManager.Register("test_embedding_idx", false, *embeddingConfig))

	require.NoError(t, indexManager.Start(false))

	return db, dir
}

// TestVectorSearch_FilterQueryNoMatchesReturnsEmpty tests that when a filter_query
// matches no documents, the vector search returns 0 results instead of ignoring the filter.
func TestVectorSearch_FilterQueryNoMatchesReturnsEmpty(t *testing.T) {
	db, dir := setupTestDBWithFullTextAndEmbeddings(t, 3)
	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Insert a document with content "hello world" and an embedding
	doc := map[string]any{
		"content": "hello world",
		"title":   "test document",
		"_embeddings": map[string]any{
			"test_embedding_idx": []any{1.0, 0.0, 0.0},
		},
	}
	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	key := []byte("doc1")
	// Use SyncLevelAknn to wait for the vector index write to complete
	err = db.Batch(ctx, [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Search with a filter_query that DOES match ("hello" should be found in content)
	searchReqMatching := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"test_embedding_idx": {1.0, 0.0, 0.0},
		},
		FilterQuery: json.RawMessage(`{"match":"hello","field":"content"}`),
		Limit:       10,
	}
	reqBytes, err := json.Marshal(searchReqMatching)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var resultMatching indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &resultMatching))
	assert.NotNil(t, resultMatching.VectorSearchResult["test_embedding_idx"])
	assert.Len(t, resultMatching.VectorSearchResult["test_embedding_idx"].Hits, 1,
		"filter_query matching documents should return results")

	// Search with a filter_query that does NOT match ("nonexistent" is not in the document)
	searchReqNoMatch := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"test_embedding_idx": {1.0, 0.0, 0.0},
		},
		FilterQuery: json.RawMessage(`{"match":"nonexistent_term_xyz","field":"content"}`),
		Limit:       10,
	}
	reqBytesNoMatch, err := json.Marshal(searchReqNoMatch)
	require.NoError(t, err)

	resBytesNoMatch, err := db.Search(ctx, reqBytesNoMatch)
	require.NoError(t, err)

	var resultNoMatch indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytesNoMatch, &resultNoMatch))
	assert.NotNil(t, resultNoMatch.VectorSearchResult["test_embedding_idx"])
	assert.Empty(t, resultNoMatch.VectorSearchResult["test_embedding_idx"].Hits,
		"filter_query matching NO documents should return 0 results, not all results")
}

func TestVectorSearch_WithAggregations(t *testing.T) {
	// Create a custom setup with category field in schema
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
						"content":  map[string]any{"type": "string"},
						"title":    map[string]any{"type": "string"},
						"category": map[string]any{"type": "string", "x-antfly-types": []string{"keyword"}},
					},
				},
			},
		},
	}

	indexManager, err := NewIndexManager(
		lg,
		&common.Config{},
		db.pdb,
		dir,
		tableSchema,
		types.Range{nil, []byte{0xFF}},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	// Register full-text index
	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index_v0", false)
	require.NoError(t, indexManager.Register("full_text_index_v0", false, *fullTextConfig))

	// Register embedding index
	embeddingConfig := indexes.NewEmbeddingsConfig("test_embedding_idx", indexes.EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	})
	require.NoError(t, indexManager.Register("test_embedding_idx", false, *embeddingConfig))

	require.NoError(t, indexManager.Start(false))

	defer db.Close()
	defer os.RemoveAll(dir)

	ctx := context.Background()

	// Insert multiple documents with different categories
	docs := []struct {
		id        string
		content   string
		category  string
		embedding []any
	}{
		{"doc1", "machine learning algorithms", "tech", []any{1.0, 0.0, 0.0}},
		{"doc2", "deep learning neural networks", "tech", []any{0.9, 0.1, 0.0}},
		{"doc3", "cooking recipes pasta", "food", []any{0.0, 1.0, 0.0}},
		{"doc4", "italian cuisine pizza", "food", []any{0.0, 0.9, 0.1}},
		{"doc5", "sports football soccer", "sports", []any{0.0, 0.0, 1.0}},
	}

	for _, d := range docs {
		doc := map[string]any{
			"content":  d.content,
			"category": d.category,
			"_embeddings": map[string]any{
				"test_embedding_idx": d.embedding,
			},
		}
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		key := []byte(d.id)
		err = db.Batch(ctx, [][2][]byte{{key, docJSON}}, nil, Op_SyncLevelEmbeddings)
		if err != nil && !errors.Is(err, ErrPartialSuccess) {
			require.NoError(t, err)
		}
	}

	// Search with vector query and aggregations on category field
	searchReq := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"test_embedding_idx": {1.0, 0.0, 0.0}, // Should match tech docs best
		},
		Limit: 5,
		AggregationRequests: indexes.AggregationRequests{
			"category_agg": &indexes.AggregationRequest{
				Type:  "terms",
				Field: "category",
				Size:  10,
			},
		},
	}
	reqBytes, err := json.Marshal(searchReq)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var result indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &result))

	// Debug logging
	t.Logf("VectorSearchResult: %v", result.VectorSearchResult != nil)
	t.Logf("AggregationResults: %v", result.AggregationResults != nil)
	t.Logf("FusionResult: %v", result.FusionResult != nil)
	t.Logf("BleveSearchResult: %v", result.BleveSearchResult != nil)

	// Verify we got vector search results
	assert.NotNil(t, result.VectorSearchResult["test_embedding_idx"])
	assert.NotEmpty(t, result.VectorSearchResult["test_embedding_idx"].Hits,
		"should have vector search hits")

	// Verify we got aggregation results
	require.NotNil(t, result.AggregationResults, "aggregations should be computed for vector search")
	categoryAgg, exists := result.AggregationResults["category_agg"]
	assert.True(t, exists, "category_agg should exist in results")
	assert.NotNil(t, categoryAgg, "category aggregation should not be nil")

	// Verify aggregation contains categories from matched documents
	// Since we're searching for tech-related content, we should see "tech" in results
	buckets := categoryAgg.Buckets
	assert.NotEmpty(t, buckets, "aggregation should have buckets")

	// Log aggregation results for debugging
	t.Logf("Aggregation buckets: %d", len(buckets))
	for _, bucket := range buckets {
		t.Logf("  Category: %v, Count: %d", bucket.Key, bucket.Count)
	}
}
