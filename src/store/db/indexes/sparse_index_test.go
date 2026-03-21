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
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/time/rate"
)

func newSparseTestConfig(field string) *IndexConfig {
	cfg := &IndexConfig{
		Type: IndexTypeEmbeddings,
		Name: "test_sparse",
	}
	_ = cfg.FromEmbeddingsIndexConfig(EmbeddingsIndexConfig{
		Field: field,
	})
	return cfg
}

// setupSparseIndex creates a sparse index backed by a temp Pebble DB, opens it,
// and returns both the index and the underlying DB (for injecting test data).
func setupSparseIndex(t *testing.T) (*SparseIndex, *pebble.DB) {
	t.Helper()
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	idx, err := NewSparseIndex(
		logger,
		nil,
		db,
		tempDir,
		"test_sparse",
		newSparseTestConfig("content"),
		nil, // no shared cache
	)
	require.NoError(t, err)

	si := idx.(*SparseIndex)
	err = si.Open(true, &schema.TableSchema{}, types.Range{nil, nil})
	require.NoError(t, err)
	t.Cleanup(func() { si.Close() })

	return si, db
}

// writeSparseEmbedding stores a sparse embedding key in Pebble the way the DB
// apply loop would: <docKey><sparseSuffix> → [hashID uint64][encoded SparseVec].
func writeSparseEmbedding(t *testing.T, db *pebble.DB, docKey []byte, suffix []byte, sv *vector.SparseVector) {
	t.Helper()
	key := append(bytes.Clone(docKey), suffix...)
	encoded := EncodeSparseVec(sv)

	// Prepend 8-byte hashID (just use zero for tests)
	value := make([]byte, 8+len(encoded))
	binary.LittleEndian.PutUint64(value[:8], 0)
	copy(value[8:], encoded)

	require.NoError(t, db.Set(key, value, nil))
}

// writeCompressedDoc stores a compressed document in Pebble (like the DB does).
func writeCompressedDoc(t *testing.T, db *pebble.DB, docKey []byte, doc map[string]any) {
	t.Helper()
	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)

	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := writer.EncodeAll(docBytes, nil)

	require.NoError(t, db.Set(append(bytes.Clone(docKey), storeutils.DBRangeStart...), compressed, nil))
}

func TestNewSparseIndex(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name    string
		config  EmbeddingsIndexConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid config with field",
			config:  EmbeddingsIndexConfig{Field: "content"},
			wantErr: false,
		},
		{
			name:    "valid config with template",
			config:  EmbeddingsIndexConfig{Template: "{{content}}"},
			wantErr: false,
		},
		{
			name:    "missing field and template",
			config:  EmbeddingsIndexConfig{},
			wantErr: true,
			errMsg:  "field or template must be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &IndexConfig{Type: IndexTypeEmbeddings, Name: tt.name}
			_ = cfg.FromEmbeddingsIndexConfig(tt.config)

			idx, err := NewSparseIndex(logger, nil, db, tempDir, tt.name, cfg, nil)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				si := idx.(*SparseIndex)
				assert.Equal(t, IndexTypeEmbeddings, si.Type())
				assert.Equal(t, 0, si.GetDimension())
			}
		})
	}
}

func TestSparseIndex_Open(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	cfg := newSparseTestConfig("content")
	idx, err := NewSparseIndex(logger, nil, db, tempDir, "test_sparse", cfg, nil)
	require.NoError(t, err)
	si := idx.(*SparseIndex)

	// Open with rebuild
	err = si.Open(true, &schema.TableSchema{}, types.Range{nil, nil})
	require.NoError(t, err)
	assert.NotNil(t, si.sparseIdx)
	assert.NotNil(t, si.walBuf)

	// Close and reopen without rebuild
	err = si.Close()
	require.NoError(t, err)

	si.egCtx, si.egCancel = context.WithCancel(context.TODO())
	err = si.Open(false, &schema.TableSchema{}, types.Range{nil, nil})
	require.NoError(t, err)
	assert.NotNil(t, si.sparseIdx)

	waitDone := make(chan struct{})
	go func() {
		si.WaitForBackfill(context.Background())
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("WaitForBackfill blocked after sparse reopen without rebuild")
	}
}

func TestSparseIndex_BatchAndSearch(t *testing.T) {
	si, db := setupSparseIndex(t)

	// Write sparse embeddings directly to Pebble (simulating DB apply loop)
	writeSparseEmbedding(t, db, []byte("doc1"), si.sparseSuffix, vector.NewSparseVector(
		[]uint32{10, 20, 30}, []float32{2.5, 1.0, 0.5},
	))
	writeSparseEmbedding(t, db, []byte("doc2"), si.sparseSuffix, vector.NewSparseVector(
		[]uint32{10, 40, 50}, []float32{0.1, 3.0, 1.5},
	))
	writeSparseEmbedding(t, db, []byte("doc3"), si.sparseSuffix, vector.NewSparseVector(
		[]uint32{10, 20, 60}, []float32{1.8, 0.8, 2.0},
	))

	// Batch: feed the sparse suffix keys as writes
	writes := [][2][]byte{
		{append(bytes.Clone([]byte("doc1")), si.sparseSuffix...), nil},
		{append(bytes.Clone([]byte("doc2")), si.sparseSuffix...), nil},
		{append(bytes.Clone([]byte("doc3")), si.sparseSuffix...), nil},
	}
	err := si.Batch(t.Context(), writes, nil, false)
	require.NoError(t, err)

	// Search with a query vector that overlaps with doc1 and doc3 on terms 10 and 20
	result, err := si.Search(t.Context(), &SparseSearchRequest{
		QueryVec: vector.NewSparseVector([]uint32{10, 20}, []float32{1.0, 1.0}),
		K:        3,
	})
	require.NoError(t, err)

	sr := result.(*vectorindex.SearchResult)
	require.NotEmpty(t, sr.Hits, "expected search hits")

	// doc1: 2.5*1.0 + 1.0*1.0 = 3.5
	// doc3: 1.8*1.0 + 0.8*1.0 = 2.6
	// doc2: 0.1*1.0            = 0.1 (only term 10 matches)
	assert.Equal(t, "doc1", sr.Hits[0].ID)
	assert.Equal(t, "doc3", sr.Hits[1].ID)
	assert.Equal(t, "doc2", sr.Hits[2].ID)

	// Verify scores are reasonable (within quantization tolerance)
	assert.InDelta(t, 3.5, float64(sr.Hits[0].Score), 0.15)
	assert.InDelta(t, 2.6, float64(sr.Hits[1].Score), 0.15)
	assert.InDelta(t, 0.1, float64(sr.Hits[2].Score), 0.05)
}

func TestSparseIndex_SearchEmpty(t *testing.T) {
	si, _ := setupSparseIndex(t)

	// Search on empty index should return empty results, not error
	result, err := si.Search(t.Context(), &SparseSearchRequest{
		QueryVec: vector.NewSparseVector([]uint32{10, 20}, []float32{1.0, 1.0}),
		K:        10,
	})
	require.NoError(t, err)

	sr := result.(*vectorindex.SearchResult)
	assert.Empty(t, sr.Hits)
}

func TestSparseIndex_SearchWithPreComputedVec(t *testing.T) {
	si, db := setupSparseIndex(t)

	// Insert one document
	writeSparseEmbedding(t, db, []byte("doc1"), si.sparseSuffix, vector.NewSparseVector(
		[]uint32{100, 200}, []float32{1.5, 2.0},
	))
	writes := [][2][]byte{
		{append(bytes.Clone([]byte("doc1")), si.sparseSuffix...), nil},
	}
	require.NoError(t, si.Batch(t.Context(), writes, nil, false))

	// Search with pre-computed vector (the primary search path from db.go)
	result, err := si.Search(t.Context(), &SparseSearchRequest{
		QueryVec: vector.NewSparseVector([]uint32{100, 200}, []float32{1.0, 1.0}),
		K:        5,
	})
	require.NoError(t, err)

	sr := result.(*vectorindex.SearchResult)
	require.Len(t, sr.Hits, 1)
	assert.Equal(t, "doc1", sr.Hits[0].ID)
	// 1.5*1.0 + 2.0*1.0 = 3.5
	assert.InDelta(t, 3.5, float64(sr.Hits[0].Score), 0.15)
}

func TestSparseIndex_SearchInvalidQueryType(t *testing.T) {
	si, _ := setupSparseIndex(t)

	// vectorindex.SearchRequest is not supported for sparse
	_, err := si.Search(t.Context(), &vectorindex.SearchRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	// nil query
	_, err = si.Search(t.Context(), nil)
	require.Error(t, err)

	// wrong type entirely
	_, err = si.Search(t.Context(), "not a request")
	require.Error(t, err)
}

func TestSparseIndex_BatchDelete(t *testing.T) {
	si, db := setupSparseIndex(t)

	// Insert two documents
	writeSparseEmbedding(t, db, []byte("doc1"), si.sparseSuffix, vector.NewSparseVector(
		[]uint32{10}, []float32{1.0},
	))
	writeSparseEmbedding(t, db, []byte("doc2"), si.sparseSuffix, vector.NewSparseVector(
		[]uint32{10}, []float32{2.0},
	))
	writes := [][2][]byte{
		{append(bytes.Clone([]byte("doc1")), si.sparseSuffix...), nil},
		{append(bytes.Clone([]byte("doc2")), si.sparseSuffix...), nil},
	}
	require.NoError(t, si.Batch(t.Context(), writes, nil, false))

	// Verify both are searchable
	result, err := si.Search(t.Context(), &SparseSearchRequest{
		QueryVec: vector.NewSparseVector([]uint32{10}, []float32{1.0}),
		K:        10,
	})
	require.NoError(t, err)
	sr := result.(*vectorindex.SearchResult)
	require.Len(t, sr.Hits, 2)

	// Delete doc1
	err = si.Batch(t.Context(), nil, [][]byte{[]byte("doc1")}, false)
	require.NoError(t, err)

	// Only doc2 should remain
	result, err = si.Search(t.Context(), &SparseSearchRequest{
		QueryVec: vector.NewSparseVector([]uint32{10}, []float32{1.0}),
		K:        10,
	})
	require.NoError(t, err)
	sr = result.(*vectorindex.SearchResult)
	require.Len(t, sr.Hits, 1)
	assert.Equal(t, "doc2", sr.Hits[0].ID)
}

func TestSparseIndex_BatchDeleteChunkKey(t *testing.T) {
	si, db := setupSparseIndex(t)

	chunkKey := storeutils.MakeChunkKey([]byte("doc1"), si.name, 0)
	writeSparseEmbedding(t, db, chunkKey, si.sparseSuffix, vector.NewSparseVector(
		[]uint32{10}, []float32{1.0},
	))

	writes := [][2][]byte{
		{append(bytes.Clone(chunkKey), si.sparseSuffix...), nil},
	}
	require.NoError(t, si.Batch(t.Context(), writes, nil, false))

	result, err := si.Search(t.Context(), &SparseSearchRequest{
		QueryVec: vector.NewSparseVector([]uint32{10}, []float32{1.0}),
		K:        10,
	})
	require.NoError(t, err)
	sr := result.(*vectorindex.SearchResult)
	require.Len(t, sr.Hits, 1)
	assert.Equal(t, string(chunkKey), sr.Hits[0].ID)

	require.NoError(t, si.Batch(t.Context(), nil, [][]byte{chunkKey}, false))

	result, err = si.Search(t.Context(), &SparseSearchRequest{
		QueryVec: vector.NewSparseVector([]uint32{10}, []float32{1.0}),
		K:        10,
	})
	require.NoError(t, err)
	sr = result.(*vectorindex.SearchResult)
	require.Empty(t, sr.Hits)
}

func TestSparseIndex_RenderPrompt(t *testing.T) {
	si, _ := setupSparseIndex(t)

	tests := []struct {
		name     string
		doc      map[string]any
		wantText string
		wantErr  bool
	}{
		{
			name:     "string field",
			doc:      map[string]any{"content": "hello world"},
			wantText: "hello world",
		},
		{
			name:     "missing field",
			doc:      map[string]any{"other": "value"},
			wantText: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			text, _, err := si.RenderPrompt(tt.doc)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantText, text)
			}
		})
	}
}

func TestEncodeSparseVec_RoundTrip(t *testing.T) {
	original := vector.NewSparseVector(
		[]uint32{1, 42, 1337, 9001},
		[]float32{0.5, 1.2, 3.14, 0.001},
	)

	encoded := EncodeSparseVec(original)
	decoded, err := decodeSparseVec(encoded)
	require.NoError(t, err)

	assert.Equal(t, original.GetIndices(), decoded.GetIndices())
	for i := range original.GetValues() {
		assert.Equal(t, original.GetValues()[i], decoded.GetValues()[i])
	}
}

func TestEncodeSparseVec_Empty(t *testing.T) {
	empty := vector.NewSparseVector(nil, nil)
	encoded := EncodeSparseVec(empty)
	decoded, err := decodeSparseVec(encoded)
	require.NoError(t, err)
	assert.Empty(t, decoded.GetIndices())
	assert.Empty(t, decoded.GetValues())
}

func TestDecodeSparseVec_TooShort(t *testing.T) {
	_, err := decodeSparseVec([]byte{1, 2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestEncodeSparseVec_LargeValues(t *testing.T) {
	sv := vector.NewSparseVector(
		[]uint32{math.MaxUint32, 0, 1},
		[]float32{math.MaxFloat32, math.SmallestNonzeroFloat32, 0},
	)
	encoded := EncodeSparseVec(sv)
	decoded, err := decodeSparseVec(encoded)
	require.NoError(t, err)
	assert.Equal(t, sv.GetIndices(), decoded.GetIndices())
	for i := range sv.GetValues() {
		assert.Equal(t, sv.GetValues()[i], decoded.GetValues()[i])
	}
}

func TestSparseIndex_Stats(t *testing.T) {
	si, db := setupSparseIndex(t)

	// Insert a document
	writeSparseEmbedding(t, db, []byte("doc1"), si.sparseSuffix, vector.NewSparseVector(
		[]uint32{10, 20}, []float32{1.0, 2.0},
	))
	writes := [][2][]byte{
		{append(bytes.Clone([]byte("doc1")), si.sparseSuffix...), nil},
	}
	require.NoError(t, si.Batch(t.Context(), writes, nil, false))

	stats := si.Stats()
	indexStats, err := stats.AsEmbeddingsIndexStats()
	require.NoError(t, err)
	assert.Positive(t, indexStats.TotalIndexed)
}

// mockSparseEmbedder implements both embeddings.Embedder and embeddings.SparseEmbedder
// so it can be registered via RegisterEmbedder and pass the SparseEmbedder type assertion.
type mockSparseEmbedder struct {
	embedFunc       func(ctx context.Context, values []string) ([][]float32, error)
	sparseEmbedFunc func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error)
}

func (m *mockSparseEmbedder) Capabilities() embeddings.EmbedderCapabilities {
	return embeddings.TextOnlyCapabilities()
}

func (m *mockSparseEmbedder) RateLimiter() *rate.Limiter {
	return nil
}

func (m *mockSparseEmbedder) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	values := embeddings.ExtractText(contents)
	if m.embedFunc != nil {
		return m.embedFunc(ctx, values)
	}
	result := make([][]float32, len(values))
	for i := range values {
		result[i] = []float32{float32(i)}
	}
	return result, nil
}

func (m *mockSparseEmbedder) SparseEmbed(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
	if m.sparseEmbedFunc != nil {
		return m.sparseEmbedFunc(ctx, texts)
	}
	// Default: return deterministic sparse vectors based on text hash
	result := make([]embeddings.SparseVector, len(texts))
	for i, text := range texts {
		result[i] = embeddings.SparseVector{
			Indices: []uint32{uint32(len(text) % 100), uint32((len(text) + 1) % 100)},
			Values:  []float32{float32(i) + 1.0, 0.5},
		}
	}
	return result, nil
}

// setupSparseIndexWithPipeline creates a SparseIndex with chunking and/or summarization configured.
func setupSparseIndexWithPipeline(t *testing.T, withChunker, withSummarizer bool) (*SparseIndex, *pebble.DB) {
	t.Helper()
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	embConfig := EmbeddingsIndexConfig{
		Field: "content",
		Embedder: embeddings.NewEmbedderConfigFromJSON(
			"mock",
			[]byte(`{"provider": "mock", "model": "test-sparse"}`),
		),
	}

	if withChunker {
		chunkerConfig, err := chunking.NewChunkerConfig(chunking.TermiteChunkerConfig{
			ApiUrl: "http://mock",
			Model:  "semantic",
		})
		require.NoError(t, err)
		chunkerConfig.StoreChunks = true
		embConfig.Chunker = chunkerConfig
	}

	if withSummarizer {
		embConfig.Summarizer = ai.NewGeneratorConfigFromJSON(
			"mock",
			[]byte(`{"provider": "mock", "model": "test-summarizer"}`),
		)
	}

	cfg := &IndexConfig{
		Type: IndexTypeEmbeddings,
		Name: "test_sparse_pipeline",
	}
	_ = cfg.FromEmbeddingsIndexConfig(embConfig)

	idx, err := NewSparseIndex(logger, nil, db, tempDir, "test_sparse_pipeline", cfg, nil)
	require.NoError(t, err)

	si := idx.(*SparseIndex)
	err = si.Open(true, &schema.TableSchema{}, types.Range{nil, nil})
	require.NoError(t, err)
	t.Cleanup(func() { si.Close() })

	return si, db
}

// TestSparseIndex_NewWithChunkerSuffix verifies the chunkerSuffix field is set when chunker is configured.
func TestSparseIndex_NewWithChunkerSuffix(t *testing.T) {
	si, _ := setupSparseIndexWithPipeline(t, true, false)

	assert.NotNil(t, si.chunkerSuffix, "chunkerSuffix should be set when chunker is configured")
	assert.Contains(t, string(si.chunkerSuffix), ":i:test_sparse_pipeline:")
	assert.NotNil(t, si.conf.Chunker, "Chunker config should be preserved")
}

// TestSparseIndex_NewWithoutChunkerSuffix verifies chunkerSuffix is nil when no chunker.
func TestSparseIndex_NewWithoutChunkerSuffix(t *testing.T) {
	si, _ := setupSparseIndex(t)

	assert.Nil(t, si.chunkerSuffix, "chunkerSuffix should be nil without chunker config")
}

// TestSparseIndex_BatchKeyRouting_Pipeline verifies that Batch correctly routes
// chunk keys, summary keys, and document keys when pipeline is configured.
func TestSparseIndex_BatchKeyRouting_Pipeline(t *testing.T) {
	si, db := setupSparseIndexWithPipeline(t, true, true)

	t.Run("SparseEmbeddingKey_IndexedDirectly", func(t *testing.T) {
		// Write a sparse embedding to Pebble
		writeSparseEmbedding(t, db, []byte("doc1"), si.sparseSuffix, vector.NewSparseVector(
			[]uint32{10, 20}, []float32{2.0, 1.0},
		))

		// Batch with the sparse suffix key - should be indexed into the sparse inverted index
		sparseKey := append(bytes.Clone([]byte("doc1")), si.sparseSuffix...)
		writes := [][2][]byte{{sparseKey, nil}}
		err := si.Batch(t.Context(), writes, nil, false)
		require.NoError(t, err)

		// Verify the document is searchable
		result, err := si.Search(t.Context(), &SparseSearchRequest{
			QueryVec: vector.NewSparseVector([]uint32{10}, []float32{1.0}),
			K:        5,
		})
		require.NoError(t, err)
		sr := result.(*vectorindex.SearchResult)
		require.Len(t, sr.Hits, 1)
		assert.Equal(t, "doc1", sr.Hits[0].ID)
	})

	t.Run("ChunkKey_RoutedToEnricher", func(t *testing.T) {
		// Create a chunk key that belongs to this index
		chunkKey := storeutils.MakeChunkKey([]byte("doc2"), "test_sparse_pipeline", 0)

		// Write chunk data in Pebble (so enricher can read it)
		chunk := chunking.NewTextChunk(0, "This is chunk text.", 0, 19)
		chunkJSON, err := json.Marshal(chunk)
		require.NoError(t, err)
		b := make([]byte, 0, len(chunkJSON)+8)
		b = encoding.EncodeUint64Ascending(b, 12345)
		b = append(b, chunkJSON...)
		require.NoError(t, db.Set(chunkKey, b, nil))

		// Batch: chunk key should be recognized as needing enrichment (promptKeys path)
		// No enricher is set, so it won't actually process, but this verifies the routing logic
		writes := [][2][]byte{{chunkKey, nil}}
		err = si.Batch(t.Context(), writes, nil, false)
		require.NoError(t, err)
		// No error means it was routed correctly; without enricher it's a no-op
	})

	t.Run("SummaryKey_RoutedToEnricher", func(t *testing.T) {
		// Create a summary key
		sumKey := append(bytes.Clone([]byte("doc3")), si.summarizerSuffix...)

		// Write summary data in Pebble
		b := make([]byte, 0, 30)
		b = encoding.EncodeUint64Ascending(b, 67890)
		b = append(b, []byte("Summary text")...)
		require.NoError(t, db.Set(sumKey, b, nil))

		// Batch: summary key should be recognized as needing enrichment
		writes := [][2][]byte{{sumKey, nil}}
		err := si.Batch(t.Context(), writes, nil, false)
		require.NoError(t, err)
	})

	t.Run("DocumentKey_RoutedToEnricher", func(t *testing.T) {
		// Write a document in Pebble
		writeCompressedDoc(t, db, []byte("doc4"), map[string]any{"content": "hello world"})

		// Batch: document key (no :i: prefix) should be routed to promptKeys
		writes := [][2][]byte{{[]byte("doc4"), nil}}
		err := si.Batch(t.Context(), writes, nil, false)
		require.NoError(t, err)
	})

	t.Run("OtherIndexKey_Ignored", func(t *testing.T) {
		// Keys belonging to other indexes (contain :i: but not our sparse suffix) should be ignored
		otherKey := []byte("doc5:i:other_index:e")
		writes := [][2][]byte{{otherKey, nil}}
		err := si.Batch(t.Context(), writes, nil, false)
		require.NoError(t, err)
		// No error, no crash, key ignored
	})
}

// TestSparseIndex_BatchKeyRouting_NoPipeline verifies that without pipeline config,
// chunk/summary keys are NOT routed (only sparse suffix keys and document keys are).
func TestSparseIndex_BatchKeyRouting_NoPipeline(t *testing.T) {
	si, db := setupSparseIndex(t) // No chunker, no summarizer

	// Write a document
	writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"content": "hello"})

	// Document key should be routed to promptKeys
	writes := [][2][]byte{{[]byte("doc1"), nil}}
	err := si.Batch(t.Context(), writes, nil, false)
	require.NoError(t, err)

	// A chunk key for this index should NOT be routed (needsPipeline=false)
	chunkKey := storeutils.MakeChunkKey([]byte("doc2"), "test_sparse", 0)
	writes = [][2][]byte{{chunkKey, nil}}
	err = si.Batch(t.Context(), writes, nil, false)
	require.NoError(t, err) // Just verifies no panic/error
}

// TestSparsePipelineAdapter verifies that SparsePipelineAdapter correctly
// implements TerminalEmbeddingEnricher by wrapping SparseEmbeddingEnricher.
func TestSparsePipelineAdapter(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	var mu sync.Mutex
	var persistedWrites [][2][]byte

	sparseEmb := &mockSparseEmbedder{
		sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
			result := make([]embeddings.SparseVector, len(texts))
			for i := range texts {
				result[i] = embeddings.SparseVector{
					Indices: []uint32{uint32(i + 1)},
					Values:  []float32{1.0},
				}
			}
			return result, nil
		},
	}

	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		mu.Lock()
		persistedWrites = append(persistedWrites, writes...)
		mu.Unlock()
		return nil
	}

	inner := NewSparseEmbeddingEnricher(
		logger, "test_sparse", db, sparseEmb, persistFunc,
		[]byte(":sp"), "content", "", types.Range{nil, nil},
	)

	ctx := t.Context()
	adapter := NewSparsePipelineAdapter(ctx, inner)

	// Verify it implements TerminalEmbeddingEnricher
	var _ TerminalEmbeddingEnricher = adapter

	t.Run("Close", func(t *testing.T) {
		err := adapter.Close()
		assert.NoError(t, err)
	})

	t.Run("GenerateEmbeddingsWithoutPersist_Documents", func(t *testing.T) {
		// Write a test document to Pebble
		docKey := []byte("test_doc")
		doc := map[string]any{"content": "hello world"}
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)

		documentValues := map[string][]byte{
			string(docKey): docBytes,
		}

		writes, chunkWrites, failed, err := adapter.GenerateEmbeddingsWithoutPersist(
			ctx, [][]byte{docKey}, documentValues, nil,
		)
		require.NoError(t, err)
		assert.Nil(t, chunkWrites, "Sparse adapter should return nil chunk writes")
		assert.Empty(t, failed)
		assert.Len(t, writes, 1, "Should generate one sparse embedding write")

		// Verify the write key has the sparse suffix
		assert.True(t, bytes.HasSuffix(writes[0][0], []byte(":sp")))
	})
}

// TestPipelineSparseEnricher verifies the pipelineSparseEnricher adapter
// correctly bridges the SparseEnricher interface to the Enricher interface.
func TestPipelineSparseEnricher(t *testing.T) {
	var enrichedKeys [][]byte
	var mu sync.Mutex

	mockEnricher := &mockPipelineEnricher{
		enrichBatchFunc: func(keys [][]byte) error {
			mu.Lock()
			enrichedKeys = append(enrichedKeys, keys...)
			mu.Unlock()
			return nil
		},
		closeFunc: func() error {
			return nil
		},
	}

	pse := &pipelineSparseEnricher{inner: mockEnricher}

	t.Run("EnrichBatch_DropsContext", func(t *testing.T) {
		// pipelineSparseEnricher should drop ctx (SparseEnricher has ctx, Enricher doesn't)
		keys := [][]byte{[]byte("key1"), []byte("key2")}
		err := pse.EnrichBatch(t.Context(), keys)
		require.NoError(t, err)

		mu.Lock()
		assert.Len(t, enrichedKeys, 2)
		mu.Unlock()
	})

	t.Run("Close", func(t *testing.T) {
		err := pse.Close()
		assert.NoError(t, err)
	})
}

// mockPipelineEnricher implements the Enricher interface for testing pipelineSparseEnricher.
type mockPipelineEnricher struct {
	enrichBatchFunc func(keys [][]byte) error
	closeFunc       func() error
}

func (m *mockPipelineEnricher) EnrichBatch(keys [][]byte) error {
	if m.enrichBatchFunc != nil {
		return m.enrichBatchFunc(keys)
	}
	return nil
}

func (m *mockPipelineEnricher) GenerateEmbeddingsWithoutPersist(
	ctx context.Context,
	keys [][]byte,
	documentValues map[string][]byte,
	generatePrompts generatePromptsFunc,
) ([][2][]byte, [][2][]byte, [][]byte, error) {
	return nil, nil, nil, nil
}

func (m *mockPipelineEnricher) EnricherStats() EnricherStats {
	return EnricherStats{}
}

func (m *mockPipelineEnricher) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

// TestSparseIndex_GeneratePrompts tests the GeneratePrompts method for sparse indexes.
func TestSparseIndex_GeneratePrompts(t *testing.T) {
	t.Run("EmptyStates", func(t *testing.T) {
		si, _ := setupSparseIndex(t)

		keys, prompts, hashIDs, err := si.GeneratePrompts(t.Context(), nil)
		require.NoError(t, err)
		assert.Nil(t, keys)
		assert.Nil(t, prompts)
		assert.Nil(t, hashIDs)
	})

	t.Run("DocumentWithField", func(t *testing.T) {
		si, db := setupSparseIndex(t)

		// Write a document to Pebble
		writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"content": "test prompt text"})

		states := []storeutils.DocumentScanState{
			{CurrentDocKey: []byte("doc1")},
		}

		keys, prompts, hashIDs, err := si.GeneratePrompts(t.Context(), states)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.Equal(t, []byte("doc1"), keys[0])
		assert.Equal(t, "test prompt text", prompts[0])
		assert.NotZero(t, hashIDs[0])
	})

	t.Run("DocumentMissingField", func(t *testing.T) {
		si, db := setupSparseIndex(t)

		// Write a document WITHOUT the "content" field
		writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"other": "value"})

		states := []storeutils.DocumentScanState{
			{CurrentDocKey: []byte("doc1")},
		}

		keys, prompts, hashIDs, err := si.GeneratePrompts(t.Context(), states)
		require.NoError(t, err)
		// Missing field => empty prompt => skipped
		assert.Empty(t, keys)
		assert.Empty(t, prompts)
		assert.Empty(t, hashIDs)
	})

	t.Run("SkipsAlreadyEnriched", func(t *testing.T) {
		si, db := setupSparseIndex(t)

		docContent := "already enriched text"

		// Write document
		writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"content": docContent})

		// Write the sparse embedding with matching hashID
		hashID := xxhash.Sum64String(docContent)
		sparseKey := append(bytes.Clone([]byte("doc1")), si.sparseSuffix...)
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value[:8], hashID)
		value = append(value, EncodeSparseVec(vector.NewSparseVector([]uint32{1}, []float32{1.0}))...)
		require.NoError(t, db.Set(sparseKey, value, nil))

		states := []storeutils.DocumentScanState{
			{CurrentDocKey: []byte("doc1")},
		}

		keys, prompts, _, err := si.GeneratePrompts(t.Context(), states)
		require.NoError(t, err)
		// Should be skipped because hashID matches
		assert.Empty(t, keys)
		assert.Empty(t, prompts)
	})

	t.Run("MultipleDocuments", func(t *testing.T) {
		si, db := setupSparseIndex(t)

		writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"content": "first document"})
		writeCompressedDoc(t, db, []byte("doc2"), map[string]any{"content": "second document"})
		writeCompressedDoc(t, db, []byte("doc3"), map[string]any{"other": "no content field"})

		states := []storeutils.DocumentScanState{
			{CurrentDocKey: []byte("doc1")},
			{CurrentDocKey: []byte("doc2")},
			{CurrentDocKey: []byte("doc3")},
		}

		keys, prompts, hashIDs, err := si.GeneratePrompts(t.Context(), states)
		require.NoError(t, err)
		// doc3 has no "content" field, so it should be skipped
		require.Len(t, keys, 2)
		assert.Equal(t, "first document", prompts[0])
		assert.Equal(t, "second document", prompts[1])
		assert.Len(t, hashIDs, 2)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		si, db := setupSparseIndex(t)

		writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"content": "text"})

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		_, _, _, err := si.GeneratePrompts(ctx, []storeutils.DocumentScanState{
			{CurrentDocKey: []byte("doc1")},
		})
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestSparseIndex_GeneratePrompts_WithPipeline verifies that GeneratePrompts
// uses the correct dedup suffix based on pipeline configuration.
func TestSparseIndex_GeneratePrompts_WithPipeline(t *testing.T) {
	t.Run("WithChunkerOnly", func(t *testing.T) {
		si, db := setupSparseIndexWithPipeline(t, true, false)

		// When chunker is configured, it should check chunkerSuffix for dedup
		writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"content": "chunk test text"})

		states := []storeutils.DocumentScanState{
			{CurrentDocKey: []byte("doc1")},
		}

		keys, prompts, _, err := si.GeneratePrompts(t.Context(), states)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.Equal(t, "chunk test text", prompts[0])
	})

	t.Run("WithSummarizerOnly", func(t *testing.T) {
		si, db := setupSparseIndexWithPipeline(t, false, true)

		// When summarizer is configured, it should check summarizerSuffix for dedup
		writeCompressedDoc(t, db, []byte("doc1"), map[string]any{"content": "summary test text"})

		states := []storeutils.DocumentScanState{
			{CurrentDocKey: []byte("doc1")},
		}

		keys, prompts, _, err := si.GeneratePrompts(t.Context(), states)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.Equal(t, "summary test text", prompts[0])
	})
}

// TestSparseIndex_LeaderFactory_WithPipeline tests the full pipeline creation in LeaderFactory.
func TestSparseIndex_LeaderFactory_WithPipeline(t *testing.T) {
	origDefaultFlushTime := DefaultFlushTime
	DefaultFlushTime = 50 * time.Millisecond
	t.Cleanup(func() { DefaultFlushTime = origDefaultFlushTime })

	si, db := setupSparseIndexWithPipeline(t, true, true) // chunker + summarizer

	// Register mock embedder that implements SparseEmbedder
	var sparseEmbedMu sync.Mutex
	var sparseEmbeddedTexts []string

	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockSparseEmbedder{
				sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
					sparseEmbedMu.Lock()
					sparseEmbeddedTexts = append(sparseEmbeddedTexts, texts...)
					sparseEmbedMu.Unlock()
					result := make([]embeddings.SparseVector, len(texts))
					for i := range texts {
						result[i] = embeddings.SparseVector{
							Indices: []uint32{uint32(i + 1), uint32(i + 2)},
							Values:  []float32{1.0, 0.5},
						}
					}
					return result, nil
				},
			}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	// Register mock summarizer
	ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)
	ai.RegisterDocumentSummarizer(
		ai.GeneratorProviderMock,
		func(ctx context.Context, config ai.GeneratorConfig) (ai.DocumentSummarizer, error) {
			return &mockSummarizer{}, nil
		},
	)
	t.Cleanup(func() { ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock) })

	// Start LeaderFactory in background
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		pbatch := db.NewBatch()
		defer pbatch.Close()
		for _, w := range writes {
			if err := pbatch.Set(w[0], w[1], nil); err != nil {
				return err
			}
		}
		return pbatch.Commit(pebble.Sync)
	}

	go func() {
		_ = si.LeaderFactory(ctx, persistFunc)
	}()

	// Wait for enricher to be created
	require.Eventually(t, func() bool {
		si.enricherMu.RLock()
		defer si.enricherMu.RUnlock()
		return si.enricher != nil
	}, 5*time.Second, 50*time.Millisecond, "Enricher should be created by LeaderFactory")

	// Verify that the enricher is a pipelineSparseEnricher
	si.enricherMu.RLock()
	_, isPipeline := si.enricher.(*pipelineSparseEnricher)
	si.enricherMu.RUnlock()
	assert.True(t, isPipeline, "Enricher should be pipelineSparseEnricher when chunker/summarizer configured")

	// Write a document and trigger enrichment
	writeCompressedDoc(t, db, []byte("pipe_doc"), map[string]any{"content": "Pipeline test document for sparse embedding"})

	writes := [][2][]byte{{[]byte("pipe_doc"), nil}}
	err := si.Batch(t.Context(), writes, nil, false)
	require.NoError(t, err)

	// Cancel to stop leader factory
	cancel()
}

// TestSparseIndex_LeaderFactory_NoPipeline tests that without chunker/summarizer,
// LeaderFactory creates a simple SparseEmbeddingEnricher.
func TestSparseIndex_LeaderFactory_NoPipeline(t *testing.T) {
	si, db := setupSparseIndexWithPipeline(t, false, false) // no chunker, no summarizer

	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockSparseEmbedder{}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		pbatch := db.NewBatch()
		defer pbatch.Close()
		for _, w := range writes {
			if err := pbatch.Set(w[0], w[1], nil); err != nil {
				return err
			}
		}
		return pbatch.Commit(pebble.Sync)
	}

	go func() {
		_ = si.LeaderFactory(ctx, persistFunc)
	}()

	require.Eventually(t, func() bool {
		si.enricherMu.RLock()
		defer si.enricherMu.RUnlock()
		return si.enricher != nil
	}, 5*time.Second, 50*time.Millisecond)

	// Verify it's NOT a pipeline enricher
	si.enricherMu.RLock()
	_, isPipeline := si.enricher.(*pipelineSparseEnricher)
	si.enricherMu.RUnlock()
	assert.False(t, isPipeline, "Enricher should be SparseEmbeddingEnricher, not pipeline")

	cancel()
}

// TestSparseIndex_ComputeEnrichments_Simple tests ComputeEnrichments without pipeline.
func TestSparseIndex_ComputeEnrichments_Simple(t *testing.T) {
	si, _ := setupSparseIndexWithPipeline(t, false, false) // embedder configured but no pipeline

	// Register mock sparse embedder
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockSparseEmbedder{
				sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
					result := make([]embeddings.SparseVector, len(texts))
					for i := range texts {
						result[i] = embeddings.SparseVector{
							Indices: []uint32{10, 20},
							Values:  []float32{1.5, 0.5},
						}
					}
					return result, nil
				},
			}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	// Set up document writes with JSON values
	doc := map[string]any{"content": "test document for enrichment"}
	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)

	writes := [][2][]byte{
		{[]byte("doc1"), docBytes},
	}

	enrichWrites, failedKeys, err := si.ComputeEnrichments(t.Context(), writes)
	require.NoError(t, err)
	assert.Empty(t, failedKeys)
	require.Len(t, enrichWrites, 1)

	// Verify the key has the sparse suffix
	assert.True(t, bytes.HasSuffix(enrichWrites[0][0], si.sparseSuffix))

	// Verify the value contains hashID + encoded sparse vector
	value := enrichWrites[0][1]
	assert.GreaterOrEqual(t, len(value), 8, "Value should contain at least hashID")
}

// TestSparseIndex_ComputeEnrichments_SkipsIndexKeys verifies that enrichment keys
// (containing :i:) are skipped in ComputeEnrichments.
func TestSparseIndex_ComputeEnrichments_SkipsIndexKeys(t *testing.T) {
	si, _ := setupSparseIndexWithPipeline(t, false, false)

	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockSparseEmbedder{
				sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
					t.Fatal("SparseEmbed should not be called for index keys")
					return nil, nil
				},
			}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	// Write with :i: key (should be skipped)
	writes := [][2][]byte{
		{[]byte("doc1:i:some_index:e"), []byte(`{"content":"test"}`)},
	}

	enrichWrites, failedKeys, err := si.ComputeEnrichments(t.Context(), writes)
	require.NoError(t, err)
	assert.Empty(t, enrichWrites)
	assert.Empty(t, failedKeys)
}

// TestSparseIndex_ComputeEnrichments_Pipeline verifies that when a pipeline enricher
// is configured, ComputeEnrichments delegates to it.
func TestSparseIndex_ComputeEnrichments_Pipeline(t *testing.T) {
	si, db := setupSparseIndexWithPipeline(t, true, true)

	// Register sparse embedder mock
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockSparseEmbedder{
				sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
					result := make([]embeddings.SparseVector, len(texts))
					for i := range texts {
						result[i] = embeddings.SparseVector{
							Indices: []uint32{uint32(i*10 + 1)},
							Values:  []float32{1.0},
						}
					}
					return result, nil
				},
			}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	// Register mock summarizer
	ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)
	ai.RegisterDocumentSummarizer(
		ai.GeneratorProviderMock,
		func(ctx context.Context, config ai.GeneratorConfig) (ai.DocumentSummarizer, error) {
			return &mockSummarizer{}, nil
		},
	)
	t.Cleanup(func() { ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock) })

	// Create pipeline enricher via LeaderFactory
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		pbatch := db.NewBatch()
		defer pbatch.Close()
		for _, w := range writes {
			if err := pbatch.Set(w[0], w[1], nil); err != nil {
				return err
			}
		}
		return pbatch.Commit(pebble.Sync)
	}

	go func() {
		_ = si.LeaderFactory(ctx, persistFunc)
	}()

	require.Eventually(t, func() bool {
		si.enricherMu.RLock()
		defer si.enricherMu.RUnlock()
		return si.enricher != nil
	}, 5*time.Second, 50*time.Millisecond)

	// Verify it's a pipeline enricher
	si.enricherMu.RLock()
	_, isPipeline := si.enricher.(*pipelineSparseEnricher)
	si.enricherMu.RUnlock()
	require.True(t, isPipeline, "Should have pipeline enricher")

	// Write a document
	doc := map[string]any{"content": "pipeline enrichment test"}
	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)

	// Compress it (like production)
	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := writer.EncodeAll(docBytes, nil)

	writes := [][2][]byte{
		{[]byte("pipe_doc"), compressed},
	}

	// ComputeEnrichments should delegate to computeEnrichmentsPipeline
	enrichWrites, failedKeys, err := si.ComputeEnrichments(t.Context(), writes)
	require.NoError(t, err)

	// Pipeline produces enrichment writes (chunks and/or sparse embeddings)
	// The exact output depends on the pipeline stages, but it should not error
	_ = enrichWrites
	_ = failedKeys

	cancel()
}

// TestSparseEmbeddingEnricher_ChunkKeys verifies that SparseEmbeddingEnricher
// can read and embed chunk keys.
func TestSparseEmbeddingEnricher_ChunkKeys(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	var mu sync.Mutex
	var persistedWrites [][2][]byte

	sparseEmb := &mockSparseEmbedder{
		sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
			result := make([]embeddings.SparseVector, len(texts))
			for i := range texts {
				result[i] = embeddings.SparseVector{
					Indices: []uint32{uint32(i + 1)},
					Values:  []float32{1.0},
				}
			}
			return result, nil
		},
	}

	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		mu.Lock()
		persistedWrites = append(persistedWrites, writes...)
		mu.Unlock()
		return nil
	}

	sparseSuffix := []byte(":i:test:sp")
	enricher := NewSparseEmbeddingEnricher(
		logger, "test", db, sparseEmb, persistFunc,
		sparseSuffix, "content", "", types.Range{nil, nil},
	)
	defer enricher.Close()

	// Write a chunk to Pebble
	chunk := chunking.NewTextChunk(0, "This is a test chunk for sparse embedding.", 0, 44)
	chunkJSON, err := json.Marshal(chunk)
	require.NoError(t, err)

	chunkKey := storeutils.MakeChunkKey([]byte("doc1"), "test", 0)

	// Chunk format: [hashID:uint64][chunkJSON]
	b := make([]byte, 0, len(chunkJSON)+8)
	b = encoding.EncodeUint64Ascending(b, 12345)
	b = append(b, chunkJSON...)
	require.NoError(t, db.Set(chunkKey, b, nil))

	// EnrichBatch with chunk key
	err = enricher.EnrichBatch(t.Context(), [][]byte{chunkKey})
	require.NoError(t, err)

	mu.Lock()
	assert.Len(t, persistedWrites, 1, "Should persist one sparse embedding for the chunk")
	if len(persistedWrites) > 0 {
		assert.True(t, bytes.HasSuffix(persistedWrites[0][0], sparseSuffix),
			"Persisted key should have sparse suffix")
	}
	mu.Unlock()
}

// TestSparseEmbeddingEnricher_SummaryKeys verifies that SparseEmbeddingEnricher
// can read and embed summary keys (when sumSuffix is set).
func TestSparseEmbeddingEnricher_SummaryKeys(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	var mu sync.Mutex
	var persistedWrites [][2][]byte

	sparseEmb := &mockSparseEmbedder{
		sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
			result := make([]embeddings.SparseVector, len(texts))
			for i := range texts {
				result[i] = embeddings.SparseVector{
					Indices: []uint32{42},
					Values:  []float32{2.0},
				}
			}
			return result, nil
		},
	}

	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		mu.Lock()
		persistedWrites = append(persistedWrites, writes...)
		mu.Unlock()
		return nil
	}

	sparseSuffix := []byte(":i:test:sp")
	sumSuffix := []byte(":i:test:s")
	enricher := NewSparseEmbeddingEnricher(
		logger, "test", db, sparseEmb, persistFunc,
		sparseSuffix, "content", "", types.Range{nil, nil},
	)
	enricher.sumSuffix = sumSuffix
	defer enricher.Close()

	// Write a summary to Pebble
	summaryKey := append(bytes.Clone([]byte("doc1")), sumSuffix...)
	b := make([]byte, 0, 50)
	b = encoding.EncodeUint64Ascending(b, 99999)
	b = append(b, []byte("This is a summary of the document.")...)
	require.NoError(t, db.Set(summaryKey, b, nil))

	// EnrichBatch with summary key
	err = enricher.EnrichBatch(t.Context(), [][]byte{summaryKey})
	require.NoError(t, err)

	mu.Lock()
	assert.Len(t, persistedWrites, 1, "Should persist one sparse embedding for the summary")
	if len(persistedWrites) > 0 {
		assert.True(t, bytes.HasSuffix(persistedWrites[0][0], sparseSuffix))
	}
	mu.Unlock()
}

// TestSparseIndex_PipelineWithTerminalAdapter_Integration tests the full flow:
// SparseIndex -> LeaderFactory -> PipelineEnricher(sparse terminal) -> Batch -> async enrichment
func TestSparseIndex_PipelineWithTerminalAdapter_Integration(t *testing.T) {
	origDefaultFlushTime := DefaultFlushTime
	DefaultFlushTime = 50 * time.Millisecond
	t.Cleanup(func() { DefaultFlushTime = origDefaultFlushTime })

	si, db := setupSparseIndexWithPipeline(t, false, true) // summarizer only

	var sparseEmbedMu sync.Mutex
	var sparseEmbeddedTexts []string

	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockSparseEmbedder{
				sparseEmbedFunc: func(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
					sparseEmbedMu.Lock()
					sparseEmbeddedTexts = append(sparseEmbeddedTexts, texts...)
					sparseEmbedMu.Unlock()
					result := make([]embeddings.SparseVector, len(texts))
					for i := range texts {
						result[i] = embeddings.SparseVector{
							Indices: []uint32{uint32(i*10 + 1), uint32(i*10 + 2)},
							Values:  []float32{float32(i) + 1.0, 0.5},
						}
					}
					return result, nil
				},
			}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	var summariesMu sync.Mutex
	var generatedSummaries []string

	ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)
	ai.RegisterDocumentSummarizer(
		ai.GeneratorProviderMock,
		func(ctx context.Context, config ai.GeneratorConfig) (ai.DocumentSummarizer, error) {
			return &mockSummarizer{
				summarizeFunc: func(ctx context.Context, contents [][]ai.ContentPart, _ ...ai.GenerateOption) ([]string, error) {
					result := make([]string, len(contents))
					for i, c := range contents {
						if len(c) > 0 {
							if tp, ok := c[0].(ai.TextContent); ok {
								result[i] = "Summary: " + tp.Text
								summariesMu.Lock()
								generatedSummaries = append(generatedSummaries, result[i])
								summariesMu.Unlock()
							}
						}
					}
					return result, nil
				},
			}, nil
		},
	)
	t.Cleanup(func() { ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// The persist function writes to Pebble AND calls Batch to simulate
	// the Raft apply loop re-entering the index with new keys
	persistFunc := func(ctx context.Context, writes [][2][]byte) error {
		pbatch := db.NewBatch()
		defer pbatch.Close()
		for _, w := range writes {
			if err := pbatch.Set(w[0], w[1], nil); err != nil {
				return err
			}
		}
		if err := pbatch.Commit(pebble.Sync); err != nil {
			return err
		}
		// Re-enter Batch to simulate how Raft apply works
		return si.Batch(ctx, writes, nil, false)
	}

	go func() {
		_ = si.LeaderFactory(ctx, persistFunc)
	}()

	require.Eventually(t, func() bool {
		si.enricherMu.RLock()
		defer si.enricherMu.RUnlock()
		return si.enricher != nil
	}, 5*time.Second, 50*time.Millisecond)

	// Write test documents
	for i := range 3 {
		key := fmt.Appendf(nil, "int_doc%d", i)
		writeCompressedDoc(t, db, key, map[string]any{
			"content": fmt.Sprintf("Integration test document number %d for sparse pipeline", i),
		})

		writes := [][2][]byte{{key, nil}}
		err := si.Batch(t.Context(), writes, nil, false)
		require.NoError(t, err)
	}

	// Wait for async enrichment pipeline to complete
	// Doc → summary → sparse embedding (multiple async stages)
	time.Sleep(3*DefaultFlushTime + 500*time.Millisecond)

	// Verify summaries were generated
	summariesMu.Lock()
	numSummaries := len(generatedSummaries)
	summariesMu.Unlock()
	assert.Positive(t, numSummaries, "Summarizer should have been called")

	cancel()
}
