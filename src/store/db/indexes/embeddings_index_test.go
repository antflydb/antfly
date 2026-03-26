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
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/embeddings"
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

// Mock embedder for testing
type mockEmbedder struct {
	embedFunc func(ctx context.Context, values []string) ([][]float32, error)
}

type stubStatsVectorIndex struct {
	stats map[string]any
}

func (s *stubStatsVectorIndex) Name() string { return "stub" }
func (s *stubStatsVectorIndex) Batch(context.Context, *vectorindex.Batch) error {
	return nil
}
func (s *stubStatsVectorIndex) Search(*vectorindex.SearchRequest) ([]*vectorindex.Result, error) {
	return nil, nil
}
func (s *stubStatsVectorIndex) Delete(...uint64) error { return nil }
func (s *stubStatsVectorIndex) GetMetadata(uint64) ([]byte, error) {
	return nil, nil
}
func (s *stubStatsVectorIndex) Stats() map[string]any { return s.stats }
func (s *stubStatsVectorIndex) TotalVectors() uint64  { return 0 }
func (s *stubStatsVectorIndex) Close() error          { return nil }

func (m *mockEmbedder) Capabilities() embeddings.EmbedderCapabilities {
	return embeddings.TextOnlyCapabilities()
}

func (m *mockEmbedder) RateLimiter() *rate.Limiter {
	return nil // no rate limit in tests
}

func (m *mockEmbedder) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	values := embeddings.ExtractText(contents)
	if m.embedFunc != nil {
		return m.embedFunc(ctx, values)
	}
	// Default implementation returns simple embeddings
	result := make([][]float32, len(values))
	for i := range values {
		result[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
	}
	return result, nil
}

func TestNewEmbeddingIndex(t *testing.T) {
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
			name: "valid config",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "{{content}}",
			},
			wantErr: false,
		},
		{
			name: "missing dimension",
			config: EmbeddingsIndexConfig{
				Template: "{{content}}",
			},
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name: "valid config (missing template)",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Field:     "content",
			},
			wantErr: false,
		},
		{
			name: "with embedder config",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "{{content}}",
				Embedder: embeddings.NewEmbedderConfigFromJSON(
					"mock",
					[]byte(`{ "model": "test-model" }`),
				),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := NewEmbeddingIndex(
				logger,
				nil,
				db,
				tempDir,
				"test_index",
				NewEmbeddingsConfig("test_index", tt.config),
				nil,
			)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, idx)
				ei := idx.(*EmbeddingIndex)
				assert.Equal(t, "test_index", ei.Name())
				if tt.config.Embedder != nil {
					assert.True(t, ei.NeedsEnricher())
					assert.NotNil(t, ei.embedderConf)
				}
			}
		})
	}
}

func TestEmbeddingIndex_Open(t *testing.T) {
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

	schema := &schema.TableSchema{
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
	byteRange := types.Range{[]byte("a"), []byte("z")}

	// Test opening with rebuild
	err = ei.Open(true, schema, byteRange)
	assert.NoError(t, err)
	assert.NotNil(t, ei.idx)
	assert.NotNil(t, ei.walBuf)

	// Close and reopen without rebuild
	err = ei.Close()
	assert.NoError(t, err)

	err = ei.Open(false, schema, byteRange)
	assert.NoError(t, err)
}

func TestEmbeddingIndex_Batch(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	}

	idx, err := NewEmbeddingIndex(
		logger,
		nil,
		db,
		tempDir,
		"test_index",
		NewEmbeddingsConfig("test_index", config),
		nil,
	)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	schema := &schema.TableSchema{}
	byteRange := types.Range{nil, nil}
	err = ei.Open(true, schema, byteRange)
	require.NoError(t, err)
	defer ei.Close()

	// Store test documents with string content field for prompt extraction
	testDocs := []struct {
		key []byte
		doc map[string]any
	}{
		{
			key: []byte("doc1"),
			doc: map[string]any{
				"content": "first document content",
			},
		},
		{
			key: []byte("doc2"),
			doc: map[string]any{
				"content": "second document content",
			},
		},
	}

	// Store documents in the database
	// Compress document
	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	for _, td := range testDocs {
		docBytes, err := json.Marshal(td.doc)
		require.NoError(t, err)
		compressed := writer.EncodeAll(docBytes, nil)
		require.NoError(t, err)
		err = db.Set(append(td.key, storeutils.DBRangeStart...), compressed, nil)
		require.NoError(t, err)
	}

	// Test batching writes
	writes := [][2][]byte{
		{[]byte("doc1"), []byte{}},
		{[]byte("doc2"), []byte{}},
	}

	err = ei.Batch(t.Context(), writes, nil, false)
	assert.NoError(t, err)

	// Test batching deletes
	deletes := [][]byte{[]byte("doc1")}
	err = ei.Batch(t.Context(), nil, deletes, false)
	assert.NoError(t, err)
}

func TestEmbeddingIndex_Search(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "embedding",
	}

	idx, err := NewEmbeddingIndex(
		logger,
		nil,
		db,
		tempDir,
		"test_index",
		NewEmbeddingsConfig("test_index", config),
		nil,
	)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	schema := &schema.TableSchema{}
	byteRange := types.Range{nil, nil}
	err = ei.Open(true, schema, byteRange)
	require.NoError(t, err)

	// Add some test vectors
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

	// Wait a bit for the index to process
	time.Sleep(100 * time.Millisecond)

	// Test search
	searchReq := &vectorindex.SearchRequest{
		Embedding: []float32{1.0, 0.0, 0.0},
		K:         2,
	}
	ctx := context.Background()
	resp, err := ei.Search(ctx, searchReq)
	assert.NoError(t, err)

	searchResp, ok := resp.(*vectorindex.SearchResult)
	require.True(t, ok)
	assert.NotNil(t, searchResp.Hits)
	require.NotNil(t, searchReq.RerankPolicy)
	assert.Equal(t, vectorindex.RerankPolicyBoundary, *searchReq.RerankPolicy)

	// Test invalid requests
	_, err = ei.Search(ctx, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, storeutils.ErrEmptyRequest)

	_, err = ei.Search(ctx, []byte("invalid json"))
	assert.Error(t, err)

	// Test dimension mismatch
	badReq := &vectorindex.SearchRequest{
		Embedding: []float32{1.0, 2.0}, // Wrong dimension
		K:         2,
	}
	_, err = ei.Search(ctx, badReq)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dimensionality mismatch")
}

func TestEmbeddingsBatch(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "embedding",
	}

	idx, err := NewEmbeddingIndex(
		logger,
		nil,
		db,
		tempDir,
		"test_index",
		NewEmbeddingsConfig("test_index", config),
		nil,
	)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	schema := &schema.TableSchema{}
	byteRange := types.Range{nil, nil}
	err = ei.Open(true, schema, byteRange)
	require.NoError(t, err)

	vecBatch := ei.db.NewBatch()
	keys := [][]byte{[]byte("key1"), []byte("key2")}
	vectors := []vector.T{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}
	for i := range keys {
		k := append(bytes.Clone(keys[i]), bytes.Clone(ei.embedderSuffix)...)
		e, err := vectorindex.EncodeEmbeddingWithHashID(nil, vectors[i], 0)
		require.NoError(t, err)
		require.NoError(t, vecBatch.Set(k, e, nil))
	}
	err = vecBatch.Commit(pebble.Sync)
	require.NoError(t, err)

	batch := ei.NewBatch()

	// Test insert
	err = batch.Insert(keys, vectors)
	assert.NoError(t, err)

	// Test delete
	deleteKeys := [][]byte{[]byte("key3")}
	err = batch.Delete(deleteKeys)
	assert.NoError(t, err)

	// Test commit
	err = batch.Commit(t.Context())
	assert.NoError(t, err)

	// Test reset
	batch.Reset()
	assert.Empty(t, batch.batch.Deletes)
	assert.Empty(t, batch.batch.Vectors)
}

func TestVectorOp_Pool(t *testing.T) {
	// Get an op from the pool
	vo1 := vectorOpPool.Get().(*vectorOp)
	assert.NotNil(t, vo1)
	assert.NotNil(t, vo1.Keys)
	assert.NotNil(t, vo1.Vectors)
	assert.NotNil(t, vo1.Deletes)

	// Add some data
	vo1.Keys = append(vo1.Keys, []byte("key1"))
	vo1.Vectors = append(vo1.Vectors, []float32{1.0, 2.0, 3.0})
	vo1.Deletes = append(vo1.Deletes, []byte("del1"))

	// Reset and return to pool
	vo1.reset()
	assert.Empty(t, vo1.Keys)
	assert.Empty(t, vo1.Vectors)
	assert.Empty(t, vo1.Deletes)
	vectorOpPool.Put(vo1)

	// Get another op - should reuse the same object
	vo2 := vectorOpPool.Get().(*vectorOp)
	assert.NotNil(t, vo2)
	// Capacity should be preserved
	assert.GreaterOrEqual(t, cap(vo2.Keys), 100)
}

func TestGeneratePrompts(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := EmbeddingsIndexConfig{
		Dimension: 3,
		Template:  "{{title}}: {{content}}",
	}

	idx, err := NewEmbeddingIndex(
		logger,
		nil,
		db,
		tempDir,
		"test_index",
		NewEmbeddingsConfig("test_index", config),
		nil,
	)
	require.NoError(t, err)
	ei := idx.(*EmbeddingIndex)

	// Store test documents
	testDocs := []struct {
		key []byte
		doc map[string]any
	}{
		{
			key: []byte("doc1"),
			doc: map[string]any{
				"title":   "Title 1",
				"content": "Content 1",
			},
		},
		{
			key: []byte("doc2"),
			doc: map[string]any{
				"title":   "Title 2",
				"content": "Content 2",
			},
		},
		{
			key: []byte("doc3"),
			doc: map[string]any{
				"missing": "fields",
			},
		},
	}

	// Store compressed documents in the database
	// Compress the document
	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	for _, td := range testDocs {
		docBytes, err := json.Marshal(td.doc)
		require.NoError(t, err)

		compressed := writer.EncodeAll(docBytes, nil)

		err = db.Set(append(td.key, storeutils.DBRangeStart...), compressed, nil)
		require.NoError(t, err)
	}

	// Test generating prompts
	docIDs := [][]byte{[]byte("doc1"), []byte("doc2"), []byte("doc3"), []byte("nonexistent")}
	states := make([]storeutils.DocumentScanState, len(docIDs))
	for i, id := range docIDs {
		states[i] = storeutils.DocumentScanState{CurrentDocKey: id}
	}
	keys, prompts, _, err := ei.GeneratePrompts(context.TODO(), states)
	assert.NoError(t, err)
	require.Len(t, keys, 3) // doc3 has missing fields but still generates empty prompt
	require.Len(t, prompts, 3)
	assert.Equal(t, "Title 1: Content 1", prompts[0])
	assert.Equal(t, "Title 2: Content 2", prompts[1])
	assert.Equal(
		t,
		":",
		prompts[2],
	) // Template with missing fields - Handlebars returns empty strings
}

func TestEmbeddingIndex_RenderPrompt(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name          string
		config        EmbeddingsIndexConfig
		doc           map[string]any
		expectedText  string
		expectError   bool
		errorContains string
	}{
		{
			name: "field extraction - string field",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Field:     "content",
			},
			doc: map[string]any{
				"content": "test content",
				"title":   "test title",
			},
			expectedText: "test content",
			expectError:  false,
		},
		{
			name: "field extraction - missing field",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Field:     "missing_field",
			},
			doc: map[string]any{
				"content": "test content",
			},
			expectError:   true,
			errorContains: "not found in document",
		},
		{
			name: "field extraction - non-string field",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Field:     "count",
			},
			doc: map[string]any{
				"count": 42,
			},
			expectError:   true,
			errorContains: "is not a string",
		},
		{
			name: "template rendering - simple handlebars",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "{{title}}: {{content}}",
			},
			doc: map[string]any{
				"title":   "My Title",
				"content": "My Content",
			},
			expectedText: "My Title: My Content",
			expectError:  false,
		},
		{
			name: "template rendering - with whitespace trimming",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "  {{content}}  ",
			},
			doc: map[string]any{
				"content": "test",
			},
			expectedText: "test",
			expectError:  false,
		},
		{
			name: "template rendering - missing variable (renders empty in handlebars)",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "{{missing}}",
			},
			doc: map[string]any{
				"content": "test",
			},
			expectedText: "",
			expectError:  false,
		},
		{
			name: "template rendering - complex handlebars template",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "Title: {{title}}\nAuthor: {{author}}\nContent: {{content}}",
			},
			doc: map[string]any{
				"title":   "The Great Article",
				"author":  "Jane Doe",
				"content": "This is the content.",
			},
			expectedText: "Title: The Great Article\nAuthor: Jane Doe\nContent: This is the content.",
			expectError:  false,
		},
		{
			name: "template rendering - media helper with file:// URL",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "{{media url=thumbnail_url}}{{title}}",
			},
			doc: map[string]any{
				"title":         "Ancient Ruins",
				"thumbnail_url": "file:///tmp/test-image.jpg",
			},
			// The media helper renders to <<<dotprompt:media:url URL>>> markers
			expectedText: "<<<dotprompt:media:url file:///tmp/test-image.jpg>>>Ancient Ruins",
			expectError:  false,
		},
		{
			name: "template rendering - media helper with https URL",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "{{media url=image_url}}{{title}}",
			},
			doc: map[string]any{
				"title":     "Photo Article",
				"image_url": "https://example.com/photo.jpg",
			},
			expectedText: "<<<dotprompt:media:url https://example.com/photo.jpg>>>Photo Article",
			expectError:  false,
		},
		{
			name: "template rendering - media helper with missing URL field renders empty",
			config: EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  "{{media url=missing_field}}{{title}}",
			},
			doc: map[string]any{
				"title": "No Image",
			},
			// When the url field is missing, media helper returns empty string
			expectedText: "No Image",
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a unique directory for each test to avoid conflicts
			testDir := filepath.Join(tempDir, tt.name)
			idxCfg := NewEmbeddingsConfig("test_idx", tt.config)
			idx, err := NewEmbeddingIndex(logger, nil, db, testDir, "test_idx", idxCfg, nil)
			require.NoError(t, err)
			require.NotNil(t, idx, "index should not be nil")

			ei := idx.(*EmbeddingIndex)

			prompt, hashID, err := ei.RenderPrompt(tt.doc)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedText, prompt)
				// Verify hashID matches the prompt
				expectedHashID := xxhash.Sum64String(tt.expectedText)
				assert.Equal(t, expectedHashID, hashID)
			}
		})
	}
}

func TestEmbeddingIndex_RenderPrompt_HashIDConsistency(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	}

	idxCfg := NewEmbeddingsConfig("test_idx", config)
	idx, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_idx", idxCfg, nil)
	require.NoError(t, err)

	ei := idx.(*EmbeddingIndex)

	doc := map[string]any{
		"content": "test content for hashing",
		"id":      "123",
	}

	// Call RenderPrompt multiple times with the same document
	prompt1, hashID1, err1 := ei.RenderPrompt(doc)
	require.NoError(t, err1)

	prompt2, hashID2, err2 := ei.RenderPrompt(doc)
	require.NoError(t, err2)

	prompt3, hashID3, err3 := ei.RenderPrompt(doc)
	require.NoError(t, err3)

	// All prompts should be identical
	assert.Equal(t, prompt1, prompt2)
	assert.Equal(t, prompt2, prompt3)

	// All hashIDs should be identical
	assert.Equal(t, hashID1, hashID2)
	assert.Equal(t, hashID2, hashID3)

	// HashID should be deterministic
	expectedHashID := xxhash.Sum64String("test content for hashing")
	assert.Equal(t, expectedHashID, hashID1)
}

func TestEmbeddingIndex_RenderPrompt_DifferentPromptsDifferentHashes(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	config := EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
	}

	idxCfg := NewEmbeddingsConfig("test_idx", config)
	idx, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_idx", idxCfg, nil)
	require.NoError(t, err)

	ei := idx.(*EmbeddingIndex)

	doc1 := map[string]any{
		"content": "first document",
	}

	doc2 := map[string]any{
		"content": "second document",
	}

	prompt1, hashID1, err1 := ei.RenderPrompt(doc1)
	require.NoError(t, err1)

	prompt2, hashID2, err2 := ei.RenderPrompt(doc2)
	require.NoError(t, err2)

	// Prompts should be different
	assert.NotEqual(t, prompt1, prompt2)

	// HashIDs should be different
	assert.NotEqual(t, hashID1, hashID2)

	// Verify each hashID matches its prompt
	assert.Equal(t, xxhash.Sum64String(prompt1), hashID1)
	assert.Equal(t, xxhash.Sum64String(prompt2), hashID2)
}

func TestEmbeddingIndex_GetDimension(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name      string
		dimension int
	}{
		{"dimension 3", 3},
		{"dimension 128", 128},
		{"dimension 512", 512},
		{"dimension 1536", 1536},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EmbeddingsIndexConfig{
				Dimension: tt.dimension,
				Field:     "content",
			}

			idxCfg := NewEmbeddingsConfig("test_idx", config)
			idx, err := NewEmbeddingIndex(logger, nil, db, tempDir, "test_idx", idxCfg, nil)
			require.NoError(t, err)

			ei := idx.(*EmbeddingIndex)
			assert.Equal(t, tt.dimension, ei.GetDimension())
		})
	}
}

// TestEmbeddingIndex_RenderPrompt_MediaTemplate_TextToParts verifies the end-to-end
// pipeline: a {{media url=...}} template renders to dotprompt markers, and ai.TextToParts
// correctly parses them into ImageURLContent parts alongside TextContent.
// This covers file:// URLs (local images) and https:// URLs.
func TestEmbeddingIndex_RenderPrompt_MediaTemplate_TextToParts(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name           string
		template       string
		doc            map[string]any
		wantTextParts  []string // expected text segments
		wantImageURLs  []string // expected image URLs
		wantTotalParts int      // total parts from TextToParts
	}{
		{
			name:     "media with file:// URL produces ImageURLContent",
			template: "{{media url=thumbnail_url}}{{title}}",
			doc: map[string]any{
				"title":         "Ancient Ruins",
				"thumbnail_url": "file:///tmp/test-image.jpg",
			},
			wantImageURLs:  []string{"file:///tmp/test-image.jpg"},
			wantTextParts:  []string{"Ancient Ruins"},
			wantTotalParts: 2,
		},
		{
			name:     "media with https:// URL produces ImageURLContent",
			template: "{{media url=image_url}}{{title}}",
			doc: map[string]any{
				"title":     "Photo Article",
				"image_url": "https://example.com/photo.jpg",
			},
			wantImageURLs:  []string{"https://example.com/photo.jpg"},
			wantTextParts:  []string{"Photo Article"},
			wantTotalParts: 2,
		},
		{
			name:     "text-only template produces only TextContent",
			template: "{{title}}: {{content}}",
			doc: map[string]any{
				"title":   "Hello",
				"content": "World",
			},
			wantImageURLs:  nil,
			wantTextParts:  []string{"Hello: World"},
			wantTotalParts: 1,
		},
		{
			name:     "media with missing URL field produces text-only",
			template: "{{media url=missing}}{{title}}",
			doc: map[string]any{
				"title": "No Image",
			},
			wantImageURLs:  nil,
			wantTextParts:  []string{"No Image"},
			wantTotalParts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := filepath.Join(tempDir, tt.name)
			idxCfg := NewEmbeddingsConfig("test_idx", EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  tt.template,
			})
			idx, err := NewEmbeddingIndex(logger, nil, db, testDir, "test_idx", idxCfg, nil)
			require.NoError(t, err)

			ei := idx.(*EmbeddingIndex)

			// Step 1: RenderPrompt produces dotprompt markers
			prompt, _, err := ei.RenderPrompt(tt.doc)
			require.NoError(t, err)

			// Step 2: TextToParts parses markers into typed parts
			parts, err := ai.TextToParts(prompt)
			require.NoError(t, err)
			assert.Len(t, parts, tt.wantTotalParts, "unexpected number of parts for prompt: %q", prompt)

			// Verify ImageURLContent parts
			var gotImageURLs []string
			var gotTextParts []string
			for _, part := range parts {
				switch p := part.(type) {
				case ai.ImageURLContent:
					gotImageURLs = append(gotImageURLs, p.URL)
				case ai.TextContent:
					gotTextParts = append(gotTextParts, p.Text)
				}
			}

			assert.Equal(t, tt.wantImageURLs, gotImageURLs, "image URLs mismatch")
			for _, wantText := range tt.wantTextParts {
				found := false
				for _, gotText := range gotTextParts {
					if assert.ObjectsAreEqual(wantText, gotText) {
						found = true
						break
					}
				}
				assert.True(t, found, "expected text part %q not found in %v", wantText, gotTextParts)
			}
		})
	}
}

func TestRenderPrompt_ErrorDirectives(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name        string
		doc         map[string]any
		template    string
		wantErr     error
		errContains string
	}{
		{
			// Use triple-stache {{{...}}} to bypass Handlebars HTML escaping,
			// matching production behavior where helpers return raymond.SafeString.
			name:     "permanent 404 error directive",
			template: "{{{title}}}",
			doc: map[string]any{
				"title": "<<<error:status=404 message=Not Found>>>",
			},
			wantErr:     ErrPermanentPromptFailure,
			errContains: "Not Found",
		},
		{
			name:     "permanent 410 error directive",
			template: "{{{title}}}",
			doc: map[string]any{
				"title": "<<<error:status=410 message=Gone>>>",
			},
			wantErr:     ErrPermanentPromptFailure,
			errContains: "Gone",
		},
		{
			name:     "transient 503 error directive",
			template: "{{{title}}}",
			doc: map[string]any{
				"title": "<<<error:status=503 message=Service Unavailable>>>",
			},
			wantErr:     ErrTransientPromptFailure,
			errContains: "Service Unavailable",
		},
		{
			name:     "transient no-status error directive",
			template: "{{{title}}}",
			doc: map[string]any{
				"title": "<<<error:message=connection refused>>>",
			},
			wantErr:     ErrTransientPromptFailure,
			errContains: "connection refused",
		},
		{
			name:     "mixed permanent and transient — permanent wins",
			template: "{{{a}}} {{{b}}}",
			doc: map[string]any{
				"a": "<<<error:status=503 message=Unavailable>>>",
				"b": "<<<error:status=404 message=Not Found>>>",
			},
			wantErr:     ErrPermanentPromptFailure,
			errContains: "Not Found",
		},
		{
			name:     "429 is transient",
			template: "{{{title}}}",
			doc: map[string]any{
				"title": "<<<error:status=429 message=Rate Limited>>>",
			},
			wantErr:     ErrTransientPromptFailure,
			errContains: "Rate Limited",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := filepath.Join(tempDir, tt.name)
			idxCfg := NewEmbeddingsConfig("test_idx", EmbeddingsIndexConfig{
				Dimension: 3,
				Template:  tt.template,
			})
			idx, err := NewEmbeddingIndex(logger, nil, db, testDir, "test_idx", idxCfg, nil)
			require.NoError(t, err)

			ei := idx.(*EmbeddingIndex)
			_, _, err = ei.RenderPrompt(tt.doc)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

func TestResolveSearchEffort(t *testing.T) {
	var idx EmbeddingIndex
	f32 := func(v float32) *float32 { return &v }

	t.Run("nil effort uses balanced defaults", func(t *testing.T) {
		req := &vectorindex.SearchRequest{K: 10}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 2080, *req.SearchWidth)
		assert.InDelta(t, 7.0, *req.Epsilon2, 0.01)
	})

	t.Run("NaN effort falls back to balanced defaults", func(t *testing.T) {
		nan := float32(math.NaN())
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: &nan}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 2080, *req.SearchWidth)
		assert.InDelta(t, 7.0, *req.Epsilon2, 0.01)
	})

	t.Run("effort 0.0 gives minimum values", func(t *testing.T) {
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(0.0)}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 64, *req.SearchWidth) // max(10, 64) = 64
		assert.InDelta(t, 1.0, *req.Epsilon2, 0.01)
	})

	t.Run("effort 0.5 gives default epsilon", func(t *testing.T) {
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(0.5)}
		idx.resolveSearchEffort(req)
		assert.InDelta(t, 7.0, *req.Epsilon2, 0.01)
	})

	t.Run("effort 1.0 gives maximum values", func(t *testing.T) {
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(1.0)}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 4096, *req.SearchWidth) // max(64*20, 4096) = 4096
		assert.InDelta(t, 100.0, *req.Epsilon2, 0.01)
	})

	t.Run("large index scales upper effort width by total nodes", func(t *testing.T) {
		idx.idx = &stubStatsVectorIndex{stats: map[string]any{"total_nodes": uint64(17_591)}}
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(1.0)}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 17_591, *req.SearchWidth)
		assert.InDelta(t, 100.0, *req.Epsilon2, 0.01)
	})

	t.Run("mid effort stays on the balanced preset even on large index", func(t *testing.T) {
		idx.idx = &stubStatsVectorIndex{stats: map[string]any{"total_nodes": uint64(17_591)}}
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(0.5)}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 2080, *req.SearchWidth) // legacy midpoint between 64 and 4096
		assert.InDelta(t, 7.0, *req.Epsilon2, 0.01)
	})

	t.Run("small index does not reduce legacy cap", func(t *testing.T) {
		idx.idx = &stubStatsVectorIndex{stats: map[string]any{"total_nodes": uint64(879)}}
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(1.0)}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 4096, *req.SearchWidth)
	})

	t.Run("negative effort clamped to 0", func(t *testing.T) {
		idx.idx = nil
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(-0.5)}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 64, *req.SearchWidth)
		assert.InDelta(t, 1.0, *req.Epsilon2, 0.01)
	})

	t.Run("effort > 1 clamped to 1", func(t *testing.T) {
		idx.idx = nil
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(5.0)}
		idx.resolveSearchEffort(req)
		assert.InDelta(t, 100.0, *req.Epsilon2, 0.01)
	})

	t.Run("explicit SearchWidth not overwritten", func(t *testing.T) {
		idx.idx = nil
		w := 500
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(1.0), SearchWidth: &w}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 500, *req.SearchWidth)
		assert.NotNil(t, req.Epsilon2) // epsilon2 still set
	})

	t.Run("explicit Epsilon2 not overwritten", func(t *testing.T) {
		idx.idx = nil
		e := float32(3.0)
		req := &vectorindex.SearchRequest{K: 10, SearchEffort: f32(1.0), Epsilon2: &e}
		idx.resolveSearchEffort(req)
		assert.Equal(t, float32(3.0), *req.Epsilon2)
		assert.NotNil(t, req.SearchWidth) // search width still set
	})

	t.Run("explicit width without effort keeps epsilon at index default", func(t *testing.T) {
		idx.idx = nil
		w := 500
		req := &vectorindex.SearchRequest{K: 10, SearchWidth: &w}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 500, *req.SearchWidth)
		assert.Nil(t, req.Epsilon2)
	})

	t.Run("explicit epsilon without effort keeps width at index default", func(t *testing.T) {
		idx.idx = nil
		e := float32(3.0)
		req := &vectorindex.SearchRequest{K: 10, Epsilon2: &e}
		idx.resolveSearchEffort(req)
		assert.Nil(t, req.SearchWidth)
		assert.Equal(t, float32(3.0), *req.Epsilon2)
	})

	t.Run("large K scales search width", func(t *testing.T) {
		idx.idx = nil
		req := &vectorindex.SearchRequest{K: 500, SearchEffort: f32(0.0)}
		idx.resolveSearchEffort(req)
		assert.Equal(t, 500, *req.SearchWidth) // max(500, 64) = 500
	})
}
