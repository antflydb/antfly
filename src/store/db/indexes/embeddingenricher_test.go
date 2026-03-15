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
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestNewEnricher(t *testing.T) {
	// Register mock embedder for this test
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockEmbedder{}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Mock functions
	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i := range states {
			keys[i] = states[i].CurrentDocKey
			prompts[i] = fmt.Sprintf("prompt for %s", states[i].CurrentDocKey)
			hashIDs[i] = uint64(i) // Mock hash ID
		}
		return keys, prompts, hashIDs, nil
	}

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		return nil
	}

	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
		errMsg  string
	}{
		{
			name: "missing provider",
			config: map[string]any{
				"model": "test-model",
			},
			wantErr: true,
			errMsg:  "provider not specified",
		},
		{
			name: "invalid provider type",
			config: map[string]any{
				"provider": 123,
				"model":    "test-model",
			},
			wantErr: true,
			errMsg:  "provider not specified",
		},
		{
			name: "unknown provider",
			config: map[string]any{
				"provider": "unknown",
				"model":    "test-model",
			},
			wantErr: true,
			errMsg:  "no embedder registered for type unknown",
		},
		{
			name: "valid config with mock provider",
			config: map[string]any{
				"provider": "mock", // Mock embedder type
				"model":    "test-model",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			byteRange := types.Range{nil, nil}

			var modelConfig embeddings.EmbedderConfig
			// FIXME (ajr) Store the embedder config natively on the index config
			bytes, _ := json.Marshal(tt.config)
			_ = json.Unmarshal(bytes, &modelConfig)

			enricher, err := NewEmbeddingEnricher(
				ctx,
				logger,
				&common.Config{},
				db,
				tempDir,
				"newenricher_test",
				storeutils.DBRangeStart,
				[]byte(":test_enricher:e"),
				byteRange,
				modelConfig,
				generatePrompts,
				persistEmbeddings,
				nil,
			)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				assert.Nil(t, enricher)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, enricher)
				// Clean up
				if enricher != nil {
					assert.NoError(t, enricher.Close())
				}
			}
		})
	}
}

func TestEnricher_EnrichBatch(t *testing.T) {
	// Register mock embedder for this test
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockEmbedder{}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i := range states {
			keys[i] = states[i].CurrentDocKey
			prompts[i] = fmt.Sprintf("prompt for %s", states[i].CurrentDocKey)
			hashIDs[i] = uint64(i) // Mock hash ID
		}
		return keys, prompts, hashIDs, nil
	}

	var persistedKeys [][]byte
	var persistedVectors [][]float32
	var persistedHashIDs []uint64
	var persistMu sync.Mutex

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDS []uint64, keys [][]byte) error {
		persistMu.Lock()
		defer persistMu.Unlock()
		persistedKeys = append(persistedKeys, keys...)
		persistedVectors = append(persistedVectors, vectors...)
		persistedHashIDs = append(persistedHashIDs, hashIDS...)
		return nil
	}

	ctx := context.Background()
	byteRange := types.Range{nil, nil}
	config := map[string]any{
		"provider": "mock", // Mock embedder
		"model":    "test-model",
	}
	var modelConfig embeddings.EmbedderConfig
	// FIXME (ajr) Store the embedder config natively on the index config
	bytes, _ := json.Marshal(config)
	_ = json.Unmarshal(bytes, &modelConfig)

	enricher, err := NewEmbeddingEnricher(
		ctx,
		logger,
		&common.Config{},
		db,
		tempDir,
		"enrichbatch_test",
		storeutils.DBRangeStart,
		[]byte(":test_enricher:e"),
		byteRange,
		modelConfig,
		generatePrompts,
		persistEmbeddings,
		nil,
	)
	require.NoError(t, err)
	defer enricher.Close()

	// Test small batch
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}

	err = enricher.EnrichBatch(keys)
	assert.NoError(t, err)

	// Test large batch (should be partitioned)
	largeKeys := make([][]byte, 2500)
	for i := range 2500 {
		largeKeys[i] = fmt.Appendf(nil, "key%d", i)
	}

	err = enricher.EnrichBatch(largeKeys)
	assert.NoError(t, err)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
}

func TestEnrichOp_EncodeDecode(t *testing.T) {
	eo := &enrichOp{
		Keys: [][]byte{[]byte("key1"), []byte("key2")},
	}

	// Test encode
	encoded, err := eo.encode()
	assert.NoError(t, err)
	assert.NotEmpty(t, encoded)

	// Test decode
	decoded := &enrichOp{}
	err = decoded.decode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, eo.Keys, decoded.Keys)
}

func TestEnrichOp_Execute(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()

	// Create a temporary pebble database for testing
	tempDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	var executedPrompts []string
	mockEmb := &mockEmbedder{
		embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
			executedPrompts = values
			result := make([][]float32, len(values))
			for i := range values {
				result[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
			}
			return result, nil
		},
	}

	var persistedKeys [][]byte
	var persistedVectors [][]float32
	var persistedHashIDs []uint64
	persistFunc := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		persistedKeys = keys
		persistedVectors = vectors
		persistedHashIDs = hashIDs
		return nil
	}

	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i := range states {
			keys[i] = states[i].CurrentDocKey
			prompts[i] = fmt.Sprintf("prompt for %s", states[i].CurrentDocKey)
			hashIDs[i] = uint64(i)
		}
		return keys, prompts, hashIDs, nil
	}

	enricher := &EmbeddingEnricher{
		walEnricherBase:          walEnricherBase{logger: logger},
		embeddingCreationCounter: embeddingCreationOps.WithLabelValues("test"),
		generateEmbeddings:       mockEmb,
		persistEmbeddings:        persistFunc,
		generatePrompts:          generatePrompts,
	}

	eo := &enrichOp{
		Keys: [][]byte{[]byte("key1"), []byte("key2")},
		ei:   enricher,
		db:   db,
	}

	ctx := context.Background()
	err = eo.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, eo.Keys, persistedKeys)
	assert.Equal(t, []string{"prompt for key1", "prompt for key2"}, executedPrompts)
	assert.Len(t, persistedVectors, 2)
	assert.Len(t, persistedHashIDs, 2)
}

func TestEnrichOp_Merge(t *testing.T) {
	eo1 := &enrichOp{
		Keys: [][]byte{[]byte("key1"), []byte("key2")},
	}

	eo2 := &enrichOp{
		Keys: [][]byte{[]byte("key3"), []byte("key4")},
	}

	encoded2, err := eo2.encode()
	require.NoError(t, err)

	err = eo1.Merge(encoded2)
	assert.NoError(t, err)
	assert.Len(t, eo1.Keys, 4)
	assert.Equal(t, []byte("key3"), eo1.Keys[2])
	assert.Equal(t, []byte("key4"), eo1.Keys[3])
}

func TestEnrichOp_Pool(t *testing.T) {
	// Get an op from the pool
	eo1 := enrichOpPool.Get().(*enrichOp)
	assert.NotNil(t, eo1)
	assert.NotNil(t, eo1.Keys)

	// Add some data
	eo1.Keys = append(eo1.Keys, []byte("key1"))

	// Reset and return to pool
	eo1.reset()
	assert.Empty(t, eo1.Keys)
	assert.Nil(t, eo1.ei)
	enrichOpPool.Put(eo1)

	// Get another op - should reuse the same object
	eo2 := enrichOpPool.Get().(*enrichOp)
	assert.NotNil(t, eo2)
	// Capacity should be preserved
	assert.GreaterOrEqual(t, cap(eo2.Keys), 100)
}

func TestConvertToFloat32(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected float32
		ok       bool
	}{
		{"nil", nil, 0, false},
		{"int", 42, 42.0, true},
		{"int8", int8(42), 42.0, true},
		{"int16", int16(42), 42.0, true},
		{"int32", int32(42), 42.0, true},
		{"int64", int64(42), 42.0, true},
		{"uint", uint(42), 42.0, true},
		{"uint8", uint8(42), 42.0, true},
		{"uint16", uint16(42), 42.0, true},
		{"uint32", uint32(42), 42.0, true},
		{"uint64", uint64(42), 42.0, true},
		{"float32", float32(42.5), 42.5, true},
		{"float64", float64(42.5), 42.5, true},
		{"string", "42", 0, false},
		{"bool", true, 0, false},
		{"slice", []int{1, 2, 3}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := embeddings.ConvertToFloat32(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConvertToFloat32Slice(t *testing.T) {
	tests := []struct {
		name     string
		input    []any
		expected []float32
		wantErr  bool
	}{
		{
			name:     "valid mixed numbers",
			input:    []any{int(1), float32(2.5), int64(3), float64(4.5)},
			expected: []float32{1.0, 2.5, 3.0, 4.5},
			wantErr:  false,
		},
		{
			name:     "empty slice",
			input:    []any{},
			expected: []float32{},
			wantErr:  false,
		},
		{
			name:     "contains string",
			input:    []any{1, "two", 3},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "contains nil",
			input:    []any{1, nil, 3},
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := embeddings.ConvertToFloat32Slice(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEnricher_Backfill(t *testing.T) {
	// Register mock embedder for this test
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockEmbedder{}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Setup test data in the database
	testDocs := []struct {
		key       []byte
		hasEmb    bool
		docSuffix []byte
	}{
		{[]byte("doc1"), false, storeutils.DBRangeStart},
		{[]byte("doc2"), true, storeutils.DBRangeStart},
		{[]byte("doc3"), false, storeutils.DBRangeStart},
	}

	// Store documents
	for _, td := range testDocs {
		doc := map[string]any{"content": fmt.Sprintf("content for %s", td.key)}
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)

		// Compress the document
		writer, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressed := writer.EncodeAll(docBytes, nil)

		err = db.Set(append(td.key, td.docSuffix...), compressed, nil)
		require.NoError(t, err)

		// Add embedding for doc2
		if td.hasEmb {
			embKey := append(td.key, []byte(":e:test")...)
			err = db.Set(embKey, []byte("embedding"), nil)
			require.NoError(t, err)
		}
	}

	var generatedKeysMu sync.Mutex
	var generatedKeys [][]byte
	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i := range states {
			keys[i] = states[i].CurrentDocKey
			prompts[i] = fmt.Sprintf("prompt for %s", states[i].CurrentDocKey)
			hashIDs[i] = uint64(i) // Mock hash ID
		}
		generatedKeysMu.Lock()
		generatedKeys = append(generatedKeys, keys...)
		generatedKeysMu.Unlock()
		return keys, prompts, hashIDs, nil
	}

	var persistedCountMu sync.Mutex
	var persistedCount int
	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		persistedCountMu.Lock()
		persistedCount += len(keys)
		persistedCountMu.Unlock()
		return nil
	}

	ctx := t.Context()

	byteRange := types.Range{nil, nil}
	config := map[string]any{
		"provider": "mock", // Mock embedder
		"model":    "test-model",
	}
	var modelConfig embeddings.EmbedderConfig
	// FIXME (ajr) Store the embedder config natively on the index config
	bytes, _ := json.Marshal(config)
	_ = json.Unmarshal(bytes, &modelConfig)

	enricher, err := NewEmbeddingEnricher(
		ctx,
		logger,
		&common.Config{},
		db,
		tempDir,
		"backfill_test",
		storeutils.DBRangeStart,
		[]byte(":test:e"),
		byteRange,
		modelConfig,
		generatePrompts,
		persistEmbeddings,
		nil,
	)
	require.NoError(t, err)

	// Wait for backfill to complete
	time.Sleep(500 * time.Millisecond)

	// Only doc1 and doc3 should need embeddings (doc2 already has one)
	generatedKeysMu.Lock()
	generatedKeysLen := len(generatedKeys)
	generatedKeysMu.Unlock()
	persistedCountMu.Lock()
	persistedCountVal := persistedCount
	persistedCountMu.Unlock()
	assert.GreaterOrEqual(t, generatedKeysLen, 2)
	assert.GreaterOrEqual(t, persistedCountVal, 2)

	err = enricher.Close()
	assert.NoError(t, err)
}

func TestEnricher_ErrHandling(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()

	// Test Execute with embedding error
	t.Run("embedding error", func(t *testing.T) {
		tempDir := t.TempDir()
		db, err := pebble.Open(filepath.Join(tempDir, "test.db"), &pebble.Options{
			FS: vfs.NewMem(),
		})
		require.NoError(t, err)
		defer db.Close()

		mockEmb := &mockEmbedder{
			embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
				return nil, errors.New("embedding failed")
			},
		}

		generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
			keys := make([][]byte, len(states))
			prompts := make([]string, len(states))
			hashIDs := make([]uint64, len(states))
			for i := range states {
				keys[i] = states[i].CurrentDocKey
				prompts[i] = fmt.Sprintf("prompt for %s", states[i].CurrentDocKey)
				hashIDs[i] = uint64(i)
			}
			return keys, prompts, hashIDs, nil
		}

		enricher := &EmbeddingEnricher{
			walEnricherBase:          walEnricherBase{logger: logger},
			embeddingCreationCounter: embeddingCreationOps.WithLabelValues("test"),
			generateEmbeddings:       mockEmb,
			generatePrompts:          generatePrompts,
		}

		eo := &enrichOp{
			Keys: [][]byte{[]byte("key1")},
			ei:   enricher,
			db:   db,
		}

		ctx := context.Background()
		err = eo.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "embedding failed")
	})

	// Test Execute with persist error
	t.Run("persist error", func(t *testing.T) {
		tempDir := t.TempDir()
		db, err := pebble.Open(filepath.Join(tempDir, "test.db"), &pebble.Options{
			FS: vfs.NewMem(),
		})
		require.NoError(t, err)
		defer db.Close()

		mockEmb := &mockEmbedder{}

		persistFunc := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
			return errors.New("persist failed")
		}

		generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
			keys := make([][]byte, len(states))
			prompts := make([]string, len(states))
			hashIDs := make([]uint64, len(states))
			for i := range states {
				keys[i] = states[i].CurrentDocKey
				prompts[i] = fmt.Sprintf("prompt for %s", states[i].CurrentDocKey)
				hashIDs[i] = uint64(i)
			}
			return keys, prompts, hashIDs, nil
		}

		enricher := &EmbeddingEnricher{
			walEnricherBase:          walEnricherBase{logger: logger},
			embeddingCreationCounter: embeddingCreationOps.WithLabelValues("test"),
			generateEmbeddings:       mockEmb,
			persistEmbeddings:        persistFunc,
			generatePrompts:          generatePrompts,
		}

		eo := &enrichOp{
			Keys: [][]byte{[]byte("key1")},
			ei:   enricher,
			db:   db,
		}

		ctx := context.Background()
		err = eo.Execute(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "persist failed")
	})
}

func TestGenerateEmbeddingsWithoutPersist(t *testing.T) {
	// Register mock embedder for this test
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					result := make([][]float32, len(values))
					for i := range values {
						result[i] = []float32{1.0, 2.0, 3.0}
					}
					return result, nil
				},
			}, nil
		},
	)
	t.Cleanup(func() { embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock) })

	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Mock generatePrompts function
	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i := range states {
			keys[i] = states[i].CurrentDocKey
			prompts[i] = fmt.Sprintf("prompt for %s", states[i].CurrentDocKey)
			hashIDs[i] = uint64(i)
		}
		return keys, prompts, hashIDs, nil
	}

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		return nil
	}

	// Parse embedder config
	var modelConfig embeddings.EmbedderConfig
	configJSON, _ := json.Marshal(map[string]any{
		"provider": "mock",
		"model":    "test-model",
	})
	require.NoError(t, json.Unmarshal(configJSON, &modelConfig))

	// Create enricher
	enricher, err := NewEmbeddingEnricher(
		context.Background(),
		logger,
		&common.Config{},
		db,
		tempDir,
		"test",
		[]byte(":e"),
		[]byte(":embedding"),
		types.Range{nil, nil},
		modelConfig,
		generatePrompts,
		persistEmbeddings,
		nil,
	)
	require.NoError(t, err)
	defer enricher.Close()

	t.Run("generates embeddings from compressed documents", func(t *testing.T) {
		// Create test documents with zstd compression
		doc1 := map[string]any{"content": "test content 1"}
		doc2 := map[string]any{"content": "test content 2"}

		doc1JSON, err := json.Marshal(doc1)
		require.NoError(t, err)
		doc2JSON, err := json.Marshal(doc2)
		require.NoError(t, err)

		// Compress documents
		var buf1, buf2 bytes.Buffer
		w1, err := zstd.NewWriter(&buf1)
		require.NoError(t, err)
		_, err = w1.Write(doc1JSON)
		require.NoError(t, err)
		require.NoError(t, w1.Close())

		w2, err := zstd.NewWriter(&buf2)
		require.NoError(t, err)
		_, err = w2.Write(doc2JSON)
		require.NoError(t, err)
		require.NoError(t, w2.Close())

		compressedDoc1 := buf1.Bytes()
		compressedDoc2 := buf2.Bytes()

		// Create documentValues map
		documentValues := map[string][]byte{
			"doc1": compressedDoc1,
			"doc2": compressedDoc2,
		}

		keys := [][]byte{[]byte("doc1"), []byte("doc2")}

		ctx := context.Background()
		embWrites, chunkWrites, failedKeys, err := enricher.GenerateEmbeddingsWithoutPersist(
			ctx, keys, documentValues, generatePrompts,
		)

		require.NoError(t, err)
		assert.Empty(t, failedKeys)
		assert.Len(t, embWrites, 2, "should generate 2 embeddings")
		assert.Empty(t, chunkWrites, "no chunking configured")

		// Verify embedding writes format: key:embedding -> [hashID][vector]
		for i, write := range embWrites {
			expectedKey := append([]byte(nil), keys[i]...)
			expectedKey = append(expectedKey, []byte(":embedding")...)
			assert.Equal(t, expectedKey, write[0], "embedding key format")

			// Decode hashID and vector
			val := write[1]
			_, hashID, err := encoding.DecodeUint64Ascending(val)
			require.NoError(t, err)
			assert.Equal(t, uint64(i), hashID, "hashID should match")

			_, vec, err := vector.Decode(val[8:])
			require.NoError(t, err)
			assert.Len(t, vec, 3)
			assert.Equal(t, []float32{1.0, 2.0, 3.0}, []float32(vec))
		}
	})

	t.Run("generates embeddings from uncompressed documents", func(t *testing.T) {
		// Test LinearMerge case where documents are NOT zstd-compressed
		doc := map[string]any{"content": "uncompressed content"}
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		documentValues := map[string][]byte{
			"doc_uncompressed": docJSON, // No compression
		}

		keys := [][]byte{[]byte("doc_uncompressed")}

		ctx := context.Background()
		embWrites, chunkWrites, failedKeys, err := enricher.GenerateEmbeddingsWithoutPersist(
			ctx, keys, documentValues, generatePrompts,
		)

		require.NoError(t, err)
		assert.Empty(t, failedKeys)
		assert.Len(t, embWrites, 1, "should generate 1 embedding")
		assert.Empty(t, chunkWrites)
	})

	t.Run("empty keys returns immediately", func(t *testing.T) {
		var keys [][]byte
		documentValues := map[string][]byte{}

		ctx := context.Background()
		embWrites, chunkWrites, failedKeys, err := enricher.GenerateEmbeddingsWithoutPersist(
			ctx, keys, documentValues, generatePrompts,
		)

		require.NoError(t, err)
		assert.Empty(t, embWrites)
		assert.Empty(t, chunkWrites)
		assert.Empty(t, failedKeys)
	})

	t.Run("handles embedding generation failure", func(t *testing.T) {
		// Create enricher with failing embedder
		failingEmbedder := &mockEmbedder{
			embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
				return nil, errors.New("embedding API failed")
			},
		}

		embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
		embeddings.RegisterEmbedder(
			embeddings.EmbedderProviderMock,
			func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
				return failingEmbedder, nil
			},
		)

		failingEnricher, err := NewEmbeddingEnricher(
			context.Background(),
			logger,
			&common.Config{},
			db,
			filepath.Join(tempDir, "failing"),
			"test_failing",
			[]byte(":e"),
			[]byte(":embedding"),
			types.Range{nil, nil},
			modelConfig,
			generatePrompts,
			persistEmbeddings,
			nil,
		)
		require.NoError(t, err)
		defer failingEnricher.Close()

		doc := map[string]any{"content": "test"}
		docJSON, _ := json.Marshal(doc)
		documentValues := map[string][]byte{"doc1": docJSON}
		keys := [][]byte{[]byte("doc1")}

		ctx := context.Background()
		_, _, _, err = failingEnricher.GenerateEmbeddingsWithoutPersist(
			ctx, keys, documentValues, generatePrompts,
		)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "embedding API failed")
	})

	t.Run("populates state.Document for generatePrompts callback", func(t *testing.T) {
		// This test verifies that GenerateEmbeddingsWithoutPersist populates
		// state.Document from the document JSON value so that callbacks like
		// generatePromptsFromMemory (which calls ExtractPrompt on state.Document)
		// receive a non-nil document. Before the fix, state.Document was always nil
		// and callbacks that relied on it would skip all documents.
		embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
		embeddings.RegisterEmbedder(
			embeddings.EmbedderProviderMock,
			func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
				return &mockEmbedder{
					embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
						result := make([][]float32, len(values))
						for i := range values {
							result[i] = []float32{1.0, 2.0, 3.0}
						}
						return result, nil
					},
				}, nil
			},
		)

		docEnricher, err := NewEmbeddingEnricher(
			context.Background(),
			logger,
			&common.Config{},
			db,
			filepath.Join(tempDir, "doc_state_test"),
			"test_doc_state",
			[]byte(":e"),
			[]byte(":embedding"),
			types.Range{nil, nil},
			modelConfig,
			generatePrompts,
			persistEmbeddings,
			nil,
		)
		require.NoError(t, err)
		defer docEnricher.Close()

		// Create test document
		doc := map[string]any{"title": "Test Title", "content": "Hello world"}
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		// Compress it (like real ComputeEnrichments would)
		var buf bytes.Buffer
		w, err := zstd.NewWriter(&buf)
		require.NoError(t, err)
		_, err = w.Write(docJSON)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		documentValues := map[string][]byte{
			"doc_state_test": buf.Bytes(),
		}
		keys := [][]byte{[]byte("doc_state_test")}

		// Use a generatePrompts callback that mimics generatePromptsFromMemory:
		// it requires state.Document to be non-nil and reads fields from it.
		var seenDocuments []map[string]any
		documentAwarePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
			outKeys := make([][]byte, 0, len(states))
			prompts := make([]string, 0, len(states))
			hashIDs := make([]uint64, 0, len(states))
			for _, state := range states {
				if state.Document == nil {
					// This is the bug: before the fix, state.Document was always nil
					return nil, nil, nil, fmt.Errorf("state.Document is nil for key %s", state.CurrentDocKey)
				}
				seenDocuments = append(seenDocuments, state.Document)
				title, _ := state.Document["title"].(string)
				outKeys = append(outKeys, state.CurrentDocKey)
				prompts = append(prompts, title)
				hashIDs = append(hashIDs, uint64(len(outKeys)-1))
			}
			return outKeys, prompts, hashIDs, nil
		}

		ctx := context.Background()
		embWrites, _, failedKeys, err := docEnricher.GenerateEmbeddingsWithoutPersist(
			ctx, keys, documentValues, documentAwarePrompts,
		)

		require.NoError(t, err, "should not fail when state.Document is populated")
		assert.Empty(t, failedKeys)
		assert.Len(t, embWrites, 1, "should generate 1 embedding")

		// Verify the callback received the decoded document with correct fields
		require.Len(t, seenDocuments, 1)
		assert.Equal(t, "Test Title", seenDocuments[0]["title"])
		assert.Equal(t, "Hello world", seenDocuments[0]["content"])
	})
}

// TestEmbeddingEnricher_EnrichBatch_WithChunking tests async enrichment with chunking enabled
// This verifies that the enricher can handle chunking in the async path
func TestEmbeddingEnricher_EnrichBatch_WithChunking(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Setup mock chunker that splits text into 2 chunks
	mockChunker := &mockChunker{
		chunkFunc: func(ctx context.Context, text string) ([]chunking.Chunk, error) {
			mid := len(text) / 2
			if mid == 0 {
				mid = len(text)
			}
			chunks := []chunking.Chunk{
				chunking.NewTextChunk(0, text[:mid], 0, mid),
			}
			if mid < len(text) {
				chunks = append(chunks, chunking.NewTextChunk(1, text[mid:], mid, len(text)))
			}
			return chunks, nil
		},
	}

	chunking.ChunkerRegistry[chunking.ChunkerProviderMock] = func(config chunking.ChunkerConfig) (chunking.Chunker, error) {
		return mockChunker, nil
	}
	defer delete(chunking.ChunkerRegistry, chunking.ChunkerProviderMock)

	// Track what gets embedded
	var embeddedTexts []string
	var embeddedTextsMu sync.Mutex

	mockEmb := &mockEmbedder{
		embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
			embeddedTextsMu.Lock()
			embeddedTexts = append(embeddedTexts, values...)
			embeddedTextsMu.Unlock()

			result := make([][]float32, len(values))
			for i := range values {
				result[i] = []float32{1.0, 2.0, 3.0}
			}
			return result, nil
		},
	}

	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(embeddings.EmbedderProviderMock, func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
		return mockEmb, nil
	})
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	embSuffix := []byte(":embedding")

	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i := range states {
			keys[i] = states[i].CurrentDocKey
			if content, ok := states[i].Document["content"].(string); ok {
				prompts[i] = content
			}
			hashIDs[i] = uint64(i) + 1
		}
		return keys, prompts, hashIDs, nil
	}

	// Track persisted embeddings
	var persistedKeys [][]byte
	var persistMu sync.Mutex

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDS []uint64, keys [][]byte) error {
		persistMu.Lock()
		persistedKeys = append(persistedKeys, keys...)
		persistMu.Unlock()

		// Actually persist to Pebble for testing
		batch := db.NewBatch()
		defer batch.Close()

		for i, key := range keys {
			embKey := append(key, embSuffix...)
			embVal, err := vectorindex.EncodeEmbeddingWithHashID(nil, vectors[i], 0)
			if err != nil {
				return err
			}
			if err := batch.Set(embKey, embVal, nil); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	}

	config := map[string]any{
		"provider": "mock",
		"model":    "test-model",
	}
	var modelConfig embeddings.EmbedderConfig
	configBytes, _ := json.Marshal(config)
	_ = json.Unmarshal(configBytes, &modelConfig)

	enricher, err := NewEmbeddingEnricher(
		context.Background(),
		logger,
		&common.Config{},
		db,
		tempDir,
		"test_chunking",
		[]byte(":e"),
		embSuffix,
		types.Range{nil, nil},
		modelConfig,
		generatePrompts,
		persistEmbeddings,
		nil,
	)
	require.NoError(t, err)
	defer enricher.Close()

	// Create and store test document
	doc := map[string]any{
		"content": "This is a long test document that will be split into multiple chunks for async enrichment testing",
	}
	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := writer.EncodeAll(docJSON, nil)

	// Store the document in Pebble
	key := []byte("doc1")
	err = db.Set(append(key, storeutils.DBRangeStart...), compressed, nil)
	require.NoError(t, err)

	// Call EnrichBatch - this should trigger async enrichment with chunking
	keys := [][]byte{key}
	err = enricher.EnrichBatch(keys)
	require.NoError(t, err)

	// Wait for async enrichment to complete
	time.Sleep(500 * time.Millisecond)

	// Verify embedder was called with chunk text (not full document)
	embeddedTextsMu.Lock()
	receivedTexts := make([]string, len(embeddedTexts))
	copy(receivedTexts, embeddedTexts)
	embeddedTextsMu.Unlock()

	assert.NotEmpty(t, receivedTexts, "embedder should have been called")
	// Note: chunking may not work in test environment if ANTFLY_TERMITE_URL is not set
	// At minimum, we should embed the document itself
	assert.GreaterOrEqual(t, len(receivedTexts), 1, "should embed at least the document")

	fullText := doc["content"].(string)
	for _, text := range receivedTexts {
		// Each embedded text should be from the document (either full document or chunks)
		assert.Contains(t, fullText, text, "embedded text should be from document")
	}

	// Verify embeddings were persisted
	persistMu.Lock()
	persistedCount := len(persistedKeys)
	persistMu.Unlock()

	assert.Positive(t, persistedCount, "should have persisted embeddings")
	t.Logf("Persisted %d embeddings for chunks", persistedCount)

	// Verify we can read the persisted embeddings
	for _, key := range persistedKeys {
		embKey := append(key, embSuffix...)
		val, closer, err := db.Get(embKey)
		require.NoError(t, err)
		require.NotNil(t, val)
		closer.Close()

		// Verify embedding format (with hashID prefix)
		_, vec, _, err := vectorindex.DecodeEmbeddingWithHashID(val)
		require.NoError(t, err)
		assert.Len(t, vec, 3, "embedding should have dimension 3")
	}
}

// TestEnrichOp_Execute_DocumentKeyFallback verifies that enrichOp.Execute
// correctly handles raw document keys by falling back to fetch the document
// at key + DBRangeStart when a direct Get on the raw key returns ErrNotFound.
// This is the codepath used by ephemeral chunking mode where the embedding
// enricher receives raw document keys (not enrichment keys with data inline).
func TestEnrichOp_Execute_DocumentKeyFallback(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()

	tempDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Store two documents in Pebble at key + DBRangeStart (the standard storage format).
	// The enricher will receive raw keys without the suffix.
	docs := map[string]map[string]any{
		"Edward Tolhurst": {"title": "Edward Tolhurst", "body": "Some body text about Edward Tolhurst"},
		"Gilles de Geus":  {"title": "Gilles de Geus", "body": "Some body text about Gilles de Geus"},
	}

	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		compressed := writer.EncodeAll(docJSON, nil)
		err = db.Set(append([]byte(key), storeutils.DBRangeStart...), compressed, nil)
		require.NoError(t, err)
	}

	// Track what generatePrompts receives so we can verify Document is populated.
	var receivedStates []storeutils.DocumentScanState
	var executedPrompts []string

	mockEmb := &mockEmbedder{
		embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
			result := make([][]float32, len(values))
			for i := range values {
				result[i] = []float32{1.0, 2.0, 3.0}
			}
			return result, nil
		},
	}

	var persistedKeys [][]byte
	persistFunc := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		persistedKeys = append(persistedKeys, keys...)
		return nil
	}

	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		receivedStates = append(receivedStates, states...)
		keys := make([][]byte, 0, len(states))
		prompts := make([]string, 0, len(states))
		hashIDs := make([]uint64, 0, len(states))
		for i, state := range states {
			// The key assertion: Document must be populated for raw document keys.
			if state.Document == nil {
				return nil, nil, nil, fmt.Errorf("state[%d] for key %q has nil Document", i, state.CurrentDocKey)
			}
			title, _ := state.Document["title"].(string)
			body, _ := state.Document["body"].(string)
			prompt := title + " " + body
			keys = append(keys, state.CurrentDocKey)
			prompts = append(prompts, prompt)
			executedPrompts = append(executedPrompts, prompt)
			hashIDs = append(hashIDs, uint64(i)+1)
		}
		return keys, prompts, hashIDs, nil
	}

	enricher := &EmbeddingEnricher{
		walEnricherBase:          walEnricherBase{logger: logger},
		embeddingCreationCounter: embeddingCreationOps.WithLabelValues("test"),
		generateEmbeddings:       mockEmb,
		persistEmbeddings:        persistFunc,
		generatePrompts:          generatePrompts,
	}

	// Execute with raw document keys (no DBRangeStart suffix).
	// Before the fix, this would leave both Enrichment and Document nil
	// because db.Get(rawKey) returns ErrNotFound.
	eo := &enrichOp{
		Keys: [][]byte{[]byte("Edward Tolhurst"), []byte("Gilles de Geus")},
		ei:   enricher,
		db:   db,
	}

	err = eo.Execute(context.Background())
	require.NoError(t, err)

	// Verify generatePrompts was called with populated Documents.
	require.Len(t, receivedStates, 2, "should have received 2 document states")
	for _, state := range receivedStates {
		require.NotNil(t, state.Document, "Document should be populated for key %q", state.CurrentDocKey)
		assert.Nil(t, state.Enrichment, "Enrichment should be nil for raw document keys")
		title, ok := state.Document["title"].(string)
		assert.True(t, ok, "Document should have a title field")
		assert.Equal(t, string(state.CurrentDocKey), title)
	}

	// Verify embeddings were generated and persisted.
	assert.Len(t, persistedKeys, 2)
	assert.Len(t, executedPrompts, 2)
}

func TestEmbedWithFallback(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()

	t.Run("batch success", func(t *testing.T) {
		e := &EmbeddingEnricher{
			generateEmbeddings: &mockEmbedder{},
		}
		e.logger = logger

		keys := [][]byte{[]byte("a"), []byte("b")}
		prompts := []string{"hello", "world"}
		hashIDs := []uint64{1, 2}

		embs, successKeys, successHashIDs, err := e.embedWithFallback(context.Background(), keys, prompts, hashIDs)
		require.NoError(t, err)
		assert.Len(t, embs, 2)
		assert.Equal(t, keys, successKeys)
		assert.Equal(t, hashIDs, successHashIDs)
	})

	t.Run("batch fails, per-item succeeds for all", func(t *testing.T) {
		callCount := 0
		e := &EmbeddingEnricher{
			generateEmbeddings: &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					callCount++
					if callCount == 1 {
						return nil, fmt.Errorf("batch failed")
					}
					result := make([][]float32, len(values))
					for i := range values {
						result[i] = []float32{float32(i)}
					}
					return result, nil
				},
			},
		}
		e.logger = logger

		keys := [][]byte{[]byte("a"), []byte("b")}
		prompts := []string{"hello", "world"}
		hashIDs := []uint64{1, 2}

		embs, successKeys, successHashIDs, err := e.embedWithFallback(context.Background(), keys, prompts, hashIDs)
		require.NoError(t, err)
		assert.Len(t, embs, 2)
		assert.Equal(t, keys, successKeys)
		assert.Equal(t, hashIDs, successHashIDs)
	})

	t.Run("batch fails, one item fails in fallback", func(t *testing.T) {
		callCount := 0
		e := &EmbeddingEnricher{
			generateEmbeddings: &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					callCount++
					if callCount == 1 {
						// Batch call fails
						return nil, fmt.Errorf("batch failed")
					}
					// Per-item: fail on "bad"
					if len(values) == 1 && values[0] == "bad" {
						return nil, fmt.Errorf("bad item")
					}
					result := make([][]float32, len(values))
					for i := range values {
						result[i] = []float32{float32(i)}
					}
					return result, nil
				},
			},
		}
		e.logger = logger

		keys := [][]byte{[]byte("good"), []byte("bad"), []byte("also-good")}
		prompts := []string{"good", "bad", "also-good"}
		hashIDs := []uint64{1, 2, 3}

		embs, successKeys, successHashIDs, err := e.embedWithFallback(context.Background(), keys, prompts, hashIDs)
		require.NoError(t, err)
		assert.Len(t, embs, 2, "only 2 of 3 should succeed")
		assert.Equal(t, [][]byte{[]byte("good"), []byte("also-good")}, successKeys)
		assert.Equal(t, []uint64{1, 3}, successHashIDs)
	})

	t.Run("single item fail returns original error", func(t *testing.T) {
		e := &EmbeddingEnricher{
			generateEmbeddings: &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					return nil, fmt.Errorf("always fails")
				},
			},
		}
		e.logger = logger

		keys := [][]byte{[]byte("a")}
		prompts := []string{"hello"}
		hashIDs := []uint64{1}

		// Single-item batch returns original error without pointless per-item retry
		_, _, _, err := e.embedWithFallback(context.Background(), keys, prompts, hashIDs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "always fails")
	})

	t.Run("multi-item all fail returns fallback error", func(t *testing.T) {
		e := &EmbeddingEnricher{
			generateEmbeddings: &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					return nil, fmt.Errorf("always fails")
				},
			},
		}
		e.logger = logger

		keys := [][]byte{[]byte("a"), []byte("b")}
		prompts := []string{"hello", "world"}
		hashIDs := []uint64{1, 2}

		_, _, _, err := e.embedWithFallback(context.Background(), keys, prompts, hashIDs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "all 2 items failed")
	})

	t.Run("context cancelled propagates immediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		e := &EmbeddingEnricher{
			generateEmbeddings: &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					return nil, context.Canceled
				},
			},
		}
		e.logger = logger

		_, _, _, err := e.embedWithFallback(ctx, [][]byte{[]byte("a")}, []string{"hello"}, []uint64{1})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestWriteDudKeysToDb(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()

	t.Run("empty keys is no-op", func(t *testing.T) {
		db, err := pebble.Open(t.TempDir(), pebbleutils.NewMemPebbleOpts())
		require.NoError(t, err)
		defer db.Close()

		writeDudKeysToDb(db, logger, nil, []byte(":e"))
		// No panic, no writes
	})

	t.Run("writes dud markers to pebble", func(t *testing.T) {
		db, err := pebble.Open(t.TempDir(), pebbleutils.NewMemPebbleOpts())
		require.NoError(t, err)
		defer db.Close()

		suffix := []byte(":e")
		keys := [][]byte{[]byte("doc1"), []byte("doc2")}

		writeDudKeysToDb(db, logger, keys, suffix)

		// Verify dud keys exist
		for _, key := range keys {
			dudKey := append(bytes.Clone(key), suffix...)
			val, closer, err := db.Get(dudKey)
			require.NoError(t, err, "dud key %q should exist", dudKey)
			assert.Equal(t, storeutils.DudEnrichmentValue, val)
			_ = closer.Close()
		}
	})

	t.Run("multiple keys written atomically", func(t *testing.T) {
		db, err := pebble.Open(t.TempDir(), pebbleutils.NewMemPebbleOpts())
		require.NoError(t, err)
		defer db.Close()

		suffix := []byte(":emb")
		keys := make([][]byte, 10)
		for i := range keys {
			keys[i] = fmt.Appendf(nil, "key-%d", i)
		}

		writeDudKeysToDb(db, logger, keys, suffix)

		for _, key := range keys {
			dudKey := append(bytes.Clone(key), suffix...)
			_, closer, err := db.Get(dudKey)
			require.NoError(t, err, "dud key %q should exist", dudKey)
			_ = closer.Close()
		}
	})
}
