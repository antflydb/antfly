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
	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// mockChunker is a test helper for chunking
type mockChunker struct {
	chunkFunc func(ctx context.Context, text string) ([]chunking.Chunk, error)
}

func (m *mockChunker) Chunk(ctx context.Context, text string) ([]chunking.Chunk, error) {
	if m.chunkFunc != nil {
		return m.chunkFunc(ctx, text)
	}
	return nil, nil
}

func (m *mockChunker) ChunkMedia(ctx context.Context, data []byte, mimeType string) ([]chunking.Chunk, error) {
	return nil, nil
}

func (m *mockChunker) Close() error {
	return nil
}

// TestPipelineEnricher_OptionalSummarizer verifies that the summarizer is truly optional
func TestPipelineEnricher_OptionalSummarizer(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Register mock embedder
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
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	embSuffix := []byte(":e")
	sumSuffix := []byte(":s")
	byteRange := types.Range{nil, nil}

	embedderConfig := embeddings.EmbedderConfig{
		Provider: embeddings.EmbedderProviderMock,
	}

	var persistEmbeddingsCalledMu sync.Mutex
	persistEmbeddingsCalled := false
	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		persistEmbeddingsCalledMu.Lock()
		persistEmbeddingsCalled = true
		persistEmbeddingsCalledMu.Unlock()
		return nil
	}

	generatePrompts := func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i, state := range states {
			keys[i] = state.CurrentDocKey
			prompts[i] = string(state.CurrentDocKey)
			hashIDs[i] = uint64(i)
		}
		return keys, prompts, hashIDs, nil
	}

	ctx := t.Context()

	t.Run("WithoutSummarizer", func(t *testing.T) {
		persistEmbeddingsCalledMu.Lock()
		persistEmbeddingsCalled = false
		persistEmbeddingsCalledMu.Unlock()

		// Create pipeline enricher WITHOUT summarizer
		enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
			Logger:            logger,
			AntflyConfig:      &common.Config{},
			DB:                db,
			Dir:               tempDir,
			Name:              "test_no_sum",
			EmbSuffix:         embSuffix,
			SumSuffix:         sumSuffix,
			ByteRange:         byteRange,
			EmbedderConfig:    embedderConfig,
			GeneratePrompts:   generatePrompts,
			PersistEmbeddings: persistEmbeddings,
			StoreChunks:       true,
		})
		require.NoError(t, err)
		require.NotNil(t, enricher)
		defer enricher.Close()

		pe := enricher.(*PipelineEnricher)
		assert.Nil(t, pe.Summarizer, "Summarizer should be nil")
		assert.NotNil(t, pe.Terminal, "EmbeddingEnricher should not be nil")
		assert.Nil(t, pe.ChunkingEnricher, "ChunkingEnricher should be nil")

		// Test that document keys route directly to embedder
		docKey := []byte("doc1")
		err = pe.EnrichBatch([][]byte{docKey})
		require.NoError(t, err)

		// Wait for async processing
		time.Sleep(200 * time.Millisecond)
		persistEmbeddingsCalledMu.Lock()
		defer persistEmbeddingsCalledMu.Unlock()
		assert.True(t, persistEmbeddingsCalled, "Should embed documents directly without summarizer")
	})

	t.Run("WithSummarizer", func(t *testing.T) {
		persistEmbeddingsCalledMu.Lock()
		persistEmbeddingsCalled = false
		persistEmbeddingsCalledMu.Unlock()
		var persistSummariesCalledMu sync.Mutex
		persistSummariesCalled := false

		// Register mock summarizer
		ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)
		ai.RegisterDocumentSummarizer(
			ai.GeneratorProviderMock,
			func(ctx context.Context, config ai.GeneratorConfig) (ai.DocumentSummarizer, error) {
				return &mockSummarizer{}, nil
			},
		)
		defer ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)

		summarizerConfig := &ai.GeneratorConfig{
			Provider: ai.GeneratorProviderMock,
		}

		persistSummaries := func(ctx context.Context, keys [][]byte, hashIDs []uint64, summaries []string) error {
			persistSummariesCalledMu.Lock()
			persistSummariesCalled = true
			persistSummariesCalledMu.Unlock()
			return nil
		}

		// Create pipeline enricher WITH summarizer
		enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
			Logger:            logger,
			AntflyConfig:      &common.Config{},
			DB:                db,
			Dir:               tempDir,
			Name:              "test_with_sum",
			EmbSuffix:         embSuffix,
			SumSuffix:         sumSuffix,
			ByteRange:         byteRange,
			EmbedderConfig:    embedderConfig,
			SummarizerConfig:  summarizerConfig,
			GeneratePrompts:   generatePrompts,
			PersistEmbeddings: persistEmbeddings,
			PersistSummaries:  persistSummaries,
			StoreChunks:       true,
		})
		require.NoError(t, err)
		require.NotNil(t, enricher)
		defer enricher.Close()

		pe := enricher.(*PipelineEnricher)
		assert.NotNil(t, pe.Summarizer, "Summarizer should not be nil")
		assert.NotNil(t, pe.Terminal, "EmbeddingEnricher should not be nil")
		assert.Nil(t, pe.ChunkingEnricher, "ChunkingEnricher should be nil")

		// Test that document keys route to summarizer first
		docKey := []byte("doc1")
		err = pe.EnrichBatch([][]byte{docKey})
		require.NoError(t, err)

		// Wait for async processing
		time.Sleep(200 * time.Millisecond)
		persistSummariesCalledMu.Lock()
		defer persistSummariesCalledMu.Unlock()
		assert.True(t, persistSummariesCalled, "Should summarize documents first")
	})
}

// TestPipelineEnricher_KeyRouting verifies correct routing of different key types
func TestPipelineEnricher_KeyRouting(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Register mocks
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
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	embSuffix := []byte(":e")
	sumSuffix := []byte(":s")
	byteRange := types.Range{nil, nil}

	embedderConfig := embeddings.EmbedderConfig{
		Provider: embeddings.EmbedderProviderMock,
	}

	var embeddedKeysMu sync.Mutex
	var embeddedKeys []string
	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		embeddedKeysMu.Lock()
		defer embeddedKeysMu.Unlock()
		for _, key := range keys {
			embeddedKeys = append(embeddedKeys, string(key))
		}
		return nil
	}

	generatePrompts := func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i, state := range states {
			keys[i] = state.CurrentDocKey
			prompts[i] = string(state.CurrentDocKey)
			hashIDs[i] = uint64(i)
		}
		return keys, prompts, hashIDs, nil
	}

	ctx := t.Context()

	// Create pipeline enricher without summarizer (for simpler testing)
	enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
		Logger:            logger,
		AntflyConfig:      &common.Config{},
		DB:                db,
		Dir:               tempDir,
		Name:              "test_routing",
		EmbSuffix:         embSuffix,
		SumSuffix:         sumSuffix,
		ByteRange:         byteRange,
		EmbedderConfig:    embedderConfig,
		GeneratePrompts:   generatePrompts,
		PersistEmbeddings: persistEmbeddings,
		StoreChunks:       true,
	})
	require.NoError(t, err)
	require.NotNil(t, enricher)
	defer enricher.Close()

	pe := enricher.(*PipelineEnricher)

	t.Run("ChunkKeyRouting_NoSummarizer", func(t *testing.T) {
		embeddedKeysMu.Lock()
		embeddedKeys = nil
		embeddedKeysMu.Unlock()

		// Create a chunk key (contains :c: pattern)
		chunkKey := storeutils.MakeChunkKey([]byte("doc1"), "test_index", 0)

		err = pe.EnrichBatch([][]byte{chunkKey})
		require.NoError(t, err)

		// Wait for async processing
		time.Sleep(200 * time.Millisecond)

		// With no summarizer, chunk keys should go directly to embedder
		embeddedKeysMu.Lock()
		defer embeddedKeysMu.Unlock()
		assert.Len(t, embeddedKeys, 1, "Chunk key should be embedded directly")
		assert.Equal(t, string(chunkKey), embeddedKeys[0])
	})

	t.Run("SummaryKeyRouting", func(t *testing.T) {
		embeddedKeysMu.Lock()
		embeddedKeys = nil
		embeddedKeysMu.Unlock()

		// Create a summary key (has :s suffix)
		sumKey := append([]byte("doc1"), sumSuffix...)

		err = pe.EnrichBatch([][]byte{sumKey})
		require.NoError(t, err)

		// Wait for async processing
		time.Sleep(200 * time.Millisecond)

		// Summary keys always go to embedder
		embeddedKeysMu.Lock()
		defer embeddedKeysMu.Unlock()
		assert.Len(t, embeddedKeys, 1, "Summary key should be embedded")
		assert.Equal(t, string(sumKey), embeddedKeys[0])
	})

	t.Run("MixedKeyBatch", func(t *testing.T) {
		embeddedKeysMu.Lock()
		embeddedKeys = nil
		embeddedKeysMu.Unlock()

		// Mix of different key types
		docKey := []byte("doc1")
		chunkKey := storeutils.MakeChunkKey([]byte("doc2"), "test_index", 0)
		sumKey := append([]byte("doc3"), sumSuffix...)

		err = pe.EnrichBatch([][]byte{docKey, chunkKey, sumKey})
		require.NoError(t, err)

		// Wait for async processing
		time.Sleep(200 * time.Millisecond)

		// All should be embedded (no summarizer, no chunker)
		embeddedKeysMu.Lock()
		defer embeddedKeysMu.Unlock()
		assert.Len(t, embeddedKeys, 3, "All keys should be embedded")
	})
}

// TestPipelineEnricher_CloseWithNilComponents verifies Close() handles nil components
func TestPipelineEnricher_CloseWithNilComponents(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Register mock embedder
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					return nil, nil
				},
			}, nil
		},
	)
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	embSuffix := []byte(":e")
	sumSuffix := []byte(":s")
	byteRange := types.Range{nil, nil}

	embedderConfig := embeddings.EmbedderConfig{
		Provider: embeddings.EmbedderProviderMock,
	}

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		return nil
	}

	generatePrompts := func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		return nil, nil, nil, nil
	}

	ctx := t.Context()

	// Create enricher with nil summarizer and nil chunker
	enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
		Logger:            logger,
		AntflyConfig:      &common.Config{},
		DB:                db,
		Dir:               tempDir,
		Name:              "test_close",
		EmbSuffix:         embSuffix,
		SumSuffix:         sumSuffix,
		ByteRange:         byteRange,
		EmbedderConfig:    embedderConfig,
		GeneratePrompts:   generatePrompts,
		PersistEmbeddings: persistEmbeddings,
		StoreChunks:       true,
	})
	require.NoError(t, err)
	require.NotNil(t, enricher)

	// Close should not panic even with nil components
	err = enricher.Close()
	assert.NoError(t, err, "Close should handle nil components gracefully")
}

// TestPipelineEnricher_ChunkKeyWithSummarizer verifies chunk routing with summarizer
func TestPipelineEnricher_ChunkKeyWithSummarizer(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Register mocks
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
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)
	var summarizedKeysMu sync.Mutex
	var summarizedKeys []string
	ai.RegisterDocumentSummarizer(
		ai.GeneratorProviderMock,
		func(ctx context.Context, config ai.GeneratorConfig) (ai.DocumentSummarizer, error) {
			return &mockSummarizer{
				summarizeFunc: func(ctx context.Context, contents [][]ai.ContentPart, _ ...ai.GenerateOption) ([]string, error) {
					result := make([]string, len(contents))
					for i, content := range contents {
						if len(content) > 0 {
							if textPart, ok := content[0].(ai.TextContent); ok {
								summarizedKeysMu.Lock()
								summarizedKeys = append(summarizedKeys, textPart.Text)
								summarizedKeysMu.Unlock()
								result[i] = "Summary of: " + textPart.Text
							}
						}
					}
					return result, nil
				},
			}, nil
		},
	)
	defer ai.DeregisterDocumentSummarizer(ai.GeneratorProviderMock)

	embSuffix := []byte(":e")
	sumSuffix := []byte(":s")
	byteRange := types.Range{nil, nil}

	embedderConfig := embeddings.EmbedderConfig{
		Provider: embeddings.EmbedderProviderMock,
	}

	summarizerConfig := &ai.GeneratorConfig{
		Provider: ai.GeneratorProviderMock,
	}

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		return nil
	}

	persistSummaries := func(ctx context.Context, keys [][]byte, hashIDs []uint64, summaries []string) error {
		return nil
	}

	generatePrompts := func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, len(states))
		prompts := make([]string, len(states))
		hashIDs := make([]uint64, len(states))
		for i, state := range states {
			keys[i] = state.CurrentDocKey
			prompts[i] = string(state.CurrentDocKey)
			hashIDs[i] = uint64(i)
		}
		return keys, prompts, hashIDs, nil
	}

	ctx := t.Context()

	// Create pipeline enricher WITH summarizer
	enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
		Logger:            logger,
		AntflyConfig:      &common.Config{},
		DB:                db,
		Dir:               tempDir,
		Name:              "test_chunk_sum",
		EmbSuffix:         embSuffix,
		SumSuffix:         sumSuffix,
		ByteRange:         byteRange,
		EmbedderConfig:    embedderConfig,
		SummarizerConfig:  summarizerConfig,
		GeneratePrompts:   generatePrompts,
		PersistEmbeddings: persistEmbeddings,
		PersistSummaries:  persistSummaries,
		StoreChunks:       true,
	})
	require.NoError(t, err)
	require.NotNil(t, enricher)
	defer enricher.Close()

	pe := enricher.(*PipelineEnricher)

	// Test chunk key routing with summarizer
	summarizedKeysMu.Lock()
	summarizedKeys = nil
	summarizedKeysMu.Unlock()
	chunkKey := storeutils.MakeChunkKey([]byte("doc1"), "test_index", 0)

	err = pe.EnrichBatch([][]byte{chunkKey})
	require.NoError(t, err)

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)

	// With summarizer, chunk keys should be summarized
	summarizedKeysMu.Lock()
	defer summarizedKeysMu.Unlock()
	assert.Len(t, summarizedKeys, 1, "Chunk key should be summarized when summarizer present")
	assert.True(t, bytes.Contains([]byte(summarizedKeys[0]), storeutils.ChunkingSuffix), "Should have processed chunk key")
}

// TestPipelineEnricher_ChunkKeySummarizationRouting verifies chunk keys route to summarization when both enrichers present

// TestPipelineEnricher_FullPipelineIntegration tests the complete doc→chunk→summary→embed pipeline with real Termite
// This test requires ONNX models in models/chunkers/ directory (enables ONNX-based chunking)
//
// To run this test:
//
//  1. Ensure ONNX models are in models/chunkers/chonky-mmbert-small-multilingual-1/:
//     ls models/chunkers/chonky-mmbert-small-multilingual-1/model.onnx
//
//  2. Run the test (Termite is started automatically as an embedded service):
//     go test -v ./src/store/indexes -run TestPipelineEnricher_FullPipelineIntegration
//
// The test automatically:
// - Starts an embedded Termite service on port 18433
// - Loads ONNX models from models/chunkers/ directory
// - Runs the full doc→chunk→summary→embed pipeline synchronously
// - Shuts down Termite when the test completes
//
// This test verifies:
// - Documents are chunked using ONNX-based semantic chunking with neural networks
// - Each chunk is summarized using the mock LLM (simulates GPT/Claude summarization)
// - Summaries are embedded using the mock embedder (simulates vector embedding generation)
// - The entire pipeline completes synchronously within configured timeouts:
//   - Chunking: 1s base + 200ms/doc (max 5s)
//   - Summarization: 10s base + 2s/doc (max 30s)
//   - Embedding: 2s base + 500ms/doc (max 10s)
//
// - Proper routing through all stages: doc→chunk→summary→embed (not doc→chunk→embed)
// - Chunk keys contain `:c:` suffix, summary keys contain `:s:` suffix
// - Counts match: chunks created = summaries generated = embeddings produced

// TestEmbeddingIndex_GeneratePrompts_ChunkKeys verifies that GeneratePrompts
// correctly reads chunk keys stored in [hashID:uint64][chunkJSON] format.
// This tests the fix for the bug where GeneratePrompts couldn't read chunks
// because it tried to use GetDocument() which doesn't handle chunk format.
func TestEmbeddingIndex_GeneratePrompts_ChunkKeys(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Test data
	indexName := "test_chunk_reading_index"
	docKey := []byte("test_doc")

	// Track what gets embedded via the mock embedder
	var embeddedPrompts []string
	var mu sync.Mutex

	// Register mock embedder that records what it embeds
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					mu.Lock()
					embeddedPrompts = append(embeddedPrompts, values...)
					mu.Unlock()
					result := make([][]float32, len(values))
					for i := range values {
						result[i] = []float32{1.0, 2.0, 3.0}
					}
					return result, nil
				},
			}, nil
		},
	)
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	// Create EmbeddingIndex config with chunking enabled
	// StoreChunks must be true because this test writes chunks directly to storage
	// and expects them to be read back (non-ephemeral mode)
	chunkerConfig, err := chunking.NewChunkerConfig(chunking.TermiteChunkerConfig{
		ApiUrl: "http://mock",
		Model:  "semantic",
	})
	require.NoError(t, err)
	chunkerConfig.StoreChunks = true // Enable persistent chunking mode
	indexConfig := NewEmbeddingsConfig(indexName, EmbeddingsIndexConfig{
		Dimension: 3,
		Template:  "{{content}}",
		Embedder: embeddings.NewEmbedderConfigFromJSON(
			"mock",
			[]byte(`{"provider": "mock", "model": "test-model"}`),
		),
		Chunker: chunkerConfig,
	})

	// Create EmbeddingIndex
	idx, err := NewEmbeddingIndex(
		logger,
		&common.Config{},
		db,
		tempDir,
		indexName,
		indexConfig,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, idx)

	embIndex := idx.(*EmbeddingIndex)
	defer embIndex.Close()

	// Open the index
	tableSchema := &schema.TableSchema{}
	byteRange := types.Range{[]byte{0x00}, []byte{0xFF}}
	err = embIndex.Open(true, tableSchema, byteRange)
	require.NoError(t, err)

	// Start leader factory for async embedding
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	defer leaderCancel()
	go func() {
		_ = embIndex.LeaderFactory(leaderCtx, func(ctx context.Context, writes [][2][]byte) error {
			// Enricher handles persistence internally
			return nil
		})
	}()

	// Wait for enricher to be created (LeaderFactory creates it asynchronously)
	require.Eventually(t, func() bool {
		embIndex.enricherMu.RLock()
		defer embIndex.enricherMu.RUnlock()
		return embIndex.enricher != nil
	}, 5*time.Second, 50*time.Millisecond, "Enricher should be created by LeaderFactory")

	t.Run("ChunkKeys_ProperFormat_ReadSuccessfully", func(t *testing.T) {
		// Reset embedded prompts tracking
		mu.Lock()
		embeddedPrompts = nil
		mu.Unlock()

		// Create chunks in PROPER production format: [hashID:uint64][chunkJSON]
		chunk1 := chunking.NewTextChunk(0, "This is the first chunk of text that should be embedded.", 0, 57)
		chunk2 := chunking.NewTextChunk(1, "This is the second chunk with different content.", 57, 106)

		chunks := []chunking.Chunk{chunk1, chunk2}
		chunkKeys := make([][]byte, len(chunks))
		hashIDs := []uint64{12345, 67890}

		for i, chunk := range chunks {
			// Create chunk key
			chunkKeys[i] = storeutils.MakeChunkKey(docKey, indexName, chunk.Id)

			// Marshal chunk JSON using sonic (like production)
			chunkJSON, err := json.Marshal(chunk)
			require.NoError(t, err)

			// Encode: [hashID:uint64][chunkJSON] (exactly like production in aknn_v0.go:901-923)
			b := make([]byte, 0, len(chunkJSON)+8)
			b = encoding.EncodeUint64Ascending(b, hashIDs[i])
			b = append(b, chunkJSON...)

			// Write chunk to database
			require.NoError(t, db.Set(chunkKeys[i], b, pebble.NoSync))

			logger.Debug("Wrote chunk in proper format",
				zap.ByteString("chunkKey", chunkKeys[i]),
				zap.String("text", chunk.GetText()),
				zap.Uint64("hashID", hashIDs[i]))
		}

		// Call Batch with chunk keys - should recognize them and embed them
		// Batch signature: (ctx, writes [][2][]byte, deletes [][]byte, sync bool)
		writes := make([][2][]byte, len(chunkKeys))
		for i, key := range chunkKeys {
			writes[i] = [2][]byte{key, nil} // Value already in DB, just need key
		}
		err = embIndex.Batch(ctx, writes, nil, false)
		require.NoError(t, err, "Batch should not error when processing chunk keys")

		// Wait for async embedding to complete
		assert.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(embeddedPrompts) == len(chunks)
		}, 5*time.Second, 50*time.Millisecond, "All chunks should be embedded within 5 seconds")

		// Verify chunks were read and processed successfully
		// The bug fix allows GeneratePrompts to read chunks in [hashID:uint64][chunkJSON] format
		mu.Lock()
		actualPrompts := make([]string, len(embeddedPrompts))
		copy(actualPrompts, embeddedPrompts)
		mu.Unlock()

		assert.Len(t, actualPrompts, len(chunks), "All chunk texts should be extracted as prompts")

		// Verify each chunk's text was extracted and sent to embedder
		for i, chunk := range chunks {
			assert.Contains(t, actualPrompts, chunk.GetText(),
				"Chunk %d text should be extracted from [hashID:uint64][chunkJSON] format", i)
		}
	})
}

// TestEmbeddingIndex_Batch_ChunkKeyRouting verifies that Batch method
// correctly recognizes chunk keys and routes them to promptKeys.
// This tests the fix for the bug where chunk keys weren't recognized
// in Batch() and fell through to GetDocument() causing failures.
func TestEmbeddingIndex_Batch_ChunkKeyRouting(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Test data
	indexName := "test_batch_routing_index"

	// Track what gets embedded via the mock embedder
	var embeddedPrompts []string
	var mu sync.Mutex

	// Register mock embedder that records what it embeds
	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return &mockEmbedder{
				embedFunc: func(ctx context.Context, values []string) ([][]float32, error) {
					mu.Lock()
					embeddedPrompts = append(embeddedPrompts, values...)
					mu.Unlock()
					result := make([][]float32, len(values))
					for i := range values {
						result[i] = []float32{1.0, 2.0, 3.0}
					}
					return result, nil
				},
			}, nil
		},
	)
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	// Create EmbeddingIndex config with chunking enabled
	// StoreChunks must be true because this test writes chunks directly to storage
	// and expects them to be read back (non-ephemeral mode)
	chunkerConfig, err := chunking.NewChunkerConfig(chunking.TermiteChunkerConfig{
		ApiUrl: "http://mock",
		Model:  "semantic",
	})
	require.NoError(t, err)
	chunkerConfig.StoreChunks = true // Enable persistent chunking mode
	indexConfig := NewEmbeddingsConfig(indexName, EmbeddingsIndexConfig{
		Dimension: 3,
		Template:  "{{content}}",
		Embedder: embeddings.NewEmbedderConfigFromJSON(
			"mock",
			[]byte(`{"provider": "mock", "model": "test-model"}`),
		),
		Chunker: chunkerConfig,
	})

	// Create EmbeddingIndex
	idx, err := NewEmbeddingIndex(
		logger,
		&common.Config{},
		db,
		tempDir,
		indexName,
		indexConfig,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, idx)

	embIndex := idx.(*EmbeddingIndex)
	defer embIndex.Close()

	// Open the index
	tableSchema := &schema.TableSchema{}
	byteRange := types.Range{[]byte{0x00}, []byte{0xFF}}
	err = embIndex.Open(true, tableSchema, byteRange)
	require.NoError(t, err)

	// Start leader factory for async embedding
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	defer leaderCancel()
	go func() {
		_ = embIndex.LeaderFactory(leaderCtx, func(ctx context.Context, writes [][2][]byte) error {
			// Enricher handles persistence internally
			return nil
		})
	}()

	// Wait for enricher to be created (LeaderFactory creates it asynchronously)
	require.Eventually(t, func() bool {
		embIndex.enricherMu.RLock()
		defer embIndex.enricherMu.RUnlock()
		return embIndex.enricher != nil
	}, 5*time.Second, 50*time.Millisecond, "Enricher should be created by LeaderFactory")

	t.Run("ChunkKeys_RecognizedAndRouted", func(t *testing.T) {
		// Reset embedded prompts tracking
		mu.Lock()
		embeddedPrompts = nil
		mu.Unlock()

		// Create chunks with distinct text
		chunks := []chunking.Chunk{
			chunking.NewTextChunk(0, "First chunk text for routing test", 0, 34),
			chunking.NewTextChunk(1, "Second chunk text for routing test", 34, 69),
			chunking.NewTextChunk(2, "Third chunk text for routing test", 69, 103),
		}

		docKey := []byte("routing_test_doc")
		chunkKeys := make([][]byte, len(chunks))
		hashIDs := []uint64{1001, 1002, 1003}

		for i, chunk := range chunks {
			chunkKeys[i] = storeutils.MakeChunkKey(docKey, indexName, chunk.Id)

			// Write chunk in proper format
			chunkJSON, err := json.Marshal(chunk)
			require.NoError(t, err)

			b := make([]byte, 0, len(chunkJSON)+8)
			b = encoding.EncodeUint64Ascending(b, hashIDs[i])
			b = append(b, chunkJSON...)

			require.NoError(t, db.Set(chunkKeys[i], b, pebble.NoSync))
		}

		// Call Batch with chunk keys
		writes := make([][2][]byte, len(chunkKeys))
		for i, key := range chunkKeys {
			writes[i] = [2][]byte{key, nil}
		}
		err = embIndex.Batch(ctx, writes, nil, false)
		require.NoError(t, err, "Batch should not error when processing chunk keys")

		// Wait for async processing
		time.Sleep(500 * time.Millisecond)

		// Verify chunks were recognized and routed correctly (not to GetDocument)
		mu.Lock()
		actualPrompts := make([]string, len(embeddedPrompts))
		copy(actualPrompts, embeddedPrompts)
		mu.Unlock()

		// Verify the chunk text was extracted as prompts
		assert.Len(t, actualPrompts, len(chunks), "All chunk texts should be extracted as prompts")

		// Verify each chunk's text was used
		for i, chunk := range chunks {
			assert.Contains(t, actualPrompts, chunk.GetText(),
				"Chunk %d text should be in embedded prompts", i)
		}
	})

	t.Run("MixedKeys_ChunksAndDocuments", func(t *testing.T) {
		// Reset embedded prompts tracking
		mu.Lock()
		embeddedPrompts = nil
		mu.Unlock()

		// Create chunk key
		chunk := chunking.NewTextChunk(0, "Chunk in mixed batch", 0, 20)
		docKey := []byte("mixed_doc")
		chunkKey := storeutils.MakeChunkKey(docKey, indexName, chunk.Id)

		// Write chunk
		chunkJSON, err := json.Marshal(chunk)
		require.NoError(t, err)

		chunkValue := make([]byte, 0, len(chunkJSON)+8)
		chunkValue = encoding.EncodeUint64Ascending(chunkValue, uint64(5555))
		chunkValue = append(chunkValue, chunkJSON...)
		require.NoError(t, db.Set(chunkKey, chunkValue, pebble.NoSync))

		// Create regular document key
		regularDocKey := []byte("regular_doc")
		regularDocValue := map[string]any{"content": "Regular document content"}
		regularDocBytes, err := json.Marshal(regularDocValue)
		require.NoError(t, err)

		// Compress document value with zstd (required by scanner)
		var compressedBuf bytes.Buffer
		zstdWriter, err := zstd.NewWriter(&compressedBuf)
		require.NoError(t, err)
		_, err = zstdWriter.Write(regularDocBytes)
		require.NoError(t, err)
		require.NoError(t, zstdWriter.Close())

		actualDocKey := storeutils.KeyRangeStart(regularDocKey)
		require.NoError(t, db.Set(actualDocKey, compressedBuf.Bytes(), pebble.NoSync))

		// Call Batch with mixed keys
		writes := [][2][]byte{{chunkKey, nil}, {actualDocKey, nil}}
		err = embIndex.Batch(ctx, writes, nil, false)
		require.NoError(t, err, "Batch should handle mixed chunk and document keys")

		// Wait for async processing
		time.Sleep(500 * time.Millisecond)

		// Verify chunk text was extracted
		mu.Lock()
		actualPrompts := make([]string, len(embeddedPrompts))
		copy(actualPrompts, embeddedPrompts)
		mu.Unlock()

		// Both chunk and document should be embedded
		assert.GreaterOrEqual(t, len(actualPrompts), 1, "At least chunk should be embedded")

		// Verify chunk text was extracted from chunk key (not from GetDocument)
		assert.Contains(t, actualPrompts, chunk.GetText(), "Chunk text should be in embedded prompts")
	})

	t.Run("ChunkKeys_NotFound_LoggedButNoError", func(t *testing.T) {
		// Reset embedded prompts tracking
		mu.Lock()
		embeddedPrompts = nil
		mu.Unlock()

		// Create chunk key that doesn't exist in database
		nonExistentChunkKey := storeutils.MakeChunkKey([]byte("nonexistent_doc"), indexName, 99)

		// Call Batch - should log warning but not error
		writes := [][2][]byte{{nonExistentChunkKey, nil}}
		err = embIndex.Batch(ctx, writes, nil, false)
		assert.NoError(t, err, "Batch should not error on missing chunk key")

		// Wait briefly
		time.Sleep(200 * time.Millisecond)

		// Verify nothing was embedded (key not found, so no prompts generated)
		mu.Lock()
		actualPrompts := make([]string, len(embeddedPrompts))
		copy(actualPrompts, embeddedPrompts)
		mu.Unlock()

		assert.Empty(t, actualPrompts, "Non-existent chunk should not generate prompts")
	})

	// Give background LeaderFactory goroutine time to shut down gracefully
	// before test cleanup (embIndex.Close() and leaderCancel() defers)
	time.Sleep(500 * time.Millisecond)
}

// TestPipelineEnricher_GenerateEmbeddingsWithoutPersist_Chunking tests the critical fix:
// chunks generated by ChunkingEnricher must be added to documentValues map so that
// EmbeddingEnricher can read them without accessing Pebble during pre-enrichment.
func TestPipelineEnricher_GenerateEmbeddingsWithoutPersist_Chunking(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Track what the embedder receives
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

	// Create PipelineEnricher with chunking enabled
	indexName := "test_chunking_pipeline"
	embSuffix := []byte(":i:" + indexName + ":e")
	sumSuffix := []byte(":i:" + indexName + ":s")
	byteRange := types.Range{nil, nil}

	config := map[string]any{
		"provider": "mock",
		"model":    "test-model",
	}
	var embedderConfig embeddings.EmbedderConfig
	configBytes, _ := json.Marshal(config)
	_ = json.Unmarshal(configBytes, &embedderConfig)

	antflyChunkingConfig := chunking.AntflyChunkerConfig{
		Text: chunking.TextChunkOptions{
			TargetTokens:  50, // Small chunks for testing
			OverlapTokens: 10,
		},
	}
	chunkingConfig, err := chunking.NewChunkerConfig(antflyChunkingConfig)
	require.NoError(t, err)

	antflyConfig := &common.Config{}

	// Create a simple template function
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

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		t.Fatal("persistEmbeddings should not be called in GenerateEmbeddingsWithoutPersist")
		return nil
	}

	persistSummaries := func(ctx context.Context, keys [][]byte, hashIDs []uint64, summaries []string) error {
		t.Fatal("persistSummaries should not be called in GenerateEmbeddingsWithoutPersist")
		return nil
	}

	persistChunks := func(ctx context.Context, keys [][]byte, hashIDs []uint64, chunks [][]chunking.Chunk) error {
		t.Fatal("persistChunks should not be called in GenerateEmbeddingsWithoutPersist")
		return nil
	}

	pe, err := NewPipelineEnricher(context.Background(), PipelineEnricherConfig{
		Logger:            logger,
		AntflyConfig:      antflyConfig,
		DB:                db,
		Dir:               tempDir,
		Name:              indexName,
		EmbSuffix:         embSuffix,
		SumSuffix:         sumSuffix,
		ByteRange:         byteRange,
		EmbedderConfig:    embedderConfig,
		ChunkingConfig:    chunkingConfig,
		StoreChunks:       true,
		GeneratePrompts:   generatePrompts,
		PersistEmbeddings: persistEmbeddings,
		PersistSummaries:  persistSummaries,
		PersistChunks:     persistChunks,
	})
	require.NoError(t, err)
	defer pe.Close()

	// Create test document - make it long enough to be chunked into multiple pieces
	doc := map[string]any{
		"content": "This is the first paragraph of a test document. " +
			"It contains multiple sentences to ensure proper chunking. " +
			"The semantic chunker should identify this as a coherent unit of meaning. " +
			"This is the second paragraph which discusses different content. " +
			"It should ideally be placed in a separate chunk from the first paragraph. " +
			"The chunker uses semantic analysis to determine optimal chunk boundaries. " +
			"This final paragraph contains additional context and information. " +
			"It helps to test the chunking logic with enough text to create multiple chunks.",
	}
	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)

	// Compress document
	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := writer.EncodeAll(docBytes, nil)

	// Prepare documentValues map with compressed document
	documentValues := map[string][]byte{
		"doc1": compressed,
	}

	// Call GenerateEmbeddingsWithoutPersist
	keys := [][]byte{[]byte("doc1")}
	embWrites, chunkWrites, failedKeys, err := pe.GenerateEmbeddingsWithoutPersist(
		context.Background(),
		keys,
		documentValues,
		generatePrompts,
	)

	require.NoError(t, err)
	assert.Empty(t, failedKeys, "should have no failed keys")

	// CRITICAL VERIFICATION 1: Chunks should be returned in chunkWrites
	assert.NotEmpty(t, chunkWrites, "should return chunk writes")
	assert.GreaterOrEqual(t, len(chunkWrites), 1, "should create at least 1 chunk")

	// CRITICAL VERIFICATION 2: Verify chunk writes format
	for i, chunkWrite := range chunkWrites {
		chunkKey := chunkWrite[0]
		chunkVal := chunkWrite[1]

		// Verify chunk key format: doc1:i:test_chunking_pipeline:<chunkID>:c
		assert.True(t, bytes.HasPrefix(chunkKey, []byte("doc1:i:"+indexName+":")),
			"chunk key should have correct format")
		assert.True(t, bytes.HasSuffix(chunkKey, []byte(":c")),
			"chunk key should end with :c suffix")

		// Verify chunk value format: [hashID:8bytes][chunkJSON]
		_, hashID, err := encoding.DecodeUint64Ascending(chunkVal)
		require.NoError(t, err)
		assert.Positive(t, hashID, "chunk hashID should be non-zero")

		// Decode chunk JSON
		var chunk chunking.Chunk
		err = json.Unmarshal(chunkVal[8:], &chunk)
		require.NoError(t, err)
		assert.NotEmpty(t, chunk.GetText(), "chunk should have text")
		t.Logf("Chunk %d: %s", i, chunk.GetText())
	}

	// CRITICAL VERIFICATION 3: Verify chunks were added to documentValues map
	// The embedding enricher should have been able to read chunks from documentValues
	// (not from Pebble, which would fail since we haven't persisted anything)
	for _, chunkWrite := range chunkWrites {
		chunkVal := chunkWrite[1]

		storedVal, ok := documentValues[string(chunkWrite[0])]
		assert.True(t, ok, "chunk should be in documentValues map")
		assert.Equal(t, chunkVal, storedVal, "chunk value should match")
	}

	// CRITICAL VERIFICATION 4: Embeddings should be generated for chunks
	assert.NotEmpty(t, embWrites, "should return embedding writes")
	// We expect embeddings for the chunks (at least 1)
	assert.GreaterOrEqual(t, len(embWrites), 1, "should create embeddings for chunks")

	// CRITICAL VERIFICATION 5: Verify embedding enricher received chunk text
	embeddedTextsMu.Lock()
	receivedTexts := make([]string, len(embeddedTexts))
	copy(receivedTexts, embeddedTexts)
	embeddedTextsMu.Unlock()

	assert.NotEmpty(t, receivedTexts, "embedder should have been called")
	assert.GreaterOrEqual(t, len(receivedTexts), 1, "embedder should receive chunk texts")

	// Verify the embedder received chunk text, not full document text
	fullDocText := doc["content"].(string)
	for _, text := range receivedTexts {
		// The chunker may add formatting (like newlines), so just verify we got chunk text
		// Check that the chunk is not empty and has reasonable content
		assert.NotEmpty(t, text, "chunk text should not be empty")
		// Verify chunk text is shorter than the full document (it was actually chunked)
		assert.Less(t, len(text), len(fullDocText), "chunk should be smaller than full document")
	}

	// CRITICAL VERIFICATION 6: Verify embedding writes format
	for i, embWrite := range embWrites {
		embKey := embWrite[0]
		embVal := embWrite[1]

		// Embedding keys should reference chunks, not the original document
		// Format: doc1:i:test_chunking_pipeline:c:<chunkID>:i:test_chunking_pipeline:e
		// OR: doc1:i:test_chunking_pipeline:c:<chunkID>:embedding (depending on suffix)
		assert.True(t, bytes.Contains(embKey, []byte(":c:")),
			"embedding key should reference chunk key")

		// Verify embedding value format: [hashID:8bytes][vector]
		_, hashID, err := encoding.DecodeUint64Ascending(embVal)
		require.NoError(t, err)
		assert.Positive(t, hashID, "embedding hashID should be non-zero")

		_, vec, err := vector.Decode(embVal[8:])
		require.NoError(t, err)
		assert.Len(t, vec, 3, "embedding should have dimension 3")
		t.Logf("Embedding %d: key=%s, vector=%v", i, embKey, vec)
	}

	t.Log("✅ All critical verifications passed:")
	t.Log("  1. Chunks returned in chunkWrites")
	t.Log("  2. Chunk writes have correct format")
	t.Log("  3. Chunks added to documentValues map")
	t.Log("  4. Embeddings generated for chunks")
	t.Log("  5. Embedder received chunk text (not full document)")
	t.Log("  6. Embedding writes reference chunks")
}

// TestGenerateEphemeralChunkPrompts_BackfillWithDocument verifies that
// GenerateEphemeralChunkPrompts falls back to extractPrompt when
// state.Enrichment is nil but state.Document is populated (the backfill case).
func TestGenerateEphemeralChunkPrompts_BackfillWithDocument(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Register mock embedder
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
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	embSuffix := []byte(":e")
	sumSuffix := []byte(":s")
	byteRange := types.Range{nil, nil}
	indexName := "test_backfill_ephemeral"

	embedderConfig := embeddings.EmbedderConfig{
		Provider: embeddings.EmbedderProviderMock,
	}

	generatePrompts := func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		return nil, nil, nil, nil
	}

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		return nil
	}

	// extractPrompt returns content field from the document map
	extractPromptCalled := false
	extractPrompt := func(doc map[string]any) (string, uint64, error) {
		extractPromptCalled = true
		content, ok := doc["content"].(string)
		if !ok {
			return "", 0, fmt.Errorf("missing content field")
		}
		return content, xxhash.Sum64String(content), nil
	}

	antflyChunkingConfig := chunking.AntflyChunkerConfig{
		Text: chunking.TextChunkOptions{
			TargetTokens:  50,
			OverlapTokens: 10,
		},
	}
	chunkingConfig, err := chunking.NewChunkerConfig(antflyChunkingConfig)
	require.NoError(t, err)

	ctx := t.Context()

	// Create pipeline enricher with ephemeral chunking and extractPrompt
	enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
		Logger:            logger,
		AntflyConfig:      &common.Config{},
		DB:                db,
		Dir:               tempDir,
		Name:              indexName,
		EmbSuffix:         embSuffix,
		SumSuffix:         sumSuffix,
		ByteRange:         byteRange,
		EmbedderConfig:    embedderConfig,
		ChunkingConfig:    chunkingConfig,
		GeneratePrompts:   generatePrompts,
		PersistEmbeddings: persistEmbeddings,
		ExtractPrompt:     extractPrompt,
	})
	require.NoError(t, err)
	require.NotNil(t, enricher)
	defer enricher.Close()

	pe := enricher.(*PipelineEnricher)
	require.NotNil(t, pe.chunkingHelper, "chunkingHelper should be set in ephemeral mode")

	// Simulate backfill: states have Document but nil Enrichment
	docText := "This is paragraph one about distributed systems. " +
		"It covers many important topics including consensus algorithms. " +
		"This is paragraph two about vector search and embeddings. " +
		"It discusses approximate nearest neighbor algorithms in detail."

	states := []storeutils.DocumentScanState{
		{
			CurrentDocKey: []byte("doc1"),
			Document:      map[string]any{"content": docText},
			Enrichment:    nil, // nil - simulating backfill
		},
	}

	keys, prompts, hashIDs, err := pe.GenerateEphemeralChunkPrompts(ctx, states)
	require.NoError(t, err)

	// extractPrompt should have been called since Enrichment was nil
	assert.True(t, extractPromptCalled, "extractPrompt should be called when Enrichment is nil")

	// Should produce at least 1 chunk prompt
	assert.NotEmpty(t, keys, "should produce chunk keys")
	assert.NotEmpty(t, prompts, "should produce chunk prompts")
	assert.Len(t, prompts, len(keys), "keys and prompts should have same length")
	assert.Len(t, hashIDs, len(keys), "keys and hashIDs should have same length")

	// Verify chunk keys have the correct format
	for _, key := range keys {
		assert.True(t, storeutils.IsChunkKey(key),
			"generated key should be a chunk key: %s", string(key))
	}

	// Verify prompts are non-empty substrings (chunks, not the full document)
	for _, prompt := range prompts {
		assert.NotEmpty(t, prompt, "chunk prompt should not be empty")
	}

	t.Logf("Backfill produced %d chunks from document", len(prompts))
}

// TestGenerateEphemeralChunkPrompts_EnrichmentStillWorks verifies that
// the existing Enrichment path still works correctly when extractPrompt is set,
// and that extractPrompt is NOT called when Enrichment is already available.
func TestGenerateEphemeralChunkPrompts_EnrichmentStillWorks(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Register mock embedder
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
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	embSuffix := []byte(":e")
	sumSuffix := []byte(":s")
	byteRange := types.Range{nil, nil}
	indexName := "test_enrichment_path"

	embedderConfig := embeddings.EmbedderConfig{
		Provider: embeddings.EmbedderProviderMock,
	}

	generatePrompts := func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		return nil, nil, nil, nil
	}

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		return nil
	}

	// extractPrompt should NOT be called when Enrichment is set
	extractPromptCalled := false
	extractPrompt := func(doc map[string]any) (string, uint64, error) {
		extractPromptCalled = true
		return "", 0, fmt.Errorf("should not be called")
	}

	antflyChunkingConfig := chunking.AntflyChunkerConfig{
		Text: chunking.TextChunkOptions{
			TargetTokens:  50,
			OverlapTokens: 10,
		},
	}
	chunkingConfig, err := chunking.NewChunkerConfig(antflyChunkingConfig)
	require.NoError(t, err)

	ctx := t.Context()

	// Create pipeline enricher with ephemeral chunking and extractPrompt
	enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
		Logger:            logger,
		AntflyConfig:      &common.Config{},
		DB:                db,
		Dir:               tempDir,
		Name:              indexName,
		EmbSuffix:         embSuffix,
		SumSuffix:         sumSuffix,
		ByteRange:         byteRange,
		EmbedderConfig:    embedderConfig,
		ChunkingConfig:    chunkingConfig,
		GeneratePrompts:   generatePrompts,
		PersistEmbeddings: persistEmbeddings,
		ExtractPrompt:     extractPrompt, // Should not be called
	})
	require.NoError(t, err)
	require.NotNil(t, enricher)
	defer enricher.Close()

	pe := enricher.(*PipelineEnricher)

	// Simulate normal path: states have Enrichment set (non-backfill)
	docText := "This is paragraph one about distributed systems. " +
		"It covers many important topics including consensus algorithms. " +
		"This is paragraph two about vector search and embeddings. " +
		"It discusses approximate nearest neighbor algorithms in detail."

	states := []storeutils.DocumentScanState{
		{
			CurrentDocKey: []byte("doc1"),
			Enrichment:    docText, // Enrichment IS set
			Document:      map[string]any{"content": docText},
		},
	}

	keys, prompts, hashIDs, err := pe.GenerateEphemeralChunkPrompts(ctx, states)
	require.NoError(t, err)

	// extractPrompt should NOT have been called
	assert.False(t, extractPromptCalled, "extractPrompt should NOT be called when Enrichment is set")

	// Should produce chunks from the Enrichment text
	assert.NotEmpty(t, keys, "should produce chunk keys")
	assert.NotEmpty(t, prompts, "should produce chunk prompts")
	assert.Len(t, prompts, len(keys), "keys and prompts should have same length")
	assert.Len(t, hashIDs, len(keys), "keys and hashIDs should have same length")

	t.Logf("Enrichment path produced %d chunks", len(prompts))
}

// TestGenerateEphemeralChunkPrompts_ChunkOffsetWrites verifies that chunk offset metadata
// (start_char, end_char) is accumulated as pending writes when persistFunc is provided,
// and that the text field is stripped from the stored chunks.
func TestGenerateEphemeralChunkPrompts_ChunkOffsetWrites(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Register mock embedder
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
	defer embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)

	indexName := "test_offset_writes"
	embSuffix := []byte(":e")
	sumSuffix := []byte(":s")
	byteRange := types.Range{}

	embedderConfig := embeddings.EmbedderConfig{
		Provider: embeddings.EmbedderProviderMock,
	}

	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		return nil, nil, nil, nil
	}

	persistEmbeddings := func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		return nil
	}

	// Track what persistFunc receives
	var persistedWritesMu sync.Mutex
	var persistedWrites [][2][]byte
	raftPersistFunc := func(ctx context.Context, writes [][2][]byte) error {
		persistedWritesMu.Lock()
		persistedWrites = append(persistedWrites, writes...)
		persistedWritesMu.Unlock()
		return nil
	}

	antflyChunkingConfig := chunking.AntflyChunkerConfig{
		Text: chunking.TextChunkOptions{
			TargetTokens:  50,
			OverlapTokens: 10,
		},
	}
	chunkingConfig, err := chunking.NewChunkerConfig(antflyChunkingConfig)
	require.NoError(t, err)

	ctx := t.Context()

	enricher, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
		Logger:            logger,
		AntflyConfig:      &common.Config{},
		DB:                db,
		Dir:               tempDir,
		Name:              indexName,
		EmbSuffix:         embSuffix,
		SumSuffix:         sumSuffix,
		ByteRange:         byteRange,
		EmbedderConfig:    embedderConfig,
		ChunkingConfig:    chunkingConfig,
		GeneratePrompts:   generatePrompts,
		PersistEmbeddings: persistEmbeddings,
		PersistFunc:       raftPersistFunc,
	})
	require.NoError(t, err)
	require.NotNil(t, enricher)
	defer enricher.Close()

	pe := enricher.(*PipelineEnricher)
	require.NotNil(t, pe.chunkingHelper, "chunkingHelper should be set in ephemeral mode")
	require.NotNil(t, pe.persistFunc, "persistFunc should be stored")

	// Generate prompts with a document that will produce chunks
	text := "This is paragraph one about distributed systems. It covers many important topics including consensus algorithms and replication. " +
		"This is paragraph two about database indexing and search. It discusses B-trees, LSM trees, and inverted indexes for full-text search."

	states := []storeutils.DocumentScanState{
		{
			CurrentDocKey: []byte("doc1"),
			Enrichment:    text,
		},
	}

	keys, prompts, hashIDs, err := pe.GenerateEphemeralChunkPrompts(ctx, states)
	require.NoError(t, err)
	require.NotEmpty(t, keys, "should produce chunk prompts")
	require.Len(t, prompts, len(keys))
	require.Len(t, hashIDs, len(keys))

	// Check that pending writes were accumulated
	pe.pendingChunkWritesMu.Lock()
	pendingCount := len(pe.pendingChunkWrites)
	pe.pendingChunkWritesMu.Unlock()
	require.Equal(t, len(keys), pendingCount, "should have one pending chunk write per prompt")

	// Flush and verify
	err = pe.flushPendingChunkWrites(ctx)
	require.NoError(t, err)

	// Pending should be drained
	pe.pendingChunkWritesMu.Lock()
	require.Empty(t, pe.pendingChunkWrites, "pending writes should be drained after flush")
	pe.pendingChunkWritesMu.Unlock()

	// Verify persisted writes
	persistedWritesMu.Lock()
	defer persistedWritesMu.Unlock()
	require.Len(t, persistedWrites, len(keys), "should persist one write per chunk")

	for i, write := range persistedWrites {
		chunkKey := write[0]
		chunkValue := write[1]

		// Key should be a chunk key
		assert.True(t, storeutils.IsChunkKey(chunkKey),
			"write %d: key should be a chunk key, got %s", i, types.FormatKey(chunkKey))

		// Value should have [hashID:8bytes][chunkJSON]
		require.Greater(t, len(chunkValue), 8, "write %d: value too short", i)

		// Decode the chunk JSON (skip 8-byte hashID prefix)
		chunkJSON := chunkValue[8:]
		var chunk chunking.Chunk
		err := json.Unmarshal(chunkJSON, &chunk)
		require.NoError(t, err, "write %d: should unmarshal chunk JSON", i)

		// Text should be empty (stripped)
		assert.Empty(t, chunk.GetText(), "write %d: chunk text should be stripped", i)

		// Offsets should be present
		tc, tcErr := chunk.AsTextContent()
		require.NoError(t, tcErr, "write %d: should be text content", i)
		assert.GreaterOrEqual(t, tc.EndChar, tc.StartChar,
			"write %d: end_char should be >= start_char", i)

		t.Logf("Chunk %d: key=%s start=%d end=%d text=%q",
			i, types.FormatKey(chunkKey), tc.StartChar, tc.EndChar, chunk.GetText())
	}
}

// TestFlushPendingChunkWrites_NoPersistFunc verifies that flushing is a no-op
// when persistFunc is nil (no Raft persist available).
func TestFlushPendingChunkWrites_NoPersistFunc(t *testing.T) {
	pe := &PipelineEnricher{
		pendingChunkWrites: [][2][]byte{
			{[]byte("key1"), []byte("val1")},
		},
	}

	err := pe.flushPendingChunkWrites(context.Background())
	require.NoError(t, err)

	// Writes should be drained (set to nil) even though persistFunc is nil — they're silently dropped
	pe.pendingChunkWritesMu.Lock()
	defer pe.pendingChunkWritesMu.Unlock()
	assert.Nil(t, pe.pendingChunkWrites, "should drain even without persistFunc")
}

// TestFlushPendingChunkWrites_Empty verifies that flushing empty writes is a no-op.
// TestPipelineEnricher_GenerateEmbeddingsWithoutPersist_EphemeralChunking verifies that
// ephemeral chunk keys (which store raw prompt text, not chunk JSON) are handled correctly
// during pre-enrichment. Previously, GeneratePrompts would try to JSON-parse the raw prompt
// text as a chunking.Chunk and fail with "invalid character" errors.
func TestPipelineEnricher_GenerateEmbeddingsWithoutPersist_EphemeralChunking(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()
	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

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

	indexName := "test_ephemeral"
	embSuffix := []byte(":i:" + indexName + ":e")
	sumSuffix := []byte(":i:" + indexName + ":s")

	config := map[string]any{"provider": "mock", "model": "test-model"}
	var embedderConfig embeddings.EmbedderConfig
	configBytes, _ := json.Marshal(config)
	_ = json.Unmarshal(configBytes, &embedderConfig)

	antflyChunkingConfig := chunking.AntflyChunkerConfig{
		Text: chunking.TextChunkOptions{
			TargetTokens:  50,
			OverlapTokens: 10,
		},
	}
	chunkingConfig, err := chunking.NewChunkerConfig(antflyChunkingConfig)
	require.NoError(t, err)

	antflyConfig := &common.Config{}

	generatePrompts := func(_ context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, 0, len(states))
		prompts := make([]string, 0, len(states))
		hashIDs := make([]uint64, 0, len(states))
		for _, s := range states {
			if content, ok := s.Document["content"].(string); ok {
				keys = append(keys, s.CurrentDocKey)
				prompts = append(prompts, content)
				hashIDs = append(hashIDs, xxhash.Sum64String(content))
			}
		}
		return keys, prompts, hashIDs, nil
	}

	extractPrompt := func(doc map[string]any) (string, uint64, error) {
		content, _ := doc["content"].(string)
		return content, xxhash.Sum64String(content), nil
	}

	// StoreChunks: false triggers ephemeral chunking mode
	pe, err := NewPipelineEnricher(context.Background(), PipelineEnricherConfig{
		Logger:          logger,
		AntflyConfig:    antflyConfig,
		DB:              db,
		Dir:             tempDir,
		Name:            indexName,
		EmbSuffix:       embSuffix,
		SumSuffix:       sumSuffix,
		ByteRange:       types.Range{nil, nil},
		EmbedderConfig:  embedderConfig,
		ChunkingConfig:  chunkingConfig,
		StoreChunks:     false, // ephemeral mode
		GeneratePrompts: generatePrompts,
		ExtractPrompt:   extractPrompt,
		PersistEmbeddings: func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
			t.Fatal("persistEmbeddings should not be called")
			return nil
		},
	})
	require.NoError(t, err)
	defer pe.Close()

	// Verify we're in ephemeral mode
	pePipeline, ok := pe.(*PipelineEnricher)
	require.True(t, ok, "should be a *PipelineEnricher")
	assert.Nil(t, pePipeline.ChunkingEnricher, "should NOT have ChunkingEnricher in ephemeral mode")
	assert.NotNil(t, pePipeline.chunkingHelper, "should have chunkingHelper in ephemeral mode")

	doc := map[string]any{
		"content": "This is the first paragraph of a test document. " +
			"It contains multiple sentences to ensure proper chunking. " +
			"The semantic chunker should identify this as a coherent unit. " +
			"This is the second paragraph which discusses different content. " +
			"It should be placed in a separate chunk from the first paragraph. " +
			"The chunker uses semantic analysis to determine chunk boundaries. " +
			"This final paragraph contains additional context and information. " +
			"It helps to test the chunking logic with enough text for multiple chunks.",
	}
	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)

	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := writer.EncodeAll(docBytes, nil)

	documentValues := map[string][]byte{"doc1": compressed}
	keys := [][]byte{[]byte("doc1")}

	// This previously failed with "decoding chunk JSON from state: invalid character 'M'"
	// because ephemeral chunk keys store raw prompt text, not chunk JSON.
	embWrites, _, failedKeys, err := pe.GenerateEmbeddingsWithoutPersist(
		context.Background(), keys, documentValues, generatePrompts,
	)

	require.NoError(t, err, "should not fail parsing ephemeral chunk prompts as JSON")
	assert.Empty(t, failedKeys, "should have no failed keys")
	assert.NotEmpty(t, embWrites, "should generate embeddings for ephemeral chunks")

	embeddedTextsMu.Lock()
	receivedTexts := make([]string, len(embeddedTexts))
	copy(receivedTexts, embeddedTexts)
	embeddedTextsMu.Unlock()

	assert.NotEmpty(t, receivedTexts, "embedder should have been called with chunk text")
	for _, text := range receivedTexts {
		assert.NotEmpty(t, text, "each chunk prompt should be non-empty")
	}
}

func TestFlushPendingChunkWrites_Empty(t *testing.T) {
	called := false
	pe := &PipelineEnricher{
		persistFunc: func(ctx context.Context, writes [][2][]byte) error {
			called = true
			return nil
		},
	}

	err := pe.flushPendingChunkWrites(context.Background())
	require.NoError(t, err)
	assert.False(t, called, "should not call persistFunc when no pending writes")
}
