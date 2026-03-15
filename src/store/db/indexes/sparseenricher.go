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
	"errors"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/template"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/src/store/storeutils"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/sethvargo/go-retry"
	"github.com/theory/jsonpath"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// PersistSparseFunc persists sparse embedding writes through Raft.
type PersistSparseFunc func(ctx context.Context, writes [][2][]byte) error

// extractSparsePrompt extracts text from a document using the given field or template
// and returns the prompt string with its xxhash for deduplication.
func extractSparsePrompt(promptTemplate, indexingField string, doc map[string]any) (string, uint64, error) {
	var prompt string

	if promptTemplate != "" {
		rendered, err := template.Render(promptTemplate, doc)
		if err != nil {
			return "", 0, fmt.Errorf("rendering template: %w", err)
		}
		prompt = rendered
	} else if indexingField != "" {
		pathStr := "$." + indexingField
		path, err := jsonpath.Parse(pathStr)
		if err != nil {
			return "", 0, fmt.Errorf("parsing field path %q: %w", pathStr, err)
		}
		results := path.Select(doc)
		if len(results) == 0 {
			return "", 0, nil
		}
		switch v := results[0].(type) {
		case string:
			prompt = v
		default:
			data, err := json.Marshal(v)
			if err != nil {
				return "", 0, fmt.Errorf("marshaling field value: %w", err)
			}
			prompt = string(data)
		}
	}

	hashID := xxhash.Sum64String(prompt)
	return prompt, hashID, nil
}

// SparseEmbeddingEnricher generates sparse embeddings for documents and persists
// them through Raft. It implements the SparseEnricher interface.
type SparseEmbeddingEnricher struct {
	logger         *zap.Logger
	name           string
	db             *pebble.DB
	embedder       embeddings.SparseEmbedder
	persistFunc    PersistSparseFunc
	sparseSuffix   []byte
	sumSuffix      []byte // Optional: set when used inside a pipeline with summarization
	indexingField  string
	promptTemplate string
	byteRange      types.Range
	rateLimiter    *rate.Limiter // Per-provider rate limiter (nil for local providers)
}

// NewSparseEmbeddingEnricher creates a new sparse embedding enricher.
func NewSparseEmbeddingEnricher(
	logger *zap.Logger,
	name string,
	db *pebble.DB,
	embedder embeddings.SparseEmbedder,
	persistFunc PersistSparseFunc,
	sparseSuffix []byte,
	indexingField string,
	promptTemplate string,
	byteRange types.Range,
) *SparseEmbeddingEnricher {
	// If the sparse embedder also implements the full Embedder interface
	// (e.g. TermiteClient), use its rate limiter. Otherwise no rate limit.
	var limiter *rate.Limiter
	if e, ok := embedder.(embeddings.Embedder); ok {
		limiter = resolveRateLimiter(e)
	}
	return &SparseEmbeddingEnricher{
		logger:         logger,
		name:           name,
		db:             db,
		embedder:       embedder,
		persistFunc:    persistFunc,
		sparseSuffix:   sparseSuffix,
		indexingField:  indexingField,
		promptTemplate: promptTemplate,
		byteRange:      byteRange,
		rateLimiter:    limiter,
	}
}

var _ SparseEnricher = (*SparseEmbeddingEnricher)(nil)

func (e *SparseEmbeddingEnricher) EnricherStats() EnricherStats {
	return EnricherStats{} // No WAL, no backfill tracking
}

func (e *SparseEmbeddingEnricher) Close() error {
	return nil
}

// EnrichBatch generates sparse embeddings for the given keys and persists them.
// Keys may be document keys, chunk keys, or summary keys (when used inside a pipeline).
func (e *SparseEmbeddingEnricher) EnrichBatch(ctx context.Context, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	var (
		filteredKeys [][]byte
		prompts      []string
		hashIDs      []uint64
	)

	for _, key := range keys {
		if !e.byteRange.Contains(key) {
			continue
		}

		var prompt string
		var hashID uint64

		if storeutils.IsChunkKey(key) {
			// Chunk key: read chunk data from Pebble at the key directly
			val, closer, err := e.db.Get(key)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					e.logger.Warn("Failed to read chunk for sparse enrichment",
						zap.ByteString("key", key), zap.Error(err))
				}
				continue
			}
			if len(val) < 8 {
				_ = closer.Close()
				continue
			}
			var chunk chunking.Chunk
			if err := json.Unmarshal(val[8:], &chunk); err != nil {
				_ = closer.Close()
				e.logger.Warn("Failed to unmarshal chunk for sparse enrichment",
					zap.ByteString("key", key), zap.Error(err))
				continue
			}
			_ = closer.Close()
			prompt = strings.TrimSpace(chunk.GetText())
			if prompt == "" {
				continue
			}
			hashID = xxhash.Sum64String(prompt)
		} else if len(e.sumSuffix) > 0 && bytes.HasSuffix(key, e.sumSuffix) {
			// Summary key: read summary data from Pebble at the key directly
			val, closer, err := e.db.Get(key)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					e.logger.Warn("Failed to read summary for sparse enrichment",
						zap.ByteString("key", key), zap.Error(err))
				}
				continue
			}
			if len(val) < 8 {
				_ = closer.Close()
				continue
			}
			_ = closer.Close()
			prompt = strings.TrimSpace(string(val[8:]))
			if prompt == "" {
				continue
			}
			hashID = xxhash.Sum64String(prompt)
		} else {
			// Document key: read from Pebble (stored at key + DBRangeStart)
			docKey := storeutils.KeyRangeStart(key)
			val, closer, err := e.db.Get(docKey)
			if err != nil {
				if errors.Is(err, pebble.ErrNotFound) {
					continue
				}
				e.logger.Warn("Failed to read document for sparse enrichment",
					zap.ByteString("key", key),
					zap.Error(err))
				continue
			}

			// Decode document JSON (handles both zstd-compressed and plain JSON)
			doc, decErr := storeutils.DecodeDocumentJSON(val)
			_ = closer.Close()
			if decErr != nil {
				continue
			}

			// Extract prompt text from document
			var renderErr error
			prompt, hashID, renderErr = e.renderPrompt(doc)
			if renderErr != nil {
				e.logger.Warn("Failed to render prompt for sparse enrichment",
					zap.ByteString("key", key),
					zap.Error(renderErr))
				continue
			}
			if prompt == "" {
				continue
			}
		}

		// Check if existing sparse embedding has same hashID (skip if unchanged)
		sparseKey := append(bytes.Clone(key), e.sparseSuffix...)
		existing, existCloser, err := e.db.Get(sparseKey)
		if err == nil && len(existing) >= 8 {
			existingHashID := binary.LittleEndian.Uint64(existing[:8])
			_ = existCloser.Close()
			if existingHashID == hashID {
				continue
			}
		} else if err == nil {
			_ = existCloser.Close()
		}

		filteredKeys = append(filteredKeys, key)
		prompts = append(prompts, prompt)
		hashIDs = append(hashIDs, hashID)
	}

	if len(prompts) == 0 {
		return nil
	}

	// Rate limit before calling embedding API
	if err := waitRateLimiter(ctx, e.rateLimiter, len(prompts)); err != nil {
		return fmt.Errorf("waiting for rate limiter: %w", err)
	}

	// Generate sparse embeddings
	vecs, err := e.embedder.SparseEmbed(ctx, prompts)
	if err != nil {
		return retry.RetryableError(
			fmt.Errorf("generating sparse embeddings: %w", err),
		)
	}

	if len(vecs) != len(prompts) {
		return fmt.Errorf("expected %d sparse vectors, got %d", len(prompts), len(vecs))
	}

	// Encode and create writes for persistence through Raft
	writes := make([][2][]byte, 0, len(vecs))
	for i, vec := range vecs {
		sv := vector.NewSparseVector(vec.Indices, vec.Values)
		sparseBytes := EncodeSparseVec(sv)

		// Value format: [hashID uint64 LE][encoded sparse vec]
		value := make([]byte, 8+len(sparseBytes))
		binary.LittleEndian.PutUint64(value[:8], hashIDs[i])
		copy(value[8:], sparseBytes)

		sparseKey := append(bytes.Clone(filteredKeys[i]), e.sparseSuffix...)
		writes = append(writes, [2][]byte{sparseKey, value})
	}

	if len(writes) > 0 {
		if err := e.persistFunc(ctx, writes); err != nil {
			return fmt.Errorf("persisting sparse embeddings: %w", err)
		}
		e.logger.Debug("Persisted sparse embeddings",
			zap.Int("count", len(writes)),
			zap.String("index", e.name))
	}

	return nil
}

// GenerateSparseWithoutPersist generates sparse embeddings synchronously without
// persisting them, reading document bytes from documentValues (for pre-Raft path).
// Keys may be document keys or chunk keys. For chunk keys, decodes the chunk JSON
// from documentValues to extract prompt text.
// Returns [][2][]byte writes with sparseSuffix-suffixed keys.
func (e *SparseEmbeddingEnricher) GenerateSparseWithoutPersist(
	ctx context.Context,
	keys [][]byte,
	documentValues map[string][]byte,
) (writes [][2][]byte, failedKeys [][]byte, err error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}

	var (
		filteredKeys [][]byte
		prompts      []string
		hashIDs      []uint64
	)

	for _, key := range keys {
		var prompt string
		var hashID uint64

		if storeutils.IsChunkKey(key) {
			// Chunk key: value is [hashID:uint64][chunkJSON] from ChunkingEnricher
			val, ok := documentValues[string(key)]
			if !ok {
				failedKeys = append(failedKeys, key)
				continue
			}
			if len(val) < 8 {
				failedKeys = append(failedKeys, key)
				continue
			}
			var chunk chunking.Chunk
			if err := json.Unmarshal(val[8:], &chunk); err != nil {
				e.logger.Warn("Failed to unmarshal chunk in sparse pre-enrichment",
					zap.ByteString("key", key), zap.Error(err))
				failedKeys = append(failedKeys, key)
				continue
			}
			prompt = strings.TrimSpace(chunk.GetText())
			if prompt == "" {
				continue
			}
			hashID = xxhash.Sum64String(prompt)
		} else if len(e.sumSuffix) > 0 && bytes.HasSuffix(key, e.sumSuffix) {
			// Summary key: value is [hashID:uint64 big-endian][summary text]
			val, ok := documentValues[string(key)]
			if !ok {
				failedKeys = append(failedKeys, key)
				continue
			}
			if len(val) < 8 {
				failedKeys = append(failedKeys, key)
				continue
			}
			prompt = strings.TrimSpace(string(val[8:]))
			if prompt == "" {
				continue
			}
			hashID = xxhash.Sum64String(prompt)
		} else {
			// Document key: try documentValues first, fall back to Pebble
			val, ok := documentValues[string(key)]
			if ok {
				// Decode document JSON (handles both zstd-compressed and plain JSON)
				doc, decErr := storeutils.DecodeDocumentJSON(val)
				if decErr != nil {
					failedKeys = append(failedKeys, key)
					continue
				}
				var renderErr error
				prompt, hashID, renderErr = e.renderPrompt(doc)
				if renderErr != nil || prompt == "" {
					if renderErr != nil {
						failedKeys = append(failedKeys, key)
					}
					continue
				}
			} else {
				// Fall back to Pebble
				docKey := storeutils.KeyRangeStart(key)
				pebbleVal, closer, pebbleErr := e.db.Get(docKey)
				if pebbleErr != nil {
					if !errors.Is(pebbleErr, pebble.ErrNotFound) {
						failedKeys = append(failedKeys, key)
					}
					continue
				}
				doc, decErr := storeutils.DecodeDocumentJSON(pebbleVal)
				_ = closer.Close()
				if decErr != nil {
					failedKeys = append(failedKeys, key)
					continue
				}
				var renderErr error
				prompt, hashID, renderErr = e.renderPrompt(doc)
				if renderErr != nil || prompt == "" {
					if renderErr != nil {
						failedKeys = append(failedKeys, key)
					}
					continue
				}
			}
		}

		filteredKeys = append(filteredKeys, key)
		prompts = append(prompts, prompt)
		hashIDs = append(hashIDs, hashID)
	}

	if len(prompts) == 0 {
		return nil, failedKeys, nil
	}

	if err := waitRateLimiter(ctx, e.rateLimiter, len(prompts)); err != nil {
		return nil, append(failedKeys, filteredKeys...), fmt.Errorf("rate limiter: %w", err)
	}

	vecs, err := e.embedder.SparseEmbed(ctx, prompts)
	if err != nil {
		return nil, append(failedKeys, filteredKeys...), fmt.Errorf("sparse embed: %w", err)
	}

	if len(vecs) != len(prompts) {
		return nil, append(failedKeys, filteredKeys...), fmt.Errorf("expected %d sparse vectors, got %d", len(prompts), len(vecs))
	}

	writes = make([][2][]byte, 0, len(vecs))
	for i, vec := range vecs {
		sv := vector.NewSparseVector(vec.Indices, vec.Values)
		sparseBytes := EncodeSparseVec(sv)
		value := make([]byte, 8+len(sparseBytes))
		binary.LittleEndian.PutUint64(value[:8], hashIDs[i])
		copy(value[8:], sparseBytes)
		sparseKey := append(bytes.Clone(filteredKeys[i]), e.sparseSuffix...)
		writes = append(writes, [2][]byte{sparseKey, value})
	}

	return writes, failedKeys, nil
}

// SparsePipelineAdapter wraps SparseEmbeddingEnricher to implement
// TerminalEmbeddingEnricher for use inside PipelineEnricher.
type SparsePipelineAdapter struct {
	inner *SparseEmbeddingEnricher
	ctx   context.Context
}

var _ TerminalEmbeddingEnricher = (*SparsePipelineAdapter)(nil)

// NewSparsePipelineAdapter creates an adapter that makes SparseEmbeddingEnricher
// usable as a TerminalEmbeddingEnricher inside PipelineEnricher.
func NewSparsePipelineAdapter(ctx context.Context, inner *SparseEmbeddingEnricher) *SparsePipelineAdapter {
	return &SparsePipelineAdapter{inner: inner, ctx: ctx}
}

func (a *SparsePipelineAdapter) EnrichBatch(keys [][]byte) error {
	return a.inner.EnrichBatch(a.ctx, keys)
}

func (a *SparsePipelineAdapter) GenerateEmbeddingsWithoutPersist(
	ctx context.Context,
	keys [][]byte,
	documentValues map[string][]byte,
	_ generatePromptsFunc,
) ([][2][]byte, [][2][]byte, [][]byte, error) {
	writes, failed, err := a.inner.GenerateSparseWithoutPersist(ctx, keys, documentValues)
	return writes, nil, failed, err
}

func (a *SparsePipelineAdapter) EnricherStats() EnricherStats {
	return EnricherStats{} // Sparse adapter has no WAL
}

func (a *SparsePipelineAdapter) Close() error {
	return a.inner.Close()
}

// renderPrompt extracts text from a document using the configured field or template.
func (e *SparseEmbeddingEnricher) renderPrompt(doc map[string]any) (string, uint64, error) {
	return extractSparsePrompt(e.promptTemplate, e.indexingField, doc)
}
