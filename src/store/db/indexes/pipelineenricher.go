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
	"strings"
	"sync"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

// PipelineEnricherConfig holds all configuration for creating a PipelineEnricher.
type PipelineEnricherConfig struct {
	// Core dependencies
	Logger       *zap.Logger
	AntflyConfig *common.Config
	DB           *pebble.DB
	Dir          string
	Name         string

	// Key suffixes and range
	EmbSuffix []byte
	SumSuffix []byte
	ByteRange types.Range

	// Terminal enricher and embedder
	Terminal       TerminalEmbeddingEnricher // Optional: pre-built terminal (e.g. sparse adapter)
	EmbedderConfig embeddings.EmbedderConfig // Only used when Terminal is nil

	// Optional enrichment stages
	SummarizerConfig *ai.GeneratorConfig     // Optional: nil if summarization disabled
	ChunkingConfig   *chunking.ChunkerConfig // Optional: nil if chunking disabled
	StoreChunks      bool                    // When false, chunks are ephemeral (in-memory only)

	// Prompt and persistence functions
	GeneratePrompts   generatePromptsFunc
	PersistEmbeddings PersistEmbeddingsFunc // Only used when Terminal is nil
	PersistSummaries  PersistSummariesFunc
	PersistChunks     PersistChunksFunc
	PersistFunc       PersistFunc // Raw Raft persist for ephemeral chunk offset writes

	// Optional backfill support
	ExtractPrompt extractPromptFunc // Optional: extracts text from Document when Enrichment is nil (backfill)
}

func NewPipelineEnricher(ctx context.Context, cfg PipelineEnricherConfig) (Enricher, error) {
	// Create the PipelineEnricher struct first so we can use its methods
	pe := &PipelineEnricher{
		logger:         cfg.Logger,
		indexName:      cfg.Name,
		sumSuffix:      cfg.SumSuffix,
		storeChunks:    cfg.StoreChunks,
		chunkingConfig: cfg.ChunkingConfig,
		extractPrompt:  cfg.ExtractPrompt,
		persistFunc:    cfg.PersistFunc,
	}

	// Initialize chunking enricher or helper based on storeChunks setting
	var ce *ChunkingEnricher
	var err error
	var promptsFunc = cfg.GeneratePrompts

	if cfg.ChunkingConfig != nil {
		if cfg.StoreChunks {
			// Traditional mode: create ChunkingEnricher that persists chunks
			ce, err = NewChunkingEnricher(
				ctx,
				cfg.Logger,
				cfg.DB,
				cfg.Dir,
				cfg.Name,
				cfg.ByteRange,
				*cfg.ChunkingConfig,
				cfg.PersistChunks,
				cfg.GeneratePrompts, // Pass generatePrompts to use index config for text extraction
			)
			if err != nil {
				return nil, fmt.Errorf("creating chunking enricher: %w", err)
			}
			promptsFunc = pe.GeneratePrompts
		} else {
			// Ephemeral mode: create ChunkingHelper for in-memory chunking
			// No ChunkingEnricher created (chunks won't be persisted)
			helper, err := chunking.NewChunkingHelper(cfg.Name, *cfg.ChunkingConfig, cfg.Logger)
			if err != nil {
				return nil, fmt.Errorf("creating chunking helper for ephemeral mode: %w", err)
			}
			pe.chunkingHelper = helper
			// Use ephemeral prompt generator that does inline chunking + fan-out
			promptsFunc = pe.GenerateEphemeralChunkPrompts
			cfg.Logger.Info("Ephemeral chunking mode enabled",
				zap.String("index", cfg.Name),
				zap.String("provider", string(cfg.ChunkingConfig.Provider)))
		}
	}

	// Initialize summarize enricher if configured
	var se *SummarizeEnricher
	if cfg.SummarizerConfig != nil {
		se, err = NewSummarizeEnricher(
			ctx,
			cfg.Logger,
			cfg.DB,
			cfg.Dir,
			cfg.Name,
			cfg.EmbSuffix,
			cfg.SumSuffix,
			cfg.ByteRange,
			*cfg.SummarizerConfig,
			cfg.GeneratePrompts,
			cfg.PersistSummaries,
		)
		if err != nil {
			return nil, err
		}
		promptsFunc = pe.GeneratePrompts
	}

	if cfg.Terminal != nil {
		// Use pre-built terminal enricher (e.g. SparsePipelineAdapter)
		pe.Terminal = cfg.Terminal
	} else {
		// Create dense EmbeddingEnricher internally

		// Determine the suffix that the embedding enricher should scan for during backfill
		var toIndexSuffix []byte
		if cfg.SummarizerConfig != nil {
			// If we have a summarizer, embed summaries
			toIndexSuffix = cfg.SumSuffix
		} else if cfg.ChunkingConfig != nil && cfg.StoreChunks {
			// If we have chunking with persistence, embed chunks directly
			if chunking.GetFullTextIndex(*cfg.ChunkingConfig) != nil {
				toIndexSuffix = storeutils.ChunkingFullTextSuffix
			} else {
				toIndexSuffix = storeutils.ChunkingSuffix
			}
		} else {
			// No chunking, no summarizer, OR ephemeral chunking - embed documents directly
			// In ephemeral mode, we scan for documents (DBRangeStart) and the sentinel key
			// at docKey+embSuffix signals completion
			toIndexSuffix = storeutils.DBRangeStart
		}

		// In ephemeral chunking mode, wire up the post-persist hook to flush
		// chunk offset writes accumulated during GenerateEphemeralChunkPrompts.
		var hook PostPersistHookFunc
		if cfg.ChunkingConfig != nil && !cfg.StoreChunks && cfg.PersistFunc != nil {
			hook = pe.flushPendingChunkWrites
		}

		ee, err := NewEmbeddingEnricher(
			ctx,
			cfg.Logger,
			cfg.AntflyConfig,
			cfg.DB,
			cfg.Dir,
			cfg.Name,
			toIndexSuffix,
			cfg.EmbSuffix,
			cfg.ByteRange,
			cfg.EmbedderConfig,
			promptsFunc,
			cfg.PersistEmbeddings,
			hook,
		)
		if err != nil {
			return nil, err
		}
		pe.Terminal = ee
	}

	pe.Summarizer = se
	pe.ChunkingEnricher = ce

	return pe, nil
}

// extractPromptFunc extracts a prompt string and hash ID from a document map.
// Used as a fallback in ephemeral chunking when state.Enrichment is nil (backfill case).
type extractPromptFunc func(doc map[string]any) (prompt string, hashID uint64, err error)

type PipelineEnricher struct {
	logger    *zap.Logger
	indexName string // Index name for chunk key routing
	sumSuffix []byte // Suffix for summary keys which should be embedded.
	// Enrich the data with embeddings, summaries, and chunks.
	Terminal         TerminalEmbeddingEnricher
	Summarizer       *SummarizeEnricher
	ChunkingEnricher *ChunkingEnricher // Optional: nil if chunking disabled or ephemeral

	// Ephemeral chunking fields (when storeChunks == false)
	storeChunks    bool                     // Whether to persist chunks to storage
	chunkingConfig *chunking.ChunkerConfig  // Stored for ephemeral chunk use
	chunkingHelper *chunking.ChunkingHelper // Helper for in-memory chunking (ephemeral mode)
	extractPrompt  extractPromptFunc        // Optional: extracts text from Document when Enrichment is nil

	// Ephemeral chunk offset persistence: accumulates offset-only chunk writes
	// during GenerateEphemeralChunkPrompts, flushed after embeddings are persisted.
	// Concurrency note: the backfill goroutine and sync ComputeEnrichments path
	// can both call GenerateEphemeralChunkPrompts concurrently. The mutex serializes
	// individual drain/append operations but not entire generate-then-flush cycles.
	// This is acceptable because chunk offset writes are idempotent (same key always
	// produces the same value), so cross-path interference at worst causes a
	// duplicate write or a missed write that gets re-discovered on next backfill.
	persistFunc          PersistFunc // Raw Raft persist for chunk offset writes
	pendingChunkWritesMu sync.Mutex
	pendingChunkWrites   [][2][]byte
}

func (e *PipelineEnricher) EnricherStats() EnricherStats {
	stats := e.Terminal.EnricherStats()
	if e.ChunkingEnricher != nil {
		cs := e.ChunkingEnricher.enricherStats()
		stats.WALBacklog += cs.WALBacklog
		wasBackfilling := stats.Backfilling
		stats.Backfilling = stats.Backfilling || cs.Backfilling
		stats.BackfillItemsProcessed += cs.BackfillItemsProcessed
		if cs.Backfilling {
			if !wasBackfilling || cs.BackfillProgress < stats.BackfillProgress {
				stats.BackfillProgress = cs.BackfillProgress
			}
		}
	}
	if e.Summarizer != nil {
		ss := e.Summarizer.enricherStats()
		stats.WALBacklog += ss.WALBacklog
		wasBackfilling := stats.Backfilling
		stats.Backfilling = stats.Backfilling || ss.Backfilling
		stats.BackfillItemsProcessed += ss.BackfillItemsProcessed
		if ss.Backfilling {
			if !wasBackfilling || ss.BackfillProgress < stats.BackfillProgress {
				stats.BackfillProgress = ss.BackfillProgress
			}
		}
	}
	return stats
}

func (e *PipelineEnricher) Close() error {
	// Close the underlying enrichers.
	if e.ChunkingEnricher != nil {
		if err := e.ChunkingEnricher.Close(); err != nil {
			return fmt.Errorf("closing chunking enricher: %w", err)
		}
	}
	// Note: chunkingHelper doesn't need explicit cleanup (no Close method)
	if err := e.Terminal.Close(); err != nil {
		return fmt.Errorf("closing embedding enricher: %w", err)
	}
	if e.Summarizer != nil {
		if err := e.Summarizer.Close(); err != nil {
			return fmt.Errorf("closing summarizer: %w", err)
		}
	}
	return nil
}

func (e *PipelineEnricher) EnrichBatch(keys [][]byte) error {
	chunkKeys := make([][]byte, 0, len(keys))
	summarizeKeys := make([][]byte, 0, len(keys))
	embedKeys := make([][]byte, 0, len(keys))

	// Route keys based on suffix
	// Pipeline supports these combinations:
	// 1. docs → embeddings (no chunking, no summarization)
	// 2. docs → summaries → embeddings (summarization only)
	// 3. docs → chunks → embeddings (chunking only)
	// 4. docs → chunks → summaries → embeddings (chunking + summarization)
	for _, key := range keys {
		// Check if key is a chunk key (contains :c: or :cft: pattern)
		if storeutils.IsChunkKey(key) {
			// Chunk keys → summarizer (if available) OR embedder (if no summarizer)
			if e.Summarizer != nil {
				summarizeKeys = append(summarizeKeys, key)
			} else {
				// No summarizer: embed chunks directly
				embedKeys = append(embedKeys, key)
			}
			continue
		}

		if bytes.HasSuffix(key, e.sumSuffix) {
			// Summary suffix keys → embedding enricher
			embedKeys = append(embedKeys, key)
			continue
		}

		// Regular document keys
		if e.ChunkingEnricher != nil {
			// If chunking enabled → chunking enricher
			chunkKeys = append(chunkKeys, key)
		} else if e.Summarizer != nil {
			// No chunking, but summarizer enabled → summarizer directly
			summarizeKeys = append(summarizeKeys, key)
		} else {
			// No chunking, no summarizer → embed documents directly
			embedKeys = append(embedKeys, key)
		}
	}

	// Process in pipeline order
	if len(chunkKeys) > 0 {
		e.logger.Debug("Chunking documents", zap.Int("numKeys", len(chunkKeys)))
		if err := e.ChunkingEnricher.EnrichBatch(chunkKeys); err != nil {
			return fmt.Errorf("enriching batch with chunks: %w", err)
		}
	}

	if len(summarizeKeys) > 0 {
		e.logger.Debug("Summarizing documents/chunks", zap.Int("numKeys", len(summarizeKeys)))
		// Enrich the batch with summaries.
		if err := e.Summarizer.EnrichBatch(summarizeKeys); err != nil {
			return fmt.Errorf("enriching batch with summaries: %w", err)
		}
	}

	if len(embedKeys) > 0 {
		e.logger.Debug("Enriching summaries to embeddings", zap.Int("numKeys", len(embedKeys)))
		// Enrich the batch with embeddings.
		if err := e.Terminal.EnrichBatch(embedKeys); err != nil {
			return fmt.Errorf("enriching batch with embeddings: %w", err)
		}
	}

	return nil
}

// GenerateEmbeddingsWithoutPersist generates embeddings, chunks, and summaries synchronously WITHOUT persisting them
// This implements the full pipeline: Document → Chunks → Summaries → Embeddings
// All enrichment writes are returned for inclusion in the same Raft batch
// documentValues map provides zstd-compressed document values for document keys (not enrichment keys)
// generatePrompts is a function that extracts prompts from in-memory documents (uses ExtractPrompt, not Pebble reads)
func (e *PipelineEnricher) GenerateEmbeddingsWithoutPersist(
	ctx context.Context,
	keys [][]byte,
	documentValues map[string][]byte,
	generatePrompts generatePromptsFunc,
) (embeddingWrites [][2][]byte, chunkWrites [][2][]byte, failedKeys [][]byte, err error) {
	if len(keys) == 0 {
		return nil, nil, nil, nil
	}

	// Separate keys by type: document keys vs enrichment keys (chunks, summaries)
	var documentKeys [][]byte   // Need chunking/summarization
	var enrichmentKeys [][]byte // Already chunks or summaries, can embed directly

	for _, key := range keys {
		if bytes.HasSuffix(key, e.sumSuffix) || storeutils.IsChunkKey(key) {
			// Summary or chunk keys can be directly embedded
			enrichmentKeys = append(enrichmentKeys, key)
		} else {
			// Regular document keys need processing through the pipeline
			documentKeys = append(documentKeys, key)
		}
	}

	allChunkWrites := make([][2][]byte, 0, len(keys)*4)
	allEmbeddingWrites := make([][2][]byte, 0, len(keys))
	allFailedKeys := make([][]byte, 0)

	// Step 1: Process document keys through chunking (if configured)
	if len(documentKeys) > 0 && e.ChunkingEnricher != nil {
		e.logger.Debug("Chunking documents in pre-enrichment",
			zap.Int("numDocs", len(documentKeys)))

		// Build writes from documentValues map for document keys
		documentWrites := make([][2][]byte, 0, len(documentKeys))
		for _, key := range documentKeys {
			if val, ok := documentValues[string(key)]; ok {
				documentWrites = append(documentWrites, [2][]byte{key, val})
			} else {
				e.logger.Warn("Document value not found for chunking pre-enrichment",
					zap.String("key", types.FormatKey(key)))
				allFailedKeys = append(allFailedKeys, key)
			}
		}

		chunkWrites, chunkFailed, err := e.ChunkingEnricher.GenerateChunksWithoutPersist(ctx, documentWrites, generatePrompts)
		if err != nil {
			e.logger.Warn("Failed to generate chunks in pre-enrichment",
				zap.Error(err),
				zap.Int("numKeys", len(documentKeys)))
			// On error, mark all document keys as failed
			allFailedKeys = append(allFailedKeys, documentKeys...)
		} else {
			// Collect chunk writes
			allChunkWrites = append(allChunkWrites, chunkWrites...)

			// Extract chunk keys to embed AND add chunk values to documentValues
			// so embedding enricher can read them from memory instead of Pebble
			for _, chunkWrite := range chunkWrites {
				enrichmentKeys = append(enrichmentKeys, chunkWrite[0])
				// Add chunk value to documentValues map for pre-enrichment
				documentValues[string(chunkWrite[0])] = chunkWrite[1]
			}

			// Track failed keys
			allFailedKeys = append(allFailedKeys, chunkFailed...)
		}
	} else if len(documentKeys) > 0 && e.chunkingHelper != nil {
		// Ephemeral chunking: chunk in memory, produce offset writes + prompt keys
		chunkWrites, promptKeys, err := e.generateEphemeralChunksForSync(ctx, documentKeys, documentValues)
		if err != nil {
			e.logger.Warn("Failed ephemeral chunking in pre-enrichment", zap.Error(err))
			allFailedKeys = append(allFailedKeys, documentKeys...)
		} else {
			allChunkWrites = append(allChunkWrites, chunkWrites...)
			// Prompt keys are virtual chunk keys that need embedding
			enrichmentKeys = append(enrichmentKeys, promptKeys...)
		}
	} else if len(documentKeys) > 0 {
		// No chunking configured - document keys can be embedded directly
		enrichmentKeys = append(enrichmentKeys, documentKeys...)
	}

	// Step 2: Generate embeddings for enrichment keys (chunks, summaries, or documents)
	if len(enrichmentKeys) > 0 {
		e.logger.Debug("Embedding enrichment keys in pre-enrichment",
			zap.Int("numKeys", len(enrichmentKeys)))

		// Create a wrapper that populates state.Enrichment from documentValues
		// and then calls PipelineEnricher.GeneratePrompts (which doesn't use Pebble)
		generatePromptsFromMemory := func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
			// Populate state.Enrichment from documentValues
			// For ephemeral chunk keys, the value is [hashID][prompt text] (not chunk JSON),
			// so we return prompts directly instead of calling GeneratePrompts which would
			// try to JSON-parse the prompt text as a chunking.Chunk.
			missingKeys := 0
			var directKeys [][]byte
			var directPrompts []string
			var directHashIDs []uint64
			var delegateStates []storeutils.DocumentScanState

			for i := range states {
				val, ok := documentValues[string(states[i].CurrentDocKey)]
				if !ok {
					e.logger.Warn("Key not found in documentValues for prompt generation",
						zap.String("key", types.FormatKey(states[i].CurrentDocKey)))
					missingKeys++
					continue
				}

				if len(val) < 8 {
					continue
				}

				content := string(val[8:])

				// Ephemeral chunk keys have raw prompt text in documentValues (not chunk JSON).
				// Detect this by checking if the content is NOT valid JSON starting with '{'.
				// Regular (persisted) chunk keys have serialized chunk JSON.
				if e.chunkingHelper != nil && storeutils.IsChunkKey(states[i].CurrentDocKey) && (len(content) == 0 || content[0] != '{') {
					prompt := strings.TrimSpace(content)
					if len(prompt) == 0 {
						continue
					}
					hashID := xxhash.Sum64String(prompt)
					directKeys = append(directKeys, states[i].CurrentDocKey)
					directPrompts = append(directPrompts, prompt)
					directHashIDs = append(directHashIDs, hashID)
				} else {
					states[i].Enrichment = content
					delegateStates = append(delegateStates, states[i])
				}
			}

			if missingKeys > 0 {
				e.logger.Warn("Keys missing from documentValues during pre-enrichment",
					zap.Int("totalStates", len(states)),
					zap.Int("missingKeys", missingKeys),
					zap.Int("documentValuesSize", len(documentValues)))
			}

			// Delegate non-ephemeral states to GeneratePrompts
			if len(delegateStates) > 0 {
				k, p, h, err := e.GeneratePrompts(ctx, delegateStates)
				if err != nil {
					return nil, nil, nil, err
				}
				directKeys = append(directKeys, k...)
				directPrompts = append(directPrompts, p...)
				directHashIDs = append(directHashIDs, h...)
			}

			return directKeys, directPrompts, directHashIDs, nil
		}

		embWrites, embChunkWrites, embFailed, err := e.Terminal.GenerateEmbeddingsWithoutPersist(ctx, enrichmentKeys, documentValues, generatePromptsFromMemory)
		if err != nil {
			e.logger.Warn("Failed to generate embeddings in pre-enrichment",
				zap.Error(err),
				zap.Int("numKeys", len(enrichmentKeys)))
			// On error, mark all enrichment keys as failed
			allFailedKeys = append(allFailedKeys, enrichmentKeys...)
		} else {
			// Collect embedding writes
			allEmbeddingWrites = append(allEmbeddingWrites, embWrites...)

			// Collect any additional chunk writes from embedding enricher
			// (in case EmbeddingEnricher has its own chunking config)
			if len(embChunkWrites) > 0 {
				allChunkWrites = append(allChunkWrites, embChunkWrites...)
			}

			// Track failed keys
			allFailedKeys = append(allFailedKeys, embFailed...)
		}
	}

	e.logger.Debug("PipelineEnricher pre-enrichment complete",
		zap.Int("totalKeys", len(keys)),
		zap.Int("embeddingWrites", len(allEmbeddingWrites)),
		zap.Int("chunkWrites", len(allChunkWrites)),
		zap.Int("failedKeys", len(allFailedKeys)))

	return allEmbeddingWrites, allChunkWrites, allFailedKeys, nil
}

func (e *PipelineEnricher) GeneratePrompts(
	ctx context.Context, states []storeutils.DocumentScanState,
) (keys [][]byte, prompts []string, hashIDs []uint64, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(states) == 0 {
		return nil, nil, nil, nil
	}
	promptKeys := make([][]byte, 0, len(states))
	promptBatch := make([]string, 0, len(states))
	promptHashIDs := make([]uint64, 0, len(states))

	for _, state := range states {
		// Enrichment data (summary from previous stage) should always be populated by ScanForEnrichment
		if state.Enrichment == nil {
			e.logger.Warn(
				"GeneratePrompts: Enrichment is nil in state, skipping",
				zap.String("key", types.FormatKey(state.CurrentDocKey)),
			)
			continue
		}

		prompt, ok := state.Enrichment.(string)
		if !ok || len(prompt) == 0 {
			e.logger.Warn(
				"GeneratePrompts: Enrichment is not a string or is empty, skipping",
				zap.String("key", types.FormatKey(state.CurrentDocKey)),
			)
			continue
		}
		// Check if this is a chunk key - chunks need special handling
		if storeutils.IsChunkKey(state.CurrentDocKey) {
			var chunk chunking.Chunk
			if err := json.UnmarshalString(prompt, &chunk); err != nil {
				return nil, nil, nil, fmt.Errorf("decoding chunk JSON from state: %w", err)
			}
			// Use chunk text as prompt
			prompt = strings.TrimSpace(chunk.GetText())
			if len(prompt) == 0 {
				e.logger.Warn("GeneratePrompts: Skipping chunk with empty text after trimming whitespace",
					zap.ByteString("chunkKey", state.CurrentDocKey),
					zap.String("originalText", chunk.GetText()))
				continue
			}
		}

		promptHashID := xxhash.Sum64String(prompt)
		// Use hash ID from state (populated by ScanForEnrichment)
		if state.EnrichmentHashID != 0 && state.EnrichmentHashID == promptHashID {
			// If the hash matches, we can skip this prompt
			continue
		}
		promptKeys = append(promptKeys, state.CurrentDocKey)
		promptBatch = append(promptBatch, prompt)
		promptHashIDs = append(promptHashIDs, promptHashID)
	}
	return promptKeys, promptBatch, promptHashIDs, nil
}

// chunkOffsetJSON is a minimal struct for serializing chunk offset metadata without
// going through the Chunk union type (avoids triple JSON round-trip).
// Field names match chunking.Chunk so the output is deserializable as a Chunk.
type chunkOffsetJSON struct {
	Id          uint32          `json:"id"`
	MimeType    string          `json:"mime_type,omitempty"`
	TextContent *textOffsetJSON `json:"text_content"`
}

type textOffsetJSON struct {
	StartChar int `json:"start_char"`
	EndChar   int `json:"end_char"`
}

// GenerateEphemeralChunkPrompts handles the 1:N fan-out for ephemeral chunking mode.
// For each document state, it reads the document from Pebble, extracts text, chunks in memory,
// and returns N prompts (one per chunk). Virtual chunk keys are generated but chunks are not stored.
func (e *PipelineEnricher) GenerateEphemeralChunkPrompts(
	ctx context.Context, states []storeutils.DocumentScanState,
) (keys [][]byte, prompts []string, hashIDs []uint64, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	if len(states) == 0 {
		return nil, nil, nil, nil
	}

	if e.chunkingHelper == nil {
		return nil, nil, nil, fmt.Errorf("ephemeral chunking mode but chunkingHelper is nil")
	}

	// Safety: drain any stale writes from a previous failed call to prevent accumulation
	e.pendingChunkWritesMu.Lock()
	e.pendingChunkWrites = nil
	e.pendingChunkWritesMu.Unlock()

	promptKeys := make([][]byte, 0, len(states)*2) // Estimate 2 chunks per doc on average
	promptBatch := make([]string, 0, len(states)*2)
	promptHashIDs := make([]uint64, 0, len(states)*2)
	var chunkOffsetWrites [][2][]byte

	// Determine chunk key function based on full_text config (invariant across states)
	useFullTextKey := e.chunkingConfig != nil && chunking.GetFullTextIndex(*e.chunkingConfig) != nil

	for _, state := range states {
		var text string

		if state.Enrichment != nil {
			// Normal path: Enrichment is populated (e.g., scanning enrichment keys)
			var ok bool
			text, ok = state.Enrichment.(string)
			if !ok || len(text) == 0 {
				e.logger.Warn(
					"GenerateEphemeralChunkPrompts: Enrichment is not a string or is empty, skipping",
					zap.String("key", types.FormatKey(state.CurrentDocKey)),
				)
				continue
			}
		} else if state.Document != nil && e.extractPrompt != nil {
			// Backfill path: ScanForEnrichment with DBRangeStart populates Document
			// but leaves Enrichment nil. Use extractPrompt to get text from the document.
			prompt, _, err := e.extractPrompt(state.Document)
			if err != nil {
				e.logger.Warn(
					"GenerateEphemeralChunkPrompts: extractPrompt failed, skipping",
					zap.String("key", types.FormatKey(state.CurrentDocKey)),
					zap.Error(err),
				)
				continue
			}
			text = strings.TrimSpace(prompt)
			if len(text) == 0 {
				e.logger.Warn(
					"GenerateEphemeralChunkPrompts: extractPrompt returned empty text, skipping",
					zap.String("key", types.FormatKey(state.CurrentDocKey)),
				)
				continue
			}
		} else {
			e.logger.Warn(
				"GenerateEphemeralChunkPrompts: neither Enrichment nor Document available, skipping",
				zap.String("key", types.FormatKey(state.CurrentDocKey)),
			)
			continue
		}

		// Chunk the document in memory
		chunks, err := e.chunkingHelper.ChunkDocument(text)
		if err != nil {
			e.logger.Warn("GenerateEphemeralChunkPrompts: Chunking failed",
				zap.String("key", types.FormatKey(state.CurrentDocKey)),
				zap.Error(err))
			continue
		}

		if len(chunks) == 0 {
			e.logger.Debug("GenerateEphemeralChunkPrompts: No chunks produced",
				zap.String("key", types.FormatKey(state.CurrentDocKey)))
			continue
		}

		// Generate prompts for each chunk
		for _, chunk := range chunks {
			prompt := strings.TrimSpace(chunk.GetText())
			if len(prompt) == 0 {
				e.logger.Warn("GenerateEphemeralChunkPrompts: Skipping chunk with empty text",
					zap.String("key", types.FormatKey(state.CurrentDocKey)),
					zap.Uint32("chunkId", chunk.Id))
				continue
			}

			promptHashID := xxhash.Sum64String(prompt)

			// Generate chunk key (same format as persistent chunks)
			var chunkKey []byte
			if useFullTextKey {
				chunkKey = storeutils.MakeChunkFullTextKey(state.CurrentDocKey, e.indexName, chunk.Id)
			} else {
				chunkKey = storeutils.MakeChunkKey(state.CurrentDocKey, e.indexName, chunk.Id)
			}

			promptKeys = append(promptKeys, chunkKey)
			promptBatch = append(promptBatch, prompt)
			promptHashIDs = append(promptHashIDs, promptHashID)

			// Build offset-only chunk write (strip text, keep start_char/end_char).
			// Uses a minimal struct to avoid triple JSON round-trip through the Chunk union type.
			if e.persistFunc != nil {
				tc, tcErr := chunk.AsTextContent()
				if tcErr == nil {
					chunkJSON, marshalErr := json.Marshal(chunkOffsetJSON{
						Id:       chunk.Id,
						MimeType: chunk.MimeType,
						TextContent: &textOffsetJSON{
							StartChar: tc.StartChar,
							EndChar:   tc.EndChar,
						},
					})
					if marshalErr == nil {
						b := make([]byte, 0, len(chunkJSON)+8)
						b = encoding.EncodeUint64Ascending(b, promptHashID)
						b = append(b, chunkJSON...)
						chunkOffsetWrites = append(chunkOffsetWrites, [2][]byte{chunkKey, b})
					}
				}
			}
		}
	}

	// Store pending chunk offset writes for flushing after embeddings persist
	if len(chunkOffsetWrites) > 0 {
		e.pendingChunkWritesMu.Lock()
		e.pendingChunkWrites = append(e.pendingChunkWrites, chunkOffsetWrites...)
		e.pendingChunkWritesMu.Unlock()
	}

	e.logger.Debug("GenerateEphemeralChunkPrompts completed",
		zap.Int("inputStates", len(states)),
		zap.Int("outputPrompts", len(promptBatch)),
		zap.Int("chunkOffsetWrites", len(chunkOffsetWrites)))

	return promptKeys, promptBatch, promptHashIDs, nil
}

// flushPendingChunkWrites drains accumulated chunk offset writes and persists them via Raft.
func (e *PipelineEnricher) flushPendingChunkWrites(ctx context.Context) error {
	e.pendingChunkWritesMu.Lock()
	writes := e.pendingChunkWrites
	e.pendingChunkWrites = nil
	e.pendingChunkWritesMu.Unlock()

	if len(writes) == 0 || e.persistFunc == nil {
		return nil
	}

	e.logger.Debug("Flushing ephemeral chunk offset writes",
		zap.Int("numWrites", len(writes)))
	return e.persistFunc(ctx, writes)
}

// generateEphemeralChunksForSync performs in-memory chunking for the sync pre-enrichment path.
// Returns offset-only chunk writes and virtual chunk keys (with prompt data in documentValues).
func (e *PipelineEnricher) generateEphemeralChunksForSync(
	ctx context.Context,
	documentKeys [][]byte,
	documentValues map[string][]byte,
) (chunkWrites [][2][]byte, promptKeys [][]byte, err error) {
	// Build states from documentValues for prompt extraction
	states := make([]storeutils.DocumentScanState, 0, len(documentKeys))
	for _, key := range documentKeys {
		val, ok := documentValues[string(key)]
		if !ok {
			continue
		}
		state := storeutils.DocumentScanState{CurrentDocKey: key}
		// Try to decode document for extractPrompt fallback
		if e.extractPrompt != nil {
			if doc, err := storeutils.DecodeDocumentJSON(val); err == nil {
				state.Document = doc
			}
		}
		states = append(states, state)
	}

	// Use GenerateEphemeralChunkPrompts which does chunking + offset accumulation
	keys, prompts, hashIDs, err := e.GenerateEphemeralChunkPrompts(ctx, states)
	if err != nil {
		return nil, nil, fmt.Errorf("ephemeral chunk prompts: %w", err)
	}

	// Drain the pending chunk writes (they were accumulated by GenerateEphemeralChunkPrompts)
	e.pendingChunkWritesMu.Lock()
	chunkWrites = e.pendingChunkWrites
	e.pendingChunkWrites = nil
	e.pendingChunkWritesMu.Unlock()

	// Add prompt data to documentValues so the embedding enricher can read them
	for i, key := range keys {
		// Store as [hashID:uint64][prompt text] — same format as enrichment values
		b := make([]byte, 0, len(prompts[i])+8)
		b = encoding.EncodeUint64Ascending(b, hashIDs[i])
		b = append(b, prompts[i]...)
		documentValues[string(key)] = b
	}

	return chunkWrites, keys, nil
}
