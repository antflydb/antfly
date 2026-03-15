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
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sethvargo/go-retry"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// waitRateLimiter splits a WaitN call into burst-sized chunks so that it
// works when n exceeds the limiter's burst (which can happen after document
// chunking expands a batch into more prompts than the original batch size).
// If limiter is nil (local providers like Termite), it returns immediately.
func waitRateLimiter(ctx context.Context, limiter *rate.Limiter, n int) error {
	if limiter == nil {
		return nil
	}
	burst := limiter.Burst()
	for n > 0 {
		take := min(n, burst)
		if err := limiter.WaitN(ctx, take); err != nil {
			return err
		}
		n -= take
	}
	return nil
}

// enricherRateLimitOverride, when non-nil, overrides the per-provider rate
// limiter for all enrichers. Set via ANTFLY_ENRICHER_RATE_LIMIT env var.
var enricherRateLimitOverride *rate.Limiter

func init() {
	limitStr := os.Getenv("ANTFLY_ENRICHER_RATE_LIMIT")
	if limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			panic("Invalid ANTFLY_ENRICHER_RATE_LIMIT value: " + limitStr)
		}
		enricherRateLimitOverride = rate.NewLimiter(rate.Limit(limit), limit)
	}
}

// resolveRateLimiter returns the enricher rate limit override (if set via env),
// otherwise falls back to the embedder's own rate limiter (nil for local providers).
func resolveRateLimiter(e embeddings.Embedder) *rate.Limiter {
	if enricherRateLimitOverride != nil {
		return enricherRateLimitOverride
	}
	return e.RateLimiter()
}

type EmbeddingEnricher struct {
	walEnricherBase

	name string

	// Config
	generateEmbeddings embeddings.Embedder
	persistEmbeddings  PersistEmbeddingsFunc
	embSuffix          []byte // Suffix for embedding keys

	generatePrompts generatePromptsFunc

	// Per-provider rate limiter (nil for local providers like Termite).
	rateLimiter *rate.Limiter

	// Optional: called after embeddings are persisted (used for ephemeral chunk offset writes)
	postPersistHook func(ctx context.Context) error

	// Cached Prometheus counter to avoid WithLabelValues map lookup per batch.
	embeddingCreationCounter prometheus.Counter
}

type enrichOp struct {
	Keys [][]byte `json:"keys"`

	ei *EmbeddingEnricher `json:"-"`
	db *pebble.DB         `json:"-"`
}

func (eo *enrichOp) encode() ([]byte, error) {
	data, err := json.Marshal(eo)
	if err != nil {
		return nil, fmt.Errorf("encoding json: %w", err)
	}
	return enricherZstdEncoder.EncodeAll(data, nil), nil
}

func (eo *enrichOp) decode(data []byte) error {
	dec, err := enricherZstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(dec, eo)
}

var enrichOpPool = sync.Pool{
	New: func() any {
		return &enrichOp{
			Keys: make([][]byte, 0, 100), // Pre-allocate with reasonable capacity
		}
	},
}

func (eo *enrichOp) reset() {
	// Clear slices but keep underlying capacity
	eo.Keys = eo.Keys[:0]
	eo.ei = nil
	eo.db = nil
}

func (eo *enrichOp) Execute(ctx context.Context) (err error) {
	startTime := time.Now()
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(eo.Keys) == 0 {
		return nil
	}

	// Build DocumentScanState from keys, fetching enrichment data if available
	states := make([]storeutils.DocumentScanState, len(eo.Keys))
	for i, key := range eo.Keys {
		states[i] = storeutils.DocumentScanState{
			CurrentDocKey: key,
		}

		// If this is an enrichment key (e.g., summary), fetch the enrichment data
		// This is needed for pipeline enrichers where summaries feed into embedders
		val, closer, err := eo.db.Get(key)
		if err == nil {
			// Extract hashID and content
			if len(val) >= 8 {
				_, hashID, _ := encoding.DecodeUint64Ascending(val)
				content := string(val[8:])
				states[i].Enrichment = content
				states[i].EnrichmentHashID = hashID
			}
			_ = closer.Close()
		} else if errors.Is(err, pebble.ErrNotFound) {
			// Key not found directly — this may be a raw document key.
			// Documents are stored at key + DBRangeStart (:\x00), so try that.
			docKey := storeutils.KeyRangeStart(key)
			val, closer, err := eo.db.Get(docKey)
			if err == nil {
				if doc, decErr := storeutils.DecodeDocumentJSON(val); decErr == nil {
					states[i].Document = doc
				}
				_ = closer.Close()
			}
		}
	}

	// Call GeneratePrompts - fetches docs, renders prompts, checks hashIDs
	promptGenStart := time.Now()
	keys, prompts, hashIDs, err := eo.ei.generatePrompts(ctx, states)
	if err != nil {
		return fmt.Errorf("generating prompts: %w", err)
	}
	promptGenDuration := time.Since(promptGenStart)

	// If all keys were filtered out by hashID check, nothing to do
	if len(prompts) == 0 {
		eo.ei.logger.Debug("All keys filtered by hashID check, skipping embedding generation")
		return nil
	}

	// Apply rate limiting before generating embeddings
	rateLimitStart := time.Now()
	if err := waitRateLimiter(ctx, eo.ei.rateLimiter, len(prompts)); err != nil {
		return fmt.Errorf("waiting for rate limiter: %w", err)
	}
	rateLimitDuration := time.Since(rateLimitStart)

	// Generate embeddings with per-item fallback on batch failure
	embedStart := time.Now()
	embs, successKeys, successHashIDs, err := eo.ei.embedWithFallback(ctx, keys, prompts, hashIDs)
	if err != nil {
		// All items failed — return retryable error for WAL retry
		return retry.RetryableError(
			fmt.Errorf("generating embeddings during execution: %w", err),
		)
	}
	embedDuration := time.Since(embedStart)
	if len(embs) > 0 {
		eo.ei.embeddingCreationCounter.Add(float64(len(embs)))
	}

	// If nothing succeeded, nothing to persist
	if len(embs) == 0 {
		return nil
	}

	// Persist embeddings (use hashIDs from GeneratePrompts, no recomputation)
	// Note: chunk offset writes (from postPersistHook) are persisted as a separate
	// Raft proposal. Combining into one proposal would require refactoring
	// PersistEmbeddingsFunc to expose its internal write building, which cascades
	// across all index types. The extra proposal is acceptable here because this
	// is an async background path dominated by embedding generation latency.
	persistStart := time.Now()
	if err := eo.ei.persistEmbeddings(ctx, embs, successHashIDs, successKeys); err != nil {
		return fmt.Errorf("persisting embeddings: %w", err)
	}
	if eo.ei.postPersistHook != nil {
		if err := eo.ei.postPersistHook(ctx); err != nil {
			return fmt.Errorf("post-persist hook (chunk offsets): %w", err)
		}
	}
	persistDuration := time.Since(persistStart)

	totalDuration := time.Since(startTime)

	// Log all timings at debug level
	eo.ei.logger.Debug("Enrichment batch execution timings",
		zap.Int("numInputKeys", len(eo.Keys)),
		zap.Int("numFilteredKeys", len(successKeys)),
		zap.Int("numEmbeddings", len(embs)),
		zap.Duration("totalDuration", totalDuration),
		zap.Duration("promptGenDuration", promptGenDuration),
		zap.Duration("rateLimitDuration", rateLimitDuration),
		zap.Duration("embedDuration", embedDuration),
		zap.Duration("persistDuration", persistDuration),
	)

	return nil
}

func (eo *enrichOp) Merge(datum []byte) error {
	other := enrichOpPool.Get().(*enrichOp)
	defer func() {
		other.reset()
		enrichOpPool.Put(other)
	}()
	if err := other.decode(datum); err != nil {
		return fmt.Errorf("decoding enrich operation: %w", err)
	}
	eo.Keys = append(eo.Keys, other.Keys...)
	return nil
}

type generatePromptsFunc func(ctx context.Context, states []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error)

type PersistEmbeddingsFunc = func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error

// TerminalEmbeddingEnricher is the interface for the final embedding stage
// of a PipelineEnricher. Both dense EmbeddingEnricher and sparse
// SparsePipelineAdapter implement this interface.
type TerminalEmbeddingEnricher interface {
	// EnrichBatch enqueues keys for async embedding generation.
	EnrichBatch(keys [][]byte) error

	// GenerateEmbeddingsWithoutPersist generates embeddings synchronously without persisting.
	// Used by ComputeEnrichments() for the sync pre-enrichment path.
	GenerateEmbeddingsWithoutPersist(
		ctx context.Context,
		keys [][]byte,
		documentValues map[string][]byte,
		generatePrompts generatePromptsFunc,
	) (embeddingWrites [][2][]byte, chunkWrites [][2][]byte, failedKeys [][]byte, err error)

	// EnricherStats returns a snapshot of the enricher's backfill and WAL state.
	EnricherStats() EnricherStats

	Close() error
}

var _ TerminalEmbeddingEnricher = (*EmbeddingEnricher)(nil)

// PostPersistHookFunc is called after embeddings are persisted.
// Used by PipelineEnricher to flush ephemeral chunk offset writes.
type PostPersistHookFunc func(ctx context.Context) error

func NewEmbeddingEnricher(
	ctx context.Context,
	logger *zap.Logger,
	antflyConfig *common.Config,
	db *pebble.DB,
	dir string,
	name string,
	toIndexSuffix []byte,
	embSuffix []byte,
	byteRange types.Range,
	config embeddings.EmbedderConfig,
	generatePrompts generatePromptsFunc,
	persistEmbeddingsFunc PersistEmbeddingsFunc,
	postPersistHook PostPersistHookFunc, // Optional: called after embeddings are persisted
) (*EmbeddingEnricher, error) {
	embedder, err := embeddings.NewEmbedder(config)
	if err != nil {
		return nil, fmt.Errorf("creating embedding plugin: %w", err)
	}

	e := &EmbeddingEnricher{
		name:                     name,
		generateEmbeddings:       embedder,
		persistEmbeddings:        persistEmbeddingsFunc,
		embSuffix:                embSuffix,
		generatePrompts:          generatePrompts,
		rateLimiter:              resolveRateLimiter(embedder),
		postPersistHook:          postPersistHook,
		embeddingCreationCounter: embeddingCreationOps.WithLabelValues(name),
	}
	if err := e.init(ctx, logger, db, dir, EnricherWALDir, name); err != nil {
		return nil, err
	}
	e.logger.Debug("Starting enricher", zap.String("dir", e.dir))

	backfillWait := make(chan struct{})
	e.eg.Go(func() error {
		return e.runDequeueLoop(backfillWait, e.dequeueOnce)
	})
	e.eg.Go(func() error {
		// FIXME (ajr) Need a better mechanism of catching problems here (fails silently)
		err := func() (err error) {
			defer pebbleutils.RecoverPebbleClosed(&err)
			defer close(backfillWait)
			e.startBackfill()
			defer e.finishBackfill()
			totalProcessed := 0
			maxBatchSize := enricherMaxBatches * enricherPartitionSize

			err = storeutils.ScanForEnrichment(e.egCtx, db, storeutils.EnrichmentScanOptions{
				ByteRange:        byteRange,
				PrimarySuffix:    toIndexSuffix,
				EnrichmentSuffix: embSuffix,
				BatchSize:        maxBatchSize,
				ProcessBatch: func(ctx context.Context, batch []storeutils.DocumentScanState) error {
					// Generate prompts - in pipeline mode, batch contains summaries in Enrichment field
					docIDs, prompts, hashIDs, err := generatePrompts(ctx, batch)
					if err != nil {
						return fmt.Errorf("generating prompts during backfill: %w", err)
					}

					// Write dud enrichment keys for items that produced no valid prompt.
					// This prevents them from being re-scanned on every backfill.
					if len(docIDs) < len(batch) {
						e.writeDudKeys(batch, docIDs, embSuffix)
					}

					if len(prompts) > 0 {
						// Apply rate limiting before generating embeddings during backfill
						if err := waitRateLimiter(e.egCtx, e.rateLimiter, len(prompts)); err != nil {
							return fmt.Errorf("waiting for rate limiter during backfill: %w", err)
						}

						embs, successKeys, successHashIDs, err := e.embedWithFallback(e.egCtx, docIDs, prompts, hashIDs)
						if err != nil {
							if errors.Is(err, context.Canceled) {
								return err
							}
							e.logger.Warn(
								"Failed to generate batch of embeddings during backfill",
								zap.Error(err),
							)
						} else if len(embs) > 0 {
							e.embeddingCreationCounter.Add(float64(len(embs)))
							if err := e.persistEmbeddings(e.egCtx, embs, successHashIDs, successKeys); err != nil {
								if errors.Is(err, raft.ErrProposalDropped) || errors.Is(err, context.Canceled) {
									select {
									case <-e.egCtx.Done():
										return e.egCtx.Err()
									default:
									}
								}
								e.logger.Warn("Failed to persist embeddings during backfill", zap.Error(err))
							} else if e.postPersistHook != nil {
								if err := e.postPersistHook(e.egCtx); err != nil {
									e.logger.Warn("Failed post-persist hook during backfill", zap.Error(err))
								}
							}
						}
					}

					totalProcessed += len(batch)
					e.addBackfillItems(len(batch))
					if len(batch) > 0 {
						lastKey := batch[len(batch)-1].CurrentDocKey
						progress := estimateProgress(byteRange[0], byteRange[1], lastKey)
						e.updateBackfillProgress(progress)
					}
					e.logger.Debug(
						"Processed batch during backfill",
						zap.Int("batchSize", len(batch)),
						zap.Int("totalProcessed", totalProcessed),
					)
					return nil
				},
			})
			if err != nil {
				return fmt.Errorf("scanning for enrichment: %w", err)
			}

			e.logger.Debug("Rebuild: Finished backfill for enricher",
				zap.Stringer("range", byteRange),
				zap.String("path", e.dir),
				zap.Int("totalProcessed", totalProcessed))
			return nil
		}()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				e.logger.Error("Failed to backfill enricher", zap.String("path", e.dir),
					zap.Error(err))
			}
		}
		return err
	})
	return e, nil
}

// embedWithFallback tries batch embedding first. On failure, falls back to
// one-at-a-time embedding to isolate the failing item(s). Returns successful
// embeddings, their keys, and hashIDs. Returns an error only if ALL items fail.
func (e *EmbeddingEnricher) embedWithFallback(
	ctx context.Context,
	keys [][]byte,
	prompts []string,
	hashIDs []uint64,
) (embs [][]float32, successKeys [][]byte, successHashIDs []uint64, err error) {
	// Parse all prompts to content parts
	contents := make([][]ai.ContentPart, len(prompts))
	for i, t := range prompts {
		parts, parseErr := ai.TextToParts(t)
		if parseErr != nil {
			return nil, nil, nil, fmt.Errorf("parsing prompt %d: %w", i, parseErr)
		}
		contents[i] = parts
	}

	// Try batch embedding
	embs, err = e.generateEmbeddings.Embed(ctx, contents)
	if err == nil {
		return embs, keys, hashIDs, nil
	}

	// Context cancellation is not recoverable
	if errors.Is(err, context.Canceled) {
		return nil, nil, nil, err
	}

	// On rate limit errors, apply backoff and halve the rate limiter
	var rateLimitErr *embeddings.RateLimitError
	if errors.As(err, &rateLimitErr) {
		if e.rateLimiter != nil {
			// Halve the current rate to reduce pressure
			currentRate := e.rateLimiter.Limit()
			newRate := currentRate / 2
			if newRate < 1 {
				newRate = 1
			}
			e.rateLimiter.SetLimit(newRate)
			e.logger.Warn("Rate limited by provider, reducing enricher rate",
				zap.Float64("newRPS", float64(newRate)),
				zap.Duration("retryAfter", rateLimitErr.RetryAfter))
		}
		if rateLimitErr.RetryAfter > 0 {
			select {
			case <-ctx.Done():
				return nil, nil, nil, ctx.Err()
			case <-time.After(rateLimitErr.RetryAfter):
			}
		}
		// Retry once after backoff
		embs, err = e.generateEmbeddings.Embed(ctx, contents)
		if err == nil {
			return embs, keys, hashIDs, nil
		}
		return nil, nil, nil, err
	}

	// Single-item batch: no point retrying the same item individually
	if len(prompts) == 1 {
		return nil, nil, nil, err
	}

	e.logger.Warn("Batch embedding failed, falling back to per-item embedding",
		zap.Error(err),
		zap.Int("batchSize", len(prompts)))

	// Fall back to one-at-a-time
	embs = make([][]float32, 0, len(prompts))
	successKeys = make([][]byte, 0, len(prompts))
	successHashIDs = make([]uint64, 0, len(prompts))

	for i := range prompts {
		single, singleErr := e.generateEmbeddings.Embed(ctx, contents[i:i+1])
		if singleErr != nil {
			if errors.Is(singleErr, context.Canceled) {
				return nil, nil, nil, singleErr
			}
			e.logger.Warn("Per-item embedding failed, skipping item",
				zap.Error(singleErr),
				zap.String("key", types.FormatKey(keys[i])))
			continue
		}
		if len(single) == 1 {
			embs = append(embs, single[0])
			successKeys = append(successKeys, keys[i])
			successHashIDs = append(successHashIDs, hashIDs[i])
		}
	}

	if len(embs) == 0 {
		return nil, nil, nil, fmt.Errorf("all %d items failed during per-item fallback", len(prompts))
	}

	e.logger.Info("Per-item fallback completed",
		zap.Int("succeeded", len(embs)),
		zap.Int("failed", len(prompts)-len(embs)))

	return embs, successKeys, successHashIDs, nil
}

func (e *EmbeddingEnricher) dequeueOnce() (empty bool, err error) {
	ops := enrichOpPool.Get().(*enrichOp)
	defer func() {
		ops.reset()
		enrichOpPool.Put(ops)
	}()
	ops.ei = e
	ops.db = e.db
	if err := e.walBuf.Dequeue(e.egCtx, ops, enricherMaxBatches); err != nil {
		if errors.Is(err, inflight.ErrBufferClosed) || errors.Is(err, context.Canceled) ||
			errors.Is(err, raft.ErrProposalDropped) {
			return false, err
		}
		e.logger.Error("Failed to dequeue from WAL buffer", zap.Error(err))
	}
	return len(ops.Keys) == 0, nil
}

func (e *EmbeddingEnricher) EnrichBatch(keys [][]byte) error {
	return e.enrichBatch(keys)
}

func (e *EmbeddingEnricher) EnricherStats() EnricherStats {
	return e.enricherStats()
}

// GenerateEmbeddingsWithoutPersist generates embeddings synchronously WITHOUT persisting them
// This is used by ComputeEnrichments() to pre-compute enrichments before Raft proposal
// Returns embedding writes in key:i:<name>:e format ready to be included in the batch
// documentValues map provides zstd-compressed document values for document keys (not enrichment keys)
//
// IMPORTANT: This function does NOT call the passed generatePrompts function directly.
// Instead, it extracts prompts directly from documentValues map (for chunks/summaries that are already text).
func (e *EmbeddingEnricher) GenerateEmbeddingsWithoutPersist(
	ctx context.Context,
	keys [][]byte,
	documentValues map[string][]byte,
	generatePrompts generatePromptsFunc,
) (embeddingWrites [][2][]byte, _ [][2][]byte, failedKeys [][]byte, err error) {
	if len(keys) == 0 {
		return nil, nil, nil, nil
	}

	// Build states with enrichment data from documentValues
	// The generatePrompts function will extract prompts from state.Enrichment (for chunks/summaries)
	// or state.Document (for raw documents)
	states := make([]storeutils.DocumentScanState, 0, len(keys))
	for _, key := range keys {
		val, ok := documentValues[string(key)]
		if !ok {
			e.logger.Warn("Key not found in documentValues for pre-enrichment",
				zap.String("key", types.FormatKey(key)))
			failedKeys = append(failedKeys, key)
			continue
		}

		state := storeutils.DocumentScanState{
			CurrentDocKey: key,
		}

		// Try to decode the value as a document (zstd-compressed or plain JSON).
		// This succeeds for raw document values from ComputeEnrichments and
		// populates state.Document for callbacks that use ExtractPrompt.
		// For enrichment values (chunks with [hashID][content] format), this
		// will fail and we fall back to setting state.Enrichment only.
		if doc, decodeErr := storeutils.DecodeDocumentJSON(val); decodeErr == nil {
			state.Document = doc
		}

		// Extract hashID and content from value: [hashID:uint64][content]
		if len(val) >= 8 {
			content := string(val[8:])
			state.Enrichment = content
		}

		states = append(states, state)
	}

	// Call generatePrompts to extract prompts from states
	// This will use PipelineEnricher.GeneratePrompts (which extracts from state.Enrichment)
	// or the wrapper from aknn_v0.go (which extracts from state.Document using ExtractPrompt)
	filteredKeys, prompts, hashIDs, err := generatePrompts(ctx, states)
	if err != nil {
		e.logger.Warn("Failed to generate prompts in pre-enrichment",
			zap.Error(err),
			zap.Int("numStates", len(states)))
		return nil, nil, append(failedKeys, keys...), fmt.Errorf("generating prompts: %w", err)
	}

	// Log filtering details
	if len(prompts) < len(states) {
		e.logger.Warn("Some keys were filtered during prompt generation in pre-enrichment",
			zap.Int("inputStates", len(states)),
			zap.Int("outputPrompts", len(prompts)),
			zap.Int("filtered", len(states)-len(prompts)))
	}

	// If all keys were filtered out, nothing to do
	if len(prompts) == 0 {
		e.logger.Debug("All keys filtered (no valid prompts), no embeddings to generate")
		return nil, nil, failedKeys, nil
	}

	// Apply rate limiting before generating embeddings
	if err := waitRateLimiter(ctx, e.rateLimiter, len(prompts)); err != nil {
		return nil, nil, append(failedKeys, filteredKeys...), fmt.Errorf("waiting for rate limiter: %w", err)
	}

	// Generate embeddings with per-item fallback on batch failure
	embs, successKeys, successHashIDs, err := e.embedWithFallback(ctx, filteredKeys, prompts, hashIDs)
	if err != nil {
		e.logger.Warn("Failed to generate embeddings in pre-enrichment",
			zap.Error(err),
			zap.Int("numPrompts", len(prompts)))
		return nil, nil, append(failedKeys, filteredKeys...), fmt.Errorf("generating embeddings: %w", err)
	}
	// Track failed items from fallback
	if len(successKeys) < len(filteredKeys) {
		successSet := make(map[string]struct{}, len(successKeys))
		for _, k := range successKeys {
			successSet[string(k)] = struct{}{}
		}
		for _, k := range filteredKeys {
			if _, ok := successSet[string(k)]; !ok {
				failedKeys = append(failedKeys, k)
			}
		}
	}

	if len(embs) > 0 {
		e.embeddingCreationCounter.Add(float64(len(embs)))
	}

	// Convert embeddings to writes (WITHOUT persisting via Raft)
	embeddingWrites = make([][2][]byte, 0, len(embs))
	for i, emb := range embs {
		// Encode embedding value with hashID prefix: [hashID:uint64][vector]
		embValue := make([]byte, 0, 8+4*(len(emb)+1))
		embValue, err = vectorindex.EncodeEmbeddingWithHashID(embValue, emb, successHashIDs[i])
		if err != nil {
			e.logger.Warn("Failed to encode embedding, skipping",
				zap.Error(err),
				zap.String("key", types.FormatKey(successKeys[i])))
			failedKeys = append(failedKeys, successKeys[i])
			continue
		}

		embeddingWrites = append(embeddingWrites, [2][]byte{
			append(bytes.Clone(successKeys[i]), e.embSuffix...),
			embValue})
	}

	e.logger.Debug("Generated embeddings for pre-enrichment",
		zap.Int("numKeys", len(keys)),
		zap.Int("numEmbeddings", len(embeddingWrites)),
		zap.Int("failedKeys", len(failedKeys)))

	return embeddingWrites, nil, failedKeys, nil
}
