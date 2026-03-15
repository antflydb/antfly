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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

type ChunkingEnricher struct {
	walEnricherBase

	indexName string // Index name for key suffix

	// Chunking logic
	chunkingHelper  *chunking.ChunkingHelper
	persistChunks   PersistChunksFunc
	config          *chunking.ChunkerConfig // Store config to determine suffix
	generatePrompts generatePromptsFunc     // Function to extract text from documents using index config
}

type chunkingEnrichOp struct {
	Keys [][]byte `json:"keys"`

	e  *ChunkingEnricher `json:"-"`
	db *pebble.DB        `json:"-"`
}

func (eo *chunkingEnrichOp) decode(data []byte) error {
	dec, err := enricherZstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(dec, eo)
}

var chunkingEnrichOpPool = sync.Pool{
	New: func() any {
		return &chunkingEnrichOp{
			Keys: make([][]byte, 0, 100),
		}
	},
}

func (eo *chunkingEnrichOp) reset() {
	eo.Keys = eo.Keys[:0]
	eo.e = nil
	eo.db = nil
}

func (eo *chunkingEnrichOp) Execute(ctx context.Context) (err error) {
	startTime := time.Now()
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(eo.Keys) == 0 {
		return nil
	}

	// Build DocumentScanState objects for all keys
	states := make([]storeutils.DocumentScanState, 0, len(eo.Keys))
	for _, key := range eo.Keys {
		// Get document from Pebble
		opts := storeutils.QueryOptions{
			SkipDocument:  false,
			AllEmbeddings: false,
			AllSummaries:  false,
		}
		doc, err := storeutils.GetDocument(ctx, eo.db, key, opts)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				eo.e.logger.Debug("Document not found during chunking, skipping",
					zap.String("key", types.FormatKey(key)))
				continue
			}
			eo.e.logger.Warn("Failed to get document for chunking",
				zap.String("key", types.FormatKey(key)),
				zap.Error(err))
			continue
		}

		states = append(states, storeutils.DocumentScanState{
			CurrentDocKey: key,
			Document:      doc.Document,
		})
	}

	if len(states) == 0 {
		eo.e.logger.Debug("No documents to chunk (all keys not found)")
		return nil
	}

	// Use generatePrompts to extract text from documents using index config
	docKeys, textBatch, hashIDs, err := eo.e.generatePrompts(ctx, states)
	if err != nil {
		return fmt.Errorf("generating prompts for chunking: %w", err)
	}

	if len(textBatch) == 0 {
		eo.e.logger.Debug("All keys filtered (no text or hash match), skipping chunking")
		return nil
	}

	// Chunk documents (local operation, no rate limiting needed)
	chunkStart := time.Now()
	allChunks := make([][]chunking.Chunk, 0, len(textBatch))
	for _, text := range textBatch {
		chunks, err := eo.e.chunkingHelper.ChunkDocument(text)
		if err != nil {
			if strings.Contains(err.Error(), "reading file") {
				eo.e.logger.Error("Non-retryable error chunking document", zap.Error(err))
				return nil
			}
			return fmt.Errorf("chunking document: %w", err)
		}
		// storeutils.Chunk is just a type alias to libChunking.Chunk, so no conversion needed
		allChunks = append(allChunks, chunks)
	}
	chunkDuration := time.Since(chunkStart)

	// Persist chunks
	persistStart := time.Now()
	if err := eo.e.persistChunks(ctx, docKeys, hashIDs, allChunks); err != nil {
		return fmt.Errorf("persisting chunks: %w", err)
	}
	persistDuration := time.Since(persistStart)

	totalDuration := time.Since(startTime)

	// Log timings
	eo.e.logger.Debug("Chunking batch execution timings",
		zap.Int("numInputKeys", len(eo.Keys)),
		zap.Int("numChunkedDocs", len(docKeys)),
		zap.Duration("totalDuration", totalDuration),
		zap.Duration("chunkDuration", chunkDuration),
		zap.Duration("persistDuration", persistDuration),
	)

	return nil
}

func (eo *chunkingEnrichOp) Merge(datum []byte) error {
	other := chunkingEnrichOpPool.Get().(*chunkingEnrichOp)
	defer func() {
		other.reset()
		chunkingEnrichOpPool.Put(other)
	}()
	if err := other.decode(datum); err != nil {
		return fmt.Errorf("decoding enrich operation: %w", err)
	}
	eo.Keys = append(eo.Keys, other.Keys...)
	return nil
}

type PersistChunksFunc = func(ctx context.Context, keys [][]byte, hashIDs []uint64, chunks [][]chunking.Chunk) error

func NewChunkingEnricher(
	ctx context.Context,
	logger *zap.Logger,
	db *pebble.DB,
	dir, indexName string,
	byteRange types.Range,
	config chunking.ChunkerConfig,
	persistChunksFunc PersistChunksFunc,
	generatePrompts generatePromptsFunc,
) (*ChunkingEnricher, error) {
	chunkingHelper, err := chunking.NewChunkingHelper(indexName+"-chunker", config, logger)
	if err != nil {
		return nil, fmt.Errorf("creating chunking helper: %w", err)
	}

	e := &ChunkingEnricher{
		indexName:       indexName,
		chunkingHelper:  chunkingHelper,
		persistChunks:   persistChunksFunc,
		config:          &config,
		generatePrompts: generatePrompts,
	}

	if err := e.init(ctx, logger, db, dir, ChunkingEnricherWALDir, indexName); err != nil {
		return nil, err
	}

	e.logger.Debug("Starting chunking enricher",
		zap.String("dir", e.dir),
		zap.String("indexName", indexName),
	)

	backfillWait := make(chan struct{})

	// Dequeue goroutine
	e.eg.Go(func() error {
		return e.runDequeueLoop(backfillWait, e.dequeueOnce)
	})

	// Backfill goroutine
	e.eg.Go(func() error {
		err := func() error {
			defer close(backfillWait)
			e.startBackfill()
			defer e.finishBackfill()
			totalProcessed := 0
			maxBatchSize := enricherMaxBatches * enricherPartitionSize

			// Scan for documents without chunks - use correct suffix based on config
			var enrichmentSuffix []byte
			if chunking.GetFullTextIndex(config) != nil {
				enrichmentSuffix = []byte(":i:" + indexName + ":0:cft") // Check for first full-text chunk
			} else {
				enrichmentSuffix = []byte(":i:" + indexName + ":0:c") // Check for first vector chunk
			}
			err := storeutils.ScanForEnrichment(e.egCtx, db, storeutils.EnrichmentScanOptions{
				ByteRange:        byteRange,
				PrimarySuffix:    storeutils.DBRangeStart,
				EnrichmentSuffix: enrichmentSuffix,
				BatchSize:        maxBatchSize,
				ProcessBatch: func(ctx context.Context, batch []storeutils.DocumentScanState) error {
					// Extract keys for chunking
					keys := make([][]byte, 0, len(batch))
					for _, state := range batch {
						keys = append(keys, state.CurrentDocKey)
					}

					// Create chunking operation
					ops := &chunkingEnrichOp{
						Keys: keys,
						e:    e,
						db:   e.db,
					}

					// Execute chunking directly during backfill
					if err := ops.Execute(ctx); err != nil {
						e.logger.Warn("Failed to chunk batch during backfill", zap.Error(err))
					}

					totalProcessed += len(batch)
					e.addBackfillItems(len(batch))
					if len(batch) > 0 {
						lastKey := batch[len(batch)-1].CurrentDocKey
						e.updateBackfillProgress(estimateProgress(byteRange[0], byteRange[1], lastKey))
					}
					e.logger.Debug("Processed batch during backfill",
						zap.Int("batchSize", len(batch)),
						zap.Int("totalProcessed", totalProcessed))
					return nil
				},
			})
			if err != nil {
				return fmt.Errorf("scanning for enrichment: %w", err)
			}

			e.logger.Debug("Rebuild: Finished backfill for chunking enricher",
				zap.Stringer("range", byteRange),
				zap.String("path", e.dir),
				zap.Int("totalProcessed", totalProcessed))
			return nil
		}()
		if err != nil {
			e.logger.Error("Failed to backfill chunking enricher",
				zap.String("path", e.dir),
				zap.Error(err))
		}
		return err
	})

	return e, nil
}

func (e *ChunkingEnricher) dequeueOnce() (empty bool, err error) {
	ops := chunkingEnrichOpPool.Get().(*chunkingEnrichOp)
	defer func() {
		ops.reset()
		chunkingEnrichOpPool.Put(ops)
	}()
	ops.e = e
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

func (e *ChunkingEnricher) EnrichBatch(keys [][]byte) error {
	return e.enrichBatch(keys)
}

// GenerateChunksWithoutPersist generates chunks synchronously WITHOUT persisting them via Raft
// This is used by PipelineEnricher.GenerateEmbeddingsWithoutPersist() for pre-enrichment
// Returns chunk writes in key:i:<name>:<chunkID>:c|ctf format ready to be included in the batch
// Accepts writes (key-value pairs) and decodes documents from zstd-compressed JSON values
func (e *ChunkingEnricher) GenerateChunksWithoutPersist(
	ctx context.Context,
	writes [][2][]byte,
	generatePrompts generatePromptsFunc,
) (chunkWrites [][2][]byte, failedKeys [][]byte, err error) {
	if len(writes) == 0 {
		return nil, nil, nil
	}

	// Build DocumentScanState objects from writes
	states := make([]storeutils.DocumentScanState, 0, len(writes))
	for _, write := range writes {
		key := write[0]
		value := write[1]

		// Decode document value - handle both zstd-compressed and uncompressed JSON
		doc, err := storeutils.DecodeDocumentJSON(value)
		if err != nil {
			e.logger.Warn("Failed to decode document JSON in chunking pre-enrichment",
				zap.String("key", types.FormatKey(key)),
				zap.Error(err))
			failedKeys = append(failedKeys, key)
			continue
		}

		states = append(states, storeutils.DocumentScanState{
			CurrentDocKey: key,
			Document:      doc,
		})
	}

	if len(states) == 0 {
		e.logger.Debug("All documents failed to decode in chunking pre-enrichment")
		return nil, failedKeys, nil
	}

	// Use generatePrompts to extract text from documents using index config
	docKeys, textBatch, hashIDs, err := generatePrompts(ctx, states)
	if err != nil {
		e.logger.Warn("Failed to generate prompts in chunking pre-enrichment",
			zap.Error(err),
			zap.Int("numStates", len(states)))
		// Mark all states as failed
		for _, state := range states {
			failedKeys = append(failedKeys, state.CurrentDocKey)
		}
		return nil, failedKeys, fmt.Errorf("generating prompts: %w", err)
	}

	if len(textBatch) == 0 {
		e.logger.Debug("All keys filtered (no text or hash match), skipping chunking")
		return nil, failedKeys, nil
	}

	// Chunk documents (local operation, no rate limiting needed)
	// Track successful entries to maintain alignment between keys, hashIDs, and chunks.
	type chunkSuccess struct {
		key    []byte
		hashID uint64
		chunks []chunking.Chunk
	}
	successes := make([]chunkSuccess, 0, len(textBatch))
	for i, text := range textBatch {
		chunks, err := e.chunkingHelper.ChunkDocument(text)
		if err != nil {
			e.logger.Warn("Failed to chunk document in pre-enrichment",
				zap.String("key", types.FormatKey(docKeys[i])),
				zap.Error(err))
			failedKeys = append(failedKeys, docKeys[i])
			continue
		}
		successes = append(successes, chunkSuccess{key: docKeys[i], hashID: hashIDs[i], chunks: chunks})
	}

	// Prepare chunk writes to return (instead of persisting via Raft)
	chunkWrites = make([][2][]byte, 0, len(successes)*4) // Estimate 4 chunks per doc

	for _, s := range successes {
		if len(s.chunks) == 0 {
			continue
		}

		for _, chunk := range s.chunks {
			// Skip chunks with empty text (no point storing/indexing them)
			if len(strings.TrimSpace(chunk.GetText())) == 0 {
				e.logger.Debug("Skipping chunk with empty text",
					zap.String("key", types.FormatKey(s.key)),
					zap.Uint32("chunkID", chunk.Id))
				continue
			}

			var chunkKey []byte
			if chunking.GetFullTextIndex(*e.config) != nil {
				chunkKey = storeutils.MakeChunkFullTextKey(s.key, e.indexName, chunk.Id)
			} else {
				chunkKey = storeutils.MakeChunkKey(s.key, e.indexName, chunk.Id)
			}

			// Marshal chunk JSON
			chunkJSON, err := json.Marshal(chunk)
			if err != nil {
				e.logger.Warn("Failed to marshal chunk, skipping",
					zap.Error(err),
					zap.String("key", types.FormatKey(s.key)),
					zap.Uint32("chunkID", chunk.Id))
				continue
			}

			// Encode: [hashID:uint64][chunkJSON]
			chunkValue := make([]byte, 0, 8+len(chunkJSON))
			chunkValue = encoding.EncodeUint64Ascending(chunkValue, s.hashID)
			chunkValue = append(chunkValue, chunkJSON...)

			chunkWrites = append(chunkWrites, [2][]byte{chunkKey, chunkValue})
		}
	}

	e.logger.Debug("Generated chunks for pre-enrichment",
		zap.Int("numDocs", len(docKeys)),
		zap.Int("numChunks", len(chunkWrites)),
		zap.Int("failedKeys", len(failedKeys)))

	return chunkWrites, failedKeys, nil
}
