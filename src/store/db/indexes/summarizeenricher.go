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

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/sethvargo/go-retry"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

type SummarizeEnricher struct {
	walEnricherBase

	summarizerSuffix []byte
	embedderSuffix   []byte

	// Config
	generateSummaries ai.DocumentSummarizer
	persistSummaries  PersistSummariesFunc
	generatePrompts   generatePromptsFunc
}

type summarizerEnrichOp struct {
	Keys [][]byte `json:"keys"`

	e  *SummarizeEnricher `json:"-"`
	db *pebble.DB         `json:"-"`

	generatePrompts generatePromptsFunc `json:"-"`
}

func (eo *summarizerEnrichOp) decode(data []byte) error {
	dec, err := enricherZstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(dec, eo)
}

var summarizerEnrichOpPool = sync.Pool{
	New: func() any {
		return &summarizerEnrichOp{
			Keys: make([][]byte, 0, 100), // Pre-allocate with reasonable capacity
		}
	},
}

func (eo *summarizerEnrichOp) reset() {
	// Clear slices but keep underlying capacity
	eo.Keys = eo.Keys[:0]
	eo.e = nil
	eo.db = nil
	eo.generatePrompts = nil
}

func (eo *summarizerEnrichOp) Execute(ctx context.Context) (err error) {
	startTime := time.Now()
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(eo.Keys) == 0 {
		return nil
	}

	// Build DocumentScanState from keys (documents will be fetched by GeneratePrompts)
	states := make([]storeutils.DocumentScanState, len(eo.Keys))
	for i, key := range eo.Keys {
		states[i] = storeutils.DocumentScanState{
			CurrentDocKey: key,
		}
	}

	// Call GeneratePrompts - fetches docs, renders prompts, checks hashIDs
	promptGenStart := time.Now()
	keys, prompts, hashIDs, err := eo.generatePrompts(ctx, states)
	if err != nil {
		return fmt.Errorf("generating prompts: %w", err)
	}
	promptGenDuration := time.Since(promptGenStart)

	// If all keys were filtered out by hashID check, nothing to do
	if len(prompts) == 0 {
		eo.e.logger.Debug("All keys filtered by hashID check, skipping summary generation")
		return nil
	}

	// Generate summaries
	summarizeStart := time.Now()
	summaries, err := eo.e.generateSummaries.SummarizeRenderedDocs(ctx, prompts)
	if err != nil {
		if strings.Contains(err.Error(), "reading file") {
			// This is likely a non-retryable error
			eo.e.logger.Error("Non-retryable error generating summaries batch", zap.Error(err))
			return nil
		}
		return fmt.Errorf("generating summaries batch: %w", err)
	}
	summarizeDuration := time.Since(summarizeStart)

	// Persist summaries (use hashIDs from GeneratePrompts, no recomputation)
	persistStart := time.Now()
	if err := eo.e.persistSummaries(ctx, keys, hashIDs, summaries); err != nil {
		return fmt.Errorf("persisting summaries: %w", err)
	}
	persistDuration := time.Since(persistStart)

	totalDuration := time.Since(startTime)

	// Log all timings at debug level
	eo.e.logger.Debug("Summarizer batch execution timings",
		zap.Int("numInputKeys", len(eo.Keys)),
		zap.Int("numFilteredKeys", len(keys)),
		zap.Int("numSummaries", len(summaries)),
		zap.Duration("totalDuration", totalDuration),
		zap.Duration("promptGenDuration", promptGenDuration),
		zap.Duration("summarizeDuration", summarizeDuration),
		zap.Duration("persistDuration", persistDuration),
	)

	return nil
}

func (eo *summarizerEnrichOp) Merge(datum []byte) error {
	other := summarizerEnrichOpPool.Get().(*summarizerEnrichOp)
	defer func() {
		other.reset()
		summarizerEnrichOpPool.Put(other)
	}()
	if err := other.decode(datum); err != nil {
		return fmt.Errorf("decoding enrich operation: %w", err)
	}
	eo.Keys = append(eo.Keys, other.Keys...)
	return nil
}

type PersistSummariesFunc = func(ctx context.Context, keys [][]byte, hashIDs []uint64, summaries []string) error

func NewSummarizeEnricher(
	ctx context.Context,
	logger *zap.Logger,
	db *pebble.DB,
	dir, name string,
	embSuffix, sumSuffix []byte,
	byteRange types.Range,
	config ai.GeneratorConfig,
	generatePrompts generatePromptsFunc,
	persistSummariesFunc PersistSummariesFunc,
) (*SummarizeEnricher, error) {
	summarizer, err := ai.NewDocumentSummarizer(config)
	if err != nil {
		return nil, fmt.Errorf("creating document summarizer: %w", err)
	}
	e := &SummarizeEnricher{
		embedderSuffix:    embSuffix,
		summarizerSuffix:  sumSuffix,
		generateSummaries: summarizer,
		persistSummaries:  persistSummariesFunc,
		generatePrompts:   generatePrompts,
	}

	if err := e.init(ctx, logger, db, dir, SummarizerEnricherWALDir, name); err != nil {
		return nil, err
	}

	e.logger.Debug("Starting enricher",
		zap.String("dir", e.dir),
		zap.ByteString("embedderSuffix", embSuffix),
		zap.ByteString("summarizerSuffix", sumSuffix),
	)

	backfillWait := make(chan struct{})

	// Dequeue goroutine
	e.eg.Go(func() error {
		return e.runDequeueLoop(backfillWait, e.dequeueOnce)
	})

	// Backfill goroutine
	e.eg.Go(func() error {
		// FIXME (ajr) Need a better mechanism of catching problems here (fails silently)
		err := func() error {
			defer close(backfillWait)
			e.startBackfill()
			defer e.finishBackfill()
			totalProcessed := 0
			maxBatchSize := enricherMaxBatches * enricherPartitionSize

			err := storeutils.ScanForEnrichment(e.egCtx, db, storeutils.EnrichmentScanOptions{
				ByteRange:        byteRange,
				PrimarySuffix:    storeutils.DBRangeStart,
				EnrichmentSuffix: sumSuffix,
				BatchSize:        maxBatchSize,
				ProcessBatch: func(ctx context.Context, batch []storeutils.DocumentScanState) error {
					// Generate prompts from the documents we already have
					docIDs, prompts, hashIDs, err := generatePrompts(ctx, batch)
					if err != nil {
						return fmt.Errorf("generating prompts during backfill: %w", err)
					}

					// Write dud enrichment keys for items that produced no valid prompt.
					// This prevents them from being re-scanned on every backfill.
					if len(docIDs) < len(batch) {
						e.writeDudKeys(batch, docIDs, sumSuffix)
					}

					if len(prompts) > 0 {
						b := retry.NewConstant(time.Minute)
						b = retry.WithMaxRetries(10, b)
						b = retry.WithJitter(time.Second, b)
						var summaries []string
						err = retry.Do(e.egCtx, b,
							func(ctx context.Context) (err error) {
								summaries, err = e.generateSummaries.SummarizeRenderedDocs(
									ctx,
									prompts,
								)
								if err != nil {
									logger.Error("generating summaries during backfill",
										zap.Any("prompts", prompts),
										zap.Error(err))
									return retry.RetryableError(
										fmt.Errorf("generating summaries during backfill: %w", err),
									)
								}
								return nil
							})
						if err != nil {
							// TODO (ajr) How should we handle this? Should we restart the backfill after a failure like this?
							e.logger.Warn(
								"Failed to generate batch of summaries during backfill",
								zap.Error(err),
							)
						} else {
							// Use hashIDs from generatePrompts (already computed)
							if err := e.persistSummaries(ctx, docIDs, hashIDs, summaries); err != nil {
								e.logger.Warn("Failed to persist summaries during backfill", zap.Error(err))
							}
						}
					}

					totalProcessed += len(batch)
					e.addBackfillItems(len(batch))
					if len(batch) > 0 {
						lastKey := batch[len(batch)-1].CurrentDocKey
						e.updateBackfillProgress(estimateProgress(byteRange[0], byteRange[1], lastKey))
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
			e.logger.Error("Failed to backfill enricher", zap.String("path", e.dir),
				zap.Error(err))
		}
		return err
	})
	return e, nil
}

func (e *SummarizeEnricher) dequeueOnce() (empty bool, err error) {
	ops := summarizerEnrichOpPool.Get().(*summarizerEnrichOp)
	defer func() {
		ops.reset()
		summarizerEnrichOpPool.Put(ops)
	}()
	ops.e = e
	ops.db = e.db
	ops.generatePrompts = e.generatePrompts
	if err := e.walBuf.Dequeue(e.egCtx, ops, enricherMaxBatches); err != nil {
		if errors.Is(err, inflight.ErrBufferClosed) || errors.Is(err, context.Canceled) ||
			errors.Is(err, raft.ErrProposalDropped) {
			return false, err
		}
		e.logger.Error("Failed to dequeue from WAL buffer", zap.Error(err))
	}
	return len(ops.Keys) == 0, nil
}

func (e *SummarizeEnricher) EnrichBatch(keys [][]byte) error {
	return e.enrichBatch(keys)
}
