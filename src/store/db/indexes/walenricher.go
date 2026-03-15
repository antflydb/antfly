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
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/inflight"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	enricherMaxBatches    = 10
	enricherPartitionSize = 10

	// WAL directory names for each enricher type.
	// These must match the walName passed to walEnricherBase.init().
	EnricherWALDir           = "enricherWAL"
	ChunkingEnricherWALDir   = "chunkingEnricherWAL"
	SummarizerEnricherWALDir = "summarizerEnricherWAL"
)

// DefaultFlushTime is the default interval between dequeue timer ticks.
var DefaultFlushTime = time.Second * 30

// CleanupEnricherWALs removes all enricher WAL directories under indexPath.
// Called on leadership gain/loss to discard stale WAL state.
func CleanupEnricherWALs(indexPath string) {
	_ = os.RemoveAll(filepath.Join(indexPath, EnricherWALDir))
	_ = os.RemoveAll(filepath.Join(indexPath, ChunkingEnricherWALDir))
	_ = os.RemoveAll(filepath.Join(indexPath, SummarizerEnricherWALDir))
}

// Shared zstd encoder/decoder for enricher operations.
// EncodeAll and DecodeAll are concurrent-safe on a shared instance.
var (
	enricherZstdEncoder, _ = zstd.NewWriter(nil)
	enricherZstdDecoder, _ = zstd.NewReader(nil)
)

// EnricherStats holds backfill and WAL state for a single enricher.
type EnricherStats struct {
	Backfilling            bool
	BackfillProgress       float64
	BackfillItemsProcessed uint64
	WALBacklog             int
}

// backfillTracker provides atomic backfill progress tracking with optional
// Prometheus metrics. Embedded by walEnricherBase, BleveIndexV2, and GraphIndexV0.
type backfillTracker struct {
	backfilling            atomic.Bool
	backfillProgress       atomic.Value // stores float64
	backfillItemsProcessed atomic.Uint64

	// Optional Prometheus metrics (nil for non-enricher indexes)
	backfillProgressGauge prometheus.Gauge
	backfillItemsCounter  prometheus.Counter
}

// startBackfill marks the backfill as in progress.
func (bt *backfillTracker) startBackfill() {
	bt.backfilling.Store(true)
}

// finishBackfill marks backfilling as complete and sets progress to 1.0.
func (bt *backfillTracker) finishBackfill() {
	bt.backfillProgress.Store(float64(1.0))
	if bt.backfillProgressGauge != nil {
		bt.backfillProgressGauge.Set(1.0)
	}
	bt.backfilling.Store(false)
}

// updateBackfillProgress updates the backfill progress ratio [0.0, 1.0].
func (bt *backfillTracker) updateBackfillProgress(progress float64) {
	bt.backfillProgress.Store(progress)
	if bt.backfillProgressGauge != nil {
		bt.backfillProgressGauge.Set(progress)
	}
}

// addBackfillItems increments the backfill items-processed counter.
func (bt *backfillTracker) addBackfillItems(n int) {
	bt.backfillItemsProcessed.Add(uint64(n)) //nolint:gosec // G115: n is non-negative item count
	if bt.backfillItemsCounter != nil {
		bt.backfillItemsCounter.Add(float64(n))
	}
}

// loadBackfillProgress returns the current backfill progress as a float64.
func (bt *backfillTracker) loadBackfillProgress() float64 {
	if p, ok := bt.backfillProgress.Load().(float64); ok {
		return p
	}
	return 0
}

// walEnricherBase provides shared infrastructure for WAL-backed enrichers.
// Each enricher type (Embedding, Chunking, Summarize) embeds this and provides
// its own Execute/backfill logic.
type walEnricherBase struct {
	sync.Mutex
	logger      *zap.Logger
	walBuf      *inflight.WALBuffer
	db          *pebble.DB
	dir         string
	egCancel    context.CancelFunc
	egCtx       context.Context
	eg          *errgroup.Group
	enqueueChan chan int

	// Cached Prometheus gauge to avoid WithLabelValues map lookup per batch.
	walBacklogGauge prometheus.Gauge

	backfillTracker
}

// init initializes the base fields, WAL buffer, errgroup, and enqueue channel.
func (b *walEnricherBase) init(
	ctx context.Context,
	logger *zap.Logger,
	db *pebble.DB,
	dir, walName, metricsLabel string,
) error {
	b.logger = logger
	b.db = db
	b.dir = dir
	var err error
	b.walBuf, err = inflight.NewWALBufferWithOptions(logger, dir, walName, inflight.WALBufferOptions{
		MaxRetries: 10,
		Metrics:    NewPrometheusWALMetrics(metricsLabel),
	})
	if err != nil {
		return fmt.Errorf("creating WALBuffer: %w", err)
	}
	ctx, b.egCancel = context.WithCancel(ctx)
	b.eg, b.egCtx = errgroup.WithContext(ctx)
	b.enqueueChan = make(chan int, 5)
	b.walBacklogGauge = enricherWALBacklog.WithLabelValues(metricsLabel)
	b.backfillProgressGauge = backfillProgress.WithLabelValues(metricsLabel)
	b.backfillItemsCounter = backfillItemsProcessed.WithLabelValues(metricsLabel)
	// Reset progress gauge so a new leadership tenure starts at 0, not stale 1.0.
	b.backfillProgressGauge.Set(0.0)
	return nil
}

// Close cancels the context, closes the WAL buffer, and waits for goroutines.
// The lock is released before eg.Wait() to avoid holding the mutex while
// blocking on goroutine completion (which could deadlock if any goroutine
// ever needs the mutex).
func (b *walEnricherBase) Close() error {
	b.Lock()
	b.egCancel()
	walErr := b.walBuf.Close()
	b.Unlock()
	egErr := b.eg.Wait()
	if walErr != nil {
		return fmt.Errorf("closing buffer: %w", walErr)
	}
	return egErr
}

// encodeEnricherKeys encodes a batch of keys for WAL storage.
// All enricher op types share the same {"keys": [...]} JSON format.
func encodeEnricherKeys(keys [][]byte) ([]byte, error) {
	type keysPayload struct {
		Keys [][]byte `json:"keys"`
	}
	data, err := json.Marshal(keysPayload{Keys: keys})
	if err != nil {
		return nil, fmt.Errorf("encoding json: %w", err)
	}
	return enricherZstdEncoder.EncodeAll(data, nil), nil
}

// enrichBatch partitions keys, encodes each partition, enqueues to WAL,
// and signals the dequeue goroutine.
func (b *walEnricherBase) enrichBatch(keys [][]byte) error {
	b.logger.Debug("Enriching batch", zap.Int("numKeys", len(keys)))
	partitionedBatch := make([][]byte, 0, len(keys)/enricherPartitionSize+1)
	counts := make([]uint32, 0, cap(partitionedBatch))
	for i := 0; i < len(keys); i += enricherPartitionSize {
		end := min(i+enricherPartitionSize, len(keys))
		encoded, err := encodeEnricherKeys(keys[i:end])
		if err != nil {
			return fmt.Errorf("marshaling enrich operation: %w", err)
		}
		partitionedBatch = append(partitionedBatch, encoded)
		counts = append(counts, uint32(end-i)) //nolint:gosec // G115: partition size bounded by enricherPartitionSize
	}
	if err := b.walBuf.EnqueueBatch(partitionedBatch, counts); err != nil {
		return fmt.Errorf("enqueueing enrich operation batch: %w", err)
	}
	select {
	case b.enqueueChan <- len(partitionedBatch):
	default:
	}
	return nil
}

// runDequeueLoop waits for backfillWait, then processes enqueue signals and
// timer ticks. dequeueOnce returns (empty, err) where empty controls timer
// reset and err is a fatal error that terminates the loop.
func (b *walEnricherBase) runDequeueLoop(
	backfillWait <-chan struct{},
	dequeueOnce func() (empty bool, err error),
) error {
	select {
	case <-b.egCtx.Done():
		return nil
	case <-backfillWait:
	}
	const maxJitter = time.Millisecond * 200
	jitter := maxJitter - rand.N(maxJitter) //nolint:gosec // G404: non-security randomness for ML/jitter
	t := time.NewTimer(DefaultFlushTime + jitter)
	enqueueCounter := 0
	resetTimer := func(d time.Duration) {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(d)
	}
	dequeue := func() error {
		enqueueCounter = 0
		empty, err := dequeueOnce()
		b.walBacklogGauge.Set(float64(b.walBuf.Len()))
		if err != nil {
			return err
		}
		if empty {
			resetTimer(DefaultFlushTime + jitter)
		} else {
			resetTimer(100*time.Millisecond + jitter)
		}
		return nil
	}
	defer t.Stop()
	for {
		select {
		case <-b.egCtx.Done():
			return nil
		case n := <-b.enqueueChan:
			enqueueCounter += n
			if enqueueCounter < 1 {
				continue
			}
			if err := dequeue(); err != nil {
				return err
			}
		case <-t.C:
			if err := dequeue(); err != nil {
				return err
			}
		}
	}
}

// writeDudKeys writes dud enrichment markers for batch items that were not included
// in the successful docIDs list (i.e., items that produced no valid prompt).
// This prevents them from being re-scanned on every backfill.
//
// Design: These are written directly to Pebble (bypassing Raft) because they are
// local index metadata, not replicated data. After a leader change, the new leader
// will re-scan and re-discover unenrichable items during backfill, which is acceptable
// since dud marking is an optimization to avoid repeated failed enrichment attempts
// during a single leadership tenure.
func (b *walEnricherBase) writeDudKeys(batch []storeutils.DocumentScanState, docIDs [][]byte, enrichmentSuffix []byte) {
	successSet := make(map[string]struct{}, len(docIDs))
	for _, id := range docIDs {
		successSet[string(id)] = struct{}{}
	}

	var failedKeys [][]byte
	for _, state := range batch {
		if _, ok := successSet[string(state.CurrentDocKey)]; !ok {
			failedKeys = append(failedKeys, state.CurrentDocKey)
		}
	}

	writeDudKeysToDb(b.db, b.logger, failedKeys, enrichmentSuffix)
}

// writeDudKeysToDb writes dud enrichment markers for the given keys directly to Pebble.
// This is a standalone version of writeDudKeys for use outside the walEnricherBase context
// (e.g., from GeneratePrompts when permanent failures are detected).
func writeDudKeysToDb(db *pebble.DB, logger *zap.Logger, keys [][]byte, enrichmentSuffix []byte) {
	if len(keys) == 0 {
		return
	}
	batch := db.NewBatch()
	for _, key := range keys {
		dudKey := append(bytes.Clone(key), enrichmentSuffix...)
		if err := batch.Set(dudKey, storeutils.DudEnrichmentValue, nil); err != nil {
			logger.Warn("Failed to write dud enrichment key", zap.Error(err))
			_ = batch.Close()
			return
		}
	}
	if err := batch.Commit(nil); err != nil {
		logger.Warn("Failed to commit dud enrichment keys", zap.Error(err))
		_ = batch.Close()
	} else {
		logger.Debug("Wrote dud enrichment keys for permanently failed items",
			zap.Int("count", len(keys)))
	}
}

// enricherStats returns a snapshot of the enricher's backfill and WAL state.
func (b *walEnricherBase) enricherStats() EnricherStats {
	return EnricherStats{
		Backfilling:            b.backfilling.Load(),
		BackfillProgress:       b.loadBackfillProgress(),
		BackfillItemsProcessed: b.backfillItemsProcessed.Load(),
		WALBacklog:             b.walBuf.Len(),
	}
}
