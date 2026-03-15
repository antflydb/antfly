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

package inflight

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/tidwall/wal"
	"go.uber.org/zap"
)

const itemCountHeaderSize = 4 // uint32 little-endian prefix on each WAL entry

var batchPool = sync.Pool{
	New: func() any {
		return &wal.Batch{}
	},
}

// prependItemCount prepends a 4-byte little-endian item count header to data.
func prependItemCount(count uint32, data []byte) []byte {
	out := make([]byte, itemCountHeaderSize, itemCountHeaderSize+len(data))
	binary.LittleEndian.PutUint32(out, count)
	return append(out, data...)
}

// stripItemCount removes the 4-byte item count header and returns the payload.
// Returns the original data unchanged if it's too short to have a header.
func stripItemCount(data []byte) []byte {
	if len(data) <= itemCountHeaderSize {
		return data
	}
	return data[itemCountHeaderSize:]
}

// readItemCount extracts the item count from a header (from ReadHeader/ReadManyHeaders).
func readItemCount(header []byte) uint32 {
	if len(header) < itemCountHeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint32(header)
}

// WALBufferMetrics allows pluggable metrics implementations.
// Implement this interface to integrate with Prometheus, StatsD, or other metrics systems.
type WALBufferMetrics interface {
	// IncDequeueAttempts increments the count of dequeue attempts.
	IncDequeueAttempts(count int)
	// IncDequeueFailure increments the count of dequeue failures by error type.
	IncDequeueFailure(errorType string)
	// IncItemsDiscarded increments the count of items discarded after max retries.
	IncItemsDiscarded(count int)
	// SetPendingRetries sets the current number of items pending retry.
	SetPendingRetries(count int)
}

// NoOpMetrics is the default metrics implementation that does nothing.
type NoOpMetrics struct{}

func (NoOpMetrics) IncDequeueAttempts(int)   {}
func (NoOpMetrics) IncDequeueFailure(string) {}
func (NoOpMetrics) IncItemsDiscarded(int)    {}
func (NoOpMetrics) SetPendingRetries(int)    {}

// DiscardHandler is called when an item is discarded after exceeding max retries.
// The handler receives the WAL index, the raw item data, and the number of attempts made.
type DiscardHandler func(index uint64, data []byte, attempts int)

// WALBufferOptions configures optional WALBuffer behavior.
type WALBufferOptions struct {
	// MaxRetries is the maximum number of dequeue attempts before an item is discarded.
	// 0 means infinite retries (default, backwards compatible).
	MaxRetries int
	// Metrics is the metrics implementation to use. nil means no-op metrics.
	Metrics WALBufferMetrics
	// OnDiscard is called when an item is discarded after exceeding MaxRetries.
	// nil means items are discarded silently.
	OnDiscard DiscardHandler
}

type WALBuffer struct {
	sync.RWMutex
	log    *wal.Log
	closed bool
	logger *zap.Logger

	// Item count tracking — total logical items across all WAL entries.
	// Updated atomically on enqueue/dequeue. Recovered from headers on Open.
	itemCount atomic.Int64

	// Retry tracking
	attemptsMu sync.Mutex
	attempts   map[uint64]int // WAL index -> attempt count
	maxRetries int

	// Metrics and callbacks
	metrics   WALBufferMetrics
	onDiscard DiscardHandler
}

// Len returns the total number of logical items across all WAL entries.
// This is maintained as an in-memory counter that is recovered from entry
// headers on startup.
func (b *WALBuffer) Len() int {
	v := b.itemCount.Load()
	if v < 0 {
		return 0
	}
	return int(v)
}

// EntryCount returns the number of WAL entries (not logical items).
func (b *WALBuffer) EntryCount() (int, error) {
	b.RLock()
	defer b.RUnlock()
	if b.closed {
		return 0, ErrBufferClosed
	}
	fi, err := b.log.FirstIndex()
	if err != nil {
		return 0, err
	}
	li, err := b.log.LastIndex()
	if err != nil {
		return 0, err
	}
	if fi == 0 && li == 0 {
		return 0, nil
	}
	if fi > li {
		return 0, nil
	}
	return int(li - fi + 1), nil //nolint:gosec // G115: WAL index difference fits in int
}

// Close marks the buffer as closed, preventing further operations.
func (b *WALBuffer) Close() error {
	b.Lock()
	defer b.Unlock()
	if b.closed {
		return ErrBufferClosed
	}
	b.closed = true
	return b.log.Close()
}

// Sync flushes the WAL to disk to ensure durability.
func (b *WALBuffer) Sync() error {
	return b.log.Sync()
}

// NewWALBuffer creates a new WALBuffer with default options (infinite retries, no metrics).
func NewWALBuffer(zl *zap.Logger, dir, id string) (*WALBuffer, error) {
	return NewWALBufferWithOptions(zl, dir, id, WALBufferOptions{})
}

// NewWALBufferWithOptions creates a new WALBuffer with the specified options.
func NewWALBufferWithOptions(zl *zap.Logger, dir, id string, opts WALBufferOptions) (*WALBuffer, error) {
	log, err := wal.Open(filepath.Join(dir, id), &wal.Options{
		AllowEmpty: true,
		NoCopy:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("openning wal: %w", err)
	}
	if zl == nil {
		zl = zap.NewNop()
	}

	metrics := opts.Metrics
	if metrics == nil {
		metrics = NoOpMetrics{}
	}

	wb := &WALBuffer{
		logger:     zl,
		log:        log,
		attempts:   make(map[uint64]int),
		maxRetries: opts.MaxRetries,
		metrics:    metrics,
		onDiscard:  opts.OnDiscard,
	}

	// Recover item count from entry headers
	if err := wb.recoverItemCount(); err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("recovering item count: %w", err)
	}

	return wb, nil
}

// recoverItemCount scans all WAL entry headers to reconstruct the in-memory
// item counter. Called once on startup.
func (b *WALBuffer) recoverItemCount() error {
	fi, err := b.log.FirstIndex()
	if err != nil {
		return err
	}
	li, err := b.log.LastIndex()
	if err != nil {
		return err
	}
	if fi == 0 && li == 0 {
		return nil
	}
	if fi > li {
		return nil
	}

	count := int(li - fi + 1) //nolint:gosec // G115: WAL index difference fits in int
	headers, err := b.log.ReadManyHeaders(fi, count, itemCountHeaderSize)
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	var total int64
	for _, h := range headers {
		total += int64(readItemCount(h))
	}
	b.itemCount.Store(total)
	b.logger.Debug("recovered WAL item count",
		zap.Int64("totalItems", total),
		zap.Int("entries", len(headers)))
	return nil
}

// EnqueueBatch enqueues multiple entries, each with its own item count.
// counts[i] is the number of logical items in data[i].
func (b *WALBuffer) EnqueueBatch(data [][]byte, counts []uint32) (err error) {
	b.Lock()
	defer b.Unlock()
	if b.closed {
		return ErrBufferClosed
	}
	idx, err := b.log.LastIndex()
	if err != nil {
		return fmt.Errorf("retrieving index manager log index: %w", err)
	}
	batch := batchPool.Get().(*wal.Batch)
	defer func() {
		batch.Clear()
		batchPool.Put(batch)
	}()
	var totalItems int64
	for i := range data {
		var count uint32
		if i < len(counts) {
			count = counts[i]
		}
		batch.Write(idx+uint64(i+1), prependItemCount(count, data[i]))
		totalItems += int64(count)
	}
	if err := b.log.WriteBatch(batch); err != nil {
		return fmt.Errorf("writing log index: %w", err)
	}
	b.itemCount.Add(totalItems)
	return nil
}

// Enqueue enqueues a single entry with the given item count.
func (b *WALBuffer) Enqueue(data []byte, count uint32) (err error) {
	return b.EnqueueBatch([][]byte{data}, []uint32{count})
}

type Merger interface {
	Merge(datum []byte) error
	Execute(ctx context.Context) error
}

var emptyFile = []byte{'\x00'}

func (b *WALBuffer) Dequeue(ctx context.Context, accumulator Merger, max int) (err error) {
	b.RLock()
	if b.closed {
		b.RUnlock()
		return ErrBufferClosed
	}
	b.RUnlock()

	fi, err := b.log.FirstIndex()
	if err != nil {
		if errors.Is(err, wal.ErrClosed) {
			return ErrBufferClosed
		}
		return fmt.Errorf("getting first wal index: %w", err)
	}
	if fi == 0 {
		return nil
	}

	li, err := b.log.LastIndex()
	if err != nil {
		if errors.Is(err, wal.ErrClosed) {
			return ErrBufferClosed
		}
		return fmt.Errorf("getting last wal index: %w", err)
	}
	if fi > li {
		return nil
	}

	// Batch read entries from the WAL instead of reading one at a time.
	// This takes the lock once and reuses segment lookups.
	items, err := b.log.ReadMany(fi, max)
	if err != nil {
		if errors.Is(err, wal.ErrClosed) {
			return ErrBufferClosed
		}
		return fmt.Errorf("batch reading wal: %w", err)
	}

	var i uint64
	j := 0
	discardedCount := 0
	var itemsToDeduct int64
	mergedIndices := make([]uint64, 0, max)

	for idx, raw := range items {
		i = fi + uint64(idx)

		// Each entry has a 4-byte item count header
		entryItemCount := int64(readItemCount(raw))
		item := stripItemCount(raw)

		if len(item) == 1 && item[0] == emptyFile[0] {
			itemsToDeduct += entryItemCount
			continue
		}

		// Check if this item has exceeded max retries
		if b.shouldDiscard(i) {
			attempts := b.getAttempts(i)
			b.logger.Warn("discarding item after max retries",
				zap.Uint64("index", i),
				zap.Int("attempts", attempts),
				zap.Int("maxRetries", b.maxRetries))

			if b.onDiscard != nil {
				b.onDiscard(i, item, attempts)
			}

			b.clearAttempts(i)
			discardedCount++
			itemsToDeduct += entryItemCount
			continue
		}

		if err := accumulator.Merge(item); err != nil {
			return fmt.Errorf("merging operation: %w", err)
		}

		mergedIndices = append(mergedIndices, i)
		itemsToDeduct += entryItemCount
		j++
	}
	// Advance i past the last processed entry for truncation
	i = fi + uint64(len(items))

	// Record discarded items in metrics
	if discardedCount > 0 {
		b.metrics.IncItemsDiscarded(discardedCount)
	}

	// If we only had discards (no successful merges), still truncate
	if j == 0 && discardedCount > 0 {
		b.Lock()
		defer b.Unlock()
		if err := b.log.TruncateFront(i); err != nil {
			if errors.Is(err, wal.ErrClosed) {
				return ErrBufferClosed
			}
			return fmt.Errorf("truncating wal at %d: %w", i, err)
		}
		b.itemCount.Add(-itemsToDeduct)
		return nil
	}

	if j > 0 {
		b.metrics.IncDequeueAttempts(j)

		// Increment attempts for all merged items before execute
		for _, idx := range mergedIndices {
			b.incrementAttempts(idx)
		}

		if err := accumulator.Execute(ctx); err != nil {
			b.metrics.IncDequeueFailure(errorType(err))
			b.metrics.SetPendingRetries(b.getPendingRetriesCount())
			return fmt.Errorf("executing operation: %w", err)
		}

		// Success - clear attempts for processed items
		for _, idx := range mergedIndices {
			b.clearAttempts(idx)
		}

		b.Lock()
		defer b.Unlock()
		if err := b.log.TruncateFront(i); err != nil {
			if errors.Is(err, wal.ErrClosed) {
				return ErrBufferClosed
			}
			return fmt.Errorf("truncating wal at %d: %w", i, err)
		}

		b.itemCount.Add(-itemsToDeduct)
		b.metrics.SetPendingRetries(b.getPendingRetriesCount())
	}

	return nil
}

// shouldDiscard returns true if the item at the given index has exceeded max retries.
func (b *WALBuffer) shouldDiscard(index uint64) bool {
	if b.maxRetries == 0 {
		return false
	}
	b.attemptsMu.Lock()
	defer b.attemptsMu.Unlock()
	return b.attempts[index] >= b.maxRetries
}

// getAttempts returns the current attempt count for the given index.
func (b *WALBuffer) getAttempts(index uint64) int {
	b.attemptsMu.Lock()
	defer b.attemptsMu.Unlock()
	return b.attempts[index]
}

// incrementAttempts increments the attempt count for the given index.
func (b *WALBuffer) incrementAttempts(index uint64) {
	b.attemptsMu.Lock()
	defer b.attemptsMu.Unlock()
	b.attempts[index]++
}

// clearAttempts removes the attempt count for the given index.
func (b *WALBuffer) clearAttempts(index uint64) {
	b.attemptsMu.Lock()
	defer b.attemptsMu.Unlock()
	delete(b.attempts, index)
}

// getPendingRetriesCount returns the number of items currently pending retry.
func (b *WALBuffer) getPendingRetriesCount() int {
	b.attemptsMu.Lock()
	defer b.attemptsMu.Unlock()
	return len(b.attempts)
}

// errorType categorizes an error for metrics labeling.
func errorType(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "other"
	}
}
