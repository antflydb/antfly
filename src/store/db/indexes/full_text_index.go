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
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/util"
	"github.com/cockroachdb/pebble/v2"
	gojson "github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	fullTextMaxBatches        = 100
	fullTextPartitionSize     = 100
	fullTextBackfillBatchSize = 1_000
)

var fullTextRateLimiter *rate.Limiter = rate.NewLimiter(
	10_000,
	fullTextMaxBatches*fullTextPartitionSize,
)

func init() {
	limitStr := os.Getenv("ANTFLY_FULL_TEXT_RATE_LIMIT")
	if limitStr != "" {
		// Parse the rate limit from the environment variable
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			panic("Invalid ANTFLY_FULL_TEXT_RATE_LIMIT value: " + limitStr)
		}
		fullTextRateLimiter = rate.NewLimiter(
			rate.Limit(limit),
			fullTextPartitionSize*fullTextMaxBatches,
		)
	}

	// Use goccy/go-json for JSON (de)serialization in Bleve to improve performance
	util.MarshalJSON = gojson.Marshal
	util.UnmarshalJSON = gojson.Unmarshal
}

var fullTextIndexOpPool = sync.Pool{
	New: func() any {
		return &fullTextIndexOp{
			Writes:  make([][2][]byte, 0, 100), // Pre-allocate with reasonable capacity
			Deletes: make([][]byte, 0, 100),    // Pre-allocate with reasonable capacity
		}
	},
}

var _ Index = (*BleveIndexV2)(nil) // Compile-time check for Index interface implementation

func init() {
	RegisterIndex(IndexTypeFullTextV0, NewBleveIndexV2)
	RegisterIndex(IndexTypeFullText, NewBleveIndexV2)
}

// BleveIndex implements the Index interface using Bleve.
type BleveIndexV2 struct {
	name      string
	indexPath string
	db        *pebble.DB
	bidx      bleve.Index
	byteRange types.Range

	conf   FullTextIndexConfig
	schema *schema.TableSchema

	logger *zap.Logger

	egCancel context.CancelFunc
	egCtx    context.Context
	eg       *errgroup.Group
	walBuf   *inflight.WALBuffer

	enqueueChan chan int
	flushTime   time.Duration

	zstdWriter *zstd.Encoder
	zstdReader *zstd.Decoder

	rebuildState *RebuildState

	// State variables
	backfillDone chan struct{} // Signals when backfill is complete
	backfillTracker

	// Pause/Resume support
	pauseMu    sync.Mutex
	pauseCond  *sync.Cond
	pauseAckCh chan struct{} // Separate channel for plexer pause acknowledgment
	paused     atomic.Bool
}

// BleveBatchV2 implements the Batch interface for Bleve.
type BleveBatchV2 struct {
	index *BleveIndexV2
	batch *bleve.Batch
}

type fullTextIndexOp struct {
	Writes  [][2][]byte `json:"writes"`
	Deletes [][]byte    `json:"deletes"`

	i *BleveIndexV2 `json:"-"`
}

func (fti *fullTextIndexOp) reset() {
	// Clear slices but keep underlying capacity
	fti.Writes = fti.Writes[:0]
	fti.Deletes = fti.Deletes[:0]
}

func (fti *fullTextIndexOp) encode(buf *bytes.Buffer) error {
	payload, err := json.Marshal(fti)
	if err != nil {
		return fmt.Errorf("failed to marshal kv message: %w", err)
	}

	buf.Reset()
	_, _ = buf.Write(fti.i.zstdWriter.EncodeAll(payload, nil))
	return nil
}

func (kv *fullTextIndexOp) decode(data []byte) error {
	payload, err := kv.i.zstdReader.DecodeAll(data, nil)
	if err != nil {
		return fmt.Errorf("failed to decompress kv message: %w", err)
	}
	if err := json.Unmarshal(payload, kv); err != nil {
		return fmt.Errorf("failed to decode kv message: %w", err)
	}
	return nil
}

func (f *fullTextIndexOp) Merge(datum []byte) error {
	ftio := fullTextIndexOpPool.Get().(*fullTextIndexOp)
	ftio.i = f.i // Set the index reference to the current index
	defer func() {
		ftio.reset()
		fullTextIndexOpPool.Put(ftio)
	}()
	if err := ftio.decode(datum); err != nil {
		return fmt.Errorf("unmarshaling enrich operation: %w", err)
	}
	f.Writes = append(f.Writes, ftio.Writes...)
	f.Deletes = append(f.Deletes, ftio.Deletes...)
	return nil
}

func (f *fullTextIndexOp) Execute(ctx context.Context) (err error) {
	// startTime := time.Now()
	var currKey []byte
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", string(currKey), r) // Print the panic value
			fmt.Println("Stack trace:\n", string(debug.Stack()))     // Print the stack trace
			// panic(r)
			err = fmt.Errorf("panic during fullTextIndexOp execution for key %s: %v", currKey, r)
		}
	}()
	batch := f.i.NewBatch()
	// Limit the rate of writes to avoid overwhelming the index
	if err := fullTextRateLimiter.WaitN(ctx, len(f.Writes)); err != nil {
		return fmt.Errorf("waiting for rate limiter: %w", err)
	}
	if len(f.Writes) > 0 {
		writeOps.WithLabelValues(f.i.name).Add(float64(len(f.Writes)))
	}
	if len(f.Deletes) > 0 {
		deleteOps.WithLabelValues(f.i.name).Add(float64(len(f.Deletes)))
	}
	buf := bytes.NewBuffer(nil)
	for i := range f.Writes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		buf.Reset()
		currKey = f.Writes[i][0]
		// If this is a summary suffix key, strip the suffix to get the main document key
		docKey := currKey
		if bytes.HasSuffix(currKey, storeutils.SummarySuffix) {
			parsedDocKey, _, ok := storeutils.ParseSummaryKey(currKey)
			if !ok {
				continue // Skip malformed key
			}
			docKey = parsedDocKey
		}
		// Fetch document with all summaries and chunks using GetDocument abstraction
		result, err := storeutils.GetDocument(ctx, f.i.db, docKey, storeutils.QueryOptions{
			AllSummaries: true,
			AllChunks:    true, // Fetch all :cft: chunks (like summaries)
		})
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return fmt.Errorf("getting document with summaries for indexing: %w", err)
		}
		if len(result.Document) == 0 {
			f.i.logger.Warn(
				"Skipping empty write for Bleve index",
				zap.String("key", types.FormatKey(docKey)),
			)
			continue // Skip empty writes
		}

		// Add summaries to document if present
		if len(result.Summaries) > 0 {
			result.Document["_summaries"] = result.Summaries
		}

		// Add chunks to document if present
		if len(result.Chunks) > 0 {
			result.Document["_chunks"] = result.Chunks
		}

		if err := batch.Insert(string(docKey), result.Document); err != nil {
			return fmt.Errorf("inserting write to batch: %w", err)
		}
	}
	for _, del := range f.Deletes {
		batch.Delete(del)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("applying batch: %w", err)
	}
	return nil
}

func NewBleveIndexV2(
	logger *zap.Logger,
	_ *common.Config,
	db *pebble.DB,
	dir string,
	name string,
	config *IndexConfig,
	_ *pebbleutils.Cache,
) (Index, error) {
	// Validate config against schema
	writer, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("creating zstd writer: %w", err)
	}
	reader, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("creating zstd reader: %w", err)
	}

	var c FullTextIndexConfig
	if config != nil {
		if c, err = config.AsFullTextIndexConfig(); err != nil {
			return nil, fmt.Errorf("parsing config: %w", err)
		}
	}
	indexPath := filepath.Join(dir, name)
	bi := &BleveIndexV2{
		logger:       logger,
		db:           db,
		indexPath:    indexPath,
		name:         name,
		zstdWriter:   writer,
		zstdReader:   reader,
		rebuildState: NewRebuildState(indexPath),

		conf:       c,
		flushTime:  DefaultFlushTime,
		pauseAckCh: make(chan struct{}, 1), // Buffered to prevent blocking plexer
	}
	bi.pauseCond = sync.NewCond(&bi.pauseMu)
	return bi, nil
}

func (bi *BleveIndexV2) Name() string {
	return bi.name
}

func (bi *BleveIndexV2) Type() IndexType {
	return IndexTypeFullText
}

// Open initializes or opens the Bleve index.
func (bi *BleveIndexV2) Open(
	rebuild bool,
	tableSchema *schema.TableSchema,
	byteRange types.Range,
) error {
	var ctx context.Context
	ctx, bi.egCancel = context.WithCancel(context.TODO())
	bi.eg, bi.egCtx = errgroup.WithContext(ctx)

	bi.byteRange = byteRange
	bi.schema = tableSchema

	if rebuild {
		_ = os.RemoveAll(bi.indexPath)
	} else if !rebuild && !bi.conf.MemOnly {
		if _, statErr := os.Stat(filepath.Join(bi.indexPath, "indexWAL")); errors.Is(statErr, os.ErrNotExist) {
			bi.logger.Info("Rebuilding index, indexWAL does not exist")
			rebuild = true
		}
	}

	var rebuildFrom []byte
	if !rebuild {
		var needsRebuild bool
		var err error
		rebuildFrom, needsRebuild, err = bi.rebuildState.Check()
		if err != nil {
			return fmt.Errorf("checking rebuild status: %w", err)
		}
		if needsRebuild {
			bi.logger.Debug("Rebuild status found: resuming rebuilding index",
				zap.ByteString("status", rebuildFrom),
			)
			rebuild = true
		} else {
			bi.logger.Debug("Rebuild status not found: assuming rebuild is not needed")
		}
	}
	var err error
	bi.walBuf, err = inflight.NewWALBufferWithOptions(bi.logger, bi.indexPath, "indexWAL", inflight.WALBufferOptions{
		MaxRetries: 10,
		Metrics:    NewPrometheusWALMetrics("full_text"),
	})
	if err != nil {
		return fmt.Errorf("creating WALBuffer: %w", err)
	}
	if !bi.conf.MemOnly {
		foundExisting := false
		bleveIndexPath := bi.indexPath + "/bleve"
		_, statErr := os.Stat(bleveIndexPath)
		if statErr == nil {
			// Directory exists and not rebuilding, open existing index
			bi.bidx, err = bleve.Open(bleveIndexPath)
			if err == nil {
				bi.logger.Info("Opened existing Bleve index", zap.String("path", bleveIndexPath))
				// No need to rebuild if we successfully opened existing index
				foundExisting = true
			} else {
				// If open failed, log warning and attempt to rebuild
				bi.logger.Warn("Failed to open existing Bleve index, attempting rebuild", zap.String("path", bleveIndexPath), zap.Error(err))
				rebuild = true // Force rebuild if open failed
				// Clean up potentially corrupted index before rebuilding
				_ = os.RemoveAll(bleveIndexPath)
			}
		} else {
			if !os.IsNotExist(statErr) {
				return fmt.Errorf("checking index directory %s: %w", bleveIndexPath, statErr)
			}
			bi.logger.Info("Index directory does not exist, creating new index", zap.String("path", bleveIndexPath))
			rebuild = true // Force rebuild if directory doesn't exist
		}
		if rebuild {
			bi.startBackfill()
			if err := bi.rebuildState.Update(rebuildFrom); err != nil {
				bi.logger.Warn(
					"Failed to set rebuild status for bleve index",
					zap.String("path", bi.indexPath),
					zap.Error(err),
				)
				return fmt.Errorf("setting rebuild status: %w", err)
			}
		}
		if rebuild && !foundExisting {
			if err := os.MkdirAll(bleveIndexPath, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
				return fmt.Errorf("creating index directory: %w", err)
			}

			// Create new index if it doesn't exist or rebuild is requested
			bi.logger.Info(
				"Creating new Bleve index",
				zap.String("path", bleveIndexPath),
				zap.Bool("rebuild", rebuild),
			)
			indexMapping := schema.NewIndexMapFromSchema(tableSchema)
			bi.logger.Debug("Creating Bleve index with schema and mapping",
				zap.Any("schema", tableSchema),
				zap.Any("indexMapping", indexMapping))
			bi.bidx, err = bleve.New(bleveIndexPath, indexMapping)
			if err != nil {
				return fmt.Errorf("creating bleve index at %s: %w", bleveIndexPath, err)
			}
		}
	} else {
		indexMapping := schema.NewIndexMapFromSchema(tableSchema)
		bi.logger.Debug("Creating in-memory Bleve index with schema and mapping",
			zap.Any("schema", tableSchema),
			zap.Any("indexMapping", indexMapping))
		bi.bidx, err = bleve.NewMemOnly(indexMapping)
		if err != nil {
			return fmt.Errorf("creating bleve Memory Index: %w", err)
		}
		bi.logger.Info("Opened in-memory Bleve index")
		// Rebuild always required for mem-only
		rebuild = true
	}

	backfillWait := make(chan struct{})
	bi.backfillDone = nil
	if rebuild {
		bi.backfillDone = make(chan struct{})
	}
	bi.enqueueChan = make(chan int, 5)
	bi.eg.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered from panic:", r)              // Print the panic value
				fmt.Println("Stack trace:\n", string(debug.Stack())) // Print the stack trace
				panic(r)
			}
		}()
		select {
		case <-bi.egCtx.Done():
			return nil
		case <-backfillWait:
		}
		const maxJitter = time.Millisecond * 200
		jitter := maxJitter - rand.N(maxJitter) //nolint:gosec // G404: non-security randomness for ML/jitter
		t := time.NewTimer(bi.flushTime + jitter)
		enqueueCounter := 0
		dequeue := func() error {
			enqueueCounter = 0
			ops := fullTextIndexOpPool.Get().(*fullTextIndexOp)
			defer func() {
				ops.reset()
				fullTextIndexOpPool.Put(ops)
			}()
			ops.i = bi
			if err := bi.walBuf.Dequeue(bi.egCtx, ops, fullTextMaxBatches); err != nil {
				if errors.Is(err, inflight.ErrBufferClosed) ||
					errors.Is(err, bleve.ErrorIndexClosed) ||
					errors.Is(err, context.Canceled) {
					return err
				}
				bi.logger.Error("Failed to dequeue from WAL buffer", zap.Error(err))
			}
			if len(ops.Writes) == 0 && len(ops.Deletes) == 0 {
				t.Reset(bi.flushTime + jitter)
			} else {
				t.Reset(100*time.Millisecond + jitter)
			}
			return nil
		}
		defer t.Stop()
		for {
			// Check if paused at start of each iteration (no mutex needed)
			if bi.paused.Load() {
				// Send acknowledgment without holding any mutex
				select {
				case bi.pauseAckCh <- struct{}{}:
				default:
					// Already acknowledged
				}

				// Now wait for resume
				bi.pauseMu.Lock()
				for bi.paused.Load() {
					// Send ack before each wait in case Pause() was called again
					select {
					case bi.pauseAckCh <- struct{}{}:
					default:
					}
					bi.pauseCond.Wait()
				}
				bi.pauseMu.Unlock()
			}

			select {
			case <-bi.egCtx.Done():
				return nil
			case n := <-bi.enqueueChan:
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
	})

	if !rebuild {
		bi.logger.Debug("Skipping rebuild of bleve index", zap.String("path", bi.indexPath))
		close(backfillWait)
		return nil
	}
	// Equivalent to min of ei.byteRange[0] and rebuildFrom
	if bytes.Compare(rebuildFrom, byteRange[0]) < 0 {
		rebuildFrom = byteRange[0]
	}

	batch := bi.bidx.NewBatch()
	count := 0
	indexedCount := 0
	scanned := 0

	// FIXME (ajr) This being async means we need to make sure new writes that come in
	// are supersededed by these backfill writes.
	// Also how should we handle errors here?
	bi.eg.Go(func() error {
		defer close(backfillWait)
		defer close(bi.backfillDone)
		bi.logger.Debug("Rebuilding Bleve index from Pebble data", zap.Stringer("range", byteRange))

		defer func() {
			bi.logger.Info(
				"Rebuild: Finished rebuilding Bleve index",
				zap.Int("scanned", scanned),
				zap.Int("total_indexed", indexedCount),
				zap.Stringer("range", byteRange),
			)
		}()

		flushBatch := func(currKey []byte) error {
			if count == 0 {
				// Even with no pending docs, a nil key means the backfill scan
				// is complete and we must finalize the rebuild state.
				if currKey == nil {
					if err := bi.rebuildState.Clear(); err != nil {
						bi.logger.Warn(
							"Failed to delete rebuild status for bleve index",
							zap.Int("total_indexed", indexedCount),
							zap.String("path", bi.indexPath),
							zap.Error(err),
						)
						return fmt.Errorf("deleting rebuild status: %w", err)
					}
					bi.finishBackfill()
				}
				return nil
			}

			if err := fullTextRateLimiter.WaitN(bi.egCtx, count); err != nil {
				if errors.Is(err, context.Canceled) {
					bi.logger.Info("Rebuild: Rate limiter context done, stopping rebuild",
						zap.Int("scanned", scanned),
						zap.Int("total_indexed", indexedCount),
						zap.Error(err))
					return nil
				}
				return fmt.Errorf("waiting for rate limiter: %w", err)
			}
			if err := bi.bidx.Batch(batch); err != nil {
				bi.logger.Warn("Apply batch to bleve index",
					zap.Int("scanned", scanned),
					zap.Int("total_indexed", indexedCount),
					zap.Error(err))
				return fmt.Errorf("applying batch index: %w", err)
			}
			indexedCount += count
			// Reset batch and count
			batch.Reset()
			count = 0

			if currKey != nil {
				if indexedCount > 0 {
					if err := bi.rebuildState.Update(currKey); err != nil {
						bi.logger.Warn(
							"Failed to set rebuild status for bleve index",
							zap.Int("total_indexed", indexedCount),
							zap.String("path", bi.indexPath),
							zap.Error(err),
						)
						return fmt.Errorf("setting rebuild status: %w", err)
					}
				}
			} else {
				// This means we're done with the backfill so delete the rebuild status
				if err := bi.rebuildState.Clear(); err != nil {
					bi.logger.Warn(
						"Failed to delete rebuild status for bleve index",
						zap.Int("total_indexed", indexedCount),
						zap.String("path", bi.indexPath),
						zap.Error(err),
					)
					return fmt.Errorf("deleting rebuild status: %w", err)
				}
				bi.finishBackfill()
			}
			return nil
		}

		// Track the last document key for rebuild status updates
		var lastDocKey []byte

		// Use ScanForBackfill to iterate through documents with summaries and chunks
		err := storeutils.ScanForBackfill(bi.egCtx, bi.db, storeutils.BackfillScanOptions{
			ByteRange:        [2][]byte{rebuildFrom, byteRange[1]},
			IncludeSummaries: true,
			IncludeChunks:    true, // Fetch all :cft: chunks (like summaries)
			BatchSize:        fullTextBackfillBatchSize,
			ProcessBatch: func(ctx context.Context, docBatch []storeutils.DocumentScanState) error {
				scanned += len(docBatch)
				bi.addBackfillItems(len(docBatch))
				if len(docBatch) > 0 {
					lastKey := docBatch[len(docBatch)-1].CurrentDocKey
					bi.updateBackfillProgress(estimateProgress(byteRange[0], byteRange[1], lastKey))
				}

				for _, state := range docBatch {
					// Add summaries to document if any exist
					if len(state.Summaries) > 0 {
						state.Document["_summaries"] = state.Summaries
					}

					// Add chunks to document if any exist
					if len(state.Chunks) > 0 {
						state.Document["_chunks"] = state.Chunks
					}

					doc := &document.Document{}
					doc.SetID(string(state.CurrentDocKey))
					if err := bi.bidx.Mapping().MapDocument(doc, state.Document); err != nil {
						bi.logger.Warn(
							"Rebuild: Failed to map document, skipping",
							zap.String("key", types.FormatKey(state.CurrentDocKey)),
							zap.Error(err),
						)
						continue
					}

					if err := batch.IndexAdvanced(doc); err != nil {
						bi.logger.Error(
							"Rebuild: Failed adding index entry to batch",
							zap.String("key", types.FormatKey(state.CurrentDocKey)),
							zap.Error(err),
						)
						continue
					}
					count++
					lastDocKey = state.CurrentDocKey
				}

				// Flush batch if we've reached the size limit
				if count >= fullTextBackfillBatchSize {
					return flushBatch(lastDocKey)
				}
				return nil
			},
		})
		if err != nil {
			return fmt.Errorf("scanning for backfill: %w", err)
		}

		// Flush any remaining documents
		if err := flushBatch(nil); err != nil {
			return fmt.Errorf("flushing final batch: %w", err)
		}

		return nil
	})

	return nil
}

func (bi *BleveIndexV2) Delete() error {
	_ = os.RemoveAll(bi.indexPath)
	return nil
}

func (bi *BleveIndexV2) Close() error {
	bi.logger.Info(
		"Closing Bleve index",
		zap.String("name", bi.name),
		zap.String("indexPath", bi.indexPath),
	)
	defer bi.logger.Info(
		"Bleve index closed",
		zap.String("name", bi.name),
		zap.String("indexPath", bi.indexPath),
	)
	if bi.egCancel != nil {
		bi.egCancel()
	}
	if err := bi.walBuf.Close(); err != nil {
		bi.logger.Warn(
			"Closing walBuf",
			zap.String("name", bi.name),
			zap.String("indexPath", bi.indexPath),
			zap.Error(err),
		)
	}
	if bi.bidx != nil {
		if err := bi.bidx.Close(); err != nil {
			bi.logger.Warn(
				"Closing index",
				zap.String("name", bi.name),
				zap.String("indexPath", bi.indexPath),
				zap.Error(err),
			)
		}
	}
	if bi.eg != nil {
		if err := bi.eg.Wait(); err != nil {
			bi.zstdReader.Close()
			if errors.Is(err, context.Canceled) || errors.Is(err, inflight.ErrBufferClosed) ||
				errors.Is(err, bleve.ErrorIndexClosed) {
				return nil // Normal shutdown
			}
			return fmt.Errorf("waiting for background tasks to finish: %w", err)
		}
	}
	bi.zstdReader.Close()
	return nil
}

func (bi *BleveIndexV2) NewBatch() *BleveBatchV2 {
	return &BleveBatchV2{
		index: bi,
		batch: bi.bidx.NewBatch(),
	}
}

func (bi *BleveIndexV2) Batch(
	ctx context.Context,
	writes [][2][]byte,
	deletes [][]byte,
	sync bool,
) error {
	if len(writes) == 0 && len(deletes) == 0 {
		return nil // Nothing to do
	}

	// If sync is true, execute immediately
	if sync {
		f := &fullTextIndexOp{
			i:       bi,
			Writes:  writes,
			Deletes: deletes,
		}
		return f.Execute(ctx)
	}

	partitionedBatch := make([][]byte, 0, (len(writes)+len(deletes))/fullTextPartitionSize+1)
	partitionCounts := make([]uint32, 0, cap(partitionedBatch))

	// Process writes in partitions
	for i := 0; i < len(writes); i += fullTextPartitionSize {
		end := min(i+fullTextPartitionSize, len(writes))

		// Create a partition of writes
		writePartition := writes[i:end]

		// Also partition deletes proportionally if we're in the first iteration
		var deletePartition [][]byte
		if i == 0 && len(deletes) > 0 {
			deleteEnd := min(fullTextPartitionSize, len(deletes))
			deletePartition = deletes[:deleteEnd]
			deletes = deletes[deleteEnd:] // Remove processed deletes
		}

		f := &fullTextIndexOp{
			i:       bi,
			Writes:  writePartition,
			Deletes: deletePartition,
		}

		buf := bytes.NewBuffer(nil)
		if err := f.encode(buf); err != nil {
			return fmt.Errorf(
				"encoding fullTextIndexOp for partition %d: %w",
				i/fullTextPartitionSize,
				err,
			)
		}

		if buf.Len() == 0 {
			continue
		}

		partitionedBatch = append(partitionedBatch, buf.Bytes())
		partitionCounts = append(partitionCounts, uint32(len(writePartition)+len(deletePartition))) //nolint:gosec // G115: partition size bounded by fullTextPartitionSize
	}

	// Process any remaining deletes that weren't included in write partitions
	if len(deletes) > 0 {
		for i := 0; i < len(deletes); i += fullTextPartitionSize {
			end := min(i+fullTextPartitionSize, len(deletes))

			deletePartition := deletes[i:end]

			f := &fullTextIndexOp{
				i:       bi,
				Deletes: deletePartition,
			}

			buf := bytes.NewBuffer(nil)
			if err := f.encode(buf); err != nil {
				return fmt.Errorf(
					"encoding fullTextIndexOp for delete partition %d: %w",
					i/fullTextPartitionSize,
					err,
				)
			}

			if buf.Len() == 0 {
				continue
			}

			partitionedBatch = append(partitionedBatch, buf.Bytes())
			partitionCounts = append(partitionCounts, uint32(len(deletePartition))) //nolint:gosec // G115: partition size bounded by fullTextPartitionSize
		}
	}

	// Enqueue all partitions as a batch
	if len(partitionedBatch) > 0 {
		if err := bi.walBuf.EnqueueBatch(partitionedBatch, partitionCounts); err != nil {
			if errors.Is(err, inflight.ErrBufferClosed) || errors.Is(err, bleve.ErrorIndexClosed) {
				return nil
			}
			return fmt.Errorf("enqueueing batch to walBuf: %w", err)
		}

		// Signal dequeue AFTER enqueuing to avoid race condition
		select {
		case bi.enqueueChan <- len(partitionedBatch):
		default:
		}
	}

	return nil
}

func (bb *BleveBatchV2) Insert(key string, val map[string]any) error {
	// Remove float32/float64 slices from the map before indexing
	for fieldKey, fieldValue := range val {
		switch fieldValue.(type) {
		case []float32, []float64:
			delete(val, fieldKey)
		case []any:
			// Check if all elements are numbers (float64 by default from json.Unmarshal)
			// This is a common case for embeddings stored as JSON arrays of numbers.
			// We make a best-effort attempt to identify and remove such arrays.
			// More specific type assertions might be needed if structure is more complex.
			if s, ok := fieldValue.([]any); ok {
				isFloatSlice := true
				if len(s) > 0 {
					switch s[0].(type) {
					case float32, float64:
					default:
						isFloatSlice = false
					}
				} else { // an empty slice could have been a float slice
					isFloatSlice = true // Or, based on policy, decide not to remove empty slices
				}
				if isFloatSlice {
					delete(val, fieldKey)
				}
			}
		}
	}

	doc := &document.Document{}
	doc.SetID(key)
	if err := bb.index.bidx.Mapping().MapDocument(doc, val); err != nil {
		return fmt.Errorf("mapping document for indexing: %w", err)
	}

	if err := bb.batch.IndexAdvanced(doc); err != nil {
		return fmt.Errorf("indexing document to batch %s: %w", key, err)
	}
	return nil
}

func (bb *BleveBatchV2) Delete(key []byte) {
	deleteOps.WithLabelValues(bb.index.name).Inc()
	bb.batch.Delete(string(key))
}

func (bb *BleveBatchV2) Commit() error {
	return bb.index.bidx.Batch(bb.batch)
}

// Search performs a search against the Bleve index.
// The encodedQuery is expected to be a JSON marshaled bleve.SearchRequest.
// The result is a JSON marshaled remoteindex.RemoteIndexSearchResponse containing the bleve.SearchResult.
func (bi *BleveIndexV2) Search(ctx context.Context, query any) (any, error) {
	queryOps.WithLabelValues(bi.name).Inc()
	if query == nil {
		return nil, storeutils.ErrEmptyRequest
	}
	searchRequest, ok := query.(*bleve.SearchRequest)
	if !ok {
		return nil, fmt.Errorf("invalid query type: %T expected *bleve.SearchRequest", query)
	}
	if searchRequest == nil {
		return nil, storeutils.ErrEmptyRequest
	}

	if bi.bidx == nil {
		return nil, errors.New("bleve index is not initialized")
	}
	// Execute the search
	result, err := bi.bidx.SearchInContext(ctx, searchRequest)
	if err != nil {
		// Empty indexes have no BM25 field stats; return empty results
		// instead of an error (common during shard splits).
		if isBM25FieldStatError(err) {
			return &bleve.SearchResult{
				Status: &bleve.SearchStatus{Successful: 1},
			}, nil
		}
		return nil, fmt.Errorf("searching bleve index: %w", err)
	}
	return result, nil
}

func (bi *BleveIndexV2) Stats() IndexStats {
	if bi.bidx == nil {
		return FullTextIndexStats{
			Error: "index is not initialized",
		}.AsIndexStats()
	}
	rawStats := bi.bidx.StatsMap()

	// Extract the nested index stats if available
	indexStats, ok := rawStats["index"].(map[string]any)
	if !ok {
		// If the structure is different, return a minimal stats map
		return FullTextIndexStats{
			Error: "unable to parse index statistics",
		}.AsIndexStats()
	}

	getUnsigned := func(m map[string]any, key string) uint64 {
		if val, ok := m[key]; ok {
			switch v := val.(type) {
			case uint64:
				return v
			case int64:
				if v < 0 {
					return 0
				}
				return uint64(v)
			case int:
				if v < 0 {
					return 0
				}
				return uint64(v)
			case float64:
				if v < 0 {
					return 0
				}
				return uint64(v)
			}
		}
		return 0
	}

	// Extract relevant stats
	is := FullTextIndexStats{
		DiskUsage: getUnsigned(indexStats, "CurOnDiskBytes"),
	}
	// cleanStats := map[string]any{
	// "disk_usage": getUnsigned(indexStats, "CurOnDiskBytes"),
	// "disk_files": getNumeric(indexStats, "CurOnDiskFiles"),
	// "total_batches":       getNumeric(indexStats, "TotBatches"),
	// "total_updates":       getNumeric(indexStats, "TotUpdates"),
	// "total_deletes":       getNumeric(indexStats, "TotDeletes"),
	// "total_index_time":    getNumeric(indexStats, "TotIndexTime"),
	// "total_analysis_time": getNumeric(indexStats, "TotAnalysisTime"),
	// "total_errors":        getNumeric(indexStats, "TotOnErrors"),
	// "term_searches_started":  getNumeric(indexStats, "TotTermSearchersStarted"),
	// "term_searches_finished": getNumeric(indexStats, "TotTermSearchersFinished"),
	// "synonym_searches":       getNumeric(indexStats, "TotSynonymSearches"),
	// "bytes_indexed": getNumeric(indexStats, "TotIndexedPlainTextBytes"),
	// }

	// Add search-related stats from raw stats if available
	// if searches, ok := rawStats["searches"]; ok {
	// 	cleanStats["searches"] = searches
	// }
	// if searchTime, ok := rawStats["search_time"]; ok {
	// 	cleanStats["search_time"] = searchTime
	// }

	docCount, err := bi.bidx.DocCount()
	if err != nil {
		is.Error = err.Error()
	} else {
		is.TotalIndexed = docCount
	}
	if bi.backfilling.Load() {
		is.Rebuilding = true
		is.BackfillProgress = bi.loadBackfillProgress()
		is.BackfillItemsProcessed = bi.backfillItemsProcessed.Load()
	}

	return is.AsIndexStats()
}

func (bi *BleveIndexV2) UpdateSchema(newSchema *schema.TableSchema) error {
	oldSchema := bi.schema
	bi.schema = newSchema

	// Check if we need to update the bleve mapping due to template changes
	if !templatesEqual(oldSchema, newSchema) {
		return bi.reloadMapping(newSchema)
	}
	return nil
}

// templatesEqual compares dynamic templates between two schemas.
// Returns true if both schemas have the same DynamicTemplates (including if both are nil/empty).
func templatesEqual(old, new *schema.TableSchema) bool {
	var oldTemplates, newTemplates []schema.DynamicTemplate
	if old != nil {
		oldTemplates = old.DynamicTemplates
	}
	if new != nil {
		newTemplates = new.DynamicTemplates
	}
	return reflect.DeepEqual(oldTemplates, newTemplates)
}

// reloadMapping closes the current index and reopens it with an updated mapping.
// This is used for hot-reloading dynamic templates without requiring a full rebuild.
func (bi *BleveIndexV2) reloadMapping(newSchema *schema.TableSchema) error {
	if bi.conf.MemOnly {
		// Memory-only indexes don't support hot-reload; they need rebuild
		bi.logger.Warn("Memory-only index cannot hot-reload mapping; templates will apply to new documents only")
		return nil
	}

	// Generate new mapping with updated templates
	newMapping := schema.NewIndexMapFromSchema(newSchema)
	mappingBytes, err := json.Marshal(newMapping)
	if err != nil {
		return fmt.Errorf("marshaling new mapping: %w", err)
	}

	// Close current index
	if err := bi.bidx.Close(); err != nil {
		return fmt.Errorf("closing index for mapping update: %w", err)
	}

	// Reopen with updated mapping
	bleveIndexPath := bi.indexPath + "/bleve"
	bi.bidx, err = bleve.OpenUsing(bleveIndexPath, map[string]any{
		"updated_mapping": string(mappingBytes),
	})
	if err != nil {
		// Attempt to reopen without update on failure
		bi.bidx, _ = bleve.Open(bleveIndexPath)
		return fmt.Errorf("reopening index with updated mapping: %w", err)
	}

	bi.logger.Info("Reloaded bleve index with updated mapping",
		zap.String("path", bleveIndexPath))
	return nil
}

func (bi *BleveIndexV2) UpdateRange(newRange types.Range) error {
	bi.byteRange = newRange
	return nil
}

// WaitForBackfill waits for the index backfill to complete.
// Returns immediately if no backfill is running or context is canceled.
func (bi *BleveIndexV2) WaitForBackfill(ctx context.Context) {
	if bi.backfillDone != nil {
		select {
		case <-bi.backfillDone:
		case <-ctx.Done():
		}
	}
}

// Pause stops the plexer from processing and syncs the WAL
func (bi *BleveIndexV2) Pause(ctx context.Context) error {
	bi.pauseMu.Lock()

	// Check if already paused
	if bi.paused.Load() {
		bi.pauseMu.Unlock()
		return nil
	}

	// Set paused flag
	bi.paused.Store(true)

	// Drain any stale acknowledgments from the channel before signaling
	select {
	case <-bi.pauseAckCh:
		// Drained stale ack
	default:
		// No stale ack
	}

	// Release mutex BEFORE waiting for ack to avoid deadlock with plexer
	bi.pauseMu.Unlock()

	// Signal plexer via enqueueChan to trigger immediate check
	// Use context timeout to avoid blocking forever if enqueueChan is full
	select {
	case bi.enqueueChan <- 0:
	case <-ctx.Done():
		bi.paused.Store(false)
		return ctx.Err()
	}

	// Wait for plexer acknowledgment via separate channel (with context timeout)
	select {
	case <-bi.pauseAckCh:
		// Plexer acknowledged pause
	case <-ctx.Done():
		bi.paused.Store(false)
		return ctx.Err()
	}

	// Sync WAL to disk
	if err := bi.walBuf.Sync(); err != nil {
		bi.paused.Store(false)
		return fmt.Errorf("syncing WAL during pause: %w", err)
	}

	bi.logger.Debug("Paused Bleve index", zap.String("name", bi.name))
	return nil
}

// Resume resumes the plexer processing
func (bi *BleveIndexV2) Resume() {
	bi.pauseMu.Lock()
	defer bi.pauseMu.Unlock()

	if !bi.paused.Load() {
		return
	}

	bi.paused.Store(false)
	bi.pauseCond.Broadcast()

	bi.logger.Debug("Resumed Bleve index", zap.String("name", bi.name))
}
