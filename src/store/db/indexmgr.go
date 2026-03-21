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

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/klauspost/compress/zstd"
	"github.com/puzpuzpuz/xsync/v4"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

var ErrEmptyRequest = storeutils.ErrEmptyRequest

// ErrPartialSuccess indicates batch was written to KV store and full-text WAL, but indexes are queued for async processing
var ErrPartialSuccess = errors.New(
	"batch written to KV store and full-text WAL, indexes queued for async processing",
)

const indexManagerPartitionSize = 1000

var indexOpPool = sync.Pool{
	New: func() any {
		return &indexOp{
			Writes:  make([][2][]byte, 0, 100), // Pre-allocate with reasonable capacity
			Deletes: make([][]byte, 0, 100),    // Pre-allocate with reasonable capacity
		}
	},
}

type Index = indexes.Index

type (
	EnrichableIndex = indexes.EnrichableIndex
	PersistFunc     = indexes.PersistFunc
	IndexType       = indexes.IndexType
)

type IndexManager struct {
	logger       *zap.Logger
	antflyConfig *common.Config
	indexes      *xsync.Map[string, Index]

	enricherMu           sync.Mutex
	enrichers            *xsync.Map[string, context.CancelFunc]
	enrichersEg          *errgroup.Group
	enrichersEgCancel    context.CancelFunc
	newEnricherSignal    chan EnrichableIndex
	removeEnricherSignal chan EnrichableIndex

	walBuf         *inflight.WALBuffer
	plexerEgCancel context.CancelFunc
	plexerEgCtx    context.Context
	plexerEg       *errgroup.Group

	enqueueChan chan struct{}

	// Pause/resume support for snapshots
	pauseMu    sync.Mutex
	pauseCond  *sync.Cond
	pauseAckCh chan struct{} // Separate channel for plexer pause acknowledgment
	paused     atomic.Bool
	registerMu sync.RWMutex // Held during pause to block Register()

	// DB of the DBImpl
	db        *pebble.DB
	cache     *pebbleutils.Cache // shared block cache (may be nil)
	dir       string
	byteRange types.Range
	schema    *schema.TableSchema

	zstdWriter *zstd.Encoder
	zstdReader *zstd.Decoder
}

type indexOp struct {
	Writes    [][2][]byte  `json:"writes"`
	Deletes   [][]byte     `json:"deletes"`
	SyncLevel Op_SyncLevel `json:"sync_level"`

	im *IndexManager
}

type deleteKeyGroups struct {
	all          [][]byte
	documents    [][]byte
	chunksByName map[string][][]byte
	edgesByName  map[string][][]byte
}

func (io *indexOp) reset() {
	// Clear slices but keep underlying capacity
	io.Writes = io.Writes[:0]
	io.Deletes = io.Deletes[:0]
	io.SyncLevel = Op_SyncLevelPropose
	io.im = nil
}

func (io *indexOp) encode(buf *bytes.Buffer) ([]byte, error) {
	payload, err := json.Marshal(io)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal kv message: %w", err)
	}

	buf.Reset()
	_, _ = buf.Write(io.im.zstdWriter.EncodeAll(payload, nil))
	return buf.Bytes(), nil
}

func (io *indexOp) decode(data []byte) error {
	payload, err := io.im.zstdReader.DecodeAll(data, nil)
	if err != nil {
		return fmt.Errorf("failed to decompress kv message: %w", err)
	}
	if err := json.Unmarshal(payload, io); err != nil {
		return fmt.Errorf("failed to decode kv message: %w", err)
	}
	return nil
}

func appendDeleteKeyGroup(groups *deleteKeyGroups, key []byte) string {
	groups.all = append(groups.all, key)

	switch {
	case storeutils.IsEdgeKey(key):
		_, _, indexName, _, err := storeutils.ParseEdgeKey(key)
		if err != nil {
			return "edge"
		}
		if groups.edgesByName == nil {
			groups.edgesByName = make(map[string][][]byte)
		}
		groups.edgesByName[indexName] = append(groups.edgesByName[indexName], key)
		return "edge"
	case storeutils.IsChunkKey(key):
		_, indexName, ok := storeutils.ParseChunkKey(key)
		if !ok {
			return "chunk"
		}
		if groups.chunksByName == nil {
			groups.chunksByName = make(map[string][][]byte)
		}
		groups.chunksByName[indexName] = append(groups.chunksByName[indexName], key)
		return "chunk"
	default:
		groups.documents = append(groups.documents, key)
		return "document"
	}
}

func groupDeleteKeys(deletes [][]byte) deleteKeyGroups {
	groups := deleteKeyGroups{
		all:       make([][]byte, 0, len(deletes)),
		documents: make([][]byte, 0, len(deletes)),
	}
	seen := make(map[string]struct{}, len(deletes))

	for _, key := range deletes {
		if len(key) == 0 {
			continue
		}
		sKey := string(key)
		if _, ok := seen[sKey]; ok {
			continue
		}
		seen[sKey] = struct{}{}

		appendDeleteKeyGroup(&groups, key)
	}

	return groups
}

func deletesForIndex(indexName string, indexType IndexType, grouped deleteKeyGroups) [][]byte {
	switch {
	case indexes.IsGraphType(indexType):
		return grouped.edgesByName[indexName]
	case indexes.IsFullTextType(indexType):
		return grouped.documents
	case indexes.IsEmbeddingsType(indexType):
		chunks := grouped.chunksByName[indexName]
		if len(grouped.documents) == 0 {
			return chunks
		}
		if len(chunks) == 0 {
			return grouped.documents
		}
		deletes := make([][]byte, 0, len(grouped.documents)+len(chunks))
		deletes = append(deletes, grouped.documents...)
		deletes = append(deletes, chunks...)
		return deletes
	default:
		return grouped.all
	}
}

func indexTypeMetricLabel(indexType IndexType) string {
	switch {
	case indexes.IsFullTextType(indexType):
		return "full_text"
	case indexes.IsEmbeddingsType(indexType):
		return "embeddings"
	case indexes.IsGraphType(indexType):
		return "graph"
	default:
		return "other"
	}
}

func (io *indexOp) Execute(ctx context.Context) (err error) {
	// startTime := time.Now()
	defer func() {
		if r := recover(); r != nil {
			io.im.logger.Error("panic during indexOp execution",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())))
			err = fmt.Errorf("panic during indexOp execution: %v", r)
		}
	}()
	io.Writes = slices.DeleteFunc(io.Writes, func(w [2][]byte) bool {
		if len(w[0]) == 0 || !io.im.byteRange.Contains(w[0]) {
			return true
		}
		return false
	})
	io.Deletes = slices.DeleteFunc(io.Deletes, func(d []byte) bool {
		if len(d) == 0 || !io.im.byteRange.Contains(d) {
			return true
		}
		return false
	})
	if len(io.Writes) == 0 && len(io.Deletes) == 0 {
		return nil
	}
	groupedDeletes := groupDeleteKeys(io.Deletes)
	io.im.indexes.Range(func(idxName string, value Index) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		// Determine if this index should be synced based on type and syncLevel
		syncThisIndex := false
		indexType := value.Type()
		selectedDeletes := deletesForIndex(idxName, indexType, groupedDeletes)
		if io.SyncLevel == Op_SyncLevelFullText && indexes.IsFullTextType(indexType) {
			syncThisIndex = true
		} else if io.SyncLevel == Op_SyncLevelEmbeddings {
			// SyncLevelEmbeddings waits for vector, sparse, and full-text indexes
			if indexes.IsEmbeddingsType(indexType) || indexes.IsFullTextType(indexType) {
				syncThisIndex = true
			}
		}

		if indexes.IsFullTextType(indexType) || strings.HasPrefix(idxName, "full_text_index") {
			writes := slices.DeleteFunc(slices.Clone(io.Writes), func(w [2][]byte) bool {
				return bytes.HasSuffix(w[0], EmbeddingSuffix)
			})
			if err := value.Batch(ctx, writes, selectedDeletes, syncThisIndex); err != nil {
				io.im.logger.Error(
					"writing batch to full text index",
					zap.String("index", idxName),
					zap.Error(err),
				)
				// TODO (ajr) Should we rely on the index to manage failures?
			}
			return true
		}
		indexOps.WithLabelValues().Add(float64(len(io.Writes) + len(selectedDeletes)))
		// Other indexes (vector, etc.) - sync if syncThisIndex is true
		if err := value.Batch(ctx, io.Writes, selectedDeletes, syncThisIndex); err != nil {
			if errors.Is(err, pebble.ErrClosed) || errors.Is(err, inflight.ErrBufferClosed) {
				return false
			}
			io.im.logger.Error(
				"writing batch to index",
				zap.String("index", idxName),
				zap.Error(err),
			)
			// TODO (ajr) Should we rely on the index to manage failures?
		}
		return true
	})
	// io.im.logger.Debug("Executed index manager batch",
	// 	zap.Duration("took", time.Since(startTime)),
	// 	zap.Int("batchSize", len(io.Writes)+len(io.Deletes)))
	return nil
}

func (io *indexOp) Merge(datum []byte) error {
	idxOp := indexOpPool.Get().(*indexOp)
	idxOp.im = io.im
	defer func() {
		idxOp.reset()
		indexOpPool.Put(idxOp)
	}()
	if err := idxOp.decode(datum); err != nil {
		return fmt.Errorf("decoding index op: %w", err)
	}
	io.Writes = append(io.Writes, idxOp.Writes...)
	io.Deletes = append(io.Deletes, idxOp.Deletes...)
	return nil
}

// NewIndexManager creates a new IndexManager.
// pebble.DB is passed in for indexes that need direct access to the DB for backfills and for record keeping of
// how far they've indexed.
//
// TODO (ajr) Indexes are inherently tied to a byte range in a sharded environment.
// During a split we want to start rebuilding the new indexes for the new range but the
// data that's already in the DB might collide. We need to have a way to distinguish
// between the two ranges. For now we assume that the IndexManager is only managing
// indexes for a single byte range.
func NewIndexManager(
	logger *zap.Logger,
	antflyConfig *common.Config,
	db *pebble.DB,
	dir string,
	schema *schema.TableSchema,
	byteRange types.Range,
	cache *pebbleutils.Cache,
) (*IndexManager, error) {
	// Ensure the indexes directory exists before creating any indexes.
	// This is required during shard split when the destination directory
	// is newly created and doesn't have an indexes subdirectory yet.
	if err := os.MkdirAll(dir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, fmt.Errorf("creating indexes directory: %w", err)
	}

	writer, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("creating zstd writer: %w", err)
	}
	reader, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("creating zstd reader: %w", err)
	}
	im := &IndexManager{
		logger:       logger.Named("indexManager"),
		antflyConfig: antflyConfig,
		dir:          dir,
		db:           db,
		cache:        cache,
		schema:       schema,
		byteRange:    byteRange,
		indexes:      xsync.NewMap[string, Index](),

		zstdWriter: writer,
		zstdReader: reader,

		enrichers:  xsync.NewMap[string, context.CancelFunc](),
		pauseAckCh: make(chan struct{}, 1), // Buffered to prevent blocking plexer
	}
	im.pauseCond = sync.NewCond(&im.pauseMu)
	return im, nil
}

// CreateShadow creates a new IndexManager for a split-off byte range.
// The shadow shares the same Pebble DB but has its own index directory
// and byteRange. This is used during shard splits to pre-build indexes
// for the split-off range before the new shard is ready.
//
// The shadowDir should be a unique directory (typically under .shadow/)
// to avoid conflicts with the parent's indexes.
//
// The shadow IndexManager will only index keys within shadowRange.
// After creation, call RegisterIndex to add indexes that mirror the parent's
// configuration, then call StartPlexer to begin processing.
func (im *IndexManager) CreateShadow(shadowDir string, shadowRange types.Range) (*IndexManager, error) {
	return NewIndexManager(
		im.logger.Named("shadow"),
		im.antflyConfig,
		im.db,
		shadowDir,
		im.schema,
		shadowRange,
		im.cache,
	)
}

// RegisterIndexes registers multiple indexes from a config map.
// This is used to set up a shadow IndexManager with the same indexes as the parent.
// Each index is registered with the specified rebuild flag.
func (im *IndexManager) RegisterIndexes(configs map[string]indexes.IndexConfig, rebuild bool) error {
	var errs []error
	for name, config := range configs {
		if err := im.Register(name, rebuild, config); err != nil {
			errs = append(errs, fmt.Errorf("registering index %s: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to register some indexes: %v", errs)
	}
	return nil
}

func (im *IndexManager) CloseLeaderFactory() error {
	im.enricherMu.Lock()
	defer im.enricherMu.Unlock()
	if im.newEnricherSignal != nil {
		close(im.newEnricherSignal)
		im.newEnricherSignal = nil
	}
	if im.removeEnricherSignal != nil {
		close(im.removeEnricherSignal)
		im.removeEnricherSignal = nil
	}
	if im.enrichersEg != nil {
		im.enrichersEgCancel()
		if err := im.enrichersEg.Wait(); err != nil {
			im.enrichersEg = nil
			im.enrichersEgCancel = nil
			return fmt.Errorf("waiting for enrichers to finish: %w", err)
		}
		im.enrichersEg = nil
		im.enrichersEgCancel = nil
	}

	return nil
}

// LeaderFactory is a placeholder for leader-specific initialization.
// We should start up enrichers for each of the vector indexes here
// and we'll also need to have some mechanism to propose those changes back
// to the raft group (up into dbWrapper)
//
// The embeddings indexes will have to only reindex embeddings and not
// send any writes to the enricher.
//
// TODO (ajr) I don't think this is what we want
// Let's have an enricher in the DBImpl that calls into the
// index manager to start the enrichers
func (im *IndexManager) StartLeaderFactory(ctx context.Context, persister PersistFunc) error {
	im.enricherMu.Lock()
	im.newEnricherSignal = make(chan EnrichableIndex, 10)
	im.removeEnricherSignal = make(chan EnrichableIndex, 10)
	var egCtx context.Context
	im.enrichersEg, egCtx = errgroup.WithContext(ctx)
	egCtx, im.enrichersEgCancel = context.WithCancel(egCtx)
	im.indexes.Range(func(key string, value Index) bool {
		if idx, ok := value.(EnrichableIndex); ok {
			ctx, cancel := context.WithCancel(egCtx)
			im.enrichers.Store(key, cancel)
			im.enrichersEg.Go(
				func() error {
					if err := idx.LeaderFactory(ctx, persister); err != nil &&
						!errors.Is(err, context.Canceled) {
						return fmt.Errorf("starting enricher for index %s: %w", key, err)
					}
					return nil
				})
		}
		return true
		// TODO (ajr) Start other enrichers that need it in the Open calls
	})
	im.enricherMu.Unlock()
Outer:
	for {
		select {
		case <-egCtx.Done():
			break Outer
		case enrichableIndex, ok := <-im.removeEnricherSignal:
			if !ok {
				// Channel closed, stop processing
				break Outer
			}
			if cancelFunc, ok := im.enrichers.LoadAndDelete(enrichableIndex.Name()); ok {
				cancelFunc()
			}
		case enrichableIndex, ok := <-im.newEnricherSignal:
			if !ok {
				// Channel closed, stop processing
				break Outer
			}
			// This is a signal to start an enricher for a new index
			ctx, cancel := context.WithCancel(egCtx)
			im.enrichers.Store(enrichableIndex.Name(), cancel)
			im.enrichersEg.Go(
				func() error {
					if err := enrichableIndex.LeaderFactory(ctx, persister); err != nil && !errors.Is(err, context.Canceled) {
						return fmt.Errorf("starting enricher for index %s: %w", enrichableIndex.Name(), err)
					}
					return nil
				})
		}
	}
	if err := im.CloseLeaderFactory(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("closing leader factory: %w", err)
	}
	return nil
}

func (im *IndexManager) HasIndex(name string) bool {
	_, ok := im.indexes.Load(name)
	return ok
}

func (im *IndexManager) GetIndex(name string) Index {
	idx, ok := im.indexes.Load(name)
	if !ok {
		return nil
	}
	return idx
}

func (im *IndexManager) Close(ctx context.Context) error {
	// Check if already closed to make Close() idempotent
	if im.plexerEg == nil {
		return nil
	}

	eg, _ := errgroup.WithContext(ctx)
	im.indexes.Range(func(key string, index Index) bool {
		eg.Go(func() error {
			if err := index.Close(); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("closing index %s: %w", index.Name(), err)
			}
			return nil
		})
		return true
	})
	im.logger.Debug("IndexManager.Close() called, cancelling plexer context")
	im.plexerEgCancel()
	if err := im.CloseLeaderFactory(); err != nil && !errors.Is(err, context.Canceled) {
		im.logger.Error("failed to close leader factory", zap.Error(err))
	}
	// Close WAL to unblock any pending dequeue operations
	if err := im.walBuf.Close(); err != nil && !errors.Is(err, inflight.ErrBufferClosed) {
		im.logger.Error("failed to close wal", zap.Error(err))
	}
	// Wait for plexer goroutine to finish before cleaning up channel
	if im.plexerEg != nil {
		im.logger.Debug("Waiting for plexer goroutine to finish")
		if err := im.plexerEg.Wait(); err != nil && !errors.Is(err, context.Canceled) &&
			!errors.Is(err, inflight.ErrBufferClosed) {
			im.logger.Error("plexer goroutine failed", zap.Error(err))
		}
		im.logger.Debug("Plexer goroutine finished")
		// Clear the plexerEg so Start() can be called again
		im.plexerEg = nil
	}
	if err := eg.Wait(); err != nil && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, inflight.ErrBufferClosed) {
		im.logger.Error("failed to close all indexes", zap.Error(err))
	}
	im.zstdReader.Close()
	return nil
}

func (im *IndexManager) Batch(
	ctx context.Context,
	writes [][2][]byte,
	deletes [][]byte,
	syncLevel Op_SyncLevel,
) error {
	if len(writes) == 0 && len(deletes) == 0 {
		return nil
	}

	switch syncLevel {
	case Op_SyncLevelPropose, Op_SyncLevelWrite:
		// Async path - enqueue to WAL for background processing.
		// Zero out values because we look them up by key later from Pebble.
		for i := range writes {
			writes[i][1] = nil
		}
		// Fall through to enqueue logic below

	case Op_SyncLevelFullText, Op_SyncLevelEmbeddings:
		// Create indexOp with sync level (FullText or Embeddings).
		// Copy writes into pooled indexOp, then zero values on the copy
		// (not the caller's slice) since we look them up by key later.
		io := indexOpPool.Get().(*indexOp)
		io.im = im
		io.Writes = append(io.Writes[:0], writes...)
		for i := range io.Writes {
			io.Writes[i][1] = nil
		}
		io.Deletes = append(io.Deletes[:0], deletes...)
		io.SyncLevel = syncLevel
		defer func() {
			io.reset()
			indexOpPool.Put(io)
		}()
		// im.logger.Debug("Executing index manager batch with sync level",
		// 	zap.Int("numWrites", len(writes)),
		// 	zap.Int("numDeletes", len(deletes)),
		// 	zap.Int32("syncLevel", int32(syncLevel)))
		// Execute - will sync specified index type, async others
		if err := io.Execute(ctx); err != nil {
			return err
		}
		return ErrPartialSuccess // Indexes queued in WAL but not fully applied

	default:
		return fmt.Errorf("invalid sync level: %d", syncLevel)
	}

	partitionedBatch := make([][]byte, 0, (len(writes)+len(deletes))/indexManagerPartitionSize+1)
	partitionCounts := make([]uint32, 0, cap(partitionedBatch))
	io := indexOpPool.Get().(*indexOp)
	io.im = im
	defer func() {
		io.reset()
		indexOpPool.Put(io)
	}()

	buf := bytes.NewBuffer(nil)
	writeIdx, deleteIdx := 0, 0

	for writeIdx < len(writes) || deleteIdx < len(deletes) {
		io.Writes = io.Writes[:0]
		io.Deletes = io.Deletes[:0]
		buf.Reset()

		// Fill partition with writes
		if writeIdx < len(writes) {
			count := indexManagerPartitionSize
			end := min(writeIdx+count, len(writes))
			io.Writes = append(io.Writes, writes[writeIdx:end]...)
			writeIdx = end
		}

		// Fill remaining space in partition with deletes
		if deleteIdx < len(deletes) {
			remaining := indexManagerPartitionSize - len(io.Writes)
			if remaining > 0 {
				end := min(deleteIdx+remaining, len(deletes))
				io.Deletes = append(io.Deletes, deletes[deleteIdx:end]...)
				deleteIdx = end
			}
		}

		if len(io.Writes) == 0 && len(io.Deletes) == 0 {
			break
		}

		b, err := io.encode(buf)
		if err != nil {
			return fmt.Errorf("encoding index operation: %w", err)
		}
		partitionedBatch = append(partitionedBatch, slices.Clone(b))
		partitionCounts = append(partitionCounts, uint32(len(io.Writes)+len(io.Deletes))) //nolint:gosec // G115: partition size bounded by config
	}

	if err := im.walBuf.EnqueueBatch(partitionedBatch, partitionCounts); err != nil {
		return err
	}

	// Signal dequeue AFTER enqueuing to avoid race condition
	select {
	case im.enqueueChan <- struct{}{}:
	default:
	}

	return nil
}

// ComputeEnrichments calls ComputeEnrichments on all enrichable indexes
// Returns all enrichment writes (embeddings, summaries, chunks) to be included in the batch
// This is used for SyncLevelEnrichments to pre-compute enrichments before Raft proposal
func (im *IndexManager) ComputeEnrichments(
	ctx context.Context,
	writes [][2][]byte,
) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error) {
	var allEnrichments [][2][]byte
	var allFailedKeys [][]byte

	// Iterate through all indexes
	im.indexes.Range(func(idxName string, idx Index) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		// Check if index implements EnrichmentComputer
		enrichmentComputer, ok := idx.(indexes.EnrichmentComputer)
		if !ok {
			return true // Continue to next index
		}

		// Call ComputeEnrichments on this index
		enrichments, failed, err := enrichmentComputer.ComputeEnrichments(ctx, writes)
		if err != nil {
			im.logger.Warn("Failed to compute enrichments for index",
				zap.String("index", idxName),
				zap.Error(err))
			// Continue with other indexes even if one fails
			return true
		}

		if len(enrichments) > 0 {
			im.logger.Debug("Computed enrichments for index",
				zap.String("index", idxName),
				zap.Int("count", len(enrichments)))
			allEnrichments = append(allEnrichments, enrichments...)
		}

		if len(failed) > 0 {
			allFailedKeys = append(allFailedKeys, failed...)
		}

		return true
	})

	return allEnrichments, allFailedKeys, nil
}

func (im *IndexManager) Register(name string, rebuild bool, config indexes.IndexConfig) error {
	// Acquire read lock to allow concurrent registrations but block during pause
	im.registerMu.RLock()
	defer im.registerMu.RUnlock()

	if name == "" {
		return errors.New("index name cannot be empty")
	}

	index, err := indexes.MakeIndex(
		im.logger.Named(name),
		im.antflyConfig,
		im.db,
		im.dir,
		name,
		&config,
		im.cache,
	)
	if err != nil {
		return fmt.Errorf("creating index %s: %w", name, err)
	}
	if _, loaded := im.indexes.LoadOrStore(name, index); loaded {
		return fmt.Errorf("index %s already registered", name)
	}
	im.enricherMu.Lock()
	if idx, ok := index.(EnrichableIndex); ok {
		if idx.NeedsEnricher() && im.newEnricherSignal != nil {
			im.newEnricherSignal <- idx
		}
	}
	im.enricherMu.Unlock()

	im.logger.Info("Opening index at registration", zap.String("name", name))
	if err := index.Open(rebuild, im.schema, im.byteRange); err != nil {
		im.indexes.Delete(name)
		return fmt.Errorf("opening index %s: %w", name, err)
	}
	im.logger.Info("Registered index", zap.String("name", name))
	return nil
}

func (im *IndexManager) Start(rebuild bool) (err error) {
	// Prevent starting twice without calling Close() first
	if im.plexerEg != nil {
		return fmt.Errorf("IndexManager already started, must call Close() first")
	}
	if rebuild {
		_ = os.RemoveAll(im.dir + "/indexManagerWAL")
	}
	walBuf, err := inflight.NewWALBufferWithOptions(im.logger, im.dir, "indexManagerWAL", inflight.WALBufferOptions{
		MaxRetries: 10,
		Metrics:    indexes.NewPrometheusWALMetrics("index_manager"),
	})
	if err != nil {
		return fmt.Errorf("creating WAL buffer: %w", err)
	}
	im.walBuf = walBuf
	im.enqueueChan = make(chan struct{}, 5)
	var ctx context.Context
	ctx, im.plexerEgCancel = context.WithCancel(context.TODO())
	im.plexerEg, im.plexerEgCtx = errgroup.WithContext(ctx)
	im.plexerEg.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				im.logger.Error("panic in index plexer goroutine",
					zap.Any("panic", r),
					zap.String("stack", string(debug.Stack())))
				panic(r)
			}
		}()
		defaultWait := 30 * time.Second
		t := time.NewTimer(defaultWait)
		ops := &indexOp{
			im:      im,
			Writes:  make([][2][]byte, 0, 100),
			Deletes: make([][]byte, 0, 100),
		}
		defer t.Stop()
		dequeue := func() error {
			ops.reset()
			ops.im = im
			// im.logger.Debug("Dequeueing index manager batch from WAL buffer")
			if err := im.walBuf.Dequeue(im.plexerEgCtx, ops, 10); err != nil {
				if errors.Is(err, inflight.ErrBufferClosed) {
					return err
				}
				im.logger.Warn("Failed to dequeue from WAL buffer", zap.Error(err))
			}
			empty := len(ops.Writes) == 0 && len(ops.Deletes) == 0
			if empty {
				// im.logger.Debug("No operations to process from WAL buffer, resetting timer")
				t.Reset(defaultWait)
			} else {
				// im.logger.Debug("Dequeued index manager batch", zap.Int("numWrites", len(ops.Writes)), zap.Int("numDeletes", len(ops.Deletes)))
				t.Reset(500 * time.Millisecond)
			}
			return nil
		}
		for {
			// Check if paused at start of each iteration (no mutex needed)
			if im.paused.Load() {
				// Send acknowledgment without holding any mutex
				select {
				case im.pauseAckCh <- struct{}{}:
				default:
					// Already acknowledged
				}

				// Now wait for resume
				im.pauseMu.Lock()
				for im.paused.Load() {
					// Send ack before each wait in case Pause() was called again
					select {
					case im.pauseAckCh <- struct{}{}:
					default:
					}
					im.pauseCond.Wait()
				}
				im.pauseMu.Unlock()
			}

			select {
			case <-im.plexerEgCtx.Done():
				return im.plexerEgCtx.Err()
			case _, ok := <-im.enqueueChan:
				if !ok {
					// Channel closed, shutdown
					return nil
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
	return err
}

func (im *IndexManager) Search(ctx context.Context, name string, request any) (any, error) {
	index, ok := im.indexes.Load(name)
	if !ok {
		return nil, fmt.Errorf("index %s not registered", name)
	}
	return index.Search(ctx, request)
}

// DeleteKeys prunes existing documents from every registered index synchronously.
// Unlike the normal WAL-backed batch path, this bypasses byte-range filtering so
// split finalization can remove keys that were already moved out of the shard range.
func (im *IndexManager) DeleteKeys(ctx context.Context, deletes [][]byte) error {
	if len(deletes) == 0 {
		return nil
	}

	return im.deleteGroupedKeys(ctx, groupDeleteKeys(deletes))
}

func (im *IndexManager) deleteGroupedKeys(ctx context.Context, groupedDeletes deleteKeyGroups) error {
	var batchErr error
	im.indexes.Range(func(name string, idx Index) bool {
		select {
		case <-ctx.Done():
			batchErr = ctx.Err()
			return false
		default:
		}

		indexType := idx.Type()
		selectedDeletes := deletesForIndex(name, indexType, groupedDeletes)
		switch {
		case indexes.IsFullTextType(indexType):
			if len(groupedDeletes.documents) > 0 {
				splitPruneRoutedDeleteKeysTotal.WithLabelValues("full_text", "document").
					Add(float64(len(groupedDeletes.documents)))
			}
		case indexes.IsEmbeddingsType(indexType):
			if len(groupedDeletes.documents) > 0 {
				splitPruneRoutedDeleteKeysTotal.WithLabelValues("embeddings", "document").
					Add(float64(len(groupedDeletes.documents)))
			}
			if chunks := groupedDeletes.chunksByName[name]; len(chunks) > 0 {
				splitPruneRoutedDeleteKeysTotal.WithLabelValues("embeddings", "chunk").
					Add(float64(len(chunks)))
			}
		case indexes.IsGraphType(indexType):
			if edges := groupedDeletes.edgesByName[name]; len(edges) > 0 {
				splitPruneRoutedDeleteKeysTotal.WithLabelValues("graph", "edge").
					Add(float64(len(edges)))
			}
		default:
			if len(selectedDeletes) > 0 {
				splitPruneRoutedDeleteKeysTotal.WithLabelValues(indexTypeMetricLabel(indexType), "all").
					Add(float64(len(selectedDeletes)))
			}
		}
		for start := 0; start < len(selectedDeletes); start += indexManagerPartitionSize {
			end := min(start+indexManagerPartitionSize, len(selectedDeletes))
			if err := idx.Batch(ctx, nil, selectedDeletes[start:end], true); err != nil {
				batchErr = fmt.Errorf("deleting keys from index %s: %w", name, err)
				return false
			}
		}
		return true
	})
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (im *IndexManager) Unregister(name string) error {
	index, ok := im.indexes.LoadAndDelete(name)
	if !ok {
		return fmt.Errorf("index %s not registered", name)
	}
	im.enricherMu.Lock()
	if idx, ok := index.(EnrichableIndex); ok {
		if idx.NeedsEnricher() && im.removeEnricherSignal != nil {
			im.removeEnricherSignal <- idx
		}
	}
	im.enricherMu.Unlock()
	if err := index.Close(); err != nil {
		return fmt.Errorf("closing index %s: %w", name, err)
	}
	if err := index.Delete(); err != nil {
		return fmt.Errorf("deleting index %s: %w", name, err)
	}
	return nil
}

func (im *IndexManager) Stats() map[string]indexes.IndexStats {
	stats := make(map[string]indexes.IndexStats, im.indexes.Size())
	im.indexes.Range(func(name string, idx Index) bool {
		stats[name] = idx.Stats()
		return true
	})
	return stats
}

func (im *IndexManager) UpdateSchema(newSchema *schema.TableSchema) error {
	im.schema = newSchema
	var eg errgroup.Group
	im.indexes.Range(func(name string, idx Index) bool {
		eg.Go(func() error {
			if err := idx.UpdateSchema(newSchema); err != nil {
				return fmt.Errorf("updating schema for index %s: %w", name, err)
			}
			return nil
		})
		return true
	})
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("updating schema: %w", err)
	}
	return nil
}

func (im *IndexManager) UpdateRange(byteRange types.Range) error {
	im.byteRange = byteRange
	var eg errgroup.Group
	im.indexes.Range(func(name string, idx Index) bool {
		eg.Go(func() error {
			if err := idx.UpdateRange(byteRange); err != nil {
				return fmt.Errorf("updating range for index %s: %w", name, err)
			}
			return nil
		})
		return true
	})
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("updating range: %w", err)
	}
	return nil
}

// WaitForBackfills waits for all backfillable indexes to complete their backfill.
// Returns early if context is canceled.
func (im *IndexManager) WaitForBackfills(ctx context.Context) {
	im.indexes.Range(func(name string, idx Index) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		if backfillable, ok := idx.(indexes.BackfillableIndex); ok {
			im.logger.Debug("Waiting for index backfill to complete", zap.String("index", name))
			backfillable.WaitForBackfill(ctx)
			im.logger.Debug("Index backfill completed", zap.String("index", name))
		}
		return true
	})
}

// Pause pauses the IndexManager and all its indexes to prepare for snapshot.
// This method:
// 1. Acquires registerMu to block new index registrations
// 2. Sets paused flag and signals plexer to acknowledge
// 3. Pauses each registered index
// 4. Syncs the WAL buffer
//
// The registerMu lock is held until Resume() is called to prevent new indexes
// from being registered during snapshot.
func (im *IndexManager) Pause(ctx context.Context) error {
	im.pauseMu.Lock()

	// Already paused
	if im.paused.Load() {
		im.pauseMu.Unlock()
		return nil
	}

	im.logger.Info("Pausing IndexManager")

	// Acquire registerMu to block new registrations
	im.registerMu.Lock()

	// Set paused flag
	im.paused.Store(true)

	// Drain any stale acknowledgments from the channel before signaling
	select {
	case <-im.pauseAckCh:
		// Drained stale ack
	default:
		// No stale ack
	}

	// Release pauseMu BEFORE waiting for ack to avoid deadlock with plexer
	im.pauseMu.Unlock()

	// Signal plexer to wake up and check pause flag
	// Use context timeout to avoid blocking forever if enqueueChan is full
	select {
	case im.enqueueChan <- struct{}{}:
	case <-ctx.Done():
		im.paused.Store(false)
		im.registerMu.Unlock()
		return fmt.Errorf("pause timeout: %w", ctx.Err())
	}

	// Wait for plexer acknowledgment via separate channel (with context timeout)
	select {
	case <-im.pauseAckCh:
		// Plexer acknowledged pause
	case <-ctx.Done():
		im.paused.Store(false)
		im.registerMu.Unlock()
		return fmt.Errorf("pause timeout: %w", ctx.Err())
	}

	// Pause each registered index
	var pausedIndexes []Index
	var pauseErr error

	im.indexes.Range(func(name string, idx Index) bool {
		select {
		case <-ctx.Done():
			pauseErr = ctx.Err()
			return false
		default:
		}

		im.logger.Debug("Pausing index", zap.String("index", name))
		if err := idx.Pause(ctx); err != nil {
			im.logger.Error("Failed to pause index", zap.String("index", name), zap.Error(err))
			pauseErr = fmt.Errorf("pausing index %s: %w", name, err)
			return false
		}
		pausedIndexes = append(pausedIndexes, idx)
		return true
	})

	// If any index failed to pause, resume already-paused indexes
	if pauseErr != nil {
		im.logger.Warn("Pause failed, resuming already-paused indexes", zap.Error(pauseErr))
		for _, idx := range pausedIndexes {
			idx.Resume()
		}
		im.paused.Store(false)
		im.pauseCond.Broadcast()
		im.registerMu.Unlock()
		return pauseErr
	}

	// Sync the WAL buffer
	if err := im.walBuf.Sync(); err != nil {
		im.logger.Error("Failed to sync WAL during pause", zap.Error(err))
		// Continue despite sync error - pause is still effective
	}

	im.logger.Info("IndexManager paused successfully")
	return nil
}

// Resume resumes the IndexManager and all its indexes after snapshot.
// This releases the registerMu lock to allow new index registrations.
func (im *IndexManager) Resume() {
	im.pauseMu.Lock()
	defer im.pauseMu.Unlock()

	if !im.paused.Load() {
		return
	}

	im.logger.Info("Resuming IndexManager")

	// Resume each registered index
	im.indexes.Range(func(name string, idx Index) bool {
		im.logger.Debug("Resuming index", zap.String("index", name))
		idx.Resume()
		return true
	})

	// Clear paused flag
	im.paused.Store(false)

	// Broadcast to wake plexer
	im.pauseCond.Broadcast()

	// Release registerMu to allow new registrations
	im.registerMu.Unlock()

	im.logger.Info("IndexManager resumed successfully")
}

// GetDir returns the index directory path.
func (im *IndexManager) GetDir() string {
	return im.dir
}
