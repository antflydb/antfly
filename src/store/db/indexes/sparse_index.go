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
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/sparseindex"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"

	"github.com/antflydb/antfly/lib/logger"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var _ Index = (*SparseIndex)(nil)
var _ EnrichableIndex = (*SparseIndex)(nil)
var _ EnrichmentComputer = (*SparseIndex)(nil)

// SparseIndex implements Index and EnrichableIndex for sparse embeddings.
type SparseIndex struct {
	sync.Mutex

	db           *pebble.DB
	indexDB      *pebble.DB // Separate Pebble instance for sparse inverted index
	indexPath    string
	logger       *zap.Logger
	antflyConfig *common.Config

	enricherMu   sync.RWMutex
	enricher     SparseEnricher // SparseEnricher (standalone) or pipelineSparseEnricher (pipeline)
	embedderConf *embeddings.EmbedderConfig

	summarizerConf   *ai.GeneratorConfig
	summarizerSuffix []byte
	chunkerSuffix    []byte

	searchEmbMu sync.Mutex
	searchEmb   embeddings.SparseEmbedder

	indexingField  string
	promptTemplate string

	cache     *pebbleutils.Cache
	byteRange types.Range
	schema    *schema.TableSchema

	conf EmbeddingsIndexConfig

	egCancel context.CancelFunc
	egCtx    context.Context
	eg       *errgroup.Group
	walBuf   *inflight.WALBuffer

	enqueueChan chan int

	sparseIdx *sparseindex.SparseIndex
	name      string

	suffix       []byte
	sparseSuffix []byte

	backfillDone chan struct{}
	backfillTracker

	// Pause/Resume support
	pauseMu    sync.Mutex
	pauseCond  *sync.Cond
	pauseAckCh chan struct{}
	paused     atomic.Bool
}

// SparseEnricher is the interface for sparse enrichment workers.
type SparseEnricher interface {
	EnrichBatch(ctx context.Context, keys [][]byte) error
	EnricherStats() EnricherStats
	Close() error
}

// SparseVec is a JSON-serializable sparse vector for wire format (inter-node RPC).
// Internally, code should use *vector.SparseVector (protobuf type) instead.
type SparseVec struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

// ToVector converts the wire-format SparseVec to a protobuf *vector.SparseVector.
func (sv SparseVec) ToVector() *vector.SparseVector {
	return vector.NewSparseVector(sv.Indices, sv.Values)
}

// SparseVecFromVector converts a protobuf *vector.SparseVector to the wire-format SparseVec.
func SparseVecFromVector(v *vector.SparseVector) SparseVec {
	return SparseVec{
		Indices: v.GetIndices(),
		Values:  v.GetValues(),
	}
}

// SparseSearchRequest is the query type for sparse embeddings indexes.
type SparseSearchRequest struct {
	// QueryText is the text to embed into a sparse vector for search.
	QueryText string
	// QueryVec is a pre-computed sparse vector (used when QueryText is empty).
	QueryVec *vector.SparseVector
	// K is the number of results to return.
	K int
	// FilterPrefix filters results by key prefix.
	FilterPrefix string
	// FilterIDs restricts results to these document keys.
	FilterIDs []string
}

func NewSparseIndex(
	logger *zap.Logger,
	antflyConfig *common.Config,
	db *pebble.DB,
	dir string,
	name string,
	config *IndexConfig,
	cache *pebbleutils.Cache,
) (Index, error) {
	if config == nil {
		return nil, errors.New("sparse index config is required")
	}
	c, err := config.AsEmbeddingsIndexConfig()
	if err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if c.Template == "" && c.Field == "" {
		return nil, errors.New("field or template must be specified")
	}

	indexSuffix := fmt.Appendf(nil, ":i:%s", name)
	indexPath := filepath.Join(dir, name)

	var chunkerSuffix []byte
	if c.Chunker != nil {
		if chunking.GetFullTextIndex(*c.Chunker) != nil {
			chunkerSuffix = fmt.Appendf(bytes.Clone(indexSuffix), ":0%s", storeutils.ChunkingFullTextSuffix)
		} else {
			chunkerSuffix = fmt.Appendf(bytes.Clone(indexSuffix), ":0%s", storeutils.ChunkingSuffix)
		}
	}

	si := &SparseIndex{
		db:               db,
		antflyConfig:     antflyConfig,
		indexPath:        indexPath,
		name:             name,
		suffix:           indexSuffix,
		sparseSuffix:     fmt.Appendf(bytes.Clone(indexSuffix), "%s", storeutils.SparseSuffix),
		summarizerSuffix: fmt.Appendf(bytes.Clone(indexSuffix), "%s", storeutils.SummarySuffix),
		chunkerSuffix:    chunkerSuffix,
		logger:           logger,
		indexingField:    c.Field,
		promptTemplate:   c.Template,
		cache:            cache,
		embedderConf:     c.Embedder,
		summarizerConf:   c.Summarizer,
		conf:             c,
		pauseAckCh:       make(chan struct{}, 1),
	}

	si.pauseCond = sync.NewCond(&si.pauseMu)
	si.egCtx, si.egCancel = context.WithCancel(context.TODO())
	si.eg, si.egCtx = errgroup.WithContext(si.egCtx)
	return si, nil
}

func (si *SparseIndex) NeedsEnricher() bool {
	return si.embedderConf != nil
}

func (si *SparseIndex) Name() string {
	return si.name
}

func (si *SparseIndex) Type() IndexType {
	return IndexTypeEmbeddings
}

// GetDimension returns 0 for sparse indexes (variable-length vectors).
func (si *SparseIndex) GetDimension() int {
	return 0
}

// RenderPrompt extracts and renders the prompt from a document.
func (si *SparseIndex) RenderPrompt(doc map[string]any) (string, uint64, error) {
	return si.ExtractPrompt(doc)
}

func (si *SparseIndex) Open(
	rebuild bool,
	tableSchema *schema.TableSchema,
	byteRange types.Range,
) error {
	si.Lock()
	defer si.Unlock()
	si.byteRange = byteRange
	si.schema = tableSchema
	si.logger.Debug("Opening sparse index", zap.String("path", si.indexPath))

	if rebuild {
		_ = os.RemoveAll(si.indexPath)
	}

	// Create index directory
	if err := os.MkdirAll(si.indexPath, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return fmt.Errorf("creating sparse index directory: %w", err)
	}

	// Create WAL buffer for async enrichment
	var err error
	if si.walBuf, err = inflight.NewWALBufferWithOptions(si.logger, si.indexPath, "indexWAL", inflight.WALBufferOptions{
		MaxRetries: 10,
		Metrics:    NewPrometheusWALMetrics("sparse"),
	}); err != nil {
		return fmt.Errorf("creating WALBuffer: %w", err)
	}

	// Open separate Pebble instance for sparse index data
	reg := pebbleutils.NewRegistry()
	sparseindex.RegisterChunkMerger(reg, nil) // no prefix — dedicated DB
	pebbleOpts := &pebble.Options{
		Logger:          &logger.NoopLoggerAndTracer{},
		LoggerAndTracer: &logger.NoopLoggerAndTracer{},
		Merger:          reg.NewMerger("antfly.merge.v1"),
	}
	si.cache.Apply(pebbleOpts, 20*1024*1024) // 20MB fallback

	si.indexDB, err = pebble.Open(filepath.Join(si.indexPath, "sparsedb"), pebbleOpts)
	if err != nil {
		return fmt.Errorf("opening sparse index database: %w", err)
	}

	// Create sparse index instance
	chunkSize := si.conf.ChunkSize
	if chunkSize <= 0 {
		chunkSize = sparseindex.DefaultChunkSize
	}
	si.sparseIdx = sparseindex.New(si.indexDB, sparseindex.Config{
		ChunkSize: chunkSize,
		UseMerge:  true,
	})

	// Start background goroutines for WAL processing
	backfillWait := make(chan struct{})
	si.backfillDone = make(chan struct{})
	si.enqueueChan = make(chan int, 5)

	si.eg.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered from panic:", r)
				fmt.Println("Stack trace:\n", string(debug.Stack()))
				panic(r)
			}
		}()
		return si.runPlexer(si.egCtx, backfillWait)
	})

	si.eg.Go(func() error {
		defer close(si.backfillDone)
		if rebuild {
			return si.runBackfill(si.egCtx, backfillWait, nil)
		}
		close(backfillWait)
		return nil
	})

	return nil
}

func (si *SparseIndex) runPlexer(ctx context.Context, backfillWait chan struct{}) error {
	// Wait for backfill to start before processing WAL
	select {
	case <-backfillWait:
	case <-ctx.Done():
		return ctx.Err()
	}

	const maxJitter = time.Millisecond * 200
	jitter := maxJitter - rand.N(maxJitter) //nolint:gosec // G404: non-security randomness for ML/jitter
	t := time.NewTimer(DefaultFlushTime + jitter)
	enqueueCounter := 0
	op := &sparseOp{si: si}

	dequeue := func() error {
		enqueueCounter = 0

		// Check pause state
		si.pauseMu.Lock()
		if si.paused.Load() {
			select {
			case si.pauseAckCh <- struct{}{}:
			default:
			}
			for si.paused.Load() {
				si.pauseCond.Wait()
			}
		}
		si.pauseMu.Unlock()

		op.reset()
		if err := si.walBuf.Dequeue(ctx, op, enricherMaxBatches); err != nil {
			if errors.Is(err, inflight.ErrBufferClosed) || errors.Is(err, context.Canceled) {
				return err
			}
			si.logger.Error("Dequeue error in sparse plexer", zap.Error(err))
		}
		empty := len(op.keys) == 0
		if empty {
			t.Reset(DefaultFlushTime + jitter)
		} else {
			t.Reset(100*time.Millisecond + jitter)
		}
		return nil
	}

	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case n := <-si.enqueueChan:
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

func (si *SparseIndex) runBackfill(ctx context.Context, backfillWait chan struct{}, rebuildFrom []byte) error {
	defer close(backfillWait)

	si.startBackfill()
	defer si.finishBackfill()

	maxBatchSize := enricherMaxBatches * enricherPartitionSize
	totalProcessed := 0

	err := storeutils.ScanForEnrichment(ctx, si.db, storeutils.EnrichmentScanOptions{
		ByteRange:        si.byteRange,
		PrimarySuffix:    nil, // defaults to DBRangeStart (documents)
		EnrichmentSuffix: si.sparseSuffix,
		BatchSize:        maxBatchSize,
		ProcessBatch: func(ctx context.Context, batch []storeutils.DocumentScanState) error {
			keys := make([][]byte, len(batch))
			for i, state := range batch {
				keys[i] = state.CurrentDocKey
			}
			if err := si.enrichBatch(ctx, keys); err != nil {
				si.logger.Error("Backfill batch error", zap.Error(err))
			}
			totalProcessed += len(batch)
			si.addBackfillItems(len(batch))
			if len(batch) > 0 {
				lastKey := batch[len(batch)-1].CurrentDocKey
				si.updateBackfillProgress(estimateProgress(si.byteRange[0], si.byteRange[1], lastKey))
			}
			si.logger.Debug("Processed batch during sparse backfill",
				zap.Int("batchSize", len(batch)),
				zap.Int("totalProcessed", totalProcessed))
			return nil
		},
	})

	if err != nil {
		return fmt.Errorf("scanning for sparse enrichment: %w", err)
	}

	si.logger.Debug("Finished sparse backfill",
		zap.Stringer("range", si.byteRange),
		zap.String("path", si.indexPath),
		zap.Int("totalProcessed", totalProcessed))
	return nil
}

func (si *SparseIndex) enrichBatch(ctx context.Context, keys [][]byte) error {
	si.enricherMu.RLock()
	e := si.enricher
	si.enricherMu.RUnlock()
	if e == nil {
		return nil
	}
	return e.EnrichBatch(ctx, keys)
}

func (si *SparseIndex) Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte, syncIndex bool) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	if len(writes) == 0 && len(deletes) == 0 {
		return nil
	}

	var sparseInserts []sparseindex.BatchInsert
	var sparseDeletes [][]byte
	var promptKeys [][]byte

	needsPipeline := si.summarizerConf != nil || si.conf.Chunker != nil

	for _, write := range writes {
		key := write[0]

		// Check if this is an enriched sparse embedding key
		if bytes.HasSuffix(key, si.sparseSuffix) {
			// Read value from Pebble (IndexManager zeroes out write values)
			value, closer, err := si.db.Get(key)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					si.logger.Error("Failed to read sparse embedding from Pebble",
						zap.ByteString("key", key), zap.Error(err))
				}
				continue
			}
			if len(value) < 8 {
				_ = closer.Close()
				continue
			}

			// Decode: [hashID uint64][encoded SparseVector]
			// Skip hashID (first 8 bytes), decode sparse vector
			sparseData := value[8:]
			sv, decErr := decodeSparseVec(sparseData)
			if decErr != nil {
				_ = closer.Close()
				si.logger.Error("Failed to decode sparse vector",
					zap.ByteString("key", key), zap.Error(decErr))
				continue
			}
			_ = closer.Close()

			// Extract original document key from enrichment key
			docKey := extractDocKey(key, si.sparseSuffix)
			if docKey == nil {
				continue
			}

			sparseInserts = append(sparseInserts, sparseindex.BatchInsert{
				DocID: docKey,
				Vec:   sv,
			})
		} else if needsPipeline && bytes.HasSuffix(key, si.summarizerSuffix) {
			// Summary key written by SummarizeEnricher - route to enricher for embedding
			promptKeys = append(promptKeys, key)
		} else if needsPipeline && storeutils.IsChunkKey(key) && bytes.Contains(key, si.suffix) {
			// Chunk key for THIS index - route to enricher for summarization or embedding
			promptKeys = append(promptKeys, key)
		} else if !bytes.Contains(key, []byte(":i:")) {
			// Regular document write - may need enrichment
			promptKeys = append(promptKeys, key)
		}
	}

	for _, key := range deletes {
		if storeutils.IsChunkKey(key) {
			if bytes.Contains(key, si.suffix) {
				sparseDeletes = append(sparseDeletes, key)
			}
			continue
		}
		if !bytes.Contains(key, []byte(":i:")) {
			sparseDeletes = append(sparseDeletes, key)
		}
	}

	// Insert sparse vectors into the inverted index
	if len(sparseInserts) > 0 || len(sparseDeletes) > 0 {
		if si.sparseIdx != nil {
			if err := si.sparseIdx.Batch(sparseInserts, sparseDeletes); err != nil {
				return fmt.Errorf("batch sparse index: %w", err)
			}
		}
	}

	// Enqueue docs that need sparse embedding enrichment
	if len(promptKeys) > 0 && si.NeedsEnricher() {
		si.enricherMu.RLock()
		e := si.enricher
		si.enricherMu.RUnlock()

		if e != nil {
			if syncIndex {
				// SyncLevelAknn should have all enrichments pre-computed in
				// preEnrichBatch() before the Raft proposal. If we still have
				// promptKeys here with sync=true, it means enrichments weren't
				// pre-computed for some reason.
				//
				// We CANNOT call EnrichBatch() here because it causes a deadlock:
				// 1. Batch() is called during Raft apply
				// 2. EnrichBatch() -> persistFunc() tries to submit a NEW Raft proposal
				// 3. But we're already inside a Raft apply, so the new proposal deadlocks
				//
				// Enqueue to async enricher so embeddings eventually get generated
				si.logger.Error("SyncLevelAknn used but sparse enrichments not pre-computed",
					zap.Int("numKeys", len(promptKeys)),
					zap.String("index", si.name),
				)
			}
			// Enqueue for async enrichment via WAL
			keysJSON, err := json.Marshal(map[string]any{"keys": promptKeys})
			if err != nil {
				return fmt.Errorf("marshaling enrich keys: %w", err)
			}
			if err := si.walBuf.EnqueueBatch([][]byte{keysJSON}, []uint32{uint32(len(promptKeys))}); err != nil { //nolint:gosec // G115: slice length fits in uint32
				return fmt.Errorf("enqueueing sparse enrichment: %w", err)
			}
			select {
			case si.enqueueChan <- len(promptKeys):
			default:
			}
		}
	}

	return nil
}

func (si *SparseIndex) Search(ctx context.Context, query any) (any, error) {
	if query == nil {
		return nil, storeutils.ErrEmptyRequest
	}

	switch q := query.(type) {
	case *SparseSearchRequest:
		return si.searchSparse(ctx, q)
	case *vectorindex.SearchRequest:
		// For compatibility with hybrid search dispatch
		return si.searchFromVectorRequest(ctx, q)
	default:
		return nil, fmt.Errorf("invalid query type: %T, expected *SparseSearchRequest", query)
	}
}

func (si *SparseIndex) searchSparse(ctx context.Context, req *SparseSearchRequest) (*vectorindex.SearchResult, error) {
	if si.sparseIdx == nil {
		return nil, errors.New("sparse index not initialized")
	}

	var queryVec *vector.SparseVector

	if req.QueryVec != nil && len(req.QueryVec.GetIndices()) > 0 {
		queryVec = req.QueryVec
		si.logger.Debug("searchSparse: using pre-computed query vector",
			zap.Int("numIndices", len(queryVec.GetIndices())),
			zap.String("index", si.name))
	} else if req.QueryText != "" {
		// Generate sparse embedding from query text using cached embedder
		sparseEmb, err := si.getSearchEmbedder()
		if err != nil {
			return nil, err
		}

		vecs, err := sparseEmb.SparseEmbed(ctx, []string{req.QueryText})
		if err != nil {
			return nil, fmt.Errorf("generating sparse query embedding: %w", err)
		}
		if len(vecs) == 0 {
			return &vectorindex.SearchResult{}, nil
		}

		queryVec = vector.NewSparseVector(vecs[0].Indices, vecs[0].Values)
	} else {
		return nil, errors.New("query must specify QueryText or QueryVec")
	}

	k := req.K
	if k <= 0 {
		k = 10
	}

	si.logger.Debug("searchSparse: executing search",
		zap.Int("queryTerms", len(queryVec.GetIndices())),
		zap.Int("k", k),
		zap.Int("filterIDs", len(req.FilterIDs)),
		zap.String("index", si.name))

	result, err := si.sparseIdx.Search(queryVec, k, req.FilterIDs)
	if err != nil {
		return nil, fmt.Errorf("sparse search: %w", err)
	}

	si.logger.Debug("searchSparse: search complete",
		zap.Int("numHits", len(result.Hits)),
		zap.String("index", si.name))

	// Convert to vectorindex.SearchResult for fusion compatibility
	hits := make([]*vectorindex.SearchHit, len(result.Hits))
	for i, h := range result.Hits {
		hits[i] = &vectorindex.SearchHit{
			ID:    string(h.DocID),
			Score: float32(h.Score),
		}
	}

	return &vectorindex.SearchResult{
		Hits: hits,
		Status: &vectorindex.SearchStatus{
			Total:      1,
			Successful: 1,
		},
	}, nil
}

// searchFromVectorRequest handles a vectorindex.SearchRequest.
// Note: the primary search path in db.go creates a SparseSearchRequest with
// QueryText directly. This fallback exists for compatibility but cannot
// extract query text from a dense vector request.
func (si *SparseIndex) searchFromVectorRequest(ctx context.Context, req *vectorindex.SearchRequest) (*vectorindex.SearchResult, error) {
	return nil, errors.New("sparse embeddings search requires a SparseSearchRequest with QueryText or QueryVec; vectorindex.SearchRequest is not supported directly")
}

func (si *SparseIndex) Stats() IndexStats {
	if si.sparseIdx == nil {
		return EmbeddingsIndexStats{
			Error: "index not initialized",
		}.AsIndexStats()
	}

	stats := si.sparseIdx.Stats()
	var is EmbeddingsIndexStats
	if dc, ok := stats["doc_count"]; ok {
		if v, ok := dc.(uint64); ok {
			is.TotalIndexed = v
		}
	}

	// Index-level backfill tracking (from runBackfill)
	if si.backfilling.Load() {
		is.Rebuilding = true
		is.BackfillProgress = si.loadBackfillProgress()
		is.BackfillItemsProcessed = si.backfillItemsProcessed.Load()
	}

	// Enricher stats (WAL backlog, enricher-level backfill)
	si.enricherMu.RLock()
	e := si.enricher
	si.enricherMu.RUnlock()
	if e != nil {
		es := e.EnricherStats()
		is.Rebuilding = is.Rebuilding || es.Backfilling
		if es.Backfilling && (!si.backfilling.Load() || es.BackfillProgress < is.BackfillProgress) {
			is.BackfillProgress = es.BackfillProgress
		}
		is.BackfillItemsProcessed += es.BackfillItemsProcessed
		is.WalBacklog = uint64(es.WALBacklog) //nolint:gosec // G115: WALBacklog is non-negative
	}

	// Include index-level WAL backlog
	if si.walBuf != nil {
		is.WalBacklog += uint64(si.walBuf.Len()) //nolint:gosec // G115: Len() is non-negative
	}

	return is.AsIndexStats()
}

func (si *SparseIndex) Close() error {
	if si == nil {
		return nil
	}
	if si.egCancel == nil {
		return nil
	}
	si.egCancel()

	if si.eg != nil {
		if err := si.eg.Wait(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, inflight.ErrBufferClosed) {
				// Expected during shutdown
			} else {
				return fmt.Errorf("waiting for background tasks: %w", err)
			}
		}
	}

	if si.walBuf != nil {
		if err := si.walBuf.Close(); err != nil {
			return fmt.Errorf("closing WAL buffer: %w", err)
		}
	}

	if si.sparseIdx != nil {
		if err := si.sparseIdx.Close(); err != nil {
			return fmt.Errorf("closing sparse index: %w", err)
		}
	}

	if si.indexDB != nil {
		if err := si.indexDB.Close(); err != nil {
			si.logger.Warn("Error closing sparse indexDB", zap.Error(err))
		}
	}

	return nil
}

func (si *SparseIndex) Delete() error {
	if err := si.Close(); err != nil {
		return err
	}
	return os.RemoveAll(si.indexPath)
}

func (si *SparseIndex) UpdateRange(byteRange types.Range) error {
	si.Lock()
	defer si.Unlock()
	si.byteRange = byteRange
	return nil
}

func (si *SparseIndex) UpdateSchema(tableSchema *schema.TableSchema) error {
	si.Lock()
	defer si.Unlock()
	si.schema = tableSchema
	return nil
}

func (si *SparseIndex) Pause(ctx context.Context) error {
	// Atomically transition from unpaused to paused; no-op if already paused.
	if !si.paused.CompareAndSwap(false, true) {
		return nil
	}

	// Signal plexer to check pause
	select {
	case si.enqueueChan <- 0:
	default:
	}

	// Wait for plexer acknowledgment
	select {
	case <-si.pauseAckCh:
	case <-ctx.Done():
		si.paused.Store(false)
		return ctx.Err()
	}

	// Sync WAL
	if si.walBuf != nil {
		if err := si.walBuf.Sync(); err != nil {
			return fmt.Errorf("syncing WAL on pause: %w", err)
		}
	}

	return nil
}

func (si *SparseIndex) Resume() {
	si.pauseMu.Lock()
	si.paused.Store(false)
	si.pauseCond.Broadcast()
	si.pauseMu.Unlock()
}

func (si *SparseIndex) LeaderFactory(ctx context.Context, persistFunc PersistFunc) error {
	if si.embedderConf == nil {
		return nil
	}

	emb, err := embeddings.NewEmbedder(*si.embedderConf)
	if err != nil {
		return fmt.Errorf("creating embedder for sparse enricher: %w", err)
	}

	sparseEmb, ok := emb.(embeddings.SparseEmbedder)
	if !ok {
		return fmt.Errorf("embedder does not support sparse embedding")
	}

	// Define persist function that writes sparse embeddings through Raft
	persistSparse := func(ctx context.Context, writes [][2][]byte) error {
		return persistFunc(ctx, writes)
	}

	chunkingConfig := si.conf.Chunker
	needsPipeline := si.summarizerConf != nil || chunkingConfig != nil

	var enricher SparseEnricher

	if needsPipeline {
		// Create the sparse embedding enricher (terminal stage)
		sparseEnricher := NewSparseEmbeddingEnricher(
			si.logger,
			si.name,
			si.db,
			sparseEmb,
			persistSparse,
			si.sparseSuffix,
			si.indexingField,
			si.promptTemplate,
			si.byteRange,
		)
		sparseEnricher.sumSuffix = si.summarizerSuffix

		terminal := NewSparsePipelineAdapter(ctx, sparseEnricher)

		// Define persist summaries function
		var persistSummaries PersistSummariesFunc
		if si.summarizerConf != nil {
			persistSummaries = func(ctx context.Context, keys [][]byte, hashIDs []uint64, summaries []string) error {
				writes := make([][2][]byte, 0, len(keys))
				for j, k := range keys {
					if !si.byteRange.Contains(k) {
						return common.NewErrKeyOutOfRange(k, si.byteRange)
					}
					key := append(bytes.Clone(k), si.summarizerSuffix...)
					b := make([]byte, 0, len(summaries[j])+8)
					b = encoding.EncodeUint64Ascending(b, hashIDs[j])
					b = fmt.Append(b, summaries[j])
					writes = append(writes, [2][]byte{key, b})
				}
				return persistFunc(ctx, writes)
			}
		}

		// Define persist chunks function
		var persistChunks PersistChunksFunc
		if chunkingConfig != nil {
			persistChunks = func(ctx context.Context, keys [][]byte, hashIDs []uint64, chunks [][]chunking.Chunk) error {
				writes := make([][2][]byte, 0, len(keys)*2)
				for j, k := range keys {
					if !si.byteRange.Contains(k) {
						return common.NewErrKeyOutOfRange(k, si.byteRange)
					}
					for _, chunk := range chunks[j] {
						if len(strings.TrimSpace(chunk.GetText())) == 0 {
							continue
						}
						var chunkKey []byte
						if chunking.GetFullTextIndex(*chunkingConfig) != nil {
							chunkKey = storeutils.MakeChunkFullTextKey(k, si.name, chunk.Id)
						} else {
							chunkKey = storeutils.MakeChunkKey(k, si.name, chunk.Id)
						}
						chunkJSON, err := json.Marshal(chunk)
						if err != nil {
							return fmt.Errorf("marshaling chunk: %w", err)
						}
						b := make([]byte, 0, len(chunkJSON)+8)
						b = encoding.EncodeUint64Ascending(b, hashIDs[j])
						b = append(b, chunkJSON...)
						writes = append(writes, [2][]byte{chunkKey, b})
					}
				}
				return persistFunc(ctx, writes)
			}
		}

		storeChunks := false
		if chunkingConfig != nil {
			storeChunks = chunkingConfig.StoreChunks
		}

		// Use a zero EmbedderConfig since the terminal is pre-built
		pe, err := NewPipelineEnricher(ctx, PipelineEnricherConfig{
			Logger:           si.logger,
			AntflyConfig:     si.antflyConfig,
			DB:               si.db,
			Dir:              si.indexPath,
			Name:             si.name,
			EmbSuffix:        si.sparseSuffix,
			SumSuffix:        si.summarizerSuffix,
			ByteRange:        si.byteRange,
			Terminal:         terminal, // pre-built sparse terminal
			SummarizerConfig: si.summarizerConf,
			ChunkingConfig:   chunkingConfig,
			StoreChunks:      storeChunks,
			GeneratePrompts:  si.GeneratePrompts,
			PersistSummaries: persistSummaries,
			PersistChunks:    persistChunks,
			ExtractPrompt:    si.ExtractPrompt,
			PersistFunc:      persistFunc,
		})
		if err != nil {
			return fmt.Errorf("creating pipeline enricher for sparse index: %w", err)
		}

		enricher = &pipelineSparseEnricher{inner: pe}
	} else {
		// Simple sparse enricher (no chunking, no summarization)
		enricher = NewSparseEmbeddingEnricher(
			si.logger,
			si.name,
			si.db,
			sparseEmb,
			persistSparse,
			si.sparseSuffix,
			si.indexingField,
			si.promptTemplate,
			si.byteRange,
		)
	}

	si.enricherMu.Lock()
	// Clean up any stale enricher WALs from a previous leadership tenure
	CleanupEnricherWALs(si.indexPath)
	si.enricher = enricher
	si.enricherMu.Unlock()

	// Wait for context cancellation (leadership loss)
	<-ctx.Done()

	si.enricherMu.Lock()
	si.enricher = nil
	si.enricherMu.Unlock()

	err = enricher.Close()

	// Clean up enricher WALs — pending work will be rediscovered via backfill
	CleanupEnricherWALs(si.indexPath)

	return err
}

// ExtractPrompt extracts text from a document using the configured field or template.
// Returns the prompt string and a hash of the prompt for deduplication.
func (si *SparseIndex) ExtractPrompt(doc map[string]any) (string, uint64, error) {
	return extractSparsePrompt(si.promptTemplate, si.indexingField, doc)
}

// GeneratePrompts extracts prompts from document states for use by the pipeline enricher.
// It follows the generatePromptsFunc signature. For each state, it reads the document from
// Pebble (or uses the document already in state), extracts text via ExtractPrompt, and
// checks the appropriate hashID to skip unchanged documents.
func (si *SparseIndex) GeneratePrompts(
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
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		default:
		}

		opts := storeutils.QueryOptions{
			SkipDocument: state.Document != nil,
		}
		// For pipeline paths, use ChunkSuffix or SummarySuffix (which GetDocument can decode).
		// For the no-pipeline case, do NOT use EmbeddingSuffix because sparse embeddings
		// use a different encoding than dense embeddings and DecodeEmbeddingWithHashID would fail.
		if si.summarizerConf == nil && si.conf.Chunker != nil {
			opts.ChunkSuffix = si.chunkerSuffix
		} else if si.summarizerConf != nil {
			opts.SummarySuffix = si.summarizerSuffix
		}

		result, err := storeutils.GetDocument(ctx, si.db, state.CurrentDocKey, opts)
		if err != nil {
			if !errors.Is(err, pebble.ErrNotFound) {
				return nil, nil, nil, fmt.Errorf("getting value for indexing: %w", err)
			}
			continue
		}

		doc := state.Document
		if doc == nil {
			doc = result.Document
		}
		if doc == nil {
			si.logger.Warn("Document is nil after retrieval", zap.String("key", types.FormatKey(state.CurrentDocKey)))
			continue
		}

		var hashID uint64
		if si.conf.Chunker == nil && si.summarizerConf == nil {
			// No pipeline: read hashID directly from the sparse embedding key.
			// Sparse format: [hashID LE uint64][encoded SparseVec]
			sparseKey := append(bytes.Clone(state.CurrentDocKey), si.sparseSuffix...)
			sparseVal, closer, sparseErr := si.db.Get(sparseKey)
			if sparseErr == nil && len(sparseVal) >= 8 {
				hashID = binary.LittleEndian.Uint64(sparseVal[:8])
				_ = closer.Close()
			} else if sparseErr == nil {
				_ = closer.Close()
			}
		} else if si.summarizerConf == nil {
			hashID = result.ChunksHashID
		} else {
			hashID = result.SummaryHashID
		}

		prompt, promptHashID, err := si.ExtractPrompt(doc)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("extracting prompt: %w", err)
		}
		prompt = strings.TrimSpace(prompt)
		if len(prompt) == 0 {
			continue
		}

		if hashID != 0 && hashID == promptHashID {
			continue
		}
		promptKeys = append(promptKeys, state.CurrentDocKey)
		promptBatch = append(promptBatch, prompt)
		promptHashIDs = append(promptHashIDs, promptHashID)
	}
	return promptKeys, promptBatch, promptHashIDs, nil
}

// ComputeEnrichments generates sparse embeddings synchronously and returns the writes.
// This is called by preEnrichBatch() before Raft proposal for SyncLevelEnrichments/SyncLevelAknn.
// Returns sparse embedding writes without persisting (caller includes them in the Raft batch).
// When a pipeline enricher is configured, delegates to it for full pipeline pre-enrichment.
func (si *SparseIndex) ComputeEnrichments(
	ctx context.Context,
	writes [][2][]byte,
) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error) {
	si.enricherMu.RLock()
	e := si.enricher
	si.enricherMu.RUnlock()

	// If we have a pipeline enricher, delegate to it
	if pe, ok := e.(*pipelineSparseEnricher); ok {
		return si.computeEnrichmentsPipeline(ctx, writes, pe)
	}

	// When pipeline is configured but enricher isn't active yet (not leader),
	// skip pre-enrichment rather than generating raw sparse embeddings that
	// bypass the configured chunking/summarization pipeline.
	if si.summarizerConf != nil || si.conf.Chunker != nil {
		return nil, nil, nil
	}

	return si.computeEnrichmentsSimple(ctx, writes)
}

// computeEnrichmentsPipeline delegates pre-enrichment to the pipeline enricher,
// which handles chunking, summarization, and sparse embedding in one pass.
func (si *SparseIndex) computeEnrichmentsPipeline(
	ctx context.Context,
	writes [][2][]byte,
	pe *pipelineSparseEnricher,
) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error) {
	// Filter writes to keys in our range and build documentValues map
	filteredKeys := make([][]byte, 0, len(writes))
	documentValues := make(map[string][]byte, len(writes))

	for _, write := range writes {
		key := write[0]
		value := write[1]

		if !si.byteRange.Contains(key) {
			continue
		}
		if bytes.Contains(key, []byte(":i:")) {
			continue
		}

		filteredKeys = append(filteredKeys, key)
		documentValues[string(key)] = value
	}

	if len(filteredKeys) == 0 {
		return nil, nil, nil
	}

	// Create a generatePrompts wrapper that extracts from in-memory documents
	generatePromptsFromMemory := func(ctx context.Context, inputStates []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, 0, len(inputStates))
		prompts := make([]string, 0, len(inputStates))
		hashIDs := make([]uint64, 0, len(inputStates))

		for _, state := range inputStates {
			if state.Document == nil {
				si.logger.Warn("Document is nil in state during pre-enrichment, skipping",
					zap.String("key", types.FormatKey(state.CurrentDocKey)))
				continue
			}

			prompt, hashID, err := si.ExtractPrompt(state.Document)
			if err != nil {
				si.logger.Warn("Failed to extract prompt from document",
					zap.String("key", types.FormatKey(state.CurrentDocKey)),
					zap.Error(err))
				continue
			}

			prompt = strings.TrimSpace(prompt)
			if len(prompt) == 0 {
				continue
			}

			keys = append(keys, state.CurrentDocKey)
			prompts = append(prompts, prompt)
			hashIDs = append(hashIDs, hashID)
		}

		return keys, prompts, hashIDs, nil
	}

	embeddingWrites, chunkWrites, failed, err := pe.inner.GenerateEmbeddingsWithoutPersist(ctx, filteredKeys, documentValues, generatePromptsFromMemory)
	if err != nil {
		si.logger.Warn("Failed to generate enrichments in pipeline pre-enrichment",
			zap.Error(err),
			zap.String("index", si.name),
			zap.Int("numKeys", len(filteredKeys)))
		return nil, nil, fmt.Errorf("generating enrichments: %w", err)
	}

	// Combine embedding writes and chunk writes
	allEnrichmentWrites := make([][2][]byte, 0, len(embeddingWrites)+len(chunkWrites))
	allEnrichmentWrites = append(allEnrichmentWrites, embeddingWrites...)
	allEnrichmentWrites = append(allEnrichmentWrites, chunkWrites...)

	si.logger.Debug("ComputeEnrichments (pipeline) completed",
		zap.String("index", si.name),
		zap.Int("totalKeys", len(filteredKeys)),
		zap.Int("embeddingsGenerated", len(embeddingWrites)),
		zap.Int("chunksGenerated", len(chunkWrites)),
		zap.Int("failedKeys", len(failed)))

	return allEnrichmentWrites, failed, nil
}

// computeEnrichmentsSimple generates sparse embeddings directly (no pipeline).
func (si *SparseIndex) computeEnrichmentsSimple(
	ctx context.Context,
	writes [][2][]byte,
) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error) {
	// Get sparse embedder (lazily initialized, thread-safe)
	sparseEmb, err := si.getSearchEmbedder()
	if err != nil {
		return nil, nil, nil // No embedder available
	}

	var (
		filteredKeys [][]byte
		prompts      []string
		hashIDs      []uint64
	)

	for _, write := range writes {
		key := write[0]
		value := write[1]

		if !si.byteRange.Contains(key) {
			continue
		}
		// Skip enrichment keys (only process document keys)
		if bytes.Contains(key, []byte(":i:")) {
			continue
		}

		// Decode document value - handle both zstd-compressed and uncompressed JSON
		doc, err := storeutils.DecodeDocumentJSON(value)
		if err != nil {
			si.logger.Warn("Failed to decode document in pre-enrichment",
				zap.ByteString("key", key), zap.Error(err))
			continue
		}

		prompt, hashID, err := si.ExtractPrompt(doc)
		if err != nil {
			si.logger.Warn("Failed to extract prompt in pre-enrichment",
				zap.ByteString("key", key), zap.Error(err))
			continue
		}
		if prompt == "" {
			continue
		}

		filteredKeys = append(filteredKeys, key)
		prompts = append(prompts, strings.TrimSpace(prompt))
		hashIDs = append(hashIDs, hashID)
	}

	if len(prompts) == 0 {
		return nil, nil, nil
	}

	// Rate limit before calling embedding API (nil for local providers)
	var limiter *rate.Limiter
	if e, ok := sparseEmb.(embeddings.Embedder); ok {
		limiter = resolveRateLimiter(e)
	}
	if err := waitRateLimiter(ctx, limiter, len(prompts)); err != nil {
		return nil, nil, fmt.Errorf("waiting for rate limiter: %w", err)
	}

	// Generate sparse embeddings via Termite
	vecs, err := sparseEmb.SparseEmbed(ctx, prompts)
	if err != nil {
		si.logger.Warn("Failed to generate sparse embeddings in pre-enrichment",
			zap.Error(err), zap.String("index", si.name),
			zap.Int("numKeys", len(filteredKeys)))
		// Return keys as failed so they get picked up by async enricher
		return nil, filteredKeys, nil
	}

	if len(vecs) != len(prompts) {
		return nil, filteredKeys, fmt.Errorf("expected %d sparse vectors, got %d", len(prompts), len(vecs))
	}

	// Encode sparse embeddings as writes (key:i:<name>:sp -> [hashID][sparseVec])
	result := make([][2][]byte, 0, len(vecs))
	for i, vec := range vecs {
		sv := vector.NewSparseVector(vec.Indices, vec.Values)
		sparseBytes := EncodeSparseVec(sv)

		value := make([]byte, 8+len(sparseBytes))
		binary.LittleEndian.PutUint64(value[:8], hashIDs[i])
		copy(value[8:], sparseBytes)

		sparseKey := append(bytes.Clone(filteredKeys[i]), si.sparseSuffix...)
		result = append(result, [2][]byte{sparseKey, value})
	}

	si.logger.Debug("ComputeEnrichments completed",
		zap.String("index", si.name),
		zap.Int("totalKeys", len(filteredKeys)),
		zap.Int("sparseVecsGenerated", len(result)))

	return result, nil, nil
}

// getSearchEmbedder returns a cached sparse embedder for search queries.
// Thread-safe: uses a mutex to lazily initialize the embedder once.
func (si *SparseIndex) getSearchEmbedder() (embeddings.SparseEmbedder, error) {
	si.searchEmbMu.Lock()
	defer si.searchEmbMu.Unlock()

	if si.searchEmb != nil {
		return si.searchEmb, nil
	}

	if si.embedderConf == nil {
		return nil, errors.New("no embedder configured for query embedding")
	}

	emb, err := embeddings.NewEmbedder(*si.embedderConf)
	if err != nil {
		return nil, fmt.Errorf("creating embedder: %w", err)
	}

	sparseEmb, ok := emb.(embeddings.SparseEmbedder)
	if !ok {
		return nil, errors.New("embedder does not support sparse embedding")
	}

	si.searchEmb = sparseEmb
	return si.searchEmb, nil
}

// pipelineSparseEnricher wraps a PipelineEnricher (Enricher) to implement
// SparseEnricher so it can be stored in SparseIndex.enricher.
type pipelineSparseEnricher struct {
	inner Enricher
}

func (p *pipelineSparseEnricher) EnrichBatch(_ context.Context, keys [][]byte) error {
	return p.inner.EnrichBatch(keys)
}

func (p *pipelineSparseEnricher) EnricherStats() EnricherStats {
	return p.inner.EnricherStats()
}

func (p *pipelineSparseEnricher) Close() error {
	return p.inner.Close()
}

// --- Helper functions ---

// sparseOp implements inflight.Operation for the WAL dequeue loop.
type sparseOp struct {
	si   *SparseIndex
	keys [][]byte
}

func (op *sparseOp) Execute(ctx context.Context) error {
	if len(op.keys) == 0 {
		return nil
	}
	return op.si.enrichBatch(ctx, op.keys)
}

func (op *sparseOp) Merge(data []byte) error {
	var payload struct {
		Keys [][]byte `json:"keys"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("unmarshaling sparse enrich payload: %w", err)
	}
	op.keys = append(op.keys, payload.Keys...)
	return nil
}

func (op *sparseOp) reset() {
	op.keys = op.keys[:0]
}

// extractDocKey extracts the document key from an enrichment key.
// enrichment key format: <docKey> + suffix (where suffix = ":i:<indexName>:sp")
// Stripping the suffix yields the original document key.
func extractDocKey(key, suffix []byte) []byte {
	if !bytes.HasSuffix(key, suffix) {
		return nil
	}
	return bytes.Clone(key[:len(key)-len(suffix)])
}

// decodeSparseVec decodes a binary-encoded sparse vector.
// Format: [num_pairs uint32][indices N × uint32][values N × float32]
func decodeSparseVec(data []byte) (*vector.SparseVector, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("sparse vec data too short: %d bytes", len(data))
	}

	n := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
	if len(data) < 4+8*n {
		return nil, fmt.Errorf("sparse vec data too short for %d pairs", n)
	}

	indices := make([]uint32, n)
	values := make([]float32, n)

	offset := 4
	for i := range n {
		indices[i] = uint32(data[offset]) | uint32(data[offset+1])<<8 | uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
		offset += 4
	}
	for i := range n {
		bits := uint32(data[offset]) | uint32(data[offset+1])<<8 | uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
		values[i] = math.Float32frombits(bits)
		offset += 4
	}
	return vector.NewSparseVector(indices, values), nil
}

// EncodeSparseVec encodes a sparse vector into the binary format used for storage.
// Format: [num_pairs uint32][indices N × uint32][values N × float32]
func EncodeSparseVec(sv *vector.SparseVector) []byte {
	indices := sv.GetIndices()
	values := sv.GetValues()
	n := len(indices)
	buf := make([]byte, 4+8*n)

	buf[0] = byte(n)       //nolint:gosec // G115: bounded value, cannot overflow in practice
	buf[1] = byte(n >> 8)  //nolint:gosec // G115: bounded value, cannot overflow in practice
	buf[2] = byte(n >> 16) //nolint:gosec // G115: bounded value, cannot overflow in practice
	buf[3] = byte(n >> 24) //nolint:gosec // G115: bounded value, cannot overflow in practice

	offset := 4
	for i := range n {
		v := indices[i]
		buf[offset] = byte(v)         //nolint:gosec // G115: intentional byte extraction from uint32
		buf[offset+1] = byte(v >> 8)  //nolint:gosec // G115: intentional byte extraction from uint32
		buf[offset+2] = byte(v >> 16) //nolint:gosec // G115: intentional byte extraction from uint32
		buf[offset+3] = byte(v >> 24)
		offset += 4
	}
	for i := range n {
		v := math.Float32bits(values[i])
		buf[offset] = byte(v)         //nolint:gosec // G115: bounded value, cannot overflow in practice
		buf[offset+1] = byte(v >> 8)  //nolint:gosec // G115: bounded value, cannot overflow in practice
		buf[offset+2] = byte(v >> 16) //nolint:gosec // G115: bounded value, cannot overflow in practice
		buf[offset+3] = byte(v >> 24)
		offset += 4
	}
	return buf
}
