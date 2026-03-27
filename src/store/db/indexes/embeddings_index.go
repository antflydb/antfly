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
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/template"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/blevesearch/bleve/v2"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/klauspost/compress/zstd"
	"github.com/theory/jsonpath"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const embeddingsMaxBatches = 10
const embeddingsPartitionSize = 1000

// Prompt failure sentinel errors. Callers use errors.Is to classify.
var (
	// ErrPermanentPromptFailure indicates the prompt cannot be generated for this document
	// (e.g., a remote resource returned 404). The item should be marked as a dud.
	ErrPermanentPromptFailure = errors.New("permanent prompt failure")

	// ErrTransientPromptFailure indicates a temporary failure during prompt generation
	// (e.g., a remote resource returned 503). The item should be retried later.
	ErrTransientPromptFailure = errors.New("transient prompt failure")
)

var embeddingsRateLimiter *rate.Limiter = rate.NewLimiter(10_000, embeddingsMaxBatches*embeddingsPartitionSize)

func init() {
	limitStr := os.Getenv("ANTFLY_EMBEDDINGS_INDEX_RATE_LIMIT")
	if limitStr != "" {
		// Parse the rate limit from the environment variable
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			panic(fmt.Sprintf("Invalid ANTFLY_EMBEDDINGS_INDEX_RATE_LIMIT value: %s", limitStr))
		}
		embeddingsRateLimiter = rate.NewLimiter(rate.Limit(limit), embeddingsMaxBatches*embeddingsPartitionSize)
	}
}

var _ Index = (*EmbeddingIndex)(nil) // Compile-time check for Index interface implementation

func init() {
	RegisterIndex(IndexTypeAknnV0, NewEmbeddingIndex)
}

type Enricher interface {
	EnrichBatch(keys [][]byte) error
	GenerateEmbeddingsWithoutPersist(ctx context.Context, keys [][]byte, documentValues map[string][]byte, generatePrompts generatePromptsFunc) (embeddingWrites [][2][]byte, chunkWrites [][2][]byte, failedKeys [][]byte, err error)
	EnricherStats() EnricherStats
	Close() error
}

type EmbeddingIndex struct {
	sync.Mutex

	db           *pebble.DB
	indexDB      *pebble.DB // Separate Pebble instance for vector index metadata
	indexPath    string
	logger       *zap.Logger
	antflyConfig *common.Config

	// Config
	enricherMu     sync.RWMutex
	enricher       Enricher
	embedderConf   *embeddings.EmbedderConfig // Config for the enricher plugin
	summarizerConf *ai.GeneratorConfig        // Config for the summarizer plugin
	indexingField  string
	promptTemplate string

	cache     *pebbleutils.Cache // shared block cache (may be nil)
	memOnly   bool
	byteRange types.Range
	schema    *schema.TableSchema

	conf EmbeddingsIndexConfig

	dimension int

	egCancel context.CancelFunc
	egCtx    context.Context
	eg       *errgroup.Group
	walBuf   *inflight.WALBuffer

	enqueueChan chan int
	flushTime   time.Duration

	idx  vectorindex.VectorIndex
	name string

	suffix           []byte
	embedderSuffix   []byte
	summarizerSuffix []byte
	chunkerSuffix    []byte

	rebuildState *RebuildState

	zstdReader  *zstd.Decoder
	batchBuffer *bytes.Buffer // Buffer for batch operations

	backfillDone chan struct{} // Signals when backfill is complete

	// Pause/Resume support
	pauseMu    sync.Mutex
	pauseCond  *sync.Cond
	pauseAckCh chan struct{} // Separate channel for plexer pause acknowledgment
	paused     atomic.Bool
}

type vectorOp struct {
	Keys    [][]byte   `json:"keys"`
	Vectors []vector.T `json:"vectors"`

	Deletes [][]byte `json:"deletes"`

	ei *EmbeddingIndex
}

func (vo *vectorOp) Execute(ctx context.Context) (err error) {
	// startTime := time.Now()
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(vo.Keys) == 0 && len(vo.Deletes) == 0 {
		return nil
	}
	// TODO (ajr) This doesn't account for deletes
	if err := embeddingsRateLimiter.WaitN(ctx, len(vo.Keys)); err != nil {
		return fmt.Errorf("waiting for rate limiter: %w", err)
	}
	batch := vo.ei.NewBatch()
	if err := batch.Insert(vo.Keys, vo.Vectors); err != nil {
		vo.ei.logger.Error("Inserting vectors failed", zap.ByteStrings("keys", vo.Keys), zap.Error(err))
		return fmt.Errorf("inserting vectors: %w", err)
	}
	normalizedDeletes := make([][]byte, 0, len(vo.Deletes))
	for _, key := range vo.Deletes {
		if bytes.HasSuffix(key, vo.ei.embedderSuffix) {
			if docKey := extractDocKey(key, vo.ei.embedderSuffix); docKey != nil {
				normalizedDeletes = append(normalizedDeletes, docKey)
				continue
			}
		}
		normalizedDeletes = append(normalizedDeletes, key)
	}
	if err := batch.Delete(normalizedDeletes); err != nil {
		return fmt.Errorf("deleting vectors: %w", err)
	}
	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("applying batch: %w", err)
	}
	// vo.ei.logger.Debug(
	// 	"Executed embeddings index batch",
	// 	zap.Duration("took", time.Since(startTime)),
	// 	zap.Int("numKeys", len(vo.Keys)),
	// 	zap.Int("numDeletes", len(vo.Deletes)),
	// )
	batch.Reset()
	return nil
}

var vectorOpPool = sync.Pool{
	New: func() any {
		return &vectorOp{
			Keys:    make([][]byte, 0, 100),   // Pre-allocate with reasonable capacity
			Vectors: make([]vector.T, 0, 100), // Pre-allocate with reasonable capacity
			Deletes: make([][]byte, 0, 100),   // Pre-allocate with reasonable capacity
		}
	},
}

func (vo *vectorOp) reset() {
	// Clear slices but keep underlying capacity, re-allocating if capacity is lost
	if cap(vo.Keys) < 100 {
		vo.Keys = make([][]byte, 0, 100)
	} else {
		vo.Keys = vo.Keys[:0]
	}
	if cap(vo.Deletes) < 100 {
		vo.Deletes = make([][]byte, 0, 100)
	} else {
		vo.Deletes = vo.Deletes[:0]
	}
	if cap(vo.Vectors) < 100 {
		vo.Vectors = make([]vector.T, 0, 100)
	} else {
		vo.Vectors = vo.Vectors[:0]
	}
}

func (vo *vectorOp) Merge(datum []byte) error {
	vecOp := vectorOpPool.Get().(*vectorOp)
	defer func() {
		vecOp.reset()
		vectorOpPool.Put(vecOp)
	}()
	err := json.Unmarshal(datum, vecOp)
	if err != nil {
		return fmt.Errorf("unmarshaling enrich operation: %w", err)
	}
	vo.Keys = append(vo.Keys, vecOp.Keys...)
	vo.Vectors = append(vo.Vectors, vecOp.Vectors...)
	vo.Deletes = append(vo.Deletes, vecOp.Deletes...)
	return nil
}

func NewEmbeddingIndex(
	logger *zap.Logger,
	antflyConfig *common.Config,
	db *pebble.DB,
	dir string,
	name string,
	config *IndexConfig,
	cache *pebbleutils.Cache,
) (Index, error) {
	if config == nil {
		return nil, errors.New("embedding index config is required")
	}
	c, err := config.AsEmbeddingsIndexConfig()
	if err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if c.Dimension <= 0 {
		return nil, fmt.Errorf("dimension must be positive: got %d", c.Dimension)
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}

	reader, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("creating zstd reader: %w", err)
	}
	indexSuffix := fmt.Appendf(nil, ":i:%s", name)
	indexPath := filepath.Join(dir, name)
	// Build chunk suffix if chunking is configured
	var chunkerSuffix []byte
	if c.Chunker != nil {
		if chunking.GetFullTextIndex(*c.Chunker) != nil {
			chunkerSuffix = fmt.Appendf(bytes.Clone(indexSuffix), ":0%s", storeutils.ChunkingFullTextSuffix)
		} else {
			chunkerSuffix = fmt.Appendf(bytes.Clone(indexSuffix), ":0%s", storeutils.ChunkingSuffix)
		}
	}

	e := &EmbeddingIndex{
		db:               db,
		antflyConfig:     antflyConfig,
		indexPath:        indexPath,
		name:             name,
		suffix:           indexSuffix,
		embedderSuffix:   fmt.Appendf(bytes.Clone(indexSuffix), "%s", storeutils.EmbeddingSuffix),
		summarizerSuffix: fmt.Appendf(bytes.Clone(indexSuffix), "%s", storeutils.SummarySuffix),
		chunkerSuffix:    chunkerSuffix,
		logger:           logger,
		dimension:        c.Dimension,
		indexingField:    c.Field,
		promptTemplate:   c.Template,
		cache:            cache,
		memOnly:          c.MemOnly,
		embedderConf:     c.Embedder,
		summarizerConf:   c.Summarizer,
		conf:             c,
		rebuildState:     NewRebuildState(indexPath),
		zstdReader:       reader,
		flushTime:        DefaultFlushTime,
		pauseAckCh:       make(chan struct{}, 1), // Buffered to prevent blocking plexer
	}

	e.pauseCond = sync.NewCond(&e.pauseMu)
	e.egCtx, e.egCancel = context.WithCancel(context.TODO())
	e.eg, e.egCtx = errgroup.WithContext(e.egCtx)
	return e, nil
}

func (i *EmbeddingIndex) NeedsEnricher() bool {
	return i.embedderConf != nil
}

func (i *EmbeddingIndex) Name() string {
	return i.name
}

func (i *EmbeddingIndex) Type() IndexType {
	return IndexTypeEmbeddings
}

// GetDimension returns the expected embedding dimension for this index
func (ei *EmbeddingIndex) GetDimension() int {
	return ei.dimension
}

// RenderPrompt extracts and renders the prompt from a document,
// returning the prompt text and its hashID
func (ei *EmbeddingIndex) RenderPrompt(doc map[string]any) (string, uint64, error) {
	var prompt string
	var err error

	// Use template if configured
	if len(ei.promptTemplate) > 0 {
		prompt, err = template.Render(ei.promptTemplate, doc)
		if err != nil {
			return "", 0, fmt.Errorf("rendering template: %w", err)
		}
	} else if ei.indexingField != "" {
		// Use JSONPath for field extraction to support nested paths
		pathStr := ei.indexingField
		if pathStr[0] != '$' {
			pathStr = "$." + pathStr
		}

		path, err := jsonpath.Parse(pathStr)
		if err != nil {
			return "", 0, fmt.Errorf("parsing JSONPath %s: %w", ei.indexingField, err)
		}

		nodes := path.Select(doc)
		// Get the first matching value
		var foundValue any
		var found bool
		for node := range nodes.All() {
			foundValue = node
			found = true
			break
		}

		if !found {
			return "", 0, fmt.Errorf("field %s not found in document", ei.indexingField)
		}

		strValue, ok := foundValue.(string)
		if !ok {
			return "", 0, fmt.Errorf("field %s is not a string: %T", ei.indexingField, foundValue)
		}
		prompt = strValue
	} else {
		return "", 0, errors.New("no template or field configured for prompt extraction")
	}

	// Check for error directives from remote helpers (e.g., remoteMedia 404)
	if directives := template.ParseErrorDirectives(prompt); len(directives) > 0 {
		for _, d := range directives {
			if d.IsPermanent() {
				return "", 0, fmt.Errorf("%w: %s", ErrPermanentPromptFailure, d.Message)
			}
		}
		// Non-permanent error directives → transient failure
		return "", 0, fmt.Errorf("%w: %s", ErrTransientPromptFailure, directives[0].Message)
	}

	prompt = strings.TrimSpace(prompt)
	hashID := xxhash.Sum64String(prompt)

	return prompt, hashID, nil
}

func (i *EmbeddingIndex) Close() (err error) {
	defer pebbleutils.IgnorePebbleClosed(&err)
	defer pebbleutils.RecoverPebbleClosed(&err)

	if i == nil {
		return nil
	}
	if i.egCancel == nil {
		return nil
	}
	// Cancel context to signal goroutines to stop
	i.egCancel()

	// Wait for background goroutines (plexer) to finish BEFORE closing resources they use
	if i.eg != nil {
		if err := i.eg.Wait(); err != nil {
			i.zstdReader.Close()
			if errors.Is(err, context.Canceled) || errors.Is(err, inflight.ErrBufferClosed) ||
				errors.Is(err, bleve.ErrorIndexClosed) {
				// These are expected errors during shutdown
			} else {
				return fmt.Errorf("waiting for background tasks to finish: %w", err)
			}
		}
	}

	// Now safe to close resources since plexer has stopped
	if err := i.walBuf.Close(); err != nil && !errors.Is(err, inflight.ErrBufferClosed) {
		return fmt.Errorf("closing WAL buffer: %w", err)
	}
	if err := i.idx.Close(); err != nil {
		return fmt.Errorf("closing vector index: %w", err)
	}
	// Close the separate indexDB instance
	if i.indexDB != nil {
		if err := i.indexDB.Close(); err != nil {
			i.logger.Warn("Error closing indexDB", zap.Error(err))
		}
	}
	i.zstdReader.Close()
	return nil
}

func (s *EmbeddingIndex) Stats() IndexStats {
	if s.idx == nil {
		return EmbeddingsIndexStats{
			Error: "index not initialized",
		}.AsIndexStats()
	}

	var is EmbeddingsIndexStats
	stats := s.idx.Stats()
	if totIdx, ok := stats["total_indexed"]; ok {
		is.TotalIndexed = totIdx.(uint64)
	}
	if totNodes, ok := stats["total_nodes"]; ok {
		is.TotalNodes = totNodes.(uint64)
	}

	s.enricherMu.RLock()
	e := s.enricher
	s.enricherMu.RUnlock()
	if e != nil {
		es := e.EnricherStats()
		is.Rebuilding = es.Backfilling
		is.WalBacklog = uint64(es.WALBacklog) //nolint:gosec // G115: WALBacklog is non-negative
		is.BackfillProgress = es.BackfillProgress
		is.BackfillItemsProcessed = es.BackfillItemsProcessed
	}

	return is.AsIndexStats()
}

func (s *EmbeddingIndex) IsChunked() bool {
	return s.conf.Chunker != nil
}

func (s *EmbeddingIndex) Search(ctx context.Context, query any) (any, error) {
	queryOps.WithLabelValues(s.name).Inc()
	if query == nil {
		return nil, storeutils.ErrEmptyRequest
	}
	searchRequest, ok := query.(*vectorindex.SearchRequest)
	if !ok {
		return nil, fmt.Errorf("invalid query type: %T expected *vectorindex.SearchRequest", query)
	}
	if searchRequest == nil {
		return nil, storeutils.ErrEmptyRequest
	}
	if len(searchRequest.Embedding) == 0 {
		return nil, errors.New("search must specify an embedding")
	}
	if len(searchRequest.Embedding) != s.dimension {
		return nil, fmt.Errorf(
			"vector index dimensionality mismatch: %d",
			len(searchRequest.Embedding),
		)
	}
	if searchRequest.RerankPolicy == nil {
		policy := vectorindex.RerankPolicyBoundary
		searchRequest.RerankPolicy = &policy
	}
	s.resolveSearchEffort(searchRequest)
	searchResult, err := vectorindex.SearchInContext(ctx, s.idx, searchRequest)
	if err != nil {
		return nil, fmt.Errorf("searching: %w", err)
	}
	s.logger.Debug("searching vector index returned results",
		zap.String("dir", s.indexPath),
		zap.Int("dimension", s.dimension),
		zap.Int("results", len(searchResult.Hits)))
	return searchResult, nil
}

const defaultBalancedSearchEffort = float32(0.5)

// resolveSearchEffort translates the user-facing search_effort (0.0-1.0) into
// concrete SearchWidth and Epsilon2 overrides on the SearchRequest.
// When no search tuning is specified, it applies the balanced default effort.
func (s *EmbeddingIndex) resolveSearchEffort(req *vectorindex.SearchRequest) {
	if req == nil {
		return
	}

	effort, ok := normalizedSearchEffort(req.SearchEffort)
	if !ok {
		if req.SearchWidth != nil || req.Epsilon2 != nil {
			return
		}
		effort = defaultBalancedSearchEffort
	}

	if req.SearchWidth == nil {
		req.SearchWidth = intPtr(s.resolveSearchWidth(req.K, effort))
	}

	if req.Epsilon2 == nil {
		req.Epsilon2 = float32Ptr(resolveSearchEpsilon2(effort))
	}
}

func normalizedSearchEffort(effort *float32) (float32, bool) {
	if effort == nil {
		return 0, false
	}
	v := *effort
	if math.IsNaN(float64(v)) {
		return 0, false
	}
	if v < 0 {
		return 0, true
	}
	if v > 1 {
		return 1, true
	}
	return v, true
}

func (s *EmbeddingIndex) resolveSearchWidth(k int, effort float32) int {
	minWidth := max(k, 64)
	legacyMaxWidth := max(minWidth*20, 4096)
	sizeAwareMaxWidth := s.resolveSearchWidthCap(legacyMaxWidth)
	balancedWidth := minWidth + int(float32(legacyMaxWidth-minWidth)*defaultBalancedSearchEffort)

	if effort <= defaultBalancedSearchEffort {
		if balancedWidth <= minWidth {
			return minWidth
		}
		ratio := effort / defaultBalancedSearchEffort
		return minWidth + int(float32(balancedWidth-minWidth)*ratio)
	}

	if sizeAwareMaxWidth <= balancedWidth {
		return sizeAwareMaxWidth
	}
	ratio := (effort - defaultBalancedSearchEffort) / (1 - defaultBalancedSearchEffort)
	w := balancedWidth + int(float32(sizeAwareMaxWidth-balancedWidth)*ratio)
	if w > sizeAwareMaxWidth {
		return sizeAwareMaxWidth
	}
	return w
}

func resolveSearchEpsilon2(effort float32) float32 {
	// Piecewise linear pruning threshold:
	//   effort 0.0 -> ε₂=1.0   (aggressive pruning)
	//   effort 0.5 -> ε₂=7.0   (balanced default)
	//   effort 1.0 -> ε₂=100.0 (effectively no pruning)
	if effort < defaultBalancedSearchEffort {
		return 1.0 + (effort * 12.0)
	}
	return 7.0 + ((effort - defaultBalancedSearchEffort) * 186.0)
}

func intPtr(v int) *int {
	return &v
}

func float32Ptr(v float32) *float32 {
	return &v
}

func (s *EmbeddingIndex) resolveSearchWidthCap(legacyMaxWidth int) int {
	if s == nil || s.idx == nil {
		return legacyMaxWidth
	}
	stats := s.idx.Stats()
	totalNodes, ok := statInt(stats["total_nodes"])
	if !ok || totalNodes <= legacyMaxWidth {
		return legacyMaxWidth
	}
	return totalNodes
}

func statInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		if n < 0 {
			return 0, false
		}
		return int(n), true
	case uint:
		return int(n), true
	case uint32:
		return int(n), true
	case uint64:
		if n > math.MaxInt {
			return 0, false
		}
		return int(n), true
	case float64:
		if n < 0 || n > math.MaxInt {
			return 0, false
		}
		return int(n), true
	default:
		return 0, false
	}
}

func (ei *EmbeddingIndex) newVectorIndex(dimension int) (vectorindex.VectorIndex, error) {
	if dimension <= 0 {
		return nil, fmt.Errorf("dimension must be positive: got %d", dimension)
	}
	if dimension > math.MaxUint32 {
		return nil, fmt.Errorf("dimension must be less than %d: got %d", math.MaxUint32, dimension)
	}

	// VectorDB always uses the main shard DB passed in during index creation
	vectorDB := ei.db

	// IndexDB stores the vector index metadata (tree structure, etc.)
	// Create a separate Pebble instance for better isolation and performance
	pebbleOpts := &pebble.Options{
		Logger:          &logger.NoopLoggerAndTracer{},
		LoggerAndTracer: &logger.NoopLoggerAndTracer{},
	}
	ei.cache.Apply(pebbleOpts, 50*1024*1024) // 50MB fallback
	if ei.memOnly {
		pebbleOpts.FS = vfs.NewMem()
	}
	indexDB, err := pebble.Open(filepath.Join(ei.indexPath, "indexdb"), pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("opening index database: %w", err)
	}

	ei.indexDB = indexDB

	// Configure collapse function if chunking is enabled
	var collapseKeysFunc func([]byte) []byte
	if ei.conf.Chunker != nil {
		collapseKeysFunc = func(metadataKey []byte) []byte {
			if docKey, ok := storeutils.ExtractDocKeyFromChunk(metadataKey); ok {
				return docKey
			}
			return metadataKey // Not a chunk, use full key (no collapse)
		}
	}

	r := rand.New(rand.NewPCG(42, 1024)) //nolint:gosec // G404: non-security randomness for ML/jitter
	cfg := vectorindex.HBCConfig{
		Name:             ei.name,
		Dimension:        uint32(dimension),
		SplitAlgo:        vector.ClustAlgorithm_Kmeans,
		QuantizerSeed:    42,
		UseQuantization:  true,
		Episilon2:        7,
		BranchingFactor:  7 * 24,
		LeafSize:         7 * 24,
		SearchWidth:      2 * 3 * 7 * 24,
		CacheSizeNodes:   100_000,
		VectorDB:         vectorDB,
		IndexDB:          indexDB,
		DistanceMetric:   ei.resolveDistanceMetric(),
		CollapseKeysFunc: collapseKeysFunc,
	}
	index, err := vectorindex.NewHBCIndex(cfg, r)
	if err != nil {
		return nil, fmt.Errorf("creating index: %w", err)
	}
	return index, nil
}

// resolveDistanceMetric maps the config's distance_metric string to the vector proto enum.
func (ei *EmbeddingIndex) resolveDistanceMetric() vector.DistanceMetric {
	switch ei.conf.DistanceMetric {
	case DistanceMetricCosine:
		return vector.DistanceMetric_Cosine
	case DistanceMetricInnerProduct:
		return vector.DistanceMetric_InnerProduct
	default:
		return vector.DistanceMetric_L2Squared
	}
}

func (i *EmbeddingIndex) NewBatch() *EmbeddingsBatch {
	i.Lock()
	defer i.Unlock()
	if i.idx == nil {
		return nil
	}
	return &EmbeddingsBatch{
		idx:   i,
		batch: &vectorindex.Batch{},
	}
}

func (i *EmbeddingIndex) enricherCallback(
	ctx context.Context,
	keys [][]byte,
) error {
	writes := make([][2][]byte, len(keys))
	for i, key := range keys {
		writes[i] = [2][]byte{key, nil}
	}
	return i.Batch(ctx, writes, nil, false)
}

func (i *EmbeddingIndex) GeneratePrompts(
	ctx context.Context, states []storeutils.DocumentScanState,
) (keys [][]byte, prompts []string, hashIDs []uint64, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(states) == 0 {
		return nil, nil, nil, nil
	}
	promptKeys := make([][]byte, 0, len(states))
	promptBatch := make([]string, 0, len(states))
	promptHashIDs := make([]uint64, 0, len(states))
	var permanentFailures [][]byte

	for _, state := range states {
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		default:
		}

		opts := storeutils.QueryOptions{
			SkipDocument: state.Document != nil, // Skip if we already have the document
		}
		if i.conf.Chunker == nil && i.summarizerConf == nil {
			opts.EmbeddingSuffix = i.embedderSuffix
		} else if i.summarizerConf == nil {
			opts.ChunkSuffix = i.chunkerSuffix
		} else {
			opts.SummarySuffix = i.summarizerSuffix
		}

		// Fetch what we need (hash always, document only if needed)
		result, err := storeutils.GetDocument(ctx, i.db, state.CurrentDocKey, opts)
		if err != nil {
			if !errors.Is(err, pebble.ErrNotFound) {
				return nil, nil, nil, fmt.Errorf("getting value for indexing: %w", err)
			}
			// Key was deleted between scan and index - this is expected during concurrent writes
			continue
		}

		// Use document from state if available, otherwise from result
		doc := state.Document
		if doc == nil {
			doc = result.Document
		}
		if doc == nil {
			i.logger.Warn("Document is nil after retrieval", zap.String("key", types.FormatKey(state.CurrentDocKey)))
			continue
		}

		// Extract hash ID based on configuration
		var hashID uint64
		if i.conf.Chunker == nil && i.summarizerConf == nil {
			hashID = result.EmbeddingHashID
		} else if i.summarizerConf == nil {
			hashID = result.ChunksHashID
		} else {
			hashID = result.SummaryHashID
		}

		prompt, promptHashID, err := i.ExtractPrompt(doc)
		if err != nil {
			if errors.Is(err, ErrPermanentPromptFailure) {
				i.logger.Warn("Permanent prompt failure, will mark as dud",
					zap.String("key", types.FormatKey(state.CurrentDocKey)),
					zap.Error(err))
				permanentFailures = append(permanentFailures, state.CurrentDocKey)
				continue
			}
			if errors.Is(err, ErrTransientPromptFailure) {
				i.logger.Debug("Transient prompt failure, skipping for retry",
					zap.String("key", types.FormatKey(state.CurrentDocKey)),
					zap.Error(err))
				continue
			}
			return nil, nil, nil, fmt.Errorf("extracting embedding: %w", err)
		}
		prompt = strings.TrimSpace(prompt)
		if len(prompt) == 0 {
			continue
		}

		if hashID != 0 && hashID == promptHashID {
			// If the hash matches, we can skip this prompt
			continue
		}
		promptKeys = append(promptKeys, state.CurrentDocKey)
		promptBatch = append(promptBatch, prompt)
		promptHashIDs = append(promptHashIDs, promptHashID)
	}

	// Write dud markers for permanently failed items
	if len(permanentFailures) > 0 {
		suffix := i.embedderSuffix
		if i.conf.Chunker != nil {
			suffix = i.chunkerSuffix
		}
		if i.summarizerConf != nil {
			suffix = i.summarizerSuffix
		}
		writeDudKeysToDb(i.db, i.logger, permanentFailures, suffix)
	}

	return promptKeys, promptBatch, promptHashIDs, nil
}

func (i *EmbeddingIndex) Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte, sync bool) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(writes) == 0 && len(deletes) == 0 {
		return nil
	}
	vo := &vectorOp{
		Deletes: deletes,
		ei:      i, // Set EmbeddingIndex pointer for vo.Execute()
	}
	promptKeys := [][]byte{}
	if i.batchBuffer == nil {
		i.batchBuffer = bytes.NewBuffer(nil)
	}
	for _, write := range writes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		key := write[0]
		if len(key) == 0 {
			i.logger.Warn("Found an empty key writing", zap.Any("write", write))
			continue
		}
		if bytes.HasSuffix(key, i.embedderSuffix) {
			b, closer, err := i.db.Get(key)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					return fmt.Errorf("getting value for indexing: %w", err)
				}
				// Key was deleted between scan and index - expected during concurrent writes
				continue
			}
			// This is an embedding write directly
			_, vector, _, err := vectorindex.DecodeEmbeddingWithHashID(b)
			if err != nil {
				return fmt.Errorf("decoding embedding for key %s: %w", key, err)
			}
			if err := closer.Close(); err != nil {
				return fmt.Errorf("closing buffer after decoding vector: %w", err)
			}
			if len(vector) != i.dimension {
				return fmt.Errorf("vector dimension mismatch for key %s: expected %d, got %d",
					key, i.dimension, len(vector))
			}

			vo.Vectors = append(vo.Vectors, vector)
			vo.Keys = append(vo.Keys, key[:len(key)-len(i.embedderSuffix)])
			continue
		}
		if bytes.HasSuffix(key, i.summarizerSuffix) {
			b, closer, err := i.db.Get(key)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					return fmt.Errorf("getting value for indexing: %w", err)
				}
				// Key was deleted between scan and index - expected during concurrent writes
				continue
			}
			_, _ /* hashID */, err = encoding.DecodeUint64Ascending(b)
			if err != nil {
				return fmt.Errorf("decoding summary for key %s: %w", key, err)
			}
			if err := closer.Close(); err != nil {
				return fmt.Errorf("closing buffer after decoding summary: %w", err)
			}
			promptKeys = append(promptKeys, key)
			continue
		}
		// Check if this is a chunk key
		if storeutils.IsChunkKey(key) {
			// Verify it belongs to this index by checking if it contains our index suffix
			if !bytes.Contains(key, i.suffix) {
				// Chunk for a different index, skip it
				continue
			}
			// This is a chunk for THIS index - handle it specially
			// Chunks are stored as [hashID:uint64][chunkJSON] directly at the key (no :\x00 suffix)
			b, closer, err := i.db.Get(key)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					return fmt.Errorf("getting chunk for indexing: %w", err)
				}
				i.logger.Warn("Chunk key not found for indexing", zap.String("key", types.FormatKey(key)))
				continue
			}
			// Decode [hashID][chunkJSON]
			b, _ /* chunkHashID */, err = encoding.DecodeUint64Ascending(b)
			if err != nil {
				_ = closer.Close()
				return fmt.Errorf("decoding chunk hashID for key %s: %w", key, err)
			}
			// Unmarshal chunk JSON to get text
			var chunk chunking.Chunk
			if err := json.Unmarshal(b, &chunk); err != nil {
				_ = closer.Close()
				return fmt.Errorf("unmarshaling chunk JSON for key %s: %w", key, err)
			}
			_ = closer.Close()

			// Extract chunk text
			chunkText := strings.TrimSpace(chunk.GetText())
			if len(chunkText) == 0 {
				continue
			}

			// Check if embedding already exists with matching hash
			embKey := append(key, i.embedderSuffix...)
			promptHashID := xxhash.Sum64String(chunkText)
			embExists, embCloser, err := i.db.Get(embKey)
			if err == nil {
				// Embedding exists, check if hash matches
				_, existingHashID, err := encoding.DecodeUint64Ascending(embExists)
				_ = embCloser.Close()
				if err == nil {
					if existingHashID == promptHashID {
						// Hash matches, embedding is up-to-date
						continue
					}
				}
			} else if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("checking chunk embedding: %w", err)
			}

			// Need to embed this chunk
			promptKeys = append(promptKeys, key)
			continue
		} else if bytes.HasSuffix(key, storeutils.EmbeddingSuffix) || bytes.HasSuffix(key, storeutils.SummarySuffix) {
			// Embedding or summary for a different index, skip it
			continue
		}

		i.batchBuffer.Reset()

		// Build query options - check for chunk hashID if chunking is enabled
		result, err := storeutils.GetDocument(ctx, i.db, key, storeutils.QueryOptions{
			EmbeddingSuffix: i.embedderSuffix,
			SummarySuffix:   i.summarizerSuffix,
			ChunkSuffix:     i.chunkerSuffix,
		})
		if err != nil {
			if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("getting value for indexing: %w", err)
			}
			// Key was deleted between scan and index - expected during concurrent writes
			continue
		}
		embHashID := result.EmbeddingHashID
		sumHashID := result.SummaryHashID
		chunksHashID := result.ChunksHashID
		doc := result.Document
		if doc == nil {
			i.logger.Warn("Document is nil after retrieval", zap.String("key", types.FormatKey(key)))
			continue
		}

		prompt, promptHashID, err := i.ExtractPrompt(doc)
		if err != nil {
			if errors.Is(err, ErrPermanentPromptFailure) {
				i.logger.Warn("Permanent prompt failure in Batch, writing dud marker",
					zap.String("key", types.FormatKey(key)),
					zap.Error(err))
				writeDudKeysToDb(i.db, i.logger, [][]byte{key}, i.embedderSuffix)
				continue
			}
			if errors.Is(err, ErrTransientPromptFailure) {
				i.logger.Debug("Transient prompt failure in Batch, skipping document",
					zap.String("key", types.FormatKey(key)),
					zap.Error(err))
				continue
			}
			return fmt.Errorf("extracting embedding: %w", err)
		}
		if len(prompt) > 0 {
			// If chunking is enabled and chunks exist with matching hash, skip document
			// The document has already been chunked and chunks are up-to-date
			if i.conf.Chunker != nil && chunksHashID != 0 && promptHashID == chunksHashID {
				continue
			}
			if i.summarizerConf != nil && sumHashID != 0 && promptHashID == sumHashID {
				continue
			}
			if i.embedderConf != nil && embHashID != 0 && promptHashID == embHashID {
				continue
			}
			// TODO (ajr) Should we log if the enricher and summarizer are not configured?
			promptKeys = append(promptKeys, key)
		}
	}

	// Send keys needing enrichment to enricher (sync or async based on sync parameter)
	if i.embedderConf != nil {
		if err := i.enqueueEnrichment(promptKeys, sync); err != nil {
			return err
		}
	}
	if len(vo.Vectors) == 0 && len(vo.Deletes) == 0 {
		return nil
	}
	i.logger.Debug(
		"Enqueueing writes and deletes to vector index",
		zap.Int("writes", len(writes)),
		zap.Int("deletes", len(deletes)),
	)

	// If sync is true, execute immediately
	if sync {
		return vo.Execute(ctx)
	}

	// Partition the operations into batches of embeddingsBatchSize
	partitionedBatch := make([][]byte, 0, (len(vo.Vectors)+embeddingsPartitionSize-1)/embeddingsPartitionSize+1)
	partitionCounts := make([]uint32, 0, cap(partitionedBatch))

	// Process vectors and keys in batches
	for start := 0; start < len(vo.Vectors); start += embeddingsPartitionSize {
		end := start + embeddingsPartitionSize
		end = min(end, len(vo.Vectors))
		batchOp := &vectorOp{
			Vectors: vo.Vectors[start:end],
			Keys:    vo.Keys[start:end],
		}

		// Add a proportional share of deletes to the first batch
		if start == 0 && len(vo.Deletes) > 0 {
			batchOp.Deletes = vo.Deletes
		}

		b, err := json.Marshal(batchOp)
		if err != nil {
			return fmt.Errorf("marshaling vector operation batch: %w", err)
		}
		partitionedBatch = append(partitionedBatch, b)
		partitionCounts = append(partitionCounts, uint32(len(batchOp.Vectors)+len(batchOp.Deletes))) //nolint:gosec // G115: partition size bounded by config
	}

	// If we only have deletes, create a single batch
	if len(vo.Vectors) == 0 && len(vo.Deletes) > 0 {
		batchOp := &vectorOp{
			Deletes: vo.Deletes,
		}
		b, err := json.Marshal(batchOp)
		if err != nil {
			return fmt.Errorf("marshaling delete operation batch: %w", err)
		}
		partitionedBatch = append(partitionedBatch, b)
		partitionCounts = append(partitionCounts, uint32(len(batchOp.Deletes))) //nolint:gosec // G115: partition size bounded by config
	}

	if err := i.walBuf.EnqueueBatch(partitionedBatch, partitionCounts); err != nil {
		return err
	}

	// Signal dequeue AFTER enqueuing to avoid race condition
	select {
	case i.enqueueChan <- len(partitionedBatch):
	default:
	}

	return nil
}

// enqueueEnrichment sends keys to the enricher under enricherMu.RLock.
// Extracted to allow defer-based unlock and avoid manual unlock on error paths.
func (i *EmbeddingIndex) enqueueEnrichment(promptKeys [][]byte, sync bool) error {
	i.enricherMu.RLock()
	defer i.enricherMu.RUnlock()

	if len(promptKeys) == 0 || i.enricher == nil {
		return nil
	}
	if sync {
		// SyncLevelAknn should have all enrichments pre-computed in dbWrapper.preEnrichBatch()
		// before the Raft proposal. If we still have promptKeys here with sync=true,
		// it means enrichments weren't pre-computed for some reason.
		//
		// We CANNOT call TrySyncEmbed() here because it causes a deadlock:
		// 1. Batch() is called with SyncLevelAknn (during Raft apply)
		// 2. TrySyncEmbed() → TrySyncChunk() → persistChunks() tries to submit a NEW Raft proposal
		// 3. But we're already inside a Raft apply, so the new proposal deadlocks
		//
		// SOLUTION: Log error for visibility, but enqueue to async enricher to handle
		i.logger.Error("SyncLevelAknn used but enrichments not pre-computed (bug in pre-enrichment logic)",
			zap.Int("numKeys", len(promptKeys)),
			zap.String("index", i.name),
			zap.ByteStrings("keys", promptKeys),
		)
		// Still enqueue to async enricher so embeddings eventually get generated
		if err := i.enricher.EnrichBatch(promptKeys); err != nil {
			return fmt.Errorf("enriching missing keys asynchronously: %w", err)
		}
	} else {
		// Async path - enqueue to WAL
		i.logger.Debug("Sending keys to enricher for async embedding",
			zap.Int("numKeys", len(promptKeys)),
			zap.String("index", i.name),
		)
		if err := i.enricher.EnrichBatch(promptKeys); err != nil {
			return fmt.Errorf("enriching key: %w", err)
		}
	}
	return nil
}

// ComputeEnrichments generates embeddings synchronously and returns the writes
// This is called by dbWrapper before Raft proposal for SyncLevelEnrichments
// Returns embedding writes without persisting (caller will include in batch)
// Also returns failedKeys that need async enrichment after persistence
func (ei *EmbeddingIndex) ComputeEnrichments(
	ctx context.Context,
	writes [][2][]byte,
) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error) {
	ei.enricherMu.RLock()
	defer ei.enricherMu.RUnlock()

	// Only compute if we have an enricher (leader-only)
	if ei.enricher == nil {
		return nil, nil, nil
	}

	// Filter writes to keys in our range AND build maps for pre-enrichment
	filteredKeys := make([][]byte, 0, len(writes))
	documentValues := make(map[string][]byte, len(writes)) // Map of key -> zstd-compressed value

	for _, write := range writes {
		key := write[0]
		value := write[1]

		if !ei.byteRange.Contains(key) {
			continue
		}

		// Store value in map for downstream enrichers
		documentValues[string(key)] = value

		// Validate document value is decodeable (zstd-compressed or plain JSON)
		if err := storeutils.ValidateDocumentJSON(value); err != nil {
			ei.logger.Warn("Failed to decode document JSON in pre-enrichment",
				zap.String("key", types.FormatKey(key)),
				zap.Error(err))
			continue
		}

		filteredKeys = append(filteredKeys, key)
	}

	if len(filteredKeys) == 0 {
		return nil, nil, nil
	}

	// Create a wrapper generatePrompts function that uses ExtractPrompt on in-memory documents
	// instead of fetching from Pebble (which doesn't have them yet during pre-enrichment)
	generatePromptsFromMemory := func(ctx context.Context, inputStates []storeutils.DocumentScanState) ([][]byte, []string, []uint64, error) {
		keys := make([][]byte, 0, len(inputStates))
		prompts := make([]string, 0, len(inputStates))
		hashIDs := make([]uint64, 0, len(inputStates))

		for _, state := range inputStates {
			// Document should be pre-populated in state.Document by the enricher
			if state.Document == nil {
				ei.logger.Warn("Document is nil in state during pre-enrichment, skipping",
					zap.String("key", types.FormatKey(state.CurrentDocKey)))
				continue
			}

			// Use ExtractPrompt to extract text from document without reading from Pebble
			prompt, hashID, err := ei.ExtractPrompt(state.Document)
			if err != nil {
				ei.logger.Warn("Failed to extract prompt from document",
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

	// Call the enricher to generate embeddings AND chunks WITHOUT persisting
	// This uses the new GenerateEmbeddingsWithoutPersist() method that returns embedding + chunk data
	// Pass documentValues map so downstream enrichers (e.g., ChunkingEnricher) can access compressed values
	embeddingWrites, chunkWrites, failed, err := ei.enricher.GenerateEmbeddingsWithoutPersist(ctx, filteredKeys, documentValues, generatePromptsFromMemory)
	if err != nil {
		ei.logger.Warn("Failed to generate embeddings in pre-enrichment",
			zap.Error(err),
			zap.String("index", ei.name),
			zap.Int("numKeys", len(filteredKeys)))
		// Return error - caller will decide whether to fail or fall back to async
		return nil, nil, fmt.Errorf("generating embeddings: %w", err)
	}

	// NOTE: We do NOT queue failed keys here. They will be automatically picked up
	// by the async enrichment pipeline when db.Batch() -> indexManager.Batch() runs.
	// The index manager will detect chunks without embeddings and queue them for async enrichment.
	// This ensures chunks are persisted to Pebble BEFORE being queued to the async enricher.

	// Combine embedding writes and chunk writes
	allEnrichmentWrites := make([][2][]byte, 0, len(embeddingWrites)+len(chunkWrites))
	allEnrichmentWrites = append(allEnrichmentWrites, embeddingWrites...)
	allEnrichmentWrites = append(allEnrichmentWrites, chunkWrites...)

	if ei.conf.Chunker != nil {
		failedSet := make(map[string]struct{}, len(failed))
		for _, key := range failed {
			failedSet[string(key)] = struct{}{}
		}

		desiredByDoc := make(map[string][][]byte, len(filteredKeys))
		for _, chunkWrite := range chunkWrites {
			docKey, _, ok := storeutils.ParseChunkKey(chunkWrite[0])
			if !ok {
				continue
			}
			sKey := string(docKey)
			desiredByDoc[sKey] = append(desiredByDoc[sKey], chunkWrite[0])
		}

		for _, docKey := range filteredKeys {
			if _, failed := failedSet[string(docKey)]; failed {
				continue
			}
			deleteWrites, err := collectChunkReplacementDeletes(
				ei.db,
				docKey,
				ei.name,
				desiredByDoc[string(docKey)],
				ei.embedderSuffix,
				ei.summarizerSuffix,
			)
			if err != nil {
				return nil, nil, fmt.Errorf("collecting stale chunk deletes: %w", err)
			}
			allEnrichmentWrites = append(allEnrichmentWrites, deleteWrites...)
		}
	}

	ei.logger.Debug("ComputeEnrichments completed",
		zap.String("index", ei.name),
		zap.Int("totalKeys", len(filteredKeys)),
		zap.Int("embeddingsGenerated", len(embeddingWrites)),
		zap.Int("chunksGenerated", len(chunkWrites)),
		zap.Int("failedKeys", len(failed)))

	return allEnrichmentWrites, failed, nil
}

func (i *EmbeddingIndex) LeaderFactory(
	ctx context.Context,
	persistFunc PersistFunc,
) (err error) {
	var persistEmbeddings = func(ctx context.Context, vectors [][]float32, hashIDs []uint64, keys [][]byte) error {
		writes := make([][2][]byte, 0, len(keys))
		if len(vectors) > 0 {
			embeddingPersistOps.WithLabelValues(i.name).Add(float64(len(vectors)))
		}
		for j, k := range keys {
			if !i.byteRange.Contains(k) {
				return common.NewErrKeyOutOfRange(k, i.byteRange)
			}
			key := slices.Clone(k)
			key = bytes.TrimSuffix(key, i.summarizerSuffix)
			key = append(key, i.embedderSuffix...)

			// Encode embedding with hashID prefix: [hashID:uint64][vector]
			vecBuf := make([]byte, 0, 8+4*(len(vectors[j])+1))
			vecBuf, err := vectorindex.EncodeEmbeddingWithHashID(vecBuf, vectors[j], hashIDs[j])
			if err != nil {
				return fmt.Errorf("encoding embedding: %w", err)
			}
			writes = append(writes, [2][]byte{key, vecBuf})
		}
		return persistFunc(ctx, writes)
	}

	if i.embedderConf != nil {
		i.logger.Info("Creating enricher for index v2", zap.String("name", i.name), zap.Any("config", i.embedderConf), zap.Any("summarizerConfig", i.summarizerConf))

		// Use chunking config directly - respects the Provider field (antfly vs termite)
		var chunkingConfig *chunking.ChunkerConfig
		if i.conf.Chunker != nil {
			chunkingConfig = i.conf.Chunker
		}

		i.enricherMu.Lock()
		CleanupEnricherWALs(i.indexPath)

		// Determine if we need pipeline enricher (supports chunking and/or summarization)
		needsPipeline := i.summarizerConf != nil || chunkingConfig != nil

		if needsPipeline {
			// Define persist functions for pipeline enricher
			var persistSummaries PersistSummariesFunc
			if i.summarizerConf != nil {
				persistSummaries = func(ctx context.Context, keys [][]byte, hashIDs []uint64, summaries []string) error {
					writes := make([][2][]byte, 0, len(keys))
					for j, k := range keys {
						if !i.byteRange.Contains(k) {
							return common.NewErrKeyOutOfRange(k, i.byteRange)
						}
						key := append(slices.Clone(k), i.summarizerSuffix...)

						i.logger.Debug("Persisting summary for key",
							zap.String("key", types.FormatKey(key)),
						)

						b := make([]byte, 0, len(summaries[j])+8)
						b = encoding.EncodeUint64Ascending(b, hashIDs[j])
						b = fmt.Append(b, summaries[j])
						writes = append(writes, [2][]byte{key, b})
					}
					return persistFunc(ctx, writes)
				}
			}

			var persistChunks PersistChunksFunc
			if chunkingConfig != nil {
				persistChunks = func(ctx context.Context, keys [][]byte, hashIDs []uint64, chunks [][]chunking.Chunk) error {
					writes := make([][2][]byte, 0, len(keys)*2)
					for j, k := range keys {
						if !i.byteRange.Contains(k) {
							return common.NewErrKeyOutOfRange(k, i.byteRange)
						}
						desiredChunkKeys := make([][]byte, 0, len(chunks[j]))
						// Persist each chunk
						for _, chunk := range chunks[j] {
							// Skip chunks with empty text (no point storing/indexing them)
							if len(strings.TrimSpace(chunk.GetText())) == 0 {
								i.logger.Debug("Skipping chunk with empty text in async persist",
									zap.String("key", types.FormatKey(k)),
									zap.Uint32("chunkID", chunk.Id))
								continue
							}

							// Use :cft: suffix when full_text is configured, otherwise use :c:
							var chunkKey []byte
							if chunking.GetFullTextIndex(*chunkingConfig) != nil {
								chunkKey = storeutils.MakeChunkFullTextKey(k, i.name, chunk.Id)
							} else {
								chunkKey = storeutils.MakeChunkKey(k, i.name, chunk.Id)
							}
							desiredChunkKeys = append(desiredChunkKeys, chunkKey)
							// Marshal chunk JSON
							chunkJSON, err := json.Marshal(chunk)
							if err != nil {
								return fmt.Errorf("marshaling chunk: %w", err)
							}
							// Encode: [hashID:uint64][chunkJSON]
							b := make([]byte, 0, len(chunkJSON)+8)
							b = encoding.EncodeUint64Ascending(b, hashIDs[j])
							b = append(b, chunkJSON...)
							writes = append(writes, [2][]byte{chunkKey, b})
						}

						deleteWrites, err := collectChunkReplacementDeletes(
							i.db,
							k,
							i.name,
							desiredChunkKeys,
							i.embedderSuffix,
							i.summarizerSuffix,
						)
						if err != nil {
							return fmt.Errorf("collecting stale chunk deletes: %w", err)
						}
						writes = append(writes, deleteWrites...)
					}
					return persistFunc(ctx, writes)
				}
			}

			// Determine if chunks should be stored (default: false for ephemeral mode)
			storeChunks := false
			if chunkingConfig != nil {
				storeChunks = chunkingConfig.StoreChunks
			}

			i.logger.Info("Creating pipeline for index v2", zap.String("name", i.name), zap.Bool("storeChunks", storeChunks))
			if i.enricher, err = NewPipelineEnricher(ctx, PipelineEnricherConfig{
				Logger:            i.logger,
				AntflyConfig:      i.antflyConfig,
				DB:                i.db,
				Dir:               i.indexPath,
				Name:              i.name,
				EmbSuffix:         i.embedderSuffix,
				SumSuffix:         i.summarizerSuffix,
				ByteRange:         i.byteRange,
				EmbedderConfig:    *i.embedderConf,
				SummarizerConfig:  i.summarizerConf,
				ChunkingConfig:    chunkingConfig,
				StoreChunks:       storeChunks,
				GeneratePrompts:   i.GeneratePrompts,
				PersistEmbeddings: persistEmbeddings,
				PersistSummaries:  persistSummaries,
				PersistChunks:     persistChunks,
				ExtractPrompt:     i.ExtractPrompt,
				PersistFunc:       persistFunc,
			}); err != nil {
				i.enricherMu.Unlock()
				return fmt.Errorf("creating enricher: %w", err)
			}
		} else {
			// Simple embedding enricher (no chunking, no summarization)
			if i.enricher, err = NewEmbeddingEnricher(ctx, i.logger, i.antflyConfig, i.db, i.indexPath, i.name, storeutils.DBRangeStart, i.embedderSuffix, i.byteRange, *i.embedderConf, i.GeneratePrompts, persistEmbeddings, nil); err != nil {
				i.enricherMu.Unlock()
				return fmt.Errorf("creating enricher: %w", err)
			}
		}
		i.enricherMu.Unlock()

		<-ctx.Done()

		i.enricherMu.Lock()
		_ = i.enricher.Close()
		i.enricher = nil
		CleanupEnricherWALs(i.indexPath)
		i.enricherMu.Unlock()

		i.logger.Info("Enricher for index v2 closed", zap.String("name", i.name))
	}
	return nil
}

func (ei *EmbeddingIndex) Open(
	rebuild bool,
	tableSchema *schema.TableSchema,
	byteRange types.Range,
) error {
	ei.Lock()
	defer ei.Unlock()
	ei.byteRange = byteRange
	ei.schema = tableSchema
	ei.logger.Debug("Opening embeddings index", zap.String("path", ei.indexPath))
	if rebuild {
		_ = os.RemoveAll(ei.indexPath)
		batch := ei.db.NewBatch()
		if err := vectorindex.DeleteIndex(batch, ei.name); err != nil {
			_ = batch.Close()
			return fmt.Errorf("deleting vector index: %w", err)
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			_ = batch.Close()
			return fmt.Errorf("committing previous vector index deletion: %w", err)
		}
		_ = batch.Close()
	} else if !rebuild && !ei.memOnly {
		if _, statErr := os.Stat(filepath.Join(ei.indexPath, "indexWAL")); errors.Is(statErr, os.ErrNotExist) {
			ei.logger.Info("Rebuilding index, indexWAL does not exist")
			rebuild = true
		}
	}

	// Check for rebuild state
	var rebuildFrom []byte
	if !rebuild {
		var needsRebuild bool
		var err error
		rebuildFrom, needsRebuild, err = ei.rebuildState.Check()
		if err != nil {
			return fmt.Errorf("checking rebuild status: %w", err)
		}
		if needsRebuild {
			ei.logger.Debug("Rebuild status found: resuming rebuilding index",
				zap.ByteString("status", rebuildFrom),
			)
			rebuild = true
		} else {
			ei.logger.Debug("Rebuild status not found, assuming rebuild is not needed")
		}
	}

	// Create index directory if needed
	if !ei.memOnly {
		if err := os.MkdirAll(ei.indexPath, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
			ei.logger.Error(
				"Failed to create vectorindex directory",
				zap.Error(err),
				zap.String("path", ei.indexPath),
			)
			return fmt.Errorf("failed to create vectorindex directory: %w", err)
		}
	}

	if rebuild {
		if err := ei.rebuildState.Update(rebuildFrom); err != nil {
			ei.logger.Warn(
				"Failed to set rebuild status for embeddings index",
				zap.String("path", ei.indexPath),
				zap.Error(err),
			)
			return fmt.Errorf("setting rebuild status: %w", err)
		}
	}

	var err error
	if ei.walBuf, err = inflight.NewWALBufferWithOptions(ei.logger, ei.indexPath, "indexWAL", inflight.WALBufferOptions{
		MaxRetries: 10,
		Metrics:    NewPrometheusWALMetrics("aknn"),
	}); err != nil {
		return fmt.Errorf("creating WALBuffer: %w", err)
	}
	ei.logger.Debug("Opening vector index for embeddings wrapper", zap.String("path", ei.indexPath))
	if ei.idx, err = ei.newVectorIndex(ei.dimension); err != nil {
		ei.logger.Debug(
			"Failed to open vector index for embeddings wrapper",
			zap.Error(err),
			zap.String("path", ei.indexPath),
		)
		return fmt.Errorf("creating aknn index: %w", err)
	}
	// If we don't wait for the backfill to complete, deleted items
	// can be revived by the backfill process
	backfillWait := make(chan struct{})
	ei.backfillDone = nil
	if rebuild || ei.memOnly {
		ei.backfillDone = make(chan struct{})
	}
	ei.enqueueChan = make(chan int, 5)
	ei.eg.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered from panic:", r)              // Print the panic value
				fmt.Println("Stack trace:\n", string(debug.Stack())) // Print the stack trace
				panic(r)
			}
		}()
		select {
		case <-ei.egCtx.Done():
			return nil
		case <-backfillWait:
		}
		const maxJitter = time.Millisecond * 200
		jitter := maxJitter - rand.N(maxJitter) //nolint:gosec // G404: non-security randomness for ML/jitter
		t := time.NewTimer(ei.flushTime + jitter)
		enqueueCounter := 0
		dequeue := func() error {
			enqueueCounter = 0
			ops := vectorOpPool.Get().(*vectorOp)
			ops.ei = ei
			defer func() {
				ops.reset()
				vectorOpPool.Put(ops)
			}()
			if err := ei.walBuf.Dequeue(ei.egCtx, ops, embeddingsMaxBatches); err != nil {
				if errors.Is(err, inflight.ErrBufferClosed) ||
					errors.Is(err, pebble.ErrClosed) {
					return err
				}
				ei.logger.Error("Failed to dequeue from WAL buffer", zap.Error(err))
			}
			empty := len(ops.Vectors) == 0 && len(ops.Deletes) == 0
			if empty {
				t.Reset(ei.flushTime + jitter)
			} else {
				t.Reset(100*time.Millisecond + jitter)
			}
			return nil
		}
		defer t.Stop()
		for {
			// Check if paused at start of each iteration (no mutex needed)
			if ei.paused.Load() {
				// Send acknowledgment without holding any mutex
				select {
				case ei.pauseAckCh <- struct{}{}:
				default:
					// Already acknowledged
				}

				// Now wait for resume
				ei.pauseMu.Lock()
				for ei.paused.Load() {
					// Send ack before each wait in case Pause() was called again
					select {
					case ei.pauseAckCh <- struct{}{}:
					default:
					}
					ei.pauseCond.Wait()
				}
				ei.pauseMu.Unlock()
			}

			select {
			case <-ei.egCtx.Done():
				ei.logger.Debug("Plexer: context done, exiting")
				return nil
			case n := <-ei.enqueueChan:
				ei.logger.Debug("Plexer: received enqueue signal", zap.Int("count", n), zap.Int("enqueueCounter", enqueueCounter))
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
	if !rebuild && !ei.memOnly {
		ei.logger.Debug("Skipping rebuild of embeddings index", zap.String("path", ei.indexPath))
		// This is a recovery (restart) and not a rebuild
		// Recover the index do not rebuild it
		// but memOnlyIdx has to recover every time
		close(backfillWait)
		return nil
	}
	// Equivalent to min of ei.byteRange[0] and rebuildFrom
	if bytes.Compare(rebuildFrom, ei.byteRange[0]) < 0 {
		rebuildFrom = ei.byteRange[0]
	}
	ei.eg.Go(func() error {
		err := func() (err error) {
			defer pebbleutils.RecoverPebbleClosed(&err)
			defer close(backfillWait)
			defer close(ei.backfillDone)

			totalScanned := 0
			indexedCount := 0
			batch := &EmbeddingsBatch{
				idx:   ei,
				batch: &vectorindex.Batch{},
			}
			ei.logger.Debug("Backfilling embeddings index", zap.String("path", ei.indexPath))
			maxBatchSize := 5000

			flushBatch := func(currKey []byte) error {
				if len(batch.batch.Vectors) > 0 {
					if err := batch.Commit(ei.egCtx); err != nil {
						return fmt.Errorf("committing batch during recovery: %w", err)
					}
					batch.Reset()
					ei.logger.Debug(
						"Rebuild: Committed batch",
						zap.Int("count", len(batch.batch.Vectors)),
						zap.Int("total_indexed", indexedCount),
					)
				}
				batch.Reset()
				if currKey != nil {
					if indexedCount > 0 {
						if err := ei.rebuildState.Update(currKey); err != nil {
							ei.logger.Warn(
								"Failed to set rebuild status for embeddings index",
								zap.Int("count", len(batch.batch.Vectors)),
								zap.Int("total_indexed", indexedCount),
								zap.String("path", ei.indexPath),
								zap.Error(err),
							)
							return fmt.Errorf("setting rebuild status: %w", err)
						}
					}
				} else {
					// This means we're done with the backfill so delete the rebuild status
					if err := ei.rebuildState.Clear(); err != nil {
						ei.logger.Warn(
							"Failed to delete rebuild status for embeddings index",
							zap.Int("count", len(batch.batch.Vectors)),
							zap.Int("total_indexed", indexedCount),
							zap.String("path", ei.indexPath),
							zap.Error(err),
						)
						return fmt.Errorf("deleting rebuild status: %w", err)
					}
				}
				return nil
			}

			var lastKey []byte

			err = storeutils.Scan(ei.egCtx, ei.db, storeutils.ScanOptions{
				LowerBound: rebuildFrom,
				UpperBound: ei.byteRange[1],
				// Embeddings are persisted on explicit embedding keys for both
				// generated and user-provided vectors, including chunk embeddings.
				SkipPoint: func(userKey []byte) bool {
					return !bytes.HasSuffix(userKey, ei.embedderSuffix)
				},
			}, func(key []byte, value []byte) (bool, error) {
				totalScanned++
				lastKey = slices.Clone(key)

				var embedding vector.T
				if bytes.HasSuffix(key, ei.embedderSuffix) {
					// Decode embedding directly from value
					_, embedding, _, err = vectorindex.DecodeEmbeddingWithHashID(value)
					if err != nil {
						return false, fmt.Errorf("decoding embedding for key %s: %w", key, err)
					}
				}

				if len(embedding) == 0 {
					// Empty embedding - document doesn't have the embedding field yet
					return true, nil
				}

				indexedCount++
				w := slices.Clone(key[:len(key)-len(ei.embedderSuffix)])
				if err := batch.InsertSingle(w, embedding); err != nil {
					return false, fmt.Errorf("inserting key to batch %s: %w", key, err)
				}

				if len(batch.batch.Vectors) >= maxBatchSize {
					if err := flushBatch(lastKey); err != nil {
						return false, fmt.Errorf("flushing batch during recovery: %w", err)
					}
				}
				return true, nil
			})

			if err != nil {
				return err
			}

			ei.logger.Debug("Rebuild: Finished Pebble iterator for embeddings index",
				zap.Stringer("range", byteRange),
				zap.String("path", ei.indexPath),
				zap.Int("total_indexed", indexedCount),
				zap.Int("total_scanned", totalScanned))

			if err := flushBatch(nil); err != nil {
				return fmt.Errorf("flushing batch during recovery: %w", err)
			}
			return nil
		}()
		if err != nil &&
			(!errors.Is(err, inflight.ErrBufferClosed) && !errors.Is(err, pebble.ErrClosed) && !errors.Is(err, context.Canceled)) {
			ei.logger.Error("Failed to backfill embeddings index", zap.String("path", ei.indexPath),
				zap.Error(err))
		}
		return err
	})
	return nil
}

func (ei *EmbeddingIndex) Delete() error {
	batch := ei.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()
	if ei.embedderConf != nil {
		iter, err := ei.db.NewIter(&pebble.IterOptions{
			LowerBound: ei.byteRange[0],
			UpperBound: ei.byteRange[1],
			SkipPoint: func(userKey []byte) bool {
				if ei.summarizerConf == nil {
					return !bytes.HasSuffix(userKey, ei.embedderSuffix)
				}
				return !bytes.HasSuffix(userKey, ei.embedderSuffix) && !bytes.HasSuffix(userKey, ei.summarizerSuffix)
			},
		})
		if err != nil {
			return fmt.Errorf("creating iterator for deletion: %w", err)
		}
		defer func() { _ = iter.Close() }()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := batch.Delete(iter.Key(), nil); err != nil {
				return fmt.Errorf("adding deletion to batch for key %s: %w", iter.Key(), err)
			}
		}
	}

	_ = os.RemoveAll(ei.indexPath)
	if err := vectorindex.DeleteIndex(batch, ei.name); err != nil {
		return fmt.Errorf("deleting vector index: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing enrichment deletions: %w", err)
	}
	return nil
}

func (ei *EmbeddingIndex) ExtractPrompt(
	val map[string]any,
) (prompt string, hashID uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				// FIXME (probably need to work out better shutdown semantics, this has a code smell)
				if errors.Is(e, pebble.ErrClosed) || strings.Contains(e.Error(), "closed LogWriter") {
					err = e
					return
				}
			}
			panic(r)
		}
	}()
	prompt, hashID, err = ei.RenderPrompt(val)
	if err != nil {
		// Propagate prompt failure classification so callers can mark duds or retry.
		if errors.Is(err, ErrPermanentPromptFailure) || errors.Is(err, ErrTransientPromptFailure) {
			return "", 0, err
		}
		// RenderPrompt returns errors for missing fields and non-string fields.
		// Internal callers (enrichment pipeline) should skip these documents
		// rather than fail, so convert to empty prompt.
		return "", 0, nil
	}
	return prompt, hashID, nil
}

type EmbeddingsBatch struct {
	batch *vectorindex.Batch
	idx   *EmbeddingIndex
}

func (b *EmbeddingsBatch) Insert(keys [][]byte, vectors []vector.T) error {
	ids := make([]uint64, len(keys))
	for i, key := range keys {
		ids[i] = xxhash.Sum64(key) // Use xxhash to generate a unique ID for the vector
	}
	b.batch.Insert(ids, vectors, keys)
	return nil
}
func (b *EmbeddingsBatch) InsertSingle(key []byte, vec vector.T) error {
	id := xxhash.Sum64(key) // Use xxhash to generate a unique ID for the vector
	b.batch.InsertSingle(id, vec, key)
	return nil
}
func (b *EmbeddingsBatch) Delete(keys [][]byte) error {
	ids := make([]uint64, len(keys))
	for i, key := range keys {
		ids[i] = xxhash.Sum64(key) // Use xxhash to generate a unique ID for the vector
	}
	b.batch.Delete(ids...)
	return nil
}

func (b *EmbeddingsBatch) Reset() {
	b.batch.Reset()
}

func (b *EmbeddingsBatch) Commit(ctx context.Context) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	b.idx.Lock()
	defer b.idx.Unlock()
	if b.idx.idx == nil {
		return nil
	}
	if len(b.batch.Vectors) == 0 && len(b.batch.Deletes) == 0 {
		return nil
	}
	b.idx.logger.Debug("Committing batch",
		zap.Int("deletes", len(b.batch.Deletes)),
		zap.Int("metadata", len(b.batch.MetadataList)),
		zap.Int("writes", len(b.batch.Vectors)),
	)
	writeOps.WithLabelValues(b.idx.name).Add(float64(len(b.batch.Vectors)))
	deleteOps.WithLabelValues(b.idx.name).Add(float64(len(b.batch.Deletes)))
	if err := b.idx.idx.Batch(ctx, b.batch); err != nil {
		return fmt.Errorf("executing vector index operations: %w", err)
	}
	return nil
}

func (ei *EmbeddingIndex) UpdateSchema(newSchema *schema.TableSchema) error {
	ei.Lock()
	defer ei.Unlock()
	ei.schema = newSchema
	return nil
}
func (ei *EmbeddingIndex) UpdateRange(newRange types.Range) error {
	ei.Lock()
	defer ei.Unlock()
	ei.byteRange = newRange
	return nil
}

// WaitForBackfill waits for the index backfill to complete.
// Returns immediately if no backfill is running or context is canceled.
func (ei *EmbeddingIndex) WaitForBackfill(ctx context.Context) {
	if ei.backfillDone != nil {
		select {
		case <-ei.backfillDone:
		case <-ctx.Done():
		}
	}
}

// Pause stops the plexer from processing and syncs the WAL
func (ei *EmbeddingIndex) Pause(ctx context.Context) error {
	ei.pauseMu.Lock()

	// Check if already paused
	if ei.paused.Load() {
		ei.pauseMu.Unlock()
		return nil
	}

	// Set paused flag
	ei.paused.Store(true)

	// Drain any stale acknowledgments from the channel before signaling
	select {
	case <-ei.pauseAckCh:
		// Drained stale ack
	default:
		// No stale ack
	}

	// Release mutex BEFORE waiting for ack to avoid deadlock with plexer
	ei.pauseMu.Unlock()

	// Signal plexer via enqueueChan to trigger immediate check
	// Use context timeout to avoid blocking forever if enqueueChan is full
	select {
	case ei.enqueueChan <- 0:
	case <-ctx.Done():
		ei.paused.Store(false)
		return ctx.Err()
	}

	// Wait for plexer acknowledgment via separate channel (with context timeout)
	select {
	case <-ei.pauseAckCh:
		// Plexer acknowledged pause
	case <-ctx.Done():
		ei.paused.Store(false)
		return ctx.Err()
	}

	// Sync WAL to disk
	if err := ei.walBuf.Sync(); err != nil {
		ei.paused.Store(false)
		return fmt.Errorf("syncing WAL during pause: %w", err)
	}

	// Also sync enricher WAL if present
	ei.enricherMu.RLock()
	enricher := ei.enricher
	ei.enricherMu.RUnlock()

	if enricher != nil {
		// Enricher has its own WAL that needs syncing
		// Note: We don't have direct access to the enricher's WAL,
		// but the enricher should handle pause state through the paused flag
		ei.logger.Debug("Paused embedding index with enricher", zap.String("name", ei.name))
	}

	ei.logger.Debug("Paused embedding index", zap.String("name", ei.name))
	return nil
}

// Resume resumes the plexer processing
func (ei *EmbeddingIndex) Resume() {
	ei.pauseMu.Lock()
	defer ei.pauseMu.Unlock()

	if !ei.paused.Load() {
		return
	}

	ei.paused.Store(false)
	ei.pauseCond.Broadcast()

	ei.logger.Debug("Resumed embedding index", zap.String("name", ei.name))
}
