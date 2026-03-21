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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/reranking"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/s3storage"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/sear"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/remote"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/theory/jsonpath"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

var (
	DBRangeStart          = storeutils.DBRangeStart
	DBRangeEnd            = storeutils.DBRangeEnd
	EmbeddingSuffix       = storeutils.EmbeddingSuffix
	SummarySuffix         = storeutils.SummarySuffix
	SparseSuffix          = storeutils.SparseSuffix
	MetadataPrefix        = storeutils.MetadataPrefix
	schemaKey             = slices.Concat(MetadataPrefix, []byte("schema"))
	byteRangeKey          = slices.Concat(MetadataPrefix, []byte("byterange"))
	indexesKey            = slices.Concat(MetadataPrefix, []byte("indexes"))
	versionKey            = slices.Concat(MetadataPrefix, []byte("version"))
	splitStateKey         = slices.Concat(MetadataPrefix, []byte("splitstate"))
	splitDeltaSeqKey      = slices.Concat(MetadataPrefix, []byte("splitdelta:seq"))
	splitDeltaFinalSeqKey = slices.Concat(MetadataPrefix, []byte("splitdelta:finalseq"))
	splitDeltaPrefix      = slices.Concat(MetadataPrefix, []byte("splitdelta:entry:"))

	// DefaultPebbleCacheSizeMB is the default Pebble cache size per shard in megabytes.
	// Tests can override this to reduce memory usage.
	DefaultPebbleCacheSizeMB int64 = 320 // 5 * 64MB
)

// CurrentStorageVersion defines the current storage format version
const CurrentStorageVersion = 1

// Transaction status constants
const (
	TxnStatusPending   int32 = 0
	TxnStatusCommitted int32 = 1
	TxnStatusAborted   int32 = 2
)

// TxnRecord is the on-disk JSON representation of a transaction's coordinator record.
// TxnID and Participants are []byte which encoding/json base64-encodes automatically.
type TxnRecord struct {
	TxnID                []byte   `json:"txn_id"`
	Timestamp            uint64   `json:"timestamp"`
	CommitVersion        uint64   `json:"commit_version"`
	Status               int32    `json:"status"`
	Participants         [][]byte `json:"participants"`
	ResolvedParticipants []string `json:"resolved_participants"`
	CreatedAt            int64    `json:"created_at"`
	FinalizedAt          int64    `json:"committed_at"`
}

// Version returns the commit version if set, otherwise falls back to the begin timestamp.
func (r *TxnRecord) Version() uint64 {
	if r.CommitVersion > 0 {
		return r.CommitVersion
	}
	return r.Timestamp
}

// TxnIntent is the on-disk JSON representation of a provisional transaction intent.
type TxnIntent struct {
	TxnID            []byte `json:"txn_id"`
	UserKey          []byte `json:"user_key"`
	Value            []byte `json:"value"`
	Timestamp        uint64 `json:"timestamp"`
	CoordinatorShard []byte `json:"coordinator_shard"`
	Status           int32  `json:"status"`
	IsDelete         bool   `json:"is_delete"`
}

// Transaction key prefixes
var (
	txnRecordsPrefix = []byte("\x00\x00__txn_records__:")
	txnIntentsPrefix = []byte("\x00\x00__txn_intents__:")
)

// ParseFieldSelection separates regular document fields (for JSONPath querying) from special fields (_embeddings, _summaries, _chunks).
// Regular fields can use RFC 9535 JSONPath syntax like "user.name", "items[*].price", or "$.apps.*".
// Special fields use underscore prefix and optional dot notation for index selection.
//
// Default behavior when no special fields are specified:
//   - Embeddings: excluded
//   - Summaries: included (AllSummaries = true)
//   - Chunks: excluded
//
// Parameters:
//   - fields: Array of field paths to select. Can be empty for "select all".
//   - star: If true, indicates "select all" behavior (no filtering on regular fields).
//
// Returns:
//   - regularPaths: Field paths for JSONPath querying of the document
//   - queryOpts: QueryOptions configured for which special fields to include
func ParseFieldSelection(
	fields []string,
) (regularPaths []string, queryOpts storeutils.QueryOptions) {
	// Apply defaults: include summaries, exclude embeddings and chunks
	queryOpts.AllSummaries = true
	queryOpts.AllEmbeddings = false
	queryOpts.AllChunks = false

	var embeddingIndexes []string
	var summaryIndexes []string

	for _, field := range fields {
		if strings.HasPrefix(field, "_") {
			// Parse special field
			fieldType, includeAll, indexes := parseSpecialField(field)

			switch fieldType {
			case "_embeddings":
				if includeAll {
					queryOpts.AllEmbeddings = true
					embeddingIndexes = nil
				} else {
					embeddingIndexes = append(embeddingIndexes, indexes...)
				}
			case "_summaries":
				if includeAll {
					queryOpts.AllSummaries = true
					summaryIndexes = nil
				} else {
					// User explicitly specified specific summaries, disable default "all summaries"
					if len(summaryIndexes) == 0 {
						queryOpts.AllSummaries = false
					}
					summaryIndexes = append(summaryIndexes, indexes...)
				}
			case "_chunks":
				if includeAll {
					queryOpts.AllChunks = true
				}
				// Note: We don't support specific chunk index selection yet,
				// only AllChunks mode (_chunks or _chunks.*)
			}
		} else {
			// Regular field - pass through for JSONPath
			regularPaths = append(regularPaths, field)
		}
	}

	// Build suffixes for specific indexes if not requesting all
	if !queryOpts.AllEmbeddings && len(embeddingIndexes) > 0 {
		queryOpts.EmbeddingSuffixes = make([][]byte, 0, len(embeddingIndexes))
		for _, indexName := range embeddingIndexes {
			// Suffix format: ":i:<indexName>:e"
			suffix := []byte(":i:" + indexName + ":e")
			queryOpts.EmbeddingSuffixes = append(queryOpts.EmbeddingSuffixes, suffix)
		}
	}

	if !queryOpts.AllSummaries && len(summaryIndexes) > 0 {
		queryOpts.SummarySuffixes = make([][]byte, 0, len(summaryIndexes))
		for _, indexName := range summaryIndexes {
			// Suffix format: ":i:<indexName>:s"
			suffix := []byte(":i:" + indexName + ":s")
			queryOpts.SummarySuffixes = append(queryOpts.SummarySuffixes, suffix)
		}
	}

	return regularPaths, queryOpts
}

// parseSpecialField parses a special field string and returns parsed components.
// Supported formats:
//   - "_embeddings" → all embeddings
//   - "_embeddings.index_name" → specific embedding index
//   - "_summaries" → all summaries
//   - "_summaries.index_name" → specific summary index
//   - "_chunks" → all chunks
//   - "_chunks.*" → all chunks (wildcard syntax)
//
// Returns:
//   - fieldType: The type of special field ("_embeddings", "_summaries", or "_chunks")
//   - includeAll: true if all indexes should be included
//   - indexes: slice of specific index names (empty if includeAll is true)
func parseSpecialField(field string) (fieldType string, includeAll bool, indexes []string) {
	parts := strings.SplitN(field, ".", 2)
	fieldType = parts[0]

	if len(parts) == 1 {
		// No index specified → include all
		return fieldType, true, nil
	}

	indexName := parts[1]
	if indexName == "*" {
		// Explicit wildcard → include all
		return fieldType, true, nil
	}

	// Specific index name
	return fieldType, false, []string{indexName}
}

var _ DB = (*DBImpl)(nil) // Compile-time check for DB interface implementation

type DB interface {
	Open(dir string, recovery bool, schema *schema.TableSchema, byteRange types.Range) error
	Close() error
	FindMedianKey() ([]byte, error)
	SetRange(types.Range) error
	UpdateRange(types.Range) error
	GetRange() (types.Range, error)
	// SplitState operations for Raft-replicated split state (replaces local pendingSplitKey)
	GetSplitState() *SplitState
	SetSplitState(*SplitState) error
	ClearSplitState() error
	GetSplitDeltaSeq() (uint64, error)
	GetSplitDeltaFinalSeq() (uint64, error)
	SetSplitDeltaFinalSeq(uint64) error
	ClearSplitDeltaFinalSeq() error
	ListSplitDeltaEntriesAfter(afterSeq uint64) ([]*SplitDeltaEntry, error)
	ClearSplitDeltaEntries() error
	// Shadow IndexManager operations for shard splits
	CreateShadowIndexManager(splitKey, originalRangeEnd []byte) error
	CloseShadowIndexManager() error
	GetShadowIndexManager() *IndexManager
	GetShadowIndexDir() string // Returns the shadow index directory path, empty if no shadow exists
	UpdateSchema(*schema.TableSchema) error
	Get(ctx context.Context, key []byte) (map[string]any, error)
	Scan(ctx context.Context, startKey, endKey []byte, scanOpts ScanOptions) (*ScanResult, error)
	// Batch must validate all keys fall within the currently owned byte range as messages might be replayed
	// after a split or merge, the caller can assume the batch doesn't contain overlapping updates
	// syncLevel controls when to return: Propose (after raft proposal), Write (after KV write), FullText (after full-text index WAL)
	Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte, syncLevel Op_SyncLevel) error
	// TODO (ajr) Should this be a standardized request format?
	Search(ctx context.Context, encodedRequest []byte) (encodedResponse []byte, err error)
	Split(currRange types.Range, splitKey []byte, destDir1, destDir2 string, prepareOnly bool) error
	FinalizeSplit(newRange types.Range) error
	Snapshot(id string) (int64, error)
	// diskSize is used for the split computation and empty for merge
	Stats() (diskSize uint64, empty bool, indexStats map[string]indexes.IndexStats, err error)

	AddIndex(config indexes.IndexConfig) error
	DeleteIndex(name string) error
	HasIndex(name string) bool
	GetIndexes() map[string]indexes.IndexConfig

	LeaderFactory(ctx context.Context, persistEmbeddings PersistFunc) error

	// Enrichment operations
	ExtractEnrichments(ctx context.Context, writes [][2][]byte) (embeddingWrites, summaryWrites, edgeWrites [][2][]byte, edgeDeletes [][]byte, err error)
	ComputeEnrichments(ctx context.Context, writes [][2][]byte) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error)

	// Transaction operations
	InitTransaction(ctx context.Context, op *InitTransactionOp) error
	CommitTransaction(ctx context.Context, op *CommitTransactionOp) (uint64, error)
	AbortTransaction(ctx context.Context, op *AbortTransactionOp) error
	WriteIntent(ctx context.Context, op *WriteIntentOp) error
	ResolveIntents(ctx context.Context, op *ResolveIntentsOp) error
	GetTransactionStatus(ctx context.Context, txnID []byte) (int32, error)
	GetCommitVersion(ctx context.Context, txnID []byte) (uint64, error)
	ListTxnRecords(ctx context.Context) ([]TxnRecord, error)
	ListTxnIntents(ctx context.Context) ([]TxnIntent, error)
	GetTimestamp(key []byte) (uint64, error)

	// Graph operations
	GetEdges(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]indexes.Edge, error)
	TraverseEdges(ctx context.Context, indexName string, startKey []byte, rules indexes.TraversalRules) ([]*indexes.TraversalResult, error)
	GetNeighbors(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]*indexes.TraversalResult, error)
	FindShortestPath(ctx context.Context, indexName string, source, target []byte, edgeTypes []string, direction indexes.EdgeDirection, weightMode indexes.PathWeightMode, maxDepth int, minWeight, maxWeight float64) (*indexes.Path, error)
}

// ShardNotifier provides a way for DBImpl to send notifications to other shards
type ShardNotifier interface {
	// NotifyResolveIntent sends a resolve intent RPC to a participant shard
	NotifyResolveIntent(ctx context.Context, shardID []byte, txnID []byte, status int32, commitVersion uint64) error
}

type DBImpl struct {
	antflyConfig *common.Config
	dir          string
	pdb          *pebble.DB
	pdbMu        sync.RWMutex // Protects pdb during close/reopen cycles (e.g., during splits)

	indexManager *IndexManager

	// Shadow IndexManager for split-off range during shard splits.
	// Created in PHASE_PREPARE, receives dual-writes during split,
	// closed in PHASE_FINALIZING when new shard takes over.
	shadowIndexManager *IndexManager

	logger *zap.Logger
	// Indexes configured at creation passed to IndexManager
	indexesMu sync.RWMutex
	indexes   map[string]indexes.IndexConfig

	byteRange          types.Range
	splitState         *SplitState // Raft-replicated split state (replaces local pendingSplitKey)
	splitDeltaSeq      uint64
	splitDeltaFinalSeq uint64
	schema             *schema.TableSchema
	cachedTTLDuration  time.Duration // Parsed from schema.TtlDuration; 0 means no TTL

	bufferPool  *sync.Pool
	encoderPool *sync.Pool // reusable zstd encoders

	// Snapshot storage abstraction
	snapStore snapstore.SnapStore

	// S3 storage backend for remote sstables (leader-only writes)
	isLeader  atomic.Bool                     // Indicates if this instance is the Raft leader
	s3Storage *s3storage.LeaderAwareS3Storage // S3 storage wrapper (nil if S3 disabled)

	isLeaderMu                 sync.Mutex    // Indicates if this instance is the leader for index management
	restartIndexManagerFactory chan struct{} // Used to restart the index manager factory

	// ShardNotifier for cross-shard transaction notifications
	shardNotifier ShardNotifier

	// proposeResolveIntentsFunc is set by dbWrapper to allow proposing resolve intents through Raft
	proposeResolveIntentsFunc func(ctx context.Context, op *ResolveIntentsOp) error

	// proposeAbortTransactionFunc is set by dbWrapper to allow proposing abort transaction through Raft.
	// Used by the recovery loop to auto-abort stale Pending transactions.
	proposeAbortTransactionFunc func(ctx context.Context, op *AbortTransactionOp) error

	cache *pebbleutils.Cache // shared Pebble block cache (may be nil)
}

// NewDBImpl creates a new DBImpl instance with the given configuration.
// The returned DBImpl must be opened with Open() before use.
func NewDBImpl(
	logger *zap.Logger,
	antflyConfig *common.Config,
	schema *schema.TableSchema,
	idxs map[string]indexes.IndexConfig,
	snapStore snapstore.SnapStore,
	shardNotifier ShardNotifier,
	cache *pebbleutils.Cache,
) *DBImpl {
	return &DBImpl{
		logger:        logger,
		antflyConfig:  antflyConfig,
		schema:        schema,
		indexes:       idxs,
		snapStore:     snapStore,
		shardNotifier: shardNotifier,
		cache:         cache,
	}
}

// SetProposeResolveIntentsFunc sets the function used to propose resolve intents through Raft.
// This is called by dbWrapper to allow DBImpl to propose Raft operations.
func (db *DBImpl) SetProposeResolveIntentsFunc(fn func(ctx context.Context, op *ResolveIntentsOp) error) {
	db.proposeResolveIntentsFunc = fn
}

// SetProposeAbortTransactionFunc sets the function used to propose abort transaction through Raft.
// This is called by dbWrapper to allow the recovery loop to auto-abort stale Pending transactions.
func (db *DBImpl) SetProposeAbortTransactionFunc(fn func(ctx context.Context, op *AbortTransactionOp) error) {
	db.proposeAbortTransactionFunc = fn
}

// NewDBImplForTest creates a minimal DBImpl for testing purposes.
// The returned DBImpl must be opened with Open() before use.
func NewDBImplForTest(logger *zap.Logger, snapStore snapstore.SnapStore) *DBImpl {
	return &DBImpl{
		logger:    logger,
		snapStore: snapStore,
	}
}

// SetPebbleDB sets the underlying Pebble database. This is exposed for testing
// scenarios where the database needs to be reopened (e.g., after snapshot restore).
func (db *DBImpl) SetPebbleDB(pdb *pebble.DB) {
	db.pdbMu.Lock()
	db.pdb = pdb
	db.pdbMu.Unlock()
}

// getPDB returns the Pebble database pointer, safely guarded against concurrent
// Close() calls that nil out the pointer. Returns nil if the DB is closed.
func (db *DBImpl) getPDB() *pebble.DB {
	db.pdbMu.RLock()
	pdb := db.pdb
	db.pdbMu.RUnlock()
	return pdb
}

func (db *DBImpl) AddIndex(config indexes.IndexConfig) error {
	db.logger.Debug("Adding index", zap.Any("indexConfig", config))
	if err := db.indexManager.Register(config.Name, true, config); err != nil {
		return fmt.Errorf("registering index %s: %w", config.Name, err)
	}
	// Save updated indexes to Pebble
	db.indexesMu.Lock()
	defer db.indexesMu.Unlock()
	db.indexes[config.Name] = config
	if err := db.saveIndexes(); err != nil {
		delete(db.indexes, config.Name)
		return fmt.Errorf("saving indexes to pebble: %w", err)
	}
	return nil
}

func (db *DBImpl) HasIndex(name string) bool {
	return db.indexManager.HasIndex(name)
}

func (db *DBImpl) DeleteIndex(name string) error {
	db.indexesMu.Lock()
	savedConfig, hadConfig := db.indexes[name]
	delete(db.indexes, name)
	if err := db.indexManager.Unregister(name); err != nil {
		if hadConfig {
			db.indexes[name] = savedConfig
		}
		db.indexesMu.Unlock()
		return fmt.Errorf("unregistering index %s: %w", name, err)
	}
	db.indexesMu.Unlock()

	// Save updated indexes to Pebble
	if err := db.saveIndexes(); err != nil {
		return fmt.Errorf("saving indexes to pebble: %w", err)
	}

	return nil
}

func (db *DBImpl) GetIndexes() map[string]indexes.IndexConfig {
	db.indexesMu.RLock()
	defer db.indexesMu.RUnlock()
	return maps.Clone(db.indexes)
}

// updateCachedTTL parses and caches the TTL duration from the current schema.
// Must be called after every db.schema assignment.
func (db *DBImpl) updateCachedTTL() {
	if db.schema == nil || db.schema.TtlDuration == "" {
		db.cachedTTLDuration = 0
		return
	}
	d, err := time.ParseDuration(db.schema.TtlDuration)
	if err != nil {
		db.logger.Warn("Invalid TTL duration in schema, disabling TTL",
			zap.String("ttl_duration", db.schema.TtlDuration),
			zap.Error(err))
		db.cachedTTLDuration = 0
		return
	}
	db.cachedTTLDuration = d
}

// FIXME(ajr) These two methods have to be propagated to the IndexManager
func (db *DBImpl) UpdateSchema(schema *schema.TableSchema) error {
	oldSchema := db.schema
	db.schema = schema
	if err := db.saveSchema(); err != nil {
		db.schema = oldSchema // roll back in-memory state
		return fmt.Errorf("saving schema to pebble: %w", err)
	}
	db.updateCachedTTL()
	if err := db.indexManager.UpdateSchema(schema); err != nil {
		return fmt.Errorf("updating schema in index manager: %w", err)
	}
	return nil
}

func (db *DBImpl) SetRange(byteRange types.Range) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	// Marshal the NEW byteRange parameter, not the old db.byteRange
	data, err := json.Marshal(byteRange)
	if err != nil {
		return fmt.Errorf("marshaling byte range: %w", err)
	}
	if err := pdb.Set(byteRangeKey, data, pebble.Sync); err != nil {
		return fmt.Errorf("saving byte range: %w", err)
	}
	db.byteRange = byteRange
	if err := db.indexManager.UpdateRange(byteRange); err != nil {
		return fmt.Errorf("updating range in index manager: %w", err)
	}
	return nil
}

func (db *DBImpl) UpdateRange(byteRange types.Range) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	batch := pdb.NewBatch()
	defer func() {
		_ = batch.Close()
	}()
	if err := batch.DeleteRange(utils.PrefixSuccessor(MetadataPrefix), byteRange[0], nil); err != nil {
		return fmt.Errorf("deleting range < %s: %w", byteRange, err)
	}
	// TODO (ajr) Consider using db.pdb.Excise
	// Use EndForPebble() to convert empty End (unbounded) to RangeEndSentinel.
	// Otherwise DeleteRange with empty start would delete from beginning of keyspace.
	// Use RangeEndSentinel as sentinel for end-of-keyspace since Pebble's DeleteRange
	// with nil end deletes nothing (nil != unbounded for DeleteRange).
	rangeEnd := byteRange.EndForPebble()
	if err := batch.DeleteRange(rangeEnd, types.RangeEndSentinel, nil); err != nil {
		return fmt.Errorf("deleting range > %s: %w", byteRange, err)
	}
	// Important: marshal the new byteRange parameter, not the old db.byteRange
	data, err := json.Marshal(byteRange)
	if err != nil {
		return fmt.Errorf("marshaling byte range: %w", err)
	}
	if err := pdb.Set(byteRangeKey, data, pebble.Sync); err != nil {
		return fmt.Errorf("saving byte range: %w", err)
	}
	db.byteRange = byteRange
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing range update: %w", err)
	}
	if err := db.Close(); err != nil {
		// NOTE: "leaked iterators" errors can occur here if background workers
		// (TTL cleaner, transaction recovery) have active iterators when Close() is called.
		// These workers run under LeaderFactory context which isn't cancelled by this call.
		// The workers handle pebble.ErrClosed gracefully, so this is non-fatal.
		db.logger.Warn("Error closing db after range update", zap.Error(err))
	}
	// Re-open the DB to ensure everything is in a clean state
	if err := db.Open(db.dir, false, db.schema, db.byteRange); err != nil {
		return fmt.Errorf("re-opening db after range update: %w", err)
	}
	return nil
}

func (db *DBImpl) GetRange() (types.Range, error) {
	return db.byteRange, nil
}

// GetSplitState returns the current split state, or nil if no split is in progress.
func (db *DBImpl) GetSplitState() *SplitState {
	return db.splitState
}

// SetSplitState persists the split state to Pebble and updates the in-memory state.
// This is called after Raft commits a SetSplitStateOp.
func (db *DBImpl) SetSplitState(state *SplitState) error {
	if state == nil {
		return db.ClearSplitState()
	}
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	data, err := proto.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling split state: %w", err)
	}
	if err := pdb.Set(splitStateKey, data, pebble.Sync); err != nil {
		return fmt.Errorf("saving split state: %w", err)
	}
	db.splitState = state
	return nil
}

// ClearSplitState removes the split state from Pebble and clears the in-memory state.
// This is called after a split is finalized or rolled back.
func (db *DBImpl) ClearSplitState() error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	if err := pdb.Delete(splitStateKey, pebble.Sync); err != nil {
		return fmt.Errorf("deleting split state: %w", err)
	}
	db.splitState = nil
	return nil
}

// loadSplitState loads the split state from Pebble into memory.
// This is called during Open() to restore split state across restarts.
func (db *DBImpl) loadSplitState() error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	data, closer, err := pdb.Get(splitStateKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil // No split state stored
		}
		return fmt.Errorf("getting split state: %w", err)
	}
	defer func() { _ = closer.Close() }()

	splitState := &SplitState{}
	if err := proto.Unmarshal(data, splitState); err != nil {
		return fmt.Errorf("unmarshaling split state: %w", err)
	}
	db.splitState = splitState
	return nil
}

func isActiveSplitPhase(state *SplitState) bool {
	return state != nil &&
		(state.GetPhase() == SplitState_PHASE_PREPARE || state.GetPhase() == SplitState_PHASE_SPLITTING)
}

func isKeyInSplitOffRangeForState(key []byte, state *SplitState) bool {
	if !isActiveSplitPhase(state) {
		return false
	}
	splitKey := state.GetSplitKey()
	if len(splitKey) == 0 || bytes.Compare(key, splitKey) < 0 {
		return false
	}
	originalRangeEnd := state.GetOriginalRangeEnd()
	return len(originalRangeEnd) == 0 || bytes.Compare(key, originalRangeEnd) < 0
}

func isKeyOwnedDuringSplit(key []byte, byteRange types.Range, state *SplitState) bool {
	return byteRange.Contains(key) || isKeyInSplitOffRangeForState(key, state)
}

func splitDeltaEntryKey(seq uint64) []byte {
	return slices.Concat(splitDeltaPrefix, encoding.EncodeUint64Ascending(nil, seq))
}

func decodeStoredUint64(data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	_, value, err := encoding.DecodeUint64Ascending(data)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (db *DBImpl) GetSplitDeltaSeq() (uint64, error) {
	return db.splitDeltaSeq, nil
}

func (db *DBImpl) GetSplitDeltaFinalSeq() (uint64, error) {
	return db.splitDeltaFinalSeq, nil
}

func (db *DBImpl) SetSplitDeltaFinalSeq(seq uint64) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	encoded := encoding.EncodeUint64Ascending(nil, seq)
	if err := pdb.Set(splitDeltaFinalSeqKey, encoded, pebble.Sync); err != nil {
		return fmt.Errorf("saving split delta final seq: %w", err)
	}
	db.splitDeltaFinalSeq = seq
	return nil
}

func (db *DBImpl) ClearSplitDeltaFinalSeq() error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	if err := pdb.Delete(splitDeltaFinalSeqKey, pebble.Sync); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("deleting split delta final seq: %w", err)
	}
	db.splitDeltaFinalSeq = 0
	return nil
}

func (db *DBImpl) ListSplitDeltaEntriesAfter(afterSeq uint64) ([]*SplitDeltaEntry, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}
	lowerBound := splitDeltaEntryKey(afterSeq + 1)
	upperBound := utils.PrefixSuccessor(splitDeltaPrefix)
	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating split delta iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	entries := make([]*SplitDeltaEntry, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		entry := &SplitDeltaEntry{}
		if err := proto.Unmarshal(iter.Value(), entry); err != nil {
			return nil, fmt.Errorf("unmarshaling split delta entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating split delta entries: %w", err)
	}
	return entries, nil
}

func (db *DBImpl) ClearSplitDeltaEntries() error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	batch := pdb.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.DeleteRange(splitDeltaPrefix, utils.PrefixSuccessor(splitDeltaPrefix), nil); err != nil {
		return fmt.Errorf("clearing split delta entries: %w", err)
	}
	if err := batch.Delete(splitDeltaSeqKey, nil); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("clearing split delta sequence: %w", err)
	}
	if err := batch.Delete(splitDeltaFinalSeqKey, nil); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("clearing split delta final sequence: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing split delta clear: %w", err)
	}
	db.splitDeltaSeq = 0
	db.splitDeltaFinalSeq = 0
	return nil
}

func (db *DBImpl) appendSplitDelta(batch *pebble.Batch, writes [][2][]byte, deletes [][]byte, timestamp uint64) error {
	if len(writes) == 0 && len(deletes) == 0 {
		return nil
	}
	db.splitDeltaSeq++
	entry := SplitDeltaEntry_builder{
		Sequence:  db.splitDeltaSeq,
		Timestamp: timestamp,
		Writes:    WritesFromTuples(writes),
		Deletes:   deletes,
	}.Build()
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling split delta entry: %w", err)
	}
	if err := batch.Set(splitDeltaEntryKey(db.splitDeltaSeq), data, nil); err != nil {
		return fmt.Errorf("writing split delta entry: %w", err)
	}
	encodedSeq := encoding.EncodeUint64Ascending(nil, db.splitDeltaSeq)
	if err := batch.Set(splitDeltaSeqKey, encodedSeq, nil); err != nil {
		return fmt.Errorf("writing split delta sequence: %w", err)
	}
	return nil
}

// CreateShadowIndexManager creates a shadow IndexManager for the split-off range.
// This is called during PHASE_PREPARE to pre-build indexes for the split-off range
// [splitKey, originalRangeEnd) before the new shard is ready.
//
// The shadow shares the same Pebble DB but uses a separate directory for indexes.
// Dual-writes during PHASE_SPLITTING will write to both the primary and shadow.
func (db *DBImpl) CreateShadowIndexManager(splitKey, originalRangeEnd []byte) error {
	if db.shadowIndexManager != nil {
		return fmt.Errorf("shadow IndexManager already exists")
	}

	// Create shadow directory under the main db directory
	shadowDir := filepath.Join(db.dir, ".shadow", "indexes")

	// Shadow range is [splitKey, originalRangeEnd)
	shadowRange := types.Range{splitKey, originalRangeEnd}

	// Create shadow using the parent's CreateShadow method
	shadow, err := db.indexManager.CreateShadow(shadowDir, shadowRange)
	if err != nil {
		return fmt.Errorf("creating shadow IndexManager: %w", err)
	}

	// Register the same indexes as the parent
	db.indexesMu.RLock()
	indexConfigs := maps.Clone(db.indexes)
	db.indexesMu.RUnlock()

	if err := shadow.RegisterIndexes(indexConfigs, true); err != nil {
		_ = shadow.Close(context.Background())
		return fmt.Errorf("registering indexes in shadow: %w", err)
	}

	// Start the shadow (initializes plexer and WAL)
	if err := shadow.Start(true); err != nil {
		_ = shadow.Close(context.Background())
		return fmt.Errorf("starting shadow IndexManager: %w", err)
	}

	db.shadowIndexManager = shadow
	db.logger.Info("Created shadow IndexManager for split",
		zap.Binary("splitKey", splitKey),
		zap.Binary("originalRangeEnd", originalRangeEnd),
		zap.String("shadowDir", shadowDir))
	return nil
}

// CloseShadowIndexManager closes and cleans up the shadow IndexManager.
// This is called during PHASE_FINALIZING (success) or PHASE_ROLLING_BACK (failure).
func (db *DBImpl) CloseShadowIndexManager() error {
	if db.shadowIndexManager == nil {
		return nil // Already closed or never created
	}

	if err := db.shadowIndexManager.Close(context.Background()); err != nil {
		return fmt.Errorf("closing shadow IndexManager: %w", err)
	}

	db.shadowIndexManager = nil
	db.logger.Info("Closed shadow IndexManager")
	return nil
}

// GetShadowIndexManager returns the shadow IndexManager, or nil if none exists.
func (db *DBImpl) GetShadowIndexManager() *IndexManager {
	return db.shadowIndexManager
}

// GetShadowIndexDir returns the shadow index directory path.
// Returns empty string if no shadow IndexManager exists.
func (db *DBImpl) GetShadowIndexDir() string {
	if db.shadowIndexManager == nil {
		return ""
	}
	return filepath.Join(db.dir, ".shadow", "indexes")
}

func (db *DBImpl) LeaderFactory(
	ctx context.Context,
	persistFunc PersistFunc,
) error {
	// Set leadership flag when becoming leader
	db.isLeader.Store(true)
	defer db.isLeader.Store(false) // Clear leadership flag when losing leadership

	db.isLeaderMu.Lock()
	db.restartIndexManagerFactory = make(
		chan struct{},
		1,
	) // Reset the channel to allow for future restarts
	db.isLeaderMu.Unlock()
	defer func() {
		db.isLeaderMu.Lock()
		close(db.restartIndexManagerFactory)
		for range db.restartIndexManagerFactory {
		}
		db.restartIndexManagerFactory = nil // Prevent further writes to the channel
		db.isLeaderMu.Unlock()
	}()

	db.logger.Info("Starting leader factory",
		zap.Bool("hasPersistFuncSet", persistFunc != nil),
		zap.Bool("s3StorageEnabled", db.s3Storage != nil),
	)

	// Start transaction recovery loop
	go db.transactionRecoveryLoop(ctx)

	// Start TTL cleanup job if TTL is configured
	if db.schema != nil && db.schema.TtlDuration != "" {
		ttlCleaner := NewTTLCleaner(db)
		go func() {
			if err := ttlCleaner.Start(ctx, persistFunc); err != nil &&
				!errors.Is(err, context.Canceled) {
				db.logger.Error("TTL cleanup job failed", zap.Error(err))
			}
		}()
	}

	// Start edge TTL cleanup job for graph indexes with TTL configured
	edgeTTLCleaner := NewEdgeTTLCleaner(db)
	go func() {
		if err := edgeTTLCleaner.Start(ctx, persistFunc); err != nil &&
			!errors.Is(err, context.Canceled) {
			db.logger.Error("Edge TTL cleanup job failed", zap.Error(err))
		}
	}()

	for {
		if err := db.indexManager.StartLeaderFactory(ctx, persistFunc); err != nil &&
			!errors.Is(err, context.Canceled) {
			db.logger.Error("Failed to start index manager leader factory", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			db.logger.Info("Leader factory context cancelled, stepping down as leader")
			return ctx.Err()
		case <-db.restartIndexManagerFactory:
			db.logger.Info("Restarting index manager leader factory")
		}
	}
}

func (db *DBImpl) transactionRecoveryLoop(ctx context.Context) {
	// Immediately notify on becoming leader
	db.notifyPendingResolutions(ctx)

	// Then run periodically
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			db.notifyPendingResolutions(ctx)
		}
	}
}

func (db *DBImpl) notifyPendingResolutions(ctx context.Context) {
	// Check if context is already cancelled before proceeding
	select {
	case <-ctx.Done():
		return
	default:
	}
	pdb := db.getPDB()

	if pdb == nil {
		return
	}

	var err error
	defer func() {
		pebbleutils.RecoverPebbleClosed(&err)
		if err != nil {
			db.logger.Warn("Pebble error during transaction recovery", zap.Error(err))
		}
	}()

	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: txnRecordsPrefix,
		UpperBound: utils.PrefixSuccessor(txnRecordsPrefix),
	})
	if err != nil {
		// Check if error is due to closed database (expected during shutdown)
		if errors.Is(err, pebble.ErrClosed) {
			return
		}
		db.logger.Error("Failed to create transaction records iterator", zap.Error(err))
		return
	}
	defer func() { _ = iter.Close() }()

	cutoff := time.Now().Add(-5 * time.Minute).Unix()
	cleaned := 0
	notified := 0

	for iter.First(); iter.Valid(); iter.Next() {
		var record TxnRecord
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			db.logger.Warn("Failed to unmarshal transaction record",
				zap.String("key", types.FormatKey(iter.Key())),
				zap.Error(err))
			continue
		}

		// Auto-abort stale Pending transactions whose created_at is past the cutoff.
		// This handles the case where commitTransaction or abortTransaction failed (network error,
		// context timeout) and the orchestrator gave up, leaving the txn record stuck in Pending.
		// Without this, Pending intents permanently block future OCC transactions.
		if record.Status == TxnStatusPending && record.CreatedAt < cutoff && record.CreatedAt > 0 {
			if db.proposeAbortTransactionFunc != nil {
				abortOp := AbortTransactionOp_builder{TxnId: record.TxnID}.Build()
				if abortErr := db.proposeAbortTransactionFunc(ctx, abortOp); abortErr != nil {
					db.logger.Warn("Failed to auto-abort stale pending transaction",
						zap.Binary("txnID", record.TxnID),
						zap.Error(abortErr))
				} else {
					db.logger.Info("Auto-aborted stale pending transaction",
						zap.Binary("txnID", record.TxnID),
						zap.Int64("createdAt", record.CreatedAt))
					transactionsCleanedTotal.Inc()
				}
			} else {
				db.logger.Debug("Skipping stale pending txn abort - no abort proposer configured",
					zap.Int64("createdAt", record.CreatedAt))
			}
			continue
		}

		// Only cleanup completed transactions when ALL participants have resolved their intents.
		// This prevents orphaned intents when participants are partitioned.
		//
		// NOTE: The delete and update below write directly to Pebble, bypassing Raft.
		// This is intentional: these records are coordinator-only bookkeeping, and the
		// cleanup is leader-only idempotent GC. If leadership changes, the new leader
		// simply re-processes any undeleted records and re-notifies (all idempotent).
		if record.Status != TxnStatusPending && record.FinalizedAt < cutoff {
			// Check if all participants have resolved their intents
			allResolved := len(record.ResolvedParticipants) >= len(record.Participants)
			if allResolved {
				if err := pdb.Delete(iter.Key(), pebble.Sync); err != nil {
					db.logger.Warn("Failed to cleanup old transaction record",
						zap.String("key", types.FormatKey(iter.Key())),
						zap.Error(err))
				} else {
					cleaned++
					transactionsCleanedTotal.Inc()
				}
				continue
			} else {
				db.logger.Debug("Skipping cleanup - not all participants resolved",
					zap.String("key", types.FormatKey(iter.Key())),
					zap.Int("participants", len(record.Participants)),
					zap.Int("resolved", len(record.ResolvedParticipants)))
				// Fall through to notification logic to retry resolving remaining participants
			}
		}

		// Notify participants for committed/aborted transactions (both recent and old unresolved ones)
		if record.Status != TxnStatusPending {
			// Build set of already-resolved participants for quick lookup
			resolvedSet := make(map[string]bool, len(record.ResolvedParticipants))
			for _, rp := range record.ResolvedParticipants {
				resolvedSet[rp] = true
			}

			// Only notify if we have a notifier configured
			if db.shardNotifier != nil {
				// Find participants that haven't been resolved yet
				var unresolvedParticipants [][]byte
				for _, p := range record.Participants {
					if !resolvedSet[base64.StdEncoding.EncodeToString(p)] {
						unresolvedParticipants = append(unresolvedParticipants, p)
					}
				}

				if len(unresolvedParticipants) > 0 {
					db.logger.Debug("Notifying unresolved participants of transaction resolution",
						zap.Binary("txnID", record.TxnID),
						zap.Int("unresolvedParticipants", len(unresolvedParticipants)),
						zap.Int("totalParticipants", len(record.Participants)),
						zap.Int32("status", record.Status))

					// Notify each unresolved participant and track successful resolutions
					var newlyResolved []string
					for _, shardID := range unresolvedParticipants {
						notifyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						err = db.shardNotifier.NotifyResolveIntent(notifyCtx, shardID, record.TxnID, record.Status, record.Version())
						cancel()

						participant := base64.StdEncoding.EncodeToString(shardID)
						if err != nil {
							db.logger.Warn("Failed to notify participant",
								zap.Binary("txnID", record.TxnID),
								zap.String("participant", participant),
								zap.Error(err))
							// Will retry on next recovery loop iteration
						} else {
							newlyResolved = append(newlyResolved, participant)
							db.logger.Debug("Participant successfully resolved intents",
								zap.Binary("txnID", record.TxnID),
								zap.String("participant", participant))
						}
					}

					// Update the transaction record with newly resolved participants
					if len(newlyResolved) > 0 {
						record.ResolvedParticipants = append(record.ResolvedParticipants, newlyResolved...)

						updatedData, err := json.Marshal(record)
						if err != nil {
							db.logger.Warn("Failed to marshal updated transaction record",
								zap.Binary("txnID", record.TxnID),
								zap.Error(err))
						} else {
							if err := pdb.Set(iter.Key(), updatedData, pebble.Sync); err != nil {
								db.logger.Warn("Failed to save updated transaction record",
									zap.Binary("txnID", record.TxnID),
									zap.Error(err))
							}
						}
					}

					notified++
					transactionsRecoveredTotal.Inc()
				}
			}

			// Also resolve the coordinator's own intents (the coordinator is not in the participants list)
			if db.proposeResolveIntentsFunc != nil {
				resolveOp := ResolveIntentsOp_builder{
					TxnId:         record.TxnID,
					Status:        record.Status,
					CommitVersion: record.Version(),
				}.Build()

				if err := db.proposeResolveIntentsFunc(ctx, resolveOp); err != nil {
					db.logger.Warn("Failed to resolve coordinator's own intents",
						zap.Binary("txnID", record.TxnID),
						zap.Error(err))
				} else {
					db.logger.Debug("Resolved coordinator's own intents",
						zap.Binary("txnID", record.TxnID))
				}
			}
		}
	}

	if cleaned > 0 || notified > 0 {
		db.logger.Debug("Transaction recovery completed",
			zap.Int("cleaned", cleaned),
			zap.Int("notified", notified))
	}
}

func (db *DBImpl) openInternal(
	dir string,
	recovery bool,
	schema *schema.TableSchema,
	byteRange types.Range,
	openIndexes bool,
) error {
	pebbleDir := filepath.Join(dir, "pebble")
	// Ensure the destination directory exists
	if err := os.MkdirAll(pebbleDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return fmt.Errorf("failed to create pebble directory: %w", err)
	}
	db.dir = dir

	pebbleOpts, err := db.getPebbleOpts()
	if err != nil {
		return err
	}
	if !openIndexes {
		// Split staging DBs are short-lived. Disable async table stats so Pebble
		// does not race a background stats job against DB shutdown.
		pebbleOpts.DisableTableStats = true
	}

	pdb, err := pebble.Open(pebbleDir, pebbleOpts)
	if err != nil {
		return fmt.Errorf("opening pebble: %w", err)
	}
	// Take write lock when setting pdb to prevent concurrent access
	// during close/reopen cycles (e.g., during splits)
	db.pdbMu.Lock()
	db.pdb = pdb
	db.pdbMu.Unlock()

	// Initialize buffer pool for compression
	db.bufferPool = &sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
	db.encoderPool = &sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil)
			return enc
		},
	}

	// Initialize indexes map if not already set
	if db.indexes == nil {
		db.indexes = make(map[string]indexes.IndexConfig)
	}

	if schema == nil && len(byteRange[0]) == 0 && len(byteRange[1]) == 0 {
		// Try to load metadata if this is a restart
		if err := db.loadMetadata(); err != nil {
			db.logger.Debug(
				"Could not load metadata from pebble, using provided values",
				zap.Error(err),
			)
		}
	}

	// Use provided schema if available, otherwise keep what was loaded
	if schema != nil {
		db.schema = schema
		db.updateCachedTTL()
	}
	// Use provided byteRange if available, otherwise keep what was loaded
	if len(byteRange[0]) != 0 || len(byteRange[1]) != 0 {
		db.byteRange = byteRange
	}

	// Always load splitState from Pebble - it's Raft-replicated state that
	// persists across restarts regardless of whether other metadata is provided.
	if err := db.loadSplitState(); err != nil {
		db.logger.Warn("Could not load split state from pebble", zap.Error(err))
	}

	// Save metadata to ensure it's persisted
	if err := db.saveMetadata(); err != nil {
		return fmt.Errorf("saving metadata to pebble: %w", err)
	}
	if openIndexes {
		if err := db.openIndex(dir, recovery); err != nil {
			return fmt.Errorf("opening index: %w", err)
		}
	}
	db.logger.Debug(
		"Completed opening local keyvalue store",
		zap.Any("indexes", db.indexes),
		zap.Any("schema", db.schema),
		zap.Stringer("byteRange", db.byteRange),
	)
	return nil
}

func (db *DBImpl) Open(
	dir string,
	recovery bool,
	schema *schema.TableSchema,
	byteRange types.Range,
) error {
	return db.openInternal(dir, recovery, schema, byteRange, true)
}

func (s *DBImpl) Close() (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	if s.indexManager != nil {
		if err := s.indexManager.Close(context.Background()); err != nil {
			return fmt.Errorf("closing indexes: %w", err)
		}
	}
	// Take write lock and set pdb to nil before closing to prevent concurrent access
	// during close/reopen cycles (e.g., during splits)
	s.pdbMu.Lock()
	pdb := s.pdb
	s.pdb = nil
	s.pdbMu.Unlock()

	if pdb != nil {
		if err := pdb.Close(); err != nil {
			return fmt.Errorf("closing Pebble database: %w", err)
		}
	}
	// Close S3 storage if configured
	if s.s3Storage != nil {
		if err := s.s3Storage.Close(); err != nil {
			return fmt.Errorf("closing S3 storage: %w", err)
		}
	}
	return nil
}

// getPebbleOpts creates and configures Pebble options, including S3 storage if enabled.
// If a shared cache is set, it is used; otherwise a dedicated cache is created.
// The fallback cache size is controlled by DefaultPebbleCacheSizeMB (tests can override this).
func (db *DBImpl) getPebbleOpts() (*pebble.Options, error) {
	cacheSize := DefaultPebbleCacheSizeMB << 20 // Convert MB to bytes

	pebbleOpts := &pebble.Options{
		Logger: &logger.NoopLoggerAndTracer{},
	}
	db.cache.Apply(pebbleOpts, cacheSize)
	if db.antflyConfig.GetKeyValueStorageType() == "s3" {
		if err := db.configureS3Storage(pebbleOpts); err != nil {
			return nil, fmt.Errorf("configuring S3 storage: %w", err)
		}
	} else {
		db.logger.Debug("S3 storage disabled, using local storage only")
	}
	return pebbleOpts, nil
}

// configureS3Storage configures Pebble to use S3 for remote sstable storage.
// This sets up the LeaderAwareS3Storage wrapper which only allows the Raft leader to write to S3.
// Note: This method should only be called when S3 storage is enabled.
func (db *DBImpl) configureS3Storage(pebbleOpts *pebble.Options) error {
	s3Info := db.antflyConfig.Storage.S3
	db.logger.Info("Configuring S3 storage for Pebble",
		zap.String("endpoint", s3Info.Endpoint),
		zap.String("bucket", s3Info.Bucket),
		zap.String("prefix", s3Info.Prefix),
		zap.Bool("useSSL", s3Info.UseSsl),
	)

	// Create Minio client for S3 operations
	minioClient, err := s3Info.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}

	// Create base S3 storage
	baseS3Storage, err := s3storage.NewS3Storage(
		minioClient,
		s3Info.Bucket,
		s3Info.Prefix,
	)
	if err != nil {
		return fmt.Errorf("creating S3 storage backend: %w", err)
	}

	// Wrap with leadership-aware storage
	// This ensures only the Raft leader writes to S3, preventing 3x storage cost
	db.s3Storage = s3storage.NewLeaderAwareS3Storage(baseS3Storage, &db.isLeader)

	// Configure Pebble experimental options for remote storage
	pebbleOpts.Experimental.RemoteStorage = remote.MakeSimpleFactory(
		map[remote.Locator]remote.Storage{
			"s3": db.s3Storage,
		},
	)

	// Configure Pebble to create new sstables on shared storage (S3)
	// Only the leader will actually write (due to LeaderAwareS3Storage wrapper)
	pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedAll
	pebbleOpts.Experimental.CreateOnSharedLocator = "s3"

	db.logger.Info("S3 storage configured successfully",
		zap.String("locator", "s3"),
	)

	return nil
}

// Split splits the database at splitKey into two ranges.
// If prepareOnly is true, the split-off data is streamed to destDir2 for archiving,
// but the parent database's data and byte range are NOT modified. This allows the
// parent shard to continue serving the split-off range during the transition period
// until the new shard is ready. Call FinalizeSplit() to complete the split.
// If prepareOnly is false, the split-off data is deleted and the byte range is updated
// immediately (legacy behavior).
func (db *DBImpl) Split(
	currRange types.Range,
	splitKey []byte,
	destDir1, destDir2 string,
	prepareOnly bool,
) error {
	splitStart := time.Now()
	defer func() {
		phase := "full"
		if prepareOnly {
			phase = "prepare_only"
		}
		splitPhaseDurationSeconds.WithLabelValues(phase).Observe(time.Since(splitStart).Seconds())
	}()
	if !currRange.Contains(splitKey) {
		return fmt.Errorf("split key %s not in range %s", splitKey, currRange)
	}

	range1 := types.Range{currRange[0], splitKey}
	range2 := types.Range{splitKey, currRange[1]}
	startTime := time.Now()

	db.logger.Info("Creating shard from range",
		zap.String("destDir", destDir2),
		zap.Stringer("range", range2))

	// When prepareOnly=true, we only need to stream data for the archive - the parent
	// shard continues using its existing indexes. Build the split-off staging DB
	// with a restricted Pebble checkpoint plus rewritten metadata, avoiding the
	// previous row-by-row copy and discarded index work.
	if prepareOnly {
		childPrepareStart := time.Now()
		newDB, err := db.prepareSplitStagingDB(destDir2, range2)
		if err != nil {
			return fmt.Errorf("preparing split staging db: %w", err)
		}
		splitPhaseDurationSeconds.WithLabelValues("prepare_child_checkpoint").Observe(time.Since(childPrepareStart).Seconds())
		db.logger.Info("Prepared split staging db from checkpoint",
			zap.String("destDir", destDir2),
			zap.Stringer("range", range2),
			zap.Duration("prepareDuration", time.Since(startTime)))
		if err := newDB.Close(); err != nil {
			return fmt.Errorf("closing split staging db: %w", err)
		}

		db.logger.Info("Prepared split (data preserved for transition)",
			zap.String("destDir1", destDir1),
			zap.String("destDir2", destDir2),
			zap.Stringer("range1", range1),
			zap.Stringer("range2", range2))
		return nil
	}

	childPrepareStart := time.Now()
	newDB, err := db.prepareSplitStagingDB(destDir2, range2)
	if err != nil {
		return fmt.Errorf("preparing split staging db: %w", err)
	}
	splitPhaseDurationSeconds.WithLabelValues("full_child_checkpoint").Observe(time.Since(childPrepareStart).Seconds())
	db.logger.Info("Prepared split staging db from checkpoint",
		zap.String("destDir", destDir2),
		zap.Stringer("range", range2),
		zap.Duration("prepareDuration", time.Since(childPrepareStart)))
	defer func() {
		_ = newDB.Close()
	}()

	// Finalize the split by pruning split-off keys from the parent indexes in place,
	// then deleting the split-off range from Pebble and updating the byte range.
	if err := db.finalizeSplitInternal(range1, ""); err != nil {
		return fmt.Errorf("finalizing split: %w", err)
	}

	db.logger.Info("Completed online split",
		zap.String("destDir1", destDir1),
		zap.String("destDir2", destDir2))

	return nil
}

func (db *DBImpl) prepareSplitStagingDB(destDir string, byteRange types.Range) (*DBImpl, error) {
	prepareStart := time.Now()
	defer func() {
		splitPhaseDurationSeconds.WithLabelValues("checkpoint_prepare_helper").Observe(time.Since(prepareStart).Seconds())
	}()
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}

	if err := os.MkdirAll(destDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, fmt.Errorf("creating split staging directory: %w", err)
	}

	destPebbleDir := filepath.Join(destDir, "pebble")
	_ = os.RemoveAll(destPebbleDir)

	span := pebble.CheckpointSpan{
		Start: byteRange[0],
		End:   byteRange.EndForPebble(),
	}
	checkpointStart := time.Now()
	if err := pdb.Checkpoint(destPebbleDir,
		pebble.WithFlushedWAL(),
		pebble.WithRestrictToSpans([]pebble.CheckpointSpan{span}),
	); err != nil {
		return nil, fmt.Errorf("creating restricted checkpoint: %w", err)
	}
	splitPhaseDurationSeconds.WithLabelValues("checkpoint_create").Observe(time.Since(checkpointStart).Seconds())

	db.indexesMu.RLock()
	indexConfigs := maps.Clone(db.indexes)
	db.indexesMu.RUnlock()

	newDB := &DBImpl{
		logger:       db.logger.Named("split-shard"),
		antflyConfig: db.antflyConfig,
		indexes:      indexConfigs,
		schema:       db.schema,
	}
	openStart := time.Now()
	if err := newDB.openInternal(destDir, false, db.schema, byteRange, false); err != nil {
		return nil, fmt.Errorf("opening checkpointed split staging db: %w", err)
	}
	splitPhaseDurationSeconds.WithLabelValues("checkpoint_open").Observe(time.Since(openStart).Seconds())

	// Restricted checkpoints can still carry keys from overlapping SSTables.
	// Trim the staged child DB down to the exact split-off range before archiving.
	trimStart := time.Now()
	if err := newDB.trimToRangeWithoutIndexes(byteRange); err != nil {
		_ = newDB.Close()
		return nil, fmt.Errorf("trimming checkpointed split staging db to range: %w", err)
	}
	splitPhaseDurationSeconds.WithLabelValues("checkpoint_trim").Observe(time.Since(trimStart).Seconds())

	// Split staging archives should contain only child data plus core metadata.
	// Clear any split bookkeeping inherited from the checkpointed parent state.
	cleanupStart := time.Now()
	if newDB.splitState != nil {
		if err := newDB.ClearSplitState(); err != nil && !errors.Is(err, pebble.ErrNotFound) {
			_ = newDB.Close()
			return nil, fmt.Errorf("clearing split state from split staging db: %w", err)
		}
	}
	if err := newDB.ClearSplitDeltaEntries(); err != nil {
		_ = newDB.Close()
		return nil, fmt.Errorf("clearing split delta entries from split staging db: %w", err)
	}
	splitPhaseDurationSeconds.WithLabelValues("checkpoint_cleanup").Observe(time.Since(cleanupStart).Seconds())

	return newDB, nil
}

func (db *DBImpl) trimToRangeWithoutIndexes(byteRange types.Range) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}

	batch := pdb.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.DeleteRange(utils.PrefixSuccessor(MetadataPrefix), byteRange[0], nil); err != nil {
		return fmt.Errorf("deleting range below split staging lower bound: %w", err)
	}
	if err := batch.DeleteRange(byteRange.EndForPebble(), types.RangeEndSentinel, nil); err != nil {
		return fmt.Errorf("deleting range above split staging upper bound: %w", err)
	}

	data, err := json.Marshal(byteRange)
	if err != nil {
		return fmt.Errorf("marshaling split staging byte range: %w", err)
	}
	if err := batch.Set(byteRangeKey, data, nil); err != nil {
		return fmt.Errorf("saving split staging byte range: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing split staging range trim: %w", err)
	}

	db.byteRange = byteRange
	return nil
}

func splitOffRangeForNewRange(currentRange, newRange types.Range, splitState *SplitState) (types.Range, bool, error) {
	splitOffEnd := currentRange[1]
	if splitState != nil && len(splitState.GetOriginalRangeEnd()) > 0 {
		splitOffEnd = splitState.GetOriginalRangeEnd()
	}

	if bytes.Equal(newRange[1], splitOffEnd) {
		return types.Range{}, false, nil
	}
	if len(splitOffEnd) > 0 && bytes.Compare(newRange[1], splitOffEnd) > 0 {
		return types.Range{}, false, fmt.Errorf(
			"new range end %s exceeds original range end %s",
			types.FormatKey(newRange[1]),
			types.FormatKey(splitOffEnd),
		)
	}

	return types.Range{newRange[1], splitOffEnd}, true, nil
}

func (db *DBImpl) denseEmbeddingsIndexesForSplitRebuild() []string {
	db.indexesMu.RLock()
	defer db.indexesMu.RUnlock()

	names := make([]string, 0, len(db.indexes))
	for name, conf := range db.indexes {
		if !indexes.IsEmbeddingsType(conf.Type) {
			continue
		}
		embCfg, err := conf.AsEmbeddingsIndexConfig()
		if err != nil {
			db.logger.Warn("Failed to decode embeddings index config for split rebuild planning",
				zap.String("index", name),
				zap.Error(err))
			continue
		}
		if embCfg.Sparse {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (db *DBImpl) deleteDenseEmbeddingsSplitRebuildState(batch *pebble.Batch, indexNames []string) error {
	for _, name := range indexNames {
		if err := vectorindex.DeleteIndex(batch, name); err != nil {
			return fmt.Errorf("deleting vector index %s: %w", name, err)
		}
	}
	return nil
}

func (db *DBImpl) removeSplitRebuildIndexDirs(indexNames []string) error {
	for _, name := range indexNames {
		if err := os.RemoveAll(filepath.Join(db.dir, "indexes", name)); err != nil {
			return fmt.Errorf("removing index directory for %s: %w", name, err)
		}
	}
	return nil
}

func indexDeleteKeyForStoredKey(key []byte) ([]byte, bool) {
	if len(key) == 0 || bytes.HasPrefix(key, MetadataPrefix) {
		return nil, false
	}
	if storeutils.IsChunkKey(key) {
		return bytes.Clone(key), true
	}
	if storeutils.IsEdgeKey(key) {
		return bytes.Clone(key), true
	}
	if bytes.HasSuffix(key, storeutils.DBRangeStart) {
		return bytes.Clone(key[:len(key)-len(storeutils.DBRangeStart)]), true
	}
	return nil, false
}

func (db *DBImpl) pruneIndexesForRange(ctx context.Context, byteRange types.Range) error {
	if db.indexManager == nil {
		return nil
	}

	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}

	upperBound := byteRange[1]
	if len(upperBound) == 0 {
		upperBound = nil
	}

	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: byteRange[0],
		UpperBound: upperBound,
	})
	if err != nil {
		return fmt.Errorf("creating iterator for index prune: %w", err)
	}
	defer func() { _ = iter.Close() }()

	groupedDeletes := deleteKeyGroups{
		all:       make([][]byte, 0, indexManagerPartitionSize),
		documents: make([][]byte, 0, indexManagerPartitionSize),
	}
	pruned := 0
	prunedDocuments := 0
	prunedChunks := 0
	prunedEdges := 0
	flush := func() error {
		if len(groupedDeletes.all) == 0 {
			return nil
		}
		if err := db.indexManager.deleteGroupedKeys(ctx, groupedDeletes); err != nil {
			return err
		}
		pruned += len(groupedDeletes.all)
		groupedDeletes.all = groupedDeletes.all[:0]
		groupedDeletes.documents = groupedDeletes.documents[:0]
		clear(groupedDeletes.chunksByName)
		clear(groupedDeletes.edgesByName)
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		deleteKey, ok := indexDeleteKeyForStoredKey(iter.Key())
		if !ok {
			continue
		}
		switch appendDeleteKeyGroup(&groupedDeletes, deleteKey) {
		case "document":
			prunedDocuments++
		case "chunk":
			prunedChunks++
		case "edge":
			prunedEdges++
		}
		if len(groupedDeletes.all) == indexManagerPartitionSize {
			if err := flush(); err != nil {
				return fmt.Errorf("pruning index batch for range %s: %w", byteRange, err)
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating split-off range for index prune: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("pruning index batch for range %s: %w", byteRange, err)
	}

	if prunedDocuments > 0 {
		splitPruneCandidateKeysTotal.WithLabelValues("document").Add(float64(prunedDocuments))
	}
	if prunedChunks > 0 {
		splitPruneCandidateKeysTotal.WithLabelValues("chunk").Add(float64(prunedChunks))
	}
	if prunedEdges > 0 {
		splitPruneCandidateKeysTotal.WithLabelValues("edge").Add(float64(prunedEdges))
	}

	db.logger.Info("Pruned split-off keys from parent indexes",
		zap.Stringer("range", byteRange),
		zap.Int("keys", pruned))
	return nil
}

// FinalizeSplit completes a split that was prepared with prepareOnly=true.
// It deletes the split-off data from the parent database and updates the byte range.
// This should be called when the new shard is ready to serve traffic.
func (db *DBImpl) FinalizeSplit(newRange types.Range) error {
	db.logger.Info("Finalizing split", zap.Stringer("newRange", newRange))

	// Finalize by pruning split-off keys from the existing parent indexes before
	// deleting the underlying Pebble data. This avoids a full remaining-range rebuild.
	if err := db.finalizeSplitInternal(newRange, ""); err != nil {
		return fmt.Errorf("finalizing split: %w", err)
	}

	db.logger.Info("Finalized split", zap.Stringer("newRange", newRange))
	return nil
}

// finalizeSplitInternal handles the deletion of split-off data and byte range update.
// If destDir1 is non-empty, the new indexes are moved into place.
func (db *DBImpl) finalizeSplitInternal(newRange types.Range, destDir1 string) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}

	splitOffRange, hasSplitOffRange, err := splitOffRangeForNewRange(db.byteRange, newRange, db.splitState)
	if err != nil {
		return fmt.Errorf("computing split-off range: %w", err)
	}
	rebuildDenseEmbeddings := hasSplitOffRange && destDir1 == ""
	denseEmbeddingsToRebuild := []string(nil)
	if rebuildDenseEmbeddings {
		denseEmbeddingsToRebuild = db.denseEmbeddingsIndexesForSplitRebuild()
		if len(denseEmbeddingsToRebuild) > 0 {
			db.logger.Info("Finalize split will rebuild dense embeddings indexes for retained parent range",
				zap.Strings("indexes", denseEmbeddingsToRebuild),
				zap.Stringer("range", newRange))
		}
	}
	if hasSplitOffRange {
		pruneStart := time.Now()
		if err := db.pruneIndexesForRange(context.Background(), splitOffRange); err != nil {
			return fmt.Errorf("pruning parent indexes for split-off range %s: %w", splitOffRange, err)
		}
		splitPhaseDurationSeconds.WithLabelValues("finalize_index_prune").Observe(time.Since(pruneStart).Seconds())
	}

	batch := pdb.NewBatch()
	defer func() {
		_ = batch.Close()
	}()
	if err := batch.DeleteRange(utils.PrefixSuccessor(MetadataPrefix), newRange[0], nil); err != nil {
		return fmt.Errorf("deleting range < %s: %w", newRange, err)
	}
	// TODO (ajr) Consider using db.pdb.Excise
	// Use RangeEndSentinel as sentinel for end-of-keyspace since Pebble's DeleteRange
	// with nil end deletes nothing (nil != unbounded for DeleteRange).
	if err := batch.DeleteRange(newRange[1], types.RangeEndSentinel, nil); err != nil {
		return fmt.Errorf("deleting range > %s: %w", newRange, err)
	}
	// Important: marshal newRange (the new range), not the old db.byteRange
	data, err := json.Marshal(newRange)
	if err != nil {
		return fmt.Errorf("marshaling byte range: %w", err)
	}
	if err := pdb.Set(byteRangeKey, data, pebble.Sync); err != nil {
		return fmt.Errorf("saving byte range: %w", err)
	}
	if len(denseEmbeddingsToRebuild) > 0 {
		if err := db.deleteDenseEmbeddingsSplitRebuildState(batch, denseEmbeddingsToRebuild); err != nil {
			return fmt.Errorf("invalidating dense embeddings indexes for split rebuild: %w", err)
		}
	}
	db.byteRange = newRange
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing range update: %w", err)
	}
	if err := db.Close(); err != nil {
		// NOTE: "leaked iterators" errors can occur here if background workers
		// (TTL cleaner, transaction recovery) have active iterators when Close() is called.
		// These workers run under LeaderFactory context which isn't cancelled by this call.
		// The workers handle pebble.ErrClosed gracefully, so this is non-fatal.
		db.logger.Warn("Error closing db after range update", zap.Error(err))
	}
	if len(denseEmbeddingsToRebuild) > 0 {
		if err := db.removeSplitRebuildIndexDirs(denseEmbeddingsToRebuild); err != nil {
			return fmt.Errorf("removing dense embeddings index directories for split rebuild: %w", err)
		}
	}

	// Move prebuilt indexes into place if a caller provided them. The current split
	// path prunes the existing parent indexes in place, so this is legacy-only.
	if destDir1 != "" {
		oldIndexesPath := filepath.Join(db.dir, "indexes")
		newIndexesPath := filepath.Join(destDir1, "indexes")
		tempIndexesPath := filepath.Join(db.dir, "indexes.old")

		// Move old indexes out of the way
		if err := os.Rename(oldIndexesPath, tempIndexesPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("renaming old indexes directory: %w", err)
		}

		// Move new indexes into place
		if err := os.Rename(newIndexesPath, oldIndexesPath); err != nil {
			return fmt.Errorf("moving new indexes to db directory: %w", err)
		}

		// Clean up old indexes
		_ = os.RemoveAll(tempIndexesPath)
	}

	// Re-open the DB to ensure everything is in a clean state
	if err := db.Open(db.dir, true, db.schema, db.byteRange); err != nil {
		return fmt.Errorf("re-opening db after range update: %w", err)
	}
	if len(denseEmbeddingsToRebuild) > 0 {
		db.indexManager.WaitForNamedBackfills(context.Background(), denseEmbeddingsToRebuild)
	}

	return nil
}

// streamRangeToPebble streams all key-value pairs in the given range from the source
// Pebble DB to the destination Pebble DB. Returns the count of records streamed.
func (db *DBImpl) streamRangeToDB(
	ctx context.Context,
	destDB *DBImpl,
	byteRange [2][]byte,
) (count int, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	// Guard against nil pdb: the shard may be shutting down concurrently
	// (Close sets pdb=nil) while the Raft commit loop is still applying entries.
	pdb := db.getPDB()
	if pdb == nil {
		return 0, fmt.Errorf("source database closed during split streaming")
	}

	// Convert empty End to nil for Pebble - empty byte slice sorts BEFORE all keys,
	// but nil means unbounded (iterate to the end). This is critical for unbounded ranges.
	upperBound := byteRange[1]
	if len(upperBound) == 0 {
		upperBound = nil
	}
	iterOpts := &pebble.IterOptions{
		LowerBound: byteRange[0],
		UpperBound: upperBound,
	}

	// Flush memtable to ensure all data is visible to the iterator
	if err := pdb.Flush(); err != nil {
		return 0, fmt.Errorf("flushing memtable before streaming: %w", err)
	}

	iter, err := pdb.NewIterWithContext(ctx, iterOpts)
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	const batchSize = 1000
	destPDB := destDB.getPDB()
	if destPDB == nil {
		return 0, fmt.Errorf("destination database closed during split streaming")
	}
	batch := destPDB.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	count = 0
	for iter.First(); iter.Valid(); iter.Next() {
		// Clone key and value since Pebble reuses buffers
		key := slices.Clone(iter.Key())
		value := slices.Clone(iter.Value())

		// Write directly to Pebble, bypassing the Batch() method's range checks
		if err := batch.Set(key, value, nil); err != nil {
			return count, fmt.Errorf("setting key in batch: %w", err)
		}
		count++

		if count%batchSize == 0 {
			if err := batch.Commit(pebble.Sync); err != nil {
				return count, fmt.Errorf("committing batch: %w", err)
			}
			batch.Reset()

			// Log progress periodically
			if count%10000 == 0 {
				db.logger.Debug("Streaming progress",
					zap.Int("recordsStreamed", count))
			}
		}
	}

	if err := iter.Error(); err != nil {
		return count, fmt.Errorf("iterator error: %w", err)
	}

	// Commit remaining records
	if count%batchSize != 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return count, fmt.Errorf("committing final batch: %w", err)
		}
	}

	return count, nil
}

// SetIndexManager replaces the current IndexManager, closing the old one first if it exists
func (db *DBImpl) SetIndexManager(im *IndexManager) error {
	if db.indexManager != nil {
		if err := db.indexManager.Close(context.Background()); err != nil {
			db.logger.Warn("failed to close previous index manager", zap.Error(err))
			return err
		}
	}
	db.indexManager = im
	return nil
}

func (db *DBImpl) openIndex(dir string, recoverIndex bool) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	var err error
	im, err := NewIndexManager(
		db.logger,
		db.antflyConfig,
		pdb,
		filepath.Join(dir, "indexes"),
		db.schema,
		db.byteRange,
		db.cache,
	)
	if err != nil {
		return fmt.Errorf("opening index manager: %w", err)
	}
	// Close old IndexManager and set the new one
	if err := db.SetIndexManager(im); err != nil {
		return fmt.Errorf("setting index manager: %w", err)
	}
	for idx, conf := range db.indexes {
		db.logger.Debug("Opening index", zap.String("index", idx), zap.Any("index_config", conf))
		if err := db.indexManager.Register(idx, !recoverIndex, conf); err != nil {
			return fmt.Errorf("registering preconfigured index: %w", err)
		}
	}

	if err := db.indexManager.Start(!recoverIndex); err != nil {
		return fmt.Errorf("opening indexes: %w", err)
	}
	db.indexesMu.Lock()
	defer db.indexesMu.Unlock()
	// Hold isLeaderMu to prevent race with LeaderFactory closing the channel
	db.isLeaderMu.Lock()
	if db.restartIndexManagerFactory != nil {
		db.restartIndexManagerFactory <- struct{}{} // Signal to restart the index manager factory
	}
	db.isLeaderMu.Unlock()
	return nil
}

var ErrNotFound = errors.New("key not found")

// ErrTxnNotFound is returned when a transaction record does not exist in storage.
var ErrTxnNotFound = errors.New("transaction not found")

func (db *DBImpl) Get(ctx context.Context, key []byte) (docMap map[string]any, err error) {
	// Use default query options: include both summaries and embeddings
	opts := storeutils.QueryOptions{
		AllSummaries:  true,
		AllEmbeddings: true,
	}
	return db.getWithQueryOptions(ctx, key, opts)
}

func (db *DBImpl) getWithQueryOptions(
	ctx context.Context,
	key []byte,
	opts storeutils.QueryOptions,
) (docMap map[string]any, err error) {
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}

	doc, err := storeutils.GetDocument(ctx, pdb, key, opts)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	// Check if document is expired (TTL filtering)
	expired, err := db.isDocumentExpiredByTimestamp(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("checking TTL expiration: %w", err)
	}
	if expired {
		return nil, ErrNotFound
	}

	docMap = doc.Document

	// Add embeddings if any were retrieved
	if len(doc.Embeddings) > 0 {
		docMap["_embeddings"] = doc.Embeddings
	}

	// Add summaries if any were retrieved
	if len(doc.Summaries) > 0 {
		docMap["_summaries"] = doc.Summaries
	}

	// Add chunks if any were retrieved
	if len(doc.Chunks) > 0 {
		docMap["_chunks"] = doc.Chunks
	}

	return docMap, nil
}

func (db *DBImpl) FindMedianKey() ([]byte, error) {
	// Skip metadata keys when finding split point - they shouldn't be used as split boundaries
	skipMetadataKey := func(key []byte) bool {
		return bytes.HasPrefix(key, storeutils.MetadataPrefix)
	}
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}
	key, err := common.FindSplitKeyByFileSize(db.logger, pdb, db.byteRange, skipMetadataKey)
	if err != nil {
		return nil, err
	}
	if !bytes.HasSuffix(key, DBRangeStart) {
		// Key doesn't have DBRangeStart suffix - need to convert to a valid document key boundary
		i := bytes.LastIndex(key, []byte{':'})
		if i == -1 {
			// Key has no colon at all (raw document ID from SSTable boundary)
			// Append DBRangeStart to make it a valid document key boundary
			key = slices.Concat(key, DBRangeStart)
		} else {
			// Key has a colon but wrong suffix - replace suffix with DBRangeStart
			key = slices.Concat(key[:i], DBRangeStart)
		}
	}
	// Revalidate after suffix normalization: the transformation above can push
	// the key onto the range boundary. For example, a raw key "doc-029:t" normalized
	// to "doc-029:\x00" may equal the range end, producing an invalid split key.
	if bytes.Compare(key, db.byteRange[0]) <= 0 {
		return nil, fmt.Errorf("normalized split key %x is not greater than range start %x", key, db.byteRange[0])
	}
	if len(db.byteRange[1]) > 0 && bytes.Compare(key, db.byteRange[1]) >= 0 {
		return nil, fmt.Errorf("normalized split key %x equals or exceeds range end %x", key, db.byteRange[1])
	}
	return key, nil
}

// ExtractEnrichments extracts user-provided enrichments from writes
// Returns embedding, summary, and edge writes extracted from _embeddings, _summaries, _edges fields
// This is the public API for dbWrapper to call before Raft proposal
func (db *DBImpl) ExtractEnrichments(
	ctx context.Context,
	writes [][2][]byte,
) (embeddingWrites, summaryWrites, edgeWrites [][2][]byte, edgeDeletes [][]byte, err error) {
	embeddingWrites = make([][2][]byte, 0)
	summaryWrites = make([][2][]byte, 0)
	edgeWrites = make([][2][]byte, 0)
	edgeDeletes = make([][]byte, 0)

	for _, write := range writes {
		key := write[0]
		docJSON := write[1]

		// Skip non-JSON writes
		if len(docJSON) == 0 || docJSON[0] != '{' {
			continue
		}

		// Fast-path check for special fields
		hasSpecialFields := bytes.Contains(docJSON, []byte(`"_embeddings"`)) ||
			bytes.Contains(docJSON, []byte(`"_summaries"`)) ||
			bytes.Contains(docJSON, []byte(`"_edges"`))

		if !hasSpecialFields {
			continue
		}

		// Extract special fields using internal method
		embWrites, sumWrites, edgWrites, edgDeletes, _, _, err := db.extractSpecialFields(docJSON, key)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("extracting special fields for key %s: %w", key, err)
		}

		embeddingWrites = append(embeddingWrites, embWrites...)
		summaryWrites = append(summaryWrites, sumWrites...)
		edgeWrites = append(edgeWrites, edgWrites...)
		edgeDeletes = append(edgeDeletes, edgDeletes...)
	}

	return embeddingWrites, summaryWrites, edgeWrites, edgeDeletes, nil
}

// ComputeEnrichments generates embeddings/summaries/chunks via enrichers
// Returns writes for generated enrichments (key:i:<name>:e, key:i:<name>:s, key:i:<name>:c:*)
// Also returns failedKeys that failed enrichment and need async retry after persistence
// This is the public API for dbWrapper to call before Raft proposal (SyncLevelEnrichments)
func (db *DBImpl) ComputeEnrichments(
	ctx context.Context,
	writes [][2][]byte,
) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error) {
	if db.indexManager == nil {
		return nil, nil, nil
	}
	return db.indexManager.ComputeEnrichments(ctx, writes)
}

func (db *DBImpl) extractSpecialFields(docJSON []byte, key []byte) (
	embeddingWrites [][2][]byte,
	summaryWrites [][2][]byte,
	edgeWrites [][2][]byte,
	edgeDeletes [][]byte,
	cleanedJSON []byte,
	hasNonSpecialFields bool,
	err error,
) {
	embeddingWrites = make([][2][]byte, 0)
	summaryWrites = make([][2][]byte, 0)
	edgeWrites = make([][2][]byte, 0)
	edgeDeletes = make([][]byte, 0)

	// Unmarshal document once
	var doc map[string]any
	if err := json.Unmarshal(docJSON, &doc); err != nil {
		return nil, nil, nil, nil, nil, false, fmt.Errorf("unmarshaling document: %w", err)
	}

	// Check for _embeddings field
	if embeddingsField, hasEmbeddings := doc["_embeddings"]; hasEmbeddings {
		embeddingsObj, ok := embeddingsField.(map[string]any)
		if !ok {
			return nil, nil, nil, nil, nil, false, errors.New("_embeddings field is not a map")
		}

		for indexName := range embeddingsObj {
			// Get index from IndexManager
			idx := db.indexManager.GetIndex(indexName)
			if idx == nil {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("index not found: %s", indexName)
			}

			// Check if index implements EmbeddingsPreProcessor interface
			embPreProcessor, ok := idx.(indexes.EmbeddingsPreProcessor)
			if !ok {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("index %s does not support embeddings", indexName)
			}

			// Get the embedding value for this index
			embValueAny, ok := embeddingsObj[indexName]
			if !ok {
				continue
			}

			// Detect sparse vs dense by value type:
			// - map[string]any → sparse vector (term_id string keys → float weights)
			// - []any → dense vector (float array)
			switch embValue := embValueAny.(type) {
			case map[string]any:
				// Sparse vector path: {"42": 0.8, "1337": 1.2, ...}
				indices := make([]uint32, 0, len(embValue))
				values := make([]float32, 0, len(embValue))
				for termIDStr, weightAny := range embValue {
					var termID int64
					if _, err := fmt.Sscanf(termIDStr, "%d", &termID); err != nil {
						return nil, nil, nil, nil, nil, false, fmt.Errorf(
							"invalid sparse term ID %q for index %s: %w",
							termIDStr, indexName, err,
						)
					}
					weight, ok := weightAny.(float64)
					if !ok {
						return nil, nil, nil, nil, nil, false, fmt.Errorf(
							"invalid sparse weight for term %s in index %s: expected number",
							termIDStr, indexName,
						)
					}
					indices = append(indices, uint32(termID)) //nolint:gosec // G115: bounded value, cannot overflow in practice
					values = append(values, float32(weight))
				}

				// Get hashID from the index's prompt renderer
				_, hashID, err := embPreProcessor.RenderPrompt(doc)
				if err != nil {
					db.logger.Warn("Failed to render prompt for sparse hashID",
						zap.String("key", types.FormatKey(key)),
						zap.String("indexName", indexName),
						zap.Error(err))
					hashID = 0
				}

				// Encode sparse vector with hashID prefix
				sv := vector.NewSparseVector(indices, values)
				sparseBytes := indexes.EncodeSparseVec(sv)
				value := make([]byte, 8+len(sparseBytes))
				binary.LittleEndian.PutUint64(value[:8], hashID)
				copy(value[8:], sparseBytes)

				// Build sparse key: key:i:<indexName>:sp
				sparseKey := storeutils.MakeSparseKey(key, indexName)
				embeddingWrites = append(embeddingWrites, [2][]byte{sparseKey, value})

				db.logger.Debug("Extracted user-provided sparse embedding",
					zap.String("key", types.FormatKey(key)),
					zap.String("indexName", indexName),
					zap.Int("numTerms", len(indices)),
					zap.Uint64("hashID", hashID))

			case []any:
				// Dense vector path (existing behavior)
				expectedDim := embPreProcessor.GetDimension()

				embedding, err := embeddings.ConvertToFloat32Slice(embValue)
				if err != nil {
					return nil, nil, nil, nil, nil, false, fmt.Errorf(
						"converting embedding for index %s: %w",
						indexName, err,
					)
				}

				if len(embedding) != expectedDim {
					return nil, nil, nil, nil, nil, false, fmt.Errorf(
						"dimension mismatch for index %s: expected %d, got %d",
						indexName, expectedDim, len(embedding),
					)
				}

				_, hashID, err := embPreProcessor.RenderPrompt(doc)
				if err != nil {
					db.logger.Warn("Failed to render prompt for hashID",
						zap.String("key", types.FormatKey(key)),
						zap.String("indexName", indexName),
						zap.Error(err))
					hashID = 0
				}

				vecBuf := make([]byte, 0, 8+4*len(embedding)+4)
				vecBuf, _ = vectorindex.EncodeEmbeddingWithHashID(vecBuf, embedding, hashID)

				embKey := storeutils.MakeEmbeddingKey(key, indexName)
				embeddingWrites = append(embeddingWrites, [2][]byte{embKey, vecBuf})

				db.logger.Debug("Extracted user-provided embedding",
					zap.String("key", types.FormatKey(key)),
					zap.String("indexName", indexName),
					zap.Int("dimension", len(embedding)),
					zap.Uint64("hashID", hashID))

			default:
				return nil, nil, nil, nil, nil, false, fmt.Errorf(
					"embedding for index %s must be an array (dense) or object (sparse), got %T",
					indexName, embValueAny,
				)
			}
		}
	}

	// Check for _summaries field
	if summariesField, hasSummaries := doc["_summaries"]; hasSummaries {
		summariesObj, ok := summariesField.(map[string]any)
		if !ok {
			return nil, nil, nil, nil, nil, false, errors.New("_summaries field is not a map")
		}

		for indexName, summaryAny := range summariesObj {
			// Verify index exists
			idx := db.indexManager.GetIndex(indexName)
			if idx == nil {
				db.logger.Warn("Summary provided for non-existent index",
					zap.String("key", types.FormatKey(key)),
					zap.String("indexName", indexName))
				continue
			}

			// Get the summary string for this index
			summaryText, ok := summaryAny.(string)
			if !ok {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("summary for index %s is not a string", indexName)
			}

			// Generate hashID from summary text
			hashID := xxhash.Sum64String(summaryText)

			// Encode summary: hashID + text
			sumBuf := make([]byte, 0, len(summaryText)+8)
			sumBuf = encoding.EncodeUint64Ascending(sumBuf, hashID)
			sumBuf = append(sumBuf, []byte(summaryText)...)

			// Build summary key: key:i:<indexName>:s
			sumKey := storeutils.MakeSummaryKey(key, indexName)

			summaryWrites = append(summaryWrites, [2][]byte{sumKey, sumBuf})

			db.logger.Debug("Extracted user-provided summary",
				zap.String("key", types.FormatKey(key)),
				zap.String("indexName", indexName),
				zap.Uint64("hashID", hashID))
		}
	}

	// Check for _edges field
	if edgesField, hasEdges := doc["_edges"]; hasEdges {
		edgesObj, ok := edgesField.(map[string]any)
		if !ok {
			return nil, nil, nil, nil, nil, false, errors.New("_edges field is not a map")
		}

		// Iterate through each graph index
		for indexName, edgeTypesAny := range edgesObj {
			// Verify the index exists and is a graph index
			idx := db.indexManager.GetIndex(indexName)
			if idx == nil {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("graph index not found: %s", indexName)
			}

			// Verify it's a graph_v0 index
			db.indexesMu.RLock()
			indexConfig, exists := db.indexes[indexName]
			db.indexesMu.RUnlock()

			if !exists || !indexes.IsGraphType(indexConfig.Type) {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("index %s is not a graph index", indexName)
			}

			// Parse edge types map
			edgeTypesMap, ok := edgeTypesAny.(map[string]any)
			if !ok {
				return nil, nil, nil, nil, nil, false, fmt.Errorf("edge types for index %s is not a map", indexName)
			}

			// Iterate through each edge type
			for edgeType, edgesListAny := range edgeTypesMap {
				edgesList, ok := edgesListAny.([]any)
				if !ok {
					return nil, nil, nil, nil, nil, false, fmt.Errorf("edges for type %s is not an array", edgeType)
				}

				// Process each edge
				for _, edgeAny := range edgesList {
					edgeMap, ok := edgeAny.(map[string]any)
					if !ok {
						return nil, nil, nil, nil, nil, false, errors.New("edge is not a map")
					}

					// Extract target (required)
					targetAny, hasTarget := edgeMap["target"]
					if !hasTarget {
						return nil, nil, nil, nil, nil, false, errors.New("edge missing required 'target' field")
					}
					target, ok := targetAny.(string)
					if !ok {
						return nil, nil, nil, nil, nil, false, errors.New("edge 'target' field is not a string")
					}

					// Extract weight (optional, default 1.0)
					weight := 1.0
					if weightAny, hasWeight := edgeMap["weight"]; hasWeight {
						if weightFloat, ok := weightAny.(float64); ok {
							weight = weightFloat
						}
					}

					// Extract metadata (optional)
					var metadata map[string]any
					if metadataAny, hasMetadata := edgeMap["metadata"]; hasMetadata {
						if metadataMap, ok := metadataAny.(map[string]any); ok {
							metadata = metadataMap
						}
					}

					// Create edge
					now := time.Now()
					edge := &indexes.Edge{
						Source:    key,
						Target:    []byte(target),
						Type:      edgeType,
						Weight:    weight,
						CreatedAt: now,
						UpdatedAt: now,
						Metadata:  metadata,
					}

					// Encode edge value
					edgeValue, err := indexes.EncodeEdgeValue(edge)
					if err != nil {
						return nil, nil, nil, nil, nil, false, fmt.Errorf("encoding edge: %w", err)
					}

					// Create edge key
					edgeKey := storeutils.MakeEdgeKey(key, []byte(target), indexName, edgeType)

					edgeWrites = append(edgeWrites, [2][]byte{edgeKey, edgeValue})

					db.logger.Debug("Extracted user-provided edge",
						zap.String("key", types.FormatKey(key)),
						zap.String("index", indexName),
						zap.String("edgeType", edgeType),
						zap.String("target", target),
						zap.Float64("weight", weight))
				}
			}
		}
	}

	// Edge reconciliation: Delete edges that exist but aren't in _edges
	// This makes edge management declarative like embeddings/summaries
	if edgesField, hasEdges := doc["_edges"]; hasEdges {
		edgesObj, _ := edgesField.(map[string]any)

		// Track which edges should exist based on _edges field
		desiredEdges := make(map[string]bool) // key = edgeKey

		// Build set of desired edges from edgeWrites
		for _, edgeWrite := range edgeWrites {
			edgeKey := string(edgeWrite[0])
			desiredEdges[edgeKey] = true
		}

		// For each graph index in _edges, find edges to delete
		pdb := db.getPDB()
		for indexName := range edgesObj {
			if pdb == nil {
				break
			}
			// Scan for existing edges with this index name
			prefix := storeutils.EdgeIteratorPrefix(key, indexName, "")

			iter, err := pdb.NewIter(&pebble.IterOptions{
				LowerBound: prefix,
				UpperBound: utils.PrefixSuccessor(prefix),
			})
			if err != nil {
				db.logger.Warn("Failed to create iterator for edge reconciliation",
					zap.String("key", types.FormatKey(key)),
					zap.String("index", indexName),
					zap.Error(err))
				continue
			}

			// Use a closure to ensure iterator is always closed via defer
			func() {
				defer func() {
					if err := iter.Close(); err != nil {
						db.logger.Warn("Error closing edge reconciliation iterator",
							zap.String("key", types.FormatKey(key)),
							zap.String("index", indexName),
							zap.Error(err))
					}
				}()

				// Check each existing edge
				for iter.First(); iter.Valid(); iter.Next() {
					// If this edge isn't in the desired set, mark for deletion
					if !desiredEdges[string(iter.Key())] {
						edgeDeletes = append(edgeDeletes, bytes.Clone(iter.Key()))

						db.logger.Debug("Marking edge for deletion (not in _edges)",
							zap.String("key", types.FormatKey(key)),
							zap.String("index", indexName),
							zap.String("edgeKey", types.FormatKey(iter.Key())))
					}
				}
				if iterErr := iter.Error(); iterErr != nil {
					db.logger.Warn("Edge reconciliation iterator error",
						zap.String("key", types.FormatKey(key)),
						zap.String("index", indexName),
						zap.Error(iterErr))
				}
			}()
		}
	}

	// Check if document has any non-special fields
	// (i.e., fields other than _embeddings, _summaries, _edges)
	hasNonSpecialFields = false
	for key := range doc {
		if key != "_embeddings" && key != "_summaries" && key != "_edges" {
			hasNonSpecialFields = true
			break
		}
	}

	// Remove special fields and re-marshal if any were found
	if len(embeddingWrites) > 0 || len(summaryWrites) > 0 || len(edgeWrites) > 0 || len(edgeDeletes) > 0 {
		delete(doc, "_embeddings")
		delete(doc, "_summaries")
		delete(doc, "_edges")

		cleanedJSON, err = json.Marshal(doc)
		if err != nil {
			return nil, nil, nil, nil, nil, false, fmt.Errorf("marshaling cleaned document: %w", err)
		}
		return embeddingWrites, summaryWrites, edgeWrites, edgeDeletes, cleanedJSON, hasNonSpecialFields, nil
	}

	// No special fields found, return original JSON
	return embeddingWrites, summaryWrites, edgeWrites, edgeDeletes, docJSON, hasNonSpecialFields, nil
}

// shouldWriteTimestamp determines if a key should have a :t timestamp key written
// Timestamps are written for:
// - Document keys (main data, ending with :o)
// - Edge keys (containing :i:...:out: or :i:...:in:)
// Timestamps are NOT written for:
// - Embedding keys (ending with :e)
// - Summary keys (ending with :s)
// - Metadata keys (prefixed with MetadataPrefix)
func shouldWriteTimestamp(key []byte) bool {
	// Skip metadata keys
	if bytes.HasPrefix(key, MetadataPrefix) {
		return false
	}
	// Skip embedding keys
	if bytes.HasSuffix(key, EmbeddingSuffix) {
		return false
	}
	// Skip summary keys
	if bytes.HasSuffix(key, SummarySuffix) {
		return false
	}
	// Skip sparse embedding keys
	if bytes.HasSuffix(key, SparseSuffix) {
		return false
	}
	// Everything else gets a timestamp (documents ending with :o, edges)
	return true
}

func (db *DBImpl) Batch(
	ctx context.Context,
	writes [][2][]byte,
	deletes [][]byte,
	syncLevel Op_SyncLevel,
) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}

	// Get HLC timestamp from context (set by metadata layer for consistent timestamps)
	timestamp := storeutils.GetTimestampFromContext(ctx)
	splitState := db.splitState

	// TODO (ajr) Add size hint for big batches?
	batch := pdb.NewBatch()
	defer func() {
		_ = batch.Close()
	}()
	numWrites := 0
	numDeletes := 0
	encoder := db.encoderPool.Get().(*zstd.Encoder)
	defer db.encoderPool.Put(encoder)

	// Track all writes to pass to index manager (including extracted embeddings)
	allIndexWrites := make([][2][]byte, 0, len(writes)*2)
	allChunkDeletes := make([][]byte, 0)
	// Track all edge deletes for reconciliation
	allEdgeDeletes := make([][]byte, 0)
	splitDeltaWrites := make([][2][]byte, 0, len(writes))
	splitDeltaDeletes := make([][]byte, 0, len(deletes))

	for _, kv := range writes {
		if !isKeyOwnedDuringSplit(kv[0], db.byteRange, splitState) {
			// This can happen during a shard split when a write was committed to Raft
			// just before the split operation was applied. The write is skipped because
			// the key is now outside this shard's range.
			// WARNING: This causes data loss - the client received success but data is dropped.
			// This should be rare if pendingSplitKey mechanism is working correctly.
			db.logger.Warn("SPLIT_DATA_LOSS: Skipping write outside byte range - data will be lost",
				zap.String("key", types.FormatKey(kv[0])),
				zap.Stringer("range", db.byteRange),
				zap.String("dir", db.dir))
			continue
		}
		if isKeyInSplitOffRangeForState(kv[0], splitState) {
			splitDeltaWrites = append(splitDeltaWrites, kv)
		}

		if bytes.HasSuffix(kv[0], EmbeddingSuffix) {
			if len(kv[1]) == 0 {
				db.logger.Error("Skipping empty embedding write", zap.String("key", types.FormatKey(kv[0])))
				continue
			}
			// Skip compressing embeddings
			if err := batch.Set(kv[0], kv[1], nil); err != nil {
				db.logger.Error(
					"could not set key for write",
					zap.String("key", types.FormatKey(kv[0])),
					zap.Error(err),
				)
				continue
			}
			numWrites++
			allIndexWrites = append(allIndexWrites, kv)
			continue
		}
		if bytes.HasSuffix(kv[0], SummarySuffix) {
			if len(kv[1]) == 0 {
				db.logger.Error("Skipping empty summary write", zap.String("key", types.FormatKey(kv[0])))
				continue
			}
			// Skip compressing summaries for now
			if err := batch.Set(kv[0], kv[1], nil); err != nil {
				db.logger.Error(
					"could not set key for write",
					zap.String("key", types.FormatKey(kv[0])),
					zap.Error(err),
				)
				continue
			}
			numWrites++
			allIndexWrites = append(allIndexWrites, kv)
			continue
		}
		if bytes.HasSuffix(kv[0], SparseSuffix) && bytes.Contains(kv[0], []byte(":i:")) {
			if len(kv[1]) == 0 {
				db.logger.Error("Skipping empty sparse embedding write", zap.String("key", types.FormatKey(kv[0])))
				continue
			}
			if err := batch.Set(kv[0], kv[1], nil); err != nil {
				db.logger.Error(
					"could not set sparse embedding key",
					zap.String("key", types.FormatKey(kv[0])),
					zap.Error(err),
				)
				continue
			}
			numWrites++
			allIndexWrites = append(allIndexWrites, kv)
			continue
		}
		// Handle chunk keys which have format: docKey:i:indexName:c:chunkID
		// Chunk values are stored as [hashID:uint64][chunkJSON], so they don't start with '{'
		if storeutils.IsChunkKey(kv[0]) {
			if len(kv[1]) < 8 {
				db.logger.Error("Skipping invalid chunk write (too short)", zap.String("key", types.FormatKey(kv[0])))
				continue
			}
			// Chunks are stored as-is with hash ID prefix
			if err := batch.Set(kv[0], kv[1], nil); err != nil {
				db.logger.Error(
					"could not set key for write",
					zap.String("key", types.FormatKey(kv[0])),
					zap.Error(err),
				)
				continue
			}
			numWrites++
			allIndexWrites = append(allIndexWrites, kv)
			continue
		}
		// Handle graph edge keys (binary-encoded by EncodeEdgeValue, not JSON)
		// and field hash keys (uint64 hash for change detection).
		// Edge keys: <source>:i:<indexName>:out:<edgeType>:<target>:o
		// Field hash keys: <docKey>:i:<graphName>:<edgeType>:fh
		if storeutils.IsEdgeKey(kv[0]) || (bytes.Contains(kv[0], []byte(":i:")) && bytes.HasSuffix(kv[0], []byte(":fh"))) {
			if len(kv[1]) == 0 {
				db.logger.Error("Skipping empty graph index write", zap.String("key", types.FormatKey(kv[0])))
				continue
			}
			if err := batch.Set(kv[0], kv[1], nil); err != nil {
				db.logger.Error(
					"could not set graph index key",
					zap.String("key", types.FormatKey(kv[0])),
					zap.Error(err),
				)
				continue
			}
			numWrites++
			allIndexWrites = append(allIndexWrites, kv)
			continue
		}

		if !bytes.HasPrefix(kv[1], []byte{'{'}) {
			db.logger.Error(
				"Skipping non-JSON write",
				zap.String("key", types.FormatKey(kv[0])),
				zap.String("value", types.FormatKey(kv[1])),
			)
			continue
		}

		// Extract and write special fields (_embeddings, _summaries, _edges) if present
		valueToCompress := kv[1]
		shouldWriteDocument := true

		sfResult, err := db.extractAndWriteSpecialFields(batch, kv[1], kv[0], timestamp)
		if err != nil {
			return fmt.Errorf("extracting special fields for key %s: %w", kv[0], err)
		}
		if sfResult != nil {
			numWrites += sfResult.NumWrites
			allIndexWrites = append(allIndexWrites, sfResult.IndexWrites...)
			allEdgeDeletes = append(allEdgeDeletes, sfResult.EdgeDeletes...)
			valueToCompress = sfResult.CleanedJSON

			if !sfResult.HasNonSpecialFields {
				shouldWriteDocument = false
				db.logger.Debug("Skipping document write (only special fields present)",
					zap.String("key", types.FormatKey(kv[0])))
			}
		}

		// Only write the main document if it has non-special fields
		if shouldWriteDocument {
			key := storeutils.KeyRangeStart(kv[0])
			if err := db.compressAndSet(batch, encoder, key, valueToCompress); err != nil {
				db.logger.Error("could not write document",
					zap.String("key", types.FormatKey(kv[0])),
					zap.Error(err))
				continue
			}

			// Write timestamp for document if timestamp is available
			if timestamp > 0 && shouldWriteTimestamp(key) {
				txnKey := slices.Concat(key, storeutils.TransactionSuffix)
				timestampBytes := encoding.EncodeUint64Ascending(nil, timestamp)
				if err := batch.Set(txnKey, timestampBytes, nil); err != nil {
					db.logger.Warn("could not write timestamp for document",
						zap.String("key", types.FormatKey(key)),
						zap.Error(err))
				}
			}

			numWrites++
			allIndexWrites = append(allIndexWrites, kv)
		}
	}
	// Collect edge keys for deletion (so edge index can be updated)

	for _, k := range deletes {
		if !isKeyOwnedDuringSplit(k, db.byteRange, splitState) {
			continue
		}
		if isKeyInSplitOffRangeForState(k, splitState) {
			splitDeltaDeletes = append(splitDeltaDeletes, k)
		}

		// Scan for outgoing edges before deleting the document
		// This allows the graph index to update its edge index
		edgeKeys, err := db.collectOutgoingEdgeKeys(k)
		if err != nil {
			db.logger.Warn("Failed to collect edge keys for deletion",
				zap.String("key", types.FormatKey(k)),
				zap.Error(err))
			// Continue with deletion even if edge collection fails
		} else {
			allEdgeDeletes = append(allEdgeDeletes, edgeKeys...)
		}
		chunkKeys, err := db.collectChunkKeysForDeletion(k)
		if err != nil {
			db.logger.Warn("Failed to collect chunk keys for deletion",
				zap.String("key", types.FormatKey(k)),
				zap.Error(err))
		} else {
			allChunkDeletes = append(allChunkDeletes, chunkKeys...)
		}

		if err := batch.DeleteRange(k, utils.PrefixSuccessor(storeutils.KeyRangeEnd(k)), nil); err != nil {
			db.logger.Error(
				"could not delete key for delete",
				zap.String("key", types.FormatKey(k)),
				zap.Error(err),
			)
			continue
		}
		numDeletes++
	}

	writeOps.WithLabelValues().Add(float64(numWrites))
	deleteOps.WithLabelValues().Add(float64(numDeletes))
	if err := db.appendSplitDelta(batch, splitDeltaWrites, splitDeltaDeletes, timestamp); err != nil {
		return fmt.Errorf("appending split delta entry: %w", err)
	}
	if err := pdb.Apply(batch, pebble.Sync); err != nil {
		return fmt.Errorf("could not set key: %w", err)
	}

	// Combine document deletes, chunk deletes, and edge deletes for index manager.
	allDeletes := slices.Concat(deletes, allChunkDeletes, allEdgeDeletes)

	// Route writes and deletes to appropriate IndexManager(s) based on split state.
	// During a split, keys in [splitKey, originalRangeEnd) go to the shadow IndexManager,
	// while keys in [start, splitKey) go to the primary IndexManager.
	if err := db.routeToIndexManagers(ctx, allIndexWrites, allDeletes, syncLevel); err != nil {
		return err
	}
	return nil
}

// routeToIndexManagers routes index operations to the appropriate IndexManager(s).
// During normal operation, all operations go to the primary indexManager.
// During a split (PHASE_PREPARE or PHASE_SPLITTING), operations are partitioned:
//   - Keys in [start, splitKey) go to the primary indexManager
//   - Keys in [splitKey, originalRangeEnd) go to the shadow indexManager
func (db *DBImpl) routeToIndexManagers(ctx context.Context, writes [][2][]byte, deletes [][]byte, syncLevel Op_SyncLevel) error {
	// Check if we have an active split with a shadow IndexManager
	splitState := db.splitState
	shadow := db.shadowIndexManager

	// No split in progress or shadow not yet created - send all to primary
	if splitState == nil || shadow == nil ||
		(splitState.GetPhase() != SplitState_PHASE_PREPARE && splitState.GetPhase() != SplitState_PHASE_SPLITTING) {
		return db.sendToIndexManager(ctx, db.indexManager, writes, deletes, syncLevel)
	}

	// Partition writes and deletes by range
	splitKey := splitState.GetSplitKey()

	var primaryWrites, shadowWrites [][2][]byte
	var primaryDeletes, shadowDeletes [][]byte

	for _, w := range writes {
		key := w[0]
		if bytes.Compare(key, splitKey) >= 0 {
			shadowWrites = append(shadowWrites, w)
		} else {
			primaryWrites = append(primaryWrites, w)
		}
	}

	for _, d := range deletes {
		if bytes.Compare(d, splitKey) >= 0 {
			shadowDeletes = append(shadowDeletes, d)
		} else {
			primaryDeletes = append(primaryDeletes, d)
		}
	}

	// Send to primary IndexManager
	if len(primaryWrites) > 0 || len(primaryDeletes) > 0 {
		if err := db.sendToIndexManager(ctx, db.indexManager, primaryWrites, primaryDeletes, syncLevel); err != nil {
			return fmt.Errorf("indexing primary batch: %w", err)
		}
	}

	// Send to shadow IndexManager
	if len(shadowWrites) > 0 || len(shadowDeletes) > 0 {
		if err := db.sendToIndexManager(ctx, shadow, shadowWrites, shadowDeletes, syncLevel); err != nil {
			// Log but don't fail - shadow index errors shouldn't block the primary path
			db.logger.Warn("Failed to send batch to shadow IndexManager",
				zap.Error(err),
				zap.Int("writes", len(shadowWrites)),
				zap.Int("deletes", len(shadowDeletes)))
		}
	}

	return nil
}

// sendToIndexManager sends a batch of writes and deletes to a specific IndexManager.
func (db *DBImpl) sendToIndexManager(ctx context.Context, im *IndexManager, writes [][2][]byte, deletes [][]byte, syncLevel Op_SyncLevel) error {
	if im == nil {
		return nil
	}
	if err := im.Batch(ctx, writes, deletes, syncLevel); err != nil {
		// Handle partial success for FullText level
		if syncLevel == Op_SyncLevelFullText && errors.Is(err, ErrPartialSuccess) {
			return err // Propagate to signal WAL queued
		}
		return fmt.Errorf("indexing batch: %w", err)
	}
	return nil
}

// routeSearch routes search queries to the appropriate IndexManager(s) based on split state.
// During normal operation, queries go to the primary IndexManager.
// During a split (PHASE_PREPARE or PHASE_SPLITTING), queries go to both primary and shadow,
// and results are merged.
//
// Returns the search result and any error.
// The filterPrefix parameter is used to determine if the query overlaps with either range.
// If filterPrefix is nil, both indexes are queried.
func (db *DBImpl) routeSearch(ctx context.Context, indexName string, request any, filterPrefix []byte) (any, error) {
	// Check if we have an active split with a shadow IndexManager
	splitState := db.splitState
	shadow := db.shadowIndexManager

	// No split in progress or shadow not yet created - query primary only
	if splitState == nil || shadow == nil ||
		(splitState.GetPhase() != SplitState_PHASE_PREPARE && splitState.GetPhase() != SplitState_PHASE_SPLITTING) {
		return db.indexManager.Search(ctx, indexName, request)
	}

	splitKey := splitState.GetSplitKey()

	// Determine which indexes to query based on filterPrefix
	queryPrimary := true
	queryShadow := true

	if len(filterPrefix) > 0 {
		// If filterPrefix < splitKey, query primary only
		// If filterPrefix >= splitKey, query shadow only
		if bytes.Compare(filterPrefix, splitKey) >= 0 {
			queryPrimary = false
		} else {
			queryShadow = false
		}
	}

	if bleveReq, ok := request.(*bleve.SearchRequest); ok && len(filterPrefix) == 0 {
		if optimizedReq, route, optimized := optimizeSplitBleveDocIDRequest(bleveReq, splitKey); optimized {
			request = optimizedReq
			switch route {
			case "primary":
				queryShadow = false
			case "shadow":
				queryPrimary = false
			}
			splitSearchRoutesTotal.WithLabelValues("docid_" + route).Inc()
		}
	}

	// If we only need to query one index, do that
	if !queryPrimary {
		splitSearchRoutesTotal.WithLabelValues("shadow_only").Inc()
		if !shadow.HasIndex(indexName) {
			// Shadow doesn't have this index yet, fall back to primary
			return db.indexManager.Search(ctx, indexName, request)
		}
		return shadow.Search(ctx, indexName, request)
	}
	if !queryShadow {
		splitSearchRoutesTotal.WithLabelValues("primary_only").Inc()
		return db.indexManager.Search(ctx, indexName, request)
	}
	splitSearchRoutesTotal.WithLabelValues("both").Inc()

	// Query both indexes and merge results
	primaryResult, primaryErr := db.indexManager.Search(ctx, indexName, request)
	if primaryErr != nil {
		db.logger.Warn("Primary index search failed during split",
			zap.String("index", indexName),
			zap.Error(primaryErr))
	}

	if !shadow.HasIndex(indexName) {
		// Shadow doesn't have this index yet, return primary result
		return primaryResult, primaryErr
	}

	shadowResult, shadowErr := shadow.Search(ctx, indexName, request)
	if shadowErr != nil {
		db.logger.Warn("Shadow index search failed during split",
			zap.String("index", indexName),
			zap.Error(shadowErr))
	}

	// If primary failed, return shadow result
	if primaryErr != nil {
		return shadowResult, shadowErr
	}
	// If shadow failed, return primary result
	if shadowErr != nil {
		return primaryResult, nil
	}

	// Merge results based on type
	return db.mergeSearchResults(primaryResult, shadowResult)
}

func optimizeSplitBleveDocIDRequest(req *bleve.SearchRequest, splitKey []byte) (any, string, bool) {
	if req == nil || req.Query == nil {
		return req, "", false
	}

	docIDQuery, ok := req.Query.(*query.DocIDQuery)
	if !ok || len(docIDQuery.IDs) == 0 {
		return req, "", false
	}

	allPrimary := true
	allShadow := true
	for _, id := range docIDQuery.IDs {
		if bytes.Compare([]byte(id), splitKey) >= 0 {
			allPrimary = false
		} else {
			allShadow = false
		}
		if !allPrimary && !allShadow {
			return req, "", false
		}
	}

	cloned := *req
	cloned.Query = query.NewDocIDQuery(slices.Clone(docIDQuery.IDs))
	if allPrimary {
		return &cloned, "primary", true
	}
	if allShadow {
		return &cloned, "shadow", true
	}
	return req, "", false
}

// mergeSearchResults merges results from primary and shadow IndexManagers.
// It handles both Bleve and Vector search results.
func (db *DBImpl) mergeSearchResults(primary, shadow any) (any, error) {
	if primary == nil {
		return shadow, nil
	}
	if shadow == nil {
		return primary, nil
	}

	// Handle Bleve results
	if primaryBleve, ok := primary.(*bleve.SearchResult); ok {
		shadowBleve, ok := shadow.(*bleve.SearchResult)
		if !ok {
			return primary, nil // Type mismatch, return primary
		}
		return db.mergeBleveResults(primaryBleve, shadowBleve), nil
	}

	// Handle Vector results
	if primaryVector, ok := primary.(*vectorindex.SearchResult); ok {
		shadowVector, ok := shadow.(*vectorindex.SearchResult)
		if !ok {
			return primary, nil // Type mismatch, return primary
		}
		return db.mergeVectorResults(primaryVector, shadowVector), nil
	}

	// Unknown type, return primary
	return primary, nil
}

// mergeBleveResults merges two Bleve search results.
// Results are combined and re-sorted by score (descending).
func (db *DBImpl) mergeBleveResults(primary, shadow *bleve.SearchResult) *bleve.SearchResult {
	if primary == nil {
		return shadow
	}
	if shadow == nil {
		return primary
	}

	// Combine hits
	merged := &bleve.SearchResult{
		Status:   primary.Status,
		Request:  primary.Request,
		Total:    primary.Total + shadow.Total,
		MaxScore: max(primary.MaxScore, shadow.MaxScore),
		Took:     primary.Took + shadow.Took,
		Hits:     make(search.DocumentMatchCollection, 0, len(primary.Hits)+len(shadow.Hits)),
		Facets:   primary.Facets, // TODO: merge facets if needed
	}

	// Add hits from both results
	merged.Hits = append(merged.Hits, primary.Hits...)
	merged.Hits = append(merged.Hits, shadow.Hits...)

	// Sort by score descending
	sort.Slice(merged.Hits, func(i, j int) bool {
		return merged.Hits[i].Score > merged.Hits[j].Score
	})

	// Apply size limit from request if available
	if primary.Request != nil && primary.Request.Size > 0 && len(merged.Hits) > primary.Request.Size {
		merged.Hits = merged.Hits[:primary.Request.Size]
	}

	return merged
}

// mergeVectorResults merges two Vector search results.
// Results are combined and re-sorted by distance (ascending).
func (db *DBImpl) mergeVectorResults(primary, shadow *vectorindex.SearchResult) *vectorindex.SearchResult {
	if primary == nil {
		return shadow
	}
	if shadow == nil {
		return primary
	}

	// Combine hits
	merged := &vectorindex.SearchResult{
		Status:  primary.Status,
		Request: primary.Request,
		Hits:    make([]*vectorindex.SearchHit, 0, len(primary.Hits)+len(shadow.Hits)),
	}

	// Add hits from both results
	merged.Hits = append(merged.Hits, primary.Hits...)
	merged.Hits = append(merged.Hits, shadow.Hits...)

	// Sort by distance ascending (lower distance = better match)
	sort.Slice(merged.Hits, func(i, j int) bool {
		return merged.Hits[i].Distance < merged.Hits[j].Distance
	})

	// Apply K limit from request if available
	if primary.Request != nil && primary.Request.K > 0 && len(merged.Hits) > primary.Request.K {
		merged.Hits = merged.Hits[:primary.Request.K]
	}

	return merged
}

func (s *DBImpl) Snapshot(id string) (int64, error) {
	// Create a temporary staging directory for the snapshot (Archive Format v2)
	stagingDir := filepath.Join(os.TempDir(), fmt.Sprintf("antfly-snap-%s-%s", id, uuid.NewString()))
	defer func() {
		_ = os.RemoveAll(stagingDir)
	}()

	// Create the staging directory structure (pebble/ subdirectory)
	stagingPebbleDir := filepath.Join(stagingDir, "pebble")
	if err := os.MkdirAll(stagingDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return 0, fmt.Errorf("creating staging directory: %w", err)
	}

	// Pause IndexManager (and all indexes) before checkpoint
	indexesPaused := false
	if s.indexManager != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.indexManager.Pause(ctx); err != nil {
			s.logger.Warn("Failed to pause IndexManager, snapshot without indexes", zap.Error(err))
		} else {
			indexesPaused = true
			defer s.indexManager.Resume()
		}
	}

	// Create the pebble checkpoint into pebble/ subdirectory
	span := pebble.CheckpointSpan{
		Start: s.byteRange[0],             // Start of the original range
		End:   s.byteRange.EndForPebble(), // Use helper for Pebble-compatible End
	}
	pdb := s.getPDB()
	if pdb == nil {
		return 0, pebble.ErrClosed
	}
	err := pdb.Checkpoint(stagingPebbleDir,
		pebble.WithFlushedWAL(),
		pebble.WithRestrictToSpans([]pebble.CheckpointSpan{span}),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to create checkpoint: %w", err)
	}

	// Copy indexes directory (only if paused successfully)
	if indexesPaused {
		indexDir := s.indexManager.GetDir()
		if indexDir != "" {
			indexStagingDir := filepath.Join(stagingDir, "indexes")
			if err := common.CopyDir(indexDir, indexStagingDir); err != nil {
				s.logger.Warn("Failed to copy indexes, will rebuild on restore", zap.Error(err))
				_ = os.RemoveAll(indexStagingDir)
			} else {
				s.logger.Info("Included indexes in snapshot",
					zap.String("indexDir", indexDir),
					zap.String("stagingIndexDir", indexStagingDir))
			}
		}
	}

	// Parse shard and node IDs from directory path for snapshot metadata
	nodeID, shardID, err := common.ParseStorageDBDir(s.dir)
	if err != nil {
		s.logger.Warn("Failed to parse storage dir for snapshot metadata", zap.Error(err))
	}

	// Build snapshot options with shard metadata
	snapOpts := &snapstore.SnapshotOptions{
		ShardID: shardID,
		NodeID:  nodeID,
		Range:   s.byteRange,
	}

	// Use SnapStore to create and store the archive with metadata
	size, err := s.snapStore.CreateSnapshot(context.Background(), id, stagingDir, snapOpts)
	if err != nil {
		return 0, fmt.Errorf("creating snapshot archive: %w", err)
	}

	s.logger.Info("Created snapshot archive",
		zap.Int64("size", size),
		zap.String("snapID", id),
		zap.String("dir", s.dir))
	return size, nil
}

// filterFields applies JSONPath filtering to document fields
// If star is true, returns all fields without filtering
// Otherwise, extracts only the fields specified in regularPaths using JSONPath (RFC 9535)
// This works directly on Go maps without JSON serialization
func (s *DBImpl) filterFields(
	fields map[string]any,
	regularPaths []string,
	star bool,
) map[string]any {
	// If star, return all fields
	if star || len(regularPaths) == 0 {
		return fields
	}

	filteredFields := make(map[string]any)

	// Extract each requested path using JSONPath
	for _, originalPath := range regularPaths {
		// Ensure path starts with '$.' for root access
		pathStr := originalPath
		if pathStr[0] != '$' {
			pathStr = "$." + pathStr
		}

		path, err := jsonpath.Parse(pathStr)
		if err != nil {
			s.logger.Warn("Failed to parse JSONPath",
				zap.String("path", pathStr),
				zap.Error(err))
			continue
		}

		nodes := path.Select(fields)
		// Collect all matching values
		values := make([]any, 0)
		for node := range nodes.All() {
			values = append(values, node)
		}

		// If we got exactly one value, store it directly
		// If multiple values, store as array
		// If no values, skip this path
		// Use the original path (without $. prefix) as the key
		if len(values) == 1 {
			filteredFields[originalPath] = values[0]
		} else if len(values) > 1 {
			filteredFields[originalPath] = values
		}
	}

	// Special fields (_embeddings, _summaries, _chunks) are already in fields
	// Copy them over if they exist
	if emb, ok := fields["_embeddings"]; ok {
		filteredFields["_embeddings"] = emb
	}
	if sum, ok := fields["_summaries"]; ok {
		filteredFields["_summaries"] = sum
	}
	if chunks, ok := fields["_chunks"]; ok {
		filteredFields["_chunks"] = chunks
	}

	return filteredFields
}

func rerank(
	ctx context.Context,
	request indexes.RemoteIndexSearchRequest,
	documents []map[string]any,
) ([]float32, error) {
	if request.RerankerConfig == nil {
		return nil, nil
	}

	// Create reranker
	reranker, err := reranking.NewReranker(*request.RerankerConfig)
	if err != nil {
		return nil, fmt.Errorf("creating reranker: %w", err)
	}

	// Convert documents to schema.Document format
	schemaDocs := make([]schema.Document, len(documents))
	for i, doc := range documents {
		// Generate document ID (use index if no _id field)
		docID := fmt.Sprintf("doc_%d", i)
		if id, ok := doc["_id"].(string); ok {
			docID = id
		}

		schemaDocs[i] = schema.Document{
			ID:     docID,
			Fields: doc,
		}
	}

	// Rerank documents (reranker handles field/template extraction)
	scores, err := reranker.Rerank(ctx, request.RerankerQuery, schemaDocs)
	if err != nil {
		return nil, fmt.Errorf("reranking documents: %w", err)
	}

	// Verify we got the expected number of scores
	if len(scores) != len(documents) {
		return nil, fmt.Errorf("expected %d scores, got %d", len(documents), len(scores))
	}

	return scores, nil
}

// normalizeDocID extracts the base document ID from various key formats
// (summary keys, chunk keys, etc.)
func normalizeDocID(docID []byte) []byte {
	if bytes.HasSuffix(docID, SummarySuffix) {
		// Remove the summary suffix for the lookup
		// key has suffix ':i:<indexName>:s` so remove that
		// 1. Remove ':s' suffix
		docID = docID[:len(docID)-len(SummarySuffix)]
		// 2. Fix 'i:...' suffix
		i := bytes.LastIndex(docID, []byte(":i:"))
		// 3. Remove the 'i:<indexName>' part
		docID = docID[:i]
	} else if docKey, ok := storeutils.ExtractDocKeyFromChunk(docID); ok {
		// If this is a chunk key (e.g., "doc_xxx:i:embeddings:c:1"), extract the document key
		docID = docKey
	}
	return docID
}

// fetchDocumentsParallel fetches multiple documents in parallel with a concurrency limit.
// Returns a map of document ID to fields.
func (s *DBImpl) fetchDocumentsParallel(
	ctx context.Context,
	docIDs []string,
	queryOpts storeutils.QueryOptions,
) map[string]map[string]any {
	if len(docIDs) == 0 {
		return make(map[string]map[string]any)
	}

	results := make(map[string]map[string]any, len(docIDs))
	var mu sync.Mutex

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(10) // Limit concurrent document fetches

	for _, id := range docIDs {
		// capture for goroutine
		eg.Go(func() error {
			fields, err := s.getWithQueryOptions(egCtx, []byte(id), queryOpts)
			if err != nil {
				s.logger.Warn("Failed to lookup document",
					zap.String("key", id), zap.Error(err))
				return nil // Don't fail the whole batch for one missing doc
			}
			mu.Lock()
			results[id] = fields
			mu.Unlock()
			return nil
		})
	}

	// Wait for all fetches to complete (errors are logged, not returned)
	_ = eg.Wait()

	return results
}

func (s *DBImpl) lookupFields(
	ctx context.Context,
	request indexes.RemoteIndexSearchRequest,
	result *indexes.RemoteIndexSearchResult,
) {
	if request.CountStar {
		return
	}

	// Parse field selections to separate regular fields from special fields
	regularPaths, queryOpts := ParseFieldSelection(request.Columns)

	if result.BleveSearchResult != nil {
		// Phase 1: Normalize IDs and collect unique document IDs
		seenIDs := make(map[string]struct{})
		dupIndexes := make(map[int]struct{})
		docIDsToFetch := make([]string, 0, len(result.BleveSearchResult.Hits))

		for i, hit := range result.BleveSearchResult.Hits {
			docID := normalizeDocID([]byte(hit.ID))
			hit.ID = string(docID)

			if _, ok := seenIDs[hit.ID]; ok {
				dupIndexes[i] = struct{}{}
				continue
			}
			seenIDs[hit.ID] = struct{}{}
			docIDsToFetch = append(docIDsToFetch, hit.ID)
		}

		// Phase 2: Fetch all documents in parallel
		docFields := s.fetchDocumentsParallel(ctx, docIDsToFetch, queryOpts)

		// Phase 3: Map results back to hits and build documents slice for reranking
		documents := make([]map[string]any, 0, len(result.BleveSearchResult.Hits))
		validHits := make([]*search.DocumentMatch, 0, len(result.BleveSearchResult.Hits))

		for i, hit := range result.BleveSearchResult.Hits {
			if _, isDuplicate := dupIndexes[i]; isDuplicate {
				continue
			}
			fields, ok := docFields[hit.ID]
			if !ok {
				continue
			}
			documents = append(documents, fields)
			filteredFields := s.filterFields(fields, regularPaths, request.Star)
			hit.Fields = filteredFields
			validHits = append(validHits, hit)
		}
		result.BleveSearchResult.Hits = validHits

		scores, err := rerank(ctx, request, documents)
		if err != nil {
			// Log error but continue with original scores (graceful degradation)
			s.logger.Warn("Failed to rerank documents, using original scores", zap.Error(err))
			return
		}
		if len(scores) == 0 {
			return
		}
		for i := range scores {
			result.BleveSearchResult.Hits[i].Score = float64(scores[i])
		}
		sort.Slice(result.BleveSearchResult.Hits, func(i, j int) bool {
			return scores[i] > scores[j] // Descending order (highest scores first)
		})
		return
	} else if result.FusionResult != nil {
		// Phase 1: Collect document IDs to fetch, with deduplication tracking
		seenIDs := make(map[string]struct{})
		dupIndexes := make(map[int]struct{})
		docIDsToFetch := make([]string, 0, len(result.FusionResult.Hits))
		for i, hit := range result.FusionResult.Hits {
			docID := string(normalizeDocID([]byte(hit.ID)))
			hit.ID = docID
			if _, ok := seenIDs[docID]; ok {
				dupIndexes[i] = struct{}{}
				continue
			}
			seenIDs[docID] = struct{}{}
			docIDsToFetch = append(docIDsToFetch, docID)
		}

		// Phase 2: Fetch all unique documents in parallel
		docFields := s.fetchDocumentsParallel(ctx, docIDsToFetch, queryOpts)

		// Phase 3: Map results back to hits (skip duplicates and missing docs)
		documents := make([]map[string]any, 0, len(result.FusionResult.Hits))
		validHits := make([]*indexes.FusionHit, 0, len(result.FusionResult.Hits))

		for i, hit := range result.FusionResult.Hits {
			// Skip duplicates
			if _, isDuplicate := dupIndexes[i]; isDuplicate {
				continue
			}

			fields, ok := docFields[hit.ID]
			if !ok {
				continue
			}
			documents = append(documents, fields)
			filteredFields := s.filterFields(fields, regularPaths, request.Star)
			hit.Fields = filteredFields
			validHits = append(validHits, hit)
		}
		result.FusionResult.Hits = validHits

		if request.RerankerConfig == nil || len(documents) == 0 {
			return
		}
		scores, err := rerank(ctx, request, documents)
		if err != nil {
			// Log error but continue with original scores (graceful degradation)
			s.logger.Warn("Failed to rerank documents, using original scores", zap.Error(err))
			return
		}
		if len(scores) == 0 {
			return
		}
		for i := range scores {
			score := float64(scores[i])
			result.FusionResult.Hits[i].RerankedScore = &score
		}
		sort.Slice(result.FusionResult.Hits, func(i, j int) bool {
			return scores[i] > scores[j] // Descending order (highest scores first)
		})
		return
	} else {
		// Vector search results - collect all unique docIDs across all indexes,
		// fetch once, then distribute results to each index's hits.

		// Phase 1: Normalize IDs and collect unique docIDs across all vector indexes
		type perIndexState struct {
			dupIndexes map[int]struct{}
		}
		indexStates := make(map[string]*perIndexState, len(result.VectorSearchResult))
		globalSeenIDs := make(map[string]struct{})
		allDocIDsToFetch := make([]string, 0)

		for indexName, vResult := range result.VectorSearchResult {
			state := &perIndexState{dupIndexes: make(map[int]struct{})}
			indexStates[indexName] = state
			seenInIndex := make(map[string]struct{})

			for i, hit := range vResult.Hits {
				docID := string(normalizeDocID([]byte(hit.ID)))
				hit.ID = docID

				// Per-index dedup (same docID appearing multiple times in one index)
				if _, ok := seenInIndex[docID]; ok {
					state.dupIndexes[i] = struct{}{}
					continue
				}
				seenInIndex[docID] = struct{}{}

				// Global dedup for fetch (avoid fetching the same doc for multiple indexes)
				if _, ok := globalSeenIDs[docID]; !ok {
					globalSeenIDs[docID] = struct{}{}
					allDocIDsToFetch = append(allDocIDsToFetch, docID)
				}
			}
		}

		// Phase 2: Single parallel fetch for all unique documents across all indexes
		docFields := s.fetchDocumentsParallel(ctx, allDocIDsToFetch, queryOpts)

		// Phase 3: Distribute fetched documents to each index's hits
		for indexName, vResult := range result.VectorSearchResult {
			state := indexStates[indexName]

			documents := make([]map[string]any, 0, len(vResult.Hits))
			validHits := make([]*vectorindex.SearchHit, 0, len(vResult.Hits))

			for i, hit := range vResult.Hits {
				if _, isDuplicate := state.dupIndexes[i]; isDuplicate {
					continue
				}

				fields, ok := docFields[hit.ID]
				if !ok {
					continue
				}
				documents = append(documents, fields)
				filteredFields := s.filterFields(fields, regularPaths, request.Star)
				hit.Fields = filteredFields
				validHits = append(validHits, hit)
			}
			vResult.Hits = validHits

			if request.RerankerConfig == nil || len(documents) == 0 {
				continue
			}
			scores, err := rerank(ctx, request, documents)
			if err != nil {
				s.logger.Warn("Failed to rerank documents, using original scores", zap.Error(err))
				continue
			}
			if len(scores) == 0 {
				continue
			}
			for i := range scores {
				vResult.Hits[i].Score = scores[i]
			}
			sort.Slice(vResult.Hits, func(i, j int) bool {
				return scores[i] > scores[j]
			})
		}
	}
}

func (s *DBImpl) Search(ctx context.Context, encodedReqest []byte) (resp []byte, err error) {
	queryOps.WithLabelValues().Inc()
	searchStart := time.Now()
	defer pebbleutils.RecoverPebbleClosed(&err)
	if len(encodedReqest) == 0 {
		return nil, ErrEmptyRequest
	}
	res := indexes.RemoteIndexSearchResult{
		VectorSearchResult: make(map[string]*vectorindex.SearchResult),
	}
	var searchRequest indexes.RemoteIndexSearchRequest
	if err := json.Unmarshal(encodedReqest, &searchRequest); err != nil {
		s.logger.Error("Failed to unmarshal search request", zap.Error(err))
		return nil, fmt.Errorf("decoding search: %w", err)
	}
	fullTextIndexName := fmt.Sprintf("full_text_index_v%d", searchRequest.FullTextIndexVersion)
	if !s.indexManager.HasIndex(fullTextIndexName) {
		// Fallback: try v0
		fullTextIndexName = "full_text_index_v0"
	}
	if !s.indexManager.HasIndex(fullTextIndexName) {
		// Legacy fallback
		fullTextIndexName = "full_text_index"
	}
	if !s.indexManager.HasIndex(fullTextIndexName) {
		return nil, fmt.Errorf(
			"full_text_index_v%d does not exist",
			searchRequest.FullTextIndexVersion,
		)
	}
	if searchRequest.BleveSearchRequest != nil {
		resp, err := s.routeSearch(ctx, fullTextIndexName, searchRequest.BleveSearchRequest, searchRequest.FilterPrefix)
		if err != nil {
			return nil, fmt.Errorf("searching bleve: %w", err)
		}
		result, ok := resp.(*bleve.SearchResult)
		if !ok {
			return nil, fmt.Errorf("unexpected response type from bleve search: %T", resp)
		}
		s.logger.Debug(
			"Searched bleve with query",
			zap.Uint64("total", result.Total),
			zap.Any("satus", result.Status),
		)
		res.BleveSearchResult = result
	}
	if len(searchRequest.VectorSearches) != 0 || len(searchRequest.SparseSearches) != 0 {
		if searchRequest.CountStar {
			return nil, errors.New("count star is not supported with vector search")
		}
		pagingOpts := searchRequest.VectorPagingOpts
		var filterIDs []uint64
		var filterDocKeys []string
		skipVectorSearch := false
		hasFilterQuery := len(searchRequest.FilterQuery) > 0
		if hasFilterQuery {
			q, err := query.ParseQuery(searchRequest.FilterQuery)
			if err != nil {
				return nil, fmt.Errorf("parsing filter query: %w", err)
			}
			bleveSearchRequest := bleve.NewSearchRequest(q)
			bleveSearchRequest.Size = math.MaxInt
			resp, err := s.routeSearch(ctx, fullTextIndexName, bleveSearchRequest, searchRequest.FilterPrefix)
			if err != nil {
				return nil, fmt.Errorf("searching bleve: %w", err)
			}
			result, ok := resp.(*bleve.SearchResult)
			if !ok {
				return nil, fmt.Errorf("unexpected response type from bleve search: %T", resp)
			}
			filterIDs = make([]uint64, 0, result.Hits.Len())
			filterDocKeys = make([]string, 0, result.Hits.Len())
			for _, hit := range result.Hits {
				// This needs to match embeddingsindex.go
				filterIDs = append(filterIDs, xxhash.Sum64String(hit.ID))
				filterDocKeys = append(filterDocKeys, hit.ID)
			}
			// If filter query was provided but matched no documents, skip vector search
			// and return empty results for each index
			if len(filterIDs) == 0 {
				s.logger.Debug(
					"Filter query matched no documents, skipping vector search",
					zap.String("filterQuery", string(searchRequest.FilterQuery)),
				)
				for indexToSearch := range searchRequest.VectorSearches {
					res.VectorSearchResult[indexToSearch] = &vectorindex.SearchResult{
						Hits:   nil,
						Status: &vectorindex.SearchStatus{Total: 0, Successful: 0, Failed: 0},
					}
				}
				for indexToSearch := range searchRequest.SparseSearches {
					res.VectorSearchResult[indexToSearch] = &vectorindex.SearchResult{
						Hits:   nil,
						Status: &vectorindex.SearchStatus{Total: 0, Successful: 0, Failed: 0},
					}
				}
				skipVectorSearch = true
			}
		}
		// Sparse vector searches: use pre-computed sparse embeddings from metadata level
		if !skipVectorSearch {
			for indexToSearch, sparseVec := range searchRequest.SparseSearches {
				sparseReq := &indexes.SparseSearchRequest{
					QueryVec:     sparseVec.ToVector(),
					K:            searchRequest.Limit,
					FilterPrefix: string(searchRequest.FilterPrefix),
				}
				resp, err := s.routeSearch(ctx, indexToSearch, sparseReq, searchRequest.FilterPrefix)
				if err != nil {
					return nil, fmt.Errorf("searching sparse index: %w", err)
				}
				result, ok := resp.(*vectorindex.SearchResult)
				if !ok {
					return nil, fmt.Errorf("unexpected response type from sparse search: %T", resp)
				}
				s.logger.Debug(
					"Searched sparse index with pre-computed sparse vector",
					zap.String("index", indexToSearch),
					zap.Int("total", len(result.Hits)),
				)
				res.VectorSearchResult[indexToSearch] = result
			}
		}
		// Dense vector searches
		if !skipVectorSearch {
			for indexToSearch, vectorReq := range searchRequest.VectorSearches {
				idx := s.indexManager.GetIndex(indexToSearch)

				effectiveFilterIDs := filterIDs
				if len(filterIDs) > 0 {
					if idx != nil {
						if embIdx, ok := idx.(*indexes.EmbeddingIndex); ok && embIdx.IsChunked() {
							expanded, err := s.expandFilterIDsForChunks(filterDocKeys, indexToSearch)
							if err != nil {
								return nil, fmt.Errorf("expanding filter IDs for chunks: %w", err)
							}
							effectiveFilterIDs = expanded
						}
					}
				}
				req := &vectorindex.SearchRequest{
					FilterPrefix:  searchRequest.FilterPrefix,
					K:             searchRequest.Limit,
					DistanceOver:  pagingOpts.DistanceOver,
					DistanceUnder: pagingOpts.DistanceUnder,
					SearchEffort:  pagingOpts.SearchEffort,
					FilterIDs:     effectiveFilterIDs,
					Embedding:     vectorReq,
				}
				resp, err := s.routeSearch(ctx, indexToSearch, req, searchRequest.FilterPrefix)
				if err != nil {
					return nil, fmt.Errorf("searching vectorindex: %w", err)
				}
				result, ok := resp.(*vectorindex.SearchResult)
				if !ok {
					return nil, fmt.Errorf("unexpected response type from vector search: %T", resp)
				}
				result.Request = req
				s.logger.Debug(
					"Searched vectorindex with query",
					zap.String("index", indexToSearch),
					zap.Int("total", len(result.Hits)),
					zap.Any("status", result.Status),
				)
				res.VectorSearchResult[indexToSearch] = result
			}
		}
	}
	if searchRequest.MergeConfig != nil {
		var fusedResults *indexes.FusionResult

		mc := searchRequest.MergeConfig
		// Choose fusion strategy based on merge config
		if mc.Strategy != nil && *mc.Strategy == indexes.MergeStrategyRsf {
			// Use Relative Score Fusion
			windowSize := mc.WindowSize
			if windowSize <= 0 {
				windowSize = searchRequest.Limit
			}

			var weights map[string]float64
			if mc.Weights != nil {
				weights = *mc.Weights
			}

			fusedResults = res.RSFResults(searchRequest.Limit, windowSize, weights)
		} else {
			// Use Reciprocal Rank Fusion (default)
			rankConstant := 60.0
			if mc.RankConstant != 0 {
				rankConstant = mc.RankConstant
			}
			var weights map[string]float64
			if mc.Weights != nil {
				weights = *mc.Weights
			}
			fusedResults = res.RRFResults(searchRequest.Limit, rankConstant, weights)
		}

		if len(searchRequest.AggregationRequests) > 0 {
			ids := make([]string, 0, len(fusedResults.Hits))
			for _, hit := range fusedResults.Hits {
				ids = append(ids, hit.ID)
			}
			q := query.NewDocIDQuery(ids)
			bleveReq := bleve.NewSearchRequest(q)
			bleveReq.Aggregations = convertToBleveAggregations(searchRequest.AggregationRequests)
			resp, err := s.routeSearch(ctx, fullTextIndexName, bleveReq, searchRequest.FilterPrefix)
			if err != nil {
				return nil, fmt.Errorf("searching bleve: %w", err)
			}
			result, ok := resp.(*bleve.SearchResult)
			if !ok {
				return nil, fmt.Errorf("unexpected response type from bleve search: %T", resp)
			}
			res.AggregationResults = result.Aggregations
		}
		fusedResults.Took = time.Since(searchStart)
		res.FusionResult = fusedResults
		res.Status = &indexes.RemoteIndexSearchStatus{
			Total:      1,
			Failed:     0,
			Successful: 1,
		}
		res.BleveSearchResult = nil  // Clear out the Bleve results to save space
		res.VectorSearchResult = nil // Clear out the Vector results to save space
	} else if len(searchRequest.VectorSearches) > 0 && len(searchRequest.AggregationRequests) > 0 {
		// Handle aggregations for vector-only searches (no fusion)
		s.logger.Debug("Computing aggregations for vector-only search",
			zap.Int("numVectorSearches", len(searchRequest.VectorSearches)),
			zap.Int("numAggregations", len(searchRequest.AggregationRequests)))

		// Collect document IDs from all vector search results
		var ids []string
		for _, vectorResult := range res.VectorSearchResult {
			for _, hit := range vectorResult.Hits {
				ids = append(ids, hit.ID)
			}
		}
		s.logger.Debug("Collected document IDs from vector results",
			zap.Int("totalIDs", len(ids)))

		// Deduplicate and limit to searchRequest.Limit if specified
		seen := make(map[string]bool)
		uniqueIDs := make([]string, 0, len(ids))
		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				uniqueIDs = append(uniqueIDs, id)
				if searchRequest.Limit > 0 && len(uniqueIDs) >= searchRequest.Limit {
					break
				}
			}
		}

		if len(uniqueIDs) > 0 {
			// Create DocIDQuery with the matched document IDs
			q := query.NewDocIDQuery(uniqueIDs)
			bleveReq := bleve.NewSearchRequest(q)
			bleveReq.Aggregations = convertToBleveAggregations(searchRequest.AggregationRequests)

			s.logger.Debug("Executing DocIDQuery for aggregations",
				zap.Strings("docIDs", uniqueIDs),
				zap.Int("numAggregations", len(searchRequest.AggregationRequests)))

			// Execute aggregation query
			resp, err := s.routeSearch(ctx, fullTextIndexName, bleveReq, searchRequest.FilterPrefix)
			if err != nil {
				s.logger.Warn("Failed to compute aggregations for vector search",
					zap.Error(err),
					zap.Int("numDocIDs", len(uniqueIDs)))
			} else {
				result, ok := resp.(*bleve.SearchResult)
				if ok {
					s.logger.Debug("Aggregation query succeeded",
						zap.Int("numAggregations", len(result.Aggregations)),
						zap.Uint64("totalHits", result.Total))
					res.AggregationResults = result.Aggregations
				} else {
					s.logger.Warn("Unexpected response type from bleve aggregation search",
						zap.String("type", fmt.Sprintf("%T", resp)))
				}
			}
		} else {
			s.logger.Debug("No unique document IDs for aggregations")
		}
	}

	// Execute graph queries if present
	if len(searchRequest.GraphSearches) > 0 {
		if res.Status == nil {
			res.Status = &indexes.RemoteIndexSearchStatus{}
		}
		if res.Status.GraphStatus == nil {
			res.Status.GraphStatus = make(map[string]*indexes.SearchComponentStatus)
		}

		graphEngine := NewGraphQueryEngine(s, s.logger)
		res.GraphResults = make(map[string]*indexes.GraphQueryResult)

		// Sort queries by dependencies to ensure chained queries work correctly
		sortedQueryNames, err := SortGraphQueriesByDependencies(searchRequest.GraphSearches)
		if err != nil {
			s.logger.Error("Failed to sort graph queries by dependencies", zap.Error(err))
			// Mark all queries as failed
			for queryName := range searchRequest.GraphSearches {
				res.Status.GraphStatus[queryName] = &indexes.SearchComponentStatus{
					Success: false,
					Error:   fmt.Sprintf("dependency resolution failed: %v", err),
				}
			}
		} else {
			// Execute queries in dependency order
			for _, queryName := range sortedQueryNames {
				graphQuery := searchRequest.GraphSearches[queryName]

				// Resolve start nodes from selectors
				startNodes, err := graphEngine.resolveNodeSelector(
					ctx,
					&graphQuery.StartNodes,
					&res,
				)
				if err != nil {
					s.logger.Warn("Failed to resolve start nodes",
						zap.String("query", queryName),
						zap.Error(err))
					res.Status.GraphStatus[queryName] = &indexes.SearchComponentStatus{
						Success: false,
						Error:   fmt.Sprintf("failed to resolve start nodes: %v", err),
					}
					continue
				}

				// Execute graph query
				graphResult, status, err := graphEngine.Execute(ctx, graphQuery, startNodes)
				res.Status.GraphStatus[queryName] = status

				if err != nil {
					s.logger.Warn("Failed to execute graph query",
						zap.String("query", queryName),
						zap.Error(err))
					continue
				}

				res.GraphResults[queryName] = graphResult
			}
		}

		// Apply graph expansion strategy if specified
		if searchRequest.ExpandStrategy != "" {
			if err := applyGraphFusion(&res, searchRequest.ExpandStrategy, s.logger); err != nil {
				s.logger.Warn("Failed to apply graph expansion",
					zap.String("mode", searchRequest.ExpandStrategy),
					zap.Error(err))
			}
		}

		// Compute aggregations on graph results if requested
		if len(searchRequest.AggregationRequests) > 0 && res.AggregationResults == nil {
			var graphDocIDs []string
			for _, graphResult := range res.GraphResults {
				for _, node := range graphResult.Nodes {
					graphDocIDs = append(graphDocIDs, decodeGraphNodeKey(node.Key))
				}
			}
			if len(graphDocIDs) > 0 {
				seen := make(map[string]bool)
				uniqueIDs := make([]string, 0, len(graphDocIDs))
				for _, id := range graphDocIDs {
					if !seen[id] {
						seen[id] = true
						uniqueIDs = append(uniqueIDs, id)
					}
				}
				q := query.NewDocIDQuery(uniqueIDs)
				bleveReq := bleve.NewSearchRequest(q)
				bleveReq.Aggregations = convertToBleveAggregations(searchRequest.AggregationRequests)
				resp, err := s.routeSearch(ctx, fullTextIndexName, bleveReq, searchRequest.FilterPrefix)
				if err != nil {
					s.logger.Warn("Failed to compute aggregations for graph search", zap.Error(err))
				} else if result, ok := resp.(*bleve.SearchResult); ok {
					res.AggregationResults = result.Aggregations
				}
			}
		}
	}

	s.lookupFields(ctx, searchRequest, &res)
	resp, err = json.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("marshalling search result: %w", err)
	}
	return resp, nil
}

// expandFilterIDsForChunks translates document-level filter IDs into the
// chunk-level member IDs used by a chunked embedding index. It scans Pebble
// for each doc's chunk embedding keys and hashes the underlying chunk keys.
func (s *DBImpl) expandFilterIDsForChunks(docKeys []string, indexName string) ([]uint64, error) {
	embedderSuffix := fmt.Appendf(nil, ":i:%s:e", indexName)

	expanded := make([]uint64, 0, len(docKeys)*2)
	for _, docKey := range docKeys {
		prefix := storeutils.MakeChunkPrefix([]byte(docKey), indexName)
		pdb := s.getPDB()
		if pdb == nil {
			return nil, pebble.ErrClosed
		}
		iter, err := pdb.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: utils.PrefixSuccessor(prefix),
		})
		if err != nil {
			return nil, fmt.Errorf("creating chunk expansion iterator: %w", err)
		}
		found := false
		for iter.First(); iter.Valid(); iter.Next() {
			key := iter.Key()
			// Strip embedder suffix to recover the chunk key (the vector member key).
			if bytes.HasSuffix(key, embedderSuffix) {
				chunkKey := key[:len(key)-len(embedderSuffix)]
				if storeutils.IsChunkKey(chunkKey) {
					expanded = append(expanded, xxhash.Sum64(chunkKey))
					found = true
				}
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("closing chunk expansion iterator: %w", err)
		}
		if !found {
			// No chunks — include the doc-level hash as fallback.
			expanded = append(expanded, xxhash.Sum64String(docKey))
		}
	}
	return expanded, nil
}

// convertToBleveAggregations converts internal AggregationRequests to bleve's AggregationsRequest
func convertToBleveAggregations(reqs indexes.AggregationRequests) bleve.AggregationsRequest {
	if len(reqs) == 0 {
		return nil
	}
	result := make(bleve.AggregationsRequest)
	for name, req := range reqs {
		agg := &bleve.AggregationRequest{
			Type:  req.Type,
			Field: req.Field,
		}
		if req.Size > 0 {
			agg.Size = &req.Size
		}
		if req.TermPrefix != "" {
			agg.TermPrefix = req.TermPrefix
		}
		if req.TermPattern != "" {
			agg.TermPattern = req.TermPattern
		}
		if req.Interval > 0 {
			agg.Interval = &req.Interval
		}
		if req.CalendarInterval != "" {
			agg.CalendarInterval = req.CalendarInterval
		}
		if req.FixedInterval != "" {
			agg.FixedInterval = req.FixedInterval
		}
		if req.MinDocCount > 0 {
			agg.MinDocCount = &req.MinDocCount
		}
		if req.GeohashPrecision > 0 {
			agg.GeoHashPrecision = &req.GeohashPrecision
		}
		if req.CenterLat != 0 || req.CenterLon != 0 {
			agg.CenterLat = &req.CenterLat
			agg.CenterLon = &req.CenterLon
		}
		if req.DistanceUnit != "" {
			agg.DistanceUnit = req.DistanceUnit
		}
		if req.SignificanceAlgorithm != "" {
			agg.SignificanceAlgorithm = req.SignificanceAlgorithm
		}
		// Convert numeric ranges
		for _, nr := range req.NumericRanges {
			agg.AddNumericRange(nr.Name, nr.Start, nr.End)
		}
		// Convert datetime ranges
		for _, dr := range req.DateTimeRanges {
			agg.AddDateTimeRangeString(dr.Name, dr.Start, dr.End)
		}
		// Convert distance ranges
		for _, distRange := range req.DistanceRanges {
			agg.AddDistanceRange(distRange.Name, distRange.From, distRange.To)
		}
		// Recursively convert sub-aggregations
		if len(req.Aggregations) > 0 {
			agg.Aggregations = convertToBleveAggregations(req.Aggregations)
		}
		result[name] = agg
	}
	return result
}

// decodeGraphNodeKey decodes a base64-encoded graph node key to a raw string key.
// Graph nodes store keys as base64-encoded strings, but document lookups and
// search hit IDs use raw string keys.
func decodeGraphNodeKey(encodedKey string) string {
	decoded, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return encodedKey
	}
	return string(decoded)
}

// applyGraphFusion merges or filters search and graph results based on fusion strategy
func applyGraphFusion(res *indexes.RemoteIndexSearchResult, fusionMode string, logger *zap.Logger) error {
	appendVectorHits := func(dst map[string]*indexes.FusionHit) {
		if len(res.VectorSearchResult) == 0 {
			return
		}
		for _, vectorRes := range res.VectorSearchResult {
			if vectorRes == nil {
				continue
			}
			for _, hit := range vectorRes.Hits {
				if hit == nil {
					continue
				}
				score := float64(hit.Score)
				if score == 0 {
					score = 1.0 / (1.0 + float64(hit.Distance))
				}
				if existing, ok := dst[hit.ID]; ok {
					if existing.Fields == nil && hit.Fields != nil {
						existing.Fields = hit.Fields
					}
					if score > existing.Score {
						existing.Score = score
					}
					continue
				}
				dst[hit.ID] = &indexes.FusionHit{
					ID:     hit.ID,
					Score:  score,
					Fields: hit.Fields,
				}
			}
		}
	}

	switch fusionMode {
	case "union":
		// Collect all keys from search results (use RRF results if available, else Bleve)
		searchKeys := make(map[string]*indexes.FusionHit)

		if res.FusionResult != nil && len(res.FusionResult.Hits) > 0 {
			// Use RRF/RSF fused results
			for _, hit := range res.FusionResult.Hits {
				searchKeys[hit.ID] = hit
			}
		} else if res.BleveSearchResult != nil && len(res.BleveSearchResult.Hits) > 0 {
			// Use Bleve results
			for _, hit := range res.BleveSearchResult.Hits {
				searchKeys[hit.ID] = &indexes.FusionHit{
					ID:     hit.ID,
					Score:  hit.Score,
					Fields: hit.Fields,
				}
			}
		} else {
			// Vector-only search paths can still define an authoritative result set
			// for graph expansion even without a precomputed fusion result.
			appendVectorHits(searchKeys)
		}

		// Add graph nodes that aren't already in search results
		for _, graphResult := range res.GraphResults {
			for _, node := range graphResult.Nodes {
				nodeID := decodeGraphNodeKey(node.Key)
				if _, exists := searchKeys[nodeID]; !exists {
					// Convert graph node to search hit with synthetic score
					// Score based on graph distance: closer nodes get higher scores
					syntheticScore := 1.0 / (1.0 + node.Distance)

					hit := &indexes.FusionHit{
						ID:     nodeID,
						Score:  syntheticScore,
						Fields: node.Document,
					}

					searchKeys[nodeID] = hit
				}
			}
		}

		// Rebuild as a unified FusionResult (union always promotes to FusionResult
		// since it merges results from different sources: search + graph)
		if res.FusionResult == nil {
			res.FusionResult = &indexes.FusionResult{}
		}
		res.FusionResult.Hits = make([]*indexes.FusionHit, 0, len(searchKeys))
		for _, hit := range searchKeys {
			res.FusionResult.Hits = append(res.FusionResult.Hits, hit)
		}
		res.FusionResult.Total = uint64(len(res.FusionResult.Hits))
		res.FusionResult.FinalizeSort()

	case "intersection":
		// Only keep search hits that also appear in graph results
		graphKeys := make(map[string]bool)
		for _, graphResult := range res.GraphResults {
			for _, node := range graphResult.Nodes {
				graphKeys[decodeGraphNodeKey(node.Key)] = true
			}
		}

		if res.FusionResult != nil {
			filteredHits := make([]*indexes.FusionHit, 0)
			for _, hit := range res.FusionResult.Hits {
				if graphKeys[hit.ID] {
					filteredHits = append(filteredHits, hit)
				}
			}
			res.FusionResult.Hits = filteredHits
			res.FusionResult.Total = uint64(len(filteredHits))
		} else if res.BleveSearchResult != nil {
			// Promote to FusionResult since we're filtering across sources
			filteredHits := make([]*indexes.FusionHit, 0)
			for _, hit := range res.BleveSearchResult.Hits {
				if graphKeys[hit.ID] {
					filteredHits = append(filteredHits, &indexes.FusionHit{
						ID:     hit.ID,
						Score:  hit.Score,
						Fields: hit.Fields,
					})
				}
			}
			res.FusionResult = &indexes.FusionResult{
				Hits:  filteredHits,
				Total: uint64(len(filteredHits)),
			}
		} else if len(res.VectorSearchResult) > 0 {
			baseHits := make(map[string]*indexes.FusionHit)
			appendVectorHits(baseHits)
			filteredHits := make([]*indexes.FusionHit, 0)
			for _, hit := range baseHits {
				if graphKeys[hit.ID] {
					filteredHits = append(filteredHits, hit)
				}
			}
			res.FusionResult = &indexes.FusionResult{
				Hits:  filteredHits,
				Total: uint64(len(filteredHits)),
			}
		}

	case "":
		// No fusion - keep results separate (default behavior)
		return nil

	default:
		return fmt.Errorf("unknown graph fusion mode: %s (supported: union, intersection)", fusionMode)
	}

	return nil
}

// Stats returns statistics for the Bleve index and Pebble database.
func (s *DBImpl) Stats() (diskSize uint64, empty bool, indexStats map[string]indexes.IndexStats, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	empty, err = pebbleutils.IsPebbleEmpty(s.pdb, MetadataPrefix)
	if err != nil {
		if !errors.Is(err, pebble.ErrClosed) {
			s.logger.Warn("Failed to check if Pebble database is empty", zap.Error(err))
		}
		err = nil
		// Assume not empty or handle error appropriately
		empty = false
	}

	if pdb := s.getPDB(); pdb != nil {
		if storageMets := pdb.Metrics(); storageMets != nil {
			diskSize = storageMets.DiskSpaceUsage()
		}
	}
	indexStats = s.indexManager.Stats()
	return
}

// loadMetadata loads all metadata from Pebble
func (db *DBImpl) loadMetadata() error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	// Create iterator for metadata prefix
	iterOpts := &pebble.IterOptions{
		LowerBound: MetadataPrefix,
		UpperBound: utils.PrefixSuccessor(MetadataPrefix),
	}

	iter, err := pdb.NewIterWithContext(context.Background(), iterOpts)
	if err != nil {
		return fmt.Errorf("creating metadata iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	// Initialize defaults only if not already set
	if db.indexes == nil {
		db.indexes = make(map[string]indexes.IndexConfig)
	}
	var version int

	// Iterate through all metadata keys
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		switch {
		case bytes.Equal(key, versionKey):
			if err := json.Unmarshal(value, &version); err != nil {
				return fmt.Errorf("unmarshaling version: %w", err)
			}
			// Handle version-specific logic here if needed
			if version > CurrentStorageVersion {
				return fmt.Errorf(
					"unsupported storage version %d, current version is %d",
					version,
					CurrentStorageVersion,
				)
			}

		case bytes.Equal(key, schemaKey):
			var schema schema.TableSchema
			if err := json.Unmarshal(value, &schema); err != nil {
				return fmt.Errorf("unmarshaling schema: %w", err)
			}
			db.schema = &schema
			db.updateCachedTTL()

		case bytes.Equal(key, byteRangeKey):
			var byteRange types.Range
			if err := json.Unmarshal(value, &byteRange); err != nil {
				return fmt.Errorf("unmarshaling byte range: %w", err)
			}
			db.byteRange = byteRange

		case bytes.Equal(key, splitStateKey):
			splitState := &SplitState{}
			if err := proto.Unmarshal(value, splitState); err != nil {
				return fmt.Errorf("unmarshaling split state: %w", err)
			}
			db.splitState = splitState

		case bytes.Equal(key, splitDeltaSeqKey):
			seq, err := decodeStoredUint64(value)
			if err != nil {
				return fmt.Errorf("decoding split delta seq: %w", err)
			}
			db.splitDeltaSeq = seq

		case bytes.Equal(key, splitDeltaFinalSeqKey):
			seq, err := decodeStoredUint64(value)
			if err != nil {
				return fmt.Errorf("decoding split delta final seq: %w", err)
			}
			db.splitDeltaFinalSeq = seq

		case bytes.Equal(key, indexesKey):
			var indexes map[string]indexes.IndexConfig
			if err := json.Unmarshal(value, &indexes); err != nil {
				return fmt.Errorf("unmarshaling indexes: %w", err)
			}
			// Only overwrite indexes if we loaded something from disk
			if len(indexes) > 0 {
				db.indexes = indexes
			}
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating metadata: %w", err)
	}

	return nil
}

// saveMetadata saves all metadata to Pebble
func (db *DBImpl) saveMetadata() error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	batch := pdb.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	// Save version
	versionData, err := json.Marshal(CurrentStorageVersion)
	if err != nil {
		return fmt.Errorf("marshaling version: %w", err)
	}
	if err := batch.Set(versionKey, versionData, nil); err != nil {
		return fmt.Errorf("saving version: %w", err)
	}

	if db.schema != nil {
		data, err := json.Marshal(db.schema)
		if err != nil {
			return fmt.Errorf("marshaling schema: %w", err)
		}
		if err := batch.Set(schemaKey, data, nil); err != nil {
			return fmt.Errorf("saving schema: %w", err)
		}
	}
	data, err := json.Marshal(db.byteRange)
	if err != nil {
		return fmt.Errorf("marshaling byte range: %w", err)
	}
	if err := batch.Set(byteRangeKey, data, nil); err != nil {
		return fmt.Errorf("saving byte range: %w", err)
	}
	db.indexesMu.RLock()
	defer db.indexesMu.RUnlock()

	if data, err := json.Marshal(db.indexes); err != nil {
		return fmt.Errorf("marshaling indexes: %w", err)
	} else if err := batch.Set(indexesKey, data, nil); err != nil {
		return fmt.Errorf("saving indexes: %w", err)
	}
	return batch.Commit(pebble.Sync)
}

// saveSchema saves the schema to Pebble
func (db *DBImpl) saveSchema() error {
	if db.schema == nil {
		return nil
	}
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	data, err := json.Marshal(db.schema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}
	if err := pdb.Set(schemaKey, data, pebble.Sync); err != nil {
		return fmt.Errorf("saving schema: %w", err)
	}
	return nil
}

// saveIndexes saves the indexes configuration to Pebble
func (db *DBImpl) saveIndexes() error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	data, err := json.Marshal(db.indexes)
	if err != nil {
		return fmt.Errorf("marshaling indexes: %w", err)
	}
	if err := pdb.Set(indexesKey, data, pebble.Sync); err != nil {
		return fmt.Errorf("saving indexes: %w", err)
	}
	return nil
}

// ScanOptions configures the Scan operation
type ScanOptions struct {
	// InclusiveFrom makes the lower bound inclusive instead of exclusive
	// Default: false (exclusive lower bound for pagination: (fromKey, toKey])
	InclusiveFrom bool
	// ExclusiveTo makes the upper bound exclusive instead of inclusive
	// Default: false (inclusive upper bound)
	ExclusiveTo bool
	// IncludeDocuments makes Scan return the actual documents in addition to hashes
	// Default: false (only return hashes)
	IncludeDocuments bool
	// FilterQuery is a Bleve query (as JSON) to filter documents. Only documents
	// matching this query are included in results. If nil, no filtering is applied.
	FilterQuery json.RawMessage
	// Limit is the maximum number of results to return. If 0, no limit is applied.
	Limit int
}

// ScanResult contains the results of a Scan operation
type ScanResult struct {
	// Hashes maps document IDs to their content hashes
	Hashes map[string]uint64
	// Documents maps document IDs to their full content (only populated if IncludeDocuments is true)
	Documents map[string]map[string]any
}

// Scan scans all document keys and their content hashes within a key range.
// Default range semantics: (fromKey, toKey] (exclusive lower, inclusive upper)
// Use opts to customize boundary inclusivity and whether to include full documents.
// Returns ScanResult containing hashes and optionally full documents.
func (db *DBImpl) Scan(
	ctx context.Context,
	fromKey []byte,
	toKey []byte,
	opts ScanOptions,
) (*ScanResult, error) {
	result := &ScanResult{
		Hashes: make(map[string]uint64),
	}
	if opts.IncludeDocuments {
		result.Documents = make(map[string]map[string]any)
	}

	// Parse filter query if provided
	var filterQuery query.Query
	var searIndex bleve.Index
	hasFilterQuery := len(opts.FilterQuery) > 0 && !bytes.Equal(opts.FilterQuery, []byte("null"))
	if hasFilterQuery {
		var err error
		filterQuery, err = query.ParseQuery(opts.FilterQuery)
		if err != nil {
			return nil, fmt.Errorf("parsing filter_query: %w", err)
		}
		// Create a sear index for efficient per-document matching
		mapping := bleve.NewIndexMapping()
		mapping.StoreDynamic = false
		mapping.DocValuesDynamic = false
		searIndex, err = bleve.NewUsing("", mapping, sear.Name, "", nil)
		if err != nil {
			return nil, fmt.Errorf("creating sear index: %w", err)
		}
		defer func() { _ = searIndex.Close() }()
	}

	// Determine lower bound based on inclusivity
	var lowerBound []byte
	if opts.InclusiveFrom {
		lowerBound = storeutils.KeyRangeStart(fromKey) // Append :\x00 to include fromKey
	} else {
		lowerBound = storeutils.KeyRangeEnd(fromKey) // Append :\xFF to exclude fromKey
	}

	// Determine upper bound based on inclusivity
	// Default is inclusive upper bound
	var upperBound []byte
	if opts.ExclusiveTo {
		upperBound = storeutils.KeyRangeStart(toKey) // Append :\x00 to exclude toKey
	} else {
		upperBound = storeutils.KeyRangeEnd(toKey) // Append :\xFF to include toKey (default)
	}

	// Track result count for limit
	resultCount := 0

	// Use storeutils.Scan with appropriate options
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}
	err := storeutils.Scan(ctx, pdb, storeutils.ScanOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
		SkipPoint: func(userKey []byte) bool {
			// Skip metadata keys
			if bytes.HasPrefix(userKey, storeutils.MetadataPrefix) {
				return true
			}
			// Only include main document keys (those ending with DBRangeStart = :\x00)
			// Skip enrichment suffixes (:e, :s) and other suffixes
			return !bytes.HasSuffix(userKey, storeutils.DBRangeStart)
		},
	}, func(key []byte, value []byte) (bool, error) {
		// Check if we've reached the limit
		if opts.Limit > 0 && resultCount >= opts.Limit {
			return false, nil // Stop scanning
		}

		// Check context on each key for responsive cancellation
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		// Extract user-facing ID (remove :\x00 suffix)
		userID := string(key[:len(key)-len(storeutils.DBRangeStart)])

		// Check if document is expired (TTL filtering)
		expired, err := db.isDocumentExpiredByTimestamp(ctx, []byte(userID))
		if err != nil {
			db.logger.Warn("Failed to check TTL expiration for document",
				zap.String("id", userID), zap.Error(err))
			// Continue to next document on error
			return true, nil
		}
		if expired {
			// Skip expired documents
			return true, nil
		}

		// Decompress and decode the document
		doc, err := storeutils.DecodeDocumentJSON(value)
		if err != nil {
			db.logger.Warn("Failed to decompress/decode document",
				zap.String("id", userID), zap.Error(err))
			result.Hashes[userID] = 0
			return true, nil
		}

		// Apply filter query if specified
		if hasFilterQuery {
			// Index the document in sear (overwrites previous document)
			if err := searIndex.Index(userID, doc); err != nil {
				db.logger.Warn("Failed to index document for filtering",
					zap.String("id", userID), zap.Error(err))
				return true, nil // Skip this document
			}

			// Check if document matches the filter query
			searchReq := bleve.NewSearchRequest(filterQuery)
			searchReq.Size = 1
			searchResult, err := searIndex.Search(searchReq)
			if err != nil {
				db.logger.Warn("Failed to search document for filtering",
					zap.String("id", userID), zap.Error(err))
				return true, nil // Skip this document
			}

			if searchResult.Total == 0 {
				// Document doesn't match filter, skip it
				return true, nil
			}
		}

		// Compute hash
		hash, err := ComputeDocumentHash(doc)
		if err != nil {
			db.logger.Warn("Failed to compute hash for document, will treat as changed",
				zap.String("id", userID), zap.Error(err))
			result.Hashes[userID] = 0
		} else {
			result.Hashes[userID] = hash
		}

		// Include document if requested
		if opts.IncludeDocuments {
			result.Documents[userID] = doc
		}

		resultCount++
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("scanning keys in range: %w", err)
	}

	return result, nil
}

// ComputeDocumentHash computes a deterministic hash of a document's content.
// Uses sonic with SortMapKeys for canonical JSON, then xxhash for fast hashing.
func ComputeDocumentHash(doc map[string]any) (uint64, error) {
	// Create a copy of the document excluding system-managed metadata fields
	// that shouldn't affect content comparison (like _timestamp)
	docCopy := make(map[string]any, len(doc))
	for k, v := range doc {
		// Exclude _timestamp from hash computation
		// This allows LinearMerge to correctly detect unchanged documents
		// even when _timestamp is added/updated by the system
		if k == "_timestamp" {
			continue
		}
		docCopy[k] = v
	}

	// Use JSON encoding with sorted keys (default behavior) for deterministic output
	canonical, err := json.Marshal(docCopy)
	if err != nil {
		return 0, fmt.Errorf("encoding document for hash: %w", err)
	}

	// Use xxhash for fast hashing (already used in codebase)
	return xxhash.Sum64(canonical), nil
}

// computeDocumentHashFromCompressed decompresses a document from Pebble storage
// and computes its content hash.
func computeDocumentHashFromCompressed(compressedValue []byte) (uint64, error) {
	doc, err := storeutils.DecodeDocumentJSON(compressedValue)
	if err != nil {
		return 0, fmt.Errorf("decoding compressed document: %w", err)
	}
	return ComputeDocumentHash(doc)
}

// Transaction helper functions

func makeTxnKey(txnID []byte) []byte {
	return slices.Concat(txnRecordsPrefix, txnID)
}

func makeIntentKey(txnID []byte, userKey []byte) []byte {
	return slices.Concat(txnIntentsPrefix, txnID, []byte{':'}, userKey)
}

func parseIntentKey(intentKey []byte) (txnID []byte, userKey []byte) {
	// Format: "\x00\x00__txn_intents__:<txnID>:<userKey>"
	if !bytes.HasPrefix(intentKey, txnIntentsPrefix) {
		return nil, nil
	}
	remaining := intentKey[len(txnIntentsPrefix):]

	// UUID is always 16 bytes, followed by a colon separator
	const uuidLen = 16
	if len(remaining) < uuidLen+1 { // 16 bytes UUID + at least 1 byte for ':'
		return nil, nil
	}

	txnID = remaining[:uuidLen]
	// Verify the colon separator at position 16
	if remaining[uuidLen] != ':' {
		return nil, nil
	}
	userKey = remaining[uuidLen+1:]
	return txnID, userKey
}

// Transaction operations

// InitTransaction creates transaction record on coordinator
func (db *DBImpl) InitTransaction(ctx context.Context, op *InitTransactionOp) error {
	start := time.Now()
	defer func() {
		transactionDurationSeconds.WithLabelValues("init").Observe(time.Since(start).Seconds())
	}()

	pdb := db.getPDB()

	if pdb == nil {
		return pebble.ErrClosed
	}

	activeTransactionsGauge.Inc()

	txnKey := makeTxnKey(op.GetTxnId())

	record := TxnRecord{
		TxnID:                op.GetTxnId(),
		Timestamp:            op.GetTimestamp(),
		Status:               TxnStatusPending,
		Participants:         op.GetParticipants(),
		ResolvedParticipants: []string{},
		CreatedAt:            time.Now().Unix(),
	}

	data, err := json.Marshal(record)
	if err != nil {
		transactionOpsTotal.WithLabelValues("init", "error").Inc()
		return fmt.Errorf("marshaling transaction record: %w", err)
	}

	if err := pdb.Set(txnKey, data, pebble.Sync); err != nil {
		transactionOpsTotal.WithLabelValues("init", "error").Inc()
		return fmt.Errorf("writing transaction record: %w", err)
	}

	transactionOpsTotal.WithLabelValues("init", "success").Inc()

	db.logger.Info("Initialized transaction",
		zap.Binary("txnID", op.GetTxnId()),
		zap.Uint64("timestamp", op.GetTimestamp()),
		zap.Int("numParticipants", len(op.GetParticipants())))

	return nil
}

// CommitTransaction updates transaction status to Committed and returns the commit version.
func (db *DBImpl) CommitTransaction(ctx context.Context, op *CommitTransactionOp) (uint64, error) {
	return db.finalizeTransaction(ctx, op.GetTxnId(), TxnStatusCommitted, "commit")
}

// AbortTransaction updates transaction status to Aborted
func (db *DBImpl) AbortTransaction(ctx context.Context, op *AbortTransactionOp) error {
	_, err := db.finalizeTransaction(ctx, op.GetTxnId(), TxnStatusAborted, "abort")
	return err
}

// WriteIntent writes provisional write intent
func (db *DBImpl) WriteIntent(ctx context.Context, op *WriteIntentOp) error {
	start := time.Now()
	defer func() {
		transactionDurationSeconds.WithLabelValues("write_intent").Observe(time.Since(start).Seconds())
	}()

	pdb := db.getPDB()

	if pdb == nil {
		return pebble.ErrClosed
	}

	batch := pdb.NewBatch()
	defer func() { _ = batch.Close() }()

	txnID := op.GetTxnId()
	timestamp := op.GetTimestamp()
	coordinatorShard := op.GetCoordinatorShard()
	batchOp := op.GetBatch()

	if batchOp == nil {
		transactionOpsTotal.WithLabelValues("write_intent", "error").Inc()
		return errors.New("write intent batch operation is nil")
	}

	// Check version predicates before writing intents (OCC validation)
	// Also checks for conflicting intents from other transactions to prevent lost updates
	if predicates := op.GetPredicates(); len(predicates) > 0 {
		if err := db.checkVersionPredicates(predicates, txnID); err != nil {
			transactionOpsTotal.WithLabelValues("write_intent", "predicate_conflict").Inc()
			return fmt.Errorf("version predicate check failed: %w", err)
		}
	}

	// Write each key-value as an intent
	for _, w := range batchOp.GetWrites() {
		intentKey := makeIntentKey(txnID, w.GetKey())

		intent := map[string]any{
			"value":             w.GetValue(),
			"txn_id":            txnID,
			"timestamp":         timestamp,
			"coordinator_shard": coordinatorShard,
			"status":            TxnStatusPending,
			"is_delete":         false,
		}

		intentBytes, err := json.Marshal(intent)
		if err != nil {
			transactionOpsTotal.WithLabelValues("write_intent", "error").Inc()
			return fmt.Errorf("marshaling write intent: %w", err)
		}

		if err := batch.Set(intentKey, intentBytes, nil); err != nil {
			transactionOpsTotal.WithLabelValues("write_intent", "error").Inc()
			return fmt.Errorf("writing intent: %w", err)
		}

		transactionIntentsTotal.WithLabelValues("write").Inc()
	}

	// Write delete intents
	for _, key := range batchOp.GetDeletes() {
		intentKey := makeIntentKey(txnID, key)

		intent := map[string]any{
			"value":             nil,
			"txn_id":            txnID,
			"timestamp":         timestamp,
			"coordinator_shard": coordinatorShard,
			"status":            TxnStatusPending,
			"is_delete":         true,
		}

		intentBytes, err := json.Marshal(intent)
		if err != nil {
			transactionOpsTotal.WithLabelValues("write_intent", "error").Inc()
			return fmt.Errorf("marshaling delete intent: %w", err)
		}

		if err := batch.Set(intentKey, intentBytes, nil); err != nil {
			transactionOpsTotal.WithLabelValues("write_intent", "error").Inc()
			return fmt.Errorf("writing delete intent: %w", err)
		}

		transactionIntentsTotal.WithLabelValues("delete").Inc()
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		transactionOpsTotal.WithLabelValues("write_intent", "error").Inc()
		return fmt.Errorf("committing intent batch: %w", err)
	}

	transactionOpsTotal.WithLabelValues("write_intent", "success").Inc()

	db.logger.Debug("Wrote transaction intents",
		zap.Binary("txnID", txnID),
		zap.Int("numWrites", len(batchOp.GetWrites())),
		zap.Int("numDeletes", len(batchOp.GetDeletes())))

	return nil
}

// ResolveIntents converts intents to actual writes/deletes
func (db *DBImpl) ResolveIntents(ctx context.Context, op *ResolveIntentsOp) error {
	start := time.Now()
	defer func() {
		intentResolutionDurationSeconds.Observe(time.Since(start).Seconds())
	}()
	pdb := db.getPDB()

	if pdb == nil {
		return pebble.ErrClosed
	}

	// Scan for all intents matching this txnID
	prefix := slices.Concat(txnIntentsPrefix, op.GetTxnId())

	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		transactionOpsTotal.WithLabelValues("resolve_intents", "error").Inc()
		return fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	batch := pdb.NewBatch()
	defer func() { _ = batch.Close() }()

	encoder := db.encoderPool.Get().(*zstd.Encoder)
	defer db.encoderPool.Put(encoder)

	// The commit version is passed in the op by the coordinator (via notification or self-resolve).
	commitVersion := op.GetCommitVersion()

	count := 0
	skipped := 0
	for iter.First(); iter.Valid(); iter.Next() {
		var intent map[string]any
		if err := json.Unmarshal(iter.Value(), &intent); err != nil {
			db.logger.Warn("Failed to unmarshal intent",
				zap.ByteString("intentKey", iter.Key()),
				zap.Error(err))
			continue
		}

		_, userKey := parseIntentKey(iter.Key())
		if userKey == nil {
			db.logger.Warn("Failed to parse intent key",
				zap.ByteString("intentKey", iter.Key()))
			continue
		}

		if op.GetStatus() == TxnStatusCommitted {
			// Apply the write/delete
			isDelete, _ := intent["is_delete"].(bool)
			if isDelete {
				endKey := utils.PrefixSuccessor(storeutils.KeyRangeEnd(userKey))
				if err := batch.DeleteRange(userKey, endKey, nil); err != nil {
					transactionOpsTotal.WithLabelValues("resolve_intents", "error").Inc()
					return fmt.Errorf("deleting key: %w", err)
				}
			} else {
				actualKey := storeutils.KeyRangeStart(userKey)

				// Extract value bytes - handle both []byte and string (base64) from JSON
				var valueBytes []byte
				if v := intent["value"]; v != nil {
					switch val := v.(type) {
					case []byte:
						valueBytes = val
					case string:
						// When sonic marshals []byte to JSON, it base64 encodes it
						// So we need to decode it when reading back
						decoded, err := base64.StdEncoding.DecodeString(val)
						if err != nil {
							db.logger.Warn("Failed to decode base64 value in intent, using as-is",
								zap.String("key", types.FormatKey(userKey)),
								zap.Error(err))
							valueBytes = []byte(val)
						} else {
							valueBytes = decoded
						}
					default:
						db.logger.Warn("Unexpected value type in intent",
							zap.String("type", fmt.Sprintf("%T", val)))
					}
				} else {
					db.logger.Warn("Intent value is nil, skipping write",
						zap.String("key", types.FormatKey(userKey)),
						zap.Binary("txnID", op.GetTxnId()))
				}

				if len(valueBytes) > 0 {
					if commitVersion == 0 {
						db.logger.Warn("Commit version is zero, this may indicate a problem",
							zap.String("key", types.FormatKey(userKey)),
							zap.Binary("txnID", op.GetTxnId()))
					}

					// Check if we should write based on timestamp
					shouldWrite, err := db.shouldWriteValue(actualKey, commitVersion)
					if err != nil {
						db.logger.Warn("Error checking existing value timestamp, writing anyway",
							zap.String("key", types.FormatKey(actualKey)),
							zap.Error(err))
						shouldWrite = true
					}

					if shouldWrite {
						db.logger.Debug("Writing committed intent to storage",
							zap.String("key", types.FormatKey(actualKey)),
							zap.Binary("txnID", op.GetTxnId()),
							zap.Int("valueSize", len(valueBytes)))

						// Extract special fields (_embeddings, _summaries, _edges) from the document
						// This is needed because transaction intents bypass the normal Batch() path
						// Use userKey (not actualKey) for edge extraction since edges use the document key without storage suffix
						valueToCompress := valueBytes
						sfResult, err := db.extractAndWriteSpecialFields(batch, valueBytes, userKey, commitVersion)
						if err != nil {
							db.logger.Warn("Failed to extract special fields during intent resolution, writing raw value",
								zap.String("key", types.FormatKey(actualKey)),
								zap.Error(err))
						} else if sfResult != nil {
							valueToCompress = sfResult.CleanedJSON
							db.logger.Debug("Extracted special fields during intent resolution",
								zap.String("key", types.FormatKey(actualKey)),
								zap.Int("indexWrites", len(sfResult.IndexWrites)),
								zap.Int("edgeDeletes", len(sfResult.EdgeDeletes)))
						}

						// Compress and write the document value
						if err := db.compressAndSet(batch, encoder, actualKey, valueToCompress); err != nil {
							transactionOpsTotal.WithLabelValues("resolve_intents", "error").Inc()
							return fmt.Errorf("writing committed intent: %w", err)
						}

						// Write timestamp metadata to separate key (key:t)
						tsMetaKey := slices.Concat(actualKey, storeutils.TransactionSuffix)
						timestampBytes := encoding.EncodeUint64Ascending(nil, commitVersion)
						if err := batch.Set(tsMetaKey, timestampBytes, nil); err != nil {
							transactionOpsTotal.WithLabelValues("resolve_intents", "error").Inc()
							return fmt.Errorf("writing timestamp metadata: %w", err)
						}
					} else {
						skipped++
						db.logger.Debug("Skipped write due to lower timestamp",
							zap.String("key", types.FormatKey(actualKey)),
							zap.Uint64("commitVersion", commitVersion))
					}
				}
			}
		}
		// If Aborted, just remove intent without applying

		// Remove intent
		if err := batch.Delete(iter.Key(), nil); err != nil {
			transactionOpsTotal.WithLabelValues("resolve_intents", "error").Inc()
			return fmt.Errorf("deleting intent: %w", err)
		}

		count++
	}

	if err := iter.Error(); err != nil {
		transactionOpsTotal.WithLabelValues("resolve_intents", "error").Inc()
		return fmt.Errorf("iterating intents: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		transactionOpsTotal.WithLabelValues("resolve_intents", "error").Inc()
		return fmt.Errorf("committing resolution batch: %w", err)
	}

	// Note: batch.Commit(pebble.Sync) already ensures durability by syncing the WAL.
	// An additional Flush() call is unnecessary and can cause panics during shutdown
	// if the database is closed between Commit and Flush.

	transactionOpsTotal.WithLabelValues("resolve_intents", "success").Inc()

	statusStr := "committed"
	if op.GetStatus() == TxnStatusAborted {
		statusStr = "aborted"
	}

	db.logger.Info("Resolved transaction intents",
		zap.Binary("txnID", op.GetTxnId()),
		zap.String("status", statusStr),
		zap.Int("resolved", count),
		zap.Int("skipped", skipped))

	return nil
}

// GetTimestamp returns the HLC timestamp for a key from the :t suffix metadata key.
// Returns 0 if the key has no timestamp (never written via transactions).
func (db *DBImpl) GetTimestamp(key []byte) (uint64, error) {
	pdb := db.getPDB()

	if pdb == nil {
		return 0, pebble.ErrClosed
	}

	normalizedKey := storeutils.KeyRangeStart(key)
	txnKey := append(normalizedKey, storeutils.TransactionSuffix...)

	existingBytes, closer, err := pdb.Get(txnKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("reading timestamp metadata: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(existingBytes) < 8 {
		return 0, nil
	}

	_, ts, err := encoding.DecodeUint64Ascending(existingBytes[0:8])
	if err != nil {
		return 0, fmt.Errorf("decoding timestamp: %w", err)
	}

	return ts, nil
}

// ErrVersionConflict is returned when a version predicate check fails during WriteIntent.
type ErrVersionConflict struct {
	Key      []byte
	Expected uint64
	Actual   uint64
}

func (e *ErrVersionConflict) Error() string {
	return fmt.Sprintf("version conflict on key %x: expected %d, got %d", e.Key, e.Expected, e.Actual)
}

// ErrIntentConflict is returned when another transaction has a pending intent on the same key.
// This ensures OCC serializability - two transactions reading the same version cannot both commit.
type ErrIntentConflict struct {
	Key           []byte
	ConflictTxnID []byte
}

func (e *ErrIntentConflict) Error() string {
	return fmt.Sprintf("intent conflict on key %x: another transaction %x has pending intent", e.Key, e.ConflictTxnID)
}

// checkVersionPredicates validates that all version predicates still hold.
// Each predicate asserts a key's timestamp matches the expected version.
// expected_version == 0 means "key must not exist" (no :t entry).
// excludeTxnID is the current transaction's ID - its own intents are not considered conflicts.
func (db *DBImpl) checkVersionPredicates(predicates []*VersionPredicate, excludeTxnID []byte) error {
	// Build a map of conflicting intents in a single scan instead of scanning
	// all intents once per predicate (O(M + N) instead of O(N × M)).
	conflictingIntents, err := db.buildConflictingIntentsMap(excludeTxnID)
	if err != nil {
		return fmt.Errorf("scanning for conflicting intents: %w", err)
	}

	for _, p := range predicates {
		// Check committed version
		actual, err := db.GetTimestamp(p.GetKey())
		if err != nil {
			return fmt.Errorf("checking version predicate for key %x: %w", p.GetKey(), err)
		}
		if actual != p.GetExpectedVersion() {
			return &ErrVersionConflict{
				Key:      p.GetKey(),
				Expected: p.GetExpectedVersion(),
				Actual:   actual,
			}
		}

		// Check for conflicting intents from other transactions
		// This prevents lost updates when two transactions read the same version
		if conflictTxnID, ok := conflictingIntents[string(p.GetKey())]; ok {
			return &ErrIntentConflict{
				Key:           p.GetKey(),
				ConflictTxnID: conflictTxnID,
			}
		}
	}
	return nil
}

// buildConflictingIntentsMap scans all pending intents once and returns a map
// of userKey → conflicting txnID for intents not belonging to excludeTxnID.
func (db *DBImpl) buildConflictingIntentsMap(excludeTxnID []byte) (map[string][]byte, error) {
	pdb := db.getPDB()

	if pdb == nil {
		return nil, pebble.ErrClosed
	}

	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: txnIntentsPrefix,
		UpperBound: utils.PrefixSuccessor(txnIntentsPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	conflicts := make(map[string][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		txnID, intentUserKey := parseIntentKey(iter.Key())
		if txnID == nil || intentUserKey == nil {
			continue
		}
		if bytes.Equal(txnID, excludeTxnID) {
			continue
		}
		key := string(intentUserKey)
		if _, exists := conflicts[key]; !exists {
			conflicts[key] = bytes.Clone(txnID)
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating intents: %w", err)
	}

	return conflicts, nil
}

// hasConflictingIntentForKey checks if any other transaction has a pending intent for the given key.
// Returns the conflicting transaction ID if found, nil otherwise.
// excludeTxnID is the current transaction's ID - its own intents are not considered conflicts.
func (db *DBImpl) hasConflictingIntentForKey(userKey []byte, excludeTxnID []byte) ([]byte, error) {
	// Intent keys are structured as: \x00\x00__txn_intents__:<txnID>:<userKey>
	// We need to scan all intents and check if any match our userKey
	// This is O(n) in the number of pending intents, but necessary for correctness.
	// Could be optimized with a reverse index (userKey -> txnIDs) in the future.

	pdb := db.getPDB()

	if pdb == nil {
		return nil, pebble.ErrClosed
	}

	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: txnIntentsPrefix,
		UpperBound: utils.PrefixSuccessor(txnIntentsPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		txnID, intentUserKey := parseIntentKey(iter.Key())
		if txnID == nil || intentUserKey == nil {
			continue
		}

		// Check if this intent is for our key
		if !bytes.Equal(intentUserKey, userKey) {
			continue
		}

		// Check if this is our own transaction's intent (not a conflict)
		if bytes.Equal(txnID, excludeTxnID) {
			continue
		}

		// Found a conflicting intent from another transaction
		return txnID, nil
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating intents: %w", err)
	}

	return nil, nil
}

// shouldWriteValue checks if we should write a new value based on timestamp comparison
// Returns true if the new timestamp is higher than the existing value's timestamp
// Reads timestamp from separate key:t metadata key for optimal performance (~5x faster)
func (db *DBImpl) shouldWriteValue(key []byte, newTimestamp uint64) (bool, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return false, pebble.ErrClosed
	}
	// Construct timestamp metadata key (key:t)
	txnKey := slices.Concat(key, storeutils.TransactionSuffix)

	existingBytes, closer, err := pdb.Get(txnKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// No timestamp metadata, check for legacy _timestamp in document
			return db.shouldWriteValueLegacy(key, newTimestamp)
		}
		return false, fmt.Errorf("reading timestamp metadata: %w", err)
	}
	defer func() { _ = closer.Close() }()

	// Timestamp is stored as 8 bytes (uint64 little-endian)
	if len(existingBytes) < 8 {
		// Corrupted or invalid timestamp, allow write
		db.logger.Warn("Invalid timestamp metadata, allowing write",
			zap.String("key", types.FormatKey(key)),
			zap.Int("bytesLength", len(existingBytes)))
		return true, nil
	}

	_, existingTimestamp, err := encoding.DecodeUint64Ascending(existingBytes[0:8])
	if err != nil {
		db.logger.Warn("Error decoding timestamp metadata, allowing write",
			zap.String("key", types.FormatKey(key)),
			zap.Error(err))
		return true, nil
	}

	// Only write if new timestamp is higher
	return newTimestamp > existingTimestamp, nil
}

// shouldWriteValueLegacy handles backward compatibility by checking _timestamp field in document
// This is used when key:t metadata doesn't exist (legacy documents)
func (db *DBImpl) shouldWriteValueLegacy(key []byte, newTimestamp uint64) (bool, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return false, pebble.ErrClosed
	}
	existingBytes, closer, err := pdb.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// Key doesn't exist, safe to write
			return true, nil
		}
		return false, fmt.Errorf("reading existing value: %w", err)
	}
	defer func() { _ = closer.Close() }()

	// Decompress and decode existing value
	existingDoc, err := storeutils.DecodeDocumentJSON(existingBytes)
	if err != nil {
		// If decompression/decode fails, might be legacy uncompressed value, allow write
		return true, nil
	}

	// Check existing timestamp
	existingTimestamp, ok := existingDoc["_timestamp"].(float64)
	if !ok {
		// No timestamp field, assume legacy value, safe to overwrite
		return true, nil
	}

	// Only write if new timestamp is higher
	return newTimestamp > uint64(existingTimestamp), nil
}

// loadTxnRecord reads and unmarshals the TxnRecord for txnID from Pebble.
// Returns ErrTxnNotFound when the record does not exist.
func (db *DBImpl) loadTxnRecord(txnID []byte) (TxnRecord, error) {
	pdb := db.getPDB()

	if pdb == nil {
		return TxnRecord{}, pebble.ErrClosed
	}

	data, closer, err := pdb.Get(makeTxnKey(txnID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return TxnRecord{}, ErrTxnNotFound
		}
		return TxnRecord{}, fmt.Errorf("loading transaction record: %w", err)
	}
	defer func() { _ = closer.Close() }()

	var record TxnRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return TxnRecord{}, fmt.Errorf("unmarshaling transaction record: %w", err)
	}
	return record, nil
}

// GetTransactionStatus queries transaction status from coordinator
func (db *DBImpl) GetTransactionStatus(ctx context.Context, txnID []byte) (int32, error) {
	record, err := db.loadTxnRecord(txnID)
	if err != nil {
		if errors.Is(err, ErrTxnNotFound) {
			// Transaction not found - might be old and cleaned up
			return TxnStatusAborted, nil
		}
		return 0, err
	}
	return record.Status, nil
}

// GetCommitVersion returns the commit version for a committed transaction.
func (db *DBImpl) GetCommitVersion(ctx context.Context, txnID []byte) (uint64, error) {
	record, err := db.loadTxnRecord(txnID)
	if err != nil {
		return 0, err
	}
	return record.Version(), nil
}

func (db *DBImpl) ListTxnRecords(_ context.Context) ([]TxnRecord, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}

	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: txnRecordsPrefix,
		UpperBound: utils.PrefixSuccessor(txnRecordsPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating transaction record iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	records := make([]TxnRecord, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		var record TxnRecord
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return nil, fmt.Errorf("unmarshaling transaction record: %w", err)
		}
		record.TxnID = bytes.Clone(record.TxnID)
		record.Participants = cloneTxnParticipants(record.Participants)
		record.ResolvedParticipants = append([]string(nil), record.ResolvedParticipants...)
		records = append(records, record)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating transaction records: %w", err)
	}
	return records, nil
}

func (db *DBImpl) ListTxnIntents(_ context.Context) ([]TxnIntent, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}

	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: txnIntentsPrefix,
		UpperBound: utils.PrefixSuccessor(txnIntentsPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating transaction intent iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	intents := make([]TxnIntent, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		intent, err := decodeTxnIntent(iter.Key(), iter.Value())
		if err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating transaction intents: %w", err)
	}
	return intents, nil
}

func cloneTxnParticipants(participants [][]byte) [][]byte {
	if len(participants) == 0 {
		return nil
	}
	cloned := make([][]byte, 0, len(participants))
	for _, participant := range participants {
		cloned = append(cloned, bytes.Clone(participant))
	}
	return cloned
}

func decodeTxnIntent(intentKey, payload []byte) (TxnIntent, error) {
	var raw struct {
		Value            []byte `json:"value"`
		TxnID            []byte `json:"txn_id"`
		Timestamp        uint64 `json:"timestamp"`
		CoordinatorShard []byte `json:"coordinator_shard"`
		Status           int32  `json:"status"`
		IsDelete         bool   `json:"is_delete"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return TxnIntent{}, fmt.Errorf("unmarshaling transaction intent: %w", err)
	}
	parsedTxnID, userKey := parseIntentKey(intentKey)
	if parsedTxnID == nil || userKey == nil {
		return TxnIntent{}, fmt.Errorf("parsing transaction intent key %q", types.FormatKey(intentKey))
	}
	txnID := raw.TxnID
	if len(txnID) == 0 {
		txnID = parsedTxnID
	}
	return TxnIntent{
		TxnID:            bytes.Clone(txnID),
		UserKey:          bytes.Clone(userKey),
		Value:            bytes.Clone(raw.Value),
		Timestamp:        raw.Timestamp,
		CoordinatorShard: bytes.Clone(raw.CoordinatorShard),
		Status:           raw.Status,
		IsDelete:         raw.IsDelete,
	}, nil
}

// ========================================
// Edge Operations (Graph Database)
// ========================================

// AddEdge creates a unidirectional edge and updates the edge index
func (db *DBImpl) AddEdge(
	ctx context.Context,
	indexName string,
	source, target []byte,
	edgeType string,
	weight float64,
	metadata map[string]any,
) error {
	now := time.Now()
	edge := &indexes.Edge{
		Source:    source,
		Target:    target,
		Type:      edgeType,
		Weight:    weight,
		CreatedAt: now,
		UpdatedAt: now,
		Metadata:  metadata,
	}

	edgeValue, err := indexes.EncodeEdgeValue(edge)
	if err != nil {
		return fmt.Errorf("encoding edge: %w", err)
	}

	// Create outgoing edge key (unidirectional)
	outKey := storeutils.MakeEdgeKey(source, target, indexName, edgeType)

	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	// Write edge - the graph index will automatically update the edge index
	// via its Batch() method when this write is propagated
	if err := pdb.Set(outKey, edgeValue, pebble.Sync); err != nil {
		return fmt.Errorf("writing outgoing edge: %w", err)
	}

	// Update indexes (including graph index which maintains edge index)
	if err := db.indexManager.Batch(ctx, [][2][]byte{{outKey, edgeValue}}, nil, Op_SyncLevelWrite); err != nil {
		return fmt.Errorf("updating edge index: %w", err)
	}

	db.logger.Debug("Added edge",
		zap.String("index", indexName),
		zap.ByteString("source", source),
		zap.ByteString("target", target),
		zap.String("type", edgeType),
		zap.Float64("weight", weight))

	return nil
}

// GetEdges retrieves edges for a document in a specific graph index
func (db *DBImpl) GetEdges(
	ctx context.Context,
	indexName string,
	key []byte,
	edgeType string,
	direction indexes.EdgeDirection,
) ([]indexes.Edge, error) {
	// Check if the index exists
	if db.indexManager.GetIndex(indexName) == nil {
		return nil, fmt.Errorf("index not found: %s", indexName)
	}

	switch direction {
	case indexes.EdgeDirectionOut:
		return db.getOutgoingEdges(ctx, indexName, key, edgeType)
	case indexes.EdgeDirectionIn:
		return db.getIncomingEdges(ctx, indexName, key, edgeType)
	case indexes.EdgeDirectionBoth:
		out, err := db.getOutgoingEdges(ctx, indexName, key, edgeType)
		if err != nil {
			return nil, err
		}
		in, err := db.getIncomingEdges(ctx, indexName, key, edgeType)
		if err != nil {
			return nil, err
		}
		return append(out, in...), nil
	default:
		// Invalid direction - return empty results instead of error
		return []indexes.Edge{}, nil
	}
}

// getOutgoingEdges retrieves stored outgoing edges
func (db *DBImpl) getOutgoingEdges(
	ctx context.Context,
	indexName string,
	key []byte,
	edgeType string,
) ([]indexes.Edge, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}
	prefix := storeutils.EdgeIteratorPrefix(key, indexName, edgeType)

	iter, err := pdb.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating edge iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	edges := make([]indexes.Edge, 0)

	for iter.First(); iter.Valid(); iter.Next() {
		source, target, _, parsedType, err := storeutils.ParseEdgeKey(iter.Key())
		if err != nil {
			db.logger.Warn("Failed to parse edge key",
				zap.String("key", types.FormatKey(iter.Key())),
				zap.Error(err))
			continue
		}

		edge, err := indexes.DecodeEdgeValue(iter.Value())
		if err != nil {
			db.logger.Warn("Failed to decode edge value",
				zap.String("key", types.FormatKey(iter.Key())),
				zap.Error(err))
			continue
		}

		// Clone source and target to avoid sharing iterator's key buffer
		edge.Source = bytes.Clone(source)
		edge.Target = bytes.Clone(target)
		edge.Type = parsedType

		edges = append(edges, *edge)
	}

	return edges, iter.Error()
}

// getIncomingEdges retrieves incoming edges via edge index
func (db *DBImpl) getIncomingEdges(
	ctx context.Context,
	indexName string,
	key []byte,
	edgeType string,
) ([]indexes.Edge, error) {
	// Query the edge index for reverse lookups
	// The graph_v0 index maintains: target → [source edges]
	query := map[string]any{
		"target":   key,
		"edgeType": edgeType,
	}

	result, err := db.routeSearch(ctx, indexName, query, key)
	if err != nil {
		return nil, fmt.Errorf("querying edge index: %w", err)
	}

	// The result should be []indexes.Edge
	edges, ok := result.([]indexes.Edge)
	if !ok {
		return nil, fmt.Errorf("unexpected result type from edge index: %T", result)
	}

	return edges, nil
}

// DeleteEdge removes a unidirectional edge and updates the edge index
func (db *DBImpl) DeleteEdge(
	ctx context.Context,
	indexName string,
	source, target []byte,
	edgeType string,
) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	outKey := storeutils.MakeEdgeKey(source, target, indexName, edgeType)

	// Delete edge - the graph index will automatically update the edge index
	// via its Batch() method when this delete is propagated
	if err := pdb.Delete(outKey, pebble.Sync); err != nil {
		return fmt.Errorf("deleting outgoing edge: %w", err)
	}

	// Update indexes (including graph index which maintains edge index)
	if err := db.indexManager.Batch(ctx, nil, [][]byte{outKey}, Op_SyncLevelWrite); err != nil {
		return fmt.Errorf("updating edge index after delete: %w", err)
	}

	db.logger.Debug("Deleted edge",
		zap.String("index", indexName),
		zap.ByteString("source", source),
		zap.ByteString("target", target),
		zap.String("type", edgeType))

	return nil
}

// UpdateEdgeWeight updates the weight of an existing edge
func (db *DBImpl) UpdateEdgeWeight(
	ctx context.Context,
	indexName string,
	source, target []byte,
	edgeType string,
	newWeight float64,
) error {
	pdb := db.getPDB()
	if pdb == nil {
		return pebble.ErrClosed
	}
	outKey := storeutils.MakeEdgeKey(source, target, indexName, edgeType)

	// Read existing edge
	existingValue, closer, err := pdb.Get(outKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("edge not found")
		}
		return fmt.Errorf("reading edge: %w", err)
	}

	edge, err := indexes.DecodeEdgeValue(existingValue)
	_ = closer.Close()
	if err != nil {
		return fmt.Errorf("decoding edge: %w", err)
	}

	// Update weight and timestamp
	edge.Weight = newWeight
	edge.UpdatedAt = time.Now()

	// Encode updated edge
	updatedValue, err := indexes.EncodeEdgeValue(edge)
	if err != nil {
		return fmt.Errorf("encoding updated edge: %w", err)
	}

	// Write updated edge
	if err := pdb.Set(outKey, updatedValue, pebble.Sync); err != nil {
		return fmt.Errorf("writing updated edge: %w", err)
	}

	// Update indexes (including graph index which maintains edge index)
	if err := db.indexManager.Batch(ctx, [][2][]byte{{outKey, updatedValue}}, nil, Op_SyncLevelWrite); err != nil {
		return fmt.Errorf("updating edge index: %w", err)
	}

	db.logger.Debug("Updated edge weight",
		zap.String("index", indexName),
		zap.ByteString("source", source),
		zap.ByteString("target", target),
		zap.String("type", edgeType),
		zap.Float64("new_weight", newWeight))

	return nil
}

// collectOutgoingEdgeKeys scans and returns all outgoing edge keys for a document
// This is used before deletion to ensure the edge index can be properly updated
func (db *DBImpl) collectOutgoingEdgeKeys(key []byte) ([][]byte, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}
	edgeKeys := make([][]byte, 0)

	// Iterate through all graph indexes
	db.indexesMu.RLock()
	indexMap := db.indexes
	db.indexesMu.RUnlock()

	for indexName, indexConfig := range indexMap {
		// Skip non-graph indexes
		if !indexes.IsGraphType(indexConfig.Type) {
			continue
		}

		// Scan for outgoing edges with this index name
		prefix := storeutils.EdgeIteratorPrefix(key, indexName, "")

		iter, err := pdb.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: utils.PrefixSuccessor(prefix),
		})
		if err != nil {
			return nil, fmt.Errorf("creating edge iterator: %w", err)
		}

		// Use a closure to ensure iterator is always closed via defer
		err = func() error {
			defer func() {
				if closeErr := iter.Close(); closeErr != nil {
					db.logger.Warn("Error closing edge iterator",
						zap.String("key", types.FormatKey(key)),
						zap.String("index", indexName),
						zap.Error(closeErr))
				}
			}()

			for iter.First(); iter.Valid(); iter.Next() {
				edgeKey := bytes.Clone(iter.Key())
				edgeKeys = append(edgeKeys, edgeKey)
			}
			return iter.Error()
		}()
		if err != nil {
			return nil, fmt.Errorf("iterating edges: %w", err)
		}
	}

	if len(edgeKeys) > 0 {
		db.logger.Debug("Collected edge keys for deletion",
			zap.String("key", types.FormatKey(key)),
			zap.Int("count", len(edgeKeys)))
	}

	return edgeKeys, nil
}

func (db *DBImpl) collectChunkKeysForDeletion(key []byte) ([][]byte, error) {
	pdb := db.getPDB()
	if pdb == nil {
		return nil, pebble.ErrClosed
	}

	prefix := append(bytes.Clone(key), []byte(":i:")...)
	iter, err := pdb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating chunk iterator: %w", err)
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			db.logger.Warn("Error closing chunk iterator",
				zap.String("key", types.FormatKey(key)),
				zap.Error(closeErr))
		}
	}()

	chunkKeys := make([][]byte, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		if !storeutils.IsChunkKey(iter.Key()) {
			continue
		}
		chunkKeys = append(chunkKeys, bytes.Clone(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating chunk keys: %w", err)
	}

	if len(chunkKeys) > 0 {
		db.logger.Debug("Collected chunk keys for deletion",
			zap.String("key", types.FormatKey(key)),
			zap.Int("count", len(chunkKeys)))
	}

	return chunkKeys, nil
}

// GetNeighbors is a convenience method for single-hop graph traversal
func (db *DBImpl) GetNeighbors(
	ctx context.Context,
	indexName string,
	startKey []byte,
	edgeType string,
	direction indexes.EdgeDirection,
) ([]*indexes.TraversalResult, error) {
	rules := indexes.DefaultTraversalRules()
	rules.MaxDepth = 1
	rules.Direction = direction
	if edgeType != "" {
		rules.EdgeTypes = []string{edgeType}
	}

	return db.TraverseEdges(ctx, indexName, startKey, rules)
}

// TraverseEdges performs graph traversal from a start node using BFS
func (db *DBImpl) TraverseEdges(
	ctx context.Context,
	indexName string,
	startKey []byte,
	rules indexes.TraversalRules,
) ([]*indexes.TraversalResult, error) {
	// Check if the index exists
	if db.indexManager.GetIndex(indexName) == nil {
		return nil, fmt.Errorf("index not found: %s", indexName)
	}

	results := make([]*indexes.TraversalResult, 0)
	visited := make(map[string]bool)

	// BFS queue: (key, depth, path, pathEdges, totalWeight)
	type queueItem struct {
		key         []byte
		depth       int
		path        [][]byte
		pathEdges   []indexes.Edge
		totalWeight float64
	}

	queue := []queueItem{{
		key:         startKey,
		depth:       0,
		path:        [][]byte{startKey},
		pathEdges:   []indexes.Edge{},
		totalWeight: 1.0,
	}}

	// Mark start node as visited
	if rules.DeduplicateNodes {
		visited[string(startKey)] = true
	}

	for len(queue) > 0 {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Dequeue
		current := queue[0]
		queue = queue[1:]

		// Add current node to results (skip depth 0 if user only wants neighbors)
		if current.depth > 0 || rules.MaxDepth == 0 {
			result := &indexes.TraversalResult{
				Key:         current.key,
				Document:    nil, // Not loading documents by default
				Depth:       current.depth,
				TotalWeight: current.totalWeight,
			}

			if rules.IncludePaths {
				result.Path = current.path
				result.PathEdges = current.pathEdges
			}

			results = append(results, result)

			// Check max results limit
			if rules.MaxResults > 0 && len(results) >= rules.MaxResults {
				break
			}
		}

		// Check max depth
		if rules.MaxDepth > 0 && current.depth >= rules.MaxDepth {
			continue
		}

		// Get edges from current node
		edges, err := db.GetEdges(ctx, indexName, current.key, "", rules.Direction)
		if err != nil {
			db.logger.Warn("Failed to get edges during traversal",
				zap.String("key", types.FormatKey(current.key)),
				zap.Error(err))
			continue
		}

		// Process each edge
		for _, edge := range edges {
			// Apply edge filters
			if !rules.ShouldTraverseEdge(edge) {
				continue
			}

			// Determine next key based on direction
			var nextKey []byte
			switch rules.Direction {
			case indexes.EdgeDirectionOut:
				nextKey = edge.Target
			case indexes.EdgeDirectionIn:
				nextKey = edge.Source
			default:
				// For "both", determine which is the next node
				if bytes.Equal(edge.Source, current.key) {
					nextKey = edge.Target
				} else {
					nextKey = edge.Source
				}
			}

			// Check if already visited
			nextKeyStr := string(nextKey)
			if rules.DeduplicateNodes && visited[nextKeyStr] {
				continue
			}

			// Mark as visited
			if rules.DeduplicateNodes {
				visited[nextKeyStr] = true
			}

			// Build path for next node
			var nextPath [][]byte
			var nextPathEdges []indexes.Edge
			if rules.IncludePaths {
				nextPath = make([][]byte, len(current.path)+1)
				copy(nextPath, current.path)
				nextPath[len(current.path)] = nextKey

				nextPathEdges = make([]indexes.Edge, len(current.pathEdges)+1)
				copy(nextPathEdges, current.pathEdges)
				nextPathEdges[len(current.pathEdges)] = edge
			}

			// Enqueue next node
			queue = append(queue, queueItem{
				key:         nextKey,
				depth:       current.depth + 1,
				path:        nextPath,
				pathEdges:   nextPathEdges,
				totalWeight: current.totalWeight * edge.Weight,
			})
		}
	}

	return results, nil
}
