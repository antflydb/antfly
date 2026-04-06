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
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/inflight"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/raftkv"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"

	"github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// StoreDB wraps a DB with Raft consensus for distributed storage.
type StoreDB struct {
	logger *zap.Logger

	proposer       *raftkv.Proposer
	loadSnapshotID func(ctx context.Context) (string, error)

	dataDir         string
	dbDir           string
	coreDB          DB
	byteRange       types.Range
	byteRangeMu     sync.RWMutex // protects byteRange, pendingSplitKey, splitState, and mergeState
	pendingSplitKey []byte       // set during split to reject writes in split-off range
	splitState      *SplitState  // Raft-replicated split state (loaded from Pebble on startup)
	mergeState      *MergeState  // Raft-replicated merge state (loaded from Pebble on startup)
	schema          *schema.TableSchema
	snapStore       snapstore.SnapStore
	antflyConfig    *common.Config

	splitting                atomic.Bool
	initializing             atomic.Bool
	splitReplayRequired      atomic.Bool
	splitReplayCaughtUp      atomic.Bool
	splitCutoverReady        atomic.Bool
	splitReplaySeq           atomic.Uint64
	splitReplayParentShardID atomic.Uint64
	restoredArchiveMetadata  *common.ArchiveMetadata
	localSplitSourceLookup   func(types.ID) *StoreDB
	splitReplayCancel        context.CancelFunc
	splitReplayMu            sync.Mutex

	proposalQueue *inflight.DedupeQueue
	backgroundWG  sync.WaitGroup

	// TODO (ajr) Where to store this?
	created time.Time
	clock   clock.Clock
}

func (s *StoreDB) clockOrReal() clock.Clock {
	if s != nil && s.clock != nil {
		return s.clock
	}
	return clock.RealClock{}
}

func NewStoreDB(
	lg *zap.Logger,
	antflyConfig *common.Config,
	dbDir string,
	snapStore snapstore.SnapStore,
	idxs map[string]indexes.IndexConfig,
	schema *schema.TableSchema,
	byteRange types.Range,
	loadSnapshotID func(ctx context.Context) (string, error),
	proposeC chan<- *raft.Proposal,
	commitC <-chan *raft.Commit,
	errorC <-chan error,
	shardNotifier ShardNotifier,
	localSplitSourceLookup func(types.ID) *StoreDB,
	cache *pebbleutils.Cache,
	clk clock.Clock,
) (*StoreDB, error) {
	if idxs == nil {
		idxs = make(map[string]indexes.IndexConfig)
	}
	if clk == nil {
		clk = clock.RealClock{}
	}
	dataDir := antflyConfig.GetBaseDir()
	s := &StoreDB{
		logger:                 lg.Named("storeDB"),
		proposer:               raftkv.NewProposer(proposeC),
		dataDir:                dataDir,
		dbDir:                  dbDir,
		snapStore:              snapStore,
		byteRange:              byteRange,
		loadSnapshotID:         loadSnapshotID,
		created:                clk.Now().UTC(),
		schema:                 schema,
		antflyConfig:           antflyConfig,
		clock:                  clk,
		localSplitSourceLookup: localSplitSourceLookup,
		coreDB: NewDBImpl(
			lg.Named("coreDB"),
			antflyConfig,
			schema,
			idxs,
			snapStore,
			shardNotifier,
			cache,
		),
		proposalQueue: inflight.NewDedupeQueue(100, 100),
	}

	// Set the proposeResolveIntentsFunc and proposeAbortTransactionFunc on DBImpl
	// so it can propose resolve intents and abort stale pending transactions through Raft
	if dbImpl, ok := s.coreDB.(*DBImpl); ok {
		dbImpl.SetProposeResolveIntentsFunc(s.proposeResolveIntents)
		dbImpl.SetProposeAbortTransactionFunc(s.proposeAbortTransaction)
	}
	ctx := context.Background()
	// FIXME (ajr) If we gracefully shutdown we shouldn't need to load from the snapshot
	if _, err := os.Stat(dbDir); err == nil {
		// Directory exists
		if err := s.openDBAndIndex(true); err != nil {
			s.proposalQueue.Close()
			return nil, fmt.Errorf("opening db and indexes: %w", err)
		}
	} else {
		if err := s.loadAndRecoverFromPersistentSnapshot(ctx); err != nil {
			s.proposalQueue.Close()
			return nil, fmt.Errorf("loading and recovering from snapshot: %w", err)
		}
	}
	// read commits from raft into kvStore map until error
	s.backgroundWG.Add(2)
	go func() {
		defer s.backgroundWG.Done()
		s.readCommits(ctx, commitC, errorC)
	}()
	go func() {
		defer s.backgroundWG.Done()
		_ = s.proposalDequeuer(ctx)
	}()
	// In the case of a restart we need to make sure the byteRange is correct
	var err error
	s.byteRange, err = s.coreDB.GetRange()
	if err != nil {
		s.proposalQueue.Close()
		return nil, fmt.Errorf("getting byte range from coreDB: %w", err)
	}
	// Load split state from Pebble (if any split was in progress before restart)
	s.splitState = s.coreDB.GetSplitState()
	if s.splitState != nil {
		s.logger.Info("Loaded split state from Pebble",
			zap.Int32("phase", int32(s.splitState.GetPhase())),
			zap.Binary("splitKey", s.splitState.GetSplitKey()),
			zap.Uint64("newShardID", s.splitState.GetNewShardId()))
	}
	s.mergeState = s.coreDB.GetMergeState()
	if s.mergeState != nil {
		s.logger.Info("Loaded merge state from Pebble",
			zap.Int32("phase", int32(s.mergeState.GetPhase())),
			zap.Uint64("donorShardID", s.mergeState.GetDonorShardId()),
			zap.Uint64("receiverShardID", s.mergeState.GetReceiverShardId()))
	}
	if err := s.startSplitReplayIfNeeded(); err != nil {
		s.proposalQueue.Close()
		return nil, fmt.Errorf("starting split replay: %w", err)
	}
	return s, nil
}

func (s *StoreDB) LeaderFactory(ctx context.Context) error {
	var persist PersistFunc = func(ctx context.Context, writes [][2][]byte) error {
		filteredWrites := make([]*Write, 0, len(writes))
		filteredDeletes := make([][]byte, 0)

		s.byteRangeMu.RLock()
		byteRange := s.byteRange
		splitState := s.splitState
		mergeState := s.mergeState
		s.byteRangeMu.RUnlock()

		for _, w := range writes {
			if !isKeyOwnedDuringSplit(w[0], byteRange, splitState) ||
				!isKeyOwnedDuringMerge(w[0], byteRange, mergeState) {
				return common.NewErrKeyOutOfRange(w[0], byteRange)
			}

			// nil value indicates delete operation
			if w[1] == nil {
				filteredDeletes = append(filteredDeletes, w[0])
			} else {
				write := Write_builder{
					Key:   w[0],
					Value: w[1],
				}.Build()
				filteredWrites = append(filteredWrites, write)
			}
		}

		bo := BatchOp_builder{
			Writes:  filteredWrites,
			Deletes: filteredDeletes,
		}.Build()
		return s.proposalEnqueuer(ctx, bo)
	}
	return s.coreDB.LeaderFactory(ctx, persist)
}

func (s *StoreDB) FindMedianKey() ([]byte, error) {
	return s.coreDB.FindMedianKey()
}

func (s *StoreDB) Lookup(ctx context.Context, key string) (map[string]any, error) {
	doc, err := s.coreDB.Get(ctx, []byte(key))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, errors.New("not found")
		}
		return nil, err
	}
	return doc, nil
}

// GetTimestamp returns the HLC timestamp for a key from the :t suffix metadata key.
// Returns 0 if the key has no timestamp (never written via transactions).
func (s *StoreDB) GetTimestamp(key string) (uint64, error) {
	return s.coreDB.GetTimestamp([]byte(key))
}

func (s *StoreDB) AddIndex(
	ctx context.Context,
	config indexes.IndexConfig,
) error {
	if s.coreDB.HasIndex(config.Name) {
		// Retries shouldn't fail if the index already exists
		return nil
	}
	op, err := NewAddIndexOp(&config)
	if err != nil {
		return err
	}
	return s.syncWriteOp(ctx, op)
}

func (s *StoreDB) DeleteIndex(ctx context.Context, name string) error {
	if !s.coreDB.HasIndex(name) {
		// Retries shouldn't fail if the index already exists
		return nil
	}
	return s.syncWriteOp(ctx, NewDeleteIndexOp(name))
}

func (s *StoreDB) Batch(ctx context.Context, batch *BatchOp) error {
	return s.applyBatch(ctx, batch, false)
}

func (s *StoreDB) ApplyMergeChunk(ctx context.Context, batch *BatchOp) error {
	return s.applyBatch(ctx, batch, true)
}

func (s *StoreDB) applyBatch(ctx context.Context, batch *BatchOp, skipRangeValidation bool) error {
	// TODO (ajr) We have multiple levels of validation for keys in the range, how many do we need?
	// Use sync_level from the BatchOp (clients set this)
	syncLevel := batch.GetSyncLevel()

	// Pre-compute enrichments for SyncLevelEnrichments AND SyncLevelEmbeddings
	// This avoids the deadlock where enrichment during apply tries to submit a new Raft proposal
	if syncLevel == Op_SyncLevelEnrichments || syncLevel == Op_SyncLevelEmbeddings {
		enrichedBatch, err := s.preEnrichBatch(ctx, batch)
		if err != nil {
			return fmt.Errorf("pre-enriching batch: %w", err)
		}
		batch = enrichedBatch

		// SyncLevelEnrichments downgrades to Write (waits for Pebble write)
		// SyncLevelEmbeddings stays as Embeddings (waits for vector index write)
		if syncLevel == Op_SyncLevelEnrichments {
			batch.SetSyncLevel(Op_SyncLevelWrite)
			syncLevel = Op_SyncLevelWrite
		}
	}

	if syncLevel == Op_SyncLevelPropose {
		if skipRangeValidation {
			return s.syncWriteOpWithoutValidation(ctx, NewBatchOp(batch))
		}
		return s.proposalEnqueuer(ctx, batch)
	}
	// For Write, FullText, and Aknn levels, wait for commit
	if skipRangeValidation {
		return s.syncWriteOpWithoutValidation(ctx, NewBatchOp(batch))
	}
	return s.syncWriteOp(ctx, NewBatchOp(batch))
}

// preEnrichBatch pre-computes enrichments before Raft proposal
// Returns an enriched batch with all embeddings/summaries/chunks included
func (s *StoreDB) preEnrichBatch(
	ctx context.Context,
	batch *BatchOp,
) (*BatchOp, error) {
	writes := batch.GetWrites()

	// Convert protobuf writes to [][2][]byte
	writePairs := make([][2][]byte, 0, len(writes))
	for _, w := range writes {
		writePairs = append(writePairs, [2][]byte{w.GetKey(), w.GetValue()})
	}

	// 1. Extract user-provided enrichments (_embeddings, _summaries, _edges)
	embWrites, sumWrites, edgeWrites, indexDeletes, err := s.coreDB.ExtractEnrichments(ctx, writePairs)
	if err != nil {
		return nil, fmt.Errorf("extracting enrichments: %w", err)
	}

	// 2. Compute generated enrichments (via enrichers)
	generatedWrites, failedEnrichments, err := s.coreDB.ComputeEnrichments(ctx, writePairs)
	if err != nil {
		return nil, fmt.Errorf("computing enrichments: %w", err)
	}

	// Log if some enrichments failed during pre-enrichment
	// These will be automatically picked up by the async enrichment pipeline after persistence
	if len(failedEnrichments) > 0 {
		s.logger.Info("Some enrichments failed during pre-enrichment and will be retried asynchronously",
			zap.Int("failedCount", len(failedEnrichments)),
			zap.ByteStrings("failedKeys", failedEnrichments))
	}

	// 3. Combine all writes
	allWrites := make([]*Write, 0, len(writes)+len(embWrites)+len(sumWrites)+len(edgeWrites)+len(generatedWrites))
	allWrites = append(allWrites, writes...)
	allDeletes := append(batch.GetDeletes(), indexDeletes...)

	// Add extracted enrichments
	for _, w := range embWrites {
		if w[1] == nil {
			allDeletes = append(allDeletes, w[0])
			continue
		}
		allWrites = append(allWrites, Write_builder{Key: w[0], Value: w[1]}.Build())
	}
	for _, w := range sumWrites {
		if w[1] == nil {
			allDeletes = append(allDeletes, w[0])
			continue
		}
		allWrites = append(allWrites, Write_builder{Key: w[0], Value: w[1]}.Build())
	}
	for _, w := range edgeWrites {
		if w[1] == nil {
			allDeletes = append(allDeletes, w[0])
			continue
		}
		allWrites = append(allWrites, Write_builder{Key: w[0], Value: w[1]}.Build())
	}

	// Add generated enrichments
	for _, w := range generatedWrites {
		if w[1] == nil {
			allDeletes = append(allDeletes, w[0])
			continue
		}
		allWrites = append(allWrites, Write_builder{Key: w[0], Value: w[1]}.Build())
	}

	// Log enrichment stats
	if len(embWrites) > 0 || len(sumWrites) > 0 || len(edgeWrites) > 0 || len(generatedWrites) > 0 {
		s.logger.Debug("Pre-enriched batch",
			zap.Int("originalWrites", len(writes)),
			zap.Int("embeddingWrites", len(embWrites)),
			zap.Int("summaryWrites", len(sumWrites)),
			zap.Int("edgeWrites", len(edgeWrites)),
			zap.Int("generatedWrites", len(generatedWrites)),
			zap.Int("totalWrites", len(allWrites)))
	}

	// Return enriched batch
	syncLevel := batch.GetSyncLevel()
	timestamp := batch.GetTimestamp()
	return BatchOp_builder{
		Writes:     allWrites,
		Deletes:    allDeletes,
		Transforms: batch.GetTransforms(),
		SyncLevel:  &syncLevel,
		Timestamp:  timestamp,
	}.Build(), nil
}

const backupValidationRegex = `^[a-zA-Z0-9_-]+$`

var backupRegex = regexp.MustCompile(backupValidationRegex)

func (s *StoreDB) Backup(ctx context.Context, loc, id string) error {
	if !backupRegex.MatchString(id) {
		return fmt.Errorf("invalid backup ID: %s, must match regex %s", id, backupValidationRegex)
	}
	loc = strings.TrimPrefix(loc, "file://") // Remove "file://" prefix if present

	backupOp := BackupOp_builder{
		BackupId: id,
		Location: loc,
	}.Build()
	return s.applyOpBackup(ctx, backupOp)

	// TODO (ajr) Do we want to send the operation to the raft log?
	// or is the leader doing the backup enough?
	// return s.syncWriteOp(ctx, newBackupOp(loc, id))
}

func (s *StoreDB) ExportPortable(ctx context.Context, w io.Writer) error {
	return s.coreDB.ExportPortable(ctx, w)
}

func (s *StoreDB) ImportPortable(ctx context.Context, r io.Reader) error {
	return s.coreDB.ImportPortable(ctx, r)
}

func (s *StoreDB) SetRange(ctx context.Context, byteRange [2][]byte) error {
	return s.proposeOnlyWriteOp(ctx, NewSetRangeOp(byteRange))
}

func (s *StoreDB) UpdateSchema(ctx context.Context, schema *schema.TableSchema) error {
	op, err := NewUpdateSchemaOp(schema)
	if err != nil {
		return err
	}
	return s.proposeOnlyWriteOp(ctx, op)
}

// PrepareSplit transitions the shard into PHASE_PREPARE via Raft consensus.
// This sets the split state on all replicas, rejecting writes to the split-off range.
// Unlike the previous local-only pendingSplitKey, this state survives leadership changes.
// After calling PrepareSplit, call Split to actually execute the split.
func (s *StoreDB) PrepareSplit(ctx context.Context, splitKey []byte) error {
	s.byteRangeMu.RLock()
	byteRange := s.byteRange
	currentState := s.splitState
	s.byteRangeMu.RUnlock()

	if !byteRange.Contains(splitKey) {
		return common.NewErrKeyOutOfRange(splitKey, byteRange)
	}

	// Check if a split is already in progress
	if currentState != nil && currentState.GetPhase() != SplitState_PHASE_NONE {
		if !bytes.Equal(currentState.GetSplitKey(), splitKey) {
			return fmt.Errorf("split already in progress with different key: %x vs %x",
				currentState.GetSplitKey(), splitKey)
		}
		// Already in prepare or later phase with same key - idempotent success
		return nil
	}

	// Propose PHASE_PREPARE through Raft - this will set pendingSplitKey on all replicas
	prepareState := SplitState_builder{
		Phase:              SplitState_PHASE_PREPARE,
		SplitKey:           splitKey,
		StartedAtUnixNanos: time.Now().UnixNano(),
		OriginalRangeEnd:   byteRange[1],
	}.Build()

	return s.proposeSplitStateChange(ctx, prepareState)
}

// GetSplitState returns the current Raft-replicated split state, or nil if no split is in progress.
// This is thread-safe.
func (s *StoreDB) GetSplitState() *SplitState {
	s.byteRangeMu.RLock()
	defer s.byteRangeMu.RUnlock()
	return s.splitState
}

func (s *StoreDB) GetMergeState() *MergeState {
	s.byteRangeMu.RLock()
	defer s.byteRangeMu.RUnlock()
	return s.mergeState
}

func (s *StoreDB) IsInitializing() bool {
	return s.initializing.Load()
}

func (s *StoreDB) SplitReplayInfo() (required bool, caughtUp bool, cutoverReady bool, seq uint64, parentShardID types.ID) {
	return s.splitReplayRequired.Load(),
		s.splitReplayCaughtUp.Load(),
		s.splitCutoverReady.Load(),
		s.splitReplaySeq.Load(),
		types.ID(s.splitReplayParentShardID.Load())
}

func (s *StoreDB) setSplitReplayState(required, caughtUp, cutoverReady bool, seq uint64, parentShardID types.ID) {
	s.splitReplayRequired.Store(required)
	s.splitReplayCaughtUp.Store(caughtUp)
	s.splitCutoverReady.Store(cutoverReady)
	s.splitReplaySeq.Store(seq)
	s.splitReplayParentShardID.Store(uint64(parentShardID))
}

func (s *StoreDB) isActiveSplitReplaySource() bool {
	s.byteRangeMu.RLock()
	defer s.byteRangeMu.RUnlock()
	return isActiveSplitPhase(s.splitState)
}

// startSplitReplayIfNeeded must only be called once per StoreDB (from NewStoreDB).
// The goroutine it launches is tracked by backgroundWG, so CloseDB waits for it.
func (s *StoreDB) startSplitReplayIfNeeded() error {
	s.splitReplayMu.Lock()
	defer s.splitReplayMu.Unlock()

	if s.splitReplayCancel != nil {
		s.splitReplayCancel()
		s.splitReplayCancel = nil
	}

	s.initializing.Store(false)
	s.setSplitReplayState(false, true, true, 0, 0)

	metadata := s.restoredArchiveMetadata
	if metadata == nil || metadata.Split == nil {
		return nil
	}

	parentShardID, err := types.IDFromString(metadata.Split.ParentShardID)
	if err != nil {
		return fmt.Errorf("parsing split replay parent shard id %q: %w", metadata.Split.ParentShardID, err)
	}
	if s.localSplitSourceLookup == nil {
		return fmt.Errorf("split replay requires local parent shard lookup for child of %s", parentShardID)
	}

	startSeq := metadata.Split.ReplayFenceSeq
	s.initializing.Store(true)
	s.setSplitReplayState(true, false, false, startSeq, parentShardID)

	ctx, cancel := context.WithCancel(context.Background())
	s.splitReplayCancel = cancel
	s.backgroundWG.Add(1)
	go func() {
		defer s.backgroundWG.Done()
		s.runSplitReplayLoop(ctx, parentShardID, startSeq)
	}()
	return nil
}

func (s *StoreDB) applySplitDeltaEntries(entries []*SplitDeltaEntry) (uint64, error) {
	lastSeq := s.splitReplaySeq.Load()
	for _, entry := range entries {
		if entry == nil {
			continue
		}

		writes := make([][2][]byte, 0, len(entry.GetWrites()))
		for _, write := range entry.GetWrites() {
			writes = append(writes, [2][]byte{bytes.Clone(write.GetKey()), bytes.Clone(write.GetValue())})
		}
		deletes := make([][]byte, 0, len(entry.GetDeletes()))
		for _, key := range entry.GetDeletes() {
			deletes = append(deletes, bytes.Clone(key))
		}

		replayCtx := context.Background()
		if ts := entry.GetTimestamp(); ts > 0 {
			replayCtx = storeutils.WithTimestamp(replayCtx, ts)
		}
		if len(writes) > 0 || len(deletes) > 0 {
			if err := s.coreDB.Batch(replayCtx, writes, deletes, Op_SyncLevelWrite); err != nil {
				return lastSeq, fmt.Errorf("applying split delta seq %d: %w", entry.GetSequence(), err)
			}
		}
		lastSeq = entry.GetSequence()
		s.splitReplaySeq.Store(lastSeq)
	}
	return lastSeq, nil
}

func (s *StoreDB) runSplitReplayLoop(ctx context.Context, parentShardID types.ID, startSeq uint64) {
	logger := s.logger.With(
		zap.Stringer("splitReplayParentShardID", parentShardID),
		zap.Uint64("startSeq", startSeq),
	)
	clk := s.clockOrReal()
	currentSeq := startSeq
	ticker := clk.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	waitOrCancel := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C():
			return true
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		parent := s.localSplitSourceLookup(parentShardID)
		if parent == nil || parent.IsInitializing() {
			s.setSplitReplayState(true, false, false, currentSeq, parentShardID)
			if !waitOrCancel() {
				return
			}
			continue
		}

		targetSeq, err := parent.GetSplitDeltaSeq()
		if err != nil {
			logger.Debug("Failed to load parent split delta sequence", zap.Error(err))
			s.setSplitReplayState(true, false, false, currentSeq, parentShardID)
			if !waitOrCancel() {
				return
			}
			continue
		}
		finalSeq, err := parent.GetSplitDeltaFinalSeq()
		if err == nil && finalSeq > 0 {
			targetSeq = finalSeq
		}

		if currentSeq < targetSeq {
			entries, err := parent.ListSplitDeltaEntriesAfter(currentSeq)
			if err != nil {
				logger.Debug("Failed to list split delta entries", zap.Uint64("afterSeq", currentSeq), zap.Error(err))
				s.setSplitReplayState(true, false, false, currentSeq, parentShardID)
				if !waitOrCancel() {
					return
				}
				continue
			}
			currentSeq, err = s.applySplitDeltaEntries(entries)
			if err != nil {
				logger.Warn("Failed to apply split delta entries", zap.Error(err))
				s.setSplitReplayState(true, false, false, currentSeq, parentShardID)
				if !waitOrCancel() {
					return
				}
				continue
			}
		}

		caughtUp := currentSeq >= targetSeq
		cutoverReady := finalSeq > 0 && currentSeq >= finalSeq
		s.setSplitReplayState(true, caughtUp, cutoverReady, currentSeq, parentShardID)
		if caughtUp {
			s.initializing.Store(false)
		}

		if cutoverReady {
			logger.Info("Split replay caught up to final fence", zap.Uint64("finalSeq", finalSeq))
			return
		}
		if !parent.isActiveSplitReplaySource() && caughtUp {
			s.setSplitReplayState(true, true, true, currentSeq, parentShardID)
			logger.Info("Split replay source is inactive and child is caught up", zap.Uint64("seq", currentSeq))
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
		}
	}
}

func (s *StoreDB) GetSplitDeltaSeq() (uint64, error) {
	return s.coreDB.GetSplitDeltaSeq()
}

func (s *StoreDB) GetSplitDeltaFinalSeq() (uint64, error) {
	return s.coreDB.GetSplitDeltaFinalSeq()
}

func (s *StoreDB) ListSplitDeltaEntriesAfter(afterSeq uint64) ([]*SplitDeltaEntry, error) {
	return s.coreDB.ListSplitDeltaEntriesAfter(afterSeq)
}

// setSplitState updates the in-memory split state. This should only be called after
// the state has been persisted to Pebble (via coreDB.SetSplitState).
// Caller must hold byteRangeMu.Lock().
func (s *StoreDB) setSplitState(state *SplitState) {
	s.splitState = state
}

func (s *StoreDB) setMergeState(state *MergeState) {
	s.mergeState = state
}

func (s *StoreDB) SetMergeState(ctx context.Context, state *MergeState) error {
	return s.proposeMergeStateChange(ctx, state)
}

func (s *StoreDB) proposeMergeStateChange(ctx context.Context, state *MergeState) error {
	kvOp := &Op{}
	kvOp.SetOp(Op_OpSetMergeState)
	kvOp.SetSetMergeState(SetMergeStateOp_builder{
		State: state,
	}.Build())
	return s.syncWriteOp(ctx, kvOp)
}

func (s *StoreDB) Split(ctx context.Context, newShardID uint64, splitKey []byte) error {
	s.byteRangeMu.RLock()
	byteRange := s.byteRange
	currentState := s.splitState
	s.byteRangeMu.RUnlock()

	// Idempotent retry: if the parent range has already been narrowed to splitKey
	// and the active split state matches, the split op was already applied.
	if bytes.Equal(byteRange[1], splitKey) && currentState != nil &&
		currentState.GetPhase() != SplitState_PHASE_NONE &&
		bytes.Equal(currentState.GetSplitKey(), splitKey) &&
		currentState.GetNewShardId() == newShardID {
		return nil
	}

	if !byteRange.Contains(splitKey) {
		return common.NewErrKeyOutOfRange(splitKey, byteRange)
	}

	// Verify split state - should be in PREPARE phase with matching split key
	// If not in PREPARE phase, call PrepareSplit first (backwards compatibility)
	if currentState == nil || currentState.GetPhase() == SplitState_PHASE_NONE {
		// PrepareSplit was not called - do it now for backwards compatibility
		if err := s.PrepareSplit(ctx, splitKey); err != nil {
			return fmt.Errorf("preparing split: %w", err)
		}
		// Refresh state after PrepareSplit
		s.byteRangeMu.RLock()
		currentState = s.splitState
		s.byteRangeMu.RUnlock()
	}

	// Verify split key matches
	if currentState != nil && !bytes.Equal(currentState.GetSplitKey(), splitKey) {
		return fmt.Errorf("split key mismatch: state has %x, called with %x",
			currentState.GetSplitKey(), splitKey)
	}

	// Transition to PHASE_SPLITTING via Raft
	splittingState := SplitState_builder{
		Phase:              SplitState_PHASE_SPLITTING,
		SplitKey:           splitKey,
		NewShardId:         newShardID,
		StartedAtUnixNanos: currentState.GetStartedAtUnixNanos(),
		OriginalRangeEnd:   currentState.GetOriginalRangeEnd(),
	}.Build()

	if err := s.proposeSplitStateChange(ctx, splittingState); err != nil {
		return fmt.Errorf("transitioning to SPLITTING phase: %w", err)
	}

	// Now propose the actual split operation
	if err := s.syncWriteOp(ctx, NewSplitOp(newShardID, splitKey)); err != nil {
		// On failure, we should ideally rollback to PREPARE phase, but for now
		// leave in SPLITTING phase - the reconciler will handle cleanup
		return fmt.Errorf("proposing split op: %w", err)
	}

	return nil
}

func (s *StoreDB) proposeOnlyWriteOp(ctx context.Context, data *Op) error {
	proposed, err := EncodeProto(data)
	if err != nil {
		s.logger.Error("Failed to encode data for proposal", zap.Error(err))
		return fmt.Errorf("encoding data for proposal: %w", err)
	}
	if err := s.proposer.ProposeOnly(ctx, proposed); err != nil {
		return fmt.Errorf("proposing data to raft: %w", err)
	}
	return nil
}

type proposal struct {
	batch *BatchOp
	done  chan error
	ctx   context.Context
}

func (s *StoreDB) proposalEnqueuer(ctx context.Context, batch *BatchOp) error {
	done := make(chan error, 1)
	if err := s.proposalQueue.Enqueue(&inflight.Op{ID: 1, Data: &proposal{ctx: ctx, batch: batch, done: done}}); err != nil {
		return fmt.Errorf("enqueueing proposal: %w", err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// isInSplitOffRange returns true if key >= splitKey (i.e., in the range being split off)
func isInSplitOffRange(key, splitKey []byte) bool {
	if len(splitKey) == 0 {
		return false // no pending split
	}
	return bytes.Compare(key, splitKey) >= 0
}

// ValidateBatchKeys checks that all keys in the batch are within the current
// shard ownership. During active split or merge phases, the shard may
// temporarily accept an additional range or reject a sealed donor range.
func ValidateBatchKeys(
	batch *BatchOp,
	byteRange types.Range,
	splitState *SplitState,
	mergeStates ...*MergeState,
) error {
	var mergeState *MergeState
	if len(mergeStates) > 0 {
		mergeState = mergeStates[0]
	}
	for _, write := range batch.GetWrites() {
		if err := validateKey(write.GetKey(), byteRange, splitState, mergeState); err != nil {
			return err
		}
	}
	for _, del := range batch.GetDeletes() {
		if err := validateKey(del, byteRange, splitState, mergeState); err != nil {
			return err
		}
	}
	for _, transform := range batch.GetTransforms() {
		if err := validateKey(transform.GetKey(), byteRange, splitState, mergeState); err != nil {
			return err
		}
	}
	return nil
}

func validateKey(
	key []byte,
	byteRange types.Range,
	splitState *SplitState,
	mergeState *MergeState,
) error {
	if !isKeyOwnedDuringSplit(key, byteRange, splitState) ||
		!isKeyOwnedDuringMerge(key, byteRange, mergeState) {
		return common.NewErrKeyOutOfRange(key, byteRange)
	}
	return nil
}

func (s *StoreDB) proposalDequeuer(ctx context.Context) error {
	for s.proposalQueue.Dequeue(func(opSet *inflight.OpSet) {
		ops := opSet.Ops()
		mergedData := &BatchOp_builder{}
		doneChans := make([]chan error, 0, len(ops))

		// CRITICAL: Hold RLock from validation through proposal commit.
		// This serializes with Split() which holds Lock during pendingSplitKey
		// and split proposal. This ensures either:
		// 1. Writes are validated+proposed before Split() sets pendingSplitKey
		//    → writes enter Raft before split → applied before split → SUCCESS
		// 2. Split() sets pendingSplitKey before this validation starts
		//    → writes to split-off range fail validation → client retries → SUCCESS
		s.byteRangeMu.RLock()
		byteRange := s.byteRange
		splitState := s.splitState
		mergeState := s.mergeState

		for _, op := range ops {
			data, ok := op.Data.(*proposal)
			if !ok {
				s.logger.Error("Invalid operation data type in proposal dequeuer")
				s.byteRangeMu.RUnlock()
				return
			}
			select {
			case <-data.ctx.Done():
				continue
			default:
			}

			if err := ValidateBatchKeys(data.batch, byteRange, splitState, mergeState); err != nil {
				data.done <- err
				continue
			}

			mergedData.Writes = append(mergedData.Writes, data.batch.GetWrites()...)
			mergedData.Deletes = append(mergedData.Deletes, data.batch.GetDeletes()...)
			mergedData.Transforms = append(mergedData.Transforms, data.batch.GetTransforms()...)
			doneChans = append(doneChans, data.done)
		}

		// Propose while still holding RLock - ensures ordering with Split()
		err := s.proposeOnlyWriteOp(ctx, NewBatchOp(mergedData.Build()))
		s.byteRangeMu.RUnlock()

		for _, ch := range doneChans {
			ch <- err
		}
		opSet.FinishAll(nil /* err */, nil /* result */)
	}) {
	}
	return nil
}

func (s *StoreDB) syncWriteOp(ctx context.Context, data *Op) error {
	id := uuid.New()
	commitDoneC := s.proposer.RegisterCallback(id)
	defer s.proposer.UnregisterCallback(id)

	i := id.String()
	data.SetUuid(i)
	proposed, err := EncodeProto(data)
	if err != nil {
		s.logger.Error("Failed to encode data for proposal", zap.Error(err))
		return fmt.Errorf("encoding data for proposal: %w", err)
	}

	// CRITICAL: Hold RLock from validation through proposal to serialize with Split().
	// This ensures either:
	// 1. Writes are validated+proposed before Split() sets pendingSplitKey
	//    → writes enter Raft before split → applied before split → SUCCESS
	// 2. Split() sets pendingSplitKey before this validation starts
	//    → writes to split-off range fail validation → client retries → SUCCESS
	s.byteRangeMu.RLock()

	if batch := data.GetBatch(); batch != nil {
		if err := ValidateBatchKeys(batch, s.byteRange, s.splitState, s.mergeState); err != nil {
			s.byteRangeMu.RUnlock()
			return err
		}
	}

	// Propose while still holding RLock to ensure ordering with Split()
	err = s.proposer.ProposeOnly(ctx, proposed)
	s.byteRangeMu.RUnlock()

	if err != nil {
		return fmt.Errorf("proposing data to raft: %w", err)
	}

	return s.proposer.WaitForCommit(ctx, commitDoneC)
}

func (s *StoreDB) syncWriteOpWithoutValidation(ctx context.Context, data *Op) error {
	id := uuid.New()
	commitDoneC := s.proposer.RegisterCallback(id)
	defer s.proposer.UnregisterCallback(id)

	i := id.String()
	data.SetUuid(i)
	proposed, err := EncodeProto(data)
	if err != nil {
		s.logger.Error("Failed to encode data for proposal", zap.Error(err))
		return fmt.Errorf("encoding data for proposal: %w", err)
	}

	if err := s.proposer.ProposeOnly(ctx, proposed); err != nil {
		return fmt.Errorf("proposing data to raft: %w", err)
	}

	return s.proposer.WaitForCommit(ctx, commitDoneC)
}

func (s *StoreDB) applyOpBatch(
	ctx context.Context,
	writes [][2][]byte,
	deletes [][]byte,
	syncLevel Op_SyncLevel,
	timestamp uint64,
) error {
	if len(writes) == 0 && len(deletes) == 0 {
		return nil
	}
	if timestamp != 0 {
		ctx = storeutils.WithTimestamp(ctx, timestamp)
	}
	if err := s.coreDB.Batch(ctx, writes, deletes, syncLevel); err != nil {
		return fmt.Errorf("applying batch: %w", err)
	}
	return nil
}

// resolveTransforms resolves transform operations to writes
func (s *StoreDB) resolveTransforms(ctx context.Context, transforms []*Transform) ([][2][]byte, error) {
	resolved := make([][2][]byte, 0, len(transforms))

	for _, transform := range transforms {
		// Read current document
		existing, err := s.coreDB.Get(ctx, transform.GetKey())
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// Handle upsert flag
				if !transform.GetUpsert() {
					// Not an upsert, skip this transform
					s.logger.Debug("Skipping transform for missing key",
						zap.Binary("key", transform.GetKey()))
					continue
				}
				// Upsert: start with empty document
				existing = make(map[string]any)
			} else {
				return nil, fmt.Errorf("reading key for transform: %w", err)
			}
		}

		// Apply operations in sequence
		modified := existing
		for _, op := range transform.GetOperations() {
			modified, err = ApplyTransformOp(modified, op)
			if err != nil {
				s.logger.Warn("Failed to apply transform operation",
					zap.Error(err),
					zap.Binary("key", transform.GetKey()),
					zap.String("path", op.GetPath()))
				continue
			}
		}

		// Marshal result
		modifiedBytes, err := json.Marshal(modified)
		if err != nil {
			return nil, fmt.Errorf("marshalling transformed doc: %w", err)
		}

		resolved = append(resolved, [2][]byte{transform.GetKey(), modifiedBytes})
	}

	return resolved, nil
}

func (s *StoreDB) applyOpDeleteIndex(_ context.Context, deleteIndex *DeleteIndexOp) error {
	if deleteIndex == nil {
		return errors.New("delete index operation data is nil")
	}
	return s.coreDB.DeleteIndex(deleteIndex.GetName())
}

func (s *StoreDB) applyOpAddIndex(_ context.Context, addIndex *AddIndexOp) error {
	if addIndex == nil {
		return errors.New("add index operation data is nil")
	}

	indexConfig := indexes.IndexConfig{
		Name: addIndex.GetName(),
		Type: indexes.IndexType(addIndex.GetType()),
	}
	if err := indexConfig.UnmarshalJSON(addIndex.GetConfig()); err != nil {
		return fmt.Errorf("unmarshalling index config JSON: %w", err)
	}
	return s.coreDB.AddIndex(indexConfig)
}

func (s *StoreDB) applyOpSetRange(_ context.Context, setRange *SetRangeOp) error {
	if setRange == nil {
		return errors.New("set range operation data is nil")
	}
	s.byteRangeMu.Lock()
	s.byteRange = [2][]byte{setRange.GetStartKey(), setRange.GetEndKey()}
	byteRange := s.byteRange
	s.byteRangeMu.Unlock()
	return s.coreDB.SetRange(byteRange)
}

func (s *StoreDB) applyOpUpdateSchema(_ context.Context, updateSchema *UpdateSchemaOp) error {
	if updateSchema == nil || updateSchema.GetSchema() == nil {
		return errors.New("update schema operation data is nil")
	}
	schema := &schema.TableSchema{}
	if err := json.Unmarshal(updateSchema.GetSchema(), schema); err != nil {
		return fmt.Errorf("unmarshalling schema: %w", err)
	}

	s.schema = schema
	return s.coreDB.UpdateSchema(s.schema)
}

func (s *StoreDB) applyOpBackup(_ context.Context, backup *BackupOp) error {
	startTime := time.Now()
	if backup == nil {
		return errors.New("backup operation data is nil")
	}
	backupID := backup.GetBackupId()
	location := backup.GetLocation()
	archiveFile, err := s.snapStore.Path(backupID)
	if err != nil {
		return fmt.Errorf("getting archive file path: %w", err)
	}
	backupToBlobStore := false
	if location != "" {
		if strings.HasPrefix(location, "s3://") {
			backupToBlobStore = true
		} else {
			archiveFile = filepath.Join(location, fmt.Sprintf("%v.tar.zst", backupID))
		}
	}

	s.logger.Info("Starting backup operation",
		zap.String("backupID", backupID),
		zap.String("location", location),
		zap.String("archiveFile", archiveFile),
		zap.Bool("backupToBlobStore", backupToBlobStore),
	)

	// Check if backup already exists
	checkTime := time.Now()
	if _, err := os.Stat(archiveFile); err == nil {
		if backupToBlobStore {
			s.logger.Debug("Backup exists locally, writing to blob store",
				zap.String("backupID", backupID),
				zap.Duration("checkDuration", time.Since(checkTime)),
			)
			blobTime := time.Now()
			// Don't timeout the blob store write if the client times out, it might take a while
			err := WriteBackupToBlobStore(context.Background(), location, archiveFile, &s.antflyConfig.Storage.S3)
			s.logger.Info("Blob store write completed",
				zap.String("backupID", backupID),
				zap.Duration("blobWriteDuration", time.Since(blobTime)),
				zap.Duration("totalDuration", time.Since(startTime)),
				zap.Error(err),
			)
			return err
		}
		s.logger.Warn("Backup already exists, skipping",
			zap.String("backupID", backupID),
			zap.Duration("checkDuration", time.Since(checkTime)),
			zap.Duration("totalDuration", time.Since(startTime)),
		)
		return nil
	}

	// Remove any partial backup file
	_ = os.RemoveAll(archiveFile)

	// Create the snapshot
	snapshotTime := time.Now()
	s.logger.Debug("Creating DB snapshot",
		zap.String("backupID", backupID),
		zap.Duration("setupDuration", time.Since(startTime)),
	)
	if err := s.CreateDBSnapshot(backupID); err != nil {
		s.logger.Error(
			"Failed to create db snapshot",
			zap.String("backupID", backupID),
			zap.Duration("snapshotDuration", time.Since(snapshotTime)),
			zap.Duration("totalDuration", time.Since(startTime)),
			zap.Error(err),
		)
		return fmt.Errorf("creating db snapshot: %w", err)
	}
	snapshotDuration := time.Since(snapshotTime)

	// Write to blob store if needed
	if backupToBlobStore {
		blobTime := time.Now()
		s.logger.Debug("Writing backup to blob store",
			zap.String("backupID", backupID),
			zap.String("location", location),
		)
		// Don't timeout the blob store write if the client times out, it might take a while
		err := WriteBackupToBlobStore(context.Background(), location, archiveFile, &s.antflyConfig.Storage.S3)
		blobDuration := time.Since(blobTime)
		s.logger.Info("Backup operation completed",
			zap.String("backupID", backupID),
			zap.Duration("snapshotDuration", snapshotDuration),
			zap.Duration("blobWriteDuration", blobDuration),
			zap.Duration("totalDuration", time.Since(startTime)),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info("Backup operation completed",
		zap.String("backupID", backupID),
		zap.Duration("snapshotDuration", snapshotDuration),
		zap.Duration("totalDuration", time.Since(startTime)),
	)
	return nil
}

func (s *StoreDB) applyOpSplit(_ context.Context, split *SplitOp) error {
	if split == nil {
		return errors.New("split operation data is nil")
	}
	medianKey := split.GetSplitKey()

	// Validate split key is within our range. This also prevents re-splitting.
	s.byteRangeMu.Lock()
	if !s.byteRange.Contains(medianKey) {
		currentRange := s.byteRange
		s.byteRangeMu.Unlock()
		s.logger.Debug(
			"Ignoring split operation outside of range",
			zap.String("key", string(medianKey)),
			zap.Stringer("range", currentRange),
		)
		return fmt.Errorf("split key %s is not within the range %s", medianKey, currentRange)
	}

	// Capture original range before updating
	oldByteRange := s.byteRange
	newRange := types.Range{s.byteRange[0], medianKey}

	// CRITICAL FIX: Update byteRange BEFORE creating the archive to prevent data loss.
	// Without this, writes that arrive between archive creation and FinalizeSplit would:
	// 1. Be accepted by the parent (because byteRange still includes split-off range)
	// 2. Be deleted when FinalizeSplit runs
	// 3. Never exist in the new shard (archive was created before these writes)
	// By updating byteRange first, new proposals to the split-off range are rejected
	// at proposal time, preventing data loss.
	//
	// Note: On the leader, Split() set pendingSplitKey to reject writes during the
	// split proposal. Now that we're applying, update byteRange and clear pendingSplitKey.
	s.byteRange = newRange
	s.pendingSplitKey = nil // Clear pending split - byteRange now enforces the boundary
	s.byteRangeMu.Unlock()

	s.splitting.Store(true)
	defer s.splitting.Store(false)
	shardID, nodeID, err := common.ParseStorageDBDir(s.dbDir)
	if err != nil {
		s.logger.Fatal("could not parse dbDir", zap.String("dbDir", s.dbDir), zap.Error(err))
	}
	newShardID := types.ID(split.GetNewShardId())
	dbDir1 := common.StorageDBDir(s.dataDir, shardID, nodeID) + "-staging-remain"
	// Place splitoff staging dir under PARENT shard's directory to avoid race condition
	// with the new shard's initialization which runs concurrently on the same node.
	// Previously this was under newShardID's directory which could be affected by
	// new shard startup/cleanup operations.
	dbDir2 := common.StorageDBDir(s.dataDir, shardID, nodeID) + "-staging-splitoff-" + newShardID.String()
	_ = os.RemoveAll(dbDir1)
	_ = os.RemoveAll(dbDir2)

	// FIXME (ajr) What cleans these up after the snapshot makes the backups irrelevant?
	// for that matter what cleans up the snapshots?
	// TODO (ajr) ^ Verify this is handled in the raft code after the snapshot is made

	// Persist to coreDB and Pebble for crash recovery.
	// Fatal on failure: in-memory byteRange was already updated above, so a partial
	// Pebble write would leave inconsistent state that cannot be safely rolled back.
	if err := s.coreDB.SetRange(newRange); err != nil {
		s.logger.Fatal("failed to persist byte range during split", zap.Error(err))
	}
	replayFenceSeq, err := s.coreDB.GetSplitDeltaSeq()
	if err != nil {
		s.logger.Fatal("failed to load split delta sequence for split archive", zap.Error(err))
	}

	s.logger.Info("SPLIT_ARCHIVE: Updated byte range before archive creation",
		zap.Stringer("oldRange", oldByteRange),
		zap.Stringer("newRange", newRange),
		zap.Binary("splitKey", medianKey),
		zap.Stringer("splitOffRange", types.Range{medianKey, oldByteRange[1]}))

	t := time.Now()
	s.logger.Info("SPLIT_ARCHIVE: Creating archive for split-off shard",
		zap.Stringer("splitOffRange", types.Range{medianKey, oldByteRange[1]}),
		zap.Stringer("newShardID", newShardID))
	// Use prepareOnly=true to keep the split-off data in Pebble for the archive.
	// The byteRange has already been updated above, so new writes to the split-off
	// range will be rejected. The data remains in Pebble until FinalizeSplit deletes it.
	if err := s.coreDB.Split(oldByteRange, medianKey, dbDir1, dbDir2, true); err != nil {
		return fmt.Errorf("failed to split db: %w", err)
	}
	splitTime := time.Since(t)
	splitPhaseDurationSeconds.WithLabelValues("apply_split_prepare").Observe(splitTime.Seconds())
	s.logger.Info("SPLIT_ARCHIVE: Split completed, data staged for archive",
		zap.Duration("splitDuration", splitTime))
	t = time.Now()
	// We only care about the archiveFile (dbDir2), dbDir1 is cleaned up by Split
	defer func() {
		_ = os.RemoveAll(dbDir2)
	}()
	// Making this in the pebble staging directory so that we know it's cleaned up
	// if there's an error
	newSnapDir := common.SnapDir(s.dataDir, types.ID(newShardID), nodeID)
	tmpArchiveFile := filepath.Join(newSnapDir, "antfly-split-"+uuid.NewString()+".tar.zst")
	if err := os.MkdirAll(newSnapDir+"/", os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return fmt.Errorf("creating snapshot directory %s: %w", newSnapDir, err)
	}
	defer func() {
		_ = os.Remove(tmpArchiveFile)
	}()

	// Create combined staging directory for Archive Format v2.
	// This includes both pebble data and pre-built indexes (if shadow IndexManager exists).
	combinedStagingDir := dbDir2 + "-archive-staging"
	defer func() {
		_ = os.RemoveAll(combinedStagingDir)
	}()

	// Create the staging directory structure
	stagingPebbleDir := filepath.Join(combinedStagingDir, "pebble")
	if err := os.MkdirAll(combinedStagingDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return fmt.Errorf("creating archive staging directory: %w", err)
	}

	// Move pebble data to staging/pebble/
	pebbleDir := filepath.Join(dbDir2, "pebble")
	if _, err := os.Stat(pebbleDir); err != nil {
		s.logger.Error("Split staging directory missing before archive",
			zap.String("pebbleDir", pebbleDir),
			zap.String("dbDir2", dbDir2),
			zap.Error(err))
		return fmt.Errorf("split staging directory missing: %w", err)
	}
	if err := os.Rename(pebbleDir, stagingPebbleDir); err != nil {
		return fmt.Errorf("moving pebble to staging: %w", err)
	}

	// Copy shadow indexes to staging/indexes/ if shadow IndexManager exists.
	// This enables the new shard to start with pre-built indexes, avoiding backfill.
	shadowIndexDir := s.coreDB.GetShadowIndexDir()
	if shadowIndexDir != "" {
		if _, err := os.Stat(shadowIndexDir); err == nil {
			stagingIndexDir := filepath.Join(combinedStagingDir, "indexes")
			if err := common.CopyDir(shadowIndexDir, stagingIndexDir); err != nil {
				s.logger.Warn("Failed to copy shadow indexes to archive, new shard will rebuild",
					zap.String("shadowIndexDir", shadowIndexDir),
					zap.Error(err))
			} else {
				s.logger.Info("SPLIT_ARCHIVE: Included pre-built indexes in archive",
					zap.String("shadowIndexDir", shadowIndexDir),
					zap.String("stagingIndexDir", stagingIndexDir))
			}
		}
	}

	// Archive the combined staging directory (contains pebble/ and optionally indexes/)
	archiveInfo, err := common.CreateArchiveWithOptions(combinedStagingDir, tmpArchiveFile, common.CreateArchiveOptions{
		ArchiveType: common.ArchiveZstd,
		Metadata: &common.ArchiveMetadata{
			Split: &common.SplitMetadata{
				ParentShardID:  shardID.String(),
				ReplayFenceSeq: replayFenceSeq,
				SplitKey:       string(medianKey),
			},
		},
	})
	if err != nil {
		s.logger.Error("Failed to archive split", zap.Error(err))
		return fmt.Errorf("archiving split staging directory: %w", err)
	}
	s.logger.Debug(
		"Moving archive for executed split",
		zap.String("dbDir", dbDir2),
		zap.Int64("archiveFileSize", archiveInfo.Size()),
		zap.String("newSnapDir", newSnapDir),
	)
	archiveFile := filepath.Join(
		newSnapDir,
		fmt.Sprintf("%v.tar.zst", common.SplitArchive(newShardID)),
	)
	// Only move if the target doesn't exist or has a different size, could happen if the
	// the other shard fetches a snapshot from another node before we move it
	if statInfo, err := os.Stat(archiveFile); err != nil || archiveInfo.Size() != statInfo.Size() {
		if err := os.Rename(tmpArchiveFile, archiveFile); err != nil {
			return fmt.Errorf("renaming split archive from %s to %s: %w", tmpArchiveFile, archiveFile, err)
		}
	}
	archiveTime := time.Since(t)
	splitPhaseDurationSeconds.WithLabelValues("apply_split_archive").Observe(archiveTime.Seconds())
	s.logger.Info("SPLIT_ARCHIVE: Created snapshot archive for split-off shard",
		zap.Int64("size", archiveInfo.Size()),
		zap.String("archiveFile", archiveFile),
		zap.Stringer("newShardID", newShardID),
		zap.Duration("archiveDuration", archiveTime))

	// The byte range was already updated above (before archive creation) to prevent
	// data loss. New writes to the split-off range are now rejected at proposal time.
	// FinalizeSplit will delete the split-off data from Pebble once the new shard is ready.

	s.byteRangeMu.RLock()
	logRange := s.byteRange
	s.byteRangeMu.RUnlock()
	s.logger.Info("Applied split prepare phase, waiting for new shard to be ready",
		zap.Duration("splitTime", splitTime),
		zap.Duration("archiveTime", archiveTime),
		zap.Stringer("newByteRange", logRange),
		zap.Stringer("oldByteRange", oldByteRange),
		zap.Binary("splitKey", medianKey))
	return nil
}

// FinalizeSplit proposes a finalize split operation that completes the two-phase split.
// This should be called when the new shard is ready (has HasSnapshot=true).
// It deletes the split-off data and updates the byte range.
func (s *StoreDB) FinalizeSplit(ctx context.Context, newRangeEnd []byte) error {
	return s.syncWriteOp(ctx, newFinalizeSplitOp(newRangeEnd))
}

// RollbackSplit aborts a split operation and restores the shard to its pre-split state.
// This clears the split state, restores the original byte range, and discards the shadow index.
// It should be called when a split times out or encounters an unrecoverable error.
func (s *StoreDB) RollbackSplit(ctx context.Context) error {
	s.byteRangeMu.RLock()
	currentState := s.splitState
	s.byteRangeMu.RUnlock()

	if currentState == nil || currentState.GetPhase() == SplitState_PHASE_NONE {
		// No split in progress - nothing to rollback
		s.logger.Debug("RollbackSplit called with no split in progress")
		return nil
	}

	s.logger.Info("Rolling back split operation",
		zap.String("currentPhase", currentState.GetPhase().String()),
		zap.Binary("splitKey", currentState.GetSplitKey()),
		zap.Binary("originalRangeEnd", currentState.GetOriginalRangeEnd()))

	// First, transition to PHASE_ROLLING_BACK if not already there.
	// This triggers cleanup on all replicas while preserving the original range end.
	if currentState.GetPhase() != SplitState_PHASE_ROLLING_BACK {
		rollbackState := SplitState_builder{
			Phase:              SplitState_PHASE_ROLLING_BACK,
			SplitKey:           currentState.GetSplitKey(),
			NewShardId:         currentState.GetNewShardId(),
			StartedAtUnixNanos: currentState.GetStartedAtUnixNanos(),
			OriginalRangeEnd:   currentState.GetOriginalRangeEnd(),
		}.Build()

		if err := s.proposeSplitStateChange(ctx, rollbackState); err != nil {
			return fmt.Errorf("transitioning to ROLLING_BACK phase: %w", err)
		}
	}

	// Then, clear the split state entirely.
	// The apply function will restore the byte range using the previous state's originalRangeEnd.
	if err := s.proposeSplitStateChange(ctx, nil); err != nil {
		return fmt.Errorf("clearing split state: %w", err)
	}

	s.logger.Info("Split rollback completed successfully")
	return nil
}

func newFinalizeSplitOp(newRangeEnd []byte) *Op {
	kvOp := &Op{}
	kvOp.SetOp(Op_OpFinalizeSplit)
	kvOp.SetFinalizeSplit(FinalizeSplitOp_builder{
		NewRangeEnd: newRangeEnd,
	}.Build())
	return kvOp
}

func newFinalizeMergeOp(newRange types.Range) *Op {
	kvOp := &Op{}
	kvOp.SetOp(Op_OpFinalizeMerge)
	kvOp.SetFinalizeMerge(FinalizeMergeOp_builder{
		NewRangeStart: newRange[0],
		NewRangeEnd:   newRange[1],
	}.Build())
	return kvOp
}

func (s *StoreDB) FinalizeMerge(ctx context.Context, newRange types.Range) error {
	return s.syncWriteOp(ctx, newFinalizeMergeOp(newRange))
}

func (s *StoreDB) waitForLocalSplitChildReplay(newShardID types.ID, targetSeq uint64) error {
	if targetSeq == 0 || s.localSplitSourceLookup == nil {
		return nil
	}

	clk := s.clockOrReal()
	deadline := clk.Now().Add(15 * time.Second)
	ticker := clk.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		child := s.localSplitSourceLookup(newShardID)
		if child != nil {
			required, caughtUp, cutoverReady, seq, _ := child.SplitReplayInfo()
			if !required || (cutoverReady && seq >= targetSeq) || (targetSeq == 0 && caughtUp) {
				return nil
			}
		}
		if clk.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for local split child %s to replay through seq %d", newShardID, targetSeq)
		}
		<-ticker.C()
	}
}

func (s *StoreDB) applyOpFinalizeSplit(_ context.Context, finalize *FinalizeSplitOp) error {
	finalizeStart := time.Now()
	defer func() {
		splitPhaseDurationSeconds.WithLabelValues("apply_finalize_total").Observe(time.Since(finalizeStart).Seconds())
	}()
	if finalize == nil {
		return errors.New("finalize split operation data is nil")
	}
	newRangeEnd := finalize.GetNewRangeEnd()
	if len(newRangeEnd) == 0 {
		return errors.New("finalize split: new range end is empty")
	}

	// The byte range was already updated in applyOpSplit to [start, splitKey).
	// Verify that newRangeEnd matches our current range end (the split key).
	// This ensures consistency between the prepare and finalize phases.
	s.byteRangeMu.RLock()
	currentRange := s.byteRange
	s.byteRangeMu.RUnlock()

	if !bytes.Equal(newRangeEnd, currentRange[1]) {
		s.logger.Warn(
			"Finalize split range end mismatch - byteRange may have been updated in applyOpSplit",
			zap.Binary("requestedRangeEnd", newRangeEnd),
			zap.Stringer("currentRange", currentRange),
		)
		// This can happen if the byteRange was already correctly set in applyOpSplit.
		// Continue with the current range rather than failing.
	}

	s.byteRangeMu.RLock()
	currentState := s.splitState
	s.byteRangeMu.RUnlock()
	if currentState == nil {
		return errors.New("finalize split: no split state available")
	}

	finalSeq, err := s.coreDB.GetSplitDeltaFinalSeq()
	if err != nil {
		return fmt.Errorf("loading split delta final sequence for finalize: %w", err)
	}
	if finalSeq == 0 {
		finalSeq, err = s.coreDB.GetSplitDeltaSeq()
		if err != nil {
			return fmt.Errorf("loading split delta sequence for finalize: %w", err)
		}
		if err := s.coreDB.SetSplitDeltaFinalSeq(finalSeq); err != nil {
			return fmt.Errorf("persisting split delta final seq: %w", err)
		}
	}

	if currentState.GetPhase() != SplitState_PHASE_FINALIZING {
		finalizingState := SplitState_builder{
			Phase:              SplitState_PHASE_FINALIZING,
			SplitKey:           currentState.GetSplitKey(),
			NewShardId:         currentState.GetNewShardId(),
			StartedAtUnixNanos: currentState.GetStartedAtUnixNanos(),
			OriginalRangeEnd:   currentState.GetOriginalRangeEnd(),
		}.Build()
		if err := s.applyOpSetSplitState(context.Background(), SetSplitStateOp_builder{
			State: finalizingState,
		}.Build()); err != nil {
			return fmt.Errorf("persisting finalizing split state: %w", err)
		}
	}

	newShardID := types.ID(currentState.GetNewShardId())
	if err := s.waitForLocalSplitChildReplay(newShardID, finalSeq); err != nil {
		return err
	}

	// Use the current byteRange (already set in applyOpSplit) for the finalize operation.
	// This deletes the split-off data from Pebble only after the local child replica
	// has caught up through the persisted final split-delta fence.
	s.logger.Info("Finalizing split: deleting split-off data from Pebble",
		zap.Stringer("byteRange", currentRange),
		zap.Stringer("newShardID", newShardID),
		zap.Uint64("splitDeltaFinalSeq", finalSeq))

	deleteStart := time.Now()
	if err := s.coreDB.FinalizeSplit(currentRange); err != nil {
		s.logger.Fatal("failed to finalize split", zap.Error(err))
	}
	splitPhaseDurationSeconds.WithLabelValues("apply_finalize_delete").Observe(time.Since(deleteStart).Seconds())

	// Clear the Raft-replicated split state now that finalization is complete.
	// This also clears pendingSplitKey (redundant since byteRange enforces boundary,
	// but keeps the state machine clean).
	if err := s.coreDB.ClearSplitState(); err != nil {
		s.logger.Error("failed to clear split state after finalize", zap.Error(err))
		// Don't fail the finalize - the split data is already deleted
	}
	s.byteRangeMu.Lock()
	s.setSplitState(nil)
	s.pendingSplitKey = nil
	s.byteRangeMu.Unlock()
	if err := s.coreDB.ClearSplitDeltaEntries(); err != nil {
		s.logger.Warn("failed to clear split delta entries after finalize", zap.Error(err))
	}

	s.logger.Info("Split finalized successfully - split-off data deleted",
		zap.Stringer("byteRange", currentRange))
	return nil
}

// applyOpSetSplitState handles the Raft-committed SetSplitStateOp.
// It persists the split state to Pebble and updates the in-memory state.
// When transitioning to PREPARE or SPLITTING phases, it also sets pendingSplitKey
// to reject writes in the split-off range on all replicas.
// Additionally, it manages the shadow IndexManager lifecycle:
//   - PHASE_PREPARE: Creates shadow IndexManager for the split-off range
//   - PHASE_FINALIZING/PHASE_ROLLING_BACK: Closes the shadow IndexManager
//   - PHASE_ROLLING_BACK or clear: Restores the original byte range
func (s *StoreDB) applyOpSetSplitState(_ context.Context, op *SetSplitStateOp) error {
	if op == nil {
		return errors.New("set split state operation data is nil")
	}

	state := op.GetState()

	// Persist to Pebble and update in-memory state
	if err := s.coreDB.SetSplitState(state); err != nil {
		return fmt.Errorf("persisting split state: %w", err)
	}

	// Update storeDB's in-memory reference and pendingSplitKey
	s.byteRangeMu.Lock()
	previousState := s.splitState
	s.setSplitState(state)

	// Handle byte range restoration during rollback:
	// - When entering PHASE_ROLLING_BACK: Restore using the state's originalRangeEnd
	// - When clearing state (nil): Restore using the previous state's originalRangeEnd
	// This ensures the shard returns to serving the full original range.
	if state != nil && state.GetPhase() == SplitState_PHASE_ROLLING_BACK {
		if len(state.GetOriginalRangeEnd()) > 0 {
			s.byteRange = [2][]byte{s.byteRange[0], state.GetOriginalRangeEnd()}
			s.logger.Info("Restored byte range during rollback",
				zap.Stringer("byteRange", s.byteRange))
		}
	} else if state == nil && previousState != nil {
		// Clearing state - restore byte range if we have originalRangeEnd
		if len(previousState.GetOriginalRangeEnd()) > 0 {
			s.byteRange = [2][]byte{s.byteRange[0], previousState.GetOriginalRangeEnd()}
			s.logger.Info("Restored byte range on state clear",
				zap.Stringer("byteRange", s.byteRange))
		}
	}

	// Sync pendingSplitKey with split state for write rejection on all replicas.
	// This ensures that even after a leadership change, all replicas will reject
	// writes to the split-off range during PREPARE and SPLITTING phases.
	if state == nil {
		s.pendingSplitKey = nil
	} else {
		switch state.GetPhase() {
		case SplitState_PHASE_PREPARE, SplitState_PHASE_SPLITTING:
			s.pendingSplitKey = state.GetSplitKey()
		case SplitState_PHASE_NONE, SplitState_PHASE_FINALIZING, SplitState_PHASE_ROLLING_BACK:
			s.pendingSplitKey = nil
		}
	}
	s.byteRangeMu.Unlock()

	// Manage shadow IndexManager lifecycle based on phase transitions.
	// This runs on all replicas to ensure consistency.
	if state != nil {
		switch state.GetPhase() {
		case SplitState_PHASE_PREPARE:
			if err := s.coreDB.ClearSplitDeltaEntries(); err != nil {
				s.logger.Warn("Failed to clear previous split delta entries on PREPARE", zap.Error(err))
			}
			// Create shadow IndexManager for the split-off range [splitKey, originalRangeEnd)
			if err := s.coreDB.CreateShadowIndexManager(
				state.GetSplitKey(),
				state.GetOriginalRangeEnd(),
			); err != nil {
				s.logger.Error("Failed to create shadow IndexManager",
					zap.Error(err),
					zap.Binary("splitKey", state.GetSplitKey()))
				// Don't fail the apply - shadow creation failure shouldn't block the split
				// The split can still proceed, but dual-writes will only go to primary
			}
		case SplitState_PHASE_FINALIZING, SplitState_PHASE_ROLLING_BACK:
			// Close shadow IndexManager - either split completed or was rolled back
			if err := s.coreDB.CloseShadowIndexManager(); err != nil {
				s.logger.Error("Failed to close shadow IndexManager", zap.Error(err))
				// Don't fail the apply - shadow cleanup failure is non-fatal
			}
			if state.GetPhase() == SplitState_PHASE_ROLLING_BACK {
				if err := s.coreDB.ClearSplitDeltaEntries(); err != nil {
					s.logger.Warn("Failed to clear split delta entries on rollback", zap.Error(err))
				}
			}
		}
	} else {
		// Split state cleared - ensure shadow is closed
		if err := s.coreDB.CloseShadowIndexManager(); err != nil {
			s.logger.Error("Failed to close shadow IndexManager on state clear", zap.Error(err))
		}
		if err := s.coreDB.ClearSplitDeltaFinalSeq(); err != nil {
			s.logger.Warn("Failed to clear split delta final sequence on state clear", zap.Error(err))
		}
	}

	if state == nil {
		s.logger.Info("Split state cleared via Raft")
	} else {
		s.logger.Info("Split state updated via Raft",
			zap.String("phase", state.GetPhase().String()),
			zap.Binary("splitKey", state.GetSplitKey()),
			zap.Uint64("newShardId", state.GetNewShardId()))
	}

	return nil
}

func (s *StoreDB) applyOpSetMergeState(_ context.Context, op *SetMergeStateOp) error {
	if op == nil {
		return errors.New("set merge state operation data is nil")
	}
	state := op.GetState()
	if err := s.coreDB.SetMergeState(state); err != nil {
		return fmt.Errorf("persisting merge state: %w", err)
	}

	s.byteRangeMu.Lock()
	s.setMergeState(state)
	s.byteRangeMu.Unlock()

	switch {
	case state == nil:
		if err := s.coreDB.ClearMergeDeltaFinalSeq(); err != nil {
			s.logger.Warn("Failed to clear merge delta final seq on state clear", zap.Error(err))
		}
	case state.GetPhase() == MergeState_PHASE_PREPARE && state.GetCaptureDeltas():
		if err := s.coreDB.ClearMergeDeltaEntries(); err != nil {
			s.logger.Warn("Failed to clear merge delta entries on PREPARE", zap.Error(err))
		}
	case state.GetPhase() == MergeState_PHASE_ROLLING_BACK && state.GetAcceptDonorRange():
		deleteRange := types.Range{state.GetDonorRangeStart(), state.GetDonorRangeEnd()}
		if err := s.coreDB.DeleteDataRange(deleteRange); err != nil {
			return fmt.Errorf("rolling back merge receiver range %s: %w", deleteRange, err)
		}
	}

	if state == nil {
		s.logger.Info("Merge state cleared via Raft")
	} else {
		s.logger.Info("Merge state updated via Raft",
			zap.String("phase", state.GetPhase().String()),
			zap.Uint64("donorShardId", state.GetDonorShardId()),
			zap.Uint64("receiverShardId", state.GetReceiverShardId()),
			zap.Bool("captureDeltas", state.GetCaptureDeltas()),
			zap.Bool("acceptDonorRange", state.GetAcceptDonorRange()),
			zap.Uint64("replaySeq", state.GetReplaySeq()),
			zap.Uint64("finalSeq", state.GetFinalSeq()))
	}
	return nil
}

func (s *StoreDB) applyOpFinalizeMerge(_ context.Context, finalize *FinalizeMergeOp) error {
	if finalize == nil {
		return errors.New("finalize merge operation data is nil")
	}
	newRange := types.Range{finalize.GetNewRangeStart(), finalize.GetNewRangeEnd()}
	if err := s.coreDB.SetRange(newRange); err != nil {
		return fmt.Errorf("persisting merge receiver range: %w", err)
	}
	if err := s.coreDB.ClearMergeState(); err != nil {
		return fmt.Errorf("clearing merge state after finalize: %w", err)
	}
	if err := s.coreDB.ClearMergeDeltaEntries(); err != nil {
		s.logger.Warn("Failed to clear merge delta entries after finalize", zap.Error(err))
	}
	s.byteRangeMu.Lock()
	s.byteRange = newRange
	s.setMergeState(nil)
	s.byteRangeMu.Unlock()
	return nil
}

func (s *StoreDB) maybeCallback(id uuid.UUID, err error) {
	s.proposer.NotifyCommit(id, err)
}

func (s *StoreDB) readCommits(
	ctx context.Context,
	commitC <-chan *raft.Commit,
	errorC <-chan error,
) {
	writes := make([][2][]byte, 0, 128)
	deletes := make([][]byte, 0, 128)
	transforms := make([]*Transform, 0, 128)
	uuids := make([]uuid.UUID, 0, 128)
	syncLevels := make([]Op_SyncLevel, 0, 128)
	var batchTimestamp uint64
	flushBatch := func() {
		if len(writes) == 0 && len(deletes) == 0 && len(transforms) == 0 {
			return
		}

		// Resolve transforms to writes first
		if len(transforms) > 0 {
			resolvedWrites, err := s.resolveTransforms(ctx, transforms)
			if err != nil {
				s.logger.Error("Failed to resolve transforms", zap.Error(err))
			} else {
				writes = append(writes, resolvedWrites...)
			}
		}

		// Determine effective sync level based on callbacks
		effectiveSyncLevel := Op_SyncLevelPropose
		for i, id := range uuids {
			if s.proposer.HasCallback(id) {
				// Only honor sync level if someone is waiting (original requester)
				if syncLevels[i] > effectiveSyncLevel {
					effectiveSyncLevel = syncLevels[i]
				}
			}
		}

		err := s.applyOpBatch(ctx, writes, deletes, effectiveSyncLevel, batchTimestamp)
		for _, uuid := range uuids {
			s.maybeCallback(uuid, err)
		}
		writes = writes[:0]
		deletes = deletes[:0]
		transforms = transforms[:0]
		uuids = uuids[:0]
		syncLevels = syncLevels[:0]
		batchTimestamp = 0
	}
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			s.logger.Info("Received nil commit, attempting to load snapshot")
			if err := s.loadAndRecoverFromPersistentSnapshot(ctx); err != nil {
				s.logger.Panic(
					"Failed to load and recover from persistent snapshot after nil commit",
					zap.Error(err),
				)
			}
			continue
		}

		dataKv := &Op{}
		for _, data := range commit.Data {
			dataKv.Reset()
			if err := DecodeProto(data, dataKv); err != nil {
				// Decide how to handle decoding errors - log, skip, fatal?
				s.logger.Error("could not decode commit data, skipping entry", zap.Error(err))
				continue // Skip this data entry
			}

			var id uuid.UUID
			if dataKv.GetUuid() != "" {
				var err error
				id, err = uuid.Parse(dataKv.GetUuid())
				if err != nil {
					s.logger.Error("could not parse UUID from commit data", zap.Error(err))
					continue
				}
			}

			switch dataKv.GetOp() {
			case Op_OpBatch:
				if batch := dataKv.GetBatch(); batch != nil {
					if (len(writes) > 0 || len(deletes) > 0 || len(transforms) > 0) &&
						batch.GetTimestamp() != batchTimestamp {
						flushBatch()
					}

					// TODO (ajr) Revisit and optimize this logic, there's definitely repetitive looping going on here

					// Handle deletes removing writes and transforms
					if len(batch.GetDeletes()) > 0 {
						writes = slices.DeleteFunc(writes, func(e [2][]byte) bool {
							return slices.ContainsFunc(batch.GetDeletes(), func(e2 []byte) bool {
								return bytes.Equal(e[0], e2)
							})
						})
						// Deletes also remove transforms
						transforms = slices.DeleteFunc(transforms, func(t *Transform) bool {
							return slices.ContainsFunc(batch.GetDeletes(), func(d []byte) bool {
								return bytes.Equal(t.GetKey(), d)
							})
						})
					}

					// Handle writes removing deletes and transforms
					if len(batch.GetWrites()) > 0 {
						deletes = slices.DeleteFunc(deletes, func(e []byte) bool {
							return slices.ContainsFunc(batch.GetWrites(), func(e2 *Write) bool {
								return bytes.Equal(e, e2.GetKey())
							})
						})
						// Writes also remove transforms (write overwrites transform)
						transforms = slices.DeleteFunc(transforms, func(t *Transform) bool {
							return slices.ContainsFunc(batch.GetWrites(), func(w *Write) bool {
								return bytes.Equal(t.GetKey(), w.GetKey())
							})
						})
					}

					// Handle transforms
					if len(batch.GetTransforms()) > 0 {
						for _, newTransform := range batch.GetTransforms() {
							key := newTransform.GetKey()

							// Remove any pending writes for this key
							writes = slices.DeleteFunc(writes, func(w [2][]byte) bool {
								return bytes.Equal(w[0], key)
							})

							// Remove any pending deletes for this key
							deletes = slices.DeleteFunc(deletes, func(d []byte) bool {
								return bytes.Equal(d, key)
							})

							// Merge with existing transform for same key
							existingIdx := slices.IndexFunc(transforms, func(t *Transform) bool {
								return bytes.Equal(t.GetKey(), key)
							})

							if existingIdx >= 0 {
								// Merge: append operations
								mergedOps := append(
									transforms[existingIdx].GetOperations(),
									newTransform.GetOperations()...,
								)
								transforms[existingIdx].SetOperations(mergedOps)
							} else {
								// Add new transform
								transforms = append(transforms, newTransform)
							}
						}
					}

					for _, w := range batch.GetWrites() {
						writes = append(writes, [2][]byte{w.GetKey(), w.GetValue()})
					}
					deletes = append(deletes, batch.GetDeletes()...)
					uuids = append(uuids, id)
					syncLevels = append(syncLevels, batch.GetSyncLevel())
					if batchTimestamp == 0 {
						batchTimestamp = batch.GetTimestamp()
					}
				}
			case Op_OpBackup:
				s.logger.Debug(
					"Merging backup operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpBackup(ctx, dataKv.GetBackup()))
			case Op_OpSetRange:
				s.logger.Debug("Merging merge operation", zap.String("op", dataKv.GetOp().String()))
				flushBatch()
				s.maybeCallback(id, s.applyOpSetRange(ctx, dataKv.GetSetRange()))
			case Op_OpUpdateSchema:
				s.logger.Debug(
					"Merging schema update operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpUpdateSchema(ctx, dataKv.GetUpdateSchema()))
			case Op_OpSplit:
				s.logger.Debug("Merging split operation", zap.String("op", dataKv.GetOp().String()))
				flushBatch()
				s.maybeCallback(id, s.applyOpSplit(ctx, dataKv.GetSplit()))
			case Op_OpAddIndex:
				s.logger.Debug(
					"Merging add index operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpAddIndex(ctx, dataKv.GetAddIndex()))
			case Op_OpDeleteIndex:
				flushBatch()
				s.maybeCallback(id, s.applyOpDeleteIndex(ctx, dataKv.GetDeleteIndex()))
			case Op_OpInitTransaction:
				s.logger.Debug(
					"Applying init transaction operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpInitTransaction(ctx, dataKv.GetInitTransaction()))
			case Op_OpCommitTransaction:
				s.logger.Debug(
					"Applying commit transaction operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpCommitTransaction(ctx, dataKv.GetCommitTransaction()))
			case Op_OpAbortTransaction:
				s.logger.Debug(
					"Applying abort transaction operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpAbortTransaction(ctx, dataKv.GetAbortTransaction()))
			case Op_OpWriteIntent:
				s.logger.Debug(
					"Applying write intent operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpWriteIntent(ctx, dataKv.GetWriteIntent()))
			case Op_OpResolveIntents:
				s.logger.Debug(
					"Applying resolve intents operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpResolveIntents(ctx, dataKv.GetResolveIntents()))
			case Op_OpFinalizeSplit:
				s.logger.Debug(
					"Applying finalize split operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpFinalizeSplit(ctx, dataKv.GetFinalizeSplit()))
			case Op_OpSetSplitState:
				s.logger.Debug(
					"Applying set split state operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpSetSplitState(ctx, dataKv.GetSetSplitState()))
			case Op_OpFinalizeMerge:
				s.logger.Debug(
					"Applying finalize merge operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpFinalizeMerge(ctx, dataKv.GetFinalizeMerge()))
			case Op_OpSetMergeState:
				s.logger.Debug(
					"Applying set merge state operation",
					zap.String("op", dataKv.GetOp().String()),
				)
				flushBatch()
				s.maybeCallback(id, s.applyOpSetMergeState(ctx, dataKv.GetSetMergeState()))
			default:
				err := fmt.Errorf("unsupported operation: %v", dataKv.GetOp())
				s.maybeCallback(id, err)
				s.logger.Error(
					"unsupported operation",
					zap.String("op", dataKv.GetOp().String()),
					zap.Error(err),
				)
			}
		}
		flushBatch()
		close(commit.ApplyDoneC)
	}
	if err, ok := <-errorC; ok {
		// ErrRemoved is expected when a node is removed from a Raft group during
		// cluster reconfiguration (e.g., shard splits, rebalancing). Handle gracefully.
		if errors.Is(err, raft.ErrRemoved) {
			s.logger.Info("Received ID removal notification from raft", zap.Error(err))
			return
		}
		s.logger.Fatal("Received error from raft error channel", zap.Error(err))
	}
}

func (s *StoreDB) CreateDBSnapshot(id string) error {
	_, err := s.coreDB.Snapshot(id)
	return err
}

func (s *StoreDB) openDBAndIndex(recoverIndexes bool) error {
	s.byteRangeMu.RLock()
	byteRange := s.byteRange
	s.byteRangeMu.RUnlock()
	return s.coreDB.Open(s.dbDir, recoverIndexes, s.schema, byteRange)
}

// openDBWithoutIndexes opens only the Pebble database without initializing indexes.
// Used for portable restore where data must be imported before indexes are built.
func (s *StoreDB) openDBWithoutIndexes() error {
	s.byteRangeMu.RLock()
	byteRange := s.byteRange
	s.byteRangeMu.RUnlock()
	return s.coreDB.OpenWithoutIndexes(s.dbDir, s.schema, byteRange)
}

// openIndexes initializes and rebuilds indexes from the current Pebble data.
func (s *StoreDB) openIndexes() error {
	return s.coreDB.OpenIndexes(s.dbDir)
}

func (s *StoreDB) loadAndRecoverFromPersistentSnapshot(ctx context.Context) error {
	// Peek at the snapshot ID to detect portable (AFB) backups, which
	// require a different restore path: open an empty DB first, then import.
	snapID, err := s.loadSnapshotID(ctx)
	if errors.Is(err, pebble.ErrNotFound) {
		snapID = ""
	} else if err != nil {
		return fmt.Errorf("loading snapshot ID: %w", err)
	}

	if snapID != "" && strings.HasSuffix(snapID, ".afb") {
		// Portable backup: open Pebble without indexes, import AFB data,
		// then open indexes so they rebuild from the populated DB.
		if err := s.openDBWithoutIndexes(); err != nil {
			return fmt.Errorf("opening db for portable restore: %w", err)
		}
		if err := s.importPortableSnapshot(ctx, snapID); err != nil {
			return err
		}
		if err := s.openIndexes(); err != nil {
			return fmt.Errorf("opening indexes after portable restore: %w", err)
		}
		return nil
	}

	// Native backup path (or no snapshot at all).
	if err := s.loadPersistentSnapshot(ctx); err != nil {
		return fmt.Errorf("loading snapshot: %w", err)
	}
	if err := s.openDBAndIndex(false); err != nil {
		return fmt.Errorf("opening db and index: %w", err)
	}
	return nil
}

// importPortableSnapshot reads an AFB file from the snap store and imports
// all data into the already-open coreDB via ImportPortable.
func (s *StoreDB) importPortableSnapshot(ctx context.Context, snapID string) error {
	startTime := time.Now()
	s.logger.Info("Importing portable backup", zap.String("snapID", snapID))

	rc, err := s.snapStore.Get(ctx, snapID)
	if err != nil {
		return fmt.Errorf("opening portable backup %s: %w", snapID, err)
	}
	defer func() { _ = rc.Close() }()

	if err := s.coreDB.ImportPortable(ctx, rc); err != nil {
		return fmt.Errorf("importing portable backup %s: %w", snapID, err)
	}

	s.logger.Info("Imported portable backup",
		zap.String("snapID", snapID),
		zap.Duration("took", time.Since(startTime)))
	return nil
}

func (s *StoreDB) loadPersistentSnapshot(ctx context.Context) error {
	startTime := time.Now()
	// FIXME (ajr) Only load snapshot if our current db isn't sufficient
	snapID, err := s.loadSnapshotID(ctx)
	// FIXME (shouldn't be needed, handled by raft)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("loading snapshot: %w", err)
	}
	// In swarm mode, snapID will be empty (no Raft snapshots)
	if snapID == "" {
		return nil
	}
	if err := s.coreDB.Close(); err != nil {
		s.logger.Warn("failed to close previous db while loading snapshot", zap.Error(err))
	}

	// Extract to a temp directory first to detect archive format.
	// Archive Format v2 contains pebble/ and indexes/ subdirectories.
	// Archive Format v1 (old) contains pebble files directly.
	tempExtractDir := s.dbDir + "-extract-temp"
	defer func() {
		_ = os.RemoveAll(tempExtractDir)
	}()

	s.logger.Info("Extracting snapshot archive",
		zap.String("snapID", snapID),
		zap.String("tempDir", tempExtractDir))

	// Use SnapStore to extract the snapshot to temp directory
	metadata, err := s.snapStore.ExtractSnapshot(ctx, snapID, tempExtractDir, true)
	if err != nil {
		return fmt.Errorf("extracting snapshot %s: %w", snapID, err)
	}
	s.restoredArchiveMetadata = metadata

	// Log metadata if present
	if metadata != nil {
		s.logger.Info("Snapshot metadata",
			zap.Int("formatVersion", metadata.FormatVersion),
			zap.String("antflyVersion", metadata.AntflyVersion),
			zap.String("createdAt", metadata.CreatedAt),
			zap.String("compression", metadata.Compression))
		if metadata.Split != nil {
			s.logger.Info("Snapshot split replay metadata",
				zap.String("parentShardID", metadata.Split.ParentShardID),
				zap.Uint64("replayFenceSeq", metadata.Split.ReplayFenceSeq),
				zap.String("splitKey", metadata.Split.SplitKey))
		}
	}

	// Archive format requires pebble/ subdirectory and optionally indexes/
	pebbleSubdir := filepath.Join(tempExtractDir, "pebble")
	indexesSubdir := filepath.Join(tempExtractDir, "indexes")
	targetPebbleDir := filepath.Join(s.dbDir, "pebble")
	targetIndexesDir := filepath.Join(s.dbDir, "indexes")

	// Validate archive format
	if _, err := os.Stat(pebbleSubdir); err != nil {
		return fmt.Errorf("invalid archive format: missing pebble/ subdirectory")
	}

	// Ensure the target directory exists before moving files into it.
	// This is necessary for new shards starting from split archives where
	// the storage directory hasn't been created yet.
	if err := os.MkdirAll(s.dbDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return fmt.Errorf("creating db directory: %w", err)
	}

	// Remove existing directories if they exist
	_ = os.RemoveAll(targetPebbleDir)
	_ = os.RemoveAll(targetIndexesDir)

	// Move pebble data
	if err := os.Rename(pebbleSubdir, targetPebbleDir); err != nil {
		return fmt.Errorf("moving pebble from archive: %w", err)
	}

	// Move pre-built indexes if present
	if _, err := os.Stat(indexesSubdir); err == nil {
		if err := os.Rename(indexesSubdir, targetIndexesDir); err != nil {
			s.logger.Warn("Failed to move pre-built indexes, will rebuild",
				zap.String("from", indexesSubdir),
				zap.String("to", targetIndexesDir),
				zap.Error(err))
		} else {
			s.logger.Info("Extracted pre-built indexes from archive",
				zap.String("indexesDir", targetIndexesDir))
		}
	}

	size, _ := common.GetDirectorySize(targetPebbleDir)
	s.logger.Info("Extracted snapshot archive",
		zap.Duration("took", time.Since(startTime)),
		zap.Uint64("size", size),
		zap.String("snapID", snapID),
		zap.String("pebbleDir", targetPebbleDir))
	return nil
}

func (s *StoreDB) Search(ctx context.Context, encodedRequest []byte) ([]byte, error) {
	return s.coreDB.Search(ctx, encodedRequest)
}

func (s *StoreDB) SearchTyped(ctx context.Context, req *indexes.RemoteIndexSearchRequest) (*indexes.RemoteIndexSearchResult, error) {
	return s.coreDB.SearchTyped(ctx, req)
}

func (s *StoreDB) Scan(
	ctx context.Context,
	fromKey []byte,
	toKey []byte,
	opts ScanOptions,
) (*ScanResult, error) {
	return s.coreDB.Scan(ctx, fromKey, toKey, opts)
}

func (s *StoreDB) ExportRangeChunk(
	ctx context.Context,
	startKey, endKey, afterKey []byte,
	limit int,
) ([][2][]byte, []byte, bool, error) {
	return s.coreDB.ExportRangeChunk(ctx, startKey, endKey, afterKey, limit)
}

func (s *StoreDB) GetMergeDeltaSeq() (uint64, error) {
	return s.coreDB.GetMergeDeltaSeq()
}

func (s *StoreDB) GetMergeDeltaFinalSeq() (uint64, error) {
	return s.coreDB.GetMergeDeltaFinalSeq()
}

func (s *StoreDB) ListMergeDeltaEntriesAfter(afterSeq uint64) ([]*MergeDeltaEntry, error) {
	return s.coreDB.ListMergeDeltaEntriesAfter(afterSeq)
}

func (s *StoreDB) IsSplitting() bool {
	return s.splitting.Load()
}

// Stats returns statistics for the Bleve index and Pebble database.
func (s *StoreDB) Stats() *DBStats {
	diskSize, isEmpty, indexStats, err := s.coreDB.Stats()
	if err != nil {
		if !errors.Is(err, pebble.ErrClosed) {
			s.logger.Warn("Failed to get coreDB stats", zap.Error(err))
		}
	}

	return &DBStats{
		Created: s.created,
		Updated: s.clockOrReal().Now().UTC(),
		Storage: &DBStorageStats{
			Empty:    isEmpty,
			DiskSize: diskSize,
		},
		Indexes: indexStats,
	}
}

// Transaction operation apply methods
func (s *StoreDB) applyOpInitTransaction(
	ctx context.Context,
	op *InitTransactionOp,
) error {
	return s.coreDB.InitTransaction(ctx, op)
}

func (s *StoreDB) applyOpCommitTransaction(
	ctx context.Context,
	op *CommitTransactionOp,
) error {
	commitVersion, err := s.coreDB.CommitTransaction(ctx, op)
	if err != nil {
		return err
	}
	s.asyncResolveIntents(op.GetTxnId(), 1, commitVersion, "commit")
	return nil
}

func (s *StoreDB) applyOpAbortTransaction(
	ctx context.Context,
	op *AbortTransactionOp,
) error {
	if err := s.coreDB.AbortTransaction(ctx, op); err != nil {
		return err
	}
	s.asyncResolveIntents(op.GetTxnId(), 2, 0, "abort")
	return nil
}

// asyncResolveIntents asynchronously resolves the coordinator's own intents
// after a successful commit or abort, to avoid blocking the response.
func (s *StoreDB) asyncResolveIntents(txnID []byte, status int32, commitVersion uint64, action string) {
	go func() {
		resolveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resolveOp := ResolveIntentsOp_builder{
			TxnId:         txnID,
			Status:        status,
			CommitVersion: commitVersion,
		}.Build()

		if err := s.proposeResolveIntents(resolveCtx, resolveOp); err != nil {
			s.logger.Warn("Failed to resolve coordinator's intents after "+action,
				zap.Binary("txnID", txnID),
				zap.Error(err))
		} else {
			s.logger.Debug("Resolved coordinator's intents after "+action,
				zap.Binary("txnID", txnID))
		}
	}()
}

func (s *StoreDB) applyOpWriteIntent(
	ctx context.Context,
	op *WriteIntentOp,
) error {
	return s.coreDB.WriteIntent(ctx, op)
}

func (s *StoreDB) applyOpResolveIntents(
	ctx context.Context,
	op *ResolveIntentsOp,
) error {
	return s.coreDB.ResolveIntents(ctx, op)
}

// proposeResolveIntents proposes a ResolveIntentsOp through Raft consensus.
// This is used by the transaction recovery loop to resolve the coordinator's own intents.
func (s *StoreDB) proposeResolveIntents(ctx context.Context, op *ResolveIntentsOp) error {
	kvOp := Op_builder{
		Op:             Op_OpResolveIntents,
		ResolveIntents: op,
	}.Build()
	return s.proposeOnlyWriteOp(ctx, kvOp)
}

// proposeAbortTransaction proposes an AbortTransactionOp through Raft consensus.
// This is used by the recovery loop to auto-abort stale Pending transactions.
func (s *StoreDB) proposeAbortTransaction(ctx context.Context, op *AbortTransactionOp) error {
	kvOp := Op_builder{
		Op:               Op_OpAbortTransaction,
		AbortTransaction: op,
	}.Build()
	return s.proposeOnlyWriteOp(ctx, kvOp)
}

// proposeSplitStateChange proposes a SplitState change through Raft consensus.
// This replaces local pendingSplitKey with Raft-replicated state that survives
// leadership changes. The state parameter can be nil to clear the split state.
func (s *StoreDB) proposeSplitStateChange(ctx context.Context, state *SplitState) error {
	op := &SetSplitStateOp{}
	if state != nil {
		op.SetState(state)
	}
	kvOp := Op_builder{
		Op:            Op_OpSetSplitState,
		SetSplitState: op,
	}.Build()
	return s.syncWriteOp(ctx, kvOp)
}

// GetTransactionStatus retrieves the status of a transaction from the coordinator
func (s *StoreDB) GetTransactionStatus(ctx context.Context, txnID []byte) (int32, error) {
	return s.coreDB.GetTransactionStatus(ctx, txnID)
}

func (s *StoreDB) GetCommitVersion(ctx context.Context, txnID []byte) (uint64, error) {
	return s.coreDB.GetCommitVersion(ctx, txnID)
}

func (s *StoreDB) ListTxnRecords(ctx context.Context) ([]TxnRecord, error) {
	return s.coreDB.ListTxnRecords(ctx)
}

func (s *StoreDB) ListTxnIntents(ctx context.Context) ([]TxnIntent, error) {
	return s.coreDB.ListTxnIntents(ctx)
}

// GetEdges retrieves edges for a document in a graph index
func (s *StoreDB) GetEdges(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]indexes.Edge, error) {
	return s.coreDB.GetEdges(ctx, indexName, key, edgeType, direction)
}

// TraverseEdges performs graph traversal from a starting node
func (s *StoreDB) TraverseEdges(ctx context.Context, indexName string, startKey []byte, rules indexes.TraversalRules) ([]*indexes.TraversalResult, error) {
	return s.coreDB.TraverseEdges(ctx, indexName, startKey, rules)
}

// GetNeighbors retrieves direct neighbors of a node (1-hop traversal)
func (s *StoreDB) GetNeighbors(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]*indexes.TraversalResult, error) {
	return s.coreDB.GetNeighbors(ctx, indexName, key, edgeType, direction)
}

// FindShortestPath finds the shortest path between two nodes in the graph
func (s *StoreDB) FindShortestPath(ctx context.Context, indexName string, source, target []byte, edgeTypes []string, direction indexes.EdgeDirection, weightMode indexes.PathWeightMode, maxDepth int, minWeight, maxWeight float64) (*indexes.Path, error) {
	return s.coreDB.FindShortestPath(ctx, indexName, source, target, edgeTypes, direction, weightMode, maxDepth, minWeight, maxWeight)
}

// CloseProposeC closes the propose channel to trigger raft node shutdown.
// The caller should wait for the raft node's errorC to close before calling CloseDB.
func (s *StoreDB) CloseProposeC() {
	s.proposer.Close()
}

// CloseDB closes the database and proposal queue.
// This should only be called after the raft node has fully stopped.
func (s *StoreDB) CloseDB() error {
	if s.splitReplayCancel != nil {
		s.splitReplayCancel()
	}
	s.proposalQueue.Close()
	s.backgroundWG.Wait()
	if err := s.coreDB.Close(); err != nil {
		return fmt.Errorf("closing db: %w", err)
	}
	return nil
}

// Close closes the storeDB by first closing the propose channel,
// but does NOT wait for raft to stop. Use CloseProposeC + CloseDB for proper shutdown.
func (s *StoreDB) Close() error {
	s.CloseProposeC()
	return s.CloseDB()
}
