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

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/cockroachdb/pebble/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// ErrShardInitializing is returned when a shard operation is attempted on a
// shard that is still initializing. The client package matches this via
// substring for HTTP error responses.
var ErrShardInitializing = errors.New("shard is initializing")

// ErrShardNotFound is returned when a shard lifecycle operation targets a
// shard that does not exist on this store node.
var ErrShardNotFound = errors.New("shard not found")

var _ StoreIface = (*Store)(nil)

type StoreIface interface {
	Shard(id types.ID) (ShardIface, bool)
	Status() *StoreStatus
	StopRaftGroup(shardID types.ID) error
	StartRaftGroup(shardID types.ID, peers []common.Peer, join bool, config *ShardStartConfig) error
	ID() types.ID
	Scan(
		ctx context.Context,
		shardID types.ID,
		fromKey []byte,
		toKey []byte,
		opts db.ScanOptions,
	) (*db.ScanResult, error)
}

type errorChanCItem struct {
	errorC <-chan error
	id     types.ID
}

type Store struct {
	logger       *zap.Logger
	antflyConfig *common.Config

	// shardMu provides per-shard mutexes to serialize lifecycle operations
	// (Start/Stop) for the same shard, preventing disk-level races where
	// StopRaftGroup could RemoveAll directories while StartRaftGroup is
	// opening Pebble.
	shardMu *xsync.Map[types.ID, *sync.Mutex]

	config     *StoreInfo
	shardsMap  *xsync.Map[types.ID, *Shard]
	rs         raft.Transport
	clock      clock.Clock
	httpClient *http.Client

	db       *pebble.DB
	cache    *pebbleutils.Cache // shared Pebble block cache (may be nil)
	eg       *errgroup.Group
	egCancel context.CancelFunc

	errorChanC chan errorChanCItem
	closingCh  chan struct{} // Closed when store is shutting down
}

// TODO (ajr)
// - Add GC of snapshots
// - Save state of shards in pebble so we can recover from a crash

type Options struct {
	Clock      clock.Clock
	HTTPClient *http.Client
}

func NewStore(
	zl *zap.Logger,
	antflyConfig *common.Config,
	rs raft.Transport,
	conf *StoreInfo,
	cache *pebbleutils.Cache,
) (*Store, chan error, error) {
	return NewStoreWithOptions(zl, antflyConfig, rs, conf, cache, Options{})
}

func NewStoreWithOptions(
	zl *zap.Logger,
	antflyConfig *common.Config,
	rs raft.Transport,
	conf *StoreInfo,
	cache *pebbleutils.Cache,
	opts Options,
) (*Store, chan error, error) {
	dataDir := antflyConfig.GetBaseDir()
	storeMetadataDir := common.NodeDir(dataDir, conf.ID) + "/storeMetadataDB"

	// Ensure the directory exists before opening Pebble
	if err := os.MkdirAll(storeMetadataDir, 0755); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, nil, fmt.Errorf("creating store metadata directory: %w", err)
	}

	// Get absolute path for debugging
	absPath, err := filepath.Abs(storeMetadataDir)
	if err != nil {
		zl.Warn("Failed to get absolute path", zap.Error(err))
		absPath = storeMetadataDir
	}

	zl.Debug("Opening store metadata Pebble database",
		zap.String("path", storeMetadataDir),
		zap.String("absPath", absPath))

	storeMetadataOpts := &pebble.Options{
		Logger:          &logger.NoopLoggerAndTracer{},
		LoggerAndTracer: &logger.NoopLoggerAndTracer{},
	}
	cache.Apply(storeMetadataOpts, 64<<20) // 64MB fallback
	db, err := pebble.Open(storeMetadataDir, storeMetadataOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("opening pebble database: %w", err)
	}

	zl.Debug("Successfully opened store metadata Pebble database")

	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: long-lived store context, cancel stored in Store struct
	eg, _ := errgroup.WithContext(ctx)
	s := &Store{
		logger:       zl.With(zap.String("module", "store")),
		antflyConfig: antflyConfig,

		config:     conf,
		rs:         rs,
		clock:      opts.Clock,
		httpClient: opts.HTTPClient,

		db:       db,
		cache:    cache,
		eg:       eg,
		egCancel: cancel,

		shardMu:    xsync.NewMap[types.ID, *sync.Mutex](),
		shardsMap:  xsync.NewMap[types.ID, *Shard](),
		errorChanC: make(chan errorChanCItem, 1),
		closingCh:  make(chan struct{}),
	}

	// Load existing metadata from the database
	shards, err := s.loadMetadata()
	if err != nil {
		return nil, nil, fmt.Errorf("loading store metadata: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(len(shards))
	errChan := s.ErrorC(ctx)
	for shardID, timestamp := range shards {
		eg.Go(func() error {
			wg.Done()
			zl.Info("Starting shard from metadata",
				zap.Stringer("shardID", shardID),
				zap.ByteString("timestamp", timestamp))
			return s.StartRaftGroup(shardID, []common.Peer{}, true, &ShardStartConfig{
				Timestamp: timestamp,
			})
		})
		// For each shard, start it if it exists in the metadata
	}
	wg.Wait()
	return s, errChan, nil
}

func (s *Store) clockOrReal() clock.Clock {
	if s.clock == nil {
		return clock.RealClock{}
	}
	return s.clock
}

// FIXME (ajr) Maybe let's save some local state in pebble with the metadata of what shards we
// we're running, so we can recover from a crash and not have to re-register with the leader before
// starting the shards.
var prefix = []byte("store::shard::")

func (s *Store) saveMetadataForShard(key types.ID, timestamp []byte) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	// Save the store metadata to the Pebble database
	batch := s.db.NewBatch()
	if err := batch.Set(fmt.Appendf(prefix, "%s", key), timestamp, nil); err != nil {
		return fmt.Errorf("failed to save store metadata: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit store metadata batch: %w", err)
	}
	return batch.Close()
}

func (s *Store) deleteMetadataForShard(key types.ID) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	batch := s.db.NewBatch()
	if err := batch.Delete(fmt.Appendf(prefix, "%s", key), nil); err != nil {
		return fmt.Errorf("failed to delete store metadata: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit store metadata batch: %w", err)
	}
	return batch.Close()
}

func (s *Store) loadMetadata() (map[types.ID][]byte, error) {
	// Load the store metadata from the Pebble database
	iter, err := s.db.NewIter(&pebble.IterOptions{
		SkipPoint: func(userKey []byte) bool {
			return !bytes.HasPrefix(userKey, prefix)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for store metadata: %w", err)
	}
	defer func() { _ = iter.Close() }()

	shards := make(map[types.ID][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		key := bytes.TrimPrefix(iter.Key(), prefix)
		shardID, err := types.IDFromString(string(key))
		if err != nil {
			return nil, fmt.Errorf("invalid shard ID in metadata: %w", err)
		}
		shards[shardID] = bytes.Clone(iter.Value())
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("error iterating over store metadata: %w", err)
	}
	return shards, nil
}

type StoreStatus struct {
	ID     types.ID                `json:"id"`
	Shards map[types.ID]*ShardInfo `json:"shards"`
}

func (m *Store) ID() types.ID {
	return m.config.ID
}

// ErrorcC dynamically receives new error channels from shards and fans them in
func (m *Store) ErrorC(ctx context.Context) chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		errorChans := []<-chan error{}
		ids := []types.ID{}
		m.shardsMap.Range(func(key types.ID, shard *Shard) bool {
			errorChans = append(errorChans, shard.ErrorC())
			ids = append(ids, key)
			return true
		})
		cases := make([]reflect.SelectCase, len(errorChans)+2)
		cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(m.errorChanC)}
		cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
		for i, ch := range errorChans {
			cases[i+2] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		}
		remaining := len(cases)
		for remaining > 0 {
			chosen, value, ok := reflect.Select(cases)
			if chosen == 1 {
				// TODO (ajr) Do we want to forward the context.Cancelled errors here?
				// errCh <- ctx.Err()
				return
			} else if chosen == 0 {
				if !ok {
					// If we close the error chan channel, we're shutting down
					return
				}
				// TODO (ajr) Reuse spots in the cases and ids slices that have been cleared
				// We started up another shard so listen to it's error channel
				v := value.Interface().(errorChanCItem)
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(v.errorC)})
				ids = append(ids, v.id)
				remaining += 1
				continue
			} else if !ok {
				// The chosen channel has been closed, so zero out the channel to disable the case
				cases[chosen].Chan = reflect.ValueOf(nil)
				// Should we stop the Raft group when the error channel is closed?
				// or should we use a special error for when the shard is removed from the node
				ids[chosen-2] = 0
				remaining -= 1
				continue
			}

			err := value.Interface().(error)
			shardID := ids[chosen-2]
			if strings.Contains(err.Error(), "removed") {
				m.logger.Info("ID removed: shutting down Raft group",
					zap.Stringer("shardID", shardID),
					zap.Error(err))
				if stopErr := m.StopRaftGroup(shardID); stopErr != nil {
					m.logger.Warn("Failed to stop removed Raft group",
						zap.Stringer("shardID", shardID),
						zap.Error(stopErr))
				}
				continue
			}
			m.logger.Info("Received error from shard",
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			errCh <- fmt.Errorf("shard %s: %w", shardID, err)
		}
	}()
	return errCh
}

func (m *Store) Status() *StoreStatus {
	shardInfo := make(map[types.ID]*ShardInfo)
	m.shardsMap.Range(func(key types.ID, shard *Shard) bool {
		shardInfo[key] = shard.Info()
		return true
	})
	return &StoreStatus{
		ID:     m.config.ID,
		Shards: shardInfo,
	}
}

func (m *Store) Shard(shardID types.ID) (ShardIface, bool) {
	return m.shardsMap.Load(shardID)
}

func (m *Store) Scan(
	ctx context.Context,
	shardID types.ID,
	fromKey []byte,
	toKey []byte,
	opts db.ScanOptions,
) (*db.ScanResult, error) {
	shard, ok := m.shardsMap.Load(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}
	return shard.Scan(ctx, fromKey, toKey, opts)
}

// shardLifecycleMu returns a per-shard mutex that serializes Start and Stop
// operations, preventing disk-level races.
func (m *Store) shardLifecycleMu(shardID types.ID) *sync.Mutex {
	mu, _ := m.shardMu.LoadOrCompute(shardID, func() (*sync.Mutex, bool) {
		return &sync.Mutex{}, false
	})
	return mu
}

func (m *Store) StopRaftGroup(shardID types.ID) error {
	mu := m.shardLifecycleMu(shardID)
	mu.Lock()
	defer mu.Unlock()

	m.logger.Info("Stopping Raft Group", zap.Stringer("shardID", shardID))

	// Atomically check the shard state and remove it from the map.
	var shardToClose *Shard
	var wasInitializing bool
	m.shardsMap.Compute(shardID, func(shard *Shard, loaded bool) (*Shard, xsync.ComputeOp) {
		if !loaded {
			return shard, xsync.CancelOp
		}
		if shard.IsInitializing() {
			wasInitializing = true
			return shard, xsync.CancelOp // keep in map
		}
		shardToClose = shard
		return shard, xsync.DeleteOp
	})

	if wasInitializing {
		return ErrShardInitializing
	}
	if shardToClose == nil {
		return ErrShardNotFound
	}
	if err := shardToClose.Close(); err != nil {
		m.logger.Warn("Error closing shard while stopping Raft Group",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
	}

	dataDir := m.antflyConfig.GetBaseDir()
	_ = os.RemoveAll(common.StorageDBDir(dataDir, shardID, m.config.ID)) //nolint:gosec // G703: path from internal config
	// Clean up snapshots using SnapStore abstraction
	if snapStore, err := snapstore.NewLocalSnapStore(dataDir, shardID, m.config.ID); err == nil {
		_ = snapStore.RemoveAll(context.Background())
	}
	_ = os.RemoveAll(common.RaftLogDir(dataDir, shardID, m.config.ID)) //nolint:gosec // G703: path from internal config
	_ = m.deleteMetadataForShard(shardID)
	m.logger.Info("Deleted Raft Group disk footprint", zap.Stringer("shardID", shardID))
	return nil
}

func (m *Store) createPassThroughChannels(
	proposeC <-chan *raft.Proposal,
) (<-chan *raft.Commit, <-chan error) {
	commitC := make(chan *raft.Commit)
	errorC := make(chan error)

	// Create a goroutine that immediately commits all proposals
	go func() {
		defer close(commitC)
		defer close(errorC)

		for proposal := range proposeC {
			if proposal == nil {
				continue
			}

			// Signal that the proposal was accepted
			if proposal.ProposeDoneC != nil {
				proposal.ProposeDoneC <- nil
			}

			// Immediately commit the proposal without Raft consensus
			applyDoneC := make(chan struct{})
			commit := &raft.Commit{
				Data:       [][]byte{proposal.Data},
				ApplyDoneC: applyDoneC,
			}

			commitC <- commit
			// Wait for the commit to be applied
			<-applyDoneC
		}
	}()

	return commitC, errorC
}

func (m *Store) Close() {
	// Signal that we're closing - this prevents new shard starts from sending to errorChanC
	close(m.closingCh)

	// Close all active shards to release Pebble and Bleve resources.
	// This is critical to prevent memory leaks when store nodes are removed.
	m.shardsMap.Range(func(shardID types.ID, shard *Shard) bool {
		m.logger.Debug("Closing shard during store shutdown", zap.Stringer("shardID", shardID))

		if err := shard.Close(); err != nil && !errors.Is(err, db.ErrShardNotReady) {
			m.logger.Warn("Error closing shard database",
				zap.Stringer("shardID", shardID),
				zap.Error(err))
		}

		return true
	})

	// Close store metadata database
	if err := m.db.Close(); err != nil {
		m.logger.Warn("Error closing store metadata Pebble database", zap.Error(err))
	}

	// Cancel the store context to stop the ErrorC goroutine. This must happen
	// after closing shards so in-flight errgroup work can complete cleanly.
	m.egCancel()

	m.logger.Info("Store closed", zap.String("storeID", m.config.ID.String()))
}

type ShardStartConfig struct {
	ShardConfig

	Timestamp         []byte // Timestamp of the shard start, used for conf changes
	InitWithDBArchive string
}

func (m *Store) StartRaftGroup(
	shardID types.ID,
	peers []common.Peer,
	join bool,
	conf *ShardStartConfig,
) error {
	mu := m.shardLifecycleMu(shardID)
	mu.Lock()
	defer mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)              // Print the panic value
			fmt.Println("Stack trace:\n", string(debug.Stack())) // Print the stack trace
			panic(r)
		}
	}()
	lg := m.logger.With(zap.Stringer("shardID", shardID))
	if _, ok := m.shardsMap.Load(shardID); ok {
		lg.Info("Shard already started")
		return nil
	}
	m.shardsMap.Store(shardID, db.NewInitializingShard())
	lg.Debug("Starting Raft Group")
	// If we fail to start the shard, remove it from the map
	removeShard := true
	defer func() {
		if removeShard {
			m.shardsMap.Delete(shardID)
		}
	}()
	var dbw *db.StoreDB
	storeDBReady := make(chan struct{})
	defer close(storeDBReady)
	createDBSnapshot := func(id string) error {
		<-storeDBReady
		return dbw.CreateDBSnapshot(id)
	}
	proposeC := make(chan *raft.Proposal)
	confChangeC := make(chan *raft.ConfChangeProposal)
	dataDir := m.antflyConfig.GetBaseDir()
	snapStore, err := snapstore.NewLocalSnapStore(dataDir, shardID, m.config.ID)
	if err != nil {
		return fmt.Errorf("creating snapshot store: %w", err)
	}

	// For new shards (not rejoining existing cluster) or split shards,
	// clean up any leftover raft log state from failed previous attempts.
	// This prevents "could not open manifest file" errors when Pebble finds
	// partial state (e.g., CURRENT file without corresponding MANIFEST).
	raftLogDir := common.RaftLogDir(dataDir, shardID, m.config.ID)
	if !join || conf.InitWithDBArchive != "" {
		if err := os.RemoveAll(raftLogDir); err != nil && !os.IsNotExist(err) { //nolint:gosec // G703: path from internal config
			lg.Warn("Failed to clean raft log directory before start",
				zap.String("raftLogDir", raftLogDir),
				zap.Error(err))
		} else {
			lg.Debug("Cleaned raft log directory for fresh start",
				zap.String("raftLogDir", raftLogDir),
				zap.Bool("join", join),
				zap.String("initArchive", conf.InitWithDBArchive))
		}
	}

	var commitC <-chan *raft.Commit
	var errorC <-chan error
	var raftNode raft.RaftNode

	// In swarm mode, bypass Raft consensus with a pass-through channel
	if m.antflyConfig != nil && m.antflyConfig.SwarmMode {
		lg.Info("Starting shard in swarm mode (bypassing Raft)")
		commitC, errorC = m.createPassThroughChannels(proposeC)
	} else {
		raftConf := raft.RaftNodeConfig{
			InitWithStorageSnapshot:   conf.InitWithDBArchive,
			DisableAsyncStorageWrites: true,
			SnapStore:                 snapStore,
			RaftLogDir:                raftLogDir,
			Peers:                     peers,
			Join:                      join,
			Timetstamp:                conf.Timestamp,
			Cache:                     m.cache,
			Clock:                     m.clockOrReal(),
		}
		// Pass the store's logger to the raft node
		commitC, errorC, raftNode = raft.NewRaftNode(
			lg,
			shardID,
			m.config.ID,
			raftConf,
			m.rs,
			createDBSnapshot,
			proposeC,
			confChangeC,
		)
	}

	dbDir := common.StorageDBDir(dataDir, shardID, m.config.ID)
	var err2 error

	// In swarm mode without Raft, provide a stub function that returns the restore archive ID if set,
	// or an empty snapshot ID. This allows backup/restore to work in swarm mode.
	var getSnapshotID func(ctx context.Context) (string, error)
	if raftNode != nil {
		getSnapshotID = raftNode.GetSnapshotID
	} else {
		// In swarm mode, return the restore archive ID if provided, otherwise empty
		initArchive := conf.InitWithDBArchive
		getSnapshotID = func(ctx context.Context) (string, error) {
			return initArchive, nil
		}
	}

	// Create a shard notifier for cross-shard transaction notifications
	// The notifier routes requests through the metadata server, which handles
	// finding the correct node for each shard
	var metadataURL string
	if m.antflyConfig != nil && len(m.antflyConfig.Metadata.OrchestrationUrls) > 0 {
		urls, err := m.antflyConfig.Metadata.GetOrchestrationURLs()
		if err == nil && len(urls) > 0 {
			// Pick the first available metadata server URL
			for _, url := range urls {
				metadataURL = url
				break
			}
		}
	}
	httpClient := m.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	shardNotifier := NewHTTPShardNotifier(
		httpClient,
		metadataURL,
		lg.Named("shardNotifier"),
	)

	dbw, err2 = db.NewStoreDB(
		lg,
		m.antflyConfig,
		dbDir,
		snapStore,
		conf.Indexes,
		conf.Schema,
		conf.ByteRange,
		getSnapshotID,
		proposeC,
		commitC,
		errorC,
		shardNotifier,
		func(id types.ID) *db.StoreDB {
			shard, ok := m.shardsMap.Load(id)
			if !ok || shard == nil {
				return nil
			}
			return shard.StoreDB()
		},
		m.cache,
		m.clockOrReal(),
	)
	if err2 != nil {
		// First, trigger Raft node shutdown by closing the proposal channels.
		// This must happen BEFORE we delete the Raft log directory to avoid
		// a race condition where the Raft node tries to access Pebble files
		// that we're deleting.
		close(confChangeC)
		close(proposeC)

		// Wait for the Raft node to fully stop by draining errorC.
		// The Raft node closes errorC when it stops (see raftNode.stop()).
		// In swarm mode, errorC is closed by the pass-through goroutine.
		if errorC != nil {
			for range errorC {
				// Drain any remaining errors
			}
		}

		// FIXME (ajr) Only remove the directories if we failed to start the raft group from scratch (on restart we might lose everything)
		// Now safe to remove directories since Raft has stopped
		_ = os.RemoveAll(common.StorageDBDir(dataDir, shardID, m.config.ID)) //nolint:gosec // G703: path from internal config
		_ = snapStore.RemoveAll(context.Background())
		_ = os.RemoveAll(common.RaftLogDir(dataDir, shardID, m.config.ID)) //nolint:gosec // G703: path from internal config
		lg.Warn("Error starting raft group", zap.Error(err2))
		return fmt.Errorf("creating dbwrapper: %w", err2)
	}

	var cancelLeaderFactory context.CancelFunc
	if raftNode != nil {
		raftNode.SetLeaderFactory(dbw.LeaderFactory)
	} else {
		// In swarm mode without Raft, call the leader factory directly
		// since we're always the leader. Use a cancellable context so we
		// can properly stop the LeaderFactory when the shard is removed.
		ctx, cancel := context.WithCancel(context.Background())
		cancelLeaderFactory = cancel
		m.eg.Go(func() error {
			return dbw.LeaderFactory(ctx)
		})
	}

	// Send to errorChanC, but abort if store is closing
	select {
	case m.errorChanC <- errorChanCItem{errorC: errorC, id: shardID}:
		// Successfully registered error channel
	case <-m.closingCh:
		// Store is closing, abort shard start
		lg.Info("Store is closing, aborting shard start")
		return fmt.Errorf("store is closing")
	}
	lg.Info("Started Raft Group")
	m.shardsMap.Store(shardID, db.NewShard(dbw, raftNode, confChangeC, errorC, cancelLeaderFactory))
	// Check again if store is closing before saving metadata to avoid
	// race condition where Close() closes m.db between the earlier check
	// and this point.
	select {
	case <-m.closingCh:
		lg.Info("Store is closing, skipping metadata save")
		removeShard = false // Keep shard in map, Close() will clean it up
		return nil
	default:
	}
	if err := m.saveMetadataForShard(shardID, conf.Timestamp); err != nil {
		// If pebble is closed, the store is shutting down - don't treat as error
		if errors.Is(err, pebble.ErrClosed) {
			lg.Info("Store closed during metadata save, continuing")
			removeShard = false
			return nil
		}
		return fmt.Errorf("saving metadata for shard %s: %w", shardID, err)
	}
	lg.Info("Saved shard metadata to local store")
	removeShard = false
	return nil
}
