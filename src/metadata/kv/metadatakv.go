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

package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/raftkv"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/s3storage"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/remote"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func init() {
	gob.Register(map[string]any{})
	gob.Register(map[string]map[string]any{})
	gob.Register([]any{})
	gob.Register([2][]byte{})
}

// keyChange tracks a single key modification for listener notification.
type keyChange struct {
	key      []byte
	value    []byte
	isDelete bool
}

// metadataKV is a Raft-backed key-value store for cluster metadata.
type metadataKV struct {
	snapStore      snapstore.SnapStore
	dbDir          string
	loadSnapshotID func(ctx context.Context) (string, error)

	pdb          *pebble.DB
	cache        *pebbleutils.Cache // shared Pebble block cache (may be nil)
	logger       *zap.Logger
	antflyConfig *common.Config

	// Raft integration
	proposer    *raftkv.Proposer
	confChangeC chan<- *raft.ConfChangeProposal
	raftNode    raft.RaftNode

	// S3 storage backend for remote sstables (leader-only writes)
	isLeader  atomic.Bool
	s3Storage *s3storage.LeaderAwareS3Storage

	// Key listeners
	keyPatterns        []*KeyPattern
	keyPrefixListeners []KeyPrefixListener
	listenerMu         sync.RWMutex

	// commitDoneC is closed when the ReadCommits goroutine exits,
	// so Close() can wait for commit processing to finish before closing Pebble.
	commitDoneC chan struct{}
	cancel      context.CancelFunc

	// Pending batch for commit processing (implements raftkv.CommitProcessor).
	// These fields are only accessed from the single ReadCommits goroutine,
	// so no synchronization is needed.
	pendingWrites  [][2][]byte
	pendingDeletes [][]byte
	pendingUUIDs   []uuid.UUID
}

func (s *metadataKV) pebbleDir() string {
	return s.dbDir + "/pebble"
}

func newMetadataKV(
	lg *zap.Logger,
	antflyConfig *common.Config,
	dbDir string,
	snapStore snapstore.SnapStore,
	loadSnapshotID func(ctx context.Context) (string, error),
	proposeC chan<- *raft.Proposal,
	commitC <-chan *raft.Commit,
	errorC <-chan error,
	cache *pebbleutils.Cache,
) (*metadataKV, error) {
	s := &metadataKV{
		logger:         lg.Named("metadatakv"),
		antflyConfig:   antflyConfig,
		proposer:       raftkv.NewProposer(proposeC),
		dbDir:          dbDir,
		cache:          cache,
		snapStore:      snapStore,
		loadSnapshotID: loadSnapshotID,
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// FIXME (ajr) If we gracefully shutdown we shouldn't need to load from the snapshot
	pebbleDir := s.pebbleDir()
	if _, statErr := os.Stat(pebbleDir); statErr == nil {
		// Pebble directory exists, open it directly
		pebbleOpts, err := s.getPebbleOpts()
		if err != nil {
			return nil, err
		}
		s.pdb, err = pebble.Open(pebbleDir, pebbleOpts)
		if err != nil {
			return nil, fmt.Errorf("opening pebble at %s: %w", pebbleDir, err)
		}
	} else {
		// Pebble directory doesn't exist, load from snapshot and create
		if err := s.loadSnapshotAndOpenDB(ctx); err != nil {
			return nil, fmt.Errorf("loading persistent snapshot: %w", err)
		}
	}

	// Read commits from raft using the common commit processor
	s.commitDoneC = make(chan struct{})
	go func() {
		raftkv.ReadCommits(ctx, commitC, errorC, s, s.logger)
		close(s.commitDoneC)
	}()

	return s, nil
}

// Apply implements raftkv.CommitProcessor. It decodes and accumulates operations.
func (s *metadataKV) Apply(_ context.Context, data []byte) error {
	var op leaderOp
	if err := op.decode(data); err != nil {
		s.logger.Error("could not decode commit data, skipping entry", zap.Error(err))
		return nil // Don't fail the whole commit for one bad entry
	}

	switch op.Op {
	case OpBatch:
		s.pendingWrites = append(s.pendingWrites, op.Writes...)
		s.pendingDeletes = append(s.pendingDeletes, op.Deletes...)
		s.pendingUUIDs = append(s.pendingUUIDs, op.UUID)
	default:
		err := fmt.Errorf("unsupported operation: %v", op.Op)
		s.proposer.NotifyCommit(op.UUID, err)
		s.logger.Error("unsupported operation", zap.Stringer("op", op.Op), zap.Error(err))
	}

	return nil
}

// Flush implements raftkv.CommitProcessor. It applies the accumulated batch.
func (s *metadataKV) Flush(ctx context.Context) error {
	if len(s.pendingWrites) == 0 && len(s.pendingDeletes) == 0 {
		return nil
	}

	err := s.applyBatch(ctx, s.pendingWrites, s.pendingDeletes)

	// Notify all waiting callers
	for _, id := range s.pendingUUIDs {
		s.proposer.NotifyCommit(id, err)
	}

	// Reset pending state
	s.pendingWrites = nil
	s.pendingDeletes = nil
	s.pendingUUIDs = nil

	return err
}

// LoadSnapshot implements raftkv.CommitProcessor.
func (s *metadataKV) LoadSnapshot(ctx context.Context) error {
	return s.loadSnapshotAndOpenDB(ctx)
}

func (s *metadataKV) loadSnapshotAndOpenDB(ctx context.Context) error {
	// Close existing pdb before loading snapshot and reopening.
	// loadPersistentSnapshot closes pdb when a snapshot is found, but if no
	// snapshot exists (e.g. swarm mode) we still need to close before reopening.
	if s.pdb != nil {
		if err := s.pdb.Close(); err != nil {
			s.logger.Warn("failed to close previous db before snapshot load", zap.Error(err))
		}
		s.pdb = nil
	}

	if err := s.loadPersistentSnapshot(ctx); err != nil {
		return fmt.Errorf("loading persistent snapshot: %w", err)
	}

	// Ensure the pebble directory exists before opening
	pebbleDir := s.pebbleDir()
	if err := os.MkdirAll(pebbleDir, 0755); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return fmt.Errorf("creating pebble directory: %w", err)
	}

	s.logger.Info("Opening Pebble storage after snapshot extraction",
		zap.String("dbDir", pebbleDir),
	)
	pebbleOpts, err := s.getPebbleOpts()
	if err != nil {
		return err
	}
	s.pdb, err = pebble.Open(pebbleDir, pebbleOpts)
	if err != nil {
		return fmt.Errorf("opening pebble: %w", err)
	}
	return nil
}

func (s *metadataKV) loadPersistentSnapshot(ctx context.Context) error {
	// FIXME (ajr) Only load snapshot if our current db isn't sufficient
	snapID, err := s.loadSnapshotID(ctx)
	// FIXME (shouldn't be needed, handled by raft)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to load snapshot info: %w", err)
	}
	// In swarm mode, snapID will be empty (no Raft snapshots)
	if snapID == "" {
		return nil
	}

	targetDir := s.pebbleDir()
	s.logger.Info("Extracting snapshot archive",
		zap.String("snapID", snapID),
		zap.String("targetDir", targetDir))

	// Use SnapStore to extract the snapshot
	metadata, err := s.snapStore.ExtractSnapshot(ctx, snapID, targetDir, true)
	if err != nil {
		return fmt.Errorf("extracting snapshot %s: %w", snapID, err)
	}

	size, _ := common.GetDirectorySize(targetDir)
	s.logger.Info("Extracted snapshot archive",
		zap.Uint64("size", size),
		zap.String("snapID", snapID),
		zap.String("targetDir", targetDir))

	// Log metadata if present
	if metadata != nil {
		s.logger.Info("Snapshot metadata",
			zap.Int("formatVersion", metadata.FormatVersion),
			zap.String("antflyVersion", metadata.AntflyVersion),
			zap.String("createdAt", metadata.CreatedAt))
	}

	return nil
}

// getPebbleOpts creates and configures Pebble options, including S3 storage if enabled
func (s *metadataKV) getPebbleOpts() (*pebble.Options, error) {
	pebbleOpts := &pebble.Options{
		Logger: &logger.NoopLoggerAndTracer{},
	}
	s.cache.Apply(pebbleOpts, 64<<20) // 64MB fallback
	if s.antflyConfig.GetMetadataStorageType() == "s3" {
		if err := s.configureS3Storage(pebbleOpts); err != nil {
			return nil, fmt.Errorf("configuring S3 storage: %w", err)
		}
	} else {
		s.logger.Debug("S3 storage disabled for metadata, using local storage only")
	}
	return pebbleOpts, nil
}

// configureS3Storage configures Pebble to use S3 for remote sstable storage.
func (s *metadataKV) configureS3Storage(pebbleOpts *pebble.Options) error {
	s3Info := s.antflyConfig.Storage.S3
	s.logger.Info("Configuring S3 storage for metadata Pebble",
		zap.String("endpoint", s3Info.Endpoint),
		zap.String("bucket", s3Info.Bucket),
		zap.String("prefix", s3Info.Prefix),
		zap.Bool("useSSL", s3Info.UseSsl),
	)

	minioClient, err := s3Info.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}

	baseS3Storage, err := s3storage.NewS3Storage(
		minioClient,
		s3Info.Bucket,
		s3Info.Prefix,
	)
	if err != nil {
		return fmt.Errorf("creating S3 storage backend: %w", err)
	}

	s.s3Storage = s3storage.NewLeaderAwareS3Storage(baseS3Storage, &s.isLeader)

	pebbleOpts.Experimental.RemoteStorage = remote.MakeSimpleFactory(
		map[remote.Locator]remote.Storage{
			"s3": s.s3Storage,
		},
	)
	pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedAll
	pebbleOpts.Experimental.CreateOnSharedLocator = "s3"

	s.logger.Info("S3 storage configured successfully for metadata",
		zap.String("locator", "s3"),
	)

	return nil
}

// SetLeader updates the leader status for S3 storage writes
func (s *metadataKV) SetLeader(isLeader bool) {
	s.isLeader.Store(isLeader)
	if s.s3Storage != nil {
		s.logger.Info("Updated metadata leader status for S3 writes", zap.Bool("isLeader", isLeader))
	}
}

// SetLeaderFactory sets the leader factory function, wrapping it to manage leader status for S3 writes
func (s *metadataKV) SetLeaderFactory(f func(ctx context.Context) error) {
	wrappedFactory := func(ctx context.Context) error {
		s.SetLeader(true)
		defer s.SetLeader(false)
		return f(ctx)
	}

	if s.raftNode != nil {
		s.raftNode.SetLeaderFactory(wrappedFactory)
	} else {
		// In swarm mode without Raft, call the factory function directly
		go func() {
			if err := wrappedFactory(context.Background()); err != nil {
				s.logger.Error("Leader factory error in swarm mode", zap.Error(err))
			}
		}()
	}
}

func (s *metadataKV) Info() *store.ShardInfo {
	var raftStatus *common.RaftStatus
	var peers map[types.ID]struct{}
	if s.raftNode != nil {
		raftStatus = common.NewRaftStatus(s.raftNode.Status())
		peers = s.raftNode.Peers()
	}

	return &store.ShardInfo{
		RaftStatus: raftStatus,
		Peers:      peers,
	}
}

type Op int

const (
	OpBatch Op = iota
)

func (op Op) String() string {
	switch op {
	case OpBatch:
		return "OpBatch"
	default:
		return fmt.Sprintf("Op(%d)", op)
	}
}

type leaderOp struct {
	Op Op

	Writes  [][2][]byte
	Deletes [][]byte

	UUID uuid.UUID
}

func (kv *leaderOp) encode() ([]byte, error) {
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	if err := gob.NewEncoder(encoder).Encode(kv); err != nil {
		return nil, fmt.Errorf("failed to encode kv message: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (kv *leaderOp) decode(data []byte) error {
	buf := bytes.NewReader(data)
	decoder, err := zstd.NewReader(buf)
	if err != nil {
		return err
	}
	defer decoder.Close()
	if err := gob.NewDecoder(decoder).Decode(kv); err != nil {
		return fmt.Errorf("failed to decode kv message: %w", err)
	}
	return nil
}

func (s *metadataKV) ProposeConfChange(cc raftpb.ConfChange) error {
	proposeDoneC := make(chan error, 1)
	s.confChangeC <- &raft.ConfChangeProposal{
		ConfChange:   cc,
		ProposeDoneC: proposeDoneC,
	}
	if err := <-proposeDoneC; err != nil {
		return fmt.Errorf("proposing confChange to raft: %w", err)
	}
	return nil
}

// Batch writes and deletes keys through Raft consensus.
func (s *metadataKV) Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error {
	op := leaderOp{
		Op:      OpBatch,
		Writes:  writes,
		Deletes: deletes,
		UUID:    uuid.New(),
	}

	encodedData, err := op.encode()
	if err != nil {
		return fmt.Errorf("encoding data for proposal: %w", err)
	}

	if err := s.proposer.ProposeAndWait(ctx, encodedData, op.UUID); err != nil {
		return fmt.Errorf("syncing metadata KV operation: %w", err)
	}
	return nil
}

func (s *metadataKV) Get(ctx context.Context, key []byte) (val []byte, closer io.Closer, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	if s.pdb == nil {
		return nil, nil, errors.New("pebble database is not initialized")
	}
	return s.pdb.Get(key)
}

func (s *metadataKV) NewIter(
	ctx context.Context,
	opts *pebble.IterOptions,
) (iter *pebble.Iterator, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	if s.pdb == nil {
		return nil, errors.New("pebble database is not initialized")
	}
	return s.pdb.NewIterWithContext(ctx, opts)
}

func (s *metadataKV) createDBSnapshot(id string) error {
	stagingDir := filepath.Join(os.TempDir(), fmt.Sprintf("antfly-metadata-snap-%s-%s", id, uuid.NewString()))
	defer func() {
		_ = os.RemoveAll(stagingDir)
	}()

	if err := s.pdb.Checkpoint(
		stagingDir,
		pebble.WithFlushedWAL(),
	); err != nil {
		return fmt.Errorf("creating checkpoint: %w", err)
	}

	size, err := s.snapStore.CreateSnapshot(context.Background(), id, stagingDir, nil)
	if err != nil {
		return fmt.Errorf("creating snapshot archive: %w", err)
	}

	s.logger.Info("Created snapshot archive",
		zap.Int64("size", size),
		zap.String("snapID", id),
		zap.String("dbDir", s.dbDir))
	return nil
}

func (s *metadataKV) Close() error {
	s.proposer.Close()
	if s.cancel != nil {
		s.cancel()
	}
	<-s.commitDoneC
	if s.pdb != nil {
		if err := s.pdb.Close(); err != nil {
			return fmt.Errorf("failed to close db: %w", err)
		}
	}
	return nil
}

func (s *metadataKV) applyBatch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error {
	batch := s.pdb.NewBatch()
	defer func() {
		if err := batch.Close(); err != nil {
			s.logger.Warn("failed to close batch", zap.Error(err))
		}
	}()

	// Track keys for listener notification
	var modifiedKeys []keyChange

	for _, kv := range writes {
		if err := batch.Set(kv[0], kv[1], nil); err != nil {
			return fmt.Errorf("could not set key %s: %w", string(kv[0]), err)
		}
		modifiedKeys = append(modifiedKeys, keyChange{key: kv[0], value: kv[1]})
	}
	for _, k := range deletes {
		if err := batch.Delete(k, nil); err != nil {
			return fmt.Errorf("could not delete key %s: %w", string(k), err)
		}
		modifiedKeys = append(modifiedKeys, keyChange{key: k, isDelete: true})
	}
	if err := s.pdb.Apply(batch, pebble.Sync); err != nil {
		return fmt.Errorf("could not apply batch: %w", err)
	}

	// Notify listeners after successful commit
	s.notifyListeners(ctx, modifiedKeys)

	return nil
}

// RegisterKeyPattern registers a pattern-based listener for key changes.
func (s *metadataKV) RegisterKeyPattern(pattern string, handler func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error) error {
	kp, err := NewKeyPattern(pattern, handler)
	if err != nil {
		return fmt.Errorf("creating key pattern: %w", err)
	}

	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	s.keyPatterns = append(s.keyPatterns, kp)

	s.logger.Info("Registered key pattern listener",
		zap.String("pattern", pattern))

	return nil
}

// RegisterKeyPrefixListener registers a simple prefix-based listener for key changes.
func (s *metadataKV) RegisterKeyPrefixListener(prefix []byte, handler func(ctx context.Context, key, value []byte, isDelete bool) error) {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	s.keyPrefixListeners = append(s.keyPrefixListeners, KeyPrefixListener{
		Prefix:  prefix,
		Handler: handler,
	})

	s.logger.Info("Registered key prefix listener",
		zap.ByteString("prefix", prefix))
}

// ClearKeyListeners removes all registered key listeners.
func (s *metadataKV) ClearKeyListeners() {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	s.keyPatterns = nil
	s.keyPrefixListeners = nil

	s.logger.Info("Cleared all key listeners")
}

func (s *metadataKV) notifyListeners(_ context.Context, modifiedKeys []keyChange) {
	s.listenerMu.RLock()
	defer s.listenerMu.RUnlock()

	if len(s.keyPatterns) == 0 && len(s.keyPrefixListeners) == 0 {
		return
	}

	// Use a detached context for listener goroutines. The incoming context is
	// scoped to commit processing and may be cancelled before async handlers
	// finish their work.
	listenerCtx := context.Background()

	for _, change := range modifiedKeys {
		for _, pattern := range s.keyPatterns {
			matched, params, err := pattern.Match(change.key)
			if err != nil {
				s.logger.Error("Error matching key pattern",
					zap.String("pattern", pattern.Pattern),
					zap.String("key", types.FormatKey(change.key)),
					zap.Error(err))
				continue
			}

			if matched {
				go func(p *KeyPattern, c keyChange, prms map[string]string) {
					if err := p.Handler(listenerCtx, c.key, c.value, c.isDelete, prms); err != nil {
						s.logger.Error("Key pattern listener error",
							zap.String("pattern", p.Pattern),
							zap.String("key", types.FormatKey(c.key)),
							zap.Error(err))
					}
				}(pattern, change, params)
			}
		}

		for _, listener := range s.keyPrefixListeners {
			if listener.Match(change.key) {
				go func(l KeyPrefixListener, c keyChange) {
					if err := l.Handler(listenerCtx, c.key, c.value, c.isDelete); err != nil {
						s.logger.Error("Key prefix listener error",
							zap.String("prefix", types.FormatKey(l.Prefix)),
							zap.String("key", types.FormatKey(c.key)),
							zap.Error(err))
					}
				}(listener, change)
			}
		}
	}
}
