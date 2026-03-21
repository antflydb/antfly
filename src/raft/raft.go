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

//go:generate protoc --go_out=. --go_opt=paths=source_relative afraft.proto
package raft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	// Needed for transport layer currently
	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

type Transport interface {
	ErrorC() <-chan error
	Send(shardID types.ID, msgs []raftpb.Message)
	GetSnapshot(ctx context.Context, shardID types.ID, snapStore snapstore.SnapStore, id string) error
	AddPeer(shardID, nodeID types.ID, us []string)
	// ForwardProposal(shardID, nodeID types.ID, proposal *Proposal) error
	RemovePeer(shardID, nodeID types.ID)
	ServeRaft(shardID types.ID, shard Raft, peers []raft.Peer) error
	StopServeRaft(shardID types.ID)
}

type Commit struct {
	Data       [][]byte
	ApplyDoneC chan<- struct{}
}

type Proposal struct {
	Data         []byte
	ProposeDoneC chan<- error
}

type ConfChangeProposal struct {
	ConfChange   raftpb.ConfChange
	ProposeDoneC chan<- error

	// ApplyCallbackID is a unique identifier for this conf change proposal.
	// If set (non-zero UUID), the raft node will signal the corresponding callback
	// when the conf change has been applied to the state machine.
	ApplyCallbackID uuid.UUID
}

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan *Proposal           // proposed messages (k,v)
	confChangeC <-chan *ConfChangeProposal // proposed cluster config changes
	commitC     chan<- *Commit             // entries committed to log (k,v)

	errorMu sync.Mutex
	errorC  chan<- error // errors from raft session

	initErrMu sync.RWMutex
	initErr   error

	// confChangeCallbacks tracks conf change proposals waiting for application.
	// Maps UUID to channel that is closed when the conf change is applied.
	confChangeCallbacks *xsync.Map[uuid.UUID, chan struct{}]

	shardID               types.ID           // raft cluster ID for request forwarding
	id                    types.ID           // client ID for raft session
	isLeader              bool               // true if this node is the leader
	peers                 common.Peers       // raft peer URLs
	timestamp             []byte             // Timestamp for the conf change context (mvcc timestamp)
	join                  bool               // node is joining an existing cluster
	raftLogDir            string             // path to Pebble Raft Log storage directory
	cache                 *pebbleutils.Cache // shared Pebble block cache (may be nil)
	createStorageSnapshot func(id string) error
	snapStore             snapstore.SnapStore
	clock                 clock.Clock

	leaderFactory  func(ctx context.Context) error
	leaderEg       *errgroup.Group
	leaderEgCancel context.CancelFunc

	initWithStorageSnapshot   string
	disableProposalForwarding bool // if true, proposals are not forwarded to other nodes
	disableAsyncStorageWrites bool // if true, async storage writes are disabled

	confStateMu   sync.RWMutex
	confState     raftpb.ConfState
	snapshotIndex atomic.Uint64
	appliedIndex  atomic.Uint64

	// raft backing for the commit/error channel
	nodeMu         sync.RWMutex
	node           raft.Node
	raftLogStorage *PebbleStorage

	snapshotterReady chan func() (string, error) // signals when snapshotter is ready

	snapCount uint64
	transport Transport
	stopc     chan struct{} // signals proposal channel closed
	stopOnce  sync.Once

	serveCtxMu     sync.Mutex
	serveCtxCancel context.CancelFunc

	logger *zap.Logger
}

var DefaultSnapshotCount uint64 = 10000

var messagePool = sync.Pool{
	New: func() any {
		return make([]raftpb.Message, 0, 256)
	},
}

type RaftNode interface {
	GetSnapshotID(ctx context.Context) (string, error)
	Status() raft.Status
	Peers() map[types.ID]struct{}
	SnapshotIndex() uint64
	TransferLeadership(ctx context.Context, target types.ID)
	IsIDRemoved(id uint64) bool
	SetLeaderFactory(f func(ctx context.Context) error)
	Shutdown()
	// GetConfChangeCallback returns the channel for a registered conf change callback.
	// Returns nil if no callback is registered for the given ID.
	GetConfChangeCallback(id uuid.UUID) <-chan struct{}
}

func (r *raftNode) SetLeaderFactory(f func(ctx context.Context) error) {
	r.logger.Debug("Setting leader factory", zap.Bool("isLeader", r.isLeader))
	r.leaderFactory = f
	if r.isLeader {
		r.startLeader()
	}
}

func (r *raftNode) Shutdown() {
	r.requestStop()
}

// GetConfChangeCallback returns the channel for a registered conf change callback.
// Returns nil if no callback is registered for the given ID.
func (r *raftNode) GetConfChangeCallback(id uuid.UUID) <-chan struct{} {
	if ch, ok := r.confChangeCallbacks.Load(id); ok {
		return ch
	}
	return nil
}

func (r *raftNode) startLeader() {
	r.logger.Debug("Starting leader")
	if r.leaderFactory == nil {
		return
	}
	var ctx context.Context
	ctx, r.leaderEgCancel = context.WithCancel(context.Background())
	r.leaderEg, ctx = errgroup.WithContext(ctx)
	r.leaderEg.Go(func() error {
		return r.leaderFactory(ctx)
	})
}

func (r *raftNode) stopLeader() error {
	r.logger.Debug("Stopping leader")
	if r.leaderEg != nil {
		r.leaderEgCancel()
		if err := r.leaderEg.Wait(); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil // Normal cancellation
			}
			return fmt.Errorf("stopping leader: %w", err)
		}
	}
	return nil
}

func (r *raftNode) GetSnapshotID(ctx context.Context) (string, error) {
	<-r.snapshotterReady
	if err := r.getInitErr(); err != nil {
		return "", err
	}
	return r.loadSnapshot(ctx)
}

func (r *raftNode) SnapshotIndex() uint64 {
	return r.snapshotIndex.Load()
}

func (r *raftNode) Status() raft.Status {
	r.nodeMu.RLock()
	node := r.node
	r.nodeMu.RUnlock()
	if node == nil {
		return raft.Status{}
	}
	return node.Status()
}

func (r *raftNode) Peers() map[types.ID]struct{} {
	r.nodeMu.RLock()
	node := r.node
	r.nodeMu.RUnlock()
	if node == nil {
		return nil
	}

	peers := map[types.ID]struct{}{}
	for id := range node.Status().Config.Voters.IDs() {
		peers[types.ID(id)] = struct{}{}
	}
	return peers
}

type RaftNodeConfig struct {
	Peers                   common.Peers
	RaftLogDir              string
	SnapStore               snapstore.SnapStore
	Join                    bool
	Timetstamp              []byte // Timestamp for the conf change context (mvcc timestamp)
	InitWithStorageSnapshot string
	// Disabled is the default for us
	EnableProposalForwarding  bool
	DisableAsyncStorageWrites bool
	Cache                     *pebbleutils.Cache
	Clock                     clock.Clock
}

// NewRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRaftNode(logger *zap.Logger, shardID types.ID, id types.ID,
	config RaftNodeConfig,
	rs Transport,
	createStorageSnapshot func(id string) error,
	proposeC <-chan *Proposal,
	confChangeC <-chan *ConfChangeProposal,
) (<-chan *Commit, <-chan error, RaftNode) {
	commitC := make(chan *Commit)
	// Buffer errorC to allow initialization errors to be sent without blocking.
	// This is important because startRaft runs in a goroutine and may fail before
	// the caller sets up the error handler.
	errorC := make(chan error, 1)
	clk := config.Clock
	if clk == nil {
		clk = clock.RealClock{}
	}

	rc := &raftNode{
		shardID:                   shardID,
		proposeC:                  proposeC,
		confChangeC:               confChangeC,
		commitC:                   commitC,
		errorC:                    errorC,
		id:                        id,
		peers:                     config.Peers,
		timestamp:                 config.Timetstamp,
		join:                      config.Join,
		raftLogDir:                config.RaftLogDir,
		cache:                     config.Cache,
		snapStore:                 config.SnapStore,
		clock:                     clk,
		initWithStorageSnapshot:   config.InitWithStorageSnapshot,
		disableProposalForwarding: !config.EnableProposalForwarding,
		disableAsyncStorageWrites: config.DisableAsyncStorageWrites,
		createStorageSnapshot:     createStorageSnapshot,
		snapCount:                 DefaultSnapshotCount,
		stopc:                     make(chan struct{}),
		confChangeCallbacks:       xsync.NewMap[uuid.UUID, chan struct{}](),

		logger: logger.Named("raft"),

		snapshotterReady: make(chan func() (string, error), 1),

		transport: rs,
		// rest of structure populated after WAL replay
	}
	go rc.startRaft()
	return commitC, errorC, rc
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	appliedIndex := rc.appliedIndex.Load()
	if firstIdx > appliedIndex+1 {
		rc.logger.Fatal("first index of committed entry should be <= appliedIndex+1",
			zap.Uint64("firstIdx", firstIdx),
			zap.Uint64("appliedIndex", appliedIndex))
	}
	if appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(
	ents []raftpb.Entry,
) (applyDoneCh <-chan struct{}, err error) {
	if len(ents) == 0 {
		return nil, nil
	}
	data := make([][]byte, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			data = append(data, ents[i].Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				rc.logger.Error("Failed to unmarshal ConfChange", zap.Error(err))
				continue
			}
			rc.logger.Debug("Received ConfChange",
				zap.Uint64("confChangeID", cc.ID),
				zap.Uint64("nodeID", cc.NodeID),
				zap.String("type", cc.Type.String()))
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					ccc := &ConfChangeContext{}
					if err := proto.Unmarshal(cc.Context, ccc); err != nil {
						rc.logger.Error("Failed to unmarshal ConfChangeContext", zap.Error(err))
						continue
					}
					confState := rc.node.ApplyConfChange(cc)
					rc.confStateMu.Lock()
					rc.confState = *confState
					rc.confStateMu.Unlock()
					if cc.NodeID != uint64(rc.id) {
						rc.transport.AddPeer(rc.shardID, types.ID(cc.NodeID), []string{ccc.GetUrl()})
						_ = rc.raftLogStorage.AddPeers(
							common.Peer{ID: types.ID(cc.NodeID), URL: ccc.GetUrl()},
						)
					}
					// Signal callback if one is registered for this conf change
					rc.maybeSignalConfChangeCallback(ccc)
				}
			case raftpb.ConfChangeRemoveNode:
				// Parse context to get callback info (needed for both self and other removal)
				var ccc *ConfChangeContext
				if len(cc.Context) > 0 {
					ccc = &ConfChangeContext{}
					if err := proto.Unmarshal(cc.Context, ccc); err != nil {
						rc.logger.Error("Failed to unmarshal ConfChangeContext", zap.Error(err))
						ccc = nil
					}
				}

				if cc.NodeID == uint64(rc.id) {
					// Removing self
					if ccc != nil {
						// If the timestamp is not older than our current timestamp, we can proceed
						// with the removal.
						if string(rc.timestamp) > string(ccc.GetTimestamp()) {
							rc.logger.Debug(
								"Received ConfChange to remove self with older timestamp, ignoring",
								zap.Uint64("removedNodeID", cc.NodeID),
								zap.String("contextURL", ccc.GetUrl()),
								zap.ByteString("timestamp", ccc.GetTimestamp()),
							)
							continue
						}
						rc.logger.Debug(
							"Received ConfChange to remove self with valid timestamp",
							zap.Uint64("removedNodeID", cc.NodeID),
							zap.String("contextURL", ccc.GetUrl()),
							zap.ByteString("timestamp", ccc.GetTimestamp()),
							zap.ByteString("currentTimestamp", rc.timestamp),
						)
					}
					// FIXME (ajr) If this node was removed recently, it can replay conf
					// changes that include it's removal.
					rc.logger.Info("Received ConfChange to remove self")
					rc.confStateMu.Lock()
					rc.confState = *rc.node.ApplyConfChange(cc)
					rc.confStateMu.Unlock()
					_ = rc.raftLogStorage.RemovePeer(types.ID(cc.NodeID))
					// Signal callback before shutting down
					rc.maybeSignalConfChangeCallback(ccc)
					// If we strictly need to stop, uncomment the lines below and handle the shutdown sequence.
					rc.logger.Info("This node has been removed from the raft group! Shutting down.")
					// FIXME/TODO (ajr) Forcing a snapshot on every removal is not ideal, because we could be removing nodes a lot when
					// autoscaling up or down.
					return nil, ErrRemoved
				} else {
					// Removing another node
					rc.confStateMu.Lock()
					rc.confState = *rc.node.ApplyConfChange(cc)
					rc.confStateMu.Unlock()
					rc.transport.RemovePeer(rc.shardID, types.ID(cc.NodeID))
					// Signal callback if one is registered for this conf change
					rc.maybeSignalConfChangeCallback(ccc)
				}
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &Commit{data, applyDoneC}:
		case <-rc.stopc:
			return nil, ErrStopped
		}
	}

	// after commit, update appliedIndex
	rc.appliedIndex.Store(ents[len(ents)-1].Index)

	return applyDoneC, nil
}

var ErrStopped = errors.New("raft node stopped")

// maybeSignalConfChangeCallback signals the callback for a conf change if one is registered.
// It parses the UUID from the conf change context and closes the corresponding callback channel.
func (rc *raftNode) maybeSignalConfChangeCallback(ccc *ConfChangeContext) {
	if ccc == nil || ccc.GetUuid() == "" {
		return
	}
	callbackID, err := uuid.Parse(ccc.GetUuid())
	if err != nil {
		// Not a valid UUID, might be used for something else
		return
	}
	if ch, ok := rc.confChangeCallbacks.LoadAndDelete(callbackID); ok {
		close(ch)
		rc.logger.Debug("Signaled conf change callback",
			zap.String("callbackID", callbackID.String()))
	}
}

func (rc *raftNode) loadSnapshot(ctx context.Context) (string, error) {
	if rc.raftLogStorage == nil {
		if err := rc.getInitErr(); err != nil {
			return "", err
		}
		return "", errors.New("raft log storage not initialized")
	}

	rfSnap, err := rc.raftLogStorage.LoadSnapshot()
	if errors.Is(err, pebble.ErrClosed) {
		rc.logger.Warn("Raft log storage is closed, cannot load snapshot", zap.Error(err))
		return "", err
	}
	if errors.Is(err, pebble.ErrNotFound) {
		// TODO (ajr) Add some more validation here maybe?
		if len(rc.initWithStorageSnapshot) > 0 {
			rc.logger.Info(
				"Initializing with provided DB archive",
				zap.String("archiveID", rc.initWithStorageSnapshot),
			)
			if err := rc.maybeFetchStorageSnapshot(ctx, rc.initWithStorageSnapshot); err != nil {
				rc.logger.Fatal(
					"Failed to fetch initial snapshot",
					zap.String("snapshotID", rc.initWithStorageSnapshot),
					zap.Error(err),
				)
			}
			return rc.initWithStorageSnapshot, nil
		}
		rc.logger.Info("No existing snapshot found and no initial archive provided")
		return "", err // Return pebble.ErrNotFound
	}
	if err != nil {
		rc.logger.Fatal("Error loading snapshot", zap.Error(err))
	}
	rc.logger.Info("Loaded snapshot from Raft storage", zap.Uint64("index", rfSnap.Metadata.Index))
	var snapshotMeta common.StorageSnapshotMetadata
	if err := proto.Unmarshal(rfSnap.Data, &snapshotMeta); err != nil {
		rc.logger.Fatal("Failed to unmarshal snapshot metadata", zap.Error(err))
	}
	if err := rc.maybeFetchStorageSnapshot(ctx, snapshotMeta.GetID()); err != nil {
		rc.logger.Fatal(
			"Failed to fetch storage snapshot referenced in Raft snapshot",
			zap.String("snapshotID", snapshotMeta.GetID()),
			zap.Error(err),
		)
	}
	return snapshotMeta.GetID(), nil
}

func (rc *raftNode) setInitErr(err error) {
	rc.initErrMu.Lock()
	defer rc.initErrMu.Unlock()
	rc.initErr = err
}

func (rc *raftNode) getInitErr() error {
	rc.initErrMu.RLock()
	defer rc.initErrMu.RUnlock()
	return rc.initErr
}

func (rc *raftNode) writeError(err error) {
	rc.errorMu.Lock()
	defer rc.errorMu.Unlock()
	if rc.errorC == nil {
		return
	}
	select {
	case rc.errorC <- err:
	default:
		rc.logger.Warn("errorC full, dropping error", zap.Error(err))
	}
}

// signalInitFailure is called when startRaft encounters an unrecoverable error
// during initialization. It logs the error, signals through errorC, and closes
// channels so callers don't hang waiting.
func (rc *raftNode) signalInitFailure(err error) {
	rc.setInitErr(err)
	rc.logger.Error("Raft initialization failed", zap.Error(err))

	// Try to signal error - use select to avoid blocking if buffer is full
	select {
	case rc.errorC <- err:
	default:
		rc.logger.Warn("Could not signal init error (errorC buffer full)")
	}

	// Close channels to unblock any waiting callers
	rc.errorMu.Lock()
	if rc.errorC != nil {
		close(rc.errorC)
		rc.errorC = nil
	}
	rc.errorMu.Unlock()

	close(rc.snapshotterReady)
	close(rc.commitC)
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (rc *raftNode) startRaft() {
	oldLogExists, err := dirExists(rc.raftLogDir)
	if err != nil {
		rc.signalInitFailure(fmt.Errorf("checking raft log directory %s: %w", rc.raftLogDir, err))
		return
	}

	// Initialize Pebble storage
	pebbleLogStorage, err := NewPebbleStorage(rc.logger, rc.raftLogDir, rc.cache)
	if err != nil {
		rc.signalInitFailure(fmt.Errorf("creating pebble storage at %s: %w", rc.raftLogDir, err))
		return
	}
	rc.raftLogStorage = pebbleLogStorage
	rc.logger.Info(
		"Pebble log storage initialized",
		zap.String("path", rc.raftLogDir),
		zap.Bool("existed", oldLogExists),
	)

	// Use noop logger for etcd raft internals unless debug level is enabled.
	// This suppresses verbose INFO logs from the raft library during normal operation.
	var raftLogger *zap.Logger
	if rc.logger.Core().Enabled(zapcore.DebugLevel) {
		raftLogger = rc.logger.Named("internals")
	} else {
		raftLogger = zap.NewNop()
	}
	c := &raft.Config{
		// DisableConfChangeValidation: true,
		StepDownOnRemoval: true,

		PreVote: true,

		// The raft.go library can drop droposals silently if this is false.
		DisableConfChangeValidation: true,

		// TODO (ajr) Experiement with this
		// Applied: ...,

		// TODO (ajr) Figure out how the whole ReadOption system works and if we need it?
		// ReadOnlyOption: raft.ReadOnlyLeaseBased,
		// Tied to the option above, we use CheckQuorum to ensure that the leader steps down
		// if it cannot reach a quorum of followers. Which is important to ensure we only have one leader
		// running for lease based loops.
		CheckQuorum: true,

		// DiableProposalForwarding forces dropped proposals to always return an error
		DisableProposalForwarding: rc.disableProposalForwarding,

		AsyncStorageWrites: !rc.disableAsyncStorageWrites,
		ID:                 uint64(rc.id),
		// Defaults were set to 10 and 1 respectively.
		ElectionTick:  30,
		HeartbeatTick: 1,
		Storage:       pebbleLogStorage,
		MaxSizePerMsg: math.MaxUint64,
		// MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30, // 1GB
		MaxCommittedSizePerReady:  math.MaxUint64,
		// Use the raftNode's logger for Raft library logging
		Logger:      logger.NewZapWrapper(raftLogger), // Add a name component for clarity
		TraceLogger: nil,                              // Disable trace logging for now, enable if needed
	}

	rpeers := make([]raft.Peer, 0, len(rc.peers))
	peers := rc.peers
	if len(peers) == 0 && (oldLogExists || rc.join) {
		peersMap, err := rc.raftLogStorage.ListPeers()
		if err != nil {
			rc.logger.Fatal("Failed to list peers from storage", zap.Error(err))
		}
		for peerID, url := range peersMap {
			peers = append(peers, common.Peer{ID: peerID, URL: url})
		}
		rc.logger.Info("No peers provided, using peers from existing storage",
			zap.Stringer("peers", peers),
		)
	}
	for _, peer := range peers {
		ccc := ConfChangeContext_builder{
			Url:       peer.URL,
			Timestamp: rc.timestamp,
			// Uuid:      uuid.New().String(),
		}.Build()
		context, err := proto.Marshal(ccc)
		if err != nil {
			rc.logger.Fatal("Failed to marshal ConfChangeContext", zap.Error(err))
		}
		rpeers = append(rpeers, raft.Peer{ID: uint64(peer.ID), Context: context})
	}
	rc.nodeMu.Lock()
	if oldLogExists || rc.join {
		rc.logger.Info("Restarting or joining Raft node",
			zap.Bool("join", rc.join),
			zap.Bool("oldLogExists", oldLogExists),
		)
		// If joining a cluster but no existing log, initialize ConfState and peers with
		// provided peers. This ensures the new node has the correct voter configuration
		// before receiving ConfChanges from the leader. Without this, a joining node
		// starts with an empty ConfState and may panic when applying a RemovePeer
		// ConfChange if the AddPeer for the removed node was committed before this node
		// joined.
		if rc.join && !oldLogExists && len(peers) > 0 {
			// Add peers to storage (same as StartNode path)
			if err := rc.raftLogStorage.AddPeers(peers...); err != nil {
				rc.logger.Fatal("Failed to add initial peers to storage for joining node", zap.Error(err))
			}
			// Initialize ConfState with all peers as voters
			voterIDs := make([]uint64, len(peers))
			for i, peer := range peers {
				voterIDs[i] = uint64(peer.ID)
			}
			initialConfState := raftpb.ConfState{Voters: voterIDs}
			if err := rc.raftLogStorage.SaveRaftState(nil, initialConfState, raftpb.HardState{}, nil); err != nil {
				rc.logger.Fatal("Failed to initialize ConfState for joining node", zap.Error(err))
			}
			rc.logger.Info("Initialized ConfState for joining node",
				zap.Uint64s("voters", voterIDs),
			)
		}
		rc.node = raft.RestartNode(c)
	} else {
		if err := rc.raftLogStorage.AddPeers(peers...); err != nil {
			rc.logger.Fatal("Failed to add initial peers to storage", zap.Error(err))
		}
		rc.logger.Info("Starting new Raft node", zap.Any("peers", rc.peers))
		rc.node = raft.StartNode(c, rpeers)
	}
	rc.nodeMu.Unlock()

	if err := rc.transport.ServeRaft(rc.shardID, rc, rpeers); err != nil {
		rc.logger.Error("Error serving Raft", zap.Error(err))
	}

	// signal replay has finished
	close(rc.snapshotterReady)

	go rc.serveChannels(context.TODO())
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	// Stop leader work first, before tearing down node/storage it may depend on.
	if rc.leaderEg != nil {
		rc.leaderEgCancel()
		if err := rc.leaderEg.Wait(); err != nil {
			if !errors.Is(err, context.Canceled) {
				rc.logger.Error("Error in leader goroutine", zap.Error(err))
			}
		}
	}
	rc.logger.Info("Raft node stopping")
	rc.transport.StopServeRaft(rc.shardID)

	rc.errorMu.Lock()
	if rc.errorC != nil {
		close(rc.errorC)
		rc.errorC = nil
	}
	rc.errorMu.Unlock()

	rc.nodeMu.RLock()
	node := rc.node
	rc.nodeMu.RUnlock()
	if node != nil {
		node.Stop()
		rc.logger.Info("Raft node stopped")
	}
	if rc.raftLogStorage != nil {
		if err := rc.raftLogStorage.Close(); err != nil {
			rc.logger.Error("Error closing Pebble storage", zap.Error(err))
		}
	}
	rc.logger.Info("Closed raft log storage")
}

func (rc *raftNode) requestStop() {
	rc.stopOnce.Do(func() {
		rc.serveCtxMu.Lock()
		cancel := rc.serveCtxCancel
		rc.serveCtxCancel = nil
		rc.serveCtxMu.Unlock()
		if cancel != nil {
			cancel()
		}
		close(rc.stopc)
		rc.stop()
	})
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	currentSnapshotIndex := snapshotToSave.Metadata.Index
	rc.logger.Info("Publishing snapshot", zap.Uint64("index", currentSnapshotIndex))
	defer rc.logger.Info("Finished publishing snapshot", zap.Uint64("index", currentSnapshotIndex))

	if currentSnapshotIndex <= rc.appliedIndex.Load() {
		rc.logger.Fatal("Snapshot index should be greater than applied index",
			zap.Uint64("snapshotIndex", currentSnapshotIndex),
			zap.Uint64("appliedIndex", rc.appliedIndex.Load()))
	}
	// 1. Extract the storage snapshot metadata from the Raft snapshot data
	var snapshotMeta common.StorageSnapshotMetadata
	if err := proto.Unmarshal(snapshotToSave.Data, &snapshotMeta); err != nil {
		rc.logger.Fatal("Failed to unmarshal snapshot metadata from Raft snapshot", zap.Error(err))
	}

	// 2. Ensure the actual storage snapshot file exists locally, fetching if necessary
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	go func() {
		select {
		case <-rc.stopc:
			cancel()
			return
		case <-ctx.Done():
			return
		}
	}()
	if err := rc.maybeFetchStorageSnapshot(ctx, snapshotMeta.GetID()); err != nil {
		if errors.Is(err, context.Canceled) {
			rc.logger.Warn("Stopped while trying to fetch snapshot")
			return
		}
		// If we can't fetch the snapshot data, we can't proceed.
		rc.logger.Fatal(
			"Failed to ensure local storage snapshot exists",
			zap.String("snapshotID", snapshotMeta.GetID()),
			zap.Error(err),
		)
	}

	// 3. Signal the application layer (e.g., kvstore) to apply the snapshot state
	// Sending nil signals a snapshot load is required.
	select {
	case rc.commitC <- nil:
		rc.logger.Info(
			"Signaled application layer to load snapshot",
			zap.String("snapshotID", snapshotMeta.GetID()),
		)
	case <-rc.stopc:
		rc.logger.Warn("Stopped while trying to signal snapshot application")
		return
	}

	rc.confStateMu.Lock()
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.confStateMu.Unlock()
	rc.snapshotIndex.Store(snapshotToSave.Metadata.Index)
	rc.appliedIndex.Store(snapshotToSave.Metadata.Index)
}

// Fetch a KV snapshot from another node
func (rc *raftNode) maybeFetchStorageSnapshot(ctx context.Context, id string) error {
	return rc.transport.GetSnapshot(ctx, rc.shardID, rc.snapStore, id)
}

var SnapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	appliedIndex := rc.appliedIndex.Load()
	snapshotIndex := rc.snapshotIndex.Load()

	// Take a snapshot if we have `appliedIndex > 0` and don't have a snapshot yet
	if rc.initWithStorageSnapshot == "" || snapshotIndex != 0 || appliedIndex == 0 {
		if appliedIndex-snapshotIndex <= rc.snapCount {
			return
		}
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	rc.logger.Info("Starting storage snapshot",
		zap.Uint64("appliedIndex", appliedIndex),
		zap.Uint64("lastSnapshotIndex", snapshotIndex))

	// Get the current snapshot ID before creating a new one
	var oldSnapshotID string
	oldSnapshot, err := rc.raftLogStorage.LoadSnapshot()
	if err == nil && oldSnapshot != nil && !raft.IsEmptySnap(*oldSnapshot) {
		var oldSnapshotMeta common.StorageSnapshotMetadata
		if err := proto.Unmarshal(oldSnapshot.Data, &oldSnapshotMeta); err == nil {
			oldSnapshotID = oldSnapshotMeta.GetID()
		}
	}

	var snapshotID string
	if snapshotIndex == 0 && rc.initWithStorageSnapshot != "" {
		snapshotID = rc.initWithStorageSnapshot
		rc.logger.Info(
			"Using init snapshot for raft snapshot",
			zap.String("snapshotID", snapshotID),
		)
	} else {
		// Generate a unique ID for the storage snapshot.
		// Using appliedIndex ensures snapshots are ordered and somewhat identifiable.
		snapshotID = fmt.Sprintf("snapshot-%d-%d-%d", rc.shardID, rc.id, appliedIndex)

		// Trigger the application layer to create its snapshot.
		if err := rc.createStorageSnapshot(snapshotID); err != nil {
			// If creating the storage snapshot fails, we cannot proceed with the Raft snapshot.
			rc.logger.Error(
				"Failed to create storage snapshot; Raft snapshot aborted",
				zap.String("snapshotID", snapshotID),
				zap.Error(err),
			)
			// Potentially panic or return an error depending on desired failure mode.
			// For now, just log and return, preventing Raft snapshot/compaction.
			return
		}
		rc.logger.Info("Successfully created storage snapshot", zap.String("snapshotID", snapshotID))
	}

	// Create Raft snapshot metadata pointing to the storage snapshot.
	snapshotMeta := common.StorageSnapshotMetadata_builder{
		ID: snapshotID,
	}.Build()
	snapshotData, err := proto.Marshal(snapshotMeta)
	if err != nil {
		// This should ideally not happen with a simple struct.
		rc.logger.Fatal("Failed to marshal snapshot metadata", zap.Error(err))
	}

	// Persist the Raft snapshot entry in the Raft log storage.
	rc.confStateMu.RLock()
	confState := rc.confState
	rc.confStateMu.RUnlock()
	if err := rc.raftLogStorage.CreateSnapshot(appliedIndex, &confState, snapshotData); err != nil {
		if errors.Is(err, pebble.ErrClosed) {
			// If the storage is closed, we cannot proceed with snapshotting.
			rc.logger.Error(
				"Raft log storage is closed; cannot create snapshot entry",
				zap.Error(err),
			)
			return
		}
		// This is a critical error, potentially leading to inconsistencies.
		rc.logger.Fatal("Failed to create Raft snapshot entry in storage", zap.Error(err))
	}
	rc.logger.Info("Successfully created Raft snapshot entry", zap.Uint64("index", appliedIndex))

	var compactIndex uint64
	if appliedIndex > SnapshotCatchUpEntriesN {
		compactIndex = appliedIndex - SnapshotCatchUpEntriesN
	}
	if compactIndex > 1 {
		if err := rc.raftLogStorage.Compact(compactIndex); err != nil {
			// ErrCompacted is expected if trying to compact already compacted entries.
			if !errors.Is(err, raft.ErrCompacted) {
				// Other errors during compaction are serious.
				rc.logger.Error(
					"Failed to compact Raft log",
					zap.Uint64("compactIndex", compactIndex),
					zap.Error(err),
				)
				// Potentially panic or handle more gracefully depending on the error.
			} else {
				rc.logger.Info("Raft log already compacted", zap.Uint64("compactIndex", compactIndex))
			}
		} else {
			rc.logger.Info("Successfully compacted Raft log", zap.Uint64("upToIndex", compactIndex))
		}
	}

	// Update the snapshot index tracker.
	rc.snapshotIndex.Store(appliedIndex)
	rc.logger.Info("Finished snapshot process", zap.Uint64("newSnapshotIndex", appliedIndex))

	// Remove old snapshot after successfully creating the new one
	if oldSnapshotID != "" && oldSnapshotID != snapshotID {
		if err := rc.snapStore.Delete(context.Background(), oldSnapshotID); err != nil {
			rc.logger.Warn("Failed to remove old snapshot",
				zap.String("oldSnapshotID", oldSnapshotID),
				zap.Error(err))
		} else {
			rc.logger.Info("Removed old snapshot",
				zap.String("oldSnapshotID", oldSnapshotID))
		}
	}
}

func (rc *raftNode) TransferLeadership(ctx context.Context, target types.ID) {
	rc.node.TransferLeadership(ctx, uint64(rc.id), uint64(target))
	rc.logger.Info("Transferred leadership", zap.Stringer("target", target))
}

var ErrRemoved = errors.New("ID removed from Raft group")

func (rc *raftNode) serveChannels(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	rc.serveCtxMu.Lock()
	rc.serveCtxCancel = cancel
	rc.serveCtxMu.Unlock()

	// Load the latest snapshot metadata from storage
	snap, err := rc.raftLogStorage.Snapshot()
	if err != nil && !errors.Is(err, raft.ErrSnapshotTemporarilyUnavailable) &&
		!errors.Is(err, pebble.ErrNotFound) {
		// ErrSnapshotTemporarilyUnavailable might happen if a snapshot is in progress.
		// ErrNotFound happens on initial start.
		// Other errors are fatal.
		rc.logger.Fatal("Failed to load initial snapshot", zap.Error(err))
	}

	// Load the hard state and conf state from storage
	hardState, confState, err := rc.raftLogStorage.InitialState()
	if err != nil {
		rc.logger.Fatal("Failed to load initial Raft state", zap.Error(err))
	}

	// Close storage only when the raft node fully stops, not here.
	rc.confState = confState
	rc.snapshotIndex.Store(snap.Metadata.Index)
	// FIXME (ajr) Save the confState and with the hardState so we can recover conf together
	rc.appliedIndex.Store(max(snap.Metadata.Index, hardState.Commit))
	rc.logger.Info("Initialized from snapshot", zap.Uint64("index", rc.snapshotIndex.Load()))
	ticker := rc.clock.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer rc.requestStop()
	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case <-rc.stopc:
				return
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
					continue
				}

				// blocks until accepted by raft state machine
				// FIXME (ajr) Handle proposal errors and retries
				err := rc.node.Propose(ctx, prop.Data)
				// if err != nil {
				// 	rc.logger.Debug("Proposal failed", zap.Error(err))
				// }
				if prop.ProposeDoneC != nil {
					select {
					case prop.ProposeDoneC <- err:
					default:
					}
					close(prop.ProposeDoneC)
				}
			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
					continue
				}

				confChangeCount++
				cc.ConfChange.ID = confChangeCount

				// If a callback ID is provided, register it before proposing.
				// Store the callback ID in the conf change context's uuid field so we can
				// retrieve it when the conf change is applied.
				if cc.ApplyCallbackID != uuid.Nil {
					callbackCh := make(chan struct{})
					rc.confChangeCallbacks.Store(cc.ApplyCallbackID, callbackCh)
					// Embed the callback ID in the conf change context's uuid field
					ccc := &ConfChangeContext{}
					if len(cc.ConfChange.Context) > 0 {
						if err := proto.Unmarshal(cc.ConfChange.Context, ccc); err != nil {
							rc.logger.Error("Failed to unmarshal existing ConfChangeContext", zap.Error(err))
						}
					}
					ccc.SetUuid(cc.ApplyCallbackID.String())
					newContext, err := proto.Marshal(ccc)
					if err != nil {
						rc.logger.Error("Failed to marshal ConfChangeContext with callback ID", zap.Error(err))
					} else {
						cc.ConfChange.Context = newContext
					}
				}

				rc.logger.Info("Proposing conf change",
					zap.Stringer("type", &cc.ConfChange.Type),
					zap.Stringer("confChange", types.ID(cc.ConfChange.NodeID)))
				err := rc.node.ProposeConfChange(ctx, cc.ConfChange)
				if err != nil {
					rc.logger.Warn("Failed to propose conf change", zap.Error(err))
					// Clean up the callback if proposal failed
					if cc.ApplyCallbackID != uuid.Nil {
						if ch, ok := rc.confChangeCallbacks.LoadAndDelete(cc.ApplyCallbackID); ok {
							close(ch)
						}
					}
				}
				if cc.ProposeDoneC != nil {
					select {
					case cc.ProposeDoneC <- err:
					default:
					}
					close(cc.ProposeDoneC)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		rc.logger.Info("Closing stop channel for Raft")
		rc.requestStop()
	}()

	defer close(rc.commitC)

	toAppend := make(chan raftpb.Message, 10)
	toApply := make(chan raftpb.Message, 10)
	if !rc.disableAsyncStorageWrites {
		var wg sync.WaitGroup
		defer wg.Wait()
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer rc.requestStop()
			for {
				select {
				case msg := <-toAppend:
					rc.confStateMu.RLock()
					confState := rc.confState
					rc.confStateMu.RUnlock()
					if err := rc.raftLogStorage.SaveRaftState(msg.Snapshot, confState, raftpb.HardState{
						Term:   msg.Term,
						Vote:   msg.Vote,
						Commit: msg.Commit,
					}, msg.Entries); err != nil {
						select {
						case <-rc.stopc:
							return
						default:
							if !errors.Is(err, pebble.ErrClosed) {
								rc.logger.Error(
									"Failed to save Raft state including snapshot",
									zap.Error(err),
								)
								rc.writeError(err)
							}
							return
						}
					}
					if msg.Snapshot != nil {
						rc.publishSnapshot(*msg.Snapshot)
					}
					for _, m := range msg.Responses {
						if m.To == uint64(rc.id) {
							if err := rc.node.Step(ctx, m); err != nil {
								rc.logger.Error("Failed to step raft node", zap.Error(err))
								select {
								case <-rc.stopc:
								default:
									// FIXME (ajr) Happens if the raft group has stopped
									// I think there's a small race here when the Stop channel might not be closed yet
									// rc.writeError(err)
								}
								return
							}
						}
					}
					select {
					case <-rc.stopc:
						return
					default:
					}
					rc.processMessagesAndSend(msg.Responses)
				case <-rc.stopc:
					return
				}
			}
		}()

		// apply thread
		go func() {
			defer wg.Done()
			defer rc.requestStop()
			for {
				select {
				case msg := <-toApply:
					// Apply committed entries
					ents := rc.entriesToApply(msg.Entries)
					applyDoneC, err := rc.publishEntries(ents)
					if err != nil {
						select {
						case <-rc.stopc:
							return
						default:
						}
						if !errors.Is(err, ErrStopped) /* && !errors.Is(err, ErrRemoved) */ {
							rc.writeError(err)
						}
						return
					}
					for _, m := range msg.Responses {
						if m.To == uint64(rc.id) {
							if err := rc.node.Step(ctx, m); err != nil {
								rc.logger.Error("Failed to step raft node", zap.Error(err))
								// FIXME (ajr) Happens if the raft group has stopped
								// I think there's a small race here when the Stop channel might not be closed yet
								// rc.writeError(err)
								break
							}
						}
					}
					select {
					case <-rc.stopc:
						return
					default:
					}
					// Process and send outgoing messages
					rc.processMessagesAndSend(msg.Responses)
					rc.maybeTriggerSnapshot(applyDoneC)
				case <-rc.stopc:
					return
				}
			}
		}()
	}

	defer close(toApply)
	defer close(toAppend)

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C():
			rc.node.Tick()

		// store raft entries, then publish over commit channel
		case rd := <-rc.node.Ready():
			// After the call the ReadIndex, the applied state in ReadStates
			// is garunteed to be higher than the index in ReadStates.
			// rc.node.ReadIndex(ctx context.Context, rctx []byte)
			// rd.ReadStates
			if rd.SoftState != nil { // Check if SoftState changed
				if rd.RaftState == raft.StateLeader && rd.Lead == uint64(rc.id) {
					// Handle becoming leader
					rc.isLeader = true
					rc.startLeader()
				} else if rc.isLeader && (rd.Lead != uint64(rc.id) || rd.RaftState != raft.StateLeader) {
					// Handle losing leadership or being follower/candidate
					rc.isLeader = false
					if err := rc.stopLeader(); err != nil {
						rc.logger.Error("Error stopping leader", zap.Error(err))
					}
				}
			}
			if !rc.disableAsyncStorageWrites {
				select {
				case <-rc.stopc:
					return
				default:
				}
				for _, m := range rd.Messages {
					switch m.To {
					case raft.LocalAppendThread:
						select {
						case toAppend <- m:
						case <-rc.stopc:
							return
						}
					case raft.LocalApplyThread:
						select {
						case toApply <- m:
						case <-rc.stopc:
							return
						}
					default:
						select {
						case <-rc.stopc:
							return
						default:
						}
						rc.processMessagesAndSend([]raftpb.Message{m})
					}
				}
				continue
			}
			select {
			case <-rc.stopc:
				return
			default:
			}

			// Must save the snapshot file and WAL snapshot entry before saving any other entries
			// or hardstate to ensure that recovery after a snapshot restore is possible.
			rc.confStateMu.RLock()
			confState := rc.confState
			rc.confStateMu.RUnlock()
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.logger.Info("Received new snapshot from Raft",
					zap.Uint64("index", rd.Snapshot.Metadata.Index),
					zap.Uint64("term", rd.Snapshot.Metadata.Term))
				// Save the snapshot metadata and potentially trigger application layer processing
				if err := rc.raftLogStorage.SaveRaftState(&rd.Snapshot, confState, rd.HardState, rd.Entries); err != nil {
					rc.logger.Error("Failed to save Raft state including snapshot", zap.Error(err))
					rc.writeError(err)
					return
				}
				// Publish snapshot *after* saving Raft state
				rc.publishSnapshot(rd.Snapshot)
			} else if len(rd.Entries) > 0 || !raft.IsEmptyHardState(rd.HardState) {
				// Save HardState and Entries if the snapshot wasn't the primary update
				if err := rc.raftLogStorage.SaveRaftState(&rd.Snapshot, confState, rd.HardState, rd.Entries); err != nil {
					rc.logger.Error("Failed to save Raft state", zap.Error(err))
					rc.writeError(err)
					return
				}
			}
			// Process and send outgoing messages
			rc.processMessagesAndSend(rd.Messages)

			// Apply committed entries
			ents := rc.entriesToApply(rd.CommittedEntries)
			applyDoneC, err := rc.publishEntries(ents)
			if err != nil {
				if !errors.Is(err, ErrStopped) {
					rc.writeError(err)
				}
				return
			}
			rc.node.Advance()
			rc.maybeTriggerSnapshot(applyDoneC)
		case err := <-rc.transport.ErrorC():
			rc.writeError(err)
			return

		case <-rc.stopc:
			return
		}
	}
}

// then the confState included in the snapshot is out of date. so We need
// to update the confState before sending a snapshot to a follower.
func (rc *raftNode) processMessagesAndSend(ms []raftpb.Message) {
	// Get a message slice from the pool
	msgs := messagePool.Get().([]raftpb.Message)[:0] //nolint:staticcheck // SA6002
	msgs = rc.processMessages(msgs, ms)
	defer func() {
		// Return the slice to the pool after use
		messagePool.Put(msgs) //nolint:staticcheck // SA6002
	}()
	if len(msgs) == 0 {
		return
	}
	rc.transport.Send(rc.shardID, msgs)
}

func (rc *raftNode) processMessages(dst, ms []raftpb.Message) []raftpb.Message {
	rc.confStateMu.RLock()
	confState := rc.confState
	rc.confStateMu.RUnlock()
	for i := range ms {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = confState
		}
		if ms[i].To != uint64(rc.id) {
			dst = append(dst, ms[i])
		}
	}
	return dst
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	// Guard against a leader sending a commit index higher than our last
	// log index. This happens when a shard is removed (StopRaftGroup wipes
	// data) and re-added to the same node: the leader retains stale
	// progress (Match) while the follower restarts with an empty log.
	// etcd raft v3.6.0 does not cap the commit in handleHeartbeat, so
	// commitTo panics. storage.LastIndex() <= raftLog.lastIndex() always
	// holds, making this a safe (if sometimes conservative) bound.
	if m.Type == raftpb.MsgHeartbeat {
		if li, err := rc.raftLogStorage.LastIndex(); err == nil && m.Commit > li {
			rc.logger.Warn("Capping heartbeat commit to storage lastIndex",
				zap.Uint64("heartbeatCommit", m.Commit),
				zap.Uint64("storageLastIndex", li),
				zap.Uint64("from", m.From),
			)
			m.Commit = li
		}
	}
	return rc.node.Step(ctx, m)
}

func (rc *raftNode) IsIDRemoved(id uint64) bool {
	rc.confStateMu.RLock()
	voters := rc.confState.Voters
	rc.confStateMu.RUnlock()
	return !slices.Contains(voters, id)
}
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
