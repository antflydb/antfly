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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store"
	"github.com/cockroachdb/pebble/v2"
	"github.com/goccy/go-json"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type MetadataStore struct {
	logger *zap.Logger

	antflyConfig *common.Config
	config       *store.StoreInfo
	rs           raft.Transport
	clock        clock.Clock
	cache        *pebbleutils.Cache // shared Pebble block cache (may be nil)
	metadataKV   *metadataKV

	errorC <-chan error
}

// TODO (ajr)
// - Add GC of snapshots
// - Save state of shards in pebble so we can recover from a crash

type Options struct {
	Clock clock.Clock
}

func NewMetadataStore(
	zl *zap.Logger,
	antflyConfig *common.Config,
	rs raft.Transport,
	conf *store.StoreInfo,
	bootstrapPeers common.Peers,
	join bool,
	cache *pebbleutils.Cache,
) (*MetadataStore, error) {
	return NewMetadataStoreWithOptions(
		zl,
		antflyConfig,
		rs,
		conf,
		bootstrapPeers,
		join,
		cache,
		Options{},
	)
}

func NewMetadataStoreWithOptions(
	zl *zap.Logger,
	antflyConfig *common.Config,
	rs raft.Transport,
	conf *store.StoreInfo,
	bootstrapPeers common.Peers,
	join bool,
	cache *pebbleutils.Cache,
	opts Options,
) (*MetadataStore, error) {
	s := &MetadataStore{
		logger: zl.With(zap.String("module", "metadataStore")),

		antflyConfig: antflyConfig,
		config:       conf,
		rs:           rs,
		clock:        opts.Clock,
		cache:        cache,
	}
	return s, s.StartRaftGroup(bootstrapPeers, join)
}

func (m *MetadataStore) clockOrReal() clock.Clock {
	if m.clock == nil {
		return clock.RealClock{}
	}
	return m.clock
}

func (m *MetadataStore) SetLeaderFactory(f func(ctx context.Context) error) {
	m.metadataKV.SetLeaderFactory(f)
}

func (m *MetadataStore) ID() types.ID {
	return m.config.ID
}

func (m *MetadataStore) Info() *store.ShardInfo {
	return m.metadataKV.Info()
}

func (m *MetadataStore) Close() {
	m.logger.Info("Closing MetadataStore")
	if m.metadataKV != nil {
		if err := m.metadataKV.Close(); err != nil {
			m.logger.Error("Failed to close MetadataKV", zap.Error(err))
		}
	}
	m.logger.Info("Closed MetadataStore")
}

// ErrorC dynamically receives new error channels from shards and fans them in
func (m *MetadataStore) ErrorC() <-chan error {
	return m.errorC
}

func (s *MetadataStore) ProposeConfChange(cc raftpb.ConfChange) error {
	return s.metadataKV.ProposeConfChange(cc)
}

func (s *MetadataStore) Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error {
	return s.metadataKV.Batch(ctx, writes, deletes)
}

func (s *MetadataStore) Get(ctx context.Context, key []byte) ([]byte, io.Closer, error) {
	return s.metadataKV.Get(ctx, key)
}

func (s *MetadataStore) NewIter(
	ctx context.Context,
	opts *pebble.IterOptions,
) (*pebble.Iterator, error) {
	return s.metadataKV.NewIter(ctx, opts)
}

// RegisterKeyPattern registers a pattern-based listener for key changes on the leader.
// The handler will be called when keys matching the pattern are modified.
// Pattern examples:
// - "tables/{tableID}/shards/{shardID}" - named parameters
// - "tables/*/shards/*" - wildcard matching
// - "prefix/" - simple prefix matching
func (s *MetadataStore) RegisterKeyPattern(pattern string, handler func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error) error {
	return s.metadataKV.RegisterKeyPattern(pattern, handler)
}

// RegisterKeyPrefixListener registers a simple prefix-based listener for key changes on the leader.
// The handler will be called when keys with the specified prefix are modified.
func (s *MetadataStore) RegisterKeyPrefixListener(prefix []byte, handler func(ctx context.Context, key, value []byte, isDelete bool) error) {
	s.metadataKV.RegisterKeyPrefixListener(prefix, handler)
}

// ClearKeyListeners removes all registered key listeners.
// Useful for testing or when leadership changes.
func (s *MetadataStore) ClearKeyListeners() {
	s.metadataKV.ClearKeyListeners()
}

func (m *MetadataStore) StopRaftGroup(shardID types.ID) {
	m.logger.Info("Stopping Raft Group", zap.Stringer("shardID", shardID))
	// TODO (ajr) Delete the snapshots and pebble databases
	dataDir := m.antflyConfig.GetBaseDir()
	_ = os.RemoveAll(common.MetadataSnapDir(dataDir, m.config.ID))
	_ = os.RemoveAll(common.MetadataStorageDir(dataDir, m.config.ID))
	_ = os.RemoveAll(common.MetadataRaftLogDir(dataDir, m.config.ID))
	m.logger.Info("Deleted Raft Group disk footprint", zap.Stringer("shardID", shardID))
}

func (m *MetadataStore) createPassThroughChannels(
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

func (m *MetadataStore) StartRaftGroup(peers []common.Peer, join bool) error {
	m.logger.Debug("Starting Raft Group")
	metadataKVReady := make(chan struct{})
	defer close(metadataKVReady)
	createDBSnapshot := func(id string) error {
		<-metadataKVReady
		return m.metadataKV.createDBSnapshot(id)
	}
	proposeC := make(chan *raft.Proposal)
	confChangeC := make(chan *raft.ConfChangeProposal)
	dataDir := m.antflyConfig.GetBaseDir()
	snapStore, err := snapstore.NewLocalSnapStore(dataDir, 1, m.ID())
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}

	var commitC <-chan *raft.Commit
	var errorC <-chan error
	var raftNode raft.RaftNode

	// Check if we're running in swarm mode (single node, no Raft needed)
	// This is determined by having exactly one peer and that peer being ourselves
	swarmMode := len(peers) == 1 && peers[0].ID == m.config.ID

	if swarmMode {
		m.logger.Info("Starting metadata in swarm mode (bypassing Raft)")
		commitC, errorC = m.createPassThroughChannels(proposeC)
	} else {
		raftConf := raft.RaftNodeConfig{
			SnapStore:                 snapStore,
			DisableAsyncStorageWrites: true,
			RaftLogDir:                common.MetadataRaftLogDir(dataDir, m.config.ID),
			Peers:                     peers,
			Join:                      join,
			EnableProposalForwarding:  true,
			Cache:                     m.cache,
			Clock:                     m.clockOrReal(),
		}
		// Pass the store's logger to the raft node
		commitC, errorC, raftNode = raft.NewRaftNode(
			m.logger,
			1,
			m.config.ID,
			raftConf,
			m.rs,
			createDBSnapshot,
			proposeC,
			confChangeC,
		)
	}

	dbDir := common.MetadataStorageDir(dataDir, m.config.ID)

	// In swarm mode without Raft, provide a stub function that returns an empty snapshot ID
	var getSnapshotID func(ctx context.Context) (string, error)
	if raftNode != nil {
		getSnapshotID = raftNode.GetSnapshotID
	} else {
		getSnapshotID = func(ctx context.Context) (string, error) {
			return "", nil
		}
	}

	m.metadataKV, err = newMetadataKV(
		m.logger,
		m.antflyConfig,
		dbDir,
		snapStore,
		getSnapshotID,
		proposeC,
		commitC,
		errorC,
		m.cache,
	)
	if err != nil {
		return fmt.Errorf("creating leader raft wrapper: %w", err)
	}
	if raftNode != nil {
		m.metadataKV.raftNode = raftNode
		m.metadataKV.confChangeC = confChangeC
	}
	m.errorC = errorC
	m.logger.Info("Started Raft Group")
	return nil
}

// API handler for a http based key-value store backed by raft
type MetadataStoreAPI struct {
	logger *zap.Logger
	store  *MetadataStore
}

// NewMetadataStoreAPI creates a new MetadataStoreAPI
func NewMetadataStoreAPI(logger *zap.Logger, store *MetadataStore) *MetadataStoreAPI {
	return &MetadataStoreAPI{
		logger: logger,
		store:  store,
	}
}

type MetadataBatchRequest struct {
	Writes  [][2][]byte `json:"writes"`
	Deletes [][]byte    `json:"deletes"`
}

func (h *MetadataStoreAPI) handleBatch(w http.ResponseWriter, r *http.Request) {
	var batch MetadataBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		h.logger.Warn("Failed to read batch write request body for shard",
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Call the existing BatchWrite method on the shard
	if err := h.store.Batch(r.Context(), batch.Writes, batch.Deletes); err != nil {
		h.logger.Warn("Failed to execute batch write on shard", zap.Error(err))
		http.Error(
			w,
			fmt.Sprintf("Batch write operation failed: %v", err),
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleConfChange handles configuration change operations (POST, DELETE)
func (h *MetadataStoreAPI) handleConfChange(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("peer")
	defer func() { _ = r.Body.Close() }()

	// Extract node ID from URL path
	peerID, err := strconv.ParseUint(key, 0, 64)
	if err != nil {
		h.logger.Warn("Failed to convert ID for conf change on shard",
			zap.String("peerID", key),
			zap.Error(err))
		http.Error(w, "Failed to parse node ID", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		urlBytes, err := io.ReadAll(r.Body)
		if err != nil {
			h.logger.Warn("Failed to read on POST for conf change (add node)",
				zap.Uint64("peerID", peerID),
				zap.Error(err))
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  peerID,
			Context: urlBytes,
		}
		h.logger.Info("Received proposal to add node to shard",
			zap.Uint64("peerID", peerID),
			zap.ByteString("context", urlBytes))
		if err := h.store.ProposeConfChange(cc); err != nil {
			h.logger.Error("Failed to propose conf change for adding node",
				zap.Uint64("peerID", peerID),
				zap.Error(err))
			http.Error(w, "Failed to propose conf change", http.StatusInternalServerError)
			return
		}
		// Optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)

	case http.MethodDelete:
		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: peerID,
		}
		h.logger.Info("Received proposal to remove node from shard",
			zap.Uint64("peerID", peerID),
		)
		err := h.store.ProposeConfChange(cc)
		if err != nil {
			h.logger.Error("Failed to propose conf change for removing node",
				zap.Uint64("peerID", peerID),
				zap.Error(err))
			http.Error(w, "Failed to propose conf change", http.StatusInternalServerError)
			return
		}
		// Optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)

	default:
		w.Header().Set("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleStatus returns the metadata raft cluster status
func (h *MetadataStoreAPI) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	info := h.store.Info()
	if err := json.NewEncoder(w).Encode(info); err != nil {
		h.logger.Error("Failed to encode status response", zap.Error(err))
		http.Error(w, "Failed to encode status", http.StatusInternalServerError)
		return
	}
}

// AddRoutes adds the internal API routes to the provided mux
func (h *MetadataStoreAPI) AddRoutes(mux *http.ServeMux) {
	// These are operations that are probably handled by k8s
	mux.HandleFunc("DELETE /peer/{peer}", h.handleConfChange)
	mux.HandleFunc("POST /peer/{peer}", h.handleConfChange)

	mux.HandleFunc("POST /batch", h.handleBatch)
	mux.HandleFunc("GET /status", h.handleStatus)
}

func (m *MetadataStore) ServeAPI(
	ctx context.Context,
	mux *http.ServeMux,
	urlStr string,
	tlsInfo *common.TLSInfo,
) error {
	url, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("parsing URL: %w", err)
	}
	api := &MetadataStoreAPI{
		logger: m.logger,
		store:  m,
	}
	api.AddRoutes(mux)
	eg, egCtx := errgroup.WithContext(ctx)
	if url.Scheme == "https" {
		tlsInfo := transport.TLSInfoSimple{
			CertFile: tlsInfo.Cert,
			KeyFile:  tlsInfo.Key,
		}
		server := &http3.Server{
			QUICConfig:  &quic.Config{},
			Addr:        url.Host,
			Handler:     mux,
			IdleTimeout: time.Minute,
			TLSConfig:   tlsInfo.ServerConfig(),
		}
		eg.Go(func() error {
			err := server.ListenAndServe()
			if err != nil {
				if err == http.ErrServerClosed {
					m.logger.Info("HTTP server closed gracefully")
					return nil
				}
				m.logger.Error("HTTP/3 server failed",
					zap.String("address", server.Addr),
					zap.Error(err))

			}
			return err
		})
		eg.Go(func() error {
			<-egCtx.Done()
			m.logger.Info("HTTP server stopped")
			return server.Close()
		})
	} else {
		server := &http.Server{
			Addr:        url.Host,
			Handler:     mux,
			ReadTimeout: time.Minute,
		}
		eg.Go(func() error {
			err := server.ListenAndServe()
			if err != nil {
				if err == http.ErrServerClosed {
					m.logger.Info("HTTP server closed gracefully")
					return nil
				}
				m.logger.Error("HTTP server failed",
					zap.String("address", server.Addr),
					zap.Error(err))

			}
			return err
		})
		eg.Go(func() error {
			<-egCtx.Done()
			m.logger.Info("HTTP server stopped")
			return server.Close()
		})
	}

	eg.Go(func() error {
		select {
		case <-egCtx.Done():
			return nil
		case err := <-m.ErrorC():
			return fmt.Errorf("error occurred: %w", err)
		}
	})
	if err := eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("error in metadata store API: %w", err)
	}
	return nil
}
