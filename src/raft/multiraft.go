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

package raft

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp"
	"github.com/antflydb/antfly/lib/types"

	// stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/qlog"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error
	ReportUnreachable(id uint64)
	IsIDRemoved(id uint64) bool
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type MultiRaft struct {
	logger *zap.Logger

	storeID types.ID
	url     string

	sg               singleflight.Group
	activeShards     *xsync.Map[types.ID, Raft]
	transport        *multirafthttp.Transport
	transportHandler http.Handler
	TLSInfo          transport.TLSInfo // TLS information used when creating connection

	stopc           chan struct{}
	serverdonec     chan struct{}
	serverdoneError error
}

// //////
// Transport related needs for a single raft
// //////
var _ Transport = (*MultiRaft)(nil)

func (m *MultiRaft) ErrorC() <-chan error {
	return m.transport.ErrorC
}

func (m *MultiRaft) Send(shardID types.ID, msgs []raftpb.Message) {
	m.transport.Send(shardID, msgs)
}

func (m *MultiRaft) AddPeer(shardID, nodeID types.ID, us []string) {
	m.transport.AddPeer(shardID, nodeID, us)
}

func (m *MultiRaft) RemovePeer(shardID, nodeID types.ID) {
	m.transport.RemovePeer(shardID, nodeID)
}

func (m *MultiRaft) GetSnapshot(
	ctx context.Context,
	shardID types.ID,
	snapStore snapstore.SnapStore,
	id string,
) error {
	return m.transport.GetSnapshot(ctx, shardID, snapStore, id)
}

func (m *MultiRaft) ServeRaft(shardID types.ID, shard Raft, peers []raft.Peer) error {
	_, err, _ := m.sg.Do(shardID.String()+":start", func() (any, error) {
		if err := m.transport.Start(shardID); err != nil {
			return nil, fmt.Errorf("failed to start transport: %w", err)
		}
		for _, peer := range peers {
			if id := types.ID(peer.ID); id != m.storeID {
				ccc := &ConfChangeContext{}
				err := proto.Unmarshal(peer.Context, ccc)
				if err != nil {
					return nil, fmt.Errorf("failed to decode conf change context: %w", err)
				}
				m.transport.AddPeer(shardID, id, []string{ccc.GetUrl()})
			}
		}
		m.activeShards.Store(shardID, shard)
		return nil, nil
	})
	return err
}

func (m *MultiRaft) StopServeRaft(shardID types.ID) {
	_, _, _ = m.sg.Do(shardID.String()+":stop", func() (any, error) {
		m.transport.Stop(shardID)
		m.activeShards.Delete(shardID)
		return nil, nil
	})
}

/////////
/////////

func NewMultiRaftServer(logger *zap.Logger, dataDir string, nodeID types.ID, serverUrl string) (*MultiRaft, error) {
	url, err := url.Parse(serverUrl)
	if err != nil {
		return nil, fmt.Errorf("parsing server URL: %w", err)
	}
	tlsInfo := transport.TLSInfo{}
	if url.Scheme == "https" {
		tlsInfo = transport.TLSInfo{
			CertFile:           "certificate.crt",
			KeyFile:            "private.key",
			ClientCertFile:     "certificate.crt",
			ClientKeyFile:      "private.key",
			ClientCertAuth:     true,
			InsecureSkipVerify: true,
		}
		if _, err := tlsInfo.ClientConfig(); err != nil {
			return nil, fmt.Errorf("loading TLS client config: %w", err)
		}
		if _, err := tlsInfo.ServerConfig(); err != nil {
			return nil, fmt.Errorf("loading TLS server config: %w", err)
		}
	}
	mr := &MultiRaft{
		storeID:      nodeID,
		TLSInfo:      tlsInfo,
		logger:       logger.Named("multitraft"),
		url:          serverUrl,
		activeShards: xsync.NewMap[types.ID, Raft](),
		stopc:        make(chan struct{}),
		serverdonec:  make(chan struct{}),
	}
	transport := &multirafthttp.Transport{
		ID:          nodeID,
		DataDir:     dataDir,
		TLSInfo:     tlsInfo,
		Logger:      logger.Named("transport"),
		MultiRaft:   mr,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(uint64(nodeID), 10)),
		SnapStoreFactory: func(dataDir string, shardID, nodeID types.ID) (multirafthttp.SnapStore, error) {
			return snapstore.NewLocalSnapStore(dataDir, shardID, nodeID)
		},
		ErrorC: make(chan error),
	}
	mr.transport = transport
	return mr, nil
}

// These are the core multraft interfaces for the transport layer
//
// Process plexes raft messages to the appropriate shard
func (rs *MultiRaft) Process(ctx context.Context, shardID uint64, m raftpb.Message) error {
	if r, ok := rs.activeShards.Load(types.ID(shardID)); ok {
		return r.Process(ctx, m)
	}
	return multirafthttp.ErrShardNotFound
}

func (rs *MultiRaft) ReportUnreachable(shardID, id uint64) {
	if r, ok := rs.activeShards.Load(types.ID(shardID)); ok {
		r.ReportUnreachable(id)
		return
	}
}

func (rs *MultiRaft) IsIDRemoved(shardID, id uint64) bool {
	if r, ok := rs.activeShards.Load(types.ID(shardID)); ok {
		return r.IsIDRemoved(id)
	}
	// TODO (ajr) Default to removed if shard is missing?
	return true
}

func (rs *MultiRaft) ReportSnapshot(shardID, id uint64, status raft.SnapshotStatus) {
	if r, ok := rs.activeShards.Load(types.ID(shardID)); ok {
		r.ReportSnapshot(id, raft.SnapshotStatus(status))
		return
	}
}

//////

func (rs *MultiRaft) Start() {
	rs.transportHandler = rs.transport.Handler()
	url, err := url.Parse(rs.url)
	if err != nil {
		rs.logger.Fatal("Failed parsing URL", zap.Error(err))
	}
	stopc := rs.stopc
	rs.logger.Debug(
		"Starting MultiRaft server",
		zap.Any("url", url),
		zap.String("storeID", rs.storeID.String()),
	)
	defer rs.logger.Info("Raft Server done", zap.Error(err))
	if url.Scheme == "https" {
		rs.logger.Info("Starting Raft Quic server",
			zap.String("host", url.Host),
		)
		// Assume that the TLS Server config is valid
		tlsConfig, _ := rs.TLSInfo.ServerConfig()
		server := &http3.Server{
			Addr:    url.Host,
			Handler: rs.transportHandler,
			QUICConfig: &quic.Config{
				KeepAlivePeriod: 5 * time.Second,
				MaxIdleTimeout:  time.Minute,
				Tracer:          qlog.DefaultConnectionTracer,
			},
			IdleTimeout: time.Minute,
			TLSConfig:   tlsConfig,
		}
		go func() {
			<-stopc
			if err := server.Shutdown(context.Background()); err != nil {
				rs.logger.Error("Failed to shutdown server", zap.Error(err))
			}
		}()

		err := server.ListenAndServe()

		select {
		case <-stopc:
		default:
			rs.serverdoneError = err
		}
		close(rs.serverdonec)
		rs.logger.Info("Raft Quic Server done", zap.Error(err))
		return
	}
	rs.logger.Info("Starting Raft HTTP server")
	ln, err := newStoppableListener(url.Host, stopc)
	if err != nil {
		rs.logger.Fatal("Failed to listen", zap.Error(err))
	}
	defer func() { _ = ln.Close() }()

	server := &http.Server{
		Handler:           rs.transportHandler,
		ReadHeaderTimeout: 30 * time.Second,
	}
	// defer server.Close()
	err = server.Serve(ln)
	select {
	case <-stopc:
	default:
		rs.serverdoneError = err
	}
	close(rs.serverdonec)
}

func (rs *MultiRaft) Stop() error {
	close(rs.stopc)
	rs.transport.Close()
	<-rs.serverdonec
	return rs.serverdoneError
}
