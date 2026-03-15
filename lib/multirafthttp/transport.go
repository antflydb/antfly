// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multirafthttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/lib/types"

	"github.com/sethvargo/go-retry"
	"github.com/xiang90/probing"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type MultiRaft interface {
	Process(ctx context.Context, shardID uint64, m raftpb.Message) error
	ReportUnreachable(shardID, id uint64)
	ReportSnapshot(shardID, id uint64, status raft.SnapshotStatus)
}

type Transporter interface {
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	Start(shardID types.ID) error
	// Handler returns the HTTP handler of the transporter.
	// A transporter HTTP handler handles the HTTP requests
	// from remote peers.
	// The handler MUST be used to handle RaftPrefix(/raft)
	// endpoint.
	Handler() http.Handler
	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	Send(shardID types.ID, m []raftpb.Message)
	// AddRemote adds a remote with given peer urls into the transport.
	// A remote helps newly joined member to catch up the progress of cluster,
	// and will not be used after that.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	AddRemote(shardID types.ID, id types.ID, urls []string)
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	AddPeer(shardID types.ID, id types.ID, urls []string)
	GetSnapshot(ctx context.Context, shardID types.ID, snapStore SnapStore, id string) error
	// RemovePeer removes the peer with given id.
	RemovePeer(shardID types.ID, id types.ID)
	// RemoveAllPeers removes all the existing peers in the transport.
	RemoveAllPeers(shardID types.ID)
	// ActiveSince returns the time that the connection with the peer
	// of the given id becomes active.
	// If the connection is active since peer was added, it returns the adding time.
	// If the connection is currently inactive, it returns zero time.
	ActiveSince(id types.ID) time.Time
	// ActivePeers returns the number of active peers.
	ActivePeers() int
	// Stop closes the connections and stops the transporter.
	Stop(shardID types.ID)
}

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
// User should call Handler method to get a handler to serve requests
// received from peerURLs.
// User needs to call Start before calling other functions, and call
// Stop when the Transport is no longer used.
type Transport struct {
	Logger *zap.Logger

	init sync.Once

	DialTimeout time.Duration // maximum duration before timing out dial of the request
	// DialRetryFrequency defines the frequency of streamReader dial retrial attempts;
	// a distinct rate limiter is created per every peer (default value: 10 events/sec)
	DialRetryFrequency rate.Limit

	TLSInfo transport.TLSInfo // TLS information used when creating connection

	DataDir string // data directory for antfly
	// Factory function to create a SnapStore for a given shard
	SnapStoreFactory func(dataDir string, shardID, nodeID types.ID) (SnapStore, error)

	ID          types.ID   // local member ID
	URLs        types.URLs // local peer URLs
	MultiRaft   MultiRaft
	ServerStats *stats.ServerStats // used to record general transportation statistics
	// LeaderStats records transportation statistics with followers when
	// performing as leader in raft protocol
	LeaderStats *stats.LeaderStats
	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	StreamRt   http.RoundTripper // roundTripper used by streams
	PipelineRt http.RoundTripper // roundTripper used by pipelines
	rtClosers  []io.Closer       // closers for HTTP/3 transports registered on round trippers

	mu         sync.RWMutex                       // protect the remote and peer map
	remotes    map[types.ID]map[types.ID]*remote  // remotes map that helps newly joined member to catch up
	peers      map[types.ID]Peer                  // peers map
	shardPeers map[types.ID]map[types.ID]struct{} // shard peers map
	peerAdds   map[types.ID]int

	PipelineProber probing.Prober
	StreamProber   probing.Prober
}

var ErrShardNotFound = errors.New("shard not found")

func (t *Transport) Init() error {
	t.remotes = make(map[types.ID]map[types.ID]*remote)
	t.peers = make(map[types.ID]Peer)
	t.shardPeers = make(map[types.ID]map[types.ID]struct{})
	t.peerAdds = make(map[types.ID]int)

	var err error
	var h3Closer io.Closer
	t.StreamRt, h3Closer, err = NewStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	if h3Closer != nil {
		t.rtClosers = append(t.rtClosers, h3Closer)
	}
	t.PipelineRt, h3Closer, err = NewRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	if h3Closer != nil {
		t.rtClosers = append(t.rtClosers, h3Closer)
	}
	t.PipelineProber = probing.NewProber(t.PipelineRt)
	t.StreamProber = probing.NewProber(t.StreamRt)

	// If client didn't provide dial retry frequency, use the default
	// (100ms backoff between attempts to create a new stream),
	// so it doesn't bring too much overhead when retry.
	if t.DialRetryFrequency == 0 {
		t.DialRetryFrequency = rate.Every(100 * time.Millisecond)
	}
	return nil
}

var ErrTransportClosed = errors.New("transport has been closed")

func (t *Transport) Start(shardID types.ID) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var err error
	t.init.Do(func() {
		err = t.Init()
	})
	if err != nil {
		return err
	}

	// Check if transport was closed after init
	if t.remotes == nil {
		return ErrTransportClosed
	}

	t.remotes[shardID] = make(map[types.ID]*remote)
	t.shardPeers[shardID] = make(map[types.ID]struct{})
	return nil
}

func (t *Transport) Handler() http.Handler {
	pipelineHandler := newPipelineHandler(t, t.MultiRaft)
	streamHandler := newStreamHandler(t, t, t.MultiRaft, t.ID)

	// SnapStoreFactory must be set - panic if not (programmer error)
	if t.SnapStoreFactory == nil {
		panic("Transport.SnapStoreFactory must be set")
	}

	snapHandler := &snapHandler{
		id: t.ID,
		lg: t.Logger,
		snapStoreFunc: func(shardID types.ID) (SnapStore, error) {
			return t.SnapStoreFactory(t.DataDir, shardID, t.ID)
		},
	}

	mux := http.NewServeMux()
	mux.Handle(RaftPrefix, pipelineHandler)
	mux.Handle(RaftStreamPrefix+"/", streamHandler)
	mux.Handle(ProbingPrefix, probing.NewHandler())
	mux.Handle(SnapPrefix+"/", snapHandler)
	return mux
}

func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

var ErrNoPeersAvailable = errors.New("no peers available")

// ErrSnapshotNotFound indicates a snapshot was not found on a peer (HTTP 404).
// When all peers return this error, the snapshot has been garbage collected
// everywhere and retrying won't help.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// GetSnapshot fetches a KV snapshot from another node.
func (t *Transport) GetSnapshot(ctx context.Context, shardID types.ID, snapStore SnapStore, id string) error {
	b := retry.NewExponential(1 * time.Second)
	b = retry.WithCappedDuration(30*time.Second, b)
	b = retry.WithJitterPercent(20, b)

	// Track the last meaningful error so we can surface it if the context expires.
	// retry.Do returns bare ctx.Err() on cancellation, losing the reason we were retrying.
	var lastRetryErr error

	err := retry.Do(ctx, b, func(ctx context.Context) error {
		t.Logger.Info("Checking for local storage snapshot", zap.String("snapshotID", id))

		// Check if the snapshot file already exists locally
		exists, err := snapStore.Exists(ctx, id)
		if err != nil {
			t.Logger.Error("Error checking for local snapshot file", zap.String("snapshotID", id), zap.Error(err))
			return fmt.Errorf("failed to check for local snapshot %s: %w", id, err)
		}
		if exists {
			t.Logger.Info("Storage snapshot already exists locally", zap.String("snapshotID", id))
			return nil
		}

		t.Logger.Info("Local storage snapshot not found, attempting to fetch from peers", zap.String("snapshotID", id))

		// Snapshot peers under the read lock to avoid data races
		t.mu.RLock()
		shardPeerSet, hasPeers := t.shardPeers[shardID]
		var peerList []Peer
		if hasPeers {
			for peerID := range shardPeerSet {
				if p, ok := t.peers[peerID]; ok {
					peerList = append(peerList, p)
				}
			}
		}
		t.mu.RUnlock()

		if !hasPeers || len(peerList) == 0 {
			t.Logger.Error("No peers available to fetch snapshot",
				zap.Stringer("shardID", shardID),
				zap.String("snapshotID", id))
			lastRetryErr = ErrNoPeersAvailable
			return retry.RetryableError(ErrNoPeersAvailable)
		}

		var lastErr error
		for _, p := range peerList {
			if err := p.sendSnapshotRequest(shardID, snapStore, id); err == nil {
				lastErr = nil
				break
			} else {
				lastErr = err
			}
		}
		// If we get here without breaking, all peers failed
		if lastErr != nil {
			// Always retry — during shard splits, peers may still be creating
			// the archive when we first ask. The caller's context controls timeout.
			lastRetryErr = lastErr
			return retry.RetryableError(fmt.Errorf("sending snapshot request to all peers: %w", lastErr))
		}
		t.Logger.Info("Successfully requested snapshot", zap.String("snapshotID", id))
		return nil
	})

	// If the context expired while retrying, wrap both errors so callers
	// can see why we were retrying (e.g. ErrSnapshotNotFound) alongside
	// the context error.
	if err != nil && ctx.Err() != nil && lastRetryErr != nil {
		return fmt.Errorf("%w: %w", err, lastRetryErr)
	}
	return err
}

func (t *Transport) Send(shardID types.ID, msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		to := types.ID(m.To)

		t.mu.RLock()
		_, shardExists := t.shardPeers[shardID]
		p, pok := t.peers[to]
		g, rok := t.remotes[shardID][to]
		t.mu.RUnlock()

		if !shardExists {
			if t.Logger != nil && shardNotFoundLogRateLimiter.Allow() {
				t.Logger.Debug(
					"ignored message send request; shard not found",
					zap.Stringer("type", m.Type),
					zap.Stringer("unknown-target-shard-id", shardID),
					zap.Stringer("unknown-target-peer-id", to),
				)
			}
			return
		}

		if pok {
			if isMsgApp(m) {
				t.ServerStats.SendAppendReq(m.Size())
			}
			p.send(multiMessage{msg: m, shardID: shardID})
			continue
		}

		if rok {
			g.send(multiMessage{msg: m, shardID: shardID})
			continue
		}

		if t.Logger != nil {
			t.Logger.Debug(
				"ignored message send request; unknown remote peer target",
				zap.Stringer("type", m.Type),
				zap.Stringer("unknown-target-peer-shard-id", shardID),
				zap.Stringer("unknown-target-peer-id", to),
			)
		}
	}
}

func (t *Transport) Stop(shardID types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, g := range t.remotes[shardID] {
		g.stop()
	}
	delete(t.remotes, shardID)
	for peer := range t.shardPeers[shardID] {
		t.peerAdds[peer]--
		if t.peerAdds[peer] == 0 {
			delete(t.peerAdds, peer)
			t.peers[peer].stop()
			delete(t.peers, peer)
		}
	}
	delete(t.shardPeers, shardID)
}

// CutPeer drops messages to the specified peer.
func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	// Collect remotes for this peer across all shards
	var remotes []*remote
	for _, shardRemotes := range t.remotes {
		if r, ok := shardRemotes[id]; ok {
			remotes = append(remotes, r)
		}
	}
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	for _, r := range remotes {
		r.Pause()
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	// Collect remotes for this peer across all shards
	var remotes []*remote
	for _, shardRemotes := range t.remotes {
		if r, ok := shardRemotes[id]; ok {
			remotes = append(remotes, r)
		}
	}
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	for _, r := range remotes {
		r.Resume()
	}
}

func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.PipelineProber != nil {
		t.PipelineProber.RemoveAll()
	}
	if t.StreamProber != nil {
		t.StreamProber.RemoveAll()
	}
	if tr, ok := t.StreamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.PipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	for _, c := range t.rtClosers {
		_ = c.Close()
	}
	t.rtClosers = nil
	for peer := range t.peers {
		t.peers[peer].stop()
	}
	t.peers = nil
	t.remotes = nil
	t.shardPeers = nil
	t.peerAdds = nil
}

func (t *Transport) AddRemote(shardID, id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[shardID][id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	t.remotes[shardID][id] = startRemote(t, urls, id)

	if t.Logger != nil {
		t.Logger.Info(
			"added new remote peer",
			zap.Stringer("local-member-id", t.ID),
			zap.Stringer("remote-peer-id", id),
			zap.Stringer("local-shard-id", shardID),
			zap.Strings("remote-peer-urls", us),
		)
	}
}

func (t *Transport) AddPeer(shardID, id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.peers == nil {
		// Transport has been closed, ignore the request.
		// This can happen during shutdown when raft is still processing
		// conf changes that were committed before the transport was closed.
		return
	}
	if _, ok := t.shardPeers[shardID][id]; ok {
		return
	}
	if t.shardPeers[shardID] == nil {
		t.shardPeers[shardID] = make(map[types.ID]struct{})
	}
	t.shardPeers[shardID][id] = struct{}{}
	t.peerAdds[id]++
	if _, ok := t.peers[id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	fs := t.LeaderStats.Follower(id.String())
	t.peers[id] = startPeer(t, urls, id, fs)
	addPeerToProber(t.Logger, t.PipelineProber, id.String(), us, RoundTripperNameSnapshot, rttSec)
	addPeerToProber(t.Logger, t.StreamProber, id.String(), us, RoundTripperNameRaftMessage, rttSec)

	if t.Logger != nil {
		t.Logger.Info(
			"added remote peer",
			zap.Stringer("local-member-id", t.ID),
			zap.Stringer("remote-peer-id", id),
			zap.Strings("remote-peer-urls", us),
		)
	}
}

func (t *Transport) RemovePeer(shardID, id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(shardID, id)
}

func (t *Transport) RemoveAllPeers(shardID types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.shardPeers[shardID] {
		t.removePeer(shardID, id)
	}
}

// the caller of this function must have the peers mutex.
func (t *Transport) removePeer(shardID, id types.ID) {
	if t.shardPeers == nil {
		return
	}
	shardPeers, ok := t.shardPeers[shardID]
	if !ok {
		return
	}
	if _, ok := shardPeers[id]; !ok {
		return
	}
	delete(t.shardPeers[shardID], id)
	t.peerAdds[id]--
	if t.peerAdds[id] != 0 {
		return
	}
	delete(t.peerAdds, id)
	// etcd may remove a member again on startup due to WAL files replaying.
	peer, ok := t.peers[id]
	if ok {
		peer.stop()
		delete(t.peers, id)
		delete(t.LeaderStats.Followers, id.String())
		// Ignore not found errors
		_ = t.PipelineProber.Remove(id.String())
		_ = t.StreamProber.Remove(id.String())
	}

	if t.Logger != nil {
		if ok {
			t.Logger.Info(
				"removed remote peer",
				zap.Stringer("local-member-id", t.ID),
				zap.Stringer("removed-remote-peer-id", id),
			)
		} else {
			t.Logger.Warn(
				"skipped removing already removed peer",
				zap.Stringer("local-member-id", t.ID),
				zap.Stringer("removed-remote-peer-id", id),
			)
		}
	}
}

func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

// Pausable is a testing interface for pausing transport traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

// ActivePeers returns a channel that closes when an initial
// peer connection has been established. Use this to wait until the
// first peer connection becomes active.
func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}
