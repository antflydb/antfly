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
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/types"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/xiang90/probing"
	"go.etcd.io/raft/v3/raftpb"
)

// mockSnapStoreFactory creates a mock SnapStore for testing
func mockSnapStoreFactory(dataDir string, shardID, nodeID types.ID) (SnapStore, error) {
	return &mockSnapStore{}, nil
}

// TestTransportSend tests that transport can send messages using correct
// underlying peer, and drop local or unknown-target messages.
func TestTransportSend(t *testing.T) {
	peer1 := newFakePeer()
	peer2 := newFakePeer()
	shardID := types.ID(1)
	tr := &Transport{
		ServerStats: stats.NewServerStats("", ""),
	}
	tr.Start(shardID)
	defer tr.Close()
	tr.peers = map[types.ID]Peer{types.ID(1): peer1, types.ID(2): peer2}
	wmsgsIgnored := []raftpb.Message{
		// bad local message
		{Type: raftpb.MsgBeat},
		// bad remote message
		{Type: raftpb.MsgProp, To: 3},
	}
	wmsgsTo1 := []raftpb.Message{
		// good message
		{Type: raftpb.MsgProp, To: 1},
		{Type: raftpb.MsgApp, To: 1},
	}
	wmsgsTo2 := []raftpb.Message{
		// good message
		{Type: raftpb.MsgProp, To: 2},
		{Type: raftpb.MsgApp, To: 2},
	}
	tr.Send(shardID, wmsgsIgnored)
	tr.Send(shardID, wmsgsTo1)
	tr.Send(shardID, wmsgsTo2)

	for i, msg := range peer1.msgs {
		if !reflect.DeepEqual(msg.msg, wmsgsTo1[i]) {
			t.Errorf("msgs to peer 1 = %+v, want %+v", peer1.msgs, wmsgsTo1)
		}
	}
	for i, msg := range peer2.msgs {
		if !reflect.DeepEqual(msg.msg, wmsgsTo2[i]) {
			t.Errorf("msgs to peer 2 = %+v, want %+v", peer2.msgs, wmsgsTo2)
		}
	}
}

func TestTransportCutMend(t *testing.T) {
	peer1 := newFakePeer()
	peer2 := newFakePeer()
	shardID := types.ID(1)
	tr := &Transport{
		ServerStats: stats.NewServerStats("", ""),
	}
	tr.Start(shardID)
	defer tr.Close()
	tr.peers = map[types.ID]Peer{types.ID(1): peer1, types.ID(2): peer2}

	tr.CutPeer(types.ID(1))

	wmsgsTo := []raftpb.Message{
		// good message
		{Type: raftpb.MsgProp, To: 1},
		{Type: raftpb.MsgApp, To: 1},
	}

	tr.Send(shardID, wmsgsTo)
	if len(peer1.msgs) > 0 {
		t.Fatalf("msgs expected to be ignored, got %+v", peer1.msgs)
	}

	tr.MendPeer(types.ID(1))

	tr.Send(shardID, wmsgsTo)
	for i, msg := range peer1.msgs {
		if !reflect.DeepEqual(msg.msg, wmsgsTo[i]) {
			t.Errorf("msgs to peer 1 = %+v, want %+v", peer1.msgs, wmsgsTo)
		}
	}
}

func TestTransportAdd(t *testing.T) {
	ls := stats.NewLeaderStats("")
	tr := &Transport{
		LeaderStats:      ls,
		StreamRt:         &roundTripperRecorder{},
		peers:            make(map[types.ID]Peer),
		PipelineProber:   probing.NewProber(nil),
		StreamProber:     probing.NewProber(nil),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	shardID := types.ID(1)
	tr.Start(shardID)
	defer tr.Close()
	tr.AddPeer(shardID, 1, []string{"http://localhost:2380"})

	if _, ok := ls.Followers["1"]; !ok {
		t.Errorf("FollowerStats[1] is nil, want exists")
	}
	s, ok := tr.peers[types.ID(1)]
	if !ok {
		tr.Stop(shardID)
		t.Fatalf("senders[1] is nil, want exists")
	}

	// duplicate AddPeer is ignored
	tr.AddPeer(shardID, 1, []string{"http://localhost:2380"})
	ns := tr.peers[types.ID(1)]
	if s != ns {
		t.Errorf("sender = %v, want %v", ns, s)
	}

	tr.Stop(shardID)
}

func TestTransportRemove(t *testing.T) {
	defer goleak.VerifyNone(t,
		// Ants starts a goroutine even on import
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).purgeStaleWorkers"),
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).ticktock"),
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).goTicktock"),
		// Bleve starts analysis workers on import
		goleak.IgnoreTopFunction("github.com/blevesearch/bleve_index_api.AnalysisWorker"),
	)
	tr := &Transport{
		LeaderStats:      stats.NewLeaderStats(""),
		StreamRt:         &roundTripperRecorder{},
		peers:            make(map[types.ID]Peer),
		PipelineProber:   probing.NewProber(nil),
		StreamProber:     probing.NewProber(nil),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	shardID := types.ID(1)
	tr.Start(shardID)
	defer tr.Close()
	tr.AddPeer(shardID, 1, []string{"http://localhost:2380"})
	tr.RemovePeer(shardID, types.ID(1))
	defer tr.Stop(shardID)

	if _, ok := tr.peers[types.ID(1)]; ok {
		t.Fatalf("senders[1] exists, want removed")
	}
}

func TestTransportRemoveIsIdempotent(t *testing.T) {
	tr := &Transport{
		LeaderStats:      stats.NewLeaderStats(""),
		StreamRt:         &roundTripperRecorder{},
		peers:            make(map[types.ID]Peer),
		PipelineProber:   probing.NewProber(nil),
		StreamProber:     probing.NewProber(nil),
		SnapStoreFactory: mockSnapStoreFactory,
	}

	shardID := types.ID(1)
	tr.Start(shardID)
	defer tr.Close()
	tr.AddPeer(shardID, 1, []string{"http://localhost:2380"})
	tr.RemovePeer(shardID, 1)
	tr.RemovePeer(shardID, 1)
	defer tr.Stop(shardID)

	if _, ok := tr.peers[types.ID(1)]; ok {
		t.Fatalf("senders[1] exists, want removed")
	}
}

// snapErrPeer is a fake peer that returns a configurable error from sendSnapshotRequest.
type snapErrPeer struct {
	fakePeer
	snapErr error
}

func newSnapErrPeer(err error) *snapErrPeer {
	fp := newFakePeer()
	return &snapErrPeer{fakePeer: *fp, snapErr: err}
}

func (p *snapErrPeer) sendSnapshotRequest(_ types.ID, _ SnapStore, _ string) error {
	return p.snapErr
}

// notFoundSnapStore always says snapshot doesn't exist locally.
type notFoundSnapStore struct{ mockSnapStore }

func (s *notFoundSnapStore) Exists(_ context.Context, _ string) (bool, error) { return false, nil }

func TestGetSnapshotAllPeersNotFound(t *testing.T) {
	shardID := types.ID(1)
	tr := &Transport{
		Logger:      zap.NewNop(),
		ServerStats: stats.NewServerStats("", ""),
	}
	tr.Start(shardID)
	defer tr.Close()

	// Two peers, both return ErrSnapshotNotFound.
	// All errors are retryable (peers may still be creating the archive during
	// splits), so GetSnapshot retries until the context expires.
	peer1 := newSnapErrPeer(fmt.Errorf("snap abc on peer 1: %w", ErrSnapshotNotFound))
	peer2 := newSnapErrPeer(fmt.Errorf("snap abc on peer 2: %w", ErrSnapshotNotFound))
	tr.peers = map[types.ID]Peer{types.ID(2): peer1, types.ID(3): peer2}
	tr.shardPeers = map[types.ID]map[types.ID]struct{}{
		shardID: {types.ID(2): {}, types.ID(3): {}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := tr.GetSnapshot(ctx, shardID, &notFoundSnapStore{}, "abc")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Should retry until context deadline, not fail immediately.
	// The error wraps both the context error and the last retry error.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound in wrapped error, got: %v", err)
	}
}

// delayedSnapPeer returns ErrSnapshotNotFound for the first N calls,
// then succeeds. This simulates the shard-split race where applyOpSplit
// hasn't finished creating the archive yet when the new shard first asks.
type delayedSnapPeer struct {
	fakePeer
	mu        sync.Mutex
	remaining int
}

func newDelayedSnapPeer(failCount int) *delayedSnapPeer {
	return &delayedSnapPeer{fakePeer: *newFakePeer(), remaining: failCount}
}

func (p *delayedSnapPeer) sendSnapshotRequest(_ types.ID, _ SnapStore, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.remaining > 0 {
		p.remaining--
		return fmt.Errorf("not ready yet: %w", ErrSnapshotNotFound)
	}
	return nil
}

func TestGetSnapshotRetriesUntilAvailable(t *testing.T) {
	shardID := types.ID(1)
	tr := &Transport{
		Logger:      zap.NewNop(),
		ServerStats: stats.NewServerStats("", ""),
	}
	tr.Start(shardID)
	defer tr.Close()

	// Peer returns not-found 3 times, then succeeds on the 4th attempt.
	peer := newDelayedSnapPeer(3)
	tr.peers = map[types.ID]Peer{types.ID(2): peer}
	tr.shardPeers = map[types.ID]map[types.ID]struct{}{
		shardID: {types.ID(2): {}},
	}

	err := tr.GetSnapshot(context.Background(), shardID, &notFoundSnapStore{}, "split-abc")
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}

	peer.mu.Lock()
	remaining := peer.remaining
	peer.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected all fail attempts consumed, got %d remaining", remaining)
	}
}

func TestTransportErrc(t *testing.T) {
	defer goleak.VerifyNone(t,
		// Ants starts a goroutine even on import
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).purgeStaleWorkers"),
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).ticktock"),
		goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).goTicktock"),
		// Bleve starts analysis workers on import
		goleak.IgnoreTopFunction("github.com/blevesearch/bleve_index_api.AnalysisWorker"),
	)
	errorc := make(chan error, 1)
	tr := &Transport{
		MultiRaft:        &fakeMultiRaft{},
		LeaderStats:      stats.NewLeaderStats(""),
		ErrorC:           errorc,
		StreamRt:         newRespRoundTripper(http.StatusForbidden, nil),
		PipelineRt:       newRespRoundTripper(http.StatusForbidden, nil),
		peers:            make(map[types.ID]Peer),
		PipelineProber:   probing.NewProber(nil),
		StreamProber:     probing.NewProber(nil),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	shardID := types.ID(1)
	tr.remotes = make(map[types.ID]map[types.ID]*remote)
	tr.peers = make(map[types.ID]Peer)
	tr.shardPeers = make(map[types.ID]map[types.ID]struct{})
	tr.peerAdds = make(map[types.ID]int)
	tr.remotes[shardID] = make(map[types.ID]*remote)
	tr.shardPeers[shardID] = make(map[types.ID]struct{})
	tr.AddPeer(shardID, 1, []string{"http://localhost:2380"})
	defer tr.Close()
	defer tr.Stop(shardID)

	select {
	case <-errorc:
		t.Fatalf("received unexpected from errorc")
	case <-time.After(10 * time.Millisecond):
	}
	tr.peers[1].send(multiMessage{})

	select {
	case <-errorc:
	case <-time.After(1 * time.Second):
		t.Fatalf("cannot receive error from errorc")
	}
}
