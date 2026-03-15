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
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/types"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestSendMessage(t *testing.T) {
	shardID := types.ID(1)
	nodeID1 := types.ID(1)
	zl1 := zaptest.NewLogger(t).With(zap.Stringer("nodeID", nodeID1))
	nodeID2 := types.ID(2)
	zl2 := zaptest.NewLogger(t).With(zap.Stringer("nodeID", nodeID2))
	// member 1
	tr := &Transport{
		Logger:           zl1,
		ID:               nodeID1,
		MultiRaft:        &fakeMultiRaft{},
		ServerStats:      stats.NewServerStats("", ""),
		LeaderStats:      stats.NewLeaderStats(nodeID1.String()),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	tr.Start(shardID)
	defer tr.Close()
	srv := httptest.NewServer(tr.Handler())
	defer srv.Close()

	// member 2
	recvc := make(chan raftpb.Message, 1)
	p := &fakeMultiRaft{shards: map[types.ID]baseRaft{shardID: &fakeRaft{recvc: recvc}}}
	tr2 := &Transport{
		Logger:           zl2,
		ID:               nodeID2,
		MultiRaft:        p,
		ServerStats:      stats.NewServerStats("", ""),
		LeaderStats:      stats.NewLeaderStats(nodeID2.String()),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	tr2.Start(shardID)
	defer tr2.Close()
	srv2 := httptest.NewServer(tr2.Handler())
	defer srv2.Close()

	tr.AddPeer(shardID, nodeID2, []string{srv2.URL})
	defer tr.Stop(shardID)
	tr2.AddPeer(shardID, nodeID1, []string{srv.URL})
	defer tr2.Stop(shardID)
	if !waitStreamWorking(tr.Get(nodeID2).(*peer)) {
		t.Fatalf("stream from 1 to 2 is not in work as expected")
	}

	data := []byte("some data")
	tests := []raftpb.Message{
		// these messages are set to send to itself, which facilitates testing.
		{Type: raftpb.MsgProp, From: 1, To: 2, Entries: []raftpb.Entry{{Data: data}}},
		{Type: raftpb.MsgApp, From: 1, To: 2, Term: 1, Index: 3, LogTerm: 0, Entries: []raftpb.Entry{{Index: 4, Term: 1, Data: data}}, Commit: 3},
		{Type: raftpb.MsgAppResp, From: 1, To: 2, Term: 1, Index: 3},
		{Type: raftpb.MsgVote, From: 1, To: 2, Term: 1, Index: 3, LogTerm: 0},
		{Type: raftpb.MsgVoteResp, From: 1, To: 2, Term: 1},
		{Type: raftpb.MsgSnap, From: 1, To: 2, Term: 1, Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1000, Term: 1}, Data: data}},
		{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 1, Commit: 3},
		{Type: raftpb.MsgHeartbeatResp, From: 1, To: 2, Term: 1},
	}
	for i, tt := range tests {
		tr.Send(shardID, []raftpb.Message{tt})
		msg := <-recvc
		if !reflect.DeepEqual(msg, tt) {
			t.Errorf("#%d: msg = %+v, want %+v", i, msg, tt)
		}
	}
}

// TestSendMessageWhenStreamIsBroken tests that message can be sent to the
// remote in a limited time when all underlying connections are broken.
func TestSendMessageWhenStreamIsBroken(t *testing.T) {
	shardID := types.ID(1)
	// member 1
	tr := &Transport{
		ID:               types.ID(1),
		MultiRaft:        &fakeMultiRaft{},
		ServerStats:      stats.NewServerStats("", ""),
		LeaderStats:      stats.NewLeaderStats("1"),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	tr.Start(shardID)
	defer tr.Close()
	srv := httptest.NewServer(tr.Handler())
	defer srv.Close()

	// member 2
	recvc := make(chan raftpb.Message, 1)
	p := &fakeMultiRaft{shards: map[types.ID]baseRaft{shardID: &fakeRaft{recvc: recvc}}}
	tr2 := &Transport{
		ID:               types.ID(2),
		MultiRaft:        p,
		ServerStats:      stats.NewServerStats("", ""),
		LeaderStats:      stats.NewLeaderStats("2"),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	tr2.Start(types.ID(1))
	defer tr2.Close()
	srv2 := httptest.NewServer(tr2.Handler())
	defer srv2.Close()

	tr.AddPeer(shardID, types.ID(2), []string{srv2.URL})
	defer tr.Stop(types.ID(1))
	tr2.AddPeer(shardID, types.ID(1), []string{srv.URL})
	defer tr2.Stop(shardID)
	if !waitStreamWorking(tr.Get(types.ID(2)).(*peer)) {
		t.Fatalf("stream from 1 to 2 is not in work as expected")
	}

	// break the stream
	srv.CloseClientConnections()
	srv2.CloseClientConnections()
	var n int
	for {
		select {
		// TODO: remove this resend logic when we add retry logic into the code
		case <-time.After(time.Millisecond):
			n++
			tr.Send(shardID, []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 1, To: 2, Term: 1, Commit: 3}})
		case <-recvc:
			if n > 50 {
				t.Errorf("disconnection time = %dms, want < 50ms", n)
			}
			return
		}
	}
}

func waitStreamWorking(p *peer) bool {
	for range 1000 {
		time.Sleep(time.Millisecond)
		if _, ok := p.msgAppV2Writer.writec(); !ok {
			continue
		}
		if _, ok := p.writer.writec(); !ok {
			continue
		}
		return true
	}
	return false
}

type baseRaft interface {
	Process(ctx context.Context, m raftpb.Message) error
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type fakeMultiRaft struct {
	shards map[types.ID]baseRaft
}

func (f *fakeMultiRaft) ReportUnreachable(shardID, id uint64)                          {}
func (f *fakeMultiRaft) ReportSnapshot(shardID, id uint64, status raft.SnapshotStatus) {}
func (f *fakeMultiRaft) Process(ctx context.Context, shardID uint64, m raftpb.Message) error {
	sID := types.ID(shardID)
	var r baseRaft = &fakeRaft{}
	if f.shards != nil {
		if f.shards[sID] == nil {
			return ErrShardNotFound
		}
		r = f.shards[sID]
	}
	return r.Process(ctx, m)
}

type fakeRaft struct {
	recvc     chan<- raftpb.Message
	err       error
	removedID uint64
}

func (p *fakeRaft) Process(ctx context.Context, m raftpb.Message) error {
	select {
	case p.recvc <- m:
	default:
	}
	return p.err
}

func (p *fakeRaft) IsIDRemoved(id uint64) bool { return id == p.removedID }

func (p *fakeRaft) ReportUnreachable(id uint64) {}

func (p *fakeRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
