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
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/types"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func BenchmarkSendingMsgApp(b *testing.B) {
	// member 1
	tr := &Transport{
		ID:               types.ID(1),
		MultiRaft:        &fakeMultiRaft{},
		ServerStats:      stats.NewServerStats("", ""),
		LeaderStats:      stats.NewLeaderStats("1"),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	tr.Init()
	shardID := types.ID(1)
	tr.Start(shardID)
	defer tr.Close()
	srv := httptest.NewServer(tr.Handler())
	defer srv.Close()

	// member 2
	r := &countRaft{}
	mr := &fakeMultiRaft{
		shards: map[types.ID]baseRaft{
			shardID: r,
		},
	}
	tr2 := &Transport{
		ID:               types.ID(2),
		MultiRaft:        mr,
		ServerStats:      stats.NewServerStats("", ""),
		LeaderStats:      stats.NewLeaderStats("2"),
		SnapStoreFactory: mockSnapStoreFactory,
	}
	tr2.Start(shardID)
	defer tr2.Close()
	srv2 := httptest.NewServer(tr2.Handler())
	defer srv2.Close()

	tr.AddPeer(shardID, types.ID(2), []string{srv2.URL})
	defer tr.Stop(shardID)
	tr2.AddPeer(shardID, types.ID(1), []string{srv.URL})
	defer tr2.Stop(shardID)
	if !waitStreamWorking(tr.Get(types.ID(2)).(*peer)) {
		b.Fatalf("stream from 1 to 2 is not in work as expected")
	}

	b.ReportAllocs()
	b.SetBytes(64)

	data := make([]byte, 64)
	for i := 0; b.Loop(); i++ {
		tr.Send(shardID, []raftpb.Message{
			{
				Type:  raftpb.MsgApp,
				From:  1,
				To:    2,
				Index: uint64(i),
				Entries: []raftpb.Entry{
					{
						Index: uint64(i + 1),
						Data:  data,
					},
				},
			},
		})
	}
	// wait until all messages are received by the target raft
	for r.count() != b.N {
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()
}

type countRaft struct {
	mu  sync.Mutex
	cnt int
}

func (r *countRaft) Process(ctx context.Context, m raftpb.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cnt++
	return nil
}
func (r *countRaft) ReportUnreachable(id uint64)                          {}
func (r *countRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (r *countRaft) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cnt
}
