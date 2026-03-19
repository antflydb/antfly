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

package sim

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type fakeRaft struct {
	mu       sync.Mutex
	messages []raftpb.Message
}

func (f *fakeRaft) Process(_ context.Context, m raftpb.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, m)
	return nil
}

func (f *fakeRaft) ReportUnreachable(id uint64)                          {}
func (f *fakeRaft) IsIDRemoved(id uint64) bool                           { return false }
func (f *fakeRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (f *fakeRaft) received() []raftpb.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]raftpb.Message, len(f.messages))
	copy(out, f.messages)
	return out
}

type memorySnapStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemorySnapStore() *memorySnapStore {
	return &memorySnapStore{data: make(map[string][]byte)}
}

func (m *memorySnapStore) Get(_ context.Context, snapID string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[snapID]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), b...))), nil
}

func (m *memorySnapStore) Put(_ context.Context, snapID string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[snapID] = append([]byte(nil), b...)
	return nil
}

func (m *memorySnapStore) Delete(_ context.Context, snapID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, snapID)
	return nil
}

func (m *memorySnapStore) Path(snapID string) (string, error) { return "", nil }

func (m *memorySnapStore) Exists(_ context.Context, snapID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[snapID]
	return ok, nil
}

func (m *memorySnapStore) List(_ context.Context) ([]string, error) { return nil, nil }
func (m *memorySnapStore) RemoveAll(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string][]byte)
	return nil
}

func (m *memorySnapStore) CreateSnapshot(
	_ context.Context,
	_ string,
	_ string,
	_ *snapstore.SnapshotOptions,
) (int64, error) {
	return 0, nil
}

func (m *memorySnapStore) ExtractSnapshot(
	_ context.Context,
	_ string,
	_ string,
	_ bool,
) (*common.ArchiveMetadata, error) {
	return nil, nil
}

func TestTransportDeliversAfterAdvance(t *testing.T) {
	network := NewNetwork(time.Unix(0, 0))
	network.SetDefaultLatency(5 * time.Millisecond)

	n1 := network.NewTransport(1)
	n2 := network.NewTransport(2)
	r1 := &fakeRaft{}
	r2 := &fakeRaft{}
	peers := []raft.Peer{{ID: 1}, {ID: 2}}

	require.NoError(t, n1.ServeRaft(10, r1, peers))
	require.NoError(t, n2.ServeRaft(10, r2, peers))

	n1.Send(10, []raftpb.Message{{Type: raftpb.MsgApp, From: 1, To: 2, Index: 7}})
	require.Empty(t, r2.received())

	delivered := network.Advance(5 * time.Millisecond)
	require.Equal(t, 1, delivered)
	require.Len(t, r2.received(), 1)
	require.Equal(t, uint64(7), r2.received()[0].Index)
}

func TestTransportSupportsDirectionalCutsAndDuplication(t *testing.T) {
	network := NewNetwork(time.Unix(0, 0))
	network.SetDefaultLatency(time.Millisecond)

	n1 := network.NewTransport(1)
	n2 := network.NewTransport(2)
	r1 := &fakeRaft{}
	r2 := &fakeRaft{}
	peers := []raft.Peer{{ID: 1}, {ID: 2}}

	require.NoError(t, n1.ServeRaft(10, r1, peers))
	require.NoError(t, n2.ServeRaft(10, r2, peers))

	network.CutLink(1, 2)
	n1.Send(10, []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 1, To: 2}})
	network.Advance(time.Millisecond)
	require.Empty(t, r2.received())

	network.HealLink(1, 2)
	network.DuplicateNext(func(env MessageEnvelope) bool {
		return env.From == 1 && env.To == 2 && env.Message.Type == raftpb.MsgHeartbeat
	}, 1)
	n1.Send(10, []raftpb.Message{{Type: raftpb.MsgHeartbeat, From: 1, To: 2}})
	network.Advance(time.Millisecond)
	require.Len(t, r2.received(), 2)

	n2.Send(10, []raftpb.Message{{Type: raftpb.MsgHeartbeatResp, From: 2, To: 1}})
	network.Advance(time.Millisecond)
	require.Len(t, r1.received(), 1)
}

func TestTransportPausedNodeQueuesUntilResume(t *testing.T) {
	network := NewNetwork(time.Unix(0, 0))
	network.SetDefaultLatency(time.Millisecond)

	n1 := network.NewTransport(1)
	n2 := network.NewTransport(2)
	r2 := &fakeRaft{}
	peers := []raft.Peer{{ID: 1}, {ID: 2}}

	require.NoError(t, n1.ServeRaft(10, &fakeRaft{}, peers))
	require.NoError(t, n2.ServeRaft(10, r2, peers))

	network.PauseNode(2)
	n1.Send(10, []raftpb.Message{{Type: raftpb.MsgApp, From: 1, To: 2}})
	require.Equal(t, 0, network.Advance(time.Millisecond))
	require.Empty(t, r2.received())
	require.Equal(t, 1, network.Pending())

	network.ResumeNode(2)
	require.Equal(t, 1, network.DeliverReady())
	require.Len(t, r2.received(), 1)
}

func TestTransportReordersByLatency(t *testing.T) {
	network := NewNetwork(time.Unix(0, 0))
	network.SetDefaultLatency(10 * time.Millisecond)

	n1 := network.NewTransport(1)
	n2 := network.NewTransport(2)
	r2 := &fakeRaft{}
	peers := []raft.Peer{{ID: 1}, {ID: 2}}

	require.NoError(t, n1.ServeRaft(10, &fakeRaft{}, peers))
	require.NoError(t, n2.ServeRaft(10, r2, peers))

	n1.Send(10, []raftpb.Message{{Type: raftpb.MsgApp, From: 1, To: 2, Index: 1}})
	network.SetLinkLatency(1, 2, time.Millisecond)
	n1.Send(10, []raftpb.Message{{Type: raftpb.MsgApp, From: 1, To: 2, Index: 2}})

	require.Equal(t, 1, network.Advance(time.Millisecond))
	require.Len(t, r2.received(), 1)
	require.Equal(t, uint64(2), r2.received()[0].Index)

	require.Equal(t, 1, network.Advance(9*time.Millisecond))
	require.Len(t, r2.received(), 2)
	require.Equal(t, uint64(1), r2.received()[1].Index)
}

func TestTransportCopiesSnapshotsFromPeers(t *testing.T) {
	network := NewNetwork(time.Unix(0, 0))
	n1 := network.NewTransport(1)
	n2 := network.NewTransport(2)
	peers := []raft.Peer{{ID: 1}, {ID: 2}}

	require.NoError(t, n1.ServeRaft(10, &fakeRaft{}, peers))
	require.NoError(t, n2.ServeRaft(10, &fakeRaft{}, peers))

	source := newMemorySnapStore()
	dest := newMemorySnapStore()
	require.NoError(t, source.Put(context.Background(), "snap-1", bytes.NewReader([]byte("payload"))))
	network.RegisterSnapshotStore(2, 10, source)

	require.NoError(t, n1.GetSnapshot(context.Background(), 10, dest, "snap-1"))
	b, err := ReadSnapshot(context.Background(), dest, "snap-1")
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), b)
}

func TestTransportSnapshotCopyRespectsDropAndCuts(t *testing.T) {
	network := NewNetwork(time.Unix(0, 0))
	n1 := network.NewTransport(1)
	n2 := network.NewTransport(2)
	peers := []raft.Peer{{ID: 1}, {ID: 2}}

	require.NoError(t, n1.ServeRaft(10, &fakeRaft{}, peers))
	require.NoError(t, n2.ServeRaft(10, &fakeRaft{}, peers))

	source := newMemorySnapStore()
	dest := newMemorySnapStore()
	require.NoError(t, source.Put(context.Background(), "snap-1", bytes.NewReader([]byte("payload"))))
	network.RegisterSnapshotStore(2, 10, source)

	network.DropNext(func(env MessageEnvelope) bool {
		return env.ShardID == 10 && env.From == 2 && env.To == 1 && env.Message.Type == raftpb.MsgSnap
	})
	err := n1.GetSnapshot(context.Background(), 10, dest, "snap-1")
	require.ErrorIs(t, err, ErrSnapshotNotFound)

	network.CutLink(2, 1)
	err = n1.GetSnapshot(context.Background(), 10, dest, "snap-1")
	require.ErrorIs(t, err, ErrSnapshotNotFound)

	network.HealLink(2, 1)
	require.NoError(t, n1.GetSnapshot(context.Background(), 10, dest, "snap-1"))
	b, err := ReadSnapshot(context.Background(), dest, "snap-1")
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), b)
}
