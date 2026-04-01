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
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
)

func TestGetSnapshotIDReturnsInitError(t *testing.T) {
	expectedErr := errors.New("raft init failed")
	rc := &raftNode{
		commitC:          make(chan *Commit),
		logger:           zaptest.NewLogger(t),
		snapshotterReady: make(chan func() (string, error), 1),
	}

	rc.signalInitFailure(expectedErr)

	snapshotID, err := rc.GetSnapshotID(context.Background())
	require.Empty(t, snapshotID)
	require.ErrorIs(t, err, expectedErr)
}

func TestLoadSnapshotRetriesInitialSnapshotFetch(t *testing.T) {
	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	transport := &retrySnapshotTransport{failuresBeforeSuccess: 2}
	rc := &raftNode{
		shardID:                 10,
		id:                      1,
		raftLogStorage:          ps,
		snapStore:               &mockSnapStore{},
		initWithStorageSnapshot: "split-archive",
		transport:               transport,
		clock:                   clock.RealClock{},
		logger:                  zaptest.NewLogger(t),
	}

	_, err := rc.loadSnapshot(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 3, transport.attempts.Load())
}

func TestLoadSnapshotReturnsErrorWhenInitialSnapshotFetchFails(t *testing.T) {
	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	transport := &retrySnapshotTransport{failuresBeforeSuccess: 1 << 30}
	rc := &raftNode{
		shardID:                 10,
		id:                      1,
		raftLogStorage:          ps,
		snapStore:               &mockSnapStore{},
		initWithStorageSnapshot: "split-archive",
		transport:               transport,
		clock:                   clock.RealClock{},
		logger:                  zaptest.NewLogger(t),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	snapshotID, err := rc.loadSnapshot(ctx)
	require.Empty(t, snapshotID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetching initial snapshot split-archive")
	require.Contains(t, err.Error(), "canceled")
}

func TestProcessMessages(t *testing.T) {
	cases := []struct {
		name             string
		confState        raftpb.ConfState
		InputMessages    []raftpb.Message
		ExpectedMessages []raftpb.Message
	}{
		{
			name: "only one snapshot message",
			confState: raftpb.ConfState{
				Voters: []uint64{2, 6, 8, 10},
			},
			InputMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: &raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters:    []uint64{2, 6, 8},
								AutoLeave: true,
							},
						},
					},
				},
			},
			ExpectedMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: &raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters: []uint64{2, 6, 8, 10},
							},
						},
					},
				},
			},
		},
		{
			name: "one snapshot message and one other message",
			confState: raftpb.ConfState{
				Voters: []uint64{2, 7, 8, 12},
			},
			InputMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: &raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters:    []uint64{2, 6, 8},
								AutoLeave: true,
							},
						},
					},
				},
				{
					Type: raftpb.MsgApp,
					From: 6,
					To:   8,
				},
			},
			ExpectedMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: &raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters: []uint64{2, 7, 8, 12},
							},
						},
					},
				},
				{
					Type: raftpb.MsgApp,
					From: 6,
					To:   8,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rn := &raftNode{
				confState: tc.confState,
			}

			outputMessages := make([]raftpb.Message, 0, len(tc.InputMessages))
			outputMessages = rn.processMessages(outputMessages, tc.InputMessages)
			require.Truef(
				t,
				reflect.DeepEqual(outputMessages, tc.ExpectedMessages),
				"Unexpected messages, expected: %v, got %v",
				tc.ExpectedMessages,
				outputMessages,
			)
		})
	}
}

// mockSnapStore is a minimal SnapStore for testing that records Delete calls.
type mockSnapStore struct {
	deleted []string
}

func (m *mockSnapStore) Delete(_ context.Context, snapID string) error {
	m.deleted = append(m.deleted, snapID)
	return nil
}

func (m *mockSnapStore) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockSnapStore) Put(context.Context, string, io.Reader) error {
	return fmt.Errorf("not implemented")
}
func (m *mockSnapStore) Path(string) (string, error) {
	return "", fmt.Errorf("not implemented")
}
func (m *mockSnapStore) Exists(context.Context, string) (bool, error) {
	return false, fmt.Errorf("not implemented")
}
func (m *mockSnapStore) List(context.Context) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockSnapStore) RemoveAll(context.Context) error {
	return fmt.Errorf("not implemented")
}
func (m *mockSnapStore) CreateSnapshot(context.Context, string, string, *snapstore.SnapshotOptions) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (m *mockSnapStore) ExtractSnapshot(context.Context, string, string, bool) (*common.ArchiveMetadata, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestMaybeTriggerSnapshot_SnapshotIDNotEmpty(t *testing.T) {
	// This test verifies that subsequent snapshots (not the initial one) correctly
	// set the snapshot ID in the Raft snapshot metadata. A bug existed where
	// `:=` was used instead of `=` on the snapshotID variable in the else branch,
	// causing the outer snapshotID to remain empty. This meant the Raft snapshot
	// metadata had an empty ID, preventing followers from fetching snapshot data
	// from the leader and getting stuck in an infinite probe loop.
	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Seed entries so PebbleStorage allows CreateSnapshot at the target index.
	entries := make([]raftpb.Entry, 0, 20)
	for i := uint64(1); i <= 20; i++ {
		entries = append(entries, raftpb.Entry{Index: i, Term: 1})
	}
	require.NoError(t, ps.append(entries))

	var createdSnapshotID string
	snapStore := &mockSnapStore{}

	rn := &raftNode{
		shardID:        1,
		id:             2,
		snapCount:      5, // appliedIndex - snapshotIndex (20-10=10) > 5, so triggers
		raftLogStorage: ps,
		snapStore:      snapStore,
		confState:      raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		stopc:          make(chan struct{}),
		logger:         zaptest.NewLogger(t),
		createStorageSnapshot: func(id string) error {
			createdSnapshotID = id
			return nil
		},
	}
	rn.appliedIndex.Store(20)
	rn.snapshotIndex.Store(10) // Non-zero: forces the else branch (not init path)

	rn.maybeTriggerSnapshot(nil)

	// Verify createStorageSnapshot was called with a non-empty ID.
	expectedID := fmt.Sprintf("snapshot-%d-%d-%d", rn.shardID, rn.id, uint64(20))
	require.Equal(t, expectedID, createdSnapshotID,
		"createStorageSnapshot should be called with the correct snapshot ID")

	// Verify the Raft snapshot stored in PebbleStorage has the correct metadata.
	snap, err := ps.LoadSnapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	var meta common.StorageSnapshotMetadata
	require.NoError(t, proto.Unmarshal(snap.Data, &meta))
	require.Equal(t, expectedID, meta.GetID(),
		"Raft snapshot metadata must contain the snapshot ID, not an empty string")
}

// mockRaftNode is a minimal mock of raft.Node for testing publishEntries.
// Only ApplyConfChange is implemented; other methods panic if called.
type mockRaftNode struct {
	raft.Node // embedded nil interface — panics on unimplemented calls
	applied   []raftpb.ConfChange
}

func (m *mockRaftNode) ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState {
	if v, ok := cc.(raftpb.ConfChange); ok {
		m.applied = append(m.applied, v)
		// Return a confState that includes the new node
		voters := make([]uint64, 0)
		for _, prev := range m.applied {
			if prev.Type == raftpb.ConfChangeAddNode {
				voters = append(voters, prev.NodeID)
			}
		}
		return &raftpb.ConfState{Voters: voters}
	}
	return &raftpb.ConfState{}
}

// mockTransport is a minimal mock of Transport for testing.
type mockTransport struct {
	addedPeers []struct {
		shardID types.ID
		nodeID  types.ID
		urls    []string
	}
}

type retrySnapshotTransport struct {
	failuresBeforeSuccess int32
	attempts              atomic.Int32
}

func (m *retrySnapshotTransport) ErrorC() <-chan error                { return nil }
func (m *retrySnapshotTransport) Send(_ types.ID, _ []raftpb.Message) {}
func (m *retrySnapshotTransport) GetSnapshot(_ context.Context, _ types.ID, _ snapstore.SnapStore, _ string) error {
	attempt := m.attempts.Add(1)
	if attempt <= m.failuresBeforeSuccess {
		return errors.New("snapshot not found")
	}
	return nil
}
func (m *retrySnapshotTransport) AddPeer(_, _ types.ID, _ []string) {}
func (m *retrySnapshotTransport) RemovePeer(_, _ types.ID)          {}
func (m *retrySnapshotTransport) ServeRaft(_ types.ID, _ Raft, _ []raft.Peer) error {
	return nil
}
func (m *retrySnapshotTransport) StopServeRaft(_ types.ID) {}

func (m *mockTransport) ErrorC() <-chan error                { return nil }
func (m *mockTransport) Send(_ types.ID, _ []raftpb.Message) {}
func (m *mockTransport) GetSnapshot(_ context.Context, _ types.ID, _ snapstore.SnapStore, _ string) error {
	return nil
}
func (m *mockTransport) AddPeer(shardID, nodeID types.ID, us []string) {
	m.addedPeers = append(m.addedPeers, struct {
		shardID types.ID
		nodeID  types.ID
		urls    []string
	}{shardID, nodeID, us})
}
func (m *mockTransport) RemovePeer(_, _ types.ID)                          {}
func (m *mockTransport) ServeRaft(_ types.ID, _ Raft, _ []raft.Peer) error { return nil }
func (m *mockTransport) StopServeRaft(_ types.ID)                          {}

type blockingProposeNode struct {
	raft.Node
	proposeStarted chan struct{}
}

func (m *blockingProposeNode) Propose(ctx context.Context, _ []byte) error {
	close(m.proposeStarted)
	<-ctx.Done()
	return ctx.Err()
}

func (m *blockingProposeNode) Stop() {}

func TestRaftNodeShutdownCancelsInflightProposal(t *testing.T) {
	proposalCtx, cancel := context.WithCancel(context.Background())
	errC := make(chan error)
	node := &blockingProposeNode{proposeStarted: make(chan struct{})}
	rc := &raftNode{
		node:           node,
		transport:      &mockTransport{},
		errorC:         errC,
		stopc:          make(chan struct{}),
		logger:         zaptest.NewLogger(t),
		serveCtxCancel: cancel,
	}

	done := make(chan error, 1)
	go func() {
		done <- node.Propose(proposalCtx, []byte("blocked"))
	}()

	select {
	case <-node.proposeStarted:
	case <-time.After(time.Second):
		t.Fatal("proposal did not start")
	}

	rc.Shutdown()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("proposal did not exit after shutdown")
	}

	select {
	case <-rc.stopc:
	case <-time.After(time.Second):
		t.Fatal("raft stop channel was not closed")
	}

	select {
	case _, ok := <-errC:
		require.False(t, ok, "error channel should be closed on shutdown")
	case <-time.After(time.Second):
		t.Fatal("error channel was not closed on shutdown")
	}
}

// TestConfChangeAddNodeSelf verifies that ConfChangeAddNode for the local
// node still calls ApplyConfChange to update the raft membership. This is a
// regression test for a bug where the self-node case skipped ApplyConfChange
// entirely, leaving the raft state machine with stale membership.
func TestConfChangeAddNodeSelf(t *testing.T) {
	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	transport := &mockTransport{}
	mockNode := &mockRaftNode{}

	selfID := types.ID(1)
	otherID := types.ID(2)

	rn := &raftNode{
		shardID:             types.ID(100),
		id:                  selfID,
		node:                mockNode,
		transport:           transport,
		raftLogStorage:      ps,
		confChangeCallbacks: xsync.NewMap[uuid.UUID, chan struct{}](),
		commitC:             make(chan<- *Commit, 10),
		stopc:               make(chan struct{}),
		logger:              zaptest.NewLogger(t),
	}

	// Build ConfChangeAddNode entries for both self and another node
	selfCCC := &ConfChangeContext{}
	selfCCC.SetUrl("http://self:8080")
	selfCtx, _ := proto.Marshal(selfCCC)
	otherCCC := &ConfChangeContext{}
	otherCCC.SetUrl("http://other:8080")
	otherCtx, _ := proto.Marshal(otherCCC)

	selfCC := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(selfID),
		Context: selfCtx,
	}
	otherCC := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(otherID),
		Context: otherCtx,
	}

	selfData, _ := selfCC.Marshal()
	otherData, _ := otherCC.Marshal()

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryConfChange, Data: selfData},
		{Index: 2, Term: 1, Type: raftpb.EntryConfChange, Data: otherData},
	}

	_, err := rn.publishEntries(entries)
	require.NoError(t, err)

	// ApplyConfChange must have been called for BOTH entries (self and other)
	require.Len(t, mockNode.applied, 2,
		"ApplyConfChange must be called for self-node ConfChange, not skipped")
	require.Equal(t, uint64(selfID), mockNode.applied[0].NodeID)
	require.Equal(t, uint64(otherID), mockNode.applied[1].NodeID)

	// confState should reflect both nodes
	rn.confStateMu.RLock()
	require.Contains(t, rn.confState.Voters, uint64(selfID),
		"confState must include self after ConfChangeAddNode")
	require.Contains(t, rn.confState.Voters, uint64(otherID),
		"confState must include other after ConfChangeAddNode")
	rn.confStateMu.RUnlock()

	// Transport should NOT have AddPeer called for self, only for other
	require.Len(t, transport.addedPeers, 1,
		"transport.AddPeer should only be called for non-self nodes")
	require.Equal(t, otherID, transport.addedPeers[0].nodeID)
}

// TestProcessCapsHeartbeatCommit verifies that Process caps the commit in
// incoming MsgHeartbeat messages to the storage's LastIndex. This prevents a
// panic in etcd raft v3.6.0's commitTo when a leader has stale Match progress
// for a follower that restarted with an empty log (e.g. after StopRaftGroup
// wiped the raft log directory).
func TestProcessCapsHeartbeatCommit(t *testing.T) {
	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Storage is empty: lastIndex=0
	li, err := ps.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), li)

	// Bootstrap a minimal single-node raft cluster so we can call Process.
	require.NoError(t, ps.SaveRaftState(
		nil,
		raftpb.ConfState{Voters: []uint64{1, 2}},
		raftpb.HardState{Term: 1, Vote: 1},
		nil,
	))

	logger := zaptest.NewLogger(t)
	c := &raft.Config{
		ID:              2,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         ps,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
	}
	node := raft.RestartNode(c)
	defer node.Stop()

	rc := &raftNode{
		id:             2,
		raftLogStorage: ps,
		node:           node,
		logger:         logger,
	}

	// Simulate a heartbeat from the leader with a commit index that exceeds
	// our lastIndex. Without the cap this panics inside commitTo.
	hb := raftpb.Message{
		Type:   raftpb.MsgHeartbeat,
		From:   1,
		To:     2,
		Term:   1,
		Commit: 174, // stale — our log is empty
	}
	// This must NOT panic.
	err = rc.Process(context.Background(), hb)
	// Step may return nil or an error (e.g. the node may reject messages
	// depending on internal state), but it must not panic.
	_ = err
}
