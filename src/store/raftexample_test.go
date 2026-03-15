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

package store_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func getSnapshotPathFn() (func(string) error, <-chan struct{}) {
	snapshotTriggeredC := make(chan struct{})
	return func(string) error {
		snapshotTriggeredC <- struct{}{}
		return nil
	}, snapshotTriggeredC
}

type cluster struct {
	clusterID          uint64
	peers              []common.Peer
	commitC            []<-chan *raft.Commit
	errorC             []<-chan error
	proposeC           []chan *raft.Proposal
	confChangeC        []chan *raft.ConfChangeProposal
	snapshotTriggeredC []<-chan struct{}

	raftServers []*raft.MultiRaft
	raftNodes   []raft.RaftNode
}

// newCluster creates a cluster of n nodes
func newCluster(t *testing.T, n int) *cluster {
	t.Cleanup(func() {
		os.RemoveAll("antflydb")
	})
	peers := make(common.Peers, n)
	for i := range peers {
		peers[i] = common.Peer{
			ID:  types.ID(i + 1),
			URL: fmt.Sprintf("http://127.0.0.1:%d", 10000+i),
		}
	}

	clusterID := rand.Uint64()
	clus := &cluster{
		clusterID:          clusterID,
		peers:              peers,
		commitC:            make([]<-chan *raft.Commit, len(peers)),
		errorC:             make([]<-chan error, len(peers)),
		proposeC:           make([]chan *raft.Proposal, len(peers)),
		confChangeC:        make([]chan *raft.ConfChangeProposal, len(peers)),
		snapshotTriggeredC: make([]<-chan struct{}, len(peers)),
		raftServers:        make([]*raft.MultiRaft, len(peers)),
		raftNodes:          make([]raft.RaftNode, len(peers)),
	}

	var err error
	var wg sync.WaitGroup
	for i := range clus.peers {
		nodeID := types.ID(i + 1)
		clusterID := types.ID(clusterID)
		os.RemoveAll(common.ShardDir(common.RootAntflyDir, clusterID, nodeID))
		// logger := zaptest.NewLogger(t)
		logger := zap.NewNop()
		clus.raftServers[i], err = raft.NewMultiRaftServer(
			logger,
			common.RootAntflyDir,
			types.ID(nodeID),
			string(peers[i].URL),
		)
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func(i int) {
			wg.Done()
			clus.raftServers[i].Start()
		}(i)
		clus.proposeC[i] = make(chan *raft.Proposal, 1)
		clus.confChangeC[i] = make(chan *raft.ConfChangeProposal, 1)
		fn, snapshotTriggeredC := getSnapshotPathFn()
		clus.snapshotTriggeredC[i] = snapshotTriggeredC
		snapStore, err := snapstore.NewLocalSnapStore(common.RootAntflyDir, clusterID, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		raftConf := raft.RaftNodeConfig{
			SnapStore:                snapStore,
			RaftLogDir:               common.RaftLogDir(common.RootAntflyDir, clusterID, nodeID),
			Peers:                    peers,
			EnableProposalForwarding: true,
		}
		clus.commitC[i], clus.errorC[i], clus.raftNodes[i] = raft.NewRaftNode(
			logger,
			clusterID,
			nodeID,
			raftConf,
			clus.raftServers[i],
			fn,
			clus.proposeC[i],
			clus.confChangeC[i],
		)
	}
	wg.Wait()

	return clus
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	for i := range clus.peers {
		go func(i int) {
			for range clus.commitC[i] { //revive:disable-line:empty-block
				// drain pending commits
			}
		}(i)
		if clus.proposeC[i] != nil {
			close(clus.proposeC[i])
		}
		clus.raftServers[i].Stop()
		// wait for channel to close
		if erri := <-clus.errorC[i]; erri != nil {
			err = erri
		}
		// clean intermediates
		os.RemoveAll(common.NodeDir(common.RootAntflyDir, types.ID(i+1)))
	}
	return err
}

func (clus *cluster) closeNoErrors(t *testing.T) {
	t.Log("closing cluster...")
	require.NoError(t, clus.Close())
	t.Log("closing cluster [done]")
}

// TestProposeOnCommit starts three nodes and feeds commits back into the proposal
// channel. The intent is to ensure blocking on a proposal won't block raft progress.
func TestProposeOnCommit(t *testing.T) {
	clus := newCluster(t, 3)
	defer clus.closeNoErrors(t)
	time.Sleep(1 * time.Second)

	donec := make(chan struct{})
	for i := range clus.peers {
		// feedback for "n" committed entries, then update donec
		go func(pC chan<- *raft.Proposal, cC <-chan *raft.Commit, eC <-chan error) {
			for range 100 {
				c, ok := <-cC
				if !ok {
					pC = nil
				}
				select {
				case pC <- &raft.Proposal{Data: c.Data[0]}:
					continue
				case err := <-eC:
					t.Errorf("eC message (%v)", err)
				}
			}
			donec <- struct{}{}
			for range cC { //revive:disable-line:empty-block
				// acknowledge the commits from other nodes so
				// raft continues to make progress
			}
		}(clus.proposeC[i], clus.commitC[i], clus.errorC[i])

		// one message feedback per node
		go func(i int) { clus.proposeC[i] <- &raft.Proposal{Data: []byte("foo")} }(i)
	}

	for range clus.peers {
		<-donec
	}
}

// TestCloseProposerBeforeReplay tests closing the producer before raft starts.
func TestCloseProposerBeforeReplay(t *testing.T) {
	clus := newCluster(t, 1)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)

	// There's a race with starting up of the raft group goroutines
	time.Sleep(10 * time.Millisecond)
}

// TestCloseProposerInflight tests closing the producer while
// committed messages are being published to the client.
func TestCloseProposerInflight(t *testing.T) {
	clus := newCluster(t, 1)
	defer clus.closeNoErrors(t)
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)

	require.Eventually(t, func() bool {
		return clus.raftNodes[0].Status().Lead != 0
	}, 10*time.Second, time.Second)
	lead := clus.raftNodes[0].Status().Lead - 1
	// some inflight ops
	go func() {
		defer wg.Done()
		clus.proposeC[lead] <- &raft.Proposal{Data: []byte("foo")}
		clus.proposeC[lead] <- &raft.Proposal{Data: []byte("bar")}
	}()

	// wait for one message
	if c, ok := <-clus.commitC[0]; !ok || string(c.Data[0]) != "foo" {
		t.Fatalf("Commit failed")
	}

	wg.Wait()
}

func TestPutAndGetKeyValue(t *testing.T) {
	clusters := []common.Peer{{ID: 1, URL: "http://127.0.0.1:9021"}}

	// Use a temp directory to avoid conflicts with running antfly instances
	testDir := t.TempDir()

	// logger := zaptest.NewLogger(t)
	logger := zap.NewNop()
	rs, err := raft.NewMultiRaftServer(logger, testDir, 1, clusters[0].URL)
	require.NoError(t, err)
	go rs.Start()
	shardID := types.ID(0x1000)

	// Create a config with the test directory
	testConfig := &common.Config{}
	testConfig.Storage.Local.BaseDir = testDir
	multikvs, _ /* errChan */, err := store.NewStore(
		logger,
		testConfig,
		rs,
		&store.StoreInfo{ID: 1},
		nil,
	)
	require.NoError(t, err)
	multikvs.StartRaftGroup(shardID, clusters, false, &store.ShardStartConfig{
		ShardConfig: store.ShardConfig{
			ByteRange: [2][]byte{[]byte("a"), []byte("z")},
		},
	})
	defer multikvs.StopRaftGroup(shardID)

	srv := httptest.NewServer(multikvs.NewHttpAPI())
	defer srv.Close()

	// wait server started
	<-time.After(time.Second * 3)

	wantKey, wantValue := "test-key", `{"foo":"test-value"}`
	cli := srv.Client()
	storeClient := client.NewStoreClient(cli, 1, srv.URL)
	err = storeClient.Batch(
		context.Background(),
		shardID,
		[][2][]byte{{[]byte(wantKey), []byte(wantValue)}},
		nil,
		nil,
		db.Op_SyncLevelPropose,
	)
	require.NoError(t, err)

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(time.Second)

	data, err := storeClient.Lookup(context.Background(), shardID, []string{wantKey})
	require.NoError(t, err)
	gotValue := string(data[wantKey])
	require.Equalf(t, wantValue, gotValue, "expect %s, got %s", wantValue, gotValue)
}

// TestAddNewNode tests adding new node to the existing cluster.
func TestAddNewNode(t *testing.T) {
	clus := newCluster(t, 3)
	defer clus.closeNoErrors(t)
	clusterID := types.ID(clus.clusterID)
	err := os.RemoveAll(common.NodeDir(common.RootAntflyDir, 4))
	require.NoError(t, err, "Failed to remove node directory")
	defer func() {
		os.RemoveAll(common.NodeDir(common.RootAntflyDir, 4))
	}()
	newNodeURL := "http://127.0.0.1:10004"
	proposeConfChangeDoneC := make(chan error)
	require.Eventually(t, func() bool {
		return clus.raftNodes[0].Status().Lead != 0
	}, 10*time.Second, time.Second)
	lead := clus.raftNodes[0].Status().Lead - 1
	ccc := raft.ConfChangeContext_builder{
		Url: newNodeURL,
	}.Build()
	b, err := proto.Marshal(ccc)
	require.NoError(t, err)

	clus.confChangeC[lead] <- &raft.ConfChangeProposal{
		ProposeDoneC: proposeConfChangeDoneC,
		ConfChange: raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  4,
			Context: b,
		},
	}
	require.NoError(t, <-proposeConfChangeDoneC, "Failed to propose conf change")

	proposeC := make(chan *raft.Proposal)
	defer close(proposeC)

	confChangeC := make(chan *raft.ConfChangeProposal)
	defer close(confChangeC)

	// logger := zaptest.NewLogger(t)
	logger := zap.NewNop()
	rs, err := raft.NewMultiRaftServer(logger, common.RootAntflyDir, 4, newNodeURL)
	if err != nil {
		t.Fatalf("Failed to create raft server: %v", err)
	}
	go rs.Start()

	snapStore, err := snapstore.NewLocalSnapStore(common.RootAntflyDir, clusterID, 4)
	if err != nil {
		t.Fatal(err)
	}
	raftConf := raft.RaftNodeConfig{
		SnapStore:  snapStore,
		RaftLogDir: common.RaftLogDir(common.RootAntflyDir, clusterID, 4),
		Peers:      append(clus.peers, common.Peer{ID: 4, URL: newNodeURL}),
	}
	_, _, _ = raft.NewRaftNode(
		logger,
		clusterID,
		4,
		raftConf,
		rs,
		nil,
		proposeC,
		confChangeC,
	)
	time.Sleep(3 * time.Second)
	proposeDoneC := make(chan error)
	go func() {
		proposeC <- &raft.Proposal{Data: []byte("foo"), ProposeDoneC: proposeDoneC}
	}()

	err = <-proposeDoneC
	require.Error(t, err, "Proposal should be dropped on non leader node")
}

func TestSnapshot(t *testing.T) {
	prevDefaultSnapshotCount := raft.DefaultSnapshotCount
	prevSnapshotCatchUpEntriesN := raft.SnapshotCatchUpEntriesN
	raft.DefaultSnapshotCount = 4
	raft.SnapshotCatchUpEntriesN = 4
	defer func() {
		raft.DefaultSnapshotCount = prevDefaultSnapshotCount
		raft.SnapshotCatchUpEntriesN = prevSnapshotCatchUpEntriesN
	}()

	clus := newCluster(t, 3)
	defer clus.closeNoErrors(t)

	require.Eventually(t, func() bool {
		return clus.raftNodes[0].Status().Lead != 0
	}, 10*time.Second, time.Second)
	lead := clus.raftNodes[0].Status().Lead - 1
	go func() {
		clus.proposeC[lead] <- &raft.Proposal{Data: []byte("foo")}
	}()

	c := <-clus.commitC[0]

	select {
	case <-clus.snapshotTriggeredC[0]:
		t.Fatalf("snapshot triggered before applying done")
	default:
	}
	close(c.ApplyDoneC)
	<-clus.snapshotTriggeredC[0]
}
