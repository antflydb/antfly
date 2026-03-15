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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	raft.PebbleStorageInMem = true
}

type multicluster struct {
	nodes []*node
}

type node struct {
	nodeID    types.ID
	multiRaft *raft.MultiRaft

	shards []*shard
}
type shard struct {
	shardID  types.ID
	raftNode raft.RaftNode

	commitC            <-chan *raft.Commit
	errorC             <-chan error
	proposeC           chan *raft.Proposal
	confChangeC        chan *raft.ConfChangeProposal
	snapshotTriggeredC <-chan struct{}
}

// newCluster creates a cluster of n nodes
func newMultiCluster(t *testing.T, nodes int, groups int) *multicluster {
	ns := make([]*node, nodes)
	peers := make(common.Peers, nodes)
	var wg sync.WaitGroup
	for i := range ns {
		url := fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
		logger := zap.NewNop()
		// logger := zaptest.NewLogger(t)
		logger = logger.With(zap.Stringer("nodeID", types.ID(i+1)))
		rs, err := raft.NewMultiRaftServer(logger, common.RootAntflyDir, types.ID(i+1), url)
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func() {
			wg.Done()
			rs.Start()
		}()
		ns[i] = &node{multiRaft: rs, nodeID: types.ID(i + 1)}
		peers[i] = common.Peer{ID: types.ID(i + 1), URL: url}
	}

	for _, node := range ns {
		gs := make([]*shard, groups)
		for j := range gs {
			shardID := types.ID(j + 1)

			os.RemoveAll(common.ShardDir(common.RootAntflyDir, shardID, node.nodeID))
			proposeC := make(chan *raft.Proposal, 1)
			confChangeC := make(chan *raft.ConfChangeProposal, 1)
			fn, snapshotTriggeredC := getSnapshotPathFn()
			snapStore, err := snapstore.NewLocalSnapStore(common.RootAntflyDir, shardID, node.nodeID)
			if err != nil {
				t.Fatal(err)
			}
			raftConf := raft.RaftNodeConfig{
				SnapStore:                snapStore,
				RaftLogDir:               common.RaftLogDir(common.RootAntflyDir, shardID, node.nodeID),
				Peers:                    peers,
				EnableProposalForwarding: true,
			}
			logger := zap.NewNop()
			// logger := zaptest.NewLogger(t)
			logger = logger.With(
				zap.Stringer("nodeID", node.nodeID),
				zap.Stringer("shardID", shardID),
			)
			commitC, errorC, raftNode := raft.NewRaftNode(
				logger,
				shardID,
				node.nodeID,
				raftConf,
				node.multiRaft,
				fn,
				proposeC,
				confChangeC,
			)

			gs[j] = &shard{
				shardID:            shardID,
				raftNode:           raftNode,
				commitC:            commitC,
				proposeC:           proposeC,
				confChangeC:        confChangeC,
				errorC:             errorC,
				snapshotTriggeredC: snapshotTriggeredC,
			}
		}
		node.shards = gs
	}
	clus := &multicluster{
		nodes: ns,
	}

	wg.Wait()
	return clus
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *multicluster) Close() (err error) {
	for _, node := range clus.nodes {
		for _, group := range node.shards {
			go func(commitC <-chan *raft.Commit) {
				for range commitC { //revive:disable-line:empty-block
					// drain pending commits
				}
			}(group.commitC)
			close(group.proposeC)
		}
		node.multiRaft.Stop()
		for _, shard := range node.shards {
			// wait for channel to close
			if erri := <-shard.errorC; erri != nil {
				err = erri
			}
			// clean intermediates
			os.RemoveAll(common.NodeDir(common.RootAntflyDir, node.nodeID))
		}
	}
	return err
}

func (clus *multicluster) closeNoErrors(t *testing.T) {
	t.Log("closing cluster...")
	require.NoError(t, clus.Close())
	t.Log("closing cluster [done]")
}

// TestProposeOnCommitMulti starts three nodes and feeds commits back into the proposal
// channel. The intent is to ensure blocking on a proposal won't block raft progress.
func TestProposeOnCommitMulti(t *testing.T) {
	numShards := 3
	numNodes := 3
	clus := newMultiCluster(t, numNodes, numShards)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer clus.closeNoErrors(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	donec := make(chan struct{})
	for i := range numShards {
		require.Eventually(t, func() bool {
			return clus.nodes[0].shards[i].raftNode.Status().Lead != 0
		}, 10*time.Second, time.Second)
		t.Logf(
			"Leader started for shard: %s raftStatus: %v",
			types.ID(i+1),
			clus.nodes[0].shards[i].raftNode.Status(),
		)
	}
	for _, node := range clus.nodes {
		for _, group := range node.shards {
			wg.Add(2)
			go func(pC chan<- *raft.Proposal, cC <-chan *raft.Commit, eC <-chan error) {
				defer wg.Done()
				for range 100 {
					select {
					case <-ctx.Done():
						return
					case c, ok := <-cC:
						if !ok {
							pC = nil
						}
						var data []byte
						if c != nil {
							data = c.Data[0]
						}
						// t.Logf(
						// 	"Sending proposal for node: %s shard: %s",
						// 	node.nodeID,
						// 	group.shardID,
						// )
						select {
						case pC <- &raft.Proposal{Data: data}:
							continue
						case err := <-eC:
							t.Errorf("eC message (%v)", err)
						}
					}
				}
				select {
				case <-ctx.Done():
					return
				case donec <- struct{}{}:
				}
				for range cC { //revive:disable-line:empty-block
					// acknowledge the commits from other nodes so
					// raft continues to make progress
				}
			}(group.proposeC, group.commitC, group.errorC)

			go func(proposeC chan<- *raft.Proposal) {
				defer wg.Done()
				proposeC <- &raft.Proposal{Data: []byte("foo")}
			}(group.proposeC)
		}
	}

	for _, node := range clus.nodes {
		for range node.shards {
			select {
			case <-donec:
				t.Logf("donec received for %s", node.nodeID)
			case <-ctx.Done():
				require.NoError(t, ctx.Err(), "timeout waiting for donec")
			}
		}
	}
	cancel()
}
