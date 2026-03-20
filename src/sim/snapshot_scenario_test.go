package sim

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	afraft "github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
)

func TestHarness_FollowerSnapshotTransferRecoversAfterLinkHeal(t *testing.T) {
	withAggressiveSnapshots(t)

	h, shardID, leaderID, followerID := newSnapshotHarness(t, "snapshot-cut")

	// Identify the third store (the other follower).
	var otherID types.ID
	for _, id := range []types.ID{1, 2, 3} {
		if id != leaderID && id != followerID {
			otherID = id
			break
		}
	}

	// We cut links from ALL peers to the target follower so that a
	// leader-change after restart cannot bypass the partition.
	leaderTarget := transportFaultTarget{
		ShardID: shardID,
		Link:    transportLink{From: leaderID, To: followerID},
		Class:   RaftMessageClassSnapshot,
	}
	otherTarget := transportFaultTarget{
		ShardID: shardID,
		Link:    transportLink{From: otherID, To: followerID},
		Class:   RaftMessageClassSnapshot,
	}

	require.NoError(t, h.CrashStore(followerID))
	writeSnapshotDocs(t, h, 0, 64)
	h.CutTransportLink(leaderTarget)
	h.CutTransportLink(otherTarget)
	require.NoError(t, h.RestartStore(followerID))
	require.True(t, hasTransportFaultActionEvent(h.Trace().Events(), ActionCutLink))
	require.Error(t, h.WaitForConvergence(shardID, 5*time.Second))

	h.HealTransportLink(leaderTarget)
	h.HealTransportLink(otherTarget)
	require.NoError(t, h.WaitForConvergence(shardID, 60*time.Second))
	require.NoError(t, NewChecker(CheckerConfig{SplitLivenessTimeout: 45 * time.Second}).CheckStable(context.Background(), h))
}

func newSnapshotHarness(t *testing.T, name string) (*Harness, types.ID, types.ID, types.ID) {
	t.Helper()

	h, err := NewHarness(HarnessConfig{
		BaseDir:           filepath.Join(t.TempDir(), name),
		Start:             time.Unix(1_701_100_000, 0).UTC(),
		MetadataID:        100,
		StoreIDs:          []types.ID{1, 2, 3},
		ReplicationFactor: 3,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 1,
		StartID:   0x5000,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")

	shardID := onlyShardID(table)

	leaderID, err := h.WaitForLeader(shardID, 30*time.Second)
	require.NoError(t, err)

	followerID := types.ID(0)
	for _, storeID := range []types.ID{1, 2, 3} {
		if storeID != leaderID {
			followerID = storeID
			break
		}
	}
	require.NotZero(t, followerID)

	return h, shardID, leaderID, followerID
}

func writeSnapshotDocs(t *testing.T, h *Harness, start, count int) {
	t.Helper()

	for i := range count {
		key := fmt.Sprintf("%x/docs/%02d", i%16, start+i)
		value := fmt.Appendf(nil, `{"id":%d,"payload":"%s"}`, start+i, strings.Repeat("s", 192))
		require.NoError(t, h.WriteKey("docs", key, value))
	}
	require.NoError(t, h.Advance(5*time.Second))
}

func onlyShardID(table *store.Table) types.ID {
	shardIDs := make([]types.ID, 0, len(table.Shards))
	for shardID := range table.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)
	if len(shardIDs) == 0 {
		return 0
	}
	return shardIDs[0]
}

func hasSnapshotTransferEvent(
	events []TraceEvent,
	kind string,
	shardID, from, to types.ID,
) bool {
	wantKind := "kind=" + kind
	wantShard := "shard=" + shardID.String()
	wantFrom := "from=" + from.String()
	wantTo := "to=" + to.String()
	for _, event := range events {
		if event.Kind != "snapshot_transfer" {
			continue
		}
		if strings.Contains(event.Message, wantKind) &&
			strings.Contains(event.Message, wantShard) &&
			strings.Contains(event.Message, wantFrom) &&
			strings.Contains(event.Message, wantTo) {
			return true
		}
	}
	return false
}

func hasTransportFaultActionEvent(events []TraceEvent, action ScenarioAction) bool {
	wantAction := "action=" + string(action)
	for _, event := range events {
		if event.Kind != "transport_fault" {
			continue
		}
		if strings.Contains(event.Message, wantAction) {
			return true
		}
	}
	return false
}

func withAggressiveSnapshots(t *testing.T) {
	t.Helper()

	prevDefault := afraft.DefaultSnapshotCount
	prevCatchUp := afraft.SnapshotCatchUpEntriesN
	afraft.DefaultSnapshotCount = 4
	afraft.SnapshotCatchUpEntriesN = 4
	t.Cleanup(func() {
		afraft.DefaultSnapshotCount = prevDefault
		afraft.SnapshotCatchUpEntriesN = prevCatchUp
	})
}
