package sim

import (
	"context"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/stretchr/testify/require"
)

func TestHarness_SingleShardConvergesAfterCrashRestartAndPartition(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_000_000, 0).UTC(),
		MetadataID:        100,
		StoreIDs:          []types.ID{1, 2, 3},
		ReplicationFactor: 3,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	shardID, err := h.CreateSingleShardTable("docs", 0x1000)
	require.NoError(t, err)
	require.NoError(t, h.StartShardOnAllStores(shardID))

	leaderID, err := h.WaitForLeader(shardID, 20*time.Second)
	require.NoError(t, err)

	require.NoError(t, h.Write(shardID, "doc/1", []byte(`{"value":"one"}`)))
	require.NoError(t, h.Advance(2*time.Second))

	crashedFollower := types.ID(0)
	for _, storeID := range h.storeOrder {
		if storeID != leaderID {
			crashedFollower = storeID
			break
		}
	}
	require.NotZero(t, crashedFollower)

	require.NoError(t, h.CrashStore(crashedFollower))
	require.NoError(t, h.Advance(5*time.Second))

	currentLeaderID, err := h.WaitForLeader(shardID, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, leaderID, currentLeaderID)

	require.NoError(t, h.Write(shardID, "doc/2", []byte(`{"value":"two"}`)))
	require.NoError(t, h.Advance(2*time.Second))

	require.NoError(t, h.RestartStore(crashedFollower))
	require.NoError(t, h.Advance(10*time.Second))
	require.NoError(t, h.RefreshStatuses(context.Background()))

	partitionedFollower := types.ID(0)
	for _, storeID := range h.storeOrder {
		if storeID != currentLeaderID && storeID != crashedFollower {
			partitionedFollower = storeID
			break
		}
	}
	require.NotZero(t, partitionedFollower)

	h.PartitionStore(partitionedFollower)
	require.NoError(t, h.Advance(3*time.Second))

	h.HealStore(partitionedFollower)
	require.NoError(t, h.Advance(10*time.Second))

	require.NoError(t, h.CheckConverged(shardID))
}
