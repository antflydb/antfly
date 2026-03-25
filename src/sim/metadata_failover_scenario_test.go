package sim

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
)

func TestHarness_MetadataLeaderFailover_AllowsSubsequentMetadataWrites(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_300_000, 0).UTC(),
		MetadataIDs:       []types.ID{100, 101, 102},
		StoreIDs:          []types.ID{1, 2, 3},
		ReplicationFactor: 3,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	initialLeader, err := h.WaitForMetadataLeader(20 * time.Second)
	require.NoError(t, err)

	_, err = h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 1,
		StartID:   0x2400,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")
	require.NoError(t, h.WriteKey("docs", "k/docs/0", []byte(`{"doc":"before-failover"}`)))

	require.NoError(t, h.CrashMetadataNode(initialLeader))

	newLeader, err := h.WaitForMetadataLeader(30 * time.Second)
	require.NoError(t, err)
	require.NotEqual(t, initialLeader, newLeader)

	_, err = h.CreateTable("events", tablemgr.TableConfig{
		NumShards: 1,
		StartID:   0x2500,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "events")
	require.NoError(t, h.WriteKey("events", "k/events/0", []byte(`{"event":"after-failover"}`)))

	require.NoError(t, h.WaitFor(30*time.Second, func() error {
		before, err := h.LookupKey("docs", "k/docs/0")
		if err != nil {
			return err
		}
		if len(before) == 0 {
			return fmt.Errorf("missing value in table docs")
		}

		after, err := h.LookupKey("events", "k/events/0")
		if err != nil {
			return err
		}
		if len(after) == 0 {
			return fmt.Errorf("missing value in table events")
		}
		return nil
	}))

	require.Contains(t, h.Trace().CompactTrace(64, 0), "[metadata_crash]")
}

func TestHarness_MetadataLeaderFailover_DuringSplitReconciliation(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:                  t.TempDir(),
		Start:                    time.Unix(1_700_310_000, 0).UTC(),
		MetadataIDs:              []types.ID{100, 101, 102},
		StoreIDs:                 []types.ID{1, 2, 3},
		ReplicationFactor:        3,
		MaxShardSizeBytes:        2500,
		MaxShardsPerTable:        2,
		SplitTimeout:             30 * time.Second,
		SplitFinalizeGracePeriod: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 1,
		StartID:   0x2600,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")

	parentShardID := types.ID(0)
	for shardID := range table.Shards {
		parentShardID = shardID
	}
	require.NotZero(t, parentShardID)

	keys := []string{"0/docs/00", "z/docs/30"}
	for i, key := range keys {
		payload := fmt.Appendf(nil, `{"id":%d,"payload":"%s"}`, i, strings.Repeat("f", 1400))
		require.NoError(t, h.Write(parentShardID, key, payload))
		h.expectedKeys[key] = "docs"
	}
	require.NoError(t, h.Advance(2*time.Second))
	require.NoError(t, h.TableManager().EnqueueReallocationRequest(context.Background()))

	initialLeader, err := h.WaitForMetadataLeader(20 * time.Second)
	require.NoError(t, err)

	var childShardID types.ID
	err = h.WaitFor(60*time.Second, func() error {
		if err := h.TableManager().EnqueueReallocationRequest(context.Background()); err != nil {
			return err
		}
		if err := h.ReconcileOnce(context.Background()); err != nil {
			return err
		}
		table, err := h.GetTable("docs")
		if err != nil {
			return err
		}
		parentStatus, err := h.GetShardStatus(parentShardID)
		if err != nil {
			return err
		}
		if parentStatus.SplitState == nil {
			return fmt.Errorf("split not active yet")
		}
		for shardID := range table.Shards {
			if shardID != parentShardID {
				childShardID = shardID
				break
			}
		}
		return nil
	})
	require.NoErrorf(t, err, "split never became active before failover\n%s", h.Trace().CompactTrace(128, 16))

	h.PartitionMetadataNode(initialLeader)
	require.NoError(t, h.WaitFor(30*time.Second, func() error {
		leaderID, err := h.WaitForMetadataLeader(5 * time.Second)
		if err != nil {
			return err
		}
		if leaderID == initialLeader {
			return fmt.Errorf("metadata leader has not changed yet")
		}
		return nil
	}))
	h.HealMetadataNode(initialLeader)

	err = h.WaitFor(30*time.Second, func() error {
		if err := h.TableManager().EnqueueReallocationRequest(context.Background()); err != nil {
			return err
		}
		if err := h.ReconcileOnce(context.Background()); err != nil {
			return err
		}
		table, err := h.GetTable("docs")
		if err != nil {
			return err
		}
		parentStatus, err := h.GetShardStatus(parentShardID)
		if err != nil {
			return err
		}
		if len(table.Shards) > 1 && childShardID == 0 {
			for shardID := range table.Shards {
				if shardID != parentShardID {
					childShardID = shardID
					break
				}
			}
		}
		if childShardID != 0 {
			childStatus, err := h.GetShardStatus(childShardID)
			if err != nil {
				return err
			}
			if childStatus.Table != "docs" {
				return fmt.Errorf("child shard %s belongs to table %s", childShardID, childStatus.Table)
			}
		}
		if parentStatus.State != storedb.ShardState_Default && parentStatus.State != storedb.ShardState_Splitting {
			return fmt.Errorf("parent shard state is %s", parentStatus.State)
		}
		return nil
	})
	require.NoErrorf(t, err, "split reconciliation did not continue after metadata failover\n%s", h.Trace().CompactTrace(160, 20))

	for _, key := range []string{keys[1], keys[len(keys)-2]} {
		value, err := h.LookupKey("docs", key)
		require.NoError(t, err)
		require.NotEmpty(t, value)
	}

	checker := NewChecker(CheckerConfig{SplitLivenessTimeout: 45 * time.Second})
	require.NoError(t, h.WaitFor(15*time.Second, func() error {
		return checker.Check(context.Background(), h)
	}))
	trace := h.Trace().CompactTrace(128, 16)
	require.Contains(t, trace, "[metadata_partition]")
	require.Contains(t, trace, "[metadata_heal]")
	require.Contains(t, trace, "[split]")
}
