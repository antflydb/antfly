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

func TestHarness_SplitLifecycle_ReconcilesToTwoServingShards(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:                  t.TempDir(),
		Start:                    time.Unix(1_700_100_000, 0).UTC(),
		MetadataID:               100,
		StoreIDs:                 []types.ID{1, 2, 3},
		ReplicationFactor:        3,
		MaxShardSizeBytes:        256,
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
		StartID:   0x2000,
	})
	require.NoError(t, err)

	startTableOnAllStores(t, h, "docs")

	originalShardID := types.ID(0)
	for shardID := range table.Shards {
		originalShardID = shardID
	}
	require.NotZero(t, originalShardID)

	keys := []string{
		"0/docs/00",
		"0/docs/10",
		"1/docs/20",
		"2/docs/30",
		"a/docs/00",
		"b/docs/10",
		"y/docs/20",
		"z/docs/30",
	}
	for i, key := range keys {
		payload := fmt.Appendf(nil, `{"id":%d,"payload":"%s"}`, i, strings.Repeat("x", 96))
		require.NoError(t, h.WriteKey("docs", key, payload))
	}
	require.NoError(t, h.Advance(2*time.Second))

	require.NoError(t, h.TableManager().EnqueueReallocationRequest(context.Background()))

	lowKey := keys[1]
	highKey := keys[len(keys)-2]
	var childShardID types.ID

	err = h.WaitFor(5*time.Minute, func() error {
		if err := h.ReconcileOnce(context.Background()); err != nil {
			return err
		}

		table, err := h.GetTable("docs")
		if err != nil {
			return err
		}
		if len(table.Shards) != 2 {
			return fmt.Errorf("table still has %d shards", len(table.Shards))
		}

		parentStatus, err := h.GetShardStatus(originalShardID)
		if err != nil {
			return err
		}

		childShardID = types.ID(0)
		for shardID := range table.Shards {
			if shardID != originalShardID {
				childShardID = shardID
				break
			}
		}
		if childShardID == 0 {
			return fmt.Errorf("split child shard not found")
		}

		childStatus, err := h.GetShardStatus(childShardID)
		if err != nil {
			return err
		}
		if parentStatus.SplitState != nil {
			return fmt.Errorf(
				"parent shard %s split state still active: phase=%s child=%s childState=%s childHasSnapshot=%t childInitializing=%t childReplayRequired=%t childReplayCaughtUp=%t childCutoverReady=%t childLeader=%d",
				originalShardID,
				parentStatus.SplitState.GetPhase(),
				childShardID,
				childStatus.State,
				childStatus.HasSnapshot,
				childStatus.Initializing,
				childStatus.SplitReplayRequired,
				childStatus.SplitReplayCaughtUp,
				childStatus.SplitCutoverReady,
				childStatus.RaftStatus.Lead,
			)
		}

		return nil
	})
	require.NoErrorf(t, err, "split did not reach two-shard lifecycle\n%s", h.Trace().CompactTrace(128, 16))

	err = h.WaitFor(90*time.Second, func() error {
		if err := h.ReconcileOnce(context.Background()); err != nil {
			return err
		}
		parentStatus, err := h.GetShardStatus(originalShardID)
		if err != nil {
			return err
		}
		childStatus, err := h.GetShardStatus(childShardID)
		if err != nil {
			return err
		}
		if childStatus.State != storedb.ShardState_Default {
			return fmt.Errorf("child shard %s has unexpected state %s", childShardID, childStatus.State)
		}
		if !childStatus.IsReadyForSplitReads() {
			return fmt.Errorf(
				"child shard %s is not ready for split reads: hasSnapshot=%t initializing=%t splitReplayRequired=%t splitReplayCaughtUp=%t splitCutoverReady=%t lead=%d",
				childShardID,
				childStatus.HasSnapshot,
				childStatus.Initializing,
				childStatus.SplitReplayRequired,
				childStatus.SplitReplayCaughtUp,
				childStatus.SplitCutoverReady,
				childStatus.RaftStatus.Lead,
			)
		}
		if parentStatus.State != storedb.ShardState_Default {
			return fmt.Errorf("parent shard %s still in state %s", originalShardID, parentStatus.State)
		}
		if _, err := h.WaitForLeader(originalShardID, 3*time.Second); err != nil {
			return err
		}

		lowValue, err := h.LookupKey("docs", lowKey)
		if err != nil {
			return err
		}
		if len(lowValue) == 0 {
			return fmt.Errorf("lookup for %q returned no value", lowKey)
		}

		highValue, err := h.LookupKey("docs", highKey)
		if err != nil {
			return err
		}
		if len(highValue) == 0 {
			return fmt.Errorf("lookup for %q returned no value", highKey)
		}

		return nil
	})
	require.NoErrorf(t, err, "split did not finalize\n%s", h.Trace().CompactTrace(128, 16))

	finalTable, err := h.GetTable("docs")
	require.NoError(t, err)

	keysByShard := map[types.ID][]string{
		originalShardID: {},
		childShardID:    {},
	}
	for _, key := range keys {
		shardID, err := finalTable.FindShardForKey(key)
		require.NoError(t, err)
		keysByShard[shardID] = append(keysByShard[shardID], key)
	}

	err = h.WaitFor(20*time.Second, func() error {
		for _, storeID := range []types.ID{1, 2, 3} {
			for shardID, shardKeys := range keysByShard {
				results, err := h.LookupFromStore(storeID, shardID, shardKeys)
				if err != nil {
					return err
				}
				if len(results) != len(shardKeys) {
					return fmt.Errorf(
						"store %s shard %s returned %d docs, expected %d",
						storeID,
						shardID,
						len(results),
						len(shardKeys),
					)
				}
				for _, key := range shardKeys {
					if _, ok := results[key]; !ok {
						return fmt.Errorf("store %s shard %s missing key %q", storeID, shardID, key)
					}
				}
			}
		}
		return nil
	})
	require.NoErrorf(t, err, "split data ownership did not converge\n%s", h.Trace().CompactTrace(128, 16))

	checker := NewChecker(CheckerConfig{SplitLivenessTimeout: 45 * time.Second})
	require.NoError(t, checker.CheckStable(context.Background(), h))
}
