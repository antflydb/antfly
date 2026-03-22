package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
)

func TestHarness_EmptyShardMerge_ReconcilesToOneServingShard(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_320_000, 0).UTC(),
		MetadataID:        100,
		StoreIDs:          []types.ID{1, 2, 3},
		ReplicationFactor: 3,
		MaxShardSizeBytes: 64 * 1024 * 1024,
		MinShardSizeBytes: 32 * 1024 * 1024,
		MinShardsPerTable: 1,
		MaxShardsPerTable: 4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 2,
		StartID:   0x2800,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")

	shardIDs := orderedShardIDsByRange(table)
	require.Len(t, shardIDs, 2)
	leftShardID := shardIDs[0]
	rightShardID := shardIDs[1]

	keys := []string{"0/docs/00", "1/docs/10", "9/docs/20"}
	for i, key := range keys {
		payload := fmt.Appendf(nil, `{"id":%d,"payload":"%s"}`, i, strings.Repeat("m", 128))
		require.NoError(t, h.WriteKey("docs", key, payload))
	}
	require.NoError(t, h.Advance(2*time.Second))
	require.NoError(t, h.Advance(6*time.Minute))

	err = h.WaitFor(90*time.Second, func() error {
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
		if len(table.Shards) != 1 {
			return fmt.Errorf("table still has %d shards", len(table.Shards))
		}
		if _, ok := table.Shards[leftShardID]; !ok {
			return fmt.Errorf("expected left shard %s to survive merge", leftShardID)
		}
		if _, ok := table.Shards[rightShardID]; ok {
			return fmt.Errorf("empty right shard %s still present in table metadata", rightShardID)
		}

		leftStatus, err := h.GetShardStatus(leftShardID)
		if err != nil {
			return err
		}
		if leftStatus.State != storedb.ShardState_Default {
			return fmt.Errorf("surviving shard %s still in state %s", leftShardID, leftStatus.State)
		}
		if _, err := h.GetShardStatus(rightShardID); !errors.Is(err, tablemgr.ErrNotFound) {
			return fmt.Errorf("expected donor shard %s to be removed, got err=%v", rightShardID, err)
		}
		for _, key := range keys {
			value, err := h.LookupKey("docs", key)
			if err != nil {
				return err
			}
			if len(value) == 0 {
				return fmt.Errorf("lookup for %q returned no value", key)
			}
		}
		return nil
	})
	require.NoErrorf(
		t,
		err,
		"merge did not converge\n%s\n%s",
		describeTableAndShardStates(t, h, "docs"),
		h.Trace().CompactTrace(128, 16),
	)

	err = h.WaitFor(20*time.Second, func() error {
		for _, storeID := range []types.ID{1, 2, 3} {
			results, err := h.LookupFromStore(storeID, leftShardID, keys)
			if err != nil {
				return err
			}
			if len(results) != len(keys) {
				return fmt.Errorf(
					"store %s shard %s returned %d docs, expected %d",
					storeID,
					leftShardID,
					len(results),
					len(keys),
				)
			}
		}
		return nil
	})
	require.NoErrorf(
		t,
		err,
		"merged shard data did not converge\n%s\n%s",
		describeTableAndShardStates(t, h, "docs"),
		h.Trace().CompactTrace(128, 16),
	)

	checker := NewChecker(CheckerConfig{SplitLivenessTimeout: 45 * time.Second})
	require.NoError(t, checker.CheckStable(context.Background(), h))
}

func TestHarness_MetadataLeaderFailover_ThenEmptyShardMergeConverges(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_330_000, 0).UTC(),
		MetadataIDs:       []types.ID{110, 111, 112},
		StoreIDs:          []types.ID{11, 12, 13},
		ReplicationFactor: 3,
		MaxShardSizeBytes: 64 * 1024 * 1024,
		MinShardSizeBytes: 32 * 1024 * 1024,
		MinShardsPerTable: 1,
		MaxShardsPerTable: 4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 2,
		StartID:   0x3900,
	})
	require.NoError(t, err)
	startTableOnAllStoresWithLeaderTimeout(t, h, "docs", 90*time.Second)

	shardIDs := orderedShardIDsByRange(table)
	require.Len(t, shardIDs, 2)
	leftShardID := shardIDs[0]
	rightShardID := shardIDs[1]

	key := "0/docs/00"
	value := []byte(`{"doc":"survives-failover-merge"}`)
	require.NoError(t, writeToShardWithLeaderTimeout(h, leftShardID, key, value, 90*time.Second))
	h.TrackExpectedDoc("docs", key, value)
	require.NoError(t, h.Advance(2*time.Second))

	require.NoError(t, h.WaitFor(20*time.Second, func() error {
		node, err := h.metadataLeaderNode()
		if err != nil {
			return err
		}
		leftStatus, err := node.tm.GetShardStatus(leftShardID)
		if err != nil {
			return err
		}
		rightStatus, err := node.tm.GetShardStatus(rightShardID)
		if err != nil {
			return err
		}
		if leftStatus.ShardStats == nil || leftStatus.ShardStats.Storage == nil || leftStatus.ShardStats.Storage.Empty {
			return fmt.Errorf("left shard %s still appears empty on metadata leader", leftShardID)
		}
		if rightStatus.ShardStats == nil || rightStatus.ShardStats.Storage == nil || !rightStatus.ShardStats.Storage.Empty {
			return fmt.Errorf("right shard %s no longer appears empty on metadata leader", rightShardID)
		}
		return nil
	}))

	initialLeader, err := h.WaitForMetadataLeader(20 * time.Second)
	require.NoError(t, err)

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

	require.NoError(t, h.WaitFor(20*time.Second, func() error {
		node, err := h.metadataLeaderNode()
		if err != nil {
			return err
		}
		leftStatus, err := node.tm.GetShardStatus(leftShardID)
		if err != nil {
			return err
		}
		if leftStatus.ShardStats == nil || leftStatus.ShardStats.Storage == nil || leftStatus.ShardStats.Storage.Empty {
			return fmt.Errorf("post-failover metadata leader still sees left shard %s as empty", leftShardID)
		}
		return nil
	}))

	require.NoError(t, h.Advance(6*time.Minute))

	err = h.WaitFor(90*time.Second, func() error {
		table, err := h.GetTable("docs")
		if err != nil {
			return err
		}
		if len(table.Shards) != 1 {
			return fmt.Errorf("table still has %d shards", len(table.Shards))
		}
		if _, ok := table.Shards[leftShardID]; !ok {
			return fmt.Errorf("expected left shard %s to survive merge after failover", leftShardID)
		}
		if _, ok := table.Shards[rightShardID]; ok {
			return fmt.Errorf("empty right shard %s still present in table metadata", rightShardID)
		}
		value, err := h.LookupKey("docs", key)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			return fmt.Errorf("lookup for %q returned no value", key)
		}
		return nil
	})
	require.NoErrorf(
		t,
		err,
		"merge did not converge after metadata failover\n%s\n%s",
		describeTableAndShardStates(t, h, "docs"),
		h.Trace().CompactTrace(160, 20),
	)

	checker := NewChecker(CheckerConfig{SplitLivenessTimeout: 45 * time.Second})
	require.NoError(t, checker.CheckStable(context.Background(), h))

	trace := h.Trace().CompactTrace(128, 16)
	require.Contains(t, trace, "[metadata_partition]")
	require.Contains(t, trace, "[metadata_heal]")
}

func TestHarness_NonEmptyShardMerge_ReconcilesToOneServingShard(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_340_000, 0).UTC(),
		MetadataID:        120,
		StoreIDs:          []types.ID{21, 22, 23},
		ReplicationFactor: 3,
		MaxShardSizeBytes: 64 * 1024 * 1024,
		MinShardSizeBytes: 32 * 1024 * 1024,
		MinShardsPerTable: 1,
		MaxShardsPerTable: 4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 2,
		StartID:   0x4800,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")

	shardIDs := orderedShardIDsByRange(table)
	require.Len(t, shardIDs, 2)
	leftShardID := shardIDs[0]
	rightShardID := shardIDs[1]

	leftKey := "0/docs/00"
	rightKey := rawShardKey(0x90, "/docs/90")
	require.NoError(t, h.WriteKey("docs", leftKey, []byte(`{"doc":"left"}`)))
	require.NoError(t, h.WriteKey("docs", rightKey, []byte(`{"doc":"right"}`)))
	require.NoError(t, h.Advance(2*time.Second))
	waitForNonEmptyShardPair(t, h, leftShardID, rightShardID, 20*time.Second)
	require.NoError(t, h.Advance(6*time.Minute))

	waitForMergedTable(t, h, "docs", leftShardID, rightShardID, 90*time.Second, leftKey, rightKey)

	checker := NewChecker(CheckerConfig{
		SplitLivenessTimeout: 45 * time.Second,
		MergeLivenessTimeout: 45 * time.Second,
	})
	require.NoError(t, checker.CheckStable(context.Background(), h))
}

func TestHarness_NonEmptyShardMerge_SurvivesDonorFollowerRestart(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_350_000, 0).UTC(),
		MetadataID:        130,
		StoreIDs:          []types.ID{31, 32, 33},
		ReplicationFactor: 3,
		MaxShardSizeBytes: 64 * 1024 * 1024,
		MinShardSizeBytes: 32 * 1024 * 1024,
		MinShardsPerTable: 1,
		MaxShardsPerTable: 4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 2,
		StartID:   0x5800,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")

	shardIDs := orderedShardIDsByRange(table)
	require.Len(t, shardIDs, 2)
	leftShardID := shardIDs[0]
	rightShardID := shardIDs[1]

	leftKey := "0/docs/00"
	rightKeys := []string{
		rawShardKey(0x90, "/docs/90"),
		rawShardKey(0x91, "/docs/91"),
		rawShardKey(0x92, "/docs/92"),
	}
	require.NoError(t, h.WriteKey("docs", leftKey, []byte(`{"doc":"left"}`)))
	for i, key := range rightKeys {
		require.NoError(t, h.WriteKey("docs", key, fmt.Appendf(nil, `{"doc":"right-%d"}`, i)))
	}
	require.NoError(t, h.Advance(2*time.Second))
	waitForNonEmptyShardPair(t, h, leftShardID, rightShardID, 20*time.Second)
	require.NoError(t, h.Advance(6*time.Minute))

	restartDone := false
	err = h.WaitFor(90*time.Second, func() error {
		if err := h.TableManager().EnqueueReallocationRequest(context.Background()); err != nil {
			return err
		}
		if err := h.ReconcileOnce(context.Background()); err != nil {
			return err
		}
		if !restartDone {
			rightStatus, err := h.GetShardStatus(rightShardID)
			if err == nil && rightStatus.MergeState != nil &&
				rightStatus.MergeState.GetPhase() != storedb.MergeState_PHASE_NONE {
				leaderID, err := h.WaitForLeader(rightShardID, 20*time.Second)
				if err != nil {
					return err
				}
				var followerID types.ID
				for peerID := range rightStatus.Peers {
					if peerID != leaderID {
						followerID = peerID
						break
					}
				}
				if followerID == 0 {
					return fmt.Errorf("no donor follower found for shard %s", rightShardID)
				}
				if err := h.CrashStore(followerID); err != nil {
					return err
				}
				if err := h.Advance(2 * time.Second); err != nil {
					return err
				}
				if err := h.RestartStore(followerID); err != nil {
					return err
				}
				restartDone = true
			}
		}
		table, err := h.GetTable("docs")
		if err != nil {
			return err
		}
		if len(table.Shards) != 1 {
			return fmt.Errorf("table still has %d shards", len(table.Shards))
		}
		if _, ok := table.Shards[leftShardID]; !ok {
			return fmt.Errorf("expected left shard %s to survive merge", leftShardID)
		}
		if _, ok := table.Shards[rightShardID]; ok {
			return fmt.Errorf("donor shard %s still present in table metadata", rightShardID)
		}
		if !restartDone {
			return fmt.Errorf("merge converged before donor follower restart occurred")
		}
		keys := append([]string{leftKey}, rightKeys...)
		for _, key := range keys {
			value, err := h.LookupKey("docs", key)
			if err != nil {
				return err
			}
			if len(value) == 0 {
				return fmt.Errorf("lookup for %q returned no value", key)
			}
		}
		return nil
	})
	require.NoErrorf(
		t,
		err,
		"non-empty merge did not converge after donor follower restart\n%s\n%s",
		describeTableAndShardStates(t, h, "docs"),
		h.Trace().CompactTrace(160, 20),
	)

	checker := NewChecker(CheckerConfig{
		SplitLivenessTimeout: 45 * time.Second,
		MergeLivenessTimeout: 45 * time.Second,
	})
	require.NoError(t, checker.CheckStable(context.Background(), h))
}

func TestHarness_MetadataLeaderFailover_DuringNonEmptyMergeConverges(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_360_000, 0).UTC(),
		MetadataIDs:       []types.ID{140, 141, 142},
		StoreIDs:          []types.ID{41, 42, 43},
		ReplicationFactor: 3,
		MaxShardSizeBytes: 64 * 1024 * 1024,
		MinShardSizeBytes: 32 * 1024 * 1024,
		MinShardsPerTable: 1,
		MaxShardsPerTable: 4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 2,
		StartID:   0x6800,
	})
	require.NoError(t, err)
	startTableOnAllStoresWithLeaderTimeout(t, h, "docs", 90*time.Second)

	shardIDs := orderedShardIDsByRange(table)
	require.Len(t, shardIDs, 2)
	leftShardID := shardIDs[0]
	rightShardID := shardIDs[1]

	leftKey := "0/docs/00"
	rightKey := rawShardKey(0x90, "/docs/90")
	require.NoError(t, h.WriteKey("docs", leftKey, []byte(`{"doc":"left"}`)))
	require.NoError(t, h.WriteKey("docs", rightKey, []byte(`{"doc":"right"}`)))
	require.NoError(t, h.Advance(2*time.Second))
	waitForNonEmptyShardPair(t, h, leftShardID, rightShardID, 20*time.Second)

	initialLeader, err := h.WaitForMetadataLeader(20 * time.Second)
	require.NoError(t, err)
	failoverDone := false

	h.AddBeforeRPCFault(1, func(event RPCEvent) bool {
		return event.Method == "FinalizeMerge" && event.ShardID == leftShardID
	}, func(_ context.Context, _ RPCEvent) error {
		h.PartitionMetadataNode(initialLeader)
		if err := h.WaitFor(30*time.Second, func() error {
			leaderID, err := h.WaitForMetadataLeader(5 * time.Second)
			if err != nil {
				return err
			}
			if leaderID == initialLeader {
				return fmt.Errorf("metadata leader has not changed yet")
			}
			return nil
		}); err != nil {
			return err
		}
		h.HealMetadataNode(initialLeader)
		failoverDone = true
		return nil
	})

	require.NoError(t, h.Advance(6*time.Minute))

	err = h.WaitFor(90*time.Second, func() error {
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
		if len(table.Shards) != 1 {
			return fmt.Errorf("table still has %d shards", len(table.Shards))
		}
		if _, ok := table.Shards[leftShardID]; !ok {
			return fmt.Errorf("expected left shard %s to survive merge after failover", leftShardID)
		}
		if _, ok := table.Shards[rightShardID]; ok {
			return fmt.Errorf("donor shard %s still present after failover merge", rightShardID)
		}
		if !failoverDone {
			return fmt.Errorf("merge converged before metadata failover occurred")
		}
		for _, key := range []string{leftKey, rightKey} {
			value, err := h.LookupKey("docs", key)
			if err != nil {
				return err
			}
			if len(value) == 0 {
				return fmt.Errorf("lookup for %q returned no value", key)
			}
		}
		return nil
	})
	require.NoErrorf(
		t,
		err,
		"non-empty merge did not converge after metadata failover\n%s\n%s",
		describeTableAndShardStates(t, h, "docs"),
		h.Trace().CompactTrace(160, 20),
	)

	trace := h.Trace().CompactTrace(160, 20)
	require.Contains(t, trace, "[metadata_partition]")
	require.Contains(t, trace, "[metadata_heal]")
}

func TestHarness_NonEmptyShardMerge_RollbackRestoresOriginalShards(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:                  t.TempDir(),
		Start:                    time.Unix(1_700_370_000, 0).UTC(),
		MetadataID:               150,
		StoreIDs:                 []types.ID{51, 52, 53},
		ReplicationFactor:        3,
		MaxShardSizeBytes:        64 * 1024 * 1024,
		MinShardSizeBytes:        32 * 1024 * 1024,
		MinShardsPerTable:        1,
		MaxShardsPerTable:        4,
		SplitTimeout:             4 * time.Second,
		SplitFinalizeGracePeriod: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 2,
		StartID:   0x7800,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")

	shardIDs := orderedShardIDsByRange(table)
	require.Len(t, shardIDs, 2)
	leftShardID := shardIDs[0]
	rightShardID := shardIDs[1]

	leftKey := "0/docs/00"
	rightKey := rawShardKey(0x90, "/docs/90")
	require.NoError(t, h.WriteKey("docs", leftKey, []byte(`{"doc":"left"}`)))
	require.NoError(t, h.WriteKey("docs", rightKey, []byte(`{"doc":"right"}`)))
	require.NoError(t, h.Advance(2*time.Second))
	waitForNonEmptyShardPair(t, h, leftShardID, rightShardID, 20*time.Second)
	require.NoError(t, h.Advance(6*time.Minute))

	h.AddBeforeRPCFault(100, func(event RPCEvent) bool {
		return event.Method == "FinalizeMerge" && event.ShardID == leftShardID
	}, func(_ context.Context, _ RPCEvent) error {
		return fmt.Errorf("injected finalize merge failure")
	})

	err = h.WaitFor(45*time.Second, func() error {
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
		if len(table.Shards) != 2 {
			return fmt.Errorf("expected rollback to keep two shards, got %d", len(table.Shards))
		}
		leftStatus, err := h.GetShardStatus(leftShardID)
		if err != nil {
			return err
		}
		rightStatus, err := h.GetShardStatus(rightShardID)
		if err != nil {
			return err
		}
		if leftStatus.State != storedb.ShardState_Default || rightStatus.State != storedb.ShardState_Default {
			return fmt.Errorf("shards not restored to default state left=%s right=%s", leftStatus.State, rightStatus.State)
		}
		if leftStatus.MergeState != nil || rightStatus.MergeState != nil {
			return fmt.Errorf("merge state not cleared after rollback")
		}
		for _, key := range []string{leftKey, rightKey} {
			value, err := h.LookupKey("docs", key)
			if err != nil {
				return err
			}
			if len(value) == 0 {
				return fmt.Errorf("lookup for %q returned no value after rollback", key)
			}
		}
		return nil
	})
	require.NoErrorf(
		t,
		err,
		"non-empty merge rollback did not restore original shards\n%s\n%s",
		describeTableAndShardStates(t, h, "docs"),
		h.Trace().CompactTrace(192, 24),
	)
}

func orderedShardIDsByRange(table *store.Table) []types.ID {
	shardIDs := make([]types.ID, 0, len(table.Shards))
	for shardID := range table.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.SortFunc(shardIDs, func(a, b types.ID) int {
		return bytes.Compare(table.Shards[a].ByteRange[0], table.Shards[b].ByteRange[0])
	})
	return shardIDs
}

func rawShardKey(first byte, suffix string) string {
	return string(append([]byte{first}, []byte(suffix)...))
}

func waitForNonEmptyShardPair(
	t *testing.T,
	h *Harness,
	leftShardID, rightShardID types.ID,
	timeout time.Duration,
) {
	t.Helper()

	err := h.WaitFor(timeout, func() error {
		if err := h.RefreshStatuses(context.Background()); err != nil {
			return err
		}
		leftStatus, err := h.GetShardStatus(leftShardID)
		if err != nil {
			return err
		}
		rightStatus, err := h.GetShardStatus(rightShardID)
		if err != nil {
			return err
		}
		if leftStatus.ShardStats == nil || leftStatus.ShardStats.Storage == nil || leftStatus.ShardStats.Storage.Empty {
			return fmt.Errorf("left shard %s still appears empty", leftShardID)
		}
		if rightStatus.ShardStats == nil || rightStatus.ShardStats.Storage == nil || rightStatus.ShardStats.Storage.Empty {
			return fmt.Errorf("right shard %s still appears empty", rightShardID)
		}
		return nil
	})
	require.NoErrorf(t, err, "shard pair never became non-empty\n%s", h.Trace().CompactTrace(128, 16))
}

func waitForMergedTable(
	t *testing.T,
	h *Harness,
	tableName string,
	leftShardID, rightShardID types.ID,
	timeout time.Duration,
	keys ...string,
) {
	t.Helper()

	err := h.WaitFor(timeout, func() error {
		if err := h.TableManager().EnqueueReallocationRequest(context.Background()); err != nil {
			return err
		}
		if err := h.ReconcileOnce(context.Background()); err != nil {
			return err
		}

		table, err := h.GetTable(tableName)
		if err != nil {
			return err
		}
		if len(table.Shards) != 1 {
			return fmt.Errorf("table still has %d shards", len(table.Shards))
		}
		if _, ok := table.Shards[leftShardID]; !ok {
			return fmt.Errorf("expected left shard %s to survive merge", leftShardID)
		}
		if _, ok := table.Shards[rightShardID]; ok {
			return fmt.Errorf("donor shard %s still present in table metadata", rightShardID)
		}
		for _, key := range keys {
			value, err := h.LookupKey(tableName, key)
			if err != nil {
				return err
			}
			if len(value) == 0 {
				return fmt.Errorf("lookup for %q returned no value", key)
			}
		}
		return nil
	})
	require.NoErrorf(
		t,
		err,
		"merge did not converge\n%s\n%s",
		describeTableAndShardStates(t, h, tableName),
		h.Trace().CompactTrace(160, 20),
	)
}

func startTableOnAllStoresWithLeaderTimeout(
	t *testing.T,
	h *Harness,
	tableName string,
	timeout time.Duration,
) *store.Table {
	t.Helper()

	table, err := h.GetTable(tableName)
	require.NoError(t, err)

	shardIDs := orderedShardIDsByRange(table)
	for _, shardID := range shardIDs {
		require.NoError(t, h.StartShardOnAllStores(shardID))
	}

	for _, shardID := range shardIDs {
		_, err = h.WaitForLeader(shardID, timeout)
		require.NoError(t, err)
	}

	table, err = h.GetTable(tableName)
	require.NoError(t, err)
	return table
}

func writeToShardWithLeaderTimeout(
	h *Harness,
	shardID types.ID,
	key string,
	value []byte,
	leaderTimeout time.Duration,
) error {
	return h.WaitFor(leaderTimeout, func() error {
		return h.Write(shardID, key, value)
	})
}

func describeTableAndShardStates(t *testing.T, h *Harness, tableName string) string {
	t.Helper()

	table, err := h.GetTable(tableName)
	if err != nil {
		return fmt.Sprintf("table %s: load error: %v", tableName, err)
	}

	parts := []string{fmt.Sprintf("table %s shards=%d", tableName, len(table.Shards))}
	for _, shardID := range orderedShardIDsByRange(table) {
		status, err := h.GetShardStatus(shardID)
		if err != nil {
			parts = append(parts, fmt.Sprintf("  shard=%s status error=%v", shardID, err))
			continue
		}

		empty := false
		size := uint64(0)
		created := "<nil>"
		updated := "<nil>"
		if status.ShardStats != nil {
			created = status.ShardStats.Created.Format(time.RFC3339Nano)
			updated = status.ShardStats.Updated.Format(time.RFC3339Nano)
			if status.ShardStats.Storage != nil {
				empty = status.ShardStats.Storage.Empty
				size = status.ShardStats.Storage.DiskSize
			}
		}

		lead := types.ID(0)
		if status.RaftStatus != nil {
			lead = status.RaftStatus.Lead
		}

		parts = append(parts, fmt.Sprintf(
			"  shard=%s range=%s state=%s empty=%t size=%d created=%s updated=%s lead=%d",
			shardID,
			table.Shards[shardID].ByteRange,
			status.State,
			empty,
			size,
			created,
			updated,
			lead,
		))
	}

	return strings.Join(parts, "\n")
}
