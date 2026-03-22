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

package metadata

import (
	"bytes"
	"fmt"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	db "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
)

func isActiveSplitParentStatus(status *store.ShardStatus) bool {
	if status == nil {
		return false
	}
	if status.SplitState == nil {
		return false
	}
	switch status.SplitState.GetPhase() {
	case db.SplitState_PHASE_PREPARE, db.SplitState_PHASE_SPLITTING:
		return true
	default:
		return false
	}
}

func shouldRouteWriteToParent(status *store.ShardStatus) bool {
	switch status.State {
	case store.ShardState_SplittingOff, store.ShardState_SplitOffPreSnap:
		return !status.CanInitiateSplitCutover()
	default:
		return false
	}
}

func findParentShardForSplitOffStatus(
	allStatuses map[types.ID]*store.ShardStatus,
	splitOffStatus *store.ShardStatus,
) (types.ID, error) {
	splitKey := splitOffStatus.ByteRange[0]
	for shardID, status := range allStatuses {
		if status.Table != splitOffStatus.Table {
			continue
		}
		if isActiveSplitParentStatus(status) && bytes.Equal(status.ByteRange[1], splitKey) {
			return shardID, nil
		}
	}
	return 0, fmt.Errorf("parent shard not found for split-off shard: %w", client.ErrNotFound)
}

func resolveWriteShardID(
	shardStatuses map[types.ID]*store.ShardStatus,
	shardID types.ID,
) (types.ID, error) {
	status, ok := shardStatuses[shardID]
	if !ok || status == nil {
		return 0, fmt.Errorf("no status info available for shard %s", shardID)
	}
	if status.MergeState != nil &&
		status.MergeState.GetPhase() == db.MergeState_PHASE_FINALIZING &&
		status.MergeState.GetReceiverShardId() != 0 {
		receiverShardID := types.ID(status.MergeState.GetReceiverShardId())
		if receiverStatus, ok := shardStatuses[receiverShardID]; ok &&
			receiverStatus != nil &&
			receiverStatus.IsReadyForMergeCutover() {
			return receiverShardID, nil
		}
	}
	parentShardID, err := findParentShardForSplitOffStatus(shardStatuses, status)
	if err == nil {
		return parentShardID, nil
	}
	if status.State == store.ShardState_SplittingOff ||
		status.State == store.ShardState_SplitOffPreSnap {
		if !status.IsReadyForSplitReads() {
			return 0, err
		}
	}
	return shardID, nil
}

func findWriteShardForKey(
	tm *tablemgr.TableManager,
	table *store.Table,
	key string,
) (types.ID, error) {
	shardID, err := table.FindShardForKey(key)
	if err != nil {
		return 0, err
	}
	return resolveWriteShardIDFromTableManager(tm, shardID)
}

func resolveWriteShardIDFromTableManager(tm *tablemgr.TableManager, shardID types.ID) (types.ID, error) {
	shardStatuses, err := tm.GetShardStatuses()
	if err != nil {
		return 0, fmt.Errorf("loading shard statuses: %w", err)
	}
	return resolveWriteShardID(shardStatuses, shardID)
}

func partitionWriteKeysByShard(
	tm *tablemgr.TableManager,
	table *store.Table,
	keys []string,
) (map[types.ID][]string, []string, error) {
	partitions := make(map[types.ID][]string)
	if len(keys) == 0 {
		return partitions, nil, nil
	}

	shardStatuses, err := tm.GetShardStatuses()
	if err != nil {
		return nil, nil, fmt.Errorf("loading shard statuses: %w", err)
	}

	var unfoundKeys []string
	for _, key := range keys {
		shardID, err := table.FindShardForKey(key)
		if err != nil {
			unfoundKeys = append(unfoundKeys, key)
			continue
		}
		writeShardID, err := resolveWriteShardID(shardStatuses, shardID)
		if err != nil {
			return nil, nil, fmt.Errorf("resolving write owner for key %q: %w", key, err)
		}
		partitions[writeShardID] = append(partitions[writeShardID], key)
	}

	return partitions, unfoundKeys, nil
}
