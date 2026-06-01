// Copyright 2026 Antfly, Inc.
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
	"context"
	"encoding/base64"
	"fmt"
	"slices"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	storedb "github.com/antflydb/antfly/go/pkg/antfly/src/store/db"
)

type TxnRecordRef struct {
	StoreID types.ID
	ShardID types.ID
	Record  storedb.TxnRecord
}

type TxnIntentRef struct {
	StoreID types.ID
	ShardID types.ID
	Intent  storedb.TxnIntent
}

type ClusterTxnSnapshot struct {
	Records map[string]TxnRecordRef
	Intents map[string][]TxnIntentRef
}

func (h *Harness) TransactionSnapshot(ctx context.Context, snapshot *ClusterSnapshot) (*ClusterTxnSnapshot, error) {
	if snapshot == nil {
		var err error
		snapshot, err = h.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
	}

	txnSnapshot := &ClusterTxnSnapshot{
		Records: make(map[string]TxnRecordRef),
		Intents: make(map[string][]TxnIntentRef),
	}

	shardIDs := make([]types.ID, 0, len(snapshot.Shards))
	for shardID := range snapshot.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)

	for _, shardID := range shardIDs {
		status := snapshot.Shards[shardID]
		if status != nil && (status.Initializing || status.State.Transitioning() || status.State == storedb.ShardState_SplitOffPreSnap) {
			continue
		}
		storeID, ok := h.canonicalTxnInspectionStore(snapshot, shardID)
		if !ok {
			continue
		}
		node := h.stores[storeID]
		if node == nil || node.store == nil {
			continue
		}
		shard, ok := node.store.Shard(shardID)
		if !ok {
			continue
		}

		records, err := shard.ListTxnRecords(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing transaction records on shard %s store %s: %w", shardID, storeID, err)
		}
		for _, record := range records {
			txnSnapshot.Records[txnKeyString(record.TxnID)] = TxnRecordRef{
				StoreID: storeID,
				ShardID: shardID,
				Record:  record,
			}
		}

		intents, err := shard.ListTxnIntents(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing transaction intents on shard %s store %s: %w", shardID, storeID, err)
		}
		for _, intent := range intents {
			key := txnKeyString(intent.TxnID)
			txnSnapshot.Intents[key] = append(txnSnapshot.Intents[key], TxnIntentRef{
				StoreID: storeID,
				ShardID: shardID,
				Intent:  intent,
			})
		}
	}

	return txnSnapshot, nil
}

func (h *Harness) canonicalTxnInspectionStore(snapshot *ClusterSnapshot, shardID types.ID) (types.ID, bool) {
	status := snapshot.Shards[shardID]
	if status == nil {
		return 0, false
	}
	if status.RaftStatus != nil {
		leaderID := types.ID(status.RaftStatus.Lead)
		if leaderID != 0 {
			if storeStatus := snapshot.Stores[leaderID]; storeStatus != nil && storeStatus.IsReachable() {
				if storeStatus.Shards[shardID] != nil {
					return leaderID, true
				}
			}
		}
	}

	for _, storeID := range h.storeOrder {
		storeStatus := snapshot.Stores[storeID]
		if storeStatus == nil || !storeStatus.IsReachable() {
			continue
		}
		if storeStatus.Shards[shardID] != nil {
			return storeID, true
		}
	}
	return 0, false
}

func txnKeyString(txnID []byte) string {
	return base64.StdEncoding.EncodeToString(txnID)
}
