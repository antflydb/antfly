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

package tablemgr

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
)

func (ns *StoreStatus) IsReachable() bool {
	if ns == nil {
		return false
	}
	return ns.State == store.StoreState_Healthy
}

// StoreStatus represents the status of a node with its shards
type StoreStatus struct {
	store.StoreInfo
	State       store.StoreState              `json:"state,omitzero"`
	LastSeen    time.Time                     `json:"last_seen"`
	Shards      map[types.ID]*store.ShardInfo `json:"shards"`
	StoreClient client.StoreRPC               `json:"-"`
}

func (ss *StoreStatus) Equivalent(other *StoreStatus) bool {
	if ss == nil || other == nil {
		return ss == nil && other == nil
	}
	return ss.ID == other.ID &&
		ss.State == other.State &&
		maps.EqualFunc(ss.Shards, other.Shards, func(v1, v2 *store.ShardInfo) bool {
			return v1.Equal(v2)
		})
}

func (ss *StoreStatus) Status(ctx context.Context) (*store.StoreStatus, error) {
	if ss == nil || ss.StoreClient == nil {
		return nil, fmt.Errorf("store client not initialized")
	}
	return ss.StoreClient.Status(ctx)
}

func (ss *StoreStatus) StartShard(
	ctx context.Context,
	shardID types.ID,
	req *store.ShardStartRequest,
) error {
	if ss == nil || ss.StoreClient == nil {
		return fmt.Errorf("store client not initialized")
	}
	return ss.StoreClient.StartShard(ctx, shardID, req)
}
