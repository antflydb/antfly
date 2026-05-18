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

package tablemgr

import (
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/stretchr/testify/require"
)

func TestStoreStatusEquivalentIncludesEndpoints(t *testing.T) {
	base := &StoreStatus{
		StoreInfo: store.StoreInfo{
			ID:      types.ID(1),
			RaftURL: "http://store-1:19001",
			ApiURL:  "http://store-1:19002",
		},
		State:  store.StoreState_Healthy,
		Shards: map[types.ID]*store.ShardInfo{},
	}
	same := &StoreStatus{
		StoreInfo: store.StoreInfo{
			ID:      types.ID(1),
			RaftURL: "http://store-1:19001",
			ApiURL:  "http://store-1:19002",
		},
		State:  store.StoreState_Healthy,
		Shards: map[types.ID]*store.ShardInfo{},
	}
	moved := &StoreStatus{
		StoreInfo: store.StoreInfo{
			ID:      types.ID(1),
			RaftURL: "http://store-1:29001",
			ApiURL:  "http://store-1:29002",
		},
		State:  store.StoreState_Healthy,
		Shards: map[types.ID]*store.ShardInfo{},
	}

	require.True(t, base.Equivalent(same))
	require.False(t, base.Equivalent(moved))
}
