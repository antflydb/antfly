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
	"slices"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func startTableOnAllStores(t *testing.T, h *Harness, tableName string) *store.Table {
	t.Helper()

	table, err := h.GetTable(tableName)
	require.NoError(t, err)

	shardIDs := make([]types.ID, 0, len(table.Shards))
	for shardID := range table.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)

	for _, shardID := range shardIDs {
		require.NoError(t, h.StartShardOnAllStores(shardID))
	}

	for _, shardID := range shardIDs {
		_, err = h.WaitForLeader(shardID, 30*time.Second)
		require.NoError(t, err)
	}

	table, err = h.GetTable(tableName)
	require.NoError(t, err)
	return table
}

func pickCoordinatorForTest(txnID uuid.UUID, shardIDs ...types.ID) types.ID {
	ordered := append([]types.ID(nil), shardIDs...)
	slices.Sort(ordered)
	return ordered[xxhash.Sum64(txnID[:])%uint64(len(ordered))]
}
