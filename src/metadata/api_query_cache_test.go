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
	"context"
	"net/http"
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type testShardSearcher struct{}

func (testShardSearcher) SearchShardTyped(
	context.Context,
	types.ID,
	*indexes.RemoteIndexSearchRequest,
) (*indexes.RemoteIndexSearchResult, error) {
	return nil, nil
}

func TestGetOrCreateBaseIndexes_InvalidatesCacheWhenFactoryChanges(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	api := &TableApi{
		ln:     ms,
		tm:     ms.tm,
		logger: zaptest.NewLogger(t),
	}
	tableSchema := &schema.TableSchema{Version: 7}
	peers := map[types.ID][]string{
		types.ID(1): {"http://peer-a"},
	}

	remoteFactory := indexes.ShardIndexFactory(func(
		tableSchema *schema.TableSchema,
		shardIDs []types.ID,
		peers map[types.ID][]string,
	) (indexes.ShardIndexes, error) {
		return indexes.MakeRemoteIndexesForShards(http.DefaultClient, tableSchema, shardIDs, peers)
	})
	ms.setShardIndexFactory(remoteFactory)

	first, err := api.getOrCreateBaseIndexes(tableSchema, peers)
	require.NoError(t, err)
	require.Len(t, first, 1)
	_, ok := first[0].(*indexes.RemoteIndex)
	require.True(t, ok)

	cachedAgain, err := api.getOrCreateBaseIndexes(tableSchema, peers)
	require.NoError(t, err)
	require.Same(t, first[0], cachedAgain[0])

	localFactory := indexes.ShardIndexFactory(func(
		tableSchema *schema.TableSchema,
		shardIDs []types.ID,
		_ map[types.ID][]string,
	) (indexes.ShardIndexes, error) {
		return indexes.MakeLocalIndexesForShards(testShardSearcher{}, tableSchema, shardIDs), nil
	})
	ms.setShardIndexFactory(localFactory)

	afterSwitch, err := api.getOrCreateBaseIndexes(tableSchema, peers)
	require.NoError(t, err)
	require.Len(t, afterSwitch, 1)
	_, ok = afterSwitch[0].(*indexes.LocalIndex)
	require.True(t, ok)
	require.NotSame(t, first[0], afterSwitch[0])
}
