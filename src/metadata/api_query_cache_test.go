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

package metadata

import (
	"net/http"
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type testExecutionProvider struct {
	calls int
	out   indexes.ShardIndexes
}

func (p *testExecutionProvider) MaterializeIndexes(
	_ *http.Client,
	_ *indexes.BaseShardIndexPlan,
	_ map[types.ID][]string,
) (indexes.ShardIndexes, error) {
	p.calls++
	return p.out, nil
}

func (p *testExecutionProvider) StoreClientFactory() tablemgr.StoreClientFactory {
	return nil
}

func TestGetOrCreateBaseIndexPlan_CachesImmutablePlan(t *testing.T) {
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

	first, err := api.getOrCreateBaseIndexPlan(tableSchema, peers)
	require.NoError(t, err)
	require.Equal(t, uint32(7), first.SchemaVersion)
	require.Len(t, first.ShardIDs, 1)
	require.Equal(t, types.ID(1), first.ShardIDs[0])
	require.NotNil(t, first.IndexMapping)

	cachedAgain, err := api.getOrCreateBaseIndexPlan(tableSchema, peers)
	require.NoError(t, err)
	require.Same(t, first, cachedAgain)
}

func TestMaterializeIndexes_UsesConfiguredExecutionProvider(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)
	provider := &testExecutionProvider{
		out: indexes.ShardIndexes{
			&indexes.LocalIndex{},
		},
	}
	ms.executionProvider = provider
	plan := indexes.NewBaseShardIndexPlan(&schema.TableSchema{Version: 7}, []types.ID{1})

	materialized, err := ms.materializeIndexes(plan, map[types.ID][]string{
		types.ID(1): {"http://peer-a"},
	})
	require.NoError(t, err)
	require.Len(t, materialized, 1)
	require.Same(t, provider.out[0], materialized[0])
	require.Equal(t, 1, provider.calls)
}
