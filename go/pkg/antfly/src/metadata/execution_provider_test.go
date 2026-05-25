// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"context"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/schema"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/require"
)

type testProviderStore struct {
	status *store.StoreStatus
}

func (s *testProviderStore) Shard(types.ID) (store.ShardIface, bool) {
	return nil, false
}

func (s *testProviderStore) Status() *store.StoreStatus {
	return s.status
}

func (s *testProviderStore) StopRaftGroup(types.ID) error {
	return nil
}

func (s *testProviderStore) StartRaftGroup(types.ID, []common.Peer, bool, *store.ShardStartConfig) error {
	return nil
}

func (s *testProviderStore) ID() types.ID {
	return 0
}

func (s *testProviderStore) Scan(
	context.Context,
	types.ID,
	[]byte,
	[]byte,
	db.ScanOptions,
) (*db.ScanResult, error) {
	return nil, nil
}

func TestDeferredLocalExecutionProvider_BindStore(t *testing.T) {
	provider := NewDeferredLocalExecutionProvider()
	plan := indexes.NewBaseShardIndexPlan(&schema.TableSchema{Version: 7}, []types.ID{1})

	_, err := provider.MaterializeIndexes(nil, plan, nil)
	require.ErrorIs(t, err, ErrLocalStoreNotBound)

	client := provider.StoreClientFactory()(nil, types.ID(9), "")
	_, err = client.Status(context.Background())
	require.ErrorIs(t, err, ErrLocalStoreNotBound)

	expectedStatus := &store.StoreStatus{ID: types.ID(9)}
	provider.BindStore(&testProviderStore{status: expectedStatus})

	materialized, err := provider.MaterializeIndexes(nil, plan, nil)
	require.NoError(t, err)
	require.Len(t, materialized, 1)
	_, ok := materialized[0].(*indexes.LocalIndex)
	require.True(t, ok)

	status, err := client.Status(context.Background())
	require.NoError(t, err)
	require.Same(t, expectedStatus, status)
}
