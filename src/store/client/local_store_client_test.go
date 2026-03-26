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

package client

import (
	"context"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/stretchr/testify/require"
)

type testLocalStore struct {
	store.StoreIface
	shard store.ShardIface
}

func (s *testLocalStore) Shard(types.ID) (store.ShardIface, bool) {
	return s.shard, true
}

type testLocalShard struct {
	store.ShardIface
	lastBatch      *db.BatchOp
	lastMergeBatch *db.BatchOp
}

func (s *testLocalShard) Batch(_ context.Context, batchOp *db.BatchOp, _ bool) error {
	s.lastBatch = batchOp
	return nil
}

func (s *testLocalShard) ApplyMergeChunk(_ context.Context, batchOp *db.BatchOp) error {
	s.lastMergeBatch = batchOp
	return nil
}

func TestLocalStoreClientBatch_PropagatesTimestamp(t *testing.T) {
	shard := &testLocalShard{}
	client := NewLocalStoreClient(types.ID(1), &testLocalStore{shard: shard})
	timestamp := uint64(123456789)
	ctx := storeutils.WithTimestamp(context.Background(), timestamp)

	err := client.Batch(
		ctx,
		types.ID(42),
		[][2][]byte{{[]byte("doc-1"), []byte(`{"name":"test"}`)}},
		nil,
		nil,
		db.Op_SyncLevelWrite,
	)
	require.NoError(t, err)
	require.NotNil(t, shard.lastBatch)
	require.Equal(t, timestamp, shard.lastBatch.GetTimestamp())
}

func TestLocalStoreClientApplyMergeChunk_PropagatesTimestamp(t *testing.T) {
	shard := &testLocalShard{}
	client := NewLocalStoreClient(types.ID(1), &testLocalStore{shard: shard})
	timestamp := uint64(987654321)
	ctx := storeutils.WithTimestamp(context.Background(), timestamp)

	err := client.ApplyMergeChunk(
		ctx,
		types.ID(42),
		[][2][]byte{{[]byte("doc-1"), []byte(`{"name":"test"}`)}},
		nil,
		db.Op_SyncLevelInternalMergeCopy,
	)
	require.NoError(t, err)
	require.NotNil(t, shard.lastMergeBatch)
	require.Equal(t, timestamp, shard.lastMergeBatch.GetTimestamp())
}
