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
	"fmt"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db"
)

// ForwardTransform implements foreign.MetadataTransformer.
// It routes transform operations to the correct shard for the given document key.
func (ms *MetadataStore) ForwardTransform(ctx context.Context, tableName, key string, ops []*db.TransformOp, upsert bool) error {
	shardID, err := ms.findShardForKey(tableName, key)
	if err != nil {
		return err
	}

	transform := db.Transform_builder{
		Key:        []byte(key),
		Operations: ops,
		Upsert:     &upsert,
	}.Build()

	return ms.forwardBatchToShard(ctx, shardID, nil, nil, []*db.Transform{transform}, db.Op_SyncLevelPropose)
}

// ForwardDelete implements foreign.MetadataTransformer.
// It routes a document deletion to the correct shard for the given document key.
func (ms *MetadataStore) ForwardDelete(ctx context.Context, tableName, key string) error {
	shardID, err := ms.findShardForKey(tableName, key)
	if err != nil {
		return err
	}

	deleteKey := []byte(key)
	return ms.forwardBatchToShard(ctx, shardID, nil, [][]byte{deleteKey}, nil, db.Op_SyncLevelPropose)
}

// findShardForKey looks up the table and finds the shard responsible for the given key.
func (ms *MetadataStore) findShardForKey(tableName, key string) (types.ID, error) {
	table, err := ms.tm.GetTable(tableName)
	if err != nil {
		return 0, fmt.Errorf("getting table %s: %w", tableName, err)
	}

	shardID, err := findWriteShardForKey(ms.tm, table, key)
	if err != nil {
		return 0, fmt.Errorf("finding shard for key %q in table %s: %w", key, tableName, err)
	}

	return shardID, nil
}
