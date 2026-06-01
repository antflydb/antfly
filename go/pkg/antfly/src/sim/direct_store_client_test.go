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
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/antflydb/antfly/go/pkg/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
)

func TestDirectStoreClientStopShardPropagatesStoreErrors(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		MetadataIDs:       []types.ID{100},
		StoreIDs:          []types.ID{1},
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	client := newDirectStoreClient(h, 1)
	err = client.StopShard(context.Background(), types.ID(999))
	require.ErrorIs(t, err, store.ErrShardNotFound)
}

func TestDirectStoreClientBackupWritesRequestedFileArchive(t *testing.T) {
	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_320_000, 0).UTC(),
		MetadataIDs:       []types.ID{100},
		StoreIDs:          []types.ID{1},
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	table, err := h.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 1,
		StartID:   0x2800,
	})
	require.NoError(t, err)
	startTableOnAllStores(t, h, "docs")

	var shardID types.ID
	for id := range table.Shards {
		shardID = id
	}
	require.NotZero(t, shardID)

	client := newDirectStoreClient(h, 1)
	backupDir := t.TempDir()
	backupID := "merge_seed_test"
	require.NoError(t, client.Backup(context.Background(), shardID, "file://"+backupDir, backupID, common.BackupFormatNative))

	archiveFile := filepath.Join(backupDir, common.ShardBackupFileName(backupID, shardID))
	require.FileExists(t, archiveFile)
}
