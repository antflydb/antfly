package sim

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/tablemgr"
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
