package sim

import (
	"context"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
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
