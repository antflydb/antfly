package indexes

import (
	"context"
	"sync"
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/stretchr/testify/require"
)

type recordingShardSearcher struct {
	mu       sync.Mutex
	recorded []*RemoteIndexSearchRequest
}

func (r *recordingShardSearcher) SearchShardTyped(
	_ context.Context,
	_ types.ID,
	req *RemoteIndexSearchRequest,
) (*RemoteIndexSearchResult, error) {
	r.mu.Lock()
	r.recorded = append(r.recorded, req)
	r.mu.Unlock()
	return &RemoteIndexSearchResult{
		Status: &RemoteIndexSearchStatus{
			Total:      1,
			Successful: 1,
		},
	}, nil
}

func TestMultiSearchClonesRequestsPerShard(t *testing.T) {
	searcher := &recordingShardSearcher{}
	indexes := MakeLocalIndexesForShards(searcher, &schema.TableSchema{Version: 42}, []types.ID{1, 2})
	req := &RemoteIndexSearchRequest{Limit: 10}

	_, err := MultiSearch(context.Background(), req, indexes...)
	require.NoError(t, err)

	require.Len(t, searcher.recorded, 2)
	require.NotSame(t, req, searcher.recorded[0])
	require.NotSame(t, req, searcher.recorded[1])
	require.NotSame(t, searcher.recorded[0], searcher.recorded[1])
	require.Zero(t, req.FullTextIndexVersion)
	require.Equal(t, uint32(42), searcher.recorded[0].FullTextIndexVersion)
	require.Equal(t, uint32(42), searcher.recorded[1].FullTextIndexVersion)
}

func TestBatchMultiSearchClonesRequestsPerShard(t *testing.T) {
	searcher := &recordingShardSearcher{}
	indexes := MakeLocalIndexesForShards(searcher, &schema.TableSchema{Version: 7}, []types.ID{1, 2})
	req := &RemoteIndexSearchRequest{Limit: 5}

	_, err := BatchMultiSearch(
		context.Background(),
		[]*RemoteIndexSearchRequest{req, req},
		[]ShardIndex{indexes[0], indexes[1]},
	)
	require.NoError(t, err)

	require.Len(t, searcher.recorded, 2)
	require.NotSame(t, req, searcher.recorded[0])
	require.NotSame(t, req, searcher.recorded[1])
	require.NotSame(t, searcher.recorded[0], searcher.recorded[1])
	require.Zero(t, req.FullTextIndexVersion)
	require.Equal(t, uint32(7), searcher.recorded[0].FullTextIndexVersion)
	require.Equal(t, uint32(7), searcher.recorded[1].FullTextIndexVersion)
}
