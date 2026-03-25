package metadata

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestPrepareHybridFullTextAfterToRemoteIndexQuery(t *testing.T) {
	t.Run("match_all is tracked for hybrid execution", func(t *testing.T) {
		req := &QueryRequest{
			Table:          "docs",
			FullTextSearch: []byte(`{"match_all":{}}`),
			Limit:          10,
		}

		q, err := req.ToRemoteIndexQuery()
		require.NoError(t, err)
		q.PrepareHybridFullTextForSemanticSearch()
		assert.Equal(t, indexes.HybridFullTextModeMatchAll, q.HybridFullTextMode)
	})

	t.Run("match_none is tracked for hybrid execution", func(t *testing.T) {
		req := &QueryRequest{
			Table:          "docs",
			FullTextSearch: []byte(`{"match_none":{}}`),
			Limit:          10,
		}

		q, err := req.ToRemoteIndexQuery()
		require.NoError(t, err)
		q.PrepareHybridFullTextForSemanticSearch()
		assert.Equal(t, indexes.HybridFullTextModeMatchNone, q.HybridFullTextMode)
	})

	t.Run("normal lexical query is left alone", func(t *testing.T) {
		req := &QueryRequest{
			Table:          "docs",
			FullTextSearch: []byte(`{"match":"hello","field":"content"}`),
			Limit:          10,
		}

		q, err := req.ToRemoteIndexQuery()
		require.NoError(t, err)
		q.PrepareHybridFullTextForSemanticSearch()
		assert.Equal(t, indexes.HybridFullTextModeNone, q.HybridFullTextMode)
	})

	t.Run("wrapped match_all is simplified recursively", func(t *testing.T) {
		req := &QueryRequest{
			Table:          "docs",
			FullTextSearch: []byte(`{"disjuncts":[{"match_all":{}},{"match":"hello","field":"content"}]}`),
			Limit:          10,
		}

		q, err := req.ToRemoteIndexQuery()
		require.NoError(t, err)
		q.PrepareHybridFullTextForSemanticSearch()
		assert.Equal(t, indexes.HybridFullTextModeMatchAll, q.HybridFullTextMode)
		_, ok := q.FullTextSearch.(*query.MatchAllQuery)
		assert.True(t, ok)
	})

	t.Run("wrapped match_none is simplified recursively", func(t *testing.T) {
		req := &QueryRequest{
			Table:          "docs",
			FullTextSearch: []byte(`{"conjuncts":[{"match_none":{}},{"match":"hello","field":"content"}]}`),
			Limit:          10,
		}

		q, err := req.ToRemoteIndexQuery()
		require.NoError(t, err)
		q.PrepareHybridFullTextForSemanticSearch()
		assert.Equal(t, indexes.HybridFullTextModeMatchNone, q.HybridFullTextMode)
		_, ok := q.FullTextSearch.(*query.MatchNoneQuery)
		assert.True(t, ok)
	})

	t.Run("mixed conjunction strips match_all but keeps real lexical query", func(t *testing.T) {
		req := &QueryRequest{
			Table:          "docs",
			FullTextSearch: []byte(`{"conjuncts":[{"match_all":{}},{"match":"hello","field":"content"}]}`),
			Limit:          10,
		}

		q, err := req.ToRemoteIndexQuery()
		require.NoError(t, err)
		q.PrepareHybridFullTextForSemanticSearch()
		assert.Equal(t, indexes.HybridFullTextModeNone, q.HybridFullTextMode)
		_, ok := q.FullTextSearch.(*query.MatchQuery)
		assert.True(t, ok)
	})
}

func TestRunQuery_MatchAllHybridDoesNotFallbackWhenPrunerRemovesSemanticHits(t *testing.T) {
	var requestCount atomic.Int32

	shardClient := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		requestCount.Add(1)

		var req indexes.RemoteIndexSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}

		resp := &indexes.RemoteIndexSearchResult{
			Status: &indexes.RemoteIndexSearchStatus{
				Total:      1,
				Successful: 1,
			},
		}

		if req.BleveSearchRequest != nil {
			t.Fatalf("unexpected hybrid full-text fallback request")
		}

		resp.VectorSearchResult = map[string]*vectorindex.SearchResult{
			"emb": {
				Hits: []*vectorindex.SearchHit{
					{
						ID:     "doc1",
						Index:  "emb",
						Score:  0.9,
						Fields: map[string]any{"title": "Vector Hit"},
					},
				},
				Total: 1,
			},
		}

		respBytes, err := json.Marshal(resp)
		require.NoError(t, err)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": []string{"application/json"},
			},
			Body: io.NopCloser(bytes.NewReader(respBytes)),
		}, nil
	})}

	db, err := pebble.Open(t.TempDir(), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	tm, err := tablemgr.NewTableManager(&kv.PebbleDB{DB: db}, shardClient, 0)
	require.NoError(t, err)

	table, err := tm.CreateTable("docs", tablemgr.TableConfig{
		NumShards: 1,
		StartID:   1,
		Schema:    &schema.TableSchema{},
	})
	require.NoError(t, err)

	var shardID types.ID
	var shardStatus *store.ShardStatus
	for id := range table.Shards {
		shardID = id
	}
	shardStatus, err = tm.GetShardStatus(shardID)
	require.NoError(t, err)

	storeID := types.ID(10)
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*tablemgr.StoreStatus{
		storeID: {
			StoreInfo: store.StoreInfo{
				ID:     storeID,
				ApiURL: "http://shard.test",
			},
			State: store.StoreState_Healthy,
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: shardStatus.ShardConfig,
					Peers:       common.NewPeerSet(storeID),
					RaftStatus: &common.RaftStatus{
						Lead:   storeID,
						Voters: common.NewPeerSet(storeID),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	ln := &MetadataStore{
		logger: zap.NewNop(),
		tm:     tm,
	}
	api := &TableApi{
		ln:     ln,
		tm:     tm,
		logger: zap.NewNop(),
	}

	var emb Embedding
	require.NoError(t, emb.FromEmbedding0(Embedding0{1, 0, 0}))

	result := api.runQuery(context.Background(), &QueryRequest{
		Table:          "docs",
		FullTextSearch: []byte(`{"match_all":{}}`),
		Embeddings: map[string]Embedding{
			"emb": emb,
		},
		Limit: 1,
		Pruner: indexes.Pruner{
			MinAbsoluteScore: 1,
		},
	})

	require.Equal(t, int32(http.StatusOK), result.Status)
	assert.Empty(t, result.Error)
	assert.Empty(t, result.Hits.Hits)
	assert.Equal(t, int32(1), requestCount.Load())
}
