package metadata

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/embeddings"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/time/rate"
)

type metadataMockEmbedder struct{}

func (metadataMockEmbedder) Capabilities() embeddings.EmbedderCapabilities {
	return embeddings.EmbedderCapabilities{
		SupportedMIMETypes: []embeddings.MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{3},
		DefaultDimension:   3,
	}
}

func (metadataMockEmbedder) Embed(_ context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	result := make([][]float32, len(contents))
	for i := range contents {
		result[i] = []float32{1, 2, 3}
	}
	return result, nil
}

func (metadataMockEmbedder) RateLimiter() *rate.Limiter { return nil }

func registerMetadataMockEmbedder(t *testing.T) {
	t.Helper()

	embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	embeddings.RegisterEmbedder(
		embeddings.EmbedderProviderMock,
		func(config embeddings.EmbedderConfig) (embeddings.Embedder, error) {
			return metadataMockEmbedder{}, nil
		},
	)
	t.Cleanup(func() {
		embeddings.DeregisterEmbedder(embeddings.EmbedderProviderMock)
	})
}

func newTestTableAPI(t *testing.T) *TableApi {
	t.Helper()

	ms, _ := setupTestMetadataStore(t)
	return &TableApi{
		ln:     ms,
		tm:     ms.tm,
		logger: zaptest.NewLogger(t),
	}
}

func performCreateTable(t *testing.T, api *TableApi, tableName string, body CreateTableRequest) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables/"+tableName, bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	api.CreateTable(rec, req, tableName)
	return rec
}

func performCreateIndex(t *testing.T, api *TableApi, tableName, indexName string, body indexes.IndexConfig) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/tables/"+tableName+"/indexes/"+indexName,
		bytes.NewReader(payload),
	)
	rec := httptest.NewRecorder()
	api.CreateIndex(rec, req, tableName, indexName)
	return rec
}

func TestCreateTable_EmbeddingsIndexContract(t *testing.T) {
	t.Run("accepts external dense and sparse indexes", func(t *testing.T) {
		api := newTestTableAPI(t)

		rec := performCreateTable(t, api, "docs", CreateTableRequest{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"manual_dense": *indexes.NewEmbeddingsConfig("manual_dense", indexes.EmbeddingsIndexConfig{
					External:  true,
					Dimension: 3,
				}),
				"manual_sparse": *indexes.NewEmbeddingsConfig("manual_sparse", indexes.EmbeddingsIndexConfig{
					External: true,
					Sparse:   true,
				}),
			},
		})

		require.Equal(t, http.StatusOK, rec.Code)

		table, err := api.tm.GetTable("docs")
		require.NoError(t, err)

		dense, err := table.Indexes["manual_dense"].AsEmbeddingsIndexConfig()
		require.NoError(t, err)
		require.True(t, dense.External)
		require.False(t, dense.Sparse)
		require.EqualValues(t, 3, dense.Dimension)

		sparse, err := table.Indexes["manual_sparse"].AsEmbeddingsIndexConfig()
		require.NoError(t, err)
		require.True(t, sparse.External)
		require.True(t, sparse.Sparse)
	})

	t.Run("rejects external dense indexes without a dimension", func(t *testing.T) {
		api := newTestTableAPI(t)

		rec := performCreateTable(t, api, "docs", CreateTableRequest{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"manual_dense": *indexes.NewEmbeddingsConfig("manual_dense", indexes.EmbeddingsIndexConfig{
					External: true,
				}),
			},
		})

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "external dense index")
	})

	t.Run("rejects managed dense indexes without an embedder", func(t *testing.T) {
		api := newTestTableAPI(t)

		rec := performCreateTable(t, api, "docs", CreateTableRequest{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"managed_dense": *indexes.NewEmbeddingsConfig("managed_dense", indexes.EmbeddingsIndexConfig{
					Field: "body",
				}),
			},
		})

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "must specify an embedder")
		require.Contains(t, rec.Body.String(), "external=true")
	})

	t.Run("rejects managed sparse indexes without an embedder", func(t *testing.T) {
		api := newTestTableAPI(t)

		rec := performCreateTable(t, api, "docs", CreateTableRequest{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"managed_sparse": *indexes.NewEmbeddingsConfig("managed_sparse", indexes.EmbeddingsIndexConfig{
					Field:  "body",
					Sparse: true,
				}),
			},
		})

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "must specify an embedder")
	})

	t.Run("accepts managed dense indexes with an embedder and infers dimension", func(t *testing.T) {
		api := newTestTableAPI(t)
		registerMetadataMockEmbedder(t)

		rec := performCreateTable(t, api, "docs", CreateTableRequest{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"managed_dense": *indexes.NewEmbeddingsConfig("managed_dense", indexes.EmbeddingsIndexConfig{
					Field: "body",
					Embedder: &embeddings.EmbedderConfig{
						Provider: embeddings.EmbedderProviderMock,
					},
				}),
			},
		})

		require.Equal(t, http.StatusOK, rec.Code)

		table, err := api.tm.GetTable("docs")
		require.NoError(t, err)

		cfg, err := table.Indexes["managed_dense"].AsEmbeddingsIndexConfig()
		require.NoError(t, err)
		require.False(t, cfg.External)
		require.EqualValues(t, 3, cfg.Dimension)
		require.NotNil(t, cfg.Embedder)
	})
}

func TestCreateIndex_EmbeddingsIndexContract(t *testing.T) {
	t.Run("rejects managed dense indexes without an embedder", func(t *testing.T) {
		api := newTestTableAPI(t)
		_, err := api.tm.CreateTable("docs", tablemgr.TableConfig{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"full_text_index_v0": *indexes.NewFullTextIndexConfig("full_text_index_v0", false),
			},
		})
		require.NoError(t, err)

		rec := performCreateIndex(t, api, "docs", "managed_dense", *indexes.NewEmbeddingsConfig(
			"managed_dense",
			indexes.EmbeddingsIndexConfig{Field: "body"},
		))

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "must specify an embedder")
	})

	t.Run("rejects managed sparse indexes without an embedder", func(t *testing.T) {
		api := newTestTableAPI(t)
		_, err := api.tm.CreateTable("docs", tablemgr.TableConfig{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"full_text_index_v0": *indexes.NewFullTextIndexConfig("full_text_index_v0", false),
			},
		})
		require.NoError(t, err)

		rec := performCreateIndex(t, api, "docs", "managed_sparse", *indexes.NewEmbeddingsConfig(
			"managed_sparse",
			indexes.EmbeddingsIndexConfig{Field: "body", Sparse: true},
		))

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "must specify an embedder")
	})

	t.Run("rejects external dense indexes without a dimension", func(t *testing.T) {
		api := newTestTableAPI(t)
		_, err := api.tm.CreateTable("docs", tablemgr.TableConfig{
			NumShards: 1,
			Indexes: map[string]indexes.IndexConfig{
				"full_text_index_v0": *indexes.NewFullTextIndexConfig("full_text_index_v0", false),
			},
		})
		require.NoError(t, err)

		rec := performCreateIndex(t, api, "docs", "manual_dense", *indexes.NewEmbeddingsConfig(
			"manual_dense",
			indexes.EmbeddingsIndexConfig{External: true},
		))

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "external dense index")
	})
}
