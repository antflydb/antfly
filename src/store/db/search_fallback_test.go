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

package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func setupTestDBWithFullTextV0(t *testing.T) (*DBImpl, string) {
	t.Helper()
	dir := t.TempDir()

	db := &DBImpl{logger: zaptest.NewLogger(t)}
	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title":   map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index_v0", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))
	require.NoError(t, db.UpdateSchema(tableSchema))

	return db, dir
}

func TestSearch_FullTextIndexFallback(t *testing.T) {
	t.Run("missing version falls back to v0", func(t *testing.T) {
		db, dir := setupTestDBWithFullTextV0(t)
		defer db.Close()
		defer os.RemoveAll(dir)

		ctx := context.Background()

		doc := map[string]any{"title": "test doc", "content": "hello world"}
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelWrite))
		time.Sleep(500 * time.Millisecond)

		// Request version 1 which isn't registered — should fall back to v0
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest:   bleve.NewSearchRequest(query.NewMatchQuery("hello")),
			FullTextIndexVersion: 1,
			Limit:                10,
		}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)

		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.NotNil(t, res.BleveSearchResult)
		assert.NotEmpty(t, res.BleveSearchResult.Hits)
	})

	t.Run("no full text index returns error", func(t *testing.T) {
		dir := t.TempDir()
		db := &DBImpl{logger: zaptest.NewLogger(t)}
		require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
		defer db.Close()
		defer os.RemoveAll(dir)

		tableSchema := &schema.TableSchema{
			DefaultType: "default",
			DocumentSchemas: map[string]schema.DocumentSchema{
				"default": {
					Schema: map[string]any{
						"type": "object",
						"properties": map[string]any{
							"title": map[string]any{"type": "string"},
						},
					},
				},
			},
		}
		require.NoError(t, db.UpdateSchema(tableSchema))

		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest:   bleve.NewSearchRequest(query.NewMatchQuery("hello")),
			FullTextIndexVersion: 1,
			Limit:                10,
		}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)

		_, err = db.Search(context.Background(), reqBytes)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "full_text_index_v1 does not exist")
	})
}
