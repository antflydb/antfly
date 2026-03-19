//go:build zigdb

package db

import (
	"context"
	"encoding/base64"
	"strconv"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/reranking"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/datetime/sanitized"
	blevegeo "github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/search/query"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func openZigSearchTestDB(t *testing.T) *ZigCoreDB {
	t.Helper()

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title":        map[string]any{"type": "string"},
						"content":      map[string]any{"type": "string"},
						"price":        map[string]any{"type": "number"},
						"published_at": map[string]any{"type": "string", "format": "date-time"},
						"active":       map[string]any{"type": "boolean"},
						"ip": map[string]any{
							"type":           "string",
							"x-antfly-types": []string{"keyword"},
						},
						"location": map[string]any{"type": "geo_point"},
					},
				},
			},
		},
	}

	db := NewZigCoreDB(
		zaptest.NewLogger(t),
		nil,
		tableSchema,
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		nil,
	).(*ZigCoreDB)

	require.NoError(t, db.Open(t.TempDir(), false, tableSchema, types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return db
}

func openZigSearchTestDBWithSchema(t *testing.T, tableSchema *schema.TableSchema) *ZigCoreDB {
	t.Helper()

	db := NewZigCoreDB(
		zaptest.NewLogger(t),
		nil,
		tableSchema,
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		nil,
	).(*ZigCoreDB)

	require.NoError(t, db.Open(t.TempDir(), false, tableSchema, types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return db
}

func customTokenizerAnalyzerSearchSchema() *schema.TableSchema {
	return &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					schema.XAntflyCharFilters: map[string]any{
						"strip_html_alias": map[string]any{
							"type": "html",
						},
					},
					schema.XAntflyTokenFilters: map[string]any{
						"tri_gram_filter": map[string]any{
							"type": "ngram",
							"min":  3,
							"max":  3,
						},
					},
					schema.XAntflyTokenizers: map[string]any{
						"whitespace_alias": map[string]any{
							"type": "whitespace",
						},
					},
					schema.XAntflyAnalyzers: map[string]any{
						"tri_html_analyzer": map[string]any{
							"type":          "custom",
							"tokenizer":     "whitespace_alias",
							"char_filters":  []any{"strip_html_alias"},
							"token_filters": []any{"to_lower", "tri_gram_filter"},
						},
					},
					"properties": map[string]any{
						"title": map[string]any{
							"type":                 "string",
							schema.XAntflyTypes:    []string{"text"},
							schema.XAntflyAnalyzer: "tri_html_analyzer",
						},
					},
				},
			},
		},
	}
}

func compositeSortSearchSchema() *schema.TableSchema {
	return &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
						"meta":    map[string]any{"type": "object"},
						"tags": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "string",
							},
						},
						"scores": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "number",
							},
						},
					},
				},
			},
		},
	}
}

func floatPtr(v float64) *float64 {
	return &v
}

func TestZigCoreDB_FullTextSearchFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	docJSON, err := json.Marshal(map[string]any{
		"content": "hello world",
		"title":   "test document",
	})
	require.NoError(t, err)

	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	matchHello := query.NewMatchQuery("hello")
	matchHello.SetField("content")
	matchingReq := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchHello),
		FilterQuery:        json.RawMessage(`{"match":"hello","field":"content"}`),
		Limit:              10,
	}
	matchingReq.BleveSearchRequest.Size = 10
	reqBytes, err := json.Marshal(matchingReq)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var matching indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &matching))
	require.NotNil(t, matching.BleveSearchResult)
	assert.Len(t, matching.BleveSearchResult.Hits, 1)

	matchHelloNoFilter := query.NewMatchQuery("hello")
	matchHelloNoFilter.SetField("content")
	noMatchReq := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchHelloNoFilter),
		FilterQuery:        json.RawMessage(`{"match":"nonexistent_term_xyz","field":"content"}`),
		Limit:              10,
	}
	noMatchReq.BleveSearchRequest.Size = 10
	reqBytes, err = json.Marshal(noMatchReq)
	require.NoError(t, err)

	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var noMatch indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &noMatch))
	require.NotNil(t, noMatch.BleveSearchResult)
	assert.Empty(t, noMatch.BleveSearchResult.Hits)
}

func TestZigCoreDB_FullTextMatchNoneQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	docJSON, err := json.Marshal(map[string]any{
		"content": "hello world",
		"title":   "test document",
	})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(query.NewMatchNoneQuery()),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	assert.Empty(t, res.BleveSearchResult.Hits)
}

func TestZigCoreDB_FullTextMatchQueryUsesSchemaFieldAnalyzer(t *testing.T) {
	db := openZigSearchTestDBWithSchema(t, customTokenizerAnalyzerSearchSchema())
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	docJSON, err := json.Marshal(map[string]any{
		"title": "<b>Hello</b>",
	})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	q := query.NewMatchQuery("hello")
	q.SetField("title")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextMatchPhraseQueryUsesExplicitCustomAnalyzer(t *testing.T) {
	db := openZigSearchTestDBWithSchema(t, customTokenizerAnalyzerSearchSchema())
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	docJSON, err := json.Marshal(map[string]any{
		"title": "<b>Hello world</b>",
	})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	q := query.NewMatchPhraseQuery("hello world")
	q.SetField("title")
	q.Analyzer = "tri_html_analyzer"
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextSearchFilterPrefix(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"keep:1": {"content": "hello world", "title": "keep"},
		"skip:1": {"content": "hello world", "title": "skip"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchHello := query.NewMatchQuery("hello")
	matchHello.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchHello),
		FilterPrefix:       []byte("keep:"),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "keep:1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextDisjunctionQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha only", "title": "alpha"},
		"doc2": {"content": "beta only", "title": "beta"},
		"doc3": {"content": "gamma only", "title": "gamma"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	left := query.NewMatchQuery("alpha")
	left.SetField("content")
	right := query.NewMatchQuery("gamma")
	right.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(query.NewDisjunctionQuery([]query.Query{left, right})),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextConjunctionQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "both"},
		"doc2": {"content": "alpha only", "title": "alpha"},
		"doc3": {"content": "beta only", "title": "beta"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	left := query.NewMatchQuery("alpha")
	left.SetField("content")
	right := query.NewMatchQuery("beta")
	right.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(query.NewConjunctionQuery([]query.Query{left, right})),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextPhraseQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta gamma", "title": "exact"},
		"doc2": {"content": "alpha gamma beta", "title": "not exact"},
		"doc3": {"content": "beta alpha", "title": "different"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	phrase := query.NewMatchPhraseQuery("alpha beta")
	phrase.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(phrase),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextPhraseQueryWithFuzziness(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta"},
		"doc2": {"content": "alphi beta"},
		"doc3": {"content": "alpha delta"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewPhraseQuery([]string{"alpha", "beta"}, "content")
	q.SetFuzziness(1)
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextExactPhraseQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta gamma"},
		"doc2": {"content": "alpha gamma beta"},
		"doc3": {"content": "beta alpha gamma"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewPhraseQuery([]string{"alpha", "beta"}, "content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextMultiPhraseQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha gamma"},
		"doc2": {"content": "beta gamma"},
		"doc3": {"content": "alpha delta"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewMultiPhraseQuery([][]string{{"alpha", "beta"}, {"gamma"}}, "content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextMultiPhraseQueryWithAutoFuzziness(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta"},
		"doc2": {"content": "alphx beta"},
		"doc3": {"content": "omega delta"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewMultiPhraseQuery([][]string{{"alpha"}, {"beta"}}, "content")
	q.SetAutoFuzziness(true)
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextFuzzyQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "hello world", "title": "alpha"},
		"doc2": {"content": "help there", "title": "beta"},
		"doc3": {"content": "goodbye world", "title": "gamma"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewFuzzyQuery("helo")
	q.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextFuzzyQueryWithPrefix(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "hello world"},
		"doc2": {"content": "cello world"},
		"doc3": {"content": "yellow world"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewFuzzyQuery("helo")
	q.SetField("content")
	q.SetPrefix(1)
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextMatchPhraseQueryWithAutoFuzziness(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta gamma"},
		"doc2": {"content": "alphx beta gamma"},
		"doc3": {"content": "omega beta gamma"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewMatchPhraseQuery("alpha beta")
	q.SetField("content")
	q.SetAutoFuzziness(true)
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextNumericRangeQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "one", "price": 10.0},
		"doc2": {"title": "two", "price": 20.0},
		"doc3": {"title": "three", "price": 30.0},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	min := 15.0
	max := 30.0
	q := query.NewNumericRangeQuery(&min, &max)
	q.SetField("price")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc2", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextTermRangeQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "alpha"},
		"doc2": {"title": "beta"},
		"doc3": {"title": "gamma"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	inclusiveMin := true
	inclusiveMax := false
	q := query.NewTermRangeInclusiveQuery("beta", "gamma", &inclusiveMin, &inclusiveMax)
	q.SetField("title")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc2", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextDateRangeQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	start1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	start2 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	start3 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "one", "published_at": start1.Format(time.RFC3339)},
		"doc2": {"title": "two", "published_at": start2.Format(time.RFC3339)},
		"doc3": {"title": "three", "published_at": start3.Format(time.RFC3339)},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewDateRangeQuery(start2, start3)
	q.SetField("published_at")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc2", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextDocIDQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta"},
		"doc2": {"content": "beta gamma"},
		"doc3": {"content": "gamma delta"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewDocIDQuery([]string{"doc1", "doc3"})
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextBoolFieldQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "one", "active": true},
		"doc2": {"title": "two", "active": false},
		"doc3": {"title": "three", "active": true},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewBoolFieldQuery(true)
	q.SetField("active")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextIPRangeQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "one", "ip": "192.168.1.10"},
		"doc2": {"title": "two", "ip": "192.168.1.99"},
		"doc3": {"title": "three", "ip": "10.0.0.5"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewIPRangeQuery("192.168.1.0/24")
	q.SetField("ip")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextGeoDistanceQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "location": map[string]any{"lat": 37.7749, "lon": -122.4194}},
		"doc2": {"content": "alpha", "location": map[string]any{"lat": 37.7750, "lon": -122.4195}},
		"doc3": {"content": "alpha", "location": map[string]any{"lat": 40.7128, "lon": -74.0060}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := bleve.NewGeoDistanceQuery(-122.4194, 37.7749, "2km")
	q.SetField("location")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextGeoBoundingBoxQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "location": map[string]any{"lat": 37.7749, "lon": -122.4194}},
		"doc2": {"content": "alpha", "location": map[string]any{"lat": 37.8044, "lon": -122.2711}},
		"doc3": {"content": "alpha", "location": map[string]any{"lat": 40.7128, "lon": -74.0060}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := bleve.NewGeoBoundingBoxQuery(-122.6, 37.9, -122.2, 37.7)
	q.SetField("location")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextGeoBoundingPolygonQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "location": map[string]any{"lat": 5.0, "lon": 5.0}},
		"doc2": {"content": "alpha", "location": map[string]any{"lat": 20.0, "lon": 20.0}},
		"doc3": {"content": "alpha", "location": map[string]any{"lat": 3.0, "lon": 4.0}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewGeoBoundingPolygonQuery([]blevegeo.Point{
		{Lon: 0, Lat: 0},
		{Lon: 10, Lat: 0},
		{Lon: 10, Lat: 10},
		{Lon: 0, Lat: 10},
	})
	q.SetField("location")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextGeoShapePolygonQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "location": map[string]any{"lat": 5.0, "lon": 5.0}},
		"doc2": {"content": "alpha", "location": map[string]any{"lat": 20.0, "lon": 20.0}},
		"doc3": {"content": "alpha", "location": map[string]any{"lat": 3.0, "lon": 4.0}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q, err := query.NewGeoShapeQuery([][][][]float64{
		{
			{
				{0, 0},
				{10, 0},
				{10, 10},
				{0, 10},
				{0, 0},
			},
		},
	}, "polygon", "intersects")
	require.NoError(t, err)
	q.SetField("location")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextPrefixQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "small"},
		"doc2": {"content": "beta gamma", "title": "smile"},
		"doc3": {"content": "gamma delta", "title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewPrefixQuery("sm")
	q.SetField("title")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextWildcardQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "small"},
		"doc2": {"content": "beta gamma", "title": "smile"},
		"doc3": {"content": "gamma delta", "title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewWildcardQuery("smi*")
	q.SetField("title")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc2", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextRegexpQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "small"},
		"doc2": {"content": "beta gamma", "title": "smile"},
		"doc3": {"content": "gamma delta", "title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewRegexpQuery("sm(a|i).*")
	q.SetField("title")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextBooleanQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "small"},
		"doc2": {"content": "alpha gamma", "title": "smile"},
		"doc3": {"content": "beta gamma", "title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	must := query.NewMatchQuery("alpha")
	must.SetField("content")
	should := query.NewPrefixQuery("sm")
	should.SetField("title")
	mustNot := query.NewRegexpQuery("alp.*")
	mustNot.SetField("title")

	boolQ := query.NewBooleanQuery([]query.Query{must}, []query.Query{should}, []query.Query{mustNot})
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(boolQ),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextQueryStringQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "small"},
		"doc2": {"content": "alpha gamma", "title": "smile"},
		"doc3": {"content": "beta gamma", "title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	qs := query.NewQueryStringQuery(`title:sm*`)
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(qs),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextQueryStringQueryWithBoost(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "small"},
		"doc2": {"content": "alpha gamma", "title": "smile"},
		"doc3": {"content": "beta gamma", "title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	qs := query.NewQueryStringQuery(`title:sm*`)
	qs.SetBoost(5)
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(qs),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.ElementsMatch(t, []string{"doc1", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
	})
}

func TestZigCoreDB_FullTextPhraseQueryWithBoost(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "hello world"},
		"doc2": {"content": "hello brave world"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewPhraseQuery([]string{"hello", "world"}, "content")
	q.SetBoost(4)
	req := &indexes.RemoteIndexSearchRequest{BleveSearchRequest: bleve.NewSearchRequest(q), Limit: 10}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
	assert.InDelta(t, 4.0, res.BleveSearchResult.Hits[0].Score, 0.001)
}

func TestZigCoreDB_FullTextBoostedDictionaryQueries(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "small"},
		"doc2": {"title": "smile"},
		"doc3": {"title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	tests := []struct {
		name string
		q    blevequery.Query
		want []string
	}{
		{
			name: "prefix",
			q: func() blevequery.Query {
				q := query.NewPrefixQuery("sm")
				q.SetField("title")
				q.SetBoost(5)
				return q
			}(),
			want: []string{"doc1", "doc2"},
		},
		{
			name: "wildcard",
			q: func() blevequery.Query {
				q := query.NewWildcardQuery("smi*")
				q.SetField("title")
				q.SetBoost(5)
				return q
			}(),
			want: []string{"doc2"},
		},
		{
			name: "regexp",
			q: func() blevequery.Query {
				q := query.NewRegexpQuery("sm(a|i).*")
				q.SetField("title")
				q.SetBoost(5)
				return q
			}(),
			want: []string{"doc1", "doc2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &indexes.RemoteIndexSearchRequest{BleveSearchRequest: bleve.NewSearchRequest(tt.q), Limit: 10}
			req.BleveSearchRequest.Size = 10

			reqBytes, err := json.Marshal(req)
			require.NoError(t, err)
			resBytes, err := db.Search(ctx, reqBytes)
			require.NoError(t, err)

			var res indexes.RemoteIndexSearchResult
			require.NoError(t, json.Unmarshal(resBytes, &res))
			require.NotNil(t, res.BleveSearchResult)
			require.Len(t, res.BleveSearchResult.Hits, len(tt.want))
			got := make([]string, 0, len(res.BleveSearchResult.Hits))
			for _, hit := range res.BleveSearchResult.Hits {
				got = append(got, hit.ID)
				assert.InDelta(t, 5.0, hit.Score, 0.001)
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestZigCoreDB_FullTextBoostedRangeQueries(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"price": 10.0, "published_at": "2025-01-01T00:00:00Z"},
		"doc2": {"price": 20.0, "published_at": "2025-01-02T00:00:00Z"},
		"doc3": {"price": 30.0, "published_at": "2025-01-03T00:00:00Z"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	t.Run("numeric_range", func(t *testing.T) {
		minVal := 15.0
		maxVal := 35.0
		q := query.NewNumericRangeInclusiveQuery(&minVal, &maxVal, nil, nil)
		q.SetField("price")
		q.SetBoost(6)
		req := &indexes.RemoteIndexSearchRequest{BleveSearchRequest: bleve.NewSearchRequest(q), Limit: 10}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.Len(t, res.BleveSearchResult.Hits, 2)
		for _, hit := range res.BleveSearchResult.Hits {
			assert.InDelta(t, 6.0, hit.Score, 0.001)
		}
	})

	t.Run("date_range_string_parser", func(t *testing.T) {
		q := query.NewDateRangeStringQuery("1735689600000", "1735862400000")
		q.SetField("published_at")
		q.SetDateTimeParser("unix_milli")
		q.SetBoost(6)
		req := &indexes.RemoteIndexSearchRequest{BleveSearchRequest: bleve.NewSearchRequest(q), Limit: 10}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.Len(t, res.BleveSearchResult.Hits, 2)
		got := []string{res.BleveSearchResult.Hits[0].ID, res.BleveSearchResult.Hits[1].ID}
		assert.ElementsMatch(t, []string{"doc1", "doc2"}, got)
		for _, hit := range res.BleveSearchResult.Hits {
			assert.InDelta(t, 6.0, hit.Score, 0.001)
		}
	})
}

func TestZigCoreDB_FullTextBoostedGeoAndBooleanQueries(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta", "title": "small", "location": map[string]any{"lat": 37.7749, "lon": -122.4194}},
		"doc2": {"content": "alpha gamma", "title": "smile", "location": map[string]any{"lat": 37.7750, "lon": -122.4195}},
		"doc3": {"content": "beta gamma", "title": "alpha", "location": map[string]any{"lat": 40.7128, "lon": -74.0060}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	t.Run("geo_distance", func(t *testing.T) {
		q := bleve.NewGeoDistanceQuery(-122.4194, 37.7749, "2km")
		q.SetField("location")
		q.SetBoost(7)
		req := &indexes.RemoteIndexSearchRequest{BleveSearchRequest: bleve.NewSearchRequest(q), Limit: 10}
		req.BleveSearchRequest.Size = 10
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.Len(t, res.BleveSearchResult.Hits, 2)
		for _, hit := range res.BleveSearchResult.Hits {
			assert.InDelta(t, 7.0, hit.Score, 0.001)
		}
	})

	t.Run("geo_bbox", func(t *testing.T) {
		q := bleve.NewGeoBoundingBoxQuery(-122.6, 37.9, -122.2, 37.7)
		q.SetField("location")
		q.SetBoost(7)
		req := &indexes.RemoteIndexSearchRequest{BleveSearchRequest: bleve.NewSearchRequest(q), Limit: 10}
		req.BleveSearchRequest.Size = 10
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.Len(t, res.BleveSearchResult.Hits, 2)
		for _, hit := range res.BleveSearchResult.Hits {
			assert.InDelta(t, 7.0, hit.Score, 0.001)
		}
	})

	t.Run("boolean", func(t *testing.T) {
		must := query.NewMatchQuery("alpha")
		must.SetField("content")
		should := query.NewPrefixQuery("sm")
		should.SetField("title")
		mustNot := query.NewRegexpQuery("alp.*")
		mustNot.SetField("title")
		boolQ := query.NewBooleanQuery([]query.Query{must}, []query.Query{should}, []query.Query{mustNot})
		boolQ.SetBoost(7)

		req := &indexes.RemoteIndexSearchRequest{BleveSearchRequest: bleve.NewSearchRequest(boolQ), Limit: 10}
		req.BleveSearchRequest.Size = 10
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.Len(t, res.BleveSearchResult.Hits, 2)
		assert.InDelta(t, res.BleveSearchResult.Hits[0].Score, res.BleveSearchResult.Hits[1].Score, 0.001)
		assert.Greater(t, res.BleveSearchResult.Hits[0].Score, float64(7.0))
	})
}

func TestZigCoreDB_FullTextBooleanQueryNestedLeafBoosts(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "title": "small"},
		"doc2": {"content": "alpha", "title": "smile"},
		"doc3": {"content": "alpha", "title": "alpha"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	must := query.NewMatchQuery("alpha")
	must.SetField("content")
	must.SetBoost(2)

	should := query.NewPrefixQuery("sm")
	should.SetField("title")
	should.SetBoost(5)

	boolQ := query.NewBooleanQuery([]query.Query{must}, []query.Query{should}, nil)
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(boolQ),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)

	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
	assert.Equal(t, "doc2", res.BleveSearchResult.Hits[1].ID)
	assert.Equal(t, "doc3", res.BleveSearchResult.Hits[2].ID)
	assert.InDelta(t, res.BleveSearchResult.Hits[0].Score, res.BleveSearchResult.Hits[1].Score, 0.001)
	assert.Greater(t, res.BleveSearchResult.Hits[0].Score, res.BleveSearchResult.Hits[2].Score)
	assert.Greater(t, res.BleveSearchResult.Hits[0].Score, float64(5.0))
	assert.Greater(t, res.BleveSearchResult.Hits[2].Score, float64(0.0))
}

func TestZigCoreDB_FullTextDateRangeStringQueryWithNamedDefaultParser(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"published_at": "2025-01-01T00:00:00Z"},
		"doc2": {"published_at": "2025-01-02T00:00:00Z"},
		"doc3": {"published_at": "2025-01-03T00:00:00Z"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	orig := blevequery.QueryDateTimeParser
	blevequery.QueryDateTimeParser = "dateTimeOptional"
	defer func() { blevequery.QueryDateTimeParser = orig }()

	q := query.NewDateRangeStringQuery("2025-01-01", "2025-01-02")
	q.SetField("published_at")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestNormalizeBackendTextQuery_DateRangeStringUsesIndexMappingParser(t *testing.T) {
	indexMapping := bleve.NewIndexMapping()
	require.NoError(t, indexMapping.AddCustomDateTimeParser("queryDT", map[string]any{
		"type": sanitized.Name,
		"layouts": []any{
			"02/01/2006 3:04PM",
		},
	}))

	q := query.NewDateRangeStringQuery("01/02/2025 3:04PM", "01/03/2025 3:04PM")
	q.SetField("published_at")
	q.SetDateTimeParser("queryDT")

	normalized, err := normalizeBackendTextQuery(q, nil, indexMapping)
	require.NoError(t, err)

	payload, ok := normalized["date_range"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "published_at", payload["field"])
	assert.Equal(t, time.Date(2025, 2, 1, 15, 4, 0, 0, time.UTC).UnixNano(), payload["start_ns"])
	assert.Equal(t, time.Date(2025, 3, 1, 15, 4, 0, 0, time.UTC).UnixNano(), payload["end_ns"])
}

func TestNormalizeBackendTextQuery_DateRangeStringUsesAnalysisConfigParser(t *testing.T) {
	analysisConfig := &schema.AnalysisConfig{
		DefaultDateTimeParser: "queryDT",
		DateTimeParsers: map[string]schema.AnalysisComponentConfig{
			"queryDT": {
				Type: "sanitizedgo",
				Config: map[string]any{
					"layouts": []any{
						"02/01/2006 3:04PM",
					},
				},
			},
		},
	}

	q := query.NewDateRangeStringQuery("01/02/2025 3:04PM", "01/03/2025 3:04PM")
	q.SetField("published_at")

	normalized, err := normalizeBackendTextQuery(q, analysisConfig, nil)
	require.NoError(t, err)

	payload, ok := normalized["date_range"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, time.Date(2025, 2, 1, 15, 4, 0, 0, time.UTC).UnixNano(), payload["start_ns"])
	assert.Equal(t, time.Date(2025, 3, 1, 15, 4, 0, 0, time.UTC).UnixNano(), payload["end_ns"])
}

func TestZigCoreDB_FullTextSearchPagingOptions(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	writeDoc := func(key string, content string) {
		docJSON, err := json.Marshal(map[string]any{
			"content": content,
		})
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	writeDoc("doc1", "alpha alpha alpha")
	writeDoc("doc2", "alpha")
	writeDoc("doc3", "alpha alpha")

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			Limit:  1,
			Offset: 1,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc3", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextCountStar(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha beta"},
		"doc2": {"content": "alpha gamma"},
		"doc3": {"content": "beta gamma"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		CountStar:          true,
		Limit:              10,
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	assert.EqualValues(t, 2, res.Total)
	assert.EqualValues(t, 2, res.BleveSearchResult.Total)
	assert.Empty(t, res.BleveSearchResult.Hits)
}

func TestZigCoreDB_FullTextCountStarWithFilters(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"keep1": {"content": "alpha beta"},
		"keep2": {"content": "alpha gamma"},
		"drop1": {"content": "alpha beta"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		CountStar:          true,
		Limit:              10,
		FilterPrefix:       []byte("keep"),
		FilterQuery:        json.RawMessage(`{"term":"beta","field":"content"}`),
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	assert.EqualValues(t, 1, res.Total)
	assert.EqualValues(t, 1, res.BleveSearchResult.Total)
	assert.Empty(t, res.BleveSearchResult.Hits)
}

func TestZigCoreDB_FullTextSearchCursorPaging(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	writeDoc := func(key string, content string) {
		docJSON, err := json.Marshal(map[string]any{
			"content": content,
		})
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	writeDoc("doc1", "alpha alpha alpha")
	writeDoc("doc2", "alpha")
	writeDoc("doc3", "alpha alpha")

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			Limit:       1,
			SearchAfter: []string{"doc3"},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc2", res.BleveSearchResult.Hits[0].ID)

	req.BlevePagingOpts = indexes.FullTextPagingOptions{
		Limit:        1,
		SearchBefore: []string{"doc3"},
	}
	reqBytes, err = json.Marshal(req)
	require.NoError(t, err)

	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextSearchOrderByID(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for _, key := range []string{"doc3", "doc1", "doc2"} {
		docJSON, err := json.Marshal(map[string]any{"content": "alpha"})
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	desc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "_id", Desc: &desc}},
			Limit:   3,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
		res.BleveSearchResult.Hits[2].ID,
	})
}

func TestZigCoreDB_FullTextSearchOrderByScoreAndCursor(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	writeDoc := func(key string, content string) {
		docJSON, err := json.Marshal(map[string]any{"content": content})
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	writeDoc("doc1", "alpha alpha alpha")
	writeDoc("doc2", "alpha alpha")
	writeDoc("doc3", "alpha")

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	desc := true
	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{
				{Field: "_score", Desc: &desc},
				{Field: "_id", Desc: &asc},
			},
			Limit: 3,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
		res.BleveSearchResult.Hits[2].ID,
	})

	req.BlevePagingOpts.Limit = 1
	req.BlevePagingOpts.SearchAfter = []string{
		strconv.FormatFloat(res.BleveSearchResult.Hits[1].Score, 'f', -1, 64),
		res.BleveSearchResult.Hits[1].ID,
	}
	reqBytes, err = json.Marshal(req)
	require.NoError(t, err)
	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc3", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextSearchOrderByStoredField(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc3": {"content": "alpha", "title": "charlie"},
		"doc1": {"content": "alpha", "title": "alpha"},
		"doc2": {"content": "alpha", "title": "bravo"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}},
			Limit:   3,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
		res.BleveSearchResult.Hits[2].ID,
	})
}

func TestZigCoreDB_FullTextSearchOrderByStoredFieldAndCursor(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 10},
		"doc2": {"content": "alpha", "price": 20},
		"doc3": {"content": "alpha", "price": 30},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{
				{Field: "price", Desc: &asc},
				{Field: "_id", Desc: &asc},
			},
			Limit: 3,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
		res.BleveSearchResult.Hits[2].ID,
	})

	req.BlevePagingOpts.Limit = 1
	req.BlevePagingOpts.SearchAfter = []string{"20", "doc2"}
	reqBytes, err = json.Marshal(req)
	require.NoError(t, err)
	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc3", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextSearchCursorWithoutStableIDTieBreak(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "title": "alpha"},
		"doc2": {"content": "alpha", "title": "bravo"},
		"doc3": {"content": "alpha", "title": "bravo"},
		"doc4": {"content": "alpha", "title": "charlie"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}},
			Limit:   4,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 4)
	assert.Equal(t, []string{"doc1", "doc2", "doc3", "doc4"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
		res.BleveSearchResult.Hits[2].ID,
		res.BleveSearchResult.Hits[3].ID,
	})

	req.BlevePagingOpts.Limit = 2
	req.BlevePagingOpts.SearchAfter = []string{"bravo"}
	reqBytes, err = json.Marshal(req)
	require.NoError(t, err)
	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc4", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextSearchOrderByStringArrayField(t *testing.T) {
	db := openZigSearchTestDBWithSchema(t, compositeSortSearchSchema())
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "tags": []any{"zulu", "alpha"}},
		"doc2": {"content": "alpha", "tags": []any{"bravo"}},
		"doc3": {"content": "alpha", "tags": []any{"charlie", "delta"}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "tags", Desc: &asc}},
			Limit:   3,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
		res.BleveSearchResult.Hits[2].ID,
	})
	assert.Equal(t, []string{"alpha"}, res.BleveSearchResult.Hits[0].Sort)
	assert.Equal(t, []string{"bravo"}, res.BleveSearchResult.Hits[1].Sort)
	assert.Equal(t, []string{"charlie"}, res.BleveSearchResult.Hits[2].Sort)

	req.BlevePagingOpts.Limit = 2
	req.BlevePagingOpts.SearchAfter = []string{"bravo"}
	reqBytes, err = json.Marshal(req)
	require.NoError(t, err)
	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc3", res.BleveSearchResult.Hits[0].ID)
	assert.Equal(t, []string{"charlie"}, res.BleveSearchResult.Hits[0].Sort)
}

func TestZigCoreDB_FullTextSearchOrderByCompositeFieldUsesBleveSentinel(t *testing.T) {
	db := openZigSearchTestDBWithSchema(t, compositeSortSearchSchema())
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "meta": map[string]any{"priority": 2}, "scores": []any{9, 3}, "mixed": "zulu"},
		"doc2": {"content": "alpha", "meta": map[string]any{"priority": 1}, "scores": []any{5}, "mixed": []any{"bravo"}},
		"doc3": {"content": "alpha", "meta": map[string]any{"priority": 3}, "scores": []any{7, 8}, "mixed": map[string]any{"priority": 1}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	asc := false

	for _, field := range []string{"meta", "scores", "mixed"} {
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
			Limit:              10,
			BlevePagingOpts: indexes.FullTextPagingOptions{
				OrderBy: []indexes.SortField{{Field: field, Desc: &asc}},
				Limit:   3,
			},
		}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := db.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.NotNil(t, res.BleveSearchResult)
		require.Len(t, res.BleveSearchResult.Hits, 3)
		for _, hit := range res.BleveSearchResult.Hits {
			assert.Equal(t, []string{bleveCompositeSortSentinel}, hit.Sort)
		}

		req.BlevePagingOpts.SearchAfter = []string{bleveCompositeSortSentinel}
		reqBytes, err = json.Marshal(req)
		require.NoError(t, err)
		resBytes, err = db.Search(ctx, reqBytes)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.NotNil(t, res.BleveSearchResult)
		assert.Empty(t, res.BleveSearchResult.Hits)
	}
}

func TestZigCoreDB_FullTextSearchOrderByMixedScalarFieldUsesBleveSentinel(t *testing.T) {
	db := openZigSearchTestDBWithSchema(t, compositeSortSearchSchema())
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "mixed_scalar": "zulu"},
		"doc2": {"content": "alpha", "mixed_scalar": 5},
		"doc3": {"content": "alpha", "mixed_scalar": true},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "mixed_scalar", Desc: &asc}},
			Limit:   3,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)
	for _, hit := range res.BleveSearchResult.Hits {
		assert.Equal(t, []string{bleveCompositeSortSentinel}, hit.Sort)
	}
}

func TestZigCoreDB_FullTextSearchOrderByStoredFieldWithMissingValues(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "title": "alpha"},
		"doc2": {"content": "alpha"},
		"doc3": {"content": "alpha", "title": "bravo"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
			Limit:   3,
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 3)
	assert.Equal(t, []string{"doc1", "doc3", "doc2"}, []string{
		res.BleveSearchResult.Hits[0].ID,
		res.BleveSearchResult.Hits[1].ID,
		res.BleveSearchResult.Hits[2].ID,
	})
}

func TestZigCoreDB_ScanFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	for key, doc := range map[string]map[string]any{
		"a": {"content": "alpha hello", "title": "keep"},
		"b": {"content": "beta world", "title": "skip"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}

	result, err := db.Scan(ctx, nil, []byte{0xFF}, ScanOptions{
		IncludeDocuments: true,
		FilterQuery:      json.RawMessage(`{"match":"hello","field":"content"}`),
		Limit:            10,
	})
	require.NoError(t, err)
	require.Len(t, result.Hashes, 1)
	require.Len(t, result.Documents, 1)
	_, ok := result.Documents["a"]
	assert.True(t, ok)
}

func TestZigCoreDB_ScanFilterQueryWithoutDocuments(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	for key, doc := range map[string]map[string]any{
		"a": {"content": "alpha hello", "title": "keep"},
		"b": {"content": "beta world", "title": "skip"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}

	result, err := db.Scan(ctx, nil, []byte{0xFF}, ScanOptions{
		IncludeDocuments: false,
		FilterQuery:      json.RawMessage(`{"match":"hello","field":"content"}`),
		Limit:            10,
	})
	require.NoError(t, err)
	require.Len(t, result.Hashes, 1)
	require.Nil(t, result.Documents)
	_, ok := result.Hashes["a"]
	assert.True(t, ok)
}

func TestZigCoreDB_ListTxnRecordsAndIntents(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())
	participants := [][]byte{{1, 0, 0, 0, 0, 0, 0, 0}}
	require.NoError(t, db.InitTransaction(ctx, InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    timestamp,
		Participants: participants,
	}.Build()))

	batchOp := BatchOp_builder{
		Writes: []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
		},
	}.Build()
	require.NoError(t, db.WriteIntent(ctx, WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: []byte{9, 0, 0, 0, 0, 0, 0, 0},
		Batch:            batchOp,
	}.Build()))

	records, err := db.ListTxnRecords(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, txnID[:], records[0].TxnID)
	assert.Equal(t, participants, records[0].Participants)
	assert.Equal(t, int32(TxnStatusPending), records[0].Status)

	intents, err := db.ListTxnIntents(ctx)
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, txnID[:], intents[0].TxnID)
	assert.Equal(t, []byte("key1"), intents[0].UserKey)
	assert.Equal(t, []byte("value1"), intents[0].Value)
	assert.False(t, intents[0].IsDelete)
}

func TestZigCoreDB_FindMedianKey(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		docJSON, err := json.Marshal(map[string]any{"content": key})
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}

	// Also write transaction metadata; FindMedianKey should ignore it.
	txnID := uuid.New()
	require.NoError(t, db.InitTransaction(ctx, InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    uint64(time.Now().Unix()),
		Participants: [][]byte{{1}},
	}.Build()))

	key, err := db.FindMedianKey()
	require.NoError(t, err)
	assert.Equal(t, []byte("c"), key)
}

func TestZigCoreDB_UpdateSchema(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	updated := *db.schema
	updated.DynamicTemplates = []schema.DynamicTemplate{
		{
			Name:  "tag_text",
			Match: "tag_*",
			Mapping: schema.TemplateFieldMapping{
				Type:     schema.AntflyTypeText,
				Index:    true,
				Store:    true,
				Analyzer: "standard",
			},
		},
	}
	require.NoError(t, db.UpdateSchema(&updated))

	docJSON, err := json.Marshal(map[string]any{
		"title":    "base",
		"tag_name": "alpha dynamic",
	})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	q := query.NewMatchQuery("alpha")
	q.SetField("tag_name")
	reqBytes, err := json.Marshal(indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
	})
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestZigCoreDB_FullTextTermsAggregation(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "title": "red"},
		"doc2": {"content": "alpha", "title": "red"},
		"doc3": {"content": "alpha", "title": "blue"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {
				Type:  "terms",
				Field: "title",
				Size:  5,
			},
			"count_all": {
				Type: "count",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "titles")
	require.Contains(t, res.AggregationResults, "count_all")
	assert.Equal(t, float64(3), res.AggregationResults["count_all"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 2)
	assert.Equal(t, "red", res.AggregationResults["titles"].Buckets[0].Key)
	assert.Equal(t, int64(2), res.AggregationResults["titles"].Buckets[0].Count)
}

func TestZigCoreDB_FullTextMetricAndRangeAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "title": "red", "price": 10, "created_at": "2026-01-01T00:00:00Z"},
		"doc2": {"content": "alpha", "title": "red", "price": 20, "created_at": "2026-02-01T00:00:00Z"},
		"doc3": {"content": "alpha", "title": "blue", "price": 30, "created_at": "2026-03-01T00:00:00Z"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	startFeb := "2026-02-01T00:00:00Z"
	endApr := "2026-04-01T00:00:00Z"
	fifteen := 15.0
	thirtyFive := 35.0
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"sum_price":   {Type: "sum", Field: "price"},
			"stats_price": {Type: "stats", Field: "price"},
			"card_title":  {Type: "cardinality", Field: "title"},
			"pattern_titles": {
				Type:        "terms",
				Field:       "title",
				TermPattern: "^r",
			},
			"price_ranges": {
				Type:  "range",
				Field: "price",
				NumericRanges: []*indexes.NumericRange{
					{Name: "expensive", Start: &fifteen, End: &thirtyFive},
				},
				Aggregations: indexes.AggregationRequests{
					"colors": {Type: "terms", Field: "title"},
				},
			},
			"created_ranges": {
				Type:  "range",
				Field: "created_at",
				DateTimeRanges: []*indexes.DateTimeRange{
					{Name: "late", Start: &startFeb, End: &endApr},
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	assert.Equal(t, float64(60), res.AggregationResults["sum_price"].Value)

	statsMap, ok := res.AggregationResults["stats_price"].Value.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(3), statsMap["count"])
	assert.Equal(t, float64(20), statsMap["avg"])
	assert.Equal(t, float64(10), statsMap["min"])
	assert.Equal(t, float64(30), statsMap["max"])

	cardMap, ok := res.AggregationResults["card_title"].Value.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(2), cardMap["value"])

	require.Len(t, res.AggregationResults["pattern_titles"].Buckets, 1)
	assert.Equal(t, "red", res.AggregationResults["pattern_titles"].Buckets[0].Key)

	require.Len(t, res.AggregationResults["price_ranges"].Buckets, 1)
	priceBucket := res.AggregationResults["price_ranges"].Buckets[0]
	assert.Equal(t, "expensive", priceBucket.Key)
	assert.Equal(t, int64(2), priceBucket.Count)
	require.Contains(t, priceBucket.Aggregations, "colors")
	require.Len(t, priceBucket.Aggregations["colors"].Buckets, 2)

	require.Len(t, res.AggregationResults["created_ranges"].Buckets, 1)
	assert.Equal(t, int64(2), res.AggregationResults["created_ranges"].Buckets[0].Count)
}

func TestZigCoreDB_FullTextHistogramAggregation(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 5.0},
		"doc2": {"content": "alpha", "price": 15.0},
		"doc3": {"content": "alpha", "price": 25.0},
		"doc4": {"content": "alpha", "price": 35.0},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"price_hist": {
				Type:     "histogram",
				Field:    "price",
				Interval: 10,
			},
			"sum_price": {
				Type:  "sum",
				Field: "price",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	assert.Equal(t, float64(80), res.AggregationResults["sum_price"].Value)
	require.Contains(t, res.AggregationResults, "price_hist")
	require.Len(t, res.AggregationResults["price_hist"].Buckets, 4)
	assert.Equal(t, float64(0), res.AggregationResults["price_hist"].Buckets[0].Key)
	assert.Equal(t, int64(1), res.AggregationResults["price_hist"].Buckets[0].Count)
	assert.Equal(t, float64(30), res.AggregationResults["price_hist"].Buckets[3].Key)
	assert.Equal(t, int64(1), res.AggregationResults["price_hist"].Buckets[3].Count)
}

func TestZigCoreDB_FullTextDateHistogramAggregation(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "created_at": "2025-01-01T10:00:00Z", "price": 5.0},
		"doc2": {"content": "alpha", "created_at": "2025-01-01T18:00:00Z", "price": 15.0},
		"doc3": {"content": "alpha", "created_at": "2025-01-02T09:00:00Z", "price": 25.0},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"created_daily": {
				Type:             "date_histogram",
				Field:            "created_at",
				CalendarInterval: "day",
			},
			"sum_price": {
				Type:  "sum",
				Field: "price",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	assert.Equal(t, float64(45), res.AggregationResults["sum_price"].Value)
	require.Contains(t, res.AggregationResults, "created_daily")
	require.Len(t, res.AggregationResults["created_daily"].Buckets, 2)
	assert.Equal(t, "2025-01-01T00:00:00Z", res.AggregationResults["created_daily"].Buckets[0].Key)
	assert.Equal(t, int64(2), res.AggregationResults["created_daily"].Buckets[0].Count)
	assert.Equal(t, "2025-01-02T00:00:00Z", res.AggregationResults["created_daily"].Buckets[1].Key)
	assert.Equal(t, int64(1), res.AggregationResults["created_daily"].Buckets[1].Count)
}

func TestZigCoreDB_FullTextHistogramAggregationMinDocCountZeroFillsGaps(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 5.0},
		"doc2": {"content": "alpha", "price": 35.0},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"price_hist": {
				Type:        "histogram",
				Field:       "price",
				Interval:    10,
				MinDocCount: 0,
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "price_hist")
	require.Len(t, res.AggregationResults["price_hist"].Buckets, 4)
	assert.Equal(t, float64(0), res.AggregationResults["price_hist"].Buckets[0].Key)
	assert.Equal(t, int64(1), res.AggregationResults["price_hist"].Buckets[0].Count)
	assert.Equal(t, float64(10), res.AggregationResults["price_hist"].Buckets[1].Key)
	assert.Equal(t, int64(0), res.AggregationResults["price_hist"].Buckets[1].Count)
	assert.Equal(t, float64(20), res.AggregationResults["price_hist"].Buckets[2].Key)
	assert.Equal(t, int64(0), res.AggregationResults["price_hist"].Buckets[2].Count)
	assert.Equal(t, float64(30), res.AggregationResults["price_hist"].Buckets[3].Key)
	assert.Equal(t, int64(1), res.AggregationResults["price_hist"].Buckets[3].Count)
}

func TestZigCoreDB_FullTextDateHistogramMinDocCountZeroFillsGaps(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "created_at": "2025-01-01T10:00:00Z"},
		"doc2": {"content": "alpha", "created_at": "2025-01-03T09:00:00Z"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"created_daily": {
				Type:             "date_histogram",
				Field:            "created_at",
				CalendarInterval: "day",
				MinDocCount:      0,
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "created_daily")
	require.Len(t, res.AggregationResults["created_daily"].Buckets, 3)
	assert.Equal(t, "2025-01-01T00:00:00Z", res.AggregationResults["created_daily"].Buckets[0].Key)
	assert.Equal(t, int64(1), res.AggregationResults["created_daily"].Buckets[0].Count)
	assert.Equal(t, "2025-01-02T00:00:00Z", res.AggregationResults["created_daily"].Buckets[1].Key)
	assert.Equal(t, int64(0), res.AggregationResults["created_daily"].Buckets[1].Count)
	assert.Equal(t, "2025-01-03T00:00:00Z", res.AggregationResults["created_daily"].Buckets[2].Key)
	assert.Equal(t, int64(1), res.AggregationResults["created_daily"].Buckets[2].Count)
}

func TestZigCoreDB_FullTextGeoDistanceAggregation(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {
			"content":  "coffee place",
			"location": map[string]any{"lat": 37.7749, "lon": -122.4194}, // SF
		},
		"doc2": {
			"content":  "coffee place",
			"location": map[string]any{"lat": 37.8044, "lon": -122.2711}, // Oakland
		},
		"doc3": {
			"content":  "coffee place",
			"location": map[string]any{"lat": 38.5816, "lon": -121.4944}, // Sacramento
		},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchCoffee := query.NewMatchQuery("coffee")
	matchCoffee.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchCoffee),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"geo_ranges": {
				Type:         "range",
				Field:        "location",
				CenterLat:    37.7749,
				CenterLon:    -122.4194,
				DistanceUnit: "km",
				DistanceRanges: []*indexes.DistanceRange{
					{Name: "near", From: floatPtr(0), To: floatPtr(20)},
					{Name: "mid", From: floatPtr(20), To: floatPtr(120)},
					{Name: "far", From: floatPtr(120), To: floatPtr(300)},
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "geo_ranges")
	require.Len(t, res.AggregationResults["geo_ranges"].Buckets, 3)
	assert.Equal(t, "near", res.AggregationResults["geo_ranges"].Buckets[0].Key)
	assert.Equal(t, int64(2), res.AggregationResults["geo_ranges"].Buckets[0].Count)
	assert.Equal(t, "mid", res.AggregationResults["geo_ranges"].Buckets[1].Key)
	assert.Equal(t, int64(0), res.AggregationResults["geo_ranges"].Buckets[1].Count)
	assert.Equal(t, "far", res.AggregationResults["geo_ranges"].Buckets[2].Key)
	assert.Equal(t, int64(1), res.AggregationResults["geo_ranges"].Buckets[2].Count)
}

func TestZigCoreDB_FullTextSignificantTermsAggregation(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "database nosql scalability"},
		"doc2": {"content": "database nosql replication"},
		"doc3": {"content": "programming language compiler"},
		"doc4": {"content": "programming tutorial compiler"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchDatabase := query.NewMatchQuery("database")
	matchDatabase.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchDatabase),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"sig_terms": {
				Type:                  "significant_terms",
				Field:                 "content",
				Size:                  5,
				SignificanceAlgorithm: "jlh",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "sig_terms")

	agg := res.AggregationResults["sig_terms"]
	require.NotNil(t, agg.Metadata)
	assert.Equal(t, "jlh", agg.Metadata["algorithm"])
	require.NotEmpty(t, agg.Buckets)

	var foundNosql bool
	for _, bucket := range agg.Buckets {
		if bucket.Key == "nosql" {
			foundNosql = true
			assert.Equal(t, int64(2), bucket.Count)
			require.NotNil(t, bucket.Metadata)
			assert.EqualValues(t, 2, bucket.Metadata["bg_count"])
			score, ok := bucket.Metadata["score"].(float64)
			require.True(t, ok)
			assert.Greater(t, score, 0.0)
		}
	}
	assert.True(t, foundNosql)

	_, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)

	detailed, err := db.DetailedStats()
	require.NoError(t, err)
	require.NotNil(t, detailed)
	assert.GreaterOrEqual(t, detailed.TermDocFreqCacheMisses, uint64(1))
	assert.GreaterOrEqual(t, detailed.TermDocFreqCacheHits, uint64(1))
}

func TestZigCoreDB_FullTextSignificantTermsBackgroundFilter(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "database nosql scalability"},
		"doc2": {"content": "database nosql replication"},
		"doc3": {"content": "database sql indexing"},
		"doc4": {"content": "programming sql compiler"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchDatabase := query.NewMatchQuery("database")
	matchDatabase.SetField("content")
	backgroundMatch := query.NewMatchQuery("scalability")
	backgroundMatch.SetField("content")
	backgroundJSON, err := json.Marshal(backgroundMatch)
	require.NoError(t, err)

	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchDatabase),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"sig_terms": {
				Type:                  "significant_terms",
				Field:                 "content",
				Size:                  5,
				SignificanceAlgorithm: "jlh",
				BackgroundFilter:      backgroundJSON,
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	agg := res.AggregationResults["sig_terms"]
	require.NotNil(t, agg)
	require.NotNil(t, agg.Metadata)
	assert.EqualValues(t, 1, agg.Metadata["bg_doc_count"])

	var foundNosql bool
	for _, bucket := range agg.Buckets {
		if bucket.Key == "nosql" {
			foundNosql = true
			require.NotNil(t, bucket.Metadata)
			assert.EqualValues(t, 1, bucket.Metadata["bg_count"])
		}
	}
	assert.True(t, foundNosql)
}

func TestZigCoreDB_FullTextRejectsUnsupportedSignificantTermsBackgroundFilter(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	docJSON, err := json.Marshal(map[string]any{"content": "alpha beta gamma"})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	phraseFilter := query.NewMatchPhraseQuery("alpha beta")
	phraseFilter.SetField("content")
	backgroundJSON, err := json.Marshal(phraseFilter)
	require.NoError(t, err)

	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"sig_terms": {
				Type:             "significant_terms",
				Field:            "content",
				BackgroundFilter: backgroundJSON,
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "background_filter query type")
}

func TestZigCoreDB_FullTextHistogramPipelineAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 5},
		"doc2": {"content": "alpha", "price": 7},
		"doc3": {"content": "alpha", "price": 15},
		"doc4": {"content": "alpha", "price": 25},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"prices": {
				Type:     "histogram",
				Field:    "price",
				Interval: 10,
				Aggregations: indexes.AggregationRequests{
					"running_docs": {
						Type:       "cumulative_sum",
						BucketPath: "_count",
					},
					"delta_docs": {
						Type:       "derivative",
						BucketPath: "_count",
					},
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	agg := res.AggregationResults["prices"]
	require.NotNil(t, agg)
	require.Len(t, agg.Buckets, 3)

	assert.Equal(t, float64(0), agg.Buckets[0].Key)
	assert.Equal(t, int64(2), agg.Buckets[0].Count)
	require.Contains(t, agg.Buckets[0].Aggregations, "running_docs")
	assert.Equal(t, float64(2), agg.Buckets[0].Aggregations["running_docs"].Value)
	require.Contains(t, agg.Buckets[0].Aggregations, "delta_docs")
	assert.Nil(t, agg.Buckets[0].Aggregations["delta_docs"].Value)

	assert.Equal(t, float64(10), agg.Buckets[1].Key)
	assert.Equal(t, int64(1), agg.Buckets[1].Count)
	assert.Equal(t, float64(3), agg.Buckets[1].Aggregations["running_docs"].Value)
	assert.Equal(t, float64(-1), agg.Buckets[1].Aggregations["delta_docs"].Value)

	assert.Equal(t, float64(20), agg.Buckets[2].Key)
	assert.Equal(t, int64(1), agg.Buckets[2].Count)
	assert.Equal(t, float64(4), agg.Buckets[2].Aggregations["running_docs"].Value)
	assert.Equal(t, float64(0), agg.Buckets[2].Aggregations["delta_docs"].Value)
}

func TestZigCoreDB_FullTextHistogramBucketSortPipeline(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 5},
		"doc2": {"content": "alpha", "price": 12},
		"doc3": {"content": "alpha", "price": 14},
		"doc4": {"content": "alpha", "price": 18},
		"doc5": {"content": "alpha", "price": 22},
		"doc6": {"content": "alpha", "price": 24},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"prices": {
				Type:     "histogram",
				Field:    "price",
				Interval: 10,
				Aggregations: indexes.AggregationRequests{
					"sorted": {
						Type:            "bucket_sort",
						BucketPath:      "_count",
						BucketSortOrder: "desc",
						BucketFrom:      1,
						Size:            1,
					},
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	agg := res.AggregationResults["prices"]
	require.NotNil(t, agg)
	require.Len(t, agg.Buckets, 1)
	assert.Equal(t, float64(20), agg.Buckets[0].Key)
	assert.Equal(t, int64(2), agg.Buckets[0].Count)
}

func TestZigCoreDB_FullTextHistogramMovingAveragePipeline(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 5},
		"doc2": {"content": "alpha", "price": 7},
		"doc3": {"content": "alpha", "price": 15},
		"doc4": {"content": "alpha", "price": 25},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"prices": {
				Type:     "histogram",
				Field:    "price",
				Interval: 10,
				Aggregations: indexes.AggregationRequests{
					"moving_docs": {
						Type:              "moving_avg",
						BucketPath:        "_count",
						PipelineWindow:    2,
						PipelineGapPolicy: "skip",
					},
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	agg := res.AggregationResults["prices"]
	require.NotNil(t, agg)
	require.Len(t, agg.Buckets, 3)
	require.Contains(t, agg.Buckets[0].Aggregations, "moving_docs")
	require.Contains(t, agg.Buckets[1].Aggregations, "moving_docs")
	require.Contains(t, agg.Buckets[2].Aggregations, "moving_docs")
	assert.Equal(t, float64(2), agg.Buckets[0].Aggregations["moving_docs"].Value)
	assert.Equal(t, float64(1.5), agg.Buckets[1].Aggregations["moving_docs"].Value)
	assert.Equal(t, float64(1), agg.Buckets[2].Aggregations["moving_docs"].Value)
}

func TestZigCoreDB_FullTextBucketMetricPipelines(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 5},
		"doc2": {"content": "alpha", "price": 7},
		"doc3": {"content": "alpha", "price": 15},
		"doc4": {"content": "alpha", "price": 25},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"prices": {
				Type:     "histogram",
				Field:    "price",
				Interval: 10,
			},
			"sum_docs": {
				Type:       "sum_bucket",
				BucketPath: "prices>_count",
			},
			"avg_docs": {
				Type:       "avg_bucket",
				BucketPath: "prices>_count",
			},
			"min_docs": {
				Type:       "min_bucket",
				BucketPath: "prices>_count",
			},
			"max_docs": {
				Type:       "max_bucket",
				BucketPath: "prices>_count",
			},
			"stats_docs": {
				Type:       "stats_bucket",
				BucketPath: "prices>_count",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.AggregationResults, "sum_docs")
	require.Contains(t, res.AggregationResults, "avg_docs")
	require.Contains(t, res.AggregationResults, "min_docs")
	require.Contains(t, res.AggregationResults, "max_docs")
	require.Contains(t, res.AggregationResults, "stats_docs")
	assert.Equal(t, float64(4), res.AggregationResults["sum_docs"].Value)
	assert.Equal(t, float64(4)/3.0, res.AggregationResults["avg_docs"].Value)
	assert.Equal(t, float64(1), res.AggregationResults["min_docs"].Value)
	assert.Equal(t, float64(2), res.AggregationResults["max_docs"].Value)
	stats, ok := res.AggregationResults["stats_docs"].Value.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(3), stats["count"])
	assert.Equal(t, float64(4), stats["sum"])
	assert.Equal(t, float64(4)/3.0, stats["avg"])
	assert.Equal(t, float64(1), stats["min"])
	assert.Equal(t, float64(2), stats["max"])
}

func TestZigCoreDB_FullTextDateRangeAggregation(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "created_at": "2024-01-10T00:00:00Z"},
		"doc2": {"content": "alpha", "created_at": "2024-05-15T00:00:00Z"},
		"doc3": {"content": "alpha", "created_at": "2024-11-01T00:00:00Z"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	h1Start := "2024-01-01T00:00:00Z"
	h1End := "2024-07-01T00:00:00Z"
	h2Start := "2024-07-01T00:00:00Z"
	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"created_ranges": {
				Type:  "date_range",
				Field: "created_at",
				DateTimeRanges: []*indexes.DateTimeRange{
					{Name: "H1", Start: &h1Start, End: &h1End},
					{Name: "H2", Start: &h2Start},
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	agg := res.AggregationResults["created_ranges"]
	require.NotNil(t, agg)
	require.Len(t, agg.Buckets, 2)
	assert.Equal(t, "H1", agg.Buckets[0].Key)
	assert.Equal(t, int64(2), agg.Buckets[0].Count)
	assert.Equal(t, "H2", agg.Buckets[1].Key)
	assert.Equal(t, int64(1), agg.Buckets[1].Count)
}

func TestZigCoreDB_FullTextGeohashGridAggregation(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "location": map[string]any{"lat": 37.7749, "lon": -122.4194}},
		"doc2": {"content": "alpha", "location": map[string]any{"lat": 37.7750, "lon": -122.4195}},
		"doc3": {"content": "alpha", "location": map[string]any{"lat": 40.7128, "lon": -74.0060}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"geo_grid": {
				Type:             "geohash_grid",
				Field:            "location",
				GeohashPrecision: 5,
				Size:             10,
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	agg := res.AggregationResults["geo_grid"]
	require.NotNil(t, agg)
	require.Len(t, agg.Buckets, 2)
	assert.Equal(t, int64(2), agg.Buckets[0].Count)
	assert.Equal(t, int64(1), agg.Buckets[1].Count)
}

func TestZigCoreDB_FullTextExtendedAndPercentilesBucketPipelines(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "price": 5},
		"doc2": {"content": "alpha", "price": 7},
		"doc3": {"content": "alpha", "price": 15},
		"doc4": {"content": "alpha", "price": 25},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"prices": {
				Type:     "histogram",
				Field:    "price",
				Interval: 10,
			},
			"percentiles_docs": {
				Type:       "percentiles_bucket",
				BucketPath: "prices>_count",
			},
			"extended_docs": {
				Type:       "extended_stats_bucket",
				BucketPath: "prices>_count",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	percentiles, ok := res.AggregationResults["percentiles_docs"].Value.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), percentiles["1"])
	assert.Equal(t, float64(1), percentiles["25"])
	assert.Equal(t, float64(1), percentiles["50"])
	assert.Equal(t, float64(1.5), percentiles["75"])
	assert.Equal(t, float64(1.9), percentiles["95"])
	extended, ok := res.AggregationResults["extended_docs"].Value.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(3), extended["count"])
	assert.Equal(t, float64(4), extended["sum"])
	assert.Equal(t, float64(6), extended["sum_of_squares"])
	assert.Equal(t, float64(4)/3.0, extended["avg"])
	assert.Equal(t, float64(1), extended["min"])
	assert.Equal(t, float64(2), extended["max"])
	assert.Equal(t, float64(2)/9.0, extended["variance"])
	assert.Equal(t, float64(0.4714045207910317), extended["std_deviation"])
}

func TestZigCoreDB_FullTextRejectsUnsupportedLocalAggregationFallback(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	docJSON, err := json.Marshal(map[string]any{"content": "alpha"})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"unsupported": {
				Type:  "top_hits",
				Field: "content",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Search aggregations outside narrowed full-text path")
}

func TestZigCoreDB_GraphSearchesWithResultRefs(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"paper1": {
			"title":   "Machine Learning Basics",
			"content": "Introduction to machine learning algorithms",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "paper2", "weight": 1.0},
						map[string]any{"target": "paper3", "weight": 0.9},
					},
				},
			},
		},
		"paper2": {
			"title":   "Deep Learning",
			"content": "Neural networks and deep learning",
		},
		"paper3": {
			"title":   "Machine Learning Applications",
			"content": "Practical applications of machine learning",
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	matchML := query.NewMatchQuery("machine learning")
	matchML.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchML),
		Limit:              10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"citations": {
				Type:      "traverse",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$full_text_results",
					Limit:     5,
				},
				IncludeDocuments: true,
				Params: indexes.GraphQueryParams{
					MaxDepth:  1,
					Direction: "out",
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.NotNil(t, res.GraphResults)
	require.Contains(t, res.GraphResults, "citations")
	assert.Positive(t, res.GraphResults["citations"].Total)
	for _, node := range res.GraphResults["citations"].Nodes {
		assert.NotNil(t, node.Document)
	}
}

func TestZigCoreDB_FullTextGraphSearchesWithAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title":   "red",
			"content": "shared alpha",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "b", "weight": 1.0}},
				},
			},
		},
		"b": {
			"title":   "red",
			"content": "shared alpha",
		},
		"c": {
			"title":   "blue",
			"content": "shared alpha",
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	matchShared := query.NewMatchQuery("shared")
	matchShared.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchShared),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {
				Type:  "terms",
				Field: "title",
				Size:  5,
			},
			"count_all": {
				Type: "count",
			},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      indexes.GraphQueryTypeNeighbors,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$full_text_results",
					Limit:     3,
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "titles")
	require.Contains(t, res.AggregationResults, "count_all")
	assert.Equal(t, float64(3), res.AggregationResults["count_all"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 2)
	assert.Equal(t, "red", res.AggregationResults["titles"].Buckets[0].Key)
	assert.Equal(t, int64(2), res.AggregationResults["titles"].Buckets[0].Count)
	require.Contains(t, res.GraphResults, "neighbors")
}

func TestZigCoreDB_FullTextGraphFusionAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title": "root",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"b": {"title": "shared", "content": "shared result"},
		"c": {"title": "graph-only", "content": "other result"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelFullText))
	time.Sleep(500 * time.Millisecond)

	matchShared := query.NewMatchQuery("shared")
	matchShared.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchShared),
		Limit:              10,
		AggregationRequests: indexes.AggregationRequests{
			"count":  {Type: "count"},
			"titles": {Type: "terms", Field: "title", Size: 5},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      indexes.GraphQueryTypeNeighbors,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
		ExpandStrategy: "intersection",
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 1)
	require.Contains(t, res.AggregationResults, "count")
	assert.Equal(t, float64(1), res.AggregationResults["count"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 1)
	assert.Equal(t, "shared", res.AggregationResults["titles"].Buckets[0].Key)
}

func TestZigCoreDB_GraphSearchChainAndFusion(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title":   "Node A shared",
			"content": "shared node a",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "b", "weight": 1.0}},
				},
			},
		},
		"b": {
			"title":   "Node B shared",
			"content": "shared node b",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "c", "weight": 1.0}},
				},
			},
		},
		"c": {
			"title":   "Node C",
			"content": "other node c",
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	matchShared := query.NewMatchQuery("shared")
	matchShared.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchShared),
		Limit:              10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"first_hop": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
			"second_hop": {
				Type:      "neighbors",
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$graph_results.first_hop",
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
		},
		ExpandStrategy: "intersection",
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "first_hop")
	require.Contains(t, res.GraphResults, "second_hop")
	assert.Equal(t, 1, res.GraphResults["first_hop"].Total)
	assert.Equal(t, 1, res.GraphResults["second_hop"].Total)
	require.NotNil(t, res.FusionResult)
	assert.NotEmpty(t, res.FusionResult.Hits)
}

func TestZigCoreDB_GraphKShortestPaths(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"b": {
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "d", "weight": 1.0}},
				},
			},
		},
		"c": {
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "d", "weight": 1.0}},
				},
			},
		},
		"d": {},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"paths": {
				Type:      indexes.GraphQueryTypeKShortestPaths,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				TargetNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
				},
				Params: indexes.GraphQueryParams{
					K:          2,
					Direction:  "out",
					WeightMode: "min_hops",
					MaxDepth:   4,
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "paths")
	require.Len(t, res.GraphResults["paths"].Paths, 2)
	assert.Equal(t, 2, res.GraphResults["paths"].Total)
	assert.Equal(t, []string{"a", "b", "d"}, res.GraphResults["paths"].Paths[0].Nodes)
	assert.Equal(t, []string{"a", "c", "d"}, res.GraphResults["paths"].Paths[1].Nodes)
}

func TestZigCoreDB_MixedFullTextAndGraphKShortestPaths(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title":   "Node A",
			"content": "shared graph source",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"b": {
			"title":   "Node B",
			"content": "shared graph branch",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "d", "weight": 1.0}},
				},
			},
		},
		"c": {
			"title":   "Node C",
			"content": "shared graph branch",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "d", "weight": 1.0}},
				},
			},
		},
		"d": {
			"title":   "Node D",
			"content": "shared graph target",
		},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	matchShared := query.NewMatchQuery("shared")
	matchShared.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchShared),
		Limit:              10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"paths": {
				Type:      indexes.GraphQueryTypeKShortestPaths,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				TargetNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
				},
				Params: indexes.GraphQueryParams{
					K:          2,
					Direction:  "out",
					WeightMode: "min_hops",
					MaxDepth:   4,
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Contains(t, res.GraphResults, "paths")
	require.Len(t, res.GraphResults["paths"].Paths, 2)
	assert.Equal(t, []string{"a", "b", "d"}, res.GraphResults["paths"].Paths[0].Nodes)
	assert.Equal(t, []string{"a", "c", "d"}, res.GraphResults["paths"].Paths[1].Nodes)
}

func TestZigCoreDB_GraphPattern(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title": "Node A",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "b", "weight": 1.0}},
				},
			},
		},
		"b": {
			"title": "Node B",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "c", "weight": 1.0}},
				},
			},
		},
		"c": {"title": "Node C"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:             indexes.GraphQueryTypePattern,
				IndexName:        "citations",
				IncludeDocuments: true,
				Fields:           []string{"title"},
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				ReturnAliases: []string{"src", "dst"},
				Pattern: []indexes.PatternStep{
					{Alias: "src"},
					{
						Alias: "mid",
						Edge: indexes.PatternEdgeStep{
							Types:     []string{"cites"},
							Direction: "out",
						},
						NodeFilter: indexes.NodeFilter{FilterPrefix: "b"},
					},
					{
						Alias: "dst",
						Edge: indexes.PatternEdgeStep{
							Types:     []string{"cites"},
							Direction: "out",
						},
					},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "pattern")
	require.Len(t, res.GraphResults["pattern"].Matches, 1)
	match := res.GraphResults["pattern"].Matches[0]
	require.Len(t, match.Bindings, 2)
	assert.Contains(t, match.Bindings, "src")
	assert.Contains(t, match.Bindings, "dst")
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("a")), match.Bindings["src"].Key)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("c")), match.Bindings["dst"].Key)
	assert.Equal(t, "Node A", match.Bindings["src"].Document["title"])
	assert.Equal(t, "Node C", match.Bindings["dst"].Document["title"])
	require.Len(t, match.Path, 2)
	assert.Equal(t, "cites", match.Path[0].Type)
	assert.Equal(t, "cites", match.Path[1].Type)
}

func TestZigCoreDB_GraphPatternSupportsFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title":   "Node A",
			"content": "shared pattern start",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"b": {"title": "Node B", "content": "shared pattern middle"},
		"c": {"title": "Other C", "content": "shared pattern middle"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	matchShared := query.NewMatchQuery("shared")
	matchShared.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchShared),
		Limit:              10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:             indexes.GraphQueryTypePattern,
				IndexName:        "citations",
				IncludeDocuments: true,
				Fields:           []string{"title"},
				StartNodes: indexes.GraphNodeSelector{
					ResultRef: "$full_text_results",
					Limit:     5,
				},
				ReturnAliases: []string{"src", "dst"},
				Pattern: []indexes.PatternStep{
					{
						Alias: "src",
						NodeFilter: indexes.NodeFilter{
							FilterQuery: map[string]any{"term": map[string]any{"title": "Node A"}},
						},
					},
					{
						Alias: "dst",
						Edge: indexes.PatternEdgeStep{
							Types:     []string{"cites"},
							Direction: "out",
						},
						NodeFilter: indexes.NodeFilter{
							FilterQuery: map[string]any{"match": map[string]any{"title": "Node B"}},
						},
					},
				},
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "pattern")
	require.Len(t, res.GraphResults["pattern"].Matches, 1)
	match := res.GraphResults["pattern"].Matches[0]
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("a")), match.Bindings["src"].Key)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("b")), match.Bindings["dst"].Key)
	assert.Equal(t, "Node A", match.Bindings["src"].Document["title"])
	assert.Equal(t, "Node B", match.Bindings["dst"].Document["title"])
}

func TestZigCoreDB_GraphPatternSupportsMatchNoneFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docJSON, err := json.Marshal(map[string]any{
		"title": "Node A",
	})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("a"), docJSON}}, nil, Op_SyncLevelWrite))

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:      indexes.GraphQueryTypePattern,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Pattern: []indexes.PatternStep{
					{
						Alias: "src",
						NodeFilter: indexes.NodeFilter{
							FilterQuery: map[string]any{"match_none": map[string]any{}},
						},
					},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "pattern")
	require.Empty(t, res.GraphResults["pattern"].Matches)
}

func TestZigCoreDB_GraphPatternRejectsUnsupportedFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docJSON, err := json.Marshal(map[string]any{"title": "Node A"})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("a"), docJSON}}, nil, Op_SyncLevelWrite))

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:      indexes.GraphQueryTypePattern,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Pattern: []indexes.PatternStep{
					{
						Alias: "src",
						NodeFilter: indexes.NodeFilter{
							FilterQuery: map[string]any{
								"geo_distance": map[string]any{"field": "location", "lon": -122.0, "lat": 37.0, "radius_meters": 1000.0},
							},
						},
					},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Pattern node_filter.filter_query")
}

func TestZigCoreDB_GraphPatternSupportsConjunctiveFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	for key, doc := range map[string]map[string]any{
		"a": {"title": "Node A", "content": "graph systems"},
		"b": {"title": "Node B", "content": "graph"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:             indexes.GraphQueryTypePattern,
				IndexName:        "citations",
				IncludeDocuments: true,
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Pattern: []indexes.PatternStep{
					{
						Alias: "src",
						NodeFilter: indexes.NodeFilter{
							FilterQuery: map[string]any{
								"conjuncts": []any{
									map[string]any{"term": map[string]any{"title": "Node A"}},
									map[string]any{"match": map[string]any{"content": "graph"}},
								},
							},
						},
					},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "pattern")
	require.Len(t, res.GraphResults["pattern"].Matches, 1)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("a")), res.GraphResults["pattern"].Matches[0].Bindings["src"].Key)
}

func TestZigCoreDB_GraphPatternSupportsDisjunctiveFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	for key, doc := range map[string]map[string]any{
		"a": {"title": "Node A", "content": "graph systems"},
		"b": {"title": "Node B", "content": "storage systems"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:             indexes.GraphQueryTypePattern,
				IndexName:        "citations",
				IncludeDocuments: true,
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Pattern: []indexes.PatternStep{
					{
						Alias: "src",
						NodeFilter: indexes.NodeFilter{
							FilterQuery: map[string]any{
								"disjuncts": []any{
									map[string]any{"term": map[string]any{"title": "Node B"}},
									map[string]any{"match": map[string]any{"content": "graph"}},
								},
							},
						},
					},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "pattern")
	require.Len(t, res.GraphResults["pattern"].Matches, 1)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("a")), res.GraphResults["pattern"].Matches[0].Bindings["src"].Key)
}

func TestZigCoreDB_GraphPatternSupportsBooleanFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	for key, doc := range map[string]map[string]any{
		"a": {"title": "Node A", "content": "graph systems"},
		"b": {"title": "Node B", "content": "graph systems"},
		"c": {"title": "Node C", "content": "storage systems"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:             indexes.GraphQueryTypePattern,
				IndexName:        "citations",
				IncludeDocuments: true,
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{
						base64.StdEncoding.EncodeToString([]byte("a")),
						base64.StdEncoding.EncodeToString([]byte("b")),
						base64.StdEncoding.EncodeToString([]byte("c")),
					},
				},
				Pattern: []indexes.PatternStep{
					{
						Alias: "src",
						NodeFilter: indexes.NodeFilter{
							FilterQuery: map[string]any{
								"bool": map[string]any{
									"must": []any{
										map[string]any{"match": map[string]any{"content": "graph"}},
									},
									"should": []any{
										map[string]any{"term": map[string]any{"title": "Node A"}},
										map[string]any{"term": map[string]any{"title": "Node B"}},
									},
									"must_not": []any{
										map[string]any{"term": map[string]any{"title": "Node C"}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "pattern")
	require.Len(t, res.GraphResults["pattern"].Matches, 2)
	keys := []string{
		res.GraphResults["pattern"].Matches[0].Bindings["src"].Key,
		res.GraphResults["pattern"].Matches[1].Bindings["src"].Key,
	}
	assert.ElementsMatch(t, []string{
		base64.StdEncoding.EncodeToString([]byte("a")),
		base64.StdEncoding.EncodeToString([]byte("b")),
	}, keys)
}

func TestZigCoreDB_GraphPatternSupportsDictionaryStyleFilterQueries(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	for key, doc := range map[string]map[string]any{
		"a": {
			"title":        "Node A",
			"content":      "graph systems",
			"views":        5,
			"published_at": "2024-01-02T00:00:00Z",
		},
		"b": {
			"title":        "Node B",
			"content":      "graph storage",
			"views":        15,
			"published_at": "2024-01-03T00:00:00Z",
		},
		"c": {
			"title":        "Other C",
			"content":      "storage only",
			"views":        30,
			"published_at": "2024-02-01T00:00:00Z",
		},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}

	tests := []struct {
		name   string
		filter map[string]any
		want   []string
	}{
		{
			name:   "prefix",
			filter: map[string]any{"prefix": map[string]any{"title": "Node"}},
			want: []string{
				base64.StdEncoding.EncodeToString([]byte("a")),
				base64.StdEncoding.EncodeToString([]byte("b")),
			},
		},
		{
			name:   "wildcard",
			filter: map[string]any{"wildcard": map[string]any{"title": "Node *"}},
			want: []string{
				base64.StdEncoding.EncodeToString([]byte("a")),
				base64.StdEncoding.EncodeToString([]byte("b")),
			},
		},
		{
			name:   "regexp",
			filter: map[string]any{"regexp": map[string]any{"title": "^Node [AB]$"}},
			want: []string{
				base64.StdEncoding.EncodeToString([]byte("a")),
				base64.StdEncoding.EncodeToString([]byte("b")),
			},
		},
		{
			name:   "fuzzy",
			filter: map[string]any{"fuzzy": map[string]any{"title": map[string]any{"query": "Nod A", "max_edits": 1}}},
			want: []string{
				base64.StdEncoding.EncodeToString([]byte("a")),
			},
		},
		{
			name:   "numeric_range",
			filter: map[string]any{"numeric_range": map[string]any{"views": map[string]any{"gte": 10, "lt": 20}}},
			want: []string{
				base64.StdEncoding.EncodeToString([]byte("b")),
			},
		},
		{
			name:   "date_range",
			filter: map[string]any{"date_range": map[string]any{"published_at": map[string]any{"gte": "2024-01-03T00:00:00Z", "lt": "2024-02-01T00:00:00Z"}}},
			want: []string{
				base64.StdEncoding.EncodeToString([]byte("b")),
			},
		},
		{
			name:   "ids",
			filter: map[string]any{"ids": []any{"b", "c"}},
			want: []string{
				base64.StdEncoding.EncodeToString([]byte("b")),
				base64.StdEncoding.EncodeToString([]byte("c")),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &indexes.RemoteIndexSearchRequest{
				Limit: 10,
				GraphSearches: map[string]*indexes.GraphQuery{
					"pattern": {
						Type:             indexes.GraphQueryTypePattern,
						IndexName:        "citations",
						IncludeDocuments: true,
						StartNodes: indexes.GraphNodeSelector{
							Keys: []string{
								base64.StdEncoding.EncodeToString([]byte("a")),
								base64.StdEncoding.EncodeToString([]byte("b")),
								base64.StdEncoding.EncodeToString([]byte("c")),
							},
						},
						Pattern: []indexes.PatternStep{
							{
								Alias: "src",
								NodeFilter: indexes.NodeFilter{
									FilterQuery: tc.filter,
								},
							},
						},
					},
				},
			}

			reqBytes, err := json.Marshal(req)
			require.NoError(t, err)
			resBytes, err := db.Search(ctx, reqBytes)
			require.NoError(t, err)

			var res indexes.RemoteIndexSearchResult
			require.NoError(t, json.Unmarshal(resBytes, &res))
			require.Contains(t, res.GraphResults, "pattern")
			require.Len(t, res.GraphResults["pattern"].Matches, len(tc.want))
			got := make([]string, 0, len(res.GraphResults["pattern"].Matches))
			for _, match := range res.GraphResults["pattern"].Matches {
				got = append(got, match.Bindings["src"].Key)
			}
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestZigCoreDB_GraphOnlyAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title": "root",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"b": {"title": "red"},
		"c": {"title": "red"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		AggregationRequests: indexes.AggregationRequests{
			"titles":    {Type: "terms", Field: "title", Size: 5},
			"count_all": {Type: "count"},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      indexes.GraphQueryTypeNeighbors,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{
					Direction: "out",
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "neighbors")
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "titles")
	require.Contains(t, res.AggregationResults, "count_all")
	assert.Equal(t, float64(2), res.AggregationResults["count_all"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 1)
	assert.Equal(t, "red", res.AggregationResults["titles"].Buckets[0].Key)
	assert.Equal(t, int64(2), res.AggregationResults["titles"].Buckets[0].Count)
}

func TestZigCoreDB_GraphOnlyAggregationsWithFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {
			"title": "root",
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{
						map[string]any{"target": "b", "weight": 1.0},
						map[string]any{"target": "c", "weight": 1.0},
					},
				},
			},
		},
		"b": {"title": "red", "content": "keep me"},
		"c": {"title": "red", "content": "drop me"},
	}

	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	req := &indexes.RemoteIndexSearchRequest{
		Limit:       10,
		FilterQuery: json.RawMessage(`{"match":"keep","field":"content"}`),
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 5},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:             indexes.GraphQueryTypeNeighbors,
				IndexName:        "citations",
				IncludeDocuments: true,
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "neighbors")
	require.Len(t, res.GraphResults["neighbors"].Nodes, 1)
	assert.Equal(t, "Yg==", res.GraphResults["neighbors"].Nodes[0].Key)
	require.Contains(t, res.AggregationResults, "titles")
	require.Len(t, res.AggregationResults["titles"].Buckets, 1)
	assert.Equal(t, int64(1), res.AggregationResults["titles"].Buckets[0].Count)
}

func TestZigCoreDB_GraphPathAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {"title": "root", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "b", "weight": 1.0}, map[string]any{"target": "c", "weight": 1.0}}}}},
		"b": {"title": "middle", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "d", "weight": 1.0}}}}},
		"c": {"title": "branch", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "d", "weight": 1.0}}}}},
		"d": {"title": "leaf"},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 5},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"paths": {
				Type:      indexes.GraphQueryTypeKShortestPaths,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				TargetNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("d"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out", K: 2, MaxDepth: 4, WeightMode: "min_hops"},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "paths")
	require.Len(t, res.GraphResults["paths"].Paths, 2)
	require.Contains(t, res.AggregationResults, "titles")
	require.Len(t, res.AggregationResults["titles"].Buckets, 4)
	bucketKeys := make(map[string]int64, len(res.AggregationResults["titles"].Buckets))
	for _, bucket := range res.AggregationResults["titles"].Buckets {
		key, ok := bucket.Key.(string)
		require.True(t, ok)
		bucketKeys[key] = bucket.Count
	}
	assert.Equal(t, int64(1), bucketKeys["root"])
	assert.Equal(t, int64(1), bucketKeys["middle"])
	assert.Equal(t, int64(1), bucketKeys["branch"])
	assert.Equal(t, int64(1), bucketKeys["leaf"])
}

func TestZigCoreDB_GraphPatternAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {"title": "root", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "b", "weight": 1.0}}}}},
		"b": {"title": "middle", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "c", "weight": 1.0}}}}},
		"c": {"title": "leaf"},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 5},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:             indexes.GraphQueryTypePattern,
				IndexName:        "citations",
				IncludeDocuments: true,
				Fields:           []string{"title"},
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Pattern: []indexes.PatternStep{
					{Alias: "src"},
					{Alias: "mid", Edge: indexes.PatternEdgeStep{Types: []string{"cites"}, Direction: "out"}, NodeFilter: indexes.NodeFilter{FilterPrefix: "b"}},
					{Alias: "dst", Edge: indexes.PatternEdgeStep{Types: []string{"cites"}, Direction: "out"}},
				},
				ReturnAliases: []string{"src", "dst"},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.GraphResults, "pattern")
	require.Len(t, res.GraphResults["pattern"].Matches, 1)
	require.Contains(t, res.AggregationResults, "titles")
	require.Len(t, res.AggregationResults["titles"].Buckets, 2)
	bucketKeys := make(map[string]int64, len(res.AggregationResults["titles"].Buckets))
	for _, bucket := range res.AggregationResults["titles"].Buckets {
		key, ok := bucket.Key.(string)
		require.True(t, ok)
		bucketKeys[key] = bucket.Count
	}
	assert.Equal(t, int64(1), bucketKeys["root"])
	assert.Equal(t, int64(1), bucketKeys["leaf"])
	_, hasMiddle := bucketKeys["middle"]
	assert.False(t, hasMiddle)
}

func TestZigCoreDB_GraphOnlyOrderByStoredField(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {"title": "root", "_edges": map[string]any{"citations": map[string]any{"cites": []any{
			map[string]any{"target": "b", "weight": 1.0},
			map[string]any{"target": "c", "weight": 1.0},
		}}}},
		"b": {"title": "charlie"},
		"c": {"title": "alpha"},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
			Limit:   2,
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:             indexes.GraphQueryTypeNeighbors,
				IndexName:        "citations",
				IncludeDocuments: true,
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 2)
	assert.Equal(t, []string{"c", "b"}, []string{
		res.FusionResult.Hits[0].ID,
		res.FusionResult.Hits[1].ID,
	})
}

func TestZigCoreDB_GraphOnlyOrderByStoredFieldAndCursor(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docs := map[string]map[string]any{
		"a": {"title": "root", "_edges": map[string]any{"citations": map[string]any{"cites": []any{
			map[string]any{"target": "b", "weight": 1.0},
			map[string]any{"target": "c", "weight": 1.0},
			map[string]any{"target": "d", "weight": 1.0},
		}}}},
		"b": {"title": "charlie"},
		"c": {"title": "alpha"},
		"d": {"title": "bravo"},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
			Limit:   1,
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:             indexes.GraphQueryTypeNeighbors,
				IndexName:        "citations",
				IncludeDocuments: true,
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 1)
	assert.Equal(t, "c", res.FusionResult.Hits[0].ID)

	req.BlevePagingOpts.SearchAfter = []string{"alpha", "c"}
	reqBytes, err = json.Marshal(req)
	require.NoError(t, err)
	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 1)
	assert.Equal(t, "d", res.FusionResult.Hits[0].ID)
}

func TestZigCoreDB_GraphPathRejectsOrderBy(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	for key, doc := range map[string]map[string]any{
		"a": {"title": "root", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "b", "weight": 1.0}}}}},
		"b": {"title": "leaf"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}
	time.Sleep(500 * time.Millisecond)

	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
			Limit:   1,
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"paths": {
				Type:      indexes.GraphQueryTypeKShortestPaths,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				TargetNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("b"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out", K: 1, WeightMode: "min_hops"},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "custom bleve sort outside narrowed full-text path")
}

func TestZigCoreDB_GraphPatternRejectsOrderBy(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	for key, doc := range map[string]map[string]any{
		"a": {"title": "root", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "b", "weight": 1.0}}}}},
		"b": {"title": "leaf"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}
	time.Sleep(500 * time.Millisecond)

	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
			Limit:   1,
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"pattern": {
				Type:             indexes.GraphQueryTypePattern,
				IndexName:        "citations",
				IncludeDocuments: true,
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Pattern: []indexes.PatternStep{
					{Alias: "src"},
					{Alias: "dst", Edge: indexes.PatternEdgeStep{Types: []string{"cites"}, Direction: "out"}},
				},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "custom bleve sort outside narrowed full-text path")
}

func TestZigCoreDB_GraphVectorFusionAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*graphConfig))
	require.NoError(t, db.AddIndex(*denseConfig))

	docs := map[string]map[string]any{
		"a": {
			"title":     "root",
			"embedding": []float32{1, 0},
			"_edges": map[string]any{
				"citations": map[string]any{
					"cites": []any{map[string]any{"target": "b", "weight": 1.0}},
				},
			},
		},
		"b": {
			"title":     "child",
			"embedding": []float32{0.8, 0.2},
		},
		"c": {
			"title":     "other",
			"embedding": []float32{0, 1},
		},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, db.Batch(ctx, batch, nil, Op_SyncLevelEmbeddings))

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      indexes.GraphQueryTypeNeighbors,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
		ExpandStrategy: "union",
		Limit:          3,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 10},
			"count":  {Type: "count"},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.NotEmpty(t, res.FusionResult.Hits)
	require.Contains(t, res.AggregationResults, "count")
	assert.Equal(t, float64(3), res.AggregationResults["count"].Value)
	require.Contains(t, res.AggregationResults, "titles")
	require.Len(t, res.AggregationResults["titles"].Buckets, 3)
}

func TestZigCoreDB_DenseAndSparseSearch(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      3,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {
			"title":     "alpha",
			"embedding": []float32{1, 0, 0},
			"sparse_embedding": map[string]any{
				"indices": []uint32{0, 2},
				"values":  []float32{0.9, 0.1},
			},
		},
		"doc2": {
			"title":     "beta",
			"embedding": []float32{0, 1, 0},
			"sparse_embedding": map[string]any{
				"indices": []uint32{1},
				"values":  []float32{0.9},
			},
		},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {0, 1, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {
				Indices: []uint32{0},
				Values:  []float32{1},
			},
		},
		Limit: 2,
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.VectorSearchResult, "dense_idx")
	require.Contains(t, res.VectorSearchResult, "sparse_idx")
	require.NotEmpty(t, res.VectorSearchResult["dense_idx"].Hits)
	require.NotEmpty(t, res.VectorSearchResult["sparse_idx"].Hits)
	assert.Equal(t, "doc1", res.VectorSearchResult["dense_idx"].Hits[0].ID)
	assert.Equal(t, "doc1", res.VectorSearchResult["sparse_idx"].Hits[0].ID)
}

func TestZigCoreDB_DenseSearchAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      3,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "alpha", "embedding": []float32{1, 0, 0}},
		"doc2": {"title": "alpha", "embedding": []float32{0.9, 0.1, 0}},
		"doc3": {"title": "beta", "embedding": []float32{0, 1, 0}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0, 0},
		},
		Limit: 3,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {
				Type:  "terms",
				Field: "title",
				Size:  5,
			},
			"count_all": {
				Type: "count",
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "titles")
	require.Contains(t, res.AggregationResults, "count_all")
	assert.Equal(t, float64(3), res.AggregationResults["count_all"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 2)
	assert.Equal(t, "alpha", res.AggregationResults["titles"].Buckets[0].Key)
	assert.Equal(t, int64(2), res.AggregationResults["titles"].Buckets[0].Count)
}

func TestZigCoreDB_DenseSearchAggregationsWithFilterQuery(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      3,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "alpha", "content": "keep", "embedding": []float32{1, 0, 0}},
		"doc2": {"title": "alpha", "content": "drop", "embedding": []float32{0.9, 0.1, 0}},
		"doc3": {"title": "beta", "content": "keep", "embedding": []float32{0, 1, 0}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0, 0},
		},
		FilterQuery: json.RawMessage(`{"match":"keep","field":"content"}`),
		Limit:       3,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 5},
			"count":  {Type: "count"},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.VectorSearchResult, "dense_idx")
	require.Len(t, res.VectorSearchResult["dense_idx"].Hits, 2)
	require.Contains(t, res.AggregationResults, "count")
	assert.Equal(t, float64(2), res.AggregationResults["count"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 2)
}

func TestZigCoreDB_SparseSearchAggregations(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {
			"title": "alpha",
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{1.0},
			},
		},
		"doc2": {
			"title": "alpha",
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{0.8},
			},
		},
		"doc3": {
			"title": "beta",
			"sparse_embedding": map[string]any{
				"indices": []uint32{1},
				"values":  []float32{1.0},
			},
		},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	req := &indexes.RemoteIndexSearchRequest{
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {
				Indices: []uint32{0},
				Values:  []float32{1},
			},
		},
		Limit: 3,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {
				Type:  "terms",
				Field: "title",
				Size:  5,
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.AggregationResults)
	require.Contains(t, res.AggregationResults, "titles")
	require.Len(t, res.AggregationResults["titles"].Buckets, 1)
	assert.Equal(t, "alpha", res.AggregationResults["titles"].Buckets[0].Key)
	assert.Equal(t, int64(2), res.AggregationResults["titles"].Buckets[0].Count)
}

func TestZigCoreDB_SparseSearchAggregationsWithFilterPrefix(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"keep:1": {
			"title": "alpha",
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{1.0},
			},
		},
		"keep:2": {
			"title": "alpha",
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{0.8},
			},
		},
		"drop:1": {
			"title": "beta",
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{0.7},
			},
		},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	req := &indexes.RemoteIndexSearchRequest{
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {
				Indices: []uint32{0},
				Values:  []float32{1},
			},
		},
		FilterPrefix: []byte("keep:"),
		Limit:        3,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 5},
			"count":  {Type: "count"},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.VectorSearchResult, "sparse_idx")
	require.Len(t, res.VectorSearchResult["sparse_idx"].Hits, 2)
	require.Contains(t, res.AggregationResults, "count")
	assert.Equal(t, float64(2), res.AggregationResults["count"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 1)
	assert.Equal(t, "alpha", res.AggregationResults["titles"].Buckets[0].Key)
}

func TestZigCoreDB_VectorAndSparseAggregationsUnionWithoutMerge(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      3,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	docJSON, err := json.Marshal(map[string]any{
		"title":     "alpha",
		"embedding": []float32{1, 0, 0},
		"sparse_embedding": map[string]any{
			"indices": []uint32{0},
			"values":  []float32{1.0},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelEmbeddings))

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {
				Indices: []uint32{0},
				Values:  []float32{1},
			},
		},
		Limit: 3,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {
				Type:  "terms",
				Field: "title",
				Size:  5,
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.AggregationResults, "titles")
	require.NotEmpty(t, res.AggregationResults["titles"].Buckets)
}

func TestZigCoreDB_VectorAndSparseAggregationsWithMerge(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      3,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {
			"title":     "alpha",
			"embedding": []float32{1, 0, 0},
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{1.0},
			},
		},
		"doc2": {
			"title":     "alpha",
			"embedding": []float32{0.9, 0.1, 0},
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{0.8},
			},
		},
		"doc3": {
			"title":     "beta",
			"embedding": []float32{0, 1, 0},
			"sparse_embedding": map[string]any{
				"indices": []uint32{1},
				"values":  []float32{1.0},
			},
		},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {
				Indices: []uint32{0},
				Values:  []float32{1},
			},
		},
		MergeConfig: &indexes.MergeConfig{},
		Limit:       3,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 5},
			"count":  {Type: "count"},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.NotEmpty(t, res.FusionResult.Hits)
	require.Contains(t, res.AggregationResults, "count")
	assert.Equal(t, float64(3), res.AggregationResults["count"].Value)
	require.Len(t, res.AggregationResults["titles"].Buckets, 2)
	assert.Equal(t, "alpha", res.AggregationResults["titles"].Buckets[0].Key)
}

func TestZigCoreDB_VectorSparseMergeOrderByStoredField(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      3,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "charlie", "embedding": []float32{1, 0, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{1.0}}},
		"doc2": {"title": "alpha", "embedding": []float32{0.9, 0.1, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.8}}},
		"doc3": {"title": "bravo", "embedding": []float32{0, 1, 0}, "sparse_embedding": map[string]any{"indices": []uint32{1}, "values": []float32{1.0}}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	desc := false
	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {Indices: []uint32{0}, Values: []float32{1}},
		},
		MergeConfig: &indexes.MergeConfig{},
		Limit:       3,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &desc}, {Field: "_id", Desc: &desc}},
			Limit:   2,
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 2)
	assert.Equal(t, "doc2", res.FusionResult.Hits[0].ID)
	assert.Equal(t, "doc3", res.FusionResult.Hits[1].ID)
}

func TestZigCoreDB_VectorSparseMergeOrderByStoredFieldAndCursor(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      3,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "charlie", "embedding": []float32{1, 0, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{1.0}}},
		"doc2": {"title": "alpha", "embedding": []float32{0.9, 0.1, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.8}}},
		"doc3": {"title": "bravo", "embedding": []float32{0.8, 0.2, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.7}}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	desc := false
	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {Indices: []uint32{0}, Values: []float32{1}},
		},
		MergeConfig: &indexes.MergeConfig{},
		Limit:       3,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &desc}, {Field: "_id", Desc: &desc}},
			Limit:   1,
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 1)
	assert.Equal(t, "doc2", res.FusionResult.Hits[0].ID)

	req.BlevePagingOpts.SearchAfter = []string{"alpha", "doc2"}
	reqBytes, err = json.Marshal(req)
	require.NoError(t, err)
	resBytes, err = db.Search(ctx, reqBytes)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 1)
	assert.Equal(t, "doc3", res.FusionResult.Hits[0].ID)
}

func TestZigCoreDB_VectorSparseMergeRejectsMixedScalarSortFieldTypes(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"label": 1, "embedding": []float32{1, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{1.0}}},
		"doc2": {"label": "one", "embedding": []float32{0.9, 0.1}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.8}}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {Indices: []uint32{0}, Values: []float32{1}},
		},
		MergeConfig: &indexes.MergeConfig{},
		Limit:       2,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "label", Desc: &asc}, {Field: "_id", Desc: &asc}},
			Limit:   2,
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consistent scalar value types")
}

func TestZigCoreDB_VectorSparseMergeOrderByStoredFieldWithMissingValues(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*denseConfig))

	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, db.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "charlie", "embedding": []float32{1, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{1.0}}},
		"doc2": {"embedding": []float32{0.95, 0.05}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.9}}},
		"doc3": {"title": "alpha", "embedding": []float32{0.9, 0.1}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.8}}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	asc := false
	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {Indices: []uint32{0}, Values: []float32{1}},
		},
		MergeConfig: &indexes.MergeConfig{},
		Limit:       3,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
			Limit:   3,
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Len(t, res.FusionResult.Hits, 3)
	assert.Equal(t, []string{"doc3", "doc1", "doc2"}, []string{
		res.FusionResult.Hits[0].ID,
		res.FusionResult.Hits[1].ID,
		res.FusionResult.Hits[2].ID,
	})
}

func TestZigCoreDB_RejectsUnsupportedMixedGraphSortWithoutAuthoritativeFinalSet(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	docJSON, err := json.Marshal(map[string]any{
		"title": "root",
		"_edges": map[string]any{
			"citations": map[string]any{
				"cites": []any{map[string]any{"target": "b", "weight": 1.0}},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte("a"), docJSON}}, nil, Op_SyncLevelWrite))

	desc := false
	req := &indexes.RemoteIndexSearchRequest{
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      indexes.GraphQueryTypeNeighbors,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
		Limit: 10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "title", Desc: &desc}},
			Limit:   1,
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "custom bleve sort outside narrowed full-text path")
}

func TestZigCoreDB_RejectsVectorAggregationsRequiringFullTextCorpusStats(t *testing.T) {
	db := openZigSearchTestDB(t)

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		Limit: 10,
		AggregationRequests: indexes.AggregationRequests{
			"sig_terms": {
				Type:                  "significant_terms",
				Field:                 "title",
				SignificanceAlgorithm: "jlh",
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(context.Background(), reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requiring full-text corpus stats")
}

func TestZigCoreDB_RejectsGraphAggregationsRequiringFullTextCorpusStats(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, db.AddIndex(*graphConfig))

	for key, doc := range map[string]map[string]any{
		"a": {"title": "root", "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "b", "weight": 1.0}}}}},
		"b": {"title": "leaf"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}
	time.Sleep(500 * time.Millisecond)

	req := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		AggregationRequests: indexes.AggregationRequests{
			"sig_terms": {
				Type:                  "significant_terms",
				Field:                 "title",
				SignificanceAlgorithm: "jlh",
			},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      indexes.GraphQueryTypeNeighbors,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requiring full-text corpus stats")
}

func TestZigCoreDB_RejectsGraphVectorFusionAggregationsRequiringFullTextCorpusStats(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, db.AddIndex(*graphConfig))
	require.NoError(t, db.AddIndex(*denseConfig))

	for key, doc := range map[string]map[string]any{
		"a": {"title": "root", "embedding": []float32{1, 0}, "_edges": map[string]any{"citations": map[string]any{"cites": []any{map[string]any{"target": "b", "weight": 1.0}}}}},
		"b": {"title": "leaf", "embedding": []float32{0.8, 0.2}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		GraphSearches: map[string]*indexes.GraphQuery{
			"neighbors": {
				Type:      indexes.GraphQueryTypeNeighbors,
				IndexName: "citations",
				StartNodes: indexes.GraphNodeSelector{
					Keys: []string{base64.StdEncoding.EncodeToString([]byte("a"))},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
		ExpandStrategy: "union",
		Limit:          3,
		AggregationRequests: indexes.AggregationRequests{
			"sig_terms": {
				Type:                  "significant_terms",
				Field:                 "title",
				SignificanceAlgorithm: "jlh",
			},
		},
	}

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = db.Search(ctx, reqBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requiring full-text corpus stats")
}

func TestZigCoreDB_FullTextSearchReranker(t *testing.T) {
	db := openZigSearchTestDB(t)
	ctx := context.Background()

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc_mars": {
			"content": "mission to mars with launch details",
			"title":   "mars mission",
		},
		"doc_sales": {
			"content": "mission statement for the sales organization",
			"title":   "sales mission",
		},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	matchMission := query.NewMatchQuery("mission")
	matchMission.SetField("content")

	field := "content"
	cfg := reranking.RerankerConfig{
		Provider: reranking.RerankerProviderAntfly,
		Field:    &field,
	}
	require.NoError(t, cfg.FromAntflyRerankerConfig(map[string]any{}))

	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchMission),
		Limit:              10,
		RerankerConfig:     &cfg,
		RerankerQuery:      "mars mission",
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := db.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 2)
	assert.Equal(t, "doc_mars", res.BleveSearchResult.Hits[0].ID)
	assert.Greater(t, res.BleveSearchResult.Hits[0].Score, res.BleveSearchResult.Hits[1].Score)
}
