//go:build zigdb

package db

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func testStoreSchema() *schema.TableSchema {
	return &schema.TableSchema{
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
}

func testStoreCompositeSortSchema() *schema.TableSchema {
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
						"mixed":        map[string]any{},
						"mixed_scalar": map[string]any{},
					},
				},
			},
		},
	}
}

func testStoreDateSchema() *schema.TableSchema {
	return &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title":        map[string]any{"type": "string"},
						"content":      map[string]any{"type": "string"},
						"published_at": map[string]any{"type": "string", "format": "date-time"},
					},
				},
			},
		},
	}
}

func testStoreCustomDateParserSchema() *schema.TableSchema {
	return &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type":                              schema.DynamicTemplateMatchMappingTypeObject,
					schema.XAntflyDefaultDateTimeParser: "queryDT",
					schema.XAntflyDateTimeParsers: map[string]any{
						"queryDT": map[string]any{
							"type": "sanitizedgo",
							"layouts": []any{
								"02/01/2006 3:04PM",
							},
						},
					},
					"properties": map[string]any{
						"title":   map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
						"published_at": map[string]any{
							"type":                       "string",
							schema.XAntflyTypes:          []string{"datetime"},
							schema.XAntflyDateTimeParser: "queryDT",
						},
					},
				},
			},
		},
	}
}

func testStoreCustomAnalyzerSchema() *schema.TableSchema {
	return &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					schema.XAntflyTokenFilters: map[string]any{
						"tri_edge": map[string]any{
							"type": "edge_ngram",
							"min":  3,
							"max":  5,
						},
					},
					schema.XAntflyAnalyzers: map[string]any{
						"tri_edge_analyzer": map[string]any{
							"type":          "custom",
							"tokenizer":     "unicode",
							"token_filters": []any{"to_lower", "tri_edge"},
						},
					},
					"properties": map[string]any{
						"title": map[string]any{
							"type":                 "string",
							schema.XAntflyTypes:    []string{"text"},
							schema.XAntflyAnalyzer: "tri_edge_analyzer",
						},
					},
				},
			},
		},
	}
}

func testStoreCustomTokenizerAnalyzerSchema() *schema.TableSchema {
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

func TestNewCoreDB_SelectsZigDBWhenEnvSet(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	db := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	_, ok := db.(*ZigCoreDB)
	require.True(t, ok)
}

func TestStoreDB_ZigCoreDBLookupScanAndSearch(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	ctx := context.Background()
	docJSON, err := json.Marshal(map[string]any{
		"title":   "doc title",
		"content": "hello store wrapper world",
	})
	require.NoError(t, err)
	require.NoError(t, zigCore.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	doc, err := store.Lookup(ctx, "doc1")
	require.NoError(t, err)
	assert.Equal(t, "doc title", doc["title"])

	scanRes, err := store.Scan(ctx, nil, []byte{0xFF}, ScanOptions{
		IncludeDocuments: true,
		Limit:            10,
	})
	require.NoError(t, err)
	require.Contains(t, scanRes.Documents, "doc1")

	matchHello := query.NewMatchQuery("hello")
	matchHello.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchHello),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := store.Search(ctx, reqBytes)
	require.NoError(t, err)
	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBDateRangeStringQueryWithNamedDefaultParserAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreDateSchema()
	openStore := func(dir string) (*ZigCoreDB, *StoreDB) {
		core := newCoreDB(
			zaptest.NewLogger(t),
			nil,
			storeSchema,
			map[string]indexes.IndexConfig{},
			nil,
			nil,
			(*pebbleutils.Cache)(nil),
		)
		zigCore, ok := core.(*ZigCoreDB)
		require.True(t, ok)
		require.NoError(t, zigCore.Open(dir, false, storeSchema, types.Range{nil, []byte{0xFF}}))
		store := &StoreDB{
			logger: zaptest.NewLogger(t),
			coreDB: zigCore,
		}
		return zigCore, store
	}

	dir := t.TempDir()
	zigCore, store := openStore(dir)

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	ctx := context.Background()
	for key, doc := range map[string]map[string]any{
		"doc1": {"published_at": "2025-01-01T00:00:00Z"},
		"doc2": {"published_at": "2025-01-02T00:00:00Z"},
		"doc3": {"published_at": "2025-01-03T00:00:00Z"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	runQuery := func(activeStore *StoreDB) {
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
		resBytes, err := activeStore.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.NotNil(t, res.BleveSearchResult)
		require.Len(t, res.BleveSearchResult.Hits, 1)
		assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
	}

	runQuery(store)
	require.NoError(t, zigCore.Close())

	zigCore, store = openStore(dir)
	defer func() {
		require.NoError(t, zigCore.Close())
	}()

	runQuery(store)
}

func TestStoreDB_ZigCoreDBDateRangeStringQueryWithSchemaDefinedCustomParser(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreCustomDateParserSchema()
	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		storeSchema,
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, storeSchema, types.Range{nil, []byte{0xFF}}))
	defer func() {
		require.NoError(t, zigCore.Close())
	}()

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	ctx := context.Background()
	for key, doc := range map[string]map[string]any{
		"doc1": {"published_at": "01/02/2025 3:04PM"},
		"doc2": {"published_at": "01/03/2025 3:04PM"},
		"doc3": {"published_at": "01/04/2025 3:04PM"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	q := query.NewDateRangeStringQuery("01/02/2025 3:04PM", "01/03/2025 3:04PM")
	q.SetField("published_at")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := store.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBCustomFieldAnalyzerFromSchema(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreCustomAnalyzerSchema()
	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		storeSchema,
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, storeSchema, types.Range{nil, []byte{0xFF}}))
	defer func() {
		require.NoError(t, zigCore.Close())
	}()

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	ctx := context.Background()
	require.NoError(t, zigCore.Batch(ctx, [][2][]byte{{[]byte("doc1"), []byte(`{"title":"hello"}`)}}, nil, Op_SyncLevelFullText))

	q := query.NewTermQuery("hel")
	q.SetField("title")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := store.Search(ctx, reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBCustomFieldAnalyzerFromSchemaAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreCustomAnalyzerSchema()
	openStore := func(dir string) (*ZigCoreDB, *StoreDB) {
		core := newCoreDB(
			zaptest.NewLogger(t),
			nil,
			storeSchema,
			map[string]indexes.IndexConfig{},
			nil,
			nil,
			(*pebbleutils.Cache)(nil),
		)
		zigCore, ok := core.(*ZigCoreDB)
		require.True(t, ok)
		require.NoError(t, zigCore.Open(dir, false, storeSchema, types.Range{nil, []byte{0xFF}}))
		store := &StoreDB{
			logger: zaptest.NewLogger(t),
			coreDB: zigCore,
		}
		return zigCore, store
	}

	runQuery := func(store *StoreDB) {
		q := query.NewTermQuery("hel")
		q.SetField("title")
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(q),
			Limit:              10,
		}
		req.BleveSearchRequest.Size = 10

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.NotNil(t, res.BleveSearchResult)
		require.Len(t, res.BleveSearchResult.Hits, 1)
		assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
	}

	dir := t.TempDir()
	zigCore, store := openStore(dir)

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))
	require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte("doc1"), []byte(`{"title":"hello"}`)}}, nil, Op_SyncLevelFullText))

	runQuery(store)
	require.NoError(t, zigCore.Close())

	zigCore, store = openStore(dir)
	defer func() {
		require.NoError(t, zigCore.Close())
	}()
	runQuery(store)
}

func TestStoreDB_ZigCoreDBCustomTokenizerAnalyzerFromSchemaAcrossSnapshotRestore(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreCustomTokenizerAnalyzerSchema()
	root := t.TempDir()
	shardID := types.ID(31)
	nodeID := types.ID(9)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		storeSchema,
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, storeSchema, types.Range{nil, []byte{0xFF}}))

	store := &StoreDB{
		logger:         logger,
		dbDir:          dbDir,
		dataDir:        root,
		snapStore:      snapStore,
		coreDB:         zigCore,
		loadSnapshotID: func(context.Context) (string, error) { return "ziganalysis", nil },
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))
	require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte("doc1"), []byte(`{"title":"<b>Hello</b>"}`)}}, nil, Op_SyncLevelFullText))
	require.NoError(t, store.CreateDBSnapshot("ziganalysis"))

	require.NoError(t, zigCore.Close())
	require.NoError(t, os.RemoveAll(dbDir))
	require.NoError(t, store.loadAndRecoverFromPersistentSnapshot(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.coreDB.Close())
	})

	q := query.NewTermQuery("ell")
	q.SetField("title")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(q),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := store.Search(context.Background(), reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBTxnListingAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreSchema()
	openStore := func(dir string) (*ZigCoreDB, *StoreDB) {
		core := newCoreDB(
			zaptest.NewLogger(t),
			nil,
			storeSchema,
			map[string]indexes.IndexConfig{},
			nil,
			nil,
			(*pebbleutils.Cache)(nil),
		)
		zigCore, ok := core.(*ZigCoreDB)
		require.True(t, ok)
		require.NoError(t, zigCore.Open(dir, false, storeSchema, types.Range{nil, []byte{0xFF}}))
		store := &StoreDB{
			logger: zaptest.NewLogger(t),
			coreDB: zigCore,
			schema: storeSchema,
		}
		return zigCore, store
	}

	dir := t.TempDir()
	zigCore, store := openStore(dir)

	ctx := context.Background()
	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())
	participants := [][]byte{{1, 0, 0, 0, 0, 0, 0, 0}}

	require.NoError(t, store.applyOpInitTransaction(ctx, InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    timestamp,
		Participants: participants,
	}.Build()))
	require.NoError(t, store.applyOpWriteIntent(ctx, WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        timestamp,
		CoordinatorShard: []byte{9, 0, 0, 0, 0, 0, 0, 0},
		Batch: BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
			},
		}.Build(),
	}.Build()))

	runAssertions := func(activeStore *StoreDB) {
		records, err := activeStore.ListTxnRecords(ctx)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, txnID[:], records[0].TxnID)
		assert.Equal(t, participants, records[0].Participants)
		assert.Equal(t, int32(TxnStatusPending), records[0].Status)

		intents, err := activeStore.ListTxnIntents(ctx)
		require.NoError(t, err)
		require.Len(t, intents, 1)
		assert.Equal(t, txnID[:], intents[0].TxnID)
		assert.Equal(t, []byte("key1"), intents[0].UserKey)
		assert.Equal(t, []byte("value1"), intents[0].Value)
		assert.False(t, intents[0].IsDelete)
	}

	runAssertions(store)

	require.NoError(t, zigCore.Close())
	reopenedCore, reopenedStore := openStore(dir)
	defer func() { require.NoError(t, reopenedCore.Close()) }()
	runAssertions(reopenedStore)
}

func TestStoreDB_ZigCoreDBFindMedianKeyAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreSchema()
	openStore := func(dir string) (*ZigCoreDB, *StoreDB) {
		core := newCoreDB(
			zaptest.NewLogger(t),
			nil,
			storeSchema,
			map[string]indexes.IndexConfig{},
			nil,
			nil,
			(*pebbleutils.Cache)(nil),
		)
		zigCore, ok := core.(*ZigCoreDB)
		require.True(t, ok)
		require.NoError(t, zigCore.Open(dir, false, storeSchema, types.Range{nil, []byte{0xFF}}))
		store := &StoreDB{
			logger: zaptest.NewLogger(t),
			coreDB: zigCore,
			schema: storeSchema,
		}
		return zigCore, store
	}

	dir := t.TempDir()
	zigCore, store := openStore(dir)
	ctx := context.Background()
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		docJSON, err := json.Marshal(map[string]any{"content": key})
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(ctx, [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}
	txnID := uuid.New()
	require.NoError(t, store.applyOpInitTransaction(ctx, InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    uint64(time.Now().Unix()),
		Participants: [][]byte{{1}},
	}.Build()))

	key, err := store.FindMedianKey()
	require.NoError(t, err)
	assert.Equal(t, []byte("c"), key)

	require.NoError(t, zigCore.Close())
	reopenedCore, reopenedStore := openStore(dir)
	defer func() { require.NoError(t, reopenedCore.Close()) }()

	key, err = reopenedStore.FindMedianKey()
	require.NoError(t, err)
	assert.Equal(t, []byte("c"), key)
}

func TestStoreDB_ZigCoreDBUpdateSchemaAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreSchema()
	openStore := func(dir string, activeSchema *schema.TableSchema) (*ZigCoreDB, *StoreDB) {
		core := newCoreDB(
			zaptest.NewLogger(t),
			nil,
			activeSchema,
			map[string]indexes.IndexConfig{},
			nil,
			nil,
			(*pebbleutils.Cache)(nil),
		)
		zigCore, ok := core.(*ZigCoreDB)
		require.True(t, ok)
		require.NoError(t, zigCore.Open(dir, false, activeSchema, types.Range{nil, []byte{0xFF}}))
		store := &StoreDB{
			logger: zaptest.NewLogger(t),
			coreDB: zigCore,
			schema: activeSchema,
		}
		return zigCore, store
	}

	dir := t.TempDir()
	zigCore, store := openStore(dir, storeSchema)
	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	updated := *storeSchema
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

	op, err := NewUpdateSchemaOp(&updated)
	require.NoError(t, err)
	require.NoError(t, store.applyOpUpdateSchema(context.Background(), op.GetUpdateSchema()))

	ctx := context.Background()
	docJSON, err := json.Marshal(map[string]any{
		"title":    "base",
		"tag_name": "alpha dynamic",
	})
	require.NoError(t, err)
	require.NoError(t, zigCore.Batch(ctx, [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))

	runQuery := func(activeStore *StoreDB) {
		q := query.NewMatchQuery("alpha")
		q.SetField("tag_name")
		reqBytes, err := json.Marshal(indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(q),
		})
		require.NoError(t, err)

		resBytes, err := activeStore.Search(ctx, reqBytes)
		require.NoError(t, err)

		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		require.NotNil(t, res.BleveSearchResult)
		require.Len(t, res.BleveSearchResult.Hits, 1)
		assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
	}

	runQuery(store)

	require.NoError(t, zigCore.Close())
	reopenedCore, reopenedStore := openStore(dir, &updated)
	defer func() { require.NoError(t, reopenedCore.Close()) }()
	runQuery(reopenedStore)
}

func TestStoreDB_ZigCoreDBSearchAggregations(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "database nosql scalability"},
		"doc2": {"content": "database nosql replication"},
		"doc3": {"content": "programming language compiler"},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
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
				SignificanceAlgorithm: "jlh",
			},
		},
	}
	req.BleveSearchRequest.Size = 10

	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := store.Search(context.Background(), reqBytes)
	require.NoError(t, err)

	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.AggregationResults, "sig_terms")
	require.NotEmpty(t, res.AggregationResults["sig_terms"].Buckets)
}

func TestStoreDB_ZigCoreDBGraphAndVectorFilteredAggregations(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))
	require.NoError(t, zigCore.AddIndex(*graphConfig))
	require.NoError(t, zigCore.AddIndex(*denseConfig))
	require.NoError(t, zigCore.AddIndex(*sparseConfig))

	docs := map[string]map[string]any{
		"a": {
			"title":     "root",
			"embedding": []float32{1, 0},
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
			"title":     "alpha",
			"content":   "keep",
			"embedding": []float32{1, 0},
			"sparse_embedding": map[string]any{
				"indices": []uint32{0},
				"values":  []float32{1.0},
			},
		},
		"c": {
			"title":     "beta",
			"content":   "drop",
			"embedding": []float32{0.8, 0.2},
			"sparse_embedding": map[string]any{
				"indices": []uint32{1},
				"values":  []float32{1.0},
			},
		},
	}
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelWrite))
	}
	time.Sleep(500 * time.Millisecond)

	graphReq := &indexes.RemoteIndexSearchRequest{
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
					Keys: []string{"YQ=="},
				},
				Params: indexes.GraphQueryParams{Direction: "out"},
			},
		},
	}
	graphReqBytes, err := json.Marshal(graphReq)
	require.NoError(t, err)
	graphResBytes, err := store.Search(context.Background(), graphReqBytes)
	require.NoError(t, err)

	var graphRes indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(graphResBytes, &graphRes))
	require.Contains(t, graphRes.GraphResults, "neighbors")
	require.Len(t, graphRes.GraphResults["neighbors"].Nodes, 1)
	assert.Equal(t, "Yg==", graphRes.GraphResults["neighbors"].Nodes[0].Key)
	require.Contains(t, graphRes.AggregationResults, "titles")
	require.Len(t, graphRes.AggregationResults["titles"].Buckets, 1)
	assert.Equal(t, "alpha", graphRes.AggregationResults["titles"].Buckets[0].Key)

	vectorReq := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		FilterQuery: json.RawMessage(`{"match":"keep","field":"content"}`),
		Limit:       3,
		AggregationRequests: indexes.AggregationRequests{
			"count": {Type: "count"},
		},
	}
	vectorReqBytes, err := json.Marshal(vectorReq)
	require.NoError(t, err)
	vectorResBytes, err := store.Search(context.Background(), vectorReqBytes)
	require.NoError(t, err)

	var vectorRes indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(vectorResBytes, &vectorRes))
	require.Contains(t, vectorRes.VectorSearchResult, "dense_idx")
	require.Len(t, vectorRes.VectorSearchResult["dense_idx"].Hits, 1)
	require.Contains(t, vectorRes.AggregationResults, "count")
	assert.Equal(t, float64(1), vectorRes.AggregationResults["count"].Value)

	fusedReq := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
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
			"count": {Type: "count"},
		},
	}
	fusedReqBytes, err := json.Marshal(fusedReq)
	require.NoError(t, err)
	fusedResBytes, err := store.Search(context.Background(), fusedReqBytes)
	require.NoError(t, err)

	var fusedRes indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(fusedResBytes, &fusedRes))
	require.NotNil(t, fusedRes.FusionResult)
	require.Contains(t, fusedRes.AggregationResults, "count")
	assert.Equal(t, float64(3), fusedRes.AggregationResults["count"].Value)

	unionReq := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		SparseSearches: map[string]indexes.SparseVec{
			"sparse_idx": {
				Indices: []uint32{0},
				Values:  []float32{1},
			},
		},
		Limit: 3,
		AggregationRequests: indexes.AggregationRequests{
			"count": {Type: "count"},
		},
	}
	unionReqBytes, err := json.Marshal(unionReq)
	require.NoError(t, err)
	unionResBytes, err := store.Search(context.Background(), unionReqBytes)
	require.NoError(t, err)

	var unionRes indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(unionResBytes, &unionRes))
	require.Contains(t, unionRes.AggregationResults, "count")
	assert.GreaterOrEqual(t, unionRes.AggregationResults["count"].Value.(float64), float64(1))

	fullTextDoc, err := json.Marshal(map[string]any{
		"title":   "alpha text",
		"content": "hello alpha",
	})
	require.NoError(t, err)
	require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte("text1"), fullTextDoc}}, nil, Op_SyncLevelFullText))

	matchAlpha := query.NewMatchQuery("alpha")
	matchAlpha.SetField("content")
	textVectorReq := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
		VectorSearches: map[string]vector.T{
			"dense_idx": {1, 0},
		},
		MergeConfig: &indexes.MergeConfig{},
		Limit:       3,
		AggregationRequests: indexes.AggregationRequests{
			"count": {Type: "count"},
		},
	}
	textVectorReq.BleveSearchRequest.Size = 3
	textVectorReqBytes, err := json.Marshal(textVectorReq)
	require.NoError(t, err)
	textVectorResBytes, err := store.Search(context.Background(), textVectorReqBytes)
	require.NoError(t, err)

	var textVectorRes indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(textVectorResBytes, &textVectorRes))
	require.NotNil(t, textVectorRes.FusionResult)
	require.Contains(t, textVectorRes.AggregationResults, "count")
	assert.GreaterOrEqual(t, textVectorRes.AggregationResults["count"].Value.(float64), float64(1))
}

func TestStoreDB_ZigCoreDBGraphPathAndPatternAggregations(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, zigCore.AddIndex(*graphConfig))

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
	require.NoError(t, zigCore.Batch(context.Background(), batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	pathReq := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 10},
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
	pathReqBytes, err := json.Marshal(pathReq)
	require.NoError(t, err)
	pathResBytes, err := store.Search(context.Background(), pathReqBytes)
	require.NoError(t, err)

	var pathRes indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(pathResBytes, &pathRes))
	require.Contains(t, pathRes.AggregationResults, "titles")
	require.Len(t, pathRes.AggregationResults["titles"].Buckets, 4)
	pathBuckets := make(map[string]int64, len(pathRes.AggregationResults["titles"].Buckets))
	for _, bucket := range pathRes.AggregationResults["titles"].Buckets {
		key, ok := bucket.Key.(string)
		require.True(t, ok)
		pathBuckets[key] = bucket.Count
	}
	assert.Equal(t, int64(1), pathBuckets["root"])
	assert.Equal(t, int64(1), pathBuckets["middle"])
	assert.Equal(t, int64(1), pathBuckets["branch"])
	assert.Equal(t, int64(1), pathBuckets["leaf"])

	patternReq := &indexes.RemoteIndexSearchRequest{
		Limit: 10,
		AggregationRequests: indexes.AggregationRequests{
			"titles": {Type: "terms", Field: "title", Size: 10},
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
	patternReqBytes, err := json.Marshal(patternReq)
	require.NoError(t, err)
	patternResBytes, err := store.Search(context.Background(), patternReqBytes)
	require.NoError(t, err)

	var patternRes indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(patternResBytes, &patternRes))
	require.Contains(t, patternRes.AggregationResults, "titles")
	require.Len(t, patternRes.AggregationResults["titles"].Buckets, 2)
	patternBuckets := make(map[string]int64, len(patternRes.AggregationResults["titles"].Buckets))
	for _, bucket := range patternRes.AggregationResults["titles"].Buckets {
		key, ok := bucket.Key.(string)
		require.True(t, ok)
		patternBuckets[key] = bucket.Count
	}
	assert.Equal(t, int64(1), patternBuckets["root"])
	assert.Equal(t, int64(1), patternBuckets["leaf"])
	_, hasMiddle := patternBuckets["middle"]
	assert.False(t, hasMiddle)
}

func TestStoreDB_ZigCoreDBGraphVectorFusionAggregations(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, zigCore.AddIndex(*graphConfig))
	require.NoError(t, zigCore.AddIndex(*denseConfig))

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
		"b": {"title": "child", "embedding": []float32{0.8, 0.2}},
		"c": {"title": "other", "embedding": []float32{0, 1}},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, zigCore.Batch(context.Background(), batch, nil, Op_SyncLevelEmbeddings))

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
	resBytes, err := store.Search(context.Background(), reqBytes)
	require.NoError(t, err)
	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.FusionResult)
	require.Contains(t, res.AggregationResults, "count")
	assert.Equal(t, float64(3), res.AggregationResults["count"].Value)
}

func TestStoreDB_ZigCoreDBGraphOnlySortAndCursor(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, zigCore.AddIndex(*graphConfig))

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
	require.NoError(t, zigCore.Batch(context.Background(), batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	run := func(searchAfter []string) *indexes.RemoteIndexSearchResult {
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
		req.BlevePagingOpts.SearchAfter = searchAfter
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	first := run(nil)
	require.NotNil(t, first.FusionResult)
	require.Len(t, first.FusionResult.Hits, 1)
	assert.Equal(t, "c", first.FusionResult.Hits[0].ID)

	second := run([]string{"alpha", "c"})
	require.NotNil(t, second.FusionResult)
	require.Len(t, second.FusionResult.Hits, 1)
	assert.Equal(t, "d", second.FusionResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBGraphOnlySortAndCursorAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	shardID := types.ID(71)
	nodeID := types.ID(19)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger:    logger,
		dbDir:     dbDir,
		dataDir:   root,
		snapStore: snapStore,
		coreDB:    zigCore,
	}

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	require.NoError(t, zigCore.AddIndex(*graphConfig))

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
	require.NoError(t, zigCore.Batch(context.Background(), batch, nil, Op_SyncLevelWrite))
	time.Sleep(500 * time.Millisecond)

	run := func(searchAfter []string) *indexes.RemoteIndexSearchResult {
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
		req.BlevePagingOpts.SearchAfter = searchAfter
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	first := run(nil)
	require.NotNil(t, first.FusionResult)
	require.Len(t, first.FusionResult.Hits, 1)
	assert.Equal(t, "c", first.FusionResult.Hits[0].ID)

	require.NoError(t, zigCore.Close())
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	store.coreDB = zigCore

	second := run([]string{"alpha", "c"})
	require.NotNil(t, second.FusionResult)
	require.Len(t, second.FusionResult.Hits, 1)
	assert.Equal(t, "d", second.FusionResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBGraphVectorFusionSortAndCursor(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, zigCore.AddIndex(*graphConfig))
	require.NoError(t, zigCore.AddIndex(*denseConfig))

	docs := map[string]map[string]any{
		"a": {"title": "root", "embedding": []float32{1, 0}, "_edges": map[string]any{"citations": map[string]any{"cites": []any{
			map[string]any{"target": "b", "weight": 1.0},
		}}}},
		"b": {"title": "charlie", "embedding": []float32{0.8, 0.2}},
		"c": {"title": "alpha", "embedding": []float32{0.95, 0.05}},
		"d": {"title": "bravo", "embedding": []float32{0.9, 0.1}},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, zigCore.Batch(context.Background(), batch, nil, Op_SyncLevelEmbeddings))

	run := func(searchAfter []string) *indexes.RemoteIndexSearchResult {
		asc := false
		req := &indexes.RemoteIndexSearchRequest{
			VectorSearches: map[string]vector.T{
				"dense_idx": {1, 0},
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
			ExpandStrategy: "union",
			Limit:          4,
			BlevePagingOpts: indexes.FullTextPagingOptions{
				OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
				Limit:   1,
			},
			AggregationRequests: indexes.AggregationRequests{
				"count": {Type: "count"},
			},
		}
		req.BlevePagingOpts.SearchAfter = searchAfter
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	first := run(nil)
	require.NotNil(t, first.FusionResult)
	require.Len(t, first.FusionResult.Hits, 1)
	assert.Equal(t, "c", first.FusionResult.Hits[0].ID)
	require.Contains(t, first.AggregationResults, "count")
	assert.Equal(t, float64(4), first.AggregationResults["count"].Value)

	second := run([]string{"alpha", "c"})
	require.NotNil(t, second.FusionResult)
	require.Len(t, second.FusionResult.Hits, 1)
	assert.Equal(t, "d", second.FusionResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBFullTextStringArraySortAndCursor(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	storeSchema := testStoreCompositeSortSchema()
	core := newCoreDB(
		zaptest.NewLogger(t),
		nil,
		storeSchema,
		map[string]indexes.IndexConfig{},
		nil,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(t.TempDir(), false, storeSchema, types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger: zaptest.NewLogger(t),
		coreDB: zigCore,
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "tags": []any{"zulu", "alpha"}},
		"doc2": {"content": "alpha", "tags": []any{"bravo"}},
		"doc3": {"content": "alpha", "tags": []any{"charlie", "delta"}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	run := func(searchAfter []string) *indexes.RemoteIndexSearchResult {
		asc := false
		matchAlpha := query.NewMatchQuery("alpha")
		matchAlpha.SetField("content")
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
			Limit:              10,
			BlevePagingOpts: indexes.FullTextPagingOptions{
				OrderBy: []indexes.SortField{{Field: "tags", Desc: &asc}},
				Limit:   3,
			},
		}
		req.BleveSearchRequest.Size = 10
		req.BlevePagingOpts.SearchAfter = searchAfter
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	first := run(nil)
	require.NotNil(t, first.BleveSearchResult)
	require.Len(t, first.BleveSearchResult.Hits, 3)
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, []string{
		first.BleveSearchResult.Hits[0].ID,
		first.BleveSearchResult.Hits[1].ID,
		first.BleveSearchResult.Hits[2].ID,
	})
	assert.Equal(t, []string{"alpha"}, first.BleveSearchResult.Hits[0].Sort)
	assert.Equal(t, []string{"bravo"}, first.BleveSearchResult.Hits[1].Sort)
	assert.Equal(t, []string{"charlie"}, first.BleveSearchResult.Hits[2].Sort)

	second := run([]string{"bravo"})
	require.NotNil(t, second.BleveSearchResult)
	require.Len(t, second.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc3", second.BleveSearchResult.Hits[0].ID)
	assert.Equal(t, []string{"charlie"}, second.BleveSearchResult.Hits[0].Sort)
}

func TestStoreDB_ZigCoreDBFullTextCompositeSortSentinelAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	shardID := types.ID(91)
	nodeID := types.ID(27)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	storeSchema := testStoreCompositeSortSchema()
	core := newCoreDB(
		logger,
		nil,
		storeSchema,
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, storeSchema, types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger:    logger,
		dbDir:     dbDir,
		dataDir:   root,
		snapStore: snapStore,
		coreDB:    zigCore,
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"content": "alpha", "meta": map[string]any{"priority": 2}, "scores": []any{9, 3}, "mixed": "zulu", "mixed_scalar": "zulu"},
		"doc2": {"content": "alpha", "meta": map[string]any{"priority": 1}, "scores": []any{5}, "mixed": []any{"bravo"}, "mixed_scalar": 5},
		"doc3": {"content": "alpha", "meta": map[string]any{"priority": 3}, "scores": []any{7, 8}, "mixed": map[string]any{"priority": 1}, "mixed_scalar": true},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	run := func(field string, searchAfter []string) *indexes.RemoteIndexSearchResult {
		asc := false
		matchAlpha := query.NewMatchQuery("alpha")
		matchAlpha.SetField("content")
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
			Limit:              10,
			BlevePagingOpts: indexes.FullTextPagingOptions{
				OrderBy: []indexes.SortField{{Field: field, Desc: &asc}},
				Limit:   3,
			},
		}
		req.BleveSearchRequest.Size = 10
		req.BlevePagingOpts.SearchAfter = searchAfter
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	for _, field := range []string{"meta", "scores", "mixed", "mixed_scalar"} {
		first := run(field, nil)
		require.NotNil(t, first.BleveSearchResult)
		require.Len(t, first.BleveSearchResult.Hits, 3)
		for _, hit := range first.BleveSearchResult.Hits {
			assert.Equal(t, []string{bleveCompositeSortSentinel}, hit.Sort)
		}
	}

	require.NoError(t, zigCore.Close())
	require.NoError(t, zigCore.Open(dbDir, false, storeSchema, types.Range{nil, []byte{0xFF}}))
	store.coreDB = zigCore

	for _, field := range []string{"meta", "scores", "mixed", "mixed_scalar"} {
		second := run(field, []string{bleveCompositeSortSentinel})
		require.NotNil(t, second.BleveSearchResult)
		assert.Empty(t, second.BleveSearchResult.Hits)
	}
}

func TestStoreDB_ZigCoreDBStatsAndSnapshot(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, 1, 1)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	store := &StoreDB{
		logger:         logger,
		dbDir:          dbDir,
		dataDir:        root,
		snapStore:      snapStore,
		coreDB:         zigCore,
		loadSnapshotID: func(context.Context) (string, error) { return "zigsnap", nil },
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	denseConfig := indexes.NewEmbeddingsConfig("dense_index", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetric("l2_squared"),
	})
	sparseConfig := indexes.NewEmbeddingsConfig("sparse_index", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))
	require.NoError(t, zigCore.AddIndex(*denseConfig))
	require.NoError(t, zigCore.AddIndex(*sparseConfig))

	fullTextDoc, err := json.Marshal(map[string]any{
		"title":   "stats doc",
		"content": "snapshot content",
	})
	require.NoError(t, err)
	require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte("doc1"), fullTextDoc}}, nil, Op_SyncLevelFullText))

	embeddingDoc, err := json.Marshal(map[string]any{
		"embedding": []float32{1, 0},
		"sparse_embedding": map[string]any{
			"indices": []uint32{0, 2},
			"values":  []float32{0.9, 0.1},
		},
	})
	require.NoError(t, err)
	require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte("doc2"), embeddingDoc}}, nil, Op_SyncLevelEmbeddings))

	stats := store.Stats()
	require.NotNil(t, stats)
	require.NotNil(t, stats.Storage)
	assert.False(t, stats.Storage.Empty)
	require.Contains(t, stats.Indexes, "full_text_index")
	require.Contains(t, stats.Indexes, "dense_index")
	require.Contains(t, stats.Indexes, "sparse_index")
	ftStats, err := stats.Indexes["full_text_index"].AsFullTextIndexStats()
	require.NoError(t, err)
	assert.EqualValues(t, 1, ftStats.TotalIndexed)
	denseStats, err := stats.Indexes["dense_index"].AsEmbeddingsIndexStats()
	require.NoError(t, err)
	assert.EqualValues(t, 1, denseStats.TotalIndexed)
	assert.NotZero(t, denseStats.TotalNodes)
	sparseStats, err := stats.Indexes["sparse_index"].AsEmbeddingsIndexStats()
	require.NoError(t, err)
	assert.EqualValues(t, 1, sparseStats.TotalIndexed)
	assert.EqualValues(t, 2, sparseStats.TotalTerms)

	detailed, err := zigCore.DetailedStats()
	require.NoError(t, err)
	require.NotNil(t, detailed)
	assert.EqualValues(t, 2, detailed.DocCount)
	assert.False(t, detailed.Enrichment.Enabled)
	assert.True(t, detailed.TTLCleanup.Enabled)
	assert.False(t, detailed.TransactionRecovery.Enabled)

	require.NoError(t, store.CreateDBSnapshot("zigsnap"))
}

func TestStoreDB_ZigCoreDBSnapshotRestore(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	shardID := types.ID(21)
	nodeID := types.ID(7)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))

	store := &StoreDB{
		logger:         logger,
		dbDir:          dbDir,
		dataDir:        root,
		snapStore:      snapStore,
		coreDB:         zigCore,
		loadSnapshotID: func(context.Context) (string, error) { return "zigrestore", nil },
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))

	docJSON, err := json.Marshal(map[string]any{
		"title":   "restored doc",
		"content": "snapshot restore content",
	})
	require.NoError(t, err)
	require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))
	require.NoError(t, store.CreateDBSnapshot("zigrestore"))

	require.NoError(t, zigCore.Close())
	require.NoError(t, os.RemoveAll(dbDir))
	require.NoError(t, store.loadAndRecoverFromPersistentSnapshot(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.coreDB.Close())
	})

	doc, err := store.Lookup(context.Background(), "doc1")
	require.NoError(t, err)
	assert.Equal(t, "restored doc", doc["title"])

	matchRestore := query.NewMatchQuery("restore")
	matchRestore.SetField("content")
	req := &indexes.RemoteIndexSearchRequest{
		BleveSearchRequest: bleve.NewSearchRequest(matchRestore),
		Limit:              10,
	}
	req.BleveSearchRequest.Size = 10
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	resBytes, err := store.Search(context.Background(), reqBytes)
	require.NoError(t, err)
	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.NotNil(t, res.BleveSearchResult)
	require.Len(t, res.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc1", res.BleveSearchResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBIndexLifecycleAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	shardID := types.ID(31)
	nodeID := types.ID(9)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	denseConfig := indexes.NewEmbeddingsConfig("dense_index", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetric("l2_squared"),
	})
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))
	require.NoError(t, zigCore.AddIndex(*denseConfig))

	docJSON, err := json.Marshal(map[string]any{
		"title":     "reopen doc",
		"content":   "hello reopen",
		"embedding": []float32{1, 0},
	})
	require.NoError(t, err)
	require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte("doc1"), docJSON}}, nil, Op_SyncLevelFullText))
	require.NoError(t, zigCore.Close())

	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	indexesByName := zigCore.GetIndexes()
	require.Contains(t, indexesByName, "full_text_index")
	require.Contains(t, indexesByName, "dense_index")

	store := &StoreDB{
		logger:    logger,
		dbDir:     dbDir,
		dataDir:   root,
		snapStore: snapStore,
		coreDB:    zigCore,
	}

	req := &indexes.RemoteIndexSearchRequest{
		VectorSearches: map[string]vector.T{"dense_index": {1, 0}},
		Limit:          10,
	}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)
	resBytes, err := store.Search(context.Background(), reqBytes)
	require.NoError(t, err)
	var res indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(resBytes, &res))
	require.Contains(t, res.VectorSearchResult, "dense_index")
	require.Len(t, res.VectorSearchResult["dense_index"].Hits, 1)
	assert.Equal(t, "doc1", res.VectorSearchResult["dense_index"].Hits[0].ID)

	require.NoError(t, zigCore.DeleteIndex("dense_index"))
	require.NoError(t, zigCore.Close())
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	indexesByName = zigCore.GetIndexes()
	require.Contains(t, indexesByName, "full_text_index")
	require.NotContains(t, indexesByName, "dense_index")
}

func TestStoreDB_ZigCoreDBFusionSortAcrossReopen(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	shardID := types.ID(41)
	nodeID := types.ID(11)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() {
		require.NoError(t, zigCore.Close())
	})

	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, zigCore.AddIndex(*denseConfig))
	require.NoError(t, zigCore.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "charlie", "embedding": []float32{1, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{1.0}}},
		"doc2": {"title": "alpha", "embedding": []float32{0.95, 0.05}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.9}}},
		"doc3": {"title": "bravo", "embedding": []float32{0.9, 0.1}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.8}}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelEmbeddings))
	}

	store := &StoreDB{
		logger:    logger,
		dbDir:     dbDir,
		dataDir:   root,
		snapStore: snapStore,
		coreDB:    zigCore,
	}

	run := func(searchAfter []string) *indexes.RemoteIndexSearchResult {
		desc := false
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
				OrderBy: []indexes.SortField{{Field: "title", Desc: &desc}, {Field: "_id", Desc: &desc}},
				Limit:   1,
			},
		}
		req.BlevePagingOpts.SearchAfter = searchAfter

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	first := run(nil)
	require.NotNil(t, first.FusionResult)
	require.Len(t, first.FusionResult.Hits, 1)
	assert.Equal(t, "doc2", first.FusionResult.Hits[0].ID)

	require.NoError(t, zigCore.Close())
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))
	store.coreDB = zigCore

	second := run([]string{"alpha", "doc2"})
	require.NotNil(t, second.FusionResult)
	require.Len(t, second.FusionResult.Hits, 1)
	assert.Equal(t, "doc3", second.FusionResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBSnapshotRestorePreservesFusionAggregationsAndPaging(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	shardID := types.ID(51)
	nodeID := types.ID(13)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))

	store := &StoreDB{
		logger:         logger,
		dbDir:          dbDir,
		dataDir:        root,
		snapStore:      snapStore,
		coreDB:         zigCore,
		loadSnapshotID: func(context.Context) (string, error) { return "zigfusion", nil },
	}

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	sparseConfig := indexes.NewEmbeddingsConfig("sparse_idx", indexes.EmbeddingsIndexConfig{
		Field:  "sparse_embedding",
		Sparse: true,
	})
	require.NoError(t, zigCore.AddIndex(*fullTextConfig))
	require.NoError(t, zigCore.AddIndex(*denseConfig))
	require.NoError(t, zigCore.AddIndex(*sparseConfig))

	for key, doc := range map[string]map[string]any{
		"doc1": {"title": "charlie", "content": "alpha fusion", "embedding": []float32{1, 0}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{1.0}}},
		"doc2": {"title": "alpha", "content": "alpha fusion", "embedding": []float32{0.95, 0.05}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.9}}},
		"doc3": {"title": "bravo", "content": "alpha fusion", "embedding": []float32{0.9, 0.1}, "sparse_embedding": map[string]any{"indices": []uint32{0}, "values": []float32{0.8}}},
	} {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, zigCore.Batch(context.Background(), [][2][]byte{{[]byte(key), docJSON}}, nil, Op_SyncLevelFullText))
	}

	require.NoError(t, store.CreateDBSnapshot("zigfusion"))
	require.NoError(t, zigCore.Close())
	require.NoError(t, os.RemoveAll(dbDir))
	require.NoError(t, store.loadAndRecoverFromPersistentSnapshot(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.coreDB.Close())
	})

	run := func(searchAfter []string) *indexes.RemoteIndexSearchResult {
		desc := false
		matchAlpha := query.NewMatchQuery("alpha")
		matchAlpha.SetField("content")
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(matchAlpha),
			VectorSearches: map[string]vector.T{
				"dense_idx": {1, 0},
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
			AggregationRequests: indexes.AggregationRequests{
				"titles": {Type: "terms", Field: "title", Size: 10},
				"count":  {Type: "count"},
			},
		}
		req.BleveSearchRequest.Size = 3
		req.BlevePagingOpts.SearchAfter = searchAfter

		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	first := run(nil)
	require.NotNil(t, first.FusionResult)
	require.Len(t, first.FusionResult.Hits, 1)
	assert.Equal(t, "doc2", first.FusionResult.Hits[0].ID)
	require.Contains(t, first.AggregationResults, "count")
	assert.Equal(t, float64(3), first.AggregationResults["count"].Value)
	require.Contains(t, first.AggregationResults, "titles")
	require.Len(t, first.AggregationResults["titles"].Buckets, 3)

	second := run([]string{"alpha", "doc2"})
	require.NotNil(t, second.FusionResult)
	require.Len(t, second.FusionResult.Hits, 1)
	assert.Equal(t, "doc3", second.FusionResult.Hits[0].ID)
}

func TestStoreDB_ZigCoreDBSnapshotRestorePreservesGraphVectorFusionSortAndPaging(t *testing.T) {
	t.Setenv("ANTFLY_COREDB", "zigdb")

	root := t.TempDir()
	shardID := types.ID(61)
	nodeID := types.ID(17)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		testStoreSchema(),
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	require.NoError(t, zigCore.Open(dbDir, false, testStoreSchema(), types.Range{nil, []byte{0xFF}}))

	store := &StoreDB{
		logger:         logger,
		dbDir:          dbDir,
		dataDir:        root,
		snapStore:      snapStore,
		coreDB:         zigCore,
		loadSnapshotID: func(context.Context) (string, error) { return "ziggraphfusion", nil },
	}

	graphConfig, err := indexes.NewIndexConfig("citations", indexes.GraphIndexConfig{})
	require.NoError(t, err)
	denseConfig := indexes.NewEmbeddingsConfig("dense_idx", indexes.EmbeddingsIndexConfig{
		Field:          "embedding",
		Dimension:      2,
		DistanceMetric: indexes.DistanceMetricL2Squared,
	})
	require.NoError(t, zigCore.AddIndex(*graphConfig))
	require.NoError(t, zigCore.AddIndex(*denseConfig))

	docs := map[string]map[string]any{
		"a": {"title": "root", "embedding": []float32{1, 0}, "_edges": map[string]any{"citations": map[string]any{"cites": []any{
			map[string]any{"target": "b", "weight": 1.0},
		}}}},
		"b": {"title": "charlie", "embedding": []float32{0.8, 0.2}},
		"c": {"title": "alpha", "embedding": []float32{0.95, 0.05}},
		"d": {"title": "bravo", "embedding": []float32{0.9, 0.1}},
	}
	var batch [][2][]byte
	for key, doc := range docs {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		batch = append(batch, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, zigCore.Batch(context.Background(), batch, nil, Op_SyncLevelEmbeddings))

	require.NoError(t, store.CreateDBSnapshot("ziggraphfusion"))
	require.NoError(t, zigCore.Close())
	require.NoError(t, os.RemoveAll(dbDir))
	require.NoError(t, store.loadAndRecoverFromPersistentSnapshot(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.coreDB.Close())
	})

	run := func(searchAfter []string) *indexes.RemoteIndexSearchResult {
		asc := false
		req := &indexes.RemoteIndexSearchRequest{
			VectorSearches: map[string]vector.T{
				"dense_idx": {1, 0},
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
			ExpandStrategy: "union",
			Limit:          4,
			BlevePagingOpts: indexes.FullTextPagingOptions{
				OrderBy: []indexes.SortField{{Field: "title", Desc: &asc}, {Field: "_id", Desc: &asc}},
				Limit:   1,
			},
			AggregationRequests: indexes.AggregationRequests{
				"count": {Type: "count"},
			},
		}
		req.BlevePagingOpts.SearchAfter = searchAfter
		reqBytes, err := json.Marshal(req)
		require.NoError(t, err)
		resBytes, err := store.Search(context.Background(), reqBytes)
		require.NoError(t, err)
		var res indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(resBytes, &res))
		return &res
	}

	first := run(nil)
	require.NotNil(t, first.FusionResult)
	require.Len(t, first.FusionResult.Hits, 1)
	assert.Equal(t, "c", first.FusionResult.Hits[0].ID)
	require.Contains(t, first.AggregationResults, "count")
	assert.Equal(t, float64(4), first.AggregationResults["count"].Value)

	second := run([]string{"alpha", "c"})
	require.NotNil(t, second.FusionResult)
	require.Len(t, second.FusionResult.Hits, 1)
	assert.Equal(t, "d", second.FusionResult.Hits[0].ID)
}

func newTestStoreDBForSplitReplayZig(t *testing.T, root string, shardID, nodeID types.ID, byteRange types.Range) *StoreDB {
	return newTestStoreDBForSplitReplayZigWithSchema(t, root, shardID, nodeID, byteRange, testStoreSchema())
}

func newTestStoreDBForSplitReplayZigWithSchema(t *testing.T, root string, shardID, nodeID types.ID, byteRange types.Range, storeSchema *schema.TableSchema) *StoreDB {
	t.Helper()
	t.Setenv("ANTFLY_COREDB", "zigdb")

	logger := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	core := newCoreDB(
		logger,
		nil,
		storeSchema,
		map[string]indexes.IndexConfig{},
		snapStore,
		nil,
		(*pebbleutils.Cache)(nil),
	)
	zigCore, ok := core.(*ZigCoreDB)
	require.True(t, ok)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	require.NoError(t, zigCore.Open(dbDir, false, storeSchema, byteRange))

	return &StoreDB{
		logger:    logger,
		dataDir:   root,
		dbDir:     dbDir,
		snapStore: snapStore,
		coreDB:    zigCore,
		byteRange: byteRange,
	}
}

func TestStartSplitReplayIfNeeded_AppliesParentSplitDeltas_ZigCoreDB(t *testing.T) {
	root := t.TempDir()
	parentID := types.ID(110)
	childID := types.ID(111)
	nodeID := types.ID(1)
	splitKey := []byte("m")

	parent := newTestStoreDBForSplitReplayZig(t, root, parentID, nodeID, types.Range{[]byte("a"), splitKey})
	child := newTestStoreDBForSplitReplayZig(t, root, childID, nodeID, types.Range{splitKey, []byte("z")})
	t.Cleanup(func() {
		if child.splitReplayCancel != nil {
			child.splitReplayCancel()
		}
		require.NoError(t, parent.coreDB.Close())
		require.NoError(t, child.coreDB.Close())
	})

	parentSplitState := SplitState_builder{
		Phase:            SplitState_PHASE_SPLITTING,
		SplitKey:         splitKey,
		NewShardId:       uint64(childID),
		OriginalRangeEnd: []byte("z"),
	}.Build()
	parent.splitState = parentSplitState
	require.NoError(t, parent.coreDB.SetSplitState(parentSplitState))

	child.restoredArchiveMetadata = &common.ArchiveMetadata{
		Split: &common.SplitMetadata{
			ParentShardID:  parentID.String(),
			ReplayFenceSeq: 0,
		},
	}
	child.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == parentID {
			return parent
		}
		return nil
	}
	require.NoError(t, child.startSplitReplayIfNeeded())

	value, err := json.Marshal(map[string]any{"name": "mango"})
	require.NoError(t, err)
	writeCtx := storeutils.WithTimestamp(context.Background(), 123)
	require.NoError(t, parent.coreDB.Batch(writeCtx, [][2][]byte{{[]byte("mango"), value}}, nil, Op_SyncLevelWrite))
	splitDeltaSeq, err := parent.coreDB.GetSplitDeltaSeq()
	require.NoError(t, err)
	require.EqualValues(t, 1, splitDeltaSeq)

	require.Eventually(t, func() bool {
		doc, err := child.coreDB.Get(context.Background(), []byte("mango"))
		return err == nil && doc["name"] == "mango" && !child.IsInitializing()
	}, 5*time.Second, 100*time.Millisecond)
}

func TestApplyOpFinalizeSplit_WaitsForLocalChildReplayBeforeDeletingParentRange_ZigCoreDB(t *testing.T) {
	root := t.TempDir()
	parentID := types.ID(120)
	childID := types.ID(121)
	nodeID := types.ID(1)
	splitKey := []byte("m")

	parent := newTestStoreDBForSplitReplayZig(t, root, parentID, nodeID, types.Range{[]byte("a"), splitKey})
	child := newTestStoreDBForSplitReplayZig(t, root, childID, nodeID, types.Range{splitKey, []byte("z")})
	t.Cleanup(func() {
		if child.splitReplayCancel != nil {
			child.splitReplayCancel()
		}
		require.NoError(t, parent.coreDB.Close())
		require.NoError(t, child.coreDB.Close())
	})

	parentSplitState := SplitState_builder{
		Phase:            SplitState_PHASE_SPLITTING,
		SplitKey:         splitKey,
		NewShardId:       uint64(childID),
		OriginalRangeEnd: []byte("z"),
	}.Build()
	parent.splitState = parentSplitState
	require.NoError(t, parent.coreDB.SetSplitState(parentSplitState))

	child.restoredArchiveMetadata = &common.ArchiveMetadata{
		Split: &common.SplitMetadata{
			ParentShardID:  parentID.String(),
			ReplayFenceSeq: 0,
		},
	}
	child.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == parentID {
			return parent
		}
		return nil
	}
	parent.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == childID {
			return child
		}
		return nil
	}
	require.NoError(t, child.startSplitReplayIfNeeded())

	value, err := json.Marshal(map[string]any{"name": "mango"})
	require.NoError(t, err)
	writeCtx := storeutils.WithTimestamp(context.Background(), 456)
	require.NoError(t, parent.coreDB.Batch(writeCtx, [][2][]byte{{[]byte("mango"), value}}, nil, Op_SyncLevelWrite))

	require.NoError(t, parent.applyOpFinalizeSplit(context.Background(), FinalizeSplitOp_builder{
		NewRangeEnd: splitKey,
	}.Build()))

	require.Eventually(t, func() bool {
		doc, err := child.coreDB.Get(context.Background(), []byte("mango"))
		return err == nil && doc["name"] == "mango"
	}, 5*time.Second, 100*time.Millisecond)

	_, err = parent.coreDB.Get(context.Background(), []byte("mango"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestStartSplitReplayIfNeeded_PreservesSchemaDrivenFullTextAnalysis_ZigCoreDB(t *testing.T) {
	root := t.TempDir()
	parentID := types.ID(130)
	childID := types.ID(131)
	nodeID := types.ID(1)
	splitKey := []byte("m")
	storeSchema := testStoreCustomTokenizerAnalyzerSchema()

	parent := newTestStoreDBForSplitReplayZigWithSchema(t, root, parentID, nodeID, types.Range{[]byte("a"), splitKey}, storeSchema)
	child := newTestStoreDBForSplitReplayZigWithSchema(t, root, childID, nodeID, types.Range{splitKey, []byte("z")}, storeSchema)
	t.Cleanup(func() {
		if child.splitReplayCancel != nil {
			child.splitReplayCancel()
		}
		require.NoError(t, parent.coreDB.Close())
		require.NoError(t, child.coreDB.Close())
	})

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, parent.coreDB.AddIndex(*fullTextConfig))
	require.NoError(t, child.coreDB.AddIndex(*fullTextConfig))

	parentSplitState := SplitState_builder{
		Phase:            SplitState_PHASE_SPLITTING,
		SplitKey:         splitKey,
		NewShardId:       uint64(childID),
		OriginalRangeEnd: []byte("z"),
	}.Build()
	parent.splitState = parentSplitState
	require.NoError(t, parent.coreDB.SetSplitState(parentSplitState))

	child.restoredArchiveMetadata = &common.ArchiveMetadata{
		Split: &common.SplitMetadata{
			ParentShardID:  parentID.String(),
			ReplayFenceSeq: 0,
		},
	}
	child.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == parentID {
			return parent
		}
		return nil
	}
	require.NoError(t, child.startSplitReplayIfNeeded())

	value, err := json.Marshal(map[string]any{"title": "<b>Hello</b>"})
	require.NoError(t, err)
	writeCtx := storeutils.WithTimestamp(context.Background(), 789)
	require.NoError(t, parent.coreDB.Batch(writeCtx, [][2][]byte{{[]byte("mango"), value}}, nil, Op_SyncLevelFullText))

	require.Eventually(t, func() bool {
		q := query.NewTermQuery("ell")
		q.SetField("title")
		req := &indexes.RemoteIndexSearchRequest{
			BleveSearchRequest: bleve.NewSearchRequest(q),
			Limit:              10,
		}
		req.BleveSearchRequest.Size = 10
		reqBytes, err := json.Marshal(req)
		if err != nil {
			return false
		}
		resBytes, err := child.Search(context.Background(), reqBytes)
		if err != nil {
			return false
		}
		var res indexes.RemoteIndexSearchResult
		if err := json.Unmarshal(resBytes, &res); err != nil {
			return false
		}
		return res.BleveSearchResult != nil && len(res.BleveSearchResult.Hits) == 1 && res.BleveSearchResult.Hits[0].ID == "mango"
	}, 5*time.Second, 100*time.Millisecond)
}
