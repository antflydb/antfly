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

package indexes

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func setupTestDB(t *testing.T) (*pebble.DB, string, func()) {
	tempDir, err := os.MkdirTemp("", "bleveindexv2_test_*")
	require.NoError(t, err)

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return db, tempDir, cleanup
}

func TestNewBleveIndexV2(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name      string
		config    *IndexConfig
		wantError bool
	}{
		{
			name:      "valid config with mem_only true",
			config:    NewFullTextIndexConfig("", true),
			wantError: false,
		},
		{
			name:      "valid config with mem_only false",
			config:    NewFullTextIndexConfig("", false),
			wantError: false,
		},
		{
			name:      "nil config",
			config:    nil,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "test_index", tt.config, nil)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, idx)
				assert.Equal(t, "test_index", idx.Name())
			}
		})
	}
}

func TestBleveIndexV2_Open(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	// Create test data in pebble
	testData := map[string]any{
		"title":   "Test Document",
		"content": "This is a test document for indexing",
		"tags":    []string{"test", "document"},
	}

	key := []byte("doc1")
	keyWithSuffix := append(key, storeutils.DBRangeStart...)

	// Compress the data
	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	var buf bytes.Buffer
	writer.Reset(&buf)
	err = json.NewEncoder(writer).Encode(testData)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	err = db.Set(keyWithSuffix, buf.Bytes(), pebble.Sync)
	require.NoError(t, err)

	tests := []struct {
		name      string
		config    *IndexConfig
		rebuild   bool
		schema    *schema.TableSchema
		byteRange types.Range
	}{
		{
			name:      "memory only index",
			rebuild:   true,
			schema:    nil,
			byteRange: types.Range{[]byte(""), []byte("\xff")},
		},
		{
			name:      "disk based index with rebuild",
			rebuild:   true,
			schema:    nil,
			byteRange: types.Range{[]byte(""), []byte("\xff")},
		},
		{
			name:      "disk based index without rebuild",
			rebuild:   false,
			schema:    nil,
			byteRange: types.Range{[]byte(""), []byte("\xff")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := NewBleveIndexV2(logger, nil, db, tempDir, tt.name, tt.config, nil)
			require.NoError(t, err)

			bi := idx.(*BleveIndexV2)
			err = bi.Open(tt.rebuild, tt.schema, tt.byteRange)
			assert.NoError(t, err)
			require.NotNil(t, bi.bidx)
			assert.NotNil(t, bi.walBuf)
			assert.NotNil(t, bi.eg)

			// Give some time for rebuild to process
			if tt.rebuild {
				time.Sleep(100 * time.Millisecond)
			}

			err = bi.Close()
			assert.NoError(t, err)
		})
	}
}

func TestBleveIndexV2_BatchOperations(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "batch_test",
		NewFullTextIndexConfig("", true), nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte(""), []byte("\xff")})
	require.NoError(t, err)
	defer bi.Close()

	// Test batch insert
	batch := bi.NewBatch()
	assert.NotNil(t, batch)

	testDocs := []struct {
		key string
		val map[string]any
	}{
		{
			key: "doc1",
			val: map[string]any{
				"title":   "First Document",
				"content": "This is the first test document",
			},
		},
		{
			key: "doc2",
			val: map[string]any{
				"title":     "Second Document",
				"content":   "This is the second test document",
				"embedding": []float32{0.1, 0.2, 0.3}, // Should be removed
			},
		},
		{
			key: "doc3",
			val: map[string]any{
				"title":   "Third Document",
				"content": "This is the third test document",
				"vector":  []any{0.1, 0.2, 0.3}, // Should be removed
			},
		},
	}

	// Insert documents
	for _, doc := range testDocs {
		err := batch.Insert(doc.key, doc.val)
		assert.NoError(t, err)
	}

	// Delete a document
	batch.Delete([]byte("doc1"))

	// Commit batch
	err = batch.Commit()
	assert.NoError(t, err)

	// Give time for processing
	time.Sleep(100 * time.Millisecond)
}

func TestBleveIndexV2_Search(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "search_test",
		NewFullTextIndexConfig("", true), nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte(""), []byte("\xff")})
	require.NoError(t, err)
	defer bi.Close()

	// Index some documents directly
	batch := bi.bidx.NewBatch()
	docs := []struct {
		id      string
		content map[string]any
	}{
		{
			id: "doc1",
			content: map[string]any{
				"title":   "Golang Programming",
				"content": "Go is a statically typed, compiled programming language",
			},
		},
		{
			id: "doc2",
			content: map[string]any{
				"title":   "Python Programming",
				"content": "Python is a dynamically typed, interpreted programming language",
			},
		},
		{
			id: "doc3",
			content: map[string]any{
				"title":   "Database Systems",
				"content": "Databases store and manage data efficiently",
			},
		},
	}

	for _, doc := range docs {
		err := batch.Index(doc.id, doc.content)
		require.NoError(t, err)
	}
	err = bi.bidx.Batch(batch)
	require.NoError(t, err)

	// Test search
	tests := []struct {
		name          string
		searchRequest *bleve.SearchRequest
		expectHits    int
		expectError   bool
	}{
		{
			name: "search for programming",
			searchRequest: &bleve.SearchRequest{
				Query: bleve.NewQueryStringQuery("programming"),
				Size:  10,
			},
			expectHits:  2,
			expectError: false,
		},
		{
			name: "search for golang",
			searchRequest: &bleve.SearchRequest{
				Query: bleve.NewQueryStringQuery("golang"),
				Size:  10,
			},
			expectHits:  1,
			expectError: false,
		},
		{
			name: "search with match phrase",
			searchRequest: &bleve.SearchRequest{
				Query: bleve.NewMatchPhraseQuery("statically typed"),
				Size:  10,
			},
			expectHits:  1,
			expectError: false,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := bi.Search(ctx, tt.searchRequest)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				result, ok := resp.(*bleve.SearchResult)
				require.True(t, ok, "Expected SearchResult type: %T", resp)
				assert.Len(t, result.Hits, tt.expectHits)
			}
		})
	}

	// Test empty request
	_, err = bi.Search(ctx, nil)
	assert.ErrorIs(t, err, storeutils.ErrEmptyRequest)

	// Test invalid request
	_, err = bi.Search(ctx, []byte("invalid json"))
	assert.Error(t, err)
}

func TestSerializationErr(t *testing.T) {
	testData := `{"url":"https://en.wikipedia.org/wiki?curid=12559133","title":"184th Ordnance Battalion (EOD)","body":"\n184th Ordnance Battalion (EOD)\n\nThe 184th Ordnance Battalion (EOD) accomplish the explosive ordnance disposal (EOD) support activity. The EOD battalion operates under United States Army Forces Command (52nd Ordnance Group (EOD)) command and control with several companies (EOD) strategically located within each control area. Installations and MACOMs do not have a direct area support EOD responsibility.\nOrganization.\nSeven Ordnance Companies (EOD).\nFort Campbell, Kentucky\n-49th OD CO (EOD)\n-717th OD CO (EOD)\n-723rd OD CO (EOD)\n-744th OD CO (EOD)\n-788th OD CO (EOD)\nFort Knox, Kentucky\n-703rd OD CO (EOD)\nFort Benning, Georgia\n-789th OD CO (EOD)\n\n"}`
	v := map[string]any{}
	err := json.UnmarshalString(testData, &v)
	require.NoError(t, err)
}

func TestBleveIndexV2_Batch(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	idx, err := NewBleveIndexV2(
		logger,

		nil,
		db,
		tempDir,
		"batch_method_test",
		NewFullTextIndexConfig("", true),
		nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte(""), []byte("\xff")})
	require.NoError(t, err)
	defer bi.Close()

	// Test empty batch
	err = bi.Batch(t.Context(), nil, nil, false)
	assert.NoError(t, err)

	// Test batch with writes
	writes := [][2][]byte{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
	}
	err = bi.Batch(t.Context(), writes, nil, false)
	assert.NoError(t, err)

	// Test batch with deletes
	deletes := [][]byte{
		[]byte("key1"),
		[]byte("key3"),
	}
	err = bi.Batch(t.Context(), nil, deletes, false)
	assert.NoError(t, err)

	// Test batch with both writes and deletes
	err = bi.Batch(t.Context(), writes, deletes, false)
	assert.NoError(t, err)

	// Test large batch that requires partitioning
	largeWrites := make([][2][]byte, 2500)
	for i := range largeWrites {
		key := fmt.Appendf(nil, "key%d", i)
		val := fmt.Appendf(nil, "value%d", i)
		largeWrites[i] = [2][]byte{key, val}
	}

	largeDeletes := make([][]byte, 1500)
	for i := range largeDeletes {
		largeDeletes[i] = fmt.Appendf(nil, "delkey%d", i)
	}

	err = bi.Batch(t.Context(), largeWrites, largeDeletes, false)
	assert.NoError(t, err)
}

func TestFullTextIndexOp(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "op_test",
		NewFullTextIndexConfig("", true), nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte(""), []byte("\xff")})
	require.NoError(t, err)
	defer bi.Close()

	// Test encode/decode
	op1 := &fullTextIndexOp{
		i: bi,
		Writes: [][2][]byte{
			{[]byte("key1"), []byte("val1")},
			{[]byte("key2"), []byte("val2")},
		},
		Deletes: [][]byte{
			[]byte("delkey1"),
		},
	}

	var buf bytes.Buffer
	err = op1.encode(&buf)
	assert.NoError(t, err)
	assert.Positive(t, buf.Len())

	op2 := &fullTextIndexOp{i: bi}
	err = op2.decode(buf.Bytes())
	assert.NoError(t, err)
	assert.Len(t, op2.Writes, len(op1.Writes))
	assert.Len(t, op2.Deletes, len(op1.Deletes))

	// Test merge
	op3 := &fullTextIndexOp{
		i: bi,
		Writes: [][2][]byte{
			{[]byte("key3"), []byte("val3")},
		},
		Deletes: [][]byte{
			[]byte("delkey2"),
		},
	}

	var buf2 bytes.Buffer
	err = op3.encode(&buf2)
	assert.NoError(t, err)

	err = op1.Merge(buf2.Bytes())
	assert.NoError(t, err)
	assert.Len(t, op1.Writes, 3)
	assert.Len(t, op1.Deletes, 2)
}

func TestFullTextIndexOp_ConcurrentEncodeDecode(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "concurrent_op_test",
		NewFullTextIndexConfig("", true), nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)

	const goroutines = 32
	const iterations = 200

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := range iterations {
				op := &fullTextIndexOp{
					i: bi,
					Writes: [][2][]byte{
						{
							fmt.Appendf(nil, "key-%d-%d", id, j),
							fmt.Appendf(nil, "value-%d-%d", id, j),
						},
					},
					Deletes: [][]byte{
						fmt.Appendf(nil, "delete-%d-%d", id, j),
					},
				}

				var buf bytes.Buffer
				if err := op.encode(&buf); err != nil {
					errCh <- err
					return
				}

				decoded := &fullTextIndexOp{i: bi}
				if err := decoded.decode(buf.Bytes()); err != nil {
					errCh <- err
					return
				}

				if len(decoded.Writes) != 1 || len(decoded.Deletes) != 1 {
					errCh <- fmt.Errorf(
						"unexpected decoded sizes writes=%d deletes=%d",
						len(decoded.Writes),
						len(decoded.Deletes),
					)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestBleveIndexV2_ConcurrentOperations(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	idx, err := NewBleveIndexV2(
		logger,
		nil,
		db,
		tempDir,
		"concurrent_test",
		NewFullTextIndexConfig("", true),
		nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte(""), []byte("\xff")})
	require.NoError(t, err)
	defer bi.Close()

	// Run concurrent batch operations
	numGoroutines := 10
	numOpsPerGoroutine := 100

	errCh := make(chan error, numGoroutines)
	for i := range numGoroutines {
		go func(goroutineID int) {
			for j := range numOpsPerGoroutine {
				batch := bi.NewBatch()
				key := fmt.Sprintf("goroutine%d_doc%d", goroutineID, j)
				val := map[string]any{
					"title":   fmt.Sprintf("Document from goroutine %d", goroutineID),
					"content": fmt.Sprintf("This is document %d from goroutine %d", j, goroutineID),
				}

				if err := batch.Insert(key, val); err != nil {
					errCh <- err
					return
				}

				if err := batch.Commit(); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}(i)
	}

	// Wait for all goroutines to complete
	for range numGoroutines {
		err := <-errCh
		assert.NoError(t, err)
	}
}

func TestBleveIndexV2_ErrHandling(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	// Test closing already closed index
	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "error_test",
		NewFullTextIndexConfig("", true), nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte(""), []byte("\xff")})
	require.NoError(t, err)

	err = bi.Close()
	assert.NoError(t, err)

	// Operations after close should handle gracefully
	batch := bi.NewBatch()
	err = batch.Insert("key", map[string]any{"field": "value"})
	// This might succeed as the batch is created, but commit should fail
	if err == nil {
		err = batch.Commit()
		assert.Error(t, err)
	}
}

func TestBleveIndexV2_RebuildFromPebble(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	// Prepare test data in Pebble
	testDocs := []struct {
		key  string
		data map[string]any
	}{
		{
			key: "rebuild_doc1",
			data: map[string]any{
				"title":   "Rebuild Test 1",
				"content": "This document should be indexed during rebuild",
			},
		},
		{
			key: "rebuild_doc2",
			data: map[string]any{
				"title":   "Rebuild Test 2",
				"content": "Another document for rebuild testing",
			},
		},
	}

	writer, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	for _, doc := range testDocs {
		buf := bytes.NewBuffer(nil)
		writer.Reset(buf)
		err = json.NewEncoder(writer).Encode(doc.data)
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)

		keyWithSuffix := append([]byte(doc.key), storeutils.DBRangeStart...)
		err = db.Set(keyWithSuffix, buf.Bytes(), pebble.Sync)
		require.NoError(t, err)
	}

	// Create index with rebuild
	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "rebuild_test", nil, nil)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte("rebuild_"), []byte("rebuild_\xff")})
	require.NoError(t, err)

	// Wait for rebuild to complete
	time.Sleep(500 * time.Millisecond)

	// Search for rebuilt documents
	searchReq := &bleve.SearchRequest{
		Query: bleve.NewQueryStringQuery("rebuild"),
		Size:  10,
	}

	resp, err := bi.Search(context.Background(), searchReq)
	require.NoError(t, err)

	result, ok := resp.(*bleve.SearchResult)
	require.True(t, ok)
	assert.Len(t, result.Hits, 2)

	err = bi.Close()
	assert.NoError(t, err)
}

func TestBleveIndexV2_SearchAsYouType(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	db, tempDir, cleanup := setupTestDB(t)
	defer cleanup()

	// Create schema with search_as_you_type field
	schema := &schema.TableSchema{
		DefaultType: "product",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"product": {
				Schema: map[string]any{
					"type":                 "object",
					"additionalProperties": true,
					"properties": map[string]any{
						"name": map[string]any{
							"type":           "string",
							"x-antfly-types": []string{"search_as_you_type", "keyword"},
						},
						"description": map[string]any{
							"type":           "string",
							"x-antfly-types": []string{"text"},
						},
					},
				},
			},
		},
	}

	// Initialize BleveIndexV2
	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "search_as_you_type_test",
		NewFullTextIndexConfig("", true), nil, // memory-only for testing
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, schema, types.Range{[]byte{}, []byte{0xff}})
	require.NoError(t, err)
	defer bi.Close()

	// Verify the mapping has the correct structure
	mappingIf := bi.bidx.Mapping()
	impl, ok := mappingIf.(*mapping.IndexMappingImpl)
	require.True(t, ok)

	// Should have the correct field mappings for the name field
	productMapping := impl.TypeMapping["product"]
	require.NotNil(t, productMapping)
	nameMapping := productMapping.Properties["name"]
	require.NotNil(t, nameMapping)
	assert.Len(
		t,
		nameMapping.Fields,
		3,
		"name field should have 3 mappings: __2gram, __keyword, and text",
	)

	// Insert documents directly using batch insert (simpler than rebuild process)
	testDocs := []struct {
		id   string
		data map[string]any
	}{
		{
			id: "doc1",
			data: map[string]any{
				"name":        "Smartphone Apple iPhone",
				"description": "Latest iPhone model with advanced features",
				"_type":       "product",
			},
		},
		{
			id: "doc2",
			data: map[string]any{
				"name":        "Smart Television Samsung",
				"description": "High-definition smart TV with streaming capabilities",
				"_type":       "product",
			},
		},
		{
			id: "doc3",
			data: map[string]any{
				"name":        "Smartwatch Fitbit",
				"description": "Fitness tracker with heart rate monitoring",
				"_type":       "product",
			},
		},
		{
			id: "doc4",
			data: map[string]any{
				"name":        "Gaming Console PlayStation",
				"description": "Next-generation gaming console",
				"_type":       "product",
			},
		},
	}

	// Insert documents directly using BleveIndexV2 batch
	batch := bi.NewBatch()
	for _, doc := range testDocs {
		err := batch.Insert(doc.id, doc.data)
		require.NoError(t, err)
	}
	err = batch.Commit()
	require.NoError(t, err)

	// Give some time for indexing to complete
	time.Sleep(100 * time.Millisecond)

	// Test proper Bleve queries for search_as_you_type functionality
	tests := []struct {
		name        string
		query       query.Query
		expectHits  int
		expectDocs  []string
		description string
	}{
		{
			name: "query string for 'sm' on n-gram field",
			query: func() query.Query {
				q := bleve.NewTermQuery("sm")
				q.SetField("name__2gram")
				return q
			}(),
			expectHits:  3,
			expectDocs:  []string{"doc1", "doc2", "doc3"},
			description: "Should match Smart* products via n-grams",
		},
		{
			name: "query string for 'sma' on n-gram field",
			query: func() query.Query {
				q := bleve.NewTermQuery("sma")
				q.SetField("name__2gram")
				return q
			}(),
			expectHits:  3,
			expectDocs:  []string{"doc1", "doc2", "doc3"},
			description: "Should match Smart* products via n-grams",
		},
		{
			name: "term query for 'smar' on n-gram field",
			query: func() query.Query {
				q := bleve.NewTermQuery("smar")
				q.SetField("name__2gram")
				return q
			}(),
			expectHits:  3,
			expectDocs:  []string{"doc1", "doc2", "doc3"},
			description: "Should match Smart* products via n-grams (within 4-char limit)",
		},
		{
			name: "term query for 'iph' on n-gram field",
			query: func() query.Query {
				q := bleve.NewTermQuery("iph")
				q.SetField("name__2gram")
				return q
			}(),
			expectHits:  1,
			expectDocs:  []string{"doc1"},
			description: "Should match iPhone via n-grams",
		},
		{
			name:        "query string for 'ga' on n-gram field (test original approach)",
			query:       bleve.NewQueryStringQuery("name__2gram:ga"),
			expectHits:  1,
			expectDocs:  []string{"doc4"},
			description: "Should match Gaming via n-grams using query string",
		},
		{
			name: "prefix query for 'sm' on name field",
			query: func() query.Query {
				q := bleve.NewPrefixQuery("sm")
				q.SetField("name")
				return q
			}(),
			expectHits:  3,
			expectDocs:  []string{"doc1", "doc2", "doc3"},
			description: "Should match Smart* products via prefix",
		},
		{
			name: "wildcard query for 'sm*' on name field",
			query: func() query.Query {
				q := bleve.NewWildcardQuery("sm*")
				q.SetField("name")
				return q
			}(),
			expectHits:  3,
			expectDocs:  []string{"doc1", "doc2", "doc3"},
			description: "Should match Smart* products via wildcard",
		},
		{
			name: "match query for 'smartphone' on name field",
			query: func() query.Query {
				q := bleve.NewTermQuery("smartphone")
				q.SetField("name")
				return q
			}(),
			expectHits:  1,
			expectDocs:  []string{"doc1"},
			description: "Should match exact smartphone term",
		},
		{
			name: "match query for 'smartphone' on name field",
			query: func() query.Query {
				q := bleve.NewMatchQuery("Smartphone Apple iPhone")
				q.SetField("name")
				return q
			}(),
			expectHits:  1,
			expectDocs:  []string{"doc1"},
			description: "Should match smartphone terms on name field",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searchRequest := bleve.NewSearchRequest(tt.query)
			searchRequest.Size = 10

			result, err := bi.Search(ctx, searchRequest)
			require.NoError(t, err)

			searchResult, ok := result.(*bleve.SearchResult)
			require.True(t, ok, "Expected SearchResult type: %T", result)
			assert.Len(
				t,
				searchResult.Hits,
				tt.expectHits,
				"Query: %s - %s",
				tt.name,
				tt.description,
			)

			// Verify expected documents are returned
			if len(tt.expectDocs) > 0 {
				returnedDocs := make([]string, len(searchResult.Hits))
				for i, hit := range searchResult.Hits {
					returnedDocs[i] = hit.ID
				}
				for _, expectedDoc := range tt.expectDocs {
					assert.Contains(
						t,
						returnedDocs,
						expectedDoc,
						"Expected doc %s in results for %s",
						expectedDoc,
						tt.name,
					)
				}
			}
		})
	}
}
