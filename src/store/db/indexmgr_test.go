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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// MockIndex implements the Index interface for testing
type MockIndex struct {
	name      string
	indexType IndexType
	batches   []batchCall
	searches  []searchCall
	closed    bool
	openCalls int
	failBatch bool
	failOpen  bool
}

type batchCall struct {
	writes  [][2][]byte
	deletes [][]byte
}

type searchCall struct {
	query any
	err   error
}

type mockBackfillableIndex struct {
	*MockIndex
	backfillDone chan struct{}
}

func (m *MockIndex) Name() string {
	return m.name
}

func (m *MockIndex) Delete() error {
	return nil
}

func (m *MockIndex) Stats() indexes.IndexStats {
	return indexes.IndexStats{}
}

func (m *MockIndex) Batch(_ context.Context, writes [][2][]byte, deletes [][]byte, _ bool) error {
	if m.failBatch {
		return errors.New("batch failed")
	}
	m.batches = append(m.batches, batchCall{
		writes:  writes,
		deletes: deletes,
	})
	return nil
}

func (m *MockIndex) Search(ctx context.Context, query any) (any, error) {
	m.searches = append(m.searches, searchCall{
		query: query,
	})
	if len(m.searches) > 0 && m.searches[len(m.searches)-1].err != nil {
		return nil, m.searches[len(m.searches)-1].err
	}
	return []byte("mock search result"), nil
}

func (m *MockIndex) Close() error {
	m.closed = true
	return nil
}

func (m *MockIndex) Open(
	rebuild bool,
	schema *schema.TableSchema,
	byteRange types.Range,
) error {
	if m.failOpen {
		return errors.New("open failed")
	}
	m.openCalls++
	return nil
}

func (m *MockIndex) Type() IndexType {
	if m.indexType != "" {
		return m.indexType
	}
	return indexes.IndexTypeFullTextV0
}

func (m *MockIndex) UpdateSchema(newSchema *schema.TableSchema) error {
	return nil
}

func (m *MockIndex) UpdateRange(newRange types.Range) error {
	return nil
}

func (m *MockIndex) Pause(ctx context.Context) error {
	return nil
}

func (m *MockIndex) Resume() {
}

func (m *mockBackfillableIndex) WaitForBackfill(ctx context.Context) {
	if m.backfillDone == nil {
		return
	}
	select {
	case <-m.backfillDone:
	case <-ctx.Done():
	}
}

// Register a mock index type for testing
func init() {
	indexes.RegisterIndex(
		"mock",
		func(logger *zap.Logger, antflyConfig *common.Config, db *pebble.DB, dir string, name string, config *indexes.IndexConfig, _ *pebbleutils.Cache) (Index, error) {
			return &MockIndex{name: name}, nil
		},
	)
}

func TestIndexManagerWaitForNamedBackfills(t *testing.T) {
	t.Parallel()

	im := &IndexManager{
		logger:  zaptest.NewLogger(t),
		indexes: xsync.NewMap[string, Index](),
	}

	done := make(chan struct{})
	im.indexes.Store("embedding_index", &mockBackfillableIndex{
		MockIndex:    &MockIndex{name: "embedding_index"},
		backfillDone: done,
	})
	im.indexes.Store("full_text_index", &mockBackfillableIndex{
		MockIndex:    &MockIndex{name: "full_text_index"},
		backfillDone: make(chan struct{}),
	})

	waitDone := make(chan struct{})
	go func() {
		im.WaitForNamedBackfills(context.Background(), []string{"embedding_index"})
		close(waitDone)
	}()

	close(done)

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("WaitForNamedBackfills blocked on an unrelated index")
	}
}

func TestNewIndexManager(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	schema := &schema.TableSchema{}
	byteRange := types.Range{[]byte("a"), []byte("z")}

	im, err := NewIndexManager(logger, nil, db, tempDir, schema, byteRange, nil)
	require.NoError(t, err)
	require.NotNil(t, im)

	assert.Equal(t, tempDir, im.dir)
	assert.Equal(t, db, im.db)
	assert.Equal(t, schema, im.schema)
	assert.Equal(t, byteRange, im.byteRange)
}

func TestIndexManager_Register(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	// Start the index manager
	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Test successful registration
	err = im.Register("test-index", false, indexes.IndexConfig{
		Type: "mock",
	})
	require.NoError(t, err)
	assert.True(t, im.HasIndex("test-index"))

	// Test duplicate registration
	err = im.Register("test-index", false, indexes.IndexConfig{
		Type: "mock",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")

	// Test empty name
	err = im.Register("", false, indexes.IndexConfig{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index name cannot be empty")

	// Test unsupported index type
	err = im.Register("bad-index", false, indexes.IndexConfig{
		Type: "unsupported",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported index type")
}

func TestIndexManager_Unregister(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Register an index
	err = im.Register("test-index", false, indexes.IndexConfig{
		Type: "mock",
	})
	require.NoError(t, err)

	// Unregister it
	err = im.Unregister("test-index")
	require.NoError(t, err)
	assert.False(t, im.HasIndex("test-index"))

	// Try to unregister non-existent index
	err = im.Unregister("non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}

func TestIndexManager_Batch(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	byteRange := types.Range{[]byte("a"), []byte("z")}
	im, err := NewIndexManager(logger, nil, db, tempDir, &schema.TableSchema{}, byteRange, nil)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Register a mock index
	err = im.Register("test-index", false, indexes.IndexConfig{
		Type: "mock",
	})
	require.NoError(t, err)

	// Test batch with writes and deletes
	writes := [][2][]byte{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
	}
	deletes := [][]byte{
		[]byte("key3"),
		[]byte("key4"),
	}

	err = im.Batch(t.Context(), writes, deletes, Op_SyncLevelPropose)
	require.NoError(t, err)

	// Test empty batch (should return early)
	err = im.Batch(t.Context(), nil, nil, Op_SyncLevelPropose)
	require.NoError(t, err)

	// Give some time for async processing
	time.Sleep(100 * time.Millisecond)
}

func TestIndexManager_DeleteKeysRoutesByIndexType(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(logger, nil, db, tempDir, &schema.TableSchema{}, types.Range{nil, nil}, nil)
	require.NoError(t, err)

	fullText := &MockIndex{name: "full_text_index", indexType: indexes.IndexTypeFullText}
	embA := &MockIndex{name: "emb_a", indexType: indexes.IndexTypeEmbeddings}
	embB := &MockIndex{name: "emb_b", indexType: indexes.IndexTypeEmbeddings}
	graph := &MockIndex{name: "graph_idx", indexType: indexes.IndexTypeGraph}

	im.indexes.Store(fullText.name, fullText)
	im.indexes.Store(embA.name, embA)
	im.indexes.Store(embB.name, embB)
	im.indexes.Store(graph.name, graph)

	docKey := []byte("doc1")
	chunkA := storeutils.MakeChunkKey(docKey, embA.name, 0)
	chunkB := storeutils.MakeChunkKey(docKey, embB.name, 1)
	edge := storeutils.MakeEdgeKey([]byte("doc1"), []byte("doc2"), graph.name, "rel")

	require.NoError(t, im.DeleteKeys(t.Context(), [][]byte{
		docKey,
		chunkA,
		chunkB,
		edge,
		docKey, // duplicate should be deduped
	}))

	require.Len(t, fullText.batches, 1)
	require.Equal(t, [][]byte{docKey}, fullText.batches[0].deletes)

	require.Len(t, embA.batches, 1)
	require.Equal(t, [][]byte{docKey, chunkA}, embA.batches[0].deletes)

	require.Len(t, embB.batches, 1)
	require.Equal(t, [][]byte{docKey, chunkB}, embB.batches[0].deletes)

	require.Len(t, graph.batches, 1)
	require.Equal(t, [][]byte{edge}, graph.batches[0].deletes)
}

func TestIndexManager_DeleteKeysRoutesChunkDeletesWithIndexMarkerInDocKey(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(logger, nil, db, tempDir, &schema.TableSchema{}, types.Range{nil, nil}, nil)
	require.NoError(t, err)

	emb := &MockIndex{name: "emb_a", indexType: indexes.IndexTypeEmbeddings}
	im.indexes.Store(emb.name, emb)

	docKey := []byte("tenant:i:42")
	chunkKey := storeutils.MakeChunkKey(docKey, emb.name, 0)

	require.NoError(t, im.DeleteKeys(t.Context(), [][]byte{docKey, chunkKey}))
	require.Len(t, emb.batches, 1)
	require.Equal(t, [][]byte{docKey, chunkKey}, emb.batches[0].deletes)
}

func TestIndexManager_Search(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Register a mock index
	err = im.Register("test-index", false, indexes.IndexConfig{
		Type: "mock",
	})
	require.NoError(t, err)

	// Test search
	ctx := context.Background()
	result, err := im.Search(ctx, "test-index", []byte("test query"))
	require.NoError(t, err)
	assert.Equal(t, []byte("mock search result"), result)

	// Test search on non-existent index
	_, err = im.Search(ctx, "non-existent", []byte("test query"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}

func TestIndexOp_EncodeDecode(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	// Create an indexOp
	io := &indexOp{
		Writes: [][2][]byte{
			{[]byte("key1"), []byte("value1")},
			{[]byte("key2"), []byte("value2")},
		},
		Deletes: [][]byte{
			[]byte("key3"),
		},
		im: im,
	}

	// Encode
	buf := bytes.NewBuffer(nil)
	encoded, err := io.encode(buf)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	// Decode
	io2 := &indexOp{im: im}
	err = io2.decode(encoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, io.Writes, io2.Writes)
	assert.Equal(t, io.Deletes, io2.Deletes)
}

func TestIndexOp_ConcurrentEncodeDecode(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	const goroutines = 32
	const iterations = 200

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := range iterations {
				io := &indexOp{
					Writes: [][2][]byte{
						{
							fmt.Appendf(nil, "key-%d-%d", id, j),
							fmt.Appendf(nil, "value-%d-%d", id, j),
						},
					},
					Deletes: [][]byte{
						fmt.Appendf(nil, "delete-%d-%d", id, j),
					},
					im: im,
				}

				var buf bytes.Buffer
				encoded, err := io.encode(&buf)
				if err != nil {
					errCh <- err
					return
				}

				decoded := &indexOp{im: im}
				if err := decoded.decode(encoded); err != nil {
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

func TestIndexOp_Execute(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	byteRange := types.Range{[]byte("a"), []byte("z")}
	im, err := NewIndexManager(logger, nil, db, tempDir, &schema.TableSchema{}, byteRange, nil)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Register a mock index
	err = im.Register("test-index", false, indexes.IndexConfig{
		Type: "mock",
	})
	require.NoError(t, err)

	// Get the mock index to verify calls
	idx, _ := im.indexes.Load("test-index")
	mockIdx := idx.(*MockIndex)

	// Create and execute an indexOp
	io := &indexOp{
		Writes: [][2][]byte{
			{[]byte("key1"), []byte("value1")},
			{[]byte("key2"), []byte("value2")},
			{[]byte("zzz"), []byte("out of range")}, // This should be filtered out
		},
		Deletes: [][]byte{
			[]byte("key3"),
			{}, // Empty key should be filtered out
		},
		im: im,
	}

	ctx := t.Context()
	err = io.Execute(ctx)
	require.NoError(t, err)

	// Verify the mock index received the batch (minus filtered items)
	assert.Len(t, mockIdx.batches, 1)
	assert.Len(t, mockIdx.batches[0].writes, 2)  // zzz was filtered out
	assert.Len(t, mockIdx.batches[0].deletes, 1) // empty key was filtered out
}

func TestIndexOp_Merge(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	// Create first indexOp
	io1 := &indexOp{
		Writes: [][2][]byte{
			{[]byte("key1"), []byte("value1")},
		},
		Deletes: [][]byte{
			[]byte("del1"),
		},
		im: im,
	}

	// Create second indexOp and encode it
	io2 := &indexOp{
		Writes: [][2][]byte{
			{[]byte("key2"), []byte("value2")},
		},
		Deletes: [][]byte{
			[]byte("del2"),
		},
		im: im,
	}

	buf := bytes.NewBuffer(nil)
	encoded, err := io2.encode(buf)
	require.NoError(t, err)

	// Merge
	err = io1.Merge(encoded)
	require.NoError(t, err)

	// Verify merge
	assert.Len(t, io1.Writes, 2)
	assert.Len(t, io1.Deletes, 2)
}

func TestIndexManager_Close(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)

	// Register some indexes
	err = im.Register("test-index-1", false, indexes.IndexConfig{
		Type: "mock",
	})
	require.NoError(t, err)
	err = im.Register("test-index-2", false, indexes.IndexConfig{
		Type: "mock",
	})
	require.NoError(t, err)

	// Close the index manager
	err = im.Close(t.Context())
	require.NoError(t, err)

	// Verify all indexes were closed
	im.indexes.Range(func(key string, value Index) bool {
		mockIdx := value.(*MockIndex)
		assert.True(t, mockIdx.closed)
		return true
	})
}

func TestIndexManager_StartWithRebuild(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	// Create WAL directory to verify it gets removed on rebuild
	walDir := filepath.Join(tempDir, "indexManagerWAL")
	err = os.MkdirAll(walDir, 0o755)
	require.NoError(t, err)

	// Create a test file in WAL directory
	testFile := filepath.Join(walDir, "test.wal")
	err = os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	// Start with rebuild=true
	err = im.Start(true)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Verify WAL directory was recreated (old file should be gone)
	_, err = os.Stat(testFile)
	assert.True(t, os.IsNotExist(err))
}

func TestIndexManager_RegisterWithOpenErr(t *testing.T) {
	// Register a failing mock index type
	indexes.RegisterIndex(
		"mock-fail-open",
		func(logger *zap.Logger, antflyConfig *common.Config, db *pebble.DB, dir string, name string, config *indexes.IndexConfig, _ *pebbleutils.Cache) (Index, error) {
			return &MockIndex{name: name, failOpen: true}, nil
		},
	)

	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Try to register an index that fails to open
	err = im.Register("fail-index", false, indexes.IndexConfig{Type: "mock-fail-open"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "opening index")

	// Verify the index was not added
	assert.False(t, im.HasIndex("fail-index"))
}

func TestIndexManager_PauseResume(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Register some indexes
	err = im.Register("test-index-1", false, indexes.IndexConfig{Type: "mock"})
	require.NoError(t, err)
	err = im.Register("test-index-2", false, indexes.IndexConfig{Type: "mock"})
	require.NoError(t, err)

	ctx := context.Background()

	// Test pause
	err = im.Pause(ctx)
	require.NoError(t, err)
	assert.True(t, im.paused.Load())

	// Test pause is idempotent
	err = im.Pause(ctx)
	require.NoError(t, err)
	assert.True(t, im.paused.Load())

	// Test resume
	im.Resume()
	assert.False(t, im.paused.Load())

	// Test resume is idempotent
	im.Resume()
	assert.False(t, im.paused.Load())
}

func TestIndexManager_PauseBlocksRegister(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	ctx := context.Background()

	// Pause the index manager
	err = im.Pause(ctx)
	require.NoError(t, err)

	// Try to register an index while paused (should block)
	registerDone := make(chan struct{})
	go func() {
		err := im.Register("blocked-index", false, indexes.IndexConfig{Type: "mock"})
		assert.NoError(t, err)
		close(registerDone)
	}()

	// Give the goroutine time to attempt registration
	time.Sleep(100 * time.Millisecond)

	// Verify registration hasn't completed
	select {
	case <-registerDone:
		t.Fatal("Register should be blocked during pause")
	default:
		// Expected - register is blocked
	}

	// Resume should unblock registration
	im.Resume()

	// Wait for registration to complete
	select {
	case <-registerDone:
		// Success - registration completed after resume
	case <-time.After(2 * time.Second):
		t.Fatal("Register did not complete after resume")
	}

	// Verify index was registered
	assert.True(t, im.HasIndex("blocked-index"))
}

func TestIndexManager_PauseWithTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	im, err := NewIndexManager(
		logger,
		nil,
		db,
		tempDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	err = im.Start(false)
	require.NoError(t, err)
	defer im.Close(t.Context())

	// Create a context with a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Sleep to ensure context expires
	time.Sleep(10 * time.Millisecond)

	// Pause with expired context should fail
	err = im.Pause(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pause timeout")
	assert.False(t, im.paused.Load(), "IndexManager should not be paused after timeout")
}

func TestIndexManager_GetDir(t *testing.T) {
	logger := zaptest.NewLogger(t)
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	indexDir := filepath.Join(tempDir, "indexes")
	im, err := NewIndexManager(
		logger,
		nil,
		db,
		indexDir,
		&schema.TableSchema{},
		types.Range{nil, nil},
		nil,
	)
	require.NoError(t, err)

	// Test GetDir returns correct path
	assert.Equal(t, indexDir, im.GetDir())
}
