// Copyright 2026 Antfly, Inc.
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
	"errors"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/schema"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// corruptDir overwrites every non-empty regular file under root with random bytes
// of the same length, simulating on-disk artifact corruption (as the SearchAF repro
// does to the persisted embeddings index directory).
func corruptDir(t *testing.T, root string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size := info.Size()
		if size == 0 {
			return nil
		}
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte(rand.IntN(256))
		}
		return os.WriteFile(path, buf, info.Mode().Perm())
	}))
}

// TestCorruptIndexDegradedStartupAndDrop reproduces the SearchAF scenario where an
// embeddings index artifact is corrupt on disk after a restart. The shard must come
// up degraded (the corrupt index is unavailable, but the DB still opens) and a
// subsequent DropIndex must remove the index config and artifacts without hanging.
func TestCorruptIndexDegradedStartupAndDrop(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)
	byteRange := types.Range{nil, []byte{0xFF}}
	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	db := &DBImpl{logger: lg}
	require.NoError(t, db.Open(dir, false, tableSchema, byteRange))
	t.Cleanup(func() { _ = db.Close() })

	// Add a managed external embeddings index (no model needed) so its on-disk
	// vector-index artifacts are created under <dir>/indexes/test_idx.
	cfg := indexes.NewEmbeddingsConfig("test_idx", indexes.EmbeddingsIndexConfig{
		Dimension: 8,
		External:  true,
	})
	require.NoError(t, db.AddIndex(*cfg))

	idxDir := filepath.Join(dir, "indexes", "test_idx")
	require.DirExists(t, idxDir)
	require.True(t, db.HasIndex("test_idx"))

	// Release file handles, then corrupt the persisted artifacts, mimicking a crash
	// that left the index unreadable on disk.
	require.NoError(t, db.getIndexManager().Close(context.Background()))
	corruptDir(t, idxDir)

	// Re-open the indexes as a restart would (recoverIndex=true loads existing
	// artifacts rather than rebuilding them). This must NOT fail the whole shard:
	// the corrupt index is recorded as failed and skipped, everything else opens.
	require.NoError(t, db.openIndex(dir, true))

	require.True(t, db.IsIndexFailed("test_idx"), "corrupt index should be marked failed")
	require.True(t, db.HasIndexConfig("test_idx"), "config should remain so it can be dropped")
	require.False(t, db.HasIndex("test_idx"), "corrupt index must not be live")

	// Querying a degraded index returns a clear error rather than silently nothing.
	_, searchErr := db.getIndexManager().Search(context.Background(), "test_idx", nil)
	require.Error(t, searchErr)

	// DropIndex must succeed and clean up config + on-disk artifacts, even though the
	// index never loaded.
	require.NoError(t, db.DeleteIndex("test_idx"))
	require.False(t, db.HasIndexConfig("test_idx"))
	require.False(t, db.IsIndexFailed("test_idx"))
	require.NoDirExists(t, idxDir)
}

// TestHealthyIndexDropStillWorks guards against regressing the normal path: a healthy
// loaded index drops cleanly.
func TestHealthyIndexDropStillWorks(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)
	byteRange := types.Range{nil, []byte{0xFF}}

	db := &DBImpl{logger: lg}
	require.NoError(t, db.Open(dir, false, nil, byteRange))
	t.Cleanup(func() { _ = db.Close() })

	cfg := indexes.NewEmbeddingsConfig("test_idx", indexes.EmbeddingsIndexConfig{
		Dimension: 8,
		External:  true,
	})
	require.NoError(t, db.AddIndex(*cfg))

	idxDir := filepath.Join(dir, "indexes", "test_idx")
	require.DirExists(t, idxDir)
	require.True(t, db.HasIndex("test_idx"))

	require.NoError(t, db.DeleteIndex("test_idx"))
	require.False(t, db.HasIndex("test_idx"))
	require.False(t, db.HasIndexConfig("test_idx"))
	require.NoDirExists(t, idxDir)
}

func TestOpenIndexConstructorErrorIsFatal(t *testing.T) {
	dir := t.TempDir()
	db := &DBImpl{
		logger: zaptest.NewLogger(t),
		indexes: map[string]indexes.IndexConfig{
			"bad_idx": {
				Name: "bad_idx",
				Type: indexes.IndexType("unsupported"),
			},
		},
	}
	defer func() { _ = db.Close() }()

	err := db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported index type")
	require.False(t, db.IsIndexFailed("bad_idx"))
}

func TestAddIndexClearsFailedIndexMarker(t *testing.T) {
	dir := t.TempDir()
	db := &DBImpl{logger: zaptest.NewLogger(t)}
	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	t.Cleanup(func() { _ = db.Close() })

	db.markIndexFailed("test_idx", errors.New("old open failure"))
	require.True(t, db.IsIndexFailed("test_idx"))

	cfg := indexes.NewFullTextIndexConfig("test_idx", true)
	require.NoError(t, db.AddIndex(*cfg))
	require.True(t, db.HasIndex("test_idx"))
	require.False(t, db.IsIndexFailed("test_idx"))
}
