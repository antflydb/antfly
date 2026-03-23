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
	"path/filepath"
	"sort"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectChunkReplacementDeletes(t *testing.T) {
	db, err := pebble.Open(filepath.Join(t.TempDir(), "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	docKey := []byte("doc1")
	indexName := "idx"
	embSuffix := []byte(":i:idx:e")
	sumSuffix := []byte(":i:idx:s")

	chunk0 := storeutils.MakeChunkKey(docKey, indexName, 0)
	chunk1 := storeutils.MakeChunkKey(docKey, indexName, 1)
	chunk0Emb := append(append([]byte{}, chunk0...), embSuffix...)
	chunk1Emb := append(append([]byte{}, chunk1...), embSuffix...)
	chunk1Sum := append(append([]byte{}, chunk1...), sumSuffix...)
	docEmbedding := storeutils.MakeEmbeddingKey(docKey, indexName)

	batch := db.NewBatch()
	defer batch.Close()
	require.NoError(t, batch.Set(chunk0, []byte("chunk0"), nil))
	require.NoError(t, batch.Set(chunk1, []byte("chunk1"), nil))
	require.NoError(t, batch.Set(chunk0Emb, []byte("chunk0-emb"), nil))
	require.NoError(t, batch.Set(chunk1Emb, []byte("chunk1-emb"), nil))
	require.NoError(t, batch.Set(chunk1Sum, []byte("chunk1-sum"), nil))
	require.NoError(t, batch.Set(docEmbedding, []byte("doc-emb"), nil))
	require.NoError(t, batch.Commit(pebble.NoSync))

	deletes, err := collectChunkReplacementDeletes(db, docKey, indexName, [][]byte{chunk0}, embSuffix, sumSuffix)
	require.NoError(t, err)

	got := make([]string, 0, len(deletes))
	for _, del := range deletes {
		got = append(got, string(del[0]))
		assert.Nil(t, del[1])
	}
	sort.Strings(got)

	want := []string{
		string(chunk1),
		string(chunk1Emb),
		string(chunk1Sum),
	}
	sort.Strings(want)
	assert.Equal(t, want, got)
}

func TestCollectChunkReplacementDeletes_EmptyReplacementDeletesAllChunkKeys(t *testing.T) {
	db, err := pebble.Open(filepath.Join(t.TempDir(), "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	docKey := []byte("doc1")
	indexName := "idx"
	embSuffix := []byte(":i:idx:e")

	chunk0 := storeutils.MakeChunkFullTextKey(docKey, indexName, 0)
	chunk1 := storeutils.MakeMediaChunkKey(docKey, indexName, 1)
	chunk0Emb := append(append([]byte{}, chunk0...), embSuffix...)

	batch := db.NewBatch()
	defer batch.Close()
	require.NoError(t, batch.Set(chunk0, []byte("chunk0"), nil))
	require.NoError(t, batch.Set(chunk1, []byte("chunk1"), nil))
	require.NoError(t, batch.Set(chunk0Emb, []byte("chunk0-emb"), nil))
	require.NoError(t, batch.Commit(pebble.NoSync))

	deletes, err := collectChunkReplacementDeletes(db, docKey, indexName, nil, embSuffix)
	require.NoError(t, err)

	got := make([]string, 0, len(deletes))
	for _, del := range deletes {
		got = append(got, string(del[0]))
	}
	sort.Strings(got)

	want := []string{
		string(chunk0),
		string(chunk1),
		string(chunk0Emb),
	}
	sort.Strings(want)
	assert.Equal(t, want, got)
}
