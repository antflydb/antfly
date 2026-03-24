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
	"strconv"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type splitBenchConfig struct {
	docs         int
	chunksPerDoc int
}

func (c splitBenchConfig) name() string {
	return fmt.Sprintf("docs=%d/chunks=%d", c.docs, c.chunksPerDoc)
}

func splitBenchConfigs() []splitBenchConfig {
	docs, hasDocs := splitBenchIntEnv("ANTFLY_SPLIT_BENCH_DOCS")
	chunks, hasChunks := splitBenchIntEnv("ANTFLY_SPLIT_BENCH_CHUNKS_PER_DOC")
	if hasDocs || hasChunks {
		cfg := splitBenchConfig{
			docs:         2000,
			chunksPerDoc: 0,
		}
		if hasDocs {
			cfg.docs = docs
		}
		if hasChunks {
			cfg.chunksPerDoc = chunks
		}
		return []splitBenchConfig{cfg}
	}

	return []splitBenchConfig{
		{docs: 1000, chunksPerDoc: 0},
		{docs: 1000, chunksPerDoc: 2},
	}
}

func splitBenchIntEnv(name string) (int, bool) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

func splitBenchSchema() *schema.TableSchema {
	return &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":      map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
						"value":   map[string]any{"type": "number"},
					},
				},
			},
		},
	}
}

func setupSplitBenchDB(
	b *testing.B,
	baseDir string,
	cfg splitBenchConfig,
) (*DBImpl, []byte, string, string, types.Range) {
	b.Helper()

	logger := zap.NewNop()
	ctx := context.Background()

	srcDir := filepath.Join(baseDir, "src")
	destDir1 := filepath.Join(baseDir, "dest1")
	destDir2 := filepath.Join(baseDir, "dest2")
	require.NoError(b, os.MkdirAll(srcDir, os.ModePerm))
	require.NoError(b, os.MkdirAll(destDir1, os.ModePerm))
	require.NoError(b, os.MkdirAll(destDir2, os.ModePerm))

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}
	db := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(b, db.Open(srcDir, false, splitBenchSchema(), fullRange))

	fullTextIndex := indexes.NewFullTextIndexConfig("full_text_index", false)
	chunkedEmbeddingIndex := indexes.NewEmbeddingsConfig(
		"chunked_embedding_index",
		indexes.EmbeddingsIndexConfig{
			Dimension: 3,
			Field:     "content",
			Chunker: &chunking.ChunkerConfig{
				Provider:    chunking.ChunkerProviderMock,
				StoreChunks: true,
			},
		},
	)
	sparseIndex := indexes.NewEmbeddingsConfig(
		"sparse_index",
		indexes.EmbeddingsIndexConfig{
			Field:  "content",
			Sparse: true,
		},
	)
	graphEdgeTypes := []indexes.EdgeTypeConfig{
		{
			Name:             "cites",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: &[]string{},
		},
	}
	graphIndex, err := indexes.NewIndexConfig("graph_index", indexes.GraphIndexConfig{
		EdgeTypes:           &graphEdgeTypes,
		MaxEdgesPerDocument: 4,
	})
	require.NoError(b, err)
	require.NoError(b, db.AddIndex(*fullTextIndex))
	require.NoError(b, db.AddIndex(*chunkedEmbeddingIndex))
	require.NoError(b, db.AddIndex(*sparseIndex))
	require.NoError(b, db.AddIndex(*graphIndex))

	docWrites := make([][2][]byte, 0, cfg.docs)
	indexWrites := make([][2][]byte, 0, cfg.docs*(cfg.chunksPerDoc*2+2))
	for i := range cfg.docs {
		key := fmt.Sprintf("key:%07d", i)
		docJSON, err := json.Marshal(testDocument{
			ID:      key,
			Content: fmt.Sprintf("content for %s", key),
			Value:   float64(i),
		})
		require.NoError(b, err)
		docWrites = append(docWrites, [2][]byte{[]byte(key), docJSON})

		sparseKey := fmt.Appendf(nil, "%s:i:sparse_index%s", key, storeutils.SparseSuffix)
		sparseValue := encodeSparseEmbeddingValue(generateTestSparseEmbedding(i), xxhash.Sum64String(key))
		indexWrites = append(indexWrites, [2][]byte{sparseKey, sparseValue})

		if i+1 < cfg.docs {
			target := fmt.Sprintf("key:%07d", i+1)
			edgeKey := storeutils.MakeEdgeKey([]byte(key), []byte(target), "graph_index", "cites")
			edgeValue, err := indexes.EncodeEdgeValue(&indexes.Edge{
				Source: []byte(key),
				Target: []byte(target),
				Type:   "cites",
				Weight: 1.0,
			})
			require.NoError(b, err)
			indexWrites = append(indexWrites, [2][]byte{edgeKey, edgeValue})
		}

		for chunkID := range cfg.chunksPerDoc {
			chunkKey := storeutils.MakeChunkKey([]byte(key), "chunked_embedding_index", uint32(chunkID))
			chunkText := fmt.Sprintf("chunk %d for %s", chunkID, key)
			chunkJSON, err := json.Marshal(chunking.NewTextChunk(uint32(chunkID), chunkText, 0, len(chunkText)))
			require.NoError(b, err)

			chunkValue := make([]byte, 0, len(chunkJSON)+8)
			chunkValue = encoding.EncodeUint64Ascending(chunkValue, xxhash.Sum64String(chunkText))
			chunkValue = append(chunkValue, chunkJSON...)

			embKey := append(bytes.Clone(chunkKey), []byte(":i:chunked_embedding_index:e")...)
			embValue, err := vectorindex.EncodeEmbeddingWithHashID(
				nil,
				vector.T{float32((i % 7) + 1), float32(chunkID + 1), 1.0},
				xxhash.Sum64(chunkKey),
			)
			require.NoError(b, err)

			indexWrites = append(indexWrites,
				[2][]byte{chunkKey, chunkValue},
				[2][]byte{embKey, embValue},
			)
		}
	}

	err = db.Batch(ctx, docWrites, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(b, err)
	}
	if len(indexWrites) > 0 {
		err = db.Batch(ctx, indexWrites, nil, Op_SyncLevelEmbeddings)
		if err != nil && !errors.Is(err, ErrPartialSuccess) {
			require.NoError(b, err)
		}
	}
	require.NoError(b, db.pdb.Flush())
	time.Sleep(200 * time.Millisecond)

	splitKey := fmt.Appendf(nil, "key:%07d", cfg.docs/2)
	return db, splitKey, destDir1, destDir2, fullRange
}

func BenchmarkSplitPrepareOnlyCheckpoint(b *testing.B) {
	for _, cfg := range splitBenchConfigs() {
		b.Run(cfg.name(), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(cfg.docs), "docs")
			b.ReportMetric(float64(cfg.docs*cfg.chunksPerDoc), "chunks")
			b.ReportMetric(float64(cfg.docs), "sparse_vectors")
			b.ReportMetric(float64(max(cfg.docs-1, 0)), "graph_edges")

			root := b.TempDir()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				iterDir := filepath.Join(root, fmt.Sprintf("prepare-%d", i))
				require.NoError(b, os.MkdirAll(iterDir, os.ModePerm))

				db, splitKey, destDir1, destDir2, fullRange := setupSplitBenchDB(b, iterDir, cfg)
				b.StartTimer()

				err := db.Split(fullRange, splitKey, destDir1, destDir2, true)

				b.StopTimer()
				require.NoError(b, err)
				require.NoError(b, db.Close())
				require.NoError(b, os.RemoveAll(iterDir))
			}
		})
	}
}

func BenchmarkFinalizeSplitPrune(b *testing.B) {
	for _, cfg := range splitBenchConfigs() {
		b.Run(cfg.name(), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(cfg.docs), "docs")
			b.ReportMetric(float64(cfg.docs*cfg.chunksPerDoc), "chunks")
			b.ReportMetric(float64(cfg.docs), "sparse_vectors")
			b.ReportMetric(float64(max(cfg.docs-1, 0)), "graph_edges")

			root := b.TempDir()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				iterDir := filepath.Join(root, fmt.Sprintf("finalize-%d", i))
				require.NoError(b, os.MkdirAll(iterDir, os.ModePerm))

				db, splitKey, _, _, fullRange := setupSplitBenchDB(b, iterDir, cfg)
				parentRange := types.Range{fullRange[0], splitKey}
				require.NoError(b, db.SetSplitState(SplitState_builder{
					Phase:            SplitState_PHASE_PREPARE,
					SplitKey:         splitKey,
					OriginalRangeEnd: fullRange[1],
				}.Build()))
				require.NoError(b, db.SetRange(parentRange))
				b.StartTimer()

				err := db.FinalizeSplit(parentRange)

				b.StopTimer()
				require.NoError(b, err)
				require.NoError(b, db.Close())
				require.NoError(b, os.RemoveAll(iterDir))
			}
		})
	}
}
