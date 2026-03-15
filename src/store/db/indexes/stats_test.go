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
	"context"
	"testing"

	"github.com/antflydb/antfly/lib/inflight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestMergeIndexStats_FullText(t *testing.T) {
	t.Run("sum numeric fields", func(t *testing.T) {
		dst := FullTextIndexStats{TotalIndexed: 100, DiskUsage: 500}.AsIndexStats()
		src := FullTextIndexStats{TotalIndexed: 200, DiskUsage: 300}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, err := dst.AsFullTextIndexStats()
		assert.NoError(t, err)
		assert.Equal(t, uint64(300), got.TotalIndexed)
		assert.Equal(t, uint64(800), got.DiskUsage)
	})

	t.Run("OR rebuilding", func(t *testing.T) {
		dst := FullTextIndexStats{Rebuilding: false}.AsIndexStats()
		src := FullTextIndexStats{Rebuilding: true, BackfillProgress: 0.5}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsFullTextIndexStats()
		assert.True(t, got.Rebuilding)
	})

	t.Run("min progress only among rebuilding shards", func(t *testing.T) {
		// Shard A not rebuilding (progress=0), Shard B rebuilding at 0.5
		dst := FullTextIndexStats{Rebuilding: false, BackfillProgress: 0}.AsIndexStats()
		src := FullTextIndexStats{Rebuilding: true, BackfillProgress: 0.5}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsFullTextIndexStats()
		assert.True(t, got.Rebuilding)
		assert.Equal(t, 0.5, got.BackfillProgress)
	})

	t.Run("min progress among two rebuilding shards", func(t *testing.T) {
		dst := FullTextIndexStats{Rebuilding: true, BackfillProgress: 0.8}.AsIndexStats()
		src := FullTextIndexStats{Rebuilding: true, BackfillProgress: 0.3}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsFullTextIndexStats()
		assert.Equal(t, 0.3, got.BackfillProgress)
	})

	t.Run("sum backfill items processed", func(t *testing.T) {
		dst := FullTextIndexStats{BackfillItemsProcessed: 100}.AsIndexStats()
		src := FullTextIndexStats{BackfillItemsProcessed: 200}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsFullTextIndexStats()
		assert.Equal(t, uint64(300), got.BackfillItemsProcessed)
	})

	t.Run("concatenate errors", func(t *testing.T) {
		dst := FullTextIndexStats{Error: "err1"}.AsIndexStats()
		src := FullTextIndexStats{Error: "err2"}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsFullTextIndexStats()
		assert.Equal(t, "err1; err2", got.Error)
	})

	t.Run("empty dst uses src", func(t *testing.T) {
		var dst IndexStats
		src := FullTextIndexStats{TotalIndexed: 42}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, err := dst.AsFullTextIndexStats()
		assert.NoError(t, err)
		assert.Equal(t, uint64(42), got.TotalIndexed)
	})
}

func TestMergeIndexStats_Embeddings(t *testing.T) {
	t.Run("sum numeric fields", func(t *testing.T) {
		dst := EmbeddingsIndexStats{TotalIndexed: 10, WalBacklog: 5}.AsIndexStats()
		src := EmbeddingsIndexStats{TotalIndexed: 20, WalBacklog: 3}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsEmbeddingsIndexStats()
		assert.Equal(t, uint64(30), got.TotalIndexed)
		assert.Equal(t, uint64(8), got.WalBacklog)
	})

	t.Run("min progress only among rebuilding shards", func(t *testing.T) {
		dst := EmbeddingsIndexStats{Rebuilding: false}.AsIndexStats()
		src := EmbeddingsIndexStats{Rebuilding: true, BackfillProgress: 0.7}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsEmbeddingsIndexStats()
		assert.True(t, got.Rebuilding)
		assert.Equal(t, 0.7, got.BackfillProgress)
	})

	t.Run("min progress among two rebuilding shards", func(t *testing.T) {
		dst := EmbeddingsIndexStats{Rebuilding: true, BackfillProgress: 0.9}.AsIndexStats()
		src := EmbeddingsIndexStats{Rebuilding: true, BackfillProgress: 0.4}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsEmbeddingsIndexStats()
		assert.Equal(t, 0.4, got.BackfillProgress)
	})
}

func TestMergeIndexStats_Graph(t *testing.T) {
	t.Run("sum edges", func(t *testing.T) {
		dst := GraphIndexStats{TotalEdges: 100}.AsIndexStats()
		src := GraphIndexStats{TotalEdges: 50}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsGraphIndexStats()
		assert.Equal(t, uint64(150), got.TotalEdges)
	})

	t.Run("min progress only among rebuilding shards", func(t *testing.T) {
		dst := GraphIndexStats{Rebuilding: false}.AsIndexStats()
		src := GraphIndexStats{Rebuilding: true, BackfillProgress: 0.6}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsGraphIndexStats()
		assert.True(t, got.Rebuilding)
		assert.Equal(t, 0.6, got.BackfillProgress)
	})

	t.Run("merge edge types", func(t *testing.T) {
		dstET := map[string]uint64{"parent": 10}
		srcET := map[string]uint64{"parent": 5, "child": 3}
		dst := GraphIndexStats{EdgeTypes: &dstET}.AsIndexStats()
		src := GraphIndexStats{EdgeTypes: &srcET}.AsIndexStats()
		MergeIndexStats(&dst, src)
		got, _ := dst.AsGraphIndexStats()
		assert.Equal(t, uint64(15), (*got.EdgeTypes)["parent"])
		assert.Equal(t, uint64(3), (*got.EdgeTypes)["child"])
	})
}

func TestMergeIndexStats_EmptySrc(t *testing.T) {
	dst := FullTextIndexStats{TotalIndexed: 42}.AsIndexStats()
	MergeIndexStats(&dst, IndexStats{})
	got, _ := dst.AsFullTextIndexStats()
	assert.Equal(t, uint64(42), got.TotalIndexed)
}

func TestMergeIndexStats_TypeMismatch(t *testing.T) {
	dst := FullTextIndexStats{TotalIndexed: 42}.AsIndexStats()
	// Use WalBacklog to make this distinguishable as embeddings type
	src := EmbeddingsIndexStats{TotalIndexed: 10, WalBacklog: 1}.AsIndexStats()
	// Should not panic, just warn
	MergeIndexStats(&dst, src)
	// dst should be unchanged
	got, _ := dst.AsFullTextIndexStats()
	assert.Equal(t, uint64(42), got.TotalIndexed)
}

// newTestWALBuf creates a WALBuffer in a temp dir for testing.
func newTestWALBuf(t *testing.T) *inflight.WALBuffer {
	t.Helper()
	wb, err := inflight.NewWALBuffer(zaptest.NewLogger(t), t.TempDir(), "test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = wb.Close() })
	return wb
}

func TestPipelineEnricherStats(t *testing.T) {
	t.Run("terminal only", func(t *testing.T) {
		pe := &PipelineEnricher{
			Terminal: &mockTerminalEnricher{
				stats: EnricherStats{
					Backfilling:            true,
					BackfillProgress:       0.5,
					BackfillItemsProcessed: 100,
					WALBacklog:             10,
				},
			},
		}
		got := pe.EnricherStats()
		assert.True(t, got.Backfilling)
		assert.Equal(t, 0.5, got.BackfillProgress)
		assert.Equal(t, uint64(100), got.BackfillItemsProcessed)
		assert.Equal(t, 10, got.WALBacklog)
	})

	t.Run("terminal not backfilling, chunking is", func(t *testing.T) {
		ce := &ChunkingEnricher{}
		ce.walBuf = newTestWALBuf(t)
		ce.backfilling.Store(true)
		ce.backfillProgress.Store(float64(0.6))
		ce.backfillItemsProcessed.Store(50)

		pe := &PipelineEnricher{
			Terminal: &mockTerminalEnricher{
				stats: EnricherStats{
					Backfilling:      false,
					BackfillProgress: 0,
					WALBacklog:       5,
				},
			},
			ChunkingEnricher: ce,
		}
		got := pe.EnricherStats()
		assert.True(t, got.Backfilling)
		assert.Equal(t, 0.6, got.BackfillProgress)
		assert.Equal(t, uint64(50), got.BackfillItemsProcessed)
	})

	t.Run("both backfilling takes min progress", func(t *testing.T) {
		ce := &ChunkingEnricher{}
		ce.walBuf = newTestWALBuf(t)
		ce.backfilling.Store(true)
		ce.backfillProgress.Store(float64(0.3))

		pe := &PipelineEnricher{
			Terminal: &mockTerminalEnricher{
				stats: EnricherStats{
					Backfilling:      true,
					BackfillProgress: 0.8,
					WALBacklog:       2,
				},
			},
			ChunkingEnricher: ce,
		}
		got := pe.EnricherStats()
		assert.True(t, got.Backfilling)
		assert.Equal(t, 0.3, got.BackfillProgress)
	})

	t.Run("sum WAL backlogs", func(t *testing.T) {
		se := &SummarizeEnricher{}
		se.walBuf = newTestWALBuf(t)

		pe := &PipelineEnricher{
			Terminal: &mockTerminalEnricher{
				stats: EnricherStats{WALBacklog: 10},
			},
			Summarizer: se,
		}
		got := pe.EnricherStats()
		assert.Equal(t, 10, got.WALBacklog)
	})
}

// mockTerminalEnricher implements TerminalEmbeddingEnricher for testing.
type mockTerminalEnricher struct {
	stats EnricherStats
}

func (m *mockTerminalEnricher) EnrichBatch(keys [][]byte) error { return nil }
func (m *mockTerminalEnricher) EnricherStats() EnricherStats    { return m.stats }
func (m *mockTerminalEnricher) Close() error                    { return nil }
func (m *mockTerminalEnricher) GenerateEmbeddingsWithoutPersist(
	ctx context.Context, keys [][]byte, documentValues map[string][]byte, generatePrompts generatePromptsFunc,
) ([][2][]byte, [][2][]byte, [][]byte, error) {
	return nil, nil, nil, nil
}

func TestGraphIndexStats(t *testing.T) {
	g := &GraphIndexV0{
		edgeTypeCounts: make(map[string]uint64),
	}

	// No edges
	stats := g.Stats()
	gs, err := stats.AsGraphIndexStats()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), gs.TotalEdges)
	assert.False(t, gs.Rebuilding)
	assert.Nil(t, gs.EdgeTypes)

	// Add some edges (totalEdges is derived from edgeTypeCounts)
	g.edgeTypeCountsMu.Lock()
	g.edgeTypeCounts["parent"] = 30
	g.edgeTypeCounts["child"] = 12
	g.edgeTypeCountsMu.Unlock()
	g.backfilling.Store(true)
	g.backfillProgress.Store(float64(0.7))
	g.backfillItemsProcessed.Store(100)

	stats = g.Stats()
	gs, err = stats.AsGraphIndexStats()
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), gs.TotalEdges) // 30 + 12
	assert.True(t, gs.Rebuilding)
	assert.Equal(t, 0.7, gs.BackfillProgress)
	assert.Equal(t, uint64(100), gs.BackfillItemsProcessed)
	assert.NotNil(t, gs.EdgeTypes)
	assert.Equal(t, uint64(30), (*gs.EdgeTypes)["parent"])
	assert.Equal(t, uint64(12), (*gs.EdgeTypes)["child"])
}

func TestEnricherStats(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		var es EnricherStats
		assert.False(t, es.Backfilling)
		assert.Equal(t, float64(0), es.BackfillProgress)
		assert.Equal(t, uint64(0), es.BackfillItemsProcessed)
		assert.Equal(t, 0, es.WALBacklog)
	})
}
