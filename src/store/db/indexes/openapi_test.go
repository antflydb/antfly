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
	"testing"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/pkg/generating"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeDistanceMetric(t *testing.T) {
	assert.Equal(t, DistanceMetricL2Squared, normalizeDistanceMetric(""))
	assert.Equal(t, DistanceMetricL2Squared, normalizeDistanceMetric(DistanceMetricL2Squared))
	assert.Equal(t, DistanceMetricCosine, normalizeDistanceMetric(DistanceMetricCosine))
	assert.Equal(t, DistanceMetricInnerProduct, normalizeDistanceMetric(DistanceMetricInnerProduct))
}

func TestEmbeddingsIndexConfig_Equal(t *testing.T) {
	base := EmbeddingsIndexConfig{
		Dimension:      384,
		Field:          "content",
		DistanceMetric: DistanceMetricCosine,
	}

	t.Run("identical configs", func(t *testing.T) {
		other := base
		assert.True(t, base.Equal(other))
	})

	t.Run("empty distance metric equals l2_squared default", func(t *testing.T) {
		a := EmbeddingsIndexConfig{Dimension: 384}
		b := EmbeddingsIndexConfig{Dimension: 384, DistanceMetric: DistanceMetricL2Squared}
		assert.True(t, a.Equal(b))
		assert.True(t, b.Equal(a))
	})

	t.Run("different distance metric", func(t *testing.T) {
		other := base
		other.DistanceMetric = DistanceMetricL2Squared
		assert.False(t, base.Equal(other))
	})

	t.Run("different dimension", func(t *testing.T) {
		other := base
		other.Dimension = 768
		assert.False(t, base.Equal(other))
	})

	t.Run("different field", func(t *testing.T) {
		other := base
		other.Field = "title"
		assert.False(t, base.Equal(other))
	})

	t.Run("different template", func(t *testing.T) {
		other := base
		other.Template = "{{content}}"
		assert.False(t, base.Equal(other))
	})

	t.Run("different mem_only", func(t *testing.T) {
		other := base
		other.MemOnly = true
		assert.False(t, base.Equal(other))
	})

	t.Run("different sparse", func(t *testing.T) {
		other := base
		other.Sparse = true
		assert.False(t, base.Equal(other))
	})

	t.Run("different external", func(t *testing.T) {
		other := base
		other.External = true
		assert.False(t, base.Equal(other))
	})

	t.Run("different chunk_size", func(t *testing.T) {
		other := base
		other.ChunkSize = 512
		assert.False(t, base.Equal(other))
	})

	t.Run("different min_weight", func(t *testing.T) {
		other := base
		other.MinWeight = 0.5
		assert.False(t, base.Equal(other))
	})

	t.Run("different top_k", func(t *testing.T) {
		other := base
		other.TopK = 100
		assert.False(t, base.Equal(other))
	})

	t.Run("nil vs non-nil embedder", func(t *testing.T) {
		other := base
		other.Embedder = &embeddings.EmbedderConfig{}
		assert.False(t, base.Equal(other))
	})

	t.Run("nil vs non-nil summarizer", func(t *testing.T) {
		other := base
		other.Summarizer = &generating.GeneratorConfig{}
		assert.False(t, base.Equal(other))
	})

	t.Run("nil vs non-nil chunker", func(t *testing.T) {
		other := base
		other.Chunker = &chunking.ChunkerConfig{}
		assert.False(t, base.Equal(other))
	})

	t.Run("both nil chunkers are equal", func(t *testing.T) {
		a := EmbeddingsIndexConfig{Dimension: 384}
		b := EmbeddingsIndexConfig{Dimension: 384}
		assert.True(t, a.Equal(b))
	})
}

func TestIndexConfig_Equal(t *testing.T) {
	t.Run("same embeddings config", func(t *testing.T) {
		a := NewEmbeddingsConfig("vec", EmbeddingsIndexConfig{
			Dimension:      384,
			DistanceMetric: DistanceMetricCosine,
		})
		b := NewEmbeddingsConfig("vec", EmbeddingsIndexConfig{
			Dimension:      384,
			DistanceMetric: DistanceMetricCosine,
		})
		assert.True(t, a.Equal(*b))
	})

	t.Run("different distance metric detected", func(t *testing.T) {
		a := NewEmbeddingsConfig("vec", EmbeddingsIndexConfig{
			Dimension:      384,
			DistanceMetric: DistanceMetricCosine,
		})
		b := NewEmbeddingsConfig("vec", EmbeddingsIndexConfig{
			Dimension:      384,
			DistanceMetric: DistanceMetricL2Squared,
		})
		assert.False(t, a.Equal(*b))
	})

	t.Run("legacy aknn_v0 equals embeddings", func(t *testing.T) {
		a := NewEmbeddingsConfig("vec", EmbeddingsIndexConfig{Dimension: 384})
		b := NewEmbeddingsConfig("vec", EmbeddingsIndexConfig{Dimension: 384})
		b.Type = IndexTypeAknnV0
		assert.True(t, a.Equal(*b))
	})

	t.Run("different names", func(t *testing.T) {
		a := NewEmbeddingsConfig("vec", EmbeddingsIndexConfig{Dimension: 384})
		b := NewEmbeddingsConfig("vec2", EmbeddingsIndexConfig{Dimension: 384})
		assert.False(t, a.Equal(*b))
	})

	t.Run("different types", func(t *testing.T) {
		a := NewEmbeddingsConfig("idx", EmbeddingsIndexConfig{Dimension: 384})
		b := NewFullTextIndexConfig("idx", false)
		assert.False(t, a.Equal(*b))
	})
}
