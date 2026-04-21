// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package termite

import (
	"context"
	"strconv"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// CachedSparseEmbedder wraps a SparseEmbedder with caching support.
type CachedSparseEmbedder struct {
	embedder embeddings.SparseEmbedder
	model    string
	cache    *ResultCache[[]embeddings.SparseVector]
	logger   *zap.Logger
}

// NewCachedSparseEmbedder wraps a sparse embedder with caching
func NewCachedSparseEmbedder(
	embedder embeddings.SparseEmbedder,
	model string,
	cache *ResultCache[[]embeddings.SparseVector],
	logger *zap.Logger,
) *CachedSparseEmbedder {
	return &CachedSparseEmbedder{
		embedder: embedder,
		model:    model,
		cache:    cache,
		logger:   logger,
	}
}

// SparseEmbed generates sparse embeddings with caching and singleflight deduplication.
func (c *CachedSparseEmbedder) SparseEmbed(ctx context.Context, texts []string) ([]embeddings.SparseVector, error) {
	key := c.cacheKey(texts)

	// Check cache
	if item := c.cache.Cache().Get(key); item != nil {
		RecordCacheHit("sparse_embedding")
		return item.Value(), nil
	}

	// Shared singleflight deduplication
	result, err, _ := c.cache.SFGroup().Do(key, func() (any, error) {
		// Double-check cache (another goroutine may have populated it)
		if item := c.cache.Cache().Get(key); item != nil {
			return item.Value(), nil
		}

		RecordCacheMiss("sparse_embedding")

		start := time.Now()
		vecs, err := c.embedder.SparseEmbed(ctx, texts)
		if err != nil {
			return nil, err
		}

		RecordRequestDuration("sparse_embed", c.model, "200", time.Since(start).Seconds())
		c.cache.Cache().Set(key, vecs, 0)
		return vecs, nil
	})

	if err != nil {
		return nil, err
	}
	return result.([]embeddings.SparseVector), nil
}

func (c *CachedSparseEmbedder) cacheKey(texts []string) string {
	h := xxhash.New()
	_, _ = h.WriteString(c.model)
	_, _ = h.WriteString("|sparse|")
	for _, text := range texts {
		_, _ = h.WriteString(text)
		_, _ = h.WriteString("|")
	}
	return strconv.FormatUint(h.Sum64(), 16)
}
