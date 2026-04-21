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
	"encoding/binary"
	"strconv"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/reranking"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// CachedReranker wraps a reranker with caching support
type CachedReranker struct {
	reranker reranking.Model
	model    string
	cache    *ResultCache[[]float32]
	logger   *zap.Logger
}

// NewCachedReranker wraps a reranker with caching
func NewCachedReranker(
	reranker reranking.Model,
	model string,
	cache *ResultCache[[]float32],
	logger *zap.Logger,
) *CachedReranker {
	return &CachedReranker{
		reranker: reranker,
		model:    model,
		cache:    cache,
		logger:   logger,
	}
}

// Rerank scores prompts with caching support
func (c *CachedReranker) Rerank(ctx context.Context, query string, prompts []string) ([]float32, error) {
	key := c.cacheKey(query, prompts)

	// Check cache first
	if item := c.cache.Cache().Get(key); item != nil {
		RecordCacheHit("reranking")
		c.logger.Debug("Reranking cache hit",
			zap.String("model", c.model),
			zap.Int("num_prompts", len(prompts)))
		return item.Value(), nil
	}

	// Use shared singleflight to deduplicate concurrent identical requests
	result, err, shared := c.cache.SFGroup().Do(key, func() (any, error) {
		// Double-check cache (another goroutine may have populated it)
		if item := c.cache.Cache().Get(key); item != nil {
			return item.Value(), nil
		}

		RecordCacheMiss("reranking")

		start := time.Now()
		scores, err := c.reranker.Rerank(ctx, query, prompts)
		if err != nil {
			return nil, err
		}

		RecordRequestDuration("rerank", c.model, "200", time.Since(start).Seconds())

		c.cache.Cache().Set(key, scores, 0)

		c.logger.Debug("Reranking completed and cached",
			zap.String("model", c.model),
			zap.Int("num_prompts", len(prompts)),
			zap.Duration("duration", time.Since(start)))

		return scores, nil
	})

	if err != nil {
		return nil, err
	}

	if shared {
		c.logger.Debug("Singleflight hit for reranking request",
			zap.String("model", c.model))
	}

	return result.([]float32), nil
}

// cacheKey generates a unique cache key from model + query + prompts
func (c *CachedReranker) cacheKey(query string, prompts []string) string {
	h := xxhash.New()

	_, _ = h.WriteString(c.model)
	_, _ = h.WriteString("|")

	_, _ = h.WriteString("q:")
	_, _ = h.WriteString(query)
	_, _ = h.WriteString("|")

	for i, prompt := range prompts {
		_, _ = h.WriteString("p")
		var idxBuf [4]byte
		binary.BigEndian.PutUint32(idxBuf[:], uint32(i))
		_, _ = h.Write(idxBuf[:])
		_, _ = h.WriteString(":")
		_, _ = h.WriteString(prompt)
		_, _ = h.WriteString("|")
	}

	return strconv.FormatUint(h.Sum64(), 16)
}

// Close closes the underlying reranker
func (c *CachedReranker) Close() error {
	if closer, ok := c.reranker.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
