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
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// CachedEmbedder wraps an embedder with caching support
type CachedEmbedder struct {
	embedder embeddings.Embedder
	model    string
	cache    *ResultCache[[][]float32]
	logger   *zap.Logger
}

// NewCachedEmbedder wraps an embedder with caching
func NewCachedEmbedder(
	embedder embeddings.Embedder,
	model string,
	cache *ResultCache[[][]float32],
	logger *zap.Logger,
) *CachedEmbedder {
	return &CachedEmbedder{
		embedder: embedder,
		model:    model,
		cache:    cache,
		logger:   logger,
	}
}

// Capabilities returns the underlying embedder's capabilities
func (c *CachedEmbedder) Capabilities() embeddings.EmbedderCapabilities {
	return c.embedder.Capabilities()
}

// Embed generates embeddings with caching support
func (c *CachedEmbedder) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	key := c.cacheKey(contents)

	// Check cache first
	if item := c.cache.Cache().Get(key); item != nil {
		RecordCacheHit("embedding")
		c.logger.Debug("Embedding cache hit",
			zap.String("model", c.model),
			zap.Int("num_embeddings", len(item.Value())))
		return item.Value(), nil
	}

	// Use shared singleflight to deduplicate concurrent identical requests
	result, err, shared := c.cache.SFGroup().Do(key, func() (any, error) {
		// Double-check cache (another goroutine may have populated it)
		if item := c.cache.Cache().Get(key); item != nil {
			return item.Value(), nil
		}

		RecordCacheMiss("embedding")

		start := time.Now()
		embeds, err := c.embedder.Embed(ctx, contents)
		if err != nil {
			return nil, err
		}

		RecordRequestDuration("embed", c.model, "200", time.Since(start).Seconds())

		c.cache.Cache().Set(key, embeds, 0)

		c.logger.Debug("Embedding generated and cached",
			zap.String("model", c.model),
			zap.Int("num_embeddings", len(embeds)),
			zap.Duration("duration", time.Since(start)))

		return embeds, nil
	})

	if err != nil {
		return nil, err
	}

	if shared {
		c.logger.Debug("Singleflight hit for embedding request",
			zap.String("model", c.model))
	}

	return result.([][]float32), nil
}

// cacheKey generates a unique cache key from model + content
func (c *CachedEmbedder) cacheKey(contents [][]ai.ContentPart) string {
	h := xxhash.New()

	_, _ = h.WriteString(c.model)
	_, _ = h.WriteString("|")

	for _, parts := range contents {
		for _, part := range parts {
			switch p := part.(type) {
			case ai.TextContent:
				_, _ = h.WriteString("t:")
				_, _ = h.WriteString(p.Text)
				c.logger.Debug("Cache key: text content",
					zap.String("text_prefix", truncateString(p.Text, 50)))
			case ai.BinaryContent:
				_, _ = h.WriteString("b:")
				_, _ = h.WriteString(p.MIMEType)
				_, _ = h.WriteString(":")
				binHash := sha256.Sum256(p.Data)
				_, _ = h.Write(binHash[:])
				c.logger.Debug("Cache key: binary content",
					zap.String("mime_type", p.MIMEType),
					zap.Int("data_len", len(p.Data)),
					zap.String("sha256_prefix", fmt.Sprintf("%x", binHash[:8])))
			default:
				c.logger.Warn("Cache key: unknown content type",
					zap.String("type", fmt.Sprintf("%T", part)))
			}
			_, _ = h.WriteString("|")
		}
		_, _ = h.WriteString("||")
	}

	return strconv.FormatUint(h.Sum64(), 16)
}

// Close closes the underlying embedder
func (c *CachedEmbedder) Close() error {
	if closer, ok := c.embedder.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
