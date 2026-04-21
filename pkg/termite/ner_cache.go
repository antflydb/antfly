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

	"github.com/antflydb/antfly/pkg/termite/lib/ner"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// CachedNER wraps a NER model with caching support
type CachedNER struct {
	model  ner.Model
	name   string
	cache  *ResultCache[[][]ner.Entity]
	logger *zap.Logger
}

// NewCachedNER wraps a NER model with caching
func NewCachedNER(
	model ner.Model,
	name string,
	cache *ResultCache[[][]ner.Entity],
	logger *zap.Logger,
) *CachedNER {
	return &CachedNER{
		model:  model,
		name:   name,
		cache:  cache,
		logger: logger,
	}
}

// Recognize extracts entities with caching support
func (c *CachedNER) Recognize(ctx context.Context, texts []string) ([][]ner.Entity, error) {
	key := c.cacheKey(texts)

	// Check cache first
	if item := c.cache.Cache().Get(key); item != nil {
		RecordCacheHit("ner")
		c.logger.Debug("NER cache hit",
			zap.String("model", c.name),
			zap.Int("num_texts", len(texts)))
		return item.Value(), nil
	}

	// Use shared singleflight to deduplicate concurrent identical requests
	result, err, shared := c.cache.SFGroup().Do(key, func() (any, error) {
		// Double-check cache (another goroutine may have populated it)
		if item := c.cache.Cache().Get(key); item != nil {
			return item.Value(), nil
		}

		RecordCacheMiss("ner")

		start := time.Now()
		entities, err := c.model.Recognize(ctx, texts)
		if err != nil {
			return nil, err
		}

		RecordRequestDuration("ner", c.name, "200", time.Since(start).Seconds())

		c.cache.Cache().Set(key, entities, 0)

		c.logger.Debug("NER completed and cached",
			zap.String("model", c.name),
			zap.Int("num_texts", len(texts)),
			zap.Duration("duration", time.Since(start)))

		return entities, nil
	})

	if err != nil {
		return nil, err
	}

	if shared {
		c.logger.Debug("Singleflight hit for NER request",
			zap.String("model", c.name))
	}

	return result.([][]ner.Entity), nil
}

// cacheKey generates a unique cache key from model + texts
func (c *CachedNER) cacheKey(texts []string) string {
	h := xxhash.New()

	_, _ = h.WriteString(c.name)
	_, _ = h.WriteString("|")

	for i, text := range texts {
		_, _ = h.WriteString("t")
		var idxBuf [4]byte
		binary.BigEndian.PutUint32(idxBuf[:], uint32(i))
		_, _ = h.Write(idxBuf[:])
		_, _ = h.WriteString(":")
		_, _ = h.WriteString(text)
		_, _ = h.WriteString("|")
	}

	return strconv.FormatUint(h.Sum64(), 16)
}

// Close closes the underlying model
func (c *CachedNER) Close() error {
	return c.model.Close()
}
