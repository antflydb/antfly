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
	"image"
	"image/jpeg"
	"strconv"
	"time"

	"github.com/antflydb/antfly/pkg/termite/lib/reading"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// CachedReader wraps a reader with caching support
type CachedReader struct {
	reader reading.Reader
	model  string
	cache  *ResultCache[[]reading.Result]
	logger *zap.Logger
}

// NewCachedReader wraps a reader with caching
func NewCachedReader(
	reader reading.Reader,
	model string,
	cache *ResultCache[[]reading.Result],
	logger *zap.Logger,
) *CachedReader {
	return &CachedReader{
		reader: reader,
		model:  model,
		cache:  cache,
		logger: logger,
	}
}

// Read extracts text from images with caching support
func (c *CachedReader) Read(ctx context.Context, images []image.Image, prompt string, maxTokens int) ([]reading.Result, error) {
	key := c.cacheKey(images, prompt, maxTokens)

	// Check cache first
	if item := c.cache.Cache().Get(key); item != nil {
		RecordCacheHit("reading")
		c.logger.Debug("Reading cache hit",
			zap.String("model", c.model),
			zap.Int("num_images", len(images)))
		return item.Value(), nil
	}

	// Use shared singleflight to deduplicate concurrent identical requests
	result, err, shared := c.cache.SFGroup().Do(key, func() (any, error) {
		// Double-check cache (another goroutine may have populated it)
		if item := c.cache.Cache().Get(key); item != nil {
			return item.Value(), nil
		}

		RecordCacheMiss("reading")

		start := time.Now()
		results, err := c.reader.Read(ctx, images, prompt, maxTokens)
		if err != nil {
			return nil, err
		}

		RecordRequestDuration("read", c.model, "200", time.Since(start).Seconds())

		c.cache.Cache().Set(key, results, 0)

		c.logger.Debug("Reading completed and cached",
			zap.String("model", c.model),
			zap.Int("num_images", len(images)),
			zap.Duration("duration", time.Since(start)))

		return results, nil
	})

	if err != nil {
		return nil, err
	}

	if shared {
		c.logger.Debug("Singleflight hit for reading request",
			zap.String("model", c.model))
	}

	return result.([]reading.Result), nil
}

// cacheKey generates a unique cache key from model + images + prompt + maxTokens
func (c *CachedReader) cacheKey(images []image.Image, prompt string, maxTokens int) string {
	h := xxhash.New()

	_, _ = h.WriteString(c.model)
	_, _ = h.WriteString("|")

	_, _ = h.WriteString("p:")
	_, _ = h.WriteString(prompt)
	_, _ = h.WriteString("|")

	_, _ = h.WriteString("t:")
	var tokenBuf [4]byte
	binary.BigEndian.PutUint32(tokenBuf[:], uint32(maxTokens))
	_, _ = h.Write(tokenBuf[:])
	_, _ = h.WriteString("|")

	for i, img := range images {
		_, _ = h.WriteString("i")
		var idxBuf [4]byte
		binary.BigEndian.PutUint32(idxBuf[:], uint32(i))
		_, _ = h.Write(idxBuf[:])
		_, _ = h.WriteString(":")

		bounds := img.Bounds()
		var dimBuf [16]byte
		binary.BigEndian.PutUint32(dimBuf[0:4], uint32(bounds.Min.X))
		binary.BigEndian.PutUint32(dimBuf[4:8], uint32(bounds.Min.Y))
		binary.BigEndian.PutUint32(dimBuf[8:12], uint32(bounds.Max.X))
		binary.BigEndian.PutUint32(dimBuf[12:16], uint32(bounds.Max.Y))
		_, _ = h.Write(dimBuf[:])

		imgHash := hashImage(img)
		var imgHashBuf [8]byte
		binary.BigEndian.PutUint64(imgHashBuf[:], imgHash)
		_, _ = h.Write(imgHashBuf[:])

		_, _ = h.WriteString("|")
	}

	return strconv.FormatUint(h.Sum64(), 16)
}

// hashImage generates a hash for an image
func hashImage(img image.Image) uint64 {
	h := xxhash.New()

	encoder := jpeg.Options{Quality: 50}
	if err := jpeg.Encode(h, img, &encoder); err != nil {
		bounds := img.Bounds()
		var buf [16]byte
		binary.BigEndian.PutUint32(buf[0:4], uint32(bounds.Dx()))
		binary.BigEndian.PutUint32(buf[4:8], uint32(bounds.Dy()))
		_, _ = h.Write(buf[:])
	}

	return h.Sum64()
}

// Close closes the underlying reader
func (c *CachedReader) Close() error {
	return c.reader.Close()
}
