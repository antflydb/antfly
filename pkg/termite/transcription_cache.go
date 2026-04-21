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
	"strconv"
	"time"

	"github.com/antflydb/antfly/pkg/termite/lib/transcribing"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// CachedTranscriber wraps a transcriber with caching and singleflight deduplication.
type CachedTranscriber struct {
	transcriber transcribing.Transcriber
	model       string
	cache       *ResultCache[*transcribing.Result]
	logger      *zap.Logger
}

// NewCachedTranscriber wraps a transcriber with caching
func NewCachedTranscriber(
	transcriber transcribing.Transcriber,
	model string,
	cache *ResultCache[*transcribing.Result],
	logger *zap.Logger,
) *CachedTranscriber {
	return &CachedTranscriber{
		transcriber: transcriber,
		model:       model,
		cache:       cache,
		logger:      logger,
	}
}

// TranscribeWithOptions transcribes audio with caching support
func (c *CachedTranscriber) TranscribeWithOptions(ctx context.Context, audioData []byte, opts transcribing.TranscribeOptions) (*transcribing.Result, error) {
	key := c.cacheKey(audioData, opts)

	// Check cache first
	if item := c.cache.Cache().Get(key); item != nil {
		RecordCacheHit("transcription")
		c.logger.Debug("Transcription cache hit",
			zap.String("model", c.model),
			zap.Int("audio_bytes", len(audioData)))
		return item.Value(), nil
	}

	// Use shared singleflight to deduplicate concurrent identical requests
	result, err, shared := c.cache.SFGroup().Do(key, func() (any, error) {
		// Double-check cache (another goroutine may have populated it)
		if item := c.cache.Cache().Get(key); item != nil {
			return item.Value(), nil
		}

		RecordCacheMiss("transcription")

		start := time.Now()
		result, err := c.transcriber.TranscribeWithOptions(ctx, audioData, opts)
		if err != nil {
			return nil, err
		}

		RecordRequestDuration("transcribe", c.model, "200", time.Since(start).Seconds())

		c.cache.Cache().Set(key, result, 0)

		c.logger.Debug("Transcription completed and cached",
			zap.String("model", c.model),
			zap.Int("audio_bytes", len(audioData)),
			zap.Int("text_length", len(result.Text)),
			zap.Duration("duration", time.Since(start)))

		return result, nil
	})

	if err != nil {
		return nil, err
	}

	if shared {
		c.logger.Debug("Singleflight hit for transcription request",
			zap.String("model", c.model))
	}

	return result.(*transcribing.Result), nil
}

// cacheKey generates a unique cache key from model + audio data + options
func (c *CachedTranscriber) cacheKey(audioData []byte, opts transcribing.TranscribeOptions) string {
	h := xxhash.New()

	_, _ = h.WriteString(c.model)
	_, _ = h.WriteString("|")

	// Hash audio data with SHA256 (collision-resistant for binary data)
	audioHash := sha256.Sum256(audioData)
	_, _ = h.Write(audioHash[:])
	_, _ = h.WriteString("|")

	// Include options
	_, _ = h.WriteString("lang:")
	_, _ = h.WriteString(opts.Language)

	return strconv.FormatUint(h.Sum64(), 16)
}
