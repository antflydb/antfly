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
	"time"

	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// ResultCache is a generic cache manager for inference results.
// Multiple per-model wrappers (CachedEmbedder, CachedReranker, etc.) share
// the same underlying ttlcache via Cache().
type ResultCache[V any] struct {
	cache   *ttlcache.Cache[string, V]
	sfGroup singleflight.Group
	logger  *zap.Logger
	cancel  context.CancelFunc
	name    string
}

// NewResultCache creates a new result cache with the given TTL and starts
// periodic stats logging.
func NewResultCache[V any](name string, ttl time.Duration, logger *zap.Logger) *ResultCache[V] {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, V](ttl),
	)
	go cache.Start()

	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is stored in rc.cancel and called in Close()
	rc := &ResultCache[V]{
		cache:  cache,
		logger: logger,
		cancel: cancel,
		name:   name,
	}

	go rc.logStats(ctx)

	return rc
}

// Cache returns the underlying ttlcache for use by per-model wrappers.
func (rc *ResultCache[V]) Cache() *ttlcache.Cache[string, V] {
	return rc.cache
}

// SFGroup returns the shared singleflight group for deduplicating concurrent
// identical requests across all per-model wrappers sharing this cache.
func (rc *ResultCache[V]) SFGroup() *singleflight.Group {
	return &rc.sfGroup
}

// Close stops the stats goroutine and the cache eviction loop.
func (rc *ResultCache[V]) Close() {
	rc.cancel()
	rc.cache.Stop()
}

// Stats returns global cache statistics.
func (rc *ResultCache[V]) Stats() map[string]any {
	metrics := rc.cache.Metrics()
	return map[string]any{
		"hits":   metrics.Hits,
		"misses": metrics.Misses,
		"items":  rc.cache.Len(),
	}
}

// logStats logs cache statistics periodically using interval-based deltas
// so that stats reflect activity since the last log rather than cumulative totals.
func (rc *ResultCache[V]) logStats(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	var prevHits, prevMisses uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics := rc.cache.Metrics()
			if metrics.Hits == prevHits && metrics.Misses == prevMisses {
				continue
			}
			intervalHits := metrics.Hits - prevHits
			intervalMisses := metrics.Misses - prevMisses
			prevHits = metrics.Hits
			prevMisses = metrics.Misses

			hitRate := float64(0)
			total := intervalHits + intervalMisses
			if total > 0 {
				hitRate = float64(intervalHits) / float64(total) * 100
			}
			rc.logger.Info(rc.name+" cache stats",
				zap.Uint64("hits", intervalHits),
				zap.Uint64("misses", intervalMisses),
				zap.Float64("hit_rate_pct", hitRate),
				zap.Int("items", rc.cache.Len()))
		}
	}
}

// truncateString returns the first n characters of s, or s if len(s) <= n.
func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
