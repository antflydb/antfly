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
	"sync"

	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// refTracker manages reference counting and orphaned eviction handles for model
// registries. When a model is evicted from a ttlcache while its refcount is > 0,
// the close function is stored as an "orphan" and deferred until the last Release().
//
// Embed this in any registry that uses ttlcache with Acquire/Release semantics.
type refTracker struct {
	mu             sync.Mutex
	refCounts      map[string]int
	evictedHandles map[string][]func() error
}

func newRefTracker() refTracker {
	return refTracker{
		refCounts:      make(map[string]int),
		evictedHandles: make(map[string][]func() error),
	}
}

// incRef increments the reference count for a model key and returns the new count.
// Call before loading/returning a model in Acquire() to prevent the eviction
// callback from closing the model before the caller can use it.
func (rt *refTracker) incRef(key string) int {
	rt.mu.Lock()
	rt.refCounts[key]++
	count := rt.refCounts[key]
	rt.mu.Unlock()
	return count
}

// rollbackRef decrements the reference count after a failed Acquire().
// Cleans up the map entry if the count reaches zero.
func (rt *refTracker) rollbackRef(key string) {
	rt.mu.Lock()
	rt.refCounts[key]--
	if rt.refCounts[key] == 0 {
		delete(rt.refCounts, key)
	}
	rt.mu.Unlock()
}

// releaseRef decrements the reference count and returns any orphaned close
// handles if the count reached zero. The caller must close the returned
// handles outside of any lock. Returns the new count and any orphans.
// If the key has no active references, this is a no-op (returns 0, nil).
func (rt *refTracker) releaseRef(key string) (int, []func() error) {
	rt.mu.Lock()
	if rt.refCounts[key] == 0 {
		// No matching Acquire — spurious Release is a no-op.
		rt.mu.Unlock()
		return 0, nil
	}
	rt.refCounts[key]--
	count := rt.refCounts[key]

	var orphans []func() error
	if count == 0 {
		delete(rt.refCounts, key)
		orphans = rt.evictedHandles[key]
		delete(rt.evictedHandles, key)
	}
	rt.mu.Unlock()
	return count, orphans
}

// deferCloseIfInUse checks whether the model key is actively acquired.
// If refcount > 0, it appends closeFn to the orphan list and returns true.
// If refcount == 0, it returns false and the caller should close immediately.
func (rt *refTracker) deferCloseIfInUse(key string, closeFn func() error) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.refCounts[key] > 0 {
		rt.evictedHandles[key] = append(rt.evictedHandles[key], closeFn)
		return true
	}
	return false
}

// drainOrphans closes all orphaned handles and clears the map.
// Returns a map of model name → close errors for logging.
func (rt *refTracker) drainOrphans() map[string][]error {
	rt.mu.Lock()
	handles := rt.evictedHandles
	rt.evictedHandles = make(map[string][]func() error)
	rt.mu.Unlock()

	errs := make(map[string][]error)
	for name, orphans := range handles {
		for _, closeFn := range orphans {
			if err := closeFn(); err != nil {
				errs[name] = append(errs[name], err)
			}
		}
	}
	return errs
}

// closeOrphans closes orphaned model handles and logs any errors.
func closeOrphans(logger *zap.Logger, modelType, key string, orphans []func() error) {
	for _, closeFn := range orphans {
		if err := closeFn(); err != nil {
			logger.Warn("Error closing orphaned "+modelType+" model",
				zap.String("model", key),
				zap.Error(err))
		}
	}
}

// logDrainErrors logs errors from drainOrphans during shutdown.
func logDrainErrors(logger *zap.Logger, modelType string, errs map[string][]error) {
	for name, errors := range errs {
		for _, err := range errors {
			logger.Warn("Error closing orphaned "+modelType+" model during shutdown",
				zap.String("model", name),
				zap.Error(err))
		}
	}
}

// evictionReasonString returns a human-readable string for a ttlcache eviction reason.
func evictionReasonString(reason ttlcache.EvictionReason) string {
	switch reason {
	case ttlcache.EvictionReasonExpired:
		return "expired (keep-alive timeout)"
	case ttlcache.EvictionReasonCapacityReached:
		return "capacity reached (LRU eviction)"
	default:
		return "unknown"
	}
}
