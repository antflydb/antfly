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
	"fmt"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// BaseRegistryConfig holds shared configuration for constructing a BaseRegistry.
type BaseRegistryConfig[Info any, Model any] struct {
	ModelType       string        // e.g. "embedder", "seq2seq" (for logging)
	KeepAlive       time.Duration // 0 = NoTTL
	MaxLoadedModels uint64        // 0 = unlimited per-registry capacity

	// NameFunc extracts the cache key (model name) from an Info.
	NameFunc func(*Info) string
	// LoadFn loads a model from disk given its discovery info.
	// Called under the registry write lock with double-check-after-lock.
	LoadFn func(*Info) (Model, error)
	// CloseFn releases resources for a loaded model.
	CloseFn func(Model) error
	// DiscoverFn scans the models directory and returns newly discovered entries.
	// It should merge into the existing discovered map (skip already-known entries).
	// Called with NO lock held; the function must acquire locks as needed via
	// the updateDiscovered callback.
	DiscoverFn func() error

	Budget *ModelBudget // optional global budget, nil = no global limit
	Logger *zap.Logger
}

// BaseRegistry provides the shared infrastructure for all model registries:
// TTL cache with LRU eviction, ref-counting via refTracker, variant resolution,
// lazy loading with double-check-after-lock, and coordinated shutdown.
//
// Concrete registries embed this and delegate standard methods, keeping only
// their unique logic (discovery filtering, load functions, extra methods).
type BaseRegistry[Info any, Model any] struct {
	logger    *zap.Logger
	modelType string

	// Model discovery (paths only, not loaded)
	discovered map[string]*Info
	mu         sync.RWMutex

	// Loaded models with TTL cache
	cache *ttlcache.Cache[string, Model]

	// Reference counting to prevent eviction during active use
	refs refTracker

	keepAlive time.Duration

	// Injected callbacks
	nameFunc   func(*Info) string
	loadFn     func(*Info) (Model, error)
	closeFn    func(Model) error
	discoverFn func() error

	budget *ModelBudget
}

// newBaseRegistry creates a BaseRegistry with cache, eviction callbacks, and
// starts the cache cleanup goroutine. The caller should call discoverFn after
// construction to populate the discovered map.
func newBaseRegistry[Info any, Model any](cfg BaseRegistryConfig[Info, Model]) *BaseRegistry[Info, Model] {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	keepAlive := cfg.KeepAlive
	if keepAlive == 0 {
		keepAlive = ttlcache.NoTTL
	}

	r := &BaseRegistry[Info, Model]{
		logger:     logger,
		modelType:  cfg.ModelType,
		discovered: make(map[string]*Info),
		refs:       newRefTracker(),
		keepAlive:  keepAlive,
		nameFunc:   cfg.NameFunc,
		loadFn:     cfg.LoadFn,
		closeFn:    cfg.CloseFn,
		discoverFn: cfg.DiscoverFn,
		budget:     cfg.Budget,
	}

	// Configure TTL cache
	cacheOpts := []ttlcache.Option[string, Model]{
		ttlcache.WithTTL[string, Model](keepAlive),
	}
	if cfg.MaxLoadedModels > 0 {
		cacheOpts = append(cacheOpts,
			ttlcache.WithCapacity[string, Model](cfg.MaxLoadedModels))
	}
	r.cache = ttlcache.New(cacheOpts...)

	// Eviction callback: close models on TTL expiration or LRU eviction,
	// but not on manual deletion (Close() handles that synchronously).
	r.cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, Model]) {
		if reason == ttlcache.EvictionReasonDeleted {
			logger.Debug(cfg.ModelType+" model removed from cache (cleanup handled separately)",
				zap.String("model", item.Key()))
			return
		}

		reasonStr := evictionReasonString(reason)
		model := item.Value()

		// Defer close if model is still acquired
		if r.refs.deferCloseIfInUse(item.Key(), func() error { return r.closeFn(model) }) {
			logger.Warn(cfg.ModelType+" model evicted while in use, deferring close",
				zap.String("model", item.Key()),
				zap.String("reason", reasonStr))
			if r.budget != nil {
				r.budget.Release()
			}
			return
		}

		logger.Info("Evicting "+cfg.ModelType+" model from cache",
			zap.String("model", item.Key()),
			zap.String("reason", reasonStr))
		if err := r.closeFn(model); err != nil {
			logger.Warn("Error closing evicted "+cfg.ModelType+" model",
				zap.String("model", item.Key()),
				zap.Error(err))
		}
		if r.budget != nil {
			r.budget.Release()
		}
	})

	go r.cache.Start()

	// Register with global budget for cross-registry eviction
	if r.budget != nil {
		r.budget.Register(cfg.ModelType, r)
	}

	return r
}

// EvictLRU implements budgetCache. It finds the least-recently-used model
// that is not actively acquired, removes it from the cache, closes it, and
// releases the budget slot. We close the model here directly rather than
// relying on the eviction callback because cache.Delete triggers
// EvictionReasonDeleted which is intentionally skipped (to avoid double-close
// during Close() shutdown).
func (r *BaseRegistry[Info, Model]) EvictLRU() string {
	var victim string
	var victimModel Model
	r.cache.RangeBackwards(func(item *ttlcache.Item[string, Model]) bool {
		key := item.Key()
		r.refs.mu.Lock()
		inUse := r.refs.refCounts[key] > 0
		r.refs.mu.Unlock()
		if inUse {
			return true // skip, continue to next
		}
		victim = key
		victimModel = item.Value()
		return false // found victim, stop
	})
	if victim == "" {
		return ""
	}

	// Re-check ref count before deleting — a concurrent acquire may have
	// incremented the ref between our RangeBackwards scan and now.
	r.refs.mu.Lock()
	if r.refs.refCounts[victim] > 0 {
		r.refs.mu.Unlock()
		return "" // victim was acquired concurrently, abort eviction
	}
	r.refs.mu.Unlock()

	r.cache.Delete(victim)

	r.logger.Info("Budget-evicting "+r.modelType+" model",
		zap.String("model", victim))
	if err := r.closeFn(victimModel); err != nil {
		r.logger.Warn("Error closing budget-evicted "+r.modelType+" model",
			zap.String("model", victim),
			zap.Error(err))
	}
	if r.budget != nil {
		r.budget.Release()
	}
	return victim
}

// acquire returns a model by name and increments its reference count.
// The caller MUST call release() when done.
func (r *BaseRegistry[Info, Model]) acquire(modelName string) (Model, error) {
	info, refKey, err := r.resolveModel(modelName)
	if err != nil {
		var zero Model
		return zero, err
	}

	r.refs.incRef(refKey)

	model, err := r.loadModel(info)
	if err != nil {
		r.refs.rollbackRef(refKey)
		var zero Model
		return zero, err
	}

	r.logger.Debug("Acquired "+r.modelType+" model",
		zap.String("model", refKey))

	return model, nil
}

// release decrements the reference count for a model.
func (r *BaseRegistry[Info, Model]) release(modelName string) {
	r.mu.RLock()
	refKey := resolveRefName(modelName, r.discovered)
	r.mu.RUnlock()

	count, orphans := r.refs.releaseRef(refKey)

	r.logger.Debug("Released "+r.modelType+" model",
		zap.String("model", refKey),
		zap.Int("refCount", count))

	closeOrphans(r.logger, r.modelType, refKey, orphans)
}

// get returns a model by name, loading it if necessary.
// DEPRECATED: Use acquire() instead to prevent eviction during use.
func (r *BaseRegistry[Info, Model]) get(modelName string) (Model, error) {
	// Check cache first
	if item := r.cache.Get(modelName); item != nil {
		r.logger.Debug(r.modelType+" cache hit", zap.String("model", modelName))
		return item.Value(), nil
	}

	info, resolved, err := r.resolveModel(modelName)
	if err != nil {
		var zero Model
		return zero, err
	}
	if resolved != modelName {
		// Check cache under resolved name
		if item := r.cache.Get(resolved); item != nil {
			r.logger.Debug(r.modelType+" cache hit (resolved)", zap.String("model", resolved))
			return item.Value(), nil
		}
	}

	return r.loadModel(info)
}

// resolveModel looks up a model by name, re-discovering if needed, and
// falls back to variant resolution. Returns the info, the resolved cache key,
// and any error.
func (r *BaseRegistry[Info, Model]) resolveModel(modelName string) (*Info, string, error) {
	r.mu.RLock()
	info, ok := r.discovered[modelName]
	r.mu.RUnlock()

	if ok {
		return info, modelName, nil
	}

	// Re-scan disk for newly pulled models
	if r.discoverFn != nil {
		if err := r.discoverFn(); err != nil {
			r.logger.Debug(r.modelType+" re-discovery failed", zap.Error(err))
		}
	}

	r.mu.RLock()
	info, resolved, ok := resolveVariant(modelName, r.discovered)
	r.mu.RUnlock()
	if !ok {
		return nil, "", fmt.Errorf("%s model not found: %s", r.modelType, modelName)
	}
	if resolved != modelName {
		r.logger.Info("Resolved model name to variant",
			zap.String("requested", modelName),
			zap.String("resolved", resolved))
	}
	return info, resolved, nil
}

// loadModel loads a model from disk with double-check-after-lock to prevent
// concurrent duplicate loads. Reserves a budget slot before loading.
func (r *BaseRegistry[Info, Model]) loadModel(info *Info) (Model, error) {
	name := r.nameFunc(info)

	// Reserve budget slot BEFORE acquiring registry lock (avoid nested locks)
	if r.budget != nil {
		if err := r.budget.Reserve(); err != nil {
			var zero Model
			return zero, fmt.Errorf("loading %s model %s: %w", r.modelType, name, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check cache after acquiring lock
	if item := r.cache.Get(name); item != nil {
		// Model was loaded by another goroutine; release the budget slot we just reserved
		if r.budget != nil {
			r.budget.Release()
		}
		return item.Value(), nil
	}

	r.logger.Info("Loading "+r.modelType+" model on demand",
		zap.String("model", name))

	model, err := r.loadFn(info)
	if err != nil {
		if r.budget != nil {
			r.budget.Release()
		}
		return model, err
	}

	r.cache.Set(name, model, r.keepAlive)
	return model, nil
}

// list returns all discovered model names, re-scanning the models directory first.
func (r *BaseRegistry[Info, Model]) list() []string {
	if r.discoverFn != nil {
		_ = r.discoverFn()
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.discovered))
	for name := range r.discovered {
		names = append(names, name)
	}
	return names
}

// listLoaded returns the names of currently loaded (cached) models.
func (r *BaseRegistry[Info, Model]) listLoaded() []string {
	return r.cache.Keys()
}

// isLoaded returns whether a model is currently loaded in memory.
func (r *BaseRegistry[Info, Model]) isLoaded(modelName string) bool {
	return r.cache.Has(modelName)
}

// preload loads the specified models to avoid first-request latency.
func (r *BaseRegistry[Info, Model]) preload(modelNames []string) error {
	if len(modelNames) == 0 {
		return nil
	}

	r.logger.Info("Preloading "+r.modelType+" models", zap.Strings("models", modelNames))

	var loaded, failed int
	for _, name := range modelNames {
		if _, err := r.get(name); err != nil {
			r.logger.Warn("Failed to preload "+r.modelType+" model",
				zap.String("model", name),
				zap.Error(err))
			failed++
		} else {
			r.logger.Info("Preloaded "+r.modelType+" model",
				zap.String("model", name))
			loaded++
		}
	}

	r.logger.Info(r.modelType+" preloading complete",
		zap.Int("loaded", loaded),
		zap.Int("failed", failed))

	if failed > 0 && loaded == 0 {
		return fmt.Errorf("all %d %s models failed to preload", failed, r.modelType)
	}

	return nil
}

// preloadAll loads all discovered models (for eager loading mode).
func (r *BaseRegistry[Info, Model]) preloadAll() error {
	return r.preload(r.list())
}

// close stops the cache, closes all loaded models synchronously, and drains orphans.
func (r *BaseRegistry[Info, Model]) close() error {
	r.logger.Info("Closing " + r.modelType + " registry")

	// Stop cache cleanup goroutine first to prevent eviction callbacks during shutdown
	r.cache.Stop()

	// Close all cached models synchronously
	for _, key := range r.cache.Keys() {
		if item := r.cache.Get(key); item != nil {
			r.logger.Debug("Closing cached "+r.modelType+" model",
				zap.String("model", key))
			if err := r.closeFn(item.Value()); err != nil {
				r.logger.Warn("Error closing "+r.modelType+" model",
					zap.String("model", key),
					zap.Error(err))
			}
			if r.budget != nil {
				r.budget.Release()
			}
		}
	}

	// Clear cache (eviction callbacks skip close for EvictionReasonDeleted)
	r.cache.DeleteAll()

	// Drain orphaned handles from models evicted while in use
	logDrainErrors(r.logger, r.modelType, r.refs.drainOrphans())

	return nil
}
