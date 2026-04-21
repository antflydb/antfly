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
	"os"
	"path/filepath"
	"time"

	"slices"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	termchunking "github.com/antflydb/antfly/pkg/termite/lib/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// ChunkerModelInfo holds metadata about a discovered chunker model (not loaded yet)
type ChunkerModelInfo struct {
	Name         string
	Path         string
	OnnxFilename string                   // Text chunkers (variant ONNX file)
	PoolSize     int                      // Pipeline pool size (text chunkers)
	Capabilities []string                 // From manifest (e.g., ["audio"])
	Backends     []string                 // Required backends from manifest
	SessionOpts  []backends.SessionOption // For GoMLX backend support
}

// mediaCapabilities are the capabilities that qualify a chunker model as a media chunker.
var mediaCapabilities = []modelregistry.Capability{
	modelregistry.CapabilityAudio,
}

// ChunkerRegistry manages chunker models with lazy loading and TTL-based unloading.
// It handles both text chunkers and media chunkers (audio, etc.) in a single registry
// with capability-based dispatch.
type ChunkerRegistry struct {
	base           *BaseRegistry[ChunkerModelInfo, chunking.Chunker]
	modelsDir      string
	sessionManager *backends.SessionManager
	poolSize       int

	// Loaded media chunker models with TTL cache (separate Go type from text chunkers)
	mediaCache     *ttlcache.Cache[string, termchunking.CloseableMediaChunker]
	mediaKeepAlive time.Duration
}

// ChunkerConfig configures the lazy chunker registry
type ChunkerConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep text models loaded (0 = forever)
	MediaKeepAlive  time.Duration // How long to keep media models loaded (0 = use KeepAlive)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
	PoolSize        int           // Number of concurrent pipelines per model (0 = default)
}

// NewChunkerRegistry creates a new lazy-loading chunker registry
func NewChunkerRegistry(
	config ChunkerConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*ChunkerRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	mediaKeepAlive := config.MediaKeepAlive
	if mediaKeepAlive == 0 {
		ka := config.KeepAlive
		if ka == 0 {
			ka = ttlcache.NoTTL
		}
		mediaKeepAlive = ka
	}

	r := &ChunkerRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
		poolSize:       poolSize,
		mediaKeepAlive: mediaKeepAlive,
	}

	r.base = newBaseRegistry(BaseRegistryConfig[ChunkerModelInfo, chunking.Chunker]{
		ModelType:       "chunker",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *ChunkerModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         func(m chunking.Chunker) error { return m.Close() },
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	// Configure media chunker TTL cache
	mediaCacheOpts := []ttlcache.Option[string, termchunking.CloseableMediaChunker]{
		ttlcache.WithTTL[string, termchunking.CloseableMediaChunker](mediaKeepAlive),
	}
	if config.MaxLoadedModels > 0 {
		mediaCacheOpts = append(mediaCacheOpts,
			ttlcache.WithCapacity[string, termchunking.CloseableMediaChunker](config.MaxLoadedModels))
	}
	r.mediaCache = ttlcache.New(mediaCacheOpts...)

	// Set up media cache eviction callback (shares refs with base)
	r.mediaCache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, termchunking.CloseableMediaChunker]) {
		if reason == ttlcache.EvictionReasonDeleted {
			logger.Debug("Media chunker model removed from cache (cleanup handled separately)",
				zap.String("model", item.Key()))
			return
		}

		reasonStr := evictionReasonString(reason)
		model := item.Value()
		if r.base.refs.deferCloseIfInUse(item.Key(), func() error { return model.Close() }) {
			logger.Warn("Media chunker model evicted while in use, deferring close",
				zap.String("model", item.Key()),
				zap.String("reason", reasonStr))
			return
		}

		logger.Info("Evicting media chunker model from cache",
			zap.String("model", item.Key()),
			zap.String("reason", reasonStr))
		if err := model.Close(); err != nil {
			logger.Warn("Error closing evicted media chunker model",
				zap.String("model", item.Key()),
				zap.Error(err))
		}
	})

	go r.mediaCache.Start()

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		r.mediaCache.Stop()
		return nil, err
	}

	logger.Info("Lazy chunker registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all chunker models in the models directory without loading them
func (r *ChunkerRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No chunker models directory configured")
		return nil
	}

	if _, err := os.Stat(filepath.Clean(r.modelsDir)); os.IsNotExist(err) {
		r.base.logger.Warn("Chunker models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeChunker, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering chunker models: %w", err)
	}

	poolSize := r.poolSize

	// Collect log entries to emit outside the lock
	type discoveryLog struct {
		name         string
		path         string
		capabilities []string // non-nil for media models
		variants     []string // non-nil for text models
	}
	var logEntries []discoveryLog

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()
		variants := dm.Variants

		// Check if this model has media capabilities (e.g., audio)
		hasMediaCap := false
		if dm.Manifest != nil {
			if slices.ContainsFunc(mediaCapabilities, dm.Manifest.HasCapability) {
				hasMediaCap = true
			}
		}

		// Media-capable models: register with capabilities and session options
		if hasMediaCap && dm.Manifest != nil {
			if _, exists := r.base.discovered[registryFullName]; exists {
				continue
			}

			// Build session options from manifest
			var sessionOpts []backends.SessionOption
			if dm.Manifest.SessionOptions != nil {
				if len(dm.Manifest.SessionOptions.InputConstants) > 0 {
					sessionOpts = append(sessionOpts, backends.WithInputConstants(dm.Manifest.SessionOptions.InputConstants))
				}
				if len(dm.Manifest.SessionOptions.DynamicAxes) > 0 {
					overrides := make([]backends.DynamicAxisOverride, len(dm.Manifest.SessionOptions.DynamicAxes))
					for i, da := range dm.Manifest.SessionOptions.DynamicAxes {
						overrides[i] = backends.DynamicAxisOverride{
							InputName: da.InputName,
							Axis:      da.Axis,
							ParamName: da.ParamName,
						}
					}
					sessionOpts = append(sessionOpts, backends.WithDynamicAxes(overrides))
				}
			}

			r.base.discovered[registryFullName] = &ChunkerModelInfo{
				Name:         registryFullName,
				Path:         modelPath,
				Capabilities: dm.Manifest.Capabilities,
				Backends:     dm.Manifest.Backends,
				SessionOpts:  sessionOpts,
			}

			logEntries = append(logEntries, discoveryLog{
				name:         registryFullName,
				path:         modelPath,
				capabilities: dm.Manifest.Capabilities,
			})
			continue
		}

		// Text chunker models: register each variant
		if len(variants) == 0 {
			continue
		}

		anyNew := false
		for variantID, onnxFilename := range variants {
			registryName := registryFullName
			if variantID != "" {
				registryName = registryFullName + "-" + variantID
			}

			if _, exists := r.base.discovered[registryName]; exists {
				continue
			}

			r.base.discovered[registryName] = &ChunkerModelInfo{
				Name:         registryName,
				Path:         modelPath,
				OnnxFilename: onnxFilename,
				PoolSize:     poolSize,
			}
			anyNew = true
		}

		if anyNew {
			variantIDs := make([]string, 0, len(variants))
			for v := range variants {
				if v == "" {
					variantIDs = append(variantIDs, "default")
				} else {
					variantIDs = append(variantIDs, v)
				}
			}
			logEntries = append(logEntries, discoveryLog{
				name:     registryFullName,
				path:     modelPath,
				variants: variantIDs,
			})
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	// Log discovered models outside the lock
	for _, entry := range logEntries {
		if entry.capabilities != nil {
			r.base.logger.Info("Discovered media chunker model (not loaded)",
				zap.String("name", entry.name),
				zap.String("path", entry.path),
				zap.Strings("capabilities", entry.capabilities))
		} else {
			r.base.logger.Info("Discovered text chunker model (not loaded)",
				zap.String("name", entry.name),
				zap.String("path", entry.path),
				zap.Strings("variants", entry.variants))
		}
	}

	r.base.logger.Info("Chunker model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a text chunker model from disk. Called by BaseRegistry.loadModel.
func (r *ChunkerRegistry) loadModel(info *ChunkerModelInfo) (chunking.Chunker, error) {
	cfg := termchunking.PooledChunkerConfig{
		ModelPath:     info.Path,
		PoolSize:      info.PoolSize,
		ChunkerConfig: termchunking.DefaultChunkerConfig(),
		ModelBackends: nil, // Use all available backends
		Logger:        r.base.logger.Named(info.Name),
	}
	chunker, backendUsed, err := termchunking.NewPooledChunker(cfg, r.sessionManager)
	if err != nil {
		return nil, fmt.Errorf("loading chunker model %s: %w", info.Name, err)
	}

	r.base.logger.Info("Successfully loaded chunker model",
		zap.String("name", info.Name),
		zap.String("backend", string(backendUsed)),
		zap.Int("poolSize", info.PoolSize))

	return chunker, nil
}

// Get returns a chunker by name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *ChunkerRegistry) Get(modelName string) (chunking.Chunker, error) {
	return r.base.get(modelName)
}

// Acquire returns a chunker by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *ChunkerRegistry) Acquire(modelName string) (chunking.Chunker, error) {
	return r.base.acquire(modelName)
}

// Release decrements the reference count for a model.
func (r *ChunkerRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// AcquireMedia returns a media chunker by name and increments its reference count.
// Only valid for models with media capabilities (e.g., "audio").
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *ChunkerRegistry) AcquireMedia(modelName string) (termchunking.MediaChunker, error) {
	// Resolve variant so the ref key matches the cache key.
	r.base.mu.RLock()
	info, ok := r.base.discovered[modelName]
	refKey := modelName
	r.base.mu.RUnlock()

	if !ok {
		if err := r.discoverModels(); err != nil {
			r.base.logger.Debug("Chunker re-discovery failed", zap.Error(err))
		}
		r.base.mu.RLock()
		var resolved string
		info, resolved, ok = resolveVariant(modelName, r.base.discovered)
		r.base.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("chunker model not found: %s", modelName)
		}
		refKey = resolved
		if resolved != modelName {
			r.base.logger.Info("Resolved model name to variant",
				zap.String("requested", modelName),
				zap.String("resolved", resolved))
		}
	}

	if !r.isMediaModel(info) {
		return nil, fmt.Errorf("model %s does not have media capabilities", modelName)
	}

	r.base.refs.incRef(refKey)

	chunker, err := r.loadMediaModel(info)
	if err != nil {
		r.base.refs.rollbackRef(refKey)
		return nil, err
	}

	r.base.logger.Debug("Acquired media chunker model",
		zap.String("model", refKey))

	return chunker, nil
}

// isMediaModel checks if a model has any media capability.
func (r *ChunkerRegistry) isMediaModel(info *ChunkerModelInfo) bool {
	for _, cap := range mediaCapabilities {
		if slices.Contains(info.Capabilities, string(cap)) {
			return true
		}
	}
	return false
}

// loadMediaModel loads a media chunker model from disk.
// Note: media models are managed by a separate cache and are NOT tracked by
// the global ModelBudget. They have their own TTL and capacity limits.
func (r *ChunkerRegistry) loadMediaModel(info *ChunkerModelInfo) (termchunking.CloseableMediaChunker, error) {
	r.base.mu.Lock()
	defer r.base.mu.Unlock()

	// Double-check cache after acquiring lock
	if item := r.mediaCache.Get(info.Name); item != nil {
		return item.Value(), nil
	}

	r.base.logger.Info("Loading media chunker model on demand",
		zap.String("model", info.Name),
		zap.String("path", info.Path))

	cfg := termchunking.MediaChunkerConfig{
		ModelPath:     info.Path,
		Capabilities:  info.Capabilities,
		ModelBackends: info.Backends,
		SessionOpts:   info.SessionOpts,
		Logger:        r.base.logger.Named(info.Name),
	}

	chunker, backendUsed, err := termchunking.NewMediaChunkerFromModel(cfg, r.sessionManager)
	if err != nil {
		return nil, fmt.Errorf("loading media chunker model %s: %w", info.Name, err)
	}

	r.base.logger.Info("Successfully loaded media chunker model",
		zap.String("name", info.Name),
		zap.String("backend", string(backendUsed)))

	r.mediaCache.Set(info.Name, chunker, r.mediaKeepAlive)

	return chunker, nil
}

// ListWithCapabilities returns a map of model names to their capabilities.
func (r *ChunkerRegistry) ListWithCapabilities() map[string][]string {
	_ = r.discoverModels()

	r.base.mu.RLock()
	defer r.base.mu.RUnlock()

	result := make(map[string][]string, len(r.base.discovered))
	for name, info := range r.base.discovered {
		result[name] = info.Capabilities
	}
	return result
}

// HasCapability checks if a model has a specific capability (e.g., audio).
func (r *ChunkerRegistry) HasCapability(modelName string, capability modelregistry.Capability) bool {
	r.base.mu.RLock()
	info, known := r.base.discovered[modelName]
	r.base.mu.RUnlock()

	if !known {
		if err := r.discoverModels(); err != nil {
			r.base.logger.Debug("Chunker re-discovery failed", zap.Error(err))
		}
		r.base.mu.RLock()
		info, known = r.base.discovered[modelName]
		r.base.mu.RUnlock()
		if !known {
			return false
		}
	}

	return slices.Contains(info.Capabilities, string(capability))
}

func (r *ChunkerRegistry) List() []string               { return r.base.list() }
func (r *ChunkerRegistry) ListLoaded() []string         { return r.base.listLoaded() }
func (r *ChunkerRegistry) IsLoaded(name string) bool    { return r.base.isLoaded(name) }
func (r *ChunkerRegistry) Preload(names []string) error { return r.base.preload(names) }
func (r *ChunkerRegistry) PreloadAll() error            { return r.base.preloadAll() }

// Close stops the caches and unloads all models
func (r *ChunkerRegistry) Close() error {
	// Close base registry (text chunkers)
	if err := r.base.close(); err != nil {
		return err
	}

	// Stop media cache
	r.mediaCache.Stop()

	// Close all cached media models synchronously
	for _, key := range r.mediaCache.Keys() {
		if item := r.mediaCache.Get(key); item != nil {
			r.base.logger.Debug("Closing cached media chunker model",
				zap.String("model", key))
			if err := item.Value().Close(); err != nil {
				r.base.logger.Warn("Error closing media chunker model",
					zap.String("model", key),
					zap.Error(err))
			}
		}
	}

	r.mediaCache.DeleteAll()

	return nil
}
