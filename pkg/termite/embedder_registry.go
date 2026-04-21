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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	termembeddings "github.com/antflydb/antfly/pkg/termite/lib/embeddings"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// Default keep-alive duration (matches Ollama's 5-minute default)
const DefaultKeepAlive = 5 * time.Minute

// EmbedderModelInfo holds metadata about a discovered model (not loaded yet)
type EmbedderModelInfo struct {
	Name             string
	Path             string
	OnnxFilename     string // e.g., "model.onnx", "model_f16.onnx", "model_i8.onnx"
	PoolSize         int
	ModelType        string   // "embedder" or "multimodal"
	Quantized        bool     // Whether to load quantized variant (*_quantized.onnx)
	Capabilities     []string // e.g., ["image"], ["audio"], ["image", "audio"]
	Variants         []string // Available variant IDs (e.g., ["f16", "i8"])
	RequiredBackends []string // If set, only use these backends (e.g., ["onnx"] for models with XLA-incompatible ops)
}

// modelsRequiringONNX lists model name patterns that require the ONNX backend.
var modelsRequiringONNX = []string{
	"nomic-ai/nomic-embed-text-v1.5", // Uses dynamic Range op in rotary embeddings
}

// getRequiredBackends returns the required backends for a model.
func getRequiredBackends(modelName string, manifest *modelregistry.ModelManifest) []string {
	if manifest != nil && len(manifest.Backends) > 0 {
		return manifest.Backends
	}
	for _, pattern := range modelsRequiringONNX {
		if strings.Contains(modelName, pattern) {
			return []string{"onnx"}
		}
	}
	return nil
}

// EmbedderRegistry manages embedding models with lazy loading and TTL-based unloading
type EmbedderRegistry struct {
	base           *BaseRegistry[EmbedderModelInfo, embeddings.Embedder]
	modelsDir      string
	sessionManager *backends.SessionManager
	poolSize       int

	// Sparse embedder cache (separate from dense because of different Go types)
	sparseCache *ttlcache.Cache[string, embeddings.SparseEmbedder]

	// Pinned models (never evicted, stored separately from cache)
	pinned   map[string]embeddings.Embedder
	pinnedMu sync.RWMutex
}

// EmbedderConfig configures the embedder registry
type EmbedderConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
	PoolSize        int           // Number of concurrent pipelines per model (0 = default)
}

// closeFnEmbedder closes an embeddings.Embedder if it implements Close().
func closeFnEmbedder(m embeddings.Embedder) error {
	if closer, ok := m.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// NewEmbedderRegistry creates a new lazy-loading embedder registry
func NewEmbedderRegistry(
	config EmbedderConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*EmbedderRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	r := &EmbedderRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
		poolSize:       poolSize,
		pinned:         make(map[string]embeddings.Embedder),
	}

	r.base = newBaseRegistry(BaseRegistryConfig[EmbedderModelInfo, embeddings.Embedder]{
		ModelType:       "embedder",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *EmbedderModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         closeFnEmbedder,
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	// Configure sparse embedder cache
	keepAlive := config.KeepAlive
	if keepAlive == 0 {
		keepAlive = ttlcache.NoTTL
	}
	sparseCacheOpts := []ttlcache.Option[string, embeddings.SparseEmbedder]{
		ttlcache.WithTTL[string, embeddings.SparseEmbedder](keepAlive),
	}
	if config.MaxLoadedModels > 0 {
		sparseCacheOpts = append(sparseCacheOpts,
			ttlcache.WithCapacity[string, embeddings.SparseEmbedder](config.MaxLoadedModels))
	}
	r.sparseCache = ttlcache.New(sparseCacheOpts...)

	// Sparse cache eviction callback (shares refs with base)
	r.sparseCache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, embeddings.SparseEmbedder]) {
		modelName := item.Key()
		embedder := item.Value()

		if reason == ttlcache.EvictionReasonDeleted {
			logger.Debug("Sparse embedder model removed from cache (cleanup handled separately)",
				zap.String("model", modelName))
			return
		}

		reasonStr := evictionReasonString(reason)

		if r.base.refs.deferCloseIfInUse(modelName, func() error {
			if closer, ok := embedder.(interface{ Close() error }); ok {
				return closer.Close()
			}
			return nil
		}) {
			logger.Warn("Sparse embedder model evicted while in use, deferring close",
				zap.String("model", modelName),
				zap.String("reason", reasonStr))
			return
		}

		logger.Info("Unloading sparse embedder model",
			zap.String("model", modelName),
			zap.String("reason", reasonStr))

		if closer, ok := embedder.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				logger.Warn("Error closing sparse embedder",
					zap.String("model", modelName),
					zap.Error(err))
			}
		}
	})

	go r.sparseCache.Start()

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		r.sparseCache.Stop()
		return nil, err
	}

	// Register any built-in embedders that were registered via init()
	for _, factory := range getBuiltinEmbedderFactories() {
		name, embedder, err := factory()
		if err != nil {
			logger.Warn("Failed to initialize built-in embedder", zap.Error(err))
			continue
		}
		r.pinnedMu.Lock()
		r.pinned[name] = embedder
		r.pinnedMu.Unlock()
		logger.Info("Registered built-in embedder as pinned model",
			zap.String("model", name))
	}

	return r, nil
}

// discoverModels scans the models directory and records available models
func (r *EmbedderRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No embedder models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("Embedder models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeEmbedder, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering embedder models: %w", err)
	}

	poolSize := r.poolSize

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()
		variants := dm.Variants
		requiredBackends := getRequiredBackends(registryFullName, dm.Manifest)

		// Detect multimodal capabilities from manifest or file presence
		mc := modelregistry.DetectMultimodalCapabilities(modelPath)
		var caps []string
		if dm.Manifest != nil && len(dm.Manifest.Capabilities) > 0 {
			for _, c := range dm.Manifest.Capabilities {
				if c == string(modelregistry.CapabilityImage) || c == string(modelregistry.CapabilityAudio) {
					caps = append(caps, c)
				}
			}
		}
		if len(caps) == 0 {
			if mc.HasImage || mc.HasImageQuantized {
				caps = append(caps, string(modelregistry.CapabilityImage))
			}
			if mc.HasAudio || mc.HasAudioQuantized {
				caps = append(caps, string(modelregistry.CapabilityAudio))
			}
		}

		// Multimodal models: register standard + quantized variants
		if len(caps) > 0 {
			if _, exists := r.base.discovered[registryFullName]; exists {
				continue
			}

			hasStandard := (mc.HasImage || mc.HasAudio)
			hasQuantized := (mc.HasImageQuantized || mc.HasAudioQuantized)

			r.base.logger.Info("Discovered multimodal embedder model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath),
				zap.Strings("capabilities", caps),
				zap.Bool("has_standard", hasStandard),
				zap.Bool("has_quantized", hasQuantized))

			if hasStandard {
				r.base.discovered[registryFullName] = &EmbedderModelInfo{
					Name:             registryFullName,
					Path:             modelPath,
					PoolSize:         1,
					ModelType:        "multimodal",
					Capabilities:     caps,
					Variants:         []string{"default"},
					RequiredBackends: requiredBackends,
				}
			}

			if hasQuantized {
				quantizedName := registryFullName + "-i8-qt"
				if _, exists := r.base.discovered[quantizedName]; !exists {
					r.base.discovered[quantizedName] = &EmbedderModelInfo{
						Name:             quantizedName,
						Path:             modelPath,
						PoolSize:         1,
						ModelType:        "multimodal",
						Quantized:        true,
						Capabilities:     caps,
						Variants:         []string{"quantized"},
						RequiredBackends: requiredBackends,
					}
				}
			}
			continue
		}

		// Standard text-only embedder
		if len(variants) == 0 {
			continue
		}

		anyNew := false
		variantIDs := make([]string, 0, len(variants))
		for v := range variants {
			if v == "" {
				variantIDs = append(variantIDs, "default")
			} else {
				variantIDs = append(variantIDs, v)
			}
		}

		var textCaps []string
		if dm.Manifest != nil {
			for _, c := range dm.Manifest.Capabilities {
				if c != string(modelregistry.CapabilityImage) && c != string(modelregistry.CapabilityAudio) {
					textCaps = append(textCaps, c)
				}
			}
		}

		for variantID, onnxFilename := range variants {
			registryName := registryFullName
			if variantID != "" {
				registryName = registryFullName + "-" + variantID
			}

			if _, exists := r.base.discovered[registryName]; exists {
				continue
			}

			r.base.discovered[registryName] = &EmbedderModelInfo{
				Name:             registryName,
				Path:             modelPath,
				OnnxFilename:     onnxFilename,
				PoolSize:         poolSize,
				ModelType:        "embedder",
				Capabilities:     textCaps,
				Variants:         variantIDs,
				RequiredBackends: requiredBackends,
			}
			anyNew = true
		}

		if anyNew {
			r.base.logger.Info("Discovered embedder model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath),
				zap.Strings("variants", variantIDs))
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("Embedder model discovery complete",
		zap.Int("models_discovered", discoveredCount),
		zap.Duration("keep_alive", r.base.keepAlive))

	return nil
}

// loadModel loads a dense embedder model on demand. Called by BaseRegistry.loadModel.
func (r *EmbedderRegistry) loadModel(info *EmbedderModelInfo) (embeddings.Embedder, error) {
	r.base.logger.Info("Loading embedder model on demand",
		zap.String("model", info.Name),
		zap.String("path", info.Path),
		zap.String("model_type", info.ModelType),
		zap.String("onnx_filename", info.OnnxFilename),
		zap.Int("pool_size", info.PoolSize))

	cfg := termembeddings.PooledEmbedderConfig{
		ModelPath:     info.Path,
		PoolSize:      info.PoolSize,
		Normalize:     true,
		Quantized:     info.Quantized,
		ModelBackends: info.RequiredBackends,
		Logger:        r.base.logger.Named(info.Name),
	}
	embedder, backendUsed, err := termembeddings.NewPooledEmbedder(cfg, r.sessionManager)
	if err != nil {
		return nil, fmt.Errorf("loading embedder model %s: %w", info.Name, err)
	}

	r.base.logger.Info("Successfully loaded embedder model",
		zap.String("model", info.Name),
		zap.String("model_type", info.ModelType),
		zap.String("backend", string(backendUsed)),
		zap.Duration("keep_alive", r.base.keepAlive))

	return embedder, nil
}

// Get returns an embedder by model name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *EmbedderRegistry) Get(modelName string) (embeddings.Embedder, error) {
	// Check pinned first
	r.pinnedMu.RLock()
	if embedder, ok := r.pinned[modelName]; ok {
		r.pinnedMu.RUnlock()
		return embedder, nil
	}
	r.pinnedMu.RUnlock()

	return r.base.get(modelName)
}

// Acquire returns an embedder by model name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *EmbedderRegistry) Acquire(modelName string) (embeddings.Embedder, error) {
	// Pinned models are never evicted — no ref-counting needed.
	r.pinnedMu.RLock()
	if embedder, ok := r.pinned[modelName]; ok {
		r.pinnedMu.RUnlock()
		return embedder, nil
	}
	r.pinnedMu.RUnlock()

	return r.base.acquire(modelName)
}

// Release decrements the reference count for a model.
func (r *EmbedderRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// AcquireSparse returns a sparse embedder by model name and increments its reference count.
// Only valid for models with the "sparse" capability.
func (r *EmbedderRegistry) AcquireSparse(modelName string) (embeddings.SparseEmbedder, error) {
	// Resolve variant inline so the ref key matches the cache key.
	r.base.mu.RLock()
	info, known := r.base.discovered[modelName]
	refKey := modelName
	r.base.mu.RUnlock()

	if !known {
		if err := r.discoverModels(); err != nil {
			r.base.logger.Debug("Embedder re-discovery failed", zap.Error(err))
		}
		r.base.mu.RLock()
		var resolved string
		info, resolved, known = resolveVariant(modelName, r.base.discovered)
		r.base.mu.RUnlock()
		if !known {
			return nil, fmt.Errorf("embedder model not found: %s", modelName)
		}
		refKey = resolved
		if resolved != modelName {
			r.base.logger.Info("Resolved model name to variant",
				zap.String("requested", modelName),
				zap.String("resolved", resolved))
		}
	}

	if !slices.Contains(info.Capabilities, string(modelregistry.CapabilitySparse)) {
		return nil, fmt.Errorf("model %s does not have sparse capability", modelName)
	}

	r.base.refs.incRef(refKey)

	embedder, err := r.loadSparseModel(info)
	if err != nil {
		r.base.refs.rollbackRef(refKey)
		return nil, err
	}

	r.base.logger.Debug("Acquired sparse embedder model",
		zap.String("model", refKey))

	return embedder, nil
}

// loadSparseModel loads a sparse model on demand.
// Note: sparse models are managed by a separate cache and are NOT tracked by
// the global ModelBudget. They have their own TTL and capacity limits.
func (r *EmbedderRegistry) loadSparseModel(info *EmbedderModelInfo) (embeddings.SparseEmbedder, error) {
	r.base.mu.Lock()
	defer r.base.mu.Unlock()

	// Double-check sparse cache after acquiring lock
	if item := r.sparseCache.Get(info.Name); item != nil {
		return item.Value(), nil
	}

	r.base.logger.Info("Loading sparse embedder model on demand",
		zap.String("model", info.Name),
		zap.String("path", info.Path),
		zap.Int("pool_size", info.PoolSize))

	cfg := termembeddings.PooledSparseEmbedderConfig{
		ModelPath:     info.Path,
		PoolSize:      info.PoolSize,
		ModelBackends: info.RequiredBackends,
		Logger:        r.base.logger.Named(info.Name),
	}
	embedder, backendUsed, err := termembeddings.NewPooledSparseEmbedder(cfg, r.sessionManager)
	if err != nil {
		return nil, fmt.Errorf("loading sparse embedder model %s: %w", info.Name, err)
	}

	r.sparseCache.Set(info.Name, embedder, ttlcache.DefaultTTL)

	r.base.logger.Info("Successfully loaded sparse embedder model",
		zap.String("model", info.Name),
		zap.String("backend", string(backendUsed)),
		zap.Duration("keep_alive", r.base.keepAlive))

	return embedder, nil
}

// Touch refreshes the TTL for a model (call after each use to implement Ollama-style keep-alive)
func (r *EmbedderRegistry) Touch(modelName string) {
	if item := r.base.cache.Get(modelName); item != nil {
		r.base.logger.Debug("Refreshed model keep-alive",
			zap.String("model", modelName))
	}
}

// List returns all available model names (discovered + pinned built-ins).
func (r *EmbedderRegistry) List() []string {
	names := r.base.list()

	r.pinnedMu.RLock()
	for name := range r.pinned {
		names = append(names, name)
	}
	r.pinnedMu.RUnlock()

	return names
}

// ListWithCapabilities returns a map of model names to their capabilities.
func (r *EmbedderRegistry) ListWithCapabilities() map[string][]string {
	_ = r.discoverModels()

	result := make(map[string][]string)

	r.base.mu.RLock()
	for name, info := range r.base.discovered {
		result[name] = info.Capabilities
	}
	r.base.mu.RUnlock()

	r.pinnedMu.RLock()
	for name := range r.pinned {
		if _, exists := result[name]; !exists {
			result[name] = nil
		}
	}
	r.pinnedMu.RUnlock()

	return result
}

// ListLoaded returns currently loaded model names (from cache and pinned)
func (r *EmbedderRegistry) ListLoaded() []string {
	keys := r.base.listLoaded()

	r.pinnedMu.RLock()
	for name := range r.pinned {
		keys = append(keys, name)
	}
	r.pinnedMu.RUnlock()

	return keys
}

// IsLoaded checks if a model is currently loaded (in cache or pinned)
func (r *EmbedderRegistry) IsLoaded(modelName string) bool {
	r.pinnedMu.RLock()
	isPinned := r.pinned[modelName] != nil
	r.pinnedMu.RUnlock()
	return isPinned || r.base.isLoaded(modelName)
}

// Unload explicitly unloads a model (triggers eviction callback)
func (r *EmbedderRegistry) Unload(modelName string) {
	r.pinnedMu.RLock()
	isPinned := r.pinned[modelName] != nil
	r.pinnedMu.RUnlock()

	if isPinned {
		r.base.logger.Debug("Cannot unload pinned model",
			zap.String("model", modelName))
		return
	}
	r.base.cache.Delete(modelName)
}

// Pin marks a model as pinned (never evicted).
func (r *EmbedderRegistry) Pin(modelName string) error {
	r.pinnedMu.RLock()
	if r.pinned[modelName] != nil {
		r.pinnedMu.RUnlock()
		return nil
	}
	r.pinnedMu.RUnlock()

	embedder, err := r.Get(modelName)
	if err != nil {
		return fmt.Errorf("pin model %s: %w", modelName, err)
	}

	r.pinnedMu.Lock()
	r.pinned[modelName] = embedder
	r.pinnedMu.Unlock()

	r.base.cache.Delete(modelName)

	r.base.logger.Info("Pinned model (will not be evicted)",
		zap.String("model", modelName))

	return nil
}

// IsPinned returns true if a model is pinned (never evicted)
func (r *EmbedderRegistry) IsPinned(modelName string) bool {
	r.pinnedMu.RLock()
	defer r.pinnedMu.RUnlock()
	return r.pinned[modelName] != nil
}

func (r *EmbedderRegistry) Preload(names []string) error { return r.base.preload(names) }

// HasCapability checks if a model has a specific capability (e.g., image, audio).
func (r *EmbedderRegistry) HasCapability(modelName string, capability modelregistry.Capability) bool {
	r.base.mu.RLock()
	info, known := r.base.discovered[modelName]
	r.base.mu.RUnlock()

	if !known {
		if err := r.discoverModels(); err != nil {
			r.base.logger.Debug("Embedder re-discovery failed", zap.Error(err))
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

// Stats returns cache statistics
func (r *EmbedderRegistry) Stats() map[string]any {
	metrics := r.base.cache.Metrics()

	r.pinnedMu.RLock()
	pinnedCount := len(r.pinned)
	pinnedNames := make([]string, 0, pinnedCount)
	for name := range r.pinned {
		pinnedNames = append(pinnedNames, name)
	}
	r.pinnedMu.RUnlock()

	return map[string]any{
		"discovered":    len(r.base.discovered),
		"loaded":        r.base.cache.Len() + pinnedCount,
		"pinned":        pinnedCount,
		"pinned_models": pinnedNames,
		"cached":        r.base.cache.Len(),
		"hits":          metrics.Hits,
		"misses":        metrics.Misses,
		"keep_alive":    r.base.keepAlive.String(),
		"loaded_models": r.ListLoaded(),
	}
}

// Close stops the cache and unloads all models (including pinned)
func (r *EmbedderRegistry) Close() error {
	// Close base registry (dense embedders)
	if err := r.base.close(); err != nil {
		return err
	}

	// Stop and close sparse cache
	r.sparseCache.Stop()
	for _, key := range r.sparseCache.Keys() {
		if item := r.sparseCache.Get(key); item != nil {
			r.base.logger.Debug("Closing cached sparse embedder",
				zap.String("model", key))
			if closer, ok := item.Value().(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					r.base.logger.Warn("Error closing sparse embedder",
						zap.String("model", key),
						zap.Error(err))
				}
			}
		}
	}
	r.sparseCache.DeleteAll()

	// Close all pinned models
	r.pinnedMu.Lock()
	for name, embedder := range r.pinned {
		r.base.logger.Debug("Closing pinned model",
			zap.String("model", name))
		if closer, ok := embedder.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				r.base.logger.Warn("Error closing pinned embedder",
					zap.String("model", name),
					zap.Error(err))
			}
		}
	}
	r.pinned = make(map[string]embeddings.Embedder)
	r.pinnedMu.Unlock()

	return nil
}
