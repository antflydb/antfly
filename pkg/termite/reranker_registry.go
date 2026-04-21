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
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/reranking"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	termreranking "github.com/antflydb/antfly/pkg/termite/lib/reranking"
	"go.uber.org/zap"
)

// RerankerModelInfo holds metadata about a discovered reranker model (not loaded yet)
type RerankerModelInfo struct {
	Name         string
	Path         string
	OnnxFilename string
	PoolSize     int
}

// RerankerRegistry manages reranker models with lazy loading and TTL-based unloading
type RerankerRegistry struct {
	base           *BaseRegistry[RerankerModelInfo, reranking.Model]
	modelsDir      string
	sessionManager *backends.SessionManager
	poolSize       int

	// Pinned models (never evicted, stored separately from cache)
	pinned   map[string]reranking.Model
	pinnedMu sync.RWMutex
}

// RerankerConfig configures the reranker registry
type RerankerConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
	PoolSize        int           // Number of concurrent pipelines per model (0 = default)
}

// NewRerankerRegistry creates a new lazy-loading reranker registry
func NewRerankerRegistry(
	config RerankerConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*RerankerRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	r := &RerankerRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
		poolSize:       poolSize,
		pinned:         make(map[string]reranking.Model),
	}

	r.base = newBaseRegistry(BaseRegistryConfig[RerankerModelInfo, reranking.Model]{
		ModelType:       "reranker",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *RerankerModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         func(m reranking.Model) error { return m.Close() },
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		return nil, err
	}

	// Register any built-in rerankers that were registered via init()
	for _, factory := range getBuiltinRerankerFactories() {
		name, model, err := factory()
		if err != nil {
			logger.Warn("Failed to initialize built-in reranker", zap.Error(err))
			continue
		}
		r.pinnedMu.Lock()
		r.pinned[name] = model
		r.pinnedMu.Unlock()
		logger.Info("Registered built-in reranker as pinned model",
			zap.String("model", name))
	}

	logger.Info("Lazy reranker registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all reranker models in the models directory without loading them
func (r *RerankerRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No reranker models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("Reranker models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeReranker, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering reranker models: %w", err)
	}

	poolSize := r.poolSize

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()
		variants := dm.Variants

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

			r.base.discovered[registryName] = &RerankerModelInfo{
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
			r.base.logger.Info("Discovered reranker model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath),
				zap.Strings("variants", variantIDs))
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("Reranker model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a reranker model from disk. Called by BaseRegistry.loadModel.
func (r *RerankerRegistry) loadModel(info *RerankerModelInfo) (reranking.Model, error) {
	cfg := termreranking.PooledRerankerConfig{
		ModelPath:     info.Path,
		PoolSize:      info.PoolSize,
		ModelBackends: nil, // Use all available backends
		Logger:        r.base.logger.Named(info.Name),
	}
	model, backendUsed, err := termreranking.NewPooledReranker(cfg, r.sessionManager)
	if err != nil {
		return nil, fmt.Errorf("loading reranker model %s: %w", info.Name, err)
	}

	r.base.logger.Info("Successfully loaded reranker model",
		zap.String("name", info.Name),
		zap.String("backend", string(backendUsed)),
		zap.Int("poolSize", info.PoolSize))

	return model, nil
}

// Get returns a reranker by name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *RerankerRegistry) Get(modelName string) (reranking.Model, error) {
	// Check pinned first (never evicted)
	r.pinnedMu.RLock()
	if model, ok := r.pinned[modelName]; ok {
		r.pinnedMu.RUnlock()
		return model, nil
	}
	r.pinnedMu.RUnlock()

	return r.base.get(modelName)
}

// Acquire returns a reranker by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *RerankerRegistry) Acquire(modelName string) (reranking.Model, error) {
	// Pinned models are never evicted — no ref-counting needed.
	r.pinnedMu.RLock()
	if model, ok := r.pinned[modelName]; ok {
		r.pinnedMu.RUnlock()
		return model, nil
	}
	r.pinnedMu.RUnlock()

	return r.base.acquire(modelName)
}

// Release decrements the reference count for a model.
func (r *RerankerRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// List returns all available reranker model names (discovered + pinned built-ins).
func (r *RerankerRegistry) List() []string {
	names := r.base.list()

	// Include pinned (built-in) models
	r.pinnedMu.RLock()
	for name := range r.pinned {
		names = append(names, name)
	}
	r.pinnedMu.RUnlock()

	return names
}

// ListLoaded returns only the currently loaded reranker model names (from cache and pinned)
func (r *RerankerRegistry) ListLoaded() []string {
	keys := r.base.listLoaded()

	r.pinnedMu.RLock()
	for name := range r.pinned {
		keys = append(keys, name)
	}
	r.pinnedMu.RUnlock()

	return keys
}

// IsLoaded returns whether a model is currently loaded in memory (in cache or pinned)
func (r *RerankerRegistry) IsLoaded(modelName string) bool {
	r.pinnedMu.RLock()
	isPinned := r.pinned[modelName] != nil
	r.pinnedMu.RUnlock()
	return isPinned || r.base.isLoaded(modelName)
}

func (r *RerankerRegistry) Preload(names []string) error { return r.base.preload(names) }
func (r *RerankerRegistry) PreloadAll() error            { return r.Preload(r.List()) }

// Close stops the cache and unloads all models
func (r *RerankerRegistry) Close() error {
	if err := r.base.close(); err != nil {
		return err
	}

	// Close all pinned models
	r.pinnedMu.Lock()
	for name, model := range r.pinned {
		r.base.logger.Debug("Closing pinned reranker model",
			zap.String("model", name))
		if err := model.Close(); err != nil {
			r.base.logger.Warn("Error closing pinned reranker model",
				zap.String("model", name),
				zap.Error(err))
		}
	}
	r.pinned = make(map[string]reranking.Model)
	r.pinnedMu.Unlock()

	return nil
}
