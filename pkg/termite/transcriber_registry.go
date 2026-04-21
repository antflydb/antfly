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
	"time"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/antflydb/antfly/pkg/termite/lib/transcribing"
	"go.uber.org/zap"
)

// TranscriberModelInfo holds metadata about a discovered transcriber model (not loaded yet)
type TranscriberModelInfo struct {
	Name     string
	Path     string
	PoolSize int
}

// TranscriberRegistry manages transcriber models with lazy loading and TTL-based unloading
type TranscriberRegistry struct {
	base           *BaseRegistry[TranscriberModelInfo, transcribing.Transcriber]
	modelsDir      string
	sessionManager *backends.SessionManager
	poolSize       int
}

// TranscriberConfig configures the transcriber registry
type TranscriberConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
	PoolSize        int           // Number of concurrent pipelines per model (0 = default)
}

// NewTranscriberRegistry creates a new lazy-loading transcriber registry
func NewTranscriberRegistry(
	config TranscriberConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*TranscriberRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	r := &TranscriberRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
		poolSize:       poolSize,
	}

	r.base = newBaseRegistry(BaseRegistryConfig[TranscriberModelInfo, transcribing.Transcriber]{
		ModelType:       "transcriber",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *TranscriberModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         func(m transcribing.Transcriber) error { return m.Close() },
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		return nil, err
	}

	logger.Info("Lazy transcriber registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all transcriber models in the models directory without loading them
func (r *TranscriberRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No transcriber models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("Transcriber models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeTranscriber, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering transcriber models: %w", err)
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
		for variantID := range variants {
			registryName := registryFullName
			if variantID != "" {
				registryName = registryFullName + "-" + variantID
			}

			if _, exists := r.base.discovered[registryName]; exists {
				continue
			}

			r.base.discovered[registryName] = &TranscriberModelInfo{
				Name:     registryName,
				Path:     modelPath,
				PoolSize: poolSize,
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
			r.base.logger.Info("Discovered transcriber model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath),
				zap.Strings("variants", variantIDs))
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("Transcriber model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a transcriber model from disk. Called by BaseRegistry.loadModel
// under the write lock with double-check-after-lock already handled.
func (r *TranscriberRegistry) loadModel(info *TranscriberModelInfo) (transcribing.Transcriber, error) {
	cfg := &transcribing.PooledTranscriberConfig{
		ModelPath: info.Path,
		PoolSize:  info.PoolSize,
		Logger:    r.base.logger.Named(info.Name),
	}
	model, backendUsed, err := transcribing.NewPooledTranscriber(cfg, r.sessionManager, nil)
	if err != nil {
		return nil, fmt.Errorf("loading transcriber model %s: %w", info.Name, err)
	}

	r.base.logger.Info("Successfully loaded transcriber model",
		zap.String("name", info.Name),
		zap.String("backend", string(backendUsed)),
		zap.Int("poolSize", info.PoolSize))

	return model, nil
}

// Acquire returns a transcriber by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *TranscriberRegistry) Acquire(modelName string) (transcribing.Transcriber, error) {
	return r.base.acquire(modelName)
}

// Release decrements the reference count for a model.
// Must be called after Acquire() when the caller is done using the transcriber.
func (r *TranscriberRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// Get returns a transcriber by name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *TranscriberRegistry) Get(modelName string) (transcribing.Transcriber, error) {
	return r.base.get(modelName)
}

// List returns all available transcriber model names (discovered, not necessarily loaded).
func (r *TranscriberRegistry) List() []string               { return r.base.list() }
func (r *TranscriberRegistry) ListLoaded() []string         { return r.base.listLoaded() }
func (r *TranscriberRegistry) IsLoaded(name string) bool    { return r.base.isLoaded(name) }
func (r *TranscriberRegistry) Preload(names []string) error { return r.base.preload(names) }
func (r *TranscriberRegistry) PreloadAll() error            { return r.base.preloadAll() }
func (r *TranscriberRegistry) Close() error                 { return r.base.close() }
