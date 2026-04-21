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
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/reading"
	"go.uber.org/zap"
)

// ReaderModelEntry holds metadata about a discovered reader model (not loaded yet).
// Distinct from the OpenAPI-generated ReaderModelEntry used in API responses.
type ReaderModelEntry struct {
	Name         string
	Path         string
	PoolSize     int
	Capabilities []string
}

// ReaderRegistry manages reader models with lazy loading and TTL-based unloading
type ReaderRegistry struct {
	base           *BaseRegistry[ReaderModelEntry, reading.Reader]
	modelsDir      string
	sessionManager *backends.SessionManager
	poolSize       int
}

// ReaderConfig configures the reader registry
type ReaderConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
	PoolSize        int           // Number of concurrent pipelines per model (0 = default)
}

// NewReaderRegistry creates a new lazy-loading reader registry
func NewReaderRegistry(
	config ReaderConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*ReaderRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	r := &ReaderRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
		poolSize:       poolSize,
	}

	r.base = newBaseRegistry(BaseRegistryConfig[ReaderModelEntry, reading.Reader]{
		ModelType:       "reader",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *ReaderModelEntry) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         func(m reading.Reader) error { return m.Close() },
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		return nil, err
	}

	logger.Info("Lazy reader registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all reader models in the models directory without loading them
func (r *ReaderRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No reader models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("Reader models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeReader, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering reader models: %w", err)
	}

	poolSize := r.poolSize

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()
		variants := dm.Variants

		// Models without standard model.onnx variants (e.g. PaddleOCR, Florence-2)
		// are still valid as long as they contain at least one .onnx file.
		if len(variants) == 0 && !modelregistry.HasAnyONNXFiles(modelPath) {
			continue
		}

		if len(variants) == 0 {
			variants = map[string]string{"": ""}
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

			var caps []string
			if dm.Manifest != nil {
				caps = dm.Manifest.Capabilities
			}

			r.base.discovered[registryName] = &ReaderModelEntry{
				Name:         registryName,
				Path:         modelPath,
				PoolSize:     poolSize,
				Capabilities: caps,
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
			r.base.logger.Info("Discovered reader model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath),
				zap.Strings("variants", variantIDs))
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("Reader model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a reader model from disk. Called by BaseRegistry.loadModel.
// Multi-stage OCR models (Surya, PaddleOCR) are dispatched to MultiStageReader,
// while Vision2Seq models (TrOCR, Donut, Florence-2, Nougat, Pix2Struct) use PooledReader.
func (r *ReaderRegistry) loadModel(info *ReaderModelEntry) (reading.Reader, error) {
	r.base.logger.Info("Loading reader model on demand",
		zap.String("model", info.Name),
		zap.String("path", info.Path))

	var model reading.Reader
	var backendUsed backends.BackendType
	var err error

	if pipelines.IsMultiStageModel(info.Path) {
		r.base.logger.Info("Detected multi-stage OCR model",
			zap.String("model", info.Name))

		cfg := &reading.MultiStageReaderConfig{
			ModelPath: info.Path,
			Logger:    r.base.logger.Named(info.Name),
		}
		model, backendUsed, err = reading.NewMultiStageReader(cfg, r.sessionManager, nil)
		if err != nil {
			return nil, fmt.Errorf("loading multi-stage reader model %s: %w", info.Name, err)
		}
	} else {
		cfg := &reading.PooledReaderConfig{
			ModelPath: info.Path,
			PoolSize:  info.PoolSize,
			Logger:    r.base.logger.Named(info.Name),
		}
		model, backendUsed, err = reading.NewPooledReader(cfg, r.sessionManager, nil)
		if err != nil {
			return nil, fmt.Errorf("loading reader model %s: %w", info.Name, err)
		}
	}

	r.base.logger.Info("Successfully loaded reader model",
		zap.String("name", info.Name),
		zap.String("backend", string(backendUsed)))

	return model, nil
}

// Acquire returns a reader by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *ReaderRegistry) Acquire(modelName string) (reading.Reader, error) {
	return r.base.acquire(modelName)
}

// Release decrements the reference count for a model.
func (r *ReaderRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// Get returns a reader by name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *ReaderRegistry) Get(modelName string) (reading.Reader, error) {
	return r.base.get(modelName)
}

// ListWithCapabilities returns a map of model name to capabilities for all discovered models.
func (r *ReaderRegistry) ListWithCapabilities() map[string][]string {
	r.base.mu.RLock()
	defer r.base.mu.RUnlock()

	result := make(map[string][]string, len(r.base.discovered))
	for name, info := range r.base.discovered {
		result[name] = info.Capabilities
	}
	return result
}

func (r *ReaderRegistry) List() []string               { return r.base.list() }
func (r *ReaderRegistry) ListLoaded() []string         { return r.base.listLoaded() }
func (r *ReaderRegistry) IsLoaded(name string) bool    { return r.base.isLoaded(name) }
func (r *ReaderRegistry) Preload(names []string) error { return r.base.preload(names) }
func (r *ReaderRegistry) PreloadAll() error            { return r.base.preloadAll() }
func (r *ReaderRegistry) Close() error                 { return r.base.close() }
