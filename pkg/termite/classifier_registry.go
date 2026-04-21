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
	"github.com/antflydb/antfly/pkg/termite/lib/classification"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"go.uber.org/zap"
)

// ClassifierModelInfo holds metadata about a discovered classifier model (not loaded yet)
type ClassifierModelInfo struct {
	Name         string
	Path         string
	OnnxFilename string
	PoolSize     int
}

// loadedClassifier wraps a loaded classifier
type loadedClassifier struct {
	classifier classification.Classifier
	config     classification.Config
}

// ClassifierRegistry manages zero-shot classification models with lazy loading and TTL-based unloading
type ClassifierRegistry struct {
	base           *BaseRegistry[ClassifierModelInfo, *loadedClassifier]
	modelsDir      string
	sessionManager *backends.SessionManager
	poolSize       int
}

// ClassifierConfig configures the classifier registry
type ClassifierConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
	PoolSize        int           // Number of concurrent pipelines per model (0 = default)
}

// NewClassifierRegistry creates a new lazy-loading classifier registry
func NewClassifierRegistry(
	config ClassifierConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*ClassifierRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	r := &ClassifierRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
		poolSize:       poolSize,
	}

	r.base = newBaseRegistry(BaseRegistryConfig[ClassifierModelInfo, *loadedClassifier]{
		ModelType:       "classifier",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *ClassifierModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         func(m *loadedClassifier) error { return m.classifier.Close() },
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		return nil, err
	}

	logger.Info("Lazy classifier registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all classifier models in the models directory without loading them
func (r *ClassifierRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No classifier models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("Classifier models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeClassifier, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering classifier models: %w", err)
	}

	poolSize := r.poolSize

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()

		if !classification.IsClassifierModel(modelPath) {
			r.base.logger.Debug("Skipping non-classifier model",
				zap.String("dir", registryFullName))
			continue
		}

		variants := dm.Variants
		if len(variants) == 0 {
			continue
		}

		anyNew := false
		for variantID, onnxFilename := range variants {
			registryName := registryFullName
			if variantID != "" {
				registryName = registryFullName + ":" + variantID
			}

			if _, exists := r.base.discovered[registryName]; exists {
				continue
			}

			r.base.discovered[registryName] = &ClassifierModelInfo{
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
			r.base.logger.Info("Discovered classifier model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath),
				zap.Strings("variants", variantIDs))
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("Classifier model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a classifier model from disk. Called by BaseRegistry.loadModel.
func (r *ClassifierRegistry) loadModel(info *ClassifierModelInfo) (*loadedClassifier, error) {
	cfg := classification.PooledClassifierConfig{
		ModelPath:     info.Path,
		PoolSize:      info.PoolSize,
		ModelBackends: nil, // Use all available backends
		Logger:        r.base.logger.Named(info.Name),
	}
	model, backendUsed, err := classification.NewPooledClassifier(cfg, r.sessionManager)
	if err != nil {
		return nil, fmt.Errorf("loading classifier model %s: %w", info.Name, err)
	}

	r.base.logger.Info("Successfully loaded classifier model",
		zap.String("name", info.Name),
		zap.String("backend", string(backendUsed)),
		zap.Int("poolSize", info.PoolSize))

	return &loadedClassifier{
		classifier: model,
		config:     model.Config(),
	}, nil
}

// Get returns a classifier model by name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *ClassifierRegistry) Get(modelName string) (classification.Classifier, error) {
	loaded, err := r.base.get(modelName)
	if err != nil {
		return nil, err
	}
	return loaded.classifier, nil
}

// Acquire returns a classifier by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *ClassifierRegistry) Acquire(modelName string) (classification.Classifier, error) {
	loaded, err := r.base.acquire(modelName)
	if err != nil {
		return nil, err
	}
	return loaded.classifier, nil
}

// Release decrements the reference count for a model.
func (r *ClassifierRegistry) Release(modelName string) {
	r.base.release(modelName)
}

func (r *ClassifierRegistry) List() []string               { return r.base.list() }
func (r *ClassifierRegistry) ListLoaded() []string         { return r.base.listLoaded() }
func (r *ClassifierRegistry) IsLoaded(name string) bool    { return r.base.isLoaded(name) }
func (r *ClassifierRegistry) Preload(names []string) error { return r.base.preload(names) }
func (r *ClassifierRegistry) Close() error                 { return r.base.close() }
