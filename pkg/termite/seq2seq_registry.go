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
	"github.com/antflydb/antfly/pkg/termite/lib/seq2seq"
	"go.uber.org/zap"
)

// Seq2SeqModelInfo holds metadata about a discovered Seq2Seq model (not loaded yet)
type Seq2SeqModelInfo struct {
	Name string
	Path string
}

// Seq2SeqRegistry manages Seq2Seq models with lazy loading and TTL-based unloading
type Seq2SeqRegistry struct {
	base           *BaseRegistry[Seq2SeqModelInfo, seq2seq.Model]
	modelsDir      string
	sessionManager *backends.SessionManager
}

// Seq2SeqConfig configures the Seq2Seq registry
type Seq2SeqConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
}

// NewSeq2SeqRegistry creates a new lazy-loading Seq2Seq registry
func NewSeq2SeqRegistry(
	config Seq2SeqConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*Seq2SeqRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	r := &Seq2SeqRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
	}

	r.base = newBaseRegistry(BaseRegistryConfig[Seq2SeqModelInfo, seq2seq.Model]{
		ModelType:       "seq2seq",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *Seq2SeqModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         func(m seq2seq.Model) error { return m.Close() },
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		return nil, err
	}

	logger.Info("Lazy Seq2Seq registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all Seq2Seq models in the models directory without loading them
func (r *Seq2SeqRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No Seq2Seq models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("Seq2Seq models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeRewriter, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering Seq2Seq models: %w", err)
	}

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()

		if !seq2seq.IsSeq2SeqModel(modelPath) {
			r.base.logger.Debug("Skipping directory - not a Seq2Seq model",
				zap.String("dir", registryFullName))
			continue
		}

		if _, exists := r.base.discovered[registryFullName]; exists {
			continue
		}

		r.base.logger.Info("Discovered Seq2Seq model (not loaded)",
			zap.String("name", registryFullName),
			zap.String("path", modelPath))

		r.base.discovered[registryFullName] = &Seq2SeqModelInfo{
			Name: registryFullName,
			Path: modelPath,
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("Seq2Seq model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a Seq2Seq model from disk. Called by BaseRegistry.loadModel
// under the write lock with double-check-after-lock already handled.
func (r *Seq2SeqRegistry) loadModel(info *Seq2SeqModelInfo) (seq2seq.Model, error) {
	cfg := seq2seq.PooledSeq2SeqConfig{
		ModelPath:     info.Path,
		PoolSize:      1,   // Registry manages pooling at a higher level
		ModelBackends: nil, // Use all available backends
		Logger:        r.base.logger.Named(info.Name),
	}
	model, backendUsed, err := seq2seq.NewPooledSeq2Seq(cfg, r.sessionManager)
	if err != nil {
		return nil, fmt.Errorf("loading Seq2Seq model %s: %w", info.Name, err)
	}

	config := model.Config()
	r.base.logger.Info("Successfully loaded Seq2Seq model",
		zap.String("name", info.Name),
		zap.String("task", config.Task),
		zap.Int("max_length", config.MaxLength),
		zap.String("backend", string(backendUsed)))

	return model, nil
}

// Acquire returns a Seq2Seq model by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *Seq2SeqRegistry) Acquire(modelName string) (seq2seq.Model, error) {
	return r.base.acquire(modelName)
}

// Release decrements the reference count for a model.
// Must be called after Acquire() when the caller is done using the model.
func (r *Seq2SeqRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// Get returns a Seq2Seq model by name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *Seq2SeqRegistry) Get(modelName string) (seq2seq.Model, error) {
	return r.base.get(modelName)
}

// GetQuestionGenerator returns a Seq2Seq model as a QuestionGenerator by name
func (r *Seq2SeqRegistry) GetQuestionGenerator(modelName string) (seq2seq.QuestionGenerator, error) {
	model, err := r.Get(modelName)
	if err != nil {
		return nil, err
	}

	qg, ok := model.(seq2seq.QuestionGenerator)
	if !ok {
		return nil, fmt.Errorf("model %s does not support question generation", modelName)
	}
	return qg, nil
}

// List returns all available Seq2Seq model names (discovered, not necessarily loaded).
func (r *Seq2SeqRegistry) List() []string               { return r.base.list() }
func (r *Seq2SeqRegistry) ListLoaded() []string         { return r.base.listLoaded() }
func (r *Seq2SeqRegistry) IsLoaded(name string) bool    { return r.base.isLoaded(name) }
func (r *Seq2SeqRegistry) Preload(names []string) error { return r.base.preload(names) }
func (r *Seq2SeqRegistry) PreloadAll() error            { return r.base.preloadAll() }
func (r *Seq2SeqRegistry) Close() error                 { return r.base.close() }
