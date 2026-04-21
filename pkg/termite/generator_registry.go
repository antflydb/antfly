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
	"path/filepath"
	"time"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/generation"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"go.uber.org/zap"
)

// GeneratorModelInfo holds metadata about a discovered generator model (not loaded yet)
type GeneratorModelInfo struct {
	Name      string
	Path      string // Path to base variant
	ModelType string
	Variants  map[string]string // variant name -> path (e.g., "i4" -> "/path/to/model/i4")
}

// GeneratorRegistry manages generator models with lazy loading and TTL-based unloading
type GeneratorRegistry struct {
	base           *BaseRegistry[GeneratorModelInfo, generation.Generator]
	modelsDir      string
	sessionManager *backends.SessionManager
}

// GeneratorConfig configures the generator registry
type GeneratorConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
}

// NewGeneratorRegistry creates a new lazy-loading generator registry
func NewGeneratorRegistry(
	config GeneratorConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*GeneratorRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	r := &GeneratorRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
	}

	r.base = newBaseRegistry(BaseRegistryConfig[GeneratorModelInfo, generation.Generator]{
		ModelType:       "generator",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *GeneratorModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn: func(m generation.Generator) error {
			if closer, ok := m.(interface{ Close() error }); ok {
				return closer.Close()
			}
			return nil
		},
		DiscoverFn: func() error { return r.discoverModels() },
		Budget:     budget,
		Logger:     logger,
	})

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		return nil, err
	}

	logger.Info("Lazy generator registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all generator models in the models directory without loading them
func (r *GeneratorRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No generator models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("Generator models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeGenerator, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering generator models: %w", err)
	}

	// Known variant subdirectory names for generators
	knownVariants := []string{"i4", "i4-cuda", "i4-dml"}

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()

		// Skip if already discovered
		if _, exists := r.base.discovered[registryFullName]; exists {
			continue
		}

		// Check for genai_config.json (preferred) or model.onnx in root or onnx/ subdirectory
		if !isValidGeneratorModel(modelPath) {
			// Also check onnx/ subdirectory
			onnxSubpath := filepath.Join(modelPath, "onnx")
			if isValidGeneratorModel(onnxSubpath) {
				modelPath = onnxSubpath
			} else {
				r.base.logger.Debug("Skipping directory - not a valid generator model",
					zap.String("dir", registryFullName))
				continue
			}
		}

		// Discover available variant subdirectories
		variants := make(map[string]string)
		for _, variantName := range knownVariants {
			variantPath := filepath.Join(dm.Path, variantName)
			if isValidGeneratorModel(variantPath) {
				variants[variantName] = variantPath
				r.base.logger.Debug("Found generator variant",
					zap.String("model", registryFullName),
					zap.String("variant", variantName),
					zap.String("path", variantPath))
			}
		}

		r.base.logger.Info("Discovered generator model (not loaded)",
			zap.String("name", registryFullName),
			zap.String("path", modelPath),
			zap.Int("variants", len(variants)))

		r.base.discovered[registryFullName] = &GeneratorModelInfo{
			Name:     registryFullName,
			Path:     modelPath,
			Variants: variants,
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("Generator model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a generator model from disk (base variant). Called by BaseRegistry.loadModel
// which already holds r.base.mu and handles budget/cache — do NOT lock, reserve budget, or
// set cache here.
func (r *GeneratorRegistry) loadModel(info *GeneratorModelInfo) (generation.Generator, error) {
	return r.loadGeneratorFromPath(info.Name, info.Path)
}

// loadModelWithBudget loads a generator model from a specific path, reserving a budget
// slot. Used by variant paths (AcquireWithVariant/GetWithVariant) that bypass BaseRegistry.loadModel.
func (r *GeneratorRegistry) loadModelWithBudget(cacheKey, modelPath string) (generation.Generator, error) {
	if r.base.budget != nil {
		if err := r.base.budget.Reserve(); err != nil {
			return nil, fmt.Errorf("loading generator model %s: %w", cacheKey, err)
		}
	}

	r.base.mu.Lock()
	defer r.base.mu.Unlock()

	// Double-check cache after acquiring lock
	if item := r.base.cache.Get(cacheKey); item != nil {
		if r.base.budget != nil {
			r.base.budget.Release()
		}
		return item.Value(), nil
	}

	model, err := r.loadGeneratorFromPath(cacheKey, modelPath)
	if err != nil {
		if r.base.budget != nil {
			r.base.budget.Release()
		}
		return nil, err
	}

	r.base.cache.Set(cacheKey, model, r.base.keepAlive)
	return model, nil
}

// loadGeneratorFromPath loads a generator model from a specific path.
// Caller must hold r.base.mu. Does NOT manage budget or cache — caller is responsible.
func (r *GeneratorRegistry) loadGeneratorFromPath(cacheKey, modelPath string) (generation.Generator, error) {
	r.base.logger.Info("Loading generator model on demand",
		zap.String("cacheKey", cacheKey),
		zap.String("path", modelPath))

	// Ensure chat_template.jinja exists for chat template rendering.
	ensureGeneratorPrereqs(modelPath, r.base.logger)

	model, backendUsed, loadErr := generation.LoadGenerator(
		modelPath,
		1, // Use single pipeline, registry manages caching
		r.base.logger.Named(cacheKey),
		r.sessionManager,
		[]string{"onnx"}, // Generative models currently only support ONNX
	)

	if loadErr != nil {
		return nil, fmt.Errorf("loading generator model %s: %w", cacheKey, loadErr)
	}

	r.base.logger.Info("Successfully loaded generator model",
		zap.String("cacheKey", cacheKey),
		zap.String("backend", string(backendUsed)))

	return model, nil
}

// Get returns a generator by name, loading it if necessary.
// DEPRECATED: Use Acquire() instead for long-running operations.
func (r *GeneratorRegistry) Get(modelName string) (generation.Generator, error) {
	return r.base.get(modelName)
}

// Acquire returns a generator by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
func (r *GeneratorRegistry) Acquire(modelName string) (generation.Generator, error) {
	return r.base.acquire(modelName)
}

// Release decrements the reference count for a model.
func (r *GeneratorRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// AcquireWithVariant returns a generator by name with a specific variant
// and increments its reference count.
// The caller MUST call ReleaseWithVariant() when done.
func (r *GeneratorRegistry) AcquireWithVariant(modelName, variant string) (generation.Generator, error) {
	// Resolve base model name inline so the ref key matches the cache key.
	r.base.mu.RLock()
	info, ok := r.base.discovered[modelName]
	resolvedBase := modelName
	r.base.mu.RUnlock()

	if !ok {
		if err := r.discoverModels(); err != nil {
			r.base.logger.Debug("Generator re-discovery failed", zap.Error(err))
		}
		r.base.mu.RLock()
		var resolved string
		info, resolved, ok = resolveVariant(modelName, r.base.discovered)
		r.base.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("generator model not found: %s", modelName)
		}
		resolvedBase = resolved
		if resolved != modelName {
			r.base.logger.Info("Resolved model name to variant",
				zap.String("requested", modelName),
				zap.String("resolved", resolved))
		}
	}

	// Build cache key including variant
	cacheKey := resolvedBase
	if variant != "" {
		cacheKey = resolvedBase + ":" + variant
	}

	// Determine path based on variant
	modelPath := info.Path
	if variant != "" {
		variantPath, found := info.Variants[variant]
		if !found {
			return nil, fmt.Errorf("variant %q not found for model %s (available: %v)",
				variant, modelName, r.ListVariants(modelName))
		}
		modelPath = variantPath
	}

	r.base.refs.incRef(cacheKey)

	gen, err := r.loadModelWithBudget(cacheKey, modelPath)
	if err != nil {
		r.base.refs.rollbackRef(cacheKey)
		return nil, err
	}

	r.base.logger.Debug("Acquired generator model with variant",
		zap.String("model", cacheKey))

	return gen, nil
}

// ReleaseWithVariant decrements the reference count for a model variant.
func (r *GeneratorRegistry) ReleaseWithVariant(modelName, variant string) {
	r.base.mu.RLock()
	resolved := resolveRefName(modelName, r.base.discovered)
	r.base.mu.RUnlock()

	cacheKey := resolved
	if variant != "" {
		cacheKey = resolved + ":" + variant
	}

	count, orphans := r.base.refs.releaseRef(cacheKey)

	r.base.logger.Debug("Released generator model with variant",
		zap.String("model", cacheKey),
		zap.Int("refCount", count))

	closeOrphans(r.base.logger, "generator", cacheKey, orphans)
}

// GetWithVariant returns a generator by name with a specific variant, loading it if necessary.
func (r *GeneratorRegistry) GetWithVariant(modelName, variant string) (generation.Generator, error) {
	// Build cache key including variant
	cacheKey := modelName
	if variant != "" {
		cacheKey = modelName + ":" + variant
	}

	// Check cache first
	if item := r.base.cache.Get(cacheKey); item != nil {
		r.base.logger.Debug("Generator cache hit", zap.String("model", cacheKey))
		return item.Value(), nil
	}

	// Check if model is discovered
	r.base.mu.RLock()
	info, ok := r.base.discovered[modelName]
	r.base.mu.RUnlock()

	if !ok {
		if err := r.discoverModels(); err != nil {
			r.base.logger.Debug("Generator re-discovery failed", zap.Error(err))
		}
		r.base.mu.RLock()
		var resolved string
		info, resolved, ok = resolveVariant(modelName, r.base.discovered)
		r.base.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("generator model not found: %s", modelName)
		}
		if resolved != modelName {
			r.base.logger.Info("Resolved model name to variant",
				zap.String("requested", modelName),
				zap.String("resolved", resolved))
		}
	}

	// Determine path based on variant
	modelPath := info.Path
	if variant != "" {
		variantPath, ok := info.Variants[variant]
		if !ok {
			return nil, fmt.Errorf("variant %q not found for model %s (available: %v)",
				variant, modelName, r.ListVariants(modelName))
		}
		modelPath = variantPath
	}

	return r.loadModelWithBudget(cacheKey, modelPath)
}

// ListVariants returns the available variant names for a model
func (r *GeneratorRegistry) ListVariants(modelName string) []string {
	r.base.mu.RLock()
	defer r.base.mu.RUnlock()

	info, ok := r.base.discovered[modelName]
	if !ok {
		return nil
	}

	variants := make([]string, 0, len(info.Variants))
	for name := range info.Variants {
		variants = append(variants, name)
	}
	return variants
}

func (r *GeneratorRegistry) List() []string               { return r.base.list() }
func (r *GeneratorRegistry) ListLoaded() []string         { return r.base.listLoaded() }
func (r *GeneratorRegistry) IsLoaded(name string) bool    { return r.base.isLoaded(name) }
func (r *GeneratorRegistry) Preload(names []string) error { return r.base.preload(names) }
func (r *GeneratorRegistry) PreloadAll() error            { return r.base.preloadAll() }
func (r *GeneratorRegistry) Close() error                 { return r.base.close() }
