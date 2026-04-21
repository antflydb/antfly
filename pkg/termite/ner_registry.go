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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/antflydb/antfly/pkg/termite/lib/ner"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"go.uber.org/zap"
)

// NERModelType indicates the type of NER model
type NERModelType int

const (
	NERModelTypeStandard NERModelType = iota
	NERModelTypeGLiNER
	NERModelTypeREBEL
)

// NERModelInfo holds metadata about a discovered NER model (not loaded yet)
type NERModelInfo struct {
	Name         string
	Path         string
	OnnxFilename string
	PoolSize     int
	ModelType    NERModelType
	Quantized    bool
	Capabilities []string
}

// loadedNERModel wraps both Model and optional Recognizer interfaces
type loadedNERModel struct {
	model        ner.Model
	recognizer   ner.Recognizer // May be nil for standard NER models
	modelType    NERModelType
	capabilities []string
}

// NERRegistry manages NER models with lazy loading and TTL-based unloading
type NERRegistry struct {
	base           *BaseRegistry[NERModelInfo, *loadedNERModel]
	modelsDir      string
	sessionManager *backends.SessionManager
	poolSize       int
}

// NERConfig configures the NER registry
type NERConfig struct {
	ModelsDir       string
	KeepAlive       time.Duration // How long to keep models loaded (0 = forever)
	MaxLoadedModels uint64        // Max models in memory (0 = unlimited)
	PoolSize        int           // Number of concurrent pipelines per model (0 = default)
}

// NewNERRegistry creates a new lazy-loading NER registry
func NewNERRegistry(
	config NERConfig,
	sessionManager *backends.SessionManager,
	budget *ModelBudget,
	logger *zap.Logger,
) (*NERRegistry, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := config.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	r := &NERRegistry{
		modelsDir:      config.ModelsDir,
		sessionManager: sessionManager,
		poolSize:       poolSize,
	}

	r.base = newBaseRegistry(BaseRegistryConfig[NERModelInfo, *loadedNERModel]{
		ModelType:       "NER",
		KeepAlive:       config.KeepAlive,
		MaxLoadedModels: config.MaxLoadedModels,
		NameFunc:        func(info *NERModelInfo) string { return info.Name },
		LoadFn:          r.loadModel,
		CloseFn:         func(m *loadedNERModel) error { return m.model.Close() },
		DiscoverFn:      func() error { return r.discoverModels() },
		Budget:          budget,
		Logger:          logger,
	})

	if err := r.discoverModels(); err != nil {
		r.base.cache.Stop()
		return nil, err
	}

	logger.Info("Lazy NER registry initialized",
		zap.Int("models_discovered", len(r.base.discovered)),
		zap.Duration("keep_alive", r.base.keepAlive),
		zap.Uint64("max_loaded_models", config.MaxLoadedModels))

	return r, nil
}

// discoverModels finds all NER models in the models directory without loading them
func (r *NERRegistry) discoverModels() error {
	if r.modelsDir == "" {
		r.base.logger.Info("No NER models directory configured")
		return nil
	}

	if _, err := os.Stat(r.modelsDir); os.IsNotExist(err) {
		r.base.logger.Warn("NER models directory does not exist",
			zap.String("dir", r.modelsDir))
		return nil
	}

	discovered, err := modelregistry.DiscoverModelsInDir(r.modelsDir, modelregistry.ModelTypeRecognizer, zapLogf(r.base.logger))
	if err != nil {
		return fmt.Errorf("discovering NER models: %w", err)
	}

	poolSize := r.poolSize

	r.base.mu.Lock()
	for _, dm := range discovered {
		modelPath := dm.Path
		registryFullName := dm.FullName()

		isGLiNER := pipelines.IsGLiNERModel(modelPath)
		isREBEL := ner.IsREBELModel(modelPath)

		if isREBEL {
			if _, exists := r.base.discovered[registryFullName]; exists {
				continue
			}

			r.base.logger.Info("Discovered REBEL model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath))

			caps := []string{string(modelregistry.CapabilityRelations), string(modelregistry.CapabilityZeroshot), string(modelregistry.CapabilityExtraction)}
			manifestPath := filepath.Join(modelPath, "manifest.json")
			if data, err := os.ReadFile(manifestPath); err == nil {
				var manifest modelregistry.ModelManifest
				if err := json.Unmarshal(data, &manifest); err == nil && len(manifest.Capabilities) > 0 {
					caps = manifest.Capabilities
				}
			}

			r.base.discovered[registryFullName] = &NERModelInfo{
				Name:         registryFullName,
				Path:         modelPath,
				PoolSize:     poolSize,
				ModelType:    NERModelTypeREBEL,
				Capabilities: caps,
			}
		} else if isGLiNER {
			if _, exists := r.base.discovered[registryFullName]; exists {
				continue
			}

			quantized := false
			if _, err := os.Stat(filepath.Join(modelPath, "model_quantized.onnx")); err == nil {
				quantized = true
			} else if _, err := os.Stat(filepath.Join(modelPath, "model.onnx")); err != nil {
				r.base.logger.Debug("Skipping GLiNER directory without model files",
					zap.String("dir", registryFullName))
				continue
			}

			r.base.logger.Info("Discovered GLiNER model (not loaded)",
				zap.String("name", registryFullName),
				zap.String("path", modelPath))

			caps := []string{string(modelregistry.CapabilityLabels), string(modelregistry.CapabilityZeroshot)}
			manifestPath := filepath.Join(modelPath, "manifest.json")
			if data, err := os.ReadFile(manifestPath); err == nil {
				var manifest modelregistry.ModelManifest
				if err := json.Unmarshal(data, &manifest); err == nil && len(manifest.Capabilities) > 0 {
					caps = manifest.Capabilities
				}
			}

			glinerConfigPath := filepath.Join(modelPath, "gliner_config.json")
			if data, err := os.ReadFile(glinerConfigPath); err == nil {
				var glinerConfig struct {
					Capabilities []string `json:"capabilities"`
				}
				if err := json.Unmarshal(data, &glinerConfig); err == nil && len(glinerConfig.Capabilities) > 0 {
					for _, cap := range glinerConfig.Capabilities {
						if !slices.Contains(caps, cap) {
							caps = append(caps, cap)
						}
					}
				}
			}

			r.base.discovered[registryFullName] = &NERModelInfo{
				Name:         registryFullName,
				Path:         modelPath,
				PoolSize:     poolSize,
				ModelType:    NERModelTypeGLiNER,
				Quantized:    quantized,
				Capabilities: caps,
			}
		} else {
			variants := dm.Variants
			if len(variants) == 0 {
				continue
			}

			caps := []string{string(modelregistry.CapabilityLabels)}
			manifestPath := filepath.Join(modelPath, "manifest.json")
			if data, err := os.ReadFile(manifestPath); err == nil {
				var manifest modelregistry.ModelManifest
				if err := json.Unmarshal(data, &manifest); err == nil && len(manifest.Capabilities) > 0 {
					caps = manifest.Capabilities
				}
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

				r.base.discovered[registryName] = &NERModelInfo{
					Name:         registryName,
					Path:         modelPath,
					OnnxFilename: onnxFilename,
					PoolSize:     poolSize,
					ModelType:    NERModelTypeStandard,
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
				r.base.logger.Info("Discovered NER model (not loaded)",
					zap.String("name", registryFullName),
					zap.String("path", modelPath),
					zap.Strings("variants", variantIDs))
			}
		}
	}
	discoveredCount := len(r.base.discovered)
	r.base.mu.Unlock()

	r.base.logger.Info("NER model discovery complete",
		zap.Int("models_discovered", discoveredCount))

	return nil
}

// loadModel loads a NER model from disk. Called by BaseRegistry.loadModel.
func (r *NERRegistry) loadModel(info *NERModelInfo) (*loadedNERModel, error) {
	r.base.logger.Info("Loading NER model on demand",
		zap.String("model", info.Name),
		zap.String("path", info.Path),
		zap.Int("modelType", int(info.ModelType)))

	var loaded *loadedNERModel

	switch info.ModelType {
	case NERModelTypeREBEL:
		cfg := ner.PooledREBELConfig{
			ModelPath:     info.Path,
			PoolSize:      info.PoolSize,
			ModelBackends: nil,
			Logger:        r.base.logger.Named(info.Name),
		}
		model, backendUsed, err := ner.NewPooledREBEL(cfg, r.sessionManager)
		if err != nil {
			return nil, fmt.Errorf("loading REBEL model %s: %w", info.Name, err)
		}
		r.base.logger.Info("Successfully loaded REBEL model",
			zap.String("name", info.Name),
			zap.String("backend", string(backendUsed)),
			zap.Int("poolSize", info.PoolSize),
			zap.Strings("capabilities", info.Capabilities))
		loaded = &loadedNERModel{
			model:        model,
			recognizer:   model,
			modelType:    NERModelTypeREBEL,
			capabilities: info.Capabilities,
		}

	case NERModelTypeGLiNER:
		cfg := ner.PooledGLiNERConfig{
			ModelPath:     info.Path,
			PoolSize:      info.PoolSize,
			Quantized:     info.Quantized,
			ModelBackends: nil,
			Logger:        r.base.logger.Named(info.Name),
		}
		model, backendUsed, err := ner.NewPooledGLiNER(cfg, r.sessionManager)
		if err != nil {
			return nil, fmt.Errorf("loading GLiNER model %s: %w", info.Name, err)
		}
		// Use the pipeline's authoritative capabilities, supplemented by manifest extras.
		caps := slices.Concat(
			[]string{string(modelregistry.CapabilityLabels), string(modelregistry.CapabilityZeroshot)},
			model.Capabilities(),
			info.Capabilities,
		)
		caps = slices.Compact(slices.Sorted(slices.Values(caps)))
		r.base.logger.Info("Successfully loaded GLiNER model",
			zap.String("name", info.Name),
			zap.Bool("quantized", info.Quantized),
			zap.String("backend", string(backendUsed)),
			zap.Int("poolSize", info.PoolSize),
			zap.Strings("default_labels", model.Labels()),
			zap.Strings("capabilities", caps))
		loaded = &loadedNERModel{
			model:        model,
			recognizer:   model,
			modelType:    NERModelTypeGLiNER,
			capabilities: caps,
		}

	default: // NERModelTypeStandard
		cfg := ner.PooledNERConfig{
			ModelPath:     info.Path,
			PoolSize:      info.PoolSize,
			ModelBackends: nil,
			Logger:        r.base.logger.Named(info.Name),
		}
		model, backendUsed, err := ner.NewPooledNER(cfg, r.sessionManager)
		if err != nil {
			return nil, fmt.Errorf("loading NER model %s: %w", info.Name, err)
		}
		r.base.logger.Info("Successfully loaded NER model",
			zap.String("name", info.Name),
			zap.String("backend", string(backendUsed)),
			zap.Int("poolSize", info.PoolSize),
			zap.Strings("capabilities", info.Capabilities))
		loaded = &loadedNERModel{
			model:        model,
			recognizer:   nil,
			modelType:    NERModelTypeStandard,
			capabilities: info.Capabilities,
		}
	}

	return loaded, nil
}

// Get returns a NER model by name, loading it if necessary.
// Returns ner.Recognizer when available (GLiNER, REBEL), otherwise ner.Model.
func (r *NERRegistry) Get(modelName string) (ner.Model, error) {
	loaded, err := r.base.get(modelName)
	if err != nil {
		return nil, err
	}
	if loaded.recognizer != nil {
		return loaded.recognizer, nil
	}
	return loaded.model, nil
}

// Acquire returns a NER model by name and increments its reference count.
// The caller MUST call Release() when done to allow the model to be evicted.
// Type-assert to ner.Recognizer if HasCapability returns true for CapabilityZeroshot.
func (r *NERRegistry) Acquire(modelName string) (ner.Model, error) {
	loaded, err := r.base.acquire(modelName)
	if err != nil {
		return nil, err
	}
	// Return recognizer if available (it embeds ner.Model), otherwise return model
	if loaded.recognizer != nil {
		return loaded.recognizer, nil
	}
	return loaded.model, nil
}

// Release decrements the reference count for a model.
func (r *NERRegistry) Release(modelName string) {
	r.base.release(modelName)
}

// List returns all available NER model names (discovered, not necessarily loaded).
// Re-scans the models directory to pick up newly pulled models.
func (r *NERRegistry) List() map[string][]string {
	if r.base.discoverFn != nil {
		_ = r.base.discoverFn()
	}

	r.base.mu.RLock()
	defer r.base.mu.RUnlock()

	result := make(map[string][]string, len(r.base.discovered))
	for name, info := range r.base.discovered {
		capsCopy := make([]string, len(info.Capabilities))
		copy(capsCopy, info.Capabilities)
		result[name] = capsCopy
	}
	return result
}

// GetCapabilities returns the capabilities for a specific model.
func (r *NERRegistry) GetCapabilities(modelName string) []string {
	r.base.mu.RLock()
	info, ok := r.base.discovered[modelName]
	r.base.mu.RUnlock()

	if !ok {
		return nil
	}
	return info.Capabilities
}

// HasCapability checks if a model has a specific capability.
func (r *NERRegistry) HasCapability(modelName string, capability modelregistry.Capability) bool {
	caps := r.GetCapabilities(modelName)
	return slices.Contains(caps, string(capability))
}

func (r *NERRegistry) ListLoaded() []string         { return r.base.listLoaded() }
func (r *NERRegistry) IsLoaded(name string) bool    { return r.base.isLoaded(name) }
func (r *NERRegistry) Preload(names []string) error { return r.base.preload(names) }

// PreloadAll loads all discovered models (for eager loading mode)
func (r *NERRegistry) PreloadAll() error {
	models := r.List()
	names := make([]string, 0, len(models))
	for name := range models {
		names = append(names, name)
	}
	return r.base.preload(names)
}

func (r *NERRegistry) Close() error { return r.base.close() }
