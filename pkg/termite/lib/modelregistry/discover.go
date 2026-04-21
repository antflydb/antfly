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

package modelregistry

import (
	"fmt"
	"os"
	"path/filepath"
)

// DiscoveredModel represents a model found during directory scanning.
type DiscoveredModel struct {
	// Ref is the parsed model reference (owner/name).
	Ref ModelRef
	// Path is the absolute path to the model directory.
	Path string
	// Manifest is the loaded manifest (nil if not found).
	Manifest *ModelManifest
	// Variants maps variant ID to ONNX filename.
	Variants map[string]string
}

// RegistryName returns the short name for registry lookups.
func (d *DiscoveredModel) RegistryName() string {
	return d.Ref.Name
}

// FullName returns the full owner/name.
func (d *DiscoveredModel) FullName() string {
	return d.Ref.FullName()
}

// MultimodalCaps describes which multimodal capabilities a model directory has,
// for both standard and quantized variants.
type MultimodalCaps struct {
	HasImage          bool
	HasAudio          bool
	HasImageQuantized bool
	HasAudioQuantized bool
}

// DetectMultimodalCapabilities checks a model directory for visual and audio encoder files.
// Returns which modalities are available for both standard and quantized variants.
// Also checks variant filenames (e.g., visual_model_i8.onnx) and the onnx/ subdirectory
// for HuggingFace transformers.js models.
func DetectMultimodalCapabilities(modelPath string) MultimodalCaps {
	var caps MultimodalCaps
	onnxDir := filepath.Join(modelPath, "onnx")

	// Check for visual encoder (standard + variants, root + onnx/)
	caps.HasImage = fileExistsStemAny(modelPath, "visual_model") ||
		fileExistsStemAny(onnxDir, "visual_model")
	caps.HasImageQuantized = fileExists(filepath.Join(modelPath, "visual_model_quantized.onnx"))

	// Check for audio encoder (standard + variants, root + onnx/)
	caps.HasAudio = fileExistsStemAny(modelPath, "audio_model") ||
		fileExistsStemAny(onnxDir, "audio_model")
	caps.HasAudioQuantized = fileExists(filepath.Join(modelPath, "audio_model_quantized.onnx")) ||
		fileExists(filepath.Join(onnxDir, "audio_model_quantized.onnx"))

	return caps
}

// fileExistsStemAny checks if the base model or any variant of a model stem exists.
// e.g., for stem "visual_model", checks visual_model.onnx, visual_model_f16.onnx, etc.
func fileExistsStemAny(dir, stem string) bool {
	if fileExists(filepath.Join(dir, stem+".onnx")) {
		return true
	}
	for _, suffix := range VariantSuffixes {
		if fileExists(filepath.Join(dir, stem+"_"+suffix+".onnx")) {
			return true
		}
	}
	return false
}

// DiscoverModelsInDir scans a directory for models using the owner/model structure.
//
// Structure: modelsDir/owner/model-name/model.onnx
//
// The logger parameter is optional; pass nil to suppress log output.
func DiscoverModelsInDir(modelsDir string, modelType ModelType, logf func(string, ...any)) ([]DiscoveredModel, error) {
	if modelsDir == "" {
		return nil, nil
	}

	if _, err := os.Stat(modelsDir); os.IsNotExist(err) {
		return nil, nil
	}

	if logf == nil {
		logf = func(string, ...any) {}
	}

	var discovered []DiscoveredModel

	// First level: owner directories
	ownerEntries, err := os.ReadDir(modelsDir)
	if err != nil {
		return nil, fmt.Errorf("reading models directory: %w", err)
	}

	for _, ownerEntry := range ownerEntries {
		// Use os.Stat to follow symlinks (DirEntry.IsDir() returns false for symlinks)
		ownerPath := filepath.Join(modelsDir, ownerEntry.Name())
		ownerInfo, err := os.Stat(ownerPath)
		if err != nil || !ownerInfo.IsDir() {
			continue
		}

		owner := ownerEntry.Name()

		// Second level: model directories within owner
		modelEntries, err := os.ReadDir(ownerPath)
		if err != nil {
			logf("failed to read owner directory %s: %v", ownerPath, err)
			continue
		}

		for _, modelEntry := range modelEntries {
			modelPath := filepath.Join(ownerPath, modelEntry.Name())
			// Use os.Stat to follow symlinks
			modelInfo, err := os.Stat(modelPath)
			if err != nil || !modelInfo.IsDir() {
				continue
			}
			model := discoverSingleModel(modelPath, owner, modelEntry.Name(), modelType)
			if model != nil {
				discovered = append(discovered, *model)
			}
		}
	}

	return discovered, nil
}

// discoverSingleModel discovers a model from a directory.
func discoverSingleModel(modelPath, owner, name string, modelType ModelType) *DiscoveredModel {
	variants := DiscoverModelVariants(modelPath)

	// Try to load manifest first
	manifest, err := LoadManifestFromDir(modelPath)
	if err != nil {
		// Manifest not found or invalid - discover from files.
		// Accept any directory that contains at least one .onnx file;
		// architecture-specific file layouts are resolved at load time.
		if len(variants) == 0 && !HasAnyONNXFiles(modelPath) {
			return nil // No model files found
		}

		// Create a basic manifest from discovery
		manifest = &ModelManifest{
			SchemaVersion: CurrentSchemaVersion,
			Name:          name,
			Owner:         owner,
			Type:          modelType,
		}
		if owner != "" {
			manifest.Source = owner + "/" + name
		} else {
			manifest.Source = name
		}
	} else {
		// Override owner/name from directory structure if manifest is legacy
		if manifest.Owner == "" && owner != "" {
			manifest.Owner = owner
			manifest.Source = owner + "/" + manifest.Name
		}
	}

	return &DiscoveredModel{
		Ref: ModelRef{
			Owner: owner,
			Name:  name,
		},
		Path:     modelPath,
		Manifest: manifest,
		Variants: variants,
	}
}

// DiscoverModelVariants scans a model directory and returns a map of variant ID to ONNX filename.
// The default FP32 model (model.onnx) is returned with an empty string key.
func DiscoverModelVariants(modelPath string) map[string]string {
	variants := make(map[string]string)
	usedFilenames := make(map[string]bool)

	// Check for standard FP32 model
	if _, err := os.Stat(filepath.Join(modelPath, "model.onnx")); err == nil {
		variants[""] = "model.onnx" // Empty key = default/FP32
		usedFilenames["model.onnx"] = true
	} else if _, err := os.Stat(filepath.Join(modelPath, "encoder_model.onnx")); err == nil {
		// Encoder/decoder split models (e.g. Whisper, T5) don't have model.onnx
		// but are valid default variants loaded by their respective pipelines.
		variants[""] = "encoder_model.onnx"
		usedFilenames["encoder_model.onnx"] = true
	}

	// Check for all known variant files, but skip if filename already used
	for variantID, filename := range VariantFilenames {
		if usedFilenames[filename] {
			continue // Skip duplicates (e.g., f32 uses same model.onnx as default)
		}
		if _, err := os.Stat(filepath.Join(modelPath, filename)); err == nil {
			variants[variantID] = filename
			usedFilenames[filename] = true
		}
	}
	return variants
}

// HasAnyONNXFiles reports whether dir contains at least one .onnx file.
func HasAnyONNXFiles(dir string) bool {
	matches, _ := filepath.Glob(filepath.Join(dir, "*.onnx"))
	if len(matches) > 0 {
		return true
	}
	// Also check onnx/ subdirectory (HuggingFace optimum layout)
	matches, _ = filepath.Glob(filepath.Join(dir, "onnx", "*.onnx"))
	return len(matches) > 0
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
