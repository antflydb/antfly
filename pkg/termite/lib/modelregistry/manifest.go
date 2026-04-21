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

// Package modelregistry provides functionality for pulling ONNX models from
// a remote registry (like Cloudflare R2) in an Ollama-style fashion.
package modelregistry

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// ModelType represents the type of model (embedder, chunker, reranker)
type ModelType string

const (
	ModelTypeEmbedder    ModelType = "embedder"
	ModelTypeChunker     ModelType = "chunker"
	ModelTypeReranker    ModelType = "reranker"
	ModelTypeGenerator   ModelType = "generator"
	ModelTypeRecognizer  ModelType = "recognizer"
	ModelTypeRewriter    ModelType = "rewriter"
	ModelTypeClassifier  ModelType = "classifier"
	ModelTypeReader      ModelType = "reader"
	ModelTypeTranscriber ModelType = "transcriber"
)

// AllModelTypes is the complete list of known model types.
var AllModelTypes = []ModelType{
	ModelTypeEmbedder,
	ModelTypeChunker,
	ModelTypeReranker,
	ModelTypeGenerator,
	ModelTypeRecognizer,
	ModelTypeRewriter,
	ModelTypeClassifier,
	ModelTypeReader,
	ModelTypeTranscriber,
}

// Capability represents a model capability
type Capability string

// Model capabilities
const (
	// CapabilityImage indicates the model can embed images (e.g., CLIP visual encoder)
	CapabilityImage Capability = "image"

	// CapabilityAudio indicates the model can embed audio (e.g., CLAP audio encoder)
	CapabilityAudio Capability = "audio"

	// Recognizer capabilities - describe what extraction tasks the model supports

	// CapabilityLabels indicates the model performs entity extraction (NER)
	// extracting labeled spans from text (e.g., PER, ORG, LOC)
	CapabilityLabels Capability = "labels"

	// CapabilityZeroshot indicates the model supports arbitrary labels at inference time
	// (e.g., GLiNER models that can extract any entity type without retraining)
	CapabilityZeroshot Capability = "zeroshot"

	// CapabilityRelations indicates the model supports relation extraction between entities
	// (e.g., GLiNER multitask models, REBEL)
	CapabilityRelations Capability = "relations"

	// CapabilityAnswers indicates the model supports extractive question answering
	// (e.g., GLiNER multitask models)
	CapabilityAnswers Capability = "answers"

	// CapabilityClassification indicates the model supports text classification
	// (e.g., GLiNER2 models)
	CapabilityClassification Capability = "classification"

	// Reader capabilities - describe what differentiates reader models at the API level.
	// All readers perform OCR (text extraction from images) as a baseline.

	// CapabilityDocumentQA indicates the model supports visual document question answering,
	// answering questions about the content of a document image (e.g., Donut, Pix2Struct).
	CapabilityDocumentQA Capability = "document_qa"

	// CapabilityRegions indicates the model returns spatial text regions with bounding boxes,
	// not just plain text output (e.g., PaddleOCR, Surya multi-stage pipelines).
	CapabilityRegions Capability = "regions"

	// CapabilityExtraction indicates the model supports structured extraction
	// (e.g., GLiNER2 models)
	CapabilityExtraction Capability = "extraction"

	// CapabilitySparse indicates the model generates sparse (SPLADE-style) embeddings
	// instead of dense embeddings. The model outputs vocab-dimension logits that are
	// activated with softplus and sparsified to top-k entries.
	CapabilitySparse Capability = "sparse"
)

// ParseModelType parses a string into a ModelType
func ParseModelType(s string) (ModelType, error) {
	switch strings.ToLower(s) {
	case "embedder", "embedders":
		return ModelTypeEmbedder, nil
	case "chunker", "chunkers":
		return ModelTypeChunker, nil
	case "reranker", "rerankers":
		return ModelTypeReranker, nil
	case "generator", "generators":
		return ModelTypeGenerator, nil
	case "recognizer", "recognizers":
		return ModelTypeRecognizer, nil
	case "rewriter", "rewriters":
		return ModelTypeRewriter, nil
	case "classifier", "classifiers":
		return ModelTypeClassifier, nil
	case "reader", "readers":
		return ModelTypeReader, nil
	case "transcriber", "transcribers":
		return ModelTypeTranscriber, nil
	default:
		return "", fmt.Errorf("unknown model type: %s (valid: embedder, chunker, reranker, generator, recognizer, rewriter, classifier, reader, transcriber)", s)
	}
}

// String returns the string representation of the model type
func (t ModelType) String() string {
	return string(t)
}

// DirName returns the directory name for this model type (plural form)
func (t ModelType) DirName() string {
	switch t {
	case ModelTypeEmbedder:
		return "embedders"
	case ModelTypeChunker:
		return "chunkers"
	case ModelTypeReranker:
		return "rerankers"
	case ModelTypeGenerator:
		return "generators"
	case ModelTypeRecognizer:
		return "recognizers"
	case ModelTypeRewriter:
		return "rewriters"
	case ModelTypeClassifier:
		return "classifiers"
	case ModelTypeReader:
		return "readers"
	case ModelTypeTranscriber:
		return "transcribers"
	default:
		return string(t) + "s"
	}
}

// ModelFile represents a single file in the model manifest
type ModelFile struct {
	// Name is the filename (e.g., "model.onnx", "tokenizer.json")
	Name string `json:"name"`
	// Digest is the SHA256 hash of the file (e.g., "sha256:abc123...")
	Digest string `json:"digest"`
	// Size is the file size in bytes
	Size int64 `json:"size"`
}

// ModelProvenance tracks model origin and download metadata
type ModelProvenance struct {
	// DownloadedFrom is the source: "registry", "huggingface", "local"
	DownloadedFrom string `json:"downloadedFrom"`
	// DownloadedAt is when the model was downloaded
	DownloadedAt time.Time `json:"downloadedAt"`
	// RegistryDigest is the manifest digest if from registry
	RegistryDigest string `json:"registryDigest,omitempty"`
	// HuggingFaceCommit is the HF commit hash if from HuggingFace
	HuggingFaceCommit string `json:"huggingfaceCommit,omitempty"`
}

// Variant identifiers for quantized/precision model variants
const (
	// VariantF32 is the default FP32 model (model.onnx)
	VariantF32 = "f32"
	// VariantF16 is FP16 half precision
	VariantF16 = "f16"
	// VariantBF16 is BFloat16 precision
	VariantBF16 = "bf16"
	// VariantI8 is INT8 dynamic quantization
	VariantI8 = "i8"
	// VariantI8Static is INT8 static quantization with calibration
	VariantI8Static = "i8-st"
	// VariantI4 is INT4 quantization
	VariantI4 = "i4"
)

// VariantSuffixes lists the non-default variant suffixes (excludes f32 which
// uses the base "model.onnx" filename). These are appended to model stems
// when checking for variant files (e.g., visual_model_i8.onnx).
var VariantSuffixes = []string{VariantF16, VariantBF16, VariantI8, VariantI8Static, VariantI4}

// VariantFilenames maps variant identifiers to their ONNX filenames
var VariantFilenames = map[string]string{
	VariantF32:      "model.onnx",
	VariantF16:      "model_f16.onnx",
	VariantBF16:     "model_bf16.onnx",
	VariantI8:       "model_i8.onnx",
	VariantI8Static: "model_i8-st.onnx",
	VariantI4:       "model_i4.onnx",
}

// FilenameToVariant maps ONNX filenames back to variant identifiers
var FilenameToVariant = map[string]string{
	"model.onnx":       VariantF32,
	"model_f16.onnx":   VariantF16,
	"model_bf16.onnx":  VariantBF16,
	"model_i8.onnx":    VariantI8,
	"model_i8-st.onnx": VariantI8Static,
	"model_i4.onnx":    VariantI4,
}

// isONNXFile reports whether name has an ONNX model or external-data suffix.
func isONNXFile(name string) bool {
	return strings.HasSuffix(name, ".onnx") ||
		strings.HasSuffix(name, ".onnx.data") ||
		strings.HasSuffix(name, ".onnx_data")
}

// onnxStem strips ONNX suffixes from a filename, returning the base stem.
func onnxStem(name string) string {
	for _, suffix := range []string{".onnx.data", ".onnx_data", ".onnx"} {
		if s := strings.TrimSuffix(name, suffix); s != name {
			return s
		}
	}
	return name
}

// VariantBaseStems returns the set of ONNX base stems (e.g., "visual_model") that
// have at least one non-f32 variant entry in the manifest. Files in manifest.Files
// whose stems are in this set are the f32 encoder files; they should only be
// downloaded/counted when f32 is explicitly requested.
//
// This assumes base model stems don't themselves end with a variant suffix
// (e.g., no model named "foo_i8.onnx" as its f32 base). Our export scripts
// and registry conventions guarantee this.
func (m *ModelManifest) VariantBaseStems() map[string]bool {
	stems := make(map[string]bool)
	for _, entry := range m.Variants {
		for _, vf := range entry.Files {
			stem := onnxStem(vf.Name)
			for _, suffix := range VariantSuffixes {
				if strings.HasSuffix(stem, "_"+suffix) {
					stems[strings.TrimSuffix(stem, "_"+suffix)] = true
					break
				}
			}
		}
	}
	return stems
}

// DownloadSize returns the total byte size of files that would be downloaded
// for the given set of variant IDs. It accounts for auxiliary ONNX files
// (always included), encoder ONNX files (only when f32 is requested), and
// non-f32 variant files. If variants is empty, defaults to f32.
func (m *ModelManifest) DownloadSize(variants []string) int64 {
	if len(variants) == 0 {
		variants = []string{VariantF32}
	}

	requested := make(map[string]bool, len(variants))
	for _, v := range variants {
		requested[v] = true
	}

	variantStems := m.VariantBaseStems()

	var total int64
	for _, f := range m.Files {
		if isONNXFile(f.Name) && variantStems[onnxStem(f.Name)] {
			if requested[VariantF32] {
				total += f.Size
			}
			continue
		}
		total += f.Size
	}

	for _, variantID := range variants {
		if variantID == VariantF32 {
			continue
		}
		if entry, ok := m.Variants[variantID]; ok {
			for _, vf := range entry.Files {
				total += vf.Size
			}
		}
	}
	return total
}

// CurrentSchemaVersion is the current manifest schema version
const CurrentSchemaVersion = 2

// CurrentIndexSchemaVersion is the current registry index schema version
const CurrentIndexSchemaVersion = 2

// ModelManifest describes an ONNX model and its files
type ModelManifest struct {
	// SchemaVersion is the manifest format version (1 = legacy, 2 = with owner/source)
	SchemaVersion int `json:"schemaVersion"`
	// Name is the model identifier (e.g., "bge-small-en-v1.5")
	Name string `json:"name"`
	// Source is the full owner/model identifier from HuggingFace (e.g., "BAAI/bge-small-en-v1.5")
	Source string `json:"source,omitempty"`
	// Owner is the namespace/organization (e.g., "BAAI", "sentence-transformers")
	Owner string `json:"owner,omitempty"`
	// Type is the model type (embedder, chunker, reranker)
	Type ModelType `json:"type"`
	// Description is a human-readable description
	Description string `json:"description,omitempty"`
	// Capabilities lists special capabilities of the model.
	// Valid values: "image" (CLIP), "audio" (CLAP), "labels", "zeroshot", "relations", "answers", "classification"
	Capabilities []string `json:"capabilities,omitempty"`
	// Files lists all required files for the model (includes model.onnx)
	Files []ModelFile `json:"files"`
	// Variants maps variant identifiers to their model files.
	// For single-model types (embedder, chunker, reranker), this is a single file.
	// For multimodal embedders (CLIP), this can be an array of files (visual + text).
	// Use VariantFiles() to get the slice of files for a variant.
	Variants map[string]VariantEntry `json:"variants,omitempty"`
	// Backends lists supported inference backends for this model.
	// Valid values: "onnx", "xla", "go"
	// If empty, all backends are supported (default).
	Backends []string `json:"backends,omitempty"`
	// SessionOptions declares backend-specific session configuration for this model.
	// Used by GoMLX-based backends to resolve ONNX If nodes statically and
	// to override fixed input dimensions that should be dynamic.
	SessionOptions *ManifestSessionOptions `json:"session_options,omitempty"`
	// Provenance tracks where/when the model was obtained
	Provenance *ModelProvenance `json:"provenance,omitempty"`
}

// ManifestSessionOptions declares backend-specific session options for a model.
// These are applied when creating inference sessions, allowing models to declare
// their own requirements (e.g., compile-time constants, dynamic axes) without
// hardcoding them in the runner.
type ManifestSessionOptions struct {
	// InputConstants maps input names to compile-time constant values.
	// On GoMLX-based backends, these inputs are baked into the computation graph,
	// enabling static resolution of ONNX If node conditions.
	InputConstants map[string]any `json:"input_constants,omitempty"`
	// DynamicAxes overrides fixed input dimensions to be treated as dynamic.
	DynamicAxes []ManifestDynamicAxis `json:"dynamic_axes,omitempty"`
}

// ManifestDynamicAxis specifies that a fixed input dimension should be dynamic.
type ManifestDynamicAxis struct {
	// InputName is the ONNX input name (e.g., "input")
	InputName string `json:"input_name"`
	// Axis is the dimension index to override (e.g., 0 for batch)
	Axis int `json:"axis"`
	// ParamName is the symbolic name for the dynamic dimension (e.g., "batch")
	ParamName string `json:"param_name"`
}

// UnmarshalJSON custom-decodes ManifestSessionOptions to ensure JSON integer
// values in InputConstants are stored as int64 (not float64). Go's default
// json.Unmarshal into map[string]any produces float64 for all numbers, but
// backends need int64 for integer constants (e.g., sample rate).
func (o *ManifestSessionOptions) UnmarshalJSON(data []byte) error {
	var raw struct {
		InputConstants map[string]json.RawMessage `json:"input_constants,omitempty"`
		DynamicAxes    []ManifestDynamicAxis      `json:"dynamic_axes,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	o.DynamicAxes = raw.DynamicAxes
	if len(raw.InputConstants) > 0 {
		o.InputConstants = make(map[string]any, len(raw.InputConstants))
		for k, v := range raw.InputConstants {
			raw := strings.TrimSpace(string(v))
			// Only try int64 when the raw token has no decimal point or exponent,
			// so that "1.0" stays float64 and only "1" becomes int64.
			if !strings.ContainsAny(raw, ".eE") {
				var i int64
				if err := json.Unmarshal(v, &i); err == nil {
					o.InputConstants[k] = i
					continue
				}
			}
			// Try float64
			var f float64
			if err := json.Unmarshal(v, &f); err == nil {
				o.InputConstants[k] = f
				continue
			}
			// Try string
			var s string
			if err := json.Unmarshal(v, &s); err == nil {
				o.InputConstants[k] = s
				continue
			}
			// Try bool
			var b bool
			if err := json.Unmarshal(v, &b); err == nil {
				o.InputConstants[k] = b
				continue
			}
			// Fallback: store raw
			o.InputConstants[k] = string(v)
		}
	}
	return nil
}

// VariantEntry can be either a single ModelFile or an array of ModelFiles.
// This supports both single-model variants and multi-model variants (like CLIP).
type VariantEntry struct {
	Files []ModelFile
}

// UnmarshalJSON handles multiple formats:
// 1. Array of files: [{"name": "...", ...}]
// 2. Single file object: {"name": "...", ...}
// 3. Object with files key: {"files": [{"name": "...", ...}]}
func (v *VariantEntry) UnmarshalJSON(data []byte) error {
	// Try as array of files first
	var files []ModelFile
	if err := json.Unmarshal(data, &files); err == nil {
		v.Files = files
		return nil
	}

	// Try as object with "files" key (registry format)
	var wrapper struct {
		Files []ModelFile `json:"files"`
	}
	if err := json.Unmarshal(data, &wrapper); err == nil && len(wrapper.Files) > 0 {
		v.Files = wrapper.Files
		return nil
	}

	// Try as single file object
	var file ModelFile
	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}
	v.Files = []ModelFile{file}
	return nil
}

// MarshalJSON serializes the variant entry
func (v VariantEntry) MarshalJSON() ([]byte, error) {
	if len(v.Files) == 1 {
		return json.Marshal(v.Files[0])
	}
	return json.Marshal(v.Files)
}

// SupportsBackend returns true if the model supports the given backend.
// If no backends are specified, all backends are supported.
func (m *ModelManifest) SupportsBackend(backend string) bool {
	if len(m.Backends) == 0 {
		return true // All backends supported by default
	}
	return slices.Contains(m.Backends, backend)
}

// HasCapability returns true if the model has the specified capability.
func (m *ModelManifest) HasCapability(capability Capability) bool {
	return slices.Contains(m.Capabilities, string(capability))
}

// FullName returns the full owner/name format (e.g., "BAAI/bge-small-en-v1.5")
// Falls back to just Name if Owner is empty (legacy manifests)
// NOTE: Use DirPath() for filesystem operations to ensure cross-platform compatibility.
func (m *ModelManifest) FullName() string {
	if m.Owner != "" {
		return m.Owner + "/" + m.Name
	}
	return m.Name
}

// DirPath returns the directory path for this model using platform-appropriate separators.
// Use this instead of FullName() when constructing filesystem paths.
func (m *ModelManifest) DirPath() string {
	if m.Owner != "" {
		return filepath.Join(m.Owner, m.Name)
	}
	return m.Name
}

// Validate checks that the manifest is well-formed
func (m *ModelManifest) Validate() error {
	if m.SchemaVersion < 1 || m.SchemaVersion > CurrentSchemaVersion {
		return fmt.Errorf("unsupported schema version: %d (expected 1-%d)", m.SchemaVersion, CurrentSchemaVersion)
	}
	if m.Name == "" {
		return fmt.Errorf("manifest missing required field: name")
	}
	if m.Type == "" {
		return fmt.Errorf("manifest missing required field: type")
	}
	if _, err := ParseModelType(string(m.Type)); err != nil {
		return fmt.Errorf("invalid model type: %s", m.Type)
	}
	if len(m.Files) == 0 {
		return fmt.Errorf("manifest must have at least one file")
	}

	// Validate file entries and track which ONNX files are present
	hasAnyOnnx := false
	hasVisualOnnx := false
	hasTextOnnx := false
	hasAudioOnnx := false
	hasEncoderOnnx := false
	hasDecoderOnnx := false
	for _, f := range m.Files {
		if f.Name == "" {
			return fmt.Errorf("file entry missing name")
		}
		if f.Digest == "" {
			return fmt.Errorf("file %s missing digest", f.Name)
		}
		if !strings.HasPrefix(f.Digest, "sha256:") {
			return fmt.Errorf("file %s has invalid digest format (expected sha256:...)", f.Name)
		}
		if strings.HasSuffix(f.Name, ".onnx") {
			hasAnyOnnx = true
		}
		switch f.Name {
		case "visual_model.onnx":
			hasVisualOnnx = true
		case "text_model.onnx":
			hasTextOnnx = true
		case "audio_model.onnx":
			hasAudioOnnx = true
		case "encoder.onnx":
			hasEncoderOnnx = true
		case "decoder.onnx":
			hasDecoderOnnx = true
		}
	}

	// Check for required ONNX files based on model type and capability.
	// Specific model types enforce exact filenames; the fallback just
	// requires at least one .onnx file (covers multistage OCR pipelines
	// like PaddleOCR which use det.onnx/rec.onnx instead of model.onnx).
	if m.HasCapability(CapabilityImage) {
		// Image embedders (CLIP) require visual_model.onnx + text_model.onnx
		if !hasVisualOnnx || !hasTextOnnx {
			return fmt.Errorf("image embedder must include visual_model.onnx and text_model.onnx")
		}
		// Image models only support ONNX runtime
		if len(m.Backends) > 0 && !m.SupportsBackend("onnx") {
			return fmt.Errorf("image embedders only support ONNX backend")
		}
	} else if m.HasCapability(CapabilityAudio) && m.Type == ModelTypeEmbedder {
		// Audio embedders (CLAP) require audio_model.onnx + text_model.onnx
		if !hasAudioOnnx || !hasTextOnnx {
			return fmt.Errorf("audio embedder must include audio_model.onnx and text_model.onnx")
		}
		// Audio models only support ONNX runtime
		if len(m.Backends) > 0 && !m.SupportsBackend("onnx") {
			return fmt.Errorf("audio embedders only support ONNX backend")
		}
	} else if m.Type == ModelTypeRewriter {
		// Seq2seq models (rewriters) require encoder.onnx + decoder.onnx
		if !hasEncoderOnnx || !hasDecoderOnnx {
			return fmt.Errorf("rewriter model must include encoder.onnx and decoder.onnx")
		}
	} else if hasEncoderOnnx && hasDecoderOnnx {
		// Seq2seq recognizers (REBEL) have encoder/decoder instead of model.onnx
		// This is valid for recognizers with 'relations' capability
	} else if !hasAnyOnnx {
		return fmt.Errorf("manifest must include at least one .onnx model file")
	}

	// Validate variant files if present
	for variantID, variantEntry := range m.Variants {
		if len(variantEntry.Files) == 0 {
			return fmt.Errorf("variant %s has no files", variantID)
		}
		for _, variantFile := range variantEntry.Files {
			if variantFile.Name == "" {
				return fmt.Errorf("variant %s file missing name", variantID)
			}
			if variantFile.Digest == "" {
				return fmt.Errorf("variant %s file missing digest", variantID)
			}
		}
		// Validate variant ID is known
		if _, ok := VariantFilenames[variantID]; !ok {
			return fmt.Errorf("unknown variant identifier: %s (valid: f16, bf16, i8, i8-st, i4)", variantID)
		}
	}

	return nil
}

// ParseManifest parses a JSON manifest
func ParseManifest(data []byte) (*ModelManifest, error) {
	var manifest ModelManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// RegistryIndex lists all available models in the registry
type RegistryIndex struct {
	// SchemaVersion is the index format version
	SchemaVersion int `json:"schemaVersion"`
	// Models lists all available model manifests
	Models []ModelIndexEntry `json:"models"`
}

// ModelIndexEntry is a summary of a model in the registry index
type ModelIndexEntry struct {
	// Name is the model identifier
	Name string `json:"name"`
	// Source is the full owner/model identifier (e.g., "BAAI/bge-small-en-v1.5")
	Source string `json:"source,omitempty"`
	// Owner is the namespace/organization
	Owner string `json:"owner,omitempty"`
	// Type is the model type
	Type ModelType `json:"type"`
	// Description is a human-readable description
	Description string `json:"description,omitempty"`
	// Capabilities lists special capabilities (e.g., ["multimodal"])
	Capabilities []string `json:"capabilities,omitempty"`
	// Size is the total size of all files in bytes
	Size int64 `json:"size,omitempty"`
	// Variants lists available variant identifiers (e.g., ["f16", "i8"])
	Variants []string `json:"variants,omitempty"`
	// Backends lists required backends for this model (e.g., ["onnx"] for models with XLA-incompatible ops)
	// If empty/nil, all backends are supported
	Backends []string `json:"backends,omitempty"`
}

// ParseRegistryIndex parses a JSON registry index
func ParseRegistryIndex(data []byte) (*RegistryIndex, error) {
	var index RegistryIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, fmt.Errorf("parsing registry index: %w", err)
	}
	if index.SchemaVersion < 1 || index.SchemaVersion > CurrentIndexSchemaVersion {
		return nil, fmt.Errorf("unsupported index schema version: %d (expected 1-%d)", index.SchemaVersion, CurrentIndexSchemaVersion)
	}
	return &index, nil
}

// ManifestFilename is the standard filename for model manifests
const ManifestFilename = "model_manifest.json"

// SaveTo writes the manifest to a file as JSON
func (m *ModelManifest) SaveTo(path string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing manifest: %w", err)
	}
	return nil
}

// LoadManifestFromFile loads and validates a manifest from a file
func LoadManifestFromFile(path string) (*ModelManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	return ParseManifest(data)
}

// LoadManifestFromDir loads a manifest from a model directory
// Looks for ManifestFilename in the directory
func LoadManifestFromDir(modelDir string) (*ModelManifest, error) {
	return LoadManifestFromFile(filepath.Join(modelDir, ManifestFilename))
}

// ComputeFileDigest computes the SHA256 digest of a file in "sha256:..." format
func ComputeFileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening file: %w", err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("reading file: %w", err)
	}

	return fmt.Sprintf("sha256:%x", h.Sum(nil)), nil
}

// ScanModelFiles scans a directory and returns ModelFile entries for all files
func ScanModelFiles(modelDir string) ([]ModelFile, error) {
	entries, err := os.ReadDir(modelDir)
	if err != nil {
		return nil, fmt.Errorf("reading directory: %w", err)
	}

	var files []ModelFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// Skip the manifest itself
		if entry.Name() == ManifestFilename {
			continue
		}

		filePath := filepath.Join(modelDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		digest, err := ComputeFileDigest(filePath)
		if err != nil {
			continue
		}

		files = append(files, ModelFile{
			Name:   entry.Name(),
			Digest: digest,
			Size:   info.Size(),
		})
	}

	return files, nil
}

// GenerateManifestFromDir creates a new manifest by scanning a model directory
func GenerateManifestFromDir(modelDir, owner, name string, modelType ModelType) (*ModelManifest, error) {
	files, err := ScanModelFiles(modelDir)
	if err != nil {
		return nil, fmt.Errorf("scanning files: %w", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no model files found in directory")
	}

	source := name
	if owner != "" {
		source = owner + "/" + name
	}

	manifest := &ModelManifest{
		SchemaVersion: CurrentSchemaVersion,
		Name:          name,
		Source:        source,
		Owner:         owner,
		Type:          modelType,
		Files:         files,
		Provenance: &ModelProvenance{
			DownloadedFrom: "local",
			DownloadedAt:   time.Now(),
		},
	}

	// Discover variants from ONNX files
	manifest.Variants = discoverVariantsFromFiles(files)

	return manifest, nil
}

// discoverVariantsFromFiles examines the file list to discover model variants
func discoverVariantsFromFiles(files []ModelFile) map[string]VariantEntry {
	variants := make(map[string]VariantEntry)

	for _, f := range files {
		if variantID, ok := FilenameToVariant[f.Name]; ok {
			// Skip the default f32 variant (it's the base model)
			if variantID == VariantF32 {
				continue
			}
			variants[variantID] = VariantEntry{Files: []ModelFile{f}}
		}
	}

	return variants
}
