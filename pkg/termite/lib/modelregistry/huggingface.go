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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/gomlx/go-huggingface/hub"
)

// HuggingFaceClient pulls ONNX models from HuggingFace Hub
type HuggingFaceClient struct {
	token           string
	progressHandler ProgressHandler
}

// HFClientOption configures the HuggingFace client
type HFClientOption func(*HuggingFaceClient)

// NewHuggingFaceClient creates a new HuggingFace client
func NewHuggingFaceClient(opts ...HFClientOption) *HuggingFaceClient {
	c := &HuggingFaceClient{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// WithHFToken sets the HuggingFace API token for gated models
func WithHFToken(token string) HFClientOption {
	return func(c *HuggingFaceClient) { c.token = token }
}

// WithHFProgressHandler sets the progress handler for downloads
func WithHFProgressHandler(h ProgressHandler) HFClientOption {
	return func(c *HuggingFaceClient) { c.progressHandler = h }
}

// PullFromHuggingFace downloads ONNX model files from a HuggingFace repo.
// variant can be: "", "fp16", "q4", "q4f16", "quantized"
//
// The model is stored in the owner/model directory structure:
//
//	destDir/modelType/owner/model-name/
//
// A model_manifest.json is generated and saved with the model files.
func (c *HuggingFaceClient) PullFromHuggingFace(
	ctx context.Context,
	repoID string,
	modelType ModelType,
	destDir string,
	variant string,
) error {
	// Parse repo ID to get owner and model name
	ref, err := ParseModelRef(repoID)
	if err != nil {
		return fmt.Errorf("parsing repo ID: %w", err)
	}

	// If no owner in ref, try to extract from repoID directly (e.g., "BAAI/bge-small-en-v1.5")
	if ref.Owner == "" {
		parts := strings.SplitN(repoID, "/", 2)
		if len(parts) == 2 {
			ref.Owner = parts[0]
			ref.Name = parts[1]
		}
	}

	repo := hub.New(repoID)
	if c.token != "" {
		repo = repo.WithAuth(c.token)
	}

	// List all files in repo
	var files []string
	for fileName, err := range repo.IterFileNames() {
		if err != nil {
			return fmt.Errorf("listing files: %w", err)
		}
		files = append(files, fileName)
	}

	// Filter and select files to download based on model type
	var toDownload []string
	if modelType == ModelTypeGenerator || modelType == ModelTypeReader || modelType == ModelTypeTranscriber {
		// For generators, readers (Vision2Seq), and transcribers (Speech2Seq) -
		// all encoder-decoder models that need multiple ONNX files.
		// Auto-detect smallest variant if not specified.
		if variant == "" {
			variant = findSmallestGeneratorVariant(files)
			if variant != "" {
				fmt.Printf("Auto-selected variant: %s\n", variant)
			}
		}
		toDownload = selectGeneratorFiles(files, variant)
	} else {
		toDownload = selectONNXFiles(files, variant)
	}
	if len(toDownload) == 0 {
		return fmt.Errorf("no model files found in %s", repoID)
	}

	// Create destination directory with owner/model structure
	modelDir := filepath.Join(destDir, modelType.DirName(), ref.DirPath())
	if err := os.MkdirAll(modelDir, 0755); err != nil {
		return fmt.Errorf("creating directory: %w", err)
	}

	// Download each file
	for _, fileName := range toDownload {
		localPath, err := repo.DownloadFileCtx(ctx, fileName)
		if err != nil {
			return fmt.Errorf("downloading %s: %w", fileName, err)
		}

		// Flatten path (e.g., "onnx/model.onnx" -> "model.onnx")
		destName := filepath.Base(fileName)
		destPath := filepath.Join(modelDir, destName)

		// Report progress before copy
		if c.progressHandler != nil {
			c.progressHandler(0, 0, destName)
		}

		// Copy from cache to destination
		if err := copyFile(localPath, destPath); err != nil {
			return fmt.Errorf("copying %s: %w", fileName, err)
		}

		// Report completion
		if c.progressHandler != nil {
			if info, err := os.Stat(destPath); err == nil {
				c.progressHandler(info.Size(), info.Size(), destName)
			}
		}
	}

	// Generate and save local manifest
	if err := c.generateAndSaveManifest(modelDir, repoID, ref, modelType); err != nil {
		// Log warning but don't fail the download
		fmt.Printf("Warning: failed to generate manifest: %v\n", err)
	}

	return nil
}

// generateAndSaveManifest creates a manifest for downloaded model files
func (c *HuggingFaceClient) generateAndSaveManifest(
	modelDir string,
	repoID string,
	ref ModelRef,
	modelType ModelType,
) error {
	// Scan downloaded files
	files, err := ScanModelFiles(modelDir)
	if err != nil {
		return fmt.Errorf("scanning files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no files found in %s", modelDir)
	}

	// Create manifest
	// Note: Backends is left empty (nil) meaning all backends are supported.
	// Models that require specific backends (e.g., ONNX-only due to unsupported ops)
	// should specify this in the registry manifest or hardcoded list.
	manifest := &ModelManifest{
		SchemaVersion: CurrentSchemaVersion,
		Name:          ref.Name,
		Source:        repoID,
		Owner:         ref.Owner,
		Type:          modelType,
		Files:         files,
		Provenance: &ModelProvenance{
			DownloadedFrom: "huggingface",
			DownloadedAt:   time.Now(),
		},
	}

	// Discover variants from downloaded files
	manifest.Variants = discoverVariantsFromFiles(files)

	// Detect capabilities for multimodal models (CLIP for visual, CLAP for audio)
	hasVisual := false
	hasAudio := false
	for _, f := range files {
		switch f.Name {
		case "visual_model.onnx", "visual_model_quantized.onnx":
			hasVisual = true
		case "audio_model.onnx", "audio_model_quantized.onnx":
			hasAudio = true
		}
	}
	if hasVisual {
		manifest.Capabilities = append(manifest.Capabilities, string(CapabilityImage))
	}
	if hasAudio {
		manifest.Capabilities = append(manifest.Capabilities, string(CapabilityAudio))
	}

	// Save manifest
	manifestPath := filepath.Join(modelDir, ManifestFilename)
	return manifest.SaveTo(manifestPath)
}

// knownONNXVariantSuffixes maps filename suffixes (before .onnx) to variant names.
// A file like "decoder_model_fp16.onnx" has suffix "_fp16".
// Longer suffixes are checked first to avoid false matches (e.g., "_q4f16" before "_q4").
var knownONNXVariantSuffixes = []struct {
	suffix  string
	variant string
}{
	{"_q4f16", "q4f16"},
	{"_quantized", "quantized"},
	{"_fp16", "fp16"},
	{"_int8", "int8"},
	{"_uint8", "uint8"},
	{"_bnb4", "bnb4"},
	{"_q4", "q4"},
}

// onnxVariantSuffix returns the variant suffix of an ONNX filename, or "" if it is
// a base (non-variant) file. For example:
//   - "decoder_model_fp16.onnx" → "_fp16"
//   - "decoder_model.onnx"      → ""
//   - "encoder_model_fp16.onnx_data" → "_fp16"
func onnxVariantSuffix(filename string) string {
	base := filepath.Base(filename)
	if !isONNXFile(base) {
		return ""
	}
	stem := onnxStem(base)

	for _, v := range knownONNXVariantSuffixes {
		if strings.HasSuffix(stem, v.suffix) {
			return v.suffix
		}
	}
	return ""
}

// matchesVariantSuffix checks if an ONNX file matches the requested variant.
// For variant="" (default/base), only files without a known variant suffix match.
// For variant="fp16", only files with the "_fp16" suffix match.
// Non-ONNX files always match (they are supporting files like tokenizer.json).
func matchesVariantSuffix(filename string, variant string) bool {
	base := filepath.Base(filename)
	if !isONNXFile(base) {
		return true
	}

	fileSuffix := onnxVariantSuffix(filename)

	if variant == "" {
		// Default: only base files (no variant suffix)
		return fileSuffix == ""
	}

	// Find the suffix for the requested variant
	for _, v := range knownONNXVariantSuffixes {
		if v.variant == variant {
			return fileSuffix == v.suffix
		}
	}

	// Unknown variant, try direct suffix match
	return fileSuffix == "_"+variant
}

// hasFilesInSubdir checks if any files exist under the given subdirectory prefix.
func hasFilesInSubdir(files []string, subdir string) bool {
	prefix := subdir + "/"
	for _, f := range files {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	return false
}

// selectGeneratorFiles selects files needed for onnxruntime-genai and encoder-decoder models.
// This includes genai_config.json, ONNX files, and tokenizer files.
//
// For repos with subdirectory-based variants (e.g., cpu-int4/model.onnx), only files
// from the selected subdirectory are included.
//
// For repos with flat file layouts (e.g., decoder_model.onnx, decoder_model_fp16.onnx),
// filename-based variant filtering is applied to avoid downloading all variants.
func selectGeneratorFiles(files []string, variant string) []string {
	// If no variant specified, auto-select the smallest cpu variant (subdirectory-based)
	if variant == "" {
		variant = findSmallestGeneratorVariant(files)
	}

	// Determine if we're using subdirectory-based variants
	useSubdirFilter := variant != "" && hasFilesInSubdir(files, variant)

	var result []string

	// Files to include by exact basename match
	includeExact := map[string]bool{
		"genai_config.json":       true,
		"tokenizer.json":          true,
		"tokenizer.model":         true,
		"tokenizer_config.json":   true,
		"config.json":             true,
		"special_tokens_map.json": true,
		"added_tokens.json":       true,
		"generation_config.json":  true,
	}

	// Files to include by suffix
	includeSuffixes := []string{
		".onnx",
		".onnx.data",            // External data files
		".onnx_data",            // Alternative naming
		".txt",                  // Vocab files like vocab.txt, merges.txt
		".spm",                  // SentencePiece model files
		".tiktoken",             // Tiktoken encoding files
		".jinja",                // Chat template files (e.g., chat_template.jinja)
		"processor_config.json", // For multimodal models
	}

	for _, f := range files {
		// Filter by variant subdirectory if using subdirectory-based variants
		if useSubdirFilter && !strings.HasPrefix(f, variant+"/") && f != variant {
			continue
		}

		base := filepath.Base(f)

		// Check exact matches
		if includeExact[base] {
			result = append(result, f)
			continue
		}

		// Check suffix matches
		for _, suffix := range includeSuffixes {
			if strings.HasSuffix(base, suffix) {
				// For flat repos, apply filename-based variant filtering on ONNX files
				if !useSubdirFilter && !matchesVariantSuffix(f, variant) {
					break
				}
				result = append(result, f)
				break
			}
		}
	}

	return result
}

// findSmallestGeneratorVariant finds the smallest generator variant path.
// It looks for cpu-int4 variants first, then falls back to any cpu variant,
// then any variant with genai_config.json.
// It prefers smaller model sizes (e.g., 1b over 4b over 12b).
func findSmallestGeneratorVariant(files []string) string {
	// Find all directories containing genai_config.json
	variantDirs := make(map[string]bool)
	for _, f := range files {
		if filepath.Base(f) == "genai_config.json" {
			dir := filepath.Dir(f)
			if dir != "." {
				variantDirs[dir] = true
			}
		}
	}

	if len(variantDirs) == 0 {
		return "" // No variants found, download everything
	}

	// Priority order for selecting variants:
	// 1. cpu-int4 variants (smallest)
	// 2. Any cpu variant
	// 3. Any variant

	var cpuInt4Variants []string
	var cpuVariants []string
	var allVariants []string

	for dir := range variantDirs {
		allVariants = append(allVariants, dir)
		lowerDir := strings.ToLower(dir)
		if strings.Contains(lowerDir, "cpu") {
			cpuVariants = append(cpuVariants, dir)
			if strings.Contains(lowerDir, "int4") {
				cpuInt4Variants = append(cpuInt4Variants, dir)
			}
		}
	}

	// Sort by model size (prefer smaller: 1b < 4b < 12b < 27b)
	sortByModelSize := func(variants []string) {
		slices.SortFunc(variants, func(a, b string) int {
			sizeA := extractModelSize(a)
			sizeB := extractModelSize(b)
			if sizeA != sizeB {
				return sizeA - sizeB
			}
			// Fall back to alphabetical for same size
			return strings.Compare(a, b)
		})
	}

	sortByModelSize(cpuInt4Variants)
	sortByModelSize(cpuVariants)
	sortByModelSize(allVariants)

	// Return the first match in priority order
	if len(cpuInt4Variants) > 0 {
		return cpuInt4Variants[0]
	}
	if len(cpuVariants) > 0 {
		return cpuVariants[0]
	}
	return allVariants[0]
}

// extractModelSize extracts the numeric model size from a path like "gemma-3-4b-it/..."
// Returns a large number if no size found so those sort last.
func extractModelSize(path string) int {
	// Look for patterns like "1b", "4b", "12b", "27b" in the path
	lowerPath := strings.ToLower(path)

	// Common size patterns
	sizePatterns := []struct {
		pattern string
		size    int
	}{
		{"-1b", 1},
		{"-2b", 2},
		{"-3b", 3},
		{"-4b", 4},
		{"-7b", 7},
		{"-8b", 8},
		{"-12b", 12},
		{"-13b", 13},
		{"-27b", 27},
		{"-70b", 70},
		{"1b-", 1},
		{"2b-", 2},
		{"3b-", 3},
		{"4b-", 4},
		{"7b-", 7},
		{"8b-", 8},
		{"12b-", 12},
		{"13b-", 13},
		{"27b-", 27},
		{"70b-", 70},
	}

	for _, sp := range sizePatterns {
		if strings.Contains(lowerPath, sp.pattern) {
			return sp.size
		}
	}

	return 999 // No size found, sort last
}

// selectONNXFiles filters files based on variant preference.
// It returns tokenizer files plus the ONNX model file(s) matching the variant.
// For multimodal models, it also includes separate encoder files (text_model, visual_model, audio_model).
func selectONNXFiles(files []string, variant string) []string {
	var result []string

	// Always include tokenizer/config files from anywhere in the repo
	tokenizerFiles := []string{"tokenizer.json", "tokenizer.model", "tokenizer_config.json", "config.json", "special_tokens_map.json"}
	for _, tf := range tokenizerFiles {
		for _, f := range files {
			if filepath.Base(f) == tf {
				result = append(result, f)
				break
			}
		}
	}

	// Determine ONNX file patterns based on variant
	var onnxBases []string
	var quantizedSuffix string
	switch variant {
	case "fp16":
		onnxBases = []string{"model_fp16", "text_model_fp16", "visual_model_fp16", "audio_model_fp16"}
		quantizedSuffix = "_fp16"
	case "q4":
		onnxBases = []string{"model_q4", "text_model_q4", "visual_model_q4", "audio_model_q4"}
		quantizedSuffix = "_q4"
	case "q4f16":
		onnxBases = []string{"model_q4f16", "text_model_q4f16", "visual_model_q4f16", "audio_model_q4f16"}
		quantizedSuffix = "_q4f16"
	case "quantized":
		onnxBases = []string{"model_quantized", "text_model_quantized", "visual_model_quantized", "audio_model_quantized"}
		quantizedSuffix = "_quantized"
	default:
		// Default variant - include both standard and quantized multimodal encoders
		onnxBases = []string{"model", "text_model", "visual_model", "audio_model"}
		quantizedSuffix = ""
	}

	// Find matching ONNX files (model.onnx + model.onnx_data)
	for _, f := range files {
		base := filepath.Base(f)
		for _, onnxBase := range onnxBases {
			// Match exact model file or its data file
			if base == onnxBase+".onnx" || base == onnxBase+".onnx_data" {
				result = append(result, f)
				break
			}
		}
	}

	// For default variant, also include quantized versions of multimodal encoders if they exist
	// (some repos like Xenova/clap-htsat-unfused only have quantized encoders in onnx/ subdir)
	if quantizedSuffix == "" {
		quantizedEncoders := []string{"text_model_quantized", "visual_model_quantized", "audio_model_quantized"}
		for _, f := range files {
			base := filepath.Base(f)
			for _, encoder := range quantizedEncoders {
				if base == encoder+".onnx" || base == encoder+".onnx_data" {
					result = append(result, f)
					break
				}
			}
		}
	}

	return result
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening source: %w", err)
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("creating destination: %w", err)
	}

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		_ = dstFile.Close()
		return fmt.Errorf("copying: %w", err)
	}

	return dstFile.Close()
}

// ValidVariants returns the list of valid ONNX variant names
func ValidVariants() []string {
	return []string{"", "fp16", "q4", "q4f16", "quantized"}
}

// IsValidVariant checks if a variant name is valid
func IsValidVariant(variant string) bool {
	return slices.Contains(ValidVariants(), variant)
}

// VariantDescription returns a human-readable description of a variant
func VariantDescription(variant string) string {
	switch variant {
	case "":
		return "full precision (default)"
	case "fp16":
		return "half precision (FP16)"
	case "q4":
		return "4-bit quantized"
	case "q4f16":
		return "4-bit quantized with FP16"
	case "quantized":
		return "INT8 quantized"
	default:
		return "unknown"
	}
}

// ListRepoFiles returns all files in a HuggingFace repo (useful for inspection)
func (c *HuggingFaceClient) ListRepoFiles(ctx context.Context, repoID string) ([]string, error) {
	repo := hub.New(repoID)
	if c.token != "" {
		repo = repo.WithAuth(c.token)
	}

	var files []string
	for fileName, err := range repo.IterFileNames() {
		if err != nil {
			return nil, fmt.Errorf("listing files: %w", err)
		}
		files = append(files, fileName)
	}
	return files, nil
}

// DetectAvailableVariants returns which ONNX variants are available in a repo
func (c *HuggingFaceClient) DetectAvailableVariants(ctx context.Context, repoID string) ([]string, error) {
	files, err := c.ListRepoFiles(ctx, repoID)
	if err != nil {
		return nil, err
	}

	variants := []string{}
	variantPatterns := map[string]string{
		"":          "model.onnx",
		"fp16":      "model_fp16.onnx",
		"q4":        "model_q4.onnx",
		"q4f16":     "model_q4f16.onnx",
		"quantized": "model_quantized.onnx",
	}

	for variant, pattern := range variantPatterns {
		for _, f := range files {
			if filepath.Base(f) == pattern {
				if variant == "" {
					variants = append(variants, "default")
				} else {
					variants = append(variants, variant)
				}
				break
			}
		}
	}

	return variants, nil
}

// ParseHuggingFaceRef parses a model reference like "hf:owner/repo" and returns the repo ID
func ParseHuggingFaceRef(ref string) (repoID string, isHF bool) {
	if after, ok := strings.CutPrefix(ref, "hf:"); ok {
		return after, true
	}
	return "", false
}

// DetectGeneratorVariants returns available onnxruntime-genai variants in a repo.
// These are subdirectories containing genai_config.json files.
func (c *HuggingFaceClient) DetectGeneratorVariants(ctx context.Context, repoID string) ([]string, error) {
	files, err := c.ListRepoFiles(ctx, repoID)
	if err != nil {
		return nil, err
	}

	// Find all directories containing genai_config.json
	variantDirs := make(map[string]bool)
	for _, f := range files {
		if filepath.Base(f) == "genai_config.json" {
			dir := filepath.Dir(f)
			if dir != "." {
				variantDirs[dir] = true
			}
		}
	}

	variants := make([]string, 0, len(variantDirs))
	for dir := range variantDirs {
		variants = append(variants, dir)
	}
	slices.Sort(variants)
	return variants, nil
}

// pipelineTagToModelType maps HuggingFace pipeline_tag values to termite ModelType.
// See https://huggingface.co/docs/hub/models-widgets#enabling-a-widget for the full list.
var pipelineTagToModelType = map[string]ModelType{
	// Embedder models
	"feature-extraction":             ModelTypeEmbedder,
	"sentence-similarity":            ModelTypeEmbedder,
	"zero-shot-image-classification": ModelTypeEmbedder,

	// Reranker models
	"text-ranking": ModelTypeReranker,

	// Generator models
	"text-generation": ModelTypeGenerator,

	// Rewriter / seq2seq models
	"text2text-generation": ModelTypeRewriter,
	"summarization":        ModelTypeRewriter,
	"translation":          ModelTypeRewriter,

	// Reader / OCR / vision-language models
	"image-text-to-text":          ModelTypeReader,
	"image-to-text":               ModelTypeReader,
	"document-question-answering": ModelTypeReader,

	// Transcriber models
	"automatic-speech-recognition": ModelTypeTranscriber,

	// Classifier models
	"text-classification":      ModelTypeClassifier,
	"token-classification":     ModelTypeClassifier,
	"zero-shot-classification": ModelTypeClassifier,

	// Recognizer models
	"object-detection":     ModelTypeRecognizer,
	"image-classification": ModelTypeRecognizer,
	"image-segmentation":   ModelTypeRecognizer,
}

// GetRepoInfo fetches the HuggingFace model info including pipeline_tag and tags.
func (c *HuggingFaceClient) GetRepoInfo(ctx context.Context, repoID string) (*hub.RepoInfo, error) {
	repo := hub.New(repoID)
	if c.token != "" {
		repo = repo.WithAuth(c.token)
	}
	if err := repo.DownloadInfo(false); err != nil {
		return nil, fmt.Errorf("fetching model info: %w", err)
	}
	info := repo.Info()
	if info == nil {
		return nil, fmt.Errorf("no model info available for %s", repoID)
	}
	return info, nil
}

// DetectModelType attempts to detect the model type from repo contents and metadata.
// It first checks file structure (genai_config.json, encoder/decoder, visual/text models),
// then falls back to the HuggingFace pipeline_tag for disambiguation.
func (c *HuggingFaceClient) DetectModelType(ctx context.Context, repoID string) (ModelType, error) {
	files, err := c.ListRepoFiles(ctx, repoID)
	if err != nil {
		return "", fmt.Errorf("listing files: %w", err)
	}

	hasGenaiConfig := false
	hasEncoder := false
	hasDecoder := false
	hasVisual := false
	hasAudio := false
	hasText := false
	hasModelOnnx := false

	for _, f := range files {
		base := filepath.Base(f)
		switch base {
		case "genai_config.json":
			hasGenaiConfig = true
		case "encoder.onnx":
			hasEncoder = true
		case "decoder.onnx":
			hasDecoder = true
		case "visual_model.onnx", "visual_model_quantized.onnx":
			hasVisual = true
		case "audio_model.onnx", "audio_model_quantized.onnx":
			hasAudio = true
		case "text_model.onnx", "text_model_quantized.onnx":
			hasText = true
		case "model.onnx":
			hasModelOnnx = true
		}
	}

	// Check for generator (onnxruntime-genai format)
	if hasGenaiConfig {
		return ModelTypeGenerator, nil
	}

	// Check for seq2seq (rewriter)
	if hasEncoder && hasDecoder {
		return ModelTypeRewriter, nil
	}

	// Check for multimodal CLIP (visual + text embedder)
	if hasVisual && hasText {
		return ModelTypeEmbedder, nil
	}

	// Check for multimodal CLAP (audio + text embedder)
	if hasAudio && hasText {
		return ModelTypeEmbedder, nil
	}

	// Fall back to HuggingFace pipeline_tag for disambiguation
	if modelType, err := c.detectFromPipelineTag(ctx, repoID); err == nil {
		return modelType, nil
	}

	if hasModelOnnx {
		return "", fmt.Errorf("cannot auto-detect model type for %s (use --type flag to specify: embedder, chunker, reranker, reader, etc.)", repoID)
	}

	return "", fmt.Errorf("no recognizable model files found in repository %s", repoID)
}

// detectFromPipelineTag fetches the HuggingFace model info and maps the pipeline_tag to a ModelType.
func (c *HuggingFaceClient) detectFromPipelineTag(ctx context.Context, repoID string) (ModelType, error) {
	info, err := c.GetRepoInfo(ctx, repoID)
	if err != nil {
		return "", err
	}

	if info.PipelineTag != "" {
		if modelType, ok := pipelineTagToModelType[info.PipelineTag]; ok {
			return modelType, nil
		}
		return "", fmt.Errorf("unknown pipeline_tag %q", info.PipelineTag)
	}

	return "", fmt.Errorf("no pipeline_tag in model info")
}
