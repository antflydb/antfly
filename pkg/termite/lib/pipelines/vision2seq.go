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

package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	"os"
	"path/filepath"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// =============================================================================
// Configuration Types
// =============================================================================

// Vision2SeqModelConfig holds parsed configuration for a Vision2Seq model.
// This is loaded from config.json and preprocessor_config.json.
type Vision2SeqModelConfig struct {
	// Path to the model directory
	ModelPath string

	// Paths to ONNX files (if present)
	EncoderPath string
	DecoderPath string

	// Optional split decoder paths for XLA compatibility.
	// These avoid 0-dimension tensors on the first decoder step.
	DecoderFirstStepPath string // decoder_model.onnx (no past KV inputs)
	DecoderWithPastPath  string // decoder_with_past_model.onnx (with past KV inputs)

	// Optional embed_tokens path (decoder-only VLMs like Moondream2)
	EmbedTokensPath string

	// Optional projection layer path (legacy Moondream2 layouts)
	ProjectionPath string

	// Decoder configuration
	DecoderConfig *backends.DecoderConfig

	// Image preprocessing configuration
	ImageConfig *backends.ImageConfig

	// Architecture details for KV-cache
	NumLayers  int
	NumHeads   int
	HeadDim    int
	HiddenSize int
}

// Vision2SeqConfig holds configuration for creating a Vision2SeqPipeline.
type Vision2SeqConfig struct {
	// ImageConfig for preprocessing. If nil, uses model's default.
	ImageConfig *backends.ImageConfig

	// GenerationConfig for text generation. If nil, uses defaults.
	GenerationConfig *backends.GenerationConfig
}

// Vision2SeqResult is an alias for EncoderDecoderResult for backwards compatibility.
type Vision2SeqResult = EncoderDecoderResult

// =============================================================================
// Configuration Loading
// =============================================================================

// LoadVision2SeqModelConfig loads and parses configuration for a Vision2Seq model.
// This is backend-agnostic and can be used by both ONNX and GoMLX backends.
func LoadVision2SeqModelConfig(modelPath string) (*Vision2SeqModelConfig, error) {
	// Find encoder ONNX file
	// NOTE: vision_encoder.onnx is preferred over encoder_model.onnx because
	// some models (like Florence-2) have both, but encoder_model.onnx expects
	// inputs_embeds (concatenated image+text features) while vision_encoder.onnx
	// takes pixel_values directly and outputs hidden states usable by the decoder.
	encoderPath := FindONNXFile(modelPath, []string{
		"encoder.onnx",
		"vision_encoder.onnx",
		"encoder_model.onnx",
	})

	// Find decoder ONNX file
	decoderPath := FindONNXFile(modelPath, []string{
		"decoder_model_merged.onnx", // Preferred: merged decoder with KV-cache
		"decoder_with_past.onnx",
		"decoder.onnx",
		"decoder_model.onnx",
	})

	// Find split decoder files for XLA compatibility
	// First step decoder (no past KV inputs required)
	decoderFirstStepPath := FindONNXFile(modelPath, []string{
		"decoder_model.onnx", // Standard naming for first step decoder
	})
	// With past decoder (requires past KV inputs)
	decoderWithPastPath := FindONNXFile(modelPath, []string{
		"decoder_with_past_model.onnx",
		"decoder_with_past.onnx",
	})

	// Find embed_tokens ONNX file (decoder-only VLMs like Moondream2)
	embedTokensPath := FindONNXFile(modelPath, []string{"embed_tokens.onnx"})

	// Find projection ONNX file (if present, legacy Moondream2 layouts)
	projectionPath := FindONNXFile(modelPath, []string{"projection.onnx"})

	// Load model configuration from config.json
	rawConfig, err := loadRawVision2SeqConfig(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading model config: %w", err)
	}

	// Load preprocessor config if available
	preprocConfig := loadPreprocessorConfig(modelPath)

	// Build decoder config
	decoderConfig := buildDecoderConfig(rawConfig)

	// Build image config
	imageConfig := buildImageConfig(rawConfig, preprocConfig)

	// Determine architecture - prefer nested decoder config for VisionEncoderDecoder models
	var numLayers, numHeads, hiddenSize int
	if rawConfig.DecoderConfig != nil {
		dec := rawConfig.DecoderConfig
		numLayers = FirstNonZero(dec.DecoderLayers, 6)
		numHeads = FirstNonZero(dec.DecoderAttentionHeads, 8)
		hiddenSize = FirstNonZero(dec.DModel, 768)
	} else if rawConfig.TextConfig != nil {
		// Florence-2 uses text_config for decoder settings
		text := rawConfig.TextConfig
		numLayers = FirstNonZero(text.DecoderLayers, 6)
		numHeads = FirstNonZero(text.DecoderAttentionHeads, 12) // Florence-2 default is 12
		hiddenSize = FirstNonZero(text.DModel, 768)
	} else {
		numLayers = FirstNonZero(rawConfig.DecoderLayers, rawConfig.NumDecoderLayers, rawConfig.NumHiddenLayers, 6)
		numHeads = FirstNonZero(rawConfig.DecoderAttentionHeads, rawConfig.NumAttentionHeads, rawConfig.DecoderNumHeads, 8)
		hiddenSize = FirstNonZero(rawConfig.DecoderHiddenSize, rawConfig.HiddenSize, 768)
	}
	headDim := hiddenSize / numHeads

	return &Vision2SeqModelConfig{
		ModelPath:            modelPath,
		EncoderPath:          encoderPath,
		DecoderPath:          decoderPath,
		DecoderFirstStepPath: decoderFirstStepPath,
		DecoderWithPastPath:  decoderWithPastPath,
		EmbedTokensPath:      embedTokensPath,
		ProjectionPath:       projectionPath,
		DecoderConfig:        decoderConfig,
		ImageConfig:          imageConfig,
		NumLayers:            numLayers,
		NumHeads:             numHeads,
		HeadDim:              headDim,
		HiddenSize:           hiddenSize,
	}, nil
}

// IsVision2SeqModel checks if a model path contains a Vision2Seq model.
// This includes standard encoder-decoder models, encoder-decoder VLMs
// (Florence-2), and decoder-only VLMs (Moondream2).
func IsVision2SeqModel(path string) bool {
	// Check for decoder-only VLM (vision_encoder + embed_tokens, no encoder_model)
	if IsDecoderOnlyVLMModel(path) {
		return true
	}

	// Check for encoder-decoder VLM (vision_encoder + embed_tokens + encoder_model + decoder)
	if IsEncoderDecoderVLMModel(path) {
		return true
	}

	// Standard Vision2Seq (encoder + decoder)
	encoderPath := FindONNXFile(path, []string{
		"encoder.onnx",
		"encoder_model.onnx",
		"vision_encoder.onnx",
	})
	decoderPath := FindONNXFile(path, []string{
		"decoder_model_merged.onnx",
		"decoder_with_past.onnx",
		"decoder.onnx",
		"decoder_model.onnx",
	})
	return encoderPath != "" && decoderPath != ""
}

// =============================================================================
// Raw Config Structs and Parsing Helpers
// =============================================================================

// rawVision2SeqConfig represents the model's config.json structure.
type rawVision2SeqConfig struct {
	// Top-level decoder config (some models use this)
	VocabSize           int   `json:"vocab_size"`
	DecoderStartTokenID int32 `json:"decoder_start_token_id"`
	EOSTokenID          any   `json:"eos_token_id"` // Can be int or []int
	BOSTokenID          int32 `json:"bos_token_id"`
	PadTokenID          int32 `json:"pad_token_id"`
	MaxLength           int   `json:"max_length"`

	// Top-level decoder architecture (some models use this)
	DecoderLayers         int `json:"decoder_layers"`
	DecoderAttentionHeads int `json:"decoder_attention_heads"`
	DecoderHiddenSize     int `json:"d_model"`
	NumDecoderLayers      int `json:"num_decoder_layers"`
	NumHiddenLayers       int `json:"num_hidden_layers"` // GPT-style models (Moondream2/Phi-2)
	NumAttentionHeads     int `json:"num_attention_heads"`
	DecoderNumHeads       int `json:"decoder_num_heads"`
	HiddenSize            int `json:"hidden_size"`

	// Image config (from config.json)
	ImageSize    any       `json:"image_size"`
	DoCenterCrop bool      `json:"do_center_crop"`
	ImageMean    []float32 `json:"image_mean"`
	ImageStd     []float32 `json:"image_std"`
	Size         any       `json:"size"`

	// Nested decoder config (for VisionEncoderDecoder models like TrOCR)
	DecoderConfig *struct {
		VocabSize             int   `json:"vocab_size"`
		DecoderStartTokenID   int32 `json:"decoder_start_token_id"`
		EOSTokenID            any   `json:"eos_token_id"`
		BOSTokenID            int32 `json:"bos_token_id"`
		PadTokenID            int32 `json:"pad_token_id"`
		MaxLength             int   `json:"max_length"`
		DecoderLayers         int   `json:"decoder_layers"`
		DecoderAttentionHeads int   `json:"decoder_attention_heads"`
		DModel                int   `json:"d_model"`
	} `json:"decoder"`

	// Encoder config (for vision models)
	EncoderConfig *struct {
		ImageSize         any `json:"image_size"` // Can be int or []int (e.g., [width, height])
		HiddenSize        int `json:"hidden_size"`
		NumAttentionHeads int `json:"num_attention_heads"`
	} `json:"encoder"`

	// Vision config (for some models)
	VisionConfig *struct {
		ImageSize int `json:"image_size"`
	} `json:"vision_config"`

	// Text config (for Florence-2 and similar models)
	TextConfig *struct {
		VocabSize             int   `json:"vocab_size"`
		DecoderStartTokenID   int32 `json:"decoder_start_token_id"`
		EOSTokenID            any   `json:"eos_token_id"`
		BOSTokenID            int32 `json:"bos_token_id"`
		PadTokenID            int32 `json:"pad_token_id"`
		MaxLength             int   `json:"max_length"`
		DecoderLayers         int   `json:"decoder_layers"`
		DecoderAttentionHeads int   `json:"decoder_attention_heads"`
		DModel                int   `json:"d_model"`
	} `json:"text_config"`
}

// rawPreprocessorConfig represents preprocessor_config.json
type rawPreprocessorConfig struct {
	ImageMean     []float32 `json:"image_mean"`
	ImageStd      []float32 `json:"image_std"`
	DoNormalize   bool      `json:"do_normalize"`
	DoCenterCrop  bool      `json:"do_center_crop"`
	RescaleFactor float32   `json:"rescale_factor"`
	Size          any       `json:"size"`
	CropSize      any       `json:"crop_size"`
}

// loadRawVision2SeqConfig loads the model configuration from config.json.
// Some models (e.g., Moondream2) nest architecture parameters under a
// model-specific key (phi_config, llm_config, etc.) instead of at the top level.
// This function promotes those nested fields so the rest of the config pipeline
// can read them from standard top-level positions.
func loadRawVision2SeqConfig(path string) (*rawVision2SeqConfig, error) {
	configPath := filepath.Join(path, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config.json: %w", err)
	}

	// First pass: promote architecture fields from nested config objects.
	// Models like Moondream2 store decoder parameters under "phi_config",
	// while other models may use "llm_config", "language_config", etc.
	// We detect any nested object containing standard architecture fields
	// (hidden_size, num_hidden_layers, etc.) and merge them into the top level.
	data = promoteNestedArchConfig(data)

	var config rawVision2SeqConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config.json: %w", err)
	}

	return &config, nil
}

// architectureFields are the standard field names that identify a nested
// config object as containing model architecture parameters.
var architectureFields = []string{
	"hidden_size", "num_hidden_layers", "num_attention_heads", "vocab_size",
}

// promoteNestedArchConfig finds nested JSON objects containing architecture
// fields and promotes those fields to the top level. This handles models that
// nest their decoder/LLM config under a model-specific key (phi_config,
// llm_config, language_config, etc.) without requiring explicit knowledge
// of each key name.
func promoteNestedArchConfig(data []byte) []byte {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return data
	}

	// Check if top-level already has architecture fields
	if _, hasHiddenSize := raw["hidden_size"]; hasHiddenSize {
		return data
	}
	if _, hasNumLayers := raw["num_hidden_layers"]; hasNumLayers {
		return data
	}

	// Look for a nested object containing architecture fields
	for key, val := range raw {
		// Skip known non-architecture keys
		switch key {
		case "decoder", "encoder", "text_config", "vision_config", "decoder_config", "encoder_config":
			continue
		}

		var nested map[string]json.RawMessage
		if err := json.Unmarshal(val, &nested); err != nil {
			continue
		}

		// Count how many architecture fields this nested object has
		matches := 0
		for _, field := range architectureFields {
			if _, ok := nested[field]; ok {
				matches++
			}
		}

		// Require at least 2 matching fields to avoid false positives
		if matches < 2 {
			continue
		}

		// Promote nested fields to the top level (don't overwrite existing)
		for nestedKey, nestedVal := range nested {
			if _, exists := raw[nestedKey]; !exists {
				raw[nestedKey] = nestedVal
			}
		}

		// Only promote from the first matching nested config
		break
	}

	promoted, err := json.Marshal(raw)
	if err != nil {
		return data
	}
	return promoted
}

// loadPreprocessorConfig loads preprocessor_config.json if it exists.
func loadPreprocessorConfig(path string) *rawPreprocessorConfig {
	configPath := filepath.Join(path, "preprocessor_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil
	}

	var config rawPreprocessorConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil
	}

	return &config
}

// buildDecoderConfig creates a DecoderConfig from the raw config.
func buildDecoderConfig(cfg *rawVision2SeqConfig) *backends.DecoderConfig {
	// Try nested decoder config first (for VisionEncoderDecoder models like TrOCR)
	if cfg.DecoderConfig != nil {
		dec := cfg.DecoderConfig

		eosTokenID := ParseTokenID(dec.EOSTokenID)

		maxLength := dec.MaxLength
		if maxLength == 0 {
			maxLength = 512
		}

		numHeads := FirstNonZero(dec.DecoderAttentionHeads, 8)
		hiddenSize := FirstNonZero(dec.DModel, 768)

		return &backends.DecoderConfig{
			VocabSize:           dec.VocabSize,
			MaxLength:           maxLength,
			EOSTokenID:          eosTokenID,
			BOSTokenID:          dec.BOSTokenID,
			PadTokenID:          dec.PadTokenID,
			DecoderStartTokenID: dec.DecoderStartTokenID,
			NumLayers:           FirstNonZero(dec.DecoderLayers, 6),
			NumHeads:            numHeads,
			HeadDim:             hiddenSize / numHeads,
		}
	}

	// Try text_config for Florence-2 style models
	if cfg.TextConfig != nil {
		text := cfg.TextConfig

		eosTokenID := ParseTokenID(text.EOSTokenID)

		maxLength := text.MaxLength
		if maxLength == 0 {
			maxLength = 512
		}

		numHeads := FirstNonZero(text.DecoderAttentionHeads, 12) // Florence-2 default is 12
		hiddenSize := FirstNonZero(text.DModel, 768)

		return &backends.DecoderConfig{
			VocabSize:           text.VocabSize,
			MaxLength:           maxLength,
			EOSTokenID:          eosTokenID,
			BOSTokenID:          text.BOSTokenID,
			PadTokenID:          text.PadTokenID,
			DecoderStartTokenID: text.DecoderStartTokenID,
			NumLayers:           FirstNonZero(text.DecoderLayers, 6),
			NumHeads:            numHeads,
			HeadDim:             hiddenSize / numHeads,
		}
	}

	// Fall back to top-level fields
	eosTokenID := ParseTokenID(cfg.EOSTokenID)

	maxLength := cfg.MaxLength
	if maxLength == 0 {
		maxLength = 512
	}

	numHeads := FirstNonZero(cfg.DecoderAttentionHeads, cfg.NumAttentionHeads, cfg.DecoderNumHeads, 8)
	hiddenSize := FirstNonZero(cfg.DecoderHiddenSize, cfg.HiddenSize, 768)

	// Fall back to BOS token if decoder_start_token_id is not set (e.g., Moondream2/Phi-2)
	decoderStartTokenID := cfg.DecoderStartTokenID
	if decoderStartTokenID == 0 && cfg.BOSTokenID != 0 {
		decoderStartTokenID = cfg.BOSTokenID
	}

	return &backends.DecoderConfig{
		VocabSize:           cfg.VocabSize,
		MaxLength:           maxLength,
		EOSTokenID:          eosTokenID,
		BOSTokenID:          cfg.BOSTokenID,
		PadTokenID:          cfg.PadTokenID,
		DecoderStartTokenID: decoderStartTokenID,
		NumLayers:           FirstNonZero(cfg.DecoderLayers, cfg.NumDecoderLayers, cfg.NumHiddenLayers, 6),
		NumHeads:            numHeads,
		HeadDim:             hiddenSize / numHeads,
	}
}

// buildImageConfig creates an ImageConfig from the raw configs.
func buildImageConfig(cfg *rawVision2SeqConfig, preproc *rawPreprocessorConfig) *backends.ImageConfig {
	// Merge preprocessor config if available
	var imageMean, imageStd []float32
	var doCenterCrop bool
	var rescaleFactor float32
	var sizeField any

	if preproc != nil {
		imageMean = preproc.ImageMean
		imageStd = preproc.ImageStd
		doCenterCrop = preproc.DoCenterCrop
		rescaleFactor = preproc.RescaleFactor
		sizeField = preproc.Size
	}

	// Fall back to config.json values
	if len(imageMean) == 0 {
		imageMean = cfg.ImageMean
	}
	if len(imageStd) == 0 {
		imageStd = cfg.ImageStd
	}
	if !doCenterCrop {
		doCenterCrop = cfg.DoCenterCrop
	}
	if sizeField == nil {
		sizeField = cfg.Size
	}

	// Determine image size
	imageSize := 224 // default
	if size := extractImageSize(cfg.ImageSize); size > 0 {
		imageSize = size
	} else if size := extractImageSize(sizeField); size > 0 {
		imageSize = size
	} else if cfg.EncoderConfig != nil {
		if size := extractImageSize(cfg.EncoderConfig.ImageSize); size > 0 {
			imageSize = size
		}
	} else if cfg.VisionConfig != nil && cfg.VisionConfig.ImageSize > 0 {
		imageSize = cfg.VisionConfig.ImageSize
	}

	// Default normalization values (ImageNet)
	mean := [3]float32{0.485, 0.456, 0.406}
	std := [3]float32{0.229, 0.224, 0.225}
	if len(imageMean) == 3 {
		copy(mean[:], imageMean)
	}
	if len(imageStd) == 3 {
		copy(std[:], imageStd)
	}

	if rescaleFactor == 0 {
		rescaleFactor = 1.0 / 255.0
	}

	return &backends.ImageConfig{
		Width:         imageSize,
		Height:        imageSize,
		Channels:      3,
		Mean:          mean,
		Std:           std,
		RescaleFactor: rescaleFactor,
		DoCenterCrop:  doCenterCrop,
	}
}

// extractImageSize extracts an integer size from various JSON formats.
func extractImageSize(v any) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case []any:
		// Handle array like [width, height] - return the first element (width)
		if len(val) > 0 {
			if f, ok := val[0].(float64); ok {
				return int(f)
			}
		}
	case map[string]any:
		// Handle {height: N, width: N} or {shortest_edge: N}
		if h, ok := val["height"].(float64); ok {
			return int(h)
		}
		if se, ok := val["shortest_edge"].(float64); ok {
			return int(se)
		}
	}
	return 0
}

// =============================================================================
// Model Management (Vision Encoder + Decoder)
// =============================================================================

// Ensure vision2SeqModel implements backends.Model
var _ backends.Model = (*vision2SeqModel)(nil)

// vision2SeqModel implements backends.Model for image-to-text tasks.
// It uses separate encoder and decoder sessions (TrOCR, Donut, etc.).
// For XLA compatibility, it can use split decoders to avoid 0-dimension tensors.
type vision2SeqModel struct {
	config *Vision2SeqModelConfig

	encoderSession backends.Session
	decoderSession backends.Session // Main decoder (merged or with-past)

	// Optional split decoder sessions for XLA compatibility.
	decoderFirstStepSession backends.Session // First step (no past KV inputs)
	decoderWithPastSession  backends.Session // Subsequent steps (with past KV inputs)
	useSplitDecoders        bool             // True if split decoders are loaded

	backendType backends.BackendType
}

// NewVision2SeqModel creates a Model from encoder and decoder sessions.
func NewVision2SeqModel(
	config *Vision2SeqModelConfig,
	encoderSession backends.Session,
	decoderSession backends.Session,
	backendType backends.BackendType,
) backends.Model {
	return &vision2SeqModel{
		config:         config,
		encoderSession: encoderSession,
		decoderSession: decoderSession,
		backendType:    backendType,
	}
}

// LoadVision2SeqModel loads a vision2seq Model using the given session factory.
// It automatically discovers encoder and decoder ONNX files and creates sessions.
func LoadVision2SeqModel(modelPath string, factory backends.SessionFactory, opts ...backends.SessionOption) (backends.Model, error) {
	// Load configuration
	config, err := LoadVision2SeqModelConfig(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading model config: %w", err)
	}

	if config.EncoderPath == "" {
		return nil, fmt.Errorf("encoder ONNX file not found in %s", modelPath)
	}
	if config.DecoderPath == "" {
		return nil, fmt.Errorf("decoder ONNX file not found in %s", modelPath)
	}

	// Create encoder session
	encoderSession, err := factory.CreateSession(config.EncoderPath, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating encoder session: %w", err)
	}

	// Create decoder session (main/merged decoder)
	decoderSession, err := factory.CreateSession(config.DecoderPath, opts...)
	if err != nil {
		_ = encoderSession.Close()
		return nil, fmt.Errorf("creating decoder session: %w", err)
	}

	model := &vision2SeqModel{
		config:         config,
		encoderSession: encoderSession,
		decoderSession: decoderSession,
		backendType:    factory.Backend(),
	}

	// Try to load split decoders for XLA compatibility.
	// These avoid 0-dimension tensors on the first decoder step.
	if config.DecoderFirstStepPath != "" && config.DecoderWithPastPath != "" {
		firstStepSession, err := factory.CreateSession(config.DecoderFirstStepPath, opts...)
		if err == nil {
			withPastSession, err := factory.CreateSession(config.DecoderWithPastPath, opts...)
			if err == nil {
				model.decoderFirstStepSession = firstStepSession
				model.decoderWithPastSession = withPastSession
				model.useSplitDecoders = true
			} else {
				// Clean up first step session if with-past failed
				_ = firstStepSession.Close()
			}
		}
	}

	return model, nil
}

// Forward runs encoder or decoder based on inputs.
// - If ImagePixels is set (and EncoderOutput is nil): runs vision encoder, returns EncoderOutput
// - If EncoderOutput is set: runs decoder step, returns Logits and PastKeyValues
func (m *vision2SeqModel) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	if inputs == nil {
		return nil, fmt.Errorf("nil inputs")
	}

	// If encoder output provided, run decoder
	if inputs.EncoderOutput != nil {
		return m.runDecoder(ctx, inputs)
	}

	// Otherwise run encoder on image
	if len(inputs.ImagePixels) == 0 {
		return nil, fmt.Errorf("no image pixels or encoder output provided")
	}

	return m.runEncoder(ctx, inputs)
}

// runEncoder encodes preprocessed image pixels into encoder hidden states.
func (m *vision2SeqModel) runEncoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	// Create input tensor [batch, channels, height, width]
	input := backends.NamedTensor{
		Name:  m.getEncoderInputName(),
		Shape: []int64{int64(inputs.ImageBatch), int64(inputs.ImageChannels), int64(inputs.ImageHeight), int64(inputs.ImageWidth)},
		Data:  inputs.ImagePixels,
	}

	// Run encoder
	outputs, err := m.encoderSession.Run([]backends.NamedTensor{input})
	if err != nil {
		return nil, fmt.Errorf("running encoder: %w", err)
	}

	if len(outputs) == 0 {
		return nil, fmt.Errorf("no encoder output")
	}

	// Extract encoder output (typically "last_hidden_state" or first output)
	output := outputs[0]
	if len(output.Shape) < 3 {
		return nil, fmt.Errorf("unexpected encoder output shape: %v", output.Shape)
	}

	// Extract data
	hiddenStates, ok := output.Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("encoder output is not float32")
	}

	encoderOutput := &backends.EncoderOutput{
		HiddenStates: hiddenStates,
		Shape:        [3]int{int(output.Shape[0]), int(output.Shape[1]), int(output.Shape[2])},
	}

	return &backends.ModelOutput{
		EncoderOutput: encoderOutput,
	}, nil
}

// getEncoderInputName returns the expected input name for the encoder.
func (m *vision2SeqModel) getEncoderInputName() string {
	// Check session input info for the actual name
	inputInfo := m.encoderSession.InputInfo()
	if len(inputInfo) > 0 {
		return inputInfo[0].Name
	}
	// Default name for vision encoders
	return "pixel_values"
}

// runDecoder performs one step of autoregressive decoding.
func (m *vision2SeqModel) runDecoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	inputIDs := inputs.InputIDs
	encoderOutput := inputs.EncoderOutput
	pastKeyValues := inputs.PastKeyValues

	batchSize := len(inputIDs)
	if batchSize == 0 {
		return nil, fmt.Errorf("empty input")
	}

	seqLen := len(inputIDs[0])

	// Flatten input IDs to int64 for most models
	flatInputIDs := make([]int64, batchSize*seqLen)
	for i := range batchSize {
		for j := range seqLen {
			flatInputIDs[i*seqLen+j] = int64(inputIDs[i][j])
		}
	}

	// Choose the appropriate decoder session:
	// - Use split decoders if available (for XLA compatibility)
	// - Use first step decoder if no past KV cache
	// - Use with-past decoder for subsequent steps
	var decoderSession backends.Session
	isFirstStep := pastKeyValues == nil || pastKeyValues.SeqLen == 0

	if m.useSplitDecoders {
		if isFirstStep {
			decoderSession = m.decoderFirstStepSession
		} else {
			decoderSession = m.decoderWithPastSession
		}
	} else {
		decoderSession = m.decoderSession
	}

	// Build decoder inputs using the selected session
	tensorInputs, err := m.buildDecoderInputsForSession(decoderSession, flatInputIDs, batchSize, seqLen, encoderOutput, pastKeyValues)
	if err != nil {
		return nil, fmt.Errorf("building decoder inputs: %w", err)
	}

	// Run decoder
	outputs, err := decoderSession.Run(tensorInputs)
	if err != nil {
		return nil, fmt.Errorf("running decoder: %w", err)
	}

	if len(outputs) == 0 {
		return nil, fmt.Errorf("no decoder output")
	}

	// Extract logits (first output)
	logitsOutput := outputs[0]
	logitsData, ok := logitsOutput.Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("logits tensor is not float32")
	}

	logitsShape := logitsOutput.Shape

	// Reshape logits to [batch, vocab_size] (taking last position if sequence)
	vocabSize := int(logitsShape[len(logitsShape)-1])
	logits := make([][]float32, batchSize)
	for i := range batchSize {
		logits[i] = make([]float32, vocabSize)
		// Take logits from last position
		startIdx := i*seqLen*vocabSize + (seqLen-1)*vocabSize
		copy(logits[i], logitsData[startIdx:startIdx+vocabSize])
	}

	// Extract updated KV-cache from outputs (if present)
	newKVCache := m.extractKVCache(outputs, batchSize, pastKeyValues)

	return &backends.ModelOutput{
		Logits:        logits,
		PastKeyValues: newKVCache,
	}, nil
}

// buildDecoderInputsForSession creates the input tensors for the specified decoder session.
// This allows using different sessions for first step (no KV-cache) and subsequent steps.
func (m *vision2SeqModel) buildDecoderInputsForSession(session backends.Session, inputIDs []int64, batchSize, seqLen int, encoderOutput *backends.EncoderOutput, pastKV *backends.KVCache) ([]backends.NamedTensor, error) {
	return buildDecoderInputs(session, inputIDs, batchSize, seqLen, encoderOutput, pastKV, m.config.NumHeads, m.config.HeadDim)
}

// buildDecoderInputs creates input tensors for a decoder session based on the
// session's declared input requirements. It handles input_ids, encoder hidden
// states (under various names: encoder_hidden_states, encoder_outputs, or
// inputs_embeds), attention masks, use_cache_branch, and past key/value tensors.
//
// For models without KV-cache support (like Moondream2's Phi-2 decoder),
// pass nil for pastKV. The function only creates KV tensors for inputs that
// the session actually expects.
func buildDecoderInputs(
	session backends.Session,
	inputIDs []int64,
	batchSize, seqLen int,
	encoderOutput *backends.EncoderOutput,
	pastKV *backends.KVCache,
	numHeads, headDim int,
) ([]backends.NamedTensor, error) {
	var inputs []backends.NamedTensor

	// Get decoder input names from the specified session
	inputInfo := session.InputInfo()
	inputNames := make(map[string]bool)
	for _, info := range inputInfo {
		inputNames[info.Name] = true
	}

	// Add input_ids
	inputs = append(inputs, backends.NamedTensor{
		Name:  GetDecoderInputIDsName(inputNames),
		Shape: []int64{int64(batchSize), int64(seqLen)},
		Data:  inputIDs,
	})

	// Add encoder hidden states under whichever name the session expects.
	// Standard models use encoder_hidden_states or encoder_outputs;
	// Moondream2 uses inputs_embeds as a fallback.
	if inputNames["encoder_hidden_states"] || inputNames["encoder_outputs"] || inputNames["inputs_embeds"] {
		name := "encoder_hidden_states"
		if inputNames["encoder_outputs"] && !inputNames["encoder_hidden_states"] {
			name = "encoder_outputs"
		}
		if inputNames["inputs_embeds"] && !inputNames["encoder_hidden_states"] && !inputNames["encoder_outputs"] {
			name = "inputs_embeds"
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  name,
			Shape: []int64{int64(encoderOutput.Shape[0]), int64(encoderOutput.Shape[1]), int64(encoderOutput.Shape[2])},
			Data:  encoderOutput.HiddenStates,
		})
	}

	// Add decoder attention mask if the session expects it
	if inputNames["attention_mask"] {
		mask := make([]int64, batchSize*seqLen)
		for i := range mask {
			mask[i] = 1
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "attention_mask",
			Shape: []int64{int64(batchSize), int64(seqLen)},
			Data:  mask,
		})
	}

	// Add encoder attention mask if needed
	if inputNames["encoder_attention_mask"] {
		encSeqLen := encoderOutput.Shape[1]
		mask := make([]int64, batchSize*encSeqLen)
		for i := range mask {
			mask[i] = 1
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "encoder_attention_mask",
			Shape: []int64{int64(batchSize), int64(encSeqLen)},
			Data:  mask,
		})
	}

	// Add use_cache_branch if needed
	if inputNames["use_cache_branch"] {
		var useCacheDataType = backends.DataTypeBool
		for _, info := range inputInfo {
			if info.Name == "use_cache_branch" {
				useCacheDataType = info.DataType
				break
			}
		}

		useCacheVal := pastKV != nil && pastKV.SeqLen > 0
		if useCacheDataType == backends.DataTypeFloat32 {
			useCache := []float32{0}
			if useCacheVal {
				useCache[0] = 1
			}
			inputs = append(inputs, backends.NamedTensor{
				Name:  "use_cache_branch",
				Shape: []int64{1},
				Data:  useCache,
			})
		} else {
			inputs = append(inputs, backends.NamedTensor{
				Name:  "use_cache_branch",
				Shape: []int64{1},
				Data:  []bool{useCacheVal},
			})
		}
	}

	// Add past_key_values inputs if the session expects them.
	// For models without KV-cache (like Moondream2), no past_key_values
	// inputs will be declared and this loop is a no-op.
	encoderSeqLen := encoderOutput.Shape[1]
	if numHeads == 0 {
		numHeads = 8
	}
	if headDim == 0 {
		headDim = 64
	}
	for _, info := range inputInfo {
		if IsPastKeyValueInput(info.Name) {
			var kvSeqLen int
			if pastKV != nil && pastKV.SeqLen > 0 {
				if isEncoderKVTensor(info.Name) {
					kvSeqLen = encoderSeqLen
				} else {
					kvSeqLen = pastKV.SeqLen
				}
			}
			size := batchSize * numHeads * kvSeqLen * headDim
			inputs = append(inputs, backends.NamedTensor{
				Name:  info.Name,
				Shape: []int64{int64(batchSize), int64(numHeads), int64(kvSeqLen), int64(headDim)},
				Data:  make([]float32, size),
			})
		}
	}

	return inputs, nil
}

// isEncoderKVTensor returns true if the tensor name indicates it's for encoder cross-attention.
func isEncoderKVTensor(name string) bool {
	// Encoder KV tensors typically have ".encoder." in their name
	// e.g., "past_key_values.0.encoder.key" vs "past_key_values.0.decoder.key"
	return strings.Contains(name, ".encoder.")
}

// extractKVCache extracts the KV-cache from decoder outputs.
func (m *vision2SeqModel) extractKVCache(outputs []backends.NamedTensor, batchSize int, pastKV *backends.KVCache) *backends.KVCache {
	// For now, return nil - full implementation would extract present_key_values
	// from the output tensors and build a KVCache struct
	// This is a simplified implementation that doesn't use KV-cache
	return nil
}

// DecoderConfig returns configuration needed for generation.
func (m *vision2SeqModel) DecoderConfig() *backends.DecoderConfig {
	return m.config.DecoderConfig
}

// ImageConfig returns configuration for image preprocessing.
func (m *vision2SeqModel) ImageConfig() *backends.ImageConfig {
	return m.config.ImageConfig
}

// Close releases resources associated with the model.
func (m *vision2SeqModel) Close() error {
	var errs []error

	if m.encoderSession != nil {
		if err := m.encoderSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing encoder: %w", err))
		}
		m.encoderSession = nil
	}

	if m.decoderSession != nil {
		if err := m.decoderSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing decoder: %w", err))
		}
		m.decoderSession = nil
	}

	// Close split decoder sessions if loaded
	if m.decoderFirstStepSession != nil {
		if err := m.decoderFirstStepSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing first step decoder: %w", err))
		}
		m.decoderFirstStepSession = nil
	}

	if m.decoderWithPastSession != nil {
		if err := m.decoderWithPastSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing with-past decoder: %w", err))
		}
		m.decoderWithPastSession = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing model: %v", errs)
	}
	return nil
}

// Name returns the model name for logging and debugging.
func (m *vision2SeqModel) Name() string {
	return m.config.ModelPath
}

// Backend returns the backend type this model uses.
func (m *vision2SeqModel) Backend() backends.BackendType {
	return m.backendType
}

// =============================================================================
// Pipeline
// =============================================================================

// Vision2SeqPipeline handles image-to-text tasks like OCR and document understanding.
// It combines image preprocessing, vision encoding, and autoregressive text generation.
type Vision2SeqPipeline struct {
	// EncoderDecoderPipeline provides shared generation functionality.
	*EncoderDecoderPipeline

	// ImageProcessor handles image preprocessing.
	ImageProcessor *ImageProcessor
}

// NewVision2SeqPipeline creates a new Vision2SeqPipeline.
func NewVision2SeqPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	config *Vision2SeqConfig,
) *Vision2SeqPipeline {
	if config == nil {
		config = &Vision2SeqConfig{}
	}

	// Resolve image config
	imageConfig := ResolveImageConfig(model, config.ImageConfig)

	// Create base encoder-decoder pipeline
	base := NewEncoderDecoderPipeline(model, tokenizer, config.GenerationConfig)

	return &Vision2SeqPipeline{
		EncoderDecoderPipeline: base,
		ImageProcessor:         NewImageProcessor(imageConfig),
	}
}

// Run processes an image and generates text.
func (p *Vision2SeqPipeline) Run(ctx context.Context, img image.Image) (*Vision2SeqResult, error) {
	return p.RunWithPrompt(ctx, img, "")
}

// RunWithPrompt processes an image with an optional text prompt.
// The prompt can be used for task-conditioned generation (e.g., Florence-2).
func (p *Vision2SeqPipeline) RunWithPrompt(ctx context.Context, img image.Image, prompt string) (*Vision2SeqResult, error) {
	// Encode image (with prompt tokens for Florence-2)
	encoderOutput, err := p.encodeImageWithPrompt(ctx, img, prompt)
	if err != nil {
		return nil, err
	}

	// Get start tokens
	// For Florence-2, the prompt is already in the encoder, so just use decoder start token
	// For other models, the prompt goes to the decoder
	var startTokens []int32
	if p.isEncoderDecoderVLMModel() {
		startTokens = []int32{p.DecoderConfig.DecoderStartTokenID}
	} else {
		startTokens = p.GetStartTokens(prompt)
	}

	// Generate using shared base
	return p.GenerateFromEncoderOutput(ctx, encoderOutput, startTokens)
}

// RunBytes processes an image from bytes and generates text.
func (p *Vision2SeqPipeline) RunBytes(ctx context.Context, data []byte) (*Vision2SeqResult, error) {
	// Encode image from bytes
	encoderOutput, err := p.encodeImageBytes(ctx, data)
	if err != nil {
		return nil, err
	}

	// Get start tokens (no prompt)
	startTokens := p.GetStartTokens("")

	// Generate using shared base
	return p.GenerateFromEncoderOutput(ctx, encoderOutput, startTokens)
}

// RunBatch processes multiple images and generates text for each.
func (p *Vision2SeqPipeline) RunBatch(ctx context.Context, images []image.Image) ([]*Vision2SeqResult, error) {
	results := make([]*Vision2SeqResult, len(images))

	// Process images one at a time for now
	// TODO: Batch processing with proper batched encoder/decoder
	for i, img := range images {
		result, err := p.Run(ctx, img)
		if err != nil {
			return nil, fmt.Errorf("processing image %d: %w", i, err)
		}
		results[i] = result
	}

	return results, nil
}

// RunWithStreaming processes an image and streams generated tokens.
// The callback is called for each generated token. Return false to stop generation.
func (p *Vision2SeqPipeline) RunWithStreaming(
	ctx context.Context,
	img image.Image,
	prompt string,
	callback func(token int32, text string) bool,
) (*Vision2SeqResult, error) {
	// Encode image
	encoderOutput, err := p.encodeImage(ctx, img)
	if err != nil {
		return nil, err
	}

	// Get start tokens
	startTokens := p.GetStartTokens(prompt)

	// Generate with streaming using shared base
	return p.GenerateFromEncoderOutputStreaming(ctx, encoderOutput, startTokens, callback)
}

// encodeImage preprocesses and encodes an image.
func (p *Vision2SeqPipeline) encodeImage(ctx context.Context, img image.Image) (*backends.EncoderOutput, error) {
	// Preprocess image
	pixels, err := p.ImageProcessor.Process(img)
	if err != nil {
		return nil, fmt.Errorf("preprocessing image: %w", err)
	}

	return p.encodePixels(ctx, pixels)
}

// encodeImageBytes preprocesses and encodes an image from bytes.
func (p *Vision2SeqPipeline) encodeImageBytes(ctx context.Context, data []byte) (*backends.EncoderOutput, error) {
	// Preprocess image from bytes
	pixels, err := p.ImageProcessor.ProcessBytes(data)
	if err != nil {
		return nil, fmt.Errorf("preprocessing image: %w", err)
	}

	return p.encodePixels(ctx, pixels)
}

// encodePixels runs the encoder on preprocessed image pixels.
func (p *Vision2SeqPipeline) encodePixels(ctx context.Context, pixels []float32) (*backends.EncoderOutput, error) {
	return p.encodePixelsWithPrompt(ctx, pixels, nil)
}

// encodePixelsWithPrompt runs the encoder on preprocessed image pixels with optional prompt tokens.
// For Florence-2 models, the prompt tokens are embedded alongside the image in the encoder.
func (p *Vision2SeqPipeline) encodePixelsWithPrompt(ctx context.Context, pixels []float32, promptTokenIDs [][]int32) (*backends.EncoderOutput, error) {
	cfg := p.ImageProcessor.Config
	batchSize := 1

	// Encode image using Forward with image inputs
	encodeOutput, err := p.Model.Forward(ctx, &backends.ModelInputs{
		ImagePixels:   pixels,
		ImageBatch:    batchSize,
		ImageChannels: cfg.Channels,
		ImageHeight:   cfg.Height,
		ImageWidth:    cfg.Width,
		InputIDs:      promptTokenIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("encoding image: %w", err)
	}

	return encodeOutput.EncoderOutput, nil
}

// encodeImageWithPrompt preprocesses and encodes an image with an optional prompt.
// For Florence-2 models, the prompt is embedded alongside the image in the encoder.
func (p *Vision2SeqPipeline) encodeImageWithPrompt(ctx context.Context, img image.Image, prompt string) (*backends.EncoderOutput, error) {
	// Preprocess image
	pixels, err := p.ImageProcessor.Process(img)
	if err != nil {
		return nil, fmt.Errorf("preprocessing image: %w", err)
	}

	// Tokenize prompt if provided and this is a Florence-2 model
	var promptTokenIDs [][]int32
	if prompt != "" && p.isEncoderDecoderVLMModel() {
		tokens := p.Tokenizer.Encode(prompt)
		promptTokenIDs = [][]int32{IntToInt32(tokens)}
	}

	return p.encodePixelsWithPrompt(ctx, pixels, promptTokenIDs)
}

// isEncoderDecoderVLMModel checks if the underlying model is an encoder-decoder VLM.
func (p *Vision2SeqPipeline) isEncoderDecoderVLMModel() bool {
	_, ok := p.Model.(*encoderDecoderVLMModel)
	return ok
}

// =============================================================================
// Loader
// =============================================================================

// Vision2SeqPipelineOption is a functional option for configuring Vision2SeqPipeline loading.
type Vision2SeqPipelineOption func(*Vision2SeqConfig)

// WithVision2SeqImageConfig sets the image config for the pipeline.
func WithVision2SeqImageConfig(config *backends.ImageConfig) Vision2SeqPipelineOption {
	return func(c *Vision2SeqConfig) {
		c.ImageConfig = config
	}
}

// WithVision2SeqGenerationConfig sets the generation config for the pipeline.
func WithVision2SeqGenerationConfig(config *backends.GenerationConfig) Vision2SeqPipelineOption {
	return func(c *Vision2SeqConfig) {
		c.GenerationConfig = config
	}
}

// LoadVision2SeqPipeline loads a complete Vision2Seq pipeline from a model directory.
// It automatically loads the model, tokenizer, and creates the pipeline.
// This signature matches the encoder-based pipeline loaders for consistency.
func LoadVision2SeqPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...Vision2SeqPipelineOption,
) (*Vision2SeqPipeline, backends.BackendType, error) {
	// Get session factory from manager
	factory, backendType, err := sessionManager.GetSessionFactoryForModel(modelBackends)
	if err != nil {
		return nil, "", fmt.Errorf("getting session factory: %w", err)
	}

	// Load the model - check for specific architectures
	var model backends.Model
	if IsDecoderOnlyVLMModel(modelPath) {
		// Decoder-only VLM: vision_encoder + embed_tokens + decoder (e.g., Moondream2)
		model, err = LoadDecoderOnlyVLMModel(modelPath, factory)
		if err != nil {
			return nil, "", fmt.Errorf("loading decoder-only VLM model: %w", err)
		}
	} else if IsEncoderDecoderVLMModel(modelPath) {
		// Encoder-decoder VLM: vision_encoder + embed_tokens + encoder_model + decoder
		model, err = LoadEncoderDecoderVLMModel(modelPath, factory)
		if err != nil {
			return nil, "", fmt.Errorf("loading encoder-decoder VLM model: %w", err)
		}
	} else {
		// Standard Vision2Seq: encoder + decoder (TrOCR, Donut, etc.)
		model, err = LoadVision2SeqModel(modelPath, factory)
		if err != nil {
			return nil, "", fmt.Errorf("loading model: %w", err)
		}
	}

	// Load the tokenizer
	tokenizer, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		_ = model.Close()
		return nil, "", fmt.Errorf("loading tokenizer: %w", err)
	}

	// Apply options
	config := &Vision2SeqConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// Create the pipeline
	pipeline := NewVision2SeqPipeline(model, tokenizer, config)

	return pipeline, backendType, nil
}
