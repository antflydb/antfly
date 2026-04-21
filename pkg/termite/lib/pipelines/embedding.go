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
	"bytes"
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

// ============================================================================
// Config Types and Loading
// ============================================================================

// EmbeddingModelConfig holds parsed configuration for an embedding model.
// Supports text-only, image-only, audio-only, or multimodal configurations.
type EmbeddingModelConfig struct {
	// Path to the model directory
	ModelPath string

	// Text encoder (optional - nil if no text encoder found)
	TextEncoderFile string

	// Visual encoder (optional - nil if no visual encoder found)
	VisualEncoderFile string

	// Audio encoder (optional - nil if no audio encoder found)
	AudioEncoderFile string

	// Embedding dimension (projection_dim for CLIP, hidden_size for BERT, etc.)
	EmbeddingDim int

	// Image preprocessing config (for visual encoder)
	ImageConfig *backends.ImageConfig

	// Audio preprocessing config (for audio encoder)
	AudioConfig *backends.AudioConfig

	// Maximum text sequence length (from max_position_embeddings)
	MaxTextLength int

	// Model architecture type (clip, siglip, clap, bert, vit, etc.)
	ModelType string
}

// EmbeddingCapabilities describes what modalities an embedding model supports.
type EmbeddingCapabilities struct {
	Text   bool
	Visual bool
	Audio  bool
}

// HasText returns true if the model supports text.
func (c EmbeddingCapabilities) HasText() bool {
	return c.Text
}

// HasVisual returns true if the model supports visual (images).
func (c EmbeddingCapabilities) HasVisual() bool {
	return c.Visual
}

// HasAudio returns true if the model supports audio.
func (c EmbeddingCapabilities) HasAudio() bool {
	return c.Audio
}

// Capabilities returns the modalities this model supports.
func (c *EmbeddingModelConfig) Capabilities() EmbeddingCapabilities {
	return EmbeddingCapabilities{
		Text:   c.TextEncoderFile != "",
		Visual: c.VisualEncoderFile != "",
		Audio:  c.AudioEncoderFile != "",
	}
}

// HasTextEncoder returns true if the model has a text encoder.
func (c *EmbeddingModelConfig) HasTextEncoder() bool {
	return c.TextEncoderFile != ""
}

// HasVisualEncoder returns true if the model has a visual encoder.
func (c *EmbeddingModelConfig) HasVisualEncoder() bool {
	return c.VisualEncoderFile != ""
}

// HasAudioEncoder returns true if the model has an audio encoder.
func (c *EmbeddingModelConfig) HasAudioEncoder() bool {
	return c.AudioEncoderFile != ""
}

// LoadEmbeddingModelConfig loads and parses configuration for an embedding model.
// It auto-detects text and visual encoders based on file presence.
func LoadEmbeddingModelConfig(modelPath string) (*EmbeddingModelConfig, error) {
	config := &EmbeddingModelConfig{
		ModelPath: modelPath,
	}

	// Detect text encoder
	config.TextEncoderFile = FindONNXFile(modelPath, []string{
		"text_model.onnx",
		"text_model_quantized.onnx",
		"model.onnx", // Generic encoder models
		"encoder.onnx",
	})

	// Detect visual encoder
	config.VisualEncoderFile = FindONNXFile(modelPath, []string{
		"visual_model.onnx",
		"visual_model_quantized.onnx",
		"vision_model.onnx",
		"vision_model_quantized.onnx",
		"image_model.onnx",
	})

	// Detect audio encoder
	config.AudioEncoderFile = FindONNXFile(modelPath, []string{
		"audio_model.onnx",
		"audio_model_quantized.onnx",
		"audio_model_fp16.onnx",
		"audio_encoder.onnx",
	})

	// If we found model.onnx but also have a visual encoder, it's likely a multimodal model
	// where model.onnx is the text encoder. Otherwise, model.onnx is the only encoder.
	if config.TextEncoderFile == filepath.Join(modelPath, "model.onnx") && config.VisualEncoderFile != "" {
		// Keep text encoder as model.onnx for multimodal
	} else if config.TextEncoderFile != "" && config.VisualEncoderFile == "" { //nolint:staticcheck // SA9003: intentionally empty branch for text-only model
	}

	// Load configuration from config.json
	rawConfig, err := loadRawEmbeddingConfig(modelPath)
	if err != nil {
		// Config is optional for some models
		rawConfig = &rawEmbeddingConfig{}
	}

	// Determine model type
	config.ModelType = rawConfig.ModelType
	if config.ModelType == "" {
		config.ModelType = detectEmbeddingModelType(modelPath, config)
	}

	// Extract embedding dimension
	config.EmbeddingDim = FirstNonZero(
		rawConfig.ProjectionDim,
		rawConfig.HiddenSize,
		rawConfig.VisionConfig.ProjectionDim,
		rawConfig.TextConfig.HiddenSize,
		512, // Default for CLIP
	)

	// Extract max text sequence length from config (e.g., CLIP uses 77)
	config.MaxTextLength = FirstNonZero(rawConfig.MaxPositionEmbeddings, rawConfig.TextConfig.MaxPositionEmbeddings)

	// Build image config if we have a visual encoder
	if config.VisualEncoderFile != "" {
		config.ImageConfig = buildEmbeddingImageConfig(rawConfig)
	}

	// Build audio config if we have an audio encoder
	if config.AudioEncoderFile != "" {
		config.AudioConfig = buildCLAPAudioConfig()
	}

	return config, nil
}

// buildCLAPAudioConfig returns audio config for CLAP models.
// CLAP uses 48kHz sample rate, 64 mel bins, and 10 second max length.
// Uses simple normalization (not Whisper-specific).
func buildCLAPAudioConfig() *backends.AudioConfig {
	return &backends.AudioConfig{
		SampleRate:    48000,
		FeatureSize:   64,
		NFft:          1024,
		HopLength:     480,
		ChunkLength:   10,
		NMels:         64,
		PaddingValue:  0.0,
		Normalization: backends.AudioNormSimple,
	}
}

// IsEmbeddingModel checks if a model path contains an embedding model.
// Returns true for text encoders (BERT, etc.), vision encoders (ViT, etc.),
// or multimodal models (CLIP, SigLIP, etc.).
func IsEmbeddingModel(path string) bool {
	config, err := LoadEmbeddingModelConfig(path)
	if err != nil {
		return false
	}
	return config.HasTextEncoder() || config.HasVisualEncoder()
}

// rawEmbeddingConfig represents config.json for embedding models.
type rawEmbeddingConfig struct {
	ModelType             string `json:"model_type"`
	HiddenSize            int    `json:"hidden_size"`
	ProjectionDim         int    `json:"projection_dim"`
	MaxPositionEmbeddings int    `json:"max_position_embeddings"`

	// Vision config (CLIP, SigLIP, etc.)
	VisionConfig struct {
		ImageSize     int `json:"image_size"`
		ProjectionDim int `json:"projection_dim"`
		HiddenSize    int `json:"hidden_size"`
	} `json:"vision_config"`

	// Text config (CLIP, SigLIP, etc.)
	TextConfig struct {
		HiddenSize            int `json:"hidden_size"`
		ProjectionDim         int `json:"projection_dim"`
		MaxPositionEmbeddings int `json:"max_position_embeddings"`
	} `json:"text_config"`

	// Image preprocessing (some models)
	ImageSize int       `json:"image_size"`
	ImageMean []float32 `json:"image_mean"`
	ImageStd  []float32 `json:"image_std"`
}

// loadRawEmbeddingConfig loads config.json for embedding models.
func loadRawEmbeddingConfig(modelPath string) (*rawEmbeddingConfig, error) {
	configPaths := []string{
		filepath.Join(modelPath, "clip_config.json"),
		filepath.Join(modelPath, "config.json"),
	}

	for _, path := range configPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var config rawEmbeddingConfig
		if err := json.Unmarshal(data, &config); err != nil {
			continue
		}

		return &config, nil
	}

	return nil, fmt.Errorf("no config.json found in %s", modelPath)
}

// detectEmbeddingModelType attempts to detect the model type from files and config.
func detectEmbeddingModelType(modelPath string, config *EmbeddingModelConfig) string {
	caps := config.Capabilities()

	// Check for audio multimodal models (CLAP)
	if caps.HasText() && caps.HasAudio() {
		lowerPath := strings.ToLower(modelPath)
		if strings.Contains(lowerPath, "clap") {
			return "clap"
		}
		return "clap" // Default audio multimodal type
	}

	// Check for visual multimodal models (CLIP, SigLIP, etc.)
	if caps.HasText() && caps.HasVisual() {
		// Could be CLIP, SigLIP, ALIGN, etc.
		lowerPath := strings.ToLower(modelPath)
		if strings.Contains(lowerPath, "siglip") {
			return "siglip"
		}
		if strings.Contains(lowerPath, "align") {
			return "align"
		}
		return "clip" // Default multimodal type
	}

	// Check for image-only models
	if config.HasVisualEncoder() && !config.HasTextEncoder() {
		lowerPath := strings.ToLower(modelPath)
		if strings.Contains(lowerPath, "vit") {
			return "vit"
		}
		return "vision_encoder"
	}

	// Check for text-only models
	if config.HasTextEncoder() && !config.HasVisualEncoder() {
		lowerPath := strings.ToLower(modelPath)
		if strings.Contains(lowerPath, "bert") {
			return "bert"
		}
		if strings.Contains(lowerPath, "roberta") {
			return "roberta"
		}
		if strings.Contains(lowerPath, "mpnet") {
			return "mpnet"
		}
		if strings.Contains(lowerPath, "e5") {
			return "e5"
		}
		return "text_encoder"
	}

	return "unknown"
}

// buildEmbeddingImageConfig creates ImageConfig from the raw config.
func buildEmbeddingImageConfig(rawConfig *rawEmbeddingConfig) *backends.ImageConfig {
	imageSize := FirstNonZero(rawConfig.VisionConfig.ImageSize, rawConfig.ImageSize, 224)

	// Default CLIP normalization values
	mean := [3]float32{0.48145466, 0.4578275, 0.40821073}
	std := [3]float32{0.26862954, 0.26130258, 0.27577711}

	if len(rawConfig.ImageMean) == 3 {
		copy(mean[:], rawConfig.ImageMean)
	}
	if len(rawConfig.ImageStd) == 3 {
		copy(std[:], rawConfig.ImageStd)
	}

	return &backends.ImageConfig{
		Width:         imageSize,
		Height:        imageSize,
		Channels:      3,
		Mean:          mean,
		Std:           std,
		RescaleFactor: 1.0 / 255.0,
		DoCenterCrop:  true,
		CropSize:      imageSize,
	}
}

// ============================================================================
// Pipeline Struct and Methods
// ============================================================================

// Ensure EmbeddingPipeline implements backends.FeatureExtractionModel
var _ backends.FeatureExtractionModel = (*EmbeddingPipeline)(nil)

// EmbeddingPipeline wraps a model for generating embeddings from text or images.
// It handles tokenization/image preprocessing, model inference, and optional normalization.
// For text models, use Embed(). For vision models (CLIP visual), use EmbedImages().
type EmbeddingPipeline struct {
	// Model performs inference on tokenized/preprocessed inputs.
	Model backends.Model

	// Projector is an optional projection model (e.g., visual_projection.onnx for CLIP).
	// If set, embeddings are run through this model after extraction.
	Projector backends.Model

	// Tokenizer handles text-to-token conversion (required for text mode).
	Tokenizer tokenizers.Tokenizer

	// ImageProcessor handles image preprocessing (required for image mode).
	ImageProcessor *ImageProcessor

	// AudioProcessor handles audio preprocessing (required for audio mode).
	AudioProcessor *AudioProcessor

	// Config holds pipeline configuration.
	Config *EmbeddingPipelineConfig
}

// EmbeddingPipelineConfig holds configuration for an EmbeddingPipeline.
type EmbeddingPipelineConfig struct {
	// MaxLength is the maximum sequence length (text mode).
	MaxLength int

	// Normalize enables L2 normalization of embeddings.
	Normalize bool

	// Pooling specifies the pooling strategy ("mean", "cls", "max").
	Pooling backends.PoolingStrategy

	// AddSpecialTokens controls whether to add [CLS], [SEP], etc. (text mode).
	AddSpecialTokens bool

	// ImageConfig holds image preprocessing configuration (image mode).
	// If nil, defaults will be used when EmbedImages is called.
	ImageConfig *backends.ImageConfig

	// AudioConfig holds audio preprocessing configuration (audio mode).
	// If nil, defaults will be used when EmbedAudio is called.
	AudioConfig *backends.AudioConfig
}

// DefaultEmbeddingPipelineConfig returns sensible defaults for embedding.
func DefaultEmbeddingPipelineConfig() *EmbeddingPipelineConfig {
	return &EmbeddingPipelineConfig{
		MaxLength:        512,
		Normalize:        true,
		Pooling:          backends.PoolingMean,
		AddSpecialTokens: true,
	}
}

// NewEmbeddingPipeline creates a new EmbeddingPipeline for text embeddings.
func NewEmbeddingPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	config *EmbeddingPipelineConfig,
) *EmbeddingPipeline {
	if config == nil {
		config = DefaultEmbeddingPipelineConfig()
	}
	return &EmbeddingPipeline{
		Model:     model,
		Tokenizer: tokenizer,
		Config:    config,
	}
}

// NewImageEmbeddingPipeline creates a new EmbeddingPipeline for image embeddings.
// Use this for vision encoders like CLIP's visual encoder.
func NewImageEmbeddingPipeline(
	model backends.Model,
	imageConfig *backends.ImageConfig,
	config *EmbeddingPipelineConfig,
) *EmbeddingPipeline {
	if config == nil {
		config = DefaultEmbeddingPipelineConfig()
	}
	if imageConfig == nil {
		imageConfig = backends.DefaultImageConfig()
	}
	config.ImageConfig = imageConfig
	return &EmbeddingPipeline{
		Model:          model,
		ImageProcessor: NewImageProcessor(imageConfig),
		Config:         config,
	}
}

// NewAudioEmbeddingPipeline creates a new EmbeddingPipeline for audio embeddings.
// Use this for audio encoders like CLAP's audio encoder.
func NewAudioEmbeddingPipeline(
	model backends.Model,
	audioConfig *backends.AudioConfig,
	config *EmbeddingPipelineConfig,
) *EmbeddingPipeline {
	if config == nil {
		config = DefaultEmbeddingPipelineConfig()
	}
	if audioConfig == nil {
		audioConfig = buildCLAPAudioConfig()
	}
	config.AudioConfig = audioConfig
	return &EmbeddingPipeline{
		Model:          model,
		AudioProcessor: NewAudioProcessor(audioConfig),
		Config:         config,
	}
}

// NewMultimodalEmbeddingPipeline creates an EmbeddingPipeline that supports both text and images.
// Use this for multimodal models like CLIP that have separate text and visual encoders.
func NewMultimodalEmbeddingPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	imageConfig *backends.ImageConfig,
	config *EmbeddingPipelineConfig,
) *EmbeddingPipeline {
	if config == nil {
		config = DefaultEmbeddingPipelineConfig()
	}
	if imageConfig == nil {
		imageConfig = backends.DefaultImageConfig()
	}
	config.ImageConfig = imageConfig
	return &EmbeddingPipeline{
		Model:          model,
		Tokenizer:      tokenizer,
		ImageProcessor: NewImageProcessor(imageConfig),
		Config:         config,
	}
}

// EmbedBatch generates embeddings for a batch of tokenized inputs.
// Implements backends.FeatureExtractionModel.
func (p *EmbeddingPipeline) EmbedBatch(ctx context.Context, inputs *backends.ModelInputs) ([][]float32, error) {
	// Run model forward pass
	output, err := p.Model.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("forward pass: %w", err)
	}

	// Get embeddings - either from pre-computed or pool from hidden states
	var embeddings [][]float32
	if len(output.Embeddings) > 0 {
		// Model already computed embeddings (pooling done in Forward)
		embeddings = output.Embeddings
	} else if len(output.LastHiddenState) > 0 {
		// Pool hidden states to get embeddings
		embeddings = backends.PoolHiddenStates(
			output.LastHiddenState,
			inputs.AttentionMask,
			p.Config.Pooling,
		)
	} else {
		return nil, fmt.Errorf("model output contains neither embeddings nor hidden states")
	}

	// Apply projection model if present (e.g., CLIP visual_projection)
	if p.Projector != nil {
		embeddings, err = p.applyProjection(ctx, embeddings)
		if err != nil {
			return nil, fmt.Errorf("applying projection: %w", err)
		}
	}

	// Optionally normalize embeddings
	if p.Config.Normalize {
		for i := range embeddings {
			embeddings[i] = backends.NormalizeL2(embeddings[i])
		}
	}

	return embeddings, nil
}

// applyProjection runs embeddings through the projector model (e.g., visual_projection.onnx).
func (p *EmbeddingPipeline) applyProjection(ctx context.Context, embeddings [][]float32) ([][]float32, error) {
	if p.Projector == nil {
		return embeddings, nil
	}

	// Create input for projection model
	// The projection model takes a 2D tensor [batch, hidden_size] and outputs [batch, projection_dim]
	inputs := &backends.ModelInputs{
		Embeddings: embeddings,
	}

	output, err := p.Projector.Forward(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("projection forward: %w", err)
	}

	if len(output.Embeddings) > 0 {
		return output.Embeddings, nil
	}

	return nil, fmt.Errorf("projection model did not return embeddings")
}

// Embed generates embeddings for a batch of text strings.
// This is a convenience method that handles tokenization.
func (p *EmbeddingPipeline) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	inputs := TokenizeTexts(p.Tokenizer, texts, p.Config.MaxLength)
	return p.EmbedBatch(ctx, inputs)
}

// EmbedOne generates an embedding for a single text string.
// Use this for models that only support batch_size=1.
func (p *EmbeddingPipeline) EmbedOne(ctx context.Context, text string) ([]float32, error) {
	// Tokenize single text
	tokens := p.Tokenizer.Encode(text)
	if len(tokens) > p.Config.MaxLength {
		tokens = tokens[:p.Config.MaxLength]
	}
	inputIDs := IntToInt32(tokens)
	seqLen := len(inputIDs)

	// Create attention mask
	attentionMask := make([]int32, seqLen)
	for j := range attentionMask {
		attentionMask[j] = 1
	}

	// Create model inputs for single item
	inputs := &backends.ModelInputs{
		InputIDs:      [][]int32{inputIDs},
		AttentionMask: [][]int32{attentionMask},
	}

	// Run inference
	result, err := p.EmbedBatch(ctx, inputs)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}
	return result[0], nil
}

// EmbedImages generates embeddings for a batch of images.
// Use this for vision encoders like CLIP's visual encoder.
// The pipeline must have an ImageProcessor configured (use NewImageEmbeddingPipeline
// or NewMultimodalEmbeddingPipeline).
func (p *EmbeddingPipeline) EmbedImages(ctx context.Context, images []image.Image) ([][]float32, error) {
	if len(images) == 0 {
		return nil, nil
	}

	if p.ImageProcessor == nil {
		return nil, fmt.Errorf("EmbedImages requires an ImageProcessor; use NewImageEmbeddingPipeline or NewMultimodalEmbeddingPipeline")
	}

	// Check if the model supports dynamic batching by inspecting input shapes.
	// If the batch dimension (first axis) is fixed, process images one at a time.
	if !modelSupportsBatching(p.Model) && len(images) > 1 {
		allEmbeddings := make([][]float32, len(images))
		for i, img := range images {
			emb, err := p.EmbedImages(ctx, []image.Image{img})
			if err != nil {
				return nil, fmt.Errorf("embedding image %d: %w", i, err)
			}
			allEmbeddings[i] = emb[0]
		}
		return allEmbeddings, nil
	}

	// Preprocess all images
	pixels, err := p.ImageProcessor.ProcessBatch(images)
	if err != nil {
		return nil, fmt.Errorf("preprocessing images: %w", err)
	}

	// Create model inputs with image data
	cfg := p.ImageProcessor.Config
	inputs := &backends.ModelInputs{
		ImagePixels:   pixels,
		ImageBatch:    len(images),
		ImageChannels: cfg.Channels,
		ImageHeight:   cfg.Height,
		ImageWidth:    cfg.Width,
	}

	return p.EmbedBatch(ctx, inputs)
}

// EmbedImageBytes generates embeddings for images provided as byte slices.
// Convenience method that decodes images before embedding.
func (p *EmbeddingPipeline) EmbedImageBytes(ctx context.Context, imageData [][]byte) ([][]float32, error) {
	if len(imageData) == 0 {
		return nil, nil
	}

	if p.ImageProcessor == nil {
		return nil, fmt.Errorf("EmbedImageBytes requires an ImageProcessor; use NewImageEmbeddingPipeline or NewMultimodalEmbeddingPipeline")
	}

	// Decode all images
	images := make([]image.Image, len(imageData))
	for i, data := range imageData {
		img, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("decoding image %d: %w", i, err)
		}
		images[i] = img
	}

	return p.EmbedImages(ctx, images)
}

// EmbedAudio generates embeddings for a batch of audio files.
// Use this for audio encoders like CLAP's audio encoder.
// The pipeline must have an AudioProcessor configured (use NewAudioEmbeddingPipeline).
func (p *EmbeddingPipeline) EmbedAudio(ctx context.Context, audioData [][]byte) ([][]float32, error) {
	if len(audioData) == 0 {
		return nil, nil
	}

	if p.AudioProcessor == nil {
		return nil, fmt.Errorf("EmbedAudio requires an AudioProcessor; use NewAudioEmbeddingPipeline")
	}

	// Process all audio files and collect mel spectrograms
	// For now, process one at a time since AudioProcessor.Process expects single audio
	results := make([][]float32, len(audioData))
	for i, data := range audioData {
		// Process audio to mel spectrogram
		melSpec, numFrames, err := p.AudioProcessor.Process(data)
		if err != nil {
			return nil, fmt.Errorf("preprocessing audio %d: %w", i, err)
		}

		// Create model inputs with audio data
		inputs := &backends.ModelInputs{
			AudioFeatures: melSpec,
			AudioBatch:    1,
			AudioTime:     numFrames,
			AudioMels:     p.AudioProcessor.Config.NMels,
		}

		// Run inference
		embeddings, err := p.EmbedBatch(ctx, inputs)
		if err != nil {
			return nil, fmt.Errorf("embedding audio %d: %w", i, err)
		}
		if len(embeddings) == 0 {
			return nil, fmt.Errorf("no embedding returned for audio %d", i)
		}
		results[i] = embeddings[0]
	}

	return results, nil
}

// SupportsImages returns true if this pipeline can embed images.
func (p *EmbeddingPipeline) SupportsImages() bool {
	return p.ImageProcessor != nil
}

// SupportsText returns true if this pipeline can embed text.
func (p *EmbeddingPipeline) SupportsText() bool {
	return p.Tokenizer != nil
}

// SupportsAudio returns true if this pipeline can embed audio.
func (p *EmbeddingPipeline) SupportsAudio() bool {
	return p.AudioProcessor != nil
}

// Forward runs inference on the given inputs and returns the model outputs.
// Implements backends.Model.
func (p *EmbeddingPipeline) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	return p.Model.Forward(ctx, inputs)
}

// Close releases resources held by the pipeline.
// Implements backends.Model.
func (p *EmbeddingPipeline) Close() error {
	var errs []error
	if err := p.Model.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing model: %w", err))
	}
	if p.Projector != nil {
		if err := p.Projector.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing projector: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing pipeline: %v", errs)
	}
	return nil
}

// Name returns the model name for logging and debugging.
// Implements backends.Model.
func (p *EmbeddingPipeline) Name() string {
	return p.Model.Name()
}

// Backend returns the backend type this model uses.
// Implements backends.Model.
func (p *EmbeddingPipeline) Backend() backends.BackendType {
	return p.Model.Backend()
}

// modelSupportsBatching checks whether a model's first input dimension is
// dynamic (-1), indicating it can accept variable batch sizes. If the model
// doesn't implement InputInfoProvider or has no inputs, batching is assumed
// to be supported (optimistic default).
func modelSupportsBatching(model backends.Model) bool {
	provider, ok := model.(backends.InputInfoProvider)
	if !ok {
		return true
	}
	for _, info := range provider.InputInfo() {
		if len(info.Dimensions) > 0 && info.Dimensions[0] != -1 {
			return false
		}
	}
	return true
}

// ============================================================================
// Loader Functions
// ============================================================================

// EmbeddingLoaderOption configures embedding pipeline loading.
type EmbeddingLoaderOption func(*embeddingLoaderConfig)

type embeddingLoaderConfig struct {
	normalize   bool
	pooling     backends.PoolingStrategy
	maxLength   int
	quantized   bool
	imageConfig *backends.ImageConfig
	audioConfig *backends.AudioConfig
}

// WithEmbeddingNormalization enables L2 normalization of embeddings.
func WithEmbeddingNormalization(normalize bool) EmbeddingLoaderOption {
	return func(c *embeddingLoaderConfig) {
		c.normalize = normalize
	}
}

// WithPoolingStrategy sets the pooling strategy for text embeddings.
func WithPoolingStrategy(pooling backends.PoolingStrategy) EmbeddingLoaderOption {
	return func(c *embeddingLoaderConfig) {
		c.pooling = pooling
	}
}

// WithEmbeddingMaxLength sets the maximum sequence length for text.
func WithEmbeddingMaxLength(maxLength int) EmbeddingLoaderOption {
	return func(c *embeddingLoaderConfig) {
		c.maxLength = maxLength
	}
}

// WithQuantized uses quantized model files if available.
func WithQuantized(quantized bool) EmbeddingLoaderOption {
	return func(c *embeddingLoaderConfig) {
		c.quantized = quantized
	}
}

// WithEmbeddingImageConfig overrides the image preprocessing configuration.
func WithEmbeddingImageConfig(imageConfig *backends.ImageConfig) EmbeddingLoaderOption {
	return func(c *embeddingLoaderConfig) {
		c.imageConfig = imageConfig
	}
}

// WithEmbeddingAudioConfig overrides the audio preprocessing configuration.
func WithEmbeddingAudioConfig(audioConfig *backends.AudioConfig) EmbeddingLoaderOption {
	return func(c *embeddingLoaderConfig) {
		c.audioConfig = audioConfig
	}
}

// LoadEmbeddingPipelines loads embedding pipelines from a model directory.
// Returns text, visual, and/or audio pipelines based on what's available.
// For text-only models, visualPipeline and audioPipeline will be nil.
// For CLIP models, textPipeline and visualPipeline will be returned.
// For CLAP models, textPipeline and audioPipeline will be returned.
func LoadEmbeddingPipelines(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...EmbeddingLoaderOption,
) (textPipeline, visualPipeline, audioPipeline *EmbeddingPipeline, backendType backends.BackendType, err error) {
	// Apply options
	loaderCfg := &embeddingLoaderConfig{}
	for _, opt := range opts {
		opt(loaderCfg)
	}

	// Load model configuration
	config, err := LoadEmbeddingModelConfig(modelPath)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("loading embedding config: %w", err)
	}

	if !config.HasTextEncoder() && !config.HasVisualEncoder() && !config.HasAudioEncoder() {
		return nil, nil, nil, "", fmt.Errorf("no text, visual, or audio encoder found in %s", modelPath)
	}

	// Get a loader for the model
	loader, backendType, err := sessionManager.GetLoaderForModel(modelBackends)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("getting model loader: %w", err)
	}

	// Helper to close already-loaded pipelines on error
	closePipelines := func() {
		if textPipeline != nil {
			_ = textPipeline.Close()
		}
		if visualPipeline != nil {
			_ = visualPipeline.Close()
		}
	}

	// Load text encoder pipeline if available
	if config.HasTextEncoder() {
		textPipeline, err = loadTextEmbeddingPipeline(modelPath, config, loader, loaderCfg)
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("loading text encoder: %w", err)
		}
	}

	// Load visual encoder pipeline if available
	if config.HasVisualEncoder() {
		visualPipeline, err = loadVisualEmbeddingPipeline(modelPath, config, loader, loaderCfg)
		if err != nil {
			closePipelines()
			return nil, nil, nil, "", fmt.Errorf("loading visual encoder: %w", err)
		}
	}

	// Load audio encoder pipeline if available
	if config.HasAudioEncoder() {
		audioPipeline, err = loadAudioEmbeddingPipeline(modelPath, config, loader, loaderCfg)
		if err != nil {
			closePipelines()
			return nil, nil, nil, "", fmt.Errorf("loading audio encoder: %w", err)
		}
	}

	return textPipeline, visualPipeline, audioPipeline, backendType, nil
}

// loadTextEmbeddingPipeline loads the text encoder as an EmbeddingPipeline.
func loadTextEmbeddingPipeline(
	modelPath string,
	config *EmbeddingModelConfig,
	loader backends.ModelLoader,
	loaderCfg *embeddingLoaderConfig,
) (*EmbeddingPipeline, error) {
	// Load tokenizer
	tokenizer, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading tokenizer: %w", err)
	}

	// Compute relative path for the ONNX file (may be in onnx/ subdirectory)
	onnxRelPath, err := filepath.Rel(modelPath, config.TextEncoderFile)
	if err != nil {
		onnxRelPath = filepath.Base(config.TextEncoderFile)
	}

	// Load model
	model, err := loader.Load(modelPath, backends.WithONNXFile(onnxRelPath))
	if err != nil {
		return nil, fmt.Errorf("loading text model: %w", err)
	}

	// Check for text projection model (e.g., text_projection.onnx for CLIP/CLAP)
	// This projects from hidden_size (e.g., 768) to projection_dim (e.g., 512)
	var projector backends.Model
	projectionFile := FindONNXFile(modelPath, []string{
		"text_projection.onnx",
	})
	if projectionFile != "" {
		projRelPath, err := filepath.Rel(modelPath, projectionFile)
		if err != nil {
			projRelPath = filepath.Base(projectionFile)
		}
		projector, err = loader.Load(modelPath, backends.WithONNXFile(projRelPath))
		if err != nil {
			_ = model.Close()
			return nil, fmt.Errorf("loading text projection: %w", err)
		}
	}

	// Build pipeline config
	pipelineConfig := &EmbeddingPipelineConfig{
		MaxLength:        FirstNonZero(loaderCfg.maxLength, config.MaxTextLength, 512),
		Normalize:        loaderCfg.normalize,
		Pooling:          loaderCfg.pooling,
		AddSpecialTokens: true,
	}
	if pipelineConfig.Pooling == "" {
		pipelineConfig.Pooling = backends.PoolingMean
	}

	pipeline := NewEmbeddingPipeline(model, tokenizer, pipelineConfig)
	pipeline.Projector = projector
	return pipeline, nil
}

// loadVisualEmbeddingPipeline loads the visual encoder as an EmbeddingPipeline.
func loadVisualEmbeddingPipeline(
	modelPath string,
	config *EmbeddingModelConfig,
	loader backends.ModelLoader,
	loaderCfg *embeddingLoaderConfig,
) (*EmbeddingPipeline, error) {
	// Compute relative path for the ONNX file (may be in onnx/ subdirectory)
	onnxRelPath, err := filepath.Rel(modelPath, config.VisualEncoderFile)
	if err != nil {
		onnxRelPath = filepath.Base(config.VisualEncoderFile)
	}

	// Load model
	model, err := loader.Load(modelPath, backends.WithONNXFile(onnxRelPath))
	if err != nil {
		return nil, fmt.Errorf("loading visual model: %w", err)
	}

	// Check for visual projection model (e.g., visual_projection.onnx for CLIP)
	// This projects from hidden_size (e.g., 768) to projection_dim (e.g., 512)
	var projector backends.Model
	projectionFile := FindONNXFile(modelPath, []string{
		"visual_projection.onnx",
		"vision_projection.onnx",
	})
	if projectionFile != "" {
		projRelPath, err := filepath.Rel(modelPath, projectionFile)
		if err != nil {
			projRelPath = filepath.Base(projectionFile)
		}
		projector, err = loader.Load(modelPath, backends.WithONNXFile(projRelPath))
		if err != nil {
			_ = model.Close()
			return nil, fmt.Errorf("loading visual projection: %w", err)
		}
	}

	// Use provided image config or the one from model config
	imageConfig := loaderCfg.imageConfig
	if imageConfig == nil {
		imageConfig = config.ImageConfig
	}
	if imageConfig == nil {
		imageConfig = backends.DefaultImageConfig()
	}

	// Build pipeline config
	pipelineConfig := &EmbeddingPipelineConfig{
		Normalize:   loaderCfg.normalize,
		Pooling:     loaderCfg.pooling,
		ImageConfig: imageConfig,
	}
	if pipelineConfig.Pooling == "" {
		pipelineConfig.Pooling = backends.PoolingMean
	}

	pipeline := NewImageEmbeddingPipeline(model, imageConfig, pipelineConfig)
	pipeline.Projector = projector
	return pipeline, nil
}

// loadAudioEmbeddingPipeline loads the audio encoder as an EmbeddingPipeline.
func loadAudioEmbeddingPipeline(
	modelPath string,
	config *EmbeddingModelConfig,
	loader backends.ModelLoader,
	loaderCfg *embeddingLoaderConfig,
) (*EmbeddingPipeline, error) {
	// Compute relative path for the ONNX file (may be in onnx/ subdirectory)
	onnxRelPath, err := filepath.Rel(modelPath, config.AudioEncoderFile)
	if err != nil {
		onnxRelPath = filepath.Base(config.AudioEncoderFile)
	}

	// Load model
	model, err := loader.Load(modelPath, backends.WithONNXFile(onnxRelPath))
	if err != nil {
		return nil, fmt.Errorf("loading audio model: %w", err)
	}

	// Check for audio projection model (e.g., audio_projection.onnx for CLAP)
	var projector backends.Model
	projectionFile := FindONNXFile(modelPath, []string{
		"audio_projection.onnx",
	})
	if projectionFile != "" {
		projRelPath, err := filepath.Rel(modelPath, projectionFile)
		if err != nil {
			projRelPath = filepath.Base(projectionFile)
		}
		projector, err = loader.Load(modelPath, backends.WithONNXFile(projRelPath))
		if err != nil {
			_ = model.Close()
			return nil, fmt.Errorf("loading audio projection: %w", err)
		}
	}

	// Use provided audio config or the one from model config
	audioConfig := loaderCfg.audioConfig
	if audioConfig == nil {
		audioConfig = config.AudioConfig
	}
	if audioConfig == nil {
		audioConfig = buildCLAPAudioConfig()
	}

	// Build pipeline config
	pipelineConfig := &EmbeddingPipelineConfig{
		Normalize:   loaderCfg.normalize,
		Pooling:     loaderCfg.pooling,
		AudioConfig: audioConfig,
	}
	if pipelineConfig.Pooling == "" {
		pipelineConfig.Pooling = backends.PoolingMean
	}

	pipeline := NewAudioEmbeddingPipeline(model, audioConfig, pipelineConfig)
	pipeline.Projector = projector
	return pipeline, nil
}
