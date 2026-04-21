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

// Package backends provides a unified interface for creating ML inference sessions
// with multi-backend support:
//
//   - go-huggingface: Hub download, tokenizers, safetensors parsing
//   - onnx-gomlx: ONNX model execution via GoMLX
//   - onnxruntime_go: Direct ONNX Runtime inference
//
// Available backends:
//   - GoMLX: Always available, supports HuggingFace and ONNX model formats
//     Engine options: "simplego" (pure Go) or "xla" (hardware accelerated via PJRT)
//   - ONNX Runtime: Fastest inference, requires -tags="onnx,ORT"
//
// Build example:
//
//	go build -tags="onnx,ORT" ./cmd/termite
//
// Backend selection at runtime follows a configurable priority order (default: ONNX > GoMLX).
// Models can restrict which backends they support via their manifest.
package backends

import "fmt"

// BackendType identifies the inference backend
type BackendType string

const (
	// BackendONNX is the ONNX Runtime backend - fast CPU/GPU inference
	BackendONNX BackendType = "onnx"

	// BackendXLA is the GoMLX backend with XLA engine (hardware accelerated via PJRT)
	// Supports TPU, CUDA, and optimized CPU. Requires XLA/PJRT runtime.
	BackendXLA BackendType = "xla"

	// BackendCoreML is the GoMLX backend with CoreML engine (macOS only)
	// Uses Apple's Neural Engine, GPU, and CPU. Requires CGO on Darwin.
	BackendCoreML BackendType = "coreml"

	// BackendGo is the GoMLX backend with pure Go engine (no CGO)
	// Always available, slower than XLA but no external dependencies.
	BackendGo BackendType = "go"
)

// DeviceType identifies the hardware device for inference
type DeviceType string

const (
	// DeviceAuto auto-detects the best available device (default)
	DeviceAuto DeviceType = "auto"

	// DeviceCUDA uses NVIDIA CUDA GPU
	DeviceCUDA DeviceType = "cuda"

	// DeviceCoreML uses Apple CoreML (macOS only)
	DeviceCoreML DeviceType = "coreml"

	// DeviceTPU uses Google TPU
	DeviceTPU DeviceType = "tpu"

	// DeviceCPU forces CPU-only inference
	DeviceCPU DeviceType = "cpu"
)

// GPUMode controls how GPU acceleration is enabled.
// Values must match the GPUMode enum in openapi.yaml.
type GPUMode string

const (
	GPUModeAuto   GPUMode = "auto"   // Auto-detect GPU availability
	GPUModeTpu    GPUMode = "tpu"    // Force TPU
	GPUModeCuda   GPUMode = "cuda"   // Force CUDA
	GPUModeCoreML GPUMode = "coreml" // Force CoreML (macOS only)
	GPUModeOff    GPUMode = "off"    // CPU only
)

// ToGPUMode converts DeviceType to GPUMode.
func (d DeviceType) ToGPUMode() GPUMode {
	switch d {
	case DeviceAuto:
		return GPUModeAuto
	case DeviceCUDA:
		return GPUModeCuda
	case DeviceCoreML:
		return GPUModeCoreML
	case DeviceTPU:
		return GPUModeTpu
	case DeviceCPU:
		return GPUModeOff
	default:
		return GPUModeAuto
	}
}

// BackendSpec combines a backend type with a device specification.
// Used for configuring backend priority with device preferences.
type BackendSpec struct {
	Backend BackendType
	Device  DeviceType
}

// String returns the string representation (e.g., "onnx:cuda" or "go")
func (s BackendSpec) String() string {
	if s.Device == DeviceAuto || s.Device == "" {
		return string(s.Backend)
	}
	return string(s.Backend) + ":" + string(s.Device)
}

// GPUInfo contains information about the detected GPU
type GPUInfo struct {
	Available   bool   `json:"available"`
	Type        string `json:"type"` // "cuda", "coreml", "tpu", "none"
	DeviceName  string `json:"device_name,omitempty"`
	DriverVer   string `json:"driver_version,omitempty"`
	CUDAVersion string `json:"cuda_version,omitempty"`
}

// ModelInputs contains the inputs for model inference.
// Different model types use different subsets of fields.
type ModelInputs struct {
	// Text inputs (encoder-only, generative, seq2seq)
	InputIDs      [][]int32 // Token IDs [batch, seq]
	AttentionMask [][]int32 // Attention mask [batch, seq]
	TokenTypeIDs  [][]int32 // Optional: token type IDs for BERT-style models [batch, seq]

	// Image inputs (vision models)
	ImagePixels   []float32 // Preprocessed image data [batch, channels, height, width]
	ImageBatch    int       // Batch size for images
	ImageChannels int       // Number of channels (typically 3)
	ImageHeight   int       // Image height
	ImageWidth    int       // Image width

	// Pre-computed embeddings (for projection models like visual_projection.onnx)
	Embeddings [][]float32 // Embeddings to project [batch, hidden_size]

	// Audio inputs (speech2seq models)
	AudioFeatures []float32 // Preprocessed mel spectrogram [batch, time, features]
	AudioBatch    int       // Batch size for audio
	AudioTime     int       // Time steps (frames)
	AudioMels     int       // Feature dimension (mel bins)

	// For encoder-decoder models (seq2seq, vision2seq, speech2seq)
	EncoderOutput *EncoderOutput // Output from encoder to pass to decoder

	// For autoregressive models (generative, seq2seq decoder, vision2seq decoder, speech2seq decoder)
	PastKeyValues *KVCache // KV-cache from previous steps
}

// Shape represents tensor dimensions.
type Shape []int64

// String returns a string representation of the shape.
func (s Shape) String() string {
	return fmt.Sprintf("%v", []int64(s))
}

// ValuesInt converts the shape to an int slice.
func (s Shape) ValuesInt() []int {
	result := make([]int, len(s))
	for i, d := range s {
		result[i] = int(d)
	}
	return result
}

// NewShape returns a Shape with the given dimensions.
func NewShape(dimensions ...int64) Shape {
	return dimensions
}

// InputOutputInfo describes the shape and type of a model input or output.
type InputOutputInfo struct {
	Name       string
	Dimensions Shape
	DataType   string // "float32", "int64", "int32", etc.
}

// PoolingStrategy defines how to pool hidden states into embeddings.
type PoolingStrategy string

const (
	PoolingMean PoolingStrategy = "mean"
	PoolingCLS  PoolingStrategy = "cls"
	PoolingMax  PoolingStrategy = "max"
	PoolingNone PoolingStrategy = "none"
)

// ModelOutput contains the outputs from a forward pass.
// Different model types populate different subsets of fields.
type ModelOutput struct {
	// For encoder models (text or vision)
	LastHiddenState [][][]float32  // [batch, seq, hidden]
	EncoderOutput   *EncoderOutput // Structured encoder output for decoder consumption

	// For embedding models
	Embeddings [][]float32 // [batch, hidden] (after pooling)

	// For classification and generative models
	Logits [][]float32 // [batch, num_classes] or [batch, vocab_size]

	// For autoregressive models
	PastKeyValues *KVCache // Updated KV-cache for next step
}

// EncoderOutput holds the output of a vision or text encoder.
type EncoderOutput struct {
	// HiddenStates are the encoder's hidden states.
	// For vision encoders: [batch, num_patches, hidden]
	// For text encoders: [batch, seq, hidden]
	HiddenStates []float32
	// Shape holds the tensor dimensions [batch, seq, hidden].
	Shape [3]int
}

// KVCache holds the key-value cache for autoregressive decoding.
// Used to avoid recomputing attention for previously generated tokens.
type KVCache struct {
	// Keys holds the key cache. Shape depends on model architecture.
	// Typically [num_layers, batch, num_heads, seq, head_dim] or similar.
	Keys []float32
	// Values holds the value cache, same shape as Keys.
	Values []float32
	// SeqLen is the current sequence length in the cache.
	SeqLen int
	// NumLayers is the number of decoder layers.
	NumLayers int
	// NumHeads is the number of attention heads.
	NumHeads int
	// HeadDim is the dimension of each attention head.
	HeadDim int
	// BatchSize is the batch size.
	BatchSize int
	// Tensors holds named KV-cache tensors for models with complex cache structures.
	// Keys are output tensor names (e.g., "present.0.decoder.key").
	// Used by BART/REBEL models with separate self-attention and cross-attention caches.
	Tensors map[string]NamedTensor
}

// DecoderConfig holds decoder configuration for generation.
type DecoderConfig struct {
	// VocabSize is the size of the vocabulary.
	VocabSize int
	// MaxLength is the maximum generation length.
	MaxLength int
	// EOSTokenID is the primary end-of-sequence token ID.
	EOSTokenID int32
	// EOSTokenIDs contains all end-of-sequence token IDs (for models with multiple stop tokens).
	// If non-empty, generation stops when any of these tokens is produced.
	EOSTokenIDs []int32
	// BOSTokenID is the beginning-of-sequence token ID.
	BOSTokenID int32
	// PadTokenID is the padding token ID.
	PadTokenID int32
	// DecoderStartTokenID is the token ID to start decoding with.
	DecoderStartTokenID int32
	// NumLayers is the number of decoder layers.
	NumLayers int
	// NumHeads is the number of attention heads per layer.
	NumHeads int
	// HeadDim is the dimension of each attention head.
	HeadDim int

	// ForcedDecoderIds are token IDs that must be generated at specific positions.
	// Each entry is [position, token_id]. Used by Whisper for language/task tokens.
	ForcedDecoderIds [][]int32
	// SuppressTokens are token IDs that should never be generated.
	SuppressTokens []int32
}

// ImageConfig holds configuration for image preprocessing.
type ImageConfig struct {
	// Width is the target image width.
	Width int
	// Height is the target image height.
	Height int
	// Channels is the number of color channels (typically 3 for RGB).
	Channels int
	// Mean is the per-channel mean for normalization.
	Mean [3]float32
	// Std is the per-channel standard deviation for normalization.
	Std [3]float32
	// RescaleFactor scales pixel values (e.g., 1/255 to convert 0-255 to 0-1).
	RescaleFactor float32
	// DoCenterCrop indicates whether to center crop before resize.
	DoCenterCrop bool
	// CropSize is the size for center cropping (if DoCenterCrop is true).
	CropSize int
}

// DefaultImageConfig returns sensible defaults for image preprocessing.
// These values are typical for ViT-based models.
func DefaultImageConfig() *ImageConfig {
	return &ImageConfig{
		Width:         384,
		Height:        384,
		Channels:      3,
		Mean:          [3]float32{0.5, 0.5, 0.5},
		Std:           [3]float32{0.5, 0.5, 0.5},
		RescaleFactor: 1.0 / 255.0,
		DoCenterCrop:  false,
	}
}

// AudioNormalizationType specifies how to normalize mel spectrogram features.
type AudioNormalizationType string

const (
	// AudioNormWhisper uses Whisper-specific normalization:
	// log10, clip to (globalMax - 8.0), then (x + 4.0) / 4.0
	AudioNormWhisper AudioNormalizationType = "whisper"

	// AudioNormSimple uses simple log normalization without Whisper-specific scaling.
	// Suitable for CLAP and other audio models.
	AudioNormSimple AudioNormalizationType = "simple"
)

// AudioConfig holds configuration for audio preprocessing.
type AudioConfig struct {
	// SampleRate is the target sample rate (typically 16000 for speech models).
	SampleRate int
	// FeatureSize is the mel spectrogram feature dimension (typically 80 or 128).
	FeatureSize int
	// NFft is the FFT window size (typically 400 for 25ms at 16kHz).
	NFft int
	// HopLength is the hop length between frames (typically 160 for 10ms at 16kHz).
	HopLength int
	// ChunkLength is the audio chunk length in seconds (typically 30 for Whisper).
	ChunkLength int
	// NMels is the number of mel filter banks.
	NMels int
	// PaddingValue is the value to pad with (typically 0.0).
	PaddingValue float32
	// Normalization specifies the normalization type for mel spectrograms.
	// Defaults to AudioNormWhisper if empty for backward compatibility.
	Normalization AudioNormalizationType
}

// DefaultAudioConfig returns sensible defaults for Whisper-style models.
func DefaultAudioConfig() *AudioConfig {
	return &AudioConfig{
		SampleRate:    16000,
		FeatureSize:   80,
		NFft:          400,
		HopLength:     160,
		ChunkLength:   30,
		NMels:         80,
		PaddingValue:  0.0,
		Normalization: AudioNormWhisper,
	}
}

// GenerationConfig holds parameters for text generation.
type GenerationConfig struct {
	// MaxNewTokens is the maximum number of tokens to generate.
	MaxNewTokens int
	// MinLength is the minimum generation length.
	MinLength int
	// DoSample enables sampling (vs greedy decoding).
	DoSample bool
	// Temperature for sampling (higher = more random).
	Temperature float32
	// TopK limits sampling to top K tokens.
	TopK int
	// TopP (nucleus sampling) limits to tokens with cumulative probability <= TopP.
	TopP float32
	// RepetitionPenalty penalizes repeated tokens.
	RepetitionPenalty float32
	// NumBeams for beam search (1 = greedy/sampling).
	NumBeams int
	// EarlyStopping for beam search.
	EarlyStopping bool
}

// DefaultGenerationConfig returns sensible defaults for generation.
func DefaultGenerationConfig() *GenerationConfig {
	return &GenerationConfig{
		MaxNewTokens:      256,
		MinLength:         1,
		DoSample:          false, // greedy by default
		Temperature:       1.0,
		TopK:              50,
		TopP:              1.0,
		RepetitionPenalty: 1.0,
		NumBeams:          1,
		EarlyStopping:     false,
	}
}
