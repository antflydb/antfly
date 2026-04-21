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

package backends

import "context"

// Session represents a low-level inference session that can run tensor computations.
// This is the primitive interface that backends provide - it handles tensor I/O
// without knowledge of model semantics (encoder-decoder, vision, etc.).
//
// Higher-level model types (Vision2SeqModel, EncoderDecoderModel) are built
// on top of Session in the pipelines package.
type Session interface {
	// Run executes the session with the given named inputs.
	// Returns named outputs as tensors.
	Run(inputs []NamedTensor) ([]NamedTensor, error)

	// InputInfo returns metadata about expected inputs.
	InputInfo() []TensorInfo

	// OutputInfo returns metadata about outputs.
	OutputInfo() []TensorInfo

	// Close releases resources associated with the session.
	Close() error
}

// NamedTensor associates a name with tensor data.
type NamedTensor struct {
	Name  string
	Shape []int64
	Data  any // []float32, []int64, []int32, etc.
}

// TensorInfo describes a tensor's metadata.
type TensorInfo struct {
	Name     string
	Shape    []int64  // -1 for dynamic dimensions
	DataType DataType // float32, int64, etc.
}

// DataType represents tensor element types.
type DataType string

const (
	DataTypeFloat32 DataType = "float32"
	DataTypeFloat16 DataType = "float16"
	DataTypeInt64   DataType = "int64"
	DataTypeInt32   DataType = "int32"
	DataTypeBool    DataType = "bool"
)

// SessionFactory creates sessions from model files.
// Each backend implements this to provide its session creation mechanism.
type SessionFactory interface {
	// CreateSession creates a session from a model file (e.g., ONNX file).
	CreateSession(modelPath string, opts ...SessionOption) (Session, error)

	// Backend returns the backend type this factory uses.
	Backend() BackendType
}

// SessionFactoryProvider is an optional interface that backends can implement
// to provide access to a SessionFactory for creating raw sessions.
// This allows higher-level code to build custom model types from sessions.
type SessionFactoryProvider interface {
	// SessionFactory returns a factory for creating raw sessions.
	SessionFactory() SessionFactory
}

// SessionOption configures session creation.
type SessionOption func(*SessionConfig)

// SessionConfig holds configuration for session creation.
type SessionConfig struct {
	// NumThreads for inference (0 = auto)
	NumThreads int

	// GPUMode controls GPU acceleration
	GPUMode GPUMode

	// GraphOptimizationLevel for ONNX (0-3)
	GraphOptimizationLevel int

	// InputConstants maps input names to compile-time constant values.
	// These inputs are baked into the computation graph as constants rather
	// than passed as dynamic inputs at inference time. This enables static
	// resolution of ONNX If node conditions, allowing the graph compiler
	// to build only the taken branch.
	//
	// Only used by GoMLX-based backends (Go, XLA, CoreML). ONNX Runtime
	// handles If nodes natively at runtime, so this field is ignored.
	InputConstants map[string]any

	// DynamicAxes overrides fixed input dimensions to be dynamic.
	// Some ONNX models (notably HuggingFace optimum exports) bake specific
	// integer values into input dimension specs during tracing, when those
	// dimensions should actually be dynamic (e.g., sequence length exported
	// as fixed 16 instead of symbolic "decoder_sequence_length").
	//
	// Only used by GoMLX-based backends. ONNX Runtime handles dynamic
	// shapes natively, so this field is ignored.
	DynamicAxes []DynamicAxisOverride
}

// DefaultSessionConfig returns sensible defaults.
func DefaultSessionConfig() *SessionConfig {
	return &SessionConfig{
		NumThreads:             0,
		GPUMode:                GPUModeAuto,
		GraphOptimizationLevel: 3,
	}
}

// WithSessionThreads sets the number of threads.
func WithSessionThreads(n int) SessionOption {
	return func(c *SessionConfig) {
		c.NumThreads = n
	}
}

// WithSessionGPUMode sets the GPU mode.
func WithSessionGPUMode(mode GPUMode) SessionOption {
	return func(c *SessionConfig) {
		c.GPUMode = mode
	}
}

// WithInputConstants marks inputs as compile-time constants.
// Constant inputs are baked into the computation graph and must not be
// passed as dynamic inputs at inference time. This enables static resolution
// of ONNX If node conditions on GoMLX-based backends.
func WithInputConstants(constants map[string]any) SessionOption {
	return func(c *SessionConfig) {
		c.InputConstants = constants
	}
}

// DynamicAxisOverride specifies that a fixed input dimension should be
// treated as dynamic. This works around ONNX models where the exporter
// baked a specific integer value into a dimension that should be symbolic.
type DynamicAxisOverride struct {
	InputName string // ONNX input name (e.g., "inputs_embeds")
	Axis      int    // Dimension index to override (e.g., 1 for seq_len)
	ParamName string // Symbolic name for the dynamic dim (e.g., "decoder_sequence_length")
}

// WithDynamicAxes overrides fixed input dimensions to be dynamic.
// Only affects GoMLX-based backends; ONNX Runtime ignores this.
func WithDynamicAxes(overrides []DynamicAxisOverride) SessionOption {
	return func(c *SessionConfig) {
		c.DynamicAxes = overrides
	}
}

// ApplySessionOptions applies options to a config.
func ApplySessionOptions(opts ...SessionOption) *SessionConfig {
	cfg := DefaultSessionConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// GenerativeSession represents a session for generative (LLM) models.
// This wraps backends like ortgenai that handle chat completion and text generation.
type GenerativeSession interface {
	// Generate produces text from the given messages.
	// Returns the generated text, token count, and any error.
	Generate(ctx context.Context, messages []GenerativeMessage, opts *GenerativeOptions) (*GenerativeResult, error)

	// GenerateStream produces tokens one at a time via channels.
	// Returns a channel for tokens, a channel for errors, and any setup error.
	GenerateStream(ctx context.Context, messages []GenerativeMessage, opts *GenerativeOptions) (<-chan GenerativeToken, <-chan error, error)

	// Close releases resources associated with the session.
	Close() error
}

// GenerativeMessage represents a chat message for generative models.
type GenerativeMessage struct {
	Role      string   // "system", "user", "assistant"
	Content   string   // Text content
	ImageURLs []string // Optional image URLs for multimodal models
}

// GenerativeOptions holds parameters for text generation.
type GenerativeOptions struct {
	MaxTokens   int
	Temperature float32
	TopP        float32
	TopK        int
	StopTokens  []string
}

// DefaultGenerativeOptions returns sensible defaults for generation.
func DefaultGenerativeOptions() *GenerativeOptions {
	return &GenerativeOptions{
		MaxTokens:   2048,
		Temperature: 1.0,
		TopP:        1.0,
		TopK:        0,
	}
}

// GenerativeResult holds the output of text generation.
type GenerativeResult struct {
	Text         string
	TokensUsed   int
	FinishReason string // "stop", "length", etc.
}

// GenerativeToken represents a single token in streaming output.
type GenerativeToken struct {
	Token   string
	Index   int
	IsFinal bool
}

// GenerativeSessionFactory creates generative sessions from model directories.
// Each backend that supports generative models implements this.
type GenerativeSessionFactory interface {
	// CreateGenerativeSession creates a session from a model directory.
	// The model directory should contain genai_config.json or config.json + model.onnx.
	CreateGenerativeSession(modelPath string, opts ...SessionOption) (GenerativeSession, error)

	// SupportsGenerativeModel returns true if the model at the given path can be loaded.
	SupportsGenerativeModel(modelPath string) bool

	// Backend returns the backend type this factory uses.
	Backend() BackendType
}

// GenerativeSessionFactoryProvider is an optional interface that backends can implement
// to provide access to a GenerativeSessionFactory for creating generative sessions.
type GenerativeSessionFactoryProvider interface {
	// GenerativeSessionFactory returns a factory for creating generative sessions.
	GenerativeSessionFactory() GenerativeSessionFactory
}
