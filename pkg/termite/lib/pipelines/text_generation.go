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
	"os"
	"path/filepath"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// =============================================================================
// Tool Parsing Support
// =============================================================================

// ToolParser handles prompt formatting and output parsing for tool calling.
// Different model families implement this interface with their specific formats.
// This interface mirrors the one in lib/generation for use by pipelines.
type ToolParser interface {
	// Name returns the parser identifier (e.g., "functiongemma", "json", "hermes")
	Name() string

	// FormatToolsPrompt creates tool declarations for the system prompt.
	FormatToolsPrompt(tools []ToolDefinition) string

	// Feed processes incoming tokens and detects complete tool calls.
	// Returns newly completed tool calls (for real-time streaming emission).
	Feed(token string) []ToolCall

	// Finish completes parsing and returns all tool calls and remaining text.
	Finish() (toolCalls []ToolCall, remainingText string)

	// Reset clears parser state for reuse.
	Reset()
}

// ToolDefinition describes a tool that the model can call.
type ToolDefinition struct {
	Type     string             `json:"type"` // "function"
	Function FunctionDefinition `json:"function"`
}

// FunctionDefinition describes a function that can be called by the model.
type FunctionDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"` // JSON Schema
	Strict      bool           `json:"strict,omitempty"`     // Whether to enforce strict parameter validation
}

// ToolCall represents a tool call made by the model.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"` // "function"
	Function ToolCallFunction `json:"function"`
}

// ToolCallFunction contains the function name and arguments for a tool call.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON string
}

// ToolParserFactory creates a ToolParser for a given model path.
type ToolParserFactory func(modelPath string) (ToolParser, error)

// genAIConfig holds the config structure for genai_config.json files.
type genAIConfig struct {
	Model struct {
		Type      string `json:"type"`
		VocabSize int    `json:"vocab_size"`
	} `json:"model"`

	// Termite extension: tool calling format
	ToolCallFormat string `json:"tool_call_format,omitempty"`
}

// readGenAIConfig reads the genai_config.json file from the model directory.
func readGenAIConfig(modelPath string) *genAIConfig {
	configPath := filepath.Join(modelPath, "genai_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil
	}

	var config genAIConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil
	}

	return &config
}

// =============================================================================
// Config Types
// =============================================================================

// GenerativeModelConfig holds parsed configuration for a generative decoder model.
// This is loaded from config.json and generation_config.json.
type GenerativeModelConfig struct {
	// Path to the model directory
	ModelPath string

	// Path to ONNX file
	DecoderPath string

	// Decoder configuration
	DecoderConfig *backends.DecoderConfig

	// GenerationConfig holds sampling parameters from generation_config.json.
	// May be nil if the model has no generation_config.json.
	GenerationConfig *backends.GenerationConfig

	// Architecture details for KV-cache
	NumLayers        int
	NumHeads         int
	NumKVHeads       int // For grouped-query attention (GQA), may differ from NumHeads
	HeadDim          int
	HiddenSize       int
	IntermediateSize int
}

// TextGenerationConfig holds configuration for creating a TextGenerationPipeline.
type TextGenerationConfig struct {
	// GenerationConfig for text generation. If nil, uses defaults.
	GenerationConfig *backends.GenerationConfig

	// ToolParserFactory creates tool parsers for models that support tool calling.
	// If nil, tool parser will be loaded automatically based on genai_config.json.
	ToolParserFactory ToolParserFactory

	// ModelPath is set internally by LoadTextGenerationPipeline.
	ModelPath string
}

// =============================================================================
// Config Loading
// =============================================================================

// LoadGenerativeModelConfig loads and parses configuration for a generative decoder model.
// This is backend-agnostic and can be used by both ONNX and GoMLX backends.
func LoadGenerativeModelConfig(modelPath string) (*GenerativeModelConfig, error) {
	// Find decoder ONNX file - decoder-only models have different naming conventions
	decoderPath := FindONNXFile(modelPath, []string{
		"model.onnx", // Standard naming
		"decoder_model_merged.onnx",
		"decoder_model.onnx",
		"decoder.onnx",
		"gpt2.onnx",    // GPT-2 specific
		"llama.onnx",   // LLaMA specific
		"mistral.onnx", // Mistral specific
	})

	// Load model configuration from config.json
	rawConfig, err := loadRawGenerativeConfig(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading model config: %w", err)
	}

	// Load generation config if available (overrides some values)
	genConfig := loadGenerationConfig(modelPath)

	// Build decoder config
	decoderConfig := buildGenerativeConfig(rawConfig, genConfig)

	// Determine architecture
	numLayers := FirstNonZero(rawConfig.NumHiddenLayers, rawConfig.NumLayers, rawConfig.NLayer, 12)
	numHeads := FirstNonZero(rawConfig.NumAttentionHeads, rawConfig.NumHeads, rawConfig.NHead, 12)
	numKVHeads := FirstNonZero(rawConfig.NumKeyValueHeads, numHeads) // Default to numHeads if not using GQA
	hiddenSize := FirstNonZero(rawConfig.HiddenSize, rawConfig.NEmbd, 768)
	headDim := FirstNonZero(rawConfig.HeadDim, hiddenSize/numHeads)
	intermediateSize := FirstNonZero(rawConfig.IntermediateSize, rawConfig.NInner, hiddenSize*4)

	// Build generation config from generation_config.json sampling parameters
	var generationConfig *backends.GenerationConfig
	if genConfig != nil {
		generationConfig = buildGenerationConfig(genConfig)
	}

	return &GenerativeModelConfig{
		ModelPath:        modelPath,
		DecoderPath:      decoderPath,
		DecoderConfig:    decoderConfig,
		GenerationConfig: generationConfig,
		NumLayers:        numLayers,
		NumHeads:         numHeads,
		NumKVHeads:       numKVHeads,
		HeadDim:          headDim,
		HiddenSize:       hiddenSize,
		IntermediateSize: intermediateSize,
	}, nil
}

// IsGenerativeModel checks if a model path contains a generative decoder model.
// Returns true for decoder-only models like GPT-2, LLaMA, Mistral, etc.
func IsGenerativeModel(path string) bool {
	// Check for ONNX file
	decoderPath := FindONNXFile(path, []string{
		"model.onnx",
		"decoder_model_merged.onnx",
		"decoder_model.onnx",
		"decoder.onnx",
		"gpt2.onnx",
		"llama.onnx",
		"mistral.onnx",
	})
	if decoderPath == "" {
		return false
	}

	// Check config.json for decoder-only architecture markers
	rawConfig, err := loadRawGenerativeConfig(path)
	if err != nil {
		return false
	}

	// Check for decoder-only model types
	decoderOnlyTypes := map[string]bool{
		"gpt2":        true,
		"gpt_neo":     true,
		"gpt_neox":    true,
		"gptj":        true,
		"llama":       true,
		"mistral":     true,
		"phi":         true,
		"qwen2":       true,
		"gemma":       true,
		"gemma2":      true,
		"gemma3_text": true,
		"falcon":      true,
		"opt":         true,
		"bloom":       true,
		"codegen":     true,
		"starcoder":   true,
	}

	return decoderOnlyTypes[rawConfig.ModelType]
}

// =============================================================================
// Raw Config Structs and Parsing Helpers
// =============================================================================

// rawGenerativeConfig represents the model's config.json structure for decoder-only models.
type rawGenerativeConfig struct {
	// Model type
	ModelType string `json:"model_type"`

	// Vocab and token IDs
	VocabSize  int   `json:"vocab_size"`
	EOSTokenID any   `json:"eos_token_id"` // Can be int or []int
	BOSTokenID int32 `json:"bos_token_id"`
	PadTokenID any   `json:"pad_token_id"` // Can be int or null

	// Architecture - different names across models
	NumHiddenLayers   int `json:"num_hidden_layers"`
	NumLayers         int `json:"n_layers"`
	NLayer            int `json:"n_layer"`
	NumAttentionHeads int `json:"num_attention_heads"`
	NumHeads          int `json:"num_heads"`
	NHead             int `json:"n_head"`
	NumKeyValueHeads  int `json:"num_key_value_heads"` // For GQA (Grouped Query Attention)
	HiddenSize        int `json:"hidden_size"`
	NEmbd             int `json:"n_embd"`
	IntermediateSize  int `json:"intermediate_size"`
	NInner            int `json:"n_inner"`
	HeadDim           int `json:"head_dim"` // Explicit head dimension (e.g., Gemma 3 uses 256 != hidden_size/num_heads)

	// Sequence length
	MaxPositionEmbeddings int `json:"max_position_embeddings"`
	NCtx                  int `json:"n_ctx"`
	NPositions            int `json:"n_positions"`
}

// rawGenerationConfig represents generation_config.json
type rawGenerationConfig struct {
	MaxLength         int     `json:"max_length"`
	MaxNewTokens      int     `json:"max_new_tokens"`
	EOSTokenID        any     `json:"eos_token_id"`
	BOSTokenID        int32   `json:"bos_token_id"`
	PadTokenID        any     `json:"pad_token_id"`
	DoSample          bool    `json:"do_sample"`
	Temperature       float32 `json:"temperature"`
	TopK              int     `json:"top_k"`
	TopP              float32 `json:"top_p"`
	RepetitionPenalty float32 `json:"repetition_penalty"`
}

// loadRawGenerativeConfig loads the model configuration from config.json.
func loadRawGenerativeConfig(path string) (*rawGenerativeConfig, error) {
	configPath := filepath.Join(path, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config.json: %w", err)
	}

	var config rawGenerativeConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config.json: %w", err)
	}

	return &config, nil
}

// loadGenerationConfig loads generation_config.json if it exists.
func loadGenerationConfig(path string) *rawGenerationConfig {
	configPath := filepath.Join(path, "generation_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil
	}

	var config rawGenerationConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil
	}

	return &config
}

// buildGenerationConfig creates a GenerationConfig from generation_config.json.
// It starts with defaults and overrides with values from the model's config.
func buildGenerationConfig(genCfg *rawGenerationConfig) *backends.GenerationConfig {
	cfg := backends.DefaultGenerationConfig()
	if genCfg.MaxNewTokens > 0 {
		cfg.MaxNewTokens = genCfg.MaxNewTokens
	}
	cfg.DoSample = genCfg.DoSample
	if genCfg.Temperature > 0 {
		cfg.Temperature = genCfg.Temperature
	}
	if genCfg.TopK > 0 {
		cfg.TopK = genCfg.TopK
	}
	if genCfg.TopP > 0 {
		cfg.TopP = genCfg.TopP
	}
	if genCfg.RepetitionPenalty > 0 {
		cfg.RepetitionPenalty = genCfg.RepetitionPenalty
	}
	return cfg
}

// buildGenerativeConfig creates a DecoderConfig from the raw configs.
func buildGenerativeConfig(cfg *rawGenerativeConfig, genCfg *rawGenerationConfig) *backends.DecoderConfig {
	// Handle eos_token_id which can be int or []int
	eosTokenID := ParseTokenID(cfg.EOSTokenID)
	eosTokenIDs := ParseTokenIDs(cfg.EOSTokenID)
	// Override from generation config if present
	if genCfg != nil && genCfg.EOSTokenID != nil {
		eosTokenID = ParseTokenID(genCfg.EOSTokenID)
		eosTokenIDs = ParseTokenIDs(genCfg.EOSTokenID)
	}

	// Handle pad_token_id which can be int or null
	var padTokenID int32
	switch v := cfg.PadTokenID.(type) {
	case float64:
		padTokenID = int32(v)
	case nil:
		padTokenID = eosTokenID // Common fallback
	}

	// Max length
	maxLength := FirstNonZero(cfg.MaxPositionEmbeddings, cfg.NCtx, cfg.NPositions, 2048)
	if genCfg != nil && genCfg.MaxLength > 0 {
		maxLength = genCfg.MaxLength
	}

	numHeads := FirstNonZero(cfg.NumAttentionHeads, cfg.NumHeads, cfg.NHead, 12)
	hiddenSize := FirstNonZero(cfg.HiddenSize, cfg.NEmbd, 768)

	return &backends.DecoderConfig{
		VocabSize:           cfg.VocabSize,
		MaxLength:           maxLength,
		EOSTokenID:          eosTokenID,
		EOSTokenIDs:         eosTokenIDs,
		BOSTokenID:          cfg.BOSTokenID,
		PadTokenID:          padTokenID,
		DecoderStartTokenID: cfg.BOSTokenID, // For decoder-only, start with BOS
		NumLayers:           FirstNonZero(cfg.NumHiddenLayers, cfg.NumLayers, cfg.NLayer, 12),
		NumHeads:            numHeads,
		HeadDim:             FirstNonZero(cfg.HeadDim, hiddenSize/numHeads),
	}
}

// =============================================================================
// Model Wrapper
// =============================================================================

// Ensure generativeModel implements backends.Model
var _ backends.Model = (*generativeModel)(nil)

// generativeModel implements backends.Model for decoder-only text generation.
// It handles GPT, LLaMA, etc. with KV-cache support.
type generativeModel struct {
	config *GenerativeModelConfig

	decoderSession backends.Session

	backendType backends.BackendType
}

// NewGenerativeModel creates a Model from a decoder session.
func NewGenerativeModel(
	config *GenerativeModelConfig,
	decoderSession backends.Session,
	backendType backends.BackendType,
) backends.Model {
	return &generativeModel{
		config:         config,
		decoderSession: decoderSession,
		backendType:    backendType,
	}
}

// LoadGenerativeModel loads a generative Model using the given session factory.
// It automatically discovers the ONNX file and creates a session.
func LoadGenerativeModel(modelPath string, factory backends.SessionFactory, opts ...backends.SessionOption) (backends.Model, error) {
	// Load configuration
	config, err := LoadGenerativeModelConfig(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading model config: %w", err)
	}

	if config.DecoderPath == "" {
		return nil, fmt.Errorf("decoder ONNX file not found in %s", modelPath)
	}

	// Create decoder session
	decoderSession, err := factory.CreateSession(config.DecoderPath, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating decoder session: %w", err)
	}

	return &generativeModel{
		config:         config,
		decoderSession: decoderSession,
		backendType:    factory.Backend(),
	}, nil
}

// Forward performs one decoding step given input tokens.
// Uses InputIDs and PastKeyValues from inputs.
func (m *generativeModel) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	if inputs == nil || len(inputs.InputIDs) == 0 {
		return nil, fmt.Errorf("empty input")
	}

	inputIDs := inputs.InputIDs
	pastKeyValues := inputs.PastKeyValues

	batchSize := len(inputIDs)
	seqLen := len(inputIDs[0])

	// Flatten input IDs to int64 for most models
	flatInputIDs := make([]int64, batchSize*seqLen)
	for i := range batchSize {
		for j := range seqLen {
			flatInputIDs[i*seqLen+j] = int64(inputIDs[i][j])
		}
	}

	// Build decoder inputs
	tensorInputs, err := m.buildDecoderInputs(flatInputIDs, batchSize, seqLen, pastKeyValues)
	if err != nil {
		return nil, fmt.Errorf("building decoder inputs: %w", err)
	}

	// Run decoder
	outputs, err := m.decoderSession.Run(tensorInputs)
	if err != nil {
		return nil, fmt.Errorf("running decoder: %w", err)
	}

	if len(outputs) == 0 {
		return nil, fmt.Errorf("no decoder output")
	}

	// Extract logits (first output, typically named "logits" or "output")
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

// buildDecoderInputs creates the input tensors for the decoder.
func (m *generativeModel) buildDecoderInputs(inputIDs []int64, batchSize, seqLen int, pastKV *backends.KVCache) ([]backends.NamedTensor, error) {
	var inputs []backends.NamedTensor

	// Get decoder input names from session
	inputInfo := m.decoderSession.InputInfo()
	inputNames := make(map[string]bool)
	for _, info := range inputInfo {
		inputNames[info.Name] = true
	}

	// Add input_ids
	inputIDsName := "input_ids"
	if inputNames["decoder_input_ids"] {
		inputIDsName = "decoder_input_ids"
	}
	inputs = append(inputs, backends.NamedTensor{
		Name:  inputIDsName,
		Shape: []int64{int64(batchSize), int64(seqLen)},
		Data:  inputIDs,
	})

	// Add attention_mask if needed
	if inputNames["attention_mask"] {
		// Create attention mask (all 1s for non-padded tokens)
		// For KV-cached generation, mask should cover full sequence including past
		totalSeqLen := seqLen
		if pastKV != nil {
			totalSeqLen = pastKV.SeqLen + seqLen
		}
		mask := make([]int64, batchSize*totalSeqLen)
		for i := range mask {
			mask[i] = 1
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "attention_mask",
			Shape: []int64{int64(batchSize), int64(totalSeqLen)},
			Data:  mask,
		})
	}

	// Add position_ids if needed
	if inputNames["position_ids"] {
		// Calculate position IDs accounting for past sequence length
		startPos := 0
		if pastKV != nil {
			startPos = pastKV.SeqLen
		}
		posIDs := make([]int64, batchSize*seqLen)
		for i := range batchSize {
			for j := range seqLen {
				posIDs[i*seqLen+j] = int64(startPos + j)
			}
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "position_ids",
			Shape: []int64{int64(batchSize), int64(seqLen)},
			Data:  posIDs,
		})
	}

	// Add use_cache_branch if needed (ONNX models expect float, not bool)
	if inputNames["use_cache_branch"] {
		useCache := []float32{0}
		if pastKV != nil && pastKV.SeqLen > 0 {
			useCache[0] = 1
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "use_cache_branch",
			Shape: []int64{1},
			Data:  useCache,
		})
	}

	// Add past_key_values inputs if needed
	for _, info := range inputInfo {
		if IsPastKeyValueInput(info.Name) {
			tensor := m.createPastKVTensor(info.Name, pastKV, batchSize)
			inputs = append(inputs, tensor)
		}
	}

	return inputs, nil
}

// createPastKVTensor creates a tensor for past key/value cache.
// Maps input names like "past_key_values.0.key" to stored output names like "present.0.key".
func (m *generativeModel) createPastKVTensor(name string, pastKV *backends.KVCache, batchSize int) backends.NamedTensor {
	// Check if we have past KV cache with stored tensors
	if pastKV != nil && pastKV.SeqLen > 0 && pastKV.Tensors != nil {
		outputName := mapPastToPresent(name)
		if tensor, ok := pastKV.Tensors[outputName]; ok {
			return backends.NamedTensor{
				Name:  name,
				Shape: tensor.Shape,
				Data:  tensor.Data,
			}
		}
	}

	// First step or tensor not found - create zero-sized tensor
	// Shape: [batch, num_heads, 0, head_dim] (common ONNX format)
	numHeads := m.config.NumKVHeads
	if numHeads == 0 {
		numHeads = m.config.NumHeads
	}
	return backends.NamedTensor{
		Name:  name,
		Shape: []int64{int64(batchSize), int64(numHeads), 0, int64(m.config.HeadDim)},
		Data:  []float32{},
	}
}

// extractKVCache extracts the KV-cache from decoder outputs.
// Stores all present.* tensors which will be passed as past_key_values.* inputs
// to subsequent decoder steps.
func (m *generativeModel) extractKVCache(outputs []backends.NamedTensor, batchSize int, pastKV *backends.KVCache) *backends.KVCache {
	tensors := make(map[string]backends.NamedTensor)
	hasKVOutputs := false

	for _, output := range outputs {
		if IsPresentKeyValueOutput(output.Name) {
			hasKVOutputs = true
			data, ok := output.Data.([]float32)
			if ok {
				dataCopy := make([]float32, len(data))
				copy(dataCopy, data)
				shapeCopy := make([]int64, len(output.Shape))
				copy(shapeCopy, output.Shape)
				tensors[output.Name] = backends.NamedTensor{
					Name:  output.Name,
					Shape: shapeCopy,
					Data:  dataCopy,
				}
			}
		}
	}

	if hasKVOutputs {
		// Determine sequence length from the KV cache tensors.
		// The present.*.key tensors have shape [batch, num_heads, seq_len, head_dim].
		// On the first step this equals the prompt length, not 1.
		seqLen := 0
		for _, tensor := range tensors {
			if len(tensor.Shape) >= 3 {
				seqLen = int(tensor.Shape[2]) // seq_len dimension
				break
			}
		}
		return &backends.KVCache{
			SeqLen:    seqLen,
			NumLayers: m.config.NumLayers,
			NumHeads:  m.config.NumKVHeads,
			HeadDim:   m.config.HeadDim,
			BatchSize: batchSize,
			Tensors:   tensors,
		}
	}

	// If no explicit KV-cache output, return updated cache based on input
	if pastKV != nil {
		return &backends.KVCache{
			SeqLen:    pastKV.SeqLen + 1,
			NumLayers: m.config.NumLayers,
			NumHeads:  m.config.NumKVHeads,
			HeadDim:   m.config.HeadDim,
			BatchSize: batchSize,
			Tensors:   pastKV.Tensors,
		}
	}

	return &backends.KVCache{
		SeqLen:    1,
		NumLayers: m.config.NumLayers,
		NumHeads:  m.config.NumKVHeads,
		HeadDim:   m.config.HeadDim,
		BatchSize: batchSize,
	}
}

// DecoderConfig returns configuration needed for generation.
func (m *generativeModel) DecoderConfig() *backends.DecoderConfig {
	return m.config.DecoderConfig
}

// Close releases resources associated with the model.
func (m *generativeModel) Close() error {
	if m.decoderSession != nil {
		if err := m.decoderSession.Close(); err != nil {
			return fmt.Errorf("closing decoder: %w", err)
		}
		m.decoderSession = nil
	}
	return nil
}

// Name returns the model name for logging and debugging.
func (m *generativeModel) Name() string {
	return m.config.ModelPath
}

// Backend returns the backend type this model uses.
func (m *generativeModel) Backend() backends.BackendType {
	return m.backendType
}

// =============================================================================
// Pipeline
// =============================================================================

// TextGenerationPipeline handles decoder-only text generation tasks (GPT, LLaMA, etc.).
// It combines tokenization with autoregressive text generation.
type TextGenerationPipeline struct {
	// Model is the generative decoder model (implements backends.Model).
	Model backends.Model

	// Tokenizer handles text encoding/decoding.
	Tokenizer tokenizers.Tokenizer

	// Generator handles the autoregressive generation loop.
	Generator *Generator

	// GenerationConfig holds generation parameters (also accessible via Generator.Config).
	GenerationConfig *backends.GenerationConfig

	// decoderConfig cached from model for generation
	decoderConfig *backends.DecoderConfig

	// Tool calling support
	modelPath      string
	toolParser     ToolParser
	toolCallFormat string
}

// TextGenerationResult holds the result of text generation.
type TextGenerationResult struct {
	// Text is the generated text.
	Text string

	// TokenIDs are the generated token IDs.
	TokenIDs []int32

	// TokenCount is the number of tokens generated.
	TokenCount int

	// StoppedAtEOS indicates whether generation stopped due to EOS token.
	StoppedAtEOS bool
}

// NewTextGenerationPipeline creates a new TextGenerationPipeline.
func NewTextGenerationPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	config *TextGenerationConfig,
) *TextGenerationPipeline {
	if config == nil {
		config = &TextGenerationConfig{}
	}

	// Resolve configs using shared helpers
	genConfig := ResolveGenerationConfig(config.GenerationConfig)
	decoderConfig := ResolveDecoderConfig(model)
	if decoderConfig.VocabSize == 0 {
		// Use decoder-only defaults for GPT-style models
		decoderConfig = DefaultDecoderOnlyConfig()
	}

	// Create generator using decoder config from model
	generator := NewGeneratorFromDecoderConfig(genConfig, decoderConfig)

	return &TextGenerationPipeline{
		Model:            model,
		Tokenizer:        tokenizer,
		Generator:        generator,
		GenerationConfig: genConfig,
		decoderConfig:    decoderConfig,
		modelPath:        config.ModelPath,
	}
}

// Generate generates text from a prompt.
func (p *TextGenerationPipeline) Generate(ctx context.Context, prompt string) (*TextGenerationResult, error) {
	// Tokenize prompt
	promptTokens := p.Tokenizer.Encode(prompt)
	startTokens := IntToInt32(promptTokens)

	// Create decoder step function that uses our model
	stepFn := func(ctx context.Context, state *DecoderState) ([]float32, *backends.KVCache, error) {
		output, err := p.Model.Forward(ctx, &backends.ModelInputs{
			InputIDs:      [][]int32{state.InputIDs},
			PastKeyValues: state.KVCache,
		})
		if err != nil {
			return nil, nil, err
		}
		return output.Logits[0], output.PastKeyValues, nil
	}

	// Generate using the shared generator
	result, err := p.Generator.Generate(ctx, startTokens, stepFn, true)
	if err != nil {
		return nil, fmt.Errorf("generating text: %w", err)
	}

	// Strip prompt tokens from the result.
	// The decoder includes startTokens[1:] in GeneratedTokens (designed for
	// encoder-decoder models like Whisper). For decoder-only text generation,
	// we only want the newly generated tokens, not the prompt echo.
	generatedTokens := result.TokenIDs
	promptSuffix := len(startTokens) - 1 // decoder skips first token
	if promptSuffix > 0 && len(generatedTokens) >= promptSuffix {
		generatedTokens = generatedTokens[promptSuffix:]
	}

	// Decode only the generated tokens to text
	text := p.Tokenizer.Decode(Int32ToInt(generatedTokens))

	return &TextGenerationResult{
		Text:         text,
		TokenIDs:     generatedTokens,
		TokenCount:   len(generatedTokens),
		StoppedAtEOS: result.StoppedAtEOS,
	}, nil
}

// GenerateWithTokens generates text from pre-tokenized input.
func (p *TextGenerationPipeline) GenerateWithTokens(ctx context.Context, inputTokens []int32) (*TextGenerationResult, error) {
	// Create decoder step function
	stepFn := func(ctx context.Context, state *DecoderState) ([]float32, *backends.KVCache, error) {
		output, err := p.Model.Forward(ctx, &backends.ModelInputs{
			InputIDs:      [][]int32{state.InputIDs},
			PastKeyValues: state.KVCache,
		})
		if err != nil {
			return nil, nil, err
		}
		return output.Logits[0], output.PastKeyValues, nil
	}

	// Generate using the shared generator
	result, err := p.Generator.Generate(ctx, inputTokens, stepFn, true)
	if err != nil {
		return nil, fmt.Errorf("generating text: %w", err)
	}

	// Decode tokens to text
	text := p.Tokenizer.Decode(Int32ToInt(result.TokenIDs))

	return &TextGenerationResult{
		Text:         text,
		TokenIDs:     result.TokenIDs,
		TokenCount:   len(result.TokenIDs),
		StoppedAtEOS: result.StoppedAtEOS,
	}, nil
}

// GenerateWithStreaming generates text and streams tokens as they are generated.
// The callback is called for each generated token. Return false to stop generation.
func (p *TextGenerationPipeline) GenerateWithStreaming(
	ctx context.Context,
	prompt string,
	callback func(token int32, text string) bool,
) (*TextGenerationResult, error) {
	// Tokenize prompt
	promptTokens := p.Tokenizer.Encode(prompt)
	startTokens := IntToInt32(promptTokens)

	// Create decoder step function
	stepFn := func(ctx context.Context, state *DecoderState) ([]float32, *backends.KVCache, error) {
		output, err := p.Model.Forward(ctx, &backends.ModelInputs{
			InputIDs:      [][]int32{state.InputIDs},
			PastKeyValues: state.KVCache,
		})
		if err != nil {
			return nil, nil, err
		}
		return output.Logits[0], output.PastKeyValues, nil
	}

	// Wrap callback to decode token to text
	wrappedCallback := func(token int32) bool {
		text := p.Tokenizer.Decode([]int{int(token)})
		return callback(token, text)
	}

	// Generate with streaming
	result, err := p.Generator.GenerateStreaming(ctx, startTokens, stepFn, true, wrappedCallback)
	if err != nil {
		return nil, fmt.Errorf("generating text: %w", err)
	}

	// Decode all tokens to final text
	text := p.Tokenizer.Decode(Int32ToInt(result.TokenIDs))

	return &TextGenerationResult{
		Text:         text,
		TokenIDs:     result.TokenIDs,
		TokenCount:   len(result.TokenIDs),
		StoppedAtEOS: result.StoppedAtEOS,
	}, nil
}

// GenerateBatch generates text for multiple prompts with true batched decoding.
// All prompts are processed together in a single forward pass per generation step,
// which is significantly more efficient than processing them sequentially.
func (p *TextGenerationPipeline) GenerateBatch(ctx context.Context, prompts []string) ([]*TextGenerationResult, error) {
	if len(prompts) == 0 {
		return nil, nil
	}

	// Single prompt optimization - use non-batched path
	if len(prompts) == 1 {
		result, err := p.Generate(ctx, prompts[0])
		if err != nil {
			return nil, err
		}
		return []*TextGenerationResult{result}, nil
	}

	// Tokenize all prompts
	batchSize := len(prompts)
	tokenizedPrompts := make([][]int32, batchSize)
	maxLen := 0

	for i, prompt := range prompts {
		tokens := p.Tokenizer.Encode(prompt)
		tokenizedPrompts[i] = IntToInt32(tokens)
		if len(tokenizedPrompts[i]) > maxLen {
			maxLen = len(tokenizedPrompts[i])
		}
	}

	// Pad all sequences to the same length (left-padding for decoder-only models)
	padTokenID := p.decoderConfig.PadTokenID

	paddedInputs := make([][]int32, batchSize)
	for i, tokens := range tokenizedPrompts {
		padLen := maxLen - len(tokens)
		paddedInputs[i] = make([]int32, maxLen)
		// Left-pad with pad token
		for j := range padLen {
			paddedInputs[i][j] = padTokenID
		}
		copy(paddedInputs[i][padLen:], tokens)
	}

	// Track which sequences are still generating
	finished := make([]bool, batchSize)
	finishedAtEOS := make([]bool, batchSize)
	generatedTokens := make([][]int32, batchSize)
	for i := range generatedTokens {
		generatedTokens[i] = make([]int32, 0, p.GenerationConfig.MaxNewTokens)
	}

	// Current input IDs for each sequence
	currentInputs := paddedInputs
	var kvCache *backends.KVCache

	// Generation loop
	for step := 0; step < p.GenerationConfig.MaxNewTokens; step++ {
		select {
		case <-ctx.Done():
			return p.buildBatchResults(generatedTokens, finishedAtEOS), ctx.Err()
		default:
		}

		// Check if all sequences are finished
		allFinished := true
		for _, f := range finished {
			if !f {
				allFinished = false
				break
			}
		}
		if allFinished {
			break
		}

		// For KV-cached models, only pass the last token after first step
		inputsForStep := currentInputs
		if kvCache != nil && kvCache.SeqLen > 0 {
			inputsForStep = make([][]int32, batchSize)
			for i := range inputsForStep {
				inputsForStep[i] = currentInputs[i][len(currentInputs[i])-1:]
			}
		}

		// Run batched forward pass
		output, err := p.Model.Forward(ctx, &backends.ModelInputs{
			InputIDs:      inputsForStep,
			PastKeyValues: kvCache,
		})
		if err != nil {
			return nil, fmt.Errorf("forward pass at step %d: %w", step, err)
		}
		kvCache = output.PastKeyValues

		// Select next token for each sequence
		for i := range batchSize {
			if finished[i] {
				continue
			}

			// Select next token using the generator's selection logic
			nextToken := p.Generator.selectNextToken(output.Logits[i], generatedTokens[i])

			// Check for EOS (supports multiple EOS token IDs)
			if p.Generator.isEOS(nextToken) {
				if len(generatedTokens[i]) >= p.GenerationConfig.MinLength {
					finished[i] = true
					finishedAtEOS[i] = true
					continue
				}
				// Force continue - select again without EOS
				logitsCopy := make([]float32, len(output.Logits[i]))
				copy(logitsCopy, output.Logits[i])
				p.Generator.suppressAllEOS(logitsCopy)
				nextToken = p.Generator.selectNextToken(logitsCopy, generatedTokens[i])
			}

			// Append token
			generatedTokens[i] = append(generatedTokens[i], nextToken)
			currentInputs[i] = append(currentInputs[i], nextToken)
		}
	}

	return p.buildBatchResults(generatedTokens, finishedAtEOS), nil
}

// buildBatchResults converts generated token arrays to TextGenerationResult structs.
func (p *TextGenerationPipeline) buildBatchResults(generatedTokens [][]int32, finishedAtEOS []bool) []*TextGenerationResult {
	results := make([]*TextGenerationResult, len(generatedTokens))
	for i, tokens := range generatedTokens {
		text := p.Tokenizer.Decode(Int32ToInt(tokens))
		results[i] = &TextGenerationResult{
			Text:         text,
			TokenIDs:     tokens,
			TokenCount:   len(tokens),
			StoppedAtEOS: finishedAtEOS[i],
		}
	}
	return results
}

// Close releases resources held by the pipeline.
func (p *TextGenerationPipeline) Close() error {
	return p.Model.Close()
}

// SupportsTools returns true if this pipeline supports tool calling.
func (p *TextGenerationPipeline) SupportsTools() bool {
	return p.toolParser != nil
}

// ToolParser returns the tool parser for this pipeline, or nil if not supported.
func (p *TextGenerationPipeline) GetToolParser() ToolParser {
	return p.toolParser
}

// SetToolParser sets the tool parser for this pipeline.
func (p *TextGenerationPipeline) SetToolParser(parser ToolParser) {
	p.toolParser = parser
}

// ToolCallFormat returns the tool call format name (e.g., "functiongemma").
func (p *TextGenerationPipeline) ToolCallFormat() string {
	return p.toolCallFormat
}

// SetToolCallFormat sets the tool call format name.
func (p *TextGenerationPipeline) SetToolCallFormat(format string) {
	p.toolCallFormat = format
}

// ModelPath returns the path to the model directory.
func (p *TextGenerationPipeline) ModelPath() string {
	return p.modelPath
}

// SetModelPath sets the model path (used for tool parser loading).
func (p *TextGenerationPipeline) SetModelPath(path string) {
	p.modelPath = path
}

// =============================================================================
// Loader
// =============================================================================

// TextGenerationPipelineOption is a functional option for configuring TextGenerationPipeline loading.
type TextGenerationPipelineOption func(*TextGenerationConfig)

// WithTextGenerationConfig sets the generation config for the pipeline.
func WithTextGenerationConfig(config *backends.GenerationConfig) TextGenerationPipelineOption {
	return func(c *TextGenerationConfig) {
		c.GenerationConfig = config
	}
}

// WithToolParserFactory sets a custom tool parser factory for the pipeline.
func WithToolParserFactory(factory ToolParserFactory) TextGenerationPipelineOption {
	return func(c *TextGenerationConfig) {
		c.ToolParserFactory = factory
	}
}

// LoadTextGenerationPipeline loads a complete text generation pipeline from a model directory.
// It automatically loads the model, tokenizer, and creates the pipeline.
// If the model has a genai_config.json with tool_call_format, it will load the tool parser.
// This signature matches the encoder-based pipeline loaders for consistency.
func LoadTextGenerationPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...TextGenerationPipelineOption,
) (*TextGenerationPipeline, backends.BackendType, error) {
	// Get session factory from manager
	factory, backendType, err := sessionManager.GetSessionFactoryForModel(modelBackends)
	if err != nil {
		return nil, "", fmt.Errorf("getting session factory: %w", err)
	}

	// Load the model
	model, err := LoadGenerativeModel(modelPath, factory)
	if err != nil {
		return nil, "", fmt.Errorf("loading model: %w", err)
	}

	// Load the tokenizer.
	// Both the Rust and Go tokenizers recognize special token strings
	// (like <bos>, <start_of_turn>) in input text by default.
	tokenizer, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		_ = model.Close()
		return nil, "", fmt.Errorf("loading tokenizer: %w", err)
	}

	// Apply options
	config := &TextGenerationConfig{
		ModelPath: modelPath,
	}
	for _, opt := range opts {
		opt(config)
	}

	// If no explicit GenerationConfig was provided, use the model's generation_config.json.
	// This respects do_sample, temperature, top_k, top_p from the model author.
	if config.GenerationConfig == nil {
		if genCfg := loadGenerationConfig(modelPath); genCfg != nil {
			config.GenerationConfig = buildGenerationConfig(genCfg)
		}
	}

	// Create the pipeline
	pipeline := NewTextGenerationPipeline(model, tokenizer, config)

	// Try to load tool parser from genai_config.json
	if genaiConfig := readGenAIConfig(modelPath); genaiConfig != nil && genaiConfig.ToolCallFormat != "" {
		pipeline.toolCallFormat = genaiConfig.ToolCallFormat
		// If a custom factory was provided, use it
		if config.ToolParserFactory != nil {
			if parser, err := config.ToolParserFactory(modelPath); err == nil {
				pipeline.toolParser = parser
			}
		}
		// Otherwise the caller should set the tool parser manually using SetToolParser
		// since we can't import lib/generation here due to circular dependency
	}

	return pipeline, backendType, nil
}
