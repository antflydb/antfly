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
// Configuration Types
// =============================================================================

// Speech2SeqModelConfig holds parsed configuration for a Speech2Seq model.
// This is loaded from config.json and preprocessor_config.json.
type Speech2SeqModelConfig struct {
	// Path to the model directory
	ModelPath string

	// Paths to ONNX files (if present)
	EncoderPath string
	DecoderPath string

	// Optional split decoder paths for XLA compatibility.
	// When available, these avoid 0-dimension tensor issues on the first step.
	DecoderFirstStepPath string // decoder_model.onnx (no past KV inputs)
	DecoderWithPastPath  string // decoder_with_past_model.onnx (with past KV inputs)

	// Decoder configuration
	DecoderConfig *backends.DecoderConfig

	// Audio preprocessing configuration
	AudioConfig *backends.AudioConfig

	// Architecture details for KV-cache
	NumLayers  int
	NumHeads   int
	HeadDim    int
	HiddenSize int
}

// Speech2SeqConfig holds configuration for creating a Speech2SeqPipeline.
type Speech2SeqConfig struct {
	// AudioConfig for preprocessing. If nil, uses model's default.
	AudioConfig *backends.AudioConfig

	// GenerationConfig for text generation. If nil, uses defaults.
	GenerationConfig *backends.GenerationConfig
}

// Speech2SeqResult is an alias for EncoderDecoderResult for backwards compatibility.
type Speech2SeqResult = EncoderDecoderResult

// =============================================================================
// Configuration Loading
// =============================================================================

// LoadSpeech2SeqModelConfig loads and parses configuration for a Speech2Seq model.
// This is backend-agnostic and can be used by both ONNX and GoMLX backends.
func LoadSpeech2SeqModelConfig(modelPath string) (*Speech2SeqModelConfig, error) {
	// Find encoder ONNX file
	encoderPath := FindONNXFile(modelPath, []string{
		"encoder_model.onnx",
		"encoder.onnx",
	})

	// Find decoder ONNX file (merged or with-past)
	decoderPath := FindONNXFile(modelPath, []string{
		"decoder_model_merged.onnx", // Preferred: merged decoder with KV-cache
		"decoder_with_past_model.onnx",
		"decoder_with_past.onnx",
		"decoder.onnx",
		"decoder_model.onnx",
	})

	// Find split decoder files for XLA compatibility.
	// These avoid 0-dimension tensor issues on the first decoder step.
	decoderFirstStepPath := FindONNXFile(modelPath, []string{
		"decoder_model.onnx", // First step decoder (no past KV inputs)
	})
	decoderWithPastPath := FindONNXFile(modelPath, []string{
		"decoder_with_past_model.onnx", // Subsequent steps decoder (with past KV inputs)
		"decoder_with_past.onnx",
	})

	// Load model configuration from config.json
	rawConfig, err := loadRawSpeech2SeqConfig(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading model config: %w", err)
	}

	// Load preprocessor config if available
	audioConfig := loadAudioPreprocessorConfig(modelPath)
	if audioConfig == nil {
		audioConfig = backends.DefaultAudioConfig()
	}

	// Build decoder config
	decoderConfig := buildSpeech2SeqDecoderConfig(rawConfig)

	// Determine architecture
	numLayers := FirstNonZero(rawConfig.DecoderLayers, rawConfig.NumDecoderLayers, 6)
	numHeads := FirstNonZero(rawConfig.DecoderAttentionHeads, rawConfig.NumAttentionHeads, 8)
	hiddenSize := FirstNonZero(rawConfig.DModel, rawConfig.HiddenSize, 768)
	headDim := hiddenSize / numHeads

	return &Speech2SeqModelConfig{
		ModelPath:            modelPath,
		EncoderPath:          encoderPath,
		DecoderPath:          decoderPath,
		DecoderFirstStepPath: decoderFirstStepPath,
		DecoderWithPastPath:  decoderWithPastPath,
		DecoderConfig:        decoderConfig,
		AudioConfig:          audioConfig,
		NumLayers:            numLayers,
		NumHeads:             numHeads,
		HeadDim:              headDim,
		HiddenSize:           hiddenSize,
	}, nil
}

// IsSpeech2SeqModel checks if a model path contains a Speech2Seq model.
func IsSpeech2SeqModel(path string) bool {
	encoderPath := FindONNXFile(path, []string{
		"encoder_model.onnx",
		"encoder.onnx",
	})
	decoderPath := FindONNXFile(path, []string{
		"decoder_model_merged.onnx",
		"decoder_with_past_model.onnx",
		"decoder_with_past.onnx",
		"decoder.onnx",
		"decoder_model.onnx",
	})

	// Must have both encoder and decoder
	if encoderPath == "" || decoderPath == "" {
		return false
	}

	// Check config.json for speech model types
	rawConfig, err := loadRawSpeech2SeqConfig(path)
	if err != nil {
		return false
	}

	// Check for speech model types
	speechTypes := map[string]bool{
		"whisper":          true,
		"wav2vec2":         true,
		"hubert":           true,
		"speech_to_text":   true,
		"speech_to_text_2": true,
		"seamless_m4t":     true,
	}

	return speechTypes[rawConfig.ModelType]
}

// =============================================================================
// Raw Config Structs and Parsing Helpers
// =============================================================================

// rawSpeech2SeqConfig represents the model's config.json structure.
type rawSpeech2SeqConfig struct {
	// Model type
	ModelType string `json:"model_type"`

	// Vocab and token IDs
	VocabSize           int   `json:"vocab_size"`
	EOSTokenID          any   `json:"eos_token_id"` // Can be int or []int
	BOSTokenID          int32 `json:"bos_token_id"`
	PadTokenID          any   `json:"pad_token_id"`
	DecoderStartTokenID int32 `json:"decoder_start_token_id"`

	// Architecture
	DecoderLayers         int `json:"decoder_layers"`
	NumDecoderLayers      int `json:"num_decoder_layers"`
	DecoderAttentionHeads int `json:"decoder_attention_heads"`
	NumAttentionHeads     int `json:"num_attention_heads"`
	DModel                int `json:"d_model"`
	HiddenSize            int `json:"hidden_size"`

	// Sequence length
	MaxLength             int `json:"max_length"`
	MaxTargetPositions    int `json:"max_target_positions"`
	MaxSourcePositions    int `json:"max_source_positions"`
	MaxPositionEmbeddings int `json:"max_position_embeddings"`

	// Whisper-specific
	ForcedDecoderIds [][]int `json:"forced_decoder_ids"`
	SuppressTokens   []int   `json:"suppress_tokens"`
}

// rawAudioPreprocessorConfig represents preprocessor_config.json for audio models.
type rawAudioPreprocessorConfig struct {
	FeatureSize      int     `json:"feature_size"`
	SamplingRate     int     `json:"sampling_rate"`
	HopLength        int     `json:"hop_length"`
	ChunkLength      int     `json:"chunk_length"`
	NFft             int     `json:"n_fft"`
	NMels            int     `json:"n_mels"`
	PaddingValue     float32 `json:"padding_value"`
	FeatureExtractor string  `json:"feature_extractor_type"`
	ProcessorClass   string  `json:"processor_class"`
}

// loadRawSpeech2SeqConfig loads the model configuration from config.json.
func loadRawSpeech2SeqConfig(path string) (*rawSpeech2SeqConfig, error) {
	configPath := filepath.Join(path, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config.json: %w", err)
	}

	var config rawSpeech2SeqConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config.json: %w", err)
	}

	return &config, nil
}

// loadAudioPreprocessorConfig loads preprocessor_config.json if it exists.
func loadAudioPreprocessorConfig(path string) *backends.AudioConfig {
	configPath := filepath.Join(path, "preprocessor_config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil
	}

	var raw rawAudioPreprocessorConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}

	config := backends.DefaultAudioConfig()

	if raw.SamplingRate > 0 {
		config.SampleRate = raw.SamplingRate
	}
	if raw.FeatureSize > 0 {
		config.FeatureSize = raw.FeatureSize
	}
	if raw.NFft > 0 {
		config.NFft = raw.NFft
	}
	if raw.HopLength > 0 {
		config.HopLength = raw.HopLength
	}
	if raw.ChunkLength > 0 {
		config.ChunkLength = raw.ChunkLength
	}
	if raw.NMels > 0 {
		config.NMels = raw.NMels
	}
	config.PaddingValue = raw.PaddingValue

	return config
}

// buildSpeech2SeqDecoderConfig creates a DecoderConfig from the raw config.
func buildSpeech2SeqDecoderConfig(cfg *rawSpeech2SeqConfig) *backends.DecoderConfig {
	eosTokenID := ParseTokenID(cfg.EOSTokenID)

	// Handle pad_token_id which can be int or null
	var padTokenID int32
	switch v := cfg.PadTokenID.(type) {
	case float64:
		padTokenID = int32(v)
	case nil:
		padTokenID = eosTokenID // Common fallback
	}

	// Max length
	maxLength := FirstNonZero(cfg.MaxLength, cfg.MaxTargetPositions, cfg.MaxPositionEmbeddings, 448)

	numHeads := FirstNonZero(cfg.DecoderAttentionHeads, cfg.NumAttentionHeads, 8)
	hiddenSize := FirstNonZero(cfg.DModel, cfg.HiddenSize, 768)

	// Convert forced_decoder_ids from [][]int to [][]int32
	var forcedDecoderIds [][]int32
	for _, pair := range cfg.ForcedDecoderIds {
		if len(pair) == 2 {
			forcedDecoderIds = append(forcedDecoderIds, []int32{int32(pair[0]), int32(pair[1])})
		}
	}

	// Convert suppress_tokens from []int to []int32
	var suppressTokens []int32
	for _, tok := range cfg.SuppressTokens {
		suppressTokens = append(suppressTokens, int32(tok))
	}

	return &backends.DecoderConfig{
		VocabSize:           cfg.VocabSize,
		MaxLength:           maxLength,
		EOSTokenID:          eosTokenID,
		BOSTokenID:          cfg.BOSTokenID,
		PadTokenID:          padTokenID,
		DecoderStartTokenID: cfg.DecoderStartTokenID,
		NumLayers:           FirstNonZero(cfg.DecoderLayers, cfg.NumDecoderLayers, 6),
		NumHeads:            numHeads,
		HeadDim:             hiddenSize / numHeads,
		ForcedDecoderIds:    forcedDecoderIds,
		SuppressTokens:      suppressTokens,
	}
}

// =============================================================================
// Model Management (Audio Encoder + Decoder)
// =============================================================================

// Ensure speech2SeqModel implements backends.Model
var _ backends.Model = (*speech2SeqModel)(nil)

// speech2SeqModel implements backends.Model for speech-to-text tasks.
// It uses separate encoder and decoder sessions (Whisper, etc.).
type speech2SeqModel struct {
	config *Speech2SeqModelConfig

	encoderSession backends.Session
	decoderSession backends.Session

	// Optional split decoder sessions for XLA compatibility.
	// When available, these avoid 0-dimension tensor issues on the first step.
	decoderFirstStepSession backends.Session // First step (no past KV inputs)
	decoderWithPastSession  backends.Session // Subsequent steps (with past KV inputs)
	useSplitDecoders        bool             // True if split decoders are loaded

	backendType backends.BackendType
}

// NewSpeech2SeqModel creates a Model from encoder and decoder sessions.
func NewSpeech2SeqModel(
	config *Speech2SeqModelConfig,
	encoderSession backends.Session,
	decoderSession backends.Session,
	backendType backends.BackendType,
) backends.Model {
	return &speech2SeqModel{
		config:         config,
		encoderSession: encoderSession,
		decoderSession: decoderSession,
		backendType:    backendType,
	}
}

// LoadSpeech2SeqModel loads a speech2seq Model using the given session factory.
// It automatically discovers encoder and decoder ONNX files and creates sessions.
// When split decoders are available (decoder_model.onnx + decoder_with_past_model.onnx),
// they are used for XLA compatibility to avoid 0-dimension tensor issues.
func LoadSpeech2SeqModel(modelPath string, factory backends.SessionFactory, opts ...backends.SessionOption) (backends.Model, error) {
	// Load configuration
	config, err := LoadSpeech2SeqModelConfig(modelPath)
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

	model := &speech2SeqModel{
		config:         config,
		encoderSession: encoderSession,
		backendType:    factory.Backend(),
	}

	// Try to load split decoders for XLA compatibility.
	// Split decoders avoid 0-dimension tensor issues on the first step.
	if config.DecoderFirstStepPath != "" && config.DecoderWithPastPath != "" {
		// Load first-step decoder (no past KV inputs)
		firstStepSession, err := factory.CreateSession(config.DecoderFirstStepPath, opts...)
		if err == nil {
			// Load with-past decoder
			withPastSession, err := factory.CreateSession(config.DecoderWithPastPath, opts...)
			if err == nil {
				model.decoderFirstStepSession = firstStepSession
				model.decoderWithPastSession = withPastSession
				model.useSplitDecoders = true
				// Also keep the merged decoder as fallback (not needed but consistent)
				decoderSession, _ := factory.CreateSession(config.DecoderPath, opts...)
				model.decoderSession = decoderSession
				return model, nil
			}
			// Failed to load with-past decoder, close first-step session
			_ = firstStepSession.Close()
		}
		// Fall through to use merged decoder
	}

	// Create merged decoder session (fallback)
	decoderSession, err := factory.CreateSession(config.DecoderPath, opts...)
	if err != nil {
		_ = encoderSession.Close()
		return nil, fmt.Errorf("creating decoder session: %w", err)
	}
	model.decoderSession = decoderSession

	return model, nil
}

// Forward runs encoder or decoder based on inputs.
// - If AudioFeatures is set (and EncoderOutput is nil): runs audio encoder, returns EncoderOutput
// - If EncoderOutput is set: runs decoder step, returns Logits and PastKeyValues
func (m *speech2SeqModel) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	if inputs == nil {
		return nil, fmt.Errorf("nil inputs")
	}

	// If encoder output provided, run decoder
	if inputs.EncoderOutput != nil {
		return m.runDecoder(ctx, inputs)
	}

	// Otherwise run encoder on audio features
	if len(inputs.AudioFeatures) == 0 {
		return nil, fmt.Errorf("no audio features or encoder output provided")
	}

	return m.runEncoder(ctx, inputs)
}

// runEncoder encodes preprocessed audio features into encoder hidden states.
func (m *speech2SeqModel) runEncoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	// Whisper expects input_features with shape [batch, n_mels, time]
	// Our AudioProcessor outputs [time, n_mels], so we need to transpose
	batch := inputs.AudioBatch
	if batch == 0 {
		batch = 1
	}
	time := inputs.AudioTime
	nMels := inputs.AudioMels

	// Transpose from [time, n_mels] to [n_mels, time]
	transposed := make([]float32, len(inputs.AudioFeatures))
	for t := range time {
		for mel := range nMels {
			// Original: [t, mel] = t * nMels + mel
			// Target: [mel, t] = mel * time + t
			transposed[mel*time+t] = inputs.AudioFeatures[t*nMels+mel]
		}
	}

	// Create input tensor [batch, n_mels, time]
	input := backends.NamedTensor{
		Name:  m.getEncoderInputName(),
		Shape: []int64{int64(batch), int64(nMels), int64(time)},
		Data:  transposed,
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
func (m *speech2SeqModel) getEncoderInputName() string {
	// Check session input info for the actual name
	inputInfo := m.encoderSession.InputInfo()
	if len(inputInfo) > 0 {
		return inputInfo[0].Name
	}
	// Default name for Whisper-style encoders
	return "input_features"
}

// runDecoder performs one step of autoregressive decoding.
func (m *speech2SeqModel) runDecoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
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

	// Choose the appropriate decoder session.
	// When using split decoders (for XLA compatibility):
	// - First step (no past KV): use decoderFirstStepSession
	// - Subsequent steps (with past KV): use decoderWithPastSession
	var decoderSession backends.Session
	var isFirstStep bool
	if m.useSplitDecoders {
		if pastKeyValues == nil || pastKeyValues.SeqLen == 0 {
			decoderSession = m.decoderFirstStepSession
			isFirstStep = true
		} else {
			decoderSession = m.decoderWithPastSession
			isFirstStep = false
		}
	} else {
		decoderSession = m.decoderSession
		isFirstStep = pastKeyValues == nil || pastKeyValues.SeqLen == 0
	}

	// Build decoder inputs
	tensorInputs, err := m.buildDecoderInputs(flatInputIDs, batchSize, seqLen, encoderOutput, pastKeyValues, decoderSession, isFirstStep)
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

// buildDecoderInputs creates the input tensors for the decoder.
// When using split decoders, the decoderSession parameter indicates which decoder is being used,
// and isFirstStep indicates whether this is the first decoder step (no past KV needed).
func (m *speech2SeqModel) buildDecoderInputs(inputIDs []int64, batchSize, seqLen int, encoderOutput *backends.EncoderOutput, pastKV *backends.KVCache, decoderSession backends.Session, isFirstStep bool) ([]backends.NamedTensor, error) {
	var inputs []backends.NamedTensor

	// Get decoder input names from the session being used
	inputInfo := decoderSession.InputInfo()
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

	// Add encoder hidden states
	if inputNames["encoder_hidden_states"] || inputNames["encoder_outputs"] {
		name := "encoder_hidden_states"
		if inputNames["encoder_outputs"] {
			name = "encoder_outputs"
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  name,
			Shape: []int64{int64(encoderOutput.Shape[0]), int64(encoderOutput.Shape[1]), int64(encoderOutput.Shape[2])},
			Data:  encoderOutput.HiddenStates,
		})
	}

	// Add encoder attention mask if needed (all 1s for full audio)
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
		// Find the expected data type for use_cache_branch
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

	// Add past_key_values inputs if needed.
	// Note: When using split decoders, the first-step decoder (decoder_model.onnx) doesn't have
	// past_key_values inputs, so this loop won't add anything for the first step.
	// The with-past decoder (decoder_with_past_model.onnx) requires past_key_values.
	encoderSeqLen := encoderOutput.Shape[1]
	for _, info := range inputInfo {
		if IsPastKeyValueInput(info.Name) {
			tensor := m.createPastKVTensor(info.Name, pastKV, batchSize, encoderSeqLen)
			inputs = append(inputs, tensor)
		}
	}

	return inputs, nil
}

// createPastKVTensor creates a tensor for past key/value cache.
// Maps input names like "past_key_values.0.decoder.key" to stored output names like "present.0.decoder.key".
func (m *speech2SeqModel) createPastKVTensor(name string, pastKV *backends.KVCache, batchSize int, encoderSeqLen int) backends.NamedTensor {
	numHeads := m.config.NumHeads
	headDim := m.config.HeadDim

	if numHeads == 0 {
		numHeads = 8
	}
	if headDim == 0 {
		headDim = 64
	}

	// If we have past KV cache with stored tensors, look up the cached data.
	// Map input name (past_key_values.*) to output name (present.*) for lookup.
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

	// No cached data - create empty tensor.
	// Note: For XLA/GoMLX backends, we use split decoders to avoid this code path
	// on the first step, since 0-dimension tensors cause issues with XLA.
	// For ONNX Runtime, 0-dimension tensors work fine.
	var seqLen int
	if pastKV != nil && pastKV.SeqLen > 0 {
		if isEncoderKVTensor(name) {
			seqLen = encoderSeqLen
		} else {
			seqLen = pastKV.SeqLen
		}
	} else {
		seqLen = 0
	}

	size := batchSize * numHeads * seqLen * headDim
	data := make([]float32, size)
	return backends.NamedTensor{
		Name:  name,
		Shape: []int64{int64(batchSize), int64(numHeads), int64(seqLen), int64(headDim)},
		Data:  data,
	}
}

// extractKVCache extracts the KV-cache from decoder outputs.
// Encoder cross-attention KV tensors are carried forward from the previous
// step when the current decoder doesn't re-emit them.
func (m *speech2SeqModel) extractKVCache(outputs []backends.NamedTensor, batchSize int, pastKV *backends.KVCache) *backends.KVCache {
	// Collect all present_key_values or present.* outputs
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

	// Carry forward encoder cross-attention KV tensors from the previous
	// step when the with-past decoder doesn't re-emit them.
	if pastKV != nil && pastKV.Tensors != nil {
		for name, tensor := range pastKV.Tensors {
			if _, exists := tensors[name]; !exists && isEncoderKVTensor(name) {
				tensors[name] = tensor
				hasKVOutputs = true
			}
		}
	}

	if hasKVOutputs {
		seqLen := 1
		if pastKV != nil {
			seqLen = pastKV.SeqLen + 1
		}
		return &backends.KVCache{
			SeqLen:    seqLen,
			NumLayers: m.config.NumLayers,
			NumHeads:  m.config.NumHeads,
			HeadDim:   m.config.HeadDim,
			BatchSize: batchSize,
			Tensors:   tensors,
		}
	}

	if pastKV != nil {
		return &backends.KVCache{
			SeqLen:    pastKV.SeqLen + 1,
			NumLayers: m.config.NumLayers,
			NumHeads:  m.config.NumHeads,
			HeadDim:   m.config.HeadDim,
			BatchSize: batchSize,
			Tensors:   pastKV.Tensors,
		}
	}

	return &backends.KVCache{
		SeqLen:    1,
		NumLayers: m.config.NumLayers,
		NumHeads:  m.config.NumHeads,
		HeadDim:   m.config.HeadDim,
		BatchSize: batchSize,
	}
}

// DecoderConfig returns configuration needed for generation.
func (m *speech2SeqModel) DecoderConfig() *backends.DecoderConfig {
	return m.config.DecoderConfig
}

// AudioConfig returns configuration for audio preprocessing.
func (m *speech2SeqModel) AudioConfig() *backends.AudioConfig {
	return m.config.AudioConfig
}

// Close releases resources associated with the model.
func (m *speech2SeqModel) Close() error {
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
			errs = append(errs, fmt.Errorf("closing first-step decoder: %w", err))
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
func (m *speech2SeqModel) Name() string {
	return m.config.ModelPath
}

// Backend returns the backend type this model uses.
func (m *speech2SeqModel) Backend() backends.BackendType {
	return m.backendType
}

// =============================================================================
// Pipeline
// =============================================================================

// Speech2SeqPipeline handles speech-to-text tasks like transcription.
// It combines audio preprocessing, audio encoding, and autoregressive text generation.
type Speech2SeqPipeline struct {
	// EncoderDecoderPipeline provides shared generation functionality.
	*EncoderDecoderPipeline

	// AudioProcessor handles audio preprocessing.
	AudioProcessor *AudioProcessor
}

// NewSpeech2SeqPipeline creates a new Speech2SeqPipeline.
func NewSpeech2SeqPipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	config *Speech2SeqConfig,
) *Speech2SeqPipeline {
	if config == nil {
		config = &Speech2SeqConfig{}
	}

	// Resolve audio config
	audioConfig := ResolveAudioConfig(model, config.AudioConfig)

	// Create base encoder-decoder pipeline
	base := NewEncoderDecoderPipeline(model, tokenizer, config.GenerationConfig)

	return &Speech2SeqPipeline{
		EncoderDecoderPipeline: base,
		AudioProcessor:         NewAudioProcessor(audioConfig),
	}
}

// Transcribe processes audio bytes and generates transcription text.
func (p *Speech2SeqPipeline) Transcribe(ctx context.Context, audioData []byte) (*Speech2SeqResult, error) {
	// Encode audio
	encoderOutput, err := p.encodeAudio(ctx, audioData)
	if err != nil {
		return nil, err
	}

	// Get start tokens (decoder start token for Whisper)
	startTokens := p.GetStartTokens("")

	// Generate using shared base
	return p.GenerateFromEncoderOutput(ctx, encoderOutput, startTokens)
}

// TranscribeSamples processes raw audio samples and generates transcription text.
// Samples should be float32, mono, at the model's expected sample rate.
func (p *Speech2SeqPipeline) TranscribeSamples(ctx context.Context, samples []float32) (*Speech2SeqResult, error) {
	// Encode audio samples
	encoderOutput, err := p.encodeAudioSamples(ctx, samples)
	if err != nil {
		return nil, err
	}

	// Get start tokens
	startTokens := p.GetStartTokens("")

	// Generate using shared base
	return p.GenerateFromEncoderOutput(ctx, encoderOutput, startTokens)
}

// TranscribeWithStreaming processes audio and streams generated tokens.
// The callback is called for each generated token. Return false to stop generation.
func (p *Speech2SeqPipeline) TranscribeWithStreaming(
	ctx context.Context,
	audioData []byte,
	callback func(token int32, text string) bool,
) (*Speech2SeqResult, error) {
	// Encode audio
	encoderOutput, err := p.encodeAudio(ctx, audioData)
	if err != nil {
		return nil, err
	}

	// Get start tokens
	startTokens := p.GetStartTokens("")

	// Generate with streaming using shared base
	return p.GenerateFromEncoderOutputStreaming(ctx, encoderOutput, startTokens, callback)
}

// encodeAudio preprocesses and encodes audio bytes.
func (p *Speech2SeqPipeline) encodeAudio(ctx context.Context, audioData []byte) (*backends.EncoderOutput, error) {
	// Preprocess audio to mel spectrogram
	features, numFrames, err := p.AudioProcessor.Process(audioData)
	if err != nil {
		return nil, fmt.Errorf("preprocessing audio: %w", err)
	}

	return p.encodeFeatures(ctx, features, numFrames)
}

// encodeAudioSamples preprocesses and encodes raw audio samples.
func (p *Speech2SeqPipeline) encodeAudioSamples(ctx context.Context, samples []float32) (*backends.EncoderOutput, error) {
	// Process samples to mel spectrogram
	features, numFrames := p.AudioProcessor.ProcessSamples(samples)

	return p.encodeFeatures(ctx, features, numFrames)
}

// encodeFeatures runs the encoder on preprocessed audio features.
func (p *Speech2SeqPipeline) encodeFeatures(ctx context.Context, features []float32, numFrames int) (*backends.EncoderOutput, error) {
	nMels := p.AudioProcessor.Config.NMels

	// Encode audio using Forward with audio inputs
	encodeOutput, err := p.Model.Forward(ctx, &backends.ModelInputs{
		AudioFeatures: features,
		AudioBatch:    1,
		AudioTime:     numFrames,
		AudioMels:     nMels,
	})
	if err != nil {
		return nil, fmt.Errorf("encoding audio: %w", err)
	}

	return encodeOutput.EncoderOutput, nil
}

// =============================================================================
// Loader
// =============================================================================

// Speech2SeqPipelineOption is a functional option for configuring Speech2SeqPipeline loading.
type Speech2SeqPipelineOption func(*Speech2SeqConfig)

// WithSpeech2SeqAudioConfig sets the audio config for the pipeline.
func WithSpeech2SeqAudioConfig(config *backends.AudioConfig) Speech2SeqPipelineOption {
	return func(c *Speech2SeqConfig) {
		c.AudioConfig = config
	}
}

// WithSpeech2SeqGenerationConfig sets the generation config for the pipeline.
func WithSpeech2SeqGenerationConfig(config *backends.GenerationConfig) Speech2SeqPipelineOption {
	return func(c *Speech2SeqConfig) {
		c.GenerationConfig = config
	}
}

// LoadSpeech2SeqPipeline loads a complete Speech2Seq pipeline from a model directory.
// It automatically loads the model, tokenizer, and creates the pipeline.
func LoadSpeech2SeqPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...Speech2SeqPipelineOption,
) (*Speech2SeqPipeline, backends.BackendType, error) {
	// Get session factory from manager
	factory, backendType, err := sessionManager.GetSessionFactoryForModel(modelBackends)
	if err != nil {
		return nil, "", fmt.Errorf("getting session factory: %w", err)
	}

	// Load the model
	model, err := LoadSpeech2SeqModel(modelPath, factory)
	if err != nil {
		return nil, "", fmt.Errorf("loading model: %w", err)
	}

	// Load the tokenizer
	tokenizer, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		_ = model.Close()
		return nil, "", fmt.Errorf("loading tokenizer: %w", err)
	}

	// Apply options
	config := &Speech2SeqConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// Create the pipeline
	pipeline := NewSpeech2SeqPipeline(model, tokenizer, config)

	return pipeline, backendType, nil
}
