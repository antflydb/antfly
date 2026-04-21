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
	"fmt"
	"strings"

	"github.com/gomlx/gomlx/pkg/core/tensors/bucketing"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// =============================================================================
// VLM Model Detection
// =============================================================================

// IsEncoderDecoderVLMModel checks if a model path contains an encoder-decoder VLM
// (e.g., Florence-2). Detected by the presence of vision_encoder.onnx,
// embed_tokens.onnx, and encoder_model.onnx.
func IsEncoderDecoderVLMModel(path string) bool {
	visionEncoder := FindONNXFile(path, []string{"vision_encoder.onnx"})
	embedTokens := FindONNXFile(path, []string{"embed_tokens.onnx"})
	encoderModel := FindONNXFile(path, []string{"encoder_model.onnx"})

	return visionEncoder != "" && embedTokens != "" && encoderModel != ""
}

// IsDecoderOnlyVLMModel checks if a model path contains a decoder-only VLM
// (e.g., Moondream2). Detected by the presence of vision_encoder.onnx and
// embed_tokens.onnx, with no encoder_model.onnx (which would indicate an
// encoder-decoder VLM like Florence-2).
func IsDecoderOnlyVLMModel(path string) bool {
	visionEncoder := FindONNXFile(path, []string{"vision_encoder.onnx"})
	embedTokens := FindONNXFile(path, []string{"embed_tokens.onnx"})
	encoderModel := FindONNXFile(path, []string{"encoder_model.onnx"})

	return visionEncoder != "" && embedTokens != "" && encoderModel == ""
}

// =============================================================================
// Encoder-Decoder VLM Model
// =============================================================================

// encoderDecoderVLMModel implements backends.Model for encoder-decoder VLM
// architectures (e.g., Florence-2). Uses a multi-stage encoder:
//   - vision_encoder: pixel_values → image_features
//   - embed_tokens: input_ids → text_embeddings
//   - encoder_model: inputs_embeds (concat of image_features + text_embeddings) → hidden_states
//   - decoder: hidden_states + decoder_input_ids → logits
type encoderDecoderVLMModel struct {
	config *Vision2SeqModelConfig

	// Model sessions
	visionEncoderSession backends.Session // vision_encoder.onnx
	embedTokensSession   backends.Session // embed_tokens.onnx
	encoderModelSession  backends.Session // encoder_model.onnx
	decoderSession       backends.Session // decoder_model_merged.onnx (ONNX Runtime fallback)

	// Split decoder sessions for GoMLX backends (XLA, Go, CoreML).
	// The merged decoder's ONNX If node cannot be evaluated at runtime by these
	// backends. Instead we use separate ONNX files (decoder_model.onnx and
	// decoder_with_past_model.onnx) that are purpose-built for each phase.
	decoderFirstStepSession backends.Session // decoder_model.onnx (first step, no KV cache)
	decoderWithPastSession  backends.Session // decoder_with_past_model.onnx (subsequent steps, with KV cache)
	useSplitDecoders        bool

	// kvBucketStrategy buckets past_key_values sequence lengths to reduce
	// the number of unique shapes seen by JIT backends (XLA, CoreML).
	// Without this, each autoregressive step produces a unique KV cache
	// shape, triggering a separate compilation (~60s each on XLA CPU).
	// With Pow2 bucketing, ~7 compilations cover 128 tokens.
	//
	// Trade-off: zero-padded KV positions produce attention score 0, receiving
	// softmax weight 1/Z. Since padded V=0, their output contribution is
	// zero, but they dilute real attention weights by the factor
	// sum(exp(real_scores)) / (sum(exp(real_scores)) + num_padded).
	// LayerNorm and residual connections partially compensate. The padding
	// is trimmed from present outputs after each step (trimPresentDecoderKV)
	// so the dilution does not compound across steps.
	kvBucketStrategy bucketing.Strategy

	backendType backends.BackendType
}

// NewEncoderDecoderVLMModel creates a Model for encoder-decoder VLM architecture.
func NewEncoderDecoderVLMModel(
	config *Vision2SeqModelConfig,
	visionEncoder backends.Session,
	embedTokens backends.Session,
	encoderModel backends.Session,
	decoder backends.Session,
	backendType backends.BackendType,
) backends.Model {
	return &encoderDecoderVLMModel{
		config:               config,
		visionEncoderSession: visionEncoder,
		embedTokensSession:   embedTokens,
		encoderModelSession:  encoderModel,
		decoderSession:       decoder,
		backendType:          backendType,
	}
}

// LoadEncoderDecoderVLMModel loads an encoder-decoder VLM model using the given session factory.
func LoadEncoderDecoderVLMModel(modelPath string, factory backends.SessionFactory, opts ...backends.SessionOption) (backends.Model, error) {
	// Load configuration
	config, err := LoadVision2SeqModelConfig(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading model config: %w", err)
	}

	// Find all required ONNX files
	visionEncoderPath := FindONNXFile(modelPath, []string{"vision_encoder.onnx"})
	embedTokensPath := FindONNXFile(modelPath, []string{"embed_tokens.onnx"})
	encoderModelPath := FindONNXFile(modelPath, []string{"encoder_model.onnx"})
	decoderPath := FindONNXFile(modelPath, []string{
		"decoder_model_merged.onnx",
		"decoder_with_past.onnx",
		"decoder.onnx",
		"decoder_model.onnx",
	})

	if visionEncoderPath == "" {
		return nil, fmt.Errorf("vision_encoder.onnx not found in %s", modelPath)
	}
	if embedTokensPath == "" {
		return nil, fmt.Errorf("embed_tokens.onnx not found in %s", modelPath)
	}
	if encoderModelPath == "" {
		return nil, fmt.Errorf("encoder_model.onnx not found in %s", modelPath)
	}
	if decoderPath == "" {
		return nil, fmt.Errorf("decoder ONNX file not found in %s", modelPath)
	}

	// Update config with correct encoder path (encoder_model, not vision_encoder)
	config.EncoderPath = encoderModelPath
	config.DecoderPath = decoderPath

	// Create sessions
	visionEncoderSession, err := factory.CreateSession(visionEncoderPath, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating vision encoder session: %w", err)
	}

	embedTokensSession, err := factory.CreateSession(embedTokensPath, opts...)
	if err != nil {
		_ = visionEncoderSession.Close()
		return nil, fmt.Errorf("creating embed_tokens session: %w", err)
	}

	encoderModelSession, err := factory.CreateSession(encoderModelPath, opts...)
	if err != nil {
		_ = visionEncoderSession.Close()
		_ = embedTokensSession.Close()
		return nil, fmt.Errorf("creating encoder_model session: %w", err)
	}

	model := &encoderDecoderVLMModel{
		config:               config,
		visionEncoderSession: visionEncoderSession,
		embedTokensSession:   embedTokensSession,
		encoderModelSession:  encoderModelSession,
		backendType:          factory.Backend(),
	}

	closeOnError := func() {
		_ = visionEncoderSession.Close()
		_ = embedTokensSession.Close()
		_ = encoderModelSession.Close()
	}

	// Create the main decoder session
	decoderSession, err := factory.CreateSession(decoderPath, opts...)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("creating decoder session: %w", err)
	}
	model.decoderSession = decoderSession

	// Try to load split decoders for GoMLX backends (XLA, Go, CoreML).
	// These backends cannot evaluate ONNX If nodes at runtime, so the merged
	// decoder (with use_cache_branch=false baked in) disables KV caching.
	// The separate decoder_model.onnx and decoder_with_past_model.onnx files
	// are purpose-built for each phase and don't contain If nodes.
	// ONNX Runtime handles If nodes natively, so it uses the merged decoder.
	isGoMLXBackend := false
	switch factory.Backend() {
	case backends.BackendGo, backends.BackendXLA, backends.BackendCoreML:
		isGoMLXBackend = true
	}
	if isGoMLXBackend && config.DecoderFirstStepPath != "" && config.DecoderWithPastPath != "" {
		firstStepSession, err := factory.CreateSession(config.DecoderFirstStepPath, opts...)
		if err == nil {
			// The with-past decoder may have been exported with fixed sequence
			// length dimensions (e.g., inputs_embeds dim[1]=16 from HuggingFace
			// optimum tracing). Override to dynamic so it accepts seq_len=1
			// during KV-cached decoding.
			withPastOpts := append(opts, backends.WithDynamicAxes([]backends.DynamicAxisOverride{
				{InputName: "inputs_embeds", Axis: 1, ParamName: "decoder_sequence_length"},
				{InputName: "input_ids", Axis: 1, ParamName: "decoder_sequence_length"},
			}))
			withPastSession, err := factory.CreateSession(config.DecoderWithPastPath, withPastOpts...)
			if err == nil {
				model.decoderFirstStepSession = firstStepSession
				model.decoderWithPastSession = withPastSession
				model.useSplitDecoders = true

				// Enable KV cache shape bucketing for JIT backends.
				// XLA and CoreML compile a separate program per unique
				// input shape; without bucketing, each autoregressive
				// step triggers a new compilation (~60s on XLA CPU).
				switch factory.Backend() {
				case backends.BackendXLA, backends.BackendCoreML:
					model.kvBucketStrategy = bucketing.Pow2()
				}
			} else {
				_ = firstStepSession.Close()
			}
		}
	}

	return model, nil
}

// Forward runs the encoder-decoder VLM model.
// - If ImagePixels is set (and EncoderOutput is nil): runs multi-stage encoder
// - If EncoderOutput is set: runs decoder step
func (m *encoderDecoderVLMModel) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	if inputs == nil {
		return nil, fmt.Errorf("nil inputs")
	}

	// If encoder output provided, run decoder
	if inputs.EncoderOutput != nil {
		return m.runDecoder(ctx, inputs)
	}

	// Otherwise run multi-stage encoder
	if len(inputs.ImagePixels) == 0 {
		return nil, fmt.Errorf("no image pixels or encoder output provided")
	}

	return m.runEncoder(ctx, inputs)
}

// runEncoder runs the multi-stage encoder.
// 1. vision_encoder(pixel_values) → image_features
// 2. embed_tokens(input_ids) → prompt_embeds
// 3. concat([image_features, prompt_embeds]) → inputs_embeds
// 4. encoder_model(inputs_embeds) → hidden_states
func (m *encoderDecoderVLMModel) runEncoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	batchSize := inputs.ImageBatch

	// Step 1: Run vision encoder on pixel_values
	pixelValues := backends.NamedTensor{
		Name:  "pixel_values",
		Shape: []int64{int64(batchSize), int64(inputs.ImageChannels), int64(inputs.ImageHeight), int64(inputs.ImageWidth)},
		Data:  inputs.ImagePixels,
	}

	visionOutputs, err := m.visionEncoderSession.Run([]backends.NamedTensor{pixelValues})
	if err != nil {
		return nil, fmt.Errorf("running vision encoder: %w", err)
	}

	if len(visionOutputs) == 0 {
		return nil, fmt.Errorf("no output from vision encoder")
	}

	// Get image features [batch, image_seq_len, hidden_size]
	imageFeatures := visionOutputs[0]
	imageFeaturesData, ok := imageFeatures.Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("vision encoder output is not float32")
	}

	if len(imageFeatures.Shape) != 3 {
		return nil, fmt.Errorf("unexpected image features shape: %v (expected 3D)", imageFeatures.Shape)
	}

	imageSeqLen := int(imageFeatures.Shape[1])
	hiddenSize := int(imageFeatures.Shape[2])

	// Step 2: Get prompt tokens and run embed_tokens
	// For Florence-2, prompts are embedded and concatenated with image features
	promptTokenIDs := inputs.InputIDs
	var promptLen int
	var promptEmbedsData []float32

	if len(promptTokenIDs) > 0 && len(promptTokenIDs[0]) > 0 {
		promptLen = len(promptTokenIDs[0])

		// Flatten prompt tokens to int64
		flatPromptTokens := make([]int64, batchSize*promptLen)
		for i := range batchSize {
			for j := 0; j < promptLen; j++ {
				if i < len(promptTokenIDs) && j < len(promptTokenIDs[i]) {
					flatPromptTokens[i*promptLen+j] = int64(promptTokenIDs[i][j])
				}
			}
		}

		inputIdsTensor := backends.NamedTensor{
			Name:  "input_ids",
			Shape: []int64{int64(batchSize), int64(promptLen)},
			Data:  flatPromptTokens,
		}

		embedOutputs, err := m.embedTokensSession.Run([]backends.NamedTensor{inputIdsTensor})
		if err != nil {
			return nil, fmt.Errorf("running embed_tokens: %w", err)
		}

		if len(embedOutputs) == 0 {
			return nil, fmt.Errorf("no output from embed_tokens")
		}

		var embedOk bool
		promptEmbedsData, embedOk = embedOutputs[0].Data.([]float32)
		if !embedOk {
			return nil, fmt.Errorf("embed_tokens output is not float32")
		}
	}

	// Step 3: Concatenate [image_features | prompt_embeds] → inputs_embeds
	totalSeqLen := imageSeqLen + promptLen
	inputsEmbeds := make([]float32, batchSize*totalSeqLen*hiddenSize)

	for b := range batchSize {
		// Copy image features
		for s := range imageSeqLen {
			srcIdx := b*imageSeqLen*hiddenSize + s*hiddenSize
			dstIdx := b*totalSeqLen*hiddenSize + s*hiddenSize
			copy(inputsEmbeds[dstIdx:dstIdx+hiddenSize], imageFeaturesData[srcIdx:srcIdx+hiddenSize])
		}
		// Copy prompt embeds
		if promptLen > 0 {
			for s := 0; s < promptLen; s++ {
				srcIdx := b*promptLen*hiddenSize + s*hiddenSize
				dstIdx := b*totalSeqLen*hiddenSize + (imageSeqLen+s)*hiddenSize
				copy(inputsEmbeds[dstIdx:dstIdx+hiddenSize], promptEmbedsData[srcIdx:srcIdx+hiddenSize])
			}
		}
	}

	// Step 4: Create attention mask (all 1s)
	attentionMask := make([]int64, batchSize*totalSeqLen)
	for i := range attentionMask {
		attentionMask[i] = 1
	}

	// Step 5: Run encoder_model with inputs_embeds
	encoderInputs := []backends.NamedTensor{
		{
			Name:  "inputs_embeds",
			Shape: []int64{int64(batchSize), int64(totalSeqLen), int64(hiddenSize)},
			Data:  inputsEmbeds,
		},
		{
			Name:  "attention_mask",
			Shape: []int64{int64(batchSize), int64(totalSeqLen)},
			Data:  attentionMask,
		},
	}

	encoderOutputs, err := m.encoderModelSession.Run(encoderInputs)
	if err != nil {
		return nil, fmt.Errorf("running encoder_model: %w", err)
	}

	if len(encoderOutputs) == 0 {
		return nil, fmt.Errorf("no output from encoder_model")
	}

	// Extract encoder hidden states
	outputTensor := encoderOutputs[0]
	hiddenStates, ok := outputTensor.Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("encoder_model output is not float32")
	}

	encoderOutput := &backends.EncoderOutput{
		HiddenStates: hiddenStates,
		Shape:        [3]int{int(outputTensor.Shape[0]), int(outputTensor.Shape[1]), int(outputTensor.Shape[2])},
	}

	return &backends.ModelOutput{
		EncoderOutput: encoderOutput,
	}, nil
}

// runDecoder performs one step of autoregressive decoding.
func (m *encoderDecoderVLMModel) runDecoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	inputIDs := inputs.InputIDs
	encoderOutput := inputs.EncoderOutput
	pastKeyValues := inputs.PastKeyValues

	batchSize := len(inputIDs)
	if batchSize == 0 {
		return nil, fmt.Errorf("empty input")
	}

	seqLen := len(inputIDs[0])

	// Flatten input IDs to int64
	flatInputIDs := make([]int64, batchSize*seqLen)
	for i := range batchSize {
		for j := range seqLen {
			flatInputIDs[i*seqLen+j] = int64(inputIDs[i][j])
		}
	}

	// Choose the appropriate decoder session:
	// - Use split decoders if available (GoMLX backends)
	// - Use first-step session if no past KV cache
	// - Use with-past session for subsequent steps
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

	// Build decoder inputs using the selected session's input info
	tensorInputs, err := m.buildDecoderInputsForSession(decoderSession, flatInputIDs, batchSize, seqLen, encoderOutput, pastKeyValues)
	if err != nil {
		return nil, fmt.Errorf("building decoder inputs: %w", err)
	}

	// Pad decoder KV cache tensors to bucketed shapes so JIT backends
	// see fewer unique input shapes and reuse compiled programs.
	var realPastSeqLen int
	if m.kvBucketStrategy != nil && !isFirstStep {
		realPastSeqLen = pastKeyValues.SeqLen
		bucketedSeqLen := m.kvBucketStrategy.Bucket(realPastSeqLen)
		if bucketedSeqLen > realPastSeqLen {
			tensorInputs = padDecoderKVInputs(tensorInputs, realPastSeqLen, bucketedSeqLen)
		}
	}

	// Run decoder
	outputs, err := decoderSession.Run(tensorInputs)
	if err != nil {
		return nil, fmt.Errorf("running decoder: %w", err)
	}

	if len(outputs) == 0 {
		return nil, fmt.Errorf("no decoder output")
	}

	// Trim padded positions from present.*.decoder.* outputs so the
	// stored KV cache contains only real token data. Without this the
	// padding would compound across steps.
	if m.kvBucketStrategy != nil && !isFirstStep && realPastSeqLen > 0 {
		bucketedSeqLen := m.kvBucketStrategy.Bucket(realPastSeqLen)
		if bucketedSeqLen > realPastSeqLen {
			outputs = trimPresentDecoderKV(outputs, realPastSeqLen, bucketedSeqLen)
		}
	}

	// Extract logits (first output)
	logitsOutput := outputs[0]
	logitsData, ok := logitsOutput.Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("logits tensor is not float32")
	}

	logitsShape := logitsOutput.Shape

	// Reshape logits to [batch, vocab_size]
	vocabSize := int(logitsShape[len(logitsShape)-1])
	logits := make([][]float32, batchSize)
	for i := range batchSize {
		logits[i] = make([]float32, vocabSize)
		startIdx := i*seqLen*vocabSize + (seqLen-1)*vocabSize
		copy(logits[i], logitsData[startIdx:startIdx+vocabSize])
	}

	// Extract KV cache from decoder outputs.
	// Only extract when using split decoders (GoMLX backends). For ONNX Runtime
	// the merged decoder handles KV caching internally via the If node, and
	// returning cached values here would cause shape mismatches on step 2.
	var newKVCache *backends.KVCache
	if m.useSplitDecoders {
		newKVCache = m.extractKVCache(outputs, batchSize, pastKeyValues)
	}

	return &backends.ModelOutput{
		Logits:        logits,
		PastKeyValues: newKVCache,
	}, nil
}

// buildDecoderInputsForSession creates the input tensors for the specified decoder session.
// This allows using different sessions for the first step (no KV cache) and subsequent steps.
// Florence-2 decoder expects inputs_embeds instead of input_ids.
func (m *encoderDecoderVLMModel) buildDecoderInputsForSession(session backends.Session, inputIDs []int64, batchSize, seqLen int, encoderOutput *backends.EncoderOutput, pastKV *backends.KVCache) ([]backends.NamedTensor, error) {
	var inputs []backends.NamedTensor

	// Get decoder input names from the specified session
	inputInfo := session.InputInfo()
	inputNames := make(map[string]bool)
	for _, info := range inputInfo {
		inputNames[info.Name] = true
	}

	// Florence-2 decoder expects inputs_embeds, not input_ids
	// We need to run embed_tokens on the decoder input IDs first
	if inputNames["inputs_embeds"] {
		// Run embed_tokens on the decoder input IDs
		inputIdsTensor := backends.NamedTensor{
			Name:  "input_ids",
			Shape: []int64{int64(batchSize), int64(seqLen)},
			Data:  inputIDs,
		}

		embedOutputs, err := m.embedTokensSession.Run([]backends.NamedTensor{inputIdsTensor})
		if err != nil {
			return nil, fmt.Errorf("running embed_tokens for decoder: %w", err)
		}

		if len(embedOutputs) == 0 {
			return nil, fmt.Errorf("no output from embed_tokens for decoder")
		}

		embedsData, ok := embedOutputs[0].Data.([]float32)
		if !ok {
			return nil, fmt.Errorf("embed_tokens output is not float32")
		}

		// embed_tokens output shape is [batch, seq_len, hidden_size]
		hiddenSize := int(embedOutputs[0].Shape[2])

		inputs = append(inputs, backends.NamedTensor{
			Name:  "inputs_embeds",
			Shape: []int64{int64(batchSize), int64(seqLen), int64(hiddenSize)},
			Data:  embedsData,
		})
	} else {
		// Standard decoder uses input_ids
		inputs = append(inputs, backends.NamedTensor{
			Name:  GetDecoderInputIDsName(inputNames),
			Shape: []int64{int64(batchSize), int64(seqLen)},
			Data:  inputIDs,
		})
	}

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

	// Add past_key_values inputs if needed
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
// Maps input names like "past_key_values.0.decoder.key" to stored output names
// like "present.0.decoder.key" to retrieve cached values from previous steps.
func (m *encoderDecoderVLMModel) createPastKVTensor(name string, pastKV *backends.KVCache, batchSize int, encoderSeqLen int) backends.NamedTensor {
	// Check if we have cached tensor data from a previous step
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

	// No cached data -- create appropriately-shaped zero tensors.
	// Encoder KV (cross-attention) uses the full encoder sequence length
	// because the decoder always needs cross-attention over encoder outputs.
	// Decoder KV (self-attention) uses 0-length for the first step.
	numHeads := m.config.NumHeads
	headDim := m.config.HeadDim

	if numHeads == 0 {
		numHeads = 8
	}
	if headDim == 0 {
		headDim = 64
	}

	if isEncoderKVTensor(name) {
		tensorSize := batchSize * numHeads * encoderSeqLen * headDim
		return backends.NamedTensor{
			Name:  name,
			Shape: []int64{int64(batchSize), int64(numHeads), int64(encoderSeqLen), int64(headDim)},
			Data:  make([]float32, tensorSize),
		}
	}

	return backends.NamedTensor{
		Name:  name,
		Shape: []int64{int64(batchSize), int64(numHeads), 0, int64(headDim)},
		Data:  []float32{},
	}
}

// extractKVCache extracts the KV cache from decoder outputs.
// Collects all present.* output tensors and stores them for the next step.
// Encoder cross-attention KV tensors (present.N.encoder.*) are carried
// forward from the previous step when the current decoder doesn't re-emit
// them (the with-past decoder only outputs decoder self-attention KV).
func (m *encoderDecoderVLMModel) extractKVCache(outputs []backends.NamedTensor, batchSize int, pastKV *backends.KVCache) *backends.KVCache {
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
	// step. The with-past decoder only outputs present.N.decoder.* tensors;
	// without this the encoder KV computed on the first step would be lost.
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

	return nil
}

// padDecoderKVInputs pads past_key_values.*.decoder.* tensors from
// realSeqLen to bucketedSeqLen on axis 2 (the sequence dimension).
// Encoder KV tensors are left unchanged since their shape is constant.
//
// Input tensor layout: [batch, heads, seqLen, headDim] stored as flat float32.
// Padding inserts zeros after the real data on the sequence axis.
//
// Caveat: zero-padded KV positions produce attention score 0, receiving
// softmax weight 1/Z. Since padded V=0, their output contribution is
// zero, but they dilute real attention weights by the factor
// sum(exp(real_scores)) / (sum(exp(real_scores)) + num_padded).
// LayerNorm and residual connections partially compensate. The padding
// is trimmed from present outputs after each step (trimPresentDecoderKV)
// so the dilution does not compound across steps.
func padDecoderKVInputs(inputs []backends.NamedTensor, realSeqLen, bucketedSeqLen int) []backends.NamedTensor {
	result := make([]backends.NamedTensor, len(inputs))
	for i, t := range inputs {
		if !isDecoderKVTensor(t.Name) {
			result[i] = t
			continue
		}

		data, ok := t.Data.([]float32)
		if !ok || len(t.Shape) != 4 {
			result[i] = t
			continue
		}

		batch := int(t.Shape[0])
		heads := int(t.Shape[1])
		// t.Shape[2] == realSeqLen
		headDim := int(t.Shape[3])

		paddedSize := batch * heads * bucketedSeqLen * headDim
		padded := make([]float32, paddedSize)

		// Copy each [batch, head] slice, leaving zero gaps for padding.
		for b := range batch {
			for h := range heads {
				srcOff := (b*heads + h) * realSeqLen * headDim
				dstOff := (b*heads + h) * bucketedSeqLen * headDim
				copy(padded[dstOff:dstOff+realSeqLen*headDim], data[srcOff:srcOff+realSeqLen*headDim])
			}
		}

		result[i] = backends.NamedTensor{
			Name:  t.Name,
			Shape: []int64{t.Shape[0], t.Shape[1], int64(bucketedSeqLen), t.Shape[3]},
			Data:  padded,
		}
	}
	return result
}

// trimPresentDecoderKV removes zero-padding from present.*.decoder.*
// output tensors so the stored KV cache contains only real token data.
//
// After a padded forward pass the present tensor has shape
// [batch, heads, bucketedSeqLen+1, headDim] laid out as:
//
//	[real_0 … real_N | pad_0 … pad_M | new_token]
//
// We keep positions [0:realSeqLen] and [bucketedSeqLen:bucketedSeqLen+1],
// producing shape [batch, heads, realSeqLen+1, headDim].
func trimPresentDecoderKV(outputs []backends.NamedTensor, realSeqLen, bucketedSeqLen int) []backends.NamedTensor {
	result := make([]backends.NamedTensor, len(outputs))
	for i, t := range outputs {
		if !IsPresentKeyValueOutput(t.Name) || !isDecoderKVPresent(t.Name) {
			result[i] = t
			continue
		}

		data, ok := t.Data.([]float32)
		if !ok || len(t.Shape) != 4 {
			result[i] = t
			continue
		}

		batch := int(t.Shape[0])
		heads := int(t.Shape[1])
		srcSeqLen := int(t.Shape[2]) // bucketedSeqLen + 1
		headDim := int(t.Shape[3])
		trimmedSeqLen := realSeqLen + 1

		trimmedSize := batch * heads * trimmedSeqLen * headDim
		trimmed := make([]float32, trimmedSize)

		for b := range batch {
			for h := range heads {
				srcBase := (b*heads + h) * srcSeqLen * headDim
				dstBase := (b*heads + h) * trimmedSeqLen * headDim

				// Copy the real past positions [0:realSeqLen].
				copy(trimmed[dstBase:dstBase+realSeqLen*headDim],
					data[srcBase:srcBase+realSeqLen*headDim])

				// Copy the new token position [bucketedSeqLen].
				newTokSrc := srcBase + bucketedSeqLen*headDim
				newTokDst := dstBase + realSeqLen*headDim
				copy(trimmed[newTokDst:newTokDst+headDim],
					data[newTokSrc:newTokSrc+headDim])
			}
		}

		result[i] = backends.NamedTensor{
			Name:  t.Name,
			Shape: []int64{t.Shape[0], t.Shape[1], int64(trimmedSeqLen), t.Shape[3]},
			Data:  trimmed,
		}
	}
	return result
}

// isDecoderKVTensor returns true for past_key_values.*.decoder.* tensors.
func isDecoderKVTensor(name string) bool {
	return IsPastKeyValueInput(name) && !isEncoderKVTensor(name)
}

// isDecoderKVPresent returns true for present.*.decoder.* tensors.
func isDecoderKVPresent(name string) bool {
	return strings.Contains(name, ".decoder.")
}

// DecoderConfig returns configuration needed for generation.
func (m *encoderDecoderVLMModel) DecoderConfig() *backends.DecoderConfig {
	return m.config.DecoderConfig
}

// ImageConfig returns configuration for image preprocessing.
func (m *encoderDecoderVLMModel) ImageConfig() *backends.ImageConfig {
	return m.config.ImageConfig
}

// Close releases resources associated with the model.
func (m *encoderDecoderVLMModel) Close() error {
	var errs []error

	if m.visionEncoderSession != nil {
		if err := m.visionEncoderSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing vision encoder: %w", err))
		}
		m.visionEncoderSession = nil
	}

	if m.embedTokensSession != nil {
		if err := m.embedTokensSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing embed_tokens: %w", err))
		}
		m.embedTokensSession = nil
	}

	if m.encoderModelSession != nil {
		if err := m.encoderModelSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing encoder_model: %w", err))
		}
		m.encoderModelSession = nil
	}

	if m.decoderSession != nil {
		if err := m.decoderSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing decoder: %w", err))
		}
		m.decoderSession = nil
	}

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
func (m *encoderDecoderVLMModel) Name() string {
	return m.config.ModelPath
}

// Backend returns the backend type this model uses.
func (m *encoderDecoderVLMModel) Backend() backends.BackendType {
	return m.backendType
}

// =============================================================================
// Decoder-Only VLM Model
// =============================================================================

// decoderOnlyVLMModel implements backends.Model for decoder-only VLM
// architectures (e.g., Moondream2). Uses a vision encoder to extract image
// features, embed_tokens to embed text, then concatenates them as inputs_embeds
// for a decoder-only transformer (no cross-attention):
//   - vision_encoder: pixel_values → image_features
//   - embed_tokens: input_ids → text_embeddings
//   - decoder: inputs_embeds (concat of [image_features | text_embeddings]) + position_ids → logits (first step)
//   - decoder: inputs_embeds (single token via embed_tokens) + past_key_values + position_ids → logits (subsequent steps)
//
// Unlike encoder-decoder VLMs (Florence-2), there is no separate encoder_model.
// Image features are injected as prefix tokens through concatenation with text
// embeddings in inputs_embeds. The decoder always takes inputs_embeds (not
// input_ids) and always outputs KV cache tensors.
type decoderOnlyVLMModel struct {
	config *Vision2SeqModelConfig

	// Model sessions
	visionEncoderSession backends.Session // vision_encoder.onnx
	embedTokensSession   backends.Session // embed_tokens.onnx
	decoderSession       backends.Session // decoder_model_merged.onnx (ONNX Runtime fallback)

	// Split decoder sessions for GoMLX backends (XLA, Go, CoreML).
	// The merged decoder's ONNX If node cannot be evaluated at runtime by these
	// backends. Instead we use separate ONNX files (decoder_model.onnx and
	// decoder_with_past_model.onnx) that are purpose-built for each phase.
	decoderFirstStepSession backends.Session // decoder_model.onnx (first step, no KV cache)
	decoderWithPastSession  backends.Session // decoder_with_past_model.onnx (subsequent steps, with KV cache)
	useSplitDecoders        bool

	// kvBucketStrategy buckets past_key_values sequence lengths to reduce
	// the number of unique shapes seen by JIT backends (XLA, CoreML).
	kvBucketStrategy bucketing.Strategy

	backendType backends.BackendType
}

// LoadDecoderOnlyVLMModel loads a decoder-only VLM model using the given session factory.
func LoadDecoderOnlyVLMModel(modelPath string, factory backends.SessionFactory, opts ...backends.SessionOption) (backends.Model, error) {
	// Load configuration
	config, err := LoadVision2SeqModelConfig(modelPath)
	if err != nil {
		return nil, fmt.Errorf("loading model config: %w", err)
	}

	// Find required ONNX files
	visionEncoderPath := FindONNXFile(modelPath, []string{"vision_encoder.onnx"})
	embedTokensPath := FindONNXFile(modelPath, []string{"embed_tokens.onnx"})
	decoderPath := FindONNXFile(modelPath, []string{
		"decoder_model_merged.onnx",
		"decoder_with_past.onnx",
		"decoder.onnx",
		"decoder_model.onnx",
	})

	if visionEncoderPath == "" {
		return nil, fmt.Errorf("vision_encoder.onnx not found in %s", modelPath)
	}
	if embedTokensPath == "" {
		return nil, fmt.Errorf("embed_tokens.onnx not found in %s", modelPath)
	}
	if decoderPath == "" {
		return nil, fmt.Errorf("decoder ONNX file not found in %s", modelPath)
	}

	config.DecoderPath = decoderPath

	// Create sessions with cascading cleanup on error
	visionEncoderSession, err := factory.CreateSession(visionEncoderPath, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating vision encoder session: %w", err)
	}

	embedTokensSession, err := factory.CreateSession(embedTokensPath, opts...)
	if err != nil {
		_ = visionEncoderSession.Close()
		return nil, fmt.Errorf("creating embed_tokens session: %w", err)
	}

	model := &decoderOnlyVLMModel{
		config:               config,
		visionEncoderSession: visionEncoderSession,
		embedTokensSession:   embedTokensSession,
		backendType:          factory.Backend(),
	}

	closeOnError := func() {
		_ = visionEncoderSession.Close()
		_ = embedTokensSession.Close()
	}

	// Create the main decoder session
	decoderSession, err := factory.CreateSession(decoderPath, opts...)
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("creating decoder session: %w", err)
	}
	model.decoderSession = decoderSession

	// Try to load split decoders for GoMLX backends (XLA, Go, CoreML).
	// These backends cannot evaluate ONNX If nodes at runtime, so the merged
	// decoder disables KV caching. The separate decoder_model.onnx and
	// decoder_with_past_model.onnx files are purpose-built for each phase.
	isGoMLXBackend := false
	switch factory.Backend() {
	case backends.BackendGo, backends.BackendXLA, backends.BackendCoreML:
		isGoMLXBackend = true
	}
	if isGoMLXBackend && config.DecoderFirstStepPath != "" && config.DecoderWithPastPath != "" {
		firstStepSession, err := factory.CreateSession(config.DecoderFirstStepPath, opts...)
		if err == nil {
			withPastOpts := append(opts, backends.WithDynamicAxes([]backends.DynamicAxisOverride{
				{InputName: "inputs_embeds", Axis: 1, ParamName: "decoder_sequence_length"},
				{InputName: "input_ids", Axis: 1, ParamName: "decoder_sequence_length"},
			}))
			withPastSession, err := factory.CreateSession(config.DecoderWithPastPath, withPastOpts...)
			if err == nil {
				model.decoderFirstStepSession = firstStepSession
				model.decoderWithPastSession = withPastSession
				model.useSplitDecoders = true

				switch factory.Backend() {
				case backends.BackendXLA, backends.BackendCoreML:
					model.kvBucketStrategy = bucketing.Pow2()
				}
			} else {
				_ = firstStepSession.Close()
			}
		}
	}

	return model, nil
}

// Forward runs the decoder-only VLM model.
// - If ImagePixels is set (and EncoderOutput is nil): runs vision encoder
// - If EncoderOutput is set: runs decoder step
func (m *decoderOnlyVLMModel) Forward(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	if inputs == nil {
		return nil, fmt.Errorf("nil inputs")
	}

	if inputs.EncoderOutput != nil {
		return m.runDecoder(ctx, inputs)
	}

	if len(inputs.ImagePixels) == 0 {
		return nil, fmt.Errorf("no image pixels or encoder output provided")
	}

	return m.runEncoder(ctx, inputs)
}

// runEncoder runs the vision encoder on pixel values.
// Unlike encoder-decoder VLMs, there is no separate encoder_model stage —
// the vision encoder output is concatenated with text embeddings in the
// decoder's inputs_embeds.
func (m *decoderOnlyVLMModel) runEncoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	batchSize := inputs.ImageBatch

	pixelValues := backends.NamedTensor{
		Name:  "pixel_values",
		Shape: []int64{int64(batchSize), int64(inputs.ImageChannels), int64(inputs.ImageHeight), int64(inputs.ImageWidth)},
		Data:  inputs.ImagePixels,
	}

	visionOutputs, err := m.visionEncoderSession.Run([]backends.NamedTensor{pixelValues})
	if err != nil {
		return nil, fmt.Errorf("running vision encoder: %w", err)
	}

	if len(visionOutputs) == 0 {
		return nil, fmt.Errorf("no output from vision encoder")
	}

	imageFeatures := visionOutputs[0]
	imageFeaturesData, ok := imageFeatures.Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("vision encoder output is not float32")
	}

	if len(imageFeatures.Shape) != 3 {
		return nil, fmt.Errorf("unexpected image features shape: %v (expected 3D)", imageFeatures.Shape)
	}

	encoderOutput := &backends.EncoderOutput{
		HiddenStates: imageFeaturesData,
		Shape:        [3]int{int(imageFeatures.Shape[0]), int(imageFeatures.Shape[1]), int(imageFeatures.Shape[2])},
	}

	return &backends.ModelOutput{
		EncoderOutput: encoderOutput,
	}, nil
}

// runDecoder performs one step of autoregressive decoding for a decoder-only VLM.
//
// First step (no KV cache): embed text tokens via embed_tokens, concatenate
// [image_features | text_embeds] into inputs_embeds, and run the decoder.
//
// Subsequent steps (with KV cache): embed the new token via embed_tokens,
// pass its embedding as inputs_embeds along with past_key_values and position_ids.
//
// The decoder always takes inputs_embeds (not input_ids) and always outputs
// present.* KV cache tensors. Unlike some merged decoders, there is no
// use_cache_branch input — the model switches behavior based on whether
// past_key_values has sequence length 0 or not.
func (m *decoderOnlyVLMModel) runDecoder(ctx context.Context, inputs *backends.ModelInputs) (*backends.ModelOutput, error) {
	inputIDs := inputs.InputIDs
	encoderOutput := inputs.EncoderOutput
	pastKeyValues := inputs.PastKeyValues

	batchSize := len(inputIDs)
	if batchSize == 0 {
		return nil, fmt.Errorf("empty input")
	}

	seqLen := len(inputIDs[0])
	isFirstStep := pastKeyValues == nil || pastKeyValues.SeqLen == 0

	// Choose decoder session
	var decoderSession backends.Session
	if m.useSplitDecoders {
		if isFirstStep {
			decoderSession = m.decoderFirstStepSession
		} else {
			decoderSession = m.decoderWithPastSession
		}
	} else {
		decoderSession = m.decoderSession
	}

	var tensorInputs []backends.NamedTensor

	if isFirstStep {
		// First step: embed text and concatenate with image features
		embeds, err := m.buildFirstStepInputs(decoderSession, inputIDs, batchSize, seqLen, encoderOutput)
		if err != nil {
			return nil, err
		}
		tensorInputs = embeds
	} else {
		// Subsequent steps: embed new token, pass with KV cache
		embeds, err := m.buildSubsequentStepInputs(decoderSession, inputIDs, batchSize, seqLen, pastKeyValues)
		if err != nil {
			return nil, err
		}
		tensorInputs = embeds
	}

	// Pad KV cache tensors for bucketing
	var realPastSeqLen int
	if m.kvBucketStrategy != nil && !isFirstStep {
		realPastSeqLen = kvCacheSeqLen(pastKeyValues)
		bucketedSeqLen := m.kvBucketStrategy.Bucket(realPastSeqLen)
		if bucketedSeqLen > realPastSeqLen {
			tensorInputs = padDecoderKVInputs(tensorInputs, realPastSeqLen, bucketedSeqLen)
		}
	}

	// Run decoder
	outputs, err := decoderSession.Run(tensorInputs)
	if err != nil {
		return nil, fmt.Errorf("running decoder: %w", err)
	}

	if len(outputs) == 0 {
		return nil, fmt.Errorf("no decoder output")
	}

	// Trim padded positions from present outputs
	if m.kvBucketStrategy != nil && !isFirstStep && realPastSeqLen > 0 {
		bucketedSeqLen := m.kvBucketStrategy.Bucket(realPastSeqLen)
		if bucketedSeqLen > realPastSeqLen {
			outputs = trimPresentKV(outputs, realPastSeqLen, bucketedSeqLen)
		}
	}

	// Extract logits (first output)
	logitsOutput := outputs[0]
	logitsData, ok := logitsOutput.Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("logits tensor is not float32")
	}

	logitsShape := logitsOutput.Shape

	// Reshape logits to [batch, vocab_size] (taking last position)
	outputSeqLen := int(logitsShape[1])
	vocabSize := int(logitsShape[len(logitsShape)-1])
	logits := make([][]float32, batchSize)
	for i := range batchSize {
		logits[i] = make([]float32, vocabSize)
		startIdx := i*outputSeqLen*vocabSize + (outputSeqLen-1)*vocabSize
		copy(logits[i], logitsData[startIdx:startIdx+vocabSize])
	}

	// Always extract KV cache from decoder outputs. The merged decoder outputs
	// present.* tensors on every step. Returning them triggers the generation
	// loop to use the KV cache path (trimming InputIDs to just the last token).
	newKVCache := m.extractKVCache(outputs, batchSize, pastKeyValues)

	return &backends.ModelOutput{
		Logits:        logits,
		PastKeyValues: newKVCache,
	}, nil
}

// buildFirstStepInputs creates decoder inputs for the first step.
// Embeds text tokens via embed_tokens, then concatenates [image_features | text_embeds]
// into inputs_embeds for the decoder.
func (m *decoderOnlyVLMModel) buildFirstStepInputs(
	session backends.Session,
	inputIDs [][]int32,
	batchSize, seqLen int,
	encoderOutput *backends.EncoderOutput,
) ([]backends.NamedTensor, error) {
	// Flatten input IDs for embed_tokens
	flatInputIDs := make([]int64, batchSize*seqLen)
	for i := range batchSize {
		for j := range seqLen {
			flatInputIDs[i*seqLen+j] = int64(inputIDs[i][j])
		}
	}

	// Run embed_tokens on text input_ids
	embedInput := backends.NamedTensor{
		Name:  "input_ids",
		Shape: []int64{int64(batchSize), int64(seqLen)},
		Data:  flatInputIDs,
	}

	embedOutputs, err := m.embedTokensSession.Run([]backends.NamedTensor{embedInput})
	if err != nil {
		return nil, fmt.Errorf("running embed_tokens: %w", err)
	}
	if len(embedOutputs) == 0 {
		return nil, fmt.Errorf("no output from embed_tokens")
	}

	textEmbedsData, ok := embedOutputs[0].Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("embed_tokens output is not float32")
	}

	hiddenSize := int(embedOutputs[0].Shape[2])
	imageSeqLen := encoderOutput.Shape[1]

	// Concatenate [image_features | text_embeds] → inputs_embeds
	totalSeqLen := imageSeqLen + seqLen
	inputsEmbeds := make([]float32, batchSize*totalSeqLen*hiddenSize)

	for b := range batchSize {
		// Copy image features
		for s := range imageSeqLen {
			srcIdx := b*imageSeqLen*hiddenSize + s*hiddenSize
			dstIdx := b*totalSeqLen*hiddenSize + s*hiddenSize
			copy(inputsEmbeds[dstIdx:dstIdx+hiddenSize], encoderOutput.HiddenStates[srcIdx:srcIdx+hiddenSize])
		}
		// Copy text embeds
		for s := range seqLen {
			srcIdx := b*seqLen*hiddenSize + s*hiddenSize
			dstIdx := b*totalSeqLen*hiddenSize + (imageSeqLen+s)*hiddenSize
			copy(inputsEmbeds[dstIdx:dstIdx+hiddenSize], textEmbedsData[srcIdx:srcIdx+hiddenSize])
		}
	}

	var inputs []backends.NamedTensor

	// Get session input names
	inputInfo := session.InputInfo()
	inputNames := make(map[string]bool)
	for _, info := range inputInfo {
		inputNames[info.Name] = true
	}

	// Add inputs_embeds
	inputs = append(inputs, backends.NamedTensor{
		Name:  "inputs_embeds",
		Shape: []int64{int64(batchSize), int64(totalSeqLen), int64(hiddenSize)},
		Data:  inputsEmbeds,
	})

	// Add attention mask if needed
	if inputNames["attention_mask"] {
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

	// Add position_ids if needed: [0, 1, 2, ..., totalSeqLen-1]
	if inputNames["position_ids"] {
		posIDs := make([]int64, batchSize*totalSeqLen)
		for b := range batchSize {
			for s := range totalSeqLen {
				posIDs[b*totalSeqLen+s] = int64(s)
			}
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "position_ids",
			Shape: []int64{int64(batchSize), int64(totalSeqLen)},
			Data:  posIDs,
		})
	}

	// Add use_cache_branch if needed (first step → false)
	if inputNames["use_cache_branch"] {
		inputs = append(inputs, createUseCacheBranchTensor(inputInfo, false))
	}

	// Add zero-initialized past_key_values (decoder-only: no encoder KV)
	for _, info := range inputInfo {
		if IsPastKeyValueInput(info.Name) {
			inputs = append(inputs, m.createZeroPastKVTensor(info.Name, batchSize))
		}
	}

	return inputs, nil
}

// buildSubsequentStepInputs creates decoder inputs for subsequent steps.
// The decoder always takes inputs_embeds (not input_ids), so we run
// embed_tokens on the new token to get its embedding, then pass it
// along with the KV cache and position_ids.
func (m *decoderOnlyVLMModel) buildSubsequentStepInputs(
	session backends.Session,
	inputIDs [][]int32,
	batchSize, seqLen int,
	pastKV *backends.KVCache,
) ([]backends.NamedTensor, error) {
	// Flatten input IDs for embed_tokens
	flatInputIDs := make([]int64, batchSize*seqLen)
	for i := range batchSize {
		for j := range seqLen {
			flatInputIDs[i*seqLen+j] = int64(inputIDs[i][j])
		}
	}

	// Run embed_tokens to convert token(s) to embeddings
	embedInput := backends.NamedTensor{
		Name:  "input_ids",
		Shape: []int64{int64(batchSize), int64(seqLen)},
		Data:  flatInputIDs,
	}
	embedOutputs, err := m.embedTokensSession.Run([]backends.NamedTensor{embedInput})
	if err != nil {
		return nil, fmt.Errorf("running embed_tokens: %w", err)
	}
	if len(embedOutputs) == 0 {
		return nil, fmt.Errorf("no output from embed_tokens")
	}

	embedsData, ok := embedOutputs[0].Data.([]float32)
	if !ok {
		return nil, fmt.Errorf("embed_tokens output is not float32")
	}
	hiddenSize := int(embedOutputs[0].Shape[2])

	var inputs []backends.NamedTensor

	inputInfo := session.InputInfo()
	inputNames := make(map[string]bool)
	for _, info := range inputInfo {
		inputNames[info.Name] = true
	}

	// Add inputs_embeds (the decoder always takes inputs_embeds, not input_ids)
	inputs = append(inputs, backends.NamedTensor{
		Name:  "inputs_embeds",
		Shape: []int64{int64(batchSize), int64(seqLen), int64(hiddenSize)},
		Data:  embedsData,
	})

	// Get actual past sequence length from KV cache tensor shapes
	pastSeqLen := kvCacheSeqLen(pastKV)

	// Add attention mask covering all past + current tokens
	if inputNames["attention_mask"] {
		totalLen := pastSeqLen + seqLen
		mask := make([]int64, batchSize*totalLen)
		for i := range mask {
			mask[i] = 1
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "attention_mask",
			Shape: []int64{int64(batchSize), int64(totalLen)},
			Data:  mask,
		})
	}

	// Add position_ids: [pastSeqLen, pastSeqLen+1, ..., pastSeqLen+seqLen-1]
	if inputNames["position_ids"] {
		posIDs := make([]int64, batchSize*seqLen)
		for b := range batchSize {
			for s := range seqLen {
				posIDs[b*seqLen+s] = int64(pastSeqLen + s)
			}
		}
		inputs = append(inputs, backends.NamedTensor{
			Name:  "position_ids",
			Shape: []int64{int64(batchSize), int64(seqLen)},
			Data:  posIDs,
		})
	}

	// Add use_cache_branch if needed (subsequent step → true)
	if inputNames["use_cache_branch"] {
		inputs = append(inputs, createUseCacheBranchTensor(inputInfo, true))
	}

	// Add past_key_values from cache
	for _, info := range inputInfo {
		if IsPastKeyValueInput(info.Name) {
			tensor := m.createPastKVTensor(info.Name, pastKV, batchSize)
			inputs = append(inputs, tensor)
		}
	}

	return inputs, nil
}

// createUseCacheBranchTensor creates the use_cache_branch tensor.
func createUseCacheBranchTensor(inputInfo []backends.TensorInfo, useCache bool) backends.NamedTensor {
	var dataType = backends.DataTypeBool
	for _, info := range inputInfo {
		if info.Name == "use_cache_branch" {
			dataType = info.DataType
			break
		}
	}

	if dataType == backends.DataTypeFloat32 {
		val := []float32{0}
		if useCache {
			val[0] = 1
		}
		return backends.NamedTensor{
			Name:  "use_cache_branch",
			Shape: []int64{1},
			Data:  val,
		}
	}
	return backends.NamedTensor{
		Name:  "use_cache_branch",
		Shape: []int64{1},
		Data:  []bool{useCache},
	}
}

// createZeroPastKVTensor creates zero-initialized past KV tensors for the first step.
// Decoder-only models have no encoder KV tensors — all are self-attention only.
func (m *decoderOnlyVLMModel) createZeroPastKVTensor(name string, batchSize int) backends.NamedTensor {
	numHeads := m.config.NumHeads
	headDim := m.config.HeadDim
	if numHeads == 0 {
		numHeads = 8
	}
	if headDim == 0 {
		headDim = 64
	}

	return backends.NamedTensor{
		Name:  name,
		Shape: []int64{int64(batchSize), int64(numHeads), 0, int64(headDim)},
		Data:  []float32{},
	}
}

// createPastKVTensor retrieves a cached KV tensor from the previous step.
func (m *decoderOnlyVLMModel) createPastKVTensor(name string, pastKV *backends.KVCache, batchSize int) backends.NamedTensor {
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

	// Fallback to zero tensor
	return m.createZeroPastKVTensor(name, batchSize)
}

// extractKVCache extracts the KV cache from decoder outputs.
// Collects all present.* output tensors and stores them for the next step.
func (m *decoderOnlyVLMModel) extractKVCache(outputs []backends.NamedTensor, batchSize int, pastKV *backends.KVCache) *backends.KVCache {
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

	return nil
}

// kvCacheSeqLen returns the actual sequence length from KV cache tensor shapes.
// This reflects the total past sequence (including image tokens from the first step)
// rather than the step counter in KVCache.SeqLen.
func kvCacheSeqLen(pastKV *backends.KVCache) int {
	if pastKV == nil || pastKV.Tensors == nil {
		return 0
	}
	for _, tensor := range pastKV.Tensors {
		if len(tensor.Shape) == 4 {
			return int(tensor.Shape[2])
		}
	}
	return 0
}

// trimPresentKV removes zero-padding from all present.* KV output tensors.
// This is the decoder-only equivalent of trimPresentDecoderKV — since decoder-only
// models have no encoder KV tensors, all present outputs are trimmed.
//
// After a padded forward pass the present tensor has shape
// [batch, heads, bucketedSeqLen+1, headDim]. We keep positions [0:realSeqLen]
// and [bucketedSeqLen:bucketedSeqLen+1], producing [batch, heads, realSeqLen+1, headDim].
func trimPresentKV(outputs []backends.NamedTensor, realSeqLen, bucketedSeqLen int) []backends.NamedTensor {
	result := make([]backends.NamedTensor, len(outputs))
	for i, t := range outputs {
		if !IsPresentKeyValueOutput(t.Name) {
			result[i] = t
			continue
		}

		data, ok := t.Data.([]float32)
		if !ok || len(t.Shape) != 4 {
			result[i] = t
			continue
		}

		batch := int(t.Shape[0])
		heads := int(t.Shape[1])
		srcSeqLen := int(t.Shape[2]) // bucketedSeqLen + 1
		headDim := int(t.Shape[3])
		trimmedSeqLen := realSeqLen + 1

		trimmedSize := batch * heads * trimmedSeqLen * headDim
		trimmed := make([]float32, trimmedSize)

		for b := range batch {
			for h := range heads {
				srcBase := (b*heads + h) * srcSeqLen * headDim
				dstBase := (b*heads + h) * trimmedSeqLen * headDim

				// Copy the real past positions [0:realSeqLen].
				copy(trimmed[dstBase:dstBase+realSeqLen*headDim],
					data[srcBase:srcBase+realSeqLen*headDim])

				// Copy the new token position [bucketedSeqLen].
				newTokSrc := srcBase + bucketedSeqLen*headDim
				newTokDst := dstBase + realSeqLen*headDim
				copy(trimmed[newTokDst:newTokDst+headDim],
					data[newTokSrc:newTokSrc+headDim])
			}
		}

		result[i] = backends.NamedTensor{
			Name:  t.Name,
			Shape: []int64{t.Shape[0], t.Shape[1], int64(trimmedSeqLen), t.Shape[3]},
			Data:  trimmed,
		}
	}
	return result
}

// DecoderConfig returns configuration needed for generation.
func (m *decoderOnlyVLMModel) DecoderConfig() *backends.DecoderConfig {
	return m.config.DecoderConfig
}

// ImageConfig returns configuration for image preprocessing.
func (m *decoderOnlyVLMModel) ImageConfig() *backends.ImageConfig {
	return m.config.ImageConfig
}

// Close releases resources associated with the model.
func (m *decoderOnlyVLMModel) Close() error {
	var errs []error

	if m.visionEncoderSession != nil {
		if err := m.visionEncoderSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing vision encoder: %w", err))
		}
		m.visionEncoderSession = nil
	}

	if m.embedTokensSession != nil {
		if err := m.embedTokensSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing embed_tokens: %w", err))
		}
		m.embedTokensSession = nil
	}

	if m.decoderSession != nil {
		if err := m.decoderSession.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing decoder: %w", err))
		}
		m.decoderSession = nil
	}

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
func (m *decoderOnlyVLMModel) Name() string {
	return m.config.ModelPath
}

// Backend returns the backend type this model uses.
func (m *decoderOnlyVLMModel) Backend() backends.BackendType {
	return m.backendType
}

// =============================================================================
// Florence-2 Pipeline
// =============================================================================

// Florence2Pipeline extends Vision2SeqPipeline for Florence-2 specific handling.
// The key difference is that prompts are embedded alongside images in the encoder,
// not passed to the decoder.
type Florence2Pipeline struct {
	*EncoderDecoderPipeline

	// ImageProcessor handles image preprocessing.
	ImageProcessor *ImageProcessor

	// model provides access to the underlying encoder-decoder VLM model
	model *encoderDecoderVLMModel
}

// NewFlorence2Pipeline creates a new Florence-2 pipeline.
func NewFlorence2Pipeline(
	model backends.Model,
	tokenizer tokenizers.Tokenizer,
	config *Vision2SeqConfig,
) *Florence2Pipeline {
	if config == nil {
		config = &Vision2SeqConfig{}
	}

	// Resolve image config
	imageConfig := ResolveImageConfig(model, config.ImageConfig)

	// Create base encoder-decoder pipeline
	base := NewEncoderDecoderPipeline(model, tokenizer, config.GenerationConfig)

	// Get the encoderDecoderVLMModel if available
	f2m, _ := model.(*encoderDecoderVLMModel)

	return &Florence2Pipeline{
		EncoderDecoderPipeline: base,
		ImageProcessor:         NewImageProcessor(imageConfig),
		model:                  f2m,
	}
}

// RunWithPrompt processes an image with a text prompt.
// For Florence-2, the prompt is embedded alongside the image in the encoder.
func (p *Florence2Pipeline) RunWithPrompt(ctx context.Context, img any, prompt string) (*Vision2SeqResult, error) {
	// Preprocess image
	var pixels []float32
	var err error

	switch v := img.(type) {
	case []byte:
		pixels, err = p.ImageProcessor.ProcessBytes(v)
	default:
		return nil, fmt.Errorf("unsupported image type, use image.Image or []byte")
	}

	if err != nil {
		return nil, fmt.Errorf("preprocessing image: %w", err)
	}

	// Tokenize prompt
	var promptTokenIDs [][]int32
	if prompt != "" {
		tokens := p.Tokenizer.Encode(prompt)
		promptTokenIDs = [][]int32{IntToInt32(tokens)}
	}

	cfg := p.ImageProcessor.Config
	batchSize := 1

	// Encode image with prompt tokens
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

	// Get start tokens for decoder (just the decoder start token, not the prompt)
	startTokens := []int32{p.DecoderConfig.DecoderStartTokenID}

	// Generate using shared base
	return p.GenerateFromEncoderOutput(ctx, encodeOutput.EncoderOutput, startTokens)
}

// =============================================================================
// Florence-2 Loader
// =============================================================================

// LoadFlorence2Pipeline loads a complete Florence-2 pipeline from a model directory.
func LoadFlorence2Pipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...Vision2SeqPipelineOption,
) (*Florence2Pipeline, backends.BackendType, error) {
	// Get session factory from manager
	factory, backendType, err := sessionManager.GetSessionFactoryForModel(modelBackends)
	if err != nil {
		return nil, "", fmt.Errorf("getting session factory: %w", err)
	}

	// Load the tokenizer (needed for the pipeline)
	tokenizer, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading tokenizer: %w", err)
	}

	// Load the encoder-decoder VLM model
	model, err := LoadEncoderDecoderVLMModel(modelPath, factory)
	if err != nil {
		return nil, "", fmt.Errorf("loading encoder-decoder VLM model: %w", err)
	}

	// Apply options
	config := &Vision2SeqConfig{}
	for _, opt := range opts {
		opt(config)
	}

	// Create the pipeline
	pipeline := NewFlorence2Pipeline(model, tokenizer, config)

	return pipeline, backendType, nil
}

// =============================================================================
// Helper: Parse Florence-2 Output
// =============================================================================

// FlorenceParseOCR cleans Florence-2 OCR output.
// Florence-2 outputs are relatively clean but may have trailing artifacts.
func FlorenceParseOCR(text string) string {
	// Remove common artifacts
	text = strings.TrimSpace(text)

	// Remove trailing </s> if present
	text = strings.TrimSuffix(text, "</s>")
	text = strings.TrimSpace(text)

	return text
}

// GetFlorence2PromptForTask returns the natural language prompt for a Florence-2 task.
// Florence-2 uses natural language prompts like "What is the text in the image?"
// instead of task tokens like "<OCR>".
func GetFlorence2PromptForTask(task string) string {
	prompts := map[string]string{
		"<OCR>":                   "What is the text in the image?",
		"<OCR_WITH_REGION>":       "What is the text in the image, with regions?",
		"<CAPTION>":               "What does the image describe?",
		"<DETAILED_CAPTION>":      "Describe in detail what is shown in the image.",
		"<MORE_DETAILED_CAPTION>": "Describe with a paragraph what is shown in the image.",
		"<OD>":                    "Locate the objects with category name in the image.",
		"<DENSE_REGION_CAPTION>":  "Locate the objects in the image, with their descriptions.",
		"<REGION_PROPOSAL>":       "Locate the region proposals in the image.",
	}

	if prompt, ok := prompts[task]; ok {
		return prompt
	}
	return task // Return as-is if not a known task token
}
