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
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// KV-cache input/output detection helpers

// IsPastKeyValueInput checks if the input name is a past key/value tensor.
// Common patterns: past_key_values.0.key, past_key_values.0.value, etc.
func IsPastKeyValueInput(name string) bool {
	return len(name) > 15 && name[:15] == "past_key_values"
}

// IsPresentKeyValueOutput checks if the output name is a present key/value tensor.
// Common patterns: present.0.key, present.0.value, present_key_values, etc.
func IsPresentKeyValueOutput(name string) bool {
	return len(name) >= 7 && name[:7] == "present"
}

// Config resolution helpers

// ResolveDecoderConfig resolves decoder configuration from a model.
// If the model implements DecoderConfigProvider, it returns the model's config.
// Otherwise, it returns a default configuration.
func ResolveDecoderConfig(model backends.Model) *backends.DecoderConfig {
	if provider, ok := model.(backends.DecoderConfigProvider); ok {
		if config := provider.DecoderConfig(); config != nil {
			return config
		}
	}
	return DefaultDecoderConfig()
}

// ResolveGenerationConfig resolves generation configuration.
// If config is nil, it returns the default configuration.
func ResolveGenerationConfig(config *backends.GenerationConfig) *backends.GenerationConfig {
	if config == nil {
		return backends.DefaultGenerationConfig()
	}
	return config
}

// ResolveImageConfig resolves image configuration from a model.
// If the model implements ImageConfigProvider, it returns the model's config.
// Otherwise, it returns the provided config or a default configuration.
func ResolveImageConfig(model backends.Model, config *backends.ImageConfig) *backends.ImageConfig {
	if config != nil {
		return config
	}
	if provider, ok := model.(backends.ImageConfigProvider); ok {
		if modelConfig := provider.ImageConfig(); modelConfig != nil {
			return modelConfig
		}
	}
	return backends.DefaultImageConfig()
}

// DefaultDecoderConfig returns sensible default decoder configuration.
// This is used when the model doesn't provide its own configuration.
func DefaultDecoderConfig() *backends.DecoderConfig {
	return &backends.DecoderConfig{
		VocabSize:           50000,
		MaxLength:           512,
		EOSTokenID:          2,
		BOSTokenID:          0,
		PadTokenID:          1,
		DecoderStartTokenID: 2,
		NumLayers:           6,
		NumHeads:            8,
		HeadDim:             64,
	}
}

// DefaultDecoderOnlyConfig returns default config for decoder-only models (GPT-style).
func DefaultDecoderOnlyConfig() *backends.DecoderConfig {
	return &backends.DecoderConfig{
		VocabSize:  32000,
		MaxLength:  512,
		EOSTokenID: 2,
		BOSTokenID: 1,
		PadTokenID: 0,
		NumLayers:  6,
		NumHeads:   8,
		HeadDim:    64,
	}
}

// Encoder/decoder input name helpers

// EncoderONNXCandidates returns common ONNX file names for encoders.
func EncoderONNXCandidates() []string {
	return []string{
		"encoder.onnx",
		"encoder_model.onnx",
		"vision_encoder.onnx",
	}
}

// DecoderONNXCandidates returns common ONNX file names for decoders.
func DecoderONNXCandidates() []string {
	return []string{
		"decoder_model_merged.onnx", // Preferred: merged decoder with KV-cache
		"decoder_with_past.onnx",
		"decoder_with_past_model.onnx",
		"decoder.onnx",
		"decoder_model.onnx",
	}
}

// GetDecoderInputIDsName returns the name for decoder input IDs based on available inputs.
func GetDecoderInputIDsName(inputNames map[string]bool) string {
	if inputNames["decoder_input_ids"] {
		return "decoder_input_ids"
	}
	return "input_ids"
}

// GetEncoderOutputName returns the name for encoder output based on available inputs.
func GetEncoderOutputName(inputNames map[string]bool) string {
	if inputNames["encoder_outputs"] {
		return "encoder_outputs"
	}
	return "encoder_hidden_states"
}
