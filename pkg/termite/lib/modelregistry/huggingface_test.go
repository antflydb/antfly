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
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestOnnxVariantSuffix(t *testing.T) {
	tests := []struct {
		filename string
		want     string
	}{
		// Base files (no variant)
		{"decoder_model.onnx", ""},
		{"encoder_model.onnx", ""},
		{"vision_encoder.onnx", ""},
		{"embed_tokens.onnx", ""},
		{"model.onnx", ""},

		// Known variant suffixes
		{"decoder_model_fp16.onnx", "_fp16"},
		{"decoder_model_int8.onnx", "_int8"},
		{"decoder_model_uint8.onnx", "_uint8"},
		{"decoder_model_bnb4.onnx", "_bnb4"},
		{"decoder_model_quantized.onnx", "_quantized"},
		{"decoder_model_q4.onnx", "_q4"},
		{"decoder_model_q4f16.onnx", "_q4f16"},

		// External data files
		{"encoder_model.onnx_data", ""},
		{"encoder_model_fp16.onnx_data", "_fp16"},
		{"encoder_model.onnx.data", ""},
		{"encoder_model_int8.onnx.data", "_int8"},

		// Non-ONNX files
		{"tokenizer.json", ""},
		{"config.json", ""},
		{"merges.txt", ""},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got := onnxVariantSuffix(tt.filename)
			if got != tt.want {
				t.Errorf("onnxVariantSuffix(%q) = %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}

func TestMatchesVariantSuffix(t *testing.T) {
	tests := []struct {
		filename string
		variant  string
		want     bool
	}{
		// Default variant: only base files match
		{"decoder_model.onnx", "", true},
		{"decoder_model_fp16.onnx", "", false},
		{"decoder_model_int8.onnx", "", false},

		// FP16 variant: only _fp16 files match
		{"decoder_model_fp16.onnx", "fp16", true},
		{"decoder_model.onnx", "fp16", false},
		{"decoder_model_int8.onnx", "fp16", false},

		// INT8 variant
		{"decoder_model_int8.onnx", "int8", true},
		{"decoder_model.onnx", "int8", false},

		// Non-ONNX files always match
		{"tokenizer.json", "", true},
		{"tokenizer.json", "fp16", true},
		{"config.json", "int8", true},
		{"merges.txt", "", true},

		// External data files follow the same rules
		{"encoder_model.onnx_data", "", true},
		{"encoder_model_fp16.onnx_data", "", false},
		{"encoder_model_fp16.onnx_data", "fp16", true},
	}

	for _, tt := range tests {
		name := tt.filename + "_variant_" + tt.variant
		t.Run(name, func(t *testing.T) {
			got := matchesVariantSuffix(tt.filename, tt.variant)
			if got != tt.want {
				t.Errorf("matchesVariantSuffix(%q, %q) = %v, want %v", tt.filename, tt.variant, got, tt.want)
			}
		})
	}
}

func TestSelectGeneratorFiles_FlatRepoDefaultVariant(t *testing.T) {
	// Simulates onnx-community/Florence-2-base-ft file listing
	files := []string{
		"config.json",
		"tokenizer.json",
		"tokenizer_config.json",
		"special_tokens_map.json",
		"generation_config.json",
		"added_tokens.json",
		"merges.txt",
		"onnx/decoder_model.onnx",
		"onnx/decoder_model_fp16.onnx",
		"onnx/decoder_model_int8.onnx",
		"onnx/decoder_model_bnb4.onnx",
		"onnx/decoder_model_merged.onnx",
		"onnx/decoder_model_merged_fp16.onnx",
		"onnx/decoder_model_merged_int8.onnx",
		"onnx/decoder_with_past_model.onnx",
		"onnx/decoder_with_past_model_fp16.onnx",
		"onnx/embed_tokens.onnx",
		"onnx/embed_tokens_fp16.onnx",
		"onnx/embed_tokens_int8.onnx",
		"onnx/encoder_model.onnx",
		"onnx/encoder_model_fp16.onnx",
		"onnx/encoder_model_int8.onnx",
		"onnx/vision_encoder.onnx",
		"onnx/vision_encoder_fp16.onnx",
		"onnx/vision_encoder_int8.onnx",
	}

	result := selectGeneratorFiles(files, "")

	// Collect only ONNX files from result
	var onnxFiles []string
	for _, f := range result {
		base := filepath.Base(f)
		if isONNXFile(base) {
			onnxFiles = append(onnxFiles, base)
		}
	}

	// No variant files should be included
	for _, f := range onnxFiles {
		if suffix := onnxVariantSuffix(f); suffix != "" {
			t.Errorf("unexpected variant file in result: %s (suffix %s)", f, suffix)
		}
	}

	// All base ONNX files should be present
	expectedBase := []string{
		"decoder_model.onnx",
		"decoder_model_merged.onnx",
		"decoder_with_past_model.onnx",
		"embed_tokens.onnx",
		"encoder_model.onnx",
		"vision_encoder.onnx",
	}
	for _, expected := range expectedBase {
		found := slices.Contains(onnxFiles, expected)
		if !found {
			t.Errorf("expected base file %q not in result", expected)
		}
	}

	// Config/tokenizer files should be present
	hasConfig := false
	hasTokenizer := false
	for _, f := range result {
		base := filepath.Base(f)
		if base == "config.json" {
			hasConfig = true
		}
		if base == "tokenizer.json" {
			hasTokenizer = true
		}
	}
	if !hasConfig {
		t.Error("config.json not in result")
	}
	if !hasTokenizer {
		t.Error("tokenizer.json not in result")
	}
}

func TestSelectGeneratorFiles_FlatRepoFP16Variant(t *testing.T) {
	files := []string{
		"config.json",
		"tokenizer.json",
		"onnx/decoder_model.onnx",
		"onnx/decoder_model_fp16.onnx",
		"onnx/decoder_model_int8.onnx",
		"onnx/encoder_model.onnx",
		"onnx/encoder_model_fp16.onnx",
		"onnx/encoder_model_int8.onnx",
		"onnx/vision_encoder.onnx",
		"onnx/vision_encoder_fp16.onnx",
		"onnx/embed_tokens.onnx",
		"onnx/embed_tokens_fp16.onnx",
	}

	result := selectGeneratorFiles(files, "fp16")

	var onnxFiles []string
	for _, f := range result {
		base := filepath.Base(f)
		if strings.HasSuffix(base, ".onnx") {
			onnxFiles = append(onnxFiles, base)
		}
	}

	// Should only have _fp16 variants
	for _, f := range onnxFiles {
		if suffix := onnxVariantSuffix(f); suffix != "_fp16" {
			t.Errorf("expected only _fp16 variants, got: %s (suffix %q)", f, suffix)
		}
	}

	// Should have all fp16 variants
	expectedFP16 := []string{
		"decoder_model_fp16.onnx",
		"encoder_model_fp16.onnx",
		"vision_encoder_fp16.onnx",
		"embed_tokens_fp16.onnx",
	}
	for _, expected := range expectedFP16 {
		found := slices.Contains(onnxFiles, expected)
		if !found {
			t.Errorf("expected fp16 file %q not in result", expected)
		}
	}

	// Config files should still be included
	hasConfig := false
	for _, f := range result {
		if filepath.Base(f) == "config.json" {
			hasConfig = true
		}
	}
	if !hasConfig {
		t.Error("config.json not in result")
	}
}

func TestSelectGeneratorFiles_SubdirVariant(t *testing.T) {
	// Simulates a repo with subdirectory variants (e.g., Phi-3)
	files := []string{
		"cpu-int4-awq-block-128/genai_config.json",
		"cpu-int4-awq-block-128/model.onnx",
		"cpu-int4-awq-block-128/model.onnx.data",
		"cpu-int4-awq-block-128/tokenizer.json",
		"cuda-int4-awq-block-128/genai_config.json",
		"cuda-int4-awq-block-128/model.onnx",
		"cuda-int4-awq-block-128/model.onnx.data",
		"cuda-int4-awq-block-128/tokenizer.json",
	}

	result := selectGeneratorFiles(files, "cpu-int4-awq-block-128")

	// Should only include files from the cpu-int4 subdirectory
	for _, f := range result {
		if !strings.HasPrefix(f, "cpu-int4-awq-block-128/") {
			t.Errorf("unexpected file from wrong variant: %s", f)
		}
	}

	// Should include the genai_config, model, and tokenizer
	if len(result) != 4 {
		t.Errorf("expected 4 files, got %d: %v", len(result), result)
	}
}

func TestFindSmallestGeneratorVariant_FlatRepo(t *testing.T) {
	// Florence-2 style: no genai_config.json anywhere
	files := []string{
		"config.json",
		"onnx/encoder_model.onnx",
		"onnx/decoder_model.onnx",
		"onnx/decoder_model_fp16.onnx",
	}

	variant := findSmallestGeneratorVariant(files)
	if variant != "" {
		t.Errorf("expected empty variant for flat repo, got %q", variant)
	}
}

func TestFindSmallestGeneratorVariant_SubdirRepo(t *testing.T) {
	files := []string{
		"cpu-int4/genai_config.json",
		"cpu-int4/model.onnx",
		"cpu/genai_config.json",
		"cpu/model.onnx",
		"cuda-int4/genai_config.json",
		"cuda-int4/model.onnx",
	}

	variant := findSmallestGeneratorVariant(files)
	if variant != "cpu-int4" {
		t.Errorf("expected cpu-int4, got %q", variant)
	}
}
