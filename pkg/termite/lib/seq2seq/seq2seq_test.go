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

package seq2seq

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatLMQGInput(t *testing.T) {
	tests := []struct {
		name     string
		answer   string
		context  string
		expected string
	}{
		{
			name:     "simple input",
			answer:   "Beyonce",
			context:  "Beyonce starred as Etta James in Cadillac Records.",
			expected: "generate question: <hl> Beyonce <hl> Beyonce starred as Etta James in Cadillac Records.",
		},
		{
			name:     "input with numbers",
			answer:   "1955",
			context:  "Steve Jobs was born in 1955 in San Francisco.",
			expected: "generate question: <hl> 1955 <hl> Steve Jobs was born in 1955 in San Francisco.",
		},
		{
			name:     "empty answer",
			answer:   "",
			context:  "Some context here.",
			expected: "generate question: <hl>  <hl> Some context here.",
		},
		{
			name:     "empty context",
			answer:   "Answer",
			context:  "",
			expected: "generate question: <hl> Answer <hl> ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatLMQGInput(tt.answer, tt.context)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatLMQGInputBatch(t *testing.T) {
	pairs := []AnswerContextPair{
		{Answer: "Beyonce", Context: "Beyonce starred as Etta James."},
		{Answer: "Python", Context: "Python is a programming language."},
		{Answer: "1990", Context: "The event happened in 1990."},
	}

	results := FormatLMQGInputBatch(pairs)

	require.Len(t, results, 3)
	assert.Equal(t, "generate question: <hl> Beyonce <hl> Beyonce starred as Etta James.", results[0])
	assert.Equal(t, "generate question: <hl> Python <hl> Python is a programming language.", results[1])
	assert.Equal(t, "generate question: <hl> 1990 <hl> The event happened in 1990.", results[2])
}

func TestFormatLMQGInputBatch_Empty(t *testing.T) {
	pairs := []AnswerContextPair{}
	results := FormatLMQGInputBatch(pairs)
	assert.Empty(t, results)
}

func TestIsSeq2SeqModel(t *testing.T) {
	// Create a temporary directory with seq2seq model files
	tmpDir := t.TempDir()

	// Test 1: Directory without required files should return false
	assert.False(t, IsSeq2SeqModel(tmpDir), "Empty directory should not be a seq2seq model")

	// Create required seq2seq files
	requiredFiles := []string{"encoder.onnx", "decoder-init.onnx", "decoder.onnx"}
	for _, file := range requiredFiles {
		err := os.WriteFile(filepath.Join(tmpDir, file), []byte("dummy"), 0644)
		require.NoError(t, err)
	}

	// Test 2: Directory with all required files should return true
	assert.True(t, IsSeq2SeqModel(tmpDir), "Directory with all seq2seq files should be a seq2seq model")

	// Test 3: Missing one file should return false
	_ = os.Remove(filepath.Join(tmpDir, "encoder.onnx"))
	assert.False(t, IsSeq2SeqModel(tmpDir), "Directory missing encoder.onnx should not be a seq2seq model")
}

func TestIsQuestionGenerationModel(t *testing.T) {
	tmpDir := t.TempDir()

	// Create required seq2seq files
	requiredFiles := []string{"encoder.onnx", "decoder-init.onnx", "decoder.onnx"}
	for _, file := range requiredFiles {
		err := os.WriteFile(filepath.Join(tmpDir, file), []byte("dummy"), 0644)
		require.NoError(t, err)
	}

	// Test 1: Seq2seq model without config, name doesn't contain hints
	assert.False(t, IsQuestionGenerationModel(tmpDir), "Model without qg hints should not be question generation")

	// Test 2: Add config with question_generation task
	config := `{"task": "question_generation", "max_length": 64}`
	err := os.WriteFile(filepath.Join(tmpDir, "seq2seq_config.json"), []byte(config), 0644)
	require.NoError(t, err)
	assert.True(t, IsQuestionGenerationModel(tmpDir), "Model with question_generation task should be detected")

	// Test 3: Change task to something else
	config = `{"task": "paraphrase", "max_length": 60}`
	err = os.WriteFile(filepath.Join(tmpDir, "seq2seq_config.json"), []byte(config), 0644)
	require.NoError(t, err)
	assert.False(t, IsQuestionGenerationModel(tmpDir), "Model with paraphrase task should not be question generation")
}

func TestIsParaphraseModel(t *testing.T) {
	tmpDir := t.TempDir()

	// Create required seq2seq files
	requiredFiles := []string{"encoder.onnx", "decoder-init.onnx", "decoder.onnx"}
	for _, file := range requiredFiles {
		err := os.WriteFile(filepath.Join(tmpDir, file), []byte("dummy"), 0644)
		require.NoError(t, err)
	}

	// Test 1: Seq2seq model without config, name doesn't contain hints
	assert.False(t, IsParaphraseModel(tmpDir), "Model without paraphrase hints should not be paraphrase model")

	// Test 2: Add config with paraphrase task
	config := `{"task": "paraphrase", "max_length": 60}`
	err := os.WriteFile(filepath.Join(tmpDir, "seq2seq_config.json"), []byte(config), 0644)
	require.NoError(t, err)
	assert.True(t, IsParaphraseModel(tmpDir), "Model with paraphrase task should be detected")

	// Test 3: Change task to something else
	config = `{"task": "question_generation", "max_length": 64}`
	err = os.WriteFile(filepath.Join(tmpDir, "seq2seq_config.json"), []byte(config), 0644)
	require.NoError(t, err)
	assert.False(t, IsParaphraseModel(tmpDir), "Model with question_generation task should not be paraphrase model")
}

func TestIsParaphraseModel_ByName(t *testing.T) {
	// Test detection by model name when no config exists
	tmpDir := t.TempDir()

	// Create a subdirectory with paraphrase in the name
	paraphraseDir := filepath.Join(tmpDir, "pegasus_paraphrase")
	err := os.MkdirAll(paraphraseDir, 0755)
	require.NoError(t, err)

	// Create required seq2seq files
	requiredFiles := []string{"encoder.onnx", "decoder-init.onnx", "decoder.onnx"}
	for _, file := range requiredFiles {
		err := os.WriteFile(filepath.Join(paraphraseDir, file), []byte("dummy"), 0644)
		require.NoError(t, err)
	}

	// Model should be detected by name
	assert.True(t, IsParaphraseModel(paraphraseDir), "Model with paraphrase in name should be detected")
}

func TestConfigDefaults(t *testing.T) {
	config := Config{}

	// Default values should be zero-values
	assert.Empty(t, config.ModelID)
	assert.Empty(t, config.Task)
	assert.Equal(t, 0, config.MaxLength)
	assert.Equal(t, 0, config.NumBeams)
	assert.Equal(t, 0, config.NumReturnSequences)
	assert.Empty(t, config.InputFormat)
	assert.False(t, config.DoSample)
	assert.Equal(t, float32(0), config.TopP)
	assert.Equal(t, float32(0), config.Temperature)
}

func TestConfigParaphraseSettings(t *testing.T) {
	// Test config with paraphrase-typical settings
	config := Config{
		ModelID:            "tuner007/pegasus_paraphrase",
		Task:               "paraphrase",
		MaxLength:          60,
		NumBeams:           10,
		NumReturnSequences: 10,
		DoSample:           false,
		Temperature:        1.5,
		InputFormat:        "{text}",
	}

	assert.Equal(t, "tuner007/pegasus_paraphrase", config.ModelID)
	assert.Equal(t, "paraphrase", config.Task)
	assert.Equal(t, 60, config.MaxLength)
	assert.Equal(t, 10, config.NumBeams)
	assert.Equal(t, 10, config.NumReturnSequences)
	assert.Equal(t, float32(1.5), config.Temperature)
	assert.Equal(t, "{text}", config.InputFormat)
}

func TestAnswerContextPair(t *testing.T) {
	pair := AnswerContextPair{
		Answer:  "Python",
		Context: "Python is a high-level programming language.",
	}

	assert.Equal(t, "Python", pair.Answer)
	assert.Equal(t, "Python is a high-level programming language.", pair.Context)
}

func TestGeneratedOutput(t *testing.T) {
	output := GeneratedOutput{
		Texts: [][]string{
			{"paraphrase 1", "paraphrase 2"},
			{"another paraphrase"},
		},
		Tokens: [][][]uint32{
			{{1, 2, 3}, {4, 5, 6}},
			{{7, 8, 9}},
		},
	}

	assert.Len(t, output.Texts, 2)
	assert.Len(t, output.Texts[0], 2)
	assert.Len(t, output.Texts[1], 1)
	assert.Equal(t, "paraphrase 1", output.Texts[0][0])
	assert.Equal(t, "paraphrase 2", output.Texts[0][1])
	assert.Equal(t, "another paraphrase", output.Texts[1][0])

	assert.Len(t, output.Tokens, 2)
	assert.Len(t, output.Tokens[0], 2)
	assert.Equal(t, []uint32{1, 2, 3}, output.Tokens[0][0])
}

func TestContainsAny(t *testing.T) {
	tests := []struct {
		s           string
		substrings  []string
		expected    bool
		description string
	}{
		{"pegasus_paraphrase", []string{"paraphrase"}, true, "contains paraphrase"},
		{"flan-t5-small-qg", []string{"qg", "question"}, true, "contains qg"},
		{"bge-small-en", []string{"paraphrase", "qg"}, false, "contains neither"},
		{"", []string{"test"}, false, "empty string"},
		{"test", []string{}, false, "empty substrings"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			result := containsAny(tt.s, tt.substrings...)
			assert.Equal(t, tt.expected, result)
		})
	}
}
