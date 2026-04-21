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

//go:build onnx && ORT

package backends

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/knights-analytics/ortgenai"
)

// onnxGenerativeSessionFactory creates GenerativeSessions using ortgenai.
type onnxGenerativeSessionFactory struct{}

func (f *onnxGenerativeSessionFactory) CreateGenerativeSession(modelPath string, opts ...SessionOption) (GenerativeSession, error) {
	session, err := createOrtgenaiSession(modelPath)
	if err != nil {
		return nil, err
	}
	// Read context_length from genai_config.json
	contextLength := readContextLength(modelPath)
	return &onnxGenerativeSession{
		session:       session,
		modelPath:     modelPath,
		contextLength: contextLength,
	}, nil
}

func (f *onnxGenerativeSessionFactory) SupportsGenerativeModel(modelPath string) bool {
	// Check for genai_config.json (native ortgenai format)
	if _, err := os.Stat(filepath.Join(modelPath, "genai_config.json")); err == nil {
		return true
	}
	// Check for config.json + model.onnx (HuggingFace ONNX format)
	hasConfig := false
	hasModel := false
	if _, err := os.Stat(filepath.Join(modelPath, "config.json")); err == nil {
		hasConfig = true
	}
	if _, err := os.Stat(filepath.Join(modelPath, "model.onnx")); err == nil {
		hasModel = true
	}
	return hasConfig && hasModel
}

func (f *onnxGenerativeSessionFactory) Backend() BackendType {
	return BackendONNX
}

// onnxGenerativeSession wraps ortgenai.Session for the GenerativeSession interface.
type onnxGenerativeSession struct {
	session       *ortgenai.Session
	modelPath     string
	contextLength int // max sequence length from genai_config.json
}

func (s *onnxGenerativeSession) Generate(ctx context.Context, messages []GenerativeMessage, opts *GenerativeOptions) (*GenerativeResult, error) {
	if opts == nil {
		opts = DefaultGenerativeOptions()
	}

	// MaxLength is ortgenai's total sequence budget (input + output).
	// Use the model's full context length so large inputs (e.g. RAG prompts) are not rejected.
	maxLength := s.contextLength
	if maxLength <= 0 {
		maxLength = 8192 // reasonable default
	}

	genOpts := &ortgenai.GenerationOptions{
		MaxLength: maxLength,
		BatchSize: 1,
	}

	// Convert messages to ortgenai format
	ortMessages := toOrtgenaiMessages(messages)

	// Use a cancellable context so we can stop generation when MaxTokens is reached.
	// ortgenai only has MaxLength (input + output budget), so we enforce the output
	// token limit here by cancelling the context after receiving enough tokens.
	genCtx, genCancel := context.WithCancel(ctx)
	defer genCancel()

	// Check for multimodal (images)
	var outputChan <-chan ortgenai.SequenceDelta
	var errChan <-chan error
	var err error

	if hasImageMessages(messages) {
		imageURLs := extractImageURLsFromMessages(messages)
		images, loadErr := ortgenai.LoadImages(imageURLs)
		if loadErr != nil {
			return nil, fmt.Errorf("loading images: %w", loadErr)
		}
		defer images.Destroy()
		outputChan, errChan, err = s.session.GenerateWithImages(genCtx, [][]ortgenai.Message{ortMessages}, images, genOpts)
	} else {
		outputChan, errChan, err = s.session.Generate(genCtx, [][]ortgenai.Message{ortMessages}, genOpts)
	}

	if err != nil {
		return nil, fmt.Errorf("starting generation: %w", err)
	}

	// Collect tokens, enforcing MaxTokens output limit
	maxOutputTokens := opts.MaxTokens
	var generatedText strings.Builder
	var tokenCount int
	for delta := range outputChan {
		generatedText.WriteString(delta.Token)
		tokenCount++
		if maxOutputTokens > 0 && tokenCount >= maxOutputTokens {
			genCancel()
			break
		}
	}

	// Determine finish reason
	finishReason := "stop"
	if maxOutputTokens > 0 && tokenCount >= maxOutputTokens {
		finishReason = "length"
	}

	// Drain remaining tokens after cancel (channel may still have buffered items)
	for range outputChan {
	}

	// Check for errors (ignore context.Canceled since we may have cancelled intentionally)
	for err := range errChan {
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, fmt.Errorf("generation error: %w", err)
		}
	}

	return &GenerativeResult{
		Text:         generatedText.String(),
		TokensUsed:   tokenCount,
		FinishReason: finishReason,
	}, nil
}

func (s *onnxGenerativeSession) GenerateStream(ctx context.Context, messages []GenerativeMessage, opts *GenerativeOptions) (<-chan GenerativeToken, <-chan error, error) {
	if opts == nil {
		opts = DefaultGenerativeOptions()
	}

	// MaxLength is ortgenai's total sequence budget (input + output).
	// Use the model's full context length so large inputs (e.g. RAG prompts) are not rejected.
	maxLength := s.contextLength
	if maxLength <= 0 {
		maxLength = 8192 // reasonable default
	}

	genOpts := &ortgenai.GenerationOptions{
		MaxLength: maxLength,
		BatchSize: 1,
	}

	// Convert messages to ortgenai format
	ortMessages := toOrtgenaiMessages(messages)

	// Use a cancellable context so we can stop generation when MaxTokens is reached.
	genCtx, genCancel := context.WithCancel(ctx)

	// Start generation
	outputChan, ortErrChan, err := s.session.Generate(genCtx, [][]ortgenai.Message{ortMessages}, genOpts)
	if err != nil {
		genCancel()
		return nil, nil, fmt.Errorf("starting streaming generation: %w", err)
	}

	// Adapt channels, enforcing MaxTokens output limit
	maxOutputTokens := opts.MaxTokens
	tokenChan := make(chan GenerativeToken)
	errChan := make(chan error, 1)

	go func() {
		defer close(tokenChan)
		defer close(errChan)
		defer genCancel()

		var tokenCount int
		for delta := range outputChan {
			select {
			case <-ctx.Done():
				return
			case tokenChan <- GenerativeToken{Token: delta.Token, Index: delta.Sequence}:
			}
			tokenCount++
			if maxOutputTokens > 0 && tokenCount >= maxOutputTokens {
				genCancel()
				break
			}
		}

		// Drain remaining tokens after cancel
		for range outputChan {
		}

		for err := range ortErrChan {
			if err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errChan <- err:
				default:
				}
			}
		}
	}()

	return tokenChan, errChan, nil
}

func (s *onnxGenerativeSession) Close() error {
	if s.session != nil {
		s.session.Destroy()
		s.session = nil
	}
	return nil
}

// InitOrtgenaiForModel sets library path, initializes the ortgenai environment,
// and ensures required config files (config.json, chat_template.jinja) exist.
func InitOrtgenaiForModel(modelPath string) error {
	if genaiPath := getGenAILibraryPath(); genaiPath != "" {
		ortgenai.SetSharedLibraryPath(genaiPath)
	}
	if err := ortgenai.InitializeEnvironment(); err != nil {
		if !strings.Contains(err.Error(), "already") {
			return fmt.Errorf("initializing ortgenai environment: %w", err)
		}
	}
	if err := ensureHuggingFaceConfig(modelPath); err != nil {
		return fmt.Errorf("ensuring config.json: %w", err)
	}
	if err := ensureChatTemplate(modelPath); err != nil {
		return fmt.Errorf("ensuring chat_template.jinja: %w", err)
	}
	return nil
}

// createOrtgenaiSession creates an ortgenai session.
func createOrtgenaiSession(modelPath string) (*ortgenai.Session, error) {
	if err := InitOrtgenaiForModel(modelPath); err != nil {
		return nil, err
	}
	session, err := ortgenai.CreateSession(modelPath)
	if err != nil {
		return nil, fmt.Errorf("creating ortgenai session: %w", err)
	}
	return session, nil
}

// CreateOrtgenaiEngine creates an ortgenai Engine for continuous batching.
func CreateOrtgenaiEngine(modelPath string) (*ortgenai.Engine, error) {
	if err := InitOrtgenaiForModel(modelPath); err != nil {
		return nil, err
	}
	engine, err := ortgenai.CreateEngine(modelPath)
	if err != nil {
		return nil, fmt.Errorf("creating ortgenai engine: %w", err)
	}
	return engine, nil
}

// ReadContextLength reads the context_length from genai_config.json.
// Returns 0 if not found or error.
func ReadContextLength(modelPath string) int {
	return readContextLength(modelPath)
}

// ensureHuggingFaceConfig generates a minimal config.json from genai_config.json if missing.
// ortgenai requires config.json even when genai_config.json is present.
func ensureHuggingFaceConfig(modelPath string) error {
	configPath := filepath.Join(modelPath, "config.json")
	genaiConfigPath := filepath.Join(modelPath, "genai_config.json")

	// Skip if config.json already exists
	if _, err := os.Stat(configPath); err == nil {
		return nil
	}

	// Need genai_config.json to generate from
	genaiData, err := os.ReadFile(genaiConfigPath)
	if err != nil {
		return nil // No genai_config.json either, nothing to generate from
	}

	var genaiConfig map[string]any
	if err := json.Unmarshal(genaiData, &genaiConfig); err != nil {
		return fmt.Errorf("parsing genai_config.json: %w", err)
	}

	// Extract model config from genai_config.json
	modelConfig, ok := genaiConfig["model"].(map[string]any)
	if !ok {
		return fmt.Errorf("genai_config.json missing 'model' section")
	}

	// Build minimal HuggingFace config.json
	hfConfig := map[string]any{
		"architectures": []string{"GemmaForCausalLM"}, // Default
	}

	// Extract model type and set architectures
	if mt, ok := modelConfig["type"].(string); ok {
		hfConfig["model_type"] = mt
		switch mt {
		case "gemma":
			hfConfig["architectures"] = []string{"GemmaForCausalLM"}
		case "llama":
			hfConfig["architectures"] = []string{"LlamaForCausalLM"}
		case "mistral":
			hfConfig["architectures"] = []string{"MistralForCausalLM"}
		case "phi", "phi3":
			hfConfig["architectures"] = []string{"PhiForCausalLM"}
		case "qwen2":
			hfConfig["architectures"] = []string{"Qwen2ForCausalLM"}
		case "gpt2":
			hfConfig["architectures"] = []string{"GPT2LMHeadModel"}
		}
	}

	// Extract decoder config if present
	if decoder, ok := modelConfig["decoder"].(map[string]any); ok {
		if vs, ok := decoder["vocab_size"].(float64); ok {
			hfConfig["vocab_size"] = int(vs)
		}
		if hs, ok := decoder["hidden_size"].(float64); ok {
			hfConfig["hidden_size"] = int(hs)
		}
		if nl, ok := decoder["num_hidden_layers"].(float64); ok {
			hfConfig["num_hidden_layers"] = int(nl)
		}
		if nh, ok := decoder["num_attention_heads"].(float64); ok {
			hfConfig["num_attention_heads"] = int(nh)
		}
		if nkv, ok := decoder["num_key_value_heads"].(float64); ok {
			hfConfig["num_key_value_heads"] = int(nkv)
		}
		if is, ok := decoder["intermediate_size"].(float64); ok {
			hfConfig["intermediate_size"] = int(is)
		}
		if hd, ok := decoder["head_size"].(float64); ok {
			hfConfig["head_dim"] = int(hd)
		}
	}

	// Extract context length and special tokens
	if cl, ok := modelConfig["context_length"].(float64); ok {
		hfConfig["max_position_embeddings"] = int(cl)
	}
	if bos, ok := modelConfig["bos_token_id"].(float64); ok {
		hfConfig["bos_token_id"] = int(bos)
	}
	if eos, ok := modelConfig["eos_token_id"].(float64); ok {
		hfConfig["eos_token_id"] = int(eos)
	}
	if pad, ok := modelConfig["pad_token_id"].(float64); ok {
		hfConfig["pad_token_id"] = int(pad)
	}

	configData, err := json.MarshalIndent(hfConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config.json: %w", err)
	}

	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return fmt.Errorf("writing config.json: %w", err)
	}

	return nil
}

// ensureChatTemplate ensures chat_template.jinja exists for generative models.
// ortgenai requires a chat template for message formatting.
func ensureChatTemplate(modelPath string) error {
	chatTemplatePath := filepath.Join(modelPath, "chat_template.jinja")

	// Skip if chat_template.jinja already exists
	if _, err := os.Stat(chatTemplatePath); err == nil {
		return nil
	}

	// Try to extract from tokenizer_config.json
	tokenizerConfigPath := filepath.Join(modelPath, "tokenizer_config.json")
	if data, err := os.ReadFile(tokenizerConfigPath); err == nil {
		var tokenizerConfig map[string]any
		if err := json.Unmarshal(data, &tokenizerConfig); err == nil {
			if template, ok := tokenizerConfig["chat_template"].(string); ok && template != "" {
				if err := os.WriteFile(chatTemplatePath, []byte(template), 0644); err != nil {
					return fmt.Errorf("writing chat_template.jinja from tokenizer_config: %w", err)
				}
				return nil
			}
		}
	}

	// Determine model type from genai_config.json or config.json
	modelType := getModelType(modelPath)

	// Get default template for model type
	template := getDefaultChatTemplate(modelType)
	if template == "" {
		// Use generic Gemma-style template as fallback
		template = defaultGemmaTemplate
	}

	if err := os.WriteFile(chatTemplatePath, []byte(template), 0644); err != nil {
		return fmt.Errorf("writing default chat_template.jinja: %w", err)
	}

	return nil
}

// readContextLength reads the context_length from genai_config.json.
// Returns 0 if not found or error.
func readContextLength(modelPath string) int {
	genaiConfigPath := filepath.Join(modelPath, "genai_config.json")
	data, err := os.ReadFile(genaiConfigPath)
	if err != nil {
		return 0
	}

	var genaiConfig map[string]any
	if err := json.Unmarshal(data, &genaiConfig); err != nil {
		return 0
	}

	if modelSection, ok := genaiConfig["model"].(map[string]any); ok {
		if cl, ok := modelSection["context_length"].(float64); ok {
			return int(cl)
		}
	}

	return 0
}

// getModelType determines the model type from config files.
func getModelType(modelPath string) string {
	// Try genai_config.json first
	genaiConfigPath := filepath.Join(modelPath, "genai_config.json")
	if data, err := os.ReadFile(genaiConfigPath); err == nil {
		var genaiConfig map[string]any
		if err := json.Unmarshal(data, &genaiConfig); err == nil {
			if modelSection, ok := genaiConfig["model"].(map[string]any); ok {
				if mt, ok := modelSection["type"].(string); ok {
					return mt
				}
			}
		}
	}

	// Try config.json
	configPath := filepath.Join(modelPath, "config.json")
	if data, err := os.ReadFile(configPath); err == nil {
		var config map[string]any
		if err := json.Unmarshal(data, &config); err == nil {
			if mt, ok := config["model_type"].(string); ok {
				return mt
			}
		}
	}

	return ""
}

// getDefaultChatTemplate returns a default Jinja2 chat template for the given model type.
func getDefaultChatTemplate(modelType string) string {
	switch strings.ToLower(modelType) {
	case "gemma", "gemma2", "gemma3", "gemma3_text":
		return defaultGemmaTemplate
	case "llama", "llama2", "llama3":
		return defaultLlamaTemplate
	case "phi", "phi3":
		return defaultPhiTemplate
	case "mistral":
		return defaultMistralTemplate
	default:
		return ""
	}
}

// Default Jinja2 chat templates for common model types.
const defaultGemmaTemplate = `{{ bos_token }}{% for message in messages %}{% if message['role'] == 'user' %}<start_of_turn>user
{{ message['content'] }}<end_of_turn>
{% elif message['role'] == 'assistant' %}<start_of_turn>model
{{ message['content'] }}<end_of_turn>
{% elif message['role'] == 'system' %}<start_of_turn>user
{{ message['content'] }}

{% endif %}{% endfor %}{% if add_generation_prompt %}<start_of_turn>model
{% endif %}`

const defaultLlamaTemplate = `{% for message in messages %}{% if message['role'] == 'system' %}<<SYS>>
{{ message['content'] }}
<</SYS>>

{% elif message['role'] == 'user' %}[INST] {{ message['content'] }} [/INST]
{% elif message['role'] == 'assistant' %}{{ message['content'] }}
{% endif %}{% endfor %}`

const defaultMistralTemplate = `{% for message in messages %}{% if message['role'] == 'user' %}[INST] {{ message['content'] }} [/INST]{% elif message['role'] == 'assistant' %}{{ message['content'] }}</s>{% endif %}{% endfor %}`

const defaultPhiTemplate = `{% for message in messages %}{% if message['role'] == 'system' %}<|system|>
{{ message['content'] }}<|end|>
{% elif message['role'] == 'user' %}<|user|>
{{ message['content'] }}<|end|>
{% elif message['role'] == 'assistant' %}<|assistant|>
{{ message['content'] }}<|end|>
{% endif %}{% endfor %}{% if add_generation_prompt %}<|assistant|>
{% endif %}`

// getGenAILibraryPath returns the full path to libonnxruntime-genai.
// Discovery order:
//  1. ORTGENAI_DYLIB_PATH environment variable (explicit path)
//  2. ONNXRUNTIME_ROOT environment variable
//  3. lib/ directory next to the running binary (omni install layout)
//  4. LD_LIBRARY_PATH or DYLD_LIBRARY_PATH
func getGenAILibraryPath() string {
	libName := getGenAILibraryName()

	// Check explicit ORTGENAI_DYLIB_PATH first
	if path := os.Getenv("ORTGENAI_DYLIB_PATH"); path != "" {
		return path
	}

	// Check ONNXRUNTIME_ROOT
	if root := os.Getenv("ONNXRUNTIME_ROOT"); root != "" {
		platform := runtime.GOOS + "-" + runtime.GOARCH
		platformPath := filepath.Join(root, platform, "lib", libName)
		if _, err := os.Stat(platformPath); err == nil {
			return platformPath
		}
		directPath := filepath.Join(root, "lib", libName)
		if _, err := os.Stat(directPath); err == nil {
			return directPath
		}
	}

	// Check lib/ relative to the binary (omni install layout)
	if dir := findLibRelativeToBinary(libName); dir != "" {
		return filepath.Join(dir, libName)
	}

	// Check LD_LIBRARY_PATH / DYLD_LIBRARY_PATH
	ldPath := os.Getenv("LD_LIBRARY_PATH")
	if runtime.GOOS == "darwin" {
		if dyldPath := os.Getenv("DYLD_LIBRARY_PATH"); dyldPath != "" {
			ldPath = dyldPath
		}
	}
	if ldPath != "" {
		for _, dir := range filepath.SplitList(ldPath) {
			libPath := filepath.Join(dir, libName)
			if _, err := os.Stat(libPath); err == nil {
				return libPath
			}
		}
	}

	return ""
}

// getGenAILibraryName returns the platform-specific library name.
func getGenAILibraryName() string {
	switch runtime.GOOS {
	case "windows":
		return "onnxruntime-genai.dll"
	case "darwin":
		return "libonnxruntime-genai.dylib"
	default:
		return "libonnxruntime-genai.so"
	}
}

// toOrtgenaiMessages converts GenerativeMessage to ortgenai.Message format.
// For multimodal models, it inserts image placeholder tokens (<start_of_image>)
// at the beginning of messages that have images.
func toOrtgenaiMessages(messages []GenerativeMessage) []ortgenai.Message {
	result := make([]ortgenai.Message, len(messages))
	for i, m := range messages {
		content := m.Content
		// Insert image placeholder tokens for each image in this message
		// Multimodal models like Gemma-3 expect <start_of_image> as placeholder
		if len(m.ImageURLs) > 0 {
			var imagePlaceholders strings.Builder
			for range m.ImageURLs {
				imagePlaceholders.WriteString("<start_of_image>")
			}
			content = imagePlaceholders.String() + content
		}
		result[i] = ortgenai.Message{
			Role:    m.Role,
			Content: content,
		}
	}
	return result
}

// hasImageMessages checks if any message has images.
func hasImageMessages(messages []GenerativeMessage) bool {
	for _, m := range messages {
		if len(m.ImageURLs) > 0 {
			return true
		}
	}
	return false
}

// extractImageURLsFromMessages extracts all image URLs from messages.
func extractImageURLsFromMessages(messages []GenerativeMessage) []string {
	var urls []string
	for _, m := range messages {
		urls = append(urls, m.ImageURLs...)
	}
	return urls
}
