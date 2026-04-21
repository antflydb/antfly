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

package generation

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ToolParser handles prompt formatting and output parsing for tool calling.
// Different model families implement this interface with their specific formats.
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

// ToolParserFactory creates a ToolParser for a given model path.
type ToolParserFactory func(modelPath string) (ToolParser, error)

// toolParserRegistry maps format names to factory functions.
var (
	toolParserRegistry   = make(map[string]ToolParserFactory)
	toolParserRegistryMu sync.RWMutex
)

func init() {
	// Register built-in parsers
	RegisterToolParser("functiongemma", NewFunctionGemmaParser)
}

// RegisterToolParser adds a new parser factory to the registry.
func RegisterToolParser(name string, factory ToolParserFactory) {
	toolParserRegistryMu.Lock()
	defer toolParserRegistryMu.Unlock()
	toolParserRegistry[name] = factory
}

// GetToolParser returns a parser for the given format name.
// Returns nil, nil if format is empty (no tool calling support).
// Returns an error if format is unknown.
func GetToolParser(format string, modelPath string) (ToolParser, error) {
	if format == "" {
		return nil, nil // No tool calling support
	}

	toolParserRegistryMu.RLock()
	factory, ok := toolParserRegistry[format]
	toolParserRegistryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown tool_call_format: %s", format)
	}

	return factory(modelPath)
}

// ListToolParsers returns the names of all registered tool parsers.
func ListToolParsers() []string {
	toolParserRegistryMu.RLock()
	defer toolParserRegistryMu.RUnlock()

	names := make([]string, 0, len(toolParserRegistry))
	for name := range toolParserRegistry {
		names = append(names, name)
	}
	return names
}

// FunctionGemmaTokens holds special tokens read from the model's tokenizer config.
type FunctionGemmaTokens struct {
	StartFunctionDecl string `json:"start_function_declaration"`
	EndFunctionDecl   string `json:"end_function_declaration"`
	StartFunctionCall string `json:"start_function_call"`
	EndFunctionCall   string `json:"end_function_call"`
	Escape            string `json:"escape"`
}

// DefaultFunctionGemmaTokens provides fallback tokens if not found in config.
var DefaultFunctionGemmaTokens = FunctionGemmaTokens{
	StartFunctionDecl: "<start_function_declaration>",
	EndFunctionDecl:   "<end_function_declaration>",
	StartFunctionCall: "<start_function_call>",
	EndFunctionCall:   "<end_function_call>",
	Escape:            "<escape>",
}

// FunctionGemmaParser handles FunctionGemma's tool calling format.
// It parses output in the format:
//
//	<start_function_call>function_name{param1:<escape>value1<escape>,param2:<escape>value2<escape>}<end_function_call>
type FunctionGemmaParser struct {
	tokens        FunctionGemmaTokens
	buffer        strings.Builder
	toolCalls     []ToolCall
	textSegments  []string
	callIDCounter int
	inFuncCall    bool
}

// NewFunctionGemmaParser creates a parser, loading tokens from model config.
func NewFunctionGemmaParser(modelPath string) (ToolParser, error) {
	tokens, err := loadFunctionGemmaTokens(modelPath)
	if err != nil {
		// Use defaults if config not found
		tokens = DefaultFunctionGemmaTokens
	}
	return &FunctionGemmaParser{tokens: tokens}, nil
}

// loadFunctionGemmaTokens reads tokens from the model's tokenizer config files.
func loadFunctionGemmaTokens(modelPath string) (FunctionGemmaTokens, error) {
	// Try special_tokens_map.json first
	tokenMapPath := filepath.Join(modelPath, "special_tokens_map.json")
	if tokens, err := loadTokensFromFile(tokenMapPath); err == nil {
		return tokens, nil
	}

	// Fall back to tokenizer_config.json
	tokenizerPath := filepath.Join(modelPath, "tokenizer_config.json")
	return loadTokensFromFile(tokenizerPath)
}

// loadTokensFromFile attempts to load FunctionGemma tokens from a JSON config file.
func loadTokensFromFile(path string) (FunctionGemmaTokens, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FunctionGemmaTokens{}, err
	}

	// Try direct unmarshaling first (for special_tokens_map.json format)
	var tokens FunctionGemmaTokens
	if err := json.Unmarshal(data, &tokens); err == nil {
		if tokens.StartFunctionCall != "" && tokens.EndFunctionCall != "" {
			return tokens, nil
		}
	}

	// Try extracting from added_tokens_decoder (tokenizer_config.json format)
	var config struct {
		AddedTokensDecoder map[string]struct {
			Content string `json:"content"`
		} `json:"added_tokens_decoder"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return FunctionGemmaTokens{}, err
	}

	// Search for FunctionGemma tokens in added_tokens_decoder
	for _, tokenInfo := range config.AddedTokensDecoder {
		content := tokenInfo.Content
		switch {
		case strings.Contains(content, "start_function_declaration"):
			tokens.StartFunctionDecl = content
		case strings.Contains(content, "end_function_declaration"):
			tokens.EndFunctionDecl = content
		case strings.Contains(content, "start_function_call"):
			tokens.StartFunctionCall = content
		case strings.Contains(content, "end_function_call"):
			tokens.EndFunctionCall = content
		case strings.Contains(content, "escape"):
			tokens.Escape = content
		}
	}

	if tokens.StartFunctionCall == "" || tokens.EndFunctionCall == "" {
		return FunctionGemmaTokens{}, fmt.Errorf("missing required FunctionGemma tokens")
	}

	return tokens, nil
}

// Name returns the parser identifier.
func (p *FunctionGemmaParser) Name() string {
	return "functiongemma"
}

// FormatToolsPrompt creates tool declarations for the system prompt.
func (p *FunctionGemmaParser) FormatToolsPrompt(tools []ToolDefinition) string {
	if len(tools) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("You are a model that can do function calling with the following functions.\n\n")

	for _, tool := range tools {
		sb.WriteString(p.tokens.StartFunctionDecl)
		sb.WriteString("declaration:")
		sb.WriteString(tool.Function.Name)
		sb.WriteString("{description:")
		sb.WriteString(p.tokens.Escape)
		sb.WriteString(tool.Function.Description)
		sb.WriteString(p.tokens.Escape)
		sb.WriteString(",parameters:")
		sb.WriteString(p.formatParams(tool.Function.Parameters))
		sb.WriteString("}")
		sb.WriteString(p.tokens.EndFunctionDecl)
		sb.WriteString("\n")
	}

	sb.WriteString("\nWhen you want to call a function, output in this format:\n")
	sb.WriteString(p.tokens.StartFunctionCall)
	sb.WriteString("function_name{param1:")
	sb.WriteString(p.tokens.Escape)
	sb.WriteString("value1")
	sb.WriteString(p.tokens.Escape)
	sb.WriteString(",param2:")
	sb.WriteString(p.tokens.Escape)
	sb.WriteString("value2")
	sb.WriteString(p.tokens.Escape)
	sb.WriteString("}")
	sb.WriteString(p.tokens.EndFunctionCall)
	sb.WriteString("\n")

	return sb.String()
}

// formatParams formats a JSON Schema parameters object for the prompt.
func (p *FunctionGemmaParser) formatParams(params map[string]any) string {
	if len(params) == 0 {
		return "{}"
	}

	// Extract properties from JSON Schema
	properties, ok := params["properties"].(map[string]any)
	if !ok {
		// If not a JSON Schema, just stringify the params
		data, err := json.Marshal(params)
		if err != nil {
			return "{}"
		}
		return string(data)
	}

	required, _ := params["required"].([]any)
	requiredSet := make(map[string]bool)
	for _, r := range required {
		if s, ok := r.(string); ok {
			requiredSet[s] = true
		}
	}

	var parts []string
	for name, propRaw := range properties {
		prop, ok := propRaw.(map[string]any)
		if !ok {
			continue
		}

		propType, _ := prop["type"].(string)
		desc, _ := prop["description"].(string)

		paramStr := fmt.Sprintf("%s:{type:%s%s,description:%s%s%s}",
			name, propType,
			p.requiredStr(requiredSet[name]),
			p.tokens.Escape, desc, p.tokens.Escape)
		parts = append(parts, paramStr)
	}

	return "{" + strings.Join(parts, ",") + "}"
}

func (p *FunctionGemmaParser) requiredStr(required bool) string {
	if required {
		return ",required:true"
	}
	return ""
}

// Feed processes incoming tokens and detects complete tool calls.
func (p *FunctionGemmaParser) Feed(token string) []ToolCall {
	p.buffer.WriteString(token)
	content := p.buffer.String()

	var newCalls []ToolCall

	// Look for complete function calls
	for {
		startIdx := strings.Index(content, p.tokens.StartFunctionCall)
		if startIdx == -1 {
			break
		}

		endIdx := strings.Index(content[startIdx:], p.tokens.EndFunctionCall)
		if endIdx == -1 {
			// Incomplete call, wait for more tokens
			break
		}
		endIdx += startIdx + len(p.tokens.EndFunctionCall)

		// Extract the function call
		callContent := content[startIdx+len(p.tokens.StartFunctionCall) : endIdx-len(p.tokens.EndFunctionCall)]

		// Parse the call
		if tc, ok := p.parseCall(callContent); ok {
			newCalls = append(newCalls, tc)
			p.toolCalls = append(p.toolCalls, tc)
		}

		// Save any text before the call
		if startIdx > 0 {
			p.textSegments = append(p.textSegments, content[:startIdx])
		}

		// Continue scanning after this call
		content = content[endIdx:]
	}

	// Update buffer with remaining content
	p.buffer.Reset()
	p.buffer.WriteString(content)

	return newCalls
}

// parseCall parses a single function call from the extracted content.
// Format: function_name{param1:<escape>value1<escape>,param2:<escape>value2<escape>}
func (p *FunctionGemmaParser) parseCall(content string) (ToolCall, bool) {
	// Find the function name (everything before the first {)
	before, after, ok := strings.Cut(content, "{")
	if !ok {
		return ToolCall{}, false
	}

	funcName := strings.TrimSpace(before)
	if funcName == "" {
		return ToolCall{}, false
	}

	// Extract parameters
	paramsStr := after
	paramsStr = strings.TrimSuffix(paramsStr, "}")

	// Parse key-value pairs
	params := p.splitParams(paramsStr)

	// Convert to JSON
	argsJSON, err := json.Marshal(params)
	if err != nil {
		argsJSON = []byte("{}")
	}

	// Generate unique call ID
	p.callIDCounter++
	callID := generateCallID()

	return ToolCall{
		ID:   callID,
		Type: "function",
		Function: ToolCallFunction{
			Name:      funcName,
			Arguments: string(argsJSON),
		},
	}, true
}

// splitParams parses the parameter string into a map.
// Format: param1:<escape>value1<escape>,param2:<escape>value2<escape>
func (p *FunctionGemmaParser) splitParams(s string) map[string]any {
	result := make(map[string]any)
	if s == "" {
		return result
	}

	// Split by comma (but not within escape sequences)
	var current strings.Builder
	inEscape := false
	escapeToken := p.tokens.Escape

	i := 0
	for i < len(s) {
		// Check for escape token
		if strings.HasPrefix(s[i:], escapeToken) {
			inEscape = !inEscape
			i += len(escapeToken)
			continue
		}

		// Check for comma separator (only outside escapes)
		if !inEscape && s[i] == ',' {
			p.parseParam(current.String(), result)
			current.Reset()
			i++
			continue
		}

		current.WriteByte(s[i])
		i++
	}

	// Parse final param
	if current.Len() > 0 {
		p.parseParam(current.String(), result)
	}

	return result
}

// parseParam parses a single "key:value" parameter.
func (p *FunctionGemmaParser) parseParam(param string, result map[string]any) {
	before, after, ok := strings.Cut(param, ":")
	if !ok {
		return
	}

	key := strings.TrimSpace(before)
	value := strings.TrimSpace(after)

	if key == "" {
		return
	}

	// Try to parse value as JSON (for numbers, booleans, etc.)
	var jsonValue any
	if err := json.Unmarshal([]byte(value), &jsonValue); err == nil {
		result[key] = jsonValue
	} else {
		result[key] = value
	}
}

// Finish completes parsing and returns all tool calls and remaining text.
func (p *FunctionGemmaParser) Finish() (toolCalls []ToolCall, remainingText string) {
	// Process any remaining content in buffer
	remaining := p.buffer.String()
	if remaining != "" {
		p.textSegments = append(p.textSegments, remaining)
	}

	return p.toolCalls, strings.Join(p.textSegments, "")
}

// Reset clears parser state for reuse.
func (p *FunctionGemmaParser) Reset() {
	p.buffer.Reset()
	p.toolCalls = nil
	p.textSegments = nil
	p.callIDCounter = 0
	p.inFuncCall = false
}

// generateCallID generates a unique call ID similar to OpenAI's format.
func generateCallID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "call_" + hex.EncodeToString(b)
}
