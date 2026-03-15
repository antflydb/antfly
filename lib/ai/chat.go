// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"go.uber.org/zap"
)

// Aliases for ChatToolName constants for convenience
const (
	ToolNameFilter        = ChatToolNameAddFilter
	ToolNameClarification = ChatToolNameAskClarification
	ToolNameSearch        = ChatToolNameSearch
	ToolNameWebSearch     = ChatToolNameWebsearch
	ToolNameFetch         = ChatToolNameFetch

	// Retrieval-specific tool names
	ToolNameSemanticSearch = ChatToolNameSemanticSearch
	ToolNameFullTextSearch = ChatToolNameFullTextSearch
	ToolNameTreeSearch     = ChatToolNameTreeSearch
	ToolNameGraphSearch    = ChatToolNameGraphSearch
)

// Static tool schemas for native tools (those that don't depend on runtime index names).
var (
	filterSchema = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"field": map[string]any{
				"type":        "string",
				"description": "The field name to filter on (e.g., 'published_date', 'category', 'status')",
			},
			"operator": map[string]any{
				"type":        "string",
				"enum":        []string{"eq", "ne", "gt", "gte", "lt", "lte", "contains", "prefix", "range", "in"},
				"description": "The filter operator: eq (equals), ne (not equals), gt/gte (greater than), lt/lte (less than), contains (substring), prefix (starts with), range (between values), in (value in list)",
			},
			"value": map[string]any{
				"description": "The filter value. For range operator, provide [min, max] array. For 'in' operator, provide array of values.",
			},
		},
		"required": []string{"field", "operator", "value"},
	}

	clarificationSchema = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"question": map[string]any{
				"type":        "string",
				"description": "The clarifying question to ask the user",
			},
			"options": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional list of suggested answers for the user to choose from (2-5 options recommended)",
			},
		},
		"required": []string{"question"},
	}

	searchSchema = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "The semantic search query to execute",
			},
		},
		"required": []string{"query"},
	}

	websearchSchema = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "The web search query",
			},
			"num_results": map[string]any{
				"type":        "integer",
				"description": "Number of results to return (1-10, default 5)",
				"minimum":     1,
				"maximum":     10,
			},
		},
		"required": []string{"query"},
	}

	fetchSchema = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"url": map[string]any{
				"type":        "string",
				"format":      "uri",
				"description": "The URL to fetch content from",
			},
			"extract_mode": map[string]any{
				"type":        "string",
				"enum":        []string{"text", "markdown", "raw"},
				"description": "How to extract content: 'text' (readable text, default), 'markdown' (preserve structure), 'raw' (minimal processing)",
			},
		},
		"required": []string{"url"},
	}
)

// Tool description constants shared between native and fallback paths.
const (
	filterDescription        = "Add a filter to narrow search results. Use this when the user wants to filter by specific field values like date ranges, categories, status, or other attributes."
	clarificationDescription = "Ask the user a clarifying question before searching. Use this when the query is ambiguous, missing important details, or could be interpreted multiple ways."
	searchDescription        = "Execute a semantic search query. Use this to find relevant documents based on the user's request or to explore related topics."
	websearchDescription     = "Search the web for external information. Use this when the internal documents don't have the answer, or when the user explicitly asks for current/external information. Returns search results with titles, snippets, and URLs."
	fetchDescription         = "Fetch and extract content from a URL. Supports web pages (extracts readable text), PDFs (extracts text), and plain text files. Use this to get detailed content from a specific URL, such as from web search results."
)

// defaultChatTools are the tools enabled when EnabledTools is nil/empty.
var defaultChatTools = []ChatToolName{ToolNameFilter, ToolNameClarification, ToolNameSearch}

// indexTools are auto-enabled when EnabledTools is nil/empty.
var indexTools = []ChatToolName{ToolNameSemanticSearch, ToolNameFullTextSearch, ToolNameTreeSearch, ToolNameGraphSearch}

// IsToolEnabled checks if a specific tool is enabled in the config.
// When EnabledTools is nil/empty, the default chat tools plus all index tools are enabled.
// When EnabledTools is explicitly set, only the listed tools are enabled.
func (c ChatToolsConfig) IsToolEnabled(tool ChatToolName) bool {
	if c.EnabledTools == nil || len(*c.EnabledTools) == 0 {
		return slices.Contains(defaultChatTools, tool) || slices.Contains(indexTools, tool)
	}
	return slices.Contains(*c.EnabledTools, tool)
}

// groupIndexesByType groups indexes by their canonical type.
// Accepts both canonical ("embeddings") and legacy ("aknn_v0") type names.
func groupIndexesByType(indexes []IndexInfo) (embeddings, fullText, graph []IndexInfo) {
	for _, idx := range indexes {
		switch idx.Type {
		case "embeddings", "aknn_v0":
			embeddings = append(embeddings, idx)
		case "full_text", "full_text_v0":
			fullText = append(fullText, idx)
		case "graph", "graph_v0":
			graph = append(graph, idx)
		}
	}
	return
}

// configWithoutSearch returns a copy of config with ToolNameSearch removed from EnabledTools.
// Used by retrieval tools which replace the generic "search" with specific search tools.
func configWithoutSearch(config ChatToolsConfig) ChatToolsConfig {
	if config.EnabledTools == nil || !slices.Contains(*config.EnabledTools, ToolNameSearch) {
		return config
	}
	filtered := make([]ChatToolName, 0, len(*config.EnabledTools)-1)
	for _, t := range *config.EnabledTools {
		if t != ToolNameSearch {
			filtered = append(filtered, t)
		}
	}
	config.EnabledTools = &filtered
	return config
}

// indexNames extracts index names from a slice of IndexInfo.
func indexNames(indexes []IndexInfo) []string {
	names := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		names = append(names, idx.Name)
	}
	return names
}

// Schema builders for index-based tools. These produce rich schemas with
// runtime index name enums, shared between native and fallback paths.

func semanticSearchSchema(indexNames []string, defaultIndex string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "The natural language search query",
			},
			"index": map[string]any{
				"type":        "string",
				"description": fmt.Sprintf("Which index to search. Available: %s. Default: %s", strings.Join(indexNames, ", "), defaultIndex),
				"enum":        indexNames,
			},
			"limit": map[string]any{
				"type":        "integer",
				"description": "Maximum number of results to return (default: 10)",
				"minimum":     1,
				"maximum":     100,
			},
		},
		"required": []string{"query"},
	}
}

func semanticSearchDescription(indexNames []string) string {
	return fmt.Sprintf("Execute a semantic/vector search. Available indexes: %s. Use this to find documents by meaning similarity.", strings.Join(indexNames, ", "))
}

func fullTextSearchSchema(indexNames []string, defaultIndex string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "The search query (supports BM25 text matching)",
			},
			"index": map[string]any{
				"type":        "string",
				"description": fmt.Sprintf("Which index to search. Available: %s. Default: %s", strings.Join(indexNames, ", "), defaultIndex),
				"enum":        indexNames,
			},
			"limit": map[string]any{
				"type":        "integer",
				"description": "Maximum number of results to return (default: 10)",
				"minimum":     1,
				"maximum":     100,
			},
			"fields": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional list of fields to search within. If empty, searches all indexed fields.",
			},
		},
		"required": []string{"query"},
	}
}

func fullTextSearchDescription(indexNames []string) string {
	return fmt.Sprintf("Execute a full-text BM25 search for keyword matching. Available indexes: %s. Use this for exact keyword/phrase matching.", strings.Join(indexNames, ", "))
}

func treeSearchSchema(indexNames []string, defaultIndex string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "The natural language query guiding the tree exploration",
			},
			"index": map[string]any{
				"type":        "string",
				"description": fmt.Sprintf("Which graph index to use. Available: %s. Default: %s", strings.Join(indexNames, ", "), defaultIndex),
				"enum":        indexNames,
			},
			"start_nodes": map[string]any{
				"type":        "string",
				"description": "Optional starting node query to begin the tree search from. If empty, starts from graph roots.",
			},
			"max_depth": map[string]any{
				"type":        "integer",
				"description": "Maximum depth to traverse (default: 3)",
				"minimum":     1,
				"maximum":     10,
			},
			"beam_width": map[string]any{
				"type":        "integer",
				"description": "Number of branches to explore at each level (default: 3)",
				"minimum":     1,
				"maximum":     10,
			},
		},
		"required": []string{"query"},
	}
}

func treeSearchDescription(indexNames []string) string {
	return fmt.Sprintf("Execute a tree search with beam search navigation through a document graph. Available indexes: %s. Use this to explore hierarchical or linked document structures.", strings.Join(indexNames, ", "))
}

func graphSearchSchema(indexNames []string, defaultIndex string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"start_node": map[string]any{
				"type":        "string",
				"description": "The ID of the starting node for the traversal",
			},
			"index": map[string]any{
				"type":        "string",
				"description": fmt.Sprintf("Which graph index to use. Available: %s. Default: %s", strings.Join(indexNames, ", "), defaultIndex),
				"enum":        indexNames,
			},
			"edge_type": map[string]any{
				"type":        "string",
				"description": "Optional edge type to filter by (e.g., 'references', 'parent_of')",
			},
			"direction": map[string]any{
				"type":        "string",
				"enum":        []string{"outgoing", "incoming", "both"},
				"description": "Direction of edges to follow (default: outgoing)",
			},
			"depth": map[string]any{
				"type":        "integer",
				"description": "Maximum traversal depth (default: 1)",
				"minimum":     1,
				"maximum":     10,
			},
		},
		"required": []string{"start_node"},
	}
}

func graphSearchDescription(indexNames []string) string {
	return fmt.Sprintf("Execute a graph traversal to explore relationships between documents. Available indexes: %s. Use this to find connected documents by following edges.", strings.Join(indexNames, ", "))
}

// ProviderCapabilities describes what a generator provider supports
type ProviderCapabilities struct {
	SupportsTools     bool
	SupportsMultiturn bool
	SupportsMedia     bool
}

// GetProviderCapabilities returns the capabilities for a given provider
func GetProviderCapabilities(provider GeneratorProvider) ProviderCapabilities {
	switch provider {
	case GeneratorProviderOllama:
		// Ollama does not support native tool calling
		return ProviderCapabilities{
			SupportsTools:     false,
			SupportsMultiturn: true,
			SupportsMedia:     true,
		}
	case GeneratorProviderAnthropic, GeneratorProviderOpenai, GeneratorProviderOpenrouter, GeneratorProviderGemini, GeneratorProviderVertex:
		// These providers support native tool calling
		return ProviderCapabilities{
			SupportsTools:     true,
			SupportsMultiturn: true,
			SupportsMedia:     true,
		}
	case GeneratorProviderBedrock:
		// Bedrock supports tools for most models
		return ProviderCapabilities{
			SupportsTools:     true,
			SupportsMultiturn: true,
			SupportsMedia:     true,
		}
	case GeneratorProviderCohere:
		// Cohere Command R+ supports tools
		return ProviderCapabilities{
			SupportsTools:     true,
			SupportsMultiturn: true,
			SupportsMedia:     false,
		}
	default:
		// Default to no tools for unknown providers
		return ProviderCapabilities{
			SupportsTools:     false,
			SupportsMultiturn: true,
			SupportsMedia:     false,
		}
	}
}

// ChatMessagesToGenKit converts Antfly ChatMessage types to GenKit ai.Message types.
// This enables passing conversation history from the API layer through to GenKit prompts.
func ChatMessagesToGenKit(messages []ChatMessage) []*ai.Message {
	if len(messages) == 0 {
		return nil
	}

	result := make([]*ai.Message, 0, len(messages))
	for _, msg := range messages {
		var role ai.Role
		switch msg.Role {
		case ChatMessageRoleUser:
			role = ai.RoleUser
		case ChatMessageRoleAssistant:
			role = ai.RoleModel
		case ChatMessageRoleSystem:
			role = ai.RoleSystem
		case ChatMessageRoleTool:
			role = ai.RoleTool
		default:
			role = ai.RoleUser
		}

		var parts []*ai.Part

		// Add text content
		if msg.Content != "" {
			parts = append(parts, ai.NewTextPart(msg.Content))
		}

		// Add tool call parts for assistant messages
		if msg.ToolCalls != nil {
			for _, tc := range *msg.ToolCalls {
				parts = append(parts, ai.NewToolRequestPart(&ai.ToolRequest{
					Name:  tc.Name,
					Input: any(tc.Arguments),
					Ref:   tc.Id,
				}))
			}
		}

		// Add tool result parts for tool messages
		if msg.ToolResults != nil {
			for _, tr := range *msg.ToolResults {
				parts = append(parts, ai.NewToolResponsePart(&ai.ToolResponse{
					Output: any(tr.Result),
					Ref:    tr.ToolCallId,
				}))
			}
		}

		if len(parts) > 0 {
			result = append(result, &ai.Message{
				Role:    role,
				Content: parts,
			})
		}
	}

	return result
}

// GenKitToChatMessages converts GenKit ai.Message types back to Antfly ChatMessage types.
// This is used to populate response messages from the LLM output.
func GenKitToChatMessages(messages []*ai.Message) []ChatMessage {
	if len(messages) == 0 {
		return nil
	}

	result := make([]ChatMessage, 0, len(messages))
	for _, msg := range messages {
		var role ChatMessageRole
		switch msg.Role {
		case ai.RoleUser:
			role = ChatMessageRoleUser
		case ai.RoleModel:
			role = ChatMessageRoleAssistant
		case ai.RoleSystem:
			role = ChatMessageRoleSystem
		case ai.RoleTool:
			role = ChatMessageRoleTool
		default:
			role = ChatMessageRoleUser
		}

		cm := ChatMessage{
			Role: role,
		}

		// Extract text content and tool parts
		var toolCalls []ChatToolCall
		var toolResults []ChatToolResult

		for _, part := range msg.Content {
			if part.Text != "" {
				cm.Content += part.Text
			}
			if part.IsToolRequest() {
				tr := part.ToolRequest
				args := make(map[string]any)
				if input, ok := tr.Input.(map[string]any); ok {
					args = input
				}
				toolCalls = append(toolCalls, ChatToolCall{
					Id:        tr.Ref,
					Name:      tr.Name,
					Arguments: args,
				})
			}
			if part.IsToolResponse() {
				tr := part.ToolResponse
				resultMap := make(map[string]any)
				if output, ok := tr.Output.(map[string]any); ok {
					resultMap = output
				}
				toolResults = append(toolResults, ChatToolResult{
					ToolCallId: tr.Ref,
					Result:     resultMap,
				})
			}
		}

		if len(toolCalls) > 0 {
			cm.ToolCalls = &toolCalls
		}
		if len(toolResults) > 0 {
			cm.ToolResults = &toolResults
		}

		result = append(result, cm)
	}

	return result
}

// ChatToolDefinitions returns the tool definitions for chat agent
func ChatToolDefinitions(config ChatToolsConfig) []ai.ToolDefinition {
	var tools []ai.ToolDefinition

	if config.IsToolEnabled(ToolNameFilter) {
		tools = append(tools, ai.ToolDefinition{Name: string(ToolNameFilter), Description: filterDescription, InputSchema: filterSchema})
	}
	if config.IsToolEnabled(ToolNameClarification) {
		tools = append(tools, ai.ToolDefinition{Name: string(ToolNameClarification), Description: clarificationDescription, InputSchema: clarificationSchema})
	}
	if config.IsToolEnabled(ToolNameSearch) {
		tools = append(tools, ai.ToolDefinition{Name: string(ToolNameSearch), Description: searchDescription, InputSchema: searchSchema})
	}
	if config.IsToolEnabled(ToolNameWebSearch) {
		tools = append(tools, ai.ToolDefinition{Name: string(ToolNameWebSearch), Description: websearchDescription, InputSchema: websearchSchema})
	}
	if config.IsToolEnabled(ToolNameFetch) {
		tools = append(tools, ai.ToolDefinition{Name: string(ToolNameFetch), Description: fetchDescription, InputSchema: fetchSchema})
	}

	return tools
}

// ParsedToolAction represents a tool action parsed from structured output
type ParsedToolAction struct {
	ToolName  ChatToolName
	Arguments map[string]any
}

// ParseStructuredOutput parses tool actions from structured output format
// Used for models that don't support native tool calling
func ParseStructuredOutput(text string) ([]ParsedToolAction, string, error) {
	var actions []ParsedToolAction
	remainingText := text

	// Parse <filter> tags
	filterStart := strings.Index(text, "<filter>")
	for filterStart != -1 {
		filterEnd := strings.Index(text[filterStart:], "</filter>")
		if filterEnd == -1 {
			break
		}
		filterEnd += filterStart + len("</filter>")

		filterContent := text[filterStart+len("<filter>") : filterEnd-len("</filter>")]
		action, err := parseFilterContent(filterContent)
		if err == nil {
			actions = append(actions, action)
		}

		// Remove from remaining text
		remainingText = strings.Replace(remainingText, text[filterStart:filterEnd], "", 1)
		filterStart = strings.Index(text[filterEnd:], "<filter>")
		if filterStart != -1 {
			filterStart += filterEnd
		}
	}

	// Parse <clarification> tags
	clarStart := strings.Index(text, "<clarification>")
	for clarStart != -1 {
		clarEnd := strings.Index(text[clarStart:], "</clarification>")
		if clarEnd == -1 {
			break
		}
		clarEnd += clarStart + len("</clarification>")

		clarContent := text[clarStart+len("<clarification>") : clarEnd-len("</clarification>")]
		action, err := parseClarificationContent(clarContent)
		if err == nil {
			actions = append(actions, action)
		}

		remainingText = strings.Replace(remainingText, text[clarStart:clarEnd], "", 1)
		clarStart = strings.Index(text[clarEnd:], "<clarification>")
		if clarStart != -1 {
			clarStart += clarEnd
		}
	}

	// Parse <search> tags
	searchStart := strings.Index(text, "<search>")
	for searchStart != -1 {
		searchEnd := strings.Index(text[searchStart:], "</search>")
		if searchEnd == -1 {
			break
		}
		searchEnd += searchStart + len("</search>")

		searchContent := text[searchStart+len("<search>") : searchEnd-len("</search>")]
		action, err := parseSearchContent(searchContent)
		if err == nil {
			actions = append(actions, action)
		}

		remainingText = strings.Replace(remainingText, text[searchStart:searchEnd], "", 1)
		searchStart = strings.Index(text[searchEnd:], "<search>")
		if searchStart != -1 {
			searchStart += searchEnd
		}
	}

	// Parse <websearch> tags
	wsStart := strings.Index(text, "<websearch>")
	for wsStart != -1 {
		wsEnd := strings.Index(text[wsStart:], "</websearch>")
		if wsEnd == -1 {
			break
		}
		wsEnd += wsStart + len("</websearch>")

		wsContent := text[wsStart+len("<websearch>") : wsEnd-len("</websearch>")]
		action, err := parseWebsearchContent(wsContent)
		if err == nil {
			actions = append(actions, action)
		}

		remainingText = strings.Replace(remainingText, text[wsStart:wsEnd], "", 1)
		wsStart = strings.Index(text[wsEnd:], "<websearch>")
		if wsStart != -1 {
			wsStart += wsEnd
		}
	}

	// Parse <fetch> tags
	fetchStart := strings.Index(text, "<fetch>")
	for fetchStart != -1 {
		fetchEnd := strings.Index(text[fetchStart:], "</fetch>")
		if fetchEnd == -1 {
			break
		}
		fetchEnd += fetchStart + len("</fetch>")

		fetchContent := text[fetchStart+len("<fetch>") : fetchEnd-len("</fetch>")]
		action, err := parseFetchContent(fetchContent)
		if err == nil {
			actions = append(actions, action)
		}

		remainingText = strings.Replace(remainingText, text[fetchStart:fetchEnd], "", 1)
		fetchStart = strings.Index(text[fetchEnd:], "<fetch>")
		if fetchStart != -1 {
			fetchStart += fetchEnd
		}
	}

	// Parse <semantic_search> tags
	actions, remainingText = parseTaggedActions(text, remainingText, "semantic_search", ToolNameSemanticSearch, actions, parseSemanticSearchContent)

	// Parse <full_text_search> tags
	actions, remainingText = parseTaggedActions(text, remainingText, "full_text_search", ToolNameFullTextSearch, actions, parseFullTextSearchContent)

	// Parse <tree_search> tags
	actions, remainingText = parseTaggedActions(text, remainingText, "tree_search", ToolNameTreeSearch, actions, parseTreeSearchContent)

	// Parse <graph_search> tags
	actions, remainingText = parseTaggedActions(text, remainingText, "graph_search", ToolNameGraphSearch, actions, parseGraphSearchContent)

	return actions, strings.TrimSpace(remainingText), nil
}

func parseFilterContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "field:"); ok {
			args["field"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "operator:"); ok {
			args["operator"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "value:"); ok {
			args["value"] = strings.TrimSpace(after)
		}
	}

	if args["field"] == nil || args["operator"] == nil || args["value"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required filter fields")
	}

	return ParsedToolAction{
		ToolName:  ToolNameFilter,
		Arguments: args,
	}, nil
}

func parseClarificationContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)
	var options []string
	inOptions := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "question:"); ok {
			args["question"] = strings.TrimSpace(after)
			inOptions = false
		} else if strings.HasPrefix(line, "options:") {
			inOptions = true
		} else if inOptions && strings.HasPrefix(line, "- ") {
			options = append(options, strings.TrimPrefix(line, "- "))
		}
	}

	if args["question"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required question field")
	}

	if len(options) > 0 {
		args["options"] = options
	}

	return ParsedToolAction{
		ToolName:  ToolNameClarification,
		Arguments: args,
	}, nil
}

func parseSearchContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "query:"); ok {
			args["query"] = strings.TrimSpace(after)
		}
	}

	if args["query"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required query field")
	}

	return ParsedToolAction{
		ToolName:  ToolNameSearch,
		Arguments: args,
	}, nil
}

func parseWebsearchContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "query:"); ok {
			args["query"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "num_results:"); ok {
			args["num_results"] = strings.TrimSpace(after)
		}
	}

	if args["query"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required query field")
	}

	return ParsedToolAction{
		ToolName:  ToolNameWebSearch,
		Arguments: args,
	}, nil
}

func parseFetchContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if url, ok := strings.CutPrefix(line, "url:"); ok {
			args["url"] = strings.TrimSpace(url)
		} else if extractMode, ok := strings.CutPrefix(line, "extract_mode:"); ok {
			args["extract_mode"] = strings.TrimSpace(extractMode)
		}
	}

	if args["url"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required url field")
	}

	return ParsedToolAction{
		ToolName:  ToolNameFetch,
		Arguments: args,
	}, nil
}

// parseTaggedActions is a generic helper for parsing XML-tagged tool actions from structured output
func parseTaggedActions(text, remainingText, tagName string, toolName ChatToolName, actions []ParsedToolAction, parser func(string) (ParsedToolAction, error)) ([]ParsedToolAction, string) {
	openTag := "<" + tagName + ">"
	closeTag := "</" + tagName + ">"
	start := strings.Index(text, openTag)
	for start != -1 {
		end := strings.Index(text[start:], closeTag)
		if end == -1 {
			break
		}
		end += start + len(closeTag)

		content := text[start+len(openTag) : end-len(closeTag)]
		action, err := parser(content)
		if err == nil {
			actions = append(actions, action)
		}

		remainingText = strings.Replace(remainingText, text[start:end], "", 1)
		start = strings.Index(text[end:], openTag)
		if start != -1 {
			start += end
		}
	}
	return actions, remainingText
}

func parseSemanticSearchContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "query:"); ok {
			args["query"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "index:"); ok {
			args["index"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "limit:"); ok {
			args["limit"] = strings.TrimSpace(after)
		}
	}

	if args["query"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required query field")
	}

	return ParsedToolAction{
		ToolName:  ToolNameSemanticSearch,
		Arguments: args,
	}, nil
}

func parseFullTextSearchContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "query:"); ok {
			args["query"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "index:"); ok {
			args["index"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "limit:"); ok {
			args["limit"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "fields:"); ok {
			args["fields"] = strings.TrimSpace(after)
		}
	}

	if args["query"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required query field")
	}

	return ParsedToolAction{
		ToolName:  ToolNameFullTextSearch,
		Arguments: args,
	}, nil
}

func parseTreeSearchContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "query:"); ok {
			args["query"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "index:"); ok {
			args["index"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "start_nodes:"); ok {
			args["start_nodes"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "max_depth:"); ok {
			args["max_depth"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "beam_width:"); ok {
			args["beam_width"] = strings.TrimSpace(after)
		}
	}

	if args["query"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required query field")
	}

	return ParsedToolAction{
		ToolName:  ToolNameTreeSearch,
		Arguments: args,
	}, nil
}

func parseGraphSearchContent(content string) (ParsedToolAction, error) {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	args := make(map[string]any)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "start_node:"); ok {
			args["start_node"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "index:"); ok {
			args["index"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "edge_type:"); ok {
			args["edge_type"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "direction:"); ok {
			args["direction"] = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "depth:"); ok {
			args["depth"] = strings.TrimSpace(after)
		}
	}

	if args["start_node"] == nil {
		return ParsedToolAction{}, fmt.Errorf("missing required start_node field")
	}

	return ParsedToolAction{
		ToolName:  ToolNameGraphSearch,
		Arguments: args,
	}, nil
}

// ChatContext holds the state for a chat conversation
type ChatContext struct {
	Messages       []ChatMessage
	Filters        []FilterSpec
	ToolIterations int
	Logger         *zap.Logger
}

// FilterSpecToQuery converts a FilterSpec to a Bleve query fragment
func FilterSpecToQuery(filter FilterSpec) (map[string]any, error) {
	switch filter.Operator {
	case "eq":
		return map[string]any{
			"term": map[string]any{
				filter.Field: filter.Value,
			},
		}, nil
	case "ne":
		return map[string]any{
			"bool": map[string]any{
				"must_not": []any{
					map[string]any{
						"term": map[string]any{
							filter.Field: filter.Value,
						},
					},
				},
			},
		}, nil
	case "contains":
		valueStr, ok := filter.Value.(string)
		if !ok {
			return nil, fmt.Errorf("contains operator requires string value")
		}
		return map[string]any{
			"query": fmt.Sprintf("%s:*%s*", filter.Field, valueStr),
		}, nil
	case "prefix":
		valueStr, ok := filter.Value.(string)
		if !ok {
			return nil, fmt.Errorf("prefix operator requires string value")
		}
		return map[string]any{
			"prefix": map[string]any{
				filter.Field: valueStr,
			},
		}, nil
	case "gt", "gte", "lt", "lte":
		return map[string]any{
			"range": map[string]any{
				filter.Field: map[string]any{
					string(filter.Operator): filter.Value,
				},
			},
		}, nil
	case "range":
		values, ok := filter.Value.([]any)
		if !ok || len(values) != 2 {
			return nil, fmt.Errorf("range operator requires [min, max] array")
		}
		return map[string]any{
			"range": map[string]any{
				filter.Field: map[string]any{
					"gte": values[0],
					"lte": values[1],
				},
			},
		}, nil
	case "in":
		values, ok := filter.Value.([]any)
		if !ok {
			return nil, fmt.Errorf("in operator requires array value")
		}
		var shouldClauses []any
		for _, v := range values {
			shouldClauses = append(shouldClauses, map[string]any{
				"term": map[string]any{
					filter.Field: v,
				},
			})
		}
		return map[string]any{
			"bool": map[string]any{
				"should":               shouldClauses,
				"minimum_should_match": 1,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operator: %s", filter.Operator)
	}
}

// ConvertToolCallToArgs extracts arguments from a genkit tool request
func ConvertToolCallToArgs(toolRequest any) (map[string]any, error) {
	// Handle different tool request formats from different providers
	data, err := json.Marshal(toolRequest)
	if err != nil {
		return nil, err
	}

	var args map[string]any
	if err := json.Unmarshal(data, &args); err != nil {
		return nil, err
	}

	return args, nil
}

// ToolExecutionResult represents the result of executing a tool
type ToolExecutionResult struct {
	ToolName ChatToolName `json:"tool_name"`
	Success  bool         `json:"success"`
	Result   any          `json:"result,omitempty"`
	Error    string       `json:"error,omitempty"`
}

// ToolExecutor defines the interface for executing tools
type ToolExecutor interface {
	// ExecuteSearch runs a semantic search query
	ExecuteSearch(ctx context.Context, query string, filters []FilterSpec) ([]map[string]any, error)
	// ExecuteWebSearch runs a web search
	ExecuteWebSearch(ctx context.Context, query string, numResults int) ([]map[string]any, error)
	// ExecuteFetch fetches content from a URL
	ExecuteFetch(ctx context.Context, url string) (map[string]any, error)
}

// ToolExecutorWithCallbacks extends ToolExecutor with callbacks for clarification and filters.
// Methods accept a context so implementations can stream SSE events without
// storing a context.Context in a struct.
type ToolExecutorWithCallbacks interface {
	ToolExecutor
	// SetClarification is called when the model requests clarification
	SetClarification(ctx context.Context, clarification ClarificationRequest)
	// AddFilter is called when the model adds a filter
	AddFilter(ctx context.Context, filter FilterSpec)
}

// IndexInfo describes an available index for dynamic tool creation
type IndexInfo struct {
	// Table is the table this index belongs to
	Table string
	// Name is the index name (e.g., "my_embeddings", "content_search")
	Name string
	// Type is the index type (e.g., "embeddings", "full_text", "graph")
	Type string
	// Description is an optional human-readable description of the index
	Description string
}

// RetrievalToolExecutor extends ToolExecutor with retrieval-specific tool implementations.
// Each method corresponds to a retrieval tool the LLM can invoke.
type RetrievalToolExecutor interface {
	ToolExecutor
	// ExecuteSemanticSearch runs a vector/semantic search against the specified table and index
	ExecuteSemanticSearch(ctx context.Context, table string, query string, index string, limit int, filters []FilterSpec) ([]map[string]any, error)
	// ExecuteFullTextSearch runs a BM25 full-text search against the specified table and index
	ExecuteFullTextSearch(ctx context.Context, table string, query string, index string, limit int, fields []string, filters []FilterSpec) ([]map[string]any, error)
	// ExecuteTreeSearch runs a tree search with beam search navigation on the specified table
	ExecuteTreeSearch(ctx context.Context, table string, index string, startNodes string, query string, maxDepth int, beamWidth int) ([]map[string]any, error)
	// ExecuteGraphSearch runs a graph traversal search on the specified table
	ExecuteGraphSearch(ctx context.Context, table string, index string, startNode string, edgeType string, direction string, depth int) ([]map[string]any, error)
}

// ChatToolRequest represents a parsed tool request from the model
type ChatToolRequest struct {
	ID        string         `json:"id"`
	Name      ChatToolName   `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// ChatToolResponse represents a tool response to send back to the model
type ChatToolResponse struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Content string `json:"content"`
}

// CreateNativeTools creates genkit Tool instances from the tool configuration.
// These tools can be passed to ai.WithTools() for native tool calling.
func CreateNativeTools(g *genkit.Genkit, config ChatToolsConfig, executor ToolExecutor) []ai.Tool {
	var tools []ai.Tool

	// Check if executor supports callbacks
	execWithCallbacks, hasCallbacks := executor.(ToolExecutorWithCallbacks)

	// Filter tool - adds search filters (no execution needed, just returns the filter spec)
	if config.IsToolEnabled(ToolNameFilter) {
		filterTool := ai.NewTool[any, FilterSpec](
			string(ToolNameFilter),
			filterDescription,
			func(ctx *ai.ToolContext, input any) (FilterSpec, error) {
				inputMap, ok := input.(map[string]any)
				if !ok {
					return FilterSpec{}, fmt.Errorf("invalid filter input type")
				}

				field, _ := inputMap["field"].(string)
				operator, _ := inputMap["operator"].(string)
				value := inputMap["value"]

				filter := FilterSpec{
					Field:    field,
					Operator: FilterSpecOperator(operator),
					Value:    value,
				}

				if hasCallbacks {
					execWithCallbacks.AddFilter(ctx, filter)
				}

				return filter, nil
			},
			ai.WithInputSchema(filterSchema),
		)
		tools = append(tools, filterTool)
	}

	// Clarification tool - asks user for more information
	if config.IsToolEnabled(ToolNameClarification) {
		clarificationTool := ai.NewTool[any, ClarificationRequest](
			string(ToolNameClarification),
			clarificationDescription,
			func(ctx *ai.ToolContext, input any) (ClarificationRequest, error) {
				inputMap, ok := input.(map[string]any)
				if !ok {
					return ClarificationRequest{}, fmt.Errorf("invalid clarification input type")
				}

				question, _ := inputMap["question"].(string)
				var options []string
				if optionsRaw, ok := inputMap["options"].([]any); ok {
					for _, opt := range optionsRaw {
						if optStr, ok := opt.(string); ok {
							options = append(options, optStr)
						}
					}
				}

				clarification := ClarificationRequest{
					Question: question,
					Options:  &options,
				}

				if hasCallbacks {
					execWithCallbacks.SetClarification(ctx, clarification)
				}

				return clarification, nil
			},
			ai.WithInputSchema(clarificationSchema),
		)
		tools = append(tools, clarificationTool)
	}

	// Search tool - executes additional searches
	if config.IsToolEnabled(ToolNameSearch) && executor != nil {
		searchTool := ai.NewTool[any, []map[string]any](
			string(ToolNameSearch),
			searchDescription,
			func(ctx *ai.ToolContext, input any) ([]map[string]any, error) {
				inputMap, ok := input.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid search input type")
				}

				query, _ := inputMap["query"].(string)
				if query == "" {
					return nil, fmt.Errorf("query is required")
				}

				return executor.ExecuteSearch(ctx.Context, query, nil)
			},
			ai.WithInputSchema(searchSchema),
		)
		tools = append(tools, searchTool)
	}

	// Web search tool
	if config.IsToolEnabled(ToolNameWebSearch) && executor != nil {
		websearchTool := ai.NewTool[any, []map[string]any](
			string(ToolNameWebSearch),
			websearchDescription,
			func(ctx *ai.ToolContext, input any) ([]map[string]any, error) {
				inputMap, ok := input.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid websearch input type")
				}

				query, _ := inputMap["query"].(string)
				if query == "" {
					return nil, fmt.Errorf("query is required")
				}

				numResults := 5
				if nr, ok := inputMap["num_results"].(float64); ok {
					numResults = int(nr)
				}

				return executor.ExecuteWebSearch(ctx.Context, query, numResults)
			},
			ai.WithInputSchema(websearchSchema),
		)
		tools = append(tools, websearchTool)
	}

	// Fetch tool
	if config.IsToolEnabled(ToolNameFetch) && executor != nil {
		fetchTool := ai.NewTool[any, map[string]any](
			string(ToolNameFetch),
			fetchDescription,
			func(ctx *ai.ToolContext, input any) (map[string]any, error) {
				inputMap, ok := input.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid fetch input type")
				}

				url, _ := inputMap["url"].(string)
				if url == "" {
					return nil, fmt.Errorf("url is required")
				}

				return executor.ExecuteFetch(ctx.Context, url)
			},
			ai.WithInputSchema(fetchSchema),
		)
		tools = append(tools, fetchTool)
	}

	return tools
}

// CreateRetrievalTools creates genkit Tool instances for the retrieval agent based on the
// table's available indexes. Tools are dynamically created from the index configuration.
// Chat tools (filter, clarification, websearch, fetch) are also included if enabled.
func CreateRetrievalTools(g *genkit.Genkit, config ChatToolsConfig, executor RetrievalToolExecutor, availableIndexes []IndexInfo) []ai.Tool {
	// Start with the base chat tools (filter, clarification, websearch, fetch)
	// but exclude "search" since retrieval uses specific search tools instead
	tools := CreateNativeTools(g, configWithoutSearch(config), executor)

	// Build index-name-to-table lookup and group indexes by type (single pass)
	indexTable := make(map[string]string, len(availableIndexes))
	var aknnIndexes, fullTextIndexes, graphIndexes []IndexInfo
	for _, idx := range availableIndexes {
		indexTable[idx.Name] = idx.Table
		switch idx.Type {
		case "embeddings", "aknn_v0":
			aknnIndexes = append(aknnIndexes, idx)
		case "full_text", "full_text_v0":
			fullTextIndexes = append(fullTextIndexes, idx)
		case "graph", "graph_v0":
			graphIndexes = append(graphIndexes, idx)
		}
	}

	// Semantic search tool
	if len(aknnIndexes) > 0 && config.IsToolEnabled(ToolNameSemanticSearch) {
		names := indexNames(aknnIndexes)
		defaultIndex := names[0]
		tools = append(tools, ai.NewTool[any, []map[string]any](
			string(ToolNameSemanticSearch),
			semanticSearchDescription(names),
			func(ctx *ai.ToolContext, input any) ([]map[string]any, error) {
				inputMap, ok := input.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid semantic_search input type")
				}

				query, _ := inputMap["query"].(string)
				if query == "" {
					return nil, fmt.Errorf("query is required")
				}

				index := defaultIndex
				if idx, ok := inputMap["index"].(string); ok && idx != "" {
					index = idx
				}

				limit := 10
				if l, ok := inputMap["limit"].(float64); ok {
					limit = int(l)
				}

				return executor.ExecuteSemanticSearch(ctx.Context, indexTable[index], query, index, limit, nil)
			},
			ai.WithInputSchema(semanticSearchSchema(names, defaultIndex)),
		))
	}

	// Full text search tool
	if len(fullTextIndexes) > 0 && config.IsToolEnabled(ToolNameFullTextSearch) {
		names := indexNames(fullTextIndexes)
		defaultIndex := names[0]
		tools = append(tools, ai.NewTool[any, []map[string]any](
			string(ToolNameFullTextSearch),
			fullTextSearchDescription(names),
			func(ctx *ai.ToolContext, input any) ([]map[string]any, error) {
				inputMap, ok := input.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid full_text_search input type")
				}

				query, _ := inputMap["query"].(string)
				if query == "" {
					return nil, fmt.Errorf("query is required")
				}

				index := defaultIndex
				if idx, ok := inputMap["index"].(string); ok && idx != "" {
					index = idx
				}

				limit := 10
				if l, ok := inputMap["limit"].(float64); ok {
					limit = int(l)
				}

				var fields []string
				if rawFields, ok := inputMap["fields"].([]any); ok {
					for _, f := range rawFields {
						if s, ok := f.(string); ok {
							fields = append(fields, s)
						}
					}
				}

				return executor.ExecuteFullTextSearch(ctx.Context, indexTable[index], query, index, limit, fields, nil)
			},
			ai.WithInputSchema(fullTextSearchSchema(names, defaultIndex)),
		))
	}

	// Tree search and graph search tools
	if len(graphIndexes) > 0 {
		names := indexNames(graphIndexes)
		defaultIndex := names[0]

		if config.IsToolEnabled(ToolNameTreeSearch) {
			tools = append(tools, ai.NewTool[any, []map[string]any](
				string(ToolNameTreeSearch),
				treeSearchDescription(names),
				func(ctx *ai.ToolContext, input any) ([]map[string]any, error) {
					inputMap, ok := input.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("invalid tree_search input type")
					}

					query, _ := inputMap["query"].(string)
					if query == "" {
						return nil, fmt.Errorf("query is required")
					}

					index := defaultIndex
					if idx, ok := inputMap["index"].(string); ok && idx != "" {
						index = idx
					}

					startNodes, _ := inputMap["start_nodes"].(string)

					maxDepth := 3
					if d, ok := inputMap["max_depth"].(float64); ok {
						maxDepth = int(d)
					}

					beamWidth := 3
					if b, ok := inputMap["beam_width"].(float64); ok {
						beamWidth = int(b)
					}

					return executor.ExecuteTreeSearch(ctx.Context, indexTable[index], index, startNodes, query, maxDepth, beamWidth)
				},
				ai.WithInputSchema(treeSearchSchema(names, defaultIndex)),
			))
		}

		if config.IsToolEnabled(ToolNameGraphSearch) {
			tools = append(tools, ai.NewTool[any, []map[string]any](
				string(ToolNameGraphSearch),
				graphSearchDescription(names),
				func(ctx *ai.ToolContext, input any) ([]map[string]any, error) {
					inputMap, ok := input.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("invalid graph_search input type")
					}

					startNode, _ := inputMap["start_node"].(string)
					if startNode == "" {
						return nil, fmt.Errorf("start_node is required")
					}

					index := defaultIndex
					if idx, ok := inputMap["index"].(string); ok && idx != "" {
						index = idx
					}

					edgeType, _ := inputMap["edge_type"].(string)

					direction := "outgoing"
					if d, ok := inputMap["direction"].(string); ok && d != "" {
						direction = d
					}

					depth := 1
					if d, ok := inputMap["depth"].(float64); ok {
						depth = int(d)
					}

					return executor.ExecuteGraphSearch(ctx.Context, indexTable[index], index, startNode, edgeType, direction, depth)
				},
				ai.WithInputSchema(graphSearchSchema(names, defaultIndex)),
			))
		}
	}

	return tools
}

// RetrievalToolDefinitions returns tool definitions for the retrieval agent
// (used for structured output fallback with non-native tool providers)
func RetrievalToolDefinitions(config ChatToolsConfig, availableIndexes []IndexInfo) []ai.ToolDefinition {
	// Start with base chat tool definitions (minus "search" which is replaced by specific search tools)
	tools := ChatToolDefinitions(configWithoutSearch(config))

	aknnIndexes, fullTextIndexes, graphIndexes := groupIndexesByType(availableIndexes)

	if len(aknnIndexes) > 0 && config.IsToolEnabled(ToolNameSemanticSearch) {
		names := indexNames(aknnIndexes)
		tools = append(tools, ai.ToolDefinition{Name: string(ToolNameSemanticSearch), Description: semanticSearchDescription(names), InputSchema: semanticSearchSchema(names, names[0])})
	}

	if len(fullTextIndexes) > 0 && config.IsToolEnabled(ToolNameFullTextSearch) {
		names := indexNames(fullTextIndexes)
		tools = append(tools, ai.ToolDefinition{Name: string(ToolNameFullTextSearch), Description: fullTextSearchDescription(names), InputSchema: fullTextSearchSchema(names, names[0])})
	}

	if len(graphIndexes) > 0 {
		names := indexNames(graphIndexes)
		if config.IsToolEnabled(ToolNameTreeSearch) {
			tools = append(tools, ai.ToolDefinition{Name: string(ToolNameTreeSearch), Description: treeSearchDescription(names), InputSchema: treeSearchSchema(names, names[0])})
		}
		if config.IsToolEnabled(ToolNameGraphSearch) {
			tools = append(tools, ai.ToolDefinition{Name: string(ToolNameGraphSearch), Description: graphSearchDescription(names), InputSchema: graphSearchSchema(names, names[0])})
		}
	}

	return tools
}

// ParseToolRequestsFromResponse extracts tool requests from a model response
func ParseToolRequestsFromResponse(resp *ai.ModelResponse) []ChatToolRequest {
	if resp == nil || resp.Message == nil {
		return nil
	}

	var requests []ChatToolRequest
	toolReqs := resp.ToolRequests()

	for _, tr := range toolReqs {
		args, _ := ConvertToolCallToArgs(tr.Input)
		requests = append(requests, ChatToolRequest{
			ID:        tr.Ref,
			Name:      ChatToolName(tr.Name),
			Arguments: args,
		})
	}

	return requests
}

// BuildToolResponseMessage creates a message with tool responses for the model
func BuildToolResponseMessage(responses []ChatToolResponse) *ai.Message {
	var parts []*ai.Part

	for _, resp := range responses {
		parts = append(parts, ai.NewToolResponsePart(&ai.ToolResponse{
			Ref:  resp.ID,
			Name: resp.Name,
			Output: map[string]any{
				"content": resp.Content,
			},
		}))
	}

	return &ai.Message{
		Role:    ai.RoleTool,
		Content: parts,
	}
}

// ChatGenerateOptions contains options for chat generation with tools
type ChatGenerateOptions struct {
	Messages       []*ai.Message
	SystemPrompt   string
	Tools          []ai.Tool
	StreamCallback func(ctx context.Context, chunk *ai.ModelResponseChunk) error
	// MaxToolIterations limits how many tool calling rounds can occur (default: 5)
	MaxToolIterations int
	// ReturnToolRequests if true, returns tool requests without executing them
	ReturnToolRequests bool
}
