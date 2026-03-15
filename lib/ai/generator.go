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
	"maps"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/firebase/genkit/go/ai"
)

// GenerationUsage tracks token usage from a single LLM call.
type GenerationUsage struct {
	InputTokens  int `json:"input_tokens,omitempty"`
	OutputTokens int `json:"output_tokens,omitempty"`
	TotalTokens  int `json:"total_tokens,omitempty"`
	CachedTokens int `json:"cached_tokens,omitempty"`
}

// GenerateOption is a functional option for customizing document summarization behavior
type GenerateOption interface {
	applyGenerateOption(*GenerateOptions)
}

type GenerateOptions struct {
	systemPrompt string // Custom system prompt for the LLM
	prompt       string // Custom user prompt using {{this}} to reference rendered doc
}

func CollectGenerateOptions(opts ...GenerateOption) GenerateOptions {
	var options GenerateOptions
	for _, opt := range opts {
		opt.applyGenerateOption(&options)
	}
	return options
}

func (s *GenerateOptions) GetSystemPrompt() string {
	return s.systemPrompt
}

func (s *GenerateOptions) GetPrompt() string {
	return s.prompt
}

type generateSystemPromptOption struct {
	prompt string
}

func (s generateSystemPromptOption) applyGenerateOption(opts *GenerateOptions) {
	opts.systemPrompt = s.prompt
}

// WithGenerateSystemPrompt configures a custom system prompt for summarization
func WithGenerateSystemPrompt(prompt string) GenerateOption {
	return generateSystemPromptOption{prompt: prompt}
}

type generatePromptOption struct {
	prompt string
}

func (p generatePromptOption) applyGenerateOption(opts *GenerateOptions) {
	opts.prompt = p.prompt
}

// WithGeneratePrompt configures a custom prompt template for document summarization
// The template should use {{this}} to reference the pre-rendered document string
// Example: "Summarize the following:\n\n{{this}}\n\nProvide a concise summary."
func WithGeneratePrompt(prompt string) GenerateOption {
	return generatePromptOption{prompt: prompt}
}

// RAGOption is a functional option for customizing RAG behavior
type RAGOption interface {
	applyRAGOption(*RAGOptions)
}

type RAGOptions struct {
	streamCallback     func(ctx context.Context, eventType string, data any) error // For RAG() streaming
	systemPrompt       string                                                      // Custom system prompt for the LLM
	documentRenderer   string                                                      // Handlebars template string for rendering document content (for RAG())
	semanticQuery      string                                                      // User's semantic search query (for RAG())
	userPromptTemplate string                                                      // Custom user prompt template for RAG() - overrides default prompt structure
	templateVariables  map[string]any                                              // Additional variables to pass to the template (for custom prompts)
	messages           []*ai.Message                                               // Conversation history for multi-turn chat
}

func CollectRAGOptions(opts ...RAGOption) RAGOptions {
	var options RAGOptions
	for _, opt := range opts {
		opt.applyRAGOption(&options)
	}
	return options
}

func (s *RAGOptions) GetStreamCallback() func(ctx context.Context, eventType string, data any) error {
	return s.streamCallback
}

func (s *RAGOptions) GetSystemPrompt() string {
	return s.systemPrompt
}

func (s *RAGOptions) GetDocumentRenderer() string {
	return s.documentRenderer
}

func (s *RAGOptions) GetSemanticQuery() string {
	return s.semanticQuery
}

func (s *RAGOptions) GetUserPromptTemplate() string {
	return s.userPromptTemplate
}

func (s *RAGOptions) GetTemplateVariables() map[string]any {
	return s.templateVariables
}

func (s *RAGOptions) GetMessages() []*ai.Message {
	return s.messages
}

type ragMessagesOption struct {
	messages []*ai.Message
}

func (m ragMessagesOption) applyRAGOption(opts *RAGOptions) {
	opts.messages = m.messages
}

// WithRAGMessages provides conversation history for multi-turn RAG.
// Messages are inserted between the system prompt and the current user message.
func WithRAGMessages(messages []*ai.Message) RAGOption {
	return ragMessagesOption{messages: messages}
}

type systemPromptOption struct {
	prompt string
}

func (s systemPromptOption) applyRAGOption(opts *RAGOptions) {
	opts.systemPrompt = s.prompt
}

// WithSystemPrompt configures the RAG to use a custom system prompt
func WithSystemPrompt(prompt string) RAGOption {
	return systemPromptOption{prompt: prompt}
}

type streamCallbackOption struct {
	callback func(ctx context.Context, eventType string, data any) error
}

func (s streamCallbackOption) applyRAGOption(opts *RAGOptions) {
	opts.streamCallback = s.callback
}

// WithStreaming configures the RAG function to stream partial results via the provided callback
// The callback receives eventType ("generation", etc.) and data
// Note: This option is only used by RAG(), not by DocumentSummarizer
func WithStreaming(callback func(ctx context.Context, eventType string, data any) error) RAGOption {
	return streamCallbackOption{callback: callback}
}

type documentRendererOption struct {
	template string
}

func (d documentRendererOption) applyRAGOption(opts *RAGOptions) {
	opts.documentRenderer = d.template
}

// WithDocumentRenderer configures a custom Handlebars template for rendering document content
// The template is injected directly into the GenKit prompt and has access to document fields via {{this.fields}}
// Example template: "Title: {{this.fields.title}}\nBody: {{this.fields.body}}"
// Note: This option is only used by RAG(), not by DocumentSummarizer
func WithDocumentRenderer(template string) RAGOption {
	return documentRendererOption{template: template}
}

type semanticQueryOption struct {
	query string
}

func (s semanticQueryOption) applyRAGOption(opts *RAGOptions) {
	opts.semanticQuery = s.query
}

// WithSemanticQuery configures the RAG function to use the user's semantic search query
// The query may be empty if not provided by the user
// Note: This option is only used by RAG(), not by DocumentSummarizer
func WithSemanticQuery(query string) RAGOption {
	return semanticQueryOption{query: query}
}

type userPromptTemplateOption struct {
	prompt string
}

func (u userPromptTemplateOption) applyRAGOption(opts *RAGOptions) {
	opts.userPromptTemplate = u.prompt
}

// WithPromptTemplate configures a custom prompt template for RAG()
// This allows complete control over the prompt sent to the LLM, overriding the default structure.
// The template can use Handlebars syntax and has access to:
// - {{documents}}: Array of documents with .id and .fields
// - {{semantic_search}}: The user's search query (if provided)
// - {{reasoning_context}}, {{generation_context}}, {{followup_context}}: Custom instructions (if provided)
// You can use Handlebars helpers like loops and conditionals.
// To generate a comma-separated list of document IDs: {{#each documents}}{{this.id}}{{#unless @last}}, {{/unless}}{{/each}}
// Example: "Based on these documents:\n{{#each documents}}\nDoc {{this.id}}: {{this.fields}}\n{{/each}}\n\nAnswer the question."
// Note: This option is only used by RAG(), not by DocumentSummarizer
func WithPromptTemplate(prompt string) RAGOption {
	return userPromptTemplateOption{prompt: prompt}
}

type templateVariablesOption struct {
	variables map[string]any
}

func (t templateVariablesOption) applyRAGOption(opts *RAGOptions) {
	if opts.templateVariables == nil {
		opts.templateVariables = make(map[string]any)
	}
	maps.Copy(opts.templateVariables, t.variables)
}

// WithPromptTemplateVariables adds custom variables to pass to the template
// These variables can be accessed in the template using Handlebars syntax (e.g., {{variable_name}})
// This is useful for custom prompts that need additional dynamic values beyond documents and semantic_search
func WithPromptTemplateVariables(variables map[string]any) RAGOption {
	return templateVariablesOption{variables: variables}
}

// RetrievalAugmentedGenerator extends DocumentSummarizer to support RAG with inline document references
type RetrievalAugmentedGenerator interface {
	// RAG returns markdown with inline document references like [resource_id doc1] or [resource_id doc1, doc2].
	// Also returns token usage from the underlying model, if available.
	RAG(ctx context.Context, contents []schema.Document, opts ...RAGOption) (string, *GenerationUsage, error)
}
