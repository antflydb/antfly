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
	"crypto/sha256"
	"fmt"
	"maps"
	"unicode/utf8"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/cespare/xxhash/v2"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"go.uber.org/zap"
)

type GenKitModelImpl struct {
	Genkit          *genkit.Genkit
	Model           ai.Model
	logger          *zap.Logger
	maxOutputTokens int // If >0, passed as max_tokens config to the model provider
}

// SummarizeInput defines the input structure for summarization prompts
type SummarizeInput struct {
	TextContent   []string `json:"text_content,omitempty"`
	MediaContent  []string `json:"media_content,omitempty"`
	SemanticQuery string   `json:"semantic_query,omitempty"`
}

// SummarizeOutput defines the output structure for summarization
type SummarizeOutput struct {
	Summary string `json:"summary"`
}

// SummarizeDocumentsInput defines the input structure
type SummarizeDocumentsInput struct {
	Documents                  []schema.Document                   `json:"documents"`
	SemanticSearch             string                              `json:"semantic_search,omitempty"`
	ConfidenceContext          string                              `json:"confidence_context,omitempty"`
	Reasoning                  string                              `json:"reasoning,omitempty"`
	GenerationContext          string                              `json:"generation_context,omitempty"`
	FollowupContext            string                              `json:"followup_context,omitempty"`
	ClassificationResult       *ClassificationTransformationResult `json:"classification_result,omitempty"`
	ResourceSubQuestionMapping []ResourceMapping                   `json:"resource_sub_question_mapping,omitempty"`
	AgentKnowledge             string                              `json:"agent_knowledge,omitempty"`
}

// ResourceMapping represents a resource-to-subquestion mapping for template rendering.
// Used to show which resources answer which sub-questions in decompose strategy.
type ResourceMapping struct {
	ResourceID   string   `json:"resource_id"   handlebars:"resource_id"`
	SubQuestions []string `json:"sub_questions" handlebars:"sub_questions"`
}

// splitValidUTF8 splits the input string into a valid UTF-8 prefix and remaining bytes.
// This ensures we don't emit incomplete UTF-8 sequences during streaming.
// Returns (validPrefix, remainingSuffix)
func splitValidUTF8(s string) (string, string) {
	if s == "" {
		return "", ""
	}

	// If the entire string is valid UTF-8, return it all
	if utf8.ValidString(s) {
		return s, ""
	}

	// Walk through the string to find where it becomes invalid
	// We'll return everything up to the last valid rune
	validLen := 0
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError {
			// Check if this is actually an error or just a small remaining chunk
			if size == 1 {
				// This is either an invalid byte or an incomplete sequence
				// Return everything we've validated so far
				return s[:validLen], s[validLen:]
			}
		}
		validLen = i + size
		i += size
	}

	// We validated the whole string
	return s[:validLen], s[validLen:]
}

var (
	_ RetrievalAugmentedGenerator = (*GenKitModelImpl)(nil)
	_ DocumentSummarizer          = (*GenKitModelImpl)(nil)
)

// GenKitOption configures a GenKitModelImpl instance.
type GenKitOption func(*GenKitModelImpl)

// WithMaxOutputTokens sets the maximum output tokens passed to the model provider.
func WithMaxOutputTokens(n int) GenKitOption {
	return func(g *GenKitModelImpl) {
		g.maxOutputTokens = n
	}
}

func NewGenKitSummarizer(genkitInstance *genkit.Genkit, model ai.Model, opts ...GenKitOption) *GenKitModelImpl {
	// Define static partials for RAG prompts
	// These are reusable across all RAG calls and reduce prompt duplication

	// documentIds: Header listing valid document IDs
	genkit.DefinePartial(genkitInstance, "documentIds",
		`VALID DOCUMENT IDs (you MUST use these exact IDs): {{#each documents}}{{this.id}}{{#unless @last}}, {{/unless}}{{/each}}`)

	// citationRules: Instructions for inline document references
	genkit.DefinePartial(genkitInstance, "citationRules",
		`Citation format: [resource_id ID] or [resource_id ID1, ID2] - "resource_id " appears ONCE, then comma-separated IDs
   CORRECT: [resource_id doc_abc123] or [resource_id doc_abc123, doc_def456]
   INCORRECT: [resource_id doc_abc123, resource_id doc_def456] ❌ (don't repeat "resource_id")`)

	// ragHeader: Conditional header for Q&A vs summary mode
	genkit.DefinePartial(genkitInstance, "ragHeader",
		`{{#if semantic_search}}
=== USER'S QUESTION ===
"{{semantic_search}}"

IMPORTANT: Your primary task is to ANSWER this question using information from the documents below.
Directly address the user's question - do not just provide a general summary.
{{else}}
Your task is to provide a comprehensive summary of the documents below.
{{/if}}`)

	// ragInstructions: Conditional instructions for Q&A vs summary mode
	genkit.DefinePartial(genkitInstance, "ragInstructions",
		`Instructions:
{{#if semantic_search}}
1. Write a direct answer in markdown format to the user's question: "{{semantic_search}}"
{{else}}
1. Write a comprehensive summary in markdown format of the key information from all documents
{{/if}}
2. Use markdown formatting (headings, bullets, bold/italic, code blocks) to structure your response
3. {{>citationRules}}`)

	// documentIdReminder: Final reminder to use exact document IDs
	genkit.DefinePartial(genkitInstance, "documentIdReminder",
		`CRITICAL: Use these EXACT document IDs with "resource_id " prefix: {{#each documents}}{{this.id}}{{#unless @last}}, {{/unless}}{{/each}}`)

	impl := &GenKitModelImpl{
		Genkit: genkitInstance,
		Model:  model,
		logger: zap.L().Named("ai.GenKit"),
	}
	for _, opt := range opts {
		opt(impl)
	}
	return impl
}

func (g *GenKitModelImpl) SummarizeRenderedDocs(
	ctx context.Context,
	renderedDocs []string,
	opts ...GenerateOption,
) ([]string, error) {
	if len(renderedDocs) == 0 {
		return []string{}, nil
	}

	// Apply options
	options := CollectGenerateOptions(opts...)

	response := make([]string, 0, len(renderedDocs))

	// Determine prompt template
	var promptTemplate string
	if options.GetPrompt() != "" {
		// Use custom prompt
		promptTemplate = options.GetPrompt()
	} else {
		// Use default template
		promptTemplate = `{{this}}

Provide a concise summary of the content above.`
	}

	// Determine system prompt
	systemPrompt := "You are an expert content analyst. Generate clear, concise summaries of the provided content."
	if options.GetSystemPrompt() != "" {
		systemPrompt = options.GetSystemPrompt()
	}

	// Generate a unique prompt name based on template hash
	hash := sha256.Sum256([]byte(promptTemplate + systemPrompt))
	promptName := fmt.Sprintf("summarize-rendered-%x", hash[:8])

	// Check if prompt already exists, if not define it
	summarizePrompt := genkit.LookupPrompt(g.Genkit, promptName)
	if summarizePrompt == nil {
		// Define the summarization prompt
		summarizePrompt = genkit.DefinePrompt(g.Genkit, promptName,
			ai.WithDescription("Generates a concise summary of pre-rendered document content."),
			ai.WithModel(g.Model),
			ai.WithSystem(systemPrompt),
			ai.WithPrompt(promptTemplate),
		)
	}

	// Process each rendered document
	for _, renderedDoc := range renderedDocs {
		// Wrap string in a map for GenKit template processing
		input := map[string]any{
			"this": renderedDoc,
		}
		resp, err := summarizePrompt.Execute(ctx, ai.WithInput(input))
		if err != nil {
			return nil, fmt.Errorf("executing prompt: %w", err)
		}
		response = append(response, resp.Text())
	}

	return response, nil
}

// RAG generates markdown with inline document references from documents
// This is the core RAG implementation that supports customization via RAGOptions
func (g *GenKitModelImpl) RAG(
	ctx context.Context,
	documents []schema.Document,
	opts ...RAGOption,
) (string, *GenerationUsage, error) {
	// Allow empty documents - LLM will generate appropriate response

	// Apply options
	options := CollectRAGOptions(opts...)

	// Convert documents to structured format for the prompt
	// Pass raw fields to GenKit's handlebars engine for rendering
	docContents := []schema.Document{}
	for _, doc := range documents {
		// Filter out internal fields like _embeddings
		fields := make(map[string]any)
		for key, value := range doc.Fields {
			if key == "_embeddings" {
				continue
			}
			fields[key] = value
		}

		docContents = append(docContents, schema.Document{
			ID:     doc.ID,
			Fields: fields,
		})
	}

	// Use system prompt or default
	var systemPrompt string
	if options.GetSystemPrompt() != "" {
		systemPrompt = options.GetSystemPrompt() + SystemPromptV1CustomSuffix
	} else {
		systemPrompt = SystemPromptV1
	}

	// Check if custom user prompt template is provided
	var userPromptText string
	if options.GetUserPromptTemplate() != "" {
		// Use custom template directly
		userPromptText = options.GetUserPromptTemplate()
	} else {
		// Get document renderer (default or custom)
		documentTemplate := options.GetDocumentRenderer()
		if documentTemplate == "" {
			documentTemplate = DefaultDocumentRenderer
		}

		// Build prompt using partials for static text and fmt.Sprintf for variable documentTemplate
		// Document wrapper format matches DefaultDocumentWrapper for consistency with token pruner
		userPromptText = fmt.Sprintf(`{{#if documents}}
{{>documentIds documents=documents}}

{{>ragHeader semantic_search=semantic_search}}

Below are documents to analyze:

{{#each documents}}
==================================================
Document ID: {{this.id}}
Content:
%s
==================================================
{{/each}}

{{>ragInstructions semantic_search=semantic_search}}

{{>documentIdReminder documents=documents}}
{{else}}
{{>ragHeader semantic_search=semantic_search}}

NO DOCUMENTS FOUND: The search did not return any relevant documents for this query.

YOUR TASK:
1. Acknowledge that no relevant information was found in the knowledge base
2. Be helpful by suggesting what the user might try:
   - Different search terms or phrasings
   - Breaking down complex questions into simpler ones
   - Checking if the topic is covered in the knowledge base
3. Do NOT make up information or answer from general knowledge
4. Keep your response concise and helpful
{{/if}}
`, documentTemplate)
	}

	// Generate a unique prompt name based on the system prompt, user prompt, and document count
	promptHash := xxhash.New()
	_, _ = promptHash.WriteString(systemPrompt)
	_, _ = promptHash.WriteString(userPromptText)
	_, _ = fmt.Fprintf(promptHash, "docs:%d", len(docContents))
	for _, doc := range docContents {
		_, _ = promptHash.WriteString(doc.ID)
	}
	promptName := fmt.Sprintf("answer-events-%x", promptHash.Sum64())

	// Check if prompt already exists, if not define it
	answerPrompt := genkit.LookupPrompt(g.Genkit, promptName)
	if answerPrompt == nil {
		answerPrompt = genkit.DefinePrompt(g.Genkit, promptName,
			ai.WithDescription("Generates a markdown summary with inline document references."),
			ai.WithModel(g.Model),
			ai.WithSystem(systemPrompt),
			ai.WithPrompt(userPromptText),
			ai.WithInputType(SummarizeDocumentsInput{}),
		)
	}

	// Build base input
	baseInput := map[string]any{
		"documents":       docContents,
		"semantic_search": options.GetSemanticQuery(),
	}

	// Merge in custom template variables if provided
	if templateVars := options.GetTemplateVariables(); templateVars != nil {
		maps.Copy(baseInput, templateVars)
	}

	// Build prompt execution options
	execOpts := []ai.PromptExecuteOption{
		ai.WithInput(baseInput),
	}

	// Pass max output tokens if configured on the generator
	if g.maxOutputTokens > 0 {
		execOpts = append(execOpts, ai.WithConfig(map[string]any{
			"max_tokens": g.maxOutputTokens,
		}))
	}

	// Add conversation history for multi-turn chat
	if msgs := options.GetMessages(); len(msgs) > 0 {
		execOpts = append(execOpts, ai.WithMessages(msgs...))
	}

	// Add streaming callback if provided
	streamChunkCount := 0
	if streamCallback := options.GetStreamCallback(); streamCallback != nil {
		execOpts = append(
			execOpts,
			ai.WithStreaming(func(ctx context.Context, chunk *ai.ModelResponseChunk) error {
				streamChunkCount++

				if len(chunk.Content) > 0 {
					for _, part := range chunk.Content {
						if part.Text != "" {
							// Stream markdown generation chunks directly
							if err := streamCallback(ctx, "generation", part.Text); err != nil {
								return fmt.Errorf("streaming callback error: %w", err)
							}
						}
					}
				}
				return nil
			}),
		)
	}

	resp, err := answerPrompt.Execute(ctx, execOpts...)
	if err != nil {
		return "", nil, fmt.Errorf("executing prompt: %w", err)
	}

	// Capture token usage from model response
	var usage *GenerationUsage
	if resp.Usage != nil {
		usage = &GenerationUsage{
			InputTokens:  resp.Usage.InputTokens,
			OutputTokens: resp.Usage.OutputTokens,
			TotalTokens:  resp.Usage.TotalTokens,
			CachedTokens: resp.Usage.CachedContentTokens,
		}
	}

	respText := resp.Text()
	if respText == "" {
		g.logger.Warn("RAG received empty response from model")
		return "", usage, fmt.Errorf("model returned empty response")
	}
	// Return markdown summary with inline references
	return respText, usage, nil
}
