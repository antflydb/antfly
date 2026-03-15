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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"go.uber.org/zap"
)

// Streaming buffer constants
const (
	// streamLookaheadSize is the number of bytes to keep in buffer for detecting section headers
	// while streaming. This prevents cutting off content right before a header appears.
	streamLookaheadSize = 50

	// streamMinBufferSize is the minimum buffer size before we start streaming content.
	// This ensures we have enough context to detect section boundaries.
	streamMinBufferSize = 100

	// defaultConfidenceScore is the fallback confidence value when parsing fails
	defaultConfidenceScore = 0.5
)

// modelEndTokens contains model-specific end-of-turn tokens that should be stripped from output.
// These tokens are used internally by various LLMs to signal the end of their response,
// but should not appear in the final user-facing output.
var modelEndTokens = []string{
	"</end_of_turn>", // Gemini
	"<end_of_turn>",  // Gemini (alternate)
	"<|endoftext|>",  // GPT/OpenAI style
	"<|im_end|>",     // ChatML style
	"<|eot_id|>",     // Llama 3 style
}

// Default prompt context strings
const (
	defaultConfidenceContext = "Assess your confidence in answering this question. Consider: (1) how well the resources address the question, (2) your general ability to answer reliably. 'generation_confidence' is your overall confidence (0.0-1.0), 'context_relevance' is how relevant the resources are to the question (0.0-1.0)."
	defaultFollowupContext   = "3 related topics as search queries (e.g., 'How X works', 'Y examples'). NOT user questions."
)

// GenerationOutput is the final result from GenerateQueryResponse
type GenerationOutput struct {
	Classification       RouteType        `json:"classification"`
	GenerationConfidence float32          `json:"generation_confidence,omitempty"` // Overall generation confidence (0.0-1.0)
	ContextRelevance     float32          `json:"context_relevance,omitempty"`     // Resource relevance to question (0.0-1.0)
	Generation           string           `json:"generation,omitempty"`            // Generated answer
	FollowupQuestions    []string         `json:"followup_questions,omitempty"`    // Suggested follow-up questions
	Usage                *GenerationUsage `json:"usage,omitempty"`                 // Token usage from LLM call
}

// GenerationOption is a functional option for customizing answer behavior
type GenerationOption interface {
	applyGenerationOption(*generationOptions)
}

type generationOptions struct {
	streamCallback              func(ctx context.Context, eventType string, data any) error
	systemPrompt                string
	withFollowup                bool
	withConfidence              bool
	withClassificationReasoning bool
	semanticQuery               string
	generationContext           string
	followupContext             string
	confidenceContext           string
	reasoning                   string
	classificationResult        *ClassificationTransformationResult
	resourceSubQuestionMap      map[string][]int // Maps resource ID to sub-question indices
	agentKnowledge              string           // Domain-specific background knowledge
	tools                       []ai.Tool        // Tools available for chat agent
	maxToolIterations           int              // Max iterations for tool loop (default 5)
	messages                    []*ai.Message    // Conversation history for multi-turn chat
}

type generationStreamCallbackOption struct {
	callback func(ctx context.Context, eventType string, data any) error
}

func (a generationStreamCallbackOption) applyGenerationOption(opts *generationOptions) {
	opts.streamCallback = a.callback
}

// WithGenerationStreaming configures the answer function to stream partial results via the provided callback
// The callback receives eventType ("classification", "keywords", "generation") and corresponding data
func WithGenerationStreaming(
	callback func(ctx context.Context, eventType string, data any) error,
) GenerationOption {
	return generationStreamCallbackOption{callback: callback}
}

type generationSystemPromptOption struct {
	prompt string
}

func (a generationSystemPromptOption) applyGenerationOption(opts *generationOptions) {
	opts.systemPrompt = a.prompt
}

// WithGenerationSystemPrompt configures a custom system prompt for classification
func WithGenerationSystemPrompt(prompt string) GenerationOption {
	return generationSystemPromptOption{prompt: prompt}
}

type generationStreamFollowupQuestionsOption struct {
	enabled bool
}

func (a generationStreamFollowupQuestionsOption) applyGenerationOption(opts *generationOptions) {
	opts.withFollowup = a.enabled
}

// WithFollowup enables streaming of follow-up question suggestions
func WithFollowup(enabled bool) GenerationOption {
	return generationStreamFollowupQuestionsOption{enabled: enabled}
}

type generationSemanticQueryOption struct {
	query string
}

func (a generationSemanticQueryOption) applyGenerationOption(opts *generationOptions) {
	opts.semanticQuery = a.query
}

// WithGenerationSemanticQuery sets the semantic query for the answer
func WithGenerationSemanticQuery(query string) GenerationOption {
	return generationSemanticQueryOption{query: query}
}

type generationContextOption struct {
	context string
}

func (a generationContextOption) applyGenerationOption(opts *generationOptions) {
	opts.generationContext = a.context
}

// WithGenerationContext adds custom instructions for the answer section
func WithGenerationContext(context string) GenerationOption {
	return generationContextOption{context: context}
}

type followupContextOption struct {
	context string
}

func (f followupContextOption) applyGenerationOption(opts *generationOptions) {
	opts.followupContext = f.context
}

// WithFollowupContext adds custom instructions for the follow-up questions section
func WithFollowupContext(context string) GenerationOption {
	return followupContextOption{context: context}
}

type generationConfidenceOption struct {
	enabled bool
}

func (a generationConfidenceOption) applyGenerationOption(opts *generationOptions) {
	opts.withConfidence = a.enabled
}

// WithConfidence enables confidence scoring for the answer
func WithConfidence(enabled bool) GenerationOption {
	return generationConfidenceOption{enabled: enabled}
}

type confidenceContextOption struct {
	context string
}

func (c confidenceContextOption) applyGenerationOption(opts *generationOptions) {
	opts.confidenceContext = c.context
}

// WithConfidenceContext adds custom instructions for the confidence section
func WithConfidenceContext(context string) GenerationOption {
	return confidenceContextOption{context: context}
}

type classificationReasoningOption struct {
	enabled bool
}

func (c classificationReasoningOption) applyGenerationOption(opts *generationOptions) {
	opts.withClassificationReasoning = c.enabled
}

// WithClassificationReasoning enables pre-retrieval reasoning in the classification result
func WithClassificationReasoning(enabled bool) GenerationOption {
	return classificationReasoningOption{enabled: enabled}
}

type reasoningOption struct {
	reasoning string
}

func (p reasoningOption) applyGenerationOption(opts *generationOptions) {
	opts.reasoning = p.reasoning
}

// WithReasoning passes reasoning from classification to the answer phase
func WithReasoning(reasoning string) GenerationOption {
	return reasoningOption{reasoning: reasoning}
}

type classificationResultOption struct {
	result *ClassificationTransformationResult
}

func (c classificationResultOption) applyGenerationOption(opts *generationOptions) {
	opts.classificationResult = c.result
}

// WithClassificationResult passes the full classification result to the answer phase,
// providing context about the query strategy, sub-questions, and other transformation details.
func WithClassificationResult(result *ClassificationTransformationResult) GenerationOption {
	return classificationResultOption{result: result}
}

type resourceSubQuestionMapOption struct {
	mapping map[string][]int
}

func (r resourceSubQuestionMapOption) applyGenerationOption(opts *generationOptions) {
	opts.resourceSubQuestionMap = r.mapping
}

// WithResourceSubQuestionMap passes a mapping from resource IDs to sub-question indices,
// indicating which sub-questions each resource was retrieved for in multiquery mode.
func WithResourceSubQuestionMap(mapping map[string][]int) GenerationOption {
	return resourceSubQuestionMapOption{mapping: mapping}
}

type agentKnowledgeOption struct {
	knowledge string
}

func (a agentKnowledgeOption) applyGenerationOption(opts *generationOptions) {
	opts.agentKnowledge = a.knowledge
}

// WithAgentKnowledge provides domain-specific background knowledge that guides
// the agent's understanding. This context is passed to both classification
// and answer generation steps, similar to CLAUDE.md context files.
func WithAgentKnowledge(knowledge string) GenerationOption {
	return agentKnowledgeOption{knowledge: knowledge}
}

type toolsOption struct {
	tools []ai.Tool
}

func (t toolsOption) applyGenerationOption(opts *generationOptions) {
	opts.tools = t.tools
}

// WithTools provides tools for the chat agent to use during generation.
// The generation will execute a tool loop, calling tools as needed.
func WithTools(tools []ai.Tool) GenerationOption {
	return toolsOption{tools: tools}
}

type maxToolIterationsOption struct {
	maxIterations int
}

func (m maxToolIterationsOption) applyGenerationOption(opts *generationOptions) {
	opts.maxToolIterations = m.maxIterations
}

// WithMaxToolIterations sets the maximum number of tool iterations (default 5)
func WithMaxToolIterations(max int) GenerationOption {
	return maxToolIterationsOption{maxIterations: max}
}

type messagesOption struct {
	messages []*ai.Message
}

func (m messagesOption) applyGenerationOption(opts *generationOptions) {
	opts.messages = m.messages
}

// WithMessages provides conversation history for multi-turn chat.
// Messages are passed through to the GenKit prompt execution as context
// between the system prompt and the current user message.
func WithMessages(messages []*ai.Message) GenerationOption {
	return messagesOption{messages: messages}
}

// addClassificationContextToTemplateVars adds classification context to template variables
// for multiquery awareness in the answer generation phase.
func addClassificationContextToTemplateVars(templateVars map[string]any, result *ClassificationTransformationResult) {
	if result == nil {
		return
	}

	// Pass the entire classification result for nested template access
	templateVars["classification_result"] = result
}

// addResourceSubQuestionMapping adds resource-to-subquestion mapping to template variables.
// This creates a formatted list showing which resources answer which sub-questions.
func addResourceSubQuestionMapping(templateVars map[string]any, resourceSubQMap map[string][]int, subQuestions []string) {
	if len(resourceSubQMap) == 0 || len(subQuestions) == 0 {
		return
	}

	mappings := make([]ResourceMapping, 0, len(resourceSubQMap))
	for resourceID, subQIndices := range resourceSubQMap {
		sqNames := make([]string, 0, len(subQIndices))
		for _, idx := range subQIndices {
			if idx >= 0 && idx < len(subQuestions) {
				sqNames = append(sqNames, subQuestions[idx])
			}
		}
		if len(sqNames) > 0 {
			mappings = append(mappings, ResourceMapping{
				ResourceID:   resourceID,
				SubQuestions: sqNames,
			})
		}
	}

	if len(mappings) > 0 {
		templateVars["resource_sub_question_mapping"] = mappings
	}
}

// buildBaseTemplateVars creates the common template variables shared by both
// structured and non-structured generation paths: generation_context, reasoning,
// classification context, and agent_knowledge.
func buildBaseTemplateVars(options *generationOptions) map[string]any {
	templateVars := make(map[string]any)

	generationCtx := options.generationContext
	if generationCtx == "" {
		generationCtx = fmt.Sprintf("Provide a comprehensive answer to the question %q.", options.semanticQuery)
	}
	templateVars["generation_context"] = generationCtx

	if options.reasoning != "" {
		templateVars["reasoning"] = options.reasoning
	}

	if options.classificationResult != nil {
		addClassificationContextToTemplateVars(templateVars, options.classificationResult)

		if len(options.resourceSubQuestionMap) > 0 && len(options.classificationResult.SubQuestions) > 0 {
			addResourceSubQuestionMapping(templateVars, options.resourceSubQuestionMap, options.classificationResult.SubQuestions)
		}
	}

	if options.agentKnowledge != "" {
		templateVars["agent_knowledge"] = options.agentKnowledge
	}

	return templateVars
}

// Generate generates an answer from documents with optional reasoning and follow-up questions
func (g *GenKitModelImpl) Generate(
	ctx context.Context,
	query string,
	documents []schema.Document,
	opts ...GenerationOption,
) (*GenerationResult, *GenerationUsage, error) {
	// Allow empty documents - LLM will generate appropriate response
	// (e.g., "I couldn't find relevant information to answer your question")

	// Apply options
	options := &generationOptions{}
	for _, opt := range opts {
		opt.applyGenerationOption(options)
	}

	// Check if we need structured output with confidence/follow-up
	needsStructured := options.withConfidence || options.withFollowup

	// Build RAG options
	ragOpts := []RAGOption{}
	if options.systemPrompt != "" {
		ragOpts = append(ragOpts, WithSystemPrompt(options.systemPrompt))
	}
	if options.semanticQuery != "" {
		ragOpts = append(ragOpts, WithSemanticQuery(options.semanticQuery))
	}
	if len(options.messages) > 0 {
		ragOpts = append(ragOpts, WithRAGMessages(options.messages))
	}

	if needsStructured {
		// Create custom prompt template for structured output with markdown sections
		ragOpts = append(ragOpts, WithPromptTemplate(StructuredGenerationPromptTemplate))

		// Build base template vars and add structured-output-specific sections
		templateVars := buildBaseTemplateVars(options)
		if options.withConfidence {
			confidenceCtx := options.confidenceContext
			if confidenceCtx == "" {
				confidenceCtx = defaultConfidenceContext
			}
			templateVars["confidence_context"] = confidenceCtx
		}
		if options.withFollowup {
			followupCtx := options.followupContext
			if followupCtx == "" {
				followupCtx = defaultFollowupContext
			}
			templateVars["followup_context"] = followupCtx
		}

		ragOpts = append(ragOpts, WithPromptTemplateVariables(templateVars))

		// Wrap streaming callback to parse markdown sections
		var parser *markdownSectionParser
		if options.streamCallback != nil {
			parser = newMarkdownSectionParser(
				options.streamCallback,
				options.withConfidence,
				false, // withReasoning - no longer supported in generation step
				options.withFollowup,
			)
			ragOpts = append(ragOpts, WithStreaming(parser.parseChunk))
		}

		// Generate with RAG
		fullResponse, usage, err := g.RAG(ctx, documents, ragOpts...)
		if err != nil {
			g.logger.Error("RAG call failed in structured path", zap.Error(err))
			return nil, nil, err
		}

		// Flush any remaining buffered content from streaming parser
		if parser != nil {
			parser.flush(ctx)
		}

		// Parse final response for structured fields
		result := parseStructuredResponse(
			fullResponse,
			options.withConfidence,
			false, // withReasoning - no longer supported in generation step
			options.withFollowup,
		)

		// Emit followup questions as individual streaming events (if streaming is enabled)
		if options.streamCallback != nil && options.withFollowup && len(result.FollowupQuestions) > 0 {
			for _, question := range result.FollowupQuestions {
				if err := options.streamCallback(ctx, "followup", question); err != nil {
					g.logger.Warn("Failed to stream followup question", zap.Error(err))
				}
			}
		}

		return result, usage, nil
	}

	// Use structured prompt template with only Generation section
	ragOpts = append(ragOpts, WithPromptTemplate(StructuredGenerationPromptTemplate))

	// Build base template vars (no confidence, reasoning, or followup sections)
	templateVars := buildBaseTemplateVars(options)

	ragOpts = append(ragOpts, WithPromptTemplateVariables(templateVars))

	// Wrap callback with markdown parser for consistent streaming
	var parser *markdownSectionParser
	if options.streamCallback != nil {
		parser = newMarkdownSectionParser(
			options.streamCallback,
			false, // withConfidence
			false, // withReasoning
			false, // withFollowup
		)
		ragOpts = append(ragOpts, WithStreaming(parser.parseChunk))
	}

	fullResponse, usage, err := g.RAG(ctx, documents, ragOpts...)
	if err != nil {
		return nil, nil, err
	}

	// Flush any remaining buffered content from streaming parser
	if parser != nil {
		parser.flush(ctx)
	}

	// Parse final response for answer
	return parseStructuredResponse(fullResponse, false, false, false), usage, nil
}

// headerLocation describes where a markdown header was found in a combined buffer
type headerLocation struct {
	// Index of the header in the current buffer (after accumulated content)
	idxInBuffer int
	// Content before the header that should be added to accumulated buffer
	beforeHeader string
	// Buffer starting at the header position
	remaining string
}

// findMarkdownHeaderInCombined searches for a markdown header across accumulated and current buffers.
// This handles the case where header text like "## Answer" arrives character-by-character across
// multiple streaming chunks, so we need to check both buffers together.
//
// Returns (headerLocation, found). If not found, the boolean is false.
// If found, headerLocation contains:
//   - idxInBuffer: always 0 (header starts at beginning of returned remaining buffer)
//   - beforeHeader: content that should be appended to accumulated buffer
//   - remaining: buffer content starting from the header
func findMarkdownHeaderInCombined(accumulated, buffer, headerName string) (headerLocation, bool) {
	combined := accumulated + buffer
	combinedIdx := findMarkdownHeader(combined, headerName)
	if combinedIdx < 0 {
		return headerLocation{}, false
	}

	// Header found - determine how to split the buffers
	if combinedIdx >= len(accumulated) {
		// Header is in the current buffer
		headerPosInBuffer := combinedIdx - len(accumulated)
		return headerLocation{
			idxInBuffer:  0,
			beforeHeader: buffer[:headerPosInBuffer],
			remaining:    buffer[headerPosInBuffer:],
		}, true
	}

	// Header is in the accumulated buffer (rare, but possible if accumulated wasn't checked yet)
	return headerLocation{
		idxInBuffer:  0,
		beforeHeader: "",
		remaining:    combined[combinedIdx:],
	}, true
}

// skipHeaderLine skips past a markdown header line (e.g., "## Answer\n").
// Returns the remaining buffer after the header line, and ok=true if successful.
// If the header line is incomplete (no newline found), returns ("", false).
func skipHeaderLine(buffer string) (remaining string, ok bool) {
	newlineIdx := findNextNewline(buffer)
	if newlineIdx < 0 {
		return "", false
	}
	return buffer[newlineIdx+1:], true
}

// splitAtHeader splits a buffer at the position of a markdown header.
// Returns (contentBeforeHeader, contentFromHeader, found).
// If the header is not found, found=false and the other values are empty.
func splitAtHeader(buffer, headerName string) (beforeHeader, afterHeader string, found bool) {
	idx := findMarkdownHeader(buffer, headerName)
	if idx < 0 {
		return "", "", false
	}
	return buffer[:idx], buffer[idx:], true
}

// streamContentWithLookahead streams content from a buffer while keeping a lookahead for header detection.
// This is used for reasoning and answer sections which stream incrementally.
//
// The function:
//  1. Checks if buffer is large enough to stream (> minStreamSize)
//  2. Splits content at UTF-8 boundary to avoid breaking multi-byte characters
//  3. Returns (streamable content, remaining buffer with lookahead)
//
// Parameters:
//   - buffer: current accumulated buffer
//   - lookaheadSize: bytes to keep in buffer for detecting next section header (typically 50)
//   - minStreamSize: minimum buffer size before streaming (typically 100)
//
// Returns (streamableContent, remainingBuffer, shouldStream).
// If buffer is too small, shouldStream=false and streamableContent="".
func streamContentWithLookahead(buffer string, lookaheadSize, minStreamSize int) (streamable, remaining string, shouldStream bool) {
	if len(buffer) <= minStreamSize {
		return "", buffer, false
	}

	// Calculate how much we can stream (everything except the lookahead)
	splitPoint := len(buffer) - lookaheadSize

	// Split at UTF-8 boundary to avoid breaking multi-byte characters
	validPrefix, _ := splitValidUTF8(buffer[:splitPoint])

	// Keep everything after the valid prefix (includes any incomplete UTF-8 bytes + lookahead)
	remaining = buffer[len(validPrefix):]

	return validPrefix, remaining, validPrefix != ""
}

// markdownSectionParser handles streaming and parsing of markdown sections
//
// Buffering Strategy:
//   - Confidence section: Fully buffered (needs complete content to parse confidence values)
//   - Reasoning section: Streamed with lookahead (reduces latency for long content)
//   - Answer section: Streamed with lookahead (reduces latency for long content)
//   - Follow-up section: Fully buffered (needs complete content to parse question list)
//
// Note: The asymmetric buffering (confidence/followup fully buffered vs reasoning/answer streamed)
// is maintained for backward compatibility with existing callback timing semantics.
type markdownSectionParser struct {
	callback             func(ctx context.Context, eventType string, data any) error
	withConfidence       bool
	withReasoning        bool
	withFollowup         bool
	buffer               string
	currentSection       string
	confidenceDone       bool
	confidenceBuffer     string
	reasoningDone        bool
	generationBuffer     string
	followUpBuffer       string
	pendingHeaderSkip    bool   // true when we've detected a section but haven't skipped its header line yet
	pendingHeaderSection string // the section whose header we're waiting to skip
}

// newMarkdownSectionParser creates a new markdown section parser
func newMarkdownSectionParser(
	callback func(ctx context.Context, eventType string, data any) error,
	withConfidence, withReasoning, withFollowup bool,
) *markdownSectionParser {
	return &markdownSectionParser{
		callback:       callback,
		withConfidence: withConfidence,
		withReasoning:  withReasoning,
		withFollowup:   withFollowup,
	}
}

// parseChunk processes a streaming chunk and routes it to the appropriate callback
func (p *markdownSectionParser) parseChunk(ctx context.Context, eventType string, data any) error {
	// Only process "generation" events (markdown chunks from RAG)
	if eventType != "generation" {
		return nil
	}

	text, ok := data.(string)
	if !ok {
		return nil
	}

	p.buffer += text

	// Process buffer to detect section headers and stream content
	for {
		sectionChanged := false

		// First, check if we have a pending header skip from a previous chunk
		// This handles the case where a section header was detected but the newline
		// hadn't arrived yet (e.g., "## Answer" without "\n")
		if p.pendingHeaderSkip {
			if remaining, ok := skipHeaderLine(p.buffer); ok {
				p.buffer = remaining
				p.pendingHeaderSkip = false
				p.pendingHeaderSection = ""
				sectionChanged = true
				continue
			} else {
				// Still waiting for newline, don't process anything else
				break
			}
		}

		// ALWAYS check for ALL section headers first to detect transitions
		// This prevents accumulating content past section boundaries

		// Check for Confidence section (comes first)
		if p.withConfidence && !p.confidenceDone {
			if confidenceIdx := findMarkdownHeader(p.buffer, "Confidence"); confidenceIdx >= 0 && p.currentSection == "" {
				p.currentSection = "confidence"
				p.buffer = p.buffer[confidenceIdx:]
				// Skip the header line
				if remaining, ok := skipHeaderLine(p.buffer); ok {
					p.buffer = remaining
					sectionChanged = true
					continue
				} else {
					// Header incomplete, mark pending and wait for more data
					p.pendingHeaderSkip = true
					p.pendingHeaderSection = "confidence"
					break
				}
			}
		}

		// Check for Reasoning section
		// When transitioning from confidence, we need to check the combined buffer
		// because the header might span across confidenceBuffer and p.buffer
		reasoningIdx := -1
		if p.withReasoning && !p.reasoningDone {
			if p.currentSection == "confidence" {
				// Use combined buffer to detect header that might span chunks
				if loc, found := findMarkdownHeaderInCombined(p.confidenceBuffer, p.buffer, "Reasoning"); found {
					p.confidenceBuffer += loc.beforeHeader
					p.buffer = loc.remaining
					reasoningIdx = 0
				}
			} else {
				reasoningIdx = findMarkdownHeader(p.buffer, "Reasoning")
			}

			if reasoningIdx >= 0 {
				// Transition from confidence to reasoning
				if err := p.transitionToReasoning(ctx, reasoningIdx); err != nil {
					return err
				}
				// Skip the header line
				if remaining, ok := skipHeaderLine(p.buffer); ok {
					p.buffer = remaining
					sectionChanged = true
					continue
				} else {
					// Header incomplete, mark pending and wait for more data
					p.pendingHeaderSkip = true
					p.pendingHeaderSection = "reasoning"
					break
				}
			}
		}

		// Check for Answer section
		// This can transition from either confidence or reasoning
		answerIdx := -1
		if p.currentSection == "confidence" && !p.confidenceDone {
			// Use combined buffer when transitioning from confidence
			if loc, found := findMarkdownHeaderInCombined(p.confidenceBuffer, p.buffer, "Generation"); found {
				p.confidenceBuffer += loc.beforeHeader
				p.buffer = loc.remaining
				answerIdx = 0
			}
		} else if p.currentSection == "reasoning" && !p.reasoningDone {
			// Can't use combined buffer for reasoning since we stream it incrementally
			answerIdx = findMarkdownHeader(p.buffer, "Generation")
		} else if p.currentSection != "generation" && p.currentSection != "followup" {
			// For other sections (or no section yet), just check p.buffer
			// Skip if already in answer or followup to prevent re-detection
			answerIdx = findMarkdownHeader(p.buffer, "Generation")
		}

		if answerIdx >= 0 {
			// Transition to answer section
			if err := p.transitionToAnswer(ctx, answerIdx); err != nil {
				return err
			}
			// Skip the header line
			if remaining, ok := skipHeaderLine(p.buffer); ok {
				p.buffer = remaining
				sectionChanged = true
				continue
			} else {
				// Header incomplete, mark pending and wait for more data
				p.pendingHeaderSkip = true
				p.pendingHeaderSection = "generation"
				break
			}
		}

		// Check for Follow-up Questions section
		if p.withFollowup {
			if idx := findMarkdownHeader(p.buffer, "Follow-up Questions"); idx >= 0 {
				// Transition to followup section
				if err := p.transitionToFollowup(ctx, idx); err != nil {
					return err
				}
				// Skip the header line
				if remaining, ok := skipHeaderLine(p.buffer); ok {
					p.buffer = remaining
					sectionChanged = true
					continue
				} else {
					// Header incomplete, mark pending and wait for more data
					p.pendingHeaderSkip = true
					p.pendingHeaderSection = "followup"
					break
				}
			}
		}

		// Stream content for current section
		// IMPORTANT: Don't accumulate if we've already detected a section transition above
		// This prevents infinite loops where we keep detecting the same header
		if sectionChanged {
			continue
		}

		// No section transition detected - stream or accumulate content for current section
		if p.currentSection == "confidence" && !p.confidenceDone {
			// Fully buffer confidence section (needs complete JSON to parse)
			p.confidenceBuffer += p.buffer
			p.buffer = ""
		} else if p.currentSection == "reasoning" && !p.reasoningDone {
			// Stream reasoning with lookahead for Answer header detection
			if streamable, remaining, shouldStream := streamContentWithLookahead(p.buffer, streamLookaheadSize, streamMinBufferSize); shouldStream {
				if p.callback != nil {
					if err := p.callback(ctx, "reasoning", streamable); err != nil {
						return err
					}
				}
				p.buffer = remaining
			}
		} else if p.currentSection == "generation" {
			// Stream answer with lookahead for Follow-up header detection
			if streamable, remaining, shouldStream := streamContentWithLookahead(p.buffer, streamLookaheadSize, streamMinBufferSize); shouldStream {
				p.generationBuffer += streamable
				if p.callback != nil {
					if err := p.callback(ctx, "generation", streamable); err != nil {
						return err
					}
				}
				p.buffer = remaining
			}
		} else if p.currentSection == "followup" {
			// Fully buffer followup section (needs complete content to parse questions)
			p.followUpBuffer += p.buffer
			p.buffer = ""
		}

		// If we didn't change sections, exit the loop
		break
	}

	return nil
}

// flushConfidenceBuffer parses and emits the accumulated confidence buffer.
// This is extracted to avoid duplication between transition methods.
// Returns error if callback fails.
func (p *markdownSectionParser) flushConfidenceBuffer(ctx context.Context, additionalContent string) error {
	if !p.confidenceDone && p.currentSection == "confidence" && p.callback != nil {
		if additionalContent != "" {
			p.confidenceBuffer += additionalContent
		}
		// Parse and emit confidence as a single event
		if p.confidenceBuffer != "" {
			confidence := parseConfidenceMarkdown(p.confidenceBuffer)
			if err := p.callback(ctx, "confidence", confidence); err != nil {
				return err
			}
		}
		p.confidenceDone = true
	}
	return nil
}

// transitionToReasoning flushes confidence section and transitions to reasoning.
// Returns error if callback fails.
func (p *markdownSectionParser) transitionToReasoning(ctx context.Context, headerIdx int) error {
	// Flush any remaining confidence content
	trimmed := strings.TrimSpace(p.buffer[:headerIdx])
	if err := p.flushConfidenceBuffer(ctx, trimmed); err != nil {
		return err
	}
	p.currentSection = "reasoning"
	p.buffer = p.buffer[headerIdx:]
	return nil
}

// transitionToAnswer flushes the current section (confidence or reasoning) and transitions to answer.
// Returns error if callback fails.
func (p *markdownSectionParser) transitionToAnswer(ctx context.Context, headerIdx int) error {
	trimmed := strings.TrimSpace(p.buffer[:headerIdx])

	// Flush any remaining confidence content (if transitioning from confidence)
	if err := p.flushConfidenceBuffer(ctx, trimmed); err != nil {
		return err
	}

	// Flush any remaining reasoning content (if transitioning from reasoning)
	if p.currentSection == "reasoning" && !p.reasoningDone && p.callback != nil {
		if trimmed != "" {
			if err := p.callback(ctx, "reasoning", trimmed); err != nil {
				return err
			}
		}
		p.reasoningDone = true
	}

	p.currentSection = "generation"
	p.buffer = p.buffer[headerIdx:]
	return nil
}

// transitionToFollowup flushes answer section and transitions to followup.
// Returns error if callback fails.
func (p *markdownSectionParser) transitionToFollowup(ctx context.Context, headerIdx int) error {
	// Flush any remaining answer content
	if p.currentSection == "generation" && p.callback != nil {
		if trimmed := strings.TrimSpace(p.buffer[:headerIdx]); trimmed != "" {
			p.generationBuffer += trimmed
			if err := p.callback(ctx, "generation", trimmed); err != nil {
				return err
			}
		}
	}
	p.currentSection = "followup"
	p.buffer = p.buffer[headerIdx:]
	return nil
}

// flush emits any remaining buffered content at the end of streaming
func (p *markdownSectionParser) flush(ctx context.Context) {
	// If we were waiting for a header line to complete, try to skip it now
	// This handles the case where the stream ends with an incomplete header line
	if p.pendingHeaderSkip {
		if remaining, ok := skipHeaderLine(p.buffer); ok {
			p.buffer = remaining
			p.pendingHeaderSkip = false
			p.pendingHeaderSection = ""
		}
		// If skipHeaderLine fails, the buffer still contains the header - we'll emit it as content below
	}

	if p.buffer == "" {
		return
	}

	// Flush remaining content based on current section
	// TODO: Consider adding logger to struct to properly log callback errors instead of silently discarding them
	if p.currentSection == "confidence" && !p.confidenceDone {
		p.confidenceBuffer += p.buffer
		if p.confidenceBuffer != "" && p.callback != nil {
			confidence := parseConfidenceMarkdown(p.confidenceBuffer)
			_ = p.callback(ctx, "confidence", confidence)
		}
		p.confidenceDone = true
	} else if p.currentSection == "reasoning" && !p.reasoningDone && p.callback != nil {
		if trimmed := strings.TrimSpace(p.buffer); trimmed != "" {
			_ = p.callback(ctx, "reasoning", trimmed)
		}
		p.reasoningDone = true
	} else if p.currentSection == "generation" && p.callback != nil {
		if trimmed := stripModelEndTokens(p.buffer); trimmed != "" {
			p.generationBuffer += trimmed
			_ = p.callback(ctx, "generation", trimmed)
		}
	} else if p.currentSection == "followup" {
		p.followUpBuffer += p.buffer
	} else if p.currentSection == "" && p.callback != nil {
		// No section header was ever found - emit entire buffer as answer
		// This handles edge cases where the LLM doesn't follow the expected format
		if trimmed := stripModelEndTokens(p.buffer); trimmed != "" {
			p.generationBuffer = trimmed
			_ = p.callback(ctx, "generation", trimmed)
		}
	}

	p.buffer = ""
}

// findMarkdownHeader finds a markdown header (## HeaderName) in the text
func findMarkdownHeader(text, headerName string) int {
	// Look for "## HeaderName" pattern
	pattern := "## " + headerName
	for i := 0; i <= len(text)-len(pattern); i++ {
		// Check if we're at the start of a line (beginning of text or after newline)
		if i == 0 || text[i-1] == '\n' {
			match := true
			for j := 0; j < len(pattern); j++ {
				if text[i+j] != pattern[j] {
					match = false
					break
				}
			}
			if match {
				return i
			}
		}
	}
	return -1
}

// findNextNewline finds the next newline character in the text
func findNextNewline(text string) int {
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			return i
		}
	}
	return -1
}

// parseStructuredResponse parses the complete response into structured fields
func parseStructuredResponse(fullResponse string, withConfidence, withReasoning, withFollowup bool) *GenerationResult {
	result := &GenerationResult{}

	sections := splitIntoSections(fullResponse)

	if withConfidence {
		if confidenceText, ok := sections["confidence"]; ok {
			confidence := parseConfidenceMarkdown(confidenceText)
			result.GenerationConfidence = confidence.GenerationConfidence
			result.ContextRelevance = confidence.ContextRelevance
		}
	}

	if answer, ok := sections["generation"]; ok {
		result.Generation = stripModelEndTokens(answer)
	} else {
		// If no sections found, use the whole response as answer
		result.Generation = stripModelEndTokens(fullResponse)
	}

	if withFollowup {
		if followup, ok := sections["followup"]; ok {
			result.FollowupQuestions = parseFollowupQuestions(followup)
		}
	}

	return result
}

// ConfidenceResult holds the parsed confidence scores
type ConfidenceResult struct {
	GenerationConfidence float32 `json:"generation_confidence"`
	ContextRelevance     float32 `json:"context_relevance"`
}

// parseConfidenceMarkdown parses the confidence scores from markdown format
// Expected format:
//
//	Generation Confidence: 0.95
//	Context Relevance: 0.98
func parseConfidenceMarkdown(text string) ConfidenceResult {
	result := ConfidenceResult{
		GenerationConfidence: defaultConfidenceScore,
		ContextRelevance:     defaultConfidenceScore,
	}

	text = strings.TrimSpace(text)
	lines := splitLines(text)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse "Generation Confidence: X.XX" format
		if after, ok := strings.CutPrefix(line, "Generation Confidence:"); ok {
			valueStr := strings.TrimSpace(after)
			if value, err := parseFloat32(valueStr); err == nil {
				if value >= 0 && value <= 1 {
					result.GenerationConfidence = value
				}
			}
		}

		// Parse "Context Relevance: X.XX" format
		if after, ok := strings.CutPrefix(line, "Context Relevance:"); ok {
			valueStr := strings.TrimSpace(after)
			if value, err := parseFloat32(valueStr); err == nil {
				if value >= 0 && value <= 1 {
					result.ContextRelevance = value
				}
			}
		}
	}

	return result
}

// parseFloat32 parses a string to float32
func parseFloat32(s string) (float32, error) {
	// Handle common float formats
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}

	// Try to parse as float
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	if err != nil {
		return 0, err
	}

	return float32(f), nil
}

// splitIntoSections splits markdown text into sections based on headers
func splitIntoSections(text string) map[string]string {
	sections := make(map[string]string)

	lines := splitLines(text)
	var currentSection string
	var currentContent []string

	for _, line := range lines {
		// Check if this is a section header
		if len(line) >= 3 && line[0] == '#' && line[1] == '#' && line[2] == ' ' {
			// Save previous section
			if currentSection != "" {
				sections[currentSection] = joinLines(currentContent)
			}

			// Start new section
			headerText := strings.TrimSpace(line[3:])
			switch headerText {
			case "Confidence":
				currentSection = "confidence"
			case "Reasoning":
				currentSection = "reasoning"
			case "Classification Result":
				currentSection = "classification result"
			case "Generation":
				currentSection = "generation"
			case "Follow-up Questions", "Follow-up":
				currentSection = "followup"
			default:
				currentSection = ""
			}
			currentContent = []string{}
		} else if currentSection != "" {
			currentContent = append(currentContent, line)
		}
	}

	// Save final section
	if currentSection != "" {
		sections[currentSection] = joinLines(currentContent)
	}

	return sections
}

// joinLines joins lines with newlines
func joinLines(lines []string) string {
	return strings.Join(lines, "\n")
}

// parseFollowupQuestions extracts follow-up questions from markdown list
func parseFollowupQuestions(text string) []string {
	var questions []string
	lines := splitLines(text)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Remove leading bullet points or numbers
		if len(line) > 2 &&
			(line[0] == '-' || line[0] == '*' || (line[0] >= '0' && line[0] <= '9')) {
			if line[1] == ' ' || line[1] == '.' {
				line = strings.TrimSpace(line[2:])
			}
		}
		// Remove surrounding quotes (both single and double), recursively for multiple layers
		for len(line) >= 2 {
			if (line[0] == '"' && line[len(line)-1] == '"') ||
				(line[0] == '\'' && line[len(line)-1] == '\'') {
				line = line[1 : len(line)-1]
				line = strings.TrimSpace(line) // Trim any whitespace between quote layers
			} else {
				break
			}
		}
		if line != "" {
			questions = append(questions, line)
		}
	}
	return questions
}

// splitLines splits text into lines
func splitLines(text string) []string {
	return strings.Split(text, "\n")
}

// stripModelEndTokens removes model-specific end-of-turn tokens from the end of text.
// These tokens (like </end_of_turn> for Gemini) sometimes leak through in LLM responses.
func stripModelEndTokens(text string) string {
	text = strings.TrimSpace(text)
	for _, token := range modelEndTokens {
		if trimmed, found := strings.CutSuffix(text, token); found {
			return strings.TrimSpace(trimmed)
		}
	}
	return text
}

// classificationStreamingParser handles streaming of classification markdown response
type classificationStreamingParser struct {
	callback             func(ctx context.Context, eventType string, data any) error
	buffer               string
	reasoningDone        bool
	currentSection       string
	streamedReasoningLen int // Track how much reasoning content we've already streamed
}

// newClassificationStreamingParser creates a new classification streaming parser
func newClassificationStreamingParser(callback func(ctx context.Context, eventType string, data any) error) *classificationStreamingParser {
	return &classificationStreamingParser{
		callback: callback,
	}
}

// parseChunk processes a streaming chunk of text
func (p *classificationStreamingParser) parseChunk(ctx context.Context, chunk *ai.ModelResponseChunk) error {
	if len(chunk.Content) == 0 {
		return nil
	}

	for _, part := range chunk.Content {
		if part.Text == "" {
			continue
		}

		p.buffer += part.Text

		// Check for section boundaries
		if strings.Contains(p.buffer, "## Reasoning") && !p.reasoningDone {
			p.currentSection = "reasoning"
		} else if strings.Contains(p.buffer, "## Classification Result") {
			p.currentSection = "classification"
			if !p.reasoningDone {
				p.reasoningDone = true
			}
		}

		// Stream reasoning section in real-time
		if p.currentSection == "reasoning" && !p.reasoningDone {
			// Extract text after "## Reasoning" header
			headerIdx := strings.Index(p.buffer, "## Reasoning")
			if headerIdx != -1 {
				afterHeader := p.buffer[headerIdx+len("## Reasoning"):]

				// Check if we've hit the next section
				if before, _, ok := strings.Cut(afterHeader, "## Classification Result"); ok {
					// Stream only the NEW reasoning content we haven't streamed yet
					// We work with the untrimmed content and track position in it
					fullReasoning := before
					if len(fullReasoning) > p.streamedReasoningLen {
						newContent := fullReasoning[p.streamedReasoningLen:]
						// Only trim the newContent for callback, not for length tracking
						trimmedNew := strings.TrimSpace(newContent)
						if len(trimmedNew) > 0 {
							if err := p.callback(ctx, "reasoning", trimmedNew); err != nil {
								return err
							}
						}
						p.streamedReasoningLen = len(fullReasoning)
					}
					p.reasoningDone = true
					p.currentSection = "classification"
				} else {
					// Still in reasoning section, stream incrementally
					// Keep a lookahead buffer to avoid streaming partial markdown headers
					if len(afterHeader) > streamLookaheadSize {
						// Calculate how much NEW content we can stream (from untrimmed content)
						availableToStream := len(afterHeader) - streamLookaheadSize
						if availableToStream > p.streamedReasoningLen {
							toStream := afterHeader[p.streamedReasoningLen:availableToStream]
							// Split at UTF-8 boundary to avoid breaking multi-byte characters
							validPrefix, _ := splitValidUTF8(toStream)
							if len(validPrefix) > 0 {
								if err := p.callback(ctx, "reasoning", validPrefix); err != nil {
									return err
								}
								p.streamedReasoningLen += len(validPrefix)
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// flush sends any remaining buffered content
func (p *classificationStreamingParser) flush(ctx context.Context) {
	if !p.reasoningDone && p.currentSection == "reasoning" {
		// Send any remaining reasoning content that hasn't been streamed yet
		headerIdx := strings.Index(p.buffer, "## Reasoning")
		if headerIdx != -1 {
			afterHeader := p.buffer[headerIdx+len("## Reasoning"):]
			if before, _, ok := strings.Cut(afterHeader, "## Classification Result"); ok {
				fullReasoning := before
				if len(fullReasoning) > p.streamedReasoningLen {
					newContent := fullReasoning[p.streamedReasoningLen:]
					trimmedNew := strings.TrimSpace(newContent)
					if len(trimmedNew) > 0 {
						_ = p.callback(ctx, "reasoning", trimmedNew)
					}
				}
			} else {
				fullReasoning := afterHeader
				if len(fullReasoning) > p.streamedReasoningLen {
					newContent := fullReasoning[p.streamedReasoningLen:]
					trimmedNew := strings.TrimSpace(newContent)
					if len(trimmedNew) > 0 {
						_ = p.callback(ctx, "reasoning", trimmedNew)
					}
				}
			}
		}
		p.reasoningDone = true
	}
}

// ClassifyImproveAndTransformQuery performs all query enhancements in a single LLM call:
// 1. Classifies as question or search
// 2. Selects optimal retrieval strategy (simple, decompose, step_back, hyde)
// 3. Chooses semantic query mode (rewrite vs hypothetical)
// 4. Improves the query (spelling, grammar, clarity)
// 5. Transforms for optimal semantic search based on selected mode
// 6. Generates strategy-specific outputs (sub_questions, step_back_query, multi_phrases)
func (g *GenKitModelImpl) ClassifyImproveAndTransformQuery(
	ctx context.Context,
	query string,
	opts ...GenerationOption,
) (*ClassificationTransformationResult, error) {
	if query == "" {
		return nil, errors.New("query cannot be empty")
	}

	// Apply options
	options := &generationOptions{}
	for _, opt := range opts {
		opt.applyGenerationOption(options)
	}

	// Use classification system prompt or custom prompt
	systemPrompt := ClassificationTransformationPrompt
	if options.systemPrompt != "" {
		systemPrompt = options.systemPrompt
	}

	// Build the prompt template with conditional reasoning
	promptTemplate := ClassificationTransformationUserPrompt

	// Generate a unique prompt name based on system prompt hash and reasoning flag
	hashInput := systemPrompt + promptTemplate
	if options.withClassificationReasoning {
		hashInput += "_with_reasoning"
	}
	hash := sha256.Sum256([]byte(hashInput))
	promptName := fmt.Sprintf("classification-transform-%x", hash[:8])

	// Check if prompt already exists, if not define it
	enhancedPrompt := genkit.LookupPrompt(g.Genkit, promptName)
	if enhancedPrompt == nil {
		enhancedPrompt = genkit.DefinePrompt(g.Genkit, promptName,
			ai.WithDescription("Classification and query transformation with strategy selection"),
			ai.WithModel(g.Model),
			ai.WithSystem(systemPrompt),
			ai.WithPrompt(promptTemplate),
		)
	}

	// Prepare input
	input := map[string]any{
		"query":          query,
		"with_reasoning": options.withClassificationReasoning,
	}

	// Add agent knowledge if provided
	if options.agentKnowledge != "" {
		input["agent_knowledge"] = options.agentKnowledge
	}

	// Setup streaming if callback is provided
	var streamingParser *classificationStreamingParser
	execOpts := []ai.PromptExecuteOption{ai.WithInput(input)}

	if options.streamCallback != nil {
		streamingParser = newClassificationStreamingParser(options.streamCallback)
		execOpts = append(execOpts, ai.WithStreaming(streamingParser.parseChunk))
	}

	// Execute prompt
	resp, err := enhancedPrompt.Execute(ctx, execOpts...)
	if err != nil {
		logger := zap.L()
		logger.Warn("Classification query transformation failed, using default values",
			zap.Error(err),
			zap.String("query", query),
		)
		// Return default fallback with simple strategy
		return &ClassificationTransformationResult{
			RouteType:     RouteTypeSearch,
			Strategy:      QueryStrategySimple,
			SemanticMode:  SemanticQueryModeRewrite,
			ImprovedQuery: query,
			SemanticQuery: query,
			// TODO (ajr) MultiPhrases doesn't look used
			MultiPhrases: []string{query},
			Confidence:   0.5,
		}, nil
	}

	// Flush any remaining buffered content from streaming parser
	if streamingParser != nil {
		streamingParser.flush(ctx)
	}

	// Extract and parse the response
	responseText := strings.TrimSpace(resp.Text())

	// Try to parse the response (markdown or JSON)
	result, parseErr := parseClassificationTransformationResponse(responseText, query)
	if parseErr != nil {
		logger := zap.L()
		logger.Warn("Failed to parse classification transformation response, using default values",
			zap.Error(parseErr),
			zap.String("query", query),
			zap.String("response_text", responseText),
		)
		// Return default fallback with simple strategy
		return &ClassificationTransformationResult{
			RouteType:     RouteTypeSearch,
			Strategy:      QueryStrategySimple,
			SemanticMode:  SemanticQueryModeRewrite,
			ImprovedQuery: query,
			SemanticQuery: query,
			MultiPhrases:  []string{query},
			Confidence:    0.5,
		}, nil
	}

	// Apply defaults for missing/invalid values
	applyTransformationDefaults(result, query)

	return result, nil
}

// parseClassificationMarkdown parses markdown-formatted classification response
func parseClassificationMarkdown(responseText string, originalQuery string) (*ClassificationTransformationResult, error) {
	result := &ClassificationTransformationResult{}

	// Split into sections
	sections := splitIntoSections(responseText)

	// Extract reasoning if present
	if reasoning, ok := sections["reasoning"]; ok {
		result.Reasoning = strings.TrimSpace(reasoning)
	}

	// Parse Classification Result section
	classificationText, ok := sections["classification result"]
	if !ok {
		return nil, fmt.Errorf("missing Classification Result section")
	}

	// Parse key-value pairs from the classification section
	lines := strings.Split(classificationText, "\n")
	var multiPhrases []string
	var subQuestions []string
	inMultiPhrases := false
	inSubQuestions := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Check for bullet list items
		if strings.HasPrefix(line, "-") || strings.HasPrefix(line, "*") {
			item := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "-"), "*"))
			item = strings.TrimSpace(strings.TrimPrefix(item, "*"))
			if inMultiPhrases {
				multiPhrases = append(multiPhrases, item)
			} else if inSubQuestions {
				subQuestions = append(subQuestions, item)
			}
			continue
		}

		// Check for field labels
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(parts[0]))
			value := strings.TrimSpace(parts[1])
			// Strip surrounding quotes if present
			value = strings.Trim(value, `"`)

			// Reset list tracking when we hit a new field
			inMultiPhrases = false
			inSubQuestions = false

			switch key {
			case "route type":
				result.RouteType = RouteType(value)
			case "strategy":
				result.Strategy = QueryStrategy(value)
			case "semantic mode":
				result.SemanticMode = SemanticQueryMode(value)
			case "confidence":
				if conf, err := strconv.ParseFloat(value, 32); err == nil {
					result.Confidence = float32(conf)
				}
			case "improved query":
				result.ImprovedQuery = value
			case "semantic query":
				result.SemanticQuery = value
			case "step back query":
				result.StepBackQuery = value
			case "multi phrases":
				inMultiPhrases = true
			case "sub questions":
				inSubQuestions = true
			}
		}
	}

	result.MultiPhrases = multiPhrases
	result.SubQuestions = subQuestions

	return result, nil
}

// parseClassificationTransformationResponse parses the LLM response for classification query transformation
// Supports both markdown format (preferred) and JSON format (fallback for backwards compatibility)
func parseClassificationTransformationResponse(responseText string, originalQuery string) (*ClassificationTransformationResult, error) {
	// Try markdown format first (check for ## Classification Result header)
	if strings.Contains(responseText, "## Classification Result") || strings.Contains(responseText, "## Reasoning") {
		result, err := parseClassificationMarkdown(responseText, originalQuery)
		if err == nil {
			return result, nil
		}
		// If markdown parsing failed, log but continue to try JSON
	}

	// Fall back to JSON format for backwards compatibility
	jsonStart := strings.Index(responseText, "{")
	jsonEnd := strings.LastIndex(responseText, "}")

	if jsonStart == -1 || jsonEnd == -1 || jsonEnd <= jsonStart {
		return nil, fmt.Errorf("no valid markdown or JSON found in response")
	}

	jsonText := responseText[jsonStart : jsonEnd+1]

	// Parse into a flexible map first to handle all fields
	var rawResult map[string]any
	if err := json.Unmarshal([]byte(jsonText), &rawResult); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	result := &ClassificationTransformationResult{}

	// Parse route_type
	if rt, ok := rawResult["route_type"].(string); ok {
		result.RouteType = RouteType(rt)
	}

	// Parse strategy
	if s, ok := rawResult["strategy"].(string); ok {
		result.Strategy = QueryStrategy(s)
	}

	// Parse semantic_mode
	if sm, ok := rawResult["semantic_mode"].(string); ok {
		result.SemanticMode = SemanticQueryMode(sm)
	}

	// Parse improved_query
	if iq, ok := rawResult["improved_query"].(string); ok {
		result.ImprovedQuery = iq
	}

	// Parse semantic_query
	if sq, ok := rawResult["semantic_query"].(string); ok {
		result.SemanticQuery = sq
	}

	// Parse confidence
	if c, ok := rawResult["confidence"].(float64); ok {
		result.Confidence = float32(c)
	}

	// Parse reasoning (optional)
	if r, ok := rawResult["reasoning"].(string); ok {
		result.Reasoning = r
	}

	// Parse step_back_query (optional, for step_back strategy)
	if sbq, ok := rawResult["step_back_query"].(string); ok {
		result.StepBackQuery = sbq
	}

	// Parse sub_questions (optional, for decompose strategy)
	if sqs, ok := rawResult["sub_questions"].([]any); ok {
		for _, sq := range sqs {
			if s, ok := sq.(string); ok && s != "" {
				result.SubQuestions = append(result.SubQuestions, s)
			}
		}
	}

	// Parse multi_phrases (optional)
	if mps, ok := rawResult["multi_phrases"].([]any); ok {
		for _, mp := range mps {
			if s, ok := mp.(string); ok && s != "" {
				result.MultiPhrases = append(result.MultiPhrases, s)
			}
		}
	}

	return result, nil
}

// applyTransformationDefaults applies default values for missing/invalid fields
func applyTransformationDefaults(result *ClassificationTransformationResult, originalQuery string) {
	// Default route_type
	if result.RouteType == "" {
		result.RouteType = RouteTypeSearch
	}

	// Default strategy
	if result.Strategy == "" {
		result.Strategy = QueryStrategySimple
	}

	// Default semantic_mode based on strategy
	if result.SemanticMode == "" {
		if result.Strategy == QueryStrategyHyde {
			result.SemanticMode = SemanticQueryModeHypothetical
		} else {
			result.SemanticMode = SemanticQueryModeRewrite
		}
	}

	// Default improved_query
	if result.ImprovedQuery == "" {
		result.ImprovedQuery = originalQuery
	}

	// Default semantic_query
	if result.SemanticQuery == "" {
		result.SemanticQuery = originalQuery
	}

	// Default confidence
	if result.Confidence < 0 || result.Confidence > 1 {
		result.Confidence = 0.5
	}

	// Default multi_phrases to include at least the original query
	if len(result.MultiPhrases) == 0 {
		result.MultiPhrases = []string{originalQuery}
	}

	// Validate strategy-specific fields
	if result.Strategy == QueryStrategyDecompose && len(result.SubQuestions) == 0 {
		// If decompose was chosen but no sub_questions, fall back to simple
		result.Strategy = QueryStrategySimple
	}

	if result.Strategy == QueryStrategyStepBack && result.StepBackQuery == "" {
		// If step_back was chosen but no step_back_query, fall back to simple
		result.Strategy = QueryStrategySimple
	}
}

// GenerateQueryResponse generates the final answer for both questions and search queries
func (g *GenKitModelImpl) GenerateQueryResponse(
	ctx context.Context,
	query string,
	docs []schema.Document,
	classificationResult *ClassificationTransformationResult,
	opts ...GenerationOption,
) (*GenerationOutput, error) {
	if query == "" {
		return nil, errors.New("query cannot be empty")
	}
	if classificationResult == nil {
		return nil, errors.New("classification result cannot be nil")
	}

	// Apply options
	options := &generationOptions{}
	for _, opt := range opts {
		opt.applyGenerationOption(options)
	}

	output := &GenerationOutput{
		Classification: classificationResult.RouteType,
	}

	// Always generate answer, but adjust style based on classification
	// Set semantic query for the answer
	opts = append(opts, WithGenerationSemanticQuery(query))

	// Pass the full classification result for multiquery context awareness
	opts = append(opts, WithClassificationResult(classificationResult))

	result, usage, err := g.Generate(ctx, query, docs, opts...)
	if err != nil {
		g.logger.Error("GenerateQueryResponse: Answer failed", zap.Error(err))
		return nil, fmt.Errorf("generating answer: %w", err)
	}

	output.GenerationConfidence = result.GenerationConfidence
	output.ContextRelevance = result.ContextRelevance
	output.Generation = result.Generation
	output.FollowupQuestions = result.FollowupQuestions
	output.Usage = usage

	return output, nil
}
