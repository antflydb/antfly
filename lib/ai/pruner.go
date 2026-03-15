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
	"fmt"
	"strings"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/template"
	"github.com/antflydb/termite/pkg/termite/lib/tokenizers"
)

// DefaultDocumentRenderer is the default handlebars template for rendering document content.
// Uses TOON format for 30-60% token reduction compared to JSON.
// This is used by both the RAG pipeline and token pruner for consistency.
const DefaultDocumentRenderer = `{{encodeToon this.fields}}`

// DefaultDocumentWrapper is the wrapper template for each document.
// Used to format documents consistently in RAG prompts and token estimation.
// The format string expects (documentID, renderedContent).
const DefaultDocumentWrapper = `==================================================
Document ID: %s
Content:
%s
==================================================
`

// Pruner estimates token counts and prunes documents to fit within a budget.
// Documents are taken in order (highest-ranked first) until the budget is
// exhausted, preserving the relevance ranking from the retriever/reranker.
type Pruner struct {
	tokenizer        tokenizers.TokenCounter
	documentRenderer string
}

// PruneStats tracks pruning statistics
type PruneStats struct {
	// ResourcesKept is the number of documents kept
	ResourcesKept int `json:"resources_kept"`
	// TokensKept is the total tokens in kept documents
	TokensKept int `json:"tokens_kept"`
	// ResourcesPruned is the number of documents pruned
	ResourcesPruned int `json:"resources_pruned"`
	// TokensPruned is the total tokens in pruned documents
	TokensPruned int `json:"tokens_pruned"`
}

// NewPruner creates a new Pruner.
// The documentRenderer parameter specifies the handlebars template for rendering document content.
// Pass empty string to use DefaultDocumentRenderer.
// Uses BERT tokenizer for token counting, which provides good general-purpose estimates.
func NewPruner(documentRenderer string) (*Pruner, error) {
	tk, err := tokenizers.NewTokenCounter()
	if err != nil {
		return nil, fmt.Errorf("failed to create token counter: %w", err)
	}

	if documentRenderer == "" {
		documentRenderer = DefaultDocumentRenderer
	}

	return &Pruner{
		tokenizer:        tk,
		documentRenderer: documentRenderer,
	}, nil
}

// CountTokens returns the token count for the given text using the BERT tokenizer.
func (tp *Pruner) CountTokens(text string) int {
	if text == "" {
		return 0
	}

	return tp.tokenizer.CountTokens(text)
}

// RenderDocument renders a document using the configured template.
// Returns the rendered content including the document wrapper.
func (tp *Pruner) RenderDocument(doc schema.Document) (string, error) {
	// Filter out underscore-prefixed internal fields (e.g. _embeddings, _chunks)
	fields := make(map[string]any)
	for key, value := range doc.Fields {
		if strings.HasPrefix(key, "_") {
			continue
		}
		fields[key] = value
	}

	// Render using handlebars template
	context := map[string]any{
		"id":     doc.ID,
		"fields": fields,
	}

	content, err := template.RenderHandlebars(tp.documentRenderer, context)
	if err != nil {
		return "", fmt.Errorf("failed to render document %s: %w", doc.ID, err)
	}

	// Wrap in document format matching RAG pipeline
	return fmt.Sprintf(DefaultDocumentWrapper, doc.ID, content), nil
}

// EstimateDocumentTokens estimates the token count for a single document.
func (tp *Pruner) EstimateDocumentTokens(doc schema.Document) (int, error) {
	rendered, err := tp.RenderDocument(doc)
	if err != nil {
		return 0, err
	}
	return tp.CountTokens(rendered), nil
}

// PruneToTokenBudget takes documents in ranked order until the token budget is exhausted.
// Documents should already be sorted by relevance (highest-ranked first).
// Returns the kept documents (a strict prefix of the ranked list) and statistics.
//
// Parameters:
//   - docs: Documents sorted by relevance (highest first)
//   - maxTokens: Maximum total tokens allowed for context
//   - reserveTokens: Tokens to reserve for system prompt, answer generation, etc.
func (tp *Pruner) PruneToTokenBudget(
	docs []schema.Document,
	maxTokens int,
	reserveTokens int,
) ([]schema.Document, PruneStats, error) {
	availableTokens := maxTokens - reserveTokens
	if availableTokens <= 0 {
		return nil, PruneStats{
			ResourcesPruned: len(docs),
			TokensPruned:    0, // We don't count since we're rejecting all
		}, nil
	}

	var result []schema.Document
	var totalTokens int

	for _, doc := range docs {
		rendered, err := tp.RenderDocument(doc)
		if err != nil {
			continue
		}

		docTokens := tp.CountTokens(rendered)

		// Stop once a document exceeds the remaining budget — don't skip
		// to include lower-ranked docs, as that would undermine the ranking.
		if totalTokens+docTokens > availableTokens {
			break
		}

		result = append(result, doc)
		totalTokens += docTokens
	}

	stats := PruneStats{
		ResourcesKept:   len(result),
		TokensKept:      totalTokens,
		ResourcesPruned: len(docs) - len(result),
	}

	return result, stats, nil
}

// EstimateTotalTokens calculates the total token count for a list of documents.
func (tp *Pruner) EstimateTotalTokens(docs []schema.Document) (int, error) {
	var total int
	for _, doc := range docs {
		tokens, err := tp.EstimateDocumentTokens(doc)
		if err != nil {
			return 0, err
		}
		total += tokens
	}
	return total, nil
}
