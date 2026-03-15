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

package reranking

import (
	"context"
	"fmt"
	"os"

	cohere "github.com/cohere-ai/cohere-go/v2"
	cohereclient "github.com/cohere-ai/cohere-go/v2/client"

	"github.com/antflydb/antfly/lib/schema"
)

// CohereReranker implements the Reranker interface using Cohere's native rerank API
type CohereReranker struct {
	client          *cohereclient.Client
	model           string
	topN            *int
	maxChunksPerDoc *int
	field           string
	template        string
}

func init() {
	RegisterReranker(RerankerProviderCohere, NewCohereReranker)
}

// NewCohereReranker creates a new Cohere reranker from configuration
func NewCohereReranker(config RerankerConfig) (Reranker, error) {
	c, err := config.AsCohereRerankerConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing cohere config: %w", err)
	}

	// Get API key from config or environment
	apiKey := ""
	if c.ApiKey != nil && *c.ApiKey != "" {
		apiKey = *c.ApiKey
	} else {
		apiKey = os.Getenv("COHERE_API_KEY")
	}

	if apiKey == "" {
		return nil, fmt.Errorf("cohere API key not provided (set api_key in config or COHERE_API_KEY env var)")
	}

	client := cohereclient.NewClient(cohereclient.WithToken(apiKey))

	// Default model if not specified
	model := c.Model
	if model == "" {
		model = "rerank-english-v3.0"
	}

	// Extract field and template from config
	field := ""
	if config.Field != nil {
		field = *config.Field
	}

	template := ""
	if config.Template != nil {
		template = *config.Template
	}

	return &CohereReranker{
		client:          client,
		model:           model,
		topN:            c.TopN,
		maxChunksPerDoc: c.MaxChunksPerDoc,
		field:           field,
		template:        template,
	}, nil
}

func (r *CohereReranker) Rerank(
	ctx context.Context,
	query string,
	documents []schema.Document,
) ([]float32, error) {
	if len(documents) == 0 {
		return []float32{}, nil
	}

	// Extract text from documents using field or template
	documentTexts, err := ExtractDocumentTexts(documents, r.field, r.template)
	if err != nil {
		return nil, fmt.Errorf("extracting document texts: %w", err)
	}

	// Convert strings to RerankRequestDocumentsItem
	docs := make([]*cohere.RerankRequestDocumentsItem, len(documentTexts))
	for i, text := range documentTexts {
		docs[i] = &cohere.RerankRequestDocumentsItem{String: text}
	}

	// Build rerank request with string documents
	req := &cohere.RerankRequest{
		Query:     query,
		Documents: docs,
		Model:     &r.model,
	}

	if r.topN != nil {
		req.TopN = r.topN
	}

	if r.maxChunksPerDoc != nil {
		req.MaxChunksPerDoc = r.maxChunksPerDoc
	}

	// Always return results for all documents
	returnAllChunks := false
	req.ReturnDocuments = &returnAllChunks

	resp, err := r.client.Rerank(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cohere rerank request failed: %w", err)
	}

	// Initialize scores array with zeros (for documents not in top_n if specified)
	scores := make([]float32, len(documents))

	// Map results back to original document positions
	for _, result := range resp.Results {
		if result.Index < len(scores) {
			scores[result.Index] = float32(result.RelevanceScore)
		}
	}

	return scores, nil
}
