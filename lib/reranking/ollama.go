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
	"net/http"
	"net/url"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/schema"
	ollamaapi "github.com/ollama/ollama/api"
)

type OllamaReranker struct {
	client         *ollamaapi.Client
	embeddingModel string
	field          string
	template       string
}

func init() {
	RegisterReranker(RerankerProviderOllama, NewOllamaReranker)
}

func NewOllamaReranker(config RerankerConfig) (Reranker, error) {
	var client *ollamaapi.Client
	var err error

	c, err := config.AsOllamaRerankerConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing ollama config: %w", err)
	}

	if c.Url != nil && *c.Url != "" {
		// Parse URL and create client with custom base URL
		u, err := url.Parse(*c.Url)
		if err != nil {
			return nil, fmt.Errorf("parsing ollama URL: %w", err)
		}
		client = ollamaapi.NewClient(u, http.DefaultClient)
	} else {
		// Use environment-based client (respects OLLAMA_HOST)
		client, err = ollamaapi.ClientFromEnvironment()
		if err != nil {
			return nil, fmt.Errorf("creating ollama client from environment: %w", err)
		}
	}

	field := ""
	if config.Field != nil {
		field = *config.Field
	}

	template := ""
	if config.Template != nil {
		template = *config.Template
	}

	return &OllamaReranker{
		client:         client,
		embeddingModel: c.Model,
		field:          field,
		template:       template,
	}, nil
}

func (o *OllamaReranker) Rerank(
	ctx context.Context,
	query string,
	documents []schema.Document,
) ([]float32, error) {
	// Extract text from documents using field or template
	documentTexts, err := ExtractDocumentTexts(documents, o.field, o.template)
	if err != nil {
		return nil, fmt.Errorf("extracting document texts: %w", err)
	}

	// Create prompts for reranking by combining query and document
	prompts := make([]string, len(documentTexts))
	for i, doc := range documentTexts {
		prompts[i] = fmt.Sprintf("Query: %s\n\nDocument: %s\n\nRelevance:", query, doc)
	}

	// Get embeddings for the combined prompt of queries and documents
	embeddings, err := o.embed(ctx, prompts)
	if err != nil {
		return nil, fmt.Errorf("getting reranking embeddings: %w", err)
	}

	if len(embeddings) != len(documents) {
		return nil, fmt.Errorf(
			"expected %d reranking embeddings, got %d",
			len(documents),
			len(embeddings),
		)
	}

	// Calculate relevance scores from embeddings
	scores := make([]float32, len(documents))
	for i, emb := range embeddings {
		scores[i] = calculateRelevanceScore(emb)
	}

	return scores, nil
}

// embed calls Ollama to generate embeddings for the given values
func (o *OllamaReranker) embed(ctx context.Context, values []string) ([][]float32, error) {
	if len(values) == 0 {
		return [][]float32{}, nil
	}

	req := &ollamaapi.EmbedRequest{
		Model: o.embeddingModel,
		Input: values,
	}
	resp, err := o.client.Embed(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ollama embed request failed: %w", err)
	}
	if len(resp.Embeddings) != len(values) {
		return nil, fmt.Errorf("expected %d embeddings, got %d", len(values), len(resp.Embeddings))
	}
	return resp.Embeddings, nil
}

// calculateRelevanceScore computes a relevance score from an embedding vector
// This is a fake reranking implementation using embedding similarity
func calculateRelevanceScore(embedding []float32) float32 {
	// Simple scoring based on embedding magnitude and positive values
	var sumPositive float32
	for _, val := range embedding {
		if val > 0 {
			sumPositive += val
		}
	}
	// Normalize and combine magnitude with positive bias
	return (vec.L2DistanceFloat32(embedding, embedding) + sumPositive) / float32(2*len(embedding))
}
