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
	"fmt"
	"os"
	"strings"

	cohere "github.com/cohere-ai/cohere-go/v2"
	cohereclient "github.com/cohere-ai/cohere-go/v2/client"
)

// CohereGenerator implements DocumentSummarizer using Cohere's Chat API
type CohereGenerator struct {
	client           *cohereclient.Client
	model            string
	temperature      *float64
	maxTokens        *int
	topP             *float64
	topK             *int
	frequencyPenalty *float64
	presencePenalty  *float64
}

func init() {
	RegisterDocumentSummarizer(GeneratorProviderCohere,
		func(ctx context.Context, config GeneratorConfig) (DocumentSummarizer, error) {
			return NewCohereGenerator(ctx, config)
		})
}

// NewCohereGenerator creates a new Cohere generator from configuration
func NewCohereGenerator(ctx context.Context, config GeneratorConfig) (*CohereGenerator, error) {
	c, err := config.AsCohereGeneratorConfig()
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
		model = "command-r-plus"
	}

	gen := &CohereGenerator{
		client: client,
		model:  model,
	}

	// Set optional parameters
	if c.Temperature != nil {
		temp := float64(*c.Temperature)
		gen.temperature = &temp
	}

	if c.MaxTokens != nil {
		gen.maxTokens = c.MaxTokens
	}

	if c.TopP != nil {
		topP := float64(*c.TopP)
		gen.topP = &topP
	}

	if c.TopK != nil {
		gen.topK = c.TopK
	}

	if c.FrequencyPenalty != nil {
		fp := float64(*c.FrequencyPenalty)
		gen.frequencyPenalty = &fp
	}

	if c.PresencePenalty != nil {
		pp := float64(*c.PresencePenalty)
		gen.presencePenalty = &pp
	}

	return gen, nil
}

// SummarizeRenderedDocs implements DocumentSummarizer interface
func (g *CohereGenerator) SummarizeRenderedDocs(
	ctx context.Context,
	renderedDocs []string,
	opts ...GenerateOption,
) ([]string, error) {
	if len(renderedDocs) == 0 {
		return []string{}, nil
	}

	// Apply options
	options := CollectGenerateOptions(opts...)

	summaries := make([]string, len(renderedDocs))

	for i, doc := range renderedDocs {
		summary, err := g.generateOne(ctx, doc, &options)
		if err != nil {
			return nil, fmt.Errorf("summarizing document %d: %w", i, err)
		}
		summaries[i] = summary
	}

	return summaries, nil
}

func (g *CohereGenerator) generateOne(
	ctx context.Context,
	doc string,
	options *GenerateOptions,
) (string, error) {
	// Build the prompt
	prompt := doc
	if options.GetSystemPrompt() != "" {
		prompt = options.GetSystemPrompt() + "\n\n" + doc
	}

	req := &cohere.ChatRequest{
		Message: prompt,
		Model:   &g.model,
	}

	// Apply generation parameters
	if g.temperature != nil {
		req.Temperature = g.temperature
	}

	if g.maxTokens != nil {
		req.MaxTokens = g.maxTokens
	}

	if g.topP != nil {
		req.P = g.topP
	}

	if g.topK != nil {
		req.K = g.topK
	}

	if g.frequencyPenalty != nil {
		req.FrequencyPenalty = g.frequencyPenalty
	}

	if g.presencePenalty != nil {
		req.PresencePenalty = g.presencePenalty
	}

	resp, err := g.client.Chat(ctx, req)
	if err != nil {
		return "", fmt.Errorf("cohere chat request failed: %w", err)
	}

	return strings.TrimSpace(resp.Text), nil
}
