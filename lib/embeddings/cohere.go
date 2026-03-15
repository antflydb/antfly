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

package embeddings

import (
	"context"
	"fmt"
	"os"

	cohere "github.com/cohere-ai/cohere-go/v2"
	cohereclient "github.com/cohere-ai/cohere-go/v2/client"
	"golang.org/x/time/rate"

	"github.com/antflydb/antfly/lib/ai"
)

// CohereEmbedder implements the Embedder interface for Cohere
type CohereEmbedder struct {
	client    *cohereclient.Client
	model     string
	inputType cohere.EmbedInputType
	truncate  *cohere.EmbedRequestTruncate
	caps      EmbedderCapabilities
	limiter   *rate.Limiter
}

func init() {
	RegisterEmbedder(EmbedderProviderCohere, NewCohereEmbedder)
}

// NewCohereEmbedder creates a new Cohere embedder from configuration
func NewCohereEmbedder(config EmbedderConfig) (Embedder, error) {
	c, err := config.AsCohereEmbedderConfig()
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

	// Parse input type
	inputType := cohere.EmbedInputTypeSearchDocument
	if c.InputType != nil {
		switch *c.InputType {
		case CohereEmbedderConfigInputTypeSearchDocument:
			inputType = cohere.EmbedInputTypeSearchDocument
		case CohereEmbedderConfigInputTypeSearchQuery:
			inputType = cohere.EmbedInputTypeSearchQuery
		case CohereEmbedderConfigInputTypeClassification:
			inputType = cohere.EmbedInputTypeClassification
		case CohereEmbedderConfigInputTypeClustering:
			inputType = cohere.EmbedInputTypeClustering
		}
	}

	// Parse truncate option
	var truncate *cohere.EmbedRequestTruncate
	if c.Truncate != nil {
		switch *c.Truncate {
		case CohereEmbedderConfigTruncateNONE:
			t := cohere.EmbedRequestTruncateNone
			truncate = &t
		case CohereEmbedderConfigTruncateSTART:
			t := cohere.EmbedRequestTruncateStart
			truncate = &t
		case CohereEmbedderConfigTruncateEND:
			t := cohere.EmbedRequestTruncateEnd
			truncate = &t
		}
	}

	// Default model if not specified
	model := c.Model
	if model == "" {
		model = "embed-english-v3.0"
	}

	return &CohereEmbedder{
		client:    client,
		model:     model,
		inputType: inputType,
		truncate:  truncate,
		caps:      ResolveCapabilities(model, nil),
		limiter:   rate.NewLimiter(CohereDefaultRPS, CohereDefaultRPS),
	}, nil
}

func (e *CohereEmbedder) Capabilities() EmbedderCapabilities {
	return e.caps
}

func (e *CohereEmbedder) RateLimiter() *rate.Limiter {
	return e.limiter
}

func (e *CohereEmbedder) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	// Cohere only supports text embeddings, extract text from content parts
	texts := ExtractText(contents)

	req := &cohere.EmbedRequest{
		Texts:     texts,
		Model:     &e.model,
		InputType: &e.inputType,
		Truncate:  e.truncate,
	}

	resp, err := e.client.Embed(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cohere embed request failed: %w", err)
	}

	// Extract embeddings from response
	if resp.EmbeddingsFloats == nil || len(resp.EmbeddingsFloats.Embeddings) == 0 {
		return nil, fmt.Errorf("cohere returned no embeddings")
	}

	embeddings := make([][]float32, len(resp.EmbeddingsFloats.Embeddings))
	for i, emb := range resp.EmbeddingsFloats.Embeddings {
		// Convert []float64 to []float32
		embeddings[i] = make([]float32, len(emb))
		for j, v := range emb {
			embeddings[i][j] = float32(v)
		}
	}

	return embeddings, nil
}
