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

	"github.com/antflydb/antfly/lib/ai"
	"github.com/revrost/go-openrouter"
	"golang.org/x/time/rate"
)

// OpenRouterImpl implements embeddings using OpenRouter's API.
// OpenRouter provides a unified API for multiple embedding providers.
type OpenRouterImpl struct {
	client     *openrouter.Client
	model      string
	dimensions *int
	caps       EmbedderCapabilities
	limiter    *rate.Limiter
}

func init() {
	RegisterEmbedder(EmbedderProviderOpenrouter, NewOpenRouterImpl)
}

// NewOpenRouterImpl creates a new OpenRouter embedder from configuration.
func NewOpenRouterImpl(config EmbedderConfig) (Embedder, error) {
	c, err := config.AsOpenRouterEmbedderConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Set API key from config or environment variable
	var apiKey string
	if c.ApiKey != nil && *c.ApiKey != "" {
		apiKey = *c.ApiKey
	} else {
		apiKey = os.Getenv("OPENROUTER_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("OpenRouter API key not provided (set api_key in config or OPENROUTER_API_KEY env var)")
	}

	client := openrouter.NewClient(apiKey)

	// Resolve capabilities for the model
	// OpenRouter model names are prefixed with provider, e.g., "openai/text-embedding-3-small"
	caps := ResolveCapabilities(c.Model, config.GetConfigCapabilities())

	return &OpenRouterImpl{
		client:     client,
		model:      c.Model,
		dimensions: c.Dimensions,
		caps:       caps,
		limiter:    rate.NewLimiter(OpenRouterDefaultRPS, OpenRouterDefaultRPS),
	}, nil
}

func (l *OpenRouterImpl) Capabilities() EmbedderCapabilities {
	return l.caps
}

func (l *OpenRouterImpl) RateLimiter() *rate.Limiter {
	return l.limiter
}

func (l *OpenRouterImpl) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	// OpenRouter only supports text embeddings, extract text from content parts
	values := ExtractText(contents)

	req := openrouter.EmbeddingsRequest{
		Model:      l.model,
		Input:      values,
		Dimensions: l.dimensions,
	}

	resp, err := l.client.CreateEmbeddings(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("creating embeddings via OpenRouter: %w", err)
	}

	embeddings := make([][]float32, len(resp.Data))
	for i, data := range resp.Data {
		// Convert float64 vector to float32
		emb := make([]float32, len(data.Embedding.Vector))
		for j, v := range data.Embedding.Vector {
			emb[j] = float32(v)
		}
		embeddings[i] = emb
	}
	return embeddings, nil
}
