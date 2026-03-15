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
	"net/http"
	"net/url"

	"github.com/antflydb/antfly/lib/ai"
	ollamaapi "github.com/ollama/ollama/api"
	"golang.org/x/time/rate"
)

type OllamaImpl struct {
	client         *ollamaapi.Client
	embeddingModel string
	caps           EmbedderCapabilities
}

func (o *OllamaImpl) GetModels(ctx context.Context) ([]string, error) {
	resp, err := o.client.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("ollama list models failed: %w", err)
	}
	models := make([]string, len(resp.Models))
	for i, m := range resp.Models {
		models[i] = m.Name
	}
	return models, nil
}

func NewOllamaEmbedderImpl(config EmbedderConfig) (Embedder, error) {
	// Validate config
	c, err := config.AsOllamaEmbedderConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing ollama config: %w", err)
	}

	var client *ollamaapi.Client

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

	return &OllamaImpl{
		client:         client,
		embeddingModel: c.Model,
		caps:           ResolveCapabilities(c.Model, nil),
	}, nil
}

func (o *OllamaImpl) Capabilities() EmbedderCapabilities {
	return o.caps
}

func (o *OllamaImpl) RateLimiter() *rate.Limiter {
	return nil // local inference, no rate limit
}

func (o *OllamaImpl) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	// Ollama only supports text embeddings
	values := ExtractText(contents)

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
