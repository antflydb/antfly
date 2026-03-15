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
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/antflydb/antfly/lib/ai"
	"golang.org/x/time/rate"
	"google.golang.org/genai"
)

type GenaiGoogleImpl struct {
	client            *genai.Client
	embeddingModel    string
	dimension         uint64
	location, project string
	caps              EmbedderCapabilities
	limiter           *rate.Limiter
}

func NewGenaiGoogleImpl(config EmbedderConfig) (Embedder, error) {
	// Validate config
	c, err := config.AsGoogleEmbedderConfig()
	if err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	var dimension uint64
	if c.Dimension != nil {
		dimension = uint64(*c.Dimension) //nolint:gosec // G115: bounded value, cannot overflow in practice
	}
	apiKey := os.Getenv("GEMINI_API_KEY")
	if c.ApiKey != nil && *c.ApiKey != "" {
		apiKey = *c.ApiKey
	}

	// Can also be set via the GOOGLE_CLOUD_PROJECT environment variable.
	project := os.Getenv("VERTEXAI_PROJECT")
	// Can also be set via the GOOGLE_CLOUD_LOCATION or GOOGLE_CLOUD_REGION environment variable.
	location := os.Getenv("VERTEXAI_LOCATION")
	// Optional. Backend for GenAI. See Backend constants. Defaults to BackendGeminiAPI unless explicitly set to BackendVertexAI,
	// or the environment variable GOOGLE_GENAI_USE_VERTEXAI is set to "1" or "true".
	backendEnv := os.Getenv("GOOGLE_GENAI_USE_VERTEXAI")
	backend := genai.BackendGeminiAPI
	if backendEnv != "" {
		backend = genai.BackendVertexAI
	}
	cc := genai.ClientConfig{
		Backend: backend,
	}
	// API Key and Project are mutually exclusive in the GenAI SDK:
	// - Vertex AI backend uses Project + Location (ADC credentials)
	// - Gemini API backend uses API Key
	if backend == genai.BackendVertexAI {
		cc.Project = project
		cc.Location = location
	} else {
		cc.APIKey = apiKey
	}
	client, err := genai.NewClient(context.Background(), &cc)
	if err != nil {
		return nil, fmt.Errorf("creating genai client: %w", err)
	}
	return &GenaiGoogleImpl{
		client:         client,
		embeddingModel: c.Model,
		dimension:      dimension,
		project:        project,
		location:       location,
		caps:           ResolveCapabilities(c.Model, nil),
		limiter:        rate.NewLimiter(GeminiDefaultRPS, GeminiDefaultRPS),
	}, nil
}

func (l *GenaiGoogleImpl) Capabilities() EmbedderCapabilities {
	return l.caps
}

func (l *GenaiGoogleImpl) RateLimiter() *rate.Limiter {
	return l.limiter
}

func (l *GenaiGoogleImpl) Embed(ctx context.Context, contentParts [][]ai.ContentPart) ([][]float32, error) {
	if len(contentParts) == 0 {
		return [][]float32{}, nil
	}

	// Gemini only supports text embeddings
	values := ExtractText(contentParts)

	contents := make([]*genai.Content, len(values))
	for i, v := range values {
		contents[i] = genai.NewContentFromText(v, "" /* genai.Role */)
	}
	var cc *genai.EmbedContentConfig
	if l.dimension > 0 {
		d := int32(l.dimension) //nolint:gosec // G115: bounded value, cannot overflow in practice
		cc = &genai.EmbedContentConfig{OutputDimensionality: &d}
	}
	result, err := l.client.Models.EmbedContent(ctx,
		l.embeddingModel,
		contents,
		cc,
	)
	if err != nil {
		return nil, fmt.Errorf("embedding content: %w", err)
	}
	if result == nil || len(result.Embeddings) == 0 {
		return nil, errors.New("no embeddings returned")
	}
	emb := make([][]float32, len(result.Embeddings))
	for i, e := range result.Embeddings {
		emb[i] = e.Values
	}
	return emb, err
}

func (l *GenaiGoogleImpl) GetModels(ctx context.Context) ([]string, error) {
	resp, err := l.client.Models.List(ctx, &genai.ListModelsConfig{
		// FIXME (ajr) Filter for embedding models,
		// Filter: "",
	})
	if err != nil {
		return nil, fmt.Errorf("ollama list models failed: %w", err)
	}
	models := make([]string, len(resp.Items))
	// TODO (ajr) Use resp.Next with paging?
	for i, m := range resp.Items {
		if slices.Contains(m.SupportedActions, "embedContent") {
			models[i] = m.Name
		}
	}
	return models, nil
}
