// Copyright 2026 Antfly, Inc.
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

package chunking

import (
	"context"
	"fmt"
	"net/http"
	"time"

	libtermite "github.com/antflydb/antfly/go/pkg/antfly/lib/termite"
	client "github.com/antflydb/antfly/go/pkg/sdk"
)

// AntflyProvider is a client for the Antfly inference chunking service.
type AntflyProvider struct {
	client *client.InferenceClient
	config AntflyChunkerConfig
}

// NewAntflyProvider creates a new Antfly inference chunking client.
func NewAntflyProvider(config ChunkerConfig) (Chunker, error) {
	// Extract inference config
	antflyConfig, err := config.AsAntflyChunkerConfig()
	if err != nil {
		return nil, err
	}

	// Get API URL from config, env var, or global default
	apiURL := libtermite.ResolveURL(antflyConfig.ApiUrl)
	if apiURL == "" {
		return nil, fmt.Errorf("antfly inference API URL must be set via api_url config field or ANTFLY_INFERENCE_URL environment variable")
	}

	// Set defaults for zero-value fields
	if antflyConfig.Text.TargetTokens == 0 {
		antflyConfig.Text.TargetTokens = 500
	}
	if antflyConfig.Text.OverlapTokens == 0 {
		antflyConfig.Text.OverlapTokens = 50
	}
	if antflyConfig.MaxChunks == 0 {
		antflyConfig.MaxChunks = 50
	}
	if antflyConfig.Text.Separator == "" {
		antflyConfig.Text.Separator = "\n\n"
	}
	if antflyConfig.Threshold == 0 {
		antflyConfig.Threshold = 0.5
	}

	httpClient := &http.Client{
		Timeout: 5 * time.Minute, // Increased from 30s to 5min for large document chunking
	}
	termiteClient, err := client.NewInferenceClient(apiURL, httpClient)
	if err != nil {
		return nil, fmt.Errorf("creating inference client: %w", err)
	}

	return &AntflyProvider{
		client: termiteClient,
		config: antflyConfig,
	}, nil
}

// Chunk performs text chunking via the Antfly inference service.
func (tp *AntflyProvider) Chunk(ctx context.Context, text string) ([]Chunk, error) {
	if text == "" {
		return nil, nil
	}

	// The Chunk type in . is an alias to libaf/chunking.Chunk,
	// so no conversion is needed - just return the chunks directly.
	return tp.client.Chunk(ctx, text, client.ChunkConfig{
		Model:         tp.config.Model,
		TargetTokens:  tp.config.Text.TargetTokens,
		OverlapTokens: tp.config.Text.OverlapTokens,
		Separator:     tp.config.Text.Separator,
		MaxChunks:     tp.config.MaxChunks,
		Threshold:     tp.config.Threshold,
	})
}

// ChunkMedia performs media chunking via the Antfly inference service.
func (tp *AntflyProvider) ChunkMedia(ctx context.Context, data []byte, mimeType string) ([]Chunk, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return tp.client.ChunkMedia(ctx, data, mimeType, client.MediaChunkConfig{
		MaxChunks: tp.config.MaxChunks,
	})
}

// Close implements the Chunker interface (no cleanup needed for HTTP client)
func (tp *AntflyProvider) Close() error {
	return nil
}

// Register Antfly inference provider in the registry.
func init() {
	ChunkerRegistry[ChunkerProviderAntfly] = NewAntflyProvider
}
