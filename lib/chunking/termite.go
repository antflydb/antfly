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

package chunking

import (
	"context"
	"fmt"
	"net/http"
	"time"

	libtermite "github.com/antflydb/antfly/lib/termite"
	"github.com/antflydb/antfly/pkg/termite-client"
)

// TermiteProvider is a client for the Termite chunking service
type TermiteProvider struct {
	client *client.TermiteClient
	config TermiteChunkerConfig
}

// NewTermiteProvider creates a new Termite chunking client
func NewTermiteProvider(config ChunkerConfig) (Chunker, error) {
	// Extract termite config
	termiteConfig, err := config.AsTermiteChunkerConfig()
	if err != nil {
		return nil, err
	}

	// Get API URL from config, env var, or global default
	apiURL := libtermite.ResolveURL(termiteConfig.ApiUrl)
	if apiURL == "" {
		return nil, fmt.Errorf("termite API URL must be set via api_url config field or ANTFLY_TERMITE_URL environment variable")
	}

	// Set defaults for zero-value fields
	if termiteConfig.Text.TargetTokens == 0 {
		termiteConfig.Text.TargetTokens = 500
	}
	if termiteConfig.Text.OverlapTokens == 0 {
		termiteConfig.Text.OverlapTokens = 50
	}
	if termiteConfig.MaxChunks == 0 {
		termiteConfig.MaxChunks = 50
	}
	if termiteConfig.Text.Separator == "" {
		termiteConfig.Text.Separator = "\n\n"
	}
	if termiteConfig.Threshold == 0 {
		termiteConfig.Threshold = 0.5
	}

	httpClient := &http.Client{
		Timeout: 5 * time.Minute, // Increased from 30s to 5min for large document chunking
	}
	termiteClient, err := client.NewTermiteClient(apiURL, httpClient)
	if err != nil {
		return nil, fmt.Errorf("creating termite client: %w", err)
	}

	return &TermiteProvider{
		client: termiteClient,
		config: termiteConfig,
	}, nil
}

// Chunk performs text chunking via the Termite service
func (tp *TermiteProvider) Chunk(ctx context.Context, text string) ([]Chunk, error) {
	if text == "" {
		return nil, nil
	}

	// The Chunk type in lib/chunking is an alias to libaf/chunking.Chunk,
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

// ChunkMedia performs media chunking via the Termite service
func (tp *TermiteProvider) ChunkMedia(ctx context.Context, data []byte, mimeType string) ([]Chunk, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return tp.client.ChunkMedia(ctx, data, mimeType, client.MediaChunkConfig{
		MaxChunks: tp.config.MaxChunks,
	})
}

// Close implements the Chunker interface (no cleanup needed for HTTP client)
func (tp *TermiteProvider) Close() error {
	return nil
}

// Register termite provider in the registry
func init() {
	ChunkerRegistry[ChunkerProviderTermite] = NewTermiteProvider
}
