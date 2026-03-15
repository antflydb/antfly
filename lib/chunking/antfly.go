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

	libafchunking "github.com/antflydb/antfly/pkg/libaf/chunking"
	termchunking "github.com/antflydb/termite/pkg/termite/lib/chunking"
)

// AntflyProvider provides local chunking without requiring an external Termite service
type AntflyProvider struct {
	chunker      libafchunking.Chunker
	mediaChunker *termchunking.FixedMediaChunker
	config       AntflyChunkerConfig
}

// NewAntflyProvider creates a new local Antfly chunker
func NewAntflyProvider(config ChunkerConfig) (Chunker, error) {
	// Extract antfly config
	antflyConfig, err := config.AsAntflyChunkerConfig()
	if err != nil {
		return nil, err
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

	// Convert AntflyChunkerConfig to termite's FixedChunkerConfig
	termiteConfig := termchunking.FixedChunkerConfig{
		Model:         termchunking.ModelFixedBert,
		TargetTokens:  antflyConfig.Text.TargetTokens,
		OverlapTokens: antflyConfig.Text.OverlapTokens,
		Separator:     antflyConfig.Text.Separator,
		MaxChunks:     antflyConfig.MaxChunks,
	}

	// Create the underlying fixed chunker from termite
	fixedChunker, err := termchunking.NewFixedChunker(termiteConfig)
	if err != nil {
		return nil, err
	}

	return &AntflyProvider{
		chunker:      fixedChunker,
		mediaChunker: termchunking.NewFixedMediaChunker(),
		config:       antflyConfig,
	}, nil
}

// Chunk delegates to the underlying fixed chunker
func (ap *AntflyProvider) Chunk(ctx context.Context, text string) ([]Chunk, error) {
	// Build ChunkOptions from stored config
	opts := libafchunking.ChunkOptions{
		MaxChunks: ap.config.MaxChunks,
		Text: libafchunking.TextChunkOptions{
			TargetTokens: ap.config.Text.TargetTokens,
		},
	}

	// Call termite chunker
	libafChunks, err := ap.chunker.Chunk(ctx, text, opts)
	if err != nil {
		return nil, err
	}

	// Return nil if no chunks (preserves nil semantics for empty input)
	if libafChunks == nil {
		return nil, nil
	}

	// Convert libaf chunks to local Chunk type
	chunks := make([]Chunk, len(libafChunks))
	for i, c := range libafChunks {
		chunks[i] = Chunk(c)
	}

	return chunks, nil
}

// ChunkMedia delegates to the media chunker for binary content
func (ap *AntflyProvider) ChunkMedia(ctx context.Context, data []byte, mimeType string) ([]Chunk, error) {
	opts := libafchunking.ChunkOptions{
		MaxChunks: ap.config.MaxChunks,
	}
	return ap.mediaChunker.ChunkMedia(ctx, data, mimeType, opts)
}

// Close releases resources
func (ap *AntflyProvider) Close() error {
	return ap.chunker.Close()
}

// Register antfly provider in the registry
func init() {
	ChunkerRegistry[ChunkerProviderAntfly] = NewAntflyProvider
}
