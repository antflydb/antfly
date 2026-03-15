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

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package chunking

import (
	"context"
	"fmt"

	externalRef0 "github.com/antflydb/antfly/pkg/libaf/chunking"
)

// NewTextChunk creates a Chunk containing text content with the given parameters.
// Re-exported from libaf/chunking for convenience.
var NewTextChunk = externalRef0.NewTextChunk

// TextChunkOptions is re-exported from libaf/chunking for convenience.
type TextChunkOptions = externalRef0.TextChunkOptions

// AudioChunkOptions is re-exported from libaf/chunking for convenience.
type AudioChunkOptions = externalRef0.AudioChunkOptions

// Chunker splits text into semantically meaningful chunks
type Chunker interface {
	Chunk(ctx context.Context, text string) ([]Chunk, error)
	ChunkMedia(ctx context.Context, data []byte, mimeType string) ([]Chunk, error)
	Close() error
}

// ChunkerRegistry maps provider names to constructor functions
var ChunkerRegistry = map[ChunkerProvider]func(config ChunkerConfig) (Chunker, error){}

// NewChunker creates a new chunker instance based on the provider configuration
func NewChunker(config ChunkerConfig) (Chunker, error) {
	if config.Provider == "" {
		return nil, fmt.Errorf("provider is required")
	}

	constructor, ok := ChunkerRegistry[config.Provider]
	if !ok {
		return nil, fmt.Errorf("unknown chunker provider: %s", config.Provider)
	}

	return constructor(config)
}

// defaultChunkerConfig is the default chunker configuration, set from config at startup.
var defaultChunkerConfig *ChunkerConfig

// SetDefaultChunkerConfig sets the default chunker configuration.
// This should be called during config initialization.
func SetDefaultChunkerConfig(config *ChunkerConfig) {
	defaultChunkerConfig = config
}

// GetDefaultChunkerConfig returns the current default chunker configuration.
func GetDefaultChunkerConfig() *ChunkerConfig {
	return defaultChunkerConfig
}
