// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package termite

import (
	"context"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/libaf/reranking"
	"github.com/antflydb/antfly/pkg/termite/lib/classification"
	"github.com/antflydb/antfly/pkg/termite/lib/generation"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/antflydb/antfly/pkg/termite/lib/ner"
	"github.com/antflydb/antfly/pkg/termite/lib/reading"
	"github.com/antflydb/antfly/pkg/termite/lib/seq2seq"
	"github.com/antflydb/antfly/pkg/termite/lib/transcribing"
)

// EmbedderRegistryInterface defines the interface for embedder model registries.
// This enables testing with mock implementations.
type EmbedderRegistryInterface interface {
	// Acquire retrieves an embedder and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (embeddings.Embedder, error)
	// AcquireSparse retrieves a sparse embedder and increments reference count.
	// Only valid for models with the "sparse" capability.
	// Caller MUST call Release when done.
	AcquireSparse(modelName string) (embeddings.SparseEmbedder, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns all available model names
	List() []string
	// ListWithCapabilities returns a map of model name to capabilities
	ListWithCapabilities() map[string][]string
	// HasCapability checks if a model has a specific capability (e.g., image, audio, sparse)
	HasCapability(modelName string, capability modelregistry.Capability) bool
	// Close shuts down the registry and releases resources
	Close() error
}

// RerankerRegistryInterface defines the interface for reranker model registries.
// This enables testing with mock implementations.
type RerankerRegistryInterface interface {
	// Acquire retrieves a reranker and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (reranking.Model, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns all available model names
	List() []string
	// Close shuts down the registry and releases resources
	Close() error
}

// NERRegistryInterface defines the interface for NER model registries.
// This enables testing with mock implementations.
type NERRegistryInterface interface {
	// Get retrieves a NER model by name, loading it if necessary
	Get(modelName string) (ner.Model, error)
	// Acquire retrieves a NER model and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (ner.Model, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns a map of model names to their capabilities
	List() map[string][]string
	// HasCapability checks if a model has a specific capability
	HasCapability(modelName string, capability modelregistry.Capability) bool
	// Close shuts down the registry and releases resources
	Close() error
}

// GeneratorRegistryInterface defines the interface for generator model registries.
// This enables testing with mock implementations.
type GeneratorRegistryInterface interface {
	// Acquire retrieves a generator and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (generation.Generator, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns all available model names
	List() []string
	// Close shuts down the registry and releases resources
	Close() error
}

// Seq2SeqRegistryInterface defines the interface for seq2seq model registries.
// This enables testing with mock implementations.
type Seq2SeqRegistryInterface interface {
	// Acquire retrieves a seq2seq model and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (seq2seq.Model, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns all available model names
	List() []string
	// Close shuts down the registry and releases resources
	Close() error
}

// ClassifierRegistryInterface defines the interface for classifier model registries.
// This enables testing with mock implementations.
type ClassifierRegistryInterface interface {
	// Acquire retrieves a classifier and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (classification.Classifier, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns all available model names
	List() []string
	// Close shuts down the registry and releases resources
	Close() error
}

// ReaderRegistryInterface defines the interface for reader model registries.
// This enables testing with mock implementations.
type ReaderRegistryInterface interface {
	// Acquire retrieves a reader and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (reading.Reader, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns all available model names
	List() []string
	// ListWithCapabilities returns a map of model name to capabilities
	ListWithCapabilities() map[string][]string
	// Close shuts down the registry and releases resources
	Close() error
}

// TranscriberRegistryInterface defines the interface for transcriber model registries.
// This enables testing with mock implementations.
type TranscriberRegistryInterface interface {
	// Acquire retrieves a transcriber and increments reference count to prevent eviction.
	// Caller MUST call Release when done.
	Acquire(modelName string) (transcribing.Transcriber, error)
	// Release decrements reference count, allowing the model to be evicted.
	Release(modelName string)
	// List returns all available model names
	List() []string
	// Close shuts down the registry and releases resources
	Close() error
}

// ChunkerInterface defines the interface for chunking services.
// It handles both text chunking and media chunking (audio, etc.) through a unified interface.
// This enables testing with mock implementations.
type ChunkerInterface interface {
	// Chunk splits text into chunks using the specified configuration
	// Returns chunks, cache hit status, and any error
	Chunk(ctx context.Context, text string, config chunkConfig) ([]chunking.Chunk, bool, error)
	// ChunkMedia splits binary media content into chunks, using a model-based chunker
	// if available, or falling back to algorithmic chunking.
	ChunkMedia(ctx context.Context, data []byte, mimeType, model string, opts chunking.ChunkOptions) ([]chunking.Chunk, error)
	// ListModels returns all available chunker model names
	ListModels() []string
	// ListWithCapabilities returns a map of model name to capabilities
	ListWithCapabilities() map[string][]string
	// HasCapability checks if a model has a specific capability (e.g., audio)
	HasCapability(modelName string, capability modelregistry.Capability) bool
	// Close shuts down the chunker and releases resources
	Close() error
}

// Ensure concrete types implement the interfaces
var (
	_ EmbedderRegistryInterface    = (*EmbedderRegistry)(nil)
	_ RerankerRegistryInterface    = (*RerankerRegistry)(nil)
	_ NERRegistryInterface         = (*NERRegistry)(nil)
	_ GeneratorRegistryInterface   = (*GeneratorRegistry)(nil)
	_ Seq2SeqRegistryInterface     = (*Seq2SeqRegistry)(nil)
	_ ClassifierRegistryInterface  = (*ClassifierRegistry)(nil)
	_ ReaderRegistryInterface      = (*ReaderRegistry)(nil)
	_ TranscriberRegistryInterface = (*TranscriberRegistry)(nil)
	_ ChunkerInterface             = (*CachedChunker)(nil)
)
