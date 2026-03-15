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

package indexes

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

type Index interface {
	Name() string
	Type() IndexType
	Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte, sync bool) error
	Search(ctx context.Context, query any) (any, error)
	Close() error
	Delete() error
	// This doesn't need to be pebble just something ordered
	Open(rebuild bool, schema *schema.TableSchema, byteRange types.Range) error
	Stats() IndexStats

	// FIXME (ajr) Need to update the index if the byte range changes
	UpdateRange(byteRange types.Range) error
	UpdateSchema(schema *schema.TableSchema) error

	// Pause/Resume support for snapshot safety
	Pause(ctx context.Context) error
	Resume()
}

type EnrichableIndex interface {
	Index
	NeedsEnricher() bool
	LeaderFactory(ctx context.Context, persistEmbeddings PersistFunc) error
}

// EnrichmentComputer is implemented by indexes that can compute enrichments synchronously
type EnrichmentComputer interface {
	// ComputeEnrichments generates embeddings/summaries/chunks synchronously before Raft proposal
	// This is called by dbWrapper.preEnrichBatch() when sync_level is "enrichments" or "aknn"
	//
	// Returns writes for ALL enrichments:
	//  - Embeddings: key:i:<name>:e -> [hashID:uint64][embedding_vector]
	//  - Summaries: key:i:<name>:s -> [hashID:uint64][summary_text]
	//  - Chunks: key:i:<name>:c:<chunkID> -> [hashID:uint64][chunk_json]
	//
	// Also returns failedKeys that need async enrichment after persistence.
	// Failed keys are NOT queued here - they will be automatically picked up by the async
	// enrichment pipeline when indexManager.Batch() runs after chunks are persisted.
	//
	// All returned writes are included in the same Raft batch as the original document writes,
	// avoiding nested Raft proposals and ensuring atomic commits.
	//
	// Should only be called on the leader node with an active enricher.
	// Returns error if enrichment generation fails (e.g., timeout, API error).
	ComputeEnrichments(ctx context.Context, writes [][2][]byte) (enrichmentWrites [][2][]byte, failedKeys [][]byte, err error)
}

type BackfillableIndex interface {
	Index
	WaitForBackfill(ctx context.Context)
}

// EmbeddingsPreProcessor is implemented by indexes that support direct embedding writes
type EmbeddingsPreProcessor interface {
	Index

	// GetDimension returns the expected embedding dimension
	GetDimension() int

	// RenderPrompt extracts and renders the prompt from a document,
	// returning the prompt text and its hashID
	RenderPrompt(doc map[string]any) (prompt string, hashID uint64, err error)
}

type PersistFunc func(ctx context.Context, writes [][2][]byte) error

func RegisterIndex(t IndexType, fn NewIndexFunc) {
	indexRegistry[t] = fn
}

func MakeIndex(
	logger *zap.Logger,
	antflyConfig *common.Config,
	db *pebble.DB,
	dir string,
	name string,
	config *IndexConfig,
	cache *pebbleutils.Cache,
) (Index, error) {
	if fn, ok := indexRegistry[config.Type]; ok {
		return fn(logger, antflyConfig, db, dir, name, config, cache)
	}
	return nil, fmt.Errorf("unsupported index type: %v", config.Type)
}

// NewEmbeddingsIndex dispatches to the dense or sparse constructor based
// on the Sparse flag in the config.
func NewEmbeddingsIndex(logger *zap.Logger, antflyConfig *common.Config, db *pebble.DB, dir string, name string, config *IndexConfig, cache *pebbleutils.Cache) (Index, error) {
	cfg, err := config.AsEmbeddingsIndexConfig()
	if err != nil {
		return nil, fmt.Errorf("reading embeddings config: %w", err)
	}
	if cfg.Sparse {
		return NewSparseIndex(logger, antflyConfig, db, dir, name, config, cache)
	}
	return NewEmbeddingIndex(logger, antflyConfig, db, dir, name, config, cache)
}

func init() {
	RegisterIndex(IndexTypeEmbeddings, NewEmbeddingsIndex)
}

type NewIndexFunc func(logger *zap.Logger, antflyConfig *common.Config, db *pebble.DB, dir string, name string, config *IndexConfig, cache *pebbleutils.Cache) (Index, error)

var indexRegistry = map[IndexType]NewIndexFunc{}

// IsEmbeddingsType returns true if the IndexType is any embeddings variant (new or legacy).
func IsEmbeddingsType(t IndexType) bool {
	return t == IndexTypeEmbeddings || t == IndexTypeAknnV0
}

// IsFullTextType returns true if the IndexType is any full-text variant (new or legacy).
func IsFullTextType(t IndexType) bool {
	return t == IndexTypeFullText || t == IndexTypeFullTextV0
}

// IsGraphType returns true if the IndexType is any graph variant (new or legacy).
func IsGraphType(t IndexType) bool {
	return t == IndexTypeGraph || t == IndexTypeGraphV0
}

// NormalizeIndexType converts legacy type names to their canonical forms.
func NormalizeIndexType(t IndexType) IndexType {
	switch t {
	case IndexTypeFullTextV0:
		return IndexTypeFullText
	case IndexTypeAknnV0:
		return IndexTypeEmbeddings
	case IndexTypeGraphV0:
		return IndexTypeGraph
	default:
		return t
	}
}
