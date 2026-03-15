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

package metadata

import (
	"context"
	"fmt"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/src/store/db/indexes"
)

// validateEmbedderConfig validates an embedder (and optional summarizer) by
// instantiating the plugin and running a test embedding. For dense indexes it
// also infers the vector dimension when not explicitly set.
//
// The indexLabel is used in error messages (e.g. `index "my_index"`).
func validateEmbedderConfig(ctx context.Context, embeddingsConfig *indexes.EmbeddingsIndexConfig, indexLabel string) error {
	modelConfig := *embeddingsConfig.Embedder
	if err := modelConfig.Validate(); err != nil {
		return fmt.Errorf("invalid embedder configuration for %s: %w", indexLabel, err)
	}

	embedder, err := embeddings.NewEmbedder(modelConfig)
	if err != nil {
		return fmt.Errorf("failed to create embedding plugin: %w", err)
	}

	// Sparse indexes use SparseEmbed for validation, not dense EmbedText.
	if embeddingsConfig.Sparse {
		sparseEmb, ok := embedder.(embeddings.SparseEmbedder)
		if !ok {
			return fmt.Errorf("embedder for sparse %s does not support sparse embedding", indexLabel)
		}
		testCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		testVecs, err := sparseEmb.SparseEmbed(testCtx, []string{"test"})
		if err != nil {
			return fmt.Errorf("failed to validate sparse embedding configuration with test: %w", err)
		}
		if len(testVecs) == 0 {
			return fmt.Errorf("failed to validate sparse embedding configuration: no vectors returned from test")
		}
		return nil
	}

	// Dense embedding test + dimension inference.
	testCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	testEmbeddings, err := embeddings.EmbedText(testCtx, embedder, []string{"test"})
	if err != nil {
		return fmt.Errorf("failed to validate embedding configuration with test: %w", err)
	}
	if len(testEmbeddings) == 0 {
		return fmt.Errorf("failed to validate embedding configuration: no embeddings returned from test")
	}
	if embeddingsConfig.Dimension <= 0 {
		embeddingsConfig.Dimension = len(testEmbeddings[0])
	}
	if embeddingsConfig.Dimension <= 0 {
		return fmt.Errorf("embedding dimension must be greater than 0")
	}
	if len(testEmbeddings[0]) != int(embeddingsConfig.Dimension) {
		return fmt.Errorf("embedding dimension mismatch: expected %d, got %d",
			embeddingsConfig.Dimension, len(testEmbeddings[0]))
	}

	// Validate the summarizer config if present. Use a fresh timeout so the
	// summarizer gets the full 30 seconds regardless of how long the embedding
	// test took.
	if embeddingsConfig.Summarizer != nil {
		sumCtx, sumCancel := context.WithTimeout(ctx, 30*time.Second)
		defer sumCancel()
		if err := validateSummarizerConfig(sumCtx, embeddingsConfig.Summarizer, indexLabel); err != nil {
			return err
		}
	}

	return nil
}

// validateSummarizerConfig validates a summarizer by instantiating the plugin
// and running a test summarization.
func validateSummarizerConfig(ctx context.Context, summarizer *ai.GeneratorConfig, indexLabel string) error {
	generatorConfig := *summarizer

	if err := generatorConfig.Validate(); err != nil {
		return fmt.Errorf("invalid summarizer configuration for %s: %w", indexLabel, err)
	}

	sum, err := ai.NewDocumentSummarizer(generatorConfig)
	if err != nil {
		return fmt.Errorf("failed to create summarizer plugin: %w", err)
	}

	testSummaries, err := sum.SummarizeRenderedDocs(ctx, []string{"test"})
	if err != nil {
		return fmt.Errorf("failed to validate summarizer configuration with test: %w", err)
	}
	if len(testSummaries) == 0 {
		return fmt.Errorf("failed to validate summarizer configuration: no summaries returned from test")
	}

	return nil
}
