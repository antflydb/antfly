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

// Package embeddings provides benchmarks comparing embedding performance.
//
// # Usage
//
// Run all benchmarks:
//
//	EMBEDDING_MODEL_PATH=./models/bge-small-en-v1.5 go test -bench=. -benchmem
//
// Run embedder benchmarks only:
//
//	EMBEDDING_MODEL_PATH=./models/bge-small-en-v1.5 go test -bench=Embedder -benchmem
//
// Run throughput analysis:
//
//	EMBEDDING_MODEL_PATH=./models/bge-small-en-v1.5 go test -bench=Throughput -benchmem
//
// # Environment Variables
//
//   - EMBEDDING_MODEL_PATH: Path to ONNX model directory for embedder benchmarks
//
// # Benchmark Categories
//
//   - BenchmarkPooledEmbedder_*: Tests pooled embedder with various text lengths
//   - BenchmarkLatency_*: Single-text embedding latency measurements
//   - BenchmarkThroughput_*: Batch throughput analysis across different sizes
//   - BenchmarkWarmup: Cold vs warm start performance analysis
package embeddings

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	// Benchmark configuration
	benchSmallBatchSize  = 10
	benchMediumBatchSize = 50
	benchLargeBatchSize  = 100

	// Sample texts of varying lengths
	benchShortText  = "Quick search query"
	benchMediumText = "This is a medium length document that contains several sentences with various topics to embed."
	benchLongText   = "This is a longer document that simulates a more realistic use case with multiple paragraphs. " +
		"It contains detailed information about various topics that need to be embedded for semantic search. " +
		"The embedding system should handle this efficiently while maintaining good quality representations. " +
		"Performance characteristics may vary based on the length and complexity of the input text."
)

// generateBenchmarkTexts creates a slice of test texts for benchmarking
func generateBenchmarkTexts(count int, textType string) []string {
	texts := make([]string, count)
	var baseText string

	switch textType {
	case "short":
		baseText = benchShortText
	case "medium":
		baseText = benchMediumText
	case "long":
		baseText = benchLongText
	default:
		baseText = benchMediumText
	}

	for i := range texts {
		texts[i] = fmt.Sprintf("%s [doc %d]", baseText, i)
	}
	return texts
}

// createTestEmbedder creates a PooledEmbedder for testing
func createTestEmbedder(b *testing.B, modelPath string, poolSize int) *PooledEmbedder {
	b.Helper()

	logger := zap.NewNop()
	sessionManager := backends.NewSessionManager()

	cfg := PooledEmbedderConfig{
		ModelPath: modelPath,
		PoolSize:  poolSize,
		Normalize: true,
		Logger:    logger,
	}

	embedder, _, err := NewPooledEmbedder(cfg, sessionManager)
	require.NoError(b, err)
	return embedder
}

// BenchmarkPooledEmbedder_ShortTexts benchmarks embedder with short texts
func BenchmarkPooledEmbedder_ShortTexts(b *testing.B) {
	modelPath := os.Getenv("EMBEDDING_MODEL_PATH")
	if modelPath == "" {
		b.Skip("EMBEDDING_MODEL_PATH not set, skipping embedder benchmark")
	}

	embedder := createTestEmbedder(b, modelPath, 1)
	defer func() { _ = embedder.Close() }()

	texts := generateBenchmarkTexts(benchSmallBatchSize, "short")
	ctx := context.Background()

	b.ResetTimer()
	for b.Loop() {
		_, err := embeddings.EmbedText(ctx, embedder, texts)
		if err != nil {
			b.Fatalf("Embed failed: %v", err)
		}
	}
}

// BenchmarkPooledEmbedder_MediumTexts benchmarks embedder with medium-length texts
func BenchmarkPooledEmbedder_MediumTexts(b *testing.B) {
	modelPath := os.Getenv("EMBEDDING_MODEL_PATH")
	if modelPath == "" {
		b.Skip("EMBEDDING_MODEL_PATH not set, skipping embedder benchmark")
	}

	embedder := createTestEmbedder(b, modelPath, 1)
	defer func() { _ = embedder.Close() }()

	texts := generateBenchmarkTexts(benchMediumBatchSize, "medium")
	ctx := context.Background()

	b.ResetTimer()
	for b.Loop() {
		_, err := embeddings.EmbedText(ctx, embedder, texts)
		if err != nil {
			b.Fatalf("Embed failed: %v", err)
		}
	}
}

// BenchmarkPooledEmbedder_LongTexts benchmarks embedder with long texts
func BenchmarkPooledEmbedder_LongTexts(b *testing.B) {
	modelPath := os.Getenv("EMBEDDING_MODEL_PATH")
	if modelPath == "" {
		b.Skip("EMBEDDING_MODEL_PATH not set, skipping embedder benchmark")
	}

	embedder := createTestEmbedder(b, modelPath, 1)
	defer func() { _ = embedder.Close() }()

	texts := generateBenchmarkTexts(benchLargeBatchSize, "long")
	ctx := context.Background()

	b.ResetTimer()
	for b.Loop() {
		_, err := embeddings.EmbedText(ctx, embedder, texts)
		if err != nil {
			b.Fatalf("Embed failed: %v", err)
		}
	}
}

// BenchmarkLatency_SingleText measures single-text embedding latency
func BenchmarkLatency_SingleText(b *testing.B) {
	modelPath := os.Getenv("EMBEDDING_MODEL_PATH")
	if modelPath == "" {
		b.Skip("EMBEDDING_MODEL_PATH not set, skipping latency benchmark")
	}

	singleText := []string{benchMediumText}
	ctx := context.Background()

	b.Run("PooledEmbedder", func(b *testing.B) {
		embedder := createTestEmbedder(b, modelPath, 1)
		defer func() { _ = embedder.Close() }()

		b.ResetTimer()
		for b.Loop() {
			_, err := embeddings.EmbedText(ctx, embedder, singleText)
			if err != nil {
				b.Fatalf("Embed failed: %v", err)
			}
		}
	})
}

// BenchmarkThroughput_BatchSizes compares throughput across different batch sizes
func BenchmarkThroughput_BatchSizes(b *testing.B) {
	modelPath := os.Getenv("EMBEDDING_MODEL_PATH")
	if modelPath == "" {
		b.Skip("EMBEDDING_MODEL_PATH not set, skipping throughput benchmark")
	}

	ctx := context.Background()
	batchSizes := []int{1, 10, 50, 100}

	for _, batchSize := range batchSizes {
		texts := generateBenchmarkTexts(batchSize, "medium")

		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			embedder := createTestEmbedder(b, modelPath, 1)
			defer func() { _ = embedder.Close() }()

			b.ResetTimer()
			for b.Loop() {
				_, err := embeddings.EmbedText(ctx, embedder, texts)
				if err != nil {
					b.Fatalf("Embed failed: %v", err)
				}
			}

			// Report throughput
			docsPerSec := float64(b.N*batchSize) / b.Elapsed().Seconds()
			b.ReportMetric(docsPerSec, "docs/sec")
		})
	}
}

// BenchmarkWarmup tests cold vs warm start performance
func BenchmarkWarmup(b *testing.B) {
	modelPath := os.Getenv("EMBEDDING_MODEL_PATH")
	if modelPath == "" {
		b.Skip("EMBEDDING_MODEL_PATH not set, skipping warmup benchmark")
	}

	texts := generateBenchmarkTexts(benchSmallBatchSize, "medium")
	ctx := context.Background()

	b.Run("ColdStart", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			embedder := createTestEmbedder(b, modelPath, 1)
			b.StartTimer()

			_, err := embeddings.EmbedText(ctx, embedder, texts)
			if err != nil {
				b.Fatalf("Embed failed: %v", err)
			}

			b.StopTimer()
			_ = embedder.Close()
			b.StartTimer()
		}
	})

	b.Run("WarmStart", func(b *testing.B) {
		embedder := createTestEmbedder(b, modelPath, 1)
		defer func() { _ = embedder.Close() }()

		// Warmup
		_, _ = embeddings.EmbedText(ctx, embedder, texts)
		time.Sleep(100 * time.Millisecond)

		b.ResetTimer()
		for b.Loop() {
			_, err := embeddings.EmbedText(ctx, embedder, texts)
			if err != nil {
				b.Fatalf("Embed failed: %v", err)
			}
		}
	})
}
