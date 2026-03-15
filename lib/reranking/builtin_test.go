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

package reranking

import (
	"context"
	"testing"

	builtinreranker "github.com/antflydb/termite/pkg/termite/lib/builtin/reranker"
)

func getBuiltinReranker(t *testing.T) *builtinreranker.BuiltinReranker {
	t.Helper()
	r, err := builtinreranker.Get()
	if err != nil {
		t.Fatalf("builtinreranker.Get: %v", err)
	}
	return r
}

func getBuiltinRerankerB(b *testing.B) *builtinreranker.BuiltinReranker {
	b.Helper()
	r, err := builtinreranker.Get()
	if err != nil {
		b.Fatalf("builtinreranker.Get: %v", err)
	}
	return r
}

func TestAntflyRerankerBasic(t *testing.T) {
	r := getBuiltinReranker(t)
	ctx := context.Background()

	query := "What is machine learning?"
	documents := []string{
		"Machine learning is a subset of artificial intelligence that focuses on building systems that learn from data.",
		"The weather today is sunny with a chance of rain in the afternoon.",
		"Deep learning uses neural networks with multiple layers to learn hierarchical representations.",
		"Cooking pasta requires boiling water and adding salt.",
	}

	scores, err := r.RerankTexts(ctx, query, documents)
	if err != nil {
		t.Fatalf("RerankTexts: %v", err)
	}
	if len(scores) != 4 {
		t.Fatalf("expected 4 scores, got %d", len(scores))
	}

	// ML-related docs (0, 2) should score higher than unrelated docs (1, 3)
	if scores[0] <= scores[1] {
		t.Errorf("expected ML doc (%.4f) to score higher than weather doc (%.4f)", scores[0], scores[1])
	}
	if scores[2] <= scores[3] {
		t.Errorf("expected deep learning doc (%.4f) to score higher than cooking doc (%.4f)", scores[2], scores[3])
	}

	for i, s := range scores {
		t.Logf("  [%d] score=%.4f  %s", i, s, documents[i][:50])
	}
}

func TestAntflyRerankerBatch(t *testing.T) {
	r := getBuiltinReranker(t)
	ctx := context.Background()

	query := "What is machine learning?"
	documents := []string{
		"Machine learning is a subset of artificial intelligence.",
		"The weather today is sunny.",
		"Deep learning uses neural networks.",
		"Cooking pasta requires boiling water.",
		"Supervised learning algorithms learn from labeled data.",
		"The stock market fluctuates based on economic factors.",
		"Natural language processing enables computers to understand text.",
		"Gardening is a relaxing hobby.",
		"Reinforcement learning involves agents learning through trial and error.",
		"Classical music has been popular for centuries.",
	}

	scores, err := r.RerankTexts(ctx, query, documents)
	if err != nil {
		t.Fatalf("RerankTexts: %v", err)
	}
	if len(scores) != 10 {
		t.Fatalf("expected 10 scores, got %d", len(scores))
	}

	for i, s := range scores {
		t.Logf("  [%d] score=%.4f  %s", i, s, documents[i])
	}
}

func TestAntflyRerankerEmptyInput(t *testing.T) {
	r := getBuiltinReranker(t)
	ctx := context.Background()

	scores, err := r.RerankTexts(ctx, "query", nil)
	if err != nil {
		t.Fatalf("RerankTexts: %v", err)
	}
	if len(scores) != 0 {
		t.Errorf("expected 0 scores, got %d", len(scores))
	}
}

func TestAntflyRerankerViaInterface(t *testing.T) {
	reranker, err := NewAntflyRerankerFromConfig(RerankerConfig{
		Provider: RerankerProviderAntfly,
	})
	if err != nil {
		t.Fatalf("NewAntflyRerankerFromConfig: %v", err)
	}

	// Verify it satisfies the Reranker interface
	var _ = reranker

	// Empty documents should return empty scores
	scores, err := reranker.Rerank(context.Background(), "query", nil)
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	if len(scores) != 0 {
		t.Errorf("expected 0 scores, got %d", len(scores))
	}
}

func BenchmarkAntflyRerankerSingle(b *testing.B) {
	r := getBuiltinRerankerB(b)
	ctx := context.Background()
	query := "What is machine learning?"
	docs := []string{"Machine learning is a subset of artificial intelligence that focuses on building systems that learn from data."}

	b.ReportAllocs()

	for b.Loop() {
		_, err := r.RerankTexts(ctx, query, docs)
		if err != nil {
			b.Fatalf("RerankTexts: %v", err)
		}
	}
}

func BenchmarkAntflyRerankerBatch10(b *testing.B) {
	r := getBuiltinRerankerB(b)
	ctx := context.Background()
	query := "What is machine learning?"
	docs := []string{
		"Machine learning is a subset of artificial intelligence that focuses on building systems that learn from data.",
		"The weather today is sunny with a chance of rain in the afternoon.",
		"Deep learning uses neural networks with multiple layers to learn hierarchical representations.",
		"Cooking pasta requires boiling water and adding salt.",
		"Supervised learning algorithms learn from labeled training data to make predictions.",
		"The stock market fluctuates based on various economic factors.",
		"Natural language processing enables computers to understand and generate human language.",
		"Gardening is a relaxing hobby that connects people with nature.",
		"Reinforcement learning involves agents learning through trial and error with rewards.",
		"Classical music has been popular for centuries across many cultures.",
	}

	b.ReportAllocs()

	for b.Loop() {
		_, err := r.RerankTexts(ctx, query, docs)
		if err != nil {
			b.Fatalf("RerankTexts: %v", err)
		}
	}
}

func BenchmarkAntflyRerankerLongText(b *testing.B) {
	r := getBuiltinRerankerB(b)
	ctx := context.Background()
	query := "What is a distributed key-value store?"
	longDoc := "Antfly is a distributed key-value store and vector search engine built on Raft consensus. " +
		"It provides hybrid search capabilities combining full-text search with vector similarity search, " +
		"supporting multimodal data including images, audio, and video. The system uses Pebble for storage " +
		"and supports various embedding models for generating vector representations of documents."
	docs := []string{longDoc}

	b.ReportAllocs()

	for b.Loop() {
		_, err := r.RerankTexts(ctx, query, docs)
		if err != nil {
			b.Fatalf("RerankTexts: %v", err)
		}
	}
}
