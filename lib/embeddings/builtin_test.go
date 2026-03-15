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
	"math"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
)

func getAntflyEmbedder(t *testing.T) Embedder {
	t.Helper()
	emb, err := NewAntflyEmbedderFromConfig(EmbedderConfig{})
	if err != nil {
		t.Fatalf("NewAntflyEmbedderFromConfig: %v", err)
	}
	return emb
}

func TestAntflyCapabilities(t *testing.T) {
	emb := getAntflyEmbedder(t)
	caps := emb.Capabilities()

	if caps.DefaultDimension != 384 {
		t.Errorf("expected default dimension 384, got %d", caps.DefaultDimension)
	}
	if !caps.IsTextOnly() {
		t.Error("expected text-only capabilities")
	}
	if caps.IsMultimodal() {
		t.Error("expected non-multimodal capabilities")
	}
	if !caps.SupportsMIMEType("text/plain") {
		t.Error("expected text/plain support")
	}
}

func TestAntflySingleEmbed(t *testing.T) {
	emb := getAntflyEmbedder(t)
	ctx := context.Background()

	results, err := EmbedText(ctx, emb, []string{"hello world"})
	if err != nil {
		t.Fatalf("EmbedText: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0]) != 384 {
		t.Errorf("expected dimension 384, got %d", len(results[0]))
	}

	// Check L2 normalized (magnitude ~1.0)
	var sum float64
	for _, v := range results[0] {
		sum += float64(v) * float64(v)
	}
	magnitude := math.Sqrt(sum)
	if math.Abs(magnitude-1.0) > 0.01 {
		t.Errorf("expected magnitude ~1.0, got %f", magnitude)
	}
}

func TestAntflyBatchEmbed(t *testing.T) {
	emb := getAntflyEmbedder(t)
	ctx := context.Background()

	texts := []string{
		"the quick brown fox",
		"jumped over the lazy dog",
		"machine learning is powerful",
	}
	results, err := EmbedText(ctx, emb, texts)
	if err != nil {
		t.Fatalf("EmbedText: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if len(r) != 384 {
			t.Errorf("result %d: expected dimension 384, got %d", i, len(r))
		}
	}
}

func TestAntflySemanticSimilarity(t *testing.T) {
	emb := getAntflyEmbedder(t)
	ctx := context.Background()

	texts := []string{"cat", "dog", "quantum physics"}
	results, err := EmbedText(ctx, emb, texts)
	if err != nil {
		t.Fatalf("EmbedText: %v", err)
	}

	catDog := cosineSim(results[0], results[1])
	catPhysics := cosineSim(results[0], results[2])

	if catDog <= catPhysics {
		t.Errorf("expected 'cat' closer to 'dog' than to 'quantum physics': cat-dog=%f, cat-physics=%f", catDog, catPhysics)
	}
}

func TestAntflyEmptyInput(t *testing.T) {
	emb := getAntflyEmbedder(t)
	ctx := context.Background()

	results, err := emb.Embed(ctx, [][]ai.ContentPart{})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func getAntflyEmbedderB(b *testing.B) Embedder {
	b.Helper()
	emb, err := NewAntflyEmbedderFromConfig(EmbedderConfig{})
	if err != nil {
		b.Fatalf("NewAntflyEmbedderFromConfig: %v", err)
	}
	return emb
}

func BenchmarkAntflySingleEmbed(b *testing.B) {
	emb := getAntflyEmbedderB(b)
	ctx := context.Background()
	contents := [][]ai.ContentPart{{ai.TextContent{Text: "hello world"}}}

	b.ReportAllocs()

	for b.Loop() {
		_, err := emb.Embed(ctx, contents)
		if err != nil {
			b.Fatalf("Embed: %v", err)
		}
	}
}

func BenchmarkAntflyBatchEmbed10(b *testing.B) {
	emb := getAntflyEmbedderB(b)
	ctx := context.Background()
	contents := make([][]ai.ContentPart, 10)
	for i := range contents {
		contents[i] = []ai.ContentPart{ai.TextContent{Text: "The quick brown fox jumps over the lazy dog."}}
	}

	b.ReportAllocs()

	for b.Loop() {
		_, err := emb.Embed(ctx, contents)
		if err != nil {
			b.Fatalf("Embed: %v", err)
		}
	}
}

func BenchmarkAntflyLongText(b *testing.B) {
	emb := getAntflyEmbedderB(b)
	ctx := context.Background()
	// ~128 tokens
	longText := "Antfly is a distributed key-value store and vector search engine built on Raft consensus. " +
		"It provides hybrid search capabilities combining full-text search with vector similarity search, " +
		"supporting multimodal data including images, audio, and video. The system uses Pebble for storage " +
		"and supports various embedding models for generating vector representations of documents."
	contents := [][]ai.ContentPart{{ai.TextContent{Text: longText}}}

	b.ReportAllocs()

	for b.Loop() {
		_, err := emb.Embed(ctx, contents)
		if err != nil {
			b.Fatalf("Embed: %v", err)
		}
	}
}

func cosineSim(a, b []float32) float64 {
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}
