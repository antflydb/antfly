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

// getOllamaEmbedder creates an Ollama embedder using the all-minilm model.
// It skips the test/benchmark if Ollama is not reachable.
func getOllamaEmbedderT(t *testing.T) Embedder {
	t.Helper()
	cfg := EmbedderConfig{Provider: EmbedderProviderOllama}
	if err := cfg.FromOllamaEmbedderConfig(OllamaEmbedderConfig{Model: "all-minilm"}); err != nil {
		t.Fatalf("FromOllamaEmbedderConfig: %v", err)
	}
	emb, err := NewOllamaEmbedderImpl(cfg)
	if err != nil {
		t.Skipf("skipping: ollama not available: %v", err)
	}
	// Probe with a single embed to verify the model is pulled and server is running.
	_, err = emb.Embed(context.Background(), [][]ai.ContentPart{{ai.TextContent{Text: "probe"}}})
	if err != nil {
		t.Skipf("skipping: ollama embed probe failed: %v", err)
	}
	return emb
}

func getOllamaEmbedderB(b *testing.B) Embedder {
	b.Helper()
	cfg := EmbedderConfig{Provider: EmbedderProviderOllama}
	if err := cfg.FromOllamaEmbedderConfig(OllamaEmbedderConfig{Model: "all-minilm"}); err != nil {
		b.Fatalf("FromOllamaEmbedderConfig: %v", err)
	}
	emb, err := NewOllamaEmbedderImpl(cfg)
	if err != nil {
		b.Skipf("skipping: ollama not available: %v", err)
	}
	_, err = emb.Embed(context.Background(), [][]ai.ContentPart{{ai.TextContent{Text: "probe"}}})
	if err != nil {
		b.Skipf("skipping: ollama embed probe failed: %v", err)
	}
	return emb
}

func BenchmarkCompare_SingleEmbed(b *testing.B) {
	contents := [][]ai.ContentPart{{ai.TextContent{Text: "hello world"}}}
	ctx := context.Background()

	b.Run("Builtin", func(b *testing.B) {
		emb := getAntflyEmbedderB(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := emb.Embed(ctx, contents); err != nil {
				b.Fatalf("Embed: %v", err)
			}
		}
	})

	b.Run("Ollama", func(b *testing.B) {
		emb := getOllamaEmbedderB(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := emb.Embed(ctx, contents); err != nil {
				b.Fatalf("Embed: %v", err)
			}
		}
	})
}

func BenchmarkCompare_BatchEmbed10(b *testing.B) {
	contents := make([][]ai.ContentPart, 10)
	for i := range contents {
		contents[i] = []ai.ContentPart{ai.TextContent{Text: "The quick brown fox jumps over the lazy dog."}}
	}
	ctx := context.Background()

	b.Run("Builtin", func(b *testing.B) {
		emb := getAntflyEmbedderB(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := emb.Embed(ctx, contents); err != nil {
				b.Fatalf("Embed: %v", err)
			}
		}
	})

	b.Run("Ollama", func(b *testing.B) {
		emb := getOllamaEmbedderB(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := emb.Embed(ctx, contents); err != nil {
				b.Fatalf("Embed: %v", err)
			}
		}
	})
}

func BenchmarkCompare_LongText(b *testing.B) {
	longText := "Antfly is a distributed key-value store and vector search engine built on Raft consensus. " +
		"It provides hybrid search capabilities combining full-text search with vector similarity search, " +
		"supporting multimodal data including images, audio, and video. The system uses Pebble for storage " +
		"and supports various embedding models for generating vector representations of documents."
	contents := [][]ai.ContentPart{{ai.TextContent{Text: longText}}}
	ctx := context.Background()

	b.Run("Builtin", func(b *testing.B) {
		emb := getAntflyEmbedderB(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := emb.Embed(ctx, contents); err != nil {
				b.Fatalf("Embed: %v", err)
			}
		}
	})

	b.Run("Ollama", func(b *testing.B) {
		emb := getOllamaEmbedderB(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := emb.Embed(ctx, contents); err != nil {
				b.Fatalf("Embed: %v", err)
			}
		}
	})
}

func TestCompare_OutputSimilarity(t *testing.T) {
	builtinEmb := getAntflyEmbedder(t)
	ollamaEmb := getOllamaEmbedderT(t)
	ctx := context.Background()

	texts := []string{
		"hello world",
		"the quick brown fox jumps over the lazy dog",
		"machine learning and artificial intelligence",
		"Antfly is a distributed key-value store and vector search engine built on Raft consensus.",
	}

	builtinResults, err := EmbedText(ctx, builtinEmb, texts)
	if err != nil {
		t.Fatalf("builtin EmbedText: %v", err)
	}
	ollamaResults, err := EmbedText(ctx, ollamaEmb, texts)
	if err != nil {
		t.Fatalf("ollama EmbedText: %v", err)
	}

	if len(builtinResults) != len(ollamaResults) {
		t.Fatalf("result count mismatch: builtin=%d, ollama=%d", len(builtinResults), len(ollamaResults))
	}

	for i, text := range texts {
		sim := cosineSim(builtinResults[i], ollamaResults[i])
		t.Logf("text %d (%q): cosine similarity = %.6f", i, truncate(text, 40), sim)
	}

	// Verify both providers preserve relative semantic ordering.
	// "cat" should be closer to "dog" than to "quantum physics" for both.
	orderTexts := []string{"cat", "dog", "quantum physics"}
	builtinOrder, err := EmbedText(ctx, builtinEmb, orderTexts)
	if err != nil {
		t.Fatalf("builtin EmbedText (order): %v", err)
	}
	ollamaOrder, err := EmbedText(ctx, ollamaEmb, orderTexts)
	if err != nil {
		t.Fatalf("ollama EmbedText (order): %v", err)
	}

	builtinCatDog := cosineSim(builtinOrder[0], builtinOrder[1])
	builtinCatPhysics := cosineSim(builtinOrder[0], builtinOrder[2])
	ollamaCatDog := cosineSim(ollamaOrder[0], ollamaOrder[1])
	ollamaCatPhysics := cosineSim(ollamaOrder[0], ollamaOrder[2])

	t.Logf("semantic ordering: builtin cat-dog=%.4f cat-physics=%.4f | ollama cat-dog=%.4f cat-physics=%.4f",
		builtinCatDog, builtinCatPhysics, ollamaCatDog, ollamaCatPhysics)

	if builtinCatDog <= builtinCatPhysics {
		t.Errorf("builtin: expected cat closer to dog than quantum physics")
	}
	if ollamaCatDog <= ollamaCatPhysics {
		t.Errorf("ollama: expected cat closer to dog than quantum physics")
	}

	// Also log dimension info for debugging
	t.Logf("builtin dimensions: %d", len(builtinResults[0]))
	t.Logf("ollama dimensions:  %d", len(ollamaResults[0]))

	// Log L2 magnitudes to check normalization differences
	for i := range texts {
		builtinMag := magnitude(builtinResults[i])
		ollamaMag := magnitude(ollamaResults[i])
		t.Logf("text %d: builtin magnitude=%.4f, ollama magnitude=%.4f", i, builtinMag, ollamaMag)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func magnitude(v []float32) float64 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	return math.Sqrt(sum)
}
