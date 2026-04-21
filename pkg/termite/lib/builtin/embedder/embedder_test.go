package embedder

import (
	"context"
	"math"
	"testing"
)

func getEmbedder(t *testing.T) *BuiltinEmbedder {
	t.Helper()
	e, err := Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return e
}

func TestDimension(t *testing.T) {
	if Dimension != 384 {
		t.Errorf("expected Dimension 384, got %d", Dimension)
	}
}

func TestSingleEmbed(t *testing.T) {
	e := getEmbedder(t)
	ctx := context.Background()

	results, err := e.EmbedTexts(ctx, []string{"hello world"})
	if err != nil {
		t.Fatalf("EmbedTexts: %v", err)
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

func TestBatchEmbed(t *testing.T) {
	e := getEmbedder(t)
	ctx := context.Background()

	texts := []string{
		"the quick brown fox",
		"jumped over the lazy dog",
		"machine learning is powerful",
	}
	results, err := e.EmbedTexts(ctx, texts)
	if err != nil {
		t.Fatalf("EmbedTexts: %v", err)
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

func TestSemanticSimilarity(t *testing.T) {
	e := getEmbedder(t)
	ctx := context.Background()

	texts := []string{"cat", "dog", "quantum physics"}
	results, err := e.EmbedTexts(ctx, texts)
	if err != nil {
		t.Fatalf("EmbedTexts: %v", err)
	}

	catDog := cosineSim(results[0], results[1])
	catPhysics := cosineSim(results[0], results[2])

	if catDog <= catPhysics {
		t.Errorf("expected 'cat' closer to 'dog' than to 'quantum physics': cat-dog=%f, cat-physics=%f", catDog, catPhysics)
	}
}

func TestLongTextTruncation(t *testing.T) {
	e := getEmbedder(t)
	ctx := context.Background()

	// Build a text that tokenizes to well over MaxSequenceLength (512) tokens.
	// Repeating a sentence many times ensures we exceed the limit.
	var longText string
	for range 200 {
		longText += "The quick brown fox jumps over the lazy dog. "
	}

	// Verify it actually tokenizes to more than 512 tokens.
	tokens := e.tok.Encode(longText)
	if len(tokens) <= MaxSequenceLength {
		t.Fatalf("expected >%d tokens, got %d — test text is too short", MaxSequenceLength, len(tokens))
	}
	t.Logf("Long text tokenizes to %d tokens (limit %d)", len(tokens), MaxSequenceLength)

	// This should succeed (not panic or return an ONNX shape error) because
	// EmbedTexts truncates to MaxSequenceLength before inference.
	results, err := e.EmbedTexts(ctx, []string{longText})
	if err != nil {
		t.Fatalf("EmbedTexts with long text: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0]) != Dimension {
		t.Errorf("expected dimension %d, got %d", Dimension, len(results[0]))
	}

	// Also test a batch where some texts are long and some are short.
	results, err = e.EmbedTexts(ctx, []string{longText, "short text", longText})
	if err != nil {
		t.Fatalf("EmbedTexts with mixed-length batch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if len(r) != Dimension {
			t.Errorf("result %d: expected dimension %d, got %d", i, Dimension, len(r))
		}
	}
}

func TestEmptyInput(t *testing.T) {
	e := getEmbedder(t)
	ctx := context.Background()

	results, err := e.EmbedTexts(ctx, nil)
	if err != nil {
		t.Fatalf("EmbedTexts: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func BenchmarkSingleEmbed(b *testing.B) {
	e, err := Get()
	if err != nil {
		b.Fatalf("Get: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		_, err := e.EmbedTexts(ctx, []string{"hello world"})
		if err != nil {
			b.Fatalf("EmbedTexts: %v", err)
		}
	}
}

func BenchmarkBatchEmbed10(b *testing.B) {
	e, err := Get()
	if err != nil {
		b.Fatalf("Get: %v", err)
	}
	ctx := context.Background()
	texts := make([]string, 10)
	for i := range texts {
		texts[i] = "The quick brown fox jumps over the lazy dog."
	}

	b.ReportAllocs()

	for b.Loop() {
		_, err := e.EmbedTexts(ctx, texts)
		if err != nil {
			b.Fatalf("EmbedTexts: %v", err)
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
