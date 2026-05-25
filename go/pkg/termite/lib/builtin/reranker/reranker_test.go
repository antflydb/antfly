package reranker

import (
	"context"
	"testing"
)

func getReranker(t *testing.T) *BuiltinReranker {
	t.Helper()
	r, err := Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return r
}

func TestBasic(t *testing.T) {
	r := getReranker(t)
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

func TestBatch(t *testing.T) {
	r := getReranker(t)
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

func TestLongTextTruncation(t *testing.T) {
	r := getReranker(t)
	ctx := context.Background()

	// Build documents that tokenize to well over MaxSequenceLength tokens.
	var longDoc string
	for range 200 {
		longDoc += "The quick brown fox jumps over the lazy dog. "
	}

	// Verify it actually exceeds the limit.
	tokens := r.tok.Encode(longDoc)
	if len(tokens) <= MaxSequenceLength {
		t.Fatalf("expected >%d tokens, got %d — test text is too short", MaxSequenceLength, len(tokens))
	}
	t.Logf("Long document tokenizes to %d tokens (limit %d)", len(tokens), MaxSequenceLength)

	// Should succeed without ONNX shape errors.
	scores, err := r.RerankTexts(ctx, "What is machine learning?", []string{longDoc, "short text", longDoc})
	if err != nil {
		t.Fatalf("RerankTexts with long text: %v", err)
	}
	if len(scores) != 3 {
		t.Fatalf("expected 3 scores, got %d", len(scores))
	}
	for i, s := range scores {
		t.Logf("  [%d] score=%.4f", i, s)
	}
}

func TestEmptyInput(t *testing.T) {
	r := getReranker(t)
	ctx := context.Background()

	scores, err := r.RerankTexts(ctx, "query", nil)
	if err != nil {
		t.Fatalf("RerankTexts: %v", err)
	}
	if len(scores) != 0 {
		t.Errorf("expected 0 scores, got %d", len(scores))
	}
}

func BenchmarkSingle(b *testing.B) {
	r, err := Get()
	if err != nil {
		b.Fatalf("Get: %v", err)
	}
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

func BenchmarkBatch10(b *testing.B) {
	r, err := Get()
	if err != nil {
		b.Fatalf("Get: %v", err)
	}
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
