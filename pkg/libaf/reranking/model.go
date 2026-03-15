package reranking

import (
	"context"
)

// Model represents a reranking model that can score text relevance.
// This interface allows different model implementations (ONNX, API-based, etc.)
// to be used interchangeably in the reranking pipeline.
type Model interface {
	// Rerank scores pre-rendered document texts based on their relevance to the query.
	// Returns a slice of scores with the same length as prompts.
	// Higher scores indicate higher relevance.
	Rerank(ctx context.Context, query string, prompts []string) ([]float32, error)

	// Close releases any resources held by the model (sessions, connections, etc.)
	Close() error
}
