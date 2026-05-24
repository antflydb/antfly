package embeddings

import "context"

// SparseVector represents a sparse embedding as parallel arrays of indices and values.
// Indices are token IDs from the model's vocabulary, sorted ascending.
// Values are the corresponding weights (always positive after SPLADE activation).
type SparseVector struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

// SparseEmbedder generates sparse (SPLADE-style) embeddings from text.
// Unlike dense Embedder which returns fixed-dimension float vectors,
// SparseEmbedder returns variable-length sparse vectors with vocab-space indices.
type SparseEmbedder interface {
	// SparseEmbed generates sparse embeddings for the given texts.
	// Returns one SparseVector per input text.
	SparseEmbed(ctx context.Context, texts []string) ([]SparseVector, error)
}
