package embedder

import (
	"context"

	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
)

// Adapter wraps BuiltinEmbedder to implement the libaf embeddings.Embedder interface.
type Adapter struct {
	inner *BuiltinEmbedder
}

// NewAdapter returns an Adapter that implements embeddings.Embedder.
func NewAdapter(e *BuiltinEmbedder) *Adapter {
	return &Adapter{inner: e}
}

// Capabilities implements embeddings.Embedder.
func (a *Adapter) Capabilities() embeddings.EmbedderCapabilities {
	return embeddings.EmbedderCapabilities{
		SupportedMIMETypes: []embeddings.MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{Dimension},
		DefaultDimension:   Dimension,
	}
}

// Embed implements embeddings.Embedder.
func (a *Adapter) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}
	texts := embeddings.ExtractText(contents)
	return a.inner.EmbedTexts(ctx, texts)
}

// Close is a no-op (singleton).
func (a *Adapter) Close() error {
	return nil
}
