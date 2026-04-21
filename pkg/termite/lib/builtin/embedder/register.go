package embedder

import (
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/termite"
)

func init() {
	termite.RegisterBuiltinEmbedder(func() (string, embeddings.Embedder, error) {
		be, err := Get()
		if err != nil {
			return "", nil, err
		}
		return ModelName, NewAdapter(be), nil
	})
}
