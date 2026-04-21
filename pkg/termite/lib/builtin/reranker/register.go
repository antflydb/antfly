package reranker

import (
	"github.com/antflydb/antfly/pkg/libaf/reranking"
	"github.com/antflydb/antfly/pkg/termite"
)

func init() {
	termite.RegisterBuiltinReranker(func() (string, reranking.Model, error) {
		br, err := Get()
		if err != nil {
			return "", nil, err
		}
		return ModelName, br, nil
	})
}
