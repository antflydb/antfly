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
	"fmt"

	"github.com/antflydb/antfly/lib/schema"
	builtinreranker "github.com/antflydb/antfly/pkg/termite/lib/builtin/reranker"
)

// antflyReranker wraps the builtin reranker from termite with per-config field/template settings.
type antflyReranker struct {
	inner *builtinreranker.BuiltinReranker
	field string
	tmpl  string
}

// NewAntflyRerankerFromConfig creates an antfly reranker from the unified config.
func NewAntflyRerankerFromConfig(config RerankerConfig) (Reranker, error) {
	br, err := builtinreranker.Get()
	if err != nil {
		return nil, err
	}

	ar := &antflyReranker{inner: br}
	if config.Field != nil {
		ar.field = *config.Field
	}
	if config.Template != nil {
		ar.tmpl = *config.Template
	}
	return ar, nil
}

func (r *antflyReranker) Rerank(ctx context.Context, query string, documents []schema.Document) ([]float32, error) {
	if len(documents) == 0 {
		return []float32{}, nil
	}

	prompts, err := ExtractDocumentTexts(documents, r.field, r.tmpl)
	if err != nil {
		return nil, fmt.Errorf("extracting document texts: %w", err)
	}

	return r.inner.RerankTexts(ctx, query, prompts)
}

func init() {
	RegisterReranker(RerankerProviderAntfly, NewAntflyRerankerFromConfig)
}
