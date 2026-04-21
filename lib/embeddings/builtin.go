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

	"github.com/antflydb/antfly/lib/ai"
	builtinembedder "github.com/antflydb/antfly/pkg/termite/lib/builtin/embedder"
	"golang.org/x/time/rate"
)

// antflyEmbedder wraps the builtin embedder from termite.
type antflyEmbedder struct {
	inner *builtinembedder.BuiltinEmbedder
}

// NewAntflyEmbedderFromConfig creates an antfly embedder from the unified config.
func NewAntflyEmbedderFromConfig(config EmbedderConfig) (Embedder, error) {
	be, err := builtinembedder.Get()
	if err != nil {
		return nil, err
	}
	return &antflyEmbedder{inner: be}, nil
}

func (e *antflyEmbedder) Capabilities() EmbedderCapabilities {
	return EmbedderCapabilities{
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{builtinembedder.Dimension},
		DefaultDimension:   builtinembedder.Dimension,
	}
}

func (e *antflyEmbedder) RateLimiter() *rate.Limiter {
	return nil // local inference, no rate limit
}

func (e *antflyEmbedder) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}
	texts := ExtractText(contents)
	return e.inner.EmbedTexts(ctx, texts)
}

func init() {
	RegisterEmbedder(EmbedderProviderAntfly, NewAntflyEmbedderFromConfig)
}
