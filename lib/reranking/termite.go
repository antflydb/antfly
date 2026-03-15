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
	"net/http"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/termite/pkg/client"
)

type TermiteReranker struct {
	client   *client.TermiteClient
	config   RerankerConfig
	field    string
	template string
}

func init() {
	RegisterReranker(RerankerProviderTermite, NewTermiteReranker)
}

func NewTermiteReranker(config RerankerConfig) (Reranker, error) {
	c, err := config.AsTermiteRerankerConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing termite config: %w", err)
	}

	url := "http://localhost:11433"
	if c.Url != nil && *c.Url != "" {
		url = *c.Url
	}

	field := ""
	if config.Field != nil {
		field = *config.Field
	}

	template := ""
	if config.Template != nil {
		template = *config.Template
	}

	httpClient := &http.Client{Timeout: time.Second * 540}
	termiteClient, err := client.NewTermiteClient(url, httpClient)
	if err != nil {
		return nil, fmt.Errorf("creating termite client: %w", err)
	}

	return &TermiteReranker{
		client:   termiteClient,
		config:   config,
		field:    field,
		template: template,
	}, nil
}

func (t *TermiteReranker) Rerank(
	ctx context.Context,
	query string,
	documents []schema.Document,
) ([]float32, error) {
	// Extract model name from config
	termiteConfig, err := t.config.AsTermiteRerankerConfig()
	if err != nil {
		return nil, fmt.Errorf("extracting termite config: %w", err)
	}

	// Extract text from documents using field or template
	prompts, err := ExtractDocumentTexts(documents, t.field, t.template)
	if err != nil {
		return nil, fmt.Errorf("extracting document texts: %w", err)
	}

	// Rerank using termite client
	scores, err := t.client.Rerank(ctx, termiteConfig.Model, query, prompts)
	if err != nil {
		return nil, err
	}

	// Validate response
	if len(scores) != len(prompts) {
		return nil, fmt.Errorf(
			"expected %d scores, got %d",
			len(prompts),
			len(scores),
		)
	}

	return scores, nil
}
