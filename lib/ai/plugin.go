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

//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package ai

import (
	"context"
	"errors"
	"fmt"

	generating "github.com/antflydb/antfly/pkg/generating"
)

// DocumentSummarizer is the interface for document summarization providers
type DocumentSummarizer interface {
	// SummarizeRenderedDocs generates summaries from pre-rendered document strings
	SummarizeRenderedDocs(
		ctx context.Context,
		renderedDocs []string,
		opts ...GenerateOption,
	) ([]string, error)
}

func init() {
	RegisterDocumentSummarizer(GeneratorProviderOllama,
		func(ctx context.Context, config GeneratorConfig) (DocumentSummarizer, error) {
			return NewGenKitGenerator(ctx, config)
		})
	RegisterDocumentSummarizer(GeneratorProviderGemini,
		func(ctx context.Context, config GeneratorConfig) (DocumentSummarizer, error) {
			return NewGenKitGenerator(ctx, config)
		})
	RegisterDocumentSummarizer(GeneratorProviderTermite,
		func(ctx context.Context, config GeneratorConfig) (DocumentSummarizer, error) {
			return NewGenKitGenerator(ctx, config)
		})
}

// DocumentSummarizerRegistry maps provider types to their constructors
var DocumentSummarizerRegistry = map[GeneratorProvider]func(ctx context.Context, config GeneratorConfig) (DocumentSummarizer, error){}

// NewDocumentSummarizer creates a new DocumentSummarizer from a GeneratorConfig
func NewDocumentSummarizer(conf GeneratorConfig) (DocumentSummarizer, error) {
	if conf.Provider == "" {
		return nil, errors.New("provider not specified")
	}
	g, ok := DocumentSummarizerRegistry[conf.Provider]
	if !ok {
		return nil, fmt.Errorf("no document summarizer registered for type %s", conf.Provider)
	}
	gen, err := g(context.Background(), conf)
	if err != nil {
		return nil, fmt.Errorf("creating document summarizer from conf: %w", err)
	}
	return gen, nil
}

// RegisterDocumentSummarizer registers a document summarizer constructor for a provider
func RegisterDocumentSummarizer(
	typ GeneratorProvider,
	constructor func(ctx context.Context, config GeneratorConfig) (DocumentSummarizer, error),
) {
	if _, exists := DocumentSummarizerRegistry[typ]; exists {
		panic(fmt.Sprintf("document summarizer provider %s already registered", typ))
	}
	DocumentSummarizerRegistry[typ] = constructor
}

// DeregisterDocumentSummarizer removes a document summarizer from the registry
func DeregisterDocumentSummarizer(typ GeneratorProvider) {
	delete(DocumentSummarizerRegistry, typ)
}

// NewGeneratorConfigFromJSON creates a GeneratorConfig from a JSON byte slice.
// Mostly useful for testing.
func NewGeneratorConfigFromJSON(provider string, data []byte) *GeneratorConfig {
	return (*GeneratorConfig)(generating.NewGeneratorConfigFromJSON(provider, data))
}
