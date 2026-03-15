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
	"errors"
	"fmt"

	"github.com/antflydb/antfly/lib/schema"
)

// Reranker interface for document reranking
type Reranker interface {
	Rerank(ctx context.Context, query string, documents []schema.Document) ([]float32, error)
}

// NewReranker creates a new reranker from configuration
func NewReranker(conf RerankerConfig) (Reranker, error) {
	if conf.Provider == "" {
		return nil, errors.New("provider not specified")
	}
	r, ok := RerankerRegistry[conf.Provider]
	if !ok {
		return nil, fmt.Errorf("no reranker registered for type %s", conf.Provider)
	}
	reranker, err := r(conf)
	if err != nil {
		return nil, fmt.Errorf("creating reranker from conf: %w", err)
	}
	return reranker, nil
}

// RegisterReranker registers a reranker provider
func RegisterReranker(
	typ RerankerProvider,
	constructor func(config RerankerConfig) (Reranker, error),
) {
	if _, exists := RerankerRegistry[typ]; exists {
		panic(fmt.Sprintf("reranker provider %s already registered", typ))
	}
	RerankerRegistry[typ] = constructor
}

// DeregisterReranker removes a reranker provider from the registry
func DeregisterReranker(typ RerankerProvider) {
	delete(RerankerRegistry, typ)
}

// RerankerRegistry maps provider types to constructor functions
var RerankerRegistry = map[RerankerProvider]func(config RerankerConfig) (Reranker, error){}

// defaultRerankerConfig is the default reranker configuration, set from config at startup.
var defaultRerankerConfig *RerankerConfig

// SetDefaultRerankerConfig sets the default reranker configuration.
// This should be called during config initialization.
func SetDefaultRerankerConfig(config *RerankerConfig) {
	defaultRerankerConfig = config
}

// GetDefaultRerankerConfig returns the current default reranker configuration.
func GetDefaultRerankerConfig() *RerankerConfig {
	return defaultRerankerConfig
}
