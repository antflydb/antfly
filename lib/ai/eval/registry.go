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

package eval

import (
	"context"
	"fmt"
	"sync"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/firebase/genkit/go/genkit"
)

// EvalCategory categorizes evaluators for result organization.
type EvalCategory string

const (
	CategoryRetrieval  EvalCategory = "retrieval"
	CategoryGeneration EvalCategory = "generation"
)

// InternalEvalInput is the internal evaluation input format.
type InternalEvalInput struct {
	Query        string
	Output       string
	RetrievedIDs []string
	Context      []any
	GroundTruth  *GroundTruth
	Expectations string
	Options      EvalOptions
}

// Evaluator interface for inline evaluation.
type Evaluator interface {
	// Name returns the evaluator name.
	Name() string

	// Category returns the evaluator category (retrieval or generation).
	Category() EvalCategory

	// Evaluate runs evaluation on the input and returns a score.
	Evaluate(ctx context.Context, input InternalEvalInput) (*EvaluatorScore, error)
}

// EvaluatorFactory creates evaluators with config.
type EvaluatorFactory func(cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error)

// Registry manages evaluator factories.
type Registry struct {
	mu        sync.RWMutex
	factories map[EvaluatorName]EvaluatorFactory
}

// NewRegistry creates a new empty registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[EvaluatorName]EvaluatorFactory),
	}
}

// Register adds a new evaluator factory.
func (r *Registry) Register(name EvaluatorName, factory EvaluatorFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

// Get creates an evaluator instance.
func (r *Registry) Get(name EvaluatorName, cfg EvalConfig, g *genkit.Genkit, judgeConfig *ai.GeneratorConfig) (Evaluator, error) {
	r.mu.RLock()
	factory, ok := r.factories[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown evaluator: %s", name)
	}
	return factory(cfg, g, judgeConfig)
}

// Has checks if an evaluator is registered.
func (r *Registry) Has(name EvaluatorName) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.factories[name]
	return ok
}

// defaultRegistry is the global registry with all built-in evaluators.
var defaultRegistry *Registry
var registryOnce sync.Once

// DefaultRegistry returns the global registry with all built-in evaluators.
func DefaultRegistry() *Registry {
	registryOnce.Do(func() {
		defaultRegistry = NewRegistry()
		registerBuiltinEvaluators(defaultRegistry)
	})
	return defaultRegistry
}

// registerBuiltinEvaluators registers all built-in evaluators.
func registerBuiltinEvaluators(r *Registry) {
	// Retrieval metrics
	r.Register(EvaluatorNameRecall, newRecallFactory)
	r.Register(EvaluatorNamePrecision, newPrecisionFactory)
	r.Register(EvaluatorNameNdcg, newNDCGFactory)
	r.Register(EvaluatorNameMrr, newMRRFactory)
	r.Register(EvaluatorNameMap, newMAPFactory)

	// LLM-as-judge metrics
	r.Register(EvaluatorNameRelevance, newRelevanceFactory)
	r.Register(EvaluatorNameFaithfulness, newFaithfulnessFactory)
	r.Register(EvaluatorNameCompleteness, newCompletenessFactory)
	r.Register(EvaluatorNameCoherence, newCoherenceFactory)
	r.Register(EvaluatorNameSafety, newSafetyFactory)
	r.Register(EvaluatorNameHelpfulness, newHelpfulnessFactory)
	r.Register(EvaluatorNameCorrectness, newCorrectnessFactory)
	r.Register(EvaluatorNameCitationQuality, newCitationQualityFactory)
}

// IsRetrievalMetric returns true if the evaluator is a retrieval metric.
func IsRetrievalMetric(name EvaluatorName) bool {
	switch name {
	case EvaluatorNameRecall, EvaluatorNamePrecision, EvaluatorNameNdcg, EvaluatorNameMrr, EvaluatorNameMap:
		return true
	default:
		return false
	}
}

// IsLLMMetric returns true if the evaluator requires an LLM judge.
func IsLLMMetric(name EvaluatorName) bool {
	switch name {
	case EvaluatorNameRelevance, EvaluatorNameFaithfulness, EvaluatorNameCompleteness,
		EvaluatorNameCoherence, EvaluatorNameSafety, EvaluatorNameHelpfulness,
		EvaluatorNameCorrectness, EvaluatorNameCitationQuality:
		return true
	default:
		return false
	}
}
