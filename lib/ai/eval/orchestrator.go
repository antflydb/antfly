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
	"slices"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/firebase/genkit/go/genkit"
	"go.uber.org/zap"
)

// Orchestrator coordinates running multiple evaluators.
type Orchestrator struct {
	registry *Registry
}

// NewOrchestrator creates a new evaluation orchestrator.
func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		registry: DefaultRegistry(),
	}
}

// NewOrchestratorWithRegistry creates an orchestrator with a custom registry.
func NewOrchestratorWithRegistry(r *Registry) *Orchestrator {
	return &Orchestrator{
		registry: r,
	}
}

// EvaluateInput contains all data needed for evaluation.
type EvaluateInput struct {
	Config       EvalConfig
	Query        string
	Output       string
	RetrievedIDs []string
	Context      []any
}

// Evaluate runs all configured evaluators and returns aggregated results.
func (o *Orchestrator) Evaluate(ctx context.Context, input EvaluateInput) (*EvalResult, error) {
	start := time.Now()

	if len(input.Config.Evaluators) == 0 {
		return nil, fmt.Errorf("no evaluators specified")
	}

	// Apply defaults
	ApplyDefaults(&input.Config)

	// Check if we need genkit for LLM evaluators
	var g *genkit.Genkit
	var judgeConfig *ai.GeneratorConfig
	needsLLM := slices.ContainsFunc(input.Config.Evaluators, IsLLMMetric)

	log := zap.L()
	if needsLLM {
		// Use provided judge config or default
		if input.Config.Judge.Provider != "" {
			judgeConfig = &input.Config.Judge
			log.Debug("Using provided judge config for evaluation",
				zap.String("provider", string(judgeConfig.Provider)),
			)
		} else {
			defaultJudge := DefaultJudge()
			judgeConfig = &defaultJudge
			log.Debug("Using default judge config for evaluation",
				zap.String("provider", string(judgeConfig.Provider)),
			)
		}

		var modelName string
		var err error
		g, modelName, err = InitGenkit(ctx, judgeConfig)
		if err != nil {
			return nil, fmt.Errorf("initializing genkit for LLM evaluation: %w", err)
		}
		log.Debug("Initialized genkit for LLM evaluation",
			zap.String("modelName", modelName),
		)
	}

	// Build internal input
	internalInput := InternalEvalInput{
		Query:        input.Query,
		Output:       input.Output,
		RetrievedIDs: input.RetrievedIDs,
		Context:      input.Context,
		GroundTruth:  &input.Config.GroundTruth,
		Options:      input.Config.Options,
		Expectations: input.Config.GroundTruth.Expectations,
	}

	// Create evaluators
	evaluators := make([]Evaluator, 0, len(input.Config.Evaluators))
	for _, name := range input.Config.Evaluators {
		evaluator, err := o.registry.Get(name, input.Config, g, judgeConfig)
		if err != nil {
			return nil, fmt.Errorf("creating evaluator %s: %w", name, err)
		}
		evaluators = append(evaluators, evaluator)
	}

	// Run evaluators in parallel
	type evaluatorResult struct {
		name     EvaluatorName
		category EvalCategory
		score    *EvaluatorScore
		err      error
	}

	results := make(chan evaluatorResult, len(evaluators))
	var wg sync.WaitGroup

	for _, ev := range evaluators {
		wg.Add(1)
		go func(evaluator Evaluator) {
			defer wg.Done()

			// Create timeout context
			timeout := time.Duration(input.Config.Options.TimeoutSeconds) * time.Second
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			evalCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			score, err := evaluator.Evaluate(evalCtx, internalInput)
			results <- evaluatorResult{
				name:     EvaluatorName(evaluator.Name()),
				category: evaluator.Category(),
				score:    score,
				err:      err,
			}
		}(ev)
	}

	// Close results channel when all evaluators complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	retrievalScores := make(map[string]EvaluatorScore)
	generationScores := make(map[string]EvaluatorScore)
	var totalScore float64
	var passed, failed, total int

	for result := range results {
		total++
		if result.err != nil {
			// Record error as failed evaluation
			failed++
			log.Debug("Evaluator failed",
				zap.String("evaluator", string(result.name)),
				zap.String("category", string(result.category)),
				zap.Error(result.err),
			)
			errScore := EvaluatorScore{
				Score:  0,
				Pass:   false,
				Reason: fmt.Sprintf("evaluation error: %v", result.err),
			}
			switch result.category {
			case CategoryRetrieval:
				retrievalScores[string(result.name)] = errScore
			case CategoryGeneration:
				generationScores[string(result.name)] = errScore
			}
			continue
		}

		totalScore += float64(result.score.Score)
		if result.score.Pass {
			passed++
		} else {
			failed++
		}

		switch result.category {
		case CategoryRetrieval:
			retrievalScores[string(result.name)] = *result.score
		case CategoryGeneration:
			generationScores[string(result.name)] = *result.score
		}
	}

	// Build result
	var avgScore float32
	if total > 0 {
		avgScore = float32(totalScore / float64(total))
	}

	durationMs := int(time.Since(start).Milliseconds())

	finalResult := &EvalResult{
		Scores: EvalScores{
			Retrieval:  retrievalScores,
			Generation: generationScores,
		},
		Summary: EvalSummary{
			AverageScore: avgScore,
			Passed:       passed,
			Failed:       failed,
			Total:        total,
		},
		DurationMs: durationMs,
	}

	return finalResult, nil
}

// EvaluateRequest handles a standalone evaluation request (POST /eval).
func (o *Orchestrator) EvaluateRequest(ctx context.Context, req EvalRequest) (*EvalResult, error) {
	// Convert context to []any
	var contextAny []any
	if len(req.Context) > 0 {
		contextAny = make([]any, len(req.Context))
		for i, v := range req.Context {
			contextAny[i] = v
		}
	}

	input := EvaluateInput{
		Config: EvalConfig{
			Evaluators:  req.Evaluators,
			Judge:       req.Judge,
			GroundTruth: req.GroundTruth,
			Options:     req.Options,
		},
		Query:        req.Query,
		Output:       req.Output,
		RetrievedIDs: req.RetrievedIds,
		Context:      contextAny,
	}

	return o.Evaluate(ctx, input)
}
