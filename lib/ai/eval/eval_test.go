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
	"testing"
)

func TestRetrievalEvaluators(t *testing.T) {
	tests := []struct {
		name          string
		evaluatorName EvaluatorName
		retrievedIDs  []string
		relevantIDs   []string
		k             int
		expectedPass  bool
		minScore      float32
		maxScore      float32
	}{
		{
			name:          "recall_perfect",
			evaluatorName: EvaluatorNameRecall,
			retrievedIDs:  []string{"doc1", "doc2", "doc3"},
			relevantIDs:   []string{"doc1", "doc2", "doc3"},
			k:             10,
			expectedPass:  true,
			minScore:      1.0,
			maxScore:      1.0,
		},
		{
			name:          "recall_partial",
			evaluatorName: EvaluatorNameRecall,
			retrievedIDs:  []string{"doc1", "doc4", "doc5"},
			relevantIDs:   []string{"doc1", "doc2", "doc3"},
			k:             10,
			expectedPass:  false,
			minScore:      0.3,
			maxScore:      0.4,
		},
		{
			name:          "precision_perfect",
			evaluatorName: EvaluatorNamePrecision,
			retrievedIDs:  []string{"doc1", "doc2", "doc3"},
			relevantIDs:   []string{"doc1", "doc2", "doc3"},
			k:             3,
			expectedPass:  true,
			minScore:      1.0,
			maxScore:      1.0,
		},
		{
			name:          "precision_partial",
			evaluatorName: EvaluatorNamePrecision,
			retrievedIDs:  []string{"doc1", "doc4", "doc5"},
			relevantIDs:   []string{"doc1", "doc2", "doc3"},
			k:             3,
			expectedPass:  false,
			minScore:      0.3,
			maxScore:      0.4,
		},
		{
			name:          "ndcg_perfect",
			evaluatorName: EvaluatorNameNdcg,
			retrievedIDs:  []string{"doc1", "doc2", "doc3"},
			relevantIDs:   []string{"doc1", "doc2", "doc3"},
			k:             3,
			expectedPass:  true,
			minScore:      1.0,
			maxScore:      1.0,
		},
		{
			name:          "mrr_first_position",
			evaluatorName: EvaluatorNameMrr,
			retrievedIDs:  []string{"doc1", "doc2", "doc3"},
			relevantIDs:   []string{"doc1"},
			k:             10,
			expectedPass:  true,
			minScore:      1.0,
			maxScore:      1.0,
		},
		{
			name:          "mrr_second_position",
			evaluatorName: EvaluatorNameMrr,
			retrievedIDs:  []string{"doc4", "doc1", "doc3"},
			relevantIDs:   []string{"doc1"},
			k:             10,
			expectedPass:  true,
			minScore:      0.5,
			maxScore:      0.5,
		},
		{
			name:          "map_perfect",
			evaluatorName: EvaluatorNameMap,
			retrievedIDs:  []string{"doc1", "doc2", "doc3"},
			relevantIDs:   []string{"doc1", "doc2", "doc3"},
			k:             10,
			expectedPass:  true,
			minScore:      1.0,
			maxScore:      1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := DefaultRegistry()

			cfg := EvalConfig{
				Options: EvalOptions{
					K:             tt.k,
					PassThreshold: 0.5,
				},
				GroundTruth: GroundTruth{
					RelevantIds: tt.relevantIDs,
				},
			}

			evaluator, err := registry.Get(tt.evaluatorName, cfg, nil, nil)
			if err != nil {
				t.Fatalf("Failed to get evaluator: %v", err)
			}

			input := InternalEvalInput{
				Query:        "test query",
				RetrievedIDs: tt.retrievedIDs,
				GroundTruth:  &cfg.GroundTruth,
				Options:      cfg.Options,
			}

			result, err := evaluator.Evaluate(context.Background(), input)
			if err != nil {
				t.Fatalf("Evaluation failed: %v", err)
			}

			t.Logf("Result: Score=%v, Pass=%v, Reason=%s, Metadata=%v", result.Score, result.Pass, result.Reason, result.Metadata)

			if result.Pass != tt.expectedPass {
				t.Errorf("Expected pass=%v, got %v", tt.expectedPass, result.Pass)
			}

			if result.Score < tt.minScore || result.Score > tt.maxScore {
				t.Errorf("Expected score in [%v, %v], got %v (reason: %s)", tt.minScore, tt.maxScore, result.Score, result.Reason)
			}
		})
	}
}

func TestRegistryHas(t *testing.T) {
	registry := DefaultRegistry()

	// Test retrieval metrics
	for _, name := range []EvaluatorName{EvaluatorNameRecall, EvaluatorNamePrecision, EvaluatorNameNdcg, EvaluatorNameMrr, EvaluatorNameMap} {
		if !registry.Has(name) {
			t.Errorf("Registry should have %s", name)
		}
		if !IsRetrievalMetric(name) {
			t.Errorf("%s should be a retrieval metric", name)
		}
		if IsLLMMetric(name) {
			t.Errorf("%s should not be an LLM metric", name)
		}
	}

	// Test LLM metrics
	for _, name := range []EvaluatorName{
		EvaluatorNameRelevance, EvaluatorNameFaithfulness, EvaluatorNameCompleteness,
		EvaluatorNameCoherence, EvaluatorNameSafety, EvaluatorNameHelpfulness,
		EvaluatorNameCorrectness, EvaluatorNameCitationQuality,
	} {
		if !registry.Has(name) {
			t.Errorf("Registry should have %s", name)
		}
		if IsRetrievalMetric(name) {
			t.Errorf("%s should not be a retrieval metric", name)
		}
		if !IsLLMMetric(name) {
			t.Errorf("%s should be an LLM metric", name)
		}
	}

	// Test unknown metric
	if registry.Has("unknown") {
		t.Error("Registry should not have unknown metric")
	}
}

func TestApplyDefaults(t *testing.T) {
	cfg := &EvalConfig{}
	ApplyDefaults(cfg)

	if cfg.Options.K != 10 {
		t.Errorf("Expected default K=10, got %d", cfg.Options.K)
	}
	if cfg.Options.PassThreshold != 0.5 {
		t.Errorf("Expected default PassThreshold=0.5, got %f", cfg.Options.PassThreshold)
	}
	if cfg.Options.TimeoutSeconds != 30 {
		t.Errorf("Expected default TimeoutSeconds=30, got %d", cfg.Options.TimeoutSeconds)
	}
}

func TestDefaultJudge(t *testing.T) {
	judge := DefaultJudge()

	if judge.Provider == "" {
		t.Error("Default judge should have a provider")
	}

	model, err := judge.GetModel()
	if err != nil {
		t.Fatalf("Failed to get model: %v", err)
	}
	if model == "" {
		t.Error("Default judge should have a model")
	}
}

func TestOrchestratorWithRetrievalMetrics(t *testing.T) {
	orchestrator := NewOrchestrator()

	input := EvaluateInput{
		Config: EvalConfig{
			Evaluators: []EvaluatorName{EvaluatorNameRecall, EvaluatorNamePrecision},
			GroundTruth: GroundTruth{
				RelevantIds: []string{"doc1", "doc2"},
			},
			Options: EvalOptions{
				K:             5,
				PassThreshold: 0.5,
			},
		},
		Query:        "test query",
		RetrievedIDs: []string{"doc1", "doc2", "doc3"},
	}

	result, err := orchestrator.Evaluate(context.Background(), input)
	if err != nil {
		t.Fatalf("Evaluation failed: %v", err)
	}

	if result.Summary.Total != 2 {
		t.Errorf("Expected 2 evaluators, got %d", result.Summary.Total)
	}

	if result.Scores.Retrieval == nil {
		t.Error("Expected retrieval scores")
	}

	if len(result.Scores.Retrieval) != 2 {
		t.Errorf("Expected 2 retrieval scores, got %d", len(result.Scores.Retrieval))
	}

	// Check recall (should be 1.0 since both relevant docs are in top 5)
	recallScore, ok := result.Scores.Retrieval["recall"]
	if !ok {
		t.Error("Expected recall score")
	} else if recallScore.Score != 1.0 {
		t.Errorf("Expected recall score 1.0, got %f", recallScore.Score)
	}

	// Check precision (should be 2/3 since 2 of 3 retrieved are relevant)
	precisionScore, ok := result.Scores.Retrieval["precision"]
	if !ok {
		t.Error("Expected precision score")
	} else if precisionScore.Score < 0.66 || precisionScore.Score > 0.67 {
		t.Errorf("Expected precision score ~0.67, got %f", precisionScore.Score)
	}
}
