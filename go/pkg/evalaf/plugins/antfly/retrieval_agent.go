// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package antflyevalaf

import (
	"github.com/antflydb/antfly/go/pkg/evalaf/agent"
	antfly "github.com/antflydb/antfly/go/pkg/sdk"
)

// NewRetrievalAgentClassificationEvaluator creates a classification evaluator for Antfly's Retrieval Agent.
// The Retrieval Agent classifies queries as "question" or "search".
func NewRetrievalAgentClassificationEvaluator(name string) *agent.ClassificationEvaluator {
	if name == "" {
		name = "retrieval_agent_classification"
	}
	return agent.NewClassificationEvaluator(name, []string{"question", "search"})
}

// NewRetrievalAgentConfidenceEvaluator creates a confidence evaluator for Retrieval Agent.
// Default threshold is 0.7 (70% confidence).
func NewRetrievalAgentConfidenceEvaluator(name string, minConfidence float64) *agent.ConfidenceEvaluator {
	if name == "" {
		name = "retrieval_agent_confidence"
	}
	if minConfidence <= 0 {
		minConfidence = 0.7
	}
	return agent.NewConfidenceEvaluator(name, minConfidence)
}

// RetrievalAgentResponse represents the structured response from Antfly's Retrieval Agent,
// mirroring the subset of RetrievalAgentResult (specs/openapi/antfly/metadata.yaml) that
// evaluators care about. Route type, improved/semantic query, confidence, and reasoning
// live under Classification, which is only populated when steps.classification was
// configured on the request.
type RetrievalAgentResponse struct {
	Classification       *antfly.ClassificationTransformationResult `json:"classification,omitempty"`
	Generation           string                                     `json:"generation,omitempty"`            // Generated answer, present when steps.generation was configured
	GenerationConfidence float64                                    `json:"generation_confidence,omitempty"` // Requires steps.confidence
	ContextRelevance     float64                                    `json:"context_relevance,omitempty"`     // Requires steps.confidence
	FollowupQuestions    []string                                   `json:"followup_questions,omitempty"`    // Requires steps.followup
}
