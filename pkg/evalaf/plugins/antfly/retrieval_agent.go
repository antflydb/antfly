package antflyevalaf

import (
	"github.com/antflydb/antfly/pkg/evalaf/agent"
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

// RetrievalAgentResponse represents the structured response from Antfly's Retrieval Agent.
type RetrievalAgentResponse struct {
	RouteType         string   `json:"route_type"`                    // "question" or "search"
	ImprovedQuery     string   `json:"improved_query"`                // Improved version of query
	SemanticQuery     string   `json:"semantic_query"`                // Query optimized for semantic search
	Confidence        float64  `json:"confidence"`                    // Confidence score (0-1)
	Generation        string   `json:"generation,omitempty"`          // Generated answer (for questions)
	Reasoning         string   `json:"reasoning,omitempty"`           // Reasoning (if enabled)
	FollowUpQuestions []string `json:"follow_up_questions,omitempty"` // Follow-up questions
}
