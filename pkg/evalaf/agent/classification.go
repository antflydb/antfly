package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
)

// ClassificationEvaluator evaluates classification accuracy.
// It checks if the agent's classification matches the expected classification.
type ClassificationEvaluator struct {
	name    string
	classes []string
}

// NewClassificationEvaluator creates a new classification evaluator.
func NewClassificationEvaluator(name string, classes []string) *ClassificationEvaluator {
	if name == "" {
		name = "classification_accuracy"
	}

	return &ClassificationEvaluator{
		name:    name,
		classes: classes,
	}
}

// Name returns the evaluator name.
func (e *ClassificationEvaluator) Name() string {
	return e.name
}

// Evaluate checks if the classification is correct.
// Expects:
// - input.Reference: expected classification (string)
// - input.Output: actual classification (string or map with "classification" key)
func (e *ClassificationEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	if input.Reference == nil {
		return nil, fmt.Errorf("reference classification is required")
	}

	expectedClass, ok := input.Reference.(string)
	if !ok {
		return nil, fmt.Errorf("reference must be a string")
	}

	// Extract actual classification from output
	var actualClass string

	switch output := input.Output.(type) {
	case string:
		actualClass = output
	case map[string]any:
		// Handle structured output (e.g., {"classification": "question", "confidence": 0.9})
		if class, ok := output["classification"].(string); ok {
			actualClass = class
		} else if class, ok := output["route_type"].(string); ok {
			// Support Antfly's answer agent format
			actualClass = class
		} else {
			return nil, fmt.Errorf("output map does not contain 'classification' or 'route_type' key")
		}
	default:
		return nil, fmt.Errorf("output must be a string or map, got %T", input.Output)
	}

	// Normalize classifications (lowercase, trim whitespace)
	expectedClass = strings.ToLower(strings.TrimSpace(expectedClass))
	actualClass = strings.ToLower(strings.TrimSpace(actualClass))

	pass := expectedClass == actualClass
	score := 0.0
	if pass {
		score = 1.0
	}

	reason := fmt.Sprintf("Expected: %q, Got: %q", expectedClass, actualClass)
	if pass {
		reason += " ✓"
	} else {
		reason += " ✗"
	}

	return &eval.EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
		Metadata: map[string]any{
			"expected":      expectedClass,
			"actual":        actualClass,
			"valid_classes": e.classes,
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *ClassificationEvaluator) SupportsStreaming() bool {
	return false
}

// MultiClassAccuracyEvaluator evaluates multi-class classification accuracy across multiple examples.
type MultiClassAccuracyEvaluator struct {
	name    string
	classes []string
}

// NewMultiClassAccuracyEvaluator creates a multi-class accuracy evaluator.
func NewMultiClassAccuracyEvaluator(name string, classes []string) *MultiClassAccuracyEvaluator {
	if name == "" {
		name = "multiclass_accuracy"
	}

	return &MultiClassAccuracyEvaluator{
		name:    name,
		classes: classes,
	}
}

// Name returns the evaluator name.
func (e *MultiClassAccuracyEvaluator) Name() string {
	return e.name
}

// Evaluate performs the same check as ClassificationEvaluator.
// Use the runner's aggregate statistics to get overall accuracy.
func (e *MultiClassAccuracyEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	basicEval := NewClassificationEvaluator("", e.classes)
	return basicEval.Evaluate(ctx, input)
}

// SupportsStreaming returns false.
func (e *MultiClassAccuracyEvaluator) SupportsStreaming() bool {
	return false
}

// ConfidenceEvaluator evaluates if the confidence score is reasonable.
type ConfidenceEvaluator struct {
	name              string
	minConfidence     float64
	requireConfidence bool
}

// NewConfidenceEvaluator creates a confidence evaluator.
func NewConfidenceEvaluator(name string, minConfidence float64) *ConfidenceEvaluator {
	if name == "" {
		name = "confidence_check"
	}

	if minConfidence <= 0 || minConfidence > 1 {
		minConfidence = 0.7 // default 70%
	}

	return &ConfidenceEvaluator{
		name:              name,
		minConfidence:     minConfidence,
		requireConfidence: true,
	}
}

// Name returns the evaluator name.
func (e *ConfidenceEvaluator) Name() string {
	return e.name
}

// Evaluate checks if confidence is above threshold.
// Expects output to be a map with "confidence" key.
func (e *ConfidenceEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
	outputMap, ok := input.Output.(map[string]any)
	if !ok {
		if e.requireConfidence {
			return &eval.EvalResult{
				Pass:   false,
				Score:  0.0,
				Reason: "Output does not contain confidence information",
			}, nil
		}
		// If confidence not required, pass
		return &eval.EvalResult{
			Pass:   true,
			Score:  1.0,
			Reason: "Confidence not required",
		}, nil
	}

	// Extract confidence
	var confidence float64
	if conf, ok := outputMap["confidence"].(float64); ok {
		confidence = conf
	} else if conf, ok := outputMap["confidence"].(float32); ok {
		confidence = float64(conf)
	} else if conf, ok := outputMap["confidence"].(int); ok {
		confidence = float64(conf)
	} else {
		if e.requireConfidence {
			return &eval.EvalResult{
				Pass:   false,
				Score:  0.0,
				Reason: "Confidence field missing or invalid",
			}, nil
		}
		return &eval.EvalResult{
			Pass:   true,
			Score:  1.0,
			Reason: "Confidence not required",
		}, nil
	}

	// Check if confidence is in valid range [0, 1]
	if confidence < 0 || confidence > 1 {
		return &eval.EvalResult{
			Pass:   false,
			Score:  0.0,
			Reason: fmt.Sprintf("Confidence %.2f out of range [0, 1]", confidence),
		}, nil
	}

	// Check if confidence meets minimum threshold
	pass := confidence >= e.minConfidence
	score := confidence

	reason := fmt.Sprintf("Confidence: %.2f (threshold: %.2f)", confidence, e.minConfidence)
	if pass {
		reason += " ✓"
	} else {
		reason += " ✗"
	}

	return &eval.EvalResult{
		Pass:   pass,
		Score:  score,
		Reason: reason,
		Metadata: map[string]any{
			"confidence":     confidence,
			"min_confidence": e.minConfidence,
		},
	}, nil
}

// SupportsStreaming returns false.
func (e *ConfidenceEvaluator) SupportsStreaming() bool {
	return false
}
