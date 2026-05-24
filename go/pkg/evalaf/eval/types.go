package eval

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Example represents a single test case in a dataset.
type Example struct {
	// Input is the input to evaluate (e.g., a query, prompt, or message)
	Input any `json:"input"`

	// Reference is the expected output (optional, used by some evaluators)
	Reference any `json:"reference,omitempty"`

	// Context provides additional context (e.g., retrieved documents for RAG)
	Context []any `json:"context,omitempty"`

	// Metadata contains custom metadata about this example
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Dataset represents a collection of examples to evaluate.
type Dataset interface {
	// Name returns the dataset name
	Name() string

	// Load returns all examples in the dataset
	Load(ctx context.Context) ([]Example, error)

	// Len returns the number of examples
	Len() int

	// Get returns a specific example by index
	Get(idx int) (*Example, error)
}

// EvalInput represents the input to an evaluator.
type EvalInput struct {
	// Input is the original input from the example
	Input any

	// Output is the actual output produced by the system under test
	Output any

	// Reference is the expected output (optional)
	Reference any

	// Context provides additional context (e.g., retrieved documents)
	Context []any

	// Metadata contains custom metadata
	Metadata map[string]any
}

// EvalResult represents the result of evaluating a single example.
type EvalResult struct {
	// Pass indicates whether the evaluation passed
	Pass bool `json:"pass"`

	// Score is a numeric score (typically 0-1, but can be any range)
	Score float64 `json:"score"`

	// Reason provides a human-readable explanation of the result
	Reason string `json:"reason,omitempty"`

	// Metadata contains additional data about the evaluation
	Metadata map[string]any `json:"metadata,omitempty"`

	// Error contains any error that occurred during evaluation
	Error error `json:"error,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for EvalResult.
// This is needed because the Error field (type error) doesn't marshal properly.
func (r EvalResult) MarshalJSON() ([]byte, error) {
	type Alias EvalResult
	aux := struct {
		Alias
		Error string `json:"error,omitempty"`
	}{
		Alias: Alias(r),
	}
	if r.Error != nil {
		aux.Error = r.Error.Error()
	}
	return json.Marshal(aux)
}

// UnmarshalJSON implements custom JSON unmarshaling for EvalResult.
func (r *EvalResult) UnmarshalJSON(data []byte) error {
	type Alias EvalResult
	aux := struct {
		*Alias
		Error string `json:"error,omitempty"`
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Error != "" {
		r.Error = errors.New(aux.Error)
	}
	return nil
}

// Evaluator defines the interface for evaluation metrics.
type Evaluator interface {
	// Name returns the evaluator name
	Name() string

	// Evaluate runs evaluation on a single example
	Evaluate(ctx context.Context, input EvalInput) (*EvalResult, error)

	// SupportsStreaming indicates if evaluator can stream results
	SupportsStreaming() bool
}

// StreamingEvaluator extends Evaluator with streaming support.
type StreamingEvaluator interface {
	Evaluator

	// EvaluateStream runs evaluation with streaming callback
	EvaluateStream(ctx context.Context, input EvalInput, callback func(partial *EvalResult)) (*EvalResult, error)
}

// ExampleResult represents the evaluation result for a single example.
type ExampleResult struct {
	// Example is the original example that was evaluated
	Example Example `json:"example"`

	// Output is the actual output produced
	Output any `json:"output"`

	// Results contains the evaluation result from each evaluator
	Results map[string]*EvalResult `json:"results"`

	// Duration is how long it took to evaluate this example
	Duration time.Duration `json:"duration"`

	// Timestamp is when the evaluation was performed
	Timestamp time.Time `json:"timestamp"`
}

// Summary contains aggregate statistics across all examples.
type Summary struct {
	// TotalExamples is the total number of examples evaluated
	TotalExamples int `json:"total_examples"`

	// PassedExamples is the number of examples that passed all evaluators
	PassedExamples int `json:"passed_examples"`

	// FailedExamples is the number of examples that failed any evaluator
	FailedExamples int `json:"failed_examples"`

	// AverageScore is the average score across all evaluators and examples
	AverageScore float64 `json:"average_score"`

	// PassRate is the percentage of examples that passed (0-1)
	PassRate float64 `json:"pass_rate"`

	// TotalDuration is the total time spent evaluating
	TotalDuration time.Duration `json:"total_duration"`

	// EvaluatorStats contains per-evaluator statistics
	EvaluatorStats map[string]*EvaluatorStats `json:"evaluator_stats"`
}

// EvaluatorStats contains statistics for a single evaluator.
type EvaluatorStats struct {
	// Name is the evaluator name
	Name string `json:"name"`

	// TotalEvaluations is the number of evaluations performed
	TotalEvaluations int `json:"total_evaluations"`

	// Passed is the number of evaluations that passed
	Passed int `json:"passed"`

	// Failed is the number of evaluations that failed
	Failed int `json:"failed"`

	// Errors is the number of evaluations that resulted in errors
	Errors int `json:"errors"`

	// AverageScore is the average score for this evaluator
	AverageScore float64 `json:"average_score"`

	// PassRate is the pass rate for this evaluator (0-1)
	PassRate float64 `json:"pass_rate"`

	// AverageDuration is the average time per evaluation
	AverageDuration time.Duration `json:"average_duration"`
}

// Report contains the complete evaluation results.
type Report struct {
	// Summary contains aggregate statistics
	Summary Summary `json:"summary"`

	// Results contains per-example results
	Results []ExampleResult `json:"results"`

	// Config contains the configuration used for evaluation
	Config Config `json:"config"`

	// Timestamp is when the evaluation was started
	Timestamp time.Time `json:"timestamp"`
}

// TargetFunc is a function that produces output for a given example.
// This allows evaluating any system by providing a function that calls it.
type TargetFunc func(ctx context.Context, example Example) (output any, err error)
