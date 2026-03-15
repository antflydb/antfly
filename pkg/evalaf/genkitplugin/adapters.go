package genkitplugin

import (
	"fmt"
	"maps"
	"strings"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/firebase/genkit/go/ai"
)

// ToEvalafInput converts Genkit ai.Example to evalaf EvalInput format
func ToEvalafInput(example ai.Example) eval.EvalInput {
	return eval.EvalInput{
		Input:     example.Input,
		Output:    example.Output,
		Reference: example.Reference,
		Context:   example.Context,
		Metadata: map[string]any{
			"test_case_id": example.TestCaseId,
			"trace_ids":    example.TraceIds,
		},
	}
}

// FromEvalafResult converts evalaf EvalResult to Genkit ai.Score
func FromEvalafResult(evalResult *eval.EvalResult, evaluatorName string) ai.Score {
	score := ai.Score{
		Id: evaluatorName,
	}

	// Set score value
	score.Score = evalResult.Score

	// Set status based on pass/fail
	if evalResult.Error != nil {
		score.Status = "FAIL"
		score.Error = evalResult.Error.Error()
	} else if evalResult.Pass {
		score.Status = "PASS"
	} else {
		score.Status = "FAIL"
	}

	// Add details
	score.Details = map[string]any{
		"reason": evalResult.Reason,
	}
	if evalResult.Metadata != nil {
		maps.Copy(score.Details, evalResult.Metadata)
	}

	return score
}

// ConvertGenkitDataset converts a Genkit dataset (array of ai.Example) to evalaf format
func ConvertGenkitDataset(examples []ai.Example) []eval.Example {
	evalExamples := make([]eval.Example, len(examples))
	for i, example := range examples {
		evalExamples[i] = eval.Example{
			Input:     example.Input,
			Reference: example.Reference,
			Context:   example.Context,
			Metadata: map[string]any{
				"test_case_id": example.TestCaseId,
				"trace_ids":    example.TraceIds,
				"output":       example.Output,
			},
		}
	}
	return evalExamples
}

// ConvertEvalafDataset converts evalaf dataset to Genkit ai.Example format
// Useful for exporting evalaf datasets to use with Genkit Dev UI
func ConvertEvalafDataset(examples []eval.Example) []ai.Example {
	genkitExamples := make([]ai.Example, len(examples))
	for i, example := range examples {
		gExample := ai.Example{
			Input:     example.Input,
			Reference: example.Reference,
			Context:   example.Context,
		}

		// Extract test case ID and trace IDs from metadata if present
		if example.Metadata != nil {
			if testCaseID, ok := example.Metadata["test_case_id"].(string); ok {
				gExample.TestCaseId = testCaseID
			} else {
				gExample.TestCaseId = fmt.Sprintf("example_%d", i)
			}

			if traceIDs, ok := example.Metadata["trace_ids"].([]string); ok {
				gExample.TraceIds = traceIDs
			}

			// If output is stored in metadata (from previous evaluation)
			if output, ok := example.Metadata["output"]; ok {
				gExample.Output = output
			}
		} else {
			gExample.TestCaseId = fmt.Sprintf("example_%d", i)
		}

		genkitExamples[i] = gExample
	}
	return genkitExamples
}

// ConvertEvalafReport converts evalaf report to Genkit evaluation results
// Returns a slice of ai.EvaluationResult, one per example
func ConvertEvalafReport(report *eval.Report) []ai.EvaluationResult {
	results := make([]ai.EvaluationResult, len(report.Results))
	for i, exampleResult := range report.Results {
		// Create scores for each evaluator
		scores := make([]ai.Score, 0, len(exampleResult.Results))
		for evalName, evalResult := range exampleResult.Results {
			score := FromEvalafResult(evalResult, evalName)
			scores = append(scores, score)
		}

		// Extract test case ID from example metadata
		testCaseID := fmt.Sprintf("example_%d", i)
		if exampleResult.Example.Metadata != nil {
			if id, ok := exampleResult.Example.Metadata["test_case_id"].(string); ok {
				testCaseID = id
			}
		}

		results[i] = ai.EvaluationResult{
			TestCaseId: testCaseID,
			Evaluation: scores,
		}
	}
	return results
}

// MergeResults combines multiple evaluator results into a single ai.Score
// This is useful when running multiple evalaf evaluators on the same input
func MergeResults(results []*eval.EvalResult, evaluatorName string) ai.Score {
	allPass := true
	totalScore := 0.0
	reasons := []string{}
	errors := []string{}

	for _, result := range results {
		if result.Error != nil {
			allPass = false
			errors = append(errors, result.Error.Error())
		}
		if !result.Pass {
			allPass = false
		}
		totalScore += result.Score
		if result.Reason != "" {
			reasons = append(reasons, result.Reason)
		}
	}

	avgScore := 0.0
	if len(results) > 0 {
		avgScore = totalScore / float64(len(results))
	}

	score := ai.Score{
		Id:     evaluatorName,
		Score:  avgScore,
		Status: "PASS",
		Details: map[string]any{
			"reason": strings.Join(reasons, "; "),
		},
	}

	if !allPass {
		score.Status = "FAIL"
	}

	if len(errors) > 0 {
		score.Error = strings.Join(errors, "; ")
	}

	return score
}
