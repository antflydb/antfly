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

package metadata

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/pkg/evalaf/eval"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/ollama"
	"github.com/stretchr/testify/require"
)

// skipIfOllamaUnavailable skips tests that require Ollama unless integration tests are enabled.
func skipIfOllamaUnavailable(t *testing.T) {
	t.Helper()
	if os.Getenv("SKIP_INTEGRATION") == "true" {
		t.Skip("Skipping retrieval agent integration test - requires Ollama (set SKIP_INTEGRATION=false to run)")
	}
	// Also skip eval tests unless RUN_EVAL_TESTS is set
	// This allows running integration tests while skipping eval-specific tests
	if os.Getenv("RUN_EVAL_TESTS") != "true" && isEvalTest(t) {
		t.Skip("Skipping eval test (set RUN_EVAL_TESTS=true to run)")
	}
}

// isEvalTest checks if the current test is an eval test based on naming convention.
// All eval tests should end with "Eval" suffix.
func isEvalTest(t *testing.T) bool {
	t.Helper()
	name := t.Name()

	// Check if test name ends with "Eval"
	return len(name) >= 4 && name[len(name)-4:] == "Eval"
}

// skipIfEvalDisabled skips comprehensive evaluation tests unless explicitly enabled.
// Set ENABLE_COMPREHENSIVE_EVAL=true to run LLM-as-judge evaluations.
func skipIfEvalDisabled(t *testing.T) {
	t.Helper()
	if os.Getenv("ENABLE_COMPREHENSIVE_EVAL") != "true" {
		t.Skip("Skipping comprehensive eval - requires LLM (set ENABLE_COMPREHENSIVE_EVAL=true to run)")
	}
	skipIfOllamaUnavailable(t)
}

// getDefaultOllamaConfig returns a default Ollama generator config for testing.
func getDefaultOllamaConfig(t *testing.T) *ai.GeneratorConfig {
	t.Helper()
	url := "http://localhost:11434"
	config, err := ai.NewGeneratorConfig(ai.OllamaGeneratorConfig{
		Model: "gemma3:4b",
		Url:   &url,
	})
	require.NoError(t, err)
	return config
}

// initGenkitForEval initializes a Genkit instance for LLM-as-judge evaluations.
func initGenkitForEval(t *testing.T) *genkit.Genkit {
	t.Helper()

	// Initialize Ollama plugin
	url := "http://localhost:11434"
	ollamaPlugin := &ollama.Ollama{ServerAddress: url}

	// Initialize Genkit with Ollama plugin
	g := genkit.Init(t.Context(),
		genkit.WithPlugins(ollamaPlugin),
	)

	// Define commonly used models for eval
	// This registers the models with Genkit so they can be used by name
	modelName := "gemma3:4b"
	ollamaPlugin.DefineModel(
		g,
		ollama.ModelDefinition{
			Name: modelName,
			Type: "chat",
		},
		nil, // Use default options
	)

	return g
}

// loadDataset is a helper to load a JSON dataset from testdata.
func loadDataset(t *testing.T, name, filename string) eval.Dataset {
	t.Helper()
	dataset, err := eval.NewJSONDataset(name, filename)
	require.NoError(t, err)
	return dataset
}

// saveReportToTestdata saves an evaluation report to testdata directory.
func saveReportToTestdata(t *testing.T, report *eval.Report, filename string) {
	t.Helper()
	testdataPath := "testdata/" + filename
	err := report.SaveToFile(testdataPath, "json", true)
	require.NoError(t, err)
	t.Logf("Saved evaluation report to %s", testdataPath)
}

// assertPassRate is a helper to assert minimum pass rate for evaluations.
func assertPassRate(t *testing.T, report *eval.Report, minPassRate float64, message string) {
	t.Helper()
	if report.Summary.PassRate < minPassRate {
		// Print detailed failure information
		t.Logf("Evaluation failed: Pass rate %.2f%% below threshold %.2f%%",
			report.Summary.PassRate*100, minPassRate*100)
		t.Logf("Summary: %s", mustMarshalJSON(report.Summary))

		// Show failed examples
		for _, result := range report.Results {
			// Check if example failed any evaluator
			failed := false
			for _, evalResult := range result.Results {
				if !evalResult.Pass {
					failed = true
					break
				}
			}
			if failed {
				t.Logf("Failed example: Input=%v, Output=%v",
					result.Example.Input, result.Output)
				for evalName, evalResult := range result.Results {
					if !evalResult.Pass {
						t.Logf("  - %s: %s (Score: %.2f)",
							evalName, evalResult.Reason, evalResult.Score)
					}
				}
			}
		}

		t.Errorf("%s: Pass rate %.2f%% below threshold %.2f%%",
			message, report.Summary.PassRate*100, minPassRate*100)
	}
}

// mustMarshalJSON is a helper to marshal JSON for logging.
func mustMarshalJSON(v any) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "<error marshaling JSON>"
	}
	return string(data)
}
