# eval - Core Evaluation Library

The `eval` package provides the core abstractions and functionality for evalaf. It contains minimal dependencies and can be used standalone or as a foundation for more complex evaluators.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/antflydb/anteval/eval"
)

func main() {
    ctx := context.Background()

    // Create a simple dataset
    dataset := eval.NewJSONDatasetFromExamples("test", []eval.Example{
        {
            Input:     "What is 2+2?",
            Reference: "4",
        },
        {
            Input:     "What is the capital of France?",
            Reference: "Paris",
        },
    })

    // Create evaluators
    evaluators := []eval.Evaluator{
        eval.NewExactMatchEvaluator("exact_match"),
    }

    // Create a simple target function
    target := func(ctx context.Context, example eval.Example) (any, error) {
        // In real usage, this would call your LLM, RAG system, etc.
        if example.Input == "What is 2+2?" {
            return "4", nil
        }
        return "Paris", nil
    }

    // Run evaluation
    runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
    report, err := runner.RunWithTarget(ctx, dataset, target)
    if err != nil {
        panic(err)
    }

    // Print results
    report.Print()
}
```

## Core Types

### Example

Represents a single test case:

```go
type Example struct {
    Input     any            // Input to evaluate
    Reference any            // Expected output (optional)
    Context   []any          // Additional context (e.g., retrieved docs)
    Metadata  map[string]any // Custom metadata
}
```

### Dataset

Interface for loading test examples:

```go
type Dataset interface {
    Name() string
    Load(ctx context.Context) ([]Example, error)
    Len() int
    Get(idx int) (*Example, error)
}
```

Implementations:
- `JSONDataset` - Load from JSON files

### Evaluator

Interface for evaluation metrics:

```go
type Evaluator interface {
    Name() string
    Evaluate(ctx context.Context, input EvalInput) (*EvalResult, error)
    SupportsStreaming() bool
}
```

Built-in evaluators:
- `ExactMatchEvaluator` - Exact string matching
- `RegexEvaluator` - Regex pattern matching
- `ContainsEvaluator` - Substring matching
- `FuzzyMatchEvaluator` - Levenshtein distance-based matching

### Runner

Orchestrates the evaluation process:

```go
runner := eval.NewRunner(config, evaluators)

// Evaluate with a target function
report, err := runner.RunWithTarget(ctx, dataset, targetFunc)

// Or evaluate pre-collected outputs
report, err := runner.Run(ctx, dataset)
```

## Configuration

Load configuration from YAML:

```yaml
version: 1

evaluators:
  exact_match:
    type: exact_match

  regex_check:
    type: regex
    pattern: "(?i)paris"

  fuzzy_match:
    type: fuzzy_match
    threshold: 0.8

datasets:
  - name: test_dataset
    path: testdata/test.json
    type: json

output:
  format: json
  path: results/report.json
  pretty: true

execution:
  parallel: true
  max_concurrency: 10
  timeout: 5m
```

Load in code:

```go
config, err := eval.LoadConfig("evalaf.yaml")
```

## Datasets

### JSON Dataset Format

```json
[
  {
    "input": "What is 2+2?",
    "reference": "4",
    "context": ["Math fact: 2+2 equals 4"],
    "metadata": {
      "domain": "math",
      "difficulty": "easy"
    }
  }
]
```

Load from file:

```go
dataset, err := eval.NewJSONDataset("my_dataset", "testdata/examples.json")
```

Load from bytes:

```go
dataset, err := eval.NewJSONDatasetFromBytes("my_dataset", jsonData)
```

Create programmatically:

```go
dataset := eval.NewJSONDatasetFromExamples("my_dataset", examples)
```

## Reports

Reports contain evaluation results with aggregate statistics:

```go
type Report struct {
    Summary   Summary           // Aggregate stats
    Results   []ExampleResult   // Per-example results
    Config    Config            // Configuration used
    Timestamp time.Time         // When evaluation started
}
```

### Output Formats

Print to console:

```go
report.Print()
```

Save as JSON:

```go
err := report.SaveToFile("results.json", "json", true)
```

Save as YAML:

```go
err := report.SaveToFile("results.yaml", "yaml", false)
```

Save as Markdown:

```go
err := report.SaveToFile("results.md", "markdown", false)
```

Auto-save using config:

```go
err := report.Save() // Uses config.Output settings
```

## Creating Custom Evaluators

Implement the `Evaluator` interface:

```go
type MyCustomEvaluator struct {
    name string
}

func (e *MyCustomEvaluator) Name() string {
    return e.name
}

func (e *MyCustomEvaluator) Evaluate(ctx context.Context, input eval.EvalInput) (*eval.EvalResult, error) {
    // Your evaluation logic here
    pass := true
    score := 1.0
    reason := "Evaluation passed"

    return &eval.EvalResult{
        Pass:   pass,
        Score:  score,
        Reason: reason,
    }, nil
}

func (e *MyCustomEvaluator) SupportsStreaming() bool {
    return false
}
```

## Parallel Execution

Control parallelism via configuration:

```yaml
execution:
  parallel: true          # Run examples in parallel
  max_concurrency: 10     # Max concurrent evaluations
```

Or in code:

```go
config := eval.DefaultConfig()
config.Execution.Parallel = true
config.Execution.MaxConcurrency = 20
```

## Dependencies

Minimal dependencies:
- `gopkg.in/yaml.v3` - YAML parsing

No external ML/AI libraries required.
