# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Evalaf is an embeddable evaluation framework for testing and monitoring LLM applications, including RAG systems, agents, and general LLM outputs. It's designed as a reusable Go module that can be embedded in any Go project or used as a standalone CLI tool - essentially "promptfoo for golang".

**Module Path**: `github.com/antflydb/antfly/pkg/evalaf` (while living in the `evalaf/` directory of the antfly repository as a nested Go module)

## Architecture

### Package Structure

The codebase follows a layered architecture with minimal dependencies at the core:

- **`eval/`**: Core evaluation library with minimal dependencies (stdlib + `gopkg.in/yaml.v3`)
  - `types.go`: Core interfaces (Dataset, Evaluator, Example, EvalResult, Report)
  - `runner.go`: Evaluation orchestration with parallel/sequential execution
  - `dataset.go`: Dataset loading and management
  - `metrics.go`: Built-in evaluators (exact match, regex, contains, fuzzy match)
  - `config.go`: YAML/JSON configuration
  - `report.go`: Result reporting and formatting

- **`genkit/`**: Firebase Genkit integration for LLM-as-judge evaluators
  - Creates LLM-based evaluators for subjective metrics (faithfulness, relevance, safety, etc.)
  - Supports multiple models (Ollama, OpenAI, Gemini, Claude)
  - Uses template-based prompts with structured JSON output

- **`genkitplugin/`**: Genkit evaluation framework integration
  - Adapts evalaf evaluators to work with Genkit's built-in evaluation system
  - Enables using evalaf evaluators in Genkit Dev UI (localhost:4000)
  - Format conversion utilities between Genkit and evalaf data structures

- **`rag/`**: RAG-specific evaluators
  - Citation validation and coverage metrics
  - Retrieval metrics (NDCG, MRR, Precision@k, Recall@k, MAP)

- **`agent/`**: Agent-specific evaluators
  - Query classification accuracy
  - Tool selection correctness
  - Confidence threshold validation

- **`redteam/`**: Red-teaming and security evaluators
  - Prompt injection and jailbreak detection
  - Harmful content detection (violence, hate, sexual, illegal, self-harm, misinformation, toxicity)
  - PII leakage detection (email, phone, SSN, credit cards, API keys, passwords)
  - Refusal detection and unauthorized access checks
  - LLM-based judges for sophisticated security analysis
  - Preset configurations (Quick, Safety, Privacy, Security, Comprehensive, LLM Judge, Hybrid)

- **`antfly/`**: Antfly-specific integration
  - Target functions for calling Antfly RAG and Answer Agent endpoints
  - Preset evaluator configurations (Quick, RAG, AnswerAgent, Comprehensive)
  - Citation parsers for Antfly's `[doc_id X]` format

- **`ui/`**: UI embedding helpers for dashboard integration
- **`cmd/evalaf/`**: CLI tool built with cobra/viper
- **`examples/`**: Usage examples (simple, antfly, genkit)

### Key Design Patterns

**Evaluator Interface**: All evaluators implement the `Evaluator` interface from `eval/types.go`:
```go
type Evaluator interface {
    Name() string
    Evaluate(ctx context.Context, input EvalInput) (*EvalResult, error)
    SupportsStreaming() bool
}
```

**Target Function Pattern**: Evaluations use a `TargetFunc` to produce outputs:
```go
type TargetFunc func(ctx context.Context, example Example) (output any, err error)
```
This decouples the evaluation framework from the system being tested.

**Preset Pattern**: Multiple packages provide preset evaluator configurations:

*Antfly presets:*
- `QuickEvaluatorPreset()`: Fast, no LLM calls (citations, classification)
- `RAGEvaluatorPreset()`: RAG-focused (citations + LLM-as-judge for faithfulness/relevance)
- `AnswerAgentEvaluatorPreset()`: Agent-focused (classification + confidence + quality)
- `ComprehensiveEvaluatorPreset()`: All evaluators combined

*Red-team presets:*
- `QuickRedTeamPreset()`: Fast security checks, no LLM calls (prompt injection, jailbreak, PII)
- `SafetyRedTeamPreset()`: Harmful content detection
- `PrivacyRedTeamPreset()`: Data leakage and PII detection
- `SecurityRedTeamPreset()`: Security threat detection
- `ComprehensiveRedTeamPreset()`: All pattern-based red-team evaluators
- `LLMJudgeRedTeamPreset()`: LLM-based security analysis (requires Genkit)
- `HybridRedTeamPreset()`: Pattern + LLM combination

**Format Adapters**: The `genkitplugin/adapters.go` provides bidirectional conversion between Genkit and evalaf formats.

## Development Commands

### Running Tests

```bash
# Run all tests
go test ./...

# Run specific package tests
go test ./eval
go test ./genkit
go test ./genkitplugin
go test ./redteam

# Run with race detector
go test -race ./...

# Run specific test
go test ./eval -run TestRunnerSequential
```

### Building

```bash
# Build CLI tool
go build -o evalaf ./cmd/evalaf

# Install CLI globally
go install ./cmd/evalaf

# Build examples
go build ./examples/simple
go build ./examples/antfly
go build ./examples/genkit
go build ./examples/redteam
```

### Running Examples

```bash
# Simple evaluation example
cd examples/simple && go run main.go

# Antfly integration example
cd examples/antfly && go run main.go

# Genkit integration example
cd examples/genkit && go run main.go

# Red-teaming example
cd examples/redteam && go run main.go
```

### Using the CLI

```bash
# Run evaluation with config
evalaf run --config evalaf.yaml

# Run with specific dataset
evalaf run --config evalaf.yaml --dataset testdata/datasets/simple_qa.json

# Run specific evaluators only
evalaf run --config evalaf.yaml --evaluators faithfulness,relevance

# List available metrics
evalaf metrics list

# Validate dataset
evalaf datasets validate testdata/datasets/simple_qa.json
```

## Configuration

Evalaf uses YAML configuration files (typically `evalaf.yaml`):

```yaml
version: 1

evaluators:
  faithfulness:
    type: genkit_llm_judge
    model: ollama/mistral
    temperature: 0.0

  citation_accuracy:
    type: rag_citation
    pattern: '\[doc_id\s+([^\]]+)\]'

  exact_match:
    type: exact_match

datasets:
  - name: rag_quality
    path: testdata/datasets/rag_quality.json
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

## Dataset Format

Datasets are JSON arrays of examples:

```json
[
  {
    "input": "What is the capital of France?",
    "reference": "Paris",
    "context": [
      "France is a country in Europe",
      "Paris is the capital and largest city of France"
    ],
    "metadata": {
      "domain": "geography",
      "difficulty": "easy"
    }
  }
]
```

## Integration Patterns

### Standalone Library Usage

```go
// Create dataset
dataset := eval.NewJSONDatasetFromExamples("test", []eval.Example{
    {Input: "What is 2+2?", Reference: "4"},
})

// Create evaluators
evaluators := []eval.Evaluator{
    eval.NewExactMatchEvaluator("exact_match"),
}

// Define target function
target := func(ctx context.Context, example eval.Example) (any, error) {
    return callMySystem(example.Input)
}

// Run evaluation
runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
report, _ := runner.RunWithTarget(ctx, dataset, target)
report.Print()
```

### Genkit Plugin Usage

For interactive development with Genkit Dev UI:

```go
metrics := []genkitplugin.MetricConfig{
    {
        MetricType: genkitplugin.MetricCitation,
        Name:       "citation_check",
    },
}

g, _ := genkit.Init(ctx,
    genkit.WithPlugins(&genkitplugin.EvalafPlugin{Metrics: metrics}),
)

// Use Genkit Dev UI at localhost:4000
```

### Antfly Integration

For evaluating Antfly RAG/Answer Agent:

```go
client := antfly.NewClient("http://localhost:3210")

// Use preset evaluators
evaluators := antfly.RAGEvaluatorPreset(nil, "")

// Create target function that calls Antfly
target := client.CreateRAGTargetFunc([]string{"my_table"})

// Run evaluation
runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
report, _ := runner.RunWithTarget(ctx, dataset, target)
```

### Red-Team Security Testing

For security and safety evaluation:

```go
// Quick pattern-based security checks (no LLM, fast for CI/CD)
evaluators := redteam.QuickRedTeamPreset()

// Or comprehensive LLM-based security analysis
g, _ := genkit.Init(ctx, genkit.WithPlugins(&ollama.Ollama{}))
evaluators, _ := redteam.LLMJudgeRedTeamPreset(g, "ollama/mistral")

// Custom configuration
evaluators := redteam.NewCustomRedTeamPreset().
    WithPromptInjection(true).
    WithPIILeakage([]redteam.PIIType{redteam.PIIEmail, redteam.PIIPhone}).
    WithHarmfulContent([]redteam.HarmCategory{redteam.HarmViolence}).
    Build()

// Run security evaluation
runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
report, _ := runner.RunWithTarget(ctx, dataset, target)
```

## LLM-as-Judge

The `genkit/` package provides LLM-based evaluators:

- Uses Firebase Genkit for model abstraction
- Supports Ollama (local), OpenAI, Gemini, Claude
- Template-based prompts with structured JSON output
- Temperature 0.0 recommended for consistent evaluation
- Pre-defined prompts for common metrics (faithfulness, relevance, safety, etc.)

**Creating LLM evaluators**:
```go
// Initialize Genkit with a model provider
g, _ := genkit.Init(ctx, genkit.WithPlugins(&ollama.Ollama{}))

// Create evaluators
faithfulness, _ := genkit.NewFaithfulnessEvaluator(g, "ollama/mistral")
relevance, _ := genkit.NewRelevanceEvaluator(g, "ollama/mistral")

// Or create custom evaluators with custom prompts
custom, _ := genkit.NewCustomEvaluator(g, "my_evaluator", "ollama/mistral", myPromptTemplate)
```

## Execution Modes

**Sequential**: Evaluates examples one at a time (useful for debugging)
```go
config.Execution.Parallel = false
```

**Parallel**: Evaluates examples concurrently with configurable concurrency limit
```go
config.Execution.Parallel = true
config.Execution.MaxConcurrency = 10
```

The runner can also run evaluators in parallel within each example evaluation.

## Common Use Cases

1. **CI/CD Regression Testing**: Use Quick preset without LLM calls for fast feedback
2. **Development**: Use Genkit plugin with Dev UI for interactive evaluation
3. **Production Monitoring**: Extract production traces and evaluate with `runner.Run()` (expects output in example metadata)
4. **A/B Testing**: Compare prompts/models by running evaluations on same dataset
5. **Antfly RAG/Agent Evaluation**: Use Antfly-specific presets and target functions
6. **Security Red-Teaming**: Use `QuickRedTeamPreset()` in CI/CD for fast security checks, or `LLMJudgeRedTeamPreset()` for comprehensive security audits (prompt injection, jailbreak, PII leakage, harmful content detection)

## Dependencies

- **Core**: Minimal (stdlib + `gopkg.in/yaml.v3`)
- **Genkit Integration**: `github.com/firebase/genkit/go/genkit`, `github.com/firebase/genkit/go/ai`
- **CLI**: `github.com/spf13/cobra`, `github.com/spf13/viper`

## Module Information

This is a **nested Go module** within the Antfly repository:
- Module path: `github.com/antflydb/antfly/pkg/evalaf`
- Directory: `evalaf/` in antfly repo
- Go version: 1.26

When importing:
```go
import "github.com/antflydb/antfly/pkg/evalaf/eval"
import "github.com/antflydb/antfly/pkg/evalaf/genkit"
import "github.com/antflydb/antfly/pkg/evalaf/redteam"
import "github.com/antflydb/antfly/pkg/evalaf/rag"
```

## Testing Strategy

- Unit tests in `eval/eval_test.go` test core evaluation logic
- Integration tests in `genkitplugin/integration_test.go` test Genkit adapter
- Red-team tests in `redteam/redteam_test.go` test security evaluators
- Examples serve as integration tests and documentation
- Testdata datasets in `testdata/datasets/` for example evaluations
