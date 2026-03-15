# Evalaf - Embeddable LLM/RAG/Agent Evaluation Framework

> "promptfoo for golang" - A flexible, embeddable evaluation framework for LLM applications

Evalaf is a comprehensive evaluation framework for testing and monitoring LLM applications, including RAG systems, agents, and general LLM outputs. Built as a reusable Go module, it can be embedded in any Go project or used as a standalone CLI tool.

## Features

- **Embeddable**: Use as a library in your Go projects
- **Extensible**: Plugin-based architecture for custom evaluators
- **Flexible**: Evaluate RAG, agents, LLMs, or any system
- **Configurable**: YAML/JSON configuration with runtime customization
- **Parallel**: Concurrent evaluation for fast execution
- **Multiple Output Formats**: JSON, YAML, Markdown, HTML
- **LLM-as-Judge**: Use LLMs to evaluate LLM outputs (via Genkit integration)
- **Production-Ready**: Extract and evaluate production traces

## Quick Start

### Installation

```bash
go get github.com/antflydb/anteval
```

### Basic Usage

```go
package main

import (
    "context"
    "github.com/antflydb/anteval/eval"
)

func main() {
    ctx := context.Background()

    // Create dataset
    dataset := eval.NewJSONDatasetFromExamples("test", []eval.Example{
        {Input: "What is 2+2?", Reference: "4"},
    })

    // Create evaluators
    evaluators := []eval.Evaluator{
        eval.NewExactMatchEvaluator("exact_match"),
    }

    // Define target function (your LLM/RAG/agent)
    target := func(ctx context.Context, example eval.Example) (any, error) {
        // Call your system here
        return "4", nil
    }

    // Run evaluation
    runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
    report, _ := runner.RunWithTarget(ctx, dataset, target)

    // Print results
    report.Print()
}
```

### Using the CLI

```bash
# Run evaluation with config file
evalaf run --config evalaf.yaml

# List available metrics
evalaf metrics list

# Validate dataset
evalaf datasets validate testdata/datasets/rag_quality.json
```

## Architecture

### Core Packages

- **`eval/`**: Core evaluation library (minimal dependencies)
  - Dataset loading and management
  - Evaluator interface and built-in metrics
  - Evaluation runner and reporting
  - Configuration management

- **`genkit/`**: Genkit integration for LLM-as-judge
  - LLM-based evaluators (faithfulness, relevance, safety)
  - Support for multiple models (Ollama, OpenAI, Gemini, Claude)
  - Streaming evaluation support

- **`rag/`**: RAG-specific evaluators
  - Faithfulness (answer grounded in documents)
  - Relevance (answer addresses query)
  - Citation accuracy
  - Retrieval metrics (NDCG, MRR, Precision@k, Recall@k)

- **`agent/`**: Agent-specific evaluators
  - Query classification accuracy
  - Tool selection correctness
  - Reasoning quality

- **`ui/`**: UI embedding helpers
  - Result formatting for dashboards
  - Visualization data structures

- **`cmd/evalaf/`**: CLI tool

## Built-in Evaluators

### Simple Evaluators (no LLM required)

- **Exact Match**: Output exactly matches reference
- **Regex**: Output matches regex pattern
- **Contains**: Output contains substring
- **Fuzzy Match**: Levenshtein distance-based similarity

### RAG Evaluators

- **Faithfulness**: Answer grounded in retrieved documents (LLM-as-judge)
- **Relevance**: Answer addresses the query (LLM-as-judge)
- **Citation Accuracy**: Validates citation references like `[doc_id X]`
- **NDCG@k**: Retrieval ranking quality
- **MRR**: Mean Reciprocal Rank
- **Precision@k / Recall@k**: Retrieval effectiveness

### Agent Evaluators

- **Classification Accuracy**: Correct query classification
- **Tool Selection**: Appropriate tool chosen
- **Reasoning Quality**: Logical and coherent reasoning (LLM-as-judge)

## Configuration

Create a `evalaf.yaml` configuration file:

```yaml
version: 1

evaluators:
  # LLM-as-judge evaluators
  faithfulness:
    type: genkit_llm_judge
    model: ollama/mistral
    temperature: 0.0

  relevance:
    type: genkit_llm_judge
    model: ollama/mistral
    temperature: 0.0

  # RAG evaluators
  citation_accuracy:
    type: rag_citation
    pattern: '\[doc_id\s+([^\]]+)\]'

  retrieval_ndcg:
    type: rag_retrieval_metric
    metric: ndcg
    k: 10

  # Simple evaluators
  exact_match:
    type: exact_match

  regex_check:
    type: regex
    pattern: "(?i)expected_pattern"

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

## Use Cases

### 1. Antfly RAG Evaluation

Evaluate Antfly's RAG system quality:

```go
// Evaluate RAG endpoint
ragEndpoint := "http://localhost:3210/api/v1/rag"

target := func(ctx context.Context, example eval.Example) (any, error) {
    return callAntflyRAG(ragEndpoint, example.Input)
}

runner.RunWithTarget(ctx, dataset, target)
```

### 2. searchaf Customer Monitoring

Monitor customer-specific prompts:

```go
// Evaluate customer's configuration
customerDataset := loadCustomerDataset(customerID)
customerConfig := loadCustomerConfig(customerID)

runner := eval.NewRunner(customerConfig, evaluators)
report, _ := runner.Run(ctx, customerDataset)

// Embed in searchaf dashboard
dashboardData := ui.FormatForDashboard(report)
```

### 3. CI/CD Regression Testing

```bash
# In CI/CD pipeline
evalaf run --config ci_eval.yaml --dataset testdata/regression.json

# Fail if pass rate < 95%
if [ $(jq '.summary.pass_rate < 0.95' results.json) = "true" ]; then
  exit 1
fi
```

### 4. A/B Testing Prompts

```go
// Compare two prompt variations
reportA, _ := runEvaluation(promptA, dataset)
reportB, _ := runEvaluation(promptB, dataset)

// Compare results
if reportB.Summary.AverageScore > reportA.Summary.AverageScore {
    fmt.Println("Prompt B is better!")
}
```

## Examples

See the `examples/` directory for complete examples:

- **`examples/simple/`**: Basic evaluation example
- **`examples/antfly/`**: Antfly RAG and Answer Agent evaluation
- **`examples/searchaf/`**: searchaf integration example

## Development

### Project Structure

```
evalaf/
├── eval/           # Core library
├── genkit/         # Genkit integration
├── rag/            # RAG evaluators
├── agent/          # Agent evaluators
├── ui/             # UI helpers
├── cmd/evalaf/     # CLI tool
├── examples/       # Usage examples
├── testdata/       # Test datasets
├── work-log/       # Design docs
└── docs/           # Documentation
```

### Running Examples

```bash
cd examples/simple
go run main.go
```

### Running Tests

```bash
go test ./...
```

## Module Path

This project uses the module path `github.com/antflydb/anteval` while living in the `evalaf/` directory of the antfly repository. This is a nested Go module.

## Dependencies

### Core (`eval/`)
- Minimal dependencies: stdlib + `gopkg.in/yaml.v3`

### Genkit Integration (`genkit/`)
- `github.com/firebase/genkit/go/genkit`
- `github.com/firebase/genkit/go/ai`

### CLI (`cmd/evalaf/`)
- `github.com/spf13/cobra`
- `github.com/spf13/viper`

## Roadmap

- [x] Core evaluation library
- [ ] Genkit integration (LLM-as-judge)
- [ ] RAG evaluators
- [ ] Agent evaluators
- [ ] CLI tool
- [ ] UI integration helpers
- [ ] Production trace extraction
- [ ] Web UI for evaluation management
- [ ] Continuous evaluation mode
- [ ] Multi-turn conversation evaluation

## Contributing

This project is part of the Antfly ecosystem. For contribution guidelines, see the main Antfly repository.

## License

[Your license here]

## Related Projects

- [Antfly](https://github.com/antflydb/antfly) - Distributed key-value store and vector search engine
- [Firebase Genkit](https://firebase.google.com/docs/genkit) - AI application framework
- [promptfoo](https://github.com/promptfoo/promptfoo) - Inspiration for this project

## Support

- Issues: [GitHub Issues](https://github.com/antflydb/antfly/issues)
- Documentation: See `docs/` directory
- Design Docs: See `work-log/PLAN.md`
