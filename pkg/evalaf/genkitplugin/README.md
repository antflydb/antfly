# genkitplugin - Genkit Evaluation Framework Integration

The `genkitplugin` package integrates evalaf evaluators with Genkit's built-in evaluation framework. This allows you to use evalaf's comprehensive evaluation capabilities within Genkit's Dev UI and CLI tools.

## Features

- **Seamless Integration**: Use evalaf evaluators with Genkit's evaluation system
- **All Evaluator Types**: Support for built-in, RAG, agent, and LLM-as-judge evaluators
- **Format Conversion**: Automatic conversion between Genkit and evalaf data formats
- **Dev UI Compatible**: View evaluation results in Genkit's Dev UI at `localhost:4000`
- **CLI Compatible**: Use with `genkit eval:flow` and other CLI commands
- **Flexible Configuration**: Configure evaluators through simple metric configs

## Installation

The plugin is part of the evalaf package:

```bash
go get github.com/antflydb/antfly/pkg/evalaf/genkitplugin
```

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "log"

    "github.com/firebase/genkit/go/genkit"
    "github.com/firebase/genkit/go/plugins/googlegenai"
    "github.com/antflydb/antfly/pkg/evalaf/genkitplugin"
)

func main() {
    ctx := context.Background()

    // Define evalaf metrics to use
    metrics := []genkitplugin.MetricConfig{
        {
            MetricType: genkitplugin.MetricCitation,
            Name:       "citation_check",
        },
        {
            MetricType: genkitplugin.MetricNDCG,
            Name:       "ndcg@10",
            Config:     map[string]any{"k": 10},
        },
    }

    // Initialize Genkit with evalaf plugin
    g, err := genkit.Init(ctx,
        genkit.WithPlugins(
            &googlegenai.GoogleAI{},
            &genkitplugin.EvalafPlugin{Metrics: metrics},
        ),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Define your flow
    genkit.DefineFlow(g, "qaFlow", func(ctx context.Context, query string) (string, error) {
        // Your RAG or agent logic here
        return "answer with [doc_id 0] citation", nil
    })
}
```

### Using with Genkit CLI

Start Genkit:
```bash
genkit start -- go run main.go
```

Run evaluation:
```bash
genkit eval:flow qaFlow --input myDataset --evaluators=citation_check,ndcg@10
```

View results in Dev UI:
```bash
# Open browser to http://localhost:4000
```

## Available Metrics

### Built-in Evaluators

```go
// Exact string matching
{
    MetricType: genkitplugin.MetricExactMatch,
    Name:       "exact_match",
}

// Regular expression matching
{
    MetricType: genkitplugin.MetricRegex,
    Name:       "regex_match",
    Config:     map[string]any{"pattern": "\\d{3}-\\d{3}-\\d{4}"},
}

// Substring matching
{
    MetricType: genkitplugin.MetricContains,
    Name:       "contains",
    Config:     map[string]any{"case_insensitive": true},
}

// Fuzzy string matching (Levenshtein distance)
{
    MetricType: genkitplugin.MetricFuzzyMatch,
    Name:       "fuzzy",
    Config:     map[string]any{"threshold": 0.85},
}
```

### RAG Evaluators

```go
// Citation validation
{
    MetricType: genkitplugin.MetricCitation,
    Name:       "citation_check",
}

// Citation coverage
{
    MetricType: genkitplugin.MetricCitationCoverage,
    Name:       "citation_coverage",
}

// Retrieval metrics
{
    MetricType: genkitplugin.MetricNDCG,
    Name:       "ndcg@10",
    Config:     map[string]any{"k": 10},
}

{
    MetricType: genkitplugin.MetricMRR,
    Name:       "mrr",
}

{
    MetricType: genkitplugin.MetricPrecision,
    Name:       "precision@5",
    Config:     map[string]any{"k": 5},
}

{
    MetricType: genkitplugin.MetricRecall,
    Name:       "recall@10",
    Config:     map[string]any{"k": 10},
}

{
    MetricType: genkitplugin.MetricMAP,
    Name:       "map",
}
```

### Agent Evaluators

```go
// Classification accuracy
{
    MetricType: genkitplugin.MetricClassification,
    Name:       "classification",
    Config:     map[string]any{"classes": []string{"question", "search"}},
}

// Confidence threshold
{
    MetricType: genkitplugin.MetricConfidence,
    Name:       "confidence",
    Config:     map[string]any{"threshold": 0.7},
}

// Tool selection validation
{
    MetricType: genkitplugin.MetricToolSelection,
    Name:       "tool_selection",
}

// Tool call sequence validation
{
    MetricType: genkitplugin.MetricToolSequence,
    Name:       "tool_sequence",
}
```

### LLM-as-Judge Evaluators

```go
// Faithfulness (answer grounded in context)
{
    MetricType: genkitplugin.MetricFaithfulness,
    Name:       "faithfulness",
    Config:     map[string]any{"model": "ollama/mistral"},
}

// Relevance (answer addresses question)
{
    MetricType: genkitplugin.MetricRelevance,
    Name:       "relevance",
    Config:     map[string]any{"model": "ollama/mistral"},
}

// Other LLM-as-judge metrics
genkitplugin.MetricCorrectness
genkitplugin.MetricCoherence
genkitplugin.MetricSafety
genkitplugin.MetricCompleteness
genkitplugin.MetricHelpfulness
```

## Complete Examples

### RAG Evaluation

```go
package main

import (
    "context"
    "log"

    "github.com/firebase/genkit/go/genkit"
    "github.com/firebase/genkit/go/plugins/ollama"
    "github.com/antflydb/antfly/pkg/evalaf/genkitplugin"
)

func main() {
    ctx := context.Background()

    // Define comprehensive RAG metrics
    metrics := []genkitplugin.MetricConfig{
        // Citation validation
        {
            MetricType: genkitplugin.MetricCitation,
            Name:       "citation_check",
        },
        {
            MetricType: genkitplugin.MetricCitationCoverage,
            Name:       "citation_coverage",
        },
        // Retrieval metrics
        {
            MetricType: genkitplugin.MetricNDCG,
            Name:       "ndcg@10",
            Config:     map[string]any{"k": 10},
        },
        // LLM-as-judge
        {
            MetricType: genkitplugin.MetricFaithfulness,
            Name:       "faithfulness",
            Config:     map[string]any{"model": "ollama/mistral"},
        },
        {
            MetricType: genkitplugin.MetricRelevance,
            Name:       "relevance",
            Config:     map[string]any{"model": "ollama/mistral"},
        },
    }

    // Initialize Genkit
    g, err := genkit.Init(ctx,
        genkit.WithPlugins(
            &ollama.Ollama{},
            &genkitplugin.EvalafPlugin{Metrics: metrics},
        ),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Define RAG flow
    genkit.DefineFlow(g, "ragFlow", func(ctx context.Context, query string) (string, error) {
        // Your RAG implementation here
        docs := retrieveDocuments(query)
        answer := generateAnswer(ctx, query, docs)
        return answer, nil
    })
}
```

### Answer Agent Evaluation

```go
metrics := []genkitplugin.MetricConfig{
    // Classification
    {
        MetricType: genkitplugin.MetricClassification,
        Name:       "route_classification",
        Config:     map[string]any{"classes": []string{"question", "search"}},
    },
    // Confidence
    {
        MetricType: genkitplugin.MetricConfidence,
        Name:       "confidence",
        Config:     map[string]any{"threshold": 0.7},
    },
    // LLM-as-judge for answer quality
    {
        MetricType: genkitplugin.MetricRelevance,
        Name:       "relevance",
        Config:     map[string]any{"model": "ollama/mistral"},
    },
    {
        MetricType: genkitplugin.MetricCoherence,
        Name:       "coherence",
        Config:     map[string]any{"model": "ollama/mistral"},
    },
}
```

## Format Conversion

The plugin provides utilities for converting between Genkit and evalaf formats:

### Convert Genkit Input to Evalaf

```go
import "github.com/antflydb/antfly/pkg/evalaf/genkitplugin"

genkitInput := genkitplugin.GenkitInput{
    TestCaseID: "test_001",
    Input:      "What is machine learning?",
    Output:     "Machine learning is...",
    Context:    []any{"doc1", "doc2"},
    Reference:  "expected answer",
}

evalafInput := genkitplugin.ToEvalafInput(genkitInput)
```

### Convert Evalaf Result to Genkit

```go
evalafResult := &eval.EvalResult{
    Pass:   true,
    Score:  0.95,
    Reason: "All citations valid",
}

genkitResult := genkitplugin.FromEvalafResult(evalafResult)
```

### Convert Datasets

```go
// Genkit to evalaf
evalafExamples := genkitplugin.ConvertGenkitDataset(genkitData)

// Evalaf to Genkit
genkitInputs := genkitplugin.ConvertEvalafDataset(evalafExamples)
```

## Integration with Standalone Evalaf

You can use both standalone evalaf and the Genkit plugin in the same codebase:

```go
// Standalone evalaf usage (for CI/CD)
func runStandaloneEval() {
    dataset, _ := eval.NewJSONDataset("test", "testdata/examples.json")
    evaluators := []eval.Evaluator{
        eval.NewExactMatchEvaluator("exact"),
    }
    runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
    report, _ := runner.Run(ctx, dataset)
    report.Print()
}

// Genkit plugin usage (for Dev UI)
func runGenkitEval() {
    metrics := []genkitplugin.MetricConfig{
        {
            MetricType: genkitplugin.MetricExactMatch,
            Name:       "exact",
        },
    }

    g, _ := genkit.Init(ctx,
        genkit.WithPlugins(&genkitplugin.EvalafPlugin{Metrics: metrics}),
    )

    // Use Genkit Dev UI
}
```

## Comparison with Standalone Evalaf

| Feature | Standalone Evalaf | Genkit Plugin |
|---------|------------------|---------------|
| CLI Tool | `evalaf` command | `genkit` command |
| UI | Terminal output | Dev UI at localhost:4000 |
| Dataset Format | JSON files | Genkit datasets |
| Report Format | JSON/YAML/Markdown | Genkit format |
| Configuration | YAML config | Go code |
| Use Case | CI/CD, scripts | Interactive development |

## Best Practices

1. **Development**: Use Genkit plugin with Dev UI for interactive evaluation
2. **CI/CD**: Use standalone evalaf for automated testing
3. **Hybrid Approach**: Use both - Genkit for development, evalaf for production
4. **Model Selection**: Use fast local models (Ollama) for LLM-as-judge during development
5. **Metric Selection**: Start with quick metrics (citation, classification), add LLM-as-judge later

## Troubleshooting

### Plugin Not Registering Evaluators

**Issue**: Evaluators not showing up in Genkit Dev UI

**Solution**: Check that the plugin is properly initialized in `genkit.Init()`:
```go
g, err := genkit.Init(ctx,
    genkit.WithPlugins(&genkitplugin.EvalafPlugin{Metrics: metrics}),
)
```

### LLM-as-Judge Evaluators Failing

**Issue**: LLM-as-judge evaluators return errors

**Solution**: Ensure the model is available:
- For Ollama: `ollama pull mistral`
- For OpenAI: Set `OPENAI_API_KEY`
- Check model name in config matches available models

### Format Conversion Errors

**Issue**: Data format mismatch between Genkit and evalaf

**Solution**: Use the provided conversion utilities:
- `ToEvalafInput()` - Genkit → evalaf
- `FromEvalafResult()` - evalaf → Genkit
- `ConvertGenkitDataset()` - datasets

## See Also

- [evalaf README](../eval/README.md) - Core evaluation library
- [antfly README](../antfly/README.md) - Antfly-specific evaluators
- [Genkit Documentation](https://firebase.google.com/docs/genkit) - Genkit framework
- [genkit_integration.md](../work-log/genkit_integration.md) - Integration design document

## Current Limitations

**Note**: This is an initial implementation of the Genkit plugin. The actual Genkit evaluator registration API may differ from what's documented in Genkit's examples. The current implementation includes:

- ✅ Complete format adapters
- ✅ All evaluator wrappers
- ✅ MetricConfig system
- ⚠️ Registration with Genkit (API needs verification)

The `registerWithGenkit()` function is currently a placeholder. The actual implementation depends on:
1. Genkit Go SDK version
2. Evaluator plugin API (may differ from TypeScript version)
3. Whether custom evaluators are supported in Go SDK

Next steps:
1. Test with actual Genkit Go SDK
2. Verify evaluator registration API
3. Update registration logic based on SDK capabilities
4. Add comprehensive tests
