# rag - RAG-Specific Evaluators

The `rag` package provides evaluators specifically designed for Retrieval-Augmented Generation (RAG) systems. These evaluators assess both retrieval quality and generation quality.

## Features

- Citation accuracy validation
- Citation coverage analysis
- Retrieval effectiveness metrics (NDCG, MRR, Precision@k, Recall@k, MAP)
- Integration with LLM-as-judge for faithfulness and relevance

## Citation Evaluators

### CitationEvaluator

Validates that citations in the output are accurate and valid.

```go
import "github.com/antflydb/antfly/pkg/evalaf/rag"

evaluator := rag.NewCitationEvaluator("citation_check")
```

**Features:**
- Validates citation format (e.g., `[doc_id 0]`, `[doc_id 0, 1, 2]`)
- Checks if cited document IDs are within range
- Detects malformed citations

**Example Input:**
```go
eval.EvalInput{
    Input: "What is the capital of France?",
    Output: "The capital of France is Paris [doc_id 1].",
    Context: []any{
        "France is a country in Europe.",
        "Paris is the capital of France.",
    },
}
```

**Custom Citation Pattern:**
```go
evaluator, err := rag.NewCitationEvaluatorWithPattern(
    "custom_citation",
    `\[source:\s*(\d+)\]`, // Matches [source: 0]
)
```

### CitationCoverageEvaluator

Checks if all (or most) context documents are cited in the output.

```go
evaluator := rag.NewCitationCoverageEvaluator("coverage")
```

**Pass Criteria:**
- At least 80% of context documents should be cited
- Useful for ensuring comprehensive use of retrieved documents

## Retrieval Evaluators

### RetrievalEvaluator

Evaluates retrieval quality using various information retrieval metrics.

**Supported Metrics:**
- **NDCG@k**: Normalized Discounted Cumulative Gain
- **MRR**: Mean Reciprocal Rank
- **Precision@k**: Precision at k
- **Recall@k**: Recall at k
- **MAP**: Mean Average Precision

```go
import "github.com/antflydb/antfly/pkg/evalaf/rag"

// NDCG@10
ndcg := rag.NewRetrievalEvaluator("ndcg@10", rag.MetricNDCG, 10)

// MRR
mrr := rag.NewRetrievalEvaluator("mrr", rag.MetricMRR, 0)

// Precision@5
precision := rag.NewRetrievalEvaluator("precision@5", rag.MetricPrecision, 5)

// Recall@10
recall := rag.NewRetrievalEvaluator("recall@10", rag.MetricRecall, 10)

// MAP
mapMetric := rag.NewRetrievalEvaluator("map", rag.MetricMAP, 0)
```

**Input Format:**

Retrieval evaluators expect `retrieved_ids` and `relevant_ids` in the example metadata:

```go
eval.Example{
    Input: "What is machine learning?",
    Metadata: map[string]any{
        "retrieved_ids": []int{5, 12, 3, 8, 1},  // Order matters!
        "relevant_ids":  []int{3, 8},            // Ground truth relevant docs
    },
}
```

**Metrics Explained:**

1. **NDCG@k** (Normalized Discounted Cumulative Gain)
   - Measures ranking quality
   - Higher weight for relevant docs at top positions
   - Score: 0-1 (1 = perfect ranking)
   - Pass threshold: 0.7

2. **MRR** (Mean Reciprocal Rank)
   - Reciprocal of the rank of the first relevant document
   - Example: First relevant at position 3 → MRR = 1/3 = 0.333
   - Score: 0-1 (1 = first doc is relevant)
   - Pass threshold: 0.5

3. **Precision@k**
   - Fraction of top-k retrieved docs that are relevant
   - Example: 3 relevant in top 5 → P@5 = 3/5 = 0.6
   - Score: 0-1
   - Pass threshold: 0.5

4. **Recall@k**
   - Fraction of relevant docs found in top-k
   - Example: 2 of 5 relevant docs in top 10 → R@10 = 2/5 = 0.4
   - Score: 0-1
   - Pass threshold: 0.5

5. **MAP** (Mean Average Precision)
   - Average of precision values at each relevant document position
   - Considers both precision and recall
   - Score: 0-1
   - Pass threshold: 0.5

## Complete RAG Evaluation Example

```go
package main

import (
    "context"
    "github.com/antflydb/antfly/pkg/evalaf/eval"
    "github.com/antflydb/antfly/pkg/evalaf/rag"
    "github.com/antflydb/antfly/pkg/evalaf/genkit"
    genkitpkg "github.com/firebase/genkit/go/genkit"
    "github.com/firebase/genkit/go/plugins/ollama"
)

func main() {
    ctx := context.Background()

    // Initialize Genkit
    g, _ := genkitpkg.Init(ctx,
        genkitpkg.WithPlugins(&ollama.Ollama{}),
    )

    // Create evaluators
    evaluators := []eval.Evaluator{
        // Citation quality
        rag.NewCitationEvaluator("citations"),
        rag.NewCitationCoverageEvaluator("coverage"),

        // Retrieval metrics
        rag.NewRetrievalEvaluator("ndcg@10", rag.MetricNDCG, 10),
        rag.NewRetrievalEvaluator("precision@5", rag.MetricPrecision, 5),
        rag.NewRetrievalEvaluator("recall@10", rag.MetricRecall, 10),

        // LLM-as-judge
        genkit.NewFaithfulnessEvaluator(g, "ollama/mistral"),
        genkit.NewRelevanceEvaluator(g, "ollama/mistral"),
    }

    // Create dataset
    dataset := eval.NewJSONDatasetFromExamples("rag_eval", []eval.Example{
        {
            Input: "What is the capital of France?",
            Context: []any{
                "France is a country in Europe.",
                "Paris is the capital of France.",
                "The Eiffel Tower is in Paris.",
            },
            Metadata: map[string]any{
                "retrieved_ids": []int{1, 0, 2}, // Retrieved in this order
                "relevant_ids":  []int{1, 2},    // Docs 1 and 2 are relevant
            },
        },
    })

    // Target function (your RAG system)
    target := func(ctx context.Context, example eval.Example) (any, error) {
        // Call your RAG system here
        // For demo, return a sample answer
        return "The capital of France is Paris [doc_id 1]. It's known for the Eiffel Tower [doc_id 2].", nil
    }

    // Run evaluation
    runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
    report, _ := runner.RunWithTarget(ctx, dataset, target)

    // Print results
    report.Print()
}
```

## Evaluation Dataset Format

For RAG evaluation, use this format:

```json
[
  {
    "input": "What is machine learning?",
    "context": [
      "Machine learning is a subset of AI.",
      "It involves training models on data.",
      "Neural networks are a type of ML model."
    ],
    "metadata": {
      "retrieved_ids": [0, 1, 2],
      "relevant_ids": [0, 1],
      "domain": "ai"
    }
  }
]
```

## Integration with Antfly

### Evaluating Antfly's RAG Endpoint

```go
func evaluateAntflyRAG() {
    // Create evaluators
    evaluators := []eval.Evaluator{
        rag.NewCitationEvaluator("citations"),
        genkit.NewFaithfulnessEvaluator(g, "ollama/mistral"),
    }

    // Target function calls Antfly RAG API
    target := func(ctx context.Context, example eval.Example) (any, error) {
        resp, err := http.Post(
            "http://localhost:3210/api/v1/rag",
            "application/json",
            bytes.NewBuffer(ragRequest),
        )
        // Parse response and return answer
        return answer, nil
    }

    runner := eval.NewRunner(config, evaluators)
    report, _ := runner.RunWithTarget(ctx, dataset, target)
}
```

### Evaluating Antfly's Answer Agent

```go
target := func(ctx context.Context, example eval.Example) (any, error) {
    resp, err := http.Post(
        "http://localhost:3210/api/v1/agents/answer",
        "application/json",
        bytes.NewBuffer(agentRequest),
    )
    // Parse streaming response
    return answer, nil
}
```

## Best Practices

1. **Citation Validation**: Always validate citations to ensure answer quality
2. **Multiple Metrics**: Use multiple retrieval metrics for comprehensive evaluation
3. **NDCG for Ranking**: Use NDCG when ranking quality matters
4. **MRR for Search**: Use MRR when first result is most important
5. **Precision/Recall Trade-off**: Balance precision and recall based on use case
6. **LLM-as-Judge**: Combine with LLM-based evaluators for comprehensive quality assessment

## See Also

- [eval package](../eval/README.md) - Core evaluation library
- [genkit package](../genkit/README.md) - LLM-as-judge evaluators
- [Information Retrieval Metrics](https://en.wikipedia.org/wiki/Evaluation_measures_(information_retrieval))
