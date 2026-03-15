# agent - Agent-Specific Evaluators

The `agent` package provides evaluators for AI agent systems, including query classification, tool selection, and reasoning quality.

## Features

- Query classification accuracy
- Tool selection correctness
- Tool call sequence validation
- Confidence score evaluation
- Reasoning quality assessment (via LLM-as-judge)

## Classification Evaluators

### ClassificationEvaluator

Evaluates if the agent correctly classified the input.

```go
import "github.com/antflydb/antfly/pkg/evalaf/agent"

evaluator := agent.NewClassificationEvaluator("classification", []string{"question", "search"})
```

**Example:**
```go
eval.Example{
    Input:     "What is the capital of France?",
    Reference: "question",  // Expected classification
}

// Agent output can be:
// - Simple string: "question"
// - Structured: {"classification": "question", "confidence": 0.95}
// - Antfly format: {"route_type": "question", "confidence": 0.9}
```

### ConfidenceEvaluator

Checks if the confidence score is above a threshold.

```go
evaluator := agent.NewConfidenceEvaluator("confidence", 0.7) // 70% threshold
```

**Example:**
```go
eval.Example{
    Input: "What is the capital of France?",
}

// Agent output must include confidence:
// {"classification": "question", "confidence": 0.95}
```

## Tool Use Evaluators

### ToolSelectionEvaluator

Evaluates if the agent selected the correct tool(s).

```go
evaluator := agent.NewToolSelectionEvaluator("tool_selection")
```

**Example:**
```go
eval.Example{
    Input:     "Search for recent papers on transformers",
    Reference: "search_papers",  // Expected tool
    // Or multiple tools: []string{"search_papers", "summarize"}
}

// Agent output can be:
// - Simple string: "search_papers"
// - List: ["search_papers", "summarize"]
// - Structured: {"tool": "search_papers"} or {"tools": ["search_papers"]}
```

**Scoring:**
- Exact match: score = 1.0
- Partial match: score = overlap / expected_count
- No match: score = 0.0

### ToolCallSequenceEvaluator

Evaluates if tools were called in the correct order.

```go
evaluator := agent.NewToolCallSequenceEvaluator("sequence")
```

**Example:**
```go
eval.Example{
    Input:     "Search for papers and then summarize them",
    Reference: []string{"search_papers", "summarize", "format_output"},
}

// Agent output:
// - List: ["search_papers", "summarize", "format_output"]
// - Structured: {"tool_sequence": ["search_papers", "summarize"]}
```

**Scoring:**
- Uses Longest Common Subsequence (LCS) algorithm
- score = len(LCS) / len(expected_sequence)
- Allows partial credit for correct subsequences

## Complete Agent Evaluation Example

```go
package main

import (
    "context"
    "github.com/antflydb/antfly/pkg/evalaf/eval"
    "github.com/antflydb/antfly/pkg/evalaf/agent"
    "github.com/antflydb/antfly/pkg/evalaf/genkit"
    genkitpkg "github.com/firebase/genkit/go/genkit"
    "github.com/firebase/genkit/go/plugins/ollama"
)

func main() {
    ctx := context.Background()

    // Initialize Genkit for reasoning evaluation
    g, _ := genkitpkg.Init(ctx,
        genkitpkg.WithPlugins(&ollama.Ollama{}),
    )

    // Create evaluators
    evaluators := []eval.Evaluator{
        // Classification
        agent.NewClassificationEvaluator("classification", []string{"question", "search"}),
        agent.NewConfidenceEvaluator("confidence", 0.7),

        // Tool selection
        agent.NewToolSelectionEvaluator("tool_selection"),
        agent.NewToolCallSequenceEvaluator("sequence"),

        // Reasoning quality (LLM-as-judge)
        genkit.NewCoherenceEvaluator(g, "ollama/mistral"),
    }

    // Create dataset
    dataset := eval.NewJSONDatasetFromExamples("agent_eval", []eval.Example{
        {
            Input:     "What is the weather in Paris?",
            Reference: "question",  // Expected classification
            Metadata: map[string]any{
                "expected_tool": "get_weather",
            },
        },
        {
            Input:     "Find all documents about AI",
            Reference: "search",
            Metadata: map[string]any{
                "expected_tools":    []string{"search", "filter"},
                "expected_sequence": []string{"search", "filter", "rank"},
            },
        },
    })

    // Target function calls your agent
    target := func(ctx context.Context, example eval.Example) (any, error) {
        // Call your agent endpoint
        return callMyAgent(example.Input)
    }

    // Run evaluation
    runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
    report, _ := runner.RunWithTarget(ctx, dataset, target)

    report.Print()
}
```

## Evaluating Antfly's Answer Agent

```go
func evaluateAntflyAnswerAgent() {
    ctx := context.Background()

    // Create evaluators
    evaluators := []eval.Evaluator{
        agent.NewClassificationEvaluator("route_type", []string{"question", "search"}),
        agent.NewConfidenceEvaluator("confidence", 0.7),
    }

    // Dataset with expected classifications
    dataset := eval.NewJSONDatasetFromExamples("antfly_agent", []eval.Example{
        {
            Input:     "What is machine learning?",
            Reference: "question",
        },
        {
            Input:     "papers about transformers",
            Reference: "search",
        },
    })

    // Target function calls Antfly Answer Agent
    target := func(ctx context.Context, example eval.Example) (any, error) {
        // Call /api/v1/agents/answer
        resp, err := http.Post(
            "http://localhost:3210/api/v1/agents/answer",
            "application/json",
            createAgentRequest(example.Input),
        )

        // Parse response - extract classification from SSE stream
        // Returns: {"route_type": "question", "confidence": 0.95}
        return parseAgentResponse(resp)
    }

    runner := eval.NewRunner(eval.DefaultConfig(), evaluators)
    report, _ := runner.RunWithTarget(ctx, dataset, target)

    report.Print()
}
```

## Dataset Format

### Classification Dataset

```json
[
  {
    "input": "What is the weather?",
    "reference": "question",
    "metadata": {
      "domain": "weather"
    }
  },
  {
    "input": "papers about AI",
    "reference": "search",
    "metadata": {
      "domain": "academic"
    }
  }
]
```

### Tool Selection Dataset

```json
[
  {
    "input": "Get the weather in Paris",
    "reference": "get_weather",
    "metadata": {
      "category": "single_tool"
    }
  },
  {
    "input": "Search and summarize papers",
    "reference": ["search_papers", "summarize"],
    "metadata": {
      "category": "multi_tool",
      "expected_sequence": ["search_papers", "summarize", "format"]
    }
  }
]
```

## Best Practices

1. **Classification**:
   - Test edge cases and ambiguous inputs
   - Monitor confidence scores to detect uncertainty
   - Track per-class accuracy to find weak spots

2. **Tool Selection**:
   - Test both single and multi-tool scenarios
   - Validate tool sequences for multi-step tasks
   - Use partial credit scoring for complex tasks

3. **Confidence**:
   - Set appropriate thresholds (0.7-0.8 is common)
   - Low confidence may indicate need for clarification
   - Track confidence trends over time

4. **Continuous Monitoring**:
   - Run evaluations on production data
   - A/B test different classification models
   - Track accuracy degradation over time

## Integration with Other Packages

Combine with other evaluators for comprehensive assessment:

```go
evaluators := []eval.Evaluator{
    // Agent evaluators
    agent.NewClassificationEvaluator("classification", classes),
    agent.NewToolSelectionEvaluator("tools"),

    // RAG evaluators (if agent does RAG)
    rag.NewCitationEvaluator("citations"),

    // LLM-as-judge
    genkit.NewCoherenceEvaluator(g, "ollama/mistral"),
    genkit.NewHelpfulnessEvaluator(g, "ollama/mistral"),
}
```

## Metrics Summary

| Evaluator | Input | Output | Pass Criteria |
|-----------|-------|--------|---------------|
| ClassificationEvaluator | Query + expected class | Classification | Exact match |
| ConfidenceEvaluator | - | Confidence score | Above threshold |
| ToolSelectionEvaluator | Expected tools | Selected tools | Exact set match |
| ToolCallSequenceEvaluator | Expected sequence | Actual sequence | Exact order match |

## See Also

- [eval package](../eval/README.md) - Core evaluation library
- [genkit package](../genkit/README.md) - LLM-as-judge for reasoning quality
- [rag package](../rag/README.md) - RAG-specific evaluators
