# genkit - Genkit Integration for LLM-as-Judge

The `genkit` package provides LLM-as-judge evaluators using Firebase Genkit. It enables using LLMs to evaluate LLM outputs for qualities like faithfulness, relevance, safety, and more.

## Features

- LLM-as-judge pattern for subjective evaluation
- Handlebars-based dotprompt system for prompt management
- Pre-defined prompts for common evaluation tasks
- Support for multiple models (Ollama, OpenAI, Gemini, Claude)
- Structured JSON output parsing
- Model and configuration defined in `.prompt` files

## Installation

```bash
go get github.com/firebase/genkit/go/genkit
go get github.com/firebase/genkit/go/ai
```

## Quick Start

```go
package main

import (
    "context"
    "github.com/antflydb/antfly/pkg/evalaf/eval"
    "github.com/antflydb/antfly/pkg/evalaf/genkit"
    genkitpkg "github.com/firebase/genkit/go/genkit"
    "github.com/firebase/genkit/go/plugins/ollama"
)

func main() {
    ctx := context.Background()

    // Initialize Genkit with Ollama plugin and prompt directory
    g, _ := genkitpkg.Init(ctx,
        genkitpkg.WithPlugins(&ollama.Ollama{}),
        genkitpkg.WithPromptDir("./genkit/prompts"),
    )

    // Create LLM-as-judge evaluators (model specified in .prompt files)
    faithfulness, _ := genkit.NewFaithfulnessEvaluator(g)
    relevance, _ := genkit.NewRelevanceEvaluator(g)

    // Use in evaluation
    evaluators := []eval.Evaluator{faithfulness, relevance}
    runner := eval.NewRunner(eval.DefaultConfig(), evaluators)

    // ... run evaluation
}
```

## Available Evaluators

### Pre-defined Evaluators

Each evaluator has a corresponding `.prompt` file in `genkit/prompts/`:

- **Faithfulness** (`faithfulness.prompt`): Answer is grounded in provided context
- **Relevance** (`relevance.prompt`): Answer addresses the question
- **Correctness** (`correctness.prompt`): Answer is factually correct
- **Coherence** (`coherence.prompt`): Answer is logically coherent
- **Safety** (`safety.prompt`): Answer is safe and non-harmful
- **Completeness** (`completeness.prompt`): Answer is comprehensive
- **Helpfulness** (`helpfulness.prompt`): Answer is useful and actionable
- **Tone** (`tone.prompt`): Answer has appropriate tone
- **Citation Quality** (`citation_quality.prompt`): Citations are accurate and complete

### Creating Evaluators

Using helper functions:

```go
faithfulness, err := genkit.NewFaithfulnessEvaluator(g)
```

Using custom prompts:

1. Create a `.prompt` file in your prompts directory:

```yaml
---
model: googleai/gemini-2.5-flash
config:
  temperature: 0.0
input:
  schema:
    Input: string
    Output: any
output:
  schema:
    pass: boolean
    score: number
    reason: string
---
You are an evaluator. Evaluate if the answer is good.

Question: {{Input}}
Answer: {{Output}}

Respond in JSON:
{
  "pass": true/false,
  "score": 0.0-1.0,
  "reason": "explanation"
}
```

2. Load it with the custom evaluator function:

```go
custom, err := genkit.NewCustomEvaluator(g, "my_evaluator", "my_custom_prompt")
```

Using the low-level API:

```go
evaluator, err := genkit.NewLLMJudge(genkit.LLMJudgeConfig{
    Name:       "custom",
    PromptName: "my_custom_prompt",  // References my_custom_prompt.prompt
    Genkit:     g,
})
```

## Prompt Files Format

Prompts use Genkit's dotprompt format with Handlebars templates:

### Front Matter

The YAML front matter defines the model, configuration, and input/output schemas:

```yaml
---
model: googleai/gemini-2.5-flash
config:
  temperature: 0.0
input:
  schema:
    Input: string
    Output: any
    Context(array): string
    Reference?: any
    Metadata?: object
output:
  schema:
    pass: boolean
    score: number
    reason: string
    confidence?: number
---
```

### Template Body

The template uses Handlebars syntax with access to input data:

```handlebars
You are an evaluator. Determine if the answer is faithful to the context.

Context:
{{#each Context}}
- {{this}}
{{/each}}

Question: {{Input}}
Answer: {{#if Output.answer}}{{Output.answer}}{{else}}{{Output}}{{/if}}

Respond in JSON format:
{
  "pass": true/false,
  "score": 0.0-1.0,
  "reason": "explanation",
  "confidence": 0.0-1.0
}
```

### Available Template Variables

Templates have access to the following data:

- `Input`: Original input (e.g., question)
- `Output`: System output to evaluate (e.g., answer)
- `Reference`: Expected output (optional, for correctness checks)
- `Context`: Additional context (e.g., documents for RAG)
- `Metadata`: Custom metadata

## Supported Models

### Ollama (Local)

```go
import "github.com/firebase/genkit/go/plugins/ollama"

g, _ := genkit.Init(ctx,
    genkit.WithPlugins(&ollama.Ollama{}),
    genkit.WithPromptDir("./genkit/prompts"),
)

// Model specified in .prompt file (e.g., ollama/mistral)
evaluator, _ := genkit.NewFaithfulnessEvaluator(g)
```

### OpenAI

```go
import "github.com/firebase/genkit/go/plugins/openai"

g, _ := genkit.Init(ctx,
    genkit.WithPlugins(&openai.OpenAI{APIKey: "..."}),
    genkit.WithPromptDir("./genkit/prompts"),
)

// Model specified in .prompt file (e.g., openai/gpt-4)
evaluator, _ := genkit.NewFaithfulnessEvaluator(g)
```

### Google Gemini

```go
import "github.com/firebase/genkit/go/plugins/googlegenai"

g, _ := genkit.Init(ctx,
    genkit.WithPlugins(&googlegenai.GoogleAI{APIKey: "..."}),
    genkit.WithPromptDir("./genkit/prompts"),
)

// Model specified in .prompt file (e.g., googleai/gemini-2.5-flash)
evaluator, _ := genkit.NewFaithfulnessEvaluator(g)
```

### Anthropic Claude

```go
import "github.com/firebase/genkit/go/plugins/anthropic"

g, _ := genkit.Init(ctx,
    genkit.WithPlugins(&anthropic.Anthropic{APIKey: "..."}),
    genkit.WithPromptDir("./genkit/prompts"),
)

// Model specified in .prompt file (e.g., anthropic/claude-3-sonnet)
evaluator, _ := genkit.NewFaithfulnessEvaluator(g)
```

## Customizing Models

You can override the model for a specific use case by:

1. Creating a prompt variant (e.g., `faithfulness.gpt4.prompt`)
2. Or editing the model in the `.prompt` file's front matter

Example variant:

```yaml
---
model: openai/gpt-4
config:
  temperature: 0.0
---
(same template content)
```

Load the variant:

```go
evaluator, _ := genkit.NewCustomEvaluator(g, "faithfulness_gpt4", "faithfulness.gpt4")
```

## Response Format

LLM judges return structured output matching the output schema:

```json
{
  "pass": true,
  "score": 0.95,
  "reason": "The answer is faithful to the context and addresses the question.",
  "confidence": 0.9
}
```

- **pass**: Boolean indicating if evaluation passed
- **score**: Numeric score (0-1)
- **reason**: Human-readable explanation
- **confidence**: Optional confidence level (0-1)

## Error Handling

If the LLM fails to return valid JSON, the evaluator falls back to text parsing:

```go
// Fallback parsing looks for positive indicators
positiveWords := []string{"pass", "correct", "accurate", "good", "yes"}
```

For production use, consider:
- Retry logic for LLM failures
- Fallback models
- Logging raw LLM responses
- Monitoring evaluation quality

## Best Practices

1. **Define prompts in `.prompt` files** for better maintainability and version control
2. **Use temperature 0.0** for consistent evaluation (set in front matter)
3. **Test prompts** using Genkit Dev UI before deploying
4. **Monitor LLM costs** - evaluation can be expensive at scale
5. **Use local models** (Ollama) for CI/CD to avoid costs
6. **Validate structured output** in your `.prompt` file schemas
7. **Cache results** for identical inputs to save costs
8. **Version your prompts** using variants for A/B testing

## Example: Evaluating RAG Quality

```go
package main

import (
    "context"
    "github.com/antflydb/antfly/pkg/evalaf/eval"
    "github.com/antflydb/antfly/pkg/evalaf/genkit"
    genkitpkg "github.com/firebase/genkit/go/genkit"
    "github.com/firebase/genkit/go/plugins/ollama"
)

func main() {
    ctx := context.Background()

    // Initialize Genkit with prompt directory
    g, _ := genkitpkg.Init(ctx,
        genkitpkg.WithPlugins(&ollama.Ollama{}),
        genkitpkg.WithPromptDir("./genkit/prompts"),
    )

    // Create RAG evaluators (models specified in .prompt files)
    faithfulness, _ := genkit.NewFaithfulnessEvaluator(g)
    relevance, _ := genkit.NewRelevanceEvaluator(g)
    completeness, _ := genkit.NewCompletenessEvaluator(g)

    // Create dataset
    dataset := eval.NewJSONDatasetFromExamples("rag_test", []eval.Example{
        {
            Input: "What is the capital of France?",
            Context: []any{
                "France is a country in Europe.",
                "Paris is the capital and largest city of France.",
            },
            // Output will be added by target function
        },
    })

    // Target function calls your RAG system
    target := func(ctx context.Context, example eval.Example) (any, error) {
        // Call your RAG endpoint here
        return callMyRAGSystem(example.Input)
    }

    // Run evaluation
    runner := eval.NewRunner(eval.DefaultConfig(), []eval.Evaluator{
        faithfulness,
        relevance,
        completeness,
    })

    report, _ := runner.RunWithTarget(ctx, dataset, target)
    report.Print()
}
```

## Migration from text/template

If you're upgrading from the old text/template approach:

1. **Initialize with prompt directory**:
   ```go
   g, _ := genkit.Init(ctx,
       genkit.WithPromptDir("./genkit/prompts"),
   )
   ```

2. **Remove modelName parameter** from evaluator constructors:
   ```go
   // Old
   faithfulness, _ := genkit.NewFaithfulnessEvaluator(g, "ollama/mistral")

   // New
   faithfulness, _ := genkit.NewFaithfulnessEvaluator(g)
   ```

3. **Update custom evaluators** to use prompt names instead of template strings:
   ```go
   // Old
   custom, _ := genkit.NewCustomEvaluator(g, "my_eval", "ollama/mistral", templateString)

   // New (create my_eval.prompt file first)
   custom, _ := genkit.NewCustomEvaluator(g, "my_eval", "my_eval")
   ```

4. **Convert template syntax** from Go templates to Handlebars:
   - `{{.Input}}` → `{{Input}}`
   - `{{range .Context}}` → `{{#each Context}}`
   - `{{if .Output.answer}}` → `{{#if Output.answer}}`

## Dependencies

- `github.com/firebase/genkit/go/genkit` - Genkit core
- `github.com/firebase/genkit/go/ai` - AI abstractions
- `github.com/antflydb/antfly/pkg/evalaf/eval` - Core evaluation library

## See Also

- [eval package](../eval/README.md) - Core evaluation library
- [Firebase Genkit Docs](https://firebase.google.com/docs/genkit) - Official Genkit documentation
- [Genkit Dotprompt Guide](https://firebase.google.com/docs/genkit/dotprompt) - Dotprompt documentation
- [Handlebars Templates](https://handlebarsjs.com/) - Handlebars syntax reference
- [Prompt Engineering Guide](https://www.promptingguide.ai/) - Best practices for prompts
