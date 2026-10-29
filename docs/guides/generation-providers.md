# Generation providers

Antfly accepts a generator anywhere it generates text: RAG answers, retrieval
agents, enrichments, and generator chains. A generator is the built-in Antfly
runtime or a connected provider such as OpenAI, OpenRouter, Ollama, Gemini,
Vertex, or Bedrock.

## Tool calling

Server-side agents support function tools through the Antfly, OpenAI, OpenRouter,
Ollama, Gemini, and Vertex generator adapters. Choose a model that supports
function calling; adapter support does not guarantee that every model does.

OpenRouter and Ollama use the OpenAI-compatible tool protocol. Gemini and Vertex
translate function definitions, tool choices, calls, and results to Google's
`generateContent` protocol. Google response parts, including thought signatures,
are retained for later turns and count toward the agent's context limit.

Antfly's embedded runtime must expose its JSON generation callback to use tools.
Legacy text-only callbacks cannot preserve tool calls and results. The mock
provider is not a tool-capable agent backend.

## OpenAI options

For OpenAI models that require a completion budget, configure the generator with
`max_completion_tokens` instead of `max_tokens`:

```json
{
  "provider": "openai",
  "model": "gpt-5.6-luna",
  "url": "https://api.openai.com/v1",
  "api_key": "${secret:openai-api-key}",
  "max_completion_tokens": 1024,
  "reasoning_effort": "none"
}
```

The completion budget includes both visible output and reasoning tokens, and
provider token quotas reserve it. The two token-limit fields are mutually
exclusive; `max_tokens` defaults to 256 when neither is supplied. Antfly does not
infer parameter support from model names or rewrite one limit into the other.

`reasoning_effort` is optional and forwarded unchanged: `none`, `minimal`, `low`,
`medium`, `high`, `xhigh`, or `max`, depending on the model. Sampling options such
as `temperature` are also opt-in and forwarded unchanged; omit any the selected
model does not support. These options apply to the `openai` provider only.

See the [OpenAI Chat Completions reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
for parameter semantics and model-specific support, and
[provider rate limits](provider-rate-limits.md) for quota configuration.
