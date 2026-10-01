# OpenAI generation options

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

This generator configuration works anywhere Antfly accepts a generator, including
retrieval agents and generator chains. The completion budget includes both visible
output and reasoning tokens. Provider token quotas reserve this budget, rather
than the legacy default output limit.

The two token-limit fields are mutually exclusive. Existing `max_tokens`
configurations retain their previous wire format and default of 256 when neither
limit is supplied. Antfly does not infer parameter support from model names or
silently rewrite a legacy token limit.

`reasoning_effort` is optional. Omit it to use the model's default; supported values
depend on the model. Antfly accepts `none`, `minimal`, `low`, `medium`, `high`,
`xhigh`, and `max`, and forwards the explicit value to OpenAI. Sampling options
such as `temperature` are also opt-in and forwarded unchanged. Omit unsupported
sampling options for the selected model and reasoning mode.

These options belong to the `openai` provider. Other providers, including Ollama
and OpenRouter, retain their existing parameter handling. OpenAI generation uses
the request type generated from the bundled upstream OpenAPI spec; non-OpenAI
compatible providers continue using their shared compatibility encoder.

See the [OpenAI Chat Completions reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
for parameter semantics and model-specific support.
