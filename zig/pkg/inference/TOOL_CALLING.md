# Tool Calling Design

Antfly inference exposes and consumes tool calls using the OpenAI-compatible
Chat Completions shape. Provider and model adapters may have provider-specific
prompting or parsing internally, but the boundary between inference and
downstream Antfly runtimes is normalized `tool_calls`.

## Wire Contract

Requests use `tools` and `tool_choice` at the top level:

```json
{
  "model": "ggml-org/gemma-4-E4B-it-GGUF",
  "messages": [
    { "role": "user", "content": "Extract relations from this text." }
  ],
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "emit_relations",
        "description": "Extract supported relations from document text.",
        "parameters": {
          "type": "object",
          "properties": {
            "relations": { "type": "array" }
          },
          "required": ["relations"]
        }
      }
    }
  ],
  "tool_choice": {
    "type": "function",
    "function": { "name": "emit_relations" }
  }
}
```

Responses are normalized to OpenAI Chat Completions tool calls:

```json
{
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": null,
        "tool_calls": [
          {
            "id": "call_...",
            "type": "function",
            "function": {
              "name": "emit_relations",
              "arguments": "{\"relations\":[]}"
            }
          }
        ]
      },
      "finish_reason": "tool_calls"
    }
  ]
}
```

`function.arguments` is a JSON string, not a JSON object. This matches OpenAI
Chat Completions and keeps Ollama, vLLM, native Gemma, and hosted providers on
one internal representation.

## Normalization Boundary

Tool-call normalization belongs in inference provider/model adapters:

- OpenAI-compatible providers pass through `message.tool_calls`.
- Ollama/vLLM-compatible providers are normalized into the same shape before
  returning `GenerateResult`.
- Native Gemma/tool-call formats may use model-specific prompting/parsing, but
  must emit normalized `ToolCall { name, arguments }` values to Antfly.

Downstream consumers, including artifact producers and graph/autograph indexes,
must not implement provider-specific tool-call parsing. If an asset producer is
configured with `tool_output: "arguments"`, it requires a normalized tool call
and should fail when one is absent.

## Forced-Tool JSON Fallback

Some OpenAI-compatible or native model paths produce the forced function
arguments as plain JSON content instead of a `tool_calls` array. The inference
adapter may synthesize a normalized tool call only when all of these conditions
hold:

- `tool_choice` forces exactly one function.
- The forced function exists in the supplied `tools` array.
- The model response has no `tool_calls`.
- The response content parses as JSON. Fenced JSON is accepted only after
  removing the surrounding code fence.

The synthesized internal result is equivalent to:

```json
{
  "tool_calls": [
    {
      "id": "call_forced_0",
      "type": "function",
      "function": {
        "name": "emit_relations",
        "arguments": "{\"relations\":[]}"
      }
    }
  ]
}
```

This fallback is intentionally narrow. It is provider/model normalization, not
asset-producer behavior, and it must not cause arbitrary JSON content to be
treated as a tool call unless a single function was explicitly forced.

## Artifact Producer Rule

Artifact producers should stay provider-neutral:

```json
{
  "type": "generator",
  "config": {
    "provider": "antfly",
    "model": "ggml-org/gemma-4-E4B-it-GGUF",
    "api_url": "http://127.0.0.1:8080/ai/v1",
    "tools": [ ... ],
    "tool_choice": {
      "type": "function",
      "function": { "name": "emit_relations" }
    },
    "tool_name": "emit_relations",
    "tool_output": "arguments"
  }
}
```

The producer asks for tool arguments and consumes normalized tool calls. It does
not know whether the underlying model used OpenAI native tool calls, Ollama,
vLLM, native Gemma, or a forced-tool JSON fallback.
