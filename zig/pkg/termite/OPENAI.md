# OpenAI Compatibility in termite-zig

termite-zig can be used as a local OpenAI-compatible inference server for chat-style generation. The goal is not to copy the entire OpenAI platform, but to make common OpenAI client integrations work with termite-zig using a familiar API shape.

## What Works

### Chat completions

Use the OpenAI-style endpoint:

```text
/api/chat/completions
```

termite-zig also keeps its native generator endpoint:

```text
/api/generate
```

Both endpoints route to the same generation logic. If you are configuring an OpenAI SDK, point `base_url` at:

```text
http://host:port/api
```

and use normal chat completions calls.

### Response format

Non-streaming chat responses follow the OpenAI chat completion shape, including:

- `id`
- `object`
- `created`
- `model`
- `choices`
- `usage.prompt_tokens`
- `usage.completion_tokens`
- `usage.total_tokens`

### Streaming

Streaming responses use Server-Sent Events and emit OpenAI-style chat completion chunks:

- `object: "chat.completion.chunk"`
- stable `id` across the stream
- stable `created` timestamp across the stream
- an initial `delta.role = "assistant"` event
- incremental `delta.content` updates
- final `finish_reason`
- terminating `data: [DONE]`

Streaming works for both ONNX-backed generation and native generation paths.

### Models listing

`/api/models` includes an OpenAI-friendly top-level model list:

```json
{
  "object": "list",
  "data": [
    {
      "id": "model-name",
      "object": "model",
      "created": 1234567890,
      "owned_by": "termite"
    }
  ]
}
```

termite-zig still keeps its richer nested task-based model metadata in the same response so existing termite clients continue to work.

### Health endpoints

Operational endpoints are available outside `/api`:

- `/healthz`
- `/readyz`

`/healthz` reports basic liveness.  
`/readyz` reports whether termite-zig can discover usable models and includes per-task counts.

## Compatibility Notes

### Token accounting

termite-zig reports:

- prompt tokens when the server can determine them directly
- completion tokens from the generated output
- total tokens as the sum of the two

For some ONNX generation paths, prompt token accounting may be approximate or unavailable at the backend layer. In those cases termite-zig still returns a valid OpenAI-compatible response shape.

### Tool calling

Tool-call parsing and streaming are supported through termite-zig's existing tool parser integration. When a model emits tool calls, termite-zig maps them into OpenAI-style `tool_calls` structures in both full responses and streaming deltas.

### Scope

This compatibility layer is aimed at practical SDK interoperability:

- Python OpenAI SDK chat completions
- streamed chat completions
- model listing
- health checks for local and Kubernetes-style deployments

It should be treated as API compatibility for common client flows, not as a promise that every OpenAI endpoint or field is implemented.

## Example

Python OpenAI SDK:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/api",
    api_key="unused",
)

resp = client.chat.completions.create(
    model="your-local-model",
    messages=[
        {"role": "user", "content": "Say hello briefly."},
    ],
)

print(resp.choices[0].message.content)
```

## Verification

Build:

```text
zig build
```

Run e2e tests:

```text
cd e2e
uv sync
TERMITE_BIN=../zig-out/bin/termite .venv/bin/pytest -q
```

Useful manual checks:

```text
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/api/models
```

## Files Involved

- `src/pipelines/generation.zig`
- `src/pipelines/onnx_decoder_only_vlm.zig`
- `src/server/server.zig`
- `../../../specs/openapi/termite/api.yaml`
- `../../e2e/termite/test_generate.py`
- `../../e2e/termite/test_models.py`
- `../../e2e/termite/test_chat.py`
- `../../e2e/termite/test_health.py`
- `../../e2e/termite/test_openai_compat.py`
