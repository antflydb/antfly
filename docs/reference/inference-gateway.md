# Named inference gateway

Antfly serves `/ai/v1` on database API nodes, including remote-only deployments
with `inference.api_url` and no local model files. SQL, search, enrichment and the
gateway use the existing named provider configurations, secret sources and
provider quota identities. Local execution crosses the inference boundary in
memory; it does not call the public gateway over HTTP.

Select a model explicitly. Canonical IDs include `generators/support`,
`chains/interactive`, `deciders/triage`, `embedders/dense`, `readers/ocr`,
`transcribers/speech`, `synthesizers/voice`, `predictors/risk`,
`connections/gpu-east/models/owner/model` and `local/owner/model`. Bare aliases
resolve within the endpoint's task namespace. A collision with a raw local model
returns an ambiguity error. Existing raw model IDs remain valid.

```yaml
inference:
  api_url: http://inference.internal:8080
  api_key: '${secret:inference.api_key}'
generators:
  support:
    provider: openai
    model: gpt-4.1-mini
    api_key: '${secret:openai.api_key}'
  reserve:
    provider: antfly
    model: owner/chat
inference_capabilities:
  generators/support:
    streaming: supported
    tools: supported
    structured_output: supported
  generators/reserve:
    streaming: supported
    tools: supported
chains:
  interactive:
    - generator: support
      condition: on_rate_limit
    - generator: reserve
readers:
  ocr:
    provider: antfly
    model: owner/ocr
transcribers:
  speech:
    provider: antfly
    model: openai/whisper-base
text_to_speech:
  voice:
    provider: openai
    model: tts-1
    voice: alloy
    api_key: '${secret:openai.api_key}'
predictors:
  risk:
    model: risk-v2
    connection: default-inference
```

With authentication enabled, discovery requires `inference:read`; task execution
and all session operations require `inference:write`. Table permissions alone do
not authorize either surface. Named and physical connection invocation use the
same inference authority.

Capability declarations describe the selected backend, and must be accurate for
that model. Adapter support is also required. An unknown feature never qualifies
a target for tools, streaming, media or structured output. Configured aliases
stay in discovery when their upstream is offline; availability is a separate
observation, not evidence of feature support.

## Public contracts

| Surface | Contract |
| --- | --- |
| `/ai/v1/chat/completions` | OpenAI chat subset; text/image content, tool history and calls, declared structured output, incremental SSE and actual upstream usage |
| `/ai/v1/decisions` | OpenAI Decisions common subset and Antfly typed/embedding-similarity extensions |
| `/ai/v1/embeddings`, `/embed` | Dense floats or little-endian float32 base64; sparse `{indices, values}` and ordered multimodal parts use the Antfly extensions |
| `/ai/v1/generate`, `/generate/batch` | Existing generation envelopes; native options require an Antfly executor; batches share a deadline and preserve item IDs |
| `/ai/v1/rerank`, `/chunk`, `/read`, `/extract`, `/rewrite` | Existing task envelopes, source indices, coordinates and schemas |
| `/ai/v1/transcribe`, `/dictate`, `/transcription/sessions/...` | Existing audio and session contracts; native dictation cleanup options require an Antfly runtime |
| `/ai/v1/synthesize` | `{model, text, voice?, format?, speed?}`; returns audio bytes |
| `/ml/v1/predict`, `/models`, `/predictors` | Existing ordered feature-vector requests and predictor schemas |

The supported OpenAI compatibility matrix is:

| Adapter | Supported subset |
| --- | --- |
| OpenAI, OpenRouter, Ollama and Antfly generators | Chat messages, image parts, tool history/calls, structured output and genuine SSE, intersected with the model's declared capabilities |
| Other existing generator adapters | Completed text generation through their existing typed executor; rich chat features fail explicitly |
| Dense embedding adapters | Text input, float vectors and float32 base64; dimensions must match the selected target |
| Decision adapters | Typed predicates/choices and declared similarity extensions; refusal, abstention and empty selections remain distinct |

Unsupported chat fields fail before provider work. Responses API, audio chat
content and arbitrary provider-specific options are outside this subset.

Sparse named embedders declare `sparse: true` in the existing embedder config.
Sparse output cannot request dense dimensions or base64 encoding. Multimodal
requests retain ordered parts and groups; unsupported modalities fail explicitly.
Embedding-space/artifact pins and decision method/calibration metadata retain
meaning. There is no cross-model embedding or decision fallback.

Named transcription defaults apply to language, timestamp-aware decoding and
diarization. Request values take precedence, including explicit `false`.
Native timestamp decoding retains its historical defaults when omitted: off for
`transcribe`, on for `dictate`. The TypeScript transcription helper forwards the
same options as the public request contract.

Synthesis uses existing `text_to_speech` configs. OpenAI supports its configured
speech formats; Vertex supports MP3, WAV and Ogg Opus; ElevenLabs currently
supports MP3 at normal speed. Unsupported formats and S3-output options fail
explicitly. Provider usage is omitted when unavailable; character counts or text
tokens are not invented as billing estimates. Provider API references:
[Google](https://docs.cloud.google.com/text-to-speech/docs/reference/rest/v1/text/synthesize)
and [ElevenLabs](https://elevenlabs.io/docs/api-reference/text-to-speech/convert).

An official OpenAI client can use the gateway directly:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/ai/v1",
    api_key="your-antfly-api-key",
    max_retries=0,
)
for event in client.chat.completions.create(
    model="generators/support",
    messages=[{"role": "user", "content": "Hello"}],
    stream=True,
):
    print(event.choices)
```

The TypeScript `AntflyClient.inference` API shares database authentication.
`models()` lists canonical targets; `invoke(operation, body, signal)` returns the
live response, including SSE or audio, and issues each mutation once. The
existing typed `InferenceClient` and generated API client remain available.
Antfarm's **Models → Gateway** playground lists the same IDs and supports
cancellation, streamed output and synthesized audio.

## Fallback, limits and telemetry

A chain validates every candidate against the full request before executing.
The failed link's `condition` explicitly controls fallback. `on_error` and
`always` permit only explicit transient upstream rate-limit/unavailable responses;
`on_rate_limit` permits rate limiting; `on_timeout` explicitly permits timeouts.
Invalid input, credentials, refusals, malformed output and cancellation do not
switch models. Headers or any emitted SSE data permanently commit the target.
Retries and fallback share at most eight attempts and one total deadline.

Batches inherit the caller's attempt cap for every item and one total deadline.
Each item executes with a buffered response, including local and qualified
connection targets; only the completed batch envelope is sent to the client.

The proxy owns physical placement. Gateway fallback owns logical model changes.
`X-Antfly-Inference-Attempts-Remaining` and
`X-Antfly-Inference-Timeout-Ms` can reduce the server's limits. Gateway-to-runtime
calls carry a single downstream attempt; nested proxies cannot multiply retries.
Independent client retries are separate invocations; configure the SDK policy
explicitly. Disconnects cancel upstream work. Public request admission is
acquired once for local calls; gateway outbound limits and remote runtime
capacity remain independent.

`/ai/v1/models` preserves native grouped fields and the OpenAI `data` list.
Descriptors include task, executor, backend identity, limits, execution policy,
`gateway_capabilities`, availability and freshness. The legacy capability-name
array retains its existing shape. `/db/v1/connections?include=models` uses the
same registry view and bounded connection-discovery cache. `named-inference` is
a reserved virtual connection for configured aliases. Qualified vendor
connections use their task adapters and connection-scoped credentials; a
capability denial cannot be bypassed through a model alias. Public descriptors
exclude credentials and upstream URLs.

`X-Request-Id` identifies the gateway invocation;
`X-Antfly-Upstream-Request-Id` preserves upstream correlation when supplied.
Per-attempt telemetry records the requested alias, resolved target/backend,
connection, latency, time to first streamed output, status/errors and actual
provider token usage when present. Prompts, media, tool arguments, secrets and
billing estimates are excluded. Refusals have a distinct `DecisionRefused` code;
abstention and empty selections retain their decision semantics.

## Session affinity

A transcription session is bound to its authenticated credential, one gateway
owner, one connection and one proxy endpoint incarnation. Public handles replace
backend session IDs in ordinary responses and incremental session events.
Sessions never fail over, including when an endpoint is replaced at the same
address. Unknown or expired handles fail closed.

With multiple gateway or proxy replicas, configure sticky routing to the owner
that created the session, using the returned
`X-Antfly-Inference-Affinity` handle or a load-balancer affinity cookie. Each
layer must preserve its own owner. Handles are process-local; there is no implied
shared session store or recovery after process restart. The bounded owner maps
hold at most 4096 sessions and discard expired entries. Use a single replica
when owner-affinity routing is unavailable.

The proxy serves canonical `/ai/v1/decisions`; `/ai/v1/decide` remains a legacy
alias using the same scoped `decide` policy operation. Operator operations also
include read, rewrite, transcribe, dictate, transcription sessions, synthesis and
prediction. Capability leases remain bound to authorization, operation, route
generation and endpoint incarnation.

## Conformance

`scripts/ci/zig-inference-gateway-conformance.py` starts a remote-only Antfly node
and controlled HTTP fixtures. It exercises an official OpenAI SDK, HTTP/SQL
named decisions, fallback restrictions and budgets, task adapters, predictor
schemas, sparse vectors, synthesis and the complete session lifecycle. It does
not call paid providers. Gateway/shared-executor tests and proxy tests cover
post-commit failure, quotas, admission ownership, credential isolation and stale
endpoint/session handling.
