# Inference batching and per-item failures

Antfly inference has two different batching concerns:

1. Run several inputs through one model call for throughput.
2. Report failures precisely enough that ingestion systems can retry transient work and quarantine permanently bad items.

These are related but not the same. A throughput batch should not force all-or-nothing ingestion semantics when one item is permanently invalid.

## Provider context

OpenAI and Gemini both separate the request envelope from individual work items in their async batch APIs. A malformed batch file or invalid model can fail the whole request, but each JSONL line has its own identity and can produce either a result or an error. OpenAI embeddings also accept multiple inputs in a synchronous request, but that OpenAI-compatible endpoint is still naturally all-or-error unless the caller uses a higher-level batch contract. Gemini follows the same basic model for async `generateContent` and embedding batches: each request has its own key and result status.

Ollama is thinner. It exposes synchronous generation/chat/embed endpoints, and `/api/embed` accepts an array of strings. It does not provide an offline batch API with per-line result and error files, so callers usually own retry classification and per-item fallback.

The useful design point for Antfly is provider-agnostic:

- Envelope failures are HTTP failures.
- Item failures are indexed item results.
- Permanent item failures are not retried forever.
- Valid sibling inputs keep moving.

## Local failure mode

The media embedding path historically batched image preprocessing and inference together. If one image could not be decoded, the whole `/embeddings` request failed, and a DB batch write could reject sibling documents that did not share the bad media. This is especially painful for remote-media ingestion because an unsupported or corrupt image is a permanent input problem, while HTTP 500 looks transient to durable queues.

The current image decoder supports PNG, JPEG, GIF, BMP, and WebP through the shared image layer, but decode and preprocessing are still item-specific stages. They should be classified per input when the caller asks for per-item semantics.

## Embeddings contract

`POST /ai/v1/embeddings` and `/ai/v1/embed` keep OpenAI-compatible fail-fast behavior by default:

```json
{
  "model": "antflydb/clipclap",
  "input": ["hello", "world"]
}
```

For ingestion and enrichment callers, dense embedding requests can opt into per-item results:

```json
{
  "model": "antflydb/clipclap",
  "error_policy": "per_item",
  "input": [
    {"type": "image_url", "image_url": {"url": "https://example.com/good.jpg"}},
    {"type": "media", "mime_type": "image/webp", "data": "not-valid-base64-or-image"},
    "text that can still be embedded"
  ]
}
```

With `error_policy: "per_item"`, the endpoint returns HTTP 200 for a valid envelope and model request. Successful embeddings appear in `data` using original input indexes. Failed inputs appear in `errors`:

```json
{
  "object": "list",
  "model": "antflydb/clipclap",
  "data": [
    {"object": "embedding", "index": 0, "embedding": [0.1, 0.2]},
    {"object": "embedding", "index": 2, "embedding": [0.3, 0.4]}
  ],
  "errors": [
    {
      "index": 1,
      "code": "INVALID_MEDIA",
      "message": "invalid base64 media data",
      "stage": "parse",
      "retryable": false,
      "status": 400
    }
  ],
  "summary": {"total": 3, "succeeded": 2, "failed": 1},
  "usage": {"prompt_tokens": 12, "total_tokens": 12}
}
```

HTTP still fails for envelope-level problems: malformed JSON, unknown model, invalid dimensions, model load failure, queue/service failure, or unsupported request options. Sparse embedding requests remain fail-fast for now because the immediate media blast-radius problem is in dense multimodal embedding.

## Implementation notes

The dense embedding server path first attempts the efficient modality batch:

- all text inputs through `EmbeddingPipeline.embed`
- all image inputs through `EmbeddingPipeline.embedImages`
- all audio inputs through `EmbeddingPipeline.embedEncodedAudio`

When `error_policy` is `fail_fast`, any modality failure keeps the old behavior.

When `error_policy` is `per_item`, malformed content parts, failed media fetches, and invalid media payloads are classified into indexed `errors` before inference. Parsed inputs still use the efficient modality batch first; a failed modality batch is retried one input at a time. Successful single-item results are retained. `ImageDecodeFailed` is a permanent `INVALID_IMAGE` item error with `retryable: false` and `status: 400`; unclassified runtime failures are reported as retryable `INFERENCE_FAILED` item errors.

This keeps the fast path fast while giving ingestion callers a way to isolate poisoned media without forcing DB batch rollback.

## Synchronous generation batch endpoint

Generative LLM batching should start with a synchronous `/generate/batch`
endpoint for artifact workers and enrichment pipelines. This endpoint batches
independent generation requests for throughput, but it is still an online call:
the client keeps the connection open and receives per-item results for that
submitted batch.

The single-request `/generate` endpoint should stay a normal one-request API.
Batch transport belongs on `/generate/batch` so clients can rely on a stable
response shape and so streaming, retries, and per-item failures are explicit.

For JSON requests, the body is an envelope with an optional `mode`. The initial
implementation only supports `sync`; `async` is reserved for future durable
batch jobs:

```json
{
  "mode": "sync",
  "requests": [
    {
      "custom_id": "doc-a",
      "body": {"model": "qwen", "messages": [{"role": "user", "content": "Summarize A"}]}
    },
    {
      "custom_id": "doc-b",
      "body": {"model": "qwen", "messages": [{"role": "user", "content": "Summarize B"}]}
    }
  ]
}
```

The JSON response returns one item per request. Results are matched by
`custom_id` and `index`, not response order:

```json
{
  "object": "generate.batch",
  "data": [
    {"custom_id": "doc-a", "index": 0, "response": {"object": "chat.completion", "choices": [{"message": {"content": "..."}}]}, "error": null},
    {"custom_id": "doc-b", "index": 1, "response": null, "error": {"code": "INFERENCE_FAILED", "message": "...", "retryable": true}}
  ],
  "summary": {"total": 2, "succeeded": 1, "failed": 1}
}
```

The initial implementation exposes the JSON envelope only. A future
`application/x-ndjson` transport should imply synchronous streaming batch mode.
The request body would be one JSON object per line, each equivalent to one
entry in the JSON `requests` array:

```jsonl
{"custom_id":"doc-a","body":{"model":"qwen","messages":[{"role":"user","content":"Summarize A"}]}}
{"custom_id":"doc-b","body":{"model":"qwen","messages":[{"role":"user","content":"Summarize B"}]}}
```

The response content type would also be `application/x-ndjson`, with each
output line as a completed item:

```jsonl
{"custom_id":"doc-a","index":0,"response":{"text":"..."},"error":null}
{"custom_id":"doc-b","index":1,"response":null,"error":{"code":"INFERENCE_FAILED","message":"...","retryable":true}}
```

Envelope failures remain HTTP failures: malformed JSON/NDJSON, unknown model,
unsupported `mode`, oversized envelope, or invalid request options that prevent
the batch from starting. Per-item failures are encoded in item results.

Future durable async batching should reuse the same per-item result model but
use a separate job lifecycle: `mode: "async"` on a JSON `/generate/batch`
request, an `input_file_id` or object-store reference, a returned batch/job ID,
status polling, and output/error files. NDJSON uploads should not imply async;
large streamed uploads belong in a file API or object-store upload path before
creating the durable job.

## Reader/OCR batching

The `/read` endpoint accepts multiple images and should route them through a reader-level batch API instead of invoking the model once per image. The reader abstraction has two layers:

- `LoadedReader.readBatch`: the stable reader contract used by the server and local direct calls.
- Model-family implementations: native Florence can use a real batch fast path; VLM, GenAI, Pix2Struct, and multistage OCR may keep the serial fallback until their runtimes expose safe batch execution.

Native Florence batching is throughput-oriented. A request batch is chunked by `ANTFLY_INFERENCE_READ_BATCH_SIZE` (default 8, clamped to 1..64), preprocessed into `[batch, 3, H, W]`, then run through the Florence encoder once for the chunk. CUDA and Metal use the incremental decoder KV cache in batch mode when available: self-attention keys/values are appended per row, cross-attention keys/values are precomputed for the whole encoder batch, and the LM head is applied over `[batch, hidden]` at each generated position. Metal exposes the generic eager backend hooks needed by the preallocated KV slab path (`allocUninitF32Shape`, `copyRows2D`, and device-backed row concat/slice) and has a batched device `attention_f32` entry that dispatches all `batch * q_len * heads` rows in one Metal command. The reader pipeline does not need a Florence-specific Metal API. Rows that emit EOS stop contributing new text tokens while the rest of the batch continues. If the batched KV path is unsupported by a backend shape or operator, native Florence falls back to the full batched decoder path; non-native and unsupported reader families keep the serial fallback behind the same `LoadedReader.readBatch` contract.

The current `/read` HTTP response remains all-or-error at the envelope level, matching the existing API behavior. The future generic batch wrapper above should add per-item errors for bad image bytes, media fetch failures, and per-row inference failures without changing the `/read` response schema.
