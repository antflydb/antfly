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

## Future batch wrapper

OCR/read, transcription, extraction, and generative LLM calls should use the same envelope-vs-item distinction, but they should not overload every native response shape independently. A future generic synchronous batch wrapper can route multiple independent endpoint requests:

```json
{
  "endpoint": "/read",
  "requests": [
    {"custom_id": "doc-a", "body": {"model": "reader", "input": "..."}},
    {"custom_id": "doc-b", "body": {"model": "reader", "input": "..."}}
  ]
}
```

Each item would return `custom_id`, `status`, and either `response` or `error`. That mirrors OpenAI/Gemini-style batch semantics and can later be backed by an async JSONL job API without changing the per-item result model.
