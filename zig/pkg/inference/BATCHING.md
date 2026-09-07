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

### Cross-request model scheduling

Antfly groups ready logical work before materializing document assets. The
inference Node separately schedules work across callers against the loaded
model generation. Neither layer merges task semantics or resets deadlines.

| Executor | Cross-request implementation |
| --- | --- |
| Florence reader | Existing encoded/raster broker adapter |
| Dense text, including Qwen3 embedding profiles | Text broker; task/instruction-aware keys; observed adaptive execution |
| Sparse text | Typed sparse-vector broker; fixed-shape/serial graphs bypass native fill delay |
| CLIP/ClipClap images | Encoded-image or borrowed-raster groups |
| CLAP/ClipClap audio | Indexed decoding and bounded PCM/feature windows |
| GLiNER NER, relations, structured extraction, classification | Shared encoder boundary; classification retains independent score decoding; ONNX remains compatibility execution |
| Native generators, including multimodal architectures | Existing token-step coordinator, now reachable from isolated direct and non-streaming HTTP requests |
| Fixed chunker | Immediate bounded transform; no model fill delay |
| Text reranking | Shared tensor-forward dispatcher before the model gate; qualified cross-encoder, late-interaction and yes/no stages |
| BERT-style NER | Bounded eight-row request windows plus cross-request tensor fusion; span decoding stays per text |
| Rewriting / REBEL | Shared encoder/decoder-stage dispatcher; rewriting arrays expose up to eight independent sequences, with masked encoder padding |
| Whisper transcription | Shared dispatcher for qualified encoder and decoder forwards; audio decoding and token results remain per request |

Generation qualification is backend-specific, not inferred from the model
name. Shared Metal/CUDA runtime state, graph execution, speculative decoding,
prompt caching, and streaming retain their existing whole-request ownership.
Native whole-request owners do not wait for a token turn while holding the
model gate. Remote Antfly calls reach these same Node paths; Antfly cannot
enable fused execution inside an external vendor's service.

### Task-neutral tensor-forward fusion

`server/tensor_microbatch.zig` is the common forward boundary for qualified
row-independent stages, not a replacement for typed reader, embedder, extraction
or token-generation executors. Pipelines explicitly opt in. Graph sessions must
declare dynamic leading input/output axes; multi-entry native sessions instead
qualify their concrete invocation through `Session.independentBatchRows`.
A backend veto overrides metadata. Neither a family name nor dynamic axes alone
prove that shared caches or model state are safe to batch.

Compatibility includes the concrete session generation, task, execution gate,
input names/dtypes/non-batch shapes, supervising process boundary, and admission
controller/limits. A window retains at most eight caller submissions, 64 tensor
rows and 64 MiB of input data. Larger existing request batches bypass the queue.
The existing Node broker owns scheduling; no new thread pool or background
worker is allocated. Model gates cover physical admission, packing and execution,
but are not held while waiting for a compatible group.

Packing and aggregate execution are admitted before allocation. Queued caller
permits retain their input/preprocessing bytes and yield idle compute workspace.
The physical forward acquires execution once; singleton fallback and later
reuse of a yielded permit reacquire execution without dropping input residency.
Uncovered original inputs are additionally charged when a packed copy is needed.
Contiguous borrowed columns consume no packing allocation, and a live owner in
the same admission domain provides residency credit for already-covered bytes.
Pointer adjacency or ownership in a different resource domain provides no credit.
Capacity subdivision occurs before any forward: oversized groups are replanned
as smaller groups down to singleton execution. Failed or malformed forwards are
never replayed. Physical subgroups receive distinct execution identities and
native-batch counters. After packing buffers are freed, the shared output owner
reduces its lease to materialized output bytes rather than retaining scratch
through downstream decoding.

Outputs are validated before scatter. Row views share the physical tensor and
its admission lease through a reference-counted owner; only the outer result
container is transferred to each caller's allocator. Cancellation is per caller;
healthy peers keep their rows, and process supervision observes the group rather
than inheriting the first caller's deadline. Request-array rewriting keeps
tokenizer and progress-sink access sequential and gives workers independent
tensor/token storage. Fixed/specialized graphs and whole-request-locked
multimodal reranking retain their established execution paths.

Rewriting first admits a bounded preprocessing window and tokenizes it once.
It partitions that window by token-length class before materializing tensors,
then restores original result order. Subdivision never inherits an unrelated
long request's padding width.
Execution groups are then sized using actual padded token lengths against both
encoder and worst-case full-prefix decoder peaks, including retained encoder
output and preprocessing. Each physical stage counts one workspace rather than
one workspace per sequence; encoder and decoder gates serialize admission and
packing as well as their forwards. Execution width shrinks to singleton when a
larger group does not fit permanent limits, without repeating tokenization.
Preprocessing admission can independently shrink the window under live pressure.
This is conservative planning, not a reservation of future execution: concurrent
request admission can still return temporary exhaustion.

`Session.planRun` combines concrete input geometry with an optional native stage
descriptor for output bytes and workspace. Native Whisper describes encoder
hidden states, decoder vocabulary logits and attention/FFN peaks separately;
placeholder output metadata is not used for these stages. Both direct and fused
host-tensor forwards use the descriptor, and reusable preprocessing permits
recheck geometry before each invocation. Imported seq2seq stages use a
backend-independent named-input projection descriptor: Whisper's output time is
ceil(input_features.time / 2), not its mel-channel axis. Exported hidden widths
or explicit model configuration bound feature dimensions. Unresolved non-text
output transforms bypass fusion until a concrete descriptor is available.

Rewriting, REBEL and split Whisper acquire lazy, single-flight composite
runtimes from ModelManager. A pinned publication owns both ManagedSessions,
the managed tokenizer, decoder metadata and optional Whisper prompt metadata.
Keys include artifact generation, runtime kind and backend preference. Generation
covers the validated component dependency closure, including ONNX external data
and native tensor shards, plus tokenizer/configuration/managed receipt sidecars.
Warm serving lookups reuse the component-plan dependency list rather than
reparsing graph contents. Existing
LRU/TTL/pressure eviction applies to idle composites, including device-resident
component leases. A request releases its handle, never the borrowed sessions.
Cold construction uses the manager's load executor and shared waiter control;
one canceled waiter cannot cancel initialization needed by its peers.
Execution gates are owned by the pinned model or composite stage and borrowed
by session copies; unrelated model addresses no longer collide in a hashed
mutex array.

Variable-length masked rewriting and reranking encoders use small sequence
length classes (at most 25% padding) when a dispatcher is installed. Planning
and materialization use the same widths; fixed graphs retain their exact shapes.
Late-interaction query/document execution now honors its admitted trimmed
lengths. Other stage layouts are not blindly padded by the tensor broker.

Shared output row views carry allocation provenance. Subsequent compatible
stages reorder tickets into storage order and borrow contiguous columns without
repacking them; result slots preserve each request's identity. Split groups or
noncontiguous rows still pack normally. Generic seq2seq decoding also reuses its
token-ID buffer, borrows attention masks and selects argmax directly from logits,
eliminating the per-token vocabulary copy.

`pipelines/seq2seq_decode.zig` provides request-owned incremental execution for
qualified merged exports with `use_cache_branch`, matching `past_key_values` /
`present` self/cross-attention tensors, and concrete head dimensions. Generic
seq2seq and transcription pipelines submit only newly appended tokens after
prefill and reuse encoder K/V when subsequent outputs are empty. The adapter
reuses `graph/onnx_kv_cache.zig`, preserves tensor lifetime/admission ownership,
and reserves host KV residency against the existing KV ceiling before forwarding.
Planning includes old-cache/replacement overlap. Cache state stays bound to one
encoder context and request; it is not a second cross-request prefix cache.

Existing native generation prefix caching and Florence incremental caching are
unchanged. Other seq2seq layouts retain their existing path. Separate init/past
graphs, a native Whisper incremental adapter, device-resident graph caches and
fused stepping with broadcast cache-branch inputs still require qualification.
The merged adapter does not change artifact selection order or claim that every
artifact containing an optional cached decoder selects it automatically.

This is shared fused-batching infrastructure, not a claim that every artifact
and provider now executes natively batched. GPU generation's shared graph,
stream, scratch and request-budget state still needs backend-specific isolation;
the existing validated generation scheduler remains authoritative. Hardware
qualification and throughput measurements are still required per artifact.

### Cross-request embedding details

The Node-owned microbatch broker coalesces fail-fast modality work for loaded
models advertising native embedding batching. Mixed requests scatter each
modality's vectors back to their original input indexes. Local
encoded inputs and distributed `/embed` share the adapter; borrowed PDF page
rasters use a distinct representation group. Keys include loaded generation,
task/instruction options, representation, and backend resource class. Each
caller retains its model handle and admitted media until the synchronous join;
model locks are taken only after grouping. Aggregate preprocessing and compute
retain the pipeline's resource admission.

Before tensor materialization, the broker partitions an oversized group using
the resolved session's permanent workspace limits (including a known visual
projection input shape). Planning does not reserve memory or treat transient
process pressure as a reason to retry. Normal execution still acquires live
admission. Media/decoded-byte/item limits remain independently enforced.

Fused vectors use thread-safe temporary allocation and transfer to each caller's
allocator after joining. Cancellation is per caller and reaches decode/resize
workers and synchronous codec kernels. Corrupt compressed images and individual
preprocessing-ceiling failures are recorded at their original indexes. Healthy
normalized rows compact in place for one backend batch and scatter back to
their caller slots, without a second decode just to isolate bad media.
Scratch-exhausted preprocessing waves may still grow their slab or reduce
worker width within the hard ceiling; that bounded adaptation is separate
from retrying model batches for item isolation.
Capacity, cancellation, and runtime failures do not fan out into retries.
Known unsupported backend batch shapes retain explicit compatibility fallback.
The pipeline reports native,
serial, and fallback execution explicitly. A deterministic concurrent regression
verifies that two callers produce one vision-session invocation and independent
owned vectors. This is not a hardware throughput benchmark.

Audio groups also key their working-memory ceiling. Decoder failures stay
indexed, and retained PCM plus feature scratch must fit before adding a clip.
The executor flushes a bounded window when the next clip does not fit, without
retrying a failed backend invocation. Audio asset leases are exclusive and
released before executing other modalities. Text options are validated and
their applied prefix is included in token admission and usage accounting.

Explicit per-item-error requests and traced HTTP requests keep their existing
executors. Native cross-request support is not inferred merely from sharing a
model family or provider. Observed native results and coalesced groups are
separate broker metrics; a compatibility fallback is not called a native batch.

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
batch jobs. The synchronous envelope is capped at 128 items; larger jobs should
be split by the client today and should move to durable async batches when that
mode is implemented:

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

The `/read` endpoint accepts multiple images and routes them through a reader-level batch API instead of invoking the model once per image. The public HTTP request is capped at 64 images and an aggregate downloaded-input byte cap (`ANTFLY_INFERENCE_READ_BATCH_BYTES`, default 256 MiB). With positive admission capacity, the effective cap is also limited to 16 MiB per configured `max_concurrent_requests` unit and the request reserves the larger of one unit per 16 MiB of possible downloaded input and one unit per two declared images. The same policy covers image-backed `/extract` requests and embedded direct reads, and admission occurs before model resolution or downloading. `max_concurrent_requests: 0` intentionally disables both admission accounting and the capacity-derived byte clamp and is not safe for untrusted production traffic. The reader abstraction has two layers:

- `LoadedReader.readBatch`: the stable reader contract used by the server and local direct calls.
- Model-family implementations: native Florence can use a real batch fast path; VLM, GenAI, Pix2Struct, and multistage OCR may keep the serial fallback until their runtimes expose safe batch execution.

Native Florence batching is throughput-oriented. A request batch is chunked by `ANTFLY_INFERENCE_READ_BATCH_SIZE` (default 8, clamped to 1..64), preprocessed into `[batch, 3, H, W]`, then run through the Florence encoder once for the chunk. CUDA and Metal use the incremental decoder KV cache in batch mode when available: self-attention keys/values are appended per row, cross-attention keys/values are precomputed for the whole encoder batch, and the LM head is applied over `[batch, hidden]` at each generated position. Metal exposes the generic eager backend hooks needed by the preallocated KV slab path (`allocUninitF32Shape`, `copyRows2D`, and device-backed row concat/slice) and has a batched device `attention_f32` entry that dispatches all `batch * q_len * heads` rows in one Metal command. The reader pipeline does not need a Florence-specific Metal API. Rows that emit EOS stop contributing new text tokens while the rest of the batch continues. If the batched KV path is unsupported by a backend shape or operator, native Florence falls back to the full batched decoder path; non-native and unsupported reader families keep the serial fallback behind the same `LoadedReader.readBatch` contract.

The current `/read` HTTP response remains all-or-error at the envelope level, matching the existing API behavior. The future generic batch wrapper above should add per-item errors for bad image bytes, media fetch failures, and per-row inference failures without changing the `/read` response schema.
