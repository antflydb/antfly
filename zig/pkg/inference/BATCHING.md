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
| CLAP/ClipClap audio | Indexed decoding, bounded waves of up to eight clips, and one future CPU feature wave overlapping the resident native encoder |
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
input names/dtypes/non-batch shapes, explicit broadcast controls and their exact
values, supervising process boundary, and admission
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

Rewriting first admits a prepared token owner for the bounded request queue and
tokenizes it once, before validation or any model forward. HTTP and direct
execution share these IDs; input usage uses the stored counts and output usage
uses generated IDs rather than re-encoding decoded text. Token IDs are retained
for whole-request validation; padded tensors and model execution stay bounded
to eight-row windows. Tokenizer allocations acquire host capacity before the
backing allocation, including normalization/Viterbi scratch and realloc overlap;
freed scratch returns credit to a request-local pool. Admission grows in 64 KiB
quanta (with exact-size fallback under tight limits), not one RPC per allocation.
After preparation, unused credit is trimmed; teardown reclaims abandoned blocks
before releasing leases. Real Metaspace allocation-failure tests cover both direct
cleanup and this owner rollback. This is enforced allocation accounting, not an
estimate based on UTF-8 length. The retained token owner is
bound to the same session, controller and limits as its consumer.
It partitions the whole bounded queue by token-length class before materializing tensors,
then restores original result order. Subdivision never inherits an unrelated
long request's padding width.
Execution groups are then sized using actual padded token lengths against both
encoder and worst-case full-prefix decoder peaks, including retained encoder
output and preprocessing. Each physical stage counts one workspace rather than
one workspace per sequence; encoder and decoder gates serialize admission and
packing as well as their forwards. Execution width shrinks to singleton when a
larger group does not fit permanent limits, without repeating tokenization.
The prepared owner reserves token storage plus one execution window, not a
window of materialized model inputs for every item in the request.
This is conservative planning, not a reservation of future execution: concurrent
request admission can still return temporary exhaustion.

`Session.planShapes` and `Session.planRun` use the same allocation-free shape
view and native stage descriptor for output bytes and workspace. Native Whisper describes encoder
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

Each output column has its own shared storage lifetime. Direct and fused forwards
coalesce admission reductions for related self-cache/cross-cache columns, retaining
conservative credit until each cohort's last column is freed. Other outputs keep
independent credit lifetimes. This avoids per-layer resource RPCs without retaining
obsolete logits/self-cache buffers. A 129-output callback regression needs only two
intermediate reductions and a final release.

A lone surviving host-cache row can detach from an oversized shared allocation
before its next decode. Exclusive lifetime proof, at least 64 KiB savings and
twofold column shrink are required. Old/new overlap is admitted before copying;
all copies commit together, and capacity/allocation failures retain the original
cache. This does not implement device-resident cache execution or multi-survivor
device gather/scatter.

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
graphs, a native Whisper incremental adapter and device-resident graph caches
still require implementation and qualification. Qualified merged steps now fuse
both prefill (empty past inputs) and equal-position cached steps. The explicitly
broadcast `use_cache_branch` is passed once; differing values cannot coalesce.
Empty cross-cache outputs retain each request's previous cache owner.

Composite runtimes inspect an optional merged decoder's declared graph I/O before
loading either backend session. Unqualified candidates never load weights or
constrain backend selection. Backend-neutral artifact-closure caching fingerprints
all considered graphs; executable policy checks only the selected encoder/decoder
pair. The existing runtime single-flight caches selection by artifact generation,
so warm requests stat dependencies without repeating graph-signature inspection.
The loaded cache ABI is checked again before publication.
Unsupported optional graphs preserve the ordinary route, while resource and
cancellation failures propagate. The considered graphs and their external data
participate in the generation key; selection is pinned with the runtime.

The supervised embedded worker uses protocol v4: provider calls carry the same
task-neutral attachment envelope as HTTP, including per-item provenance in
options. RPC sends borrowed segments without constructing a second media slab;
the receiver acquires capacity for the complete logical message before transfer.
Dense vectors and scores return bounded little-endian f32 payloads with row
descriptors, not JSON arrays. A typed invocation rejects an absent numeric result
rather than falling back to JSON under a numeric-only memory allowance. These
contracts apply independently of model family and do not change public APIs.

Worker route capabilities constrain rendering before materialization. The
worker advertises a 64 MiB complete request envelope
(`attachment_envelope_max_bytes`, applied even with zero attachments) and a
separate 1 MiB attachment-metadata ceiling (`attachment_metadata_max_bytes`).
Metadata is measured from the actual serialized JSON, including escaping, not
from UTF-8 length divided by an expansion factor; text-only requests keep the
original logical JSON ceiling. Only raw PDF planning derives a tightly packed
RGBA pixel ceiling from the payload limit; encoded images keep the model's
pixel allowance, and raw accounting includes stride padding. Embedding windows
check both ceilings with framing arithmetic shared with the envelope encoder,
split mixed text/image windows without shrinking valid text-only batches, and
reject an indivisible oversized item before provider dispatch.

Private provider errors are diagnosed by their owning runtime before the first
ABI or worker-RPC hop normalizes them to `InferenceProviderFailure`. Linked and
isolated inference share one bounded operation/error/model diagnostic policy;
the receiving bridge does not re-log normalized errors or expose
backend-specific error names in its wire ABI, and logs never include request
bodies or media bytes.

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

## GPU residency for media embeddings

Intermediate numeric tensors stay on the inference device for an admitted batch.
Compressed media, parsing, metadata and final vectors remain host boundaries.
Ownership, resource admission and cancellation extend through the last consumer;
residency does not mean retaining an ingestion corpus in VRAM. A backend lacking
required operations retains a typed compatibility route. Explicit resident entry
points reject unsupported capabilities rather than silently exporting activations.

### Transfer boundaries

| Input | Host preparation | Resident work |
| --- | --- | --- |
| Image | Codec decode, model-exact resize/crop, normalized pixels/patches | Patch/token assembly, attention, pooling and projection |
| PDF raster | CPU parser/renderer, borrowed RGBA; no PNG round trip | Vision and image-only embedding prompt/encoder/pooling |
| PDF text | Text extraction/tokenization | Text encoder; rasterization is unnecessary for text embeddings |
| Gemma4 audio | Codec decode, bounded resampling, Hann-window frame packing | DFT, magnitude, mel/log features, subsampling, conformer where supported, projected rows and embedding encoder |
| CLAP audio | Mel/batch-norm/spectrogram preparation and compatibility AFF frontend | Resident Swin encoder, pooling and projection |

GPU preprocessing is not an unconditional speed win: full-resolution pixels can
be much larger than model-sized pixels, and preprocessing competes with inference
for device resources. Unified-memory Metal still incurs synchronization and host
materialization. NVIDIA nvJPEG/nvImageCodec or DALI are potential implementation
references, not dependencies introduced by this work. The shared CPU
[JPEG/PDF preparation](../../../docs/design/inference/gemma4-pdf-performance.md#jpeg-preparation-follow-up)
and [WebP decoder](../../../docs/design/inference/webp-preparation-performance.md)
optimizations do not make codecs GPU-resident.

### Implemented first phase: CLIP eager vision residency

`visionEncoderForwardTensor` borrows normalized F32 NCHW pixels and returns an
owned tensor freed with the creating backend. Patch packing, CLS/position
assembly, head layouts, CLS gather and projection use backend operations.
Accelerated host-facing calls export only the final embedding; Metal explicitly
stages pixels into device storage. CPU retains contiguous host patch extraction.
Static quantized-weight materialization and graph-runtime routes are separate
concerns. CLIP's path does not imply residency in every architecture.

### Gemma 4 vision residency

Patch projection, position addition, head RMS, spatial RoPE, vision blocks,
spatial pooling, standardization, norm and projection retain backend tensors.
Backend transposes preserve Gemma's rotary spatial-axis pairing and SDPA scale;
pooling gathers preserve image/merge order. Static position offsets may be
prepared on CPU. Decode, resize, initial patch packing and the direct projector's
first patch norm remain host work. Diagnostic stage dumps and unsupported
providers retain explicit export/fallback boundaries.

Image-only embedding and generative prompt handoffs now retain projected rows
through the next encoder/prefill where supported. Public host-array projector
APIs, mixed/titled compatibility groups and providers missing operations retain
their existing boundaries. Do not infer residency merely from model family.

### Gemma 4 audio, CLAP, OCR and Qwen extensions

| Route | Numeric residency contract | Remaining boundaries |
| --- | --- | --- |
| Gemma4 audio | 320-sample Hann windows, 160-hop, 512-point zero-padded DFT, **magnitude**, mel projection, additive 1e-3 floor, natural log and valid-frame mask; subsampling/encoder consume features directly | CPU decode/resample/frame packing; CUDA conformer operations without kernels retain host compatibility |
| CLAP eager audio | Patch convolution/layout/norm, shifted/unshifted Swin, relative bias/masks, patch merges, pooling/projection | CPU features/input norm; AFF compatibility before Swin; graph strategies remain explicit |
| OCR CTC | Resident session output, argmax, winning-score gather | Download O(time) choices/scores for blank/repeat collapse, dictionary and confidence; detection/crops/layout remain host routes |
| Qwen3 VL | Patch position addition and base-text/image prompt assembly | CPU static interpolation, final visual/deepstack arrays, opt-in qualification hashes |
| Qwen2 VL / ColQwen | Backend patch-weight reshape and spatial/temporal vision RoPE in merge order | CPU decode/resize/patch packing and metadata; unsupported attention/RoPE retains typed fallback |

Gemma's audio frontend retains the 480,000-sample bound. Transient DFT/mel
matrices are not cached as model weights. `TERMITE_GEMMA4_AUDIO_HOST_OPS=features`
(or `all`) restores CPU operations for diagnosis. Account for live frame/spectrum/
mel scratch before raising concurrency. CLAP's window/head bias scratch scales
with batch, heads and window area; small maps/masks remain host metadata.

CTC validates `[1,time,vocabulary]`, preserves original score/confidence semantics
and reuses the already-computed resident output if reductions are unsupported.
Cancellation/resource failures propagate instead of re-running inference.
Qwen3 hashes are explicit diagnostic synchronization; Qwen2 spatial RoPE restarts
axes in the original grouping, including temporal slices. Synthetic parity and
transfer fixtures qualify these boundaries; CUDA cross-compilation is not CUDA
hardware qualification. Dated pass counts are in the history summary below.

### Gemma 4 cross-image batches and measurement

Same-grid images share patch projection, vision blocks, pooling and projection.
Rows flatten in image/patch order; attention retains independent batch/sequence
axes, and spatial RoPE restarts for each image. Different grids and token/scratch
ceilings split cohorts without padding inside vision attention. Low-level
`ImageBatchOptions` defaults: four images, 10,080 patch tokens, 768 MiB estimated
scratch; explicit offline APIs allow eight, while serving remains at four.
An oversized singleton returns `ImageBatchBudgetExceeded` before GPU work.
Geometry/pixel lengths are validated before allocation.

Encoded and borrowed-raster pipelines reserve bounded host preprocessing before
decode, then acquire GPU scratch under the model execution gate. Queued requests
do not retain vision scratch. Vision and encoder temporaries have disjoint
lifetimes: reserve their larger workspace plus retained/output storage. A
physical vision cohort may split while the downstream encoder still batches.
Codec/validation failures keep original indexes; resource/control/backend failures
propagate. The geometry-qualified image-only broker below coalesces across callers;
client concurrency remains distinct from physical model batching.

`antfly-gemma4-image-bench` scopes: prepared `projector`, encoded `image`, borrowed
raster full `embedding`, and independent-decode `encoded-embedding`. Shared-raster
scope decodes once; encoded scope decodes every row. File reads/model load stay
outside warm timing. Requested count differs from actual projector occupancy;
internal embedding projector counters may be unavailable and reported null.
Final-vector agreement, actual occupancy, scoped transfer counters and memory
counter coverage are separate evidence. Runtime/model pools and bounded workspace
are not interchangeable peak-memory measurements.

Use [image campaigns](../../../tools/embeddinggemma2/image_campaign.py) for pinned
baseline/candidate order, hashes, complete vectors and HTTP concurrency 1/2/4/8.
Warm distributions need ten warmups and at least 100 samples; cold first-forward
runs use a separate process. Amortized batch seconds/image is not caller latency.
Qualify residency and batching separately at actual patch geometry; retain errors
and host-contention observations instead of silently retrying overload.

### Local Gemma 4 image measurements (2026-10-11)

Dated measurements moved to the [media throughput history](../../../docs/design/inference/history/gemma4/media-throughput-2026-10.md#image-residency-and-initial-batches).
The initial four-row projector gained ~11%; eight rows were slower and doubled
activation storage. Serving remains capped at four. Later Flash/frame results
are qualified separately; historical HTTP concurrency did not establish increased
physical batching or a throughput gain. [Original image evidence](../../../docs/design/inference/gemma4-image-performance.json)
retains all samples, hashes, limits and unavailable-counter annotations.

### Gemma4 PDF measurement and resident embedding handoff

Image-only `embedImages`/`embedRasters` retain projected visual rows through
backend prompt assembly, embedding encoder and masked pooling; only final pooled
vectors download. The owner retains its creating backend through cleanup.
`Options.resident_images=false` selects the host comparison. Mixed/titled groups
and unsupported providers retain compatibility. Current audio-only handoff has
its own qualification below.

The [PDF benchmark design](../../../docs/design/inference/gemma4-pdf-performance.md)
measures actual parser/rendered rasters through embedding, with explicit document/
page IDs, round-robin scheduling, at most four live pages, four private readers,
256 MiB raster output, private 256 MiB scratch per worker and eight-million-pixel
page/DPI/dimension bounds. Initial parsing/model load are reported separately.
CPU production rendering avoids image re-encoding between renderer and model.

The Metal PDF painter is **benchmark-only**. It supports opaque closed fills and
axis-aligned nearest-sampled images; any unsupported text/stroke/transparency/
clipping/shading/pattern/rotation or lossy coordinates rejects the page before
submission and uses exact CPU rendering. Bounded workers publish scenes/fallback
classification, permitting painting to overlap remaining CPU fallback; cleanup
joins every worker. Paint order survives 32×32 tile binning, capped at four
million references/16 MiB per page; dense pages use bounds-culled rendering.
Four synchronous reusable buffer slots are capped at 512 MiB, accounted separately.
GPU rasters copy to host for model-exact CPU preprocessing. There is no resident
all-GPU PDF decoder or prepared-page cache.

Pixel gates and vector gates precede timing; failed GPU execution/parity is not
silently replaced by CPU. Compare same-batch render-only and full-pipeline cases,
retaining fallback counts, actual geometry/cohorts, copies and scoped memory.
Enqueue/phase times are not isolated GPU kernel durations. Dated renderer/JPEG
results and unresolved end-to-end variation are in the history and PDF report.
Production rendering stays CPU until representative coverage and end-to-end gains
are qualified.

### Further implementation and measurement

Durable rules above supersede the earlier step-by-step implementation diary.
The [media history](../../../docs/design/inference/history/gemma4/media-throughput-2026-10.md)
records decisions, rejected trials and exact baseline scopes. Extend qualification
with realistic pages/audio lengths and idle-host repetitions. Backend-specific
Flash/fusion/residency results do not transfer automatically to other models or
CUDA. Keep startup, preparation, recognition, embedding and renderer timings
separate; only a measured full pipeline establishes ingestion throughput.

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
- Model-family implementations: native Florence can use a real batch fast path; Pix2Struct and multistage OCR may keep the serial fallback until their runtimes expose safe batch execution.

`/read` can also use Qwen3-VL, a general vision-language model, for OCR and
document transcription. Split GGUF generator bundles are accepted when their
selected artifacts and backend pass the normal runtime compatibility checks.
Qwen requests reuse the resident native Metal generation pipeline and execute
one image at a time so every input still maps to one independent read result;
the outer request admission covers the complete serial batch. This is a
correctness path, not a claim of batched Qwen projector or decoder execution.

Native Florence batching is throughput-oriented. A request batch is chunked by `ANTFLY_INFERENCE_READ_BATCH_SIZE` (default 8, clamped to 1..64), preprocessed into `[batch, 3, H, W]`, then run through the Florence encoder once for the chunk. CUDA and Metal use the incremental decoder KV cache in batch mode when available: self-attention keys/values are appended per row, cross-attention keys/values are precomputed for the whole encoder batch, and the LM head is applied over `[batch, hidden]` at each generated position. Metal exposes the generic eager backend hooks needed by the preallocated KV slab path (`allocUninitF32Shape`, `copyRows2D`, and device-backed row concat/slice) and has a batched device `attention_f32` entry that dispatches all `batch * q_len * heads` rows in one Metal command. The reader pipeline does not need a Florence-specific Metal API. Rows that emit EOS stop contributing new text tokens while the rest of the batch continues. If the batched KV path is unsupported by a backend shape or operator, native Florence falls back to the full batched decoder path; non-native and unsupported reader families keep the serial fallback behind the same `LoadedReader.readBatch` contract.

Resident Florence OCR separates cross-attention cache backing ownership from active tensor shape. In-place EOS compaction moves surviving rows without resizing the backing allocation; the decoder then publishes exact active-prefix views and attends through them. Backing buffers outlive every view (CUDA row views are non-retaining; Metal keeps retained device views), repeated compaction replaces only view descriptors, and teardown releases views before backing storage.

The current `/read` HTTP response remains all-or-error at the envelope level, matching the existing API behavior. The future generic batch wrapper above should add per-item errors for bad image bytes, media fetch failures, and per-row inference failures without changing the `/read` response schema.


### Gemma4 vision throughput follow-up

[The design and qualification report](../../../docs/design/inference/gemma4-vision-throughput.md)
covers model-scoped Flash attention, per-layer Metal command frames, image-only
geometry buckets across requests and bounded CPU preparation lookahead. The
serving ceiling remains four. Grid axes, model generation, task, dimensions and
source/options must match; mixed groups retain their compatibility capability.
Preparation holds a current wave and at most one future CPU wave, acquires no
speculative GPU permit and drains workers on every exit. Diagnostic flags and
A/B benchmark commands are in the report. `encoded-embedding` additionally
measures an independent decode for every row; the existing `embedding` scope
uses a shared borrowed raster after one decode.

### Gemma4 and CLAP audio preparation and batching

See [audio throughput](../../../docs/design/inference/audio-throughput.md) for
real-model paired measurements and limits. Gemma4 audio-only accelerated cohorts
retain projected rows through prompt packing and physically batch the embedding
encoder in length-qualified groups of at most four (25% padding, 1,024-token
sequence cap); conformer execution remains per clip. CLAP retains native batches
of up to eight and singleton fusion/fallback behavior. Both prepare one bounded
future CPU wave, join producers on every exit and preserve indexed codec errors.
Gemma4 audio-only server groups can use the generation-qualified broker; mixed
and titled groups retain compatibility. No CUDA performance gain is claimed.

### Gemma4 generative OCR optimization

The [OCR design](../../../docs/design/inference/gemma4-ocr-optimization.md)
owns runtime controls, operation-family contracts and qualification gates;
[OCR history](../../../docs/design/inference/history/gemma4/ocr-throughput-2026-10.md)
summarizes the superseded matrix/MTP experiments and links full evidence.

Ordinary generation remains serialized. The native OCR opt-in retains independent
page positions/KV in bounded cohorts of at most four, with optional continuous
request windows of eight (maximum sixteen), reserving every live KV allocation.
Projector/prefill and decode use distinct protected ownership boundaries; only
token IDs return during eligible greedy decode. Finished pages retire promptly.
The full prepared singleton operation family is retained by default. Matrix
arithmetic additionally requires `ANTFLY_GEMMA4_METAL_OCR_MATRIX_ARITHMETIC=1`;
the legacy multirow switch alone no longer selects it. Scoped rollbacks/EOS
confirmation cannot repair accumulated noncanonical history.

The final local Q4 invariant-tail/QKV/carrier/head-frame case takes 48.819–49.304
seconds for **all eight pages**, versus same-binary ordinary brackets of
55.339–57.572 seconds: 12.8% less mean time with exact transcripts/counts.
Shared-weight kernels are excluded from that configuration. All experimental
controls remain off; no Antfly MTP speedup, CUDA performance gain or audio
text-generation speedup is qualified by this OCR campaign. PDF rendering and
startup are outside recognition timing. Historical matrix speedups failed later
paragraph/order/memory gates and are not current serving claims.

The live design preserves the experiment flags, diagnostic exclusivity and frame/
KV failure rules. The sweep rejects incomplete/capped output before ranking
throughput, excludes diagnostic/probe timing, samples worker RSS/Metal separately
and records HTTP payload limits independently from model admission. Promotion
requires complete free-running output gates and repeated bracketed gains; token
counts, `stop` status or operator equality alone are insufficient.
