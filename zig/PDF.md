# Bounded document preparation and multimodal inference

This document describes how Antfly turns documents into bounded inference work.
PDF extraction, page rendering, OCR, generation, and embedding share document
preparation, media transport, scheduling, admission, identity, and failure
semantics. The same model-work contract also covers reranking, chunking,
schema-driven extraction (including classification), rewriting, and
transcription. Every public task keeps its own typed request and output
semantics; classifier remains an internal extraction executor kind.

Florence 2 is the first natively batched reader implementation, not the
architecture boundary. Gemma4 multimodal is a generator: PDF OCR with Gemma4 is
a batch of independent generation requests containing page images and an OCR
prompt. ClipClap is a multimodal embedder: it embeds PDF-derived page images,
extracted text, or chunks, but does not interpret a multi-page PDF container.
Antfly must prepare those inputs first.

The durable document pipeline must not be confused with template-time PDF
helpers. `remotePDF` is a deprecated compatibility helper that extracts text.
`remoteMedia mode="render"` renders only the first PDF page. Those helpers
prepare one inference request and do not provide durable, bounded, multi-page
orchestration.

## Implementation status

Status: implemented. Bounded single-pass PDF preparation, shared render
windows, indexed reader/generator/embedder execution, distributed model-aware
routing with capability leases, lease-fenced durable page-image embedding, and
BackendRuntime-owned render and preprocessing lanes are in production paths.
Release-environment qualification is tracked under [Open work](#open-work).

## Limits and defaults

Operator environment variables are read by the enrichment runtime
(`enrichment_runtime.zig`) unless noted. Empty, malformed, or overflowing values
fall back to the default; values that would stop progress clamp to one. The
public `execution` policy (`ExecutionPolicy` in
`specs/openapi/antfly/indexes.yaml`) can request smaller values, never larger
ones.

| Setting | Default | Bound and semantics |
| --- | --- | --- |
| `execution.max_document_pages` / `ANTFLY_ENRICHMENT_PDF_MAX_DOCUMENT_PAGES` | 2,048 pages | Effective limit is `min(request, operator, 16,384)`; an omitted request uses the operator ceiling. Exceeding it is a stable terminal request failure. |
| `execution.batch_items` (OCR) / `ANTFLY_ENRICHMENT_OCR_BATCH_ITEMS` | 8 | At least 1 |
| `ANTFLY_ENRICHMENT_OCR_BATCH_MAX_ITEMS` | 16 | Clamped to 1..128; 128 is the absolute control-plane batch ceiling, also applied by the inference server |
| `execution.batch_bytes` (OCR) / `ANTFLY_ENRICHMENT_OCR_BATCH_BYTES` | 64 MiB | At least 1 |
| PDF page-image embedding batch bytes | 64 MiB | Separate from the text-embedding defaults (32 items / 256 KiB, `ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS` / `_BYTES`) so an eight-page image batch keeps singleton per-page quality |
| `ANTFLY_ENRICHMENT_OCR_RENDER_PARALLEL_PAGES` | 1 | Clamped to 1..8; never derived from CPU count |
| `ANTFLY_ENRICHMENT_OCR_RENDER_INFLIGHT_PIXELS` | 50,000,000 | Pixels in one concurrent render wave |
| `ANTFLY_ENRICHMENT_OCR_RENDER_INFLIGHT_BYTES` | 256 MiB | Render working-set ceiling |
| `ANTFLY_ENRICHMENT_PDF_RENDER_PREFETCH_BATCHES` | 1 | `0` restores strict acquire-render-infer-release; values above one clamp to one |
| `ANTFLY_ENRICHMENT_PDF_SHARED_WINDOW_MAX_PAGES` | 32 | Clamped to 1..128; widens shared render windows only, never a model batch |
| `ANTFLY_INFERENCE_READ_BATCH_SIZE` (inference) | 8 | Clamped to 1..64; `1` publishes `serial_compatibility`. Shared by the Florence executor and the capability resolver |
| `ANTFLY_INFERENCE_READ_PROFILE` (inference) | off | Per-page render and OCR batch profile events |
| OCR `render_dpi` / `render_resolution` | 150 / `"requested_dpi"` | `"model_input"` is an explicit opt-in |
| OCR `max_rendered_pixels` / `max_rendered_dimension` | 40,000,000 / 4096 | Per-page spatial caps |
| Read endpoint images per request (inference) | 64 | Aggregate downloaded input is capped by `ANTFLY_INFERENCE_READ_BATCH_BYTES` (default 256 MiB); decoded-pixel admission also applies. See [`pkg/inference/BATCHING.md`](pkg/inference/BATCHING.md#readerocr-batching) |
| PDF render lane (`threaded_io_limits.zig`) | 4 workers, queue 8 | 512 MiB window scratch, 2 MiB retained scratch per worker |
| Lazy PNG representation encode (`encodeParallel`) | 8 jobs | Also capped by the PDF render lane's concurrent capacity; each job holds an admitted scratch grant |
| Concurrent window inference jobs (`WindowJobs.jobs`) | 4 | Embedding and controlled read/generation jobs share the slots; also capped by the inference lane's concurrent capacity and admitted memory |
| Image-embedding codec slab (`BatchPreprocessOptions` in `pkg/inference/src/pipelines/image.zig`) | 128 MiB, 8 preprocess workers | Physical ceiling for all codec scratch in a wave; the slab grows from wave demand and each growth delta is admitted first. Workers are also capped by CPU count |
| Per-document scratch hint | measured peak + 12.5% | Reservation hint only; never changes quality |
| Remote capability cache (`remote_capabilities.zig`) | 30 s fresh, 5 min stale-if-error | 64 entries, 30 s discovery timeout, 4 MiB catalog response |
| Go inference proxy request body (`Config.MaxRequestBodyBytes`) | 256 MiB | Reduced if needed to fit the retained budget |
| Go inference proxy body+decode budget (`Config.MaxRetainedBodyBytes`) | 768 MiB | Process-wide, FIFO by arrival, held through retries |
| Proxy catalog fan-out | 8 concurrent bodies | 8 MiB per body, 32 MiB merged descriptor set, 64 MiB snapshot residency; freshness is twice the refresh interval, clamped to 30 s..5 min |
| Embedded worker transport | 64 MiB envelope (`attachment_envelope_max_bytes`) | 1 MiB attachment metadata (`attachment_metadata_max_bytes`) |
| Numeric response frames (`application/vnd.antfly.numeric.v1`) | 4 MiB | Dense vectors and scores only |
| Tensor-forward fusion window | 8 submissions | 64 rows, 64 MiB input |

## Quality-preserving rendering and scratch admission

`ocr.render_resolution` defaults to `"requested_dpi"`: render at `render_dpi`
subject to the declared spatial caps, then apply the model's preprocessing.
The explicit `"model_input"` policy permits direct rendering near the model's
preferred input dimensions. It is an opt-in quality/performance tradeoff, not
an automatic consequence of resolving model capabilities. Rasterizing text at
low DPI is not equivalent to high-DPI rasterization followed by resampling.
The same policy applies to page-image embedding and OCR generators/readers.

Aggregate pixel pressure shortens windows; scratch pressure reduces renderer
concurrency. Neither changes the prepared page geometry. Insufficient
single-page admission is an explicit failure. Under `requested_dpi`, encoded
output limits cannot trigger repeated render/downscale attempts. Spatial caps
and provider input limits remain explicit constraints on admissible geometry.
The same wave planner runs before reservation and after a partial owned grant.
An estimate larger than the grant reduces concurrency to one; it does not
reject that page without attempting the hard-bounded renderer. Scratch and
retained outputs share the extraction slice, so this distinction matters even
when the page fits the configured per-window scratch ceiling.

Render lanes use private freeing size-class slabs with immediate slot reuse,
not a debug allocator. Backing pages, including cached empty pages, are charged
to the hard scratch grant. Idle slabs/pages are evicted before allocation
failure; all lane memory is released after joined execution. Small scanner
allocations therefore avoid repeated system mappings and allocator quarantine
fragmentation without sharing mutable PDF readers across threads.
The heap implements moving remaps as bounded allocate/copy/free when in-place
growth is unavailable. Both old and replacement buffers stay accounted during
the move. This preserves the serial renderer's realloc semantics and logical
materialization charges; forcing replacement allocations through the caller
would artificially exhaust the vector-text work budget and degrade valid pages.

An underestimated window may request one non-blocking resource-manager grant
up to its configured scratch ceiling while the page is still rendering. The
callback cannot reclaim memory or wait for another invocation's grant. Workers
share one atomic byte budget and serialize this growth operation. Output and
model reservations remain live throughout. If a larger grant requires a
replay, only failed page identities are retried, serially, under the original
deadline; completed page buffers are preserved. A single page that already
failed after growth is not replayed with the same grant.

Each prepared document records a measured per-worker scratch high-water hint,
including allocator backing and content-dependent font/path work, with 12.5%
headroom for subsequent windows. Hints affect reservations only, never quality,
and remain subordinate to the node's hard limit. They are document-local and
do not create an unbounded global cache.

Resolution policy participates in shared-transform and page-embedding
identities. Document metadata/content fingerprint domains and page-embedding
identity versions are advanced so pre-policy completed artifacts cannot pass
the normal unchanged-source checks on subsequent processing. This does not
automatically schedule a cluster-wide reindex.

Validation requires equivalent page text and geometry, bounded memory,
per-item ownership/cancellation tests, and paired end-to-end measurements.
Use `scripts/bench/pdf/compare.py` to alternate frozen baseline/candidate
processes. Separate `auto` and `always` OCR, and sweep `--reader-batch-size`
1/2/4/8 independently of renderer concurrency. Keep failed or unequal-output
runs in the report; never turn them into speedup claims. The renderer-only
`render-window` diagnostic accepts dimension `0` for requested-DPI rendering.

The macOS compatibility renderer produces RGBA directly from its serialized
session, with no PNG encode/decode round trip charged to a one-canvas output
reservation. Native quarter-turn rotation renders the unrotated canvas in
scratch and allocates only the final rotated canvas from retained-output
credit. On a worker-budget failure, OCR/generation and embedding windows share
one scratch-retry path: it reserves the delta non-blockingly up to the
configured scratch ceiling (accepting a partial grant, clamped atomically to
slice and host headroom without invoking reclaimers), keeps output credit
pinned, and retries rendering once with one worker. No model invocation is
replayed; deadlines, transforms, page identities, and output caps stay in force.

## Long-term architecture

```text
DocumentSource
      |
      v
PreparedDocument
  identity, MIME, page metadata, immutable parsed state
      |
      v
bounded transformation stream
  PageText | ChunkText | PageImage | other media
      |
      +--> ReaderExecutor    -> structured read/OCR results
      |
      +--> GeneratorExecutor -> generated text or tool output
      |
      +--> EmbedderExecutor  -> vectors
      +--> Other typed model-family executors
           rerank | chunk | extract | rewrite | transcribe
           (`classifier` is an internal extraction executor)
```

The reusable abstraction is document preparation plus typed work scheduling,
not a universal reader. All model-family executors may share attachments,
admission, batching, provenance, cancellation, and per-item result envelopes;
they must not share task semantics merely because several can consume the same
prepared asset.

### Two-level scheduling for every task and provider

Scheduling belongs on both sides, with different ownership. Antfly owns the
logical work queue: partition ready requests by execution compatibility before
materializing media, share source/transform windows, apply host memory limits,
and preserve durable identities and typed publication. The inference backend
owns physical execution: combine eligible work from multiple Antfly clients,
admit it against the loaded model and node/device resources, and dispatch it
through that task's executor. Embedded inference uses the same separation
without requiring an HTTP hop.

This contract applies to readers, generators, dense and sparse embedders,
rerankers, extractors (including classification), chunkers, rewriters,
transcribers, and other model families. It also applies to local and distributed
Antfly routes and external providers. It does **not** imply that every executor
supports a fused batch, every task accepts images, or that Antfly controls an
external vendor's internal scheduler. Unsupported batching uses bounded
singleton/serial execution with accurate observed telemetry.

Compatibility must include the resolved model generation, task, provider and
endpoint/authorization scope, prompt/schema/options, input representation and
resource class—not just the model name. Embedding rows remain independent;
reranking preserves each query/candidate group and its score mapping;
extraction preserves schema and per-item results; generation preserves separate
conversations, sampling state, and output streams. Continuous token batching
is a generator-executor capability, not a generic concatenation of messages.

Antfly should batch already-ready work without an additional fill delay. A
backend may use a bounded microbatch delay where useful, within the caller's
original deadline; both layers must not independently restart that deadline or
wait to fill maximum-sized batches. Limits cover queued descriptors, retained
media, tokens/pixels, output bytes, concurrency, and tenant fairness. Host
executor slots and memory grants do not imply remote model permits: node-wide
admission remains authoritative even when jobs target different models.

The node broker has reader, dense/sparse text embedding, image/audio embedding,
and GLiNER encoder adapters. Mixed embedding requests partition by modality and
scatter results to their original indexes. GLiNER entity, relation, structured
extraction, and classification passes share encoder preparation but retain
their typed decoding. Classification excludes padded neighboring words from
its score reduction. Native split GLiNER bundles register native extraction;
ONNX exports retain conservative compatibility execution.

Fail-fast encoded-image calls (including distributed `/embed`) and local
borrowed page rasters coalesce only when the **loaded model** advertises native
image batching. Group identity includes immutable loaded generation,
task/instruction options, media representation, and backend resource class.
Callers keep their model handle and admitted inputs while waiting; grouping
happens before model locks. Individual cancellation does not stop live peers,
corrupt images fail only their own slots, and resource/runtime failures do not
cause retry amplification. Traced HTTP requests and explicit per-item-error
envelopes keep their existing executors. Audio holds an exclusive asset lease
and flushes bounded PCM/feature windows. Broker metrics count native results
separately from groups merely collected. See
[`pkg/inference/BATCHING.md`](pkg/inference/BATCHING.md#cross-request-embedding-details).

Generation uses its existing token-step coordinator, not the encoder broker.
Direct native generation and non-streaming HTTP native generation with
request-local backend state can participate. Whole-request graph, speculative,
prompt-cache, streaming, and shared Metal/CUDA owners retain their gates.
Whole-request native owners leave the token coordinator before taking the
model gate, preventing a turn/model-lock cycle with isolated peers. Temporary
request allocators accessed by peer token steps are synchronized; no returned
value retains the stack-local allocator wrapper.

Rerankers, rewriters, transcribers, and other unqualified executors retain their
existing typed routes and bounded execution. The current chunk endpoint is a
fixed text/media transformation, not a learned model: it has no model batch to
fill and should not incur coalescing latency. These distinctions are deliberate;
universal fused batching across every backend/provider is not a completed
feature claim. External vendor scheduling remains outside Antfly's control.

### Task-neutral document preparation

A `PreparedDocument` owns source identity, MIME type, stable page metadata, and
the lifetime of immutable parsed state. It exposes lazy transformations rather
than eagerly retaining every representation:

- embedded page text and layout metadata;
- chunked text;
- rendered page images;
- stable document, page, and transformation identities; and
- source fingerprint plus render/extraction parameters.

Consumers declare the assets they require. The planner renders only the next
admitted window, shares an immutable parse and safe window-local results when
multiple consumers have compatible requirements, and releases rendered bytes
after all consumers of that window finish. A future transform cache is keyed by
source fingerprint, page, renderer version, DPI, pixel/dimension limits, and
render profile; it must never be keyed by URL alone.

Admitted page preparation returns an ordered plan or a page-local geometry
failure. Invalid boxes, rotations, and page numbers occupy result slots but
consume no pixel budget and launch no render worker. Configuration, resource,
cancellation, and document-reader failures still terminate the operation; they
are not disguised as isolated bad pages.

Persistent preparation and planning memory participates in the same
`document_extraction_working_set` admission as rendered windows. Coordinator
state, page descriptors, stage identities, and publication lookup tables use
budgeted allocators; moving a key into a bounded replay window explicitly
copies ownership to that window's allocator. Retaining a parse or a plan does
not silently bypass the renderer's process budget.

The prepared-source cache is pressure-reclaimable, not pinned for the complete
document group. A source lease pins downloaded bytes; a PDF lease pins both
its parse variant and its source. Idle sources and unused parse variants can
be evicted on cache allocation pressure or another executor's ResourceManager
admission request. Reclamation never waits on a cache lock held by the caller,
and shutdown retires the registered reclaimer before destroying its context.
Active preparations retain their source lease through publication.
ResourceManager resolves slice-local pressure from that slice's reclaimers
before considering aggregate pressure, rather than evicting unrelated caches
that cannot satisfy the local limit. Reclaiming preparation also returns
unused amortized allocator credit, while preserving live allocations and
explicit invocation pins.

Compatible text consumers additionally share task-neutral embedded text,
regions, geometry, and extraction warnings. The first traversal stores these
records in bounded, batched, attempt-private storage keyed by content digest,
decode limits, and page. Later consumers load the neutral record before any
page-text extraction and apply their own OCR mode, quality policy, offsets,
and prompting. Records are separate from finalized model-specific OCR units.
Only documents with another compatible text consumer enroll; resource denial
disables this optional cache and falls back to ordinary bounded extraction.
Recovery markers and attempt cleanup cover these records too, without
retaining all page text in memory or rerendering pages to recover metadata.
The optional write buffer is admitted by actual allocated capacity, not by a
full replay-segment reservation. A nonblocking reclaimer may discard its
unpublished rows when required work needs that memory; published rows remain
valid. The collector flushes and releases this buffer at its actual generated
work boundary, including short document tails and byte-limited batches.
This prevents a second consumer's optimization from starving the first
consumer's render/inference window. During a sequential scan, the first absent
record marks the end of the cached prefix, so cold documents do not pay a
storage transaction and temporary read admission for every remaining page.

Consumer byte fingerprints compose the prepared source and locator SHA-256
digests with length-framed extraction parameters. Fingerprint composition no
longer rescans downloaded bytes or large inline data URLs for each consumer;
source-cache lookup and JSON materialization remain separate costs. The v2
fingerprint domain intentionally invalidates legacy
byte fingerprints once; metadata-based fingerprints retain their existing
contract. This is identity preparation shared across typed executors, not a
change to model inputs or outputs.

### Compatibility-first queues and shared render windows

The bounded deferred-asset queue is partitioned by execution compatibility
**before materialization**. Interleaved producers `A, B, A, B` become compatible
groups `A, A` and `B, B`; ordering within a group is stable. Only request indexes
and borrowed configuration participate in planning. The materializer retains
one byte-bounded batch plus its next candidate, not every producer's inputs.
Dependency-establishing copy and document-extraction requests remain in their
normal ordered path.

Within a document group, the enrichment thread owns a shared-window scheduler.
Before collecting OCR pages or preparing an embedding window it resolves
compatible consumers and chooses a bounded common-multiple render width. A
four-item OCR executor or page embedder can therefore produce a sixteen-page
shared window for a compatible visual embedder, while still invoking its own
model in batches of at most four. Image-only owners do not enroll OCR peers
without the embedded-text metadata needed by their selection policy.
The optional widening ceiling
is `ANTFLY_ENRICHMENT_PDF_SHARED_WINDOW_MAX_PAGES` (default 32, absolute maximum
128); it does not change the OCR model batch ceiling or provider request limits.
Collection remains byte-bounded and flushes its current prefix if lookahead
allocation is denied. Generated units transfer ownership into the execution
slice rather than duplicating all retained text and regions. Renderer geometry,
pixel ceilings, and composite memory admission can shorten any planned window.
The retained window uses the renderer's `pdf_render_max_inflight_pixels`, not
one model invocation's pixel ceiling. Per-page geometry still fits the model;
OCR and embedding executors independently partition the window by their own
item, byte, and aggregate pixel limits. Renderer waves keep their thread,
scratch-byte, and pixel bounds independently of those executor partitions.
Preparation uses the native renderer's admission-adjusted geometry, including
the per-worker scratch/decode reservation, physical pixel ceiling, and raw
output allowance. Both window-prefix accounting and execution use that same
source-bound plan. A page larger than the physical window is adaptively scaled
before prefix selection, rather than rejected before the renderer can admit it.
After a widening denial, candidates descend through whole owner batch widths
before trying sub-batch prefixes. Pixel-limited prefixes follow the same rule.
This preserves native owner batches when a wider consumer cannot be enrolled.
Embedding owners partition borrowed descriptors by their own item/byte/pixel
limits and stage each successful sub-batch before moving to the next one.
Memory contracts are requested only for legal model sub-batches. Embedding
windows retain the page buffers plus the maximum sequential invocation peak,
not the sum of invocation peaks: vectors are staged and freed between calls.
The envelope includes every legal item count so smaller provider invocations
with larger memory requirements remain covered. OCR's request-specific
contracts retain their conservative composite allowance; its persistent typed
document results remain on the independently accounted document allocator.
Remote reader, generator, and extractor memory plans use the same task/model/
authentication-scoped capability cache as execution to select framed versus
legacy transport. Compatible remote singleton generators use the batch media
transport too; unsupported batch configurations retain data-URI accounting.
Generator planning reuses its parsed, routed configuration for compatibility
and capability resolution rather than duplicating config trees within the
bounded contract-resolution allocator.
Before dispatching an owner's rendered window or starting speculative render
prefetch, it offers the borrowed pages to later compatible consumers. Physical
compatibility includes source identity and credentials, DPI, pixel/dimension
limits, preferred image geometry, renderer ceilings, and decode limits. Model
names, prompts, and result types are not render keys. Representation and encoded
output allowances remain consumer contracts, separate from physical geometry.
A raw-raster owner can lazily encode one lossless PNG representation of its
window for compatible PNG consumers. Every consumer borrows those same PNGs;
encoding scratch is released before inference and only the actual PNG bytes
stay reserved. A separate bounded, non-reclaiming consumer lease accounts for
compression growth and retention alongside the pinned raster. Cancellation and
deadlines bound compression work. No representation survives the window.
Demanded pages may encode concurrently on the BackendRuntime-owned PDF CPU
lane (at most eight jobs and no more than the lane capacity; `encodeParallel`). The coordinator
admits every codec peak before launching a wave, joins it, then releases unused
scratch credit. A narrow byte grant reduces concurrency without reducing image
quality. There is no additional encoder pool.

Context-aware embedding peers may run concurrently with the window owner's
OCR or embedding invocation, as well as later text consumers. A structured
window group returns pending work to the owner instead of joining inside the
fan-out callback. The owner joins before releasing or transferring away any
borrowed media; rejected pages and early exits cancel and join first. At most
four embedding invocations are retained (`WindowJobs.jobs`), further bounded
by the lazy inference lane and jointly admitted memory. Each job owns its
allocator, cancellation/deadline, descriptors and result; image bytes remain
borrowed. Embedding jobs launch before text consumers, independent of their
declaration order. Only the coordinator attributes failures and publishes
fenced stages. Early exit cancels and joins all jobs before releasing media.
Legacy embedding callbacks remain synchronous. This overlaps independent
embedding peers with reader/generator consumers; it does not parallelize all
text consumers or change the owner's traversal.
If a PNG exceeds a consumer's singleton output allowance, or the representation
cannot be admitted, that consumer retains its ordinary bounded traversal. The
shared cache never resizes pixels to fit another consumer's wire policy.
This optimization is intentionally directional: an encoded owner may already
have resized its pages and cannot supply the original raster geometry. It is
not a document-wide cache or a claim of order-independent render sharing.

Shared raster windows resolve each consumer's durable pending-page mask before
PNG materialization and reuse it during consumption. All-cached consumers
perform no encoding; a page-indexed window cache compresses only demanded
pages and releases them with the window. Metadata, retained PNGs, and the
active compressor's peak share one bounded budget.

- Enrollment checks completed OCR state using the same metadata/byte
  fingerprint and navigation-readiness gate as ordinary document extraction,
  before capability discovery or inference. The ordered request loop reuses
  that completed decision instead of rereading state and hashing the source
  again. Byte checks borrow the prepared source; enrollment never downloads it
  again. Speculative discovery failures leave the consumer on its ordinary
  path, where completed-state skips and normal error handling remain
  authoritative.
- Each consumer partitions the borrowed window by its own item, byte, and
  pixel limits. Admission follows the window's physical lifetime: after all
  render waves join and their lane Readers/heaps are destroyed, scratch credit
  is returned. Pooled executor threads do not retain those per-window heaps.
  Media and the owner's output/inference allowance remain pinned continuously.
  Synchronous consumers borrow the idle part of that allowance, reserving only
  any excess transport/inference/result peak. Nested OCR result-segment and
  invocation scopes claim disjoint credit. The owner's allocator cannot grow
  while credit is lent; scopes must free all allocations before returning it.
  Failed admission leaves the owner's grant unchanged, and owner inference
  never has to reacquire its baseline credit. Consumers neither copy the page
  buffers nor retain their allocator.
- Sharing must preserve batching, not merely reduce rendering. A partial
  window is declined for the consumer's remaining traversal unless it fills
  a whole number of that consumer's item windows. Thus a singleton owner cannot
  turn a sixteen-item consumer into synchronous singleton calls. A whole short
  document can still be shared. This conservative admission rule may forgo
  sharing when pixels/bytes would independently force small batches; it keeps
  the normal bounded traversal and does not retain images across windows or
  reorder dependency-establishing requests. Joint-width planning retains this
  fallback: the media window receives composite admission, and each executor
  must acquire its incremental peak before consuming the borrowed pages.
- Typed OCR replay tracks staged page bounds per consumer, separately from the
  neutral text-cache registry. A consumer with no staged results performs no
  replay admission or reads. Populated ranges are copied in byte/item-bounded
  segments under one read transaction, then decoded after the transaction closes.
  Read contention remains retryable; denied optional replay memory falls back
  to ordinary execution without discarding already restored siblings.
- Page embedders stage vectors in their existing source-hashed, lease-epoch
  namespace. Final or staged matching vectors are skipped before inference.
  Pending page descriptors are compacted before byte/pixel/item partitioning,
  preserving page identities and borrowing the original buffers. Alternating
  cached/pending pages therefore produce full batches instead of singleton
  calls. If filtering leaves an item-count tail in a partial document window,
  sharing is declined before staging so the independent traversal can fill
  the batch from later pages; whole short documents remain shareable.
  Both owner and shared-consumer paths checkpoint successful page vectors
  before returning a sibling page failure. Retries invoke only missing pages;
  final publication, stale cleanup, and coverage still require a complete
  document. Invalid result identities are rejected before owner-stage writes.
  Page identity is described under
  [PDF embedding semantics](#pdf-embedding-semantics).
- Reader- and generator-backed OCR consumers preserve their distinct prompts
  and typed output handling. Compatible OCR page-selection and quality policies
  permit finalized units to be privately spooled and restored during the
  consumer's ordinary traversal. A page-image-only owner does not invent the
  embedded-text metadata needed to decide another consumer's auto-OCR policy.
  Custom extraction routes and incompatible policies retain independent
  execution.
- Only typed results survive the window callback. Each consumer keeps its own
  publication and coverage boundary. An unavailable optional memory lease
  falls back to its normal traversal; a consumer failure does not mutate the
  owner's units or prevent other compatible consumers from using the window.
  Private OCR rows are removed when their active group finishes. The first
  result transaction also registers the outstanding attempt; cleanup removes
  that marker only after every result row has been deleted. The single-flight,
  lease-owning replay pass scavenges the bounded registry before starting any
  document groups, including attempts whose consumers or source documents
  were removed. Ordinary text documents incur no per-document PDF cursor or
  cleanup transaction. Both rows and registry are store-local metadata outside
  document ranges, so shard range transfer cannot separate temporary results
  from their recovery metadata. They never participate in public artifact scans.
- Planning and publication stay on the enrichment thread. Synchronous text
  peers run first; bounded window jobs hold independent grants, and a
  controlled owner may overlap peers within the same host concurrency ceiling.
  Explicit admission denial drains the cohort before one serial retry. Optional next-window rendering may overlap the final
  admitted embedding cohort. Renderer workers never enter enrichment state or
  concurrently use another consumer's PDF session. There is no document-sized
  rendered-image cache.

Owned key collections retain their allocator extent: growing lists free their
elements and deinitialize the list by capacity; exact owned slices use slice
cleanup. This applies to normal publication and failure cleanup, including
embedding-stage, stale-artifact, and chunk-key collectors. Allocation-failure
regressions exercise partial list construction with a checked allocator.

Typed asset batch failures consume the failing item's durable retry budget,
not the identity of whichever item happened to lead the batch. Successful
siblings are applied before a retained retry is returned, including serial
compatibility execution. Publishing those siblings does not forgive the
pending failed request's durable attempt count. Borrowed-image read
invocations carry cancellation and deadlines through model loading and backend
execution. A fused invocation stays live while any member is live; once all
members cancel or expire, its backend control stops the work. Canceled members
do not receive results belonging to still-live siblings.

### Runtime-owned execution lanes

Worker lifecycle belongs to `BackendRuntime`, not to a renderer, codec, model,
or request. The runtime constructs bounded executor objects, while
`std.Io.Threaded` staffs their worker teams lazily on the first asynchronous
submission and retains those workers until coordinated runtime shutdown.
Production document rendering and image preprocessing borrow the inference
executor through a lifetime-safe owner; they do not create process-global or
per-request pools. Their per-window item, pixel, and byte limits remain local
admission controls layered below the aggregate runtime thread ceiling.

Low-level libraries accept a generic borrowed `std.Io` rather than importing
Antfly storage/runtime types. This preserves layering and lets tests or embedded
hosts supply their own executor. A missing executor is an explicit compatibility
mode: image preprocessing runs serially, while the PDF library retains its
bounded call-scoped worker fallback. If profiling eventually justifies isolated
render and model-compute queues, `BackendRuntime` should expose additional lazy
leased lanes while retaining one aggregate process admission budget.

### Task-specific executors

| Task | Example | Input | Output |
| --- | --- | --- | --- |
| Read/OCR | Florence 2 | page image plus read prompt | structured page text |
| Generate | Gemma4 multimodal | independent message containing a page image | generated text/tool output |
| Embed | ClipClap | page image, text chunk, or audio | vector |
| Rerank | text or multimodal reranker | query plus prepared candidates | ranked candidates |
| Chunk | tokenizer/semantic chunker | extracted page/document text | chunks |
| Extract | GLiNER, classifier, or multimodal extractor | text, page image, or prepared item | structured extraction, including classifications |
| Rewrite | rewriter model | extracted or generated text | rewritten text |
| Transcribe | Whisper-family model | prepared audio item | transcription |

A generator used for OCR is still a generator. It keeps generator sampling,
chat-template, output-schema, tool, and result semantics. A reader keeps its
structured reading result. An embedder produces vectors and never passes
through text-quality selection.

### Resolved model capabilities

Batching decisions belong to the resolved model and backend, not merely the
provider enum or a model-name substring. The model resolver exposes an
`InferenceCapabilities` value containing at least:

```zig
const InferenceCapabilities = struct {
    task: Task,
    input_modalities: Modalities,
    accepted_mime_types: MimeTypes,
    input_granularity: enum { document, page, chunk, item },
    batch: struct {
        mode: enum { none, serial_compatibility, native },
        preferred_items: usize,
        max_items: usize,
        // null = unknown/not published; zero = encoded media is disabled.
        max_encoded_media_bytes: ?usize,
        max_decoded_pixels: ?u64,
        max_media_parts_per_item: usize,
        per_item_failures: bool,
    },
    task_limits: struct {
        max_text_bytes_per_item: ?usize,
        max_input_tokens_per_item: ?usize,
        max_output_tokens_per_item: ?usize,
        max_candidates_per_request: ?usize,
        max_schema_bytes: ?usize,
    },
    output: enum {
        read_result, generated_text, embedding, ranked_items, chunks,
        extraction, rewritten_text, transcription,
    },
    result_cardinality: enum { one_per_item, one_per_request },
    prompt_policy: enum { explicit, model_default, structured_schema },
    borrowed_attachments: bool,
};
```

`MimeTypes` is a bounded value-semantic set: common types have compact flags,
while additional validated type/subtype essences travel inline through the
checked native ABI. The public catalog carries canonical essence strings, so a
new TIFF, FLAC, or vendor document format does not require a capability-schema
change.

Capabilities are discovered after model resolution and may differ by backend.
Configuration may restrict a capability but must not assert support the loaded
model does not have. Compatibility serialization is reported as
`serial_compatibility`; it is never described in telemetry as a native batch.
The local resolver and remote `/ai/v1/models` catalog use the same normalized
native-batch flags. Manifests may lower the server defaults with
`inference.batch.preferred_items=`, `inference.batch.max_items=`,
`inference.batch.max_encoded_media_bytes=`,
`inference.batch.max_decoded_pixels=`, and
`inference.batch.max_media_parts_per_item=` capabilities. Planning uses these
values for window formation and every executor validates the concrete
invocation again, so a caller cannot bypass the limits by skipping the planner.

### Distributed Antfly inference boundary

The same design applies when storage/enrichment and inference run on different
Antfly nodes. Direct execution against a configured dedicated inference-node
URL is supported by the typed model clients. The node that
owns enrichment prepares the document, performs bounded PDF parsing and page
rendering, and retains each rendered window only until its consumers finish. A
remote executor then sends that already-bounded window to the inference node.
The PDF container and an unbounded set of page images are never forwarded as
implicit remote state.

Capability discovery is scoped to the model, semantic task, concrete transport
operation, and effective authentication identity. Antfly clients query
`/ai/v1/models?model=<model>&task=<task>&operation=<operation>`, where task is
one of `read`, `generate`, `embed`, `rerank`, `chunk`, `extract`, `rewrite`, or
`transcribe`, and operation names the endpoint that will actually
execute (for example `generate.batch`, `generate`, `rerank`, or
`embeddings`). The same authorization is used for discovery and execution. The
proxy validates that the operation belongs to the task, resolves only that
operation's route capability cohort, and surveys only healthy endpoint
incarnations in those possible pools under that authorization. It leases only
nodes that explicitly advertise the corresponding operation. Execution then
selects within that configured route and intersects the concrete pool with the
leased endpoint set. Bootstrap
inventory is eligible only before the first successful catalog refresh;
successfully discovered generic inventory is task-unknown and fails closed.
Thus admission uses limits that every routing candidate can honor rather than
assuming all inference nodes have the same model or backend. Work and result
envelopes retain item, source-fingerprint, and page identities across the HTTP
boundary, and the client validates cardinality and observed execution before
publication.

Admission has two owners in a distributed deployment:

- the enrichment node admits source bytes, renderer scratch space, retained
  encoded images, decoded pixels, staging, and an optional prefetch window;
- the inference node independently admits request bytes, media items, decoded
  model inputs, accelerator memory, and model concurrency.

The client-side reservation is released only after the remote invocation no
longer borrows the page buffers. Cancellation and deadlines cover capability
lookup, single-flight waits, and inference transport; retry or failover must
reuse stable work identities so durable publication stays idempotent.
The physical attachment representation is selected by that concrete route:
linked execution charges borrowed binary bytes, ordinary HTTP media fields
charge base64 payload bytes, and reader URL adapters charge the complete data
URI including its MIME prefix. A remote catalog always publishes
`borrowed_attachments=false`, even when its upstream inference process also
has a linked ABI, because the proxy's outward route cannot borrow caller
memory.

Batch formation is currently per resolved endpoint and model invocation. A
load balancer may route one complete bounded request to an eligible inference
node, but the coordinator does not split one native model batch across several
nodes and then describe it as one native batch. Future cross-node fan-out, if
needed for throughput, belongs above the executor: partition into independent
bounded requests, preserve per-item identity and failure, and aggregate their
observed execution reports without assuming completion order.

The Go inference proxy exposes `GET /ai/v1/models` and the reader, generator,
embedding, reranking (including multimodal), chunking, extraction, rewriting,
and transcription surfaces. Classification is selected by an extraction
schema rather than a parallel route. The proxy routes a
homogeneous bounded batch intact by its nested model identity and rejects
mixed-model batches before forwarding. Refreshed endpoint inventory retains
the advertised task for each model; selection and failover filter by both model
and operation, while the pool-only compatibility fallback is allowed only
before that endpoint's first successful catalog refresh. The cluster catalog
is assembled from healthy inference nodes and merges duplicate descriptors conservatively:
accepted inputs and boolean support are intersected, numeric ceilings use the
minimum, and native batching is advertised only when every eligible duplicate
supports it. A failed or authorization-ineligible node is omitted from the
immutable lease, making a partial successful merge safe because execution cannot
expand back to omitted nodes. Discovery fails only when no safe candidate
remains. Upstream authorization is forwarded for both discovery and execution.

The long-term proxy contract is model-aware rather than a transparent
round-robin surface:

- expose a cluster capability catalog for every routable
  model/task/concrete-operation scope;
- advertise the conservative intersection of eligible nodes' limits, or return
  a resolution/affinity token that binds discovery and execution to one node;
- route each bounded native batch intact to one capable node;
- preserve cancellation, item identities, per-item failures, response
  cardinality, and observed execution reports; and
- retry or fail over only at an independent request boundary, without merging
  several node executions into one claimed native batch.

### Generic bounded scheduler

The scheduler groups work only when task, resolved model identity, backend,
prompt/schema, transformation parameters, and output cardinality are
compatible. Each window is bounded simultaneously by:

- item count and serialized bytes;
- retained encoded bytes and decoded pixels;
- PDF decoder and renderer scratch memory;
- renderer worker count and thread-safety mode;
- model admission and provider request limits; and
- cancellation and deadline.

One render window is the baseline. Durable visual embedding can keep exactly
one speculative render window ahead. The active window retains its composite
lease; the next window must acquire a second composite lease against that live
usage before any rendering begins. If the combined peak does not fit, the
speculative attempt stops and is retried synchronously after the active lease
is released. Estimates guide scheduling; hard bounded allocators, decoder
limits, and model admission remain the enforcement boundary for memory visible
to Antfly allocators. Framework-private renderer memory is handled separately
under the compatibility-backend contract below.

The implemented preparation work is currently task-specific: generation keeps
per-model contracts and per-item prompt estimates, while reranking and
classification-backed extraction own typed encoded inputs through admission and
execution. A future convergence step may place a process-level microbatch
broker behind the document coordinator. That broker could coalesce compatible
tail windows from several documents, but would have to enforce fairness,
deadlines, cancellation, and independent byte/pixel ceilings. The following
`PreparedTaskInput` is an illustrative future unification, not a current public
type:

```zig
const PreparedTaskInput = union(Task) {
    read: PreparedReadRows,
    generate: PreparedGenerationSequences,
    embed: PreparedEmbeddingRows,
    rerank: PreparedQueryCandidateRows,
    extract: PreparedSchemaRows,
    // Remaining task families retain equally explicit layouts.
};
```

Current prepared values own or borrow the encoded rows, masks, exact lengths,
and execution permits needed by their concrete task. Preflight, usage
accounting, and execution consume that same value. The future common shape must
also carry media references, row-to-item mapping, provenance, and admission
charges. Executor stage support remains explicit—transport grouping, fetch,
render, decode/preprocess, fused encoder rows, vision prefill, and decoder
scheduling—because accepting an outer array does not prove that every stage
executes natively as a batch.

Resource admission is correspondingly multi-axis. Item, candidate, label,
schema, page/chunk, token, encoded-byte, decoded-pixel, result-byte, and
accelerator-work dimensions remain distinct. Existing adapters preserve
input-indexed results with task-specific mappings. A future generic
`BatchLayout` can make that mapping reusable for fused model rows or generation
sequences; it must never infer identity from completion order or erase the
different result semantics of readers, generators, embedders, rerankers,
extractors/classifiers, chunkers, rewriters, and transcribers.

### Generic attachment transport ABI

Reader, generator, embedder, extractor, multimodal reranker, chunker, and
transcriber invocations use one versioned borrowed attachment representation.
Text-only classifiers, rewriters, and rerankers need no binary attachment, but
can adopt the same representation if a future executor accepts media without
changing document preparation:

```zig
const Attachment = extern struct {
    bytes: String,
    mime_type: String,
};

const AttachmentRef = struct {
    attachment_index: usize,
    item_index: usize,
    source_fingerprint: ?[]const u8,
    page_number: ?u32,
};
```

Operation JSON contains attachment references; bytes are borrowed for the
duration of the synchronous invocation. The host validates ABI version,
pointer/count consistency, indexes, MIME types, byte limits, and per-item
cardinality before borrowing memory. Unsupported remote transports perform
base64 or multipart adaptation only at their final provider boundary.

Distributed Antfly-to-Antfly read, generation, embedding, extraction,
multimodal-reranking, chunking, and transcription traffic now negotiates a
versioned binary envelope containing bounded JSON metadata, attachment
descriptors, MIME essences, and raw attachment bytes. The receiver validates reserved fields,
declared lengths, aggregate limits, trailing data, attachment indexes, MIME
agreement, and exact reference cardinality before preprocessing.
`framed_attachments` is an additive v4 capability bit: older clients ignore it,
newer clients default it to false for older nodes, and the leased concrete route
remains the sole transport authority. External providers retain their admitted
JSON/data-URI or multipart representation. The envelope codec is task-neutral,
but each endpoint advertises it only after its parser and admission path consume
the raw attachments; model capability alone never implies wire support. The
current request-bounded v1 envelope is deliberately synchronous. A later
streaming variant may add per-frame checksums and flow control without changing
the logical borrowed-attachment contract.

Per-item identity removes the current need to split otherwise compatible local
batches at document boundaries solely for profiling. Cross-document batching
is then permitted without losing provenance.

### Typed per-item results and honest execution reports

All executors return an indexed envelope while retaining task-specific values:

```zig
const WorkItemResult = struct {
    item_id: []const u8,
    source_fingerprint: ?[]const u8,
    page_number: ?u32,
    value: union(Task) {
        read: ReadResult,
        generate: GenerateResult,
        embed: []const f32,
    },
    failure: ?ItemFailure,
};

const ExecutionReport = struct {
    requested_items: usize,
    native_batches: usize,
    native_items: usize,
    serial_items: usize,
    rejected_items: usize,
    fallback_items: usize,
    fallback_reason: ?[]const u8,
};
```

Envelope failures fail the invocation. Deterministic item failures remain
attached to the failed item so valid siblings continue. Telemetry distinguishes
requested batching, native execution, serial compatibility, and fallback.

### PDF embedding semantics

"Embed this PDF" is not one implicit operation. Configuration selects one or
more durable artifacts:

- one vector per extracted-text chunk;
- one visual vector per rendered page;
- both text and visual vectors in named embedding spaces; or
- an explicit document-level reduction with a named, versioned reducer.

Page vectors retain page identity. Text and visual spaces are never silently
mixed, and page vectors are never implicitly pooled into one document vector.
This preserves incremental updates, selective reprocessing, and explainable
retrieval.

Page-image embedding identity is versioned (`antfly-pdf-page-embedding-v4`).
It hashes the source digest and page, render DPI and resolution policy,
per-page pixel/dimension caps, renderer in-flight pixel/byte ceilings, PDF
decoder stream and working-set limits, model pixel cap, output-byte caps,
transport (borrowed raster or encoded), expected dimensions, and the resolved
image transform (target geometry, resize mode, resampling). Changing any of
these invalidates staged and published vectors; older identities recompute
through the ordinary durable update path.

> **Relocated:** The per-review defect ledger ("Review findings and required
> fixes", 184 items) and the "Migration sequence and implementation status"
> ledger → [docs/design/pdf/history/review-and-migration-ledger-2026.md](../docs/design/pdf/history/review-and-migration-ledger-2026.md).
> Their standing limits and defaults are in [Limits and defaults](#limits-and-defaults).

## Post-review implementation contract

The implementation follows these long-term rules:

- A capability lease (descriptor, routing token, and descriptor revision) is
  resolved atomically once per runtime/model/task/operation/auth scope and reused by
  planning and execution. The current implementation uses a bounded
  30-second fresh cache, five-minute stale-if-error interval, and single-flight
  refresh. A valid legacy catalog without an exact contract falls back to
  conservative compatibility execution; it never upgrades an unknown model to
  native batching. Discovery and single-flight waits are deadline-bounded and
  cancelable; runtime shutdown drains owners.
  Only retryable transport, overload, and 5xx catalog failures may use a
  still-bounded stale capability lease. Capability-stale conflicts retry once
  within the owning single-flight; authoritative 4xx responses, invalid
  contracts, expired contexts, and canceled callers never publish stale data.
  An upstream catalog 404, 405, or 501 is a negatively cached
  `discovery unsupported` result, not an inference outage: explicitly
  configured legacy endpoints continue with conservative singleton/serial
  execution. The discovering owner still keeps its own cancellation or
  deadline outcome; publishing the legacy cache entry for other callers never
  turns a canceled operation into fallback work
  ([history](../docs/design/pdf/history/review-and-migration-ledger-2026.md)).
- Capability caches and concurrent I/O executors are runtime-owned services,
  not document-owned implementation details. Read and write enrichment borrow
  them for their invocation lifetimes; shutdown destroys them only after their
  dependent DB/runtime owners have drained. Stateless APIs may construct a
  bounded fallback, but durable PDF and semantic-chunk pipelines do not.
- Distributed execution re-resolves endpoint eligibility from a current
  per-endpoint model/task/operation catalog on every initial attempt and retry, then
  intersects it with the exact endpoint identities in the lease. Weighted
  destination selection and route conditions see only leased endpoint
  incarnations, so a partially unavailable catalog fan-out cannot select an
  uncovered pool while a covered destination remains. A cached
  planner snapshot may make a request temporarily too ambitious after topology
  change, but the proxy neither expands the allowed set to a newly joined node
  nor sends work to a node whose operation is bootstrap-unknown; the concrete
  node remains the final limit validator. Route matching is also generation
  fenced: policy replacement invalidates the lease rather than applying the new
  route to an endpoint cohort planned under the old policy. Informer resyncs and
  equivalent route objects are explicitly generation-neutral, and retry-time
  stale decisions use the same invalidation response as initial execution.
- Discovery and execution form one capability lease. A scoped catalog response
  carries an opaque token and revision bound to model, semantic task, concrete
  operation, route-policy generation, effective authorization, exact descriptor
  bytes, and the exact eligible endpoint incarnations. Both phases construct
  the same normalized routing context;
  source identity is supplied by an authenticated resolver, never copied from
  caller identity headers. The endpoint set is a structured route capability
  cohort, not a cluster-wide union: a definite terminal reject cannot fall
  through, while an unresolved conditional route includes every reachable
  route and fallback pool. Configured route policy precedes an explicit pool,
  which precedes the process default. Execution then intersects the selected
  pool and concrete operation with the leased endpoints. Equivalent immutable
  snapshots renew and reuse their token rather than consuming another bounded
  table slot. Issuance purges leases from obsolete route generations and dead
  endpoint incarnations before its capacity check, and refuses to publish a
  token if discovery raced a policy or endpoint replacement. Validation also
  removes a lease immediately after detecting either lifecycle mismatch.
  Antfly clients attach both values to read,
  generation, embedding, reranking, chunking, extraction, rewriting, and
  transcription transports; classification uses the extraction task and lease.
  An expired lease, replaced endpoint, or mismatched
  task, authorization, or revision returns an explicit stale-plan 409,
  invalidates the client snapshot, and requires rediscovery and replanning
  before retry. Legacy clients may omit the token, but never receive the
  planner/executor consistency guarantee. A lease-aware client never silently
  downgrades when its discovered snapshot is missing or evicted.
- Scale-to-zero is a preparation step of scoped discovery, not an escape hatch
  in scoped execution. Namespace-qualified reachable pools are renewed before
  catalog fan-out; cold pools must register within their individual activation
  deadlines, and the route generation must remain stable across that wait.
  Only those discovered endpoint incarnations enter the lease. Execution can
  refresh a represented pool's activation lease, while an absent pool requires
  rediscovery. This preserves exact capability fencing on distributed
  inference nodes without disabling request-driven cold starts.
- Every executor owns final admission. Remote read and generation calls and
  multimodal embedding calls are split at both model item and encoded-byte
  ceilings. Image-bearing readers, generators, embedders, and extractors also
  split on aggregate decoded pixels measured from physical headers. A single
  item larger than the model ceiling fails before transport.
- Renderer admission and model admission are distinct dimensions. In-flight
  render pixels bound one concurrent worker wave; cumulative preflighted pixels
  bound every retained page image passed to one model invocation. Neither limit
  may stand in for the other. A numeric renderer limit is valid only while the
  document operation owns the corresponding process-wide reservation. Worker
  allocators stay private and thread-safe inside that owned grant.
- Distributed catalog admission converts retained bytes into fan-out
  concurrency: one fixed merge reservation plus one working-set reservation per
  active endpoint request. Low-memory configurations reduce parallelism and
  reuse workers across the remaining nodes rather than rejecting a fan-out that
  can complete sequentially.
- Render planners preserve a singleton quality invariant. The retained-output
  allowance for an item in a batch is never smaller than its admitted singleton
  allowance. If that invariant does not fit, the planner reduces the window;
  it does not spend page resolution to preserve batch cardinality.
- Admission receives a route-owned `InvocationMemoryPlan`, containing the
  selected attachment representation, allocator owner, complete host-boundary
  peak, and independent per-item and aggregate result limits. Media-capable
  producer and part-item embedder implementations fail
  closed when this plan is absent rather than inferring it from model
  capabilities. Linked callbacks charge borrowed bytes, base64 transports
  charge exact expansion, and data-URI adapters charge the complete URI plus
  downstream serialization copy. Caller-owned adapters run under the plan's
  hard allocator ceiling. The same boundary allocator remains active when a
  concrete inference node owns decoder/model admission; only allocations kept
  on the node's private admitted allocator are excluded. This avoids both an
  incomplete model cap and unbounded host preparation. Every public producer
  invocation from a contract-publishing runtime and every part-item embedding
  invocation applies those ceilings even when invoked outside the PDF
  scheduler. Structured JSON parts, inline data URIs, URL-only inputs,
  transcription sources, and non-model producers do not bypass this boundary.
  Legacy callbacks cannot execute any model-backed producer family without a
  plan. Provider
  wire ceilings and process resident ceilings are distinct: render planning
  reserves retained raw media, one concrete transport body, and the complete
  route-specific fixed peak for envelopes, responses, parsing, typed results,
  and bounded control storage against the same operation grant.
- Route resolution and input normalization are themselves admitted work. They
  run under small request-derived preprocessing ceilings before model/provider
  execution begins; an implementation cannot allocate an unbounded parse tree
  in order to discover what its later memory limit would have been. Dense part
  planning carries the complete heterogeneous item shape rather than a common
  MIME guess, and UTF-8 repair uses exact allocation while accounting for its
  structural arrays and final repaired values.
- Inline media has one RFC 2397 authority. Scheme and encoding markers are
  case-insensitive, payload validation and decoded sizing precede allocation,
  MIME policy compares normalized essences, and materialization consumes the
  same parse contract used by admission.
- MIME admission has one complete parser. Parameters are preserved on the wire;
  capability and modality checks compare the validated essence, while redundant
  declarations must also agree on every parameter they explicitly name.
- A result policy names logical retained bytes. Remote transport planning maps
  that policy to a conservative encoded JSON response ceiling and lowers the
  logical allowance when an outer client or provider limit is tighter. Local
  and remote result validation therefore enforce achievable limits in the same
  unit. Both the per-item ceiling and aggregate batch ceiling are enforced;
  dense vectors additionally require exact dimensions and finite values.
- Every logical result carries the request identity. Remote indexed responses
  are reordered and validated at the transport boundary, then enriched with
  the original identity. Enrichment rejects any remaining identity mismatch.
- Durable publication is generation-shaped. Private typed stage records are
  invisible; complete current-page promotion, stale-page artifact deletion,
  dense artifact counters, and vector replay append are one atomic
  document-store commit. The replay record is the public generation boundary,
  so no promoted artifact set can exist without its durable vector replacement.
  Stage payloads are borrowed from the write transaction during promotion; the
  commit does not retain a second document-sized heap copy. During an active
  split, the exact handoff delta is encoded directly from those borrowed values
  and committed by the same transaction; only the durable encoded delta buffer
  is materialized, after exact byte admission against the shard-transition
  working-set budget.
  Coverage counters remain idempotent post-append metadata: failure prevents
  the enrichment source watermark from advancing, so retry reconciles them
  from the already durable generation.
  A future storage backend that cannot provide the artifact transaction must
  instead publish through an atomic active-generation manifest pointer. The
  current vector index is not generation-filtered, so request atomicity is
  protected by a hard document-page admission ceiling until that migration is
  complete.

## Durable visual embedding configuration

A dense index opts into page-image semantics explicitly; existing text and
chunk configurations do not change behavior:

```json
{
  "generator": {
    "kind": "dense_embedding",
    "source_field": "document_url",
    "artifact_name": "pdf_pages_v1",
    "embedding_name": "pdf_visual_v1",
    "input": "pdf_page_images"
  },
  "execution": {
    "embedding": {
      "batch_items": 8,
      "batch_bytes": 67108864
    }
  }
}
```

`artifact_name` is the stable page-unit namespace. Vector keys derive from
`(document, artifact_name, page:NNNNNN, embedding_name)`. Resolved model
capabilities can only reduce the configured item, byte, and pixel windows;
document data and index configuration cannot raise them.

Renderer scratch and retained media/provider memory are separate subcredits of
one atomic, window-scoped composite lease. The output subcredit backs a bounded
allocator that stays alive through the consumer invocation; the scratch
subcredit bounds the render wave. The lease is released at the end of each
window, creating a fair admission opportunity between documents. This avoids
both depending on allocator failure and pinning a maximum-document reservation
while inference or persistence is idle.

The document-scoped streaming microbatcher, for every consumer:

```text
parse once; inspect embedded text and select pages that need OCR
      |
render the next admitted page window (bounded CPU parallelism)
      |
invoke each compatible consumer's model batch on the borrowed window
      |
merge by page number, stage/persist, release the window --> repeat
```

## Goals and non-goals

Goals: amortize PDF parsing and resource discovery across a document; fill
native model batches rather than invoking per page; render in parallel without
sharing mutable reader state; bound compressed bytes, decoded pixels, renderer
scratch, request bytes, model memory, and reorder buffers; preserve request
order, page identity, embedded-text quality comparison, durable retry, and
per-page failure isolation; apply backpressure so rendering cannot run
arbitrarily far ahead of inference or persistence; keep unsupported families
and remote providers correct through observable serial fallback.

Non-goals: making every model natively batched; sharing mutable renderer
caches between threads; changing page or chunk artifact keys.

## Current implementation

- Document extraction marks PDF pages with missing or low-quality embedded text
  `pending_ocr`; born-digital pages that pass quality checks never enter the
  render scheduler.
- One heap-owned PDF OCR coordinator lives for the whole document operation.
  Its `PdfRenderSession` discovers the page tree once and refreshes an
  immutable `RenderForkTemplate`; each render worker instantiates a private
  reader from it and never mutates the source reader.
- `renderParsedPagesBatchAlloc` takes an explicit page window, preserves
  request order and page identity, isolates deterministic page failures, and
  enforces batch-page, parallelism, pixel, in-flight byte, retained-output, and
  per-worker allocator limits.
- Inspection is charged through its own bounded allocator. Each window acquires
  one atomic composite lease: scratch may be partially granted, while the
  transport and provider output peak is required in full and transferred to a
  window-local `BudgetedAllocator`. Worker allowance is recomputed before every
  window, and a tighter pixel cap lowers DPI rather than rejecting a page.
- Production render workers belong to the lazy BackendRuntime PDF lane (fixed
  workers, capped retained arenas, bounded queue). Standalone callers keep a
  call-scoped compatibility executor. Retained output is written directly
  through a thread-safe window allocator shared with the provider invocation.
- The local reader producer flattens compatible page requests into one image
  list while retaining request boundaries. Borrowed pages carry item, source,
  and page identity, so compatible pages from different documents may share a
  native batch. Linked native readers and visual embedders that advertise the
  raster capability receive validated RGBA8 pages; other routes keep encoded
  transport and adapt at their final boundary.
- Native Florence chunks inputs by `ANTFLY_INFERENCE_READ_BATCH_SIZE` with a
  batched encoder and, where supported, a batched incremental KV decoder.
  Mixed-EOS decoding compacts supported KV caches to unfinished rows while
  preserving result identity.
- Generator-backed OCR uses independent page messages and generic OCR prompt
  semantics, not `LoadedReader`. Multimodal generation reports
  `serial_compatibility` while projector/session state is serialized.
- Multimodal embedding returns one vector per page image or chunk and never
  silently pools a document.
- Page render latency is measured inside each worker; window scheduling and
  later OCR work are not counted as render time. Results return in input
  order; malformed native batch responses fall back to isolated serial work.
- PDF OCR and durable page-image embedding prefetch at most one second window
  under its own composite lease; otherwise the current window completes and
  releases first. Every other executor keeps acquire-render-infer-release.

## Required invariants

The implementation must preserve the following invariants:

1. Page identity is explicit. Results are joined by `page_number`, never by
   completion order alone.
2. At most one admitted render window is retained per document unless an
   explicitly admitted prefetch window is enabled.
3. Every concurrent renderer owns all mutable state it touches.
4. The parsed document outlives every page render task and is destroyed only
   after all tasks have joined.
5. A page cannot be persisted as successfully OCRed until its output has passed
   OCR validation and embedded-text quality comparison.
6. A permanent page failure does not fail valid sibling pages.
7. A systemic failure such as shutdown, allocator failure, or unavailable
   capacity cancels and joins the whole render group.
8. Item and byte limits are supplemented by aggregate pixel and working-set
   limits.
9. Provider fallbacks are observable; a batch must never silently become
   serial work.
10. Admission estimates improve scheduling, while hard allocators and decoder
    limits enforce safety when estimates are low.
11. Native decoder reservation must leave explicit capacity for every
    allocator-backed owner charged to the same resource slice.
12. Reported peak render concurrency is measured from workers actually
    executing, not from the planned wave width.

## Document preparation pipeline

### 1. Inspect the document and select OCR candidates

Embedded text extraction produces stable page units. OCR candidates are page
metadata (`unit_index`, `page_number`, embedded `OcrQuality`, `force_ocr`)
chosen by `ocr_mode` and quality, not rendered bytes. Sparse candidate lists
(for example pages 1, 5, and 19) keep their original unit positions.

### 2. Parse once; separate immutable from mutable render state

`PdfRenderSession` is not thread-safe. Parallel rendering gives every worker a
private `Reader.forkForRendering` snapshot with its own allocator and caches.
Object tables, page-tree structure, source bytes, and immutable resource
descriptions may be shared; decoded streams, graphics stacks, font mutation,
image/transparency buffers, and encoder state are task-local. A lazy cache
must be frozen before workers start, moved into the task-local context, or
protected by narrowly scoped synchronization whose contention is documented;
a mutex around a whole session is serialization and must be reported as such.
Preparation is fused: one parse serves embedded-text analysis and rendering
within a pending document group. There is no opaque cross-ABI session handle.

### 3. Bounded multi-page render boundary

`zig/lib/pdf` exposes an in-process batch operation (`PageRenderRequest`,
`PageRenderBatchOptions` in `zig/lib/pdf/src/mod.zig`). Library defaults are
150 DPI, 40,000,000 pixels and 4096 px per page; 8 batch pages, 1 parallel
page, 50,000,000 in-flight pixels, 512 MiB in-flight bytes, 64 MiB retained
PNG, 256 MiB retained raster, and a 12-byte-per-pixel reservation. Encoded and
raster variants return request-ordered results, each with its page number and
exactly one payload or failure. Inside one call the provider validates limits
and page numbers, reuses the parsed reader, renders admitted pages in private
contexts, writes retained payloads directly into a thread-safe caller allocator
when a fixed executor is supplied (compatibility executors copy on the caller
thread), cancels and joins workers on systemic failure, and destroys the parse
only after the last worker joins. Adaptive encoded-size retries happen inside
the worker before retained-output admission. Single-page functions remain.

### 4. Controlled parallel rendering

Two levels of admission apply: global resource-manager byte admission plus
bounded enrichment lanes keep concurrent PDFs from multiplying memory, and the
BackendRuntime PDF lane supplies a small fixed worker set with a per-document
worker cap. PDF and model modules own no process-global pools.

Each window owns one split reservation covering the physical peak implied by
its logical media cap, resolved attachment transport, bounded invocation
allocator, and native scratch. The native side may be partially granted but
must still render one valid page; the output side is required. Page weight is
estimated from post-clamp dimensions:

```text
RGBA output + resampling scratch + drawing/compositing scratch
+ bounded stream-decode reservation + encoded PNG estimate + retained request bytes
```

Estimates are conservative; the hard `BudgetedAllocator` and decoder
stream/working-set limits enforce allocator-visible memory, so an underestimate
becomes an identified resource failure. CoreGraphics/CoreFoundation memory is
outside that allocator: compatibility pages are serialized per document, their
caller-owned buffers stay allocator- and pixel-bounded, and the native
reservation carries a conservative framework allowance that release
qualification checks with peak RSS.

Effective per-document concurrency is
`min(requested, operator cap, memory-derived, pages remaining in the window)`,
defaulting to one. Pages render in document order; page-number-keyed results
make completion order irrelevant to correctness.

### 5. Fill one bounded render/inference window

Rendering never runs across the whole document ahead of inference. A window is
bounded by the item cap, the serialized/encoded byte cap, render-window pixels,
and the working-set estimate. The loop renders the next window, submits its
successful pages as one batch, applies page results and render failures by page
number, then advances. The tail may be smaller. A page whose request alone
exceeds the operation cap is a permanent request-size failure that does not
block later pages. The document default of eight matches the native Florence
default; byte and pixel caps shrink windows for large pages.

### 6. Binary local media handoff

Rendered pages reach local readers, generators, and embedders as borrowed
binary attachments on the asset-producer request, not base64 JSON or data URIs.
Remote providers serialize at their final transport boundary only. Linked
native executors that advertise the raster capability receive RGBA8 directly
(see [Optional decoded-image handoff](#optional-decoded-image-handoff)).

### 7. Execute the native batch

Compatible local reader requests are flattened and submitted to
`LoadedReader.readBatch`, retaining request-to-image ranges. Florence chunks by
its effective cap, batches the encoder, and uses the batched incremental KV
path on CUDA and Metal where supported. Rows that reach EOS stop accumulating
text. Unsupported shapes fall back to the full batched decoder, then to the
serial reader only for documented compatibility errors. The result count must
equal the submitted image count. The document cap and Florence cap are exposed
together in profiling, since a document batch larger than the Florence cap
creates a hidden chunk boundary.

### 8. Merge and persist page results

Render and OCR results are keyed by page number and mapped back to stable unit
indexes; failures carry stage (render, preprocess, inference, validation),
retryability, and identity. OCR must not erase substantial embedded text with
a trivial or worse response. Selection checks candidate availability before
comparing quality: empty embedded text cannot beat meaningful OCR, while empty,
punctuation-only, and prompt-echo OCR stay rejected and a short transcription
cannot replace substantial embedded content. Selected OCR that still fails
quality checks carries the `ocr_selected_low_quality` warning. Selection,
execution success, and transcription quality are separate outcomes. Method,
status, confidence, regions, render metadata, and failure provenance are
updated before final unit and chunk artifacts are generated, and rendered
buffers are released as soon as their result is applied.

## Backpressure and controlled overlap

The baseline renders one window, consumes it, releases it, and then renders the
next. This remains the fallback whenever no concurrent runtime is attached,
overlap is disabled, or a second reservation does not fit.

The double-buffered mode overlaps CPU rendering of window N+1 with model
inference for window N:

```text
CPU render:  [ window N ] [ window N+1 ] [ window N+2 ]
model:                    [ window N   ] [ window N+1 ]
```

PDF OCR and durable visual embedding enable this with `prefetch_batches = 1`.
The active window and prefetched window each own a composite scratch/output
reservation; the second reservation is acquired while the first remains live,
which admits their combined peak before rendering. The shared parsed session
has only one render-window caller at a time: the foreground consumes already
rendered buffers while the worker prepares the next window. Values greater than
one are hard-clamped because they recreate whole-document buffering without a
useful pipeline stage. `prefetch_batches = 0` restores strictly sequential
execution.

If the inference queue is saturated, rendering stops at the admitted window. A
render scheduler with no permits never reserves an inference slot while
waiting. Resources are acquired in one global order and released in reverse
where practical:

1. Document extraction working-set lease.
2. Render-window lease.
3. Inference queue units, only after the window is ready.

Inference units are never held while waiting for render workers.

Hard decode-limit admission and scheduling concurrency are governed
separately: raster/fork estimates and measured per-document worker peaks
choose how much work to run concurrently, but every allocation still passes
through the same admitted, shared physical-memory cap regardless of that
estimate. When concurrency is underestimated, retry proceeds serially on only
the failed identities rather than assuming additional grant is available;
successful page buffers keep their existing owners.

A speculative window's failures from scratch pressure carry bounded retry
metadata rather than failing outright: once the prior window's consumers
finish and release its grant, the coordinator acquires fresh scratch
admission and retries only the pages that failed, once. The same mechanism
serves OCR/generation and visual embedding, preserves page geometry,
deadlines/cancellation, and output credit, retains no invocation-local
callback pointers, and does not retry a deterministic decode-limit violation
or a foreground failure.

## Configuration and operator caps

The public `execution` policy carries `batch_items`, `batch_bytes`, and
`max_document_pages`; per-index embedding policy lives under
`execution.embedding`. OCR render geometry is part of the document-extraction
`ocr` config (`render_dpi`, `render_resolution`, `max_rendered_pixels`,
`max_rendered_dimension`). Render concurrency, in-flight pixels and bytes,
prefetch, and shared-window width are operator-only environment settings; they
are not part of the public API. Every value and its clamp is listed under
[Limits and defaults](#limits-and-defaults).

```yaml
execution:
  batch_items: 8
  batch_bytes: 67108864
  max_document_pages: 2048
```

Operators raise render parallelism above one only after assigning an aggregate
byte budget that admits it. Prefetch falls back to sequential work when the
second composite lease is unavailable. Profile output reports requested and
effective values, so operators can tell when admission or a hard cap reduced
concurrency or batch size.

## Failure semantics

Failures fall into three categories.

### Per-page permanent failures

Examples include an unsupported page resource, a malformed page content
stream, a page exceeding a hard dimension limit, trivial OCR output, or prompt
echo. Record the page failure and continue with valid siblings.

Reader/generator OCR results may preserve successful siblings because their
page text is independently attributable. Durable page-image embedding has a
stronger publication rule: one failed or missing vector fails that document
attempt before coverage or stale cleanup. Successful sibling vectors remain
private staging records and are discarded at retry start. This prevents a
short or partially failed provider response from silently shrinking the
searchable document.

### Batch/provider failures

Examples include a response-count mismatch or a backend operator unsupported
for the batch shape. For a permanent batch-compatibility failure, retry only
that microbatch serially and record the fallback reason. Do not restart already
completed windows.

If one item poisons an otherwise valid provider batch, preprocessing should
identify it before model execution where possible. Otherwise, serial isolation
is limited to the failed microbatch.

### Systemic retryable failures

Examples include inference capacity, model availability, transient transport,
shutdown, and global resource pressure. These yield to the durable enrichment
worker according to the existing retry budget. Do not convert a capacity
failure into permanent page coverage merely because it occurred during a
batch.

All outstanding render tasks must be cancelled and joined before returning a
systemic error. No callback may access the parsed document after the provider
returns. Each render wave composes the caller's deadline with a wave-local
atomic stop flag. A worker that observes cancellation or a systemic allocator
failure stops sibling work cooperatively; the coordinator still joins every
spawned thread before propagating the error. Peak concurrency telemetry counts
workers between their actual enter/leave transitions, including inline
thread-spawn fallbacks.

## Observability

The opt-in `ANTFLY_INFERENCE_READ_PROFILE` stream now reports per-page render
events, render-window page ranges, requested and peak concurrency, peak
admitted pixels and bytes, thread-spawn fallbacks, OCR batch mode, fallback
reason, request bytes, source fingerprint, and elapsed time. Long-lived status
counters can be added if these events prove useful operationally. The full
desired inventory is:

- PDFs parsed for text and PDFs parsed for rendering.
- Render-session reuse count and pages per session.
- Requested and effective render concurrency.
- Active render workers, pixels, estimated bytes, and actual peak bytes.
- Render windows started/completed and pages per window.
- Page render latency and window fill latency.
- OCR batches started/completed, pages, encoded bytes, and rendered pixels.
- Native Florence batch size and internal chunk count.
- Batch, full-decoder fallback, and serial fallback counts with reasons.
- Per-stage page failures and retryability.
- Time waiting for render permits and inference queue units.
- GPU OCR utilization relative to CPU render time.

Existing source fingerprints are included in profile logs without logging
document content or rendered image bytes. Borrowed attachments carry item,
source, and page identity independently, so compatible local work may batch
across documents while every inner chunk and serial fallback retains exact
provenance.

## Test plan

Hermetic coverage, by area:

- **Functional batching:** eight pages produce one window and one Florence
  batch; ten pages produce eight and two; mixed and sparse PDFs batch only
  selected pages and map back to the right units; batch and serial output match
  byte for byte on deterministic fixtures, including mixed EOS lengths.
- **Rendering and thread safety:** one render parse per document operation;
  concurrency one matches the legacy path and higher concurrency produces
  identical images under stress; a wave-start rendezvous makes launched-worker
  counts deterministic; cancellation at every stage, including before all
  workers arrive at the gate, joins and frees; allocation-failure injection at
  every ownership transfer is leak-free.
- **Admission and memory:** item, byte, pixel, and working-set thresholds each
  split windows at the right boundary; an oversized page fails alone; owned
  output credit transfers atomically to the invocation allocator; partial
  native grants still admit a minimum raster; a tight pixel cap lowers DPI;
  prefetch never retains more than the configured windows; queue saturation
  applies backpressure; acquisition order cannot deadlock.
- **Failure isolation and replay:** render or trivial-OCR failures do not
  poison siblings; a malformed native response falls back only for its window;
  transient inference failure persists no false terminal coverage; retry
  neither duplicates nor skips artifacts; a crash between render and persist
  rerenders safely; embedded text survives an inferior OCR response; retry
  startup keeps the published vector while clearing private partial output.
- **Boundary compatibility:** single-page render functions remain; invalid
  limits and page numbers fail explicitly; batch buffers are destroyed exactly
  once; the standalone ABI version gates borrowed binary segments; the
  encoded-image codec round-trips ordered payloads and rejects count mismatches.

Named gates:

- `zig build pdf-ocr-integration-test` runs in production mode
  (`builtin.is_test == false`), renders both pages of a real fixture through
  one coordinator, carries both PNGs through the asset-producer path to one
  ordered two-image reader callback, asserts two launched workers, and checks
  the observed active-renderer range of one through two. It is a dependency of
  `lib-pdf-test` and `antfly-unit-test`, so CI runs it.
- `zig build linked-inference-abi-integration-test` links the generated
  production inference archive, resolves only
  `antfly_standalone_inference_get_function_table`, and checks function-table
  prefix validation, version rejection, `Status` mapping, and borrowed binary
  MIME rejection without importing `pkg/inference/src/host/host.zig`.
- `zig build pdf-model-qualification-test` is the non-hermetic release gate. It
  sends the two-page fixture through real remote reader, generator, and
  embedder contracts and requires two non-empty reader results, two non-empty
  generator results, and two vectors of the configured dimension. It fails when
  its environment is absent:

```sh
ANTFLY_PDF_QUALIFICATION_URL=http://127.0.0.1:8082/ai/v1 \
ANTFLY_PDF_QUALIFICATION_READER_MODEL=florence2 \
ANTFLY_PDF_QUALIFICATION_GENERATOR_MODEL=gemma4 \
ANTFLY_PDF_QUALIFICATION_EMBEDDER_MODEL=clipclap \
ANTFLY_PDF_QUALIFICATION_EMBED_DIMS=768 \
zig build pdf-model-qualification-test
```

The layered qualification workflow (model, execution, and document layers) is
in [`scripts/bench/pdf/QUALIFICATION.md`](../scripts/bench/pdf/QUALIFICATION.md).
Fake providers qualify contracts, not native fusion; missing or skipped
evidence, schema mismatch, or source/artifact drift cannot produce a passing
plan. Real-model qualification must compare OCR content against the source
page: non-empty artifacts and searchable-vector counts do not establish
transcription accuracy.

## Performance architecture after review

The durable optimization boundary is the resolved executor, not a Florence-
specific PDF loop. Every task follows the same four-stage ownership model:

1. prepare task-neutral document units under source admission;
2. form capability-compatible, resource-bounded windows;
3. preprocess independent CPU/media inputs outside the model lock; and
4. acquire the resolved executor once for the fused model operation.

This applies to readers, generators, embedders, rerankers, extractors,
classifiers through extraction, rewriters, chunkers, and transcribers. A family
that has only singleton execution may use a bounded ordered compatibility
window, but it must continue to advertise `serial_compatibility`. Only a real
fused kernel/provider operation may advertise `native`.

Durable properties from the post-implementation performance review:

- Florence's greedy batch decoder uses CUDA's fused LM-head/argmax where
  available; Metal's per-row path is reported as `batch_projected_argmax`,
  never fused. Model sidecars and bias capabilities resolve once per model
  generation, not per microbatch.
- Visual preprocessing (decode, resize, normalize) runs before the model lock
  under a host-only admission lease; accelerator workspace is acquired only
  after the lock.
- Broker candidates do not hold singleton leases while waiting; the group
  leader admits the realized fused shape once. The broker is sharded by the
  complete resolved executor key.
- PDF vector publication is windowed independently of render/inference
  windows, so no document-sized apply transaction exists.
- Resolved image transforms feed render planning before allocation.

> **Relocated:** The complete performance-review change log and the dated OCR
> verification narrative →
> [docs/design/pdf/history/performance-and-ocr-verification-2026.md](../docs/design/pdf/history/performance-and-ocr-verification-2026.md).

### Resource and pool ownership

Long-lived pools are lazy children of the runtime that owns their physical
resource:

- `BackendRuntime` owns fixed CPU lanes, including PDF rendering and bounded
  preprocessing scratch;
- the inference `Node` owns model-generation-aware microbatch groups and model
  admission;
- a resolved provider entry owns reusable HTTP connections, pacing, and
  provider credentials; and
- `PreparedDocument` owns source bytes, immutable parse metadata, and
  document-affine transformation state only for the pending document group.

This prevents process-global document caches, avoids worker pools hidden in
individual task adapters, and gives shutdown one explicit join order. Pools
are created on first use and destroyed only after their admission gate has
closed and outstanding leases have drained.

### Remaining high-cost boundaries

Two further optimizations require explicit protocols rather than local
shortcuts:

- The shipped distributed attachment envelope is bounded, authenticated by the
  existing route lease, cancellation-aware at the request boundary, and exact
  in reference cardinality. V1 is produced as replayable borrowed segments;
  the proxy streams both one-attempt and retry-enabled first attempts, and the
  inference node decodes transport bodies incrementally into one admitted
  payload slab. Independently produced media and resumable partial retries
  still need a successor with checksums, frame-level flow control,
  cancellation, and provenance in the wire descriptors. A proxy must not infer
  either version from logical model modalities.
- Logical render-lane reader/font caches now remove repeated initialization
  across joined waves within a render batch even when fixed-executor arena
  scratch resets after each callback. Each reader uses a private, freeing lane
  heap backed by the shared byte-budget allocator, so page scratch reuses
  already-admitted pages without paying one system allocation per object. The
  reader is never accessed concurrently, so a logical lane may resume on
  another physical worker without retaining resettable executor memory.
  Reuse across independent render windows would
  still need a byte-bounded immutable resource cache owned by
  `PreparedDocument`, with forks borrowing only frozen entries. Mutable reader
  caches must never cross threads or escape into resettable backend-lane
  scratch; corpus benchmarks should justify the added immutable cache before it
  is introduced.

Similarly, Gemma4 multimodal generation stays `serial_compatibility` until its
resolved generator exposes a genuinely fused image-message batch. The generic
envelope and media ABI do not by themselves make serial projector/session calls
a native batch.

## Component implementation notes

Each component below has shipped; remaining release-environment work is under
[Open work](#open-work).

### Unified OCR batching policy

The document OCR default is eight. Document and Florence caps stay
independently operator-controlled and are profiled together. Item and byte caps
are supplemented by an aggregate rendered-pixel cap; the serial provider
fallback remains.

### Batch-render ABI with serial execution

Batch rendering is an in-process `zig/lib/pdf` boundary that parses once per
document operation; the single-page API remains. Model configuration
describes what executes; `InferenceExecutionContext` describes where (default
endpoint, shared capability cache and I/O ownership, trusted source routing).
Task adapters may not synthesize their own localhost, cache, or tenant-routing
policy.

### Immutable document and controlled concurrency

Parallelism defaults to one and is capped at eight. Immutable parsed state is
borrowed into wave-local forks on the fixed BackendRuntime lane; mutable page
state stays thread-confined and reusable scratch resets with a strict
retention cap after every job. The composite lease releases before the next
window is planned. The native allocator ceiling and the CoreGraphics allowance
are separate guarantees.

### Binary local media handoff

Borrowed binary segments cross reader, generator, and embedder standalone
runtime calls, with data-URI fallback only at unsupported provider boundaries.
Direct encoded media is admitted as already resident.

### Render/OCR pipeline overlap

Complete for PDF OCR and durable visual embedding, as described under
[Backpressure and controlled overlap](#backpressure-and-controlled-overlap).

### Optional decoded-image handoff

Complete for linked native Florence reads and linked visual embedders: bounded
direct-to-tensor preprocessing on the inference lane, direct RGBA consumption,
and baseline-JPEG component-plane-to-CHW writes for encoded CLIP input.
Resamplers that need scratch (including Pillow-compatible bicubic) allocate
from a synchronized wave budget that grows only between joined waves, reduces
worker width before rejecting input, and maps exhaustion to the preprocessing
byte limit. Encoded-image fallback remains everywhere else.

### PDF inspection and render preparation fusion

Complete within one pending document group: a credential-scoped source cache
owns the download and one prepared reader, lends immutable metadata to render
forks, and may be evicted under pressure. A bounded attempt spool and neutral
page-text records keep publication and later consumers from walking the PDF
again. The prepared handle is document-group-scoped, never process-global.

## Acceptance criteria

The implemented baseline satisfies these criteria:

- Compatible multi-page OCR uses the native Florence batch path by default,
  with an eight-item document default.
- A changed PDF is parsed and transformed once per extraction operation.
- Render concurrency is explicitly capped and never derived from CPU count;
  lazy lane shares cannot exceed the aggregate thread ceiling.
- Aggregate pixels and working bytes are admitted before concurrent work, and
  every worker has a hard live-allocation ceiling.
- Coordinator and planner allocations are admitted independently of render
  windows; source-cache eviction cannot invalidate an active lease.
- A second consumer reuses neutral page-text records under a different OCR
  policy without changing geometry or regions.
- Page order and artifact identity are stable across concurrency levels; a
  permanent page failure does not block siblings; systemic failure joins the
  wave and keeps durable retry semantics.
- Profile events identify batched, serialized, chunked, and fallback work.
- The production-mode integration target drives a real two-page PDF through
  reader, generator, and embedder contracts (see [Test plan](#test-plan)).

Remaining qualification (not architectural blockers): run
`pdf-model-qualification-test` on accelerators for the shipped Florence,
Gemma4, and ClipClap artifacts; run serial-versus-batch Florence parity against
production artifacts in CI; publish 1/4/8/16-page corpus RSS and pages-per-second
thresholds; promote actionable profile fields to status counters.

## Shared model-forward batching

Document scheduling and inference scheduling remain separate admission layers.
The inference Node's tensor-forward dispatcher (`server/tensor_microbatch.zig`)
fuses explicitly row-independent stages (text rerankers, BERT-style NER,
rewriting, REBEL, Whisper) on local and distributed paths. It complements the
typed Florence, embedding, GLiNER, and generation schedulers rather than
forcing them through one interface. It groups by session generation, task,
non-batch geometry, execution gate, supervision boundary, and resource policy;
packs at most eight submissions / 64 rows / 64 MiB per window; admits packing
and compute before execution; and scatters validated zero-copy views. One
expired caller never discards a healthy peer's rows, capacity fallback happens
before a fused forward, and backend failures do not trigger singleton replay.

Qualification is explicit: dynamic graph metadata plus pipeline opt-in, or a
native session's row-independence callback. Whole-request GPU generation and
multimodal paths keep their locks; fixed chunking has no forward to fuse;
external providers control their own execution. Composite runtimes, stage
planning, length classes, the embedded-worker transport, and the remaining
exclusions are specified in
[`pkg/inference/BATCHING.md`](pkg/inference/BATCHING.md). BackendRuntime lane
limits and the maintenance scheduler are in
[MANAGERS.md](MANAGERS.md#lane-profile-and-maintenance-scheduler).

> **Relocated:** The previous detailed notes in this section (runtime
> ownership, worker transport and preparation admission, maintenance scheduler,
> lane limits, model-load flight handling) →
> [docs/design/pdf/history/shared-batching-and-runtime-notes-2026.md](../docs/design/pdf/history/shared-batching-and-runtime-notes-2026.md).

### Remaining resident-decoder execution contract

The next cache optimization must live under the pinned backend runtime, not in
PDF preparation or a second process-global prefix cache. A backend-owned decoder
state should carry request/context identity, artifact generation, cache position,
device handles and a residency lease. Prefill/append operations consume admitted
input tokens and return logits without exporting the growing cache to host memory.
Cache replacement must reserve old/new overlap in the actual device domain and
retain cross-attention buffers until the last request or batch-row view releases
them. Cancellation must not release storage before device completion.

Fusing resident states additionally requires qualified device-side row
gather/scatter and compatible cache positions. Backends lacking those operations
must keep the validated host-tensor fusion path or use singleton resident
execution; a device handle must never masquerade as an empty host tensor.
Separate init/past graph plans must pin both artifacts, validate matching cache
names/dtypes/layouts, and share the same admission and completion ownership.
Enabling these paths requires per-backend parity, mixed-EOS, cancellation,
allocation-failure, cache-pressure and measured transfer/throughput tests. They
are not implemented by the host fusion changes above.

Branch-specific admission additionally needs a backend-qualified output contract
for each prefill/append branch. The current conservative planner must retain full
cross-output allowances until the backend proves that its cached branch returns
empty cross outputs. Observing an empty output once, or recognizing cache names,
is insufficient proof for future calls. Qualification should bind to the pinned
artifact generation, expose branch geometry to both cold planning and execution,
and validate outputs against that contract. Device-resident execution and this
branch qualification remain unimplemented; existing resident hooks alone do not
provide independent cache ownership and device-domain KV admission.

## History and verification notes

Dated implementation records for this document live in
`docs/design/pdf/history/`:

- [render-control-verification-2026-09.md](../docs/design/pdf/history/render-control-verification-2026-09.md):
  September 2026 render-control verification notes; durable decisions are in
  [Backpressure and controlled overlap](#backpressure-and-controlled-overlap).
- [review-and-migration-ledger-2026.md](../docs/design/pdf/history/review-and-migration-ledger-2026.md):
  the review defect ledger and the migration/implementation-status ledger.
- [performance-and-ocr-verification-2026.md](../docs/design/pdf/history/performance-and-ocr-verification-2026.md):
  the performance-review change log and the resident-OCR, scratch-retry, and
  OCR-selection verification narrative.
- [shared-batching-and-runtime-notes-2026.md](../docs/design/pdf/history/shared-batching-and-runtime-notes-2026.md):
  former shared model-forward batching and BackendRuntime notes.

## Open work

This section collects the genuinely unresolved design questions and the
release-environment work that has not shipped yet, so the rest of this
document can describe the pipeline as it exists.

### Baseline and parity coverage owed by release environments

Production plumbing for document preparation and the opt-in real-model gate
are complete (see [Component implementation notes](#component-implementation-notes)),
but release environments still own recurring model-parity and
performance-baseline work rather than one-time implementation tasks:

- End-to-end metrics for parse, render, handoff, and Florence execution.
- Native Florence batch-versus-serial parity tests running against production
  model artifacts in CI.
- A representative scanned PDF benchmark with 1, 4, 8, and 16 pages, with
  recorded parse count, peak memory, render time, OCR time, and total
  throughput published as thresholds.

This overlaps with the qualification work already called out under
[Acceptance criteria](#acceptance-criteria); both describe the same
outstanding release-environment ownership rather than an architectural gap.

### Execution-path qualification and admission recovery

The September render-control experiments exposed two distinct ingestion paths.
`full_index`/`enrichments` precompute generated artifacts into the commit; `write`
uses durable enrichment replay. Both use bounded PDF rendering. Precommit now
uses invocation-local text-result staging with the common window scheduler;
replay retains its own durable staging and publication. Benchmark provenance must pin
the sync level and require final artifact/vector coverage for either path.

Precommit staged results are an optional, reclaimable cache, not session-long
working-set ownership. Restoring a typed unit consumes its serialized row and
releases duplicate bytes, empty map capacity, and unused allocator credit.
Unconsumed rows participate in resource-manager reclamation so a later source
download or larger render window can recover their budget. Eviction preserves
consumer identities and source digests; a missing row follows ordinary execution.
Cache access is locked, pressure callbacks skip locked borrowers, and teardown
unregisters and drains callbacks before releasing the owner. Callbacks stop
evicting rows once the requested deficit is recovered, preserving useful peers.

Document download/preparation allocators explicitly opt into one bounded
reclaim-and-retry admission attempt. The default budgeted allocator remains
callback-free for storage transactions and other lock-sensitive callers; the
cache's own allocator does not invoke reclaimers. Neither path increases hard
limits or changes render DPI. Tight slice/aggregate-budget regressions cover
consumed-row release, failed restores, larger window admission, download
allocation, partial eviction, and concurrent/reentrant borrowed-row protection.

The convergence boundary is a document execution session independent of its
publication sink: immutable prepared source, bounded page windows, task-specific
consumers, and typed staged results. Precommit execution must collect into the
pending commit, while replay retains attempt-scoped durable staging and lease
recovery. Share preparation/execution policy, not mutable runtime state or the
commit protocol. Moving full-index work after commit merely to activate replay
would change failure/atomicity semantics and is not this optimization.

Speculative render preparers snapshot their owner's cancellation/deadline and
lifecycle token before dispatch. Renewing an operation timeout for scratch retry
must never renew the request deadline or read a sibling's mutable active guard.

Local inference transport now distinguishes a request rejected before dispatch
(`QueueFull`) from response-capacity/transport failure after possible execution.
Only the former enters the scheduler's bounded drain-and-retry path. RPC request
offers are admitted before attachment transfer; response denial remains a
generic retryable failure, not a claim that model execution never occurred.
This preserves useful parallel consumers without repeatedly restarting a whole
document for admission pressure or blindly replaying successful generation.

The cached native provider on Metal is leased exclusively per model store
through compute teardown, because concurrent readers encoding the same mutable
frame raced on one command buffer. Contention returns `QueueFull` without
parking an executor; unfinished frames are retired before handoff. Native
fused batches remain intact and distinct stores stay independent. Running
multiple lanes for one store requires separately admitted mutable
providers/frames over immutable shared weights; it must not be enabled by
removing this lease.

- Whether immutable font program parsing should eventually be frozen and
  shared across render windows. The safe implementation retains a private font
  cache only for the joined waves of one bounded batch and keeps image caches
  task-local. Cross-window reuse needs explicit document-lifetime byte
  admission; it must not hide persistent allocations in executor scratch.
- Whether the conservative source-size, decode-working-set, and
  bytes-per-pixel reservation can be tightened with measured high-water data.
- Whether profiling justifies splitting the lazily activated inference lane
  further by model resource class. PDF rendering already owns a small fixed
  physical lane because renderer scratch benefits from worker affinity; any
  additional split must remain beneath the same aggregate ceiling and preserve
  shutdown ordering.
- Whether the streamed v1 distributed attachment envelope should gain a
  checksummed, resumable frame format. Reading, batched generation, embedding,
  extraction, multimodal reranking, chunking, and transcription now consume raw
  framed attachments; older nodes and external providers retain JSON/base64 or
  multipart compatibility. A successor is justified by independently produced
  media or partial retry, not by ordinary bounded streaming, which v1 now
  supports end to end.
- Whether cross-request broker adapters should be enabled for a model family is
  executor-specific. The task-neutral broker exists, but a family must expose a
  genuinely fused native batch implementation before it may opt in; accepting
  a batch envelope or looping serially is not sufficient.
- Whether the attempt spool should eventually use a compact versioned binary
  unit codec instead of JSON after corpus measurements quantify serialization
  CPU and temporary storage size.
