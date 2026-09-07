# Bounded document preparation and multimodal inference

Status: bounded document preparation, indexed reader execution, multimodal
generation transport, distributed model-aware routing, lease-fenced durable
page-image embedding, observed remote execution, and post-review batching
hardening, single-pass PDF preparation, bounded unit replay, and hot-path
allocation reductions implemented. Backend-runtime-owned rendering and
preprocessing tasks, dynamically bounded parallel preprocessing, direct
JPEG-to-CHW writes, prepared tokenizer inputs, lease-renewed and
transaction-fenced attempt storage, and bounded concurrent proxy partitions
are also implemented.

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
Before collecting OCR pages it resolves compatible consumers and chooses a
bounded common-multiple render width. A four-item OCR executor can therefore
produce a sixteen-page shared window for a compatible visual embedder, while
still invoking OCR in batches of at most four. The optional widening ceiling
is `ANTFLY_ENRICHMENT_PDF_SHARED_WINDOW_MAX_PAGES` (default 32, absolute maximum
128); it does not change the OCR model batch ceiling or provider request limits.
Collection remains byte-bounded and flushes its current prefix if lookahead
allocation is denied. Generated units transfer ownership into the execution
slice rather than duplicating all retained text and regions. Renderer geometry,
pixel ceilings, and composite memory admission can shorten any planned window.
Memory contracts are requested only for legal model sub-batches; their summed
ceilings conservatively account for the larger retained render window.
Before dispatching an owner's rendered window or starting speculative render
prefetch, it offers the borrowed pages to later compatible consumers. Physical
compatibility includes source identity and credentials, DPI, pixel/dimension
limits, preferred image geometry, encoded-output allowance, representation,
and decode limits. Model names, prompts, and result types are not render keys.

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
  The v2 page identity includes the resolved model's target geometry, resize
  mode, and resampling policy, so a changed preprocessing contract invalidates
  old vectors even when the configured model name is unchanged.
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
- Fan-out is synchronous on the enrichment thread, before prefetch. Renderer
  workers never enter enrichment state or concurrently use another consumer's
  PDF session. There is no document-sized rendered-image cache.

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
execute (for example `generate.batch`, `generate`, `rerank_multimodal`, or
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

## Review findings and required fixes

The following findings apply to the implementation described later in this
document. They are architectural requirements, not Florence-specific cleanup:

1. **OCR was coupled to readers.** The durable configuration and planner must
   select a task-specific reader or generator executor. Gemma4 must not be
   wrapped in `LoadedReader` merely to reuse page rendering.
2. **Batch support was inferred from provider identity.** Replace
   `provider == antfly` checks with resolved model/backend capabilities.
3. **An accepted outer batch could execute serially inside the reader.** Return
   an execution report and label non-native families as serial compatibility.
4. **The local binary ABI was operation-specific.** Generalize binary payloads
   and per-item attachment references across read, generate, and embed.
5. **One source fingerprint described an entire model call.** Carry identity
   per item so cross-document native batches retain exact provenance.
6. **Prompt behavior was inferred from model-name text.** Prompt family and
   output schema come from resolved capabilities or explicit configuration.
   Model-name detection remains compatibility-only and emits a diagnostic.
7. **Embedding templates were mistaken for durable PDF processing.** Durable
   page/chunk artifacts use enrichment replay and the bounded document
   renderer; template helpers remain request-local compatibility conveniences.
8. **Local embedders were capped at one media part at the provider level.**
   Apply model cardinality limits and expose a true per-page embedding batch
   rather than sending an ambiguous multi-page content list as one vector.
9. **The generation batch endpoint rejected multimodal requests.** Accept
   bounded independent multimodal items when the resolved model supports them;
   report serial execution honestly until a native multimodal batch path is
   available.
10. **Production integration coverage was Florence-shaped.** Add the same
    bounded render fixture through generic fake reader, generator, and embedder
    executors, plus real-model opt-in tests for Florence, Gemma4, and ClipClap.
11. **Remote reader batches lost page identity.** HTTP reader responses are
    indexed but do not echo Antfly's internal provenance. The transport adapter
    must validate response indexes and cardinality, then reattach the original
    item, source, and page identity before returning to enrichment. Identity is
    never inferred from response completion order.
12. **PDF staging keys used concatenated user strings.** A textual marker plus
    substring scanning could alias another artifact/embedding pair or match a
    legitimate asset-state key. Staging needs a dedicated internal key kind and
    independently length-encoded document, artifact, embedding, and unit
    components. Cleanup scans only that exact typed prefix.
13. **Page-vector publication was not an exact set when a PDF shrank.** Current
    page artifacts, stale artifact deletion, dense artifact counters, and the
    durable replay record for vector replacement must change in the same
    document-store transaction. Stage promotion is
    therefore an input to the generated-record writer, not an earlier
    enrichment-side transaction. A missing stage or replay-append failure
    changes neither generation.
14. **Capability discovery was repeated on hot paths.** A runtime-owned cache
    must key snapshots by endpoint, model, semantic task, concrete operation,
    and an authentication digest; coalesce concurrent misses; bound entry
    count; refresh on a short TTL; and use a previously validated snapshot
    briefly during catalog outages. Raw authentication material must not become
    a cache key.
15. **Remote model limits were advisory.** Reader, generator, and embedder
    executor boundaries must partition calls by resolved item and encoded-byte
    limits, then validate MIME, modality, media cardinality, pixels where known,
    and aggregate bytes on every concrete provider invocation. Planner windows
    remain an optimization, not the security or memory boundary.
16. **Generator item errors aborted successful siblings.** Batched production
    returns `WorkItemResult(T)` per request. A deterministic item error remains
    attached to that item; valid siblings are applied once and are not rerun
    serially. Compatibility callers that cannot represent item errors may fail
    the outer call, but must still free successful sibling outputs.
17. **Failure validation could leak a completed producer batch.** Validation
    owns the returned batch until both its execution report and cardinality
    pass. Every invalid-report and invalid-cardinality edge destroys successful
    sibling payloads before returning an error.
18. **PDF publication retained an unbounded document-sized commit set.** Query
    visibility is request-atomic, so page artifacts, vector replacement, stale
    deletion, and coverage cannot be flushed independently without
    generation-aware index filtering. The implemented safe boundary admits a
    configurable `max_document_pages` before staging, clamps it to an absolute
    16,384-page ceiling, cleans invisible staging records in fixed-size delete
    windows. Stale discovery streams the page namespace, retains only artifacts
    for the selected embedding, and applies the fanout ceiling after that
    filter; unrelated embedders cannot consume the limit. A separate total
    scan-work ceiling bounds malformed or adversarial namespaces without
    misreporting them as source page-count failures. Fanout overflow, scan-work
    exhaustion, and source page-count admission have distinct errors and are
    stable terminal request failures. The default is 2,048 pages. Lifting the
    absolute limit requires
    adding an active generation to vector records and query filtering first; an
    artifact manifest pointer by itself is insufficient.
19. **Per-page accumulation had ambiguous allocation ownership.** Page keys,
    stage keys, and each field of a dense embedding record transfer ownership
    only after their destination append succeeds. Error paths free exactly the
    still-local allocations.
20. **URL reader admission ignored inline payload size and MIME.** Base64
    `data:` inputs are parsed at the executor boundary, their complete resident
    data-URI size is charged after canonical base64 validation, and their MIME
    is checked against the resolved model. Network
    URLs remain provider-owned until download and must be checked by the
    server-side media budget afterward.
21. **Remote execution telemetry was predicted from capabilities.** Read and
    generation responses now optionally carry an observed execution report.
    New clients validate and use it; old servers without the field are treated
    conservatively as serial compatibility. Mixed native/serial/fallback
    reports retain their exact counters. Aggregate counters are not converted
    into invented per-item modes; the report's requested-item unit remains the
    concrete reader work item (an image). The generation batch endpoint reports
    serial items while it schedules independent decode invocations.
22. **Capability single-flight waits ignored cancellation and shutdown.** Waits
    now poll a semantic cancellation token, honor the request deadline, and
    bound the catalog HTTP request. Cache shutdown closes admission and drains
    outstanding flights before destroying their events or keys. Cancellation
    and timeout are control flow, not discovery failure: they propagate through
    the managed embedder instead of selecting conservative execution or stale
    cache data. All absolute comparisons use `antfly_platform.time`'s monotonic
    clock; `std.Io` clocks are used only for relative waits.
23. **Generator modality admission omitted raw documents.**
    `application/pdf` maps to the document modality and is accepted only when
    the resolved generator advertises document input and PDF MIME support.
24. **Generic generator media was serialized but ignored by the HTTP server.**
    The generation parser now consumes the shared `media` form for image and
    audio attachments, validates declared and decoded MIME types, charges the
    existing aggregate media budgets, and rejects unknown or malformed content
    parts instead of silently dropping them.
25. **Distributed routing omitted the document inference surfaces.** The Go
    proxy now routes read and generation-batch operations, extracts model
    identity from nested batch bodies, rejects mixed-model batches, and merges
    node catalogs using conservative capability intersection.
26. **Per-item generator failures collapsed remote retry policy into one local
    error.** The common result envelope now retains a stable failure code,
    authoritative `retryable` flag, and optional `retry_after_ms`. Durable
    enrichment retries only retryable items, applies bounded provider backoff
    guidance, and records deterministic failures without rerunning successful
    siblings.
27. **Leases did not fence publication.** Every lease tenure now has a
    monotonic epoch. Long render/inference windows heartbeat the exact tenure,
    and the generated-record writer validates owner, epoch, and expiry inside
    the same transaction that promotes stage records and appends replay. A
    stale owner cannot publish or release a newer tenure, even when a process
    restart reuses the configured owner ID.
28. **Retry staging was shared across attempts.** Stage keys now include the
    lease epoch and request attempt identity. Failure cleanup deletes only the
    current attempt, successful promotion consumes only that attempt, and the
    active fenced owner retires abandoned attempt namespaces without exposing
    partial vectors.
29. **Catalog limits and telemetry described aspirations, not execution.** The
    server publishes a resolved task descriptor with live item, byte,
    media-part, batch-mode, and per-item-failure facts. Legacy catalogs fail
    closed to singleton execution. Multimodal generation remains
    `serial_compatibility` until the backend actually executes natively, and
    execution reports separately count pre-execution rejected items.
30. **Split-delta admission could invoke arbitrary reclaimers under storage
    locks.** Resource management now exposes a single-attempt, non-reclaiming
    reservation for transaction-owned work. Exact split-delta accounting uses
    that primitive, so callback lock ordering cannot enter the write
    transaction; temporary pressure is returned to the durable supervisor.
31. **Authoritative API text still said multimodal batches were rejected.**
    Inference and shared security specifications now describe bounded image and
    audio batch media, strict malformed-part rejection, and the same aggregate
    byte, image-header, and decoded-image admission used by single requests.
32. **Distributed inventory was model-aware but not task-aware.** A node that
    advertised the same name only as a generator could be selected for reading,
    and partial catalog fan-out could overstate the capabilities of another
    still-routable node. Registry snapshots now retain per-model operations,
    every initial and retry selection applies that filter, executable routes
    never use unknown bootstrap inventory, and scoped cluster discovery leases
    only successful task-eligible responders so omitted nodes cannot become
    routing candidates.
33. **Generic generator media bypassed generation-batch byte and decoded-pixel
    admission.** Preflight now includes raw `media` parts in the aggregate
    resident-byte shape. Parsing uses that admitted byte ceiling; image headers,
    dimensions, and aggregate pixels are validated before model loading; and
    slot ownership grows to the decoded peak or rejects the request. The
    published eight-part ceiling is enforced by the parser rather than remaining
    advisory.
34. **A manifest could upgrade a resolved executor to native batching.** Model
    manifests may lower resource ceilings, but the server and standalone
    resolver restore execution mode from the concrete backend after applying
    those limits. Generator execution remains serial compatibility and native
    reader batching remains Florence-specific until another executor implements
    the same contract.
35. **The distributed proxy retained request bodies without a ceiling.** The
    proxy needs the body for nested-model routing and bounded failover, but now
    rejects declared and streaming bodies beyond a configurable retained-byte
    limit (256 MiB by default) before forwarding. A process-wide weighted body
    body-plus-decode admission budget (768 MiB by default) is acquired before
    reading and held through retries, so concurrent requests cannot multiply
    that per-request ceiling into unbounded proxy memory. Admission is FIFO by
    request arrival, preventing a steady stream of small requests from starving
    an older large
    PDF body; cancellation removes a waiter and immediately advances the queue.
    Inference nodes still apply their stricter decoded-media and model admission
    independently.
36. **Cluster catalog fan-out multiplied memory by node count.** Discovery now
    permits at most eight simultaneous upstream catalog bodies, caps each at
    8 MiB, bounds the merged descriptor set at 32 MiB, and drains every worker
    before returning an error. Catalog scale therefore cannot create an
    unbounded request-scoped memory spike.
37. **Successfully discovered task-unknown models still routed as every
    operation.** Model inventory now has explicit bootstrap, task-unknown, and
    known-operation states. Only known operation advertisements participate in
    normal routing. Bootstrap compatibility is restricted to task-unscoped
    legacy inventory; a legacy generic entry cannot receive any executable
    model-family traffic merely because its model name matches.
38. **The proxy catalog described a different pool from request execution.**
    Capability discovery now carries model and task scope and builds an
    immutable lease from caller-authorized healthy responders in the matching
    route cohort. Execution selects within that route and intersects its
    concrete pool and operation with the lease. Unscoped legacy listing remains
    pool-scoped.
39. **Zero overloaded both an unknown limit and a real numeric value.** Version
    2 capability descriptors use null for unknown/not published and preserve
    zero as a known disabled ceiling. The media field is named
    `max_encoded_media_bytes`, because text and URL strings do not consume the
    encoded-media budget. A v1 `max_encoded_bytes = 0` is translated only at
    the legacy boundary. If any eligible endpoint has an unknown optional
    ceiling, conservative intersection remains unknown; a known value cannot
    safely describe a heterogeneous route that may select the unknown node.
    Two known ceilings intersect by minimum, including zero.
40. **A render-window deadline replaced its session cancellation owner.** PDF
    batch rendering now installs a stack-stable composite probe for the full
    synchronous render call. Preflight, worker gates, private Reader forks, and
    render/decode loops stop when either the document/session owner or the
    window deadline cancels, and all launched workers are joined before return.
41. **The normalized contract and distributed catalog still stopped at three
    model families.** `Task`, typed output kinds, local/remote capability
    resolution, server descriptors, and proxy-scoped discovery now cover all
    eight public tasks: readers, generators, embedders, rerankers, chunkers,
    extractors, rewriters, and transcribers. Classifier models remain an
    internal extraction implementation. Array-oriented extract and rewrite
    executors advertise bounded
    `serial_compatibility`; single-operation families advertise `mode = none`,
    `max_items = 1`. No family borrows native-batch claims from another family.
    The proxy also forwards rerank-multimodal, rewrite, and transcribe routes.
42. **Chunk routing inspected the wrong model field and execution ignored the
    selected model.** `/chunk` carries model identity in `config.model`, not the
    request root. The proxy now extracts that nested identity and canonicalizes
    legacy fixed aliases to `fixed`. The server advertises exactly the built-in
    executor it can run, returns `fixed` in results, and rejects an unsupported
    semantic model instead of silently executing the fixed tokenizer. A future
    semantic chunker is added only when model resolution and a concrete direct
    executor land together.
43. **Classification was incorrectly promoted from an internal executor to a
    public task.** Classification is part of the canonical schema-driven
    `POST /extract` API. The public OpenAPI, proxy route, capability task,
    output kind, lease scope, and `classifiers` catalog surface therefore do
    not expose `classify`. Classification-capable models are advertised as
    extractors and execute under the bounded extraction contract. The linked
    classification callback remains an internal convenience for that executor.
44. **Several embedded model families stopped at catalog discovery.** Provider
    ABI v23 includes typed chunk and rewrite operations plus an internal
    classification callback alongside the existing embed, rerank, generate,
    read, transcribe, and extract operations. Linked providers wire all eight
    public task families plus that internal callback to concrete server
    executors. Descriptor presence is therefore no longer used as a substitute
    for an executable callback.
45. **Catalog and execution used different authorization identities.** Both
    paths now use one policy: a configured upstream service credential wins;
    otherwise the inbound Authorization value is forwarded. Scoped capability
    discovery and the request it admits therefore observe the same upstream
    identity. Background refreshes, which have no caller identity, continue to
    require the configured service credential.
46. **A byte ceiling mixed text with media.** Invocation shapes now count only
    retained encoded attachment bytes against the model media limit. Text is
    governed by tokenizer/request limits, and remote URLs are admitted after
    download by the provider-owned request boundary. Inline binary PDF, image,
    and audio attachments are charged before dispatch.
47. **Array-oriented families advertised single-item execution.** Rewrite and
    extract requests already carry multiple logical inputs. They
    now publish a shared hard item ceiling and `serial_compatibility` rather
    than `max_items = 1`; HTTP and direct/linked executors enforce the same
    ceiling. This preserves batching across a distributed hop without claiming
    native model batching where an implementation still loops safely under one
    admitted model invocation. Rerank, chunk, and transcribe remain one logical
    request item because their inner document/chunk/audio collections are part
    of that operation's typed input rather than independent routed requests.
48. **Durable chunk enrichment declared a callback but never invoked it.**
    Configured chunk enrichment now projects the full embedded provider into a
    borrowed, chunk-only descriptor and calls it through the checked native
    boundary. The narrow descriptor avoids pulling generator, reader, and other
    model-family dependencies into minimal embedded storage builds. Fixed
    chunking remains the local fallback; a selected unsupported semantic model
    still fails closed.
49. **Capability truth was duplicated across the server catalog, embedded
    resolver, and asset scheduler.** One exported resolver now derives task
    ceilings, execution mode, media limits, and manifest reductions for both
    local callbacks and `/ai/v1/models`. Extract and rewrite resolve to bounded
    serial-compatibility contracts in either deployment; classification
    executors inherit the extraction contract rather than inventing a public
    classifier task. Callers no longer upgrade an extractor merely because its
    provider is Antfly.
50. **Extractor asset requests dropped prepared media.** Extraction requests
    now carry input-indexed borrowed attachments. Provider ABI v23 transports
    those bytes without JSON/base64 through the linked boundary, the inference
    node charges them as borrowed request media, and external HTTP adapters
    base64-encode only while constructing the final wire request. Extractor
    windows obey resolved item, byte, MIME, modality, and per-item media-part
    limits and report serial versus native execution honestly.
51. **Canonical fixed chunk requests could not route through older nodes that
    advertised `fixed_bert` or `fixed_bpe`.** Inventory extraction, catalog
    lookup, scoped merge, and unscoped merge now normalize all built-in aliases
    to `fixed`. This is a compatibility boundary at discovery/routing; the
    executor continues to expose only the canonical identity.
52. **The distributed capability descriptor was too coarse to round-trip the
    normalized contract.** Capability wire version 3 publishes exact input
    modalities, accepted MIME types, granularity, output kind, result
    cardinality, prompt policy, local borrowed-attachment compatibility, and the
    bounded batch descriptor. The proxy conservatively intersects arrays and
    boolean support and requires scalar semantics to agree. V1/V2 remain
    readable at their legacy boundary but cannot acquire V3 claims.
53. **HTTP extraction and linked classification admitted different request
    shapes.** Both paths now call the same extraction-family validators.
    Extraction requires 1..128 logical inputs; classification schemas require
    nonempty texts and labels,
    caps each dimension at 128, and caps Cartesian text-label work at 4,096.
    The source OpenAPI schemas publish the same array limits.
54. **Extractor windowing validated the whole document against one window's
    ceiling.** Batch compatibility is now checked across the logical request,
    while item count and aggregate media bytes are validated separately for
    every emitted window immediately before dispatch. A PDF larger than one
    model batch is split instead of rejected, and an oversized single page
    still fails before transport.
55. **An attachment-only HTTP extraction request could skip attachment
    validation when it had no logical inputs.** Attachment indexes, MIME
    presence, and nonempty bytes are now validated once at the request
    boundary, before the per-input content encoder runs. The direct inference
    boundary independently enforces the same index and nonempty-byte rules.
56. **Version 3 exact capability sets silently ignored unknown or duplicate
    values.** Remote capability parsing now rejects unrecognized modalities,
    MIME types, and duplicate set members. A malformed catalog therefore
    fails closed instead of being interpreted as a narrower but apparently
    valid executor contract.
57. **Image-extraction batches could silently use the first item's prompt for
    every page.** The scheduler now groups media extraction only when the
    effective content/prompt representation matches. The direct executor also
    rejects conflicting nonempty per-image prompts, so fallback and direct ABI
    callers cannot produce prompt-dependent results under the wrong prompt.
58. **Manifest document inputs were advertised as executable raw-PDF support.**
    Resolved modalities are now the intersection of manifest inputs and the
    concrete task executor. Current read, generate, embed, rerank, extract, and
    other executors do not decode a PDF container directly, so none advertises
    `document` or `application/pdf`. PDFs enter inference only after bounded
    preparation produces page images or text chunks. A future raw-document
    executor may advertise that modality only when its request parser,
    admission, and backend implement it end to end.
59. **Extractor admission inspected borrowed attachments but not media inside
    `source_parts_json`.** One normalized extractor item-shape parser now drives
    batch compatibility, executor admission, and window formation. It accounts
    for inline data and data URLs using the encoded representation that remains
    resident alongside decoded buffers, fully validates the base64 alphabet,
    padding, and trailing bits, validates trust and MIME, tracks unknown remote
    media until provider-owned download admission, enforces one media part per
    item, and derives only the prompt text when grouping image work. Borrowed
    attachments charge their binary length; HTTP-bound attachments charge
    their exact base64 expansion. Generator planning uses the same
    transport-aware resident-byte rule.
60. **Extraction batch responses accepted missing or extra results and copied
    a non-JSON batch response to every input.** Batch execution now requires a
    structured output representation, parses the provider envelope once into
    the generated extraction response type, verifies the resolved model, and
    rejects malformed top-level and known per-item fields while preserving
    schema-permitted provider extensions. Every input carries a unique,
    invocation-local opaque wire ID that is independent of the caller's item
    identity; response items are mapped by that ID, then the caller-visible ID
    is restored. Missing, duplicate, or unknown wire identities fail the whole
    batch before any output is assigned. Non-JSON
    extraction falls back to independent task executions, where one response
    has one unambiguous owner.
61. **The proxy validated numeric batch fields but treated malformed V3 exact
    sets as a harmless narrower contract.** Every V3 descriptor, including a
    singleton catalog result, is now normalized before merge. Unknown enum
    values, non-string members, duplicates, missing booleans, empty exact
    sets, empty intersections, non-string task values, contradictory
    preferred/maximum item limits, and nonsingleton `none` mode poison the
    model descriptor. Batch invariants are also checked on legacy singleton
    descriptors without changing their published version. Validation runs on
    each source contract before taking conservative minima, so a malformed
    eligible endpoint cannot lend apparently valid capabilities to a
    heterogeneous route or panic the proxy.
62. **Legacy capability parsing assigned generator-like cardinality and prompt
    defaults to every model family.** V1/V2 compatibility now derives stable
    operation semantics from the task: rerank, chunk, and transcribe return one
    result per request; extract requires a structured schema; chunk and
    transcribe use model-default prompting. The common capability validator
    enforces task/output/cardinality invariants; exact descriptors retain their
    resolved model-specific prompt policy rather than inheriting a legacy
    default.
63. **The HTTP catalog computed extractor decoded-pixel capacity for one image
    while advertising a multi-item extraction request.** Catalog pixel
    admission now uses the extractor executor's full serial-family item cap,
    matching the linked resolver and its concrete invocation ceiling.
64. **Admission inferred transport representation from model capabilities.**
    A model can be reached through both linked and HTTP executors, so
    `borrowed_attachments` cannot determine transport accounting. A common
    `AttachmentTransport` now represents borrowed binary, base64 payload, or
    data URI. It exposes provider wire size separately from peak resident media
    size; batch upper bounds include per-item base64 padding and repeated data
    URI prefixes. Reader, generator, embedder, and extractor windowing and
    final admission receive the transport selected by the concrete route. Remote
    capabilities are normalized to non-borrowed, and the distributed proxy
    never republishes an upstream linked-memory claim.
65. **Scoped model discovery broke the HTTP test boundary.** The reusable test
    server treated the full request target, including `?model=...&task=...`, as
    the route path. It now parses and exposes target, path, and query
    independently and matches only the path. The full generation/reader target
    consequently exercises successful scoped discovery without 404 fallback,
    excess requests, or a blocked server fiber.
66. **Operation URL normalization could drift from the task enum.** Primary
    public operation suffixes now come from an exhaustive `Task` switch;
    only explicit compatibility aliases remain separate, longest-first.
    Classifiers have no operation URL because they execute through `/extract`.
67. **Extraction used caller item IDs as provider demultiplexing keys.** Page
    identities are intentionally local to a source, so two documents may both
    contain `page:000001`. Extraction now generates opaque invocation-local
    wire IDs, maps reordered results by those IDs, and restores or removes the
    caller-visible ID in each typed output. Cross-document batching therefore
    preserves the full work identity without rejecting legitimate repeated
    item labels or leaking transport identifiers.
68. **Inline embedding data URIs bypassed executor admission.** Multimodal
    embedding previously treated every `media_url` as provider-owned and zero
    bytes, even when the URL was an inline data URI whose MIME, canonical
    base64, decoded nonempty size, and complete encoded length were already
    known. Inline image URIs now use the shared strict parser, participate in
    MIME and byte validation, and split at the resolved provider ceiling.
    Genuine network URLs remain unknown until inference-node download
    admission.
69. **The PDF image-embedding render budget confused wire bytes with peak
    memory.** The planner used `max_encoded_media_bytes` directly as a raw PNG
    retention allowance and retained the complete page window while the remote
    adapter created base64 and JSON buffers. Dense embedders now expose one
    route-specific invocation-memory plan: the concrete attachment transport
    plus the complete fixed peak for request envelopes, bounded response bodies,
    parser state, typed vector outputs, and transport/control storage. The
    planner subtracts that fixed peak and then inverses both wire and resident
    ceilings, including per-item padding, before rendering. Remote multimodal
    embedding encodes base64 directly into one final request allocation owned by
    the operation allocator, so raw pages coexist with one admitted body rather
    than an intermediate encoding and a second JSON copy.
70. **The scoped TestServer regression could pass on a 404.** Client fibers
    swallowed failed response assertions and the owner swallowed their group
    result, so an unmatched route never ran the server assertion yet still
    passed. Reusable client outcomes now require an explicit successful
    completion, and group failures propagate before the test returns.
71. **Inline empty media differed from borrowed media.** Borrowed reader images
    rejected empty bytes, while `data:image/png;base64,` and empty inline base64
    crossed the remote boundary and failed later. The shared canonical parser
    still permits callers to validate generic empty base64, but every media
    boundary now requires a nonzero decoded size before batching or transport,
    including generator, extractor, embedder, and direct inference-node binary
    attachment entry points.
72. **Embedding admission omitted allocations that coexist with rendered
    pages.** Reserving only raw PNGs, the encoded request body, and its JSON
    envelope still allowed the HTTP response, JSON parse tree, copied vectors,
    result arrays, and client control storage to exceed the operation grant.
    The resolved dense-embedder route now publishes all of those fixed classes
    in one checked invocation plan. Local linked execution publishes borrowed
    attachments plus its vector/result peak; remote execution additionally
    reserves its response ceiling, parser copy, request envelope, and bounded
    transport control allowance. Allocation arithmetic is overflow checked and
    a plan that does not fit fails before rendering a page.
73. **The host accepted fewer data URIs than the inference node.** The shared
    preflight parser required a bare MIME followed by canonical `;base64`, while
    the node's remote-content RFC 2397 decoder also accepts MIME parameters and
    percent-encoded payloads. The common non-materializing parser now accepts
    both encodings,
    extracts the media-type essence for capability checks, validates every
    percent escape or canonical base64 quantum, and reports decoded size. The
    inference-node direct-media decoder now implements the same complete RFC
    2397 surface, including case-insensitive scheme/encoding tokens, MIME
    parameters, and percent payloads. Reader and embedder admission therefore
    cannot accept an inline value that fails only after distributed dispatch.
74. **Remote generator batching retained several full attachment copies.** The
    previous adapter allocated base64 for each image, formatted another complete
    content part, accumulated a per-item content array, formatted each request,
    and finally copied all requests into the batch body. The adapter now makes
    a measurement pass over each request without retaining canonical content
    copies, allocates the exact final JSON body once, reparses one request at a
    time, and writes its content plus base64 directly into that body. A final
    length assertion makes serializer drift fail closed. Allocation-failure
    coverage exercises both passes and the final-body allocation.
75. **Dense JSON response cleanup leaked earlier vectors after a later
    allocation failed.** The parser now tracks the initialized vector prefix and
    releases it together with the outer vector array on every error. Exhaustive
    allocation-failure testing covers both parsing and per-vector duplication.
76. **PDF OCR still used a fixed four-times-PNG heuristic.** Reader and
    generator producers now expose the same route-owned
    `InvocationMemoryPlan` used by multimodal embedding. Before rendering a
    window, the planner builds attachment-size-independent request prototypes,
    resolves the concrete transport and fixed peak, releases the prototypes,
    and inverses both provider wire and resident-memory limits. The fixed peak
    charges non-media request parsing/serialization, bounded response/parser
    storage, result storage, and control allocations; no provider-name test or
    Florence-specific multiplier selects the budget.
77. **A data-URI adapter was charged for only one encoded copy.** The reader
    compatibility route retains the generated URI while its downstream
    provider serializes the URI into JSON. `AttachmentTransport.data_uri` now
    represents raw bytes plus both encoded copies at peak. Borrowed binary and
    a base64-only streaming executor retain their lower concrete peaks. A
    local compatibility adapter reserves that peak. A distributed generator
    host charges only the base64 batch body it owns; serial fallback inside an
    inference node is admitted independently by that node.
78. **Optional embedder memory hooks silently implied borrowed media.** The
    part-item embedding path now fails closed with
    `InferenceInvocationMemoryUnavailable` unless its concrete implementation
    publishes a plan. The managed ClipClap/remote embedding implementation
    supplies one; adding a new multimodal embedder can no longer accidentally
    bypass wire and response accounting.
79. **Remote response memory had no matching transport ceiling.** Provider
    requests now carry operation-scoped response ceilings derived from the
    selected task's configured result policy and item count. The HTTP client's
    64-MiB setting is only an outer safety ceiling; catalog discovery retains a
    separate four-MiB limit. Caller-supplied clients use the smaller of their
    own outer ceiling and the route ceiling, so the planner and transport
    enforce the same value.
80. **The invocation plan described an estimate as a complete contract.** Every
    media plan now identifies the allocator owner and publishes independent
    per-item and aggregate result ceilings. Caller-owned adapters execute
    through a freeing peak-live bounded allocator. A linked or distributed
    inference node instead owns decoder/model admission and hard caps; the host
    still bounds request/result bridge allocations but does not charge private
    model working memory to an incomplete host estimate. It also validates
    returned cardinality and result bytes. The per-task defaults are
    policy, not guesses: reader, generator, extractor, transcriber, copy, and
    document-extraction limits are independently configurable on the asset
    runtime. A route that needs a larger result must raise and reserve that
    explicit hard limit.
81. **Callers could bypass media admission by invoking the executor directly.**
    `Producer.produce`, `produceBatch`, `produceBatchReported`, and
    `DenseEmbedder.embedDensePartItems` now resolve the concrete route plan
    themselves whenever borrowed media is present. Missing or invalid plans
    fail before entering a callback. PDF scheduling still obtains the same plan
    before rendering, but executor safety no longer depends on that one caller
    remembering the protocol.
82. **Distributed fallback accounting crossed process boundaries.** The host
    generator plan now describes its actual base64 `/generate/batch` request.
    The inference node owns and admits any singleton/data-URI fallback it
    performs after receiving that request. Execution reports cross the boundary;
    transient memory reservations do not.
83. **A global response cap coupled unrelated operations.** Reader, generator,
    extractor, and transcriber adapters propagate a route-owned response limit
    into each HTTP request. Capability discovery has its own smaller ceiling.
    This avoids both rejecting configured large extraction/generation results at four MiB
    and reserving four times a caller-owned client's default 100-MiB ceiling for
    a small OCR request.
84. **RFC 2397 parsing existed in several subtly different forms.** A single
    dependency-neutral parser in `antfly_scraping` now validates canonical
    standard base64, percent decoding, case-insensitive markers, parameters,
    and the RFC's omitted-media-type form. The generic parser returns the
    `text/plain;charset=US-ASCII` default; image/audio admission layers require
    an explicit compatible MIME type as policy. Host preflight, inference-node
    decoding, remote-content downloads, reader adaptation, transcription, and
    Vertex/Gemini serialization use that shared implementation.
85. **The reader wire adapter did not consume the shared report's rejected-item
    count.** Reader batches currently return a success value for every input,
    so a nonzero remote rejection count contradicts their result cardinality.
    The adapter now rejects that report instead of dropping the field, and its
    fixtures initialize the complete generated wire type.
86. **Decoded data-URI MIME metadata had ambiguous ownership.** The shared
    decoder returns owned MIME and payload buffers. Consumers that retain only
    the payload explicitly release the MIME allocation; consumers that retain
    both transfer both. This keeps request parsing allocation-failure safe while
    preserving parameter-only RFC media types such as `;charset=utf-8`.
87. **Invocation enforcement depended on the `media` field instead of the
    effective input.** Generator and extractor media can live in
    `source_parts_json`, and transcription audio lives in `source_text`; copy
    and document extraction also return potentially large results without a
    model media array. A runtime that publishes an invocation contract now
    applies it to every nonempty producer call. Legacy implementations fail
    closed when the request contains borrowed media, structured content parts,
    or transcription input. Result ceilings consequently protect all producer
    families, including copy and document extraction, rather than only visual
    model calls. A planned batch must share one producer task, configuration,
    and attachment representation; mixed work is repartitioned by its caller
    rather than borrowing the first item's route contract.
88. **URL-only embedding items bypassed the memory and result contract.** The
    part-item embedder previously resolved a plan only after finding a binary
    attachment. It now resolves the concrete route before sanitization for
    text, network URL, inline data URI, and binary items alike. Sanitized text,
    serialized URL/content-part upper bounds, transport copies, provider
    allocations, returned cardinality, per-vector dimensions and finiteness,
    per-item bytes, and aggregate vector bytes are all enforced at the public
    boundary. A URL-only embedder without a route plan fails closed.
89. **Data-URI classification could disagree with RFC 2397 decoding.** Several
    call sites tested a case-sensitive `data:` prefix before calling the shared
    case-insensitive parser. Uppercase schemes could therefore be classified as
    network URLs or bare base64, bypassing the admission calculation that later
    decoding required. Scheme recognition now belongs to the shared parser and
    is used by readers, generators, embedders, templates, transcription,
    Bedrock/Vertex adapters, and inference-node request parsing. Local
    generation preflight and materialization use the same parsed payload,
    encoding, decoded size, and MIME essence; borrowed attachment MIME
    comparisons are also essence-based and case-insensitive.
90. **Route-plan resolution allocated before the invocation was bounded.** A
    provider config or structured input could force JSON parsing and route
    construction on the unrestricted caller allocator before the executor
    ceiling existed. Producer contract resolution now runs through a separate
    bounded allocator derived with overflow-checked arithmetic from the exact
    config, source, structured-parts, and item counts. Dense part sanitization
    similarly starts only after its route plan and preprocessing allowance are
    established. Exceeding either pre-execution ceiling returns the same
    invocation-memory failure used during execution.
91. **Logical result policy and HTTP wire limits described different maximums.**
    A configured logical result can expand to six bytes per byte when encoded
    as a JSON string, while an unrelated 64-MiB transport clamp could make the
    advertised logical limit impossible. Remote producer planning now derives
    both limits together: the response body reserves a conservative six-times
    JSON expansion plus its envelope, and the logical result ceiling is reduced
    when the caller/client/provider outer cap is smaller. Execution validates
    the effective logical ceiling, so a request is never admitted under a
    result promise its transport cannot carry.
92. **Local inference was hard-bounded by an incomplete host estimate.** The
    local dense-embedding plan reserved vectors plus a small control allowance,
    then used that value to cap manifest parsing, URL download, media decode,
    preprocessing, model execution, and results. Valid local PDF pages and URL
    inputs could therefore fail before the inference node's real admission ran.
    `InvocationMemoryPlan` now explicitly distinguishes caller-owned adapters
    from executor-owned inference. Every adapter retains a hard bounded
    request/result allocator; linked local and distributed nodes additionally
    use their concrete decoder/model admission, media caps, deadline, and
    result caps for allocations that never cross that allocator. This makes
    the same ownership rule apply whether the node is in-process or remote.
    Executor ownership is an explicit `AntflyProvider` guarantee; arbitrary
    callbacks that do not publish it fail closed rather than receiving the
    inference node's privilege by provider name.
93. **Legacy reader `source_text` bypassed the plan requirement.** Reader input
    may be a URL or JSON URL list even when `media` and `source_parts_json` are
    empty. Every model-backed producer type—reader, generator, extractor, and
    transcriber—now requires an invocation plan regardless of which legacy
    field carries its effective input. Copy and built-in document extraction
    retain their non-model compatibility path; the production runtime still
    publishes and enforces result plans for them.
94. **Aggregate embedding dimensions allowed compensating malformed vectors.**
    A two-item response with lengths `dims - 1` and `dims + 1` had the expected
    total value count and passed the generic boundary. The boundary now checks
    every vector for exact dimensions and finite values before measuring result
    bytes, matching the managed provider validator instead of relying on it.
95. **Per-item result policy was enforced only as a batch sum.** A single item
    could consume another item's allowance as long as the aggregate stayed
    below `items * bytes_per_item`. Invocation plans now carry both
    `max_result_bytes_per_item` and `max_result_bytes`; producer values,
    per-item reported successes, and embedding vectors must satisfy both.
96. **MIME parameters changed capability and attachment decisions.** Exact
    string checks rejected values such as `image/png; charset=binary`, while
    local helpers independently stripped parameters with different validation.
    `antfly_scraping.data_uri.mediaTypeEssence` is now the shared validated
    authority used by capability admission, borrowed attachments, host/model
    modality classification, linked ABI MIME matching, and direct inference
    extraction. Parameters remain available for transport but do not invent a
    new model input type.
97. **Dense preprocessing accounting omitted structural and growth peaks.** An
    invalid UTF-8 repair allocates a `ContentPart` array, an optional-owned-text
    array, and replacement text. UTF-8 repair now computes the exact output size
    first and allocates once, eliminating geometric growth and the transient
    old-buffer-plus-final-slice peak. The preprocessing allowance includes each
    remaining coexisting structure with checked arithmetic. Request JSON is no
    longer represented by a single guessed MIME: a concrete invocation shape
    sums each text, URL, MIME string, and variant envelope, so mixed MIME batches
    and URL/text items publish the bytes their adapter actually writes. That
    shape also carries the normalization peak, so scheduler admission and the
    public executor enforce the same complete plan.
98. **Executor-owned model admission accidentally disabled the caller boundary
    cap.** Executor ownership now excludes only decoder/model allocations that
    stay on the inference node's admitted allocator. Request serialization,
    attachment descriptors, response parsing, typed results, and all asset-stage
    preparation continue to use the bounded caller allocator. Linked and remote
    routes therefore have the same two-layer contract: a hard transport/result
    ceiling plus independently admitted model memory.
99. **MIME essence extraction accepted malformed parameters and erased codec
    conflicts.** A shared parser now validates the complete media type,
    including token and quoted-string parameters and duplicate names, before
    returning its essence. Capability checks remain parameter-insensitive, but
    a redundant declaration that names parameters must match the physical
    attachment using case-insensitive names and exact logical values, so
    `codecs=opus` cannot authorize `codecs=vorbis` or a differently cased value
    unless that parameter's task-specific layer explicitly normalizes it.
100. **The exact capability wire used a closed eight-MIME allowlist.** The Zig
    value contract now retains common flags plus bounded additional canonical
    essences. Capability wire V4 and the distributed proxy validate and
    conservatively intersect arbitrary legal essence strings instead of
    rejecting every new image, audio, or document format. Catalog values are
    lowercase canonical essences, aliases are normalized at attachment
    admission rather than advertised as distinct capabilities, extension
    counts are bounded consistently on every node, and each MIME must map to an
    advertised input modality.
101. **The normalized scheduler contract described only media pressure.** V4
    adds optional task-resource ceilings for text bytes, input and output tokens,
    candidate counts, and schema bytes. Manifest limits reduce executor defaults;
    remote discovery and proxy intersection preserve unknowns conservatively;
    and linked read, generate, embed, rerank, chunk, extract, rewrite, and
    transcribe boundaries validate the dimensions they can measure before
    model work. Request-scoped limits such as schema and encoded bytes remain
    active even for a malformed zero-item invocation. Exact tokenizer-dependent
    counts remain executor-owned when a planner cannot compute them.
102. **Render-window byte pressure could silently reduce visual quality.** OCR
    and page-embedding windows previously selected their item count first and
    divided retained PNG bytes afterward. A large batch could therefore render
    each page at a lower resolution than the same page would receive alone;
    visual embedding then persisted the degraded vector as a successful durable
    artifact. Window formation now computes each page's singleton encoded-output
    allowance, admits only the largest prefix whose combined render/inference
    budget preserves those allowances, and shortens the window when fixed or
    retained memory cannot fit. Page embedding also enforces the same minimum
    render dimension and bounded retry count as OCR. Memory pressure changes
    throughput, not semantic input quality.
103. **HTTP media adapters retained a weaker MIME comparison than linked
    execution.** Generation, embedding, chunking, and extraction decoded data
    URIs with the shared parser but compared only MIME essences afterward. They
    now use the same declaration-compatibility relation as borrowed attachments:
    an essence-only declaration may accept physical parameters, while every
    explicitly declared parameter must match the physical media value.
104. **The encoded-reader ABI trusted its runtime wrapper to enforce model
    capabilities.** Direct linked callers could bypass the resolved model's MIME,
    item, encoded-byte, prompt, and output-token ceilings, and the reader-level
    validator accepted malformed `image/*` parameter syntax. The reader contract
    now parses the complete MIME type, and the production host resolves and
    validates the concrete read invocation before dispatch. The only exemption
    is the explicit test executor override, which has no model catalog.
105. **Decoded-pixel admission bounded render waves instead of model
    invocations.** The renderer correctly reset its in-flight pixel counter
    after each joined worker wave, but every completed page remained retained
    for one later OCR or embedding call. A serial eight-page render could
    therefore satisfy a 50-megapixel wave limit while constructing an
    80-megapixel model batch. PDF sessions now expose allocation-free adaptive
    geometry preflight; OCR and page-embedding planners accumulate that exact
    geometry and shorten the invocation window at the resolved model ceiling.
    The render-wave limit remains a separate scratch/concurrency control.
    Independently, reader, generator, embedder, and extractor executor
    boundaries inspect concrete encoded-image headers, populate aggregate
    `InvocationShape.decoded_pixels`, reject declared/physical MIME mismatch,
    and partition candidate batches on pixels as well as items and bytes. The
    shared image-header inspector is also used by inference nodes, so linked
    and distributed execution measure the same physical payload.
106. **Resolved model limits were planner hints at the distributed generation
    boundary.** `/generate` and `/generate/batch` applied node-wide media
    admission, but a caller that bypassed or outlived the document planner could
    exceed stricter limits published by the selected model manifest. Generation
    now derives an executor contract from the lightweight manifest before model
    loading. Single requests and independently grouped batch items validate
    modalities, item count, encoded bytes, decoded pixels, media cardinality,
    text bytes, requested output tokens, and exact input tokens. Batch execution
    constructs the largest valid prefix under the resolved item, byte, and pixel
    ceilings; invalid singleton items retain per-item failures and unconsumed
    compatible items remain pending for the next bounded window.
107. **The linked generator preflight discarded concrete media facts.** Its
    allocation preflight retained byte and item counts but not the declared MIME
    or physical pixel count, so direct ABI callers could bypass a model-specific
    MIME or decoded-pixel limit. Capability resolution now precedes preflight;
    every inline declaration or borrowed attachment is checked while the input
    is still encoded, and the materialized image headers are inspected before
    dispatch. The early shape protects allocation while the final concrete
    shape protects model execution.
108. **Extensible image MIME advertisement was not coupled to a codec.** A
    manifest could publish any syntactically valid `image/*` essence even though
    the common inference decoder and physical header inspector supported only a
    smaller set. The image library now owns one inference codec registry mapping
    MIME essence to physical format. Catalog publication, linked capability
    resolution, physical accounting, and decoding all consult that registry;
    an unsupported extension makes the resolved model contract invalid instead
    of advertising a request that must fail later. Adding a future format is one
    atomic change: header probe, bounded decoder, registry entry, and tests.
109. **Inline encoded-byte rejection happened after payload materialization.**
    Reader, generator, embedder, and extractor shaping could decode a complete
    data URI merely to discover that its already-known wire length exceeded the
    model ceiling. Shaping and batch partitioning now consume encoded budgets
    first. Only an item that fits the current invocation may allocate scratch to
    inspect physical dimensions; the subsequent pixel check remains independent.
    This ordering applies to both singleton validation and cumulative windows.
110. **Distributed model-specific limits were enforced only by generation.**
    Capability discovery normalized task limits for every family, but the
    inference HTTP node consumed that resolved manifest contract only in
    `/generate` and `/generate/batch`. A direct HTTP caller could therefore
    bypass a classifier candidate ceiling, extractor schema ceiling, or a
    stricter embed, rerank, read, rewrite, or transcribe limit while the remote
    planner correctly advertised it. The inference node now resolves one
    task-neutral executor contract from the lightweight manifest and applies
    one validator and error taxonomy at every manifest-backed HTTP boundary.
    Task adapters still construct typed invocation shapes: rerankers count
    candidates, extractors count schemas, generators/readers count requested
    output, and media consumers inspect physical encoded bytes and pixels.
    Byte and modality checks precede model loading; exact tokenizer counts are
    checked after tokenizer acquisition. Florence/VLM readers, NER, GLiNER,
    REBEL, zero-shot classification, and composed reader/recognizer extractors
    expose concrete input-token measurement at the executor boundary. GLiNER
    relation extraction admits the larger of its entity and conditional
    composite-relation rows. No character- or byte-count proxy stands in for a
    model tokenizer. Fixed chunking remains an explicit built-in executor with
    no model manifest and therefore keeps its existing hard-coded contract.
111. **A remote exact MIME set could exceed the local physical codec set.**
    Version 4 capability parsing accepted any bounded syntactically valid
    `image/*` essence, even though the same client later had to identify and
    measure that image before dispatch. A remote node could advertise TIFF and
    pass planning, only for local physical admission to reject it. Exact remote
    image capabilities now intersect the shared inference codec registry while
    parsing. Unsupported image essences invalidate the snapshot; supported
    extensions such as GIF retain value-semantic round-trip behavior. This
    keeps catalog publication, distributed planning, and physical execution on
    one codec authority.
112. **Audio capability publication exceeded the linked decoder surface.**
    Audio MIME values are now accepted and advertised only when the compiled
    audio registry can decode that exact essence. Optional codecs such as MP3
    therefore disappear from the effective model contract when their build
    support is absent, and arbitrary `audio/*` extensions fail catalog
    validation instead of surviving until execution.
113. **Native reader capability ignored the executor's batch-size override.**
    The Florence executor and capability resolver now share one effective
    `ANTFLY_INFERENCE_READ_BATCH_SIZE` authority. A value of one publishes
    serial compatibility, while larger values clamp both preferred and maximum
    item counts. Local and distributed planners can no longer dispatch a batch
    that the selected node has already configured itself to serialize.
114. **Linked text-family wrappers were treated as the final trust boundary.**
    Early linked-ABI checks are useful for cheap rejection, but another
    in-process caller could invoke the node directly. Dense and sparse
    embedding, reranking, rewriting, generation, transcription, reading, and
    extraction—including its classification executors—now resolve and enforce
    their model contract again
    inside the concrete node executor. Tokenizer-dependent limits are measured
    there after model acquisition; media and schema limits remain available for
    pre-load rejection. The same invariant therefore holds behind HTTP, the
    linked runtime, and a distributed proxy: adapters may narrow work, but only
    the executor grants final admission.
115. **Page-image embedding gave renderer workers a local byte ceiling without
    owning that memory globally.** Worker allocators remain task-local for
    thread safety, but the document operation now pre-reserves the entire
    native render grant from the process resource manager before coordinator
    parsing or worker creation. A distinct output grant is transferred to the
    allocator that retains PNG and provider buffers. OCR and visual embedding
    therefore share one ownership rule: local allocators enforce the granted
    partition; they never create global capacity.
116. **Proxy byte admission was bounded but not fair.** Broadcast wakeups let
    newer small requests repeatedly consume capacity ahead of an older large
    PDF. The weighted semaphore now queues in strict FIFO order, grants only
    from the head, removes canceled waiters under the same lock, and rejects an
    impossible request larger than total capacity instead of waiting forever.
117. **Bootstrap routing could rebind a cached capability to an undiscovered
    replacement node.** A client could discover model/task/operation limits from node A,
    then reach a newly registered node B after A disappeared even though B had
    not published that operation. Every executable route now requires a current
    per-endpoint model/task advertisement. Bootstrap names remain available only
    for task-unscoped legacy inventory, so topology churn produces temporary
    unavailability rather than capability-unsafe execution. This applies to
    read, generate, embed, rerank, chunk, extract, rewrite, and transcribe
    equally.
118. **Artifact execution could inherit a colliding query index after rebasing.**
    Public query aliases and durable embedding artifacts are distinct
    namespaces. Dense execution, media-part limits, capability discovery, and
    invocation-memory planning now all resolve the artifact producer; a public
    index with the same name cannot change the model or transport contract used
    by enrichment.
119. **A generator catalog lease was accidentally bound to an ambiguous task
    family.** Capability discovery now carries both semantic task and concrete
    operation. `generate`, `generate.batch`, and `chat.completions` have
    independent cache entries, route cohorts, conservative descriptors, and
    leases; embed and rerank aliases follow the same rule. A batch-specific GPU
    route can no longer be weakened by a default-pool single-request model, and
    a terminal batch reject cannot appear available through another alias.
120. **The token did not identify the descriptor revision.** Scoped catalogs
    now return a SHA-256 capability revision alongside the opaque token. The
    client cache stores descriptor, token, and revision as one value and every
    lease-aware transport sends both headers. The proxy binds the immutable
    lease to the exact merged descriptor bytes, authorization scope, semantic
    task, concrete operation, route-policy generation, and endpoint incarnations
    observed during discovery. A later catalog
    refresh cannot mutate or evict that admitted snapshot; a replaced endpoint,
    changed authorization/task/revision header, or expired token returns an
    explicit stale-plan conflict. Newly joining endpoints remain excluded until
    rediscovery. If a model tightens its live contract during the short lease,
    the concrete node's final executor validation rejects over-limit work.
121. **Planning and execution read capability state in separate cache calls.**
    `CapabilityLease` is now the atomic cache API. Single-flight publication,
    stale-if-error fallback, and allocation failure publish or reject the whole
    descriptor/token/revision tuple. Reader, generator, embedder, reranker,
    extractor, chunker, and transcriber execution consume that tuple directly;
    cache-admission OOM is no longer silently ignored.
122. **Proxy admission charged logical body length rather than live memory.**
    Known-length bodies receive an exact-size replay allocation. Unknown-length
    bodies reserve the configured maximum only while their bounded stream is
    being read, grow storage in proportion to bytes actually received, and
    immediately return unused admission once their retained buffer capacity is
    known. Admission then charges that physical replay capacity and JSON
    model-extraction working representation through retries, using saturating
    arithmetic. A tiny chunked request therefore neither allocates nor retains
    admission for the full 256-MiB ceiling.
123. **Remote reranking recreated discovery state for every query.** The
    provisioned table-read runtime now owns one bounded capability cache and
    lends it through the typed provider descriptor to reranking. Stateless API
    compatibility callers retain a bounded fallback, while production query
    traffic reuses single-flight snapshots across requests.
124. **Chunk and transcription routes were catalogued but bypassed capability
    fencing.** Remote chunking and transcription now discover their task/model
    contract, validate the invocation shape, attach the token and descriptor
    revision, recognize stale-plan responses, and invalidate the tuple before
    retry. This completes lease-aware HTTP transport for all currently routed
    families; task-specific request and result types remain separate.
125. **Pool-bound discovery could not execute a non-default configured route.**
    Scoped discovery now resolves a route capability cohort: an explicit pool,
    a stable fully matched route's destination/fallback pools, or—when source
    context is unavailable or a time window may change during the lease—the
    priority-bounded union of operation/model-compatible route pools. It no
    longer intersects a GPU route with unrelated slow or serial pools merely
    because they host the same model name. Discovery and execution now share
    one normalized routing context and precedence rule: configured routes win,
    then the explicit pool, then the process default. Execution intersects the
    selected pool and concrete operation with the immutable endpoint set.
126. **Caller-visible models were prefiltered through service-credential
    inventory.** Scoped discovery begins from healthy endpoint incarnations and
    treats each caller-authorized catalog as the eligibility authority. Leased
    execution consumes the operation set captured by that scoped result rather
    than rechecking the global background inventory, so tenant-only models are
    discoverable without weakening legacy bootstrap rules.
127. **One failed catalog node disabled the entire distributed pool.** Catalog
    aggregation now merges successful eligible responders and places only those
    exact endpoint incarnations in the lease. Destination eligibility and
    condition statistics are evaluated against that set before weighted route
    selection, and endpoint selection intersects it again. Omitted failures
    therefore cannot make the descriptor an over-promise or steer an otherwise
    serviceable request into an uncovered pool.
128. **Mutable per-authorization revision slots evicted live work.** Capability
    leases are now immutable snapshots and endpoint state no longer carries a
    capped authorization-revision map. The bounded lease table removes expired
    entries but refuses new issuance under live saturation instead of evicting
    an admitted lease. Equivalent model/task/operation/route-generation/
    authorization/descriptor/endpoint snapshots reuse one indexed token and
    renew its expiry, so a 30-second client
    refresh cannot fill a five-minute server table with duplicate leases.
    Refresh changes affect new identities; endpoint replacement, expiry, or
    mismatched headers still invalidate old work explicitly.
129. **Write-side semantic chunking recreated discovery and I/O state per
    document.** Remote runtime services are now independent of the optional
    local callback provider. Provisioned storage passes its capability cache and
    long-lived backend I/O executor directly into chunk and asset execution even
    on distributed data-only nodes. Hosted writers own one cache per hosted
    root. Durable chunking and other enrichment traffic therefore reuse bounded
    single-flight snapshots without constructing a threaded runtime or minting a
    lease for every document; stateless compatibility entry points retain local
    cache and I/O fallbacks.
130. **The generic text-embedding byte default serialized PDF page images.** A
    256-KiB total batch ceiling gave every candidate batch less per-page output
    allowance than its singleton, so the quality-preserving planner necessarily
    reduced every page-image window to one. PDF visual embedding now has a
    separate 64-MiB media default, clamped by explicit policy and the resolved
    model ceiling. Text batches retain their smaller default. Window reduction
    is driven by the actual transport, invocation-memory, pixel, and model byte
    limits rather than an unrelated text heuristic.
131. **Empty batches crossed some local boundaries as successful work.** The
    generic capability validator, encoded-reader contract, standalone bridge,
    and direct inference executor now agree that an invocation contains at
    least one item. High-level schedulers may still represent an empty plan as
    a no-op, but no model executor can turn an upstream cardinality bug into a
    successful durable publication.
132. **Remote embedders copied and fragmented capability-cache ownership.**
    Managed embedders now borrow the provisioned runtime cache, with an owned
    fallback only for standalone construction. Request-scoped cancellation
    overlays borrow the configured entry's cache instead of copying a live map
    and mutex by value. Distributed text and visual embedding therefore share
    the same synchronized discovery flights and renewable leases as the other
    task executors.
133. **Discovery and execution disagreed about explicit-pool precedence.** The
    proxy now derives both paths from the same `RoutingContext`: canonicalized
    headers, verified source identity, explicit fallback pool, and timestamp.
    Configured route policy takes precedence, followed by the explicit pool and
    then the process default. A capability lease therefore contains every pool
    that its eventual concrete request can reach, without letting an
    unauthenticated pool header bypass route policy.
134. **Source-scoped discovery trusted identity headers that execution
    ignored.** Organization, project, and API-key routing fields now come only
    from the configured `VerifiedSourceResolver`; the resolver is invoked for
    both model discovery and inference execution. Without one, those fields
    remain unknown during conservative discovery and empty during exact
    execution. The legacy table routing attribute remains a non-identity header
    fallback. Resolver failure rejects the request before catalog or model
    routing.
135. **An empty potential-pool list conflated fallthrough with a terminal
    route.** Potential planning now returns a structured `RouteCohort` with
    `Matched`, `Terminal`, and ordered `Pools`. A definite reject is terminal
    with no pools and cannot silently expose the default pool; a conditional or
    time-window route remains non-terminal and includes the explicit/default
    fallback that execution may later select.
136. **Multimodal reranking was routed as text reranking.** The
    `/rerank_multimodal` handler now preserves its concrete operation through
    route matching, endpoint operation inventory, capability fencing, workload
    classification, metrics, and forwarding. Text and multimodal reranking use
    separate operation-scoped leases and cache entries while retaining the same
    semantic task and typed result family.
137. **A route update could reinterpret an unexpired capability lease.** Every
    semantic route replacement or removal now advances a policy generation. Scoped
    discovery captures that generation in the cohort and lease identity;
    execution linearizes route matching against it. A mismatch returns the
    standard capability-stale conflict, causing clients to invalidate and
    rediscover immediately instead of repeatedly receiving an ordinary 503 from
    the intersection of a new route and an old endpoint set.
138. **Informer resyncs looked like route-policy changes.** The watcher now
    drops same-resource-version resync notifications before conversion, and the
    route manager independently compares the complete declarative policy before
    replacing it. `UpsertRoute` owns a deep immutable copy of every declarative
    map, matcher, time window, destination condition, fallback, and retry rule;
    neither the submitted object nor a returned match can mutate installed
    policy behind the generation fence. Equivalent updates preserve the
    generation and synchronized rate-limiter runtime. Only a semantic policy
    change invalidates capability plans.
139. **Retry-time resolution discarded stale-plan semantics.** Initial
    resolution, admission, and every retry now share one typed
    `ResolutionError` response path. Capability staleness is an explicit error
    property rather than an inference from status 409, so every model family
    receives the same conflict header and immediately invalidates its cached
    plan while unrelated conflicts remain ordinary conflicts.
140. **Catalog memory admission made concurrency an all-or-nothing constant.**
    Scoped discovery now derives its worker count from the configured retained
    byte ceiling after reserving the merge buffer. A limit that fits one worker
    processes any additional inference nodes sequentially; only a limit that
    cannot fit the merge buffer and one bounded response fails admission.
141. **Obsolete routing generations occupied the bounded lease table.** Lease
    issuance rejects a discovery generation that is already stale, rechecks it
    before publishing the token, and removes expired or older-generation
    entries before applying the capacity limit. Real route changes therefore
    invalidate execution without turning already-dead safety state into a
    five-minute availability leak.
142. **Authoritative discovery failures could resurrect an obsolete plan.**
    Model-catalog discovery now distinguishes capability-stale conflicts,
    permanent HTTP rejection or invalid contracts, and retryable transport or
    server failure. A routing-stale conflict revokes the exact cache entry
    before one retry inside the same single-flight, so a transient retry failure
    cannot resurrect the already-invalid tuple. Permanent failures never use
    stale data. Only explicitly transient discovery failure may reuse a
    still-bounded stale descriptor/token/revision tuple.
143. **Endpoint replacement left unusable leases consuming bounded capacity.**
    The registry now assigns a monotonic incarnation ID whenever an address is
    newly registered or its pool, health target, or workload class changes.
    Catalog fan-out captures that identity before network I/O. Lease identity
    and background model-refresh publication both fence on it, rather than on
    Go pointer formatting alone. Issuance linearizes against the registry
    topology, sweeps every lease containing a dead incarnation before its
    capacity check, and validation deletes a stale lease as soon as it observes
    replacement.
144. **The routing API obscured replacement semantics.** The route manager now
    exposes the deliberate breaking API `UpsertRoute(*Route) (bool, error)`;
    the boolean is true only for an installed semantic change and invalid
    declarative matchers return an error. Watchers and direct callers use that
    contract explicitly, so no compatibility shim can accidentally discard
    change detection, matcher validation, or imply append-only behavior.
145. **PDF stale-page cleanup retained a superseded vector-delete shape.**
    Embedding artifacts now remain the sole owner of their projection cleanup:
    stale PDF-page scans return bounded artifact keys only, and replay applies
    those deletes atomically. The old parallel `vector_keys` list is neither
    constructed nor freed, matching the artifact-backed chunk path and avoiding
    duplicate or prematurely visible projection deletion.
146. **Endpoint incarnation was discarded before reservation.** Capability
    validation now produces immutable endpoint references containing the exact
    endpoint allocation, incarnation, and circuit breaker. Candidate filtering
    rechecks all three under the registry lock, and selection atomically
    reserves that reference. Resolution completion retains the same breaker
    instead of looking one up by address, so an old request can neither execute
    against nor charge failure state to an address-reused replacement.
147. **Mutable registry exports bypassed endpoint authority.** Endpoint routing
    state is now private and topology changes replace an immutable endpoint
    identity while retaining only its shared atomic load counters. Raw registry
    lock, backing-map, and circuit-breaker accessors are removed; operators
    receive detached, stably ordered `EndpointSnapshot` values with copied,
    deterministic per-model operation catalogs. The public routing primitive
    is an owned `EndpointLease` with explicit success, failure, and release completion.
    Every routing mutation therefore remains inside the registry incarnation
    boundary.
148. **Capability invalidation raced successful single-flight completion.** An
    execution-side capability-stale response now marks the matching active
    discovery flight invalidated while removing the cache entry. A response
    already in flight at that boundary is retired and rediscovered; it cannot
    republish its older descriptor, routing token, or revision for either the
    owner or joined waiters.
149. **Compiled regex objects were not a declarative policy identity.** Route
    patterns now carry an expression and explicit leftmost-first,
    leftmost-longest, or POSIX syntax mode. `UpsertRoute` validates and compiles
    private programs for its immutable snapshot, while equality and generation
    changes compare the declarative fields. This removes dependence on
    `regexp.String()`, which does not encode compilation mode.
150. **Safe route ownership allocated on every proxy request.** The route
    manager now has a private installed-snapshot match path used by proxy
    discovery and execution. Exported match and resolution APIs still return
    detached copies, but ordinary HTTP forwarding reuses the immutable
    copy-on-write policy and compiled programs without configuration-sized
    allocations per request.
151. **Scale-to-zero and capability leases were competing routing contracts.**
    They now form one state machine. Scoped discovery renews every managed pool
    in the operation's reachable route cohort, waits only for pools that were
    actually cold, and then fans out the model catalog over the resulting exact
    endpoint incarnations. Execution may renew a selected pool already present
    in its lease, but it can never wake a new incarnation and smuggle it into an
    old plan. Legacy unscoped execution retains request-driven activation.
152. **Cold discovery discarded the namespace needed by the activator.**
    `RouteCohort` now carries namespace-qualified activation targets in
    addition to its registry pool projection. Route destinations and redirect
    fallbacks retain their route namespace; explicit-pool and process-default
    fallback use the immutable configured default scope, whose empty namespace
    is explicitly global. Activation waits use each pool's declared deadline
    without polling past it.
153. **A route could change while its cold capability cohort was waking.**
    Discovery re-derives the cohort after activation and accepts it only at the
    same route generation. Bounded retries cover ordinary informer races;
    continuing policy churn returns the standard capability-stale conflict
    instead of minting a lease for a mixture of generations.
154. **Dynamic destination checks and operation admission used different
    registry snapshots.** Condition statistics, capability-incarnation
    filtering, exact operation support, endpoint choice, and circuit-breaker
    reservation now linearize under one registry read lock. The resulting
    reservation retains the admitted endpoint and breaker identities through
    completion. Task-scoped routes never fall back to bootstrap or arbitrary
    pool endpoints merely because the requested model has no exact executor.
155. **A stale PDF worker could delete its replacement's private stages.**
    Request unwind now deletes only the exact attempt namespace it owns.
    Abandoned attempt keys are discovered without mutation and transferred
    with the replacement generation; promotion and garbage collection execute
    in one lease-fenced storage transaction. An old owner may observe newer
    keys, but losing the fence prevents it from deleting or publishing any of
    them. Stage discovery has a bounded scan-work ceiling and transfers at most
    one fixed-size garbage page per successful replacement, so cleanup memory
    and transaction size do not grow with crash history. The promotion/delete
    ownership transfer reserves both replay-window destinations before moving
    any key.
156. **Generation-batch routing materialized the entire typed request a second
    time.** The proxy now scans `requests` incrementally, retains only one
    item's routing model, rejects a 129th item at the OpenAPI `maxItems: 128`
    boundary, and never allocates a request slice proportional to attacker
    supplied cardinality. Oversized batches fail at the proxy before endpoint
    selection or forwarding.
157. **Transcription model defaults diverged across direct and distributed
    execution.** The public transcription schema now requires a nonempty model.
    The inference node checks it before admission, audio decoding, or model
    resolution; the direct linked boundary rejects an empty model as well; and
    Antfly transcriber adapters require the same identity before capability
    discovery. Generated SDK contracts, handwritten clients, and integration
    helpers expose the model as required rather than restoring an implicit
    default. Provider-specific defaults remain private to providers such as
    OpenAI and Vertex and can no longer alter Antfly routing semantics.
158. **Direct transcription compiled only through monorepo dependency
    injection.** The reusable inference runtime graph now constructs and
    exports its transcriber module, including the audio and S3 schema
    dependencies, and installs it in both the runtime and package test roots.
    The standalone inference package can therefore compile and test its direct
    transcription API without relying on a later root-build mutation.
159. **Paused managed-index activation dropped distributed capability state.**
    The reconfiguration boundary now accepts and forwards the shared remote
    capability cache into every recreated embedder and asset executor. Index
    activation therefore preserves the same resolved-model contracts as steady
    state execution, and the Linux storage/e2e build paths compile the identical
    distributed configuration instead of a reduced local-only signature.
160. **Data-only nodes classified URL-less asset producers as linked-local
    before applying the configured inference endpoint.** The durable boundary
    is now an immutable `InferenceExecutionContext`: explicit model URLs win,
    an actually available linked task callback wins for URL-less configs, and
    otherwise the provisioned default inference endpoint is selected before
    capability lookup, batch classification, foreground-bounded reporting, or
    invocation-memory admission. Reader, generator, extractor, and transcriber
    execution all consume that resolved route, so a distributed data node no
    longer fails with `InferenceInvocationMemoryUnavailable` merely because
    the model config intentionally omits a per-index URL.
161. **Rerankers constructed an isolated executor and capability cache for
    every query.** Query post-processing now receives the complete managed-read
    execution context, reuses the provisioned backend I/O executor and shared
    capability cache, and resolves the same default inference endpoint as the
    other model families. A private threaded executor and bounded cache remain
    compatibility fallbacks only for genuinely standalone callers.
162. **Capability discovery knew the authenticated request but lost its source
    table before distributed execution.** Trusted routing metadata is now a
    distinct internal context, never a user model-config field. The source
    table header participates in capability-cache identity and is sent on both
    discovery and execution for remote reads, generation, embeddings,
    reranking, extraction, and transcription. Persistent asset and embedding
    runtimes own their copied routing strings, preventing request-lifetime
    slices from escaping into background enrichment work.
163. **Antfly readers and rerankers admitted an empty routing model.** Both
    families now require a nonempty, trimmed model at their configuration
    admission boundary, matching transcription and the inference proxy.
    Provider-specific defaults remain available only to providers whose
    contracts define them; Antfly's distributed route identity is always
    explicit.
164. **Query-time reranking treated an optional backend executor as an
    unconditional I/O value.** Reranking now acquires a lifetime-fenced
    inference-lane lease from the backend runtime and uses a private threaded
    executor only for standalone construction. This both restores the complete
    Linux storage-kernel build and prevents shutdown from retiring the shared
    inference executor during an active query.
165. **The production hosted-read coordinator did not receive the distributed
    inference context.** Hosted reads now carry the configured inference
    endpoint, backend runtime, secret and remote-content services, and the same
    hosted-root capability cache as durable writes. Local-shard query opening
    and coordinator post-processing use that complete context, so a URL-less
    Antfly reranker or query embedder cannot accidentally target coordinator
    localhost.
166. **The shared execution context described cancellation and bounds but
    could not carry them, and a later request-context layer duplicated its
    nominal ABI type.** The unified execution-control module now distinguishes
    two composable lifetimes: a durable `ExecutionEnvironment` owns routing,
    capability-cache, I/O-pool, and provider defaults, while one canonical
    `RequestContext` borrows per-invocation I/O, absolute deadline,
    cancellation, and progress. Every task family and checked callback uses
    that same request type; the public `request_context` namespace is an alias
    of the canonical module and cannot define a second ABI. Reranker discovery and
    execution intersect those controls with finite family limits; generation
    forwards provider cancellation and the original progress sink rather than
    deriving a reduced callback context. A canceled or expired public query
    therefore bounds both catalog discovery and the admitted inference
    request.
167. **Durable semantic chunking retained only I/O and a capability-cache
    pointer.** Chunking now resolves explicit, linked, and provisioned-default
    routes through the same execution context, scopes discovery and execution
    by source table, binds the capability token and revision, and applies
    cancellation, deadline, and response limits. The durable chunk provider
    owns copies of endpoint and source-table strings, so request-scoped catalog
    slices cannot escape into background replay.
168. **Only batched page-image embedding consumed its capability lease.**
    Remote single-item multimodal, sparse-text, and dense-text embedding now
    discover and validate the concrete invocation, bind its routing token and
    descriptor revision, and invalidate the exact scoped lease on a stale
    response. Every Antfly embedding surface therefore uses the same
    model/operation/auth/source-table fence. Hosted execution borrows its
    runtime-owned concurrent I/O lane; standalone managed embedders own one
    concurrent executor for their complete lifetime, so request volume cannot
    multiply thread pools and bounded capability discovery never runs on a
    non-concurrent singleton event loop. Catalog-only dimension probes bind a
    scoped executor for the complete validation operation.
169. **Linked reranking and chunking dropped request controls at the callback
    boundary.** Their provider contracts now prefer contextual callbacks that
    receive the caller-owned I/O executor, absolute deadline, and semantic
    cancellation token. The versioned standalone provider ABI carries the same
    borrowed cancellation view and applies uniform pre/post dispatch checks;
    reranking and chunking also pass the absolute deadline into their direct
    inference entry points. Legacy callbacks remain compatibility fallbacks,
    but production standalone registration publishes the contextual variants.
170. **Remote transport timeouts were computed before capability discovery.**
    Reranking and every remote Antfly embedding path now capture one absolute
    operation deadline, use it for discovery, and recompute the residual
    transport timeout immediately before execution. Discovery latency can no
    longer reset or extend the owning query/enrichment budget.
171. **An unused provider-only enrichment set leaked after a successful plain
    managed DB open.** Enrichment construction now follows unconditional
    owner cleanup: `takeConfig` clears each transferred owner, and the local
    aggregate is always deinitialized on both success and error. The same rule
    applies to active and paused reconfiguration, while partial construction
    is guarded by one aggregate `errdefer`.
172. **Standalone remote embedding allocated a threaded executor per request.**
    A `ManagedEmbedder` without injected runtime I/O now creates one stable,
    heap-backed concurrent executor, shares it across all entries and calls,
    and destroys it only after entries and pacers drain. Remote request code
    fails closed when no owner-bound HTTP executor exists, preventing a future
    fallback from silently restoring thread-pool amplification.
173. **Only embedding honored the checked linked-provider callback ABI.**
    `AntflyProviderBoundary` is now the single public, versioned invocation
    boundary for every callback carried by `AntflyProvider`. Capability
    discovery, generation, reranking, reading, extraction, transcription, and
    model listing invoke through the provider's owning dispatch just like dense
    and sparse embedding. Hidden static runtime units therefore validate the
    method, callback signature, argument layout, and result layout before
    interpreting native pointers, and private Zig errors cross the boundary
    only through the stable status ABI. New model-family callbacks must be
    appended to the same provider vtable and may not introduce an unchecked
    direct-call path.
174. **Residual embedding deadlines were calculated but not applied to the
    `/embed` request.** The Antfly wire provider now constructs every JSON POST
    through one task-neutral request-control helper carrying the residual
    timeout, semantic cancellation, bounded response size, and routing headers.
    Dense text, sparse text, multimodal embedding, generation, and reranking
    cannot independently omit those controls when their wire encodings evolve.
175. **The generation batch OpenAPI description contradicted executable
    multimodal behavior.** The inference specification is the authoritative
    contract and now describes bounded image/audio admission plus per-item
    capability failures. Generated clients and joined public specifications are
    regenerated from that source; security-contract tests assert the same
    behavior instead of preserving a stale text-only promise.
176. **A merge-resolved content-security assertion was not repository-formatted.**
    The test is formatted by the pinned Python formatter and remains covered by
    the normal SDK formatting gate.
177. **Capability discovery normalized transient transport failures without
    preserving dimension-probe policy.** Managed embedding probes now classify
    `RemoteCapabilityDiscoveryTransient` as operational. An explicitly
    dimensioned index using `validation: defer_probe` can therefore persist its
    stable producer identity while a distributed inference catalog is briefly
    unavailable; authoritative rejection and malformed capability contracts
    still fail closed.
178. **Proxy construction enlarged explicit process memory ceilings.** The
    retained-body and retained-batch-response settings are authoritative
    process limits, not hints or requested minima. The proxy now intersects
    each logical per-request ceiling with the largest conservative physical
    reservation that fits its configured process admission. A conflicting
    configuration therefore lowers the effective request limit and emits a
    diagnostic; it never silently increases the operator's memory boundary.
179. **A discarded partition consumed the mixed-batch response budget.** The
    coordinator previously charged any bounded 2xx body before proving that it
    was a valid, identity-preserving batch envelope. Malformed and otherwise
    rejected responses are now released without entering retained-result
    accounting, so an untrusted partition cannot deny budget to a later valid
    sibling. Only validated partitions whose raw results remain referenced
    until ordered reassembly count against the aggregate logical limit.
180. **The proxy reported unknown upstream failures as pre-execution
    rejection.** `rejected_items` is reserved for work that is provably stopped
    before model execution. Encoding and local preflight failures may increment
    it; an upstream non-2xx, oversized response, or malformed success envelope
    does not reveal whether the model ran. If any partition has unknown
    execution state, the coordinator still returns typed per-item failures but
    omits the aggregate execution report instead of manufacturing telemetry.
181. **PDF OCR reserved logical media bytes instead of the physical invocation
    peak.** A 64 MiB PNG window can require substantially more retained memory
    when a distributed route builds a base64 body or a reader adapter builds
    data URIs, and request/response parsing has its own bounded allocator peak.
    PDF OCR now resolves the concrete producer route before admission and
    atomically reserves the transport-specific media peak plus the invocation
    plan's allocator limit beside native parser/render memory.
182. **A stale base64 precheck overruled route-specific admission.** The OCR
    loop rejected raw PNGs using base64 expansion even when the resolved linked
    route borrowed binary attachments, while still failing to describe the
    complete peak of remote routes. The generic invocation plan is now the
    sole transport authority; logical request bytes remain checked separately.
183. **OCR staging capacity followed an operator ceiling instead of pending
    work.** A tiny document, including a transcription-only unit, could reserve
    media arrays for an arbitrarily large configured batch. The scheduler now
    caps its control-plane batch at 128 items and allocates PDF media staging
    only for `min(pending_pages, admitted_batch_items)`; non-media tasks do not
    allocate those arrays.
184. **Batch capability was inferred from task/provider names instead of the
    concrete executor.** A resolved executor now supplies its implemented mode,
    preferred and maximum item counts, and per-item failure contract. Manifest
    capabilities may narrow that descriptor but cannot promote a serial loop to
    native execution. The mapping covers every public model family—reader,
    generator, embedder, reranker, extractor, chunker, rewriter, and
    transcriber—without claiming fused execution where it has not landed.
185. **Sibling document tasks downloaded and prepared the same source
    independently.** One pending document group now owns a credential-scoped
    `PreparedDocumentSourceCache`. It charges retained bytes to the global
    document working-set slice, reuses a download across compatible consumers,
    prepares the immutable PDF page/xref state once, and lends task-private
    render forks with independent mutable decoder caches. Cache entries are
    individually allocated so their addresses remain stable as new source or
    credential variants are discovered. A fixed SHA-256 identity is retained
    instead of duplicating a potentially large inline base64 source URL.
    Retained preparation is charged before transient task-window admission, so
    a peak reservation cannot starve the cache that it depends upon, and the
    complete cache dies before the document lease is released.
186. **Backend runtime construction eagerly allocated every specialized
    executor lane.** API, inference, control, and raft directions are now lazy
    lanes owned by `BackendRuntime`; the durable storage lane remains eager
    because background jobs need it immediately. First publication is
    serialized, shutdown prevents new activation and drains public lane leases,
    and an unused subsystem consumes neither lane state nor workers. The
    inference node also reuses its
    attached runtime executor across generators, readers, embedders, rerankers,
    extractors, classifiers, rewriters, transcribers, and warmup. Only an
    explicitly unattached direct/test invocation creates a call-scoped fallback.
187. **Encoded-output retries rerendered a PDF page.** A worker now paints the
    page exactly once. If the encoded PNG exceeds its byte ceiling, it decodes
    that raster once and performs a bounded sequence of strict-progress
    bilinear reductions, each sampled from the original pixels to avoid
    cumulative quality loss. The worker allocator charges the original PNG,
    decoded raster, resized raster, encoder scratch, and candidate output, and
    cancellation is checked throughout the resize.
188. **Enrichment configuration cleanup dropped an owned chunk-provider
    routing table.** Uninstalled and partially installed configuration now
    deinitializes the provider before clearing it. The full database suite uses
    the testing allocator and verifies zero leaks, including the focused
    ownership regression.
189. **Integration fixtures bypassed the production capability contract.** The
    PDF OCR test server now publishes the same versioned reader descriptor that
    planning requires before accepting work, and the local embedder test names
    its required model explicitly. CI no longer depends on a production
    discovery bypass for catalog-less fake endpoints.

### Post-review implementation contract

The hardening above follows these long-term rules:

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

## Migration sequence and implementation status

1. **Implemented:** shared task, work identity, borrowed attachment,
   capability, batch-mode, and execution-report contracts were added without
   changing durable artifact keys.
2. **Implemented:** local reader batching is selected from resolved model
   capabilities. Native and serial-compatibility modes are distinct, and OCR
   profiling no longer labels an accepted serial batch as native.
3. **Implemented:** provider ABI v23 separates borrowed binary payload storage
   from logical attachment references. One generator item may own several
   attachments, while read and embedding batches retain independent item,
   source, and page identity. Reader, generator, embedder, and extractor host
   paths borrow the same representation; remote transports encode only at the
   HTTP boundary and explicitly select their resident representation for
   admission. The version also makes capability v2 media-limit semantics and
   executable chunk and rewrite operations, the internal classification
   callback, and borrowed extraction an explicit
   host/component compatibility boundary.
4. **Implemented:** document OCR selects `reader` or `generator` explicitly.
   The generation batch endpoint accepts bounded multimodal requests and uses
   controlled serial execution for projector/session safety until a resolved
   model advertises native multimodal generation batching. Embedded local
   generators use the same bounded batch boundary and report
   `serial_compatibility` while invoking page messages synchronously.
5. **Implemented:** dense multimodal embedders expose a true
   `embedDensePartItems` operation with strict one-vector-per-item cardinality.
   A bounded page window can therefore enter ClipClap as one item per rendered
   page. `embedRenderedPdfPageBatch` performs that connector while retaining
   page numbers and render failures. The durable `pdf_page_images` input plans
   one admitted render/inference window at a time, persists one vector artifact
   per stable page key, publishes it to each consuming index, and removes stale
   vectors when the document loses pages. Each page artifact fingerprints the
   exact rendered image plus semantic model configuration, never only the
   source URL. Page outputs are written to a private staging namespace. A retry
   clears that request's prior staging records, all page failures are checked,
   and only a complete attempt promotes artifacts and performs stale-page
   cleanup. A partial attempt therefore cannot overwrite or delete the last
   complete public page-vector set; the normal durable retry supervisor either
   retries it or records terminal repair debt.
6. **Implemented:** capability lookup participates in reader, generator,
   dense-embedder, and extractor planning. The same normalized catalog covers
   rerank, chunk, rewrite, and transcribe even when their current executor is
   singleton or serial compatibility. Classification-capable models use the
   extraction descriptor. MIME acceptance, item count,
   encoded bytes, decoded pixels, media cardinality, and result cardinality are
   validated at executor boundaries. Unknown remote capabilities remain
   conservative.
7. **Implemented:** observed reader execution propagates from the native
   pipeline through the standalone boundary. OCR telemetry distinguishes
   native completion, compatibility serialization, and native-to-serial
   fallback instead of logging the preflight prediction. Mixed completion
   retains exact native, serial, fallback, and native-batch counters; aggregate
   reports are never forced into a single invocation mode.
8. **Implemented locally:** prompt policy is resolved during configuration
   parsing, and execution no longer guesses prompt semantics from a model name.
   Model-name detection is confined to backward-compatible config migration.
   Remote Antfly execution discovers resolved inputs, normalized native-batch
   support, and model-owned limits from `/ai/v1/models`; discovery failure uses
   conservative serial/single-item behavior instead of guessing.
9. **Implemented for durable visual embedding:** fused preparation reuses the
   cached immutable PDF session, and a single controlled prefetch overlaps
   rendering window N+1 with inference and staging for window N. Both live
   windows are independently admitted against the same process budget; an
   admission miss falls back to sequential execution after releasing N.
10. **Implemented for deterministic CI:** the production-mode two-page PDF
    fixture traverses reader, generator, and embedder contracts and verifies
    native reader batching, honest serial generator reporting, and one visual
    vector per page. `zig build pdf-model-qualification-test` is the opt-in
    real Florence, Gemma4, and ClipClap release gate; it requires the endpoint,
    three resolved model names, and embedding dimensions listed under Testing.
    Model bundles and accelerator backends remain outside hermetic CI.
11. **Implemented after review:** remote capability snapshots are cached and
    single-flight in both the asset producer runtime and managed multimodal
    embedder. Reader, generator, and per-page embed calls enforce the resolved
    limits at the executor boundary and partition oversized valid windows.
12. **Implemented after review:** `ProducedBatch` contains typed per-item
    values or failures with exact work identity. Remote read adapters restore
    page provenance, remote generator failures no longer discard successful
    siblings, and enrichment consumes each item exactly once.
13. **Implemented after review:** PDF page embedding stages use a dedicated
    typed internal namespace. The generated-record writer reads complete stage
    values and atomically commits their canonical keys, stale artifact deletes,
    counter changes, and the replacement replay record. Stage keys are deleted
    only by that commit, and their values remain transaction-borrowed during
    ordinary publication instead of being accumulated in memory.
14. **Implemented after review:** request-atomic PDF publication has explicit
    page-count admission (`execution.max_document_pages`, default 2,048,
    absolute maximum 16,384). The effective limit is the minimum of the public
    request, operator ceiling, and absolute ceiling; a request cannot raise an
    operator setting. Invisible stage cleanup uses fixed-size delete pages,
    stale-artifact maintenance streams keys and counts only the selected
    embedding, and allocation transfer is failure-safe.
15. **Implemented after review:** remote read and generation responses carry
    optional observed execution reports. Clients validate and preserve mixed
    execution counters, preserve backward compatibility as serial execution,
    and never upgrade telemetry from a capability prediction. Reader reports
    are validated against every physical image chunk before aggregation, so
    malformed local reports cannot cancel each other out.
16. **Implemented after review:** reader URI admission measures decoded base64
    data URIs and validates MIME before local callback or remote adaptation.
    Generic work contracts can represent a document modality, but current
    generator and embedder executors deliberately do not advertise raw PDF;
    the document planner must first produce bounded page images or text chunks.
17. **Implemented after review:** capability single-flight waiters observe
    cancellation/deadlines, catalog fetches have a finite timeout, and cache
    teardown drains in-flight owners instead of asserting they do not exist.
    Owner cancellation is wired into the catalog HTTP request itself and is
    rechecked after discovery; an abandoned owner releases unrelated waiters to
    retry under their own contexts. Cancellation/timeout propagate through
    managed embedders, and every absolute deadline shares one monotonic clock
    domain on Darwin and other platforms.
18. **Implemented after review:** stale page-artifact discovery remains bounded
    by both total scan work and selected-model fanout, uses a hash set for desired
    pages, and appends canonical scan keys in linear time rather than performing
    quadratic list de-duplication at the page ceiling.
19. **Implemented after distributed review:** the proxy exposes model catalog,
    read, generation-batch, and embedding operations. Nested batch model
    routing and conservative catalog merging keep discovery consistent with
    the node that can execute the complete bounded request.
20. **Implemented after wire review:** generation consumes generic borrowed
    image/audio media end to end, item failures retain retry metadata, and
    execution accounting distinguishes attempted serial/native work from
    parser/admission rejection.
21. **Implemented after durability review:** PDF stage namespaces are unique to
    one lease epoch and request attempt. Lease heartbeats surround render and
    inference windows, while final promotion is transactionally fenced by the
    live lease record. Takeover and stale-release regression tests cover both
    different and reused owner identities.
22. **Implemented after lock-order review:** exact split-delta admission uses a
    non-reclaiming resource reservation while the storage transaction is held;
    regression coverage proves no reclaimer callback can run on that path.
23. **Implemented after distributed routing review:** endpoint catalogs retain
    task identity, routing and retry are operation-aware, known-missing models
    no longer use pool fallback, and partial scoped catalogs are published only
    with an endpoint-constraining lease that excludes every omitted node.
24. **Implemented after admission review:** generic generator media participates
    in batch byte, image-header, dimension, decoded-pixel, and slot admission;
    the advertised media-part ceiling is a hard parser limit.
25. **Implemented after capability review:** model manifest strings may tighten
    limits but cannot promote a serial executor to native batching.
26. **Implemented after proxy-memory review:** inference request bodies use a
    configurable hard retained-byte ceiling for both known and chunked lengths.
27. **Implemented after discovery-memory review:** catalog fan-out concurrency,
    per-node bytes, and merged catalog bytes are independently bounded.
28. **Implemented after model-family review:** the normalized capability and
    distributed discovery contracts cover every current model family. Existing
    task-specific executors remain distinct, and unbatched families publish a
    conservative singleton capability until their own batch ABI exists.
29. **Implemented after contract review:** one shared server/embedded resolver,
    exact capability wire V3, mixed-version chunk alias normalization,
    capability-aware extractor windowing, borrowed extractor attachments, and
    shared HTTP/linked validators remove the remaining deployment-specific
    interpretations of the model-work contract.
30. **Implemented after exact-contract review:** task/executor modality
    intersection removes false raw-document claims; extraction content has one
    normalized admission shape and exact batch demultiplexing; proxy V3
    descriptors are validated before singleton publication or intersection;
    legacy task semantics and extractor pixel ceilings match the concrete
    executors.
31. **Implemented after invocation-memory review:** reader, generator, extractor,
    and part-item embedder routes publish a shared invocation-memory plan. PDF
    OCR and visual embedding form render windows from that plan, data-URI
    adaptation charges its retained URI and downstream JSON copy, and missing
    media plans fail closed.
32. **Implemented after serialization/URI review:** Antfly generation batches
    measure then directly emit non-metadata content without retaining a
    canonical copy for every request; base64 is written into the one exact body.
    Direct inference-node media decoding accepts the same validated RFC 2397
    base64 and percent forms as host preflight.
33. **Implemented after enforcement review:** media executor plans now carry
    explicit allocator ownership plus result ceilings. Public boundaries hard
    bound caller-owned adapters, while linked/distributed inference nodes admit
    their own decoder/model work and return through independently validated
    result caps. Distributed generator fallback is admitted in the process
    that executes it; provider response ceilings are operation-scoped; and all
    inline-data consumers share the RFC 2397 parser while keeping MIME
    acceptance and ownership as explicit task-layer contracts.
34. **Implemented after representation review:** producer plans cover every
    runtime invocation rather than only the binary `media` field, transcription
    and structured-part routes fail closed without a contract, URL-only dense
    embedding is bounded before normalization, route discovery has its own
    request-derived allocation ceiling, and logical output policy is reconciled
    with worst-case JSON wire expansion. Batches with different tasks,
    configurations, or attachment representations cannot inherit the first
    item's plan.
35. **Implemented after boundary-completeness review:** every model-backed
    producer requires a plan even when legacy reader input is carried only in
    `source_text`; result policy is enforced per item and in aggregate; dense
    vectors are individually dimensioned and finite; heterogeneous dense-part
    shapes account each variant/string without a common-MIME guess; UTF-8
    repair uses an exactly sized allocation and accounts its coexisting
    structures; and validated MIME essence handling is shared across
    capability, host, linked, and inference-node boundaries.
36. **Implemented after executor-parity review:** the inference node derives a
    generic resolved executor contract for reader, generator, embedder,
    reranker, extractor, rewriter, classifier, and transcriber routes. Typed
    adapters populate the common item, text/token, output, candidate, schema,
    media-part, encoded-byte, and decoded-pixel dimensions; model-specific
    limits are hard execution checks rather than discovery-only hints.
37. **Implemented after remote-codec review:** version 4 remote image MIME
    extensions must be measurable by the local shared inference codec registry
    before the capability snapshot can become routable.
38. **Implemented after renderer-ownership review:** durable PDF page-image
    embedding acquires an operation-scoped native scratch reservation and
    allocator-owned output credit before constructing its render coordinator.
39. **Implemented after proxy-admission review:** retained-body admission is a
    cancellation-aware aging weighted semaphore with impossible-weight
    rejection. A small request may consume otherwise idle slack behind an
    oversized head only a bounded number of times; the aged head then becomes
    a barrier, preserving utilization without large-request starvation.
40. **Implemented after distributed TOCTOU review:** operation-scoped routing
    is eligible only from a current endpoint catalog; bootstrap pool fallback is
    task-unscoped and cannot execute any model-family request.
41. **Implemented during the semantic rebase:** every dense artifact hook uses
    artifact-first lookup, including invocation-memory planning; query lookup
    precedence remains confined to query execution.
42. **Implemented after endpoint-lifecycle review:** registration wakes an
    event-driven catalog refresh worker even when periodic refresh is disabled.
    Initial startup also refreshes immediately, and refresh completion is
    pointer-fenced so an unregistered endpoint or a new endpoint reusing the
    same address cannot be mutated by an older in-flight request.
43. **Implemented after conservative-fallback review:** PDF page-image
    embedding no longer invents a 256-MiB media ceiling when remote discovery
    omits one. An explicit operator batch-byte ceiling may safely supply the
    missing host bound; otherwise unknown encoded-media capacity is a retryable
    capability gap. It cannot create an impossible reservation equal to the
    default whole inference budget and terminalize otherwise recoverable work.
44. **Implemented after allocator-ownership review:** a PDF embedding window
    reserves renderer scratch separately from the complete caller-owned output
    peak. The output grant includes retained raw PNG bytes, exact base64/request
    transport residency, fixed provider/result allocation, and vector result
    capacity. Window formation derives its raw-media limit by inverting that
    same accounting instead of treating wire bytes as total resident bytes.
45. **Implemented after capability-TOCTOU review:** scoped model discovery now
    returns a bounded opaque capability token. The proxy validates its
    model/task/concrete operation, route-policy generation, authorization
    digest, revision header, and eligible endpoint incarnations before every
    initial route and retry, then intersects the configured execution pool.
    Remote capability caches retain the
    token with the normalized descriptor; reader, generator, dense embedder,
    reranker-provider, and extractor HTTP boundaries can carry it. A stale
    response invalidates the exact model/task/operation/auth cache entry so
    durable retry rediscovers and replans.
46. **Implemented after catalog-admission review:** cluster model-catalog
    fan-out is covered by a process-wide weighted byte admission in addition to
    per-response and merged-output limits. The reservation accounts for the
    merged catalog plus each concurrent raw body, decoded catalog, and task
    inventory parse, rejects an impossible request immediately, and observes
    request cancellation while queued.
47. **Implemented after completed-flight invalidation review:** capability
    invalidation poisons both active discovery owners and completed flights
    retained by existing waiters. A completed flight keeps its reference-counted
    allocation until those waiters release it, but its capability value, routing
    token, and descriptor revision are cleared immediately. New callers cannot
    join the tracked completion and resurrect a tuple that execution has
    authoritatively revoked.
48. **Implemented after route-input validation review:** route installation is
    a strict compile boundary. Nil model patterns and header matchers are
    rejected before publication, invalid declarative regular expressions remain
    errors, and a rejected route does not advance the routing-policy generation.
    Matching can therefore operate on immutable, fully compiled policy without
    defensive hot-path allocation or nil-dependent semantics.
49. **Implemented after linked-runtime verification review:** the encoded-reader
    ABI enforces the same non-empty batch invariant at structural decode and at
    execution. Its production-linked round-trip test now expects the decoder to
    reject an empty attachment set instead of treating an invalid batch as a
    valid codec-only value.
50. **Implemented after scale-to-zero discovery review:** conservative catalog
    membership and activation are separate concepts. Discovery may retain every
    namespace-qualified pool that a partially specified request could reach,
    but it activates only the exact matched route's weighted destination. A
    redirect fallback is attempted after an unavailable or timed-out cold
    destination, so one broken optional GPU pool cannot suppress a healthy CPU
    executor. Unknown header/source branches remain passive and cannot wake
    unrelated tenant workloads.
51. **Implemented after namespace-isolation review:** an endpoint's routing
    identity is `(namespace, pool, address, incarnation)`, not a bare pool name.
    Kubernetes pod, EndpointSlice, and external-pool discovery preserve the
    source namespace through catalog construction, route-condition statistics,
    circuit admission, execution, and retry. Same-named pools in different
    namespaces cannot enter each other's capability leases. The legacy empty
    namespace is an explicit global standalone scope rather than an accidental
    loss of Kubernetes identity. The process default is one immutable
    `RoutePoolTarget`, not independently mutable pool and namespace strings:
    its zero value disables fallback, while `{Namespace: "", Pool: "gpu"}`
    explicitly selects a global standalone pool. Watcher construction never
    rewrites that choice. Unscoped model discovery without an explicit or
    configured default pool fails closed instead of merging a cluster-wide
    cross-namespace catalog.
    Route policy carries `Namespace` separately from `Name`; a slash in `Name`
    has no namespace-scoping meaning. The complete `(namespace, name)` identity
    is deliberately the stable salt for deterministic weighted selection, so a
    route rename is an identity change and may redistribute traffic. Route
    installation, equality, ordering, removal, cohort discovery, activation,
    and execution use `(namespace, name)` or `(namespace, pool)` as appropriate,
    so programmatic and informer-installed routes have identical namespace
    semantics.
52. **Implemented after route-generation review:** potential cohort discovery
    and exact activation matching bind to the same immutable route generation.
    A mutation observed while constructing that plan causes no activation. A
    mutation during an already-started activation may complete that harmless
    wake-up, but bounded preparation publishes no stale capability lease and
    retries against a fresh snapshot.
53. **Implemented after circuit-ownership review:** every API that can claim a
    half-open circuit probe returns an owned `EndpointLease`, never a bare
    endpoint. Success, failure, or release completes the exact endpoint
    incarnation and breaker reservation once, preventing an abandoned exported
    selection from permanently occupying half-open admission.
54. **Implemented after build-graph review:** focused Zig test roots declare the
    complete import closure they compile. `index-manager-test` now imports the
    JSON, scraping, and image modules used transitively by document extraction,
    and the full target is part of verification rather than relying on a larger
    build step to hide missing root imports.
55. **Implemented after observability-isolation review:** endpoint health,
    model-load, queue-depth, active-connection, request-count, and latency
    metrics use the qualified `namespace/pool` label for Kubernetes endpoints.
    Global standalone endpoints retain their existing bare-pool label. Bare-pool
    compatibility views are inspection-only and are never fed back into
    distributed selection or per-tenant accounting.
56. **Implemented after CI build-closure review:** caller-side reader planning
    no longer imports the heavyweight inference server merely to obtain its
    microbatch cap. A language-neutral reader policy owns defaulting and
    clamping; both the asset planner and Florence executor apply it to the same
    process environment value. Distributed storage kernels therefore retain
    their inference-free dependency boundary while advertising exactly the cap
    enforced by a linked local inference runtime.
57. **Implemented after result-envelope review:** `ProducedBatch` is a strict
    request-scoped boundary. It centrally validates report arithmetic, one
    execution item and result per submitted request, and the complete item,
    source, and page identity before any consumer can observe a result. Invalid
    envelopes are destroyed while still owned by the producer boundary. Reader
    requests containing several images remain supported through compatibility
    execution, but cross-request native batching accepts one image/page per
    work item so nested image cardinality cannot be mislabeled as outer request
    telemetry. Task executors may retain finer-grained model telemetry in their
    typed protocol responses instead of overloading the shared envelope.
58. **Implemented after retry-atomicity review:** generated reader and generator
    batches preflight every per-item retryable failure before applying any
    successful sibling to the document-unit cache. Output parsing, OCR-quality
    selection, and deterministic failure materialization occur on owned unit
    clones; the complete batch is swapped into place only after every stage
    succeeds. Singleton compatibility execution uses the same transactional
    unit replacement. A request-wide yield can therefore retry intact state
    without transient partial mutation; deterministic item failures still
    remain isolated and valid siblings are consumed exactly once.
59. **Implemented after informer-lifecycle review:** distributed route deletion
    resolves both ordinary informer objects and
    `DeletedFinalStateUnknown` tombstones. A missed Kubernetes delete can no
    longer leave a stale route policy, generation, or capability-routing path
    installed indefinitely.
60. **Implemented after end-to-end execution-context review:** hosted reads,
    reranking, semantic chunking, and every remote embedding variant now use
    one route-aware, deadline/cancellation-bounded, capability-lease-fenced
    execution contract. Persistent adapters own any routing strings that
    outlive an invocation, while query adapters hold an inference-lane lifetime
    lease for the complete remote call.
61. **Implemented after invocation-lifetime review:** asset execution receives
    an immutable invocation-scoped context instead of mutating a shared
    producer. Planning, capability discovery, memory resolution, batch-mode
    selection, and execution observe the same I/O owner, absolute monotonic
    deadline, cancellation token, and response ceiling. Foreground and
    background calls may therefore overlap safely with distinct controls.
    Invocation controls intersect configured runtime controls, so a scoped
    caller can only tighten a deadline, cancellation condition, or response
    ceiling and can never erase the runtime's existing safety contract.
    Remote reader, generator, extractor, and transcriber adapters recompute a
    residual timeout and pass cancellation into the live HTTP request. The
    linked provider exposes append-only context-aware generation, reading,
    extraction, transcription, and capability callbacks; the standalone
    bridge carries those controls through its existing invocation deadline and
    borrowed cancellation view. Legacy callbacks remain usable as compatibility
    execution but cannot claim the foreground-bounded contract.
62. **Implemented after generic batch-consumption review:** every generated
    asset consumer uses the authoritative `ProducedBatch` envelope. A valid
    per-item failure is no longer converted into a request-wide error followed
    by destructive singleton replay. Retryable items are preflighted before
    any sibling mutation and retain the provider's `retry_after_ms`; a
    deterministic item failure is durably isolated while each successful
    sibling is applied and freed exactly once. Sequential fallback is reserved
    for unsupported batching, a request-wide execution failure, or a malformed
    envelope whose results have already been destroyed at the producer
    boundary.
63. **Implemented after distributed mixed-batch review:** the proxy accepts the
    same mixed-model generation batch contract as direct inference nodes. It
    authenticates and admits the outer body once, stably partitions items by
    resolved model, and executes independent partitions with bounded
    concurrency. Each
    partition performs ordinary operation-aware routing and admission against
    a fresh route snapshot; an outer capability token is removed because it
    cannot fence several model routes. Results and typed failures are validated
    against partition-local identity, restored to original indexes, and merged
    into one ordered response. A failed model partition does not discard
    successful partitions. Retained response bytes are capped in aggregate
    across all partitions and admitted through a separate process-wide weighted
    semaphore with allowance for capture growth, JSON decoding, ordered result
    retention, and final encoding, so neither model fanout nor concurrent mixed
    requests can multiply memory. Homogeneous batches retain the direct
    streaming proxy path. Framed attachment batches are intentionally
    homogeneous: a capability lease belongs to one model, and partitioning a
    binary body would copy large attachments and ambiguously apply one lease to
    multiple backends. Producers group framed work by model before transport.
64. **Implemented after exact-operation lease review:** singleton generation
    discovers, caches, validates, carries, and invalidates only the `.generate`
    capability lease it executes. Batch planning continues to use the distinct
    `.generate_batch` lease. No ignored batch discovery can precede a singleton
    call, so operation-specific limits, route eligibility, revisions, and stale
    fencing remain coherent from plan through execution.
65. **Implemented after process-admission review:** explicit retained request
    and mixed-batch response limits remain authoritative. Their logical
    per-request limits are reduced to the greatest values whose conservative
    three-copy request or four-copy response reservations fit, with a startup
    diagnostic identifying the configured and effective limits.
66. **Implemented after retained-result review:** mixed-model response bytes
    become retained only after JSON, cardinality, identity, summary, and
    execution-envelope validation succeeds. A discarded partition releases its
    capture and cannot consume the logical aggregate budget needed by a valid
    sibling partition. Each active partition receives an independent fair share
    of one process-admitted capture budget; only validated envelopes enter the
    request-wide retained-result budget.
67. **Implemented after execution-telemetry review:** the proxy counts only
    provable local pre-execution failures as rejected items. Upstream status,
    oversize, and malformed-envelope failures make aggregate execution state
    unknowable, so the ordered response preserves typed item failures while
    omitting its execution report.
68. **Implemented after PDF invocation-admission review:** OCR pre-admission
    builds a bounded representative page batch for the resolved reader,
    generator, or extractor route and uses its attachment transport and
    allocator ceiling to calculate conservative physical output credit without
    requiring remote discovery before PDF inspection. Execution subsequently
    applies model capabilities. Native renderer scratch and allocator-owned
    invocation memory remain independent owners, so one is never numerically
    subtracted from the other.
69. **Implemented after allocator-lifetime review:** retained document state
    and transient PDF invocation memory have distinct BudgetedAllocators. The
    atomically reserved secondary credit transfers only to a window-scoped
    transient allocator. Each render/inference window owns one composite lease
    covering native scratch and its complete transport/provider peak, and
    releases both sides before another window competes for capacity. Completed
    text is admitted independently and cannot consume the next window's
    guarantee.
70. **Implemented after transport and staging review:** the legacy unconditional
    base64 gate is removed. Exact logical request limits and the resolved
    transport planner govern admission, while PDF-only staging arrays are
    sized from actual pending work under a process-wide control-item ceiling.
71. **Implemented after repeated-work review:** one prepared PDF reader now
    owns inspection, page metadata, and the immutable xref/trailer state used
    by OCR render forks. Final resolved units are serialized once into a
    bounded, attempt-private hybrid spool and replayed one unit at a time for
    artifact materialization and publication. Attempts that remain below both
    the 128-unit and four-MiB write thresholds replay directly from admitted
    memory; larger unit streams spill in bounded store batches. The old three
    extraction walks are therefore one transformation walk plus two cheap
    typed replays. Spill writes are
    transactional but intentionally do not force durability or enter artifact
    accounting; a retry prefix-clears every stale attempt for the leased
    document artifact before writing, and every exit performs best-effort
    cleanup. The prepared reader, downloaded source, and native render
    reservation are released after the input-unit spool is sealed and before
    bounded materialization/publication replay begins.
72. **Implemented after renderer metadata review:** render workers borrow the
    prepared document's immutable page index, xref entries, and trailer rather
    than cloning structures proportional to the PDF for every page. Each fork
    still owns mutable font, image, encryption, decode, and diagnostic caches,
    so sharing does not weaken thread isolation. Serial preparation publishes
    an explicit immutable fork template, including the encrypted-stream
    identities; executor threads instantiate private readers only from that
    template and never call methods on the shared source Reader. Admission
    charges a bounded fork-control allowance instead of charging the source
    document once per worker.
73. **Implemented after render scheduling review:** production render calls
    submit each admitted wave to a lazily activated, backend-runtime-owned PDF
    lane. It has a fixed physical worker set, a bounded queue, thread-confined
    reusable arenas, a lane-wide physical scratch ceiling, and a per-window
    scratch ceiling. Windows therefore do not construct thread teams and
    concurrent documents cannot multiply pools. Standalone library callers
    without a runtime retain the call-scoped bounded worker fallback. The
    immutable prepared document survives across waves, while each page still
    receives private mutable render state. Planned adaptive geometry is passed
    into the first render attempt, avoiding duplicate page-box/rotation work;
    geometry is recomputed only for a quality/size retry.
74. **Implemented after output-credit and storage review:** page-image
    embedding and OCR use fair window-scoped composite leases instead of
    pinning their maximum output credit for a whole document. Rendered media,
    provider request/response storage, and result parsing retain the same lease
    identity until the consumer finishes, after which scratch and output credit
    are released together. Attempt-private page embeddings are committed once
    per bounded render window rather than once per page. These changes remove
    allocator races and writer-transaction amplification without retaining
    more than one window.
75. **Implemented after hot-path allocation review:** OCR and visual embedding
    choose the largest admissible prefix with monotonic binary search rather
    than decrementing one page at a time. Generic image, CLIP, and Florence
    batches assign deterministic final tensor slices before dispatch and write
    into them directly, eliminating one full float tensor allocation and copy
    per image. Supported baseline color JPEGs additionally decode component
    planes and write normalized CLIP CHW values into the caller-owned final
    slice without materializing an intermediate RGBA image; unsupported JPEG
    variants and other formats retain the bounded decoded-image fallback.
76. **Implemented after renderer performance review:** render-output retries
    shrink from the raster's measured dimensions and therefore make strict
    progress even when the configured ceiling is much larger than the page.
    The page is painted once; bounded candidate encodings are generated from
    one decode of the original rendered PNG rather than rerendering the PDF.
    One atomic document-batch allocator is the hard aggregate boundary for
    allocations made through the native Zig render forks, while per-lane
    limits remain safety rails; page-wave admission also reserves each page's
    decoder working-set allowance without charging the entire document decoder
    ceiling to every worker. This allocator cannot observe CoreGraphics or
    CoreFoundation's internal framework allocations. The macOS compatibility
    backend therefore borrows the PDF source through no-copy `CFData`, retains
    one document session, serializes compatibility pages whose CoreGraphics
    concurrency/retention contract is unknown, and keeps their caller-owned
    RGBA/PNG buffers under the allocator and pixel ceilings. The surrounding
    process reservation conservatively accounts for framework work, but peak
    RSS qualification—not `BudgetedAllocator`—is the enforcement evidence for
    memory allocated internally by Apple frameworks.
77. **Implemented after lease, LMDB, and publication review:** every potentially
    blocking document/media producer keeps its enrichment tenure alive through
    complete materialization and final publication, not only through PDF OCR.
    The renewal task owns a cloned lease backed by the process thread-safe
    allocator; it performs parsing/stringification outside the runtime mutex and
    publishes only the renewed scalar expiry under that lock. Every private
    spool mutation and public promotion validates the exact owner/epoch/expiry
    inside its write transaction. Expired attempt prefixes are scavenged before
    fingerprint fast paths, downloaded PDF bytes and immutable preparation are
    retained once for compatible tasks in the pending document group, and
    spilled PDFs retain no duplicate generated-unit cache. Unit replay copies
    a count- and byte-bounded segment, closes its LMDB read transaction, and only
    then invokes a sink that may write. Materialization writes unit, chunk,
    navigation, state, and manifest values solely to an attempt-private
    publication spool; a failure before sealing cannot overwrite the prior
    generation or enqueue searchable work. After complete materialization,
    bounded replay windows atomically promote each staged artifact with its
    matching derived/outbox obligation, with the converged manifest staged last.
    Mixed born-digital and scanned pages preserve ordered slots while coalescing
    compatible OCR candidates.
78. **Implemented after repeated-tokenization review:** a generation batch
    resolves one provisional immutable manifest/contract per requested model.
    It rejects known-invalid raw envelopes before base64 decoding or model
    loading, caches the exact encoded-byte and decoded-pixel measurements used
    to plan bounded windows, then revalidates those measurements against the
    exact loaded artifact generation before model admission. Each admitted item
    retains its formatted and tokenized prompt for execution. Reranking and
    classification-backed extraction expose typed prepared values that own
    their encoded rows, exact token counts, execution permits, and
    pipeline-generation identity. Classification additionally snapshots label
    contents and order so borrowed result labels cannot be re-attributed after
    preparation; late-interaction reranking binds every tokenization control,
    including single-text framing and BOS insertion. Admission, usage
    accounting, and execution consume that same preparation instead of
    tokenizing again.
79. **Implemented after remote-client review:** a managed embedder owns a
    persistent keep-alive HTTP client for its lifetime. The client shell follows
    the embedder owner, while request, response, pool, DNS, and TLS allocations
    use a process thread-safe allocator so concurrent calls never borrow a
    request arena for shared state. Capability discovery, Antfly, Bedrock, and
    OpenAI-compatible embedding calls reuse that transport with cookies
    disabled. A Bedrock operation creates one absolute cancellation/deadline
    context and spends its residual budget across cache waits, STS/ECS/IMDS
    credential acquisition, every internal model batch, and the final request.
    Cache-owned credential snapshots likewise use thread-safe lifetime storage;
    caller-owned result clones retain their caller allocator. Constructor
    ownership transfer is allocation-transactional: failure after pacer or
    transport creation releases every shared reference and owned allocation.
80. **Implemented after model-catalog API review:** classification remains an
    extraction executor and is absent from the public `/ai/v1/models` family
    map. CI asserts that clients discover it through `extract` rather than
    reviving a parallel `/classify` surface.
81. **Implemented after preprocessing review:** generic image, CLIP, and
    Florence batches preprocess in deterministic input-indexed waves on the
    caller's backend-runtime-owned inference executor. Worker count is capped
    at eight, the runtime caps both `async` and `concurrent` fan-out (with the
    async ceiling also limited by detected CPU count), and the final job runs
    inline to guarantee progress. Offline callers with no runtime use a serial
    fallback and never activate a hidden process-global pool. Every codec
    allocation in a wave (decoded pixels, PNG scan buffers, progressive-JPEG
    coefficient state, and other scratch) is suballocated from one reusable,
    thread-safe request slab. The slab starts from the current admitted wave's
    decoded demand plus bounded worker headroom, grows geometrically only
    between waves with zero live allocations, retires old backing before a
    replacement is allocated, and never grows past the 128-MiB default physical
    boundary. A cap-exhausted wave reduces concurrency; successful waves grow
    worker width back toward the configured limit so one scratch-heavy prefix
    cannot serialize a cheap tail. Caller-owned final tensor storage is
    admitted separately and receives disjoint indexed writes.
    Florence consumes decoded RGBA directly, while supported baseline JPEG CLIP
    inputs take the direct component-plane to normalized-CHW path described
    above.
82. **Implemented after proxy-fanout review:** mixed-model generation partitions
    use a bounded eight-worker pool. Routing, inference, and independently
    bounded body draining overlap; reconstruction alone remains in stable model
    and item order. Each active partition drains into its own request-bounded
    hybrid response spool under one process-wide retained-memory and disk
    admission. Ordinary responses remain in memory; only a response crossing
    the fixed one-MiB threshold migrates its prefix and subsequent bytes to an
    unlinked temporary file. Thus small batches avoid filesystem setup while an
    uneven, oversized, or malformed response cannot steal a sibling's allowance
    or hold its upstream connection behind reconstruction order.
    The coordinator consumes each completion immediately, removes its spill,
    and refills that worker slot; indexed results and per-partition execution
    reports are merged only in stable order. On POSIX, spill files are unlinked
    as soon as they open for crash cleanup; explicit close/removal covers other
    platforms and every normal or cancellation path. The aggregate logical
    allowance still reserves the final outer envelope and worst-case
    proxy-generated failures. Shallow validation discards malformed payloads,
    while read and close errors invalidate even complete JSON. A
    configured transport header ceiling and conservative per-worker metadata
    reservation bound completed response headers; captures retain only
    `Retry-After`. Caller-supplied transports remain in use through a bounded
    wrapper, owned `http.Transport` clones are tracked, and proxy shutdown
    closes idle upstream connections.
83. **Implemented after executor-capability review:** model-family batch
    descriptors come from the resolved executor implementation, then intersect
    endpoint and manifest limits. Serial compatibility remains visible as
    serial compatibility instead of being advertised as native batching.
84. **Implemented after document-reuse review:** extraction and page-image
    embedding within one pending document group share credential-scoped source
    bytes and immutable PDF preparation. Each consumer still receives a private
    mutable render/decode session, preserving renderer thread isolation.
85. **Implemented after runtime-pool review:** specialized backend lanes activate
    on demand, and all direct inference families prefer the node's attached
    runtime executor. This removes idle executor allocation and
    request-scoped executor construction in production. API, durable work,
    inbound and outbound Raft, inference, control, and PDF rendering have
    independently configurable nonzero shares beneath one 256-thread aggregate
    ceiling; capacity can be redistributed without allowing every lazy lane to
    assume the old service-wide maximum.
86. **Implemented after adaptive-output review:** output-byte retries downsample
    one decoded renderer result and never re-enter PDF parsing, font/image
    decoding, or page painting.
87. **Implemented after CI ownership review:** chunk-provider routing ownership
    participates in aggregate enrichment-config teardown, closing the leak seen
    by the base Zig and end-to-end CI jobs.
88. **Implemented after CI contract review:** fake reader and local embedder
    fixtures satisfy the public catalog/model contract instead of weakening
    production discovery to accommodate tests.
89. **Implemented after document hot-path review:** prepared-document caching
    fingerprints source bytes once, keys stable preparations by every decode
    limit that affects validity, and reuses prepared page geometry across
    planning and rendering. Page selection and materialization use monotonic
    cursors or hash membership rather than repeated suffix scans. PDF visual
    embedding renews operation tenure for its full execution, and spilled-unit
    replay copies one bounded segment per read transaction before invoking a
    sink that may write.
90. **Implemented after bounded-materialization review:** generation batches
    resolve provisional model contracts from envelope metadata before decoding
    inline media, form capability-sized windows, and retain materialized media
    only for the active window. Exact decoded sizes and pixels are revalidated
    against the loaded model generation before execution, preserving stable
    outer result order without holding the complete batch's decoded payloads.
91. **Implemented after cross-request batching review:** the inference node owns
    a lazy, task-neutral microbatch broker keyed by exact model generation,
    task, prompt/schema, transform options, resource class, and limits. It
    coalesces only executors that advertise native batching; compatibility
    executors bypass the queue. Per-item deadlines, cancellation, provenance,
    resource shapes, typed failures, and actual execution mode survive the
    merge. Native encoded-image reads are the first adapter; generation,
    embedding, reranking, chunking, extraction, rewriting, and transcription
    use the same broker contract as their fused executors become available.
    Existing arrays publish a bounded wave of tickets before executing any
    leader; enrollment does not launch one async task per item. Native batches
    therefore survive executor saturation and a zero coalescing delay. Byte,
    pixel, token, and item limits still partition groups, and compatible
    cross-request tails may join them. Every caller executes its owned groups
    before joining foreign leaders, avoiding cyclic follower waits. Ticket
    scratch is bounded by the model's maximum group size; final ordered output
    remains request-sized. Allocation failures are isolated per item, and all
    published tickets are joined before borrowed payloads are released.
92. **Implemented after local image-transport review:** the generic borrowed
    attachment ABI now also describes validated RGBA8 rasters with width,
    height, stride, item identity, source fingerprint, and page number. Native
    Florence reads can consume PDF renderer output without PNG encode/copy/
    decode; capability negotiation preserves encoded PNG fallback for remote
    nodes, other model families, and incompatible providers. Stride is resolved
    once before preprocessing rather than recomputed in the pixel loop.
93. **Implemented after storage lock-order review:** unit and publication spool
    replay pre-admit one fixed-size segment with the resource manager's
    no-reclaim path before opening a backend read transaction. Keys, records,
    and staged payloads are copied through that fixed allocator; the read
    transaction closes before JSON sinks, generated writers, or publication
    can allocate or reclaim. This preserves bounded replay without allowing a
    memory-reclaim callback to enter storage behind an already-held LMDB
    transaction.
94. **Implemented after retained-raster performance review:** fixed PDF workers
    no longer render into page-local memory and duplicate the completed frame
    into the invocation window. They write directly through an explicitly
    thread-safe window allocator, detach the same allocation into the result,
    and leave provider request/result allocation under that allocator's one
    aggregate ceiling. Compatibility executors keep the isolated allocator and
    copy path. Concurrent-cap and pointer-identity tests prove both the shared
    bound and the absence of a second page-sized allocation.
95. **Implemented after microbatch generation and admission review:** inline
    transport bytes and decoded encoded-media bytes are measured separately;
    request residency uses the former while model contracts and batch windows
    use the latter. Cross-request groups include the immutable loaded-model
    generation and actual backend resource class, not merely a reusable path.
    Queued requests retain cheap generation fences and the leader constructs
    one reader for the executed batch. Provisional MIME/header checks still run
    before model acquisition, but only the fenced manifest supplies
    authoritative limits. The leader waits until the earlier of its bounded
    coalescing delay and caller deadline.
96. **Implemented after enrichment ownership review:** enrichment configuration
    is transferred through one explicit move boundary. A detached runtime
    clears each provider field from the source only after successful runtime
    construction; every failure and disabled-runtime path deinitializes the
    still-owned dense, sparse, asset, and chunk providers exactly once.
97. **Implemented after rolling-upgrade review:** an upstream catalog response
    of 404, 405, or 501 is a negatively cached `discovery unsupported` result,
    not an inference outage. Explicitly configured legacy endpoints continue
    with conservative singleton/serial execution, while authoritative catalog
    rejections, stale route tokens, invalid descriptors, and transient failures
    retain their distinct fail-closed semantics.
98. **Implemented after preparation-lock review:** prepared-document fetch and
    PDF parsing are keyed single-flight operations. The cache mutex now covers
    lookup and atomic publication only; unrelated source downloads, source
    hashing, and PDF parses run concurrently. A separately serialized budgeted
    allocator preserves its accounting contract, and teardown drains both
    leases and unpublished preparations before freeing cache memory.
99. **Implemented after coalescing-window review:** a follower deadline updates
    a native microbatch's earliest deadline without immediately flushing the
    group. The leader recomputes the fixed creation-time window after each
    update, preserving batching opportunity without exceeding any request's
    deadline. The standalone provider passes its absolute deadline and semantic
    cancellation callback into the node broker across the independently
    compiled linked-runtime boundary. Distributed inference keeps the same
    controls at its bounded HTTP request boundary and submits already-formed
    page batches to the remote node.
100. **Implemented after visual-embedding performance review:** linked image
    embedders advertise a concrete borrowed-raster executor. PDF page embedding
    can render directly to bounded RGBA8 windows and feed row-stride-aware
    default or CLIP preprocessing without PNG encode/decode. Vectors remain
    one-per-page and raw buffers remain invocation-borrowed. Remote inference
    deliberately retains encoded page transport because sending full RGBA over
    the network is usually a bandwidth regression.
101. **Implemented after render-overlap review:** durable visual embedding
    defaults to a single speculative render window and never queues more than
    one. The render session, planning arrays, composite lease, and retained
    output use thread-safe allocation because rendering overlaps enrichment
    staging. The next window acquires its own scratch/output reservation while
    the current reservation is live; insufficient combined capacity becomes
    sequential backpressure, not durable request failure. Task-spawn pressure
    and an operation-local prefetch timeout also return to the synchronous
    baseline. Cancellation and all error paths join the prefetch task before
    either session or page storage is destroyed. Operators can disable overlap
    with `ANTFLY_ENRICHMENT_PDF_RENDER_PREFETCH_BATCHES=0`; values above one
    are hard-clamped to one.
102. **Implemented after OCR result-copy review:** a fused reader microbatch
    materializes each public result directly into that ticket's allocator.
    The leader owns only the result pointer array and transfers every item into
    its matching slot, removing the second text/fields/regions/provenance copy
    without weakening independent ticket lifetimes.
103. **Implemented after render-admission review:** PDF OCR and page-image
    embedding reserve the scratch required by the largest planned worker wave,
    not the renderer's configured ceiling. The planner uses prepared geometry,
    fork metadata, decode working-set limits, and the bytes-per-pixel reserve;
    it reduces wave parallelism before reducing the item window and applies the
    same reduction to a partial resource grant before rejecting it. This
    leaves genuinely unused process credit available for another document or
    the one admitted prefetch window.
104. **Implemented after replay-cost review:** visual page artifacts use a
    versioned canonical input identity covering the complete source digest,
    page number, renderer contract, effective model pixel limit, page-output
    cap, local raster versus encoded transport, dimensions, and semantic model
    producer. The planner checks final artifacts and fenced generation stages
    before rendering, forms sparse ordered windows from only missing pages, and
    sends only those pages to inference. Successful window stages survive a
    later-page failure and are reused by a retry in the same lease generation;
    a new lease epoch receives a disjoint private namespace. Publication still
    promotes every required stage, removes stale page artifacts, and advances
    coverage in one fenced transaction.
105. **Implemented after raster-preprocessing review:** borrowed raster batches
    assign disjoint final tensor slices and preprocess them with bounded jobs on
    the backend runtime's lazy inference lane. The completed float buffer is
    adopted by the input tensor instead of copied into a second allocation, so
    admission and peak residency reflect one normalized tensor plus the
    borrowed raster window. Error collection remains deterministic by input
    index and the serial compatibility path remains available without an
    executor.
106. **Implemented after legacy-discovery cancellation review:** a 404/405/501
    discovery result may still publish the conservative legacy cache entry for
    other callers, but the owner that performed discovery retains its own
    cancellation or deadline outcome. Cache publication no longer converts a
    canceled operation into successful fallback work.
107. **Implemented after distributed transport review:** an owned asset
    producer runtime now owns a synchronized, bounded keep-alive connection
    pool and resolved-address cache for its full lifetime. Remote document
    windows reuse TCP/TLS connections instead of constructing a fresh
    connection for every model call; cookies remain disabled so explicit
    per-request authorization cannot acquire ambient cross-request state.
108. **Implemented after pipeline-overlap review:** primary PDF OCR uses the
    same operator-controlled one-window prefetch as durable visual embedding.
    The next render owns a thread-safe allocator and an independent composite
    lease while inference consumes the current window. The parsed session has
    only one renderer caller, cancellation is combined with an operation-local
    deadline, all exits join the worker, and failed speculative admission or
    expiry falls back to synchronous rendering after releasing the current
    lease.
109. **Implemented after generation preflight review:** multimodal generation
    retains a structural decoded-size fact for inline base64 even when its
    alphabet or pad bits are invalid. Resolved model byte limits therefore
    reject definitely oversized envelopes before decoding or model loading,
    while malformed payloads that fit the limit still receive the precise
    syntax error. Direct and batch generation now apply the same ordering.
110. **Implemented after codec-admission performance review:** image embedding
    no longer reserves the configured 128-MiB codec ceiling for every request.
    The shared preprocessing slab asks the backend session for exact physical
    growth deltas before its initial allocation and each between-wave resize,
    retains those permits while the slab exists, and releases them immediately
    after preprocessing. Caller media and the normalized tensor retain their
    independent stable lease, so concurrent requests cannot overbook memory and
    small images no longer suffer a worst-case scratch charge.
111. **Implemented after artifact-probe performance review:** durable PDF page
    embedding computes every page identity once, opens one short-lived immutable
    read snapshot, and checks final and staged artifacts through borrowed values.
    This removes two transaction setups and full stored-vector copies per page.
    Out-of-memory remains fatal, ordinary cache read failures remain conservative
    misses, and the snapshot is released before rendering or inference begins.
112. **Implemented after planner hot-path review:** PDF OCR creates page metadata
    once and lends it to both invocation planning and execution. Retained page
    bytes use prefix sums, and visual embedding compiles count-indexed,
    route-resolved invocation-memory plans once per document so later windows
    and fallback sizes perform constant-time lookups without repeated capability
    discovery. Temporary prototypes are released or transferred transactionally
    with the render window.
113. **Implemented after mixed-EOS decoder review:** native Florence incremental
    decoding tracks cache rows separately from original result rows. When a page
    reaches EOS, CUDA and Metal gather the surviving cross-attention cache,
    self-attention cache, and masks into a transactionally built smaller batch;
    subsequent decoder, LM-head, and argmax work excludes finished pages while
    output identity and order remain original-page indexed. A backend without
    row gathering retains the prior padded compatibility behavior.
114. **Implemented after local OCR representation review:** a singleton plain
    reader result transfers its owned text buffer into the producer result
    envelope instead of joining/copying it. Enrichment recognizes non-JSON text
    before invoking the JSON parser, while structured generator and extractor
    results keep their typed parsing rules. The remaining allocator-domain copy
    is the one required to move bounded invocation output into durable document
    ownership.
115. **Implemented after catalog-admission performance review:** the proxy
    partitions immutable, incarnation-fenced catalog cache hits before sizing
    worker admission. Hot catalog requests reserve only the bounded merged
    response; actual misses alone determine fetch fanout. Snapshots are checked
    again after a potentially blocking admission wait, and endpoint incarnation
    is still validated before merge, so the optimization cannot mint a lease
    from expired routing state.
116. **Implemented after distributed embedding transport review:** remote
    Antfly visual embedding can send raw page bytes in the shared
    `application/vnd.antfly.attachments.v1` envelope instead of materializing
    base64 JSON. The capability-negotiated route accounts for simultaneous
    source and framed-body residency; the inference node borrows slices from the
    bounded request body and validates one exact attachment reference per input.
    Older nodes and external providers remain on their existing admitted
    transport. The v4 feature bit is intentionally additive, preserving rolling
    compatibility in both upgrade directions.
117. **Implemented after framed-body residency review:** `httpx` now has an
    explicit synchronous borrowed-body option. Attachment envelopes keep their
    existing owned encoding buffer through redirects and retries but are no
    longer duplicated into request-owned storage. Ordinary `body` and `json`
    retain their copying contracts, conflicting body sources fail closed, and
    the remote attachment planner's two-live-representation admission bound is
    once again exact.
118. **Implemented after replay-window scalability review:** durable PDF page
    embeddings finish model work first, then promote and index page units in
    bounded replay windows. A page promotion and all of its consumer-index
    writes stay in one window; old page visibility remains available while a
    partial publication is retried; stale finals and abandoned private stages
    are deleted in bounded windows; and produced coverage is committed last.
    The publisher reserves coverage capacity in its final window and retains at
    least one derived page/index or cleanup mutation there, so completion stays
    attached to the normal transaction-fenced generated write without adding
    an otherwise empty replay sequence.
119. **Implemented after large-document validation review:** desired pages,
    staged promotions, and artifact deletes are validated with pre-sized hash
    sets. Both enrichment-side assembly and database-side promotion validation
    are linear in item count instead of repeatedly scanning document-sized
    arrays while the apply path is held.
120. **Implemented after Florence cache-copy review:** preallocated Florence
    decoder caches compact surviving rows in place on CUDA and Metal. Cross
    attention moves only displaced page slabs; self attention moves only the
    populated prefix at capacity-strided destinations. This removes the second
    full KV-cache allocation and avoids copying unused decode capacity at every
    mixed-EOS transition. Backends without device row copy retain the existing
    transactional gather fallback.
121. **Implemented after cross-request batching review:** the task-neutral
    broker accepts an existing item array, launches borrowed tickets together,
    and preserves per-item identity, limits, deadline, cancellation, allocator,
    result, and execution mode. Encoded reader windows now flatten through this
    path, so PDF pages can coalesce with compatible work from other documents
    under the exact immutable model generation instead of bypassing the broker
    merely because the caller already supplied more than one image. Multi-item
    results always use a thread-safe temporary allocation domain because even a
    singleton ticket may execute on another request's leader thread; transfer
    to the caller allocator occurs only after every borrowed ticket has joined.
122. **Implemented after borrowed-raster batching review:** PDF raster reader
    windows now submit the same per-page broker tickets as encoded images.
    Compatible pages from independent documents share one fenced native model
    invocation; the leader admits aggregate decoded pixels and output-token
    work only after the batch is realized. Window buffers remain borrowed until
    every synchronous ticket completes. The linked raster ABI forwards its
    original deadline and cancellation view into broker submission and checks
    both again before returning late results.
123. **Implemented after proxy-admission fairness review:** byte admission may
    use slack behind an oversized head waiter, but only for eight bounded
    bypasses. The aged head then becomes a barrier until enough capacity is
    available. This retains utilization for mixed small/large requests without
    allowing a stream of page-sized requests to starve a large document body.
124. **Implemented after renderer-cache performance review:** each fixed PDF
    render lane retains its private mutable reader/font cache across joined
    pages and waves of one render batch. Executor scratch is still reset after
    every callback, image caches remain render-scope bounded, cancellation is
    refreshed per page, and no mutable reader crosses worker lanes. This
    removes repeated font initialization while preserving thread confinement
    and the existing aggregate byte budget.
125. **Implemented after model-eligibility hot-path review:** reader broker
    admission acquires the authoritative model generation and asks the loaded
    session for its concrete Florence configuration. It no longer reparses
    sidecars, guesses model families from paths, or searches encoder/decoder
    files on every request. Non-Florence readers retain their typed direct
    loader and honest serial-compatibility report.
126. **Implemented after distributed-family transport review:** read,
    generation, embedding, extraction, multimodal reranking, chunking, and
    transcription endpoints consume the task-neutral framed attachment envelope and advertise
    support only from their resolved endpoint descriptor. Clients retain
    admitted base64/multipart fallback for older Antfly nodes and external
    providers. Every framed parser enforces canonical indexes, single-use exact
    cardinality, MIME agreement, and aggregate byte limits before model work.
127. **Implemented after framed-media ownership review:** generation,
    multimodal reranking, extraction, reading, embedding, chunking, and transcription
    borrow attachment slices directly from the synchronous request envelope.
    Mixed inline/downloaded/framed parsers track ownership per media item, so
    cleanup frees only materialized buffers. This removes the inference-node
    attachment copy as well as base64 expansion while retaining allocation-
    failure safety.
128. **Implemented after distributed-envelope routing review:** the Go inference
    proxy validates the complete v1 attachment envelope, borrows only its JSON
    metadata to resolve the model, and forwards the admitted binary body
    unchanged. Framed requests therefore work through independently deployed
    inference nodes without base64 rematerialization at the routing tier.
    Mixed-model framed generation is rejected before dispatch; homogeneous
    model groups are the lease-safe, zero-copy unit.
129. **Implemented after broker allocator-threading review:** reader broker
    leaders never allocate results through an unknown request allocator.
    Encoded and raw-raster tickets materialize small result objects in the
    process thread-safe domain and clone them into the caller domain after
    join, including singleton submissions that coalesce cross-request.
130. **Implemented after mixed-source read ownership review:** HTTP read batches
    may interleave framed attachments and fetched URLs while retaining request
    order. Owned downloads are stored in a dense cleanup table separate from
    the ordered borrowed image view, so every initialized response is freed
    exactly once and no sparse slot is destroyed.
131. **Implemented after allocation-failure injection review:** paired media
    bytes and ownership flags transfer from parser builders transactionally.
    Both result slices are allocated before either builder is consumed, so a
    failure in the second allocation cannot leave cleanup with unequal arrays
    or lose the ownership bit needed to release an inline/downloaded payload.
132. **Implemented after distributed lease-lifetime review:** capability tokens
    and descriptor revisions used after discovery either borrow through a
    pointer to the still-live lease field or are copied into request-owned
    storage. Reader, generator, chunker, and transcriber HTTP headers therefore
    never retain slices into block-local optional captures; extractor and
    reusable provider configs retain their existing owned replacement model.
133. **Implemented after framed-audio compatibility review:** transcription
    canonicalizes generic `application/octet-stream` attachments from physical
    audio signatures and rejects declared/physical format mismatches before
    decode. Resolved transcriber capabilities advertise every canonical codec
    enabled in the node audio runtime, rather than making framed transport
    narrower than the preexisting base64 compatibility path.
134. **Implemented after all-family transport review:** fixed multimodal
    chunking now negotiates the same framed attachment envelope as other binary
    task families. The client charges the selected physical representation,
    emits one canonical attachment reference, and retains base64 fallback for
    older nodes. The receiver enforces one attachment, exact reference
    cardinality, MIME agreement, byte limits, and borrowed request lifetime.
    Its resolved descriptor now truthfully exposes text, image, and audio input
    instead of making the efficient route unreachable through capability
    validation.
135. **Implemented after chunker decode-pressure review:** animated GIF
    chunking applies `max_chunks` inside the decoder instead of decoding every
    frame and discarding the suffix. The bounded decoder also caps aggregate
    retained RGBA bytes and validates inference image dimensions. HTTP
    admission grows before decode for the possible bounded GIF working set and
    WAV expansion, while framed inputs avoid charging a fictitious decoded
    transport copy.
136. **Implemented after chunk-config boundary review:** signed OpenAPI numeric
    fields are validated transactionally before conversion to native chunker
    types. Negative values no longer reach trapping integer casts; output
    cardinality, token targets, audio windows, thresholds, and overlap
    invariants are bounded before decode or tokenization. Failed validation
    leaves the destination config unchanged.
137. **Implemented after catalog/executor truth review:** the model catalog no
    longer publishes discovered or merely loaded chunker manifests when the
    public endpoint has no model-backed chunk executor. The built-in fixed
    multimodal chunker remains advertised with its real transport and modality
    contract. A future semantic chunker becomes visible only when its concrete
    task executor, admission, and typed result path are implemented.
138. **Implemented after retained-output and allocation-failure review:** fixed
    media chunking now has a core owned-output ceiling that is checked before
    each WAV or GIF frame encode. The HTTP executor selects a tighter
    input-derived ceiling, admits the legacy response's live base64 and JSON
    copies before execution, and reports output amplification as a bounded 413
    instead of allocating until process exhaustion. Framed/pass-through input
    bytes remain charged once. The encoded-reader result builder also releases
    its first allocation if the companion assignment table cannot be created,
    so allocator failure cannot strand an uninitialized result slice.
139. **Implemented after SDK-generation review:** repeated `allOf` chunking
    fields now carry byte-for-byte equivalent numeric constraints. In
    particular, both `max_chunks` declarations accept zero as the documented
    default sentinel and cap explicit values at 4096. This preserves the
    shallow repetition required by weaker generators without asking schema
    mergers to reconcile contradictory ranges.
140. **Implemented after distributed-copy review:** HTTPX request bodies may be
    replayable borrowed segments. The v1 attachment encoder owns only its
    fixed header/descriptor table and segment index while borrowing JSON,
    MIME, and media slices. HTTP/1 serializes only the small request head before
    writing segments; HTTP/2 emits each segment as DATA frames and places
    END_STREAM on the final nonempty segment. Readers, generators, embedders,
    extractors, chunkers, rerankers, and transcribers therefore share the same
    copy-free client transport contract across redirects and retries.
141. **Implemented after distributed-proxy residency review:** the Go proxy
    reads the fixed header first, derives and admits the exact v1
    descriptor/metadata prefix, and reads only that prefix for model
    resolution. It no longer reserves the protocol-wide maximum prefix for
    every request. An invocation-owned admission lease records bytes only
    after successful acquisition and releases them idempotently on every exit,
    including malformed metadata, routing failure, cancellation, and retries.
    Deferring the lease before header inspection does not capture a stale
    zero-byte reservation or release a failed grant's requested bytes.
    A one-attempt route streams the untouched attachment tail
    directly to the inference node. A retry-enabled route sends its first
    attempt through a tee while writing the same bytes to a process-admitted
    replay file; it does not delay the first upstream byte until the entire
    upload has been received. Transport `Close` is nonblocking and only marks
    the attempt closed; the forwarding owner then serializes after any active
    read. Only an actual retry decision seals the admitted spool, under the
    request/attempt deadline and a maximum 30-second sealing deadline.
    Cancellation and terminal responses interrupt incomplete socket reads using
    a read deadline; they never drain the remaining upload. Custom response
    writers without deadline support must expose an interruptible request body.
    Cancellation callbacks and active reads join before replay storage is freed.
    Opening a retry is read-only and cannot restart a failed or incomplete seal.
    Framed v1
    requires its exact descriptor-derived
    `Content-Length`; descriptor/length mismatches and ambiguous chunked framing
    fail before routing, while a physically truncated tail fails request
    forwarding. Request retry spills share the proxy's existing process-wide
    spool admission and configured spool directory.
142. **Implemented after Florence patch-embedding review:** tensor-native CUDA
    and Metal vision paths upload normalized NCHW pixels once, execute the
    stage-zero convolution on device, transpose NCHW output to NHWC on device,
    reshape to token-major storage, and apply the first patch normalization
    without downloading. At the default 768px geometry this removes roughly
    36 MiB of device-to-host-to-device traffic per page before the first vision
    block.
143. **Implemented after LM-head telemetry review:** `batch_fused_argmax` is
    reserved for CUDA's genuine fused projection/reduction kernel. Metal's
    current device-resident row projection followed by argmax reports
    `batch_projected_argmax`; compatibility paths keep their existing labels.
    A future Metal multi-row fused kernel may claim the fused label only after
    it supports the production dense and quantized LM-head formats.
144. **Implemented after inference-node residency review:** HTTPX exposes a
    route-scoped incremental request reader for fixed-length framed bodies.
    Attachment-capable inference POST routes opt in, and the transport also
    requires the exact v1 attachment MIME before dispatching after headers. The
    envelope decoder
    validates its fixed header and descriptor table incrementally, admits the
    exact retained metadata/media payload, then reads it into one compact slab.
    HTTP/1 consumes already-buffered bytes before the socket and never crosses
    `Content-Length`, preserving pipelined requests; HTTP/2 consumes the
    existing stream mailbox. JSON and non-opted-in routes retain normal
    buffering. Temporary payload admission is released with the envelope, and
    capacity exhaustion is a retryable 503 rather than malformed input.
145. **Implemented after transform-amplification review:** resolved model
    capabilities may publish an exact image transform: target width, target
    height, resize mode, and resampler. Concrete native reader/embedder
    registrations derive this from the vision/preprocessor configuration. The
    architecture/session geometry is authoritative and a processor sidecar may
    only fill missing dimensions, never overwrite them; this matches the actual
    executor and prevents discovery from publishing a conflicting resize;
    generator metadata is not treated as an executor promise. Standalone and
    distributed catalogs preserve it, the proxy retains it only when all
    eligible nodes agree exactly, and remote parsing validates it. PDF planning
    uses both target
    axes to choose the lowest DPI that still supplies the model input whenever
    operator limits permit; if it cannot reach the target it keeps the highest
    admissible geometry. This avoids rendering a default high-resolution page
    only for the model preprocessor to downsample it again.
146. **Implemented after executor-registration review:** native batching is
    selected from a typed resolved executor kind, not from a broad provider or
    task test. Dense and sparse embedding registrations are distinct; only an
    explicit native-batch declaration or a resolved Clip/Clap or decoder-
    embedding implementation may select them. The native Florence reader has
    its own registration. Every other resolved family remains the compatibility
    executor until it supplies a true fused operation. Catalog, direct, and
    standalone capability publication share the same resolver, so advertised
    execution mode cannot drift between deployment shapes.
147. **Implemented after generated-client review:** the Python client was
    regenerated from the corrected chunker schema, including the documented
    zero-as-default `max_chunks` contract, instead of carrying a hand-maintained
    stale model description.
148. **Implemented after embedded-build verification:** the durable chunk
    provider stores a lazily connecting HTTPX client, so HTTPX is now an
    explicit dependency of the shared embedded support module in both native
    and WASM build graphs. Embedded API and database surfaces therefore compile
    from their declared module graph instead of relying on a transitive import
    present only in the full server build.

The detailed PDF renderer design below remains normative for the
`PreparedDocument -> PageImage` transformation. References to Florence describe
the initial `ReaderExecutor`, not a restriction on the shared pipeline.

### Durable visual embedding configuration

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

The central idea is a document-scoped streaming microbatcher:

```text
parse and inspect embedded text
           |
           v
select pages that need OCR
           |
           v
prepare one document-scoped render session
           |
           v
render a bounded page window
  (controlled CPU parallelism)
           |
           v
run one Florence batch
           |
           v
merge by page number, persist, release memory
           |
           +---------- repeat ----------+
```

## Goals

- Amortize PDF parsing and resource discovery across all OCR pages in a
  document.
- Fill native Florence batches rather than invoking the model once per page.
- Permit controlled parallel page rendering without assuming that the current
  PDF reader is thread-safe.
- Bound live compressed bytes, decoded pixels, renderer scratch memory, OCR
  request bytes, model memory, and the result reorder buffer.
- Preserve request order, page identity, embedded-text quality comparison,
  durable retry behavior, and per-page failure isolation.
- Apply backpressure so a large PDF cannot render arbitrarily far ahead of OCR
  or persistence.
- Keep unsupported reader families and remote providers correct through a
  deliberate serial fallback.

## Non-goals

- This proposal does not make every reader model natively batched. Florence 2
  is the initial optimized model family.
- This proposal does not require concurrent rendering in the first milestone.
  A document-scoped serial renderer behind the batch contract is already a
  useful improvement.
- This proposal does not initially share mutable PDF renderer caches between
  threads.
- This proposal does not change page or chunk artifact keys.

## Current implementation

Antfly now has a bounded document-scoped render and OCR pipeline:

- Document extraction identifies PDF pages with missing or low-quality
  embedded text and marks them `pending_ocr`.
- Generated OCR work is collected using an item and byte policy. The defaults
  are eight items, an operator maximum of eight items, and 64 MiB of request
  bytes.
- One stable, heap-owned PDF OCR coordinator is retained for the entire
  document operation, including across streaming OCR microbatch flushes. Its
  `PdfRenderSession` discovers the page tree once and serially refreshes an
  immutable `RenderForkTemplate`. Each admitted render worker instantiates a
  private reader from that template with its own allocator, caches,
  cancellation probe, render targets, and diagnostics; workers never mutate
  or lazily discover state through the source reader.
- `renderParsedPagesBatchAlloc` accepts an explicit page window, preserves
  request order and page identity, isolates deterministic page failures, and
  enforces batch-page, parallelism, pixel, in-flight byte, retained PNG, and
  per-worker allocator limits.
- PDF inspection is charged by actual allocation through its own bounded
  allocator. Each render/inference window then acquires one atomic composite
  lease: scratch may receive a partial grant, while the calculated transport
  and provider output peak is required in full. Its output side is transferred
  to a window-local `BudgetedAllocator`, so rendered media and provider buffers
  cannot outlive their reservation. The available worker allowance is
  recomputed from the lease before every window. Both the byte-derived pixel
  allowance and the explicit in-flight pixel cap participate in adaptive
  geometry, so a tighter operator pixel limit reduces DPI instead of rejecting
  an otherwise renderable page.
- Production render workers belong to one lazy BackendRuntime PDF lane. Its
  fixed physical workers receive private reusable arenas whose retained backing
  is capped per worker; a lane-wide physical ceiling and the current window's
  logical scratch ceiling bound new backing. The bounded queue applies
  backpressure across concurrent documents, shutdown drains admitted jobs, and
  no page callback can escape the synchronous batch lifetime. Standalone PDF
  callers retain the call-scoped compatibility executor. The fixed lane writes
  retained output directly through a thread-safe window allocator with one
  atomic byte ceiling shared by renderer workers and the provider invocation;
  provider request/result allocation therefore sees the exact remaining
  credit. Each page downsizes output to its request ceiling before ownership is
  detached into the batch result, avoiding a page-sized copy.
- The enrichment runtime renders only the next OCR microbatch. It holds no
  prefetched window, transfers each rendered PNG into the matching producer
  request, flushes OCR, and releases the bytes before preparing another
  window.
- The local Antfly reader producer flattens compatible page requests into one
  image list while retaining original request boundaries. Borrowed encoded
  pages carry item, source, and page identity independently, so compatible
  pages from different documents may now share a native batch. Legacy URL-only
  reader inputs remain source-bounded until that transport carries the same
  per-item identity.
- Encoded pages cross the asset producer and standalone inference ABI as
  generic borrowed attachments. For a linked native reader that advertises the
  raster capability, validated RGBA8 pages use the same borrowed payload array
  plus dimensions, stride, pixel format, and per-item provenance, avoiding the
  local PNG round trip. Reader, generator, and embedding operations retain the
  encoded path, and remote nodes or providers without the raw-raster callback
  adapt at the final transport boundary. The inference host rejects redundant
  count, cardinality, index, pointer, geometry, stride, and lifetime violations
  before borrowing bytes.
- The read endpoint and direct read interface accept up to 64 images and apply
  aggregate encoded-byte and decoded-pixel admission. Direct encoded-image
  calls charge their already-resident bytes once; only URL/data-URI paths
  reserve prospective download storage.
- Native Florence chunks image inputs by
  `ANTFLY_INFERENCE_READ_BATCH_SIZE`, which defaults to eight. Each chunk uses
  a batched encoder and, where supported, a batched incremental KV decoder.
- Page render latency is captured inside each worker and returned with its
  indexed page result. Window scheduling, start-gate wait, and later OCR work
  are not mislabeled as page render time.
- Results are returned in input order. Unsupported providers and malformed
  native batch responses fall back to isolated serial work.
- Generator-backed OCR uses independent page messages and generic OCR prompt
  semantics; it is not routed through `LoadedReader`. Multimodal generation
  batches are admitted but currently reported as `serial_compatibility` while
  shared projector/session state is serialized.
- Multimodal embedding exposes one-vector-per-part batch semantics for page
  images and text chunks. It never silently pools a document. The integration
  fixture exercises the same bounded two-page render window through fake
  Florence reader, Gemma generator, and ClipClap embedder boundaries.
- Mixed-EOS Florence batch decoding preserves original result identity while
  compacting supported KV caches to unfinished rows. Unsupported backends keep
  finished rows padded through the compatibility path, and regression coverage
  verifies independent lengths and original-row updates.

PDF OCR and durable page-image embedding can prefetch exactly one second render
window. The current window remains admitted while the speculative window
acquires its own composite lease, so the resource manager observes and bounds
their combined peak before the second render starts. If that reservation cannot
be acquired, the current window completes and releases first, then the same page
range is prepared synchronously. Every executor without an explicit overlap
policy retains acquire-render-infer-release sequencing.

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

## Target pipeline

### 1. Inspect the document and select OCR candidates

Embedded PDF text extraction continues to produce stable page units. OCR
candidate selection remains based on `ocr_mode` and `OcrQuality`.

The candidate list contains stable page metadata rather than rendered bytes:

```zig
const PdfOcrCandidate = struct {
    unit_index: usize,
    page_number: u32,
    embedded_quality: OcrQuality,
    force_ocr: bool,
};
```

Born-digital pages that pass quality checks do not enter the render scheduler.
Sparse candidate lists such as pages 1, 5, and 19 remain valid and preserve
their original unit positions.

### 2. Parse once and separate immutable from mutable render state

The coordinator's `PdfRenderSession` is deliberately treated as
non-thread-safe. Parallel rendering is implemented by preparing the base parse
and giving every worker a private `Reader.forkForRendering` snapshot with its
own allocator and mutable caches. Conceptually, that is the following split:

```zig
const ParsedPdfDocument = struct {
    source: []const u8,
    object_index: ImmutableObjectIndex,
    page_tree: ImmutablePageTree,
    resources: ImmutableResourceMetadata,

    fn createRenderContext(
        self: *const ParsedPdfDocument,
        allocator: Allocator,
    ) !PdfPageRenderContext;
};

const PdfPageRenderContext = struct {
    allocator: Allocator,
    graphics_state: GraphicsState,
    decoded_streams: TaskLocalDecodeCache,
    font_state: TaskLocalFontState,
    compositing_scratch: TaskLocalCompositingScratch,
};
```

The names above are illustrative; the important ownership boundary is not.
Object tables, page-tree structure, source bytes, and immutable resource
descriptions may be shared. Decoded streams, graphics stacks, font mutation,
image buffers, transparency buffers, and encoder state must initially be
task-local.

Any existing lazy cache must be handled in one of three ways:

1. Populate and freeze it before starting render tasks.
2. Move it into `PdfPageRenderContext`.
3. Protect it with narrowly scoped synchronization and document why contention
   is acceptable.

The implemented choice is task-local state. A future renderer may use a mutex
around a narrowly scoped shared cache, but a mutex around a whole session would
be serialized and must be reported as such.

The first batch-render milestone may parse once for the render operation in
addition to the earlier embedded-text extraction parse. A later fused PDF
preparation operation can share one parsed document across embedded-text
analysis and rendering, reducing the document to one total parse. Avoid an
opaque cross-ABI session handle unless measurements show that the fused or
coarse streaming operation cannot meet backpressure requirements; handles add
lifetime and runtime-artifact compatibility risk.

### 3. Use a bounded multi-page render boundary

`zig/lib/pdf` exposes a coarse in-process document operation. Origin main no
longer has the older enrichment-compute render ABI, so reintroducing an opaque
cross-artifact session handle would add lifecycle risk without helping the
current call graph. The implemented public shape is:

```zig
pub const PageRenderRequest = struct {
    page_number: usize,
    requested_dpi: u16 = 150,
    max_pixels: u64 = 40_000_000,
    max_dimension: u32 = 4096,
};

pub const PageRenderBatchOptions = struct {
    max_batch_pages: usize = 8,
    max_parallel_pages: usize = 1,
    max_inflight_pixels: u64 = 50_000_000,
    max_inflight_bytes: usize = 512 * 1024 * 1024,
    max_retained_png_bytes: usize = 64 * 1024 * 1024,
    max_retained_raster_bytes: usize = 256 * 1024 * 1024,
    bytes_per_pixel_reserve: usize = 12,
    cancellation: reader.CancellationProbe = .{},
    executor: ?PageRenderExecutor = null,
    concurrent_output_allocator: ?Allocator = null,
};
```

The encoded and raster batch variants both return results in request order.
Each result contains the explicit page number and exactly one payload or page
failure. Batch deinitialization releases every result with the same allocator
that owns that invocation window.

The provider performs these steps inside one call:

1. Validate all limits and page numbers.
2. Reuse the document-scoped parsed reader.
3. Create a bounded render group.
4. Render admitted pages using private contexts.
5. With a fixed executor and an explicitly thread-safe caller allocator,
   allocate retained encoded or raster payloads directly in caller-owned
   storage and detach them in coordinator order. Compatibility executors copy
   completed payloads on the caller thread.
6. Cancel and join workers on a systemic failure.
7. Destroy the parsed document after the final worker joins.

The existing single-page functions remain available for compatibility. New
document OCR work performs adaptive encoded-size retries inside the batch
worker, before retained-output admission.

### 4. Use controlled parallel rendering

Parallel rendering uses two levels of admission in the current runtime:

- Global resource-manager byte admission and bounded enrichment execution lanes
  prevent concurrent PDFs from multiplying memory without limit.
- A backend-runtime-owned PDF lane supplies a small fixed physical worker set
  across documents; a per-document worker cap prevents one large PDF from
  monopolizing it. The lane is created lazily on first use and its configured
  share participates in the aggregate BackendRuntime thread ceiling.

Resource-manager admission uses one owned split reservation per execution
window. With the defaults, a window must own the physical peak implied by its
logical media cap, resolved attachment transport, bounded invocation allocator,
and requested native scratch. Page-image embedding uses the same mechanism.
The native side may be partially granted but must still render one valid page;
the output side is required. An unusable scratch grant or unavailable output
credit fails before worker submission, and successful windows release both
sides before the next admission opportunity.

PDF and model modules do not own process-global worker pools. The specialized
PDF lane and model-compute lanes are lazily activated behind `BackendRuntime`
with a common aggregate configuration, lease, shutdown, and metrics contract.

Page count is not a sufficient weight. Before scheduling a page, derive its
target dimensions after DPI and dimension clamping, then estimate:

```text
RGBA output bytes
+ resampling scratch
+ drawing and compositing scratch
+ bounded PDF stream decode reservation
+ encoded PNG estimate
+ retained OCR request bytes
```

The scheduler acquires a weighted reservation:

```zig
const PdfRenderAdmission = struct {
    workers: usize = 1,
    pixels: u64,
    working_bytes: usize,
};
```

Estimates are intentionally conservative. Every task also uses a hard
`BudgetedAllocator` for allocations routed through the native Zig renderer,
and PDF stream decoding retains its existing decoded-stream and peak
working-set limits. An underestimate in allocator-accounted work therefore
causes an identified resource failure rather than unbounded growth.

CoreGraphics and CoreFoundation may retain internal memory outside that
allocator. Compatibility rendering is consequently a distinct containment
case: its caller-owned RGBA and PNG buffers remain allocator- and pixel-bounded,
the PDF source is borrowed without a copy, and one document-scoped compatibility
session serializes fallback pages. The process-wide native reservation includes
a conservative allowance for this framework work, but it is admission
accounting rather than a hard allocator interception. Release qualification
must measure peak RSS for PDFs that exercise the compatibility backend.

Effective per-document concurrency is:

```text
min(requested concurrency,
    operator concurrency cap,
    memory-budget-derived concurrency,
    pages remaining in the active OCR window)
```

Start with a default concurrency of one, not the machine CPU count.
Concurrency greater than one must remain disabled until the immutable document
and task-local context split is complete.

The renderer should initially schedule pages in document order. More complex
size-aware scheduling is possible later, but it increases reorder-buffer
pressure and makes latency harder to reason about. Page-number-keyed results
still make completion order irrelevant to correctness.

### 5. Fill one bounded render/OCR window

Rendering must not run across the whole document before inference. The
enrichment runtime fills one microbatch subject to all limits:

```text
candidate count       <= OCR batch item cap
serialized/encoded    <= OCR batch byte cap
rendered pixels       <= render window pixel cap
working-set estimate  <= render window memory cap
```

Pseudocode:

```zig
while (next_candidate < candidates.len) {
    const window = try scheduler.renderNextWindow(candidates[next_candidate..]);
    defer window.deinit();

    const readable_pages = collectSuccessfulPagesByPageNumber(window.results);
    const ocr_results = try reader.readBatch(readable_pages);
    try applyPageResults(units, readable_pages, ocr_results);
    try recordRenderFailures(units, window.results);

    next_candidate += window.consumed_candidates;
}
```

The tail batch is allowed to be smaller. A page whose individual request is
larger than the operation cap is recorded as a permanent request-size failure
without preventing later pages from progressing.

The initial unified default should be eight OCR pages, matching the native
Florence default. The byte and pixel caps may produce smaller batches for large
pages.

### 6. Remove the local base64 and JSON round trip

Add an internal binary media representation to `asset_producer.Request`:

```zig
pub const MediaInput = struct {
    bytes: []const u8,
    mime_type: []const u8,
    trusted_internal: bool,
};

pub const Request = struct {
    // Existing fields...
    media: []const MediaInput = &.{},
};
```

The local Antfly reader producer can pass trusted rendered PNG bytes directly
to the direct reader interface. Remote providers may serialize media according
to their transport requirements, but that serialization should not be imposed
on the local path.

This removes:

- PNG-to-base64 expansion.
- JSON escaping and parsing.
- Data-URI parsing.
- Base64 decoding into a second encoded-image allocation.

A later optimization may pass decoded RGB images or preprocessed Florence
pixel tensors directly to the reader. That should be a separate milestone:
crossing the enrichment/inference boundary with decoded images increases ABI
surface area and makes pixel-buffer admission and format compatibility more
important. The encoded binary handoff captures most of the avoidable overhead
with substantially less coupling.

### 7. Execute the native OCR batch

Compatible local Antfly reader requests are flattened into an image list and
submitted to `LoadedReader.readBatch`. Preserve these behaviors:

- The original request-to-image ranges are retained so results can be
  reassembled correctly.
- Native Florence chunks by its effective model batch cap.
- Florence encoder execution is batched.
- CUDA and Metal use the batched incremental KV path where supported.
- Rows that reach EOS stop accumulating output text while unfinished rows
  continue.
- Unsupported shapes fall back to the full batched decoder, then to the serial
  reader only for documented compatibility errors.
- The batch result count must equal the submitted image count.

The document batch cap and Florence batch cap should share a common effective
value or, at minimum, be exposed together in status and profiling. A document
batch larger than the Florence cap is still correct, but it creates an extra
hidden chunk boundary.

### 8. Merge and persist page results

Every render and OCR result is keyed by page number and mapped back to its
stable unit index:

```zig
const PageOcrResult = union(enum) {
    success: struct {
        page_number: u32,
        output: ReaderResult,
    },
    failure: struct {
        page_number: u32,
        stage: enum { render, preprocess, inference, validation },
        retryable: bool,
        identity: FailureIdentity,
    },
};
```

For successful OCR, retain the existing comparison between embedded and OCR
quality. OCR must not erase substantial embedded text with a trivial or worse
response. Update method, status, confidence, regions, render metadata, and
failure provenance before generating the final unit and chunk artifacts.

Rendered buffers are released immediately after their OCR result is applied.
The pipeline must not retain completed PNGs through later artifact writes.

## Backpressure and controlled overlap

The baseline renders one window, consumes it, releases it, and then renders the
next. This remains the fallback whenever no concurrent runtime is attached,
overlap is disabled, or a second reservation does not fit.

An optional double-buffered mode can overlap CPU rendering of window N+1 with
model inference for window N:

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

If the inference queue is saturated, rendering must stop at the admitted
window. Likewise, a render scheduler with no permits must not reserve an
inference slot while waiting if doing so can create a resource-order deadlock.
Define and test one global acquisition order, for example:

1. Document extraction working-set lease.
2. Render-window lease.
3. Inference queue units only after the window is ready.

Release in reverse order where practical. Do not hold inference units while
waiting for render workers.

## Configuration and operator caps

The public execution policy should express desired batching while operator
settings impose hard ceilings. In particular, the page limit is
`min(requested max_document_pages, operator max_document_pages, 16384)`; an
omitted request uses the operator ceiling. A possible configuration shape is:

```yaml
execution:
  batch_items: 8
  batch_bytes: 67108864
  max_document_pages: 2048

ocr:
  render:
    concurrency: 2
    max_inflight_pages: 8
    max_inflight_pixels: 50000000
    max_inflight_bytes: 268435456
    prefetch_batches: 1
```

Suggested operator controls:

```text
ANTFLY_ENRICHMENT_OCR_BATCH_ITEMS
ANTFLY_ENRICHMENT_OCR_BATCH_MAX_ITEMS
ANTFLY_ENRICHMENT_OCR_BATCH_BYTES
ANTFLY_ENRICHMENT_OCR_RENDER_PARALLEL_PAGES
ANTFLY_ENRICHMENT_OCR_RENDER_INFLIGHT_PIXELS
ANTFLY_ENRICHMENT_OCR_RENDER_INFLIGHT_BYTES
ANTFLY_ENRICHMENT_PDF_RENDER_PREFETCH_BATCHES
ANTFLY_ENRICHMENT_PDF_MAX_DOCUMENT_PAGES
```

Empty, zero, overflowing, and malformed values need explicit semantics. In
general, requested values are clamped to at least one where zero would disable
progress. Parallel pages are hard-clamped to eight. Parallel rendering defaults
to one; operators can opt into higher CPU concurrency only after assigning an
aggregate byte budget that admits it. Visual-embedding prefetch defaults to one
window, accepts only zero or one effectively, and falls back to sequential work
when the second composite lease is unavailable.

Status output should report requested and effective values so operators can
tell when admission or a hard cap reduced concurrency or batch size.

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

### Functional batching

- An eight-page scanned PDF produces one render window and one Florence batch.
- A ten-page scanned PDF produces windows and OCR batches of eight and two.
- A mixed PDF batches only selected pages while retaining original unit order.
- Sparse candidate pages map back to the correct units.
- Batch output is byte-for-byte equivalent to serial output for deterministic
  fixtures.
- Florence rows with different EOS lengths match serial decoding.

### Rendering and thread safety

- Instrumentation proves one render parse/session per document operation.
- Concurrency one uses the same batch ABI and output as the legacy path.
- Concurrency two or greater produces identical page images under repeated
  stress.
- A wave-start rendezvous makes launched-worker concurrency deterministic even
  for fixtures whose individual pages render faster than thread scheduling.
- Task-local decode, font, graphics, and compositing state does not leak across
  pages.
- The parsed document remains alive until all workers join.
- Cancellation during every render stage joins workers and frees buffers.
- Cancellation while workers are still arriving at the wave-start gate
  releases both arrived and late workers without waiting for the missing
  arrival.
- Allocation-failure injection at every ownership transfer is leak-free.
- Thread-sanitizer or equivalent stress coverage is used where supported.
- `zig build pdf-ocr-integration-test` runs as a production-mode executable
  (`builtin.is_test == false`) and renders both pages of a real fixture through
  one document-scoped coordinator. It carries both PNGs through the
  asset-producer encoded-media path and verifies one local reader callback with
  two ordered images. It asserts two launched workers and separately validates
  the honest active-renderer range of one through two, preventing
  the normal Antfly unit-test PDF stub or a start-gate wait count from making
  native integration coverage pass vacuously. The same executable is a
  dependency of `lib-pdf-test` and the aggregate `unit-test`, so CI runs the
  production path rather than leaving it as an opt-in check.
- Durable enrichment tests seed an existing public page vector, stage one
  successful sibling beside a failed page, and verify that retry startup keeps
  the old vector while clearing private partial output. A complete retry then
  promotes both vectors and removes both staging records.
- Capability tests verify local executor rejection and remote catalog parsing
  for modalities, MIME, item, byte, pixel, and media-part limits.

### Real-model qualification

The non-hermetic release gate renders the same two-page fixture and sends its
pages through actual remote Antfly reader, generator, and embedder contracts:

```sh
ANTFLY_PDF_QUALIFICATION_URL=http://127.0.0.1:8082/ai/v1 \
ANTFLY_PDF_QUALIFICATION_READER_MODEL=florence2 \
ANTFLY_PDF_QUALIFICATION_GENERATOR_MODEL=gemma4 \
ANTFLY_PDF_QUALIFICATION_EMBEDDER_MODEL=clipclap \
ANTFLY_PDF_QUALIFICATION_EMBED_DIMS=768 \
zig build pdf-model-qualification-test
```

The gate requires two non-empty reader results, two non-empty generator
results, and two vectors with the configured dimensions. It intentionally
fails when the environment is absent, so a release job cannot accidentally
report model qualification after running only the fake providers.

### Admission and memory

- Item, encoded-byte, pixel, and working-set thresholds each split windows at
  the correct boundary.
- A single oversized page fails individually without blocking later pages.
- Actual live memory remains below the combined declared caps.
- The native reservation preserves the configured tracked-output headroom
  as an owned credit under competing slice owners.
- The owned output credit transfers atomically into a dedicated pinned
  invocation allocator; manager usage does not change during the transfer,
  idle windows retain their guarantee, and the credit is released at operation
  teardown. Retained OCR state is admitted through a different allocator.
- Partial native grants reserve a viable minimum raster after persistent parse
  state and a bounded decode workspace. Page geometry is derived from the
  remaining byte grant before worker admission.
- A per-page pixel cap below the requested geometry adaptively lowers DPI and
  succeeds when the minimum supported render geometry still fits.
- The synchronous precompute path downloads and materializes its temporary
  extraction and OCR state through a BudgetedAllocator before allocation; it
  uses the same atomic inspection/render/output partition as streaming
  extraction. Its extracted units remain under inspection ownership until they
  are destroyed, so the native reservation is not released early.
- Streaming text inspection, its borrowed callback unit, the persistent render
  coordinator, and render forks are simultaneously covered by distinct hard
  ceilings whose sum is the one native reservation.
- In the streaming path, provider request construction and returned transient
  buffers use a dedicated allocator holding the transport-aware output credit;
  persistent OCR text uses the independently admitted collection allocator.
  The synchronous path likewise keeps extracted and replacement unit state
  under its inspection ceiling while a separate output allocator owns rendered
  media and provider buffers. Thus retained results cannot consume the next
  window's guaranteed invocation headroom.
- Source-session cache growth between windows reduces the next window's
  computed worker allowance.
- Underestimated pages are stopped by the budgeted allocator.
- Many concurrent documents obey the global weighted-byte cap and bounded
  enrichment execution lanes.
- Prefetch mode admits both buffers and never retains more than the configured
  number of windows.
- Queue saturation applies backpressure instead of accumulating rendered
  pages.
- Resource acquisition order cannot deadlock render and inference workers.

### Failure isolation and replay

- One render failure does not poison sibling pages.
- One trivial OCR result does not poison sibling pages.
- A malformed native batch response falls back only for that window.
- A transient inference failure yields without persisting false terminal
  coverage.
- Retry resumes deterministically without duplicating or skipping page
  artifacts.
- A crash after rendering but before persistence does not require rendered
  images to be durable; replay safely rerenders the pages.
- Existing embedded text survives an inferior OCR response.

### Boundary compatibility

- Existing single-page render functions remain available.
- Invalid batch limits and page numbers return explicit failures.
- Batch-owned buffers are destroyed exactly once, including partial failure.
- The standalone inference ABI version is bumped when borrowed binary payload
  segments are added, and prefix validation rejects mismatched artifacts.
- The standalone encoded-image codec round-trips multiple borrowed payloads
  with ordered MIME types and metadata, accepts empty input without invoking a
  model, and rejects missing pointers or JSON/binary count mismatches. The
  unit conformance test traverses request construction, dispatch, response
  serialization, and destruction through a model-free handler seam.
- `zig build linked-inference-abi-integration-test` links the separately
  generated production inference archive, resolves only
  `antfly_standalone_inference_get_function_table`, and verifies function-table
  prefix validation, wrapper-owned version rejection, stable `Status` mapping,
  and borrowed binary MIME rejection without importing `inference_host.zig`.
  The focused standalone runtime test depends on this executable.

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

The post-implementation performance review resulted in these concrete changes:

- Florence's ordinary greedy batch decoder uses CUDA's fused LM-head/argmax
  reduction when available, returning only one token id per active row. Metal
  retains device-resident projection and argmax but currently materializes one
  logits row at a time for small OCR batches; telemetry calls that path
  `batch_projected_argmax`, never fused. Full host-visible logits remain only
  for a non-zero final-logits bias, no-repeat-ngram filtering, or a backend
  without device selection.
- CUDA Florence batches keep the final vision stage, positional/temporal
  source expansion, image projection, and image normalization device-resident
  for every admitted batch size. The underlying kernel was already batch-aware;
  removing the single-item policy gate eliminates a large device-to-host-to-
  device round trip for batched OCR.
- Florence decoder/preprocessor sidecars and the final-logits-bias capability
  are resolved once before an immutable model generation is published, not
  once per executed microbatch. Lightweight reader wrappers copy that immutable
  metadata, so hot batches perform neither filesystem discovery, synchronization,
  nor repeated device-to-host bias scans.
- Encoded visual embedding now allocates normalized model input once and
  adopts that allocation into the tensor. Image and borrowed-raster decode,
  resize, and normalization happen before the model execution lock; only
  resident projector/session use is serialized. Caller-retained media, the
  normalized tensor, and the bounded codec slab hold a host-only process
  admission lease during preprocessing; backend/GPU workspace is acquired
  only after the model lock, so requests queued for one session do not pin
  accelerator capacity. After codec scratch is released, the host permit is
  reduced to the actual retained inputs and composed with a run request that
  credits exactly those bytes. Inputs therefore stay continuously admitted
  without a release/reacquire gap or double-counted host residency.
- CLIP/SigLIP raster preprocessing resolves channel count and row stride once
  per image rather than in the inner sample loop.
- A native reader broker candidate no longer owns a singleton inference lease
  while waiting. The group leader derives decoded-pixel residency for the
  complete fused batch and acquires one authoritative lease immediately before
  execution.
- Existing reader request batches are enrolled as bounded waves of broker tickets,
  allowing a short PDF window and compatible pages from another request to
  fill one native model batch. The broker still groups on immutable generation,
  prompt, schema, transform, options, and resource class, and the leader admits
  the realized aggregate media shape once.
- Preallocated Florence KV compaction reuses resident tensors and copies only
  displaced cross-attention slabs plus the populated self-attention prefix.
  Mixed-length batches therefore avoid both a second cache-sized peak and work
  proportional to the unused output-token capacity.
- Framed remote attachments borrow their already-owned envelope through the
  synchronous HTTP call. Peak host transport residency is source bytes plus one
  envelope rather than source bytes plus two envelope copies.
- PDF vector publication is windowed independently of render/inference windows.
  Promotion, consumer-index updates, stale cleanup, and final coverage never
  accumulate a document-sized apply transaction, and validation remains linear
  as the page ceiling grows.
- The task-neutral microbatch broker is sharded by the complete resolved
  executor key. Independent models, tasks, prompts, schemas, transformations,
  options, and resource classes therefore do not contend on one global group
  mutex.
- Remote transcription compatibility batches run through a bounded ordered
  window when runtime I/O is available. Worker results use a thread-safe
  temporary allocator, are joined before transfer to the caller allocator, and
  preserve input order and cancellation semantics. Linked model state remains
  serial.
- Gemini, Vertex, and Cohere embedding adapters borrow a lazily created HTTP
  client from the service-scoped `ProviderRuntime`. Connection pools, DNS/TLS
  state, Google credentials, and regional Bedrock credential caches therefore
  survive request-scoped `ManagedEmbedder` construction. First publication is
  mutex-protected and request allocation uses a thread-safe allocator; a
  standalone embedder retains the owned compatibility fallback.
- Ordered remote compatibility batches transfer each completed wave into final
  caller-owned results and release the thread-safe temporary responses before
  admitting the next wave. The unavoidable final outputs scale with item
  count, while the second response copy is bounded by compatibility width.
  Asset-producer and reranker service pools likewise use thread-safe internal
  HTTP allocation because their admitted calls may run concurrently even when
  the runtime shell itself was constructed by an arena-backed owner. Durable
  remote chunk providers now also own one keep-alive client and publish it as
  a borrowed execution-context dependency; standalone chunk calls create the
  old call-scoped client only when no provider runtime was supplied.
- The distributed proxy reuses the registry refresher's exact node catalog
  snapshot when the endpoint incarnation and authorization digest match. It
  publishes canonical immutable category maps, so hot requests borrow one
  generation without cloning the body, reparsing category JSON, or repeatedly
  sanitizing unique capability descriptors. Merged response size is tracked
  incrementally with a conservative JSON-escaping bound and asserted exactly
  after serialization, avoiding the prior quadratic full-catalog rescans.
  Snapshot residency has a process-wide 64 MiB cap with oldest-snapshot
  eviction; charges include owned keys, descriptors, map buckets, headers, and
  allocator/GC overhead rather than only original wire bytes. Freshness is
  bounded to twice the configured refresh interval
  (between 30 seconds and five minutes). Caller-specific authorization never
  reuses another authority's catalog. This removes request-time all-node
  discovery and repeated node manifest scans on the common service-credential
  path while retaining exact capability leases.
- The distributed ingress path overlaps client upload, retry-spool writes, and
  the first upstream attempt. Both the proxy and inference node retain only the
  exact routing/payload regions they need, so a large media request no longer
  incurs a full pre-forward delay or simultaneous wire-body and decoded-payload
  copies on the common HTTP/1 and HTTP/2 paths.
- Resolved image transforms feed PDF render planning before allocation. This
  turns model preprocessing geometry into a producer-side resource constraint,
  reducing renderer CPU, encoded bytes, transport bytes, and decode work as one
  coordinated optimization rather than a backend-specific resize tweak.

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

## Implementation status and follow-ups

### Phase 0: Baseline and parity coverage

Status: production plumbing and the opt-in real-model gate are complete;
release environments still own model parity and performance baselines.

- Add end-to-end metrics for parse, render, handoff, and Florence execution.
- Add native Florence batch-versus-serial parity tests.
- Add a representative scanned PDF benchmark with 1, 4, 8, and 16 pages.
- Record current parse count, peak memory, render time, OCR time, and total
  throughput.

### Phase 1: Unify OCR batching policy

Status: complete.

- Change the document OCR default from four to eight where resource defaults
  permit it.
- Keep the document and Florence caps independently operator-controlled and
  profile actual batch and internal chunk behavior.
- Retain item and byte caps and add an aggregate rendered-pixel cap.
- Preserve current serial provider fallback behavior.

### Phase 2: Batch-render ABI with serial execution

Status: complete as an in-process `zig/lib/pdf` boundary; the obsolete
enrichment-compute ABI is not reintroduced.

- Add the streaming multi-page render ABI and indexed page results.
- Parse once per batch-render document operation.
- Implement it with `max_parallel_pages = 1` using the current renderer.
- Migrate enrichment runtime callers.
- Keep the existing single-page API alongside the batch API for compatibility.

This phase removes repeated per-page parsing without making thread-safety
claims.
- Model configuration describes what executes; `InferenceExecutionContext`
  describes where it executes. The context carries the default Antfly
  endpoint, shared capability cache/I/O ownership, and trusted source routing.
  Task adapters may consume this context but may not synthesize their own
  localhost, cache, or tenant-routing policy.

### Phase 3: Immutable document and controlled concurrency

Status: complete for document-local concurrency plus global resource-manager
byte admission. Parallelism defaults to one and is operator-capped at eight.

- Split parsed immutable PDF state from mutable page render contexts.
- Move lazy mutable caches to task-local state or freeze them before workers
  start.
- Use global resource-manager byte admission plus a per-document worker cap.
- Add private freeing budgeted allocators per task.
- Enable operator-capped concurrency, defaulting to one.
- Add cancellation, join, and concurrency stress tests.
- Keep one stable coordinator across streaming OCR flushes, compose
  wave-local cancellation with the external deadline, and report measured
  rather than planned concurrency.
- Reserve native and transient output memory as one owned split, reclaim before
  partial fallback, atomically transfer output credit to a dedicated
  window-scoped invocation allocator, partition partial native grants into
  decode/raster budgets, and recompute available render bytes before each
  window. Release the complete composite lease before planning the next window.
- Downsize encoded output within each worker before retaining a completed page.
- Borrow immutable document metadata into wave-local worker forks and reuse one
  fixed backend-runtime worker lane across documents and waves; mutable page
  state remains thread-confined and reusable scratch is reset with a strict
  retention cap after every job.
- Treat the native Zig allocator ceiling and the CoreGraphics framework
  allowance as separate guarantees. Compatibility pages are serialized and
  RSS-qualified because framework-private allocations cannot be intercepted by
  `BudgetedAllocator`.

### Phase 4: Binary local media handoff

Status: complete, including operation-neutral borrowed binary segments across
reader, generator, and embedder standalone runtime calls, with data-URI
fallback at unsupported provider boundaries.

- Extend the internal asset producer request with trusted binary media.
- Add a direct encoded-image batch reader entry point.
- Reconstruct generator media placeholders and embedding binary parts from the
  same borrowed attachment array with strict redundant-count validation.
- Treat direct encoded media as already resident so admission does not add a
  fictitious second download allocation.
- Remove base64/data-URI serialization from local PDF OCR.
- Keep serialization adapters for remote providers.

### Phase 5: Render/OCR pipeline overlap

Status: complete for PDF OCR and durable visual embedding. The active window
releases its scratch and output credit as soon as its consumer finishes. At
most one speculative window may acquire a second composite lease against the
still-live first lease; inability to admit the combined peak falls back to
synchronous preparation after release. Cancellation and every exit path join
the speculative task before destroying its document session.

- Keep prefetch operator-controlled and hard-cap it at one window.
- Keep every live render and invocation byte under a composite reservation.
- Join speculative rendering before tearing down its document session or
  buffers.
- Compare end-to-end throughput and peak memory against the non-overlapped
  implementation.

### Phase 6: Optional decoded-image handoff

Status: complete for linked native Florence reads and linked visual embedders.
Bounded direct-to-batch-tensor preprocessing uses the backend runtime's lazy
inference lane, deterministic output slices, direct RGBA consumption for
Florence and ClipClap-class embedders, and direct baseline-JPEG
component-plane-to-CHW writes for encoded CLIP input. The borrowed raster ABI
carries validated RGBA8 dimensions and stride through the standalone bridge;
capability negotiation retains encoded-image fallback everywhere else.

Resamplers that require temporary storage, including Pillow-compatible
bicubic, allocate from a caller-backed, synchronized wave budget. That budget
grows only between joined waves, reduces worker width before rejecting valid
input, and maps terminal exhaustion to the same explicit preprocessing byte
limit as encoded-image decoding. Borrowed-page preprocessing therefore has no
unadmitted per-worker scratch allocator escape.

- Measure the realized encode/decode reduction and copy cost on the production
  corpus.
- Keep the decoded pixel format and ownership/admission contract narrow and
  append-only until another executor demonstrates a need for additional pixel
  formats.
- Preserve encoded-image fallback for other reader families.

### Phase 7: Fuse PDF inspection and render preparation if needed

Status: complete within one pending document group. A credential-scoped source
cache owns the download and one prepared reader, lends immutable metadata to
task-private render forks, and retains idle preparation for page-image
embedding to reuse unless process pressure requires eviction. A bounded
attempt spool prevents publication from walking the PDF again; neutral page
text records let compatible consumers avoid repeating text inspection.

- Reuse one preparation per credential-scoped source and decode envelope in a
  pending document group; permit recomputation after pressure eviction.
- Keep the prepared handle document-group-scoped, never process-global.
- Replay resolved typed units from bounded attempt storage, then clean that
  private keyspace.

## Acceptance criteria

The implemented baseline satisfies these criteria:

- Compatible multi-page PDF OCR requests use the native Florence batch path by
  default, with an eight-item document default.
- A changed PDF is parsed and transformed once for its complete extraction
  operation, independent of the number of OCR pages and publication passes.
- Render concurrency is explicitly capped and never derived from CPU count.
- Lazy backend lanes have independently configurable shares whose sum cannot
  exceed the process-wide aggregate thread ceiling.
- Aggregate pixels and working bytes are admitted before concurrent work, and
  every worker also has a hard live-allocation ceiling.
- Persistent coordinator and planner allocations remain admitted independently
  of render windows; source-cache eviction cannot invalidate an active lease.
- Native integration verifies a second consumer reuses every neutral page-text
  record while applying a different OCR policy without changing geometry or
  text-region metadata.
- Page order and artifact identity are stable across concurrency levels.
- A permanent page failure does not prevent valid siblings from completing.
- A systemic cancellation or allocator failure joins the render wave and
  retains durable retry semantics.
- Profile events identify batched, serialized, chunked, and fallback work.
- The production-mode native integration target submits both pages of a real
  two-page PDF through one document-scoped coordinator, then exercises a
  bounded rendered-page window through reader, generator, and embedder task
  contracts.
- That integration launches a two-worker render wave, reports the independently
  observed active-renderer range, and makes one ordered two-image encoded reader
  callback through the production asset producer runtime. The generic portion
  additionally verifies borrowed Gemma-style generation attachments and one
  ClipClap-style vector per page in one embedding invocation.

The following remain qualification work rather than architectural blockers:

- Run `pdf-model-qualification-test` in the accelerator-backed release lane
  for the exact Florence, Gemma4, and ClipClap artifacts being shipped.
- Run serial-versus-batch Florence output parity against the production model
  fixture in CI; the mixed-EOS state transition already has hermetic coverage.
- Add a corpus benchmark for 1, 4, 8, and 16 pages and publish peak RSS and
  pages-per-second thresholds.
- Promote selected profile fields to long-lived runtime status counters after
  operational use establishes which counters are actionable.

## Open decisions

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
