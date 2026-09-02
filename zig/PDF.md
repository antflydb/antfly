# Bounded document preparation and multimodal inference

Status: bounded document preparation, indexed reader execution, multimodal
generation transport, distributed model-aware routing, lease-fenced durable
page-image embedding, observed remote execution, and post-review batching
hardening implemented

This document describes how Antfly turns documents into bounded inference work.
PDF extraction, page rendering, OCR, generation, and embedding share document
preparation, media transport, scheduling, admission, identity, and failure
semantics. The same model-work contract also covers reranking, chunking,
extraction, rewriting, classification, and transcription. Every family keeps
its own typed request and output semantics.

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
           rerank | chunk | extract | rewrite | classify | transcribe
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

### Task-specific executors

| Task | Example | Input | Output |
| --- | --- | --- | --- |
| Read/OCR | Florence 2 | page image plus read prompt | structured page text |
| Generate | Gemma4 multimodal | independent message containing a page image | generated text/tool output |
| Embed | ClipClap | page image, text chunk, or audio | vector |
| Rerank | text or multimodal reranker | query plus prepared candidates | ranked candidates |
| Chunk | tokenizer/semantic chunker | extracted page/document text | chunks |
| Extract | GLiNER or multimodal extractor | text, page image, or prepared item | structured extraction |
| Rewrite | rewriter model | extracted or generated text | rewritten text |
| Classify | classifier | one prepared logical item | classifications |
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
    output: enum {
        read_result, generated_text, embedding, ranked_items, chunks,
        extraction, rewritten_text, classification, transcription,
    },
    result_cardinality: enum { one_per_item, one_per_request },
    prompt_policy: enum { explicit, model_default, structured_schema },
    borrowed_attachments: bool,
};
```

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

Capability discovery is scoped to the selected/default inference pool, model,
task, and authentication identity. Antfly clients query
`/ai/v1/models?model=<model>&task=<task>`, where task is one of `read`,
`generate`, `embed`, `rerank`, `chunk`, `extract`, `rewrite`, `classify`, or
`transcribe`, with the same pool and
authorization headers used for execution. The proxy intersects only healthy
endpoints that explicitly advertise the corresponding operation. Bootstrap
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
classification, and transcription surfaces. It routes a
homogeneous bounded batch intact by its nested model identity and rejects
mixed-model batches before forwarding. Refreshed endpoint inventory retains
the advertised task for each model; selection and failover filter by both model
and operation, while the pool-only compatibility fallback is allowed only
before that endpoint's first successful catalog refresh. The cluster catalog
is assembled from healthy inference nodes and merges duplicate descriptors conservatively:
accepted inputs and boolean support are intersected, numeric ceilings use the
minimum, and native batching is advertised only when every eligible duplicate
supports it. If any currently routable node's catalog cannot be read, discovery
fails closed rather than publishing a partial union that routing could violate.
Upstream authorization is forwarded for both discovery and execution.

The long-term proxy contract is model-aware rather than a transparent
round-robin surface:

- expose a cluster capability catalog for every routable model/task;
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

One render window is the default. Optional render/inference overlap uses one
prefetch window and is enabled only after admission reserves the combined peak
of both windows. Estimates guide scheduling; hard bounded allocators, decoder
limits, and model admission remain the enforcement boundary.

### Generic attachment transport ABI

Reader, generator, embedder, and extractor invocations use one versioned
borrowed attachment representation. The same representation is available to
future multimodal rerankers, classifiers, rewriters, transcribers, or other
task families without changing document preparation:

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
    must key snapshots by endpoint, model, task, and an authentication digest;
    coalesce concurrent misses; bound entry count; refresh on a short TTL; and
    use a previously validated snapshot briefly during catalog outages. Raw
    authentication material must not become a cache key.
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
    every initial and retry selection applies that filter, successful discovery
    disables the unknown bootstrap fallback, and cluster discovery fails closed
    if any eligible catalog is unavailable.
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
    limit (256 MiB by default) before forwarding. Inference nodes still apply
    their stricter decoded-media and model admission independently.
36. **Cluster catalog fan-out multiplied memory by node count.** Discovery now
    permits at most eight simultaneous upstream catalog bodies, caps each at
    8 MiB, bounds the merged descriptor set at 32 MiB, and drains every worker
    before returning an error. Catalog scale therefore cannot create an
    unbounded request-scoped memory spike.
37. **Successfully discovered task-unknown models still routed as every
    operation.** Model inventory now has explicit bootstrap, task-unknown, and
    known-operation states. Only known operation advertisements participate in
    normal routing. Bootstrap compatibility is used only while a catalog has
    never completed; a legacy generic entry cannot receive read or generation
    traffic merely because its model name matches.
38. **The proxy catalog described a different pool from request execution.**
    Capability discovery now carries model and task scope and honors the same
    explicit/default pool as execution. The proxy fans out only to healthy
    operation-eligible endpoints in that scope and rejects a refresh if any
    candidate no longer advertises the requested model/task. Unscoped legacy
    listing remains pool-scoped rather than publishing a cluster-wide union.
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
    nine current families: readers, generators, embedders, rerankers, chunkers,
    extractors, rewriters, classifiers, and transcribers. Array-oriented
    extract, rewrite, and classify executors advertise bounded
    `serial_compatibility`; single-operation families advertise `mode = none`,
    `max_items = 1`. No family borrows native-batch claims from another family.
    The proxy also forwards rerank-multimodal, rewrite, classify, and transcribe
    routes.
42. **Chunk routing inspected the wrong model field and execution ignored the
    selected model.** `/chunk` carries model identity in `config.model`, not the
    request root. The proxy now extracts that nested identity and canonicalizes
    legacy fixed aliases to `fixed`. The server advertises exactly the built-in
    executor it can run, returns `fixed` in results, and rejects an unsupported
    semantic model instead of silently executing the fixed tokenizer. A future
    semantic chunker is added only when model resolution and a concrete direct
    executor land together.
43. **Classification was a descriptor without a routable operation.** The
    inference OpenAPI and generated router now expose `POST /classify`; server
    catalog discovery advertises classification-capable classifier or
    extraction models; the distributed proxy routes and scopes the family as
    `classifiers`; and linked execution has a typed classification call.
44. **Several embedded model families stopped at catalog discovery.** Provider
    ABI v23 includes typed chunk, rewrite, and classify operations alongside the
    existing embed, rerank, generate, read, transcribe, and extract operations.
    Linked providers wire all nine current task families to concrete server
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
47. **Array-oriented families advertised single-item execution.** Rewrite,
    classify, and extract requests already carry multiple logical inputs. They
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
    local callbacks and `/ai/v1/models`. Extract, rewrite, and classify resolve
    to the same bounded serial-compatibility contract in either deployment;
    callers no longer upgrade an extractor merely because its provider is
    Antfly.
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
53. **HTTP and linked classification/extraction admitted different request
    shapes.** Both paths now call the same validators. Extraction requires
    1..128 logical inputs; classification requires nonempty texts and labels,
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
66. **Operation URL normalization could drift from the task enum.** Classifier
    URLs were omitted from a hand-maintained suffix list. Primary operation
    suffixes now come from an exhaustive `Task` switch; only explicit
    compatibility aliases remain separate, longest-first. Every current model
    family, including classifiers, normalizes to the scoped model catalog.
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
    does not impose an incomplete second allocator ceiling around it, but still
    validates returned cardinality and result bytes. The per-task defaults are
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
    from executor-owned inference. HTTP adapters remain hard-bounded by the
    supplied allocator; linked local and distributed nodes use their concrete
    decoder/model admission, media caps, deadline, and result caps. This makes
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

### Post-review implementation contract

The hardening above follows these long-term rules:

- A capability snapshot is resolved once per runtime/model/task/auth scope and
  reused by planning and execution. The current implementation uses a bounded
  30-second fresh cache, five-minute stale-if-error interval, and single-flight
  refresh. Discovery failure falls back to compatibility execution; it never
  upgrades an unknown model to native batching. Discovery and single-flight
  waits are deadline-bounded and cancelable; runtime shutdown drains owners.
  Valid catalog failures may use stale or conservative capabilities, but an
  expired or canceled caller never does.
- Every executor owns final admission. Remote read and generation calls and
  multimodal embedding calls are split at both model item and encoded-byte
  ceilings. A single item larger than the model ceiling fails before transport.
- Admission receives a route-owned `InvocationMemoryPlan`, containing the
  selected attachment representation, allocator owner, complete host-boundary
  peak, and independent per-item and aggregate result limits. Media-capable
  producer and part-item embedder implementations fail
  closed when this plan is absent rather than inferring it from model
  capabilities. Linked callbacks charge borrowed bytes, base64 transports
  charge exact expansion, and data-URI adapters charge the complete URI plus
  downstream serialization copy. Caller-owned adapters run under the plan's
  hard allocator ceiling. Concrete inference nodes own decoder/model admission
  for both local and distributed dispatch, avoiding an incomplete outer cap
  and double charging while preserving hard node limits. Every public producer
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
- MIME admission has one essence parser. Parameters are preserved on the wire,
  while capability, modality, and redundant attachment checks compare the
  validated type/subtype essence case-insensitively.
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
   executable chunk, rewrite, classification, and borrowed extraction
   operations an explicit
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
   rerank, chunk, rewrite, classify, and transcribe even when their current
   executor is singleton or serial compatibility. MIME acceptance, item count,
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
9. **Deliberately deferred:** fused inspection/render preparation and a single
   prefetched window require profiling and combined admission. One-window
   execution remains the safe default.
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
    no longer use pool fallback, and an incomplete cluster catalog is not
    published.
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

Renderer scratch and retained PNGs are two ceilings over the same
operation-owned allocator grant. The planner splits that grant before each
window and requires `scratch_bytes + retained_bytes <= available_bytes`; it
also refreshes the cancellation deadline at each window boundary. This avoids
depending on allocator failure to enforce the combined peak.

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
  `PdfRenderSession` discovers the page tree once, and each admitted render
  worker receives a private
  `Reader.forkForRendering` snapshot with its own allocator, caches,
  cancellation probe, render targets, and diagnostics.
- `renderParsedPagesBatchAlloc` accepts an explicit page window, preserves
  request order and page identity, isolates deterministic page failures, and
  enforces batch-page, parallelism, pixel, in-flight byte, retained PNG, and
  per-worker allocator limits.
- One atomic operation reservation owns three disjoint subcredits: PDF text
  inspection, PDF render coordination/workers, and retained/transient OCR
  output. Inspection and rendering use separate hard bounded allocators because
  the inspection reader remains alive while a streaming callback renders a
  window; they can never spend the same logical native bytes. The synchronous
  path uses the inspection subcredit for its initial extraction rather than
  transferring only output credit and leaving native capacity idle.
  The combined native side is partially grantable and the output side is
  required.
  Concurrent documents cannot steal the output credit after admission, and a
  full request gets normal cache reclamation before partial native fallback.
  The output credit is atomically transferred into the `BudgetedAllocator`
  that owns streaming retained state and transient provider buffers (and the
  synchronous path's downloaded source and provider buffers). Native credit
  is partitioned after admission into non-overlapping inspection and render
  ceilings. After the render session is prepared, its remaining credit must
  still fit a minimum raster; otherwise the operation fails before launching a
  worker. Decode limits and render geometry shrink to a smaller partial grant
  instead of admitting a mathematically impossible worker. The available worker
  allowance is recomputed from live coordinator allocations before every
  window. Both the byte-derived pixel allowance and the explicit in-flight
  pixel cap participate in adaptive geometry, so a tighter operator pixel
  limit reduces DPI instead of rejecting an otherwise renderable page.
- Render workers use freeing, task-local bounded allocators rather than arenas,
  so their limits measure peak live memory instead of cumulative allocations.
  Multi-worker waves rendezvous after thread creation and are released from a
  start gate together. Telemetry reports `peak_launched_workers` for the
  deterministic wave width and `peak_parallelism` separately for workers
  actually inside rendering; waiting workers are never counted as renderers.
  Cancellation
  releases the gate immediately, so a late worker cannot extend the operation
  past its render deadline.
  Each worker downsizes an oversized PNG to its request byte ceiling before
  the result is copied into retained batch storage.
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
- Rendered PNGs cross the asset producer and standalone inference ABI as
  generic borrowed attachments. Reader, generator, and embedding operations
  share payload validation and provenance. Providers without a binary callback
  adapt to data URIs only at their final transport boundary. JSON metadata
  repeats payload count, and the inference host rejects cardinality mismatches,
  invalid item indexes, and missing pointers before borrowing bytes.
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
- Mixed-EOS Florence batch decoding uses the same row update helper for the KV
  and full-decoder paths and has a regression test proving that finished rows
  remain padded while active rows retain independent lengths.

The implementation intentionally does not prefetch a second render window.
That keeps memory and backpressure simple: render, infer, merge, release, then
advance. Decoded-pixel handoff and sharing the extraction parse with the render
session remain measurement-driven follow-ups.

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
    bytes_per_pixel_reserve: usize = 12,
    cancellation: reader.CancellationProbe = .{},
};
```

The returned `RenderedPageBatch.results` array is in request order. Each result
contains the explicit page number and exactly one of a rendered PNG or a page
failure. `RenderedPageBatch.deinit` releases every unclaimed result. The
enrichment coordinator may take a result by clearing its optional rendered
value, after which it owns and eventually releases that PNG.

The provider performs these steps inside one call:

1. Validate all limits and page numbers.
2. Reuse the document-scoped parsed reader.
3. Create a bounded render group.
4. Render admitted pages using private contexts.
5. Copy completed PNGs to the caller allocator in coordinator order.
6. Cancel and join workers on a systemic failure.
7. Destroy the parsed document after the final worker joins.

The existing single-page functions remain available for compatibility. New
document OCR work performs adaptive encoded-size retries inside the batch
worker, before retained-output admission.

### 4. Use controlled parallel rendering

Parallel rendering uses two levels of admission in the current runtime:

- Global resource-manager byte admission and bounded enrichment execution lanes
  prevent concurrent PDFs from multiplying memory without limit.
- A per-document worker cap prevents one large PDF from creating an unbounded
  thread group.

The resource-manager admission is one owned split reservation. With the
defaults, the operation must own the 64 MiB OCR transient-output credit and may
own the requested inspection-plus-render native capacity. A partial native
grant is divided proportionally into non-overlapping inspection and render
ceilings; both must remain nonzero when both phases are requested. Another
operation cannot consume the already-owned output side. An unusable native
partition or unavailable required output credit fails before parsing starts.

A separate process-wide CPU permit pool remains an optional follow-up if
operational measurements show that the bounded enrichment lanes are too coarse.

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
`BudgetedAllocator`, and PDF stream decoding retains its existing decoded
stream and peak working-set limits. An underestimate therefore causes an
identified resource failure rather than unbounded growth.

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

## Backpressure and optional overlap

The initial implementation should render one window, OCR it, release it, and
then render the next. This is simple and has a clear peak-memory bound.

An optional double-buffered mode can overlap CPU rendering of window N+1 with
GPU OCR of window N:

```text
CPU render:  [ window N ] [ window N+1 ] [ window N+2 ]
GPU OCR:                  [ window N   ] [ window N+1 ]
```

Enable this only with `prefetch_batches = 1`, and admit the combined memory of
the active OCR window and the prefetched render window. Values greater than one
are unnecessary initially and risk recreating whole-document buffering.

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
    prefetch_batches: 0
```

Suggested operator controls:

```text
ANTFLY_ENRICHMENT_OCR_BATCH_ITEMS
ANTFLY_ENRICHMENT_OCR_BATCH_MAX_ITEMS
ANTFLY_ENRICHMENT_OCR_BATCH_BYTES
ANTFLY_ENRICHMENT_OCR_RENDER_PARALLEL_PAGES
ANTFLY_ENRICHMENT_OCR_RENDER_INFLIGHT_PIXELS
ANTFLY_ENRICHMENT_OCR_RENDER_INFLIGHT_BYTES
ANTFLY_ENRICHMENT_PDF_MAX_DOCUMENT_PAGES
```

Empty, zero, overflowing, and malformed values need explicit semantics. In
general, requested values are clamped to at least one where zero would disable
progress. Parallel pages are hard-clamped to eight. Parallel rendering defaults
to one; operators can opt into higher CPU concurrency only after assigning an
aggregate byte budget that admits it. Prefetch is not implemented and is
therefore effectively zero.

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
- The owned output credit transfers atomically into the allocator that owns
  both retained OCR state and transient provider buffers; manager usage does
  not change during the transfer and exact live-byte accounting resumes as
  buffers are released.
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
- In the streaming path, provider request construction, returned transient
  buffers, and persistent OCR text use the same tracked allocator, so a full
  split reservation cannot starve the retained-state copy it was intended to
  protect. The synchronous path keeps extracted and replacement unit state
  under its inspection ceiling until teardown while its output allocator owns
  provider buffers.
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
  partial fallback, atomically transfer output credit to the live allocator,
  partition partial native grants into persistent/decode/raster budgets, and
  recompute available render bytes before each window.
- Downsize encoded output within each worker before retaining a completed page.

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

Status: optional follow-up. The implemented pipeline intentionally keeps
prefetch at zero.

- Add optional one-window prefetch.
- Admit active OCR and prefetched render memory together.
- Stop rendering when inference backpressure prevents the current window from
  advancing.
- Compare end-to-end throughput and peak memory against the non-overlapped
  implementation.

### Phase 6: Optional decoded-image handoff

Status: measurement-driven follow-up.

- Measure whether PNG encode/decode remains material after Phase 4.
- If justified, define a stable decoded pixel format at the local boundary.
- Feed decoded image batches directly into Florence preprocessing.
- Preserve encoded-image fallback for other reader families.

### Phase 7: Fuse PDF inspection and render preparation if needed

Status: measurement-driven follow-up.

- Measure the remaining cost of parsing once for embedded text and once for the
  render operation.
- If material, add a coarse PDF preparation operation that shares one parsed
  document across text analysis and candidate rendering.
- Prefer a single bounded streaming operation over a long-lived opaque ABI
  handle.

## Acceptance criteria

The implemented baseline satisfies these criteria:

- Compatible multi-page PDF OCR requests use the native Florence batch path by
  default, with an eight-item document default.
- A document is parsed once for its render operation, independent of the
  number of OCR pages.
- Render concurrency is explicitly capped and never derived from CPU count.
- Aggregate pixels and working bytes are admitted before concurrent work, and
  every worker also has a hard live-allocation ceiling.
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
  shared. The safe implementation keeps font and image caches task-local.
- Whether the conservative source-size, decode-working-set, and
  bytes-per-pixel reservation can be tightened with measured high-water data.
- Whether a process-wide CPU permit pool materially improves control beyond
  the existing per-document cap, bounded enrichment execution lanes, and
  global resource-manager byte reservation.
- Whether the long-term pipeline should retain PNG as the local interchange
  format or move directly to RGB pixels after measuring Phase 4.
- Whether extraction and render preparation should share one parse after
  measuring the remaining second document parse.
