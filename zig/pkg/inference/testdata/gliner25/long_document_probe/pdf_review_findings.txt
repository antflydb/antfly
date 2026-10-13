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
