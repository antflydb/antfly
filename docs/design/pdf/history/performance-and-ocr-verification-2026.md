# PDF Performance Review and OCR Verification Notes (2026)

> Relocated verbatim from `zig/PDF.md` (lines 4570–4670 and 5118–5192 at commit 0f7fec50c5) on 2026-10-12. Durable decisions were folded into [PDF.md](../../../../zig/PDF.md) before the move.

The first part is the change log from the post-implementation performance review. The second part is the dated end-to-end verification narrative for the resident-OCR, renderer scratch-retry, and OCR-selection fixes.

## Post-implementation performance review change log

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

## OCR verification narrative

Verification of this fix includes the real seven-page remote-PDF forced-OCR
case on Metal: all seven OCR attempts finish without the previous `InvalidShape`
failure. Follow-up verification exposed a newspaper-page
`RenderWorkerMemoryLimitExceeded` and multi-table `ConcurrencyUnavailable`.

The renderer now produces RGBA directly from the serialized macOS compatibility
session: there is no PNG encode/decode round trip or temporary PNG decoder state
charged to a one-canvas output reservation. Native quarter-turn rotation renders
the unrotated canvas in scratch and allocates only the final rotated canvas from
retained-output credit. Both paths keep existing memory ceilings and cancellation
cleanup. Exact-one-canvas allocator regressions cover rotations and compatibility
output parity. CoreGraphics's internal allocations remain outside the portable
allocator contract, as before; this does not claim complete native-framework RSS
accounting. Retained embedded text never counts as successful forced OCR.

The newspaper also demonstrates that geometry plus decoder limits is an
estimate, not a bound on expanded font outlines. OCR/generation and embedding
windows therefore share one admitted scratch-retry path. On a worker-budget
failure, it non-blockingly reserves the delta up to the already configured
scratch ceiling, keeps output credit pinned, frees the joined attempt's outputs,
and retries rendering once with one worker. Growth accepts a partial grant:
the document-working-set slice also covers retained output and metadata, so
requiring the full configured scratch ceiling can reject useful headroom even
with just one page. The resource manager atomically clamps growth to both slice
and host headroom without invoking reclaimers. No model invocation is replayed.
The original deadline, transforms, page identities and output caps remain in
force. If extra admission is denied, the ordered original failures are retained;
if the second attempt still exceeds the cap, it remains a failure. Both grants
are released when rendering finishes, including cancellation/error cleanup.
Regressions cover both output representations, denied growth, repeated failure,
and cancellation. The newspaper renders at its existing 256 MiB ceiling in the
isolated render-window probe. End-to-end verification of `afd021e81` with Metal
and the pinned OHR fixtures additionally passes the three-document, nine-page
forced-OCR text suite, including the newspaper, with no OCR failures and complete
chunk/vector coverage. All three previously failing multi-table E2E cases pass.
These are correctness checks, not an RC-versus-branch throughput comparison.

The additional four-document, ten-page suite exposed an OCR selection bug on
the required-OCR administrative scan. Its persisted page metadata recorded zero
embedded characters and 45 OCR characters. Both candidates were below the
50-character quality threshold, but absent embedded text had a zero
single-character-word ratio and won against the OCR candidate's 0.2727 ratio.
The same failure reproduced in a fresh one-document process; it did not require
cross-document batching.

Selection now checks candidate availability before comparing quality: empty
embedded text cannot beat meaningful OCR. Empty, punctuation-only and prompt-echo
OCR remain rejected, and a short transcription still cannot replace substantial
embedded content merely because its other ratios look cleaner. Selected OCR
that retains quality failures carries `ocr_selected_low_quality`, in addition to
the existing per-candidate quality metadata and any render/provider warnings.
Selection, execution success and transcription quality are separate outcomes.
Regressions cover empty/whitespace candidates, rejected output, nonempty-content
protection, numeric-table preservation and transactional allocation failure.
Previously persisted empty artifacts need explicit reprocessing; restarting a
node does not rerun already completed document extraction.

The scan also contains substantially more visible text than 45 characters.
Recovering a chunk/vector therefore does not by itself qualify transcription
accuracy. Real-model qualification must check content against the source page,
not only nonempty artifacts and searchable-vector counts.

The rebuilt `65084a2c4` Metal binary passes both the isolated scan and the
four-document, ten-page forced-OCR suite, with no OCR execution failures and
complete chunk/vector coverage. All three multi-table regression tests also
pass. Unit verification includes 51 document-extraction/OCR tests in Debug and
13 focused OCR tests in both Debug and ReleaseFast, with no reported leaks.
The scan's recovered 45-character output persists `ocr_selected_low_quality`.
It is an unrelated image description, not a transcription of the source form.
An independent Poppler rendering also produces unrelated text through the
Florence/Metal path, so that quality failure does not require native PDF
rendering. The CPU comparison was interrupted and supplies no parity evidence.
Model artifacts, shared preprocessing and backend inference still need separate
qualification; the structural checks above must not be advertised as OCR
accuracy or RC-versus-branch performance results.
