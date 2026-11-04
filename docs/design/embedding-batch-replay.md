# Diagnosing managed embedding throughput

Use a ReleaseFast build for throughput measurements and an isolated database.
Compare the same model revision, backend, task type, instruction, and exact
batch contents. A chunker's target token count is not necessarily the model's
token count; a mixed-length batch is padded to its longest encoded row.

## Backfill batch formation

The enrichment worker batches rendered text across documents, including text
produced by index templates such as `passage: {{embedding_text}}`. It inspects
the rendered content parts: one text part can use the text API only when the
provider explicitly guarantees equivalent results. Managed text-only adapters
provide that guarantee; custom and declared multimodal adapters retain their
content-parts route by default. Backfill, synchronous embedding and LEANN
re-embedding share the same preparation rule. Media parts stay together in one
invocation, and empty output produces no vector.

Text invocations obey the provider's advertised item ceiling as well as the
execution policy's item and byte limits. Known adapters publish their existing
request/model limits; unknown Antfly endpoints retain a conservative one-item
ceiling. Provider quota pacing still applies. No Ollama-specific batching switch
is needed.

Capability discovery and generation revalidation receive the original request
cancellation token and deadline. Custom providers may retain a synchronous,
non-blocking capability snapshot; I/O-backed discovery must implement the
controlled capability callback or lease resolver. Providers executing an
immutable lease pin the model revision. Legacy callbacks instead revalidate
that revision after each completed partition, before returning vectors or
starting another partition. A stale plan is replanned after yielding rather
than retried inline against the same obsolete lease.

Ordered generated text vectors bind their receipt input to the resolved model
revision. A revision change replaces the old generated result only if the
physical output still matches the pre-inference observation. Duplicate work for
the same revision retains its canonical accepted output, and explicit authored
vectors remain authoritative. Catalog effect protocol 17 and Raft batch protocol
26 gate the new execution identity; older publication/proof bytes retain their
existing digests and decoders.

Grouped multimodal requests preserve ordered parts as one logical input across
native and remote execution. Remote multipart requests use
`input: [{"content": [...]}]` and expect one vector in JSON or numeric responses.
Independent media batches keep their existing one-vector-per-item contract;
a singleton group retains the unimodal wire shape. Framed groups borrow raw
attachments, and JSON groups encode directly into one pre-sized body.
The native provider wire carries the captured model revision. Each acquired
model, including a recovery attempt, must match it before inference dispatch;
post-execution alias observations alone cannot protect against A→B→A changes.

Downloaded media sources are shared across consumers within a preparation
window. Synchronous consumers release idle source bodies at the document
boundary and reuse the empty cache handle; deferred assets take ownership until
execution completes. The preparation
window dispatches at `ANTFLY_ENRICHMENT_PREPARATION_SOURCE_BYTES` (64 MiB by
default), independently of request count. A document's individually bounded
downloads may cross that soft threshold; dispatch occurs before preparing the
next document. Existing resource budgets still bound actual memory admission.

Publication failures remain pipeline-owned for the entire execution quantum.
They stop that lane before later document work and cannot consume another
document's inference retry budget, even when both failures have the same name.

`ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS` and
`ANTFLY_ENRICHMENT_EMBED_BATCH_BYTES` bound text batches (defaults: 32 items and
256 KiB). Per-enrichment execution limits can override these defaults. Batch
counters in `enrichment_runtime` describe logical worker batches; a provider may
split one logical batch into multiple HTTP requests. The worker fills complete
provider batches before scheduling parallel invocations, rather than dividing a
single full batch into smaller equal slices.

## Opt-in capture

`ANTFLY_EMBED_TRACE_DIR` enables text-only captures at the inference boundary.
It must name an existing absolute directory. **Files contain private corpus
text, instructions, and embedding vectors.** Never enable this on sensitive
production traffic without approval. Use a private directory, do not commit
captures, and remove them under your normal data-retention policy after use.
Normal logs do not contain inputs or vectors.

```sh
trace_dir=$(mktemp -d /private/tmp/antfly-embed-trace.XXXXXX)
ANTFLY_EMBED_TRACE_DIR="$trace_dir" ./zig-out/bin/antfly standalone \
  --port 18085 --health-port 14205 \
  --data-dir /private/tmp/antfly-embed-replay-data \
  --models-dir /path/to/isolated/models \
  --inference-host-budget-mb 8192
```

Load a bounded fixture into a fresh managed embedding index on this server.
Wait for that fixture's indexing to finish before replaying, so background
indexing does not compete with the endpoint. Prewarm before ingestion if the
experiment is intended to compare warm paths.

Capture is limited to 64 managed/direct and 64 HTTP requests per process;
the separate quotas leave room for endpoint replay after backfill. Each
request must contain 1–32 text inputs with at most 256 KiB of combined text
and instruction. Files are at most 2 MiB, created exclusively with mode 0600
on POSIX, and never overwrite existing files. Mixed-media and partial-success
HTTP requests are not captured as text batches. Restarting resets the quotas;
use a fresh directory for each experiment. Capture failures do not fail an
otherwise successful embedding operation.

## Replay

From the repository root, with the same server and model still available:

```sh
python3 zig/tools/replay_embedding_trace.py "$trace_dir" \
  --url http://127.0.0.1:18085 \
  --max-batches 8 --batch-size 8 --warmup 1 --repeat 2 \
  --output /private/tmp/antfly-embed-replay-results.json
```

The output file must not already exist. The tool checks batch indices,
dimensions, finite output values, and maximum absolute vector error (default
`1e-4`). It preserves text, task type, and instruction from each capture.
Warmups are excluded from reported HTTP timings. Reports contain timings,
shapes, and request hashes, not corpus text or vectors. Non-loopback targets
require explicit `--allow-remote`; redirects and environment proxy settings
are disabled to avoid inadvertently forwarding private inputs elsewhere.

This is input-identical replay, not a counterbalanced benchmark: the original
managed observations precede the HTTP observations. Thermal conditions,
memory pressure, graph warmup, and competing requests can still differ.
Repeat in a fresh process/database with controlled warmup to test stability.

## Reading timings

Capture timings use a monotonic clock and exclude JSON capture serialization
and file writing from `total`. Instrumentation still adds overhead to outer
managed and HTTP wall times, so disable it for final throughput qualification.

- `admission`: entering the inference admission budget.
- `resolve_manifest`: resolving the model and reading its lightweight manifest.
- `model_acquire`: acquiring/loading the model, summed over recovery attempts.
- `asset_prepare`: preparing required embedding assets/pipeline.
- `tokenize`: prefix application, tokenization, and trailing-token preparation.
- `execution_lock`: waiting for the model's shared execution lock.
- `execute_pool_normalize`: synchronous backend execution, pooling, and
  normalization. This is host-observed time including device waits, **not a
  GPU-only kernel timer**.

Stages are not an exhaustive additive accounting of `total`: tensor setup,
admission around preprocessing, output handling, and other boundary work may
remain unattributed. `attempts > 1` identifies inference runtime recovery;
multiple shapes can also reflect adaptive batch splitting. Shape records
include per-item active token lengths, padded token count, and padded sequence
length. Zero-filled unused slots in `token_lengths` are not extra inputs.

Opt-in `managed embed trace` log lines additionally show pacer/context time,
provider start/end, success, and overall adapter time. Match their monotonic
interval against a capture's `started_ns` to locate bridge/result overhead.
The enrichment worker's existing total embedding timer includes provider
retry/backoff time but not later artifact writes/publication. Inspect retry
counters/logs separately. Timing totals are runtime-scoped; do not attribute
shared-runtime totals to one model when several indexes are active.

If padded-token work explains the gap, investigate bounded length-aware
batching while preserving source/vector association. If lock/acquisition time
dominates, investigate contention or reloads. Do not increase concurrency
before identifying which stage limits throughput.

Background text execution retains the request context through capability discovery and revision fences even when its execution callback is synchronous. The shared text boundary permits that fallback explicitly for background replay; foreground calls continue to require an interruptible executor.

Grouped media execution has an explicit provider callback and wire flag. One ordered text/media group is admitted as one item and produces one vector; independent media batches retain one row per item. Linked inference borrows binary payloads and wraps descriptors in the native ordered-content input, preserving retrieval task, instruction, dimensions, identity, and request controls.

Index admission and local installation share named-media-output validation. Enrichment installation uses a deterministic topological order (kind/name only breaks independent ties), supporting chunk -> media-output asset -> transcript chunks without kind-order assumptions. The planner runs in O((V + E) log V) time and rejects cycles before publishing the reordered plan.

Remote source preparation propagates unexpected renderer/transport errors into the durable request retry budget instead of reporting an empty source. Template directives distinguish permanent 4xx rejection from 408/425/429 and transient transport/server failures. HTTP fetch outcomes retain Retry-After (delta seconds or validated HTTP-date); document extraction forwards it to the existing bounded worker backoff.

The opt-in `linked embedding large image parity across public and grouped inputs` qualification uses `ANTFLY_MEDIA_PARITY_IMAGE` and `ANTFLY_EMBEDDINGGEMMA2_MODEL`. Its pinned NASA fixture is documented in `zig/pkg/inference/src/testdata/large-image-parity.json` and cached under `~/.cache/antfly/conformance-fixtures/multimodal/`. The test verifies its checksum and 6000×4000 dimensions, adds valid JPEG comment segments to reach 16 MiB without changing pixels, serves the image through a local HTTP origin, and compares the public embed vector with the grouped local vector using the same retrieval task. It does not alter model preprocessing or increase inference budgets to obtain parity.

Restore fault injection takes the same catalog pin and per-index apply guard as replay and projection-checkpoint publication. The index-only vector injector must not bypass this writer boundary: doing so can invalidate a posting delta base while a checkpoint worker still holds it. A coordinated regression holds checkpoint admission, observes the injector’s catalog pin, verifies that no vector is added until the writer releases, then completes the insertion. Restore coverage tests retain their ordinary background workers.

Synchronous target-coverage rebuilds acquire external dense admission before taking each index’s catalog/apply guard for the complete capture. This drains streaming replay sessions and excludes checkpoint writers until the capture finishes or aborts. Snapshot runtime repair opens its temporary unpublished destination without replay or optional background runtimes; the eventual serving owner starts them normally.

Text-template artifact reuse resolves one provider generation per batch key before reading cached vectors and includes that revision in the persisted source hash. Both inferred and cache-only publication windows revalidate the generation. Providers without revision metadata retain their existing hashes.

Model-pass chunk preparation and named-output projection share one budgeted source owner and metadata probe. The scanner transfers the heap-owned cache to the deferred asset lane after preparation, retaining the independent batching lanes and retiring source leases only after output consumers finish. Standalone source replay uses an owned instance of the same budgeted cache. HTTP status retry classification is shared with document extraction, including 408 and 425. Ordinary asset chains may terminate at an already validated media-output asset.

The native inference-host gate always executes its tests, so changing opt-in fixture/model environment variables cannot reuse a cached skip. A missing fixture therefore fails even after a preceding run without qualification inputs.

Public HTTP and linked native ordered groups now execute through the same acquired-model grouped pipeline on every backend. The native wire carries the captured model revision, which is checked before dispatch on every recovery attempt. Remote groups use one nested ordered `content` input and require exactly one vector; independent item batches retain their original cardinality. Singleton groups preserve the unimodal wire shape.

Source image decode planning and background preparation waves share the same checked working-memory estimate, including decoder and color-conversion buffers before model resizing. The large-image qualification also compares joint text/image vectors through HTTP and the real native provider bridge, rejects stale revisions, and verifies released request admission.

Grouped encoded audio uses the shared `audio.EncodedClip` descriptor, retaining borrowed bytes and each clip's decode options through HTTP/native parsing, broker shape planning, CPU preparation, and resident/serial execution. Mixed groups never substitute one request-wide codec hint for their individual clips. Serial audio forwarding also preserves the descriptor instead of allocating a byte-only array. Revision-bound local parts requests reject legacy callbacks that cannot receive the captured revision, matching the text boundary's identity contract.

The opt-in `linked embedding audio MIME hints match public and native grouped vectors` qualification takes `ANTFLY_AUDIO_PARITY_MP3` and `ANTFLY_EMBEDDINGGEMMA2_MODEL`. Use the repository's `zig/lib/audio/testdata/tone.mp3` fixture. It adds a leading zero byte so sniffing alone cannot identify the codec, then compares public and native joint text/audio vectors with an explicit `audio/mpeg` declaration under the default model budgets.

Output guards are captured independently of model revision discovery. Protocol 17
publications with unknown execution identity still compare the observed artifact
bytes and revision, so a late unversioned worker cannot overwrite a newer result
or pass an ABA check. Legacy catalog epochs retain their original wire contract.

Encoded audio carries a request-local decoder working-set limit through mixed
preparation, inline execution, audio waves, projected CUDA composition, and serial
fallback. The limit is distinct from the preparation allocator ceiling. Broker
cohorts include this limit for encoded audio; image and PCM-only cohorts do not
split on an unused decoder policy. Public and direct JSON grouped requests reserve
the same resident-media-plus-decoder admission as flat audio requests.

Fetch retry scheduling treats `Retry-After` as a minimum delay independent of the
exponential backoff cap. The supervisor persists the absolute retry deadline,
yields its execution slot while waiting, and restores the same deadline after
restart. Saturating deadline arithmetic prevents oversized server durations from
wrapping into an immediate retry.

Grouped-source activation uses the same catalog protocol planner as normal
inventory publication: it meets current producer requirements and preserves the
installed protocol floor across catalog changes. New `activate_catalog` controls
require the protocol-26 membership barrier. Historical `activate` controls retain
their original replay behavior; readiness detects an insufficient legacy binding
and causes the worker to submit an upgrade. Duplicate current activations retain
the existing epoch and do not invalidate in-flight work.
