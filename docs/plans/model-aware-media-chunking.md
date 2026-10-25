# Model-aware media chunking and embeddings

Status: implemented across shared planning, native/HTTP inference, SDKs and
chunk/embedding artifact integration. Qualification commands and execution bounds
are documented below.

Antfly must retain the identity and source coordinates of each searchable chunk.
Inference should optionally plan and embed those chunks in one execution, sharing
source reads, extraction, decoding and preparation. Vectors alone hide boundaries
from the database; newly encoded media for every window adds avoidable work.

## Existing behavior

The shared fixed chunker tokenizes text, splits WAV into duration windows, and
extracts GIF frames. Its text tokenizer is not necessarily the embedding model's
tokenizer. Images use model preparation; PDFs use document extraction and
downstream text chunking. These are not a universal model-budget splitter.

EmbeddingGemma 2 embedding takes one ordered group to one vector. Image input
uses image geometry; GIF image decoding uses its first rendered frame; audio
input is limited to 30 seconds. Video supports explicit timestamp intervals and
defaults to 1 FPS with a uniform 32-frame cap. The combined sequence must fit
8192 tokens; video geometry produces up to 140 visual tokens per selected frame.
Oversized groups fail instead of automatically producing multiple vectors.
Explicit groups can share a request-local frame-projection cache.

Transcription already processes long recordings in model-sized windows and
returns source-relative segments. That is the intended long-input execution
pattern, although embedding chunks remain independently searchable results.

Implementation boundaries are the [shared chunker](../../zig/lib/chunker/src/mod.zig),
[model adapter](../../zig/pkg/inference/src/pipelines/embedding_gemma2.zig),
[media](../../zig/lib/media/MEDIA.md) and [video](../../zig/lib/video/VIDEO.md)
libraries, and [enrichment/artifact ownership](../../zig/ENRICHMENTS.md).

## API contract

Extend `POST /chunk` with an explicit optional embedding target. Preserve existing
behavior when it is absent. Keep chunker selection and policy separate from
embedding model, task and dimensions. Resolve the embedding target before planning
so its actual limits constrain boundaries. Normal `/embed` remains one vector per
input group; it does not silently fan out.

The opt-in operation returns a typed `ChunkEmbeddingResponse`. Every result
attaches the vector or embedding failure to the same chunk descriptor:

- Stable chunk ID and source/parent identity, including immutable source version.
- Typed source coordinates: UTF-8 byte span, PDF page/region, animation picture
  identity, or audio/video interval and selected logical pictures as appropriate.
- Resolved chunk/sampling/rendering policy and plan identity.
- Embedding-model identity, task, dimensions and vector, or an explicit error.
- Optional text/media content; otherwise a resolvable source reference and recipe.

Metadata is mandatory when returning embeddings. Content inclusion is independent
of embedding execution, allowing native database calls to avoid payload copies.
Do not return parallel chunk/vector arrays requiring positional reconciliation.
Local and HTTP adapters share typed descriptors and planning. Transport must not
require remuxing video or turning every frame into a PNG.

Implement public schemas in [shared chunking](../../specs/openapi/shared/chunking.yaml)
and the [inference API](../../specs/openapi/inference/api.yaml), then regenerate
bindings. These schemas are the public source of truth.

Responses report complete, partial or failed status, covered intervals, explicit
gaps and a source/plan-bound continuation when resumable. A chunk limit, output
budget, cancellation or failed embedding cannot silently certify full coverage.
Streaming delivers bounded result batches and a terminal coverage record; losing
that record leaves completion unconfirmed. Finite-source completion is distinct
from a live watermark, which never claims end of stream. Legacy `max_chunks`
semantics remain compatible; the new response makes capped coverage explicit.

## Planning and modality policies

Count exact target-model text tokens, wrappers and projected media tokens before
expensive preparation. Reserve output tokens for generation models. Token
capacity, modality limits and admitted memory are independent constraints.
Pressure changes scheduling or fails under existing retry policy; it does not
silently change a chunk's semantic policy.

| Input | Planned boundaries and provenance |
|---|---|
| Text | Target-model token windows with explicit overlap and original UTF-8 byte spans. |
| PDF | Existing extracted page/unit identity, text spans and page coordinates; declared page-image preparation. |
| Image | Declared resizing; separately embedded tiles/crops only under an explicit policy retaining original regions. |
| GIF | Composited logical frames or declared time windows, preserving disposal dependencies and display delays. |
| Audio | Model-compatible duration windows with optional pause/VAD boundaries and overlap; source sample/time offsets survive resampling and codec priming. |
| Video | Consecutive source-time windows preserving sampling cadence, capped by duration, frame count and actual token cost. |

Whole-video overview sampling and retrieval coverage are separate policies. A
uniform 32-frame overview of an hour is not coverage of every interval. At 1 FPS,
a half-open 32-second window can select 32 frames, consuming at most 4480 visual
tokens before text; text or other model limits can shorten it. The live
coordinator's two-second default is not a model limit.

Preserve rational source clocks, VFR, reordered output, fields/partitions and
half-open boundaries. Store exact ticks/timescale or integer nanoseconds; floating
millisecond fields are compatibility/display projections. Configuration epochs
and discontinuities may require separate
windows and explicit gaps. Audio/transcript inclusion is opt-in and time-aligned;
video does not implicitly consume its audio track. Select relevant transcript
spans rather than copying a whole transcript into every window.

## Efficient execution and ownership

Serverless media checkpoints use internal WAL mutation kind 3, rejected by public
mutation admission. Each incomplete page carries an `AFDCP001` output patch and
a reference to one already published source body. Fact roots at version 9 retain
both that source and the authenticated output ancestry, plus bounded completion
witnesses; readers still accept versions 3–8. Query reads and WAL overlays reconstruct ordinary JSON, while
closure and retention read only output pages. Ancestry reconstruction charges
the independent finite replay budget; logical source, output, and working
allocation limits remain separate. Completion publishes a complete
ordinary document snapshot and retires the dependency chain through normal GC.

Retained text batches capture the provider revision before cache lookup and
execution. Native dispatch validates that revision against the loaded executor;
HTTP dispatch retains the planned route token and descriptor revision. Published
hashes and memo keys include the captured revision. Terminal scalar embedding
receipts delete stale vectors and acknowledge failed coverage without model
replay, while a different deployed revision can start a new computation.

The deterministic 64-page fixture with a 256 KiB source closes within a 2 MiB
read allowance. Compact records reduce cumulative derived-body bytes from
3,386,395 to 767,375 across eleven commits, including the identical 526,679-byte
final document. Each of the three fake consumers still executes exactly 64
times. This measures serialized WAL/body payloads, not model throughput.

Container index reuse uses hashed lookup and LRU eviction under the 64 MiB host
admission budget, with a 4096-entry metadata ceiling. Active leases remain pinned
through memory pressure. The deterministic 32-source MP3 regression halves
encoded input reads on its second pass; this measures codec/cache I/O, not live
model throughput.


One source owner pins the version and retains admitted indexes and dependency
proofs across windows. Reuse authenticated ranges, metadata and source hashing;
never key reusable results only by a mutable URL. Source-backed MP4/WebM readers
retain their index and lease bounded packet windows to the model. The 64 MiB
coded-window limit applies to each MP4 dependency closure, independently of the
object length. Sources that exceed index, dependency or read budgets fail with
explicit incomplete coverage.

Each coded window retains verified restart/preroll proofs. Forward AVC
selections retain a bounded decoder owner across windows on portable H.264 and
Apple VideoToolbox. Portable H.264 reuse includes separate colour planes and
parameter-set updates within compatible epochs; geometry/representation changes
construct fresh owners. Portable VP8/VP9/AV1 WebM windows retain entropy/reference
state. Backward selections and previously consumed outputs within a multi-output
packet use verified replay; projections still remain reusable. Eligible Apple
VP9/AV1 WebM retains a request-owned native session across forward windows.
Native callbacks drain before each window's borrowed capture/control expires;
backward requests replay independently without disturbing forward references.
Failed owners are destroyed before retry. Native callbacks drain before borrowed
window state retires, and Apple decoder surface reservations survive until owner
release. MP4 windows borrow immutable coded storage and copy packet metadata only;
dynamic SPS/PPS bootstrap data overlays the first packet. Request-bound selection
uses immutable source/track identity and absolute picture IDs without rehashing
coded dependencies. Standalone untrusted selections retain content verification.
Decode required references while preparing only selected outputs. Reuse frame preparations and vision/projector results when
source dependencies, model identity and display/geometry policy match. Retain
device projections on Metal where possible. Each window still runs its own
contextual language backbone; reused KV state or averaged window vectors do not
imply reference-equivalent embeddings.

Bound queues and caches under shared host/device/command admission. Overlap
read/decode/preparation with model work and batch compatible shapes/lengths
without changing identities. Backpressure reaches the source. Cancellation
fences leases, decoder callbacks and GPU work before release. Delivery retries
retain completed results when admitted; eviction may require deterministic
recomputation rather than unlimited retention.

## Database artifacts and publication

Map descriptors into existing chunk artifacts and vectors into embedding
artifacts, linked through canonical `ArtifactRef` provenance. Inference chunk IDs
identify a source/plan/ordinal; they are not internal database keys or independently
authorized database artifact IDs. Preserve parent document, source artifact/unit,
version and source coordinates. Omitted payloads must remain retrievable or
reconstructable with authorization while that source version is retained.

Boundaries and rendering belong to semantic identity. Model/tokenizer changes
that alter planning invalidate the chunk plan and dependent vectors; embedding-only
changes can reuse an unchanged plan. Batch size, queue depth and concurrency do
not change identity. Changes to extraction, rendering or boundary semantics must
bump the planning recipe version even when the public policy fields stay equal.
Several embedding enrichments can consume a compatible chunk
stream; incompatible plans remain distinct.

A fused inference call is an execution optimization, not a new artifact family or
publication authority. Only the active leader schedules managed enrichment;
existing durability/readiness rules apply. Inference completion does not certify
database index coverage. Resume and retry bind source version and plan identity
and deduplicate results before publication. Search returns source intervals/regions
with explicit parent grouping; averaging chunk vectors does not establish quality.

## Delivery stages and validation

1. Define typed request/response and coordinate schemas, continuation and partial
   failures; add model-aware planning and local/HTTP parity.
2. Implement the source-backed runner, bounded streaming, decoder/projection
   reuse, and CPU/Metal execution through existing admission.
3. Connect database enrichments and SDKs, preserving provenance, reprocessing,
   publication and retrieval navigation.

Compare each planned embedding with independently embedding that exact window.
Exercise token accounting, capped coverage, VFR, GIF disposal, fields, codec
epochs, audio trim, source mutation, continuation and cancellation/retry. Verify
database invalidation/publication separately from inference success. Measure
source/wire bytes, reads, decoded packets, unique frame projections, completed GPU
work, transfers, throughput and peak memory on realistic overlapping inputs.

## Implemented request and delivery

The public route is `POST /ai/v1/chunk` (`/chunk` relative to the inference base).
An `embedding` target opts into this contract. For example:

```json
{
  "input": {"type":"source","uri":"s3://clips/session.mp4","mime_type":"video/mp4","version":"immutable-version-id","version_kind":"s3_version_id","length":419430400},
  "embedding": {"model":"embeddinggemma2","task_type":"RETRIEVAL_DOCUMENT","dimensions":768},
  "policy": {"window_ns":32000000000,"step_ns":1000000000,"overlap_ns":2000000000},
  "config": {"max_chunks":8},
  "stream": true
}
```

String/text and inline base64 `media` inputs use the same planner. Version-pinned
HTTP sources require a quoted strong ETag; S3 supports a strong ETag or version
ID. Every range verifies version, exact object length and returned interval.
Redirects and servers that ignore `Range` fail closed. Configured content security,
S3 credentials, cancellation and byte/request limits apply; credentials are never
accepted in a source recipe. Source length is separate from its admitted read
budget. Retention and authorization remain the caller/database's responsibility.

`embedding.execute:false` resolves tokenizer, processor and manifest assets
without constructing a backend session or importing model tensors. Immutable
identity verification can still read checkpoint bytes on a cold identity cache;
warm verified identities are guarded by file signatures. Planning is not a promise
of zero file I/O. It returns descriptors without computing vectors;
`include_content:true` includes text or independently usable PNG/WAV windows.
Video returns a source/window recipe without remuxing. Native database calls keep
text and descriptors while avoiding PNG/WAV transport encoding. A supplied
`embedding.model_identity` pins the required vector generation. Scoped
`/models?model=…&task=embed` discovery returns the immutable `model_revision`
used to approve local/remote fused-vector reuse before publication.

NDJSON has three records: `chunk.embedding.start` carries source, plan, model
identity/revision, task, policy and starting ordinal before any vector;
`chunk.embedding` carries one descriptor with vector/error; and
`chunk.embedding.complete` carries terminal coverage. Initialization failure uses
`chunk.embedding.error` before a start record. The terminal object's `status`,
`gaps`, `covered_through_ordinal` (exclusive), and `next_continuation` determine
coverage; the event's name alone does not imply success. Consumers acknowledge
an item before its ordinal advances. Resume cursors bind the source version,
planning identity and semantic policy. Chunk IDs alone cannot deduplicate vectors
after weights change: include model identity, task and dimensions.

Text boundaries count the target tokenizer, task prefix and trailing token. PDF
text pages retain page/byte coordinates; image pages render at 144 DPI and retain
page box/rotation. Raster descriptors include the full raster width/height before
tiling, so cropped PDF regions map back to page points even after adaptive render
sizing. Image tiling is explicit. GIF visits composited frames with
bounded canvases, display times and disposal dependencies. Audio boundaries use
sample clocks, actual projected-token counts, optional amplitude/silence boundaries
and a model-compatible final tail. Video retains source clocks and exact selected
picture IDs; the default hold policy samples the picture displayed at each target
time rather than jumping a long VFR hold to the next picture.

MP4 uses one index/configuration owner and bounded coded windows including verified
preroll, reorder and picture-assembly dependencies. WebM shares a logical/header
index across model windows and retains portable VP8/VP9/AV1 reference state for
forward selections, using verified restart points for cold or backward requests. WAV
range input reads/decode only the selected samples. Compressed MP3, FLAC, AAC including SBR/PS, ALAC and Opus/Vorbis paths deliver codec blocks
through a two-block queue and retain only the current PCM window plus overlap.
Container trim, gain, per-channel clipping, and supported WebM timeline gaps are
preserved. Formats/profiles without an incremental visitor and single-threaded execution
retain the bounded fallback. AIFF/AIFC, AU and PCM/G.711 CAF stream bounded
blocks. Chained Ogg Vorbis/Opus/FLAC, including mixed-codec chains, normalize each logical
stream with bounded resampling;
Streaming startup does not decode the source to count samples. The first block
establishes the actual output rate; EOF publishes the final sample count. Bounded
lookahead preserves final-window duration policy, including short tails. Unknown
Ogg chain lengths normalize incrementally with one pending interpolation frame;
unknown-total WebM FLAC retains declared trailing padding in a budgeted ring. Compressed input and packet metadata remain bounded contiguous owners;
this does not yet promise remote packet-range streaming for every audio codec.
Contiguous image/PDF decoders also use a bounded source/decode allocation;
unsupported sizes fail without claiming completion. Model calls reuse the
request's projected-frame cache and bounded CPU/Metal preparation queue. Device
projections assemble in one allocation. One separately admitted following window
can plan/decode/prepare during the current backbone; decoder access stays serialized,
resource pressure retries serially, and abandonment joins before freeing source
storage. Execution
admission includes source ownership, transport/cache allowance, output retention
and existing model resource permits. Resource limits never alter chunk identity.

Finite video selection uses one admitted augmented presentation index, retaining
exact VFR/hold and stable tie semantics. H.264 packet-interval queries bound
field/partition closure work to relevant pictures; immutable request-owned seek
caches avoid repeated IDR packet probes across windows. Compatible projection
cohorts grow to eight within geometry-derived scratch and queue limits. Consumers
do not wait for a full cohort. Known video/text shapes use derived admission
bounds with enforced host ceilings and a final rendered-sequence guard;
uninspected image/audio retains conservative initial bounds. Prepared image/audio
cohorts use known row counts, and every rendered group shrinks its execution lease
before backbone/next-window overlap while retaining live inputs and required
workspace. Finite advanced AVC planning assembles slice headers, fields, planes,
partitions and configuration epochs without whole-clip pixel reconstruction;
entropy/reference validation remains in selected dependency decoding and live
publication. Cohort calibration uses completed backend/geometry measurements and
bounded exploration within admission, rather than assuming the largest fitting
batch wins. Metal vision calibrates layer grouping separately for each geometry
and frame-batch size within its admitted one/two-layer envelope. Owned live windows fill one final snapshot allocation, and sequential
spools use admitted geometric growth with no full-input copy at EOF.
CPU and Metal ordered media groups may share a prepared backbone cohort,
including video and mixed prompts. Towers retain independent source clocks and
part ordering; length buckets bound padding and restore result order. CPU
attention keeps grouped K/V compact and uses bounded dense vision tiles.
Audio cohorts share conformer projections/FFNs while resetting temporal kernels
at clip boundaries. Device media tokens compose without intermediate activation
readbacks. Preparation leases retain ownership while separately admitted encoder
supplements cover larger peaks; cancellation drains backend owners before both
leases release. Cohort pressure falls back only after complete cleanup.
Compatible mixed-group image/audio parts also share tower waves, with one
separately admitted CPU preparation wave overlapping current tower execution.
Scatter maps preserve source part order; errors retain their group identity.
Embedding output keeps token RMSNorm before masked pooling, projects once per
group and normalizes compact vectors. Metal retains the batch through one final
readback. Packed model-owned projections share existing bounded cache classes;
audio packing preserves independently declared clipping bounds.

Flat text arrays tokenize once and share length-sorted encoder calls, with at
most four items, 8192 padded rows and 25% padding overhead. Results scatter to
input order; admission pressure drains and splits a cohort before retrying.
Explicit alternate pooling/projection configurations retain their own contract.
Metal grouped GEMM tiles stop at disjoint item ranges and compute only each
tile's visible key envelope; score storage uses that envelope's stride. CPU
portable grouped attention shares packed K/V across bounded head workers, with
sequential inner GEMMs. BLAS retains its own worker scheduler.
Backbone head preparation reads packed projections directly and combines head
RMSNorm, split-half RoPE and row gathering/compact-KV expansion. Gather indices,
window ranges and aligned device visibility metadata are prepared once per
forward call and retained until completion/cancellation. Vision FFNs share
admitted packed gate/up weights and consume them directly on CPU/Metal without
slice buffers. Image/video position lookups preserve typed table storage and
gather the required rows instead of copying the full 60 MiB F32 position table.

Busy provider queues normalize to retryable capacity errors at chunk execution;
responses preserve the acknowledged prefix and an exact continuation, including
with partial-result delivery enabled. Speculative preparation yields on that
pressure instead of making a successful serial request fail.

Video windows can share one masked backbone execution: the endpoint groups two
already available windows and the native API accepts at most four. Cohorts are
bounded by estimated row length (2048), padding (25%) and shared admission. Metal
bounds total padded rows and uses workspace-bounded layer groups across the
former 128/512-row frame boundaries.
Rejected cohorts keep individual execution and preparation overlap; a compatible
prefetched producer is consumed rather than discarded. Each window retains independent positions and normalized pooling. Decode/projection
stays ordered, and one following preparation wave may overlap the backbone.
Capacity or padding rejection falls back to independent execution after draining
consumed producers. Computed rows are acknowledged individually; sink failure
never advances past the last acknowledged ordinal.

Native unmasked vision uses tiled F32 streaming attention instead of a serial
quadratic score matrix. Image/audio projection assembly allocates its destination
once and copies each cohort into final row offsets. Finite AVC configuration and
picture planning share bounded sparse NAL prefixes; decoder entropy validation
still consumes original coded ranges. The optional prefix cache has its own
admitted ceiling and yields to sparse rereads on long clips.


Database chunk artifacts store the `_chunk` reconstruction descriptor; vectors
remain in the existing embedding artifact family. Compatible fused vectors enter
the normal result cache and guarded publication path. Ordered artifact replay
can reconstruct a missing media vector from the retained parent and pinned plan;
text payloads retain normal batched embedding, including rendered templates and
extracted units. Planning-only recipes never stand in for computed vectors:
text keeps that batched path and deferred media uses pinned recipe replay. Capability revision and model identity gate reuse. A result-cache
hit confers no publication authority and cannot bypass source/readiness fencing.
Native failures retain semantic identities across runtime units through named
worker replies and checked callback statuses. The generated registry and callback
round-trip tests preserve coverage, continuation and source-version failures;
the registry is checked with the other generated contracts.

## Operational limits and admission

`NodeConfig.chunking_limits` and `ANTFLY_CHUNK_<UPPERCASE_FIELD>` configure the
same ceilings in standalone and embedded inference. `/models` embedding entries
publish `chunking_limits` in scoped and unscoped discovery. These limits do not
change chunk/plan identities; a retry can resume under a larger operational budget
with the same source, model and semantic policy. Native callers can request lower
ceilings but cannot exceed the configured node ceiling.

| Field | Default |
|---|---:|
| `max_working_bytes` | 512 MiB |
| `max_output_bytes` | 100 MiB |
| `max_event_bytes` | 16 MiB (also the protocol ceiling) |
| `max_chunks` | 4096 for explicit caps; zero request cap means no count cap |
| `max_dependency_packets` | 8192 (also the current decoder ceiling) |
| `max_coded_window_bytes` | 64 MiB |
| `max_index_packets` | 1,000,000; index memory/metadata ceilings also apply |
| `remote_fetched_bytes` | 512 MiB per execution |
| `remote_requests` | 100,000 upstream reads per execution |
| `remote_cache_bytes` | 256 KiB |

Source/extraction and HTTP result allocation grow admission in small leases before
allocating, instead of reserving the full working/output ceilings for every
request. Bounds wrap reclaiming allocators; failed growth rolls back, and released
allocations return spare reservations. Shared process/model admission still
arbitrates concurrent requests. Model execution workspace estimates remain
independent of source allocation; these changes do not remove backend safety
margins. Buffered HTTP response serialization still follows normal HTTP ownership.

Remote fetched-byte, upstream-request and cache budgets are independent of
whole-object download size policy. Cache hits consume neither another upstream
request nor fetched bytes; the final partial fill uses the remaining wire budget.
Content security, immutable versions and exact ranges continue to apply, including
per-response transport size bounds. The object can be much larger than the bytes
fetched. Logical source lease counters are distinct from wire transfer counters.

Budget errors distinguish working allocation, coded dependency count/bytes,
stream event size, remote fetched bytes and remote request count. Coverage and
continuations survive retryable stops; named native errors preserve those
identities across runtime units. Resource errors never certify complete coverage.

## Reproduction

Run `zig build --build-file zig/pkg/inference/build.zig test-media-chunking`
with the repository's pinned Zig, and `zig build --build-file zig/build.zig
lib-scraping-test` for HTTP range/version enforcement. Run
`zig build --build-file zig/build.zig antfly-media-chunking-test` for artifact
descriptor ownership, remote capability revision and provider transport
qualification, and the storage owner source suite for publication integration.
Run the Go, Python and TypeScript SDK tests for generated bindings and stream
context/order/terminal validation.

With a checkpoint-backed CPU or Metal server, run
`python3 zig/pkg/inference/scripts/qualify_media_chunks.py --url
http://127.0.0.1:18083 --model model --backend native --repeat 2 --stream`.
Use `--telemetry` for admission, transfer and Metal memory counters,
`--video /path/to/real-clip.mp4` for larger clips and `--output` under ignored
`.benchmark-results/`. This checks completed calls against independently embedding
each exact text, crop, composited frame, PDF page, audio window and video interval;
reported warm/cold throughput is backend-specific. Retrieval relevance remains an
end-user workload evaluation, separate from preserving reference embeddings.

For sustained load, run `python3 zig/pkg/inference/scripts/qualify_chunk_load.py
--binary zig/zig-out/bin/antfly-inference-bench-server --model-dir
.benchmark-assets/native-video/model --backend metal --seconds 60 --concurrency 2
--video /path/to/real-clip.mp4 --output .benchmark-results/chunk-load.json`.
The harness checks cold planning without a backend session, page-by-page vector
and descriptor equivalence, disconnect/retry cleanup, completed concurrent
requests, latency and sampled memory/admission/transfer counters. Admission
rejections use bounded backoff and source/plan-bound continuations, retaining
only acknowledged ordinals; generation/context changes and non-admission errors
remain failures. Reports separate admission retries from failed operations and
verify request/scratch admission drains after cancellation and load. Add `--audio
/path/to/recording.mp3` to exercise longer compressed inputs. Sampled peaks
are not allocator-independent RSS high-water measurements. Use longer runs and
larger production clips to qualify a deployment rather than extrapolating one
host's throughput.

Run `python3 -m unittest discover -s zig/pkg/inference/scripts -p
test_qualify_chunk_load.py` for fault-injected retry qualification: partial
acknowledgement, initial admission rejection, bounded exhaustion and refusal to
mix generations or retry non-admission errors.

## Model execution performance qualification

Language, vision and audio immutable preparation reuse belongs to the loaded
model generation with independent admitted cache ceilings. Named vision QKV
projections avoid reopening/widening individual weights on warm execution.
Packed snapshots replace individual copies. Native audio attention and gated
GELU use portable SIMD and bounded parallel work. Metal retains independent
window batching across 512 total rows through bounded layer groups and a capped
packed per-layer projection; it drains GPU work before releasing transient views.

Qualification compares each case against pinned PyTorch CPU/MPS, separately for
prepared text encoder and completed requests. The maintained
`embeddinggemma2/performance_gate.py` rejects mismatched artifacts and insufficient
samples, checks vectors and per-case median latency, and preserves failed
outcomes. Image tiles/PDF pages/GIF frames share vision execution, while their
source extraction and chunk delivery remain complete-request measurements.
Throughput and p95 under concurrent load, cold setup, sampled/process memory and
cancellation/retry are separate gates. CPU-versus-CPU and Metal-versus-MPS claims
must name their timing boundary and measured binary; historical reports do not
qualify a subsequent implementation.


## Stored-vector label preparation qualification

The `antfly-media-chunking-test` target includes production label-path fixtures
for the vocabulary working set and persisted parent receipts. Two passes over
32 small vocabularies make 32 prompt-model calls, compared with 64 under the
former eight-entry limit, with 35,584 retained bytes. This tests the runtime
matrix cache without the optional dense computation memo. The cache uses hashed
lookup and LRU eviction under its 32 MiB budget and a 4,096-entry safety ceiling;
entry headers and matrix allocations hold resource admission, and scoring
leases prevent pressure eviction.

A production `PreparedRows.initRecipes` fixture with 32 windows and 128-dimensional
vectors persists a 14,579-byte parent containing 13,623 bytes of identity receipts.
Label verification retains that receipt array once per parent, instead of
435,936 bytes of repeated receipt strings. Parsing plus selected-window
verification peaks at 215,232 admitted bytes and passes under a 256 KiB hard
limit. Persisted recipes already omit vector and output payloads; this saving
concerns identity metadata. These deterministic counters do not measure live
model latency or process RSS.


## Secondary repair and transcript preparation qualification

`antfly-serverless-test` covers durable secondary-only repair overlays. A
64-parent fixture with a 262,180-byte source retains all 128 healthy index
vectors after every pending publication, including reads through stored fact
bodies. Eleven repair commits serialize 771,215 bytes, compared with 5,794,801
for complete snapshots preserving the same healthy vectors. Both modes produce
the same 526,679-byte final document and make 64 boundary, 64 index, and 128
speech calls across initial preparation plus speech repair. The earlier
4,872,630-byte repair measurement dropped healthy tail vectors; it is not an
equivalent-output baseline. These are serialized payload counts, not latency
or network-throughput measurements.

Repair groups use the plain leased baseline as the compact envelope base.
Queries and builders materialize the repaired prefix plus healthy unvisited
outputs; workers resume the compact cursor against the baseline's accepted
receipts. Materialization checks the baseline and index recipe and never mixes
unvisited vectors from a replaced index generation. Version 9 fact roots retain
formats 3–8 as readable inputs while fencing readers unaware of witness references.

The `benchmark transcript span stamping full scan and ordered sweep` test
compares the production sweep with an output-equivalent full-scan mode. Run it
with the pinned Zig through `zig build antfly-storage-test -Doptimize=ReleaseFast
'-Dtest-filter=benchmark transcript span stamping'` from `zig/`. The fixture
contains 30,000 spans and 30,000 text chunks, includes JSON
parsing and serialization, and compares every output metadata string. Clock
results describe this component on the measured host; they do not qualify live
model latency. Three ReleaseFast runs measured 304.0–304.7 ms for the full scan
and 14.9–15.6 ms for the sweep, with identical metadata outputs. Normal media
tests additionally cover unordered spans/cuts,
UTF-8 byte offsets, phrase clocks, and local speaker scopes.


## Completion audit and classifier admission qualification

A 32-document fixture with 64 parents and 128 vectors per document measured
16,853,824 body bytes read during a settled audit before completion witnesses.
The witness path reads 618,880 bytes: one full body for a fresh deployed-generation
observation and 31 compact witnesses, a reduction of approximately 96.3%.
Both paths make zero inference calls and append no WAL mutations. This measures
object-store body response bytes; it does not include fact-tree routing bytes or
claim a live-model latency improvement. Publication derives each witness from
the exact after-image, commits its inline sources, and preserves all distinct
reusable receipt identities. Mutable source references, incomplete/oversized
summaries and older records use full hydration. Source, pipeline and secondary
model changes invalidate witness reuse; retention follows its immutable body ref.

The classifier's preprocessing lease shrinks to the retained prepared tensor
before model execution acquires its own reservation. For 480,000 waveform
samples, peak scratch admission falls from 28,800,128 to 23,040,096 bytes (20%).
The raw call succeeds under a 25 MiB budget and matches the prepared call's
label and score. The admission controller drains after both paths. This measures
reservation headroom, not RSS or live-model latency.


## Durable input accounting and bounded ranking

Fact-root version 9 stores optional admitted input byte counts. Internal kind-4
completed-output WAL records use an `AFDEN001` envelope; kind-3 checkpoints carry
the count in their descriptor. Materialization transfers that private accounting
into fact metadata, not document JSON. It survives publication, checkpoint resume,
GC, reopen, and policy rebuilds. Public ingestion rejects internal records and
both binary envelopes. An ordinary public upsert resets the count, preventing
caller JSON from authorizing a smaller resource admission. Older records without
trusted accounting retain conservative full-body/base admission.

A source of 262,202 bytes, admitted with that exact input limit and a larger
output allowance, now completes and repairs a changed speech generation without
recomputing healthy boundary/index outputs. Publishing an ordinary upsert of the
expanded body restores full-input admission.

Under a 1 MiB batch budget, the 32-document audit previously hydrated all 32
bodies across 32 ticks (16,854,944 bytes and 32 generation observations). Checking
witnesses before full-body batch admission processes the same cycle in one tick,
reading 618,880 body bytes and observing one generation. A stale witness still
must pass full-body admission before hydration; it cannot overrun the remaining
allowance based on a small witness estimate. Both steady-state paths make zero
inference calls and zero WAL writes.

Classifier and stored-vector labels share a stable bounded heap selector with
caller-defined tie ordering. Classifier scoring still rounds probabilities to
float32 before ranking and preserves both softmax and sigmoid semantics. With
65,536 classes and top-3, three optimized `fast` runs over 64 rankings measured
389.7–392.5 ms for the previous full sort and 24.0–24.2 ms for production bounded
selection, approximately 16x for this component. Smaller 527/1,000-class vocabularies
improved approximately 1.7–2.1x. This measures ranking with equal results and the
same label-string ownership, not end-to-end model latency. Selection scratch is
bounded by top-k, and no full-vocabulary result array is allocated.


## Bounded classifier admission and adaptive batches

Prepared single-frame and batched classifiers share checked scratch accounting:
one owned float32 input, tensor metadata, one bounded selector, and selected
result rows with copied label strings. Raw PCM/raster preparation retains its
separate temporary allowance and shrinks it before execution. Session model
workspace and logits admission remain active. Batch tensors adopt their prepared
storage instead of allocating and copying another full input tensor.

Dynamic exports reduce a rejected batch to smaller supported sizes, including
when the original shape exceeds a permanent resource limit. Accepted frames are
not recomputed. Cancellation, deadlines and invalid outputs propagate without
retry. Fixed larger exports keep their required batch size and padded-tail path;
a denial at the smallest supported size still fails.

A deterministic 32-frame, 224x224, 65,536-class, top-1 fixture using real session
admission reduces peak admitted scratch from 312,606,720 to 221,255,640 bytes
(about 29%). At a 240 MiB cap, it needs one model call instead of 32 serial calls.
At a 64 MiB cap, it succeeds with four eight-frame calls, peaking at 55,317,264
bytes; previously the whole request failed before inference. Results match and
admission drains after success and failure. Top-128 also fits the 240 MiB cap in
one call. Allocation-failure probes cover ownership transfer and ranking cleanup.
For 480,000 waveform samples, current raw/prepared peaks are 17,284,616 and
15,364,616 bytes. These are admission and call-count measurements with a model
stub, not RSS or live-model latency measurements.


## Pause-aware audio planning checkpoints

Pause-aware continuation now keeps a bounded directory of locally derived
`(ordinal, next offset)` checkpoints, separate from inference output receipts.
Known-length random-access providers restore the nearest checkpoint before
reconstructing any gap. The existing ordinal continuation remains the durable
worker cursor; no caller-provided offset becomes trusted planning state.

The directory key commits the pinned source binding, complete boundary plan,
sample rate, sample count, format and inference release version. The plan already
commits source identity/version, policy, model planning identity and capacity.
The shared host-admitted index cache owns immutable snapshots and preserves
borrowed readers during replacement or pressure eviction. Request-local metadata
has a separate bounded admission lease. Admission or allocation denial disables
this optional accelerator, preserving ordinary reconstruction.

An inference-owned disk store beneath `models_dir/.antfly-audio-plans` preserves
checksummed snapshots across process restarts. Fixed hash slots bound storage to
256 snapshots (16.2 MiB), 256 temporary snapshots and 256 empty lock files. Full
keys reject collisions. Per-slot cooperative mutexes and nonblocking OS file
locks coordinate owners sharing a models directory. Writes sync and rename a
complete snapshot; torn/missing records reconstruct rather than weakening
source or model fences. A plan retains at most 4,096 ordered anchors; retention
keeps a uniform ordinal grid, recent repair anchors and the advancing tail,
independent of duration.
Read-only/ephemeral model directories and cache eviction remain safe fallbacks.
Unpinned and unknown-length forward streams do not restore seek checkpoints.
Codec-directory cold starts retain their existing behavior.

The 32-window pinned float32 WAV fixture reads 101,377,408 source bytes when
reconstructing every one-window page, versus 6,145,408 with checkpoints (about
94% fewer bytes). Recreating the inference cache and store owner before every
page reads the same 6,145,408 bytes from the media source. A single full request
reads 6,144,044 bytes. Source coordinates match exactly. Silence cuts, overlap,
44.1 kHz sample clocks, minimum-duration tails, denied admission, sparse-anchor
compaction, checksum/binding rejection and allocation cleanup are covered. This
measures production planning and range reads with a deterministic executor, not
RSS or live-model latency.


## Checkpoint ownership and unchanged repairs

Requests borrow immutable snapshots only while selecting a restart anchor, then
release the view before decoding or model execution. Only the selected point and
high-water ordinal remain. An inline buffer retains at most 4,096 newly derived
anchors (64 KiB), using the same bounded grid retention as retained plans. It needs
no heap lease during inference. Model pressure can reclaim the cached snapshot
without invalidating the selected restart point.

After PCM buffers retire, a request with new anchors reacquires the current
prefix, merges, and publishes under an independent host-admitted allocator with
a fixed metadata scratch ceiling. This metadata never uses the capped PCM
allocator. Missing/evicted prefixes can be recovered from disk. A disk snapshot
loaded for an unchanged repair is promoted into RAM once; later warm repairs
allocate nothing for checkpoints and skip disk validation and cache replacement.
A stale RAM prefix may consult a newer durable snapshot when it cannot reach the
requested ordinal. Admission/allocation failures only discard optional metadata.

A 48,000-sample window with a 350,000-byte PCM allowance previously succeeded
uncached at 323,072 bytes but failed when a warm 4,096-point index occupied that
same allocator. Both unchanged repair and advancing-page fixtures now emit their
window with the identical 323,072-byte peak and no budget denial. Cache eviction
can also release every checkpoint lease before model admission.

For 1,000 unchanged repairs over 4,096 checkpoints, three optimized `fast` runs
measured 69.3–71.1 ms before the ownership change and 0.52–0.56 ms after it. The
fixture now reads zero checkpoint bytes and makes no redundant cache copies,
versus 65,612,000 disk bytes and 65,536,000 copied bytes. This is approximately
130x for the metadata-only planning/checkpoint component, with identical emitted
ordinals and no model execution. It is not live-model throughput or RSS evidence.


## Long-stream spacing and repair backfill

The former every-other compaction repeatedly thinned the whole directory while
retaining a dense new tail. After 100,000 windows, its first gap spanned 75,776
ordinals. New directories retain exact derived boundaries on a power-of-two
ordinal grid, with space reserved for the frontier and 64 recent inserted repair
anchors. A continuously built directory bounds every historical gap by its
current grid stride (32 at 100,000 windows), within the same 4,096-anchor limit.
Recent repair anchors use insertion order; reads do not rewrite their age.

The version-2 disk encoding persists spacing and repair hints and still reads
version-1 snapshots. Legacy gaps are filled only from boundaries reconstructed
by the real planner. A repair retains grid points traversed before its target
and the exact target, even without advancing the frontier. Cache updates merge
off-lock and compare content identities before replacement, retrying concurrent
updates; disk updates merge under the existing slot locks. Neither path can
overwrite a newer frontier with an older repair. Public cursors and identity
fences remain unchanged.

A production-planner fixture repairs ordinal 37,889 in a synthetic 100,000-window
range-backed WAV. The legacy directory reads 151,560,044 source bytes (37,894
reads) for the first repair. After persisting its backfill and recreating cache
and store owners, the same repair reads 4,044 bytes (5 reads). An incrementally
built new directory reads 8,044 bytes (6 reads), approximately 99.995% fewer
bytes than the legacy first repair. All three emit identical source coordinates.
These are deterministic planning/range-read measurements, not live-model latency.
