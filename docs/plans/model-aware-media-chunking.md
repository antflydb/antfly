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
uninspected image/audio retains conservative bounds. Owned live windows fill one
final snapshot allocation, eliminating the second full encoded-byte copy.


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
