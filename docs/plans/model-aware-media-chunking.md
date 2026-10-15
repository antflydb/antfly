# Model-aware media chunking and embeddings

Status: proposed; the optional embedding response and automatic video chunking
described here are not implemented.

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
- Typed source coordinates: character span, PDF page/region, animation picture
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
bindings. This plan does not add fields to the advertised schemas yet.

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
| Text | Target-model token windows with explicit overlap and original character spans. |
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
never key reusable results only by a mutable URL. The current model packet-reader
boundary requires immutable borrowed bytes capped at 64 MiB and rejects range
sources. Replace it with bounded source-backed leases or admitted packet windows
before claiming efficient large-file inference.

For dense consecutive windows, advance a retained decoder in decode order. For
sparse selections, choose seeking versus forward decoding from measured costs
and verified restart points. Decode required references while preparing only
selected outputs. Reuse frame preparations and vision/projector results when
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
not change identity. Several embedding enrichments can consume a compatible chunk
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

Before implementation, settle generated schema field names, continuation lifetimes
and source-reference resolution/retention with existing API/artifact conventions.
Do not advertise capabilities until their implementation and qualification land.
