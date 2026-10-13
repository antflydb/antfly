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

The target is a bounded streaming pipeline whose intermediate numeric tensors
stay on the inference device. Residency lasts for an admitted batch, rather
than retaining an ingestion corpus in VRAM. Compressed inputs, parsing, request
metadata and final vectors may remain on the host. Device ownership must extend
through preprocessing and the encoder; uploading an intermediate only to export
it again at the next layout or pooling operation defeats the intended benefit.

### Transfer boundaries

| Input | Initial boundary | Subsequent device work |
| --- | --- | --- |
| Image | CPU decode and resize/crop, then upload model-sized pixels | Patch packing, token assembly, attention, pooling and projection |
| PDF page raster | Borrow rendered pixels and reuse image preprocessing | Same vision path as an image; avoid PNG encode/decode between rendering and inference |
| PDF text | Extract and tokenize on CPU | Text encoder; do not rasterize text merely to obtain embeddings |
| Audio | CPU codec decode; current implementation also prepares features on CPU | Target: upload PCM once, then resample, STFT, mel/log normalization and encode |

GPU preprocessing is a throughput option, not an unconditional latency win.
A 4000 x 3000 RGB u8 image is 36 MB; a 224 x 224 RGB f32 tensor is 602,112 bytes.
Uploading the former to resize on GPU can lose to CPU resizing followed by a
small upload. GPU-assisted decode from compressed bytes changes this tradeoff.
Small batches can also lose to launch overhead, and preprocessing shares GPU
compute and memory bandwidth with inference. Unified-memory Metal devices have
a different transfer cost, but host exports still force materialization and
synchronization.

NVIDIA's [nvJPEG](https://docs.nvidia.com/cuda/nvjpeg/) offers hybrid and
hardware-assisted JPEG decoding; availability depends on the GPU and encoded
image. [nvImageCodec](https://docs.nvidia.com/cuda/nvimagecodec/codec_options.html)
provides codec-specific accelerated and CPU backends. Neither makes every media
format entirely GPU-only. For audio, GPU signal processing is feasible separately
from codec decoding; for example, [DALI mel filtering](https://docs.nvidia.com/deeplearning/dali/main-user-guide/docs/operations/nvidia.dali.fn.mel_filter_bank.html)
supports CPU and GPU execution. These are possible implementation references,
not new runtime dependencies in the first phase.

The shared CPU decoder also has a measured [WebP preparation optimization](../../../docs/design/inference/webp-preparation-performance.md):
borrowed lossless prefix tables, short-code lookup and canonical long-code ranges.
It covers VP8L and compressed alpha attached to VP8. Ordinary VP8 decoding and
model resize semantics remain unchanged; codec measurements do not establish
end-to-end embedding gains.

### Implemented first phase: CLIP eager vision residency

`architectures/clip.zig` exposes `visionEncoderForwardTensor`, which borrows
normalized F32 NCHW pixels as a backend tensor and returns an owned embedding tensor
on the same backend. The caller owns the input and frees the output with
`ComputeBackend.free`. It uses existing reshape, transpose, row concatenation,
row gather and embedding lookup operations for:

- NCHW-to-patch packing;
- inserting each image's CLS token and adding position embeddings;
- token/head layout conversion around bidirectional attention;
- gathering CLS rows before visual projection.

The existing host-facing forward automatically uses this path on CUDA and
Metal when these operations are available, exporting only the final embedding.
Metal explicitly stages model-sized pixels into device storage because its
ordinary F32 constructor may return a host-backed handle. CPU callers retain
contiguous host patch extraction. Backends without the
required operations retain compatibility assembly/layout/pooling paths; the
explicit tensor entry point reports unsupported capability instead of silently
exporting activations. Dense Metal patch weights use the backend's row reshape; static quantized
patch-weight conversion remains a separate weight-materialization concern. Backend-internal compatibility fallbacks
must still be considered when profiling residency.

This phase does not change codec decoding, model-specific resize/crop semantics,
CPU audio feature extraction, the CLAP audio encoder's host intermediates, or the
graph-runtime vision entry point. It benefits image and PDF raster inputs that
resolve to the eager CLIP encoder. Other vision architectures need their own
resident handoff and qualification; this is not a claim that all multimodal
models now run a fully resident pipeline.

The regression fixture runs a complete small vision encoder with attention and
projection at batch sizes 1, 2 and 4, compares it with the host compatibility
path, and compares batched rows with independent forwards. Its tensor entry
point runs with backend host exports disabled to catch intermediate readbacks.
Hardware fixtures check Metal device-to-host accesses/download bytes and CUDA
D2H byte counters before exporting final vectors; Metal inputs are explicitly
device-backed. The tests also cover absent optional projection, unsupported
capabilities, execution failures, ownership cleanup and reuse after failure.
These are correctness and ownership checks, not hardware throughput measurements.

Validation on 2026-10-11: the resident fixture passes on the local Metal GPU at
batches 1/2/4 with no device-to-host access during the forward. The CUDA-enabled
Linux test binary cross-compiles; CUDA hardware execution remains unqualified.
Reproduce from `zig/pkg/inference/` with Zig 0.17:

```sh
zig build test -Donnx=false -Dmetal=true -Dtest-filter=clip
zig build test-bin -Dtarget=x86_64-linux-gnu -Donnx=false -Dmetal=false -Dcuda=true -Dblas=off -Dtest-filter='clip resident'
```

Run the CUDA parity fixture on a CUDA host with `-Dcuda=true -Dmetal=false`;
keep the hardware failure visible rather than treating a missing device as a
successful qualification.

### Gemma 4 vision residency

`architectures/gemma4_projector.zig` now retains patch projection outputs as
backend tensors through position addition, the vision blocks, spatial pooling,
optional standardization, RMS normalization and the final projection. GGUF
convolution output is transposed on the backend instead of exported for CPU
layout conversion. The HF patch-linear path retains its exact patch order.
Static position offsets are prepared from the learned weight table on CPU and
uploaded separately; image activations are not exported to add them.

Q/K/V head RMS normalization uses backend operations. Gemma's 2D rotary layout
pairs channels within each spatial axis; the existing GPU vision-RoPE primitive
pairs the global halves. A device transpose converts between those layouts in
both directions, preserving the query scale that compensates for SDPA scaling.
Spatial pooling gathers and adds each merge offset in the original order, then
applies the hidden-width scale. The unified direct image projector likewise
keeps its post-projection bias, layer norms and position addition on the backend.
CPU decode, resize, initial pixel/patch preparation and the direct projector's
first patch norm remain on the host. Providers missing the relevant RoPE or
pooling operations retain their host compatibility paths.

This phase covers vision projector activation boundaries. The later cross-image
batching phase below extends it; GPU codec/resize kernels remain separate work. Scalar clipping already has a
device path; vector clipping compatibility and opt-in diagnostic stage dumps
can still export tensors. Static weight materialization, including HF position
tables, is a separate transfer concern. The public projected image/audio structs
still contain host arrays, so their final export and subsequent prompt upload
remain. The following audio phase extends Gemma 4's existing device encoder operations
with a resident spectral/log-mel frontend.

Synthetic regression fixtures compare head normalization, both query/key rotary
scales, value normalization and spatial pooling against the existing host
reference. They use multiple heads, rectangular grids and merge sizes 1/2.
Patch projection and position addition are checked on native and Metal backends.
Metal counters assert no intermediate device-to-host accesses/download bytes in
these tested operations. Host export is also disabled during the activation
operations to catch accidental readbacks. These checks establish numerical
parity and residency, not an end-to-end throughput improvement or real-model
quality qualification. Validation on 2026-10-11: native and local Metal fixtures
pass, and the Gemma 4 regression filter reports 118 passed / 3 skipped (optional
real-model fixtures and CUDA hardware). The CUDA-enabled Linux test binary
cross-compiles; CUDA execution remains unqualified.

```sh
zig build test -Donnx=false -Dmetal=true -Dtest-filter=gemma4
zig build test-bin -Dtarget=x86_64-linux-gnu -Donnx=false -Dmetal=false -Dcuda=true -Dblas=off -Dtest-filter='gemma4 resident vision'
```

### Gemma 4 audio, CLAP, OCR and Qwen extensions

The next phase removes additional activation exports using existing compute
backend operations. These routes keep the same public vectors, token ordering
and CTC text/confidence semantics. They do not introduce codec dependencies,
CUDA-only preprocessing libraries or cross-request scheduling.

| Route | Resident work added | Remaining host boundaries |
| --- | --- | --- |
| Gemma 4 audio | Zero-padded DFT, magnitude, mel projection, log/floor and frame masking; features feed subsampling directly; valid output rows gathered before export | Codec decode, bounded resampling, Hann-window frame packing; final projected arrays. Metal has the existing specialized conformer kernels; CUDA still uses host compatibility for conformer operations without CUDA kernels |
| CLAP eager audio | Non-fused patch convolution/layout/norm, Swin blocks including cyclic shift/window maps, relative bias, attention masks, Q/K/V, MLP, patch merges, pooling and projection | Mel preparation, input batch norm and spectrogram image preparation; the long-audio AFF fusion frontend remains compatibility work before the resident Swin chain. Explicit graph strategies retain their existing route |
| OCR CTC recognition | Resident session forward, batched argmax, gather winning confidence per timestep | Only O(timesteps) choices/scores downloaded for blank/repeat collapse and dictionary lookup. Image decode, crop/resize, detection/layout postprocessing and Apple-native OCR remain their existing routes |
| Qwen 3 VL | Position addition to patch tensors and base-text/image prompt assembly | CPU interpolation of static position offsets; final visual/deepstack arrays; opt-in qualification hashes deliberately export activations |
| Qwen 2 VL / ColQwen | Patch weight reshape without export/re-upload; spatial vision RoPE on backend tensors | CPU decode, resize/patch packing and rotary position metadata; final API outputs; unsupported attention/RoPE providers retain typed compatibility paths |

Gemma's frontend uses the same 320-sample Hann windows, 160-sample hop,
512-point zero-padded transform, **magnitudes** rather than power, mel
filterbank, additive 1e-3 floor, natural logarithm and valid-frame mask as the
CPU reference. Dense backend dot operations consume frame and coefficient
matrices without treating transient DFT/filter tensors as cached model weights.
This avoids a Metal weight-materialization readback and persistent linear slots.
The frontend keeps the existing 480,000-sample source bound. Packed frames,
spectrum, squared pairs, magnitudes and mel/log tensors add bounded request
scratch; admission/peak-memory qualification must account for those live arrays
before increasing audio concurrency. `TERMITE_GEMMA4_AUDIO_HOST_OPS=features`
(or `all`) selects the CPU frontend for diagnosis. Decode and resampling remain
CPU operations; this is not a PCM-only device pipeline.

CLAP exposes `audioEncoderFromPatchTokensTensor`, borrowing patch tokens and
returning an owned projected tensor. Only small index/mask metadata is assembled
on the host; row gathers implement window shifts and patch merge ordering.
Relative-position tables are gathered and broadcast on the backend, so Q/K/V
and attention activations stay resident. Shifted-window bias includes the same
mask and relative-bias terms as the original CPU attention. Explicit graph
execution is preserved. The expanded window/head bias and attention scratch
scale with batch size, head count and window area; qualify those bytes before
increasing admitted batches. This phase does not add a resident CLAP text tower.

OCR uses the session's admitted resident-output route when available and retains
the host session fallback. It validates a single-image `[1, time, vocabulary]`
CTC output, downloads argmax choices, gathers their original scores on the
backend and downloads those scores. UTF-8 dictionary assembly, repeat/blank
handling and mean confidence use the original CPU semantics. Cancellation and
backend failures propagate through the controlled session permit. This change
does not move detection maps, polygon finding or image crops onto GPU.

Qwen 2's earlier device path used ordinary sequential text RoPE; the new path
uses the spatial axes in merge-group order, matching the existing CPU vision
reference, including repeated temporal slices. Unsupported attention operations
may still use the CPU reference, while resource/cancellation failures propagate.
Qwen 3 position/prompt changes preserve the existing qualification evidence path;
hash collection is an explicit diagnostic synchronization boundary. Deepstack
feature arrays still follow the existing host-facing API.

The `media residency` fixtures cover native and Metal parity for a full CLAP
Swin chain with shifted/unshifted attention and three patch merges at batches
1/2, its batched patch convolution, Gemma log-mel features and invalid-frame
masking, Qwen 2 spatial/temporal rotary ordering, Qwen 3 rectangular position
interpolation and scaled prompt assembly, and CTC text/confidence. Metal
counters assert zero intermediate downloads/accesses for tested numeric
operations; CTC permits only compact O(time) downloads. An OCR session fixture
also verifies that unsupported reductions reuse the resident forward output
without a second inference. Gemma DFT-vs-FFT
comparison uses the existing 2e-3 log-feature tolerance near the mel floor.
CUDA fixtures compile into a Linux CUDA-enabled test binary and require CUDA
hardware to execute. These tests are synthetic correctness/transfer checks;
real-model quality and end-to-end throughput remain unmeasured. Validation on
2026-10-11: 11 native/Metal media fixtures passed (5 CUDA hardware fixtures
skipped); the combined regression filter passed 320 checks with 15 optional
fixtures skipped. The CUDA-enabled Linux test binary cross-compiles.

```sh
zig build test -Donnx=false -Dmetal=true -Dtest-filter='media residency'
zig build test -Donnx=false -Dmetal=true -Dtest-filter=clap -Dtest-filter=gemma4 -Dtest-filter=ocr -Dtest-filter=CTC -Dtest-filter=qwen -Dtest-filter=Qwen
zig build test-bin -Dtarget=x86_64-linux-gnu -Donnx=false -Dmetal=false -Dcuda=true -Dblas=off -Dtest-filter='media residency'
```

### Gemma 4 cross-image batches and measurement

Same-grid images can now share patch projection, encoder blocks, spatial
pooling and final projection. `encodePreparedEmbeddingGemma2Images` borrows
normalized pixels and returns owned, image-ordered projected arrays. Rows are
flattened in image/patch order for matrix operations; SDPA receives separate
batch and sequence dimensions. Both spatial rotary axes restart for each image.
Pooling gathers include the image offset, and learned position offsets repeat
per image. The GGUF projector's multi-image entry also uses bounded cohorts;
its direct unified projector batches patch norm/projection and positional work.

Cohorts contain contiguous images with identical target grids, with no padding
or reordering inside vision attention. Different grids, image-count limits,
patch-token limits and scratch limits split cohorts. `ImageBatchOptions` defaults
to four images, 10,080 patch tokens and 768 MiB of estimated scratch. The
estimate includes dense attention scores plus widened activation intermediates;
it is a conservative admission bound, not an exact provider peak. A singleton
that exceeds a supplied bound returns `ImageBatchBudgetExceeded` before GPU work.
Prepared geometry and pixel lengths are checked before allocation. APIs support
an explicit ceiling of eight images; this does not make eight the serving default.

EmbeddingGemma2's encoded-image and borrowed-PDF-raster paths now use vision
batches before their existing embedding-encoder cohorts of up to four rows.
CPU decode/resize and final token-array export/prompt upload remain. Raster
admission reserves estimated vision cohort scratch, capped at 2 GiB for
accelerators and 512 MiB for CPU. The lookahead path reserves the larger of
vision and encoder phase workspace, with retained/output storage accounted
separately; their temporary workspaces have disjoint lifetimes. Encoded
images reserve host preprocessing before decoding, retain only encoded sources
and normalized pixels while queued, then compose the run reservation under the
model execution lock. GPU reservations are acquired after the lock, so queued
requests do not occupy vision scratch. Native execution can split
vision into smaller cohorts while still batching the downstream encoder. Indexed
codec/validation failures retain original error slots; backend, memory and
cancellation failures propagate. Image-only Gemma4 groups now use the geometry-
qualified cross-request broker described in the follow-up below; mixed groups
retain the compatibility executor. Client concurrency is measured separately
from physical model batches.

The new `bench-gemma4-image` executable loads a real HF EmbeddingGemma2 checkpoint
once and supports three synchronized scopes: `projector` starts from prepared
normalized pixels; `image` includes encoded-image decode/resize; `embedding`
includes decode/RGBA packing, the production raster pipeline, tokenization,
embedding encoding and vector readback. For repeated benchmark inputs the last
scope decodes once into a shared borrowed raster, matching a PDF renderer's
ownership boundary. File reads and model load stay outside the warm interval.
Session-load time is reported separately. Measure a cold first forward in its own
process with `--warmups 0 --iterations 1`; do not mix it into warm percentiles. `--batching off` executes singleton
forwards; `on` requests bounded cohorts. Reported batch count is distinct from
`max_projector_batch`, since the memory limit may split an offline request.
Embedding scope uses the production ceiling of four regardless of requested
batch size. Its projector batch counter is unavailable; it is reported as null.

Reports include raw samples, p50/p95 batch latency, images/sec, final outputs,
bounded host peak and Metal device-owned peak, runtime memory snapshots, host
download bytes and device readback calls. Transfer/workspace counters are
available for direct projector/image scopes; embedding scope's internally owned
workspaces are not attributed by those counters. Runtime memory includes
retained pools/model slots and is not equivalent to transient activation peak.
Upload bytes and a complete synchronization count need provider instrumentation;
device readback calls and runtime frame submissions are useful existing evidence.
Disable intermediate stage dumps and timing diagnostics for measured campaigns.

```sh
cd zig/pkg/inference
zig build build-gemma4-image-bench -Donnx=false -Dmetal=true -Doptimize=fast
zig build bench-gemma4-image -Donnx=false -Dmetal=true -Doptimize=fast -- \
  --model /path/to/embeddinggemma-2 --image /path/to/page.png \
  --scope projector --batch 4 --batching on --warmups 10 --iterations 100 \
  --output /tmp/gemma4-image.json
cd ../../..
python3 tools/embeddinggemma2/image_campaign.py \
  --baseline /path/to/frozen-baseline --candidate /path/to/frozen-candidate \
  --model-dir /path/to/embeddinggemma-2 --image /path/to/photo.jpg \
  --image /path/to/page.png --output-dir /tmp/gemma4-image-campaign
python3 tools/embeddinggemma2/image_campaign.py \
  --url http://127.0.0.1:8090/ai/v1 --http-model embeddinggemma-2 \
  --image /path/to/photo.jpg --output-dir /tmp/gemma4-image-concurrency
```

The campaign driver alternates baseline/candidate order, records binary,
checkpoint/config and input hashes, checks output shape/finiteness/cosine, and
keeps raw artifacts. `--baseline-singletons` compares each batch with singleton
throughput and reports amortized per-image batch latency explicitly; this is not
the latency of an individual queued request. Use `--baseline-batching on` to
compare an older PDF path with its downstream encoder batching preserved. HTTP mode sends one image per request at concurrency
1/2/4/8 and reports successful throughput, latency, errors and serial-vector
parity; overload errors are retained rather than retried invisibly. Run on the
same otherwise-idle GPU, pin backend/weights/quantization and keep binaries
immutable. Use 10 warmups and at least 100 samples for latency distributions;
report cold startup separately. Photos, rectangular grids and document pages
must be compared by actual patch/projected-token counts. Residency and batching
are separate A/B comparisons: pre-residency vs resident singleton, then resident
singleton vs same-grid batches. Synthetic native/Metal fixtures cover distinct
images at batches 1/2/4/8, mixed-grid splits, order, budget rejection and isolation.
Metal fixtures assert only the final projected output downloads. CUDA fixtures
cross-compile; executing them needs CUDA hardware.

### Local Gemma 4 image measurements (2026-10-11)

On an Apple M4 Max (32 GPU cores, 36 GiB unified memory), the real HF
EmbeddingGemma2 checkpoint has BF16 tensors. Each warm case used 10 warmups
and 100 synchronized samples. The prepared square image has 2,304 patch tokens
and 256 projected tokens per image. Each speedup below uses its own paired
resident singleton run, rather than the first row of the table.

| Requested / actual vision batch | p50 batch seconds | p95 batch seconds | Images/sec | Paired throughput speedup | Device-owned tensor peak MiB |
| --- | --- | --- | --- | --- | --- |
| 1 / 1 | 1.459 | 1.517 | 0.682 | 1.015x | 155.3 |
| 2 / 2 | 2.868 | 3.211 | 0.687 | 1.026x | 310.5 |
| 4 / 4 | 5.287 | 5.350 | 0.755 | 1.109x | 621.0 |
| 8 / 8 | 12.449 | 12.978 | 0.663 | 0.971x | 1242.1 |

Batch four improved throughput about 11%; batch eight was about 3% slower
than its paired singleton and used twice batch four's device-owned tensor
memory. Keep the serving ceiling at four. These are local development results,
not a CUDA or GGUF runtime qualification. The separate preliminary residency
comparison measured 1.35–1.42x singleton p50 speedups across square, landscape
and document-shaped inputs, reducing vision device readbacks from 47 to one
final projected-token export. Outputs matched with cosine above 0.99999999999.
Those preliminary runs overlapped some CPU builds and tiny fixture tests. Our
benchmark processes ran sequentially. Unrelated build/test activity appeared
during the four-image full-embedding baseline; affected later timings are local
loaded-machine measurements, rather than controlled performance comparisons.

The encoded-image scope measured about 29% higher throughput for the JPEG
and 35% for the document-shaped PNG. Single-image full embedding throughput
increased about 35%, with p50 falling from 2.31s to 1.73s. The four-image full
embedding comparison observed about 32% higher throughput under resource
contention; treat that row as contextual. Every offline pair passed output
shape/finiteness checks and per-image cosine of at least 0.9999.

| Scope / input | Batch | Baseline p50 s | Candidate p50 s | Candidate p95 s | Baseline images/sec | Candidate images/sec |
| --- | --- | --- | --- | --- | --- | --- |
| Image / photo.jpg | 1 | 2.038 | 1.581 | 1.742 | 0.485 | 0.626 |
| Image / page.png | 1 | 2.511 | 1.834 | 2.246 | 0.396 | 0.533 |
| Embedding / page.png | 1 | 2.311 | 1.735 | 1.826 | 0.424 | 0.572 |
| Embedding / page.png (contended) | 4 | 10.147 | 8.234 | 8.598 | 0.376 | 0.495 |

The HTTP sweep sent one image per request, 100 measured requests at each
concurrency, using the dedicated Metal server and admission budgets recorded
in the report. All 400 requests succeeded and matched the serial vector.
These loaded-machine results show queueing latency rising without a throughput
benefit from additional clients:

| Concurrent clients | Successful requests | Images/sec | p50 s | p95 s |
| --- | --- | --- | --- | --- |
| 1 | 100/100 | 0.546 | 1.769 | 2.187 |
| 2 | 100/100 | 0.560 | 3.429 | 4.438 |
| 4 | 100/100 | 0.539 | 6.523 | 16.115 |
| 8 | 100/100 | 0.492 | 14.002 | 37.571 |

Separate cold-process image runs observed model-session load / first-forward
seconds of 0.104 / 2.253 before residency and 0.086 / 1.563 after. These are
single observations with existing system caches, not warm percentiles or
machine-cold startup qualification.

Real-model HTTP checks also compared four distinct same-grid images with their
singleton vectors and verified that a valid PNG header with a truncated codec
body retains successful indexes 0 and 2 with an indexed error at 1. Malformed
headers are rejected during request validation before decoder execution.
The final native/Metal regression selection passed 165 tests, with 10 optional
skips; residency and image-batching CUDA fixtures cross-compiled successfully.

[The compact results report](../../../docs/design/inference/gemma4-image-performance.json)
records hashes, hardware, latency, throughput, memory-counter availability,
parity, admission budgets and contention evidence. Raw samples, final vectors,
fixtures, logs and measured-source snapshots are retained locally under
`.benchmark-results/gemma4-image-2026-10-11/`. Host peaks cover bounded workspace,
not model memory or RSS; device-owned tensor peaks do not include every provider
buffer. Unavailable internal embedding/HTTP counters are not total-memory
measurements. Relative baseline parity does not replace an independent model
quality oracle. GPU-assisted decode/resize remains future work. Geometry-qualified
cross-request image scheduling is implemented in the follow-up below.

### Gemma4 PDF measurement and resident embedding handoff

Image-only EmbeddingGemma 2 cohorts now keep the projected Gemma4 visual rows
resident through prompt assembly, the embedding encoder, and masked pooling.
Metal/CUDA use backend row concatenation and copy operations to assemble the
prompt; CPU tokenization produces only IDs and masks. Each result downloads
its pooled vector, rather than visual soft tokens and the full encoder output.
The CT owner retains its creating backend until cleanup. Existing model gates,
preprocessing admission, scratch limits, geometry subdivision, cancellation,
and original item indexes still apply. Native and backends without the required
row operations retain the host path. `Options.resident_images = false` selects
that path for a same-binary comparison. Mixed text/media groups and audio keep
their existing handoff; this change covers `embedImages` and `embedRasters`.

`antfly-gemma4-pdf-bench` measures actual PDF bytes through the existing parser,
exact renderer, borrowed RGBA rasters, image processor, projector, and embedding
encoder. It selects pages round-robin across documents and preserves explicit
document/page IDs. A wave retains at most four pages, 256 MiB of output rasters,
and four CPU render workers, each with a private 256 MiB scratch ceiling and
immutable render-fork template. Pages have an eight-million-pixel ceiling and
an adaptive DPI/dimension bound. Model loading and initial document parsing are
reported separately from warm wave timings. No PNG/JPEG encode/decode round trip
is introduced between rendering and inference.

The accompanying Metal PDF painter is a benchmark-only prototype. It batches
eligible pages in each bounded worker group in one command buffer; supported content is opaque, closed polygon
fills and axis-aligned opaque nearest-sampled images. Parsing, codec decoding,
image sample-index tables, and model-exact bicubic preprocessing remain on CPU.
Text, strokes, groups/transparency, complex clipping, shading, patterns, rotated
pages, and other unsupported content reject the entire page before submission
and use the exact CPU renderer. Lossy f32 polygon coordinates also fall back.
Page preparation uses independent render forks in the same bounded worker pool
as CPU rendering. Classification publishes an owned scene or CPU fallback before
fallback completes, so the GPU painter can overlap the remaining CPU work.
Fallback uses its existing private reader instead of a fresh second fork. Parser
scratch remains charged through worker cleanup; every exit joins workers before
releasing scenes or rasters. This does not cache prepared pages between passes.

The painter bins commands into 32x32 pixel tiles and retains original paint order
within each tile. Conservative bounds reject irrelevant commands before polygon
sampling. Tile lists are capped at four million references (16 MiB/page); dense
pages use the same bounds-culled kernel without a tile list. Four synchronous
buffer slots reuse shared Metal storage after command completion, with a 512 MiB
cached-buffer ceiling. `gpu_cached_buffer_bytes` reports that retained storage
separately from model tensor counters. Shared buffers still require logical input
copies and host output copies.

The prototype copies GPU raster output to host to share the existing processor;
it does not establish a resident GPU PDF-to-model pipeline. The first iteration
compares every GPU-painted page with native pixels, rejects any byte error above
one, and reports both maximum error and MAE. This gate is outside measured wave
boundaries. Warm up at least once to exclude its cache effects from measurement.

The campaign compares CPU render/host handoff, CPU render/resident handoff,
and GPU render/resident handoff, plus separate render-only runs at each batch
size. It saves input/checkpoint/binary hashes, ordered vectors, raw samples,
p50/p95, throughput, fallback counts, per-stage observations, retained raster
bytes, worker scratch, and available Metal transfer/device counters. All vectors
must be finite, have 768 dimensions, retain page identity, and match the serial
reference at cosine >= 0.9999. Stage durations measure CPU enqueue time and may
include implicit synchronization; only final readback closes the model timing
boundary. They are not isolated GPU kernel times. Device-owned tensor peaks
exclude weights and provider pools; scratch observations are not total RSS.

[The PDF benchmark guide and local evidence](../../../docs/design/inference/gemma4-pdf-performance.md)
contain commands, corpus provenance, validation, and the limits of the results.
Production PDF rendering remains on the exact CPU path. Promote GPU rendering
only after representative supported pages show an end-to-end benefit, adequate
coverage, and cancellation/admission behavior with owned device rasters. A future
resident raster interface must carry strides, device/runtime identity, completion
events, allocation ownership and its admission lease through preprocessing.
Benchmark an overlapped CPU-render/GPU-inference baseline before choosing it.

### Further implementation and measurement

1. Profile decode/render, resize/feature preparation, H2D, encoder, D2H,
   synchronization and peak host/device bytes separately. Measure end-to-end
   items/s and p50/p95 latency against the CPU preprocessing baseline.
2. Reuse bounded pinned-host/device slabs, streams and scratch. Overlap preparation
   and transfer of batch N+1 with encoding batch N using explicit completion
   events. Cancellation must drain in-flight work before buffer reuse, and every
   buffer must remain charged to existing admission until its last consumer ends.
3. Add model-exact GPU resize/crop/normalize and layout packing. Choose the upload
   boundary using source size and measured costs; keep CPU fallback for unsupported
   formats and small workloads. Preserve interpolation, antialiasing, color,
   scaling and crop semantics and qualify final embedding similarity.
4. Add optional compressed-image GPU-assisted decode for qualified CUDA hardware.
   Keep the codec implementation behind the existing image boundary; CPU formats
   must continue to work. PDF rendering remains CPU initially, with borrowed
   rasters rather than a compressed round trip.
5. Extend residency through resampling/channel mixing and CLAP feature/AFF
   fusion preparation, and add missing CUDA conformer kernels. Qualify windows,
   FFT, filterbanks, log floors, padding and long-clip fusion against each model's
   existing processor. Cache/reuse spectral coefficient matrices and scratch
   after measuring the bounded frontend against the CPU FFT/BLAS baseline.

The microbatch broker remains responsible for cross-request grouping and indexed
results. Bucket images by resolved dimensions/patch count and audio by feature
length to bound padding. Device bytes, decoded pixels, audio windows and scratch
must limit batches alongside item count. Every overlapped buffer counts against
the working-memory ceiling, including both batches in a double buffer. Preserve
per-item decode errors, original input indexes and caller cancellation; do not
retry a failed model batch merely to recover residency.

Qualify batches 1/4/16/32 where admission permits, mixed resolutions and audio
lengths, cold and warm execution, and saturated GPU workloads. Enable additional
GPU preprocessing by default only when end-to-end evidence shows a benefit on
the qualified backend. A faster preprocessing kernel alone is insufficient.

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
- Model-family implementations: native Florence can use a real batch fast path; VLM, GenAI, Pix2Struct, and multistage OCR may keep the serial fallback until their runtimes expose safe batch execution.

`/read` can also use Qwen3-VL, a general vision-language model, for OCR and
document transcription. Split GGUF generator bundles are accepted when their
selected artifacts and backend pass the normal runtime compatibility checks.
Qwen requests reuse the resident native Metal generation pipeline and execute
one image at a time so every input still maps to one independent read result;
the outer request admission covers the complete serial batch. This is a
correctness path, not a claim of batched Qwen projector or decoder execution.

Native Florence batching is throughput-oriented. A request batch is chunked by `ANTFLY_INFERENCE_READ_BATCH_SIZE` (default 8, clamped to 1..64), preprocessed into `[batch, 3, H, W]`, then run through the Florence encoder once for the chunk. CUDA and Metal use the incremental decoder KV cache in batch mode when available: self-attention keys/values are appended per row, cross-attention keys/values are precomputed for the whole encoder batch, and the LM head is applied over `[batch, hidden]` at each generated position. Metal exposes the generic eager backend hooks needed by the preallocated KV slab path (`allocUninitF32Shape`, `copyRows2D`, and device-backed row concat/slice) and has a batched device `attention_f32` entry that dispatches all `batch * q_len * heads` rows in one Metal command. The reader pipeline does not need a Florence-specific Metal API. Rows that emit EOS stop contributing new text tokens while the rest of the batch continues. If the batched KV path is unsupported by a backend shape or operator, native Florence falls back to the full batched decoder path; non-native and unsupported reader families keep the serial fallback behind the same `LoadedReader.readBatch` contract.

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

The historical matrix follow-up used `ANTFLY_GEMMA4_METAL_OCR_MULTIROW=1`
within the experimental OCR lane. Current builds additionally require explicit
`ANTFLY_GEMMA4_METAL_OCR_MATRIX_ARITHMETIC=1` for that research path; otherwise
cohorts preserve the complete singleton operation family. In the matrix path,
QKV, FFN, PLE and vocabulary projections span the active rows, with
per-page RoPE/attention and frame-local F32/F16 KV suffix writes. The four-row
corpus run submitted one frame per multi-row step. A subsequent patch selects
parallel RMS normalization for short matrices and groups projections into
tracked compute scopes. Four-row recognition improved to 40.139s versus
54.501s for ordinary generation, with identical transcripts in both eight-page
passes. Two-row repeats changed a diagram transcript. This switch remains
opt-in pending broader qualification; the normalization rollback is
`TERMITE_METAL_DISABLE_SMALL_ROWS_NORM_REDUCE=1`.
A later campaign changed a diagram transcript even with the new specialization
option disabled, so the earlier pass does not establish repeatable exact text
parity. `TERMITE_METAL_ENABLE_Q4_0_SHORT_ROWS_SPECIALIZED=1` adds opt-in exact
column-count Q4_0 kernels for two and three F32 rows; other rows retain their
existing kernels. Six focused checks pass with this option on and off. Shared
machine timing drift prevents an additional end-to-end speed claim.
`ANTFLY_GEMMA4_MTP_LONG_CONTEXT_FRAME=1` also qualifies canonical one-token
hidden/choice recovery past the local-attention window. K=2, including bonus
acceptance, matched the complete OCR corpus but did not pass the speed gate.
`TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_INPUT_FRAME=1` starts the verification
frame before token embedding and PLE preparation for dense Gemma4 suffixes of
two to four tokens. The direct decoder adopts that frame; preparation failures
cancel only a frame owned by this call. Existing debug/compare and direct/frame
disable policies still apply. Pair it with the existing
`TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_TAIL_FRAME=1` to keep prepared vocabulary
readout and argmax in the frame too. Both options default off, and their matching
`TERMITE_METAL_DISABLE_GEMMA4_MTP_VERIFY_{INPUT,TAIL}_FRAME=1` switches override
the enables. This changes command framing, retaining the existing weights and
arithmetic. The target vocabulary head is already Q8_0; the earlier BF16-head
attribution was a PLE projection.
`ANTFLY_GEMMA4_MTP_SEED_CACHED_TARGET=1` is a separate experimental opt-in for
fixed K=2 resident greedy drafting with `ANTFLY_GEMMA4_MTP_ADAPTIVE_K=0`, final
hidden states, default constant position and canonical recovery. Recovery
retains the normalized target activation paired with its cached next choice;
the next round uses that known choice as its first candidate and runs one
assistant prediction for the second. Initial/predictor-only activations do not
qualify, deferred recovery and other K values use existing behavior. The
`cached_seeds` phase counter distinguishes known target choices from physical
`draft_steps`. The total two-token verification shape and target acceptance
remain unchanged. Full-corpus OCR qualification omits the last paragraph on
one page with this option enabled, including with both frame opt-ins disabled;
the seed lane has not passed the output-parity gate and defaults off.
The clustered Metal drafter head requires
`ANTFLY_GEMMA4_MTP_ENABLE_MASKED_EMBEDDING=1` and
`TERMITE_METAL_ENABLE_GEMMA4_MTP_CLUSTER_HEAD=1`. It scores selected clusters
on the GPU and returns a scalar token. The target retains its full vocabulary
verification. `TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_REPLAY=1` optionally replays
its four dispatches with a stable input buffer; released weights invalidate
the template. This replay scope covers the drafter head, not whole target
KV/RoPE/attention encoding.
`ANTFLY_GEMMA4_MTP_RECOVERY_CHOICE_ONLY=1` skips unused hidden snapshots during
canonical KV/choice recovery, while retaining snapshots when an activation is
needed. `TERMITE_METAL_ENABLE_GEMMA4_DECODE_SCRATCH_ENVELOPE=1` reserves shared
singleton scratch at layer maxima for uniform KV-head geometry.
All are experimental opt-ins; the design doc records complete-output and timing
gates separately from isolated operator measurements.
See the design doc below for source pins, flags and measured limits.

[The OCR optimization investigation](../../../docs/design/inference/gemma4-ocr-optimization.md)
compares the identical GGUF model/projector with llama.cpp, profiles optimized
Metal decode hits and transfer/wait counters, and keeps generative projected
image features and prompt packing resident. The embedding measurements above
are separate from OCR text generation.

`ANTFLY_GEMMA4_METAL_OCR_CONTINUOUS=1` extends the explicit OCR batch lane to
an eight-page resident window by default (`ANTFLY_GEMMA4_METAL_OCR_WINDOW`,
maximum sixteen), while GPU cohorts remain bounded by
`ANTFLY_GEMMA4_METAL_OCR_BATCH_ROWS` at four. Finished leases retire immediately
and remaining ready pages fill subsequent cohorts. Manifest/media limits still
bound materialization and all live KV caches remain covered by admission.
This operates within one explicit batch request. It does not enable a global
cross-request image service or promote the matrix lane's output parity.
`ANTFLY_GEMMA4_METAL_OCR_CONFIRM_EOS=1` optionally recomputes a batched stop
choice through the canonical prepared singleton at the same KV position before
retiring a page. `TERMITE_METAL_TRACE_GEMMA4_OCR_STOP=1` records those choices.
This checks termination only, not the equivalence of earlier matrix steps.
Continuous scalar cohorts preserve the measured transcripts but are slower;
matrix cohorts have intermittent paragraph omissions. All options default off.

An experimental `ANTFLY_GEMMA4_METAL_OCR_BATCH=1` lane coordinates independent
GGUF Gemma4 page decoders at two or four rows. It holds an outer model/provider
lease, serializes projector and prefill work with a distinct step mutex, reserves
all live KV/scratch allocations, retains per-page paged KV identities and uses
device argmax for eligible greedy rows. Finished requests retire from the
scheduler promptly; ordinary singletons retain the optimized direct decoder.
The default Metal multimodal generation lane stays serialized. Larger envelopes
alone do not qualify a native decoder batch.

`ANTFLY_GEMMA4_MTP_MULTIMODAL=1` is a separate experimental lane. It seeds a
compatible MTP assistant from the actual image prefill's final hidden row and
target KV. An unavailable image hidden state fails instead of reconstructing a
text-only placeholder prompt. Warm OCR speed, draft acceptance and exact
transcript parity must pass before changing the existing automatic policy.

For parity isolation, `ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL_STEP=1` shadows
complete matrix steps with canonical singleton steps at the same KV positions
and emits canonical choices, restoring canonical history for the next step.
`ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL=1` compares and canonically restores each
layer instead. They are mutually exclusive diagnostics, perform host readbacks,
and cannot qualify throughput. Logit traces include the top-two margin.
Scoped `ANTFLY_GEMMA4_METAL_OCR_CANONICAL_{QKV,PLE,TAIL}=1` rollbacks keep other
matrix work enabled. `TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN=1` explicitly
overrides the FFN opt-ins; setting an enable flag to zero is insufficient.
All options default off. The source-pinned `tools/bench/gemma4_ocr_sweep.py`
checks full transcripts/counts/caps, cohort/window sizes, reversed page order,
and fixed MTP K=1–4 before ranking time per committed token.

`TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME=1` appends clustered-head scoring
to the assistant frame, removing its separate submission/wait. It preserves
caller ownership with an empty frame after completion; the caller cancels it.
This is an opt-in synchronization change, independent of ICB head replay and
of the existing target verification-input/tail framing switches.

The canonical tail rollback completes the backbone frame, then uses the existing
fused singleton norm/head/argmax per row. It is mutually exclusive with parity
diagnostics. The initial generic in-frame packed tail is rejected for failing
transcript/cap gates. Buffer-copy transitions now use their explicit runtime;
Shared copies inside active/submitted frames follow GPU producers instead of
reading pending values on CPU.

For memory experiments, the sweep samples the server/worker process-tree RSS,
which can double-count shared mappings. The optional `metal_memory_probe.m`
records worker `MTLDevice.currentAllocatedSize` after waits; its sampled maximum
is not an instantaneous peak. Probe-injected runs are diagnostics and excluded
from throughput ranking.

The HTTP encoded-payload cap is independent of the resident window. Large inline
media batches can exceed the default 10 MiB before reaching inference.
`antfly inference run --http-max-body-mb 32` (run JSON `http_max_body_mb`) selects
a finite larger cap; the default remains 10 MiB and aggregate HTTP body retention
and model/KV admission remain bounded. The qualification sweep records the cap.

The final bracketed sixteen-text-page sweep matches all 9,126 completion tokens
with four-row/eight-window canonical QKV in both passes: 93.548/92.430 seconds
versus ordinary controls of 123.357–129.584 seconds. It also matches both
reversed eight-page passes. Two-row/four-window fails the reversed-order gate,
and four-row/four-window canonical QKV fails the expanded-corpus gate. Increasing
the request window to sixteen is not an established additional speedup. These
are per-campaign recognition measurements over pre-rendered pages; matrix
batching remains opt-in: the memory-instrumented four-row/eight-window QKV run
also omits the paragraph, so that rollback is not a robust parity fix. See
[the investigation](../../../docs/design/inference/gemma4-ocr-parity-investigation.json)
for source/binary pins, exact flags, latency/memory counters and rejected cases.


Native Gemma4 OCR cohorts now preserve the complete prepared singleton operation
family by default. The free-running matrix trace reproduced a premature EOS at
sequence length 1,011 after an otherwise matching prefix, omitting 41 tokens.
The legacy `ANTFLY_GEMMA4_METAL_OCR_MULTIROW=1` alone selects canonical cohorts;
shape-dependent matrix arithmetic additionally requires explicit
`ANTFLY_GEMMA4_METAL_OCR_MATRIX_ARITHMETIC=1` (or a differential diagnostic).
Canonical cohorts retain independent GPU KV and one shared layer-major frame;
this correction does not require CPU image/hidden/logit processing. Matrix
research timing gains do not qualify the normal serving path. The
`cohort4w8stable` sweep case records its real native cohort occupancy separately
from matrix-frame profiling. [The correctness report](../../../docs/design/inference/gemma4-ocr-paragraph-correctness.json)
retains the first changed logits, failed scoped rollbacks and final corpus gates.

The experimental `TERMITE_METAL_Q4_0_BATCH_INVARIANT=1` policy selects singleton
Q4_0/F32 reduction geometry for two-to-eight-row projections and dispatches over
the actual row dimension. Fused gate/up uses one cohort dispatch; attention and
FFN projection/norm/residual tails retain the singleton family per row,
including the default-off sum-of-squares policy. Use the
`cohort4w8invariant` sweep case to include canonical QKV, PLE and head families.
This does not enable matrix OCR serving automatically. Kernel bitwise equality
is insufficient to qualify full transcripts, and the repeated-page campaign
shows no throughput gain. [The design and evidence](../../../docs/design/inference/gemma4-ocr-optimization.md#singleton-invariant-q4-cohort-dispatch)
retain output gates and exact source/binary/corpus pins.


Further Metal Q4_0 experiments separate weight sharing from dispatch/allocation
changes. `TERMITE_METAL_Q4_0_BATCH_SHARED=1` shares packed weight tiles and paired
half-to-float scales across two/four rows, preserving the selected singleton
lane/reduction geometry; unsupported threadgroups fall back. Native output
checks pass, but the first representative operator measurements regress most
shapes, so weight sharing remains an independent research switch.

`TERMITE_METAL_Q4_0_BATCH_FUSED_TAILS=1` requires the invariant policy and keeps
its float32 attention/FFN projection, normalization and residual family in two
cohort dispatches. Optional sum-of-squares stays off by default; float16
projection retains the per-row fallback. `ANTFLY_GEMMA4_METAL_OCR_BATCH_QKV=1`
batches eligible zero-bias Q4 Q + paired K/V scratch without per-row packing.
`ANTFLY_GEMMA4_METAL_OCR_REUSE_CARRIERS=1` reuses private four-row buffers for
attention/FFN results and other semantic carriers, with retained growth views
and existing encoder/blit hazards. PLE and final head arithmetic stay canonical.

Sweep variants `tails` and `pool` qualify tails/QKV and then carrier reuse with
weight sharing disabled; `share`, `fused` and `reuse` measure the combined
sharing experiments. `invariantprofile` and `optimizedprofile` add sampled GPU
stage and head-wall/allocation counters; all diagnostics and allocation probes
are excluded from timing recommendations. Full-transcript/count/cap gates and
bracketed repeated ordinary controls are mandatory. See the
[shared-tile design](../../../docs/design/inference/gemma4-ocr-optimization.md#shared-q4-tiles-batched-tails-and-carrier-reuse)
for scope, rejected prototypes and qualification evidence.


`ANTFLY_GEMMA4_METAL_OCR_HEAD_FRAME=1` additionally groups two-to-four quantized
heads in one prepared frame after the backbone, preserving actual one-row
checkpoint norm/head operations and batching suppressed argmax/readback. Dense
heads and final single rows keep the previous route. `head`/`headprofile` add it
to the non-sharing `pool` configuration. This removes head waits without claiming
shared head projection or enabling an approximate/transformed head. GPU stage
samples in this configuration can include both backbone and head frames.


At runtime `7f4d7fa577`, bracketed two-pass eight-page OCR preserves all 4,057
completion tokens/transcripts in every condition. Ordinary takes 55.339–57.572
seconds, `pool` 52.206/52.843 and `head` 48.819/49.304. The latter reduces mean
recognition time by 12.8% in this campaign with weight sharing disabled. It
remains opt-in; rendering/startup and unbracketed expanded-corpus timings do not
establish an overall PDF throughput gain. [Pinned evidence](../../../docs/design/inference/gemma4-ocr-shared-batching.json)
records controls, excluded noisy runs, buffer reuse, per-output hashes and
native checks (66 selected enabled / 101 default, zero skips; nine sweep tests).

The extended sixteen-page head configuration preserves all 9,126 tokens forward,
reversed and with GPU allocation instrumentation. Across the recorded native
conditions, 128 page observations / 79,222 tokens match references. Instrumented
Metal/RSS measurements and GPU stage diagnostics remain separate from throughput;
head-frame samples include a different mix of work than backbone-only samples.
