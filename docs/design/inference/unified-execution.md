# Inference preparation and execution

GGUF, safetensors and imported ONNX models share preparation and execution
contracts. Architecture adapters describe model semantics and checkpoint layouts;
backends retain their tuned kernels and whole-model implementations. Deployment
uses the existing platform and driver interfaces without an external inference
framework.

## Ownership

Paths below are relative to `zig/pkg/inference/src/`.

| Owner | Responsibility |
|---|---|
| `architectures/model_definition.zig` | Backend-independent configuration and loaded weight sources |
| `models/session_contract.zig` | Resolved task, processor, padding, pooling, normalization and modality semantics |
| `backends/session.zig` | Published session contracts and typed backend acquisition across wrappers |
| `backends/compute_context.zig` | Common backend construction, exact typed imports and shared context lifetime |
| `execution/program.zig` | Optimized IR, composed node mappings and input/output/weight/state bindings |
| `execution/executable.zig` | Preparation, reusable execution, output placement and optional diagnostics |
| `execution/model_candidate.zig` | Admission and dispatch of specialized whole-model implementations |
| `execution/physical_buffers.zig` | Assigned outputs, transfers, workspace and bounded invocation frames |
| `execution/decode_state_runtime.zig` | KV/recurrent/paged state ownership and advancement |

`execution/root.zig` exposes the common `ModelProgram`, `prepare`, `Executable`
and `BackendContext` boundary. Imported, directly constructed and traced graphs
use the shared fixed-point optimizer. Public and state bindings follow composed
node mappings through rewrites. Already analyzed graphs can borrow their cached
analysis and immutable weights during shape specialization.

## Execution invariants

- Select a backend or specialized candidate using dtype, shape, layout, storage,
  semantics and required features. A model candidate may decline before
  submission; errors after submission propagate without replaying partially
  advanced state through another implementation.
- Separate legality from profitability. Placement accounts for computation,
  launches and transfers while preserving exposed outputs and stateful regions.
  Unsupported signatures fail explicitly or use an observable admitted fallback.
- Bind eligible operations to planned physical outputs and workspace. Native
  schedules and compatibility arenas remain available behind their existing
  admission rules. Logical lifetimes include reordered submissions and aliases.
- Retain storage leases for views and published results across frame reuse,
  executable eviction and producing-session teardown. External contexts must
  outlive values that do not retain their context themselves.
- CUDA capture retains stable operand, output and workspace addresses. Completion
  protects reuse and retirement; captured executables retire before their storage
  owners. Reset and cancellation preserve the existing state/capture guards.
- Resident stage contracts keep intermediates on their admitted backend. Explicit
  host export reads directly from the producer. Typed transfers preserve storage
  bytes or reject the operation; they do not silently convert through F32.
- Diagnostics record actual dispatch, allocation/reuse, transfers and bytes,
  cache behavior and fallback. Nested transfer audits cannot weaken an outer
  strict policy. Planner-only assertions do not establish execution parity.

## Model binding and retained native execution

Model configuration, checkpoint names and model layer composition belong to
architecture and backend adapters. `backends/embedding_gemma2_cuda.zig` binds EG2
configuration and statically composes existing CUDA operations. The operation
bindings in `ops/cuda/encoder_multimodal.zig` contain no model configuration or
checkpoint imports. Historical device exports remain artifact ABI compatibility
names; new host APIs use operation names.

`ops/cuda/retained_execution_cache.zig` owns reusable buffers, capture, completion
and retirement. Its compile-time recipe supplies admission geometry, complete
capture identity and compatibility checks. The model adapter compares the entire
configuration, numerical policy and resolved route before reusing a graph. This
separation introduces no runtime kernel interpreter or virtual dispatch.

Prepared media rows enter the existing `ModelRuntime` prefill request as a
borrowed `execution/resident_rows.zig` value. Geometry, physical readability and
provider identity are checked before state reset or KV mutation. Separate wrapper
objects require an explicit live operation capability proving the same provider.
The synchronous call does not retain borrowed rows. Token lookup and prepared
rows then use the same qualified Metal transformer and tail schedules, chunking,
KV ring policy and output intent. Intermediate chunks can publish KV only.
Automatic media selection is limited to the qualified F16 profile; explicit
modes and unsupported precision retain their existing selection rules.

These changes reuse `ModelProgram`, `Executable`, candidate admission and physical
buffer planning. They do not make the portable whole-model Tile prototype a
production replacement for native tuned programs. Numerical sharing and schedule
qualification remain separate from moving model ownership.

## Shared stage adapters across supported models

`architectures/frontends/` owns the semantic stage adapters for Whisper's
encoder/decoder, dense Qwen-family decoding and prefix programs, companion and
patch projectors, embedding media, structured extraction heads, and contextual
marker classification for GLiNER-DECIDE. The loaded
session publishes them through `execution/model_frontend.zig`; capture,
`ModelProgram`, `Executable`, precision and physical buffers remain shared.
`execution/capture.zig` owns common host ABI validation and single-output
publication, including failure cleanup. Architecture adapters retain vocabulary,
mask, rotary, configuration and checkpoint interpretation.

GLiNER boundary task orchestration lives in
`pipelines/gliner_boundary_program.zig`. Its DeBERTa/Ettin CUDA checkpoint-role
and qualified geometry selectors live in `backends/structured_encoder_cuda.zig`.
The reusable CUDA operation bindings are `resident_structured.zig`; numerical
programs, deterministic profiles and F32/BF16 gates remain unchanged.

LightOnOCR and Qwen hybrid execution use operation-named retained native owners:
`bounded_decode_graph.zig`, `recurrent_prefill.zig`,
`paged_prefill_attention.zig` and `recurrent_device_policy.zig`. These own their
existing graph/SDK/storage lifetimes and admitted schedules. Historical device
exports, optional-library ABI names and telemetry remain compatibility names.
The migration does not apply Gemma4's shared-KV policy to other architectures.
Whisper keeps encoder-decoder/cross-attention state; Qwen keeps its own dense or
recurrent state, rotary geometry and cache contracts.

Task CLIs for reading, transcription, extraction and decisions accept the same
explicit PJRT CPU/CUDA precision and memory policy. A task requesting XLA loads
a PJRT session and cannot silently use the native preference that compiled
partitioned generation uses for its seed. Automatic native placement retains
its existing order, and CUDA policy still requires explicit feasible device and
capacity bounds.

Direct task placement is independent for each stage: an explicitly placed
Whisper decoder receives its own manager policy, including split encoder/decoder
artifacts. Provider policy is fixed before creating a serving node. Task controls
are forwarded to execution or rejected when the selected task/provider cannot
honor them; parser acceptance alone is not support. Transcription uses the shared
request-owned PJRT encoder/decoder cache when the learned-position contract and
arithmetic policy qualify. Explicit cache enablement fails on unsupported paths.

`pipelines/pjrt_generation_request.zig` owns one stable execution request shared
by HTTP generation and direct text/media reading. Its compute provider, graph
cache and generation pipeline retire in that order's reverse, under the provider
guard. Callers retain the model handle, generation slot and prepaid media lease
through destruction. The loaded session owns compiled specializations and model
parameters; the task supplies messages and stopping/publication policy. Direct
reading cannot substitute a native compute backend for a PJRT session.

GLiNER-DECIDE's F32 schema classifier uses the same marker encoder/readout stage
as typed decisions. Its exact dense 1B artifact and actual CPU or GB10 PJRT
provider metadata and the default GEMM policy select separate bounded profiles:
one request, at most 64
prepared sequences, each at most 1024 tokens. The current classifier executes
one physical row at a time and rejects an explicit larger extraction batch.
BF16, other devices, quantized
artifacts and span-head extraction receive no qualification from this route.
The compiled executor does not acquire the eager model mutex or construct a
native backend; each session call owns its provider admission and execution lane.
Independent pinned-logit and direct/HTTP tests qualify the actual serving path. Captured symmetric-window bias uses shared integer position
algebra in `graph/attention_bias.zig`; it does not embed a dense per-head mask
for every layer. Only linear position/bound vectors and scalar values are
constants. Independent 512- and 2048-token upstream captures check this definition
without widening the serving envelope or PJRT's program-size guard.

CUDA marker classification has a separate artifact-bound full-context policy:
the exact dense Ettin 1B identity may use its 7999-token configuration. An
8192-token physical budget retains up to eight short rows, or one full-context
row. Unknown and quantized artifacts keep the conservative limit. This
does not widen span extraction's independently qualified 1024-token processor
window. Managed qualification fixtures use the real registry publication gate
and hardlink shared immutable weights, rather than inventing capability metadata
or modifying the default model directory.

CUDA singleton Q4/Q8 schedules use the same immutable recipe for ordinary and
assigned-output projection. Exact row/width/column checks bind the function, ABI,
threads and grid before submission. Upstream SM89 opt-in qualification is retained;
reusing a packaged kernel in a GB10 oracle does not widen that serving gate.
Video color conversion stays in the shared embedded-module owner; RGB rescaling,
processor options and patch interpretation stay in the model adapter. The shared
image-layout proof covers both cached and uncached resident resize paths.

## Kernel ownership and validation

`ops/kernel_descriptor.zig` describes operation semantics and requirements;
`ops/kernel_geometry.zig` shares geometry with legality checks. Backend plans
retain SIMD/BLAS, packed quantization, native GPU schedules and profitable region
fusions. Compile-time recipes share GGUF encoding mathematics without imposing
one accumulation or scheduling policy on every backend.

See [kernel authoring](kernel-authoring.md) for source ownership, typed schedules,
regeneration, source identities and hardware qualification. Portable Tile model
execution remains a prototype; its scope and commands are in
`zig/pkg/inference/src/tile/README.md`.

Preparation and lifetime regressions live with their implementation owners,
including `execution/executable.zig`, `execution/model_candidate.zig`,
`execution/transfer_audit.zig` and `backends/imported_onnx_session.zig`. Run focused
scopes from the inference package with the pinned Zig compiler, for example:

```sh
zig build test-embeddinggemma2 -Dmetal=false -Dcuda=false \
  -Dtest-filter='ModelProgram' -Dtest-filter='prepared physical'
zig build check-kernel-ownership test-tile-kernels
```

Enable the backend under test for physical device coverage. Qualify numerical
changes with independent references, retained native resource/schedule checks,
and controlled baseline/candidate timing before retiring a fast path.

## Indexed routes and resident encoder/decoder requests

Prepared route variants retain a node-to-alias table and compressed consumer
index. Refresh classifies each source once per pass, including source-wrapper
consumers, before removing unsafe aliases. Exact frame dispatch reuses the same
index; warm alias lookup does not scan the graph or alias table. The existing
monotone selection fixed point and provider validation remain authoritative.

Whisper compiled requests use the resident PJRT bank publication owner. Prefill
produces self K/V and immutable encoder cross K/V; subsequent single-token
programs consume those banks and runtime learned positions. Decode reachability
stops at cross K/V before binding encoder projection inputs, so those weights
and encoder activations are not copied, uploaded or projected again per token. Masks
remain dynamic, including fully masked rows. Two non-donated generations and
buffer headers are admitted before execution, and cancellation prevents bank
publication. The task owns transcript, timestamp, language and retry semantics.
Explicit decoder-cache controls are propagated through direct, serving and
benchmark transcription paths. Unsupported arithmetic retains ordinary execution;
explicit enablement rejects unsupported or denied state. Encoder source storage
is borrowed immutably for the request, and source or mask changes reject reuse.

Resident executable inputs and outputs have an explicit layout contract. Padded
self-attention keys are excluded by predicates, independently of score magnitude.
Single-query CUDA LayerNorm accumulates mean and variance in F64 before F32
affine operations and publication; matrix and activation precision remain
unchanged. This preserves the independent native F32 accuracy gate. Non-finite
logits reject the generation before any prior cache buffers or cursor change.

BF16 decoder branches retain their BF16 publication boundaries while CPU and
CUDA GPT residual streams accumulate in F32. Automatic reduced-precision
caching requires the qualified CPU residual policy; CUDA BF16 keeps ordinary
execution automatically. Explicit CUDA BF16 resident programs resolve the default
GEMM policy to deterministic vendor GEMM without changing the shared client's
policy or ordinary programs. Explicit GEMM overrides remain authoritative.
Compact rotary coefficients are shared across heads, preserve F32 angle order,
and use widened trigonometry before the original BF16 publication. Ordinary and
resident causal attention share exact visibility predicates instead of finite
mask sentinels. Together these repairs pass the sampled Qwen context matrix,
including the frozen error limits from the two previously failing contexts.
Accuracy qualification retains its independent F32 reference, relative-error
limit and generated-token checks. This does not establish performance across
models or qualify automatic CUDA BF16 selection.

For the default CUDA BF16 resident schedule, single-row linear projections use
F32 products and a reduction over row-major weights. Existing BF16 publication
boundaries remain intact; inputs are not rounded to obtain this schedule.
Multi-row projections and attention keep the qualified vendor GEMM path, and
explicit GEMM overrides retain their original behavior. This lowering uses the
shared HLO compiler rather than a model-specific native kernel. On shared GB10,
the qualified Qwen3-0.6B short-prompt comparison improves from 138.0 ms uncached
to 111.0 ms cached; the 1,173-token prompt improves from 1,059.0 to 169.9 ms.
A same-binary ABBA control improves the accurate cached short-prompt baseline
from 176.4 to 110.6 ms. All three pass exact-token, quality, median, p95 and
AA/BB gates. CPU/CUDA projection oracles, the continuation matrix through 2,047
initial tokens and 16 continuation steps, and three frozen failing-context
limits also pass. These are Qwen request-latency results, not broader serving
or model qualification; automatic CUDA BF16 selection remains gated.

KV capacity remains bucketed in 64-row increments. Prefill query geometry is
separate: prompts below 64 rows use power-of-two query buckets, bounding padded
work below twice the logical prompt without creating an unbounded shape cache.
F32 causal text prefill has a 512-row query policy independently of KV capacity;
explicit deterministic CUDA BF16 caching uses the same bound. F32 requests
prefer the whole-prefix matrix schedule when a shape-only speculative
lease admits its complete peak beside the charged banks. The probe creates no
provider objects, gives no future quota credit, and releases its lease before
execution. Quota or pressure retains bounded queries without publishing a
speculative failure; cancellation and unexpected errors propagate. The request
can supply a different explicit query bound through the
existing prefill-chunk setting. The physical query shape, including padding,
never exceeds that bound: non-aligned limits select an existing canonical
bucket beneath the ceiling (for example 64 rows under a limit of 65), retaining
tuned shapes and the same bounded specialization cache. Every chunk publishes its valid rows before the
next chunk. Intermediate queries produce a checked hidden row without loading
or executing the unused vocabulary head; only the final chunk's last-row logits
reach sampling. The capture identity and output admission distinguish these
variants while sharing their decoder body. Reports include the actual completed
prefill-step and skipped-head counts. Recurrent layouts retain their
separately qualified shape. Bidirectional media prefixes retain whole-prefix
execution. Their frontend publishes one checked layout for host inputs, padded
embeddings, positions, PLE, visibility and the fixed six-input packet; admission
and materialization consume that same layout. Before selecting caching, the
whole decoder prefill peak must fit beside retained KV state. Automatic quota
or pressure denial releases that reservation and reuses the prepared prefix
through ordinary execution; it never retries submitted decoder work. Explicit
cache requests retain their original admission error. A requested query ceiling
is accepted when it covers the complete padded prefix and otherwise rejected
rather than changing visibility.
CPU BF16 retains its qualified whole-prefix cached route.

The opt-in full-window test exercises Qwen3-0.6B's actual 40,960-token capacity
on CUDA F32: 40,959-token prefill, the final decode step, over-window rejection
without cursor publication, and logits/token agreement between 512- and 256-row
query schedules. This is execution and numerical coverage, not full-window task
accuracy or a claim about bidirectional media.

Automatic state admission declines
quota and transient-pressure denials before execution; explicit cache requests,
cancellation and provider failures preserve their errors. Neither path replays
submitted numerical work. An explicit query bound that requires resident state
also preserves its admission error rather than silently using a larger query.

Opaque serial native calls use the same prepared Source owner as dispatched
calls. The original scratch floor, final Tensor container, owned shape arrays
and allocator headers are declared before numerical entry. KV outputs and
foreign resource owners retain independent custody through final publication.

Whisper position metadata uses checked per-row extents for both encoder and
full-prefix decoder batches, independently of batch size. Prepared absolute
embeddings enforce their configured context before weight access and transfer
input ownership before fallible preparation.

The native CUDA automatic shared-KV policy is restricted to the qualified
SM121 device, exact dense geometry and complete unique Q4 donor inventory with
checked tensor shapes. Other devices and artifacts retain compatibility policy;
explicit experiments retain their existing overrides. Default donor mirrors are
funded during managed model load from their checked BF16 matrix extents, on top
of the original packed-weight floor. Config-backed GGUFs use the same inventory
qualification as generator-only artifacts; raw constructors retain their
caller-owned resource contract. If optional mirrors
do not fit, admission retries the mandatory artifact before construction and
passes that compatibility policy directly to the constructor. Explicit operator
requests retain their original admission errors. Projector dispatch
consults the loaded execution owner, rather than treating a process-wide policy
as proof that a particular model installed the plan. Automatic decode replay
requires that installed automatic policy, a bounded request KV reservation and
the existing greedy/capture safety gates. Shadow checkpoint queries reject
actual typed replay before reading or mutating source KV.

Reader epoch draining has distinct compatibility and retirement owners. A queued
incompatible head owns its compatibility drain; cancellation or timeout removes
that hold and wakes the next FIFO waiter. An adopted final-page resource instead
keeps the epoch in retirement until its last outstanding cleanup completes.
Cancelling a waiter cannot reopen that retirement or weaken epoch join bounds.

The server broker installs `execution/tensor_dispatch.zig` callbacks; synchronous
Source preparation, batch signature qualification, admission, lane entry and
output custody belong to execution. Tasks no longer import a server dispatch
implementation for their production stage interfaces. Direct encoder/decoder
tasks qualify resident incremental receivers through `decoder_stage`; custom
dispatchers retain their scheduling contract. Encoder placement decisions publish
exact capability/ABI fallback reasons through the uniform diagnostic report.
Full-prefix host and resident decoders share one input signature bound per
request. Token and encoder-mask slices remain borrowed. An optional all-ones
query mask grows in the request owner and is reused at later steps.

`execution/qwen_embedding_workspace.zig` owns bounded Qwen embedding workspace
geometry, declared admission, receiving banks, and retirement. The task supplies
token rows, pooling intent, and its numerical lane. Source cohort admission and
backend gates retain their order. A stale cold description can retry before
numerical execution, and a submitted operation keeps its original failure.
The EmbeddingGemma2 execution planner shares cohort limits across direct text
and prepared media. The text packet sizing and physical encoder row geometry
share one admission calculation for compact and padded schedules.

`execution/audio_schedule.zig` owns the existing audio instruction tape and its
arena layout. Alias lifetime propagation uses ML's same allocation-free liveness
implementation, preserving the tape's splitting/coalescing reuse policy and
escaping views. Instruction-capacity and checked storage-size bounds apply before
arena allocation. This does not establish complete physical ownership of every
operation executed by that tape. Prepared clipped projections declare an explicit
input-clamp workspace as well as their output. Serial projections share one
scratch tail, with its view metadata funded before replay. The tuned fused
projection remains first choice; an unfused fallback consumes the supplied span
without allocating a clamp tensor. Optional scratch pressure preserves the old
output bank. A staged, backed-off upgrade can later fund a separate scratch bank
without invalidating queued output references or resetting replay selection.
Both placements use the same checked view geometry and byte bounds. Partial
upgrades retire only their own quota and views before retry.
The merged seq2seq route accepts mixed inputs. Tokens, masks, and branch
controls use host tensors. A qualified encoder in the same provider context
passes its activation directly into the merged decoder. Self and cross K/V
feed backend tensors directly into the next invocation on imported ONNX CPU,
Metal, and CUDA. Exact cache shapes use the provider comparison before
allocating metadata for a compatibility case. The decoder
reads only the final vocabulary row. The serving broker gathers compatible
resident rows into admitted backing and keeps each request's output rows
separate. Fixed batch exports run through the serial mixed-input route. CPU
serving uses that route by default and batches when the collection window is
explicitly enabled.

Standalone self K/V can keep its allocation under an independent K/V charge.
Admitted pooled outputs retain their physical lease. An omitted cross output
can move its prior cache to independently admitted backing. Admission pressure
preserves the producer pin where a separate reservation would exceed limits.
The model loader selects the ordinary full-prefix decoder when an optional
merged export lacks a supported operator, mixed-input ABI, or compatible
geometry. Receipt and checksum failures remain errors.

Metal reports the device's instantaneous worker-process allocation gauge alongside
tracked project allocations. It includes vendor resources visible to the driver;
it is neither a per-request peak nor a second ownership ledger. No inferred
"untracked bytes" subtraction combines these separately sampled scopes.

## Remaining migration boundaries

Issue [#1166](https://github.com/antflydb/antfly/issues/1166) also requests a
complete migration of execution policy. Shared kernel ownership and the common
preparation API do not by themselves complete these remaining requirements:

- The remaining eager token-prefill route binds each admitted `ChunkCursor`
  attempt through `execution/decoder_prefill.zig`. The runtime supplies
  scheduler eligibility, direct execution exclusion, resize permission,
  output intent and the numerical inputs together. Generation coordinates
  scheduler turns and publishes progress after successful runtime completion.
  The CUDA replay-capacity and native prefill chunk-size decisions named in
  #1771 already live in `decoder_storage.zig` and `decoder_prefill.zig`,
  respectively.

- Imported ONNX and architecture graphs share the bounded specialization owner
  in `execution/specialization_cache.zig`, with stable executable handles,
  allocation-before-eviction admission and explicit retirement. Their limits
  remain workload-specific. Traced resources are scoped to the exact backend
  context; switching contexts retires device bindings while preserving semantic
  graphs and analysis. ONNX initializers retain their shared immutable context.
- ML interval planning, interpreter/native/Metal handle retirement and the
  physical buffer plan share one dependency liveness analyzer in
  `lib/ml/graph/passes/memory.zig`. Reachability, actual submission ordinals,
  selected outputs and their lifetime bounds are explicit policy. Physical
  backing-view propagation and conservative donation groups retain their
  storage-specific rules. Allocation-failure tests cover interval growth and
  transfer of interval ownership into the reusable-slot plan. Backward runtime
  input groups share one checked remapping helper: removed nodes contribute no
  binding, surviving inputs retain their backend owners, and invalid mappings
  fail before allocation. Production and diagnostic Laya training share it.
- Assigned outputs and workspace cover admitted Native/CUDA descriptors and
  Metal's physical frame routes. Exact backend services attest each complete
  program's input, literal, alias, kernel and workspace ownership before replacing
  compatibility admission estimates. Unsupported regions and private temporary
  owners retain their existing envelopes. CUDA prepared projections declare
  supported reduced/packed conversions and vendor workspace; these declarations
  preserve their tuned launches rather than replacing them with generic math.
  Automatic F32 scalar residual fusion preserves the original product and sum
  roundings. Native, Metal and current CUDA artifacts write the assigned
  destination in one pass. Older CUDA artifacts use two launches into that
  buffer. Authored CUDA residuals retain their contracting kernel.
  Ordinary F32 matrix multiplication stages a dynamic RHS transpose in assigned
  workspace and reuses the projection family's original SIMT/vendor policy.
  The current RHS is validated on every invocation rather than treated as an
  immutable cached weight; transpose and vendor workspace remain disjoint.
  Eligible CUDA frames capture stable output, workspace, mutable-input and
  immutable-literal banks. Binding roles, exact context identity, immutable
  handle ownership and copy/launch costs gate admission. Explicit literal
  overrides use ordinary execution without mutating captured storage. Recording
  and instantiation failures fall back before compute submission; submission
  errors propagate. Captures retain address leases; published outputs and foreign
  captures prevent mutable frame reuse. Cancellation and unsupported signatures
  retain ordinary execution with explicit replay decisions in diagnostics.
  Bounded CUDA decoder and audio execution seal their stream workspace recipes
  in `execution/stream_workspace.zig`. Admission and dispatch consume the same
  size, alignment, event sequence and published span. The providers own learning,
  shape-specific backing and retirement. Legacy arenas with incomplete recipes
  retain their compatibility accounting. Physical replay, bounded decoder replay
  and EmbeddingGemma2 capture share a checked recording end before reclaiming or
  reusing addresses.
  Qualified CUDA projection and head-normalization regions attest assigned
  outputs and temporary banks through the physical coverage check. Synchronous
  Metal regions skip legacy partition graph and output banks after selection.
- `model_definition.zig` owns artifact loading. Native and PJRT sessions share
  one failure-atomic host preparation path for normalization, resident/lazy
  weights and prefetch ownership. CUDA reuses its admitted manifest snapshot
  when opening that source, retaining early device/artifact preflight. Prefetch
  guards are installed only after the final session owner is in place.
  `models/load_plan.zig` derives checked artifact/tokenizer preparation peaks,
  retained weights and run-admission geometry. PJRT host load estimates credit
  only aligned F32 tensors proved to alias an actual safetensors mapping, and
  only when captured architecture/adapter policy preserves those views. BF16,
  unaligned tensors and rewriting preparations keep their independent host
  representation charge. CPU provider operands and the bounded CUDA arena
  remain charged; aliases never enlarge an operator's memory envelope.
  `models/session_loader.zig` owns
  primary/component backend attempts, artifact selection, construction guards,
  preparation order and failure rollback. Serving supplies health, admission
  limits/leases, eviction and close protection through synchronous borrowed
  services; its callback context never becomes the loaded model owner. The
  session's resident-preparation provider retains the actual model context
  through wrappers while receiving the published admission/interruption policy.
  Construction peaks remain leased until preparation succeeds; component load
  ordering and immediate cancellation checkpoints retain their existing rules.
  `compute_context.zig`, prepared executables and model candidates separate
  further responsibilities. Optional specialized hooks use typed
  operation regions (decoder, replay, relative/window attention, encoder,
  projection, vision/audio, speculative verification and token selection).
  Historical public method names are compatibility adapters; new implementations
  extend operation descriptors/regions, preserving each admitted native schedule.
  `graph/execution.Context` declares backend, model-weight adapter, execution
  control and compiled-backend dependencies explicitly. Serving and offline
  compilation use that same typed contract instead of pipeline-shaped structs.
  Backend pointers are borrowed from their real owner; temporary context values
  never become the owner of cached executors or physical bindings. Immutable
  model configuration is borrowed, avoiding a full metadata copy per step;
  cache keys canonicalize semantic fields, ignoring struct padding and prefix
  allocation addresses while hashing prefix bytes. Native and Metal whole-model
  executors share `execution/session_decoder.zig` for immutable definition ownership.
  Definitions own configuration slices and survive executor retirement. Runtimes
  share metadata within the same allocator lifetime or clone it into their own
  allocator before request allocator retirement; failed migration preserves the
  original definition. No metadata allocation or reference counting occurs per
  decoder step. Tied heads, optional
  scales and omitted-projection aliases live in the architecture weight adapter.
  Self-contained Metal session runtimes have an explicit loaded-session/config,
  KV dtype and shared-expert-cache owner key. Transient request backend wrappers
  retire context-bound bindings while preserving that independent owner.
  Replacement is failure-atomic; borrowed CPU/PJRT runtimes retain their stricter
  backend lifetime. Repeated-request qualification forces distinct wrapper
  addresses and checks logical decoder reset and exact output parity.
  Embedding indices explicitly select host IDs or a resident tensor. Traced
  host-ID placeholders need no materialized tensor; imported integer tensors
  retain their resident fast path and exact integer fallback.
- Resident stage contracts, exact typed transfers and strict readback auditing
  are implemented. Text encoders use the session's declared resident capability
  by default, with an explicit host override. `execution/sequence_pool.zig`
  owns mean, CLS, max and last-token pooling, including flattened token rows,
  masked NaNs, empty mean rows and exact integer gather fallback. It batches
  pooling/normalization and materializes only requested final vectors. The
  existing fused masked-mean/L2 program accepts checked generic dimensions;
  historical Nomic request/method/native symbols remain compatibility bindings.
  Single-row means reuse that program to normalize other pooling results.
  No new Metal shader is needed. Prepared normalized heads skip empty batches.
  Producers that already return final normalized host rows declare that output
  contract explicitly; requested host vectors consume the original host ABI
  without uploading them solely to download them through a resident wrapper.
  Owning consumers write prefixes and normalization directly into their cold
  output bank. Compatibility row copies declare their exact packet, input and
  allocator-header overlap before execution; admission checks the complete
  request against the actual retained allocator owner after trimming.
  Before reserving a partial task floor, metadata-only preflight rejects a
  known impossible sum of that floor and the mandatory fresh output bank.
  This grants no quota; otherwise feasible requests retain ordinary contention
  and each Source still admits its complete execution geometry. Token usage
  follows successful output consumption and survives scoped wrapper teardown.
  `execution/submission.zig` owns command scopes, preserving foreign batches,
  cancelling only owned unsubmitted work and avoiding replay after submission
  errors. Nine generic command callbacks live in `CommandSubmissionRegion`;
  historical decoder methods route to the same backend implementations.
  Declared text input planning is shared by embedding and reranking. It retains
  input order and integer precision, supplies positions/additive attention bias,
  and sizes input/temporary storage before allocating bias buffers. Host token
  outputs retain pooling semantics when imported graphs flatten sequence rows.
  CUDA max reduction uses negative infinity as its identity, including fully
  masked rows. Retained native targets keep their existing launch resources.
  Migrating every remaining task to resident chaining and moving all
  KV/recurrent execution choices behind runtime owners remains incremental.
  Reranking, GLiNER, NER, classification, sparse embeddings and the explicit
  host encoder API share the tensor-stage dispatch entry. Prepaid serial stages
  retain explicitly supplied guards; custom dispatchers retain gate ownership.
  Cancellation before acquiring a guard never releases another owner's lock,
  and a failed numerical call releases its guard without replay. Wider task
  guards and stateful cache scopes keep their existing lifetimes.
  `execution/decode_batch.zig` now owns uniform, mixed prefill/decode and packed
  batch metadata assembly, including per-sequence physical KV views, recurrent
  caches and position ownership. Generation supplies typed state and geometry.
  Batch layout and its callers use the runtime owner directly.
  `execution/decoder_work.zig` owns borrowed queued prefill/decode records, resident
  prefix geometry and validation. Bounded device bursts read an immutable token
  budget rather than dereferencing task continuation state. Task publication and
  stopping/streaming policy remain in generation. The physical scheduled forward
  is owned by `execution/decoder_step_executor.zig`: it receives explicit context,
  queued work, padding, continuation control and lane access, without a task
  pipeline or tokenizer. It owns frame submission/readback cleanup, bounded
  device bursts, KV reservation rollback and CUDA epoch retirement. The reader
  epoch calls it directly; a separate task adapter publishes completed results
  while retaining the scheduler turn and lane. A shared execution-access record
  preserves FIFO/phase priority and cancellation-independent cleanup.
  `execution/decoder_forward.zig` owns logits/hidden-state and compiled token
  dispatch behind a borrowed runtime context, without task or tokenizer
  dependencies. It derives eager query geometry from live decode state and
  selects compiled token or logits execution. Ordinary speculative draft and
  verification queries also use that owner for physical windows, target
  reservation and failed-forward rollback. Host logit windows retain their
  producer allocator; proposal acceptance and prefix selection remain task
  policy. Resident token slots own handoff,
  readback and cancellation-independent retirement across ordinary and pending
  decode. A bounded acceptance cursor owns reserved-prefix accounting and KV
  advancement for published burst tokens. Ordinary and scheduled prefill share
  runtime post-chunk donor maintenance before publishing their cursor.
  Generation supplies semantic eligibility, schedules turns and publishes
  results. Positioned-prefix scheduling borrows one typed runtime request
  rather than the generation pipeline. It retains one executor through the
  queued chunks, shares the wait/claim/cancellation-drain protocol with ordinary
  decode, and merges task counters once after completion. Server batching keeps
  grouping, deadlines and publication while passing typed numerical work.
  Native whole-model execution shares the last-logits implementation;
  retained hidden outputs point to the real backend, never the temporary adapter.
  `execution/decoder_storage.zig` owns pool geometry, metadata-only CUDA request
  pools, checkpoint geometry, route capacity and lazy reservation policy. Native,
  Metal and generation reuse it while explicitly preserving architectural versus
  compact stored-layer layouts and host-mirror choices. Remaining specialized
  task transitions and compatibility arenas retain their explicit owners.
  Decoder storage installs the request-local replay bound before CUDA KV writes.
  Prepared-media and text prefill share chunk policy, and decoder prefill
  selects the prepared-media KV window before numerical submission.
  Incremental encoder-decoder callers supply new tokens and their absolute
  starting position. Native, compiled and merged decoders validate the position
  before submission and advance it only on success. Merged seq2seq ONNX
  decoders resolve input/cache bindings at construction, then validate outputs
  before committing them. An empty cross KV output retains the previous cross
  KV. Token history stays with the caller.
  Canonical KV owners prepare unpublished attachment, full prefill, generated-token page growth,
  shared-tail copies, window trimming, replay reservations and view metadata
  before committing either owner. The unchanged-ID path preserves the existing
  table without allocation or a history copy. At reservation/page boundaries,
  paired plans prove an unchanged combined ID prefix before copying only new
  IDs; growth, COW, window trimming and unsynchronized views use full copies.
  Grown host tables retain their allocating owner through state teardown;
  generation admission declares their
  checked host footprint and both simultaneous plans' ID-array scratch
  separately from device scratch. Serial batches take
  the peak after request-state death; coordinated participants add their live
  metadata. Metadata synchronization failures propagate before publishing a new
  view. Canonical truncation stages both owners before the final backend
  notification and paired metadata commit. Session admission combines distinct
  prepaid workspaces before applying explicit operator limits once. Independently
  declared opaque scratch remains charged even when a prepaid arena covers the
  invocation workspace. Opaque providers and incompatible state transitions
  retain compatibility ownership. Host-table
  retention alone does not certify every device cancellation/drain endpoint.
  Ordinary F32 attention reuses the existing physical producer: CUDA singleton
  batch rows write directly into their output parent, and Metal cold paged
  layers reuse their reserved page-offset storage and supplied destinations.
  Both validate the current canonical storage before using these routes and
  preserve their original launches and retirement owners. These ordinary
  optimizations grant no complete-Frame or capture credit; learned private
  decoder/audio temporary arenas retain compatibility ownership.
  Eligible Native F32 incremental audio decoders instead prepare one authored
  single-token schedule through the common physical pool. The architecture
  supplies operation order and state bindings; its existing numerical forward
  consumes the prepared views. Cross K/V projections write directly into the
  original KV slabs, and self attention receives actual live prefixes without
  allocating slice headers. Declared contiguous initial prompts use a separate
  causal/multiquery schedule with the original tiled arithmetic and explicit
  scratch. Both phase banks are funded together, with shared KV funded once;
  undeclared or mismatched initial queries retain eager execution.
  Managed decoders share immutable graph, descriptor, liveness and layout facts
  through a bounded Model cache; weights, KV, Frames and execution epochs stay
  private. Shared metadata retains its admission Domain, with final quota refunds
  deferred through the actual retirement boundary outside execution. Retained
  shared metadata and pending mandatory KV remain in the full request fit.
  Cold metadata has independently admitted storage, physical banks have exact
  additional declarations, and the old request envelope remains intact. Source
  stamps revalidate under the Model gate; construction capabilities are consumed
  before the cache owns its stable execution epoch. Teardown releases callbacks
  after leaving execution. Unknown allocator contracts, reduced formats and
  unsupported schedules retain compatibility; this migration does not certify
  every decoder or imply performance acceptance.
- Native decoder templates retain only backend-free placement, buffer layout and
  output-policy facts. Graph runtime and decoder preparation use the same layout
  builder; each decoder program creates fresh physical pools and revalidates the
  current provider policy. It does not construct unused graph executors or a
  device mesh. Escaping outputs retain their storage lease independently of the
  program, graph and template lifetime.
- `execution/speculative_activation.zig` owns predictor rounds, including draft
  chains, logits and accepted activation handoff. After a gated or BitNet
  prefix submits, a prepared vocabulary-head miss completes from its resident
  final row. Gated frame submission failures propagate before output publication.
  The physical decoder owns verification suffix geometry and pending
  tensor/scalar selection; task code
  retains acceptance, grammar, stopping and streaming. Unused private adapters
  are removed while public compatibility entries remain.
- `execution/decoder_prefill.zig` assembles plain embedding rows once and keeps
  hidden rows resident through projection. ColQwen document and query paths
  export final scoring vectors. Complete source-plane and destination-namespace
  checks precede copies; packed or opaque sources retain the host path.
  Qwen multimodal prompts replace image rows one producer at a time without
  exporting supported resident embeddings. Unsupported image storage converts
  only that source, without replaying earlier producers or assembled rows.
  Gemma prompt entrypoints share the same ordered replacement owner; nonunit
  embedding scales preserve their original CPU arithmetic. The extra resident
  prompt destination receives no admission credit or reduced memory floor.
  Native and GGUF vision weights use one patch-encoder/projector definition
  through typed format adapters. Debug captures are optional; ordinary output
  avoids the unused normalization readback and final host-vector duplicate.
  Host position addition, ordered pooling and native kernel schedules remain.
  Resident projection inputs and final dense-row materialization use the shared
  sequence-pooling owner. Tasks supply pooling, prefix width, normalization and
  finite-output requirements; the execution owner preserves full-width resident
  normalization before export and the original prefix-copy order.
  Projected image inputs own a typed host/resident row source; the model producer
  resolves placement while preserving early resident projection, later host
  preparation and video priority. Prompt tasks consume borrowed row views through
  the same numerical composer. Incremental-input cleanup qualifies actual session
  pairs and supervision in its execution owner. Florence's full-prefix and
  incremental decoder paths share architecture bindings and ordered vocabulary
  head selection; tasks retain suppression construction, EOS and publication.
  Eligible scalar Native F32 readers reuse the same immutable Definition cache
  and private physical schedule. The original full task permit moves into the
  Source owner without another KV reservation. Construction cleanup drains
  before scheduling selection; physical retirement precedes numerical unlock
  and metadata refunds. Reading telemetry copies actual decoder Frame counts
  before teardown, excluding encoder and vocabulary-head execution.
- Metal installs synchronous assigned pair/QKV regions for complete F32
  projection heads through the production partition executor. Row-one regions
  use the existing source ownership. One semantic descriptor recognizes independent
  projections, the optimizer's fused pair/second-output marker and grouped
  concat/projection/slice forms. Source preparation and assigned regions share
  those exact node identities. Region-only heads retain physical output plans
  without pretending to own individual primitive kernels. Public host-output,
  host-to-resident and resident-input entrypoints consult the same provider-owned
  preparation hint for eligible managed fixed heads. They share the existing
  Source owner, release the FIFO selection before cold funding and reselect it
  before execution; the hint grants no admission credit. Managed multirow
  no-bias heads can prepare one
  immutable packed weight/bias pair after the original compatibility call has
  established source slots. The actual Source cold phase funds both host
  metadata and backend roots outside execution gates, then creates a fresh
  request-bound profile. Publication rechecks the exact source request under
  execution and index exclusion after funding; concurrent losers refund their
  unpublished allocation. Pre-entry source checks acquire the actual index or
  validate the entered metadata owner. Its scope fields share the singleton HOST
  charge. Dispatch retains the original direct GEMM/split policy
  and waits its exact private command buffer before publishing outputs.
  Each reuse retains its actual operation ticket, independently owned metadata
  and cleanup boundary; SDK payload death and model-close cleanup both gate
  refunds. Models sharing a backend and admission Domain share the same cache
  binding, counted once by held-model conservation snapshots. Mixed graphs,
  MPS/MPP selectors, raw receivers and direct decoder APIs retain compatibility
  ownership. Canonical single-partition pair and QKV heads use complete
  payload accounting when the synchronous region owns every output and either
  imports the activation into its assigned bank or verifies the exact resident
  input. The packed multirow variant attests the live pack and uses its planned
  split workspace. The retained weight pack keeps independent cache admission.
  Attested primitive frames skip compatibility output banks and promotion caches.
  Mixed scopes and entries without an attested region keep conservative admission.
- CUDA prepared projections describe eligible BF16-rounded output separately
  from F32 publication, activation conversion and vendor workspace. Cold
  preparation loads the original widening program; dispatch retains the
  original GEMM and widening order. Capture uses assigned activation staging,
  rounded output and vendor workspace. A rounded projection with a separate
  activation mirror uses ordinary execution.
  A witnessed RMSNorm BF16 auxiliary can supply activation rows without another
  conversion. Allocator alignment is explicit metadata; SDK binding rechecks
  the actual primary and auxiliary leases against the current frame. External
  mirrors, offsets and unknown alignment retain compatibility ownership.
- `execution/testing/model_formats.zig` compares deterministic one-layer BERT
  and sliding-window decoder graphs loaded through real ONNX, safetensors and
  GGUF parsers. Deliberately lossy Q8_0 weights use an independently rounded CPU
  reference, with a `2e-4` execution tolerance. Decoder K/V stays resident across
  four transitions; held earlier outputs detect frame overwrites. Immutable
  matrix packing is warmed before strict readback checks. This matrix validates
  format equivalence; it does not benchmark packed quantized serving kernels.
- Execution diagnostics publish one owned report for actual invocation or
  pending-phase observations: transfers and bytes, accepted dispatch/submission
  and wait units, specialization decisions, selected Native bodies and workers,
  known fallback producers, and registered host/device allocation owners. Batch
  participants and pending outcomes survive model retirement. Each pending
  owner sums instrumented transfer and submission units, backend callback
  returns by backend, and callback host wall time
  through readback and cleanup. Metal serial audio reports cold selection
  decisions even before a schedule has bank gauges. A ready schedule reports
  its eager/replay choice and latest planned, bound and charged bank sizes.
  The managed stage also records funding pressure, stale funding requests,
  declined publication and published arenas at the provider handoff.
  Reports retain the initiating failure
  and count the observed callbacks. Inter-phase work and GPU completion remain
  outside the sums. Registered owner intervals can include concurrent activity,
  and lifetime peaks are labelled as such.
  Successful frames and entered Native calls copy their prepared payload admission
  coverage into the report, including the existing compatibility reason. This
  describes prepared accounting rather than a numerical fallback or quota grant.
  Assigned regions report selected regions, accepted callbacks, and covered
  nodes from the finished Frame across CUDA and Metal. A recorded callback
  is separate from a graph launch. Instrumented Metal legacy region-policy
  counters remain absent until their actual planner runs.
  Unobserved fallback reasons and opaque vendor/private allocation domains remain
  unavailable. These scoped observations do not claim a whole-backend census or
  an exclusive invocation memory peak.

### Compatibility routes still in production

| Signature | Owner and boundary | Replacement and qualification needed |
| --- | --- | --- |
| Metal graph with a deferred producer or specialized region | Product-add, dense F32 single- and dual-scaled add, eligible shared transpose-dot groups, and F32 gather-after-bias can publish through assigned regions inside the prepared frame. The scaled-add route uses the existing fused shader and shares scalar-product matching with the compatibility route. Physical Metal at `1b33d39ea0` verified both operand orders, retained outputs across reuse, and one fused dispatch for the dual-scaled case in both prepared and eligible compatibility execution. The cancellation case verified separate product rounding. At `c4ad51d61a`, baked-only constants had no live physical allocation. A scalar also published as an output retained its backing and returned the same value. A complete frame skips the compatibility region planner. Published intermediates and unqualified representations still use their supported routes. | Map remaining deferred edges to their existing tuned regions and prove producer lifetime, command count, numerical parity and held outputs before removing the corresponding allocating branch. |
| Metal vendor projections and raw decoder entrypoints | Eligible prepared F32 MPS projections bind planned inputs and outputs and retire their matrix views with the command buffer. At `3cbdc9d93b`, the first result remained correct after a second assigned MPS submission with different input, with zero intermediate device-to-host accesses in both calls. Compatibility admission still covers opaque SDK workspace. Eligible raw decoder frames reserve one shared graph scratch bank across layers and bind direct-Q, hidden, projection, and hot FFN slots from it. Physical Metal at `60486c69ee` also verified the assigned packed pair and QKV routes against Native values, including output offsets, resident dispatch, retained outputs and allocation-failure bounds. Other vendor or raw decoder signatures can still retain provider-visible scratch. | Assign the remaining provider-visible inputs, destinations and scratch. Retain admission for opaque SDK storage and qualify cancellation and retirement before removing redundant envelopes. |
| Whisper with an undeclared or unsupported causal prefill shape, or with denied prefill workspace | The accepted single-token plan keeps its own bank and KV charge. Prefill uses the ordinary decoder when its phase is unavailable. A joint phase admission denial retires the cold attempt before retrying with the decode plan alone. Construction diagnostics record the requested phase outcome separately. At `074f568642`, physical Metal tests verified encoder publication and a foreign-provider Whisper handoff without host transport. | At `e7b4110b5a`, the Native fixture cancelled during cross-cache construction, retained prepared weight ownership and retried successfully. Spark GB10 transcribed the same English clip five times with the shared Whisper Small checkpoint, returning identical text with cached K/V. At `3f0c53809f`, Spark also cancelled during cross-cache construction with the released weights, verified that the caller retained the weights, rebuilt the cache, and transcribed the same clip five times in the same model session. At `5a45d64ec0`, eager CUDA admission used the actual retained F16 cross-cache width while keeping the F32 projection and conversion allowance. The physical masked-attention oracle verified both cache widths. Five calls each with the default and explicit reduced policies returned identical Whisper Small transcripts with cached K/V. At `7e8e97c714`, both constructors included declared prefill rows in the compatibility workspace bound. Native and Metal passed the workspace, admission and decoder tests. Spark GB10 passed the geometry test and returned ten identical cached transcripts across the default and reduced policies. Exact packed workspace ownership and complete phase coverage remain open. The physical Metal fixture at `40c1b8c072` matched ordinary logits through both prepared phases. Both the ordinary Metal compute constructor and the same-provider resident handoff already hold a managed provider operation before the decoder resolves projection slots. An attempted cold-funding insertion at that point left the pack cache empty. At `573d80fb3b`, opt-in host-input prefill released its cold provider lease for pack funding, reentered the provider, and dispatched with assigned workspace. Physical Metal Whisper Small selected packed QKV in all 12 decoder layers on three successive constructions. Four-token prefill matched the ordinary Metal route within 0.01 per logit. The failed priming probe used request-local slot declarations. The opt-in model preparation now retains admitted F32 declarations with the provider and primes their packs before the encoder enters. The resident or host-input receiver borrows those declarations while preserving its source operation. Model load fetches only self-attention Q, K and V for this preparation. Requests neither publish persistent slots nor repack weights during decoder construction. Missing packs use the existing per-slot path. The model-owned F32 slots reserve SDK backing bounds before preparation and validate the actual retained weight and bias sizes before publication. The reservation conservatively includes a bias buffer bound per slot, including shared canonical zero bias. The physical Metal Whisper Small fixture executes four-token prefill with all 12 packed regions after the same-model resident encoder handoff. The public single-window Metal benchmark used a request arena and independently admitted decoder storage. On M4 Max, Whisper Small with a fixed English clip and four-token decoder prefix returned identical text, tokens, language, and EOS across 60 warmed samples. Five warmups preceded each 12-sample phase in A–A–B–B–A order. Pooled ABBA task lifecycle median/p95 improved from 432.23/433.49 ms to 418.43/419.55 ms with packed preparation enabled. The separate diagnostic run reported prepared four-token prefill and 2,869,437 retained definition bytes. The lifecycle check subtracts those measured model-owned definition bytes and requires all request-private charges to return to baseline. This measures the combined prepared route against the same revision with the opt-in disabled. A separate frozen comparison used automatic language detection on both revisions, the same clip and shared Whisper Small checkpoint, and the same warmup/sample schedule. Relative to the issue baseline `9e4af7f847`, the packed-preparation opt-in at `9e9d40a2d7` improved pooled ABBA median/p95 from 462.15/463.70 ms to 448.85/449.80 ms. All 60 measured outcomes matched exactly and reached EOS. The diagnostic run reported one initial query row for automatic language detection, so this measures full-route parity without a multirow packed prefill. The fixed-English frozen comparison used the same schedule and added only the benchmark language option to `9e4af7f847`. Inference source at the baseline was unchanged. The four-token candidate improved median/p95 from 431.97/433.01 ms to 419.25/420.41 ms. All 60 measured outputs matched exactly and reached EOS. On the public Metal HTTP reader at `f6b0f889cb`, packed preparation was compared with the same binary with the opt-in disabled. Fixed-English requests ran at offered concurrency 1, 2, and 4, using three warmup waves and twelve measured waves per phase in A–A–B–B–A order. ABBA successful requests per second were 2.189 → 2.255, 2.188 → 2.254, and 2.180 → 2.242, respectively. Both configurations admitted exactly one request per wave and returned retryable HTTP 503 for the remaining overlapping requests. All successful responses matched exactly. The one-admission limit came from the HTTP weighted work cap, which defaulted to the 16 request slots. With the request cap held at 16 and weighted work units explicitly set to 128, all 525 HTTP requests at offered concurrency 1, 2, and 4 completed with identical responses. The run sent no application deadline, so the experimental deadline-only FIFO was bypassed and subsequently removed. Under the existing scheduler, pooled ABBA throughput was 2.218 → 2.279, 2.267 → 2.336, and 2.289 → 2.361 successful requests per second. The corresponding p95 latencies were 455.40 → 445.56 ms, 890.23 → 862.28 ms, and 1,756.09 → 1,701.64 ms. These cohorts establish shared-model HTTP behavior under overlapping requests. A separate M4 Max single-window cohort held the full encoder window fixed and varied audio duration and decoder prompt geometry. A 14.483-second repeated clip used four initial rows, the timestamped short clip used three, and the conditioned short clip used 22. All 120 warmed samples matched text, token IDs, language, timestamps where requested, and EOS within each case. The same-binary A–A–B–B–A comparison with three warmups and eight samples per phase gave ABBA median/p95 of 423.06/443.21 → 415.19/418.86 ms for repeated audio, 445.66/449.34 → 432.31/436.08 ms for timestamps, and 440.76/448.01 → 427.62/434.15 ms for conditioning. Separate diagnostics confirmed all three prepared initial shapes. The repeated clip returned one sentence in both arms. Multiple-window and quality comparisons remain outside this cohort. |
| Metal audio with a cold, disabled, or unfunded serial schedule | The frontend uses eager execution; the backend owns learned metadata and any retained bank. A managed stage funds banks across its quiescent boundary and backs off repeat funding denials. Schedule planning uses the retained cache capacity. Owned F32 output compaction can run in the final frame and publish after retirement. Eager fused and prepared audio projections share dispatch, and an unfused input clamp binds either the planned scratch view or one invocation-owned device tensor. Physical Metal at `074f568642` verified eight projections through planned scratch with no additional device tensor allocations. The eager route submitted once per projection and reused its completed scratch on the second call while preserving both held outputs. A separate test verified recovery after workspace denial. | The earlier `1b17c39854` Metal batch covered ragged compaction and stage arena funding with the official Gemma4 E4B projector. A funded schedule can also lend the same clamp scratch to eager projections in synchronous submissions and ordinary caller-owned frames with tracked resources. The physical Metal fixture queued two distinct inputs in one frame, reused the scratch for both, preserved both results and declined borrowing while the frame was submitted. Cold and unfunded schedules and concurrent planned encoders keep their existing workspace path. |
| CUDA legacy decoder workspace with an incomplete event recipe | The decoder provider keeps learned periodic scratch and replay retirement. Legacy replay inputs and outputs use shared physical leases. Replay validates the captured scratch phase and rejects borrowed slots before submission. Spark GB10 qualification at `4bab4cafc5` verified phase mismatch, transient borrows, preserved warm graphs, and refusal before device submission. Bounded production recipes use stable workspace and explicit publication spans. At `3037d7a4b7`, Glowworm verified that a recording with unpinned ordinary scratch produces its result once without publishing a persistent exec. Whole-iteration replay records the proven trace directly and reconstructs it if a later allocation changes the boundary. | Require ordinary and capture execution to consume the same event, state and workspace plan before deleting learned address policy for a production signature. |
| Opaque or unsupported encoder-to-decoder provider pair | The resident-stage dispatcher retains the declared host route when the provider cannot accept the exact source or controlled invocation. On Spark GB10 at `416c70fe8c`, the imported ONNX path copied independently admitted cross-cache groups with one host wait, preserved the previous cache on denied admission, and passed merged T5-small decoder parity with CUDA capture and replay. The Metal run at `be941f1990` verified one command submission and wait for a two-group copy and passed merged T5-small parity. The admission-enabled fixture at `0e133b822b` verified independent ownership of all self- and cross-cache slots after the first suffix at all tested prefixes on CPU, Metal and Spark, and zero retained charges after shutdown. At `074f568642` on Metal and CPU and `3037d7a4b7` on CUDA, the real merged decoder retained all 24 cache slots independently after the first suffix at prefixes 1, 8, 32 and 64. At `295d708326`, the real Metal merged decoder independently owned all 24 slots before the first suffix at those same prefixes. At 21c7bab6ec, exact-span pooled cross-cache adoption also gave Spark independently owned backing for all 24 slots at the end of prefill at all four prefixes, with ONNX Runtime logit parity and successful capture and replay. Native now detaches cache slots that still borrow a larger producer bank with a bounded, synchronous copy at prefill. The released T5-small fixture requires independent self- and cross-cache backing at prefill on Native, Metal and CUDA, and verifies the same logit reference across all four prefixes. The released graph also proves that encoder-payload bindings are eliminated from cached specializations after shape folding. Keeping a separate encoder upload was redundant for these suffixes and was omitted after it disturbed CUDA replay selection at prefix 64. Prepared stage dispatch now skips host ingress for any payload eliminated by the selected graph. The released fixture clears the encoder bytes after prefill and verifies ONNX Runtime parity and CUDA replay with shape metadata alone. | Qualify source custody, cancellation and final-row readback for each supported producer and consumer pair before removing intermediate host materialization. |

The issue checklist records implementation and evidence separately from remaining
migration. Compatibility paths stay until their replacements satisfy correctness,
residency, lifetime and performance checks. Physical L4/A100 qualification remains
deferred; CPU, local Metal and Glowworm GB10 provide the available hardware tests.

The released T5-small decoder also exercises the serving broker with two
independent four-token prefills and distinct suffixes. Both encoder payloads
are freed before the broker gathers the retained cache inputs into one batch.
Both complete vocabulary rows match the full-prefix references within 0.001
on Native, Metal, and Spark CUDA. Published logits readback waits for the
provider with request cancellation checks if a sibling still owns the scope.
This fixture verifies physical batching and output correctness. It does not
measure serving throughput.

An in-process serving comparison at `e2b8e9bfb5` timed the pair of cached
suffixes after independent four-token prefills, excluding prefill and
including broker collection, device gathering and final row readback. Each
phase used three warmups and 12 measurements. The serial control had 48
measured pairs. Each broker arm had 24, and all broker pairs physically fused.
All runs checked both full vocabulary rows against full-prefix decoding.

| Provider | Serial p50 / p95 (ms) | 200 µs broker | 1,000 µs broker |
| --- | ---: | ---: | ---: |
| Native | 3.36 / 3.52 | 5.84 / 5.96 | 6.87 / 7.21 |
| Metal, M4 Max | 18.61 / 31.45 | 14.43 / 16.83 | 18.72 / 21.97 |
| CUDA, shared Spark GB10 | 2.59 / 2.65 | 3.45 / 3.60 | 4.34 / 4.47 |

Native and CUDA were faster serially at this shape. On Metal, the serial
phase medians moved from 14.8–14.9 ms to 25.7–27.3 ms.
Metal throughput remains uncertain. This was a same-revision
comparison at one shape. HTTP throughput and broader shape qualification
remain open.

On the same shared Spark, grouping the cache-column copies with
`cuMemcpyBatchAsync_v2` reduced the 200 µs broker p50 from 3.45 to 3.20 ms
and the 1,000 µs broker p50 from 4.34 to 4.06 ms. The serial control stayed
at 2.59 ms. The grouped copy brings this CUDA path closer to serial and
still trails it by about 24% at the shorter collection window. Both arms
retained logit parity and fused all 24 measured pairs per window. Providers
without the grouped copy capability use their per-column path.

### Inkling 2 Mini CUDA integration

The released safetensors text and single-image routes use the shared model
session and admission controller. Request state and concurrent workspace are
admitted together. The same cold decoder layout supplies the allocation sizes
for hybrid BF16 K/V, convolution history, page tables, attention scratch and
command buffers. The existing conservative allowance remains for compatibility
and vendor workspace. Image scratch and DSpark state retain their distinct
storage classifications. The checkpoint loader uses the shared bounded pinned
upload arena. The numerical path reuses the standalone Tile CUDA catalog.

Spark GB10 tests at `89315aae16` exercised the released checkpoint, compared
physical request allocation with the declared envelope, and checked model
logits across 130 positions with capture and replay. The merged hybrid K/V
and relative-attention suites passed on the same device, including the
32,768-token, 48-visit cache rollback case. Warm native CLI generation
returned identical bytes for 64 code tokens before and after the layout
change, with observed decode intervals of 1,532.52 and 1,532.21 ms in one
run each. A production HTTP test on the
initial merge showed a different greedy continuation with batched DSpark
verification for a 26-token code prompt. Production now selects exact
row-wise verification. The batched path remains available for explicit
qualification with `ANTFLY_INFERENCE_INKLING_BATCHED_SPECULATION=1`. The
final production build matched all 96 generated tokens on that prompt with
and without forced DSpark and returned the expected text and invoice answers.
A single warm request took 2,524 ms without speculation and 2,808 ms with
exact DSpark. The earlier batched throughput applies only to that verifier.

At `465b08ea7d`, Spark GB10 passed all five tests across the CUDA
relative-attention and hybrid K/V suites. The shared staged range supplied
the physical paged GQA test with relative logits, a sink and a gate. The
32,768-token rollback test preserved BF16 payload across 48 visits using
2,048 full pages and 81 sliding pages. Local Metal passed its 63-test packed
expert suite, and the shared host KV suite passed 13 tests. Metal coverage
here reaches the resident expert stage. The Metal decoder and released
checkpoint serving path remain open.

At `27d09ee01f`, the released checkpoint passed all four CUDA decoder
suite tests on Spark GB10. The seed token 42 produced 34,851, 220 and 110,104,
with peak logits 26.41237, 27.44930 and 24.88801. The suite also checked
capture through position 130, heterogeneous batching, image prefill and
speculative rollback. The first three decode intervals were 301.52, 37.99
and 36.98 ms in this single run. The Mac GPU first exercised all 48 Metal visits with synthetic weights.

The released Metal checkpoint at Hugging Face revision
`c9d028302cd0f035fdf8e7e91f089a4ce94d2195` then passed all 80 tests in the
focused suite. Metal reproduced CUDA's first three greedy token IDs. Its
peak logits were 26.42824, 27.44696, and 24.87517. The fourth Metal step
produced token 1541 with a peak of 26.31770. The decoder reported
18,881,301,664 resident device bytes and a 55.88 s load. The four decode
intervals were 21,549.53, 564.39, 61.19, and 60.54 ms with concurrent server
compilation. These timings are from one run and leave the on-Mac HTTP and
reference-engine gates open.

### Official Gemma 4 E2B QAT serving comparison

The decoder and projector came from Google's E2B QAT Q4_0 GGUF revision
`675cff42a74c774d6cb76f76d8eacb49b48c9b93`. The comparison against llama.cpp
`b12706` used warm loopback requests, identical media files, F16 K/V, 16
greedy output tokens and disabled prompt reuse.

| Hardware and Antfly route | Text | Image | Audio | Video |
| --- | ---: | ---: | ---: | ---: |
| M4 Max CPU, opt-in shared suffix | 7032 / 416 | 4562 / 14917 | 1003 / 1260 | 4114 / 24000 |
| M4 Max CPU, tiled prefill and grouped decode | 1606 / 416 | 2799 / 14917 | 790 / 1260 | 2118 / 24000 |
| M4 Max Metal, warm manifest | 424 / 185 | 991 / 255 | 310 / 157 | 941 / 361 |
| M4 Max Metal, resident projector | 423 / 185 | 643 / 255 | 310 / 156 | 600 / 358 |
| Spark GB10 CUDA, standard | 1169 / 168 | 4672 / 206 | 875 / 169 | 4447 / 280 |
| Spark GB10 CUDA, BF16 donors and replay | 885 / 169 | 607 / 206 | 286 / 164 | 603 / 281 |
| Spark GB10 CUDA, BF16 donors, tensor-core prefill and warp-split decode | 218 / 169 | 288 / 206 | 223 / 163 | 265 / 280 |
| Spark GB10 CUDA, same plan with persistent decode replay | 201.2 / 168.8 | 271.7 / 206.0 | 220.8 / 164.1 | 243.4 / 279.4 |

Cells report Antfly and llama.cpp p50 milliseconds. CPU used eight measured
samples per engine and modality. Metal and standard CUDA used 24. The CUDA
replay, tuned CUDA and Metal resident comparisons used 12 samples per engine in ABBA
order. The persistent decode replay comparison used 16 samples per engine in
ABBA order at `f42ac57257` and reproduced all four semantic responses exactly.
Spark had other resident CUDA contexts. Both engines used 514 text prompt tokens. Audio prompt token counts varied
with the request and serving route. The image and video serving routes produced different
projected token counts. The video fixture used a silent H.264 clip.

On Native CPU, a four-sample trial of tiled paged prefill on top of shared
suffix pruning reduced p50 from 4997 to 2299 ms for text, 3979 to 3111 ms for
image, 923 to 857 ms for audio and 3453 to 2459 ms for video. All sampled
responses matched the direct paged route. A four-sample follow-up using the
system BLAS provider and skipping masked key tiles measured 1883, 2975, 848
and 2304 ms in the same order. All four semantic response strings again
matched llama.cpp.
The subsequent grouped decode trial reused each converted F16 page row across
query heads. It used four samples per case and the earlier uncontended
llama.cpp reference. The semantic responses remained identical.

On Metal, a four-sample trial of the opt-in resident projector reduced image
p50 from 1015 to 660 ms and video from 959 to 617 ms. Text and audio were
effectively unchanged. The trial produced identical sampled output with and
without the option. In semantic checks, both engines recovered the invoice
number and amount, transcribed the audio, and named the video colors in order.
All four semantic response strings matched llama.cpp on each backend.

At `3d6872cceb`, the production Metal server admitted the prepared 35-layer
prefill plan for each of the four E2B prompts and matched the four llama.cpp
responses with a 64-token semantic allowance. A separate MP4 with an embedded
speech track failed the transcription fixture on both servers. The video
latency measurements above cover frame processing on a silent clip.

A separate four-sample CUDA trial using the explicit BF16 donor plan measured
897, 620, 300 and 624 ms in the same order. The trial kept the text, image and
video response bytes from the ordinary route. The audio response changed an
article and retained the transcript's content. Automatic placement still
requires the existing geometry and mirror inventory qualification. Required CUDA
replay completed one request per modality. The explicitly required SM89 Flash
prefill profile rejected the GB10 capability before launch. The eager gated
token-tensor variant ran correctly and was slower across all four cases.

On GB10, the generic 32-key split decode trial measured 457, 378, 251 and
362 ms across text, image, audio and video, with three samples per case. A
two-sample trial combining that decode schedule with tensor-core prefill
measured 252 ms for text and 287 ms for video, with the same sampled output.

At `c354c55ac4`, functional prefill returned all four E2B semantic answers. Its
representation declined physical replay, and the eager graph was slower in
every case. New geometries now require physical replay admission before
numerical submission. The ordinary CUDA route handles a declined graph.

At `4cc90259a9`, the official E2B checkpoint passed the four fixture fact
checks on both Spark servers with a 48-token cap. The earlier 16-token
semantic run was too short to emit the invoice total on either server. For
timing, the unchanged 16-token workloads used eight measured samples per arm
and the Antfly/llama.cpp/llama.cpp/Antfly order on the shared Spark GPU.
Values are the mean of the two arm medians, in milliseconds.

| Workload | Antfly defaults | BF16 donors and MMA prefill | Also enable F16 MQA warp split | llama.cpp with MQA comparison |
| --- | ---: | ---: | ---: | ---: |
| Text | 1168 | 679 | 201 | 170 |
| Image | 4646 | 543 | 271 | 205 |
| Audio | 880 | 281 | 220 | 165 |
| Video | 4448 | 530 | 242 | 279 |

The F16 MQA route reproduced the earlier tuned regime on the same binary.
It leads the llama.cpp reference on the silent video fixture and trails on
text, image and audio. The tuned flags are explicit, so these measurements
leave default qualification open.

The GB10 automatic policy now requires the exact MQA geometry and complete
Q4_0 donor inventory before admitting its 1,104,150,528 bytes of BF16 mirrors.
The official checkpoint selected 15 donor layers, MMA prefill and F16 MQA
warp split without tuning variables. The same 16-token serving measurement
gave Antfly 200 / 271 / 220 / 244 ms for text / image / audio / silent video.
The contemporaneous llama.cpp medians were 168 / 205 / 165 / 280 ms.
All 64 measured Antfly responses matched the explicitly tuned run's content
and finish reasons. Both engines passed all four fixture fact checks with
48 tokens. The Spark tests passed 94 cases with one Metal-only skip.

### Issue closure audit

This audit uses `97ae654504` as the implementation head and `2fd277baea`
as the merged-main ancestor. It covers the seven open #1166 entries and
the implementation, deletion and qualification gates in #1771.

| Requirement | State at the audited head | Evidence and remaining gate |
| --- | --- | --- |
| Shared liveness and authoritative storage | Partial | Dependency analysis and admitted destination/workspace are shared. The physical Metal scopes and opaque vendor workspaces in the compatibility table still have separate allocation ownership. |
| Stable addresses and CUDA capture | Partial | Prepared plans retain stable storage. Released T5-small has bounded replay at prefixes 1, 8, 32 and 64. Legacy decoder and audio signatures still need ordinary/capture recipe parity before deleting learned address policy. |
| Runtime-owned state and physical policy | Partial | `decoder_storage.zig` owns Gemma4 replay capacity. `decoder_prefill.zig` selects chunk sizes, binds eager routes and owns scheduled work and cancellation. Task-bound state and publication decisions still need the remaining architecture-specific migration. |
| Resident placement | Partial | Released fused T5-small frees both encoder activations before the cached step on Native, Metal and CUDA. Glowworm recorded 393,216 device-to-device gather bytes with zero gather downloads. Untested or opaque encoder/consumer pairs still need residency and output-custody qualification. |
| Task orchestration | Partial | Generation supplies publication callbacks to shared scheduled steps and preserves independent outputs through the publication boundary. Compatibility routes still own numerical transitions for some model/provider pairs. |
| Ownership and deletion | Partial | Ten detected source renames establish common execution ownership. The original `cuda_owned_coverage.zig` wrapper was removed and its provider check is shared by `device_owned_coverage.zig`. The remaining physical compatibility owners have deletion gates in the preceding table. |
| Common telemetry | Partial | Invocation reports now record cache-gather copy counts, bytes and stream ordering alongside the separate model-forward observations. The gather receipt covers submission and ordering. Cross-provider lifetime and retirement observations still need coverage. |

The diff from merged main contains 46 added paths, 225 modified paths, ten
detected renames and one deleted path. The deleted path is the 24-line
CUDA coverage wrapper, of which nine lines follow the license header.
These counts describe the source tree diff. A rename can include edits,
and generated copies and binary sizes are outside this deletion count.

The publication and optional-component suite at the audited head passed
199 of 203 selected tests with four skips. The targeted prefill and
T5-small evidence covers smaller physical and model-specific cohorts.
Closure still requires a current-head tier-one regression and performance
matrix against frozen merged main, including Whisper multi-window,
Gemma4 text/image/audio/video and the E2B comparative deficits above,
EmbeddingGemma2, LightOnOCR, GLiNER2.5/DECIDE, Inkling 2 Mini and the
representative Qwen3 routes. Spark GB10, local Metal and CPU are the
available physical targets. L4 and A100 remain the user-deferred targets.

The Metal integer-gather compatibility guard now applies to the actual
axis-zero column-bias fusion. An axis-one I32 gather after a live F32 add
uses two assigned dispatches and retains both results independently across
later requests. The fixture also covers negative and invalid indices,
zero intermediate downloads, and ownership after session shutdown. The
initial mismatch came from the shutdown probe reusing the second request's
host index buffer. Restoring the first index before the final dispatch
makes that probe compare the intended result.

The same graph with a resident I32 index tensor and a host F32 add operand
now uses two assigned dispatches and issues zero downloads. The selected
Metal frame binds the host operand through its admitted ingress plan and
keeps the resident index on the device path. The stage adapter skips the
temporary import only for inputs named by the selected host plan. A live
Metal cache-branch operand uses the same ingress when the specialized
frame owns it. The boolean selector is validated before dispatch and the
fixture verifies that specialization eliminates its device binding.

### Whisper Small multi-window Metal serving

The default enables packed Metal prefill for the 12-layer, 768-wide,
80-mel Whisper Small geometry. The existing environment override and the
provider’s admission and per-request fallback checks remain available.

The public reader decoded a 71.489-second input containing three separated
copies of the same English clip with timestamps enabled. The five phases
used one warmup and five measured requests in A–A–B–B–A order. A explicitly
disabled packed preparation and B used the default with no override. All 30
publications matched exactly, including three phrase segments and timestamp
offsets. The pooled warm p50 was 1413.81 ms with the override and 1374.38 ms
with the default on M4 Max, a 2.8% reduction.

The 71.49-second Whisper Small fixture also ran against whisper.cpp
`v1.9.6` on M4 Max Metal. Both warm HTTP servers used fixed English,
greedy decoding and phrase timestamps. With ten measured requests per
engine in ABBA order, Antfly measured 1378.43 ms p50 and whisper.cpp
measured 321.47 ms p50. Both returned the same three sentences after
trimming whitespace. The first phrase bounds matched. The two later
starts differed by 10 and 20 ms, and the last phrase end differed by
2.98 s. Antfly's CPU, Metal and Spark CUDA segment text and bounds
matched one another exactly on this fixture.

Whisper Small's owned encoder frames now prefer the existing Metal MPS
dense matrix-multiply path. The preference is scoped to encoder execution
and respects `TERMITE_METAL_ENABLE_DENSE_MPS_LINEAR`. With packed decoder
prefill enabled on both arms and the same five-phase schedule, the
disabled/default warm p50 was 1378.75/1132.22 ms. All 30 publications
matched the previous route exactly, including phrase boundaries.

Whisper Small now stores eligible immutable encoder matrices as F16 after
checking the full F32 round trip. A failed check leaves that matrix in F32.
Using the same five-phase protocol, warm p50 moved from 1134.25 to
1070.42 ms, with the same text and timestamps in every request. English
and Spanish MP4 fixtures also produced identical publications with fixed
and automatic language selection. Provider traces confirmed F16 slot use.

### Gemma 4 resident Metal projection

The qualified Gemma 4 E2B and E4B channel geometries use the verified
model-owned projector inventory by default on Metal. The existing
`ANTFLY_INFERENCE_METAL_RESIDENT_PROJECTOR=0` override disables this policy.

A same-binary M4 Max comparison used the official Google QAT Q4_0 GGUF
checkpoints and projectors, with 12 warm requests per case with the policy
disabled and eight with the default. All responses matched exactly within
each case. Warm p50 latency in milliseconds:

| Checkpoint | Image disabled → default | Silent video disabled → default |
| --- | ---: | ---: |
| E2B | 1084.52 → 740.02 | 933.40 → 601.07 |
| E4B | 1333.56 → 983.89 | 1149.18 → 819.58 |

A separate warm loopback comparison used llama.cpp `b12706`, F16 K/V,
no prompt reuse and a 48-token output cap. Both engines passed the four
short semantic fixtures. Cells are Antfly / llama.cpp p50 milliseconds,
with eight measured requests per engine and case. The Spark GPU had other
resident CUDA contexts during the comparison.

| Checkpoint and device | Text | Image | Audio | Silent video |
| --- | ---: | ---: | ---: | ---: |
| E2B M4 Max Metal | 472 / 223 | 740 / 314 | 251 / 121 | 600 / 363 |
| E4B M4 Max Metal | 567 / 436 | 987 / 517 | 332 / 211 | 818 / 620 |
| E2B Spark GB10 CUDA | 248 / 214 | 348 / 275 | 161 / 126 | 246 / 280 |
| E4B Spark GB10 CUDA | 352 / 324 | 540 / 462 | 260 / 215 | 386 / 464 |

On Spark GB10, both official QAT GGUFs served 131,060 text prompt
tokens with a 12-token output reservation using the default memory limits
and F16 K/V. Both retrieved the code placed at the beginning of the prompt
and returned HTTP 400 when the prompt plus the requested output exceeded
131,072 tokens. The single successful near-limit request per model took
570.17 s for E2B and 54.98 s for E4B with other GPU contexts present. The
E2B prefill used the single-K/V-head MMA schedule. The expanded generated
Flash schedule on GB10 covers the two-K/V-head geometry used by E4B.

The sequential llama.cpp `b12706` comparison used the same model,
131,060-token prompt and four-token response on the same shared Spark.
It took 16.19 s for E2B and 29.85 s for E4B.

On M4 Max CPU, the qualified Gemma 4 shared-KV route skips suffix rows
that cannot affect the published token and reuses decoded K/V across the
query tiles. A same-binary comparison used two warm control requests and
four warm default requests per model and modality. Every response matched
exactly within its case. Cells report disabled / default warm p50 ms.

| Checkpoint | Text | Image | Audio | Silent video |
| --- | ---: | ---: | ---: | ---: |
| E2B | 10301 / 1829 | 6444 / 3242 | 1066 / 639 | 5735 / 2229 |
| E4B | 20078 / 3619 | 10788 / 4758 | 1926 / 1174 | 10173 / 3573 |

The short warm CPU llama.cpp reference measured 467 / 15027 / 1224 /
24059 ms for E2B (four samples per modality) and 1126 / 15437 / 1405 /
24679 ms for E4B (one sample per modality), in the same modality order.

Both checkpoints also retrieved the same code on M4 Max Metal at 16,384
and 131,060 prompt tokens and rejected overflow with the 12-token output
reservation. These Metal runs qualified context admission and retrieval.

On GB10, the generated 64-row CUDA Flash kernel now also handles one
physical K/V head. It is selected for the qualified single-head decoder
beyond 8,192 live keys, and the short MMA route and explicit profiles
keep their existing contracts. With the regenerated CUDA 13.2 fatbin,
E2B returned the same code at 16,384 and 131,060 prompt tokens. The
near-limit request took 33.87 s versus 568.55 s with the original MMA
route on the same shared Spark. E4B returned the same answer in 55.48 s
at 131,060 prompt tokens. Both models retained the overflow rejection.
