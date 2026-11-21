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
| `graph/decode_state_runtime.zig` | KV/recurrent/paged state ownership and advancement |

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
Partial upgrades retire only their own quota and views before retry.
Merged seq2seq caches and full-prefix host decoders use the same execution-owned
stage entry as encoders. That entry preserves an installed dispatcher's exact
permit, gate and cancellation contract, propagates its failures without serial
replay, and rejects a foreign prepaid model permit on the serial path. Host
outputs leave through the same prepared Source custody and retirement boundary.

Decoder construction publishes the actual native, compiled, merged-graph or
full-prefix state route and requested policy through the uniform report. Failed
construction publishes an owned error and no numerical counters or completion
claim. These are request-construction observations, not a backend memory census.

Metal reports the device's instantaneous worker-process allocation gauge alongside
tracked project allocations. It includes vendor resources visible to the driver;
it is neither a per-request peak nor a second ownership ledger. No inferred
"untracked bytes" subtraction combines these separately sampled scopes.

## Remaining migration boundaries

Issue [#1166](https://github.com/antflydb/antfly/issues/1166) also requests a
complete migration of execution policy. Shared kernel ownership and the common
preparation API do not by themselves complete these remaining requirements:

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
  Learned decoder/audio arenas remain compatibility owners where their complete
  temporary recipes have not migrated; capture alone grants no ownership credit.
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
  executors share `graph/session_decoder.zig` for immutable definition ownership.
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
  `graph/decode_batch.zig` now owns uniform, mixed prefill/decode and packed
  batch metadata assembly, including per-sequence physical KV views, recurrent
  caches and position ownership. Generation retains compatibility exports and
  supplies typed state/geometry; batch layout does not depend on a task pipeline.
  `graph/decoder_work.zig` owns borrowed queued prefill/decode records, resident
  prefix geometry and validation. Bounded device bursts read an immutable token
  budget rather than dereferencing task continuation state. Task publication and
  stopping/streaming policy remain in generation. The physical scheduled forward
  is owned by `graph/decoder_step_executor.zig`: it receives explicit context,
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
  prepaid workspaces before applying explicit operator limits once. Opaque
  providers and incompatible state transitions retain compatibility ownership; host-table
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
  chains, logits and accepted activation handoff. The physical decoder owns
  verification suffix geometry and pending tensor/scalar selection; task code
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
  ownership. These regions receive no complete-payload quota credit.
- CUDA prepared projections describe eligible BF16-rounded output separately
  from F32 publication, activation conversion and vendor workspace. Cold
  preparation loads the original widening program; dispatch retains the
  original GEMM and widening order. This ordinary-only route preserves the
  existing capture restriction and declines separate mutable mirror planes.
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
  participants and pending outcomes survive model retirement. Registered owner
  intervals can include concurrent activity; lifetime peaks are labelled as such.
  Successful frames and entered Native calls copy their prepared payload admission
  coverage into the report, including the existing compatibility reason. This
  describes prepared accounting rather than a numerical fallback or quota grant.
  Instrumented Metal region-policy counters remain absent until their actual
  planner runs.
  Unobserved fallback reasons and opaque vendor/private allocation domains remain
  unavailable. These scoped observations do not claim a whole-backend census or
  an exclusive invocation memory peak.

The issue checklist records implementation and evidence separately from remaining
migration. Compatibility paths stay until their replacements satisfy correctness,
residency, lifetime and performance checks. Physical L4/A100 qualification remains
deferred; CPU, local Metal and Glowworm GB10 provide the available hardware tests.
