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

Whisper's ordinary PJRT stages preserve portable attention through model
program capture so XLA owns its lowering. On Apple Silicon, the GoMLX
CPU PJRT plugin v0.117.2 passed both released Whisper Small fixtures:
public-loader encoder and decoder parity with Native, and full-window
resident bank and first decode parity with the full-prefix path.

The shared PJRT capture policy at `59d19fe609` applies the same portable
graph pipeline before specialization for all captured stages. The Apple
Silicon CPU cohort passed seven released-checkpoint tests. EmbeddingGemma2
passed public text parity and the vision, composition and embedding
stages in F32 and BF16. The measured final image embedding cosine was
0.99988–0.99992 across the two cases. GLiNER2.5 passed DeBERTa backbone
parity. Both Whisper Small fixtures passed again. Gemma 4 E4B QAT GGUF
passed positional-table HLO serialization, which exercises compilation
input construction but leaves numerical execution to a separate fixture.

With the same plugin, released Gemma 4 E4B passed image, audio and video
projection parity and short resident decoding against full-prefix
execution. The mixed embedding fixture supplied the model's per-layer
token IDs. F32 logit differences were below 0.0001 and BF16 passed its
F32-relative error and greedy-token checks. DECIDE-1B passed exact F32
direct and HTTP classifier parity. These tests exercise model execution
and serving correctness on CPU; they provide no latency gate.

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
  Eager and compiled prefill use the same speculative in-flight row bound.
  Decoder storage validates the opt-in sampled ring against scheduling and
  compiled routes and selects cooperative KV metadata scopes. The decoder
  runtime checks Metal driver-frame eligibility. Generation supplies the
  request's grammar, scheduling and output intent.
  The checkpoint runtime validates the loaded route and append limit before
  KV mutation. Preparation, snapshot export and restore share the donor
  geometry builder while retaining their existing serialization contracts.
  The checkpoint cohort passed 152 selected host tests with 62 skips, including
  the E4B donor geometry, conflicting requests and exact append boundary.
  The hosted-audio fixture required loopback access for the same binary.
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
compilation. These timings are from one run.

The experimental Metal chat route, enabled with
`ANTFLY_INFERENCE_INKLING_METAL_QUALIFICATION=1` and measured with Metal
required, returned `4` for a 28-token arithmetic prompt through
`/ml/v1/chat/completions`. Cold and warm HTTP requests completed in 37.24 s
and 1.78 s. The warm streaming response emitted the same answer, a stop
reason and `[DONE]` in 1.72 s. The focused Metal suite also covers the
BF16 device embedding handoff and in-frame greedy argmax on a synthetic
decoder. A released-checkpoint suite with batched text prefill passed all
80 tests. On a 16-token prefix, prefill fell from 957.19 ms serial to
521.92 ms batched, and the next-token logit vectors matched in that run.
On the M4 Max, the batched build returned `4` for the same short HTTP
request in 1.30 s median over three warm runs. llama.cpp b12706 returned
`4` in 0.339 s median using the same serialized 28-token prompt and its
NVFP4 GGUF, with all 49 layers offloaded to Metal. Both engines returned
`271` on the lookup request. Antfly used 6,397 prompt tokens and completed
in 215.74 s. llama.cpp used 6,396 and completed in 20.52 s. The reference
was configured for an 8,192-token window. Antfly needs faster prompt
execution, and full-context qualification remains open.

The Metal BF16 vision tower and spatial adapter ran the same 57 by 62
patterned RGB fixture used by the CUDA suite. Both produced four embedding
rows and selected token 12. The Metal peak logit was 19.88336 and the CUDA
peak was 19.87989. Metal encoded the image in 113.32 ms in that run. The
released-checkpoint suite, including legacy managed manifest repair,
passed all 81 tests. The opt-in Metal HTTP route correctly identified
80 by 80 red and blue PNGs in separate requests. Warm responses took
1.64–1.65 s. Streaming returned `Blue`, a stop reason, token usage, and
`[DONE]` at 1.64 s. The same 33-token color prompts and 80 by 80 PNGs
returned `Red` and `Blue` on llama.cpp b12706 using its NVFP4 GGUF and
BF16 projector. Its six warm completions took 0.405 s median with prompt
reuse disabled.

The opt-in `cache_dtype: "int4"` format stores four-bit global K/V with one
FP16 scale per 128-element head. Sliding K/V and convolution history stay
BF16. At the advertised 1,048,576 positions, the request planned and
allocated 9,035,481,352 KV bytes versus 34,536,849,672 bytes in the exact
BF16 layout. The warmed Metal process completed the first decode with that
capacity reserved. A separate GPU test read the remapped last page across
a synthetic million-token sequence. All 83 tests passed. Running the full
model through a million-token prompt and measuring accuracy and latency
at that length remain open. The HTTP option returned the expected answers
for a 28-token text prompt, a 33-token image prompt, and a mixed 6,404-token
lookup request. The last request returned `271` in 220.80 s. llama.cpp's
all-Metal full-window configuration with Q4_0 K/V ran out of GPU memory at
first decode with prompt batches of 64 and four. The four-row run reported
29,928 MiB of GPU buffers against a 28,753 MiB recommended working set.
This compares the tested memory configurations. A full-length benchmark
still needs completed prompts on both engines.

The 16-token compressed probe selected the same next token as BF16, with
a maximum logit deviation of 1.89. The experimental native BF16 matrix
projection was replaced with a shared-weight projection that retains
the scalar arithmetic order. It reused one weight read across four input
rows. All output bits matched on the three physical model-width projection
fixtures, which improved by 1.5–1.8× on the M4 Max.

The grouped routed projection builds a device-side map of tokens sharing an
expert and reuses it across the gate/up and down projections. Its precise
variant shares the decoded weight while preserving scalar accumulation
order. All 294,912 BF16 outputs matched bit for bit on the synthetic
model-width fixture, where the grouped path took 2.86 ms versus 5.51 ms for
the scalar kernel.

On the released checkpoint, the 16-token grouped prefill took 438.86 ms.
The 64-token prefill took 1,312.42 ms versus 2,089.60 ms for four ungrouped
16-token chunks. Both comparisons matched the complete next-token logit
vectors. All 84 tests passed with grouping enabled. The final two warm
decode steps took about 62 ms wall time and 59–60 ms of GPU time, so most
decode latency is on the device. The exact grouped path is enabled inside
the opt-in Metal backend and can be disabled with
`ANTFLY_METAL_GROUPED_ROUTED_PREFILL=0`. Text and device-resident image rows
now share the same prefill batch. The combined exact path passed all 84
tests and took 1.18 s for the 64-token prefill, with identical logits to
the four ungrouped chunks. Grouped-query attention also shares one K/V load
across three query heads for eligible long spans. At 32 queries across
4,096 keys, the BF16 kernel fell from 2.60 to 1.14 ms and the packed kernel
from 2.33 to 1.10 ms. All FP32 partials matched bit for bit. The released
checkpoint suite still passed all 84 tests, including three queries
reading the remapped final position of the synthetic million-token cache.

The follow-up HTTP comparison on the same M4 Max used the released
Safetensors in Antfly and the pinned NVFP4 GGUF in llama.cpp b12706. Both
answered `4`, classified the red and blue PNGs correctly, and returned
`271` for the long text lookup. Each engine received the same serialized
prompt where its chat interface allowed it. The lookup token counts differ
by one. Warm short and image figures are medians over three and six runs
respectively. Long figures are individual runs.

| HTTP workload | Antfly Metal | llama.cpp Metal |
| --- | ---: | ---: |
| Short text, 28 prompt tokens | 0.719 s | 0.339 s |
| Red and blue images, 33 prompt tokens | 0.832 s | 0.405 s |
| Text lookup, 6,397 / 6,396 prompt tokens | 127.76 s | 20.52 s |
| Packed K/V image lookup, 6,404 prompt tokens | 125.12 s | — |

The packed-cache image lookup also returned `271`. The streaming image
request returned `Blue`, a stop reason, usage and `[DONE]` in 0.833 s.
No comparable vLLM Metal run was available for this checkpoint.

The follow-up used llama.cpp `b12776` (`e38ea719b`) on the same host and
GGUF, with explicit M4 Max Metal offload. Three warm HTTP observations
per text case matched answer and token count. Antfly and llama.cpp p50
were 0.591 s and 0.292 s at 28 prompt tokens, and 80.035 s and 17.546 s
at 6,396 tokens. The image comparisons failed parity. llama.cpp emitted
`<|message_model|>` and counted 31 and 6,401 prompt tokens where Antfly
counted 33 and 6,403, so those latencies do not qualify image performance.

The patched Antfly HTTP server also accepted two ordered images in
both red-first and blue-first order. Each Metal request returned the
first image's color, 50 prompt tokens, one completion token, a stop
reason, and `[DONE]`. The shared generation media, byte, and pixel
limits still apply.

Inkling awaits a verified vLLM-Metal runner. See the
[vLLM-Metal support matrix](https://github.com/vllm-project/vllm-metal/blob/main/docs/supported_models.md).

The full-window HTTP comparison uses exactly 1,048,576 prompt tokens
and one completion token. Its image case asks for an early lookup key
for a blue image and supplies a different answer for a red image.
The comparison checks the prompt and image hashes, reported token counts,
expected answer, and cache reuse. It records the comparator's actual GPU
layer placement. Only completed requests with all layers offloaded count
toward the Metal latency target. The Antfly stream must return final usage
and `[DONE]`, and a request reserving one additional output token must
return HTTP 400 before opening the stream.

With the normal model directory and both backend override variables unset,
the optimized HTTP server selected Metal. A mixed `/ml/v1/generate/batch`
returned `4` and `Red` for valid items and an item-level error for an audio
part. The normal generate route returned `4` in 0.72 s warm. Streaming image
chat returned `Blue`, a stop reason, token usage, and `[DONE]` in 0.84 s.
After disconnecting a longer stream, the next text request returned `4`
in 0.84 s. These requests use existing model selection, memory admission
and the GPU runtime's execution gate.

On the pinned Metal HTTP build at `25ec163bbd`, the same-binary
A–B–B–A comparison measured the opt-in final-frame greedy selection.
With 12 measured requests per configuration and case, median latency
fell from 0.590 to 0.543 s for 28-token text and from 0.678 to 0.643 s
for a 33-token image. Both 257-token tail cases varied by less than 0.3%
between configurations. The before/after and repeated candidate medians
varied by less than 0.4% in each case. The requests checked answer and
token counts. The head-to-head llama.cpp comparison remains open.

A wider Metal prefill batch increased capacity from 64 to 256 rows, compacted
the final partial batch into a separately sized workspace, and gave each
query its own context-dependent attention reduction boundaries. All 84
tests passed. A 273-token released-checkpoint prefix took 4.95 s versus
5.78 s with 64-row groups and a scalar remainder, with identical next-token
logits. The warm HTTP medians were 0.716 s for short text and 0.831 s for
images. The 6,397-token text lookup returned `271` in 118.94 s, and the
6,404-token packed K/V image lookup gave the same answer in 119.12 s.
The 256-row plan allocated 9,074,803,012 bytes of packed K/V at the full
window and completed a decode with that capacity reserved. Its BF16 K/V
layout would require 34,576,171,332 bytes.

A GPU counter run of the focused test cohort attributed 51.1% of GPU
time to grouped routed expert projections and 14.2% to grouped BF16
dense projections. Fusing the routed gate improved the prefill probe
but changed released-checkpoint logits and slowed single-row decode.
Compact GPU dispatch preserved the logits and took 4.94 s on the
273-token probe, compared with 4.95 s before the change. The branch
keeps the previously qualified projection path.

The first full-window image HTTP request passed tokenization and reached
Metal admission. The request and required model residency needed
29,159,583,056 bytes against the automatically selected combined limit
of 28,991,029,248 bytes. This included 280,661,148 bytes of admitted host
residency. The runtime now tries smaller prefill workspaces before taking
a permit for the selected request geometry. This keeps the context size
and K/V format fixed and checks the same live limits at acquisition.
The optimized build passed the text/image batch and streaming smoke test
and admitted the same full-window image request under the unchanged
automatic limits. The older admission build completed that image
request on the M4 Max with packed K/V. It returned `271` at 63,517.22 s
and reported 1,048,576 prompt tokens, one completion token, and `[DONE]`.
This single instrumented run establishes completion for that build and
fixture. The matched image-dependent and text full-window comparisons
remain open. The next Metal qualification run covers short through
32K prompts before returning to the full-window comparisons.

The shared ByteLevel tokenizer now applies the checkpoint's exact
Unicode Split expression, including casing, three-digit groups, and
punctuation followed by newlines. With the released tokenizer, the
short prompt still has 28 tokens and the long text prompt now matches
the reference at 6,396 tokens. The earlier HTTP timings retain their
original measured token counts.

Metal now reuses attention reduction storage across query tiles when the
combined memory budget would otherwise shrink the projection batch. With
24 query heads and 64 key splits, a 256-row batch can use about 25.6 MB
of reduction storage with 32-row query tiles instead of about 204.5 MB.
The physical Metal suite passed 83 tests, including bitwise parity for
tiled and untiled output. The released-checkpoint test was deferred while
the full-window HTTP scan held the resident model.

The generated Metal attention program shares K/V loads across two adjacent
queries and their three query heads on eligible global spans. It reads the
page table once per visited page and preserves each query's accumulation
order. BF16 and packed K/V probes matched every FP32 partial bit. The
physical Metal test also checks that the penultimate query in a synthetic
million-token span cannot see the value at the remapped final position.
End-to-end latency for this change awaits a released-checkpoint rerun.

Paired prefill now skips relative bias on pages older than both queries'
bias range. When that page also fits both original split bounds, the
same kernel drops the per-key visibility checks. A physical M4 Max probe
with seven query rows at position 4,096 and shuffled pages matched all
FP32 partial bits against the per-key visibility version for BF16 and
packed Q4, with global and 257-token sliding attention. The Metal driver
reported SIMD width 32, a 1,024-thread limit and zero static threadgroup
bytes for all eight before and after entry points. The generated Metal
source check and production HTTP build passed. The physical packed
million-token attention test passed with 1,024 relative positions,
remapped final pages, and distinct causal limits on adjacent queries.
Serving latency and completed full-window HTTP requests remain pending.

The public Metal route now selects the resident Inkling model automatically.
It handles ordered images, sends idle SSE comments at prefill checkpoints,
and validates context and cache options before starting the stream. The
short-context decoder fixture uses six Metal state buffers after packing
history and KV planes into bounded arenas. It verifies capacity accounting,
retirement, and retry after a failed tail allocation. A separate
physical Metal fixture writes and reads packed KV through nonzero offsets
in the same backing buffer before publishing the append.

At `17d928a106`, Metal stages both KV planes in one dispatch per visit for
BF16 and Q4. Staging and attention share the request's device page table.
The previous Metal path walked host page spans and encoded copies or packing
for each span. Physical tests verified a reversed page mapping across the
append boundary, untouched BF16 rows outside the suffix, matching packed
attention output, and the complete synthetic decoder visit schedule.
At `d8758959ca`, the single-row Metal attention path also consumed
one contiguous projection slab. Physical counters measured one dispatch
for the six base projections with shared input and three with separate
KV inputs. Both produced bitwise-identical BF16 outputs to the independent
projections. The production HTTP semantic check passed. Released-checkpoint
latency for these dispatch changes remains open.

The later Metal decoder groups its prefill projections in one launch for
a shared input and three for separate K and V inputs. The focused cohort
at `00a9b69a12` passed 11 of 12 checks, with the full released decoder
deferred. The vision encoder now dispatches one Q/K/V projection per
layer and takes its intermediate views from a single arena. At
`3ca1ebbb1a`, the grouped encoder produced bitwise-identical projected
image rows to separate projections on the released checkpoint. The
workspace receipt at `86ec14fea1` checks that planned prefill storage
matches the physical binding, including a final partial batch.

On the M4 Max, the combined cohort at `f2e941c98c` passed 32 of 33 selected
tests. The released-checkpoint test was deferred while the full-window
request ran. At `ab3b1e1693`, 20 of 21 retirement and preparation checks
passed, including real Metal KV buffer aliases and shared stateful decoder
preparation from ONNX, Safetensors, GGUF, and Q8. The CUDA fixture was
skipped in that build. The production Metal HTTP executable passed semantic
checking. Live current-head HTTP latency and full-window completion remain
open.

On Spark GB10, the CUDA HTTP binary at `31623aefd4` automatically selected
CUDA and returned `4`, `Blue`, and `271` for the 28-token text, 33-token image,
and 6,403-token mixed requests, respectively. The over-limit streaming
request returned HTTP 400 before opening the stream. A separate public
batch returned red image, text, and blue image results in submission order
while rejecting the unsupported streaming item. The execution report
recorded three serial items and one rejected item. These were shared-device
correctness runs.

A subsequent Spark run at `b047fa328f` used the shared checkpoint's registered
name `thinkingmachines/Inkling-2-Mini`. Three warmed samples per case gave
267.7 ms for 28-token text, 362.7 ms for a 33-token blue image, 56.64 s
for 6,396-token text, and 56.75 s for a 6,403-token image lookup. Short image
streaming and the mixed batch passed. The long image stream returned `271`
and `[DONE]` but did not emit an idle comment before its first content at
56.86 s. That result leaves long-stream liveness unqualified at this revision.
These are shared-device correctness samples with measured wall time.

The optimized Spark HTTP server at `1cc84f1aa2` emitted SSE comments at
15.03, 30.07, and 45.13 s before the long image stream returned its first
content at 56.53 s. It returned the expected answer and final usage.
The prefill receipt recorded 6,402 committed input tokens in 402 chunks.
The 28-token text and 33-token image requests recorded 27 committed
tokens in two chunks and 32 in four chunks, respectively. One report covers
each prefill phase, so the bounded diagnostics queue can retain its counts
even when the request needs hundreds of chunks. These counts exclude the
last prompt token handled by the first decode step.

The same binary served two exact 32,768-token streams and returned `271`
in both cases. Text committed 32,767 prefill tokens in 2,048 chunks and
reached first content in 383.01 s including cold load. The warm image
request reached first content in 345.67 s and committed 32,767 tokens in
2,050 chunks, including the projected image rows. Both streams emitted
periodic keepalives before content and returned final usage and `[DONE]`.
These single requests establish correctness at this depth on Spark.

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

This audit covers implementation head `dec232d868` and merged main
`451f3add98`. The serving measurements below identify the revisions that
actually ran. The seven remaining #1166 requirements map to the #1771
implementation, deletion and qualification gates.

| Requirement | State | Evidence and remaining gate |
| --- | --- | --- |
| Shared liveness and authoritative storage | Partial | Assigned execution and admitted storage share dependency analysis. Metal image and video bind workspaces through the vision region. CUDA grouped audio and the bounded decoder still learn their composite allocation schedules. The compatibility table identifies opaque vendor workspaces with separate accounting. |
| Stable addresses and CUDA capture | Partial | Released T5-small replayed at prefixes 1, 8, 32 and 64. Released EmbeddingGemma 2 grouped audio captured and replayed using a 38,017,792-byte arena. The context-level projector guard declines unplanned cuBLASLt staging before allocation. Declared ordinary and capture workspace parity remains open for the learned composite routes. |
| Runtime-owned state and physical policy | Partial | `decoder_storage.zig` owns Gemma 4 replay capacity and `decoder_prefill.zig` owns chunk selection, eager binding and scheduled cancellation. The shared frame owner now controls media view retirement. Residual task-bound state and cache policy need provider-specific verification and migration. |
| Resident placement | Partial | The released fused T5-small step retired its encoder activations before decoding on CPU, Metal and CUDA. Spark recorded 393,216 device-to-device gather bytes and zero gather downloads. Imported ONNX handoff tests exercise exact output custody and host fallback on all three providers. Other eligible stateful and opaque producer/consumer pairs still need qualification. |
| Task orchestration | Partial | Media tasks call shared frame and projection retirement owners. Scheduled generation routes publish independent outputs through runtime callbacks. Some hardware-specific paths still choose or own physical transitions in model and task code. |
| Ownership and deletion | Partial | Nine execution source moves, removal of the 34-line legacy ONNX KV file and 24-line CUDA coverage wrapper, and removal of direct Metal frame-view, projection-slot and vision-workspace wrappers. The remaining compatibility owners retain the deletion gates in the preceding table. |
| Common telemetry | Partial | Invocation reports distinguish gather transfers and ordering, CUDA audio learning and replay outcomes, and actual workspace bindings. The Metal audio selector uses completed GPU timing through existing frame retirement. Cross-provider lifetime attribution and coverage of opaque and pending routes remain open. |

Relative to merged main, Git reports 106 added paths, 276 modified paths,
ten rename matches and one deletion. The match from the old CUDA coverage
wrapper to a new GLiNER test reflects shared boilerplate. The provider
check moved into `device_owned_coverage.zig`. File counts include generated
copies and artifacts. They do not measure authored deletions.

Current scoped serving evidence includes Spark Whisper multi-window,
EmbeddingGemma 2 text and two-clip audio, LightOnOCR, GLiNER2.5, Qwen3,
and all four official Gemma 4 E4B modalities. Local Metal image and video
improved and the two-clip audio regression passed requalification after
the completed-frame fix. Isolated Native text embedding and GLiNER2.5
passed. Native two-clip audio reproduced a same-binary repeatability
failure with the timing-based FFT/DFT selector. Fixing the arithmetic
route stabilized outputs across all requests in each of the FFT and DFT
control and comparison arms. The 32-sample-per-side comparison measured
224.28/227.19 ms for DFT and 223.67/227.30 ms for FFT (median/p95).
The largest coordinate difference between routes was 2.98e-7 across
the two 768-component vectors. The fixed FFT route passed the independent
log-mel reference test. This comparison used one candidate server with
the DFT override for the reference.
Final closure requires the remaining tier-one coverage, including DECIDE
on the remaining supported targets, Inkling 2 Mini, and the E2B comparative
deficits above, as well as normal PR review and CI. L4 and A100 remain user-deferred.

The original FP32 DECIDE Native HTTP comparison on Spark ARM CPU used
`9e4af7f847` and `5be5fbf2a3` with 32 samples per side in each phase.
All responses matched exactly at batches 1, 8 and 32. Batches 8 and 32
passed the latency gates. Median/p95 changed from 2676.57/2844.89 to
2672.74/2793.00 ms and from 10780.63/11300.32 to 10776.62/11076.40 ms.
Batch 1 remains unqualified for latency because candidate-only median
controls differed by 13.13%. Its measured A/B median was 339.54 to
320.10 ms.

Subsequent scoped qualification covers these production paths:

| Revision | Backend and workload | Result | Remaining evidence |
| --- | --- | --- | --- |
| `b7f0ba1f9e` | M4 Max Native and Metal, Florence admission and GLiNER reduced formats | The combined regression suite passed 120 checks with 21 skips. The Native Florence public path used the planned decoder with retained preprocessing, unwound cancellation and allocation failures, and preserved compatibility at the exact base admission floor. The Metal Florence frame and reduced-format GLiNER numerical checks passed. | Released-model throughput on this revision. The CUDA checks in this local run covered metadata and geometry only. |
| `676b75ffb9` | Spark GB10 CUDA, released merged T5-small | Batched suffix decoding completed after encoder retirement. The grouped CUDA suite passed 16 of 16 checks with no skips. | A current-head throughput comparison against merged main. |
| `ae601982e2` | Spark GB10 CUDA, audio input contract and cache fixtures | All six checks passed, including physical audio capture and replay. | The composite scratch recipe still comes from an observed allocation schedule. Released-model performance remains unqualified. |
| `a4d50e41a0` | M4 Max Metal, released LightOnOCR-3-0.8B and audio cohorts | Both HTTP pages matched the single-image OCR output. Five four-token bursts completed with zero frame topology mismatches. The three-clip audio test used 108,101,632 incremental device bytes within its 134,217,728-byte ceiling. Managed audio prepared two arenas in one pause. | Frozen-main latency and throughput comparisons. These tests establish parity, submission topology, and the measured memory bound. |
| `17a365c92d` | Spark GB10 CUDA, released LightOnOCR-3-0.8B and EmbeddingGemma2 | Ten checks passed with zero skips. The decoder reused the admitted input buffer for both steps, and the warm-up head fixture wrote to the assigned logits buffer. The released two-row LightOnOCR graph replay used 44 bytes of admitted pinned readback. Released EmbeddingGemma2 flat audio and mixed-input admission checks also passed. | Current-head performance and measured peak scratch. Legacy decoder and composite audio still obtain their full workspace layouts from observed lifetimes. |
| `00609abb56` | M4 Max Metal, released Gemma 4 E2B QAT audio projector and segmented EmbeddingGemma2 audio | Three checks passed with zero skips. The Gemma 4 projector differed from the host reference by at most 0.000354 with all device operations enabled. Resident one- and two-clip prompt output matched the device reference exactly. | Peak scratch and latency on the final head. |
| `00609abb56` | Spark GB10 CUDA, original FP32 GLiNER2.5-Decide | The hash-pinned original checkpoint passed the distinct-input HTTP fixture, including four physical forwards for 32 items. A queued disconnect drained, restored leases, launched no queued GPU work and preserved retry results. | The GLiNER fast projection and Decide BF16 release fixtures require the eligible L4 configuration. GB10 evidence covers FP32 serving and the separately tested FP32 attention routes. |
| `918e1c597c` | Spark GB10 CUDA, released EmbeddingGemma2 audio and assigned-input fixture | Four checks passed with zero skips. Released flat audio, mixed-input admission and segmented audio passed. The source-linear fixture completed learning, actual graph capture and replay with refreshed device inputs. Borrowed outputs were rejected by the custody fixture. | CUDA memory and latency comparisons at the final head. |
| `3dcb19fb77` | Spark GB10 CUDA, released EmbeddingGemma2 grouped media and workspace fixtures | Six checks passed with zero skips, including physical warm-up, learning, capture, replay and assigned execution diagnostics. The two-clip cohort learned one workspace, captured one graph and replayed twice. Planned and bound arena capacity both measured 38,017,792 bytes, with zero capacity saved by external scratch repacking at this shape. The fixture also checked mixed text, image and audio, plus video. | Released HTTP latency and peak scratch comparisons, and replacement of the observed composite workspace with a declared layout. |
| `4e7d8cbc46` | Spark GB10 CUDA, original FP32 DECIDE, grouped EmbeddingGemma 2 and bounded decoder receipts | Ten checks passed with zero skips. DECIDE preserved exact outputs and restored leases for distinct-input batches up to 32 in both orders. Grouped audio replayed twice and the decoder drained a failed receipt transfer. | Frozen-baseline DECIDE throughput and declaration of the learned composite workspaces. |

At `6cf0c97584`, a controlled Whisper-small run used the released
checkpoint and the checked-in audio fixture against issue #1771's pinned
baseline `9e4af7f847`. Every run reached EOS on its selected backend, and
the output payloads matched byte for byte. Baseline and candidate
same-binary controls were within 0.9% in both directions at p50 and p95.
CPU lifecycle p50/p95 changed from 661.44/664.68 ms to 661.23/663.56 ms.
Metal changed from 460.49/462.25 ms to 344.22/345.35 ms. This comparison
covers the single fixture on CPU and Metal. CUDA qualification remains
open.

At `918e1c597c`, the public HTTP route compared the same pinned baseline
against a 71.49-second Whisper-small recording containing three speech
windows. On both backends the exact transcript and segment timestamps
matched across all arms. Baseline and candidate same-binary controls
were within 0.4% at p50 and p95 in both directions. CPU HTTP p50/p95
changed from 2049.20/2057.71 ms to 2049.89/2055.17 ms. Metal changed from
1410.80/1422.07 ms to 1065.45/1075.19 ms. These are whole-request timings
with the selected backend confirmed in each server log.

On Spark GB10 at `5be5fbf2a3`, the same 71.49-second Whisper-small
HTTP fixture matched the frozen `9e4af7f847` transcript and all three
segment timestamps exactly. All 12 arms verified CUDA selection.
Baseline/candidate p50 was 1370.54/1370.02 ms and p95 was
1372.55/1375.61 ms. Baseline-only and candidate-only controls differed
by at most 0.11% at p50 or p95. The run shared Spark with a separate
184 MiB CUDA context.

The same Spark revisions also passed the exact-output EmbeddingGemma 2
HTTP comparison with 16 measured requests per side in AA, ABBA and BB.
Text p50/p95 changed from 7.91/8.24 to 7.98/8.58 ms. Two distinct
equal-length audio clips in one request changed from 49.90/50.45 to
48.56/49.51 ms. All arms confirmed CUDA selection. The experimental
CUDA audio graph flag was enabled for both binaries.

Seven additional Spark HTTP cases passed exact output, backend selection
and latency gates against the same frozen baseline, with 16 measured
requests per side per phase. The separate 184 MiB context remained
resident at the start and end.

| Workload | Baseline p50/p95 (ms) | Candidate p50/p95 (ms) |
| --- | ---: | ---: |
| LightOnOCR | 326.07/328.31 | 322.63/325.79 |
| GLiNER2.5 | 12.64/13.53 | 10.43/11.37 |
| Qwen3 | 85.90/87.14 | 86.45/87.49 |
| Gemma 4 E4B text | 222.17/224.15 | 222.82/225.88 |
| Gemma 4 E4B image | 279.29/282.30 | 279.80/281.77 |
| Gemma 4 E4B audio | 217.47/221.51 | 215.84/222.66 |
| Gemma 4 E4B video | 253.30/256.94 | 252.78/255.20 |

Original FP32 DECIDE on the same Spark revisions passed all timing controls
with 32 measured requests per side per phase. Singleton payloads matched
exactly. Eight- and 32-item payloads matched in every field other than
classification scores, whose maximum absolute difference was 6.56e-7,
within the existing FP32 fixture tolerance. The strict bitwise failure is
retained in the qualification receipt.

| DECIDE batch | Baseline p50/p95 (ms) | Candidate p50/p95 (ms) |
| --- | ---: | ---: |
| 1 | 23.25/24.17 | 22.27/23.09 |
| 8 | 67.14/68.61 | 61.69/62.78 |
| 32 | 259.78/262.53 | 240.71/242.81 |

A separate local DECIDE-1B singleton case at `da4dea80de` passed
exact outputs and all timing controls with 32 samples per side per
phase. Native CPU p50/p95 was 123.38/127.72 ms on the baseline and
123.97/127.19 ms on the candidate. Metal was 29.86/30.46 and
29.95/30.35 ms, respectively. This used the cached DECIDE-1B checkpoint.

On M4 Max at `da4dea80de`, all ten Metal HTTP workloads matched
outputs and confirmed Metal selection against the frozen baseline.
Eight workloads passed the timing gates. Gemma 4 E4B image p50 fell
from 1085.83 to 737.17 ms and silent video from 1039.62 to 704.76 ms.
Whisper p50 fell from 488.56 to 371.62 ms. The two-clip EmbeddingGemma 2
audio p50 rose from 37.40 to 40.73 ms with valid same-binary controls.
EmbeddingGemma 2 text had unstable timing controls. Both embedding
cases require isolated repeats.

The 48-sample-per-side Metal audio repeat retained exact outputs and
confirmed the regression. Baseline p50/p95 was 35.40/36.14 ms and
candidate p50/p95 was 40.66/41.19 ms. Baseline-only and candidate-only
p50 drift stayed below 0.3%. The isolated Native CPU text embedding and
GLiNER2.5 repeats both passed exact output and timing gates with the
same sample count.

Metal audio route selection now samples completed GPU frames through
the existing retirement wait and keeps a measured slower route out of
the warm latency tail. A completed-frame trace measured roughly 4 ms
for eager frames and 6 ms for replay on the two-clip fixture. Against
the same frozen baseline, the fixed server passed with 48 measured
requests per side per phase, exact outputs, and Metal selection in
every arm. Baseline/candidate p50 was 35.30/35.52 ms and p95 was
35.62/35.97 ms. Same-binary p50 and p95 differences stayed within 1%.

At `34296ffb5a`, five Native CPU fixtures passed for resident handoff and
task output custody. The incremental decoder matched host-reference logits
after the encoder and destination session handles closed, and allocation
failure preserved the resident source for retry. The CLIP and CLAP fixtures
exercised quota lifetime and cancellation. The BERT head fixtures checked
resident input, final-logit materialization and the unknown-dtype fallback.
A separate seven-test CPU cohort passed for pending submission, readback,
retirement and observation. These use fixture models and do not establish
released-model or GPU coverage.

At `4e7233ca5f`, the imported ONNX reading fixture passed on Native CPU,
M4 Max Metal, and Spark CUDA. The explicit host reference kept its original
session and used admitted input storage. The resident GPU routes read back
the 12-byte vocabulary row and avoided the intermediate encoder download.
Both branches matched text, stop reason, and token count. The fixture also
checked cancellation and confirmed that separate Native contexts can
share resident storage while foreign GPU contexts take the host route.

At `60468a11d5`, the shared cache-role parser passed the focused physical
Metal and Spark CUDA checks for imported ONNX handoff and recognition of
qualified omitted cross-cache columns. Exact and omitted cache shape
validation also passed on both hosts. These checks exercise the resident
provider path and the batching contract. The declared host route remains
available for provider pairs without resident handoff support.

At `86ec14fea1`, six focused checks passed with zero skips on Spark GB10.
They covered imported ONNX cache handoff, retirement of an actual captured
KV lease outside its physical frame, BF16 projection replay against
frozen schedules, and source-audio learning, capture, and replay with
cuDNN. The same run checked publication of prefill geometry and CUDA
audio workspace provenance. The ONNX graph target and all 94 Wasm
compute tests passed locally, and the browser wasm32 build succeeded.
Released-model performance and replacement of the learned audio layout
remain open.

The Spark GB10 HTTP comparison used frozen main `20563ec50a` and candidate
`9e6097c363`, the shared cached models, and Google's official E4B QAT
Q4_0 GGUF with its projector. Each AA, AB, and BB phase used four arms,
two warmups per arm, and six retained measurements per engine and case.
All compared outputs matched exactly. The server logs verified CUDA
selection for every model in every arm. Another CUDA context remained
resident on the device.

| Workload | Main p50 (ms) | Candidate p50 (ms) | Change | Largest same-binary median difference |
| --- | ---: | ---: | ---: | ---: |
| Whisper | 382.46 | 382.73 | +0.07% | 0.24% |
| EmbeddingGemma2 | 9.07 | 9.01 | −0.68% | 4.39% |
| LightOnOCR | 320.18 | 321.08 | +0.28% | 0.73% |
| GLiNER2.5 | 12.83 | 13.32 | +3.87% | 1.63% |
| DECIDE | 40.75 | 40.16 | −1.43% | 1.94% |
| Qwen3 | 84.99 | 84.54 | −0.53% | 0.92% |
| Gemma 4 E4B text | 221.81 | 224.57 | +1.24% | 0.70% |
| Gemma 4 E4B image | 279.65 | 280.63 | +0.35% | 0.56% |
| Gemma 4 E4B audio | 216.67 | 219.62 | +1.36% | 0.35% |
| Gemma 4 E4B video | 254.58 | 256.78 | +0.87% | 0.42% |

The GLiNER2.5 and Gemma 4 text, audio, and video increases exceed the
observed controls in this cohort. Their higher-sample repeat is pending.

At `3e6bcf05c5`, both physical GPU fixtures passed the incremental
ONNX cache handoff and output-retirement regression. The shared role
parser now drives declared geometry and retained quota for self and cross
cache. The admission fixture rejects malformed merged-cache names while
decoder-only artifact outputs retain their compact self-cache spelling.

The same source revision passed three Spark workspace checks. The
legacy decoder recording with unpinned scratch executed the ordinary
result once and published no reusable graph. The audio metadata owner
retained custody when cleanup authority was missing. With the existing
cuDNN library explicitly selected, the BF16 source-audio fixture passed
learning, cancellation, recording, replay, and held-output checks.
These fixtures exercise the compatibility paths. They do not remove the
remaining declarative workspace migration.

At `61adc44649`, the M4 Max retirement cohort passed 129 of 142
selected tests in each run with the released T5-small merged decoder
enabled on Native CPU and Metal. On both providers, the released decoder
processed fused suffixes after encoder retirement. The physical KV
reservation followed actual Metal buffer aliases until the SDK retired
their backing. The remaining skips comprise 12 CUDA fixtures and the
opt-in Metal pipelined-runtime fixture. These runs cover retirement and
quota lifetimes alongside released T5-small correctness.

On Spark GB10 at `41465abfe9`, the CUDA retirement cohort passed 124 of
140 selected tests. The released merged T5-small decoder processed fused
suffixes after encoder retirement using the same ONNX artifact as the
local runs. Physical CUDA checks also covered bank-free failure, audio
arena output custody and detached NVDEC retirement. The 16 skips were
15 Metal cases and the CUDA-disabled video fixture. The separate Spark
capture cohort at the same revision passed 122 of 173 selected tests.
The packed-row and dequantized vendor recipes passed physical capture,
held output and eviction checks. The public BF16 audio fixture skipped in
the default-library run. It passed on the same Spark executable with the
existing cuDNN 9.19 CUDA 13 runtime, covering learning, cancelled capture,
recapture and replay. The full cuDNN capture rerun passed 123 of 173
selected tests. A subsequent opt-in CUDA check used the pinned LightOn/Qwen
reader packet fixture and verified exact two-page token streams across
capture and replay. With the same fixture and the pinned recurrent
prefill library, standalone CUDA generation matched unmanaged raw control
logits bitwise and verified that admission callbacks ran outside request
gates. The source still learns its audio workspace layout.
Replacing that learned layout and qualifying released-model performance
remain open.

At `552aba3e45`, the focused Spark GB10 cohort passed all 15 capture and
workspace tests with zero skips. The physical tests covered immutable
integer gather, F32 vendor GEMM capture, held outputs across eviction,
and address-bank retirement after a launch error. Capture-initialization
failure and vendor-cache invalidation exercised fallback before submission.
The ingress fixture recorded one capture and three replays, with one
32-byte upload and zero downloads on each replay. Pending scalar observation
covered submit, await and cancel on the physical provider. The workspace
tests covered sealed event drift, publication and disjoint output storage.
The legacy decoder and audio recipe gates remain open.

At `41237da4cc`, both the BERT and stateful decoder fixtures passed shared
preparation and output parity on Native CPU and physical Metal using ONNX,
Safetensors, GGUF and Q8 artifacts. The two CUDA fixtures also passed on
Spark GB10 at `35c208ee25`, completing this cross-provider fixture cohort.

A Spark GB10 loopback HTTP run at the same revision selected CUDA using
the default Inkling checkpoint directory. With execution diagnostics
enabled, the 28-token text prompt returned `4` and the 33-token image
prompt returned `Blue`. The common exporter emitted encoder, prefill
and decode-step reports. This run covers serial HTTP requests.

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

At `644591f1b8`, asynchronous scalar readback requires a backend to
provide both finish and checked drain before submission. CUDA was the
only production provider and already supplied both. The unchecked
cancellation callback and its dispatch wrapper were removed. A failed
CUDA event submission settles the compute stream and, for staged
copies, the auxiliary copy stream before returning the error. The
worker restarts if retirement itself fails. The successful path adds
no synchronization.

Three focused Native tests passed, covering reservation rollback,
retirement and observation across an await failure. A simulated backend
without checked drain declined before submission. On Spark GB10, both
focused CUDA tests ran on the device. The ordinary test observed one
event wait for readback and one for draining an abandoned handle, each
on its own invocation. The failure fixture injected an event-record
error after a real direct or staged copy, checked the required
stream waits, and returned the original scalar on retry.

The public Spark HTTP matrix compared `644591f1b8` with frozen main
`20563ec50a`. It used the shared default checkpoints, the official E4B QAT
GGUF and projector, identical request and media hashes, and two warmups
and ten measurements per arm. Each engine contributed 20 crossover
measurements per workload. All ten output digests matched the frozen
reference and all 120 model-and-arm receipts selected CUDA. The same
184 MiB external GPU context was present before and after the run.

| Workload | Main p50 / p95 (ms) | Candidate p50 / p95 (ms) | Largest same-binary p50 difference |
| --- | ---: | ---: | ---: |
| Whisper | 382.73 / 384.00 | 383.51 / 384.63 | 0.20% |
| EmbeddingGemma2 | 8.42 / 8.52 | 8.70 / 9.22 | 3.27% |
| LightOnOCR | 322.24 / 324.56 | 319.37 / 321.03 | 0.76% |
| GLiNER2.5 | 12.94 / 13.85 | 12.73 / 13.48 | 2.24% |
| DECIDE | 39.98 / 40.85 | 40.06 / 41.26 | 0.46% |
| Qwen3 | 86.59 / 87.59 | 85.90 / 86.71 | 0.89% |
| Gemma 4 E4B text | 222.56 / 223.56 | 223.72 / 225.17 | 0.32% |
| Gemma 4 E4B image | 279.79 / 281.56 | 280.68 / 282.02 | 0.36% |
| Gemma 4 E4B audio | 218.01 / 220.85 | 216.88 / 218.88 | 0.99% |
| Gemma 4 E4B video | 253.81 / 255.73 | 254.42 / 257.67 | 1.34% |

All cases passed the 1.05 p50 and 1.10 p95 ratios, including both
same-binary controls. EmbeddingGemma2 increased 3.36% at p50 and 8.17%
at p95. The observed p50 increase was close to the 3.27% control
difference. Whisper and Gemma 4 text also had small p50 increases.
The cohort establishes output parity and the specified latency gates
on a shared Spark GPU.

The focused repeat used the same binaries, three warmups and 20
measurements per arm. It matched all three prior output digests and
verified all 36 CUDA model-and-arm receipts. EmbeddingGemma2 measured
+1.75% p50 and +2.67% p95, with a 4.60% same-binary p50 difference.
Whisper measured +0.26% p50 and +0.70% p95, with a 0.29% control
difference. Gemma 4 text measured +0.89% p50 and +1.30% p95, with
a 0.07% control difference. All three passed the latency gates. The
Gemma 4 text increase repeated above the measured control variation.

An instrumented Spark crossover used streaming for both revisions so
each emitted paired prefill and decode event reports. All four arms
produced 11 complete reports per phase with no collection failures.
After excluding three warmups per arm, the profiled HTTP p50 was
225.67 ms on main and 227.75 ms on the candidate, with exact stream
output parity. The candidate adds timing scopes for 24 assigned
attention operations per prefill, so the new attention time is a
coverage change and cannot be compared with the baseline's zero.
Other common prefill categories moved by at most 29 µs at their
medians. Learned arena high-water was 11,703,744 bytes on both
revisions, with 1,100 slots on main and 1,099 on the candidate.
These instrumented scopes do not account for the full HTTP increase.
A separate host-stage crossover used uninstrumented CUDA kernels and
the existing generation wall timers. The 24 warm responses per engine
again matched exactly. Median prefill stayed at 92 ms. Median decode
went from 123.0 to 124.5 ms, while total generation went from 215 to
217 ms and HTTP p50 went from 224.30 to 225.83 ms. The whole-millisecond
timers localize the observed difference to the decode interval.

A separate graph trace reproduced exact outputs and the same direction
of latency change. Each main arm began and instantiated 11 decode
graphs across 11 requests, followed by 43 persistent replays. Each
candidate arm began and instantiated 22 graphs and recorded 32
persistent replays. The total number of graph executions stayed at 54
per arm. In the trace, the candidate captured a second slot on the
next token, then alternated persistent replays between the two slots.
This identifies an extra capture per request as a candidate cause
of the small decode increase. The source of the extra slot selection
still needs qualification before changing the replay custody checks.

Two follow-up probes preserved the outputs and still captured 22 graphs
with 32 persistent replays in each arm. One released the owned token
after its embedding gathers. The other preferred a ready graph over an
unused compatible bank and ran with another CUDA context present. The
trace showed that a matching graph had both input banks available and
its output bank held by tensor-only lookahead. The lookahead now releases
its owned CUDA token after both embedding gathers enqueue on the decode
stream. If the tensor route declines after consuming the input, the
already resolved scalar seeds continuation before the step commits.

Across 11 requests per traced arm, the fix restored 11 captures and
43 persistent replays from 22 and 32, with the same 54 graph executions
and exact output parity. The uninstrumented comparison to frozen main
`20563ec50a` used 40 warm samples per engine with same-binary controls.
HTTP p50 was 223.14 → 223.59 ms (+0.20%) and p95 was 225.20 → 224.64 ms
(−0.25%). Outputs matched exactly. All 16 trace and timing arms selected
CUDA. Both control and latency gates passed. The median difference
exceeded the observed 0.11% same-binary spread.

A ten-workload rerun of `4c4c7493bb` against the same frozen main used the
default library path. All 120 model-and-arm receipts selected CUDA.
Outputs matched between the two binaries in every arm and every
crossover case passed both latency ratios. The GLiNER same-binary
median spread was 5.53%, above the 5% control limit. With 40 warm samples
per engine, its isolated repeat measured 12.68 → 12.58 ms at p50 and
13.51 → 13.62 ms at p95, with 1.91% same-binary median spread. Both
control and crossover thresholds passed on the repeat.

The Spark CPU crossover used the same two source revisions with 20
warm observations per binary and mode. Every response matched the
frozen Native output and all 120 model-and-arm receipts selected Native.
Only Gemma 4 audio and video passed both the control and latency gates.
The other crossover results remain provisional because their
same-binary controls exceeded the allowed drift.

| Workload | Candidate p50 change | Candidate p95 change | Controls within limits |
| --- | ---: | ---: | --- |
| Whisper | −1.9% | −36.0% | No |
| EmbeddingGemma2 | −6.5% | −5.6% | No |
| LightOnOCR | −0.3% | +33.6% | No |
| GLiNER2.5 | +14.4% | +17.9% | No |
| DECIDE | +2.7% | +5.4% | No |
| Qwen3 | −4.4% | −6.0% | No |
| Gemma 4 text | −46.6% | −61.9% | No |
| Gemma 4 image | −25.7% | −18.4% | No |
| Gemma 4 audio | −15.3% | −12.5% | Yes |
| Gemma 4 video | −25.5% | −11.1% | Yes |

The isolated GLiNER2.5 and DECIDE repeat used 40 warm observations per
binary and mode. All outputs matched and all 24 receipts selected
Native. DECIDE passed, with p50 594.79 → 610.92 ms and p95
611.90 → 628.19 ms. GLiNER2.5 measured p50 81.84 → 72.35 ms and p95
86.16 → 87.09 ms. Its main control drifted 7.98% at p50, so the
reversal of the initial median increase remains inconclusive.

The isolated LightOnOCR repeat also matched all frozen outputs and all
12 Native backend receipts. Its 40 warm samples per binary and mode
gave p50 11,389.27 → 11,339.78 ms and p95 12,208.70 → 11,876.24 ms.
The main control drifted 25.03% at p50, leaving the latency gate
inconclusive despite the lower candidate medians.

On Spark GB10 at `1a509ebfa1`, seven focused CUDA and workspace
fixtures passed on the default loader path. The BF16 source-audio
fixture skipped there. A repeat with the identical compiled test
binary and pinned cuDNN 9.19 CUDA 13 library passed it without skips,
including learning, cancellation, capture, replay, and held-output
checks. This establishes the observed recipe path's correctness
at that revision. Declared production recipe coverage remains open.

The default library path selected EmbeddingGemma2's F32 audio fallback.
The previous reference used the source BF16 recipe with cuDNN 9.19.
On frozen main, the same text request returned all 768 coordinates
identically under both policies. Its public model identity accounts for
the different digest, and the cuDNN run reproduced the historical digest.
The default-path focused comparison matched the earlier fallback digest
and passed all control and crossover gates for both models. With the
same installed cuDNN 9.19 runtime as the reference, the 40-sample focused
repeat matched both historical output digests and all 24 provider receipts
selected CUDA. EmbeddingGemma2 p50 was 9.00 → 8.88 ms and p95 was
10.18 → 9.56 ms. Its control and crossover gates passed. GLiNER's
crossover passed, while its repeated-main p50 spread was 5.22%, above
the 5% control limit.

The complete matched-cuDNN run of the same binary reproduced all ten
historical output digests. All 120 model-and-arm receipts selected CUDA,
and all crossover latency ratios passed. The initial repeated-main
controls exceeded the limits for GLiNER p50 (6.71%), DECIDE p95
(16.06%), and Qwen3 p95 (11.27%). Each isolated 96-sample-per-engine
repeat matched the historical output, passed the unchanged 1.05 p50
and 1.10 p95 control and crossover ratios, and recorded 12 CUDA
backend receipts. A separate 184 MiB CUDA context was present during
the matrix and repeats.

| Isolated workload | Main p50 / p95 (ms) | Candidate p50 / p95 (ms) | Largest same-binary p50 difference |
| --- | ---: | ---: | ---: |
| GLiNER2.5 | 12.66 / 13.71 | 12.89 / 14.02 | 2.61% |
| DECIDE | 40.04 / 40.85 | 40.13 / 40.96 | 0.41% |
| Qwen3 | 85.69 / 86.70 | 85.23 / 86.45 | 0.56% |

The same Spark binaries also served grouped EmbeddingGemma2 image, audio,
and video requests through the public embedding endpoint. Each crossover
engine contributed 16 measurements per modality, with two warmups per
arm. All embedding output digests matched and all 36 model-and-arm
receipts selected CUDA. Image p50/p95 went from 125.07/125.72 to
124.42/125.73 ms, audio from 26.49/27.76 to 26.40/28.13 ms, and video
from 159.60/160.83 to 158.91/161.36 ms. The three same-binary control
pairs and crossover cases passed the latency ratios. The data cover
these grouped inputs with one item per request on shared Spark GB10.

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
