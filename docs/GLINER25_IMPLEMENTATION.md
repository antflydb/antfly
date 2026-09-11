# GLiNER2.5 implementation and release gates

This tracks the native implementation against the full approved scope. It is
not a production qualification receipt. The public runtime and legacy GGUF
exporter reject boundary models until their respective integrations are ready;
they must never dispatch GLiNER2.5 through the GLiNER2 span head.

## Architecture and compatibility contract

The discriminator is `architecture: boundary`, architecture version 1, config
version 3. Published base, small, and multilingual configurations are parsed
strictly. The encoder is DeBERTa; the extraction head operates on boundaries
and a shared document candidate pool. Classification, explicit-span attributes
and enums, sparse relations, and record assignment have distinct learned
scorers. A single entity-head approximation cannot implement these tasks.

The reference source, dependencies, model revisions and artifact hashes are
pinned in [GLINER25_ORACLE.md](GLINER25_ORACLE.md). Native inference has no Python
runtime dependency. The Python environment exists only to produce and verify
differential evidence.

The native schema compiler accepts a versioned canonical schema and produces
an owned immutable intermediate representation with a deterministic fingerprint.
The legacy translator is explicit. Unknown fields, ambiguous routes, injected
schema markers, invalid dimensions and unsupported semantic switches fail with
typed errors. Regex validators require an installed executor and fail closed
when one is unavailable.

Tokenization must match the published normalizer, Unicode word splitting,
lowercasing, enum prefixes, special-marker placement and fragment boundaries.
Original source coordinates survive normalization; UTF-8 bytes are the default
offset unit, with explicit Unicode codepoint and UTF-16 conversions available.
`max_len` counts body words, including a synthetic terminal period, before enum
prefixes. Encoded schema tokens and total routed words have separate budgets.

## Implemented native building blocks

| Component | Implementation | Evidence |
| --- | --- | --- |
| Model configuration and detection | `models/gliner_boundary.zig`, manifest/registry/capability guards | All three published configurations; malformed, conflicting and unsupported config tests |
| Artifact schema and precision policy | `models/gliner_boundary_artifact.zig` | 334 exact tensor shapes per variant; complete small checkpoint header cross-check, strict metadata rejection and protected task heads |
| Tokenization and schema routing | `lib/tokenizer`, `gliner_boundary_processor.zig`, `extraction_schema.zig` | Full tokenizer suite; exact published-small token IDs and structural routes on five preprocessing cases |
| Boundary proposal and scoring | `gliner_boundary_ops.zig`, `gliner_boundary_head.zig` | Complete tiny FP32 head and explicit scorer; published-small head against captured encoder states for eight requests |
| Primitive gradients | Centered inside prefix, local attention and range pooling VJPs | Oracle prefix VJP plus finite-difference attention and range checks |
| Learned task heads | `gliner_boundary_tasks.zig` | Independent classification, directional/biaffine relation and all three record-mode forward references |
| Entity overlap and offsets | `gliner_boundary_decode.zig` | 24 overlap-policy references, deterministic ties, Unicode offsets and allocation failures |
| Record assignment | `extraction_assignment.zig`, `gliner_boundary_records.zig` | 128 pinned SciPy assignments and 18 natural/latent/anchorless decodes |
| Ordinary relation proposals and deduplication | `gliner_boundary_relations.zig` | Ragged proposal masks/caps plus 39 contained-span, repeated-mention, token-subset and Unicode cases |
| Classification constraints | `extraction_constraints.zig` | Exhaustive small-space checks, ordinal predicates, exact/beam termination, cancellation and strict infeasibility |
| JointIE graph solver | `extraction_joint_ie.zig` | Exhaustive graph selection, shared endpoint rescue cost, symmetry/inverse edges, slots, degrees, cycles, overlap and bounded-search tests |
| Encoder state routing | `gliner_boundary_engine.zig` | Published-small FP32 encoder/routing parity on three requests, including forced tiled attention and cancellation probes; admission and allocation-failure tests |
| Bounded CPU encoder attention | `ops/deberta_tiled_attention.zig` | Tiled BLAS against materialized/portable references, ragged/masked inputs, explicit workspace limits and cancellation inside tiles |
| Complete CPU task orchestration | `gliner_boundary_pipeline.zig`, neural JointIE adapter | All 30 end-to-end requests across pinned small/base/multilingual FP32 checkpoints; exact selections and source spans, confidence tolerance 5e-4; heterogeneous-batch ownership, strict/best-effort, cancellation and allocation-failure checks |

The primitive tests do not qualify complete training gradients. Head tests using
captured encoder states do not qualify end-to-end native inference. The small
smoke corpus does not establish task quality, multilingual quality, long-context
behavior, throughput, p95 latency or Metal performance.

## Latest local checkpoints

The public boundary-runtime gate remains closed and the production qualification
table is empty. These additive receipts have distinct scopes; they do not turn
an earlier failed aggregate into a pass or qualify the full release matrix.

| Checkpoint | Completed scope and remaining limit |
| --- | --- |
| Published-small regional training | All six native LoRA/DoRA uninterrupted, pause and fresh-resume phases pass using replay-tiled attention, regional activation recomputation and all 131 Linear targets. Each mode completes five microbatches/three updates with byte-identical final checkpoint/export files. The [published campaign](GLINER25_RECOMPUTED_TRAINING.md#published-small-native-campaign) records the exact binary, bounded profiles and failures; published regional Metal, full-context numerics and convergence remain open. |
| Learned multiwindow execution | All four focused tests pass, including actual published-small CPU and Metal task/retry cases. The containing final-focused v2 run remains failed: 67 selected, 65 passed, one child-fixture skip, one TTL failure and zero leaks. Runtime control/OOM, resident-program, qualification and tiny regional full/head tests passed within that scope. |
| Fatal watchdog progress | Nine fresh child probes pass against the frozen v2 executable with exit 86 and no outer kill. Both held-stderr polling and final-release cases retain admission until termination. These synthetic callbacks do not qualify a real driver hang or a production-timeout measurement. |
| Observation and idle TTL | The corrected narrow run passes 11/11 selected tests with no skips/failures/leaks: three snapshot tests, retirement, six listing cases and actual published-small Metal retention/eviction/reload. Metrics/listing observation preserves idle timestamps; ordinary inference still refreshes use. The same-executable first attempt was denied before inference by the unchanged live-memory guard and remains a failed receipt. |

The additive [qualification/lifecycle ledger](../zig/pkg/inference/testdata/gliner25/qualification_lifecycle_execution_v1/ledger.json) preserves exact source,
executable, supervision and failure receipts. See the
[operations checkpoint](GLINER25_OPERATIONS.md#latest-local-service-checkpoint)
for the ownership changes and current actual-TTL status.

## Milestones and remaining release gates

| Milestone | Required completion evidence |
| --- | --- |
| M0: reference contract | Immutable source/dependency/artifact pins, full task fixtures, deterministic reproduction and reviewed fixture digests. Initial references are present; expand adversarial and held-out coverage. |
| M1: artifacts and processor | Strict tensor inventory and shapes, versioned dense/quantized bundles, atomic conversion, runtime content rehashing, exact preprocessing for every variant. Converter and actual CPU loader are implemented; all nine reduced bundles pass integrity checks and execute ten diagnostic requests each. The managed Metal loader also passes actual converted-small FP32 wire parity. All 100 strict same-bundle CPU/Metal diagnostic comparisons pass across the nine reduced bundles plus small FP32; full quality qualification remains. |
| M2: complete native CPU inference | All three published checkpoints pass the initial ten-task reference corpus through native preprocessing, encoder, all learned heads and decoding. Expand adversarial/held-out coverage and verify the admitted public service path; these smoke cases alone do not establish production quality. |
| M3: Metal and long documents | Resident FP32 encoder and all learned heads pass all thirty initial small/base/multilingual reference requests. Strict reduced-weight primitives pass F16/Q8_0/Q4_0/Q4_K embedding and linear checks at one, three and 129 rows after removing hidden half-operand dispatch from this profile. Managed Metal execution passes actual converted-small FP32 wire parity, ten one-window tasks and overlapping-window batch recovery. Global merges preserve latent multiplicity, legacy required fields and whole-record alternatives; synthetic cross-window tests include Unicode, constraints, cancellation, allocation failures and strict search exhaustion. Device admission, model locking and watchdog ownership are integrated with the public gate closed. No Metal or long-document release receipt exists yet. |
| M4: serving and clients | A versioned raw-JSON adapter, HTTP/embedded dispatch, bounded reclaiming request heap, shared resource admission and managed backend are implemented with the public capability gate closed. Generated Go/Python/TypeScript and Zig contracts preserve presence, per-input replacement, offsets, long-document metadata and solver errors. Latest focused SDK checks pass: seven Go top-level tests, 43 Python tests, 41 TypeScript tests and SDK typechecking. Single and batch enrichment now share bounded canonical-response and typed-value validation, require the expected model/cardinality/identity, and preserve raw extensions; seven focused asset tests pass without skips or leaks, and the extracting library suite passes. Actual native session/wire parity and extraction transport tests pass. The pinned-small FP32 HTTP handler now passes managed loading, all-five-file identity checks, the unchanged 128 MiB request cap, exact fixture outputs, atomic second-item 413 failure/retry, cached-model reuse, metrics and transient-resource cleanup through a test-only per-Node gate. The independent 16 MiB Node exposed redundant tokenizer-vocabulary parsing and a misclassified budget denial; lightweight architecture preflight and terminal allocator attribution fix both without raising caps. All eight V2 tests pass in the latest scoped receipt (combined run: 21 selected, 17 passed, four expected Metal skips, zero failures or leaks); the two unrelated inactive-training failures from the preceding checkpoint are resolved. Real pending-Io private cleanup and published-output preservation tests also pass in their separately scoped checkpoint. Metal admission reserves simultaneous encoder/head owner caps and serializes the model runtime through teardown. Supervised actual-model executor tests pass. Fixed-enum lifecycle, rejection, solver and long-document Prometheus hooks pass five observability tests plus renderer and V2 dispatch/cancellation checks in a 25-selected-test CPU checkpoint with no skips or leaks. Shared histogram exact-boundary regressions also pass. The optional [monitoring artifacts](GLINER25_MONITORING.md) provide a dashboard, 13 recording rules and seven warning/info alerts; all 17 offline monitoring/bootstrap tests pass with pinned official promtool, including native PromQL and alert fixtures. Actual HTTP/1.1 delivery now passes through a caller-owned ephemeral loopback listener: the pinned-small CPU generated route covers default 400, exact success, atomic second-item 413 and retry, with real metrics scraping. A separate model-free transport case passes TCP-reset cancellation, same-listener recovery, graceful-shutdown cancellation and joined resource cleanup. Both CPU/transport cases pass in a 17-selected-test Metal-enabled checkpoint with zero skips or leaks. A separate actual managed-Metal HTTP case now passes the same fixture, five pins, confidence tolerance, default rejection, atomic failure/retry, metrics and physical device-allocation cleanup under an explicit 1/4/5 GiB host/backend/combined profile with 3 GiB scratch; it preserves the executor caps and live-memory guard. Its combined checkpoint has five selected, four passed, one concurrency-driver failure, zero skips or leaks; that historical aggregate remains failed. The corrected concurrency test then passes separately: one selected, one passed, zero skips/failures/leaks, proving a real cold CPU request holds one Node slot, a competing HTTP client receives exact 503 admission metadata, and release/retry/metrics/resource cleanup succeed. A [versioned service ledger](../zig/pkg/inference/testdata/gliner25/service_qualification_v1/manifest.json) preserves both scopes and the resolved test failures. A separate actual managed-Metal queued-request cancellation test passes one selected test with no skips or leaks: socket reset releases admitted host/device scratch and watchdog state before the held execution mutex is unlocked, followed by exact same-session retry, metrics, physical cleanup and five-file rehash. Its source and successful receipt are added to that ledger, with the preceding unrelated interpreter compile failure retained. The historical [manager-teardown ledger](../zig/pkg/inference/testdata/gliner25/service_teardown_execution_v1/manifest.json) preserves its CPU and seven-child proof. Later nine-child, multiwindow and observation-TTL results are tracked in [latest local checkpoints](#latest-local-checkpoints). Simultaneous learned forwards, cancellation inside a learned forward or kernel, broader cache/soak service qualification, live monitoring delivery and backfill execution remain. See [caller and enrichment contracts](GLINER25_OPERATIONS.md). |
| M5: inference qualification | All three original FP32 models match Python on the locked 431-example CrossNER AI test split in native CPU and Metal. The Metal extension verifies 1,293 model/example pairs against both Python and historical CPU outputs: 2,586 exact token/decision comparisons, unchanged confidence tolerance and zero errors; all overall/type metrics agree. Historical CPU `0069cdad...` and Metal `a43fcdee...` executables differ. Separate fresh small/base/multi CPU captures now match saved Metal outputs on the same `a43fcdee...` executable for 1,293/1,293 requests, with exact tokens/decisions and all 15 metric families unchanged. The additive [FP32 CPU/Metal ledger](../zig/pkg/inference/testdata/gliner25/crossner_fp32_metal_execution_v1.json) preserves source, build, raw-report, helper and audit identities separately from the historical ledger. All nine reduced CPU profiles also completed without errors. All FP16 profiles and base/multi Q8 satisfy this corpus's relative overall/type loss limits; small Q8 and every Q4 profile fail at least one declared gate. The official BIO audit passes the declared exact-span metric across 15 historical reports, 6,465 scored document instances and 225 metric families. That CrossNER evidence covers short English entity extraction. The small model also completes the fixed 60-label English MASSIVE intent profile on Python, CPU and Metal: 2,974/2,974 exact token sequences and selected labels, zero errors, maximum CPU/Metal selected-confidence difference 6.7652e-6 at the unchanged 5e-4 bound. All 61 metric families agree (54.9092% accuracy); the full probability vector is not exposed and is not qualified. The additive [complete intent ledger](../zig/pkg/inference/testdata/gliner25/massive_intent_small_execution_v2.json) preserves all three aggregates and nine shards; no held-out tuning or absolute quality-floor claim follows. The separate English entity profile retains two source-coordinate errors as documented in [MASSIVE evidence](GLINER25_MASSIVE_EXECUTION.md). A model-free [bounded parser/property target](GLINER25_FUZZING.md) covers malformed input, schema replacement/presence, solver limits, Unicode window ownership and allocation-failure recovery; all five tests pass. Its finite native mutation campaign completes 1,133 executions with 89 unique executions and 3,469/18,231 instrumented PCs covered (19.03%), without a failing input. The binary, cache and provenance are preserved; this is harness coverage, not model-feature coverage. Scoped Zig 0.16 runner/link workarounds are documented. Reduced-precision Metal held-out quality, broader tasks/languages, performance, broader long-context/concurrency/cancellation/eviction/soak qualification and platform CI remain open. See [evaluation evidence](GLINER25_EVALUATION.md). |
| M6: training | Tiny CPU full/head/LoRA/DoRA steps match pinned losses, gradient presence/values and two AdamW updates with zero and controlled dropout, including durable resume. All eight tiny Metal profile/dropout combinations now pass losses/VJPs at unchanged tolerances using exact semantic relation membership, source mask-row transport and exact backend-local controlled-dropout replay. The separate GPU optimizer fixture matches Torch updates; managed tiny full/head-only jobs pass resident updates, cancellation/OOM retry and exact fresh-owner resume. The earlier hardware checkpoint passed 15/15 selected tests with no skips or leaks, including restored partial-flush admission and large-budget arithmetic. These Metal step fixtures use captured post-update inputs; that managed proof covers tiny full/head-only jobs; the later inactive-adapter source proof below additionally covers tiny managed LoRA/DoRA updates, and separate published classifier-only Metal jobs below prove durable continuity. Immutable five-file Source ownership, native/resident orchestration, versioned job/CLI, checkpoints and portable full/head/PEFT export are implemented. All three FP32 sources pass the 334-tensor/tokenizer checks. The real-small head-only library job and persistent CPU CLI produce identical state/model hashes; pinned Fastino verifies all 334 export tensors and runs ten requests. A fresh supervised CPU heads CLI also passes pause-after-one/resume, four total microbatches/two updates, identical final hashes and complete redirected stdout under explicit 256 MiB host/128 MiB backend limits. Eight process tests and three focused CLI tests prove the separate worker/argv/config/streaming boundaries. Separate published-small CPU LoRA/DoRA jobs now pass four microbatches/two updates and exact pause-after-one/fresh resume for all 59 task-head Linear targets. Their 118/177-tensor standard adapters load byte-exactly over all 334 source tensors using an isolated official PEFT 0.18.0 export profile and execute ten requests each; the original PEFT 0.17.1 inside_weight loader failure remains documented. Separate public CPU jobs now prove the same four-microbatch/two-update and exact fresh-resume behavior for all 131 encoder-plus-head Linear targets, with 262/393 LoRA/DoRA tensors; both exports pass the same strict 334-source-tensor PEFT load and ten requests. Explicit host/backend ceilings were 128/384 MiB for LoRA and 128/512 MiB for DoRA, with other owners separately admitted and smaller backend profiles rejected. Actual native materialization and the fixed-tolerance independent merge checker now pass for both all-target adapters: all 334 tensors, zero violations across 131 adapted matrices, exact untouched/bias/sidecar bytes, and ten matching requests in all three comparisons of unmerged PEFT, official PEFT merge and the Python-loaded native-produced file. The separate trained-artifact worker now passes both all-131-target small merged FP32 files on Zig CPU and Metal: 40 requests, 120 comparisons against the three Python forms and 20 CPU–Metal comparisons, exact tokens/decisions/source spans, unchanged confidence bounds and four clean private-copy lifecycles. This proves artifact execution. The inactive-adapter correction separately passes all eight tiny source LoRA/DoRA profiles through actual CPU and Metal NativeTrainer: exact token/routing tensors and gradient None/zero, fallback objective, component losses and VJPs, native AdamW states/counters and exact fresh-owner durable resume. The source H16 fixture injects only captured initial A/B/magnitude values before microbatch zero through a compile-time test-only constructor; later updates are native and production inventory checks remain unchanged. Its normal five-row epoch flushes at [2,4,5]; the immutable [2,3,5] control fixture has a separate passing optimizer-only consumer. The subsequent hardware checkpoint passes 17/17 selected tests with zero skips or leaks, including both actual source consumers, separate control consumers, managed classifier regressions, no-gradient tapes and sockets. The earlier CPU/socket MIME failure is retained as a failed checkpoint. Source Metal uses unchanged numerical tolerances and explicit bounded diagnostic gradient readbacks; updates remain resident. Separate ordinary production CPU CLI jobs now pass the published-small classifier-only active/inactive five-row sequence for rank-2 LoRA/DoRA: six clean uninterrupted/pause/resume invocations, five microbatches/three updates, exact zero-loss fallback and raw-term preservation, every selected slot at three Adam steps, independently reconstructed Controller/state hashes, and byte-identical final results/checkpoints/all four export files. The frozen ReleaseFast binary is 9f0d349e…786f9; the [compact CPU ledger](../zig/pkg/inference/testdata/gliner25/published_inactive_classifier_cpu_v1/manifest.json) retains all six receipts, both checker revisions, original resolved checker-only failure, build inventory and final artifact pins. Enforced trainer caps remain 128 MiB host/128 MiB backend/1 GiB combined with other owners separately admitted, and all six outer process trees finish below the 2 GiB sampled RSS guard. Six separate resident Metal invocations of that same frozen executable now also pass the published-small classifier-only LoRA/DoRA recipe: five microbatches/three updates, exact inactive fallback with raw terms, every slot at three Adam steps, and byte-identical final results/checkpoints/all four exports after fresh-owner resume. Both final and all four continuation receipts independently reconstruct exactly, including Controller/state hashes and clean process ownership. The [separate Metal ledger](../zig/pkg/inference/testdata/gliner25/published_inactive_classifier_metal_v1/manifest.json) preserves 86 files under an explicit 128 MiB host/1 GiB backend/2 GiB combined profile and unchanged live-memory guard. Resident admitted upper bounds are 775,275,748/775,343,792 bytes for LoRA/DoRA, with distinct sampled RSS maxima 581,730,304/661,929,984 bytes below the 3 GiB outer guard. These runs retain materialized attention and establish same-backend policy and durable continuity. All four final classifier-only CPU/Metal LoRA/DoRA artifacts now also pass the unchanged upstream CPU export checker with isolated official PEFT 0.18.0: every 334 base and four/six adapter tensor byte, the completed checkpoint and resumed export identity, and ten fixed requests each. All four bounded processes and private-copy lifecycles finish cleanly; the [additive reload ledger](../zig/pkg/inference/testdata/gliner25/published_inactive_classifier_export_reload_v1/manifest.json) preserves reports, portable adapters and helper pins. Reload execution does not imply training-update equality between backends. The later completed published-small regional native campaign is summarized in [latest local checkpoints](#latest-local-checkpoints). Other inactive target/backbone cases, published-source and CPU–Metal numerical VJPs, broader published-model GPU jobs, full-context replay-tiled training, representative held-out improvement, convergence, performance and GA gates remain open. Quantized training is excluded. See [training evidence](GLINER25_TRAINING.md), [data](GLINER25_TRAINING_DATA.md), [job/CLI contracts](GLINER25_TRAINING_JOB.md) and [adapter export evidence](GLINER25_TRAINING_EXPORT.md). |
| M7: rollout | Artifact, evaluation and training contract tests are wired into the existing amd64 and arm64 Zig CI jobs through `scripts/run_model_contract_tests.py gliner25`. The archived attention checkpoint passes all 203 Python contract tests with zero skips in 3.879 seconds, using the unchanged pinned oracle interpreter and official promtool 3.14.0. The preceding interpreter-selection failure (201 discovered, 25 import/dependency errors from missing psutil/packaging) is preserved alongside the passing receipt in the [attention execution ledger](../zig/pkg/inference/testdata/gliner25/training_attention_execution_v1/manifest.json); no dependencies were changed. This is local contract validation, not model execution or remote CI. Both CI steps now acquire a checksum-pinned official promtool in a bounded private temporary owner and require the native monitoring checks; acquisition failure fails the step. The 17 focused monitoring/bootstrap tests pass locally, while remote CI and hardware qualification remain separate. The [migration and operations guide](GLINER25_OPERATIONS.md) and [optional monitoring runbook](GLINER25_MONITORING.md) document immutable artifacts, V2 contracts, bounded errors, recovery, scrape identity and calibrated alert prerequisites. Live canary/rollback/backfill receipts, scrape/dashboard/notification delivery and a qualified support matrix remain. |

## Resource and decoding invariants

- Physical weights belong to the model resource manager; backend residency and
  scratch belong to the backend runtime; every request has explicit admission
  and execution control. New allocations must be included in these existing
  ownership boundaries before serving integration is enabled.
- Candidate and pair caps are model proposal semantics. Separate safety limits
  reject excess requests rather than silently truncating their results.
- Exact/beam solvers distinguish feasible witnesses from proven optimality and
  infeasibility. Node-budget exhaustion remains explicit even when a witness
  exists. Strict serving rejects exhaustion; best effort requires explicit opt-in.
- Ordinary relation output collapses semantic repeated mentions according to
  the reference. JointIE preserves mention identities and validates its global
  graph. The ordinary relation deduplicator must not replace the graph solver.
- Natural record anchors retain identity. Latent/anchorless records deduplicate
  only after assignment. Exclusive scalar fields use global assignment;
  deterministic SciPy tie behavior is part of the pinned compatibility profile.
- Long-document chunks cannot be combined through independent output union:
  offsets, duplicate ownership, record identity and global constraints require
  a document-level merge and validation stage.

## Precision and performance policy

Initial CPU qualification uses FP32. Initial Metal qualification uses an FP16
encoder and FP32 extraction/task heads, with a separate FP32 diagnostic path.
Quantized profiles are Q8_0 for all variants, Q4_K for base/multilingual and Q4_0
for small. Heads, norms, learned boundaries and assignment parameters require
an explicit tensor policy; generic matrix quantization is insufficient.
The initial versioned tensor policy protects encoder biases, normalization and
relative-position tables in FP32 as well. It casts or quantizes only declared
encoder linear matrices and word embeddings. Metadata validation does not
establish numerical qualification for any reduced-precision profile.

Promotion targets are median end-to-end latency no slower than the pinned
Fastino implementation and p95 no more than 1.1 times the reference. FP16/Q8
quality loss must be at most 0.5 percentage points and Q4 at most 1 point, by
task and declared slice. These are gates, not current measured claims.

Every comparison records exact model and tokenizer hashes, tokens, schema,
precision, backend, build, hardware, warmups, batch/length, timing boundary and
resource conditions. Interleave runs and report quality alongside latency,
throughput and peak memory. Promotion receipts must rehash the runtime files;
receipt metadata alone cannot establish artifact identity.

The 2026-09-09 direct-core CPU FP32 run passed output/token parity for all thirty
variant/task cases, then measured twenty balanced pairs after three warmups
with one math thread and a fully ReleaseFast native graph. Paired median
native/Python latency ratios ranged from 0.529–0.791 (small), 0.583–0.801 (base),
and 0.562–0.798 (multilingual); every per-case bootstrap upper bound was below
one. These are short curated requests from one host/session, excluding the
service path. They do not complete M5. The full task table, artifact/report
hashes, RSS observations and limitations are recorded in
[`BENCHMARK.md`](../zig/pkg/inference/scripts/gliner25/BENCHMARK.md).

The locked CrossNER run is kept separate from calibration. Its full fixed
14-type ontology is supplied to every request, including absent types; errors
and oversized requests stay in the denominator. Reduced-weight quality loss
is distinct from same-artifact CPU/Metal arithmetic parity. In particular,
the small Q4_0 bundle loses 2.104 F1 percentage points overall on this corpus,
exceeding the agreed one-point limit. Thresholds and schemas must not be tuned
on this locked test to remove that failure.

Training run fingerprints bind model and sidecar bytes, ordered dataset and
schema digests, mode, PEFT configuration, schedules and random-stream protocol.
Native epoch ordering derives from durable optimizer counters, including an
unfinished final accumulation window, and has checkpoint/resume coverage.
This deterministic native protocol does not reproduce PyTorch's random-number
stream. Source optimizer compatibility also preserves its substring-based
`encoder` learning-rate groups, which include some extraction-head modules.

## Focused local verification

From `zig/`, using the repository build graph:

```sh
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build lib-tokenizer-test -Dmetal=false -Dcuda=false -j1
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build lib-ml-test -Dmetal=false -Dcuda=false -j1
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build inference-test -Dmetal=false -Dcuda=false -j1 -- --test-filter 'gliner boundary' --test-filter 'boundary processor' --test-filter 'extraction schema' --test-filter 'constraint' --test-filter 'joint '
ANTFLY_GLINER25_SMALL_MODEL_DIR=/private/tmp/antfly-gliner25-models/small ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build inference-test -Dmetal=false -Dcuda=false -j1 -- --test-filter 'pinned small checkpoint'
ANTFLY_GLINER25_SMALL_MODEL_DIR=/private/tmp/antfly-gliner25-models/small ANTFLY_GLINER25_BASE_MODEL_DIR=/private/tmp/antfly-gliner25-models/base ANTFLY_GLINER25_MULTI_MODEL_DIR=/private/tmp/antfly-gliner25-models/multi ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-gliner25-zig-cache zig build inference-test -Dmetal=false -Dcuda=false -j1 -- --test-filter 'checkpoint all inference tasks'
```

Without the environment variable, real-model tests explicitly skip; a skipped
model test is not release evidence. Run heavy model/Metal checks serially on
the constrained development host. Hardware performance qualification requires
the intended backend to be available and independently identified.
