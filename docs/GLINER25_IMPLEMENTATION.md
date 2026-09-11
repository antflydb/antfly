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

## Support and remaining release gates

Implementation coverage and release qualification are separate. The public
boundary-runtime gate remains closed and the production qualification table
is empty. Test-only admission does not make a model available to callers.

| Area | Implemented scope | Remaining qualification |
| --- | --- | --- |
| Artifacts | Strict original FP32 inventory, atomic conversion, declared reduced-weight policies and identity checks. | Promote only exact tested artifacts; generic GGUF conversion is not a substitute. |
| Native CPU | Full boundary encoder, task heads and decoders; pinned token/output fixtures across small, base and multilingual models. | Broader task/language and full-context quality. |
| Metal | Strict FP32 execution, model-owned weights, bounded request/workspace owners, cancellation and teardown. | Exact-artifact production policy and broader workload/hardware coverage. |
| API and lifecycle | V2 schema, owned atomic responses, limits, retry, metrics, snapshot-safe TTL and process-required close protection. | Deployment, concurrent learned forwards, long-document HTTP and soak qualification. |
| Evaluation | Fixed CrossNER and MASSIVE schemas, explicit error denominators, CPU/Metal comparisons and bounded benchmark tools. | Absolute quality floors, additional task corpora, reduced-precision Metal and representative performance. |
| Training/export | Full/head/LoRA/DoRA objectives, CPU/resident-Metal updates, deterministic resume, PEFT reload and atomic materialization. | Published-source gradient parity, the broader backbone/rank/target matrix, full-context training and convergence. |
| Rollout | Contract tests, optional monitoring assets and operational recovery contracts. | Remote CI, canary/rollback/backfill evidence and a qualified support matrix. |

Reusable tests and reference fixtures are retained. Run logs, binaries, source
snapshots and per-attempt campaign receipts are external artifacts; they do not
belong in the regression fixture tree. See the
[fixture policy](../zig/pkg/inference/testdata/gliner25/README.md),
[evaluation contract](GLINER25_EVALUATION.md),
[training contract](GLINER25_TRAINING.md), and
[operations guide](GLINER25_OPERATIONS.md).

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

The current optimized Metal milestone compares original FP32 models with
Fastino/PyTorch MPS; native CPU preservation is a separate FP32 comparison.
Strict Metal execution uses FP32 activations, accumulation and task heads.
The benchmark admits only FP32 weights and forbids silent host fallback or
reduced precision. Its [v2 protocol and frozen-build results](../zig/pkg/inference/scripts/gliner25/METAL_BENCHMARK_RESULTS_V2.md)
do not qualify a later build or open the public runtime gate.

Separately admitted weight-storage formats are FP16 encoder weights, Q8_0 for
all variants, Q4_K for base/multilingual and Q4_0 for small. The versioned tensor
policy casts or quantizes only declared encoder linear matrices and word
embeddings. Task heads, biases, normalization and learned relative-position
tables remain FP32; activation precision is a separate execution contract.
Metadata admission alone establishes neither same-artifact numerical parity
nor model quality for a reduced-weight format.

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
service path. They do not establish representative performance or release
readiness. The full task table, artifact/report
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
zig build lib-tokenizer-test -Dmetal=false -Dcuda=false -j1
zig build lib-ml-test -Dmetal=false -Dcuda=false -j1
zig build inference-test -Dmetal=false -Dcuda=false -j1 -- --test-filter 'gliner boundary' --test-filter 'boundary processor' --test-filter 'extraction schema' --test-filter 'constraint' --test-filter 'joint '
ANTFLY_GLINER25_SMALL_MODEL_DIR=/private/tmp/antfly-gliner25-models/small zig build inference-test -Dmetal=false -Dcuda=false -j1 -- --test-filter 'pinned small checkpoint'
ANTFLY_GLINER25_SMALL_MODEL_DIR=/private/tmp/antfly-gliner25-models/small ANTFLY_GLINER25_BASE_MODEL_DIR=/private/tmp/antfly-gliner25-models/base ANTFLY_GLINER25_MULTI_MODEL_DIR=/private/tmp/antfly-gliner25-models/multi zig build inference-test -Dmetal=false -Dcuda=false -j1 -- --test-filter 'checkpoint all inference tasks'
```

Without the environment variable, real-model tests explicitly skip; a skipped
model test is not release evidence. Run heavy model/Metal checks serially on
the constrained development host. Hardware performance qualification requires
the intended backend to be available and independently identified.
