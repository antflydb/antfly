# GLiNER2.5 Native Inference and Training

## Context

GLiNER2.5 introduced a boundary-based extraction architecture that cannot use
the existing GLiNER2 span-grid head. This work added native Zig CPU and Metal
execution for the small, base, and multilingual checkpoints, extended the
extraction API, and implemented training and artifact interoperability.
Native inference and training do not require Python; pinned Fastino/PyTorch
code supplies independent reference results and compatibility checks.

This completed work log records the implementation. Public model promotion
remains separate: the production entries in
[`gliner_boundary_qualification.zig`](../../zig/pkg/inference/src/models/gliner_boundary_qualification.zig)
are empty. Recognizing a model, converting its weights, or passing a test-only
request does not enable it for production callers.

## Inference

- Added strict boundary architecture/configuration detection and the complete
  334-tensor inventory for each published variant. Boundary checkpoints retain
  their own identity and cannot fall through to the legacy GLiNER2 head.
- Implemented DeBERTa encoding, boundary proposals, shared document candidates,
  explicit-span scoring, and learned classification, relation, and record heads.
- Added entities and attributes, ordinary/constrained/ordinal classification,
  legacy structures, natural/latent/anchorless records, enum fields, directional
  relations, overlap policies, and deterministic assignment and deduplication.
- Implemented JointIE candidate rescue and graph selection, including endpoint
  identity, inverse/symmetric relations, cardinality, exclusivity, and bounded
  exact/beam search. Infeasibility, exhausted search, and a valid best-effort
  witness remain distinct results.
- Matched tokenizer normalization, word splitting, case handling, schema
  markers, enum prefixes, and source offsets against pinned references.

The core paths are the
[CPU engine](../../zig/pkg/inference/src/architectures/gliner_boundary_engine.zig),
[Metal engine](../../zig/pkg/inference/src/architectures/gliner_boundary_engine_device.zig),
[task pipeline](../../zig/pkg/inference/src/pipelines/gliner_boundary_pipeline.zig),
and [schema compiler](../../zig/pkg/inference/src/pipelines/extraction_schema.zig).

### API and lifecycle

Schema version 2 extends `POST /ai/v1/extract` and the inference service's
`POST /extract` with mixed tasks, per-input schema/options replacement,
confidence and span controls, record metadata, JointIE, and long-document
options. Version 1 remains the default when `schema_version` is omitted;
advanced fields are rejected in a version-1 request.

Go `ExtractV2`, TypeScript `extractV2`, and Python `extract_v2` helpers preserve
optional-field presence. An input's schema or options replaces the entire
shared object; `{}` resets options to defaults. Omitted, null, false, zero,
and empty arrays are not interchangeable. Offsets are half-open coordinates
in the original text, with explicit UTF-8 byte, Unicode codepoint, and UTF-16
code-unit modes. The authoritative wire contract is
[`extraction.yaml`](../../specs/openapi/ai/extraction.yaml).

The executor owns parsed inputs and responses, propagates cancellation and
deadlines, bounds admission and output allocation, and publishes batch results
atomically. Server tests cover invalid requests, unsupported capabilities,
queued cancellation, retry, socket transport, cache/session lifetime, and
cleanup after allocation failures. Shared model teardown waits for borrowers;
cooperative cleanup and supervised worker termination have separate ownership.

### Long documents and validators

The [long-document planner](../../zig/pkg/inference/src/pipelines/gliner_boundary_long_document.zig)
and [executor](../../zig/pkg/inference/src/extractors/gliner_boundary_long_executor.zig)
implement explicit windowing and global merging of scores, records, and
relations. The default length policy rejects oversized inputs. Windowed mode
preserves original text and global offsets, bounds overlap and aggregate work,
and applies constraints to merged results. Each window still passes tokenizer
admission. This versioned Antfly aggregation policy is a declared extension;
it is not a claim of exact upstream long-document behavior.

The [regex executor](../../zig/pkg/inference/src/pipelines/extraction_regex.zig)
implements bounded boolean full/partial matching against original UTF-8 values,
with Python 3.12/Unicode 15 character and case semantics. Unsupported syntax
fails before inference; compile/match work and request-local caches are bounded.
Enum validators apply to canonical choices before assignment and fallback.
Required fields with no valid choice fail explicitly. Parser property tests
cover malformed inputs, owned lifetimes, Unicode offsets, limits, and solver
witnesses without loading a model.

## Artifacts and precision

The [bundle implementation](../../zig/pkg/inference/src/models/gliner_boundary_bundle.zig)
supports a complete single-file GGUF plus the original four configuration and
tokenizer sidecars. It preserves original tensor names rather than using the
legacy encoder/head split format.

| Weight profile | Supported conversion variants |
| --- | --- |
| FP32, FP16 encoder, Q8_0 | Small, base, multilingual |
| Q4_0 | Small |
| Q4_K | Base, multilingual |

Reduced storage applies only to declared encoder linear matrices and word
embeddings. Task heads, biases, normalization, and relative-position tables
remain FP32. `strict_f32_activations_v1` independently fixes activation
arithmetic; storage precision does not imply activation quantization.

Conversion validates the full inventory and publishes into a new directory
with an exclusive atomic rename. Receipts bind source/output hashes and sizes;
loaders verify the actual opened weights and sidecar bytes they consume.
Actively mapped artifacts must remain immutable. Receipts establish integrity,
not publisher authenticity or numerical qualification. The
[bundle checker guide](../../zig/pkg/inference/scripts/gliner25/BUNDLE_CHECK.md)
defines same-bundle CPU/Metal comparisons separately from quality relative to
the original FP32 model.

## Training and export

Native training implements full, head-only, LoRA, and DoRA modes on CPU and
resident Metal, using the existing graph/autodiff and optimizer infrastructure.
The [training job](../../zig/pkg/inference/src/finetune/gliner_boundary_training_job.zig)
owns source admission, dataset preflight, optimization, checkpoint/resume, and
final export through the managed CLI or embedded interfaces.

- Versioned JSONL carries complete per-example schemas and explicit entity,
  classification, relation, and record annotations. Unknown labels, malformed
  offsets, and unrepresentable targets fail preflight. Schemas are never
  inferred from positive labels; missing supervision and explicit negatives
  remain distinct. The [dataset parser](../../zig/pkg/inference/src/finetune/gliner_boundary_dataset.zig)
  owns bounded snapshots and rejects overlapping declared splits.
- Losses and gradients cover boundary, classification, relation, and record
  objectives, with detached proposal selection, gold injection, hard negatives,
  and matching. Native tests compare independent Torch losses and gradients.
- Optimizer groups preserve the source learning-rate and weight-decay rules.
  Absent gradients remain distinct from explicit zeros, including optional-head
  touches and wholly inactive fallback. Accumulation includes partial-window
  flushes; failed computation does not advance the run.
- Checkpoints bind source/sidecar bytes, dataset/schema identity, parameter
  layout, schedules, random streams, moments, gradient presence, and counters.
  Restore validates staged state before publication. Same-backend interrupted
  and uninterrupted runs must agree exactly.
- Host, backend, combined admission, replay work, and checkpoint transactions
  have explicit limits. Quantized training is unsupported. Held-out datasets
  receive preflight only; the training job performs no automatic calibration,
  model selection, or quality evaluation.

### Activation recomputation

`attention_profile` defaults to `materialized_v1`; `replay_tiled_v1` selects
the dedicated tiled attention primitive. Independently, `activation_profile`
defaults to `retained_v1`; `layer_recompute_v1` rebuilds one encoder region at
a time during backward. Both profiles enter durable run identity.

Regional replay preserves parameter identities and accumulates shared gradients
while running task-head decisions once. Dropout sites retain sealed mask
bindings across forward and replay. Native counter RNG supports deterministic
resume; it does not reproduce PyTorch's RNG. Explicit shared-mask fixtures
provide the separate source comparison. See the
[attention oracle](../../zig/pkg/inference/scripts/gliner25/TRAINING_ATTENTION_ORACLE.md)
and [composed-step oracle](../../zig/pkg/inference/scripts/gliner25/TRAIN_STEP_ORACLE.md).

### Export and adapter materialization

Full/head exports contain all 334 FP32 source tensors and four sidecars.
LoRA/DoRA exports retain exact selected targets, A/B tensors, magnitudes where
applicable, and source/run identity. The
[materialization job](../../zig/pkg/inference/src/finetune/gliner_boundary_merge_job.zig)
merges adapters into a new complete FP32 model with bounded scratch, immutable
input snapshots, supervised cancellation, and exclusive atomic publication.

The pinned PEFT 0.17.1 loader incorrectly rewrites the `inside_weight` module
name during adapter loading. A separately pinned `peft-0.18.0-export-v1` profile
uses the released corrected loader in a verified temporary overlay; original
training fixtures and environments retain their own identities. No targets
are omitted to make a reload pass. Reproduction details remain in the
[export checker](../../zig/pkg/inference/scripts/gliner25/TRAINING_EXPORT_CHECK.md),
[three-form merge checker](../../zig/pkg/inference/scripts/gliner25/TRAINING_MERGE_CHECK.md),
and [trained-execution checker](../../zig/pkg/inference/scripts/gliner25/TRAINED_EXECUTION_CHECK.md).
Reload, materialized execution, source-update parity, and useful trained quality
are separate checks.

## Validation and performance

The [oracle manifest](../../zig/pkg/inference/scripts/gliner25/oracle_manifest.json)
pins Fastino commit `3c913c7369301133d3b7699252074c4303ada50e`, model revisions,
and dependencies. [Regression fixtures](../../zig/pkg/inference/testdata/gliner25/README.md)
retain independent expected values for preprocessing, task heads, decoding,
gradients, optimizer updates, dropout, and resume. Fixture consolidation shares
identical metadata and tensors without changing distinct cases or tolerances.
Run logs, binaries, model copies, and historical campaign receipts stay outside
the source tree.

The final fixture-consolidation pass before this documentation move passed
143 Python checks, 34 CPU tests, and 20 actual-device Metal tests with no selected
skips, plus 11 pinned source replays. The native runs logged a support-test
memory-estimate diagnostic before completing successfully. These are scoped
local results, not a claim that all CI or release gates passed.

Performance work includes bounded CPU attention and exact GELU, resident Metal
weights/projections, compact embedding uploads, FP32 execution, and bounded
workspaces. Shared changes also improve legacy GLiNER2 inference and preserve
per-sample contextual labels. The [CPU benchmark](../../zig/pkg/inference/scripts/gliner25/BENCHMARK.md)
and [Metal comparison contract](../../zig/pkg/inference/scripts/gliner25/METAL_BENCHMARK_V2.md)
record workload and timing boundaries. Historical speedups apply to their
measured artifacts, shapes, and builds; later correctness checks do not refresh
latency measurements or establish service-tail performance.

### Held-out evaluation

CrossNER AI preparation locks the full 14-type ontology, reconstructs explicit
BIO-token text, audits split overlap, and separates blinded requests from gold.
The [evaluation contract](../../zig/pkg/inference/scripts/gliner25/evaluation_contract.py)
and [official BIO metric audit](../../zig/pkg/inference/scripts/gliner25/audit_crossner_bio_scoring.py)
retain errors in the denominator and distinguish exact-fact and BIO metrics.
The small Q4_0 result exceeded the declared one-point overall F1-loss limit;
conversion success must not erase that qualification failure.

MASSIVE 1.1 preparation covers multilingual slots and ordinary/constrained
classification with frozen training-derived schemas, source-text offsets,
translation/duplicate-family split audits, and original attribution. The
[execution contract](../../zig/pkg/inference/scripts/gliner25/massive11_execution.json)
pins ten profiles and three blinded shards per profile. The runner verifies
complete ordered coverage of all 2,974 requests before publishing metrics.
Preparation and runner implementation alone establish no model-quality result.

### Reproduction entry points

From the repository root, verify checked-in source and fixture identities:

```sh
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-fixtures
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-references
```

Source regeneration requires the exact checkout, model artifacts, and
[pinned environment](../../zig/pkg/inference/scripts/gliner25/requirements.txt).
Generators and checkers expose their inputs through `--help`; the adjacent
oracle/checker guides describe the individual proof boundaries. Run native
tests through the build graph from `zig/`, with heavy model/device work serial:

```sh
zig build inference-test -Dmetal=false -Dcuda=false -j1 -- --test-filter 'gliner boundary' --test-filter 'boundary processor'
zig build inference-test-gliner25-fuzz -Dmetal=false -Dcuda=false -Donnx=false -Dpjrt=false -Dsystem-blas=false -j1
```

Real-model tests require the explicitly selected `ANTFLY_GLINER25_*_MODEL_DIR`
fixtures; Metal tests require an available device and Metal-enabled build.
Missing artifacts or devices are explicit skips, not correctness evidence.

## Operations and monitoring

V2 dispatch instrumentation records fixed-enum outcomes, phases, admission,
solver work, and request-owned memory without request-derived labels. The
inference metrics route is `/ml/v1/metrics`. Dispatch timing excludes HTTP body
collection and response publication; decoded items can belong to a later
failed atomic request, and returned items are not client acknowledgments.
The request-heap high-water gauge is not process RSS or device/model residency.

Optional dashboard/rule examples use `gliner25_monitor="enabled"` and retain
`job`/`instance` identity. Rates precede aggregation; missing telemetry remains
missing. They do not configure a running scrape, alert receiver, or paging.
The [monitoring contracts](../../zig/pkg/inference/scripts/gliner25/test_monitoring.py)
check selectors, units, bounds, and alert scenarios; the
[bootstrap](../../zig/pkg/inference/scripts/gliner25/bootstrap_monitoring.py)
verifies a pinned promtool in temporary storage for native rule evaluation.
Offline checks do not prove live scraping, dashboard rendering, or delivery.

### GLiNER25ScrapeUnavailable

Check the opted-in worker's listener, network, TLS/auth, and parent/worker
lifecycle. Missing completions after termination are not successful requests.

### GLiNER25MetricsMissing

Confirm `/ml/v1/metrics`, the actual binary, and V2 metric presence. A healthy
legacy endpoint may lack these metrics; do not replace missing series with zero.

### GLiNER25ServerFailureRatio

Inspect backing OOM, model/internal failures, artifact identity, and worker
logs. Pause a canary or backfill when its reviewed error budget is exceeded.

### GLiNER25AdmissionPressure

Reduce offered concurrency and check capacity leases. Retry only transient
admission failures under the caller's bounded policy.

### GLiNER25FixedLimitRejections

Identify the failing input, output, window, or memory ceiling. An unchanged
request cannot fit through retry alone; adjust admitted capacity explicitly.

### GLiNER25DispatchLatency

Check phase timing, cold loads, and window counts against the workload and
client-observed SLO. Dispatch latency excludes network publication.

### GLiNER25StrictSearchExhaustion

Review declared search capacity and constraints. Never silently drop constraints
or switch to best effort; preserve witness validity and exhaustion metadata.

## Remaining qualification

Public promotion requires exact artifact/backend/precision/profile entries,
representative task/language quality, and supported geometry/resource bounds.
The declared performance targets are median end-to-end latency no slower than
the pinned reference and p95 at most 1.1 times it. Reduced-weight quality-loss
targets are at most 0.5 percentage points for FP16/Q8 and one point for Q4 by
task and declared slice. These are acceptance criteria, not achieved claims.

Published-small training checks establish bounded authored-job restart/export
consistency. They do not establish published-model PyTorch gradient equality,
cross-backend update equality, full-context training, convergence, or the full
backbone/rank/target matrix. Published regional Metal remains unqualified.
Broader service concurrency, live monitoring, canary/rollback, and final-tree
CI evidence remain deployment gates. This implementation record does not
change those gates.
