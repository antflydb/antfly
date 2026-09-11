# GLiNER2.5 API migration and operations

The native implementation is under qualification. The public boundary-model
runtime gate is closed. This document describes the implemented contracts and
the evidence needed to deploy them; it does not authorize enabling the gate or
represent a supported production model matrix. Follow the current
[milestone ledger](GLINER25_IMPLEMENTATION.md) for measured results and failures.

## Select an immutable artifact

GLiNER2.5 is a boundary architecture. Do not overwrite a GLiNER2 model directory,
reuse its split encoder/head bundle, or route a boundary checkpoint through the
legacy span head. Give each source, conversion, or trained export an immutable
version directory and a distinct model identity. Retain the complete tokenizer
and configuration sidecars with the weights.

The [bundle converter](GLINER25_BUNDLES.md) verifies all 334 tensor names/shapes
and publishes into a new directory atomically. Retain its receipt and an
independent expected artifact digest. Receipt and weight hashes must be checked
against the actual files consumed by the runtime. A receipt alone does not prove
quality or publisher authenticity. Never mutate a mapped artifact in place.

The current converter accepts FP32, an FP16 encoder with FP32 task heads, Q8_0,
small Q4_0, and base/multilingual Q4_K. Conversion success is distinct from
qualification. In the locked English CrossNER AI evaluation, all FP16 profiles
and base/multilingual Q8 passed the relative overall/type quality limits; small
Q8 and every Q4 profile failed at least one limit. None of those results covers
the full production task/language matrix. See the exact
[evaluation receipts](GLINER25_EVALUATION.md).

## Move a caller explicitly to schema version 2

The public endpoint is `POST /ai/v1/extract`. The inference service endpoint is
`POST /extract`. Version 1 remains the default when `schema_version` is omitted.
Version 2 adds mixed tasks, per-input replacements, explicit offsets, constrained
decoding, record metadata, JointIE and long-document controls. An advanced field
in a version-1 request is rejected before model execution.

For a future qualified model ID, a version-2 request has this form:

```json
{
  "schema_version": 2,
  "model": "qualified-gliner25-model-id",
  "schema": {"entities": ["person", "organization"]},
  "options": {
    "threshold": 0.5,
    "include_confidence": true,
    "include_spans": true,
    "offset_unit": "utf8_bytes",
    "word_splitter": "whitespace"
  },
  "inputs": [
    {"id": "document-1", "content": "Ada works at Acme."}
  ]
}
```

The Go `ExtractV2`, TypeScript `extractV2`, and Python `extract_v2` helpers supply
version 2. Generated types retain optional-field presence. In Go, use the
dedicated `ExtractionV2Options` when explicit zero/false values or an empty
replacement matter. Do not encode omission as a zero, empty array or null.

Each input's `schema` or `options` replaces that entire shared object. For
example, `options: {}` resets that input to runtime defaults, including the
default whitespace splitter; it does not inherit a shared char splitter.
An omitted input option object inherits the shared object.

Offsets are half-open coordinates in the original input text. UTF-8 bytes are
the default; Unicode codepoint and UTF-16 units are explicit alternatives.
Neither lowercase normalization nor a synthetic terminal period changes the
returned source coordinate system. Content-part text is joined with newlines;
consumers must interpret offsets against that joined text.

The `char` splitter preserves ASCII alphanumeric and `@._-+` runs and splits
other non-whitespace codepoints. It is useful for scripts without spaces but
changes proposal boundaries, token counts and model behavior. Choose the
splitter before evaluation and keep it fixed in artifact/request receipts.
It is not equivalent to splitting every UTF-8 byte or every displayed grapheme.

Read optional spans and `entity_index` by presence: an absent endpoint reference
is different from entity zero. Keep scalar/list structure values, record anchors,
source-free enum values, derived relations and solver diagnostics in their
typed forms. Do not flatten all task results into entity strings.

## Validate enrichment responses before publication

Single-item and batch enrichment use the same bounded canonical-envelope
validation. A response must identify the requested model and contain exactly
the expected number of object rows. Returned IDs must match the wire request;
the current single-item bridge sends an anonymous input, so its row omits
`id` even when the durable enrichment item has a local ID. Present `id: null`
is invalid. The local durable item ID is not an implicit wire ID.

The bridge then validates generated typed fields, finite numeric values and
the declared offset/value contracts. Malformed rows, duplicate properties,
wrong models, empty or extra rows and invalid typed values fail before
enrichment publication. New GLiNER2.5 fields and unknown extensions survive
validation in the original bounded JSON representation; legacy optional
fields and opaque structure values retain their existing contract. Consumers
must still retain the model, schema, input and destination versions needed
for a later migration or backfill.

Regression coverage includes single/batch identity, extensions, malformed
values, legacy omission, bounded responses and allocation-failure ownership.
Repeated and empty string IDs remain valid where sent by a caller. These
contracts do not establish live service or reindex/backfill qualification.

## Bound decoding and long documents

The default long-document policy rejects an oversized input. Explicit windowing
uses overlapping source-word windows and a document-level merge, followed by
global constraint validation. Classification aggregates owned-word-weighted raw
logits. Natural records retain source-anchor identity; latent and anchorless
record identity follows the requested occurrence/semantic policy.

Candidate/proposal caps are part of the model semantics. Host/device allocation,
text, sequence, window, output and solver-work limits are separate safety
contracts. A rejected input must not be converted into a successful empty result
or silently truncated to fit. Preserve the original schema and thresholds when
retrying on a larger admitted worker.

Exact search distinguishes optimality, infeasibility and exhaustion. A feasible
beam result does not establish global optimality. Strict requests reject search
exhaustion; explicit best effort accepts only a validated witness and retains
its exhaustion/status metadata. See
[long-document semantics](GLINER25_LONG_DOCUMENT.md).

Requests are atomic: an input failure publishes no partial `data` array. Retain
the structured error code, input index and stage. A declared memory-budget
denial is HTTP 507 with `MEMORY_BUDGET_EXCEEDED`; transient admission pressure
remains distinct from that fixed-cap rejection. Raising a retry count cannot
make an unchanged fixed-cap request fit. Genuine backing-allocation failures
remain internal failures.

## HTTP and lifecycle regression coverage

Model-free tests exercise schema rejection, response parsing, atomic batch
failure, cancellation, bounded metrics and retry. Model-gated tests additionally
exercise the pinned-small handler and generated HTTP/1.1 route on CPU and
Metal, compare exact selections/source coordinates at the fixed `5e-4`
confidence bound, and rehash the source files before and after execution.

The public availability gate stays closed. A default-false, test-only per-Node
field admits these cases; it has no production environment or request override.
A missing model or backend produces an explicit skip, not qualification.

Regression assertions include second-item failures without partial output,
anonymous same-cache retry, competing-request admission with one learned
forward at a time, transport disconnect/shutdown cancellation, and cleanup of
request, listener, cache, residency and watchdog owners. Metrics/listing
snapshot release must preserve the exact last-use timestamp so observation
cannot keep idle models alive indefinitely. Ordinary inference handles still
refresh usage.

Private-output cleanup masks task-local cooperative I/O cancellation only
while removing owned unpublished files. The process watchdog remains armed,
and already published artifacts are preserved. These tests do not establish
concurrent learned forwards, long-document HTTP, non-loopback deployment or
production release qualification.

## Bound model and session teardown

Process-required sessions now own dormant teardown tickets created before
backend entry. Model destruction starts protection before cached executors and
component sessions close; nested cleanup cannot extend the original deadline.
The independent monitor and retained driver IO can outlive the manager when a
raw session escapes. The production close deadline is 30 seconds. Expiration
terminates the supervised worker with exit 86, leaving process restart to its
supervisor. Admission remains held until physical destruction returns.

Model-free child regressions block physical close and cache destruction,
including polling/final-release paths with stderr locked. Expiration must exit
86 without synchronous logging, retain any admission lease until destruction,
and complete outer ownership cleanup. An escaped raw session has no admission
lease; its ticket and driver IO must still outlive the original manager.

Actual-model TTL tests cover held handles, expiry, fresh reload, same-cache
retry, metrics observation without usage refresh, and return of caches,
leases, tickets and transient allocations to their empty baseline. Memory
admission failures remain failures and cannot count as successful TTL tests.
The 30-second production close bound is unchanged. Synthetic short-deadline
child tests do not simulate a real driver hang or measure production latency.

Run logs and intermediate failed checkpoints are external artifacts, separate
from reusable regression sources. Public availability and the exact-artifact
production policy remain closed pending release qualification.

## Inspect extraction lifecycle metrics

The inference Prometheus renderer includes fixed-cardinality extraction
instrumentation. Recording uses scalar events and atomic counters without
allocating label values. Labels contain only declared transport, outcome,
lifecycle-stage and solver enums; they never contain model names, request IDs,
text, schemas, entity labels or record values. The optional executor observer
does not alter request options, returned JSON or inference fingerprints.

All names below have the `antfly_inference_extract_v2_` prefix unless a complete
name is shown:

| Metric suffix | Interpretation |
| --- | --- |
| `requests_total{transport}`, `active`, `outcomes_total{outcome}` | Recognized V2 calls through HTTP or embedded dispatch, starting before execution-control and admission checks. Outcome distinguishes validation, unsupported features, admission pressure, explicit resource/memory ceilings, backing OOM, cancellation, timeout, search exhaustion, infeasibility, model failure and internal failure. |
| `duration_ns` | Histogram from recognized V2 dispatch through owned request cleanup. It excludes HTTP body collection and the version probe, and stops before HTTP response publication. |
| `phase_visits_total{stage}`, `phase_duration_ns_total{stage}`, `failures_total{stage}` | Admission, parsing, preflight, model preparation, tokenizing, execution, windowing, merging, serialization and teardown. Failed-phase time includes error cleanup; successful calls record a separate teardown phase. CPU and Metal use the same combined execution boundary. |
| `parsed_items_total`, `input_bytes_total` | Items and joined source-text bytes in successfully parsed envelopes. |
| `decoded_items_total`, `decoded_prompt_tokens_total`, `decoded_output_values_total` | Completed item work, including items in a request that later fails. Prompt tokens include overlapping windows; output values use the executor's bounded scalar-value count. |
| `returned_items_total` | Items returned by successful atomic dispatches. A later item or serialization failure leaves this count unchanged for the whole request. This is not a client-delivery acknowledgment. |
| `documents_planned_total`, `windows_planned_total`, `windows_completed_total` | Source-word plans before encoded-token admission, and windows whose learned output/evidence was retained. A rejected document can have planned or completed windows without a returned item. |
| `solver_results_total{solver,status}`, `solver_exhausted_total{solver}`, `solver_nodes_total{solver}` | Classification, JointIE and record diagnostics from decoded witnesses. Strict search failures appear in request outcomes; these witness counters do not claim complete search-work accounting for failed solvers. |
| `host_peak_bytes_max` | Maximum observed allocation peak of the capped request-owned host heap. It excludes model residency, device allocations, transport buffers and caller-owned response/typed-request serialization buffers. It is not RSS or a device-memory measurement. |
| `antfly_inference_extract_envelope_failures_total{outcome}` | Bounded envelope/version probe failures before a trustworthy V2 dispatch. This separate family can include version-1 requests. |

A forced worker exit cannot finish an in-process trace or preserve its
in-memory counters. Correlate parent supervision and worker-exit evidence with
these metrics; missing completions must not be interpreted as successful
requests. Process restarts reset counters. Use the deployment's existing
process/resource metrics for RSS, device allocation and hard-kill accounting.
Observability regressions cover rendering, dispatch, cancellation/admission
and scalar/vector histogram boundaries: `le` buckets include values equal to
the declared upper bound. The optional
[dashboard, recording rules and alert runbook](GLINER25_MONITORING.md) now have
offline native Prometheus validation and a pinned temporary-tool CI bootstrap.
They keep missing data, worker identity, strict rejection and decoded witnesses
distinct. No deployment or paging receiver is configured; live scrape/Grafana
behavior, threshold calibration and concurrent service qualification remain.

## Training and recovery

[Version-1 training jobs](GLINER25_TRAINING_JOB.md) accept original FP32 source
weights and `full`, `heads`, `lora` or `dora`. Quantized training is rejected.
CPU and resident Metal have separate execution/resource contracts. Successful
tiny GPU and real-small CPU tests do not qualify all published GPU training
profiles, adapter targets, ranks or convergence.

The supervised command owns one disposable worker and a new output directory.
SIGINT/SIGTERM requests a checkpoint at the next safe boundary. A second signal,
hard deadline or parent loss can terminate work before a new checkpoint is
published. Failures never restart the command automatically. Recover from the
last successfully published checkpoint, not the last progress line.

Resume into a new output directory and pin the actual 32-byte canonical state
digest from the saved result. Keep model, tokenizer, ordered data, schema,
training schedules and adapter configuration unchanged. Operational memory
limits may be adjusted explicitly, with admission still enforced. Retain both
the original and resumed receipts. Training success, portable export loading,
same-trained-artifact output parity and held-out quality improvement are four
different acceptance checks.

## Production promotion and rollback

Before deployment, establish a reviewed support matrix for exact model bytes,
backend, precision, task, language and length. Every row needs the agreed
quality, correctness, end-to-end median/p95 latency, resource and platform
evidence. Missing or skipped hardware tests are not passing rows. The current
implementation ledger contains no complete production promotion receipt.

Use a separate result field or versioned destination for an initial shadow
evaluation. Preserve the existing model/configuration and compare outputs
without overwriting production enrichment. Record artifact and schema hashes,
input identity/version, splitter, offset unit and decoder policy with each run.
Match the exact input set when comparing quality and include failures in its
denominator. Do not tune thresholds on that held-out comparison.

After qualification, use the deployment system's existing traffic and quota
controls for a bounded canary. Observe errors by code/stage, admission denials,
deadline and worker exits, queue time, end-to-end latency, host/device memory,
solver exhaustion and long-document window counts. Exercise cancellation and
model eviction under concurrent load before widening traffic. These are
required deployment checks. Optional dashboard/rule import artifacts are
provided; a live canary, notification route and monitoring deployment still
require their own execution receipts.

Backfills need an explicit concurrency limit, resumable input cursor and a
destination version. Publish results only for the input version actually
processed. Keep errors and retry state separate from extraction facts. Reserve
capacity for foreground requests and pause the backfill when its capacity or
error budget is exhausted. There is no automatic GLiNER2-to-GLiNER2.5 reindex or
background backfill in this implementation.

To roll back, stop new canary/backfill admission, drain or cancel its work using
the existing runtime controls, restore the previous model/configuration and
read the previous result version. Retain the failed artifact, receipts and
error samples for diagnosis. Do not mutate an active model, delete its files
before draining, or merge failed partial results into the older destination.
