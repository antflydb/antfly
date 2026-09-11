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

The extracting library suite passes. Seven focused asset tests pass with no
skips or leaks, including single/batch identity, extensions, malformed values,
legacy omission and allocation-failure ownership. The receipt is
`/private/tmp/gliner25-enrichment-typed-v2.log`, SHA-256
`fcf07ca834a820defd14afad0012085884a56d51350ad0f4eaff1c241ee82204`.
Separate SDK checks pass for seven Go top-level tests, 43 Python tests,
41 TypeScript tests and TypeScript typechecking. These include exact
version/model/count/object/positional-ID contracts and bounded responses;
repeated and empty string IDs remain valid where sent by a caller. These
focused checks do not establish live service or reindex/backfill qualification.

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

## Measured HTTP handler and loopback qualification

The pinned original small FP32 model now passes the real `Node.extractJSON`
handler path with managed loading, the Node-owned watchdog, shared admission,
owned HTTP response bytes and lifecycle metrics. The test supplies
`ANTFLY_GLINER25_SMALL_MODEL_DIR`, verifies the existing pipeline fixture's five
artifact hashes before and after execution, and compares those hashes with the
managed session's immutable identity. It checks four entities and one
classification from the first mixed-task fixture, including exact selections
and source offsets and the unchanged confidence tolerance of `5e-4`.

This test crosses the public capability gate through a default-false field on
one test Node only. That field has type `void` in production builds; there is no
environment, configuration or request override. The primary Node and an
independent Node both reject the model by default, and `runtime_available`
remains false. The original test qualifies the handler directly. Separate
loopback tests now qualify actual HTTP/1.1 delivery on CPU and managed Metal as
described below, including competing-request admission. These do not prove
simultaneous learned forwards, long-document HTTP handling, non-loopback
deployment or production release qualification.

The primary Node retains a 128 MiB request-scratch ceiling. After a successful
request, a two-item request decodes its first item and rejects the second item's
4,097-word text with HTTP 413, `EXTRACTION_LIMIT_EXCEEDED`, input index 1 and
stage `tokenizing`. No partial `data` or usage escapes. An anonymous retry
succeeds with the same cached model and fixture outputs. The primary Node's
four calls produce two successes, one unsupported-model rejection and one
resource-limit rejection; counters report five parsed, three decoded and two
returned items. Active request slots, model handles, scratch/KV reservations
and watchdog leases return to zero. The cached model's residency remains
admitted and unchanged.

The initial test exposed a separate preflight defect in the independent Node's
16 MiB request heap: full manifest parsing materialized the tokenizer vocabulary
before the capability gate and reported its declared allocation denial as HTTP
500 `OutOfMemory`. The primary 128 MiB Node's default-gate check had already
passed. Preflight now uses the existing lightweight manifest reader, which
fully validates the boundary discriminator/configuration while leaving
tokenizer materialization to the admitted managed loader. Both original caps
remain unchanged. A model-free regression supplies a vocabulary larger than
the whole request heap, then an oversized valid configuration: the former
reaches the default gate, the latter returns model-stage HTTP 507, and restoring
the configuration recovers. Terminal allocator-failure attribution covers the
bounded scope; failed resize/remap attempts do not turn a later genuine backing
OOM into a fixed-budget rejection. The managed model owner and caller-owned
response allocation keep their independent failure attribution. A post-control
check discards completed JSON if cancellation or a deadline wins during inner
model/backend teardown.

A separate caller-owned `httpx.Server.ListenerTask` now passes actual HTTP/1.1
requests on `127.0.0.1` with an OS-assigned ephemeral port. It registers the
Node's generated `/ai/v1/extract` route and `/ml/v1/metrics`, and uses independent
server/client executors with a 16 MiB transport allocation ceiling, 64 KiB
request limit, 1 MiB response limit, two connection/request lanes, explicit
connect/read/write/request deadlines, and no client retries or redirects. The
pinned-small model executes on CPU under the same unchanged 128 MiB request
scratch cap. Default rejection, exact fixture success, atomic second-item 413,
anonymous retry, cached-model reuse, all-five-file identity, decoded-versus-
returned counters and the delivered metrics body all pass. The listener joins
before routes, Node or executor ownership can be destroyed.

A separate actual managed-Metal socket case now passes the same first fixture,
five source-file hashes, confidence tolerance, default 400, atomic second-item
413, anonymous retry and delivered metrics. Both session managers require
Metal explicitly on this test Node. Its declared profile is 1 GiB host, 4 GiB
backend, 5 GiB combined and 3 GiB scratch; the normal live-physical-memory guard
still applies. This differs from the CPU fixture's 128 MiB scratch profile.
The executor's unchanged 2 GiB encoder and 256 MiB head device ceilings combine
with its 512 MiB request heap for 2,816 MiB of admitted transient scratch.
Model/tokenizer residency remains separately admitted. A model-free budget
regression proves that 128 MiB rejects this reservation; it does not claim an
actual 128 MiB Metal HTTP run.

The Metal case checks the loaded session's real backend, FP32 identity,
process-required interruption, serialized model mutex and ready strict-device
compute backend. Physical MetalTensor allocation/release counters advance
across each decoded request, without a diagnostic upload or a counter reset.
Request cleanup returns host scratch, KV, active handles, watchdog entries and
model execution ownership; retained model scratch is reconciled against the
actual model lease. The full admitted residency stays unchanged through the
atomic failure and retry. After Node destruction, physical owned-tensor and
host-mirror live bytes return to their initial values and created/released
device bytes balance. This is resident execution evidence, not a claim that
request-owned FP32 weights remain resident after request teardown.

The accompanying model-free transport test owns a separate cancellation
barrier route. A real TCP reset makes the transport signal cancellation; the
Node observes that signal and rejects before parsing, admission or loading.
The generated extraction route remains usable afterward. A second connected
barrier request is cancelled when the caller's 25 ms graceful shutdown period
ends; joining drains handlers, sockets, request-body capacity, cancellation
observers and listener leases. This tests the ingress cancellation boundary,
not interruption in the middle of a learned encoder operation.

A separate actual pinned-small CPU concurrency test now passes through two
independent HTTP clients. The test owner holds the existing model-registry
lock until the first cold request owns the single Node slot and its real
128 MiB request-scratch lease. The competing request receives HTTP 503 with
the exact inference-admission reason and retry metadata. Releasing the lock
lets the original request succeed; an anonymous retry reuses the same admitted
cached model with unchanged fixture outputs. Delivered metrics reconcile two
successes and one admission rejection. The transport reaches two active
requests while the Node peaks at one, and all tasks, sockets, request leases,
handles and watchdog entries drain. This qualifies competing-request
admission with one learned forward at a time.

The concurrency test's first run exposed a test race with lazy resource-owner
initialization; the shared helper now mirrors `Node.serve` by initializing that
owner before listener publication. Its next run rejected the test's extra
outer driver with `ConcurrencyUnavailable`: the client already uses four lanes
for request/watchdog and connect/watchdog. The corrected driver has one
separate executor lane within the existing 4 MiB test metadata owner. Client
and production capacities remain unchanged. The targeted correction passes
one selected test with no skips, failures or leaks; the earlier aborted and
failed receipts remain historical failures.

The first socket attempt reached an installed Zig 0.16 POSIX connect-timeout
TODO and aborted; the test now uses a bounded `Io.Select` race, joins its timer
and closes a connection that completes after the deadline. No installed
toolchain code changed. The next run completed the model requests but both
socket tests expected an incorrect metrics MIME. The established handler's
final `ctx.text()` emits `text/plain; charset=utf-8`; only the test expectation
changed. Both corrections pass in the latest socket receipt below.

An additional actual managed-Metal case now qualifies cancellation while a
request waits for the loaded model's existing execution mutex. After a real
HTTP warmup, the test retains that model handle and locks the mutex. A second
raw TCP request acquires one Node slot, 512 MiB of host scratch and 2,304 MiB of
device scratch under the unchanged Metal profile. Resetting its owned socket
must release those request leases and watchdog state within five seconds while
the mutex remains locked. The retained model lease remains live. Unlocking
then permits an exact-fixture retry on the same cached model/session; metrics,
physical device cleanup and all five source pins are checked. This case uses
the existing cancellation-aware lock, with no production hook, artificial
reservation or mid-kernel interruption claim. It passes one selected test with
zero skips, failures or leaks.

The compact [service qualification ledger](../zig/pkg/inference/testdata/gliner25/service_qualification_v1/manifest.json)
preserves the five concurrency/Metal/cancellation checkpoint logs, source-fixture and
test-source hashes, exact five model pins, commands, build/backend/resource
profiles and result scopes. The successful concurrency and queued-cancellation
logs omit exact test-executable paths; those identities remain unrecorded, and the
command comes from the root execution transcript. The ledger does not change
upstream numerical fixtures or public availability. Evidence remains scoped
to the individual receipts:

| Receipt | Result and limits |
| --- | --- |
| `/private/tmp/gliner25-socket-queued-metal-v2.log`, SHA-256 `dfbc576d1d6fc7cde50fe5631e445e73386ff155c0bd072c53ea494d9e89c496` | Actual queued managed-Metal cancellation exits zero: one selected, one passed, zero skips/failures/leaks. Host/device request leases and watchdog state drain before the execution mutex is unlocked; same-session retry, exact outputs, metrics and source/device cleanup pass. |
| `/private/tmp/gliner25-socket-queued-metal-v1.log`, SHA-256 `d33e79ec9e6fd0865b0fb0e67cd2ed3625ab2cf7edb89c5ad1223f1055b6bcf3` | Earlier compilation stopped at an unrelated interpreter switch after two new training-attention tags were added. The socket test did not run; the dedicated hook/interpreter integration resolves this in v2. |
| `/private/tmp/gliner25-socket-concurrency-metalbuild-v3.log`, SHA-256 `11db69c03acb9ce6e34ecce5a9958bcfe567d983f6c8f338b26619b15e062ba3` | Targeted corrected concurrency test exits zero: one selected, one passed, zero skips/failures/leaks. The real pinned-small CPU request/rejection/release/retry and metric/resource contract passes. This is separate from v2's Metal success. |
| `/private/tmp/gliner25-socket-all-metalbuild-v2.log`, SHA-256 `e3328a86b1455be4003074efa4dc9542afd5ab38b7c234ec17ca477538213508` | Actual managed-Metal HTTP, its model-free scratch-budget regression, and both earlier socket cases pass. Five selected, four passed, zero skips, one concurrency-driver failure, zero leaks. The command exits nonzero; v3 resolves its concurrency failure without turning this historical aggregate into a pass. |
| `/private/tmp/gliner25-socket-concurrency-metalbuild-v1.log`, SHA-256 `37ec45b73bb1180c936cffb4d7fe2c6f8172973e2732f97504775b5e72fa4b19` | Historical ABRT from the test's premature lazy-domain unwrap. No aggregate or leak result is claimed. Serving-owner initialization was corrected before v2; v3 completes the concurrency contract. |
| `/private/tmp/gliner25-source-socket-metal-v1.log`, SHA-256 `7960df3ce368de2937e397b819f4e5f239845287fa8b422ff7b3b8c3cbf8481b` | Both corrected socket tests pass. The combined Metal-enabled run has 17 selected, 17 passed, zero skips/failures/leaks; other selected cases qualify source/training behavior separately. The socket fixture itself forces native CPU inference. |
| `/private/tmp/gliner25-source-socket-cpu-v3.log`, SHA-256 `2bb388fc18c3954a98b740a44c7f69dc738f40c5a22f53d32d2a6edcd7eb9bc3` | Historical MIME-only socket failures: eight selected, four passed, two expected Metal skips, two socket failures, zero leaks. Both socket extraction paths had completed before the incorrect MIME assertion. |
| `/private/tmp/gliner25-source-socket-cpu-v2.log`, SHA-256 `ee39edc9f34079051f26b0fb7643319bab7c8851ad45dfaf48841cd3d334f630` | Historical aborted socket attempt at Zig 0.16 native connect timeout; no successful aggregate or leak result is claimed for this run. |
| `/private/tmp/gliner25-service-inactive-cpu-v4.log`, SHA-256 `e1fa91f1a99b7b7d005e68699c25ea4cf3196efe9c97441adbafa4359b879184` | Latest focused run exits zero: 21 selected, 17 passed, four expected Metal skips, zero failures or leaks. All eight V2 tests and all four adapter-file regressions pass, along with the corrected inactive/no-gradient training tests. |
| `/private/tmp/gliner25-service-inactive-cpu-v3.log`, SHA-256 `9020d6ac8fdacc2ce83887107d482a95cae2e6d4d84c84226af48f01eddddb92` | Earlier checkpoint: all eight selected V2 tests and all four adapter-file regressions pass. The combined run has 21 selected, 15 passed, four Metal skips, two unrelated newly added inactive-training failures, and zero leaks. Those two failures are resolved in v4; v3 itself was not a passing aggregate suite. |
| `/private/tmp/gliner25-service-cleanup-v1.log`, SHA-256 `76492a1386739e85b34f28287bd3f4329f0da119828d2e7529848ea7db3a22a4` | Common private-publication cleanup and actual merge cleanup both pass with pending Io cancellation, alongside selected export/job/merge regressions. The combined run has 14 selected, 12 passed, one optional published-export skip, the initial secondary-Node service failure described above, and zero leaks. |

Private cleanup tests re-arm real Io cancellation before unwinding with a
distinct execution-control error. They verify removal of the owned unpublished
tree/file, restoration of the prior cancellation state, and preservation of
the published payload. The cleanup helpers block only task-local cooperative
Io cancellation; the caller's process watchdog remains armed through teardown.

## Bound model and session teardown

Process-required sessions now own dormant teardown tickets created before
backend entry. Model destruction starts protection before cached executors and
component sessions close; nested cleanup cannot extend the original deadline.
The independent monitor and retained driver IO can outlive the manager when a
raw session escapes. The production close deadline is 30 seconds. Expiration
terminates the supervised worker with exit 86, leaving process restart to its
supervisor. Admission remains held until physical destruction returns.

The separate [teardown ledger](../zig/pkg/inference/testdata/gliner25/service_teardown_execution_v1/manifest.json)
preserves three CPU attempts, exact helpers, the observed executable identity
and three frozen source files. Its final CPU run passes nine tests with one
expected process-fixture skip. Seven fresh model-free children then all exit 86
through the watchdog, with no outer kill and complete reaping. They exercise
cache destruction, TTL/admission eviction, retired-handle release, shutdown,
load rollback and an escaped raw session. The first six prove a 64-byte lease
remains held during blocked destruction; the escaped case explicitly has no
lease. Earlier optional macOS argv-observation failure and six fixture compile
errors remain recorded as failures, with successful owned-process cleanup.

These child fixtures block synthetic callbacks and use a private 100 ms test
deadline, bounded externally at five seconds per child. They qualify ownership
and watchdog behavior, not a real driver hang or a measured production
30-second timeout. The later nine-child proof and actual-model TTL status are
tracked in the [latest service checkpoint](#latest-local-service-checkpoint);
the historical source snapshot is not itself execution evidence. Public
availability remains closed.

## Latest local service checkpoint

The [additive qualification/lifecycle ledger](../zig/pkg/inference/testdata/gliner25/qualification_lifecycle_execution_v1/ledger.json) binds the following
scopes to their own source, executable and supervision receipts. The public
runtime gate is still false, and the exact-artifact production policy is empty.

All four learned-multiwindow tests pass, including the actual small CPU and
Metal task/retry cases. Their final-focused v2 aggregate remains failed:
67 selected, 65 passed, one expected child-fixture skip, one TTL failure and
zero leaks. This does not qualify long-document HTTP or concurrent forwards.

Nine separate fresh child probes then pass against that frozen executable,
all with exit 86 and no outer kill. Eight retain their 64-byte admission lease;
the escaped-session case has no lease. New polling and final-release cases
hold stderr's real lock: fatal watchdog paths exit immediately without logging,
so a wedged logging thread cannot prevent termination. These are synthetic
100 ms close deadlines under five-second outer guards, not measured driver hangs.

The actual TTL failure exposed an operational issue: releasing metrics/listing
snapshot handles refreshed each model's last-use timestamp, so frequent scrapes
could prevent idle eviction indefinitely. Snapshot release now preserves that
timestamp while retaining the same active-handle and retired-final-owner cleanup.
Ordinary inference handles still refresh usage. The model-free snapshot,
retirement and listing checks pass; no public release-mode knob was added.

Corrected actual-Metal TTL validation now passes all 11 selected tests with
no skips, failures or leaks, using the same frozen executable as the first
post-fix attempt. The first attempt passed ten model-free checks but hit the
live-memory guard before initial inference; that 11/10/0/1 receipt remains a
failed admission checkpoint. No cap or source change was made for the retry.

The successful test retains the same 1/4/5 GiB host/backend/combined profile,
3 GiB scratch and live-memory guard. A held model survives expiry, then actual
eviction releases its aliases, residency leases and teardown ticket. Fresh reload
and same-cache retry pass the fixture and five source-file hashes. A real metrics
scrape preserves the exact pre-scrape last-use timestamp; final eviction uses that
original expiry and returns cache, leases, tickets and transient device ownership
to their empty baseline. Both closing phases pass the unchanged 30-second bound
(15.871518 and 20.680144 seconds); held/pre-expiry/empty checks each take 1 microsecond
under their five-second bound. These are whole-maintenance timings, including
post-close allocator reclaim, not isolated driver latency or a performance gate.
The 74.024-second run reports 1,001,390,080 bytes sampled peak child-tree RSS,
unchanged 2,588-file source inventory and complete owned-process cleanup.

The independent [published regional training campaign](GLINER25_RECOMPUTED_TRAINING.md#published-small-native-campaign)
is also complete for native small all-target LoRA/DoRA restart/artifact consistency.
It does not qualify published regional Metal, broader source numerics or release.

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
The five focused observability tests pass, along with renderer, V2 dispatch
and cancellation/admission tests, in a 25-selected-test CPU checkpoint with
no skips or leaks. Its receipt is
`/private/tmp/gliner25-merge-metrics-integration-v1.log`, SHA-256
`0a0b81baffed9696beef213f13fa01dec138e0f7346c1b1ed79fd63712c41c7c`.
The shared Prometheus package suite also exits successfully, including scalar
and vector exact-boundary regressions: histogram `le` buckets include values
equal to the declared upper bound. The optional
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
