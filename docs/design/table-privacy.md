# Table privacy implementation

Implements the table-scoped **Privacy** design in
[issue #1401](https://github.com/antflydb/antfly/issues/1401). The user-facing
contract and examples are in [Table privacy](../reference/table-privacy.md).
The branch starts at `f2b3baaee1cb508005451843c17fa07f2e069d4b`.
Main was merged at `6817ccb184`; privacy uses metadata protocol version 79.

## Policy and user experience

The six `/db/v1/tables/{table}/privacy` operations are declared in
`specs/openapi/antfly/privacy.yaml`, implemented in `api/privacy.zig`, and
included in the Go, Python, Rust and TypeScript generated SDKs. TypeScript also
exposes `client.privacy` with ETag handling, typed errors, cancellation and
bounded JSON responses.

The Privacy tab provides selected fields, category groups and custom category
definitions, sensitivity and per-category thresholds, replacement style,
artifact-compatible producer configuration, explicit sample keys, UTF-8 span
highlights, external-processing disclosure, and false-positive threshold tuning.
Protective saves require a fresh preview and an availability acknowledgment.
The editor pins the ETag of its loaded policy. Status polling never advances
that save precondition; explicit reload adopts a concurrent policy and clears
the previous preview/availability acknowledgment. Coverage and findings
distinguish unknown, pending, failed and successfully
inspected documents; the interface says “No findings detected.”

Policies use the artifact API's extractor/generated-extractor/generator
configuration model. Custom revisions are mandatory. Credentials remain
references or environment defaults; literal secrets are rejected. The default
native PII checkpoint is pinned at
`1cb4166094dc58fa8d836429f060d6c95f62b495`. Preview, backfill and new writes
use the same validated producer contract.

PUT installs policy, managed producer graph and protected consumers in one
metadata definition CAS. Exact ETag/If-Match protects replacement. Canonical
policy identity includes fields, resolved categories/thresholds, mode,
replacement and producer identity. Batch/retry tuning is stored separately
from producer identity and does not invalidate accepted inspections. Scan
progress is excluded from owner configuration comparisons so advancing a job
cannot restart producer workers.

`redact_before_storage` returns typed 422
`privacy_synchronous_ingestion_required`. No asynchronous path claims to erase
original values before persistence. Protected document tables reject unsupported
relational/graph consumers, external authored vectors, and unsupported source
chains during admission. Index/enrichment mutation cannot bypass the managed
graph while protection is active.

## Inspection and publication

`privacy/contract.zig`, `inspection.zig`, `document.zig` and `materialize.zig`
validate half-open UTF-8 source spans, optional confidence, category identity,
complete source fingerprints and per-field receipts. Invalid or missing evidence
is a failure, never an empty successful scan. Redaction replaces the union of
accepted overlapping spans. Durable findings omit matched values.

Compatible short inputs batch across documents and fields. The default native
long-document route uses overlapping source windows, exact composed tokenizer
admission of at most 512 prepared tokens, at most 64 windows per document,
source-offset rebasing and deduplication. Compatible windows use physical
Metal/CUDA batches up to eight; CPU remains serial. Detector windows never
become search chunks.

Protected roots and descendant producer identities include the immutable policy
version. A stable input-free guard fences policy replacement. Default and named
text/vector consumers are rewritten together; chunking and embedding consume
the sanitized projection. Ordered chunk completion admits privacy document
roots through their accepted manifests, including inherited source guards.
A current owner-accepted chunk-vector stream closure releases its replay request;
document and physical projection obligations retain independent completion
witnesses. Private inspection receipts are stripped before
ordinary downstream producers and public document hydration.

A provider response cannot authorize serving. Replicated owners require accepted
ordered publication provenance, including current primary/derived input guards
and output revision witnesses. Privacy backfill admits the native artifact
catalog and activates its epoch through the routed Raft owner before scheduling
producer work. Standalone owners commit storage-owned local witnesses atomically
with artifact bytes; those witnesses bind source, policy and output and are
never accepted on a replicated owner. After ordered publication owns vector
sources, retiring an ANN consumer reclaims index metadata while retaining the
source artifacts. Existing retirement jobs recover under the same ownership
rule, so restoring original consumers after a switch to monitoring can rebuild
without unordered source deletion. Document deletion also retires local
witnesses for every previous policy generation. Retry admission writes an
owner-local sequence floor atomically with its durable work record; coverage
ignores a terminal failure from an older attempt, even if its worker finishes
after the retry was queued. New terminal failures retain their attempt identity.

Point reads, scans, filtering, hydration, stored search callbacks, text planning
statistics and AI source loading enforce current projection evidence. Search
additionally fences producer completion and physical index checkpoints at one
apply-shared cut before candidates and statistics are observed. Pending, stale
and failed content cannot influence protected results. Readiness is deliberately
conservative across the table's managed consumers: search may be unavailable
until the affected generation is complete.

Receivers independently capture fresh metadata policy fences, including requests
from older coordinators. Internal lookup/query/scan/statistics protocols carry
policy assertions and require matching receiver acknowledgments before exposing
results. Missing or mismatched acknowledgment fails retryably. Document SQL
uses retained owner snapshots and projects current sanitized artifacts before
predicates and column projection; remote cursor creation carries the same
policy assertion and acknowledgment. SQL mutations and delayed serializable
snapshots on protected tables fail with an unsupported response. This avoids
using a sanitized preimage to overwrite an original document. Ordinary batch
writes preserve originals and enqueue the new source revision. Opaque retained
query paths and relational storage remain unsupported. Public request flags cannot
authorize original access. Privacy operations and original artifact inspection
require table administration permission when access control is enabled.

The rollout requires the new metadata decoder and data-store privacy protocol;
older data stores cannot register into a privacy-enabled fleet. Distributed
admission also requires a dedicated internal-service signing identity.

## Jobs and coverage

Automatic and explicit scans retain durable identities, bounded cursors,
per-shard progress, failures and transport backoff in metadata. The metadata
leader's supervisor owns distributed scans; standalone uses its local catalog
authority. Every progress acknowledgment rechecks leadership term and the exact
prior table definition. Each scheduling quantum retains the exact oldest
runnable scan and table, allowing bounded retries to progress alongside a
full-table backfill. Scan retention preserves every active job and retires only
terminal or superseded jobs; a full set of 16 active jobs applies HTTP 409
backpressure without changing scan identity or sequence.

Backfill queues ordinary producer work without rewriting primary documents or
renewing TTLs. `processing` verifies current receipts and consumer publication
before success. Verification uses bounded resumable census pages. Public coverage is collected
independently in durable bounded pages by the same fair leader-owned scheduler;
GET status never opens data shards. The latest complete result remains visible
with observation-interval timestamps while another census runs. Unavailable
pages retain their cursor and reduce their page size, and policy replacement
resets coverage. These asynchronous observations never authorize protected reads. Source-bound
terminal failure markers are separate from clean receipts; source changes
invalidate them, and explicit scans can retry after provider recovery.
An ordered primary deletion closes generated-output requirements only when the
document's complete artifact directory is empty. Completion remains bound to
the materialization snapshot, so orphaned outputs keep work pending and a
resurrected document invalidates the old deletion witness.
Retired producer generation heads and clocks are metadata beside that output
directory and do not block deletion completion. Standalone audits each new
catalog generation after producer replay becomes idle, handing late consumer
receipt gaps to the existing paged recovery jobs.
Primary deletion retires both asset and embedding rerun targets along with
outputs, including targets whose provider never created an artifact. Managed
dense generators may repeat their own output name on the consumer config;
ordered publication rejects a consumer name that selects another output.
An accepted base-vector result for an older primary input is a cache miss for
a freshly captured source. The current-input recheck still rejects stale
tokens; delayed old publications cannot overwrite the replacement output.

Coverage reports total/current/pending/known-failed documents, documents with
findings, category/field counts and protected-ready/blocked counts. Census rows carry
status only, without original values. Parse scratch is released per row and
failure details are bounded. An incomplete or unavailable census yields null
counts. Findings pagination binds policy and filters and handles resumes within
a document; missing/deleted documents contribute no current findings.


Background work selection uses one ordered due entry per privacy-enabled table.
Standalone maintains a replaceable in-memory tree rebuilt by catalog replay;
Raft maintains forward/reverse scheduling hints in the existing derived catalog
indexes, atomically with table CAS writes. Snapshot restore and index migration
rebuild those hints from durable scan/coverage state. Selection reads one due
entry and its table, then checks local leadership and rechecks the term before
persisting progress. No page enumerates the entire administrative catalog.
Bounded-range verification pages shrink after an unavailable two-second read
and persist the learned size, cursor and accumulated evidence together. Full-table
jobs share the durable coverage census, accepting only a completed pass started
strictly after the job entered verification. Active jobs do not multiply the
shard-wide census workload; protected completion still requires serving readiness.
Readiness probes have a two-second execution budget. Routing, transport and
readiness failures persist bounded backoff without marking inspection failed;
the table advances its due entry so other tables can run. Publication still
rechecks metadata authority and term before that progress can commit.

Policy PUT acknowledges a logical resource version, independently of mutable
job JSON. A bounded CAS retry recomposes against current progress while
preserving the caller's exact policy ETag precondition. Executable policy
normalization sorts selected fields, resolves category groups/thresholds and
canonicalizes producer configuration before deriving managed enrichments.
Validation bounds the resolved union of group and explicit categories to 128,
so authored and canonical executable policies obey the same admission limit.
Finding deduplication uses a hash index preserving first-seen order and maximum
confidence across repeated field/window evidence, with expected linear cost.

Findings enumeration reads identifiers only. A per-document arena holds the
manifest, authoritative source and validation scratch; only accepted findings
survive into the response arena. Inspection and output each have an 8 MiB page
budget, with one complete document admitted before inspection yields. The
continuation includes the next unexamined document or finding ordinal, bound
to the policy and filters, including pages with zero accepted findings.
The storage scanner borrows plain document bytes for the duration of each
callback, including ID-only and hash-only scans. Privacy projection scratch
uses an independently backed row arena reused between callbacks; retained
response allocations cannot pin whole-document temporary copies.
Scratch backing uses the platform allocator's arena-unwrapping helper, retaining
allocation accounting and failure injection beneath request arenas.

## Validation

- ReleaseFast deduplication microbenchmark (three repetitions): 65,536 distinct
  valid findings improved from a median 965 ms to 10.3 ms; 16,384 improved from
  58.4 ms to 2.75 ms. This measures normalization, not model inference.
- Focused API target: `zig build antfly-privacy-test` — 21 API/metadata tests and 62 storage/runtime tests passed,
  including scan bounds, adaptive verification, evidence ownership, byte-budgeted findings,
  scheduling index allocation failures/snapshot restore, and transport policy fences.
- Storage contract target: 15 checks passed, including real DB ID-scan memory,
  borrowed projection/cancellation, allocation ownership and publication contracts.
  The ID-scan regression reads twelve 1 MiB originals while retained request
  allocations remain below 256 KiB.
- Scheduler regression drives complete passes through durable progress updates
  for both backfill and verification, including shard-leader, routing and timeout
  failures, and verifies another table can become the next runnable entry.
- Pure catalog/inspection suite: 26 passed. Privacy TypeScript SDK: 3 passed;
  dashboard privacy tests: 6 passed. Generated Go SDK tests passed. Full
  API/SDK/dashboard generation, formatting and license checks passed.
- Standalone HTTP: 9 passed, 1 optional model-weight test skipped (260 seconds).
  Raft HTTP: 9 passed, 1 optional model-weight test skipped (486 seconds).
  Both include resolved-category admission and policy replacement. The large
  membership cases took 24/23 seconds in standalone and 157/92 seconds in Raft
  for monitor/protective modes respectively.
- An initial standalone one-shard policy-replacement check timed out waiting
  for serving readiness. It passed in an isolated run, in the preceding-monitor
  sequence, and in the complete rerun with unchanged code and binary. The cause
  remains unconfirmed; the test retains scan, vector-index and server diagnostics.
- Large Raft membership tests enforce a 90-second no-progress watchdog and a
  ten-minute hard convergence cap, accounting for bounded publication backpressure.

### Earlier broader validation

The following results were recorded during implementation before this main merge.

- Compiled metadata owner regression verifies indexed due selection and point reads
  across the storage archive. Table-read regression checks both static vtable
  variants across capability selection; the full table-read target passed 125 checks.
- Runtime error transport: 43 tests passed, including privacy retry/corruption
  identities; SQL diagnostics: 7 passed. Remote scan transport: 2 passed.
- Standalone publication witnesses: 4;
  current-status/serving suites: 10 each.
- Python SDK: 779 passed; Go SDK tests passed; Rust SDK check passed.
- TypeScript SDK: 535 passed, one unrelated skip. Dashboard: 280 passed;
  UTF-8 preview highlighting: 3 passed. Full API/SDK/dashboard generation passed.
- Standalone and Raft HTTP lifecycle suites each pass six cases covering
  preview, CAS, backfill, current findings, protected query/hydration, sanitized
  SQL predicates, source edits, invalid-evidence failure, repeated retry,
  policy replacement, restart, deletion, null sources and empty tables. Raft
  covers one and two shards; monitor and protective cases include a managed
  vector index. Protective cases verify sanitized embedder inputs, return to
  monitor, null-to-text source updates and restart. Real default-producer HTTP qualification passed on
  required Metal with two selected fields and a long document.
- Fresh native/Metal/CUDA comparison uses three synthetic documents, UTF-8
  offsets and seven inspection windows. Labels, text and coordinates match
  exactly. Maximum score differences from native were `4.18e-7` on Metal and
  `5.37e-7` on CUDA; the final-window email is detected.

These synthetic checks establish the exercised runtime and publication
contract, not customer-text recall or calibrated confidence. Representative
application text and languages still require evaluation before deployment;
threshold tuning and privacy redaction reduce exposure of detected information
without promising anonymization or compliance.

The repeatable backend gate is
[`qualify_privacy_windows.py`](../../scripts/gliner_specialty/qualify_privacy_windows.py).
It requires the requested backend and records discrete-span and confidence
comparisons against a native reference, with task-owned server cleanup.
