# Table privacy implementation

Implements the table-scoped **Privacy** design in
[issue #1401](https://github.com/antflydb/antfly/issues/1401). The user-facing
contract and examples are in [Table privacy](../reference/table-privacy.md).
The branch starts at `f2b3baaee1cb508005451843c17fa07f2e069d4b`.
Main is merged through `ab58a77e243983386345e86969f5f8ddee8811e7`; privacy uses metadata protocol version 80.

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

Backfill discovery streams original identifiers and queues ordinary generated
asset references directly. It does not retain complete source pages or map
source documents before admission. Each page has a 1,000-row maximum and an
8 MiB budget for owned identifiers, producer names and reference storage; one
oversized identifier may advance alone. A page ending exactly at its ceiling
may require an empty follow-up page to establish EOF. Source capture, mapping
and detector admission remain owned by ordinary replay, with the same durable
retry floors and publication requirements.

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
Findings offer equivalent GET query and POST JSON-body forms. TypeScript SDK/UI clients use
POST so a continuation can carry any admitted document identifier without the
HTTP request-line limit. Versioned binary cursors avoid JSON-escape growth;
issuance and decoding share a bound derived from the document-write budget.
The findings POST body allowance covers that cursor plus document/field filters,
while other endpoint limits remain unchanged. Legacy JSON cursors remain readable.
Large internal lookup and manifest keys use a private document-read POST with
base64 key bytes, preserving binary namespaces without URL or JSON escaping
amplification. Short keys retain legacy GET compatibility. Body and response
budgets derive from the admitted document size and apply only to the relevant
endpoints; native and linked-kernel registrars use the same body limits. Lookup
controls, service-identity checks, policy acknowledgments, deadlines and read
consistency follow the same handler as the GET form.
Coverage status rows admit a maximum document ID plus its bounded status
envelope and are parsed once into per-row scratch. Failure counts include every
observed failure; examples retain at most 100 entries and 1 MiB of encoded JSON
across pages, so unusually large IDs cannot inflate the durable failure list.
Findings retain the ordinary 8 MiB page budget with one oversized-entry
allowance; the TypeScript SDK accounts separately for that entry, escaped key,
producer revision and continuation instead of imposing its generic response cap.

Ordered producer commands account for document identities separately from the
64 MiB artifact-value budget. Baseline, validation, completion and readiness
cursors share the admitted-key limit. Unit-worker directory probes, persisted
unit jobs and chunk scan/retirement cursors use that same limit; producer and
child names retain their separate smaller budget. Scan progress stores either
the shard resumes or a legacy global cursor, avoiding duplicate large keys;
an empty shard list correctly falls back to the legacy cursor. Public scan
status omits range bounds and every cursor, keeping polling responses small
while native continuations remain durable. Keys over 4 KiB, and literal keys
with the reserved reference prefix, are stored as immutable, SHA-256-verified
generations in metadata job state. The catalog contains only small references;
workers hydrate only the selected scan or census cursor. Existing raw-key
catalogs migrate on their next write. A standalone definition CAS sends an
exact digest of the previous indexes instead of duplicating the legacy payload.

Checkpoint uploads use 256 KiB chunks and a per-table 256 MiB staging quota.
Completed uploads are reused across catalog CAS retries, so contention cannot
repeatedly upload the same range key or consume staging quota for each attempt.
The metadata writer verifies all chunks before marking a generation ready, then
atomically attaches references with the table update and releases staging quota.
Unattached generations expire after one hour. The leader collects unreferenced
generations, including those left by deleted tables or failed uploads, in
transactions deleting at most eight chunks. Every deletion rechecks current
catalog references in the same writer transaction; generation identities are
never reused. Snapshot recovery includes chunks, manifests and deletion progress.
Protocol version 80 is required on metadata and data nodes. Data coordinators
forward policy and scan mutations to the metadata owner, preserving preconditions,
request controls and ambiguous outcomes without replaying scan admission.
The compiled metadata registrar exposes exactly the two authenticated privacy
control routes. They opt into the public 64 MiB request allowance through a
separate route ceiling in the HTTP parser; ordinary metadata routes retain their
32 MiB fallback limit. Buffered-body accounting covers one full opted-in request.
Ordinary pages keep their small work quantum; one oversized identity can advance
a page without starving forever.
Large control commands borrow bounded shared staging capacity while small
controls retain reserved headroom. Upload chunks remain 1 MiB. Progress records
preserve APP1 encoding through 128 chunks and use APP2 above that boundary.
Pending baseline commands coalesce per owner until apply; a changing observed
Raft watermark cannot fill the queue with repeated large upper bounds. Full-text
rebuild cursors use the same admitted identity limit. Standalone catalog updates
use compact UTF-8 text with a binary-safe fallback on the process-local transport;
canonical command encoding remains unchanged. Unavailable census pages retain
null counts and reduce the work quantum without advancing their cursor. The
census yields at a complete row boundary after its soft work quantum and commits
that validated prefix with a continuation. Routing/setup and one oversized row
have a separate bounded hard deadline; caller deadlines still take precedence.
Completed rows preceding a hard timeout also form a resumable prefix, while an
incomplete row is discarded and retried. Storage compares long binary key
prefixes in vector-sized chunks. Snappy match extension uses the same exact
vectorized prefix comparison, full-width copy tags use bulk repetition, and
overlap expansion uses bulk copies (with a single-byte repeat fast path).
Differential tests retain byte-identical compressed output. Ordering and encoded
formats remain unchanged, avoiding scalar byte loops during large-key SST
reads/compaction.
Ordered Raft receipt preflight, document commit, artifact publication, catalog
installation and upload staging use nonblocking apply admission. Storage reports
the existing stable `WriterLocked` identity; the server adapter normalizes it to
Raft deferral, whose retry identity also survives the outer compiled owner ABI.
Contention defers the committed entry so its owner releases the Raft progress
lock before retrying; the receipt is rechecked under the mutation fence. This
prevents an apply reader waiting for quorum progress from cycling with a Raft
owner waiting for exclusive apply admission.
Producer-readiness scans retain disposable snapshots outside apply. Their short
counter/revision transactions share primary mutation admission, preventing lost
read-modify-write updates between background settlement and new enrollment.
The v2 derived-summary namespace rebuilds older potentially undercounted caches
from durable obligations and publication witnesses. Legacy summaries retire in
bounded pages, with one oversized identity allowed to advance each page.
Enrollment and serving-status checks read a compact descriptor and point
counters. A separate root-scoped revision counter avoids rewriting large scan
checkpoints per document; only background checkpoint progress persists cursor
bytes. The enrollment descriptor has an independent small size/shape bound.
Durable producer obligations similarly separate a fixed 122-byte count/revision
record from the potentially large baseline cursor. Legacy APO2 records migrate
atomically on the first write; APO3 checkpoints require the new metadata and
older readers reject the new format rather than trusting stale counts.
Internal publication batches also use compact UTF-8 identities with a binary
fallback; the canonical command writer/digest and legacy JSON decoder remain
compatible. Publication decoding budgets identities separately from values.
Raft checkpoint byte targets amortize against the prior retained-log image,
avoiding a full-log rewrite for every large control entry. The configured
replay-record limit and durable Ready append/watermark ordering remain intact;
recovery restores the prior image-size estimate from its checkpoint.
The raw apply store syncs each commit to the WAL and batches SST publication at
8 MiB with a bounded 64 MiB deferred queue and 32 MiB flush windows. Shared
background maintenance keeps compaction off the Raft progress worker when
capacity is available; bounded foreground assistance remains for pressure.
Compaction domains separate mutable metadata from append-only command history.
Read-only inspection replays durable WAL state before writer shutdown, and
reopen retains exact replay/conflict detection.

Background work selection uses one ordered due entry per privacy-enabled table.
Standalone maintains a replaceable in-memory tree rebuilt by catalog replay;
Raft maintains forward/reverse scheduling hints in the existing derived catalog
indexes, atomically with table CAS writes. Snapshot restore and index migration
rebuild those hints from durable scan/coverage state. Selection reads one due
entry and its table, then checks local leadership and rechecks the term before
persisting progress. No page enumerates the entire administrative catalog.
Progress-only table CAS updates use an explicit runtime-field allowlist
(`coverage`, `scans`, `scan_id`, `scan_sequence`). Every other table, index,
and policy field must remain identical. The authoritative row, schema digest,
and due index commit atomically without a semantic catalog notification or
revision change. Unconditional reconciliation upserts retain the durable
privacy resource and reject stale policy bindings, so a cached semantic catalog
cannot rewind job progress. Remote data APIs fetch current physical table records
through a linearizable private catalog point read; progress never depends on a
cached administrative/routing snapshot. Definition-CAS ingress also checks this
current row through a shared validator. Runtime-only admission uses that point
read and the allowlist, bypassing catalog-wide ownership validation and extra
reconciliation rounds. Authored changes retain ordinary admission. Policy installation is acknowledged by its immutable
resource version even if worker progress has already advanced. Standalone advances its durability revision while retaining
routing and join-planning generations, including allocation-free rollback of
the existing due node. Policy changes retain ordinary catalog publication.
If a worker advances the row between policy admission and Raft apply, the
rejected authored CAS records its command digest and exact term/index in the
existing bounded per-table rejection slot. Successful or rejected runtime
progress cannot overwrite that receipt. The policy endpoint can then retry a
known rejection; a superseded or missing receipt remains an unknown outcome.
This preserves the full-record CAS boundary without creating an unbounded
per-command history or adding a wire command.
Bounded-range verification pages shrink after an unavailable two-second read
and persist the learned size, cursor and accumulated evidence together. Full-table
jobs share the durable coverage census, accepting only a completed pass started
strictly after the job entered verification. Active jobs do not multiply the
shard-wide census workload; protected completion still requires serving readiness.
Readiness probes have a two-second execution budget. Routing, transport and
readiness failures persist bounded backoff without marking inspection failed;
the table advances its due entry so other tables can run. Publication still
rechecks metadata authority and term before that progress can commit.
An idle producer watermark and completed discovery cursor cannot discharge
missing consumer publication receipts. The coverage audit re-arms the same
durable source-discovery job using a control-revision CAS; its existing bounded
pages restore proof without replacing the physical generation. Pending replay,
paused jobs, known terminal producer debt, concurrent handoffs, and active
candidate construction retain their existing owner boundaries. Aggregate
failure receipts also suppress rediscovery when legacy per-source receipts are
absent, preventing terminal failures from becoming repeated inference work.

Policy PUT acknowledges a logical resource version, independently of mutable
job JSON. A bounded CAS retry recomposes against current progress while
preserving the caller's exact policy ETag precondition. Executable policy
normalization sorts selected fields, resolves category groups/thresholds and
canonicalizes producer configuration before deriving managed enrichments.
Validation bounds the resolved union of group and explicit categories to 128,
so authored and canonical executable policies obey the same admission limit.
Finding deduplication uses a hash index preserving first-seen order and maximum
confidence across repeated field/window evidence, with expected linear cost.

Findings enumeration streams identifiers and stops at 50 IDs or 8 MiB of
encoded ID rows, allowing one oversized ID. Independent line scratch is released
before manifest/source reads; enumeration never materializes an entire large-ID
page before applying its budget. Manifest reads propagate request deadlines and
cancellation through routing, compiled owners, read-index admission and the
private HTTP POST, whose remaining budget is enforced again at the receiver.
A per-document arena holds the manifest, authoritative source and validation scratch; only accepted findings
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

Managed inspection shares one validated policy/normalizer across its field
cohort. Physical batches retain only normalized, source-bound field evidence;
provider envelopes and arbitrary extra response fields are released per batch.
Composition uses the same inspector for serial and batched execution. The host
memory plan covers logical output retention plus the largest physical field
batch, including configuration and field-control overhead. Admission parses
one document at a time in reclaimable scratch rather than retaining a DOM for
every document within its resolution budget. A scratch allocator unwraps arenas
while preserving every nested invocation quota; temporary frees
therefore release actual backing memory even when results are interleaved.

Detector evidence, normalized field text and final document projections have
separate bounds. Normalized text may retain the source-sized selected values;
composition also preserves unselected fields. The host's logical-result bound
is three source byte lengths plus twice the producer evidence allowance and a
512-byte envelope. Admission charges the actual source sum across the cohort
and independently accounts for normalized receipts retained alongside final
results; one large source never multiplies every sibling's reservation. Checked
arithmetic fails closed on overflow. The underlying producer's result limits
continue to bound raw detector output on every call.

Dependency priming consults the same incarnation-, source- and definition-bound
retry tickets as ordinary document replay. Its healthy path does one indexed
lookup without reading primary bytes. It publishes successful siblings and
allows durably parked provider failures to yield; control, publication and
worker failures still stop the quantum. Ordinary replay reloads delayed roots
after priming, so their consumers wait without re-invoking a rate-limited root.
Provider Retry-After clocks survive restart and new document admissions.

External-processing disclosure uses the same pure endpoint resolver and linked
callback availability as execution. Explicit endpoints override callbacks;
otherwise callbacks override the configured inference endpoint. Generated
extractors use generator routing. Policy/status reads perform no capability
fetch, credential resolution or model invocation to report this flag.
Forwarded metadata acknowledgments are localized to the issuing data API's
route, preserving ETags, mutation outcome and unknown response fields (including
exact numeric literals). An already-correct flag or a scan-only response is
returned without replacing its body.

Checkpoint reclamation shares eight chunks (at most 2 MiB), 32 CAS operations,
and a 25 ms soft time budget across each quantum. A completed small blob can
lose its header in the same quantum. Partial blobs rotate behind later rows.
Backlogged work polls after 100 ms; idle work polls after one second, subject to
the maintenance supervisor's scheduling frequency. Writer-side reference and
term checks still fence every deletion.

New checkpoint generations retain a per-table payload charge until final header
erasure, including attached and retired generations; each generation charges at
least 4 KiB. The retained limit is 8 GiB, alongside the existing 256 MiB staged
upload limit. Denied reservations change neither counter nor manifest. Capacity
pressure yields the entire table for five seconds using its last committed
cursors, allowing cleanup and other tables to progress. It never skips a source
page or turns capacity pressure into a terminal inspection failure.

Metadata protocol 81 gates new accounting writes. Legacy headers remain
readable and drain normally; their serialized bytes omit the new false flag so
historical CAS hashes continue to replay exactly. New accounting counters and
partial reclamation progress survive snapshots and restart.

## Validation

### Latest discovery, preview and identifier fixes (2026-11-04)

- The initial main fetch was already incorporated through `4bb2b815aa`.
  Main advanced during qualification; merged
  `8c1eddf12be59f4f14672d5f63c9d2fbadb232d9`, including resumable full-text
  replay. The benchmark conflict retains main's multi-document calls and the
  branch's existing diagnostics; all 18 benchmark helper tests pass.
- Discovery now retains references instead of full source pages and cleaned
  document copies. The real-store memory regression exercises discovery over
  twelve 1 MiB documents; separate cases cover byte limits, row limits and
  progress for an oversized identifier.
- Preview computes one policy version for response construction and reuses it
  across findings. The HTTP fixture includes 2,000 findings and a 16 KiB category
  description, checking every finding's version against the response.
- The review's claim that saved execution controls were discarded was incorrect:
  the existing policy-bound override intentionally keeps controls outside the
  immutable producer. The new HTTP regression tunes from one to five attempts,
  preserves identity/generation/scan, restarts, and requires the fifth response
  to recover during an explicit rescan. No producer-identity change is needed.
- Pre-merge integrated build: 106/106 steps, 58 API/metadata tests, 151 native
  storage/privacy tests and two ABI checks passed. Formatting and license
  checks passed.
- The synthetic 2,000-finding preview with a 16 KiB category description took
  0.091 seconds on the qualified build, compared with 0.712 seconds in the review
  reproduction. The 341,433-byte response shape is unchanged. This measures
  synthetic HTTP/response processing, not model inference or customer throughput.
- Pre-merge regression server SHA-256:
  `7342d8158de254d7ae123b8c9f4d98ad7af55b2e498475c5997295ff869337ec`.
- Model-weight, Metal and CUDA execution are unchanged; this qualification
  does not repeat those backend runs.
- The initial standalone suite hit the existing 60-second replacement wait:
  receipts were current and the named text/vector indexes were ready, while
  the implicit text index still reported source publication/backfill pending.
  On the unchanged binary, the isolated case passed (60.28 seconds including
  setup), as did the preceding one-shard monitor/protective sequence (114.94
  seconds). No timeout was widened or publication check bypassed.
- The complete pre-merge Raft HTTP suite passed 21 cases with one optional
  model-weight skip (749.57 seconds), including the 1,000-row rescan whose two
  5 MB identifiers force byte-bound continuation.
- The complete unchanged-binary standalone rerun passed 21 cases with one
  optional model-weight skip (475.36 seconds); the replacement lifecycle passed
  in 55.85 seconds. The initial timeout remains recorded above.
- Merged-main build before the empty-ID follow-up: 106/106 steps, 58 API/metadata tests, 151 native
  storage/privacy tests and two ABI checks passed. The execution-control fixture
  now uses four documents to verify serial physical batches as well as recovery
  on the fifth configured attempt after tuning and restart.
- On the merged binary, the same 2,000-finding preview took 0.078 seconds with
  either an empty or 16 KiB category description; the per-finding policy-size
  cost is removed. Server SHA-256 before the empty-ID follow-up:
  `09dfd1572ee6f52579d2ba841fe5995250e6fa9d92ba9d9d4ba3ba6dfd7483ac`.
- That merged binary passed the complete standalone suite (21 passed, one
  optional model-weight skip, 468.94 seconds) and Raft suite (21 passed, one
  optional model-weight skip, 758.70 seconds).
- A final boundary check found that empty document IDs were admitted but an
  empty continuation key was indistinguishable from the initial open bound.
  Discovery, coverage and range verification now explicitly preserve exclusive
  continuation through local scans, shard routing and trusted internal/owner
  transports. A boolean marker keeps large identifiers on the wire only once;
  public scan requests cannot enable this private option. Native and HTTP
  regressions cover empty, NUL and non-ASCII IDs with one-row discovery pages.
- The new HTTP boundary regression also exposed inconsistent artifact validators:
  the primary store admitted empty IDs while artifact manifests, publication and
  completion rejected them. Document identities now follow the store's key
  contract; producer and unit names remain nonempty, and byte, ordering,
  authority and checksum checks remain enforced. Retry state records a present
  empty cursor explicitly; existing nonempty/absent cursor framing is preserved,
  and readers accept both header forms. Focused tests cover empty retry tickets,
  replay cursors, scope keys and unit-job round trips.
- The first Raft boundary run detected an additional remote shard encoder that
  reconstructed only three request fields and dropped the continuation marker.
  It now serializes the typed request, matching the local/owner path.
- Catalog-install waiting preserves the original checkpoint instead of returning
  an unadmitted exclusive cursor. The internal HTTP route/client preserve the
  pending-publication error identity, and the coordinator yields without
  consuming its failure budget. Scheduler and transport regressions cover this
  retry path.
- HTTP completion then exposed two more pre-existing empty-ID omissions: producer
  readiness rejected a valid zero-length document suffix, and journal/index
  replay dropped empty document keys. Readiness now validates the encoded prefix
  and producer digest independently of document length. Journal/replay retain
  empty IDs; existing tombstone slot metadata distinguishes a real empty delete
  from a canceled nonempty artifact delete without another map or allocation.
  The stream-readiness fencing regression now uses an empty ID, and dedicated
  native cases cover journal references/deletes and all index replay kinds.
- The completed production build passed all 106 steps, with 61 API/metadata,
  158 native privacy/storage and two owner/ABI tests. Adding the existing vector
  tombstone cancellation/revival regression to the permanent privacy target
  then passed all 57 target steps, 61 API/metadata, 159 native and two owner/ABI
  tests. The separate compatibility run passed all 11 selected retry-workset,
  replay-cursor and journal tests, including healthy progress across restart,
  allocation failures and maximum durable metadata budgets.
- The final empty-ID HTTP cases pass for monitor and protective modes in both
  standalone (38.48 seconds) and two-shard Raft (46.35 seconds). Each discovers
  exactly three empty/NUL/non-ASCII IDs and reaches current coverage.
- Final production server SHA-256:
  `62a8ba8c821d23fa9f8f107b49e276d658b82ccf3363eb798fa0978a22b6f6cb`.
- Final complete HTTP suites on that binary: standalone **23 passed**, one
  optional model-weight skip (488.93 seconds); Raft **23 passed**, one optional
  model-weight skip (739.60 seconds). The final runs include serial cohort/retry
  tuning across restart, dense 2,000-finding previews, byte-bound discovery with
  5 MB identifiers, empty-ID completion and monitor/protective lifecycle cases.
- Final synthetic preview recheck: 2,000 findings took 0.083 seconds with an
  empty category description and 0.065 seconds with a 16 KiB description,
  compared with 0.712 seconds in the original large-policy reproduction. Both
  responses remain 341,433 bytes. These individual synthetic samples establish
  removal of the policy-size multiplier, not a production throughput guarantee.
- Final Zig/Python formatting, Python lint, diff and license checks pass;
  the Apache boundary covers 2,407 source files. Required cache cleanup completed
  successfully, preserving active builds and leaving 100.8 GB free at completion.
- Review fixes are committed through `704547d306`; main remains merged through
  `8c1eddf12b`. Model-weight/backend qualification was not repeated because
  inference execution and model math are unchanged.

### Previous retry, projection and disclosure fixes (2026-11-04)

- Merged main through `4bb2b815aa7d359d979d28cb9a4f4fa6205e70fc`. The merge
  keeps compact producer enrollment separate from mutable readiness cursors,
  adopts the bounded transaction-local empty-input proof, and regenerates the
  Go client with its license and SQL transport policy hooks.
- The real-store priming regression covers a one-hour provider retry, restart,
  healthy-consumer progress after a write 30 seconds later, preservation of the
  provider deadline, and completion when the failed root becomes due. All
  three retry workset regressions pass on the integrated source, alongside
  218 ordered-artifact tests and 10 owner/ABI checks (67/67 build steps).
- The HTTP regression now covers 2 MiB selected and unrelated fields in serial
  and native batched previews, protective backfill and reads, and rejection of
  oversized raw provider output. The selected-text fixture explicitly configures
  a 4 MiB constrained-input budget; production input defaults remain unchanged.
  Pure tests cover overflow-safe budgets,
  source-sum admission for mixed-size cohorts, explicit/default endpoints,
  independent callback availability, and generated-extractor disclosure.
- Integrated server/focused build: 106/106 steps, 58 API/metadata tests,
  150 native storage/privacy tests and two ABI checks passed. The forwarded
  acknowledgment regression verifies route correction without losing exact
  numeric literals, future producer fields, ETags or unchanged-body reuse.
- Full Go SDK tests, Zig/Python formatting and license checks passed. `make gc`
  completed while protecting active builds. This review does not repeat
  model-weight, Metal or CUDA qualification.
- Qualified server SHA-256:
  `7a7372f3f65702e3e3d948c3478d031538c30afa710ae69daa4c8db21ece10d9`.
- Complete standalone HTTP suite: 20 passed, one optional model-weight test
  skipped (393.29 seconds), including the expanded source-budget regression.
- Complete Raft HTTP suite on the same binary: 20 passed, one optional
  model-weight test skipped (771.49 seconds). Both suites include one/two
  shards, policy replacement, restart, membership changes, large identifiers,
  private scan boundaries, source-sized projections and raw-output limits.

### Previous detector memory and checkpoint retention fixes (2026-11-04)

- Merged main at `7bc16af5383a8be6e9f95877f4bd443487d06fc4`, retaining privacy
  dependency batching and failure markers alongside durable scoped retries and
  shared source preparation. Regenerated the combined Go API client; Go SDK
  tests and all 13 host allocator tests pass.
- The focused regressions cover the maximum 50-document/64-field memory plan,
  preservation of both inner and outer invocation quotas, atomic retained-byte
  admission, snapshot restoration, multi-manifest GC budgets and fairness, and
  capacity backoff without cursor advancement.
- Integrated server/focused build: 106/106 steps, 53 API/metadata tests,
  147 native storage/privacy tests and two ABI checks pass. The two durable
  scoped-retry regressions pass after the merge. The ordered-artifact target
  passes 218 native tests and 10 owner/ABI checks, including the merged
  execution-identity and protocol-floor cases. Formatting and license checks
  pass. Qualified server SHA-256:
  `63d02912ec3dde876ef3b2503c2b70509a0b5d15a6775e543a9087cb9bd1a9dc`.
- Standalone and Raft upgrade probes start protocol-80 binary SHA-256
  `c4faece05bee12a4f234fabed47df2f900043fe460bddc3243ca133aaf7a0c80`,
  persist an out-of-line legacy checkpoint, switch to the integrated server,
  finish that scan, update the policy with its exact ETag, then persist an
  accounted checkpoint and restart again. Both pass (13.49/10.36 seconds).
- Complete integrated HTTP suites each pass 20 cases with one optional
  model-weight skip: standalone 405.12 seconds and Raft 708.42 seconds.
  They run concurrently; these are qualification durations, not isolated
  throughput benchmarks. The oversized-preview regression
  waits for full publication of its 9 MiB identifier before testing sample
  admission, so a concurrent Raft read-fence timeout does not replace the
  intended 422 assertion.
- A synthetic HTTP memory probe uses 32 KiB of provider padding per field.
  The maximum 50-document/64-field cohort completes in 400 native batches of
  eight, discards padding, and returns 1,191,657 bytes. On the integrated
  server it takes 3.75 seconds; sampled warm-process RSS grows by 32,912 KiB.
  This measures host orchestration with a synthetic detector, not model/GPU
  throughput. Model weights, Metal and CUDA are not requalified in this fix.

### Previous preview and listener fixes (2026-11-04)

- Main remains incorporated through `ab58a77e243983386345e86969f5f8ddee8811e7`.
  Final server SHA-256:
  `c4faece05bee12a4f234fabed47df2f900043fe460bddc3243ca133aaf7a0c80`.
- Data, standalone and metadata listeners admit the existing explicit document-read
  and findings body budgets. Ordinary routes retain their existing limits and
  aggregate transport admission still bounds buffered bodies.
- Preview includes escaped identifiers in its 8 MiB input budget, streams
  automatic samples, and counts serialized output before allocating its response.
  Responses above 8 MiB return `privacy_sample_too_large` rather than exceeding
  SDK limits. Native inference batching remains unchanged.
- Checkpoint GC borrows validated catalog slices and reuses the pinned table row
  within a page instead of repeatedly copying schemas and catalogs. The regression
  exercises 32 projections of an 8 MiB schema with borrowed catalog ownership.
  A real-store GC page with 32 headers and an 8 MiB table description also
  stays below 32 MiB of measured allocations.
- Final focused gate: 49 API/metadata tests, 145 storage/runtime tests and two ABI
  checks pass. Server build recheck passes all 71 steps; HTTP parser/server tests
  execute directly with 173/173 passing. Formatting and license checks pass.
- Full privacy lifecycles each pass 19 cases with one optional model-weight skip:
  standalone 403.55 seconds and Raft 741.54 seconds. These include preview
  response expansion from repeated IDs and run concurrently, so their durations
  are qualification times rather than isolated throughput benchmarks.
- The two new HTTP boundaries pass in standalone (31.88 s) and Raft (26.75 s),
  including absent 50 MiB key reads, explicit/automatic oversized preview rejection,
  and successful automatic sampling at a row limit and at end of stream.

### Previous checkpoint and findings fixes (2026-11-04)

- Main is incorporated through `ab58a77e243983386345e86969f5f8ddee8811e7`.
  The qualified final binary is SHA-256
  `a1cb6458db1f4e211591f928a62fc8060cb82a2e2c3b9cbbeba7701e40bf078d`.
- Server/focused gate: 109/109 build steps and 45 API/metadata checks passed,
  plus 145 storage/runtime tests and two active ABI checks. HTTP server and
  parser regressions passed 173/173, including explicit route opt-in and the
  unchanged ordinary-body limit.
- The 33 MiB empty-table boundary lifecycle passes in standalone and Raft,
  including restart, completion, polling bodies below 4 KiB and another policy
  update. Calls took 18.54/21.87 seconds respectively, including restart.
- Upgrade qualification starts the preserved pre-fix binary, persists a raw
  boundary, switches binaries and verifies completion and another policy update.
  Standalone covers 33 MiB (77.08-second call); Raft covers 17 MiB
  (19.93-second call), within the old metadata listener's 32 MiB admission cap.
- Full lifecycle cohorts on the final binary each passed 16 cases with one
  optional model skip: standalone 366.27 seconds and Raft 694.88 seconds.
  Boundary cases are recorded separately above. The final lifecycle suites ran
  concurrently; these durations are qualification times, not isolated throughput
  benchmarks. Raft's 5,000,000-byte ID case took 105.64 seconds.
- TypeScript SDK: 543 passed with one unrelated skip; typecheck and Go SDK tests
  pass. API/SDK generation, formatting and license checks pass. Inference/model
  execution is unchanged; these fixes do not add a new GPU qualification run.

### Earlier boundary and performance fixes (2026-11-04)

- Merged main at `ab58a77e24` and regenerated the API, SDKs and dashboard types.
- Focused contracts pass 145 native tests without leaks, 37 API tests and two
  active runtime error-registry checks. The full active registry passes 11 tests;
  the WAL replica suite passes 28 tests.
- Raw Raft apply-store suite: 34 tests pass without leaks, including shared
  background runtime wiring, live read-only WAL visibility, replay conflicts,
  snapshots, overlap recovery and compaction-domain boundaries.
- Regressions cover large binary internal reads, large coverage rows and cursor
  round trips, bounded failure examples, producer/readiness checkpoint migration,
  stale completion fences, concurrent writer admission, compact publication wire
  encoding, amortized Raft checkpoints, and byte-identical Snappy encoding.
- TypeScript SDK: 543 passed, one unrelated skip; TypeScript type checking and
  generated Go SDK tests pass. Generation, formatting and license checks pass.
- A local Debug microbenchmark encoding a repeated 5 MiB input improved from
  21 ms to 3 ms with identical compressed bytes. This measures the encoder only.
- Standalone HTTP: 16 passed, one optional model-weight test skipped (354.20 s).
- Raft 5 MB identifier boundary regression: passed (128.89 s total, 109.91 s
  call), with all 101 roots current, a completed resumed scan, signed lookup,
  manifest and manifest-list body reads, and rejection of unsigned originals.
  Remaining Raft HTTP cases: 15 passed, one optional model-weight test skipped
  (521.71 s); together the two Raft runs cover all 17 cases with 16 passes and
  one skip on the same binary. Cases include one/two shards, policy replacement,
  restart, resumed coverage, membership changes and large/escaped identifiers.
- Qualified server binary SHA-256:
  `a25e75266da97d87c62e3659eadf2d2f5e820806b050c1a5613a04a2c42416c8`.
  This review does not repeat model-weight, Metal or CUDA qualification.

### Previous review validation (2026-11-03)

- Merged main at `e6144685f8` and regenerated the public API and all SDKs.
- Focused privacy build: 121/121 steps and 25/25 API/metadata tests passed;
  the native storage/privacy group passed 65 tests without leaks. Regressions
  cover cache retention, current-row admission, stale authored-CAS rejection
  receipts, durable progress reconciliation, bounded publication recovery,
  snapshot scheduling rebuilds, and modern/legacy cursor round trips.
- Final standalone HTTP suite: 13 passed, 1 optional model-weight test skipped
  in 279 seconds. Final Raft HTTP suite: 13 passed, 1 optional model-weight test
  skipped in 406 seconds. Both run the same final binary and include one/two
  shards, monitor/protective policy saves and replacements, restart, resumed
  coverage, 3 KiB/36 KiB escaped document identifiers, and rejection of
  unsupported storage before preview, installation or rescan.
- Generated Go SDK tests, TypeScript privacy SDK tests (3), dashboard UTF-8
  preview tests (3), full generation, formatting and license checks passed.
  This review did not repeat native model-weight, Metal or CUDA qualification.
- Final binary SHA-256:
  `10e133029c1269edee2e9c951a8350a00344e2ddfee833566b09a3045bec3852`.

### Prior validation

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
  was subsequently traced to missing default text-index publication receipts after
  completed initial discovery. The bounded recovery fix described above and its
  deterministic regression now cover that state; the HTTP test retains scan,
  vector-index and server diagnostics.
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
