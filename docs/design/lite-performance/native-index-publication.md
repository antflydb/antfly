# Lite native publication architecture

Lite's native transaction engine owns the durable checkpoint, recovery,
allocator, snapshots and physical reclamation. Document changes and text-index
activation share this engine; an index's metadata layer does not create a
second physical durability domain.

## Request ownership and admission

Requests own parsed values, immutable schema/catalog views, prepared postimages,
posting inputs and content proofs before entering the writer lane. Captured
plans retain namespace/incarnation and relevant revision witnesses. Final
admission checks those witnesses against current state. Predicates and transforms
retain their ordinary atomic concurrency checks; an old prepared postimage
cannot authorize a mutation of a changed document or recreated table.

Queued publication requests form bounded cohorts before acquiring writer gates.
Each member retains its own sequence, predicates and outcome. Unsupported cohort
shapes use the ordinary ordered publication path. Cancellation before publication
can refuse work; cancellation after a durable write cannot make the write
retryable. A request's contention budget counts waiting rather than expensive
proof or preparation work, and spans its admission handoffs.

Native batch publication returns owned visibility completion inputs before
releasing admission. Cohort members share immutable inputs, while each caller
keeps its own observation cursor. Waiting happens after leaving the publication
queue. Short admitted probes validate the table incarnation, logical metadata,
index identities and committed progress; they do not run providers, replay or
merges. Admission and API locks are released between probes. The background
owner performs bounded service, or a workerless owner performs that same service
outside request admission. A completed producer's accepted output cut is retained
across probes so later unrelated producer debt does not extend the wait.
Graph endpoint jobs are serviced by the owner too. Full-index completion first
observes those durable jobs becoming clean and then retains the resulting replay
cut. A second generic post-batch wait must not reintroduce replay under writer
admission. Graph coverage targets are also captured once per waiter and owned
through its completion, rather than recaptured as later vector writes arrive.

SQL normalization can retain an owned packed relational row authority. The final
mutation reuses that authority only for exact key/value/JSON-null provenance,
schema/layout and catalog witnesses. Conflict postimages that change those bytes
require new preparation. SQL RETURNING does not require repacking the same row
solely for commit. Session staging retains session-owned state; it never borrows
a shorter SQL request allocation. Coordinated constraints and unsupported owned
plans retain authoritative validation rather than weakening concurrency rules.

## One durable primary and text publication

New native-hosted text metadata uses transactional native namespaces. Immutable
posting artifacts remain useful, but their activation, deletion metadata,
projection progress and the primary change can join one native transaction.
Existing metadata layouts retain conservative compatibility; creating an empty
new root must never hide an existing index.

A prepared native participant stages effects into the coordinator's transaction.
It cannot independently commit or activate in-memory state. Exact index/source
identities, deletion revisions, input fingerprints and segment-ID frontiers are
checked before durable publication. All fallible preparation and replacement
allocation precedes commit. In-memory activation follows durable success, with
snapshot capture fenced across the durable-to-memory transition.

Small prepared artifacts can join activation directly. Larger immutable artifacts
use bounded unpublished construction leases before an admitted activation links
them. A collision never authorizes replacing or deleting a foreign artifact.
A possibly committed publication fences its runtime until fresh recovery
reconciles the checkpoint; it does not report a retryable pre-commit refusal.

Checked allocator ownership and publication proofs enable one synchronization
boundary per native commit group. Unverified legacy state keeps its conservative
barriers until authenticated. Recovery and failure injection rules are described
in [native-commit-proofs.md](native-commit-proofs.md); removing a synchronization
call without those proofs is not a valid optimization.

## Posting windows independent of RPC boundaries

Native text writes retain bounded, durable normalized posting deltas rather than
forcing an FST build for every request. Readers can query these deltas directly.
The window bounds document space, bytes and fragment count; freezing it produces
an ordinary immutable posting segment. Tokenization and field/analyzer semantics
are preserved, including non-English input.

Primary document identities are assigned by the native transaction. Immutable
posting bytes cannot substitute a guessed ordinal or retain an unstable buffer.
Prepared value ownership and sealing bind those identities before publication.
Old snapshots retain the exact posting and deletion state they captured.

## Bounded asynchronous replay and credits

Expensive consumers use ordered, seekable canonical journal chunks. Continuations
select the next document family without decoding and sorting the entire original
request again. A family's overwrite, deletion and artifact effects stay together.
Consumer references identify shared canonical payloads instead of duplicating
whole records for every consumer.

Source completion and committed-effect progress are distinct. A producer can
publish several output fragments before finishing a source record. Independent
consumers must be able to consume those committed outputs, releasing finite
output credits without waiting for the unfinished producer. Full visibility waits
first fence source completion and then fence the resulting output cut.

Reclamation uses the minimum required frontier for every relevant consumer lane.
Repair and replacement readers retain their required lanes; failed catalog loads
never authorize deletion. Advancing a cursor alone does not refund bytes. Credits
return only after every required reference and physical retained payload has been
deleted. Partial continuation accounting follows physical ownership rather than
pretending a retained whole record is free.

Text producer capacity is shared by the foreground and replay paths. The owned
foreground plan checks the current per-index segment limit before capture and
checks the actual replacement inventory again under final admission. Declining
this optional fast path leaves the primary write and canonical journal intact.

A detached replay producer reports a distinct capacity yield. The owner retains
valid immutable inputs and runs the artifact service that restores headroom;
it does not classify capacity as a stale-source failure and skip that service.
A ready merge takes priority over another source attempt. Capacity yields do not
mark the captured source target serviced or imply that replay has completed.

A successful merge that restores producer capacity explicitly requeues its exact
namespace and incarnation when replay debt remains. A capacity refusal advances
neither a whole-record watermark nor a fragment cursor, so source-progress
notifications cannot supply that wake. Depending on a later catalog audit can
strand retained file-wide credits behind unrelated hot namespaces. Merge
completion changes scheduling eligibility; it does not advance coverage or refund
credits before the required physical records are deleted.

Capacity refusal is local to a consumer. Both the manual executor used by Lite
and the general replay driver give independent consumers a bounded turn before
returning that refusal. Source, producer-output and consumer lanes retain their
own progress; a parked text consumer cannot prevent another consumer from
releasing shared journal references. Optional graph maintenance similarly keeps
its continuation while the control sweep visits later cleanup services.

Merge capture precedes optional control work, so a refused control service cannot
prevent the artifact task that restores text capacity from starting. Private
construction owns its scratch context and directory bytes captured under the
catalog lock. It never dereferences a relocatable catalog entry from its worker;
final publication still validates the current index identity.

Normalized posting deltas use a merge-only navigation representation: retained
encoded bytes, one eight-byte reference per term contribution and document
norms. Positions are decoded for the current term instead of expanding every
input vocabulary into hash-map accumulators. Admission inspects physical layout
without constructing that vocabulary, reserves both retained navigation and
temporary term expansion, and reduces the selected prefix when its known demand
exceeds capacity. Query readers and the durable posting format are unchanged.

Source preparation reads an independently retained immutable cut. It does not
require an allocator proof for that obsolete cut to match a moving writer tip.
The admitted publisher rebases and authenticates the current allocator before
mutation. Legacy state retains its detached proof path.

## Separate source work, artifact work and physical service

Replay service has bounded producer and consumer turns. Optional idle maintenance,
status publication and physical cleanup do not run merely because replay was
requested. Unchanged advisory status does not require a timestamp-only commit.

An exact covered-source witness can acknowledge a new already-covered text tip
without reopening every table/index runtime. Metadata and control changes,
unsupported index mixtures, pending replay, recovery debt or an audit deadline
invalidate that shortcut. A document namespace with no consumers can use the
same fenced witness; advancing its covered tip still schedules independent
physical retirement. Immutable source/layout caches have explicit entry,
byte, range and lifetime bounds and exact inode/value identity checks. They do
not retain a mutable DB actor as though it were immutable.

Artifact control work opens short current primary reads after the replay epoch
ends. It must join current admission before pinning those checkpoints; registering
a reader after its old pages were reused cannot retroactively protect that cut.
Source computation still retains its independently captured immutable epoch.
Unexpected page failures are counted separately from cooperative yields and remain
visible without per-operation tracing. Qualification rejects them even if later
integrity checks succeed.

Native page failures also retain their identities across the document-provider
RPC boundary. They stop the provider runtime as storage failures rather than
entering document retry or isolation policy. The qualification fault marker covers
this path as well as maintenance, so an apparently successful importer cannot
hide a provider's persisted storage error.

Physical service has independent deadlines and bounded retirement batches. It
can reclaim eligible storage while writers remain active, rather than depending
on a quiet period. Reader frontiers and artifact leases protect exact resources;
semantic index generations do not change merely because an obsolete physical
artifact was removed. Foreign allocator state is validated once and its verified
ledger/digest can transfer to a replacement runtime under exact checkpoint checks.

## Prepared background merges

A merge captures immutable sources and deletion state under a short admitted
checkpoint. Segment construction and private output staging run outside writer
admission, with resource admission and lifecycle ownership independent of the
foreground producer. Structural mutations and shutdown drain all admitted tasks,
including manual assistance, before releasing their borrowed state.

Ready publication uses a fresh admitted index inventory. It validates immutable
source identities, preserves newer appended segments and adopts current deletion
state. File-backed deletion coordinates are resolved outside admission. Final
publication compares cumulative deletion witnesses and requests another lookup
pass when necessary. Resolved source coordinates remain cached across re-adoption;
read/allocation failures retain retry-safe ownership.

Prepared linked readers retain their runtime through destruction. Native alias
cleanup occurs while the required writer admission is held; private task disposal
and worker joining occur after releasing the owner gate. A completed merge is
scheduled before optional cleanup so unrelated maintenance cannot starve activation.

## Snapshots, recovery and outcomes

Snapshot protection uses durable inode-safe coordination identities, not an alias
pathname assumption. Unsafe aliases retain conservative inode fencing. Bounded
coordination component names support long database filenames; marker scans stat
opened descriptors when directory enumeration cannot supply a type. Integrity
and allocator inspection require metadata protection in addition to a data fence.

Each reader coordinator caches one bounded namespace descriptor bundle. Every
registration and scan revalidates the current directory entry, identity-file inode
and binding seed. Reader frontiers are scanned afresh, including after an earlier
scan reached the end of the directory. Identity or directory replacement cannot
inherit cached authority. The cache owns no database snapshot or reader marker;
idle descriptors do not retain database pages or extend reader lifetimes.
A changed process identity discards inherited descriptors before taking a guard
lock, since `flock` ownership follows the inherited open file description.
Marker sharing stays scoped to an exact checkpoint. Once an admitted writer
observes a newer checkpoint, it drops obsolete batch anchors immediately;
escaped readers keep their own references and continue protecting their cut.

Logical checkpoint identities survive publication and replacement correctly.
Replacement/compaction generations cannot inherit an unrelated clean recovery
witness. Paged DDL sweeps retain completion across pages, never skip a failed
namespace check, and affected-namespace admission requires recovery even while
background sweep work remains bounded.

Pre-commit Busy means no mutation was applied. Post-commit visibility or repair
failure preserves a committed/pending outcome and its receipt, including raw
non-idempotent transforms. A cancelled admission sleep cannot re-enter the writer
lane once more before reporting cancellation.

## Qualification

Component Debug receipts record the native, C API, ownership/fault and SDK gates.
Performance acceptance requires the unchanged importer, retained logical-field
parity, active namespace visibility, contended SQL/CLI readers, matched synchronous
text visibility, and sustained active-write throughput and storage bounds. Idle
shrink alone does not qualify reclamation. Retained reference receipts are reused;
reference builds and workloads are not rerun for each candidate.

Final optimized host results are recorded separately from Debug diagnostics and
from externally owned aggregate CI and full factory qualification. Successful
case databases are deleted after their dependent checks; receipts and hashes are
retained rather than a database archive for every run.

The optimized `66f0ff9d54` candidate failed live namespace visibility despite
preserving acknowledged writes and passing integrity/reopen checks. Local
reproduction exposed foreground segment fan-out bypassing replay's capacity
limit and capacity refusal skipping the merge work needed to recover. The new
capacity handoff passes deterministic lifecycle regressions and a 120-second
Debug diagnostic with 45 idle namespaces; this does not qualify optimized
end-to-end throughput. Final optimized qualification must run on the corrected
candidate before the PR is ready.

The [capacity handoff receipt](replay-capacity-debug.json) records the regressions,
full Debug gates and diagnostic limits. The
[artifact primary-admission receipt](artifact-primary-admission-debug.json)
records a reproduced stale checkpoint during repair-summary lookup, the corrected
phase boundary, thirty repeated setup checks and the updated C API/SDK gates.

The subsequent optimized candidate passed live namespace visibility, but its
synchronous indexed churn found another committed/pending stall. The native
publication handoff did not cover the generic endpoint-cleanup completion hook,
which still waited for current index progress under foreground admission. That
hook now joins the owned outside-admission completion path, including cleanup's
accepted replay cut. The [publication visibility Debug receipt](publication-visibility-debug.json)
records local reproductions, operation counts and correctness checks. These
local diagnostics do not establish optimized importer or contended throughput.

The next final-host run passed namespace, SQL and matched synchronous-write
checks, then hit replay admission Busy on the eleventh importer input. Read-only
integrity passed; one mixed-index namespace retained a full text-capacity window
and most of the file-wide replay budget. The
[capacity-restoration receipt](replay-capacity-resume-debug.json) records an
independently reproduced missing source wake after successful merge publication,
its fix and the local gates. Corrected-candidate end-to-end qualification remains
required; local completion on a different host does not override that failure.

Component receipts: [native namespaces](direct-native-namespaces-debug.json),
[owned publication](owned-native-publication-debug.json),
[publication service](owned-publication-service-debug.json),
[primary content](owned-primary-content-debug.json),
[consumer progress](publication-progress-debug.json),
[posting windows](normalized-posting-tail-debug.json),
[physical service](physical-service-debug.json),
[owned relational rows](owned-relational-rows-debug.json),
[prepared merges](prepared-merge-publication-debug.json), and
[shared SQL authority](shared-sql-authority-debug.json).
