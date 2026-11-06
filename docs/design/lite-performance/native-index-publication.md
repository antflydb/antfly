# Native index publication

The Lite index LSM is a logical index-state machine inside the native file. It
must not become an independent physical durability domain. An LSM WAL record is
still useful as a recovery encoding; writing that record, installing its
immutable inputs and publishing its replay continuation must share one native
checkpoint.

## Ownership and commit boundary

Preparation owns immutable artifact bytes, deletion bitmaps and a prepared
replacement snapshot. Large artifacts use bounded unpublished construction
leases, so staging does not monopolize writer admission. Artifacts up to 256 KiB
stay in provider-owned memory, independent of request scratch, and join the
activation transaction directly. Larger artifacts use a construction lease that protects
the immutable reader before a public alias exists.

Under the checked final writer admission, a publication validates the captured
index generation, fragment identity, input fingerprint and segment-ID frontier.
It allocates replacement readers and memory before attempting durability. A
borrowed participant travels explicitly through the namespace transaction and
LSM WAL append. The native storage host stages that participant's artifact links
and the encoded WAL append in one native transaction. The participant has no
commit, abort or in-memory activation capability. Unsupported storage providers
refuse this operation; a sequence of independent writes is not a fallback.

WAL directory initialization, rotation and pressure relief do not receive this
participant. The participant belongs only to the actual activation record. LSM
memtable activation follows successful native publication; index snapshot
activation follows successful LSM commit. Every artifact needed by a committed
activation therefore exists in the same durable checkpoint.

A prepared replacement guards new retained snapshot captures across the external
commit and in-memory activation. The native participant arms this guard after
LSM pressure relief and candidate preparation. It releases it after installing the
successor pointer, or on abort after the owner has classified/fenced the failed
publication. This prevents newly visible primary progress from authorizing a
new capture of the old in-memory index. Previously retained snapshots continue
to represent their original cut. Contended capture waits yield the CPU.

## Index-specific admission

A table-wide index identity is too broad for detached text work: eager graph
publication can change it after every text preparation, even though no text
input changed. Revision 12 therefore gives each `/indexes/<name>` domain a
random durable mutation identity. Its bounded hash key lives outside the virtual
file namespace. File writes, appends, deletes and both domains of a cross-index
rename stage their identities in the same native transaction as their effects.
An aborted transaction restores both; unchanged inline file writes do not
advance the identity or create another checkpoint.

The replay publication port names its target index. The maintenance turn captures
owned identities before releasing admission and compares the selected identity
against the current inode under its final writer lease. Catalog, namespace,
incarnation and recovery validation still apply, including after allocator-only
admission has already adopted a newer header. Changes to other indexes still
invalidate whole-runtime cache reuse and unscoped operations; they no longer
invalidate this prepared text publication. The selected witness is refreshed
before releasing the lease, so later pieces recognize the worker's own writes.
A witness-read failure after commit invalidates future reuse rather than
returning a retryable error for a committed publication.

The signature change excludes writers that do not maintain these identities.
Older file revisions keep conservative admission until writable-owner migration.
This is a scoped publication fix, not a replacement for validating merge inputs
or for qualifying sustained mixed-index traffic. The [Debug correctness receipt](scoped-replay-admission-debug.json)
records the full native/C API results and remaining qualification. Local regressions inject a
foreign index mutation before each text publication, retain target-replacement
refusal, and verify durable fragment progress across runtime replacement.

## Refusal and uncertainty

A prepublication failure aborts all staged links and the WAL append. Prepared
immutable inputs remain available for another attempt. Alias collisions never
permit overwriting or deleting a foreign artifact. A later attempt may allocate
another numerical segment ID.

A publication error that may have committed fences the native owner and its
serving wrappers. The old in-memory snapshot remains active, but must not be
used to claim the new request failed before commit. A fresh owner reconciles the
checkpoint and WAL. Ordinary file-WAL tail repair must not truncate this atomic
native publication: it could remove activation state from a successful commit.

## Primary progress

The physical activation includes its exact fragment continuation. A partial
fragment cannot advance the coarse primary watermark. Joining coarse progress
requires proving that every preceding fragment and required consumer effect is
complete, preserving the catalog identity and revocation rules used by existing
projection metadata. Advisory status must describe the prepared successor, not
the old in-memory snapshot. The implementation joins complete-fragment primary
progress and its physical index seal through a namespace-scoped metadata participant. It rechecks the
captured authority, including epoch and catalog digest, inside the native
transaction and uses the existing progress/revocation rules. A later coarse
flush skips progress already covered by that atomic publication. Diagnostic
status remains advisory and is refreshed by the separate status service.
These changes still require correctness and performance qualification.

## Verification

Require one native checkpoint for an already initialized multi-artifact
activation, preserved old snapshots, deletion/overwrite correctness, exact
fragment resume after reopening, collision safety, participant-stage rollback,
allocation failure and native durability failures. An uncertain commit must
recover complete activation state and preserve its artifact leases. Existing
WAL crash models and native allocator/reclamation checks remain required.

Operation counts are local feedback. Final throughput, tail latency, active
storage plateau, strict index visibility and unchanged-importer contention are
qualified separately on wolfspider using the retained baseline receipts.

## Explicit drains and finite replay credit

Explicit native drains use bounded producer, independent-consumer and dependent-
consumer turns, followed by physical retirement after releasing source reads.
The connection rotates all namespaces sharing the native replay ledger before
revisiting one namespace. It retains the original producer interval when auditing
terminal failures, so an advanced cursor cannot hide an earlier failed request.
Strict idle finalization follows the bounded service round. No quota or visibility
timeout is raised to permit progress.

The local regression previously retained 13,282 bytes of a 13,425-byte consumer
allowance after two output publications, with producer progress still zero. The
next output could not fit while the producer held the drain. Interleaved service
now finishes that fixture and verifies physical reclamation between outputs.

Explicit drains and compaction must not wait for a detached replay continuation
while holding its writer publication gate. A refused continuation borrow unwinds
all runtime-local work, releases admission, and yields before reacquiring and
refreshing the foreground runtime. Durable progress remains authoritative across
that handoff; callers never replay a document mutation to resolve this wait.

A fair explicit round also advances pending graph replacement construction and
cutover using their existing finite work budgets. Merely reporting a pending
replacement would prevent idle finalization from ever servicing it. Background
scheduling retains blocked resolver debt for a later wake; explicit drains finish
after runnable consumers settle and leave the blocked outcome visible. The
configured stuck-index diagnostic is checked between explicit rounds as well.

Memory-backed artifacts use the same resource-accounted decoded-lease contract as
native range sources. Resident bytes reserve host capacity before allocation;
a declined reservation selects bounded file staging. Decoded leases independently
retain the immutable bytes and their resource owner after the runtime closes.
Optional cache rejection refunds metadata before the normal decoding fallback.

## Producer completion and committed-effect progress

A producer's source watermark and an index's committed-effect watermark answer
separate questions. A multi-document source record can produce several output
publications before the producer completes that record. Each output has its own
durable journal sequence. Index replay must consume those committed effects
without waiting for the unfinished source record; otherwise finite output
credits can form a circular wait. A synchronized visibility request first fences
source completion, then captures and fences the resulting publication cut.
Advancing an index watermark alone never proves future producer output complete.

Native retirement uses a durable cut for each hint lane. Multiple indexes of the
same kind contribute their minimum cut. Enrichment, resolution and promotion
retain their own input lanes. Repair and graph-replacement readers retain all
lanes, and failed catalog loads authorize no reclamation. A canonical payload
can disappear only after every relevant consumer acknowledges it and every
reference is physically deleted, including deletions in the same transaction.
Legacy records without explicit hints retain the conservative uniform minimum.
Continuation state becomes clean only for the exact set of cuts it examined.

The integrated retry workset writes `ESQ4`, with explicit empty-document and
fragment cursor fields. Its decoder accepts the earlier `ESQ2` header and both
unreleased `ESQ3` layouts using exact lengths and bounded slot counts. Empty-key
and fragment cursors cannot be claimed simultaneously. This preserves main's
opaque empty-document IDs together with authenticated seekable discovery.

## Immutable inputs across admission yields

A changed foreground tip can refuse artifact publication after source replay
completed successfully. That refusal invalidates mutable actors and captured
publication authority. It does not invalidate immutable bytes protected by a
live durable value lease. Before closing the refused runtime, preserve its
immutable inputs under the existing file-wide entry, byte, range and time bounds.
No primary epoch or mutable DB state is retained by this handoff. Exact current
path/inode/value identity is checked again before any pooled source is reused.
An uncertain outcome on either the detached or serving owner clears the handoff.
Shutdown does not extend it. Source and decoded-lease resource owners continue
to outlive any retained readers independently of the closed runtime.


## Direct native namespace stage

Hosted Native providers can open transactional namespaces directly in the native
catalog. New persistent text main stores and logical WALs consume this capability;
filesystem, memory and existing LSM roots keep their previous backend. A namespace
layout marker is published with its first mutations. No empty native root may
supersede existing LSM metadata. Native revision 13 fences older binaries before
they could mistake these namespaces for empty LSMs.

Namespace keys preserve byte ordering through bounded-component hex encoding.
This keeps arbitrary binary names and keys compatible with generation manifests
and repair without making the virtual filesystem responsible for the transaction.
All main metadata, artifact activation and joined projection progress still share
one native checkpoint. Prepared replacements can release the writer mutex before
admission; binding checks the retained base and each deletion revision. Aborting a
detached replacement does not dereference its former writer.

Immutable leased sources keep their existing bounded handoff pool across runtime
replacement. C API native visibility acknowledgements notify bounded retirement
service; physical deletion remains the condition for returning replay credits.

The direct backend is an architectural prerequisite, not evidence that the
primary/index durability gap has closed. An intermediate 128-update diagnostic
still recorded 544 foreground syncs. That receipt predates the checked one-sync
protocol and typed semantic retirement, described in
[native-commit-proofs.md](native-commit-proofs.md). Combining foreground primary
and index publication and batching posting deltas remain outstanding. The direct
namespace stage itself makes no end-to-end or optimized performance claim.

Current-stage Debug validation and explicit qualification limits are recorded in
[direct-native-namespaces-debug.json](direct-native-namespaces-debug.json).

## Prepared namespace participation in primary publication

A namespace write transaction can detach into an owned prepared publication.
Detachment refuses live cursors, materializes the write descriptors and successor
generation before admission, and closes the metadata snapshot before the writer
queue. Its retained backend and IO owner remain alive until the prepared handle
is released. It has staging authority; it cannot commit or activate a runtime.

The primary native transaction accepts this capability independently of its
restricted document-metadata observer. The native host validates inode, file
namespace and the exact captured namespace generation, then stages the namespace
and artifact changes beside the primary write set. An injected staging error
rolls the entire transaction back. A stale namespace witness refuses publication
without leaving the primary changes committed.

Persistent text publication also accepts a synchronous primary committer. Its
namespace witness is captured before reading the segment frontier and fragment
position, and retained throughout preparation. The caller publishes primary
changes, index metadata, immutable aliases and progress in one checkpoint.
Prepared reader activation follows durable success; uncertain primary outcomes
propagate the group's captured decision back to the originating caller and
fence the serving owner. An already applied replay fragment cannot silently
skip an attached primary commit.

This is the shared publication mechanism. Foreground callers still need to
supply their text plans before admission and use this mechanism; merely exposing
the capability does not eliminate the remaining separate primary/index commits.
Commit cohorts must compose same-index plans before staging. Queuing separate
prepared replacements that each hold the same writer mutex through durability
would deadlock a shared group and is not a supported coalescing strategy.

Hosted graph forward/reverse stores and sparse metadata now select the direct
native namespace provider for new roots. Existing LSM roots retain their
existing authority. Scalar namespace views retain read snapshots across fork
and owner shutdown, preserve ordered binary keys, and keep transaction-wide
write serialization explicit. Their native transactions provide durability;
there is no inner manifest or WAL to sync or maintain.

Revision 14 fences the additional native graph, sparse and HBC metadata roots.
Revision-13 namespace data remains readable, and writable owners migrate through
the existing generation-replacement protocol before publishing new namespace
mutations. A version-13 binary must not open a new native graph root as an empty
LSM. Existing logical-identity and allocator/recovery encodings remain intact.

Ordinary native text segment insertion and deletion use the same prepared
activation machinery as replay. Small artifacts, active metadata and immutable
replacement snapshots publish in one native checkpoint. Large unpublished
construction remains bounded. Ordinary mutations leave the replay fragment
unchanged. Deletion prepares private bitmap replacements before durability,
instead of exposing an in-place tombstone and attempting to undo it on failure.


## Bounded resident sources during reopen

Small native text artifacts retain the same owned-memory policy on cold/runtime
reopen as during publication. The optional resident-source capability copies at
most 256 KiB under a short logical read pin, hashes the owned bytes and releases
the pin. The resulting immutable source and decoded-cache leases retain their
resource/IO owner independently of DB and Store lifetime. They need no durable
alias, marker, inode fence or retained native extent.

Resident allocation reserves memory before copying. Size or optional residency
refusal selects the established range-backed source; corruption, cancellation
and operational read errors propagate. Large artifacts keep bounded native ranges
and their exact leases. Providers can expose either capability independently;
the generic mapped/heap fallback remains available when both are absent.

This removes durable lease publications for small artifacts when maintenance
reconstructs a runtime. It does not remove the need to bind primary and index
updates together or to compose posting deltas before freezing segments.

## Owned text builds and grouped native activation

Native text projection plans now retain an independent build-resource capability.
It owns the IO service and ResourceManager lifetime, an analysis-cache reference,
and deep copies of analyzer, sort and spool configuration. Building a captured
window requires no live DB, index entry, Store or primary snapshot. A regression
closes all those owners before building multilingual postings. Foreign providers
without this optional capability keep their existing fenced build path.

`PersistentIndex.prepareNativePublication` separates namespace, artifact and
replacement-snapshot preparation from the primary commit. Its handle closes the
native metadata transaction and detaches the writer snapshot before returning.
The caller retains index lifecycle admission and local storage serialization;
multiple indexes must be acquired in consistent catalog order. This is a
synchronous publication capability, not a lifetime-independent cached runtime.

`commitNativePublications` stages all handles through one primary committer.
Exact namespace witnesses and detached snapshot/deletion revisions are checked
inside the host transaction. Failure activates none of the indexes. Uncertainty
fences every possibly affected index and preserves the host's outcome. After
success, all replacement snapshots are installed before any external reader
fence is released. Both activation phases are allocation-free and infallible.
Existing replay-with-primary publication uses this path too.

A two-index regression covers injected failure, a foreign mutation to the
second namespace, and successful primary/index publication in one checkpoint.
An independent index test checks the intermediate activation state: after the
first snapshot is installed, readers of both indexes remain fenced until the
second installation completes.

Foreground primary/text routing and an ordered same-index request cohort remain
separate integration work. This API intentionally refuses duplicate target
indexes in one group: logical effects for a shared index must be composed before
preparing its single replacement. It does not make separately prepared stale
replacements composable, nor does it establish end-to-end performance.


Resident sources also participate in bounded handoff. An allocation-free weak
pool holds at most 128 entries and owns no references itself. Reuse requires the
same database path, digest, length and full content comparison; native page
addresses are insufficient because resident copies retain no page leases.
Registration and final release share a mutex, preventing resurrection after the
last reference. A file owner's working set may retain these copies alongside
leased ranges within its existing memory/count/expiry limits. Resident copies
add no physical range charge. Changing bytes cannot reuse an old decoded cache,
including under an artificially forced digest collision.


Detached builds require an owned resource lifetime as well as copied config.
The native backend explicitly marks managers it transfers to the registry on
close. A borrowed external ResourceManager cannot establish that guarantee, so
its stager declines detached building and retains the fenced compatibility path.
Positive and negative ownership regressions cover both cases.

When a private merge exhausts its build budget, recording a smaller merge width
also makes that namespace's artifact debt runnable and wakes the owner. The
owner applies a 100 ms retry delay, so persistent pressure cannot cause a
zero-delay worker loop. Progress no longer depends on a later write or an idle
poll rediscovering the debt. The regression checks both explicit scheduling and
the bounded delay before verifying the smaller retry.

Exact-commit Debug validation for this stage is recorded in
[owned-native-publication-debug.json](owned-native-publication-debug.json).
It qualifies the listed correctness and ownership checks; foreground integration
and final end-to-end performance qualification are still pending.

### Native document identity binding

Revision 16 separates the authoritative ordinal column from immutable posting
bytes. `native_publication_value.Value` owns a fixed address and length. Its
producer initializes the buffer before preparing namespace mutations, then seals
it with release ordering after primary identity assignment. Reads and native
namespace staging require the matching acquire-ordered seal. An unbound buffer
fails before host publication; it never produces a retryable post-commit error.

The `AFTSADM2` active marker contains the segment ID, physical length, exact AFSM
footer, optional `AFTSADM1` navigation and mandatory ordinal column, protected by
an outer checksum. Large navigation falls back to source admission while keeping
the identity column. Readers accept legacy inline ordinals. Scalar lookup,
ordinal cursors, query-bound readers and compaction all prefer the native binding.
Deletion-only replacements retain the column and republish it with their bitmap.
Compaction can fold it back into an ordinary inline column.

`prepareNativeTextSegment` retains owned preparation resources and copies keys in
physical segment order. `prepareNativePublication` allocates the final metadata
buffer and replacement snapshot before native writer admission. The owner binds
its authoritative ordinals before `commitNativePublications`. Primary changes,
metadata, activation and all participating indexes then share the host commit.
The ordinary document batch and SQL paths still need to consume this prepared
foreground capability; this component alone does not unify those entry points
or implement publication cohorts.

Publication values retain the native resource owner and charge their complete
buffer/state reservation until their final snapshot reference disappears.
Decoded columns retain the actual source accounting owner, including sources
reused from retired runtimes. Namespace cursor snapshots retain their own value
references; overwriting a pending key cannot invalidate an existing cursor.
The namespace's inode, file namespace and generation witness remains exact even
when the value is bound later.
