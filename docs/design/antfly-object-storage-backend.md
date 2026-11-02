# Antfly object-store durability and recovery

One native Antfly engine owns metadata, accepted WAL progress, receipts and collection.
R2/S3/GCS provide typed conditional object operations; host adapters provide execution,
clocks, cancellation and credentials. See [architecture](cloudflare-serverless.md)
for serving and [qualification](cloudflare-independent-http.md) for acceptance.

## Transactional metadata and commit

The shared transactional store combines authenticated ordered pages, an immutable
commit log, conditional roots and bounded checkpoints. A transaction domain binds
provider, bucket, prefix, namespace and incarnation. Page references and compact
physical locators are validated relative to that domain. A point lookup or changed
path must not load the entire database. Root hints and inline rows are disposable
accelerators, never evidence that an authoritative value is absent.

Preparation can produce immutable objects before admission. Acceptance requires a
conditional native commit under current ownership; losing uploads remain unreachable.
A successful keyed write retains its payload hash and receipt atomically with progress.
Replaying an operation ID with a different payload fails. Resolve ambiguous successful
responses through durable receipts rather than blindly appending again. Group commit
retains every member's receipt and uses the original byte/record/queue limits.

Ownership takeover, append certificates, seals and root revision checks fence stale
writers, including late PUT/ABA cases. Read validation must follow authenticated
committed prefixes, not trust an old root or cached absence. Checkpoint repair must
prove the covered committed state; it cannot skip an unverified accepted interval.
Deadline expiry after a successful durable publication cannot undo that publication.

## Indexed WAL and format selection

The indexed WAL stores LSN-to-canonical-record and operation-ID-to-receipt trees,
with an optional document-point projection. A single HEAD CAS commits their roots,
watermarks, floor and upload epoch. Captured ranges seek directly and validate a
contiguous prefix with record, byte, deadline and cancellation bounds. Retention
never resets the accepted high watermark or forgets replay receipts.

An immutable create-only `.antfly/WAL_FORMAT` marker serializes compatible layout
selection. Indexed and legacy formats are explicit choices. An unmarked prefix
requires conservative inventory; a marker is not protection against an old writer
that ignores it. Use a fresh prefix/coordinated export-import when compatibility
cannot be established. HEAD version four and managed reader policy cannot be
rolled back by disabling a feature flag.

## Read lifetime and physical completion

Published manifest leases, captured WAL lifetimes and execution slots are separate
resources. Acquire authority/proofs before scarce execution where possible, then
run against the original captured view and fence. Immutable cached state does not
authorize dereferencing an expired root. Owned decoded values may outlive a root;
lazy references may not.

Every operation carries its original suspend-inclusive deadline and cancellation.
Check it around asynchronous page operations and before returning results. Logical
cancellation does not release buffers, slots, input bytes or writer reservations
while physical I/O remains active. Join every dispatched peer, drain obsolete
speculation and retain late failures. A retry or later successful response cannot
erase a resource error or reinterpret an ambiguous commit as rejected.

## Publication, collection and receipts

Publication consumes a bounded contiguous accepted prefix under native work leases,
commits immutable artifacts and a fenced manifest, and resumes after eviction.
Enrichment/reindex progress is durable; stale model results cannot overwrite newer
source state. A stable compaction operation outcome commits with HEAD and lease
release, so Queue redelivery returns the same result. Admission scheduling and
receipt persistence are separate: a failed scheduling call cannot erase acceptance.

Both metadata and WAL collectors persist frontier, mark membership, phase and sweep
cursor in native transactional storage. A JavaScript map is not durable progress.
Protect current, retained, receipt, document, pending-publication and reader roots.
Never delete a whole prefix because the local process considers it idle.

Managed indexed WAL activation requires a pristine compatible location, including
historical-version inventory. Managed/non-managed and empty/nonempty-prefix markers
are distinct. A managed HEAD persists the 30-second maximum root-use lifetime even
when GC is locally disabled. Older readers must reject this format. A new collection
job atomically advances the epoch and captures record, receipt and document roots
in HEAD; there is no second mutable `GC_CURRENT` publication.

Bind every job operation to its ID and retired epoch. Resume the current durable job;
never recreate a stale cached one after another finishes. New writes use the new
epoch. Sweep only strictly older epochs after tracing authenticated roots with bounded
partial-node continuations. Before deletion, wait 60 seconds of local suspend-inclusive
elapsed time after observing the committed barrier; restart/takeover waits the entire
grace again. Cross-host wall timestamps cannot authorize deletion. Clock failure or
rollback fails closed. Complete the job durably before releasing its HEAD pointer;
a lost finish response is reconciled with fresh status.

## Provider-local durable bootstrap

`serialized-owner-v3` binds immutable authority configuration to the actual provider
actor, namespace, storage ID, bucket/prefix and collector policy, with a matching R2
descriptor. Only that exclusive owner may supply its local storage capability.
Non-owner observers route admission through it. V1/v2 and incompatible modules reject
v3; disabling the flag is not rollback.

The provider stores one opaque Antfly capsule via its storage API, without an
application SQL schema. Import checks codec capability, native roots, scope and
checksum. A checksum detects corruption, not freshness. Import restores no acquired
writer flag, append proof, secret MAC material or clock anchor.

1. Persist and synchronize **dirty intent** before metadata can advance, including
   ownership recovery. A confirmed dirty interval can coalesce later intents.
2. Perform the ordinary native **R2 commit** and receipt publication.
3. Return the application **acknowledgment** only after that durable commit.
4. After a quiet interval, attempt a **clean capsule** on the serialized maintenance
   lane using try-only observation capacity. Busy capacity defers without holding
   the queue behind foreground work. Persist and synchronize the checkpoint before
   confirming it clean. An optional checkpoint is not on the write ACK path.
5. For retirement/migration, stop admission, drain writes/readers/physical work and
   pending checkpoints, then durably install **blocked/frozen** authority.

Clear the confirmed-dirty latch before each local PUT/sync and set it only after
synchronization. A clean PUT may have taken effect even when its sync failed; the
next writer must persist dirty intent again. Dirty/crashed state recovers through
R2. Corrupt capsules become dirty before fallback; resource failures do not masquerade
as corruption. An optional checkpoint failure remains diagnostic evidence and cannot
revoke a previously committed write. Busy maintenance may remain dirty until later
work; it must not retry exceptions invisibly.

A clean reconstructed owner can serve its immutable snapshot while fresh first-write
admission is pending. The first writer obtains a new native proof; uncertain state is
invalidated. Dirty reconstruction uses ordinary takeover/fencing. A faster authoritative
provider-local write log with asynchronous R2 checkpointing would be a different
acknowledgment, replication and takeover protocol; it is not implemented by this cache.

## Compatibility and migration

The SQLite compatibility source remains available for explicit frozen migration.
Native target initialization, queries, publication, collectors, reindex and reopen
must work with application SQL/KV access guarded to throw. Erasing platform state
must not hide accepted native R2 data. Migration preserves receipts, LSNs, retention,
visibility and physical identity through fenced cutover; changing a backend variable
is not migration. Keep compatible writers and collectors together during rollout.

R2 lacks atomic conditional DELETE in the host contract, so unsupported operations
fail closed. S3 ETags and GCS generations remain typed. Provider transport correctness,
managed-GC recovery and actual-provider performance require separate evidence.
