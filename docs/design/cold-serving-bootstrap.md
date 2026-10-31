# Cold serving plans and durable native bootstrap

This extends #1278 and draft #1320. `RESIDENT_COLD_PIPELINE=true` selects
compressed inline bootstrap headers, native cohort-wide dependency discovery,
combined metadata projections and byte-based dependency admission.
`RESIDENT_DURABLE_BOOTSTRAP=true` additionally selects a new, incompatible
`serialized-owner-v3` namespace protocol. Both remain opt-in. This is an
implementation design, not a declaration that the latency or hosted gates pass.

## Native cold read plan

Publication preparation compresses the existing bounded manifest/facts-root
bootstrap together with native publication-emitted small routing headers and includes its metadata key in the native inline read set. The
ordinary journal/checkpoint commit owns that copy; publication does not add a
mutable R2 pointer write. The existing 12 KiB root and 16 KiB journal frame
limits still apply. Inline rows may be evicted; absence from the inline capsule
means an ordinary tree lookup, never absence of the actual metadata value.

Point admission projects migration state, publication HEAD, WAL HEAD, GC floor
and bootstrap in one bounded native read. The new point path does not also load
the redundant immutable read-hints row. Manifest/root bootstrap bytes still pass
native content validation. Compression is bounded on both encoding and decoded
size; invalid compressed data fails rather than being silently trusted.

Native cohort discovery continues across members when an immutable dependency
is missing. The host records the native-emitted exact descriptors, deduplicates
them and loads the complete currently discoverable frontier. It then resumes
against the original captured view. Unknown child dependencies require another
frontier; the host never guesses keys or treats unknown metadata as absence.
Visibility errors remain per member; integrity and resource failures fail the
operation. Bodies and oversized results retain the existing individual path.

Physical response admission has the previous two-transcript aggregate budget:
200 KiB, now charged by response size rather than two fixed permits. Scheduling
uses bounded request-owned timer/data tickets, fairness between namespaces and
aging for larger requests. At most four loader tasks per query run concurrently.
Retained transcripts reserve up to 1 MiB per query from the existing global
8 MiB input budget, leaving at least 4 MiB for writers. Descriptor/JSON overhead
and response copies are charged. The native role ceiling remains 64 MiB;
per-query operation, transfer, deadline and lease limits are unchanged.

Logical timeout keeps the physical work's reservations until settlement. Late
resource errors remain observable. Shared I/O promises and R2 handles are never
handed to another request. New metrics distinguish graph waves/nodes, active
response bytes, queued jobs and retained input reservations.

## Provider-local durable read state

The v3 owner persists one provider-local opaque record using the Durable Object
storage API. Antfly still owns metadata trees, WALs, indexes, receipts and capsule
encoding. Capsule import/export run in the 8 MiB observation module, preserving write-module capacity. The actual module capability is checked before activation and on each codec call. No application SQL schema is introduced. The provider's storage
implementation is an additional durable tier; this mode is not R2-only durability
and does not establish that historical platform SQLITE_NOMEM is resolved.

A native capsule binds namespace, bucket, prefix, provider actor scope and native
metadata state. It carries a checksum and validated native root. The checksum
is corruption detection, not a freshness proof. The provider-local commit
protocol grants read admission. Import restores no append certificate, MAC key,
clock anchor or acquired writer flag. An explicit module capability must exist
before the adapter can activate, avoiding a partial write on incompatible WASM.

The record states are:

1. **Dirty intent:** persisted and explicitly synchronized before any native
   operation that can advance accepted metadata, including ownership recovery.
2. **R2 committed:** native durable metadata finalization succeeds. R2 acceptance
   and idempotency receipts remain unchanged.
3. **Acknowledgment:** the application write returns after native R2 commit.
   A confirmed dirty interval coalesces subsequent intents without another local
   write. No optional clean checkpoint is on this acknowledgment path.
4. **Clean capsule:** after one quiet second, native attempts to export confirmed
   state on the coordinator's existing serialized maintenance queue. Export uses
   try-only admission to the 8 MiB observation lane at maintenance priority. Busy
   capacity defers immediately; it cannot retain the metadata queue while waiting
   for a slot needed by foreground preparation/finalization. The provider record
   is persisted and synchronized before a clean checkpoint is confirmed.
5. **Frozen:** new admission stops, queued work and outstanding I/O drain, then a
   durable blocked record is installed before namespace retirement/migration.

A crash before a confirmed clean checkpoint leaves dirty intent and requires
native R2 recovery. A checkpoint failure cannot revoke an already accepted R2
write; it remains visible in diagnostics and invalidates qualification. The
scheduler does not retry checkpoint exceptions automatically, and busy admission
is bounded to four checks within ten seconds. A future write can schedule another
attempt. This is a best-effort accelerator: exhausted busy checks can leave a
dirty record until another write or explicit maintenance attempt. A crash in
that interval takes the ordinary R2 recovery path. The owner never installs a
clean checkpoint concurrently with a write. Scheduling
tracks a host-local dirty generation, so ownership/checkpoint work that leaves
the native metadata revision unchanged can still become clean after quiescence.

Before every local PUT/sync, the in-memory confirmed-dirty latch is cleared.
Only successful synchronization confirms it. This matters when a clean PUT took
effect but its sync failed: the next write must persist dirty intent again, even
though the last confirmed in-memory record was dirty. A lost clean acknowledgment
is safe because the capsule only contains state already committed in R2. Corrupt
capsules are marked dirty before fallback; resource failures are not treated as
corruption. Mandatory dirty durability is checked against the original operation
deadline before R2 advancement. Checkpoints have their own bounded deadline.
Freeze drains pending checkpoints before installing the blocked record.

A clean physical restart can serve its confirmed snapshot while first-write
admission is pending. The snapshot is immutable and linearizes before the
concurrent write; uncertain state is invalidated. The first writer persists a
new intent, then may reuse the clean capsule's native identity/cursor to obtain
**fresh native admission and a new proof**. This avoids unnecessary takeover
seals when no accepted-prefix PUT was outstanding. Dirty reconstruction uses a
new identity and ordinary takeover/fencing. The adapter never marks an imported
capsule as an acquired writer or reuses its prior proof.

## Authority and migration contract

V3 authority is immutable provider-local namespace configuration, with a matching
R2 descriptor and `authority_storage: provider-native-v1`. This removes the R2
authority GET on clean restart. New namespaces first create/validate the R2
configuration and durably adopt it locally. Cached configuration is bound to the
namespace, storage ID, actual actor ID and collector policy. Non-owner observers
recognize the protocol and route admission through the owner; they cannot adopt
its durable storage capability.

V1/v2 adapters and incompatible flag combinations reject v3. Disabling the flag
is not rollback. Retirement must first call the owner's private freeze operation
and drain active work before changing the R2 descriptor. Out-of-band edits to an
active v3 descriptor are outside the protocol. The DATABASE binding and its
logical storage identity are also immutable: rebinding an R2 bucket requires
retirement and a new namespace/storage ID. The adapter cannot independently
identify a different physical bucket behind the same logical binding. No in-place migration or unfreeze
tool is provided; rollout uses a fresh compatible namespace. Native capsule
primitives can support other providers only when their adapters supply an
explicit equivalent exclusive-owner/durability contract.

## Qualification

Keep physically cold object reads, clean Worker reconstruction, dirty recovery,
warmed reads and first writes separate. At 50 ms injected mandatory GET latency,
a 47 ms truly cold target is impossible. This tier removes metadata dependencies;
it cannot make a missing data block arrive faster than its storage tier.

Qualification must include crashes around both local synchronizations and R2
commit, expired proofs, wrong scopes, corrupt capsules, delayed old-owner PUTs,
concurrent readers during first-write admission, both collectors, large bodies,
namespace freeze, and original role/input/I/O bounds. Measure write p95 as well
as cold and warm read p95. Use both execution orders and the approved fixed
control for hosted qualification; current-module local comparisons are design
experiments. Do not reinterpret warm local-cache results as physically cold data.

## Dense-load design correction

The synchronous prototype at `546a104e9a` passed its focused correctness suite
and the first 48-case local matrix, but failed the denser 512-read/128-write run
with `database is busy` (102 failed offered operations; a repeated diagnostic
run failed 46). Those reports are retained under `cold-v4-final-dense32-forward`
and `cold-v4-dense-diagnostic-2`. Instrumented provider get/put/sync calls did not
attribute that error to local capsule storage. It would be incorrect to call
the platform error resolved based on that evidence.

The corrected design removes synchronous per-write clean exports and durability
barriers. Dirty intent and R2 acknowledgment remain mandatory; clean checkpoints
are a coalesced recovery accelerator. Dense offered-load runs, both execution
orders, recovered-error histories and actual-workerd restart tests are required
to evaluate this correction. Earlier synchronous measurements remain historical
and must not be combined with corrected measurements as a single passing run.

The provider storage contract requires ordered writes and a durability barrier
under the exclusive owner. Cloudflare documents [storage output gates](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
and the [storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
Native capsules do not manufacture those guarantees for another provider.
