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
encoding. No application SQL schema is introduced. The provider's storage
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
3. **Clean capsule:** native exports the confirmed state; the provider record is
   persisted and synchronized before the application write is acknowledged.
4. **Frozen:** new admission stops, queued work and outstanding I/O drain, then a
   durable blocked record is installed before namespace retirement/migration.

A crash before step 3 leaves dirty intent and requires native R2 recovery.
Failure to persist clean state never produces a successful ACK. If a clean
record was actually durable despite a lost local acknowledgement, its state is
already R2 committed and remains safe to read. Corrupt capsules are marked dirty
before fallback; resource failures are not hidden as corruption. Local durability
remains on the original operation's wall/monotonic deadline.

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
active v3 descriptor are outside the protocol. No in-place migration or unfreeze
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
