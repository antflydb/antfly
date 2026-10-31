# Bounded serving dependencies

The next serving experiments target storage dependency depth and occupied native
slots. They build on the existing read/write plans, observation continuations,
packed canonical facts, checkpoint repair and scoped immutable caches. They do
not increase native memory, cache allowances, certificate lifetimes or gate
thresholds. All five switches are disabled by default and sealed in qualification
provenance. Native storage primitives apply through the common object-store
client; hosted scheduling is integrated in the Cloudflare adapter. Local tests
are not live R2/S3/GCS latency qualifications.

## Point-read dependencies

`NATIVE_POINT_CONTINUATIONS=true` enables a capability-negotiated request-owned
immutable read transcript. Native `antfly_host_read_dependency_abi` advertises
version 1. The shared WASM host status 10 means `ImmutableReadDependency`; an
explicit lookup-only envelope converts it to an internal yield result after
native defers unwind. Mutations cannot enter this protocol. Older modules omit
the new field and use ordinary I/O.

Only exact artifact, immutable manifest and WAL-page GETs can yield. Ranges and
conditions are part of the exact descriptor identity. Mutable metadata is not
recorded. Responses have bounded bodies and headers; only typed not-found is
recorded as absence. Resource, transport and integrity failures propagate.
The native query revalidates the original admitted view and immutable bytes on
resumption. A transcript carries no root authority, proof, lease or native pointer.
This trades bounded CPU replay for releasing the execution slot during I/O.

A request has at most 64 dependencies, with retained and transient transcript
charges inside the existing proof-prefetch allowance. Charges cover two body
buffers, two headers, descriptor copies and entry overhead. A larger dependency
or exhausted transcript follows the ordinary bounded native path. All native
replays and detached physical reads share the original 1,024-operation / 32 MiB
aggregate allowance; the native read allowance remains separate and unchanged.

Immutable preparation overlaps fresh admission. It returns its one-shot ticket
and host preference to the final query. Before waiting for admission it releases
its transcript buffers and auxiliary permit, so metadata observation can use the
same bounded budget. Completed immutable bytes may remain only in their normal
charged cache. Final execution re-acquires a permit and respects the shorter
original lease deadline. Timeout/cancellation retains that permit and the caller
background lifetime until physical settlement; late errors remain observable.
Intermediate yields are preparation diagnostics, not completed logical reads.

## Reusable immutable preparation

`IMMUTABLE_PREPARED_REUSE=true` allows the shared native prepared cache to retain
complete immutable membership/absence and body results keyed by namespace,
document and exact semantic facts root. It shares the existing four-entry,
256 KiB allocation pool with one-shot tickets. Entries and returned values own
all bytes. Mismatched roots/domains/IDs and eviction are ordinary misses.

Loading the hinted immutable root still validates domain and checksum. Reuse
skips tree traversal and body materialization for a matching root. Final live
session admission, floor/pin checks and fresh WAL overlay remain mandatory; a
new update or tombstone wins over the prepared published result. No live session,
request I/O, clock or authority is retained. Scope changes clear the cache.

## Replay-cost checkpoint scheduling

`REPLAY_COST_CHECKPOINTS=true` coalesces maintenance using committed revision
lag, accumulated metadata bytes and a conservative replay-wave estimate. Eight
revisions, 64 KiB or four estimated waves make repair immediately eligible;
otherwise the initial delay is the remainder of a one-second quiet-age target.
This age is a scheduling target, not a durability or freshness guarantee.

Dirty revisions survive deferrals and failures. Existing bounded retry count,
ten-second attempt budget, native proof/owner checks, conditional publication
and strict collector barrier remain unchanged. Foreground ACKs never await hint
success. A successful captured checkpoint does not erase debt from later commits;
restart conservatively rediscovers debt. This does not promise a bounded floor
under permanent contention or provider failure.

## Small-leaf co-location

`INLINE_FACTS_LEAVES=true`, together with canonical packed facts, publishes
explicit metadata version 14 (`AFDFACTE`). In addition to the existing small
interior directory, a root may embed its complete primary leaf when that leaf is
at most 4 KiB. The existing 832-byte packed header and root size ceiling remain.
Native decoding checks root/source version, domain, leaf length and checksum;
normal page decoding validates the referenced page. Larger pages keep the
existing packed dependency path. Referenced external pages retain ordinary
collection protection; embedding bytes does not relax reachability checks.

This can remove one dependent object read for small tables. It does not flatten
large trees or duplicate unbounded bodies. Version 13 continues rejecting inline
leaves; old readers fail closed on version 14. Roll out capable readers before
enabling writers. Disabling the flag does not rewrite existing roots; rollback
to old binaries requires an explicit compatible full rebuild/migration. There
is no automatic namespace-format rollback.

## Write preparation and refresh

`NATIVE_WRITE_OVERLAP=true` overlaps an expired warm writer's authority refresh
with authority-free immutable write preparation from its captured state. Cold
or recovering writers keep the serial path. The native finalizer always waits
for refreshed authority and checks WAL/head/GC dependencies against current
state. Missing old immutable inputs and dependency conflicts retain bounded
rebuild/retry behavior. ACK still follows the durable commit.

Concurrent phases reserve disjoint portions of the original aggregate I/O
budget before starting. The recovery child receives a charged prefix representing
its unused portion of the fixed native ceiling; preparation sees the parent's
reservation. Joining replaces reserved capacity with actual use and cannot
forgive child overspend. Both lifetimes settle on failure. Conservative
partitioning can reject a larger optional workload earlier; it never increases
capacity or retries arbitrary resource errors as success.

## Measurement and placement

The local matrix isolates each switch and their combination against observation
continuations alone, with identical capacities, two corpus sizes and both
execution orders. Unique-key and explicit hot-read workloads distinguish cache
reuse from cold preparation. Warm and expired-authority cases are separate.
The harness preserves diagnostic snapshots before assertions, so failures retain
measurements without being promoted to correct cases. Per-request phases overlap;
their percentiles must not be added.

Placement is a separate hosted experiment after local candidate selection.
The existing read-only placement probe reports measured RPC/storage latency and
hinted geography separately. Probe calls consume compute identities and cannot
be used as physical-cold qualification. Compare the same application workload,
resource policy, source/modules and diagnostics completeness across placement
variants; a HEAD probe alone cannot establish application improvement.

The hosted gate, complete live diagnostic coverage, historical `SQLITE_NOMEM`,
three paired hour-long trials and physical cold samples remain open. Successful
injected-error lifetime tests do not identify or fix the historical memory error.
