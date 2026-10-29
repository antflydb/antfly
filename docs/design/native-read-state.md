# Shared native read state and point projections

This extends Antfly's object-backed native engine rather than introducing a
Cloudflare database format alongside it. It combines fresh metadata admission,
reusable derived WAL state, an atomic document projection and foreground cache
admission. R2 remains the durable source. Durable Objects supply execution and
sequencing; no new platform storage is required.

## One native admission

The shared transactional object store's `observe_read` operation first performs
the existing committed-root observation (or owned observation for a current
writer). It returns exact requested rows and a matching inline selected record
from that observed root. The Cloudflare adapter requests migration, HEAD, WAL,
GC floor and hint records plus the selected read-pin prefix. Inline rows normally
complete admission in one native call instead of separate observation, metadata
reads and pin reads. Missing inline values use the existing authenticated ordered
tree; an absent selected pin retains the ordinary read/admission fallback.

There is no independently maintained mutable read HEAD and no reusable admission
token. Ownership, checkpoint floors, seal authentication, incomplete recovery and
pin validation retain their existing contracts. The operation cannot execute
mutations. Ordinary read and observation operations remain available to all hosts.

## Bounded derived native state

`query.ReadStateCache` owns up to four immutable overlay entries with a combined
256 KiB charge. Each binds the namespace, published base LSN, fresh WAL identity
and lineage. Cached mutations own their bytes. Request contexts, transports,
resolvers, read pins, authority times and live sessions are never retained.
Each request receives an independent clone with its own body resolver.

A fresh WAL snapshot can reuse an exact cached identity. An append can extend a
cached overlay only when its native-published immediate-parent identity matches
that cached root and LSN in the same lineage. Only the missing contiguous interval
is read and decoded. Missed intermediate commits, epoch changes, changed bases,
historical cutoffs and unsupported providers rebuild through the existing bounded
range path. A fresh WAL floor is checked before cache reuse. Published-generation
changes invalidate the derived overlay; this does not introduce an older-base
retention protocol.

Snapshot identities bind namespace/domain, write epoch, physical record floor,
LSN and the authenticated record-tree reference. Parent identity is part of the
same durable native HEAD update as append, not a claim supplied by a reader.
Parent references are identity material, not liveness edges: future reads do not
fetch their pages. Collectors need not retain historical parent trees.

Small point-read tails with an inline last record can seed this working set
through a probe bounded to 16 records and 64 KiB of encoded input. An explicit
capacity failure selects the document index. A four-entry, root/base/budget-bound
cost-hint set suppresses repeated oversized probes against the same immutable
prefix; it conveys no absence or freshness claim and shares the cache byte cap.
Integrity, expiry, cancellation and transport failures are not converted to misses.

The cache clones before replacement, so failed allocation or WAL extension cannot
partially advance a cached watermark. It uses short native synchronization while
copying/replacing entries and releases its lock before object I/O. All raw reads
and continuations remain owned by the submitting caller. Persistent bytes remain
inside existing kernel memory reservations; replacement scratch is also bounded.
Exact root hits do not replace persistent cache entries. Normal kernel recycling
can discard the entire optional working set.

`WalStore` exposes optional snapshot and point-read callbacks. Providers without
those capabilities continue using bounded range reads; they do not silently
weaken consistency or emulate unbounded replay.

## Opportunistic preparation

With `OPPORTUNISTIC_READ_PREPARE`, immutable point preparation is admitted only
when a compatible physical reservation is immediately available and fresh
authority is still pending. It never waits behind queued work or promotes to a
larger preparation lane. If it is skipped or reaches its small-lane capacity,
the final fresh query proceeds with the remaining original I/O allowance.
In-flight preparation still drains in its submitting context; authority retains
priority while it runs. No additional slot or memory reservation is introduced.
The original detached/barrier protocols remain available for controlled rollout.

## Indexed point reads of unpublished changes

The native indexed WAL can maintain a third authenticated ordered tree:
`document ID -> latest committed record`. It reuses the existing ordered-page
engine. Values contain the LSN and, for encoded records of at most 1,536 bytes,
the record itself. Larger values retain an LSN reference into the ordinary record
tree. Repeated changes within a batch produce one final document-tree mutation.
Deletes retain their native tombstones.

Records, receipts, document projection and parent identity publish through one
conditional WAL HEAD update. Immutable pages upload before this boundary; a
failed CAS exposes none of those tentative roots. Receipt replay retains its
original LSN interval. The native write planner's exact WAL dependency also
covers the projection, so no second coordinator transaction is introduced.

Activation records the first completely indexed LSN. A point read can use the
projection only if its requested interval is completely covered and ends at the
captured latest LSN. Otherwise it uses the ordinary range path. Membership and
absence are authenticated against the captured projection root. Returned records
must match the requested document and interval. A point-only overlay cannot be
used for a different key, scan, count or search. Scans and historical reads keep
the original complete overlay and visibility contracts.

This removes replay proportional to the entire unpublished tail for eligible
point reads. It introduces extra index writes and retained pages; whether it wins
for a given read/write mix must be measured separately from read-state reuse.

## Collection and progress

WAL collection protects record, receipt and document roots under the same epoch
barrier. Prefix compaction deletes a document entry only if its latest LSN is in
the physically removed prefix; a newer replacement or tombstone survives.
Projection pruning and physical record progress commit together. A slice admits
at most the existing record limit and two MiB of indexed-record input (with
bounded single-record progress). Receipts and the high watermark survive cleanup.
No new collector may ignore the document root.

## Cache admission

The shared immutable transport cache supports lookup without population and
foreground promotion. Under the optional foreground policy, list and exact-scan operations can reuse completed immutable bytes
without inserting or refreshing entries. Maintenance and writes can populate
probationary space without evicting protected foreground entries; dedicated
builder caches therefore retain useful cross-slice warming.
Ordinary indexed search remains foreground traffic. Repeated foreground hits promote blocks; protected entries are capped at 75% of
the existing byte/entry budget, and probationary entries are evicted first.
The metadata/data partition and aggregate memory caps are unchanged. Mutable
HEADs, authority and negative results remain outside this cache. No request-owned
I/O promise is retained or shared by the cache.

## Rollout

The operator flags are `FUSED_READ_ADMISSION`, `NATIVE_READ_STATE`,
`WAL_DOCUMENT_INDEX`, `FOREGROUND_CACHE_POLICY` and `OPPORTUNISTIC_READ_PREPARE`. They default off, are included
in sealed provenance, and cannot be activated through public native request
fields. Disabled adapters omit new ABI fields for older-module compatibility.
Deploy compatible native modules and collectors across all roles before enabling
native WAL extensions.

Read-state parent identities and the document projection use WAL HEAD format 4.
Older HEAD formats remain readable. Once a HEAD contains either extension, native
writers preserve it even if the activation flag is disabled; disabling flags is
not a persisted-format downgrade. Older binaries must not serve or collect these
namespaces without an explicit supported downgrade procedure. There is no implicit
rollback to an incompatible reader or collector.

Existing deadlines, working-set, physical memory, operation and transfer caps
remain unchanged. These optimizations do not establish cloud gate acceptance.

## Validation and scope

The generic object store owns fused observation. `WalStore` owns optional snapshot
and point interfaces, `query.ReadStateCache` owns reusable overlays, and the
indexed WAL owns its atomic projection and collection semantics. These components
are reusable by other Antfly object-backed hosts. The Cloudflare adapter enables
them through operator configuration; existing local Lite/LSM storage does not
acquire this object-WAL format or automatically enable the cache.

Final validation passes 316 Node tests, all 31 workerd cases and 603 native
serverless tests (six existing skips), with no native leaks. The focused native
read-state target passes all nine tests. Zig formatting and diff checks pass.
The passing workerd log includes the intentional legacy `SQLITE_NOMEM`
fault injection, not an observed emulator allocation failure. See the
[SQLite memory audit](cloudflare-sqlite-memory.md).

Native regression coverage includes cache identity/lineage/floor checks, failed
allocation without watermark advancement, bounded warming, atomic projection
coverage, authenticated record identity, historical fallback and prefix pruning.
Actual workerd exercises app-side fused admission, fresh recovery, visibility,
tenant isolation, caller contexts and held-authority progress. Its assertion
counts `observe_read`, rather than relying only on configured flags.

The first exploratory direct-app latency reports in `read-state-lab` did not
activate fused admission: `AppReadViews` omitted its flag from the configuration
allowlist. Those reports and their exact bundles remain preserved. The corrected
adapter forwards the flag and includes it in observer identity. Final comparisons
assert native fused calls inside the measured app. Local injected binding delay
is an experimental model; zero injected delay still includes emulator R2 I/O.

## Local latency evidence

The final actual-workerd comparison uses 512 documents per tenant, four tenants,
192 reads and 64 concurrent writes, with 20 ms injected binding delay and every
fourth offered round at 100 ms. Each side verifies bodies, visibility and final
counts. The baseline uses the preserved `9c6e075608` modules. Final candidates use
full native module SHA-256
`feb007cc25315a419a5c9203973808772c2008a76ac8c312578c7b7b2e595de0`.

| Active controls | Forward read/write p95 (ms) | Reverse read/write p95 (ms) |
| --- | ---: | ---: |
| Baseline | 945 / 1,227 | 969 / 1,297 |
| Fused admission, overlay state, foreground cache, opportunistic preparation | 674 / 1,240 | 669 / 1,220 |
| Same controls plus WAL document index | 937 / 1,346 | 847 / 1,287 |

The forward run overlaps final tests and repository cache cleanup; treat it as
exploratory. The reverse run starts after both finish. It reduces read p95 by
about 31% with overlay reuse and about 13% with indexing also enabled. This small
tail workload favors overlay reuse: adding the index increases durable write and
retention cost and obtains fewer cache hits/extensions. Point indexing remains
independently controlled for workloads where avoiding large-tail replay pays for
its maintenance. These results do not justify enabling every control together.

Reports `lifecycle-latency-1793212179617.json` and
`lifecycle-latency-1793212607620.json` preserve native fusion activation counts
(196 observed read traces per optimized case), modules, role bundles, bindings,
physical I/O and sampled spans under
`examples/cloudflare-serverless/dist/qualification/read-state-lab/`.
A final uniform/no-injected-delay check uses 64 documents per tenant, 96 reads
and 32 writes. Baseline read/write p95 is 942/913 ms, overlay reuse is
1,006/912 ms, and the indexed combination is 793/793 ms (report
`lifecycle-latency-1793213047726.json`, 100 observed fused read traces per
optimized case). Overlay reuse regresses read p95 by about 7% here. Emulator I/O,
queueing and cold kernel effects remain; no universal latency improvement or
cloud SLO follows from these local populations. Roll out controls independently
against the application's read/write mix and delay distribution.

The frozen cloud control `c44896ac8c` and its gate thresholds are unchanged.
All 1,173 older sealed artifacts remain hash-valid. No Cloudflare resources were
created. Local results do not pass the cloud acceptance gate.

## Research informing the design

[SlateDB readers](https://slatedb.io/docs/design/readers/) use pinned manifests
and reader-local replayed WAL state. Antfly retains per-request fresh admission;
background refresh alone does not satisfy that contract.
[SlateDB caching](https://slatedb.io/docs/design/caching/) separates foreground
and scan/maintenance cache admission.
[FoundationDB's technical overview](https://github.com/apple/foundationdb/wiki/Technical-Overview-of-the-Database)
separates obtaining a read version from reads at that version.

Cloudflare documents that [R2 bindings bypass public-domain caching](https://developers.cloudflare.com/r2/reference/consistency/)
and the [Workers Cache API is local and does not support tiered caching](https://developers.cloudflare.com/workers/reference/how-the-cache-works/).
This implementation does not introduce a new public/tiered object endpoint or
cache mutable authority through that route.
