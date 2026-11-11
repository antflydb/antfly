# Table-level object storage

Status: initial implementation. The catalog/API engine contract is shared across
standalone and distributed deployment. Existing native tables keep their storage
and placement contracts. The serverless command remains the specialized worker
preset.

## Independent choices

A table chooses `storage.engine: local | object`. `local` is the compatibility
default and uses the hosting process's local/Lite persistence and normal shard
placement. `object` uses shared durable objects without data Raft replicas.
`schema.storage_mode` continues to describe document versus relational data;
`schema.base_source` continues to distinguish an external lake from owned data.
Deployment controls process roles, not the table's durable engine.

Examples:

```json
{
  "storage": {"engine": "object"},
  "schema": {
    "storage_mode": "relational",
    "base_source": {
      "kind": "external",
      "table_id": "events",
      "format": "parquet",
      "uri": "s3://lake/events",
      "write_policy": "read_only"
    }
  }
}
```

```json
{
  "storage": {"engine": "object"},
  "indexes": {"search": {"type": "full_text"}}
}
```

The first table reads lake data and maintains native catalog-fenced sidecars. The
second table accepts document batches into the object WAL and queries published
object generations. The object engine is immutable; changing it requires an
explicit migration into another table. Omit `num_shards` and replication sources
on object tables. Dense `vector_store` ownership belongs to native local tables.

## Implementation sequence

1. Expose lake data and durable sidecars through the native table API. Reuse the
   lake readers, builders, publication CAS, reader leases, and collection leases.
2. Admit object tables as catalog objects with zero data ranges and zero data
   replicas. Neither table creation nor inherited tablespace range defaults may
   manufacture data Raft groups for them.
3. Host the existing object WAL/manifest/query/maintenance stack inside a native
   API process for owned document tables. Reuse the existing write acknowledgments,
   synchronization levels, work leases, publication fencing, and recovery logic.
4. Keep the serverless API/query/maintenance/combined commands as deployment
   presets. Their independent scaling is useful even when storage is a table
   property. A native process may serve native and object tables together.

This implementation provides the sequence's initial data paths: lake/SQL reads
and sidecars, plus document batch, key lookup, and search execution using the
object runtime.
Writable object document definitions are initially immutable. Unsupported native
operations must fail without allocating a local shard or claiming a local commit.
Further capability parity and explicit engine migration remain separate work.

## Authority and ownership

The native catalog owns existence, logical naming, schema, indexes, and table
incarnation. In distributed mode it retains metadata consensus. Removing data
Raft does not remove metadata authority or coordinated publication.

### Native owner serving cuts

The candidate adds opt-in native owner admission for sources whose complete
write path Antfly controls. `lake_indexes.serving_cuts.enabled` starts durable
activation after all writers are upgraded. Reader sharing alone retains strict
remote checks; an activated owned source instead serves its native admitted
publication/WAL record. This candidate still requires end-to-end qualification
before release or deployment.

Eligible sources are managed Iceberg behind an enforced writer gateway and
sealed immutable Parquet manifests. An immutable-file prefix is insufficient:
its membership can still change. Shared external catalogs retain fresh catalog
coordination or an explicit bounded-staleness contract.

Activation conditionally claims the managed catalog and WAL heads with the
same durable owner nonce. Unhooked writers fail, including older binaries whose
strict parsers reject the ownership field. Claiming stages ownership; admitting
a complete first cut atomically enables native reads. Strict coordinated reads
remain available while initial capture is incomplete. Turning the configuration
off does not remove an established fence. Sealed manifests have immutable
membership, and capture retains their exact parsed head even without a published
file-navigation index.

The native owner persists source identity, incarnation, ownership epoch,
acknowledged WAL watermark and the current reusable serving-cut reference.
A compact projection is written in the same native transaction as lifecycle
state, so fresh queries do not transfer retained publication history. Native
snapshots include both projections; older checkpoints reconstruct the compact
view on the metadata owner. Absence is materialized too, keeping unowned sources
on a small point read. During rolling upgrades, an older peer rejecting this
private tag selects the existing authoritative lifecycle read. Permission and
identity failures never select that fallback. Every accepted write advances or invalidates that
record before acknowledgment.
Catalog changes invalidate it before returning success. If remote commitment
succeeds but native acknowledgment fails, the API reports `state: unknown`
without a read receipt. Exact retries repeat native acknowledgment without
appending the WAL again; resolving a committed catalog request also repairs
native invalidation before reporting success. Admission of a rebuilt
cut compares the epoch captured before observing catalog/WAL state, and cannot
regress the acknowledged watermark. An unavailable replacement cut returns a
retryable rebuilding response rather than serving an older acknowledged state.
The checks belong inside the native metadata transaction, including table
incarnation checks. Distributed mutation and warming obey leader ownership.

Activation must fence all writers, including standalone, SQL, CDC and older
processes. Conditional WAL/catalog ownership prevents an unfenced writer from
successfully appending after activation; a separate advisory lease is not enough.
Only the gateway holds vendor write credentials. Ownership transitions must
preserve valid old cursors or drain them explicitly. Unconfigured sources keep
their existing remote checks; failure to establish a fence cannot silently
enable this mode.

Publication and write acceptance construct reusable immutable state references
containing the published generation and bounded pending row images. Queries
issue authenticated cursor references to that state and a WAL position, without
uploading another retained image or cursor envelope. The implementation uses
HMAC-authenticated `lake3:` and `source3:` capabilities, while retaining legacy
`lake2:`/`source2:` reads. A compact framed payload avoids JSON-escape expansion
within the existing token bound; existing JSON-envelope capabilities remain
readable through their original expiry. Signing keys stay in private native
metadata. Cursor validation still
checks current permissions, incarnation, recipe, ownership and expiry. Reader
leases protect published generations through every issued cursor's expiry;
state images and their row groups must live at least as long. Admission extends
the publication reader through twice the cursor TTL plus a refresh margin,
atomically with accepting the image reference. Existing generation and metadata
bounds can impose explicit backpressure at high publication rates. Refresh happens
before the remaining retention horizon becomes too short for a new cursor.
Increasing the configured cursor lifetime forces refresh when the existing
horizon cannot cover it; unavailable cuts also trigger keyed reconciliation.
Required recovery coalesces by table incarnation within four process-wide lanes.
Each waiting caller keeps its own deadline/cancellation; recovery also observes
shutdown and a 60-second maximum deadline. Readiness does not swallow a required
recovery failure for an active owner or advertise a cut whose retention cannot
cover the configured cursor lifetime. Optional warming cannot establish readiness.
Raw captured WAL heads cannot substitute for retained row images when their
underlying roots can be collected independently.

The owner warms the admitted cut's bounded working set: navigation, dictionaries,
postings, row locators and replacement-key indexes. Existing process accounting,
eviction and cancellation apply; warming yields to foreground work and stops
when the admitted generation changes. Four bounded continuation plans retain
completed navigation waves and their working-set quota across timeout, with
eviction-aware rewarming after a cooldown. Plans own immutable references, never request credentials or reader
leases. Compressed opening bundles replace redundant individual metadata fetches.
Retained state, row groups and sealed heads also use the authenticated durable
byte cache, with exact provider generation and credential scope in their keys.
Cache hits still require current native authority or a valid retained capability.
Query execution merges pinned readers
directly, fetches remote blocks on misses and hydrates winning hits. These
optimizations must preserve unfiltered replacement probes and exact pagination.
External-writer freshness, vector overlays and unsupported cursor shapes keep
their established paths until separately qualified. Warm fixture latency targets
do not qualify full-archive performance or scale-to-zero startup.

### Cold opening and restart recovery

Publication reader protocol 98 adds optional `opening_bundles` to the committed
catalog record. Each bundle contains compressed, individually authenticated
opening artifacts and page ranges: source and index roots, row-identity paths,
term statistics/directories, routing, file dictionaries, and a bounded selection
of segment opening metadata. Their identities and sizes are available from the
publication itself, so serving can fetch them without first discovering a chain
of child objects. Original artifacts remain independently rooted; missing or
invalid bundle contents fall back to verified demand reads. Bundles carry no
credentials, leases, mutable HEAD, or source authority.

The producer reads at most 32 MiB of opening payloads, visits at most 64 pages
through three child levels, and opens metadata for at most 16 segments. It emits
at most four bundles, each at most 4 MiB decoded. These limits prevent archive
size or index count from turning a publication hint into a corpus download.
The cold reader coalesces concurrent preparation of the same publication and
credential scope. Independently owned provider clients fetch bundles in waves
with at most four workers, a conservative 32 MiB wave reservation, and the shared
I/O scheduler's admission. Provider cancellation polling includes the request
deadline and explicit cancellation, while fallible artifact checkpoints preserve
reader protection and precise deadline errors. Transport polling never invokes
possibly task-bound checkpoint I/O. Cancellation joins workers before releasing clients
or scratch memory. Denied optional scheduling falls back to demand reads.
Cloud generation maps, retention certificates, and collection mark the new
bundle objects alongside the original closure.

A cleanly drained disk-cache owner writes a bounded inventory checkpoint in its
private, exclusively locked directory. Restart validates its checksum, directory
identity and timestamps, consumes it before serving, and reconstructs accounting
and eviction order without opening every cached file. An interrupted owner,
changed directory, corrupt checkpoint, or smaller capacity falls back to the
existing complete scan. Cached payloads still require exact identity, key,
length, and digest verification. API startup starts optional restoration only
after the server occupies its final address, before public HTTP admission;
queries retain the RAM/source fallback while restoration runs.

The admitted native cut remains the authority for committed generation and WAL
watermark. Restart cache recovery does not relax freshness, reader expiry,
incarnation, credential rotation, or external-catalog checks. Protocol 97 cuts
remain readable without bundles; a new publication/rebuild is needed to gain
publication-level opening bundles. Metadata capability 86 fences older readers
from new lake catalog writes.

Protocol 99 additionally admits up to 128 provider-qualified demand locators
inside the authenticated opening descriptors. The producer spends at most 512
lookup attempts, with navigation limited to the first 256 attempts. Native payload
selectors displace lower-priority navigation already copied into byte-cache
bundles; duplicate IDs upgrade priority without changing the admitted selector.
The fixed 128-entry catalog envelope remains unchanged. Their identity,
length, generation and retention fields participate in the publication's existing
certificate binding. A reader borrows them only under the exact admitted map
root, including its generation, checksum, length and retention identity; namespace
mismatches and duplicate IDs fail closed. Unknown IDs retain ordinary map lookup.
Locators introduce no new payload reachability roots. Protocol-97/98 cuts remain
readable, and protocol-98 publication paths omit the new fields.

Compact identity tables and stored-column navigation join the native opening
metadata, bounded to 384 KiB and 128 KiB respectively before native page alignment.
Stored-column discovery visits at most eight sections per segment.
The existing one-MiB metadata ceiling still applies; if delivery metadata would
displace useful core opening pages, publication falls back to the core set.
Payload identities, bodies and selected column chunks remain demand reads.

Ranking resolves up to 64 query terms per field before waiting for their posting
headers, overlapping independent norms and postings through the existing bounded
source queue. Native hydration sorts identity validation into physical order,
plans 64-winner tiles and at most four column cursors per tile, then scatters
values to ranked output positions. Selected Parquet hydration warms up to eight
file footers and can split selected files, or selected row groups within one
file, into four disjoint cursor lanes. Point coordinates and bitmap blocks share
group boundaries; a two-pass merge over sorted selections uses O(lanes) memory
and never materializes the archive's group ordinals. The page gatherer submits all admitted lanes before inline fallback, joins each wave
before result assembly, and cancels/joins outstanding work on failure. Returned
column leases retain a stable synchronized allocator owner through final release.
Single-group selections and declined worker admission retain the column pipeline
and serial cursor fallback.

Explicitly planned native spans avoid unrelated whole-pack expansion. Their
speculative jobs share the request's eight-MiB conservative reservation allowance;
rejected jobs leave required reads available. Request profiles additionally expose
navigation/payload provider loads, generation-map loads and direct-locator hits,
prefetch submissions/declines/queue time, shared-cache wait time and decoded-cache
hits. Timers can overlap and are not an HTTP-attempt or billing count.

Opening bundle descriptors can also carry the exact provider generation captured
by the publication writer. That descriptor is authenticated with the publication
and bound to its cloud-map identity. A private seed reader can fetch that one
bundle without first walking the generation map; authenticated publication
locators also resolve their exact artifacts directly. Missing selectors retain
the ordinary map path.
Absent selectors are omitted from serialization, preserving existing bundle and
certificate wire shapes. The seed worker always reads and hashes the full bundle
and does not repeat the parent's provider-clock/certificate admission.

For legacy bundles without publication locators, bounded map-root warming runs
alongside direct bundle fetches, using the same credential-scoped immutable
navigation pool. Its conservative 8 MiB admission reduces the bundle wave's
allowance, keeping their combined cap at 32 MiB. It is optional and is canceled
and joined when the required seed wave is ready. Independent stop flags interrupt
provider polling before any worker arena or selector binding can be destroyed.

After all opening navigation has priority, a second publication pass may seed
complete tiny text indexes whose authenticated total serving size is at most
256 KiB and whose segment count is at most sixteen. Their complete bounded
payload avoids separate dictionary/posting/source GETs for small current-data
tables. Large archive indexes do not receive arbitrary payload prefixes; this
pass shares the existing pack count, decoded-byte and capture-read limits.

Cold demand reads also use bounded byte read-ahead. A touched native text pack
can be fetched and authenticated once (at most 1 MiB, at most 16 times its
requested block). Parquet data reads can reuse aligned 1 MiB windows across
headers and payloads, including stitching up to three windows for a bounded
request. Complete source objects of at most 64 KiB may use one transfer even
when a first projected column is smaller than the usual amplification ratio;
the same request and node budgets still apply. Independent misses overlap through admitted workers only when the
provider advertises concurrency and its construction allocator is synchronized.
Source clients own a stable locked, reclaiming allocator until teardown; workers
use private state and independent result allocators. Cancellation drains every
window task before releasing its capability or buffers. Every source GET still enforces the admitted version/ETag. Footer and
page-index interpretations keep their exact ranges. Existing exact disk entries
are checked first, so upgrading does not discard their restart benefit.

Text and source windows share one atomic 8 MiB reservation allowance across all
composed leaves and refills; failed admissions also consume it. Each widened
read additionally obtains conservative node-wide I/O memory admission. A bounded
request-local failure memo prevents repeated failed speculation across refills;
provider errors fall back to the exact required read, while cancellation,
deadlines and reader authority failures remain precise. Already verified
coalesced demand data takes precedence over new speculation. Small caches, exhausted allowances and saturated nodes retain the exact-range path.
No archive-wide segment traversal or arbitrary neighbouring file fetch is
introduced. A failed optional whole-pack checksum falls back to independently
authenticated required ranges. Parquet page-index discovery runs inside admitted
header workers instead of serially opening every column before its pipeline.
Serving header probes can use the existing 64 KiB workspace to combine header
and payload discovery; shared byte slices avoid another payload copy.

Request-owned atomic I/O profiles record immutable RAM/disk hits, provider read
count/bytes/errors, overlapping provider elapsed time, peak active loads, and
bundle-opening time. They attach to the prepared reader's lifetime, not a leaf's
scratch arena, and survive only through the request's bounded diagnostic
snapshot. Process counters continue to cover all provider traffic; these request
profiles separately attribute immutable index/cache reads and planned Parquet
source loads. Source counters include footer/index/data range payloads, not all
authentication, metadata probes, wire attempts or billing operations. The
read-ahead byte counter reports conservative reservations, not extra wire bytes.
Transport cancellation polling checks deadlines and explicit cancellation only;
fallible reader/lease checkpoints run before and after required operations, not
inside provider polling callbacks where they could perform nested I/O.
Slow-request health
metrics expose both individual leaves and accumulated composed-request I/O
without table names, object paths, or credentials. Refill calls contribute only
their new work, and serial leaf peaks combine by maximum rather than sum.
Disk metrics distinguish checkpoint recovery from scanned inventory entries.

This follows [Lance's upfront metadata size hints and reading strategy](https://lance.org/format/file/#reading-strategy),
[DuckLake's snapshot/file metadata catalog](https://ducklake.select/docs/stable/specification/queries),
and [DuckDB's asynchronous I/O and governed readahead](https://duckdb.org/2026/07/31/asynchronous-io).
The cache follows the same distinction as [Databricks disk caching](https://docs.databricks.com/aws/en/optimizations/disk-cache):
local restart reuse improves repeated access, while a genuinely empty cache must
still fetch remote bytes. [Lance's cache and concurrency guidance](https://lance.org/guide/performance/)
also motivates separate metadata residency and modest, byte-budgeted parallelism
on small instances. These changes preserve selective posting reads and winning-hit
hydration; they do not download full archive indexes to make a first query fast.

External lake sidecars retain one metadata authority: a builder first commits a
pending attempt through definition CAS, renews its fence, uploads immutable
artifacts, and publishes through the exact admitted CAS. Upload completion is
never publication. Query workers pin source coverage and a published generation.
Reader and collection leases protect retained generations and artifacts.

Owned object document tables have a private object catalog projection per native
incarnation. Public requests cannot invoke object-catalog DDL. That projection
holds the immutable schema/index definition required by the existing object
builder; it is not a competing table-management authority. The native API
resolves an authoritative table binding before executing its data operation.

The object runtime owns WAL append order and visible HEAD. Ownership takeover and
HEAD publication use the existing conditionally written coordination record and
fencing token. A lease checked separately from publication is insufficient: an
old worker must not publish after another worker takes over. Multiple native API
workers may host the same object table and contend through that protocol.

Local worker affinity is a scheduling/cache optimization. Durable objects and
fences determine correctness. Object tables have no data shard voters or leaders.


Cold-query attribution retains at most 16 serial composed leaf calls, including
failed calls and refills. Each records a numeric source slot, source kind,
preparation count, binding/preparation/dispatch/ranking/hydration durations, and
request-owned I/O deltas. No table names, paths, credentials, or reader owners
escape into metrics. Elapsed phases overlap; peak-inflight I/O values are
cumulative high-water marks rather than subtractable counters. Overflow is
counted without growing diagnostic storage.

Before first-page decoding, projected Parquet OffsetIndex ranges enter one
bounded prefetch wave. Contiguous page indexes publish exact member cache
aliases, as data-page envelopes do, so consumers do not reread each index.
Unindexed selected-row hydration may probe a complete projected chunk up to
256 KiB, subject to input and worker admission. Larger chunks and ordinary
scans retain 64 KiB discovery; every encoded page remains independently
validated. This policy bounds transfer amplification without assuming that a
footer reveals the number of data pages.

Opening-bundle workers bind the same publication-authenticated provider locators
as demand readers. Publications carrying locators skip speculative map-root
warming; a missing locator still uses authoritative map lookup. Legacy bundles
retain map warming. These changes do not establish a sub-second cold-query
result; candidate qualification must measure them separately from the earlier
roughly five-second fixture.


Reader protocol 100 adds at most 32 direct provider selectors for publication
roots and declaration roots. Selectors are copied into the stable reader binding,
validated against its namespace and exact map generation, and covered by the
retention certificate when present. Older descriptors keep their original
serialization and fall back to generation-map navigation. Publication activation
requires topology capability 87.

Prepared text leaves share one query request gate across composed sources:
eight requests and a 32 MiB transfer allowance, beneath a process gate of eight
requests and 64 MiB. Reservations include request bodies and response ceilings.
A required transfer larger than the allowance runs alone under its response
limit; these allowances do not claim a hard bound on total query memory. Unknown
response sizes reserve the whole allowance. Optional warming declines admission
under pressure and reserves two request slots for demand. Queue time honors the
original deadline and cancellation, without occupying provider transport workers.

Text source opening waits for the first seed pack, then allows ranking and demand
reads to overlap remaining packs. Query teardown cancels and joins every seed
worker before releasing its store, arena, or reader lease. Missing or declined
seeds fall back to authoritative reads. SQL retains the joined opening path.
Generation-map bytes now use the existing credential-scoped, checksum-verified
immutable cache as well as decoded RAM navigation. Its configured disk tier can
reuse these bytes after process restart; cache hits still validate namespace and
node depth. Table-owner routing remains the locality policy; this does not add a
new distributed cache placement service. HN already publishes stored source and
now projects the URL alongside its other rendering fields, avoiding Parquet
hydration when the stored projection covers the request.

Independent composed sources use waves of at most four initial page/count calls.
Each source owns its mutable preparation scope, cancellation signal, and phase
profile. The root scope shares the 64 MiB preparation budget, sixteen prepared
reader slots, and 8 MiB read-ahead allowance across all sources. Pull coordinators
leave child I/O/CPU admission available; denied optional admission runs serially.
Responses are bounded and moved or copied to the caller only after joining.
A failing source cancels siblings before joining parked providers and preserves
the initiating failure. Overlay suppression and global ranking run after initial
pages arrive; serial refills reuse the same source scope and retained cut. Cursor
publication resolves readers across all child scopes. Leaf elapsed sums overlap;
`composed_leaf_wall` measures actual source-wave/refill wall time.

Publication capture records physical artifact selectors separately from synthetic
range cache keys. Thus a page inside a navigation pack retains the pack's exact
provider selector in the publication-bound locator set. Missing/overflow locators
still take the authoritative map path. Selecting a publication promotes an
existing map-only binding in place: it installs locators and validates its
retention certificate while keeping the reader address stable. Identical admitted
certificates retain their clock qualification. Store binding transitions are
serialized; a fully admitted reader remains immutable under parallel hydration,
and changed authority is rejected. Failed upgrades preserve prior binding state.
Payload prefetch may include one adjacent
64 KiB block from the same physical pack, batching a header hint and the next
posting pull. The existing shared byte allowance, queue cap, physical adjacency,
pack limits, and at-most-32-member envelopes bound speculation; demand continues
to authenticate every member independently.

### Bounded scoring admission in opening bundles

Opening publication observes up to 64 validated, fragmented routing records and
captures scoring admissions, canonical file manifests and physical row maps.
Inline source spans and source-page prefixes participate within 256 direct seed
read attempts; already captured identities are skipped before provider reads. Canonical and compacted serving
roots both participate, since exact ordering can select canonical segments even
when a compacted root exists. Routing roots deduplicate within the bounded queue.
Navigation roots retain priority;
at most 32 segment openings and 16 tiny complete segments are seeded, within the
existing decoded-byte, read-byte and bundle-count limits. This is an optional
prefix cache, never a segment-selection limit: demand outside the prefix uses
the original authenticated objects and generation-map fallback. No wire-format
change or rewritten source identity is required.

After navigation, up to 32 admitted segments contribute at most 8 MiB of useful
working-set units, at most 2 MiB per segment. Complete small inverted sections
are preferred; larger sections can contribute complete bounded dictionary/norm
tails. Compact identity bytes and source-column sections up to 256 KiB follow.
A selected section either fits in the remaining unit budget or stays on demand;
arbitrary posting prefixes do not consume the remaining envelope. These copies
use the existing four authenticated opening bundles and their byte limits.
Every member retains its original range digest and provider fallback.

Winner hydration plans adjacent selected source chunks together before submitting
prefetch. A tile emits at most four physical spans and one MiB, examines at most
64 selected positions, and merges only physically contiguous selected chunks.
Sparse gaps are excluded and required reads retain identity validation, exact
field projection and cancellation. Planning does not change the cursor's live
decoded chunk or transfer its value ownership.

Canonical hints are optional when a compacted serving representation exists:
compaction may have reclaimed their original postings. An opt-in artifact read
view bypasses derived-recovery classification for those hints while preserving
namespace, allocator, capabilities and I/O admission. Known missing/corrupt
optional artifacts are skipped independently. Required serving reads retain their
normal recovery policy; cancellation, deadlines and allocation failures propagate.
The same distinction follows navigation pages and tiny payload seeds.

Ranking decodes a separate authenticated admission view of each descriptor. It
validates segment references and native section admissions without expanding
external physical-source pages. Winning-row hydration and maintenance retain
full physical-route validation. The decoded admission cache has its own type,
so it cannot be mistaken for a fully validated physical FileGroup. Prepared cloud
bindings own their locator metadata through teardown, including lazy SQL readers
that outlive the publication parser.

Legacy `numeric_range` predicates on declared integer columns lower to exact
integer index bounds. A bounded binary search preserves f64 conversion plateaus
at 2^53 and the signed endpoints, including inclusive/exclusive comparisons and
NULL exclusion. Simple ceil/floor conversion is insufficient. Floating columns
and non-finite bounds keep their existing residual path; no NaN ordering contract
is inferred from an integer index.

## Storage binding and incarnation isolation

Use the capability-scoped `storage.artifacts` destination for native object
tables. Distributed API nodes must share this destination and have authorized
`storage.primary` access. Source lake credentials do not grant sidecar or WAL
write authority. Standalone may use the existing filesystem artifact
fallback for development.

Before owned-object data execution, the API pins a digest of the physical store
locator in native metadata through CAS and verifies the committed definition.
The digest includes protocol, connection, endpoint/root, bucket, prefix and TLS;
it excludes credential material, so rotating credentials does not relocate data.
A mismatched node fails closed. Relocation requires an explicit migration.

The durable table record carries an object generation. Distributed creation uses
the native table transition fence; standalone creation uses its persisted catalog
epoch because tables with no ranges do not advance range transition fences. The
private root includes table identity and that generation.
A late request or collector for a dropped incarnation cannot touch a recreated
table's root. The old generation remains isolated and retained; automatic
whole-table deletion of owned-object roots is not introduced by this change.

Native processes lazily host bounded per-incarnation runtimes. Each runtime owns
its authorized client, query state, and maintenance worker, and stops before its
client is destroyed at API shutdown. There is an initial cap of 128 hosted
incarnations per API process. Dropped runtimes remain retained until restart;
a later retirement owner can drain and reclaim them without racing requests.

## Compatibility and admission

Legacy table records retain their existing JSON interpretation, binary encoding,
and definition fingerprints. Native storage metadata continues using extension
version 1. Object records use storage extension version 2, including engine,
incarnation generation and physical-store identity. Metadata decoder capability
30 gates object records at admission and at final command append.

Empty data topology is valid only for object records. Native create requests with
zero ranges remain invalid. Reconciliation must preserve the engine and storage
binding when comparing records. An existing native table never becomes an object
table merely because a deployment default changes.

Tablespaces currently describe placement policy. They can later provide named
object destinations and inherited creation defaults, but this implementation
does not reinterpret their location metadata as a storage override. Resolved
bindings must remain table-owned when defaults change.

## Request routing and read consistency

The narrow query definition carries the table engine from the same catalog read
that binds the query. Single-table JSON, table NDJSON, and global multi-query
requests all dispatch through that binding, authorize each line independently,
and retain logical table labels in responses. Retrieval/RAG adapters use the same
object dispatch and published data rather than bypassing it through native shards.
Query definitions carry the object incarnation generation along with table ID;
dispatch validates both against the authoritative snapshot. Drop/recreate or an
engine change during binding returns a catalog conflict, never a result from the
replacement table. Mixed local/object requests select
a data path per table. Binding preserves the distinction between a request-local
foreign alias and a catalog table: foreign primaries go directly to their source
executor without native engine discovery. Join admission checks every native
participant, including nested right sides, against definitions from that same
catalog revision before executing any side. Owned object document participants
return HTTP 400 even when another side would produce no hits. External lake
participants retain native join support.
Point writes and lookups use a targeted definition read to select the engine,
rather than cloning every table and range in an administrative snapshot. Object
execution still revalidates the authoritative incarnation and physical binding.

Owned object key lookup requires explicit `consistency=stale` (the legacy
`read_consistency=stale` spelling is also accepted). It reads the published HEAD;
a WAL-only acknowledgment does not imply that HEAD contains the write. The
normal default `read_index` and explicit `leader_lease` return HTTP 400 for owned
object tables, and invalid consistency values are rejected before execution.
Clients requiring publication before lookup can write with `sync_level=full_index`
and then perform a stale lookup. Supporting read-index-equivalent object reads
will require a WAL visibility fence or an authoritative WAL overlay; metadata
consensus alone cannot provide that data guarantee. External lake tables retain
their existing native read contracts.

## Hosted execution configuration and request lifetime

Hosted object stacks inherit the native API's configured graph execution limits
and remote content configuration. Engine selection must not restore serverless
bootstrap defaults in place of explicit host limits.

The bound query retains its original request context and absolute deadline across
catalog resolution and object dispatch. The object runtime accepts that context,
normalizes borrowed executor clocks to platform monotonic time while preserving
remaining time, and checks it during admission, cold initialization, lookup and
response delivery. Waiting for the runtime registry mutex remains cancelable.
Request-scoped deadline checkpoints are also exposed through a fallible
cancellation token to object query and artifact readers. Session acquisition
passes that token through HEAD and manifest reads into the object-store client,
before opening reader leases or accessing document and index artifacts. The
callback is borrowed only for synchronous request execution; background workers
retain their own
lifetime and do not retain inbound request callbacks. Store operations that do
not support interruption are checked at their boundaries; their individual
transport timeouts still bound an in-flight call.

Before WAL append, expiration rejects the write. After WAL acceptance, expiration
must preserve its durable outcome. Publication synchronization uses the earlier
of the request deadline and the existing 30-second synchronization ceiling and
returns the committed/pending acknowledgment when that budget expires. A final
response checkpoint must never turn an accepted batch into a pre-commit timeout.

## Initial capability boundaries

External read-only Parquet/Iceberg tables keep their existing SQL and sidecar
capabilities. Owned object tables expose document batch, published key lookup,
and search paths. Their schema/index definitions are fixed at creation; this prevents
stale workers from projecting a newer native definition backward into the private
object catalog. Native engine changes and pinned-store changes are rejected.

Cross-engine atomic transactions, owned relational object writes, native
split/merge, native backup/restore of owned object tables, and native credential
row filters are not made equivalent to local-shard operations by this setting.
They need explicit protocols before admission. Existing serverless APIs retain
those APIs' existing semantics; the preset does not imply native capability parity.

## Validation

Cover engine parsing and admission, legacy framing, object framing and command
round trips, decoder gates, zero-range topology, definition/binding immutability,
and clone/reopen preservation. Exercise native API storage-binding admission and
policy-authority failure, JSON/NDJSON/global query routing, explicit lookup
consistency after WAL-only writes, narrow native engine selection, and an owned-object batch with synchronous index
publication, lookup, search, runtime reopen, and recreation into an empty generation.
Existing lake API and catalog suites cover source reads and sidecar publication.

Direct embedded storage APIs require a hosted object runtime and reject the object
engine rather than opening a local DB with different durability semantics.

## Iceberg catalog authority

See [Native Iceberg catalog authorities](lake-catalogs.md) for managed object-store
and external REST configuration, durable commit recovery and the boundary between
Iceberg commits and searchable index publication.

Publication reconciliation does not scan retained-query/recent-vector namespaces;
the bounded durable query collector owns those categories. Scheduled lake
maintenance runs after publication on the cleanup executor, with at most 32
admitted table incarnations per node and one local job per table. Jobs resolve
fresh table identity, retain the provider scheduler's CAS lease, and requeue
publication when maintenance changes data or leaves resumable work. Shutdown
joins the same cleanup owner. Interrupted scheduler completion attempts a
revision-conditional lease release under an independent two-second deadline;
CAS conflicts preserve a successor's ownership and transport failures fall back
to normal lease expiry. Every claim carries a fresh nonce, so identical requests
claimed in the same millisecond cannot recreate an ETag revision. Collector
reader grace remains mandatory.

Leased immutable reads capture the same physical selectors as allocated reads.
When a demand range overlaps this query's pending prefetch, it uses the existing
bounded physical-flight path instead of initiating speculative whole-pack I/O.
Warm units still return without joining unrelated speculative work; each decoded
member retains its own integrity and request-authority checks.

### Serving recovery outside publication

Serving-cut recovery, recent enrichment and optional navigation warming run as
bounded cleanup-owner jobs, outside the serialized index-publication lane and
its metadata CAS callbacks. Eight jobs are admitted by table ID and object
incarnation. A duplicate trigger marks the active job dirty; completion releases
its sequence-fenced ticket and resubmits that key, so a racing publication does
not lose the recovery wakeup. Workers obtain a fresh local control snapshot,
reject recreated tables and honor the existing shutdown and 60-second recovery
budget. The cleanup owner joins them before server/runtime teardown.

Catalog-commit acknowledgement durably invalidates the source epoch, then wakes
publication and recovery independently. Trying to recover that epoch's old search
cut synchronously can block the publisher needed to replace it. Accepted WAL
acknowledgements retain their eager searchable-cut behavior. Request-side cut
validation and mandatory recovery are unchanged. Ingestion group commits share a
60-second pass deadline, including provider retries; durable intents preserve
ambiguous attempts for a later pass. Public query/readiness timeouts are unchanged.

Provider clients borrow the live server I/O runtime through native store opening,
reader reopening, source resolution and WAL/catalog helpers. Clients, credentials,
request buffers and generation bindings remain independently owned. Native
execution clears the raw I/O field from source options and reconstructs clients
from the checked `IoBorrow` in the receiving owner. Clients and cache entries are
destroyed before their borrowed runtime; callers without a runtime retain the
existing self-contained fallback. This avoids one private executor per reopen
without sharing authentication state or request arenas.

Bootstrap-root provider selectors take precedence over payload selectors, which
in turn take precedence over already copied navigation, within the same 128-entry
catalog limit. The 32 fully warmed segment cap no longer truncates selector
discovery: later observed routes can contribute their bounded directory's pack
locations without warming payloads. Complete tiny segments are excluded from a
second working-set pass. Opening workers retain a 64-MiB per-publication wave
budget and global scheduler admission, accounting compressed bytes, decoding and
member copies. Candidate latency, amplification and peak memory must be measured
before claiming that these changes meet a cold-query target.

For an authenticated owned cut, a manifest of at most four MiB can be fetched
alongside its retained descriptor. A resident-only probe skips warm work. The
optional worker owns its client and scratch, verifies the captured credential
scope, and admits only immutable decoded bytes. A stopped flag and joined task
keep the cut reference/request context alive through every exit path; failed or
unadmitted speculation retains required-reader fallback. Publication leases and
normal manifest validation remain mandatory. This removes a sequential remote
read without query-time uploads or carrying clients into cached metadata.

Provider transport and query execution use separate executors. API workers may
block on Parquet, warming or native-build work; lending their bounded executor to
HTTP watchdogs and connect tasks creates a nested-admission dependency. The API
owner instead lazily owns one 32-worker transport executor. All its GCS and S3
clients share four whole-request grants, leaving room for each HTTP/1 request's
six-task envelope and completion. Waiting callers consume no transport workers;
queue waits preserve the original deadline, cancellation and client shutdown.
This transport owns no shared credentials or mutable query state. Clients and
background work drain before its executor and admission counter are destroyed.

Native build contract v4 carries separate checked I/O borrows for execution and
transport, plus the layout-checked, value-only shared admission counter. The
receiving owner reconstructs each I/O view; no raw creator executor or provider
client crosses the boundary. Temporary local admission/resource exhaustion stays
pending in index readiness while the fenced coordinator backs off and resumes
verified progress. Integrity, authorization and hard work-limit failures retain
their terminal classification.
