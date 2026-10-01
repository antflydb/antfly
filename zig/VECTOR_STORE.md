# Separate Vector Store: Intended Design

> Paths under `.benchmark-results/` refer to local benchmark output that is not tracked in git.

## Status

Local, single-shard standalone tables without HA or replication now default to
`vector_store` source-embedding ownership on creation; explicit `primary_lsm`
remains available and is retained for existing tables and unsupported
deployments. The qualified read-path defaults that ship with that promotion are
in [Table setting](#table-setting). Snapshot/backup and split operations on
`vector_store` tables are not yet supported. See
[Existing-table migration](#existing-table-migration) below for moving an
existing table between ownership modes, and [Objective](#objective) onward for
the design this implements.

> **Relocated:** The dated implementation log (September 2026) documenting the
> experiments, qualification rounds, and the final pre-refinement 1M repeat
> qualification that led to this design is preserved verbatim in
> [work-log/completed/vector-store/experiments-2026-09.md](../work-log/completed/vector-store/experiments-2026-09.md).
> Durable decisions from it are folded into the Objective, Ownership, Table
> setting, and Reads/updates/deletion sections below.

## Existing-table migration

Source ownership and ANN format are separate transitions. The logical index
remains `embeddings`: migration preserves the table/document incarnation, every
artifact/producer identity, exact vector bytes, models, dimensions, metrics,
chunks and index definitions. It does not re-embed documents. Dense artifacts
with no ANN consumers migrate too, and dropping the last consumer preserves
source ownership.

The first implementation supports local, single-shard, single-replica standalone
tables moving from `primary_lsm` to `vector_store`. It is an explicit operation,
not a setting PATCH or an automatic conversion on open. HA, replication, Lite,
serverless, schema migration, restore and topology changes are not admitted.
Index/producer/schema changes and table deletion are fenced while a job is
active. Existing native generation repair handles legacy ANN conversion;
discarded experimental formats do not gain compatibility decoders.

### Online operator

Use the same binary for the server and its compiled runtime libraries:

```sh
zig/zig-out/bin/antfly storage migrate \
  --url http://127.0.0.1:8080 --table documents --to vector-store --job vectors-20260915
```

The command creates a job with `POST /db/v1/tables/{table}/storage/migrations`,
requiring table admin permission when authentication is enabled. `ANTFLY_API_KEY`
supplies its Bearer token. Creation takes `{"job_id":"...","target":"vector_store",
"budget":{...}}`. `GET /db/v1/tables/{table}/storage/migrations/{job}` observes the
receipt; `POST` on that job takes `{"action":"step|publish|cancel"}` and uses its
durable budgets. GET never admits work or reconciles catalog publication. An
`admitted` receipt means the catalog marker exists but DB preparation has not
begun; retry creation or send a job action to recover that boundary.

The CLI defaults to `--action run`, which creates/resumes the job, advances
bounded steps, and publishes when verification reaches `ready`. Actions `start`,
`step`, `publish`, `status` and `cancel` provide explicit operator control. Ctrl-C
stops the driver; durable capture continues, and running the identical command
resumes it. The server does not schedule an unattended migration loop.
Use a migration-capable server throughout the job; do not downgrade between
admission and completion or cancellation. Older binaries do not maintain the
candidate map required by an active job.

Job ID, target and budgets form the creation idempotency contract. Keep them
equal when retrying creation, including after a timeout. Job actions use the
persisted configuration, so callers do not have to repeat budgets. DB publication
is authoritative if its response or the catalog update is lost. Opening the DB
can bridge that specific stale catalog setting using the matching durable job
and table identity. A creation/action retry reconciles the catalog decision.
The table retains its current receipt until a later job replaces it; this is
not a permanent job-history service.

Defaults are 4 MiB and 1,024 primary rows per step, a 64 GiB temporary allowance,
and a 1 GiB free-space reserve in addition to normal resource admission. The
driver accepts `--batch-bytes`, `--batch-rows`, `--temporary-bytes` and
`--disk-reserve-bytes`. Before publication, an individual dense artifact must
fit the byte budget. Unrelated values contribute only their cursor keys to a
page. Draining hashes borrowed inline vectors into compact references before
retaining the page; an oversized vector captured after verification consumes
one page by itself, without copying its payload into page memory.
Preparation charges a conservative eight times payload/reference/metadata size,
including concurrent embedding writes; the source also checks retained candidate
bytes, covering failed preparations. This is an admission allowance, not a
measurement of physical disk usage. It deliberately overestimates preparation
cost and does not promise an exact filesystem quota. Free space is checked
before preparation. Resource rejection preserves progress and reports the
reason. Before publication, cancel and start a new ID if a larger allowance is
needed; after publication, finish draining to retire the migration allowance.
Reads/deletes continue when preparation is backpressured.

The durable phases are:

| Phase | Authority and work |
| --- | --- |
| `backfill` | Inline primary values remain authoritative; prepare candidate source payloads in bounded pages. |
| `verifying` / `ready` | Verify exact identity/version bindings and byte equality for the cutover corpus. Concurrent writes keep the candidate current. |
| `draining` | Ownership and the publication fence are durable. New writes use references; replace old inline values with already-prepared references. |
| `final_verification` | Prove that every live dense artifact is a valid, resolvable reference. |
| `serving` | Convert any legacy ANN generations and consolidate serving vectors into source references, retaining healthy query generations during replacement. |
| `cleanup` | Delete temporary candidate mappings. |
| `reclaiming` | Flush the final replacements once, durably request primary overlap rewrites, and advance bounded streaming compaction until those requests are discharged. |
| `complete` | Reference, serving and primary rewrite closure are certified. Source GC and reader retirement can finish reclaiming retained versions. |
| `cancelling` / `cancelled` | Before publication only: disable capture, remove candidate mappings, retain inline authority and a durable receipt. |

Progress includes the ownership epoch, snapshot/publication fences, an exclusive
hex-encoded primary cursor, scanned/prepared/verified/rewritten counts, preparation
bytes, charged temporary allowance and the last admission error. Existing table
and index status endpoints provide source-store accounting, index readiness and
repair status. `primary_reclamation_requested` records the durable primary
rewrite request. `complete` includes discharge of those requests, but old readers,
retention windows and source GC may still hold files; it does not mean all old
files or cache pages have already been reclaimed. The request uses persistent
run metadata and ordinary admitted streaming GC. Partial level jobs and splits
carry the request even when their outputs contain no tombstones. Only a
validated full overlap rewrite clears it. A crash between the manifest request
and its job receipt safely repeats the request after reopening.

### Mutation, reader and recovery protocol

A compacted candidate map replaces an additional payload replay journal. Each
dense mutation prepares the payload and co-commits its full artifact-key/version
reference with the authoritative inline primary value, reference epoch and
allowance ledger. Deletes remove the candidate in the same transaction. There
is no asynchronous capture lag. The backfill compares exact current bytes before
installing a candidate, so it cannot overwrite an update or resurrect a deleted
artifact. Normal enrichment producer/source-version fencing remains in force.

Publication commits the table setting and migration decision in one primary
transaction under write admission. Its candidate map covers the entire cutover
corpus. Mixed readers continue accepting inline bytes until draining finishes;
stable old snapshots retain their original payloads and source leases protect
reference snapshots. A live probe admitted before activation retries if it
encounters a reference without a source lease. An ambiguous preparation/commit
fences the shared DocStore, including transaction-recovery owners, until reopen.

Draining validates and reuses the durable candidate reference; it does not
append the same payload again or create another permanent corpus. ANN format
conversion uses native generation publication and coverage checks independently
of the source rewrite. The healthy serving generation remains queryable while
its replacement is staged. The source retains candidates throughout the active
job, including cancellation, while checkpoints and memory admission continue.
Once the job finishes, ordinary snapshot/ANN ownership and journal retirement
control reclamation. Transaction and replay journals are included in total-disk
qualification; old inline payloads are not retained indefinitely for rollback.
After cancellation reaches `cancelled`, native and portable backups are eligible
again without restarting. Retained source objects may still protect existing
readers; snapshot eligibility checks durable cancellation and inline authority,
and rechecks under capture admission before selecting a snapshot.

### Offline operator

The same `antfly storage migrate` subcommand supports stopped-server migration.
The offline candidate uses a 64 MiB shared LSM block cache for repeated
verification point reads when the caller has not supplied a cache. It shares the
normal standalone memory budget and is released after the candidate closes.
Stop standalone, then run:

```sh
zig/zig-out/bin/antfly storage migrate \
  --catalog /data/metadata/local-metadata.json \
  --replica-root /data/data/replicas \
  --table documents --to vector-store --job vectors-offline-20260915
```

Use the actual configured catalog and replica-root paths. The command and the
new standalone runtime lock the same stable catalog sibling inode. Older
running binaries do not participate in this new operator lock: stop them first.
Public table names resolve through the system catalog to stable physical identities.
The command publishes the selected table and epoch atomically in the standalone
catalog row store, preserving unrelated resources, indexes, and extensions.
Existing JSON checkpoints remain an import boundary. `--action status` inspects
the stopped table's persisted catalog record. The command records offline
admission before copying; standalone refuses to start while that marker
is present. `--once` executes one bounded unit and leaves a resumable candidate;
retry the same command and budgets to continue. `--cancel` discards only the
unpublished candidate, persists a cancellation receipt and clears admission.
It cannot cancel an already-published generation.

Under exclusive generation admission, the command inventories and streams the
whole physical database root into a durable sibling, recording a synced file and
byte cursor. It preserves opaque internal namespaces, identity/version records,
artifacts and ANN state; document-only export would lose required information.
It rejects symlinks and storage configurations whose physical state is outside
the lifecycle-owned root. The shadow replays committed derived work, runs the
same source conversion/verifier and native ANN lifecycle, syncs, seals and
publishes through the existing recoverable generation exchange. Repeated restart
or a lost publication response resolves the same selected generation. Old roots
are retired by the generation lifecycle after their readers release them.

### Qualification and remaining scope

Recovery checks cover preparation/commit/publication boundaries, interrupted
physical copies, repeated restart, ambiguous retries, old readers, concurrent
updates/deletes, distinct models, no ANN indexes, last-index drop/rebuild,
resource rejection, cancellation and catalog fencing. The production HTTP and
offline-command suites additionally check compiled-owner routing, admission,
catalog recovery and queries across serving conversion.

Performance qualification must compare migrated and fresh vector-store tables
at 50K and then 1M, with fixed-count churn, restart, readiness, recall, QPS/tails,
lock waits, memory and complete disk accounting. Report retained/orphan bytes and
reclamation separately from logical completion. A passing migration correctness
suite is not evidence of equivalent steady-state throughput.

The [migration qualification findings](VECTOR_STORAGE_MIGRATION_FINDINGS.md)
record the initial screen, the WAL-only page durability fix, and the shared
restart cost found in both fresh and migrated tables. Page durability must not
force one SSTable per progress update. Query comparisons include a matched
restart in every arm so ingestion-time identity caches do not confound them.

Reverse migration, migration-overlap backup/restore, HA/replication and broader
topology remain separately qualified work. Migration-overlap backups/restores
are rejected; a primary-only backup cannot capture reference closure. After
publication, changing the setting or booting an older binary is not rollback.
Returning to primary ownership requires a reverse conversion or a consistent
pre-migration backup with an explicit data-loss boundary.

## Objective

Make a separate embedded vector store the durable owner of exact embedding
payloads. Keep documents, artifact identity, source freshness, and transactional
references in the primary LSM. Let ANN indexes consume those shared embeddings
and own their search-specific structures.

The vector store is part of Antfly's storage engine, with table ownership and
shard-local persistence alongside the primary store. It is not a new remote
service or a second database that applications must coordinate.

This removes full embedding payloads from ordinary primary SSTable compaction
and avoids retaining a permanent primary copy plus a separate exact-vector
serving copy. It also allows vector-specific layout, caching, maintenance, and
reclamation. Performance improvements must be measured; physical separation
alone does not eliminate vector WAL traffic, vector compaction, or ANN work.

Repeated qualification at 1M-document scale confirms the intended benefit in
practice: promoting the vector store as the default source-embedding owner
reduces total disk usage by roughly 41% at comparable recall and query
throughput.

## Ownership

| Component | Durable responsibility |
| --- | --- |
| Primary LSM | Documents; artifact identity and ownership; source version/hash; producer identity; enrichment status; committed vector references; small structured artifacts |
| Vector store | Exact embedding payloads and their immutable versions; physical placement; payload integrity; retained generations and reclamation |
| ANN indexes | Routing, postings, membership, quantized search representations, and coverage of committed artifact mutations |

An embedding artifact is independent of an ANN index. Multiple indexes using
the same artifact can share its exact payload. Dropping or rebuilding one index
must not delete the source embedding needed by another index, artifact reads,
or future rebuilds. Embeddings from different producers or source versions are
distinct artifacts even when their dimensions match.

One artifact API can eventually cover chunks, extractions, edges, assets, and
embeddings while dispatching to different physical stores. Dense embeddings are
the initial scope. Text and structured artifacts may remain compressed LSM
records; graph access may require its own structures. A common lifecycle does
not require one storage format for every enrichment kind.

### Ownership paths

`storage/artifact_payload.zig` provides the common transactional boundary for
dense artifact writes. `DocStore` converts dense artifact writes into
references and reconstructs their original envelope on reads, including
cursors and multi-get; documents, sparse vectors and other artifact payloads
retain their existing representation.

`storage/vector_payload_store.zig` owns `source-vectors/` at DB scope and
reuses the native shared vector-block/WAL implementation. Each reference
contains the original source envelope, dimensions, and a SHA-256 identity over
the logical artifact key plus the complete versioned artifact; different
models, embedding names, source hashes and dimensions remain distinct
identities, and reusing an identical artifact on retry reuses the payload
identity without physical compaction changing the reference. Source payloads
remain exact float32.

Preparation synchronously appends source payloads before the primary
transaction can commit references; a failed or ambiguous source append fences
that source owner until reopen, and collection defers while any old reader or
preparation is active. The collector runs at writer reopen and explicit full
DB sync (`DB.collectSourceVectorGarbage`), scanning committed references and
publishing a replacement generation that reclaims obsolete versions and
uncommitted preparations; its default mode is synchronous.

## Logical references, independent physical placement

Keep each embedding's reference on its independently versioned artifact record,
associated with its parent document or chunk. Avoid a growing physical-offset
directory inside the parent document: asynchronous enrichment completion and
regeneration should not require rewriting unrelated parent fields.

Conceptually, resolution is:

```text
Primary artifact record
  artifact identity + committed artifact version + source/producer identity
                            |
                            v
Vector store version directory
  logical payload reference -> physical segment / block / row
                            |
                            v
Exact vector payload
```

The logical reference must distinguish table/shard incarnation where needed,
document or chunk identity, embedding name, and artifact version. The precise
encoding may use compact IDs. A source hash can help validate freshness; it is
not a substitute for commit identity or protection against delete/recreate
aliasing.

Physical offsets are internal to a pinned vector-store generation. Compaction
can relocate vectors without rewriting primary documents or artifact records.
Cached physical handles must retain their generation or be re-resolved when it
changes. A request for an older committed artifact version must never silently
resolve to the newest vector under the same logical key.

## Physical organization

Build on append-oriented mutation storage plus immutable, indexed vector
segments. Group payload blocks by compatible dimension and encoding, allowing
compact shared metadata and batched positional reads. Use independent integrity
checks and a compression policy suited to vector bytes, separate from primary
document and metadata compression.

The authoritative representation must preserve the exact source float32 vector,
either directly or through an exactly reconstructible encoding. Float16 or
quantized approximations alone cannot replace the exact payload under the
current scoring contract. ANN indexes may retain their own approximate
representations where those improve search locality.

The version directory can have its own compact indexing structure; its design
does not require another general-purpose LSM containing full vector values.
Bound write buffers, directory residency, read caches, and maintenance scratch
through the shared resource manager. Use streaming builders and retained input
generations so background work does not buffer an entire corpus.

SSTable-local value blocks remain a possible smaller experiment, but they have
a different boundary: if payload blocks die with their SSTable, primary
compaction still copies surviving vectors and rewrites their offsets. The
intended separate store retains payloads independently of primary SSTables.

## Commit and visibility contract

The two physical stores must present one logical commit decision. Two unrelated
successful writes do not establish atomicity.

The preferred starting protocol is payload preparation followed by publication
of the reference in the primary transaction:

1. Allocate an immutable payload version and stable operation identity. Append
   the vector and establish the recovery evidence required by the requested
   durability level. Register the preparation so reclamation cannot race it.
2. In the primary transaction, revalidate the source version, producer, and
   artifact incarnation. Commit the payload reference, artifact status, and
   replay/outbox metadata for downstream consumers together. This is the
   visibility decision. Coordinate distributed transaction intents with their
   existing commit decision rather than exposing prepared references.
3. Publish the committed version to readers and downstream index workers.
   Retire preparation ownership only after commit or abort is resolved.

Prepared payloads must remain invisible to artifact reads and ANN consumption.
An obsolete asynchronous enrichment result may leave an unused prepared
payload, but must not replace the artifact for a newer source version.

An API acknowledgement must preserve the existing sync-level contract. In
particular, a durable primary reference cannot outlive its recoverable payload:
any primary sync or checkpoint that makes that reference durable must first
establish the corresponding payload durability, or retain a durable journal
that can reconstruct it. Merely ordering unsynced writes to two files does not
provide that guarantee. `sync_level=write` must not acquire an implicit wait for
ANN completion.

A shared transaction journal carrying payload mutations and the commit decision
is an alternative implementation. Choose the protocol to fit Antfly's existing
Raft, replay, and transaction machinery. The visibility and recovery invariants
apply to either choice; this document does not prescribe an additional fsync
per vector or a new distributed commit protocol.

## Failure and recovery rules

| Failure boundary | Required result |
| --- | --- |
| Before payload preparation completes | No committed artifact reference; retry or abort safely |
| Payload prepared, primary commit absent | Invisible prepared/orphan payload; reclaim after proving no pending commit or retained owner can reference it |
| Primary commit outcome ambiguous | Resolve the existing operation through recovery; preserve possibly referenced payloads and prevent conflicting retries |
| Primary commit complete | The exact referenced payload is recoverable and readable at the committed version |
| Segment publication or compaction interrupted | Recover a complete published generation and its committed mutation suffix; retained readers remain valid |
| Committed payload missing or corrupt | Report failure or repair from an exact durable recovery source; never substitute a different version or an approximate vector |

Replay must be idempotent by stable mutation/operation identity. Local file
offsets are not portable replication identities. Every replica that exposes a
committed reference must possess its payload or sufficient retained recovery
data to materialize it. Replication and log truncation must account for both
stores' recovery progress.

The primary artifact commit sequence, vector-store physical publication
generation, and each ANN index's coverage watermark are separate concepts.
Physical payload presence alone proves neither artifact visibility nor ANN
readiness.

## Reads, updates, and deletion

The runtime LSM exposes two read contracts that vector-store read paths must
choose between deliberately: a snapshot read clones the mutable generation so
a multi-operation transaction stays stable across concurrent writes, while a
point probe reads the current tip without cloning it. Generic storage adapters
must preserve and advertise the point-probe and current-scan operations
end-to-end; an adapter that fails to advertise them silently falls back to a
full snapshot clone, which is correctness-neutral but can be a large
performance regression for a workload that expects a bounded single-value
read.

Artifact reads resolve the version selected by the primary transaction or
snapshot. Search uses compatible retained index/vector views and the existing
source-visibility rules. Batch exact-vector requests by segment/block after
logical version resolution, without widening a query's visible version set.

An update prepares a new immutable vector version and atomically replaces the
artifact reference. Old versions remain available to existing readers and
recovery consumers. A document deletion or artifact invalidation commits the
appropriate logical deletion and downstream mutation; physical reclamation is
asynchronous. Recreating a document, artifact, or index must not revive stale
references from an earlier incarnation.

Reclamation must account for current committed references, retained snapshots
and query generations, pending preparations or ambiguous commits, index/replay
consumers, and backup/restore ownership. Observing no reference in today's
primary tip is insufficient proof that a payload is dead. The implemented
collector marks primary references and the latest durable ANN references,
retains post-cut preparations, and pins old serving generations. Incremental
copying and checkpoint receipts follow the same collector described in
[Ownership paths](#ownership-paths) above. Retired ANN scopes can still retain
excess versions conservatively.

Table-owned source payloads and shared ANN payload ownership are the retained
defaults. An append-only/selective-GC combination has a repeatable write-I/O
benefit but is kept as an opt-in experiment rather than the default, since its
churn cost at larger scale outweighs the benefit; adaptive cache and snapshot
reads remain out of the default candidate, and group commit stays off until
payload preparations can overlap outside the outer DB lock.

Dense completion compares the captured raw source digest inside the primary
write transaction, including parent existence for materialized chunks. It also
checks the document's persisted identity state: a deleted identity or a creation
sequence newer than the enrichment request rejects publication even if the
document bytes are identical. Both sequences use the existing derived replay
sequence domain. This reuses the document identity metadata already committed
with primary mutations; it adds no new per-document version key.

The tests interleave provider completion with changed-source updates, deletion,
and identical-content delete/recreate in both storage modes and both
plain/chunked paths, checking the actual output vector as well as its source
hash. Standalone runtime callers without DB identity metadata or a nonzero
request sequence retain content fencing only. Producer/index generation
ownership still follows the existing catalog and coverage protocol; document
identity is not a replacement for that separate boundary.

Backups capture a consistent primary snapshot plus all vector generations and
WAL boundaries needed by its references. Restore validates that closure before
exposing the table. Shard split, merge, and movement must transfer the same
logical ownership and recovery evidence; no imported primary reference may
depend on an unretained file in the old shard.

## Relationship to this branch

The branch already contains relevant machinery:

- [vector_block_store.zig](pkg/antfly/src/storage/vector_block_store.zig):
  table-level exact-vector blocks, committed WAL batches, `CURRENT`
  publication, and retained readers.
- [vector_wal_view.zig](pkg/antfly/src/storage/vector_wal_view.zig): vector WAL
  read/version machinery.
- [vector_block_manifest.zig](lib/vectorindex/src/vector_block_manifest.zig):
  vector generation metadata and coverage.
- [artifact_codec.zig](pkg/antfly/src/storage/db/enrichment/artifact_codec.zig):
  existing embedding artifact representation and source metadata.
- [VECTORDBBENCH_FINDINGS.md](VECTORDBBENCH_FINDINGS.md): measured primary-store
  costs, shared exact-vector experiments, and current qualification limits.

Reuse and evolve this shared store rather than introducing another permanent
exact-vector copy. Existing durable vector files and native ANN authority do
not by themselves prove that all primary embedding payloads can be removed.
Source ownership, transactions, repair, replication, and backup must first
support the reference-only representation.

## Table setting

The optional, persisted setting at table creation lets fresh tables exercise
either ownership model with the same binary and public API. Implemented request
shape:

```json
{
  "num_shards": 1,
  "storage": {
    "dense_embeddings": "vector_store"
  }
}
```

| Mode | Source embedding ownership |
| --- | --- |
| `primary_lsm` | Preserve the primary artifact representation; default for legacy records and unqualified deployments |
| `vector_store` | Store exact payloads in the shared vector store and committed references in primary artifact records; default for fresh qualified standalone tables |

These modes select source ownership. `primary_lsm` may still use shared vector
files for serving; it does not mean disabling the existing vector read path.
Encoding, ANN configuration, scoring precision, and sync semantics remain
independent of the setting.

The setting must:

- Be immutable after table creation during initial qualification. Reject an
  attempt to change the mode on an existing table.
- Persist in catalog metadata, be reported through table metadata, and survive
  provisioning, reopen, restart, and backup/restore. Restore must preserve the
  mode or reject an unsupported format rather than silently defaulting it.
- Apply to all dense embedding artifacts in the table, including external
  embeddings and generated document/chunk embeddings. Sparse embeddings and
  other artifact kinds retain their existing storage paths initially.
- Require explicit capability admission for the selected deployment. Reject
  unsupported configurations before exposing the table; an environment flag
  must not silently change the persisted source authority.
- Preserve artifact ownership even when the table has no ANN indexes. Dropping
  the last index must leave its source artifacts available to reads and future
  index creation.

Initial performance qualification uses fresh, single-shard standalone tables.
Replication, HA, shard movement, and other deployment paths remain unavailable
for the experimental mode until their lifecycle contracts are implemented and
validated. Supported backup/restore paths must preserve reference closure; any
unimplemented path must reject the operation explicitly.

Switching an existing table uses the explicit offline or online protocol in
[Existing-table migration](#existing-table-migration). Direct configuration
changes remain rejected. A runtime toggle is not a rollback mechanism for
reference-only artifacts.

### Scoped default promotion

New local, single-shard standalone tables select `vector_store` by default
when the create request omits `storage`, provided HA and replication are
disabled. An explicit `{"storage":{"dense_embeddings":"primary_lsm"}}` or an
explicitly supplied empty storage object retains `primary_lsm`; unsupported
deployments retain `primary_lsm` on omission and reject an explicit
`vector_store`. Existing tables, including older catalog records with no
storage field, keep `primary_lsm` — DB open does not reinterpret them using
the current creation default. Snapshot/backup and split operations on
`vector_store` tables reject with `VectorStoreLifecycleUnsupported` until
source-reference closure is supported by those paths.

Promotion carries its qualified read-path settings so ordinary launches obtain
them without extra configuration:

| Default behavior | Override |
| --- | --- |
| Float32 source and ANN payload encoding for fresh stores | `ANTFLY_HBC_VECTOR_BLOCK_ENCODING=float16` selects the residual-backed alternative |
| Direct ANN member bindings | `ANTFLY_SOURCE_VECTOR_MEMBER_BINDINGS=0` disables |
| Exact mapped reads | `ANTFLY_EXPERIMENT_EXACT_MAPPED=0` disables |
| Reduction-based query packing | `ANTFLY_EXPERIMENT_QUERY_PACKING=lanes` restores the prior path |
| One bounded vector-read helper | `ANTFLY_EXPERIMENT_VECTOR_READ_SINGLE_HELPER=0` disables |
| Batched source reads and positional batches | `ANTFLY_SOURCE_VECTOR_BATCH_READS=0`, `ANTFLY_SOURCE_VECTOR_POSITIONAL_BATCH_READS=0` disable |
| Shared immutable source catalogs | `ANTFLY_SOURCE_VECTOR_SHARED_CATALOG=0` disables |
| Replay-aware matrix loads | `ANTFLY_SOURCE_VECTOR_REPLAY_READS=0` disables |

Existing source stores retain their persisted encoding, including float16;
opening a source store does not migrate its encoding. These settings apply
wherever their existing capability checks permit, including applicable
LSM-serving paths, and do not change a persisted format or metric. Other
experimental GC, admission, cache and read policies remain off by default.

## Implementation sequence and acceptance

### First milestone: fresh-table experiment

The first milestone is a small, crash-safe implementation that actually removes
full embedding payloads from primary artifact values. Migration of existing
tables is subsequent work.

1. Thread the mode through public table creation, catalog persistence,
   provisioning, and DB open. Define a common artifact payload interface for
   both modes. Separate source-store lifetime from `IndexManager`'s ANN
   lifecycle so a table with zero indexes can still own embeddings.
2. Specify stable artifact-version references, source fencing, and the
   prepare/commit recovery protocol. Reuse the shared vector store for external
   embeddings, enrichment results, updates, deletes, artifact reads, and index
   rebuilds. The experimental mode must commit references instead of full
   primary artifact payloads.
3. Prove recovery before timing the implementation. Exercise failures around
   preparation and primary commit, ambiguous outcomes, retries, stale results,
   retained readers, and restart. Verify that dropping the last ANN index
   preserves artifacts and that a new index rebuilds from them. Validate or
   explicitly gate each deployment and lifecycle path before admitting use.
4. Add per-table accounting for both stores and transaction/replay journals.
   Attribute physical bytes and retained buffers to their owners and record
   retention boundaries. A dual-write diagnostic may help verify equivalence,
   but does not qualify the reference-only design's performance.
5. Run controlled public-API A/B qualification as described below. Only then
   expand deployment support and implement existing-table migration.

### Broader rollout

1. Specify reference identity, source-version fencing, and the commit/recovery
   protocol. Inventory every producer and consumer of primary embedding bytes,
   including enrichment, external embeddings, artifact APIs, rebuilds, repair,
   backup, replication, and shard movement.
2. Add versioned resolution through the shared vector store. During transition,
   any dual representation has explicit authority and verification rules;
   fallback cannot mask missing committed data after authority has moved.
3. Move artifact commits to prepared payloads plus primary references. Keep
   vector bytes out of primary memtables and SSTables. Any temporary full bytes
   required in transaction/replay journals have explicit retention boundaries.
4. Gate reference-only authority on the capabilities of every relevant reader,
   replica, backup, and recovery path. Migrate existing artifacts with a captured
   source boundary, preserve concurrent mutations, and switch authority
   atomically before reclaiming old primary payloads.
5. Qualify correctness and performance before removing transitional paths.

Required correctness coverage includes crash injection around prepare/commit
and publication, ambiguous writes, idempotent retries, stale enrichment results,
old snapshots across update/delete/compaction, multi-index sharing, index drop
and rebuild, backup/restore, and replication/shard lifecycle recovery. Include
allocation failures and admission pressure where ownership changes occur.

Measure primary WAL/memtable/SSTable bytes, total durable bytes across both
stores and journals, compression CPU, total compaction read/write bytes, vector
read amplification, directory/cache demand, retained-version and orphan bytes,
and maintenance/replay lag. Report memory demand separately from cache-inclusive
RSS. Include overwrite/delete workloads to expose reclamation debt.

### Controlled A/B qualification

Create fresh `primary_lsm` and `vector_store` tables using the same binary and
public API. Run them sequentially on the same host to avoid mutual resource
contention, alternate run order, and repeat measurements. Record the effective
persisted mode in each result. Hold dataset/order, shard count, vector encoding,
ANN settings, exact scoring policy, batch size, writer concurrency, sync level,
and resource limits constant. A change in storage ownership must not silently
select a different search or durability configuration.

Start with public 50K qualification, then run 1M after correctness and any 50K
regressions are understood. Compare ingest and catch-up separately as well as
total readiness; query throughput and latency tails; total disk usage; memory
demand and RSS; and compaction/reclamation work. Include mixed updates and
deletes, cold and warm restart, and post-churn reclamation. Report completed
operation counts so fixed-duration runs with different write volumes are not
treated as identical memory workloads.

Also exercise normal enrichment and mixed full-text/vector use. An improvement
must not come from moving unaccounted bytes into another store, weakening
durability or exact scoring, or waiting indefinitely to reclaim obsolete
payloads. Report tradeoffs and unresolved regressions explicitly before deciding
whether the result warrants migration and broader deployment support.

