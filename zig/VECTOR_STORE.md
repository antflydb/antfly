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
> [docs/design/vector-store/history/experiments-2026-09.md](../docs/design/vector-store/history/experiments-2026-09.md).
> Durable decisions from it are folded into the Objective, Ownership, Table
> setting, and Reads/updates/deletion sections below. The source-ownership
> migration qualification record and the 2026-09-08 correctness/design review
> are preserved in
> [migration-qualification-findings.md](../docs/design/vector-store/history/migration-qualification-findings.md)
> and [review-2026-09-08.md](../docs/design/vector-store/history/review-2026-09-08.md);
> their standing conclusions are folded into
> [Qualification and remaining scope](#qualification-and-remaining-scope) and
> [Correctness boundaries](#correctness-boundaries).

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

Online mutation handlers hold a per-table command slot from the standalone
catalog owner across the DB command and catalog reconciliation, so a paused
start cannot resume after another handler cancels or replaces its admission. A
contending migration mutation returns retryable HTTP 503; status reads and other
tables are unaffected. Catalog-only admission can be cancelled without starting
the source store or passing the disk reserve: the DB writes a durable cancelled
receipt (bound to the source identity, offline too) before admission is cleared,
and exact retries return that receipt. Migration page commits use the WAL
durability barrier, not a memtable flush; only ownership publication takes the
full storage barrier.

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
publishes through the existing recoverable generation exchange. Copying syncs
the whole shadow directory chain before acknowledging a file's first chunk, and
retries resync existing directories. Repeated restart
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
[`scripts/qualify_vector_migration.py`](scripts/qualify_vector_migration.py) runs the screen. It fails if source
collection does not reach the expected live payload count and, for corpora of at
least 512 dimensions, if migrated primary SSTables exceed half the raw vector
plane. Query comparisons include a matched restart in every arm so
ingestion-time identity caches do not confound them. ANN neighbor comparisons
wait for a clean posting-refresh certificate first (see
[DENSE_INDEXING_LIFECYCLE.md](DENSE_INDEXING_LIFECYCLE.md#status-and-metrics)).

Current single-screen results (final binary, 768-D synthetic corpus, 1,000
updates and deletes; not repeated ABBA): at 1M, online migration plus churn takes
449 s and offline 531 s, and both migrated tables finish within 1% of fresh total
disk (about 3.98 GB) once the `reclaiming` phase has run. After a matched
restart, online semantic QPS is within about 6% of fresh and offline within about
12%. Online migration's sampled peak RSS is about 71% above a fresh table
(11.7 versus 6.8 GiB), so online operators need temporary memory headroom within
normal admission. Same-index conversion preserves ordered top-10 neighbor lists
exactly through online and offline migration and restart.

Reverse migration, migration-overlap backup/restore, HA/replication and broader
topology remain separately qualified work. Migration-overlap backups/restores
are rejected; a primary-only backup cannot capture reference closure. After
publication, changing the setting or booting an older binary is not rollback.
Returning to primary ownership requires a reverse conversion or a consistent
pre-migration backup with an explicit data-loss boundary.

A shared post-churn query cost affects fresh and migrated tables alike:
live-document constraints map nonvisible document ordinals to ANN member IDs
before search, and missing mappings fall through primary and legacy identity
lookups without being cached as misses. Fixing it must keep generation-aware
visibility and one-to-many chunk/multi-source behavior.

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
practice. The current same-binary comparison (two AB/BA pairs) shows 43.4% less
total allocated disk after restart, including journals, at 0.990 recall, with
higher peak and mixed query throughput; churn and reclamation are slower. The
earlier pre-refinement qualification measured about 41%. Numbers and tradeoffs
are in [VECTORDBBENCH_FINDINGS.md](VECTORDBBENCH_FINDINGS.md#source-ownership-at-1m-2026-09-14).

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

Shared exact-vector projection coverage is certified by embedding artifact family
and dimension. Recreating an external index with a different dimension selects
only compatible artifacts, and compaction maintains an independent certificate
for each vector space. Older name-only certificates cannot certify a new vector space; maintenance
rebuilds coverage at a pinned source boundary.

## Code map


- [vector_block_store.zig](pkg/antfly-embedded/src/storage/vector_block_store.zig):
  table-level exact-vector blocks, committed WAL batches, `CURRENT`
  publication, and retained readers.
- [vector_wal_view.zig](pkg/antfly-embedded/src/storage/vector_wal_view.zig): vector WAL
  read/version machinery.
- [vector_block_manifest.zig](lib/vectorindex/src/vector_block_manifest.zig):
  vector generation metadata and coverage.
- [artifact_codec.zig](pkg/antfly-embedded/src/storage/db/enrichment/artifact_codec.zig):
  existing embedding artifact representation and source metadata.
- [artifact_payload.zig](pkg/antfly-embedded/src/storage/artifact_payload.zig)
  and [vector_payload_store.zig](pkg/antfly-embedded/src/storage/vector_payload_store.zig):
  reference conversion and the table-owned source store (see
  [Ownership paths](#ownership-paths)).
- [table_storage.zig](pkg/antfly-embedded/src/common/table_storage.zig): the
  persisted `dense_embeddings` setting and creation policy.
- [VECTORDBBENCH_FINDINGS.md](VECTORDBBENCH_FINDINGS.md): benchmark contract,
  reference numbers and experiment flags.

Reuse and evolve this shared store rather than introducing another permanent
exact-vector copy. Reference-only authority requires every relevant reader,
transaction, repair, replication and backup path to support it; paths that do
not are gated (see [Table setting](#table-setting)).

## Correctness boundaries

| Boundary | Mechanism | Still to prove |
|---|---|---|
| Artifact/model/version identity | `Reference.forArtifact` hashes the artifact key and full envelope; lookup is never keyed only by document ID | Each producer's model/index/incarnation key and stale-completion fence through update, delete, recreate and rebuild |
| Prepare before primary commit | `Txn.commit` stages the reference epoch and prepares durable payloads before the primary commit; unresolved outcomes fence the session | Injected-failure sweeps across every allocation and durability boundary |
| GC reachability | Primary snapshot, durable ANN references, post-cut tail and rescued preparations are retained; a newly rescued owner invalidates the complete receipt | Concurrent mark/prepare/delete/cancel/publication and repeated reopen |
| Historical readers | Read sessions precede primary read transactions; ANN snapshots retain source blocks and WAL roots | Teardown order and borrowed directory/cache lifetimes across serving replacement and last-index drop |
| Publication failure | Failed planning discards its mark; ambiguous publication poisons the writer and preserves files | Failure sweeps across planning, reader reopening, inventory and receipt publication |
| Deployment scope | Source open rejects unsupported backends/HA; lifecycle operations have explicit gates | Restore, movement, split and HA ownership proofs |

Collector memory is admitted before allocation: GC prepares readers and
inventory before publication, reuses the committed WAL suffix, and keeps the
current generation on pre-publication failure; the mark map and source leases
are admitted against the resident ANN snapshot, and rejected setup releases its
snapshots and retries later. As of the 2026-09-08 review, exact all-live
reachability verification reads (CRC-checks) payload bytes; separating it from
scrubbing must keep an explicit integrity boundary.

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

## Qualification requirements

The fresh-table milestone (mode threading, reference/recovery protocol,
recovery tests before timing, per-table accounting, public A/B) and the scoped
default promotion are complete. Broader rollout keeps these requirements:

- Inventory every producer and consumer of primary embedding bytes, including
  enrichment, external embeddings, artifact APIs, rebuilds, repair, backup,
  replication and shard movement. During any transition, a dual representation
  has explicit authority and verification rules; fallback must not mask missing
  committed data after authority has moved.
- Keep vector bytes out of primary memtables and SSTables. Temporary full bytes in
  transaction/replay journals need explicit retention boundaries.
- Correctness coverage: crash injection around prepare/commit and publication,
  ambiguous writes, idempotent retries, stale enrichment results, old snapshots
  across update/delete/compaction, multi-index sharing, index drop and rebuild,
  backup/restore, replication and shard lifecycle, allocation failure and
  admission pressure.
- Measure primary WAL/memtable/SSTable bytes, total durable bytes across both
  stores and journals, compression CPU, compaction read/write bytes, vector read
  amplification, directory/cache demand, retained-version and orphan bytes, and
  maintenance/replay lag. Report memory demand separately from cache-inclusive
  RSS, and include overwrite/delete workloads.

A/B qualification creates fresh `primary_lsm` and `vector_store` tables with the
same binary and public API, runs them sequentially on one host in alternating
order, records the effective persisted mode, and holds dataset/order, shard
count, encoding, ANN settings, exact scoring, batch size, writer concurrency,
sync level and resource limits constant. Run 50K, then 1M once correctness and
50K regressions are understood. Compare ingest and catch-up separately, query
throughput and tails, disk, memory, compaction/reclamation, mixed updates and
deletes, cold and warm restart, and post-churn reclamation, with completed
operation counts. Also exercise enrichment and mixed full-text/vector use. An
improvement must not come from unaccounted bytes in another store, weaker
durability or exact scoring, or deferred reclamation. The benchmark contract is
in [VECTORDBBENCH_FINDINGS.md](VECTORDBBENCH_FINDINGS.md#benchmark-contract).

## Open work

- Reverse migration, migration-overlap backup/restore, snapshot/backup and split
  for `vector_store` tables, HA/replication and topology changes.
- Small-table query variability and slower reclamation/churn before any broader
  default promotion (see [VECTORDBBENCH_FINDINGS.md](VECTORDBBENCH_FINDINGS.md#open-work)).
- The shared post-churn identity-lookup cost above.
- Separate foreground and idle source-GC copy budgets remain unimplemented:
  background steps use one `ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES` budget (capped at
  8 MiB), so sparse-only fallback can still copy during foreground work, and
  copy-set construction and sorting remain locked
  ([history](../docs/design/vector-store/history/experiments-2026-09.md)).
- Finish the failure/lifetime audit in [Correctness boundaries](#correctness-boundaries)
  before adding another ownership or publication mechanism. A fully incremental
  owner-delta collector would need primary/ANN reconciliation and restart proofs.
