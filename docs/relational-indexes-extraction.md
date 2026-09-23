# Relational-indexes extraction status

This is an implementation ledger, not a statement of available API features.
The follow-up spans R3–R6 of the relational restructure (landed in #784): relational indexes and
constraints, foreign-key integrity, shared row reads, and relational mutations.
SQL ingress, sessions, pgwire, and unrelated lake/catalog changes remain outside
this extraction.

## Source and retained architecture

The source is `combine-pr-141-143-144` at
`79644dfa1605e8da0f486d021d1c1393577d6265`. The extraction started from main at
`2b57462b1`. Native descriptor vocabulary and pure generation/lease decisions
were extracted from the source's `storage/schema.zig`. Its row/tuple machinery
must not replace main's AROW v2, immutable schema epochs, prepared-row pipeline,
transactional row catalog, or LSM hardening.

Wire vocabulary belongs in OpenAPI. Native durable tags and ownership-bearing
runtime types remain separate, with explicit name-based conversions. Generated
enum ordinals must never become persisted tags.

## Implemented foundations

- Relational indexes use the existing `/tables/{tableName}/indexes` resource,
  with `type: relational` and ordered composite `keys`. Create/drop publish the
  schema through the existing whole-table CAS; list/get synthesize the shared
  `IndexStatus` response. Definitions remain solely in `schema.relational_indexes`,
  never duplicated in the artifact catalog. Create-table index declarations use
  the same normalization and reject cross-catalog name collisions. Public types
  and enums are generated from the canonical OpenAPI specs, including SDK unions.
- Distributed row queries can select a ready named index with typed bounds and
  opaque continuation. Bounded heap fan-in orders by tuple and primary-key suffix;
  authorization precedes projection. Schema/name/comparison changes reject
  stale public cursors; replica-local generation/slot values are not portable
  cursor identity. Owners independently reconstruct their physical seek keys.
  Stateful native readers remain generation-fenced. Each owner uses a pinned snapshot, but pages and different owners
  do not share a globally repeatable snapshot. Cancellation/deadlines cross the
  compiled-owner and streaming scan boundary.
- Shared index status requires matching coverage from all expected owners and
  exposes `milestones` plus generation-bound range progress. Missing owners are
  unavailable, not ready. List collection batches all indexes into one RPC per owner,
  bounded by one five-second request deadline and 16,384 index-owner observations;
  individual GET remains available above the list cap. Owners share one schema
  compilation/read transaction and release apply locks before response encoding.
  A 32-index/eight-owner regression observes eight RPCs instead of 256; this is
  a work-count improvement, not a measured end-to-end latency claim.
  Public generation-fenced retry and repair use the shared named index resource.
  The distinct artifact-only serverless engine rejects relational index mutation;
  it does not pretend that artifact publication builds native relational indexes.
  Catalog publication and write admission reject unsupported mutable relational
  definitions, including retained read schemas/index metadata. Existing read-only
  external Parquet/Iceberg typed schemas remain supported and non-writable.
- Generation compatibility fingerprints depend on indexed column semantics,
  rather than the entire schema digest. Retired-generation admission is bounded;
  cleanup tolerates corrupt retired values. Retirement coordination shrinks pages
  on commit pressure and durably advances at most one owner proof per tick.
  Readiness and maintenance owner proofs bind durable table/shard/range identity
  as well as range bounds; same-bound reassignment invalidates and wakes coverage.
- Compiled storage owners start shared background maintenance only after their
  DB reaches its stable address and configuration is installed. This closes a
  startup gap that otherwise left index builds pending indefinitely. Hidden
  restore owners remain quiescent; publication invalidates their cached bootstrap
  descriptor, and normal public acquisition reopens them with maintenance enabled.
- Graph metric and relational maintenance share the existing `antfly index
  maintenance` operator interface and request cancellation/deadline control.
  Type-specific executors retain their own proofs, algorithms, and progress.
  Relational `/indexes/{name}/retry` and `/repair` admit at most 128 selected
  owners per request through ordinary replicated transactions. A fixed-size
  generation control ticket carries the desired epoch and last request receipt;
  replica-local progress is never compared in replicated apply. Local workers
  fence both pending control intents and changed tickets. Admission can be
  partial; replay the exact request, without refreshing its owner proofs. The CLI
  saves and syncs these requests to a private recovery JSONL file before sending.
  `antfly index maintenance replay --table <table> --index <index>
  --recovery-file <path>` validates the complete bounded, immutable input before
  sending the saved requests without refreshing proofs. It rejects mixed targets,
  epochs/actions and duplicate owners, and ignores an unsent torn final line.
  Runtimes unable to guarantee
  private file permissions/durable directory publication reject this CLI action
  before sending requests; the authenticated API remains the recovery interface.
  Retry selects failed owners; repair/rebuild selects ready or failed owners.
  Unsupported pause/resume/cancel operations fail explicitly.
- Relational repair verifies primary rows, forward entries, and reverse cleanup
  records in bounded durable phases. Healthy pairs produce no repair writes;
  corrupt authoritative rows fail closed. Missing companions and derived orphans
  are repaired before readiness publication. Reverse-only retired generations
  are cleaned even when the active index catalog is empty.

- `TableSchema.checks` now declares named typed scalar CHECKs through the
  existing schema APIs, with generated Zig/Go/Python/TypeScript contracts.
  Integer operands accept exact decimal strings; NULL follows SQL CHECK
  three-valued logic, and collation shares ordered-index comparison rules.
  New writes and transaction preparation enforce checks immediately. Removing
  checks requires a new immutable schema version (omission or `[]` declares
  none); pending durable schema leases fence CHECK-changing publication.
- CHECK activation has local, bounded, checksummed validation progress separate
  from declarations. Pages read historical AROW layouts from the same snapshot,
  fence schema/namespace/owner/progress at publication, and recheck a failing
  source before publishing failure. Failed validation survives restart and has
  an epoch-bound explicit retry; it never disables enforcement of new writes.
  The existing `std.Io` maintenance worker advances coverage even without an
  index catalog. Progress is private local metadata, excluded from portable
  backup. This local proof remains replica-specific restore readiness, distinct
  from the shared distributed CHECK coverage described below.
- CHECK activation also uses the existing public constraint status/repair/retry
  and distributed transaction pipeline. A bounded CHECK phase follows UNIQUE/FK;
  physical source predicates and checkpoint CAS commit together. Canonical CHECK
  identity is cached per immutable schema; equivalent expressions retain coverage,
  changed expressions invalidate it, and removal needs no fake routed claim drain.
  CHECK-only ordinary writes stay locally validated without all-owner fanout.
  Shared mixed document/relational restore requires both global coverage and each
  replica's local physical proof.
- Ordered indexes support `include_columns` through the schema and shared index
  APIs. Checksummed forward values contain only key/INCLUDE cells plus complete
  row hash, timestamp and source-schema identity. Reverse records do not duplicate
  INCLUDE payloads. Covered projections/predicates avoid primary probes; row
  authorization, full physical digest requests and uncovered columns fall back.
  Atomic mutation, backfill and repair maintain those values. Ownership changes
  invalidate readiness before reconstruction can expose tuple-only placeholders.
  Historical backfills retain only one source-layout binding at a time. Updates
  to unselected columns still update the small forward payload's full-row hash
  and timestamp; skipping this write would expose stale versions/TTL metadata.
- Startup now installs the complete validator-bearing schema epoch once.
  Previously, installing a layout-only epoch first caused same-version registry
  deduplication to discard the public validator after reopen. Regression tests
  cover CHECK enforcement across LSM reopen. Operand ownership and schema
  allocation-failure cleanup are also covered; CHECK compilation builds only
  the reduced column layout, not duplicate full-text/index plans.
- `TableStorageMode` is generated and retained through create/status/migration
  responses and the Go, Python, TypeScript, and Zig contracts.
- `TableSchema.relational_indexes` declares table-owned composite ordered
  indexes through the existing schema APIs. Declarations and per-key options
  are generated in Zig, Go, Python, and TypeScript; Go uses a pointer to the
  optional array so explicit `[]` remains a drop request instead of omission.
  Other shared enums remain vocabulary only until their executors are ported.
- Native index lifecycle/lease decisions have focused tests. The extracted
  scalar-column query gate now rejects non-ready states.
- `relational_index_keys.zig` binds composite keys to AROW ordinals. Components
  have independent direction, null placement, and supported string collation.
  Fixed-width keys avoid variable-byte escaping; variable components are framed
  so adjacent columns cannot alias. Integer ordering remains exact above 2^53.
- Key-definition fingerprints bind the encoding version and physical comparison
  semantics. They do not change merely because a column moves to another ordinal.
- `relational_index_plan.zig` retains the schema epoch, owns resolved index
  definitions, and prepares several indexes from one typed row view. Worker-owned
  batches use flat byte/offset buffers, retain their plan through consumption,
  and roll back an entire row's effects on failure. Publication identity is a
  retained snapshot, not a client-provided digest.
- `PreparedRelationalWrite.typedView` checks the original layout and column
  identity, not just the numeric schema version. A row from another registry
  cannot be reinterpreted as trusted data under a same-numbered epoch.
- `relational_index_catalog.zig` stores full immutable native definitions in
  canonical, checksummed blobs with a separately checksummed CAS head. Definitions
  are bound to the complete runtime schema encoding. Mutable generation progress
  is excluded. Unchanged definitions retain generations; changed/reintroduced
  definitions get monotonically newer generations. No-op requests retain the head.
- The catalog's write-publication controller compiles before entering the commit
  section and publishes a retained plan only after the catalog/outbox transaction
  commits. Old acquired plans remain alive but fail a current-plan identity check.
  The execution compiler accepts mixed column/expression ordered tuples,
  covering INCLUDE columns and typed conjunctive partial predicates; unsupported
  index-local constraint declarations fail
  explicitly (table UNIQUE/FK constraints use their shared integrity catalog).
- Each physical index uses a persisted 12-byte generation/slot identity. Names
  remain in the catalog instead of every row key. Unchanged indexes retain their
  identity when catalog ordering changes; duplicate physical identities fail
  validation and identity changes invalidate the prepared-plan fingerprint.
- `relational_index_records.zig` encodes compact forward keys and checksummed
  document-owned reverse records. Reverse records reconstruct forward companions
  without loading a primary row or the current schema. The writer consumes typed
  prepared keys and stages primary-compatible transactional effects; unchanged
  tuples produce no index writes or deletes. A four-byte document-length footer
  makes forward-key ownership inspectable without retained historical layouts;
  its position after the document terminator preserves composite ordering.
- Its batch-staging adapter owns copied effects, reads pending changes before
  the base transaction, and coalesces repeated operations to one final effect per
  key. A failed row poisons the stage so it cannot export a partially updated
  index set. Sealed arrays can join the existing primary/outbox backend batch;
  the adapter neither opens another commit nor provides its own apply fence.
- Production DB batches now pin the schema and relational write plan together,
  prepare index keys from typed rows before apply-exclusive, recheck both
  identities after admission, and include coalesced index effects in the same
  primary/catalog/outbox commit. Normal writes and transaction resolution share
  this path. API request coalescing makes deletes win over writes and last writes
  win over earlier writes. Deletes remove every document-owned generation, not
  only the current plan's IDs. Indexed schema changes reject durable schema leases
  rather than reinterpreting outstanding transaction intents.
- The TTL runtime's separate deletion context stages the same generation cleanup
  in its primary/identity/replay batch. Normal writes and expiration publish the
  table's empty/non-empty marker from the transactional range-local count, not
  namespace-wide live IDs (which deliberately survive shard splits).
- DBCore owns and recovers the catalog controller. Schema changes precompile
  the replacement index plan and pin both base identities before apply-exclusive.
  The schema, table catalog, index head/blob, and supplied outbox metadata commit
  in one backend transaction; only then are the prepared epochs/plans published.
  Removing an indexed column fails before changing durable schema metadata.
- Restore prepares catalog snapshots from the unpublished source store and
  transfers them with the schema replacement. Identical durable schema bytes
  still rebind to the new runtime epoch, avoiding stale layout-pointer identity.
- Portable manifests include the checksummed head followed by its active
  immutable definition blob. Restore rejects missing, duplicate, misplaced,
  corrupt, or schema-mismatched catalog metadata. Retired definition blobs are
  omitted, bounding backup metadata growth from catalog churn. Physical ordered
  index entries are not serialized: both import paths reconstruct them from
  canonically validated AROW rows and commit each block's primary/index records
  together. This also removes stale generations and avoids trusting forged tuples.
- Cold source projections bind column names/types once to the row's declared
  historical layout, preserving comparison bytes when ordinals move. Restore
  retains one owned hot source projection independently of its bounded schema
  cache, so cache eviction cannot invalidate key bindings. Unused historical
  schemas need not contain current indexed columns. Referenced incompatible
  layouts fail explicitly rather than silently changing missing/null semantics.
- Local DB split preparation copies the active definition root after schema
  history. Before publication, source/destination reconciliation removes
  out-of-range forward keys and reconstructs missing companions from owned
  reverse records in bounded, idempotent pages. It neither loads the complete
  index nor needs retired schemas. Relational merge-document pagination now
  returns canonical logical rows instead of skipping AROW entries.
- Schema declarations compile before metadata acceptance and flow through the
  existing atomic public-schema/hot-standby publication path. Introducing an index,
  not only changing an already-indexed schema, fences durable transaction
  leases. Ordinary batches cannot overwrite definition, progress, or retirement
  metadata. Ready-generation writes reject missing reverse state for existing
  rows; the extra primary probe occurs only when a reverse entry is missing.
- `relational_index_jobs.zig` stores checksummed generation/comparison/owner-bound
  progress separately from immutable definitions. Bounded pages prepare from a
  pinned read snapshot outside apply-exclusive, then compare-and-swap progress
  with their index writes. Changed/deleted candidates are skipped because live
  writes maintain the active generation. EOF proves owned-range coverage;
  namespace, catalog, and range changes reject stale publication.
- Deterministic source-row/schema failures become durable failed build status.
  The failing source is rechecked at commit so a repaired row cannot publish a
  stale failure. Explicit generation-fenced retry resets a failed scan; healthy
  retries are no-ops. Transient failures retain the prior durable continuation.
- The existing std.Io maintenance worker dispatches fair, bounded build pages
  and retirement pages with retry backoff. No direct std.Thread worker or yield
  was introduced. Readiness/status and indexed row reads now have public
  distributed adapters; explicit generation-fenced retry remains a DB method.
- `relational_index_gc.zig` walks each retired generation's forward and ownership
  namespaces, reconstructing reverse keys from locators without primary/reverse
  point reads during preparation. Retired-value corruption does not prevent
  deletion. Cleanup cursors and deletions commit atomically and survive reopen;
  split destinations inherit cleanup authority with reset cursors. Retirement
  records commit with catalog replacement, and superseded definition blobs are
  deleted in that transaction. This avoids repeated full-table vacuuming and
  unbounded retained definition blobs. Queue admission is bounded; orphan scrubs
  remain separate integration work.

Catalog ownership and schema/restore publication are integrated into DBCore.
Prepared index keys and record effects are consumed by production row mutations.
Generation selection alone does not constitute a uniqueness check or predicate
evaluation. The FK and typed-row extension below adds coordinated uniqueness,
FK enforcement and public primary-order and secondary-index row execution.

## Shared row execution and transaction dependencies

- `DB.beginRelationalRows` now opens an owned, snapshot-pinned storage reader.
  It supports primary-order scans and ready composite-index ranges, explicit
  column projections, typed conjunctive filters, and bounded pages. Bounds use
  index order (including descending components); prefix inclusivity includes
  or excludes the complete matching prefix. This is a DB execution API, not
  an HTTP endpoint or distributed query coordinator.
- Store, schema, active index generation, ownership range, and the TTL read
  time are pinned together. A reader can finish after concurrent row mutations,
  DDL, and generation GC. Historical layouts are faulted from its own storage
  snapshot, not the live registry, so namespace replacement cannot reinterpret
  old rows using same-numbered schemas from another database.
- Query bounds and predicates share the write-side tuple encoder. Integer
  operands stay int64, comparison types must match, and null, collation, and
  descending framing use identical semantics. Predicate source plans bind once
  per historical layout, including explicit absent-column handling and epoch
  identity checks. WHERE accepts only TRUE; the shared CHECK comparator accepts
  TRUE or UNKNOWN. This comparator does **not** activate table CHECK constraints.
- Reader pages advance only on success, retain one reusable continuation key,
  and materialize only projected cells. A regression reads a selected column
  from a row with a 1 MiB unselected payload inside a 64 KiB execution arena.
  This is an allocation-bound result, not an end-to-end latency benchmark.
- Transaction prepares now retain non-write version predicates as durable
  shared read guards. Readers of a parent coexist; ordinary writes and other
  transactions cannot invalidate the dependency before terminal resolution.
  Existing write predicates keep their exclusive intent locks. Shared guards
  also protect exact-key absence; they are not range/predicate locks.
- Guard members, a cumulative admission ledger, and key-oriented guards commit
  with the prepare vote and schema lease. Retry is idempotent, reader-to-writer
  upgrades reject other readers, resolution retires guards atomically, and
  topology transitions recognize read-only prepared participants. Ordinary
  unlocked batches pay one additional count probe; only live guards require
  key-prefix scans, which stop after the first conflicting reader and do not
  clone the LSM memtable. Guard counts are capped per transaction and charged
  against the existing cumulative transaction admission limit.
- The existing distributed coordinator retains predicate-only shards as real
  participants. Its routing regression covers a parent-only read shard and a
  separate child-write shard. TTL now checks transaction locks under apply and
  skips locked rows without blocking unrelated expiration candidates.

This foundation's read guards are consumed by the FK and shared CHECK activation
extension below. Distributed secondary-index queries, constraint retirement and
repair and immutable default/generated values are implemented. Semantic
extensions now include direct expression index keys and richer CHECK expressions;
the distributed topology fault matrix is still a
release verification gate.

## Remaining integration gates

Public composite-index creation uses the same route as vector and graph indexes:

```http
POST /db/v1/tables/orders/indexes/by_customer_date
Content-Type: application/json

{"type":"relational","keys":[{"column":"customer_id"},{"column":"created_at","direction":"desc"}]}
```

The columns must already be declared by a relational table schema. GET and DELETE
use the same named resource; GET `/db/v1/tables/orders/indexes` lists all kinds.
There is no separate public relational-indexes route namespace.

Each key component supplies either `column` or `expression` plus `result_type`.
For example, `{"expression":{"op":"lower_ascii","args":[{"op":"column","column":"email"}]},"result_type":"string"}`
orders the shared scalar engine's deterministic result. Mixed composite keys
retain per-component direction, collation and NULL ordering; query bounds supply
typed result values in key order. Index compilation owns the expression and its
column dependencies, and historical scans bind once per source epoch. Changed
expression semantics or dependency paths/types require a new generation.
Expression results are unnamed tuple positions: INCLUDE and row projections
continue to name actual table columns. Compilation is bounded to 4,096 expression
nodes and 4 MiB of literals per index; evaluation and expression-derived key
expansion share a 4 MiB per-tuple budget with a 1 MiB scalar-output limit.

Partial indexes add a `where` array of typed column comparisons to that same
resource. For example, `"where":[{"column":"status","op":"eq","value":"open"}]`
indexes only rows where that comparison is TRUE (not FALSE or SQL UNKNOWN).
An indexed row query must imply the index predicate through `conditions`.
The bounded per-column domain proof combines equality, tighter inclusive or
exclusive ranges, excluded values, and NULL-aware comparisons with matching
types and collations. Exact conjunct containment remains the fast path; scan
bounds alone do not establish implication. A missing
proof returns `PartialIndexPredicateNotImplied` instead of an incomplete result.
The reverse proof independently determines which query conditions are guaranteed
by READY index membership; stronger query bounds still execute as residuals.
READY partial-index membership discharges guaranteed query conjuncts, so
predicate-only columns need not appear in `include_columns`. Index-only scans
still require the requested fields and remaining conditions to be covered;
row authorization and physical digest requests retain primary lookup fallback.
The work-count regression returns 128 rows with zero primary probes when the
membership column is not included; an unrelated uncovered condition performs
128 primary probes and returns only its matching subset.
This is distinct from managed embedding `coverage_policy: partial`, which permits
intentional source-document skips. That coverage policy is not a relational
predicate or a proof that a filtered index can answer a broader query. Arbitrary
OR expressions and general algebraic implication remain outside this bounded
conjunctive proof.
Antfarm's Create Index dialog accepts these declarations in Raw JSON mode, using
the SDK's shared structural validation. The visual form does not yet offer a
composite relational-index editor; use Raw JSON and quote exact 64-bit literals.

- [x] Implement immutable-definition storage and durable-then-visible write-plan
      publication, including CAS, checksums, transaction aborts, and LSM reopen.
- [x] Wire the catalog/controller into DBCore startup, schema changes, and
      prepared restore publication; preserve definitions in portable manifests.
- [x] Persist/recover mutable progress separately; declarative definitions use
      the existing public-schema hot-standby payload and atomic schema/index commit.
- [x] Add relational-definition hot-standby replay/reopen integration tests and
      distributed readiness/status aggregation. A full network failover fault
      matrix remains a release verification task.
- [x] Validate both the published plan snapshot and current schema in the actual
      DB mutation path when consuming prepared effects.
- [x] Define generation-scoped forward keys, row-owned reverse cleanup records,
      and coalesced atomic-batch effects with strict failure handling.
- [x] Integrate records with local split/merge, restore, repair and ordinary
      deletion. Range transfer derives forward companions from selected reverse
      records, not the global forward namespace. Replicated split/merge command
      replay and reopen have native regressions; network failover/cutover fault
      testing remains separate. Merge refresh/rollback cleanup uses bounded
      physical-key pages instead of materializing every document in the range.
      The production Raft rollback adapter likewise requests key-only pages;
      its regression scans 256 KiB rows within 8 KiB caller scratch.
      Offline/direct donor copying also uses range-filtered 128-row/one-MiB
      pages (admitting one oversized row for forward progress) instead of
      materializing `groupState()` for the entire donor.
      A failed page never publishes bootstrap completion; reopening retries a
      clean copy attempt and preserves the receiver's original range.
      The native regression copies eight MiB of JSON with four MiB of reusable
      caller page scratch and verifies partial-failure recovery across reopen.
      This bounds memory, not the total duration of a synchronous range operation.
      Durable coordinator time slicing still needs the shared protocol below.
- [x] Commit primary rows, old/new index effects, unique claims, catalog changes,
      and hot-standby/outbox records atomically across normal writes and transaction replay.
- [x] Enforce named typed scalar CHECKs and add bounded local coverage/retry.
- [x] Implement covering payloads/index-only projections and shared distributed
      CHECK status, activation, repair and retry. Composite UNIQUE and explicit
      null-distinctness policies are handled by table constraints.
- [x] Implement conjunctive partial indexes with canonical typed dependency
      identity, historical-layout membership, atomic TRUE/nonmember transitions,
      bounded primary/forward/reverse repair and conservative query implication.
      Nonmembers encode no tuple or covering payload. Ready member rows missing
      reverse records fail closed; absent reverse records for nonmembers are valid.
- [x] Implement bounded immutable typed defaults and stored generated columns,
      dependency ordering, strict projected restore verification, and schema
      publication guards.
- [x] Implement direct typed expression index keys across mixed composite writes,
      historical build/repair, covering reads, generation dependencies and restore.
- [x] Implement typed boolean CHECK expressions with projected dependency
      validation and durable deterministic-error diagnostics.
- [ ] Implement distributed stored-generated row rewrite jobs for ALTER.
- [x] Add local bounded build/drop jobs, durable continuation CAS, cancellation,
      ownership fencing, failed status/retry, and range-local coverage proofs.
- [x] Finish retirement admission headroom, schema-dependency generation reuse,
      and public query-readiness gates.
- [x] Finish relational-index repair/orphan scrubs and public generation-fenced
      retry/repair actions. These do not replace FK/constraint integrity repair.
- [x] Port foreign-key validation/enforcement and distributed activation pages.
      Ordinary referential actions stay atomic, not asynchronous sagas.
- [x] Extract shared row read/mutation execution from SQL-owned code, then expose
      supported operations and generate all SDK request/response types.
- [x] Add an actual LSM write/rebuild/query/churn work-count benchmark. Large-scale
      sustained workloads remain necessary before making throughput claims.

The ReleaseFast 256-row fixture examined 768 primary records versus 16 indexed
records for the same 16 results. After four update/drop/rebuild/GC cycles it had
exactly 256 active forward and 256 reverse records. One run reported 206,073
logical bytes, 104,821 active SST bytes, 214,112 obsolete bytes, and 940 WAL bytes.
Logical generation cleanup is not immediate physical SST reclamation; these
measurements must not be presented as zero post-churn storage amplification.

The covering fixture returns the same 128 projected rows with zero primary probes,
versus 128 probes for an uncovered-predicate fallback. Forward values total 16,256
bytes versus 524,288 bytes of unselected JSON payload in those primary rows. These
are work/storage-count measurements, not a throughput or tail-latency claim.

### Retained-effects architecture and limits

The storage transaction boundary now has a shared **retained row-effects
substrate**, used by both ordinary write transactions and batches when a source
consumer is admitted. It records sorted, coalesced final primary after-images
and deletes atomically with the mutation. Checksummed REF3 frames retain original
physical row envelopes (including their schema identity) and timestamps from
the same transaction, including document timestamp-only updates; no JSON is parsed
and coordinated claim/reference/action-job effects share those same frames.
Derived indexes and independent artifacts are not duplicated. Inactive stores cache
catalog absence without allocating capture keys or copying values. Admission
invalidates that cache before publication; reopen rediscovers the durable state.
The catalog and every consumer operation bind the persisted logical namespace:
same-namespace transfers preserve retention, while a newly adopted namespace
does not inherit source consumers and can explicitly reclaim foreign history
in bounded, namespace-fenced pages.

The fixed-size catalog supports at most 16 independently acknowledged consumers.
Monotonic admission epochs fence retired attempts without unbounded tombstones.
Every frame is bounded to 16 MiB and 65,536 keys; the default retained-byte cap is
256 MiB. New consumers need one maximum-frame of free headroom in addition to
existing prepared-transaction reservations. Exhaustion
rejects the entire source mutation before commit, never truncates history or
acknowledges a partial write. The driver exposes that backpressure
and promptly retire failed/canceled attempts. Scope-bound reads address one
contiguous frame without scanning the log; GC removes only the minimum
acknowledged prefix, in caller-bounded frame/byte pages with atomic accounting.
An oversized first frame can consume up to the hard 16 MiB frame ceiling for
forward progress. These are bounded-work primitives, not a wall-clock scheduling
guarantee. Regressions cover coalescing, deletes, abort, LSM reopen, independent
consumer fences, aborted GC, corruption, and allocation-failure cleanup.

Private source lifecycle commands now replicate admission, exact predecessor
acknowledgement, certificate publication, final fencing, release, and bounded
reclamation. They share the native atomic mutation/marker/outbox transaction,
route to the exact source owner, and expose bounded private status through the
existing owner lookup boundary. Admission holds the apply lock through the
primary-only immutable checkpoint seal, then releases ordinary writes.
Final fencing requires the exact active topology fence and drained participants.
The standby envelope preserves the original source Raft index separately from
standby LSNs. Source pin admission and archive-position receiver pages require
Raft v12, including native-authoritative snapshots, atomic integrity effects,
and explicit source-authority scopes.
Chunked pages without an archive position retain their v9 gate. Archive-position
pages require standby V5 and the fail-closed HTTP `page_v4` discriminator;
chunk-only pages retain V4/`page_v3`. Source admission uses `online_source_v4`
and standby V7. Retention-bound pages use `page_v6`, source-bound checkpoints
use `page_v4_<kind>`, and staged restore controls use `restore_v3`; all require
Raft v12/standby V7. Projection-only owners reject these controls instead of
discarding native effects. Metadata admission requires decoder v9 on every
voter/learner, including the same proof at store registration and final append.
Unchunked
retention-bound pages without an archive position retain their v8/V3 gates.

Portable export now has a bounded, allocation-free source-cut certificate
observer. Its fixed-size, checksummed identity binds the persisted logical
namespace, Raft applied index, retained-effects sequence, immutable schema/layout
metadata, and ordered logical object content. Local cohort-pin digests, inode
metadata, filenames, and physical footer byte counts are not logical identity.
Export checks the applied marker and retained sequence in the same immutable
read transaction; a newer source cannot be exported under an older cut. With
the existing disk spool, certification adds no corpus scan. Transferred files
are verified with the shared AFB2 block/footer reader using bounded manifest and
single-block memory, including after file relocation and process restart.
Cancellation or failed output leaves no certificate result. A result alone is
not durable publication: the caller must flush/publish the immutable artifact,
then replicate its certificate receipt in the source-control ledger. Source
admission now persists a prepared pin fence in the same transaction as the
consumer and applied marker, seals only the primary LSM, then persists a local
pinned receipt before returning success. Reopen finishes a prepared cut before
schema replay or new mutations; a missing already-pinned seal fails closed and
is never recaptured from a newer live root. Schema changes remain fenced until
source release, while exact immutable-schema rehydration remains allowed.
Artifact publication exports the seal through the shared AFB2 writer, verifies
and syncs the file, and writes its certificate receipt. Its source-copy proof is
distinct from a historical backup cohort: ordinary restore rejects the artifact
unless the caller supplies the matching explicit source-copy authority. This
local pin/export machinery does not enable an online coordinator by itself.

Release retains ownership of its catalog slot until pin/staging deletion is
synced and a local cleanup receipt commits. The shared LSM maintenance loop now
advances bounded filesystem-cleanup pages. Checksummed per-slot cursors resume
the deepest owned directory across restart; a durable round-robin cursor keeps
one large or unavailable slot from starving the other fifteen. Startup,
admission, release, and exporter teardown each do at most one page, not a
recursive directory drain. Abandoned prepared-pin staging uses the same walker
and yields `OnlineSourcePinPending` until safe recapture can finish, without
advancing the frozen primary cut. Cleanup never traverses symlinks, reads SST
payloads, or truncates shared hardlinks. Metadata work/path budgets and elapsed
checks bound scheduling units; a filesystem syscall and the required durability
epilogue cannot be preempted. Slots cannot be reused before durable cleanup.

Already-certified source artifacts can be rehydrated on another replica of the
same donor owner through the compiled owner ABI. Describe/read/status/write/
finish operations transfer at most 1 MiB per chunk, with fixed-size scoped
receipts, exact retry-byte verification, data-before-offset fsync ordering, and
atomic final publication. The recipient must already hold the exact replicated
published source ledger: transport input cannot mint a source cut or bootstrap
a differently named merge receiver. Complete archive validation uses the shared
AFB2 verifier, not the donor's inode. Final verification now checkpoints explicit
SHA-256/CRC algorithm state and a checksummed fixed-record object locator index
under the source slot. Footer indexing, object hashing, and schema-certificate
entry hashing resume with bounded byte/work/time budgets after restart; no
previous corpus prefix is rehashed. Initialization still parses one manifest
(strictly capped at 16 MiB) and emits its locator inventory as a separately
bounded metadata unit. Subsequent calls default to 1 MiB / 4096 work units /
5 ms, using 64 KiB payload scratch; the three bounds apply between indivisible
filesystem operations. `.finish` reports incomplete plus verified-byte progress
until the durable verifier reaches the exact certificate. Certified random
object reads use the same locator index after donor-replica failover, without
rebuilding a database or scanning preceding objects. The existing final rename
and publication receipt remain crash-retryable.

The native DB now applies versioned source admission, certificate publication,
acknowledgement, bounded reclamation, release and final-cut controls in the same
transaction as Raft/standby markers and the durable outbox. Exact source/receiver
namespaces, transition, copy attempt and consumer epoch fence every operation;
sixteen reusable catalog slots bound metadata growth. Admission leaves writes
online. The final cut requires the exact active topology fence and drained
participants, and derives its digest from durable source state. Standby envelopes
preserve the source Raft clock separately from the standby LSN. LSM regressions
cover restart, aborted controls, headroom, stale scope, and certificate/cut parity.

Retained-tail reads own one immutable read transaction per frame, validate and
hash its bytes once, and emit owned fragments bounded to 128 effects/1 MiB
(one oversized row is allowed for progress). Historical layouts decode typed
after-images; timestamps and deletes come from the captured transaction, never
the live source. Cancellation/allocation failure cannot advance the session
cursor. Source consumers bind the complete receiver copy attempt (donor term
and sequence), separately from the topology fence attempt. Request construction
requires the durably published snapshot certificate and exact attempt match.
The raw Raft projection only records source controls when its trusted host has
attached the native document delegate; projection-only owners reject them.
Projection durability alone cannot advance the shared applied watermark after
a native failure: exact replay must finish the delegate first.
Typed rows must keep packed and sidecar timestamps equal in the same mutation;
an isolated typed sidecar update is rejected atomically while retention is active,
preventing source/receiver TTL or optimistic-version drift.
The admitted-source schema fence keeps every retained row's immutable layout
available. A session additionally pins its current historical layout in memory.
Missing historical layouts fail closed; the snapshot certificate cannot
substitute for epochs introduced after its cut. Online schema evolution during
an active source would need a separate durable layout-transfer protocol.
Raft v10 groups now capture one native primary checkpoint with the raw control
stream (AFDS v4 / NRSP1), without a duplicate JSON corpus. Capture pins the raw
applied boundary and retains a compiled-owner lease through materialization.
The checkpoint carries the retained journal, consumer ledger, immutable layouts,
and prepared transaction state. Install validates namespace and the external
Raft index, extracts once into an unpublished generation, repairs derived indexes
through the shared snapshot repair machinery, and derives the raw row projection
from that staged primary. Partial projection publication cannot advance the
shared applied watermark: the native delegate requires an explicit successful
publication receipt, and retry completes through the same generation-CAS path.
A new source replica must obtain the certified source-cut artifact from an
existing replica; missing local pin sidecars never authorize recapturing the
current primary under an old cut. Until an active source cut has a replicated
certificate, snapshot creation defers and preserves its admission WAL; ordinary
source writes remain allowed. This avoids compacting away the only replay path
that can create the original local pin on a new replica. Native install also
rejects a checkpoint containing an active unpublished cut. Transferring nested
native source-pin sidecars would remove this pre-publication compaction limit.
Ordinary pre-v10 snapshots retain AFDS v3.

Online metadata progress now uses a lease-fenced compare-and-set command rather
than unconditional transition replacement. Cutover checks the exact prior
state, cancellation observation, lease and table/namespace contract, then
updates receiver bounds, removes the donor range, and advances progress in one
transaction using the existing range-index/generation machinery. Fixed-size,
domain-separated range digests bind this transaction to the receiver's
authenticated base/merged-range receipt without expanding large binary keys in
the metadata log. Lost replies and stale attempts are idempotent no-ops; a point
projection reads one transition for bounded coordinator recovery.

The ordered raw data-Raft projection also arbitrates online source admission
against ordinary donor/split commands. Its exact-scope reservation survives
native v4 snapshots; release or cancellation cannot clear a replacement scope.
Conflicts produce durable per-entry rejection receipts instead of wedging a
committed log entry. The native delegate consumes that same outcome before
applying effects, and the receipts are reclaimed with existing Raft entry-identity
coverage after a certified snapshot. This closes the race between metadata
preflight and an already-in-flight ordinary topology command.

Automatic merges use the existing merge workflow, without a separate public
online-merge command. Schema rewrites are explicitly admitted through
`PUT/PATCH /tables/{tableName}/schema?rewrite=true` and return a durable restore
job. The existing durable merge transition carries optional revision-CAS
online phases: admit, publish, snapshot, tail, freeze, final tail, cutover, release,
and exact-attempt cancellation. Its executable driver authenticates native
source/receiver observations and performs one effect or one persisted phase
advance per reconciliation pass. Ambiguous cutover completes forward; ordinary
merge cannot silently execute an online record. An enabled-by-default
`metadata.runtime.ServerConfig.online_merge_enabled` programmatic seam installs
the complete adapter at the service's stable address using its shared HTTP
executor. Operators can disable new online merges on every metadata process with
`antfly metadata --online-merge-enabled false` (default `true`). Disabling admission
retains the recovery driver so already-admitted online merges can finish and
release their durable fences. The adapter requires the compiled native
bundle, non-serverless mode and internal service authentication; unsupported
deployments retain the guarded ordinary workflow without failing startup. When enabled,
eligible newly queued document and typed relational merges automatically use
the online path. Coordinated UNIQUE/FK merges additionally require matching
catalog/generation proofs and enforced coverage on both owners. Leader-fenced
native facts bind the actual namespaces, catalog digest, topology/consumer
epochs and copy attempt before a lease-fenced metadata CAS admits the exact
ordinary record. Only positively unsupported or already-started ordinary
candidates continue through the existing guarded path; transient discovery
errors retry without ordinary side effects. All current owner voters/learners
must positively advertise Raft v12: known older versions use the guarded
ordinary path; unreachable peers or unknown version zero retry discovery.
Metadata may authorize logical
identity reconstruction, but online checkpoints never request arbitrary raw
identity reassignment. Online-I/O
responses have an explicit 32 MiB per-request ceiling, also configured on the
owned executor; injected test executors must support that ceiling. The
failure/recovery coverage includes deterministic phase
reconstruction and real owner-link interruption tests; these are not a claim
of exhaustive network-partition or power-loss testing.
Native host-path snapshot storage is required on each participating owner.
Graph/vector/algebraic artifact mutations, independently materialized enrichment
(including full-text chunking/assets) are not represented in REF3, so those
tables remain ineligible until their ordered artifact tails are implemented.
Coordinated claims/references transfer into the donor's disjoint routing
interval on the receiver; ordinary reads/prepares retain the receiver's base
ownership until exact final-cut publication. Bounded integrity cleanup, source
snapshot pages, and retained tail effects share durable attempt-bound receipts.
No frozen full-claim scan is required at cutover. A replicated
certificate publication validates its admission cut; it does not turn arbitrary
caller input into proof of an actually transferred snapshot.

Guarded document-copy retries can reuse a completed copy only when the donor's
native catalog positively proves plain row-derived storage and a leader-fenced
receiver receipt matches the exact transition, namespaces, ranges, source cut,
donor term and copy attempt. A local follower projection is only a hint to avoid
unnecessary reads, never authority to skip copying. Typed/coordinated and
independent-artifact tables retain the guarded copy path; their native artifacts
can change independently of the Raft row watermark. Known older peers also keep
the existing path. This prevents a lost RPC reply from automatically discarding
an already-completed eligible copy and starting the same work again.

Shard lifecycle callbacks cross compiled runtime archives through the existing
checked callback ABI, including retained adapters and topology reads. Named
transition payloads keep signature fingerprints stable; the shared status
registry translates busy/not-ready outcomes instead of leaking compilation-local
Zig error integers. The separate-archive regression proves that provider and
consumer error ordinals differ while retry identities and owned payloads survive.
Guarded finalization releases its copied native owner before the ordinary
proposal preflight acquires a catalog-bound owner. Its group transition activity
remains held through publication, preserving admission fencing without waiting
for its own incompatible descriptor lease to drain.

Immutable artifact publication runs in a database-owned background job, so an
individual RPC deadline does not restart export from the beginning. While export
or a bounded artifact-recovery slice is pending, the metadata driver schedules a
quiet 250–350 ms poll without treating progress as failure or throttling subsequent
snapshot/tail pages. Recovery keeps a working source replica between slices and
advances to another candidate when that replica fails.
Cancellation is exact-scope and database close joins the owned worker; a
completed durable artifact is reused after reopen.

The online driver captures both owner routes and source-replica artifact peers
from one projected placement/store-status view. It never calls the detailed
administrative snapshot while holding the transition scheduler lock; that
snapshot may itself observe transitions. Leader selection uses Raft term and
membership evidence, with each report checked against its own placement
generation. Post-cutover source cleanup uses retained placements even after
the donor's public range has been removed.

Prepared checkpoint, page and cancellation responses are validated against the
exact requested scope before proposal; unrelated batch effects are rejected.
Checkpoint routing carries the same copy attempt as the checkpoint, including
the intentionally unbound initial acceptance. Source-owner operations translate
remaining deadline budgets at the native boundary rather than mixing executor
and platform clock epochs.

Durable retention quota rejection must not stop Raft ahead of subsequent
acknowledgement/reclamation commands. Ordinary mutations can return a
deterministic rejected outcome, but a previously committed 2PC decision cannot:
its effects must remain pending rather than being discarded. Prepared votes now
reserve their REF3 capacity through the existing replacement-aware intent
ledger and one fixed-size aggregate. Resolution/abort releases that reservation
atomically with its row effects; ordinary writes cannot consume it. Physical
AROW sizes are exact, and document serialization is counted without allocating
another full row. Source admission accounts for existing tracked prepares and
fails closed on an untracked legacy prepare root until it drains, using one
prefix probe rather than a table scan. Local OOM and corrupt/missing history are never converted
into successful or deterministic rejected data application.

- The opt-in receiver contract for time-sliced merge cleanup/copy is implemented:
  private wire protocol v7 carries a separate page command, with an immutable
  source pin digest/cut, exact source and receiver namespaces, transition/attempt,
  cleanup/row/artifact phase, sequence, effect digest, and exclusive cursor. Native
  direct and replicated apply commit page progress with primary/index/outbox
  effects; the raw Raft projection retains the same progress through snapshots.
  Pages allow at most 128 effects and 1 MiB, except one indivisible oversized
  effect. Cleanup verifies receiver-owned rows and EOF without reconstructing
  JSON; row copy preserves source timestamps and logical values without applying
  new defaults or repairing generated values. Same-attempt retries cannot reset
  progress, stale pages have no effects, and ordinary checkpoints still reject
  bundled row mutations. Private discriminator/version gates prevent old parsers
  from silently dropping the new authority fields. Standby batch payloads use a
  separate V2 gate for page commands/source binding; a Raft capability barrier
  alone does not protect standby replay. Per-row source timestamps bypass Raft's
  ordinary wall-clock batch timestamp synthesis. Regressions cover document
  and generated/covering-index receivers, failed row validation, cleanup EOF,
  reopen/retry/cancellation, source/namespace fences, exact JSON-byte transport,
  and raw projection snapshot continuation.
  Retention-bound copies additionally require tail replay: bounded fragments
  advance an exact frame sequence/offset/digest, only completed frames can be
  acknowledged, and completion requires a final source cut/watermark. Final
  bootstrap must match that watermark. Snapshot-only copies retain their
  existing contract. These checks enforce progress but do not authenticate a
  caller-supplied final cut; the coordinator remains responsible for its origin.
  The authenticated driver resumes the same attempt using native receiver
  receipts, an immutable archive object/offset locator and bounded retained
  frame sessions. Large rows retain their chunk receipt across owner reopen;
  source-replica failover rehydrates the certified artifact in bounded chunks,
  never by recapturing the live source. Local receiver finalization precedes
  atomic metadata range publication, and retiring donor placements remain
  available until source release completes. The receiver does
  not certify a caller-supplied digest as a real snapshot or manufacture a pin
  from a live root. Already-started ordinary merges retain their protocol;
  automatic online selection is enabled by default for eligible tables; an
  explicit operator disable stops new admission without stopping recovery.
  The online implementation must also retain committed effects from the pinned
  cut through receiver acknowledgement. The existing native backup seal can
  reopen a local immutable cut, but its inventory includes local file identities;
  a promoted donor cannot recreate the same handle by sealing its current root.
  The transferable content certificate and replicated per-transition retention
  consumer and atomic source-primary pin now exist, but a coordinator must still
  durably transfer/publish the artifact before ordinary donor leases can
  be released. Split deltas are enabled only while splitting and their ordinary
  batch path appends after primary commit; the derived change journal stores
  changed keys rather than historical values. Neither is currently that merge
  effects log. Reusing either without atomic append, retention admission, and
  receiver acknowledgement would lose deletes or mix source revisions.
  The desired protocol is: replicate source pin/retention admission, publish the
  receiver attempt, atomically advance cleanup/row/artifact/tail page effects
  and its exact cursor, then briefly fence/drain for the final retained tail and
  ownership cutover. Pin/retention reclamation follows durable terminal receipts.
  Missing pins must never fall back to a live root under an existing cursor.
  Long-duration write quiescence for all document tables is not an acceptable
  silent substitute for this online availability contract.
  Retention reserves committed-resolution headroom during prepare. A quota
  rejection may advance an ordinary
  rejected write, but must never discard an already-committed 2PC decision.
  Oversized rows use independently checksummed 1 MiB chunks and a whole-row
  digest. Fixed receiver-local slots and an attempt-bound assembly receipt
  survive retries/reopen without exposing partial rows or advancing page/tail
  completion. The final chunk atomically applies the verified logical row,
  indexes, outbox, cursor and spool cleanup. Snapshot and tail senders share a
  borrowed-row chunk iterator; HTTP, standby and raw projection share streamed
  base64 encoding with 4 KiB scratch space. Abandoned spool storage is bounded by
  the largest actually received row, not the number of attempts. Raw snapshots
  include and validate intermediate spool slots. The file-backed Raft snapshot
  writer streams split deltas and spool controls directly from its pinned read
  transaction; it no longer clones the entire journal/spool into an owned array.
  Final logical-row preparation and explicitly owned-KV snapshot APIs still
  require O(row) memory. The incoming snapshot envelope is still materialized;
  this is not fully streaming typed-row ingestion or snapshot installation.
- Partial indexes compile immutable typed predicates, maintain membership
  through writes/build/repair, and require a sound typed domain implication proof.
  Unsupported implications fail closed. Stored-generated/default values use a shared bounded typed expression
  compiler shared by direct expression index keys and richer CHECKs; unused
  expression vocabulary is not accepted.
- Deferred FK timing and MATCH PARTIAL use the bounded coordinated mutation
  path. Constrained active/read-schema topology changes and native relational
  execution in the artifact-only engine remain unsupported. Full network failover/cutover/restore fault testing remains a
  release gate beyond native replicated-command and coordinator fixtures.

## Immutable defaults and stored generated columns

The public schema declares `column_defaults` and `generated_columns` using
OpenAPI-generated scalar expression types. The immutable schema compiler binds
column names to typed ordinals and orders generated dependencies, rejecting
cycles, mixed arithmetic types, unsupported operations, and ambiguous target
ownership. Logical fingerprints include typed literals and dependency names,
not local ordinals or schema epochs. Supported operations are literal/column,
checked arithmetic, negate, concatenate, lazy coalesce, and explicit ASCII case
conversion; no volatile function or implicit SQL-expression parser is exposed.

Defaults fill absent fields, never explicit null. Generated columns are
output-only: submitted values are overwritten before validation, index/FK
extraction, and logical hashing. Preparation applies the graph once, then
validates without evaluating it again. Defaults cannot reference columns.
FK actions that assign a generated child column are rejected rather than
silently discarding the cascade or SET NULL effect. Durable replay does not
reapply defaults or current expressions. Restore strictly verifies declared
generated outputs against their own immutable schema version. Its cold path
reads only the typed dependency and output cells, leaving unrelated wide
JSON/blob/vector payloads unmaterialized.

Each expression is limited to 128 nodes and 16 levels; each generated/default
set to 256 targets, 4096 nodes, and 4 MiB of literal data. Evaluated outputs are
limited to 1 MiB each and share a 4 MiB allocation budget per row. Overflow,
division by zero, and budget exhaustion fail the write before publication.
The projected verification regression visits exactly two dependency/output
cells and fails if it touches the unrelated payload, while forged or missing
generated outputs are rejected.

Ordinary schema updates preserve generated semantics: additions, removals, and
changed expressions require explicit `PUT` or `PATCH /tables/{name}/schema?rewrite=true`.
This returns `202` with an existing restore-job resource and `Location`; an
`Idempotency-Key` recovers admission after a lost response. Defaults and generated
declaration reordering still use ordinary schema updates. Native schema
publication independently checks durable data under the apply fence.

Rewrite admission authorizes admin access to the complete incoming/outgoing FK
cohort, including retained read-schema dependencies, before reading source
eligibility or creating pins. Row-authored graph references conservatively
expand selection to the whole catalog; independent non-row-derived artifacts
are rejected before admission. The compact job and full immutable replacement
plan commit atomically. The shared restore worker copies into fresh hidden
identities, replays retained changes, fences the complete cohort, validates
constraints, and atomically publishes its replacement. Unchanged document
tables preserve active/read validation and logical contents. No intermediate
schema is published, and cancellation before source preparation leaves no pin.
The rewrite preserves absent values instead of retroactively applying defaults;
stored scalar-type changes and destructive column removal are not admitted.

### Typed boolean CHECK expressions

A CHECK declares either the existing column/operator form or a boolean scalar
`expression`, never both. Typed comparisons support binary or ASCII-folded string
collation, exact int64 operands, null tests and null-safe distinctness. AND, OR,
NOT and coalesce evaluate lazily with SQL three-valued semantics: TRUE and UNKNOWN
pass a CHECK, while FALSE fails. Arithmetic errors in a selected branch reject
new writes; unreachable branches cannot produce an error.

Expressions reuse the same immutable schema plan and distributed CHECK activation
phases. Coverage identities include typed semantic fingerprints, not declaration
order or physical ordinals. Activation projects only the union of referenced
columns, evaluates historical rows by their declared layout, and records
deterministic evaluation errors as durable invalid-row results. Repair/retry and
owner/epoch/row-digest fences remain the shared lifecycle; resource failures such
as allocation exhaustion are not misclassified as invalid user rows. A CHECK
set is limited to 4096 expression nodes and 4 MiB of literals, with a shared
4 MiB evaluation budget per row. This charges allocations and the upper bound
of operand bytes inspected by comparisons, including borrowed wide columns,
so repeated allocation-free comparisons cannot bypass bounded execution work.
Integer and datetime literal values outside JavaScript's safe-integer range
use exact decimal strings in persisted declarations and public responses.
Equivalent GET-to-PUT round trips retain the schema epoch and index identity.

## FK and typed-row extension

The extension uses generation-bound unique claims and per-child FK references,
routed by a logical tuple digest rather than the common metadata prefix. Child
attachments retain shared exact-value claim guards; parent removal takes the
exclusive claim intent and rechecks reference-prefix emptiness under the same
apply fence as durable intent admission. Primary rows, relational indexes,
integrity records, and activation continuations join the existing transaction
decision and recovery path. Raw integrity operations are not a public API.

Incoming JSON is prepared against a pinned typed schema. Public row mutations
require an exact schema version and decimal-string row-version preconditions.
Row queries use the routed scan/read-barrier path, exact typed conditions, and
authorization before projection. A historical row layout does not replace the
request's active schema-version fence.

The schema version supplied by a public client is not proof that integrity
planning ran. Internal prepares additionally carry the catalog generation-set
proof, and storage compares the prepared catalog with the current durable
catalog under the apply fence. This also fences declaration changes that leave
the physical layout version unchanged.

Ordinary CASCADE and SET NULL operations use bounded fixed-point discovery
before the existing atomic distributed commit. They do not silently become a
background saga. The closure has explicit row, byte, and time limits; exceeding
them must fail before publication. Native asynchronous action-job primitives
are not an alternative completion contract for ordinary batch writes.

FK declarations require administrative permission on every referenced parent.
Cascade and SET NULL closure additionally require write permission on every
table whose primary rows change, using the admitted credential scope and live
permission intersection. Claim-only participants do not require primary-write
permission. This follows the existing explicit multi-table transaction policy;
it does not silently elevate the requesting principal for referential actions.

Coordinated activation scans unique claims before foreign-key references.
Each bounded page retains source-row versions and atomically commits its
derived effects and owner/generation-bound continuation. Normal writes must
not assume a first shard's coverage proves the entire table. Positive coverage
is deduplicated within a request, not cached across independent restores without
a durable restore-incarnation proof.

Explicit boundaries still matter:

- Administrative constraint retirement uses a durable all-owner fence and
  phase-separated reference/claim drain. A subtract-only target schema is
  published after the proof completes. `drop=true` prepares an explicit table
  deletion; it never silently deletes the table from a background name-only job.
- Constrained TTL uses the normal FK-aware distributed delete planner and 2PC.
  Expired rows remain visible until that transaction commits; RESTRICT-blocked
  parents stay visible and retry on later sweeps. Maintenance observes at most
  128 candidates per page, bounds physical keys/bytes visited, and resumes with
  a short inter-page yield rather than sleeping a full interval for each page.
  Native workers only enqueue owned observations into the server's bounded
  background lane (one page per group); they never synchronously reenter the
  managed DB cache. Queue pressure retains the scan cursor for retry. Server
  jobs drain before cached DB workers close, avoiding cache-close/self-join
  deadlocks, and actual committed expirations are counted by the coordinator.
  Accepted pages retain an explicit candidate offset across bounded 16-root,
  five-second coordinator slices. Timeout/oversized roots remain visible and
  retry on a later sweep; they cannot starve the remaining page. Successors
  retain the same admission slot, including during scheduler resource pressure.
  Every observed primary row carries a snapshot-bound physical SHA256 guard in
  addition to its timestamp, including cascade descendants and activation
  backfill. Custom TTL timestamps therefore cannot hide a concurrent row change
  from the integrity transaction. A missing coordinator or strong read proof
  defers expiration instead of falling back to local deletion.
  Strong proofs are required for every observed coordinated row and backfill,
  including non-TTL tables; there is no pre-release version-only peer fallback.
  The public timestamp-based `version` field is unchanged: the private digest
  guard strengthens internal observation/commit checks, but is not a new
  monotonic public row revision or a changed version-only CAS contract.
- The guarded UNIQUE/FK split/offline-merge path requires distributed quiescent integrity protocol v1.
  Metadata voters and learners first negotiate decoder capability v7 using the
  existing exact-membership probe; the older framed status codec alone is not
  sufficient evidence. Probes happen outside the catalog lock, and emission
  reuses a term/incarnation/membership-bound readiness token.
  Before first admission every registered table-serving store must advertise
  v1; metadata-only roles are exempt. Admission atomically records a durable
  cluster protocol floor, and subsequent registration/status downgrades below
  that floor are refused. Upgrade the data fleet before scheduling constrained
  transitions. Unsupported queued transitions do not block other reconciliation.
  Standalone topology and constrained active/read-schema migrations remain
  unsupported; the immutable transition contract pins both schema mappings.
- Eligible online UNIQUE/FK merges admit coordinated transactions during source
  admission, snapshot publication/copy, and retained-tail replay. This exception
  requires a validated exact online attempt; ordinary merges and splits remain
  guarded. The freeze phase stops new transaction routing, and the durable
  native fence independently rejects new prepares while pre-existing decisions
  drain. Only the drained final cut can authorize publication. Receiver shadow
  claims remain invisible until that cut; schema/activation mutations remain
  fenced while the shadow exists.
- Native and portable dependency-complete cohorts use the shared hidden-target
  restore and global activation barrier. The independent table endpoints adapt
  to that same engine only when a certified cohort proves the complete selected
  dependency set; unrelated historical snapshots remain rejected.
- Coherent hot-standby seed replica materialization has a distinct internal entry point
  after topology validation and requires exact durable namespace identity.
  It does not enable independently restoring historical table backups.
- Distributed topology verification must check the real routed claim/reference
  handoff after failover and cutover; local range-copy helpers alone are not
  evidence of the distributed protocol's correctness.
- Deferred FK enforcement checks the final state of an atomic mutation or an
  existing multi-request transaction session at commit. Immediate checks run
  for each staged statement; SET CONSTRAINTS is not provided. NO ACTION and
  RESTRICT remain distinct. Referenced UNIQUE declarations are nondeferrable.
- MATCH PARTIAL selects parents matching all non-NULL child components; all-NULL
  children are exempt. Alternative witnesses carry exact claim guards so two
  concurrent removals cannot both commit and orphan a child. Selective ordered
  lookups and typed residual checks share the request's row/byte/time limits.
  NULL-distinct parent tuples have row-specific witness claims, without imposing
  uniqueness among those nullable tuples. Parent actions apply only when the
  final request leaves no surviving witness; update cascades preserve wildcard
  NULL child components.
  DDL creates missing parent support indexes in the reserved `__fk_partial_`
  namespace using ordinary schema CAS and persistent index builds. Activation
  and mutations remain retryably fenced while these indexes build. Public
  callers cannot create/modify/drop owned support definitions; cleanup checks
  both active and retained read-schema FK dependencies before a normal CAS.
  Interrupted child creation leaves discoverable support definitions for the
  maintenance worker; it never rolls back an ambiguously committed parent CAS.
  Shared supports retain the union of live covered columns, and user-created
  eligible indexes remain user-owned. Self-references publish support and FK
  definitions in the same table schema update.
- Constrained raw transforms remain unsupported.
- Failed-activation repair is administrator-only and row/schema conditional.
  Replacement values still satisfy UNIQUE/FK constraints. Separate retry
  restarts failed coverage; it does not grant ordinary writes repair authority.

The continuation adds public `/constraints/repair`, `/constraints/retry`, and
`/constraints/retire` operations with generated SDK contracts. Retirement status
includes durable phase/failure diagnostics. A paused retirement retry preserves
the exact job and prior drain progress instead of re-enabling ordinary writes.

The [restore architecture](relational-restore-architecture.md) uses a consistent
cohort and isolated new targets with atomic publication. The existing native
cluster backup/restore jobs now drive disk-backed pins, hidden placement,
replicated logical row import, distributed claim reconstruction, and publication.
Independent historical table restoration remains closed because unrelated
snapshots do not prove a consistent cross-table cut. The architecture document
distinguishes component regression coverage from the remaining distributed
release-verification matrix.

## Verification

The relational fault continuation also runs actual three-metadata/three-data
clusters. Online parent-owner and child-owner crashes during snapshot copying,
plus an accepted source-release reply loss, pass with acknowledged tail writes,
post-cutover UNIQUE/FK checks, and new cascading deletes. Separate cascade tests
pass after owner failure and accepted participant-reply loss; constrained-session
tests cover deferred final-state validation and savepoint rollback. These are
specific tested failure windows, not an exhaustive distributed fault matrix.

Default-on online-admission continuation: the executable builds in 37/37 steps.
Native tests pass 76/76;
metadata/driver tests 11/11; configuration/recovery-only tests 6/6; compiled
owner/source/enrichment/private boundary tests 56/56, all without leaks.
The deterministic matrix reconstructs every forward phase, loses effect and
metadata-CAS replies, and exercises exact-scope cancellation/cutover races.
Separate-archive shard callback tests prove independent error ordinals and
preserve all ten action signatures, retry outcomes, retained adapters, and
owned topology-read payloads. The error-registry suite passes 12/12.

Real six-process online tests cover publication/coordinator restart,
snapshot/donor restart, receiver-finalize/receiver restart, post-cutover source
release/coordinator restart, data-Raft quorum loss, and an accepted source release
whose successful reply is dropped. Snapshot interruptions include a retained
overwrite and insert after the immutable snapshot cut. Tests verify a chunked
2 MiB row, receiver base rows, an unrelated range, stable online scope, committed
cutover, and source release. Portable/native mixed document/relational restore
also passes coordinator and owner restart coverage. These are correctness and
recovery tests, not throughput measurements or exhaustive power-loss testing.
The final executable passes all eleven cluster scenarios: ten default-on,
restore, and fault-injection cases in one run, plus the explicit-disable guarded
merge with the same 2 MiB row. A compiled-owner regression independently proves
the guarded finalization lease contention and its resolution while retaining
the transition admission gate. Formatting, Zig generated-source checks, and
`git diff --check` pass. The repository-wide license-header check still reports
pre-existing drift outside these new files; this continuation does not rewrite
unrelated license declarations.

Incremental cleanup/artifact-transfer/online-state continuation: the final
production/snapshot build passes 43/43 steps. Native relational tests pass 68/68;
durable metadata/driver tests 6/6; compiled owner 37/37; compiled
owner source 6/6; enrichment boundary 9/9; private wire 8/8; snapshot controls 3/3.
All report zero leaks. The two-owner compiled test exercises the actual bounded
artifact adapter, donor-replica namespace isolation, cancellation, corrupt chunks,
lost acknowledgements across reopen, and the post-rename publication crash window.
Ten thousand files are reclaimed in 162 bounded pages with no payload reads;
a 16 MiB raw snapshot control spool writes successfully with a fixed 16 KiB
writer-allocation budget. These are work/space regressions, not throughput claims.
Six-process mixed document/relational restore passes for portable and native
backups against the final rebuilt binary (2/2 in 66.23 seconds), including
coordinator and owner failures. Generation, formatting, format checks,
generated-source checks and `git diff --check` pass. These
restore tests predate the production online adapter/native retained-state
snapshot integration and do not certify those later changes.

Durable pin/reservation/chunk continuation: the combined production executable
and compiled boundary/wire build passes 69/69 steps. Native relational tests
pass 62/62; API transaction contracts 54/54; compiled owner 36/36; compiled owner
source 6/6; private wire 8/8; enrichment boundary 9/9; raw receiver projection
6/6. ReleaseFast retained-effects tests pass 30/30 and native DB transaction
tests pass 47/47, with zero leaks. Work-count checks confirm zero extra probes
for 10,000 inactive mutation touches and three point reads/one write/no scans
for reservation replacement. Maximum-size chunk encoding uses fixed 4 KiB
scratch and has less than 1.5x wire expansion in the binary-payload fixture.
These are work/space bounds, not an end-to-end throughput benchmark.
The final six-process mixed document/relational restore scenarios pass for both
portable and native backups (2/2 in 78.37 seconds), including coordinator and
owner failures. These historical results do not certify the later online
copy/cutover coordinator or replace its distributed network-partition matrix.

The full Debug DB transaction run hit a MachO stack-unwinder crash in the
existing borrowed-VoprIo transaction-recovery fixture. Its sampled unwinder
then deadlocked in the signal handler; this is not reported as a passing run.
All other 46 named cases (57 including imported manifest checks) pass Debug,
and the full 47-case suite passes ReleaseFast without a source-level skip.
Generation, formatting, format checks and generated-source checks pass. The
repository-wide license-header audit remains red on 1,072 headers outside this
continuation's new source modules; no repository-wide rewrite was performed.

Source retention and cold restore recovery continuation: production build 37/37
steps; native relational suite 58/58; transaction contracts 54/54; compiled owner
36/36; compiled owner source 6/6; private batch/standby wire 8/8; enrichment
boundary 9/9; private lookup authority/wire 2/2. Focused retained-effects 25/25,
raw projection 5/5, and quota HTTP/client 3/3 also pass. These Zig suites report
zero leaks. The final six-process mixed document/relational restore tests pass
for both portable and native formats (2/2), including coordinator failure and
owner restart. Formatting, generated-source checks and `git diff --check` pass.
These historical restore results do not certify the subsequent online
merge/cutover protocol or replace the broader constrained-restore network matrix.

Earlier covering/CHECK/topology/recovery continuation: production build 37/37
steps; native relational system suite 18/18; compiled storage owner 31/31;
transaction contracts 50/50; public row contracts 21/21; index status 2/2;
maintenance/inventory 12/12; maintenance CLI 11/11; focused serverless lifecycle
2/2; and data-storage key-only projection 3/3. Targeted native runs report no
leaks. Go SDK tests pass; Python 155 pass; TypeScript 304 pass with one existing
skip and clean typechecking. Generation, generated-source checks, formatting,
format checks and `git diff --check` pass. These earlier tests do not certify
the subsequent coordinator implementation or a full distributed network matrix.

The FK extension's final focused runs pass 50 native storage/transaction/hot-standby
tests, 34 coordinator/activation/security tests, 10 public row/status tests,
two actual HTTP row/DDL authorization tests, and two authoritative metadata
constraint tests, with no test leaks. The root regression suite also passed
766 tests before the final review fixes; the focused runs above cover those
fixes. The complete Go SDK suite, 10 Python tests, seven TypeScript tests and
TypeScript typechecking passed. Full generation and OpenAPI freshness checks
passed. These are correctness/allocation-bound tests, not an end-to-end FK
throughput benchmark or proof of distributed topology handoff support.

### Historical foundation verification

The CHECK-only foundation below was committed as `5f42cf694`. These historical
test results do not certify the newer FK or public typed-row extension.

The CHECK implementation passed 90 combined storage tests and 765 root tests.
The final six-test CHECK/activation run also covers malformed declarations,
allocation-failure cleanup, exact integers, collation, NULL, failed validation,
retry, durable transaction fencing, stale-source publication and LSM reopen.
`make generate`, `make fmt`, `make zig-openapi-check`, Go SDK tests, five Python
schema tests, 36 TypeScript contract tests and TypeScript typechecking passed.
The source-format check and `git diff --check` passed after the final tests.

The continuation added 95 passing focused storage tests (including the existing
index tests and new reader, predicate, shared-guard, and TTL cases), and the root
suite passed 763 tests. All 19 dedicated distributed transaction contract tests
also passed. `make fmt` and `git diff --check` passed. The read-guard tests
cover LSM reopen, compatible readers, upgrade conflicts, ordinary writes,
absence protection, cumulative admission/retry, allocation-failure atomicity,
and no mutable-memtable cloning for conflict probes. These tests do not establish
FK enforcement or public typed-row API coverage.

The preceding integration run passed 125 targeted storage/schema/portable/TTL/split
tests and 763 root/HTTP/availability tests, with no leaks or unexpected logged
errors. Two build/cleanup integration tests were rerun after removing retirement
reverse reads and adding the retired-corruption regression. Go SDK tests, 36
TypeScript contract tests and typechecking, four Python schema tests, full `make generate`,
`make zig-openapi-check`, `make fmt`, and `git diff --check` passed. The initial
root/Go runs could not bind local test ports in the sandbox; approved reruns
passed. Repository-wide license checking still reports unrelated existing
files; the newly introduced jobs/GC headers were corrected.

Focused tests cover composite ordering and framing, null/collation equality,
definition fingerprints, plan/epoch lifetimes, stale preparation rejection,
late-failure rollback, and allocation-failure cleanup. Reserved batch buffers
can prepare keys after the parsed JSON has been released without allocating.
Catalog tests exercise atomic staging on both memory and LSM backends, injected
failure between blob/head writes, on-disk LSM reopen, idempotent retry, corrupted
or missing records, generation non-reuse, and allocation failure before publication.
Record tests exercise composite physical keys, document-range containment,
reverse checksums, atomic primary/forward/reverse rollback on memory and LSM,
unchanged-key elision, repeated-key batch coalescing, and allocation-failure
cleanup/poisoning. An on-disk LSM test recompiles fresh plans after each reopen
and verifies insert/update/delete persistence, including absence of obsolete
forward entries. These do not constitute public index API coverage.
DB tests additionally cover catalog recovery on reopen, schema/index epoch
alignment, rejection of indexed-column removal, pre-publication restore failure,
portable definition round trips, production composite-key mutation/transaction
replay/reopen/deletion on LMDB and LSM, canonical index reconstruction through
both restore paths, mixed historical ordinals and unused older schemas, and
actual DB split source/destination ownership. Manifest tests cover catalog
ordering, checksums, uniqueness, completeness, and schema binding. Public index
row query and distributed readiness coverage were added in the current integration
pass; the remainder of this section records historical foundation verification.
Local lifecycle tests cover creation on existing rows through public schema
JSON, outstanding transaction-lease rejection, live update/delete races,
competing page CAS, durable failure/retry, LSM/LMDB reopen, ownership changes,
600-row multi-page retirement, retired corruption, and private metadata guards.
Retirement is generation-local: each reverse membership has a compact private
`(generation, slot, document)` ownership locator committed atomically with it.
Bounded GC pages visit only that generation's forward and ownership prefixes,
so missing forward entries cannot hide reverse records and unrelated primary
rows do not contribute cleanup work. Locators add one key on membership insertion
and deletion, but are not rewritten for tuple/payload updates or unchanged rows.
Range transfer prunes/reconstructs locators with the other derived companions;
portable backups omit them and restore rebuilds them from canonical rows.
The LSM regression with 10,000 unrelated rows visits zero records for an empty
generation and nine for five memberships with one missing forward, reopening
between both cleanup pages.

Expiration regressions cover the actual stopped TTL runtime context, retired and
current physical generations, and empty-table transitions after a split.
The existing adaptive-aging regression now permits bounded maintenance slices
with a frozen test clock and isolates clock rollback on a fresh clean range.
This removes single-pass and leftover admission-pressure assumptions without
changing production planner policy.

The tuple microbenchmark compares key extraction from an already prepared row
against full-row materialization plus JSON parsing: 200 iterations over a 64 KiB
payload allocate zero additional key-buffer bytes versus 71,594,400 cumulative
control bytes. This is neither a peak-memory measurement nor an end-to-end
write-throughput result.

### Cohort portable and selected-table restores

Unpublished imports acknowledge durable writes without the generic post-proposal
serving-table visibility lookup. Their native scoped apply still synchronizes
derived work; the final owner validation barrier independently drains index/CHECK
coverage before its durable receipt. Unconstrained document and typed tables skip
distributed activation reads, but never skip that physical readiness barrier.
Private validation and transaction requests carry a restore plan locator beside
their exact owner-scope digest. Cold compiled owners recover their descriptor
from that authoritative plan, checking its identity, phase and namespace without
falling back to public table discovery. Acquisition retains request cancellation
and deadlines; hidden validation/status reads still require read-index. Scoped
participant IDs persist the locator in the existing durable transaction participant
set, so resolution, acknowledgement and cleanup do not depend on a warm cache.
The six-process mixed document/relational restore regression passes for both
portable and native artifacts with metadata-coordinator failure and owner restart.

Portable cohort artifacts now derive from the same durable native seal as
native artifacts, after the common-cut write fence is released. LSM export
reads the sealed primary directly; logical-backend seals use a disposable,
disk-backed decoder. The portable manifest includes the exact seal and source
namespace, but excludes routed claims, references, and constraint generations.
Only an unpublished logical-source import with the authenticated aggregate's
matching proof can decode this representation. Ordinary table-local portable
publication remains unable to bypass distributed constraint validation.

Selected tables must include all outgoing FK dependencies from both active and
read schemas. Missing parents are never bound to unrelated live data, and
selection is never silently expanded beyond the caller's authorization scope.
All selected targets receive fresh identities, normal prepared-row imports,
and global UNIQUE/FK rebuilding before atomic publication. Skipping an existing
parent cannot make a restored child's dependency valid.

Coverage includes mixed document/parent/child portable restores, invalid-child
rollback without publication, exact-seal exports after restart and later writes,
proof mismatch/corruption rejection, and dependency-closed partial selections.
Portable materialization persists a ranged-download SHA prefix, then indexes the
authenticated object directory once into its unpublished LSM decoder. Each
object is verified once and document objects become small reusable row pages;
row writes, rebuilt source identities, and the decode cursor commit atomically.
Historical-layout validation also advances with a durable bounded cursor.
Cancellation and restarts resume both phases without downloading the prefix or
decoding earlier objects again. Decoder admission is bounded by the existing
archive object/manifest limits, with at most 128 logical rows per import slice.

### Review hardening: retry ownership, activation proofs, and scan bindings

Restore dispatch excludes IDs whose previous execution slot has not yet been
released. A worker may durably queue its next slice before completion, but a
concurrent dispatcher cannot consume that successor prematurely. Unrelated
eligible jobs still run, and completion dispatches the retained successor.
Queue selection is failure-atomic and the ordinary FIFO path advances a head
cursor rather than shifting the retained queue.

Stable transaction retries consult the authoritative coordinator decision after
any failed begin, including forwarded errors that lose their original domain
classification. A durable commit resumes commit-only delivery; an unavailable
decision remains unknown and never authorizes abort. This adds no probe to the
successful-begin path and preserves recovery after a lost acknowledgement.

Constraint failure publication retains physical source-row guards, including
oversized projection failures. UNIQUE/FK rejection is revalidated after the
failed mutation transaction: terminal failure includes an exact claim guard
(including absent-parent guards) in the same distributed transaction as the
activation checkpoint. A repaired observation causes a retry, not INVALID.
Row-local MATCH FULL and CHECK failures carry their original row guards.
MATCH PARTIAL scans cannot supply a negative point predicate for an absent
witness; their missing-parent diagnostics therefore remain `validating`, with
the reason exposed in range status, and are automatically rechecked. They never
advance coverage or require an operator retry just because a parent arrived
after the scan. Successful progress clears the diagnostic.

Typed primary/fallback scans retain snapshot-local source layout, projection,
and predicate bindings in an eight-entry LRU with a 2-MiB retention budget.
One oversized binding may occupy the working slot alone. Layouts are loaded
from the pinned store snapshot, never a process-wide version-number cache.
This keeps alternating historical/current rows from recompiling a layout per
row while bounding retention under schema churn. Work-count regressions cover
512 alternating rows, eviction across ten epochs, and allocation-failure cleanup.

Retained transaction aborts deliver to the complete participant cohort, even
when this invocation never reached a follower or its BEGIN was not proposed.
Only fresh transaction IDs can use invocation-local contact evidence to skip
delivery. Failed delivery (including a missing follower record) stays enlisted
for durable recovery rather than acknowledging away an earlier execution.

Administrator repair also admits a validating `ForeignKeyParentMissing`
diagnostic. It keeps ordinary writes gated, applies normal constraint checks,
and guards the exact activation checkpoint through commit. Successful validation
clears the diagnostic; repairs never themselves advance coverage. Hidden restore
cohorts remain immutable and continue to reject invalid data before publication.

Index construction visits one document prefix per primary pass, seeking past
unrelated artifact/index companions. Forward verification scans the target
generation prefix; reverse verification scans its generation-local ownership
records. The primary pass also checks reverse-only orphans without ownership,
so this optimization does not abandon repair after a lost companion. Retired
generations use their own bounded GC jobs. A persisted-LSM regression with
64 rows and 1/8/32 populated indexes asserts exactly 192 scan records per index
across all three phases, independent of unrelated generation count.

Primary typed-row scans and local CHECK validation now share the physical
document-prefix successor rule as well. Each step visits one document family,
probes the exact primary if a companion sorts first, and seeks past all remaining
companions. Logical key extensions (including embedded NUL and 0xff) are not
skipped. Reader continuation remains failure-atomic and snapshot-local; CHECK
progress remains durable, with invalid-row publication guarding the exact primary
bytes. Persisted-LSM work-count tests cover 1/8/32 indexes, single-record pages,
orphan companions, bounded scans, concurrent mutations, OOM/oversized-page retries,
and validation restarts.

The index maintenance scheduler retains a catalog/namespace-bound sweep across
time- and entry-limited slices, rather than treating a catalog larger than one
slice as perpetual backlog. A complete clean sweep returns to the idle cadence.
Atomic wake tickets preserve repair requests arriving during clean publication;
round-robin position survives wakeups and transient failures. Only catalog or
namespace changes invalidate ordinal position. Concurrent background passes are
coalesced without waiting, and restore projection work uses an independent cursor.
