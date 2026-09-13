# Relational-indexes extraction status

This is an implementation ledger, not a statement of available API features.
The follow-up spans R3–R6 from `docs/restructure-prs`: relational indexes and
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
  backup. The local DB methods are not distributed/admin HTTP activation APIs.
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
  The initial execution compiler accepts plain ordered-tuple indexes and rejects
  unsupported constraint, partial, expression, and covering semantics explicitly.
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
  existing atomic public-schema/HA publication path. Introducing an index,
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
  was introduced. Readiness/status/retry are currently DB methods, not public
  distributed HTTP administration/query endpoints.
- `relational_index_gc.zig` walks each retired generation's forward namespace,
  reconstructing reverse keys from ownership framing without primary/reverse
  point reads during preparation. Retired-value corruption does not prevent
  deletion. Cleanup cursors and deletions commit atomically and survive reopen;
  split destinations inherit cleanup authority with reset cursors. Retirement
  records commit with catalog replacement, and superseded definition blobs are
  deleted in that transaction. This avoids repeated full-table vacuuming and
  unbounded retained definition blobs; queue admission and orphan scrubs remain
  separate integration work.

Catalog ownership and schema/restore publication are integrated into DBCore.
Prepared index keys and record effects are consumed by production row mutations.
Generation selection alone does not constitute a uniqueness check or predicate
evaluation. Local readiness now has a durable coverage proof, but public row
query/constraint execution has not yet been extracted.

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

Remaining: public/generated query and mutation contracts and adapters; bounded
distributed query execution; primary/unique claims and FK enforcement/action
jobs; CHECK activation and validation coverage; generated/default values;
cross-shard integrity/topology/failover tests. Shared read guards are necessary
for FK safety but do not enforce foreign keys by themselves. In particular,
FK key-to-parent resolution, globally routed unique claims, parent-delete
admission, and cascade/restrict semantics are not implemented yet. Nothing in
this section changes that remaining feature scope or makes the PR ready to push.

## Remaining integration gates

- [x] Implement immutable-definition storage and durable-then-visible write-plan
      publication, including CAS, checksums, transaction aborts, and LSM reopen.
- [x] Wire the catalog/controller into DBCore startup, schema changes, and
      prepared restore publication; preserve definitions in portable manifests.
- [x] Persist/recover mutable progress separately; declarative definitions use
      the existing public-schema HA payload and atomic schema/index commit.
- [ ] Add explicit relational-definition HA failover/replay integration tests
      and distributed readiness/admin status aggregation.
- [x] Validate both the published plan snapshot and current schema in the actual
      DB mutation path when consuming prepared effects.
- [x] Define generation-scoped forward keys, row-owned reverse cleanup records,
      and coalesced atomic-batch effects with strict failure handling.
- [ ] Integrate those records with split/merge, restore, repair, and every
      production deletion path. Range transfer must derive forward companions
      from selected reverse records, not copy the global forward namespace.
      Local DB split, portable restore, and local readiness/retry are covered;
      distributed transition replay and owner-coordination still need audit.
- [ ] Commit primary rows, old/new index effects, unique claims, catalog changes,
      and HA/outbox records atomically across normal writes and transaction replay.
- [x] Enforce named typed scalar CHECKs and add bounded local coverage/retry.
- [ ] Implement covering payloads, deterministic expression/partial indexes,
      composite uniqueness/null policies, defaults/generated values, and richer
      CHECK expressions.
- [x] Add local bounded build/drop jobs, durable continuation CAS, cancellation,
      ownership fencing, failed status/retry, and range-local coverage proofs.
- [ ] Finish integrity repair/orphan scrubs, retirement admission headroom,
      schema-dependency generation reuse, and public query-readiness gates.
- [ ] Port foreign-key validation/enforcement and distributed job coordination.
- [ ] Extract shared row read/mutation execution from SQL-owned code, then expose
      supported operations and generate all SDK request/response types.
- [ ] Benchmark actual index writes, rebuilds, range queries, and post-churn
      storage/read amplification on the LSM before declaring performance wins.

## Current verification scope

CHECK work is not FK enforcement/actions and does not expose public typed-row
query/mutation endpoints. Those remain unfinished, along with distributed
constraint-coverage aggregation. Generated CHECK declarations must not be
represented as completion of those features.

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
row query operations and distributed lifecycle/readiness remain outside coverage.
Local lifecycle tests cover creation on existing rows through public schema
JSON, outstanding transaction-lease rejection, live update/delete races,
competing page CAS, durable failure/retry, LSM/LMDB reopen, ownership changes,
600-row multi-page retirement, retired corruption, and private metadata guards.
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
