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
evaluation. The FK and typed-row extension below adds coordinated uniqueness,
FK enforcement, and public primary-order row execution; secondary-index query
routing is still a separate integration gate.

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

This foundation's read guards are consumed by the FK extension below. Remaining
work includes distributed secondary-index queries, generated/default values,
coordinated constraint retirement and repair, and claim-aware online topology
handoffs. Local CHECK coverage remains separate from the public UNIQUE/FK
activation status endpoint.

## Remaining integration gates

- [x] Implement immutable-definition storage and durable-then-visible write-plan
      publication, including CAS, checksums, transaction aborts, and LSM reopen.
- [x] Wire the catalog/controller into DBCore startup, schema changes, and
      prepared restore publication; preserve definitions in portable manifests.
- [x] Persist/recover mutable progress separately; declarative definitions use
      the existing public-schema hot-standby payload and atomic schema/index commit.
- [ ] Add explicit relational-definition hot-standby failover/replay integration tests
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
- [x] Commit primary rows, old/new index effects, unique claims, catalog changes,
      and hot-standby/outbox records atomically across normal writes and transaction replay.
- [x] Enforce named typed scalar CHECKs and add bounded local coverage/retry.
- [ ] Implement covering payloads, deterministic expression/partial indexes,
      composite uniqueness/null policies, defaults/generated values, and richer
      CHECK expressions.
- [x] Add local bounded build/drop jobs, durable continuation CAS, cancellation,
      ownership fencing, failed status/retry, and range-local coverage proofs.
- [ ] Finish integrity repair/orphan scrubs, retirement admission headroom,
      schema-dependency generation reuse, and public query-readiness gates.
- [x] Port foreign-key validation/enforcement and distributed activation pages.
      Ordinary referential actions stay atomic, not asynchronous sagas.
- [x] Extract shared row read/mutation execution from SQL-owned code, then expose
      supported operations and generate all SDK request/response types.
- [ ] Benchmark actual index writes, rebuilds, range queries, and post-churn
      storage/read amplification on the LSM before declaring performance wins.

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
- UNIQUE/FK split/merge requires distributed quiescent integrity protocol v1.
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
- Deferred constraints, MATCH PARTIAL, and constrained raw transforms are not
  exposed as supported operations.
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

### Cohort portable and selected-table restores

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
