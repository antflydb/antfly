# DB Contract

This note is the DB-layer landing page for contract decisions, storage backend
boundaries, and local-shard execution design. Genuinely unshipped work is
consolidated in [Open work](#open-work) at the end of this file.

For the canonical enrichment architecture and artifact identity contract, see
[ENRICHMENTS.md](ENRICHMENTS.md).

## DB Handle And Runtime Ownership Contract

`DB.open()` never leaves the caller's role ambiguous. A handle is exactly one of:
the authoritative write owner for a shard, the shared read owner for a visible
generation, or a foreground maintenance handle scoped to a transition. The
serving layer leases these roles; callers do not open them ad hoc.

### Core Invariant

The DB layer separates three concepts:

- storage capability: whether the handle can write the primary/index stores
- runtime initialization: whether helper runtimes are constructed so foreground
  code can use their replay/apply logic
- runtime worker ownership: whether background workers are started and allowed
  to keep running after open

```text
one background runtime owner per shard root
one cached write DB owner per table group
one cached read DB owner per table group/generation
exclusive generation transitions own maintenance opens and publication
queued exclusive transitions reserve both read and write admission
transactions retain the backend generation until abort or commit
cursors retain their parent transaction until cursor close
returned owned buffers are allocated in an explicit caller ownership domain
foreground handles do not wait on background workers they did not start
```

### Read And Write Owners

Ownership rules:

- Reads, cursors, lookups, scans, queries, and status reads are leases on the
  one cached read owner for a table group and visible root generation. A "read
  profile" never creates a second cached DB identity over the same root;
  duplicate instances would duplicate index/cache/runtime state and make
  restore, drop, and schema mutation depend on every caller's invalidation.
- Maintenance DB opens exist only inside a callback scoped to an exclusive
  table/group transition lease; raw handles cannot escape it, and no fallback
  path releases exclusivity and reopens the same root.
- Point-lookup exception: when the generation-matched writer/apply DB is
  resident, a lookup leases it and reads its primary document store directly
  instead of opening the query DB (which would load the full index catalog and
  add a backend generation over a path under WAL publication and compaction).
  Query-only processes and cold groups use the read cache. The lookup returns
  `null` rather than adopt an owner whose visible root generation or identity
  namespace does not match.
- Split/merge projection uses a group-exclusive transition lease: it queues
  ahead of later writers, briefly drains reads while managed auto-bulk state is
  published, then reopens reads from the published generation for snapshot
  scan and repair. An instantaneous cache-idle probe is never used as
  quiescence.
- Once an exclusive transition queues, new reads and writes wait behind it while
  admitted reads drain, so sustained traffic cannot starve the transition.
- Storage-backed status and schema-capability inspection are reads: they take
  the same table read activity as lookup/query before opening a status-only
  DB, and transitions drain them. Cache-only status snapshots need no lease.
- In raft-backed serving, the apply state machine's write source is the
  canonical local write owner. Startup catch-up and other maintenance resolve
  that owner before acquiring an activity lease or opening a DB; the startup
  cache attaches to it and drains at the end of each catch-up. A forwarding API
  source never creates a second activity or cache domain over the same path; its
  read-preparation and primary-lookup interfaces delegate to the canonical
  owner even when captured before owner attachment.

Implementation: generation-owned entries in `ProvisionedTableWriteCache` and
`ProvisionedTableReadCache`, the process `generation_lifecycle` manager, and a
local table generation transition around restore. Cache entries own their DB;
invalidated entries are retired and stay alive until their last lease releases.
At the storage boundary, LSM transaction opens are rejected once generation
close begins, backend close drains active transaction readers, and erased
cursors retain their transaction box.

The Raft apply store's per-group DB owners are a bounded LRU charged to the
node's shared `ResourceManager` under `lsm.in_memory_state`: normal pressure
keeps the per-stripe bound, soft pressure shrinks it, and hard pressure keeps
only the most recently needed unpinned owner. Snapshot staging uses the same
manager. Eviction closes only the runtime owner; durable files and apply
watermarks remain for transparent reopen.

### Restore Preparation And Publication

Restore flow, job, and manifest semantics are owned by
[BACKUPS.md](BACKUPS.md); this section owns the DB generation boundary.

Capabilities:

- Preparation lets existing and new readers continue against the live
  generation while Antfly imports, repairs, validates, and syncs an isolated
  sibling. It blocks new writes for the restore's lifetime so acknowledged
  writes cannot be discarded by publication.
- Publication, drop, and in-place schema mutation take an exclusive transition
  lease that blocks new reads and writes; drains read, write, and pending-open
  leases; invalidates read, write, startup-write, runtime-status, and shared
  path-scoped caches for the generation; performs only the live file/catalog
  mutation and unavoidable live repair; and publishes the new visible
  generation only after repair and invalidation complete.

Generation rules:

- Restore never reconstructs the live root in place. It acquires a process-wide
  preparation capability for the exact shard root, creates a unique sibling
  staging generation, imports and validates there, runs foreground
  derived/runtime repair against the staged path, closes, recursively syncs,
  and seals it, then drains serving leases and promotes to the publication
  capability.
- A prepare or repair failure destroys staging and leaves the live generation
  untouched.
- macOS and Linux publish with atomic directory exchange; platforms or
  filesystems without it reject replacement before mutating the live namespace.
- The exchanged old root goes to the shared backend runtime cleanup lane;
  recursive reclamation is off the publication path. If the parent sync after a
  visible exchange fails, the previous generation stays under the staging name
  until reconciliation confirms durability and schedules cleanup.
- `DB.restoreSnapshotToDeferredRuntimeRepair()` requires a `StagedGeneration`
  capability and rejects any path that is not its staging root; raw path
  possession cannot mutate a serving root. Startup, provisioning, and API
  restore all acquire capabilities through the process generation-lifecycle
  manager.
- `TableWriteSource.restoreTable()` acquires and releases the table restore
  reservation itself. Cluster restore, which holds the reservation across
  metadata replacement and remote staging, uses `restoreTableReserved()`.

Authority and identity rules:

- External storage authority survives distributed restore as a named
  `external_io` connection, not a URI. The ingress node checks `restore.read`,
  validates the manifest once, and submits only the connection identifier plus
  per-shard path, size, and digest to metadata. Credentials are never written to
  Raft; each replica resolves the identifier against local configuration so
  bucket, prefix, and capability policy apply where the I/O happens.
- The low-level Raft host never resolves a backup URI or ambient credentials; it
  validates the bootstrap identity and delegates I/O to the managed bootstrap
  owner. A missing connection or owner, malformed relative path, or absent
  digest fails closed before any filesystem or object-store access.
- Range restore intent, replica bootstrap record, local import marker, and
  completion progress carry the same shard digest, so retries and crash
  recovery are idempotent per immutable artifact and stale completion cannot
  satisfy a newer intent.
- Restore identity is range-scoped; table definitions are not a fallback source.
  Manifest admission sorts shard boundaries and rejects gaps and overlaps
  before topology publication.
- Import and repair progress markers are strict versioned JSON with a mandatory
  artifact digest, written by file sync, atomic rename, and parent sync.

Overwrite and completion rules:

- Overwrite-mode cluster restore preserves the existing table ID, ranges, and
  target shard IDs; it stages source data into each target's sibling generation
  and repairs it with the backup's schema and index catalog. Only after sealing
  does the exclusive transition upsert the definition and exchange the
  generation.
- Metadata publication rolls back only if storage publication fails before the
  namespace exchange; after the exchange, durability uncertainty is a committed
  result. Publication and rollback are full-definition compare-and-swap, so a
  concurrent schema, index, placement, or restore-intent update fails the
  transition closed.
- Restore completion is a replicated compare-and-clear carrying the exact table,
  group, backup, location, connection, snapshot path, byte count, and digest. It
  clears only matching restore fields and preserves range ID, boundaries,
  identity namespace, split epoch, and other topology; a delayed completion for
  a superseded artifact is a no-op.
- A committed exchange with a failed parent sync is reported as durability
  pending. Retrying the same backup reconciles publication and reports
  committed/durable; a retry against an unrelated existing table is rejected
  without modifying storage.
- The restore-job journal mirrors generation state: before irreversible work it
  persists one active manifest ordinal; publication moves it to a
  durability-pending or published range, and replica convergence moves it to
  completed. Ranges are disjoint where ownership differs and validated on load.
  Job IDs, idempotency scope, table limits (4096 per cluster, 256 per explicit
  list), and range encoding are in
  [BACKUPS.md](BACKUPS.md#durable-restore-jobs).
- Restore dispatch is generation-fenced: leadership pauses FIFO admission,
  drains the old runtime owner, rebuilds durable attempts, publishes the new
  term, then reopens admission. Completion callbacks request another dispatch
  pass instead of blocking on a dispatcher that may be destroying them.

### Backup Publication

Backup publication has a separate immutable commit point. Manifest formats
are owned by [BACKUPS.md](BACKUPS.md): the table backup manifest (format
version 2, explicit `native` or `portable`, every shard bound to its group,
range, relative artifact path, byte count, and SHA-256 digest) and the canonical
content-addressed repository of refs, manifests, seals, and blobs. The DB-layer
rules are:

- shard artifacts live under a cryptographically random internal operation
  generation, so an interrupted attempt cannot make a later request reuse or
  overwrite stale bytes; the caller's `backup_id` names only the manifest
- every shard transfer completes before the manifest is conditionally
  published; filesystem backups sync each copied file, sync directories from
  leaves to root, and file-sync, atomically rename, and parent-sync the
  manifest, so the rename is a crash-durable commit point
- native digests cover a canonical tree encoding of sorted normalized paths,
  sizes, and contents (independent of enumeration order, rejecting symlinks);
  portable digests cover the exact AFB bytes; hashing streams with bounded
  memory on the shared I/O runtime
- restore treats the manifest format as authoritative, rejects duplicate group
  or artifact identities and unbound artifacts, copies local and remote content
  into an Antfly-owned staging root, and verifies that private copy before any
  importer or publication consumes it; portable archives import block-by-block
  from a stable descriptor with memory bounded by one encoded block
- a digest mismatch or source mutation is a terminal corrupt-backup integrity
  failure that leaves the live generation and table definition unchanged; an
  unversioned or unsupported manifest fails closed before staging

### Data-Raft Snapshot Install

Data-Raft snapshot apply uses the same generation boundary:

- The apply path validates the canonical snapshot stream without materializing
  document values or per-document descriptors, builds a fresh staged DB in
  fixed-size borrowed batches, syncs and seals it, and atomically publishes it
  under a group-scoped generation capability. It never scans or deletes the old
  live table.
- Durable data-Raft projections are physically partitioned by group under one
  root-level coordinator that keeps the single process writer authority, so
  replacement cost is proportional to the incoming group snapshot.
- The apply store uses a per-group generation-preparation marker instead of
  holding its hash-stripe mutex during import. Target-group operations wait;
  other groups, including colliding stripe members and groups in the same
  table, keep applying and serving. Only reader drain, cached-owner retirement,
  atomic exchange, and allocation-free watermark publication run under the
  stripe lock.
- Shared cache-generation bookkeeping is reserved before staging and advanced
  without allocation right after the exchange, so the new root is never visible
  under the old cache identity. The generation reservation owns a map reference,
  so topology pruning cannot remove its bookkeeping mid-staging.
- Schema validation is parsed once per snapshot and applied per batch; derived
  replay stays durable in the staged generation for normal workers to resume.
- Publication retires and drains only the target group's owners behind an exact
  group read-cache admission fence. Table-wide drop, schema, index, and restore
  transitions wait for every group preparation and keep table-wide epochs.
- Exchanged old apply roots go to the backend runtime cleanup lane, off the
  Raft apply critical path.

Catalog contract:

- The install captures metadata Raft group identity, the metadata cluster's
  durable 128-bit incarnation, table identity and name, schema, index catalog,
  range topology, and identity namespace after group admission. The snapshot's
  encoded range must match the catalog range. Staging owns only this compact
  contract, so memory is independent of catalog size.
- After serving leases drain, the candidate is exchanged into the filesystem
  but stays outside serving admission. The catalog source then performs a Raft
  linearizable read (followers forward ReadIndex to the leader and wait for
  local apply) and compares the compact contract on the metadata replica. A
  mismatch restores the retained prior root before admission reopens; a match
  commits the exchange and advances the visible generation. Unrelated metadata
  changes do not force a retry.
- The fixed metadata Raft group id is not a cluster identity. Metadata
  initializes its incarnation exactly once through the replicated state
  machine and includes it in Raft snapshots. Data nodes pin the first valid
  incarnation they observe; every metadata endpoint, head/snapshot pair, and
  mutating control-plane request must prove it before affecting provisioning or
  publication. Missing, zero, or mismatched identities fail closed.

### Generation Publication And Durability

- Publication errors are pre-commit errors. Once a rename or exchange makes the
  new generation visible, publication returns `durable` or
  `durability_uncertain`, and the caller finishes cache invalidation and
  admission bookkeeping in both cases. A post-commit sync failure never enters
  the pre-commit abort or metadata-drop path; it reports committed with
  durability pending and leaves raft bootstrap inactive.
- Every staged candidate carries a durable two-phase publication record that
  moves into the live root with the exchange. `prepared` names the retained
  sibling, and startup atomically restores it (process death after exchange but
  before catalog validation). Catalog validation durably writes `committed`
  before admission advances.
- DB open completes committed publication: it syncs the parent namespace,
  submits retained or abandoned siblings to the runtime cleanup lane, and clears
  the record before admitting the root. Cleanup failures leave the generated
  sibling name as durable retry debt; they never block read admission.
- Every open DB holds a shared generation lease and a shared filesystem
  publication lock until `DB.close()`. An exclusive transition blocks new opens
  and publishes only after all prior owners close. The persistent sibling lock
  file also excludes overlapping restore publishers across processes.
- Reconciliation is cached only while a local reader holds the shared lock; the
  final reader invalidates it so a later open observes debt from another
  process. Cached read admission takes the shared lock before registering the
  reader, then revalidates under the manager mutex; if the last prior reader
  drained meanwhile, the opener retries through exclusive reconciliation.
- Stale-stage GC, under the exclusive lock, considers only names matching
  Antfly's full generated-stage grammar: marked abandoned candidates and
  markerless retired roots from a completed exchange. A manual runtime admits
  the reconciled generation and downgrades the lock before recursively
  reclaiming the identified paths.
- Retired-root deletion holds one persistent cleanup lock per parent directory,
  so duplicate workers serialize per table-group parent while unrelated shards
  reclaim in parallel. Workers recheck existence after locking; an already
  reclaimed generation is success.
- The root layout is path-relative and may lazily open run files after a
  transaction starts, so serving transitions stop admission and drain old
  owners before publication (refcounts make close ordering safe, not a path
  swap under a reader). Zero-downtime overlap of generation N readers with N+1
  publication requires immutable versioned roots plus an atomically published
  current-generation pointer.
- Multiple shard directory swaps are not crash-atomic. Until table generations
  publish through one durable table generation pointer, table backup and restore
  reject multi-range manifests right after decoding.
- LSM manifests are relocatable: on open, run paths are rebuilt from the current
  root and run identity, and obsolete run paths under the prior root's `runs/`
  are rebased.

### Shared Cache Invalidation

- Shared caches are invalidated by table-group root, not only by primary store
  path. Primary LSM caches and LSM-backed indexes use path-prefix invalidation.
- Dense/HBC uses a cache namespace per HBC index root; the cache registers
  namespace-to-path ownership, and restore or drop invalidates every namespace
  under the table-group root. Future shared index caches follow the same rule:
  engine-specific keys, table-root-scoped lifecycle owned by the generation
  transition.
- HBC namespace registrations are reference counted by open index owners and
  outlive the last owner only while retained cache entries use them; clear,
  path invalidation, and later opens prune unowned registrations.
  Per-namespace statistics retire at the same boundary.

### Restore Repair Runtime

Restore repair (for example generated chunks or embeddings) runs as foreground
maintenance under the table generation transition: it initializes only the
required runtime services, drives generated enrichment replay, drains
derived/index replay, syncs, and closes before reads become visible. It never
starts every optional runtime worker and never races a lookup/query open against
half-published files.

### Structural Reconciliation

- Queued structural reconciliation closes new write admission before draining
  current writers, so traffic cannot starve index/catalog convergence; reads
  stay admitted on the published generation until the short publication
  transition.
- Each queued request owns one immutable catalog contract captured from a single
  admin snapshot: table identity, desired schema and index definitions, and the
  exact range and identity namespace of every group. Before using the plan and
  before declaring completion, the catalog source validates a compact
  whole-table contract after a Raft linearizable-read barrier.
- The contract is an order-independent SHA-256 multiset digest plus range count
  over all range records: O(1) on the wire, allocation-free against the
  metadata owner's immutable projection, and sensitive to ranges added,
  removed, or mutated after capture.
- The catalog projection advances on a catalog-only epoch and is the sole owner
  of cloned table and range records; store reports, placement progress, and
  other control traffic live in a separate status projection with its own epoch
  and cannot evict it. Metadata builds non-owning table/range maps and topology
  digests once per stable catalog epoch.
- A dequeue advances at most a fixed number of pending groups, removes completed
  groups in O(1), and rotates busy groups. One compact contract admits the
  quantum; completed groups keep owned runtime observations until one
  post-mutation contract check accepts the quantum, then publish with the table
  epoch captured before storage work. Each productive quantum uses exactly two
  ReadIndex barriers.
- Productive quanta requeue at the tail with no delay; a short delay applies only
  when every attempted group is blocked. A changed contract discards only the
  stale plan; write admission stays reserved across the rebuild, and stale work
  cannot publish readiness for a replaced topology.
- Enqueueing any structural request, including a deduplicated one, invalidates
  the table epoch first, so an intervening catalog change, root transition, or
  new mutation makes the quantum's observations stale.
- Remote data nodes validate through a required internal route using a bounded
  pool of keep-alive clients on the process `BackendRuntime` API I/O lane. Nodes
  fail closed if group or whole-table fencing is unavailable.

User-visible table-definition mutation has a separate replicated fence:

- Each table owns an O(1) `TableTransitionFence` (monotonic generation plus
  active split/merge count), updated in the same metadata-Raft transaction as
  the transition record.
- Schema, index, enrichment, and other in-place definition changes
  compare-and-replace against both the expected definition and the fence.
- Table deletion carries the observed inactive fence generation and waits for
  committed absence; an intervening transition makes it a durable no-op and
  returns HTTP 409.
- Admission is reciprocal: a new split or merge verifies the current table
  contract and participating range identities in the transaction that creates
  it, and progress upserts can only advance an admitted transition.

### Runtime Status And Writer Ownership

Dirty visibility and status cache:

- Per-table dirty visibility tracking uses owned exact table identities, not
  hashes. Write-cache eviction advances the table's read-cache epoch; the last
  cache owner (serving or startup) also invalidates cached runtime status and
  retires the identity. A draining lease that mutates after eviction re-marks
  itself through the normal visibility hook.
- The runtime-status cache is an owned map by table, then by group. Each table
  has an `invalidation_epoch` and logical `root_generation`; restore, drop,
  schema/index replacement, and visible-root transitions advance the epoch and
  drop the group map, while routine writer status does not.
- A live publisher captures the table epoch and a monotonic observation
  generation before inspecting its DB and publishes in O(1); only a stale table
  generation or an older observation for the same group is rejected.
- A full refresh captures all table epochs and one observation watermark under a
  short lock, collects without the lock, and publishes all epoch-valid tables in
  one acquisition. Newer live observations win; tables invalidated during
  collection are recollected individually.
- Absence removal uses a separate topology revision: if topology changed during
  collection, removals are deferred, and partial retries never remove tables.
  Stable removal advances the global epoch allocator first, so an old owner
  cannot republish into a recreated name.

Writer ownership transitions:

- Configuration is published under one ordered lock set: cache-open locks by
  address, the source mutation lock, then cache lifecycle locks by address.
  Gate or mirror changes reserve serving and startup cache retirement before
  changing any source field, retire both caches, then invalidate read/status
  observations and dirty identities. Cache entries reserve their retirement
  queue slot at install, so transitions and final lease release are
  allocation-free.
- HA promotion preflights both live and raft-apply write sources before
  consuming standby ownership, so allocation failure aborts before irreversible
  changes. Promotion transfers the already-open receive-log owner after
  validating paths, the durable timeline-switch record, and the slot store; it
  never opens a second WAL writer or a close/reopen window.
- Embedded `BoundTableWriteSource` restore closes its owner, restores into a
  sibling, publishes atomically, and reopens the unchanged generation after a
  pre-commit failure or the new one after success. Provisioned serving uses
  separate prepare and publish operations. Startup/bootstrap paths publish a
  validated primary generation with a durable repair marker so the startup
  owner resumes derived repair after a crash.
- Replica bootstrap restore finishes before `ensureReplica()` activates the
  group; an active replica is never force-restored by raw path.

### Placement Reconciliation

Locking and admission:

- One reconcile-generation mutex serializes metadata snapshots; the Raft owner
  mutex protects only live `MultiRaft` mutation. Under the owner mutex,
  reconciliation captures an immutable desired-state plan and marks restore
  bootstraps preparing. Restore I/O, descriptor construction, and replica
  catalog fsync run without it, so the Raft progress driver keeps elections,
  heartbeats, transport, and apply going.
- Catalog admission is durable before a prepared descriptor is published. The
  owner mutex is reacquired only to install or retire replicas, refresh peers,
  reconcile membership, and publish the apply-store group set.
- Admission is two-phase: the apply store prepares the new maps and admits the
  union of old and new groups; success commits exactly the new set, while
  partial failure keeps the union until the next metadata sync narrows it.
  Exact commit retires owners and cached summaries without allocation; an owner
  pinned by snapshot materialization or publication is marked retired and
  closed by its last user.

Leadership and membership:

- `draining` sources and `cutover_ready` targets stay valid leaders until the
  expanded voter set converges. A `retiring` leader transfers leadership to the
  lowest-ID retained voter before proposing self-removal; if it already
  disappeared, that voter campaigns after reconciliation.
- Reconciliation resolves transport endpoints for all voters and learners before
  proposing membership changes, so a committed learner can always receive its
  log or snapshot.
- Internal leader-only routes return a typed unavailable response during
  handoff rather than honoring a removed leader or returning a generic HTTP
  failure.
- Relocation and replica-count shrink use two committed phases. A `draining`
  source stays a voter (and may lead) while learners hydrate. Once a healthy
  leader proves the exact non-joint expanded configuration, metadata publishes
  the final peer set and marks removed replicas `retiring`. That final set is
  latched in the retiring placement records; new peers are deferred and omitted
  final peers preserved until the phase completes.
- A retiring replica stays hosted and reports Raft status but is excluded from
  membership and routing. Its placement row is deleted after a healthy
  surviving leader proves the exact final voter set; a healthy source still
  reporting itself as voter blocks deletion, but an unavailable one cannot
  deadlock cleanup.
- Metadata absence is the durable routing and ownership fence: local
  reconciliation removes the persisted Raft catalog record before retiring the
  DB owner, including replication-factor-one moves.

Evidence:

- Placement intent is indexed by `(group, node)` and runtime evidence by
  `(group, store)`; hydration, cutover, and retirement accept evidence only from
  the exact named store. A zero store identity is usable only on a single-store
  node. Duplicate store identities or `(store, group)` reports fail closed. The
  evidence index is built once per pass, keeping reconciliation linear.
- Relocation hydration is proven by relocation generation, committed Raft apply
  boundary, logical document watermark, and stable voter identity. Source disk
  bytes are capacity telemetry, never a cutover predicate.

### Raft Progress, Control Lanes, And Metadata Projection

- The Raft progress lane owns consensus progress and durable metadata projection
  but never mutates the in-memory split/merge transition controller. The control
  lane is the controller's sole writer and rehydrates it from durable projected
  transition records each round, so a projection update is a durable wakeup
  that survives delay, leadership change, or restart.
- Controller access is serialized independently of the Raft owner lock;
  transition storage and network I/O never delay elections, heartbeats, or
  Ready processing.
- Each peer control RPC has a finite deadline and retries through the durable
  transition record. Health and metrics read a separately published fixed-size
  controller snapshot. Transition readiness reads a projected topology snapshot,
  never the rich admin snapshot (which may observe transitions and must not be
  called recursively by the controller).
- Replicated split observations read the durable data apply projection under its
  per-shard lock and never wait on the action lane. Split actions use fixed,
  group-sharded singleflight lanes that fail fast when busy.
- Metadata-Raft projection publication is commit-derived: the apply store stages
  projection signals, committed-key notifications, and split/merge deltas
  during the write transaction and publishes them only after the batch and
  applied watermark commit. Allocation failure or abort discards the staged
  outcome; a command rejected by identity or phase fencing is a durable no-op
  with no runtime delta.
- Production listeners and simulations consume the same committed outcome.
  Batch apply, snapshot install, and listener dispatch share one serialized
  publication lane, so consumers see changes in commit order. Transition
  records are not cloned unless a caller requests the delta list.

### Allocator Ownership

- APIs that return owned storage/index buffers accept the allocator that will
  free them, or carry it in a typed owner. Full-text stored-document
  decompression allocates in the request/result allocator, not the segment
  reader's.
- Production HTTP server construction selects the process request allocator
  internally; request-allocator injection is test-only.

## Write Contract

`DB.batch()` is document-first.

For each document write:

- parse once
- strip DB-owned special fields from the stored base document
- derive text/vector/graph directives from that parsed form
- commit the base document state
- hand derived work to index application or, next, the derived-work log

Special-field writes that do not replace the base document must preserve the
current stored document.

## Derived Artifact Contract

Derived artifacts are enrichment-owned, not index-owned. Public artifact
identity (`ArtifactRef`, `artifact_id`) is defined in
[ENRICHMENTS.md](ENRICHMENTS.md#artifact-identity-and-lookup); the document-key
codec is in [DOCID.md](DOCID.md#key-layout). Internal artifact keys (constructors
in `pkg/antfly-embedded/src/storage/internal_keys.zig`) never use textual
`<doc>:e:` suffixes:

```text
user_ns | enc(doc) | 00 00 | 0x20 | enc(type) | 00 00 | enc(name) | 00 00 | <record suffix>
```

- `0x20` is the artifact record kind; `type` is `chunk`, `asset`, `embedding`,
  `graph`, or `resolution`.
- chunk suffix: `0x30 | u32be(chunk_id)`; document units in assets add
  `0x35 | enc(unit_id)`; an embedding derived from another artifact appends
  `0x31 | enc(name)` to that artifact's key.
- Every artifact for a document stays inside that document's key range, so
  cleanup scans are bounded, and one chunking or asset pass can feed multiple
  indexes without duplicate sidecar storage.
- Model-produced text, OCR, transcripts, classifications, entity extraction, and
  other derived payloads use the `asset` type; media/schema is carried by
  `content_type`, artifact name, and enrichment metadata.
- Chunk artifacts are produced by the leased enrichment runtime; generator
  configs may declare `chunk_name`; overwrites and deletes clear stale chunks
  before regeneration.

## Derived Coverage Accounting

Coverage accounting is stronger than raw artifact or index-entry counts: a
managed embeddings index may legitimately cover a subset of documents (for
example a conditional media template), but a failed or lagging enrichment must
never read as "ready".

- Coverage measures whether every source unit of a derived generation reached a
  terminal outcome.
- Readiness is separate from health.
- A partial index is never used as a full-table access path unless the query or
  caller accepts that coverage (the partial-index precedent). Eligibility and
  terminal outcomes are explicit, never inferred from template syntax or
  artifact counts.

The status surface built on this model (`source_coverage`,
`searchable_vectors`, `publication`, `activity`, and `index wait` predicates) is
specified in
[ENRICHMENTS.md](ENRICHMENTS.md#embeddings-lifecycle-status). Dense artifact
counter bootstrap and counter/index mismatch handling are in
[DENSE_INDEXING_LIFECYCLE.md](DENSE_INDEXING_LIFECYCLE.md#artifact-counters-and-coverage).

### Coverage State

- Coverage is tracked per derived generation over logical source units, not
  physical entries. For embeddings the unit is one table document, including
  chunked embeddings (many vectors, one `produced` outcome). Artifact-to-artifact
  producers may use artifact identities only when their API exposes a matching
  source-total denominator.
- Coverage key: table or shard, derived artifact or index name, source artifact
  name (for artifact-consuming producers), source unit identity, and generation
  or config version.
- Each source unit has exactly one current outcome: `pending`, `in_flight`,
  `produced`, `skipped` (evaluated, intentionally nothing), `terminal_failed`
  (policy and retry budget exhausted), or `stale` (only an older generation
  result exists).
- Aggregate status fields: `source_total`, `eligible`, `produced`, `skipped`,
  `terminal_failed`, `retryable_failed`, `pending`, `in_flight`, `generation`,
  `complete`, `healthy`, `degraded`.

```text
strict complete      = produced >= source_total
partial complete     = produced + skipped >= source_total
best_effort complete = produced + skipped + terminal_failed >= source_total
healthy              = complete and terminal_failed == 0
degraded             = complete and terminal_failed > 0
```

- Counts are mutually exclusive per `(index, generation, source document)`. With
  a complete distributed observation, `pending = source_total -
  min(source_total, covered-by-policy)`; with any expected shard missing or
  unusable, public status reports `pending: null`, never a lower bound as exact.
- Runtime replay debt is an independent readiness gate.
- `source_total` is a durable range-local document cardinality maintained in the
  primary write transaction that maintains document identity; it is O(1) to read
  and independent of index entries. Because the identity namespace and its
  `live_ordinals` summary are shared across split descendants, split
  preparation and finalization rebase the range cardinality and each active
  generation's outcome counters from the documents and markers retained by the
  range. A missing range counter is recovered by one bounded range scan, then
  maintained by the identity visibility delta in the caller's batch.
- Distributed projections sum cardinality and outcomes only from fresh shard
  observations. `observation_complete` is false if any expected shard is
  missing, stale, remotely unknown, reports an incomplete counter summary, or
  reports a different stored-config fingerprint; incomplete observations never
  report complete or healthy and carry reasons such as `missing_group`,
  `stale_group`, `summary_unavailable`, and `config_mismatch`.
- The config fingerprint is a versioned canonical hash of semantic
  generated-output configuration (excluding object ordering, credentials,
  provider rate limits, and top-level batching), encoded as fixed-width hex and
  stable across shard-local marker generations. Idempotent or operational-only
  changes keep the catalog-owned coverage incarnation; generated-output changes
  assign a fresh one.

### Coverage Policy

- `strict`: every eligible source unit must produce; skipped or failed required
  units keep the index not ready.
- `partial`: intentional skips satisfy completion; terminal failures do not
  satisfy healthy readiness.
- `best_effort`: skips and terminal failures satisfy completion; terminal
  failures make status degraded.

```json
{
  "type": "embeddings",
  "coverage_policy": "partial",
  "template": "{{#if image_url}}{{remoteMedia url=image_url}}{{/if}}"
}
```

- Policy is declared on each consuming managed embeddings index. Enrichment
  definitions record producer outcomes only; a future artifact-level readiness
  API may add producer policy but never overrides index policy.
- Eligibility is an execution outcome today: a template that renders no input
  records a durable `skipped`. A declarative eligibility predicate must be
  implemented consistently by planning, execution, repair, and status before
  it becomes public; unknown `applies_when` configuration is rejected.

### Publication Policy And Readiness

`publication_policy` controls when a generation may answer queries;
`coverage_policy` controls which outcomes count as complete. A partial coverage
policy never authorizes querying an unsafe generation.

- `progressive` (default): a newly admitted canonical generation becomes
  queryable once it has at least one physical entry and a durable,
  generation-matching projection checkpoint beyond its admission fence, and
  stays `queryable_partial` until coverage and replay complete.
- `atomic`: a shadow generation is built, validated, and activated only after the
  complete-generation proof succeeds.
- Stored configs that predate `publication_policy` are read as `progressive`
  with no catalog rewrite, rebuild, or operator action. Externally supplied
  vector fields have no managed producer and never use the partial-admission
  exception.

| State | Queryable | Complete | Meaning |
| --- | --- | --- | --- |
| `pending` | no | no | No safely published generation exists. |
| `queryable_partial` | yes | no | One safe generation is published, but source coverage or replay is incomplete. |
| `ready` | yes | yes | The desired incarnation is fully covered, current, and published. |
| `failed` | no | no | Automatic progress requires corrective action. |

- `IndexReadinessStatus` carries `queryable`, `complete`, an opaque
  incarnation, and target and published revisions. Physical entry counts are
  reported separately; clients never infer completeness from them.
- `antfly index wait` predicates (`--until complete`,
  `searchable-artifacts=N`, `source-covered=N|N%`) are defined in
  [ENRICHMENTS.md](ENRICHMENTS.md#embeddings-lifecycle-status). Long waits
  belong in the waiter, not in a held search request; search uses the best
  safely published generation.
- Initial materialization is durable build work, not repair debt: it reports
  through backfill, coverage, publication, and activity, and query admission
  stays fail-closed until the first safe publication. Repair and
  action-required diagnostics are reserved for damaged generations and explicit
  rebuilds.
- Write synchronization is the derived-replay contract in
  [ENRICHMENTS.md](ENRICHMENTS.md#derived-replay-and-indexing-contract):
  `sync_level=full_index` waits for that write's derived-publication fences,
  which is distinct from waiting for an initial backfill.
- Partial visibility advances only at durable producer and index checkpoints,
  never at embedding-provider batches. Index-before-load workloads expose
  successive durable load revisions; a bootstrap scan of a pre-existing corpus
  currently emits one final producer revision.

Progressive publication invariants:

1. A query reads exactly one generation; models, dimensions, chunkers, and
   configuration incarnations are never mixed.
2. A mutable shadow candidate is never queried; progressive admission publishes
   only canonical-generation durable checkpoints.
3. Partial admission applies only to the managed-admission repair that created
   the incarnation, only under `progressive`, and only after a physical entry is
   visible.
4. Catalog configuration hash, coverage incarnation, projection checkpoint, and
   replay fence must agree; corrupt or untrusted state fails closed.
5. Replacement rebuilds serve the previous trustworthy generation until atomic
   activation; with none, the index stays unavailable until the progressive
   proof succeeds.

The durable repair intent owns completion, restart recovery, and fallback shadow
reconstruction after partial publication, and is retired only by the
complete-generation proof.

Physical proofs share generation, coverage, and replay fences:

- Dense: active HBC cardinality matches the durable artifact target (or
  produced-source count for non-chunked data) with no deferred posting repair.
- Sparse: derived batches apply transactionally before their checkpoint
  advances. Partial publication needs a clean matching checkpoint and one live
  sparse document; completion additionally needs complete source outcomes and
  live sparse documents covering produced sources.

For repeatable ANN comparisons on an unchanged corpus, also wait for
`status.hbc_posting.refresh_pending == false` on a fresh, complete status. This
optimization-convergence signal means a bounded clean sweep verified the
current mutation epoch; writes invalidate it and reopen starts pending. Idle
refresh can change neighbor lists without source changes. A status probe alone
may retire a cold owner, so activate the table with a query first. The signal
neither freezes the index nor changes query readiness.

Verification gates: the 500-document regression requires complete coverage,
idempotent replay, a complete second index, and a stable first index; a
deterministic partial-publication test proves `queryable_partial`, retrieval,
metadata, restart safety, and the transition to `ready`. Release benchmarks
record time to first queryable result separately from time to complete.

### Readiness And Repair

- Runtime status uses coverage accounting, never `doc_count >= table_doc_count`,
  for managed derived indexes: `rebuilding` and `backfill_active` stay true while
  units are pending or in flight; `replay_catch_up_required` is never cleared
  because one artifact exists; partial coverage never masks pending enrichment;
  terminal failures surface through degraded status and reason counters.
- Readiness never substitutes physical `doc_count` for the durable `produced`
  count, because chunked documents create many entries.
- Repair distinguishes never evaluated, intentionally skipped, produced but
  lost, stale generation, and terminal failure. Coverage-gap repair regenerates
  missing `produced` artifacts, leaves current `skipped` units alone, and
  retries or surfaces `terminal_failed` per policy.
- Per-source outcome markers are the source of truth: one marker key per source
  whose value is the outcome enum (one point read and one write per
  transition). Generation-scoped `produced`, `skipped`, and `terminal_failed`
  counters update in the same apply-lock domain and atomic batch as the marker.
  Status reads the three counters with O(1) point reads and never scans
  markers; a partial counter tuple reports degraded and incomplete; counter
  repair is a bounded maintenance scan outside the status path.
- Generated enrichment applies `produced` only after publishing its replay
  record. Non-retryable isolated request errors publish `terminal_failed`;
  shared embedding failures fan out to every consuming index; retryable
  failures stay pending.
- Direct field-backed vector indexes classify writes as `produced` or `skipped`
  at the derived-index apply boundary. Replay windows persist mixed outcomes
  under one apply lock and transaction, deduplicating source keys with hash
  sets so cleanup is linear in distinct sources.
- External embedding writes: only a durably applied `_embeddings` value is
  `produced`; a source without an external vector stays pending. That coverage
  is visible but does not make a usable external index report rebuilding;
  external query readiness depends on replay and published visibility.

### Managed Index Admission

Adding an index to a non-empty managed table is a generation transition using a
transactional outbox in the primary document store:

1. Under the DB apply lock, read the O(1) document-identity summary and capture
   live count, identity generation, configuration hash, and replay target.
2. Commit the catalog row and a fixed-size admission marker in one primary-store
   transaction; never scan or rebuild synchronously.
3. Materialize the marker idempotently into the generation repair checkpoint.
   Under `progressive`, the canonical worker publishes bounded durable
   checkpoints while the intent is pending; under `atomic`, the owner repair
   scheduler performs the bounded shadow rebuild and replay.
4. Keep the marker while repair is pending. Every DB open captures the marker
   prefix once before catalog open; writable opens suppress in-place backfill
   and recreate missing checkpoint state, and read-only/status opens evaluate
   the same fail-closed or progressive serviceability proof.
5. After the complete-generation proof (and, for `atomic`, clean shadow
   activation), delete the marker before removing the checkpoint intent, so a
   crash leaves redundant resumable repair rather than an admitted generation
   without debt.

Rules:

- Filesystem DBs publish the replica-local repair checkpoint beside the root
  with fsync and atomic rename; externally owned roots such as `.aflite` publish
  the same checksummed checkpoint through the backend's atomic namespace. The
  physical lock key identifies the replica.
- Reconciliation that observes pending repair enqueues the route with the owner
  repair scheduler, even if admission predates the runtime hook. Notifications
  are idempotent hints; the durable checkpoint is authority, and bounded
  fallback discovery only recovers missed notifications.
- Provisioned live creation returns after the catalog commit and a durable
  target-scoped activation enqueue, with status fenced as `activating` until the
  exact incarnation publishes. Activation cancellation leases are reference
  counted per group (details in
  [ENRICHMENTS.md](ENRICHMENTS.md#embeddings-lifecycle-status)). Embedded owners
  keep synchronous installation.
- The marker's identity generation is monotonic evidence; recovery fails closed
  if the summary regresses below it. Repair targets at least the marker's replay
  sequence, raised to the current derived sequence when the intent is created.
  Existing repair IDs are reused, so retries never duplicate rebuilds.
- Empty managed tables omit the marker but use the no-backfill add path; the
  durable zero-live-document summary proves completion without scanning
  (including tombstone-heavy shards). Unmanaged DB callers keep synchronous add.
- Index deletion commits catalog absence and marker deletion in one primary
  transaction while repair is quiesced. Runtime roots, coverage metadata, and
  checkpoints are post-commit cleanup; writable startup removes intents whose
  catalog entry is absent before workers start.
- Generated-artifact reclamation separates page arbitration from filesystem
  ownership: workers try the short page mutex and return `busy`; a completed
  tombstone takes a separate finalization lease, releases the page, and deletes
  checkpoints and directories while the tombstone fences same-name admission.
  Failures leave the tombstone for idempotent retry; manual runtimes leave
  contention to the next explicit poll.
- The in-memory identity summary is an optimization, not admission authority:
  admission reads the durable summary under the apply lock (an HA mirror failure
  may follow the primary commit). Only a query evaluating an in-memory repair
  gate rechecks markers with a bounded point read.
- Metadata cutover additionally requires a fresh target observation, complete
  document identity, and target full-text cardinality equal to the source live
  count. No status path infers readiness from catalog presence.

### Scope

Durable markers, counters, and public readiness are implemented for managed
dense and sparse embeddings indexes. The same outcome model is the required
contract for extending coverage to chunks, summaries, image/audio/PDF/OCR
extraction artifacts, graph extraction, artifact-backed vector indexes, and
algebraic or materialized derived indexes. Full-text indexes may project the
same shape with `eligible == source_total`. The shared model lives below the
public API.

## Search Contract

`DB.search()` is coordinator-style orchestration over named result sets.

Current result-set sources:

- `full_text`
- `full_text_queries`
- `dense`
- `dense_queries`
- `sparse`
- `sparse_queries`
- `graph_queries`
- fusion via `merge_config`

Graph queries may reference:

- `$full_text_results`
- `$full_text_results.<name>`
- `$aknn_results.<index>`
- `$graph_results.<name>`
- `$fused_results`

`expand_strategy` is a DB-level post-graph operation over the top-level hit set.

## Transaction / HLC Contract

Local transaction semantics follow the same visible-version rule used by the
shared compat corpus:

- transaction intents remain invisible until resolution
- `commitTransaction(txn_id, commit_timestamp)` makes the committed value visible
- the visible version/timestamp of the committed key becomes `commit_timestamp`,
  not the original intent timestamp
- transaction records preserve the original begin timestamp separately from the
  visible commit version so distributed resolution can propagate commit version
  without losing coordinator-start metadata
- participant-style resolution should use the propagated commit version directly;
  a node resolving remote intents should not need to infer visibility from local
  wall-clock time or from the original begin timestamp
- optimistic predicates compare against that visible committed timestamp
- `abortTransaction` removes intents without changing the prior committed value
  or its timestamp
- local recovery uses the richer transaction record for single-node cleanup:
  stale pending records auto-abort, finalized records repair any leftover
  intents using the stored status plus visible commit version, and old finalized
  records are cleaned once no local intents remain
- transaction records may also carry participant and resolved-participant
  metadata; cleanup must be deferred while any participant remains unresolved,
  even if the local coordinator intents are already repaired
- coordinator-side recovery may retry unresolved participants through a
  transport-agnostic resolver callback; only successful acknowledgements should
  mark participants resolved
- cleanup remains gated on both conditions:
  local intents repaired and all participants resolved

This keeps ordinary batch predicates and transactional predicates on one
versioning model.

## Derived Work Log And Projection Checkpoints

The async index/enrichment manager sits between document preprocessing and
derived index mutation:

1. parse and extract once
2. commit base docs
3. append one batch-shaped derived-work record
4. let per-index workers advance watermarks from that log

The derived log is sequence-based and idempotent. Rebuild-from-docstore is the
fallback, not the normal replay path. The log may be truncated at the global
minimum applied watermark once every managed index has passed a sequence. The
replay and watermark contract between producers and consumers is in
[ENRICHMENTS.md](ENRICHMENTS.md#derived-replay-and-indexing-contract).

### Sequence-Stamped Projection Checkpoints

Every derived projection (dense, sparse, full-text, graph, algebraic, and
enrichment scopes) publishes a durable checkpoint stating which base-data
sequence is queryable through it. The checkpoint is the restart contract.

- It atomically couples the visible projection artifact/root/manifest/generation,
  the highest applied derived-log sequence it covers, the projection status
  (clean, rebuilding, degraded, repair-required), and the schema/config/version
  identity needed to validate reuse.
- On open, a clean checkpoint is served immediately and only the derived-log
  tail after it is replayed. Startup never scans the primary store to prove a
  cleanly published projection queryable; full scans are only for explicit
  repair, incompatible metadata, interrupted rebuilds, or corruption markers.
- The applied sequence lives in the same atomic publish domain as projection
  effects. A separate applied-sequence row is insufficient unless the protocol
  proves the sequence never advances past durable query-visible data.
- The shared `ProjectionCheckpoint` format (applied sequence, status,
  generation, config identity, compatibility version) and helpers that read,
  validate, and atomically publish it with the owning artifact are implemented.

Per projection:

- Dense/HBC (shipped): HBC metadata format version 2 carries the checkpoint;
  saves update HBC metadata first, then the shared sidecar mirror (`AFPRJCP1`,
  current-only), and reads prefer the HBC checkpoint when its config identity
  is nonzero. Catch-up advances it only after durable artifact/root publication.
- Enrichments (shipped): each scope checkpoints only after every source change
  through the sequence has a durable artifact, skip, or failure/repair record,
  and reports clean, degraded, or repair-required. Config identity is a
  deterministic enrichment-catalog identity.
- Dense artifact counters (shipped): durable target/completed rows written in
  the same primary batch as artifact writes/deletes, classified by the shared
  embedding artifact identity decoder so direct and chunk embeddings share one
  O(1) startup coverage path; corpus recounts are repair-only.
- Full-text, sparse, graph, and algebraic: not yet on the checkpoint API; see
  [Open work](#open-work).

Runtime rules:

- Applied-sequence and status-only notifications publish status without
  invalidating cached query DBs; read-cache invalidation is reserved for
  visible-root/data publishes, blocking publish repair, and explicit table
  invalidation.
- Dense catch-up sizes replay-window coalescing and session-reuse waits to the
  current derived-log tail; the 256-record/2s coalesce window and 5s
  session-idle ceiling apply only to large hot-ingest backlogs.

## Runtime Ownership

DB background work uses a swappable execution capability instead of each
subsystem owning private threads. Native/server deployments attach a node-owned
runtime shared by DBs and stores on the node. Tests, embedded use, single-DB
usage, and WASM keep synchronous or manually pumped fallbacks.

The current model is generic runtime ownership plus typed subsystem adapters:

- the node owns capacity and lifecycle through `BackendRuntime`
- DBs borrow that runtime through `OpenOptions.backend_runtime`
- standalone DB opens may create an owned fallback runtime
- `BackendRuntime` owns the optional `std.Io.Threaded` provider, the
  owner-scoped `DurableJobLane`, and owner id allocation
- typed adapters own the meaning of their work, including derived replay, LSM
  flush/compaction, text merge, enrichment replay, TTL cleanup, and transaction
  recovery
- request-local fanout uses operation execution context and `std.Io.Group`,
  not the durable background-job lane

The important rule is that background execution is an optimization over the
inline path, not a separate durability or visibility model. Correctness cannot
depend on OS threads existing.

Runtime-backed work follows these boundaries:

- each backend owns its own flush/compaction state, queue limits, shutdown flag,
  and write-pressure policy
- the node runtime owns global worker limits so many DBs and stores do not
  multiply thread counts
- durable jobs carry an owner id so close/drain can target one DB/store/shard
  without disturbing other owners
- `BackendHandle.close()` drains or cancels submitted work before destroying the
  backend
- WASM progress is available through inline execution or explicit bounded
  maintenance/executor polling
- DBs own fallback runtimes only when no node runtime is provided

Derived replay, full-text merge, enrichment replay, TTL cleanup, transaction
recovery, and LSM background flush run under this model. `BackendRuntime` is
heap-owned at node/server construction and borrowed by DataServer, provisioned,
hosted, metadata, and standalone DB opens. `DurableJobLane` has inline and
threaded implementations; the LSM adapter blocks writers when bounded deferred
immutable queues are full. Raft replica placement reconciliation remains
foreground control-loop work.

### CDC Runtime And Cutover Ownership

Metadata-Raft owns CDC intent, lease, authority, and progress. It does not own
long-running provider execution. A metadata control round admits at most one
owner-scoped `BackendRuntime` maintenance job and returns; the job performs
snapshot and streaming work using the runtime's shared `std.Io` capability.
Service shutdown closes that runtime owner and drains the job before metadata
state is destroyed.

Every CDC job carries a `WorkPermit` tied to the replicated reconciliation
lease. Permit checks are cached for at most 250 ms to keep per-row checks cheap,
but provider calls receive a deadline shorter than the proven remaining lease.
The runner checks again before and after provider I/O, document application,
and durable status publication. It renews the lease continuously and stops
without publishing further work after leadership, lease ownership, or service
lifetime changes. Snapshot queries remain batch-bounded; exact exported
snapshots may keep one background job alive across batches because yielding
would destroy snapshot consistency, but they do not block the metadata control
loop. Reopenable snapshots persist a validated keyset cursor derived from the
document-key template, including lexicographic tuples for composite keys. Each
quantum resumes strictly after that cursor instead of using an increasingly
expensive and mutation-sensitive SQL offset. Sources without a derivable stable
key retain one consistent background snapshot rather than claiming a resumable
checkpoint they cannot honor. A provider page must advance the persisted keyset
tuple before any writes are applied, preventing a provider that ignores or
misapplies the cursor from replaying one page indefinitely.

PostgreSQL exact cutover uses two independent fences:

- a replicated authority claim compare-and-swaps the current source catalog
  and prior authority
- a stable provider advisory lock serializes physical resource lifecycle
  operations for the configured source

Physical slot and publication names include both cutover intent and authority,
so a replacement owner never mutates the predecessor's current resources by
name. The new authority durably records the superseded slot and publication,
removes the inactive slot before its publication, verifies absence, and only
then submits a dedicated retirement-completion Raft command. Ordinary status
publication must preserve the exact retirement tuple and cannot acknowledge
cleanup. The completion command compare-and-swaps both current authority and
retired resource identity, then clears only those fields while preserving newer
progress. Every provider mutation revalidates the current replicated authority.
A further authority handoff is rejected while retirement is pending, which
bounds durable cleanup state to one predecessor per source and makes retries
idempotent. Manual exact-cutover reseed first compare-and-replaces the source
catalog; it never deletes provider resources or clears source status on the
request thread. That publication revokes the prior runtime permit, and the next
runtime authority claim records and retires the predecessor's physical
resources durably. A crash between catalog publication and cleanup therefore
leaves recoverable intent rather than leaked, unowned provider state.

## Storage Backend Boundary

The DB layer should keep most of `antfly-zig` pure Zig and portable while
isolating unavoidable OS-specific storage behavior behind a narrow backend
boundary.

The current shape is:

- higher-level runtimes, indexing, query, and most tooling no longer depend on
  `std.c`
- durable storage uses the LSM backend, with an in-memory backend for local fixtures
- shared backend contracts and adapters exist
- concrete backends exist for in-memory KV and durable prefix/LSM
- backend conformance coverage exists
- top-level DB primary-backend selection exists
- snapshot export/restore and split semantics are backend-neutral, with
  backend-specific fast paths where useful

DB-level durable-LSM coverage exercises the backend seam across reopen,
full-text and derived replay, index deletion, indexed overwrites and deletes,
dense/sparse/graph queries, `_embeddings`/`_edges` special fields, split and
merge cutover, snapshot restore, TTL cleanup, named-query fusion and graph
expansion, and chunked dense/enrichment reopen.

### Backend Contract

Any DB storage backend needs to provide these semantics regardless of its
implementation strategy:

1. Transactions
   - read-only snapshot transactions
   - exclusive write transactions
   - nested child write transactions, or an explicit unsupported contract
   - commit and abort semantics
2. KV and range access
   - logical namespaces or partitions
   - point get, put, and delete
   - ordered iteration over key ranges
   - duplicate-key or multi-value support where callers rely on it
3. Durability
   - no-sync, data-sync-only, and fully durable commit policies
   - crash/reopen semantics documented at the backend boundary
4. Visibility and concurrency
   - when committed writes become visible to later readers
   - what long-lived reader snapshots see while writers commit
   - writer contention behavior
5. Maintenance operations
   - reopen
   - truncate or compaction equivalent
   - split/export/import hooks used by higher-level DB flows

The first backend-neutral code pieces are in:

- [pkg/antfly-embedded/src/storage/backend_types.zig](pkg/antfly-embedded/src/storage/backend_types.zig)
- [pkg/antfly-embedded/src/storage/backend_adapter.zig](pkg/antfly-embedded/src/storage/backend_adapter.zig)

Those model:

- durability expectations
- read visibility
- read/write transaction modes
- logical namespaces or partitions
- cursor open requests
- ordered range-scan requests
- write-batch capability hints
- cursor start/seek/iteration semantics

### Backend Contract Adoption

`docstore.zig`, `persistent.zig`, and `wal.zig` use the shared runtime store
contract. Antfly's durable backend is LSM; in-memory storage supports local
fixtures and constrained runtimes. Full-text segments can be published as
immutable files on host storage and are tracked by the LSM metadata catalog.

The standalone Zig LMDB port and its C differential oracle live in
[lib/lmdb/src/LMDB.md](lib/lmdb/src/LMDB.md). They are not Antfly storage
backends.

## Hot-Path Search Wire

The hot-path search wire is a narrow internal binary codec in the C API for the
hottest local search shapes, so embedded callers avoid JSON request/response
and per-hit allocation. The public HTTP API stays JSON, and richer search
shapes stay on the JSON C API entry points.

Entry points in
[pkg/antfly-embedded/src/capi/db.zig](pkg/antfly-embedded/src/capi/db.zig),
with the codec in `capi/search_wire.zig`:

| Entry point | Op | Shape |
| --- | --- | --- |
| `antfly_db_search_dense_wire` | `dense_search` (1) | dense kNN |
| `antfly_db_search_text_match_wire` | `text_match_search` (2) | full-text `match` |
| `antfly_db_search_text_term_wire` | `text_term_search` (3) | full-text `term` |
| `antfly_db_search_text_match_phrase_wire` | `text_match_phrase_search` (4) | full-text `match_phrase` |

Text shapes carry no stored fields, aggregations, graph, explicit sort, or
cursor. The Go consumer is `go/pkg/embedded` (`DenseSearchWire`,
`TextMatchWire`, and siblings over `include/antfly.h`).

### Search Wire Framing

All integers are little-endian. Every request and response starts with
`magic` (`0x41464442`, "AFDB", u32), `version` (1, u16), and `op` (u16);
requests then carry a u32 `flags` word before their fixed fields. Responses
use one packed shape for every op: `total_hits`, `hit_count`, and `ids_len`
(u32 each), then `hit_count` packed hits (`id_offset` u32, `id_len` u16,
reserved u16, `score` f32), then the concatenated ID blob, then an optional
8-byte `identity_read_generation` footer (see
[DOCID.md](DOCID.md#identity-generations-and-read-snapshots)). Responses carry
public IDs only, never document ordinals. Evolution is append-only: variable-width
data lives in trailing blobs referenced by offset/length. Batch/multi-search
on this framing is not implemented; see [Open work](#open-work).

## Shard Split Contract

Shard splitting avoids paying for full logical copy plus index rebuild on
every split:

1. child shard creation avoids logical KV replay where possible
2. index handoff avoids full reindex where possible
3. mixed-range cleanup happens only where strictly necessary

Current state:

- child docstore creation and parent cleanup use the runtime store's generic
  split path
- text indexes use segment handoff and mixed-segment rewrite instead of full
  child rebuild and per-doc parent text deletion
- the remaining split cost classes are non-text indexes, especially dense
  vector indexes and sparse postings — see [Open work](#open-work)

Principles:

1. Copy immutable state; do not replay documents unless forced.
2. Rewrite only mixed ranges.
3. Keep parent cleanup separate from child image construction.
4. Add metadata first so split planning is cheap and deterministic.
5. Prefer subtree, block, or segment handoff over whole-index rebuild.
6. If rebuild is required, rebuild only the mixed remainder, not the full child
   index.

### Raft-Ordered Split Control State

Online split lifecycle state belongs to the source data Raft group. Source
prepare, start, finalize, rollback, and destination acknowledgements are typed
internal batch mutations and use the same committed-entry index domain as
document writes. They must never be written directly to a side store with a
synthetic sequence, because that can reorder lifecycle state and user data after
replay or leadership changes.

The source `RaftApplyStore` durably owns:

- source split phase and split key
- the source-local split attempt epoch allocated in committed range metadata
- source delta sequence
- destination group acknowledgement and applied delta sequence
- one terminal split high-water mark per source group

Source acknowledgements are metadata-only Raft entries. They bypass the
document DB executor, are folded monotonically across the entire committed
apply batch, and are read only from `RaftApplyStore`. Destination checkpoint
markers remain in the destination DB because they gate destination bootstrap;
they are not a second source-progress authority.

Transition IDs remain opaque idempotency identities and are never ordered.
Each accepted split allocates a monotonically increasing `attempt_epoch` from
the source range record before runtime work is published. Every source command,
destination checkpoint, bootstrap marker, and acknowledgement carries both
values. This permits a new attempt after rollback even when deterministic
planning chooses the same transition ID.

Finalize and rollback atomically persist the terminal high-water mark before
removing active split state. Commands below that epoch are stale and absorbed;
commands at that epoch must match the terminal identity and outcome; a prepare
at a higher epoch may begin the successor attempt. Keeping one record per source
group bounds control-state growth without weakening replay fencing.

Raft group snapshots are versioned and include the range, primary documents,
active split state, delta sequence, pending deltas, source acknowledgement, and
terminal high-water mark. Install validates control-key ownership, duplicate
keys, framing, and encoded control values before atomically replacing document
and control state. Publishing the snapshot's applied Raft index remains a
separate state-machine step; partially installed split lifecycle state must
never become visible.

Snapshot compaction is asynchronous, fair, bounded-memory maintenance:

- each group has at most one coalesced candidate, and FIFO admission prevents a
  continuously written group from starving colder groups;
- build and publish failures retain the latest incarnation-fenced candidate and
  retry with capped exponential backoff without blocking Raft apply or transport;
- shutdown cooperatively cancels the active point-in-time source before joining
  its worker, while source cancellation and destruction remain thread-safe;
- data snapshots stream each MVCC cursor once into a temporary artifact with a
  fixed-size buffer. Current-only AFDS v3 uses terminated records, avoiding a
  separate document-count pass before encoding. Followers validate and install
  from borrowed payload slices in fixed-size batches, without a payload-sized
  copy or document-count-sized descriptor allocation;
- durable replica state publishes payloads by `(index, term)` before advancing
  checkpoint metadata, retains the preceding payload until that checkpoint is
  durable, and loads payload bytes only when snapshot transfer requests them.
  Each payload is an `AFRSPAY` v1 envelope whose SHA-256 binds its index, term,
  declared length, and bytes. Artifact publication enforces the declared length,
  and replica startup fails closed when the referenced payload is missing,
  truncated, has the wrong identity, or fails checksum validation;
- WAL/checkpoint state retains snapshot metadata but does not retain or encode a
  second full copy of the document image. Snapshot payload ownership is
  explicit; current readers accept only the exact current format, with no
  compatibility decoders or inline-payload fallback. WAL Ready deltas remain
  v3 because their record shape is unchanged.
- the transferable state snapshot index and Raft log compaction boundary are
  distinct durable identities. A snapshot captures the latest applied state,
  while the boundary retains the configured trailing log window and its term
  for incremental follower catch-up. Checkpoint v4 persists both identities;
  restart reconstructs the applied watermark from the state snapshot and the
  replication suffix from the compacted index and term.

The data and metadata point-in-time views share the same worker contract, but
metadata remains an in-memory payload because its bounded control-plane state is
small. One declarative metadata key registry drives both snapshot collection and
group-ownership validation. Its exhaustive projection enum and unique descriptor
table prevent a registered durable projection from updating one allowlist while
silently omitting the other. Operators receive only the actionable compaction
metrics: queued candidates, completions, and failures; payload volume, build
timing, and detailed scheduler outcomes remain internal diagnostics.

Transition observation reads only this already-open replicated control state:
the source phase, terminal fence, acknowledgement, and delta sequence,
atomically under one apply-store shard lock. It never opens, seeds, or scans the
live table DB, initializes indexes, or competes with the generation-owned
writer. Observation and reconciliation fail fast with a retryable unavailable
result while snapshot generation staging is active rather than consuming HTTP
workers behind generation I/O. Destination bootstrap and catch-up apply through the
destination Raft group, then acknowledge the resulting checkpoint through the
source Raft group. Retries are idempotent and every transition RPC attempt is
bounded so metadata reconciliation cannot starve metadata Raft heartbeats while
a destination group elects its first leader.

Publication requires the destination's complete configured voter set to report
healthy, a known stable leader, and an acknowledged checkpoint at least as new
as the source delta sequence. Physical handoff scans treat the encoded range as
an optimization and explicitly retain only primary documents owned by the
source group; derived records and unrelated primary records are not lifecycle
state.

### Split And Merge Transition Ownership

Split and merge control paths follow the same rule. Transition coordinators
borrow the raft host's apply store and retain the managed destination or
receiver DB lease for their full lifetime; observation never reopens either
path. Pending destinations are leased by globally unique group ID so metadata
publication and visible-root generation changes cannot hide an already-open
writer behind a stale table label. A process-local transition admission lock
serializes fallback coordinators while a destination is not yet published.
Every durable transition record owns an immutable table contract and two
explicit identity roles: `source_identity` for the source or merge donor, and
`target_identity` for the split destination or merge receiver. A split records
the same inherited identity in both roles; a merge records the donor and
receiver independently. Phase updates must preserve the complete contract, and
the binary and HTTP codecs require both roles. Runtime code selects a typed role
at each open instead of inferring one identity from current topology, which may
already have advanced past the transition.

Reconciliation-authority handoff rehydrates projected intents with an owned
copy of the durable contract and split boundary. It never reconstructs either
from the new authority's desired catalog, and it can resume an admitted
transition even when that desired catalog has already removed the table.

Writer-cache admission validates the expected namespace before returning a
lease. Normal source and target opens require an exact match. The sole relaxed
case is an explicitly admitted merge identity reassignment: while holding
exclusive transition admission, the receiver may be opened under its prior
namespace only when its persisted table ID matches the contract table ID. The
coordinator then performs the fenced reassignment before serving admission
reopens. Cross-table mismatches and all unflagged mismatches fail closed. An
idle mismatched owner is retired and closed before replacement; an active
mismatch blocks the new open until its existing lease drains. This prevents a
pre-publication destination open from becoming the serving writer for a
differently namespaced range and gives rollback enough durable information to
reopen the same-table receiver safely after restart.

Metadata reconciliation treats that contract as an active structural fence.
Schema and index changes wait until every contract for the table is terminal.
Range updates are fenced for each transition participant and its immediate
range-boundary neighbors, preventing half of a coupled boundary update from
publishing while allowing independent shard pairs in a large table to continue
reconciling. A requested table drop retains the table, every range, and their
placements until all transitions terminate; only then can normal removal
proceed.

Every destination bootstrap and catch-up batch carries a typed split replication
context containing the metadata transition ID, source group, destination group,
inherited identity namespace, operation kind, and source delta sequence where
applicable. The context is encoded in the internal HTTP command and the
data-Raft entry, validated against the catalog-visible source range on every
replica, and used for the destination's first physical DB open. Public batch
parsing rejects this internal context.

The destination stores the completed bootstrap identity and highest applied
delta sequence in the document store. A completed bootstrap fences late chunk
proposals; delta retries at or below the durable sequence are no-ops; and a
forward sequence gap is rejected. Empty source deltas are sent as sequence
barriers, so a gap can never be mistaken for an empty mutation. Document writes,
range publication, bootstrap identity, and sequence advancement share the same
DB apply lock and backend batch. The source acknowledges only after that commit,
and its durable acknowledgement includes the same transition ID. Range buffers
are prepared before commit and installed into the open DB allocation-free after
success, leaving no post-commit error path that can split durable and in-memory
ownership state.

Replicated source split lifecycle commands have one state-machine owner: the
durable data-Raft apply store. They are transition-only entries and are never
forwarded to the document DB executor. The apply store tracks its own durable
entry watermark and filters overlapping committed prefixes before producing
effects, so a Raft applied watermark that temporarily lags another state
machine cannot apply `prepare` twice while advancing to `start`.

Removing an active split or merge record is intentionally a no-op, not a
cancellation protocol. Cancellation first commits rollback intent on the same
transition identity, lets the transition owner restore source/receiver state,
then commits the `rolled_back` terminal phase. Only a terminal record may be
removed. Restart can therefore recover either an actionable rollback intent or
a terminal record, never an absent active transition whose storage side effects
remain live.

Distributed split bootstrap uses an exact destination reservation keyed by
transition identity, attempt epoch, byte range, and source delta sequence. A
replayed begin for that same reservation is idempotent and cannot erase chunks
written by another source-leader attempt. A newer sequence may replace an
incomplete reservation and fences older writes; completion publishes only the
exact reserved sequence. The source acknowledgement is proposed only after
destination completion, so metadata cutover cannot authorize an empty or
partially replaced destination generation.

A source group created before data-Raft projection has no apply watermark yet.
Its first split preparation seeds the source snapshot and a synthetic index-zero
watermark in one DocStore batch under the per-group apply lock. Index zero is
reserved for this baseline; real Raft indexes remain unchanged. A replica
replacement may instead inherit an authoritative document generation while its
new local Raft projection contains only a bootstrap batch. In that case a
non-null watermark does not prove that the projection includes the inherited
documents.

Split preparation reconciles an inherited generation only at an exact durable
apply watermark. It first waits until the document state machine has applied
through that watermark, acquires group-exclusive write admission, finishes
managed bulk state, and scans one point-in-time transaction from the
generation-owned writer. It does not hold the DB apply lock during the scan.
Existing projected values are compared through a cursor instead of being
materialized, and atomic replacement writes directly through one transaction
instead of constructing another shard-sized write list.

Successful reconciliation records the authoritative DB root's durable 128-bit
incarnation beside the projection. This identity is persisted in the staged DB
before generation sealing and atomic publication; it is stable across a process
restart but regenerated whenever a physical root is replaced. A process-local
visible-root generation is not sufficient because it restarts from zero. Later
split handoffs use an O(1) durable fast-path check: both the root incarnation and
the complete Raft watermark identity must match before the apply-store
projection can be captured without rescanning the DB. Missing, zero, malformed,
or mismatched incarnation markers fail closed. Snapshot installation naturally
drops the marker with the replaced group generation. Normal-entry history and
the watermark remain unchanged. If Raft advances during repair, publication
fails and preparation retries from a new snapshot; only replicated split deltas
may mutate an active projection.

The incarnation belongs to the core storage generation, not to a derived-index
repair checkpoint. It is stored in an independently checksummed
`root_identity.checkpoint`, written through the DB runtime's caller-owned
`std.Io`, synced, and atomically renamed. The identity belongs to the directory,
not the process-local visibility generation: cache fencing and in-place metadata
reconciliation preserve it. Every newly staged directory creates its identity
before sealing, so atomic publication carries the new identity with the root.
First creation is serialized by a blocking OS file lock and rechecks the
checkpoint after acquiring that lock. Concurrent first opens therefore observe
one durable incarnation rather than retaining different random candidates.
Derived checkpoint loss or repair cannot change storage identity. A physical
generation replacement creates a new identity even when the logical contents
and index catalog are unchanged.

This checkpoint applies only to `filesystem_managed` roots whose path is the
published storage directory. Embedded and Lite DBs use `external_backend`: the
path is a logical namespace while the enclosing single-file engine owns
locking, durability, and publication. Those handles do not synthesize a
directory incarnation and fail closed if a distributed projection asks for
one.

Managed data-Raft status collection follows the same ownership boundary: it
uses writer-published runtime snapshots when available and conservative
Raft/transition facts on a cold cache. It never opens an independent status DB
over a Raft-owned root. Transition readiness comes from observations published
by the transition owner into metadata, then from durable phase/snapshot facts;
status paths never instantiate split or merge coordinators to inspect live
roots. Non-Raft deployments may retain the bounded status-only open because no
competing managed writer exists.

Split execution dispatch follows ownership as well. A data-Raft server uses the
replicated destination route. A server with an injected local transition runtime
delegates to that runtime, and the non-Raft fallback creates a short-lived local
coordinator under transition admission. Only the replicated path requires a
destination URI; local execution must not fail merely because no remote route
exists.

### Text Segment Handoff

Text index split classifies active segments using persisted key-range
metadata (`min_doc_key`, `max_doc_key`) instead of rescanning the source
docstore:

- `right-only` segments are copied unchanged to the child active manifest
- `left-only` segments stay in the parent unchanged
- `mixed` segments are rewritten into left and right replacement segments
- the original mixed segment is retired from both manifests

Mixed-segment rewrite is driven from the segment blob itself using
`SegmentReader.storedDocDecompressed(...)` and
`buildTextSegmentFromDocuments(...)`, without rescanning the main docstore.
Temporary filtered mixed segments are a useful optimization layer: install a
mixed segment with a shard-side filter bitmap, finish the split cheaply, then
let later compaction produce clean segment ownership.

Child text indexes are built mostly by manifest/segment handoff, only mixed
segments are rebuilt, and a split can defer clean mixed-segment rewrite when
correctness is preserved.

### Graph Split

Graph split uses edge ownership and direct reverse-index rebuild instead of
generic doc replay: child graph split rebuilds reverse state directly from
owned outgoing edge keys, no longer depends on generic doc replay for
destination rebuild, and graph boundary coverage checks that reverse rebuild
respects split ownership bounds.

Dense vector split and sparse index split are not yet at the same point; see
[Open work](#open-work).

## Open work

This section consolidates genuinely unshipped or unconfirmed work from across
this document.

### Search wire batch/multi-search support

Batch/multi-search support on top of the existing search-wire framing (see
[Search Wire Framing](#search-wire-framing)) is not implemented.

### Dense vector split

Dense vector indexes are intended to reuse existing HBC structure where
possible instead of rebuilding the child from scratch (see [HBC.md](HBC.md)
for HBC-specific write routing, search/rerank boundaries, vector ownership,
and bulk-build strategy; this section owns DB-level split sequencing and
handoff requirements).

Relevant storage exists in `hbc_nodes`, `hbc_quant`, `hbc_vecs`, and
`hbc_meta`. Node/subtree split-range metadata is implemented in
`hbc_adapter.zig`, and `splitPlanningStats(...)`, `buildSplitReusePlan(...)`,
`estimateSplitRebuildWork(...)`, and split-member collection are wired into
[bench/vectors/hbc_bench.zig](bench/vectors/hbc_bench.zig). DB split
destination rebuilds child dense indexes directly from HBC split-member plans
and skips generic dense doc replay for handed-off child docs.

However, synthetic HBC workloads (`kmeans`, `hilbert`) produce almost entirely
mixed subtrees, so subtree reuse saves little over a full child rebuild; batched
child rebuild is the effective lever today, and doc-key-local leaf split
heuristics are secondary tuning. The target of child dense split mostly reusing
existing HBC subtrees, with full child vector reinsertion no longer the default
split path, is not yet met.

### Sparse index split

Sparse indexes are intended to progress from forward-entry handoff to
block/postings handoff, treating sparse index data more like text than dense
HBC: add shardable postings/block metadata, hand off fully right-only postings
blocks, and rewrite only mixed blocks.

Current status: direct split handoff copies child-side `fwd`/`rev` coverage
unchanged but rebuilds postings from source chunks while preserving
doc-number mappings — this is not yet true block/postings handoff. Sparse
posting chunks and sparse terms do persist per-chunk and term-level key-range
metadata, fully right-only sparse chunks are copied raw into the child index,
only mixed chunks fall back to filtered rebuild, and split-time generic
indexing can skip sparse docs already handed off (`zig build sparse-test`
covers Zig sparse unit behavior). The remaining gap is replacing the
from-source-chunks postings rebuild with real persisted postings/block
routing metadata.

### Remaining DB roadmap items

- keep DB behavior covered across in-memory and durable runtime storage
- use `hbc_bench` split planning output on more realistic dense datasets
- prototype dense subtree handoff for clearly right-only cases while assuming
  mixed rebuild remains important
- replace sparse live chunk planning with persisted postings/block routing
  metadata across larger sparse datasets
- add a smaller durable split prepare/equivalence target instead of relying on
  the heavyweight DB split bench for debugging
- keep graph split on the direct ownership path and broaden correctness checks
- move full-text, sparse, graph, and algebraic managed indexes onto the
  shared `ProjectionCheckpoint` API (see [Derived Work Log And Projection
  Checkpoints](#derived-work-log-and-projection-checkpoints) above)
- extend incremental artifact target/completed counters beyond dense to other
  index types
- validate the projection-checkpoint acceptance bar: clean restart with no
  new writes opens queryable projections without a primary document-store
  scan; startup work is proportional to the derived-log tail, not corpus
  size; crash tests around checkpoint publication never expose a sequence
  ahead of durable projection effects; repair markers, incompatible metadata,
  and corrupted checkpoints still force rebuild or degraded status;
  enrichment failures do not advance a clean checkpoint past missing durable
  outcomes; metrics expose checkpoint applied sequence, status, replay tail
  size, and repair-scan counts per projection
- avoid whole-range logical prune during parent finalize where runtime storage
  metadata can answer the same question, and reclaim retired segments cleanly


### Conditional source value certificates

Source-only query delivery may use the optional `SVI1` canonical-value sidecar
in `storage/source_value_identity.zig`. It is installed in the same transaction
as primary JSON bytes, the existing DPD2 projection directory, content proof and
mutation revision. It does not change DPD2 coverage or admission limits; rows
below the existing 16 KiB threshold do not receive certificates.

A certificate binds primary byte length/SHA-256, source kind, exact directory
SHA-256 and semantic canonicalizer ABI. Up to 512 sorted directory-index/digest
pairs certify disjoint scalar leaves, with a total bound of 17,519 bytes.
Canonical identities hash the typed JSON value with the versioned source-value
domain; directory hashes are binding checks, never interchangeable identities.
Deletion removes the sidecar. Stale optional metadata cannot authorize omission.
The ABI is explicitly versioned independently of source-file/compiler identity;
fixed typed-JSON and ABI golden vectors gate upgrades. Lookup/comment/test edits
cannot invalidate a table's certificates. Unknown semantic versions still reject.
The ordinary atomic write and repair paths collect identities during the final
bounded directory walk. Unescaped valid UTF-8 scalar strings, booleans and null
hash their borrowed canonical bytes directly. Other scalars retain the typed
reference canonicalizer; raw directory digests remain separate binding hashes.

Window preparation uses two sorted/deduplicated metadata batches for up to 64
rows: proofs/revisions/TTL, then directories/certificates. Decoded directories and
owned certificate records remain local to the pinned transaction and are reused
for the remaining projected field reads. Retained metadata is bounded by 64
format-limited authorities; row reconstruction scratch remains independently
recyclable. Parent masks that cannot use these facts bypass preparation entirely.

`conditional_source.zig` checks these bindings under the ordinary publication
lease and read scope. Each bounded window uses an ordinary read transaction;
current-tip point probes do not support value scopes. Only eligible source-only queries forward omission to
storage owners. Source-dependent operators retain complete values. Unsupported
layouts and missing/stale certificates retain reference projection and hashing;
existing bounded authority backfill installs certificates without advancing the
logical revision. ABI changes invalidate old certificates and trigger safe
fallback/repair. This is an internal storage optimization of the existing public
`source_projection` contract, not another caller-managed proof field.

Qualify native delivery with `e2e/antfly/bench_source_projection.py` using pinned
baseline/candidate servers. Its certificate-hit counters establish proof reuse,
not physical disk-read counts. Remaining metadata, frame layout and ordinary
source fallback still determine physical I/O.
