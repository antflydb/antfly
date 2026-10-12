# Dense Indexing Lifecycle: Implementation Log

> Relocated verbatim from zig/DENSE_INDEXING_LIFECYCLE.md (lines 3–187, 1767–1803, 1819–1948, 2030–2056, 2090–2106, 2133–2148 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [DENSE_INDEXING_LIFECYCLE.md](../../../../zig/DENSE_INDEXING_LIFECYCLE.md) before the move.
>
> Phase 3 (performance qualification) and Phase 5 (repair scheduling and production UX) remain in the design doc because their qualification gates are still open. The lines below are unchanged apart from the section headings added to separate the excerpts.

## Excerpt: Former Status section (implementation-status bullets)

## Status

Core implementation complete; the single-run one-million-document qualification
passes. Repeated baseline/candidate performance runs and the full deterministic
fault matrix remain rollout gates.

Implemented in the current tree:

- ordinary dense replay uses reopenable streaming sessions rather than active
  bulk-publication state
- `IncompleteBulkPublish` is quarantined and repaired through a durable,
  restart-resumable shadow-generation state machine
- an artifact projection with missing counter metadata bootstraps its authoritative target
  count from a stable snapshot plus a signed concurrent-write delta; only the
  two short marker/finalization boundaries hold the apply lock, and restart
  safely repeats the scan under the existing durable repair intent. A random
  per-attempt nonce prevents a stale concurrent scanner from finalizing another
  attempt's delta. Counter routing is derived from the complete index catalog,
  including quarantined status-only configs, so writes remain accounted while
  the affected runtime index is absent. Document-only commits bypass catalog
  construction entirely
- document deletion, TTL cleanup, and remote document-child artifact batches
  commit artifact writes/deletes, the derived journal record, and dense repair
  target-counter mutations in one primary-store transaction. Duplicate keys and
  delete-then-rewrite batches are normalized to their final state before the
  counter delta is calculated. The old helper that deleted artifacts in a
  separate transaction has been removed
- dense and sparse replay distinguish a genuinely missing source artifact from
  an artifact removed by a later document deletion. A stale upsert whose source
  document is already absent advances without creating repair debt; a missing
  or corrupt artifact for a live source document still fails closed
- explicit generation rebuilds for dense, sparse, graph, and full-text indexes
  use the same durable intent and post-pointer restart reconciliation
- startup repair is ownership-fenced, resource-admitted, and driven by a
  debt-only fair queue with O(1) enqueue/removal and a 32-entry bounded
  round-robin inspection window; lost notifications are recovered by an
  independent adaptive 16-to-256-group cursor window rather than a periodic
  full-node sweep, with a measured full-rotation estimate and SLO violation
  metric when the configured group count exceeds the qualified envelope
- pending repair outcomes are re-enqueued before a lost-wakeup cursor may
  advance. Allocation failure is logged, counted, scheduler-backed off, and
  leaves the cursor on the same route instead of silently degrading exact debt
  notification into a full fallback rotation
- progressive, queryable generations wait on source progress with a bounded
  audit deadline. Each selected audit grant carries the durable intent revision
  and is consumed by an expected-revision checkpoint transition under the
  repair control mutex. Owner handoff, pause/resume, replay, and retry mutations
  invalidate an older grant; successful consumption advances the revision and
  clears the resident token. This durable write occurs only on the audit path,
  while ordinary deferred scheduling remains process-local. An audit still
  respects the producer-completion fence before starting reconstruction
- large dense candidate scans and pre-activation replay catch-up are
  cooperatively time-sliced by the `BackendRuntime` owner. Scan slices use
  streaming publication and checkpoint the source-store cursor; catch-up
  checkpoints the applied sequence after each bounded replay window. Both leave
  the candidate reopenable before releasing the one-per-node repair slot.
  Restart or ownership transfer resumes from the durable cursor/checkpoint; a
  crash before publication safely repeats the last idempotent unit
- retry backoff uses a separate durable consecutive-failure streak; successful
  slices reset it, so a large repair's lifetime attempt count cannot turn its
  first later transient failure into the maximum ten-minute delay
- repair execution is owned by a dedicated `BackendRuntime` maintenance owner;
  shutdown cooperatively cancels at durable boundaries and drains that owner,
  leaving the candidate resumable instead of waiting for a full rebuild
- dense repair snapshot batches are sized from vector dimensions and the
  currently available `ResourceManager` repair budget. Background planning
  stays within the soft budget and retains the hard budget as an authoritative
  race/safety fence. The counter-bootstrap scan is admitted from the
  same repair slice before it publishes a marker or opens its stable snapshot
- temporarily unknown leadership retains queued debt with retry semantics;
  only an authoritative non-local ownership decision removes local queue state
- provisioned operator rebuild requests only persist/attach to the durable
  intent while holding the group lifecycle guard, then enqueue the exact group;
  the same `BackendRuntime` maintenance owner performs automatic and operator
  reconstruction. Pause/resume/cancel updates only that affected group queue,
  and ordinary writes do not wake the repair scanner
- a validated replacement is activated atomically while any prior healthy
  generation remains available
- artifact-backed dense coverage uses the durable source counter as the
  authoritative cardinality invariant at snapshot, replay advancement, and final
  fenced activation boundaries. An absent counter is explicit bootstrap debt;
  a separately committed status snapshot is never substituted as a correctness
  target. Ordinary counter/index differences are ignored while derived replay
  is behind its source target. After replay convergence, a deficit may be filled
  in place. A surplus, missing authoritative counter, configuration mismatch,
  `repair_required`, or allowlisted backend structural-validation failure
  schedules a shadow-generation rebuild and fails the affected index closed.
  Only an explicit operator rebuild of a proven healthy generation continues
  serving during construction.
  Shadow activation rejects either deficit or surplus. Cardinality detects missing/additional
  entries but is not a cryptographic proof of key-set identity. Key-set
  correctness derives from constructing an empty shadow exclusively from the
  stable source snapshot plus fenced replay; adding a per-write cryptographic
  digest is not justified unless fault injection demonstrates an equal-count
  substitution that bypasses the index backend's own integrity checks
- activation durably records and retains the previous root until the new clean
  checkpoint is published; a missing or corrupt pointer-selected generation
  fails closed and rolls back to that retained predecessor when one is valid
- root-generation rollover atomically replaces the replica repair identity and
  all pointer-selected replacement debts in one checkpoint; affected indexes
  stay fail-closed and rebuild under the new identity instead of losing an old
  activation intent during promotion or physical-root replacement
- activation adopts the already-open shadow runtime for every index family
  after the durable pointer write instead of closing and reopening a large
  index under the write fence; dense query-drain, apply-lock acquisition, final
  replay, and every reversible pre-commit step share one absolute activation
  deadline
- public repair status and errors are intentionally compact; detailed state is
  confined to operator metrics and logs; malformed durable repair state maps
  to the same compact `failed` status instead of disappearing from the response
- malformed API repair-job records are atomically removed from active/primary
  scheduler namespaces into a bounded forensic quarantine. They cannot prevent
  the primary API server or unrelated valid repair jobs from starting; the
  replica-local generation intent remains the correctness source of truth
- cache policy is node-owned by `ResourceManager` and is absent from index
  configuration
- managed index create/recreate keeps enrichment quiesced until its synchronous
  generation build and replay plan close, preventing generated writes from
  mixing streaming-replay and bulk-publication sessions on the same HBC index;
  primary writes and unrelated query serving remain available
- disk growth is estimated from the selected index generation and claimed by
  `ResourceManager` in a storage-backend capacity domain; provisioned storage
  supplies a live platform filesystem probe, and claims are rechecked and may
  grow at bounded shadow publication/catch-up boundaries, including every HBC
  deferred-publication window. Before a conservative claim can deny progress,
  materialized candidate bytes are reconciled out of the future-growth claim,
  so free space and the shared reservation never charge the same bytes twice
- filesystem-observation freshness is monotonic end to end. Durable retry
  deadlines remain realtime so they survive restart, and are translated once
  into monotonic deadlines when inserted into a process-local scheduler; the
  two clock domains are never compared directly
- a forced named-index repair job requests one replacement generation across
  its bounded group traversal and then becomes observational; cancellation is
  terminal only after a bounded durable pause traversal has reached every
  affected group and signalled any active owner to yield
- forced generation completion is persisted against the stable API job
  identity before the repair intent is removed. A crash after pointer/checkpoint
  publication but before API acknowledgement therefore observes the completion
  instead of constructing a second generation
- generation completion and source-artifact debt are separate durable
  outcomes. Once a replacement pointer and clean checkpoint commit, the
  generation intent is complete even if corrupt or missing source artifacts
  remain in the bounded artifact-repair queue; those issues stay visible
  without triggering redundant full-generation rebuilds
- transient repair-job dispatch, ownership, routing, and storage failures return
  the durable job to bounded exponential backoff. Named-index cancellation
  remains queued and is resumed by the API maintenance supervisor; malformed or
  semantically invalid requests alone become terminal
- retired-generation collection is deduplicated `BackendRuntime` cleanup work.
  A rerun latch covers retirements that race an active scan, and DB shutdown
  drains the cleanup owner before destroying the index manager
- embedded/Lite databases that intentionally omit the managed durable-repair
  checkpoint treat repair-intent discovery as empty, so normal index deletion
  remains available; attempts to create managed durable repair state still
  fail closed
- replay-pin creation/removal and the in-memory pressure gate share the replay
  truncation exclusion, including concurrent repairs of different indexes
- ordinary `.write` traffic participates in derived-backlog admission: the
  node resource manager uses 200/100 pending-sequence hysteresis and also
  forces bounded progress when aggregate LSM state reaches its configured
  write-throttle pressure; if per-sequence accounting allocation fails, an
  allocation-free aggregate keeps exact byte pressure and fails closed through
  the newest affected sequence
- managed-Raft writes apply repair admission on the confirmed leader before
  proposal. Followers forward normally and committed apply always bypasses the
  gate, preserving replica determinism. The normal path performs one combined
  `ResourceManager` pressure check per distinct manager and only scans the
  exact group/table writer cache after hard replay pressure is present
- control-plane group status never cold-opens a root participating in an active
  split or merge. It reports durable transition readiness plus the most recent
  merged/store snapshot and live Raft routing; an ownership race detected after
  the cache probe falls back to the same non-opening path instead of aborting
  the complete node status refresh
- split-key discovery borrows the existing managed writer and reads primary
  keys without draining enrichment or derived-index maintenance. It finalizes
  only a pending primary-store auto-bulk boundary needed for primary visibility;
  full replay/index draining remains reserved for a DB that is actually closing
- mutable LSM snapshots have generation-specific reader references, so an old
  replay scan cannot retain unrelated later snapshots; write transactions keep
  backend-close fencing without pinning versions unless they open a cursor
- streaming-ingest qualifications at 1,000,000-, 100,000-, and 50,000-document
  scale, including a post-production-hardening provisioned-path external-vector
  guardrail, pass exact count/checkpoint, bounded memory, durable reopen, and
  post-restart search, confirming that normal writes do not enter bulk
  publication or repair

## Excerpt: Physical-versioning fix narratives

A follow-up fresh-backfill failure exposed that the posting and exact-vector
halves still established authority in the wrong order. Posting backfill could
see a direct document vector while the later vector-block snapshot searched only
for an index-managed embedding artifact which backfill had never materialized.
That both failed readiness and attempted a second primary scan. Fresh v2
construction now establishes the shared exact-vector base before capture:

- the first managed index publishes a real empty generation at source sequence
  zero with no physical shard files or primary scan;
- an existing exact table-wide generation adds a new zero-count artifact scope
  through a checksummed `CURRENT`-only transaction, preserving immutable blocks
  and the committed WAL prefix;
- synchronous backfill materializes the same index-managed source artifact as
  foreground direct-field writes, appends its exact vector to the native WAL,
  and builds HBC postings from that one pinned source transaction; and
- stable-tip publication compacts the captured native delta instead of
  rescanning primary artifacts. The wider snapshot sequence is accepted only
  while the generation is unpublished; promotion permanently closes that
  construction capability.

The regressions require zero primary vector snapshot builds for both managed
empty admission and standalone backfill, verify exact scoped coverage after a
second index joins a shared generation, reopen the metadata-only scope update
without changing WAL generation/bytes, and reject reuse of the construction
sequence override after v2 publication.

The managed corruption/recreate E2E then exposed a separate publication race:
repair-shadow orphan collection derived liveness only from one manager's
in-memory catalog. A catalog-lagging cleanup worker could therefore delete a
new native generation after its construction marker was cleared even though a
durable canonical `ACTIVE_ROOT` pointer already selected it. Cleanup now scans
all canonical pointers before orphan collection and treats their targets as
live without using payload health as deletion authority. The construction
marker protects a unique root before pointer publication; the durable pointer
protects it afterward. The exact public API corruption/delete/recreate test and
a catalog-lagging cleanup regression both pass with this rule.

## Excerpt: Native generation lifecycle hardening, r128 qualification, and completed implementation phases 0–2

### Native generation lifecycle hardening

The post-r126 PR review found three lifecycle gaps and the implementation now
uses the durable shape rather than benchmark-only workarounds:

- Native backup manifest v5 authenticates both the portable snapshot path and
  an explicit runtime `install_path`. Shared vector acceleration remains under
  the snapshot ownership namespace `indexes/vector-blocks`, but installs at
  the runtime-owned `vector-blocks` root. Duplicate or noncanonical install
  targets are rejected before any generated state is admitted.
- Snapshot admission acquires stable file-descriptor leases for exact committed
  posting/vector WAL prefixes. WAL copying, hashing, and fsync now happen after
  apply, replay, and structural mutation admission reopen. A deterministic test
  unlinks and replaces the live WAL before materialization and still recovers
  the selected committed prefix.
- Posting and vector generation directories reconcile strict native filenames
  against `CURRENT` at startup and publication boundaries. Known retirees are
  still deleted directly for storage-provider compatibility; inventory sweeps
  recover crash-before-publication orphans and retry failed unlinks. Cleanup
  reports `observed_debt`, `removed`, and `remaining_debt`, preserves unrelated
  files, and ordinary observational opens never reclaim concurrently staged
  generations.

These changes are outside the query and mutation hot paths. Snapshot fence work
is reduced from O(committed WAL bytes) to descriptor acquisition plus immutable
hardlink metadata. Publication adds one flat, filename-only inventory scan; it
does not read segment contents or recurse through the database tree.

The subsequent upgrade/restore review closed the remaining physical-generation
ownership gaps:

- Native authority can no longer appear as a side effect of an ordinary v1
  mutation. HBC requires an explicit authority-transition capability, and the
  catalog grants it only to an inactive candidate or an already-selected v2
  generation. Standalone storage skips distributed capability negotiation but
  still uses the same manifest plus incompatible pointer publication as a
  provisioned table.
- An authenticated native restore is rehomed into a deterministic v2 generation
  with an atomic directory rename, checksummed ready manifest, directory fsyncs,
  and pointer publication last. Retry validates or completes the same generation;
  it neither copies vector/index files nor replays the corpus.
- Compatibility LSM files inside the active v2 generation are restart-stable
  cleanup debt. The existing durable cleanup lane removes them only after native
  authority and the catalog capability floor are both proven (or, for standalone
  storage, after the v2 pointer has made downgrade fail closed).

The public serving and mutation loops are unchanged. Authority gating adds no
steady-state branch after the persisted-authority fast return; restore work is
O(index count) metadata plus directory renames; legacy retirement runs in the
background cleanup lane. A full DB lifecycle test now proves v1 remains
queryable during shadow construction, v2 promotion precedes retirement, native
backup/restore needs no embedder, and the restored read-only index has neither
format-migration nor repair debt.

A fresh post-merge r128 50K public-API lifecycle qualified correctness under
heavy host contention: recall was 0.9876 live, cold-reopened, and warm-reopened;
the published generation covered all 50,000 vectors; and no capture, generation,
recovery, or cleanup error was emitted. Restart RSS peaked at 350.8 MB and the
restart physical-footprint ledger at 124.2 MB. The host simultaneously ran two
unrelated CPU-saturating Zig test jobs, inflating insert to 207.68 seconds and
profiled server query time to 24.60 ms, so r128 is deliberately not timing
evidence. The uncontended r126 30.09-second lifecycle and 3.23 ms mean server
time remain the applicable performance baseline.

## Implementation Plan

The numbered phases describe dependencies, not a mandatory release order.
There are three independently reviewable workstreams:

- automatic restart repair may land before streaming replay, but its durable
  repair core and node-local scheduling/UX should be separate focused PRs
- streaming replay prevents new normal-write incidents and requires crash and
  performance qualification
- generation publication is a separate project for explicit rebuild/import
  continuity

### Phase 0: Prove and classify the current invariants

1. Inventory every HBC branch controlled by `bulk_ingest_session_depth`.
2. Classify each branch as LSM batching, cross-batch publication, metadata
   publication, search visibility, cache behavior, workspace lifetime, or
   maintenance behavior.
3. Add deterministic reopen tests at individual HBC batch boundaries.
4. Complete the insert, overwrite, move, delete, and coverage idempotency audit.

Acceptance:

- every raw depth decision has an explicit owner and recovery rationale
- a normal non-session HBC batch is proven structurally reopenable
- any failed idempotency case is fixed or has an approved durable batch-ID
  design before streaming rollout

### Phase 1: Establish the streaming contract

1. Add `WriteSessionKind` and capability predicates to HBC.
2. Separate LSM session batching from cross-batch HBC deferred state.
3. Make metadata, topology, Flat-RaBitQ node keys, and required quantized state
   publish atomically at streaming batch finish.
4. Add streaming replay begin/finish/abort APIs.
5. Add corresponding `IndexManager` APIs.
6. Switch asynchronous dense catch-up to streaming replay sessions.
7. Keep batch-level bulk optimizations enabled initially.
8. Add per-session and node-wide resource bounds.
9. Make the backend report whether finish established a durable checkpoint;
   preserve durable-finish-before-applied-sequence ordering in every mode.

Acceptance:

- normal dense catch-up never writes `__bulk_publish_state`
- every committed streaming batch is reopenable
- abort leaves a query-consistent, replayable state
- a killed process resumes through replay without rebuilding the index
- relaxed/`none` durability never advances a durable watermark until explicit
  sync succeeds
- existing dense visibility and sync-level tests continue to pass

### Phase 2: Crash and idempotency qualification

Add deterministic hooks and tests for every interruption point in the recovery
table. Exercise inserts, overwrites, deletes, mixed operations, external vectors,
managed vectors, full durability, and relaxed/`none` durability.

Acceptance:

- no duplicates or lost deletes after restart
- durable document and dense counts converge
- applied sequence never skips unapplied work
- nearest-neighbor results and recall match a clean ingest
- no streaming interruption yields `IncompleteBulkPublish`

## Excerpt: Completed Phase 4

### Phase 4: Durable automatic repair core

1. Add load-failure recovery classification.
2. Add rebuildability, artifact, disk, configuration, and ownership preflight.
3. Add the durable repair intent, candidate identity, and replay-retention pin.
   Bind all local artifacts to DB, group, replica, and root-generation identity
   and acquire snapshots through the atomic pinned-snapshot primitive.
4. Refactor shadow construction so it builds directly from a status-only
   quarantined configuration without deleting or reopening the poisoned root.
5. Add the complete restart transition table, ready, bounded convergence,
   activation, validation, cleanup, and stale-candidate recovery.
6. Make operator repair use the same state machine.
7. Add crash tests for every durable transition.

Acceptance:

- an existing `IncompleteBulkPublish` index can be repaired through the shared
  durable engine without wipe/re-ingest
- queries never observe an empty or incomplete replacement
- concurrent writes after the build floor are retained and included
- the quarantined root remains untouched until a validated replacement is
  active
- unknown failures remain fail-closed
- malformed intents, missing artifacts, and insufficient disk fail safely
- replica replacement, move/split, promotion, and backup/restore cannot resume
  a candidate belonging to a different local root

## Excerpt: Completed Phase 6

### Phase 6: Shadow generation publication

1. Refactor repair shadow construction into reusable replacement-generation
   APIs.
2. Add a durable candidate-generation manifest and ready marker.
3. Retain the previous generation until active readers release it.
4. Add crash tests before and after candidate readiness and pointer activation.
5. Use this path only for explicit operations that require continued service.

Acceptance:

- a previous generation remains searchable until activation only when it is
  still proven complete (for example, an explicit operator rebuild)
- restart deterministically chooses the active complete generation
- incomplete candidates never become active
- retired generations are garbage-collected safely

## Excerpt: Former rollout order

Rollout order:

1. tests and local benchmarks
2. fault-injection deployment canary with forced process termination
3. staged binary rollout with ordinary dense replay using streaming sessions
4. removal of any unreachable ordinary-replay bulk-session plumbing
   after qualification

Automatic repair rolls out separately:

1. classification and status reporting without automatic execution
2. operator-triggered execution through the new state machine
3. automatic execution in fault-injection and canary environments
4. automatic execution by default with concurrency one
5. higher concurrency only after multi-group resource qualification
