# Antfly TLA+ Modeling Plan

This plan describes how to turn the current TLA+ coverage into a thoroughly
well-modeled repository. The goal is not just to have many green TLC runs. The
goal is to keep executable models close enough to the Zig implementation that
they can catch bugs in replay visibility, storage ordering, crash recovery,
replication fencing, transaction cleanup, and API publication boundaries.

## Current Assessment

The current lower-level specs are useful scaffolding, but they are not yet a
complete formal model of the repository.

Strong areas:

- Existing transaction, shard split, snapshot transfer, LSM lifecycle, and raft
  trace validation specs already have deeper histories and stronger invariants.
- `AntflyHAReplication.tla` has meaningful state-space size and models several
  concrete HA decisions rather than only nominal role transitions.
- The Makefile can run fast and heavy model groups independently.

Weak areas:

- Several newer models are contract sketches with `TypeOK` plus a small number
  of semantic invariants.
- Many models do not yet encode the lower-level implementation mechanics where
  bugs are likely: stale hint lanes, partially visible replay metadata,
  generation fences, allocator failure cleanup, WAL/checkpoint publication, and
  read snapshot pinning.
- There is no traceability matrix mapping Zig unit/e2e tests to TLA actions,
  state variables, and invariants.
- There is no systematic negative validation showing that each invariant fails
  when the modeled bug is injected.
- Some green checks may be vacuous because the bad state is impossible to
  express in the model.

## Modeling Standard

Each durable model should meet these criteria before being considered mature:

1. **Implementation correspondence**: every state variable and action has a
   comment pointing to the Zig module/function/contract it abstracts.
2. **Bug expressiveness**: the model can express at least one realistic bad
   behavior observed in tests, code review, incident review, or a deliberately
   injected mutant.
3. **Non-vacuous invariants**: each semantic invariant has a known bad action or
   mutant model that violates it.
4. **Bounded but lower-level state**: constants are small enough for TLC, but the
   state still includes the lower-level concurrency, crash, replay, visibility,
   or ownership dimension being checked.
5. **Trace/test alignment**: important Zig tests have a documented model
   counterpart, and important model actions have at least one test or trace
   source where feasible.
6. **Runnable tiers**: each model has a fast safety config and, where useful, a
   heavier config. No single required check should be monolithic.

## Validation Matrix

`MODEL_COVERAGE.md` is the live coverage inventory. It maps model domains to
TLA files, Zig code anchors, test/trace anchors, current invariants, known gaps,
and next validation steps. Every serious model should eventually have entries
like:

| Area | Zig tests / traces | TLA model | Must-catch invariant | Negative validation |
|---|---|---|---|---|
| Replay hint lanes | `lib-db-test` replay tests, generated enrichment tests | `AntflyDerivedReplay` | no applied watermark skips targeted replay whose per-hint metadata has been published | mutant: advance latest metadata without the corresponding hint-lane row |
| Enrichment lease | enrichment runtime/generated enrichment tests | `AntflyEnrichmentLease` | stale lease epoch cannot publish; applied cannot skip hidden generated work | mutants: publish without epoch check; advance through hidden pending work |
| HA write gate | HA compatibility/chaos tests | `AntflyHAGates`, `AntflyHAReplication` | fail-closed never acks unsatisfied write | mutant: ack on degraded slot |

Validation rule: a model is not mature until at least one row connects it to a
real Zig test or trace and at least one row describes a negative model/test.

## Phase 0: Repository Hygiene

Objective: make model development repeatable before adding more semantics.

Tasks:

- Remove generated TLC traces from source directories whenever TLC writes them.
- Add `.gitignore` coverage for `specs/tla/*_TTrace_*`.
- Add `make tla-clean` to remove TLC trace artifacts and temporary model output.
- Add a `bash ../scripts/tla-check.sh list` or `make` help entry that shows all TLA targets.
- Document local Java/TLC bootstrapping in one place and avoid hidden machine
  dependencies.

Validation:

- `rg --files specs/tla | rg '_TTrace_'` returns no output after a clean run.
- `make tla-tools` succeeds on a clean machine or fails with clear instructions.
- `make tla-check TIER=fast` remains green.

## Phase 1: Coverage Inventory

Objective: know what is modeled, what is only sketched, and what is missing.

Status: bootstrapped in `MODEL_COVERAGE.md`; keep it current as models deepen.

Tasks:

- Build a source inventory from README, OpenAPI specs, and major Zig modules.
- Group the repo into model domains:
  - HA/raft/replication
  - replay/change journal/derived indexes
  - generated enrichment and artifact publication
  - LSM/WAL/LMDB/backends
  - document identity and visibility
  - transactions/session/recovery
  - shard split/merge/range ownership
  - Lite/serverless publication
  - OpenAPI/codegen/API compatibility
  - ML graph/runtime pass ordering
- For each existing model, record:
  - code files it claims to abstract
  - tests that exercise the same contract
  - invariants currently checked
  - known omissions
  - expected state count and runtime

Validation:

- Every existing `Antfly*.tla` file has a corresponding inventory row.
- Every high-risk subsystem has one of: mature model, draft model, or explicit
  reason it is not currently worth modeling.
- The inventory can be reviewed without reading every spec.

## Phase 2: Deepen Replay And Enrichment Models

This should be the first deepening pass. The current code/test work already
showed this is where lower-level bugs hide.

### Replay Hint Lanes

Current weakness: `AntflyDerivedReplay.tla` used to model hint lanes and
replay-all fallback at too high a level. It now distinguishes replay-all rows,
hint lanes, and latest-hint metadata, but it is still only partial because it
abstracts payload decoding, chunking, rebuild guards, and generated enrichment
interaction.

Status: partially deepened and critique-repaired. The model now follows the
shipped primary-store behavior: replay-all rows alone are not treated as a
fallback for targeted hinted replay; per-hint latest metadata must correspond to
hint-lane rows before a worker can safely advance. The remaining alignment
question is whether all-lane-only targeted rows are impossible by construction,
a producer-side bug, or a product behavior that should be changed.

Required state:

- `replayAll`: sequence-indexed records.
- `hintLane[hint]`: per-hint indexes that may lag, be unavailable, or be
  missing required targeted rows.
- `latestHintMeta[hint]`: metadata that can lag the lane.
- `visibleAll`, `visibleHint[hint]`: read visibility sets.
- `applied[index]`, `target[index]`, `queryTarget[index]`.
- `catchupActive[index]`, `bulkSessionActive[index]`.
- `truncateFloor`.

Required actions:

- append replay record with target hints
- publish replay-all before hint lane
- publish hint lane before latest metadata
- all-lane-only append that does not satisfy targeted hint-lane replay
- latest metadata publication without a hint-lane row as an expected-failure
  mutant
- advance applied watermark
- block target advance during catch-up or bulk session
- truncate replay below safe floor

Required invariants:

- no applied watermark advances past a hidden matching record
- query target never observes beyond safe applied sequence
- truncation never removes replay needed by any index
- hint-lane absence is never treated as proof that replay-all has no match
- stale latest-hint metadata cannot justify skipping replay-all

Validation:

- Add a mutant mode or separate negative config where latest hint metadata
  advances without a matching hint-lane row; TLC must fail.
- Map invariants to replay tests in `db.zig` around thin replay, generated
  enrichment, graph materialization, reopen catch-up, and replay truncation.
- Run:
  - `make tla-check CHECK=AntflyDerivedReplay`
  - focused `zig build lib-db-test -- --test-filter replay`
  - focused generated enrichment filters

### Enrichment Runtime

Status: partially deepened. `AntflyEnrichmentLease.tla` now models the worker
loop’s interaction with replay visibility, target sequence, applied sequence,
retry state, worker failure state, isolated request failures, lease-owned
collection/generation, and generated artifact publication. It also has two
expected-failure configs: stale publication after lease loss and applied
advancement through hidden pending generated work.

Remaining weakness: the model still abstracts request payload bytes, exact
provider/rate-limit error taxonomy, backoff timing, split-range fencing, and
direct shared state with `AntflyDerivedReplay.tla`.

Required state:

- lease owner and epoch
- runtime target/applied sequence
- pending document groups
- generated artifact window
- published artifacts
- retrying/worker_failed/last_error
- isolated failed indexes
- replay visibility from the replay model

Required actions:

- acquire/renew/lose lease
- collect pending documents
- process request and defer dense/chunked outputs
- flush generated replay window
- publish under lease epoch
- retry transient failure
- isolate request failure
- advance applied with no pending work

Required invariants:

- stale owner cannot publish generated artifacts
- generated artifact is not published before its source replay is visible
- applied sequence cannot skip pending generated work
- retryable failures do not permanently advance applied
- isolated failures do not poison unrelated indexes

Validation:

- Negative model: publish after lease loss must fail.
- Negative model: empty pending advances through hidden generated work must fail.
- Map to tests for generated dense/sparse/chunk assets, asset producers,
  unchanged-source skip, retry/rate-limit behavior, and split range fencing.
- Run:
  - `make tla-check CHECK=AntflyEnrichmentLease`
  - `make tla-check CHECK=AntflyEnrichmentLeaseBadStalePublish`
  - `make tla-check CHECK=AntflyEnrichmentLeaseBadEmptyPending`
  - focused `zig build lib-db-test -- --test-filter "generated enrichment"`
  - focused retry/isolation/restore enrichment runtime filters

## Phase 3: Deepen Storage Backend Models

### LSM WAL, Checkpoint, And Compaction

Status: partially deepened. `AntflyLsmWalCompaction.tla` now models WAL segment
assignment, synced versus unsynced entries, durable checkpoint prefixes,
crash/replay truncation of unsynced tails, corrupt current-tail isolation,
compaction input/publication, reader-pinned segment retention, and safe segment
retirement. It has expected-failure configs for unsynced checkpoint publication,
corrupt-tail rotation, and reader-pinned segment retirement.

Remaining weakness: the model still abstracts exact record bytes/CRC checks,
atomic temp-file replacement, run-file generations, detailed manifest contents,
multi-reader generation mapping, and the concrete mutable flush rows used by the
backend.

Required state:

- WAL segments with synced/unsynced entries
- manifest/checkpoint pointer
- mutable generation
- compacted levels
- atomic write temp/final files
- reader snapshots and pinned sequence/generation
- crash/reopen view

Required invariants:

- checkpoint never includes unsynced or corrupt-tail entries
- reopen recovers exactly the last durable prefix
- compaction publication is atomic from reader perspective
- reader-pinned generations are not reclaimed
- WAL truncation never drops needed replay before checkpoint durability

Validation:

- `make tla-check CHECK=AntflyLsmWalCompaction` is green with the segment-aware model.
- Negative model: checkpoint includes unsynced entry fails via
  `AntflyLsmWalCompactionBadCheckpoint`.
- Negative model: corrupt current tail rotates into retained history fails via
  `AntflyLsmWalCompactionBadCorruptRotate`.
- Negative model: compaction frees reader-pinned segment fails via
  `AntflyLsmWalCompactionBadPinnedRetire`.
- Current Zig alignment includes full `wal-test`, focused root snapshot pinning,
  and broad `lsm-backend-test` checkpoint/reopen/retirement coverage. The next
  validation step is to map backend APIs and tests directly to model actions and
  add trace fixtures for replay/reopen sequences.

### LMDB Commit

Status: partially deepened. `AntflyLmdbCommit.tla` now models two meta pages,
prepared data page images, data write/data sync/meta write/meta sync/publication
ordering, crash/reopen meta selection, active reader snapshots, free records
gated by the oldest reader transaction, and nested child transaction merge/abort
shape. It has expected-failure configs for writing meta before data durability
and reusing a reader-visible retired page.

Remaining weakness: the model still abstracts exact B-tree shape, page bytes and
checksums, named DB metadata details, overflow span layout, writer-thread and
async-IO scheduling internals, local mmap remap lifetime, and the no-sync /
no-meta-sync / write-map policy variants.

Required state:

- two meta pages and selected durable meta
- dirty pages
- child transaction pages
- free list / free floor
- active reader txn IDs
- crash points at each commit phase

Required invariants:

- reopened meta always points to a fully durable tree
- child abort cannot leak partial child writes
- child commit cannot race parent commit phases
- free pages are not reused while visible to a reader

Validation:

- `make tla-check CHECK=AntflyLmdbCommit` is green with the page/free-record model.
- Negative model: write meta before dirty page sync fails via
  `AntflyLmdbCommitBadMetaBeforeData`.
- Negative model: reuse reader-visible free page fails via
  `AntflyLmdbCommitBadReaderReuse`.
- Current Zig alignment includes LMDB engine reader-reclaim tests, wrapper crash
  publish phase tests, nested child transaction commit/abort tests, and full
  `lmdb-test` / `storage-lmdb-test` targets. The next validation step is to map
  storage simulation fixtures directly to model actions and add a heavier config
  with multiple dirty and retired pages.

## Phase 4: Deepen HA And Raft Integration

Current HA models are the best of the newly added set, but they still need
closer alignment with the actual HA records, write gates, sync wait providers,
timeline switches, and standby apply paths.

Status: partially deepened. `AntflyHAReplication.tla` now models sync wait
target provenance explicitly: target timeline, target LSN, target commit mode,
ack slot, ack timeline, and ack LSN. The normal ack transition requires an
eligible slot on the target timeline; the expected-failure mutant accepts a
stale-timeline ack and violates `SyncAckMatchesTargetTimeline`. To keep this
within the heavy tier, the model explores the full HA state machine until a sync
wait target is frozen, then narrows post-wait exploration to receive/apply and
status-ack paths. `AntflyHAGates.tla` now also models mutating background
runtime startup as a separate decision from owner jobs and writes, with an
expected-failure standby-runtime mutant. `AntflyHASyncWait.tla` is the first
smaller fast HA slice: it isolates frozen target timeline/LSN provenance,
promotion after freeze, captured ack source timeline/LSN evidence, and
wrong-timeline or below-target ack rejection. `AntflyHATimelineSwitch.tla` adds
the next fast HA slice: parent received/applied/safe progress at the switch
boundary, monotonic timeline/epoch checks, durable-switch crash recovery, and
old-timeline rejection after switching. `AntflyHAStandbyApply.tla` adds a fast
standby receive/apply slice for durable receive before apply, failed-apply
progress safety, idempotent replay, crash/reopen receive preservation, standby
client-write rejection, and mutating-runtime suppression. `AntflyHARejoin.tla`
adds the former-primary rejoin/reseed slice: fenced assessment, compatible
identity and parent timeline, retained fork coverage, forced-promotion policy,
fresh assessment before truncation, fork-record identity validation, and reseed
publication. `TraceAntflyHA.tla` adds checked-in HA trace-fixture refinement for
sync/apply, timeline switch, and rejoin events, with fixtures derived from
existing HA chaos/rejoin scenarios. `AntflyHAFailoverSafety.tla` adds a focused
sibling model for the critique-raised failover cases that were too easy to
assume away: a promoted standby missing a pre-promotion acknowledged write, and
old-primary writes after standby promotion.

Remaining weakness: HA record payload shape, exact standby apply callback and
DB transaction semantics, sync wait provider scheduling, remote-write versus
remote-apply mode detail in the fast sync-wait slice, exact rejoin/admin JSON
wire encoding, seed manifest/base-backup contents, and live trace emission from
HA Zig tests are still abstract. The HA trace refinement is currently
fixture-based rather than produced by instrumentation in `ha-test` or
`ha-chaos-test`.
The HA models are broad enough to catch stale timeline ack, moving
frozen-target, below-target ack, acknowledged-write loss on failover,
old-primary split-brain writes after promotion, switch-before-apply,
non-monotonic switch, old-timeline-after-switch, recovery-previous mismatch,
failed-apply progress, duplicate replay side effects, crash-lost durable
receive, standby client write, standby-runtime, unfenced rejoin, expired-WAL
rewind, forced-promotion rewind without policy, identity-mismatch rewind,
stale-assessment truncation, and fork-record mismatch bugs, but they are not
yet byte/record-level replication models.

Tasks:

- Split HA into several models rather than growing one huge one:
  - write gate and fail-closed/degraded decisions: started in
    `AntflyHAGates.tla`
  - replication stream and timeline switch: started in
    `AntflyHATimelineSwitch.tla`
  - sync wait provider and remote apply acks: started in `AntflyHASyncWait.tla`
  - standby apply/replay suppression: started in `AntflyHAStandbyApply.tla`
  - former-primary rejoin/reseed: started in `AntflyHARejoin.tla`
  - acknowledged-write failover and split-brain write safety: started in
    `AntflyHAFailoverSafety.tla`
- Add explicit record types matching HA stream records.
- Model local commit, mirrored replay payload commit, standby receive, standby
  apply, and ack reporting separately.
- Add failure modes: lost ack, delayed receive, apply failure after durable
  receive, timeline switch, fenced former primary, stale standby.

Required invariants:

- fail-closed never acknowledges unsatisfied durability/apply policy
- promoted standby has every write that the sync policy acknowledged before
  failover
- fenced former primary cannot commit client writes
- standby replicated apply cannot execute primary-only background mutation
- timeline switch prevents old timeline ack from satisfying new timeline write
- rejoin cannot serve until reseed catches up to the required floor

Validation:

- Negative model: stale timeline ack satisfies current write must fail. Current
  status: implemented by `AntflyHAReplicationBadStaleTimelineAck`.
- Negative model: failover promotes a standby that lacks an acknowledged write
  must fail. Current status: implemented by
  `AntflyHAFailoverSafetyBadPromoteMissingAck`.
- Negative model: old primary accepts a post-promotion write must fail. Current
  status: implemented by `AntflyHAFailoverSafetyBadOldPrimaryWrite`.
- Negative model: standby starts mutating background runtime must fail. Current
  status: implemented by `AntflyHAGatesBadStandbyRuntime`.
- Negative model: promotion mutates a frozen sync-wait target must fail.
  Current status: implemented by `AntflyHASyncWaitBadMoveTarget`.
- Negative model: wrong-timeline ack satisfies a frozen sync-wait target must
  fail. Current status: implemented by
  `AntflyHASyncWaitBadWrongTimelineAck`.
- Negative model: below-target ack satisfies a frozen sync-wait target must
  fail. Current status: implemented by
  `AntflyHASyncWaitBadBelowTargetAck`.
- Negative model: timeline switch before parent received/applied/safe progress
  catches up must fail. Current status: implemented by
  `AntflyHATimelineSwitchBadBeforeApplied`.
- Negative model: non-monotonic timeline/epoch switch must fail. Current
  status: implemented by `AntflyHATimelineSwitchBadNonMonotonic`.
- Negative model: old-timeline record accepted after switch must fail. Current
  status: implemented by `AntflyHATimelineSwitchBadOldTimeline`.
- Negative model: crash recovery from a durable switch with mismatched
  `previous_lsn` must fail. Current status: implemented by
  `AntflyHATimelineSwitchBadRecoveryPrevious`.
- Negative model: failed standby apply advances applied/safe-read/DB marker
  progress must fail. Current status: implemented by
  `AntflyHAStandbyApplyBadFailureAdvances`.
- Negative model: duplicate replay of an already-applied record repeats side
  effects must fail. Current status: implemented by
  `AntflyHAStandbyApplyBadDuplicateEffect`.
- Negative model: crash/reopen loses a durably received unapplied record must
  fail. Current status: implemented by
  `AntflyHAStandbyApplyBadCrashLosesReceive`.
- Negative model: standby accepts a client write must fail. Current status:
  implemented by `AntflyHAStandbyApplyBadClientWrite`.
- Negative model: standby starts a mutating background runtime must fail.
  Current status: implemented by
  `AntflyHAStandbyApplyBadBackgroundRuntime`.
- Negative model: former primary rewinds without a promotion fence must fail.
  Current status: implemented by `AntflyHARejoinBadUnfencedRewind`.
- Negative model: former primary rewinds after retained WAL no longer covers
  the fork must fail. Current status: implemented by
  `AntflyHARejoinBadExpiredWalRewind`.
- Negative model: forced-promotion rewind proceeds without explicit policy must
  fail. Current status: implemented by `AntflyHARejoinBadForcedRewind`.
- Negative model: identity, old-primary, or parent-timeline mismatch still
  rewinds must fail. Current status: implemented by
  `AntflyHARejoinBadIdentityMismatchRewind`.
- Negative model: stale assessment truncates after a late write must fail.
  Current status: implemented by `AntflyHARejoinBadStaleAssessment`.
- Negative model: missing or mismatched fork record is truncated must fail.
  Current status: implemented by `AntflyHARejoinBadForkMismatch`.
- Trace refinement: sync/apply, timeline switch, and rejoin fixtures must be
  consumed by `TraceAntflyHA.tla` and preserve HA trace safety invariants.
  Current status: implemented by `TraceAntflyHA.tla`,
  `TraceAntflyHA.cfgs`, and `specs/tla/traces/ha_*.ndjson`; validated by
  `make tla-trace-ha TRACE_FILES="specs/tla/traces/ha_*.ndjson"`.
- Map to `ha-compat-test`, `ha-chaos-test`, and HA DB tests.
- Keep `AntflyHAReplication` heavy, but add smaller fast configs for each slice.

### Managed Host Lifecycle

Status: partially deepened. `AntflyManagedHostLifecycle.tla` now models desired
metadata, hosted replicas, active replicas, live routes, durable apply stores,
file-backed replica catalog state, restart recovery, backup-restore bootstrap
prepare/success/failure, and restore cancellation on metadata removal.

Implemented:

- Restart restores active/routes only for cataloged desired groups.
- Metadata removal clears active routes, cancels pending restore bootstrap
  state, and removes replica catalog entries so removed groups cannot revive.
- Backup restore bootstrap has an explicit `preparing` state that cannot
  activate, route, or publish durable/catolog state before success.
- Hosted replicas must have durable apply-store state, but inactive hosted
  replicas may exist temporarily during cleanup.
- Added expected-failure configs:
  `AntflyManagedHostLifecycleBadPrematureRestore`,
  `AntflyManagedHostLifecycleBadStaleRoute`,
  `AntflyManagedHostLifecycleBadReviveRemoved`, and
  `AntflyManagedHostLifecycleBadRestoreCancel`.

Validation:

- `make tla-check CHECK=AntflyManagedHostLifecycle`: green, 288 distinct states.
- Negative model: restore activates/routes before bootstrap success fails via
  `AntflyManagedHostLifecycleBadPrematureRestore`.
- Negative model: metadata removal leaves stale route fails via
  `AntflyManagedHostLifecycleBadStaleRoute`.
- Negative model: removed catalog entry can revive after restart fails via
  `AntflyManagedHostLifecycleBadReviveRemoved`.
- Negative model: restore bootstrap is not cancelled on metadata removal fails
  via `AntflyManagedHostLifecycleBadRestoreCancel`.
- `bash ../scripts/tla-check.sh negative` and `make tla-check TIER=fast` are green after the
  managed-host deepening.
- Zig validation includes focused `raft-test` filters for backup-bootstrap
  catalog restart, route removal, and durable apply-store restart, plus a
  `lib-raft-sim-test` filter for deterministic route/replica removal. These
  steps are quiet on success.

Remaining precision work:

- Model peer-list refreshes, exact replica catalog records, bootstrap error
  details, WAL/proposal persistence, leader election, and queued metadata update
  ordering separately if those start producing model-worthy bugs.
- Add trace/refinement from managed host simulation events only if the event
  vocabulary can stay stable.

## Phase 5: Deepen Transactions And Sessions

The existing transaction model is mature relative to the new specs. The session
model is not.

Status: partially deepened. `AntflyTransactionSession.tla` now models
committed-base visibility versus staged writes, savepoint creation,
rollback-to-savepoint, participant prepare, commit/abort, stale pending
auto-abort, crash-finalized committed/aborted orphan intents, recovery
resolution, identity-row side effects, participant resolution, and cleanup
gating. It has expected-failure configs for rollback leakage, wrong recovery
decision, and premature cleanup with unresolved prepared participants.
`TraceAntflyTransactionSession.tla` now adds fixture-backed trace refinement for
savepoint rollback, committed/aborted finalized-orphan recovery, stale pending
auto-abort, participant resolution, and cleanup gating. The fixtures assert
post-action state fields rather than only event names.

Remaining weakness: the model still does not refine `AntflyTransaction.tla`
directly, abstracts exact transaction record and recovery batch row layout,
uses counts instead of concrete keys/versions/timestamps, and does not yet model
multi-key OCC predicate interactions. The session trace refinement is
fixture-based rather than emitted by live `transactions.zig` or public API
tests.

Tasks:

- Eventually make `AntflyTransactionSession.tla` explicitly refine or compose with the
  existing transaction protocol where possible.
- Keep the current savepoint, staged-write, participant recovery, orphaned
  intent, and cleanup model aligned with `db.zig` and `transactions.zig`.
- Add concrete version predicates, timestamps, and multi-key identity metadata
  side effects in a heavier config or sibling model.
- Add live or generated session trace hooks only if the fixture vocabulary can
  stay stable and avoid hiding behavior behind hand-maintained examples.

Required invariants:

- savepoint rollback cannot expose discarded staged writes
- committed transaction identity rows match committed document writes
- participant recovery cannot publish aborted intent data
- cleanup does not remove coordinator state before participants are resolved
- transaction/session fixtures must consume fully and match post-action staged,
  visible, identity, participant, and cleanup state

Validation:

- `make tla-check CHECK=AntflyTransactionSession` is green with 1,946 distinct states.
- Negative model: savepoint rollback leaves visible write fails via
  `AntflyTransactionSessionBadRollback`.
- Negative model: recovery resolves participant with wrong decision fails via
  `AntflyTransactionSessionBadRecoveryDecision`.
- Negative model: cleanup before prepared participants resolve fails via
  `AntflyTransactionSessionBadCleanup`.
- Trace refinement: checked-in savepoint, orphan recovery, and stale pending
  fixtures pass via `TraceAntflyTransactionSession.tla`; premature cleanup
  fixture fails via `bash ../scripts/tla-check.sh negative`.
- `bash ../scripts/tla-check.sh negative` and `make tla-check TIER=fast` are green after the
  transaction/session deepening.
- Focused Zig validation includes transaction abort/version, explicit
  participant-style resolve, stale pending auto-abort, participant cleanup
  preservation/unblock, committed orphan identity-row recovery, transaction
  recovery filters, `lib-db-txn-test`, and the public long-lived
  transaction-session HTTP route. The broader
  `root-test -- --test-filter "transaction session"` currently fails only
  because one durable-session HTTP test leaks a JSON allocation after passing
  assertions; no product fix was made in this modeling pass.
- Current gap: attempted low-level `db-test -- --test-filter ...` validation
  expanded into broad graph-heavy tests and is not counted; use a narrower
  build hook or add a dedicated transaction storage step before relying on those
  lower-level filters.

## Phase 6: Deepen Document Identity And Visibility

Current weakness: `AntflyDocumentIdentity.tla` is too small for how important
identity generation and ordinal projection are.

Status: partially deepened. `AntflyDocumentIdentity.tla` now models stable
logical-doc ordinal ownership, created/deleted generation visibility, current
public identity-read generation, resolved-doc-filter wire namespace/generation
context, namespace reassignment canonical-row rewrites, and strict open
namespace mismatch rejection. It has expected-failure configs for tombstoned
ordinal reuse, stale filter acceptance, and mismatched namespace acceptance.
`AntflyDocumentIdentityRangeRepair.tla` adds the split/merge namespace
compatibility and restore/import/runtime-repair boundary as a separate bounded
model so the original ordinal/generation model stays small.

Remaining weakness: the pair of models still abstracts exact internal key bytes,
canonical hash collisions, primary document payload/index row coupling, concrete
persisted status rows, exact split-range key encoding, full restore artifact
bytes/import streams, all-placement restore progress quorums, and multi-shard
namespace coordination.

Required state:

- logical document IDs
- physical ordinals
- generation namespace
- live/tombstone rows
- pending identity writes in a batch
- search snapshot generation
- split/restore namespace reassignment

Required invariants:

- live doc has at most one ordinal in a namespace/generation
- ordinal is not reused while old generation can be queried
- search snapshot resolves docs only through a visible identity generation
- tombstone prevents stale ordinal from being returned as live
- restore namespace mismatch fails closed unless repaired

Validation:

- `make tla-check CHECK=AntflyDocumentIdentity` is green with 899 distinct states.
- Negative model: reuse a tombstoned ordinal for a different logical document
  fails via `AntflyDocumentIdentityBadReuseOrdinal`.
- Negative model: search accepts a resolved-doc-filter with stale identity
  generation fails via `AntflyDocumentIdentityBadStaleFilter`.
- Negative model: strict open accepts mismatched configured/stored namespace
  fails via `AntflyDocumentIdentityBadNamespaceMismatch`.
- `AntflyDocumentIdentityRangeRepair.tla` is green with 95,040 distinct states.
- Negative range/restore models fail for unhealthy split acceptance, stale split
  destination namespace, merge namespace mismatch without opt-in, unapproved
  merge reassignment, strict restore namespace mismatch, and early restore-intent
  clearing.
- Focused Zig validation includes identity namespace reassignment, snapshot
  generation projection, stale writer rejection, strict namespace reopen/repair,
  ordinal exhaustion, explicit doc-id filter generation, resolved filter wire
  context, compaction preservation, and restore metadata/namespace rejection.
- Additional focused Zig validation now includes metadata split/merge
  doc-identity validators, data-storage split destination namespace handling,
  merge opt-in namespace application/rollback, and metadata-service
  restore-intent retention until runtime repair completes.
- `TraceAntflyDocumentIdentityRangeRepair.tla` validates checked restore
  fixtures for strict namespace rejection and incomplete-import recovery before
  runtime repair/restore-intent clear, plus expected-failure fixtures for
  mismatch acceptance and early clear.
- Remaining gap: the direct DB restore tests for strict namespace mismatch and
  incomplete deferred import recovery could not be cleanly isolated because
  `db-test` expanded into unrelated graph/enrichment failures. The checked
  fixtures close the model-fixture gap, but they are not live Zig-emitted
  evidence.
- Next precision step: add implementation-emitted/test-generated restore traces
  only if checked fixtures need to become live evidence; then decide whether
  concrete status rows or restore artifacts deserve their own sibling model.

## Phase 7: Deepen Split, Merge, And Range Ownership

Existing shard split model is deep. The newer DB split visibility model should
be connected to it and extended around enrichment/replay/index visibility.

Tasks:

- Model DB-local split visibility as a refinement of shard-level split state.
- Include parent range, child range, shadow indexes, split deltas, child-range
  artifact dispatch, enrichment fencing, and merge-style cutover.
- Model direct child writes and parent writes during prepare/finalize.

Required invariants:

- after cutover, parent cannot accept owning-range writes for child range
- child indexes are complete through required split generation before serving
- enrichment only publishes for current range owner
- merge receiver routes text/sparse/graph indexes through merged range

Validation:

- Negative model: parent accepts child-range write after cutover must fail.
- Negative model: child serves before replaying split deltas must fail.
- Map to split prepare/finalize, merge-style cutover, range fencing, and shadow
  index tests.

Current implementation status:

- `AntflyDbSplitVisibility.tla` now models parent/right range ownership, split
  snapshots and deltas, child replay, child text/sparse/graph index catch-up,
  child artifact placement, enrichment owner fencing, direct child writes,
  merge donor handoff, and merge receiver text/sparse/graph routing.
- Added expected-failure configs for post-cutover parent writes, premature child
  serving, post-handoff merge donor serving, and stale/non-owning enrichment
  publication.
- `AntflySplitRefinementBridge.tla` now links the shard-level fence/cutover
  boundary to DB-local replay/index readiness and metadata right-range routing.
  It has expected-failure configs for route-before-DB-serving,
  DB-serving-before-shard-cutover, and stale-fence cutover.
- `TraceAntflySplitRefinementBridge.tla` now validates checked-in cutover and
  rollback fixtures against the bridge, with an expected-failure fixture for
  metadata routing before DB serving.
- Validation completed with `make tla-check CHECK=AntflyDbSplitVisibility`,
  `make tla-check CHECK=AntflySplitRefinementBridge`, `bash ../scripts/tla-check.sh negative`,
  `make tla-check TIER=fast`, and focused split/merge root-test filters for
  split deltas, child artifact remote placement, shadow index backfill, parent
  trim/destination shard creation, split/merge enrichment fencing, durable
  text/sparse/graph routing, reopen/resume fencing, plus `db-split-vopr-test`
  and `db-split-sim-test`.
- Remaining precision work: implementation-emitted or test-generated split
  bridge traces, explicit crash/reopen interleavings during split and merge
  finalization, multiple child ranges/artifact IDs, and payload-level
  index/delete contents.

## Phase 8: Lite, Serverless, And Publication Models

Current weakness: `AntflyLitePublication.tla` is only a publication-order sketch.

Status: partially deepened. `AntflyLitePublication.tla` now models six
publication artifact families (`document`, `mutation`, `text`, `vector`,
`sparse`, and `graph`), manifest references, HEAD advancement, crash after
manifest write before HEAD advancement, retry of a stored manifest, failed
publication discard, query reader generation pinning, obsolete-generation
cleanup, and mixed-generation publication bugs. It has expected-failure configs
for manifest-before-artifacts, failed publication advancing HEAD, cleanup of a
reader-pinned generation, and visible generation refs mixing old/new artifacts.

Remaining weakness: the model still abstracts exact object-store CAS/write
bytes, native aflite page/checkpoint layout, multiple simultaneous readers,
manifest schema fields, per-index segment payload contents, real concurrent
publisher arbitration, and retention horizons beyond two generations. It is
accurate enough to catch lower-level publication-ordering and reader-pinning
bugs, but it is not a byte-level object-store or native Lite file model.

Tasks:

- Model data segment, manifest, index, graph/text/sparse/vector segment
  publication separately.
- Include crash/reopen and partially uploaded/publication-visible states.
- Include generation visibility and reader pinning.

Required invariants:

- visible manifest references only published segments
- reader generation cannot observe mixed segments from different generations
- failed publication cannot advance visible generation
- cleanup cannot delete reader-pinned segment data

Validation:

- `make tla-check CHECK=AntflyLitePublication` is green with 599 distinct states.
- Negative model: manifest published before segment fails via
  `AntflyLitePublicationBadManifestBeforeArtifacts`.
- Negative model: failed publication advances visible HEAD fails via
  `AntflyLitePublicationBadFailedHead`.
- Negative model: cleanup deletes reader-pinned generation data fails via
  `AntflyLitePublicationBadPinnedCleanup`.
- Negative model: visible generation mixes segment refs across generations fails
  via `AntflyLitePublicationBadMixedGeneration`.
- `bash ../scripts/tla-check.sh negative` and `make tla-check TIER=fast` are green after the
  Lite publication deepening.
- Zig validation currently includes broad `serverless-test` runs, full
  `lite-cli-test`, and a serial full `lite-native-test`. Several of these
  targets ignore `--test-filter`, so the validation is broader than intended
  rather than focused. A duplicate parallel Lite native run produced one
  restore-staging failure that did not reproduce serially; avoid overlapping
  full Lite targets when collecting evidence.
- Next precision step: split native checkpoint/free-page retention into a
  sibling model if deeper page-level behavior is needed, and add a serverless
  publication trace or explicit test-to-action table for manifest CAS,
  retry-after-manifest, query pinning, and retention/pruning.

## Phase 9: OpenAPI And Codegen Publication

Current weakness: `AntflyOpenApiCodegen.tla` models check/publication ordering,
but it does not connect to actual generated modules or compatibility rules.

Status: partially deepened. `AntflyOpenApiCodegen.tla` now models the checked
OpenAPI publication pipeline instead of only generator shape booleans:
representative source spec versions, joined public spec, prefixed public spec,
root `openapi.yaml`, generated package versions, generation modes, import
mappings, committed package state, failed partial generation, and the
`generated-check` pass/fail boundary. The bounded package families are schema,
indexes, public server/extractors, public client, and internal server; this
keeps the model fast while preserving the public/internal and joined/prefixed
boundaries that matter in `zig/build.zig`.

Remaining weakness: the model does not enumerate every generated directory,
does not compare exact generated bytes, does not model the full parser/resolver
AST, and does not encode every OpenAPI 3.0/3.1 shape rule. Generator internals
such as nullable-required handling, recursive oneOf, allOf flattening, and
discriminator emission are left to the standalone `lib/openapi` test suite for
now. A future sibling model may be useful if those AST-shape rules need formal
coverage independent of the publication pipeline.

Tasks:

- Inventory all OpenAPI specs and generated Zig packages.
- Model spec edit, generator run, checked generated files, client/server
  compatibility, and stale checked-in state.
- Include multiple spec families rather than one abstract spec.

Required invariants:

- checked generated package corresponds to the checked spec version
- public client cannot reference internal-only schema by accident
- server route contract cannot be published without matching schema generator
- codegen failure cannot leave mixed versions marked current

Validation:

- `make tla-check CHECK=AntflyOpenApiCodegen` is green with 216,814 distinct states.
- Negative model: generated-check passes with a stale generated package fails
  via `AntflyOpenApiCodegenBadStalePackage`.
- Negative model: generated-check passes with stale root `openapi.yaml` fails
  via `AntflyOpenApiCodegenBadStaleRoot`.
- Negative model: public client imports an internal-only generated package fails
  via `AntflyOpenApiCodegenBadInternalLeak`.
- Negative model: failed partial generation is committed fails via
  `AntflyOpenApiCodegenBadPartialCommit`.
- `bash ../scripts/tla-check.sh negative` and `make tla-check TIER=fast` are green after the
  OpenAPI deepening.
- Zig validation includes `zig build openapi-root-check`, standalone
  `lib/openapi` generator tests, `zig build antfly-client-test`, and a serial
  full `zig build ha-test` run covering generated admin/internal route
  dispatch, generated route method errors, OpenAPI ALL sync policy decoding,
  and HA HTTP client enum spelling.
- Current validation caveat: `ha-test` ignores appended `--test-filter` values,
  so exact route filters expanded into duplicate full HA suites. The serial
  `ha-test` pass is counted; duplicate parallel failures are not counted as
  product evidence.
- Next precision step: map every `addOpenApiRegenRun` entry in `build.zig` to a
  package-family row, add generated package hash/freshness evidence where
  feasible, and split generator AST-shape rules into a sibling model if
  tests-only coverage is not sufficient.

## Phase 10: ML Graph And Runtime Pass Models

Status: implemented as a bounded representative graph/pass/export/runtime
model plus a sibling bounded arbitrary-DAG CSE/DCE remapping model. It is no
longer just a generic graph scaffold, but it is still a `Partial` model rather
than a full proof over unbounded graph shapes, concrete op attributes, and
compiler artifacts.

Implemented:

- Replaced the four-node scaffold with a representative ML graph that covers
  const-fold, CSE, fuse, DCE, partition export, and graph runtime publication.
- Modeled CSE remapping, with an invariant that prevents stale dangling
  consumer inputs after duplicate elimination.
- Modeled the implementation contract that CSE must not deduplicate parameter
  or constant nodes, even when structural graph equality might otherwise look
  tempting.
- Modeled both DCE behaviors relevant to the implementation boundary:
  export-bound graphs can keep a fused op's primitive lowering closure, while
  vjp-only closure can also be pruned if the alternate is cleared.
- Modeled partition export's fused-op lower-closure requirement and
  cross-partition runtime input materialization.
- Modeled graph runtime fail-closed behavior when fallback partitions are
  present and native/PJRT runtime publication is required to reject them.
- Modeled failed pass partial-output suppression.
- Added sibling `AntflyMlCompilerPublication.tla` for partition export,
  compiler artifact, graph-version freshness, semantic KV runtime input/output
  selection, fallback gate, and runtime executor publication boundaries.
- Added sibling `AntflyMlGraphDagPasses.tla` for bounded arbitrary-DAG CSE/DCE
  rebuild contracts: duplicate op elimination, data-node preservation,
  consumer/output/parameter remapping, reachable-node DCE, compact topological
  `id_map`, and final dangling-reference exclusion.
- Added expected-failure configs:
  `AntflyMlGraphPassesBadDanglingCse`,
  `AntflyMlGraphPassesBadParameterDedup`,
  `AntflyMlGraphPassesBadMissingLowerClosure`,
  `AntflyMlGraphPassesBadFallbackRuntime`, and
  `AntflyMlGraphPassesBadPartialPublish`.
- Added compiler-publication expected-failure configs:
  `AntflyMlCompilerPublicationBadStaleCompile`,
  `AntflyMlCompilerPublicationBadMissingInput`,
  `AntflyMlCompilerPublicationBadOutputSelection`,
  `AntflyMlCompilerPublicationBadFallbackPublish`, and
  `AntflyMlCompilerPublicationBadPartialArtifact`.
- Added DAG pass expected-failure configs:
  `AntflyMlGraphDagPassesBadCseMissDuplicate`,
  `AntflyMlGraphDagPassesBadCseNoConsumerRemap`,
  `AntflyMlGraphDagPassesBadDceDropReachable`, and
  `AntflyMlGraphDagPassesBadDceNonTopoMap`.
- Wired all fourteen ML negative configs into `bash ../scripts/tla-check.sh negative`.

Tasks:

- Inventory ML graph pass pipeline and runtime artifacts. Done for the
  const-fold/CSE/fuse/DCE/export/runtime gate boundary.
- Model graph nodes, edges, pass prerequisites, pass outputs, replacement,
  publication, and rollback. Done for a bounded representative graph and a
  lower-level bounded DAG CSE/DCE remapping model.
- Include failure during pass and partial output cleanup. Done through
  `BuggyPartialPublish` and `FailedPassOutputNotVisible`.

Required invariants:

- no pass consumes an unpublished predecessor output: covered by
  `PassOrderRespected`
- no published graph has dangling node/edge references: covered by
  `CurrentGraphReferencesValid` and `ExportedGraphReferencesValid`
- failed pass output is not visible to later passes: covered by
  `FailedPassOutputNotVisible`
- replacement preserves required output nodes and shape contracts: covered by
  `OutputNodePreserved` and the representative graph's dependency invariants
- CSE preserves parameter/constant identity: covered by
  `ParameterAndConstantIdentityPreserved`
- exported fused nodes preserve their primitive lower closure when a vjp
  alternate is present: covered by `ExportedGraphReferencesValid`
- runtime native publication fails closed on fallback partitions: covered by
  `RuntimeGateFailsClosed`
- partition export materializes external runtime inputs: covered by
  `ExternalPartitionInputsMaterialized`
- compiler/runtime artifacts are fresh for the source graph version before
  executor publication: covered by
  `RuntimePublishesOnlyFreshCompleteArtifact`
- semantic KV compiler/runtime paths materialize cache inputs but expose only
  selected final runtime outputs: covered by
  `ExportMaterializesRequiredRuntimeInputs` and
  `RuntimeOutputSelectionIsExact`
- failed compiler artifacts are not visible to runtime publication: covered by
  `FailedCompileArtifactNotVisible`
- duplicate op producers in bounded DAGs are eliminated by CSE: covered by
  `CseMapIsSemantic`
- CSE remaps every consumer, output, and parameter list entry through the
  redirect map: covered by `CseRemapsConsumersOutputsAndParameters`
- CSE preserves parameter/constant nodes even in arbitrary DAG shapes: covered
  by `CseKeepsDataNodesDistinct`
- DCE keeps exactly the nodes reachable from remapped outputs: covered by
  `DceKeepsExactlyReachableNodes`
- DCE `id_map` is compact and preserves original topological order: covered by
  `DceMapIsCompactTopological`
- final compacted DAG references do not point at dropped nodes: covered by
  `FinalGraphReferencesValid`

Validation:

- `make tla-check CHECK=AntflyMlGraphPasses`: green, 36 distinct states.
- `make tla-check CHECK=AntflyMlCompilerPublication`: green, 36 distinct states.
- `make tla-check CHECK=AntflyMlGraphDagPasses`: green, 9 distinct states over three
  bounded DAG shapes.
- Negative model: stale CSE remap/dangling edge fails via
  `AntflyMlGraphPassesBadDanglingCse`.
- Negative model: parameter/constant identity collapse fails via
  `AntflyMlGraphPassesBadParameterDedup`.
- Negative model: exported fused node without lower closure fails via
  `AntflyMlGraphPassesBadMissingLowerClosure`.
- Negative model: runtime publication despite fallback partition fails via
  `AntflyMlGraphPassesBadFallbackRuntime`.
- Negative model: failed pass leaves partial output visible fails via
  `AntflyMlGraphPassesBadPartialPublish`.
- Negative model: stale graph/export compiler artifact publication fails via
  `AntflyMlCompilerPublicationBadStaleCompile`.
- Negative model: missing parameter/cache runtime input fails via
  `AntflyMlCompilerPublicationBadMissingInput`.
- Negative model: semantic KV side-output leak fails via
  `AntflyMlCompilerPublicationBadOutputSelection`.
- Negative model: executor publication despite fallback partition fails via
  `AntflyMlCompilerPublicationBadFallbackPublish`.
- Negative model: failed compiler partial artifact visibility fails via
  `AntflyMlCompilerPublicationBadPartialArtifact`.
- Negative model: missed duplicate CSE fails via
  `AntflyMlGraphDagPassesBadCseMissDuplicate`.
- Negative model: stale consumer/output/parameter remap after CSE fails via
  `AntflyMlGraphDagPassesBadCseNoConsumerRemap`.
- Negative model: DCE dropping a reachable node fails via
  `AntflyMlGraphDagPassesBadDceDropReachable`.
- Negative model: DCE non-topological compact map fails via
  `AntflyMlGraphDagPassesBadDceNonTopoMap`.
- `bash ../scripts/tla-check.sh negative` and `make tla-check TIER=fast` are green after the
  ML graph, DAG pass, and compiler-publication deepening.
- Zig validation includes focused `lib/ml` graph root filters for default
  pipeline, unary/binary CSE, DCE `id_map`, cleanup DCE, fixed-point pipeline,
  DCE vjp-pruning, chained fused DCE preservation, and custom fold-then-CSE; a
  direct lower test for fused linear primitive lowering; and package-local
  inference graph/runtime tests for partition outputs, fused export lower
  closure, external pair input materialization, fallback partition fail-closed
  gating, and native executor attachment.
- Compiler/runtime Zig validation includes package-local inference tests with
  `-Dpjrt=true` for PJRT parameter externalization, semantic KV past/present
  input/output handling, semantic KV final-output selection, and partition
  output computation, plus runtime fallback summary coverage in the default
  inference test build.
- Current validation caveat: full standalone `cd lib/ml && zig build test`
  fails in the existing SDPA dynamic-bias fuse test (`expected 9, found 4`).
  This was not fixed and is not counted as evidence for or against the model.
- Current PJRT validation caveat: default inference test builds do not include
  the PJRT compiler tests, so PJRT-specific filters must be run with
  `-Dpjrt=true` to count as evidence.

Remaining precision work:

- Add an arbitrary-DAG CSE/DCE model with bounded node classes and topological
  remapping rather than this one representative graph.
- Split pattern-specific fuse/shape rewrites into a sibling model if the SDPA,
  reshape, transpose, and scalar-broadcast rules need model-level coverage.
- Add lower-level artifact-byte/hash refinement for PJRT/native compiler outputs
  only if the HLO/native serialization becomes traceable enough to avoid a
  brittle spec.
- Add trace/refinement hooks only after the abstract graph/action vocabulary is
  stable enough to avoid noisy trace churn.

## Phase 11: Trace Refinement Expansion

The raft and transaction trace validation approach is valuable and should be
extended where traceable event streams are practical.

Candidates:

- derived replay worker traces
- enrichment runtime worker traces
- HA replication/write-gate traces
- LSM WAL/reopen simulation traces
- split/merge lifecycle traces

Tasks:

- Add trace event schemas in Zig only after the abstract model is stable.
- Build `Trace*.tla` specs that replay NDJSON events through model actions.
- Keep trace specs separate from model-checking specs.

Validation:

- For each trace spec:
  - `TraceMatched` consumes the full trace
  - core safety invariants hold at every event
  - malformed or edited trace fails
- Add Make targets that accept `TRACE_FILES=...`.

## Phase 12: Negative Validation Harness

Objective: prove invariants are doing real work.

Tasks:

- For each mature model, add one of:
  - a `*-bad.cfg` that enables a bad action
  - a small `Bad*.tla` module extending the model
  - a commented known-bad action with documented TLC output
- Add `bash ../scripts/tla-check.sh negative` that expects failure for negative configs.
- Store only concise expected-failure descriptions, not generated traces.

Current status: the aggregate harness now covers the original core models
(`AntflyTransaction`, `AntflyShardSplit`, `AntflySnapshotTransfer`, and
`AntflyLsmLifecycle`) as well as the newer focused lower-level models. The core
mutants are intentionally one representative realistic bug each, not exhaustive
per-invariant mutation coverage.

Validation:

- `bash ../scripts/tla-check.sh negative` fails if a bad model unexpectedly passes.
- Each semantic invariant has at least one negative validation entry.

## Phase 13: CI And Runtime Budgets

Model checks need tiers so they stay useful.

Targets:

- `tla-check-smoke`: implemented; parses checked-in TLA specs with SANY. It
  intentionally skips legacy `occ-2pc.tla` because its top-level module is named
  `model`, which SANY rejects when invoked by filename.
- `make tla-check TIER=fast`: the
  required developer check for focused lower-level specs.
- `tla-check-heavy`: implemented as an alias for `tla-check-new-heavy`, the
  larger focused state spaces.
- `tla-check-trace`: implemented as a dispatcher for
  `TRACE=raft|txn|txn-session|ha|split-bridge|doc-identity-range-repair TRACE_FILES=...`.
- `tla-check-negative`: implemented expected-failure checks.

Validation:

- `bash ../scripts/tla-check.sh smoke`: green.
- `bash ../scripts/tla-check.sh list`: shows smoke, fast, heavy, trace, negative, and per-model
  targets.
- `bash ../scripts/tla-check.sh negative` and `make tla-check TIER=fast` are green after the
  latest model additions.
- Every new or deepened model has a documented expected state count and focused
  validation note in `MODEL_COVERAGE.md`.

Remaining work:

- Existing legacy/core models now have representative expected-failure configs,
  and `MODEL_COVERAGE.md` has a consolidated positive model state-count/runtime
  table for the core, fast, and heavy tiers.
- Add per-invariant mutants to core models only where review finds an important
  invariant still plausibly vacuous.
- Heavy models are split before any one check becomes too large to run
  independently.
- CI can run smoke/fast; heavy remains manual or scheduled.

## Phase 14: Review Checklist For Each Model

Before calling a model mature, answer these questions in the model header:

- What exact Zig files/functions/contracts does this abstract?
- What is deliberately omitted?
- What bug class should this catch?
- Which tests or traces correspond to the modeled behavior?
- Which invariant would fail for the most likely implementation mistake?
- Can the model express stale visibility, crash, retry, or partial publication
  if that matters for the subsystem?
- What are the fast and heavy TLC budgets?

Validation:

- A reviewer can pick one invariant and identify the code path it protects.
- A reviewer can pick one major test and identify the model action/invariant
  that represents its contract.

## Immediate Next Work

The critic-response control-plane work is implemented, including the July 1
follow-up repairs:

1. Sequence/versioned split deltas: `AntflyShardSplitSeq.tla`.
2. Snapshot content/index provenance and non-vacuous needed-content GC:
   `AntflySnapshotContent.tla`.
3. HA gate role/fence/former-primary transition sibling:
   `AntflyHAGateTransitions.tla`.
4. Explicit LSM reserve/fail/cleanup ownership: `AntflyLsmReserveCleanup.tla`.
5. HA partition/fence delivery before promotion: `AntflyHAPartitionFence.tla`.
6. Batcher per-key coalescing for both delete->write and write->delete orders:
   `AntflyBatcherCoalescing.tla`.
7. CDC snapshot/stream/checkpoint cutover with modeled crash/resume:
   `AntflyCdcCutover.tla`.
8. Split query-route completeness, including route-ready missing-doc and
   double-serve mutants: `AntflyQueryCompleteness.tla`.

The July 1 final-critique coverage backlog is also closed: node drain
(`AntflyNodeDrainLifecycle.tla`), table create/drop (`AntflyTableLifecycle.tla`),
WAL retention/reseed + backup slots (`AntflyHARetentionReseed.tla`), entity
promotion single-owner handoff (`AntflyPromotionOwnerHandoff.tla`), and index
lifecycle (`AntflyIndexLifecycle.tla`), each with per-invariant pinned mutants
and a liveness property. Distributed join leases were routed to the sim
harness, owner-job gate composition to Zig race tests, and merge rollback is
not modeled because the implementation has no merge-abort path (rationale in
`TLA_CRITIQUE_REPAIR.md`). The expected-failure harness now requires a real
invariant/temporal violation in TLC output rather than any nonzero exit.

Remaining work should be validation depth, not broad speculative modeling:

1. Investigate the provisioned write coalescer delete->write order issue
   exposed by the new focused correspondence test. Keep product-code fixes out
   of the modeling-only pass; fix and validate separately.
2. Add live HA trace emission or test-generated fixture hooks if fixture-derived
   HA trace refinement is not enough; otherwise keep the current
   `TraceAntflyHA.tla` fixtures as validation evidence.
3. Add live transaction/session trace emission or test-generated fixtures if
   the `TraceAntflyTransactionSession.tla` fixtures need to become
   implementation-emitted rather than checked-in examples.
4. Add implementation-emitted or test-generated split bridge, derived replay,
   enrichment, and DB restore-repair traces only if checked fixtures and direct
   Zig tests are not enough for future changes.
5. Add per-invariant negative configs for core models only where review finds a
   serious invariant still plausibly vacuous.
6. Keep TLA+ scoped to ordering, crash, visibility, authority, and publication
   contracts. Do not model vector recall, BM25/analyzer math, predicate/hash
   correctness, backup bytes, or ranking/pagination arithmetic in TLA+ unless
   the bug is actually an ordering/visibility bug. Those need simulation,
   differential, golden-corpus, fuzz, checksum/restore, or property tests.

The priority is correspondence to the implementation and validation evidence,
not more files.

## Keeping Models Current

Use `MODEL_COVERAGE.md` as the model-to-code anchor table. Any change touching a
listed anchor should update the corresponding row if the contract changed, and
should run at least the focused target plus its negative mutant target. For
larger changes, run `make tla-check TIER=fast`; for release or merge gates, run
`make tla-check TIER=fast` and `bash ../scripts/tla-check.sh negative`.

Most drift-prone TLA areas:

- HA promotion, fencing, sync wait, timeline switch, standby apply, and rejoin:
  authority and ordering bugs are subtle, and code changes often cross module
  boundaries.
- Derived replay, enrichment, CDC, batcher, transaction/session recovery, and
  split/query routing: user-visible visibility depends on several watermarks or
  publication steps staying aligned.
- Shard split, DB split visibility, snapshot content, restore repair, managed
  host lifecycle, Lite publication, and OpenAPI generation: these are
  publication/cutover workflows where partial success or stale metadata can
  drift from the model.
- LSM/LMDB/WAL cleanup and commit paths: stable day to day, but any fsync,
  free-list, checkpoint, reserve, or cleanup ownership change should rerun the
  focused model and consider a new negative mutant.

Less drift-prone areas:

- The forked etcd/raft model is comparatively stable unless raft internals,
  trace events, snapshots, or config-change behavior are changed.
- Static gate-table cases are stable until roles, failure policy, commit mode,
  or runtime-start rules change.
- Mature legacy transaction/snapshot/split contracts are stable unless their
  public protocol boundaries change; avoid churn unless a real bug class is
  missing.
