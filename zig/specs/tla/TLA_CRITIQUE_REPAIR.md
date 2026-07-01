# TLA+ Critique Repair Tracker

This file tracks the specific modeling weaknesses raised during the critique of
the first broad TLA+ pass. It is intentionally sharper than
`MODEL_COVERAGE.md`: each item should say whether the bad behavior is actually
representable, which invariant is meant to catch it, and what validation proves
that the model is not just green by construction.

## Repaired In This Pass

| Critique | Repair | Validation |
|---|---|---|
| `AntflyDerivedReplay.tla` modeled replay-all fallback for hinted rows even though the shipped primary-store reader requires hint-lane rows. | Reworked the model around the implemented contract: replay-all rows, hint-lane rows, and per-hint latest metadata are distinct, and targeted replay is safe only when latest metadata corresponds to hint-lane rows. The expected-failure config now models latest metadata advancing without the hint lane. | `make tla-check-derived-replay`; `make tla-check-derived-replay-negative`; `make tla-check-derived-replay-heavy`; `make tla-check-critical`. |
| The HA models made acknowledged-write loss and split-brain writes structurally hard or impossible to express. | Added `AntflyHAFailoverSafety.tla`, a focused failover model with two standbys, explicit pre-promotion ack evidence, promoted-node preservation of acked writes, promotion fencing, and old-primary write authority. | `make tla-check-ha-failover-safety`; `make tla-check-ha-failover-negative-promote-missing-ack`; `make tla-check-ha-failover-negative-old-primary-write`; `make tla-check-critical`. |
| HA partition/fence propagation was still too implicit; a model could promote a standby only after a fence by construction. | Added `AntflyHAPartitionFence.tla`, where async fence delivery and old-primary writability are explicit, and the unsafe write is the normal old-primary append path. | `make tla-check-ha-partition-fence`; `make tla-check-ha-partition-fence-negative-promote-before-fence`; `make tla-check-critical`. |
| `AntflyHAGates.tla` was a static decision table and could not catch stale allow decisions after role/fence transitions. | Kept the table, added `AntflyHAGateTransitions.tla` as the transition sibling, and documented that the broad replication model delegates deepest failover obligations to focused siblings. | `make tla-check-ha-gate-transitions`; `make tla-check-ha-gate-transitions-negative-stale-allow`; `make tla-check-critical`. |
| Shard split data-loss reasoning could collapse repeated writes to a set of keys. | Added `AntflyShardSplitSeq.tla` with distinct write sequence IDs and a stale-fence/key-set cutover mutant. | `make tla-check-shard-split-seq`; `make tla-check-shard-split-seq-negative-keyset-cutover`; `make tla-check-critical`. |
| Snapshot safety did not prove content/index provenance. | Added `AntflySnapshotContent.tla` so applied content must match the applied raft snapshot index. | `make tla-check-snapshot-content`; `make tla-check-snapshot-content-negative-wrong-content`; `make tla-check-critical`. |
| LSM cleanup/reserve safety was partly represented as assumptions. | Added `AntflyLsmReserveCleanup.tla` with explicit reserve, publish, failure, and cleanup transitions. | `make tla-check-lsm-reserve-cleanup`; `make tla-check-lsm-reserve-cleanup-negative-publish-without-reserve`; `make tla-check-lsm-reserve-cleanup-negative-failure-leaks-temp`; `make tla-check-critical`. |
| Product-surface control-plane gaps included batcher coalescing, CDC cutover, and query completeness. | Added `AntflyBatcherCoalescing.tla`, `AntflyCdcCutover.tla`, and `AntflyQueryCompleteness.tla`, each with expected-failure configs. | `make tla-check-batcher-coalescing`; `make tla-check-cdc-cutover`; `make tla-check-query-completeness`; `make tla-check-negative`; `make tla-check-critical`. |

## July 1 Critic Follow-Up

| Critique | Repair | Validation |
|---|---|---|
| `AntflySnapshotContent.TargetContentNotGcBeforeApply` was vacuous because the GC mutant removed the stored snapshot and therefore disabled the invariant guard. | Added explicit follower-local fetched/needed state. The invariant now says fetched target content remains stored and content-matching until apply clears the need. Added `AntflySnapshotContentBadGcNeededContent.cfg`. | `make tla-check-snapshot-content`; `make tla-check-snapshot-content-negative-gc-needed-content`; `make tla-check-critical`; `make tla-check-negative`. |
| New negative configs mostly checked `Safety`, so future vacuity could hide behind another conjunct. | Repinned the critic-response negative configs to named semantic invariants, and added missing mutants for write->delete batcher order, CDC resume replay, snapshot GC, and query missing-doc. | `make tla-check-negative` now includes the new targets and exits 0 only when those named invariants fail. |
| `AntflyCdcCutover.CrashResume` was a stutter, batcher only explored delete->write, and HA gate transitions declared `former_primary` without entering it. | CDC now has a real crashed phase and resumes cursors from durable checkpoint. Batcher now nondeterministically explores both two-operation orders. HA gate transitions can enter `former_primary`. | `make tla-check-cdc-cutover`; `make tla-check-cdc-cutover-negative-resume-replay`; `make tla-check-batcher-coalescing`; `make tla-check-batcher-coalescing-negative-write-delete-inversion`; `make tla-check-ha-gate-transitions`. |
| `NoMissingDocs` needed its own mutant instead of relying on route-before-ready. | Added `BuggyDropMovedDoc` and `AntflyQueryCompletenessBadMissingDoc.cfg`, where the route is ready but the child lacks the moved doc. | `make tla-check-query-completeness-negative-missing-doc` fails on `NoMissingDocs`. |
| Model/code correspondence was thin for the top-stakes models. | Added focused Zig correspondence anchors for replay co-write decode-failure fallback, DB split same-key latest content, and provisioned coalescer same-key order. HA and CDC keep existing focused tests as anchors; CDC live Postgres remains environment-dependent. | TLA validation is green. Zig execution could not be run locally because no `zig` binary was on PATH; the new provisioned coalescer delete->write test is expected to expose a product bug unless implementation order handling changes. |

## July 1 Final Critic Follow-Up (Pass 2)

| Critique | Repair | Validation |
|---|---|---|
| Several negative configs still checked the `Safety` conjunction (HA failover x2, derived replay, enrichment lease x2, HA replication stale-timeline ack, transaction session x3), so drift could re-hide vacuity behind another conjunct. | All nine repinned to the specific semantic invariant each mutant targets. | Each focused negative target fails on exactly the pinned invariant name. |
| Five semantic invariants had no pinning mutant: `AckEvidenceExists`, `DeleteVisibleBeforeWriteOnly`, `StreamStartsAfterSnapshotHighWater`, `NoOldPrimaryWritesAfterPromotion`, `CutoverPreservesAllFencedWrites`. | Added `BuggyAckWithoutReceipt` (new mutant action in HA failover) plus four pinning configs that reuse existing mutants: `AntflyHAFailoverSafetyBadAckWithoutReceipt`, `AntflyBatcherCoalescingBadDeleteVisibleWithoutDelete`, `AntflyCdcCutoverBadStreamBeforeSnapshotComplete`, `AntflyHAPartitionFenceBadOldPrimaryWriteAfterPromotion`, `AntflyShardSplitSeqBadFencedWriteDropped`. All wired into `make tla-check-negative`. | Each new negative target fails on exactly the pinned invariant. |
| `PromotionRequiresFence` in HA failover is true-by-construction. | Documented in the spec as a regression tripwire for future edits to `FenceAndPromote`, not as verification evidence. | Comment in `AntflyHAFailoverSafety.tla`. |
| HA durability was not parameterized by commit mode; every ack was treated as preserve-required. | `AntflyHAFailoverSafety` now separates `syncAcked` (standby receipt required, must survive promotion) from `asyncAcked` (primary-side ack, may be lost on failover by design). The durability invariant is `PromotedNodeHasAllSyncAckedWrites`; async loss states are reachable in the positive model and violate nothing. | `make tla-check-ha-failover-safety` green (449 distinct states); promote-missing-ack mutant fails on the sync-scoped invariant. |
| No new-generation spec had liveness; no-permanent-stall was unverified. | Added fairness + temporal properties via a `FairSpec` (positive configs only; mutants keep the unfair `Spec`): HA failover `EventuallyPromoted`; HA rejoin `EventuallyAssessed` + `RejoinEventuallyExecutes` (conditional on the state settling executable, since stale assessments and failed fork-record validation legitimately refuse forever); derived replay `CatchupEventuallyCompletes` (conditional on hint-lane availability infinitely often); enrichment lease `EnrichmentEventuallyDrains` (conditional on stable lease and no permanent worker failure — window state is epoch-pinned and not re-collectable after churn in this model). | All four positive targets green with temporal checking enabled. |
| The red delete->write coalescing correspondence test sat in the modeling branch. | Moved to the coalescing bug-fix branch so the modeling PR stays green; the passing docstore/lite replay co-write tests and the DB split latest-value test remain here. | `git diff` no longer touches `api/table_writes.zig` in the modeling branch. |

Liveness follow-up (open): the new temporal properties are checked but do not
yet have stall-injecting mutants demonstrating each would fail on a real
stall; add liveness mutants if a progress regression ever needs pinning.
Lease-churn re-collection in the enrichment model is a known modeling
limitation scoped out of the drain property.

## New Coverage Backlog From The Final Critique (July 1)

Control-plane surfaces with correctness stakes found modeled nowhere and
absent from every prior backlog. Ordered by stakes:

| Surface | Anchors | Stakes |
|---|---|---|
| Node drain lifecycle (active -> draining -> complete -> removed, crash recovery, idempotent cancel) | `metadata/state.zig`, `metadata/replication_backfill.zig`, `SCALING.md` | premature termination / availability |
| Table create/drop raft coordination (ID reserve + schema persist + discoverability; drop quiesce; crash orphaning) | `metadata/table_workflow.zig`, `metadata/storage/raft_apply_store.zig` | orphaned/phantom groups, data loss |
| Entity promotion single-owner across split/merge | `storage/db/promotion_runtime.zig` (`setOwner`), `RESOLUTION.md` | duplicate canonical documents |
| Merge crash-recovery parity with split (prepare/cutover crash windows, artifact-routing idempotence) | `AntflyDbSplitVisibility.tla` models merge shallower than split | double-serve / lost range |
| WAL retention expiry -> forced reseed ordering vs slot GC | `storage/ha/slot_store.zig`, `storage/ha/rejoin.zig` | unrecoverable standby window |
| Index lifecycle (stale -> building -> converged) outside split; shadow swap atomicity | `storage/db/catalog/index_manager.zig` | stale/partial query results |
| Distributed join lease lifecycle vs ownership changes mid-query | `api/distributed_join.zig` | duplicate/missing join rows |
| Backup slot vs WAL truncation vs reseed coordination (control-plane slice only) | `storage/ha/primary.zig` backup slots | corrupted in-flight backup |
| Per-subsystem owner-job gate composition (enrichment/CDC/compaction against gate transitions) | `storage/ha/owner_job_gate.zig` | standby-mutation corruption |

## Remaining Critique Backlog

| Area | Weakness | Required next validation |
|---|---|---|
| Shard split | Sequence-level delta loss is now modeled, but concrete split-delta payload bytes, multi-child shapes, and implementation-emitted traces remain abstract. | Keep `AntflyShardSplitSeq.tla` as the focused sequence sibling; add trace fixtures only if split bugs require more correspondence. |
| Snapshot transfer | Content/index provenance is now modeled, but byte/checksum preservation and full restore artifact integrity remain outside TLA+. | Cover bytes with checksum/restore/golden tests; add only small TLA siblings for new ordering/GC bugs. |
| HA gates | Static table plus transition sibling now covers stale allow decisions, but exact request payloads and stream progress stay abstract. | Keep the split: table for exhaustive static inputs, transition model for stale authority; add traces only if gate caching changes. |
| LSM lifecycle | Explicit reserve/fail/cleanup is now modeled, but exact file bytes, compaction generation selection, and allocator classes remain abstract. | Keep byte and compaction-content validation in Zig tests/simulations. |
| LSM WAL | The reader-pinning story currently mixes WAL segment retention with table/SSTable reader retention. | Split WAL durability from table reader pinning or add an explicit refinement note showing which code path the model claims to abstract. |
| Product surface | Batcher, CDC, and split query completeness now have control-plane models. Query/search top-K, pagination, HBC/kmeans recall, backup/restore byte integrity, joins/schema semantics, predicates, and ranking math remain under-modeled in TLA+ by design. The new batcher correspondence test points at a likely real delete->write coalescer order issue. | Investigate/fix the provisioned coalescer order issue in product code separately. Add non-TLA simulation/property/golden/fuzz/differential/checksum validation for numeric, byte, and formula data-plane behavior. Add TLA+ only for ordering, crash, visibility, consensus, authority, and publication bugs. |

## Current Stakes

The model suite is materially stronger after the critique, but it is not a
clean bill of health for the repository. It says that the modeled control-plane
contracts have positive checks and reachable expected-failure mutants under the
bounded abstractions. It does not prove vector recall, search ranking,
analyzer correctness, backup byte integrity, payload serialization, or all live
implementation traces.

High-stakes areas to keep under review are HA promotion/fencing, replay and
watermark advancement, split/query routing, snapshot/restore publication,
cleanup ownership, and generated API publication. These are the places where a
small ordering drift can become acknowledged write loss, split brain, missing
query results, duplicate delivery, or corrupted recovery state.

## Maturity Rule

Do not mark a model mature merely because TLC exhausts a bounded state space.
For each serious semantic invariant, the model should have:

- a concrete code/test anchor,
- a reachable bad state under a deliberate mutant,
- a Makefile target that demonstrates the expected failure,
- a short note explaining any implementation behavior intentionally left out.
