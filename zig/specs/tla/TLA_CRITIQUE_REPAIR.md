# TLA+ Critique Repair Tracker

This file tracks the specific modeling weaknesses raised during the critique of
the first broad TLA+ pass. It is intentionally sharper than
`MODEL_COVERAGE.md`: each item should say whether the bad behavior is actually
representable, which invariant is meant to catch it, and what validation proves
that the model is not just green by construction.

## Repaired In This Pass

| Critique | Repair | Validation |
|---|---|---|
| `AntflyDerivedReplay.tla` modeled replay-all fallback for hinted rows even though the shipped primary-store reader requires hint-lane rows. | Reworked the model around the implemented contract: replay-all rows, hint-lane rows, and per-hint latest metadata are distinct, and targeted replay is safe only when latest metadata corresponds to hint-lane rows. The expected-failure config now models latest metadata advancing without the hint lane. | `make tla-check CHECK=AntflyDerivedReplay`; `make tla-check CHECK=AntflyDerivedReplayBad`; `make tla-check TIER=heavy`; `make tla-check TIER=fast`. |
| The HA models made acknowledged-write loss and split-brain writes structurally hard or impossible to express. | Added `AntflyHAFailoverSafety.tla`, a focused failover model with two standbys, explicit pre-promotion ack evidence, promoted-node preservation of acked writes, promotion fencing, and old-primary write authority. | `make tla-check CHECK=AntflyHAFailoverSafety`; `make tla-check CHECK=AntflyHAFailoverSafetyBadPromoteMissingAck`; `make tla-check CHECK=AntflyHAFailoverSafetyBadOldPrimaryWrite`; `make tla-check TIER=fast`. |
| HA partition/fence propagation was still too implicit; a model could promote a standby only after a fence by construction. | Added `AntflyHAPartitionFence.tla`, where async fence delivery and old-primary writability are explicit, and the unsafe write is the normal old-primary append path. | `make tla-check CHECK=AntflyHAPartitionFence`; `make tla-check CHECK=AntflyHAPartitionFenceBadPromoteBeforeFence`; `make tla-check TIER=fast`. |
| `AntflyHAGates.tla` was a static decision table and could not catch stale allow decisions after role/fence transitions. | Kept the table, added `AntflyHAGateTransitions.tla` as the transition sibling, and documented that the broad replication model delegates deepest failover obligations to focused siblings. | `make tla-check CHECK=AntflyHAGateTransitions`; `make tla-check CHECK=AntflyHAGateTransitionsBadStaleAllow`; `make tla-check TIER=fast`. |
| Shard split data-loss reasoning could collapse repeated writes to a set of keys. | Added `AntflyShardSplitSeq.tla` with distinct write sequence IDs and a stale-fence/key-set cutover mutant. | `make tla-check CHECK=AntflyShardSplitSeq`; `make tla-check CHECK=AntflyShardSplitSeqBadKeySetCutover`; `make tla-check TIER=fast`. |
| Snapshot safety did not prove content/index provenance. | Added `AntflySnapshotContent.tla` so applied content must match the applied raft snapshot index. | `make tla-check CHECK=AntflySnapshotContent`; `make tla-check CHECK=AntflySnapshotContentBadWrongContent`; `make tla-check TIER=fast`. |
| LSM cleanup/reserve safety was partly represented as assumptions. | Added `AntflyLsmReserveCleanup.tla` with explicit reserve, publish, failure, and cleanup transitions. | `make tla-check CHECK=AntflyLsmReserveCleanup`; `make tla-check CHECK=AntflyLsmReserveCleanupBadPublishWithoutReserve`; `make tla-check CHECK=AntflyLsmReserveCleanupBadFailureLeaksTemp`; `make tla-check TIER=fast`. |
| Product-surface control-plane gaps included batcher coalescing, CDC cutover, and query completeness. | Added `AntflyBatcherCoalescing.tla`, `AntflyCdcCutover.tla`, and `AntflyQueryCompleteness.tla`, each with expected-failure configs. | `make tla-check CHECK=AntflyBatcherCoalescing`; `make tla-check CHECK=AntflyCdcCutover`; `make tla-check CHECK=AntflyQueryCompleteness`; `bash ../scripts/tla-check.sh negative`; `make tla-check TIER=fast`. |

## July 1 Critic Follow-Up

| Critique | Repair | Validation |
|---|---|---|
| `AntflySnapshotContent.TargetContentNotGcBeforeApply` was vacuous because the GC mutant removed the stored snapshot and therefore disabled the invariant guard. | Added explicit follower-local fetched/needed state. The invariant now says fetched target content remains stored and content-matching until apply clears the need. Added `AntflySnapshotContentBadGcNeededContent`. | `make tla-check CHECK=AntflySnapshotContent`; `make tla-check CHECK=AntflySnapshotContentBadGcNeededContent`; `make tla-check TIER=fast`; `bash ../scripts/tla-check.sh negative`. |
| New negative configs mostly checked `Safety`, so future vacuity could hide behind another conjunct. | Repinned the critic-response negative configs to named semantic invariants, and added missing mutants for write->delete batcher order, CDC resume replay, snapshot GC, and query missing-doc. | `bash ../scripts/tla-check.sh negative` now includes the new targets and exits 0 only when those named invariants fail. |
| `AntflyCdcCutover.CrashResume` was a stutter, batcher only explored delete->write, and HA gate transitions declared `former_primary` without entering it. | CDC now has a real crashed phase and resumes cursors from durable checkpoint. Batcher now nondeterministically explores both two-operation orders. HA gate transitions can enter `former_primary`. | `make tla-check CHECK=AntflyCdcCutover`; `make tla-check CHECK=AntflyCdcCutoverBadResumeReplay`; `make tla-check CHECK=AntflyBatcherCoalescing`; `make tla-check CHECK=AntflyBatcherCoalescingBadWriteDeleteInversion`; `make tla-check CHECK=AntflyHAGateTransitions`. |
| `NoMissingDocs` needed its own mutant instead of relying on route-before-ready. | Added `BuggyDropMovedDoc` and `AntflyQueryCompletenessBadMissingDoc`, where the route is ready but the child lacks the moved doc. | `make tla-check CHECK=AntflyQueryCompletenessBadMissingDoc` fails on `NoMissingDocs`. |
| Model/code correspondence was thin for the top-stakes models. | Added focused Zig correspondence anchors for replay co-write decode-failure fallback, DB split same-key latest content, and provisioned coalescer same-key order. HA and CDC keep existing focused tests as anchors; CDC live Postgres remains environment-dependent. | TLA validation is green. Zig execution could not be run locally because no `zig` binary was on PATH; the new provisioned coalescer delete->write test is expected to expose a product bug unless implementation order handling changes. |

## July 1 Final Critic Follow-Up (Pass 2)

| Critique | Repair | Validation |
|---|---|---|
| Several negative configs still checked the `Safety` conjunction (HA failover x2, derived replay, enrichment lease x2, HA replication stale-timeline ack, transaction session x3), so drift could re-hide vacuity behind another conjunct. | All nine repinned to the specific semantic invariant each mutant targets. | Each focused negative target fails on exactly the pinned invariant name. |
| The "all negatives repinned" claim was itself an overclaim: 42 more configs used the single-line `INVARIANT Safety` form (missed by the earlier audit's multi-line grep) and 25 use broad multi-invariant lists. | Migrated all 42 single-line configs across the priority groups (HA sync-wait/timeline-switch/standby-apply/rejoin, document identity range repair, managed host lifecycle, ML graph/DAG/compiler publication) to named invariants, with intentional couplings documented in config headers. The remaining 25 broad-list configs are tracked migration debt, reported by `bash ../scripts/tla-check.sh audit`. | Each of the 42 verified to fail on exactly its pinned invariant (two initial pin guesses were corrected by TLC evidence: revive-removed -> `CatalogOnlyForDesiredReplica`, stale-compile -> `RuntimePublishesOnlyFreshCompleteArtifact`). |
| Five semantic invariants had no pinning mutant: `AckEvidenceExists`, `DeleteVisibleBeforeWriteOnly`, `StreamStartsAfterSnapshotHighWater`, `NoOldPrimaryWritesAfterPromotion`, `CutoverPreservesAllFencedWrites`. | Added `BuggyAckWithoutReceipt` (new mutant action in HA failover) plus four pinning configs that reuse existing mutants: `AntflyHAFailoverSafetyBadAckWithoutReceipt`, `AntflyBatcherCoalescingBadDeleteVisibleWithoutDelete`, `AntflyCdcCutoverBadStreamBeforeSnapshotComplete`, `AntflyHAPartitionFenceBadOldPrimaryWriteAfterPromotion`, `AntflyShardSplitSeqBadFencedWriteDropped`. All wired into `bash ../scripts/tla-check.sh negative`. | Each new negative target fails on exactly the pinned invariant. |
| `PromotionRequiresFence` in HA failover is true-by-construction. | Documented in the spec as a regression tripwire for future edits to `FenceAndPromote`, not as verification evidence. | Comment in `AntflyHAFailoverSafety.tla`. |
| HA durability was not parameterized by commit mode; every ack was treated as preserve-required. | `AntflyHAFailoverSafety` now separates `syncAcked` (standby receipt required, must survive promotion) from `asyncAcked` (primary-side ack, may be lost on failover by design). The durability invariant is `PromotedNodeHasAllSyncAckedWrites`; async loss states are reachable in the positive model and violate nothing. | `make tla-check CHECK=AntflyHAFailoverSafety` green (449 distinct states); promote-missing-ack mutant fails on the sync-scoped invariant. |
| No new-generation spec had liveness; no-permanent-stall was unverified. | Added fairness + temporal properties via a `FairSpec` (positive configs only; mutants keep the unfair `Spec`): HA failover `EventuallyPromoted`; HA rejoin `EventuallyAssessed` + `RejoinEventuallyExecutes` (conditional on the state settling executable, since stale assessments and failed fork-record validation legitimately refuse forever); derived replay `CatchupEventuallyCompletes` (conditional on hint-lane availability infinitely often); enrichment lease `EnrichmentEventuallyDrains` (conditional on stable lease and no permanent worker failure — window state is epoch-pinned and not re-collectable after churn in this model). | All four positive targets green with temporal checking enabled. |
| The red delete->write coalescing correspondence test sat in the modeling branch. | Moved to the coalescing bug-fix branch so the modeling PR stays green; the passing docstore/lite replay co-write tests and the DB split latest-value test remain here. | `git diff` no longer touches `api/table_writes.zig` in the modeling branch. |

Liveness follow-up (open): the new temporal properties are checked but do not
yet have stall-injecting mutants demonstrating each would fail on a real
stall; add liveness mutants if a progress regression ever needs pinning.
Lease-churn re-collection in the enrichment model is a known modeling
limitation scoped out of the drain property.

## New Coverage Backlog From The Final Critique (July 1) — RESOLVED

Every surface was either modeled or explicitly routed away from TLA+ with a
recorded rationale. Contract extraction was grounded in a fresh code read
before each model was written.

| Surface | Resolution |
|---|---|
| Node drain lifecycle | Modeled: `AntflyNodeDrainLifecycle.tla`. Drain/store-flag raft-transaction consistency, finalize preconditions (`ActiveNodeFinalizeRejected`, raft_apply_store.zig:1626/1648), safe_to_terminate debt gate (http_server.zig:1685-1691), registration-preserves-drain (the SCALING.md historical regression is a mutant), drain-eventually-safe liveness. 3 mutants. |
| Table create/drop coordination | Modeled: `AntflyTableLifecycle.tla`. In-memory desired vs raft-committed topology, per-command applies, crash rebuilds desired from committed (drop atomicity is deliberately NOT claimed — matches bootstrapDesiredFromCommitted behavior), convergence liveness. 2 mutants (UnknownTable guard, planner scope). |
| Entity promotion single-owner | Modeled: `AntflyPromotionOwnerHandoff.tla` — a SKETCH-TIER authority/ownership guardrail, not deep implementation correspondence (tiny state space; raft leadership and runtime hook detail intentionally collapsed). Detach-before-transfer-before-attach across split AND merge, runtime (non-durable) attachment with crash/reattach, isLocalOwner promotion gate, handoff-completes liveness. 3 mutants. |
| Merge crash-recovery parity | Covered by the combination of `AntflyPromotionOwnerHandoff` (the donor->receiver ownership window that AntflyDbSplitVisibility does not represent) and existing merge invariants in `AntflyDbSplitVisibility`. Prepare->cutover ROLLBACK is intentionally not modeled: the implementation has no merge-abort path, and modeling one would verify fiction. Partial per-index catch-up at finalize is covered generically by `AntflyIndexLifecycle` freshness contracts; noted as an accepted abstraction in AntflyDbSplitVisibility. |
| WAL retention expiry -> reseed vs slot GC | Modeled: `AntflyHARetentionReseed.tla`. Floor over active/unmarked slots (slot_store.zig:295-349), per-slot mark loop (crash-partial marking is the natural interleaving), mark-before-truncate invariant, no-permanent-unmarked-lag liveness. |
| Backup slot vs truncation vs reseed | Modeled in `AntflyHARetentionReseed.tla`: backup slot as a pinning slot (primary.zig:481-525); a slow backup MAY lose its slot to retention policy (real code gap, modeled as-is) but backup end must then FAIL CLOSED — success-after-truncation is the mutant. |
| Index lifecycle outside split | Modeled: `AntflyIndexLifecycle.tla`. stale->building->fresh with non-atomic durable status snapshots (db.zig:9611-9796), shadow-swap completeness, crash recovery re-validating a stale "fresh" snapshot against durable watermarks, build-converges liveness (strong fairness: crash loops interrupt enabledness but applied is durable). 2 mutants. |
| Distributed join lease lifecycle | ROUTED TO SIM HARNESS, not TLA+. The lease is advisory (no hard mutual exclusion to prove), expiry is continuous-time, and correctness rests on import-fallback and row-level dedup — data-shaped. A TLA+ model would either prove an invariant the code deliberately does not guarantee or bury the real question (imported-state completeness). CONCRETE WORK ITEM (so "routed" does not become "forgotten"): fault-injection shuffle-join scenarios in `metadata/sim_harness.zig` — kill/stall the finalizer group mid-`dispatching`, force lease expiry (`joinJobLeaseTtlMillis`), verify takeover via `tryImportRemoteJoinJobState` (api/distributed_join.zig:~1520,2653) produces a deterministic, duplicate-free result set. |
| Owner-job gate composition | ROUTED TO ZIG TESTS, not TLA+. The risk is a thread-level check-then-execute race within one role; the structural stale-allow class is already modeled in `AntflyHAGateTransitions`. CONCRETE WORK ITEM: race tests around `data/runtime.zig` `haOwnerJobCanRun` (decision cached per job kind) — acquire a fence / demote between the cached gate decision and job execution for compaction-publish, derived-effect-writer, enrichment-writer, and retention-advance job kinds; suggested home `pkg/antfly/src/storage/ha/` alongside owner_job_gate.zig tests. |

Harness hardening from this pass: `tla_check_expected_failure` now requires
an actual "Invariant ... is violated" / temporal-property violation in TLC
output — a bare nonzero exit (e.g. a spec error) no longer counts as an
expected failure. This trap was hit in practice while developing these
models (an underparenthesized ghost-variable assignment produced
"successor state not completely specified", which the old exit-code-only
check silently accepted as the mutant failing).

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
