# TLA+ Formal Specifications

## Standards

`INVENTORY.md` is the live inventory: every model's code anchors, checks,
invariants, correspondence evidence shape, assumptions, and open backlog.
The suite does not label models "mature." Exhausting a bounded abstraction is
evidence about that abstraction, not a refinement proof for the database.

For every model record: (1) code/test anchors for state and actions, (2) a
deliberately broken check for each important semantic rule, (3) explicit
bounds and fairness assumptions, (4) deliberately omitted behavior, and
(5) whether named implementation tests were actually observed executing.
The B1 and B5 July-25 smokeout findings are examples of why fairness and
enabling assumptions are part of the result rather than harmless boilerplate.

`bash ../scripts/tla-check.sh audit` is the static hygiene audit: leftover
`_TTrace_` artifacts, `Buggy*` constants no check enables, sections no tier
would ever run (hard failures), and Safety-conjunction-pinned checks
(reported migration debt). It runs as part of the default `make tla-check`
gate; run it before any handoff.

## What Belongs In TLA+

Model a contract here only when the bug lives in an interleaving, ordering,
crash window, or partial-visibility window that deterministic tests cannot
reliably trigger. Wrong values, formulas, byte layouts, and numeric
thresholds (recall, ranking math, checksums, payload bytes) belong in
property/simulation/golden/fuzz tests: a model of those abstracts away
exactly the thing that breaks. And when a deterministic checker of the real
artifact already exists (e.g. the generated-OpenAPI freshness checks), prefer
it over a model of the artifact. Routed-away work is tracked in
`INVENTORY.md` so it is not forgotten.

## Keeping Models Current

Three layers, in decreasing order of automation:

1. **Zig correspondence tests** (in the normal test suite) anchor the
   models' load-bearing assumptions; they fail on real drift, including
   after toolchain updates.
2. **CI** runs the full gate whenever specs or the TLA scripts change, and
   validates implementation-emitted raft/transaction traces whenever the
   traced code changes (`zig-tests.yml` `tla-verify`).
3. **Review discipline**: `INVENTORY.md` maps code files to models. A PR
   touching an anchored file should re-run that model and its mutants
   (`make tla-check CHECK=<id>`) and update the model if the contract
   changed. Useful/Sketch-tier models have no executable tripwire; review
   is the only guard there.


## Layout

Specs are organized by the code they model, mirroring `pkg/antfly/src`:
`metadata/` (topology, split routing, node/table lifecycle, CDC cutover),
`raft/` (snapshot transfer/content), `storage/ha/`, `storage/db/`,
`storage/lsm/`, `storage/lmdb/`, `storage/lite/`, `api/`, and `ml/`.
Vendored/legacy specs (the etcd raft family, `occ-2pc`) stay at the root in
their upstream layout, as do the docs and `traces/` fixtures.

Each model `<Model>.tla` has a sibling `<Model>.cfgs` file holding ALL of its
checks as named sections in verbatim TLC config syntax:

    ==== positive
    SPECIFICATION FairSpec
    ...
    ==== BadSomething
    SPECIFICATION Spec
    ...

The build extracts sections into `specs/tla/.generated/` at run time
(`scripts/tla-check.sh`); section names match the make-target check ids
(`positive`, `Bad*` mutants, `heavy-*`/`safety` variants). To hand-run one
check: `bash ../scripts/tla-check.sh <CheckId> <Model>` prints the generated
config path and the spec path to pass to TLC.

## Model Header Template

Every new `.tla` model must start with a comment block containing:

1. **Code anchors** — the implementation files/lines the model corresponds to.
2. **What the model proves** — the safety/liveness contracts, in one or two
   sentences each.
3. **Deliberate omissions** — what is abstracted away and why that is safe.
4. **State bounds** — the constants that bound the state space.
5. **Make targets** — the positive target and each expected-failure target.
6. **Correspondence tier** — test-backed, trace-backed, or hand-modeled
   (see the quality tiers in `INVENTORY.md`).

Negative configs must pin the specific semantic invariant the mutant
violates; if multiple invariants are intentionally coupled, say so in the
config header comment.

## Specs

### Transaction Protocol

Formal verification of the distributed 2PC + OCC + recovery + cleanup protocol.

- `AntflyTransaction.tla` -- Main specification (11 actions, 6 safety invariants, 3 liveness properties)
- `MC.tla` -- Model checking module with concrete constants for a small model
- `AntflyTransaction.cfgs` -- TLC configuration
- `AntflyTransactionBadSkipIntentConflict` -- Expected-failure pending-intent conflict mutant used by `bash ../scripts/tla-check.sh negative`
- `occ-2pc.tla` / `occ-2pc.cfg` -- Historical Piledriver spec that found the OCC lost update bug (PR #381)

### Shard Split Protocol

- `AntflyShardSplit.tla` -- Shard split lifecycle with delta replay, dual-actor cutover, child leader election, and non-atomic finalize (18 actions, 10 safety invariants, 1 liveness property)
- `ShardSplitMC.tla` -- Model checking module
- `AntflyShardSplit.cfgs` -- TLC configuration
- `AntflyShardSplitBadPrematureChildDefault` -- Expected-failure child-default-before-cutover mutant used by `bash ../scripts/tla-check.sh negative`

### Snapshot Transfer Protocol

Formal verification of the multi-raft snapshot creation, transfer, GC, and error classification.

- `AntflySnapshotTransfer.tla` -- Main specification (10 actions, 6 safety invariants, 2 liveness properties)
- `SnapshotTransferMC.tla` -- Model checking module (3 nodes, configurable retries/snapshots)
- `AntflySnapshotTransfer.cfgs` -- Full TLC configuration (safety + liveness)
- `AntflySnapshotTransfer-safety` -- Safety-only configuration (fast, ~90s)
- `AntflySnapshotTransferBadApplyWithoutPut` -- Expected-failure transfer-done-without-local-archive mutant used by `bash ../scripts/tla-check.sh negative`

### LSM Lifecycle OOM Safety

Formal verification of allocator-failure safety for Zig LSM cleanup ownership handoffs.

- `AntflyLsmLifecycle.tla` -- Provisioned read/write cache entry retirement, LSM mutable read snapshot retirement, and `IndexWriter.removeSegments` temporary allocation cleanup.
- `AntflyLsmLifecycle.cfgs` -- TLC configuration for safety invariants.
- `AntflyLsmLifecycleBadIndexTempLeak` -- Expected-failure index temporary allocation leak mutant used by `bash ../scripts/tla-check.sh negative`.

### Focused Lower-Level Models

These specs are bounded around cohesive protocols and lifecycles, not individual bugs or source files. Behaviors that mutate the same authoritative state and whose ordering creates bugs belong in one semantic module, with fast projection configs and heavier composed configs. Split models when their state/fairness assumptions are independent; use an explicit refinement bridge for a critical boundary rather than a monolith.

- `AntflyHAGates.tla` -- Exhaustive HA gate decision table for commit/read/write/owner-job/background-runtime behavior across role, fence, handoff, consistency, LSN, commit-mode, and failure-policy inputs.
- `AntflyHAGatesBadStandbyRuntime` -- Expected-failure standby mutating background runtime mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAGateTransitions.tla` -- Transition sibling for HA gate stale-decision safety across role and fence changes.
- `AntflyHAGateTransitionsBadStaleAllow` -- Expected-failure stale allow after role/fence transition mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAReplication.tla` -- Concrete HA replication slot progress, active/reseed/timeline eligibility, sync selection, sync wait target provenance, stale timeline ack rejection, fail-closed/degrade decisions, fencing receipts, standby promotion switch records, retained WAL floors, and former-primary rejoin.
- `AntflyHAReplicationBadStaleTimelineAck` -- Expected-failure stale timeline ack mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHASyncWait.tla` -- Fast HA sync-wait submodel for frozen target timeline/LSN provenance, captured standby ack evidence, timeline promotion after freeze, and below-target/wrong-timeline ack rejection.
- `AntflyHASyncWaitBadMoveTarget` -- Expected-failure promotion-mutates-frozen-target mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHASyncWaitBadWrongTimelineAck` -- Expected-failure wrong-timeline ack mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHASyncWaitBadBelowTargetAck` -- Expected-failure below-target ack mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHATimelineSwitch.tla` -- Fast HA timeline-switch boundary submodel for parent received/applied/safe progress, monotonic switch timeline/epoch, crash recovery from a durable switch record, and old-timeline rejection after switch.
- `AntflyHATimelineSwitchBadBeforeApplied` -- Expected-failure switch-before-parent-apply mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHATimelineSwitchBadNonMonotonic` -- Expected-failure non-monotonic timeline/epoch switch mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHATimelineSwitchBadOldTimeline` -- Expected-failure old-timeline record accepted after switch mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHATimelineSwitchBadRecoveryPrevious` -- Expected-failure crash recovery with mismatched switch `previous_lsn` mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAStandbyApply.tla` -- Fast HA standby-apply submodel for durable receive, failed-apply progress, idempotent replay side effects, crash/reopen receive preservation, standby write rejection, and mutating-runtime suppression.
- `AntflyHAStandbyApplyBadFailureAdvances` -- Expected-failure apply-failure-advances-progress mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAStandbyApplyBadDuplicateEffect` -- Expected-failure duplicate replay side effect mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAStandbyApplyBadCrashLosesReceive` -- Expected-failure crash-loses-durable-receive mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAStandbyApplyBadClientWrite` -- Expected-failure standby client write mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAStandbyApplyBadBackgroundRuntime` -- Expected-failure standby mutating background runtime mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHARejoin.tla` -- Fast HA former-primary rejoin submodel for fenced assessment, retained fork coverage, forced-promotion policy, stale assessment rejection, fork-record identity validation, rewind truncation, and reseed publication.
- `AntflyHARejoinBadUnfencedRewind` -- Expected-failure unfenced rewind mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHARejoinBadExpiredWalRewind` -- Expected-failure expired-WAL rewind mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHARejoinBadForcedRewind` -- Expected-failure forced-promotion rewind-without-policy mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHARejoinBadIdentityMismatchRewind` -- Expected-failure identity/timeline mismatch rewind mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHARejoinBadStaleAssessment` -- Expected-failure stale assessment truncate mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHARejoinBadForkMismatch` -- Expected-failure fork-record mismatch truncate mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAFailoverSafety.tla` -- Focused HA failover safety model for acknowledged-write preservation, promotion fencing, and old-primary split-brain write suppression. Durability is commit-mode parameterized: sync-acked writes must survive promotion, async-acked writes may be lost by design. Its positive config also checks the `EventuallyPromoted` liveness property via `FairSpec`; `AntflyHARejoin`, `AntflyDerivedReplay`, and `AntflyEnrichmentLease` follow the same pattern (fair positive spec with no-permanent-stall properties, unfair `Spec` for mutants).
- `AntflyHAFailoverSafetyBadPromoteMissingAck` -- Expected-failure promoted standby missing an acknowledged write mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAFailoverSafetyBadOldPrimaryWrite` -- Expected-failure old-primary post-promotion write mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyHAPartitionFence.tla` -- Focused HA partition model for asynchronous fence delivery before promotion and old-primary write suppression after promotion.
- `AntflyHAPartitionFenceBadPromoteBeforeFence` -- Expected-failure promote-before-fence-delivery mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyBatcherCoalescing.tla` -- Batcher per-key coalescing order and flush visibility for delete/write sequences.
- `AntflyBatcherCoalescingBadDeleteWriteInversion` -- Expected-failure delete/write inversion mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyBatcherCoalescingBadWriteDeleteInversion` -- Expected-failure write/delete inversion mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyBatcherCoalescingBadPartialVisibility` -- Expected-failure partial flush visibility mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyCdcCutover.tla` -- CDC snapshot high-water, stream cutover, and checkpoint-delivery safety.
- `AntflyCdcCutoverBadBoundaryDuplicate` -- Expected-failure snapshot/stream boundary duplicate mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyCdcCutoverBadCheckpointAhead` -- Expected-failure checkpoint-ahead-of-delivery mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyCdcCutoverBadResumeReplay` -- Expected-failure crash/resume cursor replay mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyShardSplitSeq.tla` -- Sequence-level shard split delta safety for repeated writes to the same key.
- `AntflyShardSplitSeqBadKeySetCutover` -- Expected-failure key-set cutover mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflySnapshotContent.tla` -- Snapshot content/index provenance model.
- `AntflySnapshotContentBadWrongContent` -- Expected-failure wrong-content-for-index mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflySnapshotContentBadGcNeededContent` -- Expected-failure GC-of-needed-content mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyRaftSchedulerFairness.tla` -- Bounded round-robin tick and Ready selection, including no hot-group starvation.
- `AntflyRaftSchedulerFairnessBadTickHot` -- Expected-failure activity-priority tick starvation mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyRaftSchedulerFairnessBadReadyHot` -- Expected-failure repeated-hot-group Ready scan mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyRaftReadyPipeline.tla` -- Cohesive Ready lifecycle: fair admission, denied-work deferral, message ownership, configuration apply, exact-index snapshot membership, outbox preservation, continuations, and budgets.
- Its Bad* checks cover repeated-hot visits, denied/early continuations, budget overflow, clone-before-admission, apply-before-ownership, aliased messages, and live-membership snapshot capture.
- `AntflyLsmReserveCleanup.tla` -- Explicit LSM reserve/fail/cleanup ownership model.
- `AntflyLsmReserveCleanupBadPublishWithoutReserve` -- Expected-failure publish-without-cleanup-reserve mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLsmReserveCleanupBadFailureLeaksTemp` -- Expected-failure failure-leaks-temp mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyQueryCompleteness.tla` -- Split query routing completeness model for no missing or duplicate docs during route/serving transitions.
- `AntflyNodeDrainLifecycle.tla` -- Node drain/scale-down lifecycle: drain/store-flag raft-transaction consistency, finalize preconditions, safe_to_terminate debt gate, registration-preserves-drain, and drain-eventually-safe liveness.
- `AntflyPlacementRepair.tla` -- Cohesive placement repair from duplicate-id recovery through stable replacement, expanded membership, latched final peers, authoritative leader proof, contraction, and source retirement.
- `AntflyPlacementRepairBadLoadSensitiveRetry` -- Expected-failure changing-load-selects-a-new-replacement mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyPlacementRepairBadReplicaRenumber` -- Expected-failure survivor-identity-renumbering mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyPlacementRepairBadRepairWithRebalance` -- Expected-failure mandatory-repair-plus-optional-rebalance mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyPlacementRepairBadDuplicateReplicaIds` -- Expected-failure duplicate-declared-replica-id mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyPlacementReadiness.tla` -- Store voter-report knowledge/estimate aggregation and exact stable-placement transition admission. Its B1 mutant reproduces an unknown follower count latching ambiguity against an exact leader count.
- `AntflyRuntimeStatusReconciliation.tla` -- Runtime observation precedence and storage-root provenance composed with complete join statistics, schema-migration progress, old-read-schema retention, and standby availability.
- `AntflyTableLifecycle.tla` -- Table create/drop lifecycle: in-memory desired vs raft-committed topology, per-command applies, crash rebuilding desired from committed, planner scope, and convergence liveness.
- `AntflyHARetentionReseed.tla` -- WAL retention floor vs per-slot reseed marking vs truncation, plus backup slots as retention pins with fail-closed backup end; no-permanent-unmarked-lag liveness.
- `AntflyPromotionOwnerHandoff.tla` -- Entity promotion single-owner handoff across split/merge: detach-before-transfer-before-attach, non-durable attachment with crash/reattach, isLocalOwner promotion gate, handoff-completes liveness. Sketch-tier authority guardrail; intentionally collapses raft leadership and runtime detail.
- `AntflyIndexLifecycle.tla` -- Two-generation index lifecycle with non-atomic durable status, shadow-swap completeness, bounded competing work, durable scheduler wakeups/admission, and explicit convergence assumptions. The B5-shaped mutant loses the second generation's rebuild wakeup.
- `AntflyQueryCompletenessBadRouteBeforeChildReady` -- Expected-failure route-before-child-ready mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyQueryCompletenessBadDoubleServe` -- Expected-failure parent/child double-serve mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyQueryCompletenessBadMissingDoc` -- Expected-failure child-serving-without-moved-doc mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDerivedReplay.tla` -- Derived index replay-all rows, hint lane visibility, latest hint metadata, per-index catch-up targets, applied/query targets, bulk-session blocking, and replay truncation floors.
- `AntflyDerivedReplay-heavy-depth` -- Depth-heavy single-index derived replay bounds used by `make tla-check TIER=heavy`.
- `AntflyDerivedReplay-heavy-multi-index` -- Multi-index derived replay bounds used by `make tla-check TIER=heavy`.
- `AntflyDerivedReplay-heavy` -- Full MaxSeq=3/two-index manual confidence bounds used by `make tla-check CHECK=AntflyDerivedReplay-heavy`.
- `AntflyDerivedReplayBad` -- Expected-failure stale/empty hint-lane mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyEnrichmentLease.tla` -- Generated enrichment worker target/applied watermarks, replay visibility, retry/isolation state, and lease-owned collection/generation/publication so stale work cannot publish and hidden pending generated work cannot be skipped.
- `AntflyEnrichmentLeaseBadStalePublish` -- Expected-failure stale lease publication mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyEnrichmentLeaseBadEmptyPending` -- Expected-failure hidden pending advancement mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLmdbCommit.tla` -- Zig LMDB prepared data pages, data-sync/meta-write/meta-sync publication phases, crash reopen meta selection, nested child transaction merge/abort, reader snapshots, and free-record reuse gated by oldest reader.
- `AntflyLmdbCommitBadMetaBeforeData` -- Expected-failure meta-before-data mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLmdbCommitBadReaderReuse` -- Expected-failure reader-visible page reuse mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLsmWalCompaction.tla` -- Segment-aware WAL append/sync/replay, crash truncation of unsynced tails, corrupt current-tail isolation, durable checkpointing, compaction publication, and reader-pinned segment retention.
- `AntflyLsmWalCompactionBadCheckpoint` -- Expected-failure unsynced checkpoint mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLsmWalCompactionBadCorruptRotate` -- Expected-failure corrupt-tail rotation mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLsmWalCompactionBadPinnedRetire` -- Expected-failure reader-pinned segment retirement mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDbSplitVisibility.tla` -- DB split/merge visibility for right-range snapshot copy, parent split deltas, child replay, text/sparse/graph shadow index catch-up, child artifact placement, enrichment owner fencing, direct child writes, and merge receiver index routing.
- `AntflyDbSplitVisibilityBadParentWrite` -- Expected-failure post-cutover parent child-range write mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDbSplitVisibilityBadChildServe` -- Expected-failure premature child serving before replay/index catch-up mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDbSplitVisibilityBadMergeDonor` -- Expected-failure merge donor post-handoff serving mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDbSplitVisibilityBadEnrichmentOwner` -- Expected-failure stale/non-owning enrichment publication mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflySplitRefinementBridge.tla` -- Boundary model linking stable-placement bootstrap admission, shard fence/cutover readiness, DB-local replay/index readiness, and metadata child routing.
- `AntflySplitRefinementBridgeBadRouteBeforeDbServing` -- Expected-failure metadata route-before-DB-serving mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflySplitRefinementBridgeBadDbServeBeforeShardCutover` -- Expected-failure DB child-serving-before-shard-cutover mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflySplitRefinementBridgeBadStaleFenceCutover` -- Expected-failure stale-fence split cutover mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflySplitRefinementBridgeBadBootstrapWithoutStablePlacement` -- Expected-failure leader-only destination bootstrap mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentity.tla` -- Document identity namespace, stable ordinal ownership, generation visibility, resolved-doc-filter context, canonical namespace repair, and strict namespace-open behavior.
- `AntflyDocumentIdentityBadReuseOrdinal` -- Expected-failure tombstoned ordinal reuse mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityBadStaleFilter` -- Expected-failure stale resolved-doc-filter generation mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityBadNamespaceMismatch` -- Expected-failure strict namespace-open mismatch mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityRangeRepair.tla` -- Document identity split/merge namespace compatibility and restore/import/runtime-repair ordering across representative healthy, mixed-version, reassignment-active, conflict, rebuild-required, and ordinal-capacity states.
- `AntflyDocumentIdentityRangeRepairBadSplitUnhealthy` -- Expected-failure split validation accepts unhealthy source identity status mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityRangeRepairBadSplitDestNamespace` -- Expected-failure split destination reports the wrong identity namespace mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityRangeRepairBadMergeMismatch` -- Expected-failure merge accepts incompatible donor/receiver namespaces without opt-in mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityRangeRepairBadMergeActiveReassign` -- Expected-failure merge reassignment runs without opt-in or healthy status mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityRangeRepairBadRestoreNamespace` -- Expected-failure strict deferred restore accepts a mismatched doc identity namespace mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyDocumentIdentityRangeRepairBadRestoreEarlyClear` -- Expected-failure restore intent clears before import recovery and runtime repair complete mutant used by `bash ../scripts/tla-check.sh negative`.
- `TraceAntflyDocumentIdentityRangeRepair.tla` -- Trace fixture validator for document identity range/restore repair sequences, including strict deferred restore namespace rejection and import recovery before runtime repair/intent clear.
- `TraceAntflyDocumentIdentityRangeRepair.cfgs` -- Trace validation config used by `make tla-trace-doc-identity-range-repair`.
- `traces/doc_identity_restore_namespace_reject.ndjson` and `traces/doc_identity_restore_repair_order.ndjson` -- Checked-in positive restore repair fixtures.
- `traces/negative/doc_identity_restore_accept_mismatch.ndjson` and `traces/negative/doc_identity_restore_early_clear.ndjson` -- Expected-failure restore repair fixtures used by trace-negative targets.
- `AntflyTransactionSession.tla` -- Session savepoints over distributed transaction prepare/commit/abort/recovery/resolve/cleanup, with committed-base visibility separated from staged writes, crash-finalized orphan intent recovery, identity-row side effects, and participant cleanup gating.
- `AntflyTransactionSessionBadRollback` -- Expected-failure rollback leakage mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyTransactionSessionBadRecoveryDecision` -- Expected-failure aborted-orphan wrong recovery decision mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyTransactionSessionBadCleanup` -- Expected-failure unresolved participant cleanup mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyManagedHostLifecycle.tla` -- Managed raft host desired/hosted/active/routes reconciliation, durable apply stores, replica catalog persistence, restart recovery, backup-restore bootstrap prepare/success/failure, and restore cancellation.
- `AntflyManagedHostLifecycleBadPrematureRestore` -- Expected-failure restore activation before bootstrap completion mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyManagedHostLifecycleBadStaleRoute` -- Expected-failure stale route after metadata removal mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyManagedHostLifecycleBadReviveRemoved` -- Expected-failure removed replica catalog revival mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyManagedHostLifecycleBadRestoreCancel` -- Expected-failure uncancelled restore bootstrap mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLitePublication.tla` -- Lite/serverless publication ordering for document/mutation/text/vector/sparse/graph artifacts, manifest references, HEAD advancement, crash-before-HEAD retry, reader generation pinning, failed publication discard, and cleanup retention.
- `AntflyLitePublicationBadManifestBeforeArtifacts` -- Expected-failure manifest-before-artifacts mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLitePublicationBadFailedHead` -- Expected-failure failed-publication visible HEAD mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLitePublicationBadPinnedCleanup` -- Expected-failure reader-pinned cleanup mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyLitePublicationBadMixedGeneration` -- Expected-failure mixed visible generation mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphPasses.tla` -- ML graph const-fold/CSE/fuse/DCE pass publication, parameter/constant identity preservation, fused lower-closure export, external partition runtime inputs, fallback runtime gates, and failed-pass partial-output suppression.
- `AntflyMlGraphPassesBadDanglingCse` -- Expected-failure CSE stale remap/dangling edge mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphPassesBadParameterDedup` -- Expected-failure parameter/constant identity collapse mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphPassesBadMissingLowerClosure` -- Expected-failure fused partition export without primitive lower closure mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphPassesBadFallbackRuntime` -- Expected-failure fallback partition runtime publication mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphPassesBadPartialPublish` -- Expected-failure failed-pass partial output publication mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphDagPasses.tla` -- Bounded arbitrary-DAG ML CSE/DCE remapping model for duplicate elimination, consumer/output/parameter remaps, reachable-node DCE, compact topological `id_map`, and final dangling-reference exclusion.
- `AntflyMlGraphDagPassesBadCseMissDuplicate` -- Expected-failure missed duplicate CSE mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphDagPassesBadCseNoConsumerRemap` -- Expected-failure stale consumer/output remap mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphDagPassesBadDceDropReachable` -- Expected-failure reachable node dropped by DCE mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlGraphDagPassesBadDceNonTopoMap` -- Expected-failure non-topological compact DCE map mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlCompilerPublication.tla` -- ML partition export, PJRT/native compiler artifact, semantic KV input/output selection, graph-version freshness, fallback gate, and runtime executor publication boundaries.
- `AntflyMlCompilerPublicationBadStaleCompile` -- Expected-failure stale graph/export compile publication mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlCompilerPublicationBadMissingInput` -- Expected-failure missing parameter/cache runtime input mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlCompilerPublicationBadOutputSelection` -- Expected-failure semantic KV side-output leak mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlCompilerPublicationBadFallbackPublish` -- Expected-failure fallback partition executor publication mutant used by `bash ../scripts/tla-check.sh negative`.
- `AntflyMlCompilerPublicationBadPartialArtifact` -- Expected-failure partial compiler artifact visibility mutant used by `bash ../scripts/tla-check.sh negative`.

### Raft Consensus (etcd/raft)

Abstract raft model forked from etcd's TLA+ spec, extended for antfly-zig's raft implementation.

- `etcdraft.tla` -- Core raft spec (elections, log replication, snapshots, config changes, message pipeline with `pendingMessages` -> `Ready` -> `messages`)
- `MCetcdraft.tla` / `MCetcdraft.cfg` -- Standalone model checking module
- `etcdraft.cfg` -- TLC configuration

### Raft Trace Validation

Validates that the zig raft implementation (`../raft/`) conforms to `etcdraft.tla` by replaying ndjson event traces through the TLA+ model. The zig test suite emits trace events when built with `-Dwith_tla=true`; each event is matched to a corresponding TLA+ action, and 8 safety invariants are checked at every state.

- `Traceetcdraft.tla` -- Trace refinement spec (maps ndjson events to etcdraft actions, adds `HandleSnapshotFromTrace` for cross-segment snapshots and `TracePostDrain` for end-of-trace message draining)
- `Traceetcdraft.cfg` -- TLC configuration (checks `TraceMatched`, `etcdSpec`, and 8 raft safety invariants)
- `../../scripts/tla-segment-raft-trace.py` -- Segments multi-run traces into per-cluster-lifecycle ndjson files with validity filtering
- `../../scripts/tla-validate-trace.sh` -- Runs TLC on each segment in parallel
- `../../src/tracing/raft_trace_logger.zig` -- Zig trace logger that emits ndjson events (pre-event synthesis for self-votes and self-acks, MsgSnap encoding matching the TLA+ snapshot model)

### Transaction Trace Validation

Validates that the distributed transaction implementation conforms to `AntflyTransaction.tla` by replaying ndjson traces. Constants (transactions, shards, keys) are derived from the trace file -- no MC module needed.

- `TraceAntflyTransaction.tla` -- Trace refinement spec
- `TraceAntflyTransaction.cfgs` -- TLC configuration (checks `TraceMatched` and 5 safety invariants)
- `../../scripts/tla-filter-txn-trace.py` -- Filters transaction traces for spec compatibility

### Transaction Session Trace Fixture Validation

Validates checked-in transaction/session fixtures against `AntflyTransactionSession.tla`. These fixtures mirror storage transaction recovery and public session savepoint scenarios, but they are not currently emitted live by Zig tests. Each event can assert post-action state fields so replay checks visible document count, identity-row count, staged intent count, savepoint state, participant resolution, and cleanup state.

- `TraceAntflyTransactionSession.tla` -- Trace refinement spec for `txn-session-trace` NDJSON events (`BeginSession`, `StageWrite`, `CreateSavepoint`, `RollbackToSavepoint`, `PrepareParticipant`, `Commit`, `Abort`, `MarkStalePending`, `RecoverStalePending`, `CrashFinalizeCommittedOrphan`, `CrashFinalizeAbortedOrphan`, `RecoverFinalizedIntents`, `ResolveParticipant`, `Cleanup`)
- `TraceAntflyTransactionSession.cfgs` -- TLC configuration (checks `TraceMatched` and session trace safety)
- `traces/txn_session_savepoint.ndjson` -- Savepoint rollback and commit fixture
- `traces/txn_session_orphan_recovery.ndjson` -- Committed and aborted finalized-orphan recovery fixture
- `traces/txn_session_stale_pending.ndjson` -- Stale pending auto-abort fixture
- `traces/negative/txn_session_bad_cleanup.ndjson` -- Expected-failure premature cleanup fixture

### HA Trace Fixture Validation

Validates checked-in HA trace fixtures against the focused HA contracts for standby receive/apply, sync-wait acknowledgements, timeline switch boundaries, and former-primary rejoin/reseed. These fixtures mirror existing HA chaos/rejoin scenarios, but they are not currently emitted live by the Zig HA tests.

- `TraceAntflyHA.tla` -- Trace refinement spec for `ha-trace` NDJSON events (`PrimaryAppend`, `StandbyReceive`, `StandbyApplySuccess`, `StandbyApplyFailure`, `FreezeSyncWait`, `StatusAck`, `TimelineSwitch`, `RejectOldTimeline`, `AssessRejoin`, `LateFormerPrimaryWrite`, `ExecuteRewind`, `ExecuteReseed`)
- `TraceAntflyHA.cfgs` -- TLC configuration (checks `TraceMatched` and HA trace safety invariants)
- `traces/ha_sync_apply.ndjson` -- Sync/apply fixture covering durable receive, failed apply, retry, idempotence, and frozen-target acknowledgement
- `traces/ha_timeline_switch.ndjson` -- Timeline-switch fixture covering parent progress, switch record, status acknowledgement, and old-timeline rejection
- `traces/ha_rejoin.ndjson` -- Rejoin fixture covering assessment, late write, rewind, and reseed publication paths

### Split Bridge Trace Fixture Validation

Validates checked-in split bridge fixtures against `AntflySplitRefinementBridge.tla`. These fixtures mirror representative shard fence/cutover, DB replay/index catch-up, metadata routing, and rollback orderings, but they are not currently emitted live by Zig tests.

- `TraceAntflySplitRefinementBridge.tla` -- Trace fixture spec for `split-bridge-trace` NDJSON events (`BeginSplit`, `ObserveDestinationStablePlacement`, `BootstrapDestination`, `ParentRightWrite`, `ReplayDelta`, `BuildTextIndex`, `BuildSparseIndex`, `BuildGraphIndex`, `SetShardFence`, `CompleteShardCutover`, `PublishDbChildServing`, `RouteMetadataToChild`, `Rollback`)
- `TraceAntflySplitRefinementBridge.cfgs` -- TLC configuration (checks `TraceMatched` and bridge safety invariants)
- `traces/split_bridge_cutover.ndjson` -- Positive cutover fixture covering DB catch-up before serving and metadata routing
- `traces/split_bridge_rollback.ndjson` -- Positive rollback fixture covering no child exposure after rollback
- `traces/negative/split_bridge_route_before_db_serving.ndjson` -- Expected-failure fixture where metadata routes to the child before DB serving is published

### Loading-Path Live Trace Validation

With `-Dwith_tla=true`, metadata aggregation and managed index repair emit
implementation observations for the July-25 B1 and B5 contracts. Ordinary
builds compile the emitters out. `../../scripts/extract-protocol-traces.py`
extracts per-group behaviors from mixed process logs before TLC replay.

- `TraceAntflyPlacementReadiness.tla` checks qualified voter-set evidence,
  unknown fallback reports, conflicting fingerprints, and exact stable
  transition admission.
- `TraceAntflyIndexLifecycle.tla` checks that every committed managed
  generation publishes durable work, worker admission follows that work, and
  replacement publication remains tied to the requested config generation.
- `TraceAntflyDerivedReplay.tla` checks hint-lane and replay-all fallback scan
  accounting plus catch-up watermark ordering.
- `TraceAntflyEnrichmentLease.tla` checks the durable lease record—not only the
  worker's cached ownership bit—at generated publication and checkpoint
  advancement.
- `traces/placement_readiness_b1_recovery.ndjson` and
  `traces/index_lifecycle_two_generations.ndjson`,
  `traces/derived_replay_hint_fallback.ndjson`, and
  `traces/enrichment_lease_publish.ndjson` are positive fixtures.
- `traces/negative/placement_readiness_unknown_latches_ambiguity.ndjson` and
  `traces/negative/index_lifecycle_lost_second_wakeup.ndjson`,
  `traces/negative/derived_replay_advance_beyond_target.ndjson`, and
  `traces/negative/enrichment_stale_owner_publish.ndjson` prove that the
  validators reject the corresponding failure shapes.

```bash
python3 ../scripts/extract-protocol-traces.py placement-readiness /tmp/placement /tmp/antfly.log
python3 ../scripts/extract-protocol-traces.py index-lifecycle /tmp/index /tmp/antfly.log
python3 ../scripts/extract-protocol-traces.py derived-replay /tmp/replay /tmp/antfly.log
python3 ../scripts/extract-protocol-traces.py enrichment-lease /tmp/lease /tmp/antfly.log
make tla-trace TRACE=placement-readiness TRACE_FILES="/tmp/placement/*.ndjson"
make tla-trace TRACE=index-lifecycle TRACE_FILES="/tmp/index/*.ndjson"
make tla-trace TRACE=derived-replay TRACE_FILES="/tmp/replay/*.ndjson"
make tla-trace TRACE=enrichment-lease TRACE_FILES="/tmp/lease/*.ndjson"
```

## Makefile Targets

Four verification targets (everything else is a subcommand of
scripts/tla-check.sh), plus three visualization targets (see Visualizations).

```bash
make tla-check                  # full gate: audit + parse + core + fast + all mutants
make tla-check TIER=heavy       # heavy tier (large state spaces; also: core, fast, manual)
make tla-check CHECK=<id>       # one check, e.g. CHECK=AntflyIndexLifecycleBadSwapIncomplete
make tla-trace TRACE=<family> TRACE_FILES=...   # NDJSON trace validation
make tla-clean                  # remove TLC runtime artifacts

make tla-viz                    # regenerate structural diagrams (specs/tla/diagrams/)
make tla-viz-check              # fail if committed diagrams are stale
make tla-viz-trace JSON=<file>  # render one NDJSON trace to an HTML timeline

# Direct runner subcommands (from zig/):
bash ../scripts/tla-check.sh list       # every check with its tier
bash ../scripts/tla-check.sh audit      # static hygiene audit
bash ../scripts/tla-check.sh smoke      # SANY-parse only
bash ../scripts/tla-check.sh negative   # all expected-failure checks
```

## Visualizations

Pedagogical views of the models — for reading the architecture, not for
verification. Two layers:

### Structural diagrams (auto-generated)

[`diagrams/`](diagrams/README.md) mirrors the spec layout with one generated
markdown file per model: phase state machines (extracted from action guards
and primed updates), an action/state table (reads resolved through helper
operators), and an action→variable write graph, all GitHub-rendered Mermaid.
Expected-failure mutant actions (named `Buggy*` or enabled by a bare `Buggy*`
conjunct) are omitted and counted in each file's header. Regenerate with
`make tla-viz` after editing a spec; `make tla-viz-check` is the staleness
gate. Generator: `../../scripts/tla-viz/gen_structural.py`
(tree-sitter-tlaplus based; extraction is heuristic and favors the suite's
disciplined conjunct-list action style).

### Trace timelines

`make tla-viz-trace JSON=<trace.ndjson> [OUT=<out.html>]` renders any of the
suite's NDJSON trace families as one self-contained HTML swimlane timeline
(no external assets — attachable to a PR or Zulip thread). Try the fixtures:

```bash
make tla-viz-trace JSON=specs/tla/example.ndjson OUT=/tmp/raft.html
make tla-viz-trace JSON=specs/tla/traces/split_bridge_cutover.ndjson OUT=/tmp/split.html
make tla-viz-trace JSON=specs/tla/traces/placement_readiness_b1_recovery.ndjson TLC=1 OUT=/tmp/b1.html
make tla-viz-trace JSON=specs/tla/traces/index_lifecycle_two_generations.ndjson TLC=1 OUT=/tmp/b5.html
make tla-viz-traces   # every fixture in one browsable file (sidebar per family)
```

A timeline's useful encodings are domain knowledge (what is an actor, what
deserves a tenure band, which events form causal pairs), so each trace family
is **assigned a visualization archetype** in
[`traces/viz.json`](traces/viz.json) (`scripts/tla-viz/archetypes.py`):

- `consensus` — symmetric node actors: a lane per node id, role tenure as
  lane tint, send→receive message arrows, multi-run segmentation (raft).
- `dialogue` — a small named cast encoded in the event names: lanes from
  name rules (`^Primary` → primary, `^Parent` → parent shard, ...) and
  declarative causal pairs matched on event fields
  (`PrimaryAppend → StandbyReceive` on timeline+lsn;
  `ParentRightWrite → ReplayDelta` on delta/replay seq). Used by `ha-trace`
  and `split-bridge-trace`.
- `narrative` — a single progressing actor: gets the **Replay** view, a
  frame-by-frame scene of the subsystem's stores/actors/lamps (declared in
  the binding) with per-step state diffs, flow captions, and display-
  invariant badges — step with ←/→ or autoplay, and watch e.g. a write move
  staged → intent store → visible store through a crash and recovery. Used
  by `txn-session-trace`, `doc-identity-range-repair-trace`,
  `index-lifecycle-trace`, `antfly-trace`, and as the fallback for tags with
  no binding yet. Placement readiness uses a dialogue view so replica reports,
  evidence resolution, and transition admission remain visually distinct.

Shared across archetypes: per-family category legends, and **fault emphasis**
(crash/corruption events always render in the reserved status-critical red
with a ⚠ label). Tenure-band colors can be linked to a model's phase variable
(`model` + `phaseVar` in the binding, via `scripts/tla-viz/phasecolors.py`),
which is the same assignment the diagrams layer uses for its state-machine
coloring — a phase value like `"splitting"` gets the same hue in
`diagrams/metadata/AntflySplitRefinementBridge.md` and in the timeline's
phase strips.

**Invariant violations are first-class**: pass `TLC=1` (CLI: `--tlc`) to
also replay the trace through the model with TLC and bake the verdict into
the artifact — a banner naming the violated invariant (or the `TraceMatched`
step the model refused) with the offending frame flagged in the replay rail.
Try it on a deliberately broken fixture:

```bash
make tla-viz-trace JSON=specs/tla/traces/negative/txn_session_bad_cleanup.ndjson TLC=1 OUT=/tmp/bad.html
```

If TLC/java is unavailable the render still succeeds without the verdict.
Cheap per-step `invariants` in the binding additionally render as live
green/red badges; TLC remains the authority.

**To wire up a new trace family — including at render time, without touching
the repo**: bindings can be supplied as an overlay
(`make tla-viz-trace JSON=... BINDING=my-binding.json`), merged over
`traces/viz.json` per tag. Full schema, worked example, and the
bug-to-artifact agent workflow: **`../../scripts/tla-viz/BINDINGS.md`**.

Each bound trace links to its model's generated state diagrams (the phase
colors match across both). Multi-run raft traces are split into segments
with the same boundary rules as `../../scripts/tla-segment-raft-trace.py`.

## Document Identity Range Repair Trace Fixture Validation

Checked-in fixtures can validate the restore repair boundary without invoking
the broad DB test suite:

```bash
make tla-trace-doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"
make tla-trace TRACE=doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"
bash ../scripts/tla-check.sh negative
bash ../scripts/tla-check.sh negative
```

The positive fixtures cover strict namespace-mismatch rejection and the
incomplete-import recovery order before runtime repair and restore-intent clear.
The negative fixtures ensure a mismatched strict deferred restore and an early
intent clear are rejected by `TraceMatched`/`TraceSafety`.

## Raft Trace Validation Workflow

Build the zig raft tests with tracing enabled, then validate the trace:

```bash
# 1. Build and run raft tests, capturing trace to stderr
~/bin/zig build -Dwith_tla=true raft-test 2>/tmp/zig-raft-trace.ndjson

# 2. Segment + validate (the Makefile target does both)
make tla-trace-raft TRACE_FILES=/tmp/zig-raft-trace.ndjson
```

The pipeline:
1. **Segmentation** (`scripts/tla-segment-raft-trace.py`) -- Splits the multi-run trace at cluster initialization boundaries. Filters out segments that can't be independently validated (partial runs, cross-segment snapshots without elections, missing nodes).
2. **Validation** (`scripts/tla-validate-trace.sh`) -- Runs TLC on each segment in parallel, checking that every trace event maps to a valid `etcdraft.tla` action and all 8 safety invariants hold.

### What's checked

| Property | What it ensures |
|---|---|
| `TraceMatched` | The entire trace is consumed (TLC explores the full ndjson log) |
| `etcdSpec` | Every trace event corresponds to a valid `etcdraft!NextDynamic` action |
| `LogInv` | Logs are append-only with monotonic terms |
| `ElectionSafetyInv` | At most one leader per term |
| `LogMatchingInv` | If two logs have an entry with same index and term, all preceding entries match |
| `QuorumLogInv` | Committed entries exist in a quorum of logs |
| `LeaderCompletenessInv` | A leader's log contains all committed entries from prior terms |
| `MoreThanOneLeaderInv` | No two leaders in the same term |
| `MoreUpToDateCorrectInv` | Vote comparison correctly identifies more up-to-date logs |
| `CommittedIsDurableInv` | Committed state is persisted to durable storage |

### Key design decisions

**Pre-event synthesis**: The zig raft engine processes some operations in a different order than the TLA+ model expects. The trace logger synthesizes events to bridge this:
- **Self-vote flush**: In 1-node clusters, a `Ready` event is synthesized before `ReceiveRequestVoteResponse` to flush the self-vote from `pendingMessages` to `messages`.
- **Self-ack flush**: Before `Commit` events, synthetic `Ready` + `ReceiveAppendEntriesResponse` events are emitted so `AdvanceCommitIndex` can see updated `matchIndex`.

**Post-trace drain**: After the last trace event, `TracePostDrain` uses `Ready` and `DropMessage` to empty the message bags so TLC's queue drains to zero. The `etcdSpec` check is relaxed during drain because `NextUnreliable` only allows `DropMessage` for messages with count=1, but accumulated heartbeats can have higher counts.

**Snapshot handling**: `HandleSnapshotFromTrace` handles `ReceiveSnapshot` events where the corresponding `SendAppendEntriesRequest` (MsgSnap) is in a prior trace segment. It consumes the message if present, otherwise proceeds without it, and correctly advances `commitIndex` to the snapshot index.

## Installation

### macOS

Install the TLA+ Toolbox (includes bundled JRE and `tla2tools.jar`):

```bash
brew install --cask tla+-toolbox
```

This installs:
- **GUI**: `/Applications/TLA+ Toolbox.app`
- **`tla2tools.jar`**: `/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar`
- **Bundled Java**: `/Applications/TLA+ Toolbox.app/Contents/Eclipse/plugins/org.lamport.openjdk.macosx.x86_64_14.0.1.7/Contents/Home/bin/java`

> **Note**: The Toolbox cask is built for Intel macOS and requires Rosetta 2 on Apple Silicon.

### Alternative: standalone tla2tools.jar

Download `tla2tools.jar` directly from [TLA+ GitHub releases](https://github.com/tlaplus/tlaplus/releases) and use your own Java installation (Java 11+).

## Running the Model Checker

### Using the bundled Toolbox Java

```bash
cd specs/tla

JAVA="/Applications/TLA+ Toolbox.app/Contents/Eclipse/plugins/org.lamport.openjdk.macosx.x86_64_14.0.1.7/Contents/Home/bin/java"
TLA2TOOLS="/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar"

"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC MC.tla \
    -config AntflyTransaction.cfgs -workers auto -deadlock
```

### Using system Java + standalone jar

```bash
cd specs/tla
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar tlc2.TLC MC.tla \
    -config AntflyTransaction.cfgs -workers auto -deadlock
```

### Expected output

```
Model checking completed. No error has been found.
2362 states generated, 637 distinct states found, 0 states left on queue.
```

The `-deadlock` flag suppresses false positives from terminal quiescence (all transactions completed, no more actions enabled).

## What it Verifies

### Safety Invariants (checked at every reachable state)

| Invariant | What it catches |
|---|---|
| `TypeOK` | All variables stay within their declared types |
| `AtomicityInvariant` | Aborted transaction writes never appear in the data store |
| `NoOrphanedIntents` | Txn records not deleted while intents still exist (the orphaned intents bug) |
| `OCCSerializationInvariant` | Conflicting OCC transactions can't both have intents written |
| `LWWConsistency` | Last-writer-wins timestamp ordering is correctly maintained |
| `SerializableReads` | Two txns that read the same version of a key can't both commit (catches the OCC lost update bug from PR #381) |

### Liveness Properties (checked under weak fairness)

| Property | What it ensures |
|---|---|
| `EventualCompletion` | Committed transaction intents are all eventually resolved |
| `EventualCleanup` | Fully resolved txn records are eventually deleted |
| `EventualDecision` | No transaction stays in "preparing" or "predicatesChecked" forever |

## Key Design: Split OCC Predicate Check

The spec models the OCC predicate check as two separate steps with a window between them:

1. **`CheckPredicates(t)`** -- Snapshots committed key versions (the `:t` timestamp metadata)
2. **`WriteIntentOnShard(t, s)`** -- Validates both:
   - Committed version predicates still hold (versions haven't changed since snapshot)
   - No conflicting pending intents from other transactions (`hasConflictingIntentForKey`)

This split faithfully models the vulnerability surface that caused the OCC lost update bug (PR #381): between the predicate snapshot and intent write, another transaction can interleave. The `SerializableReads` invariant catches this -- without the intent conflict check, two transactions that snapshot the same version can both commit.

## Model Configuration

The small model in `MC.tla` uses:

- **2 transactions** (`t1`, `t2`) -- enough for OCC conflict scenarios
- **2 shards** (`s1`, `s2`) -- enough for multi-shard transactions
- **2 keys** (`k1`, `k2`) -- `k1` is the conflict key shared by both txns
- **MaxTimestamp = 4** -- bounds the HLC clock for finite state space

Transaction setup:
- `t1` writes `k1` on `s1` and `k2` on `s2` (multi-shard, coordinator `s1`)
- `t2` writes `k1` on `s1` (single-shard, coordinator `s1`)
- Both read `k1` (OCC conflict on `k1`)

## Mapping to Zig Implementation

| TLA+ Variable | Zig Code |
|---|---|
| `clock` | Timestamp parameter in `initTransactionWithParticipants` |
| `txnStatus` | Orchestrator state in `src/api/distributed_txn.zig` (`ParticipantWorker` vtable) |
| `txnRecords` | Transaction records managed by `src/storage/transactions.zig` (`TxnManager`) |
| `resolvedParts` | Participant tracking in `TxnManager.resolveIntents` |
| `intents` | Write intents in `src/storage/transactions.zig` (`WriteIntent` struct) |
| `dataStore` | LMDB key-value data, written during intent resolution |
| `predicateSnapshot` | `VersionPredicate` structs passed to `writeIntents` and `checkVersionPredicates` |

| TLA+ Action | Zig Code |
|---|---|
| `InitTransaction` | `storage/transactions.zig:initTransactionWithParticipants` |
| `CheckPredicates` | `storage/transactions.zig:checkVersionPredicates` |
| `WriteIntentOnShard` | `storage/transactions.zig:writeIntents` (success path) |
| `WriteIntentFails` | `storage/transactions.zig:writeIntents` (error: VersionConflict or IntentConflict) |
| `CommitTransaction` | `api/distributed_txn.zig:resolve_group` (status=committed) |
| `AbortTransaction` | `api/distributed_txn.zig:resolve_group` (status=aborted) |
| `ResolveIntentsOnShard` | `storage/transactions.zig:resolveIntents` |
| `RecoveryResolve` | `storage/db/maintenance/transaction_runtime.zig:runRecoveryWithConfig` |
| `CleanupTxnRecord` | `storage/db/maintenance/transaction_runtime.zig` (after cleanup) |

---

## Snapshot Transfer Protocol

### Running

**Safety only** (27M distinct states, ~90s):

```bash
"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC SnapshotTransferMC.tla \
    -config AntflySnapshotTransfer-safety -workers auto -deadlock
```

**Safety + liveness** (requires reduced constants — edit `SnapshotTransferMC.tla`):

Set `MCMaxRetries == 1` and `MCMaxSnapshots == 1`, then:

```bash
"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC SnapshotTransferMC.tla \
    -config AntflySnapshotTransfer.cfgs -workers auto -deadlock
```

> **Note**: Liveness checking with strong fairness (SF) is expensive.
> MaxRetries=1, MaxSnapshots=1 completes in ~25s.
> MaxRetries=2, MaxSnapshots=2 takes 30+ minutes.
> Safety checking scales well to the full model (3,3).

### What it Verifies

#### Safety Invariants (checked at every reachable state)

| Invariant | What it catches |
|---|---|
| `TypeOK` | All variables stay within their declared types |
| `AppliedSnapshotIsValid` | A node in "done" state has the snapshot in its local store |
| `GCSafety` | A node's persisted snapshot is always in its local store |
| `RetryBound` | No node exceeds MaxRetries |
| `NoFetchingWithoutNeed` | A fetching node is always in the needsSnap set |
| `SnapshotIDsMonotonic` | Snapshot IDs never exceed the global counter |

#### Liveness Properties (checked under strong fairness)

| Property | What it ensures |
|---|---|
| `EventualTransferResolution` | A fetching node eventually reaches idle or failed (no stuck transfers) |
| `EventualPermanentDetection` | If snapshot is GC'd everywhere, the node eventually stops fetching |

### Key Design Decisions

**Split persisted vs in-flight state**: The spec separates `persistedSnap` (survives crashes, loaded from Pebble) from `targetSnap` (in-memory, lost on crash). This distinction was discovered during model checking — an earlier version using a single `currentSnap` variable violated `GCSafety` when a node crashed mid-transfer.

**Strong fairness (SF)**: Transfer-related actions (`TransferSucceeds`, `TransferPermanentFailure`, `TransferRetry`) use SF instead of WF. Peer crashes make these actions intermittently enabled/disabled. WF only guarantees firing for *continuously* enabled actions, which is insufficient when peers crash and restart. SF reflects the real system's retry loop eventually hitting a window where the peer is available.

**RaftSendsSnapshot guard**: The spec requires `persistedSnap[leader] > persistedSnap[n]` — Raft only sends snapshots to followers that are actually behind. Without this, TLC found a scenario where Raft redundantly sends an already-applied snapshot, the recipient becomes leader and GCs it, violating `AppliedSnapshotIsValid`.

### Model Configuration

The model in `SnapshotTransferMC.tla` uses:

- **3 nodes** (`n1`, `n2`, `n3`) -- leader + 2 peers for transfer dynamics
- **MaxRetries = 3** (safety) / **1** (liveness) -- retry exhaustion
- **MaxSnapshots = 3** (safety) / **1** (liveness) -- GC scenarios

### Mapping to Zig Implementation

| TLA+ Variable | Zig Code |
|---|---|
| `leader` | Raft election in `../raft/src/core/raft.zig` |
| `persistedSnap` | Snapshot state in `pkg/antfly/src/raft/host.zig` |
| `targetSnap` | In-memory snapshot metadata during transfer |
| `snapStore` | Snapshot storage in `pkg/antfly/src/raft/storage/` |
| `transferState` | Snapshot fetch in `pkg/antfly/src/raft/transport/` |
| `retryCount` | Retry logic in snapshot transport |

| TLA+ Action | Zig Code |
|---|---|
| `CreateSnapshot` | Snapshot creation in `pkg/antfly/src/raft/managed_host.zig` |
| `RaftSendsSnapshot` | Raft MsgSnap handling in `pkg/antfly/src/raft/host.zig` |
| `TransferSucceeds` | Snapshot transport success |
| `TransferPermanentFailure` | Permanent failure detection in transport |
| `TransferRetry` | Retryable error in transport |
| `ApplySnapshot` | Snapshot application via state machine |

---

## LSM Lifecycle OOM Safety

### Running

```bash
make tla-check-lsm
```

or directly:

```bash
cd zig/specs/tla
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar tlc2.TLC \
    AntflyLsmLifecycle.tla \
    -config AntflyLsmLifecycle.cfgs -workers auto -deadlock
```

### What it Verifies

| Invariant | What it catches |
|---|---|
| `CacheActiveLeaseReachable` | A leased provisioned read/write cache entry must remain live or retired-reachable. |
| `CachePublishedEntryHasRetireCapacity` | Every published cache entry has a reserved cleanup slot before unlink-time retirement. |
| `SnapshotActiveReaderReachable` | An active LSM reader must have the mutable snapshot reachable through current or retired ownership. |
| `SnapshotPublishedHasRetireCapacity` | Every published mutable read snapshot has a reserved retired-snapshot cleanup slot. |
| `IndexFailedOpFreedTemps` | Failed `IndexWriter.removeSegments` operations free temporary `SegmentEntry` arrays. |

### Mapping to Zig Implementation

| TLA+ action | Zig code |
|---|---|
| `CacheOpenSucceeds` | `ProvisionedTableReadCache.getOrOpen`, `ProvisionedTableWriteCache.getOrOpenLockedMode`, `adoptPreparedOpenLocked` reserve `retired_entries` capacity before publishing `Entry`. |
| `CacheRetireActive` | Read cache `clear`, table invalidation, eviction; write cache `clear`, stale root pruning, table removal, write-source/status pruning. |
| `CacheReleaseRetiredLease` | `releaseEntry` calls `destroyRetiredEntryLocked` on final lease. |
| `BeginReadSnapshotSucceeds` | `Backend.snapshotMutableState` creates/clones state, reserves `retired_mutable_snapshots`, then publishes `mutable_read_snapshot`. |
| `InvalidateMutableSnapshotWithActiveReader` | `invalidateMutableReadSnapshot` from mutable rotation/flush and direct bulk-ingest finish. |
| `IndexRetiredAllocationFails` / `IndexRebuildFails` | `IndexWriter.removeSegments` OOM paths after `new_segments` and/or `retired` temporary allocation. |
