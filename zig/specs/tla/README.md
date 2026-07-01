# TLA+ Formal Specifications

## Modeling Maturity Plan

`MODELING_PLAN.md` tracks the work required to turn the current set of specs
into accurate lower-level models with validation at each step. It defines model
maturity criteria, coverage inventory requirements, negative-validation
expectations, and the phased deepening order. Start there before adding new
models or broadening existing ones.

`MODEL_COVERAGE.md` is the live coverage inventory that maps each model to code
anchors, Zig tests/traces, checked invariants, known gaps, and next validation
steps.

`TLA_CRITIQUE_REPAIR.md` tracks critique-specific repairs and the remaining
known weak spots that should block any claim of repository-wide modeling
maturity.

## Specs

### Transaction Protocol

Formal verification of the distributed 2PC + OCC + recovery + cleanup protocol.

- `AntflyTransaction.tla` -- Main specification (11 actions, 6 safety invariants, 3 liveness properties)
- `MC.tla` -- Model checking module with concrete constants for a small model
- `AntflyTransaction.cfg` -- TLC configuration
- `AntflyTransactionBadSkipIntentConflict.cfg` -- Expected-failure pending-intent conflict mutant used by `make tla-check-negative`
- `occ-2pc.tla` / `occ-2pc.cfg` -- Historical Piledriver spec that found the OCC lost update bug (PR #381)

### Shard Split Protocol

- `AntflyShardSplit.tla` -- Shard split lifecycle with delta replay, dual-actor cutover, child leader election, and non-atomic finalize (18 actions, 10 safety invariants, 1 liveness property)
- `ShardSplitMC.tla` -- Model checking module
- `AntflyShardSplit.cfg` -- TLC configuration
- `AntflyShardSplitBadPrematureChildDefault.cfg` -- Expected-failure child-default-before-cutover mutant used by `make tla-check-negative`

### Snapshot Transfer Protocol

Formal verification of the multi-raft snapshot creation, transfer, GC, and error classification.

- `AntflySnapshotTransfer.tla` -- Main specification (10 actions, 6 safety invariants, 2 liveness properties)
- `SnapshotTransferMC.tla` -- Model checking module (3 nodes, configurable retries/snapshots)
- `AntflySnapshotTransfer.cfg` -- Full TLC configuration (safety + liveness)
- `AntflySnapshotTransfer-safety.cfg` -- Safety-only configuration (fast, ~90s)
- `AntflySnapshotTransferBadApplyWithoutPut.cfg` -- Expected-failure transfer-done-without-local-archive mutant used by `make tla-check-negative`

### LSM Lifecycle OOM Safety

Formal verification of allocator-failure safety for Zig LSM cleanup ownership handoffs.

- `AntflyLsmLifecycle.tla` -- Provisioned read/write cache entry retirement, LSM mutable read snapshot retirement, and `IndexWriter.removeSegments` temporary allocation cleanup.
- `AntflyLsmLifecycle.cfg` -- TLC configuration for safety invariants.
- `AntflyLsmLifecycleBadIndexTempLeak.cfg` -- Expected-failure index temporary allocation leak mutant used by `make tla-check-negative`.

### Focused Lower-Level Models

These specs are intentionally separate bounded models. They cover implementation-level contracts that are too specific to fold into one monolithic system model, while keeping each model check independently runnable.

- `AntflyHAGates.tla` -- Exhaustive HA gate decision table for commit/read/write/owner-job/background-runtime behavior across role, fence, handoff, consistency, LSN, commit-mode, and failure-policy inputs.
- `AntflyHAGatesBadStandbyRuntime.cfg` -- Expected-failure standby mutating background runtime mutant used by `make tla-check-negative`.
- `AntflyHAGateTransitions.tla` -- Transition sibling for HA gate stale-decision safety across role and fence changes.
- `AntflyHAGateTransitionsBadStaleAllow.cfg` -- Expected-failure stale allow after role/fence transition mutant used by `make tla-check-negative`.
- `AntflyHAReplication.tla` -- Concrete HA replication slot progress, active/reseed/timeline eligibility, sync selection, sync wait target provenance, stale timeline ack rejection, fail-closed/degrade decisions, fencing receipts, standby promotion switch records, retained WAL floors, and former-primary rejoin.
- `AntflyHAReplicationBadStaleTimelineAck.cfg` -- Expected-failure stale timeline ack mutant used by `make tla-check-negative`.
- `AntflyHASyncWait.tla` -- Fast HA sync-wait submodel for frozen target timeline/LSN provenance, captured standby ack evidence, timeline promotion after freeze, and below-target/wrong-timeline ack rejection.
- `AntflyHASyncWaitBadMoveTarget.cfg` -- Expected-failure promotion-mutates-frozen-target mutant used by `make tla-check-negative`.
- `AntflyHASyncWaitBadWrongTimelineAck.cfg` -- Expected-failure wrong-timeline ack mutant used by `make tla-check-negative`.
- `AntflyHASyncWaitBadBelowTargetAck.cfg` -- Expected-failure below-target ack mutant used by `make tla-check-negative`.
- `AntflyHATimelineSwitch.tla` -- Fast HA timeline-switch boundary submodel for parent received/applied/safe progress, monotonic switch timeline/epoch, crash recovery from a durable switch record, and old-timeline rejection after switch.
- `AntflyHATimelineSwitchBadBeforeApplied.cfg` -- Expected-failure switch-before-parent-apply mutant used by `make tla-check-negative`.
- `AntflyHATimelineSwitchBadNonMonotonic.cfg` -- Expected-failure non-monotonic timeline/epoch switch mutant used by `make tla-check-negative`.
- `AntflyHATimelineSwitchBadOldTimeline.cfg` -- Expected-failure old-timeline record accepted after switch mutant used by `make tla-check-negative`.
- `AntflyHATimelineSwitchBadRecoveryPrevious.cfg` -- Expected-failure crash recovery with mismatched switch `previous_lsn` mutant used by `make tla-check-negative`.
- `AntflyHAStandbyApply.tla` -- Fast HA standby-apply submodel for durable receive, failed-apply progress, idempotent replay side effects, crash/reopen receive preservation, standby write rejection, and mutating-runtime suppression.
- `AntflyHAStandbyApplyBadFailureAdvances.cfg` -- Expected-failure apply-failure-advances-progress mutant used by `make tla-check-negative`.
- `AntflyHAStandbyApplyBadDuplicateEffect.cfg` -- Expected-failure duplicate replay side effect mutant used by `make tla-check-negative`.
- `AntflyHAStandbyApplyBadCrashLosesReceive.cfg` -- Expected-failure crash-loses-durable-receive mutant used by `make tla-check-negative`.
- `AntflyHAStandbyApplyBadClientWrite.cfg` -- Expected-failure standby client write mutant used by `make tla-check-negative`.
- `AntflyHAStandbyApplyBadBackgroundRuntime.cfg` -- Expected-failure standby mutating background runtime mutant used by `make tla-check-negative`.
- `AntflyHARejoin.tla` -- Fast HA former-primary rejoin submodel for fenced assessment, retained fork coverage, forced-promotion policy, stale assessment rejection, fork-record identity validation, rewind truncation, and reseed publication.
- `AntflyHARejoinBadUnfencedRewind.cfg` -- Expected-failure unfenced rewind mutant used by `make tla-check-negative`.
- `AntflyHARejoinBadExpiredWalRewind.cfg` -- Expected-failure expired-WAL rewind mutant used by `make tla-check-negative`.
- `AntflyHARejoinBadForcedRewind.cfg` -- Expected-failure forced-promotion rewind-without-policy mutant used by `make tla-check-negative`.
- `AntflyHARejoinBadIdentityMismatchRewind.cfg` -- Expected-failure identity/timeline mismatch rewind mutant used by `make tla-check-negative`.
- `AntflyHARejoinBadStaleAssessment.cfg` -- Expected-failure stale assessment truncate mutant used by `make tla-check-negative`.
- `AntflyHARejoinBadForkMismatch.cfg` -- Expected-failure fork-record mismatch truncate mutant used by `make tla-check-negative`.
- `AntflyHAFailoverSafety.tla` -- Focused HA failover safety model for acknowledged-write preservation, promotion fencing, and old-primary split-brain write suppression. Durability is commit-mode parameterized: sync-acked writes must survive promotion, async-acked writes may be lost by design. Its positive config also checks the `EventuallyPromoted` liveness property via `FairSpec`; `AntflyHARejoin`, `AntflyDerivedReplay`, and `AntflyEnrichmentLease` follow the same pattern (fair positive spec with no-permanent-stall properties, unfair `Spec` for mutants).
- `AntflyHAFailoverSafetyBadPromoteMissingAck.cfg` -- Expected-failure promoted standby missing an acknowledged write mutant used by `make tla-check-negative`.
- `AntflyHAFailoverSafetyBadOldPrimaryWrite.cfg` -- Expected-failure old-primary post-promotion write mutant used by `make tla-check-negative`.
- `AntflyHAPartitionFence.tla` -- Focused HA partition model for asynchronous fence delivery before promotion and old-primary write suppression after promotion.
- `AntflyHAPartitionFenceBadPromoteBeforeFence.cfg` -- Expected-failure promote-before-fence-delivery mutant used by `make tla-check-negative`.
- `AntflyBatcherCoalescing.tla` -- Batcher per-key coalescing order and flush visibility for delete/write sequences.
- `AntflyBatcherCoalescingBadDeleteWriteInversion.cfg` -- Expected-failure delete/write inversion mutant used by `make tla-check-negative`.
- `AntflyBatcherCoalescingBadWriteDeleteInversion.cfg` -- Expected-failure write/delete inversion mutant used by `make tla-check-negative`.
- `AntflyBatcherCoalescingBadPartialVisibility.cfg` -- Expected-failure partial flush visibility mutant used by `make tla-check-negative`.
- `AntflyCdcCutover.tla` -- CDC snapshot high-water, stream cutover, and checkpoint-delivery safety.
- `AntflyCdcCutoverBadBoundaryDuplicate.cfg` -- Expected-failure snapshot/stream boundary duplicate mutant used by `make tla-check-negative`.
- `AntflyCdcCutoverBadCheckpointAhead.cfg` -- Expected-failure checkpoint-ahead-of-delivery mutant used by `make tla-check-negative`.
- `AntflyCdcCutoverBadResumeReplay.cfg` -- Expected-failure crash/resume cursor replay mutant used by `make tla-check-negative`.
- `AntflyShardSplitSeq.tla` -- Sequence-level shard split delta safety for repeated writes to the same key.
- `AntflyShardSplitSeqBadKeySetCutover.cfg` -- Expected-failure key-set cutover mutant used by `make tla-check-negative`.
- `AntflySnapshotContent.tla` -- Snapshot content/index provenance model.
- `AntflySnapshotContentBadWrongContent.cfg` -- Expected-failure wrong-content-for-index mutant used by `make tla-check-negative`.
- `AntflySnapshotContentBadGcNeededContent.cfg` -- Expected-failure GC-of-needed-content mutant used by `make tla-check-negative`.
- `AntflyLsmReserveCleanup.tla` -- Explicit LSM reserve/fail/cleanup ownership model.
- `AntflyLsmReserveCleanupBadPublishWithoutReserve.cfg` -- Expected-failure publish-without-cleanup-reserve mutant used by `make tla-check-negative`.
- `AntflyLsmReserveCleanupBadFailureLeaksTemp.cfg` -- Expected-failure failure-leaks-temp mutant used by `make tla-check-negative`.
- `AntflyQueryCompleteness.tla` -- Split query routing completeness model for no missing or duplicate docs during route/serving transitions.
- `AntflyNodeDrainLifecycle.tla` -- Node drain/scale-down lifecycle: drain/store-flag raft-transaction consistency, finalize preconditions, safe_to_terminate debt gate, registration-preserves-drain, and drain-eventually-safe liveness.
- `AntflyTableLifecycle.tla` -- Table create/drop lifecycle: in-memory desired vs raft-committed topology, per-command applies, crash rebuilding desired from committed, planner scope, and convergence liveness.
- `AntflyHARetentionReseed.tla` -- WAL retention floor vs per-slot reseed marking vs truncation, plus backup slots as retention pins with fail-closed backup end; no-permanent-unmarked-lag liveness.
- `AntflyPromotionOwnerHandoff.tla` -- Entity promotion single-owner handoff across split/merge: detach-before-transfer-before-attach, non-durable attachment with crash/reattach, isLocalOwner promotion gate, handoff-completes liveness.
- `AntflyIndexLifecycle.tla` -- Index lifecycle (stale->building->fresh) with non-atomic durable status snapshots, shadow-swap completeness, watermark-validating crash recovery, and build-converges liveness.
- `AntflyQueryCompletenessBadRouteBeforeChildReady.cfg` -- Expected-failure route-before-child-ready mutant used by `make tla-check-negative`.
- `AntflyQueryCompletenessBadDoubleServe.cfg` -- Expected-failure parent/child double-serve mutant used by `make tla-check-negative`.
- `AntflyQueryCompletenessBadMissingDoc.cfg` -- Expected-failure child-serving-without-moved-doc mutant used by `make tla-check-negative`.
- `AntflyDerivedReplay.tla` -- Derived index replay-all rows, hint lane visibility, latest hint metadata, per-index catch-up targets, applied/query targets, bulk-session blocking, and replay truncation floors.
- `AntflyDerivedReplay-heavy-depth.cfg` -- Depth-heavy single-index derived replay bounds used by `make tla-check-derived-replay-heavy`.
- `AntflyDerivedReplay-heavy-multi-index.cfg` -- Multi-index derived replay bounds used by `make tla-check-derived-replay-heavy`.
- `AntflyDerivedReplay-heavy.cfg` -- Full MaxSeq=3/two-index manual confidence bounds used by `make tla-check-derived-replay-heavy-full`.
- `AntflyDerivedReplayBad.cfg` -- Expected-failure stale/empty hint-lane mutant used by `make tla-check-negative`.
- `AntflyEnrichmentLease.tla` -- Generated enrichment worker target/applied watermarks, replay visibility, retry/isolation state, and lease-owned collection/generation/publication so stale work cannot publish and hidden pending generated work cannot be skipped.
- `AntflyEnrichmentLeaseBadStalePublish.cfg` -- Expected-failure stale lease publication mutant used by `make tla-check-negative`.
- `AntflyEnrichmentLeaseBadEmptyPending.cfg` -- Expected-failure hidden pending advancement mutant used by `make tla-check-negative`.
- `AntflyLmdbCommit.tla` -- Zig LMDB prepared data pages, data-sync/meta-write/meta-sync publication phases, crash reopen meta selection, nested child transaction merge/abort, reader snapshots, and free-record reuse gated by oldest reader.
- `AntflyLmdbCommitBadMetaBeforeData.cfg` -- Expected-failure meta-before-data mutant used by `make tla-check-negative`.
- `AntflyLmdbCommitBadReaderReuse.cfg` -- Expected-failure reader-visible page reuse mutant used by `make tla-check-negative`.
- `AntflyLsmWalCompaction.tla` -- Segment-aware WAL append/sync/replay, crash truncation of unsynced tails, corrupt current-tail isolation, durable checkpointing, compaction publication, and reader-pinned segment retention.
- `AntflyLsmWalCompactionBadCheckpoint.cfg` -- Expected-failure unsynced checkpoint mutant used by `make tla-check-negative`.
- `AntflyLsmWalCompactionBadCorruptRotate.cfg` -- Expected-failure corrupt-tail rotation mutant used by `make tla-check-negative`.
- `AntflyLsmWalCompactionBadPinnedRetire.cfg` -- Expected-failure reader-pinned segment retirement mutant used by `make tla-check-negative`.
- `AntflyDbSplitVisibility.tla` -- DB split/merge visibility for right-range snapshot copy, parent split deltas, child replay, text/sparse/graph shadow index catch-up, child artifact placement, enrichment owner fencing, direct child writes, and merge receiver index routing.
- `AntflyDbSplitVisibilityBadParentWrite.cfg` -- Expected-failure post-cutover parent child-range write mutant used by `make tla-check-negative`.
- `AntflyDbSplitVisibilityBadChildServe.cfg` -- Expected-failure premature child serving before replay/index catch-up mutant used by `make tla-check-negative`.
- `AntflyDbSplitVisibilityBadMergeDonor.cfg` -- Expected-failure merge donor post-handoff serving mutant used by `make tla-check-negative`.
- `AntflyDbSplitVisibilityBadEnrichmentOwner.cfg` -- Expected-failure stale/non-owning enrichment publication mutant used by `make tla-check-negative`.
- `AntflySplitRefinementBridge.tla` -- Boundary model linking shard split fence/cutover readiness to DB-local replay/index readiness and metadata child routing.
- `AntflySplitRefinementBridgeBadRouteBeforeDbServing.cfg` -- Expected-failure metadata route-before-DB-serving mutant used by `make tla-check-negative`.
- `AntflySplitRefinementBridgeBadDbServeBeforeShardCutover.cfg` -- Expected-failure DB child-serving-before-shard-cutover mutant used by `make tla-check-negative`.
- `AntflySplitRefinementBridgeBadStaleFenceCutover.cfg` -- Expected-failure stale-fence split cutover mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentity.tla` -- Document identity namespace, stable ordinal ownership, generation visibility, resolved-doc-filter context, canonical namespace repair, and strict namespace-open behavior.
- `AntflyDocumentIdentityBadReuseOrdinal.cfg` -- Expected-failure tombstoned ordinal reuse mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityBadStaleFilter.cfg` -- Expected-failure stale resolved-doc-filter generation mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityBadNamespaceMismatch.cfg` -- Expected-failure strict namespace-open mismatch mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityRangeRepair.tla` -- Document identity split/merge namespace compatibility and restore/import/runtime-repair ordering across representative healthy, mixed-version, reassignment-active, conflict, rebuild-required, and ordinal-capacity states.
- `AntflyDocumentIdentityRangeRepairBadSplitUnhealthy.cfg` -- Expected-failure split validation accepts unhealthy source identity status mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityRangeRepairBadSplitDestNamespace.cfg` -- Expected-failure split destination reports the wrong identity namespace mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityRangeRepairBadMergeMismatch.cfg` -- Expected-failure merge accepts incompatible donor/receiver namespaces without opt-in mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityRangeRepairBadMergeActiveReassign.cfg` -- Expected-failure merge reassignment runs without opt-in or healthy status mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityRangeRepairBadRestoreNamespace.cfg` -- Expected-failure strict deferred restore accepts a mismatched doc identity namespace mutant used by `make tla-check-negative`.
- `AntflyDocumentIdentityRangeRepairBadRestoreEarlyClear.cfg` -- Expected-failure restore intent clears before import recovery and runtime repair complete mutant used by `make tla-check-negative`.
- `TraceAntflyDocumentIdentityRangeRepair.tla` -- Trace fixture validator for document identity range/restore repair sequences, including strict deferred restore namespace rejection and import recovery before runtime repair/intent clear.
- `TraceAntflyDocumentIdentityRangeRepair.cfg` -- Trace validation config used by `make tla-trace-doc-identity-range-repair`.
- `traces/doc_identity_restore_namespace_reject.ndjson` and `traces/doc_identity_restore_repair_order.ndjson` -- Checked-in positive restore repair fixtures.
- `traces/negative/doc_identity_restore_accept_mismatch.ndjson` and `traces/negative/doc_identity_restore_early_clear.ndjson` -- Expected-failure restore repair fixtures used by trace-negative targets.
- `AntflyTransactionSession.tla` -- Session savepoints over distributed transaction prepare/commit/abort/recovery/resolve/cleanup, with committed-base visibility separated from staged writes, crash-finalized orphan intent recovery, identity-row side effects, and participant cleanup gating.
- `AntflyTransactionSessionBadRollback.cfg` -- Expected-failure rollback leakage mutant used by `make tla-check-negative`.
- `AntflyTransactionSessionBadRecoveryDecision.cfg` -- Expected-failure aborted-orphan wrong recovery decision mutant used by `make tla-check-negative`.
- `AntflyTransactionSessionBadCleanup.cfg` -- Expected-failure unresolved participant cleanup mutant used by `make tla-check-negative`.
- `AntflyManagedHostLifecycle.tla` -- Managed raft host desired/hosted/active/routes reconciliation, durable apply stores, replica catalog persistence, restart recovery, backup-restore bootstrap prepare/success/failure, and restore cancellation.
- `AntflyManagedHostLifecycleBadPrematureRestore.cfg` -- Expected-failure restore activation before bootstrap completion mutant used by `make tla-check-negative`.
- `AntflyManagedHostLifecycleBadStaleRoute.cfg` -- Expected-failure stale route after metadata removal mutant used by `make tla-check-negative`.
- `AntflyManagedHostLifecycleBadReviveRemoved.cfg` -- Expected-failure removed replica catalog revival mutant used by `make tla-check-negative`.
- `AntflyManagedHostLifecycleBadRestoreCancel.cfg` -- Expected-failure uncancelled restore bootstrap mutant used by `make tla-check-negative`.
- `AntflyLitePublication.tla` -- Lite/serverless publication ordering for document/mutation/text/vector/sparse/graph artifacts, manifest references, HEAD advancement, crash-before-HEAD retry, reader generation pinning, failed publication discard, and cleanup retention.
- `AntflyLitePublicationBadManifestBeforeArtifacts.cfg` -- Expected-failure manifest-before-artifacts mutant used by `make tla-check-negative`.
- `AntflyLitePublicationBadFailedHead.cfg` -- Expected-failure failed-publication visible HEAD mutant used by `make tla-check-negative`.
- `AntflyLitePublicationBadPinnedCleanup.cfg` -- Expected-failure reader-pinned cleanup mutant used by `make tla-check-negative`.
- `AntflyLitePublicationBadMixedGeneration.cfg` -- Expected-failure mixed visible generation mutant used by `make tla-check-negative`.
- `AntflyMlGraphPasses.tla` -- ML graph const-fold/CSE/fuse/DCE pass publication, parameter/constant identity preservation, fused lower-closure export, external partition runtime inputs, fallback runtime gates, and failed-pass partial-output suppression.
- `AntflyMlGraphPassesBadDanglingCse.cfg` -- Expected-failure CSE stale remap/dangling edge mutant used by `make tla-check-negative`.
- `AntflyMlGraphPassesBadParameterDedup.cfg` -- Expected-failure parameter/constant identity collapse mutant used by `make tla-check-negative`.
- `AntflyMlGraphPassesBadMissingLowerClosure.cfg` -- Expected-failure fused partition export without primitive lower closure mutant used by `make tla-check-negative`.
- `AntflyMlGraphPassesBadFallbackRuntime.cfg` -- Expected-failure fallback partition runtime publication mutant used by `make tla-check-negative`.
- `AntflyMlGraphPassesBadPartialPublish.cfg` -- Expected-failure failed-pass partial output publication mutant used by `make tla-check-negative`.
- `AntflyMlGraphDagPasses.tla` -- Bounded arbitrary-DAG ML CSE/DCE remapping model for duplicate elimination, consumer/output/parameter remaps, reachable-node DCE, compact topological `id_map`, and final dangling-reference exclusion.
- `AntflyMlGraphDagPassesBadCseMissDuplicate.cfg` -- Expected-failure missed duplicate CSE mutant used by `make tla-check-negative`.
- `AntflyMlGraphDagPassesBadCseNoConsumerRemap.cfg` -- Expected-failure stale consumer/output remap mutant used by `make tla-check-negative`.
- `AntflyMlGraphDagPassesBadDceDropReachable.cfg` -- Expected-failure reachable node dropped by DCE mutant used by `make tla-check-negative`.
- `AntflyMlGraphDagPassesBadDceNonTopoMap.cfg` -- Expected-failure non-topological compact DCE map mutant used by `make tla-check-negative`.
- `AntflyMlCompilerPublication.tla` -- ML partition export, PJRT/native compiler artifact, semantic KV input/output selection, graph-version freshness, fallback gate, and runtime executor publication boundaries.
- `AntflyMlCompilerPublicationBadStaleCompile.cfg` -- Expected-failure stale graph/export compile publication mutant used by `make tla-check-negative`.
- `AntflyMlCompilerPublicationBadMissingInput.cfg` -- Expected-failure missing parameter/cache runtime input mutant used by `make tla-check-negative`.
- `AntflyMlCompilerPublicationBadOutputSelection.cfg` -- Expected-failure semantic KV side-output leak mutant used by `make tla-check-negative`.
- `AntflyMlCompilerPublicationBadFallbackPublish.cfg` -- Expected-failure fallback partition executor publication mutant used by `make tla-check-negative`.
- `AntflyMlCompilerPublicationBadPartialArtifact.cfg` -- Expected-failure partial compiler artifact visibility mutant used by `make tla-check-negative`.
- `AntflyOpenApiCodegen.tla` -- OpenAPI checked-generation publication across modular spec versions, joined/prefixed public specs, root `openapi.yaml`, generated package modes, import mappings, public/internal package boundaries, committed state, failed partial generation, and `generated-check` pass/fail state.
- `AntflyOpenApiCodegenBadStalePackage.cfg` -- Expected-failure stale generated package mutant used by `make tla-check-negative`.
- `AntflyOpenApiCodegenBadStaleRoot.cfg` -- Expected-failure stale root `openapi.yaml` mutant used by `make tla-check-negative`.
- `AntflyOpenApiCodegenBadInternalLeak.cfg` -- Expected-failure public client internal import mutant used by `make tla-check-negative`.
- `AntflyOpenApiCodegenBadPartialCommit.cfg` -- Expected-failure failed partial generation commit mutant used by `make tla-check-negative`.

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
- `TraceAntflyTransaction.cfg` -- TLC configuration (checks `TraceMatched` and 5 safety invariants)
- `../../scripts/tla-filter-txn-trace.py` -- Filters transaction traces for spec compatibility

### Transaction Session Trace Fixture Validation

Validates checked-in transaction/session fixtures against `AntflyTransactionSession.tla`. These fixtures mirror storage transaction recovery and public session savepoint scenarios, but they are not currently emitted live by Zig tests. Each event can assert post-action state fields so replay checks visible document count, identity-row count, staged intent count, savepoint state, participant resolution, and cleanup state.

- `TraceAntflyTransactionSession.tla` -- Trace refinement spec for `txn-session-trace` NDJSON events (`BeginSession`, `StageWrite`, `CreateSavepoint`, `RollbackToSavepoint`, `PrepareParticipant`, `Commit`, `Abort`, `MarkStalePending`, `RecoverStalePending`, `CrashFinalizeCommittedOrphan`, `CrashFinalizeAbortedOrphan`, `RecoverFinalizedIntents`, `ResolveParticipant`, `Cleanup`)
- `TraceAntflyTransactionSession.cfg` -- TLC configuration (checks `TraceMatched` and session trace safety)
- `traces/txn_session_savepoint.ndjson` -- Savepoint rollback and commit fixture
- `traces/txn_session_orphan_recovery.ndjson` -- Committed and aborted finalized-orphan recovery fixture
- `traces/txn_session_stale_pending.ndjson` -- Stale pending auto-abort fixture
- `traces/negative/txn_session_bad_cleanup.ndjson` -- Expected-failure premature cleanup fixture

### HA Trace Fixture Validation

Validates checked-in HA trace fixtures against the focused HA contracts for standby receive/apply, sync-wait acknowledgements, timeline switch boundaries, and former-primary rejoin/reseed. These fixtures mirror existing HA chaos/rejoin scenarios, but they are not currently emitted live by the Zig HA tests.

- `TraceAntflyHA.tla` -- Trace refinement spec for `ha-trace` NDJSON events (`PrimaryAppend`, `StandbyReceive`, `StandbyApplySuccess`, `StandbyApplyFailure`, `FreezeSyncWait`, `StatusAck`, `TimelineSwitch`, `RejectOldTimeline`, `AssessRejoin`, `LateFormerPrimaryWrite`, `ExecuteRewind`, `ExecuteReseed`)
- `TraceAntflyHA.cfg` -- TLC configuration (checks `TraceMatched` and HA trace safety invariants)
- `traces/ha_sync_apply.ndjson` -- Sync/apply fixture covering durable receive, failed apply, retry, idempotence, and frozen-target acknowledgement
- `traces/ha_timeline_switch.ndjson` -- Timeline-switch fixture covering parent progress, switch record, status acknowledgement, and old-timeline rejection
- `traces/ha_rejoin.ndjson` -- Rejoin fixture covering assessment, late write, rewind, and reseed publication paths

### Split Bridge Trace Fixture Validation

Validates checked-in split bridge fixtures against `AntflySplitRefinementBridge.tla`. These fixtures mirror representative shard fence/cutover, DB replay/index catch-up, metadata routing, and rollback orderings, but they are not currently emitted live by Zig tests.

- `TraceAntflySplitRefinementBridge.tla` -- Trace fixture spec for `split-bridge-trace` NDJSON events (`BeginSplit`, `ParentRightWrite`, `ReplayDelta`, `BuildTextIndex`, `BuildSparseIndex`, `BuildGraphIndex`, `SetShardFence`, `CompleteShardCutover`, `PublishDbChildServing`, `RouteMetadataToChild`, `Rollback`)
- `TraceAntflySplitRefinementBridge.cfg` -- TLC configuration (checks `TraceMatched` and bridge safety invariants)
- `traces/split_bridge_cutover.ndjson` -- Positive cutover fixture covering DB catch-up before serving and metadata routing
- `traces/split_bridge_rollback.ndjson` -- Positive rollback fixture covering no child exposure after rollback
- `traces/negative/split_bridge_route_before_db_serving.ndjson` -- Expected-failure fixture where metadata routes to the child before DB serving is published

## Makefile Targets

From `zig/`:

```bash
make tla-tools                  # Download tla2tools.jar + CommunityModules (one-time)
make tla-check                  # Model check all specs (txn, split, snapshot, lsm)
make tla-check-txn              # Model check transaction spec
make tla-check-split            # Model check shard split spec
make tla-check-snap             # Model check snapshot transfer spec (safety only, ~90s)
make tla-check-lsm              # Model check LSM lifecycle OOM safety
make tla-check-new-fast         # Model check fast focused lower-level specs
make tla-check-new-heavy        # Model check heavier focused specs
make tla-check-new              # Model check all newly-added focused specs
make tla-check-critical         # Run critique-repair critical safety subset
make tla-check-negative         # Run expected-failure mutant checks
make tla-check-txn-negative-skip-intent-conflict
make tla-check-split-negative-premature-child-default
make tla-check-snap-negative-apply-without-put
make tla-check-lsm-negative-index-temp-leak
make tla-check-ml-graph-dag-passes
make tla-check-ml-graph-dag-passes-negative-cse-miss-duplicate
make tla-check-ml-graph-dag-passes-negative-cse-no-consumer-remap
make tla-check-ml-graph-dag-passes-negative-dce-drop-reachable
make tla-check-ml-graph-dag-passes-negative-dce-non-topo-map
make tla-check-ha-replication-negative-stale-timeline-ack
make tla-check-ha-gates-negative-standby-runtime
make tla-check-ha-sync-wait-negative-move-target
make tla-check-ha-sync-wait-negative-wrong-timeline-ack
make tla-check-ha-sync-wait-negative-below-target-ack
make tla-check-ha-timeline-switch-negative-before-applied
make tla-check-ha-timeline-switch-negative-non-monotonic
make tla-check-ha-timeline-switch-negative-old-timeline
make tla-check-ha-timeline-switch-negative-recovery-previous
make tla-check-ha-standby-apply-negative-failure-advances
make tla-check-ha-standby-apply-negative-duplicate-effect
make tla-check-ha-standby-apply-negative-crash-loses-receive
make tla-check-ha-standby-apply-negative-client-write
make tla-check-ha-standby-apply-negative-background-runtime
make tla-check-ha-rejoin-negative-unfenced-rewind
make tla-check-ha-rejoin-negative-expired-wal-rewind
make tla-check-ha-rejoin-negative-forced-rewind
make tla-check-ha-rejoin-negative-identity-mismatch-rewind
make tla-check-ha-rejoin-negative-stale-assessment
make tla-check-ha-rejoin-negative-fork-mismatch
make tla-check-ha-failover-safety
make tla-check-ha-failover-negative-promote-missing-ack
make tla-check-ha-failover-negative-old-primary-write
make tla-check-ha-failover-negative-ack-without-receipt
make tla-check-ha-partition-fence
make tla-check-ha-partition-fence-negative-promote-before-fence
make tla-check-ha-partition-fence-negative-old-primary-write-after-promotion
make tla-check-ha-gate-transitions
make tla-check-ha-gate-transitions-negative-stale-allow
make tla-check-batcher-coalescing
make tla-check-batcher-coalescing-negative-delete-write-inversion
make tla-check-batcher-coalescing-negative-write-delete-inversion
make tla-check-batcher-coalescing-negative-partial-visibility
make tla-check-batcher-coalescing-negative-delete-visible-without-delete
make tla-check-cdc-cutover
make tla-check-cdc-cutover-negative-boundary-duplicate
make tla-check-cdc-cutover-negative-checkpoint-ahead
make tla-check-cdc-cutover-negative-resume-replay
make tla-check-cdc-cutover-negative-stream-before-snapshot-complete
make tla-check-shard-split-seq
make tla-check-shard-split-seq-negative-keyset-cutover
make tla-check-shard-split-seq-negative-fenced-write-dropped
make tla-check-snapshot-content
make tla-check-snapshot-content-negative-wrong-content
make tla-check-snapshot-content-negative-gc-needed-content
make tla-check-lsm-reserve-cleanup
make tla-check-lsm-reserve-cleanup-negative-publish-without-reserve
make tla-check-lsm-reserve-cleanup-negative-failure-leaks-temp
make tla-check-query-completeness
make tla-check-query-completeness-negative-route-before-child-ready
make tla-check-query-completeness-negative-double-serve
make tla-check-query-completeness-negative-missing-doc
make tla-check-node-drain-lifecycle
make tla-check-node-drain-negative-finalize-active
make tla-check-node-drain-negative-registration-clears-drain
make tla-check-node-drain-negative-safe-ignores-debt
make tla-check-table-lifecycle
make tla-check-table-lifecycle-negative-range-without-table
make tla-check-table-lifecycle-negative-intent-undesired-range
make tla-check-ha-retention-reseed
make tla-check-ha-retention-negative-truncate-unmarked
make tla-check-ha-retention-negative-backup-ignores-lost-wal
make tla-check-promotion-owner-handoff
make tla-check-promotion-handoff-negative-attach-before-detach
make tla-check-promotion-handoff-negative-attach-before-transfer
make tla-check-promotion-handoff-negative-promote-unowned
make tla-check-index-lifecycle
make tla-check-index-lifecycle-negative-swap-incomplete
make tla-check-index-lifecycle-negative-recover-trusts-status
make tla-check-enrichment-lease-negative-stale-publish
make tla-check-enrichment-lease-negative-empty-pending
make tla-check-lsm-wal-compaction-negative-checkpoint
make tla-check-lsm-wal-compaction-negative-corrupt-rotate
make tla-check-lsm-wal-compaction-negative-pinned-retire
make tla-check-lmdb-commit-negative-meta-before-data
make tla-check-lmdb-commit-negative-reader-reuse
make tla-check-transaction-session-negative-rollback
make tla-check-transaction-session-negative-recovery-decision
make tla-check-transaction-session-negative-cleanup
make tla-check-document-identity-negative-reuse-ordinal
make tla-check-document-identity-negative-stale-filter
make tla-check-document-identity-negative-namespace-mismatch
make tla-check-document-identity-range-repair
make tla-check-document-identity-range-repair-negative-split-unhealthy
make tla-check-document-identity-range-repair-negative-split-dest-namespace
make tla-check-document-identity-range-repair-negative-merge-mismatch
make tla-check-document-identity-range-repair-negative-merge-active-reassign
make tla-check-document-identity-range-repair-negative-restore-namespace
make tla-check-document-identity-range-repair-negative-restore-early-clear
make tla-check-document-identity-range-repair-trace-negative-restore-mismatch
make tla-check-document-identity-range-repair-trace-negative-restore-early-clear
make tla-check-db-split-visibility-negative-parent-write
make tla-check-db-split-visibility-negative-child-serve
make tla-check-db-split-visibility-negative-merge-donor
make tla-check-db-split-visibility-negative-enrichment-owner
make tla-check-split-refinement-bridge
make tla-check-split-refinement-bridge-negative-route-before-db-serving
make tla-check-split-refinement-bridge-negative-db-serve-before-shard-cutover
make tla-check-split-refinement-bridge-negative-stale-fence-cutover
make tla-check-split-refinement-bridge-trace-negative-route-before-db-serving
make tla-check-lite-publication-negative-manifest-before-artifacts
make tla-check-lite-publication-negative-failed-head
make tla-check-lite-publication-negative-pinned-cleanup
make tla-check-lite-publication-negative-mixed-generation
make tla-check-openapi-codegen-negative-stale-package
make tla-check-openapi-codegen-negative-stale-root
make tla-check-openapi-codegen-negative-internal-leak
make tla-check-openapi-codegen-negative-partial-commit
make tla-check-ha-sync-wait     # Model check HA sync wait target provenance independently
make tla-check-ha-timeline-switch # Model check HA timeline switch boundary independently
make tla-check-ha-standby-apply # Model check HA standby apply and replay suppression independently
make tla-check-ha-rejoin       # Model check HA former-primary rejoin independently
make tla-check-ha-replication   # Model check concrete HA replication independently
make tla-check-ha-failover-safety # Model check HA acknowledged-write failover safety independently
make tla-check-derived-replay-heavy # Run split larger derived replay bounds
make tla-check-derived-replay-heavy-full # Run full MaxSeq=3/two-index confidence bounds
make tla-list                   # List model-checking and trace-validation targets
make tla-clean                  # Remove TLC runtime artifacts and generated traces

# Live trace validation (requires building with -Dwith_tla=true first)
make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson
make tla-trace-txn  TRACE_FILES=/tmp/txn-trace.ndjson

# Checked-in fixture trace validation
make tla-trace-txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson" # Checked-in fixtures; no Zig trace build required yet
make tla-trace-ha   TRACE_FILES="specs/tla/traces/ha_*.ndjson" # Checked-in fixtures; no Zig trace build required yet
make tla-trace-split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson" # Checked-in fixtures; no Zig trace build required yet
make tla-check-trace TRACE=split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson"
make tla-trace-doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson" # Checked-in fixtures; no Zig trace build required yet
make tla-check-trace TRACE=doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"
```

## Document Identity Range Repair Trace Fixture Validation

Checked-in fixtures can validate the restore repair boundary without invoking
the broad DB test suite:

```bash
make tla-trace-doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"
make tla-check-trace TRACE=doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"
make tla-check-document-identity-range-repair-trace-negative-restore-mismatch
make tla-check-document-identity-range-repair-trace-negative-restore-early-clear
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
    -config AntflyTransaction.cfg -workers auto -deadlock
```

### Using system Java + standalone jar

```bash
cd specs/tla
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar tlc2.TLC MC.tla \
    -config AntflyTransaction.cfg -workers auto -deadlock
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
    -config AntflySnapshotTransfer-safety.cfg -workers auto -deadlock
```

**Safety + liveness** (requires reduced constants — edit `SnapshotTransferMC.tla`):

Set `MCMaxRetries == 1` and `MCMaxSnapshots == 1`, then:

```bash
"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC SnapshotTransferMC.tla \
    -config AntflySnapshotTransfer.cfg -workers auto -deadlock
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
    -config AntflyLsmLifecycle.cfg -workers auto -deadlock
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
