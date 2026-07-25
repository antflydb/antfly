<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# TLA+ structural diagrams

Auto-generated overviews of each protocol spec: phase state machines, action/state tables, and write graphs. Expected-failure mutant actions (`Buggy*`-gated) are omitted. See the spec headers and `INVENTORY.md` for the authoritative protocol descriptions.

## (root)

- [etcdraft](etcdraft.md) — 13 actions, 14 variables, 0 phase state machine(s)
- [model](occ-2pc.md) — 13 actions, 7 variables, 1 phase state machine(s)

## metadata

- [AntflyCdcCutover](metadata/AntflyCdcCutover.md) — 5 actions, 5 variables, 1 phase state machine(s)
- [AntflyManagedHostLifecycle](metadata/AntflyManagedHostLifecycle.md) — 9 actions, 14 variables, 4 phase state machine(s)
- [AntflyNodeDrainLifecycle](metadata/AntflyNodeDrainLifecycle.md) — 9 actions, 8 variables, 1 phase state machine(s)
- [AntflyPlacementReadiness](metadata/AntflyPlacementReadiness.md) — 7 actions, 13 variables, 0 phase state machine(s)
- [AntflyPlacementRepair](metadata/AntflyPlacementRepair.md) — 10 actions, 15 variables, 1 phase state machine(s)
- [AntflyQueryCompleteness](metadata/AntflyQueryCompleteness.md) — 5 actions, 7 variables, 0 phase state machine(s)
- [AntflyRuntimeStatusReconciliation](metadata/AntflyRuntimeStatusReconciliation.md) — 10 actions, 26 variables, 0 phase state machine(s)
- [AntflyShardSplit](metadata/AntflyShardSplit.md) — 20 actions, 14 variables, 3 phase state machine(s)
- [AntflyShardSplitSeq](metadata/AntflyShardSplitSeq.md) — 5 actions, 5 variables, 0 phase state machine(s)
- [AntflySplitRefinementBridge](metadata/AntflySplitRefinementBridge.md) — 13 actions, 15 variables, 2 phase state machine(s)
- [AntflyTableLifecycle](metadata/AntflyTableLifecycle.md) — 11 actions, 8 variables, 0 phase state machine(s)

## ml

- [AntflyMlCompilerPublication](ml/AntflyMlCompilerPublication.md) — 6 actions, 16 variables, 0 phase state machine(s)
- [AntflyMlGraphDagPasses](ml/AntflyMlGraphDagPasses.md) — 2 actions, 14 variables, 2 phase state machine(s)
- [AntflyMlGraphPasses](ml/AntflyMlGraphPasses.md) — 9 actions, 18 variables, 0 phase state machine(s)

## raft

- [AntflyRaftReadyPipeline](raft/AntflyRaftReadyPipeline.md) — 11 actions, 19 variables, 1 phase state machine(s)
- [AntflyRaftSchedulerFairness](raft/AntflyRaftSchedulerFairness.md) — 2 actions, 4 variables, 0 phase state machine(s)
- [AntflySnapshotContent](raft/AntflySnapshotContent.md) — 4 actions, 7 variables, 0 phase state machine(s)
- [AntflySnapshotTransfer](raft/AntflySnapshotTransfer.md) — 10 actions, 9 variables, 1 phase state machine(s)

## storage/db

- [AntflyBatcherCoalescing](storage/db/AntflyBatcherCoalescing.md) — 3 actions, 5 variables, 2 phase state machine(s)
- [AntflyDbSplitVisibility](storage/db/AntflyDbSplitVisibility.md) — 15 actions, 30 variables, 4 phase state machine(s)
- [AntflyDerivedReplay](storage/db/AntflyDerivedReplay.md) — 12 actions, 12 variables, 0 phase state machine(s)
- [AntflyDocumentIdentity](storage/db/AntflyDocumentIdentity.md) — 8 actions, 16 variables, 0 phase state machine(s)
- [AntflyDocumentIdentityRangeRepair](storage/db/AntflyDocumentIdentityRangeRepair.md) — 11 actions, 23 variables, 5 phase state machine(s)
- [AntflyEnrichmentLease](storage/db/AntflyEnrichmentLease.md) — 13 actions, 20 variables, 1 phase state machine(s)
- [AntflyIndexLifecycle](storage/db/AntflyIndexLifecycle.md) — 11 actions, 11 variables, 2 phase state machine(s)
- [AntflyPromotionOwnerHandoff](storage/db/AntflyPromotionOwnerHandoff.md) — 6 actions, 5 variables, 1 phase state machine(s)
- [AntflyTransaction](storage/db/AntflyTransaction.md) — 15 actions, 9 variables, 3 phase state machine(s)
- [AntflyTransactionSession](storage/db/AntflyTransactionSession.md) — 15 actions, 13 variables, 1 phase state machine(s)

## storage/ha

- [AntflyHAFailoverSafety](storage/ha/AntflyHAFailoverSafety.md) — 6 actions, 14 variables, 0 phase state machine(s)
- [AntflyHAGateTransitions](storage/ha/AntflyHAGateTransitions.md) — 5 actions, 4 variables, 1 phase state machine(s)
- [AntflyHAGates](storage/ha/AntflyHAGates.md) — 0 actions, 16 variables, 9 phase state machine(s)
- [AntflyHAPartitionFence](storage/ha/AntflyHAPartitionFence.md) — 7 actions, 9 variables, 0 phase state machine(s)
- [AntflyHARejoin](storage/ha/AntflyHARejoin.md) — 4 actions, 22 variables, 2 phase state machine(s)
- [AntflyHAReplication](storage/ha/AntflyHAReplication.md) — 3 actions, 41 variables, 5 phase state machine(s)
- [AntflyHARetentionReseed](storage/ha/AntflyHARetentionReseed.md) — 8 actions, 8 variables, 0 phase state machine(s)
- [AntflyHAStandbyApply](storage/ha/AntflyHAStandbyApply.md) — 5 actions, 11 variables, 0 phase state machine(s)
- [AntflyHASyncWait](storage/ha/AntflyHASyncWait.md) — 6 actions, 15 variables, 0 phase state machine(s)
- [AntflyHATimelineSwitch](storage/ha/AntflyHATimelineSwitch.md) — 7 actions, 15 variables, 0 phase state machine(s)

## storage/lite

- [AntflyLitePublication](storage/lite/AntflyLitePublication.md) — 11 actions, 11 variables, 1 phase state machine(s)

## storage/lmdb

- [AntflyLmdbCommit](storage/lmdb/AntflyLmdbCommit.md) — 15 actions, 18 variables, 0 phase state machine(s)

## storage/lsm

- [AntflyLsmLifecycle](storage/lsm/AntflyLsmLifecycle.md) — 19 actions, 8 variables, 3 phase state machine(s)
- [AntflyLsmReserveCleanup](storage/lsm/AntflyLsmReserveCleanup.md) — 7 actions, 6 variables, 0 phase state machine(s)
- [AntflyLsmWalCompaction](storage/lsm/AntflyLsmWalCompaction.md) — 12 actions, 15 variables, 0 phase state machine(s)
