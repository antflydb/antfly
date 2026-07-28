<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# TLA+ structural diagrams

Auto-generated overviews of each protocol spec: phase state machines, action/state tables, and write graphs. Expected-failure mutant actions (`Buggy*`-gated) are omitted. See the spec headers and `INVENTORY.md` for the authoritative protocol descriptions.

## metadata

- [AntflyCdcCutover](metadata/AntflyCdcCutover.md) — 5 actions, 5 variables, 1 phase state machine
- [AntflyManagedHostLifecycle](metadata/AntflyManagedHostLifecycle.md) — 9 actions, 14 variables, 4 phase state machines
- [AntflyNodeDrainLifecycle](metadata/AntflyNodeDrainLifecycle.md) — 9 actions, 8 variables, 1 phase state machine
- [AntflyPlacementReadiness](metadata/AntflyPlacementReadiness.md) — 7 actions, 13 variables, 0 phase state machines
- [AntflyPlacementRepair](metadata/AntflyPlacementRepair.md) — 10 actions, 15 variables, 1 phase state machine
- [AntflyQueryCompleteness](metadata/AntflyQueryCompleteness.md) — 5 actions, 7 variables, 0 phase state machines
- [AntflyRuntimeStatusReconciliation](metadata/AntflyRuntimeStatusReconciliation.md) — 10 actions, 26 variables, 0 phase state machines
- [AntflyShardSplit](metadata/AntflyShardSplit.md) — 20 actions, 14 variables, 3 phase state machines
- [AntflyShardSplitSeq](metadata/AntflyShardSplitSeq.md) — 5 actions, 5 variables, 0 phase state machines
- [AntflySplitRefinementBridge](metadata/AntflySplitRefinementBridge.md) — 13 actions, 15 variables, 2 phase state machines
- [AntflyTableAdmission](metadata/AntflyTableAdmission.md) — 5 actions, 5 variables, 1 phase state machine
- [AntflyTableLifecycle](metadata/AntflyTableLifecycle.md) — 11 actions, 8 variables, 0 phase state machines

## ml

- [AntflyMlCompilerPublication](ml/AntflyMlCompilerPublication.md) — 6 actions, 16 variables, 0 phase state machines
- [AntflyMlGraphDagPasses](ml/AntflyMlGraphDagPasses.md) — 2 actions, 14 variables, 2 phase state machines
- [AntflyMlGraphPasses](ml/AntflyMlGraphPasses.md) — 9 actions, 18 variables, 0 phase state machines

## raft

- [AntflyRaftReadyPipeline](raft/AntflyRaftReadyPipeline.md) — 11 actions, 19 variables, 1 phase state machine
- [AntflyRaftSchedulerFairness](raft/AntflyRaftSchedulerFairness.md) — 2 actions, 4 variables, 0 phase state machines
- [AntflySnapshotContent](raft/AntflySnapshotContent.md) — 4 actions, 7 variables, 0 phase state machines
- [AntflySnapshotTransfer](raft/AntflySnapshotTransfer.md) — 10 actions, 9 variables, 1 phase state machine

## storage/db

- [AntflyBatcherCoalescing](storage/db/AntflyBatcherCoalescing.md) — 3 actions, 5 variables, 2 phase state machines
- [AntflyDbSplitVisibility](storage/db/AntflyDbSplitVisibility.md) — 15 actions, 30 variables, 4 phase state machines
- [AntflyDerivedReplay](storage/db/AntflyDerivedReplay.md) — 13 actions, 12 variables, 0 phase state machines
- [AntflyDocumentIdentity](storage/db/AntflyDocumentIdentity.md) — 8 actions, 16 variables, 0 phase state machines
- [AntflyDocumentIdentityRangeRepair](storage/db/AntflyDocumentIdentityRangeRepair.md) — 11 actions, 23 variables, 5 phase state machines
- [AntflyEnrichmentLease](storage/db/AntflyEnrichmentLease.md) — 13 actions, 20 variables, 1 phase state machine
- [AntflyIndexLifecycle](storage/db/AntflyIndexLifecycle.md) — 11 actions, 11 variables, 2 phase state machines
- [AntflyLeaseRetryBackoff](storage/db/AntflyLeaseRetryBackoff.md) — 4 actions, 5 variables, 0 phase state machines
- [AntflyPromotionOwnerHandoff](storage/db/AntflyPromotionOwnerHandoff.md) — 6 actions, 5 variables, 1 phase state machine
- [AntflyReplayEnrichmentBridge](storage/db/AntflyReplayEnrichmentBridge.md) — 10 actions, 10 variables, 0 phase state machines
- [AntflyTransaction](storage/db/AntflyTransaction.md) — 15 actions, 9 variables, 3 phase state machines
- [AntflyTransactionSession](storage/db/AntflyTransactionSession.md) — 15 actions, 13 variables, 1 phase state machine

## storage/ha

- [AntflyHAFailoverSafety](storage/ha/AntflyHAFailoverSafety.md) — 6 actions, 14 variables, 0 phase state machines
- [AntflyHAGateTransitions](storage/ha/AntflyHAGateTransitions.md) — 5 actions, 4 variables, 1 phase state machine
- [AntflyHAGates](storage/ha/AntflyHAGates.md) — 0 actions, 16 variables, 9 phase state machines
- [AntflyHAPartitionFence](storage/ha/AntflyHAPartitionFence.md) — 7 actions, 9 variables, 0 phase state machines
- [AntflyHARejoin](storage/ha/AntflyHARejoin.md) — 4 actions, 22 variables, 2 phase state machines
- [AntflyHAReplication](storage/ha/AntflyHAReplication.md) — 3 actions, 41 variables, 5 phase state machines
- [AntflyHARetentionReseed](storage/ha/AntflyHARetentionReseed.md) — 8 actions, 8 variables, 0 phase state machines
- [AntflyHAStandbyApply](storage/ha/AntflyHAStandbyApply.md) — 5 actions, 11 variables, 0 phase state machines
- [AntflyHASyncWait](storage/ha/AntflyHASyncWait.md) — 6 actions, 15 variables, 0 phase state machines
- [AntflyHATimelineSwitch](storage/ha/AntflyHATimelineSwitch.md) — 7 actions, 15 variables, 0 phase state machines

## storage/lite

- [AntflyLitePublication](storage/lite/AntflyLitePublication.md) — 11 actions, 11 variables, 1 phase state machine

## storage/lmdb

- [AntflyLmdbCommit](storage/lmdb/AntflyLmdbCommit.md) — 15 actions, 18 variables, 0 phase state machines

## storage/lsm

- [AntflyLsmLifecycle](storage/lsm/AntflyLsmLifecycle.md) — 19 actions, 8 variables, 3 phase state machines
- [AntflyLsmReserveCleanup](storage/lsm/AntflyLsmReserveCleanup.md) — 7 actions, 6 variables, 0 phase state machines
- [AntflyLsmWalCompaction](storage/lsm/AntflyLsmWalCompaction.md) — 12 actions, 15 variables, 0 phase state machines

## vendored / legacy (root)

- [etcdraft](etcdraft.md) — 13 actions, 14 variables, 0 phase state machines
- [occ-2pc](occ-2pc.md) — 13 actions, 7 variables, 1 phase state machine
