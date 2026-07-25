\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     https://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.

---------------------- MODULE AntflySplitRefinementBridge ----------------------
(*
  Cross-boundary refinement bridge between the shard-level split lifecycle and
  DB-local split visibility model.

  This model intentionally stays smaller than both source models. It tracks only
  the facts that must agree across layers:

    - shard finalization/cutover readiness,
    - destination stable-placement readiness and bootstrap admission,
    - DB split-delta replay and shadow-index readiness,
    - DB child serving state and parent write fencing,
    - metadata right-range routing.

  Code/test anchors:
    - specs/tla/AntflyShardSplit.tla
    - specs/tla/AntflyDbSplitVisibility.tla
    - pkg/antfly/src/storage/db/db.zig split replay/finalization tests
    - pkg/antfly/src/metadata/transition_driver.zig split phase transitions
    - pkg/antfly/src/metadata/reconciler.zig split/rollback decisions

  Boundary:
    - No byte ranges or concrete keys; "right" is the split-off range.
    - No exact archive bytes or shadow-index payloads.
    - The model is about cross-layer gating, not complete split mechanics.
    - AntflyPlacementReadiness owns the detailed voter-report aggregation;
      this bridge consumes only its StablePlacementReady result.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyRouteBeforeDbServing,
    BuggyDbServeBeforeShardCutover,
    BuggyCompleteWithStaleFence,
    BuggyBootstrapWithoutStablePlacement

MaxSeq == 2
Phases == {"single", "splitting", "cutover", "children", "rolledBack"}
Owners == {"parent", "child"}

VARIABLES
    phase,
    shardFenceSet,
    shardFenceSeq,
    shardCutoverReady,
    dbDeltaSeq,
    dbReplaySeq,
    dbTextIndexSeq,
    dbSparseIndexSeq,
    dbGraphIndexSeq,
    dbChildServing,
    parentAcceptsRight,
    childAcceptsRight,
    routeRightOwner,
    staleFenceCompletion,
    placementBridge

vars == <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
          dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
          dbGraphIndexSeq, dbChildServing, parentAcceptsRight,
          childAcceptsRight, routeRightOwner, staleFenceCompletion,
          placementBridge>>

CoreVars ==
    <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
      dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
      dbGraphIndexSeq, dbChildServing, parentAcceptsRight,
      childAcceptsRight, routeRightOwner, staleFenceCompletion>>

DbReplayCaughtUp == dbReplaySeq = dbDeltaSeq

DbIndexesComplete ==
    /\ dbTextIndexSeq = dbReplaySeq
    /\ dbSparseIndexSeq = dbReplaySeq
    /\ dbGraphIndexSeq = dbReplaySeq

DbReadyToServe == DbReplayCaughtUp /\ DbIndexesComplete

Init ==
    /\ phase = "single"
    /\ shardFenceSet = FALSE
    /\ shardFenceSeq = 0
    /\ shardCutoverReady = FALSE
    /\ dbDeltaSeq = 0
    /\ dbReplaySeq = 0
    /\ dbTextIndexSeq = 0
    /\ dbSparseIndexSeq = 0
    /\ dbGraphIndexSeq = 0
    /\ dbChildServing = FALSE
    /\ parentAcceptsRight = TRUE
    /\ childAcceptsRight = FALSE
    /\ routeRightOwner = "parent"
    /\ staleFenceCompletion = FALSE
    /\ placementBridge =
        [stable |-> FALSE, bootstrapped |-> FALSE, unsafe |-> FALSE]

ObserveDestinationStablePlacement ==
    /\ phase = "splitting"
    /\ ~placementBridge.stable
    /\ placementBridge' = [placementBridge EXCEPT !.stable = TRUE]
    /\ UNCHANGED CoreVars

BootstrapDestination ==
    /\ phase = "splitting"
    /\ ~placementBridge.bootstrapped
    /\ IF BuggyBootstrapWithoutStablePlacement
       THEN TRUE
       ELSE placementBridge.stable
    /\ placementBridge' =
        [placementBridge EXCEPT
            !.bootstrapped = TRUE,
            !.unsafe = (@ \/ ~placementBridge.stable)]
    /\ UNCHANGED CoreVars

BeginSplit ==
    /\ phase = "single"
    /\ phase' = "splitting"
    /\ parentAcceptsRight' = TRUE
    /\ childAcceptsRight' = FALSE
    /\ routeRightOwner' = "parent"
    /\ UNCHANGED <<shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbGraphIndexSeq, dbChildServing, staleFenceCompletion, placementBridge>>

ParentRightWriteDuringSplit ==
    /\ phase = "splitting"
    /\ parentAcceptsRight
    /\ dbDeltaSeq < MaxSeq
    /\ dbDeltaSeq' = dbDeltaSeq + 1
    /\ shardCutoverReady' = FALSE
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, dbReplaySeq,
                  dbTextIndexSeq, dbSparseIndexSeq, dbGraphIndexSeq,
                  dbChildServing, parentAcceptsRight, childAcceptsRight,
                  routeRightOwner, staleFenceCompletion, placementBridge>>

ReplayDelta ==
    /\ phase = "splitting"
    /\ dbReplaySeq < dbDeltaSeq
    /\ dbReplaySeq' = dbReplaySeq + 1
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbGraphIndexSeq, dbChildServing, parentAcceptsRight,
                  childAcceptsRight, routeRightOwner, staleFenceCompletion, placementBridge>>

BuildTextIndex ==
    /\ phase = "splitting"
    /\ dbTextIndexSeq < dbReplaySeq
    /\ dbTextIndexSeq' = dbTextIndexSeq + 1
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbSparseIndexSeq, dbGraphIndexSeq,
                  dbChildServing, parentAcceptsRight, childAcceptsRight,
                  routeRightOwner, staleFenceCompletion, placementBridge>>

BuildSparseIndex ==
    /\ phase = "splitting"
    /\ dbSparseIndexSeq < dbReplaySeq
    /\ dbSparseIndexSeq' = dbSparseIndexSeq + 1
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbGraphIndexSeq,
                  dbChildServing, parentAcceptsRight, childAcceptsRight,
                  routeRightOwner, staleFenceCompletion, placementBridge>>

BuildGraphIndex ==
    /\ phase = "splitting"
    /\ dbGraphIndexSeq < dbReplaySeq
    /\ dbGraphIndexSeq' = dbGraphIndexSeq + 1
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbChildServing, parentAcceptsRight, childAcceptsRight,
                  routeRightOwner, staleFenceCompletion, placementBridge>>

SetShardFence ==
    /\ phase = "splitting"
    /\ placementBridge.bootstrapped
    /\ DbReplayCaughtUp
    /\ shardFenceSet' = TRUE
    /\ shardFenceSeq' = dbDeltaSeq
    /\ UNCHANGED <<phase, shardCutoverReady, dbDeltaSeq, dbReplaySeq,
                  dbTextIndexSeq, dbSparseIndexSeq, dbGraphIndexSeq,
                  dbChildServing, parentAcceptsRight, childAcceptsRight,
                  routeRightOwner, staleFenceCompletion, placementBridge>>

CompleteShardCutover ==
    /\ phase = "splitting"
    /\ shardFenceSet
    /\ DbReplayCaughtUp
    /\ phase' = "cutover"
    /\ shardCutoverReady' = TRUE
    /\ UNCHANGED <<shardFenceSet, shardFenceSeq, dbDeltaSeq, dbReplaySeq,
                  dbTextIndexSeq, dbSparseIndexSeq, dbGraphIndexSeq,
                  dbChildServing, parentAcceptsRight, childAcceptsRight,
                  routeRightOwner, staleFenceCompletion, placementBridge>>

BuggyCompleteShardCutoverWithStaleFence ==
    /\ BuggyCompleteWithStaleFence
    /\ phase = "splitting"
    /\ shardFenceSet
    /\ dbReplaySeq = shardFenceSeq
    /\ dbReplaySeq < dbDeltaSeq
    /\ phase' = "cutover"
    /\ shardCutoverReady' = TRUE
    /\ staleFenceCompletion' = TRUE
    /\ UNCHANGED <<shardFenceSet, shardFenceSeq, dbDeltaSeq, dbReplaySeq,
                  dbTextIndexSeq, dbSparseIndexSeq, dbGraphIndexSeq,
                  dbChildServing, parentAcceptsRight, childAcceptsRight,
                  routeRightOwner, placementBridge>>

PublishDbChildServing ==
    /\ phase = "cutover"
    /\ shardCutoverReady
    /\ DbReadyToServe
    /\ phase' = "children"
    /\ dbChildServing' = TRUE
    /\ parentAcceptsRight' = FALSE
    /\ childAcceptsRight' = TRUE
    /\ UNCHANGED <<shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbGraphIndexSeq, routeRightOwner, staleFenceCompletion, placementBridge>>

BuggyDbChildServingBeforeShardCutover ==
    /\ BuggyDbServeBeforeShardCutover
    /\ phase = "splitting"
    /\ DbReadyToServe
    /\ dbChildServing' = TRUE
    /\ parentAcceptsRight' = FALSE
    /\ childAcceptsRight' = TRUE
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbGraphIndexSeq, routeRightOwner, staleFenceCompletion, placementBridge>>

RouteMetadataToChild ==
    /\ phase = "children"
    /\ shardCutoverReady
    /\ dbChildServing
    /\ routeRightOwner' = "child"
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbGraphIndexSeq, dbChildServing, parentAcceptsRight,
                  childAcceptsRight, staleFenceCompletion, placementBridge>>

BuggyRouteMetadataToChildBeforeDbServing ==
    /\ BuggyRouteBeforeDbServing
    /\ phase \in {"splitting", "cutover"}
    /\ routeRightOwner' = "child"
    /\ UNCHANGED <<phase, shardFenceSet, shardFenceSeq, shardCutoverReady,
                  dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbGraphIndexSeq, dbChildServing, parentAcceptsRight,
                  childAcceptsRight, staleFenceCompletion, placementBridge>>

Rollback ==
    /\ phase = "splitting"
    /\ phase' = "rolledBack"
    /\ shardFenceSet' = FALSE
    /\ shardFenceSeq' = 0
    /\ shardCutoverReady' = FALSE
    /\ dbChildServing' = FALSE
    /\ parentAcceptsRight' = TRUE
    /\ childAcceptsRight' = FALSE
    /\ routeRightOwner' = "parent"
    /\ UNCHANGED <<dbDeltaSeq, dbReplaySeq, dbTextIndexSeq, dbSparseIndexSeq,
                  dbGraphIndexSeq, staleFenceCompletion, placementBridge>>

Next ==
    \/ BeginSplit
    \/ ObserveDestinationStablePlacement
    \/ BootstrapDestination
    \/ ParentRightWriteDuringSplit
    \/ ReplayDelta
    \/ BuildTextIndex
    \/ BuildSparseIndex
    \/ BuildGraphIndex
    \/ SetShardFence
    \/ CompleteShardCutover
    \/ BuggyCompleteShardCutoverWithStaleFence
    \/ PublishDbChildServing
    \/ BuggyDbChildServingBeforeShardCutover
    \/ RouteMetadataToChild
    \/ BuggyRouteMetadataToChildBeforeDbServing
    \/ Rollback

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in Phases
    /\ shardFenceSet \in BOOLEAN
    /\ shardFenceSeq \in 0..MaxSeq
    /\ shardCutoverReady \in BOOLEAN
    /\ dbDeltaSeq \in 0..MaxSeq
    /\ dbReplaySeq \in 0..MaxSeq
    /\ dbTextIndexSeq \in 0..MaxSeq
    /\ dbSparseIndexSeq \in 0..MaxSeq
    /\ dbGraphIndexSeq \in 0..MaxSeq
    /\ dbChildServing \in BOOLEAN
    /\ parentAcceptsRight \in BOOLEAN
    /\ childAcceptsRight \in BOOLEAN
    /\ routeRightOwner \in Owners
    /\ staleFenceCompletion \in BOOLEAN
    /\ placementBridge \in
        [stable: BOOLEAN, bootstrapped: BOOLEAN, unsafe: BOOLEAN]

ReplayNeverExceedsDelta ==
    /\ dbReplaySeq <= dbDeltaSeq
    /\ dbTextIndexSeq <= dbReplaySeq
    /\ dbSparseIndexSeq <= dbReplaySeq
    /\ dbGraphIndexSeq <= dbReplaySeq

ShardCutoverRequiresCurrentReplay ==
    shardCutoverReady =>
        /\ DbReplayCaughtUp
        /\ ~staleFenceCompletion
        /\ placementBridge.bootstrapped

DestinationBootstrapRequiresStablePlacement ==
    ~placementBridge.unsafe

DbServingRequiresShardCutoverAndIndexes ==
    dbChildServing =>
        /\ shardCutoverReady
        /\ DbReadyToServe
        /\ ~parentAcceptsRight
        /\ childAcceptsRight

MetadataChildRouteRequiresBothLayersReady ==
    routeRightOwner = "child" =>
        /\ shardCutoverReady
        /\ dbChildServing
        /\ DbReadyToServe
        /\ phase = "children"

ParentAndChildDoNotBothAcceptRight ==
    ~(parentAcceptsRight /\ childAcceptsRight)

RollbackDoesNotExposeChild ==
    phase = "rolledBack" =>
        /\ routeRightOwner = "parent"
        /\ ~dbChildServing
        /\ parentAcceptsRight
        /\ ~childAcceptsRight

Safety ==
    /\ TypeOK
    /\ ReplayNeverExceedsDelta
    /\ ShardCutoverRequiresCurrentReplay
    /\ DestinationBootstrapRequiresStablePlacement
    /\ DbServingRequiresShardCutoverAndIndexes
    /\ MetadataChildRouteRequiresBothLayersReady
    /\ ParentAndChildDoNotBothAcceptRight
    /\ RollbackDoesNotExposeChild

=============================================================================
