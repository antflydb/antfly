\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.

------------------------- MODULE AntflyDbSplitVisibility -------------------------
(*
  Bounded DB-local split/merge visibility model.

  Concrete Zig contracts modeled:
    - split prepare snapshots the right range and records later parent deltas
      until finalization.
    - a split child may serve only after replaying all split deltas and building
      its text/sparse/graph shadow indexes through the required generation.
    - split finalization trims the parent right range, routes the moved child
      artifact remotely, and fences future right-range parent writes.
    - generated enrichment publication is accepted only from the current range
      owner after split or merge ownership changes.
    - merge-style cutover collapses the donor, extends the receiver, and routes
      text/sparse/graph indexes through the receiver.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyParentAcceptsChildAfterCutover,
    BuggyChildServesBeforeReplay,
    BuggyDonorServesAfterMerge,
    BuggyEnrichmentWrongOwner

MaxSeq == 2
Phases == {"single", "prepared", "replaying", "children", "mergePrepared", "merged"}
Owners == {"none", "parent", "child", "donor", "receiver"}
Placements == {"none", "local", "remote"}

VARIABLES
    phase,
    leftSeq,
    parentRightSeq,
    splitSnapshotSeq,
    splitDeltaSeq,
    childReplaySeq,
    childRightSeq,
    childTextIndexSeq,
    childSparseIndexSeq,
    childGraphIndexSeq,
    childArtifactPlacement,
    childServing,
    parentOwnsLeft,
    parentOwnsRight,
    childOwnsRight,
    donorOwnsRight,
    receiverOwnsRight,
    parentAcceptsRight,
    childAcceptsRight,
    donorAcceptsRight,
    receiverAcceptsRight,
    donorRightSeq,
    receiverRightSeq,
    receiverTextIndexSeq,
    receiverSparseIndexSeq,
    receiverGraphIndexSeq,
    rightRouteOwner,
    enrichmentOwner,
    parentAcceptedChildAfterCutover,
    donorServedAfterMerge

vars == <<phase, leftSeq, parentRightSeq, splitSnapshotSeq, splitDeltaSeq,
          childReplaySeq, childRightSeq, childTextIndexSeq, childSparseIndexSeq,
          childGraphIndexSeq, childArtifactPlacement, childServing,
          parentOwnsLeft, parentOwnsRight, childOwnsRight, donorOwnsRight,
          receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
          donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
          receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
          receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
          parentAcceptedChildAfterCutover, donorServedAfterMerge>>

CurrentRightOwner ==
    IF phase = "children" THEN "child"
    ELSE IF phase = "mergePrepared" THEN "donor"
    ELSE IF phase = "merged" THEN "receiver"
    ELSE "parent"

ChildIndexesComplete ==
    /\ childTextIndexSeq = childRightSeq
    /\ childSparseIndexSeq = childRightSeq
    /\ childGraphIndexSeq = childRightSeq

ReceiverIndexesComplete ==
    /\ receiverTextIndexSeq = receiverRightSeq
    /\ receiverSparseIndexSeq = receiverRightSeq
    /\ receiverGraphIndexSeq = receiverRightSeq

ChildCaughtUpForCutover ==
    /\ childReplaySeq = splitDeltaSeq
    /\ childRightSeq = parentRightSeq
    /\ ChildIndexesComplete

Init ==
    /\ phase = "single"
    /\ leftSeq = 0
    /\ parentRightSeq = 0
    /\ splitSnapshotSeq = 0
    /\ splitDeltaSeq = 0
    /\ childReplaySeq = 0
    /\ childRightSeq = 0
    /\ childTextIndexSeq = 0
    /\ childSparseIndexSeq = 0
    /\ childGraphIndexSeq = 0
    /\ childArtifactPlacement = "none"
    /\ childServing = FALSE
    /\ parentOwnsLeft = TRUE
    /\ parentOwnsRight = TRUE
    /\ childOwnsRight = FALSE
    /\ donorOwnsRight = FALSE
    /\ receiverOwnsRight = FALSE
    /\ parentAcceptsRight = TRUE
    /\ childAcceptsRight = FALSE
    /\ donorAcceptsRight = FALSE
    /\ receiverAcceptsRight = FALSE
    /\ donorRightSeq = 0
    /\ receiverRightSeq = 0
    /\ receiverTextIndexSeq = 0
    /\ receiverSparseIndexSeq = 0
    /\ receiverGraphIndexSeq = 0
    /\ rightRouteOwner = "parent"
    /\ enrichmentOwner = "none"
    /\ parentAcceptedChildAfterCutover = FALSE
    /\ donorServedAfterMerge = FALSE

ParentLeftWrite ==
    /\ parentOwnsLeft
    /\ leftSeq < MaxSeq
    /\ leftSeq' = leftSeq + 1
    /\ UNCHANGED <<phase, parentRightSeq, splitSnapshotSeq, splitDeltaSeq,
                  childReplaySeq, childRightSeq, childTextIndexSeq,
                  childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

ParentRightWriteBeforeSplit ==
    /\ phase = "single"
    /\ parentOwnsRight
    /\ parentAcceptsRight
    /\ parentRightSeq < MaxSeq
    /\ parentRightSeq' = parentRightSeq + 1
    /\ UNCHANGED <<phase, leftSeq, splitSnapshotSeq, splitDeltaSeq,
                  childReplaySeq, childRightSeq, childTextIndexSeq,
                  childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

PrepareSplit ==
    /\ phase = "single"
    /\ phase' = "prepared"
    /\ splitSnapshotSeq' = parentRightSeq
    /\ splitDeltaSeq' = 0
    /\ childReplaySeq' = 0
    /\ childRightSeq' = parentRightSeq
    /\ childTextIndexSeq' = 0
    /\ childSparseIndexSeq' = 0
    /\ childGraphIndexSeq' = 0
    /\ childArtifactPlacement' = "local"
    /\ rightRouteOwner' = "parent"
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<leftSeq, parentRightSeq, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, parentAcceptedChildAfterCutover,
                  donorServedAfterMerge>>

ParentRightWriteDuringSplit ==
    /\ phase \in {"prepared", "replaying"}
    /\ parentOwnsRight
    /\ parentAcceptsRight
    /\ parentRightSeq < MaxSeq
    /\ splitDeltaSeq < MaxSeq
    /\ phase' = "replaying"
    /\ parentRightSeq' = parentRightSeq + 1
    /\ splitDeltaSeq' = splitDeltaSeq + 1
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<leftSeq, splitSnapshotSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

ReplaySplitDelta ==
    /\ phase \in {"prepared", "replaying"}
    /\ childReplaySeq < splitDeltaSeq
    /\ phase' = "replaying"
    /\ childReplaySeq' = childReplaySeq + 1
    /\ childRightSeq' = splitSnapshotSeq + childReplaySeq + 1
    /\ UNCHANGED <<leftSeq, parentRightSeq, splitSnapshotSeq, splitDeltaSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

BuildChildTextIndex ==
    /\ phase \in {"prepared", "replaying"}
    /\ childTextIndexSeq < childRightSeq
    /\ childTextIndexSeq' = childTextIndexSeq + 1
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

BuildChildSparseIndex ==
    /\ phase \in {"prepared", "replaying"}
    /\ childSparseIndexSeq < childRightSeq
    /\ childSparseIndexSeq' = childSparseIndexSeq + 1
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

BuildChildGraphIndex ==
    /\ phase \in {"prepared", "replaying"}
    /\ childGraphIndexSeq < childRightSeq
    /\ childGraphIndexSeq' = childGraphIndexSeq + 1
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

FinalizeSplit ==
    /\ phase \in {"prepared", "replaying"}
    /\ ChildCaughtUpForCutover
    /\ phase' = "children"
    /\ parentOwnsRight' = FALSE
    /\ childOwnsRight' = TRUE
    /\ parentAcceptsRight' = FALSE
    /\ childAcceptsRight' = TRUE
    /\ childServing' = TRUE
    /\ childArtifactPlacement' = "remote"
    /\ rightRouteOwner' = "child"
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<leftSeq, parentRightSeq, splitSnapshotSeq, splitDeltaSeq,
                  childReplaySeq, childRightSeq, childTextIndexSeq,
                  childSparseIndexSeq, childGraphIndexSeq, parentOwnsLeft,
                  donorOwnsRight, receiverOwnsRight, donorAcceptsRight,
                  receiverAcceptsRight, donorRightSeq, receiverRightSeq,
                  receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, parentAcceptedChildAfterCutover,
                  donorServedAfterMerge>>

ChildRightWrite ==
    /\ phase = "children"
    /\ childOwnsRight
    /\ childAcceptsRight
    /\ childRightSeq < MaxSeq
    /\ parentRightSeq < MaxSeq
    /\ childRightSeq' = childRightSeq + 1
    /\ parentRightSeq' = parentRightSeq + 1
    /\ childTextIndexSeq' = childRightSeq'
    /\ childSparseIndexSeq' = childRightSeq'
    /\ childGraphIndexSeq' = childRightSeq'
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<phase, leftSeq, splitSnapshotSeq, splitDeltaSeq,
                  childReplaySeq, childArtifactPlacement, childServing,
                  parentOwnsLeft, parentOwnsRight, childOwnsRight,
                  donorOwnsRight, receiverOwnsRight, parentAcceptsRight,
                  childAcceptsRight, donorAcceptsRight, receiverAcceptsRight,
                  donorRightSeq, receiverRightSeq, receiverTextIndexSeq,
                  receiverSparseIndexSeq, receiverGraphIndexSeq,
                  rightRouteOwner, parentAcceptedChildAfterCutover,
                  donorServedAfterMerge>>

BuggyParentRightWriteAfterCutover ==
    /\ BuggyParentAcceptsChildAfterCutover
    /\ phase = "children"
    /\ parentRightSeq < MaxSeq
    /\ parentRightSeq' = parentRightSeq + 1
    /\ parentAcceptedChildAfterCutover' = TRUE
    /\ UNCHANGED <<phase, leftSeq, splitSnapshotSeq, splitDeltaSeq,
                  childReplaySeq, childRightSeq, childTextIndexSeq,
                  childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  donorServedAfterMerge>>

BuggyServeChildBeforeReplay ==
    /\ BuggyChildServesBeforeReplay
    /\ phase \in {"prepared", "replaying"}
    /\ ~ChildCaughtUpForCutover
    /\ childOwnsRight' = TRUE
    /\ childAcceptsRight' = TRUE
    /\ childServing' = TRUE
    /\ rightRouteOwner' = "child"
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, parentOwnsLeft, parentOwnsRight,
                  donorOwnsRight, receiverOwnsRight, parentAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, enrichmentOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

StartMerge ==
    /\ phase = "single"
    /\ phase' = "mergePrepared"
    /\ parentOwnsRight' = FALSE
    /\ parentAcceptsRight' = FALSE
    /\ donorOwnsRight' = TRUE
    /\ donorAcceptsRight' = TRUE
    /\ receiverOwnsRight' = FALSE
    /\ receiverAcceptsRight' = FALSE
    /\ donorRightSeq' = parentRightSeq
    /\ receiverRightSeq' = 0
    /\ receiverTextIndexSeq' = 0
    /\ receiverSparseIndexSeq' = 0
    /\ receiverGraphIndexSeq' = 0
    /\ rightRouteOwner' = "donor"
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<leftSeq, parentRightSeq, splitSnapshotSeq, splitDeltaSeq,
                  childReplaySeq, childRightSeq, childTextIndexSeq,
                  childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  childOwnsRight, childAcceptsRight,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

DonorRightWriteBeforeMerge ==
    /\ phase = "mergePrepared"
    /\ donorOwnsRight
    /\ donorAcceptsRight
    /\ donorRightSeq < MaxSeq
    /\ donorRightSeq' = donorRightSeq + 1
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, receiverRightSeq,
                  receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

FinalizeMerge ==
    /\ phase = "mergePrepared"
    /\ phase' = "merged"
    /\ donorOwnsRight' = FALSE
    /\ donorAcceptsRight' = FALSE
    /\ receiverOwnsRight' = TRUE
    /\ receiverAcceptsRight' = TRUE
    /\ receiverRightSeq' = donorRightSeq
    /\ receiverTextIndexSeq' = donorRightSeq
    /\ receiverSparseIndexSeq' = donorRightSeq
    /\ receiverGraphIndexSeq' = donorRightSeq
    /\ rightRouteOwner' = "receiver"
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<leftSeq, parentRightSeq, splitSnapshotSeq, splitDeltaSeq,
                  childReplaySeq, childRightSeq, childTextIndexSeq,
                  childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, parentAcceptsRight,
                  childAcceptsRight, donorRightSeq,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

ReceiverRightWrite ==
    /\ phase = "merged"
    /\ receiverOwnsRight
    /\ receiverAcceptsRight
    /\ receiverRightSeq < MaxSeq
    /\ receiverRightSeq' = receiverRightSeq + 1
    /\ receiverTextIndexSeq' = receiverRightSeq'
    /\ receiverSparseIndexSeq' = receiverRightSeq'
    /\ receiverGraphIndexSeq' = receiverRightSeq'
    /\ enrichmentOwner' = "none"
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  rightRouteOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

BuggyDonorRightWriteAfterMerge ==
    /\ BuggyDonorServesAfterMerge
    /\ phase = "merged"
    /\ donorRightSeq < MaxSeq
    /\ donorRightSeq' = donorRightSeq + 1
    /\ donorServedAfterMerge' = TRUE
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, receiverRightSeq,
                  receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner, enrichmentOwner,
                  parentAcceptedChildAfterCutover>>

PublishEnrichment(owner) ==
    /\ owner \in Owners
    /\ owner = CurrentRightOwner
    /\ enrichmentOwner' = owner
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

BuggyPublishWrongOwner(owner) ==
    /\ BuggyEnrichmentWrongOwner
    /\ owner \in Owners
    /\ owner # "none"
    /\ owner # CurrentRightOwner
    /\ enrichmentOwner' = owner
    /\ UNCHANGED <<phase, leftSeq, parentRightSeq, splitSnapshotSeq,
                  splitDeltaSeq, childReplaySeq, childRightSeq,
                  childTextIndexSeq, childSparseIndexSeq, childGraphIndexSeq,
                  childArtifactPlacement, childServing, parentOwnsLeft,
                  parentOwnsRight, childOwnsRight, donorOwnsRight,
                  receiverOwnsRight, parentAcceptsRight, childAcceptsRight,
                  donorAcceptsRight, receiverAcceptsRight, donorRightSeq,
                  receiverRightSeq, receiverTextIndexSeq, receiverSparseIndexSeq,
                  receiverGraphIndexSeq, rightRouteOwner,
                  parentAcceptedChildAfterCutover, donorServedAfterMerge>>

Next ==
    \/ ParentLeftWrite
    \/ ParentRightWriteBeforeSplit
    \/ PrepareSplit
    \/ ParentRightWriteDuringSplit
    \/ ReplaySplitDelta
    \/ BuildChildTextIndex
    \/ BuildChildSparseIndex
    \/ BuildChildGraphIndex
    \/ FinalizeSplit
    \/ ChildRightWrite
    \/ BuggyParentRightWriteAfterCutover
    \/ BuggyServeChildBeforeReplay
    \/ StartMerge
    \/ DonorRightWriteBeforeMerge
    \/ FinalizeMerge
    \/ ReceiverRightWrite
    \/ BuggyDonorRightWriteAfterMerge
    \/ \E owner \in Owners : PublishEnrichment(owner)
    \/ \E owner \in Owners : BuggyPublishWrongOwner(owner)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in Phases
    /\ leftSeq \in 0..MaxSeq
    /\ parentRightSeq \in 0..MaxSeq
    /\ splitSnapshotSeq \in 0..MaxSeq
    /\ splitDeltaSeq \in 0..MaxSeq
    /\ childReplaySeq \in 0..MaxSeq
    /\ childRightSeq \in 0..MaxSeq
    /\ childTextIndexSeq \in 0..MaxSeq
    /\ childSparseIndexSeq \in 0..MaxSeq
    /\ childGraphIndexSeq \in 0..MaxSeq
    /\ childArtifactPlacement \in Placements
    /\ childServing \in BOOLEAN
    /\ parentOwnsLeft \in BOOLEAN
    /\ parentOwnsRight \in BOOLEAN
    /\ childOwnsRight \in BOOLEAN
    /\ donorOwnsRight \in BOOLEAN
    /\ receiverOwnsRight \in BOOLEAN
    /\ parentAcceptsRight \in BOOLEAN
    /\ childAcceptsRight \in BOOLEAN
    /\ donorAcceptsRight \in BOOLEAN
    /\ receiverAcceptsRight \in BOOLEAN
    /\ donorRightSeq \in 0..MaxSeq
    /\ receiverRightSeq \in 0..MaxSeq
    /\ receiverTextIndexSeq \in 0..MaxSeq
    /\ receiverSparseIndexSeq \in 0..MaxSeq
    /\ receiverGraphIndexSeq \in 0..MaxSeq
    /\ rightRouteOwner \in Owners
    /\ enrichmentOwner \in Owners
    /\ parentAcceptedChildAfterCutover \in BOOLEAN
    /\ donorServedAfterMerge \in BOOLEAN

RightOwnershipMatchesPhase ==
    /\ (phase \in {"single", "prepared", "replaying"} =>
        /\ parentOwnsRight
        /\ ~childOwnsRight
        /\ ~donorOwnsRight
        /\ ~receiverOwnsRight)
    /\ (phase = "children" =>
        /\ ~parentOwnsRight
        /\ childOwnsRight
        /\ ~donorOwnsRight
        /\ ~receiverOwnsRight)
    /\ (phase = "mergePrepared" =>
        /\ ~parentOwnsRight
        /\ ~childOwnsRight
        /\ donorOwnsRight
        /\ ~receiverOwnsRight)
    /\ (phase = "merged" =>
        /\ ~parentOwnsRight
        /\ ~childOwnsRight
        /\ ~donorOwnsRight
        /\ receiverOwnsRight)

RoutesCurrentOwner ==
    rightRouteOwner = CurrentRightOwner

SplitDeltasTracked ==
    /\ (phase \in {"prepared", "replaying"} =>
        /\ parentRightSeq = splitSnapshotSeq + splitDeltaSeq
        /\ childReplaySeq <= splitDeltaSeq
        /\ childRightSeq = splitSnapshotSeq + childReplaySeq)
    /\ (phase = "children" =>
        /\ childReplaySeq = splitDeltaSeq
        /\ childRightSeq = parentRightSeq
        /\ childRightSeq >= splitSnapshotSeq + splitDeltaSeq)

ParentCannotAcceptChildRangeAfterCutover ==
    phase = "children" =>
        /\ ~parentOwnsRight
        /\ ~parentAcceptsRight
        /\ ~parentAcceptedChildAfterCutover

ChildServingRequiresReplayAndIndexes ==
    childServing =>
        /\ phase = "children"
        /\ childOwnsRight
        /\ childAcceptsRight
        /\ childRightSeq = parentRightSeq
        /\ childReplaySeq = splitDeltaSeq
        /\ ChildIndexesComplete
        /\ childArtifactPlacement = "remote"

ChildArtifactRemoteAfterSplit ==
    phase = "children" => childArtifactPlacement = "remote"

EnrichmentOnlyForCurrentRightOwner ==
    enrichmentOwner = "none" \/ enrichmentOwner = CurrentRightOwner

MergeDonorDoesNotServeAfterHandoff ==
    phase = "merged" =>
        /\ ~donorOwnsRight
        /\ ~donorAcceptsRight
        /\ ~donorServedAfterMerge

MergeReceiverRoutesAllIndexes ==
    phase = "merged" =>
        /\ receiverOwnsRight
        /\ receiverAcceptsRight
        /\ ReceiverIndexesComplete

Safety ==
    /\ TypeOK
    /\ RightOwnershipMatchesPhase
    /\ RoutesCurrentOwner
    /\ SplitDeltasTracked
    /\ ParentCannotAcceptChildRangeAfterCutover
    /\ ChildServingRequiresReplayAndIndexes
    /\ ChildArtifactRemoteAfterSplit
    /\ EnrichmentOnlyForCurrentRightOwner
    /\ MergeDonorDoesNotServeAfterHandoff
    /\ MergeReceiverRoutesAllIndexes

=============================================================================
