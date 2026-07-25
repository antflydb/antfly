---------------------- MODULE AntflyPlacementReadiness ----------------------
(*
Code anchors:
  - pkg/antfly/src/metadata/state.zig mergeHealthyGroupStatuses and the
    projected voter_count_known field.
  - pkg/antfly/src/metadata/reconciler.zig's independent aggregation of the
    same group-status evidence.
  - pkg/antfly/src/metadata/service.zig transitionStatusHasStablePlacement.
  - pkg/antfly/src/data/storage/hosted_shard_ops.zig transition admission.

Why this is a separate protocol model:
  Store reports and their epistemic quality form one authoritative metadata
  aggregation protocol.  Split replay, routing, and DB index cutover have
  independent state and fairness assumptions and remain in the split models.
  AntflySplitRefinementBridge carries the one fact that crosses the boundary:
  destination bootstrap requires stable placement.

What this proves:
  - A report that explicitly disclaims voter-set knowledge may seed an
    estimate, but cannot create an authoritative conflict.
  - Transition admission requires a known, exact, healthy, non-joint voter
    set and a correctly placed known leader.
  - Once authoritative evidence converges, recomputation can recover
    transition readiness instead of preserving an absorbing ambiguity.

The seed-versus-conflict rule is an explicit proposed contract, not a claim
that the current implementation already follows it.  The B1 mutant encodes
the July-25 smokeout behavior: a known leader reports the expected voter
count, an unknown follower reports a smaller count, and aggregation latches
voter_count_known = FALSE even though the retained count is exact.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS BuggyUnknownReportLatchesAmbiguity,
          BuggyLeaderAloneAdmitsTransition

ASSUME /\ BuggyUnknownReportLatchesAmbiguity \in BOOLEAN
       /\ BuggyLeaderAloneAdmitsTransition \in BOOLEAN

Stores == {"leader", "peer"}
Counts == 1..2
ExpectedVoters == 2

VARIABLES reportPresent, reportKnowsVoterSet, reportCount,
          leaderKnown, leaderPlaced, healthyReports, jointConsensus,
          observedVoterCount, voterCountKnown, ambiguousVoterCount,
          unknownReportLatchedConflict, transitionStarted,
          transitionStartedWithoutStablePlacement

vars ==
    <<reportPresent, reportKnowsVoterSet, reportCount,
      leaderKnown, leaderPlaced, healthyReports, jointConsensus,
      observedVoterCount, voterCountKnown, ambiguousVoterCount,
      unknownReportLatchedConflict, transitionStarted,
      transitionStartedWithoutStablePlacement>>

NoReports == [s \in Stores |-> FALSE]
NoCounts == [s \in Stores |-> 1]

Init ==
    /\ reportPresent = NoReports
    /\ reportKnowsVoterSet = NoReports
    /\ reportCount = NoCounts
    /\ leaderKnown = FALSE
    /\ leaderPlaced = FALSE
    /\ healthyReports = 0
    /\ jointConsensus = FALSE
    /\ observedVoterCount = 0
    /\ voterCountKnown = FALSE
    /\ ambiguousVoterCount = FALSE
    /\ unknownReportLatchedConflict = FALSE
    /\ transitionStarted = FALSE
    /\ transitionStartedWithoutStablePlacement = FALSE

PresentStores ==
    {s \in Stores: reportPresent[s]}

AuthoritativeStores ==
    {s \in Stores:
        reportPresent[s] /\ reportKnowsVoterSet[s]}

AuthoritativeCounts ==
    {c \in Counts:
        \E s \in AuthoritativeStores: reportCount[s] = c}

AllReportedCounts ==
    {c \in Counts:
        \E s \in PresentStores: reportCount[s] = c}

SeedCount ==
    IF reportPresent["leader"]
    THEN reportCount["leader"]
    ELSE IF reportPresent["peer"]
         THEN reportCount["peer"]
         ELSE 0

PublishLeaderReport ==
    /\ ~reportPresent["leader"]
       \/ ~reportKnowsVoterSet["leader"]
       \/ reportCount["leader"] # ExpectedVoters
       \/ ~leaderKnown
       \/ ~leaderPlaced
       \/ healthyReports < ExpectedVoters
    /\ reportPresent' = [reportPresent EXCEPT !["leader"] = TRUE]
    /\ reportKnowsVoterSet' =
        [reportKnowsVoterSet EXCEPT !["leader"] = TRUE]
    /\ reportCount' = [reportCount EXCEPT !["leader"] = ExpectedVoters]
    /\ leaderKnown' = TRUE
    /\ leaderPlaced' = TRUE
    /\ healthyReports' = ExpectedVoters
    /\ UNCHANGED <<jointConsensus, observedVoterCount, voterCountKnown,
                  ambiguousVoterCount, unknownReportLatchedConflict,
                  transitionStarted, transitionStartedWithoutStablePlacement>>

PublishUnknownFollowerConflict ==
    /\ ~reportPresent["peer"]
       \/ reportKnowsVoterSet["peer"]
       \/ reportCount["peer"] # 1
    /\ reportPresent' = [reportPresent EXCEPT !["peer"] = TRUE]
    /\ reportKnowsVoterSet' =
        [reportKnowsVoterSet EXCEPT !["peer"] = FALSE]
    /\ reportCount' = [reportCount EXCEPT !["peer"] = 1]
    /\ UNCHANGED <<leaderKnown, leaderPlaced, healthyReports,
                  jointConsensus, observedVoterCount, voterCountKnown,
                  ambiguousVoterCount, unknownReportLatchedConflict,
                  transitionStarted, transitionStartedWithoutStablePlacement>>

PublishConvergedFollowerReport ==
    /\ ~reportPresent["peer"]
       \/ ~reportKnowsVoterSet["peer"]
       \/ reportCount["peer"] # ExpectedVoters
    /\ reportPresent' = [reportPresent EXCEPT !["peer"] = TRUE]
    /\ reportKnowsVoterSet' =
        [reportKnowsVoterSet EXCEPT !["peer"] = TRUE]
    /\ reportCount' = [reportCount EXCEPT !["peer"] = ExpectedVoters]
    /\ UNCHANGED <<leaderKnown, leaderPlaced, healthyReports,
                  jointConsensus, observedVoterCount, voterCountKnown,
                  ambiguousVoterCount, unknownReportLatchedConflict,
                  transitionStarted, transitionStartedWithoutStablePlacement>>

SetJointConsensus ==
    /\ ~jointConsensus
    /\ jointConsensus' = TRUE
    /\ UNCHANGED <<reportPresent, reportKnowsVoterSet, reportCount,
                  leaderKnown, leaderPlaced, healthyReports,
                  observedVoterCount, voterCountKnown, ambiguousVoterCount,
                  unknownReportLatchedConflict, transitionStarted,
                  transitionStartedWithoutStablePlacement>>

ClearJointConsensus ==
    /\ jointConsensus
    /\ jointConsensus' = FALSE
    /\ UNCHANGED <<reportPresent, reportKnowsVoterSet, reportCount,
                  leaderKnown, leaderPlaced, healthyReports,
                  observedVoterCount, voterCountKnown, ambiguousVoterCount,
                  unknownReportLatchedConflict, transitionStarted,
                  transitionStartedWithoutStablePlacement>>

RecomputePlacementEvidence ==
    LET EvidenceCounts ==
            IF BuggyUnknownReportLatchesAmbiguity
            THEN AllReportedCounts
            ELSE AuthoritativeCounts
        NewAmbiguous == Cardinality(EvidenceCounts) > 1
        NewKnown ==
            /\ Cardinality(AuthoritativeCounts) = 1
            /\ ~NewAmbiguous
        NewObserved ==
            IF Cardinality(AuthoritativeCounts) > 0
            THEN CHOOSE c \in AuthoritativeCounts: TRUE
            ELSE SeedCount
        B1Shape ==
            /\ reportPresent["leader"]
            /\ reportKnowsVoterSet["leader"]
            /\ reportCount["leader"] = ExpectedVoters
            /\ reportPresent["peer"]
            /\ ~reportKnowsVoterSet["peer"]
            /\ reportCount["peer"] # ExpectedVoters
    IN
    /\ \/ observedVoterCount # NewObserved
       \/ voterCountKnown # NewKnown
       \/ ambiguousVoterCount # NewAmbiguous
       \/ (BuggyUnknownReportLatchesAmbiguity
           /\ B1Shape
           /\ ~unknownReportLatchedConflict)
    /\ observedVoterCount' = NewObserved
    /\ voterCountKnown' = NewKnown
    /\ ambiguousVoterCount' = NewAmbiguous
    /\ unknownReportLatchedConflict' =
        (unknownReportLatchedConflict
         \/ (BuggyUnknownReportLatchesAmbiguity /\ B1Shape /\ NewAmbiguous))
    /\ UNCHANGED <<reportPresent, reportKnowsVoterSet, reportCount,
                  leaderKnown, leaderPlaced, healthyReports,
                  jointConsensus, transitionStarted,
                  transitionStartedWithoutStablePlacement>>

StablePlacementReady ==
    /\ leaderKnown
    /\ leaderPlaced
    /\ voterCountKnown
    /\ ~ambiguousVoterCount
    /\ observedVoterCount = ExpectedVoters
    /\ healthyReports >= ExpectedVoters
    /\ ~jointConsensus

StartTransition ==
    /\ ~transitionStarted
    /\ IF BuggyLeaderAloneAdmitsTransition
       THEN leaderKnown /\ leaderPlaced
       ELSE StablePlacementReady
    /\ transitionStarted' = TRUE
    /\ transitionStartedWithoutStablePlacement' =
        (transitionStartedWithoutStablePlacement \/ ~StablePlacementReady)
    /\ UNCHANGED <<reportPresent, reportKnowsVoterSet, reportCount,
                  leaderKnown, leaderPlaced, healthyReports,
                  jointConsensus, observedVoterCount, voterCountKnown,
                  ambiguousVoterCount, unknownReportLatchedConflict>>

Next ==
    \/ PublishLeaderReport
    \/ PublishUnknownFollowerConflict
    \/ PublishConvergedFollowerReport
    \/ SetJointConsensus
    \/ ClearJointConsensus
    \/ RecomputePlacementEvidence
    \/ StartTransition

Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(PublishLeaderReport)
    /\ WF_vars(PublishConvergedFollowerReport)
    /\ WF_vars(RecomputePlacementEvidence)
    /\ WF_vars(ClearJointConsensus)
    /\ SF_vars(StartTransition)

TypeOK ==
    /\ reportPresent \in [Stores -> BOOLEAN]
    /\ reportKnowsVoterSet \in [Stores -> BOOLEAN]
    /\ reportCount \in [Stores -> Counts]
    /\ leaderKnown \in BOOLEAN
    /\ leaderPlaced \in BOOLEAN
    /\ healthyReports \in 0..2
    /\ jointConsensus \in BOOLEAN
    /\ observedVoterCount \in 0..2
    /\ voterCountKnown \in BOOLEAN
    /\ ambiguousVoterCount \in BOOLEAN
    /\ unknownReportLatchedConflict \in BOOLEAN
    /\ transitionStarted \in BOOLEAN
    /\ transitionStartedWithoutStablePlacement \in BOOLEAN

UnknownReportsDoNotCreateAuthoritativeConflict ==
    ~unknownReportLatchedConflict

TransitionRequiresStablePlacement ==
    ~transitionStartedWithoutStablePlacement

KnownAndAmbiguousAreExclusive ==
    ~(voterCountKnown /\ ambiguousVoterCount)

EvidenceEventuallyRecoversReadiness ==
    <>StablePlacementReady

TransitionEventuallyStarts ==
    <>transitionStarted

=============================================================================
