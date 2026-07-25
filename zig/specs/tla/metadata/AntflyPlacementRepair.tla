-------------------------- MODULE AntflyPlacementRepair -------------------------
(*
Code anchors:
  - pkg/antfly/src/metadata/placement_planner.zig duplicate-id recovery,
    stable replacement selection, survivor identity, and repair serialization.
  - pkg/antfly/src/metadata/reconciler.zig MembershipTransitionIndex final-peer
    latch, per-store membership evidence, and source-retirement proof.
What this proves:
  - A quota-full catalog with duplicate declared replica ids is recoverable
    without dropping the more advanced duplicate or changing placement count.
  - Replacement retries preserve their target and survivor identities.
  - The expanded membership contracts only after an authoritative live leader
    reports the exact latched final voter set from its own placement store.
  - A stale follower cannot permanently block a safe contraction.
Deliberate omissions:
  - Exact hashes/scoring, transfer bytes, Raft joint-consensus internals,
    network timing, and more than one simultaneous replacement.
State bounds:
  - One three-replica group, one duplicate-id repair, one replacement, one
    expanded membership, one live final leader, and one permanently stale peer.
Make targets:
  - AntflyPlacementRepair, heavy-liveness, and its Bad* checks.
Correspondence tier:
  - Mature: focused planner and reconciler tests anchor every transition.
*)

EXTENDS Naturals, FiniteSets

CONSTANTS BuggyLoadSensitive, BuggyRenumber, BuggyCombineRebalance,
          BuggyDuplicateIds, BuggyDropAdvancedDuplicate, BuggyRelatchFinal,
          BuggyAcceptWrongStore, BuggyAcceptWrongSet, BuggyAcceptNonLeader,
          BuggyPrematureRetire, BuggyWaitForAllFollowers

ASSUME /\ BuggyLoadSensitive \in BOOLEAN
       /\ BuggyRenumber \in BOOLEAN
       /\ BuggyCombineRebalance \in BOOLEAN
       /\ BuggyDuplicateIds \in BOOLEAN
       /\ BuggyDropAdvancedDuplicate \in BOOLEAN
       /\ BuggyRelatchFinal \in BOOLEAN
       /\ BuggyAcceptWrongStore \in BOOLEAN
       /\ BuggyAcceptWrongSet \in BOOLEAN
       /\ BuggyAcceptNonLeader \in BOOLEAN
       /\ BuggyPrematureRetire \in BOOLEAN
       /\ BuggyWaitForAllFollowers \in BOOLEAN

Slots == 1..3
InitialReplicas == [i \in Slots |->
    CASE i = 1 -> "A" [] i = 2 -> "B" [] OTHER -> "C"]
InitialIds == [i \in Slots |-> i]
DuplicateIds == [i \in Slots |-> IF i = 3 THEN 2 ELSE i]
RecoveredIds == [i \in Slots |-> CASE i = 1 -> 1 [] i = 2 -> 3 [] OTHER -> 2]
DroppedAdvancedIds == InitialIds
Progress == [i \in Slots |-> CASE i = 1 -> 1 [] i = 2 -> 1 [] OTHER -> 2]
CorrectReplicas(target) == [i \in Slots |->
    CASE i = 1 -> "A" [] i = 2 -> "B" [] OTHER -> target]
RenumberedReplicas(target) == [i \in Slots |->
    CASE i = 1 -> target [] i = 2 -> "A" [] OTHER -> "B"]
CombinedRepairReplicas(target) == [i \in Slots |->
    CASE i = 1 -> "E" [] i = 2 -> "B" [] OTHER -> target]
FinalMembership == {"A", "B", "D"}
ExpandedMembership == {"A", "B", "C", "D"}
ChurnedMembership == {"A", "D", "E"}
WrongMembership == {"A", "B", "C"}
LiveFinalLeaderStore == "A"
StaleFinalFollowers == {"B", "D"}

VARIABLES phase, loadPreference, latchedTarget, planTarget, replicas, declaredId,
          hadDuplicate, duplicateRepaired, expandedPeers, finalPeers, plannerDesired,
          proofAccepted, proofWasValid, proofStore, sourcePresent

vars == <<phase, loadPreference, latchedTarget, planTarget, replicas, declaredId,
          hadDuplicate, duplicateRepaired, expandedPeers, finalPeers, plannerDesired,
          proofAccepted, proofWasValid, proofStore, sourcePresent>>

IdsUnique(ids) == Cardinality({ids[i] : i \in Slots}) = Cardinality(Slots)

Init ==
    /\ phase = "repair_ids"
    /\ loadPreference = "D"
    /\ latchedTarget = "none"
    /\ planTarget = "none"
    /\ replicas = InitialReplicas
    /\ declaredId \in {InitialIds, DuplicateIds}
    /\ hadDuplicate = (declaredId = DuplicateIds)
    /\ duplicateRepaired = (declaredId = InitialIds)
    /\ expandedPeers = {}
    /\ finalPeers = {}
    /\ plannerDesired = FinalMembership
    /\ proofAccepted = FALSE
    /\ proofWasValid = FALSE
    /\ proofStore = "none"
    /\ sourcePresent = TRUE

RepairDuplicateIds ==
    /\ phase = "repair_ids"
    /\ ~IdsUnique(declaredId)
    /\ declaredId' =
        IF BuggyDuplicateIds THEN DuplicateIds
        ELSE IF BuggyDropAdvancedDuplicate THEN DroppedAdvancedIds
             ELSE RecoveredIds
    /\ duplicateRepaired' = TRUE
    /\ phase' = "idle"
    /\ UNCHANGED <<loadPreference, latchedTarget, planTarget, replicas, hadDuplicate,
                  expandedPeers, finalPeers, plannerDesired, proofAccepted,
                  proofWasValid, proofStore, sourcePresent>>

SkipDuplicateRepair ==
    /\ phase = "repair_ids"
    /\ IdsUnique(declaredId)
    /\ phase' = "idle"
    /\ duplicateRepaired' = TRUE
    /\ UNCHANGED <<loadPreference, latchedTarget, planTarget, replicas, hadDuplicate,
                  declaredId, expandedPeers, finalPeers, plannerDesired,
                  proofAccepted, proofWasValid, proofStore, sourcePresent>>

ChosenTarget == IF BuggyLoadSensitive THEN loadPreference ELSE "D"

PlanRepair ==
    /\ phase = "idle"
    /\ IdsUnique(declaredId)
    /\ phase' = "planned"
    /\ latchedTarget' = ChosenTarget
    /\ planTarget' = ChosenTarget
    /\ UNCHANGED <<loadPreference, replicas, declaredId, hadDuplicate, duplicateRepaired,
                  expandedPeers, finalPeers, plannerDesired, proofAccepted,
                  proofWasValid, proofStore, sourcePresent>>

FlipLoad ==
    /\ phase = "planned"
    /\ loadPreference' = "E"
    /\ UNCHANGED <<phase, latchedTarget, planTarget, replicas, declaredId, hadDuplicate,
                  duplicateRepaired, expandedPeers, finalPeers, plannerDesired,
                  proofAccepted, proofWasValid, proofStore, sourcePresent>>

RetryRepair ==
    /\ phase = "planned"
    /\ loadPreference = "E"
    /\ planTarget' = ChosenTarget
    /\ UNCHANGED <<phase, loadPreference, latchedTarget, replicas, declaredId, hadDuplicate,
                  duplicateRepaired, expandedPeers, finalPeers, plannerDesired,
                  proofAccepted, proofWasValid, proofStore, sourcePresent>>

ApplyExpansion ==
    /\ phase = "planned"
    /\ phase' = "expanded"
    /\ expandedPeers' = ExpandedMembership
    /\ replicas' =
        IF BuggyRenumber THEN RenumberedReplicas(planTarget)
        ELSE IF BuggyCombineRebalance THEN CombinedRepairReplicas(planTarget)
             ELSE CorrectReplicas(planTarget)
    /\ declaredId' = IF BuggyDuplicateIds THEN DuplicateIds ELSE declaredId
    /\ UNCHANGED <<loadPreference, latchedTarget, planTarget, hadDuplicate, duplicateRepaired,
                  finalPeers, plannerDesired, proofAccepted, proofWasValid,
                  proofStore, sourcePresent>>

LatchFinalMembership ==
    /\ phase = "expanded"
    /\ phase' = "awaiting_proof"
    /\ finalPeers' = plannerDesired
    /\ UNCHANGED <<loadPreference, latchedTarget, planTarget, replicas, hadDuplicate,
                  declaredId, duplicateRepaired, expandedPeers, plannerDesired,
                  proofAccepted, proofWasValid, proofStore, sourcePresent>>

PlannerChurn ==
    /\ phase = "awaiting_proof"
    /\ plannerDesired' = ChurnedMembership
    /\ finalPeers' = IF BuggyRelatchFinal THEN ChurnedMembership ELSE finalPeers
    /\ UNCHANGED <<phase, loadPreference, latchedTarget, planTarget, replicas, hadDuplicate,
                  declaredId, duplicateRepaired, expandedPeers, proofAccepted,
                  proofWasValid, proofStore, sourcePresent>>

ObserveMembership(store, reported, isLeader) ==
    /\ phase = "awaiting_proof"
    /\ store \in ExpandedMembership
    /\ ~proofAccepted
    /\ proofAccepted' =
        /\ (store = LiveFinalLeaderStore \/ BuggyAcceptWrongStore)
        /\ (reported = finalPeers \/ BuggyAcceptWrongSet)
        /\ (isLeader \/ BuggyAcceptNonLeader)
    /\ proofWasValid' =
        /\ store = LiveFinalLeaderStore
        /\ reported = finalPeers
        /\ isLeader
    /\ proofStore' = IF proofAccepted' THEN store ELSE "none"
    /\ UNCHANGED <<phase, loadPreference, latchedTarget, planTarget, replicas, hadDuplicate,
                  declaredId, duplicateRepaired, expandedPeers, finalPeers,
                  plannerDesired, sourcePresent>>

ObserveValidLeader ==
    ObserveMembership(LiveFinalLeaderStore, finalPeers, TRUE)

ObserveWrongStore ==
    ObserveMembership("C", finalPeers, TRUE)

ObserveWrongSet ==
    ObserveMembership(LiveFinalLeaderStore, WrongMembership, TRUE)

ObserveNonLeader ==
    ObserveMembership(LiveFinalLeaderStore, finalPeers, FALSE)

RetireSource ==
    /\ phase = "awaiting_proof"
    /\ IF BuggyPrematureRetire
       THEN TRUE
       ELSE /\ proofAccepted
            /\ IF BuggyWaitForAllFollowers
               THEN StaleFinalFollowers = {}
               ELSE TRUE
    /\ sourcePresent' = FALSE
    /\ phase' = "retired"
    /\ UNCHANGED <<loadPreference, latchedTarget, planTarget, replicas, hadDuplicate,
                  declaredId, duplicateRepaired, expandedPeers, finalPeers,
                  plannerDesired, proofAccepted, proofWasValid, proofStore>>

Next ==
    \/ RepairDuplicateIds
    \/ SkipDuplicateRepair
    \/ PlanRepair
    \/ FlipLoad
    \/ RetryRepair
    \/ ApplyExpansion
    \/ LatchFinalMembership
    \/ PlannerChurn
    \/ ObserveValidLeader
    \/ ObserveWrongStore
    \/ ObserveWrongSet
    \/ ObserveNonLeader
    \/ RetireSource

Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(RepairDuplicateIds)
    /\ WF_vars(SkipDuplicateRepair)
    /\ WF_vars(PlanRepair)
    /\ WF_vars(ApplyExpansion)
    /\ WF_vars(LatchFinalMembership)
    /\ WF_vars(ObserveValidLeader)
    /\ WF_vars(RetireSource)

ReplacementTargetLatched ==
    phase \in {"planned", "expanded", "awaiting_proof", "retired"} =>
        planTarget = latchedTarget

SurvivorReplicaIdentityPreserved ==
    phase \in {"expanded", "awaiting_proof", "retired"} =>
        /\ replicas[1] = "A"
        /\ replicas[2] = "B"

MandatoryRepairIsSerialized ==
    phase \in {"expanded", "awaiting_proof", "retired"} =>
        replicas = CorrectReplicas(planTarget)

DeclaredReplicaIdsUnique ==
    phase # "repair_ids" => IdsUnique(declaredId)

AdvancedDuplicateIsRetained ==
    hadDuplicate /\ duplicateRepaired => declaredId[3] = 2

PlacementCountPreserved ==
    Cardinality(DOMAIN replicas) = 3

FinalMembershipIsLatched ==
    phase \in {"awaiting_proof", "retired"} => finalPeers = FinalMembership

AcceptedProofIsAuthoritative ==
    proofAccepted => proofWasValid /\ proofStore = LiveFinalLeaderStore

SourceRetirementRequiresFinalLeaderProof ==
    ~sourcePresent => proofAccepted /\ proofWasValid

RepairEventuallyApplies == <>(phase \in {"expanded", "awaiting_proof", "retired"})
SourceEventuallyRetires == <>(phase = "retired")

=============================================================================
