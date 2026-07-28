------------------- MODULE TraceAntflyPlacementReadiness -------------------
(*
  Validates implementation-emitted placement evidence. The model-checking
  contract remains in AntflyPlacementReadiness; this wrapper checks that each
  observed aggregation batch and transition decision refines that contract.
*)

EXTENDS FiniteSets, Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile == IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./placement.ndjson"
TraceLog == ndJsonDeserialize(JsonFile)

VARIABLES l,
          fallbackIndex, fallbackCounts,
          knownIndex, knownCounts, knownFingerprints
vars == <<l,
          fallbackIndex, fallbackCounts,
          knownIndex, knownCounts, knownFingerprints>>

Init ==
    /\ l = 1
    /\ fallbackIndex = 0
    /\ fallbackCounts = {}
    /\ knownIndex = 0
    /\ knownCounts = {}
    /\ knownFingerprints = {}
event == TraceLog[l].event
facts == event.facts

Has(name) == name \in DOMAIN facts
Is(name) == l <= Len(TraceLog) /\ event.name = name

ObserveReport ==
    /\ event.name \in {"ObserveReportState", "ObserveReportReconciler"}
    /\ IF facts.voterCount = 0
       THEN /\ UNCHANGED <<fallbackIndex, fallbackCounts>>
       ELSE IF facts.membershipIndex > fallbackIndex
            THEN /\ fallbackIndex' = facts.membershipIndex
                 /\ fallbackCounts' = {facts.voterCount}
            ELSE IF facts.membershipIndex = fallbackIndex
                 THEN /\ fallbackCounts' = fallbackCounts \cup {facts.voterCount}
                      /\ UNCHANGED fallbackIndex
                 ELSE /\ UNCHANGED <<fallbackIndex, fallbackCounts>>
    /\ IF ~facts.voterSetKnown
       THEN /\ UNCHANGED <<knownIndex, knownCounts, knownFingerprints>>
       ELSE IF facts.membershipIndex > knownIndex \/ Cardinality(knownFingerprints) = 0
            THEN /\ knownIndex' = facts.membershipIndex
                 /\ knownCounts' = {facts.voterCount}
                 /\ knownFingerprints' = {facts.fingerprint}
            ELSE IF facts.membershipIndex = knownIndex
                 THEN /\ knownCounts' = knownCounts \cup {facts.voterCount}
                      /\ knownFingerprints' = knownFingerprints \cup {facts.fingerprint}
                      /\ UNCHANGED knownIndex
                 ELSE /\ UNCHANGED <<knownIndex, knownCounts, knownFingerprints>>

Recompute ==
    /\ event.name \in {"RecomputeEvidenceState", "RecomputeEvidenceReconciler"}
    \* A qualified voter set at least as new as all fallback evidence must
    \* remain authoritative. Unknown reporters may disagree on their fallback
    \* count, but cannot make the resolved voter set unknown (the B1 contract).
    /\ IF Cardinality(knownFingerprints) = 1 /\ knownIndex >= fallbackIndex
       THEN /\ facts.voterCountKnown
            /\ facts.voterSetKnown
            /\ facts.voterCount \in knownCounts
            /\ facts.fingerprint \in knownFingerprints
       ELSE TRUE
    \* With no qualified report, a single fallback count is count evidence,
    \* never exact voter-set evidence.
    /\ IF Cardinality(knownFingerprints) = 0 /\ Cardinality(fallbackCounts) = 1
       THEN /\ facts.voterCountKnown
            /\ ~facts.voterSetKnown
            /\ facts.voterCount \in fallbackCounts
       ELSE TRUE
    \* Conflicting qualified reports fail closed unless a separately resolved
    \* Raft leader supplies the authoritative current report.
    /\ IF Cardinality(knownFingerprints) > 1 /\ ~facts.leaderKnown
       THEN /\ ~facts.voterCountKnown
            /\ ~facts.voterSetKnown
       ELSE TRUE
    /\ fallbackIndex' = 0
    /\ fallbackCounts' = {}
    /\ knownIndex' = 0
    /\ knownCounts' = {}
    /\ knownFingerprints' = {}

TransitionDecision ==
    /\ event.name \in {"StartTransition", "RejectTransition"}
    /\ IF event.name = "StartTransition"
       THEN /\ facts.stablePlacement
            /\ facts.leaderKnown
            /\ facts.leaderPlaced
            /\ facts.voterCountKnown
            /\ facts.voterSetKnown
            /\ ~facts.ambiguous
            /\ ~facts.jointConsensus
            /\ facts.voterCount = facts.expectedVoters
            /\ facts.healthyVoterReports = facts.expectedVoters
       ELSE ~facts.stablePlacement
    /\ UNCHANGED <<fallbackIndex, fallbackCounts,
                   knownIndex, knownCounts, knownFingerprints>>

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ \/ ObserveReport
       \/ Recompute
       \/ TransitionDecision
    /\ l' = l + 1

Spec == Init /\ [][TraceNext]_vars
TypeOK ==
    /\ l \in Nat
    /\ fallbackIndex \in Nat
    /\ fallbackCounts \subseteq Nat
    /\ knownIndex \in Nat
    /\ knownCounts \subseteq Nat
    /\ knownFingerprints \subseteq STRING
TraceMatched == [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
