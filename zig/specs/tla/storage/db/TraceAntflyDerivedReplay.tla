----------------------- MODULE TraceAntflyDerivedReplay ----------------------
(*
  Validates implementation-emitted primary-store replay observations:
  monotone watermarks, honest scan accounting, and no advancement beyond the
  worker's observed target. Both the hint-lane fast path and replay-all
  fallback are explicit events.
*)

EXTENDS Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile == IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./derived-replay.ndjson"
TraceLog == ndJsonDeserialize(JsonFile)

VARIABLES l, active, applied, target
vars == <<l, active, applied, target>>

Init ==
    /\ l = 1
    /\ active = FALSE
    /\ applied = 0
    /\ target = 0

event == TraceLog[l].event
facts == event.facts

ObserveTarget ==
    /\ event.name = "ObserveTarget"
    /\ facts.appliedSequence <= facts.targetSequence
    /\ applied' = facts.appliedSequence
    /\ target' = IF facts.targetSequence = 0 THEN target ELSE facts.targetSequence
    /\ UNCHANGED active

BeginCatchUp ==
    /\ event.name = "BeginCatchUp"
    /\ ~active
    /\ facts.targetSequence = 0 \/ facts.appliedSequence <= facts.targetSequence
    /\ active' = TRUE
    /\ applied' = facts.appliedSequence
    /\ target' = IF facts.targetSequence = 0 THEN target ELSE facts.targetSequence

Scan ==
    /\ event.name \in {"HintLaneScan", "FallbackScan"}
    /\ facts.matchedEntries <= facts.scannedEntries
    /\ facts.filteredEntries <= facts.scannedEntries
    /\ event.name = "FallbackScan" <=> facts.fallbackUsed
    /\ UNCHANGED <<active, applied, target>>

FinishCatchUp ==
    /\ event.name = "FinishCatchUp"
    /\ active
    /\ facts.appliedSequence >= applied
    /\ target = 0 \/ facts.appliedSequence <= target
    /\ active' = FALSE
    /\ applied' = facts.appliedSequence
    /\ UNCHANGED target

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ ObserveTarget \/ BeginCatchUp \/ Scan \/ FinishCatchUp
    /\ l' = l + 1

Spec == Init /\ [][TraceNext]_vars
TypeOK == /\ l \in Nat /\ active \in BOOLEAN /\ applied \in Nat /\ target \in Nat
WatermarksOrdered == target = 0 \/ applied <= target
TraceMatched == [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
