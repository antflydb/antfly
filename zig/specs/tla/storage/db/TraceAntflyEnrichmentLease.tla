---------------------- MODULE TraceAntflyEnrichmentLease ---------------------
(*
  Validates implementation-emitted enrichment worker observations. Publishing
  generated artifacts and advancing the applied checkpoint both require the
  durable lease record to still name this owner and remain unexpired.
*)

EXTENDS Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile == IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./enrichment-lease.ndjson"
TraceLog == ndJsonDeserialize(JsonFile)

VARIABLES l, applied, target
vars == <<l, applied, target>>

Init == /\ l = 1 /\ applied = 0 /\ target = 0
event == TraceLog[l].event
facts == event.facts

Observe ==
    /\ event.name \in {
        "AcquireLease", "CollectPending", "PublishGenerated",
        "AdvanceApplied", "RetryTransient", "FatalWorkerFailure",
        "IsolateRequestFailure"
       }
    /\ facts.appliedSequence <= facts.targetSequence
    /\ event.name \in {"PublishGenerated", "AdvanceApplied"} => facts.leaseValid
    /\ applied' = facts.appliedSequence
    /\ target' = facts.targetSequence

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ Observe
    /\ l' = l + 1

Spec == Init /\ [][TraceNext]_vars
TypeOK == /\ l \in Nat /\ applied \in Nat /\ target \in Nat
WatermarksOrdered == applied <= target
TraceMatched == [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
