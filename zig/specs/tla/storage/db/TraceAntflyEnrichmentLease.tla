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

VARIABLES l, applied, target, denialCooling
vars == <<l, applied, target, denialCooling>>

Init == /\ l = 1 /\ applied = 0 /\ target = 0 /\ denialCooling = FALSE
event == TraceLog[l].event
facts == event.facts

ObserveWorker ==
    /\ event.name \in {
        "AcquireLease", "CollectPending", "PublishGenerated",
        "AdvanceApplied", "RetryTransient", "FatalWorkerFailure",
        "IsolateRequestFailure"
       }
    /\ event.name = "AcquireLease" => ~denialCooling
    /\ facts.appliedSequence <= facts.targetSequence
    /\ event.name \in {"PublishGenerated", "AdvanceApplied"} => facts.leaseValid
    /\ applied' = facts.appliedSequence
    /\ target' = facts.targetSequence
    /\ UNCHANGED denialCooling

ObserveLeaseDenied ==
    /\ event.name = "LeaseDenied"
    /\ ~facts.leaseValid
    /\ ~denialCooling
    /\ denialCooling' = TRUE
    /\ applied' = facts.appliedSequence
    /\ target' = facts.targetSequence

ObserveLeaseRetryReady ==
    /\ event.name = "LeaseRetryReady"
    /\ denialCooling
    /\ denialCooling' = FALSE
    /\ applied' = facts.appliedSequence
    /\ target' = facts.targetSequence

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ ObserveWorker \/ ObserveLeaseDenied \/ ObserveLeaseRetryReady
    /\ l' = l + 1

Spec == Init /\ [][TraceNext]_vars
TypeOK == /\ l \in Nat /\ applied \in Nat /\ target \in Nat /\ denialCooling \in BOOLEAN
WatermarksOrdered == applied <= target
TraceMatched == [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
