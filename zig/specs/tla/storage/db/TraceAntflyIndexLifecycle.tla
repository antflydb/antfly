---------------------- MODULE TraceAntflyIndexLifecycle ---------------------
(*
  Validates implementation-emitted managed index admission and repair events.
  In particular, every durable generation request must publish scheduler work,
  and no worker may be admitted before that publication. A trace captured after
  restart may begin at a persisted "detected" repair intent; that event is the
  durable replay boundary for request and queue events emitted by the prior
  process.
*)

EXTENDS FiniteSets, Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile == IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./index.ndjson"
TraceLog == ndJsonDeserialize(JsonFile)

VARIABLES l, requested, queued, admitted, completed
vars == <<l, requested, queued, admitted, completed>>

Init ==
    /\ l = 1
    /\ requested = {}
    /\ queued = {}
    /\ admitted = {}
    /\ completed = {}

event == TraceLog[l].event
facts == event.facts
generation == <<facts.indexName, facts.configHash>>

RequestGeneration ==
    /\ event.name = "RequestGeneration"
    /\ facts.durableWork
    /\ requested' = requested \cup {generation}
    /\ UNCHANGED <<queued, admitted, completed>>

QueueDurableWork ==
    /\ event.name = "QueueDurableWork"
    /\ generation \in requested
    /\ queued' = queued \cup {generation}
    /\ UNCHANGED <<requested, admitted, completed>>

ResumePersistedIntent ==
    /\ event.name = "PersistIntentPhase"
    /\ facts.phase = "detected"
    /\ facts.durableWork
    /\ requested' = requested \cup {generation}
    /\ queued' = queued \cup {generation}
    /\ UNCHANGED <<admitted, completed>>

PersistIntent ==
    /\ event.name = "PersistIntentPhase"
    /\ generation \in requested
    /\ UNCHANGED <<requested, queued, admitted, completed>>

AdmitWorker ==
    /\ event.name = "AdmitWorker"
    /\ generation \in queued
    /\ facts.workerAdmitted
    /\ admitted' = admitted \cup {generation}
    /\ UNCHANGED <<requested, queued, completed>>

WorkerDeferred ==
    /\ event.name \in {"WorkerDeferred", "FailBuild"}
    /\ generation \in queued
    /\ UNCHANGED <<requested, queued, admitted, completed>>

SwapGeneration ==
    /\ event.name = "SwapGeneration"
    /\ generation \in admitted
    /\ UNCHANGED <<requested, queued, admitted, completed>>

ObserveReady ==
    /\ event.name = "ObserveReady"
    /\ generation \in queued
    /\ completed' = completed \cup {generation}
    /\ UNCHANGED <<requested, queued, admitted>>

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ \/ RequestGeneration
       \/ QueueDurableWork
       \/ ResumePersistedIntent
       \/ PersistIntent
       \/ AdmitWorker
       \/ WorkerDeferred
       \/ SwapGeneration
       \/ ObserveReady
    /\ l' = l + 1

Spec == Init /\ [][TraceNext]_vars
TypeOK ==
    /\ l \in Nat
    /\ requested \subseteq (STRING \X STRING)
    /\ queued \subseteq requested
    /\ admitted \subseteq queued
    /\ completed \subseteq queued

TraceMatched == [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))
EveryRequestedGenerationHasDurableWork ==
    [](l > Len(TraceLog) => requested \subseteq queued)

=============================================================================
