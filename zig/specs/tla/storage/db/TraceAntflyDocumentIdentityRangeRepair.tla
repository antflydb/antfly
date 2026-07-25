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

-------------- MODULE TraceAntflyDocumentIdentityRangeRepair --------------
(*
  Fixture-backed trace refinement for AntflyDocumentIdentityRangeRepair.tla.

  These checked-in fixtures cover the DB restore-repair evidence that is hard
  to isolate through the broad DB test target today:

    - strict deferred restore rejects source/target doc identity namespace
      mismatch;
    - incomplete primary import recovery must happen before runtime repair;
    - restore intent clears only after runtime repair completes.

  Each trace line is ndjson:
    {"tag":"doc-identity-range-repair-trace","event":{"name":"...","after":{...}}}
*)

EXTENDS AntflyDocumentIdentityRangeRepair, Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./doc-identity-range-repair-trace.ndjson"

OriginTraceLog ==
    SelectSeq(
        ndJsonDeserialize(JsonFile),
        LAMBDA line:
            "tag" \in DOMAIN line /\ line.tag = "doc-identity-range-repair-trace")

TraceLog ==
    TLCEval(
        IF "MAX_TRACE" \in DOMAIN IOEnv
        THEN SubSeq(OriginTraceLog, 1, atoi(IOEnv.MAX_TRACE))
        ELSE OriginTraceLog)

VARIABLES
    l,
    pl

traceVars == <<l, pl, vars>>

FirstEvent == TraceLog[1].event

InitField(name) ==
    /\ "init" \in DOMAIN FirstEvent
    /\ name \in DOMAIN FirstEvent.init

InitValue(name, default) ==
    IF InitField(name) THEN FirstEvent.init[name] ELSE default

TraceInit ==
    /\ l = 1
    /\ pl = 0
    /\ sourceStatus = InitValue("sourceStatus", "healthyA")
    /\ donorStatus = InitValue("donorStatus", "healthyA")
    /\ receiverStatus = InitValue("receiverStatus", "healthyA")
    /\ destStoredNamespace = InitValue("destStoredNamespace", ExpectedDestNamespace)
    /\ mergeOptIn = InitValue("mergeOptIn", FALSE)
    /\ restoreSourceNamespace = InitValue("restoreSourceNamespace", 1)
    /\ restoreTargetNamespace = InitValue("restoreTargetNamespace", 1)
    /\ splitAccepted = InitValue("splitAccepted", FALSE)
    /\ splitStatusAccepted = InitValue("splitStatusAccepted", FALSE)
    /\ mergeAccepted = InitValue("mergeAccepted", FALSE)
    /\ receiverReassigned = InitValue("receiverReassigned", FALSE)
    /\ strictRestoreAccepted = InitValue("strictRestoreAccepted", FALSE)
    /\ importRecovered = InitValue("importRecovered", FALSE)
    /\ runtimeRepairNeeded = InitValue("runtimeRepairNeeded", FALSE)
    /\ runtimeRepairComplete = InitValue("runtimeRepairComplete", FALSE)
    /\ restoreIntentCleared = InitValue("restoreIntentCleared", FALSE)
    \* Existing fixtures trace one replica. Treat the other placement's
    \* matching progress as already observed at this abstraction boundary.
    /\ expectedArtifact = "bound"
    /\ reportedArtifact = [n \in Replicas |->
        IF n = "n2" THEN "bound" ELSE "none"]
    /\ replicaRepairComplete = {"n2"}
    /\ acceptedMismatchedArtifact = FALSE
    /\ restorePending = TRUE
    /\ groupReady = FALSE
    /\ splitStarted = FALSE

logline == TraceLog[l]
event == logline.event

StepToNextTrace ==
    /\ l' = l + 1
    /\ pl' = l

LoglineIsEvent(name) ==
    /\ l <= Len(TraceLog)
    /\ event.name = name

AfterField(name) ==
    /\ "after" \in DOMAIN event
    /\ name \in DOMAIN event.after

AfterMatches ==
    /\ IF AfterField("sourceStatus") THEN sourceStatus' = event.after.sourceStatus ELSE TRUE
    /\ IF AfterField("donorStatus") THEN donorStatus' = event.after.donorStatus ELSE TRUE
    /\ IF AfterField("receiverStatus") THEN receiverStatus' = event.after.receiverStatus ELSE TRUE
    /\ IF AfterField("destStoredNamespace") THEN destStoredNamespace' = event.after.destStoredNamespace ELSE TRUE
    /\ IF AfterField("mergeOptIn") THEN mergeOptIn' = event.after.mergeOptIn ELSE TRUE
    /\ IF AfterField("restoreSourceNamespace") THEN restoreSourceNamespace' = event.after.restoreSourceNamespace ELSE TRUE
    /\ IF AfterField("restoreTargetNamespace") THEN restoreTargetNamespace' = event.after.restoreTargetNamespace ELSE TRUE
    /\ IF AfterField("splitAccepted") THEN splitAccepted' = event.after.splitAccepted ELSE TRUE
    /\ IF AfterField("splitStatusAccepted") THEN splitStatusAccepted' = event.after.splitStatusAccepted ELSE TRUE
    /\ IF AfterField("mergeAccepted") THEN mergeAccepted' = event.after.mergeAccepted ELSE TRUE
    /\ IF AfterField("receiverReassigned") THEN receiverReassigned' = event.after.receiverReassigned ELSE TRUE
    /\ IF AfterField("strictRestoreAccepted") THEN strictRestoreAccepted' = event.after.strictRestoreAccepted ELSE TRUE
    /\ IF AfterField("importRecovered") THEN importRecovered' = event.after.importRecovered ELSE TRUE
    /\ IF AfterField("runtimeRepairNeeded") THEN runtimeRepairNeeded' = event.after.runtimeRepairNeeded ELSE TRUE
    /\ IF AfterField("runtimeRepairComplete") THEN runtimeRepairComplete' = event.after.runtimeRepairComplete ELSE TRUE
    /\ IF AfterField("restoreIntentCleared") THEN restoreIntentCleared' = event.after.restoreIntentCleared ELSE TRUE

RangeRepairActionFromTrace ==
    \/ /\ LoglineIsEvent("ValidateSplit")
       /\ ValidateSplit
    \/ /\ LoglineIsEvent("ObserveSplitDestinationStatus")
       /\ ObserveSplitDestinationStatus
    \/ /\ LoglineIsEvent("ValidateMerge")
       /\ ValidateMerge
    \/ /\ LoglineIsEvent("ReassignReceiverNamespace")
       /\ ReassignReceiverNamespace
    \/ /\ LoglineIsEvent("StrictDeferredRestore")
       /\ StrictDeferredRestore
    \/ /\ LoglineIsEvent("RecoverIncompleteImport")
       /\ RecoverIncompleteImport
    \/ /\ LoglineIsEvent("RunRuntimeRepair")
       /\ RunRuntimeRepair
    \/ /\ LoglineIsEvent("ClearRestoreIntent")
       /\ ClearRestoreIntent

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ RangeRepairActionFromTrace
    /\ AfterMatches
    /\ StepToNextTrace

TraceSpec == TraceInit /\ [][TraceNext]_traceVars

TraceView == <<vars, l>>

TraceSafety ==
    Safety

\* Violated if TLC cannot consume every line of the fixture.
TraceMatched ==
    [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
