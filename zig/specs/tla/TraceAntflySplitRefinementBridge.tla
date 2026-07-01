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

------------------ MODULE TraceAntflySplitRefinementBridge -------------------
(*
  Trace fixture validator for the split refinement bridge.

  These checked-in fixtures are not live Zig traces yet. They pin representative
  shard-to-DB split event orderings against the same cross-layer contract as
  AntflySplitRefinementBridge.tla:

    - shard fence and cutover must not outrun DB split-delta replay;
    - DB child serving must wait for shard cutover and DB index catch-up;
    - metadata right-range routing must wait for both layers;
    - rollback must not expose child ownership.

  Each trace line is ndjson:
    {"tag":"split-bridge-trace","event":{"name":"...","after":{...}}}
*)

EXTENDS AntflySplitRefinementBridge, Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./split-bridge-trace.ndjson"

OriginTraceLog ==
    SelectSeq(
        ndJsonDeserialize(JsonFile),
        LAMBDA line: "tag" \in DOMAIN line /\ line.tag = "split-bridge-trace")

TraceLog ==
    TLCEval(
        IF "MAX_TRACE" \in DOMAIN IOEnv
        THEN SubSeq(OriginTraceLog, 1, atoi(IOEnv.MAX_TRACE))
        ELSE OriginTraceLog)

VARIABLES
    l,
    pl

traceVars == <<l, pl, vars>>

TraceInit ==
    /\ l = 1
    /\ pl = 0
    /\ Init

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
    /\ IF AfterField("phase") THEN phase' = event.after.phase ELSE TRUE
    /\ IF AfterField("shardFenceSet") THEN shardFenceSet' = event.after.shardFenceSet ELSE TRUE
    /\ IF AfterField("shardFenceSeq") THEN shardFenceSeq' = event.after.shardFenceSeq ELSE TRUE
    /\ IF AfterField("shardCutoverReady") THEN shardCutoverReady' = event.after.shardCutoverReady ELSE TRUE
    /\ IF AfterField("dbDeltaSeq") THEN dbDeltaSeq' = event.after.dbDeltaSeq ELSE TRUE
    /\ IF AfterField("dbReplaySeq") THEN dbReplaySeq' = event.after.dbReplaySeq ELSE TRUE
    /\ IF AfterField("dbTextIndexSeq") THEN dbTextIndexSeq' = event.after.dbTextIndexSeq ELSE TRUE
    /\ IF AfterField("dbSparseIndexSeq") THEN dbSparseIndexSeq' = event.after.dbSparseIndexSeq ELSE TRUE
    /\ IF AfterField("dbGraphIndexSeq") THEN dbGraphIndexSeq' = event.after.dbGraphIndexSeq ELSE TRUE
    /\ IF AfterField("dbChildServing") THEN dbChildServing' = event.after.dbChildServing ELSE TRUE
    /\ IF AfterField("parentAcceptsRight") THEN parentAcceptsRight' = event.after.parentAcceptsRight ELSE TRUE
    /\ IF AfterField("childAcceptsRight") THEN childAcceptsRight' = event.after.childAcceptsRight ELSE TRUE
    /\ IF AfterField("routeRightOwner") THEN routeRightOwner' = event.after.routeRightOwner ELSE TRUE
    /\ IF AfterField("staleFenceCompletion") THEN staleFenceCompletion' = event.after.staleFenceCompletion ELSE TRUE

BridgeActionFromTrace ==
    \/ /\ LoglineIsEvent("BeginSplit")
       /\ BeginSplit
    \/ /\ LoglineIsEvent("ParentRightWrite")
       /\ ParentRightWriteDuringSplit
    \/ /\ LoglineIsEvent("ReplayDelta")
       /\ ReplayDelta
    \/ /\ LoglineIsEvent("BuildTextIndex")
       /\ BuildTextIndex
    \/ /\ LoglineIsEvent("BuildSparseIndex")
       /\ BuildSparseIndex
    \/ /\ LoglineIsEvent("BuildGraphIndex")
       /\ BuildGraphIndex
    \/ /\ LoglineIsEvent("SetShardFence")
       /\ SetShardFence
    \/ /\ LoglineIsEvent("CompleteShardCutover")
       /\ CompleteShardCutover
    \/ /\ LoglineIsEvent("PublishDbChildServing")
       /\ PublishDbChildServing
    \/ /\ LoglineIsEvent("RouteMetadataToChild")
       /\ RouteMetadataToChild
    \/ /\ LoglineIsEvent("Rollback")
       /\ Rollback

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ BridgeActionFromTrace
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
