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

-------------------------- MODULE AntflyMlGraphPasses --------------------------
(*
  Bounded model of the ML graph optimization/export/runtime boundary.

  The Zig graph pipeline runs const-folding, CSE, fuse, DCE, and a final CSE to a
  fixed point. The partition exporter then has to preserve enough fused-op
  decomposition closure for lowering, and graph runtime gates must fail closed
  when native/PJRT partitioning falls back.

  This model deliberately keeps one representative graph shape instead of
  enumerating arbitrary DAGs:

    input -> commonA/commonB -> linearA/linearB -> out
    input -> dead

  CSE may merge commonB into commonA, but must not collapse parameter or
  constant identity. Fuse replaces the two linear nodes with a fused node that
  has a primitive lowering alternate. DCE can either keep that alternate for an
  export-bound graph or prune it and clear the vjp alternate, matching the
  implementation's vjp-only cleanup behavior.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    BuggyDanglingCse,
    BuggyParameterDedup,
    BuggyMissingLowerClosure,
    BuggyFallbackRuntime,
    BuggyPartialPublish

None == "none"

Nodes ==
    {"input", "cache", "paramA", "paramB", "constA", "constB",
     "commonA", "commonB", "linearA", "linearB", "fused", "primitive",
     "dead", "out"}

NodePairs == Nodes \X Nodes
VjpTarget == Nodes \cup {None}
Params == {"paramA", "paramB"}
Consts == {"constA", "constB"}

InitialDeps ==
    {<<"commonA", "input">>,
     <<"commonB", "input">>,
     <<"linearA", "commonA">>,
     <<"linearA", "paramA">>,
     <<"linearA", "constA">>,
     <<"linearB", "commonB">>,
     <<"linearB", "paramB">>,
     <<"linearB", "constB">>,
     <<"out", "linearA">>,
     <<"out", "linearB">>,
     <<"dead", "input">>}

CseDeps ==
    (InitialDeps \ {<<"commonB", "input">>, <<"linearB", "commonB">>})
        \cup {<<"linearB", "commonA">>}

Touches(S) == {p \in NodePairs: p[1] \in S \/ p[2] \in S}

FuseDeps ==
    (CseDeps \ Touches({"linearA", "linearB"}))
        \cup {<<"fused", "commonA">>,
              <<"fused", "paramA">>,
              <<"fused", "paramB">>,
              <<"fused", "constA">>,
              <<"fused", "constB">>,
              <<"primitive", "commonA">>,
              <<"primitive", "paramA">>,
              <<"primitive", "paramB">>,
              <<"primitive", "constA">>,
              <<"primitive", "constB">>,
              <<"out", "fused">>}

DceDepsKeepClosure == FuseDeps \ {<<"dead", "input">>}
DceDepsPruneVjp == DceDepsKeepClosure \ Touches({"primitive"})

VARIABLES
    live,
    deps,
    outputs,
    vjpAlt,
    constFoldDone,
    cseDone,
    fuseDone,
    dceDone,
    passFailed,
    partialVisible,
    exported,
    exportLive,
    exportDeps,
    exportVjpAlt,
    runtimeInputs,
    fallbackPartition,
    requireNoFallback,
    runtimePublished

vars ==
    <<live, deps, outputs, vjpAlt,
      constFoldDone, cseDone, fuseDone, dceDone,
      passFailed, partialVisible,
      exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
      fallbackPartition, requireNoFallback, runtimePublished>>

Init ==
    /\ live = Nodes \ {"fused", "primitive"}
    /\ deps = InitialDeps
    /\ outputs = {"out"}
    /\ vjpAlt = [n \in Nodes |-> None]
    /\ constFoldDone = FALSE
    /\ cseDone = FALSE
    /\ fuseDone = FALSE
    /\ dceDone = FALSE
    /\ passFailed = FALSE
    /\ partialVisible = FALSE
    /\ exported = FALSE
    /\ exportLive = {}
    /\ exportDeps = {}
    /\ exportVjpAlt = [n \in Nodes |-> None]
    /\ runtimeInputs = {}
    /\ fallbackPartition = TRUE
    /\ requireNoFallback = TRUE
    /\ runtimePublished = FALSE

RunConstFold ==
    /\ ~constFoldDone
    /\ constFoldDone' = TRUE
    /\ UNCHANGED <<live, deps, outputs, vjpAlt,
                  cseDone, fuseDone, dceDone,
                  passFailed, partialVisible,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  fallbackPartition, requireNoFallback, runtimePublished>>

RunCSE ==
    /\ constFoldDone
    /\ ~cseDone
    /\ cseDone' = TRUE
    /\ IF BuggyDanglingCse THEN
          /\ live' = live \ {"commonB"}
          /\ deps' = deps
       ELSE IF BuggyParameterDedup THEN
          /\ live' = live \ {"commonB", "paramB", "constB"}
          /\ deps' =
              ((deps \ {<<"commonB", "input">>,
                        <<"linearB", "commonB">>,
                        <<"linearB", "paramB">>,
                        <<"linearB", "constB">>})
                    \cup {<<"linearB", "commonA">>,
                          <<"linearB", "paramA">>,
                          <<"linearB", "constA">>})
       ELSE
          /\ live' = live \ {"commonB"}
          /\ deps' = CseDeps
    /\ UNCHANGED <<outputs, vjpAlt,
                  constFoldDone, fuseDone, dceDone,
                  passFailed, partialVisible,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  fallbackPartition, requireNoFallback, runtimePublished>>

RunFuse ==
    /\ constFoldDone
    /\ cseDone
    /\ ~fuseDone
    /\ fuseDone' = TRUE
    /\ live' = (live \ {"linearA", "linearB"}) \cup {"fused", "primitive"}
    /\ deps' = FuseDeps
    /\ vjpAlt' = [vjpAlt EXCEPT !["fused"] = "primitive"]
    /\ UNCHANGED <<outputs,
                  constFoldDone, cseDone, dceDone,
                  passFailed, partialVisible,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  fallbackPartition, requireNoFallback, runtimePublished>>

RunDCEKeepLoweringClosure ==
    /\ fuseDone
    /\ ~dceDone
    /\ dceDone' = TRUE
    /\ live' = live \ {"dead"}
    /\ deps' = DceDepsKeepClosure
    /\ UNCHANGED <<outputs, vjpAlt,
                  constFoldDone, cseDone, fuseDone,
                  passFailed, partialVisible,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  fallbackPartition, requireNoFallback, runtimePublished>>

RunDCEPruneVjpOnlyClosure ==
    /\ fuseDone
    /\ ~dceDone
    /\ dceDone' = TRUE
    /\ live' = (live \ {"dead", "primitive"})
    /\ deps' = DceDepsPruneVjp
    /\ vjpAlt' = [vjpAlt EXCEPT !["fused"] = None]
    /\ UNCHANGED <<outputs,
                  constFoldDone, cseDone, fuseDone,
                  passFailed, partialVisible,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  fallbackPartition, requireNoFallback, runtimePublished>>

FailPass ==
    /\ ~passFailed
    /\ ~exported
    /\ passFailed' = TRUE
    /\ partialVisible' = BuggyPartialPublish
    /\ UNCHANGED <<live, deps, outputs, vjpAlt,
                  constFoldDone, cseDone, fuseDone, dceDone,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  fallbackPartition, requireNoFallback, runtimePublished>>

ExportPartition ==
    /\ dceDone
    /\ ~exported
    /\ ~partialVisible
    /\ exported' = TRUE
    /\ exportLive' =
        IF /\ BuggyMissingLowerClosure
           /\ vjpAlt["fused"] # None
        THEN live \ {vjpAlt["fused"]}
        ELSE live
    /\ exportDeps' =
        IF /\ BuggyMissingLowerClosure
           /\ vjpAlt["fused"] # None
        THEN deps \ Touches({vjpAlt["fused"]})
        ELSE deps
    /\ exportVjpAlt' = vjpAlt
    /\ runtimeInputs' = {"cache"}
    /\ UNCHANGED <<live, deps, outputs, vjpAlt,
                  constFoldDone, cseDone, fuseDone, dceDone,
                  passFailed, partialVisible,
                  fallbackPartition, requireNoFallback, runtimePublished>>

ClearFallbackPartition ==
    /\ fallbackPartition
    /\ fallbackPartition' = FALSE
    /\ UNCHANGED <<live, deps, outputs, vjpAlt,
                  constFoldDone, cseDone, fuseDone, dceDone,
                  passFailed, partialVisible,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  requireNoFallback, runtimePublished>>

AttachRuntime ==
    /\ exported
    /\ ~runtimePublished
    /\ IF BuggyFallbackRuntime THEN
          TRUE
       ELSE
          ~(requireNoFallback /\ fallbackPartition)
    /\ runtimePublished' = TRUE
    /\ UNCHANGED <<live, deps, outputs, vjpAlt,
                  constFoldDone, cseDone, fuseDone, dceDone,
                  passFailed, partialVisible,
                  exported, exportLive, exportDeps, exportVjpAlt, runtimeInputs,
                  fallbackPartition, requireNoFallback>>

Next ==
    \/ RunConstFold
    \/ RunCSE
    \/ RunFuse
    \/ RunDCEKeepLoweringClosure
    \/ RunDCEPruneVjpOnlyClosure
    \/ FailPass
    \/ ExportPartition
    \/ ClearFallbackPartition
    \/ AttachRuntime

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ live \subseteq Nodes
    /\ deps \subseteq NodePairs
    /\ outputs \subseteq Nodes
    /\ vjpAlt \in [Nodes -> VjpTarget]
    /\ constFoldDone \in BOOLEAN
    /\ cseDone \in BOOLEAN
    /\ fuseDone \in BOOLEAN
    /\ dceDone \in BOOLEAN
    /\ passFailed \in BOOLEAN
    /\ partialVisible \in BOOLEAN
    /\ exported \in BOOLEAN
    /\ exportLive \subseteq Nodes
    /\ exportDeps \subseteq NodePairs
    /\ exportVjpAlt \in [Nodes -> VjpTarget]
    /\ runtimeInputs \subseteq Nodes
    /\ fallbackPartition \in BOOLEAN
    /\ requireNoFallback \in BOOLEAN
    /\ runtimePublished \in BOOLEAN

CurrentGraphReferencesValid ==
    /\ outputs \subseteq live
    /\ \A p \in deps: /\ p[1] \in live /\ p[2] \in live
    /\ \A n \in live: vjpAlt[n] # None => vjpAlt[n] \in live

ExportedGraphReferencesValid ==
    exported =>
        /\ outputs \subseteq exportLive
        /\ \A p \in exportDeps: /\ p[1] \in exportLive /\ p[2] \in exportLive
        /\ \A n \in exportLive:
              exportVjpAlt[n] # None => exportVjpAlt[n] \in exportLive

OutputNodePreserved ==
    /\ outputs = {"out"}
    /\ "out" \in live
    /\ exported => "out" \in exportLive

PassOrderRespected ==
    /\ cseDone => constFoldDone
    /\ fuseDone => cseDone
    /\ dceDone => fuseDone
    /\ exported => dceDone
    /\ runtimePublished => exported

ParameterAndConstantIdentityPreserved ==
    /\ Params \subseteq live
    /\ Consts \subseteq live
    /\ exported => /\ Params \subseteq exportLive /\ Consts \subseteq exportLive

FailedPassOutputNotVisible ==
    passFailed => ~partialVisible

ExternalPartitionInputsMaterialized ==
    exported => "cache" \in runtimeInputs

RuntimeGateFailsClosed ==
    runtimePublished => ~(requireNoFallback /\ fallbackPartition)

Safety ==
    /\ TypeOK
    /\ CurrentGraphReferencesValid
    /\ ExportedGraphReferencesValid
    /\ OutputNodePreserved
    /\ PassOrderRespected
    /\ ParameterAndConstantIdentityPreserved
    /\ FailedPassOutputNotVisible
    /\ ExternalPartitionInputsMaterialized
    /\ RuntimeGateFailsClosed

=============================================================================
