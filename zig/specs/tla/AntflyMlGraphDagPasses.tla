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

-------------------------- MODULE AntflyMlGraphDagPasses --------------------------
(*
  Bounded arbitrary-DAG model for ML graph CSE/DCE remapping.

  This complements AntflyMlGraphPasses.tla. That model checks a representative
  export/runtime graph. This model instead enumerates several small topological
  DAG shapes and checks the lower-level rebuild/id_map contracts used by:

    - lib/ml/src/graph/passes/cse.zig
    - lib/ml/src/graph/passes/dce.zig
    - lib/ml/src/graph/passes/pipeline.zig

  Concrete contracts modeled:
    - CSE does not deduplicate parameter or constant nodes.
    - CSE redirects duplicate op nodes to an earlier equal expression.
    - Consumers, outputs, and parameter lists are remapped through redirects.
    - DCE keeps exactly nodes reachable from remapped outputs.
    - DCE id_map is compact and preserves original topological order.
    - Final compacted graphs contain no dangling inputs.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    BuggyCseMissDuplicate,
    BuggyCseNoConsumerRemap,
    BuggyDceDropReachable,
    BuggyDceNonTopoMap

Nodes == 0..4
Dropped == 5
MapTargets == 0..5
Kinds == {"param", "const", "opA", "opB", "opC"}

VARIABLES
    phase,
    kind,
    inputs,
    outputs,
    parameters,
    cseMap,
    postInputs,
    postOutputs,
    postParameters,
    dceLive,
    dceMap,
    finalInputs,
    finalOutputs,
    finalParameters

vars ==
    <<phase, kind, inputs, outputs, parameters, cseMap, postInputs,
      postOutputs, postParameters, dceLive, dceMap, finalInputs,
      finalOutputs, finalParameters>>

IdentityMap == [n \in Nodes |-> n]

ShapeDuplicateBranch ==
    /\ kind =
        [n \in Nodes |->
            CASE n = 0 -> "param"
              [] n = 1 -> "const"
              [] n = 2 -> "opA"
              [] n = 3 -> "opA"
              [] OTHER -> "opB"]
    /\ inputs =
        [n \in Nodes |->
            CASE n \in {0, 1} -> {}
              [] n \in {2, 3} -> {0, 1}
              [] OTHER -> {3}]
    /\ outputs = {4}
    /\ parameters = {0}

ShapeIndependentBranches ==
    /\ kind =
        [n \in Nodes |->
            CASE n \in {0, 1} -> "param"
              [] n \in {2, 3} -> "opA"
              [] OTHER -> "opB"]
    /\ inputs =
        [n \in Nodes |->
            CASE n \in {0, 1} -> {}
              [] n = 2 -> {0}
              [] n = 3 -> {1}
              [] OTHER -> {2, 3}]
    /\ outputs = {4}
    /\ parameters = {0, 1}

ShapeDeadBranch ==
    /\ kind =
        [n \in Nodes |->
            CASE n = 0 -> "param"
              [] n = 1 -> "const"
              [] n = 2 -> "opA"
              [] n = 3 -> "opB"
              [] OTHER -> "opC"]
    /\ inputs =
        [n \in Nodes |->
            CASE n \in {0, 1} -> {}
              [] n = 2 -> {0}
              [] n = 3 -> {1}
              [] OTHER -> {2}]
    /\ outputs = {4}
    /\ parameters = {0}

Init ==
    /\ phase = "init"
    /\ (ShapeDuplicateBranch \/ ShapeIndependentBranches \/ ShapeDeadBranch)
    /\ cseMap = IdentityMap
    /\ postInputs = inputs
    /\ postOutputs = outputs
    /\ postParameters = parameters
    /\ dceLive = {}
    /\ dceMap = [n \in Nodes |-> Dropped]
    /\ finalInputs = [n \in Nodes |-> {}]
    /\ finalOutputs = {}
    /\ finalParameters = {}

NonData(n) == kind[n] \notin {"param", "const"}

Earlier(n) == {m \in Nodes: m < n}

ResolvedSet(map, S) == {map[x] : x \in S}

SameExpr(map, a, b) ==
    /\ kind[a] = kind[b]
    /\ ResolvedSet(map, inputs[a]) = ResolvedSet(map, inputs[b])

StructurallyValidCseMap(map) ==
    /\ map \in [Nodes -> Nodes]
    /\ \A n \in Nodes:
        /\ map[n] \in 0..n
        /\ map[map[n]] = map[n]
        /\ IF NonData(n)
           THEN SameExpr(map, map[n], n)
           ELSE map[n] = n

EliminatesAvailableDuplicates(map) ==
    \A n \in Nodes:
        NonData(n) /\ (\E m \in Earlier(n): SameExpr(map, m, n)) =>
            map[n] < n

GoodCseMap(map) ==
    /\ StructurallyValidCseMap(map)
    /\ EliminatesAvailableDuplicates(map)

StepReach(S, deps) == S \cup UNION {deps[n] : n \in S}

ReachableFrom(roots, deps) ==
    LET s1 == StepReach(roots, deps) IN
    LET s2 == StepReach(s1, deps) IN
    LET s3 == StepReach(s2, deps) IN
    LET s4 == StepReach(s3, deps) IN
        StepReach(s4, deps)

Rank(n, S) == Cardinality({m \in S: m < n})

CompactTopoMap(S) ==
    [n \in Nodes |-> IF n \in S THEN Rank(n, S) ELSE Dropped]

BadNonTopoMap(S) ==
    [n \in Nodes |->
        IF n = 0 /\ 1 \in S THEN 1
        ELSE IF n = 1 /\ 0 \in S THEN 0
        ELSE IF n \in S THEN Rank(n, S)
        ELSE Dropped]

RunCSE ==
    /\ phase = "init"
    /\ phase' = "cse"
    /\ LET chosen ==
            IF BuggyCseMissDuplicate
            THEN IdentityMap
            ELSE CHOOSE map \in [Nodes -> Nodes]: GoodCseMap(map)
       IN
       /\ cseMap' = chosen
       /\ postInputs' =
            IF BuggyCseNoConsumerRemap
            THEN inputs
            ELSE [n \in Nodes |-> ResolvedSet(chosen, inputs[n])]
       /\ postOutputs' = {chosen[o] : o \in outputs}
       /\ postParameters' = {chosen[p] : p \in parameters}
    /\ UNCHANGED <<kind, inputs, outputs, parameters, dceLive, dceMap,
                  finalInputs, finalOutputs, finalParameters>>

RunDCE ==
    /\ phase = "cse"
    /\ phase' = "dce"
    /\ LET reachable == ReachableFrom(postOutputs, postInputs) IN
       LET live ==
            IF /\ BuggyDceDropReachable
               /\ parameters \cap reachable # {}
            THEN reachable \ {CHOOSE p \in parameters \cap reachable: TRUE}
            ELSE reachable
       IN
       LET map ==
            IF BuggyDceNonTopoMap THEN BadNonTopoMap(live) ELSE CompactTopoMap(live)
       IN
       /\ dceLive' = live
       /\ dceMap' = map
       /\ finalInputs' =
            [n \in Nodes |->
                IF n \in live
                THEN {map[x] : x \in postInputs[n]}
                ELSE {}]
       /\ finalOutputs' = {map[o] : o \in postOutputs}
       /\ finalParameters' = {map[p] : p \in postParameters \cap live}
    /\ UNCHANGED <<kind, inputs, outputs, parameters, cseMap, postInputs,
                  postOutputs, postParameters>>

Next == RunCSE \/ RunDCE

Spec == Init /\ [][Next]_vars

TopologicalInputs(deps) ==
    \A n \in Nodes: deps[n] \subseteq Earlier(n)

TypeOK ==
    /\ phase \in {"init", "cse", "dce"}
    /\ kind \in [Nodes -> Kinds]
    /\ inputs \in [Nodes -> SUBSET Nodes]
    /\ TopologicalInputs(inputs)
    /\ outputs \subseteq Nodes
    /\ parameters \subseteq Nodes
    /\ parameters = {n \in Nodes: kind[n] = "param"}
    /\ cseMap \in [Nodes -> Nodes]
    /\ postInputs \in [Nodes -> SUBSET Nodes]
    /\ postOutputs \subseteq Nodes
    /\ postParameters \subseteq Nodes
    /\ dceLive \subseteq Nodes
    /\ dceMap \in [Nodes -> MapTargets]
    /\ finalInputs \in [Nodes -> SUBSET MapTargets]
    /\ finalOutputs \subseteq MapTargets
    /\ finalParameters \subseteq MapTargets

CseRemapsConsumersOutputsAndParameters ==
    phase \in {"cse", "dce"} =>
        /\ postInputs = [n \in Nodes |-> ResolvedSet(cseMap, inputs[n])]
        /\ postOutputs = {cseMap[o] : o \in outputs}
        /\ postParameters = {cseMap[p] : p \in parameters}

CseMapIsSemantic ==
    phase \in {"cse", "dce"} =>
        /\ StructurallyValidCseMap(cseMap)
        /\ EliminatesAvailableDuplicates(cseMap)

CseKeepsDataNodesDistinct ==
    phase \in {"cse", "dce"} =>
        \A n \in Nodes: ~NonData(n) => cseMap[n] = n

CseGraphReferencesReachableNodes ==
    phase \in {"cse", "dce"} =>
        LET reachable == ReachableFrom(postOutputs, postInputs) IN
            /\ postOutputs \subseteq reachable
            /\ \A n \in reachable: postInputs[n] \subseteq reachable
            /\ postParameters \subseteq reachable

DceKeepsExactlyReachableNodes ==
    phase = "dce" =>
        dceLive = ReachableFrom(postOutputs, postInputs)

DceMapIsCompactTopological ==
    phase = "dce" =>
        /\ dceMap = CompactTopoMap(dceLive)
        /\ \A i, j \in dceLive: i < j => dceMap[i] < dceMap[j]

FinalGraphReferencesValid ==
    phase = "dce" =>
        LET finalIds == {dceMap[n] : n \in dceLive} IN
            /\ finalOutputs \subseteq finalIds
            /\ finalParameters \subseteq finalIds
            /\ \A n \in dceLive: finalInputs[n] \subseteq finalIds

ParametersPreservedWhenReachable ==
    phase = "dce" =>
        finalParameters = {dceMap[p] : p \in postParameters \cap dceLive}

Safety ==
    /\ TypeOK
    /\ CseRemapsConsumersOutputsAndParameters
    /\ CseMapIsSemantic
    /\ CseKeepsDataNodesDistinct
    /\ CseGraphReferencesReachableNodes
    /\ DceKeepsExactlyReachableNodes
    /\ DceMapIsCompactTopological
    /\ FinalGraphReferencesValid
    /\ ParametersPreservedWhenReachable

=============================================================================
