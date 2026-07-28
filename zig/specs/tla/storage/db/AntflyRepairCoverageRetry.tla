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

------------------------- MODULE AntflyRepairCoverageRetry -----------------------
(*
  Bounded model of dense repair while a recreated generation's source
  artifacts are still catching up.

  The strict coverage check remains a safety fence: an incomplete shadow
  generation cannot activate. Once the replacement incarnation's durable
  outcome tuple proves complete coverage, activation must use that tuple
  rather than a stale name-scoped artifact counter left by delete/recreate.

  BuggyNameScopedCounter matches the smokeout failure: current-generation
  outcome coverage catches up, but activation compares against the stale
  name-scoped counter and records terminal RepairSourceCoverageIncomplete.
*)

EXTENDS Naturals, TLC

CONSTANT BuggyNameScopedCounter

VARIABLES coverageComplete, repairPhase, attemptParity

vars == <<coverageComplete, repairPhase, attemptParity>>

Init ==
    /\ coverageComplete = FALSE
    /\ repairPhase = "building"
    /\ attemptParity = 0

ObserveIncomplete ==
    /\ repairPhase = "building"
    /\ ~coverageComplete
    /\ repairPhase' = "retry_wait"
    /\ UNCHANGED <<coverageComplete, attemptParity>>

CatchUpCoverage ==
    /\ ~coverageComplete
    /\ coverageComplete' = TRUE
    /\ UNCHANGED <<repairPhase, attemptParity>>

RetryRepair ==
    /\ repairPhase = "retry_wait"
    /\ repairPhase' = "building"
    /\ attemptParity' = 1 - attemptParity
    /\ UNCHANGED coverageComplete

ActivateComplete ==
    /\ repairPhase = "building"
    /\ coverageComplete
    /\ repairPhase' =
        IF BuggyNameScopedCounter THEN "terminal" ELSE "ready"
    /\ UNCHANGED <<coverageComplete, attemptParity>>

Next ==
    \/ ObserveIncomplete
    \/ CatchUpCoverage
    \/ RetryRepair
    \/ ActivateComplete

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(CatchUpCoverage)
    /\ WF_vars(RetryRepair)
    /\ WF_vars(ActivateComplete)

TypeOK ==
    /\ coverageComplete \in BOOLEAN
    /\ repairPhase \in {"building", "retry_wait", "ready", "terminal"}
    /\ attemptParity \in 0..1
    /\ BuggyNameScopedCounter \in BOOLEAN

IncompleteGenerationNeverActivates ==
    repairPhase = "ready" => coverageComplete

CoverageCatchUpEventuallyActivates ==
    coverageComplete ~> repairPhase = "ready"

=============================================================================
