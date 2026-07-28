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

----------------------- MODULE AntflyRepairArtifactRecovery ---------------------
(*
  A coverage-failed dense repair candidate cannot improve by being replayed:
  the missing/corrupt source artifact is below its pinned build floor. Recovery
  therefore needs all three operations observed in the smokeout prototype:

    1. reprocess managed artifacts from primary source documents; and
    2. drain that durable request in the bounded repair owner's foreground;
    3. discard the insufficient inactive candidate before rebuilding.

  After activation, startup planning must use the same current-generation
  coverage count as shadow validation. Reusing the stale name-scoped counter
  rediscovers the repaired generation as surplus and makes startup churn.

  The four mutant constants independently remove those obligations. Any
  mutant leaves automatic repair unable to publish and retain a stable
  complete generation.
*)

EXTENDS TLC

CONSTANTS
    BuggySkipArtifactRecovery,
    BuggyLeaveRecoveryUndrained,
    BuggyReuseFailedCandidate,
    BuggyPlannerUsesNameCounter

VARIABLES
    artifactQueued,
    artifactValid,
    candidateExists,
    candidateComplete,
    repairPhase,
    startupPlanChecked,
    startupPlanClean

vars ==
    <<artifactQueued, artifactValid, candidateExists, candidateComplete,
      repairPhase, startupPlanChecked, startupPlanClean>>

Init ==
    /\ artifactQueued = FALSE
    /\ artifactValid = FALSE
    /\ candidateExists = TRUE
    /\ candidateComplete = FALSE
    /\ repairPhase = "building"
    /\ startupPlanChecked = FALSE
    /\ startupPlanClean = FALSE

ObserveCoverageFailure ==
    /\ repairPhase = "building"
    /\ ~candidateComplete
    /\ repairPhase' = "coverage_failed"
    /\ UNCHANGED
        <<artifactQueued, artifactValid, candidateExists, candidateComplete,
          startupPlanChecked, startupPlanClean>>

QueueArtifactRecovery ==
    /\ repairPhase = "coverage_failed"
    /\ ~artifactQueued
    /\ ~BuggySkipArtifactRecovery
    /\ artifactQueued' = TRUE
    /\ UNCHANGED
        <<artifactValid, candidateExists, candidateComplete, repairPhase,
          startupPlanChecked, startupPlanClean>>

DrainArtifactRecoveryForeground ==
    /\ repairPhase = "coverage_failed"
    /\ artifactQueued
    /\ ~artifactValid
    /\ ~BuggyLeaveRecoveryUndrained
    /\ artifactValid' = TRUE
    /\ UNCHANGED
        <<artifactQueued, candidateExists, candidateComplete, repairPhase,
          startupPlanChecked, startupPlanClean>>

ResetCandidate ==
    /\ repairPhase = "coverage_failed"
    /\ artifactValid
    /\ repairPhase' = "retry_wait"
    /\ candidateExists' = IF BuggyReuseFailedCandidate THEN TRUE ELSE FALSE
    /\ UNCHANGED
        <<artifactQueued, artifactValid, candidateComplete,
          startupPlanChecked, startupPlanClean>>

RetryBuild ==
    /\ repairPhase = "retry_wait"
    /\ repairPhase' = "building"
    /\ candidateExists' = TRUE
    /\ candidateComplete' =
        IF candidateExists THEN candidateComplete ELSE artifactValid
    /\ UNCHANGED
        <<artifactQueued, artifactValid, startupPlanChecked, startupPlanClean>>

Activate ==
    /\ repairPhase = "building"
    /\ candidateComplete
    /\ repairPhase' = "ready"
    /\ UNCHANGED
        <<artifactQueued, artifactValid, candidateExists, candidateComplete,
          startupPlanChecked, startupPlanClean>>

CheckStartupPlanner ==
    /\ repairPhase = "ready"
    /\ ~startupPlanChecked
    /\ startupPlanChecked' = TRUE
    /\ startupPlanClean' = ~BuggyPlannerUsesNameCounter
    /\ UNCHANGED
        <<artifactQueued, artifactValid, candidateExists, candidateComplete,
          repairPhase>>

Next ==
    \/ ObserveCoverageFailure
    \/ QueueArtifactRecovery
    \/ DrainArtifactRecoveryForeground
    \/ ResetCandidate
    \/ RetryBuild
    \/ Activate
    \/ CheckStartupPlanner

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(ObserveCoverageFailure)
    /\ WF_vars(QueueArtifactRecovery)
    /\ WF_vars(DrainArtifactRecoveryForeground)
    /\ WF_vars(ResetCandidate)
    /\ WF_vars(RetryBuild)
    /\ WF_vars(Activate)
    /\ WF_vars(CheckStartupPlanner)

TypeOK ==
    /\ artifactQueued \in BOOLEAN
    /\ artifactValid \in BOOLEAN
    /\ candidateExists \in BOOLEAN
    /\ candidateComplete \in BOOLEAN
    /\ repairPhase \in {"building", "coverage_failed", "retry_wait", "ready"}
    /\ startupPlanChecked \in BOOLEAN
    /\ startupPlanClean \in BOOLEAN
    /\ BuggySkipArtifactRecovery \in BOOLEAN
    /\ BuggyLeaveRecoveryUndrained \in BOOLEAN
    /\ BuggyReuseFailedCandidate \in BOOLEAN
    /\ BuggyPlannerUsesNameCounter \in BOOLEAN

IncompleteCandidateNeverActivates ==
    repairPhase = "ready" => candidateComplete

ReadyGenerationHasNoStartupDebt ==
    (repairPhase = "ready" /\ startupPlanChecked) => startupPlanClean

AutomaticRepairEventuallyStabilizes ==
    <> (repairPhase = "ready" /\ startupPlanChecked /\ startupPlanClean)

=============================================================================
