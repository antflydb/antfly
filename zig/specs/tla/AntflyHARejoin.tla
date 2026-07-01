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

------------------------------ MODULE AntflyHARejoin --------------------------
(*
  Fast submodel for former-primary rejoin assessment and execution.

  This isolates storage/ha/rejoin.zig and the operator/admin-client rejoin
  contracts:

  - no promotion fence means the former primary is rejected, not rewound;
  - a fenced rewind requires matching identity, old-primary id, parent timeline,
    local WAL at or beyond the fork, retained WAL covering the fork, and explicit
    policy for forced-promotion rewind;
  - executing a rewind rechecks that the assessment is fresh, the fork record is
    still retained, and the fork record identity matches before truncating;
  - reseed execution must publish the reseed/base-backup requirements.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyUnfencedRewinds,
    BuggyExpiredWalRewinds,
    BuggyForcedRewinds,
    BuggyIdentityMismatchRewinds,
    BuggyStaleAssessmentTruncates,
    BuggyForkMismatchTruncates

Lsns == 0..3
Actions == {"none", "reject_unfenced", "already_current", "rewind", "reseed"}
TimelineStates == {"parent", "new", "other"}

VARIABLES
    assessed,
    hasFence,
    identityMatches,
    oldPrimaryMatches,
    timelineState,
    formerLastLsn,
    forkLsn,
    retainedFromLsn,
    forcedPromotion,
    allowForcedRewind,
    action,
    targetTimeline,
    targetEpoch,
    dataLossDiscarded,
    logLastLsn,
    logCurrentLastLsn,
    forkRecordPresent,
    forkRecordMatches,
    executed,
    executionPreviousLastLsn,
    reseedRequired,
    baseBackupRequired

vars ==
    <<assessed, hasFence, identityMatches, oldPrimaryMatches, timelineState,
      formerLastLsn, forkLsn, retainedFromLsn, forcedPromotion,
      allowForcedRewind, action, targetTimeline, targetEpoch, dataLossDiscarded,
      logLastLsn, logCurrentLastLsn, forkRecordPresent, forkRecordMatches,
      executed, executionPreviousLastLsn, reseedRequired, baseBackupRequired>>

Init ==
    /\ assessed = FALSE
    /\ hasFence = FALSE
    /\ identityMatches = TRUE
    /\ oldPrimaryMatches = TRUE
    /\ timelineState = "parent"
    /\ formerLastLsn = 0
    /\ forkLsn = 0
    /\ retainedFromLsn = 0
    /\ forcedPromotion = FALSE
    /\ allowForcedRewind = FALSE
    /\ action = "none"
    /\ targetTimeline = 1
    /\ targetEpoch = 1
    /\ dataLossDiscarded = FALSE
    /\ logLastLsn = 0
    /\ logCurrentLastLsn = 0
    /\ forkRecordPresent = TRUE
    /\ forkRecordMatches = TRUE
    /\ executed = FALSE
    /\ executionPreviousLastLsn = 0
    /\ reseedRequired = FALSE
    /\ baseBackupRequired = FALSE

ExpectedAction(hf, idOk, oldOk, ts, last, fork, retained, forced, allowForced) ==
    IF ~hf THEN
        "reject_unfenced"
    ELSE IF ts = "new" THEN
        "already_current"
    ELSE IF ~idOk \/ ~oldOk \/ ts # "parent" THEN
        "reseed"
    ELSE IF last < fork THEN
        "reseed"
    ELSE IF fork < retained THEN
        "reseed"
    ELSE IF forced /\ ~allowForced THEN
        "reseed"
    ELSE
        "rewind"

ChosenAction(hf, idOk, oldOk, ts, last, fork, retained, forced, allowForced) ==
    IF ~hf /\ BuggyUnfencedRewinds THEN
        "rewind"
    ELSE IF hf /\ (~idOk \/ ~oldOk \/ ts = "other") /\ BuggyIdentityMismatchRewinds THEN
        "rewind"
    ELSE IF hf /\ idOk /\ oldOk /\ ts = "parent" /\ last >= fork /\ fork < retained /\ BuggyExpiredWalRewinds THEN
        "rewind"
    ELSE IF hf /\ idOk /\ oldOk /\ ts = "parent" /\ last >= fork /\ fork >= retained /\ forced /\ ~allowForced /\ BuggyForcedRewinds THEN
        "rewind"
    ELSE
        ExpectedAction(hf, idOk, oldOk, ts, last, fork, retained, forced, allowForced)

Assess ==
    /\ ~assessed
    /\ \E hf \in BOOLEAN,
          idOk \in BOOLEAN,
          oldOk \in BOOLEAN,
          ts \in TimelineStates,
          last \in Lsns,
          fork \in Lsns,
          retained \in Lsns,
          forced \in BOOLEAN,
          allowForced \in BOOLEAN,
          forkPresent \in BOOLEAN,
          forkMatches \in BOOLEAN:
        /\ hasFence' = hf
        /\ identityMatches' = idOk
        /\ oldPrimaryMatches' = oldOk
        /\ timelineState' = ts
        /\ formerLastLsn' = last
        /\ forkLsn' = IF hf THEN fork ELSE last
        /\ retainedFromLsn' = retained
        /\ forcedPromotion' = forced
        /\ allowForcedRewind' = allowForced
        /\ action' = ChosenAction(hf, idOk, oldOk, ts, last, IF hf THEN fork ELSE last, retained, forced, allowForced)
        /\ targetTimeline' = IF hf THEN 2 ELSE 1
        /\ targetEpoch' = IF hf THEN 2 ELSE 1
        /\ dataLossDiscarded' =
            (ChosenAction(hf, idOk, oldOk, ts, last, IF hf THEN fork ELSE last, retained, forced, allowForced) = "rewind")
            /\ (last > (IF hf THEN fork ELSE last) \/ forced)
        /\ logLastLsn' = last
        /\ logCurrentLastLsn' = last
        /\ forkRecordPresent' = forkPresent
        /\ forkRecordMatches' = forkMatches
    /\ assessed' = TRUE
    /\ UNCHANGED <<executed, executionPreviousLastLsn, reseedRequired,
                  baseBackupRequired>>

LateFormerPrimaryWrite ==
    /\ assessed
    /\ ~executed
    /\ logLastLsn < 3
    /\ logLastLsn' = logLastLsn + 1
    /\ logCurrentLastLsn' = logLastLsn + 1
    /\ UNCHANGED <<assessed, hasFence, identityMatches, oldPrimaryMatches,
                  timelineState, formerLastLsn, forkLsn, retainedFromLsn,
                  forcedPromotion, allowForcedRewind, action, targetTimeline,
                  targetEpoch, dataLossDiscarded, forkRecordPresent,
                  forkRecordMatches, executed, executionPreviousLastLsn,
                  reseedRequired, baseBackupRequired>>

CanExecuteRewind ==
    /\ action = "rewind"
    /\ (logLastLsn = formerLastLsn \/ BuggyStaleAssessmentTruncates)
    /\ forkLsn >= retainedFromLsn
    /\ logLastLsn >= forkLsn
    /\ (forkLsn = 0 \/ forkRecordPresent \/ BuggyForkMismatchTruncates)
    /\ (forkLsn = 0 \/ forkRecordMatches \/ BuggyForkMismatchTruncates)

ExecuteRewind ==
    /\ assessed
    /\ ~executed
    /\ CanExecuteRewind
    /\ executed' = TRUE
    /\ executionPreviousLastLsn' = logLastLsn
    /\ logCurrentLastLsn' = forkLsn
    /\ UNCHANGED <<assessed, hasFence, identityMatches, oldPrimaryMatches,
                  timelineState, formerLastLsn, forkLsn, retainedFromLsn,
                  forcedPromotion, allowForcedRewind, action, targetTimeline,
                  targetEpoch, dataLossDiscarded, logLastLsn,
                  forkRecordPresent, forkRecordMatches, reseedRequired,
                  baseBackupRequired>>

ExecuteReseed ==
    /\ assessed
    /\ ~executed
    /\ action = "reseed"
    /\ executed' = TRUE
    /\ reseedRequired' = TRUE
    /\ baseBackupRequired' = TRUE
    /\ UNCHANGED <<assessed, hasFence, identityMatches, oldPrimaryMatches,
                  timelineState, formerLastLsn, forkLsn, retainedFromLsn,
                  forcedPromotion, allowForcedRewind, action, targetTimeline,
                  targetEpoch, dataLossDiscarded, logLastLsn,
                  logCurrentLastLsn, forkRecordPresent, forkRecordMatches,
                  executionPreviousLastLsn>>

Next ==
    \/ Assess
    \/ LateFormerPrimaryWrite
    \/ ExecuteRewind
    \/ ExecuteReseed

Spec == Init /\ [][Next]_vars

(*
  Liveness: rejoin never stalls permanently. Assessment always eventually
  happens; an executable decision is eventually executed. Rewind execution is
  conditional on the state settling executable (<>[]) because a late
  former-primary write or a failed fork-record validation legitimately blocks
  execution forever in this model (re-assessment is operator-driven and out of
  scope here); the safety invariants cover those refusals.
*)
RewindExecutable ==
    /\ action = "rewind"
    /\ logLastLsn = formerLastLsn
    /\ (forkLsn = 0 \/ (forkRecordPresent /\ forkRecordMatches))

Fairness ==
    /\ WF_vars(Assess)
    /\ WF_vars(ExecuteRewind)
    /\ WF_vars(ExecuteReseed)

\* Liveness-checked spec used by the positive config; mutant configs check
\* invariants only and use the unfair Spec.
FairSpec == Spec /\ Fairness

EventuallyAssessed == <>assessed

RejoinEventuallyExecutes ==
    /\ <>[](action = "reseed") => <>executed
    /\ <>[]RewindExecutable => <>executed

TypeOK ==
    /\ assessed \in BOOLEAN
    /\ hasFence \in BOOLEAN
    /\ identityMatches \in BOOLEAN
    /\ oldPrimaryMatches \in BOOLEAN
    /\ timelineState \in TimelineStates
    /\ formerLastLsn \in Lsns
    /\ forkLsn \in Lsns
    /\ retainedFromLsn \in Lsns
    /\ forcedPromotion \in BOOLEAN
    /\ allowForcedRewind \in BOOLEAN
    /\ action \in Actions
    /\ targetTimeline \in {1, 2}
    /\ targetEpoch \in {1, 2}
    /\ dataLossDiscarded \in BOOLEAN
    /\ logLastLsn \in Lsns
    /\ logCurrentLastLsn \in Lsns
    /\ forkRecordPresent \in BOOLEAN
    /\ forkRecordMatches \in BOOLEAN
    /\ executed \in BOOLEAN
    /\ executionPreviousLastLsn \in Lsns
    /\ reseedRequired \in BOOLEAN
    /\ baseBackupRequired \in BOOLEAN

AssessmentMatchesPolicy ==
    assessed => action = ExpectedAction(
        hasFence,
        identityMatches,
        oldPrimaryMatches,
        timelineState,
        formerLastLsn,
        forkLsn,
        retainedFromLsn,
        forcedPromotion,
        allowForcedRewind)

RewindRequiresCompatibleFence ==
    assessed /\ action = "rewind" =>
        /\ hasFence
        /\ identityMatches
        /\ oldPrimaryMatches
        /\ timelineState = "parent"
        /\ formerLastLsn >= forkLsn
        /\ forkLsn >= retainedFromLsn
        /\ ~forcedPromotion \/ allowForcedRewind

RejectUnfencedDoesNotExecute ==
    assessed /\ ~hasFence =>
        /\ action = "reject_unfenced"
        /\ ~executed

RewindExecutionRequiresFreshAssessment ==
    executed /\ action = "rewind" =>
        /\ executionPreviousLastLsn = formerLastLsn
        /\ logCurrentLastLsn = forkLsn

RewindExecutionValidatesForkRecord ==
    executed /\ action = "rewind" /\ forkLsn > 0 =>
        /\ forkRecordPresent
        /\ forkRecordMatches

ReseedExecutionPublishesSeedRequirement ==
    executed /\ action = "reseed" =>
        /\ reseedRequired
        /\ baseBackupRequired

DataLossFlagIsExact ==
    assessed =>
        dataLossDiscarded = (action = "rewind" /\ (formerLastLsn > forkLsn \/ forcedPromotion))

Safety ==
    /\ TypeOK
    /\ AssessmentMatchesPolicy
    /\ RewindRequiresCompatibleFence
    /\ RejectUnfencedDoesNotExecute
    /\ RewindExecutionRequiresFreshAssessment
    /\ RewindExecutionValidatesForkRecord
    /\ ReseedExecutionPublishesSeedRequirement
    /\ DataLossFlagIsExact

=============================================================================
