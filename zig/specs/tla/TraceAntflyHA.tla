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

------------------------------- MODULE TraceAntflyHA ---------------------------
(*
  Trace refinement spec for focused HA event sequences.

  The checked-in fixtures under specs/tla/traces/ha_*.ndjson mirror concrete
  HA chaos/rejoin test scenarios. This validator is intentionally narrower than
  AntflyHAReplication.tla: it checks that a concrete ordered event stream can be
  consumed by the same lower-level contracts covered by the HA fast submodels:

    - sync wait target/ack timeline and LSN provenance;
    - standby durable receive, failed apply, idempotent apply progress;
    - timeline switch boundary and old-timeline rejection;
    - former-primary rejoin assessment, rewind, and reseed execution.

  Each trace line is ndjson:
    {"tag":"ha-trace","event":{"name":"..."}}
*)

EXTENDS Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./ha-trace.ndjson"

OriginTraceLog ==
    SelectSeq(
        ndJsonDeserialize(JsonFile),
        LAMBDA line: "tag" \in DOMAIN line /\ line.tag = "ha-trace")

TraceLog ==
    TLCEval(
        IF "MAX_TRACE" \in DOMAIN IOEnv
        THEN SubSeq(OriginTraceLog, 1, atoi(IOEnv.MAX_TRACE))
        ELSE OriginTraceLog)

VARIABLES
    l,
    pl,
    primaryLsn,
    currentTimeline,
    currentEpoch,
    receivedLsn,
    appliedLsn,
    safeReadLsn,
    haMarkerLsn,
    failedApplyLsn,
    effectCount,
    syncTargetTimeline,
    syncTargetLsn,
    syncAcked,
    oldTimelineRejected,
    rejoinAction,
    rejoinForkLsn,
    rejoinFormerLastLsn,
    rejoinRetainedFromLsn,
    rejoinForced,
    rejoinAllowForced,
    rejoinDataLossDiscarded,
    logLastLsn,
    logCurrentLastLsn,
    reseedRequired,
    baseBackupRequired

haVars ==
    <<primaryLsn, currentTimeline, currentEpoch, receivedLsn, appliedLsn,
      safeReadLsn, haMarkerLsn, failedApplyLsn, effectCount,
      syncTargetTimeline, syncTargetLsn, syncAcked, oldTimelineRejected,
      rejoinAction, rejoinForkLsn, rejoinFormerLastLsn, rejoinRetainedFromLsn,
      rejoinForced, rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
      logCurrentLastLsn, reseedRequired, baseBackupRequired>>

traceVars == <<l, pl, haVars>>

TraceInit ==
    /\ l = 1
    /\ pl = 0
    /\ primaryLsn = 0
    /\ currentTimeline = 1
    /\ currentEpoch = 1
    /\ receivedLsn = 0
    /\ appliedLsn = 0
    /\ safeReadLsn = 0
    /\ haMarkerLsn = 0
    /\ failedApplyLsn = 0
    /\ effectCount = 0
    /\ syncTargetTimeline = 0
    /\ syncTargetLsn = 0
    /\ syncAcked = FALSE
    /\ oldTimelineRejected = FALSE
    /\ rejoinAction = "none"
    /\ rejoinForkLsn = 0
    /\ rejoinFormerLastLsn = 0
    /\ rejoinRetainedFromLsn = 0
    /\ rejoinForced = FALSE
    /\ rejoinAllowForced = FALSE
    /\ rejoinDataLossDiscarded = FALSE
    /\ logLastLsn = 0
    /\ logCurrentLastLsn = 0
    /\ reseedRequired = FALSE
    /\ baseBackupRequired = FALSE

logline == TraceLog[l]
event == logline.event

StepToNextTrace ==
    /\ l' = l + 1
    /\ pl' = l

LoglineIsEvent(name) ==
    /\ l <= Len(TraceLog)
    /\ event.name = name

ExpectedRejoinAction(hasFence, identityOk, oldOk, timelineState, last, fork, retained, forced, allowForced) ==
    IF ~hasFence THEN
        "reject_unfenced"
    ELSE IF timelineState = "new" THEN
        "already_current"
    ELSE IF ~identityOk \/ ~oldOk \/ timelineState # "parent" THEN
        "reseed"
    ELSE IF last < fork THEN
        "reseed"
    ELSE IF fork < retained THEN
        "reseed"
    ELSE IF forced /\ ~allowForced THEN
        "reseed"
    ELSE
        "rewind"

PrimaryAppendIfLogged ==
    /\ LoglineIsEvent("PrimaryAppend")
    /\ event.timeline = currentTimeline
    /\ event.lsn = primaryLsn + 1
    /\ primaryLsn' = event.lsn
    /\ UNCHANGED <<currentTimeline, currentEpoch, receivedLsn, appliedLsn,
                  safeReadLsn, haMarkerLsn, failedApplyLsn, effectCount,
                  syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  logCurrentLastLsn, reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

StandbyReceiveIfLogged ==
    /\ LoglineIsEvent("StandbyReceive")
    /\ event.timeline = currentTimeline
    /\ event.lsn = receivedLsn + 1
    /\ event.lsn <= primaryLsn
    /\ receivedLsn' = event.lsn
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, appliedLsn,
                  safeReadLsn, haMarkerLsn, failedApplyLsn, effectCount,
                  syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  logCurrentLastLsn, reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

StandbyApplySuccessIfLogged ==
    /\ LoglineIsEvent("StandbyApplySuccess")
    /\ event.lsn = appliedLsn + 1
    /\ event.lsn <= receivedLsn
    /\ appliedLsn' = event.lsn
    /\ safeReadLsn' = event.lsn
    /\ haMarkerLsn' = event.lsn
    /\ effectCount' = event.lsn
    /\ failedApplyLsn' = 0
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  logCurrentLastLsn, reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

StandbyApplyFailureIfLogged ==
    /\ LoglineIsEvent("StandbyApplyFailure")
    /\ event.lsn = appliedLsn + 1
    /\ event.lsn <= receivedLsn
    /\ failedApplyLsn' = event.lsn
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, effectCount,
                  syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  logCurrentLastLsn, reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

FreezeSyncWaitIfLogged ==
    /\ LoglineIsEvent("FreezeSyncWait")
    /\ syncTargetTimeline = 0
    /\ event.timeline = currentTimeline
    /\ event.lsn <= primaryLsn
    /\ syncTargetTimeline' = event.timeline
    /\ syncTargetLsn' = event.lsn
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, failedApplyLsn,
                  effectCount, syncAcked, oldTimelineRejected, rejoinAction,
                  rejoinForkLsn, rejoinFormerLastLsn, rejoinRetainedFromLsn,
                  rejoinForced, rejoinAllowForced, rejoinDataLossDiscarded,
                  logLastLsn, logCurrentLastLsn, reseedRequired,
                  baseBackupRequired>>
    /\ StepToNextTrace

StatusAckIfLogged ==
    /\ LoglineIsEvent("StatusAck")
    /\ syncTargetTimeline # 0
    /\ event.timeline = syncTargetTimeline
    /\ event.appliedLsn >= syncTargetLsn
    /\ event.appliedLsn <= appliedLsn
    /\ syncAcked' = TRUE
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, failedApplyLsn,
                  effectCount, syncTargetTimeline, syncTargetLsn,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  logCurrentLastLsn, reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

TimelineSwitchIfLogged ==
    /\ LoglineIsEvent("TimelineSwitch")
    /\ event.parentReceivedLsn = receivedLsn
    /\ event.parentAppliedLsn = appliedLsn
    /\ event.safeReadLsn = safeReadLsn
    /\ event.previousLsn = appliedLsn
    /\ event.switchLsn = appliedLsn + 1
    /\ event.newTimeline > currentTimeline
    /\ event.newEpoch > currentEpoch
    /\ currentTimeline' = event.newTimeline
    /\ currentEpoch' = event.newEpoch
    /\ primaryLsn' = event.switchLsn
    /\ receivedLsn' = event.switchLsn
    /\ appliedLsn' = event.switchLsn
    /\ safeReadLsn' = event.switchLsn
    /\ haMarkerLsn' = event.switchLsn
    /\ effectCount' = event.switchLsn
    /\ UNCHANGED <<failedApplyLsn, syncTargetTimeline, syncTargetLsn,
                  syncAcked, oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  logCurrentLastLsn, reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

RejectOldTimelineIfLogged ==
    /\ LoglineIsEvent("RejectOldTimeline")
    /\ event.timeline < currentTimeline
    /\ oldTimelineRejected' = TRUE
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, failedApplyLsn,
                  effectCount, syncTargetTimeline, syncTargetLsn, syncAcked,
                  rejoinAction, rejoinForkLsn, rejoinFormerLastLsn,
                  rejoinRetainedFromLsn, rejoinForced, rejoinAllowForced,
                  rejoinDataLossDiscarded, logLastLsn, logCurrentLastLsn,
                  reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

AssessRejoinIfLogged ==
    /\ LoglineIsEvent("AssessRejoin")
    /\ event.action =
        ExpectedRejoinAction(
            event.hasFence,
            event.identityMatches,
            event.oldPrimaryMatches,
            event.timelineState,
            event.formerLastLsn,
            event.forkLsn,
            event.retainedFromLsn,
            event.forced,
            event.allowForcedRewind)
    /\ rejoinAction' = event.action
    /\ rejoinForkLsn' = event.forkLsn
    /\ rejoinFormerLastLsn' = event.formerLastLsn
    /\ rejoinRetainedFromLsn' = event.retainedFromLsn
    /\ rejoinForced' = event.forced
    /\ rejoinAllowForced' = event.allowForcedRewind
    /\ rejoinDataLossDiscarded' =
        (event.action = "rewind" /\ (event.formerLastLsn > event.forkLsn \/ event.forced))
    /\ logLastLsn' = event.formerLastLsn
    /\ logCurrentLastLsn' = event.formerLastLsn
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, failedApplyLsn,
                  effectCount, syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

LateFormerPrimaryWriteIfLogged ==
    /\ LoglineIsEvent("LateFormerPrimaryWrite")
    /\ event.logLastLsn = logLastLsn + 1
    /\ logLastLsn' = event.logLastLsn
    /\ logCurrentLastLsn' = event.logLastLsn
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, failedApplyLsn,
                  effectCount, syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, reseedRequired,
                  baseBackupRequired>>
    /\ StepToNextTrace

ExecuteRewindIfLogged ==
    /\ LoglineIsEvent("ExecuteRewind")
    /\ rejoinAction = "rewind"
    /\ event.previousLastLsn = logLastLsn
    /\ event.previousLastLsn = rejoinFormerLastLsn
    /\ event.forkLsn = rejoinForkLsn
    /\ event.forkLsn >= rejoinRetainedFromLsn
    /\ event.forkRecordPresent
    /\ event.forkRecordMatches
    /\ logCurrentLastLsn' = event.forkLsn
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, failedApplyLsn,
                  effectCount, syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  reseedRequired, baseBackupRequired>>
    /\ StepToNextTrace

ExecuteReseedIfLogged ==
    /\ LoglineIsEvent("ExecuteReseed")
    /\ rejoinAction = "reseed"
    /\ event.forkLsn = rejoinForkLsn
    /\ event.reseedRequired
    /\ event.baseBackupRequired
    /\ reseedRequired' = TRUE
    /\ baseBackupRequired' = TRUE
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, receivedLsn,
                  appliedLsn, safeReadLsn, haMarkerLsn, failedApplyLsn,
                  effectCount, syncTargetTimeline, syncTargetLsn, syncAcked,
                  oldTimelineRejected, rejoinAction, rejoinForkLsn,
                  rejoinFormerLastLsn, rejoinRetainedFromLsn, rejoinForced,
                  rejoinAllowForced, rejoinDataLossDiscarded, logLastLsn,
                  logCurrentLastLsn>>
    /\ StepToNextTrace

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ \/ PrimaryAppendIfLogged
       \/ StandbyReceiveIfLogged
       \/ StandbyApplySuccessIfLogged
       \/ StandbyApplyFailureIfLogged
       \/ FreezeSyncWaitIfLogged
       \/ StatusAckIfLogged
       \/ TimelineSwitchIfLogged
       \/ RejectOldTimelineIfLogged
       \/ AssessRejoinIfLogged
       \/ LateFormerPrimaryWriteIfLogged
       \/ ExecuteRewindIfLogged
       \/ ExecuteReseedIfLogged

TraceSpec == TraceInit /\ [][TraceNext]_traceVars

TraceView == <<l, haVars>>

TypeOK ==
    /\ l \in 1..(Len(TraceLog) + 1)
    /\ pl \in 0..Len(TraceLog)
    /\ currentTimeline \in Nat
    /\ currentEpoch \in Nat
    /\ primaryLsn \in Nat
    /\ receivedLsn \in Nat
    /\ appliedLsn \in Nat
    /\ safeReadLsn \in Nat
    /\ haMarkerLsn \in Nat
    /\ failedApplyLsn \in Nat
    /\ effectCount \in Nat
    /\ syncTargetTimeline \in Nat
    /\ syncTargetLsn \in Nat
    /\ syncAcked \in BOOLEAN
    /\ oldTimelineRejected \in BOOLEAN
    /\ rejoinAction \in {"none", "reject_unfenced", "already_current", "rewind", "reseed"}
    /\ rejoinForkLsn \in Nat
    /\ rejoinFormerLastLsn \in Nat
    /\ rejoinRetainedFromLsn \in Nat
    /\ rejoinForced \in BOOLEAN
    /\ rejoinAllowForced \in BOOLEAN
    /\ rejoinDataLossDiscarded \in BOOLEAN
    /\ logLastLsn \in Nat
    /\ logCurrentLastLsn \in Nat
    /\ reseedRequired \in BOOLEAN
    /\ baseBackupRequired \in BOOLEAN

ProgressOrder ==
    /\ safeReadLsn <= appliedLsn
    /\ appliedLsn <= receivedLsn
    /\ haMarkerLsn <= appliedLsn
    /\ effectCount = haMarkerLsn

FailedApplyDoesNotAdvance ==
    failedApplyLsn # 0 =>
        /\ appliedLsn < failedApplyLsn
        /\ safeReadLsn < failedApplyLsn
        /\ haMarkerLsn < failedApplyLsn

SyncAckMatchesFrozenTarget ==
    syncAcked =>
        /\ syncTargetTimeline # 0
        /\ appliedLsn >= syncTargetLsn

RejoinDataLossFlagExact ==
    rejoinAction # "none" =>
        rejoinDataLossDiscarded =
            (rejoinAction = "rewind" /\ (rejoinFormerLastLsn > rejoinForkLsn \/ rejoinForced))

RejoinRewindExecutionSafe ==
    logCurrentLastLsn = rejoinForkLsn /\ rejoinAction = "rewind" =>
        /\ logLastLsn = rejoinFormerLastLsn
        /\ rejoinForkLsn >= rejoinRetainedFromLsn
        /\ ~rejoinForced \/ rejoinAllowForced

RejoinReseedPublishesRequirements ==
    rejoinAction = "reseed" /\ reseedRequired =>
        baseBackupRequired

TraceSafety ==
    /\ TypeOK
    /\ ProgressOrder
    /\ FailedApplyDoesNotAdvance
    /\ SyncAckMatchesFrozenTarget
    /\ RejoinDataLossFlagExact
    /\ RejoinRewindExecutionSafe
    /\ RejoinReseedPublishesRequirements

TraceMatched ==
    [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
