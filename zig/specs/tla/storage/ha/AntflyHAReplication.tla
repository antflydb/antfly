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

-------------------------- MODULE AntflyHAReplication --------------------------
(*
  Lower-level bounded model of storage/ha primary, standby, fencing, and rejoin.

  This broad model checks replication progress, sync wait, timeline, and rejoin
  contracts under practical bounds. Focused sibling models carry the deepest
  failover obligations:
    - AntflyHAFailoverSafety preserves acknowledged writes across promotion and
      suppresses old-primary writes after promotion.
    - AntflyHAPartitionFence models async fence delivery across a partition
      before promotion is allowed.

  Concrete Zig contracts modeled:
    - SlotStore.updateProgress is monotonic and sets restart_lsn = received_lsn.
    - paused/inactive, reseed_required, and wrong-timeline slots are ineligible.
    - remote_write uses received_lsn; remote_apply uses applied_lsn.
    - selection any/first/all have different required-count behavior.
    - promotion fence receipts have monotonic generation and timeline/epoch
      chaining; forced fences are needed when observed_lsn < required_lsn.
    - Standby promotion appends a timeline_switch at received_lsn + 1 and
      promoted-primary handoff requires received = applied = safe_read.
    - former-primary rejoin rewinds only when the retained WAL floor covers the
      fork LSN; otherwise it must reseed.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS BuggyAcceptStaleTimelineAck

Slots == {"standbyA", "standbyB"}
AckSlots == Slots \cup {"none"}
Nodes == {"oldPrimary", "standbyA", "standbyB"}
Modes == {"async", "remote_write", "remote_apply"}
AckModes == Modes \cup {"none"}
Selections == {"any", "first", "all"}
FailurePolicies == {"block", "fail_closed", "degrade_to_async"}
DurabilityStatuses == {"satisfied", "would_block", "fail_closed", "degraded_to_async"}
RejoinActions == {"none", "reject_unfenced", "already_current", "rewind", "reseed"}

MaxLsn == 2
MaxTimeline == 3
MaxEpoch == 3

VARIABLES
    primaryLsn,
    currentTimeline,
    currentEpoch,
    slotTimeline,
    slotActive,
    slotReseed,
    restartLsn,
    receivedLsn,
    appliedLsn,
    safeReadLsn,
    mode,
    selection,
    requiredCount,
    failurePolicy,
    durabilityStatus,
    durabilityProgress,
    satisfiedCount,
    candidateCount,
    fenceHeld,
    fenceGeneration,
    fenceParentTimeline,
    fenceParentEpoch,
    fenceNewTimeline,
    fenceNewEpoch,
    fenceRequiredLsn,
    fenceObservedLsn,
    fenceForced,
    promoted,
    switchLsn,
    formerTimeline,
    formerEpoch,
    formerLastLsn,
    retainedFromLsn,
    rejoinAction,
    syncTargetTimeline,
    syncTargetLsn,
    syncTargetMode,
    syncAcked,
    syncAckTimeline,
    syncAckLsn,
    syncAckSlot

vars == <<primaryLsn, currentTimeline, currentEpoch, slotTimeline, slotActive,
          slotReseed, restartLsn, receivedLsn, appliedLsn, safeReadLsn, mode,
          selection, requiredCount, failurePolicy, durabilityStatus,
          durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
          fenceGeneration, fenceParentTimeline, fenceParentEpoch,
          fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn, fenceObservedLsn,
          fenceForced, promoted, switchLsn, formerTimeline, formerEpoch,
          formerLastLsn, retainedFromLsn, rejoinAction, syncTargetTimeline,
          syncTargetLsn, syncTargetMode, syncAcked, syncAckTimeline,
          syncAckLsn, syncAckSlot>>

syncVars == <<syncTargetTimeline, syncTargetLsn, syncTargetMode, syncAcked,
              syncAckTimeline, syncAckLsn, syncAckSlot>>

Init ==
    /\ primaryLsn = 0
    /\ currentTimeline = 1
    /\ currentEpoch = 1
    /\ slotTimeline = [s \in Slots |-> 1]
    /\ slotActive = [s \in Slots |-> TRUE]
    /\ slotReseed = [s \in Slots |-> FALSE]
    /\ restartLsn = [s \in Slots |-> 0]
    /\ receivedLsn = [s \in Slots |-> 0]
    /\ appliedLsn = [s \in Slots |-> 0]
    /\ safeReadLsn = [s \in Slots |-> 0]
    /\ mode = "async"
    /\ selection = "any"
    /\ requiredCount = 1
    /\ failurePolicy = "block"
    /\ durabilityStatus = "satisfied"
    /\ durabilityProgress = 0
    /\ satisfiedCount = 0
    /\ candidateCount = 0
    /\ fenceHeld = FALSE
    /\ fenceGeneration = 0
    /\ fenceParentTimeline = 0
    /\ fenceParentEpoch = 0
    /\ fenceNewTimeline = 0
    /\ fenceNewEpoch = 0
    /\ fenceRequiredLsn = 0
    /\ fenceObservedLsn = 0
    /\ fenceForced = FALSE
    /\ promoted = FALSE
    /\ switchLsn = 0
    /\ formerTimeline = 1
    /\ formerEpoch = 1
    /\ formerLastLsn = 0
    /\ retainedFromLsn = 1
    /\ rejoinAction = "none"
    /\ syncTargetTimeline = 0
    /\ syncTargetLsn = 0
    /\ syncTargetMode = "none"
    /\ syncAcked = FALSE
    /\ syncAckTimeline = 0
    /\ syncAckLsn = 0
    /\ syncAckSlot = "none"

Eligible(s) ==
    /\ slotActive[s]
    /\ ~slotReseed[s]
    /\ slotTimeline[s] = currentTimeline

ProgressFor(s, m) ==
    IF m = "remote_write" THEN receivedLsn[s] ELSE appliedLsn[s]

SlotSatisfies(s, target, m) ==
    Eligible(s) /\ ProgressFor(s, m) >= target

EligibleCount ==
    Cardinality({s \in Slots: Eligible(s)})

AllSatisfiedCount(target, m) ==
    Cardinality({s \in Slots: SlotSatisfies(s, target, m)})

AnyProgress(required, m) ==
    IF required = 1 THEN
        IF ProgressFor("standbyA", m) >= ProgressFor("standbyB", m)
        THEN ProgressFor("standbyA", m)
        ELSE ProgressFor("standbyB", m)
    ELSE
        IF ProgressFor("standbyA", m) <= ProgressFor("standbyB", m)
        THEN ProgressFor("standbyA", m)
        ELSE ProgressFor("standbyB", m)

FirstSlot == "standbyA"

FirstProgress(m) ==
    IF Eligible("standbyA") THEN ProgressFor("standbyA", m)
    ELSE IF Eligible("standbyB") THEN ProgressFor("standbyB", m)
    ELSE 0

FirstSatisfies(target, m) ==
    IF Eligible("standbyA") THEN ProgressFor("standbyA", m) >= target
    ELSE IF Eligible("standbyB") THEN ProgressFor("standbyB", m) >= target
    ELSE FALSE

AllProgress(m) ==
    IF ProgressFor("standbyA", m) <= ProgressFor("standbyB", m)
    THEN ProgressFor("standbyA", m)
    ELSE ProgressFor("standbyB", m)

StatusForFailure(policy) ==
    CASE policy = "fail_closed" -> "fail_closed"
      [] policy = "degrade_to_async" -> "degraded_to_async"
      [] OTHER -> "would_block"

AppendPrimary ==
    /\ ~promoted
    /\ primaryLsn < MaxLsn
    /\ primaryLsn' = primaryLsn + 1
    /\ UNCHANGED <<currentTimeline, currentEpoch, slotTimeline, slotActive,
                  slotReseed, restartLsn, receivedLsn, appliedLsn, safeReadLsn,
                  mode, selection, requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, promoted, switchLsn,
                  formerTimeline, formerEpoch, formerLastLsn, retainedFromLsn,
                  rejoinAction>>

CreateOrResetSlot(s) ==
    /\ s \in Slots
    /\ primaryLsn < MaxLsn
    /\ slotTimeline' = [slotTimeline EXCEPT ![s] = currentTimeline]
    /\ slotActive' = [slotActive EXCEPT ![s] = TRUE]
    /\ slotReseed' = [slotReseed EXCEPT ![s] = FALSE]
    /\ restartLsn' = [restartLsn EXCEPT ![s] = primaryLsn + 1]
    /\ receivedLsn' = [receivedLsn EXCEPT ![s] = primaryLsn]
    /\ appliedLsn' = [appliedLsn EXCEPT ![s] = primaryLsn]
    /\ safeReadLsn' = [safeReadLsn EXCEPT ![s] = primaryLsn]
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, mode, selection,
                  requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, promoted, switchLsn,
                  formerTimeline, formerEpoch, formerLastLsn, retainedFromLsn,
                  rejoinAction>>

ReceiveOnSlot(s) ==
    /\ s \in Slots
    /\ Eligible(s)
    /\ receivedLsn[s] < primaryLsn
    /\ receivedLsn' = [receivedLsn EXCEPT ![s] = @ + 1]
    /\ restartLsn' = [restartLsn EXCEPT ![s] = receivedLsn[s] + 1]
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, appliedLsn, safeReadLsn, mode,
                  selection, requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, promoted, switchLsn,
                  formerTimeline, formerEpoch, formerLastLsn, retainedFromLsn,
                  rejoinAction>>

ApplyOnSlot(s) ==
    /\ s \in Slots
    /\ Eligible(s)
    /\ appliedLsn[s] < receivedLsn[s]
    /\ appliedLsn' = [appliedLsn EXCEPT ![s] = @ + 1]
    /\ safeReadLsn' = [safeReadLsn EXCEPT ![s] = appliedLsn[s] + 1]
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, mode,
                  selection, requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, promoted, switchLsn,
                  formerTimeline, formerEpoch, formerLastLsn, retainedFromLsn,
                  rejoinAction>>

PauseSlot(s) ==
    /\ s \in Slots
    /\ slotActive[s]
    /\ slotActive' = [slotActive EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotReseed, restartLsn, receivedLsn, appliedLsn, safeReadLsn,
                  mode, selection, requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, promoted, switchLsn,
                  formerTimeline, formerEpoch, formerLastLsn, retainedFromLsn,
                  rejoinAction>>

ResumeSlot(s) ==
    /\ s \in Slots
    /\ ~slotActive[s]
    /\ slotActive' = [slotActive EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotReseed, restartLsn, receivedLsn, appliedLsn, safeReadLsn,
                  mode, selection, requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, promoted, switchLsn,
                  formerTimeline, formerEpoch, formerLastLsn, retainedFromLsn,
                  rejoinAction>>

MarkReseedRequired(s) ==
    /\ s \in Slots
    /\ ~slotReseed[s]
    /\ slotReseed' = [slotReseed EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, restartLsn, receivedLsn, appliedLsn, safeReadLsn,
                  mode, selection, requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, promoted, switchLsn,
                  formerTimeline, formerEpoch, formerLastLsn, retainedFromLsn,
                  rejoinAction>>

ChoosePolicy ==
    /\ mode' \in Modes
    /\ selection' \in Selections
    /\ requiredCount' \in 1..2
    /\ failurePolicy' \in FailurePolicies
    /\ durabilityStatus' = IF mode' = "async" THEN "satisfied" ELSE "would_block"
    /\ durabilityProgress' = 0
    /\ satisfiedCount' = 0
    /\ candidateCount' = 0
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, fenceHeld, fenceGeneration,
                  fenceParentTimeline, fenceParentEpoch, fenceNewTimeline,
                  fenceNewEpoch, fenceRequiredLsn, fenceObservedLsn, fenceForced,
                  promoted, switchLsn, formerTimeline, formerEpoch,
                  formerLastLsn, retainedFromLsn, rejoinAction>>

EvaluateDurability ==
    /\ LET target == primaryLsn IN
       /\ durabilityStatus' =
            IF mode = "async" THEN "satisfied"
            ELSE IF selection = "all" /\ AllSatisfiedCount(target, mode) = EligibleCount /\ EligibleCount > 0 THEN "satisfied"
            ELSE IF selection = "first" /\ FirstSatisfies(target, mode) THEN "satisfied"
            ELSE IF selection = "any" /\ AllSatisfiedCount(target, mode) >= requiredCount THEN "satisfied"
            ELSE StatusForFailure(failurePolicy)
       /\ durabilityProgress' =
            IF mode = "async" THEN target
            ELSE IF selection = "first" THEN FirstProgress(mode)
            ELSE IF selection = "all" THEN AllProgress(mode)
            ELSE AnyProgress(requiredCount, mode)
       /\ satisfiedCount' = IF mode = "async" THEN 0 ELSE AllSatisfiedCount(target, mode)
       /\ candidateCount' = IF mode = "async" THEN 0 ELSE EligibleCount
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, mode, selection, requiredCount, failurePolicy,
                  fenceHeld, fenceGeneration, fenceParentTimeline,
                  fenceParentEpoch, fenceNewTimeline, fenceNewEpoch,
                  fenceRequiredLsn, fenceObservedLsn, fenceForced, promoted,
                  switchLsn, formerTimeline, formerEpoch, formerLastLsn,
                  retainedFromLsn, rejoinAction>>

AcquireFence(s, req, obs, forced) ==
    /\ s \in Slots
    /\ s = "standbyA"
    /\ req \in 1..MaxLsn
    /\ obs \in 0..MaxLsn
    /\ forced \in BOOLEAN
    /\ ~promoted
    /\ Eligible(s)
    /\ safeReadLsn[s] >= obs
    /\ (obs >= req \/ forced)
    /\ currentTimeline < MaxTimeline
    /\ currentEpoch < MaxEpoch
    /\ ( \/ ~fenceHeld
         \/ /\ fenceNewTimeline = currentTimeline
            /\ fenceNewEpoch = currentEpoch
            /\ s = "standbyA" )
    /\ fenceHeld' = TRUE
    /\ fenceGeneration' = fenceGeneration + 1
    /\ fenceParentTimeline' = currentTimeline
    /\ fenceParentEpoch' = currentEpoch
    /\ fenceNewTimeline' = currentTimeline + 1
    /\ fenceNewEpoch' = currentEpoch + 1
    /\ fenceRequiredLsn' = req
    /\ fenceObservedLsn' = obs
    /\ fenceForced' = forced
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, mode, selection, requiredCount, failurePolicy,
                  durabilityStatus, durabilityProgress, satisfiedCount,
                  candidateCount, promoted, switchLsn, formerTimeline,
                  formerEpoch, formerLastLsn, retainedFromLsn, rejoinAction>>

PromoteStandby ==
    /\ ~promoted
    /\ fenceHeld
    /\ (receivedLsn["standbyA"] >= fenceRequiredLsn \/ fenceForced)
    /\ appliedLsn["standbyA"] = receivedLsn["standbyA"]
    /\ safeReadLsn["standbyA"] = receivedLsn["standbyA"]
    /\ switchLsn' = receivedLsn["standbyA"] + 1
    /\ primaryLsn' = receivedLsn["standbyA"] + 1
    /\ currentTimeline' = fenceNewTimeline
    /\ currentEpoch' = fenceNewEpoch
    /\ promoted' = TRUE
    /\ formerTimeline' = fenceParentTimeline
    /\ formerEpoch' = fenceParentEpoch
    /\ formerLastLsn' \in fenceObservedLsn..MaxLsn
    /\ UNCHANGED <<slotTimeline, slotActive, slotReseed, restartLsn,
                  receivedLsn, appliedLsn, safeReadLsn, mode, selection,
                  requiredCount, failurePolicy, durabilityStatus,
                  durabilityProgress, satisfiedCount, candidateCount, fenceHeld,
                  fenceGeneration, fenceParentTimeline, fenceParentEpoch,
                  fenceNewTimeline, fenceNewEpoch, fenceRequiredLsn,
                  fenceObservedLsn, fenceForced, retainedFromLsn, rejoinAction>>

ChooseRetentionFloor ==
    /\ retainedFromLsn' \in 1..MaxLsn
    /\ rejoinAction' = "none"
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, mode, selection, requiredCount, failurePolicy,
                  durabilityStatus, durabilityProgress, satisfiedCount,
                  candidateCount, fenceHeld, fenceGeneration,
                  fenceParentTimeline, fenceParentEpoch, fenceNewTimeline,
                  fenceNewEpoch, fenceRequiredLsn, fenceObservedLsn, fenceForced,
                  promoted, switchLsn, formerTimeline, formerEpoch,
                  formerLastLsn>>

AssessFormerPrimaryRejoin ==
    /\ promoted
    /\ rejoinAction' =
        IF ~fenceHeld THEN "reject_unfenced"
        ELSE IF formerTimeline = currentTimeline /\ formerEpoch = currentEpoch THEN "already_current"
        ELSE IF formerTimeline # fenceParentTimeline \/ formerEpoch # fenceParentEpoch THEN "reseed"
        ELSE IF formerLastLsn < fenceObservedLsn THEN "reseed"
        ELSE IF fenceObservedLsn < retainedFromLsn THEN "reseed"
        ELSE IF fenceForced THEN "reseed"
        ELSE "rewind"
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, mode, selection, requiredCount, failurePolicy,
                  durabilityStatus, durabilityProgress, satisfiedCount,
                  candidateCount, fenceHeld, fenceGeneration,
                  fenceParentTimeline, fenceParentEpoch, fenceNewTimeline,
                  fenceNewEpoch, fenceRequiredLsn, fenceObservedLsn, fenceForced,
                  promoted, switchLsn, formerTimeline, formerEpoch,
                  formerLastLsn, retainedFromLsn>>

BeginSyncCommitWait ==
    /\ mode # "async"
    /\ primaryLsn > 0
    /\ syncTargetTimeline' = currentTimeline
    /\ syncTargetLsn' = primaryLsn
    /\ syncTargetMode' = mode
    /\ syncAcked' = FALSE
    /\ syncAckTimeline' = 0
    /\ syncAckLsn' = 0
    /\ syncAckSlot' = "none"
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, mode, selection, requiredCount, failurePolicy,
                  durabilityStatus, durabilityProgress, satisfiedCount,
                  candidateCount, fenceHeld, fenceGeneration,
                  fenceParentTimeline, fenceParentEpoch, fenceNewTimeline,
                  fenceNewEpoch, fenceRequiredLsn, fenceObservedLsn, fenceForced,
                  promoted, switchLsn, formerTimeline, formerEpoch,
                  formerLastLsn, retainedFromLsn, rejoinAction>>

RecordSyncStatusAck(s) ==
    /\ s \in Slots
    /\ syncTargetLsn > 0
    /\ syncTargetMode # "async"
    /\ Eligible(s)
    /\ slotTimeline[s] = syncTargetTimeline
    /\ ProgressFor(s, syncTargetMode) >= syncTargetLsn
    /\ syncAcked' = TRUE
    /\ syncAckTimeline' = slotTimeline[s]
    /\ syncAckLsn' = ProgressFor(s, syncTargetMode)
    /\ syncAckSlot' = s
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, mode, selection, requiredCount, failurePolicy,
                  durabilityStatus, durabilityProgress, satisfiedCount,
                  candidateCount, fenceHeld, fenceGeneration,
                  fenceParentTimeline, fenceParentEpoch, fenceNewTimeline,
                  fenceNewEpoch, fenceRequiredLsn, fenceObservedLsn, fenceForced,
                  promoted, switchLsn, formerTimeline, formerEpoch,
                  formerLastLsn, retainedFromLsn, rejoinAction,
                  syncTargetTimeline, syncTargetLsn, syncTargetMode>>

BuggyRecordStaleTimelineAck(s) ==
    /\ BuggyAcceptStaleTimelineAck
    /\ s \in Slots
    /\ syncTargetLsn > 0
    /\ syncTargetMode # "async"
    /\ slotActive[s]
    /\ ~slotReseed[s]
    /\ slotTimeline[s] # syncTargetTimeline
    /\ ProgressFor(s, syncTargetMode) >= syncTargetLsn
    /\ syncAcked' = TRUE
    /\ syncAckTimeline' = slotTimeline[s]
    /\ syncAckLsn' = ProgressFor(s, syncTargetMode)
    /\ syncAckSlot' = s
    /\ UNCHANGED <<primaryLsn, currentTimeline, currentEpoch, slotTimeline,
                  slotActive, slotReseed, restartLsn, receivedLsn, appliedLsn,
                  safeReadLsn, mode, selection, requiredCount, failurePolicy,
                  durabilityStatus, durabilityProgress, satisfiedCount,
                  candidateCount, fenceHeld, fenceGeneration,
                  fenceParentTimeline, fenceParentEpoch, fenceNewTimeline,
                  fenceNewEpoch, fenceRequiredLsn, fenceObservedLsn, fenceForced,
                  promoted, switchLsn, formerTimeline, formerEpoch,
                  formerLastLsn, retainedFromLsn, rejoinAction,
                  syncTargetTimeline, syncTargetLsn, syncTargetMode>>

Old(action) == action /\ UNCHANGED syncVars

PreSyncNext ==
    \/ Old(AppendPrimary)
    \/ Old(ChoosePolicy)
    \/ Old(EvaluateDurability)
    \/ Old(PromoteStandby)
    \/ Old(ChooseRetentionFloor)
    \/ Old(AssessFormerPrimaryRejoin)
    \/ BeginSyncCommitWait
    \/ \E s \in Slots:
        \/ Old(CreateOrResetSlot(s))
        \/ Old(ReceiveOnSlot(s))
        \/ Old(ApplyOnSlot(s))
        \/ Old(PauseSlot(s))
        \/ Old(ResumeSlot(s))
        \/ Old(MarkReseedRequired(s))
        \/ RecordSyncStatusAck(s)
        \/ BuggyRecordStaleTimelineAck(s)
        \/ \E req \in 1..MaxLsn:
            \E obs \in 0..MaxLsn:
                \E forced \in BOOLEAN:
                    Old(AcquireFence(s, req, obs, forced))

PostSyncNext ==
    \E s \in Slots:
        \/ Old(ReceiveOnSlot(s))
        \/ Old(ApplyOnSlot(s))
        \/ RecordSyncStatusAck(s)
        \/ BuggyRecordStaleTimelineAck(s)

Next ==
    /\ ~syncAcked
    /\ IF syncTargetLsn = 0 THEN PreSyncNext ELSE PostSyncNext

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ primaryLsn \in 0..(MaxLsn + 1)
    /\ currentTimeline \in 1..MaxTimeline
    /\ currentEpoch \in 1..MaxEpoch
    /\ slotTimeline \in [Slots -> 1..MaxTimeline]
    /\ slotActive \in [Slots -> BOOLEAN]
    /\ slotReseed \in [Slots -> BOOLEAN]
    /\ restartLsn \in [Slots -> 0..(MaxLsn + 1)]
    /\ receivedLsn \in [Slots -> 0..MaxLsn]
    /\ appliedLsn \in [Slots -> 0..MaxLsn]
    /\ safeReadLsn \in [Slots -> 0..MaxLsn]
    /\ mode \in Modes
    /\ selection \in Selections
    /\ requiredCount \in 1..2
    /\ failurePolicy \in FailurePolicies
    /\ durabilityStatus \in DurabilityStatuses
    /\ durabilityProgress \in 0..(MaxLsn + 1)
    /\ satisfiedCount \in 0..2
    /\ candidateCount \in 0..2
    /\ fenceHeld \in BOOLEAN
    /\ fenceGeneration \in 0..MaxTimeline
    /\ fenceParentTimeline \in 0..MaxTimeline
    /\ fenceParentEpoch \in 0..MaxEpoch
    /\ fenceNewTimeline \in 0..MaxTimeline
    /\ fenceNewEpoch \in 0..MaxEpoch
    /\ fenceRequiredLsn \in 0..MaxLsn
    /\ fenceObservedLsn \in 0..MaxLsn
    /\ fenceForced \in BOOLEAN
    /\ promoted \in BOOLEAN
    /\ switchLsn \in 0..(MaxLsn + 1)
    /\ formerTimeline \in 1..MaxTimeline
    /\ formerEpoch \in 1..MaxEpoch
    /\ formerLastLsn \in 0..MaxLsn
    /\ retainedFromLsn \in 1..MaxLsn
    /\ rejoinAction \in RejoinActions
    /\ syncTargetTimeline \in 0..MaxTimeline
    /\ syncTargetLsn \in 0..(MaxLsn + 1)
    /\ syncTargetMode \in AckModes
    /\ syncAcked \in BOOLEAN
    /\ syncAckTimeline \in 0..MaxTimeline
    /\ syncAckLsn \in 0..(MaxLsn + 1)
    /\ syncAckSlot \in AckSlots

SlotProgressOrdered ==
    \A s \in Slots:
        /\ safeReadLsn[s] <= appliedLsn[s]
        /\ appliedLsn[s] <= receivedLsn[s]
        /\ (slotTimeline[s] = currentTimeline => receivedLsn[s] <= primaryLsn)
        /\ restartLsn[s] >= receivedLsn[s]

EligibleSlotsAreCurrentActiveAndNotReseed ==
    \A s \in Slots: Eligible(s) => /\ slotActive[s] /\ ~slotReseed[s] /\ slotTimeline[s] = currentTimeline

DurabilityCountsEligibleOnly ==
    /\ candidateCount <= 2
    /\ satisfiedCount <= candidateCount

SatisfiedDurabilityHasEnoughProgress ==
    durabilityStatus = "satisfied"
        /\ mode # "async"
        /\ candidateCount = EligibleCount
        /\ satisfiedCount = AllSatisfiedCount(primaryLsn, mode)
        /\ durabilityProgress = primaryLsn =>
        CASE selection = "all" -> /\ candidateCount = EligibleCount /\ satisfiedCount = EligibleCount /\ candidateCount > 0
          [] selection = "first" -> FirstSatisfies(primaryLsn, mode)
          [] OTHER -> satisfiedCount >= requiredCount

FailClosedDoesNotAcknowledgeUnsatisfied ==
    durabilityStatus = "fail_closed" /\ candidateCount = EligibleCount =>
        /\ mode # "async"
        /\ failurePolicy = "fail_closed"

FenceReceiptValid ==
    fenceHeld =>
        /\ fenceGeneration > 0
        /\ fenceParentTimeline < fenceNewTimeline
        /\ fenceParentEpoch < fenceNewEpoch
        /\ fenceRequiredLsn > 0
        /\ (fenceObservedLsn >= fenceRequiredLsn \/ fenceForced)

PromotionMatchesFenceAndSwitchRecord ==
    promoted =>
        /\ fenceHeld
        /\ currentTimeline = fenceNewTimeline
        /\ currentEpoch = fenceNewEpoch
        /\ switchLsn = primaryLsn
        /\ switchLsn > 0
        /\ (switchLsn > fenceObservedLsn \/ fenceForced)

RewindRequiresRetainedFork ==
    rejoinAction = "rewind" =>
        /\ fenceHeld
        /\ ~fenceForced
        /\ formerTimeline = fenceParentTimeline
        /\ formerEpoch = fenceParentEpoch
        /\ formerLastLsn >= fenceObservedLsn
        /\ retainedFromLsn <= fenceObservedLsn

SyncAckMatchesTargetTimeline ==
    syncAcked =>
        /\ syncTargetTimeline > 0
        /\ syncAckTimeline = syncTargetTimeline
        /\ syncAckSlot \in Slots
        /\ syncAckLsn >= syncTargetLsn

SyncAckProgressMatchesMode ==
    syncAcked =>
        /\ syncTargetMode # "async"
        /\ syncAckLsn >= syncTargetLsn

Safety ==
    /\ TypeOK
    /\ SlotProgressOrdered
    /\ EligibleSlotsAreCurrentActiveAndNotReseed
    /\ DurabilityCountsEligibleOnly
    /\ SatisfiedDurabilityHasEnoughProgress
    /\ FailClosedDoesNotAcknowledgeUnsatisfied
    /\ FenceReceiptValid
    /\ PromotionMatchesFenceAndSwitchRecord
    /\ RewindRequiresRetainedFork
    /\ SyncAckMatchesTargetTimeline
    /\ SyncAckProgressMatchesMode

=============================================================================
