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

-------------------------- MODULE AntflyHATimelineSwitch ----------------------
(*
  Fast submodel for HA timeline-switch boundaries.

  This isolates the lower-level standby/replication-client contract around
  timeline_switch records:

  - a standby may switch only after parent received/applied/safe progress is
    caught up to the switch's previous_lsn;
  - switch timeline and epoch must be monotonic;
  - crash recovery may replay a durable switch only if it is contiguous with the
    recovered parent progress;
  - after switching, records from the old timeline must not be accepted.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggySwitchBeforeApplied,
    BuggyNonMonotonicSwitch,
    BuggyAcceptOldTimeline,
    BuggyRecoverWrongPrevious

Timelines == 1..2
Epochs == 1..2
Lsns == 0..3

VARIABLES
    identityTimeline,
    identityEpoch,
    receivedLsn,
    appliedLsn,
    safeReadLsn,
    durableSwitchPresent,
    switchTimeline,
    switchEpoch,
    switchLsn,
    switchPreviousLsn,
    switchAppliedAtAppend,
    switchReceived,
    oldTimelineAccepted,
    recoveryUsed,
    recoveredFromReceived

vars ==
    <<identityTimeline, identityEpoch, receivedLsn, appliedLsn, safeReadLsn,
      durableSwitchPresent, switchTimeline, switchEpoch, switchLsn,
      switchPreviousLsn, switchAppliedAtAppend, switchReceived,
      oldTimelineAccepted, recoveryUsed, recoveredFromReceived>>

Init ==
    /\ identityTimeline = 1
    /\ identityEpoch = 1
    /\ receivedLsn = 0
    /\ appliedLsn = 0
    /\ safeReadLsn = 0
    /\ durableSwitchPresent = FALSE
    /\ switchTimeline = 0
    /\ switchEpoch = 0
    /\ switchLsn = 0
    /\ switchPreviousLsn = 0
    /\ switchAppliedAtAppend = FALSE
    /\ switchReceived = FALSE
    /\ oldTimelineAccepted = FALSE
    /\ recoveryUsed = FALSE
    /\ recoveredFromReceived = 0

SwitchTimelines ==
    IF BuggyNonMonotonicSwitch THEN Timelines ELSE {2}

SwitchEpochs ==
    IF BuggyNonMonotonicSwitch THEN Epochs ELSE {2}

ParentProgressCaughtUp ==
    /\ appliedLsn = receivedLsn
    /\ safeReadLsn = appliedLsn

ReceiveParentRecord ==
    /\ ~durableSwitchPresent
    /\ ~switchReceived
    /\ receivedLsn < 2
    /\ receivedLsn' = receivedLsn + 1
    /\ UNCHANGED <<identityTimeline, identityEpoch, appliedLsn, safeReadLsn,
                  durableSwitchPresent, switchTimeline, switchEpoch,
                  switchLsn, switchPreviousLsn, switchAppliedAtAppend,
                  switchReceived, oldTimelineAccepted, recoveryUsed,
                  recoveredFromReceived>>

ApplyParentRecord ==
    /\ ~switchReceived
    /\ appliedLsn < receivedLsn
    /\ appliedLsn' = appliedLsn + 1
    /\ UNCHANGED <<identityTimeline, identityEpoch, receivedLsn, safeReadLsn,
                  durableSwitchPresent, switchTimeline, switchEpoch,
                  switchLsn, switchPreviousLsn, switchAppliedAtAppend,
                  switchReceived, oldTimelineAccepted, recoveryUsed,
                  recoveredFromReceived>>

AdvanceSafeRead ==
    /\ ~switchReceived
    /\ safeReadLsn < appliedLsn
    /\ safeReadLsn' = safeReadLsn + 1
    /\ UNCHANGED <<identityTimeline, identityEpoch, receivedLsn, appliedLsn,
                  durableSwitchPresent, switchTimeline, switchEpoch,
                  switchLsn, switchPreviousLsn, switchAppliedAtAppend,
                  switchReceived, oldTimelineAccepted, recoveryUsed,
                  recoveredFromReceived>>

ReceiveTimelineSwitch ==
    /\ ~durableSwitchPresent
    /\ ~switchReceived
    /\ receivedLsn < 3
    /\ IF BuggySwitchBeforeApplied THEN TRUE ELSE ParentProgressCaughtUp
    /\ \E t \in SwitchTimelines:
       \E e \in SwitchEpochs:
          /\ durableSwitchPresent' = TRUE
          /\ switchTimeline' = t
          /\ switchEpoch' = e
          /\ switchPreviousLsn' = receivedLsn
          /\ switchLsn' = receivedLsn + 1
          /\ switchAppliedAtAppend' = ParentProgressCaughtUp
          /\ identityTimeline' = t
          /\ identityEpoch' = e
          /\ receivedLsn' = receivedLsn + 1
          /\ appliedLsn' = receivedLsn + 1
          /\ safeReadLsn' = receivedLsn + 1
          /\ switchReceived' = TRUE
          /\ UNCHANGED <<oldTimelineAccepted, recoveryUsed,
                        recoveredFromReceived>>

DurableSwitchBeforeProgressPersisted ==
    /\ ~durableSwitchPresent
    /\ ~switchReceived
    /\ receivedLsn < 3
    /\ IF BuggySwitchBeforeApplied THEN TRUE ELSE ParentProgressCaughtUp
    /\ \E t \in SwitchTimelines:
       \E e \in SwitchEpochs:
          /\ durableSwitchPresent' = TRUE
          /\ switchTimeline' = t
          /\ switchEpoch' = e
          /\ switchPreviousLsn' =
              IF BuggyRecoverWrongPrevious /\ receivedLsn > 0 THEN
                  receivedLsn - 1
              ELSE
                  receivedLsn
          /\ switchLsn' =
              IF BuggyRecoverWrongPrevious /\ receivedLsn > 0 THEN
                  receivedLsn
              ELSE
                  receivedLsn + 1
          /\ switchAppliedAtAppend' = ParentProgressCaughtUp
          /\ UNCHANGED <<identityTimeline, identityEpoch, receivedLsn,
                        appliedLsn, safeReadLsn, switchReceived,
                        oldTimelineAccepted, recoveryUsed,
                        recoveredFromReceived>>

RecoverDurableTimelineSwitch ==
    /\ durableSwitchPresent
    /\ ~switchReceived
    /\ IF BuggyRecoverWrongPrevious THEN TRUE ELSE switchPreviousLsn = receivedLsn
    /\ IF BuggySwitchBeforeApplied THEN TRUE ELSE ParentProgressCaughtUp
    /\ identityTimeline' = switchTimeline
    /\ identityEpoch' = switchEpoch
    /\ receivedLsn' = switchLsn
    /\ appliedLsn' = switchLsn
    /\ safeReadLsn' = switchLsn
    /\ switchReceived' = TRUE
    /\ recoveryUsed' = TRUE
    /\ recoveredFromReceived' = receivedLsn
    /\ UNCHANGED <<durableSwitchPresent, switchTimeline, switchEpoch,
                  switchLsn, switchPreviousLsn, switchAppliedAtAppend,
                  oldTimelineAccepted>>

ReceiveCurrentTimelineRecord ==
    /\ switchReceived
    /\ receivedLsn < 3
    /\ receivedLsn' = receivedLsn + 1
    /\ UNCHANGED <<identityTimeline, identityEpoch, appliedLsn, safeReadLsn,
                  durableSwitchPresent, switchTimeline, switchEpoch,
                  switchLsn, switchPreviousLsn, switchAppliedAtAppend,
                  switchReceived, oldTimelineAccepted, recoveryUsed,
                  recoveredFromReceived>>

ReceiveOldTimelineRecordAfterSwitch ==
    /\ BuggyAcceptOldTimeline
    /\ switchReceived
    /\ receivedLsn < 3
    /\ receivedLsn' = receivedLsn + 1
    /\ oldTimelineAccepted' = TRUE
    /\ UNCHANGED <<identityTimeline, identityEpoch, appliedLsn, safeReadLsn,
                  durableSwitchPresent, switchTimeline, switchEpoch,
                  switchLsn, switchPreviousLsn, switchAppliedAtAppend,
                  switchReceived, recoveryUsed, recoveredFromReceived>>

Next ==
    \/ ReceiveParentRecord
    \/ ApplyParentRecord
    \/ AdvanceSafeRead
    \/ ReceiveTimelineSwitch
    \/ DurableSwitchBeforeProgressPersisted
    \/ RecoverDurableTimelineSwitch
    \/ ReceiveCurrentTimelineRecord
    \/ ReceiveOldTimelineRecordAfterSwitch

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ identityTimeline \in Timelines
    /\ identityEpoch \in Epochs
    /\ receivedLsn \in Lsns
    /\ appliedLsn \in Lsns
    /\ safeReadLsn \in Lsns
    /\ durableSwitchPresent \in BOOLEAN
    /\ switchTimeline \in 0..2
    /\ switchEpoch \in 0..2
    /\ switchLsn \in Lsns
    /\ switchPreviousLsn \in Lsns
    /\ switchAppliedAtAppend \in BOOLEAN
    /\ switchReceived \in BOOLEAN
    /\ oldTimelineAccepted \in BOOLEAN
    /\ recoveryUsed \in BOOLEAN
    /\ recoveredFromReceived \in Lsns

ProgressOrder ==
    /\ safeReadLsn <= appliedLsn
    /\ appliedLsn <= receivedLsn

SwitchRecordIsMonotonic ==
    durableSwitchPresent =>
        /\ switchTimeline > 1
        /\ switchEpoch > 1

SwitchRequiresAppliedProgress ==
    durableSwitchPresent => switchAppliedAtAppend

SwitchRecordIsContiguous ==
    durableSwitchPresent => switchLsn = switchPreviousLsn + 1

SwitchedIdentityMatchesRecord ==
    switchReceived =>
        /\ identityTimeline = switchTimeline
        /\ identityEpoch = switchEpoch
        /\ receivedLsn >= switchLsn
        /\ appliedLsn >= switchLsn
        /\ safeReadLsn >= switchLsn

OldTimelineRejectedAfterSwitch ==
    ~oldTimelineAccepted

RecoveryUsesRecoveredProgress ==
    recoveryUsed => switchPreviousLsn = recoveredFromReceived

Safety ==
    /\ TypeOK
    /\ ProgressOrder
    /\ SwitchRecordIsMonotonic
    /\ SwitchRequiresAppliedProgress
    /\ SwitchRecordIsContiguous
    /\ SwitchedIdentityMatchesRecord
    /\ OldTimelineRejectedAfterSwitch
    /\ RecoveryUsesRecoveredProgress

=============================================================================
