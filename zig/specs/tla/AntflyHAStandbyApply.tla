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

--------------------------- MODULE AntflyHAStandbyApply -----------------------
(*
  Fast submodel for standby durable receive, apply replay, and standby
  mutation suppression.

  This slices the contracts exercised by standby.applyAvailable, DB
  applyHAReplicationRecord, standby write gates, and standby background runtime
  startup:

  - received WAL may get ahead of applied progress and must survive restart;
  - failed apply must not advance applied/safe progress or the DB marker;
  - duplicate replicated apply is idempotent;
  - standby client writes and mutating background runtimes stay suppressed.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyApplyFailureAdvances,
    BuggyDuplicateEffect,
    BuggyCrashLosesReceive,
    BuggyClientWrite,
    BuggyBackgroundRuntime

Lsns == 0..2
EffectCounts == 0..2

VARIABLES
    receivedLsn,
    appliedLsn,
    safeReadLsn,
    haMarkerLsn,
    effectCount,
    failedPending,
    failedLsn,
    crashed,
    crashReceivedSnapshot,
    clientWriteAccepted,
    backgroundRuntimeRan

vars ==
    <<receivedLsn, appliedLsn, safeReadLsn, haMarkerLsn, effectCount,
      failedPending, failedLsn, crashed, crashReceivedSnapshot,
      clientWriteAccepted, backgroundRuntimeRan>>

Init ==
    /\ receivedLsn = 0
    /\ appliedLsn = 0
    /\ safeReadLsn = 0
    /\ haMarkerLsn = 0
    /\ effectCount = 0
    /\ failedPending = FALSE
    /\ failedLsn = 0
    /\ crashed = FALSE
    /\ crashReceivedSnapshot = 0
    /\ clientWriteAccepted = FALSE
    /\ backgroundRuntimeRan = FALSE

ReceiveRecord ==
    /\ receivedLsn < 2
    /\ receivedLsn' = receivedLsn + 1
    /\ UNCHANGED <<appliedLsn, safeReadLsn, haMarkerLsn, effectCount,
                  failedPending, failedLsn, crashed, crashReceivedSnapshot,
                  clientWriteAccepted, backgroundRuntimeRan>>

ApplyNextSuccess ==
    /\ appliedLsn < receivedLsn
    /\ ~failedPending \/ failedLsn = appliedLsn + 1
    /\ appliedLsn' = appliedLsn + 1
    /\ safeReadLsn' = appliedLsn + 1
    /\ haMarkerLsn' = appliedLsn + 1
    /\ effectCount' = effectCount + 1
    /\ failedPending' = FALSE
    /\ failedLsn' = 0
    /\ UNCHANGED <<receivedLsn, crashed, crashReceivedSnapshot,
                  clientWriteAccepted, backgroundRuntimeRan>>

ApplyNextFailure ==
    /\ appliedLsn < receivedLsn
    /\ ~failedPending
    /\ failedPending' = TRUE
    /\ failedLsn' = appliedLsn + 1
    /\ IF BuggyApplyFailureAdvances THEN
          /\ appliedLsn' = appliedLsn + 1
          /\ safeReadLsn' = appliedLsn + 1
          /\ haMarkerLsn' = appliedLsn + 1
       ELSE
          /\ UNCHANGED <<appliedLsn, safeReadLsn, haMarkerLsn>>
    /\ UNCHANGED <<receivedLsn, effectCount, crashed, crashReceivedSnapshot,
                  clientWriteAccepted, backgroundRuntimeRan>>

DuplicateApplyAlreadyMarked ==
    /\ haMarkerLsn > 0
    /\ IF BuggyDuplicateEffect THEN
          effectCount' = effectCount + 1
       ELSE
          UNCHANGED effectCount
    /\ UNCHANGED <<receivedLsn, appliedLsn, safeReadLsn, haMarkerLsn,
                  failedPending, failedLsn, crashed, crashReceivedSnapshot,
                  clientWriteAccepted, backgroundRuntimeRan>>

CrashAndReopen ==
    /\ ~crashed
    /\ crashed' = TRUE
    /\ crashReceivedSnapshot' = receivedLsn
    /\ IF BuggyCrashLosesReceive THEN
          receivedLsn' = appliedLsn
       ELSE
          UNCHANGED receivedLsn
    /\ UNCHANGED <<appliedLsn, safeReadLsn, haMarkerLsn, effectCount,
                  failedPending, failedLsn, clientWriteAccepted,
                  backgroundRuntimeRan>>

AttemptClientWriteOnStandby ==
    /\ BuggyClientWrite
    /\ clientWriteAccepted' = TRUE
    /\ UNCHANGED <<receivedLsn, appliedLsn, safeReadLsn, haMarkerLsn,
                  effectCount, failedPending, failedLsn, crashed,
                  crashReceivedSnapshot, backgroundRuntimeRan>>

StartMutatingRuntimeOnStandby ==
    /\ BuggyBackgroundRuntime
    /\ backgroundRuntimeRan' = TRUE
    /\ UNCHANGED <<receivedLsn, appliedLsn, safeReadLsn, haMarkerLsn,
                  effectCount, failedPending, failedLsn, crashed,
                  crashReceivedSnapshot, clientWriteAccepted>>

Next ==
    \/ ReceiveRecord
    \/ ApplyNextSuccess
    \/ ApplyNextFailure
    \/ DuplicateApplyAlreadyMarked
    \/ CrashAndReopen
    \/ AttemptClientWriteOnStandby
    \/ StartMutatingRuntimeOnStandby

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ receivedLsn \in Lsns
    /\ appliedLsn \in Lsns
    /\ safeReadLsn \in Lsns
    /\ haMarkerLsn \in Lsns
    /\ effectCount \in EffectCounts
    /\ failedPending \in BOOLEAN
    /\ failedLsn \in Lsns
    /\ crashed \in BOOLEAN
    /\ crashReceivedSnapshot \in Lsns
    /\ clientWriteAccepted \in BOOLEAN
    /\ backgroundRuntimeRan \in BOOLEAN

ProgressOrder ==
    /\ safeReadLsn <= appliedLsn
    /\ appliedLsn <= receivedLsn
    /\ haMarkerLsn <= appliedLsn

FailedApplyDoesNotAdvance ==
    failedPending =>
        /\ appliedLsn < failedLsn
        /\ safeReadLsn < failedLsn
        /\ haMarkerLsn < failedLsn

ReplicatedApplyIsIdempotent ==
    effectCount = haMarkerLsn

DurableReceiveSurvivesCrash ==
    crashed => receivedLsn >= crashReceivedSnapshot

ClientWritesRejectedOnStandby ==
    ~clientWriteAccepted

MutatingRuntimeSuppressedOnStandby ==
    ~backgroundRuntimeRan

Safety ==
    /\ TypeOK
    /\ ProgressOrder
    /\ FailedApplyDoesNotAdvance
    /\ ReplicatedApplyIsIdempotent
    /\ DurableReceiveSurvivesCrash
    /\ ClientWritesRejectedOnStandby
    /\ MutatingRuntimeSuppressedOnStandby

=============================================================================
