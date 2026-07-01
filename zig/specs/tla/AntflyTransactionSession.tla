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

------------------------ MODULE AntflyTransactionSession ------------------------
(*
  Session/savepoint and transaction recovery overlay for the existing
  distributed transaction model.

  Concrete Zig contracts modeled:
    - writeTransaction/writeIntents stage intent data that is not visible until
      resolve/commit.
    - rollback-to-savepoint discards later staged intent data.
    - all participants must prepare before a coordinator commit can publish.
    - recoverTransactions auto-aborts stale pending intents.
    - finalized committed orphan intents publish document and identity rows;
      finalized aborted orphan intents publish neither.
    - finalized records are not cleaned while participants remain unresolved.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyRollbackExposeDiscarded,
    BuggyRecoveryWrongDecision,
    BuggyCleanupUnresolved

Participants == {"local", "remote"}
Statuses == {"idle", "open", "prepared", "committed", "aborted"}
MaxStage == 2
MaxVisible == 4

VARIABLES
    status,
    baseVisible,
    staged,
    intentCount,
    savepoint,
    participantPrepared,
    participantResolved,
    visible,
    identityRows,
    conflict,
    stalePending,
    rollbackDiscarded,
    cleanedWithUnresolved

vars == <<status, baseVisible, staged, intentCount, savepoint,
          participantPrepared, participantResolved, visible, identityRows,
          conflict, stalePending, rollbackDiscarded, cleanedWithUnresolved>>

Init ==
    /\ status = "idle"
    /\ baseVisible = 0
    /\ staged = 0
    /\ intentCount = 0
    /\ savepoint = 0
    /\ participantPrepared = [p \in Participants |-> FALSE]
    /\ participantResolved = [p \in Participants |-> FALSE]
    /\ visible = 0
    /\ identityRows = 0
    /\ conflict = FALSE
    /\ stalePending = FALSE
    /\ rollbackDiscarded = FALSE
    /\ cleanedWithUnresolved = FALSE

AllPrepared(prepared) ==
    \A p \in Participants: prepared[p]

AllResolved ==
    \A p \in Participants: participantResolved[p]

AllPreparedParticipantsResolved ==
    \A p \in Participants: participantPrepared[p] => participantResolved[p]

BeginSession ==
    /\ status = "idle"
    /\ status' = "open"
    /\ baseVisible' = visible
    /\ staged' = 0
    /\ intentCount' = 0
    /\ savepoint' = 0
    /\ participantPrepared' = [p \in Participants |-> FALSE]
    /\ participantResolved' = [p \in Participants |-> FALSE]
    /\ conflict' = FALSE
    /\ stalePending' = FALSE
    /\ rollbackDiscarded' = FALSE
    /\ UNCHANGED <<visible, identityRows, cleanedWithUnresolved>>

StageWrite ==
    /\ status = "open"
    /\ staged < MaxStage
    /\ baseVisible + staged < MaxVisible
    /\ staged' = staged + 1
    /\ intentCount' = intentCount + 1
    /\ UNCHANGED <<status, baseVisible, savepoint, participantPrepared,
                  participantResolved, visible, identityRows, conflict,
                  stalePending, rollbackDiscarded, cleanedWithUnresolved>>

CreateSavepoint ==
    /\ status = "open"
    /\ savepoint' = staged
    /\ UNCHANGED <<status, baseVisible, staged, intentCount,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved>>

RollbackToSavepoint ==
    /\ status = "open"
    /\ savepoint <= staged
    /\ staged' = savepoint
    /\ intentCount' = savepoint
    /\ rollbackDiscarded' = (staged > savepoint)
    /\ UNCHANGED <<status, baseVisible, savepoint, participantPrepared,
                  participantResolved, visible, identityRows, conflict,
                  stalePending, cleanedWithUnresolved>>

BuggyRollbackToSavepoint ==
    /\ BuggyRollbackExposeDiscarded
    /\ status = "open"
    /\ savepoint < staged
    /\ staged' = savepoint
    /\ intentCount' = savepoint
    /\ visible' = baseVisible + staged
    /\ identityRows' = visible'
    /\ rollbackDiscarded' = TRUE
    /\ UNCHANGED <<status, baseVisible, savepoint, participantPrepared,
                  participantResolved, conflict, stalePending,
                  cleanedWithUnresolved>>

PrepareParticipant(p) ==
    /\ p \in Participants
    /\ status = "open"
    /\ intentCount > 0
    /\ ~conflict
    /\ participantPrepared' = [participantPrepared EXCEPT ![p] = TRUE]
    /\ status' =
        IF AllPrepared([participantPrepared EXCEPT ![p] = TRUE])
        THEN "prepared"
        ELSE status
    /\ UNCHANGED <<baseVisible, staged, intentCount, savepoint,
                  participantResolved, visible, identityRows, conflict,
                  stalePending, rollbackDiscarded, cleanedWithUnresolved>>

DetectConflict ==
    /\ status = "open"
    /\ conflict' = TRUE
    /\ status' = "aborted"
    /\ intentCount' = 0
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, visible, identityRows, stalePending,
                  rollbackDiscarded, cleanedWithUnresolved>>

Commit ==
    /\ status = "prepared"
    /\ status' = "committed"
    /\ visible' = baseVisible + intentCount
    /\ identityRows' = baseVisible + intentCount
    /\ intentCount' = 0
    /\ stalePending' = FALSE
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, rollbackDiscarded,
                  cleanedWithUnresolved>>

Abort ==
    /\ status \in {"open", "prepared"}
    /\ status' = "aborted"
    /\ visible' = baseVisible
    /\ identityRows' = baseVisible
    /\ intentCount' = 0
    /\ stalePending' = FALSE
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, rollbackDiscarded,
                  cleanedWithUnresolved>>

MarkStalePending ==
    /\ status = "open"
    /\ stalePending' = TRUE
    /\ UNCHANGED <<status, baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, rollbackDiscarded,
                  cleanedWithUnresolved>>

RecoverStalePending ==
    /\ status = "open"
    /\ stalePending
    /\ status' = "aborted"
    /\ visible' = baseVisible
    /\ identityRows' = baseVisible
    /\ intentCount' = 0
    /\ stalePending' = FALSE
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, rollbackDiscarded,
                  cleanedWithUnresolved>>

CrashFinalizeCommittedOrphan ==
    /\ status = "prepared"
    /\ intentCount > 0
    /\ status' = "committed"
    /\ UNCHANGED <<baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved>>

CrashFinalizeAbortedOrphan ==
    /\ status \in {"open", "prepared"}
    /\ intentCount > 0
    /\ status' = "aborted"
    /\ stalePending' = FALSE
    /\ UNCHANGED <<baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, rollbackDiscarded,
                  cleanedWithUnresolved>>

RecoverFinalizedIntents ==
    /\ status \in {"committed", "aborted"}
    /\ intentCount > 0
    /\ visible' = IF status = "committed" THEN baseVisible + intentCount ELSE baseVisible
    /\ identityRows' = visible'
    /\ intentCount' = 0
    /\ UNCHANGED <<status, baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved>>

BuggyRecoverWrongDecision ==
    /\ BuggyRecoveryWrongDecision
    /\ status = "aborted"
    /\ intentCount > 0
    /\ visible' = baseVisible + intentCount
    /\ identityRows' = visible'
    /\ intentCount' = 0
    /\ UNCHANGED <<status, baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved>>

ResolveParticipant(p) ==
    /\ p \in Participants
    /\ status \in {"committed", "aborted"}
    /\ participantPrepared[p]
    /\ participantResolved' = [participantResolved EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<status, baseVisible, staged, intentCount, savepoint,
                  participantPrepared, visible, identityRows, conflict,
                  stalePending, rollbackDiscarded, cleanedWithUnresolved>>

Cleanup ==
    /\ status \in {"committed", "aborted"}
    /\ intentCount = 0
    /\ AllPreparedParticipantsResolved
    /\ status' = "idle"
    /\ baseVisible' = visible
    /\ staged' = 0
    /\ savepoint' = 0
    /\ participantPrepared' = [p \in Participants |-> FALSE]
    /\ participantResolved' = [p \in Participants |-> FALSE]
    /\ conflict' = FALSE
    /\ stalePending' = FALSE
    /\ rollbackDiscarded' = FALSE
    /\ UNCHANGED <<intentCount, visible, identityRows, cleanedWithUnresolved>>

BuggyCleanupBeforeResolved ==
    /\ BuggyCleanupUnresolved
    /\ status \in {"committed", "aborted"}
    /\ intentCount = 0
    /\ ~AllPreparedParticipantsResolved
    /\ status' = "idle"
    /\ baseVisible' = visible
    /\ staged' = 0
    /\ savepoint' = 0
    /\ participantPrepared' = [p \in Participants |-> FALSE]
    /\ participantResolved' = [p \in Participants |-> FALSE]
    /\ conflict' = FALSE
    /\ stalePending' = FALSE
    /\ rollbackDiscarded' = FALSE
    /\ cleanedWithUnresolved' = TRUE
    /\ UNCHANGED <<intentCount, visible, identityRows>>

Next ==
    \/ BeginSession
    \/ StageWrite
    \/ CreateSavepoint
    \/ RollbackToSavepoint
    \/ BuggyRollbackToSavepoint
    \/ DetectConflict
    \/ Commit
    \/ Abort
    \/ MarkStalePending
    \/ RecoverStalePending
    \/ CrashFinalizeCommittedOrphan
    \/ CrashFinalizeAbortedOrphan
    \/ RecoverFinalizedIntents
    \/ BuggyRecoverWrongDecision
    \/ Cleanup
    \/ BuggyCleanupBeforeResolved
    \/ \E p \in Participants:
        \/ PrepareParticipant(p)
        \/ ResolveParticipant(p)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ status \in Statuses
    /\ baseVisible \in 0..MaxVisible
    /\ staged \in 0..MaxStage
    /\ intentCount \in 0..MaxStage
    /\ savepoint \in 0..MaxStage
    /\ participantPrepared \in [Participants -> BOOLEAN]
    /\ participantResolved \in [Participants -> BOOLEAN]
    /\ visible \in 0..MaxVisible
    /\ identityRows \in 0..MaxVisible
    /\ conflict \in BOOLEAN
    /\ stalePending \in BOOLEAN
    /\ rollbackDiscarded \in BOOLEAN
    /\ cleanedWithUnresolved \in BOOLEAN

NoVisibleUncommittedWrites ==
    status \in {"open", "prepared", "aborted"} => visible = baseVisible

CommitRequiresAllPrepared ==
    status = "committed" => AllPrepared(participantPrepared)

CommitPublishesStagedWrites ==
    status = "committed" /\ intentCount = 0 => visible = baseVisible + staged

AbortPreservesVisibility ==
    status = "aborted" => visible = baseVisible

SavepointWithinStage ==
    savepoint <= staged

RollbackDiscardedNotVisible ==
    status = "open" /\ rollbackDiscarded => visible = baseVisible

IntentCountWithinStagedWrites ==
    intentCount <= staged

IdentityRowsMatchVisibleDocs ==
    identityRows = visible

ParticipantRecoveryDoesNotPublishAbortedData ==
    status = "aborted" => /\ visible = baseVisible /\ identityRows = baseVisible

CleanupRequiresAllResolved ==
    ~cleanedWithUnresolved

IdleHasNoDanglingIntents ==
    status = "idle" => intentCount = 0

Safety ==
    /\ TypeOK
    /\ NoVisibleUncommittedWrites
    /\ CommitRequiresAllPrepared
    /\ CommitPublishesStagedWrites
    /\ AbortPreservesVisibility
    /\ SavepointWithinStage
    /\ RollbackDiscardedNotVisible
    /\ IntentCountWithinStagedWrites
    /\ IdentityRowsMatchVisibleDocs
    /\ ParticipantRecoveryDoesNotPublishAbortedData
    /\ CleanupRequiresAllResolved
    /\ IdleHasNoDanglingIntents

=============================================================================
