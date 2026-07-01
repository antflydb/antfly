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

---------------------------- MODULE AntflyHASyncWait --------------------------
(*
  Fast submodel for HA sync-wait target provenance.

  AntflyHAReplication.tla keeps the broad replication/promotion/rejoin model in
  the heavy tier. This smaller model isolates one critical contract: once a
  sync wait freezes a target timeline/LSN, accepted standby acks must match that
  frozen target. Later timeline changes or lower-LSN status reports cannot
  satisfy the original write.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyMoveFrozenTarget,
    BuggyAckWrongTimeline,
    BuggyAckBelowTarget

Slots == {"standbyA", "standbyB"}
Timelines == 1..2
Lsns == 0..2

VARIABLES
    currentTimeline,
    primaryLsn,
    targetFrozen,
    syncTargetTimeline,
    syncTargetLsn,
    frozenTimeline,
    frozenLsn,
    slotTimeline,
    appliedLsn,
    acked,
    ackSlot,
    ackSourceTimeline,
    ackSourceLsn,
    ackTimeline,
    ackLsn

vars ==
    <<currentTimeline, primaryLsn, targetFrozen, syncTargetTimeline,
      syncTargetLsn, frozenTimeline, frozenLsn, slotTimeline, appliedLsn,
      acked, ackSlot, ackSourceTimeline, ackSourceLsn, ackTimeline, ackLsn>>

Init ==
    /\ currentTimeline = 1
    /\ primaryLsn = 0
    /\ targetFrozen = FALSE
    /\ syncTargetTimeline = 0
    /\ syncTargetLsn = 0
    /\ frozenTimeline = 0
    /\ frozenLsn = 0
    /\ slotTimeline = [s \in Slots |-> 1]
    /\ appliedLsn = [s \in Slots |-> 0]
    /\ acked = FALSE
    /\ ackSlot = "none"
    /\ ackSourceTimeline = 0
    /\ ackSourceLsn = 0
    /\ ackTimeline = 0
    /\ ackLsn = 0

AppendWrite ==
    /\ primaryLsn < 2
    /\ primaryLsn' = primaryLsn + 1
    /\ UNCHANGED <<currentTimeline, targetFrozen, syncTargetTimeline,
                  syncTargetLsn, frozenTimeline, frozenLsn, slotTimeline,
                  appliedLsn, acked, ackSlot, ackSourceTimeline, ackSourceLsn,
                  ackTimeline, ackLsn>>

FreezeSyncTarget ==
    /\ ~targetFrozen
    /\ primaryLsn > 0
    /\ targetFrozen' = TRUE
    /\ syncTargetTimeline' = currentTimeline
    /\ syncTargetLsn' = primaryLsn
    /\ frozenTimeline' = currentTimeline
    /\ frozenLsn' = primaryLsn
    /\ UNCHANGED <<currentTimeline, primaryLsn, slotTimeline, appliedLsn,
                  acked, ackSlot, ackSourceTimeline, ackSourceLsn, ackTimeline,
                  ackLsn>>

PromoteNewTimeline ==
    /\ currentTimeline = 1
    /\ currentTimeline' = 2
    /\ primaryLsn' = 0
    /\ IF BuggyMoveFrozenTarget /\ targetFrozen THEN
          /\ syncTargetTimeline' = 2
          /\ syncTargetLsn' = 0
       ELSE
          /\ UNCHANGED <<syncTargetTimeline, syncTargetLsn>>
    /\ UNCHANGED <<targetFrozen, frozenTimeline, frozenLsn, slotTimeline,
                  appliedLsn, acked, ackSlot, ackSourceTimeline, ackSourceLsn,
                  ackTimeline, ackLsn>>

SlotJoinsCurrentTimeline(s) ==
    /\ s \in Slots
    /\ slotTimeline[s] # currentTimeline
    /\ slotTimeline' = [slotTimeline EXCEPT ![s] = currentTimeline]
    /\ appliedLsn' = [appliedLsn EXCEPT ![s] = 0]
    /\ UNCHANGED <<currentTimeline, primaryLsn, targetFrozen,
                  syncTargetTimeline, syncTargetLsn, frozenTimeline, frozenLsn,
                  acked, ackSlot, ackSourceTimeline, ackSourceLsn, ackTimeline,
                  ackLsn>>

ApplyOnSlot(s) ==
    /\ s \in Slots
    /\ appliedLsn[s] < 2
    /\ appliedLsn' = [appliedLsn EXCEPT ![s] = appliedLsn[s] + 1]
    /\ UNCHANGED <<currentTimeline, primaryLsn, targetFrozen,
                  syncTargetTimeline, syncTargetLsn, frozenTimeline, frozenLsn,
                  slotTimeline, acked, ackSlot, ackSourceTimeline,
                  ackSourceLsn, ackTimeline, ackLsn>>

ReportAck(s) ==
    /\ s \in Slots
    /\ targetFrozen
    /\ ~acked
    /\ IF BuggyAckWrongTimeline THEN TRUE ELSE slotTimeline[s] = syncTargetTimeline
    /\ IF BuggyAckBelowTarget THEN TRUE ELSE appliedLsn[s] >= syncTargetLsn
    /\ acked' = TRUE
    /\ ackSlot' = s
    /\ ackSourceTimeline' = slotTimeline[s]
    /\ ackSourceLsn' = appliedLsn[s]
    /\ ackTimeline' =
        IF BuggyAckWrongTimeline THEN slotTimeline[s] ELSE syncTargetTimeline
    /\ ackLsn' =
        IF BuggyAckBelowTarget /\ appliedLsn[s] < syncTargetLsn THEN
            appliedLsn[s]
        ELSE
            syncTargetLsn
    /\ UNCHANGED <<currentTimeline, primaryLsn, targetFrozen,
                  syncTargetTimeline, syncTargetLsn, frozenTimeline, frozenLsn,
                  slotTimeline, appliedLsn>>

Next ==
    \/ AppendWrite
    \/ FreezeSyncTarget
    \/ PromoteNewTimeline
    \/ \E s \in Slots:
        \/ SlotJoinsCurrentTimeline(s)
        \/ ApplyOnSlot(s)
        \/ ReportAck(s)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ currentTimeline \in Timelines
    /\ primaryLsn \in Lsns
    /\ targetFrozen \in BOOLEAN
    /\ syncTargetTimeline \in 0..2
    /\ syncTargetLsn \in Lsns
    /\ frozenTimeline \in 0..2
    /\ frozenLsn \in Lsns
    /\ slotTimeline \in [Slots -> Timelines]
    /\ appliedLsn \in [Slots -> Lsns]
    /\ acked \in BOOLEAN
    /\ ackSlot \in Slots \cup {"none"}
    /\ ackSourceTimeline \in 0..2
    /\ ackSourceLsn \in Lsns
    /\ ackTimeline \in 0..2
    /\ ackLsn \in Lsns

FrozenTargetDoesNotMove ==
    targetFrozen =>
        /\ syncTargetTimeline = frozenTimeline
        /\ syncTargetLsn = frozenLsn

AckMatchesFrozenTimeline ==
    acked => ackTimeline = frozenTimeline

AckSatisfiesFrozenLsn ==
    acked => ackLsn >= frozenLsn

AckComesFromMatchingSlot ==
    IF acked THEN
        /\ ackSlot \in Slots
        /\ ackSourceTimeline = frozenTimeline
    ELSE TRUE

AckSlotAppliedEnough ==
    IF acked THEN
        /\ ackSlot \in Slots
        /\ ackSourceLsn >= frozenLsn
    ELSE TRUE

Safety ==
    /\ TypeOK
    /\ FrozenTargetDoesNotMove
    /\ AckMatchesFrozenTimeline
    /\ AckSatisfiesFrozenLsn
    /\ AckComesFromMatchingSlot
    /\ AckSlotAppliedEnough

=============================================================================
