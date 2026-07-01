\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

-------------------------- MODULE AntflyHARetentionReseed --------------------------
(*
  WAL retention floor, reseed marking, slot GC, and backup-slot pinning.

  Implementation correspondence:
  - The retention floor is min(restart_lsn) over ACTIVE, NON-reseed-marked
    slots (storage/ha/slot_store.zig:295-349 retentionSnapshotInternal);
    slots whose lag exceeds policy.max_lag_lsn are marked reseed_required in
    a per-slot loop (slot_store.zig:337-339), each mark persisted separately,
    so a crash can leave the loop partially applied — the model's MarkReseed
    is naturally per-slot.
  - A standby whose fork LSN is below the retained floor must reseed
    (storage/ha/rejoin.zig:157-159 parent_timeline_wal_expired).
  - Base backups pin WAL through a slot created at begin
    (storage/ha/primary.zig:481-525 beginBaseBackup) and validated at end
    (validateBackupSlotRetention: slot exists, active, not reseed_required,
    restart_lsn <= backup_lsn). Retention policy treats the backup slot like
    any slot, so a slow backup CAN be reseed-marked and its WAL truncated —
    the contract is that endBaseBackup then FAILS CLOSED, never reports a
    successful backup whose WAL was truncated.

  Deliberate omissions: timelines (old-timeline slot eligibility is covered
  by AntflyHAReplication), retained bytes/age policy dimensions (only the
  lag dimension is modeled), WAL segment layout, and the operator
  reconciliation cadence. Two replication slots plus one backup slot.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyTruncateIgnoresUnmarkedSlot, BuggyBackupEndIgnoresLostWal

ReplSlots == {"s1", "s2"}
Slots == ReplSlots \cup {"backup"}
MaxLsn == 3
MaxLag == 1

VARIABLES
    primaryLsn,
    slotActive,        \* [Slots -> BOOLEAN]
    reseedMarked,      \* [Slots -> BOOLEAN]
    restartLsn,        \* [Slots -> 0..MaxLsn]
    truncatedBelow,    \* WAL strictly below this LSN is deleted
    backupInFlight,
    backupLsn,
    backupSucceededWithoutWal \* ghost: backup reported success though its WAL was lost

vars == <<primaryLsn, slotActive, reseedMarked, restartLsn, truncatedBelow,
          backupInFlight, backupLsn, backupSucceededWithoutWal>>

\* Slots that pin the retention floor: active and not reseed-marked.
PinningSlots == {s \in Slots: slotActive[s] /\ ~reseedMarked[s]}

Min(S) == CHOOSE x \in S: \A y \in S: x <= y

GoodFloor ==
    IF PinningSlots = {}
    THEN primaryLsn
    ELSE Min({restartLsn[s]: s \in PinningSlots})

\* The mutant's floor pretends the mark-reseed loop already ran: it also
\* skips active UNMARKED slots whose lag exceeds policy, truncating WAL a
\* still-unmarked slot needs.
LaggingUnmarked(s) ==
    /\ slotActive[s]
    /\ ~reseedMarked[s]
    /\ primaryLsn - restartLsn[s] > MaxLag

BuggyFloor ==
    LET pins == {s \in PinningSlots: ~LaggingUnmarked(s)} IN
    IF pins = {}
    THEN primaryLsn
    ELSE Min({restartLsn[s]: s \in pins})

Init ==
    /\ primaryLsn = 0
    /\ slotActive = [s \in Slots |-> s \in ReplSlots]
    /\ reseedMarked = [s \in Slots |-> FALSE]
    /\ restartLsn = [s \in Slots |-> 0]
    /\ truncatedBelow = 0
    /\ backupInFlight = FALSE
    /\ backupLsn = 0
    /\ backupSucceededWithoutWal = FALSE

Append ==
    /\ primaryLsn < MaxLsn
    /\ primaryLsn' = primaryLsn + 1
    /\ UNCHANGED <<slotActive, reseedMarked, restartLsn, truncatedBelow,
                  backupInFlight, backupLsn, backupSucceededWithoutWal>>

\* Standby streams and confirms progress; its slot's restart LSN advances.
AdvanceSlot(s) ==
    /\ s \in ReplSlots
    /\ slotActive[s]
    /\ ~reseedMarked[s]
    /\ restartLsn[s] < primaryLsn
    /\ restartLsn' = [restartLsn EXCEPT ![s] = @ + 1]
    /\ UNCHANGED <<primaryLsn, slotActive, reseedMarked, truncatedBelow,
                  backupInFlight, backupLsn, backupSucceededWithoutWal>>

\* Per-slot reseed marking (the retention snapshot's marking loop). A crash
\* between marks is just the interleaving where only some marks happened.
MarkReseed(s) ==
    /\ slotActive[s]
    /\ ~reseedMarked[s]
    /\ primaryLsn - restartLsn[s] > MaxLag
    /\ reseedMarked' = [reseedMarked EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<primaryLsn, slotActive, restartLsn, truncatedBelow,
                  backupInFlight, backupLsn, backupSucceededWithoutWal>>

\* Operator drops a reseed-marked slot (slot GC).
DropSlot(s) ==
    /\ slotActive[s]
    /\ reseedMarked[s]
    /\ slotActive' = [slotActive EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<primaryLsn, reseedMarked, restartLsn, truncatedBelow,
                  backupInFlight, backupLsn, backupSucceededWithoutWal>>

\* WAL truncation up to the retention floor. The good floor honors every
\* active unmarked slot; the mutant floor assumes marking already happened.
Truncate ==
    /\ LET floor == IF BuggyTruncateIgnoresUnmarkedSlot
                    THEN BuggyFloor
                    ELSE GoodFloor
       IN /\ floor > truncatedBelow
          /\ truncatedBelow' = floor
    /\ UNCHANGED <<primaryLsn, slotActive, reseedMarked, restartLsn,
                  backupInFlight, backupLsn, backupSucceededWithoutWal>>

BeginBackup ==
    /\ ~backupInFlight
    /\ ~slotActive["backup"]
    /\ backupInFlight' = TRUE
    /\ backupLsn' = primaryLsn
    /\ slotActive' = [slotActive EXCEPT !["backup"] = TRUE]
    /\ reseedMarked' = [reseedMarked EXCEPT !["backup"] = FALSE]
    /\ restartLsn' = [restartLsn EXCEPT !["backup"] = primaryLsn]
    /\ UNCHANGED <<primaryLsn, truncatedBelow, backupSucceededWithoutWal>>

\* Successful backup end: validateBackupSlotRetention must still hold and
\* the WAL from the backup start must not have been truncated.
EndBackupOk ==
    /\ backupInFlight
    /\ slotActive["backup"]
    /\ ~reseedMarked["backup"]
    /\ truncatedBelow <= backupLsn
    /\ backupInFlight' = FALSE
    /\ slotActive' = [slotActive EXCEPT !["backup"] = FALSE]
    /\ UNCHANGED <<primaryLsn, reseedMarked, restartLsn, truncatedBelow,
                  backupLsn, backupSucceededWithoutWal>>

\* Fail-closed backup end: the slot was lost/marked or the WAL is gone; the
\* backup reports failure and releases the attempt.
EndBackupFailed ==
    /\ backupInFlight
    /\ \/ ~slotActive["backup"]
       \/ reseedMarked["backup"]
       \/ truncatedBelow > backupLsn
    /\ backupInFlight' = FALSE
    /\ slotActive' = [slotActive EXCEPT !["backup"] = FALSE]
    /\ reseedMarked' = [reseedMarked EXCEPT !["backup"] = FALSE]
    /\ UNCHANGED <<primaryLsn, restartLsn, truncatedBelow, backupLsn,
                  backupSucceededWithoutWal>>

\* Mutant: backup end skips the retention validation and reports success
\* even though the slot was lost or the WAL was truncated.
BuggyEndBackupIgnoresLostWal ==
    /\ BuggyBackupEndIgnoresLostWal
    /\ backupInFlight
    /\ \/ ~slotActive["backup"]
       \/ reseedMarked["backup"]
       \/ truncatedBelow > backupLsn
    /\ backupInFlight' = FALSE
    /\ slotActive' = [slotActive EXCEPT !["backup"] = FALSE]
    /\ reseedMarked' = [reseedMarked EXCEPT !["backup"] = FALSE]
    /\ backupSucceededWithoutWal' = TRUE
    /\ UNCHANGED <<primaryLsn, restartLsn, truncatedBelow, backupLsn>>

Next ==
    \/ Append
    \/ Truncate
    \/ BeginBackup
    \/ EndBackupOk
    \/ EndBackupFailed
    \/ BuggyEndBackupIgnoresLostWal
    \/ \E s \in Slots:
        \/ MarkReseed(s)
        \/ DropSlot(s)
    \/ \E s \in ReplSlots: AdvanceSlot(s)

Spec == Init /\ [][Next]_vars

(*
  Liveness: a replication slot never stays lagging-and-unmarked forever —
  either it catches up or the retention pass marks it reseed-required, so
  the retention floor cannot be pinned indefinitely by a dead standby.
*)
Fairness ==
    \A s \in ReplSlots: WF_vars(MarkReseed(s))

FairSpec == Spec /\ Fairness

NoPermanentUnmarkedLag ==
    \A s \in ReplSlots: []<>(~LaggingUnmarked(s))

TypeOK ==
    /\ BuggyTruncateIgnoresUnmarkedSlot \in BOOLEAN
    /\ BuggyBackupEndIgnoresLostWal \in BOOLEAN
    /\ primaryLsn \in 0..MaxLsn
    /\ slotActive \in [Slots -> BOOLEAN]
    /\ reseedMarked \in [Slots -> BOOLEAN]
    /\ restartLsn \in [Slots -> 0..MaxLsn]
    /\ truncatedBelow \in 0..MaxLsn
    /\ backupInFlight \in BOOLEAN
    /\ backupLsn \in 0..MaxLsn
    /\ backupSucceededWithoutWal \in BOOLEAN

\* Mark-before-truncate: WAL needed by an active slot that has NOT been
\* reseed-marked is never truncated. Violating this silently invalidates a
\* standby (or backup) that still believes its fork is retained.
TruncationCoversUnmarkedActiveSlots ==
    \A s \in Slots:
        slotActive[s] /\ ~reseedMarked[s] => truncatedBelow <= restartLsn[s]

\* A backup never reports success after losing its slot or its WAL.
BackupEndFailsClosed ==
    ~backupSucceededWithoutWal

Safety ==
    /\ TypeOK
    /\ TruncationCoversUnmarkedActiveSlots
    /\ BackupEndFailsClosed

=============================================================================
