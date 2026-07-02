\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

-------------------------- MODULE AntflyBatcherCoalescing --------------------------
(*
  Focused model for storage/db/batcher-style coalescing.

  TLA+ is useful here because the bug is an ordering/visibility bug, not a
  value-computation bug: a batcher may merge multiple operations for the same
  key, but the durable visible result must be the last operation in per-key
  enqueue order and no partially coalesced result may become visible.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyDeleteWriteInversion, BuggyWriteDeleteInversion, BuggyPartialVisibility

Ops == 1..2
Values == {"none", "deleted", "written", "partial"}
OpKinds == {"delete", "write", "none"}

VARIABLES
    enqueuedThrough,
    opLog,
    flushing,
    durableValue,
    flushedThrough

vars == <<enqueuedThrough, opLog, flushing, durableValue, flushedThrough>>

Init ==
    /\ enqueuedThrough = 0
    /\ opLog = [i \in Ops |-> "none"]
    /\ flushing = FALSE
    /\ durableValue = "none"
    /\ flushedThrough = 0

Enqueue(op) ==
    /\ op \in {"delete", "write"}
    /\ enqueuedThrough < 2
    /\ enqueuedThrough' = enqueuedThrough + 1
    /\ opLog' = [opLog EXCEPT ![enqueuedThrough'] = op]
    /\ UNCHANGED <<flushing, durableValue, flushedThrough>>

DurableForOp(op) ==
    IF op = "delete" THEN "deleted" ELSE "written"

BuggyDurableValue ==
    IF /\ enqueuedThrough = 2
       /\ BuggyDeleteWriteInversion
       /\ opLog[1] = "delete"
       /\ opLog[2] = "write"
    THEN "deleted"
    ELSE IF /\ enqueuedThrough = 2
            /\ BuggyWriteDeleteInversion
            /\ opLog[1] = "write"
            /\ opLog[2] = "delete"
    THEN "written"
    ELSE DurableForOp(opLog[enqueuedThrough])

BeginFlush ==
    /\ enqueuedThrough > flushedThrough
    /\ ~flushing
    /\ flushing' = TRUE
    /\ IF BuggyPartialVisibility
       THEN durableValue' = "partial"
       ELSE durableValue' = durableValue
    /\ UNCHANGED <<enqueuedThrough, opLog, flushedThrough>>

FinishFlush ==
    /\ flushing
    /\ flushedThrough' = enqueuedThrough
    /\ flushing' = FALSE
    /\ durableValue' = BuggyDurableValue
    /\ UNCHANGED <<enqueuedThrough, opLog>>

Next ==
    \/ \E op \in {"delete", "write"}: Enqueue(op)
    \/ BeginFlush
    \/ FinishFlush

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyDeleteWriteInversion \in BOOLEAN
    /\ BuggyWriteDeleteInversion \in BOOLEAN
    /\ BuggyPartialVisibility \in BOOLEAN
    /\ enqueuedThrough \in 0..2
    /\ opLog \in [Ops -> OpKinds]
    /\ \A i \in Ops:
        IF i <= enqueuedThrough THEN opLog[i] \in {"delete", "write"} ELSE opLog[i] = "none"
    /\ flushedThrough \in 0..2
    /\ flushedThrough <= enqueuedThrough
    /\ flushing \in BOOLEAN
    /\ durableValue \in Values

LastOperationWinsPerKey ==
    flushedThrough > 0 => durableValue = DurableForOp(opLog[flushedThrough])

DeleteVisibleBeforeWriteOnly ==
    durableValue = "deleted" => /\ flushedThrough > 0 /\ opLog[flushedThrough] = "delete"

NoPartialVisibility ==
    durableValue # "partial"

Safety ==
    /\ TypeOK
    /\ LastOperationWinsPerKey
    /\ DeleteVisibleBeforeWriteOnly
    /\ NoPartialVisibility

=============================================================================
