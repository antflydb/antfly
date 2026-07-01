\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

----------------------------- MODULE AntflyCdcCutover -----------------------------
(*
  CDC snapshot-to-stream cutover model.

  Scope: snapshot rows up to a high-water mark, stream rows after that mark,
  and checkpoint resume. Row decoding/transforms are data-plane tests; this
  model checks ordering, no duplicate cutover row, no skipped snapshot row, and
  no checkpoint advancing beyond delivered output.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyStreamBoundaryDuplicate, BuggyCheckpointAhead, BuggyResumeReplaysCheckpoint

Rows == 1..3
HighWater == 2

VARIABLES
    phase,
    delivered,
    checkpoint,
    snapshotCursor,
    streamCursor

vars == <<phase, delivered, checkpoint, snapshotCursor, streamCursor>>

Init ==
    /\ phase = "snapshot"
    /\ delivered = [r \in Rows |-> 0]
    /\ checkpoint = 0
    /\ snapshotCursor = 0
    /\ streamCursor = HighWater

DeliverSnapshotNext ==
    /\ phase = "snapshot"
    /\ snapshotCursor < HighWater
    /\ LET next == snapshotCursor + 1 IN
       /\ delivered[next] = 0
       /\ delivered' = [delivered EXCEPT ![next] = 1]
       /\ snapshotCursor' = next
       /\ checkpoint' = next
    /\ UNCHANGED <<phase, streamCursor>>

FinishSnapshot ==
    /\ phase = "snapshot"
    /\ \A r \in 1..HighWater: delivered[r] = 1
    /\ phase' = "stream"
    /\ streamCursor' = HighWater
    /\ UNCHANGED <<delivered, checkpoint, snapshotCursor>>

DeliverStreamNext ==
    /\ phase = "stream"
    /\ streamCursor < 3
    /\ LET next == streamCursor + 1 IN
       /\ delivered[next] = 0
       /\ delivered' = [delivered EXCEPT ![next] = 1]
       /\ streamCursor' = next
       /\ checkpoint' = next
    /\ UNCHANGED <<phase, snapshotCursor>>

BuggyDeliverBoundaryAgain ==
    /\ BuggyStreamBoundaryDuplicate
    /\ phase = "stream"
    /\ delivered[HighWater] = 1
    /\ delivered' = [delivered EXCEPT ![HighWater] = 2]
    /\ UNCHANGED <<phase, checkpoint, snapshotCursor, streamCursor>>

BuggyAdvanceCheckpointAhead ==
    /\ BuggyCheckpointAhead
    /\ phase = "snapshot"
    /\ checkpoint' = HighWater
    /\ UNCHANGED <<phase, delivered, snapshotCursor, streamCursor>>

Crash ==
    /\ phase \in {"snapshot", "stream"}
    /\ phase' = "crashed"
    /\ snapshotCursor' = 0
    /\ streamCursor' = 0
    /\ UNCHANGED <<delivered, checkpoint>>

Resume ==
    /\ phase = "crashed"
    /\ IF checkpoint < HighWater
       THEN /\ phase' = "snapshot"
            /\ snapshotCursor' = IF BuggyResumeReplaysCheckpoint /\ checkpoint > 0 THEN checkpoint - 1 ELSE checkpoint
            /\ streamCursor' = HighWater
       ELSE /\ phase' = "stream"
            /\ snapshotCursor' = HighWater
            /\ streamCursor' = IF BuggyResumeReplaysCheckpoint THEN HighWater - 1 ELSE checkpoint
    /\ UNCHANGED <<delivered, checkpoint>>

BuggyDeliverSnapshotAgainAfterResume ==
    /\ BuggyResumeReplaysCheckpoint
    /\ phase = "snapshot"
    /\ snapshotCursor < checkpoint
    /\ LET next == snapshotCursor + 1 IN
       /\ delivered[next] = 1
       /\ delivered' = [delivered EXCEPT ![next] = 2]
       /\ snapshotCursor' = next
    /\ UNCHANGED <<phase, checkpoint, streamCursor>>

BuggyDeliverStreamAgainAfterResume ==
    /\ BuggyResumeReplaysCheckpoint
    /\ phase = "stream"
    /\ streamCursor < checkpoint
    /\ LET next == streamCursor + 1 IN
       /\ delivered[next] = 1
       /\ delivered' = [delivered EXCEPT ![next] = 2]
       /\ streamCursor' = next
    /\ UNCHANGED <<phase, checkpoint, snapshotCursor>>

Next ==
    \/ DeliverSnapshotNext
    \/ FinishSnapshot
    \/ DeliverStreamNext
    \/ BuggyDeliverBoundaryAgain
    \/ BuggyAdvanceCheckpointAhead
    \/ Crash
    \/ Resume
    \/ BuggyDeliverSnapshotAgainAfterResume
    \/ BuggyDeliverStreamAgainAfterResume

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyStreamBoundaryDuplicate \in BOOLEAN
    /\ BuggyCheckpointAhead \in BOOLEAN
    /\ BuggyResumeReplaysCheckpoint \in BOOLEAN
    /\ phase \in {"snapshot", "stream", "crashed"}
    /\ delivered \in [Rows -> 0..2]
    /\ checkpoint \in 0..3
    /\ snapshotCursor \in 0..HighWater
    /\ streamCursor \in 0..3

NoDuplicateDelivery ==
    \A r \in Rows: delivered[r] <= 1

CheckpointOnlyCoversDelivered ==
    \A r \in 1..checkpoint: delivered[r] = 1

StreamStartsAfterSnapshotHighWater ==
    phase = "stream" => \A r \in 1..HighWater: delivered[r] = 1

ResumeCursorStartsAtCheckpoint ==
    phase = "crashed" \/
    /\ snapshotCursor >= IF checkpoint < HighWater THEN checkpoint ELSE HighWater
    /\ streamCursor >= IF checkpoint >= HighWater THEN checkpoint ELSE HighWater

Safety ==
    /\ TypeOK
    /\ NoDuplicateDelivery
    /\ CheckpointOnlyCoversDelivered
    /\ StreamStartsAfterSnapshotHighWater
    /\ ResumeCursorStartsAtCheckpoint

=============================================================================
