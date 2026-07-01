\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

------------------------------- MODULE AntflyShardSplitSeq -------------------------------
(*
  Sequence-level shard split delta model.

  The legacy split model tracks delta keys as a set. This sibling makes repeated
  writes to the same child-range key distinguishable by sequence number, so the
  "second write after partial replay" data-loss class is representable.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyKeySetCutover

Writes == 1..2

VARIABLES
    parentLog,
    childLog,
    splitActive,
    fenceSeq,
    cutover

vars == <<parentLog, childLog, splitActive, fenceSeq, cutover>>

Init ==
    /\ parentLog = {}
    /\ childLog = {}
    /\ splitActive = TRUE
    /\ fenceSeq = 0
    /\ cutover = FALSE

CurrentParentSeq ==
    IF 2 \in parentLog THEN 2 ELSE IF 1 \in parentLog THEN 1 ELSE 0

AppendFirst ==
    /\ splitActive
    /\ 1 \notin parentLog
    /\ parentLog' = parentLog \cup {1}
    /\ UNCHANGED <<childLog, splitActive, fenceSeq, cutover>>

AppendSecondSameKey ==
    /\ splitActive
    /\ 1 \in parentLog
    /\ 2 \notin parentLog
    /\ parentLog' = parentLog \cup {2}
    /\ UNCHANGED <<childLog, splitActive, fenceSeq, cutover>>

Replay(w) ==
    /\ w \in parentLog
    /\ w \notin childLog
    /\ childLog' = childLog \cup {w}
    /\ UNCHANGED <<parentLog, splitActive, fenceSeq, cutover>>

SetFence ==
    /\ splitActive
    /\ fenceSeq < CurrentParentSeq
    /\ fenceSeq' = CurrentParentSeq
    /\ UNCHANGED <<parentLog, childLog, splitActive, cutover>>

CompleteCutover ==
    /\ splitActive
    /\ fenceSeq > 0
    /\ IF BuggyKeySetCutover
       THEN childLog /= {}
       ELSE /\ fenceSeq = CurrentParentSeq
            /\ \A w \in 1..fenceSeq: w \in parentLog => w \in childLog
    /\ splitActive' = FALSE
    /\ cutover' = TRUE
    /\ UNCHANGED <<parentLog, childLog, fenceSeq>>

Next ==
    \/ AppendFirst
    \/ AppendSecondSameKey
    \/ \E w \in Writes: Replay(w)
    \/ SetFence
    \/ CompleteCutover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyKeySetCutover \in BOOLEAN
    /\ parentLog \subseteq Writes
    /\ childLog \subseteq Writes
    /\ childLog \subseteq parentLog
    /\ splitActive \in BOOLEAN
    /\ fenceSeq \in 0..2
    /\ cutover \in BOOLEAN

CutoverPreservesAllFencedWrites ==
    cutover => \A w \in 1..fenceSeq: w \in parentLog => w \in childLog

SecondWriteCannotBeCollapsedByKey ==
    cutover /\ 2 \in parentLog => 2 \in childLog

Safety ==
    /\ TypeOK
    /\ CutoverPreservesAllFencedWrites
    /\ SecondWriteCannotBeCollapsedByKey

=============================================================================
