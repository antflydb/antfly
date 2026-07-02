\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

----------------------------- MODULE AntflyHAPartitionFence -----------------------------
(*
  HA partition and asynchronous fence-propagation model.

  AntflyHAFailoverSafety proves the postcondition once fencing is atomic. This
  sibling models the missing control-plane window: a partition can delay fence
  delivery to the old primary. Promotion is safe only after the old primary has
  observed the fence. If promotion happens before fence delivery, split-brain is
  reached by the normal old-primary write action, not by a direct write mutant.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyPromoteBeforeFenceDelivered

Writes == 1..2

VARIABLES
    partitioned,
    fenceRequested,
    fenceDeliveredOldPrimary,
    oldPrimaryWritable,
    promotedWritable,
    oldWrites,
    oldWritesAfterPromotion,
    promotedWrites,
    nextWrite

vars == <<partitioned, fenceRequested, fenceDeliveredOldPrimary,
          oldPrimaryWritable, promotedWritable, oldWrites, promotedWrites,
          oldWritesAfterPromotion, nextWrite>>

Init ==
    /\ partitioned = FALSE
    /\ fenceRequested = FALSE
    /\ fenceDeliveredOldPrimary = FALSE
    /\ oldPrimaryWritable = TRUE
    /\ promotedWritable = FALSE
    /\ oldWrites = {}
    /\ oldWritesAfterPromotion = {}
    /\ promotedWrites = {}
    /\ nextWrite = 1

PartitionOldPrimary ==
    /\ ~partitioned
    /\ partitioned' = TRUE
    /\ UNCHANGED <<fenceRequested, fenceDeliveredOldPrimary,
                  oldPrimaryWritable, promotedWritable, oldWrites,
                  oldWritesAfterPromotion, promotedWrites, nextWrite>>

RequestFence ==
    /\ ~fenceRequested
    /\ fenceRequested' = TRUE
    /\ UNCHANGED <<partitioned, fenceDeliveredOldPrimary,
                  oldPrimaryWritable, promotedWritable, oldWrites,
                  oldWritesAfterPromotion, promotedWrites, nextWrite>>

DeliverFenceToOldPrimary ==
    /\ fenceRequested
    /\ ~partitioned
    /\ ~fenceDeliveredOldPrimary
    /\ fenceDeliveredOldPrimary' = TRUE
    /\ oldPrimaryWritable' = FALSE
    /\ UNCHANGED <<partitioned, fenceRequested, promotedWritable, oldWrites,
                  oldWritesAfterPromotion, promotedWrites, nextWrite>>

HealPartition ==
    /\ partitioned
    /\ partitioned' = FALSE
    /\ UNCHANGED <<fenceRequested, fenceDeliveredOldPrimary,
                  oldPrimaryWritable, promotedWritable, oldWrites,
                  oldWritesAfterPromotion, promotedWrites, nextWrite>>

PromoteStandby ==
    /\ fenceRequested
    /\ ~promotedWritable
    /\ IF BuggyPromoteBeforeFenceDelivered
       THEN TRUE
       ELSE fenceDeliveredOldPrimary
    /\ promotedWritable' = TRUE
    /\ UNCHANGED <<partitioned, fenceRequested, fenceDeliveredOldPrimary,
                  oldPrimaryWritable, oldWrites, oldWritesAfterPromotion,
                  promotedWrites, nextWrite>>

OldPrimaryAppend ==
    /\ oldPrimaryWritable
    /\ nextWrite \in Writes
    /\ oldWrites' = oldWrites \cup {nextWrite}
    /\ oldWritesAfterPromotion' =
        IF promotedWritable THEN oldWritesAfterPromotion \cup {nextWrite}
        ELSE oldWritesAfterPromotion
    /\ nextWrite' = nextWrite + 1
    /\ UNCHANGED <<partitioned, fenceRequested, fenceDeliveredOldPrimary,
                  oldPrimaryWritable, promotedWritable, promotedWrites>>

PromotedAppend ==
    /\ promotedWritable
    /\ nextWrite \in Writes
    /\ promotedWrites' = promotedWrites \cup {nextWrite}
    /\ nextWrite' = nextWrite + 1
    /\ UNCHANGED <<partitioned, fenceRequested, fenceDeliveredOldPrimary,
                  oldPrimaryWritable, promotedWritable, oldWrites,
                  oldWritesAfterPromotion>>

Next ==
    \/ PartitionOldPrimary
    \/ RequestFence
    \/ DeliverFenceToOldPrimary
    \/ HealPartition
    \/ PromoteStandby
    \/ OldPrimaryAppend
    \/ PromotedAppend

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyPromoteBeforeFenceDelivered \in BOOLEAN
    /\ partitioned \in BOOLEAN
    /\ fenceRequested \in BOOLEAN
    /\ fenceDeliveredOldPrimary \in BOOLEAN
    /\ oldPrimaryWritable \in BOOLEAN
    /\ promotedWritable \in BOOLEAN
    /\ oldWrites \subseteq Writes
    /\ oldWritesAfterPromotion \subseteq Writes
    /\ promotedWrites \subseteq Writes
    /\ nextWrite \in 1..3

PromotionRequiresDeliveredFence ==
    promotedWritable => fenceDeliveredOldPrimary

NoSplitBrainWritable ==
    promotedWritable => ~oldPrimaryWritable

NoOldPrimaryWritesAfterPromotion ==
    oldWritesAfterPromotion = {}

Safety ==
    /\ TypeOK
    /\ PromotionRequiresDeliveredFence
    /\ NoSplitBrainWritable
    /\ NoOldPrimaryWritesAfterPromotion

=============================================================================
