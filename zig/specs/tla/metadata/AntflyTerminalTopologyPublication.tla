\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

---------------- MODULE AntflyTerminalTopologyPublication ----------------
(*
  Durable catalog handoff after a shard split completes.

  An active transition contract fences source/destination range mutation. The
  reconciler therefore publishes a finalized transition record in one round,
  then seeds desired state from that terminal record and publishes the
  two-range topology in a later round. The terminal intent must survive that
  projected-state seed until its topology outcome has been folded.

  Implementation anchors:
    - metadata/reconciler.zig ActiveTransitionContractIndex
    - metadata/state.zig seedDesiredFromProjected
    - metadata/table_manager.zig syncProjectedSplitTransitions
*)

EXTENDS Naturals, TLC

CONSTANT BuggyDropFinalizedIntent

TransitionPhases == {"active", "finalized", "none"}

VARIABLES
    shardCutoverComplete,
    catalogRangeCount,
    transitionPhase,
    desiredIntent

vars ==
    <<shardCutoverComplete, catalogRangeCount, transitionPhase, desiredIntent>>

Init ==
    /\ shardCutoverComplete = FALSE
    /\ catalogRangeCount = 1
    /\ transitionPhase = "active"
    /\ desiredIntent = TRUE

CompleteShardCutover ==
    /\ transitionPhase = "active"
    /\ ~shardCutoverComplete
    /\ shardCutoverComplete' = TRUE
    /\ UNCHANGED <<catalogRangeCount, transitionPhase, desiredIntent>>

PublishFinalizedTransition ==
    /\ transitionPhase = "active"
    /\ shardCutoverComplete
    /\ transitionPhase' = "finalized"
    /\ UNCHANGED <<shardCutoverComplete, catalogRangeCount, desiredIntent>>

FoldFinalizedTopology ==
    /\ transitionPhase = "finalized"
    /\ desiredIntent
    /\ catalogRangeCount' = 2
    /\ desiredIntent' = FALSE
    /\ UNCHANGED <<shardCutoverComplete, transitionPhase>>

BuggySeedDropsFinalizedIntent ==
    /\ BuggyDropFinalizedIntent
    /\ transitionPhase = "finalized"
    /\ desiredIntent
    /\ catalogRangeCount = 1
    /\ desiredIntent' = FALSE
    /\ UNCHANGED <<shardCutoverComplete, catalogRangeCount, transitionPhase>>

CompactTerminalTransition ==
    /\ transitionPhase = "finalized"
    /\ ~desiredIntent
    /\ transitionPhase' = "none"
    /\ UNCHANGED <<shardCutoverComplete, catalogRangeCount, desiredIntent>>

TerminalIdle ==
    /\ transitionPhase = "none"
    /\ UNCHANGED vars

Next ==
    \/ CompleteShardCutover
    \/ PublishFinalizedTransition
    \/ FoldFinalizedTopology
    \/ BuggySeedDropsFinalizedIntent
    \/ CompactTerminalTransition
    \/ TerminalIdle

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyDropFinalizedIntent \in BOOLEAN
    /\ shardCutoverComplete \in BOOLEAN
    /\ catalogRangeCount \in 1..2
    /\ transitionPhase \in TransitionPhases
    /\ desiredIntent \in BOOLEAN

FinalizedOutcomeRetainedUntilTopology ==
    /\ transitionPhase = "finalized"
    /\ catalogRangeCount = 1
    => desiredIntent

TerminalCompactionRequiresPublishedTopology ==
    /\ shardCutoverComplete
    /\ transitionPhase = "none"
    => catalogRangeCount = 2

Safety ==
    /\ TypeOK
    /\ FinalizedOutcomeRetainedUntilTopology
    /\ TerminalCompactionRequiresPublishedTopology

=============================================================================
