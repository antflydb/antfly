\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

--------------------------- MODULE AntflyTableLifecycle ---------------------------
(*
  Table create/drop lifecycle model: in-memory desired topology vs durable
  raft-applied records vs placement intents vs runtime groups.

  Implementation correspondence:
  - The table workflow mutates the IN-MEMORY desired TableManager
    (metadata/table_workflow.zig dropTable -> removeTableTopology; create ->
    upsertTable/upsertRange), then reconciliation diffs desired vs committed
    and proposes raft commands: upsert_table/upsert_range/
    upsert_replica_intent and remove_table/remove_range/remove_replica_intent
    (metadata/service.zig:1387-1393; metadata/reconciler.zig:126-127,297).
  - Desired-side upsertRange requires the table to exist
    (metadata/table_manager.zig:350 error.UnknownTable); the durable
    projection tolerates orphan ranges by skipping them
    (metadata/state.zig:390 skipped_orphan_ranges).
  - On restart, desired is rebuilt FROM COMMITTED
    (table_workflow.zig bootstrapDesiredFromCommitted). A drop whose removals
    were not yet raft-applied is therefore lost or partially lost after a
    crash. That is real behavior and is modeled as-is: the checked contracts
    are integrity (desired ranges always have their table; intents are only
    planned for desired ranges) and convergence, NOT drop atomicity.

  Deliberate omissions: table IDs/names (one table), schema payloads,
  replica counts (one intent per range), reconcile leases, group readiness
  reporting, and multi-node placement. Two ranges, bounded operator churn.

  Make targets: tla-check-table-lifecycle (positive);
  tla-check-table-lifecycle-negative-{range-without-table,
  intent-undesired-range}. Correspondence: hand-modeled from the cited
  anchors.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS BuggyDesiredRangeWithoutTable, BuggyPlanIntentUndesiredRange

Ranges == {"r1", "r2"}
MaxOps == 2

VARIABLES
    dTable,       \* desired: table present in in-memory TableManager
    dRanges,      \* desired: ranges present
    cTable,       \* committed: raft-applied table record
    cRanges,      \* committed: raft-applied range records
    cIntents,     \* committed: raft-applied placement intents (per range)
    groupsLive,   \* runtime groups created from intents
    opBudget,     \* bound on operator create/drop churn
    intentPlannedUndesired \* ghost: intent planned for a range not in desired

vars == <<dTable, dRanges, cTable, cRanges, cIntents, groupsLive, opBudget,
          intentPlannedUndesired>>

Init ==
    /\ dTable = FALSE
    /\ dRanges = {}
    /\ cTable = FALSE
    /\ cRanges = {}
    /\ cIntents = {}
    /\ groupsLive = {}
    /\ opBudget = MaxOps
    /\ intentPlannedUndesired = FALSE

\* Operator creates the table: desired table plus its ranges together (the
\* workflow upserts the table record, then each range, all desired-side
\* before the reconcile pass).
OpCreate ==
    /\ opBudget > 0
    /\ ~dTable
    /\ dTable' = TRUE
    /\ dRanges' = Ranges
    /\ opBudget' = opBudget - 1
    /\ UNCHANGED <<cTable, cRanges, cIntents, groupsLive,
                  intentPlannedUndesired>>

\* Mutant surface: a desired range admitted without its table (the
\* table_manager.zig:350 UnknownTable guard removed).
BuggyOpAddRangeWithoutTable(r) ==
    /\ BuggyDesiredRangeWithoutTable
    /\ ~dTable
    /\ r \notin dRanges
    /\ dRanges' = dRanges \cup {r}
    /\ UNCHANGED <<dTable, cTable, cRanges, cIntents, groupsLive, opBudget,
                  intentPlannedUndesired>>

\* Operator drops the table: clears the in-memory desired topology only.
\* Durable removal happens through subsequent reconcile applies.
OpDrop ==
    /\ opBudget > 0
    /\ dTable
    /\ dTable' = FALSE
    /\ dRanges' = {}
    /\ opBudget' = opBudget - 1
    /\ UNCHANGED <<cTable, cRanges, cIntents, groupsLive,
                  intentPlannedUndesired>>

\* Reconcile applies, one raft command per action.
ApplyUpsertTable ==
    /\ dTable /\ ~cTable
    /\ cTable' = TRUE
    /\ UNCHANGED <<dTable, dRanges, cRanges, cIntents, groupsLive, opBudget,
                  intentPlannedUndesired>>

ApplyUpsertRange(r) ==
    /\ r \in dRanges
    /\ r \notin cRanges
    /\ cRanges' = cRanges \cup {r}
    /\ UNCHANGED <<dTable, dRanges, cTable, cIntents, groupsLive, opBudget,
                  intentPlannedUndesired>>

ApplyRemoveRange(r) ==
    /\ r \notin dRanges
    /\ r \in cRanges
    /\ cRanges' = cRanges \ {r}
    /\ UNCHANGED <<dTable, dRanges, cTable, cIntents, groupsLive, opBudget,
                  intentPlannedUndesired>>

ApplyRemoveTable ==
    /\ ~dTable /\ cTable
    /\ cTable' = FALSE
    /\ UNCHANGED <<dTable, dRanges, cRanges, cIntents, groupsLive, opBudget,
                  intentPlannedUndesired>>

\* The placement planner iterates DESIRED ranges only
\* (placement_planner.zig:103-105). The mutant plans from committed ranges
\* that the desired topology no longer contains (e.g. after a drop).
PlanIntent(r) ==
    /\ r \notin cIntents
    /\ IF BuggyPlanIntentUndesiredRange
       THEN r \in cRanges /\ r \notin dRanges
       ELSE dTable /\ r \in dRanges
    /\ cIntents' = cIntents \cup {r}
    /\ intentPlannedUndesired' =
        (intentPlannedUndesired \/ r \notin dRanges)
    /\ UNCHANGED <<dTable, dRanges, cTable, cRanges, groupsLive, opBudget>>

RemoveIntent(r) ==
    /\ r \notin dRanges
    /\ r \in cIntents
    /\ cIntents' = cIntents \ {r}
    /\ UNCHANGED <<dTable, dRanges, cTable, cRanges, groupsLive, opBudget,
                  intentPlannedUndesired>>

GroupUp(r) ==
    /\ r \in cIntents
    /\ r \notin groupsLive
    /\ groupsLive' = groupsLive \cup {r}
    /\ UNCHANGED <<dTable, dRanges, cTable, cRanges, cIntents, opBudget,
                  intentPlannedUndesired>>

GroupDown(r) ==
    /\ r \notin cIntents
    /\ r \in groupsLive
    /\ groupsLive' = groupsLive \ {r}
    /\ UNCHANGED <<dTable, dRanges, cTable, cRanges, cIntents, opBudget,
                  intentPlannedUndesired>>

\* Metadata process restart: desired is rebuilt from committed; orphan
\* committed ranges (no committed table) are skipped by the projection.
CrashRestart ==
    /\ dTable' = cTable
    /\ dRanges' = IF cTable THEN cRanges ELSE {}
    /\ UNCHANGED <<cTable, cRanges, cIntents, groupsLive, opBudget,
                  intentPlannedUndesired>>

Next ==
    \/ OpCreate
    \/ OpDrop
    \/ ApplyUpsertTable
    \/ ApplyRemoveTable
    \/ CrashRestart
    \/ \E r \in Ranges:
        \/ BuggyOpAddRangeWithoutTable(r)
        \/ ApplyUpsertRange(r)
        \/ ApplyRemoveRange(r)
        \/ PlanIntent(r)
        \/ RemoveIntent(r)
        \/ GroupUp(r)
        \/ GroupDown(r)

Spec == Init /\ [][Next]_vars

(*
  Liveness: once operator churn stops (opBudget bounds it structurally),
  desired, committed, intents, and runtime groups converge. Crash/restart may
  fire forever but only copies committed into desired, which drives toward
  (not away from) agreement.
*)
Fairness ==
    /\ WF_vars(ApplyUpsertTable)
    /\ WF_vars(ApplyRemoveTable)
    /\ \A r \in Ranges:
        /\ WF_vars(ApplyUpsertRange(r))
        /\ WF_vars(ApplyRemoveRange(r))
        /\ WF_vars(PlanIntent(r))
        /\ WF_vars(RemoveIntent(r))
        /\ WF_vars(GroupUp(r))
        /\ WF_vars(GroupDown(r))

FairSpec == Spec /\ Fairness

Converged ==
    /\ cTable = dTable
    /\ cRanges = dRanges
    /\ cIntents = dRanges
    /\ groupsLive = cIntents

TopologyEventuallyConverges == <>[]Converged

TypeOK ==
    /\ BuggyDesiredRangeWithoutTable \in BOOLEAN
    /\ BuggyPlanIntentUndesiredRange \in BOOLEAN
    /\ dTable \in BOOLEAN
    /\ dRanges \subseteq Ranges
    /\ cTable \in BOOLEAN
    /\ cRanges \subseteq Ranges
    /\ cIntents \subseteq Ranges
    /\ groupsLive \subseteq Ranges
    /\ opBudget \in 0..MaxOps
    /\ intentPlannedUndesired \in BOOLEAN

\* The desired topology never contains a range without its table
\* (table_manager.zig:350 guard).
DesiredRangesHaveTable ==
    dRanges # {} => dTable

\* The planner never creates a placement intent for a range the desired
\* topology does not contain (a dropped table's groups must not be revived).
NoIntentPlannedForUndesiredRange ==
    ~intentPlannedUndesired

Safety ==
    /\ TypeOK
    /\ DesiredRangesHaveTable
    /\ NoIntentPlannedForUndesiredRange

=============================================================================
