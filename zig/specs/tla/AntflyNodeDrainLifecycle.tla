\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

-------------------------- MODULE AntflyNodeDrainLifecycle --------------------------
(*
  Node drain / scale-down lifecycle model.

  Implementation correspondence:
  - NodeRecord.lifecycle ("active"/"draining") and StoreRecord.drain_requested
    are durable metadata raft state (metadata/table_manager.zig:86-118).
  - request_node_shutdown / cancel_node_shutdown set node lifecycle and ALL
    hosted stores' drain_requested in one raft transaction
    (metadata/storage/raft_apply_store.zig:1608-1616).
  - finalize_node_shutdown rejects an active node or a hosted store without
    drain_requested (raft_apply_store.zig:1626,1648: ActiveNodeFinalizeRejected).
  - Node re-registration must NOT clear an existing draining lifecycle
    (metadata/http_server.zig:1403-1432, SCALING.md: shutdown intent survives
    restart and later self-registration). This was a real historical hazard.
  - safe_to_terminate requires zero placement intents, zero hosted group
    statuses, zero runtime groups, zero local voters/leaders
    (metadata/http_server.zig:1685-1691); a hosted group with no other voter
    reports "blocked", never "complete" (lines 1692-1699).
  - Draining nodes/stores are excluded from new placement
    (metadata/state.zig:188,454; store_observer.zig:231).

  Deliberate omissions: multiple nodes, exact reconciler batching, group
  status reporting lag, store health/no_space exclusion reasons, and the
  metadata raft transport are abstracted. One node, two stores, two hosted
  groups.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyFinalizeActiveNode, BuggyRegistrationClearsDrain,
          BuggySafeIgnoresPlacementDebt

Stores == {"store1", "store2"}
Groups == {"g1", "g2"}

VARIABLES
    nodeLifecycle,      \* "active" | "draining" | "removed"
    storeDrain,         \* [Stores -> BOOLEAN]
    placementIntent,    \* [Groups -> BOOLEAN] intent for a replica on this node
    groupHosted,        \* [Groups -> BOOLEAN] runtime group present on this node
    otherVoters,        \* [Groups -> 0..1] other voters available elsewhere
    safeReported,       \* last computed safe_to_terminate
    terminated,         \* operator terminated the node
    finalizedWhileActive \* ghost: finalize accepted while node still active

vars == <<nodeLifecycle, storeDrain, placementIntent, groupHosted,
          otherVoters, safeReported, terminated, finalizedWhileActive>>

NoTerminationDebt ==
    /\ \A g \in Groups: ~placementIntent[g]
    /\ \A g \in Groups: ~groupHosted[g]

Init ==
    /\ nodeLifecycle = "active"
    /\ storeDrain = [s \in Stores |-> FALSE]
    /\ placementIntent \in [Groups -> BOOLEAN]
    /\ groupHosted = placementIntent
    /\ otherVoters \in [Groups -> 0..1]
    /\ safeReported = FALSE
    /\ terminated = FALSE
    /\ finalizedWhileActive = FALSE

\* PUT /internal/v1/nodes/{id}/shutdown: one raft transaction flips the node
\* lifecycle and every hosted store's drain flag together.
RequestShutdown ==
    /\ nodeLifecycle = "active"
    /\ nodeLifecycle' = "draining"
    /\ storeDrain' = [s \in Stores |-> TRUE]
    /\ safeReported' = FALSE
    /\ UNCHANGED <<placementIntent, groupHosted, otherVoters, terminated,
                  finalizedWhileActive>>

\* DELETE shutdown: atomic cancel. Replicas already moved stay moved.
CancelShutdown ==
    /\ nodeLifecycle = "draining"
    /\ nodeLifecycle' = "active"
    /\ storeDrain' = [s \in Stores |-> FALSE]
    /\ safeReported' = FALSE
    /\ UNCHANGED <<placementIntent, groupHosted, otherVoters, terminated,
                  finalizedWhileActive>>

\* Node self-registration after restart. The good path preserves a draining
\* lifecycle; the mutant regresses to the historical bug where registration
\* reset lifecycle to active while stores kept their drain flags.
ReRegisterNode ==
    /\ nodeLifecycle = "draining"
    /\ IF BuggyRegistrationClearsDrain
       THEN /\ nodeLifecycle' = "active"
            /\ UNCHANGED storeDrain
       ELSE UNCHANGED <<nodeLifecycle, storeDrain>>
    /\ UNCHANGED <<placementIntent, groupHosted, otherVoters, safeReported,
                  terminated, finalizedWhileActive>>

\* Reconciler evacuates a replica: possible only while another voter exists.
EvacuateReplica(g) ==
    /\ nodeLifecycle = "draining"
    /\ placementIntent[g]
    /\ otherVoters[g] >= 1
    /\ placementIntent' = [placementIntent EXCEPT ![g] = FALSE]
    /\ UNCHANGED <<nodeLifecycle, storeDrain, groupHosted, otherVoters,
                  safeReported, terminated, finalizedWhileActive>>

\* Runtime tears down a hosted group once its intent is gone.
TeardownGroup(g) ==
    /\ ~placementIntent[g]
    /\ groupHosted[g]
    /\ groupHosted' = [groupHosted EXCEPT ![g] = FALSE]
    /\ UNCHANGED <<nodeLifecycle, storeDrain, placementIntent, otherVoters,
                  safeReported, terminated, finalizedWhileActive>>

\* Operator repairs a single-voter group by adding a voter elsewhere.
AddOtherVoter(g) ==
    /\ otherVoters[g] = 0
    /\ otherVoters' = [otherVoters EXCEPT ![g] = 1]
    /\ UNCHANGED <<nodeLifecycle, storeDrain, placementIntent, groupHosted,
                  safeReported, terminated, finalizedWhileActive>>

\* GET shutdown status computes safe_to_terminate from current debt.
ComputeStatus ==
    /\ nodeLifecycle = "draining"
    /\ safeReported' =
        IF BuggySafeIgnoresPlacementDebt
        THEN TRUE
        ELSE NoTerminationDebt
    /\ UNCHANGED <<nodeLifecycle, storeDrain, placementIntent, groupHosted,
                  otherVoters, terminated, finalizedWhileActive>>

\* Operator terminates the pod once the status endpoint reported safe.
Terminate ==
    /\ ~terminated
    /\ safeReported
    /\ terminated' = TRUE
    /\ UNCHANGED <<nodeLifecycle, storeDrain, placementIntent, groupHosted,
                  otherVoters, safeReported, finalizedWhileActive>>

\* DELETE /internal/v1/nodes/{id}: rejects an active node and any hosted
\* store without drain_requested (ActiveNodeFinalizeRejected).
FinalizeShutdown ==
    /\ nodeLifecycle # "removed"
    /\ IF BuggyFinalizeActiveNode
       THEN TRUE
       ELSE /\ nodeLifecycle = "draining"
            /\ \A s \in Stores: storeDrain[s]
    /\ nodeLifecycle' = "removed"
    /\ finalizedWhileActive' =
        (finalizedWhileActive \/ nodeLifecycle = "active")
    /\ UNCHANGED <<storeDrain, placementIntent, groupHosted, otherVoters,
                  safeReported, terminated>>

Next ==
    \/ RequestShutdown
    \/ CancelShutdown
    \/ ReRegisterNode
    \/ ComputeStatus
    \/ Terminate
    \/ FinalizeShutdown
    \/ \E g \in Groups:
        \/ EvacuateReplica(g)
        \/ TeardownGroup(g)
        \/ AddOtherVoter(g)

Spec == Init /\ [][Next]_vars

(*
  Liveness: a drain that is not cancelled eventually reaches a safe report.
  Conditional on operator repair of single-voter groups (fair AddOtherVoter)
  and on the drain not being cancelled/re-activated forever; cancellation and
  the registration mutant are excluded by the antecedent.
*)
Fairness ==
    /\ \A g \in Groups:
        /\ WF_vars(EvacuateReplica(g))
        /\ WF_vars(TeardownGroup(g))
        /\ WF_vars(AddOtherVoter(g))
    /\ WF_vars(ComputeStatus)

FairSpec == Spec /\ Fairness

DrainEventuallyReportsSafe ==
    <>[](nodeLifecycle = "draining") => <>safeReported

TypeOK ==
    /\ BuggyFinalizeActiveNode \in BOOLEAN
    /\ BuggyRegistrationClearsDrain \in BOOLEAN
    /\ BuggySafeIgnoresPlacementDebt \in BOOLEAN
    /\ nodeLifecycle \in {"active", "draining", "removed"}
    /\ storeDrain \in [Stores -> BOOLEAN]
    /\ placementIntent \in [Groups -> BOOLEAN]
    /\ groupHosted \in [Groups -> BOOLEAN]
    /\ otherVoters \in [Groups -> 0..1]
    /\ safeReported \in BOOLEAN
    /\ terminated \in BOOLEAN
    /\ finalizedWhileActive \in BOOLEAN

\* request/cancel flip node lifecycle and store drain flags in one raft
\* transaction, so they can never disagree.
DrainStateConsistent ==
    /\ nodeLifecycle = "draining" => \A s \in Stores: storeDrain[s]
    /\ nodeLifecycle = "active" => \A s \in Stores: ~storeDrain[s]

\* Finalize must never be accepted for an active node.
FinalizeRequiresDrained ==
    ~finalizedWhileActive

\* The status endpoint may report safe only with zero termination debt:
\* terminating a node that still hosts a group loses replicas/quorum.
SafeReportMatchesDebt ==
    safeReported => NoTerminationDebt

\* The operator stake: an actually-terminated node held nothing.
TerminationSafe ==
    terminated => NoTerminationDebt

Safety ==
    /\ TypeOK
    /\ DrainStateConsistent
    /\ FinalizeRequiresDrained
    /\ SafeReportMatchesDebt
    /\ TerminationSafe

=============================================================================
