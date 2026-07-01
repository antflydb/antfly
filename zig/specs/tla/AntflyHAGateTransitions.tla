\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

---------------------------- MODULE AntflyHAGateTransitions ----------------------------
(*
  Transition sibling for AntflyHAGates.tla.

  AntflyHAGates is intentionally a combinational table. This model covers the
  stateful failure the table cannot express: a cached allow decision surviving a
  role/fence transition.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyStaleAllowAfterTransition

Roles == {"primary", "standby", "former_primary"}

VARIABLES role, fenced, writeAllowed, backgroundRunning

vars == <<role, fenced, writeAllowed, backgroundRunning>>

Init ==
    /\ role = "primary"
    /\ fenced = FALSE
    /\ writeAllowed = FALSE
    /\ backgroundRunning = FALSE

RecomputeGates ==
    /\ writeAllowed' = (role = "primary" /\ ~fenced)
    /\ backgroundRunning' = (role = "primary" /\ ~fenced)
    /\ UNCHANGED <<role, fenced>>

BecomeStandby ==
    /\ role = "primary"
    /\ role' = "standby"
    /\ IF BuggyStaleAllowAfterTransition
       THEN UNCHANGED <<writeAllowed, backgroundRunning>>
       ELSE /\ writeAllowed' = FALSE
            /\ backgroundRunning' = FALSE
    /\ UNCHANGED fenced

BecomeFormerPrimary ==
    /\ role = "primary"
    /\ role' = "former_primary"
    /\ IF BuggyStaleAllowAfterTransition
       THEN UNCHANGED <<writeAllowed, backgroundRunning>>
       ELSE /\ writeAllowed' = FALSE
            /\ backgroundRunning' = FALSE
    /\ UNCHANGED fenced

FencePrimary ==
    /\ role = "primary"
    /\ ~fenced
    /\ fenced' = TRUE
    /\ IF BuggyStaleAllowAfterTransition
       THEN UNCHANGED <<writeAllowed, backgroundRunning>>
       ELSE /\ writeAllowed' = FALSE
            /\ backgroundRunning' = FALSE
    /\ UNCHANGED role

PromoteStandby ==
    /\ role = "standby"
    /\ fenced
    /\ role' = "primary"
    /\ fenced' = FALSE
    /\ writeAllowed' = FALSE
    /\ backgroundRunning' = FALSE

Next ==
    \/ RecomputeGates
    \/ BecomeStandby
    \/ BecomeFormerPrimary
    \/ FencePrimary
    \/ PromoteStandby

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyStaleAllowAfterTransition \in BOOLEAN
    /\ role \in Roles
    /\ fenced \in BOOLEAN
    /\ writeAllowed \in BOOLEAN
    /\ backgroundRunning \in BOOLEAN

AllowedWriteRequiresUnfencedPrimary ==
    writeAllowed => /\ role = "primary" /\ ~fenced

MutatingRuntimeRequiresUnfencedPrimary ==
    backgroundRunning => /\ role = "primary" /\ ~fenced

Safety ==
    /\ TypeOK
    /\ AllowedWriteRequiresUnfencedPrimary
    /\ MutatingRuntimeRequiresUnfencedPrimary

=============================================================================
