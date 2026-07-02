\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

----------------------------- MODULE AntflyLsmReserveCleanup -----------------------------
(*
  Explicit reserve/fail/cleanup sibling for AntflyLsmLifecycle.tla.

  The original lifecycle model describes the intended reserve-before-publish
  discipline. This sibling makes reserve failure and temporary cleanup explicit
  so the property is not only an action precondition.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyPublishWithoutReserve, BuggyFailureLeaksTemp

VARIABLES
    reserved,
    live,
    activeLease,
    retired,
    tempAllocated,
    leaked

vars == <<reserved, live, activeLease, retired, tempAllocated, leaked>>

Init ==
    /\ reserved = FALSE
    /\ live = FALSE
    /\ activeLease = FALSE
    /\ retired = FALSE
    /\ tempAllocated = FALSE
    /\ leaked = FALSE

ReserveCleanupSlot ==
    /\ ~reserved
    /\ reserved' = TRUE
    /\ UNCHANGED <<live, activeLease, retired, tempAllocated, leaked>>

ReserveFails ==
    /\ ~reserved
    /\ ~live
    /\ UNCHANGED vars

PublishLive ==
    /\ ~live
    /\ (reserved \/ BuggyPublishWithoutReserve)
    /\ live' = TRUE
    /\ activeLease' = TRUE
    /\ UNCHANGED <<reserved, retired, tempAllocated, leaked>>

RetireWithActiveLease ==
    /\ live
    /\ activeLease
    /\ reserved
    /\ live' = FALSE
    /\ retired' = TRUE
    /\ UNCHANGED <<reserved, activeLease, tempAllocated, leaked>>

ReleaseRetiredLease ==
    /\ retired
    /\ activeLease
    /\ retired' = FALSE
    /\ activeLease' = FALSE
    /\ UNCHANGED <<reserved, live, tempAllocated, leaked>>

AllocateTemp ==
    /\ ~tempAllocated
    /\ tempAllocated' = TRUE
    /\ UNCHANGED <<reserved, live, activeLease, retired, leaked>>

FailAfterTemp ==
    /\ tempAllocated
    /\ IF BuggyFailureLeaksTemp
       THEN /\ leaked' = TRUE
            /\ tempAllocated' = TRUE
       ELSE /\ leaked' = FALSE
            /\ tempAllocated' = FALSE
    /\ UNCHANGED <<reserved, live, activeLease, retired>>

Next ==
    \/ ReserveCleanupSlot
    \/ ReserveFails
    \/ PublishLive
    \/ RetireWithActiveLease
    \/ ReleaseRetiredLease
    \/ AllocateTemp
    \/ FailAfterTemp

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ BuggyPublishWithoutReserve \in BOOLEAN
    /\ BuggyFailureLeaksTemp \in BOOLEAN
    /\ reserved \in BOOLEAN
    /\ live \in BOOLEAN
    /\ activeLease \in BOOLEAN
    /\ retired \in BOOLEAN
    /\ tempAllocated \in BOOLEAN
    /\ leaked \in BOOLEAN

PublishedResourceHasCleanupReserve ==
    (live \/ retired) => reserved

NoTempLeakAfterFailure ==
    ~leaked

Safety ==
    /\ TypeOK
    /\ PublishedResourceHasCleanupReserve
    /\ NoTempLeakAfterFailure

=============================================================================
