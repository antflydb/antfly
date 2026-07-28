\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.

------------------------ MODULE AntflyLeaseRetryBackoff ------------------------
(*
  Ordering model for a replacement enrichment worker encountering a durable
  lease owned by a disappeared process. Exact milliseconds and CPU rate remain
  runtime-harness obligations; this model requires a wait boundary between
  denied acquisition attempts.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyImmediateRetry, MaxAttempts

VARIABLES foreignLease, coolingDown, attempts, waits, acquired
vars == <<foreignLease, coolingDown, attempts, waits, acquired>>

Init ==
    /\ foreignLease = TRUE
    /\ coolingDown = FALSE
    /\ attempts = 0
    /\ waits = 0
    /\ acquired = FALSE

AcquireDenied ==
    /\ foreignLease
    /\ ~acquired
    /\ attempts < MaxAttempts
    /\ ~coolingDown \/ BuggyImmediateRetry
    /\ attempts' = attempts + 1
    /\ coolingDown' = ~BuggyImmediateRetry
    /\ UNCHANGED <<foreignLease, waits, acquired>>

WaitRetry ==
    /\ coolingDown
    /\ coolingDown' = FALSE
    /\ waits' = waits + 1
    /\ UNCHANGED <<foreignLease, attempts, acquired>>

LeaseExpires ==
    /\ foreignLease
    /\ foreignLease' = FALSE
    /\ UNCHANGED <<coolingDown, attempts, waits, acquired>>

AcquireSuccess ==
    /\ ~foreignLease
    /\ ~acquired
    /\ ~coolingDown
    /\ acquired' = TRUE
    /\ UNCHANGED <<foreignLease, coolingDown, attempts, waits>>

Next == AcquireDenied \/ WaitRetry \/ LeaseExpires \/ AcquireSuccess
Spec == Init /\ [][Next]_vars
FairSpec ==
    Spec
    /\ WF_vars(WaitRetry)
    /\ WF_vars(LeaseExpires)
    /\ WF_vars(AcquireSuccess)

TypeOK ==
    /\ BuggyImmediateRetry \in BOOLEAN
    /\ MaxAttempts \in 2..5
    /\ foreignLease \in BOOLEAN
    /\ coolingDown \in BOOLEAN
    /\ attempts \in 0..MaxAttempts
    /\ waits \in 0..MaxAttempts
    /\ acquired \in BOOLEAN

DeniedAttemptsHaveWaitBoundaries == attempts <= waits + 1
AcquiredOnlyAfterLeaseRelease == acquired => ~foreignLease
Safety == TypeOK /\ DeniedAttemptsHaveWaitBoundaries /\ AcquiredOnlyAfterLeaseRelease
EventuallyAcquires == <>acquired

=============================================================================
