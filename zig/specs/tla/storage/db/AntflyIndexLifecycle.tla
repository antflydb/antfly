\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

---------------------------- MODULE AntflyIndexLifecycle ----------------------------
(*
  Index lifecycle (stale -> building -> fresh) with shadow swap and durable
  status snapshots, outside the split context.

  Implementation correspondence:
  - Index freshness is a runtime classification (api/runtime_status.zig
    RuntimeStatusFreshness: fresh/stale/catching_up/opening/failed) derived
    from live index state plus a DURABLE status snapshot persisted separately
    from the index build (db.zig:9611-9796 IndexStatusSnapshot,
    saveIndexStatusSnapshots) — the flip and the persist are NOT atomic.
  - Shadow index builds run behind the serving index and swap in only when
    caught up (db/catalog/index_manager.zig); a failed build parks the index
    as status-only with a load failure (index_manager.zig:1627 loadFailure).
  - The safety boundary: an index must never be SERVED as fresh while its
    applied watermark is behind the write target — that is a silent
    missing-results class, worse than staleness honestly reported.

  The July-25 B5 smokeout showed that this boundary also needs an explicit
  scheduler contract: a second schema generation rebuilt in ~9 seconds on a
  quiet host but failed to finish in 240 seconds under moderate mixed load.
  Consequently this model no longer grants BuildStep fairness directly. It
  models a durable wakeup, bounded competing work, worker admission, and two
  successive schema generations. Fairness is attached to those concrete
  scheduling actions.

  Deliberate omissions: per-segment build structure, index bytes/merge policy,
  the exact freshness-source taxonomy (live-writer vs background refresh),
  and query routing between index kinds. One index, two schema generations,
  bounded writes and bounded competing work.

  Make targets: tla-check-index-lifecycle (positive);
  tla-check-index-lifecycle-negative-{swap-incomplete,
  recover-trusts-status}. Correspondence: hand-modeled from the cited
  anchors.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggySwapIncompleteShadow, BuggyRecoverTrustsStaleStatus,
          BuggyLoseSecondSchemaWakeup

MaxSeq == 2
MaxSchema == 2
MaxCompetingWork == 2
States == {"stale", "building", "fresh", "failed"}

VARIABLES
    state,          \* runtime lifecycle state
    applied,        \* durable index build watermark
    target,         \* durable write watermark the index must reach
    statusDurable,  \* last persisted status snapshot
    servedFreshBehind, \* ghost: a query was served fresh with applied < target
    requestedSchema, \* schema generation requested by metadata
    builtSchema,     \* generation whose shadow index was successfully swapped
    wakeQueued,      \* durable rebuild debt visible to the scheduler
    workerAdmitted,  \* scheduler granted the index worker a turn
    competingWork,   \* bounded unrelated work ahead of this rebuild
    secondWakeLost   \* ghost: generation two was requested without a wakeup

vars ==
    <<state, applied, target, statusDurable, servedFreshBehind,
      requestedSchema, builtSchema, wakeQueued, workerAdmitted,
      competingWork, secondWakeLost>>

Init ==
    /\ state = "stale"
    /\ applied = 0
    /\ target = 0
    /\ statusDurable = "stale"
    /\ servedFreshBehind = FALSE
    /\ requestedSchema = 1
    /\ builtSchema = 0
    /\ wakeQueued = TRUE
    /\ workerAdmitted = FALSE
    /\ competingWork = MaxCompetingWork
    /\ secondWakeLost = FALSE

\* A write advances the target; a fresh index has new debt and degrades to
\* building (derived indexes re-enter catch-up when notified).
Write ==
    /\ target < MaxSeq
    /\ target' = target + 1
    /\ state' = IF state = "fresh" THEN "building" ELSE state
    /\ wakeQueued' = (wakeQueued \/ (state = "fresh"))
    /\ workerAdmitted' =
        IF state = "fresh" THEN FALSE ELSE workerAdmitted
    /\ UNCHANGED <<applied, statusDurable, servedFreshBehind,
                  requestedSchema, builtSchema, competingWork,
                  secondWakeLost>>

\* Moderate contention is represented as finite work, not as an unconstrained
\* environment action that can starve the index forever by construction.
RunCompetingWork ==
    /\ competingWork > 0
    /\ competingWork' = competingWork - 1
    /\ UNCHANGED <<state, applied, target, statusDurable,
                  servedFreshBehind, requestedSchema, builtSchema,
                  wakeQueued, workerAdmitted, secondWakeLost>>

AdmitIndexWorker ==
    /\ wakeQueued
    /\ ~workerAdmitted
    /\ competingWork = 0
    /\ workerAdmitted' = TRUE
    /\ wakeQueued' = FALSE
    /\ UNCHANGED <<state, applied, target, statusDurable,
                  servedFreshBehind, requestedSchema, builtSchema,
                  competingWork, secondWakeLost>>

StartBuild ==
    /\ state \in {"stale", "failed"}
    /\ workerAdmitted
    /\ state' = "building"
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind,
                  requestedSchema, builtSchema, wakeQueued,
                  workerAdmitted, competingWork, secondWakeLost>>

BuildStep ==
    /\ state = "building"
    /\ workerAdmitted
    /\ applied < target
    /\ applied' = applied + 1
    /\ UNCHANGED <<state, target, statusDurable, servedFreshBehind,
                  requestedSchema, builtSchema, wakeQueued,
                  workerAdmitted, competingWork, secondWakeLost>>

\* Shadow swap / freshness flip. The good path requires the shadow to have
\* fully caught up; the mutant swaps a shadow that is still behind.
Swap ==
    /\ state = "building"
    /\ IF BuggySwapIncompleteShadow
       THEN TRUE
       ELSE applied = target
    /\ state' = "fresh"
    /\ builtSchema' = requestedSchema
    /\ workerAdmitted' = FALSE
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind,
                  requestedSchema, wakeQueued, competingWork, secondWakeLost>>

FailBuild ==
    /\ state = "building"
    /\ state' = "failed"
    /\ workerAdmitted' = FALSE
    /\ wakeQueued' = TRUE
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind,
                  requestedSchema, builtSchema, competingWork,
                  secondWakeLost>>

\* B5 always stranded the second rebuild. Requesting generation two resets
\* the per-generation watermark and must durably queue new work even if the
\* generation-one worker just completed.
RequestSecondSchema ==
    /\ requestedSchema = 1
    /\ builtSchema = 1
    /\ state = "fresh"
    /\ target = MaxSeq
    /\ requestedSchema' = 2
    /\ applied' = 0
    /\ state' = "stale"
    /\ workerAdmitted' = FALSE
    /\ competingWork' = MaxCompetingWork
    /\ IF BuggyLoseSecondSchemaWakeup
       THEN /\ wakeQueued' = FALSE
            /\ secondWakeLost' = TRUE
       ELSE /\ wakeQueued' = TRUE
            /\ secondWakeLost' = FALSE
    /\ UNCHANGED <<target, statusDurable, servedFreshBehind, builtSchema>>

\* Status snapshots persist asynchronously and can lag the live state.
PersistStatus ==
    /\ statusDurable # state
    /\ state # "failed"
    /\ statusDurable' = state
    /\ UNCHANGED <<state, applied, target, servedFreshBehind,
                  requestedSchema, builtSchema, wakeQueued,
                  workerAdmitted, competingWork, secondWakeLost>>

\* Crash/reopen. applied and target are durable; the runtime state is
\* recovered from the durable snapshot BUT must be re-validated against the
\* watermarks: a snapshot that says "fresh" while the index is behind must
\* recover as building, not fresh. The mutant trusts the snapshot blindly.
CrashReopen ==
    /\ state' =
        IF BuggyRecoverTrustsStaleStatus
        THEN IF statusDurable = "fresh" THEN "fresh" ELSE "stale"
        ELSE IF statusDurable = "fresh"
                /\ applied = target
                /\ builtSchema = requestedSchema
             THEN "fresh"
             ELSE "stale"
    /\ workerAdmitted' = FALSE
    /\ wakeQueued' =
        IF (statusDurable # "fresh"
            \/ builtSchema < requestedSchema
            \/ applied < target)
        THEN TRUE
        ELSE wakeQueued
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind,
                  requestedSchema, builtSchema, competingWork,
                  secondWakeLost>>

\* A consistent read consults the index; serving it as fresh while behind is
\* the silent missing-results failure.
Query ==
    /\ servedFreshBehind' =
        (servedFreshBehind \/ (state = "fresh" /\ applied < target))
    /\ UNCHANGED <<state, applied, target, statusDurable,
                  requestedSchema, builtSchema, wakeQueued,
                  workerAdmitted, competingWork, secondWakeLost>>

Next ==
    \/ Write
    \/ RunCompetingWork
    \/ AdmitIndexWorker
    \/ StartBuild
    \/ BuildStep
    \/ Swap
    \/ FailBuild
    \/ RequestSecondSchema
    \/ PersistStatus
    \/ CrashReopen
    \/ Query

Spec == Init /\ [][Next]_vars

(*
  Liveness: if the index does not fail forever, the build converges to
  fresh. Writes are structurally bounded (target <= MaxSeq), so debt is
  finite. STRONG fairness is required, not weak: a crash/reopen loop resets
  the runtime state and repeatedly interrupts enabledness, but applied is
  durable, so intermittently-enabled fair build steps still drain the debt
  across crashes.
*)
Fairness ==
    /\ WF_vars(Write)
    /\ WF_vars(RunCompetingWork)
    /\ WF_vars(AdmitIndexWorker)
    /\ WF_vars(RequestSecondSchema)
    /\ SF_vars(StartBuild)
    /\ SF_vars(BuildStep)
    /\ SF_vars(Swap)

FairSpec == Spec /\ Fairness

BuildEventuallyConverges ==
    (<>[](state # "failed")) => <>(state = "fresh" /\ applied = target)

SecondSchemaEventuallyConverges ==
    [](requestedSchema = 2 =>
        <>(builtSchema = 2 /\ state = "fresh" /\ applied = target))

TypeOK ==
    /\ BuggySwapIncompleteShadow \in BOOLEAN
    /\ BuggyRecoverTrustsStaleStatus \in BOOLEAN
    /\ BuggyLoseSecondSchemaWakeup \in BOOLEAN
    /\ state \in States
    /\ applied \in 0..MaxSeq
    /\ target \in 0..MaxSeq
    /\ applied <= target
    /\ statusDurable \in States
    /\ servedFreshBehind \in BOOLEAN
    /\ requestedSchema \in 1..MaxSchema
    /\ builtSchema \in 0..MaxSchema
    /\ builtSchema <= requestedSchema
    /\ wakeQueued \in BOOLEAN
    /\ workerAdmitted \in BOOLEAN
    /\ competingWork \in 0..MaxCompetingWork
    /\ secondWakeLost \in BOOLEAN

\* The contract with queries: fresh means fully caught up.
FreshImpliesCaughtUp ==
    state = "fresh" => applied = target

\* No query is ever served fresh results from a behind index.
NoFreshServeBehind ==
    ~servedFreshBehind

EveryRequestedGenerationHasDurableWork ==
    /\ ~secondWakeLost
    /\ (builtSchema < requestedSchema /\ ~workerAdmitted =>
            wakeQueued \/ competingWork > 0)

Safety ==
    /\ TypeOK
    /\ FreshImpliesCaughtUp
    /\ NoFreshServeBehind
    /\ EveryRequestedGenerationHasDurableWork

=============================================================================
