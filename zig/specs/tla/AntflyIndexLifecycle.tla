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

  Deliberate omissions: per-segment build structure, index bytes/merge
  policy, multiple simultaneous shadow builds, the exact freshness-source
  taxonomy (live-writer vs background refresh), and query routing between
  index kinds. One index, bounded writes.

  Make targets: tla-check-index-lifecycle (positive);
  tla-check-index-lifecycle-negative-{swap-incomplete,
  recover-trusts-status}. Correspondence: hand-modeled from the cited
  anchors.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggySwapIncompleteShadow, BuggyRecoverTrustsStaleStatus

MaxSeq == 2
States == {"stale", "building", "fresh", "failed"}

VARIABLES
    state,          \* runtime lifecycle state
    applied,        \* durable index build watermark
    target,         \* durable write watermark the index must reach
    statusDurable,  \* last persisted status snapshot
    servedFreshBehind \* ghost: a query was served as fresh with applied < target

vars == <<state, applied, target, statusDurable, servedFreshBehind>>

Init ==
    /\ state = "stale"
    /\ applied = 0
    /\ target = 0
    /\ statusDurable = "stale"
    /\ servedFreshBehind = FALSE

\* A write advances the target; a fresh index has new debt and degrades to
\* building (derived indexes re-enter catch-up when notified).
Write ==
    /\ target < MaxSeq
    /\ target' = target + 1
    /\ state' = IF state = "fresh" THEN "building" ELSE state
    /\ UNCHANGED <<applied, statusDurable, servedFreshBehind>>

StartBuild ==
    /\ state \in {"stale", "failed"}
    /\ state' = "building"
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind>>

BuildStep ==
    /\ state = "building"
    /\ applied < target
    /\ applied' = applied + 1
    /\ UNCHANGED <<state, target, statusDurable, servedFreshBehind>>

\* Shadow swap / freshness flip. The good path requires the shadow to have
\* fully caught up; the mutant swaps a shadow that is still behind.
Swap ==
    /\ state = "building"
    /\ IF BuggySwapIncompleteShadow
       THEN TRUE
       ELSE applied = target
    /\ state' = "fresh"
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind>>

FailBuild ==
    /\ state = "building"
    /\ state' = "failed"
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind>>

\* Status snapshots persist asynchronously and can lag the live state.
PersistStatus ==
    /\ statusDurable # state
    /\ state # "failed"
    /\ statusDurable' = state
    /\ UNCHANGED <<state, applied, target, servedFreshBehind>>

\* Crash/reopen. applied and target are durable; the runtime state is
\* recovered from the durable snapshot BUT must be re-validated against the
\* watermarks: a snapshot that says "fresh" while the index is behind must
\* recover as building, not fresh. The mutant trusts the snapshot blindly.
CrashReopen ==
    /\ state' =
        IF BuggyRecoverTrustsStaleStatus
        THEN IF statusDurable = "fresh" THEN "fresh" ELSE "stale"
        ELSE IF statusDurable = "fresh" /\ applied = target
             THEN "fresh"
             ELSE "stale"
    /\ UNCHANGED <<applied, target, statusDurable, servedFreshBehind>>

\* A consistent read consults the index; serving it as fresh while behind is
\* the silent missing-results failure.
Query ==
    /\ servedFreshBehind' =
        (servedFreshBehind \/ (state = "fresh" /\ applied < target))
    /\ UNCHANGED <<state, applied, target, statusDurable>>

Next ==
    \/ Write
    \/ StartBuild
    \/ BuildStep
    \/ Swap
    \/ FailBuild
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
    /\ SF_vars(StartBuild)
    /\ SF_vars(BuildStep)
    /\ SF_vars(Swap)

FairSpec == Spec /\ Fairness

BuildEventuallyConverges ==
    (<>[](state # "failed")) => <>(state = "fresh" /\ applied = target)

TypeOK ==
    /\ BuggySwapIncompleteShadow \in BOOLEAN
    /\ BuggyRecoverTrustsStaleStatus \in BOOLEAN
    /\ state \in States
    /\ applied \in 0..MaxSeq
    /\ target \in 0..MaxSeq
    /\ applied <= target
    /\ statusDurable \in States
    /\ servedFreshBehind \in BOOLEAN

\* The contract with queries: fresh means fully caught up.
FreshImpliesCaughtUp ==
    state = "fresh" => applied = target

\* No query is ever served fresh results from a behind index.
NoFreshServeBehind ==
    ~servedFreshBehind

Safety ==
    /\ TypeOK
    /\ FreshImpliesCaughtUp
    /\ NoFreshServeBehind

=============================================================================
