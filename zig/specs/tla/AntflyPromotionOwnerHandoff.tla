\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.

------------------------ MODULE AntflyPromotionOwnerHandoff ------------------------
(*
  Entity-promotion single-owner handoff across split/merge.

  Exactly one side may run entity promotion for a range at a time: the
  source shard's local raft leader (RESOLUTION.md; promotion ownership is a
  runtime predicate, storage/db/promotion_runtime.zig PromotionOwner /
  isLocalOwner gate at the catch-up loop). During a split the parent detaches
  its promotion owner before the child DB attaches one
  (api/table_writes.zig applyRuntimeHooksToDb / detachRuntimeHooks;
  db.zig finalizeSplitLocked); a merge hands donor -> receiver the same way.
  This model uses "old" (parent/donor) and "new" (child/receiver).

  The stake: two concurrently attached owners can both convert resolution
  artifacts into canonical entity upserts. Alias unions are idempotent, but
  diverging canonical-field decisions from two writers corrupt entity state.

  Deliberate omissions: entity payloads and idempotent-merge semantics
  (data-plane), raft leadership election within one side (the leadership
  predicate is collapsed into range ownership + attachment), the enrichment
  pipeline feeding promotion, and multi-range shards. Ownership attachment
  is a runtime pointer, NOT durable — a crashed side always recovers
  detached and must re-attach behind the range-ownership check, which the
  model represents with per-side crash actions.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyAttachBeforeDetach, BuggyPromoteWithoutOwnership

Sides == {"old", "new"}

VARIABLES
    rangeOwner,        \* which side's group owns the range ("old"/"new")
    attached,          \* [Sides -> BOOLEAN] promotion owner installed
    handoffStarted,    \* the split/merge transition began
    promotedBy,        \* [Sides -> BOOLEAN] side has performed a promotion
    unownedPromotion   \* ghost: a promotion ran without range ownership + attachment

vars == <<rangeOwner, attached, handoffStarted, promotedBy, unownedPromotion>>

Init ==
    /\ rangeOwner = "old"
    /\ attached = [s \in Sides |-> s = "old"]
    /\ handoffStarted = FALSE
    /\ promotedBy = [s \in Sides |-> FALSE]
    /\ unownedPromotion = FALSE

\* The split/merge transition begins: the old side detaches its promotion
\* owner (setPromotionOwner(null) via detachRuntimeHooks) BEFORE the range
\* moves.
StartHandoffDetachOld ==
    /\ ~handoffStarted
    /\ handoffStarted' = TRUE
    /\ attached' = [attached EXCEPT !["old"] = FALSE]
    /\ UNCHANGED <<rangeOwner, promotedBy, unownedPromotion>>

\* The range (and its group leadership) transfers to the new side.
TransferRange ==
    /\ handoffStarted
    /\ rangeOwner = "old"
    /\ rangeOwner' = "new"
    /\ UNCHANGED <<attached, handoffStarted, promotedBy, unownedPromotion>>

\* The new side installs its promotion owner. The good path requires the
\* range to have transferred; the mutant attaches while the old side is
\* still attached (hooks applied to the child before the parent detached).
AttachNew ==
    /\ ~attached["new"]
    /\ IF BuggyAttachBeforeDetach
       THEN TRUE
       ELSE /\ handoffStarted
            /\ rangeOwner = "new"
    /\ attached' = [attached EXCEPT !["new"] = TRUE]
    /\ UNCHANGED <<rangeOwner, handoffStarted, promotedBy, unownedPromotion>>

\* A crashed side loses its runtime owner pointer (not durable).
CrashSide(s) ==
    /\ attached[s]
    /\ attached' = [attached EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<rangeOwner, handoffStarted, promotedBy, unownedPromotion>>

\* Recovery re-attaches only behind the range-ownership check.
ReattachAfterRecovery(s) ==
    /\ ~attached[s]
    /\ rangeOwner = s
    /\ (s = "old" => ~handoffStarted)
    /\ attached' = [attached EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<rangeOwner, handoffStarted, promotedBy, unownedPromotion>>

\* The promotion catch-up loop converts resolution artifacts into entity
\* upserts. The good guard is the isLocalOwner predicate: attached AND range
\* owner. The mutant drops the ownership check.
Promote(s) ==
    /\ IF BuggyPromoteWithoutOwnership
       THEN TRUE
       ELSE attached[s] /\ rangeOwner = s
    /\ promotedBy' = [promotedBy EXCEPT ![s] = TRUE]
    /\ unownedPromotion' =
        (unownedPromotion \/ ~(attached[s] /\ rangeOwner = s))
    /\ UNCHANGED <<rangeOwner, attached, handoffStarted>>

Next ==
    \/ StartHandoffDetachOld
    \/ TransferRange
    \/ AttachNew
    \/ \E s \in Sides:
        \/ CrashSide(s)
        \/ ReattachAfterRecovery(s)
        \/ Promote(s)

Spec == Init /\ [][Next]_vars

(*
  Liveness: the handoff completes — once the transition starts, the new side
  is eventually attached and able to promote (no permanently ownerless
  range). Crashes may repeat, so the property is conditional on the new side
  eventually staying up (not crashing forever).
*)
Fairness ==
    /\ WF_vars(TransferRange)
    /\ WF_vars(AttachNew)
    /\ WF_vars(ReattachAfterRecovery("new"))

FairSpec == Spec /\ Fairness

NewSideEventuallyOwns ==
    <>[](handoffStarted) => <>(attached["new"] /\ rangeOwner = "new")

TypeOK ==
    /\ BuggyAttachBeforeDetach \in BOOLEAN
    /\ BuggyPromoteWithoutOwnership \in BOOLEAN
    /\ rangeOwner \in Sides
    /\ attached \in [Sides -> BOOLEAN]
    /\ handoffStarted \in BOOLEAN
    /\ promotedBy \in [Sides -> BOOLEAN]
    /\ unownedPromotion \in BOOLEAN

\* At most one promotion owner is ever attached.
AtMostOneAttachedOwner ==
    ~(attached["old"] /\ attached["new"])

\* An attached owner is the current range owner (detach-before-transfer,
\* attach-after-transfer).
AttachedImpliesRangeOwner ==
    \A s \in Sides: attached[s] => rangeOwner = s \/ ~handoffStarted

\* No promotion ever runs without ownership evidence.
NoUnownedPromotion ==
    ~unownedPromotion

Safety ==
    /\ TypeOK
    /\ AtMostOneAttachedOwner
    /\ AttachedImpliesRangeOwner
    /\ NoUnownedPromotion

=============================================================================
