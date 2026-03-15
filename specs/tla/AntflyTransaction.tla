----------------------------- MODULE AntflyTransaction -----------------------------
(*
  TLA+ Formal Specification of Antfly's Distributed Transaction Protocol.

  Models the full 2PC + OCC + recovery + cleanup protocol as implemented in:
    - src/metadata/transaction.go   (orchestrator: ExecuteTransaction)
    - src/store/db/db.go            (storage: InitTransaction, WriteIntent,
                                     ResolveIntents, transactionRecoveryLoop,
                                     notifyPendingResolutions, checkVersionPredicates,
                                     shouldWriteValue, hasConflictingIntentForKey)
    - src/store/db/helpers.go       (finalizeTransaction)

  Previous model checking found three real bugs:
    1. Orphaned intents from premature txn record cleanup
    2. HLC timestamp collisions during concurrent commits
    3. OCC lost update: two txns reading the same version could both commit
       because checkVersionPredicates only checked committed versions, not
       pending intents (fixed by hasConflictingIntentForKey in PR #381)

  This spec models the OCC predicate check faithfully as two separate steps
  with a window between them (CheckPredicates snapshots committed versions,
  WriteIntentOnShard validates and writes). This structure is what allowed
  the Piledriver spec to catch bug #3 -- the window between snapshot and
  write is where concurrent transactions can interleave.

  Protocol summary:
    Phase 1 (Prepare):
      1. Orchestrator allocates HLC timestamp, creates txn record on coordinator
         shard with status=Pending and participant list.
      2. Orchestrator snapshots committed key versions (OCC read set).
      3. Orchestrator writes intents to all participant shards in parallel.
         Each shard validates OCC predicates (committed version unchanged AND
         no conflicting intents from other txns) before accepting intents.
      4. If any shard rejects (OCC conflict), orchestrator aborts.
    Phase 2 (Commit/Abort):
      5. Orchestrator atomically sets txn record to Committed (or Aborted).
      6. Orchestrator notifies participants to resolve intents (apply or discard).
      7. Each participant resolves intents and reports back.
    Recovery:
      8. Coordinator's recovery loop retries unresolved participants.
      9. Txn record is only deleted when all participants have resolved.

  Safety properties:
    - Atomicity:  Aborted txn writes never appear in the data store.
    - No orphaned intents: Txn records not deleted while intents exist.
    - OCC serialization: Conflicting OCC txns can't both have intents written.
    - LWW consistency: Higher-timestamp writes win during intent resolution.
    - Serializable reads: Two txns that read the same version of a key
      cannot both commit (catches the lost update bug).

  Liveness properties (under weak fairness):
    - Committed intents are eventually all resolved.
    - Fully resolved txn records are eventually cleaned up.
    - Every transaction eventually reaches a terminal decision.
*)

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* --- Constants ---

CONSTANTS
    Txns,          \* Set of transaction identifiers, e.g. {t1, t2}
    Shards,        \* Set of shard identifiers, e.g. {s1, s2}
    Keys,          \* Set of key identifiers, e.g. {k1, k2}
    TxnShards,     \* Function: Txns -> SUBSET Shards (participating shards)
    TxnKeys,       \* Function: [Txns x Shards] -> SUBSET Keys (keys written per shard)
    TxnReadSet,    \* Function: Txns -> SUBSET Keys (OCC read set)
    TxnCoord,      \* Function: Txns -> Shards (coordinator shard per txn)
    MaxTimestamp,   \* Nat: upper bound on clock for state space bounding
    StalePendingThreshold  \* Nat: minimum clock ticks before a Pending txn is
                           \* considered stale and auto-aborted by recovery

ASSUME \A t \in Txns : TxnCoord[t] \in TxnShards[t]
ASSUME \A t \in Txns : TxnShards[t] \subseteq Shards
ASSUME \A t \in Txns, s \in Shards :
    s \in TxnShards[t] => TxnKeys[t, s] \subseteq Keys
ASSUME MaxTimestamp \in Nat /\ MaxTimestamp >= 1
ASSUME StalePendingThreshold \in Nat /\ StalePendingThreshold >= 1

\* --- Variables ---

VARIABLES
    clock,            \* Nat: monotonic HLC clock (models hlc.Now())
    txnStatus,        \* Function: Txns -> {"idle","preparing","predicatesChecked",
                      \*                     "committed","aborting","aborted","done"}
    txnTimestamp,     \* Function: Txns -> Nat (HLC timestamp allocated at init)
    txnRecords,       \* Function: Txns -> {"none","pending","committed","aborted","deleted"}
                      \* State of the txn record on the coordinator shard
    resolvedParts,    \* Function: Txns -> SUBSET Shards
                      \* Which participants have confirmed intent resolution
    intents,          \* Function: [Txns x Shards] -> {"none","written","resolved"}
                      \* State of intents on each shard for each txn
    dataStore,        \* Function: Keys -> [value: Nat, ts: Nat]
                      \* The committed key-value store (value is the txn's timestamp
                      \* used as a proxy for "which txn wrote this"; ts tracks LWW)
    intentShards,     \* Function: Txns -> SUBSET Shards
                      \* Tracks which shards have had intents written (for tracking
                      \* the prepare phase across parallel shard writes)
    predicateSnapshot \* Function: [Txns x Keys] -> Nat
                      \* Snapshot of committed key versions at predicate check time.
                      \* Models the window between CheckPredicates and WriteIntents
                      \* (the vulnerability surface for the lost update bug).

vars == <<clock, txnStatus, txnTimestamp, txnRecords, resolvedParts,
          intents, dataStore, intentShards, predicateSnapshot>>

\* --- Type invariant ---

TypeOK ==
    /\ clock \in Nat
    /\ \A t \in Txns :
        /\ txnStatus[t] \in {"idle","preparing","predicatesChecked",
                              "committed","aborting","aborted","done"}
        /\ txnTimestamp[t] \in Nat
        /\ txnRecords[t] \in {"none","pending","committed","aborted","deleted"}
        /\ resolvedParts[t] \subseteq Shards
        /\ intentShards[t] \subseteq Shards
    /\ \A t \in Txns, s \in Shards :
        intents[t, s] \in {"none","written","resolved"}
    /\ \A k \in Keys :
        /\ dataStore[k].value \in Nat
        /\ dataStore[k].ts \in Nat
    /\ \A t \in Txns, k \in Keys :
        predicateSnapshot[t, k] \in Nat

\* --- Helpers ---

\* The set of non-coordinator participants for a transaction
Participants(t) == TxnShards[t] \ {TxnCoord[t]}

\* All keys that a transaction touches across all its shards
AllTxnKeys(t) == UNION {TxnKeys[t, s] : s \in TxnShards[t]}

\* Check whether two transactions have an OCC conflict:
\* t1 reads a key that t2 writes (or vice versa).
OCCConflict(t1, t2) ==
    /\ t1 /= t2
    /\ \E k \in Keys :
        \/ (k \in TxnReadSet[t1] /\ k \in AllTxnKeys(t2))
        \/ (k \in TxnReadSet[t2] /\ k \in AllTxnKeys(t1))

\* Whether committed-version predicates still hold for txn t on shard s.
\* This checks that the committed dataStore timestamps haven't changed since
\* the predicate snapshot was taken.
\* Maps to: checkVersionPredicates checking GetTimestamp() (the :t suffix).
CommittedVersionPredicatesPass(t, s) ==
    \A k \in TxnKeys[t, s] \intersect TxnReadSet[t] :
        predicateSnapshot[t, k] = dataStore[k].ts

\* Whether there are conflicting pending intents from another transaction.
\* Maps to: hasConflictingIntentForKey() added in PR #381.
NoConflictingIntents(t, s) ==
    ~\E t2 \in Txns :
        /\ t2 /= t
        /\ intents[t2, s] = "written"
        /\ txnRecords[t2] /= "aborted"
        /\ \E k \in TxnKeys[t, s] \intersect TxnReadSet[t] :
            k \in TxnKeys[t2, s]

\* --- Initial state ---

Init ==
    /\ clock = 1
    /\ txnStatus        = [t \in Txns |-> "idle"]
    /\ txnTimestamp      = [t \in Txns |-> 0]
    /\ txnRecords        = [t \in Txns |-> "none"]
    /\ resolvedParts     = [t \in Txns |-> {}]
    /\ intents           = [t \in Txns, s \in Shards |-> "none"]
    /\ dataStore         = [k \in Keys |-> [value |-> 0, ts |-> 0]]
    /\ intentShards      = [t \in Txns |-> {}]
    /\ predicateSnapshot = [t \in Txns, k \in Keys |-> 0]

\* --- Actions ---

(*
  Action 1: InitTransaction(t)
  Orchestrator allocates HLC timestamp and creates txn record on coordinator.
  Maps to: metadata/transaction.go:217 (initTransaction) +
           store/db/db.go:3883 (InitTransaction)
*)
InitTransaction(t) ==
    /\ txnStatus[t] = "idle"
    /\ clock < MaxTimestamp  \* State space bound
    /\ clock' = clock + 1
    /\ txnStatus'    = [txnStatus    EXCEPT ![t] = "preparing"]
    /\ txnTimestamp'  = [txnTimestamp  EXCEPT ![t] = clock]
    /\ txnRecords'    = [txnRecords    EXCEPT ![t] = "pending"]
    /\ UNCHANGED <<resolvedParts, intents, dataStore, intentShards,
                   predicateSnapshot>>

(*
  Action 2: CheckPredicates(t)
  Orchestrator snapshots committed key versions for the OCC read set.
  This happens during the Read phase of the transaction, before writing
  intents. The window between this snapshot and WriteIntentOnShard is
  where the lost update vulnerability exists.
  Maps to: the client-side Read that captures versions, which are later
           sent as VersionPredicates in the WriteIntent RPC.
*)
CheckPredicates(t) ==
    /\ txnStatus[t] = "preparing"
    /\ txnStatus' = [txnStatus EXCEPT ![t] = "predicatesChecked"]
    \* Snapshot current committed timestamps for all keys in read set
    /\ predicateSnapshot' = [x \in Txns \X Keys |->
            IF x[1] = t /\ x[2] \in TxnReadSet[t]
            THEN dataStore[x[2]].ts
            ELSE predicateSnapshot[x]]
    /\ UNCHANGED <<clock, txnTimestamp, txnRecords, resolvedParts,
                   intents, dataStore, intentShards>>

(*
  Action 3: WriteIntentOnShard(t, s)
  Validates OCC predicates and writes intents on shard s.
  Checks BOTH committed version predicates AND conflicting intents.
  This models the FIXED code (post-PR #381) which includes
  hasConflictingIntentForKey() in checkVersionPredicates().
  Maps to: store/db/db.go:3942 (WriteIntent) with both predicate checks.
*)
WriteIntentOnShard(t, s) ==
    /\ txnStatus[t] = "predicatesChecked"
    /\ s \in TxnShards[t]
    /\ intents[t, s] = "none"
    /\ CommittedVersionPredicatesPass(t, s)
    /\ NoConflictingIntents(t, s)
    /\ intents'      = [intents EXCEPT ![t, s] = "written"]
    /\ intentShards' = [intentShards EXCEPT ![t] = intentShards[t] \union {s}]
    /\ UNCHANGED <<clock, txnStatus, txnTimestamp, txnRecords,
                   resolvedParts, dataStore, predicateSnapshot>>

(*
  Action 4: WriteIntentFails(t, s)
  OCC predicate check fails on shard s; txn transitions to aborting.
  Fails if committed version changed OR a conflicting intent exists.
  Maps to: store/db/db.go:3962-3967 (ErrVersionConflict or ErrIntentConflict)
*)
WriteIntentFails(t, s) ==
    /\ txnStatus[t] = "predicatesChecked"
    /\ s \in TxnShards[t]
    /\ intents[t, s] = "none"
    /\ ~CommittedVersionPredicatesPass(t, s) \/ ~NoConflictingIntents(t, s)
    /\ txnStatus' = [txnStatus EXCEPT ![t] = "aborting"]
    /\ UNCHANGED <<clock, txnTimestamp, txnRecords, resolvedParts,
                   intents, dataStore, intentShards, predicateSnapshot>>

(*
  Action 5: CommitTransaction(t)
  All intents written on all shards; orchestrator commits on coordinator.
  This is the atomic commit point.
  Maps to: store/db/helpers.go:148 (finalizeTransaction with status=1)
*)
CommitTransaction(t) ==
    /\ txnStatus[t] = "predicatesChecked"
    /\ intentShards[t] = TxnShards[t]  \* All shards have intents
    /\ txnStatus'  = [txnStatus  EXCEPT ![t] = "committed"]
    /\ txnRecords' = [txnRecords EXCEPT ![t] = "committed"]
    /\ UNCHANGED <<clock, txnTimestamp, resolvedParts, intents,
                   dataStore, intentShards, predicateSnapshot>>

(*
  Action 6: AbortTransaction(t)
  Orchestrator aborts on coordinator (either from OCC failure or crash).
  Maps to: store/db/helpers.go:148 (finalizeTransaction with status=2)
*)
AbortTransaction(t) ==
    /\ txnStatus[t] = "aborting"
    /\ txnStatus'  = [txnStatus  EXCEPT ![t] = "aborted"]
    /\ txnRecords' = [txnRecords EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<clock, txnTimestamp, resolvedParts, intents,
                   dataStore, intentShards, predicateSnapshot>>

(*
  Action 7: ResolveIntentsOnShard(t, s)
  Shard resolves intents for txn t: applies writes if committed (with LWW),
  discards if aborted. Updates resolved_participants on coordinator.
  Also handles no-op resolution when intents were never written (the
  recovery loop sends ResolveIntents to all participants regardless).
  Maps to: store/db/db.go:4039 (ResolveIntents)
*)
ResolveIntentsOnShard(t, s) ==
    /\ intents[t, s] \in {"written", "none"}
    /\ intents[t, s] /= "resolved"
    /\ txnRecords[t] \in {"committed", "aborted"}
    /\ IF intents[t, s] = "written" /\ txnRecords[t] = "committed"
       THEN \* Apply writes with LWW: only write if our timestamp is higher
            dataStore' = [k \in Keys |->
                IF k \in TxnKeys[t, s] /\ txnTimestamp[t] > dataStore[k].ts
                THEN [value |-> txnTimestamp[t], ts |-> txnTimestamp[t]]
                ELSE dataStore[k]]
       ELSE UNCHANGED dataStore
    /\ intents'       = [intents EXCEPT ![t, s] =
                            IF intents[t, s] = "written" THEN "resolved"
                            ELSE intents[t, s]]
    /\ resolvedParts' = [resolvedParts EXCEPT ![t] = resolvedParts[t] \union
                            (IF s \in Participants(t) THEN {s} ELSE {})]
    /\ UNCHANGED <<clock, txnStatus, txnTimestamp, txnRecords, intentShards,
                   predicateSnapshot>>

(*
  Action 8: OrchestratorDone(t)
  Orchestrator observes all intents resolved and transitions to done.
*)
OrchestratorDone(t) ==
    /\ txnStatus[t] \in {"committed", "aborted"}
    /\ \A s \in TxnShards[t] : intents[t, s] \in {"resolved", "none"}
    /\ txnStatus' = [txnStatus EXCEPT ![t] = "done"]
    /\ UNCHANGED <<clock, txnTimestamp, txnRecords, resolvedParts,
                   intents, dataStore, intentShards, predicateSnapshot>>

(*
  Action 9: OrchestratorCrash(t)
  Orchestrator crashes after commit but before all resolves complete.
  The recovery loop on the coordinator will pick this up.
*)
OrchestratorCrash(t) ==
    /\ txnStatus[t] = "committed"
    /\ \E s \in TxnShards[t] : intents[t, s] = "written"
    /\ txnStatus' = [txnStatus EXCEPT ![t] = "done"]
    /\ UNCHANGED <<clock, txnTimestamp, txnRecords, resolvedParts,
                   intents, dataStore, intentShards, predicateSnapshot>>

(*
  Action 10: RecoveryResolve(t, s)
  Recovery loop on coordinator retries resolving intents on shard s.
  Maps to: store/db/db.go:661 (transactionRecoveryLoop) ->
           db.go:679 (notifyPendingResolutions)
*)
RecoveryResolve(t, s) ==
    /\ intents[t, s] \in {"written", "none"}
    /\ intents[t, s] /= "resolved"
    /\ txnRecords[t] \in {"committed", "aborted"}
    /\ txnStatus[t] = "done"
    /\ IF intents[t, s] = "written" /\ txnRecords[t] = "committed"
       THEN dataStore' = [k \in Keys |->
                IF k \in TxnKeys[t, s] /\ txnTimestamp[t] > dataStore[k].ts
                THEN [value |-> txnTimestamp[t], ts |-> txnTimestamp[t]]
                ELSE dataStore[k]]
       ELSE UNCHANGED dataStore
    /\ intents'       = [intents EXCEPT ![t, s] =
                            IF intents[t, s] = "written" THEN "resolved"
                            ELSE intents[t, s]]
    /\ resolvedParts' = [resolvedParts EXCEPT ![t] = resolvedParts[t] \union
                            (IF s \in Participants(t) THEN {s} ELSE {})]
    /\ UNCHANGED <<clock, txnStatus, txnTimestamp, txnRecords, intentShards,
                   predicateSnapshot>>

(*
  Action 11: CleanupTxnRecord(t)
  Recovery loop deletes the txn record only when ALL participants have resolved.
  Maps to: store/db/db.go:724-743 (allResolved check in notifyPendingResolutions)
*)
CleanupTxnRecord(t) ==
    /\ txnRecords[t] \in {"committed", "aborted"}
    /\ resolvedParts[t] = Participants(t)
    /\ \A s \in TxnShards[t] : intents[t, s] \in {"resolved", "none"}
    /\ txnRecords' = [txnRecords EXCEPT ![t] = "deleted"]
    /\ UNCHANGED <<clock, txnStatus, txnTimestamp, resolvedParts,
                   intents, dataStore, intentShards, predicateSnapshot>>

(*
  Action 12: TickClock
  Models the passage of time independent of transaction activity.
  The HLC clock advances via wall clock time, heartbeats, and other
  node activity, not just transaction operations. Without this action,
  the clock could get stuck when all transactions are terminal, preventing
  RecoveryAutoAbort from ever becoming enabled.
*)
TickClock ==
    /\ clock < MaxTimestamp
    /\ clock' = clock + 1
    /\ UNCHANGED <<txnStatus, txnTimestamp, txnRecords, resolvedParts,
                   intents, dataStore, intentShards, predicateSnapshot>>

(*
  Action 13: OrchestratorCrashPrepare(t)
  Orchestrator crashes during the prepare phase (before commit or abort),
  leaving the txn record stuck in Pending with intents potentially written.
  This models the bug: commitTransaction/abortTransaction fails (network
  error, context timeout) and the orchestrator gives up, but the txn record
  stays Pending forever because the recovery loop only processes status != 0.
  Maps to: metadata/transaction.go:54 (abort fails silently after intent write error)
*)
OrchestratorCrashPrepare(t) ==
    /\ txnStatus[t] \in {"preparing", "predicatesChecked"}
    /\ txnRecords[t] = "pending"
    /\ txnStatus' = [txnStatus EXCEPT ![t] = "done"]
    /\ UNCHANGED <<clock, txnTimestamp, txnRecords, resolvedParts,
                   intents, dataStore, intentShards, predicateSnapshot>>

(*
  Action 13: RecoveryAutoAbort(t)
  Recovery loop on the coordinator detects a stale Pending txn record
  (created_at older than StalePendingThreshold) and auto-aborts it.
  After this, the existing RecoveryResolve and CleanupTxnRecord actions
  handle intent resolution and cleanup since they accept txnRecords = "aborted".
  Maps to: store/db/db.go notifyPendingResolutions (new code that checks
           status=0 && created_at < cutoff and proposes AbortTransaction)
*)
RecoveryAutoAbort(t) ==
    /\ txnRecords[t] = "pending"
    /\ txnStatus[t] = "done"
    /\ clock - txnTimestamp[t] >= StalePendingThreshold
    /\ txnRecords' = [txnRecords EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<clock, txnStatus, txnTimestamp, resolvedParts,
                   intents, dataStore, intentShards, predicateSnapshot>>

\* --- Next-state relation ---

Next ==
    \/ TickClock
    \/ \E t \in Txns :
        \/ InitTransaction(t)
        \/ CheckPredicates(t)
        \/ CommitTransaction(t)
        \/ AbortTransaction(t)
        \/ OrchestratorDone(t)
        \/ OrchestratorCrash(t)
        \/ OrchestratorCrashPrepare(t)
        \/ RecoveryAutoAbort(t)
        \/ CleanupTxnRecord(t)
        \/ \E s \in Shards :
            \/ WriteIntentOnShard(t, s)
            \/ WriteIntentFails(t, s)
            \/ ResolveIntentsOnShard(t, s)
            \/ RecoveryResolve(t, s)

\* --- Fairness ---

Fairness ==
    /\ WF_vars(TickClock)
    /\ \A t \in Txns :
        /\ WF_vars(InitTransaction(t))
        /\ WF_vars(CheckPredicates(t))
        /\ WF_vars(CommitTransaction(t))
        /\ WF_vars(AbortTransaction(t))
        /\ WF_vars(OrchestratorDone(t))
        /\ WF_vars(RecoveryAutoAbort(t))
        /\ WF_vars(CleanupTxnRecord(t))
        /\ \A s \in Shards :
            /\ WF_vars(WriteIntentOnShard(t, s))
            /\ WF_vars(WriteIntentFails(t, s))
            /\ WF_vars(ResolveIntentsOnShard(t, s))
            /\ WF_vars(RecoveryResolve(t, s))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ========================================================================
\* Safety Invariants
\* ========================================================================

(*
  AtomicityInvariant:
  If a transaction is aborted, none of its writes appear in the data store
  with that transaction's timestamp.
*)
AtomicityInvariant ==
    \A t \in Txns :
        txnRecords[t] = "aborted" =>
            \A k \in AllTxnKeys(t) :
                dataStore[k].value /= txnTimestamp[t] \/ txnTimestamp[t] = 0

(*
  NoOrphanedIntents:
  A transaction record must not be deleted while any intents exist in
  "written" state. Catches the orphaned intents bug (bug #1).
*)
NoOrphanedIntents ==
    \A t \in Txns :
        txnRecords[t] = "deleted" =>
            \A s \in TxnShards[t] : intents[t, s] /= "written"

(*
  OCCSerializationInvariant:
  Two transactions with overlapping read-write sets cannot both have intents
  in "written" state on the same shard while neither is aborted.
*)
OCCSerializationInvariant ==
    \A t1, t2 \in Txns :
        /\ t1 /= t2
        /\ OCCConflict(t1, t2)
        =>
        ~(\E s \in Shards :
            /\ intents[t1, s] = "written"
            /\ intents[t2, s] = "written"
            /\ txnRecords[t1] /= "aborted"
            /\ txnRecords[t2] /= "aborted")

(*
  LWWConsistency:
  After a committed transaction's intents are resolved on a shard,
  the data store for each key either reflects that write or has a
  higher timestamp from another transaction.
*)
LWWConsistency ==
    \A t \in Txns :
        /\ txnRecords[t] = "committed"
        /\ txnTimestamp[t] > 0
        =>
        \A s \in TxnShards[t] :
            intents[t, s] = "resolved" =>
                \A k \in TxnKeys[t, s] :
                    dataStore[k].ts >= txnTimestamp[t]
                    \/ dataStore[k].value = txnTimestamp[t]

(*
  SerializableReads:
  Two committed transactions that both checked the same version of a key
  in their predicate snapshots cannot both have committed. This catches
  the OCC lost update bug (#3) where checkVersionPredicates only checked
  committed versions and not pending intents.
  Adapted from the Piledriver spec (occ-2pc.tla).
*)
SerializableReads ==
    \A t1, t2 \in Txns :
        /\ t1 /= t2
        /\ txnRecords[t1] = "committed"
        /\ txnRecords[t2] = "committed"
        =>
        \A k \in Keys :
            \* If both read the same key and both snapshotted it...
            /\ k \in TxnReadSet[t1]
            /\ k \in TxnReadSet[t2]
            /\ k \in AllTxnKeys(t1)
            /\ k \in AllTxnKeys(t2)
            \* ...they must have seen different versions
            => predicateSnapshot[t1, k] /= predicateSnapshot[t2, k]
               \/ predicateSnapshot[t1, k] = 0
               \/ predicateSnapshot[t2, k] = 0

\* Combined safety invariant for convenience
SafetyInvariant ==
    /\ TypeOK
    /\ AtomicityInvariant
    /\ NoOrphanedIntents
    /\ OCCSerializationInvariant
    /\ LWWConsistency
    /\ SerializableReads

\* ========================================================================
\* Liveness Properties
\* ========================================================================

(*
  EventualCompletion:
  If a transaction is committed, all of its intents are eventually resolved.
*)
EventualCompletion ==
    \A t \in Txns :
        txnRecords[t] = "committed" ~>
            \A s \in TxnShards[t] : intents[t, s] \in {"resolved", "none"}

(*
  EventualCleanup:
  Once all participants have resolved and the transaction is terminal,
  the txn record is eventually deleted.
*)
EventualCleanup ==
    \A t \in Txns :
        (/\ txnRecords[t] \in {"committed", "aborted"}
         /\ \A s \in TxnShards[t] : intents[t, s] \in {"resolved", "none"}
         /\ resolvedParts[t] = Participants(t))
        ~> txnRecords[t] = "deleted"

(*
  EventualDecision:
  No transaction stays in "preparing" or "predicatesChecked" forever.
*)
EventualDecision ==
    \A t \in Txns :
        txnStatus[t] \in {"preparing", "predicatesChecked"} ~>
            txnStatus[t] \in {"committed", "aborted", "done"}

(*
  EventualPendingResolution:
  If a transaction record is stuck Pending after the orchestrator is done
  (crashed), it is eventually auto-aborted and cleaned up by the recovery loop.
  This catches the stuck-pending bug where Pending records were ignored forever.
*)
EventualPendingResolution ==
    \A t \in Txns :
        (txnRecords[t] = "pending" /\ txnStatus[t] = "done")
        ~> txnRecords[t] \in {"aborted", "deleted"}

=============================================================================
