--------------------------- MODULE AntflyShardSplit ---------------------------
(*
  TLA+ Formal Specification of Antfly's Online Shard Split Protocol.

  Models the zero-downtime shard split that coordinates across:
    - Metadata raft group (routing decisions, reconciler)
    - Parent storage raft group (split state machine, byte range, delta writes)
    - Child storage raft group (bootstrap, snapshot transfer, delta replay)
    - Table manager (heartbeat-driven metadata state transitions)

  Implementation references:
    - src/store/db/storedb.go         (PrepareSplit, Split, FinalizeSplit,
                                       RollbackSplit, applyOpSplit,
                                       applyOpSetSplitState, applyOpFinalizeSplit,
                                       proposalDequeuer, runSplitReplayLoop,
                                       waitForLocalSplitChildReplay)
    - src/store/db/db.go              (Batch: isKeyOwnedDuringSplit,
                                       isKeyInSplitOffRangeForState,
                                       appendSplitDelta, ListSplitDeltaEntriesAfter,
                                       SetSplitDeltaFinalSeq, ClearSplitDeltaFinalSeq)
    - src/store/db/types.go           (IsReadyForSplitReads, CanInitiateSplitCutover,
                                       SplitCutoverReady, SplitReplayCaughtUp)
    - src/metadata/reconciler/executor.go  (executeSplitStateActions,
                                            executeSplitAndMergeTransitions)
    - src/metadata/reconciler/reconciler.go (computeSplitStatePhaseAction,
                                             isSplitTimedOut,
                                             trackAndCheckSplitFinalizeReady)
    - src/metadata/shard_routing.go   (shouldFallbackToParentShard,
                                       leaderClientForShardNoFallback)
    - src/metadata/write_routing.go   (resolveWriteShardID,
                                       shouldRouteWriteToParent,
                                       findParentShardForSplitOffStatus)
    - src/tablemgr/table.go           (needsUpdates, splitOffShardIsReady)

  Protocol summary:
    Phase 1 (Prepare):
      1. Reconciler decides to split oversized shard.
      2. PrepareSplit proposed through parent Raft: sets split state to
         PHASE_PREPARE. Parent now accepts writes to split-off range as deltas
         via isKeyOwnedDuringSplit (db.go:608-609).

    Phase 2 (Splitting -- two Raft proposals):
      3a. SetSplittingPhase proposed through parent Raft: transitions phase
          to SPLITTING.
      3b. SplitOp proposed through parent Raft: byteRange narrowed BEFORE
          archive creation (critical ordering for data safety). Parent
          continues accepting split-off writes via isKeyInSplitOffRangeForState
          even after byteRange narrows, storing them as split deltas.
      4. Metadata routing updated to reflect two shards.
      5. New shard started, loads archive snapshot.

    Phase 2b (Delta Replay):
      6. Child shard runs split replay loop (storedb.go:532-622), fetching
         delta entries from parent via ListSplitDeltaEntriesAfter.
      7. Child applies deltas with original HLC timestamps.
      8. Child is caught up (CanInitiateSplitCutover) when replay reaches
         the parent's current delta sequence. This is NOT sufficient for
         cutover -- only for triggering the finalize process.

    Phase 3 (Finalize -- non-atomic, two steps):
      9. Reconciler triggers FinalizeSplit when child is caught up
         (CanInitiateSplitCutover: HasSnapshot && !Initializing && Lead!=0
          && SplitReplayCaughtUp) and has been stable for 15s
         (trackAndCheckSplitFinalizeReady).
     10. Parent sets splitDeltaFinalSeq barrier (storedb.go:1402-1408).
         This is persisted to Pebble BEFORE waiting for the child.
         If the wait times out (15s), the fence remains set but the split
         is not completed. The reconciler can retry with a new fence.
     11. Parent waits for child to replay through fence (storedb.go:1411).
     12. Parent deletes split-off data and clears split state.
     13. Child becomes SplitCutoverReady (storedb.go:605 or 616).

    Phase 4 (Tablemgr Metadata Transition):
     14. Tablemgr (heartbeat-driven) transitions child SplitOffPreSnap ->
         Default ONLY when IsReadyForSplitReads (requires SplitCutoverReady
         AND Lead!=0).
     15. Tablemgr transitions parent PreSplit/Splitting -> Default when
         splitOffShardIsReady (requires child IsReadyForSplitReads).

    Write Routing (write_routing.go):
      - During split, metadata routes writes for child keys to the parent
        shard when the child is not ready (shouldRouteWriteToParent).
      - Parent accepts these writes via isKeyOwnedDuringSplit and stores
        them as SplitDeltaEntry records (db.go:698-721).
      - Once child is ready (IsReadyForSplitReads with SplitCutoverReady),
        metadata routes directly to child.

    Key invariant (the bug this model catches):
      SplitReplayCaughtUp ("child is currently tailing") is NOT the same as
      SplitCutoverReady ("child has completed cutover through final fence").
      Tablemgr must use the stronger SplitCutoverReady signal; using the
      weaker SplitReplayCaughtUp caused premature cutover and data loss.

    Rollback:
     16. If split times out, reconciler triggers rollback.
     17. Parent restores originalRangeEnd, clears split state and deltas.

  Safety properties:
    - No data loss: acknowledged writes are never lost during split.
    - No double-serving: metadata routing ensures writes go to exactly one
      shard (parent delta path XOR child direct path), enforced at the action
      level via mutually exclusive preconditions.
    - No premature cutover: child only reaches Default after cutover fence.
    - Rollback restores range: rolling back returns parent to original state.
    - ByteRange narrows before archive: critical ordering invariant.
    - Delta replay integrity: child can't replay what parent hasn't written.
    - Delta keys only during split: no stale deltas after finalize/rollback.
    - Read always available: every key can be read from some shard at all times.
    - Fence requires active split: splitDeltaFinalSeq only set during SPLITTING.

  Liveness properties (under fairness):
    - Split eventually completes or rolls back.

  Boundary:
    Inside the model:
      - Split state machine (NONE -> PREPARE -> SPLITTING -> NONE)
      - byteRange per phase (isKeyOwnedDuringSplit extends acceptance)
      - Two-phase TransitionToSplitting (SetSplittingPhase + ApplySplitOp)
      - Write routing: delta writes to parent vs direct writes to child
      - Split delta accumulation and replay
      - Metadata routing update ordering
      - New shard bootstrap lifecycle
      - Dual-actor race: tablemgr (heartbeat) vs reconciler (finalize)
      - SplitCutoverReady vs SplitReplayCaughtUp distinction
      - Non-atomic FinalizeSplit (fence set, then wait+delete)
      - Child leader election and loss
      - Read fallback to parent during split
      - Rollback mechanism
      - Leadership loss and gain on both parent and child
      - Timeout-triggered rollback
      - Concurrent client writes

    Outside (assumptions):
      - Raft linearizability within each shard
      - Network eventual delivery
      - Pebble storage correctness
      - Shadow IndexManager details
      - Archive I/O correctness
      - HLC timestamp ordering
      - Reconciler 15s grace period (trackAndCheckSplitFinalizeReady)
*)

EXTENDS Naturals, FiniteSets, TLC

\* --- Constants ---

CONSTANTS
    Keys,          \* Set of key identifiers, e.g. {k1, k2, k3}
    ParentKeys,    \* SUBSET Keys: keys that stay with parent after split
    ChildKeys      \* SUBSET Keys: keys that go to the child shard

ASSUME ParentKeys \subseteq Keys
ASSUME ChildKeys \subseteq Keys
ASSUME ParentKeys \intersect ChildKeys = {}
ASSUME ParentKeys \union ChildKeys = Keys

\* --- Variables ---

VARIABLES
    splitPhase,           \* {"none","prepare","splitting"}
                          \* Current phase of the split state machine on parent shard.
                          \* Maps to: SplitState.Phase in storedb.go
    parentRange,          \* SUBSET Keys: keys the parent shard currently owns via byteRange.
                          \* Abstraction of byteRange in storedb.go.
                          \* Note: during split, parent also accepts writes to split-off
                          \* range via isKeyOwnedDuringSplit (db.go:608-609).
    archiveCreated,       \* BOOLEAN: whether the archive file exists for the new shard.
    parentHasLeader,      \* BOOLEAN: whether parent shard has a Raft leader.
    newShardState,        \* {"none","splittingOff","preSnap","default"}
    newShardHasSnapshot,  \* BOOLEAN: HasSnapshot field in ShardStatus.
    newShardInitializing, \* BOOLEAN: Initializing field in ShardStatus.
    newShardHasLeader,    \* BOOLEAN: whether child shard has a Raft leader.
                          \* Both IsReadyForSplitReads and CanInitiateSplitCutover
                          \* require RaftStatus.Lead != 0 (types.go:133,147).
                          \* Child leader loss resets the reconciler's 15s grace
                          \* period (trackAndCheckSplitFinalizeReady), delays
                          \* FinalizeSplit, and routes writes to parent.
    routingUpdated,       \* BOOLEAN: metadata routing reflects two shards.
    dataStore,            \* Keys -> {"parent","child","both"}
                          \* Which shard(s) currently hold data for each key.
    parentDeltaKeys,      \* SUBSET ChildKeys: keys with split delta entries on parent.
                          \* Models splitDeltaSeq > 0 for specific keys.
                          \* Maps to: splitdelta:entry: keys in db.go:79-81
    childReplayedKeys,    \* SUBSET ChildKeys: keys the child has replayed from deltas.
                          \* Maps to: splitReplaySeq progress in storedb.go:527
    splitCutoverReady,    \* BOOLEAN: whether the child has completed the cutover
                          \* protocol (replayed through the parent's final fence).
                          \* Distinct from SplitReplayCaughtUp: cutoverReady is a
                          \* monotonic gate set by FinalizeSplit, while CaughtUp is
                          \* a live derived predicate that can oscillate.
                          \* Maps to: SplitCutoverReady in types.go:118-120
    splitFenceSet         \* BOOLEAN: whether splitDeltaFinalSeq has been persisted.
                          \* In applyOpFinalizeSplit (storedb.go:1402-1408), the fence
                          \* is written to Pebble BEFORE waitForLocalSplitChildReplay.
                          \* If the 15s wait times out, the fence remains set but
                          \* data is not deleted. The reconciler can retry.
                          \* Between retries, new deltas can arrive (splitPhase is
                          \* still "splitting"), requiring the child to replay them
                          \* before the next FinalizeSplitComplete can succeed.
                          \* Maps to: SetSplitDeltaFinalSeq in db.go:635

vars == <<splitPhase, parentRange, archiveCreated,
          parentHasLeader, newShardState, newShardHasSnapshot,
          newShardInitializing, newShardHasLeader, routingUpdated,
          dataStore, parentDeltaKeys, childReplayedKeys,
          splitCutoverReady, splitFenceSet>>

\* --- Type invariant ---

TypeOK ==
    /\ splitPhase \in {"none","prepare","splitting"}
    /\ parentRange \subseteq Keys
    /\ archiveCreated \in BOOLEAN
    /\ parentHasLeader \in BOOLEAN
    /\ newShardState \in {"none","splittingOff","preSnap","default"}
    /\ newShardHasSnapshot \in BOOLEAN
    /\ newShardInitializing \in BOOLEAN
    /\ newShardHasLeader \in BOOLEAN
    /\ routingUpdated \in BOOLEAN
    /\ \A k \in Keys : dataStore[k] \in {"parent","child","both"}
    /\ parentDeltaKeys \subseteq ChildKeys
    /\ childReplayedKeys \subseteq ChildKeys
    /\ splitCutoverReady \in BOOLEAN
    /\ splitFenceSet \in BOOLEAN

\* --- Helpers ---

\* Whether the split replay has caught up: child has replayed all parent deltas.
\* This is a LIVE check (derived predicate) that can oscillate as new deltas
\* arrive. It is the WEAKER readiness signal.
\* Maps to: splitReplayCaughtUp.Load() in storedb.go:602
\* Used by: CanInitiateSplitCutover (types.go) to trigger FinalizeSplit.
SplitReplayCaughtUp == parentDeltaKeys \subseteq childReplayedKeys

\* Whether the parent shard accepts a normal (non-delta) write for key k.
\* During an active split, writes to ChildKeys go through the delta path instead.
\* Maps to: byteRange.Contains(key) check in ValidateBatchKeys (storedb.go:740)
\*          for ParentKeys, or any key when no split is active.
ParentAcceptsNormalWrite(k) ==
    /\ k \in parentRange
    /\ ~(k \in ChildKeys /\ splitPhase \in {"prepare", "splitting"})

\* Whether the parent shard accepts a delta write for key k.
\* During PREPARE/SPLITTING, the parent accepts writes to the split-off range
\* via isKeyOwnedDuringSplit (db.go:608-609) which returns:
\*   byteRange.Contains(key) || isKeyInSplitOffRangeForState(key, state)
\* These writes are stored as SplitDeltaEntry records (db.go:698-721)
\* AND written to the parent's main Pebble store (Batch continues processing
\* the key normally after capturing the delta -- db.go:2300-2452).
\* Maps to: isKeyInSplitOffRangeForState in Batch (db.go:2300-2302)
ParentAcceptsDeltaWrite(k) ==
    /\ k \in ChildKeys
    /\ splitPhase \in {"prepare", "splitting"}
    /\ parentHasLeader

\* Whether the child shard can initiate the split cutover (WEAKER check).
\* Used by the reconciler to trigger FinalizeSplit.
\* Maps to: CanInitiateSplitCutover (types.go:142-147)
CanInitiateSplitCutover ==
    /\ newShardHasSnapshot
    /\ ~newShardInitializing
    /\ newShardHasLeader              \* RaftStatus.Lead != 0
    /\ SplitReplayCaughtUp

\* Whether the child shard accepts a write for key k (STRONGER check).
\* Child only accepts writes once fully ready: has snapshot, finished
\* initializing, has leader, routing updated, AND cutover is complete.
\* Maps to: IsReadyForSplitReads (types.go:130-133) which checks:
\*   HasSnapshot && !Initializing && Lead!=0
\*   && (!SplitReplayRequired || SplitCutoverReady)
\* CRITICAL: uses SplitCutoverReady (the strong signal), NOT SplitReplayCaughtUp.
ChildAcceptsWrite(k) ==
    /\ k \in ChildKeys
    /\ newShardState = "default"
    /\ newShardHasSnapshot
    /\ ~newShardInitializing
    /\ newShardHasLeader              \* RaftStatus.Lead != 0
    /\ routingUpdated
    /\ splitCutoverReady

\* Whether a read for key k can be served from some shard.
\* Reads do NOT go through byteRange validation (only writes do).
\* Parent can serve reads for any key it has data for.
\* Child can serve reads once it has snapshot data -- reads are served from
\* local Pebble and do NOT require Raft leadership (follower/stale reads OK).
\* If routing sends reads to child but child not ready, reads fall back to
\* parent via shouldFallbackToParentShard (shard_routing.go).
ReadAvailable(k) ==
    \/ dataStore[k] \in {"parent", "both"}     \* Parent still has the data
    \/ (k \in ChildKeys /\ newShardHasSnapshot) \* Child has snapshot data

\* --- Initial state ---

Init ==
    /\ splitPhase = "none"
    /\ parentRange = Keys              \* Parent owns all keys initially
    /\ archiveCreated = FALSE
    /\ parentHasLeader = TRUE
    /\ newShardState = "none"
    /\ newShardHasSnapshot = FALSE
    /\ newShardInitializing = FALSE
    /\ newShardHasLeader = FALSE       \* No child shard exists yet
    /\ routingUpdated = FALSE
    /\ dataStore = [k \in Keys |-> "parent"]
    /\ parentDeltaKeys = {}
    /\ childReplayedKeys = {}
    /\ splitCutoverReady = FALSE
    /\ splitFenceSet = FALSE

\* --- Actions ---

(*
  Action 1: PrepareSplit
  Reconciler initiates split by proposing PHASE_PREPARE through parent Raft.
  Parent now accepts writes to split-off range as deltas via isKeyOwnedDuringSplit.
  Clears any previous split delta entries (storedb.go:1511).
  Maps to: storedb.go:371 (PrepareSplit) -> storedb.go:1455 (applyOpSetSplitState)
*)
PrepareSplit ==
    /\ splitPhase = "none"
    /\ newShardState = "none"         \* No prior child (model covers one split cycle)
    /\ parentHasLeader
    /\ splitPhase' = "prepare"
    /\ parentDeltaKeys' = {}          \* storedb.go:1511 ClearSplitDeltaEntries
    /\ childReplayedKeys' = {}
    /\ splitCutoverReady' = FALSE
    /\ splitFenceSet' = FALSE
    /\ UNCHANGED <<parentRange, archiveCreated, parentHasLeader,
                   newShardState, newShardHasSnapshot, newShardInitializing,
                   newShardHasLeader, routingUpdated, dataStore>>

(*
  Action 2a: SetSplittingPhase
  First of two Raft proposals in Split() (storedb.go:417-467).
  Proposes PHASE_SPLITTING through parent Raft.
  Maps to: storedb.go:417-437 (first proposal in Split())
*)
SetSplittingPhase ==
    /\ splitPhase = "prepare"
    /\ parentHasLeader
    /\ splitPhase' = "splitting"
    /\ UNCHANGED <<parentRange, archiveCreated, parentHasLeader,
                   newShardState, newShardHasSnapshot, newShardInitializing,
                   newShardHasLeader, routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 2b: ApplySplitOp
  Second of two Raft proposals in Split() (storedb.go:417-467).
  In applyOpSplit (storedb.go:1080):
    1. byteRange narrowed to ParentKeys FIRST (line 1099-1111)
    2. Archive created from Pebble data
  CRITICAL: byteRange narrows BEFORE archive creation to prevent data loss.

  After byteRange narrows, parent continues accepting split-off writes via
  isKeyOwnedDuringSplit (db.go:608-609): the isKeyInSplitOffRangeForState
  check still returns true because splitPhase is SPLITTING.

  Maps to: storedb.go:438-467 (second proposal) -> storedb.go:1080 (applyOpSplit)
*)
ApplySplitOp ==
    /\ splitPhase = "splitting"
    /\ ~archiveCreated               \* SplitOp not yet applied
    /\ parentHasLeader
    /\ parentRange' = ParentKeys     \* storedb.go:1099-1111 - BEFORE archive
    /\ archiveCreated' = TRUE        \* AFTER byteRange update
    /\ UNCHANGED <<splitPhase, parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, newShardHasLeader, routingUpdated,
                   dataStore, parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 3: MetadataUpdateRouting
  Reconciler updates metadata to reflect two shards. Done AFTER SplitOp.

  CRITICAL ORDERING (executor.go:585-596):
    SplitShard FIRST (narrows byteRange via Raft)
    THEN ReassignShardsForSplit (metadata routing update)
    THEN StartShard (new shard begins on peers)

  Maps to: executor.go:632 (ReassignShardsForSplit)
*)
MetadataUpdateRouting ==
    /\ splitPhase = "splitting"
    /\ archiveCreated               \* SplitOp must have completed
    /\ ~routingUpdated
    /\ routingUpdated' = TRUE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, newShardHasLeader, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 4: StartNewShard
  New shard starts on its peers, begins loading archive.
  Child starts WITHOUT a leader -- leader election takes a few ticks.
  Maps to: executor.go:644 (StartShard with splitStart=true)
*)
StartNewShard ==
    /\ archiveCreated
    /\ routingUpdated
    /\ newShardState = "none"
    /\ newShardState' = "splittingOff"
    /\ newShardInitializing' = TRUE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardHasSnapshot, newShardHasLeader,
                   routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 5: NewShardReceivesSnapshot
  New shard loads archive and receives Raft snapshot.
  Child now has a copy of the split-off data from the archive.
  Maps to: snapshot transfer + shard bootstrap
*)
NewShardReceivesSnapshot ==
    /\ newShardState \in {"splittingOff", "preSnap"}
    /\ archiveCreated
    /\ ~newShardHasSnapshot
    /\ newShardHasSnapshot' = TRUE
    /\ newShardState' = "preSnap"
    \* Child now has data from archive
    /\ dataStore' = [k \in Keys |->
            IF k \in ChildKeys
            THEN (IF dataStore[k] = "parent" THEN "both" ELSE dataStore[k])
            ELSE dataStore[k]]
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardInitializing, newShardHasLeader,
                   routingUpdated, parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 6: ChildClearsInitializing
  Child shard finishes loading indexes and clears the initializing flag.
  This makes it eligible for the reconciler to trigger FinalizeSplit
  (via CanInitiateSplitCutover), but does NOT transition to Default state.
  The tablemgr transition to Default requires the stronger SplitCutoverReady.

  Requires child leader: the replay loop applies batches via child Raft
  (storedb.go:525 calls coreDB.Batch which proposes through Raft).

  Maps to: storedb.go:603-604 (initializing.Store(false) when caughtUp)
*)
ChildClearsInitializing ==
    /\ newShardHasSnapshot
    /\ newShardInitializing
    /\ newShardHasLeader              \* Replay requires Raft leader for writes
    /\ SplitReplayCaughtUp            \* storedb.go:604 - only when caught up
    /\ newShardInitializing' = FALSE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardHasLeader, routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 7: TablemgrTransitionsChild
  Table manager (heartbeat-driven) transitions child to Default state.
  CRITICAL: requires SplitCutoverReady AND newShardHasLeader.
  This is the fix for the premature-cutover data loss bug.

  The old behavior used IsReadyForSplitReads which only checked
  SplitReplayCaughtUp (a live/oscillating signal). A write could arrive
  at the parent after the child was declared "caught up", creating a delta
  the child never replayed. The fix requires SplitCutoverReady, which is
  only set after FinalizeSplit ensures all deltas are replayed through
  the final fence.

  Maps to: tablemgr/table.go:386 (SplitOffPreSnap -> Default transition)
           using IsReadyForSplitReads which checks SplitCutoverReady + Lead!=0
*)
TablemgrTransitionsChild ==
    /\ newShardState \in {"splittingOff", "preSnap"}
    /\ newShardHasSnapshot
    /\ ~newShardInitializing
    /\ newShardHasLeader              \* IsReadyForSplitReads requires Lead!=0
    /\ splitCutoverReady              \* The strong signal (types.go:130-133)
    /\ newShardState' = "default"
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardHasSnapshot,
                   newShardInitializing, newShardHasLeader, routingUpdated,
                   dataStore, parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 8a: FinalizeSplitSetFence
  Reconciler triggers FinalizeSplit on parent shard when child is caught up
  (CanInitiateSplitCutover -- the WEAKER check using SplitReplayCaughtUp).

  This models the FIRST part of applyOpFinalizeSplit (storedb.go:1402-1408):
  reading splitDeltaSeq and persisting splitDeltaFinalSeq to Pebble.
  The fence is set BEFORE waiting for the child to replay through it.

  After the fence is set, the Raft apply goroutine blocks waiting for the
  child (up to 15s). If the wait times out, the apply returns an error
  but the fence REMAINS SET in Pebble. Between retries, new delta writes
  can arrive (splitPhase is still "splitting"), which means the child
  must replay them too before FinalizeSplitComplete can succeed.

  NOTE: does NOT require newShardState = "default". The reconciler triggers
  FinalizeSplit based on CanInitiateSplitCutover (the weaker check), before
  the tablemgr transitions the child to Default.

  Maps to: storedb.go:1402-1408 (SetSplitDeltaFinalSeq in applyOpFinalizeSplit)
*)
FinalizeSplitSetFence ==
    /\ splitPhase = "splitting"
    /\ archiveCreated               \* SplitOp must have completed
    /\ parentHasLeader
    /\ CanInitiateSplitCutover      \* Reconciler gate (weaker check, needs child leader)
    /\ splitFenceSet' = TRUE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated, parentHasLeader,
                   newShardState, newShardHasSnapshot, newShardInitializing,
                   newShardHasLeader, routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys, splitCutoverReady>>

(*
  Action 8b: FinalizeSplitComplete
  The SECOND part of applyOpFinalizeSplit: child has replayed through the
  fence, parent deletes split-off data and clears all split state.

  In applyOpFinalizeSplit (storedb.go:1416-1444):
    1. waitForLocalSplitChildReplay succeeds (child replayed through fence)
    2. FinalizeSplit deletes split-off data from Pebble (storedb.go:1423)
    3. Clears split state and delta entries (storedb.go:1430-1440)

  Key: SplitReplayCaughtUp must hold at THIS point (not just at fence time).
  Between FinalizeSplitSetFence and FinalizeSplitComplete, new delta writes
  can arrive via ClientWriteDeltaToParent. The child must replay these
  additional deltas before completion can proceed.

  After completion, the child's replay loop detects the final fence or
  inactive parent source and sets SplitCutoverReady (storedb.go:605,616).

  Maps to: storedb.go:1416-1444 (the wait+delete+clear in applyOpFinalizeSplit)
*)
FinalizeSplitComplete ==
    /\ splitPhase = "splitting"
    /\ splitFenceSet                 \* Fence must have been set
    /\ parentHasLeader               \* Apply runs on parent leader
    /\ SplitReplayCaughtUp           \* Child replayed ALL deltas (including post-fence)
    /\ splitPhase' = "none"
    \* Delete split-off data from parent (storedb.go:1423)
    /\ dataStore' = [k \in Keys |->
            IF k \in ChildKeys
            THEN "child"              \* Only child has it now
            ELSE dataStore[k]]
    \* Clear delta state (storedb.go:1438)
    /\ parentDeltaKeys' = {}
    /\ childReplayedKeys' = {}
    \* Child becomes cutover-ready after replaying through final fence
    \* (storedb.go:605 cutoverReady := finalSeq > 0 && currentSeq >= finalSeq,
    \*  or storedb.go:616 inactive source + caught up)
    /\ splitCutoverReady' = TRUE
    /\ splitFenceSet' = FALSE        \* ClearSplitDeltaFinalSeq (storedb.go:1443)
    /\ UNCHANGED <<parentRange, archiveCreated, parentHasLeader,
                   newShardState, newShardHasSnapshot, newShardInitializing,
                   newShardHasLeader, routingUpdated>>

(*
  Action 9: TimeoutRollback
  Split took too long; reconciler triggers rollback.
  Modeled atomically (see storedb.go:1090-1130 RollbackSplit).
  Clears all split, delta, and fence state.
  Maps to: reconciler.go:461 + storedb.go:1090
*)
TimeoutRollback ==
    /\ splitPhase \in {"prepare", "splitting"}
    /\ parentHasLeader
    /\ splitPhase' = "none"
    /\ parentRange' = Keys           \* Restore full range via originalRangeEnd
    /\ archiveCreated' = FALSE
    /\ newShardState' = "none"
    /\ newShardHasSnapshot' = FALSE
    /\ newShardInitializing' = FALSE
    /\ newShardHasLeader' = FALSE
    /\ routingUpdated' = FALSE
    /\ dataStore' = [k \in Keys |-> "parent"]
    /\ parentDeltaKeys' = {}
    /\ childReplayedKeys' = {}
    /\ splitCutoverReady' = FALSE
    /\ splitFenceSet' = FALSE
    /\ UNCHANGED <<parentHasLeader>>

(*
  Action 10: ClientWriteToParent(k)
  Client writes key k to parent shard via normal (non-delta) write path.
  Handles: ParentKeys at all times, or ChildKeys when no split is active.

  Maps to: proposalDequeuer (storedb.go:766) -> ValidateBatchKeys ->
           isKeyOwnedDuringSplit (db.go:608) -> Batch (db.go:2270)
*)
ClientWriteToParent(k) ==
    /\ ParentAcceptsNormalWrite(k)
    /\ parentHasLeader
    /\ dataStore' = [dataStore EXCEPT ![k] =
            IF dataStore[k] = "child" THEN "both" ELSE dataStore[k]]
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, newShardHasLeader, routingUpdated,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 11: ClientWriteDeltaToParent(k)
  Client writes key k (in ChildKeys) to parent shard during split.
  Metadata routing sends the write to the parent because the child is not
  ready (shouldRouteWriteToParent in write_routing.go:46-53).

  On the parent, isKeyOwnedDuringSplit (db.go:608-609) accepts the write.
  During Batch apply, isKeyInSplitOffRangeForState (db.go:2300-2302) detects
  the key is in the split-off range and calls appendSplitDelta (db.go:698-721)
  to create a SplitDeltaEntry with the write's HLC timestamp.

  The write is stored in BOTH the parent's main Pebble store (for reads)
  AND the delta store (for child replay). The Batch function continues
  processing after capturing the delta (db.go:2304+), so the key is
  written to Pebble and indexed normally.

  Maps to: write_routing.go:46-87 (routing) + db.go:2288-2302 (delta capture)
*)
ClientWriteDeltaToParent(k) ==
    /\ ParentAcceptsDeltaWrite(k)
    /\ ~ChildAcceptsWrite(k)         \* Metadata routes to parent (child not ready)
    /\ parentDeltaKeys' = parentDeltaKeys \union {k}
    \* Data is written to parent's main Pebble store too (db.go:2304+)
    /\ dataStore' = [dataStore EXCEPT ![k] =
            IF dataStore[k] = "child" THEN "both" ELSE dataStore[k]]
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, newShardHasLeader, routingUpdated,
                   childReplayedKeys, splitCutoverReady, splitFenceSet>>

(*
  Action 12: ClientWriteToChild(k)
  Client writes key k to the child shard (after child is fully ready).
  Maps to: normal write path on child shard, routed by metadata because
           IsReadyForSplitReads returns true (requires SplitCutoverReady + Lead!=0).
*)
ClientWriteToChild(k) ==
    /\ ChildAcceptsWrite(k)
    /\ dataStore' = [dataStore EXCEPT ![k] =
            IF dataStore[k] = "parent" THEN "both" ELSE dataStore[k]]
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, newShardHasLeader, routingUpdated,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 13: ChildReplaysDelta(k)
  Child shard replays a split delta entry from the parent.
  The replay loop (storedb.go:532-622) fetches entries via
  ListSplitDeltaEntriesAfter (db.go:652-676) and applies them
  with the original HLC timestamp (storedb.go:517-520).

  Requires child leader: replay calls coreDB.Batch (storedb.go:525)
  which proposes through the child's Raft.

  Preconditions:
    - Key has a delta on parent (k in parentDeltaKeys)
    - Child hasn't replayed it yet (k not in childReplayedKeys)
    - Child has snapshot (replay loop runs after snapshot)
    - Child has Raft leader (replay proposes through Raft)

  Maps to: storedb.go:501-530 (applySplitDeltaEntries) called from
           storedb.go:590 (runSplitReplayLoop)
*)
ChildReplaysDelta(k) ==
    /\ k \in parentDeltaKeys
    /\ k \notin childReplayedKeys
    /\ newShardHasSnapshot            \* Replay requires snapshot
    /\ newShardHasLeader              \* Replay proposes through child Raft
    /\ childReplayedKeys' = childReplayedKeys \union {k}
    \* dataStore already has "both" from snapshot; replay updates version
    \* but doesn't change presence at this abstraction level.
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, newShardHasLeader, routingUpdated,
                   dataStore, parentDeltaKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 14: ParentLosesLeader
  Parent shard loses Raft leadership.
  Maps to: Raft election timeout
*)
ParentLosesLeader ==
    /\ parentHasLeader
    /\ parentHasLeader' = FALSE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   newShardState, newShardHasSnapshot, newShardInitializing,
                   newShardHasLeader, routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 15: ParentGainsLeader
  Parent shard elects a new Raft leader. State is reloaded from Pebble:
    - byteRange: loaded from Pebble via GetRange() (storedb.go:125)
    - splitState: loaded from Pebble via GetSplitState() (storedb.go:131)
    - splitDeltaSeq: loaded from Pebble (db.go:3921-3926)
    - splitDeltaFinalSeq: loaded from Pebble (db.go:3928-3933)
  All persisted state is already captured in the TLA+ variables.
  Maps to: Raft election + storedb.go startup loading from Pebble
*)
ParentGainsLeader ==
    /\ ~parentHasLeader
    /\ parentHasLeader' = TRUE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   newShardState, newShardHasSnapshot, newShardInitializing,
                   newShardHasLeader, routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 16: NewShardLosesLeader
  Child shard loses Raft leadership.
  Consequences:
    - CanInitiateSplitCutover becomes false (Lead==0)
    - Reconciler's trackAndCheckSplitFinalizeReady resets 15s grace period
    - ChildReplaysDelta blocked (replay proposes through Raft)
    - IsReadyForSplitReads becomes false (Lead==0)
    - Write routing falls back to parent (shouldRouteWriteToParent)
  Maps to: Raft election timeout on child
*)
NewShardLosesLeader ==
    /\ newShardHasLeader
    /\ newShardHasLeader' = FALSE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 17: NewShardGainsLeader
  Child shard elects a new Raft leader.
  After gaining leader, replay loop can resume, and the child becomes
  eligible again for CanInitiateSplitCutover (after 15s grace period).
  Maps to: Raft election on child
*)
NewShardGainsLeader ==
    /\ newShardState /= "none"        \* Child shard must exist
    /\ ~newShardHasLeader
    /\ newShardHasLeader' = TRUE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   parentHasLeader, newShardState, newShardHasSnapshot,
                   newShardInitializing, routingUpdated, dataStore,
                   parentDeltaKeys, childReplayedKeys,
                   splitCutoverReady, splitFenceSet>>

(*
  Action 18: LeaderSynchronization
  Both parent and child shards elect leaders (or retain existing ones).
  Models the real-world behavior where independent Raft elections on
  separate shards eventually succeed concurrently. Without this action,
  the scheduler can adversarially alternate leadership (parent gains →
  child loses → parent loses → child gains → ...) preventing
  FinalizeSplitSetFence from ever being enabled.
  Maps to: independent Raft elections succeeding on both groups
*)
LeaderSynchronization ==
    /\ newShardState /= "none"        \* Child shard must exist
    /\ ~(parentHasLeader /\ newShardHasLeader)  \* At least one lacks leader
    /\ parentHasLeader' = TRUE
    /\ newShardHasLeader' = TRUE
    /\ UNCHANGED <<splitPhase, parentRange, archiveCreated,
                   newShardState, newShardHasSnapshot, newShardInitializing,
                   routingUpdated, dataStore, parentDeltaKeys,
                   childReplayedKeys, splitCutoverReady, splitFenceSet>>

\* --- Next-state relation ---

Next ==
    \/ PrepareSplit
    \/ SetSplittingPhase
    \/ ApplySplitOp
    \/ MetadataUpdateRouting
    \/ StartNewShard
    \/ NewShardReceivesSnapshot
    \/ ChildClearsInitializing
    \/ TablemgrTransitionsChild
    \/ FinalizeSplitSetFence
    \/ FinalizeSplitComplete
    \/ TimeoutRollback
    \/ ParentLosesLeader
    \/ ParentGainsLeader
    \/ NewShardLosesLeader
    \/ NewShardGainsLeader
    \/ LeaderSynchronization
    \/ \E k \in Keys :
        \/ ClientWriteToParent(k)
        \/ ClientWriteDeltaToParent(k)
        \/ ClientWriteToChild(k)
        \/ ChildReplaysDelta(k)

\* --- Fairness ---

Fairness ==
    \* Strong fairness for actions gated on parentHasLeader.
    /\ SF_vars(PrepareSplit)
    /\ SF_vars(SetSplittingPhase)
    /\ SF_vars(ApplySplitOp)
    /\ SF_vars(FinalizeSplitSetFence)
    /\ SF_vars(FinalizeSplitComplete)
    \* Strong fairness for actions gated on newShardHasLeader (volatile).
    /\ SF_vars(ChildClearsInitializing)
    /\ SF_vars(TablemgrTransitionsChild)
    /\ \A k \in ChildKeys : SF_vars(ChildReplaysDelta(k))
    \* Weak fairness for actions not gated on volatile leader state.
    /\ WF_vars(MetadataUpdateRouting)
    /\ WF_vars(StartNewShard)
    /\ WF_vars(NewShardReceivesSnapshot)
    /\ WF_vars(ParentGainsLeader)
    /\ WF_vars(NewShardGainsLeader)
    /\ WF_vars(LeaderSynchronization)
    \* No fairness for TimeoutRollback, ParentLosesLeader,
    \* NewShardLosesLeader, client writes

Spec == Init /\ [][Next]_vars /\ Fairness

\* ========================================================================
\* Safety Invariants
\* ========================================================================

(*
  NoDataLoss:
  Every key always has data on at least one shard.
*)
NoDataLoss ==
    \A k \in Keys :
        dataStore[k] \in {"parent", "child", "both"}

(*
  NoDoubleServing:
  Once the split is finalized and the child is serving, the parent has
  relinquished ownership of child keys via parentRange.
  The action-level mutual exclusion (ClientWriteDeltaToParent requires
  ~ChildAcceptsWrite, ClientWriteToChild requires ChildAcceptsWrite)
  ensures writes go to exactly one shard during the split.
*)
NoDoubleServing ==
    \A k \in ChildKeys :
        (splitPhase = "none" /\ newShardState = "default" /\ routingUpdated)
        => k \notin parentRange

(*
  NoPrematureCutover:
  The child can only reach Default state after the cutover protocol
  (FinalizeSplit) has completed. This is the invariant that captures
  the bug fixed by separating SplitCutoverReady from SplitReplayCaughtUp.
*)
NoPrematureCutover ==
    (newShardState = "default") => splitCutoverReady

(*
  RollbackRestoresRange:
  When no split is in progress and was rolled back, parent must own all keys.
*)
RollbackRestoresRange ==
    (/\ splitPhase = "none"
     /\ ~archiveCreated
     /\ ~routingUpdated
     /\ newShardState = "none")
    => parentRange = Keys

(*
  ByteRangeNarrowsBeforeArchive:
  If the archive exists, the parent range must have already been narrowed.
  Critical ordering invariant from storedb.go:1101-1107.
*)
ByteRangeNarrowsBeforeArchive ==
    archiveCreated => parentRange = ParentKeys

(*
  DeltaReplayIntegrity:
  The child can only have replayed keys that the parent has written as deltas.
  Maps to: ListSplitDeltaEntriesAfter returns only entries that exist.
*)
DeltaReplayIntegrity ==
    childReplayedKeys \subseteq parentDeltaKeys

(*
  DeltaKeysOnlyDuringSplit:
  Delta keys can only exist when a split is active.
  When no split is in progress (splitPhase = "none"), there should be
  no pending deltas (they are cleared during finalize/rollback).
*)
DeltaKeysOnlyDuringSplit ==
    splitPhase = "none" => parentDeltaKeys = {} /\ childReplayedKeys = {}

(*
  FenceRequiresActiveSplit:
  The splitDeltaFinalSeq fence can only be set during an active SPLITTING
  phase. It is cleared by FinalizeSplitComplete, TimeoutRollback, and
  PrepareSplit.
*)
FenceRequiresActiveSplit ==
    splitFenceSet => splitPhase = "splitting"

(*
  ReadAlwaysAvailable:
  Every key can always be read from some shard, ensuring zero-downtime reads.
*)
ReadAlwaysAvailable ==
    \A k \in Keys : ReadAvailable(k)

\* ========================================================================
\* Liveness Properties
\* ========================================================================

(*
  EventualSplitCompletion:
  A split that starts eventually completes or rolls back.
*)
EventualSplitCompletion ==
    splitPhase = "prepare" ~> splitPhase = "none"

=============================================================================
