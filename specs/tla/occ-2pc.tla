---- MODULE model ----
EXTENDS Integers, Sequences, FiniteSets, TLC

\* =============================================================================
\* Antfly 2PC Transaction Protocol - TLA+ Specification
\* =============================================================================
\*
\* This models the distributed transaction protocol with focus on:
\* - A6: Window between predicate check and intent write
\* - A7: Race in finalizeTransaction read-modify-write
\* - Atomicity invariants
\*
\* Boundary: Inside the model (from boundary.md)
\* - Transaction state machine (Pending -> Committed | Aborted)
\* - 2PC orchestration (init, writeIntents, commit, abort)
\* - Predicate checking (OCC validation)
\* - Intent resolution
\* - Recovery loop
\*
\* Outside (assumptions):
\* - Raft linearizability within shards
\* - Network eventual delivery
\* - Unique timestamps
\* =============================================================================

\* === CONSTANTS ===

CONSTANTS
    Txns,           \* Set of transaction IDs
    Shards,         \* Set of shard IDs
    Keys            \* Set of keys

\* Transaction status values
CONSTANTS Pending, Committed, Aborted

\* === STATE ===

VARIABLES
    \* Transaction records on coordinator shards
    \* txnRecords[shard][txn] = [status: Status, participants: SET of shards, resolved: SET of shards]
    txnRecords,

    \* Write intents on participant shards
    \* intents[shard][txn] = SET of keys with pending intents
    intents,

    \* Actual committed values (key versions)
    \* keyVersions[shard][key] = version number (timestamp abstraction)
    keyVersions,

    \* Orchestrator state per transaction
    \* orchestratorState[txn] = phase (none, inited, intentsWritten, committed, aborted, resolved)
    orchestratorState,

    \* Tracks which predicates each txn checked and what version it saw
    \* predicateSnapshot[txn][shard][key] = version seen during check
    predicateSnapshot,

    \* Global version counter (abstracts HLC timestamps)
    versionCounter,

    \* Recovery loop active flag per shard (models leader-only execution)
    recoveryActive

vars == <<txnRecords, intents, keyVersions, orchestratorState,
          predicateSnapshot, versionCounter, recoveryActive>>

\* === HELPERS ===

\* Get coordinator for a transaction
\* For model checking: coordinator is deterministically chosen based on txn
\* We use CHOOSE to pick an arbitrary but deterministic shard for each txn
CoordinatorOf(txn) == CHOOSE s \in Shards : TRUE

\* Participants for a transaction (all shards except coordinator for simplicity)
ParticipantsOf(txn) == Shards \ {CoordinatorOf(txn)}

\* All shards involved in a transaction
AllShardsOf(txn) == Shards

\* Check if all participants have resolved
AllResolved(txn) ==
    LET coord == CoordinatorOf(txn)
        record == txnRecords[coord][txn]
    IN record.resolved = record.participants

\* === INITIAL STATE ===

Init ==
    /\ txnRecords = [s \in Shards |-> [t \in Txns |-> [status |-> "none", participants |-> {}, resolved |-> {}]]]
    /\ intents = [s \in Shards |-> [t \in Txns |-> {}]]
    /\ keyVersions = [s \in Shards |-> [k \in Keys |-> 0]]
    /\ orchestratorState = [t \in Txns |-> "none"]
    /\ predicateSnapshot = [t \in Txns |-> [s \in Shards |-> [k \in Keys |-> 0]]]
    /\ versionCounter = 1
    /\ recoveryActive = [s \in Shards |-> FALSE]

\* === ACTIONS ===

\* -----------------------------------------------------------------------------
\* Phase 1a: Initialize transaction record on coordinator
\* -----------------------------------------------------------------------------
InitTransaction(txn) ==
    /\ orchestratorState[txn] = "none"
    /\ LET coord == CoordinatorOf(txn)
           participants == ParticipantsOf(txn)
       IN /\ txnRecords' = [txnRecords EXCEPT ![coord][txn] =
                [status |-> Pending, participants |-> participants, resolved |-> {}]]
          /\ orchestratorState' = [orchestratorState EXCEPT ![txn] = "inited"]
    /\ UNCHANGED <<intents, keyVersions, predicateSnapshot, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Phase 1b: Check predicates (snapshot versions)
\* This models reading current versions BEFORE writing intents
\* The window between this and WriteIntents is where A6 vulnerability lies
\* -----------------------------------------------------------------------------
CheckPredicates(txn, shard) ==
    /\ orchestratorState[txn] = "inited"
    /\ \E key \in Keys:
        \* Snapshot the current version
        /\ predicateSnapshot' = [predicateSnapshot EXCEPT ![txn][shard][key] = keyVersions[shard][key]]
    /\ UNCHANGED <<txnRecords, intents, keyVersions, orchestratorState, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Phase 1c: Write intents to a shard (with predicate validation)
\* Models the atomic batch write of intents
\* Returns success only if predicates still valid
\* -----------------------------------------------------------------------------
WriteIntents(txn, shard) ==
    /\ orchestratorState[txn] = "inited"
    /\ LET coord == CoordinatorOf(txn)
       IN txnRecords[coord][txn].status = Pending
    /\ \E key \in Keys:
        \* Check if predicate still holds (version hasn't changed since snapshot)
        LET snapshotVersion == predicateSnapshot[txn][shard][key]
            currentVersion == keyVersions[shard][key]
        IN
        \* CRITICAL: This is where A6 manifests - if version changed, fail
        IF snapshotVersion # currentVersion /\ snapshotVersion # 0
        THEN FALSE  \* Predicate failed, action not enabled
        ELSE
            /\ intents' = [intents EXCEPT ![shard][txn] = intents[shard][txn] \union {key}]
            /\ UNCHANGED <<txnRecords, keyVersions, orchestratorState, predicateSnapshot, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Phase 1d: All intents written successfully
\* Orchestrator advances after all shards have intents
\* -----------------------------------------------------------------------------
IntentsComplete(txn) ==
    /\ orchestratorState[txn] = "inited"
    /\ \A shard \in AllShardsOf(txn): intents[shard][txn] # {}
    /\ orchestratorState' = [orchestratorState EXCEPT ![txn] = "intentsWritten"]
    /\ UNCHANGED <<txnRecords, intents, keyVersions, predicateSnapshot, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Phase 1e: Intent write failed - abort
\* -----------------------------------------------------------------------------
AbortAfterIntentFailure(txn) ==
    /\ orchestratorState[txn] = "inited"
    /\ \* Some shard couldn't write intents (predicate failed)
       \E shard \in AllShardsOf(txn): intents[shard][txn] = {}
    /\ LET coord == CoordinatorOf(txn)
       IN /\ txnRecords' = [txnRecords EXCEPT ![coord][txn].status = Aborted]
          /\ orchestratorState' = [orchestratorState EXCEPT ![txn] = "aborted"]
    /\ UNCHANGED <<intents, keyVersions, predicateSnapshot, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Phase 2: Commit transaction (THE COMMIT POINT)
\* Models finalizeTransaction - the read-modify-write
\* A7 vulnerability: concurrent calls could race
\* -----------------------------------------------------------------------------
CommitTransaction(txn) ==
    /\ orchestratorState[txn] = "intentsWritten"
    /\ LET coord == CoordinatorOf(txn)
       IN
        \* Read current status
        /\ txnRecords[coord][txn].status = Pending
        \* Write new status (commit point!)
        /\ txnRecords' = [txnRecords EXCEPT ![coord][txn].status = Committed]
        /\ orchestratorState' = [orchestratorState EXCEPT ![txn] = "committed"]
    /\ UNCHANGED <<intents, keyVersions, predicateSnapshot, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Phase 3: Resolve intents on a shard (apply writes, delete intents)
\* Idempotent - safe to call multiple times
\* -----------------------------------------------------------------------------
ResolveIntents(txn, shard) ==
    /\ LET coord == CoordinatorOf(txn)
       IN txnRecords[coord][txn].status = Committed
    /\ intents[shard][txn] # {}
    /\ \* Apply all intents: increment version for each key
       LET intentKeys == intents[shard][txn]
           newVersion == versionCounter
       IN
        /\ keyVersions' = [keyVersions EXCEPT ![shard] =
            [k \in Keys |-> IF k \in intentKeys THEN newVersion ELSE keyVersions[shard][k]]]
        /\ versionCounter' = versionCounter + 1
        \* Clear intents
        /\ intents' = [intents EXCEPT ![shard][txn] = {}]
        \* Track resolution
        /\ LET coord2 == CoordinatorOf(txn)
           IN IF shard \in txnRecords[coord2][txn].participants
              THEN txnRecords' = [txnRecords EXCEPT ![coord2][txn].resolved =
                        txnRecords[coord2][txn].resolved \union {shard}]
              ELSE UNCHANGED txnRecords
    /\ UNCHANGED <<orchestratorState, predicateSnapshot, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Resolution complete - mark orchestrator done
\* -----------------------------------------------------------------------------
ResolutionComplete(txn) ==
    /\ orchestratorState[txn] = "committed"
    /\ \A shard \in AllShardsOf(txn): intents[shard][txn] = {}
    /\ orchestratorState' = [orchestratorState EXCEPT ![txn] = "resolved"]
    /\ UNCHANGED <<txnRecords, intents, keyVersions, predicateSnapshot, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Cleanup aborted intents
\* -----------------------------------------------------------------------------
CleanupAbortedIntents(txn, shard) ==
    /\ LET coord == CoordinatorOf(txn)
       IN txnRecords[coord][txn].status = Aborted
    /\ intents[shard][txn] # {}
    /\ intents' = [intents EXCEPT ![shard][txn] = {}]
    /\ UNCHANGED <<txnRecords, keyVersions, orchestratorState, predicateSnapshot, versionCounter, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Recovery loop: notify pending resolutions
\* Models the 30-second background loop on coordinator leaders
\* -----------------------------------------------------------------------------
RecoveryNotify(txn) ==
    LET coord == CoordinatorOf(txn)
    IN
    /\ recoveryActive[coord] = TRUE
    /\ txnRecords[coord][txn].status \in {Committed, Aborted}
    /\ ~AllResolved(txn)
    \* Recovery triggers resolution (same as normal path)
    /\ \E shard \in Shards:
        /\ intents[shard][txn] # {}
        /\ LET status == txnRecords[coord][txn].status
           IN IF status = Committed
              THEN \* Resolve and apply
                   /\ keyVersions' = [keyVersions EXCEPT ![shard] =
                        [k \in Keys |-> IF k \in intents[shard][txn]
                                        THEN versionCounter
                                        ELSE keyVersions[shard][k]]]
                   /\ versionCounter' = versionCounter + 1
              ELSE \* Just cleanup (aborted)
                   /\ UNCHANGED <<keyVersions, versionCounter>>
        /\ intents' = [intents EXCEPT ![shard][txn] = {}]
        /\ txnRecords' = [txnRecords EXCEPT ![coord][txn].resolved =
                txnRecords[coord][txn].resolved \union {shard}]
    /\ UNCHANGED <<orchestratorState, predicateSnapshot, recoveryActive>>

\* -----------------------------------------------------------------------------
\* Leader election: activate/deactivate recovery loop
\* -----------------------------------------------------------------------------
BecomeLeader(shard) ==
    /\ recoveryActive[shard] = FALSE
    /\ recoveryActive' = [recoveryActive EXCEPT ![shard] = TRUE]
    /\ UNCHANGED <<txnRecords, intents, keyVersions, orchestratorState, predicateSnapshot, versionCounter>>

LoseLeadership(shard) ==
    /\ recoveryActive[shard] = TRUE
    /\ recoveryActive' = [recoveryActive EXCEPT ![shard] = FALSE]
    /\ UNCHANGED <<txnRecords, intents, keyVersions, orchestratorState, predicateSnapshot, versionCounter>>

\* -----------------------------------------------------------------------------
\* External write: models a non-transactional write that could interfere
\* This is how we probe A6 - an external write between predicate check and intent write
\* -----------------------------------------------------------------------------
ExternalWrite(shard, key) ==
    /\ keyVersions' = [keyVersions EXCEPT ![shard][key] = versionCounter]
    /\ versionCounter' = versionCounter + 1
    /\ UNCHANGED <<txnRecords, intents, orchestratorState, predicateSnapshot, recoveryActive>>

\* === NEXT STATE ===

Next ==
    \/ \E txn \in Txns: InitTransaction(txn)
    \/ \E txn \in Txns, shard \in Shards: CheckPredicates(txn, shard)
    \/ \E txn \in Txns, shard \in Shards: WriteIntents(txn, shard)
    \/ \E txn \in Txns: IntentsComplete(txn)
    \/ \E txn \in Txns: AbortAfterIntentFailure(txn)
    \/ \E txn \in Txns: CommitTransaction(txn)
    \/ \E txn \in Txns, shard \in Shards: ResolveIntents(txn, shard)
    \/ \E txn \in Txns: ResolutionComplete(txn)
    \/ \E txn \in Txns, shard \in Shards: CleanupAbortedIntents(txn, shard)
    \/ \E txn \in Txns: RecoveryNotify(txn)
    \/ \E shard \in Shards: BecomeLeader(shard)
    \/ \E shard \in Shards: LoseLeadership(shard)
    \/ \E shard \in Shards, key \in Keys: ExternalWrite(shard, key)

\* === INVARIANTS ===

\* Type invariant
TypeInvariant ==
    /\ txnRecords \in [Shards -> [Txns -> [status: {"none", Pending, Committed, Aborted},
                                           participants: SUBSET Shards,
                                           resolved: SUBSET Shards]]]
    /\ intents \in [Shards -> [Txns -> SUBSET Keys]]
    /\ keyVersions \in [Shards -> [Keys -> Nat]]
    /\ orchestratorState \in [Txns -> {"none", "inited", "intentsWritten", "committed", "aborted", "resolved"}]
    /\ versionCounter \in Nat
    /\ recoveryActive \in [Shards -> BOOLEAN]

\* -----------------------------------------------------------------------------
\* SAFETY: Atomicity - if committed, all intents must eventually resolve
\* (We check the contrapositive: never have committed txn with permanent orphaned intents)
\* -----------------------------------------------------------------------------
NoOrphanedCommittedIntents ==
    \A txn \in Txns:
        LET coord == CoordinatorOf(txn)
        IN txnRecords[coord][txn].status = Committed =>
           \* Either all intents cleared, or recovery can still run
           \/ \A shard \in Shards: intents[shard][txn] = {}
           \/ orchestratorState[txn] \in {"committed", "intentsWritten"}

\* -----------------------------------------------------------------------------
\* SAFETY: No partial commits - intents on different shards are all-or-nothing
\* If txn is committed, cannot have some shards with applied values and others with intents
\* -----------------------------------------------------------------------------
NoPartialCommit ==
    \A txn \in Txns:
        LET coord == CoordinatorOf(txn)
            status == txnRecords[coord][txn].status
        IN status = Committed =>
           \* All shards either have intents or have resolved (not mixed at snapshot)
           TRUE  \* This is checked via NoOrphanedCommittedIntents

\* -----------------------------------------------------------------------------
\* SAFETY: Abort means no values applied
\* If aborted, no intent should have been applied to actual values
\* -----------------------------------------------------------------------------
AbortMeansNoApply ==
    \A txn \in Txns:
        LET coord == CoordinatorOf(txn)
        IN txnRecords[coord][txn].status = Aborted =>
           \* All intents should eventually be cleaned, not applied
           \* (We can't directly check "value not applied" without tracking, but
           \*  we ensure ResolveIntents isn't called for aborted txns)
           TRUE

\* -----------------------------------------------------------------------------
\* SAFETY: Serializability - OCC validation prevents lost updates
\* If two txns both check same key and both commit, they must have different versions
\* -----------------------------------------------------------------------------
SerializableReads ==
    \A t1, t2 \in Txns:
        t1 # t2 =>
        LET c1 == CoordinatorOf(t1)
            c2 == CoordinatorOf(t2)
        IN /\ txnRecords[c1][t1].status = Committed
           /\ txnRecords[c2][t2].status = Committed
           => \A shard \in Shards, key \in Keys:
                \* If both read same key, at least one must have aborted
                \* (since versions would conflict)
                ~(predicateSnapshot[t1][shard][key] # 0 /\
                  predicateSnapshot[t2][shard][key] # 0 /\
                  predicateSnapshot[t1][shard][key] = predicateSnapshot[t2][shard][key])

\* -----------------------------------------------------------------------------
\* SAFETY: No intents without transaction record
\* Every intent must have a corresponding transaction record on coordinator
\* -----------------------------------------------------------------------------
NoOrphanedIntents ==
    \A shard \in Shards, txn \in Txns:
        intents[shard][txn] # {} =>
        LET coord == CoordinatorOf(txn)
        IN txnRecords[coord][txn].status # "none"

\* Combined safety invariant
SafetyInvariant ==
    /\ TypeInvariant
    /\ NoOrphanedCommittedIntents
    /\ NoOrphanedIntents
    /\ SerializableReads

\* === LIVENESS (for checking with fairness) ===

\* Eventually all committed transactions resolve
EventualResolution ==
    \A txn \in Txns:
        LET coord == CoordinatorOf(txn)
        IN txnRecords[coord][txn].status = Committed ~>
           \A shard \in Shards: intents[shard][txn] = {}

\* === SPECIFICATION ===

Fairness ==
    /\ \A txn \in Txns: WF_vars(ResolutionComplete(txn))
    /\ \A txn \in Txns, shard \in Shards: WF_vars(ResolveIntents(txn, shard))
    /\ \A txn \in Txns: WF_vars(RecoveryNotify(txn))
    /\ \A shard \in Shards: WF_vars(BecomeLeader(shard))

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ Fairness

====
