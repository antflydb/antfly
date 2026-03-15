----------------------- MODULE AntflySnapshotTransfer -----------------------
(*
  TLA+ Formal Specification of Antfly's Multi-Raft Snapshot Transfer Protocol.

  Models the snapshot lifecycle and transfer mechanism used when a node
  needs to catch up after falling behind or joining the cluster.

  Implementation references:
    - src/raft/raft.go                  (maybeTriggerSnapshot, publishSnapshot,
                                         maybeFetchStorageSnapshot)
    - lib/multirafthttp/transport.go    (GetSnapshot — retry loop with
                                         exponential backoff, peer fallback)
    - lib/multirafthttp/pipeline.go     (getSnap — HTTP GET, error classification)
    - lib/multirafthttp/http.go         (snapHandler — serves snapshot files)
    - src/snapstore/snapstore.go        (SnapStore interface — Get/Put/Delete/Exists)

  Protocol summary:
    Snapshot Creation (leader only):
      1. After SnapshotCatchUpEntriesN (10,000) applied entries, leader calls
         maybeTriggerSnapshot.
      2. createStorageSnapshot produces a tar.zst archive.
      3. Raft snapshot metadata (pointing to archive ID) is persisted.
      4. Raft log is compacted up to appliedIndex - SnapshotCatchUpEntriesN.
      5. Old snapshot archive is deleted (GC).

    Snapshot Transfer (follower/joining node):
      1. Raft sends snapshot message to follower (MsgSnap).
      2. publishSnapshot extracts storage snapshot ID from Raft snapshot data.
      3. snapStore.Exists() checks if archive is already local.
      4. If not local, GetSnapshot retries across peers with exponential backoff.
      5. For each retry: iterate peers, call sendSnapshotRequest (HTTP GET).
      6. If peer has it: streams the archive, snapStore.Put() writes atomically.
      7. If peer returns 404: snapshot was GC'd on that peer.
      8. If ALL peers return 404: permanent failure (no retry).
      9. If some peers have transient errors: retry with backoff.
      10. On success: publishSnapshot signals commitC for application layer.

    Snapshot GC:
      After a new Raft snapshot is created, the old snapshot ID is deleted
      from the local SnapStore. Each node GCs independently — a snapshot
      may exist on some nodes and not others.

  Safety properties:
    - Applied snapshot is valid: a node only applies a snapshot it fully received.
    - No stale application: a snapshot is applied at the correct Raft index.
    - GC safety: a snapshot is not GC'd while it is the current snapshot.

  Liveness properties:
    - Transfer eventually succeeds: if any peer has the snapshot, the joining
      node eventually receives it.
    - Permanent failure detection: if no peer has the snapshot, the joining
      node eventually detects this and stops retrying.

  State space bounding:
    All variables have finite domains. RetryCount is bounded by MaxRetries.

  Boundary:
    Inside the model:
      - Snapshot creation and GC on leader nodes
      - Snapshot transfer pull from peers
      - Retry loop with peer fallback
      - Error categorization (permanent vs transient)
      - Node failure and recovery
      - Leadership changes

    Outside (assumptions):
      - Raft linearizability (snapshot index ordering)
      - Network eventual delivery within retries
      - SnapStore I/O correctness (atomic Put via temp+rename)
      - Archive format correctness (tar.zst)
*)

EXTENDS Naturals, FiniteSets, TLC

\* --- Constants ---

CONSTANTS
    Nodes,          \* Set of node identifiers, e.g. {n1, n2, n3}
    MaxRetries,     \* Maximum retries before giving up (e.g. 10)
    MaxSnapshots    \* Maximum number of snapshots created (bounds state space)

ASSUME Cardinality(Nodes) >= 2
ASSUME MaxRetries \in Nat \ {0}
ASSUME MaxSnapshots \in Nat \ {0}

\* --- Variables ---

VARIABLES
    leader,             \* Node ID of current leader, or "none"
                        \* Maps to: Raft election in src/raft/raft.go

    snapCounter,        \* Nat: monotonically increasing snapshot ID generator
                        \* Abstraction of snapshot-{shardID}-{nodeID}-{appliedIndex}

    persistedSnap,      \* [Nodes -> Nat \union {0}]: the last Raft snapshot ID
                        \* that was fully applied and persisted to Pebble.
                        \* Survives node crashes. 0 means no snapshot.
                        \* Maps to: PebbleStorage.LoadSnapshot() on startup,
                        \*           raftNode.snapshotIndex.Store() in raft.go:886

    targetSnap,         \* [Nodes -> Nat \union {0}]: the snapshot ID the node is
                        \* currently trying to fetch. 0 means not fetching.
                        \* Lost on crash (in-memory state only).
                        \* Maps to: snapshotMeta.GetID() in publishSnapshot (raft.go:719)

    snapStore,          \* [Nodes -> SUBSET Nat]: set of snapshot IDs stored locally
                        \* Maps to: snapstore.SnapStore (files on disk)
                        \* Each node maintains its own store independently.

    needsSnap,          \* SUBSET Nodes: nodes that need to fetch a snapshot.
                        \* Maps to: publishSnapshot detecting need in raft.go:733

    transferState,      \* [Nodes -> {"idle","fetching","done","failed"}]
                        \* Maps to: GetSnapshot retry loop in transport.go:236

    retryCount,         \* [Nodes -> 0..MaxRetries]: current retry attempt
                        \* Maps to: retry.WithMaxRetries(10, b) in transport.go:234

    nodeUp              \* [Nodes -> BOOLEAN]: whether node is running
                        \* Models node crashes and restarts

vars == <<leader, snapCounter, persistedSnap, targetSnap, snapStore,
          needsSnap, transferState, retryCount, nodeUp>>

\* --- Type invariant ---

TypeOK ==
    /\ leader \in Nodes \union {"none"}
    /\ snapCounter \in Nat
    /\ \A n \in Nodes : persistedSnap[n] \in Nat
    /\ \A n \in Nodes : targetSnap[n] \in Nat
    /\ \A n \in Nodes : snapStore[n] \subseteq (1..snapCounter)
    /\ needsSnap \subseteq Nodes
    /\ \A n \in Nodes : transferState[n] \in {"idle","fetching","done","failed"}
    /\ \A n \in Nodes : retryCount[n] \in 0..MaxRetries
    /\ \A n \in Nodes : nodeUp[n] \in BOOLEAN

\* --- Helpers ---

\* Peers that might have a snapshot (other running nodes in the shard).
\* Maps to: transport.go:253-263 (snapshot peers under read lock)
AvailablePeers(n) == {p \in Nodes : p /= n /\ nodeUp[p]}

\* Whether any available peer has snapshot ID sid.
\* Maps to: pipeline.go:156 (HTTP 404 vs 200)
AnyPeerHasSnap(n, sid) ==
    \E p \in AvailablePeers(n) : sid \in snapStore[p]

\* Whether all available peers explicitly lack snapshot ID sid (all 404).
\* Maps to: transport.go:278-284 (allNotFound logic)
AllPeersLackSnap(n, sid) ==
    /\ AvailablePeers(n) /= {}
    /\ \A p \in AvailablePeers(n) : sid \notin snapStore[p]

\* --- Initial state ---

Init ==
    /\ leader = "none"
    /\ snapCounter = 0
    /\ persistedSnap = [n \in Nodes |-> 0]
    /\ targetSnap = [n \in Nodes |-> 0]
    /\ snapStore = [n \in Nodes |-> {}]
    /\ needsSnap = {}
    /\ transferState = [n \in Nodes |-> "idle"]
    /\ retryCount = [n \in Nodes |-> 0]
    /\ nodeUp = [n \in Nodes |-> TRUE]

\* --- Actions ---

(*
  Action 1: ElectLeader
  A node becomes the Raft leader (election completes).
  Maps to: Raft election in src/raft/raft.go
*)
ElectLeader(n) ==
    /\ nodeUp[n]
    /\ leader' = n
    /\ UNCHANGED <<snapCounter, persistedSnap, targetSnap, snapStore, needsSnap,
                   transferState, retryCount, nodeUp>>

(*
  Action 2: CreateSnapshot
  Leader triggers snapshot creation after enough entries are applied.
  Steps (raft.go:773-900):
    1. Generate new snapshot ID (snapCounter + 1)
    2. Create storage archive (createStorageSnapshot)
    3. Persist Raft snapshot metadata
    4. Compact Raft log
    5. GC old snapshot (delete oldSnapshotID from local store)

  The new snapshot replaces the old one on the leader.
  GC is modeled atomically with creation (in reality, GC is the last
  step of maybeTriggerSnapshot, line 890-899).
*)
CreateSnapshot(n) ==
    /\ n = leader
    /\ nodeUp[n]
    /\ transferState[n] /= "done"   \* in real system, maybeTriggerSnapshot
                                     \* runs after snapshot application in
                                     \* the same Ready loop iteration
    /\ snapCounter < MaxSnapshots   \* bound state space
    /\ LET newID == snapCounter + 1
           oldID == persistedSnap[n]
       IN
       /\ snapCounter' = newID
       /\ persistedSnap' = [persistedSnap EXCEPT ![n] = newID]
       \* Add new snapshot, remove old one (GC)
       /\ snapStore' = [snapStore EXCEPT ![n] =
              (@ \union {newID}) \ (IF oldID > 0 THEN {oldID} ELSE {})]
       /\ UNCHANGED <<leader, targetSnap, needsSnap, transferState, retryCount, nodeUp>>

(*
  Action 3: RaftSendsSnapshot
  Raft decides a follower needs a snapshot (follower is too far behind,
  or a new node is joining). The leader's current snapshot ID is sent.
  Maps to: Raft MsgSnap in the ready loop, triggering publishSnapshot.
  The follower's targetSnap is set to the leader's persistedSnap.
*)
RaftSendsSnapshot(n) ==
    /\ leader /= "none"
    /\ nodeUp[n]
    /\ n /= leader
    /\ persistedSnap[leader] > 0     \* leader has a snapshot to send
    /\ persistedSnap[leader] > persistedSnap[n]  \* follower is behind leader
    /\ n \notin needsSnap             \* not already fetching
    /\ transferState[n] = "idle"
    \* Record which snapshot the follower needs (in-memory target)
    /\ targetSnap' = [targetSnap EXCEPT ![n] = persistedSnap[leader]]
    /\ needsSnap' = needsSnap \union {n}
    /\ transferState' = [transferState EXCEPT ![n] = "fetching"]
    /\ retryCount' = [retryCount EXCEPT ![n] = 0]
    /\ UNCHANGED <<leader, snapCounter, persistedSnap, snapStore, nodeUp>>

(*
  Action 4: TransferSucceeds
  A fetching node successfully downloads the snapshot from a peer.
  The snapshot exists on at least one peer and is now stored locally.
  Maps to: transport.go:274 (sendSnapshotRequest succeeds) +
           pipeline.go:177 (snapStore.Put)
*)
TransferSucceeds(n) ==
    /\ transferState[n] = "fetching"
    /\ nodeUp[n]
    /\ targetSnap[n] > 0
    \* Check if already local (snapStore.Exists, transport.go:240)
    /\ \/ targetSnap[n] \in snapStore[n]
       \* Or a peer has it
       \/ AnyPeerHasSnap(n, targetSnap[n])
    /\ snapStore' = [snapStore EXCEPT ![n] = @ \union {targetSnap[n]}]
    /\ transferState' = [transferState EXCEPT ![n] = "done"]
    /\ needsSnap' = needsSnap \ {n}
    /\ UNCHANGED <<leader, snapCounter, persistedSnap, targetSnap, retryCount, nodeUp>>

(*
  Action 5: TransferRetry
  All peers fail with transient errors (not all 404). Increment retry count.
  Maps to: transport.go:283 (retry.RetryableError wrapping) + exponential backoff
*)
TransferRetry(n) ==
    /\ transferState[n] = "fetching"
    /\ nodeUp[n]
    /\ targetSnap[n] > 0
    \* Snapshot not local
    /\ targetSnap[n] \notin snapStore[n]
    \* Not all peers return "not found" — some have transient errors.
    \* This means either no peers are available, or at least one peer
    \* is down (transient) rather than all returning 404 (permanent).
    /\ ~AllPeersLackSnap(n, targetSnap[n])
    /\ retryCount[n] < MaxRetries
    /\ retryCount' = [retryCount EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<leader, snapCounter, persistedSnap, targetSnap, snapStore,
                   needsSnap, transferState, nodeUp>>

(*
  Action 6: TransferPermanentFailure
  All available peers return 404 — snapshot has been GC'd everywhere.
  Maps to: transport.go:278-281 (allNotFound check, returns non-retryable error)
  The node gives up immediately without exhausting retries.
*)
TransferPermanentFailure(n) ==
    /\ transferState[n] = "fetching"
    /\ nodeUp[n]
    /\ targetSnap[n] > 0
    /\ targetSnap[n] \notin snapStore[n]
    /\ AllPeersLackSnap(n, targetSnap[n])
    /\ transferState' = [transferState EXCEPT ![n] = "failed"]
    /\ needsSnap' = needsSnap \ {n}
    /\ UNCHANGED <<leader, snapCounter, persistedSnap, targetSnap, snapStore,
                   retryCount, nodeUp>>

(*
  Action 7: TransferExhaustedRetries
  Retry count reaches MaxRetries with transient errors. Give up.
  Maps to: retry.WithMaxRetries(10, b) in transport.go:234 causing
           retry.Do to return the last error.
  In raft.go:739 this triggers logger.Fatal (node crash).
*)
TransferExhaustedRetries(n) ==
    /\ transferState[n] = "fetching"
    /\ nodeUp[n]
    /\ retryCount[n] >= MaxRetries
    /\ targetSnap[n] \notin snapStore[n]
    /\ transferState' = [transferState EXCEPT ![n] = "failed"]
    /\ needsSnap' = needsSnap \ {n}
    /\ UNCHANGED <<leader, snapCounter, persistedSnap, targetSnap, snapStore,
                   retryCount, nodeUp>>

(*
  Action 8: ApplySnapshot
  A node that has successfully fetched a snapshot applies it.
  Persists the snapshot index to Pebble (survives future crashes).
  Maps to: publishSnapshot signaling commitC (raft.go:748-753) ->
           StoreDB.LoadSnapshot decompressing archive +
           raft.go:762 snapshotIndex.Store()
*)
ApplySnapshot(n) ==
    /\ transferState[n] = "done"
    /\ nodeUp[n]
    /\ targetSnap[n] \in snapStore[n]
    /\ persistedSnap' = [persistedSnap EXCEPT ![n] = targetSnap[n]]
    /\ targetSnap' = [targetSnap EXCEPT ![n] = 0]
    /\ transferState' = [transferState EXCEPT ![n] = "idle"]
    /\ UNCHANGED <<leader, snapCounter, snapStore,
                   needsSnap, retryCount, nodeUp>>

(*
  Action 9: NodeCrash
  A node crashes (power failure, OOM, Fatal from failed transfer, etc).
  In-flight transfer state is lost (in-memory). persistedSnap and snapStore
  survive (persisted to Pebble/disk).
  Maps to: various Fatal paths in raft.go
*)
NodeCrash(n) ==
    /\ nodeUp[n]
    /\ nodeUp' = [nodeUp EXCEPT ![n] = FALSE]
    \* Leader lost if this node was leader
    /\ leader' = IF leader = n THEN "none" ELSE leader
    \* In-flight target is lost (in-memory only)
    /\ targetSnap' = [targetSnap EXCEPT ![n] = 0]
    \* Abort any in-flight transfer
    /\ transferState' = [transferState EXCEPT ![n] = "idle"]
    /\ needsSnap' = needsSnap \ {n}
    /\ retryCount' = [retryCount EXCEPT ![n] = 0]
    \* persistedSnap and snapStore survive (on-disk)
    /\ UNCHANGED <<snapCounter, persistedSnap, snapStore>>

(*
  Action 10: NodeRestart
  A crashed node restarts. Its persistedSnap and snapStore are intact
  (loaded from Pebble/disk). Raft state is reloaded from PebbleStorage.
  Maps to: node startup, Raft state loaded from Pebble (raft.go:255-257)
*)
NodeRestart(n) ==
    /\ ~nodeUp[n]
    /\ nodeUp' = [nodeUp EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<leader, snapCounter, persistedSnap, targetSnap, snapStore,
                   needsSnap, transferState, retryCount>>

\* --- Next-state relation ---

Next ==
    \/ \E n \in Nodes :
        \/ ElectLeader(n)
        \/ CreateSnapshot(n)
        \/ RaftSendsSnapshot(n)
        \/ TransferSucceeds(n)
        \/ TransferRetry(n)
        \/ TransferPermanentFailure(n)
        \/ TransferExhaustedRetries(n)
        \/ ApplySnapshot(n)
        \/ NodeCrash(n)
        \/ NodeRestart(n)

\* --- Fairness ---

Fairness ==
    \* --- Strong fairness (SF) for actions intermittently disabled by crashes ---
    \* Peer crashes and restarts make transfer-related actions toggle between
    \* enabled and disabled. WF only guarantees firing for *continuously*
    \* enabled actions. SF ensures an action fires if it is *repeatedly*
    \* enabled, even if disabled in between.
    \*
    \* TransferSucceeds: enabled when a peer with the snapshot is up, disabled
    \* when that peer crashes. SF reflects the real system's retry loop
    \* eventually hitting a window where the peer is available.
    /\ \A n \in Nodes : SF_vars(TransferSucceeds(n))
    \* TransferPermanentFailure: enabled when peers are up and all lack the
    \* snapshot (404), disabled when peers crash. SF ensures that if all
    \* available peers repeatedly confirm the snapshot is gone, the node
    \* eventually detects this as permanent.
    /\ \A n \in Nodes : SF_vars(TransferPermanentFailure(n))
    \* TransferRetry: enabled when no peers are available (transient — can't
    \* tell if it's permanent), disabled when peers come up. SF ensures the
    \* retry counter advances even with intermittent peer availability.
    /\ \A n \in Nodes : SF_vars(TransferRetry(n))
    \* --- Weak fairness (WF) for continuously-enabled actions ---
    \* These actions, once enabled, stay enabled until they fire.
    /\ \A n \in Nodes : WF_vars(ApplySnapshot(n))
    /\ \A n \in Nodes : WF_vars(TransferExhaustedRetries(n))
    \* --- Strong fairness for leader election and node restart ---
    /\ \A n \in Nodes : SF_vars(ElectLeader(n))
    /\ \A n \in Nodes : WF_vars(NodeRestart(n))
    \* No fairness for CreateSnapshot (leader may or may not snapshot)
    \* No fairness for RaftSendsSnapshot (Raft decides when)
    \* No fairness for NodeCrash (failure action, nondeterministic)

Spec == Init /\ [][Next]_vars /\ Fairness

\* ========================================================================
\* Safety Invariants
\* ========================================================================

(*
  AppliedSnapshotIsValid:
  A node only applies a snapshot (transitions to "idle" from "done") if
  the snapshot exists in its local store. This ensures no partial or
  corrupted snapshot is applied.
  Maps to: snapStore.Exists() check (transport.go:240) and Put success
  before signaling commitC.
*)
AppliedSnapshotIsValid ==
    \A n \in Nodes :
        transferState[n] = "done" => targetSnap[n] \in snapStore[n]

(*
  GCSafety:
  A node's persisted snapshot ID is always in its local store, unless it
  hasn't created/received one yet (persistedSnap = 0).
  Maps to: maybeTriggerSnapshot creates new before GCing old (raft.go:890)
  Unlike targetSnap (in-flight, may not be stored yet), persistedSnap
  has been fully applied and its archive is on disk.
*)
GCSafety ==
    \A n \in Nodes :
        (persistedSnap[n] > 0 /\ nodeUp[n])
        => persistedSnap[n] \in snapStore[n]

(*
  RetryBound:
  No node exceeds MaxRetries. This is the fundamental guarantee that
  the retry loop terminates.
  Maps to: retry.WithMaxRetries(10, b) in transport.go:234
*)
RetryBound ==
    \A n \in Nodes : retryCount[n] <= MaxRetries

(*
  NoFetchingWithoutNeed:
  A node in "fetching" state is always in needsSnap.
  Consistency between transfer state and needs tracking.
*)
NoFetchingWithoutNeed ==
    \A n \in Nodes :
        transferState[n] = "fetching" => n \in needsSnap

(*
  SnapshotIDsMonotonic:
  Snapshot IDs are always <= snapCounter. No invalid IDs exist.
*)
SnapshotIDsMonotonic ==
    \A n \in Nodes :
        /\ persistedSnap[n] <= snapCounter
        /\ targetSnap[n] <= snapCounter
        /\ \A sid \in snapStore[n] : sid <= snapCounter

\* ========================================================================
\* Liveness Properties
\* ========================================================================

(*
  EventualTransferResolution:
  A node that needs a snapshot eventually either succeeds or fails.
  It does not stay in "fetching" forever.
  Maps to: GetSnapshot returns (either success or error after retries)
           + publishSnapshot handles the result (raft.go:733-743)
*)
EventualTransferResolution ==
    \A n \in Nodes :
        (transferState[n] = "fetching") ~> (transferState[n] \in {"idle", "failed"})

(*
  EventualPermanentDetection:
  If a snapshot has been GC'd on all peers, the fetching node
  eventually stops fetching — either by detecting the permanent
  failure (transition to "failed") or by crashing (transition to
  "idle", after which Raft will send a newer snapshot on restart).
  Maps to: allNotFound check in transport.go:278-281
*)
EventualPermanentDetection ==
    \A n \in Nodes :
        (transferState[n] = "fetching" /\ AllPeersLackSnap(n, targetSnap[n]))
        ~> (transferState[n] \in {"failed", "idle"})

=============================================================================
