------------------------------ MODULE AntflyLsmLifecycle ------------------------------
(*
  Allocation-failure lifecycle model for the migrated Zig LSM runtime.

  This model is intentionally scoped to the concrete ownership handoff bugs from
  the OOM retirement investigation, as they exist under zig/pkg/antfly/src in
  the main Antfly repository:

    1. ProvisionedTableReadCache / ProvisionedTableWriteCache entries.
       Relevant Zig shape:
         - entries: live owner
         - retired_entries: cleanup owner while active leases exist
         - get/open/adopt reserves retired_entries capacity before publishing a
           live Entry
         - retirement call sites unlink from entries, then call
           retireEntryLocked, whose appendAssumeCapacity must be infallible

    2. storage/lsm_backend mutable_read_snapshot.
       Relevant Zig shape:
         - mutable_read_snapshot: current owner used by readers
         - retired_mutable_snapshots: cleanup owner while active_readers > 0
         - snapshotMutableState reserves retired_mutable_snapshots capacity
           before publishing mutable_read_snapshot
         - invalidateMutableReadSnapshot is triggered by normal mutable
           rotation/flush and by direct bulk-ingest finish

    3. IndexWriter.removeSegments temporary SegmentEntry arrays.
       Relevant Zig shape:
         - new_segments is allocated before retired
         - failures after either allocation must free temporary arrays before
           returning error

  The model represents one distinguished cache entry, one distinguished mutable
  snapshot, and one removeSegments operation. That is enough for the safety bugs:
  each prior bug has a one-resource counterexample. Capacity is modeled as a
  safety precondition: a resource may be published only after one cleanup-slot
  reserve has succeeded.
*)

EXTENDS Naturals, TLC

CONSTANTS
    ReadCache,
    WriteCache

Caches == {ReadCache, WriteCache}

CacheLocs == {"Absent", "Live", "Retired", "Destroyed"}
SnapshotLocs == {"NoSnapshot", "MutableOwner", "RetiredOwner", "Destroyed"}
IndexTemps == {"None", "NewOnly", "BothAllocated", "Published", "Freed", "Leaked"}

VARIABLES
    cacheLoc,
    cacheLeases,
    cacheRetiredCap,
    snapshotLoc,
    activeReaders,
    snapshotRetiredCap,
    indexTemp,
    indexOpFailed

vars == <<
    cacheLoc,
    cacheLeases,
    cacheRetiredCap,
    snapshotLoc,
    activeReaders,
    snapshotRetiredCap,
    indexTemp,
    indexOpFailed
>>

Init ==
    /\ cacheLoc = [c \in Caches |-> "Absent"]
    /\ cacheLeases = [c \in Caches |-> 0]
    /\ cacheRetiredCap = [c \in Caches |-> 0]
    /\ snapshotLoc = "NoSnapshot"
    /\ activeReaders = 0
    /\ snapshotRetiredCap = 0
    /\ indexTemp = "None"
    /\ indexOpFailed = FALSE

\* --------------------------------------------------------------------------
\* Provisioned read/write cache entry lifecycle
\* --------------------------------------------------------------------------

\* OOM while reserving cleanup bookkeeping for a new entry. The opened DB and
\* staged allocations are cleaned by Zig errdefer paths before the entry becomes
\* reachable from entries, so no modeled resource is published.
CacheOpenReserveFails(c) ==
    /\ c \in Caches
    /\ cacheLoc[c] = "Absent"
    /\ UNCHANGED vars

\* Successful get/open/adopt path. This abstracts:
\*   try retired_entries.ensureUnusedCapacity(self.alloc, 1);
\*   try alloc.create(Entry);
\*   try entries.append(...);
\* The capacity reserve is retained as a cleanup slot for later retirement.
CacheOpenSucceeds(c) ==
    /\ c \in Caches
    /\ cacheLoc[c] = "Absent"
    /\ cacheLoc' = [cacheLoc EXCEPT ![c] = "Live"]
    /\ cacheLeases' = [cacheLeases EXCEPT ![c] = 1]
    /\ cacheRetiredCap' = [cacheRetiredCap EXCEPT ![c] = 1]
    /\ UNCHANGED <<snapshotLoc, activeReaders, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* A live lease is released before invalidation/eviction/prune. A later retire
\* may destroy without appending to retired_entries.
CacheReleaseLiveLease(c) ==
    /\ c \in Caches
    /\ cacheLoc[c] = "Live"
    /\ cacheLeases[c] = 1
    /\ cacheLeases' = [cacheLeases EXCEPT ![c] = 0]
    /\ UNCHANGED <<cacheLoc, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* Unlink from entries followed by retireEntryLocked with no active lease:
\* deinit/destroy is allocation-free.
CacheRetireInactive(c) ==
    /\ c \in Caches
    /\ cacheLoc[c] = "Live"
    /\ cacheLeases[c] = 0
    /\ cacheLoc' = [cacheLoc EXCEPT ![c] = "Destroyed"]
    /\ UNCHANGED <<cacheLeases, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* Unlink from entries followed by retireEntryLocked with an active lease.
\* This covers all concrete triggers:
\*   read: clear, invalidate/removeEntriesForTableLocked, evictOldestTableLocked
\*   write: clear, pruneStaleEntriesForGroupTableLocked,
\*          removeDbEntriesForTable, pruneStaleWriteCacheLocked,
\*          runtime-status stale pruning
\* appendAssumeCapacity is safe because each published entry reserved one slot.
CacheRetireActive(c) ==
    /\ c \in Caches
    /\ cacheLoc[c] = "Live"
    /\ cacheLeases[c] = 1
    /\ cacheRetiredCap[c] >= 1
    /\ cacheLoc' = [cacheLoc EXCEPT ![c] = "Retired"]
    /\ UNCHANGED <<cacheLeases, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* Final lease release finds the entry in retired_entries and destroys it.
CacheReleaseRetiredLease(c) ==
    /\ c \in Caches
    /\ cacheLoc[c] = "Retired"
    /\ cacheLeases[c] = 1
    /\ cacheLoc' = [cacheLoc EXCEPT ![c] = "Destroyed"]
    /\ cacheLeases' = [cacheLeases EXCEPT ![c] = 0]
    /\ UNCHANGED <<cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* --------------------------------------------------------------------------
\* LSM mutable_read_snapshot lifecycle
\* --------------------------------------------------------------------------

\* beginRead retains a reader before snapshotMutableState. If the cleanup-slot
\* reservation fails before publishing mutable_read_snapshot, errdefer releases
\* the reader and no snapshot owner is created.
BeginReadSnapshotReserveFails ==
    /\ snapshotLoc = "NoSnapshot"
    /\ activeReaders = 0
    /\ UNCHANGED vars

\* Successful first read snapshot:
\*   retainReader();
\*   create/clone State;
\*   ensure retired_mutable_snapshots capacity;
\*   mutable_read_snapshot = snapshot;
BeginReadSnapshotSucceeds ==
    /\ snapshotLoc = "NoSnapshot"
    /\ activeReaders = 0
    /\ snapshotLoc' = "MutableOwner"
    /\ activeReaders' = 1
    /\ snapshotRetiredCap' = 1
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, indexTemp, indexOpFailed>>

\* A second read reuses the current mutable_read_snapshot and increments the
\* reader count. The model caps activeReaders at 1 because one reader is enough
\* to expose the ownership handoff bug; this action is a stutter abstraction of
\* additional readers.
BeginAdditionalRead ==
    /\ snapshotLoc = "MutableOwner"
    /\ activeReaders = 1
    /\ UNCHANGED vars

\* All concrete invalidation triggers have the same ownership effect:
\*   invalidateMutableReadSnapshot();
\* They include mutable rotation/flush and direct bulk-ingest finish.
InvalidateMutableSnapshotWithActiveReader ==
    /\ snapshotLoc = "MutableOwner"
    /\ activeReaders = 1
    /\ snapshotRetiredCap >= 1
    /\ snapshotLoc' = "RetiredOwner"
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, activeReaders, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* If no reader is active, invalidation destroys the snapshot immediately.
ReleaseOnlyReaderBeforeInvalidation ==
    /\ snapshotLoc = "MutableOwner"
    /\ activeReaders = 1
    /\ activeReaders' = 0
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, snapshotLoc, snapshotRetiredCap, indexTemp, indexOpFailed>>

InvalidateMutableSnapshotWithoutReader ==
    /\ snapshotLoc = "MutableOwner"
    /\ activeReaders = 0
    /\ snapshotLoc' = "Destroyed"
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, activeReaders, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* Final reader release drains retired_mutable_snapshots.
ReleaseReaderDrainsRetiredSnapshot ==
    /\ snapshotLoc = "RetiredOwner"
    /\ activeReaders = 1
    /\ snapshotLoc' = "Destroyed"
    /\ activeReaders' = 0
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, snapshotRetiredCap, indexTemp, indexOpFailed>>

\* --------------------------------------------------------------------------
\* IndexWriter.removeSegments temporary allocation lifecycle
\* --------------------------------------------------------------------------

IndexAllocateNewSegments ==
    /\ indexTemp = "None"
    /\ indexTemp' = "NewOnly"
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap, indexOpFailed>>

\* OOM at the second allocation must run errdefer for new_segments.
IndexRetiredAllocationFails ==
    /\ indexTemp = "NewOnly"
    /\ indexTemp' = "Freed"
    /\ indexOpFailed' = TRUE
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap>>

IndexRetiredAllocationSucceeds ==
    /\ indexTemp = "NewOnly"
    /\ indexTemp' = "BothAllocated"
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap, indexOpFailed>>

\* rebuildSnapshot may allocate while computing field stats or creating the
\* new IndexSnapshot. Both temporary arrays must be freed on that error path.
IndexRebuildFails ==
    /\ indexTemp = "BothAllocated"
    /\ indexTemp' = "Freed"
    /\ indexOpFailed' = TRUE
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap>>

\* On success, rebuildSnapshot transfers ownership:
\*   - new_segments becomes the new snapshot's segment array
\*   - retired becomes old.retired_segments for cleanup on old snapshot release
IndexRebuildPublishes ==
    /\ indexTemp = "BothAllocated"
    /\ indexTemp' = "Published"
    /\ UNCHANGED <<cacheLoc, cacheLeases, cacheRetiredCap, snapshotLoc, activeReaders, snapshotRetiredCap, indexOpFailed>>

Stutter == UNCHANGED vars

Next ==
    \/ \E c \in Caches:
        \/ CacheOpenReserveFails(c)
        \/ CacheOpenSucceeds(c)
        \/ CacheReleaseLiveLease(c)
        \/ CacheRetireInactive(c)
        \/ CacheRetireActive(c)
        \/ CacheReleaseRetiredLease(c)
    \/ BeginReadSnapshotReserveFails
    \/ BeginReadSnapshotSucceeds
    \/ BeginAdditionalRead
    \/ InvalidateMutableSnapshotWithActiveReader
    \/ ReleaseOnlyReaderBeforeInvalidation
    \/ InvalidateMutableSnapshotWithoutReader
    \/ ReleaseReaderDrainsRetiredSnapshot
    \/ IndexAllocateNewSegments
    \/ IndexRetiredAllocationFails
    \/ IndexRetiredAllocationSucceeds
    \/ IndexRebuildFails
    \/ IndexRebuildPublishes
    \/ Stutter

TypeOK ==
    /\ cacheLoc \in [Caches -> CacheLocs]
    /\ cacheLeases \in [Caches -> 0..1]
    /\ cacheRetiredCap \in [Caches -> 0..1]
    /\ snapshotLoc \in SnapshotLocs
    /\ activeReaders \in 0..1
    /\ snapshotRetiredCap \in 0..1
    /\ indexTemp \in IndexTemps
    /\ indexOpFailed \in BOOLEAN

CacheActiveLeaseReachable ==
    \A c \in Caches:
        cacheLeases[c] = 1 => cacheLoc[c] \in {"Live", "Retired"}

CacheNoRetiredEntryWithoutLease ==
    \A c \in Caches:
        cacheLoc[c] = "Retired" => cacheLeases[c] = 1

CachePublishedEntryHasRetireCapacity ==
    \A c \in Caches:
        cacheLoc[c] \in {"Live", "Retired"} => cacheRetiredCap[c] >= 1

CacheDestroyedHasNoLease ==
    \A c \in Caches:
        cacheLoc[c] = "Destroyed" => cacheLeases[c] = 0

SnapshotActiveReaderReachable ==
    activeReaders = 1 => snapshotLoc \in {"MutableOwner", "RetiredOwner"}

SnapshotPublishedHasRetireCapacity ==
    snapshotLoc \in {"MutableOwner", "RetiredOwner"} => snapshotRetiredCap >= 1

SnapshotNoRetiredWithoutReader ==
    snapshotLoc = "RetiredOwner" => activeReaders = 1

IndexNoTempLeak ==
    indexTemp # "Leaked"

IndexFailedOpFreedTemps ==
    indexOpFailed => indexTemp = "Freed"

Safety ==
    /\ TypeOK
    /\ CacheActiveLeaseReachable
    /\ CacheNoRetiredEntryWithoutLease
    /\ CachePublishedEntryHasRetireCapacity
    /\ CacheDestroyedHasNoLease
    /\ SnapshotActiveReaderReachable
    /\ SnapshotPublishedHasRetireCapacity
    /\ SnapshotNoRetiredWithoutReader
    /\ IndexNoTempLeak
    /\ IndexFailedOpFreedTemps

Spec == Init /\ [][Next]_vars

=============================================================================
