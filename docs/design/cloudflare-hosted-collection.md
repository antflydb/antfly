# Hosted collection safety and deployment contract

The Cloudflare profile extends native serverless retention with a bounded
collector. The coordinator's SQLite tables hold the exact mark set and frontier;
Worker memory holds at most one decoded native page and a bounded batch. The
collector is host orchestration over native serving formats, not a second
encoding of the database.

1. **Retire publication authority.** Acquire/release the existing native work
   lease to advance its fencing token. Prior publishers cannot commit HEAD.
   Retain the current and parent manifest, and advance the native manifest GC
   floor before enumerating reader pins. Pins are read in bounded SQLite KV
   windows. A new reader cannot obtain rights below the floor; existing pins
   remain roots through the native deadline and grace rules.
2. **Trace protected data.** Decode manifests, document-facts roots and pages
   with native validators. Mark every physical object, including packed pages
   and document bodies. Process at most 128 child entries per trace window.
   Completing a parent and inserting children happen in the same SQLite
   transaction; an interrupted checkpoint cannot lose a subtree. Unsupported
   artifact kinds prevent the sweep from starting.
3. **Compact and fence the WAL.** Once manifest tracing identifies the oldest
   required WAL boundary, logically retire its prefix and delete expired tree
   entries in batches of at most 256. Preserve LSN high watermarks and the
   operation-receipt index. Atomically advance the WAL epoch while recording
   the compacted record/receipt roots and collection ID in HEAD. Retrying that
   job ID returns the same roots. A writer using the former HEAD loses CAS and
   rebuilds its changed paths in the new epoch. Unchanged shared pages retain
   their original identities and are traced from the protected roots.
4. **Drain prior WAL reads.** Hosted HTTP/Queue executions use a 30-second host
   deadline and storage cancellation checkpoints. Wait 60 seconds after the
   WAL barrier, including 30 seconds for bounded inter-host clock skew. A reader
   that started before the barrier cannot retain authority beyond this grace;
   readers starting later use the protected root or its newer descendants.
   Cursor resumption must reacquire its generation and validate the retained
   WAL floor, or return an explicit expiration error.
5. **Sweep retired identities.** List at most 128 objects per step. An object is
   eligible only if it is absent from the completed exact mark set and belongs
   to an upload epoch/fencing token older than the barrier. Delete manifests
   only below the irreversible native retirement floor. Unscoped artifacts and
   above-floor candidates are retained conservatively. Persist the listing
   continuation only after the batch finishes. Partial or ambiguous deletion
   is safe to retry.

Native publication scopes include a fencing token and nonce. WAL page names
now include the epoch in `tree.Ref.attempt`, and format-2 HEAD records the
physical retained floor. Newly created objects cannot alias retired identities.
Consequently a deletion authorized by a completed mark set remains safe after
its collector lease expires: future valid writers cannot resurrect a reference
to that retired orphan. A stale writer may finish an immutable PUT late, but it
cannot publish its old root; a subsequent cycle rediscovers that orphan.

Collector leases are separate from publication authority. Each coordinator
checkpoint and every native collector metadata operation verifies the active
job ID, lease nonce and expiry in the same SQLite transaction. A stale collector
cannot publish a new barrier, replace a mark frontier, or authorize more
objects. Cleanup of the SQL mark/frontier tables is itself batched.

This protocol requires stable coordinated metadata placement, unique physical
incarnations, bounded clocks, and the hosted request deadline. It is not enabled
for arbitrary native readers lacking those bounds. Deploy every reader/writer
before upgrading format-1 indexed WAL HEADs and enabling automatic collection.
The older whole-log object WAL is unaffected. Retry receipts are retained
indefinitely; their metadata grows with unique operations. This collector does
not implement an expiring idempotency window or claim constant lifetime storage.
