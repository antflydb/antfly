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

## Active reindex checkpoints

A hosted reindex publishes an immutable state artifact and a small
`progress/<namespace>/REINDEX` pointer. Both share the native publication domain
and fencing epoch. The pointer's conditional update compares its old ETag and
the live `HEAD` ETag in one coordinator transaction. A preceding lease check
followed by an ordinary pointer CAS would permit a stale worker to install
roots after the collector's barrier; the cross-key guard closes that race.
Providers without that transactional capability reject the write.

Preparation reads the active checkpoint under the namespace work lease before
retiring that epoch. It includes the source manifest and checkpoint artifact
in its roots, lowers the proposed manifest retirement floor to the source
version, and traces the checkpoint's staged facts/text/sparse/vector references.
The source and staged manifests also preserve the required WAL prefix. A step
that resumes after the barrier either publishes under a newer native epoch or
fails its guarded pointer CAS. Collection may run between every pair of steps.

Checkpoint expiry is ten minutes after the last successful checkpoint update.
An expired job cannot resume old references: its status/step observes no active
job, and a new explicit start uses the current source generation. Cancellation
writes a conditionally guarded empty pointer. Stale Queue IDs cannot create a
job or cancel its successor. Completed publication is acknowledged through
bounded immutable-lineage pages, including when a committed HEAD response was
lost and many later publications have already advanced the head.

Deploy the compatible tracer and guarded coordinator together with the reindex
runtime before admitting jobs. Older collectors do not know about job roots.
