# Diagnose a document extraction reprocess

An accepted native document-extraction reprocess records a target generation with
the source mutation. The existing artifact stays visible while replay works on the
target. The `202` response includes an `accepted_sequence` for deferred extraction.
Use it together with the table, document and artifact to correlate the request.
Replay sequence numbers are local to the accepting shard. A repeat submission
can replace the latest intent, even if its numeric target generation is unchanged.

Read the single artifact after acceptance.

```http
GET /db/v1/tables/{table}/documents/{key}/artifacts/{artifact}
```

- `generation`, `merge_status`, and `last_error_code` describe the currently
  published artifact. An old error can remain visible while a retry is pending.
- `reprocess.requested_generation` and `reprocess.status` describe the latest
  durable extraction intent. `pending` means the target has yet to appear.
  `published` means a manifest publication recorded a matching receipt. Use
  merge status and page failures to inspect the currently visible extraction.
  `superseded` means the producer no longer matches an unfulfilled intent.
  Historical intents without a receipt can report `unverified` when their
  numeric generation was reached. Before the first publication, the pending
  summary has generation zero and omits the raw manifest.
- `reprocess.last_retry` describes the most recent durable deferral for the
  accepted sequence, including any known stage, stable error code, and next
  eligible time. It remains visible after a manifest starts carrying that
  attempt's receipt. Check its timestamp and the current manifest together.
  The observation is historical and the deadline can already have elapsed.
- `reprocess.worker` reports shard-level enrichment progress while the request
  is pending. Compare the scanned, applied, and target sequences and check the
  retry queue and worker failure flag. The scan watermark can lead the applied
  watermark while replay works around parked requests. `next_retry_at_ms` is the
  worker's next retry timestamp. The worker's `last_error_code` can identify
  a wait for publication, and `last_progress_at_ms` can show recent activity.
  The worker can be processing another document or producer, and an embedded
  caller can drive replay inline. The sample can be
  omitted while the worker is stopping, reconfigured, or busy updating status.

For a specific attempt, the existing source-fetch log includes the document key
(prefix limited to 256 bytes), producer, replay sequence, and target generation.
The log omits the source URL. The accepted sequence also appears on explicit
retry deferral and publication interruption events. Each publication window
checks the source captured by extraction. Explicit attempts also validate the
acceptance receipt in the publication transaction. Replacing an accepted
attempt fences its in-flight output even if the source bytes are unchanged.
Replay can then publish from the current source and acceptance. Check the repair ledger
for terminal failures.
An extraction timeout reason on the published generation identifies the reported
stage for that generation.

Use `antfly_document_pipeline_phase_in_flight` to see phases active when the
health metrics snapshot was built. The health endpoint refreshes its snapshot
every five seconds, so a short phase can start and finish between snapshots.
`antfly_document_pipeline_phase_duration_ns` records completed phase calls,
including errors. For example, a high source-fetch gauge with flat
completion counts indicates fetches are still in progress. The reader HTTP
gauge counts outstanding requests to the reader provider. Compare matching phases on the storage and inference services when
using hosted inference. Timings can overlap and metrics carry only phase labels.

The artifact inspection and metrics are separate samples. While the target
remains pending, maintain the published generation and source row. Provider
retry hints continue to pace requests, including explicit reprocess requests.
A new explicit request starts a fresh attempt budget for its extraction after
the existing deadline.

Native replay uses an active workset of at most 128 documents and stores
overflow sleeping tickets in a durable per-shard backlog. A replay pass can
process ready retries and scan fresh documents while older tickets wait. The
worker keeps each ticket's retry deadline, attempt budget, source identity, and
sequence across restarts. The scan frontier can move after its retry debt is
durable. The applied frontier remains behind the oldest unresolved ticket, even
if later documents have already published. When a due ticket replays a
document's current recipe, it still honors sleeping deadlines for other
producers on the same source.

The backlog allows up to 4,096 sleeping tickets subject to a separate
256 MiB allowance. The byte allowance reserves each ticket's maximum producer
metadata and its retained document identifier. If neither the active workset
nor the backlog can take more work, `retry_queue_full` signals backpressure.
The worker's `backlog` reports active and sleeping ticket counts, the oldest
unresolved sequence, and the admission constraint when blocked. Deleting or
replacing sources can retire obsolete tickets on a bounded maintenance pass.
Cursor-only maintenance persists a small checkpoint. Replay also frees capacity
as retries finish. A due retry awaiting a publication receipt keeps its
sequence unresolved while discovery can continue for independent sources.

A runtime with backlog support can read older retry state. A downgrade to an
older runtime requires draining the backlog first so the older runtime cannot
advance past retry debt it cannot read.
