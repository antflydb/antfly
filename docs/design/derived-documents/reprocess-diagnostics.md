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
- `reprocess.worker` reports shard-level enrichment progress while the request
  is pending. Compare the scanned, applied, and target sequences and check the
  retry queue and worker failure flag. The scan watermark can lead the applied
  watermark while replay works around parked requests. `next_retry_at_ms` is the
  worker's next retry timestamp. The worker can be processing another document
  or producer, and an embedded caller can drive replay inline. The sample can be
  omitted while the worker is stopping, reconfigured, or busy updating status.

For a specific attempt, the existing source-fetch log includes the document key
(prefix limited to 256 bytes), producer, replay sequence, and target generation.
The log omits the source URL. The accepted sequence also appears on explicit
retry deferral and publication interruption events. Check the repair ledger for
terminal failures.
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
the existing deadline. The shard's durable retry queue still bounds admission.
