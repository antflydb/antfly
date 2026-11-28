# Diagnose a document extraction reprocess

An accepted native document-extraction reprocess records a target generation with
the source mutation. The existing artifact stays visible while replay works on the
target. Repeated requests for the same pending generation can coalesce.

Read the single artifact after acceptance.

```http
GET /db/v1/tables/{table}/documents/{key}/artifacts/{artifact}
```

- `generation`, `merge_status`, and `last_error_code` describe the currently
  published artifact. An old error can remain visible while a retry is pending.
- `reprocess.requested_generation` and `reprocess.status` describe the latest
  durable extraction intent. `pending` means the target has yet to appear.
  `published` means the target is visible. Inspect its merge status and page
  failures for the outcome. `superseded` means the producer definition changed
  after the request. Before the first publication, the pending summary has
  generation zero and omits the raw manifest.
- `reprocess.worker` reports shard-level enrichment progress while the request
  is pending. Compare the scanned, applied, and target sequences and check the
  retry queue and worker failure flag. The scan watermark can lead the applied
  watermark while replay works around parked requests. `next_retry_at_ms` is the
  worker's next retry timestamp. The worker can be processing another document
  or producer, and an embedded caller can drive replay inline.

For a specific attempt, the existing source-fetch log includes the document key
(prefix limited to 256 bytes), producer, replay sequence, and target generation.
The log omits the source URL. Check the repair ledger for terminal failures.
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
