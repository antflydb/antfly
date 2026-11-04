# Table privacy

Privacy inspects selected string fields for configured entity categories. A
policy can report findings (`monitor`) or redact the documents consumed by
search and AI (`redact_for_search_and_ai`). Originals remain stored. The
`redact_before_storage` mode returns a typed unsupported response until a
synchronous ingestion implementation is available. Policies, previews, and rescans
require document storage without an external base source; unsupported storage
returns `PrivacyRelationalStorageUnsupported` before inspection or mutation.

Start in a table's **Privacy** tab: choose fields and categories, preview a
bounded sample, inspect highlighted spans, and adjust sensitivity or individual
category thresholds. A protective save requires a fresh preview and an impact
acknowledgment. Samples contain original values and require table administrator
access. A preview is not a table-wide detection-quality assessment.

## API

All paths below are relative to `/db/v1`. Privacy operations require table
administration permission when access control is enabled.

```http
POST /tables/customers/privacy/preview
Content-Type: application/json

{
  "policy": {
    "mode": "redact_for_search_and_ai",
    "fields": ["body"],
    "category_groups": ["contact_details", "personal_identifiers"],
    "sensitivity": "balanced"
  },
  "sample": {"keys": ["customer-1", "customer-2"]}
}
```

Preview accepts up to 50 documents and does not install a policy. Save the
policy object with `PUT /tables/customers/privacy`. The response is `202` with
an ETag, immutable `policy_version`, and an automatically scheduled `scan_id`.
Read the policy, resolved producer, and current coverage with `GET` on that
path. Replacing an existing policy requires its exact ETag in `If-Match`;
missing and stale preconditions return `428` and `412` respectively.

Poll `GET /tables/customers/privacy/scans/{scan_id}`. Jobs progress through
`queued`, `running`, and `processing`; processing verifies current receipts and
consumer publication before `succeeded`. Failed jobs expose bounded document
IDs and error codes. A semantic policy replacement supersedes earlier jobs.

```http
GET /tables/customers/privacy/findings?field=body&category=email&limit=100
```

Findings include document, field, category, half-open UTF-8 byte coordinates,
policy version, source fingerprint, producer revision, and confidence when the
producer supplies it. They omit matched values. Pagination cursors bind the
policy and filters; restart pagination after changing either. Treat cursors as
opaque strings and pass them unchanged, including for large or escaped document
identifiers. Use `POST /tables/customers/privacy/findings` with the same fields
in a JSON body for large cursors or filters that exceed URL limits. The TypeScript SDK
and Privacy tab use this body form. Explicit
`policy_version` requests for a superseded policy return `410`.

Failure examples are limited to 100 entries and 1 MiB of serialized JSON.
Coverage and scan failure counts include documents omitted from those examples.

Start a bounded retry or rescan with `POST /tables/customers/privacy/scans`,
optionally supplying `from_key`, `to_key`, and a page `limit` from 1 to 1000.
Source updates also schedule inspection through the ordinary producer pipeline.

## Producers and execution

The default is the pinned
[Fastino GLiNER2 PII model](https://huggingface.co/fastino/gliner2-privacy-filter-PII-multi)
at revision `1cb4166094dc58fa8d836429f060d6c95f62b495`. It uses Antfly's
existing artifact extraction infrastructure, including native admission and
cancellation. Long documents use bounded overlapping windows with exact
512-token admission; spans are rebased into the source. Compatible inspections
are batched across documents and fields, and compatible windows use Metal/CUDA
batches of up to eight. Native CPU execution remains serial.

Override `producer` using the artifact API's configuration shape:

```json
{
  "type": "extractor",
  "revision": "immutable-deployment-revision",
  "config": {
    "provider": "antfly",
    "model": "my-pinned-extractor",
    "schema_version": 2
  }
}
```

Generated extractors and generators are supported too. Generators use
`type: "generator"` and an artifact producer config containing `generator`
and `output_schema`. Evidence must contain valid source spans; invalid evidence
fails inspection. Custom producer revisions are mandatory. Credentials must be
empty, environment references, or supported secret references; literal
credentials are rejected. Remote producers receive original selected text;
the preview and policy response disclose external processing.

The union of expanded category groups and explicit custom categories is limited
to 128 categories. Overrides of an existing group category count once. This
limit is checked before preview or policy installation.

`execution.batch_items` (1–8) and `execution.max_attempts` (1–5) tune batching
and invalid-evidence retries. Changing them preserves semantic policy identity
and accepted inspection results. Transport backoff remains owned by the
artifact runtime. Changing fields, categories, thresholds, replacement, mode,
or producer identity creates a new policy version and schedules backfill.

## Protected serving

Protective policies atomically rewrite default and named full-text/vector
consumers to use the sanitized projection. Chunking and embeddings consume
sanitized document text. Original authored vectors, unsupported graph or
relational consumers, and unsupported extraction chains are rejected during
admission. Ordinary index/enrichment mutations cannot replace policy-owned
objects while protection is active.

Point reads and hydration verify current source, policy, and storage-owned
publication evidence. Search also fences producer completion and physical
index checkpoints before candidates, statistics, filtering, and aggregation.
Queries can temporarily return unavailable while a protected generation is
pending or failed; readiness is conservative across the table's managed
consumers. Original values do not become searchable while backfill runs.
Original artifact inspection requires separate administrator access.

SQL reads of document tables apply predicates, joins, projections and aggregates
to the sanitized document snapshot. SQL mutations and delayed serializable
snapshots on protected tables return an unsupported response. Use the ordinary
document batch API for writes; it preserves originals and schedules inspection
for the new source revision. Pending inspection makes SQL reads retryably
unavailable rather than exposing an older source or original values.

Coverage reports current, pending, known failed, documents with findings,
category and field counts, and protected readiness. Unavailable or incomplete census
results have null counts instead of an optimistic zero. Detection is
probabilistic: qualify categories and thresholds against representative text
before relying on the policy for your application.


Coverage counts are collected by a durable background census in bounded pages.
`GET /tables/{table}/privacy` reads its latest completed result without scanning
the data shards. `started_at_millis` and `completed_at_millis` describe the
observation interval; these informational counts refresh asynchronously and are
not an atomic cross-shard snapshot or permission to serve a document. While
`refreshing` is true, the previous completed counts remain visible. Before the
first complete pass, coverage is unavailable. Source and policy receipts still
fence every protected read independently of these counts.

Scan history retains at most 16 jobs. Completed, failed, and superseded jobs can
be retired; active jobs are preserved. When all 16 are active, another scan
request returns HTTP 409 (`privacy_scan_limit_exceeded`) without losing work.

Policy saves acknowledge the installed resource version. Background scan and
coverage updates do not change its ETag or delay acknowledgement. Equivalent
field/category ordering and producer object-key ordering keep the executable
producer unchanged; execution tuning still updates runtime controls separately.

Findings enumerate document IDs and inspect one source and manifest at a time.
Pages stop between documents after an 8 MiB inspection budget, or within a
document when the finding count or output-byte budget is reached. A document
is validated completely before any of its findings are returned, so a single
large document can exceed the inspection budget. Follow `next_cursor` even
when a page contains no findings; missing or stale receipts count as pending.

Full-table scan jobs share the background coverage census. Completion requires a
fresh pass started after that job entered verification, plus protected serving
readiness where applicable. Range scans verify their own bounds in adaptive pages.

Preview accepts at most 50 documents. Samples, including JSON-escaped document
identifiers, have an 8 MiB budget; preview responses also have an 8 MiB budget.
Exceeding either returns HTTP 422 (`privacy_sample_too_large`); request fewer or
smaller documents. Automatic sampling streams rows and stops before retaining
an oversized sample. A rejected preview does not install a policy.

Private range checkpoints have per-table budgets of 256 MiB for staged uploads
and 8 GiB for retained payloads, including retired generations awaiting cleanup.
Capacity exhaustion returns HTTP 422 (`privacy_checkpoint_capacity_exceeded`);
allow cleanup to finish and use fewer or smaller concurrent range scans before
retrying. Metadata protocol 81 is required for new privacy mutations. Older
checkpoint headers remain readable and are reclaimed during upgrade.
