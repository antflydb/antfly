# Bounded, windowed serverless diagnostic evidence

Tracking: #1278; implementation is opt-in on draft PR #1320. This changes the
qualification observer and reader, not Antfly's database storage protocol or the
approved corrected control. It does not establish a hosted gate pass.

## Problem and shape

The previous observer captured each source's entire history before reconciling
one minute. Full sanitized OTLP child spans shared the capture with canonical
terminals and the independent native Tail delivery counter. A source could
exceed the 64 MiB capture limit even when the closed minute itself was small.
Increasing that limit would make capture and verification consume more memory
without removing the source-history dependency.

`window-v1` publishes compact proof objects into immutable UTC minute
partitions and retains the full sanitized delivery in a distinct R2 archive
bucket. The read-only reader has only the proof bucket binding. It cannot read
verbose archives, invoke producers, write counters or delete evidence.

```
LOGS/probe-v1/<capture>/LAYOUT.json
LOGS/probe-v1/<capture>/windows/<16-digit-minute-ms>/batches/<sha256>.json
LOGS/probe-v1/<capture>/windows/<16-digit-minute-ms>/counter/<delivery-uuid>.json
LOGS/probe-v1/<capture>/witnesses/<attempt-sha256>/<delivery-uuid>.json
LOGS/probe-v1/<capture>/rejections/...
ARCHIVE/archive-v1/<capture>/otel/<sha256>.json
ARCHIVE/archive-v1/<capture>/counter/<sha256>.json
```

The anchor pins the layout, source and capture. Layout discovery rejects legacy
`batches/` or `counter/` objects under the same capture. New runs explicitly
select this format; existing manifests, sealed reports and bundles are not
converted. Reader and producer scope remain the exact eight Workers belonging
to one disposable baseline/candidate pair.

## Projection and publication

Every provider terminal keeps its source/version, provider invocation hash,
trace/span hashes, handler, timestamps, outcome and CPU/wall metadata. Its
`source_row_sha256` hashes the full sanitized original row before projection.
Two copies with different verbose attributes therefore still conflict even if
their compact fields match. Benign nonterminal spans and healthy phase payloads
move to the archive. Dropped rows, critical warnings, resource failures,
exceptions, truncation, unknown/error payloads and physical I/O lifetime
records remain in the proof. Both OTLP proof diagnostics and independent Tail
diagnostics are evaluated; a healthy counter cannot hide an unsafe OTLP child.
The existing privacy sanitizer still runs before either destination is written.

The producer validates timestamps and bounds the partition fanout to 32 per
sanitized delivery. It writes the immutable verbose archive before publishing
proof partitions; archive failure cannot produce a successful clean delivery.
Conditional create collisions verify existing size and content. Partial writes
and storage failures use the existing rejection paths and fail reconciliation.
The Tail adapter preserves the original delivery ID as `dispatch_id`; each
minute partition has its own immutable delivery UUID and exact record count.
Delivery UUIDs are never represented as provider invocation identities.

Only explicit boundary witness calls carry
`__antfly_qualification_boundary=1`. At most eight witnesses are indexed per
Tail delivery. An index points to the actual immutable native Tail partition;
the reader and client check source, capture, hash, timestamp and counter key,
require a unique pointer, and fetch the real delivery. An index does not create
a provider record or establish a population by itself. Ordinary workload
attempts do not create per-attempt index writes.

## Capture and verification

The reader adds `layout`, `window`, `witnesses` and `rejections` source routes.
Window listing is restricted to one actual UTC minute. A logical closed
60-second window can span two UTC partitions. Native cursors must progress;
listed/headed ETags pin fetched objects. Each response retains the existing
32-object / 4 MiB body bounds. No archive prefix is reachable through these
routes. Source rejections are fetched unfiltered for every closed window, and
the existing global observer/receiver health checks still apply.

The client first discovers the layout and the two boundary deliveries. It then
captures each closed minute independently, deduplicating its physical partition
keys with the boundary deliveries. Each observation retains the existing
64 MiB, 20,000-object, 2,000-page and 120-second limits. The original 20,000
terminal limit and strict metadata checks still apply inside each closed
window. These limits have not been raised.

Each minute is written to its own local file. A small catalog holds paths,
timeframes, byte lengths and SHA-256 hashes. Offline and online reconciliation
verify each file and process one first/second window pair at a time. They retain
compact identity sets across the run to reject cross-window invocation or app
attempt duplication. Catalogs contain at most 61 windows and cover exactly the
witness-bounded run, which remains limited to one hour. App attempts are checked
against the full physical request ledger after the window proofs complete.

The original settlement rule remains: the first observation starts at least
60 seconds after window end, the second starts at least 30 seconds after the
first finishes, and selected canonical/native evidence must be identical.
Late delivery, missing counts, mismatched timestamps, wrong versions, recovered
resource errors, truncation and unfinished physical I/O still prevent a pass.
A catalog entry or diagnostic test never counts as a hosted qualification.

## Operation

Prepare a **new** observer from clean committed source:

```sh
node qualification/paired-observer-lifecycle.mjs prepare <gate.json> <trial> --windowed-evidence
```

The manifest records `evidence_layout: "window-v1"` and a scoped
`<observer-id>-archive` bucket. Seal and use the existing deploy, token,
reader, receiver and attachment lifecycle. Both ingest Workers receive the
archive binding; the read-only reader does not. Retirement and cleanup drain
both owned buckets before deleting them. Credential upload and permanent
cleanup still require the authorization applicable to the concrete disposable
deployment. No resources were created for the local implementation tests.

Do not enable this by editing a sealed legacy manifest. Preserve the corrected
control's original application modules and use the same diagnostic format on
both sides of a new paired trial. Copy every catalog-referenced minute file
along with the catalog when exporting or sealing evidence; the catalog alone
is not a complete observation.

## Local validation and remaining qualification

Regression tests cover archive isolation with more than 64 MiB of unrelated
verbose history, three successive closed windows and their complete app
ledger, invalid minutes/versions/full-row hashes, duplicate and missing native
witnesses, archive failure, excessive partition fanout, mixed legacy data,
changed local files, failed settlement, recovered `SQLITE_NOMEM`, truncation,
unfinished I/O and unsafe OTLP children. The original 20,000-terminal guard
still rejects an oversized compact window. Actual local workerd tests exercise
the receiver, Tail adapter with fixture provider events, conditional R2 writes,
separate archive storage, ETag-based reader gets and witness lookup. They do not
simulate exhaustive hosted Tail delivery behavior or prove physical placement.

Next run a short paired hosted rehearsal with fresh identities and sealed
application modules. Require complete source/receiver health, two settled
observations per closed minute, native population equality, the full app
attempt ledger and zero recovered resource failures before investing in the
three paired hour trials and 100 physical-cold epochs. Actual placement and
live-provider ingestion/load remain unqualified. A recovered live
`SQLITE_NOMEM` remains a failing diagnostic; these changes do not fix the
provider's internal SQLite allocator.

The committed implementation `9a943f5168` passes 461 Node tests and four focused
actual-workerd integration tests. The separate packed/deferred 50,000-record
native regression passes with zero allocator leaks. Its binary, source, logs,
failed local attempts and verifier are sealed in 37 artifacts / 15,064,285 bytes
at `dist/qualification/windowed-evidence-1793375934794/evidence.json`, SHA-256
`9fd6d6f3ad7efcadd6e4e1d12dea828431d5e9995432e0fee797a107f8e3109a`.
All 249 artifacts in the two preceding compact-serving closures were also
reverified without changes. The older, separately documented 163-artifact
availability gap remains. Guarded `make gc` deferred cleanup while native builds
were active or process inspection was unavailable; no guard was overridden.

The subsequent [memory investigation](cloudflare-sqlite-memory.md#diagnostic-memory-hardening-and-the-next-hosted-reproduction)
adds per-isolate diagnostic admission, streaming gzip input, JSON complexity
preflight and fixed storage-boundary attribution. It also includes receiver
rejections in reader health and retains reader resource failures across capture
retries and offline reconciliation. These are required for the next observer
release; local success does not establish a hosted allocator fix.
