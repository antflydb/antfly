# Grepstein

Grepstein is the GCS-backed Antfly Cloud public-records showcase tracked in
[Colony #818](https://github.com/antflydb/colony/issues/818). The executable uses
Antfly's native `document_extraction` artifacts. It has no DocsAF dependency.

V1 indexes PDFs from DOJ datasets **1–7 and 12**. Indexing datasets **8–11 is
deferred**, and the v1 downloader refuses them. Raw preservation is independent:
`stage-originals` accepts PDFs, images, audio, video, and ancillary files from any
explicitly supplied release directory or ZIP, including larger releases. The
court collection can be supplied as an existing
directory or ZIP; automatic court inventory acquisition is not implemented.
`datasets` prints the release registry, without asserting unverified document
counts. Downloading all small releases still requires several GB of local disk,
plus prepared PDF windows and any grep snapshots. Fetch defaults to a 10 GiB total
source budget (`--max-bytes`), with a 2 GiB ceiling for each original PDF. Published
parts must fit 64 MiB so they stay below Antfly’s normal remote-content ceiling.

## Ownership

1. The showcase tool inventories releases, downloads verified PDFs, splits PDF
   bytes into bounded windows, and publishes originals/parts to private GCS.
2. Antfly downloads the remote PDFs, extracts native text/OCR, and owns durable
   page and chunk artifacts. Binary PDF splitting performs no text extraction.
3. Full-text, embeddings, graph units, and canonical grep pages consume those
   same native artifacts. Adding an index does not reload source documents.
4. Colony stores credentials and runs existing GCS source jobs for canonical
   grep publication, freshness, access policy, and scoped LinearMerge. There is
   no new Cloud `/sources` API. Self-hosting uses the same source publisher and
   ordinary `antfly_sources` catalog table directly.
5. The hosted explorer uses a table-scoped read key on the server. Browsers get
   fixed read-only search operations and original PDF evidence, not credentials.

## Build and acquire

From this directory:

```sh
GOWORK=off GOEXPERIMENT=simd go build -o grepstein .
./grepstein datasets
./grepstein fetch --dataset all-small --inventory-only --output ./grepstein-data
./grepstein fetch --dataset all-small --output ./grepstein-data
./grepstein prepare-small --downloads ./grepstein-data --state ./grepstein-corpus
# Optional court PDFs: add --court /path/to/court-pdfs-or.zip to prepare-small.
```

Fetch discovers every paginated release page before downloading. HTTP failures,
age/access pages, repeated pagination, ambiguous filenames, and non-PDF responses
fail visibly. It does not bypass access gates. If access is unavailable, obtain
the public PDFs through an authorized browser and use `prepare --source` below.
Downloads have SHA-256 receipts; retries verify cached bytes. Completed release
snapshots have their own immutable directories, so an older release inventory
cannot silently contribute stale files to the next preparation.

Internet Archive can supply historical copies when official acquisition is
unavailable. Select the archived dataset explicitly:

```sh
./grepstein fetch-archive --dataset all-small --output ./.cache/archive
# Use each ZIP path printed by fetch-archive, repeating --source for the releases.
./grepstein prepare --state ./archive-pilot --source 'doj-06=/absolute/path/DataSet 6.zip'
```

The archive downloader selects original per-release ZIPs for releases 1–7 and
12 only. It enforces a total compressed-byte budget, checks advertised MD5/SHA-1
and ZIP structure, and records SHA-256 plus the acquisition URL, item, original
filename, retrieval time, and official release association beside each ZIP.
Changed archive metadata gets a separate snapshot directory. Preparation verifies
the ZIP against its receipt and carries that provenance into every source row.
Keep each ZIP with its `.json` receipt. Archive acquisition uses `prepare --source`;
`prepare-small` consumes completed official `fetch` snapshots.

Archived copies remain **private pending publication review**. Transport checksums
do not establish equivalence to current DOJ bytes or current official availability.
Official links may relocate between dataset directories: distinguish available,
relocated, unavailable, and unresolved states; an old URL's 404 alone is not a
withdrawal. Flag changed official bytes for review rather than silently replacing
or relabeling archived files. The downloader records official status as unresolved.
Do not use a merged all-files PDF as the authoritative acquisition source: it
loses individual source identities and original-page relationships.

For a small qualification fixture or manually obtained release:

```sh
./grepstein prepare --state ./pilot --source doj-12=/path/to/pdfs \
  --pages-per-record 25 --limit-files 10
```

Preparation checkpoints resume with `prepare --state STATE --resume`. Inspect
`skipped.ndjson`: publication refuses a state that skipped unreadable PDFs. Fix
or explicitly quarantine those files and prepare a new state. V1 publishes PDFs;
other media require a separately qualified transcription workflow.

## Publish and load full text

Preserve original content separately from the selection sent to Antfly:

```sh
# No Google credentials or Antfly calls are needed for this inventory.
./grepstein stage-originals --inventory-only --state ./grepstein-originals \
  --source 'doj-02=/absolute/path/DataSet 2.zip'
# Supply the same reviewed sources and an approved storage byte budget to upload.
./grepstein stage-originals --state ./grepstein-originals \
  --bucket antfly-showcases-originals \
  --source 'doj-02=/absolute/path/DataSet 2.zip'
# Repeat --source for other approved release directories/ZIPs.
```

The raw stage retains file bytes unchanged, including native video/audio/images
and `.dat`/`.opt` release metadata. It performs no OCR, transcription, image
captioning, PDF splitting, inference, or Antfly writes. Its `originals.ndjson`
catalog records dataset, member path, container, MIME, SHA-256, MD5, CRC32C, and
uploaded object generation. Known archive receipts are verified and retained.
Explicit local copies without acquisition receipts have no asserted official
provenance. The catalog cannot be used as an Antfly load manifest.

Uploads use ADC and the create-only publisher identity, with resumable GCS
uploads, an 8 MiB upload buffer, and CRC32C validation before finalization.
SHA-addressed objects are reused only after size/checksum verification; staged
PDF originals use the same keys as subsequent PDF publication. ZIP members spool
one at a time; scratch state must be outside input directories. Defaults cap
total uncompressed inputs at 10 GiB, each original at 8 GiB, and a batch at two
million files. Raise byte limits only for a reviewed batch and adequate worker
scratch space. Split larger inventories into separate states.

The catalog is sealed and uploaded only after every original succeeds. A failed
run may leave verified raw objects; rerunning reuses them and rechecks inputs.
No partial run produces a new completed catalog. Original staging is independent
of the existing `prepare` → `publish` → `load` path: keep that path restricted to
the small-release PDF state for v1. Adding raw files to the bucket never adds
them to the managed instance or existing source jobs.

For releases 8–11, run bulk acquisition and staging on a separately approved
Google Cloud worker rather than routing hundreds of GB through a laptop. The
v1 downloader does not acquire those releases and this command does not provision
that worker. Candidate archive metadata must be reviewed before transfer; some
dataset 9 mirrors explicitly declare themselves incomplete. Record that as the
**mirror's reported status**, separately from our **unverified dataset coverage**.
It does not establish that every available copy of dataset 9 is incomplete.
Retain revision/source checksums rather than treating a mirror as current official
coverage. Raw storage charges and transfer/worker costs are separate from the
Antfly instance's capacity and later enrichment costs.

Preserve available dataset 9 snapshots now and reconcile additional sources later.
Use separate staging states/catalogs for each mirror revision so the same logical
filename with different bytes remains a visible conflict. Compare actual files
against versioned expected inventories (including `.dat`/`.opt` where applicable),
recording what those inventories cover: a PDF inventory alone cannot establish
native audio/video completeness. Track present, missing, and conflicting files;
leave missing counts unknown until the expected inventory has been verified.
Merge identical content by checksum and retain every source association. Review
different bytes for the same logical identifier rather than choosing the largest
file or overwriting an earlier copy. Additional archived sources do not change
the small-release PDF selection used for Antfly v1 or authorize public release.

Provision the resources in Colony's `infra/showcases` first. The uploader uses
ADC for the create-only publisher identity. Antfly's source-reader identity is
separate; it needs read access to the originals bucket.

```sh
./grepstein publish --state ./grepstein-corpus --bucket antfly-showcases-originals
# Set ANTFLY_API_KEY securely to a write key for this instance/table.
./grepstein load --state ./grepstein-corpus --url "$ANTFLY_URL" \
  --create-table --num-shards 4 --source-connection "$GCS_SOURCE_CONNECTION" \
  --reader-config lightonocr-reader.example.json \
  --inference-url "$ANTFLY_INFERENCE_URL" \
  --inference-secret antfly.antfly_inference.api_key
./grepstein wait --url "$ANTFLY_URL"
./grepstein qualify --state ./grepstein-corpus --url "$ANTFLY_URL" \
  --output ./grepstein-corpus/extraction-qualification.json
./grepstein stats --state ./grepstein-corpus --url "$ANTFLY_URL"
```

`wait` checks index serving milestones. `qualify` separately checks every PDF in
the sealed publication against its native generation-pinned GCS artifact, exact
page count and explicit OCR counters from the raw extraction manifest. With API
authentication enabled, `qualify` needs a table-admin key to read that raw evidence.
Failed or unfinished extraction, missing counters, stale source bindings or any
OCR failure block qualification, even when full-text search is ready. Run it
before semantic/graph enrichment; a successful receipt measures extraction
coverage, not OCR transcription accuracy, source authorization or public readiness.

The showcase OCR profile uses revision-pinned LightOnOCR-3-4B on shared Antfly
inference. Qualify both reader sizes on the deployed pool and run a representative
PDF pilot before backfill. Its native `/read` contract uses
an image-only prompt; the extraction producer sends no prompt to readers.
The profile permits up to 8192 output tokens per page, matching the native
reader's dense-page ceiling. Redacted DOJ call-detail tables can exceed 2048
because their HTML table structure and repeated redaction cells are part of the
transcription. A full call-detail page that truncated at 2048 completed on
rc.14 with 6994 output tokens at the 8192 ceiling, including its closing table
and page footer. This single-page result establishes capacity for that sample;
verify the remaining corpus and deployed pool separately. The serving policy
must also permit
`resources.inference.request_limits.max_output_tokens_per_item: 8192`.
A larger ceiling increases per-request work and KV admission; it does not change
memory, concurrency, or batch limits. Size the work budget for the intended
parallelism as well: an image-only 8192-token LightOn request uses 36 work units,
so four independent requests need 144 units with the current admission policy.
Completed short pages still stop at EOS.
Use `--reader-max-output-tokens 8192` with
`zig/tools/qualify_reader_generator_pools.py` to exercise its exact rejection
boundary. The synthetic probe does not establish corpus coverage.

Truncated OCR remains an extraction failure. Inspect every page's completion
status and retained-text warning; neither readable older text nor a converged
merge proves that OCR completed. Run the
[release qualification](../../docs/design/inference/grepstein-release-qualification.md)
before backfill or enrichment, and use durable table repair jobs for long-running
retries. Pages that still truncate at the allowed ceiling require inspection and
an explicitly qualified extraction strategy; never discard their warnings or
mark them repaired merely because a manifest exists.

The LightOnOCR models return
Markdown, including table structure, through the same native reader API.
`ocr.render.sampling` defaults to `auto`: smooth continuous-tone PDF images
and retain one-bit scan coverage. Use `pdf` to preserve PDF interpolation rules,
or `smooth` to explicitly interpolate one-bit image color too (which can blur
thin strokes). Model resizing, DPI and resource caps remain independent. Native
unit and chunk provenance records the versioned rendering policy. A completed
decode is not proof of transcription accuracy; compare visible text before backfill.

Set `ANTFLY_INFERENCE_URL` to a server-reachable inference root or `/ai/v1` URL;
`--inference-url` binds the LightOnOCR profile and relation extractor to that endpoint.
The Cloud connection uses the existing `antfly.antfly_inference.api_key` server
keystore key. `--inference-secret` stores only a secret reference in the OCR,
embedding and relation configs; self-hosted users select their own key name.
Reader configuration rejects inline credential values. Repeat the endpoint and
secret flags for `enrich --stage semantic` and `enrich --stage graph`.
Without an endpoint override, Antfly's runtime inference configuration owns routing.
Hosted indexing automatically wakes a configured inference pool when model
capabilities are missing: the service advertises activation on its read-only
catalog, and the native client uses Antfly's existing authenticated
`POST /ai/v1/models/instances` API, then refreshes its capability lease. Lookups
share activation work and honor cancellation and deadlines. By default model
preparation has a five-minute cold-start budget; embedding inference keeps its
30-second budget after preparation. Explicit caller deadlines cap both phases.
Self-hosted services without the activation hint keep their existing discovery behavior.
For a purely digital-text pilot, omit `--reader-config`; scanned pages will still
need OCR. The Vertex reader example remains available as an alternative.

The relation stage defaults to pinned `fastino/gliner2.5-base-v1`, with entity and
relation schemas and bounded long-document windowing. Colony must deploy its
GLiNER2.5 and LightOnOCR GPU routes before the hosted pipeline is qualified. CUDA
kernels alone do not qualify the GLiNER2.5 serving policy: use a runtime with
measured CUDA production profiles, then verify the deployed L4 path. Preserve the existing GLiNER2 route for other callers.

For semantic backfill, use `--embedder-config antfly-embedder.example.json` to select
EmbeddingGemma 2 with 768 dimensions consistently across enrichment and the index.
This stage reuses extracted chunks. A model change requires re-embedding; never
mix the existing CLIP or Gemini vectors with EmbeddingGemma vectors. Pin query and
document vectors to the same runtime model identity (CUDA BF16 differs from CPU F32).

GCS publication uses SHA-256 object keys and `ifGenerationMatch=0`. Existing
objects are checked by size/MD5 before reuse. Originals and prepared parts have
stable identities and recorded GCS generations. The complete remote-locator
manifest is uploaded to GCS before `published.ndjson` is committed locally;
`publication.json` records its location, digest, and generation. Cloud readers reject
truncated/blocked model completions and preserve all final Vertex text parts. Interrupted
publication can be rerun. Keep the state and immutable source snapshots for
recovery. No local source server is required after publication.

Native artifact reads use `gs://BUCKET/objects/sha256/...?generation=GENERATION`
through an existing `connections` entry with protocol `gcs`, capability
`content.fetch`, an exact bucket allowlist and prefix `objects/sha256`.
Set `GCS_SOURCE_CONNECTION` to that runtime connection name. In Cloud, configure
the existing instance GCS provider connection with Google Workload Identity,
read-only access, the bucket and prefix; Colony seals this selector and publishes
the scoped runtime connection. Grant only the instance's Kubernetes Workload
Identity principal `roles/storage.objectViewer` on the originals bucket.
Self-hosting uses the same canonical connection with ADC or a keystore-backed
Google credential source. A bare `gs://` URL never enables ambient access.
No HMAC keys or organization-policy exceptions are required.

Keep the corpus private until the deployed runtime passes generation, scope
and real-PDF OCR checks for native artifact downloads.

`load` defaults to full text only and resumes durable batches against the exact
manifest, endpoint, table identity, and extraction configuration. Reuse the same
flags on resume, without `--create-table`. A replaced table cannot inherit an old
checkpoint. `wait` requires complete and queryable aggregate **and shard**
readiness; successful submission alone is not success. `grep` and `graph` also
check artifact generations and unit inventories before publishing derived data.

Resumable loading requires a stable `table_id` and a table schema with the
`simple` text analyzer. Verify both on the target instance. The server creates its system-owned default text index;
the client declares only its named artifact index. Native PDF extraction works,
but that is insufficient qualification of the complete load. Qualify the
actual load, readiness, and artifact contracts before importing the corpus. Do
not bypass table identity checks or infer complete coverage from accepted rows.

## Semantic and graph stages

```sh
./grepstein enrich --url "$ANTFLY_URL" --stage semantic \
  --embedder-config antfly-embedder.example.json
./grepstein wait --url "$ANTFLY_URL" --index embeddings
./grepstein enrich --url "$ANTFLY_URL" --stage graph
./grepstein graph --state ./grepstein-corpus --url "$ANTFLY_URL"
./grepstein wait --url "$ANTFLY_URL" --index full_text_index_v0
./grepstein wait --url "$ANTFLY_URL" --index autograph_relations
```

The alternative `--embedder-config vertex-embedder.example.json` profile selects
**`gemini-embedding-2`**, project
`antfly-showcases`, location `global`, and 3072 dimensions. The index, generated
embedding artifacts, and query embedder share the same dimensions/provider.
The stable Google model ID is not `gemini-embedding-002`. The native adapter uses
`embedContent`, one input per request, and paired query/document instructions;
the model does not accept the older `task_type` request field. Binary media
remains in raw storage; this stage embeds the selected PDFs' native text chunks.

Enable semantic enrichment after query and document embedding probes succeed
on the deployed runtime. Do not silently substitute the
older `gemini-embedding-001` model. OCR has its separate native reader profile;
both providers require the remote worker's scoped Vertex Workload Identity.
The OCR profile explicitly pins its modern model and `global` location. Stored
reader recipes that omit model/location retain their legacy defaults
(`gemini-2.5-flash` / `us-central1`); changing a process default must not
reinterpret an existing extraction recipe.

New Gemini and Vertex index configurations resolve to `gemini-embedding-2`,
`request_format: "embed_content"`, and `task_encoding: "prompt"` at creation.
Vertex defaults to the `global` endpoint. These resolved settings are stored in
the catalog and semantic producer identity; existing indexes keep their previous
protocol and task representation on reload. Artifact producer reconstruction
uses the pinned Vertex location rather than the worker's process location.
Changing model, protocol, or task
encoding requires a new vector space and re-embedding.

The transport is explicit rather than inferred from model names. For legacy
Vertex models, set `request_format: "predict"` and `task_encoding: "task_type"`;
legacy Gemini uses `batch_embed_contents` with `task_type`. `task_encoding: "none"`
sends unmodified text without a task field. Unsupported requests fail without
retrying through a different protocol. `batch_size` caps inputs per upstream
request; `embed_content` accepts only 1, preserving one embedding per record.
The current native Google adapters accept text only, even when the remote model
supports other modalities.

For a separately qualified local embedder, omit `--embedder-config` and use the
registered 768-dimensional EmbeddingGemma 2 model flags. Use a registered GLiNER extractor for
the graph stage. Default inference routing is inherited from the Antfly instance;
`--inference-url` is an explicit override. Graph materialization retains native
page provenance, detects superseded extraction revisions, and reconciles stale
unit rows. Relationship neighborhoods/path evidence are model outputs requiring
inspection, not verified factual claims.

The default OCR profile uses the native LightOnOCR reader through Antfly shared
inference. The alternative Vertex profile requires explicitly scoped Google
identity. Colony's managed external gateway currently advertises text generation and
rejects image requests whose spend cannot be bounded. Do not point an OCR reader
at that gateway and assume it works. Managed image billing/capability qualification,
BYO Vertex connection UI/CLI, and Document AI OCR are separate follow-ups.

## Native artifact grep

`grep` writes a binding to the existing corpus. It performs no extraction, TXT
export, derived-bucket upload, or source re-ingestion:

```sh
./grepstein grep --state ./grepstein-corpus --url "$ANTFLY_URL"
antfly-grep --table-config ./grepstein-corpus/grep.json 'pattern'
antfly-grep --table-config ./grepstein-corpus/grep.json --artifact chunks 'pattern'
```

Qualification streams the sealed manifest through at most four concurrent
workers by default. Set `qualify --concurrency 1` for a constrained instance or
choose another limit from 1–32. Each worker still verifies the current source
and raw native completion evidence; an error cancels sibling requests and the
receipt remains incomplete. Receipts include the configured concurrency and
elapsed seconds. This does not alter extraction/model concurrency.

The default `extracted` stream reads `document_units_v1` through
`document_units_text`. Each native extraction unit is a matching boundary; a
regex never spans pages or transcript units. The optional `chunks` stream uses
`document_text`; its semantic overlap is deliberately excluded from default
lexical search. Both use the same originals, extraction, revision fencing and
artifact lifecycle as the explorer and semantic/graph indexes. Native index
readiness is checked separately from source publication, including failed OCR.

The binding maps the corpus's existing fields to source identity and permissions.
New prepared rows include `indexed_at` and private `user:self` reader metadata;
regenerate and publish older prepared rows before using the new binding. Preparation
saves one `indexed_at` timestamp in `sources.json`, so resumed rows retain the
same manifest bytes. Older preparation states without that timestamp require a
new `--state`; do not edit sealed manifests to fabricate it. Local
principal filtering narrows results; exposed APIs still require their own current
caller authorization/RLS. The ordinary Antfly API and binding format are the
same in Cloud and self-hosted deployments. Never distribute a write/admin key
with the binding.

The former `--grep-table`, `--gcs-projection-bucket` and page source-job workflow
are retired. Existing projection tables/buckets are not automatically deleted.

### Local native smoke test

Build both binaries from the same checkout, then run the real PDF-to-grep path:

```sh
(cd zig && zig build antfly grep -j2 -Doptimize=Debug)
(cd examples/grepstein && GOWORK=off GOEXPERIMENT=simd \
  ANTFLY_GREPSTEIN_E2E_BINARY="$PWD/../../zig/zig-out/bin/antfly" \
  ANTFLY_GREPSTEIN_E2E_GREP_BINARY="$PWD/../../zig/zig-out/bin/antfly-grep" \
  go test -run '^TestLocalNativeGCSExtraction$' -count=1 -v)
```

This starts a standalone Antfly server and a loopback GCS protocol fixture,
extracts an actual digital-text PDF, qualifies the artifacts, and searches both
native extraction units and chunks with the real grep CLI. It checks original
source identity, no content reacquisition during grep, and rejection of
out-of-scope or changed-generation objects. It does not qualify Cloud Workload
Identity or GPU OCR, embeddings, or relationship extraction; those need a live
sample on the deployed shared inference runtime.

## Explorer and hosting

Public launch requires source-policy review and a qualified suppression path for
source rows/artifacts, graph units, canonical grep publications, and original PDF
delivery, including cached evidence paths. Possession of an authorized archived
copy and current official availability are separate publication criteria.
Keep hosting private until those requirements are met.

Qualify limits at every actually exposed API, including direct Cloud endpoints
if enabled. Test broad expensive grep and concurrent requests, query/result
budgets, named-table scope, and denial of writes, private tables, and inference.
Budget exhaustion must reject/throttle or report incomplete results, never a
false complete no-match. Explorer validation alone is not this qualification.
The explorer rejects excess search/status/PDF requests with HTTP 429 and
`Retry-After` once four requests are active per process; its health route remains
available. Browser cancellation and request deadlines propagate to upstream
Antfly operations and GCS reads, allowing cancelled requests to release admission
slots promptly. Cloud Run's replica cap bounds aggregate concurrency. This does not
substitute for direct Cloud API admission controls.

```sh
# ANTFLY_API_KEY must now be a read-only key scoped to the corpus table.
GREPSTEIN_BUCKET=antfly-showcases-originals ./grepstein explore --url "$ANTFLY_URL"
# Build the hosting image from the repository root:
docker build -f examples/grepstein/Dockerfile -t grepstein .
```

The explorer has full-text, semantic, and graph evidence modes. Graph nodes expose
their documents/path evidence on selection. Original PDF requests are resolved
from corpus rows and confined to the configured bucket; ADC storage credentials
and the Antfly key never go to the browser. Cloud Run obtains its key from an
explicit Secret Manager version. Search, OCR, and enrichment still require a live
Cloud qualification run after provisioning; unit tests do not establish coverage
of the downloaded releases.

Start on an ordinary managed Standard/Pro instance with custom resources, not a
new tier. Qualify a small sample first; record source bytes/pages, full-text storage,
index readiness, queue drain, and OCR spend before loading the full v1 set. Increase
CPU/memory/disk through existing instance configuration and scale inference
separately. Indexing large datasets and automatic large-corpus capacity selection
are deferred; their raw originals can be preserved independently.

### Investigating incomplete OCR

A manifest's `merge_status: converged` reports artifact publication, not OCR
success. Inspect `ocr_failed_count` and the raw manifest's
`incomplete_unit_count` and run
`qualify` before declaring extraction complete. A nonempty text index can still
contain embedded PDF text while OCR pages have failed. Failed OCR pages must not
qualify for Antfly's unchanged-source fast path. Transient failures use the
existing per-request retry budget and backoff; exhausted or terminal failures
remain repair debt. Fix the reported cause before reprocessing those artifacts.

Use the existing named-table artifact and repair APIs through the Cloud session
or an instance-scoped key. Reprocessing an artifact preserves the source document;
there is no need to delete its table or index. Keep native repair/reprocess job
IDs and advance their bounded passes until complete; job creation alone is not
proof that a repair finished. If an API response is uncertain, inspect the job or
artifact generation before submitting the mutation again.

Cloud global discovery and named-table requests must resolve the same live user
permissions. A `401` on global discovery with working named-table requests is a
proxy routing/authentication problem. A Cloudflare `403` with error `1010` is a
separate transport rejection. Diagnostic clients should identify themselves with
a descriptive application `User-Agent`, such as `antfly-cloud/grepstein-diagnostics`.
Do not treat either response as evidence that OCR failed or that the GPU is busy.

## Measuring a private sample

The bounded runners use an owned loopback server and never change Cloud tables.
Use the released runtime and the exact model revisions from the example:

```sh
python3 benchmark_inference.py --antfly /absolute/path/antfly \
  --models-dir /absolute/path/models --output ./timings/inference \
  --stages ocr embed extract --ocr-concurrency 2 --budget-seconds 180
python3 benchmark_corpus.py --antfly /absolute/path/antfly \
  --pdf-dir /absolute/path/verified-sample --output ./timings/native
```

Inference timings distinguish cold/warm model use, disable prompt caching, use
distinct embedding/extraction inputs, validate completed work, and record GPU
contention. A failed or truncated page has no completed-page throughput. The
native runner reports extraction time and incomplete units with OCR disabled;
it does not call an incomplete artifact a successful OCR result. Both runners
stop their own processes on exit and accept explicit sample/time/byte limits.
The [Spark measurement](benchmarks/spark-rc14-2026-11-11.md) records the measured
scope and the limits of corpus extrapolation.

Managed source syncs still replace a complete physical generation when inventory
content changes. The [native generation-cloning design](../../docs/design/source-generation-cloning.md)
specifies the separate engine work needed to retain unchanged derived artifacts
without full replacement writes. The qualification and load improvements here do
not implement that engine feature.
