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
  --create-table --num-shards 4 --reader-config florence-reader.example.json \
  --inference-url "$ANTFLY_INFERENCE_URL" \
  --inference-secret antfly.antfly_inference.api_key
./grepstein wait --url "$ANTFLY_URL"
./grepstein stats --state ./grepstein-corpus --url "$ANTFLY_URL"
```

The showcase OCR profile uses digest-pinned Florence-2 on shared Antfly inference.
Set `ANTFLY_INFERENCE_URL` to a server-reachable inference root or `/ai/v1` URL;
`--inference-url` binds the Florence profile and relation extractor to that endpoint.
The Cloud connection uses the existing `antfly.antfly_inference.api_key` server
keystore key. `--inference-secret` stores only a secret reference in the OCR,
embedding and relation configs; self-hosted users select their own key name.
Reader configuration rejects inline credential values. Repeat the endpoint and
secret flags for `enrich --stage semantic` and `enrich --stage graph`.
Without an endpoint override, Antfly's runtime inference configuration owns routing.
For a purely digital-text pilot, omit `--reader-config`; scanned pages will still
need OCR. The Vertex reader example remains available as an alternative.

The relation stage defaults to pinned `fastino/gliner2.5-base-v1`, with entity and
relation schemas and bounded long-document windowing. Colony must deploy its
GLiNER2.5 and Florence GPU routes before the hosted pipeline is qualified. CUDA
kernels alone do not qualify the GLiNER2.5 serving policy: use a runtime with
measured CUDA production profiles, then verify the deployed L4 path. RC.2 rejects
GLiNER2.5 CUDA. Preserve the existing GLiNER2 route for other callers.

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

V1 uses Antfly's **existing S3-compatible remote-content reader against GCS's
XML endpoint**: source rows use `s3://BUCKET/objects/sha256/...`. They are GCS
objects, not AWS objects. Configure a bucket-scoped GCS HMAC read credential in
Colony's existing S3 provider connection, endpoint `storage.googleapis.com`, SSL
enabled, with `buckets: ["antfly-showcases-originals"]`. Colony already publishes
these credentials to Antfly's secret store and hot-reloaded remote-content
configuration. Keep HMAC material out of manifests, index configs, and Pulumi
state. Self-hosted deployments configure that same bucket-scoped `remote_content.s3`
entry with secret references. Direct `gs://` remote artifact fetching is not added
by this example; the existing GCS source connector uses the JSON API separately.

`load` defaults to full text only and resumes durable batches against the exact
manifest, endpoint, table identity, and extraction configuration. Reuse the same
flags on resume, without `--create-table`. A replaced table cannot inherit an old
checkpoint. `wait` requires complete and queryable aggregate **and shard**
readiness; successful submission alone is not success. `grep` and `graph` also
check artifact generations and unit inventories before publishing derived data.

The Cloud default `v0.2.5` runtime lacks the stable `table_id` required by resumable
loading. The dedicated showcase was upgraded to digest-pinned `v0.3.0-rc.2` and
its stable table identity was verified live. Live testing
accepts the corrected combined table request, including the public schema's
`simple` text analyzer. The server creates its system-owned default text index;
the client declares only its named artifact index. Native PDF extraction works,
but that is insufficient qualification of the complete load. Qualify the
actual load, readiness, and artifact contracts before importing the corpus. Do
not bypass table identity checks or infer complete coverage from accepted rows.

## Semantic and graph stages

```sh
./grepstein enrich --url "$ANTFLY_URL" --stage semantic \
  --embedder-config vertex-embedder.example.json
./grepstein wait --url "$ANTFLY_URL" --index embeddings
./grepstein enrich --url "$ANTFLY_URL" --stage graph
./grepstein graph --state ./grepstein-corpus --url "$ANTFLY_URL"
./grepstein wait --url "$ANTFLY_URL" --index full_text_index_v0
./grepstein wait --url "$ANTFLY_URL" --index autograph_relations
```

The showcase Vertex profile selects **`gemini-embedding-2`**, project
`antfly-showcases`, location `global`, and 3072 dimensions. The index, generated
embedding artifacts, and query embedder share the same dimensions/provider.
The stable Google model ID is not `gemini-embedding-002`. The native adapter uses
`embedContent`, one input per request, and paired query/document instructions;
the model does not accept the older `task_type` request field. Binary media
remains in raw storage; this stage embeds the selected PDFs' native text chunks.

The exact `v0.3.0-rc.2` image predates this adapter correction. Keep semantic
enrichment disabled until a reviewed runtime containing it is deployed and a
live query/document embedding probe succeeds. Do not silently substitute the
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
registered 512-dimensional model flags. Use a registered GLiNER extractor for
the graph stage. Default inference routing is inherited from the Antfly instance;
`--inference-url` is an explicit override. Graph materialization retains native
page provenance, detects superseded extraction revisions, and reconciles stale
unit rows. Relationship neighborhoods/path evidence are model outputs requiring
inspection, not verified factual claims.

V1 OCR uses the native Vertex reader with explicitly scoped Google identity.
Colony's managed external gateway currently advertises text generation and
rejects image requests whose spend cannot be bounded. Do not point an OCR reader
at that gateway and assume it works. Managed image billing/capability qualification,
BYO Vertex connection UI/CLI, and Document AI OCR are separate follow-ups.

## Canonical grep in Cloud

The projection copies native page text byte-for-byte, without a second PDF/OCR
pipeline. Each item is **one page**: regex and multiline matches are exact within
that page. Cross-page multiline matching is not promised. Parts retain original
page offsets; grep paths identify the source record and original page.

```sh
./grepstein grep --state ./grepstein-corpus --url "$ANTFLY_URL" \
  --gcs-projection-bucket antfly-showcases-derived \
  --provider-auth-id "$COLONY_GCS_CONNECTION_ID"
# Review grepstein-corpus/cloud-jobs.json, then supply a Colony user access token
# securely through GREPSTEIN_CLOUD_TOKEN. This is not the table's read API key.
./grepstein cloud-jobs --plan ./grepstein-corpus/cloud-jobs.json \
  --jobs-url "$COLONY_INSTANCE_IMPORT_JOBS_URL"
```

The GCS connection reads the derived bucket with a separate read-only Google
identity. The plan uses existing scheduled `source` jobs, one per populated
dataset/hash partition, at most 10,000 pages each. It does not feed binary PDFs
into the text-only source importer. Jobs refresh hourly with a 24-hour permission
lease. Leave `max_objects` unset: an explicit value requests a non-authoritative
subset and cannot promote a searchable catalog or renew its permission lease.
The connector already enforces a 10,000-object hard ceiling. Colony owns job
UUIDs, catalog rows, readiness, and revocation policy.
`cloud-jobs` reuses identical jobs after a lost response and refuses a conflicting
existing job; it does not archive or replace jobs automatically. New extraction
snapshots require a reviewed job lifecycle/config update.

Once Colony reports source jobs searchable, use Antfly grep's ordinary Cloud
source selection. For self-hosting, omit `--gcs-projection-bucket`: `grep` uses the
shared publisher directly and writes `grep-sync/catalog.json`. For example:

```sh
antfly-grep --source-catalog ./grepstein-corpus/grep-sync/catalog.json \
  --source grepstein-doj-12-a 'pattern'
```

Use a populated alias from the catalog. Standalone catalogs need refresh before
their 24-hour lease expires. Do not publish a standalone catalog through Cloud
and expect it to substitute for Colony's source job authorization policy.

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
