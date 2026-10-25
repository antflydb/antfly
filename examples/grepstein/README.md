# Grepstein

Grepstein is the GCS-backed Antfly Cloud public-records showcase tracked in
[Colony #818](https://github.com/antflydb/colony/issues/818). The executable uses
Antfly's native `document_extraction` artifacts. It has no DocsAF dependency.

V1 acquires DOJ datasets **1–7 and 12**. Datasets **8–11 are deferred** and the
downloader refuses them. The court collection can be supplied as an existing
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

Provision the resources in Colony's `infra/showcases` first. The uploader uses
ADC for the create-only publisher identity. Antfly's source-reader identity is
separate; it needs read access to the originals bucket.

```sh
./grepstein publish --state ./grepstein-corpus --bucket antfly-showcases-originals
# Set ANTFLY_API_KEY securely to a write key for this instance/table.
./grepstein load --state ./grepstein-corpus --url "$ANTFLY_URL" \
  --create-table --num-shards 4 --reader-config vertex-reader.example.json
./grepstein wait --url "$ANTFLY_URL"
./grepstein stats --state ./grepstein-corpus --url "$ANTFLY_URL"
```

Edit the reader example's project/location/model for the approved deployment.
The **Antfly runtime** needs ADC/Workload Identity permission to invoke Vertex in
that project. Logging in with the local Antfly CLI does not configure remote
workers. Reader configuration rejects inline API keys/tokens. For a purely
digital-text pilot, omit `--reader-config`; scanned pages will still need OCR.

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

The Cloud default `v0.2.5` runtime is not qualified for this workflow: its table
response lacks the stable `table_id` required by resumable loading. Live testing
accepts the corrected combined table request, including the public schema's
`simple` text analyzer. The server creates its system-owned default text index;
the client declares only its named artifact index. Native PDF extraction works,
but that is insufficient qualification of the resumable load. Deploy a
reviewed runtime containing this PR's API and reader changes, then qualify the
actual load, readiness, and artifact contracts before importing the corpus. Do
not bypass table identity checks or infer complete coverage from accepted rows.

## Semantic and graph stages

```sh
./grepstein enrich --url "$ANTFLY_URL" --stage semantic
./grepstein wait --url "$ANTFLY_URL" --index embeddings
./grepstein enrich --url "$ANTFLY_URL" --stage graph
./grepstein graph --state ./grepstein-corpus --url "$ANTFLY_URL"
./grepstein wait --url "$ANTFLY_URL" --index full_text_index_v0
./grepstein wait --url "$ANTFLY_URL" --index autograph_relations
```

Use a qualified registered embedding model with 512 dimensions and a registered
GLiNER extractor. Default inference routing is inherited from the Antfly instance;
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
lease. Colony owns their UUIDs, catalog rows, readiness, and revocation policy.
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
available. Cloud Run's replica cap bounds aggregate concurrency. This does not
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
separately. Large datasets and automatic large-corpus capacity selection are deferred.
