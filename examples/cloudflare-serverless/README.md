# Antfly on Cloudflare Workers

This example hosts an application and Antfly's object-backed serverless engine
in a Worker. It uses the existing document facts, text index, manifest, ownership
fencing, and publication code. Worker memory contains cache and scratch only.
Related design: [#1278](https://github.com/antflydb/antfly/issues/1278) and
[hosted runtime design](../../docs/design/cloudflare-serverless.md).

It is a bounded deployment profile of the unreleased serverless engine. Local
Node and workerd tests cover durable reopen, publication, resumable collection,
and conditional enrichment. Live qualification uses separate disposable
resources; local tests do not establish deployed runtime budgets.

## Antfly-owned R2 coordination

For a fresh database, `npm run deploy:r2` selects `wrangler.r2.jsonc` and
`R2CommitCoordinator`. Antfly persists metadata, job state and both collectors'
frontiers in R2 using the shared native ordered page engine and immutable
commit log. The actor uses no application SQLite/KV storage. The example uses
separate Worker, bucket, Queue and incarnation names to avoid changing an
existing database's authority accidentally.

The original `wrangler.jsonc` remains the SQLite compatibility profile. Existing
data requires the frozen migration below; changing only the class binding does
not migrate metadata.

The shared Lite Zig exports are `object_database` and `object_commit_log`.
These expose object-native transactions; local `.aflite` and SQL/LSM APIs retain
their existing storage semantics. Full architecture and failure cases are in
[the backend design](../../docs/design/antfly-object-storage-backend.md).

`collect_metadata` drives bounded collection of the coordinator's own pages and
commit history. Queue/cron recovery schedules it when `COORDINATOR_BACKEND=r2`.
Application `collect` continues to trace document/index artifacts separately.
Acknowledged writes are durable before Queue admission. R2 latency and request
cost remain part of the commit path; qualify the target workload before rollout.

For migration, preserve the old `CommitCoordinator` class, bind it as
`LEGACY_COMMITS`, add the new R2 class with a new migration tag, and point
`COMMITS` at it. Keep `ANTFLY_INCARNATION` and the R2 bucket unchanged. Set
`COORDINATOR_MIGRATION_REQUIRED=true` and a stable `COORDINATOR_STORAGE_ID`.
Then submit the authenticated request:

```json
{"op":"migrate_coordinator"}
```

It freezes the source, waits for in-flight work, imports bounded batches and
activates the target. Queue deliveries resume it; without a Queue, repeat the
operation until `done`. Normal target operations remain blocked until cutover.
The source stays frozen afterward in migration-aware releases. A rollback must
retain the freeze/authority checks; pre-migration code is not a supported rollback
target. Its records are preserved for inspection; automatic migration does not delete them.


## R2 performance and tenant routing

The native profile opens one metadata snapshot per query, reuses durable
reader pins, executes whole mutations in the writer, coalesces publication
messages and prioritizes interactive metadata work. Immutable manifests/pages
and bounded decoded rows remain warm. Recovery overlaps independent reads.
See [the performance design](../../docs/design/cloudflare-performance.md).

`PACK_SMALL_BODIES=false` disables creation of new body packs. Facts v10 combines
body-pack references with media audit and source-provenance fields. Readers and
collectors must support v10 before builders publish it. Deployed 512-byte v3/v4
roots and main's 576-byte v4–v9 roots remain readable. Disabling packing does
not make a v10 root readable by an older runtime.
The fresh R2 profile enables application GC; metadata GC uses revision growth,
observed reclamation yield and elapsed time to avoid repeated idle passes.

Configure up to 32 independent tenant routes with `ANTFLY_TENANTS`:

```json
{"north":{"incarnation":"north-v1","storage_id":"north-store-v1"}}
```

Authenticated operator requests select one with `X-Antfly-Tenant: north`.
Unknown/colliding routes are rejected; Queue and cron dispatch stay scoped.
This is storage routing, with no cross-tenant transactions. End-user identity
and authorization must be enforced by the app before selecting a route.

`qualification/performance.mjs RUN_JSON HTTPS_ORIGIN` records 100 warm fenced
reads and eight concurrent writers. Run again with `--reopen` after replacing
the coordinator namespace against the same R2 data. Reports include retries
and exact-body/count checks, not just successful-attempt timings.

## Build and test

Use the repository's pinned Zig 0.17 toolchain and Node 24:

```sh
cd examples/cloudflare-serverless
npm ci --ignore-scripts
ANTFLY_ZIG=/path/to/zig npm run test:native
ANTFLY_ZIG=/path/to/zig npm run build
npm test
npm run test:workerd
```

`build` compiles `zig build serverless-wasm` and instruments its asynchronous
object-store and typed inference imports with Binaryen 132 Asyncify. The npm Binaryen
implementation can take several minutes. Set `ANTFLY_WASM_OPT` to a native
Binaryen 132 `wasm-opt` for faster packaging. Use a trusted release and verify
its published checksum. `dist` includes the module and license notices.

`test:workerd` bundles with Wrangler's **dry run**, then uses local Miniflare R2
and SQLite Durable Objects, with persistence paths shared across three complete
runtime restarts. It needs permission to bind localhost ports. It does not
deploy or invoke a model. `build -- --wal-only` builds a smaller storage ABI
fixture for transport development; the complete application tests need the
default full runtime build.

## Configure and deploy

Create the resources in your Cloudflare account:

```sh
npx wrangler r2 bucket create antfly-serverless
npx wrangler queues create antfly-maintenance
npx wrangler queues create antfly-maintenance-dead
npx wrangler secret put API_TOKEN
```

Set `ANTFLY_INCARNATION` in `wrangler.jsonc` to a unique, permanent physical
database ID before first use. Keep it unchanged across deployments. For a new
empty database, choose a new ID. Reusing an old ID reopens the old database;
changing its storage placement can fork its write authority.

For local development, put `API_TOKEN=...` in an ignored `.dev.vars`, then run
`npm run dev`. Build first. The page at `/` lets you add documents, list them,
and search recent writes. All `/api` and `/ai` requests require the bearer
token. This example uses a single database and administrator token; an app with
multiple users must authenticate and authorize every operation and page, and
bind its own tenant/table incarnation. A cursor or visibility token grants no
access. Clients cannot select the R2 binding, prefix, or incarnation.

Deploy with `npm run deploy` after configuring the resources and secret. The
SQLite Durable Object migration is declared in Wrangler. To back up or recover
this example, preserve **both R2 and the Durable Object's state**. The object
holds the authoritative WAL HEAD and progress/fencing metadata; R2 holds the
immutable pages, manifests, and serving artifacts. Never switch an existing
database between coordinated and R2-only metadata placement. The portable WAL
also works against R2 conditional writes alone, but the example avoids repeated
overwrites of a hot R2 HEAD by coordinating small metadata in the Durable Object.

The Queue runs bounded publications. Accepted writes enqueue work; Queue
delivery retries on failure, and a scheduled trigger repairs the crash window
between durable WAL acceptance and queue admission. Monitor the dead letter
queue and retry failed work after addressing its cause. Duplicate jobs are
safe. No correctness guarantee depends on `waitUntil` or an in-memory job list.

## API and consistency

Send JSON to `POST /api` with `Authorization: Bearer <API_TOKEN>`.

```json
{
  "op": "mutate",
  "operation_id": "client-generated-stable-uuid",
  "timestamp_ns": "0",
  "mutations": [
    {"kind": "upsert", "doc_id": "message/1", "body": "{\"text\":\"hello world\",\"read\":false}"}
  ]
}
```

Mutation bodies are JSON **object strings**. Delete with
`{"kind":"delete","doc_id":"message/1"}`. One batch commits atomically and
returns `committed: true` and a visibility token. Reuse the same operation ID,
timestamp, and payloads when retrying an ambiguous network failure. Reusing an
ID with different content fails. A scheduling failure preserves the accepted
receipt and returns `maintenance_pending: true`. An acknowledgment establishes
WAL durability; index publication occurs separately.

| Operation | Request fields | Behavior |
| --- | --- | --- |
| `lookup` | `doc_id`, optional `required_visibility` | Published point lookup plus the bounded canonical WAL tail, including before first publication. Returns `body` or null and observed `visibility`. |
| `list` | `limit` (1–100), optional `cursor`, `required_visibility` | Ascending document-ID listing. Pins the published generation and WAL cutoff in the cursor. Later writes and publications do not change that page sequence. |
| `search` | `text`, `limit` (1–100) | Searches the published text index. |
| `search` | additionally `fresh:true`, scalar `filters`, or `required_visibility` | Merges published text candidates with recent mutations before limiting; suppresses stale hits and admits newly matching documents. Returns observed `visibility`. |
| `count` | optional `required_visibility` | Exact current document count as a decimal string, including unique recent updates and tombstones. Filtered counts are not supported. |
| `status` | none | Accepted visibility token, published watermark, and published pending-embedding count. |
| `compact` | none | Run the existing native compactor on the published head, subject to deployment admission limits; unpublished WAL remains separate. |
| `collect` | optional `job_id` | Runs one durable GC step; returns phase, job ID, continuation and any grace-period delay. |
| `enrich` | none | Runs a configured native embedding batch with source validation and durable progress. |
| `publish` | none | Runs one fenced publication prefix. Returns decimal-string `version`, `wal_end_lsn`, and `has_more`. The next invocation resumes from the committed watermark. |
| `latest` | none | Returns the latest committed visibility token. |

Use `required_visibility` from a write or earlier read to preserve per-database
monotonic visibility across instances. Tokens encode a full u64 LSN and
incarnation hash without JavaScript numeric rounding. A fence that cannot be
satisfied fails explicitly. The host never discards an oversized tail and
claims freshness. A cursor is valid only for `list` with the same limit and
incarnation. An expired generation or retained WAL prefix fails explicitly.
There is no cross-table snapshot or transaction guarantee.

Fresh text search supports scalar predicates such as
`"filters":[{"field":"state.read","eq":false}]`. Dot paths address nested
objects; `$id` addresses the document ID. Up to 16 scalar equality clauses are
ANDed against current bodies, before offset/limit. Missing fields do not match.
The generic native predicate contract can enforce host-owned authorization;
caller-provided filters are not an authorization policy.

Fresh text results include an opaque `cursor`. Resume with the same text,
filters, limit, and cursor. Ranking uses the existing document-local term
frequency scorer and document-ID tie breaking. Ranked pagination and predicate
refill admit a prefix of at most 4,096 candidates; exhausting that budget fails
explicitly. Cursors pin the original generation and cutoff across publication.

Published vector and hybrid search use `"mode":"vector"` or `"hybrid"`, with a
`vector` array and optional text. A visibility fence requires that publication
covers the requested token and configured embeddings are ready. `fresh:true`
also requires publication to cover the current accepted watermark. Otherwise
an explicit visibility/publication/embedding-pending error is returned. These
reads wait for the indexed generation; they do not score unindexed vectors
from a WAL overlay. Filters and ranked cursors are currently text-only.
Corpus-dependent ranking, filtered exact counts, and aggregates remain outside
this admitted profile.

## Budgets and maintenance boundaries

The following local-lane limits describe the compatibility profile. The R2
configuration now uses tenant reader/build actors and module-wide bounded kernel
pools: two 16 MiB reader slots, one 16 MiB metadata slot and one 32 MiB build/WAL
slot, plus shared 8 MiB immutable cache, up to 4 MiB of charged decoded-row entries and 8 MiB queued
encoded inputs. Readers cap native working memory at 8 MiB. Actor queues admit
at most eight calls / 4 MiB per actor with 30-second admission deadlines;
`READERS_PER_TENANT` defaults to two (one through four logical slots). Increasing
logical actors does not increase the physical kernel pool. See the
[design and rollout contract](../../docs/design/cloudflare-performance.md#bounded-tenant-runtimes-and-prepared-wal-commits)
for colocation, GC overlap and aggregate-memory limits. New reader/build actors
use no SQLite or platform storage API.

`EDGE_IMMUTABLE_CACHE=true` optionally retains confirmed immutable objects up to
64 KiB in the Cloudflare Cache API across actor reconstruction. Enable it on
every role of a sealed profile, with the same `EDGE_IMMUTABLE_CACHE_SCOPE` UUID
assigned to this R2 bucket incarnation. Keep that UUID stable across deployments
and rotate it if the bucket is recreated. Caching defaults off; mutable heads, checkpoint floors
and misses still reach R2. Native fencing and GC checks remain authoritative.
Cache entries are disposable, and live latency qualification remains required.

In the split R2 app, `DIRECT_READS=true` runs
HTTP queries in a bounded 32 MiB app kernel pool and obtains fresh read views
from the coordinator. It defaults off. Writes and background work retain their
existing actors; the direct path preserves native visibility and fencing.
`LOCAL_METADATA_READS=true` additionally authenticates committed metadata in the
app, with bounded observer state and coordinator fallback for pin/recovery work.
Qualification cold epochs always use fresh coordinator actors. This optional
profile still needs live qualification.
Small point lookups use hard-capped eight-MiB kernels, allowing four concurrent
reads in that same pool. Explicit capacity limits promote larger point reads
to the general lane using the original snapshot and cancellation deadline.

WAL pages are prepared outside the metadata writer transaction, then accepted
with an original-HEAD ETag check and atomic publication reservation. Background
GC/compaction/reindex admission coalesces duplicate triggers and continuations.
The R2 metadata profile coalesces an idle checkpoint hint after a new
metadata revision and a 250 ms idle interval. It defers behind foreground
work and retries throttling once. Inline native checkpoints retain their
64-commit default. Missed idle hints retain the immutable log's safe recovery
path. Runtime work leases open after physical kernel admission, so a queued
request does not consume its lease while waiting.

The configured Queue concurrency of four allows different tenants to progress;
an executing bounded step can still delay its tenant's next background step.


- Separate serialized interactive and maintenance lanes keep R2 indexing waits
  from occupying the interactive stack. Calls competing for the same lane receive
  503 with `Retry-After`. Instances arbitrate writes with CAS and publication fences.
- Each lane caps WASM linear memory at 32 MiB (64 MiB combined); immutable JS cache at 8 MiB/128
  entries. The isolate's remaining memory must also accommodate JS, R2 streams,
  copied buffers, and assets. The linear-memory cap is not a measurement of
  total isolate usage.
- Lanes are recycled between requests once their linear-memory high watermark
  reaches 24 MiB. `WASM_RECYCLE_BYTES` can lower that threshold to 4 MiB. This
  releases the instance reference for GC while keeping durable state and the
  bounded immutable JS cache. An invocation can still exhaust its 32 MiB cap;
  Queue retry then uses a fresh instance. Recycling does not extend a request's
  live allocation allowance or establish total isolate memory usage.
- Full-runtime requests use the native working-set allocator with a 20 MiB default
  limit on live requested allocations. `WORKING_SET_BYTES` can lower it to at
  least 4,096 bytes; the runtime rejects values above 20 MiB. Exhaustion returns
  `HostedWorkingSetBudgetExceeded`. Allocator padding, ABI transfer buffers,
  envelope parsing/result handoff, the suspension stack, and JS memory are
  outside this counter and remain
  subject to the separate WASM and transport caps.
- Request JSON: 1 MiB. Storage operation count: 1,024. Aggregate object header,
  upload, and download traffic: 32 MiB. Host deadline: 30 seconds, checked at
  I/O and native cancellation checkpoints.
- An object upload defaults to at most 4 MiB; `OBJECT_MAX_PUT_BYTES` can set a
  ceiling up to 8 MiB. A single read is at most 8 MiB. A larger
  serving artifact requires a different admitted workload or transport budget;
  the host returns an error instead of buffering without a bound.
- WAL batches: at most 1,024 records/8 MiB (the HTTP envelope is tighter).
  Indexed tree values additionally have the native page-tree record limit.
- Interactive overlay: at most 4,096 WAL records/8 MiB. Publication: defaults to
  16 records/4 MiB of WAL input per invocation, with smaller prefixes retried
  when the byte bound is exceeded. `PUBLICATION_RECORDS` can set a measured
  ceiling from 1–256. The coordinator durably halves the admitted batch after
  a publication deadline or resource failure; Queue retries use that smaller
  prefix, so eviction does not reset tuning. These input bounds do not promise that every
  possible index build fits Workers CPU or memory; large workloads fail and
  need a different maintenance profile.

`compact` calls the existing native `Compactor`, with host I/O, its existing
work lease, and guarded publication. `COMPACTION_MAX_DOCUMENTS` defaults to
1,000 and `COMPACTION_MAX_ARTIFACT_BYTES` to 4 MiB of manifest-declared artifacts.
These coarse preflight bounds supplement the allocation and transport limits;
they do not bound every transitive body read. Exceeding them returns
`CompactionBudgetExceeded`. An unpublished empty database is a no-op. Set
`AUTO_COMPACT="true"` to dispatch a compaction Queue job hourly. Whole-pass
retries are safe, but a flat-index rebuild has no intra-build checkpoint; making
the WAL input batch smaller does not make every full rebuild fit.

The reference deployment has one database incarnation and one maintenance
consumer (`max_concurrency: 1`). Native publication already serializes that
incarnation; parallel Queue deliveries otherwise contend for the same lease
and spend retries without useful work. Interactive requests still use their
own lane. A multi-incarnation deployment needs a deliberate scheduling profile.

The [native reuse audit](../../docs/design/cloudflare-native-audit.md) identifies
which parts already exist and the actual remaining execution boundaries.

Publication and immutable content reuse come from the native engine. The
portable indexed WAL copies only affected tree paths, and indexed tail reads
do not download historical payloads. The warm cache stores copied immutable
bytes and plain metadata; it never retains request-owned R2/DO handles or caches
mutable HEAD/progress records.

## Larger indexed workloads

Set `PAGED_TEXT="true"` to publish text indexes as authenticated copy-on-write
trees, reusing the native page tree, document facts, publication fencing, and
retention protocols. Text changes update only touched projections and posting
paths. Text-only inserts/deletes no longer trigger vector or sparse rebuilds
when those projections are absent. Existing flat indexes also update postings
without re-reading unrelated document bodies, though rewriting the flat text
artifact still grows with its size.

Paged text uses text artifact metadata version 2. Deploy compatible readers
and collectors before enabling it. The setting is sticky in subsequent WAL
publications once a paged root exists; disabling the variable does not downgrade
existing roots. A small flat index can migrate during publication. Migrating a
large existing flat index can use the resumable reindex job below. Definition
changes can still require full-view builds.
Old ranked cursors retain their original generation and remain readable until
normal retention expires. Collection traces both document and posting trees.

For fully paged text/vector/sparse generations, `compact` validates roots and
returns a no-op;
publication has already updated the serving trees. It does not assemble a
full-corpus body snapshot. Common-term queries still scan matching postings and
retain document scores under request budgets; final ranking retains only the
requested offset plus limit. Paged storage remains subject to request budgets.
Application joins and production concurrency qualification remain separate work.
See [capacity observations](../../docs/design/cloudflare-native-audit.md).

Set `PAGED_PROJECTIONS="true"` to enable page-based dense and sparse indexes
and paged text together. Sparse changes replace only touched document/term
paths. Dense changes rebuild bounded hash-prefix shards (1 MiB of raw
projections or 1,024 entries) using the native clustering, quantization and
reranking implementation. Routing centroids and cluster payloads are separate
immutable objects; queries fetch selected payloads. Repeated chunks for the
same document retain their native best-score behavior. These roots also use
artifact metadata version 2, require compatible readers and collectors, and
remain sticky after the environment setting is removed.

Hash sharding changes clustering topology. Qualify ANN recall and latency for
your embedding distribution and `num_probes`/`search_effort` settings. Centroid
routing still scans the routing tree, common sparse terms retain accumulated
scores, and existing operation/transfer/working-memory limits apply. This does
not establish a production capacity or recall target. In the 10,000-row random
vector fixture, mean recall@10 was 0.18 at eight probes and 0.66 at 32; qualify
higher probe/effort settings before using this profile in production. Visiting
all routing clusters reached 0.84 in that small sample; native candidate
selection and score ties still differ from a raw-cosine brute-force reference.
Explicit flat-generation
clustering policies are rejected in paged mode.

Immutable uploads use batches of at most eight objects with at most four host
uploads in flight. Every object and byte is charged to the original request
budgets; failures join started uploads before returning. HEAD and progress
writes retain their existing transactional authority.

## Resumable bulk reindex

For an existing native document-facts generation, start a rebuild with:

```json
{"op":"reindex","reindex_action":"start"}
```

The result contains a `job_id`. Maintenance Queue messages drive bounded steps;
cron also recovers a checkpoint whose Queue enqueue failed. Without a Queue,
call steps explicitly using `reindex_action: "step"` and `reindex_job_id`.
Use `reindex_action: "status"` to inspect the active job, or `"cancel"` with its
job ID to cancel it. Duplicate/stale steps and cancellation cannot create or
cancel a successor job. A start during an active job returns that job.

The job scans a frozen facts generation, hydrates at most 4 MiB of input bodies
per pass, and builds fully paged text/vector/sparse roots. It catches up through
bounded WAL prefixes while ordinary writes and publications continue, then
publishes with the native lease/fenced HEAD CAS. A lost HEAD response is
acknowledged by walking publication lineage in bounded pages. Immutable
checkpoints and their pointers remain valid across Worker eviction. Progress
pointer CAS also compares the live HEAD ETag in the same coordinator SQL
transaction, so a stale worker cannot install a checkpoint after takeover.

Collection retains the source generation, unfinished roots and required WAL.
An inactive checkpoint expires after ten minutes; expired/stale Queue messages
finish without resurrecting it, and an explicit new start rebuilds from the
current source. The API rebuilds the current definition; changing definitions,
external bases and graph rebuilds are outside this hosted job's capability.
`PUBLICATION_RECORDS` also bounds reindex input (maximum 64); resource failures
lower the durable admission budget.

## Collection and format upgrade

`collect` drives a bounded native-format tracer with a SQLite-backed durable
frontier and exact mark set in the coordinator. It fences old publishers,
commits the native manifest retirement floor, then includes every protected
reader pin. It physically compacts expired WAL payload entries in batches,
advances the WAL write epoch atomically, marks both current WAL trees, and
waits 60 seconds before sweeping. Each invocation traces a bounded number of
pages or processes at most 128 listed/deleted objects. A Queue delivery resumes
from the last durable checkpoint after eviction. Duplicate and stale collectors
cannot advance the current job's state.

Scoped artifact and WAL attempt identities are never reused after retirement.
This makes an authorized deletion safe even if its response arrives late; a
fenced writer's late orphan upload is found by the next collection cycle.
Old manifests are deleted only below the irreversible retirement floor.
Unknown artifact formats stop collection before sweeping. Unscoped artifact
objects and above-floor unpublished candidates are conservatively retained.
Idempotency receipts remain durable indefinitely, so their index still grows
with unique operations. Collection does not promise constant lifetime storage.

Set `AUTO_GC="true"` to enqueue collection every 15 minutes. The example keeps
the latest two published generations plus native reader leases (10 minutes,
with the native clock-skew grace). Cursors can explicitly expire after their
retention rights end. No R2 lifecycle deletion should target database objects.

The indexed WAL now writes format 2: epoch-scoped immutable pages and a physical
retained floor. Existing format-1 indexed pages remain readable; the first write
upgrades HEAD. Old module versions cannot write/read the upgraded metadata.
Deploy the new runtime to every client before enabling collection. Collection
is supported only for this coordinated hosted profile with its 30-second
request deadline and bounded clock skew; do not mix unbounded native readers
into the same physical incarnation. The original whole-log native ObjectStore
format remains unchanged. See the [collection safety argument](../../docs/design/cloudflare-hosted-collection.md).

## Workers AI and AI Gateway

Optionally set `AI_MODEL` to a Workers AI model ID beginning with `@cf/`, and
`AI_GATEWAY` to a configured gateway ID. `POST /ai` passes its JSON task input
to `env.AI.run(model, input, {gateway:{id}})`. Choose a model supporting that
task's native input/output contract. With no model/gateway configured this
route returns 503. Local tests do not make billable AI calls.

For automatic database embeddings, configure `EMBEDDING_MODEL` (a Workers AI
embedding model), `EMBEDDING_DIMENSIONS` (default 384), and `AI_GATEWAY`. Publication
records pending work; the Queue runs the existing native enrichment worker,
which batches bounded chunk windows through the provider, captures a finite
pending cycle, persists progress, and conditionally
appends derived mutations only while its source WAL boundary is still current.
The next publication makes those vectors searchable. Model failures never
substitute deterministic vectors. By default each invocation completes at most
one document and one provider window (`EMBEDDING_WINDOWS`, 1–8).
`ENRICHMENT_DOCUMENTS` applies when checkpointing is disabled. Each window admits at most 64
chunks and a conservative dimension-dependent vector-output budget. Completed
windows are checkpointed as immutable, checksum-verified R2 scratch objects
with input/provider identities held in the coordinator. A yielded invocation
releases its native lease and re-enqueues maintenance; a replacement Worker
loads completed windows before calling the model again. Only the complete,
source-validated document is appended to the WAL. Missing cache objects are
recomputed; corrupt objects fail closed. This prevents repeated completed work
across ordinary eviction, but does not promise exactly-once model billing.

Checkpoint admission is at most 4,096 chunks and 8 MiB of raw vectors per
document. Final JSON, derived WAL records, native working memory, I/O and CPU
limits can impose lower bounds. `EMBEDDING_CHECKPOINTS="false"` retains the
previous bounded, non-checkpointed adapter (up to eight windows in one pass).
The coordinator fixes model, dimensions, and `EMBEDDING_REVISION` (default `1`)
for the physical incarnation; changing that profile requires a new incarnation
or explicit migration. The revision identifies deployments of a mutable model.

Scratch checkpoints are never referenced by serving manifests: final vectors
are inline in the derived document. Scheduled maintenance, or the authenticated
`prune_checkpoints` API operation, deletes expired entries in bounded batches,
keeping the current and previous UTC day. A persisted cursor resumes sweeping
payloads; metadata pruning has an independent cursor. Cleanup is best effort,
and cache expiry can cause recomputation during a long job.

The "Save mailbox state" action writes a separate `state/<document-id>` record
with `text:""` and a reference to the message. It does not rewrite or embed the
message body. There is no atomic cross-record update guarantee. Public upserts
strip `_enrichment`, `chunk_preview`, and `chunk_embeddings` so a content edit
cannot carry an old derived readiness marker forward; native enrichment writes
use their own conditional WAL path.
Search filters inspect the message's own current fields; this profile does not
join `state/` records into ranked search. An application combining the two must
read state explicitly and refill its view, or keep query-critical metadata on
the searchable record. The example demonstrates independent state writes and
fenced state reads, rather than a complete unread-mail search implementation.

The `/ai` route is a host adapter for application AI requests. Typed Antfly
Cloudflare provider integration is tracked separately in
[#1271](https://github.com/antflydb/antfly/issues/1271). Model weights are not
bundled with the database WASM module.


## Native metadata scale qualification

`node qualification/r2-scale.mjs 100000` checks 100,000 metadata records, cold
reopen, a wide tree branch, deletion, three metadata collection cycles and every
surviving value. It reports bounded native memory, object operations, transfers
and retained storage. This local fixture does not measure R2 latency or cost.

## Live qualification

See the [recorded live measurements and limits](../../docs/design/cloudflare-qualification.md).

`qualification/prepare.mjs ACCOUNT_ID` creates an ignored, uniquely named
configuration and a mode-0600 API token under `dist/qualification/`. Provision
its R2 bucket and two Queues, install the token with Wrangler, then run
`qualification/snapshot.mjs RUN_JSON` to seal a self-contained, validated bundle.
Deploy that configuration and run `qualification/run.mjs RUN_JSON HTTPS_ORIGIN`.
It exercises real R2/DO/Queue durability, concurrent writers, publication,
native compaction, collection, and a Workers AI embedding through Gateway, and saves latencies
and deployment version IDs. It performs real billable service operations.

The qualification wrapper alone has a token-protected drain route, restricted
to the generated physical incarnation. Disable/remove the Queue consumer
before `run.mjs ... --drain`; then delete the generated Worker, Queues, and empty
bucket. Production `worker.mjs` has no drain route. Reports contain no API token.
A successful fixture and absence of resource-limit errors do not measure total
isolate memory or establish all-workload production qualification.
For the Antfly-native coordinator, prepare with `--r2-coordinator`. After the
ordinary fixture, run `run.mjs RUN_JSON HTTPS_ORIGIN --collect-metadata` to check
metadata collection, preserved counts and harmless late Queue deliveries.

For live paged text qualification, prepare with `--paged-text` and run
`run.mjs RUN_JSON HTTPS_ORIGIN --scale --documents=5000`. The runner validates
edits, deletes, and an old generation cursor against the real services.
After disabling the consumer, `--collect-only` checks live collection on that
scale fixture. `--diagnostics` reports only collector counts, frontier size,
and phase through the authenticated qualification wrapper.

For a fresh incarnation with `EMBEDDING_MODEL` and `EMBEDDING_DIMENSIONS`
configured, `run.mjs RUN_JSON HTTPS_ORIGIN --enrichment --windows` validates a
48-chunk document across provider windows, native compaction, and fenced vector
search. Use a separate incarnation to avoid embedding the larger durability
fixture as part of this small model test.

For local capacity characterization after building, run:

```sh
node qualification/benchmark.mjs --sizes=100,1000,5000
node qualification/benchmark.mjs --paged-text --sizes=1000,10000,50000 --publication-records=64
node qualification/benchmark.mjs --paged-text --sizes=1000 --body-bytes=4096
```

This exercises actual native WASM publication, cold/warm reads, and compaction
over the in-process R2 contract fixture. It writes memory/object counts and
explicit admission failures to ignored `dist/qualification/local-capacity.json`.
It also checks cold point updates, deletion, and replacement compute. It does
not measure deployed R2 latency or total isolate memory.
Defaults match the host's 4 MiB upload cap, 16-record publication prefix,
24 MiB recycling threshold, and 1,000-document compaction preflight. Override
`--max-put-bytes`, `--publication-records`, `--recycle-at-bytes`, and
`--compaction-documents` to compare admitted profiles. For example, the measured
5,000-document compaction case uses `--compaction-documents=5000`; the default
host still rejects that compaction unless configured for it.

Set `LOG_METRICS="true"` to emit numeric structured `antfly.operation` logs for
runtime operations. Samples include outcome, operations, inference calls,
transferred bytes, elapsed/I/O-wait time, working allocation peak, retained
cache charge, and per-lane/combined WASM linear-memory size. Linear memory is
an instance high watermark and may stay large after allocations are freed.
These logs exclude document bodies, API tokens, and model inputs. Metrics
observer failures do not change a durable write's outcome.

## SQLite pressure qualification

The earlier live collector `SQLITE_NOMEM` recovered after deployment replacement.
Cursor draining and retryable HTTP 503 responses remain in place. Coordinator
failures now emit `antfly.sqlite.pressure` for both journal and object RPCs,
with the operation, phase and
best-effort database size; database size is not a memory measurement. Translated native transport errors
and rate limits also return retryable HTTP 503 responses.

`node qualification/sqlite-soak.mjs --local --rounds=32` compares intentionally
undrained legacy mutation cursors with drained cursors in a warm SQLite Durable
Object. The disposable live variant accepts a run manifest and its HTTPS origin.
A 32-cycle live run executed 1,340,160 statements across both modes without
reproducing the error. This isolates SQLite journal work from WASM, R2 and model
heap pressure; the original error's cause remains unconfirmed. See the
[native audit](../../docs/design/cloudflare-native-audit.md) for the evidence.

For the local dense/sparse capacity, cold-resume and recall fixture:

```sh
node qualification/benchmark.mjs --paged-projections --dimensions=384 \
  --sizes=1000,10000 --publication-records=16 --reindex --recall-queries=5 \
  --output=dist/qualification/projections.json
```

This fixture reports local memory, object traffic and wall time separately;
its in-memory object store is outside the native memory budget. Recall is
measured against brute-force cosine neighbors at eight and 32 requested probes and with `search_effort: 1` (all routing
clusters, with native quantized candidate selection and exact reranking).


For a deployed mixed workload, configure tenants `a,b,c,d` and run
`node qualification/mixed.mjs RUN_JSON HTTPS_ORIGIN`. It defaults to 512 actual
documents per tenant and eight rounds of concurrent reads, writes and compaction.
Run immediately after a fresh deployment to record the first request without
warmup, then redeploy and use `--verify` to check the original receipts and data.
Use `qualification/summarize-metrics.mjs TAIL_JSONL OUTPUT_JSON` for observed
per-operation R2 attempts. These reports distinguish first requests, warm
traffic, retry-inclusive latency, WASM memory and unavailable billing/isolate
measurements; see [the performance design](../../docs/design/cloudflare-performance.md).

Repeat just the mixed phase on that corpus with fresh operation IDs using
`--scenario=mixed-final --reuse=mixed`; recover that run using
`--scenario=mixed-final --verify`. The recovery check replays both the latest
write and an original seed receipt, and checks exact payloads and updated text.
For a separate fresh default namespace with Queue delivery externally paused,
`node qualification/publication.mjs RUN_JSON HTTPS_ORIGIN` measures one
64-document publication and verifies all bodies, count and receipt replay.

For cold measurements, rebind to a fresh physical coordinator class while
preserving storage IDs, pause Queue delivery and disable cron. Pass
`--expected-version=WORKER_VERSION_UUID`; the version-only readiness probe does
not warm WASM or a tenant object, and old-version data responses are excluded.


For disposable live qualification, `node qualification/analytics.mjs RUN_JSON`
fetches periodic whole-isolate memory percentiles and scoped R2 analytics from
Cloudflare. It uses `CLOUDFLARE_API_TOKEN` or the existing local Wrangler OAuth
credential only as authorization to Cloudflare's API. Reports contain no
credential. Recent samples can lag; percentiles are not absolute peaks and R2
adaptive request counts are not a finalized bill. Run it before deleting the
disposable namespaces/bucket. `qualification/mixed.mjs` keeps workload timings
separate from these platform measurements.


For a fresh split deployment, create the bucket and both Queues named in the
split configs, then deploy in dependency order:

```sh
npx wrangler deploy --config wrangler.split-writer.jsonc
npx wrangler deploy --config wrangler.split-reader.jsonc
npx wrangler deploy --config wrangler.split-builder.jsonc
npx wrangler deploy --config wrangler.split-app.jsonc
npx wrangler secret put API_TOKEN --config wrangler.split-app.jsonc
```

Set account, tenant/storage identities, model and Gateway policy consistently
across those configs before deployment. Private writer/reader/builder services
have no public API endpoint. The builder owns Queue consumption and scheduled
repair. Split configs use a fresh namespace and bucket; existing R2 authority
requires a quiescent fenced cutover. Do not reuse an active writer identity
across old and new services. See the
[rollout and memory budgets](../../docs/design/cloudflare-performance.md).

For controlled component comparisons and large root-format/GC qualification:

```sh
node qualification/critical-path.mjs
node qualification/r2-scale.mjs 100000 --compact --idle-checkpoint --hot-root
```

These use the actual native kernel with an in-memory R2 fixture; injected delay
is not live latency. For ANN comparisons add `--coarse-probes=32` to the paged
projection benchmark and compare recall, physical GETs and bytes to the default
zero-pruning run. Full effort retains its exact-search control. `LOG_METRICS`
adds correlated bounded critical-path traces, physical I/O waves and decoded
manifest-cache metrics. Re-run `npm run test:workerd` for persisted recovery,
collection and split service admission checks.


The [performance acceptance gate](../../docs/design/cloudflare-gates.md) provides
paired sealed deployments, fixed-arrival soak traffic, fresh physical recovery
samples, exact body/receipt checks and content-addressed ANN/failure evidence.
`qualification/evaluate-gate.mjs GATE_JSON` exits with status 2 while any required
evidence or threshold is missing. A short smoke run cannot pass this gate.


### Resumable exact vector search

With the R2 backend and paged projections, vector `search_effort: 1` scans the
stored FP32 vectors in bounded native steps. A response with `complete: false`
has no final hits; resubmit the same query with its `cursor` until it completes:

```js
let cursor;
let result;
do {
  result = await query({op: "search", mode: "vector", vector, limit: 10,
    search_effort: 1, ...(cursor ? {cursor} : {})});
  cursor = result.cursor;
} while (!result.complete);
console.log(result.hits);
```

The cursor fixes the original generation and visibility cutoff across publication
and actor eviction. It has a four-minute idle window and one-hour absolute
lifetime. Cancellation stops further steps; expired or retired snapshots require
a new query. The app authenticates cursor state before dispatch and uses
`EXACT_SCAN_CURSOR_KEY`, falling back to its existing operator `API_TOKEN`.
Configure signing secrets through Worker secrets. Key rotation invalidates cursors.
The [exact-scan design](../../docs/design/cloudflare-gates.md#bounded-exhaustive-vector-search)
details admission limits and the FP32/FP16 comparison caveat.
