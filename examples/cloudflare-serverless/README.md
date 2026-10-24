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

`build` compiles `zig build serverless-wasm` and instruments its single
asynchronous object-store import with Binaryen 132 Asyncify. The npm Binaryen
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

- Separate serialized interactive and maintenance lanes keep R2 indexing waits
  from occupying the interactive stack. Calls competing for the same lane receive
  503 with `Retry-After`. Instances arbitrate writes with CAS and publication fences.
- Each lane caps WASM linear memory at 32 MiB (64 MiB combined); immutable JS cache at 8 MiB/128
  entries. The isolate's remaining memory must also accommodate JS, R2 streams,
  copied buffers, and assets. The linear-memory cap is not a measurement of
  total isolate usage.
- Request JSON: 1 MiB. Storage operation count: 1,024. Aggregate object header,
  upload, and download traffic: 32 MiB. Host deadline: 30 seconds, checked at
  I/O and native cancellation checkpoints.
- An object upload is at most 1 MiB and a single read at most 8 MiB. A larger
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

Publication and immutable content reuse come from the native engine. The
portable indexed WAL copies only affected tree paths, and indexed tail reads
do not download historical payloads. The warm cache stores copied immutable
bytes and plain metadata; it never retains request-owned R2/DO handles or caches
mutable HEAD/progress records.

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
substitute deterministic vectors. Each invocation admits four documents and
at most eight host inference calls. Each model batch admits at most 64 chunks
and a conservative dimension-dependent vector-output budget. Larger documents
fail clearly and remain pending; they need a different admitted chunking profile.
The coordinator fixes the embedding model and dimensions for the physical
incarnation, preventing accidental reuse of vectors after a provider change.
Changing that profile requires a new incarnation or an explicit migration.

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


## Live qualification

See the [recorded live measurements and limits](../../docs/design/cloudflare-qualification.md).

`qualification/prepare.mjs ACCOUNT_ID` creates an ignored, uniquely named
configuration and a mode-0600 API token under `dist/qualification/`. Provision
its R2 bucket and two Queues, install the token with Wrangler, then run
`qualification/snapshot.mjs RUN_JSON` to seal a self-contained, validated bundle.
Deploy that configuration and run `qualification/run.mjs RUN_JSON HTTPS_ORIGIN`.
It exercises real R2/DO/Queue durability, concurrent writers, publication,
collection, and a Workers AI embedding through Gateway, and saves latencies
and deployment version IDs. It performs real billable service operations.

The qualification wrapper alone has a token-protected drain route, restricted
to the generated physical incarnation. Disable/remove the Queue consumer
before `run.mjs ... --drain`; then delete the generated Worker, Queues, and empty
bucket. Production `worker.mjs` has no drain route. Reports contain no API token.
A successful fixture and absence of resource-limit errors do not measure total
isolate memory or establish all-workload production qualification.
