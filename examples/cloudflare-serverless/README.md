# Antfly on Cloudflare Workers

This example hosts an application and Antfly's object-backed serverless engine
in a Worker. It uses the existing document facts, text index, manifest, ownership
fencing, and publication code. Worker memory contains cache and scratch only.
Related design: [#1278](https://github.com/antflydb/antfly/issues/1278) and
[hosted runtime design](../../docs/design/cloudflare-serverless.md).

It is a bounded deployment profile of the unreleased serverless engine. Local
Node and workerd tests cover durable reopen and publication. A live Cloudflare
deployment, workload qualification, physical garbage collection, vector
freshness, and automatic enrichment are **not** established by these tests.

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
| `search` | additionally `fresh:true` or `required_visibility` | Merges published text candidates with recent mutations before limiting; suppresses stale hits and admits newly matching documents. Returns observed `visibility`. |
| `publish` | none | Runs one fenced publication prefix. Returns decimal-string `version`, `wal_end_lsn`, and `has_more`. The next invocation resumes from the committed watermark. |
| `latest` | none | Returns the latest committed visibility token. |

Use `required_visibility` from a write or earlier read to preserve per-database
monotonic visibility across instances. Tokens encode a full u64 LSN and
incarnation hash without JavaScript numeric rounding. A fence that cannot be
satisfied fails explicitly. The host never discards an oversized tail and
claims freshness. A cursor is valid only for `list` with the same limit and
incarnation. An expired generation or retained WAL prefix fails explicitly.
There is no cross-table snapshot or transaction guarantee.

Fresh search currently admits simple text search with the existing
document-local term-frequency scorer. Corpus-dependent ranking, exact counts,
aggregations, vector/hybrid freshness, custom filters, and ranked pagination
need their own contracts and are not exposed by this example. The generic
ordered overlay supports predicates before limiting and refills pages, but
this administrator demo has no per-document authorization policy.

## Budgets and maintenance boundaries

- One suspended WASM execution lane per isolate; competing calls receive 503
  with `Retry-After`. Instances may run concurrently and arbitrate durable
  writes with CAS and the existing publication fences.
- WASM linear memory is capped at 64 MiB; immutable JS cache at 8 MiB/128
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
- Interactive overlay: at most 4,096 WAL records/8 MiB. Publication: at most
  256 records/4 MiB of WAL input per invocation, with smaller prefixes retried
  when the byte bound is exceeded. These input bounds do not promise that every
  possible index build fits Workers CPU or memory; large workloads fail and
  need a different maintenance profile.

Publication and immutable content reuse come from the native engine. The
portable indexed WAL copies only affected tree paths, and indexed tail reads
do not download historical payloads. The warm cache stores copied immutable
bytes and plain metadata; it never retains request-owned R2/DO handles or caches
mutable HEAD/progress records.

Hosted physical garbage collection is deliberately unavailable. Logical WAL
truncation preserves high watermarks and idempotency receipts but does not
reclaim historical pages. Superseded index artifacts and failed-attempt pages
are retained. Do not install automatic R2 lifecycle deletion on these keys.
Collection needs writer fencing, reader pins, retention policy, and durable
incremental progress before deletion can be enabled. This is a remaining
operational capability of #1278, not a claim of bounded lifetime storage.

## Workers AI and AI Gateway

Optionally set `AI_MODEL` to a Workers AI model ID beginning with `@cf/`, and
`AI_GATEWAY` to a configured gateway ID. `POST /ai` passes its JSON task input
to `env.AI.run(model, input, {gateway:{id}})`. Choose a model supporting that
task's native input/output contract. With no model/gateway configured this
route returns 503. Local tests do not make billable AI calls.

This route is a host adapter for application AI requests. It does not wire
automatic database embedding/extraction into the builder. Typed Antfly
Cloudflare provider integration is tracked separately in
[#1271](https://github.com/antflydb/antfly/issues/1271). Model weights are not
bundled with the database WASM module.
