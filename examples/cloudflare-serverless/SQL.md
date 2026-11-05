# Distributed lake SQL on Workers, Durable Objects, Queues and R2

`sql-worker.mjs` hosts the same versioned execution service used by native query
pools. A native Antfly API gateway still plans SQL and exports the exact retained
Iceberg snapshot and accepted WAL cut. The Worker receives signed internal
fragment requests; it does not expose a public arbitrary-plan endpoint.

`wrangler.sql.jsonc` is a separate deployment profile. Configure its R2 buckets,
queue and dead-letter queue, account endpoint, connection aliases and issuer,
and install `SQL_SERVICE_SECRET` as a Worker secret. The secret must match the
native gateway's dedicated SQL service key and differ from principal credentials.
No live deployment is performed by the tests below.

On the native gateway, configure matching state/exchange namespaces, signing
issuer/key, and `ANTFLY_SQL_QUERY_WORKERS='["https://your-sql-worker.example"]'`.
Use `ANTFLY_SQL_TASK_MEMORY_BYTES=8388608` for the hosted execution ceiling and
leave native DNS discovery unset. `ANTFLY_SQL_QUERY_CONCURRENCY` controls total
query fan-out; it is independent of native per-pod admission. Optional
`ANTFLY_SQL_QUERY_LIMITS` supplies a JSON object of fragment budgets, for example
`{"query_exchange_bytes":8589934592,"query_scan_bytes":8589934592}`. Task memory,
query concurrency and maximum output size remain bounded by deployment admission.

The gateway's artifact store locator must exactly match `SQL_CONFIG.artifacts`,
including protocol, connection alias, bucket, prefix, endpoint and TLS. Native
source table objects and retained artifacts must be reachable through the fixed
`SQL_BUCKET_BINDINGS` R2 map. Caller-supplied bucket names never create bindings.
Query workers need source reads, snapshot-pin writes and execution-lane writes;
they do not receive a table publication API. Source credentials remain native
connection references in retained grants; the hosted resolver admits only
explicitly configured source buckets. Keep storage locations and binding identity
stable while retained queries remain live.

## Coordination and recovery

- Each query has a Durable Object coordinator. SQLite stores only its opaque
  query reference and scope plus an alarm. R2 CAS records remain the authority
  for coordinator ownership, task epochs and accepted output identity.
- An alarm is scheduled before each coordinator turn or queue delivery. Queue
  delivery can duplicate or lose acknowledgements; durable pending task IDs
  already exist before delivery. Consumers ACK accepted or terminal records,
  retry active leases and wake the coordinator after completion.
- A bounded round-robin cron page repairs a lost submission-to-alarm handoff.
  It authenticates stored plan bytes against their SHA-256 identity and isolates
  failed queries. The same cron performs bounded expiry cleanup.
- Version-2 query references include the full content digest and expiry. Every
  state object lives under an expiry-first namespace. Delayed PUTs remain
  discoverable after discovery-marker cleanup. Expired namespaces cannot become
  live again, so their deletion does not require an atomic conditional DELETE,
  which the R2 binding does not provide.
- Cancellation is a scope-checked CAS signal that preserves the coordinator's
  lease. The fenced owner stops dispatch, and cancellation wins a later result
  completion. Abandoned gateway waits send this signal with an independent,
  bounded cleanup deadline.

The host follows Cloudflare's documented [alarm recovery](https://developers.cloudflare.com/durable-objects/api/alarms/)
and [explicit queue ACK/retry](https://developers.cloudflare.com/queues/configuration/batching-retries/)
contracts. Durable native records provide the replay and acceptance semantics.

## Resource and execution bounds

One WASM execution slot per isolate reserves 32 MiB, with at most eight queued
calls. Its working-set allocator is capped at 20 MiB and fragment tasks at
8 MiB. Requests have a 55-second deadline, a 900-operation host I/O ceiling and
32 MiB transfer ceiling. HTTP bodies are individually bounded to 1 MiB, with a
shared 2 MiB ingress reservation and a concurrent request bound. The queue
profile processes one task per delivery and permits configurable provider
consumer concurrency.

Typed Parquet scans, retained WAL overlays, scalar predicates, broadcast/shuffle/bucket
joins, accepted outputs and adaptive adjacent partition coalescing share the
native implementation. Authenticated exchange block indices support bounded
range reads. Small immutable byte caches are scoped to physical deployment
bindings; mutable query records and catalog heads are never cached. Lake decode
state remains invocation-private, and native disk caches are excluded from WASM.
Input pins cover the admitted query expiry; coordinator turns do not repeatedly
reopen source catalogs to renew them.

Managed native writes and compaction honor single-column Iceberg `bucket[N]`
specs for `int`, `long` and `string`. Compatible joins use a common divisor of
both bucket counts, prune files before opening Parquet and route copied WAL rows
with the same Iceberg hash. Proof checks all active files against their retained
specs; mixed or unsupported layouts use shuffle. Equality deletes remain global
so updates can move bucket keys and partition specs can evolve safely. Bucket
manifests merge independently by spec while retaining original file sequences.

Oversized accepted shuffle partitions resolve to durable tile-pair tasks over
bounded original map partitions. This also splits a single repeated hot key,
which additional hash bits cannot separate. Pair tasks publish compact match
bitmaps separately from result rows. Deterministic reduction trees OR accepted match witnesses with fan-in at most
32. Finalizers consume a merged root (or at most 32 direct proofs for small
fan-ins) before emitting unmatched outer rows or semi/anti existence results;
residual `ON` predicates contribute evidence only after they accept a pair.
Authenticated immutable pages hold tile descriptors; the query CAS commits only
a bounded layout reference together with the stage transition. A new
coordinator reconstructs the same tasks, and result pass-through references avoid
copying joined rows into another exchange. Sparse partitions still use ordinary
reducers and adjacent coalescing. Resolved tasks, retry/output reservations and
request estimates are checked again before tile dispatch.

Source task splitting uses canonical row ranges within Parquet row groups and
estimates retained column values instead of compressed bytes alone. A large
single row group can therefore execute across multiple bounded tasks.

Completion scans at most 64 accepted task records per turn and checkpoints a
constant-size reference to immutable linked result pages. Large result handles
contain a `manifests` head instead of expanding every output into `pages`.
Gateways stream the authenticated page chain without buffering the whole result
manifest; `ORDER BY` remains in the normal SQL engine.

Each tile remains subject to task memory, scan, output and runtime limits. Large
Cartesian results and excessive tile grids fail explicit query budgets. This
profile does not claim unbounded join support or a
cloud throughput qualification.

## Verification

```sh
ANTFLY_ZIG=/path/to/pinned/zig npm run test:sql
ANTFLY_ZIG=/path/to/pinned/zig npm run test:sql-workerd
```

The runner exports a fresh native fixture containing real typed Parquet, a
retained snapshot and accepted WAL delete/upsert images, then builds and
instruments WASM. Tests execute that fixture in a fresh WASM instance, lose a
completion response and verify accepted-output recovery. The row-range fixture
executes a 16,384-row single-group source with narrow per-task output budgets. A separate kernel stress
fixture seeds 153,600 accepted build rows across 300 hot build tiles, then executes
portable tile joins, compact match proofs and global NULL/unmatched finalization.
A 1,000-page completion regression checks lost checkpoint acknowledgements and
coordinator takeover. Hosted execution tests enforce the production 900-operation
cap throughout. Control tests cover
service JWT validation, queue retries, alarm recovery, bounded discovery and
cache scope. Workerd tests exercise actual R2, Queue and Durable Object bindings,
progress without client turns, cancellation and result reuse after restart.
Wrangler is invoked only with `--dry-run`; mutable test state stays under the
worktree's ignored `dist/` directory and is removed after the test.


## Bounded input fan-in and retained WAL

Ordinary shuffle inputs and broadcast builds use fan-in-32 compaction trees when
fragment counts exceed the direct-read threshold. Hot partitions retain their
original map tiles; ordinary reducers consume compacted roots. Resolved task
and output reservations include these stages.

Large retained WAL cuts use immutable image pages plus a Bloom filter and exact
sorted key pages. Workers load only their assigned image interval. Each base
split first scans only keys, sort candidate digests, stream each relevant exact-key page once,
and retain a suppression bitmap in the task memory budget. They then stream the
visible payloads from the same pinned split; hidden payloads are never retained.
Binary SHA-256 pages preserve exact membership while halving index transfer
bytes. All base splits still suppress the complete changed-key set.
The gateway includes WAL bytes and rows in its estimates and ensures each
fragment owns at most 128 recent WAL images. Committed base scan sizing remains
independent, avoiding tiny base fragments for a small paged WAL. Workers validate
that image bound against the cut. Candidate buffering and page decoding share
the task memory budget; hosted turns allow at most 900 operations and 64 MiB of
transfer, covering compact indexes for the retained cut size limit.
Paged cuts fall back from bucket execution to shuffle.
The cut's domain, expiry and content hashes authenticate every child page.

Hosted regressions cover 320-fragment ordinary shuffle and broadcast builds
under the 900-operation cap, plus 9,000 retained WAL changes, including a large
individual row, applied over a 16,384-row pinned Parquet snapshot with an 8 MiB
task memory ceiling.

Coordinator alarms arm recovery before durable-state reads. The firing alarm is
consumed by the runtime; handlers preserve automatic retry registration rather
than deleting it on entry. Empty and terminal coordinators explicitly retire
future alarms. Discovery reads at most four queries concurrently from each
16-query page and validates compact v2 record identity, expiry, status and tenant
before waking a coordinator. Only legacy unscoped records require downloading
and authenticating the full immutable plan.
