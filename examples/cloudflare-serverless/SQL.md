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

Typed Parquet scans, retained WAL overlays, scalar predicates, broadcast/shuffle
joins, accepted outputs and adaptive adjacent partition coalescing share the
native implementation. Authenticated exchange block indices support bounded
range reads. Small immutable byte caches are scoped to physical deployment
bindings; mutable query records and catalog heads are never cached. Lake decode
state remains invocation-private, and native disk caches are excluded from WASM.
Input pins cover the admitted query expiry; coordinator turns do not repeatedly
reopen source catalogs to renew them.

Bucket-compatible execution/native bucketed writes and distributed recursive
repartitioning/skew tiling are not implemented yet. Oversized hosted tasks fail
their explicit budgets; this profile does not claim unbounded join support or a
cloud throughput qualification.

## Verification

```sh
ANTFLY_ZIG=/path/to/pinned/zig npm run test:sql
ANTFLY_ZIG=/path/to/pinned/zig npm run test:sql-workerd
```

The runner exports a fresh native fixture containing real typed Parquet, a
retained snapshot and accepted WAL delete/upsert images, then builds and
instruments WASM. Tests execute that fixture in a fresh WASM instance, lose a
completion response and verify accepted-output recovery. Control tests cover
service JWT validation, queue retries, alarm recovery, bounded discovery and
cache scope. Workerd tests exercise actual R2, Queue and Durable Object bindings,
progress without client turns, cancellation and result reuse after restart.
Wrangler is invoked only with `--dry-run`; mutable test state stays under the
worktree's ignored `dist/` directory and is removed after the test.
