# Antfly C API

`libantfly` is the stable embedded C ABI boundary for Antfly. Storage layouts
are selected by open options; they are not separate ABIs. Language bindings
should target this API once and expose storage-specific conveniences on top.

The public header, `include/antfly.h`, is Apache-2.0 so bindings can vendor
or transcribe it; the Lite bindings built on it are Apache-2.0 too. The
`libantfly` library and the rest of first-party Antfly are Apache-2.0.

## Naming

- `antfly_*`: library-level calls that take no database handle (ABI version,
  threading mode, errors, options, buffer and result frees, artifact ID
  decoding, restoring a backup to a new path).
- `antfly_db_*`: calls on an `antfly_db *` handle, for any storage kind.
- `antfly_lite_*`: operations on the `.aflite` single-file format itself
  (integrity checks, compaction, vacuum, stable snapshots), plus shortcuts
  that open one with default options.
- `antfly_inference_*`: calls on an `antfly_inference *` handle, which runs
  inference (embed, rerank, chunk, generate, and so on) without a database.

Each operation has one name. ABI version 2 removed the earlier `antfly_lite_*`
duplicates of storage-neutral calls and the separate Lite options struct.

## ABI Contract

- `antfly_abi_version()` returns the ABI version supported by the library.
- Every options struct starts with `abi_size`.
- Callers must initialize options with the matching `*_init` function before
  setting fields.
- Readers of options structs must only read fields fully covered by `abi_size`.
- Reserved fields must be zero when present.
- New fields may be appended to options structs without breaking older callers.
- Handles are opaque `antfly_db *` values and must be closed with
  `antfly_db_close`.
- Returned buffers are owned by the caller and must be released with
  `antfly_buffer_free`.

## Storage-Neutral Open Surface

The primary embedded open surface is storage-neutral:

- `antfly_db_open(path, out_handle)`
- `antfly_db_open_with_options(path, options, out_handle)`
- `antfly_db_create_with_options(path, options, out_handle)`

`antfly_open_options` selects:

- `storage_kind`: `ANTFLY_STORAGE_KIND_DIRECTORY` for a normal Antfly
  directory, or `ANTFLY_STORAGE_KIND_LITE` for a single-file `.aflite`.
- `open_mode`: writer, read-only query, or status-only.
- `profile`: native or hosted/manual maintenance.
- `flags`: `NO_SYNC`, `TTL_CLEANUP`, remote/local inference capability state,
  and generated-enrichment replay.
- storage sizing, TTL cleanup tuning, embedded-inference resource budgets,
  and `busy_timeout_ms`.

Directory storage is the default for the generic open APIs. The
`antfly_lite_open*` / `antfly_lite_create*` shortcuts open a `.aflite` file
with default options; pass `antfly_open_options` with
`ANTFLY_STORAGE_KIND_LITE` for anything else.
`antfly_db_create_with_options` currently provides exclusive create semantics
for `ANTFLY_STORAGE_KIND_LITE` only. Directory storage should use
`antfly_db_open_with_options`, which preserves the existing directory
open-or-create behavior until the directory backend exposes an exclusive create
primitive.

## Backup and Restore

Portable `.afb` backups are storage-neutral:

- `antfly_db_backup` writes a backup of the entire embedded database.
- `antfly_db_import_backup` imports one into an empty database of either kind.
- `antfly_restore_backup_json` and `antfly_restore_backup_file_json` create a
  new database at a path, of the storage kind in the passed
  `antfly_open_options` (NULL means directory storage). The database is built
  and indexed beside the destination, then published atomically; `replace`
  swaps out an existing one. A failed or interrupted restore leaves the
  destination holding either the complete old or the complete new database,
  never a partial or missing one. Replacing a directory database that any
  process has open (through libantfly or otherwise), or a Lite file whose
  writer lease is held, fails with `ANTFLY_BUSY`; so does opening a directory
  database while a restore publishes it. `ANTFLY_OUTCOME_UNKNOWN` means the
  new database was published but its crash durability could not be confirmed.

A backup of either kind restores or imports into either kind.
`antfly_db_status_json` and `antfly_db_capabilities_json` likewise report on
any handle; the status `storage.format` is `"aflite"` or `"directory"`.

## Inference

`antfly_inference_open` starts the embedded inference runtime on its own,
with no database, and returns an `antfly_inference *` handle; close it with
`antfly_inference_close`. `antfly_inference_options` sets the models
directory, the same resource budgets as the `inference_*_budget_mb` open
options, and an optional per-call timeout. NULL options use the defaults.

Each call takes the request JSON and returns the response JSON of the
matching `/ai/v1` route of the inference HTTP API
(`specs/openapi/inference/api.yaml`), dispatched in memory to the same
handlers:

| Call | Route |
|---|---|
| `antfly_inference_embed_json` | `POST /embed` |
| `antfly_inference_rerank_json` | `POST /rerank` |
| `antfly_inference_chunk_json` | `POST /chunk` |
| `antfly_inference_generate_json` | `POST /generate` |
| `antfly_inference_generate_batch_json` | `POST /generate/batch` |
| `antfly_inference_rewrite_json` | `POST /rewrite` |
| `antfly_inference_decide_json` | `POST /decisions` |
| `antfly_inference_extract_json` | `POST /extract` |
| `antfly_inference_read_json` | `POST /read` (OCR) |
| `antfly_inference_list_models_json` | `GET /models` |

For decision question types, answer semantics, and SQL providers, see the
[typed decision guide](../docs/reference/decisions.md).

- Images and audio go inline in the JSON, as base64 or `data:` URIs.
- The `_json` calls return complete responses. To stream,
  `antfly_inference_generate_stream_json` calls a callback with each
  `chat.completion.chunk` as tokens are produced; the callback returns false
  to stop generation, and the call then returns `ANTFLY_CANCELLED`.
- The output buffer holds the response body even when a call fails, so a
  failure carries the runtime's JSON error. Free it either way. HTTP 404
  (for example a model that is not installed) maps to `ANTFLY_NOT_FOUND`,
  other 4xx to `ANTFLY_INVALID_ARGUMENT`, 429/503/504 and an elapsed timeout
  to `ANTFLY_BUSY`, and 501 or 507 (the model does not fit the budgets) to
  `ANTFLY_UNSUPPORTED`.
- Models are not downloaded on demand. `antfly_inference_pull_json`
  downloads one into the handle's models directory, like
  `antfly inference pull`, with an optional progress callback. Returning
  false from the callback cancels the download (`ANTFLY_CANCELLED`);
  completed files stay staged, so pulling again resumes. Close waits for a
  pull in progress.
- Callbacks always run on the thread that made the call, and the work waits
  for each one to return. The runtime works on its own threads and hands
  each result to the caller's thread, so bindings whose runtimes require that
  (koffi for Node, for example) are safe, and returning false is always
  honored: no further chunks are generated, and a cancelled pull never
  installs the model. The callback can only cancel when it is called:
  between generated tokens, or at a pull report (each file's start, every
  16 MiB, and its end).
- Models run in the calling process on every backend; see Inference In
  Process below. If the runtime cannot start, open returns
  `ANTFLY_UNSUPPORTED`.

Inference handles have the same guarantees as database handles (see Thread
Safety): any thread may call concurrently, close waits for in-flight calls,
and a closed handle, or a database handle passed by mistake, is rejected with
`ANTFLY_INVALID_ARGUMENT`. They live in a separate registry with its own
reserved address range on 64-bit POSIX targets, so there the two kinds can
never alias. Each handle owns its own runtime and loaded models; share one
handle rather than opening several.

## Inference In Process

libantfly runs every inference backend, including Metal, CUDA, and ONNX, in
the calling process. This applies to `antfly_inference` handles and to Lite
handles opened with local inference. A dylib-only build needs neither a
separate worker executable nor an `ANTFLY_INFERENCE_WORKER` setting.

The `antfly` server can supervise inference in a worker to enforce hard
deadlines around uninterruptible driver and native runtime calls. It can stop
a stuck call by replacing that worker. Embedded handles keep the application
in control of its process lifetime:

- Once a call reaches the device or driver it runs to completion.
  `call_timeout_ms`, and closing a handle, take effect when it returns.
  Calls on CPU backends still stop cooperatively.
- A GPU driver fault terminates the process.
- A session wedged in the driver blocks close instead of being abandoned.

Compatible native database connections retain one local inference service across
foreground and maintenance snapshots. Refreshing a checkpoint does not reload
models or reset admission accounting; the service drains and closes after its
last retained database snapshot is released. Standalone `antfly_inference`
handles continue to own independent runtimes. Explicit database `models_dir`
values are copied during open and remain valid after the caller releases its
options buffer. Idle maintenance writer admission does not schedule new replay.
An idle turn can retain the serving runtime across its own fenced allocator and
lease cleanup when primary and immutable index roots remain unchanged. An
external writer's commit still requires normal runtime validation and refresh.

`antfly_db_status_json` includes `inference.memory_admission` for a local runtime.
It records the last denied admission's pressure category, requested/admitted
bytes, configured limits, denial count, and monotonic timestamp. These are
historical diagnostic values, not a claim that a current request is blocked.
The limits describe the first request in the denied group; requested bytes are
the group's totals. An unavailable memory sample is reported as zero.

Graph replacement rows in `antfly_db_pending_work_stats_json` expose
`blocked_reason`, `producer_artifacts` (including upstream producers),
`producer_replay_target`, `producer_applied_sequence`, and
upstream retry/error details. Waiting for producer enrichment preserves the
graph's own `last_error` and does not spend its failure retry budget.

## Read-Only Modes

Read-only open modes are part of the storage contract, not just a DB-layer write
guard:

- Lite native files open with read-only file access.
- LSM primary/index backends open physical storage in read-only mode.
- LMDB primary storage opens the LMDB environment read-only and does not create
  missing directories or databases.
- In-memory backends have no physical read-only state, but DB write APIs still
  reject mutations under read-only open modes.

`status_only` should be at least as restrictive as query read-only. It may
avoid starting optional background work where the storage implementation can
support that cleanly.

## Thread Safety

`libantfly` runs in one threading mode, equivalent to SQLite's default
"serialized" mode: any thread may call any function on any handle,
concurrently. `antfly_threading_mode()` reports it as
`ANTFLY_THREADING_SERIALIZED`, like `sqlite3_threadsafe()`, and the Lite
capabilities JSON carries `"threading": "serialized"`.

Embedded database and table handles share one serialized API fence so
local multi-table decisions and catalog changes cannot interleave with other
API calls. Cursor snapshots remain pinned between fetches while other
sessions execute. Managed owner handles retain four access classes:

| Class | Exports | Runs concurrently with |
|---|---|---|
| read | lookup, get_raw, scan, search (JSON, dense, text, wire, hits), graph queries, aggregates, stats, schema/index/enrichment listing, status, capabilities, check, pending-work stats, enrichment extract/compute | everything except exclusive calls |
| write | batch, transactions and intent resolution, compact, vacuum, snapshot | reads and maintenance; one write at a time per handle |
| maintain | run-until-idle, generated-enrichment replay, backup, stable snapshot copy | reads and writes; one maintenance call at a time per handle |
| exclusive | set schema, add/delete index or enrichment, import a backup into a handle, range and split changes, shadow index managers, readable lease hook | nothing; waits for in-flight calls |

Concurrent calls on an embedded database queue behind each other instead
of failing with `ANTFLY_BUSY`. Managed owner reads run against pinned
storage snapshots while a write commits. A search stamps the current document identity generation; if a write
commits before the search re-checks it, the search restamps and retries, and
its final attempt briefly holds off writers so it always completes. A read
with a caller-pinned `identity_read_generation` that has gone stale is
rejected with `ANTFLY_INVALID_ARGUMENT` rather than retried.

A handle value is an opaque id for a slot in a process-wide registry, not a
pointer to the database. Registry slots are never freed and each carries a
generation, so every handle value a caller passes is safe to use at any time:

- `antfly_db_close` claims the slot, rejects new calls, waits for every call
  that has already entered (including calls still waiting for a lock), then
  frees the database and advances the slot's generation.
- A call racing close, or made after it, returns `ANTFLY_INVALID_ARGUMENT`
  instead of touching freed memory. This holds even for a thread paused
  before its call reached the handle.
- Concurrent and repeated `antfly_db_close` calls are no-ops after the first.
- A slot reused by a later open gets a new generation, so an old handle value
  can never reach the new database. A slot that has used every generation a
  handle value can encode is retired for the life of the process instead of
  wrapping, so generations never repeat.

On 64-bit POSIX targets a handle value is also a genuine address inside an
inaccessible region the library reserves (no memory is committed), so it is
at least 4096, aligned, and never inside any allocator's heap. Bindings can
therefore keep it in pointer-typed fields that a garbage collector inspects,
such as Go's `unsafe.Pointer`.

This is stronger than `sqlite3_close`, where using a closed connection is
undefined. Bindings may still track their own handle state to report a
closed handle before crossing the ABI.

Lite embedded handles are independent connections. Any number of writable,
read-only, and status-only connections may remain open to the same file.
Compatible connections share one process storage owner, catalog/index caches,
and, when enabled, a maintenance worker per canonical path. SQL sessions, cursor
IDs, and child handle ownership remain connection-local; closing one connection preserves the
others. Busy timeouts remain per connection. Distinct storage settings use
separate owners under the same file gate.
Opening an existing file does not reserve its writer. Complete native operations
queue on a canonical file gate within a process, and a kernel path lease
coordinates operations between processes. A writable operation owns the lease
through its durable publication; SQL COMMIT retains it through the complete
coordinator/participant recovery boundary. A competing process returns
`ANTFLY_BUSY`, or waits up to `busy_timeout_ms`. Read-only calls use a shared
lease. Calls on writable connections currently use an exclusive lease even
for reads, a conservative policy that serializes those calls across processes.
Read-only snapshots without a lock sidecar also open from read-only directories
or media. Their calls use a shared inode fence when sidecar creation is denied;
every sidecar creator takes the exclusive inode fence before installing it.
Reopened native writers discover pending generated enrichment in the durable
journals of all tables and resume it through their shared maintenance owner.

The busy timeout is an admission retry budget, not a deadline for the whole
operation. Capacity-limited reads can need allocator verification too; they
receive the same pre-execution admission retry as mutations. An expired budget
prevents further contention waits and new attempts after backoff. A detached
allocator proof already in progress can still admit the request after the
budget expires when the tip is unchanged and the path lease is immediately
available. The finished proof remains attached to that verified tip. A changed
tip still requires a new proof; an exhausted budget returns `ANTFLY_BUSY` safely.
A cancelled backoff returns before another lease acquisition or proof starts.
Mutation bodies are never replayed by admission retries.

Ordinary admission retries release registry references between attempts, so a
concurrent close may invalidate the handle before it is reacquired. Prepared
SQL retains its original references until its plan and shared preparation-budget
bytes are freed. Close can therefore wait through SQL backoff or a slow proof,
as with another in-flight call. Those references keep the connection and budget
owner alive, not a foreground runtime generation; API/path locks are released
during backoff so runtime refresh and competing publications can proceed.

`antfly_db_pending_work_stats_json` exposes process-owner lifetime totals under
`background_publications`: `allocator_proof_walks`, `allocator_proof_failures`,
`allocator_proof_ns`, and `allocator_proof_max_ns`. These count completed detached
graph walks from foreground and maintenance work, including failed/cancelled
walks. Wall-clock nanoseconds measure the graph walk itself, excluding queue/lease waits
and reader teardown. Totals survive foreground runtime refresh and are local to
the process, not persistent file-wide counters. Reading diagnostics performs
normal read admission and may itself require a proof on a capacity-limited tip.
The same object reports `mutation_admission_count`, `mutation_admission_ns`,
`mutation_admission_max_ns`, and `mutation_admission_log2_ns`. These measure
successful mutation admission with a positive busy timeout, before the mutation
body runs, including queueing, retries, and proof work. Resuming an operation
after it suspends admission (for example, during inference) records another
admission; these are not counts of unique exported calls. Zero-timeout calls keep
their existing fast path and are not sampled. Reads and diagnostic calls are
excluded. Histogram bucket `i` covers `[2^i, 2^(i+1)-1]` nanoseconds (bucket zero
also includes zero; bucket 63 ends at `UINT64_MAX`), so percentile estimates are
ranges rather than exact timings. A successfully admitted mutation can still
fail later in its body; these counters do not assert mutation success.

After an external commit or atomic file replacement, the shared owner reopens
its cached runtime before the next operation. An in-process handoff between
compatible connections reuses the live runtime. Streaming SQL cursors retain the
runtime and immutable pages they originally pinned; retiring that runtime
releases caches without flushing them over a newer generation. Read-only
connections observe new commits at operation boundaries. Background enrichment and index builds capture one table at a time under the
file gate, then compute outside it. Generated artifacts, private staging, and progress writes reacquire the
kernel writer lease and publish through the current namespace. Source digests,
document incarnations, catalog identity, and lease tenure fence acceptance.
Index file publications may adopt a newer primary checkpoint only while their
immutable index inputs and inode remain unchanged; changes to those inputs
cancel the prepared work. A bounded owner cache retains only
pure embedding results with matching model revision, credentials, task, input and
dimensions, allowing retries without publishing stale artifacts. Native capture
runtimes use inode-fenced immutable inputs and release them before reclamation.
TTL retains its short publication fence. Reclamation scans and copies outside
the gate and validates before adoption. Foreground refresh defers allocator
conversion, reclamation, and historical index backfill to that owner. Lease-scoped publication
fences new read pins until page reuse and checkpoint publication finish; temporary
lock contention reports `ANTFLY_BUSY` and SQLSTATE `55P03`.

After a native writer publishes to a revision-4 Lite file, its last clean close
lets an in-flight allocator proof finish and publishes a checked certificate
when the inode and checkpoints still match. It does not cancel and restart that
walk. If no proof started, close may bootstrap a file of at most 4 MiB; larger
files and legacy revision-3 files do not start a shutdown proof. A failed or
canceled walk is never restarted at close. Close can wait for an existing proof,
but never launches a second large scan. The walk and reader teardown run outside
both publication locks. Subsequent processes inherit a valid certificate before opening index
sources. Owners that make no publications require no new proof at close.
Queued replay and storage maintenance recheck shutdown after acquiring the
owner gate. A historical proof attempt does not keep queued maintenance alive.

Before admitting a mutation, native writers with a nonempty unverified free pool
complete or join the detached allocator proof. Admission releases the owner gate
and path lease while waiting, then refreshes and revalidates the current tip.
Before using a verified pool, admission also waits for an active proof to finish
closing its reader fence. An uncertain publication returns `OutcomeUnknown`
before starting a proof, or before adopting one if the outcome became uncertain
during the scan. Admission never rescans an unchanged uncertain checkpoint.
This applies without a storage limit: a large request must not append its whole
volume while a completed proof waits for the same gate. Empty unbounded pools
retain append-first admission, pure queries do not demand reuse proof, and
verified pools require no graph walk. Bounded owners also retain their capacity
admission and proof-reader fence.

Storage intent is separate from API locking. Materialized SQL compiles its
schema-independent statement once before admission, then reuses that plan for
execution. Handle registry references are pinned before preparation, so close
waits for accepted requests and their allocation refunds. Preparation charges
actual live allocator bytes to a shared 256 MiB cap on the canonical path gate
(or the root handle for directory storage). This includes plans waiting for the
writer gate and survives serving-runtime replacement. Small requests can still
prepare concurrently; a request that exceeds shared headroom reports SQLSTATE
`53200` and releases its partial allocations. Refunds admit subsequent requests;
errors inside a transaction retain the usual failed-transaction semantics.
Plan teardown runs outside the API/path gates while registry pins protect its
shared admission owner. SELECT and nonexecuting EXPLAIN retain exclusive session access
without demanding allocator verification. Streaming cursors accept SELECT only;
describe and session creation also use read-only storage intent. DML, DDL, and
COMMIT request mutation admission. EXPLAIN ANALYZE accepts read-only SELECT
only; mutating plans are rejected without allocator admission.

Pending append-only proof lineage survives reopening an identical unchecked
generation on the same inode. This does not enable reuse early: the original
fenced proof must complete before foreground writes can reuse pages. Foreign
publication and inode replacement still invalidate inheritance.

Directory marker sweep progress belongs to the connection owner and survives
foreground runtime replacement, including turns when allocator verification is
busy. Each turn still examines at most 64 directory entries and preserves locked
live markers. The focused regression set is:

```sh
cd zig
zig build capi-test -Dembedded-only=true \
  -Dtest-filter='capi native short lived writers' \
  -Dtest-filter='capi native unbounded writer admits reusable pool' \
  -Dtest-filter='capi native marker sweep' \
  -Dtest-filter='capi native allocator proof survives close' \
  -Dtest-filter='capi native close bounds proof work'
```

Entity curation commits policy to the destination namespace before returning.
`antfly_db_resolver_curation_json` and `antfly_db_render_resolver_key_json`
read that committed policy immediately, even while source projections and graph
artifacts are catching up. Key rendering reads only the relevant alias component
and normalized text suppression record. Its response includes
`namespace_curation_table_id` and `namespace_curation_generation`, which together
identify the observed policy incarnation and revision. `config_generation`
retains its existing resolver-generation meaning. A curation `wait` releases
writer admission while observing the captured sources and destination; `applied`
requires their policy projection and resolution/promotion replay to complete.

Entity mutation and proposal-decision responses report cascaded keys and their
`release` or `suppress` dispositions in `namespaces[].absorbed`, once per
namespace. Match a consumer summary in `results` to its outcome using
`key_template`. Consumer summaries contain replay progress; they do not repeat
absorbed keys. Direct edit retries report empty absorbed lists when there is no
new cascade; already-decided proposal retries return their receipt and progress.

For example, collect the unique keys affected across matching namespaces:

```python
response = db.curate_entity(
    "default", "concept/note", {"action": "suppress", "absorbed": "suppress"}
)
absorbed_keys = sorted({
    item["key"]
    for namespace in response.get("namespaces", [])
    for item in namespace["absorbed"]
})
```

This follows SQLite's separation of connection lifetime from writer ownership;
it does not claim SQLite WAL concurrency or change Antfly SQL's existing
READ COMMITTED transaction semantics. Low-level storage owners used by the
standalone CLI/server retain their exclusive owner contract; a connection waits
for an active owner at operation time, not at open time.

Every thread that calls into `libantfly` needs at least
`ANTFLY_MIN_THREAD_STACK_SIZE` (8 MiB) of native stack. The storage engine
keeps sizable buffers on the stack: release builds peak around 2 MiB and
debug builds use more, and a smaller stack crashes inside the engine rather
than returning an error. 8 MiB is the Linux and macOS main-thread default,
but secondary threads are often smaller: macOS pthreads default to 512 KiB
and Rust `std` threads to 2 MiB. How each binding meets the minimum:

| Binding | How it gets the minimum |
|---|---|
| Go | cgo calls run on OS threads that inherit the 8 MiB main-thread stack |
| Python | CPython threads use 8 MiB (Linux) or 16 MiB (macOS) |
| Rust | caller's responsibility; spawn threads with `antfly_embedded::MIN_THREAD_STACK_SIZE` |
| TypeScript | configures koffi's call stacks to 8 MiB before loading the library |
| C | size threads with `pthread_attr_setstacksize(&attr, ANTFLY_MIN_THREAD_STACK_SIZE)` |

The Rust binding's tests run at exactly the minimum, so stack growth in the
engine fails them.

Handles must not be carried across `fork()`: close them before forking or
open new ones in the child. The library may run background enrichment and
maintenance work on its own threads for non-hosted handles; hosted handles
leave that work to explicit run-until-idle calls.

## Lite File Operations

The `antfly_lite_*` functions are not a separate Lite ABI. Besides the open
shortcuts, they cover only operations on the `.aflite` format itself:

- Integrity checks (`antfly_lite_check_json`), including path-level checks for
  files that may not open (`antfly_lite_check_file_json`).
- Physical stable snapshots of the file (`antfly_lite_copy_stable_snapshot_json`,
  `antfly_lite_copy_stable_snapshot_file_json`).
- Compaction and vacuum of the file (`antfly_lite_compact_json`,
  `antfly_lite_vacuum_json`).

Everything else, including status, backup, import, restore, drains, and
generated-enrichment replay, is storage-neutral and named `antfly_db_*` or
`antfly_*`.

### Catalog ownership after standalone adoption

An embedded Lite file adopted by standalone retains its server-owned catalog
across physical snapshots and file moves. Embedded document reads, writes and
search remain available. Schema, index, enrichment, resolver, range and table
changes, backup import and SQL DDL return `ANTFLY_UNSUPPORTED` with an
ownership diagnostic (SQLSTATE `0A000` for SQL). Make definition changes through
`antfly standalone` and `/db/v1`; embedded clients do not synchronize a competing
catalog back into server metadata. The native Zig embedded facade enforces the
same rule.

## Testing Expectations

C ABI changes should have coverage for:

- Header/library size agreement for options structs.
- Prefix-compatible options parsing.
- Unknown flag and non-zero reserved-field rejection.
- Generic directory open, Lite open, create, read-only reopen, and write
  rejection.
- Physical read-only behavior for persistent backends.
- Binding smoke tests that compile against the installed public header.
- Every new export that takes a handle must enter through `enterHandle` with
  the right access class, and a binding test should run it concurrently with
  writes (see `go/pkg/embedded/concurrency_cgo_test.go`).

## Database SQL

ABI 3 makes SQL database-scoped: `antfly_db_sql_json(db, request, out)` resolves
tables from a durable catalog. `antfly_db_create_table_json`, `drop_table`,
`list_tables_json`, and SQL CREATE/DROP TABLE manage independent table
namespaces. Tables own their schemas, indexes, enrichment configuration, and
document identities. Dropped IDs are never reused. The root document API
addresses the catalog's `default` table; quote it as `"default"` in SQL.

`antfly_db_open_table` returns a handle accepted by the document, schema,
index, enrichment, graph, and search APIs. Table handles share the owning
database's API fence and never own the file runtime. Close them with
`antfly_db_close` before dropping their table. Closing the database drains
entered calls, invalidates its table handles, and releases all sessions and
cursors. Database SQL/catalog/session operations require the database handle.

Portable `.afb` archives contain the complete embedded database: its live
table catalog, schemas, documents, index and enrichment definitions, stored
artifacts, and UNIQUE/FK constraint state. Export and import require the
database handle. Import requires an empty database without open table handles,
SQL sessions, or cursors. Restore builds every table in an unpublished
file or directory generation before atomically publishing the whole database.
Table IDs and the next-ID counter survive restoration; dropped namespaces
are excluded. A SHA-256 digest covers the complete database archive.

Embedded SQL catalog DDL uses the shared schema translator and native schema
publication. `CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (...)`
supports expression keys, direction/null ordering, INCLUDE columns and partial
WHERE predicates. `DROP INDEX [IF EXISTS] name [ON table]` removes the index
and its paired SQL uniqueness constraint. Without ON, the name must identify
one table; ambiguity returns SQLSTATE 42725. DDL runs outside explicit
transactions. Declaration changes increment the schema version through a
compare-and-swap and return a receipt with the native build/validation state.
`ALTER TABLE ... VALIDATE CONSTRAINT ...` retries failed coverage through the
native guarded retry protocol without changing the schema version. Constraint
activation uses the same bounded, guarded worker as the server. UNIQUE removal
fences and drains the native claims with durable retirement checkpoints;
interrupted retirement resumes before the next embedded operation. Foreign keys
that own a retiring UNIQUE generation prevent admission; another equivalent
UNIQUE can be dropped if it is not the selected foreign-key target. A pending
receipt means the operation was durably admitted; inspect the schema and native
status rather than replaying it.

CHECK expressions also accept typed `IN` and `NOT IN` membership lists,
including NULL items and other supported scalar expressions. Membership uses
SQL three-valued logic and stops at the first match. SQL queries support
`strpos(text, substring)`, returning the first one-based Unicode character
position, zero for no match, and NULL when either argument is NULL.
`LIKE` and `ILIKE`, including their negations, accept `ESCAPE` with a single
Unicode character or an empty string to disable escaping. The default escape
is backslash. Escape expressions can be parameters; NULL produces UNKNOWN.

Partial-index predicates accept boolean columns, `NOT`, `IS [NOT] TRUE/FALSE`,
and conjunctions of typed column/literal comparisons and NULL tests. Boolean
shorthand shares the native equality predicates used by explicit comparisons.
Negated truth tests retain NULL rows through native null-safe distinctness;
`NOT flag` excludes NULL rows, while `flag IS NOT TRUE` includes them. Predicates
that require a disjunction remain unsupported by the native conjunction format.

Index creation advances bounded native build pages before reporting readiness.
Large builds can return pending and resume through Lite maintenance or an
explicit `run_until_idle`, which drains relational indexes as well as search
indexes. MATCH PARTIAL FK publication requires ready parent support indexes;
a readiness deadline expires before any FK publication decision is admitted.
Admission builds only the selected witness indexes; unrelated builds and
reclamation do not consume its deadline. Lite background maintenance advances
one table per turn, including one relational index page, and releases the writer lease between
turns, retaining pending work for subsequent turns.

Detached maintenance primary transactions and index publications join the fair
writer queue. They adopt the latest primary checkpoint when the namespace
and catalog remain unchanged, so unrelated foreground commits do not force a
retry. Immutable index publication additionally checks its physical inputs;
changed index inputs cancel that work while primary transactions can still
persist completed replay progress against the latest checkpoint. Inference
and segment construction run outside the writer gate; file replacement
cancels stale publication. Reclamation continues to yield on contention.

Replay-producing maintenance delivers its prepared effects to the current
coordinator under that writer lease. It validates captured documents and
artifact keys, reserves the replay sequence on the live runtime, and advances
the detached replay watermark after a successful commit. Resolver primary
effects and their replay record commit together; derived projection work runs
outside the owner gate. Resolver publication also validates the inputs read
during computation, including absent keys and scanned candidate ranges, and
rejects changes to resolver configurations. Mixed full and graph-only resolver
phases share one publication admission and input validation, preserving their
separate replay hints without invalidating themselves between commits.
Unrelated keys remain publishable.

`antfly_db_pending_work_stats_json` includes `index_replay_debt` per namespace:
each index reports its `applied_sequence`, `target_sequence`, and
`catch_up_required`. Their sequence difference measures replay lag, rather
than elapsed time or a count of documents. Lite connections also expose
`background_publications` with owner-lifetime `attempts`, `refused` admission
counts (including publication errors and source/fence rejection), and successful
checkpoint `rebased` counts. `generated_attempts` / `generated_refused` cover
generated-result commits and store acquisition, including source validation
(one refusal per acquired store lease). `replay_attempts` / `replay_refused`
cover detached replay-producing publication. All these paths contribute to
the total `attempts` / `refused` counts. These totals survive foreground runtime refreshes and reset on closing
the last compatible connection. Root results include per-table debt in `tables`.

Failed UNIQUE coverage admits constraint-checked UPDATE/DELETE repairs under
the exact native activation checkpoint, including SQL transactions with cascades and multiple repair targets.
Repair admission follows the complete mutation closure, including failed tables
first reached by CASCADE or SET NULL. Read-only FK parents retain their coverage
requirements, and every cascaded postimage still obeys its declared constraints.
Every repair target retains its failed native checkpoint guard; other participants
still require completed UNIQUE coverage. After repairing the rows, VALIDATE
CONSTRAINT resumes historical coverage. FK activation waits for parent UNIQUE
coverage without blocking reads or parent repairs.
FK-bearing CREATE TABLE and ALTER TABLE ADD/DROP CONSTRAINT publish accepted
child generations at each parent before installing the child schema. DROP TABLE
retires its outgoing FK generations before removing the namespace. These native
owner transitions share a durable root publication plan; reopen or the next API
call completes an interrupted plan before admitting more work. Recreating a
table registers its fresh identity and FK generations rather than reusing old
references or parent admission state.

`antfly_search('table', request [, candidate_limit])` is a SQL relation. The
table name is a literal so preparation can derive its columns; request is text
or a text parameter containing the same public query JSON used by search_json.
Plain query text uses the full-text query-string parser. JSON supports named
indexes, filters, highlights, explicit dense/sparse embeddings, configured
semantic embedders and hybrid fusion. The optional candidate limit overrides
the JSON limit (1–10000), and may be written as `limit => 50`. SQL LIMIT applies
after joins and filters; it does not expand the search candidate window.

The relation exposes the declared table columns plus `_id`, nullable `score`
and nullable JSON `_highlights`. `score` and `_highlights` are reserved in a
search relation; conflicting declarations are rejected. All participating
native tables are fenced together while search hits are ranked/hydrated and
other scans capture snapshots. Cursors retain this statement view across
fetches. Embedding resolution happens before capture. SQL search rejects
uncommitted writes to the searched table because native indexes cannot rank
staged rows; commit those writes first. Other session postimages remain
available to ordinary table scans.
Relational hits are hydrated from the retained native typed-row snapshot,
preserving SQL NULL separately from JSON null. Document hydration preserves
JSON number tokens, including decimals and integers beyond float precision.

```sql
SELECT t.id, s._id, s.score
FROM antfly_search('history_items', $1, 50) s
JOIN threads t ON t.id = s.thread_id
WHERE t.project_id = $2 AND NOT t.archived
ORDER BY s.score DESC, s._id
LIMIT 20;
```

SQL requests contain `statement`, positional `parameters`, a materialized
result `limit` (default 128, maximum 4096), and optional `session_id`.
Sessions are independent connection contexts created by `sql_session_open`.
BEGIN defaults to READ COMMITTED. COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO,
and RELEASE are supported; stronger isolation and transactional DDL are
rejected. Statements read a pinned native snapshot plus their session's staged
postimages. Session writes remain private until COMMIT, which prepares native
version predicates and relational integrity commands across all affected
tables. Cross-table joins and foreign keys use the same native catalog.
Closing a session abandons staged writes. Failed transactions reject further
data statements until ROLLBACK or ROLLBACK TO an existing savepoint.

The root namespace persists the native transaction's durable decision.
Interrupted preparation is aborted on recovery; decided commits finish
participant intent resolution without replaying SQL. A committed-pending
outcome remains committed. An unknown decision returns SQLSTATE 40003 and a
transaction receipt; do not replay it. Close and reopen the database to
recover/reconcile that receipt. Commit responses reserve a fallback buffer
before mutation, so response allocation cannot erase a committed outcome.

`sql_open_cursor_json` pins a read statement without a total row limit.
`sql_fetch_cursor_json` returns `{result, exhausted}` in pages of 1–4096
rows. Close cursors explicitly, including after early termination. Unsupported
streaming plans return 0A000 and may use the bounded materialized API;
materialized result overflow returns an error. Session close releases its
cursors. `sql_describe_json` binds column and parameter types without executing.
SQL buffers include structured diagnostics on failure and must always be
freed. Integer cells are decimal strings, preserving exact signed 64-bit
values; `sql_nulls` distinguishes SQL NULL from a JSON null value.

The bindings provide Go `database/sql` (`antfly`, `file:/path/app.aflite`),
Python `antfly_embedded.dbapi`, Rust's optional `sqlx` feature, and TypeScript
`Connection` / `@antfly/embedded/kysely`. Their shared SQL conformance inputs
are in `pkg/antfly-embedded/capi-conformance/sql/cases.json`.

SQL JSON requests admit at most 64 MiB (67,108,864 bytes), including source
text and parameter data. Larger requests return a structured SQLSTATE `54000`
diagnostic naming that limit, including when a SQL session is active.
Preparation and execution each default to 256 MiB (268,435,456 bytes) of
working memory; decoding, staged rows and storage representations can coexist.
Requests below the wire limit can still exhaust working memory. Token, node
and nesting limits independently bound syntax. Embedded SQL sessions admit
4,096 staged mutations and 256 MiB of staging memory; native transaction
intent admission additionally counts retained payload and metadata, with its
configured transaction-byte limit (128 MiB by default). Nested OFFSET queries and derived-table
window functions grow retained storage with observed rows and may spill to disk.

## Native SDK validation

The Zig validation workflow installs `libantfly` alongside the E2E binary and
shares its complete `lib/` and `include/` installation with the Go, Rust, Python,
and TypeScript native SDK jobs. SDK source changes also admit this build. The
E2E aggregate gates require those jobs to pass; the fast SDK checks remain
available independently. Native jobs require the installed library and cannot
pass by skipping tests when it is missing.

Run the same checks locally with:

```sh
scripts/ci/test-embedded-sdk-native.sh /path/to/libantfly-install all
```

The optional final argument selects `go`, `rust`, `python`, or `typescript`.
The runner includes Rust's embedded SQLx integration. Model-dependent inference
suites are excluded by default; set `ANTFLY_SDK_NATIVE_INFERENCE=1` to include
them after installing their required models. This is separate from the remote
PostgreSQL driver matrix in `scripts/test_pgwire_drivers.py`.


### Replacement retry and publication boundaries

Replacement `retry_count` counts consecutive transient failures since the last
acknowledged source/replay checkpoint. A durable forward step resets the count
and backoff; volatile progress does not. `total_retry_count` never resets and
bounds each accepted job to 64 lifetime retries; status exposes that count and
`max_total_retries`. Persisted deadlines more than 3.2 seconds ahead of the
scheduler's wall clock are clamped and saved once, including after reopen. Explicit resource exhaustion, full-disk
and temporary remote failures retry up to four times. Unclassified
`Unexpected`, `InputOutput`, `ReadFailed` and `WriteFailed` errors fail immediately
rather than assuming storage corruption is transient. A failure after a newer
checkpoint uses that checkpoint for both retry accounting and recovery.
A detached snapshot invalidated by concurrent publication yields and retries
from its durable checkpoint. This does not fail the job or consume its
transient-failure budget.

Replacement ownership uses an IO mutex. Blocking cancellation and deletion park
on it; backoff sleeps occur after releasing it. Regular activation catch-up has
a 100 ms cooperative budget. After eight attempts its budget rises to at most
one second, still limited by an explicit wait deadline. The durable journal
sequence and intra-entry cursor survive each yield. These are cooperative work
budgets: a storage operation already underway, including fsync, may overrun one.
There is no unlimited escalated drain. Sustained producers can require further
bounded rounds; the old graph remains available until activation commits.

A successful replacement wait observes the catalog under the publication read
lease. The write lease spans the atomic catalog transaction and old-index
retirement, so an entry appended provisionally during installation cannot make
wait return success before commit. If a missing job races publication, wait
checks the committed catalog again before returning cancellation.
`ANTFLY_CANCELLED` describes the operation: it covers replacement cancellation,
progress/stream callbacks returning false, and IO/mutex interruption. Interrupting
an owner preserves accepted work and its durable checkpoint rather than marking
the job failed. Completion may have raced interruption; inspect current state.


Pending replacements temporarily require reader version 2, because an older
writer's journal cleanup does not honor shadow retention. Registration publishes
this fence and a versioned resolver envelope with the build marker. Libraries
that predate the epoch key reject that envelope through their strict parser;
already-open Lite writers encounter it when refreshing their connection. Failed
jobs release replay retention but keep their reservation fence until cancellation.
Swap and cancellation remove the temporary fence in their catalog transaction
once no reservations remain. Cancellation rejects a foreign serving generation
with `ANTFLY_REPLACEMENT_CONFIGURATION_MISMATCH`, leaving the catalog and job intact.
A separate baseline preserves any permanent reader requirement from resolver
features; a plain completed replacement returns to version 1. Named tables
aggregate permanent requirements into the root baseline; temporary child fences
never become permanent root requirements. This also excludes
older read-only handles during the build, since older libraries do not distinguish
reader and writer compatibility epochs.

In-process writers block on the apply lock during replacement activation: the
ordinary work budget is 100 ms, escalating to about 1 s after eight attempts.
The current batch and durability checkpoint may overrun that cooperative budget.
Clock-backoff correction leaves queued jobs unchanged on replicas, write-gated
handles and databases requiring ordered maintenance; it never chooses a local
write merely to shorten a retry deadline.

Optional replacement waits capture the admitted generation before releasing
registration admission and retain it across all wait quanta. Cancelling that
reservation returns `ANTFLY_CANCELLED`, even if another index or replacement
subsequently reuses the same name and configuration. Identical requests joining
an existing reservation wait for that reservation's generation.
