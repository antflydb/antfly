# PostgreSQL wire transport

The optional production listener is owned by the API kernel in standalone,
metadata, and data runtimes. `api/sql_pgwire.zig` provides the native
runtime adapter with non-executing Describe, password authentication, live
credential/policy checks, existing row policies, native admission, and deadline
propagation. It starts after the API reaches its stable owner address and joins
all connections before API and backend teardown. Explicit transactions use the
native durable session owner, including savepoint rollback and constraint timing.
HTTP SQL support does not implicitly enable pgwire or make
unsupported SQL shapes executable.

SQL-language `PREPARE name [(types...)] AS statement`, `EXECUTE name
[(expressions...)]`, and `DEALLOCATE [PREPARE] name|ALL` use the same bounded,
connection-owned registry as wire Parse/Bind. Names survive transaction commit
and are released on disconnect. Execution rebinds against the current catalog while preserving the parameter
and result type contract, rechecks current credentials, binds typed values without SQL
interpolation, and uses pull streaming for eligible reads. EXECUTE arguments
are bounded scalar expressions; column references, parameters, and subqueries
are not an argument-evaluation escape hatch into table reads. These commands
do not create durable HTTP prepared-statement resources.

SQL `DECLARE [NO SCROLL|SCROLL] CURSOR [WITH|WITHOUT HOLD]`, `FETCH`, `MOVE`,
and `CLOSE` retain one eligible native pull snapshot. SCROLL supports forward,
backward, absolute, relative, first/last and current-row positioning. Scroll and
hold rows are copied as typed cells into a connection-owned spool: at most
65,536 rows per cursor and 8 MiB shared by all cursor spools, also charged to the
16 MiB connection limit. Quota exhaustion fails explicitly; there is no hidden
unbounded materialization or query replay. These limits are protocol defaults.

WITH HOLD materializes the remaining original snapshot before COMMIT and is
published only after a confirmed COMMIT result. Unknown/failed outcomes never
publish it. Snapshot/admission/plan resources are released on exhaustion, while
buffered rows retain a small authorization capsule. Every fetch rechecks current
credentials, scoped table identities, read grants and row filters. Held rows
survive later transaction boundaries, not disconnect or node restart. Rollback
to a savepoint closes cursors created after that savepoint without rewinding
earlier cursor positions. Blocking plans without a native pull stream remain
unsupported for SQL DECLARE.

`SET [SESSION|LOCAL] statement_timeout`, `SHOW statement_timeout`, and
`RESET statement_timeout` work with both simple and extended queries. Values
accept integer milliseconds or quoted `ms`, `s`, and `min` units. Session changes
inside a transaction commit/roll back with it; local changes expire at the
transaction boundary; savepoint rollback restores both values. Zero disables
the client-requested timeout but never the operator's hard statement deadline.
`SET [SESSION|LOCAL] application_name`, `SHOW application_name`, and
`RESET application_name` accept at most 128 bytes of UTF-8 text without
control characters. Startup `application_name` obeys the same bound. Local and
session changes follow the transaction and savepoint rules above.
`SET [SESSION|LOCAL] search_path`, `SHOW search_path`, and `RESET search_path`
support up to eight ordered existing namespaces (at most 128 bytes each), using
the same transaction and savepoint restoration rules. SET checks every
namespace's read permission and existence before installing the entire path.
An unqualified relation binds to the first matching table; authorization,
catalog-generation, and other failures never trigger fallback. Table operations
still authorize their own scoped resources. Lookup scope is separate from the
immutable durable transaction-owner scope. Existing SQL prepared statements,
Parse/Bind portals and cursors retain their original complete path. `$user`
expansion remains unsupported. Other session settings are not
acknowledged as no-ops. `SET CONSTRAINTS` is a native durable transaction command,
not a connection setting.

Enable it in the node configuration with authentication configured:

```json
{"enable_auth":true,"pgwire":{"enabled":true,"bind_host":"127.0.0.1","bind_port":5432,"max_connections":32}}
```

Startup fails if the user manager or backend runtime is missing, the port is
unavailable, or a remote bind lacks `externally_protected_transport:true`.
Remote deployments must supply a TLS proxy or authenticated private transport;
that flag does not add TLS. The default listener is disabled. Configure each
process's port separately when running multiple nodes on one host.

## Supported protocol boundary

- PostgreSQL v3 startup and password authentication through a mandatory backend
  callback, with credentials resolved before any database/catalog access.
- Single-statement simple queries and Parse/Bind/Describe/Execute/Close/Flush/Sync.
  Describe does not execute. The backend validates syntax and parameter count.
- Text and typed binary parameters, including exact signed 64-bit integers;
  values are never interpolated into SQL. Unsupported binary OIDs fail closed.
- Pull-based read portals for eligible scan/filter/projection and relational
  plans, including nonblocking nested queries. Each bounded page is released
  before the next pull; network flush supplies backpressure to storage reads.
  Blocking final sorts/aggregations retain the bounded materialized path.
  Execute suspends/resumes the same snapshot/result
  without replaying a statement or mutation. A statement can be closed
  independently of its portals. Opaque backend continuation tokens are rejected.
- Cached statements recompile before Bind and before an unstarted Execute, using
  their fixed parameter types and saved namespace/search path. Compatible DDL,
  including an added unselected column or index, transparently refreshes the
  catalog guard. Changed result types fail with `0A000` and PostgreSQL's
  `cached plan must not change result type` message before reading or mutation
  admission. Rebinding also refreshes the settings epoch; execution validates
  that fresh epoch to fence concurrent setting catalog changes.
  Suspended portals keep their original snapshot; execution never replays an
  admitted mutation. Concurrent DDL after rebinding still meets a schema fence.
- Extended-query errors drain queued messages until Sync. Backend transaction
  state is reflected by ReadyForQuery, and disconnect abandons session state.
- Separate CancelRequest connections with cryptographically random secrets;
  reserved handshake capacity permits cancellation at the authenticated limit.
- Committed mutation receipts are PostgreSQL NoticeResponse details, followed
  by CommandComplete. Unknown outcomes retain SQLSTATE 40003, transaction ID,
  and `retryable:false`; the adapter never retries mutations automatically.

Text NUMERIC parameters preserve decimal spelling; PostgreSQL binary NUMERIC
and native TLS are not implemented. Unknown startup options, unsupported
encodings, and binary representation guesses are rejected. Passwords may only
be accepted on loopback by default; remote binding requires explicit deployment
acknowledgement of an externally protected transport. This override supplies no
TLS itself.

## DDL admission and readiness

Standalone completes initial durable relation catalog publication before opening
public HTTP or PostgreSQL listeners. This also applies on restart with persisted
tables. Publication uses bounded control rounds without a fixed total startup
deadline. SIGINT/SIGTERM cancel the wait; terminal publication failures
for the current source epoch fail startup.
HA retains its separate writer/replica authority contract.

`53300` with `retryable:true` for relation catalog publication means no schema
change was admitted. Wait briefly and retry the complete DDL. Standalone's
metadata control loop advances the same bounded relation publication protocol
as clustered metadata; publication is asynchronous after catalog changes.

`55000` with a DDL receipt in `DETAIL` means the declaration committed and
activation is pending. For an initial FK table, poll for the table to become
visible, then use it. For changes to an existing table, inspect its schema and
constraint status. Retain the receipt and do not replay the DDL.

`40003` means the admission or mutation outcome is unknown. Retain the receipt,
including any FK publication ID, and inspect durable status before deciding
what to do next. This is not a retry signal. A failed operation before admission
does not reserve a hidden table or parent publication lock.

## Resource and lifecycle contract

`start(allocator, Config)` borrows the owner's `std.Io` and backend. The owner
must call `Server.deinit()` before releasing either. Structured `std.Io.Group`
cancellation joins accept, protocol workers, and deadline watchers before
freeing their state; there are no detached threads or shutdown-time state leaks.

Each connection has a total allocation cap shared by packets, prepared state,
bound parameters, and backend result arenas, in addition to frame/count/row
limits. Closed/replaced statements and portals reclaim their own arenas.
Native streaming cursors retain their original statement deadline as a hard
snapshot lifetime; subsequent Execute messages do not extend it. Pages recheck
credentials, current read grants and row-filter policy. Errors and disconnects
close snapshots before releasing credentials. Explicit SQL LIMIT is distinct
from page size; scan/work/memory exhaustion returns an error, never a truncated
successful SELECT.
Authentication, idle, and statement deadlines use the owner's monotonic clock;
an event wakes the watchdog when a shorter deadline replaces an idle deadline.
Timeout while a mutation is in flight closes the connection rather than
inventing a retry-safe outcome. Backends must call `Request.check()` at bounded
work/IO boundaries and propagate the request deadline to downstream operations.

Backend descriptions and results use caller-owned arena storage. Describe must
return all parameter types (unknown is allowed) and stable column names/types.
Execute must preserve that shape, apply authorization on every resolution, and
return its real transaction status. It must not infer that a canceled client
request means an already committed mutation was rolled back.

Native results can retain an owner release hook, which runs before the portal
arena is reclaimed. This avoids a second full row copy while charging the native
arena to the connection budget. The adapter joins native work before returning
even when the connection is canceled, preserving borrowed storage and durable
outcome handling. Authentication uses the same UserManager password verifier as
HTTP and retains only a credential-bound MAC, not plaintext passwords or hashes.
Password rotation invalidates that MAC; each statement snapshots fresh policies,
and read-page/write boundaries revalidate live grants. Per-row cancellation
checks do not acquire the global user-manager mutex.

Binary timestamps carry microseconds since PostgreSQL's 2000 epoch on the wire.
The adapter uses the existing storage datetime parser/formatter to normalize
parameters into native ISO strings and exclusively owned result datetime cells
into exact unsigned nanoseconds. The full native `u64` range is retained without
floating-point conversion. Pre-1970/out-of-range values and results requiring
submicrosecond wire precision fail explicitly rather than rounding.

## Tests

`zig build pgwire-test` covers protocol transcripts, exact binary bigint
parameters/results, statement/portal ownership, authentication ordering,
extended error recovery, allocation bounds, protected transport, active-backend
shutdown, and authenticated cancellation at connection capacity. The network
cases use local loopback sockets and need socket access in sandboxed runners.

## Driver formatting contract and conformance

Startup also accepts asyncpg's quoted UTF-8 encoding and UTF8/UTF-8/UTF_8
aliases; the effective client_encoding stays UTF8. Startup accepts case-insensitive `DateStyle` (ISO with optional MDY/DMY/YMD),
`TimeZone` (UTC, Etc/UTC, GMT, +00, Z, GMT+00:00, UTC+00:00),
`extra_float_digits` (-15 through 3), `IntervalStyle=postgres`, and
`standard_conforming_strings=on` (also true/1). These negotiate the fixed native
output contract: ISO, MDY; UTC; round-trippable floats (reported as 3);
PostgreSQL intervals; standard strings. They do not install alternative input
parsers or lossy float renderers. `SHOW`, `SET`, and `RESET` use that same
contract in simple and extended queries. ParameterStatus reports the effective
values, including IntervalStyle and extra_float_digits. Unsupported startup
settings fail with SQLSTATE 0A000 and identify the parameter; `options`, `role`,
and startup `search_path` remain rejected. Non-UTC clients, including JDBC JVMs,
must select UTC explicitly until native session time zones are supported.

Run `zig build pgwire-test` for framing, JDBC-style startup, settings, and
rejection tests. Run `scripts/test_pgwire_drivers.py --binary <antfly>` using the
uv command in its docstring for real SQLx 0.9, pgx, node-postgres, psycopg/libpq,
and asyncpg clients against an isolated authenticated standalone process. Each
client exercises pool/connection setup, SELECT 1, typed bound parameters, SHOW,
and a transaction. Go also exercises database/sql and its comment-only ping.
Portal arenas have stable addresses through registry growth and removal, so
retained native results release before their allocator is destroyed. The Python client also verifies unsupported startup values
through a real connection. Go, Node.js, and Rust (at least 1.94 for SQLx 0.9) toolchains are required.
Both base and full E2E CI gates require the driver conformance job, which reuses
the corresponding candidate binary. Use
`--url` to target an existing node or `--drivers` to select individual clients.

Relational SQL reads carry page-owned native cells from storage through the SQL
batch and pgwire cell accessor. Embedded SQL scans and ranked point hydration
use the same native cells; ordinary ranked retrieval omits the unused public
document body. Arrays decode directly into typed elements, and
NUMERIC retains canonical decimal groups; neither requires a public JSON
envelope. Materialized SELECT and mutation RETURNING results use the same typed
sink as blocking SQL operators. The result owner keeps cells alive until the
protocol releases the result. HTTP SQL continues to request public JSON results.

Private retained-read paging negotiates `application/vnd.antfly.sql-page.v1`
through Accept. New peers exchange bounded native column blocks, preserving
array bounds, exact NUMERIC, missing fields, SQL NULL, JSON null, schema epochs,
sequence numbers, and primary digests. Control messages and authentication are
unchanged. An older owner can ignore Accept and return its existing JSON page;
an older coordinator receives that legacy page shape from a new owner. SQL
requires a retained-read provider; a TableReadSource that implements only public
NDJSON scans must add the native read capability before serving SQL. Public
HTTP scan support alone cannot provide statement snapshot semantics.

Bind parameters and SQL EXECUTE arguments now carry typed Datums into one
execution-owned frame. Array envelopes and NUMERIC formatting remain at public
JSON compatibility boundaries. Parameter frames own their payloads before a
stream can outlive its opening request.

Retained pages keep decoded column blocks behind cell accessors. Coordinated
merging leases source pages instead of deep-copying their payloads; output pages
can survive cursor closure. Field mappings are compiled once per source layout.

Primitive and text SQL arrays retain canonical storage bytes, with
`cardinality()` and `flatCell()` accessors. Cloning preserves the compact form;
encoders and operators accept both representations. NUMERIC, UUID and JSONB
arrays retain their existing decoded representation. Lite ranked cursors capture
ranking and a primary snapshot together, release the writer-excluding lease,
and hydrate requested pages bounded by row demand and a 256 KiB payload target.
A single row retains the existing native row-size ceiling.

The native Bind allocation regression uses a 32,768-element bigint array and
an arena over a non-resizing allocator: public-envelope decoding peaks at
17,106,262 bytes versus 3,932,232 bytes for native cells (77% lower). The compact
stored-array regression peaks at 399,446 bytes versus 3,932,392 bytes for expanded
elements (90% lower). These are isolated ReleaseFast allocation measurements;
fixture inputs and final pgwire frames are excluded. Page bytes, ownership
copies, actual JSON/JSONB payloads, and wire output still have costs.

Focused regression gates (from `zig/`, with the repository's Zig version):

```sh
zig build sql-test pgwire-test -Doptimize=ReleaseFast
zig build sql-test -Dtest-filter='SQL compact stored array' -Dtest-filter='SQL native transport' -Doptimize=Debug
zig build antfly-api-sql-native-cells-test antfly-api-table-read-test -Doptimize=Debug
zig build antfly-coordinated-read-set-test antfly-storage-owner-test -Doptimize=Debug
zig build antfly-native-row-projection-test -Doptimize=Debug
```
