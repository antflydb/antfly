# PostgreSQL wire transport

The optional production listener is owned by the API kernel in standalone,
metadata, and data runtimes. `api/sql_pgwire.zig` provides the native
runtime adapter with non-executing Describe, password authentication, live
credential/policy checks, existing row policies, native admission, and deadline
propagation. It starts after the API reaches its stable owner address and joins
all connections before API and backend teardown. Transactions/sessions are rejected until native ownership is
implemented. HTTP SQL support does not implicitly enable pgwire or make
unsupported SQL shapes executable.

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
- Bounded materialized portals. Execute with a row count suspends/resumes the
  same result without replaying the statement or mutation. A statement can be
  closed independently of its bound portals. Backend continuation tokens are
  rejected rather than silently truncating results or re-executing queries.
- Prepared native statements retain a catalog revision, physical table ID/name,
  and schema-version fence through Bind. Execute rejects changed bindings before
  reading or mutating, including a same-shaped dropped/recreated table.
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

## Resource and lifecycle contract

`start(allocator, Config)` borrows the owner's `std.Io` and backend. The owner
must call `Server.deinit()` before releasing either. Structured `std.Io.Group`
cancellation joins accept, protocol workers, and deadline watchers before
freeing their state; there are no detached threads or shutdown-time state leaks.

Each connection has a total allocation cap shared by packets, prepared state,
bound parameters, and backend result arenas, in addition to frame/count/row
limits. Closed/replaced statements and portals reclaim their own arenas.
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
