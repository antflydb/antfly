# Permissions sweep for 0.2.0

Date: 2026-07-30

This sweep reviewed permission enforcement at public protocol adapters, table
operations, row-filter boundaries, cross-table execution, transaction paths,
MemoryAF visibility, and the Antfly-aware proxy. It intentionally uses Antfly's
existing security model rather than treating every unauthenticated local or
operator surface as a vulnerability.

## Security model used for this review

- Authentication is opt-in. A deployment with `enable_auth: false` is
  intentionally open and is not itself a finding.
- Local inference endpoints are explicitly unauthenticated in their OpenAPI
  contract.
- Table operations are independent grants: `admin` and `write` only imply other
  operations when a policy explicitly says so.
- An API key with no explicit permission scope inherits its owner's permissions.
  Owner row filters remain mandatory; key-local row filters may only narrow
  them.
- Queries must authorize every referenced table, and each table's row filter
  must be applied to that table's scan.
- The Antfly proxy treats empty table/operation lists as tenant-wide scope.
  Existing tests make this an intentional contract, not an accidental
  allow-all.
- MemoryAF receives a trusted `UserContext` from its host integration. This
  sweep enforces MemoryAF visibility within that trust boundary rather than
  adding a second transport authentication system.

## Confirmed issues fixed in this branch

### Release-blocking: Go MCP authenticated callers bypassed authorization

The Go MCP route used `authnMiddleware`, but the adapter invoked table-manager
and metadata mutation methods directly. Consequently, any valid credential,
including a read-only user or API key, could call `batch`, table/index mutation,
backup, or restore tools.

The adapter now applies the same operation mapping as the REST API:

- table create/drop, index create/drop, backup/restore: table `admin`
- batch: table `write`
- query and index listing: table `read`
- table listing: `table/*:read`, matching the REST list contract

MCP query authorization includes all nested join tables. The authenticated
principal and API-key scope are carried in request context, and an HTTP
streamable-MCP regression test verifies that the principal survives the
transport boundary. Checks fail closed when authentication is enabled and the
principal context is missing.

### Release-blocking: Go API keys dropped owner row filters

`ValidateApiKey` returned only the key-local row-filter map. An API key without
a local filter therefore bypassed mandatory row filters attached to its owner.
This contradicted both the OpenAPI contract and the Zig implementation.

Go now computes the effective filter with the same layering semantics as Zig:
table-specific filters take precedence over wildcard filters within each layer,
and an owner filter plus a key-local filter are combined with `AND`. Tests cover
unfiltered-key inheritance and owner-wildcard plus key-table narrowing.

### High: Go key scans omitted row-level filters

`ScanKeys` checked table `read` but did not apply the caller's row filter. This
exposed keys and requested fields outside the caller's row scope.

Scans now merge the security filter with the caller filter using `AND` and push
the effective filter to every shard. A regression test covers the merge.

### High: Go joins authorized only the primary table

REST and MCP joins could read a right-side or nested table without checking its
table permission. The right-side query path already receives request context
and applies that table's row filter, so the missing piece was pre-execution
authorization.

Query parsing now collects the primary, right-side, and recursively nested join
tables and denies the request before execution if any table lacks `read`.

### High: Go OCC read sets bypassed row visibility

Transaction read-set validation used a strict key lookup and returned the
actual version on conflict without checking the caller's row filter. A
row-filtered reader could probe hidden keys and versions through
`/transactions/commit`.

Read-set validation now applies the table's security filter before comparing or
returning versions. Hidden rows receive a generic not-found response.

### High: Zig table scan had no table permission requirement

The Zig permission router covered lookup and query routes but omitted the
`/tables/{table}/documents` scan route. With authentication enabled, any
authenticated principal could scan a table. Row filters were applied later,
but there was no table-level `read` check.

The route now requires table `read`; a path-decoding regression test is included.

### High: MemoryAF private visibility was bypassable

Several paths could disclose another user's private memories:

- explicitly requesting `visibility=private` removed the owner predicate;
- direct lookup ignored visibility;
- graph expansion and entity-memory results could merge private documents
  outside the ordinary query filter;
- `find_related` accepted another user's private memory as its start node.

Member requests for private visibility now retain `created_by = caller`.
Direct reads and graph-derived results enforce `team OR owner`, administrators
retain their existing override, and inaccessible start nodes are reported as
not found. Query-side filters remain in place and result-side checks provide a
defense against graph-union behavior.

## Confirmed issues requiring a release decision

### Release-blocking unless fixed or experimental: Zig transactions

When auth is enabled, Zig transaction routes authenticate the request but
`requiredPermissionForRequest` assigns no per-table permission to transaction
routes. `dispatchTransactionRoutes` then:

- lists and retrieves all transaction sessions for any authenticated caller;
- begins sessions without binding them to a principal;
- accepts read, write, delete, and commit table names from request bodies
  without table permission checks;
- performs transaction reads without table row filters.

A read-only credential can therefore reach write/commit execution, and one
authenticated principal can operate a session ID created by another. Fixing
this safely requires defining session ownership, read/write permission
collection across staged and commit bodies, and row-filter behavior for
transaction reads. Until then, these routes should not be part of a non-
experimental authenticated 0.2.0 surface.

### Release-blocking unless fixed or experimental: Go A2A facade

The Go runtime mounts `/a2a` unconditionally and does not wrap it in
`authnMiddleware`. Its adapter calls retrieval and query-builder execution
methods directly, bypassing the REST handler's wildcard read check and trusted
row-filter context. The Zig runtime, by contrast, gates A2A behind
`experimental` and requires admin permission when auth is enabled.

The team should choose and document one contract, then either add principal and
per-table enforcement to the Go adapter or gate the Go facade for 0.2.0.

### Release-blocking for network-reachable auth deployments: default `admin/admin`

Enabling auth causes the Go runtime to create a full-wildcard `admin` user with
password `admin` when none exists. There is no forced rotation or one-time
secret exchange. This turns an otherwise protected fresh deployment into a
known-credential deployment.

Bootstrap should require an operator-provided secret, emit a one-time
recoverable credential through a protected channel, or refuse network-reachable
auth startup until initialized.

### High, possibly experimental: request-defined foreign data sources

An authorized query can provide a PostgreSQL DSN in `foreign_sources`; the
server resolves secret references and opens the connection. Table `read`
authorization applies to the logical table name, but no separate permission
controls network destinations or use of stored connection secrets.

This is an SSRF/internal-database and secret-use boundary, not just query
syntax. For 0.2.0, foreign sources should use administrator-declared connection
names/allowlists, require a distinct permission, or remain experimental.

### High: MemoryAF entity metadata can still derive from private memories

Memory documents are now visibility-filtered, but extracted entity nodes are
shared records without owner/visibility provenance. `list_entities` and entity
mention counts can therefore reveal names or activity derived solely from
another user's private memories.

This needs a graph data-model decision: carry visibility provenance on
mentions/entities, compute caller-visible aggregates, or gate entity discovery
until that model exists.

### High, deployment-boundary decision: internal control routes

Go `/_internal/v1/*` and Zig internal group/HA routes intentionally skip public
authentication, but they are dispatched by the same top-level HTTP handler as
public APIs. This is safe only when ingress and service-network policy make
those paths unreachable to untrusted clients.

The release should either document and test that network boundary, bind
internal APIs to a separate listener, or add service authentication. This sweep
does not apply end-user RBAC to cluster-internal RPCs because that would be the
wrong trust model.

### Medium: proxy `/resolve` exposes routing without authentication

The Antfly-aware proxy authenticates and authorizes `/proxy` requests, but
`/resolve` returns tenant/table routing details and `target_url` without either
step. If the endpoint is operator-only discovery, it needs an explicit network
boundary; otherwise it should use the same authentication and authorization as
forwarding.

### Medium/high depending log access: standalone keystore secret in logs

When standalone mode auto-creates a keystore, it logs the generated keystore
password. This may be a deliberate recovery mechanism, but it makes centralized
logs a credential store. Prefer an explicit secret source or a protected local
one-time credential file.

### Medium: row-filter composition and planning side channels

- Go's user-manager comment says multiple grants for one table are conjuncted,
  while the authorization design says multiple grant filters should be
  disjuncted before being combined with caller predicates. Current APIs replace
  a direct user/table filter, so the intended future role/grant behavior needs
  clarification before implementation.
- Go join planning reads whole-table statistics before executing row-filtered
  right-side queries. Profiles or plan selection may reveal coarse information
  about rows outside the caller's filter. The data rows are filtered, but the
  no-inference goal in `docs/auth.md` is stronger than the current planner.

## Reviewed behaviors that are not findings

- Auth-disabled Antfly is open by configuration.
- Local inference routes are intentionally unauthenticated.
- Empty API-key permission scopes inherit the owner's permissions.
- Empty Antfly proxy table/operation scopes mean tenant-wide access.
- Cluster-internal RPCs should use a service/deployment trust boundary rather
  than end-user table RBAC.
- MemoryAF's `UserContext` is supplied by a trusted host integration; callers
  must not be allowed to forge it at that integration boundary.

## Validation

- `GOWORK=off GOEXPERIMENT=simd go test ./src/usermgr ./src/metadata -count=1`
- Full MemoryAF tests pass after temporarily supplying the pre-existing missing
  SDK alias `TransformOpTypeMin`; the shim was removed and is not part of this
  change.
- `zig fmt --check zig/pkg/antfly/src/api/http_server.zig`
- `git diff --check`

The Zig test suite was not built locally because the Zig Antfly build is known
to risk OOM on this machine. The new Zig regression should run in CI.
