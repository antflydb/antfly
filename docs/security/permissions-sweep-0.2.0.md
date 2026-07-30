# Permissions sweep for 0.2.0

Date: 2026-07-30

This sweep is scoped to the Zig Antfly runtime and independently shipped
components used with 0.2.0. The deprecated Go Antfly runtime is intentionally
out of scope; any hardening needed there belongs on the 0.1.x branch.

The review covered public protocol adapters, table operations, row-filter
boundaries, cross-table execution, transaction paths, MemoryAF visibility, and
the Antfly-aware proxy. It uses Antfly's existing trust model rather than
treating every unauthenticated local or operator surface as a vulnerability.

## Security model used for this review

- Authentication is opt-in. A deployment with authentication disabled is
  intentionally open and is not itself a finding.
- Local inference endpoints are explicitly unauthenticated in their OpenAPI
  contract.
- Table operations are independent grants: `admin` and `write` only imply other
  operations when a policy explicitly says so.
- An API key with no explicit permission scope inherits its owner's
  permissions. Owner row filters remain mandatory; key-local row filters may
  only narrow them.
- Queries must authorize every referenced table, and each table's row filter
  must be applied to that table's scan.
- Empty Antfly proxy table/operation lists mean tenant-wide scope. Existing
  tests establish this as an intentional contract.
- MemoryAF receives a trusted `UserContext` from its host integration.
  Visibility must be enforced within that boundary; transport authentication
  belongs at the integration boundary.
- Cluster-internal routes rely on a service/deployment trust boundary rather
  than end-user table RBAC.

## Native Zig MCP result

Zig Antfly has a native Streamable HTTP MCP server at `/mcp/v1`, plus scoped
extension MCP endpoints. The reported read-only write bypass applies to the
deprecated Go adapter, not to the native Zig path.

With authentication enabled, the Zig path:

1. authenticates every MCP HTTP request;
2. filters the advertised built-in and extension tools using the caller's
   effective permissions;
3. forwards every built-in tool call back through the normal authenticated
   HTTP handler with the same authorization or verified trusted-principal
   token; and
4. therefore performs the exact target-table permission and row-filter checks
   at execution time, even for tools advertised from a broader wildcard or
   "any table" capability.

A regression now verifies that a read-only trusted principal can see read tools
but not `batch` or table creation, can read its allowed table, and receives a
permission error when using a visible read tool against another table.

Extension tool discovery is also filtered by declared required capabilities
and installed scope. Extension execution additionally depends on the extension
runtime's capability boundary; that boundary deserves its own focused audit,
but it is not the same bypass as the legacy Go MCP adapter.

## Confirmed issues fixed in this branch

### High: Zig table scan had no table permission requirement

The Zig permission router covered lookup and query routes but omitted the
`/tables/{table}/documents` scan route. With authentication enabled, any
authenticated principal could scan a table. Row filters were applied later,
but there was no table-level `read` check.

The route now requires table `read`; a path-decoding regression test is
included.

### High: MemoryAF private visibility was bypassable

`go/pkg/memoryaf` is an independently shipped MemoryAF package, not the
deprecated Go Antfly runtime. Several paths in that package could disclose
another user's private memories:

- explicitly requesting `visibility=private` removed the owner predicate;
- direct lookup ignored visibility;
- graph expansion and entity-memory results could merge private documents
  outside the ordinary query filter; and
- `find_related` accepted another user's private memory as its start node.

Member requests for private visibility now retain `created_by = caller`.
Direct reads and graph-derived results enforce `team OR owner`, administrators
retain their existing override, and inaccessible start nodes are reported as
not found. Query-side filters remain in place and result-side checks provide a
defense against graph-union behavior.

## Confirmed issues requiring a release decision

### Release-blocking unless fixed or experimental: Zig transactions

When authentication is enabled, transaction routes authenticate the request
but do not consistently authorize the tables named inside transaction bodies
or bind transaction sessions to a principal.

Confirmed gaps include:

- transaction session listing and lookup expose all sessions to any
  authenticated caller;
- sessions are created without principal ownership;
- commit accepts table names from the request without `write` checks;
- staged reads do not apply table `read` permission or row filters; and
- staged writes/deletes and session operations are not tied to the principal
  that began the session.

A read-only credential can therefore reach write/commit execution, and one
authenticated principal can operate a session created by another. A safe fix
needs a small explicit transaction authorization design: session ownership,
permission collection over all referenced tables, and defined row-filter
semantics for reads and conflict responses. Until that is implemented,
authenticated transaction routes should be experimental or unavailable in
0.2.0.

### High, possibly experimental: request-defined foreign data sources

An authorized Zig query can provide a PostgreSQL DSN in `foreign_sources`; the
runtime resolves secret references and opens the connection. Table `read`
authorization applies to the logical table name, but no distinct permission
controls network destinations or use of stored connection secrets.

This is an SSRF/internal-database and secret-use boundary, not merely query
syntax. For 0.2.0, foreign sources should use administrator-declared connection
names or allowlists, require a distinct permission, or remain experimental.

### High: MemoryAF entity metadata can still derive from private memories

Memory documents are now visibility-filtered, but extracted entity nodes are
shared records without owner/visibility provenance. `list_entities` and entity
mention counts can therefore reveal names or activity derived solely from
another user's private memories.

This needs a graph data-model decision: carry visibility provenance on
mentions/entities, compute caller-visible aggregates, or gate entity discovery
until that model exists.

### High, deployment-boundary decision: internal control routes

Zig internal group and HA routes intentionally skip public authentication, but
they are dispatched by the same top-level HTTP server as public APIs. This is
safe only when ingress and service-network policy make those paths unreachable
to untrusted clients.

The release should document and test that boundary, bind internal APIs to a
separate listener, or add service authentication. End-user table RBAC would be
the wrong control for cluster-internal RPCs.

### Medium: proxy `/resolve` exposes routing without authentication

The separately shipped Antfly-aware proxy authenticates and authorizes
`/proxy` requests, but `/resolve` returns tenant/table routing details and
`target_url` without either step. If this is operator-only discovery, it needs
an explicit network boundary; otherwise it should use the same authentication
and authorization as forwarding.

## Reviewed behaviors that are not findings

- Auth-disabled Antfly is open by configuration.
- The native Zig MCP does not grant write operations to a read-only identity.
- Zig A2A is experimental and requires admin when authentication is enabled.
- Local inference routes are intentionally unauthenticated.
- Empty API-key permission scopes inherit the owner's permissions.
- Empty Antfly proxy table/operation scopes mean tenant-wide access.
- Cluster-internal RPCs should use a service/deployment trust boundary rather
  than end-user table RBAC.
- MemoryAF's `UserContext` is supplied by a trusted host integration; callers
  must not be allowed to forge it at that boundary.

## Validation

- Full MemoryAF tests passed after temporarily supplying the pre-existing
  missing SDK alias `TransformOpTypeMin`; the shim was removed and is not part
  of this change.
- `zig fmt --check zig/pkg/antfly/src/api/http_server.zig`
- `git diff --check`

The Zig suite was not built locally because the Antfly Zig build is known to
risk OOM on this machine. Zig regressions are left to CI.
