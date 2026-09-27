# Antfly Authorization Design

This document describes the intended Antfly data-plane authorization model. It
is provider-neutral: Antfly can receive principal context from an embedded auth
module, a trusted gateway, a managed control plane, or a self-hosted deployment.

## Secure Deployment

Local quickstarts disable authentication and bind to loopback. Docker examples
listen on `0.0.0.0` inside the container but publish host ports on `127.0.0.1`.
Keep ports private until authentication and TLS termination are configured.

Enable built-in authentication with `--auth true` or `"enable_auth": true` in
the config file. On first startup, provide a unique administrator password
through `ANTFLY_BOOTSTRAP_ADMIN_PASSWORD` (12 to 72 bytes):

```sh
export ANTFLY_BOOTSTRAP_ADMIN_PASSWORD="$(openssl rand -base64 24)"
# Save this password securely before removing the environment variable.
antfly standalone --auth true
```

For Docker, forward the variable without placing its value in arguments:

```sh
docker run -p 127.0.0.1:8080:8080 \
  --env ANTFLY_BOOTSTRAP_ADMIN_PASSWORD \
  -v antfly-data:/antflydb \
  ghcr.io/antflydb/antfly:latest \
  standalone --host 0.0.0.0 --data-dir /antflydb --auth true
```

The administrator username is `admin`. The password is stored as a hash and
is not logged. Existing non-default credentials are preserved on later starts,
even if the variable changes. Rotate credentials through the authenticated
user-management API. Bootstrap is local to each node, not a cluster-wide
password update.

An auth-enabled instance with legacy `admin:admin` credentials refuses to start
until a valid bootstrap password replaces that known default. HA primary and
standby startup does not rewrite credentials from a portable seed; rotate
legacy credentials before capturing a replacement HA seed.

Built-in authentication also applies to MCP tools on `/mcp/v1` on the public
port. CORS is a browser access policy, not authentication. Omitted or empty
`allowed_origins` lists deny cross-origin access; `"*"` requires explicit
opt-in and cannot be combined with credentialed CORS.

### Local files in remote templates

Remote helpers such as `remoteText`, `remotePDF`, and `remoteMedia` deny
`file://` unless `remote_content.security.allowed_paths` explicitly includes
the target. Omitted and empty path lists deny local file access. To permit an
intentional ingestion directory:

```json
{
  "remote_content": {
    "security": {
      "allowed_paths": ["/srv/antfly/ingest"]
    }
  }
}
```

Use a dedicated directory containing only files authorized callers may read.
Canonical path checks reject sibling-prefix and symlink escapes. Private-IP
blocking for HTTP downloads does not authorize local file access.

## Goals

Antfly should enforce authorization at the data-plane boundary where queries are
parsed, planned, and executed. External systems can authenticate users and
resolve policies, but Antfly must make the final table, operation, and row-level
decision for every request it executes.

The authorization system should support:

- Tenant or instance isolation.
- Principal identity and principal type.
- Table-level read, write, and admin permissions.
- Optional per-table row filters.
- Principal attributes for explicit policy templates.
- Cross-table queries, joins, subqueries, vector search, full-text search, and
  retrieval workflows.
- Fail-closed behavior when policy context is missing or invalid.

## Principal Context

Requests should carry a trusted principal context. The context may be created by
Antfly itself or by a trusted component in front of Antfly, but clients must not
be able to forge it.

Example:

```json
{
  "principal_id": "user_123",
  "principal_type": "user",
  "tenant_id": "tenant_abc",
  "tables": {
    "orders": {
      "operations": ["read"],
      "row_filter": {
        "field": "tenant_id",
        "equals": "tenant_abc"
      }
    },
    "customers": {
      "operations": ["read"],
      "row_filter": {
        "field": "region",
        "in": ["na", "eu"]
      }
    }
  },
  "attributes": {
    "tenant_id": "tenant_abc",
    "region": "na"
  },
  "expires_at": "2026-05-19T23:00:00Z"
}
```

The trusted context should include enough information for Antfly to authorize
without making per-row callbacks to an external policy service.

## Operations

Antfly should distinguish at least:

- `read`: query, search, get, list, graph traversal, retrieval.
- `write`: insert, update, delete; implies `read` only when the policy says so.
- `admin`: schema, indexes, table settings, policy metadata; implies `write`
  only when the policy says so.

Authorization should be checked against the operation actually executed, not
only the HTTP method or top-level API route.

## Row Filters

Row filters are mandatory security predicates. They are not user preferences,
default filters, or ranking hints.

Row filters should be represented as structured expressions, not raw SQL or
unvalidated string snippets. A row filter can reference trusted principal
attributes through explicit templates:

```json
{
  "field": "tenant_id",
  "equals_principal_attribute": "tenant_id"
}
```

Before execution, Antfly should compile the template against:

- The target table schema.
- The trusted principal attributes.
- The supported filter operators for the target backend.

Unknown fields, missing attributes, unsupported operators, and type mismatches
must fail closed.

## Join-Aware Enforcement

A cross-table query is a request against every table it references. A principal
with access to one table must not be able to infer restricted rows from another
table by joining through an allowed table.

Antfly should enforce joins with these rules:

- Every referenced table, index, collection, view, or saved query must resolve
  to a table authorization node before execution.
- Each referenced table must pass the required operation check.
- Each table's row filter must be attached to that table's logical scan node.
- Filters must be bound through the table alias used in the query plan.
- User predicates and security predicates combine with `AND`.
- Multiple grant filters for the same table and operation combine with `OR`,
  then the result is `AND`ed with the caller's predicate.
- If any referenced table lacks permission, deny the entire query.
- Outer joins must filter the restricted side before join evaluation while
  preserving normal join semantics for rows that pass authorization.
- Subqueries, CTEs, unions, graph traversals, vector search, full-text search,
  and retrieval-agent requests must normalize into table access nodes with the
  same authorization constraints.
- Views and saved queries must either expand to their underlying table access
  nodes or be denied unless Antfly can prove the saved object already carries an
  equivalent authorization policy.

## Planning Boundary

Authorization should run after parsing and before physical planning or
execution.

The planning pass should:

1. Extract all table references from the parsed query.
2. Determine the operation required for each reference.
3. Resolve table policies from the trusted principal context.
4. Compile row filter expressions against table schemas.
5. Bind filters to query aliases.
6. Attach filters to logical scan/search nodes.
7. Deny the request if any reference cannot be authorized.

This keeps enforcement inside Antfly, where the system has enough context to
handle joins safely. Rewriting request bodies at a proxy or route layer is not
sufficient for cross-table authorization because it cannot reliably see every
logical table access.

## Performance

The authorization path should be efficient enough for production query traffic.

Recommended approach:

- Compile policies once per request and attach them to the plan.
- Cache compiled policy fragments by tenant, principal or role version, table,
  schema version, and policy version.
- Push predicates into table scans, vector indexes, full-text indexes, and graph
  traversal sources.
- Avoid per-row calls to external authorization systems.
- Include policy IDs or versions in audit metadata for explainability.

## V1 Implementation Checklist

For a first join-aware version:

- Define a trusted auth envelope and validation rules.
- Add table-access extraction for parsed queries.
- Add a logical authorization pass before execution planning.
- Compile the row-filter DSL into Antfly predicate nodes.
- Apply filters at scan/search/index access nodes.
- Deny scoped principals from cross-table APIs until the auth pass is active.
- Add audit metadata for principal, tenant, operation, table policies, and
  denied references.

Minimum tests:

- Single-table read with a table-specific row filter.
- Wildcard table grant plus table-specific narrowing.
- Two-table join where both tables have filters.
- Join denied when one table lacks access.
- Aliased join where filters bind to the correct aliases.
- Nested subquery or CTE where inner table filters still apply.
- Outer join where restricted-side filters are applied before join output.
- Vector and full-text searches receive the same table row filters.
- Caller-supplied filters cannot override security predicates.
- Missing principal attributes fail closed.
