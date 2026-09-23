# Antfly Authorization

This document describes Antfly's data-plane authorization model: what is
implemented today, then the design for join-aware enforcement that is still
planned. The model is provider-neutral: Antfly can receive principal context
from its built-in user manager, a trusted gateway, a managed control plane, or
a self-hosted deployment.

## Current Implementation

### Credentials

When `auth_enabled` is true, public routes authenticate through the built-in
user manager (`/auth/v1`) with one of:

- `Authorization: Basic <base64(username:password)>`
- `Authorization: ApiKey <base64(key_id:key_secret)>`
- `Authorization: Bearer <base64(key_id:key_secret)>`

An API key can only narrow its owner's permissions, and that narrowing is
re-applied on every use.

### Trusted Principal Tokens

A gateway in front of Antfly can instead send an HS256-signed JWT in the
`X-Antfly-Trusted-Principal` header. Antfly verifies it with the keystore secret
`antfly.trusted_principal.secret` and, when `antfly.trusted_principal.issuer` is
set, requires a matching `iss`. The claims are:

```json
{
  "iss": "gateway",
  "sub": "user_123",
  "exp": 1790000000,
  "iat": 1789996400,
  "tables": ["orders", "customers"],
  "operations": ["read"],
  "row_filter": {
    "orders": { "term": { "tenant_id": { "$auth": "metadata.tenant_id" } } },
    "*": { "term": { "region": "na" } }
  },
  "metadata": { "tenant_id": "tenant_abc" }
}
```

- `sub` and `exp` are required; a token whose `iat` is more than 60 seconds in
  the future is rejected.
- `admin: true` grants admin on every table. Otherwise `operations` (`read`,
  `write`, `admin`, or `*`) is granted on each table in `tables`, or on every
  table when `tables` is absent or empty.
- `row_filter` maps a table name, or `*` for every table, to a row filter.
- `metadata` supplies the values that `$auth` references resolve against.

### Row Filters

Row filters are query JSON stored per table, with `*` as a fallback for every
table. They are attached to users, roles, and groups through
`/auth/v1/users/{user}/row-filters/{table}` and
`/auth/v1/subjects/{subject}/row-filters/{table}`, or carried in a trusted
principal token. A filter references trusted values with `$auth` nodes:

```json
{ "term": { "tenant_id": { "$auth": "metadata.tenant_id" } } }
```

`$auth` accepts `username`, `roles` (the user's inherited role and group
subjects, for array-aware operators such as `terms`), and `metadata.<path>`.
Filters are validated before they are stored.

At read time, Antfly:

1. Selects the table-specific filter if one exists, and otherwise the `*`
   filter. The two are not combined.
2. Combines filters from the same table with `AND`: the user's own filter, the
   filters of every role and group the user inherits, and an API key's own
   filter.
3. Resolves `$auth` references against the authenticated identity.
4. Combines the result with the caller's query with `AND`.

Queries, document scans, lookups, and retrieval-agent queries apply these
filters. In a join, the right table's filter is applied as well.

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

## Planned: Join-Aware Enforcement

The rest of this document is the design for enforcing authorization inside the
query planner, for cross-table queries such as SQL joins, subqueries, and views.
It is not implemented yet. Where it differs from the current behavior above,
the current behavior is what Antfly does today.

### Principal Context

Requests should carry a trusted principal context. The context may be created by
Antfly itself or by a trusted component in front of Antfly, but clients must not
be able to forge it.

Proposed shape (the shipped token claims are listed under Trusted Principal
Tokens above):

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

### Operations

Antfly should distinguish at least:

- `read`: query, search, get, list, graph traversal, retrieval.
- `write`: insert, update, delete; implies `read` only when the policy says so.
- `admin`: schema, indexes, table settings, policy metadata; implies `write`
  only when the policy says so.

Authorization should be checked against the operation actually executed, not
only the HTTP method or top-level API route.

### Row Filters

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

### Join-Aware Enforcement

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
- Multiple grant filters for the same table and operation combine with `AND`
  today (see Row Filters above), then the result is `AND`ed with the caller's
  predicate. Whether separate grants should instead widen access with `OR` is
  an open design question.
- If any referenced table lacks permission, deny the entire query.
- Outer joins must filter the restricted side before join evaluation while
  preserving normal join semantics for rows that pass authorization.
- Subqueries, CTEs, unions, graph traversals, vector search, full-text search,
  and retrieval-agent requests must normalize into table access nodes with the
  same authorization constraints.
- Views and saved queries must either expand to their underlying table access
  nodes or be denied unless Antfly can prove the saved object already carries an
  equivalent authorization policy.

### Planning Boundary

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

### Performance

The authorization path should be efficient enough for production query traffic.

Recommended approach:

- Compile policies once per request and attach them to the plan.
- Cache compiled policy fragments by tenant, principal or role version, table,
  schema version, and policy version.
- Push predicates into table scans, vector indexes, full-text indexes, and graph
  traversal sources.
- Avoid per-row calls to external authorization systems.
- Include policy IDs or versions in audit metadata for explainability.

### V1 Implementation Checklist

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
