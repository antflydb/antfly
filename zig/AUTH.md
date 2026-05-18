# Auth

This document records the current Antfly auth model and the planned document-level security shape for user/tenant-aware row filters.

## Current Auth Context

When `auth_enabled` is true, public non-internal routes authenticate through the configured user manager.

Supported credential forms:

- `Basic <base64(username:password)>`
- `ApiKey <base64(key_id:key_secret)>`
- `Bearer <base64(key_id:key_secret)>`

The request auth layer builds an `AuthenticatedIdentity` with:

- `username`: authenticated username, or API-key owner username
- `permissions`: table/user/global permissions used for route authorization
- `row_filter`: stored row filter policies attached to the user or API key
- `roles`: inherited Casbin grouping subjects for the authenticated user
- `metadata`: trusted user metadata used by stored row-filter `$auth` references

Route authorization is coarse-grained and permission based. Document-level read narrowing is enforced separately by row filters.

## Row Filter Enforcement

Row filters are stored as query JSON per table name, with `*` as a wildcard table fallback.

At read time the server:

1. Finds the table-specific row filter, or the wildcard row filter.
2. Resolves trusted auth-context references in that stored filter.
3. Conjoins the resolved filter with any user-supplied query filter.
4. Executes the resulting query, scan, or lookup filter.

Row-filter writes are also validated before storage. A `$auth` node must be an object with exactly one field, and the field value must be a supported auth path.

This keeps access control server-owned. Client-supplied queries may narrow their own results, but they are not trusted to enforce access control.

## Canonical Auth Reference Form

The canonical stored representation should be typed, not a whole-query template:

```json
{
  "term": {
    "tenant_id": { "$auth": "metadata.tenant_id" }
  }
}
```

The resolver expands `$auth` nodes after authentication and before query execution. The resolved filter must be normal query JSON:

```json
{
  "term": {
    "tenant_id": "acme"
  }
}
```

This is intentionally similar to Elasticsearch document-level security user templates, but the stored Antfly representation remains an inspectable JSON AST. Whole-query Handlebars rendering should not be the default.

## Why Not Template Every Query

Antfly already has two separate styles:

- Transform DSL: structured Mongo-style mutation operations such as `$set`, `$inc`, and `$addToSet`.
- Handlebars templates: rendering text, document prompts, keys, and source-derived values.

Security filters should follow the transform style: structured query shape with explicit dynamic value nodes. Templating every query would make every query parse two-phase, introduce JSON escaping/injection risks, weaken validation, and blur trusted policy from untrusted user query.

## Planned Auth Context Shape

The target auth context is:

```json
{
  "username": "alice",
  "roles": ["reader", "tenant_member"],
  "metadata": {
    "tenant_id": "acme",
    "department": "eng",
    "groups": ["search", "infra"]
  },
  "auth_type": "user"
}
```

API keys should have explicit semantics:

- `username`: the authenticated username, or the API-key owner username.
- Metadata should either inherit from the owner or merge owner metadata with key metadata by well-defined narrowing rules.
- Key metadata should not silently broaden access.
- Row filters inherit the owner's effective user/role filters; key-local filters apply as an additional narrowing layer.
- An auth type such as `user` vs `api_key` may be exposed when policies need to distinguish credential class.

## Supported First Slice

The implemented auth references resolve username, inherited roles, and user metadata in stored row filters:

```json
{ "term": { "owner": { "$auth": "username" } } }
```

```json
{ "term": { "tenant_id": { "$auth": "metadata.tenant_id" } } }
```

```json
{ "terms": { "acl.roles": { "$auth": "roles" } } }
```

Metadata and roles come from the authenticated user. API-key authentication currently inherits the API-key owner's user metadata and roles.

Do not add aliases such as `user.username` or `_user.username` unless a future auth-context field needs that distinction. Keeping the first DSL surface to `username` makes stored policy easier to read, validate, and migrate.

`$auth: "roles"` resolves to an array of inherited grouping subjects. That is useful with array-aware query operators such as `terms`; it should not be used with scalar-only operators.

## Roles And Groups

Antfly's default Casbin model already has the shape we need:

```ini
[policy_definition]
p = sub, typ, obj, act
p2 = sub, obj, filter

[role_definition]
g = _, _
```

`p` grants coarse permissions. `p2` stores row filters. `g` expresses subject inheritance.

Roles and groups are both Casbin subjects. Use prefixes to keep intent clear:

- `role:tenant_reader`
- `role:admin`
- `group:eng`
- `group:search`

Membership examples:

```text
g, alice, role:tenant_reader
g, alice, group:eng
g, group:eng, role:internal_reader
```

Effective permissions and effective row filters should follow the same inheritance rule: collect direct user policies plus policies attached to inherited `g` subjects. When multiple row filters apply to the same table, Antfly conjoins them.

For document ACLs, prefer metadata when the document stores business groups:

```json
{
  "terms": {
    "acl.groups": { "$auth": "metadata.groups" }
  }
}
```

Use Casbin `g` groups for authorization inheritance. Use user metadata groups for query-time document matching. They can share names, but they are not the same field.

## Auth Management API

The canonical auth management API is namespaced under `/auth/v1`. The older top-level `/users` shape should not be kept as a compatibility alias while this surface is still settling.

Canonical route shape:

```text
GET    /auth/v1/me
GET    /auth/v1/users
POST   /auth/v1/users/{userName}
GET    /auth/v1/users/{userName}
DELETE /auth/v1/users/{userName}
PUT    /auth/v1/users/{userName}/password

GET    /auth/v1/users/{userName}/permissions
POST   /auth/v1/users/{userName}/permissions
DELETE /auth/v1/users/{userName}/permissions?resource=...&resourceType=...

GET    /auth/v1/users/{userName}/roles
POST   /auth/v1/users/{userName}/roles
DELETE /auth/v1/users/{userName}/roles?role=...

GET    /auth/v1/users/{userName}/row-filters
GET    /auth/v1/users/{userName}/row-filters/{table}
PUT    /auth/v1/users/{userName}/row-filters/{table}
DELETE /auth/v1/users/{userName}/row-filters/{table}

GET    /auth/v1/users/{userName}/api-keys
POST   /auth/v1/users/{userName}/api-keys
DELETE /auth/v1/users/{userName}/api-keys/{keyId}

GET    /auth/v1/subjects
GET    /auth/v1/subjects/{subject}/row-filters
GET    /auth/v1/subjects/{subject}/row-filters/{table}
PUT    /auth/v1/subjects/{subject}/row-filters/{table}
DELETE /auth/v1/subjects/{subject}/row-filters/{table}
```

`GET /auth/v1/subjects` should be read-only discovery derived from Casbin policy state, not a separate subject registry.

## Startup Auth Model

Antfly should keep the canonical runtime model fixed for now:

- `p = sub, typ, obj, act`
- `p2 = sub, obj, filter`
- `g = _, _`

The server may accept startup configuration for users, metadata, roles, groups, memberships, permissions, and row filters. That is safer than accepting an arbitrary Casbin model because the runtime assumes the `p`, `p2`, and `g` shapes above.

A custom Casbin model can be an advanced future option, but it must validate that Antfly's required policy shapes still exist or provide explicit adapters for permission enforcement and row-filter extraction. Arbitrary model text should not silently change row-filter semantics.

## Examples

Per-owner access:

```json
{
  "term": {
    "owner": { "$auth": "username" }
  }
}
```

Tenant access, once metadata exists:

```json
{
  "term": {
    "tenant_id": { "$auth": "metadata.tenant_id" }
  }
}
```

Tenant and owner access:

```json
{
  "conjuncts": [
    { "term": { "tenant_id": { "$auth": "metadata.tenant_id" } } },
    { "term": { "owner": { "$auth": "username" } } }
  ]
}
```

Group ACL access with array-valued metadata and an array-aware `terms` filter:

```json
{
  "terms": {
    "acl.groups": { "$auth": "metadata.groups" }
  }
}
```

## Implementation Plan

1. Resolve `$auth` nodes in stored row filters before existing row-filter injection and scan/lookup evaluation.
2. Keep row-filter selection unchanged: table-specific first, wildcard second.
3. Validate stored row filters before persistence and validate resolved filters by parsing/evaluating them as normal query JSON.
4. Add API-key metadata if key-local narrowing is needed.
5. Keep roles/groups in Casbin `g` and collect inherited `p2` row filters for the effective authenticated user.
6. Resolve `$auth: "roles"` to inherited `g` subjects for array-aware policy queries.
7. Add optional startup configuration for users, metadata, memberships, permissions, and row filters.
8. Add optional authoring sugar only if needed, such as Elastic-style templates, but compile it into the canonical typed form before storage.
