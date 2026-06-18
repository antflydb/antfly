# Databases, Namespaces, and Tables

Antfly should use `database / namespace / table` as the durable catalog model.
The public API can keep the simple `/tables/{table}` shape by resolving omitted
catalog components through defaults.

## Mental Model

Use these terms consistently:

| Antfly | PostgreSQL | MongoDB / document mental model | Elasticsearch mental model |
| --- | --- | --- | --- |
| database | database | database | project or index namespace |
| namespace | schema | namespace or prefix | namespace |
| table | table | collection | index |
| row/document | row | document | document |

`namespace` is intentionally not called `collection`. In MongoDB-style language,
collection means the stored dataset itself. In Antfly, the stored dataset remains
the table; document APIs may call that same table a collection, but collection
should not mean PostgreSQL schema.

## Defaults

Existing table APIs should keep their current shape:

```text
/tables/users
```

That path resolves as:

```text
database = current/default database
namespace = public
table = users
```

The default namespace should be `public` for PostgreSQL compatibility. Product
copy can call it the default namespace, but the persisted default name should
stay `public` unless a future migration intentionally changes it.

Explicit catalog APIs can be added later without breaking the simple path:

```text
/databases/{database}/namespaces/{namespace}/tables/{table}
```

Request-scoped defaults, such as an authenticated current database or an
`X-Antfly-Database` header, can select the database for shorthand table APIs.
Those defaults are API sugar; storage and metadata should carry fully qualified
catalog identity.

## SQL Mapping

SQL compatibility should map directly onto the same model:

```sql
CREATE TABLE users (...);
-- current_database.public.users

CREATE TABLE analytics.users (...);
-- current_database.analytics.users

CREATE SCHEMA analytics;
-- creates namespace analytics in the current database

CREATE DATABASE tenant_ops;
-- creates database tenant_ops
```

PostgreSQL three-part object references should remain fail-closed until Antfly
has explicit cross-database resolution and authorization semantics. Two-part
references are namespace-qualified names inside the current database.

## Catalog Identity

The catalog should distinguish the human name from stable identity:

```text
database_id
namespace_name or namespace_id
table_name
table_id
```

`table_id` remains the stable storage/routing identity. Database and namespace
names participate in lookup, authorization, backup/restore scope, and SQL name
resolution. Renaming a namespace or table should update catalog metadata without
rewriting storage keys unless the storage layer has a deliberate identity
rewrite plan.

## Tablespaces

Tablespaces should model placement and storage-class policy, not local
filesystem paths. A tablespace can later select:

- placement role
- replica policy
- storage tier
- lake/cache policy
- retention class
- encryption or compliance class

SQL `CREATE TABLESPACE ... LOCATION` should stay a compatibility syntax that
captures typed placement intent only after Antfly owns the corresponding
catalog object. It must not silently redirect storage paths.

## Document Collections

Document APIs can expose a table as a collection:

```text
/collections/users
```

or use collection wording in SDKs, but that should be an alias for:

```text
/tables/users
```

under the selected database and namespace. This keeps document, relational,
search, graph, backup, auth, extension, and change-feed features on one table
catalog object instead of splitting them across parallel collection and table
systems.

## API, MCP, A2A, CLI, and Auth Integration

OpenAPI should be the contract for explicit database and namespace operations.
MCP, A2A, CLI, SDKs, and SQL adapters should bind to the same generated API
operations and shared catalog-resolution helpers rather than each inventing
separate catalog semantics.

Use one request-level target model everywhere. The model is deliberately close
to PostgreSQL naming, but it is not SQL-only; document, search, graph, MCP, A2A,
CLI, backup, and extension code should resolve through the same object:

```text
database: optional, defaults to current/default database
namespace: optional, defaults to public
table: required for table operations
```

The resolved target should be carried internally as a typed catalog target, not
as a pre-concatenated table string:

```text
CatalogTarget {
  database_name
  namespace_name
  table_name?
}
```

OpenAPI request types, SQL lowering, MCP tool handlers, A2A task handlers, CLI
commands, and authorization checks should all call the same target resolver.
That resolver owns defaulting, validation, three-part-name rejection, future
search-path behavior, and audit metadata. Each integration layer can accept
ergonomic shorthand, but none of them should reimplement catalog parsing.

The existing shorthand remains:

```text
/db/v1/tables/{table}
```

and resolves to:

```text
default.public.{table}
```

Explicit routes should be added when the backing catalog and authorization
checks exist:

```text
/db/v1/databases
/db/v1/databases/{database}
/db/v1/databases/{database}/namespaces
/db/v1/databases/{database}/namespaces/{namespace}
/db/v1/databases/{database}/namespaces/{namespace}/tables
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}
```

The generated API types should expose `database` and `namespace` as structured
fields where possible. When an older endpoint only has `{table}`, the server
must resolve that table through the same defaulting rule before planning,
authorization, audit, and routing. New explicit endpoints should not be
implemented as string rewrites into old `/tables/{table}` handlers; they should
share the handler core after target resolution so non-default namespaces remain
real catalog identity.

MCP tools should be catalog-aware wrappers over those OpenAPI operations:

```text
list_tables(database?, namespace?)
create_table(database?, namespace?, table, schema, ...)
query_table(database?, namespace?, table, query)
execute_sql(database?, sql)
```

MCP must not bypass Antfly authorization. A trusted MCP principal maps into the
normal authenticated identity model, including role memberships, role settings,
row filters, and extension capability grants. Extension-owned MCP tools should
run with the intersection of caller permissions and the extension install's
declared capabilities.

MCP tool schemas can remain protocol-native, but their fields should mirror the
generated OpenAPI request fields. For example, `query_table` should pass
`database`, `namespace`, and `table` into the OpenAPI-backed request builder,
then execute as the caller's Antfly principal. If MCP later supports per-session
default database or namespace, those defaults should be session metadata that
feeds the shared resolver rather than hidden tool-specific state.

A2A agent cards should advertise catalog-scoped skills, such as:

```json
{
  "skill": "query_table",
  "scopes": ["database:tenant_ops", "namespace:analytics", "table:events"]
}
```

A2A tasks should execute as a delegated principal. If an agent queries
`analytics.events`, it must pass the same authorization checks as HTTP, SQL,
MCP, and CLI calls. There should be no separate agent superuser path.

A2A skill declarations should describe the maximum catalog scope the agent can
use, while each task still resolves and authorizes the concrete target at
execution time. Delegation should preserve the user, service account, role
membership, extension capability grants, and row-level settings used by ordinary
API calls. Agent cards are discovery metadata; they are not permission grants.

The CLI should be a thin generated-client wrapper around the OpenAPI surface:

```sh
antfly database create tenant_ops
antfly namespace create analytics --database tenant_ops
antfly table create events --database tenant_ops --namespace analytics
antfly sql --database tenant_ops 'CREATE TABLE analytics.events (...)'
```

The CLI can carry local defaults for ergonomics:

```sh
antfly config set current-database tenant_ops
antfly config set current-namespace analytics
antfly table list
```

Explicit flags always override local defaults.

The CLI should persist defaults only in local client configuration. Server-side
effects must always include the resolved database and namespace in the request
or derive them from authenticated server-side defaults. Scripts should prefer
explicit `--database` and `--namespace` flags so catalog intent survives moving
between environments.

The user and role system should authorize qualified resources:

```text
database:tenant_ops
namespace:tenant_ops.analytics
table:tenant_ops.analytics.events
```

SQL authorization should map onto those resource names:

```sql
GRANT USAGE ON DATABASE tenant_ops TO app_reader;
GRANT USAGE ON SCHEMA analytics TO app_reader;
GRANT SELECT ON TABLE analytics.events TO app_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO app_reader;
```

Unqualified grants remain compatibility sugar for the current database and
`public` namespace. Role settings such as `app.tenant_id` remain useful for row
security. Settings that imply native catalog behavior, such as `search_path` or
`current_database`, should stay fail-closed until Antfly has real native
semantics for them.

Authorization should be checked at the most specific resource needed by the
operation and may require parent access where the operation depends on parent
catalog visibility:

```text
database read/write/admin    database lifecycle, namespace listing, backup scope
namespace read/write/admin   namespace lifecycle and table listing
table read/write/admin       row reads, row writes, table DDL, indexes
```

For compatibility, old bare table permissions may be interpreted as
`default.public.<table>` during migration, but new grants and audit records
should write the qualified resource. SQL `GRANT` and `REVOKE`, OpenAPI auth
admin endpoints, MCP tools, A2A delegated tasks, and CLI commands should all
produce or consume the same resource strings. This keeps a role granted access
to `tenant_ops.analytics.events` from accidentally gaining access to
`default.public.events`.

Cross-surface parity tests should prove at least these cases:

```text
HTTP shorthand /tables/events resolves to default.public.events.
HTTP explicit /databases/tenant_ops/namespaces/analytics/tables/events resolves
  to tenant_ops.analytics.events.
SQL SELECT FROM analytics.events resolves to current_database.analytics.events.
MCP query_table(database=tenant_ops, namespace=analytics, table=events) resolves
  and authorizes the same target as HTTP.
A2A delegated query_table tasks use the same principal and target.
CLI table commands emit the same OpenAPI requests as hand-written HTTP calls.
Qualified table grants do not leak across databases or namespaces.
```

### Integration Contract by Surface

The database and namespace model should integrate by making OpenAPI plus the
shared catalog resolver the only semantic boundary. Other protocols are
adapters:

| Surface | Role in the model | Required behavior |
| --- | --- | --- |
| OpenAPI / REST | Canonical public contract | Exposes explicit database, namespace, and table routes; resolves shorthand through the shared catalog resolver before authorization and routing. |
| SQL / relational | PostgreSQL-compatible syntax layer | Lowers `CREATE DATABASE`, `CREATE SCHEMA`, qualified table DDL/DML, `GRANT`, and `REVOKE` into the same catalog and auth operations used by REST. |
| MCP | Tool protocol adapter | Mirrors OpenAPI request fields in tool schemas, maps the MCP session to an Antfly principal, and calls the same REST/service handlers instead of maintaining protocol-local catalog state. |
| A2A | Delegated-agent adapter | Advertises catalog-scoped skills as discovery metadata, then executes each task as a delegated Antfly principal against the same resolver and authorization checks. |
| CLI | Ergonomic generated-client wrapper | Stores local defaults for convenience, sends explicit database/namespace fields when present, and never creates server-only semantics that cannot be expressed through OpenAPI. |
| User/role system | Authorization authority | Stores qualified resources (`database:name`, `namespace:db.ns`, `table:db.ns.table`) and evaluates inherited role grants consistently for HTTP, SQL, MCP, A2A, and CLI callers. |

This means MCP, A2A, and CLI should not each gain their own idea of "current
database" or "current namespace" beyond client/session defaults that feed the
shared resolver. The resolved catalog target, authenticated principal, role
memberships, extension capability grants, row-security settings, and audit
metadata must be identical no matter which surface initiated the operation.

The user/role system remains the enforcement point. Parent resources authorize
catalog visibility and lifecycle operations, while the concrete table resource
authorizes row reads, row writes, table DDL, indexes, change feeds, and
extension-owned table behavior:

```text
database:tenant_ops              database lifecycle, namespace listing, backup scope
namespace:tenant_ops.analytics   namespace lifecycle and table listing
table:tenant_ops.analytics.events row/query/write/index/change-feed access
```

Delegated protocols should intersect, not expand, authority. An MCP tool
provided by an extension runs with the caller's permissions intersected with the
extension install's declared capabilities. An A2A task runs with the delegated
principal's roles and task-scoped catalog limits. A CLI command runs as the
configured user or service account. None of those paths should have an implicit
superuser mode.

## Migration Path

1. Keep `/tables/{table}` as shorthand for the default database and `public`
   namespace.
2. Persist database and namespace metadata in table catalog records before
   exposing explicit cross-namespace APIs.
3. Add catalog objects for databases and namespaces with idempotent create,
   rename, drop, dependency, and authorization semantics.
4. Teach SQL object resolution to use current database plus namespace search
   rules instead of stripping all non-`public` prefixes.
5. Add explicit database/namespace OpenAPI routes only after the underlying catalog
   identity and authorization checks exist.
6. Bind MCP, A2A, CLI, and SDK helpers to the generated OpenAPI operations and
   shared target-resolution helpers.
7. Extend user and role permissions from bare table names to qualified
   database/namespace/table resources, then teach SQL `GRANT`/`REVOKE` to emit
   those qualified permissions.
8. Add parity tests proving the same principal can or cannot access a qualified
   table consistently through HTTP, SQL, MCP, A2A, and CLI paths.
