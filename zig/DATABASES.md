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

Explicit catalog APIs expose the full catalog identity without breaking the
simple path:

```text
/databases/{database}/namespaces/{namespace}/tables/{table}
```

Shorthand table APIs resolve through a strict precedence order:

1. explicit route or body catalog fields
2. authenticated server-side session or principal defaults, once Antfly has a
   durable native setting model
3. `default.public`

Client-supplied defaulting headers, such as `X-Antfly-Database`, must remain
unsupported until they are part of the generated OpenAPI contract, authorization
checks, and audit log. Storage and metadata should always carry fully qualified
catalog identity after resolution.

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
The SQL adapter-specific parser, session, `search_path`, and lowering contract
is documented in [SQL.md](SQL.md); this document owns the shared catalog
identity model that SQL resolves into.

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

## Physical Storage Identity

`database.namespace.table` is the logical catalog name. It is the name users see
in SQL, REST, MCP, A2A, CLI, audit logs, grants, and table listings. It should
not be the long-term physical storage key.

The storage layer should route by stable IDs derived from the catalog table
record:

```text
table:<table_id>
range:<range_id>
shard:<shard_id>
```

The table name, database name, and namespace name are mutable catalog metadata.
The table ID and range IDs are the durable storage and routing identity. A table
rename, namespace move, or future schema search-path change should update
catalog rows and authorization resources without requiring LSM, HBC, full text,
algebraic, backup, restore, lake-cache, or serverless-index storage to be
rewritten.

Current compatibility storage names are transitional:

```text
users                         # legacy shorthand for default.public.users
tenant_ops.analytics.events   # compatibility qualified table name
```

Those strings may remain lookup aliases while old APIs and existing on-disk
state migrate. New code should carry both the resolved logical target and the
catalog table record, then derive backend storage identity from `table_id` and
range metadata. Tablespaces stay separate from physical names: they select
placement, storage class, replica policy, cache/lake policy, retention, and
compliance behavior, but do not become path prefixes.

## Tablespaces

Tablespaces should model placement and storage-class policy, not local
filesystem paths. They should be first-class catalog resources:

```text
tablespace:fastspace
```

Lifecycle operations are admin operations on the `tablespace:<name>` resource.
SQL `CREATE TABLESPACE`, `ALTER TABLESPACE ... RENAME TO`, `DROP TABLESPACE`,
and REST `/db/v1/tablespaces` calls mutate the same metadata-backed catalog
object. Table creation can durably bind `tablespace_name`; that binding
validates the tablespace exists, is shown in table status responses, is updated
on tablespace rename, and prevents tablespace drop while referenced.

Database- and namespace-level bindings are public catalog operations through
the explicit `/databases/{database}/tablespace` and
`/databases/{database}/namespaces/{namespace}/tablespace` endpoints. Table
creation resolves effective placement in this order:

1. table `tablespace_name`
2. namespace `tablespace_name`
3. database `tablespace_name`
4. no tablespace binding

A tablespace can select:

- placement role
- replica policy
- storage tier
- lake/cache policy
- retention class
- encryption or compliance class

SQL `CREATE TABLESPACE ... LOCATION` is compatibility syntax. The location is
stored as catalog metadata and must not silently redirect local filesystem
paths. Native REST creation may also carry `placement_policy_json`; that policy
is durable catalog metadata. The current production policy bridge maps
`placement_role`, `desired_replica_count` / `replica_count`, and `min_ranges`
onto native table placement fields during table creation. Other storage, lake,
retention, encryption, and compliance policy fields remain durable metadata
until native schedulers consume them directly.

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

Explicit catalog routes are public API when they are present in OpenAPI:

```text
/db/v1/databases
/db/v1/databases/{database}
/db/v1/databases/{database}/tablespace
/db/v1/databases/{database}/namespaces
/db/v1/databases/{database}/namespaces/{namespace}
/db/v1/databases/{database}/namespaces/{namespace}/tablespace
/db/v1/databases/{database}/namespaces/{namespace}/tables
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}/query
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}/batch
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}/rows/batch
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}/documents/{key}
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}/indexes
/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}/indexes/{index}
/db/v1/tablespaces
/db/v1/tablespaces/{tablespace}
```

Generated artifact operations currently use the shorthand table route and
resolve through the same `default.public.{table}` rule:

```text
/db/v1/tables/{table}/artifacts
/db/v1/tables/{table}/artifacts/{artifact}/enrichment
/db/v1/tables/{table}/artifacts/{artifact}/reprocess
/db/v1/tables/{table}/artifacts/{artifact}/reprocess-jobs
/db/v1/tables/{table}/artifacts/{artifact}/reprocess-jobs/{job}
/db/v1/tables/{table}/artifacts/{artifact}/reprocess-jobs/{job}/advance
/db/v1/tables/{table}/artifacts/{artifact}/reprocess-jobs/{job}/cancel
/db/v1/tables/{table}/documents/{key}/artifacts
/db/v1/tables/{table}/documents/{key}/artifacts/{artifact}
/db/v1/tables/{table}/documents/{key}/artifacts/{artifact}/reprocess
```

The generated API types should expose `database` and `namespace` as structured
fields where possible. When an older endpoint only has `{table}`, the server
must resolve that table through the same defaulting rule before planning,
authorization, audit, and routing. New explicit endpoints should not be
implemented as string rewrites into old `/tables/{table}` handlers; they should
share the handler core after target resolution so non-default namespaces remain
real catalog identity.

Explicit table I/O routes pass a typed catalog target through the REST and
read/write API boundary. Provisioned and hosted table read/write sources expose
native catalog-target vtable methods for query, batch, indexes, runtime status,
table lifecycle, backup, and restore calls. Those methods resolve the catalog
table first, keep the typed logical target for authorization and audit, and pass
the stable `table_id` / range identity to backend storage. Direct foreign-source
query dispatch keys the source metadata by the same typed catalog target instead
of a compatibility storage key. Legacy `/tables/{table}` shorthand still maps
to `default.public.{table}` before authorization and routing, but the backend
storage identity should be derived from the catalog table record after
resolution.

While the current read/write APIs still carry a single table string through
catalog lookup and backend open/cache paths, `storageTableNameForTargetAlloc` is
a compatibility bridge. It preserves the legacy raw table name for
`default.public.<table>` and a qualified string for non-default targets. The
long-term handler shape is:

```text
CatalogTableOperation {
  logical_target: TableTarget,
  table_record: TableRecord,
  physical_identity: table:<table_id> plus range/shard metadata
}
```

Callers should not build physical storage names from logical names once that
operation model is available.

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
  "scopes": ["database:tenant_ops", "namespace:tenant_ops.analytics", "table:tenant_ops.analytics.events"]
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
antfly table create --table events --database tenant_ops --namespace analytics
```

The CLI can carry process-local defaults for ergonomics:

```sh
export ANTFLY_DATABASE=tenant_ops
export ANTFLY_NAMESPACE=analytics
antfly table list
```

Explicit flags always override environment defaults.

The CLI should persist defaults only in local client configuration if a future
config command is added. Server-side effects must always include the resolved
database and namespace in the request
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
-- Execute with current database tenant_ops.
GRANT USAGE ON DATABASE tenant_ops TO app_reader;
GRANT USAGE ON SCHEMA analytics TO app_reader;
GRANT SELECT ON TABLE analytics.events TO app_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO app_reader;
```

Unqualified grants remain compatibility sugar for the current database and
`public` namespace. Role settings such as `app.tenant_id` remain useful for row
security. SQL `SET search_path`, `RESET search_path`, `SHOW search_path`, and
`DISCARD ALL` now lower to typed session catalog plans; applying those plans
mutates an explicit SQL catalog session used by SQL catalog resolution rather
than silently rewriting table names. `current_database` is still an explicit
session input until Antfly owns a connection protocol with durable server-side
session state.

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

Explicit REST subroutes and MCP table tools must resolve catalog targets through
the shared resolver before table I/O. Read/write vtables expose catalog-target
entry points, and provisioned/hosted backends implement those methods directly.

A2A agent cards advertise wildcard catalog scopes as discovery metadata.
Query-builder and retrieval task handlers normalize explicit
`database`/`namespace`/`table` task data into a qualified catalog resource before
dispatch, then rely on the same delegated principal and table authorization path
as HTTP and MCP.

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
| Tablespaces | Placement policy catalog | Stores named placement/storage policy resources, authorizes lifecycle through `tablespace:<name>`, exposes database/namespace/table bindings through OpenAPI, and maps supported policy keys into native table placement metadata. |

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

Completed baseline:

1. Keep `/tables/{table}` as shorthand for `default.public.{table}`.
2. Persist database and namespace metadata in table catalog records.
3. Add catalog objects for databases and namespaces with create/drop,
   dependency, and authorization checks.
4. Expose explicit database, namespace, and table lifecycle routes through
   OpenAPI.
5. Extend user and role permissions from bare table names to qualified
   database/namespace/table resources, while preserving default-public migration
   compatibility.
6. Add metadata-backed tablespace lifecycle through SQL and OpenAPI, including
   `tablespace` role resources, durable table `tablespace_name` bindings,
   database/namespace binding endpoints, placement policy metadata, native
   placement-field projection, rename propagation, and drop dependency checks.
7. Add SQL catalog session resolution for current database and search path in
   the shared catalog DDL plan API.
8. Add CLI helpers for database, namespace, table, index, query, document I/O,
   tablespace lifecycle operations, and database/namespace tablespace bindings.

Transitional bridge:

1. Public explicit catalog table I/O routes (`query`, `batch`, `rows/batch`,
   `documents/{key}`, `indexes`, generated artifact inspection/reprocessing,
   `backup`, and `restore`) resolve typed catalog
   targets first, then use catalog-aware read/write vtable methods. The current
   implementation still uses compatibility storage names where a backend entry
   point only accepts one table string; the bridge must disappear once those
   entry points accept both logical target and physical table identity.
2. MCP table tools call those same public routes and inherit the same typed
   target resolution.
3. A2A query-builder and retrieval task handlers normalize explicit
   `database`/`namespace`/`table` task data into the same catalog-derived table
   identity before dispatch and still authorize the concrete table resource at
   execution time.

Production hardening now in place:

1. Explicit catalog table I/O requires native catalog-target vtables; missing
   backend support fails closed instead of falling back to compatibility storage
   keys. Backup and restore have catalog vtable coverage alongside query,
   batch, document lookup, index, status, and table lifecycle operations.
2. Tablespace placement policy validation and table-creation projection both
   consume the same supported native policy keys (`placement_role`,
   `desired_replica_count` / `replica_count`, and `min_ranges`). Unknown
   placement keys fail closed until a scheduler implements their semantics.
3. SQL `SET search_path`, `RESET search_path`, `SHOW search_path`, and
   `DISCARD ALL` use typed session catalog plans and mutate an explicit SQL
   catalog session for subsequent resolution.
4. The in-repo parity gates cover SQL catalog/session fixtures, generated
   OpenAPI freshness, explicit REST catalog routes, MCP route generation, A2A
   catalog task normalization, CLI surface compilation, and qualified
   role-resource checks.

Future expansion:

1. Split table read/write internals so catalog lookup returns a `TableRecord`
   and backend open/cache/routing consumes `table:<table_id>` plus range/shard
   identity. Compatibility names remain aliases only for migration and rollback.
2. Migrate LSM, HBC, full text, algebraic, document-row, backup/restore,
   lake-cache, and serverless-index metadata to table-id physical identity.
   Group and range resolution should never depend on mutable human table names.
3. Teach placement/storage schedulers to consume future tablespace policy fields
   for storage class, lake tier, retention, encryption, and compliance once
   those native schedulers exist.
