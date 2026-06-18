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

## Migration Path

1. Keep `/tables/{table}` as shorthand for the default database and `public`
   namespace.
2. Persist database and namespace metadata in table catalog records before
   exposing explicit cross-namespace APIs.
3. Add catalog objects for databases and namespaces with idempotent create,
   rename, drop, dependency, and authorization semantics.
4. Teach SQL object resolution to use current database plus namespace search
   rules instead of stripping all non-`public` prefixes.
5. Add explicit database/namespace API routes only after the underlying catalog
   identity and authorization checks exist.

