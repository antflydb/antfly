# Native catalog

Databases and namespaces group the existing document tables. The catalog is
independent of SQL, relational rows, and lake storage. `default.public` exists
for existing installations; short table names keep that scope.

## Names and identity

Use `/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}` for an
explicit target. Existing `/db/v1/tables/{table}` operations also accept
`database.namespace.table` and `namespace.table` (in the default database).
CLI `--table`, MCP `tableName`, and global query table names use these spellings.
Names contain ASCII letters, digits, underscores, or hyphens, begin with a
letter or underscore, and have at most 128 bytes per component.

Database, namespace, and table renames preserve IDs. A table's physical name,
shard groups, stored destination grants, and storage remain stable. Public
`TableStatus.table_id` is a decimal string so JavaScript clients retain all 64
bits. Physical names are internal and cannot be supplied as public aliases.
An old logical name stops resolving after rename.

Creating a database also creates its `public` namespace. Empty databases and
namespaces can be dropped; a database drop also removes its empty namespaces.
The compatibility database `default` and its `public` namespace are protected.

## Lifecycle and placement

The generated OpenAPI contract defines database, namespace, and tablespace
create/list/drop operations, database and tablespace lookups, rename operations, and explicit table query,
batch, document, index, backup, and restore routes. Rename uses
`POST .../rename` with `{"name":"new_name"}`.

Create a tablespace with `POST /db/v1/tablespaces/hot`:

```json
{
  "location_json": "null",
  "placement_policy_json": "{\"placement_role\":\"data\",\"desired_replica_count\":3,\"min_ranges\":2}"
}
```

Bind a database, namespace, or table with `PUT .../tablespace` and
`{"tablespace_name":"hot"}`; `DELETE .../tablespace` clears that binding.
A create-table body can set `tablespace_name` explicitly. Effective precedence
is table, namespace, database, then the normal native defaults. An explicit
`num_shards` overrides inherited `min_ranges` when creating a table.

Parent binding changes select defaults for future table creation. Changing an
existing table's binding atomically updates its native placement policy; the
normal reconciler performs any resulting placement work. Standalone retains its single local replica; Lite retains its single-range constraint. Clearing its binding
reapplies inherited policy or native defaults. Placement roles must be nonempty
name-like strings; replica counts are 1–255 and minimum ranges 1–1024.
Tablespaces with references cannot be dropped. Renaming a tablespace preserves
its bindings. `location_json` is opaque compatibility metadata: it does not
redirect filesystem paths or choose a storage engine.

## Authorization and durability

Permission resource types include `database`, `namespace`, and `tablespace`.
Table permissions and row filters use `database.namespace.table`; an old short
name refers to `default.public`. Table grants may use a terminal scoped wildcard
such as `analytics.public.*`. Catalog mutations require admin authority on the
resource; rename also requires authority on its destination name. Explicit
policy selection requires read permission on that tablespace. Resolution occurs
after authorization, and physical table grants/row filters exist only within
the authorized request.

Catalog records, name indexes, revisions, and table topology commit through
metadata Raft. Standalone persists the same logical state with table topology in
its existing atomic catalog checkpoint and rollback boundary. A table create or restore publishes the binding and topology
atomically. Snapshot installation and reopen retain the catalog. Native catalog
admission requires topology protocol version 5 throughout the metadata group;
existing atomic table operations retain their version-3 gate.

An ambiguous mutation response carries the existing `unknown` outcome contract;
observe state before retrying. A committed mutation whose local visibility is
still converging can return HTTP 202. Qualified restore uses the existing
restore job durability and cancellation machinery, with an immutable
physical destination identity persisted in the job.

## Clients

The Go and Python generated clients expose the new operations. The TypeScript
SDK exposes all generated routes through its typed `client.api` accessor:

```ts
await client.api.POST("/db/v1/databases/{databaseName}", {
  params: { path: { databaseName: "analytics" } },
});
```

The CLI provides `database`, `namespace`, and `tablespace` commands, including
rename and tablespace binding commands. Existing table and document commands
accept qualified names, for example `--table analytics.public.events`.
