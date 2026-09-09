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

## Regression coverage and lookup cost

`zig/e2e/antfly/test_native_catalog.py` covers inherited and explicit placement,
namespace isolation, index lifecycle, logical renames across restart, MCP
resolution, and qualified restore with idempotency and maximum-length names.
The catalog cases in `test_auth.py` run against both standalone and separate
metadata/data processes, checking scoped permissions, row filters, NDJSON
per-line authorization, and rename/drop behavior. They run in the regular
Python E2E job.

The focused Zig targets are `native-catalog-test`, `native-catalog-api-test`, and
`native-catalog-standalone-test`. The metadata, HTTP, and standalone test lanes
also include their corresponding catalog regressions. Raft-store tests cover
atomic topology/binding publication, stale revisions, reopen, snapshot install,
and corruption checks. HTTP tests cover ambiguous outcomes, post-commit
projection failures, and request-local aliases with live permission revocation.

Positive indexed lookups return only the immutable table ID and physical name.
The binary table-record reader validates length framing without copying or
parsing schema/index definitions. A regression resolves a record with a 256 KiB
definition using a 16 KiB allocation budget. Negative lookups check the catalog
inventory to distinguish absence from a corrupt name index. Restore-job lists
share one request-owned catalog snapshot and a physical-to-logical name map
between authorization and rendering, including bindings for renamed legacy
tables. A regression checks that 40 jobs use one snapshot and that a subsequent
request reloads the projection before applying grants.

The remaining performance opportunities below are code-path observations,
not measured throughput claims:

- Split-node resolution uses the mutation forwarding driver, which fetches
  metadata status before the catalog RPC, even for reads. A direct read path
  should return and validate authority/incarnation evidence and retain bounded
  rediscovery on leader changes. Skipping identity checks or caching names with
  a TTL would weaken the current correctness guarantees. Global NDJSON queries
  currently repeat resolution per line.
- Query-response name projection parses and serializes the entire response to
  change its table label. Carrying the logical response name to the final
  encoder would eliminate that second body traversal and allocation.
- Namespace table listings collect status and construct responses for all
  tables before filtering, then repeatedly scan catalog bindings. Select the
  relevant identities first and join through a map before collecting status.
- Catalog mutation validation and Raft apply decode all physical table
  definitions to construct an identity inventory. An identity-only inventory
  would reduce serialized metadata work for clusters with large schemas.

These optimizations can be scoped independently of the optional M1–M3 refactors.

The public API smoke E2E currently exposes a correctness gap in joins: primary
table routes resolve catalog names, but native right-hand join targets still
reach physical table lookup unchanged. For example, joining `docs` to
`customers` fails with `TableNotFound` after both tables are created through the
catalog. Resolve all native join targets before planning and execution, while
retaining logical names for authorization, row filters, and response labels;
foreign-source targets must keep their existing semantics. This regression
must be fixed before merge, independently of the performance work above.
