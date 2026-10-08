# Table-level object storage

Status: initial implementation. The catalog/API engine contract is shared across
standalone and distributed deployment. Existing native tables keep their storage
and placement contracts. The serverless command remains the specialized worker
preset.

## Independent choices

A table chooses `storage.engine: native | object`. `native` is the compatibility
default and uses the hosting process's local/Lite persistence and normal shard
placement. `object` uses shared durable objects without data Raft replicas.
`schema.storage_mode` continues to describe document versus relational data;
`schema.base_source` continues to distinguish an external lake from owned data.
Deployment controls process roles, not the table's durable engine.

Examples:

```json
{
  "storage": {"engine": "object"},
  "schema": {
    "storage_mode": "relational",
    "base_source": {
      "kind": "external",
      "table_id": "events",
      "format": "parquet",
      "uri": "s3://lake/events",
      "write_policy": "read_only"
    }
  }
}
```

```json
{
  "storage": {"engine": "object"},
  "indexes": {"search": {"type": "full_text"}}
}
```

The first table reads lake data and maintains native catalog-fenced sidecars. The
second table accepts document batches into the object WAL and queries published
object generations. The object engine is immutable; changing it requires an
explicit migration into another table. Omit `num_shards` and replication sources
on object tables. Dense `vector_store` ownership belongs to native local tables.

## Implementation sequence

1. Expose lake data and durable sidecars through the native table API. Reuse the
   lake readers, builders, publication CAS, reader leases, and collection leases.
2. Admit object tables as catalog objects with zero data ranges and zero data
   replicas. Neither table creation nor inherited tablespace range defaults may
   manufacture data Raft groups for them.
3. Host the existing object WAL/manifest/query/maintenance stack inside a native
   API process for owned document tables. Reuse the existing write acknowledgments,
   synchronization levels, work leases, publication fencing, and recovery logic.
4. Keep the serverless API/query/maintenance/combined commands as deployment
   presets. Their independent scaling is useful even when storage is a table
   property. A native process may serve native and object tables together.

This implementation provides the sequence's initial data paths: lake/SQL reads
and sidecars, plus document batch, key lookup, and search execution using the
object runtime.
Writable object document definitions are initially immutable. Unsupported native
operations must fail without allocating a local shard or claiming a local commit.
Further capability parity and explicit engine migration remain separate work.

## Authority and ownership

The native catalog owns existence, logical naming, schema, indexes, and table
incarnation. In distributed mode it retains metadata consensus. Removing data
Raft does not remove metadata authority or coordinated publication.

External lake sidecars retain one metadata authority: a builder first commits a
pending attempt through definition CAS, renews its fence, uploads immutable
artifacts, and publishes through the exact admitted CAS. Upload completion is
never publication. Query workers pin source coverage and a published generation.
Reader and collection leases protect retained generations and artifacts.

Owned object document tables have a private object catalog projection per native
incarnation. Public requests cannot invoke object-catalog DDL. That projection
holds the immutable schema/index definition required by the existing object
builder; it is not a competing table-management authority. The native API
resolves an authoritative table binding before executing its data operation.

The object runtime owns WAL append order and visible HEAD. Ownership takeover and
HEAD publication use the existing conditionally written coordination record and
fencing token. A lease checked separately from publication is insufficient: an
old worker must not publish after another worker takes over. Multiple native API
workers may host the same object table and contend through that protocol.

Local worker affinity is a scheduling/cache optimization. Durable objects and
fences determine correctness. Object tables have no data shard voters or leaders.

## Storage binding and incarnation isolation

Use the capability-scoped `storage.artifacts` destination for native object
tables. Distributed API nodes must share this destination and have authorized
`storage.primary` access. Source lake credentials do not grant sidecar or WAL
write authority. Standalone may use the existing filesystem artifact
fallback for development.

Before owned-object data execution, the API pins a digest of the physical store
locator in native metadata through CAS and verifies the committed definition.
The digest includes protocol, connection, endpoint/root, bucket, prefix and TLS;
it excludes credential material, so rotating credentials does not relocate data.
A mismatched node fails closed. Relocation requires an explicit migration.

The durable table record carries an object generation. Distributed creation uses
the native table transition fence; standalone creation uses its persisted catalog
epoch because tables with no ranges do not advance range transition fences. The
private root includes table identity and that generation.
A late request or collector for a dropped incarnation cannot touch a recreated
table's root. The old generation remains isolated and retained; automatic
whole-table deletion of owned-object roots is not introduced by this change.

Native processes lazily host bounded per-incarnation runtimes. Each runtime owns
its authorized client, query state, and maintenance worker, and stops before its
client is destroyed at API shutdown. There is an initial cap of 128 hosted
incarnations per API process. Dropped runtimes remain retained until restart;
a later retirement owner can drain and reclaim them without racing requests.

## Compatibility and admission

Legacy table records retain their existing JSON interpretation, binary encoding,
and definition fingerprints. Native storage metadata continues using extension
version 1. Object records use storage extension version 2, including engine,
incarnation generation and physical-store identity. Metadata decoder capability
30 gates object records at admission and at final command append.

Empty data topology is valid only for object records. Native create requests with
zero ranges remain invalid. Reconciliation must preserve the engine and storage
binding when comparing records. An existing native table never becomes an object
table merely because a deployment default changes.

Tablespaces currently describe placement policy. They can later provide named
object destinations and inherited creation defaults, but this implementation
does not reinterpret their location metadata as a storage override. Resolved
bindings must remain table-owned when defaults change.

## Initial capability boundaries

External read-only Parquet/Iceberg tables keep their existing SQL and sidecar
capabilities. Owned object tables expose document batch, published key lookup,
and search paths. Their schema/index definitions are fixed at creation; this prevents
stale workers from projecting a newer native definition backward into the private
object catalog. Native engine changes and pinned-store changes are rejected.

Cross-engine atomic transactions, owned relational object writes, native
split/merge, native backup/restore of owned object tables, and native credential
row filters are not made equivalent to local-shard operations by this setting.
They need explicit protocols before admission. Existing serverless APIs retain
those APIs' existing semantics; the preset does not imply native capability parity.

## Validation

Cover engine parsing and admission, legacy framing, object framing and command
round trips, decoder gates, zero-range topology, definition/binding immutability,
and clone/reopen preservation. Exercise native API storage-binding admission and
policy-authority failure, and an owned-object batch with synchronous index
publication, lookup, search, runtime reopen, and recreation into an empty generation.
Existing lake API and catalog suites cover source reads and sidecar publication.

Direct embedded storage APIs require a hosted object runtime and reject the object
engine rather than opening a local DB with different durability semantics.
