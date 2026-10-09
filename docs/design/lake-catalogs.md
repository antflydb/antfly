# Native Iceberg catalog authorities

Antfly supports two explicit catalog authorities for an external Iceberg table:
`managed`, with conditional publication in the table's object store, and `rest`,
with commits through an existing Iceberg REST service. Both use the same native
lake reader, index builder and table authorization. The ingestion and publication
roadmap is in [lake-ingestion-and-publication](../plans/lake-ingestion-and-publication.md).

## Choosing an authority

| Decision | Managed object-store catalog | External Iceberg REST catalog |
|---|---|---|
| Commit authority | Antfly CAS of one remote HEAD object | REST service validates and commits Iceberg requirements |
| Setup | Source connection with read/write access | REST HTTP connection, source read connection and durable artifact storage |
| Recommended use | HN and tables whose writers all commit through Antfly | Existing lakehouses shared with other Iceberg writers |
| Interoperability | Standard Iceberg v2 metadata, manifests and data files; private Antfly catalog protocol | Standard Iceberg REST load/create/commit protocol |
| Recovery | Immutable intents, commit chain and receipts in object storage | Immutable intent/receipt journal plus metadata commit markers and negotiated idempotency |
| Operational cost | No separate catalog service; Antfly owns retention and conflict handling | Operate or buy a catalog service and honor its capability/credential policies |

Do not point independent writers at `version-hint.text` or overwrite metadata
behind a managed catalog. Managed publication is not an Iceberg REST server.
External writers must commit through the same authority. Changing authorities
requires a controlled binding migration; it is not a fallback on network errors.

## Table configuration

The source URI is a table root. Explicit metadata-file bindings keep their
existing read behavior and cannot be used with a configured catalog.

```json
{
  "base_source": {
    "kind": "external",
    "format": "iceberg",
    "uri": "gs://archive/hackernews",
    "table_id": "hackernews",
    "credentials": {"ref": "hn_source", "scope": "hackernews"},
    "write_policy": "iceberg_writer",
    "catalog": {"type": "managed"}
  }
}
```

For REST, replace the catalog configuration:

```json
{
  "type": "rest",
  "connection": "hn_catalog",
  "uri": "https://catalog.example.com",
  "warehouse": "hackernews",
  "namespace": ["analytics", "public"],
  "name": "items"
}
```

`hn_source` is a named storage connection. Managed writes require both
`lake_read` and `lake_write`; a REST reader needs `lake_read`. `hn_catalog` is a
named `external_io` HTTP connection with `lake_catalog_read` and, for mutations,
`lake_catalog_write`. Its host policy must allow the configured catalog origin.
Credentials come from connection headers and secret references, never the table
schema. Requests do not follow redirects or replay automatically.

REST mutations also require configured native artifact storage with
`storage.primary`; the remote journal is isolated by native table identity,
object generation and catalog binding. S3 and GCS use the existing authenticated
object-store implementation. Local Antfly filesystem object storage is useful for catalog transaction
qualification; it uses Antfly's bucket layout, rather than a raw PyIceberg filesystem
warehouse. Full file-producer/read qualification uses the S3 protocol. Local caches are disposable; they are not commit authority.

`read_only` remains the default. `iceberg_writer` enables explicit catalog file
commits; it does not enable ordinary row batch mutations. Declare the document
schema when creating an empty table. Initialization finalizes an `auto` schema
fingerprint after metadata exists.

## API and commit lifecycle

All routes are under the existing Antfly API prefix:

- `GET /tables/{tableName}/lake/catalog`: load authoritative metadata.
- `POST /tables/{tableName}/lake/catalog`: initialize metadata with a stable
  `commit_id`, Iceberg `schema`, and optional partition spec, sort order and properties.
- `POST /tables/{tableName}/lake/commits`: commit prepared Iceberg
  `requirements` and `updates`, a stable `commit_id`, and the exact
  `expected_metadata_location` used to prepare the update.
- `GET /tables/{tableName}/lake/commits/{commitId}?request_hash=...`: resolve a
  prior outcome without submitting another update.

Reads require table read permission; mutations require table admin permission
and an explicit writer policy. Row-filtered identities cannot make table-wide
file commits. The authoritative native table identity is checked before dispatch.

Persist the exact request and its commit ID before submitting. Reuse that ID
only for identical content; timestamps assigned by the receiving node do not
change the request hash. HTTP 409 requires a fresh read and a new commit ID for a
new plan. HTTP 202 is an unresolved outcome, not permission to rebase the old
request. Retry the exact request or resolve its returned hash first.

Successful catalog mutations return `state: lake_committed` and
`searchable: false`. Index publication is a separate step. Initialization may
return `binding_ready: false` if metadata committed but native schema binding
could not be finalized; replay initialization to finish that binding.

## Durability and concurrency

Managed commits validate Iceberg v2 requirements and metadata updates before
writing immutable, content-addressed metadata. Prepared intents bind a commit ID
to one request. A conditional HEAD write is the commit point. Durable receipts
and the immutable predecessor chain distinguish successful lost responses from
stale writers after restart. Uncertain outcomes fail closed. Garbage collection
must retain proof for outstanding intents; this implementation does not delete
catalog history automatically.

REST commits first persist an immutable intent. They validate the caller's
requirements against the loaded metadata and add guards for table identity,
schema, partition/sort IDs and the main branch. The service remains the final
commit authority. Commit properties and snapshot summaries carry identity
markers; durable receipts preserve acknowledgement after later metadata changes.
Replay is allowed only within the service's explicitly advertised
`idempotency-key-lifetime`. Without that capability, an ambiguous outcome remains
unknown unless a marker or receipt proves success. Absence of a marker never
proves failure after another writer has advanced or expired metadata.

## Boundaries and next layers

These backends implement catalog load, initialization, explicit file commit and
outcome recovery. File producers still write Parquet and Iceberg manifests before
committing them. Native SQL schemas and schema fingerprints remain explicit
bindings; evolving a lake schema requires a coordinated native binding change. The native row-to-Parquet background writer, general CDC adapters,
compaction/garbage collection and atomic searchable publication are separate
layers of the ingestion plan. Iceberg v3 encryption and view updates are not
accepted by the managed v2 writer.

The underlying protocol is the [Iceberg REST catalog specification](https://iceberg.apache.org/docs/latest/rest-protocol/),
with metadata governed by the [Iceberg table specification](https://iceberg.apache.org/spec/).
