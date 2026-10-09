# External Iceberg maintenance

Antfly owns retirement authority for its managed object-store catalogs. Its HEAD
CAS publishes an irreversible file-retirement index before deletion. That authority
cannot be extended to an independent catalog by writing local delete markers.

For external catalogs the selected design is provider-owned maintenance, integrated
through a separately authorized controller. Antfly delegates physical deletion;
its source-file credentials never execute the external deletion path. A controller
must integrate the actual catalog's writers and external readers as well as
Antfly's durable reader registry. Generic Iceberg REST supports load and commit;
it does not implement this controller protocol.

## Implementation status

Antfly implements the client integration for both `nessie` and `polaris`: node
connection authorization, origin restriction, capability negotiation, immutable
object-store job journals, exact replay after restart, bounded/cancellable HTTP,
and receipt binding to provider, operation, request and table incarnation.
Focused tests exercise both adapters through a protocol fixture. This does **not**
qualify a real Nessie or Polaris maintenance deployment. The provider-side
controllers described below still need implementation, deployment and qualification.
Destructive vacuum remains disabled without a suitable controller.

## Configuration

Add the optional `maintenance` member to a REST catalog binding:

```json
{
  "type": "rest",
  "connection": "iceberg-catalog",
  "uri": "https://catalog.example/iceberg",
  "namespace": ["hackernews"],
  "name": "items",
  "maintenance": {
    "provider": "nessie",
    "connection": "nessie-maintenance",
    "uri": "https://maintenance.example"
  }
}
```

Use `provider: "polaris"` for Polaris. The maintenance connection is a node-owned
`external_io` HTTP connection with the `lake_maintenance` capability, an allowed
origin and secret-referenced headers. Catalog read/write permission alone never
grants maintenance authority. The table must have `iceberg_writer` policy, and the
calling identity must have table admin permission without a row filter.

The controller has separately provisioned credentials for the catalog, its files,
and the shared Antfly artifact connection. Passing a node connection name in a job
identifies the reader registry; it does not transfer credentials. HN can continue
using its Antfly-owned catalog without deploying either external provider.

## Controller protocol version 1

These paths belong to the **Antfly maintenance integration protocol**. They are
not built-in Nessie, Polaris or Iceberg REST endpoints.

`GET /v1/antfly/maintenance/capabilities` must return:

```json
{
  "protocol": 1,
  "provider": "nessie",
  "catalog_uri": "https://catalog.example/iceberg",
  "writer_fencing": true,
  "external_reader_protection": true,
  "native_reader_registry": true,
  "immutable_retirement": true,
  "idempotent_jobs": true,
  "nessie_references": true
}
```

Polaris uses `provider: "polaris"` and `polaris_table_roots: true`. Every generic
protection flag is required. Declaring these capabilities is an authority contract
with the separately trusted controller, rather than a guarantee Antfly can infer
from the catalog's HTTP version. Ordinary vendor garbage collection tools must not
advertise this contract without the additional coordination.

Antfly persists the original job body before
`POST /v1/antfly/maintenance/jobs/{request_sha256}`. It sends that hash as the
`Idempotency-Key`. The body binds the provider/catalog, native source URI, Iceberg
table UUID, original expected metadata location, protected publication snapshots,
reader registry, stable operation ID and retention/deletion limits. The registry
contains its node connection identity, bucket, prefix, 30-second grace period and
`antfly-snapshot-pins-v1` protocol. No source credentials are sent.

The provider must persist the job before acknowledgement, reject stale authority
rather than silently rebase, and return the same outcome for exact replays. A job
ID cannot be reused with different policy. Replays use the journaled metadata and
roots even after a successful job changed catalog metadata. The controller must
continue consulting live reader admission and writer authority throughout work;
the initial protected snapshot list is an additional root, not a replacement for
that coordination.

Receipts bind `protocol`, `provider`, `operation_id`, `request_hash` and
`table_uuid`. State is `queued`, `running`, `complete` or `rejected`. Counts are
`expired_snapshots`, `eligible_objects`, `deleted_objects` and `retained_objects`.
Pending receipts report no physical deletions; completed receipts report final
counts. A dry run must report zero deletions. Rejection surfaces as a conflict;
transport failure leaves the immutable journal available for exact retry.

The existing `vacuum` maintenance action selects this path whenever the REST
binding has a maintenance integration. Responses include `delegated: true`,
`provider`, `provider_state` and `complete`. A successful HTTP response can mean a
queued job; callers must inspect state. Without an integration, Antfly can perform
its local dry-run estimate, but destructive REST vacuum is forbidden even with
`exclusive_ownership: true`.

## Required coordination

Antfly snapshot admission checks retirement, conditionally raises a durable pin
deadline, then rechecks retirement. A controller publishes retirement before its
final deadline check, includes the grace period, and retains files shared with
any protected snapshot. It must use the same authoritative artifact registry,
without relying on an eventually consistent cached listing. Live publication
roots and in-flight inputs must remain protected. Lost outcomes retain data.

The controller also needs the provider's complete root set and an effective
writer fence that prevents a late commit from resurrecting a deleted object.
External readers must hold provider-recognized leases or use a rigorously enforced
retention contract. A configuration assertion that nobody is writing, a final
metadata reread, or a service-wide mutex that writers do not participate in is
insufficient. Unsupported guarantees must reject deletion.

## Nessie controller

Nessie maintenance must retain all relevant catalog branches, tags and historical
content references. A single Iceberg REST branch's current metadata is incomplete.
Use Nessie's maintained GC tooling for provider root discovery, with durable
live-content sets and separate mark, deferred sweep and deletion phases. Antfly
reader roots must be included before sweep and revalidated before deletion.
Catalog-wide coordination must cover all writers, and persisted job state must
retain the live-set identity across restart. The standard GC CLI alone does not
supply Antfly's reader admission or irreversible writer fence.

See [Nessie management](https://projectnessie.org/guides/management/) and
[GC sweep options](https://projectnessie.org/nessie-latest/generated-docs/gc-help-sweep/).

## Polaris controller

Polaris maintenance must use the actual catalog's complete Iceberg snapshot and
named-ref roots and its authorized file access. Table snapshot expiration and
orphan-file removal are distinct from Polaris persistence-store maintenance.
An Iceberg/Spark maintenance job can execute the table operations, but the
controller still must establish writer coordination, integrate external and
Antfly readers, enforce retention and own the durable job/receipt lifecycle.
Do not treat a Polaris metadata-store compaction CronJob as table-file GC.

See [Polaris documentation](https://polaris.apache.org/releases/1.7.0/) and
[Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/).

## Qualification still required

Each provider controller needs real tests for racing external commits, active
external readers, Antfly admission races, branch/tag preservation, a lost job
acknowledgement, restart between sweep and deletion, shared data/delete files,
and receipt replay after table replacement. No provider may enable physical
deletion until these guarantees hold. Provider controllers are not automatically
provisioned by table creation.
