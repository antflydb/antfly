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

Antfly implements the client integration and the provider-side gateway/controller
for both `nessie` and `polaris`. The implementation lives in
[`py/packages/lake-maintenance`](../../py/packages/lake-maintenance/README.md).
Authority, write intents, immutable job plans, retirement records and receipts use
conditional object storage. There is no separate SQLite state.

Destructive external vacuum is available under the explicitly selected **enforced
gateway contract**: vendor endpoints are private, only the gateway holds catalog
write credentials, and external readers acquire leases through the gateway.
Antfly readers retain their existing durable snapshot pins. The gateway validates
proposed metadata against irreversible file retirement before forwarding commits.
A configuration flag alone is insufficient: deployment networking and IAM must
actually enforce these rules. Direct vendor writers or unleased object readers
invalidate the contract and must not be enabled with destructive vacuum.

The controller performs real Polaris snapshot-expiration commits and protects all
catalog table roots. Nessie protects all reachable branch/tag and commit-history
roots, then deletes unreferenced objects; shortening Nessie history remains a
separate provider retention operation. Deletion targets exact S3 VersionIds or GCS
generations. Orphan version inventory and per-version progress ensure deletion
does not leave an older version visible or skip it after restart. S3 versioning must remain enabled and its administrative permission
must be denied to data clients. Unknown provider-write outcomes remain fenced;
a positive durable commit marker permits recovery.

Local qualification exercises official Nessie 0.109.0 and Polaris 1.7.0 against
versioned RustFS storage, including retained reads, writer exclusion during
vacuum, native HTTP restart/replay, lost commit responses, replacement-version survival,
native pins racing retirement, and DNS/IP isolation on an internal backend network.
This is local real-provider qualification, not a production deployment or
full-archive performance qualification. The package documents bounded traversal,
unsupported mutations, uncertain reference-change recovery, private deployment
configuration and the lease-aware Python catalog client. GCS requires separate
real-cloud qualification.

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

The controller enumerates native Nessie v2 references and paginated commit history
at immutable reference hashes. It retains every reachable Iceberg metadata root,
including branches, tags and historical PUT content. This conservative policy does
not shorten Nessie commit history. Native reference changes validate the complete
source tree against retired URIs before reaching the vendor. Unknown content types
fail closed. Real qualification covers branch creation/root retention and v2
pagination, including the provider's actual detached-reference and content syntax.

Nessie's GC tooling can provide a future bounded historical live set, but its
standard CLI alone does not integrate Antfly's leases or irreversible writer
fence. Introducing a shorter history cutoff requires equivalent protection for
external readers and native historical pins.

See [Nessie REST API](https://projectnessie.org/develop/rest/) and
[Nessie management](https://projectnessie.org/guides/management/).

## Polaris controller

The controller enumerates namespaces and tables with pagination, retains complete
current snapshot/named-ref roots and protected historical reader roots, and expires
eligible snapshots through actual Iceberg REST requirements and updates. It marks
metadata, manifest lists, manifests, live data/delete entries and statistics files
before selecting old orphan object versions. Polaris persistence-store compaction
is unrelated to this table-file vacuum.

Polaris OAuth client credentials are exchanged and refreshed inside the gateway.
Vendor tokens, data signers and upstream idempotency promises are not advertised
through the public gateway. External clients use their own scoped data identity and
the lease-aware catalog client; native Antfly uses its durable pin protocol.

See [Polaris documentation](https://polaris.apache.org/releases/1.7.0/) and
[Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/).

## Qualification and deployment scope

Local real-provider qualification passes 33 tests with four provider-specific
skips, against Nessie 0.109.0, Polaris 1.7.0 and versioned RustFS. It covers actual
native Antfly HTTP delegation and daemon restart, multi-turn owner replacement,
exact receipts after metadata advancement, writer exclusion, external reader
retention, lost commit responses, native admission/retirement races, revoked
planner epochs, retired-file resurrection rejection, branch roots, pagination,
and independently journaled orphan versions. A separate Docker qualification
confirms both gateway endpoints are reachable while vendor/object administration
is inaccessible from the client network by DNS **and IP**.

The fixture advances the controller clock for retention tests; native pin race
fixtures use that same clock. This does not qualify production clock skew, cloud
IAM or real GCS deletion. Shared-file/delete-file combinations, larger catalogs,
production deployment and full-archive performance need additional qualification.
Unsupported mutations and exceeded inventory/metadata bounds fail closed. Retain
all Nessie commit-history roots until an equally protected provider history
retention integration is configured. Table creation never automatically provisions
the external catalog, gateway, private network or provider identities.
