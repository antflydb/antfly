# Coordinated Iceberg maintenance

This package supplies the provider-side gateway/controller consumed by Antfly's
`nessie` and `polaris` maintenance bindings. It uses real Iceberg REST commits,
Nessie reference/history traversal and Polaris namespace/table enumeration.
Durable authority, plans, retirement records and receipts live in conditional
object storage. The filesystem adapter is for local tests; there is no SQLite
catalog or coordination database.

## Deployment contract

* Vendor endpoints are private. Only the gateway identity can mutate the bound
  catalog. Remove vendor credentials from every external writer and prohibit
  direct vendor access, including administrative tools that change references.
* External readers use `GatewayCatalog` below or implement the same lease
  contract. Unleased direct object readers and native vendor clients cannot safely
  coexist with destructive vacuum. Antfly's own readers use its durable snapshot
  pin registry instead.
* Give data clients read/upload permission; grant physical deletion only to the
  controller. Give the gateway a separate private authority prefix. Never grant
  clients write/delete access to authority or retirement records.
* S3 buckets must have versioning enabled. Deny data clients and the gateway
  `s3:PutBucketVersioning`; provision versioning through a separate administrator.
  Deletion inventories all orphan versions and delete markers, journals each version
  independently, and addresses exact VersionIds, including legacy null versions.
  GCS uses exact generations, including retained old versions. Files published in
  Iceberg must have immutable URIs; data clients must not overwrite published files. Do not expire authority records or active native pin objects
  through lifecycle policies. Lifecycle deletion is a separate maintenance actor
  and must follow the same retention rules.
* Keep controller and native node UTC clocks within the reader registry's
  30-second grace.
* Terminate TLS and authenticate at the gateway. Use three distinct secret tokens
  for writer, Antfly native and maintenance admin roles. One instance binds one
  catalog/warehouse; deploy separate authorities for Nessie and Polaris.

`qualification/compose.yaml` demonstrates private backend networking: vendor and
object administration ports are unpublished; only the gateway joins the client
network. Its in-memory vendor defaults are for qualification. Production Nessie
and Polaris need their supported durable databases, backup and HA configuration.
For cloud deployment, use equivalent VPC/firewall/IAM controls and cloud identity
rather than the fixture's static object keys. `gateway_enforced: true` asserts
those controls; the controller cannot inspect arbitrary cloud IAM from this flag.

## Run

Install with `uv sync --project py/packages/lake-maintenance`. Copy the appropriate
`qualification/{nessie,polaris}.json`, replace locations, public gateway URL and
upstream endpoints, and supply each `{"env":"..."}` secret through the environment.
Polaris uses configured OAuth client credentials and refreshes its vendor bearer
token before expiry. Secret rotation preserves the bound object-store authority.
The gateway never vends vendor credentials to clients.

```sh
antfly-lake-maintenance --config controller.json --host 0.0.0.0 --port 8089
```

In the Antfly REST catalog binding, point `uri` at the public gateway's `/catalog`
and set `maintenance.uri` to the gateway origin. Use the native-role token for the
catalog connection and the admin-role token for the separate `lake_maintenance`
connection. Bind `artifact_uri` and `artifact_connection` to the node's real
artifact storage; mismatched reader registries are rejected. Both connections
remain restricted to their configured origin.

```python
from antfly_lake_maintenance.client import GatewayCatalog

with GatewayCatalog("archive", uri="https://lake.example.com/catalog",
                    token=writer_token, **object_read_upload_properties) as catalog:
    table = catalog.load_table("hackernews.items")
    rows = table.scan().to_arrow()
    table.append(new_rows)
```

The SDK renews leases in the background and checks the local lease deadline before
and after file reads. Keep the catalog context open while using tables/streams.
Closing it invalidates those readers. A failed renewal cannot extend the local
read deadline. Commits attach a lease for the resulting immutable metadata before
writer admission reopens. Long uploads can additionally lease their input URIs
through `POST /v1/antfly/readers` with `input_files`.

## Operations and recovery

Antfly submits its existing maintenance protocol through the gateway. Jobs persist
an immutable plan before mutation, exclude new writers/readers, protect external
leases and native pins, publish irreversible retirement records, and delete a
bounded batch per call. Repeat the exact original job to resume or retrieve its
receipt. Existing leases may renew during a job. No owner timeout releases a
prepared plan or a provider write whose outcome is unknown.

`GET /v1/antfly/maintenance/status` reports admission ownership. After a lost
catalog response, call `POST /v1/antfly/maintenance/recover-writer`: a committed
property/commit-history marker resolves the write. An absent marker does not
prove failure. Native reference changes without a durable vendor marker remain
fenced after uncertain outcomes; drain/fence the original provider request and
reconcile its outcome before administrative recovery. Do not edit HEAD to bypass
this protection. Definite rejected requests release admission automatically.

Polaris expiration retains named refs, recent/latest snapshots, active readers,
native publications/pins and every other table's reachable roots. Nessie preserves
all reachable branch/tag **and commit-history** roots, so orphan deletion works
while retained vendor history still protects historical files. This controller
currently does not shorten Nessie commit history. External vendor GC must be
turned off or integrated with the same authority.

If concurrent planning fails and revokes admission, its late immutable plan cannot
be resumed under a new admission epoch. Submit a new operation after that explicit
conflict; prepared jobs retain their original epoch through recovery.

Traversal defaults to 100,000 objects/roots and 256 MiB of metadata; a plan selects
at most 4,096 object-version deletions and deletes 64 per turn (configurable). Exceeding a bound
fails closed. Planning holds catalog admission and is not a demonstrated
full-archive incremental marker. Unsupported mutations (merge/transplant,
registration, table purge, views and multi-table transactions) fail closed. A
candidate whose filesystem/GCS generation changed is preserved and the job stays
fenced for reconciliation. Current real-provider qualification covers S3 versioned
storage; GCS code requires separate real-cloud qualification.

## Reproduce qualification

Run the following from the package directory with Docker available. The privileged
port override is exclusively for a local fixture driver; it is deliberately
excluded from the private deployment contract.

```sh
export ANTFLY_CONTROLLER_CONFIG="$PWD/qualification/nessie.json"
export ANTFLY_CONTROLLER_ENV=/absolute/path/to/fixture.env
export ANTFLY_PRIVATE_TARGETS='[]' # The fixture driver does not run the network probe.
# Create fixture.env with the five gateway/S3 environment variables from the JSON.
docker compose -f qualification/compose.yaml -f qualification/admin-ports.yaml up -d s3 nessie polaris
uv run python qualification/bootstrap.py
ANTFLY_REAL_CATALOGS=1 uv run pytest -q tests
# Add ANTFLY_NATIVE_BINARY=/absolute/path/to/antfly for native HTTP restart tests.
docker compose -f qualification/compose.yaml -f qualification/admin-ports.yaml down
```

For the private stack, bootstrap via an administrator on its backend network,
then start the vendors and gateway using the base compose file and run:

```sh
docker compose -f qualification/compose.yaml up -d --build s3 nessie polaris gateway
export ANTFLY_PRIVATE_TARGETS="$(docker inspect antfly-maintenance-nessie antfly-maintenance-polaris antfly-maintenance-s3 | uv run python qualification/private_targets.py)"
docker compose -f qualification/compose.yaml --profile qualification run --rm network-probe
```

The probe requires gateway reachability and denies catalog/object administration
reachability from the client network. Do not run the privileged override while
qualifying that isolation: it explicitly disables backend isolation for the
privileged local driver. After removing that override, recreate the networks. The
probe checks both private IP addresses and vendor DNS names. Provider tests use official Nessie 0.109.0, Polaris
1.7.0 and RustFS 1.0.0-alpha.81. Retention tests inject a controller clock 21 minutes
ahead; real provider/storage operations are performed without waiting ten minutes.
