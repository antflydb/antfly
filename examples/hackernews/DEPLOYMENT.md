# Hacker News: Antfly Cloud deployment and standalone qualification

The intended real deployment is an organization-owned Antfly Cloud instance.
The standalone renderer below is an optional qualification tool, not the Cloud
provisioning path. This candidate is not deployed or archive-qualified. Complete the regional
public-API gates in `docs/plans/hackernews-deployment-qualification.md` before
selecting serving resources or exposing `hn.antfly.io`.

For the real deployment, merge the required runtime fixes and publish an immutable
Antfly release containing them. Add that image to Colony's development runtime
catalog as a manually selected candidate, provision an instance through Antfly
Cloud, and configure historical/current tables and the saved source through its
authenticated managed API. Verify tenant-scoped object-store credentials,
publication, replacement recovery, cursors and search performance on that instance.
A standalone binary benchmark does not qualify the Cloud control plane or proxy.
A new Colony application release is needed only when its control-plane code changes.

The hosting/storage foundation is Colony's merged [#823](https://github.com/antflydb/colony/pull/823),
under `infra/showcases`. HN receives separate archive, warehouse and producer
checkpoint buckets. The managed instance owns its engine volume and uses its
verified tenant Workload Identity for GCS. The producer owns a separately sized
retained Lite state volume. The earlier standalone 20 GiB proposal is optional
qualification infrastructure and is not attached to the real Cloud instance.

## Managed Cloud setup

The matching Colony implementation and runbook are in
`docs/cloud/hackernews-rc-qualification.md`. That change requires a new Colony
application release: it adds a pinned first-boot native runtime, immutable remote
artifact placement, instance-bound default-identity GCS connections and trusted
principal forwarding for native composed queries. A runtime catalog edit alone
cannot install it. RC.6 is published and pinned in `runtime.json`;
its build success or a local library test does not qualify a managed instance.

The plan defaults to the exact published RC.6 OCI index in `runtime.json`. An
optional `--image` accepts a registry mirror only when its digest matches that pin.
Generate reviewable requests in a new directory, without contacting Cloud:

```sh
uv run --project examples/hackernews python examples/hackernews/cloud_plan.py \
  --output /private/tmp/hn-management management
```

The plan uses Standard because Starter forbids custom capacity. Standard has a
monthly package floor even for a small instance; review the live credit estimate
before creation. The plan contains a single-mode pinned instance (1 CPU/4 GiB/20 GiB initial sample,
not archive sizing), GCS provider selectors and scoped server-owned keys. Create
the instance through Cloud's management API after reviewing credits/capacity.
Artifact placement uses the same nested name as Antfly's engine configuration:

```json
{
  "storage": {
    "artifacts": {
      "protocol": "gcs",
      "bucket": "antfly-showcases-hn-warehouse",
      "prefix": "native-indexes"
    }
  }
}
```

Cloud binds credentials and appends the instance UUID internally. This destination
is immutable after creation; relocating it requires an explicit migration.
Review the expected tenant namespace and grant its exact bucket IAM before first
boot; artifact initialization can need GCS immediately. Verify the actual runtime
digest and namespace after creation, before using it.
Create the instance-bound archive/current providers, then render native definitions
with their returned IDs:

```sh
uv run --project examples/hackernews python examples/hackernews/cloud_plan.py \
  --output /private/tmp/hn-native native \
  --archive-auth ARCHIVE_AUTH_UUID --current-auth CURRENT_AUTH_UUID \
  --archive-prefix archive/qualification-20261029
```

Use a dedicated sample prefix for private qualification. Keep a later full export
in a separate frozen prefix; do not mix duplicate sample/full-export rows beneath
the active Parquet source. Moving that source requires an explicit reviewed
publication/migration, not silently adding files.

Submit those definitions to `/tables/hn_archive`, `/tables/hn_current` and
`/sources/hackernews` at the authenticated native tenant API. Both tables explicitly
use `storage.engine: object` and omit `num_shards`. Archive is read-only Parquet;
current is managed writable Iceberg. Index/source placement uses the instance's
recorded artifact connection, separate from source credentials. Do not use the
convenience Cloud default-table API or substitute a standalone database.

The generated relational index names include `hn_archive_` or `hn_current_`:
these names share the SQL namespace across tables. Copying an archive schema
without renaming its relational indexes causes a catalog conflict when creating
the current table, even though the current table does not exist yet. For an
existing archive, keep its definition intact and generate unique current index
names before creating the missing table.

Supply `ANTFLY_API_BASE` including the complete tenant `/cloud/v1/UUID/db/v1` path.
Use either `ANTFLY_AUTHORIZATION` (the complete header value) or `ANTFLY_API_KEY`
(a bare key); never both. Authenticated destinations require HTTPS. The gateway
and native producer retain the tenant prefix and reject redirects, so credentials
and exact write retries cannot silently move to another host. Browser requests
cannot select backend URLs, table names or pass their own upstream credential.

Prepare the producer's matching RC.6 library from the repository root. This checks
published source/version metadata, archive size and SHA-256 against `runtime.json`;
it refuses to overwrite an existing extracted library. The receipt is included in
the producer image. Select the same architecture for preparation and Docker build.

```sh
uv run --project examples/hackernews python examples/hackernews/prepare_runtime.py \
  --architecture amd64
```

Build the two roles separately from the repository root:

```sh
docker build --platform linux/amd64 -f examples/hackernews/Dockerfile.gateway -t HN_GATEWAY .
# Checksum-verify and extract the matching Linux GNU embedded release into
# qualification-linux/amd64/ (or arm64/ for an explicitly selected ARM worker).
docker build --platform linux/amd64 -f examples/hackernews/Dockerfile.producer -t HN_PRODUCER .
```

The gateway has no engine, state disk or Google SDK. The producer includes the
matching native Lite library and hash-pinned Python dependencies. Cloud Run needs
an amd64 gateway; select the producer platform to match its actual GKE scheduling
architecture. BuildKit selects `qualification-linux/$TARGETARCH/lib` explicitly. Pin both pushed
images by digest. The showcase stack references existing Secret Manager secrets
by numeric version and grants each role only its selected secret. Hosted gateway
startup requires authentication. The gateway's process limit is two searches per
second, burst eight, eight in flight; with two Cloud Run instances the aggregate
limit can double. Health readiness is cached for ten seconds. Limits reduce load;
they do not enforce a hard spend ceiling.

### Initial recent seed and supervised updates

Freeze a bounded inventory of the durable archive before seeding:

```sh
uv run --project examples/hackernews python examples/hackernews/seed_gcs.py \
  --project antfly-showcases --max-bytes ADMITTED_BYTES inventory \
  --bucket antfly-showcases-hn-archive --prefix archive/ > seed.json
```

Publish `seed.json` at a new archive key, record its returned generation, and
supply both URI and numeric generation to the seed Job. The manifest binds object
generations, sizes and CRC32C. Download retries fence each generation, verify
checksums and reuse complete staged files. Byte limits and an explicit PVC size
bound staging; choose capacity from actual input and Lite state measurements.
The job records its manifest binding in Lite and refuses changed inputs.

Prepare the producer's identity, secret reference and retained claim first; it
starts no process. Then run `seed_gcs.py seed` through the one-shot Job. Initial
seeding retains all ancestry/raw item state in Lite while publishing only the
fixed recent subset to `hn_current`. Wait for seed completion and a checkpoint
before enabling the continuous `Recreate` deployment with `--require-seeded`.
Its normal native-row path forwards later older edits/deletes too; there is no
permanent age filter on ownership. This is essential for suppressing stale archive
matches in the combined overlay. Native requests and accepted watermarks persist
in Lite; ambiguous outcomes replay the exact request after restart.

The worker polls/publishes every minute and defaults to one full remote checkpoint
per day. `/state/heartbeat.json` records the latest completed poll, not proof of
searchable freshness. Monitor publication lag and checkpoint age separately. The
single retained volume is not HA; a lost volume may require restoring the latest
checkpoint and replaying subsequent changes. Stop the prior writer before restore,
use the existing backup/restore procedures, and keep unresolved native requests
and their checkpoints together.

### Private acceptance before public launch

Use an authenticated Cloud Run service proxy for local access, then collect
bounded evidence without deploying or restarting anything:

```sh
python examples/hackernews/qualify_gateway.py --gateway http://127.0.0.1:8080 \
  --runtime-image EXACT_OBSERVED_IMAGE@sha256:DIGEST --output /private/tmp/hn-before.json
# After the approved managed restart, within cursor retention:
python examples/hackernews/qualify_gateway.py --gateway http://127.0.0.1:8080 \
  --runtime-image EXACT_OBSERVED_IMAGE@sha256:DIGEST --resume-from /private/tmp/hn-before.json \
  --output /private/tmp/hn-after.json
```

For direct private HTTPS access, `HN_GATEWAY_AUTHORIZATION` is the Cloud Run
identity header, distinct from the backend Antfly key. Reports preserve failures,
opaque cursors and per-view timings with private file permissions. They verify
health, nonempty distinct results and continuation without repeated rows. They do
not establish full-archive correctness or performance. Separately qualify expected
fixture IDs, edits/deletes, backup restore, replacement-pod recovery, credential
refresh beyond a token lifetime, sustained load and the matched DuckDB comparison.

Only after private acceptance enable the HN public invoker, reviewed monitoring
channels and optional verified domain mapping. Publish the exact DNS records
returned by the provider through the existing DNS owner, then verify TLS and every
view at `hn.antfly.io`. The prior $150 ceiling covers incremental qualification;
review remaining spend before archive jobs and agree on an ongoing serving budget.

Local validation on October 27: 56 relevant HN tests passed against the checksum-verified
RC.5 draft native server and embedded library, including real filtered/date-sorted
gateway and saved-overlay pagination. Two JavaScript URL/date-state tests passed;
fixture browser checks covered desktop/mobile layout, filters, pagination, shared
URL reloads, copy-link sanitization and Back/Forward restoration. Both amd64
images built, gateway health/auth isolation passed, and the producer performed a
native Lite write/read inside its container. Images have not been pushed and no
Cloud instance, infrastructure apply or archive qualification was performed.

## Search controls and shared links

The UI supports Everything / Stories / Comments, relevance or newest-first date
ordering, preset date windows and a custom inclusive UTC date range. Date sorting
runs in Antfly before pagination, including across the saved overlay. Item type
is an exact keyword predicate; generated schemas map `item_type` and `author` as
keywords and keep numeric date/ID sort fields. Existing object tables with older
immutable schemas need an explicit migration and index publication before these
filters can be used; the gateway does not silently recreate them.

The URL records the search term, type, sort, date selection and data view. Opening
a shared link or using Back/Forward restores those controls and starts a fresh
search. Opaque result cursors and backend credentials never enter generated share
links. Preset windows resolve once per search; pagination retains those bounds
and Recent's original cutoff. A custom Through date includes the whole UTC day.

Run the pure URL/date-state tests with
`node --test examples/hackernews/tests/test_search_state.mjs`. The gateway tests
also cover shared-link reloads, fixed backend predicates and cursor binding.
Set `ANTFLY_NATIVE_BINARY` to exercise actual filtered/date-ordered native and
saved-overlay pagination rather than skipping that integration test.

## Gateway behavior

`serve.py` exposes a search form and `/api/search`. Its three views resolve only
`hn_archive`, `hn_current` and the saved overlay `hackernews`. Recent searches
filter creation time to the last year and retain the cutoff across cursor pages.
The historical view reads the immutable export; the combined view applies current
replacements by numeric `hn_id`. Results link to original HN items. Administration,
SQL, ingestion and arbitrary query DSL are not public gateway routes.

`/healthz` checks the gateway process. `/readyz` requires both text indexes to be
queryable and the saved source to exist. Readiness does not prove feed freshness,
latency targets, or replacement-pod recovery; monitor these independently.

## Optional standalone qualification

Render an offline candidate using an actual immutable image digest:

```sh
python examples/hackernews/deployment.py \
  --bucket antfly-hackernews-antfly-dev-01 \
  --image YOUR_REGISTRY_IMAGE@sha256:YOUR_IMAGE_DIGEST \
  --foundation-exists \
  --output /private/tmp/hackernews-serving

uv run --project examples/hackernews python examples/hackernews/configure_catalog.py \
  --warehouse gs://antfly-hackernews-antfly-dev-01/current \
  --source-connection hn-current --table-id hn-current --mode managed \
  > /private/tmp/hackernews-serving/current-table.json
```

Build `Dockerfile.serving` from the repository root with the qualified optimized
Linux binary at `qualification-linux/bin/antfly`. Record its source revision and
binary SHA separately from the built image digest. The initial candidate retains
the 8 CPU / 48 GiB benchmark profile for comparability; it is not an economical
serving recommendation. At the recorded rates, that engine allocation alone is
about $433/month before credits, versus about $46/month for 1 CPU / 4 GiB. Neither
smaller-profile latency nor a sustained serving budget is qualified yet.

For the optional standalone candidate, infrastructure prerequisites belong in the
Colony development Pulumi stack:

- A dedicated `us-central1` bucket with uniform access, public access prevention
  and no staging expiration lifecycle. Avoid `forceDestroy`; preserve generations
  needed for catalog recovery and cursor retention.
- A Workload Identity binding for the `hackernews/hackernews` Kubernetes service
  account. Grant object access only to this dedicated bucket. The three Antfly
  connection prefixes separately bind archive reads, current writes and derived
  artifacts. Use refreshable default credentials, never an injected bearer token
  or downloaded service-account key.
- Persistent catalog/shard/cache state on Colony's protected 20 GiB
  `hackernews-state` claim and `hackernews-retain-development` class. The
  single replica uses `Recreate` so two owners cannot write the same local state.
  This supports replacement-pod mounting; it is not high availability. Do not use
  Spot until replacement-owner recovery is actually qualified.
- A public TLS route to the gateway Service on port 80, request/body limits and
  abuse protection. Antfly itself binds loopback port 8877 and has no Service.
  Private operators can reach that API through an authenticated exec/port-forward.

The renderer creates no bucket, IAM binding, PVC, ingress, DNS record or pod.
Infrastructure changes must follow Colony's current-preview approval procedure.
With `--foundation-exists`, rendered resources omit the namespace, service
account and PVC, so application deployment does not take over Pulumi ownership.
No long-lived service should depend on the expiring import bucket or ephemeral
qualification catalog.

After infrastructure approval, copy the pinned archive objects server-side into
`archive/`, fencing each original generation and requiring absent destinations.
Capture and verify a new destination generation/checksum inventory; preserve the
original source. Create `hn_archive` with `archive-table.json`, `hn_current` with
the generated native managed-Iceberg definition, and `hackernews` with
`hackernews-source.json` through the private API. Initial publication and readiness
must complete before the public route receives traffic.

Run the existing Firebase producer in `ingest.py --native-rows` mode against
`hn_current`, with `--publish-interval 60` and persistent Antfly Lite state.
Keep older-record changes in the current replacement overlay too, so they can
suppress stale historical matches; the recent view's date filter does not alter
overlay ownership. Deleted HN items require the producer's retained ancestry and
tombstones. A rolling date boundary must not move ownership without migration.
The serving image does not package or launch the producer; its worker image,
qualified Linux Antfly Lite library, persisted ownership and supervision must be
prepared before declaring the feed operational.

Before enabling public traffic, qualify the actual image and workload identity,
credential refresh beyond one token lifetime, cold restart and replacement-pod
recovery, two-table overlay/cursor correctness, feed updates/deletes, and sustained
load. Keep source and state backups. A candidate manifest or a passing one-file
benchmark cannot substitute for those checks.

For the runtime handoff, Colony's development release overlay is
`config/antfly-releases-development.yaml`. Add the released image digest there with
`status: candidate`, `auto_upgrade: false` and an explicit rollback version.
Promote it to `dev_verified` only after the managed qualification passes. Keep the existing default
runtime intact while qualifying the candidate. The managed instance update uses
`target_antfly_image`; wait until `current_antfly_image` reports that exact digest
and the instance is ready before issuing HN data-plane requests. Colony's
`test/e2e/cloud_stack_e2e.py` accepts
`COLONY_E2E_CANDIDATE_RUNTIME_IMAGE` for this candidate health gate; it requires a
digest-pinned image. This verifies provisioning/proxy behavior separately from
standalone query timing. HN publication, object-store credential refresh,
historical/current composition and replacement recovery remain additional gates.

The repository release-line policy maps `0.3` releases to `main` and `0.2` releases
to `v0.2.x` (`scripts/release/release-lines.json`). Use the main-line release for
these Zig runtime changes; an existing 0.2 development catalog entry does not
contain them. Resolve the exact new tag from the final merged source and release
policy rather than relabelling a local qualification binary.
