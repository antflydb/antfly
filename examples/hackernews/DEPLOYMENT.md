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

The [consolidated Colony storage proposal](https://github.com/antflydb/colony/pull/762)
is disabled by default. Its 20 GiB claim is an unmeasured starting allocation for
standalone catalog/checkpoint/cache state, not the archive or a Cloud requirement.
Cloud owns its instance storage through Jobsaf/operator provisioning; do not attach
the example claim or create a parallel standalone engine for the real deployment.
The producer's persistent Antfly Lite state has separate ownership. Cloud tenant
identity must be verified before granting bucket access; the example's
`hackernews/hackernews` principal does not authorize a managed instance.

`serve.py` exposes a search form and `/api/search`. Its three views resolve only
`hn_archive`, `hn_current` and the saved overlay `hackernews`. Recent searches
filter creation time to the last year and retain the cutoff across cursor pages.
The historical view reads the immutable export; the combined view applies current
replacements by numeric `hn_id`. Results link to original HN items. Administration,
SQL, ingestion and arbitrary query DSL are not public gateway routes.

`/healthz` checks the gateway process. `/readyz` requires both text indexes to be
queryable and the saved source to exist. Readiness does not prove feed freshness,
latency targets, or replacement-pod recovery; monitor these independently.

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
