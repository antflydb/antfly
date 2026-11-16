# Managed HN rc.10 sample qualification — November 6, 2026

Runtime: `v0.3.0-rc.10`, source
`ad6f9b0c575253b01d4c42de321f07ee372f66d4`, OCI index
`sha256:c0561f5fd192ff494c96dedd11a7749ed4b24bcb9de415d39a38745b9df44bc2`.
The existing single-node Cloud instance has 1 CPU, 4 GiB memory and 20 GiB engine
storage. The archive and writable current table use tenant-scoped GCS access.
This record covers a frozen sample: 175 archive rows and 117 current rows.

## Release artifact

The published macOS server and embedded library were checksum-verified against
release metadata. The five standalone-auth/text/dense/sparse/hybrid E2Es pass
against that server in 90.27 seconds. These include unchanged-row receipt reads,
numeric inclusion/exclusion, newer writes, retained cuts and restart.

The E2E expiry assertion was updated for the released capability-sharing contract:
five-minute retention is a minimum, with expiry rounded up within a three-second
cohort. The lower bound remains five minutes and the upper bound remains bounded.

## Existing-state upgrade barrier

The rc.9 deployment used the earlier JSON lake WAL. rc.10 requires the shared
managed indexed WAL and rejects that prefix with `ManagedWalMigrationRequired`.
Existing derived serving metadata also failed to decode. Kubernetes readiness and
the Cloud version reconciler completed, while actual queries returned HTTP 500.
A clean-fixture release test does not establish existing-state upgrade safety.

The first attempt was returned to rc.9, and reads were verified before exporting
both tables, definitions, the saved source and the four accepted transactions.
Each WAL header and payload was verified against its content hash; replaying the
four transactions reconstructs all 117 current rows. A content-addressed export
is retained in the existing private checkpoint bucket. The checkpoint-4 portable
producer backup restores with rc.10, preserving its epoch, checkpoint and empty
pending intent.

Frozen logical row digests (rows sorted by `hn_id`, JSON keys sorted, compact
separators) are:

| Table | Rows | SHA-256 |
| --- | ---: | --- |
| Archive | 175 | `46fbe3c44d7d61b4e3387ed021f285719a94b11d14d58de9f4d185f50e4092d5` |
| Current | 117 | `cdc0de527ccaa35ae6e0c1d877a48469f8318ab6b2312ab2c1d98a458d51b719` |

## Fresh-incarnation procedure

Build new object tables under staging names. Keep the archive's immutable input;
use a fresh managed-Iceberg prefix for current. Use new relational-index names,
backend-assigned table identities and freshly published serving artifacts.
Replay the original four batches, preserving source epoch, batch IDs, checkpoints,
key fields and row images. Verify publication, complete row equality, index
readiness, exact retries of every original operation and the saved-overlay query.
Only then retain the original tables under dated names and rename the verified
staging tables to `hn_archive` and `hn_current`. The immutable `hackernews` saved
source continues to reference those public names.

The procedure was rehearsed locally with the published rc.10 binary, the exact
archive Parquet payload and all four original WAL payloads. Fresh tables,
complete row equality, the four renames, 72 unique `database` overlay matches and
restart passed. Original table definitions and object data are retained for
rollback; this procedure does not delete legacy tables or rewrite their WAL.

An incarnation change invalidates old receipt/cursor bindings. Qualify new
cursors across restart after cutover; do not reinterpret a prior incarnation's
capability against the replacement table. Keep the source writer stopped during
export, replay and cutover, then resume only the verified durable owner.

## Live verification

The existing Cloud instance is ready on the pinned rc.10 image, with version
reconciliation idle. Live staging passed complete row equality, publication,
index readiness, all four exact retries and the 72-match saved-overlay check.
The original bindings remain as `hn_archive_rc9_20261106` and
`hn_current_rc9_20261106`; no legacy table or object data was deleted. The saved
source retained its identity and definition, and the existing scoped read key
returned the same ranked first-page IDs for History, Recent and All after cutover.

One unchanged-row upsert advanced the original source epoch from checkpoint 4 to
5 at WAL LSN 5. Immediate accepted reads returned the exact row; combining its
numeric inclusion predicate with an exclusion returned zero rows. Exact replay
returned LSN 5, and the published read returned the same row. After one controlled
Cloud restart, accepted reads, published reads and exact replay still passed.

All three gateway cursors issued before that restart resumed without repeated
rows. Each initial page contained 20 rows; the continuation pages contained
20 History, 17 Recent and 20 All rows. Single request observations were:

| View | Before-restart first page | After-restart continuation |
| --- | ---: | ---: |
| History | 0.438 s | 3.783 s |
| Recent | 1.045 s | 3.932 s |
| All | 2.540 s | 10.580 s |

These requests exercise different pages and cache states through the managed
proxy and private gateway; they are not a matched benchmark or percentile
estimate. The 10.580-second All continuation remains a performance concern even
on this sample. Functional acceptance does not establish public-launch latency.

The authoritative Lite source owner is now checkpoint 5, with the original epoch
and no pending intent. A portable backup was restored and all 497 stored records
were compared with the owner. Its SHA-256 is
`a73f3b12c99132b0841d766752d4a24ed417d9dc2b4a589474c834ba4d6c0b5e`;
the content-addressed backup is retained in the private checkpoint bucket.
Checkpoint-4 backups are archival and must not be resumed as the current writer.
The continuous producer remains stopped and the gateway remains private. No
additional instance, VM or serving capacity was provisioned for this run.

## Follow-up latency attribution

Two additional pairs of first/continuation pages were checked through both the
managed API and private gateway, with 20 distinct rows per page. Managed request
durations were 2.518/2.105 seconds and 21.497/2.329 seconds; gateway durations were
12.893/2.242 seconds and 2.497/2.221 seconds. Do not discard the slow first pages.
Cloud Run recorded a new container at 02:58:01 UTC on November 7 and startup-probe
success at 02:58:11. Its request log recorded 12.440 seconds for the corresponding
12.893-second client observation, followed by 2.134 seconds for continuation on
the same container. This reproduces a gateway startup delay independently of an
engine restart. It does not retroactively explain the earlier 10.580-second
continuation, and the 21.497-second managed outlier still needs proxy/transport
attribution.

After this batch, the engine's retained slowest composed request was 2.454 seconds:
2.348 seconds in leaf callbacks, 0.103 seconds in cursor work and 0.003 seconds in
composition. Those are phases of one retained maximum, not per-request deltas;
the health metrics cache refreshes every five seconds. No composed request in
this process exceeded that maximum at the settled observation.

The same logical sample and four original transactions were reconstructed
locally using the released rc.10 binary and this PR's gateway. Immediately after
an isolated local engine restart, the first All page took 41.7 ms and continuation
31.1 ms. Four subsequent first/continuation pages took 28.5–30.9 ms. All six pages
passed row-count/duplicate checks and supplied valid upstream timing. These use
local filesystem data/artifacts and an unconstrained developer host, not matched
Cloud resources or remote GCS; they establish a functional local baseline rather
than a remote-storage or DuckDB comparison.

The follow-up adds search-first qualification, an optional latency gate and
numeric upstream timing to avoid attributing every end-to-end delay to search.
It also declares the generated archive files immutable, matching the fresh-name
export contract and avoiding per-file HEAD revalidation. Existing bindings still
use their previous schemas. The hosting probe correction, deployment of the new
gateway, and existing-binding optimization require their own rollout checks;
these measurements do not establish that any of them has been applied live.

The updated qualification command was also run against the existing private
gateway with `--search-first --max-search-seconds 5`. Functional and latency gates
passed for this observation: History 0.655 s, Recent 0.994 s and All 2.635 s.
Health/readiness ran afterward. The older deployed gateway does not yet supply
the new upstream timing header. This successful observation does not cancel the
earlier outliers or establish a cold-start percentile. Focused gateway,
qualification and plan tests passed (24 tests, one optional skip, nine subtests).

### Managed outlier origin correlation

The request at 02:58:17 UTC corresponding to the 21.497-second client observation
completed in Colony in 2.253 seconds. Google's load balancer recorded 2.293 seconds,
HTTP 200 and `response_sent_by_backend` for the matching route/window. This places
the unexplained additional delay beyond the Google origin, on the Cloudflare/client
delivery or local observation path. It does not identify the specific fault, and
no original Cloudflare Ray ID or client phase trace was captured for that request.

Twelve additional read-only searches with DNS/TCP/TLS/header/body/close traces
completed in 2.248–2.788 seconds; the longest body read was 104 ms. None reproduced
the 21.497-second stall. Three more real managed queries validated the new
correlation capture: 2.399, 2.518 and 2.421 seconds, with body/decode times of
3–4 ms. All returned 20 distinct rows and valid UUID response request and
Cloudflare Ray IDs. The later CLI check below found that those response IDs do
not match deployed Colony access-log IDs. The updated gateway propagates only
bounded validated IDs, including on
upstream errors, and qualification reports retain absolute start and header/body
and close times. A regression test ensures a delayed connection close remains in
the total and outside-upstream duration. Focused tests now pass 26 cases, with one
optional skip and nine subtests.
The transport fault is not claimed fixed; further attribution requires a
recurrence with these IDs/timings or Cloudflare-side telemetry.

### Applied hosting probe correction

[Colony #952](https://github.com/antflydb/colony/pull/952), source
`90d48b808776cf902d9bb4dac2743c0d91763a01`, owns the probe correction. After
successful infrastructure build/tests and explicit saved-plan approval, the
documented deploy service account's apply was denied `run.services.update`.
Read-only inspection confirmed no resource, output or pending-operation changes.
A fresh preview and explicit retry approval used the verified existing human
ADC identity; no IAM permissions were added. The retry completed with one HN
service updated and 67 resources unchanged.

Revision `hackernews-00004-njc` uses the same gateway image and a one-second
startup probe, one-second timeout and 30 failures. At 03:57:57.580 UTC November 7,
Cloud Run started its deployment-rollout container; the probe passed at
03:57:58.888, about 1.31 seconds later, versus about 10.17 seconds in the earlier
autoscaling observation. These are individual startup events, not matched
cold-start percentiles. The first post-rollout search-first qualification passed
its five-second gate: History 0.632 s, Recent 1.049 s, All 2.524 s, with the same
ranked IDs as the pre-migration golden record for every view. Readiness passed
after the searches. Private invocation, scale-to-zero, the pinned gateway image,
67 stored resources, 16 outputs and zero pending operations were verified.

The gateway timing code from merged Antfly #1547 and the correlation additions
in its follow-up are not yet deployed. This probe-only rollout did not deploy
that code, change the database image, or optimize existing table schemas. Keep
those boundaries separate from the verified probe correction.

## Additional CLI transport checks

Twenty further serial `curl` searches compared HTTP/1.1 and HTTP/2 with fresh
and reused connections. Each returned HTTP 200 and 20 distinct rows:

| Connection mode | Minimum | Median | Maximum |
| --- | ---: | ---: | ---: |
| Fresh HTTP/1.1 | 2.370 s | 2.492 s | 2.709 s |
| Fresh HTTP/2 | 2.292 s | 2.391 s | 2.443 s |
| Reused HTTP/1.1 | 2.203 s | 2.383 s | 2.613 s |
| Reused HTTP/2 | 1.941 s | 2.272 s | 2.293 s |

DNS took at most 47 ms, TCP/TLS establishment after DNS at most 76 ms, and body
transfer at most 8 ms. A local heartbeat's longest scheduling gap was 65 ms.
Engine cgroup counters showed zero throttled periods/time before and after the
run; observed engine/API CPU usage was 235m/9m. No host sleep/wake event was found
in the original outlier's ten-minute window. These checks do not exclude a past
client scheduling pause or transient packet loss. A forced IPv6 request used
IPv4-mapped addresses, so it does not qualify a native IPv6 path.

The deployed API response IDs matched none of the 20 Colony log IDs. Origin
requests were instead paired by the isolated time window and serial order:
1.850–2.509 seconds at Colony, with at most 231 ms of excess client time. This
is time/order attribution, not an exact-ID join. The canonical-ID correction is
already merged in [Colony #947](https://github.com/antflydb/colony/pull/947), but
the published releases checked predate that merge. A containing Colony release
and deployment is needed before response/log-ID correlation can be relied on.
It is an observability correction, not evidence that the transport stall is fixed.

Neither protocol nor connection reuse reproduced the 21.497-second observation.
The normal baseline in this run was mostly origin processing, while the original
outlier's unexplained excess remains beyond Google's origin. Cloudflare edge
telemetry or a fully traced recurrence remains necessary for the specific cause.

An additional three All searches were bracketed by settled engine metrics,
waiting six seconds on either side for the five-second exposition cache. The
deltas contained exactly three composed requests and 15 leaf calls. Composed
execution totaled 6.717 seconds; source setup 3.377 seconds, publication selection
1.098 seconds, leaf cursor retention 0.605 seconds, WAL-cut work 0.546 seconds,
composed cursor work 0.327 seconds and ranking 0.074 seconds. Phases nest, so do
not add every counter. Per search, source/publication averaged 1.492 seconds,
leaf/composed cursor work 0.311 seconds, WAL work 0.182 seconds and ranking
0.025 seconds. Client durations were 2.557, 2.407 and 2.240 seconds. These data
prioritize repeated source/publication setup and durable cursor round trips over
ranking CPU for the ordinary sample baseline. They do not explain the isolated
21.497-second excess or establish archive-scale performance.

## Remaining scope

These are bounded sample checks. Full-archive construction/performance,
continuous producer supervision and public launch retain their separate gates.
A general rollout needs a format-aware preflight and a resumable fenced migration
workflow; pod readiness alone did not detect this existing-state incompatibility.
Legacy bindings can still emit format-related maintenance warnings; their eventual
retirement needs a separate reviewed cleanup after the rollback retention period.
