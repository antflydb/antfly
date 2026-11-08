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

## Remaining scope

These are bounded sample checks. Full-archive construction/performance,
continuous producer supervision and public launch retain their separate gates.
A general rollout needs a format-aware preflight and a resumable fenced migration
workflow; pod readiness alone did not detect this existing-state incompatibility.
Legacy bindings can still emit format-related maintenance warnings; their eventual
retirement needs a separate reviewed cleanup after the rollback retention period.
